package zazu

import (
	"github.com/garyburd/redigo/redis"
	"reflect"
	"sync"
	"time"
)

// Pool represents a pool of workers. Pool will query the database for queued jobs
// and delegate those jobs to some number of workers. It will do this continuously
// until the main program exits or you call Pool.Close().
var Pool = &workerPoolType{}

// worker continuously executes jobs within its own goroutine.
// The jobs chan is shared between all jobs. To stop the worker,
// simply close the jobs channel.
type worker struct {
	jobs chan *Job
	wg   *sync.WaitGroup
}

// start starts a goroutine in which the worker will continuously
// execute jobs until the jobs channel is closed.
func (w *worker) start() {
	go func() {
		for job := range w.jobs {
			// Set the started field and save the job
			job.started = time.Now().UTC().UnixNano()
			t0 := newTransaction()
			args0 := redis.Args{job.key(), "started", job.started}
			t0.command("HSET", args0, nil)
			if err := t0.exec(); err != nil {
				// TODO: set the job status to StatusError instead of panicking
				panic(err)
			}
			// Use reflection to instantiate arguments for the handler
			handlerArgs := []reflect.Value{}
			if job.typ.dataType != nil {
				// Instantiate a new variable to hold this argument
				dataVal := reflect.New(job.typ.dataType)
				if err := decode(job.data, dataVal.Interface()); err != nil {
					// TODO: set the job status to StatusError instead of panicking
					panic(err)
				}
				handlerArgs = append(handlerArgs, dataVal.Elem())
			}
			// Call the handler using the arguments we just instantiated
			handlerVal := reflect.ValueOf(job.typ.handler)
			handlerVal.Call(handlerArgs)
			// Set the finished timestamp
			job.finished = time.Now().UTC().UnixNano()
			t1 := newTransaction()
			redisArgs := redis.Args{job.key(), "finished", job.finished}
			t1.command("HSET", redisArgs, nil)
			if job.isRecurring() {
				// If the job is recurring, reschedule and set status to queued
				job.time = job.nextTime()
				redisArgs = redis.Args{job.key(), "time", job.time}
				t1.command("HSET", redisArgs, nil)
				t1.addJobToTimeIndex(job)
				t1.setJobStatus(job, job.status, StatusQueued)
			} else {
				// Otherwise, set status to finished
				t1.setJobStatus(job, job.status, StatusFinished)
			}
			if err := t1.exec(); err != nil {
				// TODO: set the job status to StatusError instead of panicking
				panic(err)
			}
		}
		w.wg.Done()
	}()
}

// workerPoolType is the type of Pool. End users should not instantiate
// their own pools, so this type is private.
type workerPoolType struct {
	// workers is a slice of all workers
	workers []*worker
	// jobs is a channel through which jobs are delegated to workers
	jobs chan *Job
	// wg can be used after the jobs channel is closed to wait for all
	// workers to finish executing their current jobs.
	wg *sync.WaitGroup
	// exit is used to signal the pool to stop running the query loop
	// and close the jobs channel
	exit chan bool
	// sync.Mutex is used to lock when modifying attributes of the worker pool
	// e.g. in Start()
}

// Start starts the worker pool. This means the pool will initialize workers,
// continuously query the database for queued jobs, and delegate those jobs
// to the workers.
func (wp *workerPoolType) Start() {
	wp.workers = make([]*worker, Config.Pool.NumWorkers)
	wp.jobs = make(chan *Job, Config.Pool.BatchSize)
	wp.wg = &sync.WaitGroup{}
	wp.exit = make(chan bool)
	for i := range wp.workers {
		wp.wg.Add(1)
		worker := &worker{}
		wp.workers[i] = worker
		worker.wg = wp.wg
		worker.jobs = wp.jobs
		worker.start()
	}
	go func() {
		if err := wp.queryLoop(); err != nil {
			// TODO: send the err accross a channel instead of panicking
			panic(err)
		}
	}()
}

// Close closes the worker pool and prevents it from delegating
// any new jobs. It returns immediately. If you want to wait until
// all workers are done executing their current jobs, use the Wait
// method.
func (wp *workerPoolType) Close() {
	wp.exit <- true
}

// Wait will return when all workers are done executing their jobs.
// Wait can only possibly return after you have called Close.
func (wp *workerPoolType) Wait() {
	// The shared waitgroup will only return after each worker is finished
	wp.wg.Wait()
}

// queryLoop continuously queries the database for new jobs and, if
// it finds any, sends them through the jobs channel for execution
// by some worker.
func (wp *workerPoolType) queryLoop() error {
	if err := wp.sendNextJobs(Config.Pool.BatchSize); err != nil {
		return err
	}
	for {
		minWait := time.After(Config.Pool.MinWait)
		select {
		case <-wp.exit:
			// Close the channel to tell workers to stop executing new jobs
			close(wp.jobs)
			return nil
		case <-minWait:
			if err := wp.sendNextJobs(Config.Pool.BatchSize); err != nil {
				return err
			}
		}
	}
	return nil
}

func (wp *workerPoolType) sendNextJobs(n int) error {
	jobs, err := getNextJobs(Config.Pool.BatchSize)
	if err != nil {
		return err
	}
	// Send the jobs across the channel, where they will be picked up
	// by exactly one worker
	for _, job := range jobs {
		wp.jobs <- job
	}
	return nil
}

func getNextJobs(n int) ([]*Job, error) {
	// Start the first transaction, which gets the ids of jobs which are ready to execute
	// based on their time parameter whether or not they are in the queued set.
	t0 := newTransaction()

	// Copy the time index set to a new set with a temporary name
	jobsReadyByTimeKey := jobsReadyByTime.generateKey()
	t0.command("ZUNIONSTORE", redis.Args{jobsReadyByTimeKey, 1, keys.jobsTimeIndex}, nil)

	// Trim the new temporary set we created to leave only the jobs which have a time parameter in the past
	currentTime := time.Now().UTC().UnixNano()
	t0.command("ZREMRANGEBYSCORE", redis.Args{jobsReadyByTimeKey, currentTime, "+inf"}, nil)

	// Intersect the jobs which are ready based on their time with those in the
	// queued set. Store the results in a temporary set.
	jobsReadyAndSortedKey := jobsReadyAndSorted.generateKey()
	t0.command("ZINTERSTORE", redis.Args{jobsReadyAndSortedKey, 2, StatusQueued.key(), jobsReadyByTimeKey, "WEIGHTS", 1, 0}, nil)

	// Trim the jobs:readyAndSorted set, so it contains only the first n jobs ordered by
	// priority
	t0.command("ZREMRANGEBYRANK", redis.Args{jobsReadyAndSortedKey, 0, -n - 1}, nil)

	// Get all the jobs from the jobs:readyAndSorted set, which contains the next n jobs that
	// are ready based on their time parameter sorted by priority.
	jobIds := []string{}
	t0.script(getAndMoveJobsToExecuting, redis.Args{jobsReadyAndSortedKey, StatusQueued.key(), StatusExecuting.key()}, newScanStringsHandler(&jobIds))

	// Delete the temporary sets we created for intersecting
	t0.command("DEL", redis.Args{jobsReadyByTimeKey, jobsReadyAndSortedKey}, nil)

	// Execute the transaction
	if err := t0.exec(); err != nil {
		return nil, err
	}

	// Start the second transaction, which gets all the other data for the job ids we have
	t1 := newTransaction()
	jobs := []*Job{}
	for _, jobId := range jobIds {
		// Add a command to set the status to executing
		job := &Job{id: jobId}
		t1.command("HSET", redis.Args{job.key(), "status", string(StatusExecuting)}, nil)
		// Add a command to get the job fields from its hash in the database
		t1.scanJobById(job.id, job)
		jobs = append(jobs, job)
	}

	// Execute the transaction
	if err := t1.exec(); err != nil {
		return nil, err
	}
	return jobs, nil
}
