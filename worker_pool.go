package zazu

import (
	"github.com/garyburd/redigo/redis"
	"reflect"
	"runtime"
	"sync"
	"time"
)

// Pool represents a pool of workers. Pool will query the database for queued jobs
// and delegate those jobs to some number of workers. It will do this continuously
// until the main program exits or you call Pool.Close().
var Pool = &workerPoolType{}

var (
	// NumWorkers is the number of workers to run
	// Each worker will run inside its own goroutine
	// and execute jobs asynchronously.
	NumWorkers = runtime.GOMAXPROCS(0)
	// BatchSize is the number of jobs to send through
	// the jobs channel at once. Increasing BatchSize means
	// the worker pool will query the database less frequently,
	// so you would get higher performance. However this comes
	// at the cost that jobs with lower priority may sometimes be
	// executed before jobs with higher priority, because the jobs
	// with higher priority were not ready yet the last time the pool
	// queried the database. Decreasing BatchSize means more
	// frequent queries to the database and lower performance, but
	// greater likelihood of executing jobs in perfect order with regards
	// to priority. Setting BatchSize to 1 gaurantees that higher priority
	// jobs are always executed first as soon as they are ready.
	BatchSize = NumWorkers
	// MaxWait is the maximum time the pool will wait before checking the
	// database for queued jobs. The pool may query the database more frequently
	// than MaxWait if it thinks there is likely to be new queued job. (e.g.,
	// if it knows the time parameter of a job was satisfied since the last
	// time it checked).
	MaxWait = 200 * time.Millisecond
)

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
			// Instantiate a new variable to hold the data for this job
			dataVal := reflect.New(job.typ.dataType)
			if err := decode(job.data, dataVal.Interface()); err != nil {
				// TODO: set the job status to StatusError instead of panicking
				panic(err)
			}
			// Call the handler using the data we just instantiated and scanned
			handlerVal := reflect.ValueOf(job.typ.handler)
			handlerVal.Call([]reflect.Value{dataVal.Elem()})
			// After we have called the handler function, mark the status as finished
			// and set the finished timestamp
			job.finished = time.Now().UTC().UnixNano()
			t1 := newTransaction()
			args1 := redis.Args{job.key(), "finished", job.finished}
			t1.command("HSET", args1, nil)
			t1.setJobStatus(job, job.status, StatusFinished)
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
	wp.workers = make([]*worker, NumWorkers)
	wp.jobs = make(chan *Job, BatchSize)
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
	if err := wp.sendNextJobs(BatchSize); err != nil {
		return err
	}
	for {
		select {
		case <-wp.exit:
			// Close the channel to tell workers to stop executing new jobs
			close(wp.jobs)
			return nil
		default:
			if err := wp.sendNextJobs(BatchSize); err != nil {
				return err
			}
		}
	}
	return nil
}

func (wp *workerPoolType) sendNextJobs(n int) error {
	jobs, err := getNextJobs(BatchSize)
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
	// TODO: use lua scripting here to preserve absolute atomicity
	conn := redisPool.Get()
	defer conn.Close()
	// First get the ids of jobs which are currently ready to execute based on
	// their time parameter.
	currentTime := time.Now().UTC().UnixNano()
	timeReadyJobIds, err := redis.Values(conn.Do("ZRANGEBYSCORE", keys.jobsTimeIndex, "-inf", currentTime))
	if err != nil {
		return nil, err
	}
	conn.Close()
	// If no jobs are ready based on their time, just return
	if len(timeReadyJobIds) == 0 {
		return nil, nil
	}
	// Start the first transaction, which gets the ids of jobs which are ready to execute
	// based on their time parameter whether or not they are in the queued set.
	t0 := newTransaction()
	// Store the ids from the time index query in their own temporary set, which has a unique name
	jobsReadyByTimeKey := jobsReadyByTime.generateKey()
	args := redis.Args{jobsReadyByTimeKey}
	for _, jobId := range timeReadyJobIds {
		args = args.Add(0, jobId)
	}
	t0.command("ZADD", args, nil)
	// Intersect the jobs which are ready based on their time with those in the
	// queued set. Store the results in a temporary set.
	jobsReadyAndSortedKey := jobsReadyAndSorted.generateKey()
	args = redis.Args{jobsReadyAndSortedKey, 2, StatusQueued.key(), jobsReadyByTimeKey}
	t0.command("ZINTERSTORE", args, nil)
	// Trim the jobs:readyAndSorted set, so it contains only the first n jobs ordered by
	// priority
	args = redis.Args{jobsReadyAndSortedKey, 0, -n - 1}
	t0.command("ZREMRANGEBYRANK", args, nil)
	// Get all the jobs from the jobs:readyAndSorted set, which contains the next n jobs that
	// are ready based on their time parameter sorted by priority.
	jobIds := []string{}
	args = redis.Args{jobsReadyAndSortedKey, 0, -1}
	t0.command("ZREVRANGE", args, newScanStringsHandler(&jobIds))
	// Add the jobs to the executing set using ZUNIONINTERSTORE
	args = redis.Args{StatusExecuting.key(), 2, jobsReadyAndSortedKey, StatusExecuting.key()}
	t0.command("ZUNIONSTORE", args, nil)
	// Remove the jobs from the queued set
	args = redis.Args{StatusQueued.key(), -n, -1}
	t0.command("ZREMRANGEBYRANK", args, nil)
	// Delete the temporary sets we created for intersecting
	args = redis.Args{jobsReadyByTimeKey, jobsReadyAndSortedKey}
	t0.command("DEL", args, nil)
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
		args := redis.Args{job.key(), "status", string(StatusExecuting)}
		t1.command("HSET", args, nil)
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
