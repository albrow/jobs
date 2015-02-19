package zazu

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"reflect"
	"sync"
	"time"
)

// Pool is a pool of workers. Pool will query the database for queued jobs
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
			w.doJob(job)
		}
		w.wg.Done()
	}()
}

// doJob executes the given job. It also sets the status and timestamps for
// the job appropriately depending on the outcome of the execution.
func (w *worker) doJob(job *Job) {
	defer func() {
		if r := recover(); r != nil {
			// Get a reasonable error message from the panic
			msg := ""
			if err, ok := r.(error); ok {
				msg = err.Error()
			} else {
				msg = fmt.Sprint(r)
			}
			// Start a new transaction
			t := newTransaction()
			// Set the job error field
			t.command("HSET", redis.Args{job.key(), "error", msg}, nil)
			// Either queue the job for retry or mark it as failed depending
			// on how many retries the job has left
			t.retryOrFailJob(job, nil)
			if err := t.exec(); err != nil {
				panic(err)
			}
		}
	}()
	// Set the started field and save the job
	job.started = time.Now().UTC().UnixNano()
	t0 := newTransaction()
	t0.command("HSET", redis.Args{job.key(), "started", job.started}, nil)
	if err := t0.exec(); err != nil {
		// NOTE: panics will be caught by the recover statment above
		panic(err)
	}
	// Use reflection to instantiate arguments for the handler
	handlerArgs := []reflect.Value{}
	if job.typ.dataType != nil {
		// Instantiate a new variable to hold this argument
		dataVal := reflect.New(job.typ.dataType)
		if err := decode(job.data, dataVal.Interface()); err != nil {
			// NOTE: panics will be caught by the recover statment above
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
	t1.command("HSET", redis.Args{job.key(), "finished", job.finished}, nil)
	if job.isRecurring() {
		// If the job is recurring, reschedule and set status to queued
		job.time = job.nextTime()
		t1.command("HSET", redis.Args{job.key(), "time", job.time}, nil)
		t1.addJobToTimeIndex(job)
		t1.setJobStatus(job, StatusQueued)
	} else {
		// Otherwise, set status to finished
		t1.setJobStatus(job, StatusFinished)
	}
	if err := t1.exec(); err != nil {
		// NOTE: panics will be caught by the recover statment above
		panic(err)
	}
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
// any new jobs. However, any jobs that are currently being executed
// will still be executed. Close returns immediately. If you want to
// wait until all workers are done executing their current jobs, use the
// Wait method.
func (wp *workerPoolType) Close() {
	wp.exit <- true
}

// Wait will return when all workers are done executing their jobs.
// Wait can only possibly return after you have called Close. To prevent
// errors due to partially-executed jobs, any go program which starts a
// worker pool should call Wait (and Close before that if needed) before
// exiting.
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

// sendNextJobs queries the database to find the next n ready jobs, then
// sends those jobs to the jobs channel, effectively delegating them to
// a worker.
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

// getNextJobs queries the database and returns the next n ready jobs.
func getNextJobs(n int) ([]*Job, error) {
	// Start a new transaction
	t := newTransaction()
	// Invoke a script to get all the jobs which are ready to execute based on their
	// time parameter and whether or not they are in the queued set.
	jobs := []*Job{}
	t.popNextJobs(n, newScanJobsHandler(&jobs))

	// Execute the transaction
	if err := t.exec(); err != nil {
		return nil, err
	}
	return jobs, nil
}
