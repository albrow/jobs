package zazu

import (
	"fmt"
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
			fmt.Println("Worker received job: %v", job)
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
}

// Start starts the worker pool. This means the pool will initialize workers,
// continuously query the database for queued jobs, and delegate those jobs
// to the workers.
func (wp *workerPoolType) Start() {
	wp.workers = make([]*worker, NumWorkers)
	wp.jobs = make(chan *Job, BatchSize)
	wp.wg = &sync.WaitGroup{}
	for _, worker := range wp.workers {
		wp.wg.Add(1)
		worker.wg = wp.wg
		worker.jobs = wp.jobs
		worker.start()
	}
}

// Close closes the worker pool and prevents it from delegating
// any new jobs. It returns immediately. If you want to wait until
// all workers are done executing their current jobs, use the Wait
// method.
func (wp *workerPoolType) Close() {
	// Close the channel to tell workers to stop executing new jobs
	close(wp.jobs)
}

// Wait will return when all workers are done executing their jobs.
// Wait can only possibly return after you have called Close.
func (wp *workerPoolType) Wait() {
	// The shared waitgropu will only return after each worker is finished
	wp.wg.Wait()
}

// queryLoop continuously queries the database for new jobs and, if
// it finds any, sends them through the jobs channel for execution
// by some worker.
func (wp *workerPoolType) queryLoop() {
	// TODO: implement this
}
