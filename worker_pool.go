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
			job.Lock()
			if err := job.setStatus(StatusFinished); err != nil {
				// TODO: set the job status to StatusError instead of panicking
				panic(err)
			}
			job.Unlock()
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
	// TODO: take into account the time parameter
	conn := redisPool.Get()
	defer conn.Close()
	// Start the first transaction, which gets the job ids
	if err := conn.Send("MULTI"); err != nil {
		return nil, err
	}
	// Get the next n jobs from the queued set
	if err := conn.Send("ZREVRANGE", "jobs:"+StatusQueued, 0, n-1); err != nil {
		return nil, err
	}
	// Remove them
	if err := conn.Send("ZREMRANGEBYRANK", "jobs:"+StatusQueued, -n, -1); err != nil {
		return nil, err
	}
	// Execute the transaction
	reply, err := conn.Do("EXEC")
	if err != nil {
		return nil, err
	}
	// Parse the replies. The first one is what we care about, the ids of the jobs
	// we grabbed from the queued set
	replies, err := redis.Values(reply, nil)
	if err != nil {
		return nil, err
	}
	jobIds, err := redis.Strings(replies[0], nil)
	if err != nil {
		return nil, err
	}
	// Start the second transaction, which gets all the other data for the job ids we have
	if err := conn.Send("MULTI"); err != nil {
		return nil, err
	}
	for _, jobId := range jobIds {
		// Add a command to set the status to executing
		if err := conn.Send("HSET", "jobs:"+jobId, "status", string(StatusExecuting)); err != nil {
			return nil, err
		}
		// Add a command to get the job fields from its hash in the database
		if err := conn.Send("HGETALL", "jobs:"+jobId); err != nil {
			return nil, err
		}
	}
	// Execute the transaction
	reply, err = conn.Do("EXEC")
	if err != nil {
		return nil, err
	}
	// Parse the replies.
	replies, err = redis.Values(reply, nil)
	if err != nil {
		return nil, err
	}
	jobs := make([]*Job, len(jobIds))
	// We expect every odd reply to be the field data for a given job corresponding to an HGETALL command
	// Recall that the even replies were from commands to set the job status corresponding to an HSET command
	for i := 0; i < len(replies)-1; i += 2 {
		reply := replies[i+1]
		job := &Job{}
		jobs[i/2] = job
		job.id = jobIds[i/2]
		job.Lock()
		if err := scanJob(reply, job); err != nil {
			return nil, err
		}
		job.Unlock()
	}
	return jobs, nil
}
