// Copyright 2015 Alex Browne.  All rights reserved.
// Use of this source code is governed by the MIT
// license, which can be found in the LICENSE file.

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
	// id is a unique identifier for each worker, which is generated whenver
	// Start() is called
	id string
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
}

// addToPoolSet adds the id of the worker pool to a set of active pools
// in the database.
func (wp *workerPoolType) addToPoolSet() error {
	conn := redisPool.Get()
	defer conn.Close()
	if _, err := conn.Do("SADD", keys.activePools, wp.id); err != nil {
		return err
	}
	return nil
}

// removeFromPoolSet removes the id of the worker pool from a set of active pools
// in the database.
func (wp *workerPoolType) removeFromPoolSet() error {
	conn := redisPool.Get()
	defer conn.Close()
	if _, err := conn.Do("SREM", keys.activePools, wp.id); err != nil {
		return err
	}
	return nil
}

// pingKey is the key for a pub/sub connection which allows a pool to ping, i.e.
// check the status of, another pool.
func (wp *workerPoolType) pingKey() string {
	return "workers:" + wp.id + ":ping"
}

// pongKey is the key for a pub/sub connection which allows a pool to respond to
// pings with a pong, i.e. acknowledge that it is still alive and working.
func (wp *workerPoolType) pongKey() string {
	return "workers:" + wp.id + ":pong"
}

// purgeStalePools will first get all the ids of pools from the activePools
// set. All of these should be active, but if Pool.Wait was never called for
// a pool (possibly because of power failure), some of them might not actually
// be active. To find out for sure, purgeStalePools will ping each pool that is
// supposed to be active and wait for a pong response. If it does not receive
// a pong within some amount of time, the pool is considered stale (i.e. whatever
// process that was running it was exited and it is no longer executing jobs). If
// any stale pools are found, purgeStalePools will remove them from the set of
// active pools and then moves any jobs associated with the stale pool from the
// executing set to the queued set to be retried.
func (wp *workerPoolType) purgeStalePools() error {
	conn := redisPool.Get()
	defer conn.Close()
	poolIds, err := redis.Strings(conn.Do("SMEMBERS", keys.activePools))
	if err != nil {
		return err
	}
	for _, poolId := range poolIds {
		if poolId == wp.id {
			// Don't ping self
			continue
		}
		pool := &workerPoolType{id: poolId}
		go func(pool *workerPoolType) {
			if err := wp.pingAndPurgeIfNeeded(pool); err != nil {
				// TODO: send accross an err channel instead of panicking
				panic(err)
			}
		}(pool)
	}
	return nil
}

// pingAndPurgeIfNeeded pings other by publishing to others ping key. If it
// does not receive a pong reply within some amount of time, it will
// assume the pool is stale and purge it.
func (wp *workerPoolType) pingAndPurgeIfNeeded(other *workerPoolType) error {
	ping := redisPool.Get()
	pong := redis.PubSubConn{redisPool.Get()}

	// Listen for pongs by subscribing to the other pool's pong key
	pong.Subscribe(other.pongKey())
	// Ping the other pool by publishing to its ping key
	ping.Do("PUBLISH", other.pingKey(), 1)
	// Use a select statement to either receive the pong or timeout
	pongChan := make(chan interface{})
	errChan := make(chan error)
	go func() {
		defer func() {
			pong.Close()
			ping.Close()
		}()
		select {
		case <-wp.exit:
			return
		default:
		}
		for {
			reply := pong.Receive()
			switch reply.(type) {
			case redis.Message:
				// The pong was received
				pongChan <- reply
				return
			case error:
				// There was some unexpected error
				err := reply.(error)
				errChan <- err
				return
			}
		}
	}()
	timeout := time.After(Config.Pool.StaleTimeout)
	select {
	case <-pongChan:
		// The other pool responded with a pong
		return nil
	case err := <-errChan:
		// Received an error from the pubsub conn
		return err
	case <-timeout:
		// The pool is considered stale and should be purged
		t := newTransaction()
		t.purgeStalePool(other.id)
		if err := t.exec(); err != nil {
			return err
		}
	}
	return nil
}

// respondToPings continuously listens for pings from other worker pools and
// immediately responds with a pong. It will only return if there is an error.
func (wp *workerPoolType) respondToPings() error {
	pong := redisPool.Get()
	ping := redis.PubSubConn{redisPool.Get()}
	defer func() {
		pong.Close()
		ping.Close()
	}()
	// Subscribe to the ping key for this pool to receive pings.
	if err := ping.Subscribe(wp.pingKey()); err != nil {
		return err
	}
	for {
		// Whenever we recieve a ping, reply immediately with a pong by
		// publishing to the pong key for this pool.
		switch reply := ping.Receive().(type) {
		case redis.Message:
			if _, err := pong.Do("PUBLISH", wp.pongKey(), 0); err != nil {
				return err
			}
		case error:
			err := reply.(error)
			return err
		}
		time.Sleep(1 * time.Millisecond)
	}
}

// Start starts the worker pool. This means the pool will initialize workers,
// continuously query the database for queued jobs, and delegate those jobs
// to the workers.
func (wp *workerPoolType) Start() error {
	// Assign an id if needed
	if wp.id == "" {
		wp.id = generateRandomId()
	}
	// Do some bookkeeping related checking status of worker pools
	wp.wg = &sync.WaitGroup{}
	wp.exit = make(chan bool)
	if err := wp.addToPoolSet(); err != nil {
		return err
	}
	go func() {
		select {
		case <-wp.exit:
			return
		default:
		}
		if err := wp.respondToPings(); err != nil {
			// TODO: send the err accross a channel instead of panicking
			panic(err)
		}
	}()
	if err := wp.purgeStalePools(); err != nil {
		return err
	}

	// Initialize the fields of wp
	wp.workers = make([]*worker, Config.Pool.NumWorkers)
	wp.jobs = make(chan *Job, Config.Pool.BatchSize)
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
	return nil
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
func (wp *workerPoolType) Wait() error {
	// The shared waitgroup will only return after each worker is finished
	wp.wg.Wait()
	// Remove the pool id from the set of active pools, only after we know
	// each worker finished executing.
	if err := wp.removeFromPoolSet(); err != nil {
		return err
	}
	return nil
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
	jobs, err := wp.getNextJobs(Config.Pool.BatchSize)
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
func (wp *workerPoolType) getNextJobs(n int) ([]*Job, error) {
	return getNextJobs(n, wp.id)
}

// getNextJobs queries the database and returns the next n ready jobs.
func getNextJobs(n int, poolId string) ([]*Job, error) {
	// Start a new transaction
	t := newTransaction()
	// Invoke a script to get all the jobs which are ready to execute based on their
	// time parameter and whether or not they are in the queued set.
	jobs := []*Job{}
	t.popNextJobs(n, poolId, newScanJobsHandler(&jobs))

	// Execute the transaction
	if err := t.exec(); err != nil {
		return nil, err
	}
	return jobs, nil
}
