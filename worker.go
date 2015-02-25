// Copyright 2015 Alex Browne.  All rights reserved.
// Use of this source code is governed by the MIT
// license, which can be found in the LICENSE file.

package jobs

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"reflect"
	"sync"
	"time"
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
		t1.setStatus(job, StatusQueued)
	} else {
		// Otherwise, set status to finished
		t1.setStatus(job, StatusFinished)
	}
	if err := t1.exec(); err != nil {
		// NOTE: panics will be caught by the recover statment above
		panic(err)
	}
}
