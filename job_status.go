// Copyright 2015 Alex Browne.  All rights reserved.
// Use of this source code is governed by the MIT
// license, which can be found in the LICENSE file.

package jobs

import (
	"github.com/garyburd/redigo/redis"
)

// JobStatus represents the different statuses a job can have.
type JobStatus string

const (
	// StatusSaved is the status of any job that has been saved into the database but not yet queued
	StatusSaved JobStatus = "saved"
	// StatusQueued is the status of any job that has been queued for execution but not yet selected
	StatusQueued JobStatus = "queued"
	// StatusExecuting is the status of any job that has been selected for execution and is being delegated
	// to some worker and any job that is currently being executed by some worker.
	StatusExecuting JobStatus = "executing"
	// StatusFinished is the status of any job that has been successfully executed.
	StatusFinished JobStatus = "finished"
	// StatusFailed is the status of any job that failed to execute and for which there are no remaining retries.
	StatusFailed JobStatus = "failed"
	// StatusCancelled is the status of any job that was manually cancelled.
	StatusCancelled JobStatus = "cancelled"
	// StatusDestroyed is the status of any job that has been destroyed, i.e. completely removed
	// from the database.
	StatusDestroyed JobStatus = "destroyed"
)

// key returns the key used for the sorted set in redis which will hold
// all jobs with this status.
func (status JobStatus) key() string {
	return "jobs:" + string(status)
}

// Count returns the number of jobs that currently have the given status
// or an error if there was a problem connecting to the database.
func (status JobStatus) Count() (int, error) {
	conn := redisPool.Get()
	defer conn.Close()
	return redis.Int(conn.Do("ZCARD", status.key()))
}

// JobIds returns the ids of all jobs that have the given status, ordered by
// priority.
func (status JobStatus) JobIds() ([]string, error) {
	conn := redisPool.Get()
	defer conn.Close()
	return redis.Strings(conn.Do("ZREVRANGE", status.key(), 0, -1))
}

// possibleStatuses is simply an array of all the possible job statuses.
var possibleStatuses = []JobStatus{
	StatusSaved,
	StatusQueued,
	StatusExecuting,
	StatusFinished,
	StatusFailed,
	StatusCancelled,
	StatusDestroyed,
}
