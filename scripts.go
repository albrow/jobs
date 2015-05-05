// Copyright 2015 Alex Browne.  All rights reserved.
// Use of this source code is governed by the MIT
// license, which can be found in the LICENSE file.

package jobs

import (
	"bytes"
	"github.com/garyburd/redigo/redis"
	"os"
	"path/filepath"
	"strings"
	"text/template"
	"time"
)

var (
	// scriptContext is a map which is passed in as the context to all lua script templates.
	// It holds the keys for all the different status sets, the names of the sets, and the keys
	// for other constant sets.
	scriptContext = map[string]string{
		"statusSaved":     string(StatusSaved),
		"statusQueued":    string(StatusQueued),
		"statusExecuting": string(StatusExecuting),
		"statusFinished":  string(StatusFinished),
		"statusFailed":    string(StatusFailed),
		"statusCancelled": string(StatusCancelled),
		"statusDestroyed": string(StatusDestroyed),
		"savedSet":        StatusSaved.key(),
		"queuedSet":       StatusQueued.key(),
		"executingSet":    StatusExecuting.key(),
		"finishedSet":     StatusFinished.key(),
		"failedSet":       StatusFailed.key(),
		"cancelledSet":    StatusCancelled.key(),
		"destroyedSet":    StatusDestroyed.key(),
		"timeIndexSet":    keys.jobsTimeIndex,
		"jobsTempSet":     keys.jobsTemp,
		"activePoolsSet":  keys.activePools,
	}
)

var (
	popNextJobsScript    *redis.Script
	retryOrFailJobScript *redis.Script
	setStatusScript      *redis.Script
	destroyJobScript     *redis.Script
	purgeStalePoolScript *redis.Script
	getJobsByIdsScript   *redis.Script
)

var (
	// Split GOPATH using os.PathListSeparator and choose first path in GOPATH as scripts path.
	// Otherwise you would receive an open file error if using multiple paths in your GOPATH.
	gopath      = strings.Split(os.Getenv("GOPATH"), string(os.PathListSeparator))[0]
	scriptsPath = filepath.Join(gopath, "src", "github.com", "albrow", "jobs", "scripts")
)

func init() {
	// Parse all the script templates and create redis.Script objects
	scriptsToParse := []struct {
		script   **redis.Script
		filename string
		keyCount int
	}{
		{
			script:   &popNextJobsScript,
			filename: "pop_next_jobs.lua",
			keyCount: 0,
		},
		{
			script:   &retryOrFailJobScript,
			filename: "retry_or_fail_job.lua",
			keyCount: 0,
		},
		{
			script:   &setStatusScript,
			filename: "set_job_status.lua",
			keyCount: 0,
		},
		{
			script:   &destroyJobScript,
			filename: "destroy_job.lua",
			keyCount: 0,
		},
		{
			script:   &purgeStalePoolScript,
			filename: "purge_stale_pool.lua",
			keyCount: 0,
		},
		{
			script:   &getJobsByIdsScript,
			filename: "get_jobs_by_ids.lua",
			keyCount: 1,
		},
	}
	for _, s := range scriptsToParse {
		// Parse the file corresponding to this script
		fullPath := filepath.Join(scriptsPath, s.filename)
		tmpl, err := template.ParseFiles(fullPath)
		if err != nil {
			panic(err)
		}
		// Execute the template and pass in the scriptContext
		buf := bytes.NewBuffer([]byte{})
		if err := tmpl.Execute(buf, scriptContext); err != nil {
			panic(err)
		}
		// Set the value of the script pointer
		(*s.script) = redis.NewScript(s.keyCount, buf.String())
	}
}

// popNextJobs is a small function wrapper around getAndMovesJobToExecutingScript.
// It offers some type safety and helps make sure the arguments you pass through to the are correct.
// The script will get the next n jobs from the queue that are ready based on their time parameter.
func (t *transaction) popNextJobs(n int, poolId string, handler replyHandler) {
	currentTime := time.Now().UTC().UnixNano()
	t.script(popNextJobsScript, redis.Args{n, currentTime, poolId}, handler)
}

// retryOrFailJob is a small function wrapper around retryOrFailJobScript.
// It offers some type safety and helps make sure the arguments you pass through to the are correct.
// The script will either mark the job as failed or queue it for retry depending on the number of
// retries left.
func (t *transaction) retryOrFailJob(job *Job, handler replyHandler) {
	t.script(retryOrFailJobScript, redis.Args{job.id}, handler)
}

// setStatus is a small function wrapper around setStatusScript.
// It offers some type safety and helps make sure the arguments you pass through to the are correct.
// The script will atomically update the status of the job, removing it from its old status set and
// adding it to the new one.
func (t *transaction) setStatus(job *Job, status Status) {
	t.script(setStatusScript, redis.Args{job.id, string(status)}, nil)
}

// destroyJob is a small function wrapper around destroyJobScript.
// It offers some type safety and helps make sure the arguments you pass through to the are correct.
// The script will remove all records associated with job from the database.
func (t *transaction) destroyJob(job *Job) {
	t.script(destroyJobScript, redis.Args{job.id}, nil)
}

// purgeStalePool is a small function wrapper around purgeStalePoolScript.
// It offers some type safety and helps make sure the arguments you pass through to the are correct.
// The script will remove the stale pool from the active pools set, and then requeue any jobs associated
// with the stale pool that are stuck in the executing set.
func (t *transaction) purgeStalePool(poolId string) {
	t.script(purgeStalePoolScript, redis.Args{poolId}, nil)
}

// getJobsByIds is a small function wrapper around getJobsByIdsScript.
// It offers some type safety and helps make sure the arguments you pass through to the are correct.
// The script will return all the fields for jobs which are identified by ids in the given sorted set.
// You can use the handler to scan the jobs into a slice of jobs.
func (t *transaction) getJobsByIds(setKey string, handler replyHandler) {
	t.script(getJobsByIdsScript, redis.Args{setKey}, handler)
}
