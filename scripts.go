package zazu

import (
	"bytes"
	"github.com/garyburd/redigo/redis"
	"os"
	"path/filepath"
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
	}
)

var (
	popNextJobsScript    *redis.Script
	retryOrFailJobScript *redis.Script
	setJobStatusScript   *redis.Script
	destroyJobScript     *redis.Script
)

var (
	scriptsPath = filepath.Join(os.Getenv("GOPATH"), "src", "github.com", "albrow", "zazu", "scripts")
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
			script:   &setJobStatusScript,
			filename: "set_job_status.lua",
			keyCount: 0,
		},
		{
			script:   &destroyJobScript,
			filename: "destroy_job.lua",
			keyCount: 0,
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
func (t *transaction) popNextJobs(n int, handler replyHandler) {
	currentTime := time.Now().UTC().UnixNano()
	t.script(popNextJobsScript, redis.Args{n, currentTime}, handler)
}

// retryOrFailJob is a small function wrapper around retryOrFailJobScript.
// It offers some type safety and helps make sure the arguments you pass through to the are correct.
// The script will either mark the job as failed or queue it for retry depending on the number of
// retries left.
func (t *transaction) retryOrFailJob(job *Job, handler replyHandler) {
	t.script(retryOrFailJobScript, redis.Args{job.id}, handler)
}

// setJobStatus is a small function wrapper around setJobStatusScript.
// It offers some type safety and helps make sure the arguments you pass through to the are correct.
// The script will atomically update the status of the job, removing it from its old status set and
// adding it to the new one.
func (t *transaction) setJobStatus(job *Job, status JobStatus) {
	t.script(setJobStatusScript, redis.Args{job.id, string(status)}, nil)
}

// destroyJob is a small function wrapper around destroyJobScript.
// It offers some type safety and helps make sure the arguments you pass through to the are correct.
// The script will remove all records associated with job from the database.
func (t *transaction) destroyJob(job *Job) {
	t.script(destroyJobScript, redis.Args{job.id}, nil)
}
