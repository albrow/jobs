package zazu

import (
	"bytes"
	"github.com/garyburd/redigo/redis"
	"text/template"
	"time"
)

var (
	constantKeys = map[string]string{
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

// popNextJobsTmpl represents a lua script that takes the following arguments:
// 1) The maximum number of jobs to pop and return
// 2) The current unix time UTC with nanosecond precision
// The script gets the next available jobs from the queued set which are
// ready based on their time parameter. Then it adds those jobs to the
// executing set, sets their status to executing, and removes them from the
// queued set. It returns an array of arrays where each element contains the
// fields for a particular job.
// Here's an example response:
//	[
//		[
//			"id", "afj9afjpa30",
//			"data", [34, 67, 34, 23, 56, 67, 78, 79],
//			"type", "emailJob",
//			"time", 1234567,
//			"freq", 0,
//			"priority", 100,
//			"retries", 0,
//			"status", "executing",
//			"started", 0,
//			"finished", 0,
//		],
//		[
//			"id", "E8v2ovkdaIw",
//			"data", [46, 43, 12, 08, 34, 45, 57, 43],
//			"type", "emailJob",
//			"time", 1234568,
//			"freq", 0,
//			"priority", 100,
//			"retries", 0,
//			"status", "executing",
//			"started", 0,
//			"finished", 0,
//		]
//	]
var popNextJobsTmpl = template.Must(template.New("popNextJobsScript").Parse(`
	-- Assign keys to variables for easy reference
	local n = ARGV[1]
	local currentTime = ARGV[2]
	-- Copy the time index set to a new temporary set
	redis.call('ZUNIONSTORE', '{{.jobsTempSet}}', 1, '{{.timeIndexSet}}')
	-- Trim the new temporary set we just created to leave only the jobs which have a time
	-- parameter in the past
	redis.call('ZREMRANGEBYSCORE', '{{.jobsTempSet}}', currentTime, '+inf')
	-- Intersect the jobs which are ready based on their time with those in the
	-- queued set. Use the weights parameter to set the scores entirely based on the
	-- queued set, effectively sorting the jobs by priority. Store the results in the
	-- temporary set.
	redis.call('ZINTERSTORE', '{{.jobsTempSet}}', 2, '{{.queuedSet}}', '{{.jobsTempSet}}', 'WEIGHTS', 1, 0)
	-- Trim the temp set, so it contains only the first n jobs ordered by
	-- priority
	redis.call('ZREMRANGEBYRANK', '{{.jobsTempSet}}', 0, -n - 1)
	-- Get all job ids from the temp set
	local jobIds = redis.call('ZREVRANGE', '{{.jobsTempSet}}', 0, -1)
	if #jobIds > 0 then
		-- Add job ids to the executing set
		redis.call('ZUNIONSTORE', '{{.executingSet}}', 2, '{{.executingSet}}', '{{.jobsTempSet}}')
		-- Remove job ids from the queued set
		redis.call('ZREMRANGEBYRANK', '{{.queuedSet}}', -(#jobIds), -1)
		-- Now we are ready to construct our response.
		local allJobs = {}
		for i, jobId in ipairs(jobIds) do
			local jobKey = 'jobs:' .. jobId
			-- Set the job status to executing
			redis.call('HSET', jobKey, 'status', '{{.statusExecuting}}')
			-- Get the fields from its main hash
			local jobFields = redis.call('HGETALL', jobKey)
			-- Add the id itself to the fields
			jobFields[#jobFields+1] = 'id'
			jobFields[#jobFields+1] = jobId
			-- Add the field values to allJobs
			allJobs[#allJobs+1] = jobFields
		end
		-- Return all the fields for all the jobs
		return allJobs
	else
		-- There were no jobs in the final resulting set. Return an empty table.
		return {}
	end
`))

// retryOrFailJobTmpl represents a lua script that takes the following arguments:
// 1) The id of the job to either retry or fail
// It first checks if the job has any retries remaining. If it does,
// then it:
// 	1) Decrements the number of retries for the given job
// 	2) Adds the job to the queued set
//		3) Removes the job from the executing set
// 	4) Returns true
// If the job has no retries remaining then it:
// 	1) Adds the job to the failed set
//		3) Removes the job from the executing set
// 	2) Returns false
var retryOrFailJobTmpl = template.Must(template.New("retryOrFailJobScript").Parse(`
	-- Assign keys to variables for easy reference
	local jobId = KEYS[1]
	local jobKey = 'jobs:' .. jobId
	-- Check how many retries remain
	local retries = redis.call('HGET', jobKey, 'retries')
	local newStatus = ''
	if retries == '0' then
		-- newStatus should be failed because there are no retries left
		newStatus = '{{.statusFailed}}'
	else
		-- subtract 1 from the remaining retries
		redis.call('HINCRBY', jobKey, 'retries', -1)
		-- newStatus should be queued, so the job will be retried
		newStatus = '{{.statusQueued}}'
	end
	-- Get the job priority (used as score)
	local jobPriority = redis.call('HGET', jobKey, 'priority')
	-- Add the job to the appropriate new set
	local newStatusSet = 'jobs:' .. newStatus
	redis.call('ZADD', newStatusSet, jobPriority, jobId)	
	-- Remove the job from the old status set
	local oldStatus = redis.call('HGET', jobKey, 'status')
	if ((oldStatus ~= '') and (oldStatus ~= newStatus)) then
		local oldStatusSet = 'jobs:' .. oldStatus
		redis.call('ZREM', oldStatusSet, jobId)
	end
	-- Set the job status in the hash
	redis.call('HSET', jobKey, 'status', newStatus)
	if retries == '0' then
		-- Return false to indicate the job has not been queued for retry
		-- NOTE: 0 is used to represent false because apparently
		-- false gets converted to nil
		return 0
	else
		-- Return true to indicate the job has been queued for retry
		-- NOTE: 1 is used to represent true (for consistency)
		return 1
	end
`))

// setJobStatusTmpl represents a lua script that takes the following arguments:
// 1) The id of the job
// 2) The new status (e.g. "queued")
// It then does the following:
// 1) Adds the job to the new status set
// 2) Removes the job from the old status set (which it gets with an HGET call)
// 3) Sets the 'status' field in the main hash for the job
var setJobStatusTmpl = template.Must(template.New("setJobStatusScript").Parse(`
	-- Assign keys to variables for easy reference
	local jobId = KEYS[1]
	local newStatus = KEYS[2]
	local jobKey = 'jobs:' .. jobId
	local newStatusSet = 'jobs:' .. newStatus
	-- Add the job to the new status set
	local jobPriority = redis.call('HGET', jobKey, 'priority')
	redis.call('ZADD', newStatusSet, jobPriority, jobId)
	-- Remove the job from the old status set
	local oldStatus = redis.call('HGET', jobKey, 'status')
	if ((oldStatus ~= '') and (oldStatus ~= newStatus)) then
		local oldStatusSet = 'jobs:' .. oldStatus
		redis.call('ZREM', oldStatusSet, jobId)
	end
	-- Set the status field
	redis.call('HSET', jobKey, 'status', newStatus)
`))

// destroyJobTmpl represents a lua script that takes the following arguments:
// 1) The id of the job to destroy
// It then removes all traces of the job in the database by doing the following:
// 1) Removes the job from the status set (which it determines with an HGET call)
// 2) Removes the job from the time index
// 3) Removes the main hash for the job
var destroyJobTmpl = template.Must(template.New("destroyJobScript").Parse(`
	-- Assign keys to variables for easy reference
	local jobId = KEYS[1]
	local jobKey = 'jobs:' .. jobId
	-- Remove the job from the status set
	local jobStatus = redis.call('HGET', jobKey, 'status')
	if jobStatus ~= '' then
		local statusSet = 'jobs:' .. jobStatus
		redis.call('ZREM', statusSet, jobId)
	end
	-- Remove the job from the time index
	redis.call('ZREM', '{{.timeIndexSet}}', jobId)
	-- Remove the main hash for the job
	redis.call('DEL', jobKey)
`))

var (
	popNextJobsScript    *redis.Script
	retryOrFailJobScript *redis.Script
	setJobStatusScript   *redis.Script
	destroyJobScript     *redis.Script
)

func init() {
	// Parse all the script templates and create redis.Script objects
	scriptsToParse := []struct {
		script   **redis.Script
		tmpl     *template.Template
		keyCount int
	}{
		{
			script:   &popNextJobsScript,
			tmpl:     popNextJobsTmpl,
			keyCount: 0,
		},
		{
			script:   &retryOrFailJobScript,
			tmpl:     retryOrFailJobTmpl,
			keyCount: 1,
		},
		{
			script:   &setJobStatusScript,
			tmpl:     setJobStatusTmpl,
			keyCount: 2,
		},
		{
			script:   &destroyJobScript,
			tmpl:     destroyJobTmpl,
			keyCount: 1,
		},
	}

	for _, s := range scriptsToParse {
		buf := bytes.NewBuffer([]byte{})
		if err := s.tmpl.Execute(buf, constantKeys); err != nil {
			panic(err)
		}
		(*s.script) = redis.NewScript(s.keyCount, buf.String())
	}
}

// popNextJobs is a small function wrapper around getAndMovesJobToExecutingScript.
// It offers some type safety and helps make sure the arguments you pass through to the are correct.
func (t *transaction) popNextJobs(n int, handler replyHandler) {
	currentTime := time.Now().UTC().UnixNano()
	t.script(popNextJobsScript, redis.Args{n, currentTime}, handler)
}

// retryOrFailJob is a small function wrapper around retryOrFailJobScript.
// It offers some type safety and helps make sure the arguments you pass through to the are correct.
func (t *transaction) retryOrFailJob(job *Job, handler replyHandler) {
	t.script(retryOrFailJobScript, redis.Args{job.id}, handler)
}

// setJobStatus is a small function wrapper around setJobStatusScript.
// It offers some type safety and helps make sure the arguments you pass through to the are correct.
func (t *transaction) setJobStatus(job *Job, status JobStatus) {
	t.script(setJobStatusScript, redis.Args{job.id, string(status)}, nil)
}

// destroyJob is a small function wrapper around destroyJobScript.
// It offers some type safety and helps make sure the arguments you pass through to the are correct.
func (t *transaction) destroyJob(job *Job) {
	t.script(destroyJobScript, redis.Args{job.id}, nil)
}
