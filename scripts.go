package zazu

import (
	"bytes"
	"github.com/garyburd/redigo/redis"
	"text/template"
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
	}
)

// getAndMoveJobsToExecutingTmpl represents a lua script that takes the following arguments:
// 1) The key of the readyAndSorted set
// It then does the following:
// 1) Reads all the job ids from the readyAndSorted set in reverse order
// 2) Removes those job ids from the queued set
// 3) Adds those job ids to the executing set
// 4) Returns those job ids
var getAndMoveJobsToExecutingTmpl = template.Must(template.New("getAndMoveJobsToExecutingScript").Parse(`
	-- Assign keys to variables for easy reference
	local readyAndSortedSet = KEYS[1]
	-- Get all job ids from the readyAndSortedSet
	local jobIds = redis.call('ZREVRANGE', readyAndSortedSet, 0, -1)
	if #jobIds > 0 then
		-- Add job ids to the executing set
		redis.call('ZUNIONSTORE', '{{.executingSet}}', 2, '{{.executingSet}}', readyAndSortedSet)
		-- Remove job ids from the queued set
		redis.call('ZREMRANGEBYRANK', '{{.queuedSet}}', -(#jobIds), -1)
	end
	return jobIds
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
	getAndMoveJobsToExecutingScript *redis.Script
	retryOrFailJobScript            *redis.Script
	setJobStatusScript              *redis.Script
	destroyJobScript                *redis.Script
)

func init() {
	// Set up the getAndMoveJobsToExecutingScript
	getAndMoveJobsToExecutingBuff := bytes.NewBuffer([]byte{})
	if err := getAndMoveJobsToExecutingTmpl.Execute(getAndMoveJobsToExecutingBuff, constantKeys); err != nil {
		panic(err)
	}
	getAndMoveJobsToExecutingScript = redis.NewScript(1, getAndMoveJobsToExecutingBuff.String())

	// Set up the retryOrFailJobScript
	retryOrFailJobBuff := bytes.NewBuffer([]byte{})
	if err := retryOrFailJobTmpl.Execute(retryOrFailJobBuff, constantKeys); err != nil {
		panic(err)
	}
	retryOrFailJobScript = redis.NewScript(1, retryOrFailJobBuff.String())

	// Set up the setJobStatusScript
	setJobStatusBuff := bytes.NewBuffer([]byte{})
	if err := setJobStatusTmpl.Execute(setJobStatusBuff, constantKeys); err != nil {
		panic(err)
	}
	setJobStatusScript = redis.NewScript(2, setJobStatusBuff.String())

	// Set up the destroyJobScript
	destroyJobBuff := bytes.NewBuffer([]byte{})
	if err := destroyJobTmpl.Execute(destroyJobBuff, constantKeys); err != nil {
		panic(err)
	}
	destroyJobScript = redis.NewScript(1, destroyJobBuff.String())
}

// getAndMoveJobsToExecuting is a small function wrapper around getAndMovesJobToExecutingScript.
// It offers some type safety and helps make sure the arguments you pass through to the are correct.
func (t *transaction) getAndMoveJobsToExecuting(jobsReadyAndSortedKey string, handler replyHandler) {
	t.script(getAndMoveJobsToExecutingScript, redis.Args{jobsReadyAndSortedKey}, handler)
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
