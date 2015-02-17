package zazu

import (
	"bytes"
	"fmt"
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
// 1) The key of the job to either retry or fail
// 2) The id of the job to either retry or fail
// 3) The priority of the job
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
	local jobKey = KEYS[1]
	local jobId = KEYS[2]
	local jobPriority = KEYS[3]
	-- Check how many retries remain
	local retries = redis.call('HGET', jobKey, 'retries')
	if retries == "0" then
		-- add the job to the failed set
		redis.call('ZADD', '{{.failedSet}}', jobPriority, jobId)
		-- remove the job from the executing set
		redis.call('ZREM', '{{.executingSet}}', jobId)
		-- Set the job status in the hash
		redis.call('HSET', jobKey, 'status', '{{.statusFailed}}')
		-- Return false to indicate the job has not been queued for retry
		-- NOTE: 0 is used to represent false because apparently
		-- false gets converted to nil
		return 0
	else
		-- subtract 1 from the remaining retries
		redis.call('HINCRBY', jobKey, 'retries', -1)
		-- add the job to the queued set
		redis.call('ZADD', '{{.queuedSet}}', jobPriority, jobId)
		-- remove the job from the executing set
		redis.call('ZREM', '{{.executingSet}}', jobId)
		-- Set the job status in the hash
		redis.call('HSET', jobKey, 'status', '{{.statusQueued}}')
		-- Return true to indicate the job has been queued for retry
		-- NOTE: 1 is used to represent true (for consistency)
		return 1
	end
`))

var (
	getAndMoveJobsToExecutingScript *redis.Script
	retryOrFailJobScript            *redis.Script
)

func init() {
	// Set up the getAndMoveJobsToExecutingScript
	getAndMoveJobsToExecutingBuff := bytes.NewBuffer([]byte{})
	if err := getAndMoveJobsToExecutingTmpl.Execute(getAndMoveJobsToExecutingBuff, constantKeys); err != nil {
		panic(err)
	}
	fmt.Println(getAndMoveJobsToExecutingBuff.String())
	getAndMoveJobsToExecutingScript = redis.NewScript(1, getAndMoveJobsToExecutingBuff.String())

	// Set up the retryOrFailJobScript
	retryOrFailJobBuff := bytes.NewBuffer([]byte{})
	if err := retryOrFailJobTmpl.Execute(retryOrFailJobBuff, constantKeys); err != nil {
		panic(err)
	}
	retryOrFailJobScript = redis.NewScript(3, retryOrFailJobBuff.String())
}

// getAndMoveJobsToExecuting is a small function wrapper around getAndMovesJobToExecutingScript.
// It offers some type safety and helps make sure the arguments you pass through to the are correct.
func (t *transaction) getAndMoveJobsToExecuting(jobsReadyAndSortedKey string, handler replyHandler) {
	t.script(getAndMoveJobsToExecutingScript, redis.Args{jobsReadyAndSortedKey}, handler)
}

// retryOrFailJob is a small function wrapper around getAndMovesJobToExecutingScript.
// It offers some type safety and helps make sure the arguments you pass through to the are correct.
func (t *transaction) retryOrFailJob(job *Job, handler replyHandler) {
	t.script(retryOrFailJobScript, redis.Args{job.key(), job.id, job.priority}, handler)
}
