package zazu

import (
	"github.com/garyburd/redigo/redis"
)

// getAndMoveJobsToExecuting is a lua script that takes the following arguments:
// 1) The key of the readyAndSorted set
// 2) The key of the queued status set
// 3) The key of the executing status set
// It then does the following:
// 1) Reads all the job ids from the readyAndSorted set in reverse order
// 2) Removes those job ids from the queued set
// 3) Adds those job ids to the executing set
// 4) Returns those job ids
var getAndMoveJobsToExecuting = redis.NewScript(3, `
	-- Assign keys to variables for easy reference
	local readyAndSortedSet = KEYS[1]
	local queuedSet = KEYS[2]
	local executingSet = KEYS[3]
	-- Get all job ids from the readyAndSortedSet
	local jobIds = redis.call('ZREVRANGE', readyAndSortedSet, 0, -1)
	if #jobIds > 0 then
		-- Add job ids to the executing set
		redis.call('ZUNIONSTORE', executingSet, 2, executingSet, readyAndSortedSet)
		-- Remove job ids from the queued set
		redis.call('ZREMRANGEBYRANK', queuedSet, -(#jobIds), -1)
	end
	return jobIds
`)

// retryOrFailJob is a lua script that takes the following arguments:
// 1) The key of the job to either retry or fail
// 2) The id of the job to either retry or fail
// 3) The priority of the job
// 4) The key for the executing status set
// 5) The key for the queued status set
// 6) The key for the failed status set
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
var retryOrFailJob = redis.NewScript(6, `
	-- Assign keys to variables for easy reference
	local jobKey = KEYS[1]
	local jobId = KEYS[2]
	local jobPriority = KEYS[3]
	local executingSet = KEYS[4]
	local queuedSet = KEYS[5]
	local failedSet = KEYS[6]
	-- Check how many retries remain
	local retries = redis.call('HGET', jobKey, 'retries')
	if retries == "0" then
		-- add the job to the failed set
		redis.call('ZADD', failedSet, jobPriority, jobId)
		-- remove the job from the executing set
		redis.call('ZREM', executingSet, jobId)
		-- Set the job status in the hash
		redis.call('HSET', jobKey, 'status', 'failed')
		-- Return false to indicate the job has not been queued for retry
		-- NOTE: 0 is used to represent false because apparently
		-- false gets converted to nil
		return 0
	else
		-- subtract 1 from the remaining retries
		redis.call('HINCRBY', jobKey, 'retries', -1)
		-- add the job to the queued set
		redis.call('ZADD', queuedSet, jobPriority, jobId)
		-- remove the job from the executing set
		redis.call('ZREM', executingSet, jobId)
		-- Set the job status in the hash
		redis.call('HSET', jobKey, 'status', 'queued')
		-- Return true to indicate the job has been queued for retry
		-- NOTE: 1 is used to represent true (for consistency)
		return 1
	end
`)
