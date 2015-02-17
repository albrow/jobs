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

// decrJobRetires is a lua script that takes the following arguments:
// 1) The key of the job for which to decrement retries.
// It first checks if the job has any retries remaining. If it does,
// then it decrements the number of retries and returns true to indicate
// that the job does have retries remaining. If it does not, i.e. if
// retries == 0, then it returns false.
var decrJobRetries = redis.NewScript(1, `
	-- Assign keys to variables for easy reference
	local jobKey = KEYS[1]
	-- Check how many retries remain
	local retries = redis.call('HGET', jobKey, 'retries')
	if retries == "0" then
		-- NOTE: 0 is used to represent false because apparently
		-- false gets converted to nil
		return 0
	else
		redis.call('HINCRBY', jobKey, 'retries', -1)
		-- NOTE: 1 is used to represent true (for consistency)
		return 1
	end
`)
