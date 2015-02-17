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
