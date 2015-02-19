-- set_job_status is a lua script that takes the following arguments:
-- 	1) The id of the job
-- 	2) The new status (e.g. "queued")
-- It then does the following:
-- 	1) Adds the job to the new status set
-- 	2) Removes the job from the old status set (which it gets with an HGET call)
-- 	3) Sets the 'status' field in the main hash for the job
	
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
