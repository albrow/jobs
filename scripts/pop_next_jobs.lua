-- pop_next_jobs represents a lua script that takes the following arguments:
-- 	1) The maximum number of jobs to pop and return
-- 	2) The current unix time UTC with nanosecond precision
-- The script gets the next available jobs from the queued set which are
-- ready based on their time parameter. Then it adds those jobs to the
-- executing set, sets their status to executing, and removes them from the
-- queued set. It returns an array of arrays where each element contains the
-- fields for a particular job.
-- Here's an example response:
-- [
-- 	[
-- 		"id", "afj9afjpa30",
-- 		"data", [34, 67, 34, 23, 56, 67, 78, 79],
-- 		"type", "emailJob",
-- 		"time", 1234567,
-- 		"freq", 0,
-- 		"priority", 100,
-- 		"retries", 0,
-- 		"status", "executing",
-- 		"started", 0,
-- 		"finished", 0,
-- 	],
-- 	[
-- 		"id", "E8v2ovkdaIw",
-- 		"data", [46, 43, 12, 08, 34, 45, 57, 43],
-- 		"type", "emailJob",
-- 		"time", 1234568,
-- 		"freq", 0,
-- 		"priority", 100,
-- 		"retries", 0,
-- 		"status", "executing",
-- 		"started", 0,
-- 		"finished", 0,
-- 	]
-- ]

-- Assign args to variables for easy reference
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