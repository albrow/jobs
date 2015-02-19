-- Copyright 2015 Alex Browne.  All rights reserved.
-- Use of this source code is governed by the MIT
-- license, which can be found in the LICENSE file.

-- destroy_job is a lua script that takes the following arguments:
-- 	1) The id of the job to destroy
-- It then removes all traces of the job in the database by doing the following:
-- 	1) Removes the job from the status set (which it determines with an HGET call)
-- 	2) Removes the job from the time index
-- 	3) Removes the main hash for the job

-- Assign args to variables for easy reference
local jobId = ARGV[1]
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