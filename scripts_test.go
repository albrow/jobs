// Copyright 2015 Alex Browne.  All rights reserved.
// Use of this source code is governed by the MIT
// license, which can be found in the LICENSE file.

package zazu

import (
	"github.com/garyburd/redigo/redis"
	"reflect"
	"testing"
	"time"
)

func TestPopNextJobsScript(t *testing.T) {
	testingSetUp()
	defer testingTeardown()

	// Set up some time parameters
	pastTime := time.Now().Add(-10 * time.Millisecond).UTC().UnixNano()

	// Set up the database
	tx0 := newTransaction()
	// One set will mimic the ready and sorted jobs
	tx0.command("ZADD", redis.Args{keys.jobsTimeIndex, pastTime, "three", pastTime, "four"}, nil)
	// One set will mimic the queued set
	tx0.command("ZADD", redis.Args{StatusQueued.key(), 1, "one", 2, "two", 3, "three", 4, "four"}, nil)
	// One set will mimic the executing set
	tx0.command("ZADD", redis.Args{StatusExecuting.key(), 5, "five"}, nil)
	if err := tx0.exec(); err != nil {
		t.Errorf("Unexpected error executing transaction: %s", err.Error())
	}

	// Start a new transaction and execute the script
	tx1 := newTransaction()
	gotJobs := []*Job{}
	testPoolId := "testPool"
	tx1.popNextJobs(2, testPoolId, newScanJobsHandler(&gotJobs))
	if err := tx1.exec(); err != nil {
		t.Errorf("Unexpected error executing transaction: %s", err.Error())
	}

	gotIds := []string{}
	for _, job := range gotJobs {
		gotIds = append(gotIds, job.id)
	}

	// Check the results
	expectedIds := []string{"four", "three"}
	if !reflect.DeepEqual(expectedIds, gotIds) {
		t.Errorf("Ids returned by script were incorrect.\n\tExpected: %v\n\tBut got:  %v", expectedIds, gotIds)
	}
	conn := redisPool.Get()
	defer conn.Close()
	expectedExecuting := []string{"five", "four", "three"}
	gotExecuting, err := redis.Strings(conn.Do("ZREVRANGE", StatusExecuting.key(), 0, -1))
	if err != nil {
		t.Errorf("Unexpected error in ZREVRANGE: %s", err.Error())
	}
	if !reflect.DeepEqual(expectedExecuting, gotExecuting) {
		t.Errorf("Ids in the executing set were incorrect.\n\tExpected: %v\n\tBut got:  %v", expectedExecuting, gotExecuting)
	}
	expectedQueued := []string{"two", "one"}
	gotQueued, err := redis.Strings(conn.Do("ZREVRANGE", StatusQueued.key(), 0, -1))
	if err != nil {
		t.Errorf("Unexpected error in ZREVRANGE: %s", err.Error())
	}
	if !reflect.DeepEqual(expectedQueued, gotQueued) {
		t.Errorf("Ids in the queued set were incorrect.\n\tExpected: %v\n\tBut got:  %v", expectedQueued, gotQueued)
	}
	expectKeyNotExists(t, keys.jobsTemp)
}

func TestRetryOrFailJobScript(t *testing.T) {
	testingSetUp()
	defer testingTeardown()

	testJob, err := RegisterJobType("testJob", 0, func() {})
	if err != nil {
		t.Errorf("Unexpected error registering job type: %s", err.Error())
	}

	// We'll use table-driven tests here
	testCases := []struct {
		job             *Job
		expectedReturn  bool
		expectedRetries int
	}{
		{
			// One job will start with 2 retries remaining
			job:             &Job{typ: testJob, id: "retriesRemainingJob", retries: 2, status: StatusExecuting},
			expectedReturn:  true,
			expectedRetries: 1,
		},
		{
			// One job will start with 0 retries remaining
			job:             &Job{typ: testJob, id: "noRetriesJob", retries: 0, status: StatusExecuting},
			expectedReturn:  false,
			expectedRetries: 0,
		},
	}

	// We can test all of the cases in a single transaction
	tx := newTransaction()
	gotReturns := make([]bool, len(testCases))
	gotRetries := make([]int, len(testCases))
	for i, tc := range testCases {
		// Save the job
		tx.saveJob(tc.job)
		// Run the script and save the return value in a slice
		tx.retryOrFailJob(tc.job, newScanBoolHandler(&(gotReturns[i])))
		// Get the new number of retries from the database and save the value in a slice
		tx.command("HGET", redis.Args{tc.job.key(), "retries"}, newScanIntHandler(&(gotRetries[i])))
	}
	// Execute the transaction
	if err := tx.exec(); err != nil {
		t.Errorf("Unexpected error executing transaction: %s", err.Error())
	}

	// Iterate through test cases again and check the results
	for i, tc := range testCases {
		if gotRetries[i] != tc.expectedRetries {
			t.Errorf("Number of retries after executing script was incorrect for test case %d (job:%s). Expected %v but got %v", i, tc.job.id, tc.expectedRetries, gotRetries[i])
		}
		if gotReturns[i] != tc.expectedReturn {
			t.Errorf("Return value from script was incorrect for test case %d (job:%s). Expected %v but got %v", i, tc.job.id, tc.expectedReturn, gotReturns[i])
		}
		// Make sure the job was removed from the executing set and placed in the correct set
		if err := tc.job.Refresh(); err != nil {
			t.Errorf("Unexpected error in job.Refresh(): %s", err.Error())
		}
		if tc.expectedReturn == false {
			// We expect the job to be in the failed set because it had no retries left
			expectJobStatusEquals(t, tc.job, StatusFailed)
		} else {
			// We expect the job to be in the queued set because it was queued for retry
			expectJobStatusEquals(t, tc.job, StatusQueued)
		}
	}
}

func TestSetJobStatusScript(t *testing.T) {
	testingSetUp()
	defer testingTeardown()

	job, err := createAndSaveTestJob()
	if err != nil {
		t.Errorf("Unexpected error in createAndSaveTestJob(): %s", err.Error())
	}

	// For all possible statuses, execute the script and check that the job status was set correctly
	for _, status := range possibleStatuses {
		if status == StatusDestroyed {
			continue
		}
		tx := newTransaction()
		tx.setJobStatus(job, status)
		if err := tx.exec(); err != nil {
			t.Errorf("Unexpected error in tx.exec(): %s", err.Error())
		}
		if err := job.Refresh(); err != nil {
			t.Errorf("Unexpected error in job.Refresh(): %s", err.Error())
		}
		expectJobStatusEquals(t, job, status)
	}
}

func TestDestroyJobScript(t *testing.T) {
	testingSetUp()
	defer testingTeardown()

	job, err := createAndSaveTestJob()
	if err != nil {
		t.Errorf("Unexpected error in createAndSaveTestJob(): %s", err.Error())
	}

	// Execute the script to destroy the job
	tx := newTransaction()
	tx.destroyJob(job)
	if err := tx.exec(); err != nil {
		t.Error("Unexpected err in tx.exec(): %s", err.Error())
	}

	// Make sure the job was destroyed
	job.status = StatusDestroyed
	expectJobStatusEquals(t, job, StatusDestroyed)
}

func TestPurgeStalePoolScript(t *testing.T) {
	testingSetUp()
	defer testingTeardown()

	testJobType, err := RegisterJobType("testJobType", 0, func() {})
	if err != nil {
		t.Errorf("Unexpected error in RegisterJobType(): %s", err.Error())
	}

	// Set up the database. We'll put some jobs in the executing set with a stale poolId,
	// and some jobs with an active poolId.
	staleJobs := []*Job{}
	stalePoolId := "stalePool"
	for i := 0; i < 4; i++ {
		job := &Job{typ: testJobType, status: StatusExecuting, poolId: stalePoolId}
		if err := job.save(); err != nil {
			t.Errorf("Unexpected error in job.save(): %s", err.Error())
		}
		staleJobs = append(staleJobs, job)
	}
	activeJobs := []*Job{}
	activePoolId := "activePool"
	for i := 0; i < 4; i++ {
		job := &Job{typ: testJobType, status: StatusExecuting, poolId: activePoolId}
		if err := job.save(); err != nil {
			t.Errorf("Unexpected error in job.save(): %s", err.Error())
		}
		activeJobs = append(activeJobs, job)
	}

	// Add both pools to the set of active pools
	conn := redisPool.Get()
	defer conn.Close()
	if _, err := conn.Do("SADD", keys.activePools, stalePoolId, activePoolId); err != nil {
		t.Errorf("Unexpected error adding pools to set: %s", err)
	}

	// Execute the script to purge the stale pool
	tx := newTransaction()
	tx.purgeStalePool(stalePoolId)
	if err := tx.exec(); err != nil {
		t.Error("Unexpected err in tx.exec(): %s", err.Error())
	}

	// Check the result
	// The active pools set should contain only the activePoolId
	expectSetDoesNotContain(t, keys.activePools, stalePoolId)
	expectSetContains(t, keys.activePools, activePoolId)
	// All the active jobs should still be executing
	for _, job := range activeJobs {
		if err := job.Refresh(); err != nil {
			t.Errorf("Unexpected error in job.Refresh(): %s", err.Error())
		}
		expectJobStatusEquals(t, job, StatusExecuting)
	}
	// All the stale jobs should now be queued and have an empty poolId
	for _, job := range staleJobs {
		if err := job.Refresh(); err != nil {
			t.Errorf("Unexpected error in job.Refresh(): %s", err.Error())
		}
		expectJobStatusEquals(t, job, StatusQueued)
		expectJobFieldEquals(t, job, "poolId", "", stringConverter)
	}
}
