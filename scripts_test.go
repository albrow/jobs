package zazu

import (
	"github.com/garyburd/redigo/redis"
	"reflect"
	"testing"
)

func TestGetAndMoveJobsToExecutingScript(t *testing.T) {
	testingSetUp()
	defer testingTeardown()

	// Set up the database
	tx0 := newTransaction()
	// One set will mimic the ready and sorted jobs
	jobsReadyAndSortedKey := jobsReadyAndSorted.generateKey()
	tx0.command("ZADD", redis.Args{jobsReadyAndSortedKey, 3, "three", 4, "four"}, nil)
	// One set will mimic the queued set
	tx0.command("ZADD", redis.Args{StatusQueued.key(), 1, "one", 2, "two", 3, "three", 4, "four"}, nil)
	// One set will mimic the executing set
	tx0.command("ZADD", redis.Args{StatusExecuting.key(), 5, "five"}, nil)
	if err := tx0.exec(); err != nil {
		t.Errorf("Unexpected error executing transaction: %s", err.Error())
	}

	// Start a new transaction and execute the script
	tx1 := newTransaction()
	gotIds := []string{}
	tx1.getAndMoveJobsToExecuting(jobsReadyAndSortedKey, newScanStringsHandler(&gotIds))
	if err := tx1.exec(); err != nil {
		t.Errorf("Unexpected error executing transaction: %s", err.Error())
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
}

func TestRetryOrFailJobScript(t *testing.T) {
	testingSetUp()
	defer testingTeardown()

	testJob, err := RegisterJobType("testJob", func() {})
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
			assertJobStatusEquals(t, tc.job, StatusFailed)
		} else {
			// We expect the job to be in the queued set because it was queued for retry
			assertJobStatusEquals(t, tc.job, StatusQueued)
		}
	}
}
