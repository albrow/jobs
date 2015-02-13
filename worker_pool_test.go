package zazu

import (
	"reflect"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"
)

// TestNextJobs tests the getNextJobs function, which queries the database to find
// the next queued jobs, in order of their priority.
func TestGetNextJobs(t *testing.T) {
	flushdb()
	jobTypes = map[string]*JobType{}

	// Create a test job with high priority
	highPriorityJob, err := createTestJob()
	if err != nil {
		t.Errorf("Unexpected error creating test job: %s", err.Error())
	}
	highPriorityJob.priority = 1000
	highPriorityJob.id = "highPriorityJob"
	if err := highPriorityJob.save(); err != nil {
		t.Errorf("Unexpected error saving test job: %s", err.Error())
	}
	if err := highPriorityJob.Enqueue(); err != nil {
		t.Errorf("Unexpected error enqueuing test job: %s", err.Error())
	}

	// Create more tests with lower priorities
	for i := 0; i < 10; i++ {
		job, err := createTestJob()
		if err != nil {
			t.Errorf("Unexpected error creating test job: %s", err.Error())
		}
		job.priority = 100
		job.id = "lowPriorityJob" + strconv.Itoa(i)
		if err := job.save(); err != nil {
			t.Errorf("Unexpected error saving test job: %s", err.Error())
		}
		if err := job.Enqueue(); err != nil {
			t.Errorf("Unexpected error enqueuing test job: %s", err.Error())
		}
	}

	// Call getNextJobs with n = 1. We expect the one job returned to be the
	// highpriority one, but the status should now be executing
	jobs, err := getNextJobs(1)
	if err != nil {
		t.Errorf("Unexpected error from getNextJobs: %s", err.Error())
	}
	if len(jobs) != 1 {
		t.Errorf("Length of jobs was incorrect. Expected 1 but got %d", len(jobs))
	} else {
		gotJob := jobs[0]
		expectedJob := &Job{}
		(*expectedJob) = *highPriorityJob
		expectedJob.status = StatusExecuting
		if !reflect.DeepEqual(expectedJob, gotJob) {
			t.Errorf("Job returned by getNextJobs was incorrect.\n\tExpected: %+v\n\tBut got:  %+v", expectedJob, gotJob)
		}
	}
}

// TestJobsWithHigherPriorityExecutedFirst creates two sets of jobs: one with lower priorities
// and one with higher priorities. Then it starts the worker pool and runs for exactly one iteration.
// Then it makes sure that the jobs with higher priorities were executed, and the lower priority ones
// were not.
func TestJobsWithHigherPriorityExecutedFirst(t *testing.T) {
	flushdb()
	jobTypes = map[string]*JobType{}

	// Register some jobs which will simply set one of the values in data
	data := make([]string, 8)
	setStringJob, err := RegisterJobType("setString", func(i int) {
		data[i] = "ok"
	})
	if err != nil {
		t.Errorf("Unexpected error in RegisterJobType: %s", err.Error())
	}

	// Queue up some jobs
	queuedJobs := make([]*Job, len(data))
	for i := 0; i < len(data); i++ {
		// Lower indexes have higher priority and should be completed first
		job, err := setStringJob.Enqueue(8-i, time.Now(), i)
		if err != nil {
			t.Errorf("Unexpected error in Enqueue: %s", err.Error())
		}
		queuedJobs[i] = job
	}

	// Start the pool with 4 workers
	runtime.GOMAXPROCS(4)
	NumWorkers = 4
	BatchSize = 4
	Pool.Start()

	// Immediately stop the pool to stop the workers from doing more jobs
	Pool.Close()

	// Wait for the workers to finish
	Pool.Wait()

	// Check that the first 4 values of data were set to "ok"
	// This would mean that the first 4 jobs (in order of priority)
	// were successfully executed.
	assertTestDataOk(t, data[:4])

	// Make sure all the other values of data are still blank
	assertTestDataBlank(t, data[4:])

	// Make sure the first four jobs we queued are marked as finished
	for _, job := range queuedJobs[0:4] {
		// Since we don't have a fresh copy, set the status manually. I.e. there is
		// a difference between the reference we have to the job and what actually exists
		// in the database. The database is what we care about.
		// assertJobStatusEquals will check that the job is correct in the database.
		job.status = StatusFinished
		assertJobStatusEquals(t, job, StatusFinished)
	}

	// Make sure the next four jobs we queued are marked as queued
	for _, job := range queuedJobs[4:] {
		// Since we don't have a fresh copy, set the status manually. I.e. there is
		// a difference between the reference we have to the job and what actually exists
		// in the database. The database is what we care about.
		// assertJobStatusEquals will check that the job is correct in the database.
		job.status = StatusQueued
		assertJobStatusEquals(t, job, StatusQueued)
	}
}

// TestJobsOnlyExecutedOnce creates a few jobs that increment a counter (each job
// has its own counter). Then it starts the pool and runs the query loop for at most two
// iterations. Then it checks that each job was executed only once by observing the counters.
func TestJobsOnlyExecutedOnce(t *testing.T) {
	flushdb()
	jobTypes = map[string]*JobType{}

	// Register some jobs which will simply increment one of the values in data
	data := make([]int, 4)
	waitForJobs := sync.WaitGroup{}
	incrementJob, err := RegisterJobType("increment", func(i int) {
		data[i] += 1
		waitForJobs.Done()
	})
	if err != nil {
		t.Errorf("Unexpected error in RegisterJobType: %s", err.Error())
	}

	// Queue up some jobs
	for i := 0; i < len(data); i++ {
		waitForJobs.Add(1)
		if _, err := incrementJob.Enqueue(100, time.Now(), i); err != nil {
			t.Errorf("Unexpected error in Enqueue: %s", err.Error())
		}
	}

	// Start the pool with 4 workers
	runtime.GOMAXPROCS(4)
	NumWorkers = 4
	BatchSize = 4
	Pool.Start()

	// Wait for the wait group, which tells us each job was executed at least once
	waitForJobs.Wait()
	// Close the pool, allowing for a max of one more iteration
	Pool.Close()
	// Wait for the workers to finish
	Pool.Wait()

	// Check that each value in data equals 1.
	// This would mean that each job was only executed once
	for i, datum := range data {
		if datum != 1 {
			t.Errorf(`Expected data[%d] to be 1 but got: %d`, i, datum)
		}
	}
}

// TestAllJobsExecuted creates many more jobs than workers. Then it starts
// the pool and continuously checks if every job was executed, it which case
// it exits successfully. If some of the jobs have not been executed after 1
// second, it breaks and reports an error. 1 second should be plenty of time
// to execute the jobs.
func TestAllJobsExecuted(t *testing.T) {
	flushdb()
	jobTypes = map[string]*JobType{}

	defer func() {
		// Close the pool and wait for workers to finish
		Pool.Close()
		Pool.Wait()
	}()

	// Register some jobs which will simply set one of the elements in
	// data to "ok"
	data := make([]string, 100)
	setStringJob, err := RegisterJobType("setString", func(i int) {
		data[i] = "ok"
	})
	if err != nil {
		t.Errorf("Unexpected error in RegisterJobType: %s", err.Error())
	}

	// Queue up some jobs
	for i := 0; i < len(data); i++ {
		if _, err := setStringJob.Enqueue(100, time.Now(), i); err != nil {
			t.Errorf("Unexpected error in Enqueue: %s", err.Error())
		}
	}

	// Start the pool with 4 workers
	runtime.GOMAXPROCS(4)
	NumWorkers = 4
	BatchSize = 4
	Pool.Start()

	// Continuously check the data every 10 milliseconds. Eventually
	// we hope to see that everything was set to "ok". If 1 second has
	// passed, assume something went wrong.
	timeout := time.After(1 * time.Second)
	interval := time.Tick(10 * time.Millisecond)
	remainingJobs := len(data)
	for {
		select {
		case <-timeout:
			// More than 1 second has passed. Assume something went wrong.
			t.Errorf("1 second passed and %d jobs were not executed.", remainingJobs)
			break
		case <-interval:
			// Count the number of elements in data that equal "ok".
			// Anything that doesn't equal ok represents a job that hasn't been executed yet
			remainingJobs = len(data)
			for _, datum := range data {
				if datum == "ok" {
					remainingJobs -= 1
				}
			}
			if remainingJobs == 0 {
				// Each item in data was set to "ok", so all the jobs were executed correctly.
				return
			}
		}
	}
}

// TestJobsAreNotExecutedUntilTime sets up a few jobs with a time parameter in the future
// Then it makes sure that those jobs are not executed until after that time.
func TestJobsAreNotExecutedUntilTime(t *testing.T) {
	flushdb()
	jobTypes = map[string]*JobType{}

	defer func() {
		// Close the pool and wait for workers to finish
		Pool.Close()
		Pool.Wait()
	}()

	// Register some jobs which will set one of the elements in data
	// For this test, we want to execute two jobs at a time, so we'll
	// use a waitgroup.
	data := make([]string, 4)
	setStringJob, err := RegisterJobType("setString", func(i int) {
		data[i] = "ok"
	})
	if err != nil {
		t.Errorf("Unexpected error in RegisterJobType: %s", err.Error())
	}

	// Queue up some jobs with a time parameter in the future
	currentTime := time.Now()
	timeDiff := 200 * time.Millisecond
	futureTime := currentTime.Add(timeDiff)
	for i := 0; i < len(data); i++ {
		if _, err := setStringJob.Enqueue(100, futureTime, i); err != nil {
			t.Errorf("Unexpected error in Enqueue: %s", err.Error())
		}
	}

	// Start the pool with 4 workers
	runtime.GOMAXPROCS(4)
	NumWorkers = 4
	BatchSize = 4
	Pool.Start()

	// Continuously check the data every 10 milliseconds. Eventually
	// we hope to see that everything was set to "ok". We will check that
	// this condition is only true after futureTime has been reached, since
	// the jobs should not be executed before then.
	timeout := time.After(1 * time.Second)
	interval := time.Tick(10 * time.Millisecond)
	remainingJobs := len(data)
	for {
		select {
		case <-timeout:
			// More than 1 second has passed. Assume something went wrong.
			t.Errorf("1 second passed and %d jobs were not executed.", remainingJobs)
			break
		case <-interval:
			// Count the number of elements in data that equal "ok".
			// Anything that doesn't equal ok represents a job that hasn't been executed yet
			remainingJobs = len(data)
			for _, datum := range data {
				if datum == "ok" {
					remainingJobs -= 1
				}
			}
			if remainingJobs == 0 {
				// Each item in data was set to "ok", so all the jobs were executed correctly.
				// Check that this happend after futureTime
				if time.Now().Before(futureTime) {
					t.Errorf("jobs were executed before their time parameter was reached.")
				}
				return
			}
		}
	}
}

// assertTestDataOk reports an error if any elements in data do not equal "ok". It is only used for
// tests in this file. Many of the tests use a slice of strings as data and queue up jobs to set one
// of the elements to "ok", so this makes checking them easier.
func assertTestDataOk(t *testing.T, data []string) {
	for i, datum := range data {
		if datum != "ok" {
			t.Errorf("Expected data[%d] to be \"ok\" but got: \"%s\"\ndata was: %v.", i, datum, data)
		}
	}
}

// assertTestDataBlank is like assertTestDataOk except it does the opposite. It reports an error if any
// of the elements in data were not blank.
func assertTestDataBlank(t *testing.T, data []string) {
	for i, datum := range data {
		if datum != "" {
			t.Errorf("Expected data[%d] to be \"\" but got: \"%s\"\ndata was: %v.", i, datum, data)
		}
	}
}
