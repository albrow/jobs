package zazu

import (
	"errors"
	"reflect"
	"testing"
)

func TestJobSave(t *testing.T) {
	testingSetUp()
	defer testingTeardown()

	// Create and save a test job
	job, err := createTestJob()
	if err != nil {
		t.Error(err)
	}
	job.started = 1
	job.finished = 5
	job.freq = 10
	job.retries = 3
	if err := job.save(); err != nil {
		t.Errorf("Unexpected error saving job: %s", err.Error())
	}

	// Make sure the main hash was saved correctly
	assertJobFieldEquals(t, job, "data", job.data, nil)
	assertJobFieldEquals(t, job, "type", job.typ.name, stringConverter)
	assertJobFieldEquals(t, job, "time", job.time, int64Converter)
	assertJobFieldEquals(t, job, "freq", job.freq, int64Converter)
	assertJobFieldEquals(t, job, "priority", job.priority, intConverter)
	assertJobFieldEquals(t, job, "started", job.started, int64Converter)
	assertJobFieldEquals(t, job, "finished", job.finished, int64Converter)
	assertJobFieldEquals(t, job, "retries", job.retries, intConverter)

	// Make sure the job status was correct
	assertJobStatusEquals(t, job, StatusSaved)

	// Make sure the job was indexed by its time correctly
	assertJobInTimeIndex(t, job)
}

func TestJobRefresh(t *testing.T) {
	testingSetUp()
	defer testingTeardown()

	// Create and save a job
	job, err := createAndSaveTestJob()
	if err != nil {
		t.Errorf("Unexpected error: %s")
	}

	// Get a copy of that job directly from database
	jobCopy := &Job{}
	tx := newTransaction()
	tx.scanJobById(job.id, jobCopy)
	if err := tx.exec(); err != nil {
		t.Errorf("Unexpected error in tx.exec(): %s", err.Error())
	}

	// Modify and save the copy
	newPriority := jobCopy.priority + 100
	jobCopy.priority = newPriority
	if err := jobCopy.save(); err != nil {
		t.Errorf("Unexpected error in jobCopy.save(): %s", err.Error())
	}

	// Refresh the original job
	if err := job.Refresh(); err != nil {
		t.Errorf("Unexpected error in job.Refresh(): %s", err.Error())
	}

	// Now the original and the copy should b equal
	if !reflect.DeepEqual(job, jobCopy) {
		t.Errorf("Expected job to equal jobCopy but it did not.\n\tExpected %+v\n\tBut got  %+v", jobCopy, job)
	}
}

func TestJobEnqueue(t *testing.T) {
	// Run through a set of possible state paths and make sure the result is
	// always what we expect
	statePaths := []statePath{
		{
			steps: []func(*Job) error{
				// Just call Enqueue after creating a new job
				enqueueJob,
			},
			expected: StatusQueued,
		},
		{
			steps: []func(*Job) error{
				// Call Enqueue, then Cancel, then Enqueue again
				enqueueJob,
				cancelJob,
				enqueueJob,
			},
			expected: StatusQueued,
		},
	}
	testJobStatePaths(t, statePaths)
}

func TestJobCancel(t *testing.T) {
	// Run through a set of possible state paths and make sure the result is
	// always what we expect
	statePaths := []statePath{
		{
			steps: []func(*Job) error{
				// Just call Cancel after creating a new job
				cancelJob,
			},
			expected: StatusCancelled,
		},
		{
			steps: []func(*Job) error{
				// Call Cancel, then Enqueue, then Cancel again
				cancelJob,
				enqueueJob,
				cancelJob,
			},
			expected: StatusCancelled,
		},
	}
	testJobStatePaths(t, statePaths)
}

func TestJobDestroy(t *testing.T) {
	// Run through a set of possible state paths and make sure the result is
	// always what we expect
	statePaths := []statePath{
		{
			steps: []func(*Job) error{
				// Just call Destroy after creating a new job
				destroyJob,
			},
			expected: StatusDestroyed,
		},
		{
			steps: []func(*Job) error{
				// Call Destroy after cancel
				cancelJob,
				destroyJob,
			},
			expected: StatusDestroyed,
		},
		{
			steps: []func(*Job) error{
				// Call Destroy after enqueue
				enqueueJob,
				destroyJob,
			},
			expected: StatusDestroyed,
		},
		{
			steps: []func(*Job) error{
				// Call Destroy after enqueue then cancel
				enqueueJob,
				cancelJob,
				destroyJob,
			},
			expected: StatusDestroyed,
		},
	}
	testJobStatePaths(t, statePaths)
}

func TestJobSetError(t *testing.T) {
	job, err := createAndSaveTestJob()
	if err != nil {
		t.Errorf("Unexpected error in createAndSaveTestJob(): %s", err.Error())
	}
	testErr := errors.New("Test Error")
	if err := job.setError(testErr); err != nil {
		t.Errorf("Unexpected error in job.setError(): %s", err.Error())
	}
	assertJobFieldEquals(t, job, "error", testErr.Error(), stringConverter)
}

// statePath represents a path through which a job can travel, where each step
// potentially modifies its status. expected is what we expect the job status
// to be after the last step.
type statePath struct {
	steps    []func(*Job) error
	expected JobStatus
}

var (
	// Some easy to use step functions
	enqueueJob = func(j *Job) error {
		return j.Enqueue()
	}
	cancelJob = func(j *Job) error {
		return j.Cancel()
	}
	destroyJob = func(j *Job) error {
		return j.Destroy()
	}
)

// testJobStatePaths will for each statePath run through the steps, make sure
// there were no errors at any step, and check that the status after the last
// step is what we expect.
func testJobStatePaths(t *testing.T, statePaths []statePath) {
	for _, statePath := range statePaths {
		testingSetUp()
		defer testingTeardown()
		// Create a new test job
		job, err := createAndSaveTestJob()
		if err != nil {
			t.Error(err)
		}
		// Run the job through each step
		for _, step := range statePath.steps {
			if err := step(job); err != nil {
				t.Errorf("Unexpected error in step %v: %s", step, err)
			}
		}
		assertJobStatusEquals(t, job, statePath.expected)
	}
}

func TestJobScanReply(t *testing.T) {
	testingSetUp()
	defer testingTeardown()
	job, err := createAndSaveTestJob()
	if err != nil {
		t.Errorf("Unexpected error: %s")
	}
	conn := redisPool.Get()
	defer conn.Close()
	replies, err := conn.Do("HGETALL", job.key())
	if err != nil {
		t.Errorf("Unexpected error in HGETALL: %s", err.Error())
	}
	jobCopy := &Job{id: job.id}
	if err := scanJob(replies, jobCopy); err != nil {
		t.Errorf("Unexpected error: %s", err)
	}
	if !reflect.DeepEqual(job, jobCopy) {
		t.Errorf("Result of scanJob was incorrect.\n\tExpected %+v\n\tbut got  %+v", job, jobCopy)
	}
}

func TestJobStatusCount(t *testing.T) {
	testingSetUp()
	defer testingTeardown()
	job, err := createAndSaveTestJob()
	if err != nil {
		t.Errorf("Unexpected error: %s")
	}
	for _, status := range possibleStatuses {
		if status == StatusDestroyed {
			// Skip this one, since destroying a job means erasing all records from the database
			continue
		}
		job.setStatus(status)
		count, err := status.Count()
		if err != nil {
			t.Errorf("Unexpected error in status.Count(): %s", err.Error())
		}
		if count != 1 {
			t.Errorf("Expected %s.Count() to return 1 after setting job status to %s, but got %d", status, status, count)
		}
	}
}
