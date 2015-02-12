package zazu

import (
	"reflect"
	"testing"
)

func TestJobSave(t *testing.T) {
	flushdb()

	// Create and save a test job
	job, err := createAndSaveTestJob()
	if err != nil {
		t.Error(err)
	}

	// Make sure the main hash was saved correctly
	assertJobFieldEquals(t, job, "data", job.data, nil)
	assertJobFieldEquals(t, job, "type", job.typ.name, stringConverter)
	assertJobFieldEquals(t, job, "time", job.time, int64Converter)
	assertJobFieldEquals(t, job, "priority", job.priority, intConverter)

	// Make sure the job status was correct
	assertJobStatusEquals(t, job, StatusSaved)
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
		flushdb()
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
	flushdb()
	job, err := createAndSaveTestJob()
	if err != nil {
		t.Errorf("Unexpected error: %s")
	}
	conn := redisPool.Get()
	defer conn.Close()
	replies, err := conn.Do("HGETALL", "jobs:"+job.id)
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
