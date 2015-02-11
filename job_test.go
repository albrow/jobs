package zazu

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"reflect"
	"testing"
	"time"
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

func createTestJob() (*Job, error) {
	// Register the "testJobType"
	jobTypeName := "testJobType"
	jobType, err := RegisterJobType(jobTypeName)
	if err != nil {
		if _, ok := err.(ErrorNameAlreadyRegistered); !ok {
			// If the name was already registered, that's fine.
			// We should return any other type of error
			return nil, err
		}
	}
	// Create and return a test job
	j := &Job{
		id:       "testJob",
		data:     []byte("testData"),
		typ:      jobType,
		time:     time.Now().UTC().Unix(),
		priority: 100,
	}
	return j, nil
}

func createAndSaveTestJob() (*Job, error) {
	j, err := createTestJob()
	if err != nil {
		return nil, err
	}
	if err := j.save(); err != nil {
		return nil, fmt.Errorf("Unexpected error in j.save(): %s", err.Error())
	}
	return j, nil
}

func assertJobFieldEquals(t *testing.T, j *Job, fieldName string, expected interface{}, converter replyConverter) {
	hashKey := fmt.Sprintf("jobs:%s", j.id)
	conn := redisPool.Get()
	defer conn.Close()
	got, err := conn.Do("HGET", hashKey, fieldName)
	if err != nil {
		t.Errorf("Unexpected error: %s", err.Error())
	}
	if converter != nil {
		got, err = converter(got)
		if err != nil {
			t.Errorf("Unexpected error in converter: %s", err.Error())
		}
	}
	if !reflect.DeepEqual(expected, got) {
		t.Errorf("job.%s was not saved correctly.\n\tExpected: %v\n\tBut got:  %v", fieldName, expected, got)
	}
}

type replyConverter func(interface{}) (interface{}, error)

var int64Converter replyConverter = func(in interface{}) (interface{}, error) {
	return redis.Int64(in, nil)
}

var intConverter replyConverter = func(in interface{}) (interface{}, error) {
	return redis.Int(in, nil)
}

var stringConverter replyConverter = func(in interface{}) (interface{}, error) {
	return redis.String(in, nil)
}

func assertJobInSet(t *testing.T, j *Job, setName string) {
	conn := redisPool.Get()
	defer conn.Close()
	gotIds, err := redis.Values(conn.Do("ZRANGEBYSCORE", setName, j.priority, j.priority))
	if err != nil {
		t.Errorf("Unexpected error: %s", err.Error())
	}
	for _, id := range gotIds {
		idBytes, ok := id.([]byte)
		if !ok {
			t.Errorf("Could not convert job id of type %T to string!", id)
		}
		if string(idBytes) == j.id {
			// We found the job we were looking for
			return
		}
	}
	// If we reached here, we did not find the job we were looking for
	t.Errorf("job:%s was not found in set %s", j.id, setName)
}

func assertJobNotInSet(t *testing.T, j *Job, setName string) {
	conn := redisPool.Get()
	defer conn.Close()
	gotIds, err := redis.Values(conn.Do("ZRANGEBYSCORE", setName, j.priority, j.priority))
	if err != nil {
		t.Errorf("Unexpected error: %s", err.Error())
	}
	for _, id := range gotIds {
		idBytes, ok := id.([]byte)
		if !ok {
			t.Errorf("Could not convert job id of type %T to string!", id)
		}
		if string(idBytes) == j.id {
			// We found the job, but it wasn't supposed to be here!
			t.Errorf("job:%s was found in set %s but expected it to be removed", j.id, setName)
		}
	}
}

func assertJobStatusEquals(t *testing.T, job *Job, expected JobStatus) {
	if job.status != expected {
		t.Errorf("Expected job status to be %s but got %s", expected, job.status)
	}
	for _, status := range possibleStatuses {
		setName := string("jobs:" + status)
		if status == expected {
			assertJobInSet(t, job, setName)
			assertJobFieldEquals(t, job, "status", string(status), stringConverter)
		} else {
			assertJobNotInSet(t, job, setName)
		}
	}
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
