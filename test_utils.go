package zazu

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"reflect"
	"testing"
	"time"
)

func flushdb() {
	conn := redisPool.Get()
	if _, err := conn.Do("FLUSHDB"); err != nil {
		panic(err)
	}
}

func createTestJob() (*Job, error) {
	// Register the "testJobType"
	jobTypeName := "testJobType"
	jobType, err := RegisterJobType(jobTypeName, func() {})
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

var (
	int64Converter replyConverter = func(in interface{}) (interface{}, error) {
		return redis.Int64(in, nil)
	}
	intConverter replyConverter = func(in interface{}) (interface{}, error) {
		return redis.Int(in, nil)
	}
	stringConverter replyConverter = func(in interface{}) (interface{}, error) {
		return redis.String(in, nil)
	}
	bytesConverter replyConverter = func(in interface{}) (interface{}, error) {
		return redis.Bytes(in, nil)
	}
)

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
	if expected == StatusDestroyed {
		// If the status is destroyed, we don't expect the job to be in the database
		// anymore.
		assertJobDestroyed(t, job)
	} else {
		// For every status other status, we expect the job to be in the database
		for _, status := range possibleStatuses {
			setName := string("jobs:" + status)
			if status == expected {
				// Make sure the job hash has the correct status
				assertJobInSet(t, job, setName)
				// Make sure the job is in the correct set
				assertJobFieldEquals(t, job, "status", string(status), stringConverter)
			} else {
				// Make sure the job is not in any other set
				assertJobNotInSet(t, job, setName)
			}
		}
	}
}

func assertJobDestroyed(t *testing.T, job *Job) {
	// Make sure the main hash is gone
	assertKeyNotExists(t, "jobs:"+job.id)
	for _, status := range possibleStatuses {
		setName := string("jobs:" + status)
		assertJobNotInSet(t, job, setName)
	}
}

func assertKeyExists(t *testing.T, key string) {
	conn := redisPool.Get()
	defer conn.Close()
	if exists, err := redis.Bool(conn.Do("EXISTS", key)); err != nil {
		t.Errorf("Unexpected error in EXISTS: %s", err.Error())
	} else if !exists {
		t.Errorf("Expected key %s to exist, but it did not.", key)
	}
}

func assertKeyNotExists(t *testing.T, key string) {
	conn := redisPool.Get()
	defer conn.Close()
	if exists, err := redis.Bool(conn.Do("EXISTS", key)); err != nil {
		t.Errorf("Unexpected error in EXISTS: %s", err.Error())
	} else if exists {
		t.Errorf("Expected key %s to not exist, but it did exist.", key)
	}
}
