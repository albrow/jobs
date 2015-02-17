package zazu

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"reflect"
	"sync"
	"testing"
	"time"
)

var setUpOnce = sync.Once{}

func testingSetUp() {
	setUpOnce.Do(func() {
		// Use database 14 and a unix socket connection for testing
		Config.Db.Database = 14
		Config.Db.Address = "/tmp/redis.sock"
		Config.Db.Network = "unix"
	})
	// Clear out any old job types
	jobTypes = map[string]*JobType{}
}

func testingTeardown() {
	// Flush the database
	flushdb()
}

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
		time:     time.Now().UTC().UnixNano(),
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

func assertJobFieldEquals(t *testing.T, job *Job, fieldName string, expected interface{}, converter replyConverter) {
	conn := redisPool.Get()
	defer conn.Close()
	got, err := conn.Do("HGET", job.key(), fieldName)
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

func assertJobInStatusSet(t *testing.T, j *Job, status JobStatus) {
	conn := redisPool.Get()
	defer conn.Close()
	gotIds, err := redis.Strings(conn.Do("ZRANGEBYSCORE", status.key(), j.priority, j.priority))
	if err != nil {
		t.Errorf("Unexpected error: %s", err.Error())
	}
	for _, id := range gotIds {
		if id == j.id {
			// We found the job we were looking for
			return
		}
	}
	// If we reached here, we did not find the job we were looking for
	t.Errorf("job:%s was not found in set %s", j.id, status.key())
}

func assertJobInTimeIndex(t *testing.T, j *Job) {
	conn := redisPool.Get()
	defer conn.Close()
	gotIds, err := redis.Strings(conn.Do("ZRANGEBYSCORE", keys.jobsTimeIndex, j.time, j.time))
	if err != nil {
		t.Errorf("Unexpected error: %s", err.Error())
	}
	for _, id := range gotIds {
		if id == j.id {
			// We found the job we were looking for
			return
		}
	}
	// If we reached here, we did not find the job we were looking for
	t.Errorf("job:%s was not found in set %s", j.id, keys.jobsTimeIndex)
}

func assertJobNotInStatusSet(t *testing.T, j *Job, status JobStatus) {
	conn := redisPool.Get()
	defer conn.Close()
	gotIds, err := redis.Strings(conn.Do("ZRANGEBYSCORE", status.key(), j.priority, j.priority))
	if err != nil {
		t.Errorf("Unexpected error: %s", err.Error())
	}
	for _, id := range gotIds {
		if id == j.id {
			// We found the job, but it wasn't supposed to be here!
			t.Errorf("job:%s was found in set %s but expected it to be removed", j.id, status.key())
		}
	}
}

func assertJobNotInTimeIndex(t *testing.T, j *Job) {
	conn := redisPool.Get()
	defer conn.Close()
	gotIds, err := redis.Strings(conn.Do("ZRANGEBYSCORE", keys.jobsTimeIndex, j.time, j.time))
	if err != nil {
		t.Errorf("Unexpected error: %s", err.Error())
	}
	for _, id := range gotIds {
		if id == j.id {
			// We found the job, but it wasn't supposed to be here!
			t.Errorf("job:%s was found in set %s but expected it to be removed", j.id, keys.jobsTimeIndex)
		}
	}
}

func assertJobStatusEquals(t *testing.T, job *Job, expected JobStatus) {
	if job.status != expected {
		t.Errorf("Expected jobs:%s status to be %s but got %s", job.id, expected, job.status)
	}
	if expected == StatusDestroyed {
		// If the status is destroyed, we don't expect the job to be in the database
		// anymore.
		assertJobDestroyed(t, job)
	} else {
		// For every status other status, we expect the job to be in the database
		for _, status := range possibleStatuses {
			if status == expected {
				// Make sure the job hash has the correct status
				assertJobInStatusSet(t, job, status)
				// Make sure the job is in the correct set
				assertJobFieldEquals(t, job, "status", string(status), stringConverter)
			} else {
				// Make sure the job is not in any other set
				assertJobNotInStatusSet(t, job, status)
			}
		}
	}
}

func assertJobDestroyed(t *testing.T, job *Job) {
	// Make sure the main hash is gone
	assertKeyNotExists(t, job.key())
	assertJobNotInTimeIndex(t, job)
	for _, status := range possibleStatuses {
		assertJobNotInStatusSet(t, job, status)
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
