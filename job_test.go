package zazu

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"reflect"
	"testing"
	"time"
)

func TestJobSave(t *testing.T) {
	// Create and save a test job
	jobId := "testJob"
	jobData := []byte("testData")
	jobTypeName := "testJobType"
	jobTime := time.Now().UTC().Unix()
	jobPriority := 100
	jobType, err := RegisterJobType(jobTypeName)
	if err != nil {
		panic(err)
	}
	j := &Job{
		id:       jobId,
		data:     jobData,
		typ:      jobType,
		time:     jobTime,
		priority: jobPriority,
	}
	if err := j.Save(); err != nil {
		t.Errorf("Unexpected error in j.Save(): %s", err.Error())
	}
	// Make sure the main hash was saved correctly
	assertJobFieldEquals(t, j, "data", jobData, nil)
	assertJobFieldEquals(t, j, "time", jobTime, int64Converter)
	assertJobFieldEquals(t, j, "type", jobType.name, stringConverter)
	assertJobFieldEquals(t, j, "priority", jobPriority, intConverter)
	// Make sure the job was indexed correctly by its priority
	assertJobPriorityEquals(t, j, jobPriority)
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
		t.Errorf("job.%s was not saved correctly.\n\tExpected: %v\n\tBut got:  %v.", fieldName, expected, got)
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

func assertJobPriorityEquals(t *testing.T, j *Job, priority int) {
	conn := redisPool.Get()
	defer conn.Close()
	got, err := redis.Values(conn.Do("ZRANGEBYSCORE", "jobs:priority", priority, priority))
	if err != nil {
		t.Errorf("Unexpected error: %s", err.Error())
	}
	if len(got) != 1 {
		t.Errorf("job.priority was not indexed correctly. Expected to find one job with the given priority but found %d", len(got))
	}
}
