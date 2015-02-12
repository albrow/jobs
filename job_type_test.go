package zazu

import (
	"testing"
	"time"
)

func TestRegisterJobType(t *testing.T) {
	// Reset job types
	jobTypes = map[string]*JobType{}
	// Make sure we can register a job type without error
	testJobName := "testJob1"
	jobType, err := RegisterJobType(testJobName, func() {})
	if err != nil {
		t.Errorf("Unexpected err registering job type: %s", err.Error())
	}
	// Make sure the name property is correct
	if jobType.name != testJobName {
		t.Errorf("Got wrong name for job type. Expected %s but got %s", testJobName, jobType.name)
	}
	// Make sure the jobType was added to the global map
	if _, found := jobTypes[testJobName]; !found {
		t.Errorf("JobType was not added to the global map of job types.")
	}
	// Make sure we cannot register a job type with the same name
	if _, err := RegisterJobType(testJobName, func() {}); err == nil {
		t.Errorf("Expected error when registering job with the same name but got none")
	} else if _, ok := err.(ErrorNameAlreadyRegistered); !ok {
		t.Errorf("Expected ErrorNameAlreadyRegistered but got error of type %T", err)
	}
	// Make sure we can register a job type with a handler function that has an argument
	if _, err := RegisterJobType("testJobWithArg", func(s string) { print(s) }); err != nil {
		t.Errorf("Unexpected err registering job type with handler with one argument: %s", err)
	}
	// Make sure we cannot register a job type with an invalid handler
	invalidHandlers := []interface{}{
		"notAFunc",
		func(a, b string) {},
	}
	for _, handler := range invalidHandlers {
		if _, err := RegisterJobType("testJobWithInvalidHandler", handler); err == nil {
			t.Errorf("Expected error when registering job with invalid handler type %T %v, but got none.", handler, handler)
		}
	}
}

func TestJobTypeEnqueue(t *testing.T) {
	// Reset job types and flush database
	flushdb()
	jobTypes = map[string]*JobType{}
	// Register a new job type
	testJobName := "testJob1"
	testJobPriority := 100
	testJobTime := time.Now()
	testJobData := "testData"
	encodedData, err := encode(testJobData)
	if err != nil {
		t.Errorf("Unexpected error encoding data: %s", err)
	}
	encodedTime := testJobTime.UTC().Unix()
	jobType, err := RegisterJobType(testJobName, func(string) {})
	if err != nil {
		t.Errorf("Unexpected error registering job type: %s", err)
	}
	// Call Enqueue
	job, err := jobType.Enqueue(testJobPriority, testJobTime, testJobData)
	if err != nil {
		t.Errorf("Unexpected error in jobType.Enqueue(): %s", err)
	}
	// Make sure the job was saved in the database correctly
	if job.id == "" {
		t.Errorf("After jobType.Enqueue, job.id was empty.")
	}
	assertKeyExists(t, "jobs:"+job.id)
	assertJobFieldEquals(t, job, "priority", testJobPriority, intConverter)
	assertJobFieldEquals(t, job, "time", encodedTime, int64Converter)
	assertJobFieldEquals(t, job, "data", encodedData, bytesConverter)
	assertJobStatusEquals(t, job, StatusQueued)
	// Make sure we get an error if the data is not the correct type
	if _, err := jobType.Enqueue(0, time.Now(), 0); err == nil {
		t.Errorf("Expected error when calling jobType.Enqueue with incorrect data type but got none")
	}
}
