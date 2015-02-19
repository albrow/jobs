package zazu

import (
	"fmt"
	"reflect"
	"time"
)

// A set of all existing names for job types
var jobTypes = map[string]*JobType{}

type JobType struct {
	name     string
	handler  interface{}
	retries  uint
	dataType reflect.Type
}

type ErrorNameAlreadyRegistered struct {
	name string
}

func (e ErrorNameAlreadyRegistered) Error() string {
	return fmt.Sprintf("zazu: Cannot register job type because job type with name %s already exists", e.name)
}

func NewErrorNameAlreadyRegistered(name string) ErrorNameAlreadyRegistered {
	return ErrorNameAlreadyRegistered{name: name}
}

func RegisterJobType(name string, retries uint, handler interface{}) (*JobType, error) {
	// Make sure name is unique
	if _, found := jobTypes[name]; found {
		return jobTypes[name], NewErrorNameAlreadyRegistered(name)
	}
	// Make sure handler is a function
	handlerType := reflect.TypeOf(handler)
	if handlerType.Kind() != reflect.Func {
		return nil, fmt.Errorf("zazu: in RegisterNewJobType, handler must be a function. Got %T", handler)
	}
	if handlerType.NumIn() > 1 {
		return nil, fmt.Errorf("zazu: in RegisterNewJobType, handler must accept 0 or 1 arguments. Got %d.", handlerType.NumIn())
	}
	jobType := &JobType{
		name:    name,
		handler: handler,
		retries: retries,
	}
	if handlerType.NumIn() == 1 {
		jobType.dataType = handlerType.In(0)
	}
	jobTypes[name] = jobType
	return jobType, nil
}

func (jt *JobType) String() string {
	return jt.name
}

func (jt *JobType) Schedule(priority int, time time.Time, data interface{}) (*Job, error) {
	// Encode the data
	encodedData, err := jt.encodeData(data)
	if err != nil {
		return nil, err
	}
	// Create and save the job
	job := &Job{
		data:     encodedData,
		typ:      jt,
		time:     time.UTC().UnixNano(),
		retries:  jt.retries,
		priority: priority,
	}
	// Set the job's status to queued and save it in the database
	job.status = StatusQueued
	if err := job.save(); err != nil {
		return nil, err
	}
	return job, nil
}

func (jt *JobType) ScheduleRecurring(priority int, time time.Time, freq time.Duration, data interface{}) (*Job, error) {
	// Encode the data
	encodedData, err := jt.encodeData(data)
	if err != nil {
		return nil, err
	}
	// Create and save the job
	job := &Job{
		data:     encodedData,
		typ:      jt,
		time:     time.UTC().UnixNano(),
		retries:  jt.retries,
		freq:     freq.Nanoseconds(),
		priority: priority,
	}
	// Set the job's status to queued and save it in the database
	job.status = StatusQueued
	if err := job.save(); err != nil {
		return nil, err
	}
	return job, nil
}

// encodeData checks that the type of data is what we expect based on the handler for jt. If it is,
// it encodes the data into a slice of bytes.
func (jt *JobType) encodeData(data interface{}) ([]byte, error) {
	// Check the type of data
	dataType := reflect.TypeOf(data)
	if dataType != jt.dataType {
		return nil, fmt.Errorf("zazu: provided data was not of the correct type.\nExpected %s for JobType %s, but got %s", jt.dataType, jt, dataType)
	}
	// Encode the data
	encodedData, err := encode(data)
	if err != nil {
		return nil, fmt.Errorf("zazu: error encoding data: %s", err.Error())
	}
	return encodedData, nil
}
