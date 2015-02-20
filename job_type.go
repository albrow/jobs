// Copyright 2015 Alex Browne.  All rights reserved.
// Use of this source code is governed by the MIT
// license, which can be found in the LICENSE file.

package jobs

import (
	"fmt"
	"reflect"
	"time"
)

// jobTypes is map of job type names to *JobType
var jobTypes = map[string]*JobType{}

// JobType represents a type of job that can be executed by workers
type JobType struct {
	name     string
	handler  interface{}
	retries  uint
	dataType reflect.Type
}

// ErrorNameAlreadyRegistered is returned whenever RegisterJobType is called
// with a name that has already been registered.
type ErrorNameAlreadyRegistered struct {
	name string
}

// Error satisfies the error interface.
func (e ErrorNameAlreadyRegistered) Error() string {
	return fmt.Sprintf("jobs: Cannot register job type because job type with name %s already exists", e.name)
}

// newErrorNameAlreadyRegistered returns an ErrorNameAlreadyRegistered with the given name.
func newErrorNameAlreadyRegistered(name string) ErrorNameAlreadyRegistered {
	return ErrorNameAlreadyRegistered{name: name}
}

// RegisterJobType registers a new type of job that can be executed by workers.
// name should be a unique string identifier for the job.
// retries is the number of times this type of job should be retried if it fails.
// handler is a function that a worker will call in order to execute the job.
// handler should be a function which accepts either 0 or 1 arguments of any type,
// corresponding to the data for a job of this type. All jobs of this type must have
// data with the same type as the first argument to handler, or nil if the handler
// accepts no arguments.
func RegisterJobType(name string, retries uint, handler interface{}) (*JobType, error) {
	// Make sure name is unique
	if _, found := jobTypes[name]; found {
		return jobTypes[name], newErrorNameAlreadyRegistered(name)
	}
	// Make sure handler is a function
	handlerType := reflect.TypeOf(handler)
	if handlerType.Kind() != reflect.Func {
		return nil, fmt.Errorf("jobs: in RegisterNewJobType, handler must be a function. Got %T", handler)
	}
	if handlerType.NumIn() > 1 {
		return nil, fmt.Errorf("jobs: in RegisterNewJobType, handler must accept 0 or 1 arguments. Got %d.", handlerType.NumIn())
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

// String satisfies the Stringer interface and returns the name of the JobType.
func (jt *JobType) String() string {
	return jt.name
}

// Schedule schedules a on-off job of the given type with the given parameters.
// Jobs with a higher priority will be executed first. The job will not be
// executed until after time. data is the data associated with this particular
// job and should have the same type as the first argument to the handler for this
// JobType.
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

// ScheduleRecurring schedules a recurring job of the given type with the given parameters.
// Jobs with a higher priority will be executed first. The job will not be executed until after
// time. After time, the job will be executed with a frequency specified by freq. data is the
// data associated with this particular job and should have the same type as the first argument
// to the handler for this JobType. Every recurring execution of the job will use the
// same data.
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

// encodeData checks that the type of data is what we expect based on the handler for the JobType.
// If it is, it encodes the data into a slice of bytes.
func (jt *JobType) encodeData(data interface{}) ([]byte, error) {
	// Check the type of data
	dataType := reflect.TypeOf(data)
	if dataType != jt.dataType {
		return nil, fmt.Errorf("jobs: provided data was not of the correct type.\nExpected %s for JobType %s, but got %s", jt.dataType, jt, dataType)
	}
	// Encode the data
	encodedData, err := encode(data)
	if err != nil {
		return nil, fmt.Errorf("jobs: error encoding data: %s", err.Error())
	}
	return encodedData, nil
}
