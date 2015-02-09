package zazu

import (
	"fmt"
	"time"
)

// A set of all existing names for job types
var jobTypes = map[string]*JobType{}

type JobType struct {
	name string
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

func RegisterJobType(name string) (*JobType, error) {
	if _, found := jobTypes[name]; found {
		return jobTypes[name], NewErrorNameAlreadyRegistered(name)
	}
	jobType := &JobType{name: name}
	jobTypes[name] = jobType
	return jobType, nil
}

func (j *JobType) String() string {
	return j.name
}

func (j *JobType) Do(data interface{}, time time.Timer) (*Job, error) {

}

func (j *JobType) DoNow(data interface{}) (*Job, error) {

}

func (j *JobType) Repeat(data interface{}, frequency time.Duration) (*Job, error) {

}
