package zazu

import (
	"fmt"
	"github.com/dchest/uniuri"
	"github.com/garyburd/redigo/redis"
	"strconv"
	"time"
)

type Job struct {
	id       string
	data     []byte
	typ      *JobType
	status   JobStatus
	time     int64
	priority int
	err      error
}

type JobStatus string

const (
	StatusSaved     = "saved"
	StatusQueued    = "queued"
	StatusExecuting = "executing"
	StatusFinished  = "finished"
	StatusError     = "error"
	StatusCancelled = "cancelled"
	StatusDestroyed = "destroyed"
)

var possibleStatuses = []JobStatus{
	StatusSaved,
	StatusQueued,
	StatusExecuting,
	StatusFinished,
	StatusError,
	StatusCancelled,
	StatusDestroyed,
}

func (j *Job) Id() string {
	return j.id
}

func (j *Job) Status() JobStatus {
	return j.status
}

func (j *Job) Error() error {
	return j.err
}

// save writes the job to the database but does not enqueue it. If you want to add it
// to the queue, use the Enqueue method after save.
func (j *Job) save() error {
	t := newTransaction()
	t.saveJob(j)
	if err := t.exec(); err != nil {
		return err
	}
	return nil
}

// saveJob adds commands to the transaction to set all the fields for the main hash for the job,
// add the job to the time index, and, if necessary, move the job to the saved status set. It will
// also mutate the job by 1) generating an id if the id is empty and 2) setting the status to StatusSaved
// if the status is empty.
func (t *transaction) saveJob(job *Job) {
	// Generate id if needed
	if job.id == "" {
		job.id = generateRandomId()
	}
	// Set status to saved if needed
	if job.status == "" {
		job.status = StatusSaved
		t.addJobToStatusSet(job, StatusSaved)
	}
	// Add the Job attributes to a hash
	t.command("HMSET", job.mainHashArgs(), nil)
	// Add the job to the time index
	args := redis.Args{"jobs:time", job.time, job.id}
	t.command("ZADD", args, nil)
}

// Enqueue adds the job to the queue and sets its status to StatusQueued. Queued jobs will
// be completed by workers in order of priority.
func (j *Job) Enqueue() error {
	if err := j.setStatus(StatusQueued); err != nil {
		return err
	}
	return nil
}

// Cancel cancels the job, but does not remove it from the database. It will be
// added to a list of cancelled jobs. If you wish to remove it from the database,
// use the Destroy method.
func (j *Job) Cancel() error {
	if err := j.setStatus(StatusCancelled); err != nil {
		return err
	}
	return nil
}

// setError sets the err property of j and adds it to the set of jobs which had errors
func (j *Job) setError(err error) error {
	// TODO: implement this
	return nil
}

// Destroy removes all traces of the job from the database. If the job is currently
// being executed by a worker, the worker may still finish the job.
func (j *Job) Destroy() error {
	if j.id == "" {
		return fmt.Errorf("zazu: Cannot destroy job that doesn't have an id.")
	}
	// Start a new transaction
	t := newTransaction()
	// Remove the job hash
	t.command("DEL", j.mainHashArgs()[0:1], nil)
	// Remove the job from the set it is currently in
	setKey := fmt.Sprintf("jobs:%s", j.status)
	args := redis.Args{setKey, j.id}
	t.command("ZREM", args, nil)
	// Remove the job from the time index
	args = redis.Args{"jobs:time", j.id}
	t.command("ZREM", args, nil)
	// Execute the transaction
	if err := t.exec(); err != nil {
		return err
	}
	j.status = StatusDestroyed
	return nil
}

// setStatus updates the job's status in the database and moves it to the appropriate
// set based on its new status.
func (j *Job) setStatus(status JobStatus) error {
	if j.id == "" {
		return fmt.Errorf("zazu: Cannot set status to %s because job doesn't have an id.", status)
	}
	if j.status == StatusDestroyed {
		return fmt.Errorf("zazu: Cannot set job:%s status to %s because it was destroyed.", j.id, status)
	}
	oldStatus := j.status
	j.status = status
	// Use a transaction to move the job to the appropriate status
	t := newTransaction()
	t.setJobStatus(j, oldStatus, status)
	if err := t.exec(); err != nil {
		return err
	}
	j.status = status
	return nil
}

// setJobStatus adds commands to the transaction which will set the status field
// in the main hash for the job and move it to the appropriate status set
func (t *transaction) setJobStatus(job *Job, oldStatus, newStatus JobStatus) {
	args := redis.Args{"jobs:" + job.id, "status", string(newStatus)}
	t.command("HSET", args, nil)
	t.moveJobToStatusSet(job, oldStatus, newStatus)
}

// moveJobToStatusSet adds commands to the transaction which will remove the
// job from it's old status set and add it to the new status set
func (t *transaction) moveJobToStatusSet(job *Job, oldStatus, newStatus JobStatus) {
	t.addJobToStatusSet(job, newStatus)
	t.removeJobFromStatusSet(job, oldStatus)
}

// addJobtoStatusSet adds commands to the transaction which will add
// the job to the status set corresponding to status
func (t *transaction) addJobToStatusSet(job *Job, status JobStatus) {
	setKey := fmt.Sprintf("jobs:%s", status)
	args := redis.Args{setKey, job.priority, job.id}
	t.command("ZADD", args, nil)
}

// removeJobFromStatusSet adds commands to the transaction which will remove
// the job from the status set corresponding to status
func (t *transaction) removeJobFromStatusSet(job *Job, status JobStatus) {
	setKey := fmt.Sprintf("jobs:%s", status)
	args := redis.Args{setKey, job.id}
	t.command("ZREM", args, nil)
}

// mainHashArgs returns the args for the hash which will store the job data
func (j *Job) mainHashArgs() []interface{} {
	hashKey := fmt.Sprintf("jobs:%s", j.id)
	hashArgs := []interface{}{hashKey,
		"data", string(j.data),
		"type", j.typ.name,
		"time", j.time,
		"priority", j.priority,
		"status", j.status,
	}
	if j.err != nil {
		hashArgs = append(hashArgs, "error", j.err.Error())
	}
	return hashArgs
}

// scanJob scans the values of reply into job. reply should be the
// response of an HMGET or HGETALL query.
func scanJob(reply interface{}, job *Job) error {
	fields, err := redis.Values(reply, nil)
	if err != nil {
		return err
	}
	if len(fields)%2 != 0 {
		return fmt.Errorf("zazu: In scanJob: Expected length of fields to be even but got: %d", len(fields))
	}
	for i := 0; i < len(fields)-1; i += 2 {
		fieldName, err := redis.String(fields[i], nil)
		if err != nil {
			return fmt.Errorf("zazu: In scanJob: Could not convert fieldName (fields[%d] = %v) of type %T to string.", i, fields[i], fields[i])
		}
		fieldValue := fields[i+1]
		switch fieldName {
		case "data":
			data, err := redis.Bytes(fieldValue, nil)
			if err != nil {
				return fmt.Errorf("zazu: In scanJob: Could not convert %s (fields[%d] = %v) of type %T to []byte.", fieldName, i, fieldValue, fieldValue)
			}
			job.data = data
		case "type":
			typeName, err := redis.String(fieldValue, nil)
			if err != nil {
				return fmt.Errorf("zazu: In scanJob: Could not convert %s (fields[%d] = %v) of type %T to string.", fieldName, i, fieldValue, fieldValue)
			}
			jobType, found := jobTypes[typeName]
			if !found {
				return fmt.Errorf("zazu: In scanJob: Could not find JobType with name = %s", typeName)
			}
			job.typ = jobType
		case "time":
			time, err := redis.Int64(fieldValue, nil)
			if err != nil {
				return fmt.Errorf("zazu: In scanJob: Could not convert %s (fields[%d] = %v) of type %T to int64.", fieldName, i, fieldValue, fieldValue)
			}
			job.time = time
		case "priority":
			priority, err := redis.Int(fieldValue, nil)
			if err != nil {
				return fmt.Errorf("zazu: In scanJob: Could not convert %s (fields[%d] = %v) of type %T to int.", fieldName, i, fieldValue, fieldValue)
			}
			job.priority = priority
		case "status":
			status, err := redis.String(fieldValue, nil)
			if err != nil {
				return fmt.Errorf("zazu: In scanJob: Could not convert %s (fields[%d] = %v) of type %T to JobStatus.", fieldName, i, fieldValue, fieldValue)
			}
			job.status = JobStatus(status)
		}
	}
	return nil
}

// generateRandomId generates a random string that is more or less
// garunteed to be unique.
func generateRandomId() string {
	timeInt := time.Now().UnixNano()
	timeString := strconv.FormatInt(timeInt, 36)
	randomString := uniuri.NewLen(16)
	return randomString + timeString
}
