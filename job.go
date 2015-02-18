package zazu

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"time"
)

type Job struct {
	id       string
	data     []byte
	typ      *JobType
	status   JobStatus
	time     int64
	freq     int64
	priority int
	err      error
	retries  uint
	started  int64
	finished int64
}

type JobStatus string

const (
	StatusSaved     JobStatus = "saved"
	StatusQueued    JobStatus = "queued"
	StatusExecuting JobStatus = "executing"
	StatusFinished  JobStatus = "finished"
	StatusFailed    JobStatus = "failed"
	StatusCancelled JobStatus = "cancelled"
	StatusDestroyed JobStatus = "destroyed"
)

// key returns the key used for the sorted set in redis which will hold
// all jobs with this status.
func (status JobStatus) key() string {
	return "jobs:" + string(status)
}

// Count returns the number of jobs that currently have the given status
// or an error if there was a problem connecting to the database.
func (status JobStatus) Count() (int, error) {
	conn := redisPool.Get()
	defer conn.Close()
	return redis.Int(conn.Do("ZCARD", status.key()))
}

var possibleStatuses = []JobStatus{
	StatusSaved,
	StatusQueued,
	StatusExecuting,
	StatusFinished,
	StatusFailed,
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

// Started returns the time that the job started executing (in local time
// with nanosecond precision) or the zero time if the job has not started
// executing yet.
func (j *Job) Started() time.Time {
	return time.Unix(0, j.started).Local()
}

// Finished returns the time that the job finished executing (in local
// time with nanosecond precision) or the zero time if the job has not
// finished executing yet.
func (j *Job) Finished() time.Time {
	return time.Unix(0, j.finished).Local()
}

// Duration returns how long the job took to execute with nanosecond
// precision. I.e. the difference between j.Finished() and j.Started().
// It returns a duration of zero if the job has not finished yet.
func (j *Job) Duration() time.Duration {
	if j.Finished().IsZero() {
		return 0 * time.Second
	}
	return j.Finished().Sub(j.Started())
}

// key returns the key used for the hash in redis which stores all the
// fields for this job.
func (j *Job) key() string {
	return "jobs:" + j.id
}

// isRecurring returns true iff the job is recurring
func (j *Job) isRecurring() bool {
	return j.freq != 0
}

// nextTime returns the time (unix UTC with nanosecond precision) that the
// job should execute next, if it is a recurring job, and 0 if it is not.
func (j *Job) nextTime() int64 {
	if !j.isRecurring() {
		return 0
	}
	// NOTE: is this the proper way to handle rescheduling?
	// What if we schedule jobs faster than they can be executed?
	// Should we just let them build up and expect the end user to
	// allocate more workers? Or should we schedule for time.Now at
	// the earliest to prevent buildup?
	return j.time + j.freq
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
	t.addJobToTimeIndex(job)
}

func (t *transaction) addJobToTimeIndex(job *Job) {
	t.command("ZADD", redis.Args{keys.jobsTimeIndex, job.time, job.id}, nil)
}

// Refresh mutates the job by setting its fields to the most recent data
// found in the database. It returns an error if there was a problem connecting
// to the database or if the job was destroyed.
func (j *Job) Refresh() error {
	t := newTransaction()
	t.scanJobById(j.id, j)
	if err := t.exec(); err != nil {
		return err
	}
	return nil
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
	j.err = err
	t := newTransaction()
	t.command("HSET", redis.Args{j.key(), "error", j.err.Error()}, nil)
	if err := t.exec(); err != nil {
		return err
	}
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
	t.command("DEL", redis.Args{j.key()}, nil)
	// Remove the job from the set it is currently in
	t.command("ZREM", redis.Args{j.status.key(), j.id}, nil)
	// Remove the job from the time index
	t.command("ZREM", redis.Args{keys.jobsTimeIndex, j.id}, nil)
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
	if j.status == status {
		return nil
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
	t.command("HSET", redis.Args{job.key(), "status", string(newStatus)}, nil)
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
	t.command("ZADD", redis.Args{status.key(), job.priority, job.id}, nil)
}

// removeJobFromStatusSet adds commands to the transaction which will remove
// the job from the status set corresponding to status
func (t *transaction) removeJobFromStatusSet(job *Job, status JobStatus) {
	t.command("ZREM", redis.Args{status.key(), job.id}, nil)
}

// mainHashArgs returns the args for the hash which will store the job data
func (j *Job) mainHashArgs() []interface{} {
	hashArgs := []interface{}{j.key(),
		"data", string(j.data),
		"type", j.typ.name,
		"time", j.time,
		"freq", j.freq,
		"priority", j.priority,
		"retries", j.retries,
		"status", j.status,
		"started", j.started,
		"finished", j.finished,
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
			if err := scanBytes(fieldValue, &(job.data)); err != nil {
				return err
			}
		case "type":
			typeName := ""
			if err := scanString(fieldValue, &typeName); err != nil {
				return err
			}
			jobType, found := jobTypes[typeName]
			if !found {
				return fmt.Errorf("zazu: In scanJob: Could not find JobType with name = %s", typeName)
			}
			job.typ = jobType
		case "time":
			if err := scanInt64(fieldValue, &(job.time)); err != nil {
				return err
			}
		case "freq":
			if err := scanInt64(fieldValue, &(job.freq)); err != nil {
				return err
			}
		case "priority":
			if err := scanInt(fieldValue, &(job.priority)); err != nil {
				return err
			}
		case "retries":
			if err := scanUint(fieldValue, &(job.retries)); err != nil {
				return err
			}
		case "status":
			status := ""
			if err := scanString(fieldValue, &status); err != nil {
				return err
			}
			job.status = JobStatus(status)
		case "started":
			if err := scanInt64(fieldValue, &(job.started)); err != nil {
				return err
			}
		case "finished":
			if err := scanInt64(fieldValue, &(job.finished)); err != nil {
				return err
			}
		}
	}
	return nil
}

func scanInt(reply interface{}, v *int) error {
	if v == nil {
		return fmt.Errorf("zazu: In scanInt: argument v was nil")
	}
	val, err := redis.Int(reply, nil)
	if err != nil {
		return fmt.Errorf("zazu: In scanInt: Could not convert %v of type %T to int.", reply, reply)
	}
	(*v) = val
	return nil
}

func scanUint(reply interface{}, v *uint) error {
	if v == nil {
		return fmt.Errorf("zazu: In scanUint: argument v was nil")
	}
	val, err := redis.Uint64(reply, nil)
	if err != nil {
		return fmt.Errorf("zazu: In scanUint: Could not convert %v of type %T to uint.", reply, reply)
	}
	(*v) = uint(val)
	return nil
}

func scanInt64(reply interface{}, v *int64) error {
	if v == nil {
		return fmt.Errorf("zazu: In scanInt64: argument v was nil")
	}
	val, err := redis.Int64(reply, nil)
	if err != nil {
		return fmt.Errorf("zazu: In scanInt64: Could not convert %v of type %T to int64.", reply, reply)
	}
	(*v) = val
	return nil
}

func scanString(reply interface{}, v *string) error {
	if v == nil {
		return fmt.Errorf("zazu: In String: argument v was nil")
	}
	val, err := redis.String(reply, nil)
	if err != nil {
		return fmt.Errorf("zazu: In String: Could not convert %v of type %T to string.", reply, reply)
	}
	(*v) = val
	return nil
}

func scanBytes(reply interface{}, v *[]byte) error {
	if v == nil {
		return fmt.Errorf("zazu: In scanBytes: argument v was nil")
	}
	val, err := redis.Bytes(reply, nil)
	if err != nil {
		return fmt.Errorf("zazu: In scanBytes: Could not convert %v of type %T to []byte.", reply, reply)
	}
	(*v) = val
	return nil
}

// scanJobById adds commands and a reply handler to the transaction which, when run,
// will scan the values of the job corresponding to id into job. It does not execute
// the transaction.
func (t *transaction) scanJobById(id string, job *Job) {
	job.id = id
	t.command("HGETALL", redis.Args{job.key()}, newScanJobHandler(job))
}
