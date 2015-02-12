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
	// Generate id if needed
	if j.id == "" {
		j.id = generateRandomId()
	}
	// Get redis conn from the pool and start transaction
	conn := redisPool.Get()
	defer conn.Close()
	if err := conn.Send("MULTI"); err != nil {
		return err
	}
	// Add the Job to the saved set
	if j.status == "" {
		j.status = StatusSaved
		setKey := fmt.Sprintf("jobs:%s", j.status)
		if err := conn.Send("ZADD", setKey, j.priority, j.id); err != nil {
			return err
		}
	}
	// Add the Job attributes to a hash
	if err := conn.Send("HMSET", j.mainHashArgs()...); err != nil {
		return err
	}
	// Execute the transaction
	if _, err := conn.Do("EXEC"); err != nil {
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
	// TODO: implement this
	return nil
}

// Destroy removes all traces of the job from the database. If the job is currently
// being executed by a worker, the worker may still finish the job.
func (j *Job) Destroy() error {
	if j.id == "" {
		return fmt.Errorf("zazu: Cannot destroy job that doesn't have an id.")
	}
	// Get a redis conn from the pool and start transaction
	conn := redisPool.Get()
	defer conn.Close()
	if err := conn.Send("MULTI"); err != nil {
		return err
	}
	// Remove the job hash
	if err := conn.Send("DEL", j.mainHashArgs()[0]); err != nil {
		return err
	}
	// Remove the job from the index
	setKey := fmt.Sprintf("jobs:%s", j.status)
	if err := conn.Send("ZREM", setKey, j.id); err != nil {
		return err
	}
	// Execute the transaction
	if _, err := conn.Do("EXEC"); err != nil {
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
	// Get redis conn from the pool and start transaction
	conn := redisPool.Get()
	defer conn.Close()
	if err := conn.Send("MULTI"); err != nil {
		return err
	}
	// Set the job status in the hash
	hashKey := fmt.Sprintf("jobs:%s", j.id)
	if err := conn.Send("HSET", hashKey, "status", status); err != nil {
		return err
	}
	// Remove from the old set
	oldStatus := j.status
	oldSetKey := fmt.Sprintf("jobs:%s", oldStatus)
	if err := conn.Send("ZREM", oldSetKey, j.id); err != nil {
		return err
	}
	// Add to the new set
	newSetKey := fmt.Sprintf("jobs:%s", status)
	if err := conn.Send("ZADD", newSetKey, j.priority, j.id); err != nil {
		return err
	}
	// Execute the transaction
	if _, err := conn.Do("EXEC"); err != nil {
		return err
	}
	j.status = status
	return nil
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
	timeInt := time.Now().Unix()
	timeString := strconv.FormatInt(timeInt, 36)
	randomString := uniuri.NewLen(16)
	return randomString + timeString
}
