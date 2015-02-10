package zazu

import (
	"fmt"
	"github.com/dchest/uniuri"
	"strconv"
	"time"
)

type Job struct {
	id       string
	data     []byte
	typ      *JobType
	time     int64
	priority int
}

func (j *Job) Id() string {
	return j.id
}

func (j *Job) Save() error {
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
	// Add the Job and its attributes to a hash
	if err := conn.Send("HMSET", j.mainHashArgs()...); err != nil {
		return err
	}
	// Index the job by its priority
	if err := conn.Send("ZADD", "jobs:priority", j.priority, j.id); err != nil {
		return err
	}
	// Execute the transaction
	if _, err := conn.Do("EXEC"); err != nil {
		return err
	}
	return nil
}

func (j *Job) mainHashArgs() []interface{} {
	hashKey := fmt.Sprintf("jobs:%s", j.id)
	hashArgs := []interface{}{hashKey, "data", string(j.data), "type", j.typ.name, "time", j.time, "priority", j.priority}
	return hashArgs
}

// generateRandomId generates a random string that is more or less
// garunteed to be unique.
func generateRandomId() string {
	timeInt := time.Now().Unix()
	timeString := strconv.FormatInt(timeInt, 36)
	randomString := uniuri.NewLen(16)
	return randomString + timeString
}
