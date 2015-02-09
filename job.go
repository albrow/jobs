package zazu

import (
	"time"
)

type Job struct {
	Id   string
	data []byte
	typ  JobType
	time time.Time
}
