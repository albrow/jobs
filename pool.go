package zazu

import (
	"github.com/garyburd/redigo/redis"
	"time"
)

var redisPool = &redis.Pool{
	MaxIdle:     10,
	MaxActive:   0,
	IdleTimeout: 240 * time.Second,
	Dial: func() (redis.Conn, error) {
		// TODO: make this configurable
		return redis.Dial("tcp", "localhost:6379")
	},
}
