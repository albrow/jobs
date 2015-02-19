package zazu

import (
	"github.com/garyburd/redigo/redis"
	"time"
)

// redisPool is a thread-safe pool of redis connections. Use the Get method
// to get a new connection.
var redisPool = &redis.Pool{
	MaxIdle:     10,
	MaxActive:   0,
	IdleTimeout: 240 * time.Second,
	Dial: func() (redis.Conn, error) {
		c, err := redis.Dial(Config.Db.Network, Config.Db.Address)
		if err != nil {
			return nil, err
		}
		if _, err := c.Do("SELECT", Config.Db.Database); err != nil {
			c.Close()
			return nil, err
		}
		return c, nil
	},
}
