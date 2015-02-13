package zazu

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
)

type transaction struct {
	conn     redis.Conn
	commands []command
	handlers []func(interface{}) error
}

type command struct {
	name string
	args redis.Args
}

type replyHandler func(interface{}) error

func newTransaction() *transaction {
	t := &transaction{
		conn: redisPool.Get(),
	}
	return t
}

func (t *transaction) command(cmd string, args redis.Args, handler replyHandler) {
	t.commands = append(t.commands, command{name: cmd, args: args})
	t.handlers = append(t.handlers, handler)
}

func (t *transaction) exec() error {
	// Return the connection to the pool when we are done
	defer t.conn.Close()

	if len(t.commands) == 1 {
		// If there is only one command, no need to use MULTI/EXEC
		c := t.commands[0]
		reply, err := t.conn.Do(c.name, c.args...)
		if err != nil {
			return err
		}
		if t.handlers[0] != nil {
			if err := t.handlers[0](reply); err != nil {
				return err
			}
		}
	} else {
		// Send all the pending commands at once using MULTI/EXEC
		t.conn.Send("MULTI")
		for _, c := range t.commands {
			if err := t.conn.Send(c.name, c.args...); err != nil {
				return err
			}
		}

		// Invoke redis driver to execute the transaction
		replies, err := redis.MultiBulk(t.conn.Do("EXEC"))
		if err != nil {
			return err
		}

		// Iterate through the replies, calling the corresponding handler functions
		for i, reply := range replies {
			handler := t.handlers[i]
			if handler != nil {
				if err := handler(reply); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// newScanJobHandler returns a replyHandler which, when run, will scan the values
// of reply into job.
func newScanJobHandler(job *Job) replyHandler {
	return func(reply interface{}) error {
		return scanJob(reply, job)
	}
}

// newScanStringsHandler returns a replyHandler which, when run, will scan the values
// of reply into strings.
func newScanStringsHandler(strings *[]string) replyHandler {
	return func(reply interface{}) error {
		if strings == nil {
			return fmt.Errorf("zazu: Error in scanStringsHandler: expected strings arg to be a pointer to a slice of strings but it was nil")
		}
		var err error
		(*strings), err = redis.Strings(reply, nil)
		if err != nil {
			return fmt.Errorf("zazu: Error in scanStringsHandler: %s", err.Error())
		}
		return nil
	}
}
