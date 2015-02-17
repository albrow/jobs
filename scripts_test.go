package zazu

import (
	"github.com/garyburd/redigo/redis"
	"reflect"
	"testing"
)

func TestGetAndMoveJobsToExecutingScript(t *testing.T) {
	flushdb()

	// Set up the database
	tx0 := newTransaction()
	// One set will mimic the ready and sorted jobs
	jobsReadyAndSortedKey := jobsReadyAndSorted.generateKey()
	tx0.command("ZADD", redis.Args{jobsReadyAndSortedKey, 3, "three", 4, "four"}, nil)
	// One set will mimic the queued set
	tx0.command("ZADD", redis.Args{StatusQueued.key(), 1, "one", 2, "two", 3, "three", 4, "four"}, nil)
	// One set will mimic the executing set
	tx0.command("ZADD", redis.Args{StatusExecuting.key(), 5, "five"}, nil)
	if err := tx0.exec(); err != nil {
		t.Errorf("Unexpected error executing transaction: %s", err.Error())
	}

	// Start a new transaction and execute the script
	tx1 := newTransaction()
	gotIds := []string{}
	tx1.script(getAndMoveJobsToExecuting, redis.Args{jobsReadyAndSortedKey, StatusQueued.key(), StatusExecuting.key()}, newScanStringsHandler(&gotIds))
	if err := tx1.exec(); err != nil {
		t.Errorf("Unexpected error executing transaction: %s", err.Error())
	}

	// Check the results
	expectedIds := []string{"four", "three"}
	if !reflect.DeepEqual(expectedIds, gotIds) {
		t.Errorf("Ids returned by script were incorrect.\n\tExpected: %v\n\tBut got:  %v", expectedIds, gotIds)
	}
	conn := redisPool.Get()
	defer conn.Close()
	expectedExecuting := []string{"five", "four", "three"}
	gotExecuting, err := redis.Strings(conn.Do("ZREVRANGE", StatusExecuting.key(), 0, -1))
	if err != nil {
		t.Errorf("Unexpected error in ZREVRANGE: %s", err.Error())
	}
	if !reflect.DeepEqual(expectedExecuting, gotExecuting) {
		t.Errorf("Ids in the executing set were incorrect.\n\tExpected: %v\n\tBut got:  %v", expectedExecuting, gotExecuting)
	}
	expectedQueued := []string{"two", "one"}
	gotQueued, err := redis.Strings(conn.Do("ZREVRANGE", StatusQueued.key(), 0, -1))
	if err != nil {
		t.Errorf("Unexpected error in ZREVRANGE: %s", err.Error())
	}
	if !reflect.DeepEqual(expectedQueued, gotQueued) {
		t.Errorf("Ids in the queued set were incorrect.\n\tExpected: %v\n\tBut got:  %v", expectedQueued, gotQueued)
	}
}
