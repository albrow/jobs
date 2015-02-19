// Copyright 2015 Alex Browne.  All rights reserved.
// Use of this source code is governed by the MIT
// license, which can be found in the LICENSE file.

package zazu

import (
	"runtime"
	"time"
)

// configType holds different config variables
type configType struct {
	Pool poolConfig
	Db   databaseConfig
}

// databaseConfig holds config variables specific to the database
type databaseConfig struct {
	// Address is the address of the redis database to connect to. Default is
	// "localhost:6379".
	Address string
	// Network is the type of network to use to connect to the redis database
	// Default is "tcp".
	Network string
	// Database is the redis database number to use for storing all data. Default
	// is 0.
	Database int
}

// poolConfig holds config variables specific to the worker pool
type poolConfig struct {
	// NumWorkers is the number of workers to run
	// Each worker will run inside its own goroutine
	// and execute jobs asynchronously. Default is
	// runtime.GOMAXPROCS.
	NumWorkers int
	// BatchSize is the number of jobs to send through
	// the jobs channel at once. Increasing BatchSize means
	// the worker pool will query the database less frequently,
	// so you would get higher performance. However this comes
	// at the cost that jobs with lower priority may sometimes be
	// executed before jobs with higher priority, because the jobs
	// with higher priority were not ready yet the last time the pool
	// queried the database. Decreasing BatchSize means more
	// frequent queries to the database and lower performance, but
	// greater likelihood of executing jobs in perfect order with regards
	// to priority. Setting BatchSize to 1 gaurantees that higher priority
	// jobs are always executed first as soon as they are ready. Default is
	// runtime.GOMAXPROCS.
	BatchSize int
	// MinWait is the minimum amount of time the pool will wait before checking
	// the database for queued jobs. The pool may take longer to query the database
	// if the jobs channel is blocking (i.e. if no workers are ready to execute new
	// jobs). Default is 200ms.
	MinWait time.Duration
}

// Config is where all configuration variables are stored. You may modify Config
// directly in order to change config variables, and should only do so at the start
// of your program.
var Config = configType{
	Pool: poolConfig{
		NumWorkers: runtime.GOMAXPROCS(0),
		BatchSize:  runtime.GOMAXPROCS(0),
		MinWait:    200 * time.Millisecond,
	},
	Db: databaseConfig{
		Address:  "localhost:6379",
		Network:  "tcp",
		Database: 0,
	},
}
