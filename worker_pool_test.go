package zazu

import (
	"reflect"
	"runtime"
	"strconv"
	"testing"
	"time"
)

func TestGetNextJobs(t *testing.T) {
	// TODO: consider edge cases and make this test more rigorous.
	// e.g.:
	// 	1. What happens when there are no queued jobs?
	//		2. What happens when n > len(queued jobs)?
	//		3. Is the job status correct at every stage?
	//		4. Is a given job gauranteed to only be returned by getNextJobs() once?
	flushdb()

	// Create a test job with high priority
	highPriorityJob, err := createTestJob()
	if err != nil {
		t.Errorf("Unexpected error creating test job: %s", err.Error())
	}
	highPriorityJob.priority = 1000
	highPriorityJob.id = "highPriorityJob"
	if err := highPriorityJob.save(); err != nil {
		t.Errorf("Unexpected error saving test job: %s", err.Error())
	}
	if err := highPriorityJob.Enqueue(); err != nil {
		t.Errorf("Unexpected error enqueuing test job: %s", err.Error())
	}

	// Create more tests with lower priorities
	for i := 0; i < 10; i++ {
		job, err := createTestJob()
		if err != nil {
			t.Errorf("Unexpected error creating test job: %s", err.Error())
		}
		job.priority = 100
		job.id = "lowPriorityJob" + strconv.Itoa(i)
		if err := job.save(); err != nil {
			t.Errorf("Unexpected error saving test job: %s", err.Error())
		}
		if err := job.Enqueue(); err != nil {
			t.Errorf("Unexpected error enqueuing test job: %s", err.Error())
		}
	}

	// Call getNextJobs with n = 1. We expect the one job returned to be the
	// highpriority one, but the status should now be executing
	jobs, err := getNextJobs(1)
	if err != nil {
		t.Errorf("Unexpected error from getNextJobs: %s", err.Error())
	}
	if len(jobs) != 1 {
		t.Errorf("Length of jobs was incorrect. Expected 1 but got %d", len(jobs))
	}
	gotJob := jobs[0]
	expectedJob := &Job{}
	(*expectedJob) = *highPriorityJob
	expectedJob.status = StatusExecuting
	if !reflect.DeepEqual(expectedJob, gotJob) {
		t.Errorf("Job returned by getNextJobs was incorrect.\n\tExpected: %+v\n\tBut got:  %+v", expectedJob, gotJob)
	}
}

func TestWorkerPoolStart(t *testing.T) {
	flushdb()

	// Register some jobs which will simply set one of the values in data
	data := make([]string, 8)
	writeResponseJob, err := RegisterJobType("writeResponse", func(i int) {
		data[i] = "ok"
	})
	if err != nil {
		t.Errorf("Unexpected error in RegisterJobType: %s", err.Error())
	}

	// Queue up some jobs
	for i := 0; i < len(data); i++ {
		// Lower indexes have higher priority and should be completed first
		_, err := writeResponseJob.Enqueue(8-i, time.Now(), i)
		if err != nil {
			t.Errorf("Unexpected error in Enqueue: %s", err.Error())
		}
	}

	// Start the pool with 4 workers
	runtime.GOMAXPROCS(4)
	NumWorkers = 4
	BatchSize = 4
	Pool.Start()

	// Immediately stop the pool to stop the workers from doing more jobs
	Pool.Close()

	// Wait for the workers to finish
	Pool.Wait()

	// Check that the first 4 values of data were set to "ok"
	for i := 0; i < 4; i++ {
		if data[i] != "ok" {
			t.Errorf(`Expected data[%d] to be set to "ok" but got: "%s"`, i, data[i])
		}
	}
}
