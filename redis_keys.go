package zazu

// keys stores any constant redis keys. By storing them all here,
// we avoid using string literals which are prone to typos.
var keys = struct {
	// jobsTimeIndex is the key for a sorted set which keeps all outstanding
	// jobs sorted by their time field.
	jobsTimeIndex string
	// jobsTemp is the key for a temporary set which is created and then destroyed
	// during the process of getting the next jobs in the queue.
	jobsTemp string
}{
	jobsTimeIndex: "jobs:time",
	jobsTemp:      "jobs:temp",
}
