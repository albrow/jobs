package zazu

// keys stores any constant redis keys. By storing them all here,
// we avoid using string literals which are prone to typos.
var keys = struct {
	jobsTimeIndex string
	jobsTemp      string
}{
	jobsTimeIndex: "jobs:time",
	jobsTemp:      "jobs:temp",
}
