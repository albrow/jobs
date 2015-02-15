package zazu

import (
	"fmt"
)

// keys stores any constant redis keys. By storing them all here,
// we avoid using string literals which are prone to typos.
var keys = struct {
	jobsTimeIndex string
}{
	jobsTimeIndex: "jobs:time",
}

// uniqueKey is used for storing things temporarily in redis (such as generated sets used for ZINTERSTORE).
// It can be used to create keys which consist of a human-readable prefix and a randomly generated
// unique suffix, which is useful for avoiding naming colissions between multiple processes/threads,
// possibly on multiple machines.
type uniqueKey struct {
	prefix string
}

// generateKey returns a more or less guaranteed unique key safe for use in redis which consists
// of name.prefix and a randomly generated suffix.
func (name uniqueKey) generateKey() string {
	return fmt.Sprintf("%s:%s", name.prefix, generateRandomId())
}

var (
	// jobsReadyByTime is used for temporary sets generated when getting jobs from the queue in the database
	jobsReadyByTime = uniqueKey{prefix: "jobs:readyByTime"}
	// jobsReadyAndSorted is used for temporary sets generated when getting jobs from the queue in the database
	jobsReadyAndSorted = uniqueKey{prefix: "jobs:readyAndSorted"}
)
