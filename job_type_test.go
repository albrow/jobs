package zazu

import (
	"testing"
)

func TestRegisterJobType(t *testing.T) {
	// Make sure we can register a job type without error
	testJobName := "testJob1"
	jobType, err := RegisterJobType(testJobName)
	if err != nil {
		t.Errorf("Unexpected err registering job type: %s", err.Error())
	}
	// Make sure the name property is correct
	if jobType.name != testJobName {
		t.Errorf("Got wrong name for job type. Expected %s but got %s", testJobName, jobType.name)
	}
	// Make sure we cannot register a job type with the same name
	if _, err := RegisterJobType(testJobName); err == nil {
		t.Errorf("Expected error when registering job with the same name but got none")
	} else if _, ok := err.(ErrorNameAlreadyRegistered); !ok {
		t.Errorf("Expected ErrorNameAlreadyRegistered but got error of type %T", err)
	}
}
