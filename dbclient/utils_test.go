package dbclient

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestSetupBool(t *testing.T) {
	result := setupBool(true)
	assert.EqualValues(t, "true", result)
	result = setupBool(false)
	assert.EqualValues(t, "false", result)
}

func TestSetupInt(t *testing.T) {
	result := setupInt(0)
	assert.EqualValues(t, "0", result)
	result = setupInt(50)
	assert.EqualValues(t, "50", result)
}

func TestSetupInt64(t *testing.T) {
	result := setupInt64(0)
	assert.EqualValues(t, "0", result)
	result = setupInt64(9999999999)
	assert.EqualValues(t, "9999999999", result)
}

func TestSetupTime(t *testing.T) {
	result := setupTime(time.Duration(10) * time.Second)
	assert.EqualValues(t, "10s", result)
	result = setupTime(time.Duration(50) * time.Minute)
	assert.EqualValues(t, "50m0s", result)
	result = setupTime(time.Duration(23) * time.Hour)
	assert.EqualValues(t, "23h0m0s", result)
}
