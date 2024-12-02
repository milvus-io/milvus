package segcore

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetIndexEngineInfo(t *testing.T) {
	r := GetIndexEngineInfo()
	assert.NotZero(t, r.CurrentIndexVersion)
	assert.Zero(t, r.MinIndexVersion)
}
