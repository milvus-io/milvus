package metricsutil

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDuration(t *testing.T) {
	d := NewDuration()
	assert.Equal(t, time.Duration(0), d.Get())
	d.Add(time.Second)
	assert.Equal(t, time.Second, d.Get())
	d.Add(time.Minute)
	assert.Equal(t, time.Second+time.Minute, d.Get())
}
