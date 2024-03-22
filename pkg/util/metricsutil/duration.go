package metricsutil

import (
	"time"

	"go.uber.org/atomic"
)

// NewDuration creates a new Duration.
func NewDuration() Duration {
	return &duration{}
}

// Duration is a metric that represents a single numerical value that only ever goes up.
type Duration interface {
	// Add adds the given value to the duration. It panics if the value is negative.
	Add(d time.Duration)

	// Get returns the current value of the duration.
	Get() time.Duration
}

// duration is a Duration implementation.
type duration struct {
	valInt atomic.Int64
}

func (d *duration) Add(v time.Duration) {
	d.valInt.Add(v.Nanoseconds())
}

func (d *duration) Get() time.Duration {
	c := d.valInt.Load()
	return time.Duration(c) * time.Nanosecond
}
