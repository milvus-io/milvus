package metricsutil

import (
	"errors"
	"math"

	"go.uber.org/atomic"
)

var _ Counter = &counter{}

// NewCounter creates a new Counter.
func NewCounter() Counter {
	return &counter{}
}

// Counter is a metric that represents a single numerical value that only ever goes up.
// Reference: github.com/prometheus/client_golang/prometheus.
type Counter interface {
	// Add adds the given value to the counter. It panics if the value is negative.
	Add(float64)

	// Inc increments the counter by 1.
	Inc()

	// Get returns the current value of the counter.
	Get() float64
}

// counter is a Counter implementation.
type counter struct {
	valInt  atomic.Uint64
	valBits atomic.Uint64
}

func (c *counter) Add(v float64) {
	if v < 0 {
		panic(errors.New("counter cannot decrease in value"))
	}

	ival := uint64(v)
	if float64(ival) == v {
		c.valInt.Add(ival)
		return
	}

	for {
		oldBits := c.valBits.Load()
		newBits := math.Float64bits(math.Float64frombits(oldBits) + v)
		if c.valBits.CompareAndSwap(oldBits, newBits) {
			return
		}
	}
}

func (c *counter) Inc() {
	c.valInt.Inc()
}

func (c *counter) Get() float64 {
	fval := math.Float64frombits(c.valBits.Load())
	ival := c.valInt.Load()
	return fval + float64(ival)
}
