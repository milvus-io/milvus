package metricsutil

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCounter(t *testing.T) {
	c := NewCounter()
	c.Add(1)
	assert.Equal(t, 1, int(c.Get()))
	c.Add(1)
	assert.Equal(t, 2, int(c.Get()))
	c.Inc()
	assert.Equal(t, 3, int(c.Get()))
	c.Add(2.5)
	assert.True(t, c.Get()-5.5 < 1e-6)

	c = NewCounter()
	wg := sync.WaitGroup{}
	wg.Add(100)
	cnt := 0.0
	for i := 0; i < 100; i++ {
		go func(i int) {
			c.Add(float64(i) * 1.5)
			wg.Done()
		}(i)
		cnt += float64(i) * 1.5
	}
	wg.Wait()
	assert.True(t, c.Get()-cnt < 1e-6)
	assert.Panics(t, func() {
		c.Add(-1)
	})
}
