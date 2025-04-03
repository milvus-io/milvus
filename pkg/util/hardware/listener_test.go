package hardware

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"
)

func TestListener(t *testing.T) {
	w := NewSystemMetricsWatcher(20 * time.Millisecond)
	called := atomic.NewInt32(0)
	l := &SystemMetricsListener{
		Cooldown: 100 * time.Millisecond,
		Condition: func(stats SystemMetrics) bool {
			assert.NotZero(t, stats.UsedMemoryBytes)
			assert.NotZero(t, stats.TotalMemoryBytes)
			assert.NotZero(t, stats.UsedRatio())
			assert.NotEmpty(t, stats.String())
			return true
		},
		Callback: func(sm SystemMetrics) {
			assert.NotZero(t, sm.UsedMemoryBytes)
			assert.NotZero(t, sm.TotalMemoryBytes)
			assert.NotZero(t, sm.UsedRatio())
			assert.NotEmpty(t, sm.String())
			called.Inc()
		},
	}
	w.RegisterListener(l)
	time.Sleep(100 * time.Millisecond)
	assert.Less(t, called.Load(), int32(5))
	assert.Greater(t, called.Load(), int32(0))
	w.UnregisterListener(l)
	w.Close()

	l2 := &SystemMetricsListener{
		Cooldown:  100 * time.Millisecond,
		Condition: l.Condition,
		Callback:  l.Callback,
	}
	RegisterSystemMetricsListener(l)
	RegisterSystemMetricsListener(l2)
	RegisterSystemMetricsListener(l2)
	UnregisterSystemMetricsListener(l)
}
