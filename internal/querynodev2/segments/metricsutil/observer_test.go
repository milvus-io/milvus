package metricsutil

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSegmentGather(t *testing.T) {
	l := SegmentLabel{
		DatabaseName:  "db1",
		ResourceGroup: "rg1",
	}
	g := newSegmentObserver("1", l)

	r1 := NewCacheLoadRecord(l)
	g.Observe(r1)

	r2 := NewCacheEvictRecord(l)
	g.Observe(r2)

	r3 := NewQuerySegmentAccessRecord(l)
	g.Observe(r3)

	r4 := NewSearchSegmentAccessRecord(l)
	g.Observe(r4)

	// test observe panic.
	assert.Panics(t, func() {
		g.Observe(&QuerySegmentAccessRecord{})
	})

	assert.False(t, g.IsExpired(time.Now().Add(-30*time.Second)))
	assert.True(t, g.IsExpired(time.Now()))

	// Clear should be ok.
	g.Clear()
}

func TestSegmentsGather(t *testing.T) {
	g := newSegmentsObserver()
	r1 := NewQuerySegmentAccessRecord(SegmentLabel{
		ResourceGroup: "rg1",
		DatabaseName:  "db1",
	})
	r1.Finish(nil)
	g.Observe(r1)
	assert.Equal(t, 1, g.segments.Len())

	r2 := NewSearchSegmentAccessRecord(SegmentLabel{
		ResourceGroup: "rg2",
		DatabaseName:  "db1",
	})
	r2.Finish(nil)
	g.Observe(r2)
	assert.Equal(t, 2, g.segments.Len())

	g.Expire(time.Now().Add(-time.Minute))
	assert.Equal(t, 2, g.segments.Len())

	g.Expire(time.Now())
	assert.Zero(t, g.segments.Len())
}
