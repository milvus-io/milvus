package metricsutil

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSegmentAccessGather(t *testing.T) {
	g := newSegmentAccessObserver()
	snapshot := g.Snapshot()
	assert.Zero(t, snapshot.Total)
	assert.Zero(t, snapshot.Duration)
	assert.Zero(t, snapshot.WaitLoadDuration)
	g.Observe(newCacheMissingMetric())
	snapshot = g.Snapshot()
	assert.Equal(t, int64(1), snapshot.Total)
	assert.NotZero(t, snapshot.Duration)
	assert.NotZero(t, snapshot.WaitLoadTotal)
	assert.NotZero(t, snapshot.WaitLoadDuration)
}

func TestSegmentCacheGather(t *testing.T) {
	g := newSegmentCacheObserver()
	snapshot := g.Snapshot()
	assert.Zero(t, snapshot.LoadTotal)
	assert.Zero(t, snapshot.LoadDuration)
	g.ObserveLoad(newCacheLoadMetric())
	snapshot = g.Snapshot()
	assert.NotZero(t, snapshot.LoadTotal)
	assert.NotZero(t, snapshot.LoadDuration)
}

func TestSegmentGather(t *testing.T) {
	g := newSegmentObserver("1", SegmentLabel{
		SegmentID:     1,
		DatabaseName:  "db1",
		ResourceGroup: "rg1",
	})
	snapshot := g.Snapshot()
	assert.Zero(t, snapshot.Cache.LoadTotal)
	assert.Zero(t, snapshot.Cache.LoadDuration)
	assert.Zero(t, snapshot.SearchAccess.Total)
	assert.Zero(t, snapshot.SearchAccess.Duration)
	assert.Zero(t, snapshot.SearchAccess.WaitLoadTotal)
	assert.Zero(t, snapshot.SearchAccess.WaitLoadDuration)
	assert.Zero(t, snapshot.QueryAccess.Total)
	assert.Zero(t, snapshot.QueryAccess.Duration)
	assert.Zero(t, snapshot.QueryAccess.WaitLoadTotal)
	assert.Zero(t, snapshot.QueryAccess.WaitLoadDuration)
	g.Observe(newCacheLoadMetric())
	snapshot = g.Snapshot()
	assert.NotZero(t, snapshot.Cache.LoadTotal)
	assert.NotZero(t, snapshot.Cache.LoadDuration)
	assert.Zero(t, snapshot.SearchAccess.Total)
	assert.Zero(t, snapshot.SearchAccess.Duration)
	assert.Zero(t, snapshot.SearchAccess.WaitLoadTotal)
	assert.Zero(t, snapshot.SearchAccess.WaitLoadDuration)
	assert.Zero(t, snapshot.QueryAccess.Total)
	assert.Zero(t, snapshot.QueryAccess.Duration)
	assert.Zero(t, snapshot.QueryAccess.WaitLoadTotal)
	assert.Zero(t, snapshot.QueryAccess.WaitLoadDuration)

	g.Observe(SearchSegmentAccessRecord{
		segmentAccessRecord: newCacheMissingMetric(),
	})
	snapshot = g.Snapshot()
	assert.NotZero(t, snapshot.Cache.LoadTotal)
	assert.NotZero(t, snapshot.Cache.LoadDuration)
	assert.NotZero(t, snapshot.SearchAccess.Total)
	assert.NotZero(t, snapshot.SearchAccess.Duration)
	assert.NotZero(t, snapshot.SearchAccess.WaitLoadTotal)
	assert.NotZero(t, snapshot.SearchAccess.WaitLoadDuration)
	assert.Zero(t, snapshot.QueryAccess.Total)
	assert.Zero(t, snapshot.QueryAccess.Duration)
	assert.Zero(t, snapshot.QueryAccess.WaitLoadTotal)
	assert.Zero(t, snapshot.QueryAccess.WaitLoadDuration)

	g.Observe(QuerySegmentAccessRecord{
		segmentAccessRecord: newCacheMissingMetric(),
	})
	snapshot = g.Snapshot()
	assert.NotZero(t, snapshot.Cache.LoadTotal)
	assert.NotZero(t, snapshot.Cache.LoadDuration)
	assert.NotZero(t, snapshot.SearchAccess.Total)
	assert.NotZero(t, snapshot.SearchAccess.Duration)
	assert.NotZero(t, snapshot.SearchAccess.WaitLoadTotal)
	assert.NotZero(t, snapshot.SearchAccess.WaitLoadDuration)
	assert.NotZero(t, snapshot.QueryAccess.Total)
	assert.NotZero(t, snapshot.QueryAccess.Duration)
	assert.NotZero(t, snapshot.QueryAccess.WaitLoadTotal)
	assert.NotZero(t, snapshot.QueryAccess.WaitLoadDuration)

	assert.False(t, g.IsExpired(time.Now().Add(-time.Minute)))
	assert.True(t, g.IsExpired(time.Now()))

	assert.Panics(t, func() {
		g.Observe(&QuerySegmentAccessRecord{})
	})
}

func TestSegmentsGather(t *testing.T) {
	g := newSegmentsObserver()
	m := NewSearchSegmentAccessRecord(SegmentLabel{
		SegmentID:     1,
		ResourceGroup: "rg1",
		DatabaseName:  "db1",
	})
	m.Finish(nil)

	g.Observe(m)
	assert.Equal(t, 1, g.segments.Len())
	g.Observe(m)
	assert.Equal(t, 1, g.segments.Len())

	m2 := NewQuerySegmentAccessRecord(SegmentLabel{
		SegmentID:     1,
		ResourceGroup: "rg1",
		DatabaseName:  "db1",
	})
	m2.Finish(nil)
	g.Observe(m2)
	assert.Equal(t, 1, g.segments.Len())

	m3 := NewSearchSegmentAccessRecord(SegmentLabel{
		SegmentID:     1,
		ResourceGroup: "rg2",
		DatabaseName:  "db1",
	})
	m3.Finish(nil)
	g.Observe(m3)
	assert.Equal(t, 2, g.segments.Len())

	cnt := 0
	g.ExpireAndObserve(func(label SegmentLabel, snapshot SegmentObserverSnapshot) {
		cnt++
	}, time.Now().Add(-time.Minute))
	assert.Equal(t, 2, cnt)

	cnt = 0
	g.ExpireAndObserve(func(label SegmentLabel, snapshot SegmentObserverSnapshot) {
		cnt++
	}, time.Now())
	assert.Zero(t, cnt)
}

func newCacheMissingMetric() *segmentAccessRecord {
	m := newSegmentAccessRecord(SegmentLabel{
		SegmentID:     1,
		DatabaseName:  "db1",
		ResourceGroup: "rg1",
	})
	m.CacheMissing()
	m.finish(nil)
	return m
}

func newCacheHitMetric() *segmentAccessRecord {
	m := newSegmentAccessRecord(SegmentLabel{
		SegmentID:     1,
		DatabaseName:  "db1",
		ResourceGroup: "rg1",
	})
	m.finish(nil)
	return m
}

func newCacheLoadMetric() *CacheLoadRecord {
	m := NewCacheLoadRecord(SegmentLabel{
		SegmentID:     1,
		DatabaseName:  "db1",
		ResourceGroup: "rg1",
	})
	m.Finish(nil)
	return m
}
