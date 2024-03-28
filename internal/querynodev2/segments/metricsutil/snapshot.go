package metricsutil

import (
	"time"

	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// SegmentLabel is the label of a segment.
type SegmentLabel struct {
	SegmentID     typeutil.UniqueID `expr:"SegmentID"`
	DatabaseName  string            `expr:"DatabaseName"`
	ResourceGroup string            `expr:"ResourceGroup"`
}

// snapshot returns a snapshot of the segment gather.
// !!! All these fields and type should be exported for expr-lang predicate.
type SegmentObserverSnapshot struct {
	Time          time.Time              `expr:"Time"`
	Label         SegmentLabel           `expr:"Label"`
	ResourceUsage ResourceUsageMetrics   `expr:"ResourceUsage"`
	Cache         CacheObserverSnapshot  `expr:"Cache"`
	SearchAccess  AccessObserverSnapshot `expr:"SearchAccess"`
	QueryAccess   AccessObserverSnapshot `expr:"QueryAccess"`
}

func (s SegmentObserverSnapshot) AccessCount() int64 {
	return s.SearchAccess.Total + s.QueryAccess.Total
}

// CacheObserverSnapshot is a snapshot of segmentCacheGather.
// !!! All these fields and type should be exported for expr-lang predicate.
type CacheObserverSnapshot struct {
	LoadTotal    int64         `expr:"LoadTotal"`
	LoadDuration time.Duration `expr:"LoadDuration"`
}

func (c *CacheObserverSnapshot) Add(snapshot CacheObserverSnapshot) {
	c.LoadTotal += snapshot.LoadTotal
	c.LoadDuration += snapshot.LoadDuration
}

// AccessObserverSnapshot is a snapshot of segmentAccessGather.
// !!! All these fields and type should be exported for expr-lang predicate.
type AccessObserverSnapshot struct {
	Total            int64         `expr:"Total"`
	Duration         time.Duration `expr:"Duration"`
	WaitLoadTotal    int64         `expr:"WaitLoadTotal"`
	WaitLoadDuration time.Duration `expr:"WaitLoadDuration"`
}

func (a *AccessObserverSnapshot) Add(snapshot AccessObserverSnapshot) {
	a.Total += snapshot.Total
	a.Duration += snapshot.Duration
	a.WaitLoadTotal += snapshot.WaitLoadTotal
	a.WaitLoadDuration += snapshot.WaitLoadDuration
}
