package metricsutil

import (
	"time"
)

const (
	maxSampleCount = 50
)

// newSegmentHistory creates a new segmentPredicateEnv.
func newSegmentHistory() *segmentHistory {
	return &segmentHistory{
		Snapshots: make([]SegmentObserverSnapshot, 0, 2*maxSampleCount),
	}
}

// segmentHistory is the environment for segment predicate.
type segmentHistory struct {
	latestResourceUsage ResourceUsageMetrics
	Snapshots           []SegmentObserverSnapshot
}

// Len returns the length of the series.
// !!! don't move these method, it will be used in hot segment predicate.
func (h *segmentHistory) Len() int {
	return len(h.Snapshots)
}

// Append appends a new snapshot to the series.
func (h *segmentHistory) Append(snapshot SegmentObserverSnapshot) {
	h.Snapshots = append(h.Snapshots, snapshot)
	h.latestResourceUsage = snapshot.ResourceUsage
}

// Shrink removes old snapshots.
func (h *segmentHistory) Shrink(expireAt time.Time) {
	// keep snapshot small.
	offset := len(h.Snapshots) - maxSampleCount
	if offset > 0 {
		h.Snapshots = h.Snapshots[offset:]
	}

	// expire by time, keep snapshot set small.
	k := -1
	for i := 0; i < len(h.Snapshots); i++ {
		if h.Snapshots[i].Time.After(expireAt) {
			break
		}
		k = i
	}
	h.Snapshots = h.Snapshots[k+1:]
}
