package metricsutil

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestHistory(t *testing.T) {
	h := newSegmentHistory()
	g := newSegmentObserver("1", SegmentLabel{
		SegmentID:     1,
		DatabaseName:  "db1",
		ResourceGroup: "rg1",
	})

	h.Append(g.Snapshot())
	assert.Equal(t, 1, h.Len())

	for i := 0; i < 99; i++ {
		g = newSegmentObserver("1", SegmentLabel{
			SegmentID:     1,
			DatabaseName:  "db1",
			ResourceGroup: "rg1",
		})
		h.Append(g.Snapshot())
	}
	assert.Equal(t, 100, h.Len())
	h.Shrink(time.Now().Add(-snapshotExpire))
	assert.Equal(t, 50, h.Len())
	h.Shrink(time.Now())
	assert.Equal(t, 0, h.Len())
}

func TestHistoryGetSnapshot(t *testing.T) {
	h := newSegmentHistory()

	g := newSegmentObserver("1", SegmentLabel{
		SegmentID:     1,
		DatabaseName:  "db1",
		ResourceGroup: "rg1",
	})

	snapshot := g.Snapshot()
	now := snapshot.Time
	h.Append(snapshot)

	snapshot = g.Snapshot()
	snapshot.Time = now.Add(time.Second)
	h.Append(snapshot)

	snapshot = g.Snapshot()
	snapshot.Time = now.Add(2 * time.Second)
	h.Append(snapshot)

	assert.Len(t, h.Snapshots, 3)
}
