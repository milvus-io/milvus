//go:build test && dynamic

package qnview

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/querynodev2/segments"
)

func TestSegmentLoadTimingStatsAggregatesAndResets(t *testing.T) {
	stats := segmentLoadTimingStats{interval: time.Hour, batchSize: 2}
	startedAt := time.Unix(100, 0)
	_, ok := stats.add(startedAt, segmentLoadTimingSample{
		total:           10 * time.Millisecond,
		reserveResource: 3 * time.Millisecond,
		physicalLoad:    4 * time.Millisecond,
		physicalDetail: segments.PhysicalLoadTiming{
			NewSegment:         2 * time.Millisecond,
			LoadSegment:        4 * time.Millisecond,
			SealedLoadPoolWait: 6 * time.Millisecond,
			LocalSegmentLoad:   8 * time.Millisecond,
			CSegmentLoad:       10 * time.Millisecond,
			SyncJSONStats:      12 * time.Millisecond,
			SealedPostLoad:     14 * time.Millisecond,
			DeltaLogs:          16 * time.Millisecond,
			PKCandidate:        18 * time.Millisecond,
		},
		releaseResource: time.Millisecond,
		onLoaded:        2 * time.Millisecond,
	})
	require.False(t, ok)

	snapshot, ok := stats.add(startedAt.Add(time.Second), segmentLoadTimingSample{
		total:           20 * time.Millisecond,
		reserveResource: 6 * time.Millisecond,
		physicalLoad:    8 * time.Millisecond,
		physicalDetail: segments.PhysicalLoadTiming{
			NewSegment:         4 * time.Millisecond,
			LoadSegment:        8 * time.Millisecond,
			SealedLoadPoolWait: 12 * time.Millisecond,
			LocalSegmentLoad:   16 * time.Millisecond,
			CSegmentLoad:       20 * time.Millisecond,
			SyncJSONStats:      24 * time.Millisecond,
			SealedPostLoad:     28 * time.Millisecond,
			DeltaLogs:          32 * time.Millisecond,
			PKCandidate:        36 * time.Millisecond,
		},
		releaseResource: 2 * time.Millisecond,
		failed:          true,
	})
	require.True(t, ok)
	assert.Equal(t, int64(2), snapshot.count)
	assert.Equal(t, int64(1), snapshot.failed)
	assert.Equal(t, time.Second, snapshot.windowDuration)
	assert.Equal(t, 15*time.Millisecond, snapshot.total.average(snapshot.count))
	assert.Equal(t, 20*time.Millisecond, snapshot.total.max)
	assert.Equal(t, 6*time.Millisecond, snapshot.physicalLoad.average(snapshot.count))
	assert.Equal(t, 3*time.Millisecond, snapshot.physicalDetail.newSegment.average(snapshot.count))
	assert.Equal(t, 6*time.Millisecond, snapshot.physicalDetail.loadSegment.average(snapshot.count))
	assert.Equal(t, 9*time.Millisecond, snapshot.physicalDetail.sealedLoadPoolWait.average(snapshot.count))
	assert.Equal(t, 12*time.Millisecond, snapshot.physicalDetail.localSegmentLoad.average(snapshot.count))
	assert.Equal(t, 15*time.Millisecond, snapshot.physicalDetail.cSegmentLoad.average(snapshot.count))
	assert.Equal(t, 18*time.Millisecond, snapshot.physicalDetail.syncJSONStats.average(snapshot.count))
	assert.Equal(t, 21*time.Millisecond, snapshot.physicalDetail.sealedPostLoad.average(snapshot.count))
	assert.Equal(t, 24*time.Millisecond, snapshot.physicalDetail.deltaLogs.average(snapshot.count))
	assert.Equal(t, 27*time.Millisecond, snapshot.physicalDetail.pkCandidate.average(snapshot.count))
	assert.Equal(t, time.Millisecond, snapshot.onLoaded.average(snapshot.count))

	_, ok = stats.add(startedAt.Add(2*time.Second), segmentLoadTimingSample{})
	assert.False(t, ok)
}

func TestSegmentLoadTimingStatsLogsOnInterval(t *testing.T) {
	stats := segmentLoadTimingStats{interval: 5 * time.Second, batchSize: 100}
	startedAt := time.Unix(100, 0)
	_, ok := stats.add(startedAt, segmentLoadTimingSample{})
	require.False(t, ok)
	snapshot, ok := stats.add(startedAt.Add(5*time.Second), segmentLoadTimingSample{})
	require.True(t, ok)
	assert.Equal(t, int64(2), snapshot.count)
	assert.Equal(t, 5*time.Second, snapshot.windowDuration)
}
