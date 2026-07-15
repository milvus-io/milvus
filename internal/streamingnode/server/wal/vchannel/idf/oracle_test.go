package idf

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
)

func TestOracleRuntimeRejectsStaleAdvanceDiff(t *testing.T) {
	current := qviews.DataVersion{StreamingVersion: 10, CompactVersion: 1}
	target := qviews.DataVersion{StreamingVersion: 11, CompactVersion: 1}
	runtime := &oracleRuntime{
		currentVersion: current,
		currentStats:   make(bm25Stats),
		currentSealed:  make(map[int64]sealedContribution),
		currentGrowing: map[int64]growingContribution{
			20: {
				segmentID:   20,
				partitionID: 10,
				stats:       make(bm25Stats),
			},
		},
		growingStore: newGrowingStatsStore(nil),
		revision:     2,
	}

	committed, retry := runtime.commitDiff(&idfDiff{
		target:      target,
		revision:    1,
		positive:    make(bm25Stats),
		negative:    make(bm25Stats),
		nextSealed:  make(map[int64]sealedContribution),
		nextGrowing: make(map[int64]growingContribution),
	})

	require.False(t, committed)
	require.True(t, retry)
	require.True(t, runtime.currentVersion.EQ(current))
	require.Contains(t, runtime.currentGrowing, int64(20))
}
