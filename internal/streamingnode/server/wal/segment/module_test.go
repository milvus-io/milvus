package segment

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func TestCleanupDeleteSnapshotKeepsStableInFlightView(t *testing.T) {
	module := NewModule("p1", map[int64]*streamingpb.SegmentAssignmentMeta{
		1: {
			CollectionId:           1,
			PartitionId:            10,
			SegmentId:              1,
			Vchannel:               "v1",
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED,
			CheckpointTimeTick:     10,
			DataCheckpointTimeTick: 10,
			TombstoneTimeTick:      10,
			PersistedStorage:       &streamingpb.L1SegmentPersistedStorage{},
			Stat: &streamingpb.SegmentAssignmentStat{
				CreateSegmentTimeTick: 1,
			},
		},
	}, nil, nil)
	module.NotifyCheckpointPersisted(11, 11)

	first := module.ConsumeDirtySnapshots()
	require.Len(t, first, 1)
	assert.Equal(t, moduleapi.SnapshotOpDelete, first[0].Op())

	second := module.ConsumeDirtySnapshots()
	require.Len(t, second, 1)
	assert.Same(t, first[0], second[0])
	assert.Len(t, module.snapshotSegments(), 1)

	first[0].MarkPersisted()
	assert.Empty(t, module.ConsumeDirtySnapshots())
	assert.Empty(t, module.snapshotSegments())
}

func TestBuildCommitL1SegmentRequestUsesCreateSegmentTimetickForDeleteApply(t *testing.T) {
	meta := &streamingpb.SegmentAssignmentMeta{
		CollectionId:           1,
		PartitionId:            10,
		SegmentId:              100,
		Vchannel:               "v1",
		DataCheckpointTimeTick: 3000,
		StorageVersion:         3,
		PersistedStorage:       &streamingpb.L1SegmentPersistedStorage{ManifestPath: "manifest"},
		Stat: &streamingpb.SegmentAssignmentStat{
			CreateSegmentTimeTick: 1000,
			ModifiedRows:          10,
			Level:                 datapb.SegmentLevel_L1,
		},
	}

	req := buildCommitL1SegmentRequest(1, meta)

	assert.Equal(t, uint64(1000), req.GetDeleteApplyStartAfterTimetick())
	assert.Equal(t, int64(10), req.GetCheckPoints()[0].GetNumOfRows())
}

func TestNewGrowingSegmentInfoUsesCreateSegmentTimetickForDeleteApply(t *testing.T) {
	meta := &streamingpb.SegmentAssignmentMeta{
		CollectionId:           1,
		PartitionId:            10,
		SegmentId:              100,
		Vchannel:               "v1",
		DataCheckpointTimeTick: 3000,
		StorageVersion:         3,
		Stat: &streamingpb.SegmentAssignmentStat{
			CreateSegmentTimeTick: 1000,
			ModifiedRows:          10,
			Level:                 datapb.SegmentLevel_L1,
		},
	}

	segmentInfo := newGrowingSegmentInfo(meta)

	assert.Equal(t, uint64(1000), segmentInfo.GetDeleteApplyStartAfterTimetick())
	assert.Equal(t, uint64(1000), segmentInfo.GetStartPosition().GetTimestamp())
	assert.Equal(t, uint64(3000), segmentInfo.GetDmlPosition().GetTimestamp())
}
