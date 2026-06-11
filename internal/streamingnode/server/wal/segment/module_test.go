package segment

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
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
