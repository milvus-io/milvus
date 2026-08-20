package vchannel

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/segment"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func TestVChannelAdvancesTransformMaterializationAfterL1Commit(t *testing.T) {
	ctx := context.Background()
	scheduler := &recordingVChannelScheduler{}
	segmentMetas := map[int64]*streamingpb.SegmentAssignmentMeta{
		1: newMaterializationBlockerMeta(1, 100, false),
		2: newMaterializationBlockerMeta(2, 200, false),
	}
	module, err := NewModule(ModuleConfig{
		PChannel:         "p1",
		VChannel:         "v1",
		Segments:         segmentMetas,
		TransformLogMeta: &streamingpb.VChannelTransformLogMeta{CheckpointTimeTick: 300},
		Runtime:          moduleapi.Runtime{Scheduler: scheduler},
	})
	require.NoError(t, err)

	require.True(t, module.transformLog.RequestMaterializeThrough(300))
	require.Len(t, scheduler.tasks, 1)
	require.NoError(t, scheduler.tasks[0].Execute(ctx))
	assert.Equal(t, uint64(100), module.transformLog.SnapshotMeta().GetMaterializedTimeTick())

	first := segment.NewSegmentViewFromMetaWithConfig(
		newMaterializationBlockerMeta(1, 100, true),
		nil,
		module.segmentViewConfig(),
	)
	module.mu.Lock()
	module.segments[1] = first
	module.mu.Unlock()
	module.SegmentDataUpdated(1, first)
	require.Len(t, scheduler.tasks, 2)
	require.NoError(t, scheduler.tasks[1].Execute(ctx))
	assert.Equal(t, uint64(200), module.transformLog.SnapshotMeta().GetMaterializedTimeTick())

	second := segment.NewSegmentViewFromMetaWithConfig(
		newMaterializationBlockerMeta(2, 200, true),
		nil,
		module.segmentViewConfig(),
	)
	module.mu.Lock()
	module.segments[2] = second
	module.mu.Unlock()
	module.SegmentDataUpdated(2, second)
	require.Len(t, scheduler.tasks, 3)
	require.NoError(t, scheduler.tasks[2].Execute(ctx))
	assert.Equal(t, uint64(300), module.transformLog.SnapshotMeta().GetMaterializedTimeTick())
}

func newMaterializationBlockerMeta(segmentID int64, createTimeTick uint64, l1CommitDone bool) *streamingpb.SegmentAssignmentMeta {
	return &streamingpb.SegmentAssignmentMeta{
		PartitionId:  segmentID,
		SegmentId:    segmentID,
		Vchannel:     "v1",
		L1CommitDone: l1CommitDone,
		Stat: &streamingpb.SegmentAssignmentStat{
			CreateSegmentTimeTick: createTimeTick,
		},
	}
}
