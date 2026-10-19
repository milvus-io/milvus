//go:build test

package recovery

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// addPersistedGrowingSegment adds a growing segment that is recovered from the catalog (so it is not dirty).
func addPersistedGrowingSegment(rs *recoveryStorageImpl, segmentID, collectionID, partitionID int64, vchannel string) {
	addGrowingSegment(rs, segmentID, collectionID, partitionID, vchannel)
	rs.segments[segmentID].dirty = false
}

func TestSegmentRecoveryInfo_ObserveOrphaned(t *testing.T) {
	rs := newTestRecoveryStorage(t)
	addPersistedGrowingSegment(rs, 1001, 100, 200, "v1")

	segment := rs.segments[1001]
	assert.True(t, segment.IsGrowing())
	assert.False(t, segment.dirty)

	segment.ObserveOrphaned()
	assert.False(t, segment.IsGrowing())
	assert.True(t, segment.dirty)

	// The flushed state is persisted, which removes the segment assignment meta.
	dirtySnapshot, shouldBeRemoved := segment.ConsumeDirtyAndGetSnapshot()
	require.NotNil(t, dirtySnapshot)
	assert.True(t, shouldBeRemoved)
	assert.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED, dirtySnapshot.State)

	// Idempotent: an already flushed segment is left untouched.
	segment.ObserveOrphaned()
	assert.False(t, segment.dirty)
}

func TestGetSnapshot_OrphanedSegmentsAreRemovedFromRecoveryMeta(t *testing.T) {
	rs := newTestRecoveryStorage(t)

	// Active vchannel with a growing segment: must be recovered into the shard manager.
	addActiveVChannel(rs, "v1", 100, []int64{200})
	addGrowingSegment(rs, 1001, 100, 200, "v1")

	// Growing segment on a dropped partition of the active vchannel.
	addPersistedGrowingSegment(rs, 1002, 100, 999, "v1")

	// Dropped vchannel with an orphaned growing segment.
	addDroppedVChannel(rs, "v2", 101)
	addPersistedGrowingSegment(rs, 2001, 101, 300, "v2")

	// Growing segment whose vchannel has already been removed from the catalog
	// (e.g. the collection was dropped and the segment assignment meta survived).
	addPersistedGrowingSegment(rs, 3001, 102, 400, "v3")

	snapshot := rs.getSnapshot()

	// Only the segment of the active vchannel and partition is in the snapshot.
	assert.Len(t, snapshot.SegmentAssignments, 1)
	assert.Contains(t, snapshot.SegmentAssignments, int64(1001))
	assert.True(t, rs.segments[1001].IsGrowing())

	// The orphaned segments can never be flushed by any message anymore,
	// so they are moved into the flushed state and marked dirty to be removed from the catalog.
	for _, segmentID := range []int64{1002, 2001, 3001} {
		assert.False(t, rs.segments[segmentID].IsGrowing(), "segment %d should be flushed", segmentID)
		assert.True(t, rs.segments[segmentID].dirty, "segment %d should be dirty", segmentID)
	}
	assert.Equal(t, 3, rs.dirtyCounter)

	// Taking the snapshot again does not count them twice.
	snapshot = rs.getSnapshot()
	assert.Len(t, snapshot.SegmentAssignments, 1)
	assert.Equal(t, 3, rs.dirtyCounter)

	// The dirty snapshot carries the removal, and the orphaned segments are released from memory.
	dirtySnapshot := rs.consumeDirtySnapshot()
	require.NotNil(t, dirtySnapshot)
	for _, segmentID := range []int64{1002, 2001, 3001} {
		require.Contains(t, dirtySnapshot.SegmentAssignments, segmentID)
		assert.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED, dirtySnapshot.SegmentAssignments[segmentID].State)
		assert.NotContains(t, rs.segments, segmentID)
	}
	assert.Contains(t, rs.segments, int64(1001))
	assert.True(t, rs.segments[1001].IsGrowing())
	assert.Equal(t, 0, rs.dirtyCounter)
}

func TestGetSnapshot_NoOrphanedSegments_NothingToPersist(t *testing.T) {
	rs := newTestRecoveryStorage(t)

	addActiveVChannel(rs, "v1", 100, []int64{200})
	addPersistedGrowingSegment(rs, 1001, 100, 200, "v1")

	snapshot := rs.getSnapshot()
	assert.Len(t, snapshot.SegmentAssignments, 1)
	assert.True(t, rs.segments[1001].IsGrowing())
	assert.False(t, rs.segments[1001].dirty)
	assert.Equal(t, 0, rs.dirtyCounter)
	assert.Nil(t, rs.consumeDirtySnapshot())
}

func TestPersistDirtySnapshot_RemovesOrphanedSegmentAssignmentsFromCatalog(t *testing.T) {
	paramtable.Init()
	snCatalog := mock_metastore.NewMockStreamingNodeCataLog(t)
	persisted := make(map[int64]*streamingpb.SegmentAssignmentMeta)
	snCatalog.EXPECT().SaveSegmentAssignments(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, pchannel string, infos map[int64]*streamingpb.SegmentAssignmentMeta) error {
			for segmentID, info := range infos {
				persisted[segmentID] = info
			}
			return nil
		})
	snCatalog.EXPECT().SaveConsumeCheckpoint(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(snCatalog))

	rs := newRecoveryStorage(types.PChannelInfo{Name: "test_channel"}, &WALCheckpoint{
		MessageID: rmq.NewRmqID(0),
		TimeTick:  0,
	})
	rs.segments = make(map[int64]*segmentRecoveryInfo)
	rs.vchannels = make(map[string]*vchannelRecoveryInfo)

	// Active vchannel with a persisted growing segment: must stay untouched.
	addActiveVChannel(rs, "v1", 100, []int64{200})
	addPersistedGrowingSegment(rs, 1001, 100, 200, "v1")
	// Orphaned growing segment whose vchannel is gone.
	addPersistedGrowingSegment(rs, 2001, 101, 300, "v2")

	snapshot := rs.getSnapshot()
	assert.Len(t, snapshot.SegmentAssignments, 1)
	assert.Contains(t, snapshot.SegmentAssignments, int64(1001))

	require.NoError(t, rs.persistDirtySnapshot(context.Background(), zap.InfoLevel))

	// Only the orphaned segment is persisted, in flushed state, which removes it from the catalog.
	require.Len(t, persisted, 1)
	require.Contains(t, persisted, int64(2001))
	assert.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED, persisted[2001].State)
	assert.NotContains(t, rs.segments, int64(2001))
	assert.Contains(t, rs.segments, int64(1001))
	assert.True(t, rs.segments[1001].IsGrowing())
	assert.Equal(t, 0, rs.dirtyCounter)
}
