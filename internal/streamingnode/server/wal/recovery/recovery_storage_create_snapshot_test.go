package recovery

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
)

// TestCreateSnapshotFlushesItsCollection covers what replay has to reproduce
// about a snapshot: the fence sealed the collection's growing segments at the
// message's position, and unless replay does the same, a restart in the window
// before the asynchronous per-segment flush message arrives would rebuild those
// segments as GROWING -- handing them back to the allocator, so rows written
// after the snapshot's boundary could land in a segment the snapshot already
// declared sealed.
//
// The set is derived here rather than carried in the message, so the collection
// filter is the part worth pinning: seal too widely and an unrelated collection
// loses its growing segments to someone else's snapshot.
func TestCreateSnapshotFlushesItsCollection(t *testing.T) {
	const pchannel = "test_channel"

	newGrowingSegment := func(segmentID, collectionID int64) *segmentRecoveryInfo {
		return &segmentRecoveryInfo{
			meta: &streamingpb.SegmentAssignmentMeta{
				SegmentId:          segmentID,
				CollectionId:       collectionID,
				PartitionId:        collectionID,
				Vchannel:           pchannel + "_v0",
				State:              streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
				CheckpointTimeTick: 1,
				Stat:               &streamingpb.SegmentAssignmentStat{MaxRows: 1000, ModifiedRows: 10},
			},
		}
	}

	rs := &recoveryStorageImpl{
		cfg:              newConfig(),
		currentClusterID: "test1",
		channel:          types.PChannelInfo{Name: pchannel},
		checkpoint:       &WALCheckpoint{MessageID: rmq.NewRmqID(1), TimeTick: 1},
		segments: map[int64]*segmentRecoveryInfo{
			100: newGrowingSegment(100, 1),
			101: newGrowingSegment(101, 1),
			200: newGrowingSegment(200, 2),
		},
		vchannels: make(map[string]*vchannelRecoveryInfo),
		metrics:   newRecoveryStorageMetrics(types.PChannelInfo{Name: pchannel}),
	}

	msg := message.NewCreateSnapshotMessageBuilderV2().
		WithVChannel(pchannel + "_v0").
		WithHeader(&message.CreateSnapshotMessageHeader{CollectionId: 1, Name: "snap"}).
		WithBody(&message.CreateSnapshotMessageBody{}).
		MustBuildMutable().
		WithTimeTick(10).
		WithLastConfirmed(rmq.NewRmqID(5)).
		IntoImmutableMessage(rmq.NewRmqID(10))

	rs.observeMessage(context.Background(), msg)

	for _, segmentID := range []int64{100, 101} {
		assert.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
			rs.segments[segmentID].meta.State, "segment %d belongs to the snapshot's collection", segmentID)
		// Sealed at the boundary, not at whatever timetick the later flush message
		// carries: that is what stops an insert replayed in between from being
		// attributed to a segment the snapshot considers closed.
		assert.Equal(t, uint64(10), rs.segments[segmentID].meta.CheckpointTimeTick)
	}

	assert.True(t, rs.segments[200].IsGrowing(), "another collection's segment must be untouched")
}
