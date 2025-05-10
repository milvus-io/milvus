package recovery

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/rmq"
)

func TestNewSegmentRecoveryInfoFromSegmentAssignmentMeta(t *testing.T) {
	// Test with empty input
	metas := []*streamingpb.SegmentAssignmentMeta{}
	infos := newSegmentRecoveryInfoFromSegmentAssignmentMeta(metas)
	assert.Empty(t, infos)

	// Test with valid input
	metas = []*streamingpb.SegmentAssignmentMeta{
		{SegmentId: 1, Vchannel: "vchannel-1"},
		{SegmentId: 2, Vchannel: "vchannel-2"},
	}

	infos = newSegmentRecoveryInfoFromSegmentAssignmentMeta(metas)
	assert.Len(t, infos, 2)
	assert.Equal(t, int64(1), infos[1].meta.SegmentId)
	assert.Equal(t, int64(2), infos[2].meta.SegmentId)
	assert.False(t, infos[1].dirty)
	assert.False(t, infos[2].dirty)
}

func TestSegmentRecoveryInfo(t *testing.T) {
	msg := message.NewCreateSegmentMessageBuilderV2().
		WithHeader(&message.CreateSegmentMessageHeader{
			CollectionId:   100,
			PartitionId:    1,
			SegmentId:      2,
			StorageVersion: storage.StorageV1,
			MaxSegmentSize: 100,
		}).
		WithBody(&message.CreateSegmentMessageBody{}).
		WithVChannel("vchannel-1").
		MustBuildMutable()

	id := rmq.NewRmqID(1)
	ts := uint64(12345)
	immutableMsg := msg.WithTimeTick(ts).WithLastConfirmed(id).IntoImmutableMessage(id)

	info := newSegmentRecoveryInfoFromCreateSegmentMessage(message.MustAsImmutableCreateSegmentMessageV2(immutableMsg))
	assert.Equal(t, int64(2), info.meta.SegmentId)
	assert.Equal(t, int64(1), info.meta.PartitionId)
	assert.Equal(t, storage.StorageV1, info.meta.StorageVersion)
	assert.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING, info.meta.State)
	assert.Equal(t, uint64(100), info.meta.Stat.MaxBinarySize)

	ts += 1
	assign := &messagespb.PartitionSegmentAssignment{
		PartitionId: 1,
		Rows:        1,
		BinarySize:  10,
		SegmentAssignment: &messagespb.SegmentAssignment{
			SegmentId: 2,
		},
	}
	info.ObserveInsert(ts, assign)
	assert.True(t, info.dirty)
	snapshot, shouldBeRemoved := info.ConsumeDirtyAndGetSnapshot()
	assert.NotNil(t, snapshot)
	assert.False(t, shouldBeRemoved)
	assert.False(t, info.dirty)

	assert.Equal(t, uint64(10), snapshot.Stat.InsertedBinarySize)
	snapshot, shouldBeRemoved = info.ConsumeDirtyAndGetSnapshot()
	assert.Nil(t, snapshot)
	assert.False(t, shouldBeRemoved)

	// insert may came from same txn with same txn.
	info.ObserveInsert(ts, assign)
	assert.True(t, info.dirty)

	ts += 1
	info.ObserveFlush(ts)
	snapshot, shouldBeRemoved = info.ConsumeDirtyAndGetSnapshot()
	assert.NotNil(t, snapshot)
	assert.Equal(t, snapshot.State, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED)
	assert.True(t, shouldBeRemoved)
	assert.False(t, info.dirty)

	// idempotent
	info.ObserveFlush(ts)
	assert.NotNil(t, snapshot)
	assert.Equal(t, snapshot.State, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED)
	assert.True(t, shouldBeRemoved)
	assert.False(t, info.dirty)

	// idempotent
	info.ObserveFlush(ts + 1)
	assert.NotNil(t, snapshot)
	assert.Equal(t, snapshot.State, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED)
	assert.True(t, shouldBeRemoved)
	assert.False(t, info.dirty)
}
