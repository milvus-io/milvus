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
		WithHeader(&message.CreateSegmentMessageHeader{}).
		WithBody(&message.CreateSegmentMessageBody{
			CollectionId: 100,
			Segments: []*messagespb.CreateSegmentInfo{
				{
					PartitionId:    1,
					SegmentId:      2,
					StorageVersion: storage.StorageV1,
					MaxSegmentSize: 100,
				},
				{
					PartitionId:    2,
					SegmentId:      3,
					StorageVersion: storage.StorageV2,
					MaxSegmentSize: 200,
				},
			},
		}).
		WithVChannel("vchannel-1").
		MustBuildMutable()

	id := rmq.NewRmqID(1)
	ts := uint64(12345)
	immutableMsg := msg.WithTimeTick(ts).WithLastConfirmed(id).IntoImmutableMessage(id)

	recoverInfos := newSegmentRecoveryInfoFromCreateSegmentMessage(message.MustAsImmutableCreateSegmentMessageV2(immutableMsg))
	assert.Len(t, recoverInfos, 2)
	assert.Equal(t, int64(2), recoverInfos[0].meta.SegmentId)
	assert.Equal(t, int64(3), recoverInfos[1].meta.SegmentId)
	assert.Equal(t, int64(1), recoverInfos[0].meta.PartitionId)
	assert.Equal(t, int64(2), recoverInfos[1].meta.PartitionId)
	assert.Equal(t, storage.StorageV1, recoverInfos[0].meta.StorageVersion)
	assert.Equal(t, storage.StorageV2, recoverInfos[1].meta.StorageVersion)
	assert.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING, recoverInfos[0].meta.State)
	assert.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING, recoverInfos[1].meta.State)
	assert.Equal(t, uint64(100), recoverInfos[0].meta.Stat.MaxBinarySize)
	assert.Equal(t, uint64(200), recoverInfos[1].meta.Stat.MaxBinarySize)

	info := recoverInfos[0]
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
