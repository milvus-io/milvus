package growing

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestGrowingManagerReturnsNoBarrierForIrrelevantMessage(t *testing.T) {
	mutableMsg, err := message.NewTimeTickMessageBuilderV1().
		WithHeader(&message.TimeTickMessageHeader{}).
		WithVChannel("test-vchannel").
		WithBody(&msgpb.TimeTickMsg{}).
		BuildMutable()
	require.NoError(t, err)
	msg := mutableMsg.WithTimeTick(10).IntoImmutableMessage(nil)

	manager := NewManager(nil, nil, nil)
	result := manager.ObserveMessage(context.Background(), msg)
	assert.Nil(t, result.Meta)
	assert.Nil(t, result.Data)

	manager.metaAndData = true
	result = manager.ObserveMessage(context.Background(), msg)
	assert.Nil(t, result.Meta)
	assert.Nil(t, result.Data)
}

func TestGrowingManagerTruncateCollectionAdvancesVChannelMeta(t *testing.T) {
	manager := NewManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel: "v1",
			State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 10, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{
						Schema:             &schemapb.CollectionSchema{},
						State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
						CheckpointTimeTick: 1,
					},
				},
			},
			CheckpointTimeTick: 1,
			LatestDataVersion:  &viewpb.DataVersion{},
			GrowingSegmentMode: streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
		},
	}, nil, nil)
	mutableMsg := message.NewTruncateCollectionMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.TruncateCollectionMessageHeader{CollectionId: 1}).
		WithBody(&message.TruncateCollectionMessageBody{}).
		MustBuildMutable()
	msg := mutableMsg.WithTimeTick(10).WithLastConfirmed(walimplstest.NewTestMessageID(10)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(11))

	result := manager.ObserveMessage(context.Background(), msg)

	require.NotNil(t, result.Meta)
	vchannel := manager.vChannels()["v1"].AssignmentMeta()
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, vchannel.GetState())
	assert.Equal(t, uint64(10), vchannel.GetCheckpointTimeTick())
}

func TestGrowingManagerDoesNotFilterExistingVChannelByCollectionID(t *testing.T) {
	manager := NewManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel: "v1",
			State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 10, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: &schemapb.CollectionSchema{}, CheckpointTimeTick: 1},
				},
			},
			CheckpointTimeTick: 1,
			LatestDataVersion:  &viewpb.DataVersion{},
			GrowingSegmentMode: streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
		},
	}, nil, nil)
	msg := message.NewCreatePartitionMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&message.CreatePartitionMessageHeader{
			CollectionId: 2,
			PartitionId:  20,
		}).
		WithBody(&msgpb.CreatePartitionRequest{}).
		MustBuildMutable().
		WithTimeTick(10).
		WithLastConfirmed(walimplstest.NewTestMessageID(10)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(11))

	result := manager.ObserveMessage(context.Background(), msg)

	require.NotNil(t, result.Meta)
	assert.True(t, hasPartitionMeta(manager.vChannels()["v1"].AssignmentMeta(), 20))
}

func TestSegmentViewObserveInsertUsesMetaAndDataWatermarksSeparately(t *testing.T) {
	segment := newSegmentView(
		&streamingpb.SegmentAssignmentMeta{
			CollectionId:           1,
			PartitionId:            10,
			SegmentId:              100,
			Vchannel:               "v1",
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
			CheckpointTimeTick:     100,
			DataCheckpointTimeTick: 10,
			PersistedStorage:       &streamingpb.L1SegmentPersistedStorage{},
			Stat: &streamingpb.SegmentAssignmentStat{
				ModifiedRows:       7,
				ModifiedBinarySize: 70,
			},
		},
		0,
		0,
		false,
		writeOnlyInsertBuffer{},
		&schemapb.CollectionSchema{},
		runtimeConfig{
			metaAndData: true,
			flushPolicy: neverFlushPolicy{},
		},
	)
	assignment := &messagespb.PartitionSegmentAssignment{
		PartitionId:       10,
		Rows:              3,
		BinarySize:        30,
		SegmentAssignment: &messagespb.SegmentAssignment{SegmentId: 100},
	}
	msg := newTestInsertMessage(t, 50, assignment)

	result := segment.ObserveInsertMessageV1(context.Background(), msg, assignment)

	assert.Nil(t, result.Meta)
	require.NotNil(t, result.Data)
	assert.Equal(t, uint64(100), segment.meta.GetCheckpointTimeTick())
	assert.Equal(t, uint64(7), segment.meta.GetStat().GetModifiedRows())
	assert.Len(t, segment.pending.entries, 1)
	assert.Equal(t, uint64(50), segment.pending.DataTimeTick())

	duplicate := segment.ObserveInsertMessageV1(context.Background(), msg, assignment)
	assert.Nil(t, duplicate.Meta)
	assert.Nil(t, duplicate.Data)
	assert.Len(t, segment.pending.entries, 1)
}

func TestVChannelViewObserveDeleteUsesDataWatermarkAndBufferTail(t *testing.T) {
	vchannel := newVChannelView(
		&streamingpb.VChannelMeta{
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CheckpointTimeTick:     100,
			DataCheckpointTimeTick: 10,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 10, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: &schemapb.CollectionSchema{}, CheckpointTimeTick: 1},
				},
			},
		},
		0,
		0,
		false,
		runtimeConfig{
			metaAndData:   true,
			transformRows: 100,
		},
	)
	msg := newTestDeleteMessage(t, 50)

	result := vchannel.ObserveDeleteMessageV1(context.Background(), msg)

	require.NotNil(t, result.Data)
	assert.Len(t, vchannel.transformLogBuffer.entries, 1)
	assert.Equal(t, uint64(50), vchannel.transformLogBuffer.DataTimeTick())

	duplicate := vchannel.ObserveDeleteMessageV1(context.Background(), msg)
	assert.Nil(t, duplicate.Meta)
	assert.Nil(t, duplicate.Data)
	assert.Len(t, vchannel.transformLogBuffer.entries, 1)

	persisted := newTestDeleteMessage(t, 8)
	persistedResult := vchannel.ObserveDeleteMessageV1(context.Background(), persisted)
	assert.Nil(t, persistedResult.Meta)
	assert.Nil(t, persistedResult.Data)
	assert.Len(t, vchannel.transformLogBuffer.entries, 1)
}

func TestVChannelViewObserveCreatePartitionUsesMetaWatermark(t *testing.T) {
	vchannel := newVChannelView(
		&streamingpb.VChannelMeta{
			Vchannel:           "v1",
			State:              streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CheckpointTimeTick: 100,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 10, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: &schemapb.CollectionSchema{}, CheckpointTimeTick: 1},
				},
			},
		},
		0,
		0,
		false,
		runtimeConfig{},
	)
	msg := message.NewCreatePartitionMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&message.CreatePartitionMessageHeader{
			CollectionId: 1,
			PartitionId:  20,
		}).
		WithBody(&msgpb.CreatePartitionRequest{}).
		MustBuildMutable().
		WithTimeTick(50).
		WithLastConfirmed(walimplstest.NewTestMessageID(50)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(51))

	result := vchannel.ObserveCreatePartitionMessageV1(message.MustAsImmutableCreatePartitionMessageV1(msg))

	assert.Nil(t, result.Meta)
	assert.False(t, hasPartitionMeta(vchannel.AssignmentMeta(), 20))
}

func TestGrowingManagerDataCheckpointTimeTickUsesMinimumViewDataCheckpoint(t *testing.T) {
	manager := NewManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CheckpointTimeTick:     100,
			DataCheckpointTimeTick: 80,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 10, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: &schemapb.CollectionSchema{}, CheckpointTimeTick: 1},
				},
			},
			LatestDataVersion:  &viewpb.DataVersion{},
			GrowingSegmentMode: streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
		},
	}, map[int64]*streamingpb.SegmentAssignmentMeta{
		100: {
			CollectionId:           1,
			PartitionId:            10,
			SegmentId:              100,
			Vchannel:               "v1",
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
			CheckpointTimeTick:     100,
			DataCheckpointTimeTick: 60,
			Stat:                   &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 1},
			PersistedStorage:       &streamingpb.L1SegmentPersistedStorage{},
		},
	}, nil)

	assert.Equal(t, uint64(60), manager.DataCheckpointTimeTick())
}

type neverFlushPolicy struct{}

func (neverFlushPolicy) ShouldFlush(writeOnlyInsertBuffer, uint64) bool {
	return false
}

func newTestInsertMessage(t *testing.T, timetick uint64, assignment *messagespb.PartitionSegmentAssignment) message.ImmutableInsertMessageV1 {
	t.Helper()
	mutableMsg := message.NewInsertMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&message.InsertMessageHeader{
			CollectionId: 1,
			Partitions:   []*messagespb.PartitionSegmentAssignment{assignment},
		}).
		WithBody(&msgpb.InsertRequest{
			Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_Insert},
		}).
		MustBuildMutable()
	msg := mutableMsg.WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick + 1)))
	return message.MustAsImmutableInsertMessageV1(msg)
}

func newTestDeleteMessage(t *testing.T, timetick uint64) message.ImmutableDeleteMessageV1 {
	t.Helper()
	mutableMsg := message.NewDeleteMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&message.DeleteMessageHeader{
			CollectionId: 1,
			Rows:         1,
		}).
		WithBody(&msgpb.DeleteRequest{
			Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_Delete},
			CollectionID: 1,
			PartitionID:  10,
			PrimaryKeys:  &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}},
			Timestamps:   []uint64{timetick},
		}).
		MustBuildMutable()
	msg := mutableMsg.WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick + 1)))
	return message.MustAsImmutableDeleteMessageV1(msg)
}

func hasPartitionMeta(meta *streamingpb.VChannelMeta, partitionID int64) bool {
	for _, partition := range meta.GetCollectionInfo().GetPartitions() {
		if partition.GetPartitionId() == partitionID {
			return true
		}
	}
	return false
}
