package shards

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/mock_wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/policy"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/stats"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/utils"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

func TestShardManager(t *testing.T) {
	paramtable.Init()
	resource.InitForTest(t)
	channel := types.PChannelInfo{
		Name: "test_channel",
		Term: 1,
	}
	w := mock_wal.NewMockWAL(t)
	w.EXPECT().Available().RunAndReturn(func() <-chan struct{} {
		return make(chan struct{})
	}).Maybe()
	w.EXPECT().Append(mock.Anything, mock.Anything).Return(&types.AppendResult{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1000,
	}, nil)
	f := syncutil.NewFuture[wal.WAL]()
	f.Set(w)

	m := RecoverShardManager(&ShardManagerRecoverParam{
		ChannelInfo: channel,
		WAL:         f,
		InitialRecoverSnapshot: &recovery.RecoverySnapshot{
			VChannels: map[string]*streamingpb.VChannelMeta{
				"v1": {
					Vchannel: "v1",
					State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
					CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
						CollectionId: 1,
						Partitions: []*streamingpb.PartitionInfoOfVChannel{
							{PartitionId: 2},
							{PartitionId: 3},
						},
					},
				},
				"v2": {
					Vchannel: "v2",
					State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
					CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
						CollectionId: 4,
						Partitions: []*streamingpb.PartitionInfoOfVChannel{
							{PartitionId: 5},
							{PartitionId: 6},
						},
					},
				},
			},
			SegmentAssignments: map[int64]*streamingpb.SegmentAssignmentMeta{
				1001: {
					CollectionId:   1,
					PartitionId:    2,
					SegmentId:      1001,
					State:          streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
					StorageVersion: 2,
					Stat: &streamingpb.SegmentAssignmentStat{
						MaxBinarySize:         100,
						InsertedBinarySize:    50,
						CreateSegmentTimeTick: 101,
					},
				},
				1002: {
					CollectionId:   1,
					PartitionId:    3,
					SegmentId:      1002,
					State:          streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
					StorageVersion: 2,
					Stat: &streamingpb.SegmentAssignmentStat{
						MaxBinarySize:         100,
						InsertedBinarySize:    0,
						CreateSegmentTimeTick: 100,
					},
				},
				1013: {
					CollectionId:   4,
					PartitionId:    5,
					SegmentId:      1013,
					State:          streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
					StorageVersion: 2,
					Stat: &streamingpb.SegmentAssignmentStat{
						MaxBinarySize:         100,
						InsertedBinarySize:    0,
						CreateSegmentTimeTick: 100,
					},
				},
			},
			Checkpoint: &recovery.WALCheckpoint{
				TimeTick: 300,
			},
		},
		TxnManager: &mockedTxnManager{},
	})
	assert.Equal(t, channel, m.Channel())

	// Test Checkers
	err := m.CheckIfCollectionCanBeCreated(1)
	assert.ErrorIs(t, err, ErrCollectionExists)
	err = m.CheckIfCollectionCanBeCreated(3)
	assert.NoError(t, err)

	err = m.CheckIfCollectionExists(3)
	assert.ErrorIs(t, err, ErrCollectionNotFound)
	err = m.CheckIfCollectionExists(1)
	assert.NoError(t, err)

	err = m.CheckIfPartitionCanBeCreated(1, 2)
	assert.ErrorIs(t, err, ErrPartitionExists)
	err = m.CheckIfPartitionCanBeCreated(3, 9)
	assert.ErrorIs(t, err, ErrCollectionNotFound)
	err = m.CheckIfPartitionCanBeCreated(1, 7)
	assert.NoError(t, err)
	err = m.CheckIfPartitionExists(1, 7)
	assert.ErrorIs(t, err, ErrPartitionNotFound)
	err = m.CheckIfPartitionExists(1, 2)
	assert.NoError(t, err)

	err = m.CheckIfSegmentCanBeCreated(1, 2, 1001)
	assert.ErrorIs(t, err, ErrSegmentExists)
	err = m.CheckIfSegmentCanBeCreated(3, 2, 1001)
	assert.ErrorIs(t, err, ErrCollectionNotFound)
	err = m.CheckIfSegmentCanBeCreated(1, 4, 1001)
	assert.ErrorIs(t, err, ErrPartitionNotFound)
	err = m.CheckIfSegmentCanBeCreated(1, 2, 1003)
	assert.NoError(t, err)

	err = m.CheckIfSegmentCanBeFlushed(1, 2, 1001)
	assert.ErrorIs(t, err, ErrSegmentOnGrowing)
	err = m.CheckIfSegmentCanBeFlushed(1, 2, 1003)
	assert.ErrorIs(t, err, ErrSegmentNotFound)
	err = m.CheckIfSegmentCanBeFlushed(3, 8, 1001)
	assert.ErrorIs(t, err, ErrCollectionNotFound)
	err = m.CheckIfSegmentCanBeFlushed(1, 7, 1001)
	assert.ErrorIs(t, err, ErrPartitionNotFound)

	// Test Create and Drop
	createCollectionMsg := message.NewCreateCollectionMessageBuilderV1().
		WithVChannel("v3").
		WithHeader(&message.CreateCollectionMessageHeader{
			CollectionId: 7,
			PartitionIds: []int64{8, 9},
		}).
		WithBody(&msgpb.CreateCollectionRequest{}).
		MustBuildMutable().
		WithTimeTick(400).
		WithLastConfirmedUseMessageID().
		IntoImmutableMessage(rmq.NewRmqID(1))
	m.CreateCollection(message.MustAsImmutableCreateCollectionMessageV1(createCollectionMsg))
	assert.NoError(t, m.CheckIfCollectionExists(7))
	assert.NoError(t, m.CheckIfPartitionExists(7, 8))
	assert.NoError(t, m.CheckIfPartitionExists(7, 9))

	createPartitionMsg := message.NewCreatePartitionMessageBuilderV1().
		WithVChannel("v3").
		WithHeader(&message.CreatePartitionMessageHeader{
			CollectionId: 7,
			PartitionId:  10,
		}).
		WithBody(&msgpb.CreatePartitionRequest{}).
		MustBuildMutable().
		WithTimeTick(500).
		WithLastConfirmedUseMessageID().
		IntoImmutableMessage(rmq.NewRmqID(2))
	m.CreatePartition(message.MustAsImmutableCreatePartitionMessageV1(createPartitionMsg))
	assert.NoError(t, m.CheckIfPartitionExists(7, 10))

	createSegmentMsg := message.NewCreateSegmentMessageBuilderV2().
		WithVChannel("v3").
		WithHeader(&message.CreateSegmentMessageHeader{
			CollectionId:   7,
			PartitionId:    10,
			SegmentId:      1003,
			StorageVersion: 2,
			MaxSegmentSize: 150,
		}).
		WithBody(&message.CreateSegmentMessageBody{}).
		MustBuildMutable().
		WithTimeTick(600).
		WithLastConfirmedUseMessageID().
		IntoImmutableMessage(rmq.NewRmqID(3))
	m.partitionManagers[10].onAllocating = make(chan struct{})
	ch, err := m.WaitUntilGrowingSegmentReady(7, 10)
	assert.NoError(t, err)
	select {
	case <-time.After(10 * time.Millisecond):
	case <-ch:
		t.Error("segment should not be ready")
	}
	m.CreateSegment(message.MustAsImmutableCreateSegmentMessageV2(createSegmentMsg))
	assert.ErrorIs(t, m.CheckIfSegmentCanBeFlushed(7, 10, 1003), ErrSegmentOnGrowing)
	<-ch
	ch, err = m.WaitUntilGrowingSegmentReady(7, 10)
	assert.NoError(t, err)
	<-ch

	flushSegmentMsg := message.NewFlushMessageBuilderV2().
		WithVChannel("v3").
		WithHeader(&message.FlushMessageHeader{
			CollectionId: 7,
			PartitionId:  10,
			SegmentId:    1003,
		}).
		WithBody(&message.FlushMessageBody{}).
		MustBuildMutable().
		WithTimeTick(700).
		WithLastConfirmedUseMessageID().
		IntoImmutableMessage(rmq.NewRmqID(4))
	m.AsyncFlushSegment(utils.SealSegmentSignal{
		SegmentBelongs: utils.SegmentBelongs{
			PartitionID: 10,
			SegmentID:   1003,
		},
		SealPolicy: policy.PolicyCapacity(),
	})
	m.FlushSegment(message.MustAsImmutableFlushMessageV2(flushSegmentMsg))
	assert.ErrorIs(t, m.CheckIfSegmentCanBeFlushed(7, 10, 1003), ErrSegmentNotFound)

	dropPartitionMsg := message.NewDropPartitionMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&message.DropPartitionMessageHeader{
			CollectionId: 1,
			PartitionId:  2,
		}).
		WithBody(&msgpb.DropPartitionRequest{}).
		MustBuildMutable().
		WithTimeTick(600).
		WithLastConfirmedUseMessageID().
		IntoImmutableMessage(rmq.NewRmqID(7))
	m.DropPartition(message.MustAsImmutableDropPartitionMessageV1(dropPartitionMsg))
	assert.ErrorIs(t, m.CheckIfPartitionExists(1, 2), ErrPartitionNotFound)

	dropCollectionMsg := message.NewDropCollectionMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&message.DropCollectionMessageHeader{
			CollectionId: 1,
		}).
		WithBody(&msgpb.DropCollectionRequest{}).
		MustBuildMutable().
		WithTimeTick(700).
		WithLastConfirmedUseMessageID().
		IntoImmutableMessage(rmq.NewRmqID(8))
	m.DropCollection(message.MustAsImmutableDropCollectionMessageV1(dropCollectionMsg))
	assert.ErrorIs(t, m.checkIfCollectionExists(1), ErrCollectionNotFound)

	result, err := m.AssignSegment(&AssignSegmentRequest{
		CollectionID: 4,
		PartitionID:  5,
		TimeTick:     800,
		InsertMetrics: stats.InsertMetrics{
			Rows:       1,
			BinarySize: 20,
		},
	})
	assert.NoError(t, err)
	result.Ack()
	assert.Equal(t, result.SegmentID, int64(1013))

	segmentIDs, err := m.FlushAndFenceSegmentAllocUntil(4, 1000)
	assert.NoError(t, err)
	assert.Equal(t, len(segmentIDs), 1)
	assert.Equal(t, segmentIDs[0], int64(1013))
	m.Close()
}
