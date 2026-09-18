package redo

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

type testTxnManager struct{}

func (m *testTxnManager) RecoverDone() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

func TestRedoInterceptorWaitUntilGrowingSegmentReadyReturnsUnrecoverableForDroppedCollectionOrPartition(t *testing.T) {
	paramtable.Init()
	resource.InitForTest(t)

	tests := []struct {
		name       string
		vchannels  map[string]*streamingpb.VChannelMeta
		collection int64
		partition  int64
	}{
		{
			name:       "collection not found",
			vchannels:  map[string]*streamingpb.VChannelMeta{},
			collection: 1,
			partition:  2,
		},
		{
			name: "partition not found",
			vchannels: map[string]*streamingpb.VChannelMeta{
				"test-vchannel": {
					Vchannel: "test-vchannel",
					State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
					CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
						CollectionId: 1,
						Partitions: []*streamingpb.PartitionInfoOfVChannel{
							{PartitionId: 2},
						},
					},
				},
			},
			collection: 1,
			partition:  3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shardManager := shards.RecoverShardManager(&shards.ShardManagerRecoverParam{
				ChannelInfo: types.PChannelInfo{Name: "test-channel", Term: 1},
				WAL:         syncutil.NewFuture[wal.WAL](),
				InitialRecoverSnapshot: &recovery.RecoverySnapshot{
					VChannels:          tt.vchannels,
					SegmentAssignments: map[int64]*streamingpb.SegmentAssignmentMeta{},
					Checkpoint:         &recovery.WALCheckpoint{TimeTick: 100},
				},
				TxnManager: &testTxnManager{},
			})
			defer shardManager.Close()

			interceptor := NewInterceptorBuilder().Build(&interceptors.InterceptorBuildParam{
				ShardManager: shardManager,
			})
			defer interceptor.Close()

			msg := message.NewInsertMessageBuilderV1().
				WithVChannel("test-vchannel").
				WithHeader(&messagespb.InsertMessageHeader{
					CollectionId: tt.collection,
					Partitions: []*messagespb.PartitionSegmentAssignment{
						{PartitionId: tt.partition, Rows: 1},
					},
				}).
				WithBody(&msgpb.InsertRequest{}).
				MustBuildMutable()

			msgID, err := interceptor.DoAppend(context.Background(), msg, func(context.Context, message.MutableMessage) (message.MessageID, error) {
				t.Fatal("append should not be called when collection or partition is already dropped")
				return nil, nil
			})
			require.Nil(t, msgID)
			require.Error(t, err)
			require.True(t, status.AsStreamingError(err).IsUnrecoverable())
		})
	}
}

// TestRedoInterceptorReportsShardFencedWhenTheInsertRacesTheFence: the redo
// loop sits outside the vchannel lock, so a fence can land between one
// iteration and the next. The fence tears the source's registration down, and
// the partition lookup then fails with ErrCollectionNotFound -- the same
// answer a vchannel this pchannel never held gives. The client must see
// SHARD_FENCED for it, the answer the shard interceptor's own admission gives
// once the message reaches it: the write belongs to a shard that exists and
// the route is one routing commit behind, not a terminal error.
func TestRedoInterceptorReportsShardFencedWhenTheInsertRacesTheFence(t *testing.T) {
	paramtable.Init()
	resource.InitForTest(t)

	shardManager := shards.RecoverShardManager(&shards.ShardManagerRecoverParam{
		ChannelInfo: types.PChannelInfo{Name: "test-channel", Term: 1},
		WAL:         syncutil.NewFuture[wal.WAL](),
		InitialRecoverSnapshot: &recovery.RecoverySnapshot{
			VChannels: map[string]*streamingpb.VChannelMeta{
				"test-vchannel": {
					Vchannel: "test-vchannel",
					State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
					CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
						CollectionId: 1,
						Partitions:   []*streamingpb.PartitionInfoOfVChannel{{PartitionId: 2}},
					},
				},
			},
			SegmentAssignments: map[int64]*streamingpb.SegmentAssignmentMeta{},
			Checkpoint:         &recovery.WALCheckpoint{TimeTick: 100},
		},
		TxnManager: &testTxnManager{},
	})
	defer shardManager.Close()

	// The fence, as the shard interceptor installs it before appending the
	// fence record: the registration is gone, the name-keyed tombstone stays.
	fence := message.NewSplitShardMessageBuilderV2().
		WithVChannel("test-vchannel").
		WithHeader(&message.SplitShardMessageHeader{
			CollectionId:    1,
			SplitTaskId:     7,
			SourceVchannel:  "test-vchannel",
			TargetVchannels: []string{"test-vchannel-target1", "test-vchannel-target2"},
		}).
		WithBody(&message.SplitShardMessageBody{}).
		MustBuildMutable().
		WithTimeTick(200).
		WithLastConfirmedUseMessageID()
	shardManager.SplitShard(message.MustAsMutableSplitShardMessageV2(fence))

	interceptor := NewInterceptorBuilder().Build(&interceptors.InterceptorBuildParam{
		ShardManager: shardManager,
	})
	defer interceptor.Close()

	msg := message.NewInsertMessageBuilderV1().
		WithVChannel("test-vchannel").
		WithHeader(&messagespb.InsertMessageHeader{
			CollectionId: 1,
			Partitions: []*messagespb.PartitionSegmentAssignment{
				{PartitionId: 2, Rows: 1},
			},
		}).
		WithBody(&msgpb.InsertRequest{}).
		MustBuildMutable()

	msgID, err := interceptor.DoAppend(context.Background(), msg, func(context.Context, message.MutableMessage) (message.MessageID, error) {
		t.Fatal("append must not be called on a fenced vchannel")
		return nil, nil
	})
	require.Nil(t, msgID)
	require.Error(t, err)
	assert.True(t, status.AsStreamingError(err).IsShardFenced(), "an insert racing the fence is SHARD_FENCED, not UNRECOVERABLE: %v", err)
	assert.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_SHARD_FENCED, status.AsStreamingError(err).Code)
}
