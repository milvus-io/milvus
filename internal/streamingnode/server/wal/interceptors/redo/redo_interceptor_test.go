package redo

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
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
		vchannels  map[string]moduleapi.VChannelWritePathRecoveryState
		collection int64
		partition  int64
	}{
		{
			name:       "collection not found",
			vchannels:  map[string]moduleapi.VChannelWritePathRecoveryState{},
			collection: 1,
			partition:  2,
		},
		{
			name: "partition not found",
			vchannels: map[string]moduleapi.VChannelWritePathRecoveryState{
				"test-vchannel": {
					VChannel:     "test-vchannel",
					CollectionID: 1,
					PartitionIDs: []int64{2},
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
					WritePathRecovery: &moduleapi.WritePathRecoveryModuleSnapshot{
						VChannels:       tt.vchannels,
						GrowingSegments: map[int64]moduleapi.SegmentWritePathRecoveryState{},
					},
					Checkpoint: &recovery.WALCheckpoint{TimeTick: 100},
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

func TestRedoInterceptorReturnsShardFencedForASplitSource(t *testing.T) {
	paramtable.Init()
	resource.InitForTest(t)

	shardManager := shards.RecoverShardManager(&shards.ShardManagerRecoverParam{
		ChannelInfo: types.PChannelInfo{Name: "test-channel", Term: 1},
		WAL:         syncutil.NewFuture[wal.WAL](),
		InitialRecoverSnapshot: &recovery.RecoverySnapshot{
			WritePathRecovery: &moduleapi.WritePathRecoveryModuleSnapshot{
				VChannels: map[string]moduleapi.VChannelWritePathRecoveryState{
					"test-vchannel": {
						VChannel:     "test-vchannel",
						CollectionID: 1,
						PartitionIDs: []int64{2},
						SplitTaskID:  7,
					},
				},
				GrowingSegments: map[int64]moduleapi.SegmentWritePathRecoveryState{},
			},
			Checkpoint: &recovery.WALCheckpoint{TimeTick: 100},
		},
		TxnManager: &testTxnManager{},
	})
	defer shardManager.Close()

	interceptor := NewInterceptorBuilder().Build(&interceptors.InterceptorBuildParam{
		ShardManager: shardManager,
	})
	msg := message.NewInsertMessageBuilderV1().
		WithVChannel("test-vchannel").
		WithHeader(&messagespb.InsertMessageHeader{
			CollectionId: 1,
			Partitions:   []*messagespb.PartitionSegmentAssignment{{PartitionId: 2, Rows: 1, BinarySize: 100}},
		}).
		WithBody(&msgpb.InsertRequest{}).
		MustBuildMutable()

	appended := false
	_, err := interceptor.DoAppend(context.Background(), msg, func(context.Context, message.MutableMessage) (message.MessageID, error) {
		appended = true
		return nil, nil
	})
	// The insert is refused before it waits for a segment that is never
	// created, and before it is appended. The refusal is not terminal: the
	// caller retries the rows on the new shards.
	require.Error(t, err)
	require.False(t, appended)
	streamingErr := status.AsStreamingError(err)
	require.True(t, streamingErr.IsShardFenced())
	require.False(t, streamingErr.IsUnrecoverable())
}

// A body or the commit of a transaction that a shard split ended finds no
// session; the interceptor answers it with the fence error, so the writer
// places the whole transaction on the new shards. Everything else keeps its
// own error.
func TestRedoInterceptorAnswersAnEndedTransactionOfASplitSourceAsFenced(t *testing.T) {
	paramtable.Init()
	resource.InitForTest(t)

	newManager := func(name string, splitTaskID int64) shards.ShardManager {
		m := shards.RecoverShardManager(&shards.ShardManagerRecoverParam{
			ChannelInfo: types.PChannelInfo{Name: name, Term: 1},
			WAL:         syncutil.NewFuture[wal.WAL](),
			InitialRecoverSnapshot: &recovery.RecoverySnapshot{
				WritePathRecovery: &moduleapi.WritePathRecoveryModuleSnapshot{
					VChannels: map[string]moduleapi.VChannelWritePathRecoveryState{
						"test-vchannel": {VChannel: "test-vchannel", CollectionID: 1, PartitionIDs: []int64{2}, SplitTaskID: splitTaskID},
					},
					GrowingSegments: map[int64]moduleapi.SegmentWritePathRecoveryState{},
				},
				Checkpoint: &recovery.WALCheckpoint{TimeTick: 100},
			},
			TxnManager: &testTxnManager{},
		})
		t.Cleanup(m.Close)
		return m
	}
	fenced := NewInterceptorBuilder().Build(&interceptors.InterceptorBuildParam{ShardManager: newManager("fenced-channel", 7)})
	unfenced := NewInterceptorBuilder().Build(&interceptors.InterceptorBuildParam{ShardManager: newManager("unfenced-channel", 0)})

	txnCtx := message.TxnContext{TxnID: 11, Keepalive: message.TxnKeepaliveInfinite}
	deleteBody := message.NewDeleteMessageBuilderV1().WithVChannel("test-vchannel").
		WithHeader(&messagespb.DeleteMessageHeader{CollectionId: 1, Rows: 1}).
		WithBody(&msgpb.DeleteRequest{}).MustBuildMutable().WithTxnContext(txnCtx)
	commit := message.NewCommitTxnMessageBuilderV2().WithVChannel("test-vchannel").
		WithHeader(&message.CommitTxnMessageHeader{}).WithBody(&message.CommitTxnMessageBody{}).
		MustBuildMutable().WithTxnContext(txnCtx)
	rollback := message.NewRollbackTxnMessageBuilderV2().WithVChannel("test-vchannel").
		WithHeader(&message.RollbackTxnMessageHeader{}).WithBody(&message.RollbackTxnMessageBody{}).
		MustBuildMutable().WithTxnContext(txnCtx)
	autoCommit := message.NewDeleteMessageBuilderV1().WithVChannel("test-vchannel").
		WithHeader(&messagespb.DeleteMessageHeader{CollectionId: 1, Rows: 1}).
		WithBody(&msgpb.DeleteRequest{}).MustBuildMutable()

	gone := func(context.Context, message.MutableMessage) (message.MessageID, error) {
		return nil, status.NewTransactionExpired("txn 11 not found in manager")
	}
	other := func(context.Context, message.MutableMessage) (message.MessageID, error) {
		return nil, status.NewInvalidArgument("something else")
	}

	for _, msg := range []message.MutableMessage{deleteBody, commit} {
		_, err := fenced.DoAppend(context.Background(), msg, gone)
		streamingErr := status.AsStreamingError(err)
		require.True(t, streamingErr.IsShardFenced(), msg.MessageType().String())
		require.False(t, streamingErr.IsUnrecoverable())

		// The vchannel is not fenced: the transaction error stays.
		_, err = unfenced.DoAppend(context.Background(), msg, gone)
		require.True(t, status.AsStreamingError(err).IsTxnExpired())
		// Another error stays.
		_, err = fenced.DoAppend(context.Background(), msg, other)
		require.True(t, status.AsStreamingError(err).IsInvalidArgument())
	}
	// A rollback has nothing to place elsewhere, and a message outside a
	// transaction keeps its error.
	begin := message.NewBeginTxnMessageBuilderV2().WithVChannel("test-vchannel").
		WithHeader(&message.BeginTxnMessageHeader{}).WithBody(&message.BeginTxnMessageBody{}).
		MustBuildMutable().WithTxnContext(txnCtx)
	for _, msg := range []message.MutableMessage{begin, rollback, autoCommit} {
		_, err := fenced.DoAppend(context.Background(), msg, gone)
		require.True(t, status.AsStreamingError(err).IsTxnExpired())
	}
	// Success passes through.
	_, err := fenced.DoAppend(context.Background(), commit, func(context.Context, message.MutableMessage) (message.MessageID, error) {
		return nil, nil
	})
	require.NoError(t, err)
}
