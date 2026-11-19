package adaptor_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/lock"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/redo"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/replicate"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/registry"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

const (
	splitTxnSource  = "split-txn_1v0"
	splitTxnTarget1 = "split-txn-t1_1v1"
	splitTxnTarget2 = "split-txn-t2_1v2"
)

// A transaction on the source of a shard split, through the real interceptor
// chain of a WAL (redo, lock, replicate, time tick, shard):
//
//   - a transaction committed before the fence keeps its time tick below it;
//   - a transaction open at the fence cannot commit after it: its commit and
//     every later body get the fence error, so none of its rows becomes
//     visible above T_switch, where the source is no longer read;
//   - a transaction cannot be begun on the source after the fence, and the
//     refused Begin leaves no session to expire.
func TestSplitShardFenceEndsTheTransactionsOfTheSource(t *testing.T) {
	walimplstest.Reset()
	initResourceForTest(t)
	message.RegisterDefaultWALName(message.WALNameTest)
	opener, err := registry.MustGetBuilder(message.WALNameTest,
		redo.NewInterceptorBuilder(), lock.NewInterceptorBuilder(),
		replicate.NewInterceptorBuilder(), timetick.NewInterceptorBuilder(), shard.NewInterceptorBuilder(),
	).Build()
	require.NoError(t, err)
	defer opener.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	channel := types.PChannelInfo{Name: "split-txn", Term: 1, AccessMode: types.AccessModeRW}
	w, err := opener.Open(ctx, &wal.OpenOption{Channel: channel, DisableFlusher: true})
	require.NoError(t, err)
	defer w.Close()

	_, err = w.Append(ctx, message.NewCreateCollectionMessageBuilderV1().WithVChannel(splitTxnSource).
		WithHeader(&message.CreateCollectionMessageHeader{CollectionId: 1, PartitionIds: []int64{2}}).
		WithBody(&msgpb.CreateCollectionRequest{CollectionID: 1, CollectionSchema: splitTxnSchema()}).MustBuildMutable())
	require.NoError(t, err)

	// Committed before the fence.
	committed := beginSplitTxn(ctx, t, w)
	_, err = w.Append(ctx, splitTxnDelete().WithTxnContext(*committed))
	require.NoError(t, err)
	commitResult, err := w.Append(ctx, splitTxnCommit(*committed))
	require.NoError(t, err)

	// Open at the fence: one body is in, the commit has not been sent.
	open := beginSplitTxn(ctx, t, w)
	_, err = w.Append(ctx, splitTxnDelete().WithTxnContext(*open))
	require.NoError(t, err)

	fence, err := w.Append(ctx, splitTxnFence())
	require.NoError(t, err)
	require.Greater(t, fence.TimeTick, commitResult.TimeTick)

	// The open transaction cannot commit, and takes no more body.
	_, err = w.Append(ctx, splitTxnCommit(*open))
	requireSplitTxnFenced(t, err)
	_, err = w.Append(ctx, splitTxnDelete().WithTxnContext(*open))
	requireSplitTxnFenced(t, err)

	// No transaction can begin on the source any more.
	_, err = w.Append(ctx, splitTxnBegin())
	requireSplitTxnFenced(t, err)

	// A rollback of the transaction the fence ended is not a fence refusal:
	// there is nothing to place elsewhere.
	_, err = w.Append(ctx, message.NewRollbackTxnMessageBuilderV2().WithVChannel(splitTxnSource).
		WithHeader(&message.RollbackTxnMessageHeader{}).WithBody(&message.RollbackTxnMessageBody{}).
		MustBuildMutable().WithTxnContext(*open))
	require.Error(t, err)
	require.False(t, status.AsStreamingError(err).IsShardFenced())
	require.True(t, status.AsStreamingError(err).IsTxnUnavilable())
}

func splitTxnSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name:   "split_txn",
		Fields: []*schemapb.FieldSchema{{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}},
	}
}

func splitTxnBegin() message.MutableMessage {
	return message.NewBeginTxnMessageBuilderV2().WithVChannel(splitTxnSource).
		WithHeader(&message.BeginTxnMessageHeader{KeepaliveMilliseconds: 60_000}).
		WithBody(&message.BeginTxnMessageBody{}).MustBuildMutable()
}

func beginSplitTxn(ctx context.Context, t *testing.T, w wal.WAL) *message.TxnContext {
	t.Helper()
	result, err := w.Append(ctx, splitTxnBegin())
	require.NoError(t, err)
	require.NotNil(t, result.TxnCtx)
	return result.TxnCtx
}

func splitTxnDelete() message.MutableMessage {
	return message.NewDeleteMessageBuilderV1().WithVChannel(splitTxnSource).
		WithHeader(&messagespb.DeleteMessageHeader{CollectionId: 1, Rows: 1}).
		WithBody(&msgpb.DeleteRequest{}).MustBuildMutable()
}

func splitTxnCommit(txnCtx message.TxnContext) message.MutableMessage {
	return message.NewCommitTxnMessageBuilderV2().WithVChannel(splitTxnSource).
		WithHeader(&message.CommitTxnMessageHeader{}).WithBody(&message.CommitTxnMessageBody{}).
		MustBuildMutable().WithTxnContext(txnCtx)
}

func splitTxnFence() message.MutableMessage {
	return message.NewSplitShardMessageBuilderV2().WithVChannel(splitTxnSource).
		WithHeader(&message.SplitShardMessageHeader{
			CollectionId:    1,
			SplitTaskId:     7,
			SourceVchannel:  splitTxnSource,
			TargetVchannels: []string{splitTxnTarget1, splitTxnTarget2},
		}).
		WithBody(&message.SplitShardMessageBody{
			GenesisHeader: &message.CreateCollectionMessageHeader{CollectionId: 1, PartitionIds: []int64{2}},
			Genesis:       &message.CreateCollectionRequest{CollectionID: 1, CollectionSchema: splitTxnSchema()},
			Routing:       &message.SplitShardRouting{RoutingModulus: 2},
		}).MustBuildMutable()
}

func requireSplitTxnFenced(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	streamingErr := status.AsStreamingError(err)
	require.True(t, streamingErr.IsShardFenced(), err.Error())
	require.False(t, streamingErr.IsUnrecoverable())
}
