package replicate

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	wallock "github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/lock"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/replicate/replicates"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestLocalFlushReleasesRecoveredSessionBeforeReplay(t *testing.T) {
	resource.InitForTest(t)
	ctx := context.Background()
	id := walimplstest.NewTestMessageID(1)
	begin := message.NewBeginTxnMessageBuilderV2().WithVChannel("target_v1").WithHeader(&message.BeginTxnMessageHeader{}).WithBody(&message.BeginTxnMessageBody{}).MustBuildMutable().
		WithTxnContext(message.TxnContext{TxnID: 1, Keepalive: message.TxnKeepaliveInfinite}).WithTimeTick(1).WithLastConfirmedUseMessageID().
		WithReplicateHeader(&message.ReplicateHeader{ClusterID: "source", VChannel: "source_v1", TimeTick: 100, MessageID: id, LastConfirmedMessageID: id})
	buffer := utility.NewTxnBuffer(mlog.With(), metricsutil.NewScanMetrics(types.PChannelInfo{}).NewScannerMetrics())
	buffer.HandleImmutableMessages([]message.ImmutableMessage{begin.IntoImmutableMessage(id)}, 1)
	manager := txn.NewTxnManager(types.PChannelInfo{Name: "target"}, buffer.GetUncommittedMessageBuilder())
	session, err := manager.GetSessionOfTxn(1)
	require.NoError(t, err)
	cleaned := 0
	session.RegisterCleanup(func() { cleaned++ }, 1)
	rm, err := replicates.RecoverReplicateManager(&replicates.ReplicateManagerRecoverParam{
		CurrentClusterID: "target", ChannelInfo: types.PChannelInfo{Name: "target"},
		InitialRecoverSnapshot: &recovery.RecoverySnapshot{
			Checkpoint: &utility.WALCheckpoint{MessageID: id, TimeTick: 1}, TxnBuffer: buffer,
			PChannelControl: &streamingpb.PChannelRecoveryControlMeta{ReplicateCheckpoint: (&utility.ReplicateCheckpoint{ClusterID: "source", PChannel: "source", MessageID: id}).IntoProto(), ReplicateConfig: &commonpb.ReplicateConfiguration{
				Clusters:             []*commonpb.MilvusCluster{{ClusterId: "source", Pchannels: []string{"source"}}, {ClusterId: "target", Pchannels: []string{"target"}}},
				CrossClusterTopology: []*commonpb.CrossClusterTopology{{SourceClusterId: "source", TargetClusterId: "target"}},
			}},
		},
	})
	require.NoError(t, err)
	impl := &replicateInterceptor{replicateManager: rm}
	locker := wallock.NewInterceptorBuilder().Build(&interceptors.InterceptorBuildParam{
		ChannelInfo: types.PChannelInfo{Name: "target"}, TxnManager: manager,
	})
	appendMessage := func(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
		return locker.DoAppend(ctx, msg, func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			return impl.DoAppend(ctx, msg, appendOp)
		})
	}
	// Replayed old DDL is ignored before append. Merely taking an exclusive lock
	// must not discard either the session or the replication prefix.
	oldDrop := message.NewDropCollectionMessageBuilderV1().WithVChannel("target_v1").
		WithHeader(&message.DropCollectionMessageHeader{}).WithBody(&msgpb.DropCollectionRequest{}).
		MustBuildMutable().WithReplicateHeader(&message.ReplicateHeader{
		ClusterID: "source", VChannel: "source_v1", MessageID: id, LastConfirmedMessageID: id,
	})
	_, err = appendMessage(ctx, oldDrop, func(context.Context, message.MutableMessage) (message.MessageID, error) {
		t.Fatal("old DDL must be deduplicated before append")
		return nil, nil
	})
	require.True(t, status.AsStreamingError(err).IsIgnoredOperation())
	require.Zero(t, cleaned)
	require.Equal(t, message.TxnStateInFlight, session.State())
	flush := message.NewManualFlushMessageBuilderV2().WithVChannel("target_v1").WithUnreplicable().WithHeader(&message.ManualFlushMessageHeader{}).WithBody(&message.ManualFlushMessageBody{}).MustBuildMutable()
	_, err = appendMessage(ctx, flush, func(context.Context, message.MutableMessage) (message.MessageID, error) {
		require.Zero(t, cleaned, "references stay held until the abort record is durable")
		return nil, context.Canceled
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, cleaned)
	require.Equal(t, message.TxnStateInFlight, session.State())
	_, err = appendMessage(ctx, flush, func(context.Context, message.MutableMessage) (message.MessageID, error) {
		require.Zero(t, cleaned)
		return walimplstest.NewTestMessageID(2), nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, cleaned)
	require.Equal(t, message.TxnStateRollbacked, session.State())
	select {
	case <-manager.RecoverDone():
	default:
		t.Fatal("flush must release the recovered transaction barrier")
	}
	// Same source TxnID can now enter with a fresh local session and references.
	_, err = appendMessage(ctx, message.CloneMutableMessage(begin), func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		replayed, err := manager.BeginNewTxn(ctx, message.MustAsMutableBeginTxnMessageV2(msg))
		require.NoError(t, err)
		require.NotSame(t, session, replayed)
		replayed.RegisterCleanup(func() { cleaned++ }, 1)
		return walimplstest.NewTestMessageID(3), nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, cleaned)
	manager.FailTxnAtVChannel("")
	require.Equal(t, 2, cleaned)
	require.NoError(t, manager.GracefulClose(ctx))
}
