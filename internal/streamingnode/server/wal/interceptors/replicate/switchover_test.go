package replicate

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/lock"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/replicate/replicates"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/replicateutil"
)

func TestReplicatedSwitchoverReplay(t *testing.T) {
	ctx := t.Context()
	id := walimplstest.NewTestMessageID(57)
	clusters := []*commonpb.MilvusCluster{
		{ClusterId: "A", Pchannels: []string{"a0"}},
		{ClusterId: "B", Pchannels: []string{"b0"}},
	}
	config := func(source, target string) *commonpb.ReplicateConfiguration {
		return &commonpb.ReplicateConfiguration{
			Clusters:             clusters,
			CrossClusterTopology: []*commonpb.CrossClusterTopology{{SourceClusterId: source, TargetClusterId: target}},
		}
	}
	recoverChannel := func(t *testing.T, cfg *commonpb.ReplicateConfiguration) *replicateInterceptor {
		t.Helper()
		rm, err := replicates.RecoverReplicateManager(&replicates.ReplicateManagerRecoverParam{
			CurrentClusterID: "B",
			ChannelInfo:      types.PChannelInfo{Name: "b0"},
			InitialRecoverSnapshot: &recovery.RecoverySnapshot{
				PChannelControl: &streamingpb.PChannelRecoveryControlMeta{ReplicateConfig: cfg},
			},
		})
		require.NoError(t, err)
		return &replicateInterceptor{replicateManager: rm}
	}
	change := func(cfg *commonpb.ReplicateConfiguration) message.MutableMessage {
		broadcast := message.NewAlterReplicateConfigMessageBuilderV2().
			WithHeader(&message.AlterReplicateConfigMessageHeader{ReplicateConfiguration: cfg}).
			WithBody(&message.AlterReplicateConfigMessageBody{}).
			WithClusterLevelBroadcast(message.ClusterChannels{
				Channels: []string{"b0"}, ControlChannel: funcutil.GetControlChannel("b0"),
			}).
			MustBuildBroadcast()
		broadcast.WithBroadcastID(1)
		return broadcast.SplitIntoMutableMessage()[0]
	}
	promotion := func() message.MutableMessage {
		return change(config("B", "A")).WithReplicateHeader(&message.ReplicateHeader{
			ClusterID: "A", VChannel: funcutil.GetControlChannel("a0"), MessageID: id, LastConfirmedMessageID: id, TimeTick: 100,
		})
	}
	appendOK := func(context.Context, message.MutableMessage) (message.MessageID, error) {
		return walimplstest.NewTestMessageID(47), nil
	}
	startSecondary := func(t *testing.T) *replicateInterceptor {
		t.Helper()
		receiver := recoverChannel(t, nil)
		_, err := receiver.DoAppend(ctx, change(config("A", "B")), appendOK)
		require.NoError(t, err)
		return receiver
	}

	t.Run("concurrent delivery", func(t *testing.T) {
		receiver := startSecondary(t)
		walLock := lock.NewInterceptorBuilder().Build(&interceptors.InterceptorBuildParam{
			ChannelInfo: types.PChannelInfo{Name: "b0"},
			TxnManager:  new(txn.TxnManager),
		})
		writes := 0
		appendPromotion := func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			return receiver.DoAppend(ctx, msg, func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
				writes++
				return appendOK(ctx, msg)
			})
		}
		start := make(chan struct{})
		results := make(chan error, 2)
		for range 2 {
			go func() {
				<-start
				_, err := walLock.DoAppend(ctx, promotion(), appendPromotion)
				results <- err
			}()
		}
		close(start)
		first, second := <-results, <-results
		if first != nil {
			first, second = second, first
		}
		require.NoError(t, first)
		require.Error(t, second)
		require.True(t, status.AsStreamingError(second).IsIgnoredOperation())
		require.Equal(t, 1, writes)
	})

	t.Run("retry after failed append", func(t *testing.T) {
		receiver := startSecondary(t)
		_, err := receiver.DoAppend(ctx, promotion(), func(context.Context, message.MutableMessage) (message.MessageID, error) {
			return nil, context.Canceled
		})
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, replicateutil.RoleSecondary, receiver.replicateManager.Role())
		_, err = receiver.DoAppend(ctx, promotion(), appendOK)
		require.NoError(t, err)
		require.Equal(t, replicateutil.RolePrimary, receiver.replicateManager.Role())
	})

	t.Run("retry after recovery", func(t *testing.T) {
		receiver := recoverChannel(t, config("B", "A"))
		_, err := receiver.DoAppend(ctx, promotion(), func(context.Context, message.MutableMessage) (message.MessageID, error) {
			t.Fatal("replayed promotion reached WAL append")
			return nil, nil
		})
		require.Error(t, err)
		require.True(t, status.AsStreamingError(err).IsIgnoredOperation())
	})
}
