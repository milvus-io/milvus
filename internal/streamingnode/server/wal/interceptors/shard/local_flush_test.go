package shard

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

func TestLocalFlushAfterReplicatedDrop(t *testing.T) {
	paramtable.Init()
	resource.InitForTest(t)
	const channel = "p0_1v0"
	snapshot := &recovery.RecoverySnapshot{
		Checkpoint: &recovery.WALCheckpoint{TimeTick: 1},
		WritePathRecovery: &moduleapi.WritePathRecoveryModuleSnapshot{
			VChannels: map[string]moduleapi.VChannelWritePathRecoveryState{
				channel: {VChannel: channel, CollectionID: 1},
			},
		},
	}
	recoverManager := func() shards.ShardManager {
		return shards.RecoverShardManager(&shards.ShardManagerRecoverParam{
			ChannelInfo: types.PChannelInfo{Name: "p0", Term: 1},
			WAL:         syncutil.NewFuture[wal.WAL](), InitialRecoverSnapshot: snapshot,
		})
	}
	manager := recoverManager()
	interceptor := &shardInterceptor{shardManager: manager}
	interceptor.initOpTable()
	// DataCoord has admitted the request while collection metadata exists.
	require.NoError(t, manager.CheckIfCollectionExists(1))
	id := walimplstest.NewTestMessageID(10)
	drop := message.NewDropCollectionMessageBuilderV1().WithVChannel(channel).
		WithHeader(&message.DropCollectionMessageHeader{CollectionId: 1}).WithBody(&message.DropCollectionRequest{}).
		MustBuildMutable().WithTimeTick(10).WithLastConfirmed(id).WithReplicateHeader(&message.ReplicateHeader{ClusterID: "source", TimeTick: 10, MessageID: id, LastConfirmedMessageID: id})
	appendOK := func(context.Context, message.MutableMessage) (message.MessageID, error) { return id, nil }
	_, err := interceptor.DoAppend(context.Background(), drop, appendOK)
	require.NoError(t, err)
	require.ErrorIs(t, manager.CheckIfCollectionExists(1), shards.ErrCollectionNotFound)
	manager.Close()
	// Recovery need not retain a dropped shard or a tombstone: the validated
	// local broadcast is durable and still must finish after a handoff.
	delete(snapshot.WritePathRecovery.VChannels, channel)
	manager = recoverManager()
	defer manager.Close()
	interceptor.shardManager = manager
	flush := message.NewManualFlushMessageBuilderV2().WithUnreplicable().
		WithHeader(&message.ManualFlushMessageHeader{CollectionId: 1, SegmentIds: []int64{99}}).
		WithBody(&message.ManualFlushMessageBody{}).
		WithBroadcast([]string{channel}, message.OptBuildBroadcastAckSyncUp()).MustBuildBroadcast().WithBroadcastID(1).SplitIntoMutableMessage()[0].WithTimeTick(20)
	calls := 0
	ctx := utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{})
	_, err = interceptor.DoAppend(ctx, flush, func(_ context.Context, msg message.MutableMessage) (message.MessageID, error) {
		calls++
		require.Empty(t, message.MustAsMutableManualFlushMessageV2(msg).Header().SegmentIds)
		require.True(t, msg.BroadcastHeader().AckSyncUp)
		return nil, context.DeadlineExceeded
	})
	require.ErrorIs(t, err, context.DeadlineExceeded, "no-op still needs a successful WAL append")
	_, err = interceptor.DoAppend(ctx, flush, func(_ context.Context, msg message.MutableMessage) (message.MessageID, error) {
		calls++
		require.Empty(t, message.MustAsMutableManualFlushMessageV2(msg).Header().SegmentIds)
		return id, nil
	})
	require.NoError(t, err)
	require.Equal(t, 2, calls)
}

type localFlushShardManager struct{ shards.ShardManager }

func TestLocalFlushMissingCollectionExceptionScope(t *testing.T) {
	for _, tc := range []struct {
		name              string
		local, replicated bool
		shardErr          error
		allowed           bool
	}{
		{"local dropped", true, false, shards.ErrCollectionNotFound, true},
		{"legacy missing", false, false, shards.ErrCollectionNotFound, false},
		{"replicated missing", true, true, shards.ErrCollectionNotFound, false},
		{"other error", true, false, context.DeadlineExceeded, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manager := &localFlushShardManager{}
			patch := mockey.Mock((*localFlushShardManager).FlushAndFenceSegmentAllocUntil).Return([]int64(nil), tc.shardErr).Build()
			defer patch.UnPatch()
			b := message.NewManualFlushMessageBuilderV2().WithVChannel("p0_1v0").
				WithHeader(&message.ManualFlushMessageHeader{CollectionId: 1}).WithBody(&message.ManualFlushMessageBody{})
			if tc.local {
				b.WithUnreplicable()
			}
			msg := b.MustBuildMutable().WithTimeTick(20)
			if tc.replicated {
				msg = msg.WithReplicateHeader(&message.ReplicateHeader{ClusterID: "source", TimeTick: 10, MessageID: walimplstest.NewTestMessageID(10), LastConfirmedMessageID: walimplstest.NewTestMessageID(1)})
			}
			appended := false
			impl := &shardInterceptor{shardManager: manager}
			_, err := impl.handleManualFlushMessage(utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{}), msg, func(context.Context, message.MutableMessage) (message.MessageID, error) {
				appended = true
				return walimplstest.NewTestMessageID(20), nil
			})
			require.Equal(t, tc.allowed, appended)
			if tc.allowed {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}
