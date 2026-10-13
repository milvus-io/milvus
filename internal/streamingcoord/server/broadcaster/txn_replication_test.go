package broadcaster

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const txnControlChannel = "by-dev-rootcoord-dml_0_vcchan"

func replicaTxnMember(sequence uint32, kind messagespb.BroadcastTxnKind, keys ...message.ResourceKey) []message.ImmutableMessage {
	msg := createNewBroadcastMsg([]string{"v1", txnControlChannel})
	header := msg.BroadcastHeader()
	header.BroadcastID = uint64(100 + sequence)
	header.ResourceKeys = typeutil.NewSet(append(keys, message.NewSharedClusterResourceKey())...)
	header.Txn = &messagespb.BroadcastTxnContext{TxnId: 99, Kind: kind, Sequence: sequence}
	msg.OverwriteBroadcastHeader(header)
	if sequence == 0 {
		msg.OverwriteBroadcastAdmissionKey("replica-request")
	}
	result := make([]message.ImmutableMessage, 0, 2)
	for _, part := range msg.SplitIntoMutableMessage() {
		id := walimplstest.NewTestMessageID(int64(100 + sequence))
		result = append(result, part.WithReplicateHeader(&message.ReplicateHeader{ClusterID: "source", MessageID: id, LastConfirmedMessageID: id, TimeTick: uint64(sequence + 10), VChannel: part.VChannel()}).WithTimeTick(uint64(sequence+10)).WithLastConfirmed(id).IntoImmutableMessage(id))
	}
	return result
}

func secondaryTxnConfig() *streamingpb.ReplicateConfigurationMeta {
	return &streamingpb.ReplicateConfigurationMeta{ReplicateConfiguration: &commonpb.ReplicateConfiguration{
		Clusters:             []*commonpb.MilvusCluster{{ClusterId: "source"}, {ClusterId: paramtable.Get().CommonCfg.ClusterPrefix.GetValue()}},
		CrossClusterTopology: []*commonpb.CrossClusterTopology{{SourceClusterId: "source", TargetClusterId: paramtable.Get().CommonCfg.ClusterPrefix.GetValue()}},
	}}
}

func TestTxnReplicationRecoveryAndCallbacks(t *testing.T) {
	mockey.PatchConvey("replica uses CChannel admission and serial txn callbacks", t, func() {
		bm, store := setupTxnTest(t)
		defer func() { bm.Close() }()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		store.config = secondaryTxnConfig()
		store.roleErr = ErrNotPrimary
		key := message.NewSharedCollectionNameResourceKey("db", "c")
		begin := replicaTxnMember(0, messagespb.BroadcastTxnKind_BROADCAST_TXN_KIND_BEGIN, key)
		body := replicaTxnMember(1, messagespb.BroadcastTxnKind_BROADCAST_TXN_KIND_BODY, key)
		end := replicaTxnMember(2, messagespb.BroadcastTxnKind_BROADCAST_TXN_KIND_COMMIT, key)
		// A data-channel observation does not authorize a callback, even if Begin
		// has not reached this coordinator yet. It survives coordinator recovery.
		require.NoError(t, bm.Ack(ctx, body[0]))
		require.Len(t, store.snapshot(99), 1)
		bm.Close()
		recovered, err := RecoverBroadcaster(ctx)
		require.NoError(t, err)
		bm = recovered.(*broadcastTaskManager)
		entered, release := make(chan struct{}), make(chan struct{})
		var once sync.Once
		defer once.Do(func() { close(release) })
		var mu sync.Mutex
		var calls []uint32
		store.callback = func(ctx context.Context, msg message.BroadcastMutableMessage, _ map[string]*message.AppendResult) error {
			sequence := msg.BroadcastHeader().Txn.GetSequence()
			mu.Lock()
			calls = append(calls, sequence)
			mu.Unlock()
			if sequence == 0 {
				close(entered)
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		}
		for _, msg := range begin {
			require.NoError(t, bm.Ack(ctx, msg))
		}
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("Begin callback did not start")
		}
		require.NoError(t, bm.Ack(ctx, body[1]))
		for _, msg := range end {
			require.NoError(t, bm.Ack(ctx, msg))
		}
		// The replica never takes primary transaction locks.
		guards, err := bm.resourceKeyLocker.FastLock(message.NewExclusiveCollectionNameResourceKey("db", "c"))
		require.NoError(t, err)
		guards.Unlock()
		require.Never(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(calls) > 1 }, 30*time.Millisecond, time.Millisecond)
		once.Do(func() { close(release) })
		task, ok := bm.getBroadcastTaskByID(102)
		require.True(t, ok)
		_, err = task.BlockUntilDone(ctx)
		require.NoError(t, err)
		mu.Lock()
		require.Equal(t, []uint32{0, 1, 2}, calls)
		mu.Unlock()
		require.NoError(t, bm.Ack(ctx, end[0]))
		require.Equal(t, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE, store.snapshot(99)[0].State)
		bm.Close()
		recovered, err = RecoverBroadcaster(ctx)
		require.NoError(t, err)
		bm = recovered.(*broadcastTaskManager)
		require.NoError(t, bm.DropTombstone(ctx, 102))
		require.Empty(t, store.snapshot(99))
	})
}

func TestTxnForcePromotionTakesOverOpenGroup(t *testing.T) {
	mockey.PatchConvey("force promotion supplements known Begin and transfers its guards", t, func() {
		bm, store := setupTxnTest(t)
		defer func() { bm.Close() }()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		store.config = secondaryTxnConfig()
		store.roleErr = ErrNotPrimary
		mockey.Mock((*broadcastTaskManager).checkClusterRoleSecondary).Return(nil).Build()
		key := message.NewExclusiveCollectionNameResourceKey("db", "c")
		begin := replicaTxnMember(0, messagespb.BroadcastTxnKind_BROADCAST_TXN_KIND_BEGIN, key)
		require.NoError(t, bm.Ack(ctx, begin[0])) // CChannel still missing
		store.callback = func(_ context.Context, msg message.BroadcastMutableMessage, _ map[string]*message.AppendResult) error {
			if msg.MessageType() == message.MessageTypeAlterReplicateConfig {
				store.roleErr = nil
				store.config = nil
			}
			return nil
		}
		force, err := bm.WithSecondaryClusterResourceKey(ctx)
		require.NoError(t, err)
		msg := createAlterReplicateConfigBroadcastMsg([]string{txnControlChannel}, true)
		result := make(chan error, 1)
		go func() { _, err := force.Broadcast(ctx, msg); force.Close(); result <- err }()
		// AckSyncUp: the fence must be consumed, not merely appended.
		require.Eventually(t, func() bool { bm.mu.Lock(); defer bm.mu.Unlock(); return len(bm.tasks) == 2 }, time.Second, time.Millisecond)
		bm.mu.Lock()
		var fence *broadcastTask
		for _, task := range bm.tasks {
			if task.IsForcePromoteMessage() {
				fence = task
			}
		}
		bm.mu.Unlock()
		for _, vc := range fence.Header().VChannels {
			fence.mu.Lock()
			ack := fence.getImmutableMessageFromVChannel(vc, &types.AppendResult{MessageID: walimplstest.NewTestMessageID(1000), TimeTick: 1000})
			fence.mu.Unlock()
			require.NoError(t, bm.Ack(ctx, ack))
		}
		require.NoError(t, <-result)
		require.Equal(t, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TXN_INFLIGHT, store.snapshot(99)[0].State)
		_, err = bm.resourceKeyLocker.FastLock(key)
		require.Error(t, err)
		// A primary restart must recover the same ownership and admission index.
		bm.Close()
		recovered, err := RecoverBroadcaster(ctx)
		require.NoError(t, err)
		bm = recovered.(*broadcastTaskManager)
		_, err = bm.resourceKeyLocker.FastLock(key)
		require.Error(t, err)
		h, dup, err := bm.StartTxnBroadcastWithResourceKey(ctx, message.ResourceKey{Domain: messagespb.ResourceDomain_ResourceDomainIdempotency, Key: "replica-request"}, key)
		require.NoError(t, err)
		require.Nil(t, h)
		require.Equal(t, uint64(99), dup.TxnID)
		txn, err := bm.RecoverTxnBroadcast(ctx, 99)
		require.NoError(t, err)
		defer txn.Close()
		_, err = txn.BroadcastCommit(ctx, createNewBroadcastMsg([]string{"v1"}))
		require.NoError(t, err)
		guards, err := bm.resourceKeyLocker.FastLock(key)
		require.NoError(t, err)
		guards.Unlock()
	})
}

func TestTxnPromotionRejectsMissingMember(t *testing.T) {
	mockey.PatchConvey("missing entire Begin cannot be fabricated", t, func() {
		bm, _ := setupTxnTest(t)
		defer bm.Close()
		body := replicaTxnMember(1, messagespb.BroadcastTxnKind_BROADCAST_TXN_KIND_BODY)
		require.NoError(t, bm.Ack(context.Background(), body[0]))
		require.Error(t, bm.ackScheduler.fixIncompleteBroadcastsForForcePromote(context.Background()))
	})
}

func TestTxnReplicaTombstoneAndValidation(t *testing.T) {
	mockey.PatchConvey("replica transaction tombstone survives promotion", t, func() {
		bm, store := setupTxnTest(t)
		defer func() { bm.Close() }()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for sequence, kind := range []messagespb.BroadcastTxnKind{messagespb.BroadcastTxnKind_BROADCAST_TXN_KIND_BEGIN, messagespb.BroadcastTxnKind_BROADCAST_TXN_KIND_BODY, messagespb.BroadcastTxnKind_BROADCAST_TXN_KIND_COMMIT} {
			for _, msg := range replicaTxnMember(uint32(sequence), kind) {
				require.NoError(t, bm.Ack(ctx, msg))
			}
		}
		task, _ := bm.getBroadcastTaskByID(102)
		_, err := task.BlockUntilDone(ctx)
		require.NoError(t, err)
		bm.Close()
		recovered, err := RecoverBroadcaster(ctx)
		require.NoError(t, err)
		bm = recovered.(*broadcastTaskManager)
		handle, err := bm.RecoverTxnBroadcast(ctx, 99)
		require.Nil(t, handle)
		require.ErrorContains(t, err, "has completed")
		require.Len(t, store.snapshot(99), 3)
		invalid := replicaTxnMember(1, messagespb.BroadcastTxnKind_BROADCAST_TXN_KIND_COMMIT)[0]
		require.Error(t, bm.Ack(ctx, invalid)) // same broadcast ID, different kind
		invalid = replicaTxnMember(3, messagespb.BroadcastTxnKind_BROADCAST_TXN_KIND_BODY)[0]
		require.Error(t, bm.Ack(ctx, invalid)) // cannot append after selected terminal
	})
}

func TestTxnPrimaryReplicationAndResourceBarrier(t *testing.T) {
	mockey.PatchConvey("primary with replicas retains the existing Cluster X barrier", t, func() {
		bm, store := setupTxnTest(t)
		defer bm.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		store.config = secondaryTxnConfig() // Start uses the live role checker, not presence of topology.
		handle, _, err := bm.StartTxnBroadcastWithResourceKey(ctx, message.NewSharedCollectionNameResourceKey("db", "c"))
		require.NoError(t, err)
		defer handle.Close()
		invalid := beginTxnMessage(99)
		invalid.Properties().ToRawMap()["_ur"] = ""
		_, err = handle.BroadcastBegin(ctx, invalid)
		require.Error(t, err)
		_, err = handle.BroadcastBegin(ctx, beginTxnMessage(99))
		require.NoError(t, err)
		barrier := make(chan BroadcastAPI, 1)
		go func() {
			api, err := bm.WithResourceKeys(ctx, message.NewExclusiveClusterResourceKey())
			if err == nil {
				barrier <- api
			}
		}()
		select {
		case <-barrier:
			t.Fatal("Cluster X bypassed open transaction")
		case <-time.After(20 * time.Millisecond):
		}
		_, err = handle.BroadcastCommit(ctx, createNewBroadcastMsg([]string{"v1"}))
		require.NoError(t, err)
		select {
		case api := <-barrier:
			api.Close()
		case <-ctx.Done():
			t.Fatal("Cluster X was not released")
		}
	})
}

func TestTxnPromotionRecoveryBeforeDurableCompletion(t *testing.T) {
	mockey.PatchConvey("restart after role publication but before promotion completion", t, func() {
		bm, store := setupTxnTest(t)
		defer func() { bm.Close() }()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		key := message.NewExclusiveCollectionNameResourceKey("db", "c")
		for _, msg := range replicaTxnMember(0, messagespb.BroadcastTxnKind_BROADCAST_TXN_KIND_BEGIN, key) {
			require.NoError(t, bm.Ack(ctx, msg))
		}
		begin, _ := bm.getBroadcastTaskByID(100)
		_, err := begin.BlockUntilDone(ctx)
		require.NoError(t, err)
		bm.Close()
		// Persisted primary config, with a local promotion record still PENDING.
		// Recovery must restore the barrier first, then transfer the open Begin.
		store.mu.Lock()
		store.tasks[200] = createPendingForcePromoteTask(200, []string{txnControlChannel})
		store.mu.Unlock()
		recovered, err := RecoverBroadcaster(ctx)
		require.NoError(t, err)
		bm = recovered.(*broadcastTaskManager)
		fence, _ := bm.getBroadcastTaskByID(200)
		_, err = fence.BlockUntilDone(ctx)
		require.NoError(t, err)
		_, err = bm.resourceKeyLocker.FastLock(key)
		require.Error(t, err)
		handle, err := bm.RecoverTxnBroadcast(ctx, 99)
		require.NoError(t, err)
		defer handle.Close()
		_, err = handle.BroadcastCommit(ctx, createNewBroadcastMsg([]string{"v1"}))
		require.NoError(t, err)
	})
}

func TestTxnPromotionAdmissionBarrier(t *testing.T) {
	mockey.PatchConvey("new writes cannot enter between published role and lock handoff", t, func() {
		bm, store := setupTxnTest(t)
		defer bm.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		mockey.Mock((*broadcastTaskManager).checkClusterRoleSecondary).Return(nil).Build()
		key := message.NewExclusiveCollectionNameResourceKey("db", "c")
		for _, msg := range replicaTxnMember(0, messagespb.BroadcastTxnKind_BROADCAST_TXN_KIND_BEGIN, key) {
			require.NoError(t, bm.Ack(ctx, msg))
		}
		begin, _ := bm.getBroadcastTaskByID(100)
		_, err := begin.BlockUntilDone(ctx)
		require.NoError(t, err)
		published, release := make(chan struct{}), make(chan struct{})
		var once sync.Once
		defer once.Do(func() { close(release) })
		store.callback = func(ctx context.Context, msg message.BroadcastMutableMessage, _ map[string]*message.AppendResult) error {
			if msg.MessageType() == message.MessageTypeAlterReplicateConfig {
				close(published)
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		}
		force, err := bm.WithSecondaryClusterResourceKey(ctx)
		require.NoError(t, err)
		result := make(chan error, 1)
		caller, stop := context.WithCancel(ctx)
		go func() {
			_, err := force.Broadcast(caller, createAlterReplicateConfigBroadcastMsg([]string{txnControlChannel}, true))
			force.Close()
			result <- err
		}()
		select {
		case <-published:
		case <-ctx.Done():
			t.Fatal("promotion callback did not start")
		}
		// Gate waits respect request cancellation while the background promotion continues.
		expired, expire := context.WithCancel(ctx)
		expire()
		_, err = bm.RecoverTxnBroadcast(expired, 99)
		require.ErrorIs(t, err, context.Canceled)
		// Canceling the caller cannot release the background task's admission gate.
		stop()
		require.ErrorIs(t, <-result, context.Canceled)
		recovered := make(chan TxnBroadcaster, 1)
		go func() {
			h, err := bm.RecoverTxnBroadcast(ctx, 99)
			if err == nil {
				recovered <- h
			}
		}()
		select {
		case <-recovered:
			t.Fatal("Recover bypassed promotion barrier")
		case <-time.After(20 * time.Millisecond):
		}
		once.Do(func() { close(release) })
		select {
		case h := <-recovered:
			h.Close()
		case <-ctx.Done():
			t.Fatal("promotion did not open admission")
		}
		_, err = bm.resourceKeyLocker.FastLock(key)
		require.Error(t, err)
	})
}

func TestControlChannelPendingWriterOrder(t *testing.T) {
	mockey.PatchConvey("CChannel admission keeps a pending writer ahead of later readers", t, func() {
		bm, store := setupTxnTest(t)
		defer bm.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		entered, release := make(chan struct{}), make(chan struct{})
		var once sync.Once
		defer once.Do(func() { close(release) })
		calls := make(chan uint64, 3)
		store.callback = func(ctx context.Context, msg message.BroadcastMutableMessage, _ map[string]*message.AppendResult) error {
			id := msg.BroadcastHeader().BroadcastID
			calls <- id
			if id == 10 {
				close(entered)
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		}
		var last *broadcastTask
		for i := uint64(0); i < 3; i++ {
			key := message.NewSharedCollectionNameResourceKey("db", "c")
			if i == 1 {
				key.Shared = false
			}
			msg := createNewBroadcastMsg([]string{txnControlChannel}, key).WithBroadcastID(10 + i)
			task := newBroadcastTaskFromProto(createNewWaitAckBroadcastTaskFromMessage(msg, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_REPLICATED, []byte{1}), bm.metrics, bm.ackScheduler)
			task.SetLogger(bm.Logger())
			bm.ackScheduler.AddTask(task)
			last = task
			if i == 0 {
				select {
				case <-entered:
				case <-ctx.Done():
					t.Fatal("first reader did not start")
				}
			}
		}
		require.Equal(t, uint64(10), <-calls)
		select {
		case id := <-calls:
			t.Fatalf("callback %d bypassed an earlier conflict", id)
		case <-time.After(20 * time.Millisecond):
		}
		once.Do(func() { close(release) })
		_, err := last.BlockUntilDone(ctx)
		require.NoError(t, err)
		require.Equal(t, uint64(11), <-calls)
		require.Equal(t, uint64(12), <-calls)
	})
}

func TestTxnReplicaRejectsDifferentResourcesAndChannels(t *testing.T) {
	mockey.PatchConvey("replication and recovery preserve member scope", t, func() {
		bm, store := setupTxnTest(t)
		defer bm.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		key := message.NewExclusiveCollectionNameResourceKey("db", "c")
		begin := replicaTxnMember(0, messagespb.BroadcastTxnKind_BROADCAST_TXN_KIND_BEGIN, key)[0]
		require.NoError(t, bm.Ack(ctx, begin))
		for _, change := range []func(*message.BroadcastHeader){
			func(h *message.BroadcastHeader) { h.VChannels = []string{"v2", txnControlChannel} },
			func(h *message.BroadcastHeader) { h.VChannels = []string{txnControlChannel} },
			func(h *message.BroadcastHeader) { h.VChannels = append(h.VChannels, "v2") },
			func(h *message.BroadcastHeader) {
				h.ResourceKeys = typeutil.NewSet(message.NewSharedClusterResourceKey(), message.NewSharedCollectionNameResourceKey("db", "c"))
			},
		} {
			for _, original := range []message.ImmutableMessage{begin, replicaTxnMember(1, messagespb.BroadcastTxnKind_BROADCAST_TXN_KIND_BODY, key)[0]} {
				mutable := original.IntoBroadcastMutableMessage()
				header := mutable.BroadcastHeader()
				change(header)
				mutable.OverwriteBroadcastHeader(header)
				invalid := mutable.SplitIntoMutableMessage()[0].WithTimeTick(original.TimeTick()).WithLastConfirmedUseMessageID().IntoImmutableMessage(original.MessageID())
				require.Error(t, bm.Ack(ctx, invalid))
			}
			require.Len(t, store.snapshot(99), 1)

			body := replicaTxnMember(1, messagespb.BroadcastTxnKind_BROADCAST_TXN_KIND_BODY, key)[0].IntoBroadcastMutableMessage()
			header := body.BroadcastHeader()
			change(header)
			body.OverwriteBroadcastHeader(header)
			tasks := append(store.snapshot(99), &streamingpb.BroadcastTask{Message: body.IntoMessageProto(), State: streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_REPLICATED})
			_, _, err := splitBroadcastTasks(tasks)
			require.Error(t, err)
		}
		// An identical channel set in another order remains valid.
		body := replicaTxnMember(1, messagespb.BroadcastTxnKind_BROADCAST_TXN_KIND_BODY, key)[0].IntoBroadcastMutableMessage()
		header := body.BroadcastHeader()
		header.VChannels = []string{txnControlChannel, "v1"}
		body.OverwriteBroadcastHeader(header)
		tasks := append(store.snapshot(99), &streamingpb.BroadcastTask{Message: body.IntoMessageProto(), State: streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_REPLICATED})
		_, _, err := splitBroadcastTasks(tasks)
		require.NoError(t, err)
	})
}
