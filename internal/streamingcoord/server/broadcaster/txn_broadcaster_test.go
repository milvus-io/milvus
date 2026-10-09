package broadcaster

import (
	"context"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/mocks/distributed/mock_streaming"
	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/internal/util/idalloc"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type txnTestStore struct {
	mu         sync.Mutex
	tasks      map[uint64]*streamingpb.BroadcastTask
	config     *streamingpb.ReplicateConfigurationMeta
	configErr  error
	roleErr    error
	beforeSave func(map[uint64]*streamingpb.BroadcastTask) error
	callback   func(context.Context, message.BroadcastMutableMessage, map[string]*message.AppendResult) error
}

func (s *txnTestStore) snapshot(id uint64) []*streamingpb.BroadcastTask {
	s.mu.Lock()
	defer s.mu.Unlock()
	var tasks []*streamingpb.BroadcastTask
	for _, task := range s.tasks {
		header := message.NewBroadcastMutableMessageBeforeAppend(task.Message.Payload, task.Message.Properties).BroadcastHeader()
		if header.Txn.GetTxnId() == id {
			tasks = append(tasks, proto.Clone(task).(*streamingpb.BroadcastTask))
		}
	}
	sort.Slice(tasks, func(i, j int) bool {
		a := message.NewBroadcastMutableMessageBeforeAppend(tasks[i].Message.Payload, tasks[i].Message.Properties).BroadcastHeader().Txn.GetSequence()
		b := message.NewBroadcastMutableMessageBeforeAppend(tasks[j].Message.Payload, tasks[j].Message.Properties).BroadcastHeader().Txn.GetSequence()
		return a < b
	})
	return tasks
}

// Existing generated interface holders are patched with Mockey; no mock-framework expectations.
func setupTxnTest(t *testing.T) (*broadcastTaskManager, *txnTestStore) {
	paramtable.Init()
	old := paramtable.Get().StreamingCfg.WALBroadcasterTombstoneCheckInternal.SwapTempValue("1h")
	t.Cleanup(func() { paramtable.Get().StreamingCfg.WALBroadcasterTombstoneCheckInternal.SwapTempValue(old) })
	oldLifetime := paramtable.Get().StreamingCfg.WALBroadcasterTombstoneMaxLifetime.SwapTempValue("1h")
	oldCount := paramtable.Get().StreamingCfg.WALBroadcasterTombstoneMaxCount.SwapTempValue("8192")
	t.Cleanup(func() {
		paramtable.Get().StreamingCfg.WALBroadcasterTombstoneMaxLifetime.SwapTempValue(oldLifetime)
		paramtable.Get().StreamingCfg.WALBroadcasterTombstoneMaxCount.SwapTempValue(oldCount)
	})
	store := &txnTestStore{tasks: make(map[uint64]*streamingpb.BroadcastTask)}
	catalog := &mock_metastore.MockStreamingCoordCataLog{}
	save := func(ctx context.Context, changes map[uint64]*streamingpb.BroadcastTask) error {
		store.mu.Lock()
		defer store.mu.Unlock()
		if store.beforeSave != nil {
			if err := store.beforeSave(changes); err != nil {
				return err
			}
		}
		for id, task := range changes {
			if task.State == streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_DONE {
				delete(store.tasks, id)
			} else {
				store.tasks[id] = proto.Clone(task).(*streamingpb.BroadcastTask)
			}
		}
		return nil
	}
	mockey.Mock((*mock_metastore.MockStreamingCoordCataLog).SaveBroadcastTasks).To(save).Build()

	mockey.Mock((*mock_metastore.MockStreamingCoordCataLog).ListBroadcastTask).To(func(context.Context) ([]*streamingpb.BroadcastTask, error) {
		store.mu.Lock()
		defer store.mu.Unlock()
		tasks := make([]*streamingpb.BroadcastTask, 0, len(store.tasks))
		for _, task := range store.tasks {
			tasks = append(tasks, proto.Clone(task).(*streamingpb.BroadcastTask))
		}
		return tasks, nil
	}).Build()
	mockey.Mock((*mock_metastore.MockStreamingCoordCataLog).GetReplicateConfiguration).To(func(context.Context) (*streamingpb.ReplicateConfigurationMeta, error) {
		return store.config, store.configErr
	}).Build()
	resource.InitForTest(resource.OptStreamingCatalog(catalog))
	allocator := idalloc.NewIDAllocator(nil)
	var ids atomic.Uint64
	mockey.Mock(mockey.GetMethod(allocator, "Allocate")).To(func(context.Context) (uint64, error) { return ids.Add(1), nil }).Build()
	mockey.Mock(mockey.GetMethod(resource.Resource(), "IDAllocator")).Return(allocator).Build()
	mockey.Mock((*broadcastTaskManager).checkClusterRole).To(func(ctx context.Context) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return store.roleErr
	}).Build()
	wal := &mock_streaming.MockWALAccesser{}
	mockey.Mock((*mock_streaming.MockWALAccesser).ControlChannel).Return("by-dev-rootcoord-dml_0_vcchan").Build()
	mockey.Mock((*mock_streaming.MockWALAccesser).AppendMessages).To(func(_ *mock_streaming.MockWALAccesser, ctx context.Context, msgs ...message.MutableMessage) types.AppendResponses {
		responses := types.AppendResponses{Responses: make([]types.AppendResponse, len(msgs))}
		for i := range msgs {
			responses.Responses[i].AppendResult = &types.AppendResult{MessageID: walimplstest.NewTestMessageID(int64(i + 1)), TimeTick: 1}
		}
		return responses
	}).Build()
	streaming.SetWALForTest(wal)
	mockey.Mock(registry.CallMessageAckOnceCallbacks).Return(nil).Build()
	mockey.Mock(registry.CallMessageAckCallback).To(func(ctx context.Context, msg message.BroadcastMutableMessage, result map[string]*message.AppendResult) error {
		if store.callback != nil {
			return store.callback(ctx, msg, result)
		}
		return nil
	}).Build()
	return newBroadcastTaskManager(nil), store
}

func beginTxnMessage(id uint64) message.BroadcastMutableMessage {
	msg := createNewBroadcastMsg([]string{"v1"})
	header := msg.BroadcastHeader()
	header.Txn = &messagespb.BroadcastTxnContext{TxnId: id}
	return msg.OverwriteBroadcastHeader(header)
}

func TestTxnBroadcastLifecycle(t *testing.T) {
	mockey.PatchConvey("txn lifecycle", t, func() {
		bm, store := setupTxnTest(t)
		defer bm.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		key := message.NewIdempotencyResourceKey("import", message.NewCollectionScopedIdempotencyKey(1, "request"))
		business := message.NewExclusiveCollectionNameResourceKey("db", "collection")
		handle, dup, err := bm.StartTxnBroadcastWithResourceKey(ctx, key, business)
		require.NoError(t, err)
		require.Nil(t, dup)
		beginMsg := beginTxnMessage(99)
		begin, err := handle.BroadcastBegin(ctx, beginMsg)
		require.NoError(t, err)
		// Caller mutations must not change the admitted task or its durable header.
		header := beginMsg.BroadcastHeader()
		header.Txn.TxnId = 100
		beginMsg.OverwriteBroadcastHeader(header).OverwriteBroadcastAdmissionKey("changed")
		persistedBegin := bm.tasks[begin.BroadcastResult.BroadcastID].BroadcastMessage()
		require.Equal(t, uint64(99), persistedBegin.BroadcastHeader().Txn.TxnId)
		require.Equal(t, key.Key, message.BroadcastAdmissionKeyOf(persistedBegin))
		handle.Close()
		require.Equal(t, uint64(99), begin.TxnID)
		require.Equal(t, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TXN_INFLIGHT, store.snapshot(99)[0].State)
		_, err = bm.resourceKeyLocker.FastLock(business)
		require.Error(t, err)
		// A conflicting DDL is already waiting. The retry must not queue for its business key.
		acquired := make(chan struct{})
		releaseDDL := make(chan struct{})
		go func() { g := bm.resourceKeyLocker.Lock(business); close(acquired); <-releaseDDL; g.Unlock() }()
		retried, dup, err := bm.StartTxnBroadcastWithResourceKey(ctx, key, business)
		require.NoError(t, err)
		require.Nil(t, retried)
		require.Equal(t, begin.BroadcastResult.BroadcastID, dup.BroadcastResult.BroadcastID)
		recovered, err := bm.RecoverTxnBroadcast(ctx, 99)
		require.NoError(t, err)
		defer recovered.Close()
		body := newImportMsgWithKey("body-1")
		first, err := recovered.BroadcastBody(ctx, body)
		require.NoError(t, err)
		again, err := recovered.BroadcastBody(ctx, body)
		require.NoError(t, err)
		require.Equal(t, first.BroadcastResult.BroadcastID, again.BroadcastResult.BroadcastID)
		require.Len(t, store.snapshot(99), 2)
		commitMsg := createNewBroadcastMsg([]string{"v1"})
		done, err := recovered.BroadcastCommit(ctx, commitMsg)
		require.NoError(t, err)
		select {
		case <-acquired:
		case <-ctx.Done():
			t.Fatal("DDL did not acquire released resources")
		}
		close(releaseDDL)
		snapshot := store.snapshot(99)
		require.Len(t, snapshot, 3)
		require.Equal(t, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE, snapshot[0].State)
		for _, member := range snapshot {
			hdr := message.NewBroadcastMutableMessageBeforeAppend(member.Message.Payload, member.Message.Properties).BroadcastHeader()
			require.Equal(t, uint64(99), hdr.Txn.TxnId)
			for key := range hdr.ResourceKeys {
				require.NotEqual(t, messagespb.ResourceDomain_ResourceDomainIdempotency, key.Domain)
			}
		}
		same, err := recovered.BroadcastCommit(ctx, commitMsg)
		require.NoError(t, err)
		require.Equal(t, done.BroadcastResult.BroadcastID, same.BroadcastResult.BroadcastID)
		_, err = recovered.BroadcastCommit(ctx, newImportMsgWithKey("abort"))
		require.Error(t, err)
		ensured, err := recovered.BroadcastCommit(ctx, newImportMsgWithKey("abort"), EnsureTxnCompleted)
		require.NoError(t, err)
		require.Equal(t, done.BroadcastResult.BroadcastID, ensured.BroadcastResult.BroadcastID)
		_, err = recovered.BroadcastBody(ctx, newImportMsgWithKey("new-body"))
		require.Error(t, err)
		require.NoError(t, bm.DropTombstone(ctx, done.BroadcastResult.BroadcastID))
		require.Nil(t, store.snapshot(99))
		_, err = bm.RecoverTxnBroadcast(ctx, 99)
		require.Error(t, err)
		_, err = recovered.BroadcastCommit(ctx, commitMsg)
		require.Error(t, err)
		require.Empty(t, bm.tasks)
		require.Empty(t, bm.idempotencyIndex.scopeToBroadcastID)
	})
}

func TestTxnBeginCancellationAndConcurrentAdmission(t *testing.T) {
	mockey.PatchConvey("canceled caller does not cancel admitted Begin", t, func() {
		bm, store := setupTxnTest(t)
		defer bm.Close()
		entered, release := make(chan struct{}), make(chan struct{})
		var once sync.Once
		store.callback = func(context.Context, message.BroadcastMutableMessage, map[string]*message.AppendResult) error {
			once.Do(func() { close(entered) })
			<-release
			return nil
		}
		ctx, cancel := context.WithCancel(context.Background())
		key := message.NewIdempotencyResourceKey("create", message.NewClusterScopedIdempotencyKey("k"))
		business := message.NewExclusiveCollectionNameResourceKey("db", "c")
		handle, _, err := bm.StartTxnBroadcastWithResourceKey(ctx, key, business)
		require.NoError(t, err)
		returned := make(chan error, 1)
		go func() { _, err := handle.BroadcastBegin(ctx, beginTxnMessage(101)); returned <- err }()
		<-entered
		cancel()
		require.ErrorIs(t, <-returned, context.Canceled)
		handle.Close()
		_, err = bm.resourceKeyLocker.FastLock(business)
		require.Error(t, err)
		close(release)
		timeout, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		var wg sync.WaitGroup
		results := make(chan uint64, 8)
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, dup, err := bm.StartTxnBroadcastWithResourceKey(timeout, key, business)
				if err == nil {
					results <- dup.TxnID
				}
			}()
		}
		wg.Wait()
		close(results)
		require.Len(t, results, 8)
		for id := range results {
			require.Equal(t, uint64(101), id)
		}
		require.Len(t, store.snapshot(101), 1)
		resumed, err := bm.RecoverTxnBroadcast(timeout, 101)
		require.NoError(t, err)
		_, err = resumed.BroadcastCommit(timeout, createNewBroadcastMsg([]string{"v1"}))
		require.NoError(t, err)
	})
}

func TestTxnRecoveryAndUnsubmittedClose(t *testing.T) {
	mockey.PatchConvey("recover open and closed groups", t, func() {
		bm, store := setupTxnTest(t)
		defer func() { bm.Close() }()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		key := message.NewIdempotencyResourceKey("create", message.NewClusterScopedIdempotencyKey("recover"))
		rk := message.NewExclusiveCollectionNameResourceKey("db", "c")
		unused, _, err := bm.StartTxnBroadcastWithResourceKey(ctx, key, rk)
		require.NoError(t, err)
		unused.Close()
		unused.Close()
		handle, _, err := bm.StartTxnBroadcastWithResourceKey(ctx, key, rk)
		require.NoError(t, err)
		_, err = handle.BroadcastBegin(ctx, beginTxnMessage(77))
		require.NoError(t, err)
		bm.Close()
		recovered, err := RecoverBroadcaster(ctx)
		require.NoError(t, err)
		bm = recovered.(*broadcastTaskManager)
		_, err = bm.resourceKeyLocker.FastLock(rk)
		require.Error(t, err)
		_, dup, err := bm.StartTxnBroadcastWithResourceKey(ctx, key, rk)
		require.NoError(t, err)
		require.Equal(t, uint64(77), dup.TxnID)
		handle, err = bm.RecoverTxnBroadcast(ctx, 77)
		require.NoError(t, err)
		commit, err := handle.BroadcastCommit(ctx, createNewBroadcastMsg([]string{"v1"}))
		require.NoError(t, err)
		bm.Close()
		recovered, err = RecoverBroadcaster(ctx)
		require.NoError(t, err)
		bm = recovered.(*broadcastTaskManager)
		guards, err := bm.resourceKeyLocker.FastLock(rk)
		require.NoError(t, err)
		guards.Unlock()
		require.NoError(t, bm.DropTombstone(ctx, commit.BroadcastResult.BroadcastID))
		require.Nil(t, store.snapshot(77))
	})
}

func TestTxnCompetingTerminalsAndAckSyncUp(t *testing.T) {
	mockey.PatchConvey("terminal choice persists before ACK and rejects conflicts", t, func() {
		bm, store := setupTxnTest(t)
		defer bm.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		rk := message.NewExclusiveCollectionNameResourceKey("db", "c")
		h, _, err := bm.StartTxnBroadcastWithResourceKey(ctx, rk)
		require.NoError(t, err)
		_, err = h.BroadcastBegin(ctx, beginTxnMessage(5))
		require.NoError(t, err)
		first, err := bm.RecoverTxnBroadcast(ctx, 5)
		require.NoError(t, err)
		second, err := bm.RecoverTxnBroadcast(ctx, 5)
		require.NoError(t, err)
		end := message.NewDropCollectionMessageBuilderV1().WithHeader(&message.DropCollectionMessageHeader{}).WithBody(&msgpb.DropCollectionRequest{}).WithBroadcast([]string{"v1"}, message.OptBuildBroadcastAckSyncUp()).MustBuildBroadcast()
		result := make(chan error, 1)
		waitCtx, stop := context.WithCancel(ctx)
		go func() { _, err := first.BroadcastCommit(waitCtx, end); result <- err }()
		require.Eventually(t, func() bool { return len(store.snapshot(5)) == 2 }, time.Second, time.Millisecond)
		stop()
		require.ErrorIs(t, <-result, context.Canceled)
		// Append success cannot close a member with AckSyncUp.
		_, err = bm.resourceKeyLocker.FastLock(rk)
		require.Error(t, err)
		_, err = second.BroadcastCommit(ctx, newImportMsgWithKey("rollback"))
		require.Error(t, err)
		g := bm.txns[5]
		require.NoError(t, g.acquire(ctx))
		task := g.tasks[1]
		g.release()
		for _, vc := range task.Header().VChannels {
			task.mu.Lock()
			immutable := task.getImmutableMessageFromVChannel(vc, &types.AppendResult{MessageID: walimplstest.NewTestMessageID(1), TimeTick: 2})
			task.mu.Unlock()
			require.NoError(t, bm.Ack(ctx, immutable))
		}
		_, err = second.BroadcastCommit(ctx, end)
		require.NoError(t, err)
		require.Len(t, store.snapshot(5), 2)
		guards, err := bm.resourceKeyLocker.FastLock(rk)
		require.NoError(t, err)
		guards.Unlock()
	})
}

func TestTxnFailedCompletionRetainsResources(t *testing.T) {
	mockey.PatchConvey("failed group close is replayed after recovery", t, func() {
		bm, store := setupTxnTest(t)
		defer func() { bm.Close() }()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		rk := message.NewExclusiveCollectionNameResourceKey("db", "c")
		h, _, err := bm.StartTxnBroadcastWithResourceKey(ctx, rk)
		require.NoError(t, err)
		_, err = h.BroadcastBegin(ctx, beginTxnMessage(6))
		require.NoError(t, err)
		failed := make(chan struct{})
		var once sync.Once
		store.mu.Lock()
		store.beforeSave = func(changes map[uint64]*streamingpb.BroadcastTask) error {
			if len(changes) == 2 {
				once.Do(func() { close(failed) })
				return context.Canceled
			}
			return nil
		}
		store.mu.Unlock()
		waiting, stop := context.WithCancel(ctx)
		result := make(chan error, 1)
		go func() { _, err := h.BroadcastCommit(waiting, createNewBroadcastMsg([]string{"v1"})); result <- err }()
		<-failed
		stop()
		require.Error(t, <-result)
		require.Equal(t, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TXN_INFLIGHT, store.snapshot(6)[0].State)
		_, err = bm.resourceKeyLocker.FastLock(rk)
		require.Error(t, err)
		bm.Close()
		store.mu.Lock()
		store.beforeSave = nil
		store.mu.Unlock()
		recovered, err := RecoverBroadcaster(ctx)
		require.NoError(t, err)
		bm = recovered.(*broadcastTaskManager)
		h, err = bm.RecoverTxnBroadcast(ctx, 6)
		require.NoError(t, err)
		_, err = h.BroadcastCommit(ctx, createNewBroadcastMsg([]string{"v1"}))
		require.NoError(t, err)
		guards, err := bm.resourceKeyLocker.FastLock(rk)
		require.NoError(t, err)
		guards.Unlock()
	})
}

func TestTxnAdmissionBeforeBeginAndValidation(t *testing.T) {
	mockey.PatchConvey("first requests serialize before message construction", t, func() {
		bm, store := setupTxnTest(t)
		defer bm.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		key := message.NewIdempotencyResourceKey("create", message.NewClusterScopedIdempotencyKey("first"))
		rk := message.NewExclusiveCollectionNameResourceKey("db", "c")
		_, _, err := bm.StartTxnBroadcastWithResourceKey(ctx, key, key)
		require.Error(t, err)
		h, _, err := bm.StartTxnBroadcastWithResourceKey(ctx, key, rk)
		require.NoError(t, err)
		_, err = h.BroadcastBegin(ctx, createNewBroadcastMsg([]string{"v1"}))
		require.Error(t, err)
		_, err = h.BroadcastBody(ctx, createNewBroadcastMsg([]string{"v1"}))
		require.Error(t, err)
		_, err = bm.WithResourceKeys(ctx, key)
		require.Error(t, err)
		arrived := make(chan struct{})
		duplicate := make(chan *TxnBroadcastResult, 1)
		go func() {
			close(arrived)
			fresh, result, err := bm.StartTxnBroadcastWithResourceKey(ctx, key, rk)
			if fresh != nil {
				fresh.Close()
			}
			if err == nil {
				duplicate <- result
			}
		}()
		<-arrived
		// Both admission and business ownership belong to the first unused handle.
		_, err = bm.resourceKeyLocker.FastLock(key)
		require.Error(t, err)
		begin, err := h.BroadcastBegin(ctx, beginTxnMessage(123))
		require.NoError(t, err)
		select {
		case result := <-duplicate:
			require.NotNil(t, result)
			require.Equal(t, begin.TxnID, result.TxnID)
		case <-ctx.Done():
			t.Fatal("duplicate blocked behind Begin business locks")
		}
		require.Len(t, store.snapshot(123), 1)
		_, err = h.BroadcastBody(ctx, beginTxnMessage(124))
		require.Error(t, err)
		_, err = h.BroadcastBody(ctx, createNewBroadcastMsg([]string{"v1"}, message.NewExclusiveCollectionNameResourceKey("db", "other")))
		require.Error(t, err)
		_, err = h.BroadcastCommit(ctx, createNewBroadcastMsg([]string{"v1"}), TxnCommitOption(99))
		require.Error(t, err)
		_, err = h.BroadcastCommit(ctx, createNewBroadcastMsg([]string{"v1"}))
		require.NoError(t, err)
	})
}

func TestTxnMemberLimitReservesTerminal(t *testing.T) {
	mockey.PatchConvey("body capacity never consumes the terminal slot", t, func() {
		bm, _ := setupTxnTest(t)
		defer bm.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		h, _, err := bm.StartTxnBroadcastWithResourceKey(ctx)
		require.NoError(t, err)
		_, err = h.BroadcastBegin(ctx, beginTxnMessage(9))
		require.NoError(t, err)
		for i := 0; i < maxTxnMembers-2; i++ {
			_, err = h.BroadcastBody(ctx, createNewBroadcastMsg([]string{"v1"}))
			require.NoError(t, err)
		}
		_, err = h.BroadcastBody(ctx, createNewBroadcastMsg([]string{"v1"}))
		require.Error(t, err)
		_, err = h.BroadcastCommit(ctx, createNewBroadcastMsg([]string{"v1"}))
		require.NoError(t, err)
	})
}

func TestTxnShutdownWithPendingMember(t *testing.T) {
	mockey.PatchConvey("shutdown cancels API waits without a business terminal", t, func() {
		bm, _ := setupTxnTest(t)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		h, _, err := bm.StartTxnBroadcastWithResourceKey(ctx)
		require.NoError(t, err)
		msg := message.NewDropCollectionMessageBuilderV1().WithHeader(&message.DropCollectionMessageHeader{}).WithBody(&msgpb.DropCollectionRequest{}).WithBroadcast([]string{"v1"}, message.OptBuildBroadcastAckSyncUp()).MustBuildBroadcast()
		header := msg.BroadcastHeader()
		header.Txn = &messagespb.BroadcastTxnContext{TxnId: 18}
		msg.OverwriteBroadcastHeader(header)
		done := make(chan error, 1)
		go func() { _, err := h.BroadcastBegin(context.Background(), msg); done <- err }()
		require.Eventually(t, func() bool { bm.mu.Lock(); defer bm.mu.Unlock(); return bm.txns[18] != nil }, time.Second, time.Millisecond)
		closed := make(chan struct{})
		go func() { bm.Close(); close(closed) }()
		select {
		case <-closed:
		case <-ctx.Done():
			t.Fatal("shutdown waited for the business Commit")
		}
		require.Error(t, <-done)
	})
}

func TestTxnCapacityAndFailureBeforeAdmission(t *testing.T) {
	mockey.PatchConvey("invalid admission releases resources and members have independent size limits", t, func() {
		bm, store := setupTxnTest(t)
		defer bm.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		key := message.NewIdempotencyResourceKey("create", message.NewClusterScopedIdempotencyKey("capacity"))
		rk := message.NewSharedCollectionNameResourceKey("db", "c")
		store.roleErr = ErrNotPrimary
		_, _, err := bm.StartTxnBroadcastWithResourceKey(ctx, key, rk)
		require.ErrorIs(t, err, ErrNotPrimary)
		_, err = bm.RecoverTxnBroadcast(ctx, 1)
		require.ErrorIs(t, err, ErrNotPrimary)
		store.roleErr = nil
		store.configErr = context.DeadlineExceeded
		_, _, err = bm.StartTxnBroadcastWithResourceKey(ctx, key, rk)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		store.configErr = nil
		store.config = &streamingpb.ReplicateConfigurationMeta{ReplicateConfiguration: &commonpb.ReplicateConfiguration{CrossClusterTopology: []*commonpb.CrossClusterTopology{{}}}}
		_, _, err = bm.StartTxnBroadcastWithResourceKey(ctx, key, rk)
		require.Error(t, err)
		store.config = nil
		h, _, err := bm.StartTxnBroadcastWithResourceKey(ctx, key, rk)
		require.NoError(t, err)
		bad := beginTxnMessage(1).OverwriteBroadcastAdmissionKey("different")
		_, err = h.BroadcastBegin(ctx, bad)
		require.Error(t, err)
		legacy := newImportMsgWithKey("legacy-key")
		header := legacy.BroadcastHeader()
		header.Txn = &messagespb.BroadcastTxnContext{TxnId: 1}
		_, err = h.BroadcastBegin(ctx, legacy.OverwriteBroadcastHeader(header))
		require.Error(t, err)
		stopped, stop := context.WithCancel(ctx)
		stop()
		_, err = h.BroadcastBegin(stopped, beginTxnMessage(1))
		require.ErrorIs(t, err, context.Canceled)
		huge := message.NewDropCollectionMessageBuilderV1().WithHeader(&message.DropCollectionMessageHeader{}).WithBody(&msgpb.DropCollectionRequest{CollectionName: strings.Repeat("x", maxTxnMemberBytes)}).WithBroadcast([]string{"v1"}).MustBuildBroadcast()
		_, err = h.BroadcastBegin(ctx, huge.OverwriteBroadcastHeader(header))
		require.Error(t, err)
		_, err = h.BroadcastBegin(ctx, beginTxnMessage(1))
		require.NoError(t, err)
		// A different admission identity cannot overwrite an existing business TxnID.
		other, _, err := bm.StartTxnBroadcastWithResourceKey(ctx, rk)
		require.NoError(t, err)
		_, err = other.BroadcastBegin(ctx, beginTxnMessage(1))
		require.Error(t, err)
		other.Close()
		_, err = h.BroadcastBody(ctx, huge)
		require.Error(t, err)
		body := message.NewDropCollectionMessageBuilderV1().WithHeader(&message.DropCollectionMessageHeader{}).WithBody(&msgpb.DropCollectionRequest{CollectionName: strings.Repeat("x", 200*1024)}).WithBroadcast([]string{"v1"}).MustBuildBroadcast()
		_, err = h.BroadcastBody(ctx, body)
		require.NoError(t, err)
		_, err = h.BroadcastBody(ctx, body)
		require.NoError(t, err)
		// Exceeding the old 512 KiB aggregate limit is now allowed.
		_, err = h.BroadcastBody(ctx, body)
		require.NoError(t, err)
		_, err = h.BroadcastCommit(ctx, createNewBroadcastMsg([]string{"v1"}))
		require.NoError(t, err)
	})
}

func TestTxnRecoveryRejectsInconsistentGroup(t *testing.T) {
	mockey.PatchConvey("invalid task groups never restore partial ownership", t, func() {
		bm, store := setupTxnTest(t)
		defer bm.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		h, _, err := bm.StartTxnBroadcastWithResourceKey(ctx)
		require.NoError(t, err)
		_, err = h.BroadcastBegin(ctx, beginTxnMessage(44))
		require.NoError(t, err)
		open := store.snapshot(44)
		require.NoError(t, validateBroadcastTxn(open))
		mutateHeader := func(tasks []*streamingpb.BroadcastTask, fn func(*messagespb.BroadcastTxnContext)) {
			msg := message.NewBroadcastMutableMessageBeforeAppend(tasks[0].Message.Payload, tasks[0].Message.Properties)
			header := msg.BroadcastHeader()
			fn(header.Txn)
			tasks[0].Message = msg.OverwriteBroadcastHeader(header).IntoMessageProto()
		}
		cases := []func([]*streamingpb.BroadcastTask){
			func(tasks []*streamingpb.BroadcastTask) {
				mutateHeader(tasks, func(tc *messagespb.BroadcastTxnContext) { tc.TxnId = 0 })
			},
			func(tasks []*streamingpb.BroadcastTask) {
				mutateHeader(tasks, func(tc *messagespb.BroadcastTxnContext) { tc.Sequence = 3 })
			},
			func(tasks []*streamingpb.BroadcastTask) {
				mutateHeader(tasks, func(tc *messagespb.BroadcastTxnContext) {
					tc.Kind = messagespb.BroadcastTxnKind_BROADCAST_TXN_KIND_BODY
				})
			},
			func(tasks []*streamingpb.BroadcastTask) {
				tasks[0].State = streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_REPLICATED
			},
			func(tasks []*streamingpb.BroadcastTask) {
				tasks[0].State = streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE
			},
		}
		require.Error(t, validateBroadcastTxn(nil))
		require.Error(t, validateBroadcastTxn(append(open, open[0])))
		for _, mutate := range cases {
			broken := []*streamingpb.BroadcastTask{proto.Clone(open[0]).(*streamingpb.BroadcastTask)}
			mutate(broken)
			_, _, err := splitBroadcastTasks(broken)
			require.Error(t, err)
		}
		require.Error(t, bm.DropTombstone(ctx, message.NewBroadcastMutableMessageBeforeAppend(open[0].Message.Payload, open[0].Message.Properties).BroadcastHeader().BroadcastID))
		end, err := h.BroadcastCommit(ctx, createNewBroadcastMsg([]string{"v1"}))
		require.NoError(t, err)
		store.mu.Lock()
		store.beforeSave = func(changes map[uint64]*streamingpb.BroadcastTask) error {
			for _, task := range changes {
				if task.State == streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_DONE {
					return context.DeadlineExceeded
				}
			}
			return nil
		}
		store.mu.Unlock()
		require.Error(t, bm.DropTombstone(ctx, end.BroadcastResult.BroadcastID))
		require.Len(t, store.snapshot(44), 2)
		_, err = bm.RecoverTxnBroadcast(ctx, 44)
		require.NoError(t, err)
		store.mu.Lock()
		store.beforeSave = nil
		store.mu.Unlock()
		require.NoError(t, bm.DropTombstone(ctx, end.BroadcastResult.BroadcastID))
	})
}

func TestTxnPersistsOnlyChangedTaskKeys(t *testing.T) {
	mockey.PatchConvey("single-task ACK saves and atomic terminal and GC batches", t, func() {
		bm, store := setupTxnTest(t)
		defer bm.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var closedBatch, deletedBatch atomic.Bool
		store.beforeSave = func(changes map[uint64]*streamingpb.BroadcastTask) error {
			deleting, closing := false, false
			for _, task := range changes {
				if task.State == streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_DONE {
					deleting = true
					continue
				}
				header := message.NewBroadcastMutableMessageBeforeAppend(task.Message.Payload, task.Message.Properties).BroadcastHeader()
				if header.Txn.GetKind() == messagespb.BroadcastTxnKind_BROADCAST_TXN_KIND_COMMIT && task.State == streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE {
					closing = true
				}
			}
			switch {
			case deleting:
				require.Len(t, changes, 4, "GC must delete all four independent task keys in one call")
				deletedBatch.Store(true)
			case closing:
				require.Len(t, changes, 2, "only Begin and Commit are rewritten at terminal completion")
				for id, task := range changes {
					require.Equal(t, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE, task.State)
					prior := store.tasks[id]
					require.NotNil(t, prior)
					require.NotEqual(t, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE, prior.State)
				}
				closedBatch.Store(true)
			default:
				require.Len(t, changes, 1, "ordinary member/ACK updates must not rewrite the group")
			}
			return nil
		}
		h, _, err := bm.StartTxnBroadcastWithResourceKey(ctx)
		require.NoError(t, err)
		_, err = h.BroadcastBegin(ctx, beginTxnMessage(200))
		require.NoError(t, err)
		for i := 0; i < 2; i++ {
			_, err = h.BroadcastBody(ctx, createNewBroadcastMsg([]string{"v1"}))
			require.NoError(t, err)
		}
		require.Len(t, store.snapshot(200), 3)
		commit, err := h.BroadcastCommit(ctx, createNewBroadcastMsg([]string{"v1"}))
		require.NoError(t, err)
		require.True(t, closedBatch.Load())
		require.NoError(t, bm.DropTombstone(ctx, commit.BroadcastResult.BroadcastID))
		require.True(t, deletedBatch.Load())
		require.Empty(t, store.snapshot(200))
	})
}

func TestTxnRecoveryGroupsUnorderedTaskKeys(t *testing.T) {
	mockey.PatchConvey("one catalog listing recovers ordinary tasks and transaction groups", t, func() {
		bm, store := setupTxnTest(t)
		defer bm.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for _, id := range []uint64{201, 202} {
			h, _, err := bm.StartTxnBroadcastWithResourceKey(ctx)
			require.NoError(t, err)
			_, err = h.BroadcastBegin(ctx, beginTxnMessage(id))
			require.NoError(t, err)
			_, err = h.BroadcastBody(ctx, createNewBroadcastMsg([]string{"v1"}))
			require.NoError(t, err)
			_, err = h.BroadcastCommit(ctx, createNewBroadcastMsg([]string{"v1"}))
			require.NoError(t, err)
		}
		first, second := store.snapshot(201), store.snapshot(202)
		// WAL members from different groups and an old ordinary task are interleaved.
		ordinary := createNewBroadcastTask(999, []string{"v1"})
		tasks, groups, err := splitBroadcastTasks([]*streamingpb.BroadcastTask{first[2], second[1], ordinary, second[2], first[0], second[0], first[1]})
		require.NoError(t, err)
		require.Equal(t, []*streamingpb.BroadcastTask{ordinary}, tasks)
		require.Len(t, groups, 2)
		for _, group := range groups {
			require.Len(t, group, 3)
			require.NoError(t, validateBroadcastTxn(group))
		}
		// Missing Begin and missing middle members are both corruption, never legacy fallback.
		_, _, err = splitBroadcastTasks(first[1:])
		require.Error(t, err)
		_, _, err = splitBroadcastTasks([]*streamingpb.BroadcastTask{first[0], first[2]})
		require.Error(t, err)
	})
}
