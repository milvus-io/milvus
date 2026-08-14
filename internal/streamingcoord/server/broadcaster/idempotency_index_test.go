package broadcaster

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/mocks/distributed/mock_streaming"
	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestIdempotencyIndex(t *testing.T) {
	idx := newIdempotencyIndex()

	// An empty key never enters the index.
	idx.Add("", 1)
	_, ok := idx.Get("")
	require.False(t, ok)

	idx.Add("import/1/k", 100)
	id, ok := idx.Get("import/1/k")
	require.True(t, ok)
	require.Equal(t, uint64(100), id)

	// On a key collision the first broadcastID wins: idempotency means a retry
	// resolves to the ORIGINAL result, not to whichever retry raced in last.
	idx.Add("import/1/k", 200)
	id, ok = idx.Get("import/1/k")
	require.True(t, ok)
	require.Equal(t, uint64(100), id)

	// Removal only applies when the broadcastID matches, so a task that lost the
	// Add race cannot evict the owner's entry when its own tombstone is GC'd.
	idx.Remove("import/1/k", 200)
	id, ok = idx.Get("import/1/k")
	require.True(t, ok)
	require.Equal(t, uint64(100), id)

	idx.Remove("import/1/k", 100)
	_, ok = idx.Get("import/1/k")
	require.False(t, ok)
}

// createImportBroadcastTaskProto builds a recovery proto of an import broadcast task
// carrying the given idempotency key ("" means the task carries no key at all).
func createImportBroadcastTaskProto(broadcastID uint64, key string, state streamingpb.BroadcastTaskState, bitmap []byte) *streamingpb.BroadcastTask {
	msg := message.NewImportMessageBuilderV1().
		WithHeader(&message.ImportMessageHeader{}).
		WithBody(&msgpb.ImportMsg{}).
		WithIdempotencyKey(key).
		WithBroadcast([]string{"v1"}).
		MustBuildBroadcast().
		OverwriteBroadcastHeader(broadcastID, message.NewSharedClusterResourceKey())
	return createNewWaitAckBroadcastTaskFromMessage(msg, state, bitmap)
}

// TestIdempotencyIndexRecovery asserts the index is rebuilt from the recovery info
// across every task state, tombstones included: a tombstoned task is still what a
// late retry must resolve to, until the tombstone GC drops it.
func TestIdempotencyIndexRecovery(t *testing.T) {
	paramtable.Init()
	registry.ResetRegistration()
	// Earlier tests in this package shrink the tombstone GC window to milliseconds and
	// never restore it. Pin a long window here so the GC cannot race the assertions.
	oldInterval := paramtable.Get().StreamingCfg.WALBroadcasterTombstoneCheckInternal.SwapTempValue("1h")
	oldLifetime := paramtable.Get().StreamingCfg.WALBroadcasterTombstoneMaxLifetime.SwapTempValue("1h")
	oldCount := paramtable.Get().StreamingCfg.WALBroadcasterTombstoneMaxCount.SwapTempValue("8192")
	defer func() {
		paramtable.Get().StreamingCfg.WALBroadcasterTombstoneCheckInternal.SwapTempValue(oldInterval)
		paramtable.Get().StreamingCfg.WALBroadcasterTombstoneMaxLifetime.SwapTempValue(oldLifetime)
		paramtable.Get().StreamingCfg.WALBroadcasterTombstoneMaxCount.SwapTempValue(oldCount)
	}()

	meta := mock_metastore.NewMockStreamingCoordCataLog(t)
	meta.EXPECT().SaveBroadcastTask(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	resource.InitForTest(resource.OptStreamingCatalog(meta))

	// The recovered PENDING task is re-appended by the broadcast scheduler; serve it a
	// WAL that always succeeds and never acks, so the task simply waits for acks.
	operator := mock_streaming.NewMockWALAccesser(t)
	appendFn := func(ctx context.Context, msgs ...message.MutableMessage) types.AppendResponses {
		resps := types.AppendResponses{Responses: make([]types.AppendResponse, len(msgs))}
		for idx := range msgs {
			resps.Responses[idx] = types.AppendResponse{
				AppendResult: &types.AppendResult{
					MessageID: walimplstest.NewTestMessageID(int64(idx + 1)),
					TimeTick:  uint64(time.Now().UnixMilli()),
				},
			}
		}
		return resps
	}
	operator.EXPECT().AppendMessages(mock.Anything, mock.Anything).RunAndReturn(appendFn).Maybe()
	streaming.SetWALForTest(operator)

	bm := newBroadcastTaskManager([]*streamingpb.BroadcastTask{
		createImportBroadcastTaskProto(100, "import/1/pending",
			streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING, []byte{0x00}),
		createImportBroadcastTaskProto(200, "import/1/tombstone",
			streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE, []byte{0x01}),
		createImportBroadcastTaskProto(300, "",
			streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE, []byte{0x01}),
	})
	defer bm.Close()

	original, ok := bm.getOriginalBroadcastMessage("import/1/pending")
	require.True(t, ok)
	require.Equal(t, uint64(100), original.BroadcastHeader().BroadcastID)

	original, ok = bm.getOriginalBroadcastMessage("import/1/tombstone")
	require.True(t, ok)
	require.Equal(t, uint64(200), original.BroadcastHeader().BroadcastID)

	// The keyless task must not occupy an entry.
	require.Len(t, bm.idempotencyIndex.keyToBroadcastID, 2)
}

// newBroadcastTaskManagerForTest recovers a broadcast task manager with the same test
// resources TestIdempotencyIndexRecovery sets up: a catalog that accepts every save, a
// WAL that always appends successfully, and a tombstone GC window long enough that the
// GC cannot race the assertions.
func newBroadcastTaskManagerForTest(t *testing.T, protos ...*streamingpb.BroadcastTask) *broadcastTaskManager {
	paramtable.Init()
	registry.ResetRegistration()
	// A completed import broadcast triggers the import ack callback; without a registered
	// one the ack scheduler blocks forever waiting for the registration future.
	registry.RegisterImportV1AckCallback(func(ctx context.Context, result message.BroadcastResultImportMessageV1) error {
		return nil
	})
	oldInterval := paramtable.Get().StreamingCfg.WALBroadcasterTombstoneCheckInternal.SwapTempValue("1h")
	oldLifetime := paramtable.Get().StreamingCfg.WALBroadcasterTombstoneMaxLifetime.SwapTempValue("1h")
	oldCount := paramtable.Get().StreamingCfg.WALBroadcasterTombstoneMaxCount.SwapTempValue("8192")
	t.Cleanup(func() {
		paramtable.Get().StreamingCfg.WALBroadcasterTombstoneCheckInternal.SwapTempValue(oldInterval)
		paramtable.Get().StreamingCfg.WALBroadcasterTombstoneMaxLifetime.SwapTempValue(oldLifetime)
		paramtable.Get().StreamingCfg.WALBroadcasterTombstoneMaxCount.SwapTempValue(oldCount)
	})

	meta := mock_metastore.NewMockStreamingCoordCataLog(t)
	meta.EXPECT().SaveBroadcastTask(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	resource.InitForTest(resource.OptStreamingCatalog(meta))

	operator := mock_streaming.NewMockWALAccesser(t)
	appendFn := func(ctx context.Context, msgs ...message.MutableMessage) types.AppendResponses {
		resps := types.AppendResponses{Responses: make([]types.AppendResponse, len(msgs))}
		for idx := range msgs {
			resps.Responses[idx] = types.AppendResponse{
				AppendResult: &types.AppendResult{
					MessageID: walimplstest.NewTestMessageID(int64(idx + 1)),
					TimeTick:  uint64(time.Now().UnixMilli()),
				},
			}
		}
		return resps
	}
	operator.EXPECT().AppendMessages(mock.Anything, mock.Anything).RunAndReturn(appendFn).Maybe()
	streaming.SetWALForTest(operator)

	bm := newBroadcastTaskManager(protos)
	t.Cleanup(bm.Close)
	return bm
}

func TestBroadcastReturnsDuplicatedOnKeyHit(t *testing.T) {
	// Recover a manager that already owns "import/1/k" from a tombstoned task:
	// a tombstone is exactly what a late retry is expected to hit.
	bm := newBroadcastTaskManagerForTest(t,
		createImportBroadcastTaskProto(100, "import/1/k",
			streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE, []byte{0x01}))

	// Hold an EXCLUSIVE key, mirroring what import actually holds in production
	// (SharedDBName + ExclusiveCollectionName). A shared key would not conflict
	// with itself, which would make the release check below vacuous.
	collectionKey := message.NewExclusiveCollectionNameResourceKey("db1", "coll1")
	guards := bm.resourceKeyLocker.Lock(collectionKey)
	api := &broadcasterWithRK{broadcaster: bm, broadcastID: 999, guards: guards}

	msg := message.NewImportMessageBuilderV1().
		WithHeader(&message.ImportMessageHeader{}).
		WithBody(&msgpb.ImportMsg{}).
		WithIdempotencyKey("import/1/k").
		WithBroadcast([]string{"v1"}).
		MustBuildBroadcast()

	result, err := api.Broadcast(context.Background(), msg)
	require.NoError(t, err)
	require.NotNil(t, result.Duplicated)
	// BroadcastID must be the ORIGINAL broadcast's ID, not the freshly allocated
	// 999 that went unused.
	require.Equal(t, uint64(100), result.BroadcastID)
	require.Nil(t, result.AppendResults)
	require.Equal(t, "import/1/k", message.IdempotencyKeyOf(result.Duplicated))

	// On a hit the guards were never consumed, so Close() must actually release
	// them — a leaked guard would block every later import on that collection
	// forever. Contend on the SAME exclusive key to prove both halves: still held
	// before Close, actually released after.
	require.NotNil(t, api.guards)

	released := make(chan struct{})
	go func() {
		bm.resourceKeyLocker.Lock(collectionKey).Unlock()
		close(released)
	}()
	select {
	case <-released:
		t.Fatal("exclusive resource key was acquirable while the broadcast still held it")
	case <-time.After(200 * time.Millisecond):
	}

	api.Close()
	select {
	case <-released:
	case <-time.After(3 * time.Second):
		t.Fatal("resource key lock was not released after a deduplicated broadcast")
	}
}

func TestBroadcastWithoutKeyIsUnaffected(t *testing.T) {
	// A broadcast carrying no idempotency key follows the original path unchanged:
	// it creates a task and consumes the guards by handing them to it.
	//
	// This does NOT prove the `key != ""` guard in Broadcast is present — an empty
	// key is a structural miss in the index (Get("") always returns false), so the
	// observable outcome is identical with or without the guard. The guard exists
	// to skip a pointless lookup, not for correctness, and is deliberately left
	// unasserted rather than pinned by a test that cannot fail.
	bm := newBroadcastTaskManagerForTest(t)

	guards := bm.resourceKeyLocker.Lock(message.NewSharedClusterResourceKey())
	api := &broadcasterWithRK{broadcaster: bm, broadcastID: 999, guards: guards}

	msg := message.NewImportMessageBuilderV1().
		WithHeader(&message.ImportMessageHeader{}).
		WithBody(&msgpb.ImportMsg{}).
		WithBroadcast([]string{"v1"}).
		MustBuildBroadcast()

	result, err := api.Broadcast(context.Background(), msg)
	require.NoError(t, err)
	require.Nil(t, result.Duplicated)
	require.Equal(t, uint64(999), result.BroadcastID)
	// The keyless path consumes the guards: they now belong to the created task.
	require.Nil(t, api.guards)
}
