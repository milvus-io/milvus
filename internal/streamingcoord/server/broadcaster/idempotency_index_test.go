package broadcaster

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/mocks/distributed/mock_streaming"
	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	internaltypes "github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/idalloc"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
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

// testCollectionID is the collection every scoped test key below is bound to, unless
// a test names another one to prove two collections stay apart.
const testCollectionID = int64(7)

// newImportMsgWithKey builds an import broadcast message carrying the given client
// idempotency key ("" means the message carries no key at all), scoped to
// testCollectionID, as it looks when it reaches the broadcaster: not yet stamped with
// a broadcastID or resource keys.
func newImportMsgWithKey(key string) message.BroadcastMutableMessage {
	return newImportMsgScoped(testCollectionID, key)
}

// newImportMsgScoped is newImportMsgWithKey against a named collection.
func newImportMsgScoped(collectionID int64, key string) message.BroadcastMutableMessage {
	return message.NewImportMessageBuilderV1().
		WithHeader(&message.ImportMessageHeader{}).
		WithBody(&msgpb.ImportMsg{}).
		WithIdempotencyKey(message.NewCollectionScopedIdempotencyKey(collectionID, key)).
		WithBroadcast([]string{"v1"}).
		MustBuildBroadcast()
}

// newCreateCollectionMsgWithKey builds a broadcast message of a DIFFERENT message type
// carrying the given client idempotency key. Used to prove the message type is part of
// the dedup identity.
func newCreateCollectionMsgWithKey(key string) message.BroadcastMutableMessage {
	return message.NewCreateCollectionMessageBuilderV1().
		WithHeader(&message.CreateCollectionMessageHeader{}).
		WithBody(&msgpb.CreateCollectionRequest{}).
		WithIdempotencyKey(message.NewCollectionScopedIdempotencyKey(testCollectionID, key)).
		WithBroadcast([]string{"v1"}).
		MustBuildBroadcast()
}

// createImportBroadcastTaskProto builds a recovery proto of an import broadcast task
// carrying the given idempotency key ("" means the task carries no key at all) and the
// given resource keys, as the broadcaster would have stamped them.
func createImportBroadcastTaskProto(
	broadcastID uint64,
	key string,
	state streamingpb.BroadcastTaskState,
	bitmap []byte,
	rks ...message.ResourceKey,
) *streamingpb.BroadcastTask {
	if len(rks) == 0 {
		rks = []message.ResourceKey{message.NewSharedClusterResourceKey()}
	}
	msg := newImportMsgWithKey(key).OverwriteBroadcastHeader(broadcastID, rks...)
	return createNewWaitAckBroadcastTaskFromMessage(msg, state, bitmap)
}

// broadcastForTest drives one broadcast through broadcasterWithRK holding exactly the
// given resource keys, the same way broadcastTaskManager.WithResourceKeys would.
func broadcastForTest(
	t *testing.T,
	bm *broadcastTaskManager,
	broadcastID uint64,
	msg message.BroadcastMutableMessage,
	rks ...message.ResourceKey,
) *types.BroadcastAppendResult {
	t.Helper()
	api := &broadcasterWithRK{broadcaster: bm, broadcastID: broadcastID, guards: bm.resourceKeyLocker.Lock(rks...)}
	defer api.Close()

	result, err := api.Broadcast(context.Background(), msg)
	require.NoError(t, err)
	return result
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

	// The recovered entries are indexed by scope, not by the raw client key: the
	// recovery rebuild must derive the very same scope the broadcast path derives.
	scopeOf := func(clientKey string) string {
		return idempotencyScope(message.MessageTypeImport,
			message.NewCollectionScopedIdempotencyKey(testCollectionID, clientKey))
	}

	original, _, ok := bm.getOriginalBroadcast(scopeOf("import/1/pending"))
	require.True(t, ok)
	require.Equal(t, uint64(100), original.BroadcastHeader().BroadcastID)

	original, _, ok = bm.getOriginalBroadcast(scopeOf("import/1/tombstone"))
	require.True(t, ok)
	require.Equal(t, uint64(200), original.BroadcastHeader().BroadcastID)

	// The raw client key alone must never resolve: an unscoped lookup is exactly the
	// bug that makes the same key collide across collections and message types.
	_, _, ok = bm.getOriginalBroadcast("import/1/pending")
	require.False(t, ok)

	// The keyless task must not occupy an entry.
	require.Len(t, bm.idempotencyIndex.scopeToBroadcastID, 2)
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
	registry.RegisterCreateCollectionV1AckCallback(func(ctx context.Context, result message.BroadcastResultCreateCollectionMessageV1) error {
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
	// Hold an EXCLUSIVE key, mirroring what import actually holds in production
	// (SharedDBName + ExclusiveCollectionName). A shared key would not conflict
	// with itself, which would make the release check below vacuous.
	collectionKey := message.NewExclusiveCollectionNameResourceKey("db1", "coll1")

	// Recover a manager that already owns "import/1/k" from a tombstoned task:
	// a tombstone is exactly what a late retry is expected to hit. The original must
	// carry the SAME resource keys the retry holds, otherwise it is a different scope.
	bm := newBroadcastTaskManagerForTest(t,
		createImportBroadcastTaskProto(100, "import/1/k",
			streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE, []byte{0x01}, collectionKey))

	guards := bm.resourceKeyLocker.Lock(collectionKey)
	api := &broadcasterWithRK{broadcaster: bm, broadcastID: 999, guards: guards}

	result, err := api.Broadcast(context.Background(), newImportMsgWithKey("import/1/k"))
	require.NoError(t, err)
	require.NotNil(t, result.Duplicated)
	// BroadcastID must be the ORIGINAL broadcast's ID, not the freshly allocated
	// 999 that went unused.
	require.Equal(t, uint64(100), result.BroadcastID)
	require.Equal(t, message.NewCollectionScopedIdempotencyKey(testCollectionID, "import/1/k"),
		message.IdempotencyKeyOf(result.Duplicated))

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

// TestIdempotencyScopeEncoding pins what makes two broadcasts the same operation.
// A client key alone is not an identity: it deduplicates within
// (messageType, the scope the caller bound the key to).
func TestIdempotencyScopeEncoding(t *testing.T) {
	keyA := message.NewCollectionScopedIdempotencyKey(7, "k")

	base := idempotencyScope(message.MessageTypeImport, keyA)
	require.NotEmpty(t, base)

	// A broadcast without a client key is not idempotent and has no scope at all.
	require.Empty(t, idempotencyScope(message.MessageTypeImport, ""))

	// Same identity, same scope.
	require.Equal(t, base, idempotencyScope(message.MessageTypeImport, keyA))

	// The message type is part of the identity: CreateIndex and DropIndex on one
	// collection carry the same scope otherwise, so without it the same client key on
	// two different operations would collide and the second would be swallowed.
	require.NotEqual(t, base, idempotencyScope(message.MessageTypeCreateCollection, keyA))

	// So is the scope the caller bound the key to.
	require.NotEqual(t, base, idempotencyScope(message.MessageTypeImport,
		message.NewCollectionScopedIdempotencyKey(8, "k")))
	require.NotEqual(t, base, idempotencyScope(message.MessageTypeImport,
		message.NewDatabaseScopedIdempotencyKey(7, "k")))
	require.NotEqual(t, base, idempotencyScope(message.MessageTypeImport,
		message.NewClusterScopedIdempotencyKey("k")))

	// And so is the client key itself.
	require.NotEqual(t, base, idempotencyScope(message.MessageTypeImport,
		message.NewCollectionScopedIdempotencyKey(7, "k2")))

	// The message type is decimal digits followed by a separator, and the key is the
	// unbounded tail, so a client key that embeds the separator cannot pose as another
	// message type's scope.
	require.NotEqual(t,
		idempotencyScope(message.MessageTypeImport,
			message.NewCollectionScopedIdempotencyKey(7, "/"+strconv.Itoa(int(message.MessageTypeCreateCollection)))),
		idempotencyScope(message.MessageTypeCreateCollection, message.NewCollectionScopedIdempotencyKey(7, "")))
}

// TestIdempotencyScopeIgnoresResourceKeys is the property that closes the rename hole.
//
// The scope used to be derived from the broadcast's resource keys, which name a
// collection rather than identify it: renaming a collection between a request and its
// retry changed the scope, the lookup missed, and the same import ran a second time.
// The scope now comes from the message alone, so the resource keys a broadcast happens
// to hold -- and any rename that changes them -- cannot move it.
func TestIdempotencyScopeIgnoresResourceKeys(t *testing.T) {
	msg := newImportMsgWithKey("k")
	bare := idempotencyScopeOfMessage(msg)
	require.NotEmpty(t, bare)

	beforeRename := msg.OverwriteBroadcastHeader(100,
		message.NewSharedDBNameResourceKey("db1"),
		message.NewExclusiveCollectionNameResourceKey("db1", "coll1"))
	afterRename := newImportMsgWithKey("k").OverwriteBroadcastHeader(101,
		message.NewSharedDBNameResourceKey("db1"),
		message.NewExclusiveCollectionNameResourceKey("db1", "coll1-renamed"))

	require.Equal(t, bare, idempotencyScopeOfMessage(beforeRename))
	require.Equal(t, bare, idempotencyScopeOfMessage(afterRename))
}

// TestBroadcastDedupIsScopedToTheBroadcast drives the write side and the read side
// against each other end to end: every broadcast below goes through
// broadcasterWithRK.Broadcast, so a scope the two sides derive differently shows up
// here as a duplicate that is never detected.
func TestBroadcastDedupIsScopedToTheBroadcast(t *testing.T) {
	bm := newBroadcastTaskManagerForTest(t)

	// Shared keys throughout: a created task keeps its lock guards until it is acked,
	// and an exclusive key would then block the next broadcast of this test forever.
	dbKey := message.NewSharedDBNameResourceKey("db1")
	collA := message.NewSharedCollectionNameResourceKey("db1", "coll1")
	collB := message.NewSharedCollectionNameResourceKey("db1", "coll2")

	// The first broadcast establishes the scope.
	first := broadcastForTest(t, bm, 1, newImportMsgWithKey("k"), dbKey, collA)
	require.Nil(t, first.Duplicated)
	require.Equal(t, uint64(1), first.BroadcastID)

	// Same message type, same resource keys, same client key: a duplicate, resolved to
	// the ORIGINAL broadcastID. This is the assertion that fails if the write side and
	// the read side disagree on how the scope is built.
	dup := broadcastForTest(t, bm, 2, newImportMsgWithKey("k"), collA, dbKey)
	require.NotNil(t, dup.Duplicated)
	require.Equal(t, uint64(1), dup.BroadcastID)

	// Different message type, everything else identical: a different operation, so it
	// must proceed on its own broadcastID instead of being swallowed as a duplicate.
	otherType := broadcastForTest(t, bm, 3, newCreateCollectionMsgWithKey("k"), dbKey, collA)
	require.Nil(t, otherType.Duplicated)
	require.Equal(t, uint64(3), otherType.BroadcastID)

	// Another collection scope, everything else identical: a distinct operation.
	otherKeys := broadcastForTest(t, bm, 4, newImportMsgScoped(testCollectionID+1, "k"), dbKey, collB)
	require.Nil(t, otherKeys.Duplicated)
	require.Equal(t, uint64(4), otherKeys.BroadcastID)

	// Different client key.
	otherKey := broadcastForTest(t, bm, 5, newImportMsgWithKey("k2"), dbKey, collA)
	require.Nil(t, otherKey.Duplicated)
	require.Equal(t, uint64(5), otherKey.BroadcastID)

	// Each of those scopes now dedups within itself, the second message type included.
	dupOtherType := broadcastForTest(t, bm, 6, newCreateCollectionMsgWithKey("k"), dbKey, collA)
	require.NotNil(t, dupOtherType.Duplicated)
	require.Equal(t, uint64(3), dupOtherType.BroadcastID)

	dupOtherKeys := broadcastForTest(t, bm, 7, newImportMsgScoped(testCollectionID+1, "k"), dbKey, collB)
	require.NotNil(t, dupOtherKeys.Duplicated)
	require.Equal(t, uint64(4), dupOtherKeys.BroadcastID)

	// A retry that arrives after the collection was renamed holds DIFFERENT resource
	// keys and still resolves to its original broadcast: the scope is the collection's
	// identity, not its name. Under the resource-key scope this was a miss, and the
	// same files were imported a second time.
	renamedColl := message.NewSharedCollectionNameResourceKey("db1", "coll1-renamed")
	afterRename := broadcastForTest(t, bm, 8, newImportMsgWithKey("k"), dbKey, renamedColl)
	require.NotNil(t, afterRename.Duplicated)
	require.Equal(t, uint64(1), afterRename.BroadcastID)
}

// TestDuplicatedBroadcastReturnsOriginalAppendResults asserts the duplicate carries the
// original broadcast's per-vchannel append results. A caller that dereferences
// GetAppendResult on the duplicate path would nil-panic without them.
func TestDuplicatedBroadcastReturnsOriginalAppendResults(t *testing.T) {
	collKey := message.NewSharedCollectionNameResourceKey("db1", "coll1")
	// A tombstoned original with its single vchannel acked: bitmap 0x01 gives vchannel
	// "v1" a checkpoint of test message id 0 at time tick 1.
	bm := newBroadcastTaskManagerForTest(t,
		createImportBroadcastTaskProto(100, "k",
			streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE, []byte{0x01}, collKey))

	result := broadcastForTest(t, bm, 999, newImportMsgWithKey("k"), collKey)
	require.NotNil(t, result.Duplicated)
	require.Equal(t, uint64(100), result.BroadcastID)

	require.Len(t, result.AppendResults, 1)
	appendResult := result.AppendResults["v1"]
	require.NotNil(t, appendResult)
	require.Equal(t, uint64(1), appendResult.TimeTick)
	require.True(t, appendResult.MessageID.EQ(walimplstest.NewTestMessageID(0)))
	require.True(t, appendResult.LastConfirmedMessageID.EQ(walimplstest.NewTestMessageID(0)))
}

// TestDuplicatedBroadcastAgainstUnackedOriginalDoesNotPanic covers the case the
// reconstruction cannot assume away: the original task exists but has not been acked on
// every vchannel yet, so it has no checkpoints to zip. Reconstructing them there would
// hit the "BroadcastResult is called before the broadcast task is acked" panic — inside
// a coordinator, on a path no success-path test reaches.
func TestDuplicatedBroadcastAgainstUnackedOriginalDoesNotPanic(t *testing.T) {
	collKey := message.NewSharedCollectionNameResourceKey("db1", "coll1")
	// A replicated task with an empty ack bitmap: recovery neither re-appends nor acks
	// it, so it stays unacked for the whole test.
	bm := newBroadcastTaskManagerForTest(t,
		createImportBroadcastTaskProto(100, "k",
			streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_REPLICATED, []byte{0x00}, collKey))

	var result *types.BroadcastAppendResult
	require.NotPanics(t, func() {
		result = broadcastForTest(t, bm, 999, newImportMsgWithKey("k"), collKey)
	})
	// The duplicate is still reported, just without results to hand back.
	require.NotNil(t, result.Duplicated)
	require.Equal(t, uint64(100), result.BroadcastID)
	require.Nil(t, result.AppendResults)
}

// TestBroadcastRejectsOversizedIdempotencyKey covers the length bound at the
// broadcaster, the only place it is enforced and the one point every entry path
// passes through.
func TestBroadcastRejectsOversizedIdempotencyKey(t *testing.T) {
	bm := newBroadcastTaskManagerForTest(t)
	collKey := message.NewSharedCollectionNameResourceKey("db1", "coll1")

	broadcast := func(msg message.BroadcastMutableMessage) error {
		api := &broadcasterWithRK{broadcaster: bm, broadcastID: 1, guards: bm.resourceKeyLocker.Lock(collKey)}
		defer api.Close()
		_, err := api.Broadcast(context.Background(), msg)
		return err
	}

	old := paramtable.Get().StreamingCfg.IdempotencyMaxKeyLength.SwapTempValue("8")
	defer paramtable.Get().StreamingCfg.IdempotencyMaxKeyLength.SwapTempValue(old)

	// The bound is inclusive.
	require.NoError(t, broadcast(newImportMsgWithKey("12345678")))
	err := broadcast(newImportMsgWithKey("123456789"))
	require.Error(t, err)
	require.ErrorIs(t, err, merr.ErrParameterInvalid)

	// A limit of 0 fails closed: no non-empty key is accepted at all, while a broadcast
	// that carries no key is not idempotent and is never rejected here.
	paramtable.Get().StreamingCfg.IdempotencyMaxKeyLength.SwapTempValue("0")
	require.Error(t, broadcast(newImportMsgWithKey("1")))
	require.NoError(t, broadcast(newImportMsgWithKey("")))
}

// TestBroadcastAcceptsIdempotencyKeyAtTheConfiguredLimit guards the length budget at
// its real boundary, with the real default limit. A client is told it may send a raw
// key of up to streaming.idempotency.maxKeyLength bytes, so anything that inflates the
// key on its way here — a scoping prefix, an encoding — makes the broadcaster reject a
// key the user was told is legal. A short-key test cannot catch that; only a key
// sitting exactly on the limit can.
func TestBroadcastAcceptsIdempotencyKeyAtTheConfiguredLimit(t *testing.T) {
	bm := newBroadcastTaskManagerForTest(t)
	collKey := message.NewSharedCollectionNameResourceKey("db1", "coll1")

	limit := paramtable.Get().StreamingCfg.IdempotencyMaxKeyLength.GetAsInt()
	require.Equal(t, 256, limit, "this test asserts the boundary of the DEFAULT limit, the one advertised to clients")

	atLimit := strings.Repeat("k", limit)
	first := broadcastForTest(t, bm, 1, newImportMsgWithKey(atLimit), collKey)
	require.Nil(t, first.Duplicated)
	require.Equal(t, uint64(1), first.BroadcastID)

	// The key survived at full length: a retry of it still deduplicates, which is only
	// possible if the broadcaster indexed the whole key rather than rejecting it.
	dup := broadcastForTest(t, bm, 2, newImportMsgWithKey(atLimit), collKey)
	require.NotNil(t, dup.Duplicated)
	require.Equal(t, uint64(1), dup.BroadcastID)
	require.Equal(t, atLimit, message.IdempotencyKeyOf(dup.Duplicated).ClientKey())
	// The scoping prefix really does push the stored value past the advertised limit;
	// that is exactly why the bound is taken over the client portion and not over the
	// encoded key.
	require.Greater(t, len(message.IdempotencyKeyOf(dup.Duplicated)), limit)

	// One byte past the limit is where rejection starts.
	api := &broadcasterWithRK{broadcaster: bm, broadcastID: 3, guards: bm.resourceKeyLocker.Lock(collKey)}
	defer api.Close()
	_, err := api.Broadcast(context.Background(), newImportMsgWithKey(atLimit+"k"))
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
}

// createBroadcastTaskProtoFromMessage builds a recovery proto for an arbitrary
// broadcast message, so a test can drive the dedup path with a message type other
// than import.
func createBroadcastTaskProtoFromMessage(
	msg message.BroadcastMutableMessage,
	broadcastID uint64,
	state streamingpb.BroadcastTaskState,
	bitmap []byte,
	rks ...message.ResourceKey,
) *streamingpb.BroadcastTask {
	if len(rks) == 0 {
		rks = []message.ResourceKey{message.NewSharedClusterResourceKey()}
	}
	return createNewWaitAckBroadcastTaskFromMessage(msg.OverwriteBroadcastHeader(broadcastID, rks...), state, bitmap)
}

// TestNonImportBroadcastIsDeduplicated is the acceptance test for this mechanism
// being generic rather than import-specific.
//
// Every other dedup test here drives an import message, so all of them would still
// pass if the machinery had grown an import-shaped assumption somewhere. This one
// takes a broadcast type that knows nothing about idempotency, has no proto field
// for a key, and whose coordinator never reads one, and shows that carrying the key
// is the ONLY thing required: it deduplicates, reports the original broadcast, and
// replays the original's per-vchannel results.
func TestNonImportBroadcastIsDeduplicated(t *testing.T) {
	collKey := message.NewSharedCollectionNameResourceKey("db1", "coll1")
	// A tombstoned original with its single vchannel acked, exactly as the import
	// case sets up — only the message type differs.
	bm := newBroadcastTaskManagerForTest(t,
		createBroadcastTaskProtoFromMessage(
			newCreateCollectionMsgWithKey("run-1"), 100,
			streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE, []byte{0x01}, collKey))

	result := broadcastForTest(t, bm, 999, newCreateCollectionMsgWithKey("run-1"), collKey)

	require.NotNil(t, result.Duplicated, "a keyed non-import broadcast must deduplicate too")
	require.Equal(t, uint64(100), result.BroadcastID)
	require.Equal(t, message.MessageTypeCreateCollection, result.Duplicated.MessageType())

	// The replayed results are what make a duplicate indistinguishable from a fresh
	// broadcast for a caller that only reads append results — which is most of the
	// broadcast call sites in the repo.
	require.Len(t, result.AppendResults, 1)
	appendResult := result.AppendResults["v1"]
	require.NotNil(t, appendResult)
	require.Equal(t, uint64(1), appendResult.TimeTick)
}

// TestBroadcasterChecksKeyLengthAfterTheLookup pins the order of the length check
// against the index lookup inside broadcasterWithRK: the limit is admission control
// over NEW keys, so an already-indexed key resolves without being re-measured.
//
// This is a statement about the broadcaster only. It is NOT the cluster-level claim
// that lowering streaming.idempotency.maxKeyLength leaves an open idempotency window
// intact: the REST middleware and the propagation interceptor apply the same
// refreshable bound at the edge, so a real client's in-window retry is rejected there
// before it ever reaches this code. See the comment at broadcasterWithRK.Broadcast.
func TestBroadcasterChecksKeyLengthAfterTheLookup(t *testing.T) {
	bm := newBroadcastTaskManagerForTest(t)
	collKey := message.NewExclusiveCollectionNameResourceKey("db1", "coll1")

	old := paramtable.Get().StreamingCfg.IdempotencyMaxKeyLength.SwapTempValue("8")
	defer paramtable.Get().StreamingCfg.IdempotencyMaxKeyLength.SwapTempValue(old)

	first := broadcastForTest(t, bm, 1, newImportMsgWithKey("12345678"), collKey)
	require.Nil(t, first.Duplicated)

	// The operator lowers the bound below a key that is already indexed. A request that
	// got this far has already cleared the edge check, which is what makes this reachable.
	paramtable.Get().StreamingCfg.IdempotencyMaxKeyLength.SwapTempValue("4")

	retry := broadcastForTest(t, bm, 2, newImportMsgWithKey("12345678"), collKey)
	require.NotNil(t, retry.Duplicated, "an already-indexed key must still resolve")
	require.Equal(t, uint64(1), retry.BroadcastID)

	// A key that is not in the index is still held to the current bound.
	api := &broadcasterWithRK{broadcaster: bm, broadcastID: 3, guards: bm.resourceKeyLocker.Lock(collKey)}
	defer api.Close()
	_, err := api.Broadcast(context.Background(), newImportMsgWithKey("87654321"))
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
}

// newBroadcastTaskManagerWithEntrypointForTest is newBroadcastTaskManagerForTest plus
// the ID allocator broadcastTaskManager.WithResourceKeys needs, so a test can drive the
// real entrypoint instead of constructing broadcasterWithRK by hand.
//
// The cluster-role check is the one step of WithResourceKeys that is stubbed. It reads a
// process-wide balancer singleton that can only be Set once and has no reset, and
// TestBroadcaster in this package already claims it; registering a second one panics.
// The lock acquisition and the ID allocation -- the work that actually sits between
// taking the resource lock and reaching the idempotency lookup -- are the real ones.
func newBroadcastTaskManagerWithEntrypointForTest(t *testing.T, protos ...*streamingpb.BroadcastTask) *broadcastTaskManager {
	paramtable.Init()
	registry.ResetRegistration()
	registry.RegisterImportV1AckCallback(func(ctx context.Context, result message.BroadcastResultImportMessageV1) error {
		return nil
	})

	roleCheck := mockey.Mock((*broadcastTaskManager).checkClusterRole).Return(nil).Build()
	t.Cleanup(func() { roleCheck.UnPatch() })

	meta := mock_metastore.NewMockStreamingCoordCataLog(t)
	meta.EXPECT().SaveBroadcastTask(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	rc := idalloc.NewMockRootCoordClient(t)
	f := syncutil.NewFuture[internaltypes.MixCoordClient]()
	f.Set(rc)
	resource.InitForTest(resource.OptStreamingCatalog(meta), resource.OptMixCoordClient(f))

	operator := mock_streaming.NewMockWALAccesser(t)
	operator.EXPECT().AppendMessages(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, msgs ...message.MutableMessage) types.AppendResponses {
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
		}).Maybe()
	streaming.SetWALForTest(operator)

	bm := newBroadcastTaskManager(protos)
	t.Cleanup(bm.Close)
	return bm
}

// TestConcurrentSameKeyBroadcastsCreateExactlyOneTask is the invariant import's dedup
// correctness rests on, driven through the REAL entrypoint.
//
// Every other test in this file builds broadcasterWithRK directly, which means it never
// exercises what makes concurrency safe: the lookup lives behind resource keys that
// WithResourceKeys acquires, and WithResourceKeys does non-trivial work between taking
// the lock and reaching the lookup (allocating a broadcastID, checking the cluster
// role). If the second request were not serialized behind the first's ack, it would
// miss the index and create a second import job -- exactly the duplication this feature
// exists to prevent, and invisible to every direct-construction test.
func TestConcurrentSameKeyBroadcastsCreateExactlyOneTask(t *testing.T) {
	bm := newBroadcastTaskManagerWithEntrypointForTest(t)

	// Exclusive, and on the collection the key is scoped to: that pairing is what the
	// serialization guarantee is stated in terms of.
	collKey := message.NewExclusiveCollectionNameResourceKey("db1", "coll1")

	const concurrency = 8
	results := make([]*types.BroadcastAppendResult, concurrency)
	errs := make([]error, concurrency)

	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			api, err := bm.WithResourceKeys(context.Background(), collKey)
			if err != nil {
				errs[i] = err
				return
			}
			defer api.Close()
			results[i], errs[i] = api.Broadcast(context.Background(), newImportMsgWithKey("k"))
		}(i)
	}
	close(start)

	finished := make(chan struct{})
	go func() { wg.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(30 * time.Second):
		t.Fatal("concurrent same-key broadcasts did not all finish: a guard was leaked or an ack never completed")
	}

	fresh := 0
	var originalID uint64
	for i := 0; i < concurrency; i++ {
		require.NoError(t, errs[i])
		require.NotNil(t, results[i])
		if results[i].Duplicated == nil {
			fresh++
			originalID = results[i].BroadcastID
		}
	}
	require.Equal(t, 1, fresh, "exactly one of the concurrent same-key broadcasts may create a task")

	// Every request, the winner included, answers with the same broadcastID: a client
	// that retried while its original was still in flight gets the original's job.
	for i := 0; i < concurrency; i++ {
		require.Equal(t, originalID, results[i].BroadcastID)
	}
	// And the index holds exactly that one entry for the scope.
	require.Len(t, bm.idempotencyIndex.scopeToBroadcastID, 1)
}
