package broadcaster

import (
	"context"
	"strconv"
	"strings"
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
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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

// newImportMsgWithKey builds an import broadcast message carrying the given client
// idempotency key ("" means the message carries no key at all), as it looks when it
// reaches the broadcaster: not yet stamped with a broadcastID or resource keys.
func newImportMsgWithKey(key string) message.BroadcastMutableMessage {
	return message.NewImportMessageBuilderV1().
		WithHeader(&message.ImportMessageHeader{}).
		WithBody(&msgpb.ImportMsg{}).
		WithIdempotencyKey(key).
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
		WithIdempotencyKey(key).
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
			[]message.ResourceKey{message.NewSharedClusterResourceKey()}, clientKey)
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

// TestIdempotencyScopeEncoding pins what makes two broadcasts the same operation.
// A client key alone is not an identity: it only deduplicates within
// (messageType, sorted unique resource keys, clientKey).
func TestIdempotencyScopeEncoding(t *testing.T) {
	cluster := message.NewSharedClusterResourceKey()
	collA := message.NewSharedCollectionNameResourceKey("db1", "coll1")
	collB := message.NewSharedCollectionNameResourceKey("db1", "coll2")

	base := idempotencyScope(message.MessageTypeImport, []message.ResourceKey{cluster, collA}, "k")
	require.NotEmpty(t, base)

	// A broadcast without a client key is not idempotent and has no scope at all.
	require.Empty(t, idempotencyScope(message.MessageTypeImport, []message.ResourceKey{cluster, collA}, ""))

	// Same identity, same scope.
	require.Equal(t, base, idempotencyScope(message.MessageTypeImport, []message.ResourceKey{cluster, collA}, "k"))
	// The resource keys are a SET: order and duplicates must not change the scope.
	require.Equal(t, base, idempotencyScope(message.MessageTypeImport, []message.ResourceKey{collA, cluster}, "k"))
	require.Equal(t, base, idempotencyScope(message.MessageTypeImport, []message.ResourceKey{collA, cluster, collA}, "k"))

	// The message type is part of the identity: CreateIndex and DropIndex on one
	// collection carry identical resource keys, so without it the same client key on
	// two different operations would collide and the second would be swallowed.
	require.NotEqual(t, base, idempotencyScope(message.MessageTypeCreateCollection, []message.ResourceKey{cluster, collA}, "k"))
	// So are the resource keys: the same key against another collection is another operation.
	require.NotEqual(t, base, idempotencyScope(message.MessageTypeImport, []message.ResourceKey{cluster, collB}, "k"))
	// A missing resource key is a different scope too, not a prefix of this one.
	require.NotEqual(t, base, idempotencyScope(message.MessageTypeImport, []message.ResourceKey{cluster}, "k"))
	// And so is the client key itself.
	require.NotEqual(t, base, idempotencyScope(message.MessageTypeImport, []message.ResourceKey{cluster, collA}, "k2"))
}

// TestIdempotencyScopeResistsCraftedClientKey proves the encoding is unforgeable.
// The client key is attacker-controlled, so it must not be able to impersonate a
// different scope by embedding the encoding's own separators. The crafted key below
// reproduces, character for character, the fields a second resource key contributes:
// under a plain separator-joined encoding it would collide with the legitimate scope
// of an operation holding that extra key.
func TestIdempotencyScopeResistsCraftedClientKey(t *testing.T) {
	collA := message.NewSharedCollectionNameResourceKey("db1", "coll1")
	collB := message.NewSharedCollectionNameResourceKey("db1", "coll2")
	require.Less(t, collA.Key, collB.Key, "the crafted key below assumes collA sorts before collB")

	crafted := strconv.Itoa(int(collB.Domain)) + ":" + collB.Key + ":v"
	craftedScope := idempotencyScope(message.MessageTypeImport, []message.ResourceKey{collA}, crafted)
	legitScope := idempotencyScope(message.MessageTypeImport, []message.ResourceKey{collA, collB}, "v")

	require.NotEqual(t, legitScope, craftedScope)
}

// TestIdempotencyScopeOfMessageIsDeterministic guards the sort on the path that reads
// the resource keys back out of a message header. They live in a Set there, so Collect()
// hands them back in an arbitrary order on every call; without the sort the recovery
// rebuild and the broadcast path would compute different scopes for one message and
// dedup would silently never hit.
func TestIdempotencyScopeOfMessageIsDeterministic(t *testing.T) {
	msg := newImportMsgWithKey("k").OverwriteBroadcastHeader(100,
		message.NewSharedClusterResourceKey(),
		message.NewSharedDBNameResourceKey("db1"),
		message.NewSharedCollectionNameResourceKey("db1", "coll1"),
		message.NewSharedCollectionNameResourceKey("db1", "coll2"))

	first := idempotencyScopeOfMessage(msg)
	for i := 0; i < 100; i++ {
		require.Equal(t, first, idempotencyScopeOfMessage(msg))
	}
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

	// Different resource keys (another collection), everything else identical.
	otherKeys := broadcastForTest(t, bm, 4, newImportMsgWithKey("k"), dbKey, collB)
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

	dupOtherKeys := broadcastForTest(t, bm, 7, newImportMsgWithKey("k"), dbKey, collB)
	require.NotNil(t, dupOtherKeys.Duplicated)
	require.Equal(t, uint64(4), dupOtherKeys.BroadcastID)
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
	require.Equal(t, atLimit, message.IdempotencyKeyOf(dup.Duplicated))

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

// TestAdmissionRunsOnlyWhenANewTaskIsCreated pins the ordering the import job-count
// limit depends on: admission is what a caller uses to guard mutable cluster state, and
// a deduplicated broadcast must reach the original task without passing it. Were admit
// to run on a hit, a limit the original request is itself still counted against would
// reject its own retry.
func TestAdmissionRunsOnlyWhenANewTaskIsCreated(t *testing.T) {
	bm := newBroadcastTaskManagerForTest(t)
	collKey := message.NewExclusiveCollectionNameResourceKey("db1", "coll1")

	admitted := 0
	broadcast := func(broadcastID uint64, msg message.BroadcastMutableMessage) *types.BroadcastAppendResult {
		api := &broadcasterWithRK{broadcaster: bm, broadcastID: broadcastID, guards: bm.resourceKeyLocker.Lock(collKey)}
		defer api.Close()
		result, err := api.BroadcastWithAdmission(context.Background(), msg, func(context.Context) error {
			admitted++
			return nil
		})
		require.NoError(t, err)
		return result
	}

	first := broadcast(1, newImportMsgWithKey("run-1"))
	require.Nil(t, first.Duplicated)
	require.Equal(t, 1, admitted)

	second := broadcast(2, newImportMsgWithKey("run-1"))
	require.NotNil(t, second.Duplicated, "the retry must resolve to the original broadcast")
	require.Equal(t, uint64(1), second.BroadcastID)
	require.Equal(t, 1, admitted, "admission must not run for a deduplicated broadcast")
}

// TestAdmissionFailureLeavesNoTask asserts a rejected admission is a clean rejection:
// no task, no index entry, so the same key is still free once the condition clears.
func TestAdmissionFailureLeavesNoTask(t *testing.T) {
	bm := newBroadcastTaskManagerForTest(t)
	collKey := message.NewExclusiveCollectionNameResourceKey("db1", "coll1")

	api := &broadcasterWithRK{broadcaster: bm, broadcastID: 1, guards: bm.resourceKeyLocker.Lock(collKey)}
	_, err := api.BroadcastWithAdmission(context.Background(), newImportMsgWithKey("run-1"), func(context.Context) error {
		return merr.WrapErrServiceInternal("at the limit")
	})
	require.Error(t, err)
	api.Close()

	retry := broadcastForTest(t, bm, 2, newImportMsgWithKey("run-1"), collKey)
	require.Nil(t, retry.Duplicated, "the rejected attempt must not have claimed the key")
	require.Equal(t, uint64(2), retry.BroadcastID)
}

// TestLoweringTheKeyLengthLimitDoesNotEndTheWindow guards the order of the length check
// against the index lookup. streaming.idempotency.maxKeyLength is refreshable, so an
// operator can lower it while keys accepted under the old value are still inside their
// idempotency window. The limit is admission control over NEW keys; enforcing it on a
// lookup would reject those retries before they could recover the original broadcastID,
// and a client that then re-sends under a fresh key imports its data twice.
func TestLoweringTheKeyLengthLimitDoesNotEndTheWindow(t *testing.T) {
	bm := newBroadcastTaskManagerForTest(t)
	collKey := message.NewExclusiveCollectionNameResourceKey("db1", "coll1")

	old := paramtable.Get().StreamingCfg.IdempotencyMaxKeyLength.SwapTempValue("8")
	defer paramtable.Get().StreamingCfg.IdempotencyMaxKeyLength.SwapTempValue(old)

	first := broadcastForTest(t, bm, 1, newImportMsgWithKey("12345678"), collKey)
	require.Nil(t, first.Duplicated)

	// The operator lowers the bound below a key that is already indexed.
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
