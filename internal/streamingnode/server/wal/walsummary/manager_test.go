// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package walsummary

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/idempotencyview"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

type recordingScheduler struct {
	tasks []nodescheduler.Task
}

func (s *recordingScheduler) Submit(task nodescheduler.Task) nodescheduler.TaskHandle {
	s.tasks = append(s.tasks, task)
	return recordingTaskHandle{}
}

type recordingTaskHandle struct{}

func (recordingTaskHandle) Cancel() {}

func (recordingTaskHandle) Wait(context.Context) error { return nil }

func newTestManager(t *testing.T, store *Store, retentionMaxBytes uint64) *Manager {
	t.Helper()
	return NewManager(ManagerConfig{
		PChannel:          store.PChannel(),
		Term:              store.Term(),
		Store:             store,
		RetentionMaxBytes: retentionMaxBytes,
	})
}

func newTestManagerWithStore(t *testing.T) (*Manager, *Store) {
	t.Helper()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	store := NewStore(cm, "by-dev-rootcoord-dml_0_40451v0", 1)
	return newTestManager(t, store, 1<<30), store
}

// newTestDeleteMessage builds a delete message of the given vchannel.
func newTestDeleteMessage(t *testing.T, vchannel string, timetick uint64, partitionID int64, pks ...int64) message.ImmutableMessage {
	t.Helper()
	mutableMsg := message.NewDeleteMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&message.DeleteMessageHeader{
			CollectionId: 1,
			Rows:         1,
		}).
		WithBody(&msgpb.DeleteRequest{
			Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_Delete},
			CollectionID: 1,
			PartitionID:  partitionID,
			PrimaryKeys:  &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: pks}}},
			Timestamps:   []uint64{timetick},
		}).
		MustBuildMutable()
	return mutableMsg.WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick + 1)))
}

// observeDelete observes one delete message through the pchannel-level entry
// point and releases the owner, setting *finalized when the message is fully
// released. The summary never retains the message, so *finalized is true as
// soon as the observation returns.
// observeKeyedInsert observes one insert carrying a client key, which is what
// produces a record now that the summary has a single consumer.
func observeKeyedInsert(t *testing.T, manager *Manager, vchannel string, timetick uint64, finalized *bool) {
	t.Helper()
	msg := newTestIdempotentInsertMessage(t, vchannel, timetick,
		fmt.Sprintf("key-%d", timetick), []int64{int64(timetick)}, []uint32{0})
	manager.ObserveMessage(context.Background(), msg)
	*finalized = true
}

// persist runs the one and only write trigger: what the recovery storage calls
// from its dirty persist, before saving the checkpoint.
func persist(t *testing.T, manager *Manager) {
	t.Helper()
	require.NoError(t, manager.Persist(context.Background()))
}

// flushObserved observes one keyed insert and persists it, which is the whole
// cycle: a record staged by observation becomes durable in the next persist.
func flushObserved(t *testing.T, manager *Manager, vchannel string, timetick uint64, finalized *bool) {
	t.Helper()
	observeKeyedInsert(t, manager, vchannel, timetick, finalized)
	persist(t, manager)
}

// TestObserveMessageCopiesRecordWithoutRetaining verifies the summary never
// retains the WAL message: the record is built and copied at observation, the
// message is released immediately, and a later flush persists the copied
// record.
func TestObserveMessageCopiesRecordWithoutRetaining(t *testing.T) {
	manager, _ := newTestManagerWithStore(t)
	ctx := context.Background()

	finalized := false
	observeKeyedInsert(t, manager, "v1", 100, &finalized)
	assert.True(t, finalized, "the message must be released at observation — the summary holds no reference")
	manager.mu.Lock()
	require.Len(t, manager.pending, 1)
	assert.Equal(t, uint64(100), manager.pending[0].timeTick)
	assert.NotNil(t, manager.pending[0].insert, "the record is built at observation")
	manager.mu.Unlock()

	persist(t, manager)
	assert.Equal(t, uint64(100), manager.LatestCoveredTimeTick())

	decoded, footer, err := manager.cfg.Store.ReadChunk(ctx, 0, 1)
	require.NoError(t, err)
	require.Equal(t, uint64(0), footer.GetGeneration())
	require.Len(t, decoded, 1, "chunk must carry only vchannels that had records")
	require.Len(t, decoded["v1"].Inserts, 1)
	assert.Equal(t, uint64(100), decoded["v1"].Inserts[0].GetSourceTimetick())
	require.Len(t, decoded["v1"].Idempotency, 1)
	assert.Equal(t, "key-100", decoded["v1"].Idempotency[0].GetKey())
}

func TestObserveMessageIgnoresNonVChannelRecords(t *testing.T) {
	manager, _ := newTestManagerWithStore(t)
	ctx := context.Background()

	// A control-channel message must not be retained.
	manager.ObserveMessage(ctx, newTestDeleteMessage(t, "by-dev-rootcoord-dml_0vcchan", 100, 10, int64(100)))
	manager.mu.Lock()
	assert.Empty(t, manager.pending, "control channel must not be retained")
	manager.mu.Unlock()

	// A vchannel-less (pchannel-level) message must not be retained either.
	manager.ObserveMessage(ctx, newTestDeleteMessage(t, "", 100, 10, int64(100)))
	manager.mu.Lock()
	assert.Empty(t, manager.pending)
	manager.mu.Unlock()

	// Nothing was staged, so a persist writes no chunk at all.
	persist(t, manager)
	_, _, err := manager.cfg.Store.ReadChunk(ctx, 0, 1)
	assert.Error(t, err, "no chunk must be written when nothing was staged")
}

func TestObserveMessageSkipsRecordsAtOrBelowDurableFrontier(t *testing.T) {
	ctx := context.Background()
	manager1, _ := newTestManagerWithStore(t)

	// Seed a durable frontier: a chunk covering v1 up to 150.
	var unused bool
	flushObserved(t, manager1, "v1", 150, &unused)

	manager2 := newTestManager(t, manager1.cfg.Store, 1<<30)
	require.NoError(t, manager2.Restore(ctx))
	assert.Equal(t, uint64(150), manager2.durableFrontiers["v1"])

	// Records at or below the restored durable frontier are skipped entirely:
	// recovery replay re-observes them, and staging them again would rewrite
	// the same records into a new chunk.
	finalizedA := false
	observeKeyedInsert(t, manager2, "v1", 100, &finalizedA)
	assert.True(t, finalizedA, "a skipped record is never staged")
	manager2.mu.Lock()
	assert.Empty(t, manager2.pending)
	manager2.mu.Unlock()

	// A record past the frontier is staged and persisted as usual.
	finalizedB := false
	observeKeyedInsert(t, manager2, "v1", 200, &finalizedB)
	assert.True(t, finalizedB, "the message is released at observation")
	persist(t, manager2)
	sections, err := manager2.ReadIdempotencyEntries(ctx, "v1", 150, 1000)
	require.NoError(t, err)
	require.Len(t, sections.Inserts, 1)
	assert.Equal(t, uint64(200), sections.Inserts[0].GetSourceTimetick())
}

func TestManagerRestore(t *testing.T) {
	manager, _ := newTestManagerWithStore(t)
	ctx := context.Background()

	// Two flushes produce two chunks.
	var unused bool
	flushObserved(t, manager, "v1", 100, &unused)
	flushObserved(t, manager, "v1", 200, &unused)

	// A new manager over the same store recovers both chunks and continues
	// generations after them.
	recovered := newTestManager(t, manager.cfg.Store, 1<<30)
	require.NoError(t, recovered.Restore(ctx))
	assert.Equal(t, uint64(2), recovered.nextGeneration)
	assert.Equal(t, uint64(200), recovered.LatestCoveredTimeTick())
	assert.Len(t, recovered.Manifest().GetChunks(), 2)
}

func TestManagerRestoreProbesOrphanChunk(t *testing.T) {
	manager, _ := newTestManagerWithStore(t)
	ctx := context.Background()

	var unused bool
	flushObserved(t, manager, "v1", 100, &unused)

	// Simulate a crash between chunk write and manifest publish: write a chunk
	// directly without recording it.
	orphan := buildIdempotencySections(idempotencyWrite{timeTick: 300, key: "orphan", pk: 300})
	_, _, err := manager.cfg.Store.WriteChunk(ctx, 2, map[string]*ChunkSections{"v1": orphan})
	require.NoError(t, err)

	recovered := newTestManager(t, manager.cfg.Store, 1<<30)
	require.NoError(t, recovered.Restore(ctx))
	assert.Equal(t, uint64(3), recovered.nextGeneration)
	assert.Equal(t, uint64(300), recovered.LatestCoveredTimeTick())
	require.Len(t, recovered.Manifest().GetChunks(), 2)
	// The probed tail is sealed into a published manifest: a third recovery
	// sees it without probing again.
	again := newTestManager(t, manager.cfg.Store, 1<<30)
	require.NoError(t, again.Restore(ctx))
	assert.Equal(t, uint64(3), again.nextGeneration)
}

// TestManagerGCReleasesOldestFirstUnderBudget covers the whole retention rule:
// release is decided by bytes alone, oldest chunk first, and stops as soon as
// the retained set is back under the budget. No consumer reports a position it
// still needs, so nothing else can hold a chunk back.
func TestManagerGCReleasesOldestFirstUnderBudget(t *testing.T) {
	manager, _ := newTestManagerWithStore(t)
	ctx := context.Background()

	var unused bool
	flushObserved(t, manager, "v1", 100, &unused)
	flushObserved(t, manager, "v1", 200, &unused)
	flushObserved(t, manager, "v1", 300, &unused)
	require.Len(t, manager.Manifest().GetChunks(), 3)

	// Under the budget nothing is released, however old the chunks are.
	manager.cfg.RetentionMaxBytes = 1 << 30
	require.NoError(t, manager.GCOnce(ctx))
	assert.Len(t, manager.Manifest().GetChunks(), 3)

	// A budget that only two of the three chunks fit under releases exactly
	// the oldest one: release stops the moment the retained set is under it.
	chunks := manager.Manifest().GetChunks()
	twoChunks := chunks[1].GetObjectSize() + chunks[2].GetObjectSize()
	manager.cfg.RetentionMaxBytes = twoChunks
	require.NoError(t, manager.GCOnce(ctx))
	remaining := manager.Manifest().GetChunks()
	require.Len(t, remaining, 2)
	assert.Equal(t, uint64(1), remaining[0].GetGeneration())
	// The released object is gone, and pending_gc drained with it.
	_, _, err := manager.cfg.Store.ReadChunk(ctx, 0, 1)
	assert.Error(t, err, "chunk object must be deleted after release")
	assert.Empty(t, manager.Manifest().GetPendingGc())

	// A budget below one object releases everything: the bound is soft, and
	// release frees whole objects.
	manager.cfg.RetentionMaxBytes = 1
	require.NoError(t, manager.GCOnce(ctx))
	assert.Empty(t, manager.Manifest().GetChunks())
}

// TestManagerGCDisabledWithoutBudget covers the off switch: a zero budget
// disables release entirely rather than releasing everything.
func TestManagerGCDisabledWithoutBudget(t *testing.T) {
	manager, _ := newTestManagerWithStore(t)
	ctx := context.Background()

	var unused bool
	flushObserved(t, manager, "v1", 100, &unused)
	require.Len(t, manager.Manifest().GetChunks(), 1)

	manager.cfg.RetentionMaxBytes = 0
	require.NoError(t, manager.GCOnce(ctx))
	assert.Len(t, manager.Manifest().GetChunks(), 1)
}

func TestDurableTimeTickDerivedFromManifest(t *testing.T) {
	ctx := context.Background()
	manager, _ := newTestManagerWithStore(t)
	require.NoError(t, manager.Restore(ctx))
	assert.Zero(t, manager.DurableTimeTick("v1"))

	var unused bool
	flushObserved(t, manager, "v1", 100, &unused)
	flushObserved(t, manager, "v1", 200, &unused)
	assert.Equal(t, uint64(200), manager.DurableTimeTick("v1"))

	// A vchannel with no records has no frontier.
	assert.Zero(t, manager.DurableTimeTick("v2"))
}

// messageIDInt converts the test message ID back to its int64 value.
func messageIDInt(id message.MessageID) int64 {
	v, err := strconv.ParseInt(id.Marshal(), 10, 64)
	if err != nil {
		panic(err)
	}
	return v
}

type failInjectingChunkManager struct {
	storage.ChunkManager
	failManifest atomic.Bool
	failChunk    atomic.Bool
}

func (c *failInjectingChunkManager) Write(ctx context.Context, filePath string, content []byte) error {
	if c.failManifest.Load() && strings.Contains(filePath, "/manifest/") {
		return errors.New("injected manifest write failure")
	}
	if c.failChunk.Load() && !strings.Contains(filePath, "/manifest/") {
		return errors.New("injected chunk write failure")
	}
	return c.ChunkManager.Write(ctx, filePath, content)
}

// TestFlushPublishFailureRetriesSameGeneration covers the retry path: the
// chunk is written, the manifest publish fails, and the retry must finish the
// SAME generation — never a second chunk object for the same batch — so a
// reader can never observe the batch twice.
func TestFlushPublishFailureRetriesSameGeneration(t *testing.T) {
	ctx := context.Background()
	cm := &failInjectingChunkManager{
		ChunkManager: storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir())),
	}
	store := NewStore(cm, "by-dev-rootcoord-dml_0_40451v0", 1)
	manager := newTestManager(t, store, 1<<30)
	require.NoError(t, manager.Restore(ctx))
	finalized := false

	cm.failManifest.Store(true)
	observeKeyedInsert(t, manager, "v1", 100, &finalized)
	// Chunk write succeeds, manifest publish fails.
	require.Error(t, manager.Persist(ctx))
	// The amendment is not installed; the generation stays claimed and the
	// failed chunk stays at the queue head for the retry. The error propagates
	// to the caller, so the consume checkpoint covering this record is not
	// saved and the WAL still holds it. The chunk object itself is durable.
	assert.Len(t, manager.manifest.GetChunks(), 0)
	assert.Equal(t, uint64(1), manager.nextGeneration)
	manager.mu.Lock()
	assert.NotEmpty(t, manager.pendingSealed, "the failed chunk waits for the retry")
	manager.mu.Unlock()
	keys, _, err := storage.ListAllChunkWithPrefix(ctx, cm, buildChunkPrefix(cm, store.PChannel()), false)
	require.NoError(t, err)
	require.Len(t, keys, 1, "chunk 0 is already durable")

	// The retry succeeds: the same sealed chunk is published with the same
	// generation — exactly one chunk object and one manifest entry.
	cm.failManifest.Store(false)
	require.NoError(t, manager.Persist(ctx))
	require.Len(t, manager.manifest.GetChunks(), 1)
	assert.Equal(t, uint64(0), manager.manifest.GetChunks()[0].GetGeneration())
	keys, _, err = storage.ListAllChunkWithPrefix(ctx, cm, buildChunkPrefix(cm, store.PChannel()), false)
	require.NoError(t, err)
	assert.Len(t, keys, 1, "the retry must not duplicate the chunk object")
}

// TestFlushChunkFailureRetriesSameGeneration covers the chunk-write retry
// under staging growth: the chunk [A] write fails (the object may or may not
// be durable — a dropped ack), and a new record [B] is observed during the
// retry window. The retry must rewrite the SAME generation with the sealed
// [A] batch — the sealed chunk is immutable, so [B] can never join it — and
// the newer span seals into a fresh generation. One object per batch, never a
// conflicting rewrite of generation 0.
func TestFlushChunkFailureRetriesSameGeneration(t *testing.T) {
	ctx := context.Background()
	cm := &failInjectingChunkManager{
		ChunkManager: storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir())),
	}
	store := NewStore(cm, "by-dev-rootcoord-dml_0_40451v0", 1)
	manager := newTestManager(t, store, 1<<30)
	require.NoError(t, manager.Restore(ctx))
	finalizedA := false
	finalizedB := false

	// First attempt: the chunk write fails; the sealed chunk stays at the
	// queue head for the retry.
	cm.failChunk.Store(true)
	observeKeyedInsert(t, manager, "v1", 100, &finalizedA)
	require.Error(t, manager.Persist(ctx))
	keys, _, err := storage.ListAllChunkWithPrefix(ctx, cm, buildChunkPrefix(cm, store.PChannel()), false)
	require.NoError(t, err)
	assert.Empty(t, keys, "the failed chunk write left no durable object")

	// A new record [B] is observed during the retry window; the next persist
	// seals it into a fresh generation.
	observeKeyedInsert(t, manager, "v1", 200, &finalizedB)

	// The retry drains the whole queue: [A] under generation 0, [B] under
	// generation 1 — one object per batch, never a conflicting rewrite.
	cm.failChunk.Store(false)
	require.NoError(t, manager.Persist(ctx))
	require.Len(t, manager.manifest.GetChunks(), 2)
	assert.Equal(t, uint64(0), manager.manifest.GetChunks()[0].GetGeneration())
	assert.Equal(t, uint64(1), manager.manifest.GetChunks()[1].GetGeneration())
	keys, _, err = storage.ListAllChunkWithPrefix(ctx, cm, buildChunkPrefix(cm, store.PChannel()), false)
	require.NoError(t, err)
	assert.Len(t, keys, 2, "one object per generation; the failed generation-0 rewrite never duplicated")
}

// TestPersistSurfacesStoreCorruption covers what a terminal store error does
// now that the write is ordered against the checkpoint: it fails the persist.
//
// There is no abandonment and no frontier to pin. The error reaches the
// recovery storage, which returns before saving the consume checkpoint, so the
// records stay replayable from the WAL and the next tick tries again. A store
// that is genuinely corrupt therefore stalls the checkpoint rather than
// silently dropping records -- the remedy is the documented one (disable
// idempotency, which drops the store, then re-enable).
func TestPersistSurfacesStoreCorruption(t *testing.T) {
	ctx := context.Background()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	store := NewStore(cm, "by-dev-rootcoord-dml_0_40451v0", 1)
	manager := newTestManager(t, store, 1<<30)
	require.NoError(t, manager.Restore(ctx))
	finalized := false

	// A conflicting object already occupies generation 0 under this term:
	// WriteChunk detects the same-generation different-payload as corruption.
	require.NoError(t, cm.Write(ctx, store.ChunkKey(0), []byte("conflicting payload")))
	observeKeyedInsert(t, manager, "v1", 100, &finalized)

	err := manager.Persist(ctx)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrStoreCorrupted), "the conflict surfaces as store corruption")
	assert.Empty(t, manager.Manifest().GetChunks(), "nothing was recorded")
}

// TestGCOnceRemovesAllPending covers the snapshot iteration of the pending GC
// queue: removePendingGC compacts the live slice in place, and a naive range
// over the live slice would skip entries once the indexes shift.
func TestGCOnceRemovesAllPending(t *testing.T) {
	ctx := context.Background()
	manager, store := newTestManagerWithStore(t)
	require.NoError(t, manager.Restore(ctx))

	// Seed three chunks and three pending refs, and write the objects.
	manager.mu.Lock()
	for gen := uint64(0); gen < 3; gen++ {
		manager.manifest.Chunks = append(manager.manifest.Chunks, &streamingpb.PChannelSummaryChunkIndexEntry{
			Generation:    gen,
			Term:          1,
			StartTimetick: gen * 100,
			EndTimetick:   gen*100 + 50,
		})
		manager.manifest.PendingGc = append(manager.manifest.PendingGc, &streamingpb.PChannelSummaryChunkRef{
			Generation: gen,
			Term:       1,
		})
	}
	manager.mu.Unlock()
	for gen := uint64(0); gen < 3; gen++ {
		_, _, err := store.WriteChunk(ctx, gen, nil)
		require.NoError(t, err)
	}

	require.NoError(t, manager.GCOnce(ctx))
	assert.Empty(t, manager.manifest.GetPendingGc())
	for gen := uint64(0); gen < 3; gen++ {
		_, _, err := store.ReadChunk(ctx, gen, 1)
		require.Error(t, err, "chunk %d must be deleted", gen)
	}
}

// TestConcurrentFlushAndGCRelease exercises the manifest publish paths
// concurrently. Run with -race: a torn publish (a marshal racing an in-place
// edit of the shared manifest) would surface here as a data race, and the
// publish mutex is what keeps the persist and GC publishes from interleaving.
func TestConcurrentFlushAndGCRelease(t *testing.T) {
	ctx := context.Background()
	manager, _ := newTestManagerWithStore(t)
	manager.cfg.RetentionMaxBytes = 1 // every chunk is over the budget
	require.NoError(t, manager.Restore(ctx))

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			var unused bool
			observeKeyedInsert(t, manager, "v1", uint64(100+i), &unused)
			persist(t, manager)
		}
		close(stop)
	}()
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				if err := manager.GCOnce(ctx); err != nil {
					t.Errorf("GCOnce: %v", err)
					return
				}
			}
		}
	}()
	wg.Wait()
}

// newTestDropCollectionMessage builds a DropCollection message of the given
// vchannel: it invalidates the vchannel's idempotency window.
func newTestDropCollectionMessage(t *testing.T, vchannel string, timetick uint64) message.ImmutableMessage {
	t.Helper()
	mutableMsg := message.NewDropCollectionMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&message.DropCollectionMessageHeader{CollectionId: 1}).
		WithBody(&msgpb.DropCollectionRequest{}).
		MustBuildMutable()
	return mutableMsg.WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick + 1)))
}

// Retention has to bound the chunk COUNT as well as the bytes: a trickle of
// keyed writes seals a small chunk per staging interval, so the byte budget can
// stay orders of magnitude away from its bound while the manifest grows without
// limit -- and every publish rewrites the whole manifest while recovery reads
// one object per chunk.
func TestManagerGCReleasesOverChunkCount(t *testing.T) {
	manager, _ := newTestManagerWithStore(t)
	ctx := context.Background()
	require.NoError(t, manager.Restore(ctx))

	var unused bool
	for i := 0; i < 5; i++ {
		flushObserved(t, manager, "v1", uint64(100+i*100), &unused)
	}
	require.Len(t, manager.Manifest().GetChunks(), 5)

	// A byte budget nothing approaches: bytes alone would never release.
	manager.cfg.RetentionMaxBytes = 1 << 30
	require.NoError(t, manager.GCOnce(ctx))
	require.Len(t, manager.Manifest().GetChunks(), 5, "the byte bound alone releases nothing here")

	// The count bound does, oldest first.
	manager.cfg.MaxRetainedChunks = 2
	require.NoError(t, manager.GCOnce(ctx))
	chunks := manager.Manifest().GetChunks()
	require.Len(t, chunks, 2)
	assert.Equal(t, uint64(3), chunks[0].GetGeneration(), "release is oldest-first")

	// Both bounds zero disables release entirely.
	manager.cfg.RetentionMaxBytes = 0
	manager.cfg.MaxRetainedChunks = 0
	require.NoError(t, manager.GCOnce(ctx))
	assert.Len(t, manager.Manifest().GetChunks(), 2)
}

// TestInvalidationBuriesDurableRecords covers the durable half of the DDL
// tombstone. The in-memory window is reclaimed by the interceptor, but the
// records already sealed into chunks outlive it, and an auto-derived key is a
// hash of the destination and the payload with no collection generation in it:
// re-inserting the same rows after the collection is gone would hash to the
// same key and be answered as a duplicate, into an empty collection.
//
// So the tombstone has to reach the manifest and be applied on read, whether
// the records are chunked, sealed or still staged.
func TestInvalidationBuriesDurableRecords(t *testing.T) {
	manager, _ := newTestManagerWithStore(t)
	ctx := context.Background()
	require.NoError(t, manager.Restore(ctx))

	var unused bool
	flushObserved(t, manager, "v1", 100, &unused)
	flushObserved(t, manager, "v2", 110, &unused)
	require.Len(t, manager.Manifest().GetChunks(), 2)

	sections, err := manager.ReadIdempotencyEntries(ctx, "v1", 0, 1000)
	require.NoError(t, err)
	require.Len(t, sections.Inserts, 1, "the durable record is readable before the DDL")

	// A record staged but not yet sealed is buried too.
	observeKeyedInsert(t, manager, "v1", 200, &unused)
	manager.mu.Lock()
	require.NotEmpty(t, manager.pending)
	manager.mu.Unlock()

	manager.ObserveMessage(ctx, newTestDropCollectionMessage(t, "v1", 300))
	persist(t, manager)

	sections, err = manager.ReadIdempotencyEntries(ctx, "v1", 0, 1000)
	require.NoError(t, err)
	assert.Empty(t, sections.Inserts, "every record at or below the tombstone is unserveable")
	manager.mu.Lock()
	assert.Empty(t, manager.pending, "the staged span behind the tombstone is forgotten")
	manager.mu.Unlock()

	// Another vchannel of the same pchannel is untouched.
	sections, err = manager.ReadIdempotencyEntries(ctx, "v2", 0, 1000)
	require.NoError(t, err)
	assert.Len(t, sections.Inserts, 1, "the tombstone is per vchannel")

	// The tombstone is durable: it reached the manifest, so a restart applies
	// it instead of resurrecting the keys.
	assert.Equal(t, uint64(300), manager.Manifest().GetInvalidatedVchannels()["v1"])
	recovered := newTestManager(t, manager.cfg.Store, 1<<30)
	require.NoError(t, recovered.Restore(ctx))
	sections, err = recovered.ReadIdempotencyEntries(ctx, "v1", 0, 1000)
	require.NoError(t, err)
	assert.Empty(t, sections.Inserts, "a restart must not resurrect the buried keys")

	// A write after the DDL is served again: the tombstone buries the past,
	// not the vchannel.
	observeKeyedInsert(t, manager, "v1", 400, &unused)
	sections, err = manager.ReadIdempotencyEntries(ctx, "v1", 0, 1000)
	require.NoError(t, err)
	assert.Len(t, sections.Inserts, 1, "writes past the tombstone are served")
}

// TestInvalidationSurvivesUnwrittenSealedChunk covers the one case where the
// manifest cannot answer whether a tombstone still has work to do: a chunk is
// sealed but not yet written, so its records are below the tombstone and
// invisible to the manifest. Expiring the entry there would let that chunk land
// unfiltered and resurrect exactly the keys the DDL buried.
func TestInvalidationSurvivesUnwrittenSealedChunk(t *testing.T) {
	manager, _ := newTestManagerWithStore(t)
	ctx := context.Background()
	require.NoError(t, manager.Restore(ctx))

	// Stage a record and seal it without letting the write run.
	var unused bool
	observeKeyedInsert(t, manager, "v1", 100, &unused)
	manager.seal()
	manager.mu.Lock()
	require.NotEmpty(t, manager.pendingSealed, "the chunk must still be waiting to be written")
	manager.mu.Unlock()

	// The DDL lands while that chunk is still unwritten. The persist publishes
	// the tombstone in the same cycle that writes the chunk, and at publish
	// time the chunk is not in the manifest yet -- so the manifest cannot see
	// the records the tombstone covers.
	manager.ObserveMessage(ctx, newTestDropCollectionMessage(t, "v1", 300))
	persist(t, manager)
	assert.Equal(t, uint64(300), manager.Manifest().GetInvalidatedVchannels()["v1"],
		"the tombstone must not expire while a sealed chunk can still hold records below it")

	// The records the late chunk carried are still buried.
	sections, err := manager.ReadIdempotencyEntries(ctx, "v1", 0, 1000)
	require.NoError(t, err)
	assert.Empty(t, sections.Inserts, "the late chunk must not resurrect the buried records")
}

// TestInvalidationDroppedOnceNoChunkReachesBelowIt covers the tombstone's own
// lifetime: it describes records, so once retention has released every chunk
// that could hold them it describes nothing and must not accumulate for the
// life of the pchannel.
func TestInvalidationDroppedOnceNoChunkReachesBelowIt(t *testing.T) {
	manager, _ := newTestManagerWithStore(t)
	ctx := context.Background()
	require.NoError(t, manager.Restore(ctx))

	var unused bool
	flushObserved(t, manager, "v1", 100, &unused)
	manager.ObserveMessage(ctx, newTestDropCollectionMessage(t, "v1", 300))
	persist(t, manager)
	require.Equal(t, uint64(300), manager.Manifest().GetInvalidatedVchannels()["v1"])

	// Retention releases the chunk the tombstone was covering.
	manager.cfg.RetentionMaxBytes = 1
	require.NoError(t, manager.GCOnce(ctx))
	require.Empty(t, manager.Manifest().GetChunks())

	// The next publish drops the entry: nothing reaches below it any more.
	flushObserved(t, manager, "v2", 500, &unused)
	assert.NotContains(t, manager.Manifest().GetInvalidatedVchannels(), "v1")
}

// newTestBarrierMessage builds a CreateCollection (barrier-class) message of
// the given vchannel: it produces no record, so observing it only moves the
// confirmation frontier.
func newTestBarrierMessage(t *testing.T, vchannel string, timetick uint64) message.ImmutableMessage {
	t.Helper()
	mutableMsg := message.NewCreateCollectionMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&message.CreateCollectionMessageHeader{CollectionId: 1, PartitionIds: []int64{10}}).
		WithBody(&msgpb.CreateCollectionRequest{}).
		MustBuildMutable()
	return mutableMsg.WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick + 1)))
}

// writeIdempotencyChunk writes one chunk of idempotency records for a vchannel
// and registers it in the manager's manifest, the shape a recovery read sees.
func writeIdempotencyChunk(
	t *testing.T,
	manager *Manager,
	store *Store,
	generation uint64,
	vchannel string,
	sections *ChunkSections,
) {
	t.Helper()
	footer, objectSize, err := store.WriteChunk(context.Background(), generation, map[string]*ChunkSections{
		vchannel: sections,
	})
	require.NoError(t, err)
	manager.mu.Lock()
	recordChunk(manager.manifest, chunkIndexEntryFromFooter(footer, objectSize))
	manager.mu.Unlock()
}

func idempotencyPair(timeTick uint64, key string, pk int64) (
	*streamingpb.VChannelSummaryIdempotencyRecord,
	*streamingpb.VChannelSummaryInsertRecord,
) {
	return &streamingpb.VChannelSummaryIdempotencyRecord{Key: key, RowOffsets: []uint32{0}},
		&streamingpb.VChannelSummaryInsertRecord{
			SourceMessageId: &commonpb.MessageID{Id: fmt.Sprintf("m%d", timeTick)},
			SourceTimetick:  timeTick,
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{pk}}},
			},
		}
}

// idempotencyWrite is one write's fixture: when it landed, the client key it
// was made with (empty for a write no view remembers), and the primary key it
// produced.
type idempotencyWrite struct {
	timeTick uint64
	key      string
	pk       int64
}

func buildIdempotencySections(writes ...idempotencyWrite) *ChunkSections {
	sections := &ChunkSections{}
	for _, w := range writes {
		key, insert := idempotencyPair(w.timeTick, w.key, w.pk)
		sections.Idempotency = append(sections.Idempotency, key)
		sections.Inserts = append(sections.Inserts, insert)
	}
	return sections
}

func TestReadIdempotencyEntriesFiltersAndSpansChunks(t *testing.T) {
	ctx := context.Background()
	manager, store := newTestManagerWithStore(t)
	vchannel := "by-dev-rootcoord-dml_0_40451v0"

	writeIdempotencyChunk(t, manager, store, 0, vchannel, buildIdempotencySections(
		idempotencyWrite{timeTick: 100, key: "key-a", pk: 1},
		idempotencyWrite{timeTick: 101, key: "key-b", pk: 2},
	))
	writeIdempotencyChunk(t, manager, store, 1, vchannel, buildIdempotencySections(
		idempotencyWrite{timeTick: 102, key: "key-c", pk: 3},
	))

	// The whole range spans both chunks.
	all, err := manager.ReadIdempotencyEntries(ctx, vchannel, 0, math.MaxUint64)
	require.NoError(t, err)
	require.Len(t, all.Inserts, 3)
	require.Len(t, all.Idempotency, 3)
	records, err := idempotencyview.RecordsFromSections(all.Idempotency, all.Inserts)
	require.NoError(t, err)
	assert.Equal(t, []string{"key-a", "key-b", "key-c"},
		[]string{records[0].IdempotencyKey, records[1].IdempotencyKey, records[2].IdempotencyKey})

	// (from, to] is exclusive at the low end and inclusive at the high end, and
	// it drops both halves of a filtered write together.
	window, err := manager.ReadIdempotencyEntries(ctx, vchannel, 100, 101)
	require.NoError(t, err)
	require.Len(t, window.Inserts, 1)
	require.Len(t, window.Idempotency, 1)
	assert.Equal(t, uint64(101), window.Inserts[0].GetSourceTimetick())
	assert.Equal(t, "key-b", window.Idempotency[0].GetKey())

	// A vchannel with no sections in the manifest reads back empty, not an error.
	none, err := manager.ReadIdempotencyEntries(ctx, "other-vchannel", 0, math.MaxUint64)
	require.NoError(t, err)
	assert.Empty(t, none.Inserts)
}

func TestReadIdempotencyEntriesBackfillsChunksWithoutKeys(t *testing.T) {
	// A chunk whose writes all lacked a client key stores no idempotency
	// section. Concatenating it with a chunk that has one would leave fewer
	// keys than inserts, and the join would then pair a key with another
	// write's rows.
	ctx := context.Background()
	manager, store := newTestManagerWithStore(t)
	vchannel := "by-dev-rootcoord-dml_0_40451v0"

	keyless := buildIdempotencySections(idempotencyWrite{timeTick: 100, key: "", pk: 1})
	keyless.Idempotency = nil // nothing carried a key, so no section is stored
	writeIdempotencyChunk(t, manager, store, 0, vchannel, keyless)
	writeIdempotencyChunk(t, manager, store, 1, vchannel, buildIdempotencySections(
		idempotencyWrite{timeTick: 101, key: "key-b", pk: 2},
	))

	got, err := manager.ReadIdempotencyEntries(ctx, vchannel, 0, math.MaxUint64)
	require.NoError(t, err)
	require.Len(t, got.Inserts, 2)
	require.Len(t, got.Idempotency, 2, "the keyless chunk must be backfilled to stay aligned")

	records, err := idempotencyview.RecordsFromSections(got.Idempotency, got.Inserts)
	require.NoError(t, err)
	assert.Empty(t, records[0].IdempotencyKey)
	assert.Equal(t, []int64{1}, records[0].InsertResult.GetIds().GetIntId().GetData())
	assert.Equal(t, "key-b", records[1].IdempotencyKey)
	assert.Equal(t, []int64{2}, records[1].InsertResult.GetIds().GetIntId().GetData())
}

func TestReadIdempotencyEntriesOmitsKeysWhenNoneCarryOne(t *testing.T) {
	// When nothing in the range had a key, the keys slice stays nil rather than
	// a run of empty placeholders.
	ctx := context.Background()
	manager, store := newTestManagerWithStore(t)
	vchannel := "by-dev-rootcoord-dml_0_40451v0"

	keyless := buildIdempotencySections(idempotencyWrite{timeTick: 100, key: "", pk: 1})
	keyless.Idempotency = nil
	writeIdempotencyChunk(t, manager, store, 0, vchannel, keyless)

	got, err := manager.ReadIdempotencyEntries(ctx, vchannel, 0, math.MaxUint64)
	require.NoError(t, err)
	require.Len(t, got.Inserts, 1)
	assert.Nil(t, got.Idempotency)
}

// newTestIdempotentInsertMessage builds an insert appended with a client key,
// carrying the result a duplicate would be answered with.
func newTestIdempotentInsertMessage(
	t *testing.T,
	vchannel string,
	timetick uint64,
	key string,
	pks []int64,
	rowOffsets []uint32,
) message.ImmutableMessage {
	t.Helper()
	header := &message.InsertMessageHeader{
		CollectionId: 1,
		Partitions: []*message.PartitionSegmentAssignment{{
			PartitionId: 10,
			Rows:        uint64(len(pks)),
		}},
	}
	if len(pks) > 0 {
		message.SetInsertHeaderIdempotentInsertResult(header, &messagespb.IdempotentInsertResult{
			RowOffsets: rowOffsets,
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: pks}},
			},
		})
	}
	builder := message.NewInsertMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(header).
		WithBody(&msgpb.InsertRequest{
			Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_Insert},
			CollectionID: 1,
			PartitionID:  10,
			NumRows:      uint64(len(pks)),
		})
	if key != "" {
		builder = builder.WithIdempotencyKey(key)
	}
	return builder.MustBuildMutable().
		WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick + 1)))
}

// newTestIdempotentTxnMessage assembles the txn shape a scanner hands to an
// observer: begin + keyed insert bodies + a commit carrying the idempotency
// key. This is what a multi-message insert on one vchannel actually looks like
// on the read side -- the proxy groups them into a txn whenever a vchannel
// takes more than one message (a partition-key collection, or a payload split
// by maxMessageSize), and stamps the key on the synthesized commit only.
func newTestIdempotentTxnMessage(
	t *testing.T,
	vchannel string,
	timetick uint64,
	key string,
	bodies [][]int64,
) message.ImmutableMessage {
	t.Helper()
	txnCtx := message.TxnContext{TxnID: message.TxnID(timetick), Keepalive: time.Hour}

	beginMsg := message.NewBeginTxnMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.BeginTxnMessageHeader{}).
		WithBody(&message.BeginTxnMessageBody{}).
		MustBuildMutable().
		WithTxnContext(txnCtx).
		WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick)))
	builder := message.NewImmutableTxnMessageBuilder(
		message.MustAsImmutableBeginTxnMessageV2(beginMsg),
	)

	offset := uint32(0)
	for i, pks := range bodies {
		offsets := make([]uint32, 0, len(pks))
		for range pks {
			offsets = append(offsets, offset)
			offset++
		}
		// The bodies carry no key: only the commit is deduplicated.
		body := newTestIdempotentInsertMessage(t, vchannel, timetick+uint64(i)+1, "", pks, offsets)
		builder.Add(body)
	}

	commitMsg := message.NewCommitTxnMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.CommitTxnMessageHeader{}).
		WithBody(&message.CommitTxnMessageBody{}).
		WithIdempotencyKey(key).
		MustBuildMutable().
		WithTxnContext(txnCtx).
		WithTimeTick(timetick + uint64(len(bodies)) + 1).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick + uint64(len(bodies)) + 2)))
	txnMsg, err := builder.Build(message.MustAsImmutableCommitTxnMessageV2(commitMsg))
	require.NoError(t, err)
	return txnMsg
}

// TestObserveStagesTxnIdempotencyRecord covers the shape an observer actually
// sees for a multi-message insert. The scanner packs begin + bodies + commit
// into ONE MessageTypeTxn message, and the assembly copies only the trace
// context off the commit -- the idempotency key stays on the commit
// sub-message. Reading only Insert/CommitTxn would stage nothing here, and
// worse, would take the keyless path that advances the confirmation frontier,
// so the write would be lost from the window with no way to replay it.
func TestObserveStagesTxnIdempotencyRecord(t *testing.T) {
	manager, _ := newTestManagerWithStore(t)
	ctx := context.Background()
	require.NoError(t, manager.Restore(ctx))

	txnMsg := newTestIdempotentTxnMessage(t, "v1", 100, "txn-key", [][]int64{{1, 2}, {3}})
	manager.ObserveMessage(ctx, txnMsg)

	manager.mu.Lock()
	require.Len(t, manager.pending, 1, "the txn must stage exactly one record")
	staged := manager.pending[0]
	manager.mu.Unlock()

	assert.Equal(t, "txn-key", staged.idempotency.GetKey())
	// The per-body results are merged in append order, reproducing what the
	// interceptor built in memory and handed to the window.
	assert.Equal(t, []uint32{0, 1, 2}, staged.idempotency.GetRowOffsets())
	assert.Equal(t, []int64{1, 2, 3}, staged.insert.GetIds().GetIntId().GetData())

	// It is readable back as one record, so recovery rebuilds the window with it.
	sections, err := manager.ReadIdempotencyEntries(ctx, "v1", 0, 1000)
	require.NoError(t, err)
	require.Len(t, sections.Inserts, 1)
	require.Len(t, sections.Idempotency, 1)
	assert.Equal(t, "txn-key", sections.Idempotency[0].GetKey())
	assert.Equal(t, []int64{1, 2, 3}, sections.Inserts[0].GetIds().GetIntId().GetData())
}

// A txn whose commit carries no key is not an idempotent write; it must stage
// nothing and stay on the keyless path.
func TestObserveIgnoresKeylessTxn(t *testing.T) {
	manager, _ := newTestManagerWithStore(t)
	ctx := context.Background()
	require.NoError(t, manager.Restore(ctx))

	txnMsg := newTestIdempotentTxnMessage(t, "v1", 100, "", [][]int64{{1, 2}})
	manager.ObserveMessage(ctx, txnMsg)

	manager.mu.Lock()
	defer manager.mu.Unlock()
	assert.Empty(t, manager.pending)
}

func observeMessage(t *testing.T, manager *Manager, msg message.ImmutableMessage) {
	t.Helper()
	manager.ObserveMessage(context.Background(), msg)
}

func TestObserveMessageStagesIdempotentInserts(t *testing.T) {
	ctx := context.Background()
	manager, store := newTestManagerWithStore(t)
	vchannel := "by-dev-rootcoord-dml_0_40451v0"

	observeMessage(t, manager, newTestIdempotentInsertMessage(t, vchannel, 100, "key-a", []int64{1, 2}, []uint32{0, 1}))
	// An insert without a client key materializes nothing for any consumer and
	// must not be staged, or every insert's primary keys would reach storage.
	observeMessage(t, manager, newTestIdempotentInsertMessage(t, vchannel, 101, "", []int64{3}, nil))
	observeMessage(t, manager, newTestIdempotentInsertMessage(t, vchannel, 102, "key-b", []int64{4}, []uint32{0}))

	manager.mu.Lock()
	staged := len(manager.pending)
	manager.mu.Unlock()
	require.Equal(t, 2, staged, "only the keyed inserts are staged")

	sc := manager.seal()
	require.NotNil(t, sc)
	_, err := manager.writeOnce(ctx)
	require.NoError(t, err)

	got, err := manager.ReadIdempotencyEntries(ctx, vchannel, 0, math.MaxUint64)
	require.NoError(t, err)
	records, err := idempotencyview.RecordsFromSections(got.Idempotency, got.Inserts)
	require.NoError(t, err)
	require.Len(t, records, 2)
	assert.Equal(t, "key-a", records[0].IdempotencyKey)
	assert.Equal(t, []int64{1, 2}, records[0].InsertResult.GetIds().GetIntId().GetData())
	assert.Equal(t, []uint32{0, 1}, records[0].InsertResult.GetRowOffsets())
	assert.Equal(t, "key-b", records[1].IdempotencyKey)

	_ = store
}

func TestIdempotencyKeyOfIsGatedByOriginAndType(t *testing.T) {
	vchannel := "by-dev-rootcoord-dml_0_40451v0"

	keyed := newTestIdempotentInsertMessage(t, vchannel, 100, "key-a", []int64{1}, []uint32{0})
	assert.Equal(t, "key-a", idempotencyKeyOf(keyed))

	// A replicated insert preserves the SOURCE cluster's key. Remembering it
	// locally would drive replicated appends down the duplicate path after a
	// restart, so it must read as keyless.
	replicated := message.NewInsertMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&message.InsertMessageHeader{CollectionId: 1}).
		WithBody(&msgpb.InsertRequest{Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_Insert}}).
		WithIdempotencyKey("key-from-source").
		MustBuildMutable().
		WithReplicateHeader(&message.ReplicateHeader{
			ClusterID:              "source-cluster",
			MessageID:              walimplstest.NewTestMessageID(100),
			LastConfirmedMessageID: walimplstest.NewTestMessageID(99),
			VChannel:               vchannel,
			TimeTick:               100,
		}).
		WithTimeTick(101).
		WithLastConfirmed(walimplstest.NewTestMessageID(101)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(102))
	assert.Empty(t, idempotencyKeyOf(replicated))
	keys, insert := idempotencyHalvesOf(replicated)
	assert.Nil(t, keys)
	assert.Nil(t, insert)

	// The key property alone must not materialize a record for a type the
	// append path never deduplicates.
	deleteMsg := newTestDeleteMessage(t, vchannel, 103, 10, 1)
	assert.Empty(t, idempotencyKeyOf(deleteMsg))
}

func TestStagedRecordSizeChargesTheRecordNotTheMessage(t *testing.T) {
	// An insert message carries the whole row; the record keeps only the key,
	// the offsets and the primary keys. Charging the message would seal chunks
	// orders of magnitude too early.
	vchannel := "by-dev-rootcoord-dml_0_40451v0"
	msg := newTestIdempotentInsertMessage(t, vchannel, 100, "key-a", []int64{1, 2}, []uint32{0, 1})
	keys, insert := idempotencyHalvesOf(msg)
	require.NotNil(t, insert)

	record := &stagedRecord{idempotency: keys, insert: insert}
	assert.Less(t, stagedRecordSize(msg, record), uint64(msg.EstimateSize()))

	// A record with no idempotency halves has nothing of its own to charge, so
	// it falls back to the message size.
	keylessMsg := newTestDeleteMessage(t, vchannel, 101, 10, 1)
	assert.Equal(t, uint64(keylessMsg.EstimateSize()), stagedRecordSize(keylessMsg, &stagedRecord{}))
}
