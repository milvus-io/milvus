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
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
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

func newTestManager(t *testing.T, store *Store, flushMaxBytes, retentionMaxBytes uint64) *Manager {
	t.Helper()
	return NewManager(ManagerConfig{
		PChannel:          store.PChannel(),
		Term:              store.Term(),
		Store:             store,
		Runtime:           moduleapi.Runtime{},
		FlushMaxBytes:     flushMaxBytes,
		RetentionMaxBytes: retentionMaxBytes,
	})
}

func newTestManagerWithStore(t *testing.T) (*Manager, *Store) {
	t.Helper()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	store := NewStore(cm, "by-dev-rootcoord-dml_0_40451v0", 1)
	return newTestManager(t, store, 1<<20, 1<<30), store
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
func observeDelete(t *testing.T, manager *Manager, vchannel string, timetick uint64, finalized *bool) {
	t.Helper()
	msg := newTestDeleteMessage(t, vchannel, timetick, 10, int64(timetick))
	owner := message.NewOwnedImmutableMessage(msg, func() { *finalized = true })
	retained := owner.Clone()
	manager.ObserveMessage(context.Background(), retained)
	retained.Release()
	owner.Release()
}

// drainTasks runs every pending write task to completion. A task's Execute
// drains the whole sealed queue, so one pass normally suffices; the loop
// covers a task submitted while another was still running.
func drainTasks(t *testing.T, manager *Manager) {
	t.Helper()
	ctx := context.Background()
	for manager.HasPendingWork() {
		manager.mu.Lock()
		manager.flushTasks = compactWriteTasks(manager.flushTasks)
		tasks := append([]*summaryWriteTask(nil), manager.flushTasks...)
		manager.mu.Unlock()
		require.NotEmpty(t, tasks, "HasPendingWork true but no runnable task")
		for _, task := range tasks {
			require.NoError(t, task.Execute(ctx))
		}
	}
}

// flushObserved observes one delete message and forces a flush through it.
func flushObserved(t *testing.T, manager *Manager, vchannel string, timetick uint64, finalized *bool) {
	t.Helper()
	observeDelete(t, manager, vchannel, timetick, finalized)
	manager.RequestFlushThrough(timetick)
	drainTasks(t, manager)
}

// TestObserveMessageCopiesRecordWithoutRetaining verifies the summary never
// retains the WAL message: the record is built and copied at observation, the
// message is released immediately, and a later flush persists the copied
// record.
func TestObserveMessageCopiesRecordWithoutRetaining(t *testing.T) {
	manager, _ := newTestManagerWithStore(t)
	ctx := context.Background()

	finalized := false
	observeDelete(t, manager, "v1", 100, &finalized)
	assert.True(t, finalized, "the message must be released at observation — the summary holds no reference")
	manager.mu.Lock()
	require.Len(t, manager.pending, 1)
	assert.Equal(t, uint64(100), manager.pending[0].timeTick)
	assert.NotNil(t, manager.pending[0].entry, "the record is built at observation")
	manager.mu.Unlock()

	manager.RequestFlushThrough(100)
	assert.True(t, manager.HasPendingWork())
	drainTasks(t, manager)
	assert.Equal(t, uint64(100), manager.LatestCoveredTimeTick())

	decoded, footer, err := manager.cfg.Store.ReadChunk(ctx, 0, 1)
	require.NoError(t, err)
	require.Equal(t, uint64(0), footer.GetGeneration())
	require.Len(t, decoded, 1, "chunk must carry only vchannels that had records")
	require.Len(t, decoded["v1"], 1)
	assert.Equal(t, uint64(100), decoded["v1"][0].GetTimeTick())
	assert.Equal(t, int64(10), decoded["v1"][0].GetDelete().GetBlocks()[0].GetPartitionId())
}

func TestObserveMessageIgnoresNonVChannelRecords(t *testing.T) {
	manager, _ := newTestManagerWithStore(t)
	ctx := context.Background()

	// A control-channel message must not be retained.
	controlMsg := newTestDeleteMessage(t, "by-dev-rootcoord-dml_0vcchan", 100, 10, int64(100))
	controlRetained := message.NewOwnedImmutableMessage(controlMsg, func() {}).Clone()
	manager.ObserveMessage(ctx, controlRetained)
	controlRetained.Release()
	manager.mu.Lock()
	assert.Empty(t, manager.pending, "control channel must not be retained")
	manager.mu.Unlock()

	// A vchannel-less (pchannel-level) message must not be retained either.
	levelMsg := newTestDeleteMessage(t, "", 100, 10, int64(100))
	levelRetained := message.NewOwnedImmutableMessage(levelMsg, func() {}).Clone()
	manager.ObserveMessage(ctx, levelRetained)
	levelRetained.Release()
	manager.mu.Lock()
	assert.Empty(t, manager.pending)
	manager.mu.Unlock()

	// Nothing was pending; a forced flush produces no chunk.
	manager.RequestFlushThrough(100)
	assert.False(t, manager.HasPendingWork())
	_, _, err := manager.cfg.Store.ReadChunk(ctx, 0, 1)
	assert.Error(t, err, "no chunk must be written when nothing was staged")
}

// TestObserveAllChannelMessageAdvancesLastAcked verifies that all-channel
// messages (e.g. persisted time ticks, whose VChannel() is "") advance the
// summary confirmation frontier: the recovery storage merges that frontier
// into the persisted checkpoint, so an idle pchannel refreshes its checkpoint
// TimeTick on every persisted time tick. An unflushed staged delete record
// must still pin the frontier behind it.
func TestObserveAllChannelMessageAdvancesLastAcked(t *testing.T) {
	manager, _ := newTestManagerWithStore(t)
	ctx := context.Background()

	// The first all-channel message initializes the frontier from scratch.
	msgID := walimplstest.NewTestMessageID(100)
	ttMsg := message.CreateTestTimeTickSyncMessage(t, 1, 100, msgID).IntoImmutableMessage(msgID)
	owner := message.NewOwnedImmutableMessage(ttMsg, func() {})
	retained := owner.Clone()
	manager.ObserveMessage(ctx, retained)
	retained.Release()
	owner.Release()

	acked := manager.LastAcked()
	require.NotNil(t, acked, "all-channel message must advance the frontier")
	assert.Equal(t, uint64(100), acked.TimeTick)
	assert.True(t, acked.MessageID.EQ(msgID), "frontier must carry the all-channel message id")

	// An unflushed staged delete pins the frontier: the all-channel message
	// observed after it must not advance past the staged record.
	var finalized bool
	observeDelete(t, manager, "v1", 150, &finalized)
	manager.mu.Lock()
	require.NotEmpty(t, manager.pending, "delete record must be staged")
	manager.mu.Unlock()

	msgID2 := walimplstest.NewTestMessageID(200)
	ttMsg2 := message.CreateTestTimeTickSyncMessage(t, 1, 200, msgID2).IntoImmutableMessage(msgID2)
	owner2 := message.NewOwnedImmutableMessage(ttMsg2, func() {})
	retained2 := owner2.Clone()
	manager.ObserveMessage(ctx, retained2)
	retained2.Release()
	owner2.Release()

	acked = manager.LastAcked()
	require.NotNil(t, acked)
	assert.Equal(t, uint64(100), acked.TimeTick,
		"pending delete record must pin the frontier behind it")
	assert.True(t, acked.MessageID.EQ(msgID))

	// A vchannel-less non-time-tick message (e.g. a pchannel-level broadcast)
	// must not move the frontier either: only all-channel time ticks do.
	broadcastMsg := newTestDeleteMessage(t, "", 300, 10, int64(300))
	broadcastRetained := message.NewOwnedImmutableMessage(broadcastMsg, func() {}).Clone()
	manager.ObserveMessage(ctx, broadcastRetained)
	broadcastRetained.Release()
	acked = manager.LastAcked()
	require.NotNil(t, acked)
	assert.Equal(t, uint64(100), acked.TimeTick,
		"non-time-tick vchannel-less message must not advance the frontier")
	assert.True(t, acked.MessageID.EQ(msgID))
}

func TestObserveMessageSealsAtThreshold(t *testing.T) {
	manager, _ := newTestManagerWithStore(t)
	scheduler := &recordingScheduler{}
	manager.cfg.Runtime.Scheduler = scheduler
	manager.cfg.FlushMaxBytes = 1 // any retained record triggers.

	var unused bool
	observeDelete(t, manager, "v1", 100, &unused)
	assert.Len(t, scheduler.tasks, 1, "the byte threshold seals and submits a write task")
}

func TestObserveMessageSkipsRecordsAtOrBelowDurableFrontier(t *testing.T) {
	ctx := context.Background()
	manager1, _ := newTestManagerWithStore(t)

	// Seed a durable frontier: a chunk covering v1 up to 150.
	var unused bool
	flushObserved(t, manager1, "v1", 150, &unused)

	manager2 := newTestManager(t, manager1.cfg.Store, 1<<20, 1<<30)
	require.NoError(t, manager2.Restore(ctx, nil))
	assert.Equal(t, uint64(150), manager2.durableFrontiers["v1"])
	manager2.cfg.FlushMaxBytes = 1

	// Records at or below the restored durable frontier are skipped entirely:
	// recovery replay re-observes them, and staging them again would rewrite
	// the same records into a new chunk.
	finalizedA := false
	observeDelete(t, manager2, "v1", 100, &finalizedA)
	assert.True(t, finalizedA, "a skipped record is never staged")
	assert.False(t, manager2.HasPendingWork())

	// A record past the frontier is staged and flushed as usual (the byte
	// threshold is 1, so the observation seals it immediately).
	finalizedB := false
	observeDelete(t, manager2, "v1", 200, &finalizedB)
	assert.True(t, finalizedB, "the message is released at observation")
	drainTasks(t, manager2)
	entries, err := manager2.ReadTransformEntries(ctx, "v1", 150, 1000)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, uint64(200), entries[0].GetTimeTick())
}

func TestRequestFlushThroughSchedulesWrite(t *testing.T) {
	manager, _ := newTestManagerWithStore(t)
	scheduler := &recordingScheduler{}
	manager.cfg.Runtime.Scheduler = scheduler

	var unused bool
	observeDelete(t, manager, "v1", 100, &unused)
	assert.Empty(t, scheduler.tasks, "below the byte threshold nothing is scheduled")
	observeDelete(t, manager, "v1", 150, &unused)
	manager.RequestFlushThrough(160)
	assert.Len(t, scheduler.tasks, 1, "a forced flush seals the pending span")
}

func TestRequestFlushThroughNoopWhenCovered(t *testing.T) {
	manager, _ := newTestManagerWithStore(t)

	var unused bool
	observeDelete(t, manager, "v1", 200, &unused)
	manager.RequestFlushThrough(200) // seals a chunk covering 200.
	manager.mu.Lock()
	require.Equal(t, uint64(200), manager.pendingFlushTimeTick)
	manager.mu.Unlock()

	// The pending flush already covers the request: no-op.
	manager.RequestFlushThrough(150)
	manager.mu.Lock()
	assert.Equal(t, uint64(200), manager.pendingFlushTimeTick, "a request at or below the pending flush is a no-op")
	manager.mu.Unlock()

	// A request past the pending flush seals again.
	observeDelete(t, manager, "v1", 300, &unused)
	manager.RequestFlushThrough(300)
	manager.mu.Lock()
	assert.Equal(t, uint64(300), manager.pendingFlushTimeTick, "a request past the pending flush seals the newer span")
	manager.mu.Unlock()
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
	recovered := newTestManager(t, manager.cfg.Store, 1<<20, 1<<30)
	require.NoError(t, recovered.Restore(ctx, nil))
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
	orphanRecords := map[string][]*streamingpb.VChannelSummaryTransformRecord{
		"v1": {{TimeTick: 300, Delete: &streamingpb.TransformDeleteEntry{}}},
	}
	_, _, err := manager.cfg.Store.WriteChunk(ctx, 2, orphanRecords)
	require.NoError(t, err)

	recovered := newTestManager(t, manager.cfg.Store, 1<<20, 1<<30)
	require.NoError(t, recovered.Restore(ctx, nil))
	assert.Equal(t, uint64(3), recovered.nextGeneration)
	assert.Equal(t, uint64(300), recovered.LatestCoveredTimeTick())
	require.Len(t, recovered.Manifest().GetChunks(), 2)
	// The probed tail is sealed into a published manifest: a third recovery
	// sees it without probing again.
	again := newTestManager(t, manager.cfg.Store, 1<<20, 1<<30)
	require.NoError(t, again.Restore(ctx, nil))
	assert.Equal(t, uint64(3), again.nextGeneration)
}

// TestManagerRestoreGCFromVChannelMeta covers the cohesive recovery of the GC
// positions: Restore derives them from the catalog metas — a dropped vchannel
// releases everything, a live vchannel releases up to its persisted
// materialization frontier — so the recovery wiring does not touch the summary
// internals at all.
func TestManagerRestoreGCFromVChannelMeta(t *testing.T) {
	manager, _ := newTestManagerWithStore(t)
	ctx := context.Background()

	var unused bool
	flushObserved(t, manager, "v1", 100, &unused)
	flushObserved(t, manager, "v2", 200, &unused)

	recovered := newTestManager(t, manager.cfg.Store, 1<<20, 1<<30)
	metas := map[string]*streamingpb.VChannelMeta{
		"v1": {Vchannel: "v1", State: streamingpb.VChannelState_VCHANNEL_STATE_DROPPED},
		"v2": {Vchannel: "v2", TransformMaterializedTimeTick: 200},
	}
	require.NoError(t, recovered.Restore(ctx, metas))
	assert.Equal(t, uint64(DroppedVChannelTimeTick), recovered.gcFrontiers["v1"])
	assert.Equal(t, uint64(200), recovered.gcFrontiers["v2"])
	// The v1 chunk is releasable without any materialization frontier.
	recovered.cfg.RetentionMaxBytes = 1
	require.NoError(t, recovered.GCOnce(ctx))
	assert.Empty(t, recovered.Manifest().GetChunks())
}

func TestManagerGCReleaseAndMaterializationFloor(t *testing.T) {
	manager, _ := newTestManagerWithStore(t)
	ctx := context.Background()

	var unused bool
	flushObserved(t, manager, "v1", 100, &unused)
	flushObserved(t, manager, "v1", 200, &unused)
	flushObserved(t, manager, "v1", 300, &unused)
	require.Len(t, manager.Manifest().GetChunks(), 3)

	// Without a GC position nothing is eligible, even under budget pressure.
	manager.cfg.RetentionMaxBytes = 1
	require.NoError(t, manager.GCOnce(ctx))
	assert.Len(t, manager.Manifest().GetChunks(), 3)

	// Advance the GC position through 200 (a completed materialization):
	// chunks 0 (end 100) and 1 (end 200) are fully consumed and released;
	// chunk 2 (end 300) still holds records past the position and stays.
	manager.AdvanceGCTimeTick("v1", 200)
	require.NoError(t, manager.GCOnce(ctx))
	chunks := manager.Manifest().GetChunks()
	require.Len(t, chunks, 1)
	assert.Equal(t, uint64(2), chunks[0].GetGeneration())
	// The released object is gone.
	_, _, err := manager.cfg.Store.ReadChunk(ctx, 0, 1)
	assert.Error(t, err)
	// pending_gc drained.
	assert.Empty(t, manager.Manifest().GetPendingGc())

	// Advance past everything: all chunks are released.
	manager.AdvanceGCTimeTick("v1", 400)
	require.NoError(t, manager.GCOnce(ctx))
	assert.Empty(t, manager.Manifest().GetChunks())
}

func TestAdvanceGCTimeTickDroppedAllowsGCRelease(t *testing.T) {
	manager, _ := newTestManagerWithStore(t)
	ctx := context.Background()

	var unused bool
	flushObserved(t, manager, "v1", 100, &unused)
	require.Len(t, manager.Manifest().GetChunks(), 1)

	// Without any GC position the chunk is not releasable.
	manager.cfg.RetentionMaxBytes = 1
	require.NoError(t, manager.GCOnce(ctx))
	require.Len(t, manager.Manifest().GetChunks(), 1)

	// The GC boundary of a dropped vchannel makes its chunks releasable
	// regardless of materialization. The notification touches nothing else.
	manager.AdvanceGCTimeTick("v1", DroppedVChannelTimeTick)
	require.NoError(t, manager.GCOnce(ctx))
	assert.Empty(t, manager.Manifest().GetChunks())
	_, _, err := manager.cfg.Store.ReadChunk(ctx, 0, 1)
	assert.Error(t, err, "chunk object must be deleted after release")
}

func TestDurableTimeTickDerivedFromManifest(t *testing.T) {
	ctx := context.Background()
	manager, _ := newTestManagerWithStore(t)
	require.NoError(t, manager.Restore(ctx, nil))
	assert.Zero(t, manager.DurableTimeTick("v1"))

	var unused bool
	flushObserved(t, manager, "v1", 100, &unused)
	flushObserved(t, manager, "v1", 200, &unused)
	assert.Equal(t, uint64(200), manager.DurableTimeTick("v1"))

	// A vchannel with no records has no frontier.
	assert.Zero(t, manager.DurableTimeTick("v2"))
}

// TestFlushThresholdAccumulatesAcrossVChannels proves the autonomous flush
// decision is pchannel-wide: pending bytes accumulate across every vchannel,
// and a chunk is sealed only when the TOTAL reaches FlushMaxBytes — not when
// any single vchannel crosses it.
func TestFlushThresholdAccumulatesAcrossVChannels(t *testing.T) {
	manager, _ := newTestManagerWithStore(t)
	scheduler := &recordingScheduler{}
	manager.cfg.Runtime.Scheduler = scheduler
	msg := newTestDeleteMessage(t, "v1", 100, 10, int64(100))
	manager.cfg.FlushMaxBytes = uint64(msg.EstimateSize()) + 1

	var unused bool
	observeDelete(t, manager, "v1", 100, &unused)
	assert.Empty(t, scheduler.tasks, "one vchannel below the pchannel threshold")

	// The combined pending across vchannels crosses the threshold: seal once.
	observeDelete(t, manager, "v2", 200, &unused)
	assert.Len(t, scheduler.tasks, 1, "combined pending crosses the pchannel threshold")
}

func TestManagerHasPendingWork(t *testing.T) {
	manager, _ := newTestManagerWithStore(t)

	assert.False(t, manager.HasPendingWork())
	var unused bool
	observeDelete(t, manager, "v1", 100, &unused)
	// Pending staging alone is not pending work (no flush scheduled).
	assert.False(t, manager.HasPendingWork())
	manager.RequestFlushThrough(100)
	assert.True(t, manager.HasPendingWork())
	// Executing the scheduled task completes it and drains the queue.
	drainTasks(t, manager)
	assert.False(t, manager.HasPendingWork())
}

func TestWriteTaskMergesConcurrentRequests(t *testing.T) {
	manager, _ := newTestManagerWithStore(t)

	var unused bool
	observeDelete(t, manager, "v1", 100, &unused)
	manager.RequestFlushThrough(100)
	observeDelete(t, manager, "v1", 200, &unused)
	manager.RequestFlushThrough(200)
	observeDelete(t, manager, "v1", 300, &unused)
	manager.RequestFlushThrough(300)
	// At most one write task exists at a time: it drains the whole sealed
	// queue, so concurrent requests merge into it instead of queueing one
	// task per request.
	manager.mu.Lock()
	require.Len(t, manager.flushTasks, 1)
	manager.mu.Unlock()
	drainTasks(t, manager)
	assert.False(t, manager.HasPendingWork())
	require.Len(t, manager.Manifest().GetChunks(), 3, "each sealed span becomes one chunk")
}

// TestManagerFlushAdvancesLastAcked verifies the confirmation frontier
// advances to the record once its chunk is durable end to end.
func TestManagerFlushAdvancesLastAcked(t *testing.T) {
	ctx := context.Background()
	manager, _ := newTestManagerWithStore(t)
	require.NoError(t, manager.Restore(ctx, nil))

	var unused bool
	observeDelete(t, manager, "v1", 100, &unused)
	// While the record is staged, the frontier stays at the record's
	// last-confirmed position (the delete message is not yet durable).
	acked := manager.LastAcked()
	require.NotNil(t, acked)
	assert.Equal(t, int64(100), messageIDInt(acked.MessageID), "frontier pinned at the message before the staged record")
	manager.RequestFlushThrough(100)
	drainTasks(t, manager)
	acked = manager.LastAcked()
	require.NotNil(t, acked)
	assert.Equal(t, int64(101), messageIDInt(acked.MessageID), "frontier advances to the durable record")
}

// messageIDInt converts the test message ID back to its int64 value.
func messageIDInt(id message.MessageID) int64 {
	v, err := strconv.ParseInt(id.Marshal(), 10, 64)
	if err != nil {
		panic(err)
	}
	return v
}

func TestManagerReadTransformEntriesAcrossChunks(t *testing.T) {
	ctx := context.Background()
	manager, _ := newTestManagerWithStore(t)
	require.NoError(t, manager.Restore(ctx, nil))

	// Two flushes produce two chunks; recovery-style reads span them.
	var unused bool
	flushObserved(t, manager, "v1", 100, &unused)
	flushObserved(t, manager, "v1", 200, &unused)
	entries, err := manager.ReadTransformEntries(ctx, "v1", 0, 1000)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	assert.Equal(t, uint64(100), entries[0].GetTimeTick())
	assert.Equal(t, uint64(200), entries[1].GetTimeTick())

	// The from-boundary is exclusive.
	entries, err = manager.ReadTransformEntries(ctx, "v1", 100, 1000)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, uint64(200), entries[0].GetTimeTick())
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
	manager := newTestManager(t, store, 1, 1<<30)
	require.NoError(t, manager.Restore(ctx, nil))
	finalized := false

	cm.failManifest.Store(true)
	observeDelete(t, manager, "v1", 100, &finalized)
	manager.mu.Lock()
	require.Len(t, manager.flushTasks, 1)
	task := manager.flushTasks[0]
	manager.mu.Unlock()
	// Chunk write succeeds, manifest publish fails.
	require.Error(t, task.Execute(ctx))
	// The amendment is not installed; the generation stays claimed and the
	// failed chunk stays at the queue head for the retry. The batch is not
	// durable end to end, so the confirmation frontier stays behind it. The
	// chunk object itself is already durable.
	assert.Len(t, manager.manifest.GetChunks(), 0)
	assert.Equal(t, uint64(1), manager.nextGeneration)
	acked := manager.LastAcked()
	require.NotNil(t, acked)
	assert.Equal(t, int64(100), messageIDInt(acked.MessageID), "frontier stays at the message before the record while the batch is not durable")
	assert.True(t, manager.HasPendingWork(), "the failed chunk waits for the retry")
	keys, _, err := storage.ListAllChunkWithPrefix(ctx, cm, buildChunkPrefix(cm, store.PChannel()), false)
	require.NoError(t, err)
	require.Len(t, keys, 1, "chunk 0 is already durable")

	// The retry succeeds: the same sealed chunk is published with the same
	// generation — exactly one chunk object and one manifest entry.
	cm.failManifest.Store(false)
	require.NoError(t, task.Execute(ctx))
	acked = manager.LastAcked()
	require.NotNil(t, acked)
	assert.Equal(t, int64(101), messageIDInt(acked.MessageID), "frontier advances once the batch is durable end to end")
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
	manager := newTestManager(t, store, 1, 1<<30)
	require.NoError(t, manager.Restore(ctx, nil))
	finalizedA := false
	finalizedB := false

	// First attempt: the chunk write fails; the sealed chunk stays at the
	// queue head for the retry.
	cm.failChunk.Store(true)
	observeDelete(t, manager, "v1", 100, &finalizedA)
	manager.mu.Lock()
	require.Len(t, manager.flushTasks, 1)
	task := manager.flushTasks[0]
	manager.mu.Unlock()
	require.Error(t, task.Execute(ctx))
	keys, _, err := storage.ListAllChunkWithPrefix(ctx, cm, buildChunkPrefix(cm, store.PChannel()), false)
	require.NoError(t, err)
	assert.Empty(t, keys, "the failed chunk write left no durable object")

	// A new record [B] is observed during the retry window; it seals into a
	// fresh generation (the threshold is 1).
	observeDelete(t, manager, "v1", 200, &finalizedB)

	// The retry drains the whole queue: [A] under generation 0, [B] under
	// generation 1 — one object per batch, never a conflicting rewrite.
	cm.failChunk.Store(false)
	require.NoError(t, task.Execute(ctx))
	require.Len(t, manager.manifest.GetChunks(), 2)
	assert.Equal(t, uint64(0), manager.manifest.GetChunks()[0].GetGeneration())
	assert.Equal(t, uint64(1), manager.manifest.GetChunks()[1].GetGeneration())
	keys, _, err = storage.ListAllChunkWithPrefix(ctx, cm, buildChunkPrefix(cm, store.PChannel()), false)
	require.NoError(t, err)
	assert.Len(t, keys, 2, "one object per generation; the failed generation-0 rewrite never duplicated")
}

// TestFlushChunkCorruptedFailsTaskWithoutRetry covers the terminal-error
// classification: a store-corruption error from the chunk write must NOT be
// marked ErrDelay — the task fails loudly and is dropped (done), so the
// manager can schedule successor flushes instead of retrying the same
// corrupted write forever. The abandoned chunk pins the confirmation frontier
// before it, so the WAL checkpoint stalls and recovery replays it after a
// restart.
func TestFlushChunkCorruptedFailsTaskWithoutRetry(t *testing.T) {
	ctx := context.Background()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	store := NewStore(cm, "by-dev-rootcoord-dml_0_40451v0", 1)
	manager := newTestManager(t, store, 1, 1<<30)
	require.NoError(t, manager.Restore(ctx, nil))
	finalizedA := false
	finalizedB := false

	// A conflicting object already occupies generation 0 under this term:
	// WriteChunk detects the same-generation different-payload as corruption.
	require.NoError(t, cm.Write(ctx, store.ChunkKey(0), []byte("conflicting payload")))
	observeDelete(t, manager, "v1", 100, &finalizedA)
	manager.mu.Lock()
	require.Len(t, manager.flushTasks, 1)
	task := manager.flushTasks[0]
	manager.mu.Unlock()

	err := task.Execute(ctx)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrStoreCorrupted), "the conflict surfaces as store corruption")
	assert.False(t, errors.Is(err, nodescheduler.ErrDelay), "a terminal error must not be retried")
	assert.True(t, task.Done(), "the task is dropped so successor flushes can proceed")

	// The abandoned record pins the frontier at its last-confirmed message;
	// recovery replays it after a restart.
	acked := manager.LastAcked()
	require.NotNil(t, acked)
	assert.Equal(t, int64(100), messageIDInt(acked.MessageID), "frontier stays at the message before the abandoned record")

	// The manager is not stuck: a successor flush collects newer staging into
	// a fresh generation.
	observeDelete(t, manager, "v1", 200, &finalizedB)
	manager.mu.Lock()
	manager.flushTasks = compactWriteTasks(manager.flushTasks)
	require.Len(t, manager.flushTasks, 1)
	nextTask := manager.flushTasks[0]
	manager.mu.Unlock()
	require.NoError(t, nextTask.Execute(ctx))
	require.Len(t, manager.manifest.GetChunks(), 1)
	assert.Equal(t, uint64(1), manager.manifest.GetChunks()[0].GetGeneration())
	// The abandoned record still pins the frontier: the successor's durable
	// chunk (record at 201) must not advance the frontier past it.
	acked = manager.LastAcked()
	require.NotNil(t, acked)
	assert.Equal(t, int64(100), messageIDInt(acked.MessageID), "frontier stays behind the abandoned record")
}

// TestGCOnceRemovesAllPending covers the snapshot iteration of the pending GC
// queue: removePendingGC compacts the live slice in place, and a naive range
// over the live slice would skip entries once the indexes shift.
func TestGCOnceRemovesAllPending(t *testing.T) {
	ctx := context.Background()
	manager, store := newTestManagerWithStore(t)
	require.NoError(t, manager.Restore(ctx, nil))

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

// TestAdvanceGCTimeTickDuringRetryAdvancesFrontier covers a write racing a
// vchannel cleanup: a chunk-write failure keeps the sealed chunk at the queue
// head, the vchannel is dropped (GC boundary) during the retry window, and the
// retry's write must still advance the confirmation frontier. The GC boundary
// touches only retention state, never the pending buffer, so the honest flush
// of the records goes through unchanged.
func TestAdvanceGCTimeTickDuringRetryAdvancesFrontier(t *testing.T) {
	ctx := context.Background()
	cm := &failInjectingChunkManager{
		ChunkManager: storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir())),
	}
	store := NewStore(cm, "by-dev-rootcoord-dml_0_40451v0", 1)
	manager := newTestManager(t, store, 1, 1<<30)
	require.NoError(t, manager.Restore(ctx, nil))
	var finalized bool
	observeDelete(t, manager, "v1", 100, &finalized)

	cm.failChunk.Store(true)
	manager.mu.Lock()
	require.Len(t, manager.flushTasks, 1)
	task := manager.flushTasks[0]
	manager.mu.Unlock()
	require.Error(t, task.Execute(ctx))
	acked := manager.LastAcked()
	require.NotNil(t, acked)
	assert.Equal(t, int64(100), messageIDInt(acked.MessageID), "frontier pinned while the batch is not durable")

	// The vchannel is dropped while the chunk waits for the retry.
	manager.AdvanceGCTimeTick("v1", DroppedVChannelTimeTick)

	// The retry completes: the durable write advances the frontier.
	cm.failChunk.Store(false)
	require.NoError(t, task.Execute(ctx))
	acked = manager.LastAcked()
	require.NotNil(t, acked)
	assert.Equal(t, int64(101), messageIDInt(acked.MessageID), "frontier advances once the retry's write is durable")
}

// TestConcurrentFlushAndGCRelease exercises the manifest publish paths
// concurrently. Run with -race: a torn publish (a marshal racing an in-place
// edit of the shared manifest) would surface here as a data race, and the CAS
// through the single manager lock is what keeps flush and GC publishes from
// interleaving.
func TestConcurrentFlushAndGCRelease(t *testing.T) {
	ctx := context.Background()
	manager, _ := newTestManagerWithStore(t)
	manager.cfg.RetentionMaxBytes = 1 // any chunk becomes releasable once the GC position is set
	require.NoError(t, manager.Restore(ctx, nil))
	manager.AdvanceGCTimeTick("v1", 1<<30)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			var unused bool
			observeDelete(t, manager, "v1", uint64(100+i), &unused)
			manager.RequestFlushThrough(uint64(100 + i))
			drainTasks(t, manager)
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

// newTestBarrierMessage builds a CreateCollection (barrier-class) message of
// the given vchannel: it never produces a transform record, so observing it
// only moves the confirmation frontier.
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

// TestObserveDeletePinsLastAckedUntilDurable verifies the confirmation
// frontier is pinned before a staged delete record and advances only after
// the record's chunk is durable.
func TestObserveDeletePinsLastAckedUntilDurable(t *testing.T) {
	manager, _ := newTestManagerWithStore(t)
	ctx := context.Background()
	require.NoError(t, manager.Restore(ctx, nil))

	var unused bool
	observeDelete(t, manager, "v1", 100, &unused)
	require.Len(t, manager.pending, 1)
	acked := manager.LastAcked()
	require.NotNil(t, acked)
	assert.Equal(t, int64(100), messageIDInt(acked.MessageID), "frontier pinned at the record's last-confirmed message")
	assert.Equal(t, uint64(99), acked.TimeTick, "timetick pinned one tick before the record")

	manager.RequestFlushThrough(100)
	drainTasks(t, manager)
	acked = manager.LastAcked()
	require.NotNil(t, acked)
	assert.Equal(t, int64(101), messageIDInt(acked.MessageID), "frontier advances to the durable record")
	assert.Equal(t, uint64(100), acked.TimeTick)
}

// TestObserveNonDeleteAdvancesLastAcked verifies a DDL/flush/barrier message
// (no record) advances the confirmation frontier when nothing is staged.
func TestObserveNonDeleteAdvancesLastAcked(t *testing.T) {
	manager, _ := newTestManagerWithStore(t)
	ctx := context.Background()
	require.NoError(t, manager.Restore(ctx, nil))

	msg := newTestBarrierMessage(t, "v1", 150)
	owner := message.NewOwnedImmutableMessage(msg, func() {})
	retained := owner.Clone()
	manager.ObserveMessage(ctx, retained)
	retained.Release()
	owner.Release()

	require.Empty(t, manager.pending)
	acked := manager.LastAcked()
	require.NotNil(t, acked)
	assert.Equal(t, int64(151), messageIDInt(acked.MessageID))
	assert.Equal(t, uint64(150), acked.TimeTick)
}

// TestObserveNonDeletePinnedByPending verifies a DDL message observed while a
// delete record is staged does not advance the frontier past the record.
func TestObserveNonDeletePinnedByPending(t *testing.T) {
	manager, _ := newTestManagerWithStore(t)
	ctx := context.Background()
	require.NoError(t, manager.Restore(ctx, nil))

	var unused bool
	observeDelete(t, manager, "v1", 100, &unused)
	msg := newTestBarrierMessage(t, "v2", 150)
	owner := message.NewOwnedImmutableMessage(msg, func() {})
	retained := owner.Clone()
	manager.ObserveMessage(ctx, retained)
	retained.Release()
	owner.Release()

	acked := manager.LastAcked()
	require.NotNil(t, acked)
	assert.Equal(t, int64(100), messageIDInt(acked.MessageID), "frontier stays pinned while a record is staged")
}

// TestInitLastAckedSeedsFromCheckpoint verifies the restored checkpoint seeds
// the frontier, and that observation never regresses it.
func TestInitLastAckedSeedsFromCheckpoint(t *testing.T) {
	manager, _ := newTestManagerWithStore(t)
	ctx := context.Background()
	require.NoError(t, manager.Restore(ctx, nil))

	cp := &utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(500),
		TimeTick:  499,
		Magic:     utility.RecoveryMagicRecoveryStorageV2,
	}
	manager.InitLastAcked(cp)
	acked := manager.LastAcked()
	require.NotNil(t, acked)
	assert.Equal(t, int64(500), messageIDInt(acked.MessageID))

	// A barrier observed behind the seeded frontier does not regress it.
	msg := newTestBarrierMessage(t, "v1", 300)
	owner := message.NewOwnedImmutableMessage(msg, func() {})
	retained := owner.Clone()
	manager.ObserveMessage(ctx, retained)
	retained.Release()
	owner.Release()
	acked = manager.LastAcked()
	require.NotNil(t, acked)
	assert.Equal(t, int64(500), messageIDInt(acked.MessageID), "frontier never regresses")
}
