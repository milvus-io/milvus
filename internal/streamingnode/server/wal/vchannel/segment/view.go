package segment

import (
	"context"
	"sort"
	"sync"

	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

func newSegmentViewFromMeta(meta *streamingpb.SegmentAssignmentMeta, schema *schemapb.CollectionSchema, configs ...runtimeConfig) *SegmentView {
	return newSegmentView(
		meta,
		meta.GetCheckpointTimeTick(),
		false,
		writeOnlyInsertBuffer{},
		schema,
		firstRuntimeConfig(configs),
	)
}

func NewSegmentViewFromMetaWithConfig(meta *streamingpb.SegmentAssignmentMeta, schema *schemapb.CollectionSchema, config ViewConfig) *SegmentView {
	return newSegmentViewFromMeta(meta, schema, runtimeConfigFromViewConfig(config))
}

func newSegmentView(
	meta *streamingpb.SegmentAssignmentMeta,
	persistedCheckpointTimeTick uint64,
	dirty bool,
	pending writeOnlyInsertBuffer,
	schema *schemapb.CollectionSchema,
	config runtimeConfig,
) *SegmentView {
	flushPolicy := config.flushPolicy
	if flushPolicy == nil {
		flushPolicy = newDefaultWriteOnlyFlushPolicy()
	}
	return &SegmentView{
		meta:                        proto.Clone(meta).(*streamingpb.SegmentAssignmentMeta),
		durableMeta:                 proto.Clone(meta).(*streamingpb.SegmentAssignmentMeta),
		persistedCheckpointTimeTick: persistedCheckpointTimeTick,
		dirty:                       dirty,
		lifecycle:                   config.lifecycle,
		packWriter:                  config.packWriter,
		runtime:                     config.runtime,
		pending:                     pending,
		flushPolicy:                 flushPolicy,
		schema:                      schema,
		finalCommitDone:             *atomic.NewBool(finalCommitDoneFromMeta(meta)),
		createSegmentTimeTick:       meta.GetStat().GetCreateSegmentTimeTick(),
		segmentID:                   meta.GetSegmentId(),
		vchannel:                    meta.GetVchannel(),
		owner:                       config.owner,
	}
}

func finalCommitDoneFromMeta(meta *streamingpb.SegmentAssignmentMeta) bool {
	return meta.GetL1CommitDone()
}

func shouldRetryRecoveredFinalCommit(meta *streamingpb.SegmentAssignmentMeta) bool {
	if finalCommitDoneFromMeta(meta) {
		return false
	}
	switch meta.GetState() {
	case streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
		streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED:
		return meta.GetCheckpointTimeTick() > 0
	default:
		return false
	}
}

func newSegmentViewFromCreateSegmentMessage(msg message.ImmutableCreateSegmentMessageV2, schema *schemapb.CollectionSchema, configs ...runtimeConfig) *SegmentView {
	return newSegmentView(
		newSegmentAssignmentMetaFromCreateSegmentMessage(msg),
		0,
		false,
		writeOnlyInsertBuffer{},
		schema,
		firstRuntimeConfig(configs),
	)
}

func NewSegmentViewFromCreateSegmentMessageWithConfig(msg message.ImmutableCreateSegmentMessageV2, schema *schemapb.CollectionSchema, config ViewConfig) *SegmentView {
	return newSegmentViewFromCreateSegmentMessage(msg, schema, runtimeConfigFromViewConfig(config))
}

func newSegmentAssignmentMetaFromCreateSegmentMessage(msg message.ImmutableCreateSegmentMessageV2) *streamingpb.SegmentAssignmentMeta {
	header := msg.Header()
	now := tsoutil.PhysicalTime(msg.TimeTick()).Unix()
	return &streamingpb.SegmentAssignmentMeta{
		CollectionId:       header.CollectionId,
		PartitionId:        header.PartitionId,
		SegmentId:          header.SegmentId,
		Vchannel:           msg.VChannel(),
		State:              streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
		StorageVersion:     header.StorageVersion,
		SchemaVersion:      header.SchemaVersion,
		CheckpointTimeTick: 0,
		PersistedStorage:   &streamingpb.L1SegmentPersistedStorage{},
		Stat: &streamingpb.SegmentAssignmentStat{
			MaxRows:               header.MaxRows,
			MaxBinarySize:         header.MaxSegmentSize,
			ModifiedRows:          0,
			ModifiedBinarySize:    0,
			CreateTimestamp:       now,
			LastModifiedTimestamp: now,
			BinlogCounter:         0,
			CreateSegmentTimeTick: msg.TimeTick(),
			Level:                 header.Level,
		},
	}
}

// SegmentView tracks the metadata and durability state of a growing segment.
type SegmentView struct {
	mu sync.Mutex

	// meta is the in-memory segment recovery state. It is updated synchronously
	// by WAL observe and is used to recover the live write path. Its checkpoint
	// is the observation watermark: the largest message timetick already
	// delivered by the WAL. It starts at the durable checkpoint (the recovery
	// anchor restored from the catalog) and only advances as messages are
	// observed, so after a crash observation resumes at the last persisted data
	// point and any re-delivered message at or below it is skipped.
	meta *streamingpb.SegmentAssignmentMeta
	// durableMeta contains only effects whose object/lifecycle work has
	// completed. It is the sole source of catalog snapshots and its checkpoint
	// (the largest timetick whose data is durably flushed) is therefore the
	// recovery anchor: data at or below it is guaranteed to be in object
	// storage, and it never advances past the observation watermark.
	durableMeta *streamingpb.SegmentAssignmentMeta
	// persistedCheckpointTimeTick is the latest durable snapshot checkpoint
	// already stored in the recovery catalog.
	persistedCheckpointTimeTick uint64
	// dirty means durableMeta contains changes not yet persisted into the catalog.
	dirty bool
	// pendingDirtySnapshot is the stable in-flight catalog view returned by
	// ConsumeDirtyAndGetSnapshot and cleared by MarkSnapshotPersisted.
	pendingDirtySnapshot *streamingpb.SegmentAssignmentMeta

	// lifecycle commits data-side segment state to the coordinator after object
	// storage output is ready.
	lifecycle  Lifecycle
	packWriter PackWriter        // writes pending insert data to object storage.
	runtime    moduleapi.Runtime // schedules segment-owned data tasks.
	// pendingTasks is the segment's task queue. The head is the task currently
	// submitted to the scheduler; later entries wait until the head finishes
	// (see maybeSubmitNextLocked/finishTask), so tasks run strictly one at a
	// time in creation order without predecessor bookkeeping.
	pendingTasks []segmentTask
	// pendingFinalCommit keeps repeated flush messages from enqueueing another
	// final commit while the current one is pending or retrying.
	pendingFinalCommit segmentTask
	// unrecoverableError records the segment's terminal unrecoverable task
	// error. Once set, every later task fails fast with the same error instead
	// of executing (see execute). Stored as a pointer to keep the atomic free
	// of the different-concrete-type panic that atomic.Value would raise on a
	// second Store.
	unrecoverableError atomic.Pointer[error]
	// finalCommitDone is process-local task state. Recovery restores it from the
	// persisted L1 commit marker; object durability alone does not prove that the
	// coordinator accepted the final commit. It is published atomically so the
	// vchannel module can scan views without taking the per-view lock on the WAL
	// observation hot path.
	finalCommitDone atomic.Bool
	// createSegmentTimeTick mirrors meta.Stat.CreateSegmentTimeTick and is
	// immutable after construction; the module reads it lock-free when
	// recomputing the L1 materialization bound.
	createSegmentTimeTick uint64
	// segmentID and vchannel mirror meta and are immutable after construction;
	// log paths read them lock-free (meta is otherwise mutated under the lock).
	segmentID int64
	vchannel  string
	pending   writeOnlyInsertBuffer // in-memory insert buffer not yet written as L1.
	// pendingFlushChunks keeps chunks already handed to pending/running flush tasks,
	// ordered by toTimeTick. Chunks stay here until the segment checkpoint
	// advances over them.
	pendingFlushChunks []writeOnlyInsertBuffer
	pendingDataHandles []pendingDataHandle
	flushPolicy        flushPolicy                // decides when pending insert data should be flushed.
	schema             *schemapb.CollectionSchema // schema used to encode pending insert data.
	owner              ViewOwner
}

func (s *SegmentView) ID() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.meta.GetSegmentId()
}

func (s *SegmentView) ObserveCreateSegmentMessageV2(
	_ context.Context,
	owned message.RetainedImmutableCreateSegmentMessageV2,
) bool {
	msg := owned.Message()
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.unrecoverableErr() != nil {
		// Terminal segment: the create observation can never be persisted. Do
		// not drop it silently — poison it so a consumer can handle it
		// separately — and do not retain it. Return false: no state change.
		owned.IntoPoisoned()
		return false
	}
	timetick := msg.TimeTick()
	if !s.shouldObserveCreateSegmentLocked(timetick) {
		return false
	}
	s.meta.CheckpointTimeTick = timetick
	s.retainDataHandleLocked(timetick, owned.CloneHandle())
	s.newEnsureGrowingSegmentTaskLocked(timetick)
	s.maybeSubmitNextLocked()
	return true
}

// shouldObserveLocked is the durable-checkpoint watermark: it reports whether
// data up to timetick is already persisted to object storage. Unlike the
// observation watermark on meta, it only advances when a flush commit lands,
// so it is the correct test for flush/commit decisions (a flush point usually
// equals the largest observed insert timetick, which the observation watermark
// would misjudge as already handled).
func (s *SegmentView) shouldObserveLocked(timetick uint64) bool {
	return timetick > s.durableMeta.GetCheckpointTimeTick()
}

// shouldObserveCreateSegmentLocked filters a create segment message for an
// already-tracked view: the message must be a new observation (the WAL
// delivers in timetick order, so anything at or below the observation
// watermark is a duplicate delivery) and the segment must still be live (a
// tombstoned segment no longer accepts any create observation). An
// unrecoverable segment does not reject here: the entry points poison the
// incoming message instead (see ObserveCreateSegmentMessageV2), so this
// predicate stays a pure lifecycle/watermark test.
func (s *SegmentView) shouldObserveCreateSegmentLocked(timetick uint64) bool {
	return timetick > s.meta.GetCheckpointTimeTick() &&
		s.meta.GetState() != streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED
}

// shouldObserveInsertLocked filters an insert batch for the pending buffer:
// the message must be a new observation (the WAL delivers in timetick order, so
// anything at or below the observation watermark is a duplicate delivery) and
// the segment must still be GROWING — a FLUSHED segment's data is already
// covered by its L1 commit and no flush task will ever run for it again, so
// accepting anything would strand it in the buffer forever or release it
// without persisting it. An unrecoverable segment does not reject here: the
// entry points poison the incoming message instead (see ObserveInsert), so
// this predicate stays a pure lifecycle/watermark test.
func (s *SegmentView) shouldObserveInsertLocked(timetick uint64) bool {
	return timetick > s.meta.GetCheckpointTimeTick() &&
		s.meta.GetState() == streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING
}

func (s *SegmentView) ObserveInsert(
	_ context.Context,
	owned message.RetainedImmutableMessage,
	batch InsertBatch,
) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.unrecoverableErr() != nil {
		// Terminal segment: its data can never be persisted. Do not drop the
		// message silently — poison it so a consumer can enumerate and handle
		// it separately — and do not buffer it. Return false: no state change,
		// so the caller needs no extra work beyond observing the poison.
		owned.IntoPoisoned()
		return false
	}
	if len(batch.assignments) == 0 || !s.shouldObserveInsertLocked(batch.timeTick) {
		return false
	}
	s.meta.CheckpointTimeTick = batch.timeTick
	for _, assignment := range batch.assignments {
		s.observeInsertMetaLocked(batch.timeTick, assignment)
	}
	s.pending.appendMessage(owned.Clone(), batch.rows, batch.binarySize)
	if s.flushPolicy != nil && s.flushPolicy.ShouldFlush(s.pending, batch.timeTick) {
		s.newFlushL1BufferTaskLocked()
		s.maybeSubmitNextLocked()
	}
	return true
}

func (s *SegmentView) observeInsertMetaLocked(timetick uint64, assignment *messagespb.PartitionSegmentAssignment) {
	s.ensureStat()
	s.meta.Stat.ModifiedBinarySize += assignment.GetBinarySize()
	s.meta.Stat.ModifiedRows += assignment.GetRows()
	s.meta.Stat.LastModifiedTimestamp = tsoutil.PhysicalTime(timetick).Unix()
}

func (s *SegmentView) Flush(
	_ context.Context,
	owned message.RetainedImmutableMessage,
) bool {
	msg := owned.Message()
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.unrecoverableErr() != nil {
		// Terminal segment: nothing can be persisted, so do not observe the
		// flush or retain anything — the commit task it would enqueue fails
		// fast and the retained handle would never be released. Poison the
		// incoming flush message instead of dropping it silently, so a
		// consumer can handle it separately.
		owned.IntoPoisoned()
		return false
	}
	timetick := msg.TimeTick()
	closed, flushTimeTick, metaChanged := s.observeFlushMeta(timetick)
	if !closed || !s.shouldObserveLocked(flushTimeTick) {
		return metaChanged
	}
	// The flush message is observed like any other message: advance the
	// observation watermark. This never skips a future insert (the WAL
	// delivers in timetick order, so every insert at or below the flush point
	// has already been observed), and the durable checkpoint itself is only
	// advanced later when the L1 commit actually lands (see
	// markCheckpointDurableLocked).
	s.meta.CheckpointTimeTick = flushTimeTick
	if s.finalCommitDone.Load() {
		return metaChanged
	}
	s.retainDataHandleLocked(flushTimeTick, owned.Clone())
	task := s.newCommitL1SegmentTaskLocked(flushTimeTick)
	if task != nil {
		s.maybeSubmitNextLocked()
	}
	return metaChanged || task != nil
}

func (s *SegmentView) FlushInsertChunk(ctx context.Context, targetTimeTick uint64) error {
	if targetTimeTick == 0 {
		return nil
	}
	s.mu.Lock()
	if err := s.unrecoverableErr(); err != nil {
		// Terminal segment: its data can never be committed to DataCoord, so
		// writing another chunk to object storage would orphan binlogs with no
		// L1 commit to register them, and markCheckpointDurableLocked would
		// release retained handles and let the WAL truncate past data that must
		// stay recoverable. Reject instead (see also Flush and
		// RequestPersistThrough).
		s.mu.Unlock()
		return err
	}
	pack := s.flushPackForTimeTickLocked(targetTimeTick)
	s.mu.Unlock()
	if pack == nil {
		return nil
	}
	result, err := s.packWriter.FlushInsertBuffer(ctx, pack)
	if err != nil {
		return err
	}
	if result == nil || result.PersistedStorage == nil {
		return retry.Unrecoverable(merr.WrapErrServiceInternalMsg("growing segment pack writer returned empty persisted storage"))
	}

	s.mu.Lock()
	chunk := s.pendingFlushChunkLocked(targetTimeTick)
	if chunk == nil {
		s.mu.Unlock()
		return retry.Unrecoverable(merr.WrapErrServiceInternalMsg("growing segment flush chunk disappeared at timetick %d", targetTimeTick))
	}
	appendPersistedStorage(s.meta, result.PersistedStorage)
	appendPersistedStorage(s.durableMeta, result.PersistedStorage)
	applyInsertStat(s.durableMeta, *chunk)
	handles := s.markPendingDataDurableLocked(targetTimeTick)
	s.mu.Unlock()
	s.NotifyDataUpdated()
	releaseMessages(handles)
	return nil
}

// RequestPersistThrough schedules persistence for buffered inserts whose
// TimeTick is not greater than targetTimeTick. The whole current buffer may be
// persisted, so the scheduled task can cover a later TimeTick as well.
func (s *SegmentView) RequestPersistThrough(targetTimeTick uint64) bool {
	s.mu.Lock()
	if s.unrecoverableErr() != nil {
		s.mu.Unlock()
		return false
	}
	if len(s.pending.entries) == 0 || s.pending.fromTimeTick > targetTimeTick {
		s.mu.Unlock()
		return false
	}
	s.newFlushL1BufferTaskLocked()
	s.maybeSubmitNextLocked()
	s.mu.Unlock()
	return true
}

func (info *SegmentView) AssignmentMeta() *streamingpb.SegmentAssignmentMeta {
	info.mu.Lock()
	defer info.mu.Unlock()
	return proto.Clone(info.meta).(*streamingpb.SegmentAssignmentMeta)
}

func (info *SegmentView) WritePathRecoveryState() (moduleapi.SegmentWritePathRecoveryState, bool) {
	info.mu.Lock()
	defer info.mu.Unlock()
	// Pure lifecycle-state predicate: only the segment assignment state machine
	// decides whether this segment is a growing target. The unrecoverable
	// health marker never touches the state machine — it only fast-fails the
	// persistence tasks (see task execution and the observation gates).
	if info.meta.GetState() != streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING {
		return moduleapi.SegmentWritePathRecoveryState{}, false
	}
	state := moduleapi.SegmentWritePathRecoveryState{
		VChannel:     info.meta.GetVchannel(),
		CollectionID: info.meta.GetCollectionId(),
		PartitionID:  info.meta.GetPartitionId(),
		SegmentID:    info.meta.GetSegmentId(),
	}
	if info.meta.GetStat() != nil {
		state.Stat = proto.Clone(info.meta.GetStat()).(*streamingpb.SegmentAssignmentStat)
	}
	return state, true
}

func (info *SegmentView) IDAndVChannel() (int64, string) {
	info.mu.Lock()
	defer info.mu.Unlock()
	return info.meta.GetSegmentId(), info.meta.GetVchannel()
}

func (info *SegmentView) CollectionID() int64 {
	info.mu.Lock()
	defer info.mu.Unlock()
	return info.meta.GetCollectionId()
}

func (info *SegmentView) VChannel() string {
	info.mu.Lock()
	defer info.mu.Unlock()
	return info.meta.GetVchannel()
}

func (info *SegmentView) PartitionID() int64 {
	info.mu.Lock()
	defer info.mu.Unlock()
	return info.meta.GetPartitionId()
}

func (info *SegmentView) CreateTimeTick() uint64 {
	info.mu.Lock()
	defer info.mu.Unlock()
	return info.meta.GetStat().GetCreateSegmentTimeTick()
}

// L1MaterializationBlockerTimeTick reports the inclusive TransformLog
// materialization frontier imposed by an L1 segment whose final commit has not
// completed yet. Lock-free by design: finalCommitDone is published atomically
// and createSegmentTimeTick is immutable, so the vchannel module may scan every
// view on the WAL observation hot path without acquiring the per-view lock.
// The level is not re-checked here because the vchannel recovery module only
// tracks L1 segments.
func (info *SegmentView) L1MaterializationBlockerTimeTick() (uint64, bool) {
	if info.finalCommitDone.Load() {
		return 0, false
	}
	return info.createSegmentTimeTick, true
}

func (info *SegmentView) markCheckpointPersistedLocked(timetick uint64) {
	if timetick > info.persistedCheckpointTimeTick {
		info.persistedCheckpointTimeTick = timetick
	}
}

func (info *SegmentView) MarkSnapshotPersisted(snapshot *streamingpb.SegmentAssignmentMeta) {
	info.mu.Lock()
	defer info.mu.Unlock()
	info.markCheckpointPersistedLocked(snapshot.GetCheckpointTimeTick())
	if info.pendingDirtySnapshot != nil && proto.Equal(info.pendingDirtySnapshot, snapshot) {
		info.pendingDirtySnapshot = nil
	}
	info.dirty = !proto.Equal(info.durableMeta, snapshot)
}

// NotifyDataUpdated reports data changes to the owning module. Must not be
// called with the view's lock held: the module takes its own lock and may
// re-enter the view.
func (info *SegmentView) NotifyDataUpdated() {
	info.owner.SegmentDataUpdated(info.ID(), info)
}

func (info *SegmentView) markCheckpointDurableLocked(timetick uint64) {
	if timetick <= info.durableMeta.GetCheckpointTimeTick() {
		return
	}
	// Only the durable checkpoint advances here. The observation watermark on
	// meta already covers timetick (the flushed data was observed first), so
	// it needs no write.
	info.durableMeta.CheckpointTimeTick = timetick
	info.dirty = true
	info.prunePendingFlushChunksLocked()
}

func (info *SegmentView) TryFinalizeTombstone() bool {
	info.mu.Lock()
	defer info.mu.Unlock()
	return info.maybeMarkTombstonedLocked()
}

// EnsureFinalCommit reports whether a flushed segment has completed its durable
// DataCoord commit. Otherwise it schedules or reuses the segment final task.
//
// finalCommitDone is the authoritative commit fact and is checked first: a
// segment whose L1 commit has landed is durably committed regardless of any
// later task failure, so a terminal error must not invert the answer for an
// already-committed segment. A terminal segment that has not committed can
// never complete its commit, so it returns false — the same value as "not
// committed yet" — rather than true ("durably committed, safe to forget").
// Returning true for a segment that never committed would tell the caller it
// may drop the segment, abandoning uncommitted data that exists only in the
// WAL (its in-memory handles were already poisoned and released by
// markUnrecoverable). No task is scheduled for a terminal segment because
// canScheduleFinalCommitLocked is permanently false once unrecoverable, so
// returning false does not spin anything.
func (info *SegmentView) EnsureFinalCommit() bool {
	info.mu.Lock()
	if info.finalCommitDone.Load() {
		info.mu.Unlock()
		return true
	}
	if info.unrecoverableErr() != nil {
		info.mu.Unlock()
		return false
	}
	if info.meta.GetState() != streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED {
		info.mu.Unlock()
		return true
	}
	task := info.newCommitL1SegmentTaskLocked(info.meta.GetCheckpointTimeTick())
	if task != nil {
		info.maybeSubmitNextLocked()
	}
	info.mu.Unlock()
	return false
}

// ResumePendingRecovery retries legacy recovered final-commit work that was
// durable before restart but had not reached the coordinator.
func (info *SegmentView) ResumePendingRecovery() {
	info.mu.Lock()
	if info.unrecoverableErr() != nil {
		// Terminal segment: its commit can never reach the coordinator, so
		// there is nothing to resume — an enqueued recovered commit task would
		// only fail fast (see FlushInsertChunk for the same gate).
		info.mu.Unlock()
		return
	}
	if shouldRetryRecoveredFinalCommit(info.meta) {
		if task := info.newRecoveredCommitL1SegmentTaskLocked(info.meta.GetCheckpointTimeTick()); task != nil {
			info.maybeSubmitNextLocked()
		}
	}
	info.mu.Unlock()
}

func (info *SegmentView) IsGrowing() bool {
	info.mu.Lock()
	defer info.mu.Unlock()
	// Pure lifecycle-state predicate: whether the segment is in the GROWING
	// state of its assignment state machine. The unrecoverable health marker
	// does not participate — it only fast-fails persistence tasks, it never
	// changes what state the segment reports.
	return info.meta.State == streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING
}

// PersistedCheckpointTimeTick returns the checkpoint already stored in the
// recovery catalog (the largest timetick whose insert data was durably
// flushed and whose segment meta was persisted). It is the conservative bound
// a crash-recovery would observe; vchannel-level flush checkpoints reported
// to DataCoord must not advance past it.
func (info *SegmentView) PersistedCheckpointTimeTick() uint64 {
	info.mu.Lock()
	defer info.mu.Unlock()
	return info.persistedCheckpointTimeTick
}

func (info *SegmentView) TombstonePersisted() bool {
	info.mu.Lock()
	defer info.mu.Unlock()
	checkpointTimeTick := info.meta.GetCheckpointTimeTick()
	return info.meta.GetState() == streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED &&
		checkpointTimeTick > 0 &&
		!info.dirty &&
		info.persistedCheckpointTimeTick >= checkpointTimeTick
}

func (info *SegmentView) TombstonedCleanupReady(physicalTimeTick uint64) bool {
	info.mu.Lock()
	defer info.mu.Unlock()
	return info.tombstonedCleanupReadyLocked(physicalTimeTick)
}

func (info *SegmentView) tombstonedCleanupReadyLocked(physicalTimeTick uint64) bool {
	checkpointTimeTick := info.meta.GetCheckpointTimeTick()
	return info.meta.GetState() == streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED &&
		checkpointTimeTick > 0 &&
		!info.dirty &&
		info.persistedCheckpointTimeTick >= checkpointTimeTick &&
		physicalTimeTick > checkpointTimeTick
}

func (info *SegmentView) observeFlushMeta(timetick uint64) (bool, uint64, bool) {
	if info.meta.State == streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED {
		return false, info.meta.GetCheckpointTimeTick(), false
	}
	if timetick <= info.durableMeta.GetCheckpointTimeTick() {
		return info.meta.State == streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
			info.durableMeta.GetCheckpointTimeTick(), false
	}
	if info.meta.State == streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED {
		// idempotent
		return true, info.meta.GetCheckpointTimeTick(), false
	}
	info.ensureStat()
	info.meta.State = streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED
	info.meta.Stat.LastModifiedTimestamp = tsoutil.PhysicalTime(timetick).Unix()
	return true, timetick, true
}

func (s *SegmentView) markPendingDataDurableLocked(timetick uint64) []message.RetainedImmutableMessage {
	if timetick <= s.durableMeta.GetCheckpointTimeTick() {
		return nil
	}
	completed := s.takeDataHandlesThroughLocked(timetick)
	for _, chunk := range s.pendingFlushChunks {
		if chunk.toTimeTick > timetick {
			break
		}
		completed = append(completed, chunk.retainedHandles()...)
	}
	s.markCheckpointDurableLocked(timetick)
	return completed
}

func (s *SegmentView) retainDataHandleLocked(
	timetick uint64,
	retained message.RetainedImmutableMessage,
) {
	s.pendingDataHandles = append(s.pendingDataHandles, pendingDataHandle{
		timetick: timetick,
		message:  retained,
	})
}

func (s *SegmentView) takeDataHandlesThroughLocked(timetick uint64) []message.RetainedImmutableMessage {
	completed := make([]message.RetainedImmutableMessage, 0)
	pending := s.pendingDataHandles[:0]
	for _, item := range s.pendingDataHandles {
		if item.timetick <= timetick {
			completed = append(completed, item.message)
			continue
		}
		pending = append(pending, item)
	}
	clear(s.pendingDataHandles[len(pending):])
	s.pendingDataHandles = pending
	return completed
}

// releaseMessages releases retained messages once their data has been flushed
// and marked durable. Releasing is a memory-reclaim signal — it does NOT by
// itself drive WAL truncation. WAL truncation is synchronized with dirty
// persistence: the segment's dirty snapshot (containing the released data's
// binlog paths) is persisted to the recovery catalog first, advancing
// persistedCheckpointTimeTick, and only then does truncation advance. Because
// markCheckpointDurableLocked (which sets dirty=true) always precedes
// releaseMessages, the released data is already in durableMeta and cannot be
// truncated away before it reaches the catalog. The terminal path does not use
// this helper: markUnrecoverable poisons every retained message instead (see
// collectAllPendingLocked), since a terminal segment's data was never durable.
func releaseMessages(messages []message.RetainedImmutableMessage) {
	for _, msg := range messages {
		msg.Release()
	}
}

// collectAllPendingLocked collects every retained handle in the three pending
// structures and clears them. It is the terminal sweep of markUnrecoverable:
// a segment that can never persist again poisons every message it was holding,
// so a consumer can enumerate and handle them separately, rather than dropping
// them (which would make the failure invisible). The handles are returned
// rather than released inline because releasing must happen outside the lock:
// PoisonedRelease() runs the message finalizer (a flush-completed
// memory-reclaim signal in the real scanner wiring, which must not execute
// under s.mu). Must be called with s.mu held.
func (s *SegmentView) collectAllPendingLocked() []message.RetainedImmutableMessage {
	var handles []message.RetainedImmutableMessage
	handles = append(handles, s.pending.entries...)
	for _, item := range s.pendingDataHandles {
		handles = append(handles, item.message)
	}
	for _, chunk := range s.pendingFlushChunks {
		handles = append(handles, chunk.entries...)
	}
	// Clear the structures so the handles are released and no dangling
	// reference remains; a terminal segment accepts no new data and runs no
	// tasks, so nothing reads them afterwards.
	s.pending.reset()
	s.pendingDataHandles = nil
	s.pendingFlushChunks = nil
	return handles
}

type pendingDataHandle struct {
	timetick uint64
	message  message.RetainedImmutableMessage
}

func (s *SegmentView) maybeMarkTombstonedLocked() bool {
	if !s.tombstoneFinalizeReadyLocked() {
		return false
	}
	s.meta.State = streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED
	s.durableMeta.State = streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED
	s.dirty = true
	return true
}

func (s *SegmentView) tombstoneFinalizeReadyLocked() bool {
	return s.meta.GetState() == streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED &&
		s.meta.GetCheckpointTimeTick() > 0 &&
		s.finalCommitDone.Load() &&
		s.durableMeta.GetCheckpointTimeTick() >= s.meta.GetCheckpointTimeTick()
}

func (s *SegmentView) enqueuePendingFlushChunkLocked() uint64 {
	chunk := s.pending.takeAll()
	if len(chunk.entries) == 0 {
		return 0
	}
	s.pendingFlushChunks = append(s.pendingFlushChunks, chunk)
	return chunk.toTimeTick
}

func (s *SegmentView) flushPackForTimeTickLocked(timetick uint64) *flushPack {
	index := s.pendingFlushChunkIndexLocked(timetick)
	if index < 0 {
		return nil
	}
	chunk := &s.pendingFlushChunks[index]
	meta := proto.Clone(s.durableMeta).(*streamingpb.SegmentAssignmentMeta)
	applyInsertStat(meta, *chunk)
	meta.CheckpointTimeTick = chunk.toTimeTick
	return chunk.flushPack(meta, s.schema)
}

func (s *SegmentView) pendingFlushChunkLocked(timetick uint64) *writeOnlyInsertBuffer {
	index := s.pendingFlushChunkIndexLocked(timetick)
	if index < 0 {
		return nil
	}
	return &s.pendingFlushChunks[index]
}

func (s *SegmentView) pendingFlushChunkIndexLocked(timetick uint64) int {
	index := firstPendingFlushChunkAtOrAfter(s.pendingFlushChunks, timetick)
	if index == len(s.pendingFlushChunks) || s.pendingFlushChunks[index].toTimeTick != timetick {
		return -1
	}
	return index
}

func applyInsertStat(meta *streamingpb.SegmentAssignmentMeta, chunk writeOnlyInsertBuffer) {
	if len(chunk.entries) == 0 {
		return
	}
	if meta.Stat == nil {
		meta.Stat = &streamingpb.SegmentAssignmentStat{}
	}
	meta.Stat.ModifiedRows += chunk.rows
	meta.Stat.ModifiedBinarySize += chunk.binarySize
	meta.Stat.LastModifiedTimestamp = tsoutil.PhysicalTime(chunk.toTimeTick).Unix()
}

func (s *SegmentView) prunePendingFlushChunksLocked() {
	checkpoint := s.durableMeta.GetCheckpointTimeTick()
	firstRemaining := firstPendingFlushChunkAfter(s.pendingFlushChunks, checkpoint)
	if firstRemaining == 0 {
		return
	}
	remaining := copy(s.pendingFlushChunks, s.pendingFlushChunks[firstRemaining:])
	clear(s.pendingFlushChunks[remaining:cap(s.pendingFlushChunks)])
	s.pendingFlushChunks = s.pendingFlushChunks[:remaining]
}

func firstPendingFlushChunkAtOrAfter(chunks []writeOnlyInsertBuffer, timetick uint64) int {
	return sort.Search(len(chunks), func(index int) bool {
		return chunks[index].toTimeTick >= timetick
	})
}

func firstPendingFlushChunkAfter(chunks []writeOnlyInsertBuffer, timetick uint64) int {
	return sort.Search(len(chunks), func(index int) bool {
		return chunks[index].toTimeTick > timetick
	})
}

func appendPersistedStorage(meta *streamingpb.SegmentAssignmentMeta, storage *streamingpb.L1SegmentPersistedStorage) {
	if meta.PersistedStorage == nil {
		meta.PersistedStorage = &streamingpb.L1SegmentPersistedStorage{}
	}
	if storage.GetManifestPath() != "" {
		meta.PersistedStorage.ManifestPath = storage.GetManifestPath()
	}
	meta.PersistedStorage.Binlogs = append(
		meta.PersistedStorage.Binlogs,
		cloneL1SegmentBinLogs(storage.GetBinlogs())...,
	)
	if storage.GetMergedStatsBinlog() != nil {
		meta.PersistedStorage.MergedStatsBinlog = cloneFieldBinlog(storage.GetMergedStatsBinlog())
	}
	if storage.GetStatistics() != nil {
		meta.PersistedStorage.Statistics = proto.Clone(storage.GetStatistics()).(*datapb.Statistics)
	}
	if len(storage.GetDeltaBinlog()) > 0 {
		meta.PersistedStorage.DeltaBinlog = append(
			meta.PersistedStorage.DeltaBinlog,
			cloneFieldBinlogs(storage.GetDeltaBinlog())...,
		)
	}
}

func cloneFieldBinlogs(values []*datapb.FieldBinlog) []*datapb.FieldBinlog {
	if len(values) == 0 {
		return nil
	}
	cloned := make([]*datapb.FieldBinlog, 0, len(values))
	for _, value := range values {
		cloned = append(cloned, cloneFieldBinlog(value))
	}
	return cloned
}

func (info *SegmentView) ensureStat() {
	if info.meta.Stat == nil {
		info.meta.Stat = &streamingpb.SegmentAssignmentStat{}
	}
}

func (info *SegmentView) ConsumeDirtyAndGetSnapshot() *streamingpb.SegmentAssignmentMeta {
	info.mu.Lock()
	defer info.mu.Unlock()
	if info.pendingDirtySnapshot != nil {
		return proto.Clone(info.pendingDirtySnapshot).(*streamingpb.SegmentAssignmentMeta)
	}
	if !info.dirty {
		return nil
	}
	info.pendingDirtySnapshot = proto.Clone(info.durableMeta).(*streamingpb.SegmentAssignmentMeta)
	return proto.Clone(info.pendingDirtySnapshot).(*streamingpb.SegmentAssignmentMeta)
}
