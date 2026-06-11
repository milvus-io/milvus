package segment

import (
	"context"
	"math"
	"sync"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	walcheckpoint "github.com/milvus-io/milvus/internal/streamingnode/server/wal/checkpoint"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	scheduler "github.com/milvus-io/milvus/pkg/v3/syncutil/preconditioned"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

func newSegmentViewFromMeta(meta *streamingpb.SegmentAssignmentMeta, schema *schemapb.CollectionSchema, configs ...runtimeConfig) *segmentView {
	return newSegmentView(
		meta,
		meta.GetCheckpointTimeTick(),
		meta.GetDataCheckpointTimeTick(),
		false,
		writeOnlyInsertBuffer{},
		schema,
		firstRuntimeConfig(configs),
	)
}

func newSegmentView(
	meta *streamingpb.SegmentAssignmentMeta,
	persistedMetaTimeTick uint64,
	persistedDataTimeTick uint64,
	dirty bool,
	pending writeOnlyInsertBuffer,
	schema *schemapb.CollectionSchema,
	config runtimeConfig,
) *segmentView {
	flushPolicy := config.flushPolicy
	if flushPolicy == nil {
		flushPolicy = newDefaultWriteOnlyFlushPolicy()
	}
	return &segmentView{
		meta:                  meta,
		persistedMetaTimeTick: persistedMetaTimeTick,
		persistedDataTimeTick: persistedDataTimeTick,
		dirty:                 dirty,
		lifecycle:             config.lifecycle,
		packWriter:            config.packWriter,
		runtime:               config.runtime,
		pending:               pending,
		flushPolicy:           flushPolicy,
		onDataUpdated:         config.onDataUpdated,
		schema:                schema,
		metaAndData:           config.metaAndData,
	}
}

func newSegmentViewFromCreateSegmentMessage(msg message.ImmutableCreateSegmentMessageV2, schema *schemapb.CollectionSchema, configs ...runtimeConfig) *segmentView {
	return newSegmentView(
		newSegmentAssignmentMetaFromCreateSegmentMessage(msg),
		0,
		0,
		true,
		writeOnlyInsertBuffer{},
		schema,
		firstRuntimeConfig(configs),
	)
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
		CheckpointTimeTick: msg.TimeTick(),
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

// segmentView tracks the metadata and durability state of a growing segment.
type segmentView struct {
	mu sync.Mutex

	// meta is the in-memory segment recovery state. It is updated synchronously by
	// WAL observe and later persisted as SegmentAssignmentMeta.
	meta *streamingpb.SegmentAssignmentMeta
	// persistedMetaTimeTick is the latest meta/stat timetick already persisted to
	// the recovery catalog. It backs the segment meta barrier.
	persistedMetaTimeTick uint64
	// persistedDataTimeTick is the latest data durability timetick already
	// persisted into the recovery catalog. It backs the segment data barrier.
	persistedDataTimeTick uint64
	// dirty means current meta contains changes not yet persisted into the catalog.
	dirty bool
	// pendingDirtySnapshot is the stable in-flight catalog view returned by
	// ConsumeDirtyAndGetSnapshot and cleared by MarkSnapshotPersisted.
	pendingDirtySnapshot *streamingpb.SegmentAssignmentMeta

	// lifecycle commits data-side segment state to the coordinator after object
	// storage output is ready.
	lifecycle    segmentLifecycle
	packWriter   packWriter             // writes pending insert data to object storage.
	runtime      moduleapi.Runtime      // schedules segment-owned data tasks.
	pendingTasks []scheduler.TaskHandle // unfinished segment tasks used as preconditions.
	pending      writeOnlyInsertBuffer  // in-memory insert buffer not yet written as L1.
	// pendingFlushChunks keeps chunks already handed to pending/running flush tasks.
	// Chunks stay here until segment data checkpoint advances over them.
	pendingFlushChunks []writeOnlyInsertBuffer
	flushPolicy        flushPolicy                // decides when pending insert data should be flushed.
	onDataUpdated      func()                     // notifies checkpoint manager when data barrier may advance.
	schema             *schemapb.CollectionSchema // schema used to encode pending insert data.
	metaAndData        bool                       // false during meta-only replay; true when data tasks may run.
}

func (s *segmentView) ID() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.meta.GetSegmentId()
}

func (s *segmentView) HasDirty() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dirty
}

func (s *segmentView) ObserveCreateSegmentMessageV2(_ context.Context, msg message.ImmutableCreateSegmentMessageV2) moduleapi.ObserveResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	result := moduleapi.ObserveResult{}
	if s.shouldSkipReplayLocked(msg.TimeTick()) {
		return result
	}
	if !s.metaAndData {
		return result
	}
	timetick := msg.TimeTick()
	if timetick <= s.meta.GetDataCheckpointTimeTick() {
		return result
	}
	task := s.newEnsureGrowingSegmentTaskLocked(timetick)
	result.Data = s.dataBarrier()
	s.runtime.Scheduler.Submit(task)
	return result
}

func (s *segmentView) ObserveInsertMessageV1(
	_ context.Context,
	msg message.ImmutableInsertMessageV1,
	assignment *messagespb.PartitionSegmentAssignment,
) moduleapi.ObserveResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	result := moduleapi.ObserveResult{}
	if !s.canReplayInsertLocked(msg.TimeTick()) {
		return result
	}
	if msg.TimeTick() > s.meta.CheckpointTimeTick {
		s.observeInsertMetaLocked(msg.TimeTick(), assignment)
		result.Meta = s.metaBarrier()
	}
	if !s.metaAndData {
		return result
	}
	if msg.TimeTick() <= s.meta.GetDataCheckpointTimeTick() {
		return result
	}
	if msg.TimeTick() <= s.pending.DataTimeTick() {
		return result
	}
	s.pending.append(msg, assignment)
	if s.flushPolicy != nil && s.flushPolicy.ShouldFlush(s.pending, msg.TimeTick()) {
		task := s.newFlushL1BufferTaskLocked()
		s.runtime.Scheduler.Submit(task)
	}
	result.Data = s.dataBarrier()
	return result
}

func (s *segmentView) ObserveTxnMessage(_ context.Context, msg message.ImmutableTxnMessage) moduleapi.ObserveResult {
	var task scheduler.Task
	matched := false
	appliedData := false
	timetick := msg.TimeTick()
	s.mu.Lock()
	defer s.mu.Unlock()
	result := moduleapi.ObserveResult{}
	if s.shouldSkipReplayLocked(timetick) {
		return result
	}
	if !s.canReplayInsertLocked(timetick) {
		return result
	}

	metaTimeTick := s.meta.CheckpointTimeTick
	pendingDataTimeTick := s.pending.DataTimeTick()
	appliedMeta := false
	msg.RangeOver(func(im message.ImmutableMessage) error {
		if im.MessageType() != message.MessageTypeInsert {
			return nil
		}
		insert := message.MustAsImmutableInsertMessageV1(im)
		for _, assignment := range insert.Header().GetPartitions() {
			if assignment.GetSegmentAssignment().GetSegmentId() != s.meta.GetSegmentId() {
				continue
			}
			matched = true
			if timetick > metaTimeTick {
				s.observeInsertMetaLocked(timetick, assignment)
				appliedMeta = true
			}
			if s.metaAndData &&
				timetick > s.meta.GetDataCheckpointTimeTick() &&
				timetick > pendingDataTimeTick {
				s.pending.appendWithTimeTick(insert, assignment, timetick)
				appliedData = true
			}
		}
		return nil
	})
	if !matched {
		return result
	}
	if appliedMeta {
		result.Meta = s.metaBarrier()
	}
	if appliedData {
		result.Data = s.dataBarrier()
		if s.flushPolicy != nil && s.flushPolicy.ShouldFlush(s.pending, timetick) {
			task = s.newFlushL1BufferTaskLocked()
		}
	}
	if task != nil {
		s.runtime.Scheduler.Submit(task)
	}
	return result
}

func (s *segmentView) observeInsertMetaLocked(timetick uint64, assignment *messagespb.PartitionSegmentAssignment) {
	s.ensureStat()
	s.meta.Stat.ModifiedBinarySize += assignment.GetBinarySize()
	s.meta.Stat.ModifiedRows += assignment.GetRows()
	s.meta.Stat.LastModifiedTimestamp = tsoutil.PhysicalTime(timetick).Unix()
	s.meta.CheckpointTimeTick = timetick
	s.dirty = true
}

func (s *segmentView) Flush(_ context.Context, timetick uint64) moduleapi.ObserveResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	closed, flushTimeTick, metaChanged := s.observeFlushMeta(timetick)
	result := moduleapi.ObserveResult{}
	if metaChanged {
		result.Meta = s.metaBarrier()
	}
	if !s.metaAndData {
		return result
	}
	if !closed || flushTimeTick <= s.meta.GetDataCheckpointTimeTick() {
		return result
	}
	task := s.newCommitL1SegmentTaskLocked(flushTimeTick)
	result.Data = s.dataBarrier()
	s.runtime.Scheduler.Submit(task)
	return result
}

func (s *segmentView) FlushInsertChunk(ctx context.Context, targetTimeTick uint64) error {
	if targetTimeTick == 0 {
		return nil
	}
	s.mu.Lock()
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
		return errors.New("growing segment pack writer returned empty persisted storage")
	}

	s.mu.Lock()
	s.appendPersistedStorage(result.PersistedStorage)
	s.MarkPendingDataDurable(targetTimeTick)
	s.mu.Unlock()
	s.NotifyDataUpdated()
	return nil
}

func (info *segmentView) AssignmentMeta() *streamingpb.SegmentAssignmentMeta {
	info.mu.Lock()
	defer info.mu.Unlock()
	return proto.Clone(info.meta).(*streamingpb.SegmentAssignmentMeta)
}

func (info *segmentView) metaTimeTick() uint64 {
	info.mu.Lock()
	defer info.mu.Unlock()
	return info.persistedMetaTimeTick
}

func (info *segmentView) metaBarrier() walcheckpoint.Barrier {
	return walcheckpoint.BarrierFunc(info.metaTimeTick)
}

func (info *segmentView) dataTimeTick() uint64 {
	info.mu.Lock()
	defer info.mu.Unlock()
	return info.persistedDataTimeTick
}

func (info *segmentView) dataBarrier() walcheckpoint.Barrier {
	return walcheckpoint.BarrierFunc(info.dataTimeTick)
}

func (info *segmentView) DurableFrontierTimeTick() uint64 {
	info.mu.Lock()
	defer info.mu.Unlock()
	if info.meta.GetState() == streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED {
		if info.dirty {
			return frontierBefore(info.meta.GetTombstoneTimeTick())
		}
		return math.MaxUint64
	}
	if !info.hasPendingDataWorkLocked() {
		return math.MaxUint64
	}
	return min(info.persistedMetaTimeTick, info.persistedDataTimeTick)
}

func (info *segmentView) hasPendingDataWorkLocked() bool {
	if info.meta.GetDataCheckpointTimeTick() > info.persistedDataTimeTick {
		return true
	}
	if info.meta.GetState() == streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED &&
		info.meta.GetDataCheckpointTimeTick() < info.meta.GetCheckpointTimeTick() {
		return true
	}
	if len(info.pending.entries) > 0 || len(info.pendingFlushChunks) > 0 {
		return true
	}
	for _, task := range info.pendingTasks {
		if task != nil && !task.Done() {
			return true
		}
	}
	return false
}

func (info *segmentView) CreateTimeTick() uint64 {
	info.mu.Lock()
	defer info.mu.Unlock()
	return info.meta.GetStat().GetCreateSegmentTimeTick()
}

func (info *segmentView) markMetaPersistedLocked(timetick uint64) {
	if timetick > info.persistedMetaTimeTick {
		info.persistedMetaTimeTick = timetick
	}
}

func (info *segmentView) markDataPersistedLocked(timetick uint64) {
	if timetick > info.persistedDataTimeTick {
		info.persistedDataTimeTick = timetick
	}
}

func (info *segmentView) MarkSnapshotPersisted(snapshot *streamingpb.SegmentAssignmentMeta) {
	info.mu.Lock()
	defer info.mu.Unlock()
	info.markMetaPersistedLocked(snapshot.GetCheckpointTimeTick())
	info.markDataPersistedLocked(snapshot.GetDataCheckpointTimeTick())
	if info.pendingDirtySnapshot != nil && proto.Equal(info.pendingDirtySnapshot, snapshot) {
		info.pendingDirtySnapshot = nil
	}
	info.dirty = !proto.Equal(info.meta, snapshot)
}

func (info *segmentView) NotifyDataUpdated() {
	if info.onDataUpdated != nil {
		info.onDataUpdated()
	}
}

func (info *segmentView) markDataCheckpointLocked(timetick uint64) {
	if timetick <= info.meta.GetDataCheckpointTimeTick() {
		return
	}
	info.meta.DataCheckpointTimeTick = timetick
	info.dirty = true
	info.prunePendingFlushChunksLocked()
}

func (info *segmentView) TryFinalizeTombstone() bool {
	info.mu.Lock()
	defer info.mu.Unlock()
	return info.maybeMarkTombstonedLocked()
}

func (info *segmentView) HasReadyTombstoneFinalize() bool {
	info.mu.Lock()
	defer info.mu.Unlock()
	return info.tombstoneFinalizeReadyLocked()
}

func (info *segmentView) SwitchIntoMetaAndData() {
	info.mu.Lock()
	defer info.mu.Unlock()
	info.metaAndData = true
}

func (info *segmentView) SetSchema(schema *schemapb.CollectionSchema) {
	if schema == nil {
		return
	}
	info.mu.Lock()
	defer info.mu.Unlock()
	info.schema = schema
}

func (info *segmentView) IsGrowing() bool {
	info.mu.Lock()
	defer info.mu.Unlock()
	return info.meta.State == streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING
}

func (info *segmentView) canReplayInsertLocked(timetick uint64) bool {
	switch info.meta.GetState() {
	case streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING:
		return true
	case streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED:
		return timetick <= info.meta.GetCheckpointTimeTick()
	default:
		return false
	}
}

func (info *segmentView) shouldSkipReplayLocked(timetick uint64) bool {
	return shouldSkipTombstonedSegmentMeta(info.meta, timetick)
}

func (info *segmentView) TombstonePersisted() bool {
	info.mu.Lock()
	defer info.mu.Unlock()
	tombstoneTimeTick := info.meta.GetTombstoneTimeTick()
	return info.meta.GetState() == streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED &&
		tombstoneTimeTick > 0 &&
		!info.dirty &&
		info.persistedMetaTimeTick >= tombstoneTimeTick &&
		info.persistedDataTimeTick >= tombstoneTimeTick
}

func (info *segmentView) CoveredByTombstone(vchannel string, partitionID int64, timetick uint64) bool {
	info.mu.Lock()
	defer info.mu.Unlock()
	if info.meta.GetVchannel() != vchannel {
		return false
	}
	if partitionID != common.AllPartitionsID && info.meta.GetPartitionId() != partitionID {
		return false
	}
	createTimeTick := info.meta.GetStat().GetCreateSegmentTimeTick()
	return createTimeTick < timetick
}

func (info *segmentView) TombstonedCleanupReady(metaPhysicalTimeTick uint64, dataPhysicalTimeTick uint64) bool {
	info.mu.Lock()
	defer info.mu.Unlock()
	return info.tombstonedCleanupReadyLocked(metaPhysicalTimeTick, dataPhysicalTimeTick)
}

func (info *segmentView) tombstonedCleanupReadyLocked(metaPhysicalTimeTick uint64, dataPhysicalTimeTick uint64) bool {
	tombstoneTimeTick := info.meta.GetTombstoneTimeTick()
	return info.meta.GetState() == streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED &&
		tombstoneTimeTick > 0 &&
		!info.dirty &&
		info.persistedMetaTimeTick >= tombstoneTimeTick &&
		info.persistedDataTimeTick >= tombstoneTimeTick &&
		metaPhysicalTimeTick > tombstoneTimeTick &&
		dataPhysicalTimeTick > tombstoneTimeTick
}

func (info *segmentView) observeFlushMeta(timetick uint64) (bool, uint64, bool) {
	if info.meta.State == streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED {
		return false, info.meta.GetCheckpointTimeTick(), false
	}
	if timetick < info.meta.CheckpointTimeTick {
		// the txn message will share the same time tick.
		// (although the flush operation is not a txn message)
		// so we only filter the time tick is less than the checkpoint time tick.
		// Consistent state is guaranteed by the recovery storage's mutex.
		return info.meta.State == streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED, info.meta.GetCheckpointTimeTick(), false
	}
	if info.meta.State == streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED {
		// idempotent
		return true, info.meta.GetCheckpointTimeTick(), false
	}
	info.ensureStat()
	info.meta.State = streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED
	info.meta.Stat.LastModifiedTimestamp = tsoutil.PhysicalTime(timetick).Unix()
	info.meta.CheckpointTimeTick = timetick
	info.dirty = true
	return true, timetick, true
}

func (s *segmentView) MarkPendingDataDurable(timetick uint64) {
	if timetick <= s.meta.GetDataCheckpointTimeTick() {
		return
	}
	s.markDataCheckpointLocked(timetick)
}

func (s *segmentView) maybeMarkTombstonedLocked() bool {
	if !s.tombstoneFinalizeReadyLocked() {
		return false
	}
	tombstoneTimeTick := s.meta.GetCheckpointTimeTick()
	s.meta.State = streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED
	s.meta.TombstoneTimeTick = tombstoneTimeTick
	s.dirty = true
	return true
}

func (s *segmentView) tombstoneFinalizeReadyLocked() bool {
	tombstoneTimeTick := s.meta.GetCheckpointTimeTick()
	return s.meta.GetState() == streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED &&
		tombstoneTimeTick > 0 &&
		s.meta.GetDataCheckpointTimeTick() >= tombstoneTimeTick
}

func shouldSkipTombstonedSegmentMeta(meta *streamingpb.SegmentAssignmentMeta, timetick uint64) bool {
	return meta != nil &&
		meta.GetState() == streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED &&
		meta.GetTombstoneTimeTick() > 0 &&
		timetick <= meta.GetTombstoneTimeTick()
}

func (s *segmentView) enqueuePendingFlushChunkLocked() uint64 {
	chunk := s.pending.takeAll()
	if len(chunk.entries) == 0 {
		return 0
	}
	s.pendingFlushChunks = append(s.pendingFlushChunks, chunk)
	return chunk.toTimeTick
}

func (s *segmentView) flushPackForTimeTickLocked(timetick uint64) *flushPack {
	for _, chunk := range s.pendingFlushChunks {
		if chunk.toTimeTick == timetick {
			return chunk.flushPack(s.meta, s.schema)
		}
	}
	return nil
}

func (s *segmentView) prunePendingFlushChunksLocked() {
	dataCheckpoint := s.meta.GetDataCheckpointTimeTick()
	remaining := s.pendingFlushChunks[:0]
	for _, chunk := range s.pendingFlushChunks {
		if chunk.toTimeTick <= dataCheckpoint {
			continue
		}
		remaining = append(remaining, chunk)
	}
	s.pendingFlushChunks = remaining
}

func (s *segmentView) appendPersistedStorage(storage *streamingpb.L1SegmentPersistedStorage) {
	if s.meta.PersistedStorage == nil {
		s.meta.PersistedStorage = &streamingpb.L1SegmentPersistedStorage{}
	}
	if storage.GetManifestPath() != "" {
		s.meta.PersistedStorage.ManifestPath = storage.GetManifestPath()
	}
	s.meta.PersistedStorage.Binlogs = append(
		s.meta.PersistedStorage.Binlogs,
		cloneL1SegmentBinLogs(storage.GetBinlogs())...,
	)
	if storage.GetMergedStatsBinlog() != nil {
		s.meta.PersistedStorage.MergedStatsBinlog = cloneFieldBinlog(storage.GetMergedStatsBinlog())
	}
}

func (info *segmentView) ensureStat() {
	if info.meta.Stat == nil {
		info.meta.Stat = &streamingpb.SegmentAssignmentStat{}
	}
}

func (info *segmentView) ConsumeDirtyAndGetSnapshot() *streamingpb.SegmentAssignmentMeta {
	info.mu.Lock()
	defer info.mu.Unlock()
	if info.pendingDirtySnapshot != nil {
		return proto.Clone(info.pendingDirtySnapshot).(*streamingpb.SegmentAssignmentMeta)
	}
	if !info.dirty {
		return nil
	}
	info.pendingDirtySnapshot = proto.Clone(info.meta).(*streamingpb.SegmentAssignmentMeta)
	return proto.Clone(info.pendingDirtySnapshot).(*streamingpb.SegmentAssignmentMeta)
}
