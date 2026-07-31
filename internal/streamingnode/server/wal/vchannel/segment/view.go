package segment

import (
	"context"
	"math"
	"sort"
	"sync"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	walcheckpoint "github.com/milvus-io/milvus/internal/streamingnode/server/wal/checkpoint"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

func NewSegmentViewFromMeta(meta *streamingpb.SegmentAssignmentMeta, schema *schemapb.CollectionSchema, configs ...runtimeConfig) *SegmentView {
	return NewSegmentView(
		meta,
		meta.GetCheckpointTimeTick(),
		meta.GetDataCheckpointTimeTick(),
		false,
		writeOnlyInsertBuffer{},
		schema,
		firstRuntimeConfig(configs),
	)
}

func NewSegmentViewFromMetaWithConfig(meta *streamingpb.SegmentAssignmentMeta, schema *schemapb.CollectionSchema, config ViewConfig) *SegmentView {
	return NewSegmentViewFromMeta(meta, schema, runtimeConfigFromViewConfig(config))
}

func NewSegmentView(
	meta *streamingpb.SegmentAssignmentMeta,
	persistedMetaTimeTick uint64,
	persistedDataTimeTick uint64,
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
		onSegmentSealed:       config.onSegmentSealed,
		schema:                schema,
		finalCommitDone:       finalCommitDoneFromMeta(meta),
		metaAndData:           config.metaAndData,
		commitL1Limiter:       config.commitL1Limiter,
		owner:                 config.owner,
	}
}

func finalCommitDoneFromMeta(meta *streamingpb.SegmentAssignmentMeta) bool {
	return meta.GetSealedAtDataVersion() != nil
}

func shouldRetryRecoveredFinalCommit(meta *streamingpb.SegmentAssignmentMeta) bool {
	if finalCommitDoneFromMeta(meta) {
		return false
	}
	switch meta.GetState() {
	case streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
		streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED:
		return meta.GetCheckpointTimeTick() > 0 &&
			meta.GetDataCheckpointTimeTick() >= meta.GetCheckpointTimeTick()
	default:
		return false
	}
}

func NewSegmentViewFromCreateSegmentMessage(msg message.ImmutableCreateSegmentMessageV2, schema *schemapb.CollectionSchema, configs ...runtimeConfig) *SegmentView {
	return NewSegmentView(
		newSegmentAssignmentMetaFromCreateSegmentMessage(msg),
		0,
		0,
		true,
		writeOnlyInsertBuffer{},
		schema,
		firstRuntimeConfig(configs),
	)
}

func NewSegmentViewFromCreateSegmentMessageWithConfig(msg message.ImmutableCreateSegmentMessageV2, schema *schemapb.CollectionSchema, config ViewConfig) *SegmentView {
	return NewSegmentViewFromCreateSegmentMessage(msg, schema, runtimeConfigFromViewConfig(config))
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

// SegmentView tracks the metadata and durability state of a growing segment.
type SegmentView struct {
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
	lifecycle    Lifecycle
	packWriter   PackWriter        // writes pending insert data to object storage.
	runtime      moduleapi.Runtime // schedules segment-owned data tasks.
	pendingTasks []segmentTask     // unfinished segment tasks used as predecessors.
	// pendingFinalCommit keeps repeated flush messages from enqueueing another
	// final commit while the current one is pending or retrying.
	pendingFinalCommit segmentTask
	// finalCommitDone is process-local task state. Recovery only infers it from
	// the persisted sealed version; a data checkpoint alone does not prove that
	// the coordinator accepted the final commit.
	finalCommitDone bool
	pending         writeOnlyInsertBuffer // in-memory insert buffer not yet written as L1.
	// pendingFlushChunks keeps chunks already handed to pending/running flush tasks,
	// ordered by toTimeTick. Chunks stay here until segment data checkpoint advances
	// over them.
	pendingFlushChunks []writeOnlyInsertBuffer
	flushPolicy        flushPolicy // decides when pending insert data should be flushed.
	onDataUpdated      func()      // notifies checkpoint manager when data barrier may advance.
	onSegmentSealed    func(walview.SegmentSealedEvent)
	schema             *schemapb.CollectionSchema // schema used to encode pending insert data.
	metaAndData        bool                       // false during meta-only replay; true when data tasks may run.
	commitL1Limiter    *commitL1Limiter
	owner              ViewOwner
}

// ViewOption configures a segment-level recovery view.
type ViewOption func(*runtimeConfig)

func WithViewLifecycle(lifecycle Lifecycle) ViewOption {
	return func(config *runtimeConfig) {
		config.lifecycle = lifecycle
	}
}

func WithViewPackWriter(writer PackWriter) ViewOption {
	return func(config *runtimeConfig) {
		config.packWriter = writer
	}
}

func WithViewRuntime(runtime moduleapi.Runtime) ViewOption {
	return func(config *runtimeConfig) {
		config.runtime = runtime
	}
}

func WithViewSegmentSealedNotifier(notifier func(walview.SegmentSealedEvent)) ViewOption {
	return func(config *runtimeConfig) {
		config.onSegmentSealed = notifier
	}
}

func WithViewDataUpdatedNotifier(notifier func()) ViewOption {
	return func(config *runtimeConfig) {
		config.onDataUpdated = notifier
	}
}

// NewSegmentViewFromMetaWithOptions creates a segment-level recovery view from
// persisted meta.
func NewSegmentViewFromMetaWithOptions(meta *streamingpb.SegmentAssignmentMeta, schema *schemapb.CollectionSchema, opts ...ViewOption) *SegmentView {
	config := runtimeConfig{}
	for _, opt := range opts {
		opt(&config)
	}
	return NewSegmentViewFromMeta(meta, schema, config)
}

// NewSegmentViewFromCreateSegmentMessageWithOptions creates a segment-level
// recovery view from a CreateSegment WAL message.
func NewSegmentViewFromCreateSegmentMessageWithOptions(msg message.ImmutableCreateSegmentMessageV2, schema *schemapb.CollectionSchema, opts ...ViewOption) *SegmentView {
	config := runtimeConfig{}
	for _, opt := range opts {
		opt(&config)
	}
	return NewSegmentViewFromCreateSegmentMessage(msg, schema, config)
}

func (s *SegmentView) ID() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.meta.GetSegmentId()
}

func (s *SegmentView) HasDirty() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dirty
}

func (s *SegmentView) ObserveCreateSegmentMessageV2(_ context.Context, msg message.ImmutableCreateSegmentMessageV2) moduleapi.ObserveResult {
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

func (s *SegmentView) ObserveInsertMessageV1(
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

func (s *SegmentView) ObserveTxnMessage(_ context.Context, msg message.ImmutableTxnMessage) moduleapi.ObserveResult {
	var task segmentTask
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
	var rows uint64
	var binarySize uint64
	err := forEachSegmentInsertMessage(msg, s.meta.GetSegmentId(), func(insert segmentInsertMessage) error {
		matched = true
		if timetick > metaTimeTick {
			s.observeInsertMetaLocked(timetick, insert.Assignment)
			appliedMeta = true
		}
		if s.metaAndData &&
			timetick > s.meta.GetDataCheckpointTimeTick() &&
			timetick > pendingDataTimeTick {
			rows += insert.Assignment.GetRows()
			binarySize += insert.Assignment.GetBinarySize()
			appliedData = true
		}
		return nil
	})
	if err != nil || !matched {
		return result
	}
	if appliedMeta {
		result.Meta = s.metaBarrier()
	}
	if appliedData {
		s.pending.appendMessage(msg, rows, binarySize)
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

func (s *SegmentView) observeInsertMetaLocked(timetick uint64, assignment *messagespb.PartitionSegmentAssignment) {
	s.ensureStat()
	s.meta.Stat.ModifiedBinarySize += assignment.GetBinarySize()
	s.meta.Stat.ModifiedRows += assignment.GetRows()
	s.meta.Stat.LastModifiedTimestamp = tsoutil.PhysicalTime(timetick).Unix()
	s.meta.CheckpointTimeTick = timetick
	s.dirty = true
}

func (s *SegmentView) Flush(_ context.Context, timetick uint64) moduleapi.ObserveResult {
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
	if task != nil {
		s.runtime.Scheduler.Submit(task)
	}
	return result
}

func (s *SegmentView) FlushInsertChunk(ctx context.Context, targetTimeTick uint64) error {
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

func (info *SegmentView) AssignmentMeta() *streamingpb.SegmentAssignmentMeta {
	info.mu.Lock()
	defer info.mu.Unlock()
	return proto.Clone(info.meta).(*streamingpb.SegmentAssignmentMeta)
}

func (info *SegmentView) WritePathRecoveryState() (moduleapi.SegmentWritePathRecoveryState, bool) {
	info.mu.Lock()
	defer info.mu.Unlock()
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

func (info *SegmentView) MatchesScope(scope moduleapi.Scope) bool {
	info.mu.Lock()
	defer info.mu.Unlock()
	switch scope.Type {
	case moduleapi.ScopeAll:
		return true
	case moduleapi.ScopeVChannel:
		return info.meta.GetVchannel() == scope.VChannel
	case moduleapi.ScopePartition:
		if scope.VChannel != "" && info.meta.GetVchannel() != scope.VChannel {
			return false
		}
		return info.meta.GetCollectionId() == scope.CollectionID && info.meta.GetPartitionId() == scope.PartitionID
	default:
		return false
	}
}

func (info *SegmentView) VisibleSnapshot(vchannel string, dataVersion qviews.DataVersion) (walview.VisibleSegment, bool) {
	info.mu.Lock()
	defer info.mu.Unlock()
	if info.meta.GetVchannel() != vchannel || !info.visibleAtDataVersionLocked(dataVersion) {
		return walview.VisibleSegment{}, false
	}
	meta := proto.Clone(info.meta).(*streamingpb.SegmentAssignmentMeta)
	insertMessages := info.pending.Messages()
	for _, chunk := range info.pendingFlushChunks {
		insertMessages = append(insertMessages, chunk.Messages()...)
	}
	visible := walview.VisibleSegment{
		SegmentID:           meta.GetSegmentId(),
		PartitionID:         meta.GetPartitionId(),
		Schema:              cloneSchema(info.schema),
		Assignment:          meta,
		SealedAtDataVersion: meta.GetSealedAtDataVersion(),
		Data: walview.SegmentSnapshotData{
			PersistedStorage: clonePersistedStorage(meta.GetPersistedStorage()),
			InsertMessages:   insertMessages,
		},
	}
	return visible, true
}

func (info *SegmentView) FlushedSegmentSnapshot(vchannel string, dataVersion qviews.DataVersion) (walview.FlushedSegment, bool) {
	info.mu.Lock()
	defer info.mu.Unlock()
	if info.meta.GetVchannel() != vchannel ||
		info.meta.GetState() != streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED ||
		info.meta.GetSealedAtDataVersion() == nil ||
		qviews.FromProtoDataVersion(info.meta.GetSealedAtDataVersion()).GT(dataVersion) {
		return walview.FlushedSegment{}, false
	}
	return walview.FlushedSegment{
		SegmentID:           info.meta.GetSegmentId(),
		PartitionID:         info.meta.GetPartitionId(),
		FlushTimeTick:       info.meta.GetCheckpointTimeTick(),
		SealedAtDataVersion: qviews.FromProtoDataVersion(info.meta.GetSealedAtDataVersion()),
	}, true
}

func (info *SegmentView) visibleAtDataVersionLocked(dataVersion qviews.DataVersion) bool {
	switch info.meta.GetState() {
	case streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING:
		return true
	case streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED:
		if info.meta.GetSealedAtDataVersion() == nil {
			return true
		}
		return qviews.FromProtoDataVersion(info.meta.GetSealedAtDataVersion()).GT(dataVersion)
	default:
		return false
	}
}

func (info *SegmentView) SealedDataVersion(vchannel string) (qviews.DataVersion, bool) {
	info.mu.Lock()
	defer info.mu.Unlock()
	if info.meta.GetVchannel() != vchannel ||
		info.meta.GetState() != streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED ||
		info.meta.GetSealedAtDataVersion() == nil {
		return qviews.DataVersion{}, false
	}
	return qviews.FromProtoDataVersion(info.meta.GetSealedAtDataVersion()), true
}

func (info *SegmentView) TombstonedSealedDataVersion() (string, qviews.DataVersion, bool) {
	info.mu.Lock()
	defer info.mu.Unlock()
	if info.meta.GetState() != streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED ||
		info.meta.GetSealedAtDataVersion() == nil {
		return "", qviews.DataVersion{}, false
	}
	return info.meta.GetVchannel(), qviews.FromProtoDataVersion(info.meta.GetSealedAtDataVersion()), true
}

func cloneSchema(schema *schemapb.CollectionSchema) *schemapb.CollectionSchema {
	if schema == nil {
		return nil
	}
	return proto.Clone(schema).(*schemapb.CollectionSchema)
}

func clonePersistedStorage(storage *streamingpb.L1SegmentPersistedStorage) *streamingpb.L1SegmentPersistedStorage {
	if storage == nil {
		return nil
	}
	return proto.Clone(storage).(*streamingpb.L1SegmentPersistedStorage)
}

func (info *SegmentView) metaTimeTick() uint64 {
	info.mu.Lock()
	defer info.mu.Unlock()
	return info.persistedMetaTimeTick
}

func (info *SegmentView) metaBarrier() walcheckpoint.Barrier {
	return walcheckpoint.BarrierFunc(info.metaTimeTick)
}

func (info *SegmentView) MetaBarrier() walcheckpoint.Barrier {
	return info.metaBarrier()
}

func (info *SegmentView) dataTimeTick() uint64 {
	info.mu.Lock()
	defer info.mu.Unlock()
	return info.persistedDataTimeTick
}

func (info *SegmentView) dataBarrier() walcheckpoint.Barrier {
	return walcheckpoint.BarrierFunc(info.dataTimeTick)
}

func (info *SegmentView) DataBarrier() walcheckpoint.Barrier {
	return info.dataBarrier()
}

func (info *SegmentView) DurableFrontierTimeTick() uint64 {
	info.mu.Lock()
	defer info.mu.Unlock()
	if info.meta.GetState() == streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED {
		if info.dirty || !info.finalCommitDone || info.hasPendingDataWorkLocked() {
			return frontierBefore(info.meta.GetTombstoneTimeTick())
		}
		return math.MaxUint64
	}
	if !info.hasPendingDataWorkLocked() {
		return math.MaxUint64
	}
	frontier := min(info.persistedMetaTimeTick, info.persistedDataTimeTick)
	if info.meta.GetState() == streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED && !info.finalCommitDone {
		frontier = min(frontier, frontierBefore(info.meta.GetCheckpointTimeTick()))
	}
	return frontier
}

func frontierBefore(timetick uint64) uint64 {
	if timetick == 0 {
		return 0
	}
	return timetick - 1
}

func (info *SegmentView) hasPendingDataWorkLocked() bool {
	if info.meta.GetDataCheckpointTimeTick() > info.persistedDataTimeTick {
		return true
	}
	if info.meta.GetState() == streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED {
		if !info.finalCommitDone || info.meta.GetDataCheckpointTimeTick() < info.meta.GetCheckpointTimeTick() {
			return true
		}
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

func (info *SegmentView) CreateTimeTick() uint64 {
	info.mu.Lock()
	defer info.mu.Unlock()
	return info.meta.GetStat().GetCreateSegmentTimeTick()
}

func (info *SegmentView) markMetaPersistedLocked(timetick uint64) {
	if timetick > info.persistedMetaTimeTick {
		info.persistedMetaTimeTick = timetick
	}
}

func (info *SegmentView) markDataPersistedLocked(timetick uint64) {
	if timetick > info.persistedDataTimeTick {
		info.persistedDataTimeTick = timetick
	}
}

func (info *SegmentView) MarkSnapshotPersisted(snapshot *streamingpb.SegmentAssignmentMeta) {
	info.mu.Lock()
	defer info.mu.Unlock()
	info.markMetaPersistedLocked(snapshot.GetCheckpointTimeTick())
	info.markDataPersistedLocked(snapshot.GetDataCheckpointTimeTick())
	if info.pendingDirtySnapshot != nil && proto.Equal(info.pendingDirtySnapshot, snapshot) {
		info.pendingDirtySnapshot = nil
	}
	info.dirty = !proto.Equal(info.meta, snapshot)
}

func (info *SegmentView) NotifyDataUpdated() {
	if info.owner != nil {
		info.owner.SegmentDataUpdated(info.ID(), info)
		return
	}
	if info.onDataUpdated != nil {
		info.onDataUpdated()
	}
}

func (info *SegmentView) NotifySegmentSealed(event walview.SegmentSealedEvent) {
	if info.owner != nil {
		info.owner.SegmentSealed(event)
		return
	}
	if info.onSegmentSealed != nil {
		info.onSegmentSealed(event)
	}
}

func (info *SegmentView) markDataCheckpointLocked(timetick uint64) {
	if timetick <= info.meta.GetDataCheckpointTimeTick() {
		return
	}
	info.meta.DataCheckpointTimeTick = timetick
	info.dirty = true
	info.prunePendingFlushChunksLocked()
}

func (info *SegmentView) TryFinalizeTombstone() bool {
	info.mu.Lock()
	defer info.mu.Unlock()
	return info.maybeMarkTombstonedLocked()
}

func (info *SegmentView) TryFinalizeTombstoneAt(oldest qviews.DataVersion, hasOldest bool) bool {
	info.mu.Lock()
	defer info.mu.Unlock()
	if !info.tombstoneFinalizeReadyLocked() {
		return false
	}
	if sealedAt := info.meta.GetSealedAtDataVersion(); sealedAt != nil && hasOldest && !oldest.GTE(qviews.FromProtoDataVersion(sealedAt)) {
		return false
	}
	return info.maybeMarkTombstonedLocked()
}

func (info *SegmentView) HasReadyTombstoneFinalize() bool {
	info.mu.Lock()
	defer info.mu.Unlock()
	return info.tombstoneFinalizeReadyLocked()
}

func (info *SegmentView) SwitchIntoMetaAndData() {
	info.mu.Lock()
	if info.metaAndData {
		info.mu.Unlock()
		return
	}
	info.metaAndData = true
	var task segmentTask
	if shouldRetryRecoveredFinalCommit(info.meta) {
		task = info.newRecoveredCommitL1SegmentTaskLocked(info.meta.GetCheckpointTimeTick())
	}
	scheduler := info.runtime.Scheduler
	info.mu.Unlock()
	if task != nil {
		scheduler.Submit(task)
	}
}

func (info *SegmentView) SetSchema(schema *schemapb.CollectionSchema) {
	if schema == nil {
		return
	}
	info.mu.Lock()
	defer info.mu.Unlock()
	info.schema = schema
}

func (info *SegmentView) IsGrowing() bool {
	info.mu.Lock()
	defer info.mu.Unlock()
	return info.meta.State == streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING
}

func (info *SegmentView) canReplayInsertLocked(timetick uint64) bool {
	switch info.meta.GetState() {
	case streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING:
		return true
	case streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED:
		return timetick <= info.meta.GetCheckpointTimeTick()
	default:
		return false
	}
}

func (info *SegmentView) shouldSkipReplayLocked(timetick uint64) bool {
	return shouldSkipTombstonedSegmentMeta(info.meta, timetick)
}

func (info *SegmentView) TombstonePersisted() bool {
	info.mu.Lock()
	defer info.mu.Unlock()
	tombstoneTimeTick := info.meta.GetTombstoneTimeTick()
	return info.meta.GetState() == streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED &&
		tombstoneTimeTick > 0 &&
		!info.dirty &&
		info.persistedMetaTimeTick >= tombstoneTimeTick &&
		info.persistedDataTimeTick >= tombstoneTimeTick
}

func (info *SegmentView) CoveredByTombstone(vchannel string, partitionID int64, timetick uint64) bool {
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

func (info *SegmentView) TombstonedCleanupReady(metaPhysicalTimeTick uint64, dataPhysicalTimeTick uint64) bool {
	info.mu.Lock()
	defer info.mu.Unlock()
	return info.tombstonedCleanupReadyLocked(metaPhysicalTimeTick, dataPhysicalTimeTick)
}

func (info *SegmentView) tombstonedCleanupReadyLocked(metaPhysicalTimeTick uint64, dataPhysicalTimeTick uint64) bool {
	tombstoneTimeTick := info.meta.GetTombstoneTimeTick()
	return info.meta.GetState() == streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED &&
		tombstoneTimeTick > 0 &&
		!info.dirty &&
		info.persistedMetaTimeTick >= tombstoneTimeTick &&
		info.persistedDataTimeTick >= tombstoneTimeTick &&
		metaPhysicalTimeTick > tombstoneTimeTick &&
		dataPhysicalTimeTick > tombstoneTimeTick
}

func (info *SegmentView) observeFlushMeta(timetick uint64) (bool, uint64, bool) {
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

func (s *SegmentView) MarkPendingDataDurable(timetick uint64) {
	if timetick <= s.meta.GetDataCheckpointTimeTick() {
		return
	}
	s.markDataCheckpointLocked(timetick)
}

func (s *SegmentView) markSealedAtDataVersionLocked(version *viewpb.DataVersion) (walview.SegmentSealedEvent, bool) {
	if version == nil {
		return walview.SegmentSealedEvent{}, false
	}
	current := s.meta.GetSealedAtDataVersion()
	if current != nil {
		if proto.Equal(current, version) {
			return walview.SegmentSealedEvent{}, false
		}
		panic("conflicting sealed data version for segment assignment")
	}
	s.meta.SealedAtDataVersion = proto.Clone(version).(*viewpb.DataVersion)
	s.dirty = true
	return walview.SegmentSealedEvent{
		SegmentID:           s.meta.GetSegmentId(),
		VChannel:            s.meta.GetVchannel(),
		SealedAtDataVersion: qviews.FromProtoDataVersion(version),
	}, true
}

func (s *SegmentView) maybeMarkTombstonedLocked() bool {
	if !s.tombstoneFinalizeReadyLocked() {
		return false
	}
	tombstoneTimeTick := s.meta.GetCheckpointTimeTick()
	s.meta.State = streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED
	s.meta.TombstoneTimeTick = tombstoneTimeTick
	s.dirty = true
	return true
}

func (s *SegmentView) tombstoneFinalizeReadyLocked() bool {
	tombstoneTimeTick := s.meta.GetCheckpointTimeTick()
	return s.meta.GetState() == streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED &&
		tombstoneTimeTick > 0 &&
		s.finalCommitDone &&
		s.meta.GetSealedAtDataVersion() != nil &&
		s.meta.GetDataCheckpointTimeTick() >= tombstoneTimeTick
}

func shouldSkipTombstonedSegmentMeta(meta *streamingpb.SegmentAssignmentMeta, timetick uint64) bool {
	return meta != nil &&
		meta.GetState() == streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED &&
		meta.GetTombstoneTimeTick() > 0 &&
		timetick <= meta.GetTombstoneTimeTick()
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
	index := firstPendingFlushChunkAtOrAfter(s.pendingFlushChunks, timetick)
	if index == len(s.pendingFlushChunks) || s.pendingFlushChunks[index].toTimeTick != timetick {
		return nil
	}
	return s.pendingFlushChunks[index].flushPack(s.meta, s.schema)
}

func (s *SegmentView) prunePendingFlushChunksLocked() {
	dataCheckpoint := s.meta.GetDataCheckpointTimeTick()
	firstRemaining := firstPendingFlushChunkAfter(s.pendingFlushChunks, dataCheckpoint)
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

func (s *SegmentView) appendPersistedStorage(storage *streamingpb.L1SegmentPersistedStorage) {
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
	info.pendingDirtySnapshot = proto.Clone(info.meta).(*streamingpb.SegmentAssignmentMeta)
	return proto.Clone(info.pendingDirtySnapshot).(*streamingpb.SegmentAssignmentMeta)
}
