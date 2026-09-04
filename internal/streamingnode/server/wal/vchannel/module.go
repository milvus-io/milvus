package vchannel

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/segment"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/transformlog"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// materializeBoundStallWarnInterval is how long the L1 materialization bound
// may stay unchanged (blocked by an uncommitted L1 segment) before the module
// starts warning with the blocking segment's identity.
const materializeBoundStallWarnInterval = 1 * time.Minute

// ModuleConfig contains the initial state and dependencies for one vchannel
// recovery module.
type ModuleConfig struct {
	PChannel string
	VChannel string

	VChannelMeta *streamingpb.VChannelMeta
	Segments     map[int64]*streamingpb.SegmentAssignmentMeta

	Runtime           moduleapi.Runtime
	Logger            *mlog.Logger
	SegmentLifecycle  segment.Lifecycle
	SegmentPackWriter segment.PackWriter
	// PendingTransformEntries is the initial materialization window of the
	// transform consumer: the durable records after the restored
	// transform_materialized_time_tick, loaded once by recovery. Runtime
	// observation appends to it directly.
	PendingTransformEntries   []*streamingpb.TransformLogEntry
	TransformLogMaterializer  transformlog.Materializer
	TransformLogMaterialRows  uint64
	TransformLogMaterialBytes uint64
	OnCleanup                 func(*VChannelRecoveryModule)
}

// VChannelRecoveryModule owns all recovery_storage state for one vchannel.
type VChannelRecoveryModule struct {
	// mu serializes WAL observation with snapshot state transitions. In
	// particular, segments may grow when CreateSegment is observed while the
	// recovery background task is collecting dirty snapshots.
	//
	// Lock order is m.mu -> SegmentView.mu. Never take a SegmentView lock
	// (directly or via a Locked helper) while holding another module's mu, and
	// never call SegmentView.NotifyDataUpdated with a view lock held (it
	// re-enters the module).
	mu       sync.Mutex
	pchannel string
	vchannel string

	runtime moduleapi.Runtime
	logger  *mlog.Logger

	vchannelView    *VChannelView
	segments        map[int64]*segment.SegmentView
	dirtyMu         sync.Mutex
	dirtySegments   map[int64]*segment.SegmentView
	cleanupSegments map[int64]*segment.SegmentView
	pendingCleanup  map[int64]*segment.SegmentView

	transformLog *transformlog.TransformLog

	// materializeUpperBound mirrors the last bound published to the transform
	// log, so refreshTransformMaterializeUpperBoundLocked can skip unchanged
	// publishes on the WAL observation hot path. The stall bookkeeping drives
	// the stuck-L1 warning.
	materializeUpperBound     uint64
	materializeBoundChangedAt time.Time
	materializeBoundWarnedAt  time.Time

	segmentLifecycle  segment.Lifecycle
	segmentPackWriter segment.PackWriter

	removed   bool
	onCleanup func(*VChannelRecoveryModule)
}

// NewModule creates a single-vchannel recovery module.
func NewModule(config ModuleConfig) (*VChannelRecoveryModule, error) {
	return newModule(config, false)
}

func newModuleFromOwnedRecoveryState(config ModuleConfig) (*VChannelRecoveryModule, error) {
	return newModule(config, true)
}

func newModule(config ModuleConfig, adoptVChannelMeta bool) (*VChannelRecoveryModule, error) {
	if config.PChannel == "" {
		return nil, merr.WrapErrServiceInternalMsg("vchannel recovery module pchannel is empty")
	}
	if config.VChannel == "" {
		return nil, merr.WrapErrServiceInternalMsg("vchannel recovery module vchannel is empty")
	}
	module := &VChannelRecoveryModule{
		pchannel:                  config.PChannel,
		vchannel:                  config.VChannel,
		runtime:                   config.Runtime,
		logger:                    config.Logger,
		segments:                  make(map[int64]*segment.SegmentView),
		segmentLifecycle:          config.SegmentLifecycle,
		segmentPackWriter:         config.SegmentPackWriter,
		onCleanup:                 config.OnCleanup,
		materializeUpperBound:     math.MaxUint64,
		materializeBoundChangedAt: time.Now(),
	}
	if config.VChannelMeta != nil {
		if adoptVChannelMeta {
			module.vchannelView = newVChannelViewFromOwnedMeta(config.VChannelMeta)
		} else {
			module.vchannelView = NewVChannelViewFromMeta(config.VChannelMeta)
		}
	}
	for id, meta := range config.Segments {
		if meta.GetVchannel() != config.VChannel {
			continue
		}
		var schema *schemapb.CollectionSchema
		if module.vchannelView != nil {
			schema = module.vchannelView.CreateSegmentSchema(meta.GetPartitionId(), meta.GetStat().GetCreateSegmentTimeTick())
		}
		view := segment.NewSegmentViewFromMetaWithConfig(meta, schema, module.segmentViewConfig())
		module.segments[id] = view
		if view.TombstonePersisted() {
			if module.cleanupSegments == nil {
				module.cleanupSegments = make(map[int64]*segment.SegmentView)
			}
			module.cleanupSegments[id] = view
		}
	}
	module.transformLog = transformlog.New(transformlog.Config{
		VChannel: config.VChannel,
		// The frontier starts at the persisted value: every record at or below
		// it was already committed before the crash. Records above it are
		// re-materialized on recovery; duplicate L0 output is idempotent, and
		// unregistered output is re-registered, so the persisted frontier alone
		// guarantees no delete data is lost.
		MaterializedTimeTick: config.VChannelMeta.GetTransformMaterializedTimeTick(),
		PendingEntries:       config.PendingTransformEntries,
		MaterializeMaxRows:   config.TransformLogMaterialRows,
		MaterializeMaxBytes:  config.TransformLogMaterialBytes,
		Materializer:         config.TransformLogMaterializer,
		Runtime:              config.Runtime,
		OnMaterialized:       module.markTransformMaterialized,
	})
	module.mu.Lock()
	module.refreshTransformMaterializeUpperBoundLocked()
	module.mu.Unlock()
	return module, nil
}

func (m *VChannelRecoveryModule) segmentViewConfig() segment.ViewConfig {
	return segment.ViewConfig{
		Runtime:    m.runtime,
		Lifecycle:  m.segmentLifecycle,
		PackWriter: m.segmentPackWriter,
		Owner:      m,
	}
}

// ObserveMessage returns false only when the module was concurrently removed;
// the manager can then retry a CreateCollection against a fresh module.
func (m *VChannelRecoveryModule) ObserveMessage(
	ctx context.Context,
	retained message.RetainedImmutableMessage,
) bool {
	if m == nil {
		return true
	}
	msg := retained.Message()
	if !m.shouldObserve(msg) {
		return true
	}
	if funcutil.IsControlChannel(msg.VChannel()) && !msg.IsPChannelLevel() {
		return true
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.removed {
		return false
	}
	switch msg.MessageType() {
	case message.MessageTypeCreateCollection:
		m.handleCreateCollectionMessage(message.MustAsImmutableCreateCollectionMessageV1(msg))
	case message.MessageTypeCreatePartition:
		m.handleCreatePartitionMessage(message.MustAsImmutableCreatePartitionMessageV1(msg))
	case message.MessageTypeSchemaChange:
		m.handleSchemaChangeMessage(message.MustAsImmutableSchemaChangeMessageV2(msg))
	case message.MessageTypeAlterCollection:
		m.handleAlterCollectionMessage(ctx, message.MustAsRetainedImmutableAlterCollectionMessageV2(retained))
	case message.MessageTypeDropCollection:
		m.handleDropCollectionMessage(ctx, message.MustAsRetainedImmutableDropCollectionMessageV1(retained))
	case message.MessageTypeDropPartition:
		m.handleDropPartitionMessage(ctx, message.MustAsRetainedImmutableDropPartitionMessageV1(retained))
	case message.MessageTypeTruncateCollection:
		m.handleTruncateCollectionMessage(ctx, message.MustAsRetainedImmutableTruncateCollectionMessageV2(retained))
	case message.MessageTypeCreateSegment:
		m.handleCreateSegmentMessage(ctx, message.MustAsRetainedImmutableCreateSegmentMessageV2(retained))
	case message.MessageTypeInsert, message.MessageTypeTxn:
		m.handleInsertMessage(ctx, retained)
	case message.MessageTypeFlush:
		m.handleFlushMessage(ctx, message.MustAsRetainedImmutableFlushMessageV2(retained))
	case message.MessageTypeManualFlush:
		m.handleManualFlushMessage(ctx, retained)
	case message.MessageTypeFlushAll:
		m.handleFlushAllMessage(ctx, retained)
	case message.MessageTypeAlterWAL:
		m.handleAlterWALMessage(ctx, retained)
	}
	// The transform consumer observes the same message stream directly and
	// materializes at its own pace; the summary persists at its own pace. The
	// two are deliberately not ordered: no barrier or external write API
	// drives either of them. The summary observes the stream independently at
	// the recovery level (see recoveryStorageImpl.observeModulesMessage).
	m.transformLog.ObserveMessage(retained)
	return true
}

func (m *VChannelRecoveryModule) RecoverySnapshot() *moduleapi.WritePathRecoveryModuleSnapshot {
	snapshot := &moduleapi.WritePathRecoveryModuleSnapshot{
		VChannels:       make(map[string]moduleapi.VChannelWritePathRecoveryState),
		GrowingSegments: make(map[int64]moduleapi.SegmentWritePathRecoveryState),
	}
	if m == nil {
		return snapshot
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.vchannelView != nil {
		if state, ok := m.vchannelView.WritePathRecoveryState(); ok {
			snapshot.VChannels[m.vchannel] = state
		}
	}
	for id, view := range m.segments {
		view.ResumePendingRecovery()
		if state, ok := view.WritePathRecoveryState(); ok {
			snapshot.GrowingSegments[id] = state
		}
	}
	return snapshot
}

func (m *VChannelRecoveryModule) ConsumeDirtySnapshots() []moduleapi.DirtySnapshot {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	snapshots := make([]moduleapi.DirtySnapshot, 0)
	if m.vchannelView != nil {
		if meta, saveSchemas := m.vchannelView.ConsumeDirtyAndGetSnapshot(); meta != nil {
			owner := m.vchannelView
			snapshot := meta
			op := moduleapi.SnapshotOpUpsertBase
			if saveSchemas {
				op = moduleapi.SnapshotOpUpsert
			}
			snapshots = append(snapshots, newDirtySnapshot(
				moduleapi.ModuleNameVChannel,
				moduleapi.SnapshotKey{PChannel: m.pchannel, VChannel: m.vchannel},
				op,
				snapshot,
				func() { owner.MarkSnapshotPersisted(snapshot) },
			))
		}
	}
	for id, view := range m.takeDirtySegments() {
		if meta := view.ConsumeDirtyAndGetSnapshot(); meta != nil {
			owner := view
			snapshot := meta
			snapshots = append(snapshots, newDirtySnapshot(
				moduleapi.ModuleNameSegment,
				moduleapi.SnapshotKey{PChannel: m.pchannel, SegmentID: id},
				moduleapi.SnapshotOpUpsert,
				snapshot,
				func() {
					m.markSegmentSnapshotPersisted(id, owner, snapshot)
				},
			))
		}
	}
	return snapshots
}

func (m *VChannelRecoveryModule) IsActive() bool {
	if m == nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.vchannelView != nil && m.vchannelView.IsActive()
}

// RequestPersistThrough schedules persistence for buffered data observed by
// this VChannel through targetTimeTick.
func (m *VChannelRecoveryModule) RequestPersistThrough(targetTimeTick uint64) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.removed {
		return
	}
	for _, view := range m.segments {
		view.RequestPersistThrough(targetTimeTick)
	}
}

func (m *VChannelRecoveryModule) handleCreateCollectionMessage(msg message.ImmutableCreateCollectionMessageV1) {
	if m.vchannelView == nil {
		m.vchannelView = NewVChannelViewFromCreateCollectionMessage(msg)
	} else {
		replacement, _ := m.vchannelView.ObserveCreateCollectionMessageV1(msg)
		if replacement != nil {
			m.vchannelView = replacement
		}
	}
}

func (m *VChannelRecoveryModule) handleCreatePartitionMessage(msg message.ImmutableCreatePartitionMessageV1) {
	if m.vchannelView == nil {
		return
	}
	m.vchannelView.ObserveCreatePartitionMessageV1(msg)
}

func (m *VChannelRecoveryModule) handleSchemaChangeMessage(msg message.ImmutableSchemaChangeMessageV2) {
	if m.vchannelView == nil {
		return
	}
	m.vchannelView.ObserveSchemaChangeMessageV2(msg)
}

func (m *VChannelRecoveryModule) handleAlterCollectionMessage(
	ctx context.Context,
	owned message.RetainedImmutableAlterCollectionMessageV2,
) {
	msg := owned.Message()
	if m.vchannelView != nil {
		m.vchannelView.ObserveAlterCollectionMessageV2(msg)
	}
	if messageutil.IsSchemaChange(msg.Header()) {
		handle := owned.CloneHandle()
		defer handle.Release()
		m.flushAllSegmentsCreatedBefore(ctx, handle)
	}
}

func (m *VChannelRecoveryModule) handleDropCollectionMessage(
	ctx context.Context,
	owned message.RetainedImmutableDropCollectionMessageV1,
) {
	msg := owned.Message()
	if m.vchannelView != nil {
		m.vchannelView.ObserveDropCollectionMessageV1(msg)
	}
	handle := owned.CloneHandle()
	defer handle.Release()
	m.flushAllSegmentsCreatedBefore(ctx, handle)
}

func (m *VChannelRecoveryModule) handleDropPartitionMessage(
	ctx context.Context,
	owned message.RetainedImmutableDropPartitionMessageV1,
) {
	msg := owned.Message()
	if m.vchannelView != nil {
		m.vchannelView.ObserveDropPartitionMessageV1(msg)
	}
	handle := owned.CloneHandle()
	defer handle.Release()
	m.flushPartitionSegmentsCreatedBefore(ctx, handle, msg.Header().GetPartitionId())
}

func (m *VChannelRecoveryModule) handleTruncateCollectionMessage(
	ctx context.Context,
	owned message.RetainedImmutableTruncateCollectionMessageV2,
) {
	msg := owned.Message()
	if m.vchannelView != nil {
		m.vchannelView.ObserveTruncateCollectionMessageV2(msg)
	}
	handle := owned.CloneHandle()
	defer handle.Release()
	m.flushAllSegmentsCreatedBefore(ctx, handle)
}

func (m *VChannelRecoveryModule) handleCreateSegmentMessage(
	ctx context.Context,
	msg message.RetainedImmutableCreateSegmentMessageV2,
) {
	raw := msg.Message()
	id := raw.Header().GetSegmentId()
	view := m.segments[id]
	created := false
	if view == nil {
		var schema *schemapb.CollectionSchema
		if m.vchannelView != nil {
			schema = m.vchannelView.CreateSegmentSchema(raw.Header().GetPartitionId(), raw.TimeTick())
		}
		if schema == nil {
			// The collection schema is unresolvable for this segment. Skipping
			// the create-segment message silently would make every later insert
			// of the segment disappear without a trace, so surface it.
			mlog.Warn(ctx, "create segment message skipped: collection schema unresolvable",
				mlog.String("pchannel", m.pchannel),
				mlog.String("vchannel", m.vchannel),
				mlog.Int64("segmentID", id),
				mlog.Int64("partitionID", raw.Header().GetPartitionId()),
				mlog.Uint64("timetick", raw.TimeTick()))
			return
		}
		view = segment.NewSegmentViewFromCreateSegmentMessageWithConfig(raw, schema, m.segmentViewConfig())
		m.segments[id] = view
		created = true
	}
	changed := created
	if view != nil && view.ObserveCreateSegmentMessageV2(ctx, msg) {
		changed = true
	}
	if changed {
		m.markSegmentUpdatedLocked(id)
	}
}

func (m *VChannelRecoveryModule) handleInsertMessage(
	ctx context.Context,
	owned message.RetainedImmutableMessage,
) {
	batches, err := segment.BuildInsertBatches(owned.Message())
	if err != nil {
		return
	}
	for segmentID, batch := range batches {
		view := m.segments[segmentID]
		if view == nil {
			// The insert targets a segment this module does not track (e.g.
			// already dropped/cleaned up). Its rows are intentionally skipped,
			// but log it so a legitimate segment is never silently starved.
			mlog.Warn(ctx, "insert message skipped: segment view not found",
				mlog.String("pchannel", m.pchannel),
				mlog.String("vchannel", m.vchannel),
				mlog.Int64("segmentID", segmentID),
				mlog.Uint64("timetick", owned.Message().TimeTick()))
			continue
		}
		if view.ObserveInsert(ctx, owned, batch) {
			m.markSegmentUpdatedLocked(segmentID)
		}
	}
}

func (m *VChannelRecoveryModule) handleFlushMessage(
	ctx context.Context,
	msg message.RetainedImmutableFlushMessageV2,
) {
	id := msg.Message().Header().GetSegmentId()
	if segment := m.segments[id]; segment != nil {
		handle := msg.CloneHandle()
		defer handle.Release()
		if segment.Flush(ctx, handle) {
			m.markSegmentUpdatedLocked(id)
		}
	}
}

func (m *VChannelRecoveryModule) handleManualFlushMessage(
	ctx context.Context,
	msg message.RetainedImmutableMessage,
) {
	m.flushAllSegmentsCreatedBefore(ctx, msg)
}

func (m *VChannelRecoveryModule) handleFlushAllMessage(
	ctx context.Context,
	msg message.RetainedImmutableMessage,
) {
	m.flushAllSegmentsCreatedBefore(ctx, msg)
}

func (m *VChannelRecoveryModule) handleAlterWALMessage(
	ctx context.Context,
	msg message.RetainedImmutableMessage,
) {
	m.flushAllSegmentsCreatedBefore(ctx, msg)
}

func (m *VChannelRecoveryModule) flushAllSegmentsCreatedBefore(
	ctx context.Context,
	msg message.RetainedImmutableMessage,
) {
	for _, view := range m.segments {
		if view.CreateTimeTick() >= msg.Message().TimeTick() {
			continue
		}
		if view.Flush(ctx, msg) {
			m.markSegmentUpdatedLocked(view.ID())
		}
	}
}

func (m *VChannelRecoveryModule) flushPartitionSegmentsCreatedBefore(
	ctx context.Context,
	msg message.RetainedImmutableMessage,
	partitionID int64,
) {
	for _, view := range m.segments {
		if view.PartitionID() != partitionID || view.CreateTimeTick() >= msg.Message().TimeTick() {
			continue
		}
		if view.Flush(ctx, msg) {
			m.markSegmentUpdatedLocked(view.ID())
		}
	}
}

func (m *VChannelRecoveryModule) shouldObserve(msg message.ImmutableMessage) bool {
	return msg.VChannel() == m.vchannel || msg.VChannel() == "" || msg.IsPChannelLevel()
}

func (m *VChannelRecoveryModule) markSegmentUpdatedLocked(segmentID int64) {
	view := m.segments[segmentID]
	m.markSegmentViewUpdatedLocked(segmentID, view)
}

func (m *VChannelRecoveryModule) SegmentDataUpdated(segmentID int64, view *segment.SegmentView) {
	m.markSegmentViewUpdated(segmentID, view)
	if m.runtime.Notifier != nil {
		m.runtime.Notifier.NotifyModuleUpdated(moduleapi.ModuleNameSegment)
	}
}

func (m *VChannelRecoveryModule) markSegmentViewUpdated(segmentID int64, view *segment.SegmentView) {
	m.mu.Lock()
	m.markSegmentViewUpdatedLocked(segmentID, view)
	m.mu.Unlock()
}

func (m *VChannelRecoveryModule) markSegmentViewUpdatedLocked(segmentID int64, view *segment.SegmentView) {
	if view == nil {
		return
	}
	m.tryFinalizeSegmentLocked(segmentID, view)
	m.markSegmentDirty(segmentID, view)
	m.refreshTransformMaterializeUpperBoundLocked()
}

func (m *VChannelRecoveryModule) refreshTransformMaterializeUpperBoundLocked() {
	upperBound := uint64(math.MaxUint64)
	blockerSegmentID := int64(0)
	for _, view := range m.segments {
		// Lock-free: L1MaterializationBlockerTimeTick reads an immutable tick
		// and an atomically published commit flag, so the scan stays cheap on
		// the WAL observation hot path.
		if timetick, blocks := view.L1MaterializationBlockerTimeTick(); blocks && timetick < upperBound {
			upperBound = timetick
			blockerSegmentID = view.ID()
		}
	}
	now := time.Now()
	if upperBound == m.materializeUpperBound {
		// Bound unchanged: skip the publish. Warn if the same uncommitted L1
		// segment has pinned materialization (and therefore drop cleanup) for
		// a while — previously this stalled silently.
		m.warnIfMaterializeBoundStalledLocked(now, blockerSegmentID)
		return
	}
	m.materializeUpperBound = upperBound
	m.materializeBoundChangedAt = now
	m.materializeBoundWarnedAt = time.Time{}
	m.transformLog.SetMaterializeUpperBound(upperBound)
}

// warnIfMaterializeBoundStalledLocked emits at most one warning per interval
// while the materialization bound stays pinned by an uncommitted L1 segment.
func (m *VChannelRecoveryModule) warnIfMaterializeBoundStalledLocked(now time.Time, blockerSegmentID int64) {
	if blockerSegmentID == 0 || m.materializeBoundChangedAt.IsZero() ||
		now.Sub(m.materializeBoundChangedAt) < materializeBoundStallWarnInterval {
		return
	}
	if !m.materializeBoundWarnedAt.IsZero() && now.Sub(m.materializeBoundWarnedAt) < materializeBoundStallWarnInterval {
		return
	}
	m.materializeBoundWarnedAt = now
	mlog.Warn(context.TODO(), "L1 materialization bound stalled by uncommitted segment",
		mlog.String("pchannel", m.pchannel),
		mlog.String("vchannel", m.vchannel),
		mlog.Int64("segmentID", blockerSegmentID),
		mlog.Uint64("boundTimeTick", m.materializeUpperBound),
		mlog.Duration("stalledFor", now.Sub(m.materializeBoundChangedAt)),
	)
}

func (m *VChannelRecoveryModule) tryFinalizeSegmentLocked(segmentID int64, view *segment.SegmentView) bool {
	if !view.TryFinalizeTombstone() {
		return false
	}
	m.markSegmentDirty(segmentID, view)
	return true
}

// markTransformMaterialized mirrors the transform materialization frontier
// into the vchannel meta (transform_materialized_time_tick) and marks the
// vchannel snapshot dirty, so the frontier persists with the next catalog
// checkpoint. The transform consumer calls it after every committed batch; it
// must not call back into the TransformLog. The summary retention frontier is
// updated by the recovery persistence flow once the vchannel meta is durable
// (see recoveryStorageImpl.persistDirtySnapshot), so the summary only ever
// releases records below a frontier that a crash-recovery would observe.
func (m *VChannelRecoveryModule) markTransformMaterialized(timeTick uint64) {
	m.mu.Lock()
	if m.vchannelView != nil {
		m.vchannelView.SetTransformMaterializedTimeTick(timeTick)
	}
	m.mu.Unlock()
	if m.runtime.Notifier != nil {
		m.runtime.Notifier.NotifyModuleUpdated(moduleapi.ModuleNameVChannel)
	}
}

// FlushCheckpointTimeTick returns the vchannel-level flush position: the
// largest timetick for which every insert and delete record of this vchannel
// up to it has been durably flushed (inserts to L1 segments, deletes to L0
// output). It is min of the transform materialization frontier and every
// growing segment's durable checkpoint, both read from the vchannel / segment
// metas that are persisted with the recovery catalog — the same values a
// crash-recovery would observe, so DataCoord can trust the checkpoint to
// report flush progress (GetFlushState).
func (m *VChannelRecoveryModule) FlushCheckpointTimeTick() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	minTick := uint64(math.MaxUint64)
	if m.vchannelView != nil {
		if matTick := m.vchannelView.PersistedMaterializedTimeTick(); matTick < minTick {
			minTick = matTick
		}
	}
	for _, view := range m.segments {
		if !view.IsGrowing() {
			continue
		}
		if cp := view.PersistedCheckpointTimeTick(); cp < minTick {
			minTick = cp
		}
	}
	if minTick == math.MaxUint64 {
		return 0
	}
	return minTick
}

func (m *VChannelRecoveryModule) markSegmentSnapshotPersisted(
	segmentID int64,
	view *segment.SegmentView,
	snapshot *streamingpb.SegmentAssignmentMeta,
) {
	m.mu.Lock()
	view.MarkSnapshotPersisted(snapshot)
	tombstonePersisted := view.TombstonePersisted()
	if tombstonePersisted {
		if m.cleanupSegments == nil {
			m.cleanupSegments = make(map[int64]*segment.SegmentView)
		}
		m.cleanupSegments[segmentID] = view
	}
	m.mu.Unlock()
	if tombstonePersisted && m.runtime.Notifier != nil {
		m.runtime.Notifier.NotifyModuleUpdated(moduleapi.ModuleNameSegment)
	}
}

func (m *VChannelRecoveryModule) HasCleanupCandidates() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.hasCleanupCandidatesLocked()
}

func (m *VChannelRecoveryModule) hasCleanupCandidatesLocked() bool {
	return !m.removed && (len(m.cleanupSegments) > 0 || len(m.pendingCleanup) > 0 ||
		m.vchannelView != nil && m.vchannelView.HasCleanupCandidate())
}

func (m *VChannelRecoveryModule) ConsumeCleanupSnapshots(cleanup moduleapi.CleanupContext) []moduleapi.DirtySnapshot {
	m.mu.Lock()
	if m.removed {
		m.mu.Unlock()
		return nil
	}
	vchannelChanged := m.vchannelView != nil && m.vchannelView.TryFinalizeTombstone(cleanup.PhysicalTimeTick)
	var snapshots []moduleapi.DirtySnapshot
	for segmentID, view := range m.cleanupSegments {
		if !m.segmentCleanupReadyLocked(view, cleanup) {
			continue
		}
		meta := view.AssignmentMeta()
		delete(m.cleanupSegments, segmentID)
		if m.pendingCleanup == nil {
			m.pendingCleanup = make(map[int64]*segment.SegmentView)
		}
		m.pendingCleanup[segmentID] = view
		owner := view
		snapshots = append(snapshots, newDirtySnapshot(
			moduleapi.ModuleNameSegment,
			moduleapi.SnapshotKey{PChannel: m.pchannel, SegmentID: segmentID},
			moduleapi.SnapshotOpDelete,
			meta,
			func() { m.completeSegmentCleanup(segmentID, owner) },
		))
	}
	if m.vchannelView != nil {
		dropSnapshot, cleanupPartitions := m.vchannelView.TombstonedCleanupPlan(
			cleanup.PhysicalTimeTick,
			m.vchannelView.PersistedMaterializedTimeTick(),
		)
		if len(cleanupPartitions) > 0 {
			vchannelChanged = m.vchannelView.ApplyPartitionCleanup(cleanupPartitions) || vchannelChanged
		} else if dropSnapshot != nil && len(m.segments) == 0 {
			checkpointTimeTick := dropSnapshot.GetCheckpointTimeTick()
			snapshots = append(snapshots,
				newDirtySnapshot(
					moduleapi.ModuleNameVChannel,
					moduleapi.SnapshotKey{PChannel: m.pchannel, VChannel: m.vchannel},
					moduleapi.SnapshotOpDelete,
					dropSnapshot,
					func() { m.completeVChannelCleanup(checkpointTimeTick) },
				),
			)
		}
	}
	m.mu.Unlock()
	if vchannelChanged && m.runtime.Notifier != nil {
		m.runtime.Notifier.NotifyModuleUpdated(moduleapi.ModuleNameVChannel)
	}
	return snapshots
}

func (m *VChannelRecoveryModule) completeVChannelCleanup(checkpointTimeTick uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.removed || len(m.segments) > 0 || m.vchannelView == nil ||
		m.vchannelView.VChannelDropCleanupSnapshot(checkpointTimeTick, checkpointTimeTick) == nil {
		return
	}
	m.removed = true
	if m.onCleanup != nil {
		m.onCleanup(m)
	}
}

func (m *VChannelRecoveryModule) segmentCleanupReadyLocked(view *segment.SegmentView, cleanup moduleapi.CleanupContext) bool {
	return m.vchannelView != nil &&
		view.TombstonedCleanupReady(cleanup.PhysicalTimeTick)
}

func (m *VChannelRecoveryModule) completeSegmentCleanup(segmentID int64, view *segment.SegmentView) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.pendingCleanup[segmentID] != view || m.segments[segmentID] != view {
		return
	}
	delete(m.pendingCleanup, segmentID)
	delete(m.segments, segmentID)
	m.refreshTransformMaterializeUpperBoundLocked()
	m.dirtyMu.Lock()
	delete(m.dirtySegments, segmentID)
	m.dirtyMu.Unlock()
}

func (m *VChannelRecoveryModule) markSegmentDirty(segmentID int64, view *segment.SegmentView) {
	m.dirtyMu.Lock()
	if m.dirtySegments == nil {
		m.dirtySegments = make(map[int64]*segment.SegmentView)
	}
	m.dirtySegments[segmentID] = view
	m.dirtyMu.Unlock()
}

func (m *VChannelRecoveryModule) takeDirtySegments() map[int64]*segment.SegmentView {
	m.dirtyMu.Lock()
	dirty := m.dirtySegments
	m.dirtySegments = nil
	m.dirtyMu.Unlock()
	return dirty
}
