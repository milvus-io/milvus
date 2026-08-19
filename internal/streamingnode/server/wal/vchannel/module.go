package vchannel

import (
	"context"
	"sync"

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

// ModuleConfig contains the initial state and dependencies for one vchannel
// recovery module.
type ModuleConfig struct {
	PChannel string
	VChannel string

	VChannelMeta     *streamingpb.VChannelMeta
	Segments         map[int64]*streamingpb.SegmentAssignmentMeta
	TransformLogMeta *streamingpb.VChannelTransformLogMeta

	Runtime                   moduleapi.Runtime
	Logger                    *mlog.Logger
	SegmentLifecycle          segment.Lifecycle
	SegmentPackWriter         segment.PackWriter
	TransformLogStore         transformlog.Store
	TransformLogMaterializer  transformlog.Materializer
	TransformLogMaxRows       uint64
	TransformLogMaxBytes      uint64
	TransformLogMaterialRows  uint64
	TransformLogMaterialBytes uint64
	DataObservedTimeTick      uint64
	OnCleanup                 func(*VChannelRecoveryModule)
}

// VChannelRecoveryModule owns all recovery_storage state for one vchannel.
type VChannelRecoveryModule struct {
	// mu serializes WAL observation with snapshot state transitions. In
	// particular, segments may grow when CreateSegment is observed while the
	// recovery background task is collecting dirty snapshots.
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

	dataObservedTimeTick uint64

	transformLog *transformlog.TransformLog

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
		pchannel:             config.PChannel,
		vchannel:             config.VChannel,
		runtime:              config.Runtime,
		logger:               config.Logger,
		segments:             make(map[int64]*segment.SegmentView),
		segmentLifecycle:     config.SegmentLifecycle,
		segmentPackWriter:    config.SegmentPackWriter,
		dataObservedTimeTick: config.DataObservedTimeTick,
		onCleanup:            config.OnCleanup,
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
		VChannel:            config.VChannel,
		MaxRows:             config.TransformLogMaxRows,
		MaterializeMaxRows:  config.TransformLogMaterialRows,
		MaterializeMaxBytes: config.TransformLogMaterialBytes,
		Meta:                config.TransformLogMeta,
		Store:               config.TransformLogStore,
		Materializer:        config.TransformLogMaterializer,
		Runtime:             config.Runtime,
	})
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
	mode moduleapi.ObserveMode,
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
		if mode.ApplyMeta() {
			m.handleCreateCollectionMessage(message.MustAsImmutableCreateCollectionMessageV1(msg))
		}
	case message.MessageTypeCreatePartition:
		if mode.ApplyMeta() {
			m.handleCreatePartitionMessage(message.MustAsImmutableCreatePartitionMessageV1(msg))
		}
	case message.MessageTypeSchemaChange:
		if mode.ApplyMeta() {
			m.handleSchemaChangeMessage(message.MustAsImmutableSchemaChangeMessageV2(msg))
		}
	case message.MessageTypeAlterCollection:
		m.handleAlterCollectionMessage(ctx, message.MustAsRetainedImmutableAlterCollectionMessageV2(retained), mode)
	case message.MessageTypeDropCollection:
		m.handleDropCollectionMessage(ctx, message.MustAsRetainedImmutableDropCollectionMessageV1(retained), mode)
	case message.MessageTypeDropPartition:
		m.handleDropPartitionMessage(ctx, message.MustAsRetainedImmutableDropPartitionMessageV1(retained), mode)
	case message.MessageTypeTruncateCollection:
		m.handleTruncateCollectionMessage(ctx, message.MustAsRetainedImmutableTruncateCollectionMessageV2(retained), mode)
	case message.MessageTypeCreateSegment:
		m.handleCreateSegmentMessage(ctx, message.MustAsRetainedImmutableCreateSegmentMessageV2(retained), mode)
	case message.MessageTypeInsert, message.MessageTypeTxn:
		m.handleInsertMessage(ctx, retained, mode)
	case message.MessageTypeFlush:
		m.handleFlushMessage(ctx, message.MustAsRetainedImmutableFlushMessageV2(retained), mode)
	case message.MessageTypeManualFlush:
		m.handleManualFlushMessage(ctx, retained, mode)
	case message.MessageTypeFlushAll:
		m.handleFlushAllMessage(ctx, retained, mode)
	case message.MessageTypeAlterWAL:
		m.handleAlterWALMessage(ctx, retained, mode)
	}
	if mode.ApplyData() && m.transformLog != nil {
		m.transformLog.ObserveMessage(ctx, retained)
	}
	if mode.ApplyData() && msg.TimeTick() > m.dataObservedTimeTick {
		m.dataObservedTimeTick = msg.TimeTick()
	}
	return true
}

func (m *VChannelRecoveryModule) StartDataRecovery() moduleapi.ModuleSnapshot {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	snapshot := &moduleapi.WritePathRecoveryModuleSnapshot{}
	if m.vchannelView != nil {
		if state, ok := m.vchannelView.WritePathRecoveryState(); ok {
			snapshot.VChannels = map[string]moduleapi.VChannelWritePathRecoveryState{m.vchannel: state}
		}
	}
	for id, view := range m.segments {
		view.StartDataRecovery()
		if state, ok := view.WritePathRecoveryState(); ok {
			if snapshot.GrowingSegments == nil {
				snapshot.GrowingSegments = make(map[int64]moduleapi.SegmentWritePathRecoveryState)
			}
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
				snapshot.GetCheckpointTimeTick(),
				0,
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
				snapshot.GetCheckpointTimeTick(),
				snapshot.GetDataCheckpointTimeTick(),
				func() {
					m.markSegmentSnapshotPersisted(id, owner, snapshot)
				},
			))
		}
	}
	if m.transformLog != nil {
		if meta := m.transformLog.ConsumeDirtyAndGetSnapshot(); meta != nil {
			snapshot := meta
			snapshots = append(snapshots, newDirtySnapshot(
				moduleapi.ModuleNameTransformLog,
				moduleapi.SnapshotKey{PChannel: m.pchannel, VChannel: m.vchannel},
				moduleapi.SnapshotOpUpsert,
				snapshot,
				snapshot.GetCheckpointTimeTick(),
				snapshot.GetCheckpointTimeTick(),
				func() { m.markTransformSnapshotPersisted(snapshot) },
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
	if m.transformLog != nil {
		m.transformLog.RequestPersistThrough(targetTimeTick)
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
	mode moduleapi.ObserveMode,
) {
	msg := owned.Message()
	if mode.ApplyMeta() && m.vchannelView != nil {
		m.vchannelView.ObserveAlterCollectionMessageV2(msg)
	}
	if mode.ApplyData() && messageutil.IsSchemaChange(msg.Header()) {
		handle := owned.CloneHandle()
		defer handle.Release()
		m.flushAllSegmentsCreatedBefore(ctx, handle, mode)
	}
}

func (m *VChannelRecoveryModule) handleDropCollectionMessage(
	ctx context.Context,
	owned message.RetainedImmutableDropCollectionMessageV1,
	mode moduleapi.ObserveMode,
) {
	msg := owned.Message()
	if mode.ApplyMeta() && m.vchannelView != nil {
		m.vchannelView.ObserveDropCollectionMessageV1(msg)
	}
	if mode.ApplyData() {
		handle := owned.CloneHandle()
		defer handle.Release()
		m.flushAllSegmentsCreatedBefore(ctx, handle, mode)
	}
}

func (m *VChannelRecoveryModule) handleDropPartitionMessage(
	ctx context.Context,
	owned message.RetainedImmutableDropPartitionMessageV1,
	mode moduleapi.ObserveMode,
) {
	msg := owned.Message()
	if mode.ApplyMeta() && m.vchannelView != nil {
		m.vchannelView.ObserveDropPartitionMessageV1(msg)
	}
	if mode.ApplyData() {
		handle := owned.CloneHandle()
		defer handle.Release()
		m.flushPartitionSegmentsCreatedBefore(ctx, handle, msg.Header().GetPartitionId(), mode)
	}
}

func (m *VChannelRecoveryModule) handleTruncateCollectionMessage(
	ctx context.Context,
	owned message.RetainedImmutableTruncateCollectionMessageV2,
	mode moduleapi.ObserveMode,
) {
	msg := owned.Message()
	if mode.ApplyMeta() && m.vchannelView != nil {
		m.vchannelView.ObserveTruncateCollectionMessageV2(msg)
	}
	if mode.ApplyData() {
		handle := owned.CloneHandle()
		defer handle.Release()
		m.flushAllSegmentsCreatedBefore(ctx, handle, mode)
	}
}

func (m *VChannelRecoveryModule) handleCreateSegmentMessage(
	ctx context.Context,
	msg message.RetainedImmutableCreateSegmentMessageV2,
	mode moduleapi.ObserveMode,
) {
	raw := msg.Message()
	id := raw.Header().GetSegmentId()
	view := m.segments[id]
	created := false
	if view == nil && mode.ApplyMeta() {
		var schema *schemapb.CollectionSchema
		if m.vchannelView != nil {
			schema = m.vchannelView.CreateSegmentSchema(raw.Header().GetPartitionId(), raw.TimeTick())
		}
		if schema == nil {
			return
		}
		view = segment.NewSegmentViewFromCreateSegmentMessageWithConfig(raw, schema, m.segmentViewConfig())
		m.segments[id] = view
		created = true
	}
	changed := created
	if view != nil && view.ObserveCreateSegmentMessageV2(ctx, msg, mode) {
		changed = true
	}
	if changed {
		m.markSegmentUpdatedLocked(id)
	}
}

func (m *VChannelRecoveryModule) handleInsertMessage(
	ctx context.Context,
	owned message.RetainedImmutableMessage,
	mode moduleapi.ObserveMode,
) {
	batches, err := segment.BuildInsertBatches(owned.Message())
	if err != nil {
		return
	}
	for segmentID, batch := range batches {
		view := m.segments[segmentID]
		if view == nil {
			continue
		}
		if view.ObserveInsert(ctx, owned, batch, mode) {
			m.markSegmentUpdatedLocked(segmentID)
		}
	}
}

func (m *VChannelRecoveryModule) handleFlushMessage(
	ctx context.Context,
	msg message.RetainedImmutableFlushMessageV2,
	mode moduleapi.ObserveMode,
) {
	id := msg.Message().Header().GetSegmentId()
	if segment := m.segments[id]; segment != nil {
		handle := msg.CloneHandle()
		defer handle.Release()
		if segment.Flush(ctx, handle, mode) {
			m.markSegmentUpdatedLocked(id)
		}
	}
}

func (m *VChannelRecoveryModule) handleManualFlushMessage(
	ctx context.Context,
	msg message.RetainedImmutableMessage,
	mode moduleapi.ObserveMode,
) {
	m.flushAllSegmentsCreatedBefore(ctx, msg, mode)
}

func (m *VChannelRecoveryModule) handleFlushAllMessage(
	ctx context.Context,
	msg message.RetainedImmutableMessage,
	mode moduleapi.ObserveMode,
) {
	m.flushAllSegmentsCreatedBefore(ctx, msg, mode)
}

func (m *VChannelRecoveryModule) handleAlterWALMessage(
	ctx context.Context,
	msg message.RetainedImmutableMessage,
	mode moduleapi.ObserveMode,
) {
	m.flushAllSegmentsCreatedBefore(ctx, msg, mode)
}

func (m *VChannelRecoveryModule) flushAllSegmentsCreatedBefore(
	ctx context.Context,
	msg message.RetainedImmutableMessage,
	mode moduleapi.ObserveMode,
) {
	for _, view := range m.segments {
		if view.CreateTimeTick() >= msg.Message().TimeTick() {
			continue
		}
		if view.Flush(ctx, msg, mode) {
			m.markSegmentUpdatedLocked(view.ID())
		}
	}
}

func (m *VChannelRecoveryModule) flushPartitionSegmentsCreatedBefore(
	ctx context.Context,
	msg message.RetainedImmutableMessage,
	partitionID int64,
	mode moduleapi.ObserveMode,
) {
	for _, view := range m.segments {
		if view.PartitionID() != partitionID || view.CreateTimeTick() >= msg.Message().TimeTick() {
			continue
		}
		if view.Flush(ctx, msg, mode) {
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
}

func (m *VChannelRecoveryModule) tryFinalizeSegmentLocked(segmentID int64, view *segment.SegmentView) bool {
	if !view.TryFinalizeTombstone() {
		return false
	}
	m.markSegmentDirty(segmentID, view)
	return true
}

func (m *VChannelRecoveryModule) markTransformSnapshotPersisted(snapshot *streamingpb.VChannelTransformLogMeta) {
	m.mu.Lock()
	m.transformLog.MarkSnapshotPersisted(snapshot)
	m.mu.Unlock()
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
	vchannelChanged := m.vchannelView != nil && m.vchannelView.TryFinalizeTombstone(cleanup.DataPhysicalTimeTick)
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
			meta.GetTombstoneTimeTick(),
			meta.GetTombstoneTimeTick(),
			func() { m.completeSegmentCleanup(segmentID, owner) },
		))
	}
	if m.vchannelView != nil && m.transformLog != nil {
		dropSnapshot, cleanupPartitions := m.vchannelView.TombstonedCleanupPlan(
			cleanup.MetaPhysicalTimeTick,
			cleanup.DataPhysicalTimeTick,
			m.transformLog.PersistedMaterializedTimeTick(),
		)
		if len(cleanupPartitions) > 0 {
			vchannelChanged = m.vchannelView.ApplyPartitionCleanup(cleanupPartitions) || vchannelChanged
		} else if dropSnapshot != nil && len(m.segments) == 0 &&
			!m.transformLog.HasDirty() && !m.transformLog.HasPendingWork() {
			tombstoneTimeTick := dropSnapshot.GetTombstoneTimeTick()
			snapshots = append(snapshots,
				newDirtySnapshot(
					moduleapi.ModuleNameTransformLog,
					moduleapi.SnapshotKey{PChannel: m.pchannel, VChannel: m.vchannel},
					moduleapi.SnapshotOpDelete,
					m.transformLog.SnapshotMeta(),
					tombstoneTimeTick,
					tombstoneTimeTick,
					nil,
				),
				newDirtySnapshot(
					moduleapi.ModuleNameVChannel,
					moduleapi.SnapshotKey{PChannel: m.pchannel, VChannel: m.vchannel},
					moduleapi.SnapshotOpDelete,
					dropSnapshot,
					tombstoneTimeTick,
					tombstoneTimeTick,
					func() { m.completeVChannelCleanup(tombstoneTimeTick) },
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

func (m *VChannelRecoveryModule) completeVChannelCleanup(tombstoneTimeTick uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.removed || len(m.segments) > 0 || m.vchannelView == nil ||
		m.vchannelView.VChannelDropCleanupSnapshot(tombstoneTimeTick, tombstoneTimeTick) == nil {
		return
	}
	m.removed = true
	if m.onCleanup != nil {
		m.onCleanup(m)
	}
}

func (m *VChannelRecoveryModule) segmentCleanupReadyLocked(view *segment.SegmentView, cleanup moduleapi.CleanupContext) bool {
	return m.vchannelView != nil &&
		view.TombstonedCleanupReady(cleanup.MetaPhysicalTimeTick, cleanup.DataPhysicalTimeTick)
}

func (m *VChannelRecoveryModule) completeSegmentCleanup(segmentID int64, view *segment.SegmentView) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.pendingCleanup[segmentID] != view || m.segments[segmentID] != view {
		return
	}
	delete(m.pendingCleanup, segmentID)
	delete(m.segments, segmentID)
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
