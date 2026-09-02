package vchannel

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/queryresource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/segment"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/transformlog"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
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
	OnSegmentSealed           func(walview.SegmentSealedEvent)

	TransformLogStream         wal.TransformLogStream
	QueryRuntimeModuleBuilders []queryresource.QueryRuntimeModuleBuilder
	NodeScheduler              nodescheduler.Scheduler
	QueryRuntimeDispatcher     *queryresource.Dispatcher
	QueryViewLoadInfoProvider  queryresource.LoadInfoProvider
	DataObservedTimeTick       uint64
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

	segmentLifecycle        segment.Lifecycle
	segmentPackWriter       segment.PackWriter
	externalOnSegmentSealed func(walview.SegmentSealedEvent)

	metaAndData bool

	queryTransformLogStream wal.TransformLogStream
	queryResources          *queryresource.Manager
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
		pchannel:                config.PChannel,
		vchannel:                config.VChannel,
		runtime:                 config.Runtime,
		logger:                  config.Logger,
		segments:                make(map[int64]*segment.SegmentView),
		segmentLifecycle:        config.SegmentLifecycle,
		segmentPackWriter:       config.SegmentPackWriter,
		externalOnSegmentSealed: config.OnSegmentSealed,
		queryTransformLogStream: config.TransformLogStream,
		dataObservedTimeTick:    config.DataObservedTimeTick,
	}
	module.queryResources = queryresource.NewManager(queryresource.Config{
		Builders:         config.QueryRuntimeModuleBuilders,
		Scheduler:        config.NodeScheduler,
		Dispatcher:       config.QueryRuntimeDispatcher,
		LoadInfoProvider: config.QueryViewLoadInfoProvider,
	})
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
			module.advanceSegmentDataVersionSummaryLocked(view)
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

func (m *VChannelRecoveryModule) ObserveMessage(
	ctx context.Context,
	retained message.RetainedImmutableMessage,
) {
	if m == nil {
		return
	}
	msg := retained.Message()
	if !m.shouldObserve(msg) {
		return
	}
	if funcutil.IsControlChannel(msg.VChannel()) && !msg.IsPChannelLevel() {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
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
	case message.MessageTypeAlterLoadConfig:
		m.handleAlterLoadConfigMessage(message.MustAsImmutableAlterLoadConfigMessageV2(msg))
	case message.MessageTypeDropLoadConfig:
		m.handleDropLoadConfigMessage(message.MustAsImmutableDropLoadConfigMessageV2(msg))
	case message.MessageTypeCreateSegment:
		m.handleCreateSegmentMessage(ctx, message.MustAsRetainedImmutableCreateSegmentMessageV2(retained))
	case message.MessageTypeInsert:
		m.handleInsertMessage(ctx, message.MustAsRetainedImmutableInsertMessageV1(retained))
	case message.MessageTypeTxn:
		m.handleTxnMessage(ctx, message.MustAsRetainedImmutableTxnMessage(retained))
	case message.MessageTypeFlush:
		m.handleFlushMessage(ctx, message.MustAsRetainedImmutableFlushMessageV2(retained))
	case message.MessageTypeManualFlush:
		m.handleManualFlushMessage(ctx, retained)
	case message.MessageTypeFlushAll:
		m.handleFlushAllMessage(ctx, retained)
	case message.MessageTypeAlterWAL:
		m.handleAlterWALMessage(ctx, retained)
	}
	if m.transformLog != nil {
		m.transformLog.ObserveMessage(ctx, retained)
	}
	if m.metaAndData && msg.TimeTick() > m.dataObservedTimeTick {
		m.dataObservedTimeTick = msg.TimeTick()
	}
	m.observeQueryResourceEvent(ctx, walview.VChannelResourceEvent{Message: msg})
}

func (m *VChannelRecoveryModule) SwitchIntoMetaAndData() moduleapi.ModuleSnapshot {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.metaAndData = true
	snapshot := &moduleapi.WritePathRecoveryModuleSnapshot{}
	if m.vchannelView != nil {
		m.vchannelView.SwitchIntoMetaAndData()
		if state, ok := m.vchannelView.WritePathRecoveryState(); ok {
			snapshot.VChannels = map[string]moduleapi.VChannelWritePathRecoveryState{m.vchannel: state}
		}
	}
	for id, view := range m.segments {
		view.SwitchIntoMetaAndData()
		if state, ok := view.WritePathRecoveryState(); ok {
			if snapshot.GrowingSegments == nil {
				snapshot.GrowingSegments = make(map[int64]moduleapi.SegmentWritePathRecoveryState)
			}
			snapshot.GrowingSegments[id] = state
		}
	}
	if m.transformLog != nil {
		m.transformLog.SwitchIntoMetaAndData()
	}
	m.queryResources.TryBuildLocked(m.queryWALViewLocked)
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
			m.markSegmentDirty(id, view)
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

func (m *VChannelRecoveryModule) handleCreateCollectionMessage(msg message.ImmutableCreateCollectionMessageV1) {
	if m.vchannelView == nil {
		m.vchannelView = NewVChannelViewFromCreateCollectionMessage(msg)
		if m.metaAndData {
			m.vchannelView.SwitchIntoMetaAndData()
		}
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

func (m *VChannelRecoveryModule) handleAlterLoadConfigMessage(msg message.ImmutableAlterLoadConfigMessageV2) {
	if m.vchannelView == nil {
		return
	}
	m.vchannelView.ObserveAlterLoadConfigMessageV2(msg)
}

func (m *VChannelRecoveryModule) handleDropLoadConfigMessage(msg message.ImmutableDropLoadConfigMessageV2) {
	if m.vchannelView == nil {
		return
	}
	m.vchannelView.ObserveDropLoadConfigMessageV2(msg)
}

func (m *VChannelRecoveryModule) handleCreateSegmentMessage(
	ctx context.Context,
	msg message.RetainedImmutableCreateSegmentMessageV2,
) {
	raw := msg.Message()
	id := raw.Header().GetSegmentId()
	view := m.segments[id]
	if view == nil {
		var schema *schemapb.CollectionSchema
		if m.vchannelView != nil {
			schema = m.vchannelView.CreateSegmentSchema(raw.Header().GetPartitionId(), raw.TimeTick())
		}
		if schema == nil {
			return
		}
		view = segment.NewSegmentViewFromCreateSegmentMessageWithConfig(raw, schema, m.segmentViewConfig())
		if m.metaAndData {
			view.SwitchIntoMetaAndData()
		}
		m.segments[id] = view
	}
	view.ObserveCreateSegmentMessageV2(ctx, msg)
	m.markSegmentUpdatedLocked(id)
}

func (m *VChannelRecoveryModule) handleInsertMessage(
	ctx context.Context,
	msg message.RetainedImmutableInsertMessageV1,
) {
	for _, partition := range msg.Message().Header().GetPartitions() {
		view := m.segments[partition.GetSegmentAssignment().GetSegmentId()]
		if view == nil {
			continue
		}
		view.ObserveInsertMessageV1(ctx, msg, partition)
		m.markSegmentUpdatedLocked(view.ID())
	}
}

func (m *VChannelRecoveryModule) handleTxnMessage(
	ctx context.Context,
	owned message.RetainedImmutableTxnMessage,
) {
	msg := owned.Message()
	if msg == nil {
		return
	}
	observed := make(map[int64]struct{})
	_ = msg.RangeOver(func(inner message.ImmutableMessage) error {
		if inner.MessageType() != message.MessageTypeInsert {
			return nil
		}
		insert := message.MustAsImmutableInsertMessageV1(inner)
		for _, partition := range insert.Header().GetPartitions() {
			id := partition.GetSegmentAssignment().GetSegmentId()
			if _, ok := observed[id]; ok {
				continue
			}
			view := m.segments[id]
			if view == nil {
				continue
			}
			observed[id] = struct{}{}
			view.ObserveTxnMessage(ctx, owned)
			m.markSegmentUpdatedLocked(id)
		}
		return nil
	})
}

func (m *VChannelRecoveryModule) handleFlushMessage(
	ctx context.Context,
	msg message.RetainedImmutableFlushMessageV2,
) {
	id := msg.Message().Header().GetSegmentId()
	if segment := m.segments[id]; segment != nil {
		handle := msg.CloneHandle()
		defer handle.Release()
		segment.Flush(ctx, handle)
		m.markSegmentUpdatedLocked(id)
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
		view.Flush(ctx, msg)
		m.markSegmentUpdatedLocked(view.ID())
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
		view.Flush(ctx, msg)
		m.markSegmentUpdatedLocked(view.ID())
	}
}

func (m *VChannelRecoveryModule) shouldObserve(msg message.ImmutableMessage) bool {
	return msg.VChannel() == m.vchannel || msg.VChannel() == "" || msg.IsPChannelLevel()
}

func (m *VChannelRecoveryModule) visibleSnapshot(baseGrowingTimeTick uint64, dataVersion qviews.DataVersion) walview.VisibleSegmentSnapshot {
	snapshot := walview.VisibleSegmentSnapshot{
		VChannel:            m.vchannel,
		DataVersion:         dataVersion,
		BaseGrowingTimeTick: baseGrowingTimeTick,
	}
	for _, view := range m.segments {
		visible, ok := view.VisibleSnapshot(m.vchannel, dataVersion)
		if ok {
			if snapshot.CollectionID == 0 {
				snapshot.CollectionID = visible.Assignment.GetCollectionId()
			}
			snapshot.Segments = append(snapshot.Segments, visible)
			continue
		}
		flushed, ok := view.FlushedSegmentSnapshot(m.vchannel, dataVersion)
		if ok {
			snapshot.FlushedSegments = append(snapshot.FlushedSegments, flushed)
		}
	}
	return snapshot
}

func (m *VChannelRecoveryModule) segmentSnapshotDataVersion() qviews.DataVersion {
	dataVersion := m.vchannelView.SegmentDataVersionSummary()
	for _, view := range m.segments {
		sealedVersion, ok := view.SealedDataVersion(m.vchannel)
		if ok && sealedVersion.GT(dataVersion) {
			dataVersion = sealedVersion
		}
	}
	return dataVersion
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

func (m *VChannelRecoveryModule) SegmentSealed(event walview.SegmentSealedEvent) {
	m.observeQueryResourceEvent(context.Background(), walview.VChannelResourceEvent{SegmentSealed: &event})
	m.mu.Lock()
	m.queryResources.TryBuildLocked(m.queryWALViewLocked)
	m.mu.Unlock()
	if m.externalOnSegmentSealed != nil {
		m.externalOnSegmentSealed(event)
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
	oldest, hasOldest := m.queryResources.OldestDataVersion()
	if !view.TryFinalizeTombstoneAt(oldest, hasOldest) {
		return false
	}
	m.advanceSegmentDataVersionSummaryLocked(view)
	m.markSegmentDirty(segmentID, view)
	return true
}

func (m *VChannelRecoveryModule) tryFinalizeSegmentsLocked() bool {
	changed := false
	for segmentID, view := range m.segments {
		if m.tryFinalizeSegmentLocked(segmentID, view) {
			changed = true
		}
	}
	return changed
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
	return len(m.cleanupSegments) > 0 || len(m.pendingCleanup) > 0
}

func (m *VChannelRecoveryModule) ConsumeCleanupSnapshots(cleanup moduleapi.CleanupContext) []moduleapi.DirtySnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
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
	return snapshots
}

func (m *VChannelRecoveryModule) advanceSegmentDataVersionSummaryLocked(view *segment.SegmentView) bool {
	vchannel, sealedVersion, ok := view.TombstonedSealedDataVersion()
	if !ok || vchannel != m.vchannel || m.vchannelView == nil {
		return false
	}
	return m.vchannelView.AdvanceSegmentDataVersionSummary(sealedVersion)
}

func (m *VChannelRecoveryModule) segmentCleanupReadyLocked(view *segment.SegmentView, cleanup moduleapi.CleanupContext) bool {
	if m.vchannelView == nil || !view.TombstonedCleanupReady(cleanup.MetaPhysicalTimeTick, cleanup.DataPhysicalTimeTick) {
		return false
	}
	_, sealedVersion, ok := view.TombstonedSealedDataVersion()
	return ok && m.vchannelView.PersistedSegmentDataVersionSummary().GTE(sealedVersion)
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

func deleteReplayStartAfter(snapshot walview.VisibleSegmentSnapshot) uint64 {
	if len(snapshot.Segments) == 0 {
		return 0
	}
	minCreateTimeTick := uint64(0)
	for _, segment := range snapshot.Segments {
		createTimeTick := segment.Assignment.GetStat().GetCreateSegmentTimeTick()
		if createTimeTick == 0 {
			continue
		}
		if minCreateTimeTick == 0 || createTimeTick < minCreateTimeTick {
			minCreateTimeTick = createTimeTick
		}
	}
	if minCreateTimeTick == 0 {
		return 0
	}
	return minCreateTimeTick - 1
}

func max(a, b uint64) uint64 {
	if a >= b {
		return a
	}
	return b
}
