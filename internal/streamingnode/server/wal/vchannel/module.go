package vchannel

import (
	"context"
	"math"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	walcheckpoint "github.com/milvus-io/milvus/internal/streamingnode/server/wal/checkpoint"
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

	VChannelMeta              *streamingpb.VChannelMeta
	Segments                  map[int64]*streamingpb.SegmentAssignmentMeta
	SegmentDataVersionSummary *streamingpb.SegmentDataVersionSummary
	TransformLogMeta          *streamingpb.VChannelTransformLogMeta

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
	OnFrontierUpdated          func()
}

// VChannelRecoveryModule owns all recovery_storage state for one vchannel.
type VChannelRecoveryModule struct {
	// mu serializes WAL observation with snapshots and frontier reads. In
	// particular, segments may grow when CreateSegment is observed while the
	// recovery background task is collecting dirty snapshots.
	mu       sync.Mutex
	pchannel string
	vchannel string

	runtime moduleapi.Runtime
	logger  *mlog.Logger

	vchannelView     *VChannelView
	segments         map[int64]*segment.SegmentView
	dirtyMu          sync.Mutex
	dirtySegments    map[int64]*segment.SegmentView
	cleanupSegments  map[int64]*segment.SegmentView
	pendingCleanup   map[int64]*segment.SegmentView
	segmentFrontiers *segmentFrontierIndex

	segmentDataVersionSummary *streamingpb.SegmentDataVersionSummary
	latestInsertTimeTick      uint64

	transformLog *transformlog.TransformLog

	segmentLifecycle        segment.Lifecycle
	segmentPackWriter       segment.PackWriter
	externalOnSegmentSealed func(walview.SegmentSealedEvent)
	onFrontierUpdated       func()

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
		pchannel:                  config.PChannel,
		vchannel:                  config.VChannel,
		runtime:                   config.Runtime,
		logger:                    config.Logger,
		segments:                  make(map[int64]*segment.SegmentView),
		segmentFrontiers:          newSegmentFrontierIndex(),
		segmentDataVersionSummary: cloneSegmentDataVersionSummary(config.SegmentDataVersionSummary),
		segmentLifecycle:          config.SegmentLifecycle,
		segmentPackWriter:         config.SegmentPackWriter,
		externalOnSegmentSealed:   config.OnSegmentSealed,
		queryTransformLogStream:   config.TransformLogStream,
		onFrontierUpdated:         config.OnFrontierUpdated,
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
		var view *segment.SegmentView
		view = segment.NewSegmentViewFromMetaWithConfig(meta, schema, module.segmentViewConfig())
		module.segments[id] = view
		if view.TombstonePersisted() {
			if module.cleanupSegments == nil {
				module.cleanupSegments = make(map[int64]*segment.SegmentView)
			}
			module.cleanupSegments[id] = view
		}
		module.segmentFrontiers.Update(id, view.CollectionID(), view.PartitionID(), view.DurableFrontierTimeTick())
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

func (m *VChannelRecoveryModule) Name() moduleapi.ModuleName {
	return moduleapi.ModuleNameVChannel
}

func (m *VChannelRecoveryModule) ObserveMessage(ctx context.Context, msg message.ImmutableMessage) moduleapi.ObserveResult {
	if m == nil || msg == nil || !m.shouldObserve(msg) {
		return moduleapi.ObserveResult{}
	}
	if funcutil.IsControlChannel(msg.VChannel()) && !msg.IsPChannelLevel() {
		return moduleapi.ObserveResult{}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var result moduleapi.ObserveResult
	switch msg.MessageType() {
	case message.MessageTypeCreateCollection:
		result = m.handleCreateCollectionMessage(message.MustAsImmutableCreateCollectionMessageV1(msg))
	case message.MessageTypeCreatePartition:
		result = m.handleCreatePartitionMessage(message.MustAsImmutableCreatePartitionMessageV1(msg))
	case message.MessageTypeSchemaChange:
		result = m.handleSchemaChangeMessage(message.MustAsImmutableSchemaChangeMessageV2(msg))
	case message.MessageTypeAlterCollection:
		result = m.handleAlterCollectionMessage(ctx, message.MustAsImmutableAlterCollectionMessageV2(msg))
	case message.MessageTypeDropCollection:
		result = m.handleDropCollectionMessage(ctx, message.MustAsImmutableDropCollectionMessageV1(msg))
	case message.MessageTypeDropPartition:
		result = m.handleDropPartitionMessage(ctx, message.MustAsImmutableDropPartitionMessageV1(msg))
	case message.MessageTypeTruncateCollection:
		result = m.handleTruncateCollectionMessage(ctx, message.MustAsImmutableTruncateCollectionMessageV2(msg))
	case message.MessageTypeAlterLoadConfig:
		result = m.handleAlterLoadConfigMessage(message.MustAsImmutableAlterLoadConfigMessageV2(msg))
	case message.MessageTypeDropLoadConfig:
		result = m.handleDropLoadConfigMessage(message.MustAsImmutableDropLoadConfigMessageV2(msg))
	case message.MessageTypeCreateSegment:
		result = m.handleCreateSegmentMessage(ctx, message.MustAsImmutableCreateSegmentMessageV2(msg))
	case message.MessageTypeInsert:
		result = m.handleInsertMessage(ctx, message.MustAsImmutableInsertMessageV1(msg))
	case message.MessageTypeTxn:
		result = m.handleTxnMessage(ctx, message.AsImmutableTxnMessage(msg))
	case message.MessageTypeFlush:
		result = m.handleFlushMessage(ctx, message.MustAsImmutableFlushMessageV2(msg))
	case message.MessageTypeManualFlush:
		result = m.handleManualFlushMessage(ctx, msg)
	case message.MessageTypeFlushAll:
		result = m.handleFlushAllMessage(ctx, msg)
	case message.MessageTypeAlterWAL:
		result = m.handleAlterWALMessage(ctx, msg)
	case message.MessageTypeDelete:
		result = moduleapi.ObserveResult{}
	case message.MessageTypeRecoveryBarrier:
		result = moduleapi.ObserveResult{}
	default:
		result = moduleapi.ObserveResult{}
	}
	if m.transformLog != nil {
		result = composeObserveResults(result, m.transformLog.ObserveMessage(ctx, msg))
	}
	m.observeQueryResourceEvent(ctx, walview.VChannelResourceEvent{Message: msg})
	return result
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
		m.refreshSegmentFrontierLocked(id, view)
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
		if meta := m.vchannelView.ConsumeDirtyAndGetSnapshot(); meta != nil {
			owner := m.vchannelView
			snapshot := meta
			snapshots = append(snapshots, newDirtySnapshot(
				moduleapi.ModuleNameVChannel,
				moduleapi.SnapshotKey{PChannel: m.pchannel, VChannel: m.vchannel},
				moduleapi.SnapshotOpUpsert,
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

func (m *VChannelRecoveryModule) DataFrontier(scope moduleapi.Scope) walcheckpoint.Barrier {
	if m == nil || !m.matchesScope(scope) {
		return nil
	}
	return walcheckpoint.BarrierFunc(func() uint64 {
		m.mu.Lock()
		defer m.mu.Unlock()
		segmentTimeTick := m.segmentFrontierTimeTick(scope)
		transformTimeTick := m.transformFrontierTimeTick(scope.Kind)
		if segmentTimeTick < transformTimeTick {
			return segmentTimeTick
		}
		return transformTimeTick
	})
}

func (m *VChannelRecoveryModule) IsActive() bool {
	if m == nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.vchannelView != nil && m.vchannelView.IsActive()
}

func (m *VChannelRecoveryModule) handleCreateCollectionMessage(msg message.ImmutableCreateCollectionMessageV1) moduleapi.ObserveResult {
	vchannelResult := moduleapi.ObserveResult{}
	if m.vchannelView == nil {
		m.vchannelView = NewVChannelViewFromCreateCollectionMessage(msg)
		if m.metaAndData {
			m.vchannelView.SwitchIntoMetaAndData()
		}
		vchannelResult.Meta = m.vchannelView.MetaBarrier()
	} else {
		replacement, result := m.vchannelView.ObserveCreateCollectionMessageV1(msg)
		if replacement != nil {
			m.vchannelView = replacement
			vchannelResult.Meta = m.vchannelView.MetaBarrier()
		} else {
			vchannelResult = result
		}
	}
	return vchannelResult
}

func (m *VChannelRecoveryModule) handleCreatePartitionMessage(msg message.ImmutableCreatePartitionMessageV1) moduleapi.ObserveResult {
	if m.vchannelView == nil {
		return moduleapi.ObserveResult{}
	}
	return m.vchannelView.ObserveCreatePartitionMessageV1(msg)
}

func (m *VChannelRecoveryModule) handleSchemaChangeMessage(msg message.ImmutableSchemaChangeMessageV2) moduleapi.ObserveResult {
	if m.vchannelView == nil {
		return moduleapi.ObserveResult{}
	}
	return m.vchannelView.ObserveSchemaChangeMessageV2(msg)
}

func (m *VChannelRecoveryModule) handleAlterCollectionMessage(ctx context.Context, msg message.ImmutableAlterCollectionMessageV2) moduleapi.ObserveResult {
	result := moduleapi.ObserveResult{}
	if m.vchannelView != nil {
		result = composeObserveResults(result, m.vchannelView.ObserveAlterCollectionMessageV2(msg))
	}
	if messageutil.IsSchemaChange(msg.Header()) {
		result = composeObserveResults(result, m.flushAllSegmentsCreatedBefore(ctx, msg.TimeTick()))
	}
	return result
}

func (m *VChannelRecoveryModule) handleDropCollectionMessage(ctx context.Context, msg message.ImmutableDropCollectionMessageV1) moduleapi.ObserveResult {
	result := moduleapi.ObserveResult{}
	if m.vchannelView != nil {
		result = composeObserveResults(result, m.vchannelView.ObserveDropCollectionMessageV1(msg))
	}
	result = composeObserveResults(result, m.flushAllSegmentsCreatedBefore(ctx, msg.TimeTick()))
	return result
}

func (m *VChannelRecoveryModule) handleDropPartitionMessage(ctx context.Context, msg message.ImmutableDropPartitionMessageV1) moduleapi.ObserveResult {
	result := moduleapi.ObserveResult{}
	if m.vchannelView != nil {
		result = composeObserveResults(result, m.vchannelView.ObserveDropPartitionMessageV1(msg))
	}
	result = composeObserveResults(result, m.flushPartitionSegmentsCreatedBefore(ctx, msg.TimeTick(), msg.Header().GetPartitionId()))
	return result
}

func (m *VChannelRecoveryModule) handleTruncateCollectionMessage(ctx context.Context, msg message.ImmutableTruncateCollectionMessageV2) moduleapi.ObserveResult {
	result := moduleapi.ObserveResult{}
	if m.vchannelView != nil {
		result = composeObserveResults(result, m.vchannelView.ObserveTruncateCollectionMessageV2(msg))
	}
	result = composeObserveResults(result, m.flushAllSegmentsCreatedBefore(ctx, msg.TimeTick()))
	return result
}

func (m *VChannelRecoveryModule) handleAlterLoadConfigMessage(msg message.ImmutableAlterLoadConfigMessageV2) moduleapi.ObserveResult {
	if m.vchannelView == nil {
		return moduleapi.ObserveResult{}
	}
	return m.vchannelView.ObserveAlterLoadConfigMessageV2(msg)
}

func (m *VChannelRecoveryModule) handleDropLoadConfigMessage(msg message.ImmutableDropLoadConfigMessageV2) moduleapi.ObserveResult {
	if m.vchannelView == nil {
		return moduleapi.ObserveResult{}
	}
	return m.vchannelView.ObserveDropLoadConfigMessageV2(msg)
}

func (m *VChannelRecoveryModule) handleCreateSegmentMessage(ctx context.Context, msg message.ImmutableCreateSegmentMessageV2) moduleapi.ObserveResult {
	id := msg.Header().GetSegmentId()
	view := m.segments[id]
	result := moduleapi.ObserveResult{}
	if view == nil {
		var schema *schemapb.CollectionSchema
		if m.vchannelView != nil {
			schema = m.vchannelView.CreateSegmentSchema(msg.Header().GetPartitionId(), msg.TimeTick())
		}
		if schema == nil {
			return result
		}
		view = segment.NewSegmentViewFromCreateSegmentMessageWithConfig(msg, schema, m.segmentViewConfig())
		if m.metaAndData {
			view.SwitchIntoMetaAndData()
		}
		m.segments[id] = view
		result.Meta = view.MetaBarrier()
	}
	result = composeObserveResults(result, view.ObserveCreateSegmentMessageV2(ctx, msg))
	m.markSegmentUpdatedLocked(id)
	return result
}

func (m *VChannelRecoveryModule) handleInsertMessage(ctx context.Context, msg message.ImmutableInsertMessageV1) moduleapi.ObserveResult {
	result := moduleapi.ObserveResult{}
	for _, partition := range msg.Header().GetPartitions() {
		view := m.segments[partition.GetSegmentAssignment().GetSegmentId()]
		if view == nil {
			continue
		}
		result = composeObserveResults(result, view.ObserveInsertMessageV1(ctx, msg, partition))
		m.markSegmentUpdatedLocked(view.ID())
	}
	m.markLatestInsertTimeTick(msg.VChannel(), msg.TimeTick(), result)
	return result
}

func (m *VChannelRecoveryModule) handleTxnMessage(ctx context.Context, msg message.ImmutableTxnMessage) moduleapi.ObserveResult {
	if msg == nil {
		return moduleapi.ObserveResult{}
	}
	result := moduleapi.ObserveResult{}
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
			result = composeObserveResults(result, view.ObserveTxnMessage(ctx, msg))
			m.markSegmentUpdatedLocked(id)
		}
		return nil
	})
	m.markLatestInsertTimeTick(msg.VChannel(), msg.TimeTick(), result)
	return result
}

func (m *VChannelRecoveryModule) handleFlushMessage(ctx context.Context, msg message.ImmutableFlushMessageV2) moduleapi.ObserveResult {
	id := msg.Header().GetSegmentId()
	if segment := m.segments[id]; segment != nil {
		result := segment.Flush(ctx, msg.TimeTick())
		m.markSegmentUpdatedLocked(id)
		return result
	}
	return moduleapi.ObserveResult{}
}

func (m *VChannelRecoveryModule) handleManualFlushMessage(ctx context.Context, msg message.ImmutableMessage) moduleapi.ObserveResult {
	return m.flushAllSegmentsCreatedBefore(ctx, msg.TimeTick())
}

func (m *VChannelRecoveryModule) handleFlushAllMessage(ctx context.Context, msg message.ImmutableMessage) moduleapi.ObserveResult {
	return m.flushAllSegmentsCreatedBefore(ctx, msg.TimeTick())
}

func (m *VChannelRecoveryModule) handleAlterWALMessage(ctx context.Context, msg message.ImmutableMessage) moduleapi.ObserveResult {
	return m.flushAllSegmentsCreatedBefore(ctx, msg.TimeTick())
}

func (m *VChannelRecoveryModule) flushAllSegmentsCreatedBefore(ctx context.Context, timetick uint64) moduleapi.ObserveResult {
	result := moduleapi.ObserveResult{}
	for _, view := range m.segments {
		if view.CreateTimeTick() >= timetick {
			continue
		}
		result = composeObserveResults(result, view.Flush(ctx, timetick))
		m.markSegmentUpdatedLocked(view.ID())
	}
	return result
}

func (m *VChannelRecoveryModule) flushPartitionSegmentsCreatedBefore(ctx context.Context, timetick uint64, partitionID int64) moduleapi.ObserveResult {
	result := moduleapi.ObserveResult{}
	for _, view := range m.segments {
		if view.PartitionID() != partitionID || view.CreateTimeTick() >= timetick {
			continue
		}
		result = composeObserveResults(result, view.Flush(ctx, timetick))
		m.markSegmentUpdatedLocked(view.ID())
	}
	return result
}

func (m *VChannelRecoveryModule) shouldObserve(msg message.ImmutableMessage) bool {
	return msg.VChannel() == m.vchannel || msg.VChannel() == "" || msg.IsPChannelLevel()
}

func (m *VChannelRecoveryModule) matchesScope(scope moduleapi.Scope) bool {
	switch scope.Type {
	case moduleapi.ScopeAll:
		return true
	case moduleapi.ScopeVChannel, moduleapi.ScopePartition:
		return scope.VChannel == "" || scope.VChannel == m.vchannel
	default:
		return false
	}
}

func (m *VChannelRecoveryModule) segmentDataVersionSummaries() map[string]*streamingpb.SegmentDataVersionSummary {
	if m.segmentDataVersionSummary == nil {
		return nil
	}
	return map[string]*streamingpb.SegmentDataVersionSummary{
		m.vchannel: cloneSegmentDataVersionSummary(m.segmentDataVersionSummary),
	}
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
	dataVersion := segmentDataVersionSummary(m.segmentDataVersionSummary)
	for _, view := range m.segments {
		sealedVersion, ok := view.SealedDataVersion(m.vchannel)
		if ok && sealedVersion.GT(dataVersion) {
			dataVersion = sealedVersion
		}
	}
	return dataVersion
}

func (m *VChannelRecoveryModule) markLatestInsertTimeTick(vchannel string, timetick uint64, result moduleapi.ObserveResult) {
	if vchannel != m.vchannel || (result.Meta == nil && result.Data == nil) {
		return
	}
	if timetick > m.latestInsertTimeTick {
		m.latestInsertTimeTick = timetick
	}
}

func (m *VChannelRecoveryModule) segmentFrontierTimeTick(scope moduleapi.Scope) uint64 {
	if scope.Type == moduleapi.ScopePartition {
		return m.segmentFrontiers.Partition(scope.CollectionID, scope.PartitionID)
	}
	return m.segmentFrontiers.All()
}

func (m *VChannelRecoveryModule) dataFrontierTimeTick(kind moduleapi.DataProgressKind) uint64 {
	return min(m.segmentFrontiers.All(), m.transformFrontierTimeTick(kind))
}

func (m *VChannelRecoveryModule) markSegmentUpdatedLocked(segmentID int64) {
	view := m.segments[segmentID]
	m.markSegmentViewUpdatedLocked(segmentID, view)
}

func (m *VChannelRecoveryModule) SegmentDataUpdated(segmentID int64, view *segment.SegmentView) {
	m.markSegmentViewUpdated(segmentID, view)
	if m.runtime.Notifier != nil {
		m.runtime.Notifier.NotifyBarrierUpdated()
	}
}

func (m *VChannelRecoveryModule) SegmentSealed(event walview.SegmentSealedEvent) {
	m.observeQueryResourceEvent(context.Background(), walview.VChannelResourceEvent{SegmentSealed: &event})
	if m.externalOnSegmentSealed != nil {
		m.externalOnSegmentSealed(event)
	}
}

func (m *VChannelRecoveryModule) markSegmentViewUpdated(segmentID int64, view *segment.SegmentView) {
	m.mu.Lock()
	m.markSegmentViewUpdatedLocked(segmentID, view)
	m.mu.Unlock()
	m.notifyFrontierUpdated()
}

func (m *VChannelRecoveryModule) markSegmentViewUpdatedLocked(segmentID int64, view *segment.SegmentView) {
	if view == nil {
		return
	}
	m.tryFinalizeSegmentLocked(segmentID, view)
	m.markSegmentDirty(segmentID, view)
	m.refreshSegmentFrontierLocked(segmentID, view)
}

func (m *VChannelRecoveryModule) tryFinalizeSegmentLocked(segmentID int64, view *segment.SegmentView) bool {
	oldest, hasOldest := m.queryResources.OldestDataVersion()
	if !view.TryFinalizeTombstoneAt(oldest, hasOldest) {
		return false
	}
	m.markSegmentDirty(segmentID, view)
	return true
}

func (m *VChannelRecoveryModule) tryFinalizeSegmentsLocked() bool {
	changed := false
	for segmentID, view := range m.segments {
		if m.tryFinalizeSegmentLocked(segmentID, view) {
			m.refreshSegmentFrontierLocked(segmentID, view)
			changed = true
		}
	}
	return changed
}

func (m *VChannelRecoveryModule) markTransformSnapshotPersisted(snapshot *streamingpb.VChannelTransformLogMeta) {
	m.mu.Lock()
	m.transformLog.MarkSnapshotPersisted(snapshot)
	m.mu.Unlock()
	m.notifyFrontierUpdated()
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
	m.refreshSegmentFrontierLocked(segmentID, view)
	m.mu.Unlock()
	if tombstonePersisted && m.runtime.Notifier != nil {
		m.runtime.Notifier.NotifyModuleUpdated(moduleapi.ModuleNameSegment)
	}
	m.notifyFrontierUpdated()
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
		if !view.TombstonedCleanupReady(cleanup.MetaPhysicalTimeTick, cleanup.DataPhysicalTimeTick) {
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

func (m *VChannelRecoveryModule) completeSegmentCleanup(segmentID int64, view *segment.SegmentView) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.pendingCleanup[segmentID] != view || m.segments[segmentID] != view {
		return
	}
	delete(m.pendingCleanup, segmentID)
	delete(m.segments, segmentID)
	m.segmentFrontiers.Remove(segmentID)
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

func (m *VChannelRecoveryModule) refreshSegmentFrontierLocked(segmentID int64, view *segment.SegmentView) {
	if view == nil {
		return
	}
	m.segmentFrontiers.Update(segmentID, view.CollectionID(), view.PartitionID(), view.DurableFrontierTimeTick())
}

func (m *VChannelRecoveryModule) notifyFrontierUpdated() {
	if m.onFrontierUpdated != nil {
		m.onFrontierUpdated()
	}
}

func (m *VChannelRecoveryModule) transformFrontierTimeTick(kind moduleapi.DataProgressKind) uint64 {
	if kind == moduleapi.DataProgressMaterialized {
		if m.transformLog.HasDirty() || m.transformLog.HasPendingMaterializeTask() {
			return m.transformLog.MaterializedBarrierTimeTick()
		}
		return math.MaxUint64
	}
	if m.transformLog.HasDirty() || m.transformLog.HasPendingWork() || m.transformLog.HasPendingFlushTask() {
		return m.transformLog.DataBarrierTimeTick()
	}
	return math.MaxUint64
}

func cloneSegmentDataVersionSummary(summary *streamingpb.SegmentDataVersionSummary) *streamingpb.SegmentDataVersionSummary {
	if summary == nil {
		return nil
	}
	return proto.Clone(summary).(*streamingpb.SegmentDataVersionSummary)
}

func segmentDataVersionSummary(summary *streamingpb.SegmentDataVersionSummary) qviews.DataVersion {
	if summary == nil || summary.GetDataVersion() == nil {
		return qviews.DataVersion{}
	}
	return qviews.FromProtoDataVersion(summary.GetDataVersion())
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

func composeObserveResults(left moduleapi.ObserveResult, right moduleapi.ObserveResult) moduleapi.ObserveResult {
	return moduleapi.ComposeBarriers([]moduleapi.ObserveResult{left, right})
}

func max(a, b uint64) uint64 {
	if a >= b {
		return a
	}
	return b
}

var (
	_ moduleapi.Module               = (*VChannelRecoveryModule)(nil)
	_ moduleapi.DataFrontierProvider = (*VChannelRecoveryModule)(nil)
)
