package segment

import (
	"context"
	"math"
	"sync"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	walcheckpoint "github.com/milvus-io/milvus/internal/streamingnode/server/wal/checkpoint"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

type SchemaProvider interface {
	SchemaAt(vchannel string, partitionID int64, timetick uint64) (*schemapb.CollectionSchema, bool)
}

type Module struct {
	mu                      sync.Mutex
	pchannel                string
	segments                map[int64]*segmentView
	lifecycle               segmentLifecycle
	packWriter              packWriter
	runtime                 moduleapi.Runtime
	logger                  *mlog.Logger
	metaAndData             bool
	schemaProvider          SchemaProvider
	pendingCleanupSnapshots map[int64]*dirtySnapshot
	persistedMetaPhysicalTT uint64
	persistedDataPhysicalTT uint64
}

type runtimeConfig struct {
	lifecycle     segmentLifecycle
	packWriter    packWriter
	runtime       moduleapi.Runtime
	onDataUpdated func()
	flushPolicy   flushPolicy
	metaAndData   bool
}

func firstRuntimeConfig(configs []runtimeConfig) runtimeConfig {
	if len(configs) == 0 {
		return runtimeConfig{}
	}
	return configs[0]
}

type ModuleOption func(*Module)

func WithPackWriter(writer packWriter) ModuleOption {
	return func(module *Module) {
		module.packWriter = writer
	}
}

func WithModuleRuntime(logger *mlog.Logger, runtime moduleapi.Runtime) ModuleOption {
	return func(module *Module) {
		module.logger = logger
		module.runtime = runtime
	}
}

func NewModule(
	pchannel string,
	segments map[int64]*streamingpb.SegmentAssignmentMeta,
	schemaProvider SchemaProvider,
	lifecycle segmentLifecycle,
	opts ...ModuleOption,
) *Module {
	if segments == nil {
		segments = make(map[int64]*streamingpb.SegmentAssignmentMeta)
	}
	module := &Module{
		pchannel:                pchannel,
		segments:                make(map[int64]*segmentView, len(segments)),
		schemaProvider:          schemaProvider,
		lifecycle:               lifecycle,
		pendingCleanupSnapshots: make(map[int64]*dirtySnapshot),
	}
	for _, opt := range opts {
		opt(module)
	}
	for _, meta := range segments {
		var schema *schemapb.CollectionSchema
		if module.schemaProvider != nil {
			schema, _ = module.schemaProvider.SchemaAt(meta.GetVchannel(), meta.GetPartitionId(), meta.GetStat().GetCreateSegmentTimeTick())
		}
		module.segments[meta.GetSegmentId()] = newSegmentViewFromMeta(meta, schema, module.runtimeConfig())
	}
	return module
}

func (m *Module) runtimeConfig() runtimeConfig {
	return runtimeConfig{
		lifecycle:     m.lifecycle,
		packWriter:    m.packWriter,
		runtime:       m.runtime,
		onDataUpdated: m.notifyModuleUpdated,
		metaAndData:   m.metaAndData,
	}
}

func (m *Module) Name() moduleapi.ModuleName {
	return moduleapi.ModuleNameSegment
}

func (m *Module) ObserveMessage(ctx context.Context, msg message.ImmutableMessage) moduleapi.ObserveResult {
	if funcutil.IsControlChannel(msg.VChannel()) && !msg.IsPChannelLevel() {
		return moduleapi.ObserveResult{}
	}
	switch msg.MessageType() {
	case message.MessageTypeCreateSegment:
		return m.observeCreateSegmentMessage(ctx, message.MustAsImmutableCreateSegmentMessageV2(msg))
	case message.MessageTypeInsert:
		return m.observeInsertMessage(ctx, message.MustAsImmutableInsertMessageV1(msg))
	case message.MessageTypeFlush:
		return m.observeFlushMessage(ctx, message.MustAsImmutableFlushMessageV2(msg))
	case message.MessageTypeManualFlush:
		return m.flushVChannelSegmentsCreatedBefore(ctx, msg.TimeTick(), msg.VChannel())
	case message.MessageTypeFlushAll:
		return m.flushSegmentsCreatedBefore(ctx, msg.TimeTick(), func(*segmentView) bool { return true })
	case message.MessageTypeDropCollection, message.MessageTypeTruncateCollection:
		return m.flushVChannelSegmentsCreatedBefore(ctx, msg.TimeTick(), msg.VChannel())
	case message.MessageTypeDropPartition:
		drop := message.MustAsImmutableDropPartitionMessageV1(msg)
		return m.flushPartitionSegmentsCreatedBefore(ctx, msg.TimeTick(), msg.VChannel(), drop.Header().GetPartitionId())
	case message.MessageTypeAlterCollection:
		alter := message.MustAsImmutableAlterCollectionMessageV2(msg)
		if messageutil.IsSchemaChange(alter.Header()) {
			return m.flushVChannelSegmentsCreatedBefore(ctx, msg.TimeTick(), msg.VChannel())
		}
	case message.MessageTypeAlterWAL:
		return m.flushSegmentsCreatedBefore(ctx, msg.TimeTick(), func(*segmentView) bool { return true })
	case message.MessageTypeTxn:
		return m.observeTxnMessage(ctx, message.AsImmutableTxnMessage(msg))
	}
	return moduleapi.ObserveResult{}
}

func (m *Module) SwitchIntoMetaAndData() moduleapi.ModuleSnapshot {
	m.mu.Lock()
	m.metaAndData = true
	for _, segment := range m.segments {
		segment.SwitchIntoMetaAndData()
	}
	m.mu.Unlock()
	return &moduleapi.SegmentModuleSnapshot{Segments: m.snapshotGrowingSegments()}
}

func (m *Module) ConsumeDirtySnapshots() []moduleapi.DirtySnapshot {
	m.finalizeTombstones()
	segments := m.snapshotSegments()
	metaPhysical, dataPhysical := m.persistedPhysicalTimeTicks()
	snapshots := make([]moduleapi.DirtySnapshot, 0, len(segments))
	for segmentID, segment := range segments {
		if meta := segment.ConsumeDirtyAndGetSnapshot(); meta != nil {
			owner := segment
			snapshotMeta := meta
			snapshots = append(snapshots, newDirtySnapshot(
				moduleapi.SnapshotKey{PChannel: m.pchannel, SegmentID: segmentID},
				moduleapi.SnapshotOpUpsert,
				snapshotMeta,
				snapshotMeta.GetCheckpointTimeTick(),
				snapshotMeta.GetDataCheckpointTimeTick(),
				func() { owner.MarkSnapshotPersisted(snapshotMeta) },
			))
			continue
		}
		if segment.TombstonedCleanupReady(metaPhysical, dataPhysical) {
			snapshots = append(snapshots, m.cleanupSnapshot(segmentID, segment))
		}
	}
	return snapshots
}

func (m *Module) DataFrontier(scope moduleapi.Scope) walcheckpoint.Barrier {
	owners := make(frontierOwners, 0)
	for _, segment := range m.snapshotSegments() {
		meta := segment.AssignmentMeta()
		switch scope.Type {
		case moduleapi.ScopeAll:
		case moduleapi.ScopeVChannel:
			if meta.GetVchannel() != scope.VChannel {
				continue
			}
		case moduleapi.ScopePartition:
			if scope.VChannel != "" && meta.GetVchannel() != scope.VChannel {
				continue
			}
			if meta.GetCollectionId() != scope.CollectionID || meta.GetPartitionId() != scope.PartitionID {
				continue
			}
		default:
			continue
		}
		owners = append(owners, segment)
	}
	return owners
}

func (m *Module) NotifyCheckpointPersisted(metaTimeTick uint64, dataTimeTick uint64) {
	m.mu.Lock()
	m.persistedMetaPhysicalTT = metaTimeTick
	m.persistedDataPhysicalTT = dataTimeTick
	m.mu.Unlock()
	m.notifyModuleUpdated()
}

func (m *Module) observeCreateSegmentMessage(ctx context.Context, msg message.ImmutableCreateSegmentMessageV2) moduleapi.ObserveResult {
	segment := m.getSegment(msg.Header().GetSegmentId())
	result := moduleapi.ObserveResult{}
	if segment == nil {
		schema, ok := m.schemaAt(msg.VChannel(), msg.Header().GetPartitionId(), msg.TimeTick())
		if !ok {
			m.logInconsistency(msg, "create segment schema not found", mlog.String("vchannel", msg.VChannel()), mlog.Int64("partitionID", msg.Header().GetPartitionId()), mlog.Int64("segmentID", msg.Header().GetSegmentId()))
			return result
		}
		segment = newSegmentViewFromCreateSegmentMessage(msg, schema, m.runtimeConfig())
		m.addSegment(segment)
		result.Meta = segment.metaBarrier()
	}
	return composeObserveResults(result, segment.ObserveCreateSegmentMessageV2(ctx, msg))
}

func (m *Module) observeInsertMessage(ctx context.Context, msg message.ImmutableInsertMessageV1) moduleapi.ObserveResult {
	result := moduleapi.ObserveResult{}
	for _, partition := range msg.Header().GetPartitions() {
		segmentID := partition.GetSegmentAssignment().GetSegmentId()
		segment := m.getSegment(segmentID)
		if segment == nil {
			continue
		}
		result = composeObserveResults(result, segment.ObserveInsertMessageV1(ctx, msg, partition))
	}
	return result
}

func (m *Module) observeTxnMessage(ctx context.Context, msg message.ImmutableTxnMessage) moduleapi.ObserveResult {
	result := moduleapi.ObserveResult{}
	observedSegments := make(map[int64]struct{})
	msg.RangeOver(func(im message.ImmutableMessage) error {
		if im.MessageType() != message.MessageTypeInsert {
			return nil
		}
		insert := message.MustAsImmutableInsertMessageV1(im)
		for _, partition := range insert.Header().GetPartitions() {
			segmentID := partition.GetSegmentAssignment().GetSegmentId()
			if _, observed := observedSegments[segmentID]; observed {
				continue
			}
			segment := m.getSegment(segmentID)
			if segment == nil {
				continue
			}
			observedSegments[segmentID] = struct{}{}
			result = composeObserveResults(result, segment.ObserveTxnMessage(ctx, msg))
		}
		return nil
	})
	return result
}

func (m *Module) observeFlushMessage(ctx context.Context, msg message.ImmutableFlushMessageV2) moduleapi.ObserveResult {
	segment := m.getSegment(msg.Header().GetSegmentId())
	if segment == nil {
		m.logInconsistency(msg, "flush segment not found", mlog.Int64("segmentID", msg.Header().GetSegmentId()))
		return moduleapi.ObserveResult{}
	}
	return segment.Flush(ctx, msg.TimeTick())
}

func (m *Module) flushVChannelSegmentsCreatedBefore(ctx context.Context, timetick uint64, vchannel string) moduleapi.ObserveResult {
	return m.flushSegmentsCreatedBefore(ctx, timetick, func(segment *segmentView) bool {
		return segment.AssignmentMeta().GetVchannel() == vchannel
	})
}

func (m *Module) flushPartitionSegmentsCreatedBefore(ctx context.Context, timetick uint64, vchannel string, partitionID int64) moduleapi.ObserveResult {
	return m.flushSegmentsCreatedBefore(ctx, timetick, func(segment *segmentView) bool {
		meta := segment.AssignmentMeta()
		return meta.GetVchannel() == vchannel && meta.GetPartitionId() == partitionID
	})
}

func (m *Module) flushSegmentsCreatedBefore(ctx context.Context, timetick uint64, match func(*segmentView) bool) moduleapi.ObserveResult {
	result := moduleapi.ObserveResult{}
	for _, segment := range m.snapshotSegments() {
		if !match(segment) || segment.CreateTimeTick() >= timetick {
			continue
		}
		result = composeObserveResults(result, segment.Flush(ctx, timetick))
	}
	return result
}

func (m *Module) schemaAt(vchannel string, partitionID int64, timetick uint64) (*schemapb.CollectionSchema, bool) {
	if m.schemaProvider == nil {
		return nil, false
	}
	return m.schemaProvider.SchemaAt(vchannel, partitionID, timetick)
}

func (m *Module) addSegment(segment *segmentView) {
	meta := segment.AssignmentMeta()
	m.mu.Lock()
	m.segments[meta.GetSegmentId()] = segment
	m.mu.Unlock()
}

func (m *Module) getSegment(segmentID int64) *segmentView {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.segments[segmentID]
}

func (m *Module) removeSegmentIfOwner(segmentID int64, owner *segmentView) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.segments[segmentID] == owner {
		delete(m.segments, segmentID)
	}
}

func (m *Module) cleanupSnapshot(segmentID int64, owner *segmentView) moduleapi.DirtySnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.pendingCleanupSnapshots == nil {
		m.pendingCleanupSnapshots = make(map[int64]*dirtySnapshot)
	}
	if snapshot := m.pendingCleanupSnapshots[segmentID]; snapshot != nil {
		return snapshot
	}
	snapshot := newDirtySnapshot(
		moduleapi.SnapshotKey{PChannel: m.pchannel, SegmentID: segmentID},
		moduleapi.SnapshotOpDelete,
		nil,
		0,
		0,
		func() { m.markCleanupPersisted(segmentID, owner) },
	)
	m.pendingCleanupSnapshots[segmentID] = snapshot
	return snapshot
}

func (m *Module) markCleanupPersisted(segmentID int64, owner *segmentView) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.pendingCleanupSnapshots, segmentID)
	if m.segments[segmentID] == owner {
		delete(m.segments, segmentID)
	}
}

func (m *Module) persistedPhysicalTimeTicks() (uint64, uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.persistedMetaPhysicalTT, m.persistedDataPhysicalTT
}

func (m *Module) snapshotSegments() map[int64]*segmentView {
	m.mu.Lock()
	defer m.mu.Unlock()
	segments := make(map[int64]*segmentView, len(m.segments))
	for segmentID, segment := range m.segments {
		segments[segmentID] = segment
	}
	return segments
}

func (m *Module) snapshotGrowingSegments() map[int64]*streamingpb.SegmentAssignmentMeta {
	segments := m.snapshotSegments()
	snapshot := make(map[int64]*streamingpb.SegmentAssignmentMeta, len(segments))
	for segmentID, segment := range segments {
		if segment.IsGrowing() {
			snapshot[segmentID] = segment.AssignmentMeta()
		}
	}
	return snapshot
}

func (m *Module) finalizeTombstones() {
	for _, segment := range m.snapshotSegments() {
		if segment.TryFinalizeTombstone() {
			m.notifyModuleUpdated()
		}
	}
}

func (m *Module) notifyModuleUpdated() {
	if m.runtime.Notifier != nil {
		m.runtime.Notifier.NotifyModuleUpdated(moduleapi.ModuleNameSegment)
	}
}

func (m *Module) logInconsistency(msg message.ImmutableMessage, reason string, fields ...mlog.Field) {
	if m.logger == nil {
		return
	}
	fields = append(fields, mlog.FieldMessage(msg))
	m.logger.Warn(context.TODO(), "inconsistent segment observe state", append([]mlog.Field{mlog.String("reason", reason)}, fields...)...)
}

type frontierOwner interface {
	DurableFrontierTimeTick() uint64
}

type frontierOwners []frontierOwner

func (owners frontierOwners) TimeTick() uint64 {
	if len(owners) == 0 {
		return math.MaxUint64
	}
	frontier := uint64(math.MaxUint64)
	for _, owner := range owners {
		if timetick := owner.DurableFrontierTimeTick(); timetick < frontier {
			frontier = timetick
		}
	}
	return frontier
}

func composeBarrier(left walcheckpoint.Barrier, right walcheckpoint.Barrier) walcheckpoint.Barrier {
	if left == nil {
		return right
	}
	if right == nil {
		return left
	}
	return walcheckpoint.NewCompositeBarrier(left, right)
}

func composeObserveResults(results ...moduleapi.ObserveResult) moduleapi.ObserveResult {
	composed := moduleapi.ObserveResult{}
	for _, result := range results {
		composed.Meta = composeBarrier(composed.Meta, result.Meta)
		composed.Data = composeBarrier(composed.Data, result.Data)
	}
	return composed
}

func frontierBefore(timetick uint64) uint64 {
	if timetick == 0 {
		return 0
	}
	return timetick - 1
}

var (
	_ moduleapi.Module                      = (*Module)(nil)
	_ moduleapi.DataFrontierView            = (*Module)(nil)
	_ moduleapi.CheckpointPersistedObserver = (*Module)(nil)
	_ walcheckpoint.Barrier                 = (frontierOwners)(nil)
)
