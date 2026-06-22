package vchannel

import (
	"context"
	"math"
	"sync"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

type SchemaProvider interface {
	SchemaAt(vchannel string, partitionID int64, timetick uint64) (*schemapb.CollectionSchema, bool)
}

type Module struct {
	mu                      sync.Mutex
	pchannel                string
	views                   map[string]*vChannelView
	runtime                 moduleapi.Runtime
	logger                  *mlog.Logger
	metaAndData             bool
	pendingCleanupSnapshots map[string]*dirtySnapshot
	persistedMetaPhysicalTT uint64
	persistedDataPhysicalTT uint64
}

type runtimeConfig struct {
	metaAndData bool
}

func firstRuntimeConfig(configs []runtimeConfig) runtimeConfig {
	if len(configs) == 0 {
		return runtimeConfig{}
	}
	return configs[0]
}

type ModuleOption func(*Module)

func WithModuleRuntime(logger *mlog.Logger, runtime moduleapi.Runtime) ModuleOption {
	return func(m *Module) {
		m.logger = logger
		m.runtime = runtime
	}
}

func NewModule(pchannel string, metas map[string]*streamingpb.VChannelMeta, opts ...ModuleOption) *Module {
	if metas == nil {
		metas = make(map[string]*streamingpb.VChannelMeta)
	}
	module := &Module{
		pchannel:                pchannel,
		views:                   make(map[string]*vChannelView, len(metas)),
		pendingCleanupSnapshots: make(map[string]*dirtySnapshot),
	}
	for _, opt := range opts {
		opt(module)
	}
	for vchannel, meta := range metas {
		module.views[vchannel] = newVChannelViewFromMeta(meta, module.runtimeConfig())
	}
	return module
}

func (m *Module) runtimeConfig() runtimeConfig {
	return runtimeConfig{metaAndData: m.metaAndData}
}

func (m *Module) Name() moduleapi.ModuleName {
	return moduleapi.ModuleNameVChannel
}

func (m *Module) ObserveMessage(_ context.Context, msg message.ImmutableMessage) moduleapi.ObserveResult {
	if funcutil.IsControlChannel(msg.VChannel()) && !msg.IsPChannelLevel() {
		return moduleapi.ObserveResult{}
	}
	switch msg.MessageType() {
	case message.MessageTypeCreateCollection:
		return m.observeCreateCollectionMessage(message.MustAsImmutableCreateCollectionMessageV1(msg))
	case message.MessageTypeCreatePartition:
		return m.observeCreatePartitionMessage(message.MustAsImmutableCreatePartitionMessageV1(msg))
	case message.MessageTypeSchemaChange:
		return m.observeSchemaChangeMessage(message.MustAsImmutableSchemaChangeMessageV2(msg))
	case message.MessageTypeAlterCollection:
		return m.observeAlterCollectionMessage(message.MustAsImmutableAlterCollectionMessageV2(msg))
	case message.MessageTypeDropCollection:
		return m.observeDropCollectionMessage(message.MustAsImmutableDropCollectionMessageV1(msg))
	case message.MessageTypeDropPartition:
		return m.observeDropPartitionMessage(message.MustAsImmutableDropPartitionMessageV1(msg))
	case message.MessageTypeTruncateCollection:
		return m.observeTruncateCollectionMessage(message.MustAsImmutableTruncateCollectionMessageV2(msg))
	case message.MessageTypeAlterLoadConfig:
		return m.observeAlterLoadConfigMessage(message.MustAsImmutableAlterLoadConfigMessageV2(msg))
	case message.MessageTypeDropLoadConfig:
		return m.observeDropLoadConfigMessage(message.MustAsImmutableDropLoadConfigMessageV2(msg))
	default:
		return moduleapi.ObserveResult{}
	}
}

func (m *Module) SwitchIntoMetaAndData() moduleapi.ModuleSnapshot {
	m.mu.Lock()
	m.metaAndData = true
	for _, view := range m.views {
		view.SwitchIntoMetaAndData()
	}
	m.mu.Unlock()
	return &moduleapi.VChannelModuleSnapshot{VChannels: m.snapshotActiveVChannels()}
}

func (m *Module) ConsumeDirtySnapshots() []moduleapi.DirtySnapshot {
	m.finalizeTombstones()
	views := m.snapshotViews()
	metaPhysical, dataPhysical := m.persistedPhysicalTimeTicks()
	snapshots := make([]moduleapi.DirtySnapshot, 0, len(views))
	for vchannel, view := range views {
		if meta := view.ConsumeDirtyAndGetSnapshot(); meta != nil {
			owner := view
			snapshotMeta := meta
			snapshots = append(snapshots, newDirtySnapshot(
				moduleapi.SnapshotKey{PChannel: m.pchannel, VChannel: vchannel},
				moduleapi.SnapshotOpUpsert,
				snapshotMeta,
				snapshotMeta.GetCheckpointTimeTick(),
				func() { owner.MarkSnapshotPersisted(snapshotMeta) },
			))
			continue
		}
		dropSnapshot, cleanupPartitions := view.TombstonedCleanupPlan(
			metaPhysical,
			dataPhysical,
			math.MaxUint64,
		)
		if dropSnapshot != nil {
			snapshots = append(snapshots, m.cleanupSnapshot(vchannel, view, dropSnapshot))
			continue
		}
		if len(cleanupPartitions) > 0 && view.ApplyPartitionCleanup(cleanupPartitions) {
			if meta := view.ConsumeDirtyAndGetSnapshot(); meta != nil {
				owner := view
				snapshotMeta := meta
				snapshots = append(snapshots, newDirtySnapshot(
					moduleapi.SnapshotKey{PChannel: m.pchannel, VChannel: vchannel},
					moduleapi.SnapshotOpUpsert,
					snapshotMeta,
					snapshotMeta.GetCheckpointTimeTick(),
					func() { owner.MarkSnapshotPersisted(snapshotMeta) },
				))
			}
		}
	}
	return snapshots
}

func (m *Module) NotifyCheckpointPersisted(metaTimeTick uint64, dataTimeTick uint64) {
	m.mu.Lock()
	m.persistedMetaPhysicalTT = metaTimeTick
	m.persistedDataPhysicalTT = dataTimeTick
	m.mu.Unlock()
	if m.runtime.Notifier != nil {
		m.runtime.Notifier.NotifyModuleUpdated(moduleapi.ModuleNameVChannel)
	}
}

func (m *Module) SchemaAt(vchannel string, partitionID int64, timetick uint64) (*schemapb.CollectionSchema, bool) {
	view := m.retainedVChannel(vchannel)
	if view == nil {
		return nil, false
	}
	schema := view.CreateSegmentSchema(partitionID, timetick)
	if schema == nil {
		return nil, false
	}
	return proto.Clone(schema).(*schemapb.CollectionSchema), true
}

func (m *Module) VChannelMeta(vchannel string) *streamingpb.VChannelMeta {
	view := m.retainedVChannel(vchannel)
	if view == nil {
		return nil
	}
	return view.AssignmentMeta()
}

func (m *Module) observeCreateCollectionMessage(msg message.ImmutableCreateCollectionMessageV1) moduleapi.ObserveResult {
	if vchannel := m.retainedVChannel(msg.VChannel()); vchannel != nil {
		decision, result := vchannel.ObserveExistingCreateCollectionMessageV1(msg)
		switch decision {
		case existingCreateCollectionStartNew:
			vchannel := m.addVChannel(newVChannelMetaFromCreateCollectionMessage(msg))
			return moduleapi.ObserveResult{Meta: vchannel.MetaBarrier()}
		case existingCreateCollectionInconsistent:
			m.logInconsistency(msg, "create collection vchannel already exists", mlog.String("vchannel", msg.VChannel()))
		}
		return result
	}
	vchannel := m.addVChannel(newVChannelMetaFromCreateCollectionMessage(msg))
	return moduleapi.ObserveResult{Meta: vchannel.MetaBarrier()}
}

func (m *Module) observeCreatePartitionMessage(msg message.ImmutableCreatePartitionMessageV1) moduleapi.ObserveResult {
	vchannel := m.retainedVChannel(msg.VChannel())
	if vchannel == nil {
		return moduleapi.ObserveResult{}
	}
	return vchannel.ObserveCreatePartitionMessageV1(msg)
}

func (m *Module) observeSchemaChangeMessage(msg message.ImmutableSchemaChangeMessageV2) moduleapi.ObserveResult {
	vchannel := m.retainedVChannel(msg.VChannel())
	if vchannel == nil || !vchannel.CanObserveActiveAt(msg.TimeTick()) {
		return moduleapi.ObserveResult{}
	}
	return vchannel.ObserveSchemaChangeMessageV2(msg)
}

func (m *Module) observeAlterCollectionMessage(msg message.ImmutableAlterCollectionMessageV2) moduleapi.ObserveResult {
	vchannel := m.retainedVChannel(msg.VChannel())
	if vchannel == nil || !vchannel.CanObserveActiveAt(msg.TimeTick()) {
		return moduleapi.ObserveResult{}
	}
	return vchannel.ObserveAlterCollectionMessageV2(msg)
}

func (m *Module) observeDropCollectionMessage(msg message.ImmutableDropCollectionMessageV1) moduleapi.ObserveResult {
	vchannel := m.retainedVChannel(msg.VChannel())
	if vchannel == nil {
		return moduleapi.ObserveResult{}
	}
	return vchannel.ObserveDropCollectionMessageV1(msg)
}

func (m *Module) observeDropPartitionMessage(msg message.ImmutableDropPartitionMessageV1) moduleapi.ObserveResult {
	vchannel := m.retainedVChannel(msg.VChannel())
	if vchannel == nil {
		return moduleapi.ObserveResult{}
	}
	return vchannel.ObserveDropPartitionMessageV1(msg)
}

func (m *Module) observeTruncateCollectionMessage(msg message.ImmutableTruncateCollectionMessageV2) moduleapi.ObserveResult {
	vchannel := m.retainedVChannel(msg.VChannel())
	if vchannel == nil {
		return moduleapi.ObserveResult{}
	}
	return vchannel.ObserveTruncateCollectionMessageV2(msg)
}

func (m *Module) observeAlterLoadConfigMessage(msg message.ImmutableAlterLoadConfigMessageV2) moduleapi.ObserveResult {
	vchannel := m.retainedVChannel(msg.VChannel())
	if vchannel == nil || !vchannel.CanObserveActiveAt(msg.TimeTick()) {
		return moduleapi.ObserveResult{}
	}
	return vchannel.ObserveAlterLoadConfigMessageV2(msg)
}

func (m *Module) observeDropLoadConfigMessage(msg message.ImmutableDropLoadConfigMessageV2) moduleapi.ObserveResult {
	vchannel := m.retainedVChannel(msg.VChannel())
	if vchannel == nil || !vchannel.CanObserveActiveAt(msg.TimeTick()) {
		return moduleapi.ObserveResult{}
	}
	return vchannel.ObserveDropLoadConfigMessageV2(msg)
}

func (m *Module) addVChannel(meta *streamingpb.VChannelMeta) *vChannelView {
	view := newVChannelView(meta, 0, true, m.runtimeConfig())
	m.mu.Lock()
	m.views[view.Name()] = view
	m.mu.Unlock()
	return view
}

func (m *Module) retainedVChannel(vchannel string) *vChannelView {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.views[vchannel]
}

func (m *Module) snapshotViews() map[string]*vChannelView {
	m.mu.Lock()
	defer m.mu.Unlock()
	views := make(map[string]*vChannelView, len(m.views))
	for vchannel, view := range m.views {
		views[vchannel] = view
	}
	return views
}

func (m *Module) snapshotActiveVChannels() map[string]*streamingpb.VChannelMeta {
	views := m.snapshotViews()
	snapshot := make(map[string]*streamingpb.VChannelMeta, len(views))
	for vchannel, view := range views {
		if view.IsActive() {
			snapshot[vchannel] = view.AssignmentMeta()
		}
	}
	return snapshot
}

func (m *Module) finalizeTombstones() {
	for _, view := range m.snapshotViews() {
		if view.TryFinalizeTombstone(math.MaxUint64) && m.runtime.Notifier != nil {
			m.runtime.Notifier.NotifyModuleUpdated(moduleapi.ModuleNameVChannel)
		}
	}
}

func (m *Module) removeViewIfOwner(vchannel string, owner *vChannelView) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.views[vchannel] == owner {
		delete(m.views, vchannel)
	}
}

func (m *Module) cleanupSnapshot(vchannel string, owner *vChannelView, meta *streamingpb.VChannelMeta) moduleapi.DirtySnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.pendingCleanupSnapshots == nil {
		m.pendingCleanupSnapshots = make(map[string]*dirtySnapshot)
	}
	if snapshot := m.pendingCleanupSnapshots[vchannel]; snapshot != nil {
		return snapshot
	}
	snapshot := newDirtySnapshot(
		moduleapi.SnapshotKey{PChannel: m.pchannel, VChannel: vchannel},
		moduleapi.SnapshotOpDelete,
		meta,
		meta.GetCheckpointTimeTick(),
		func() { m.markCleanupPersisted(vchannel, owner) },
	)
	m.pendingCleanupSnapshots[vchannel] = snapshot
	return snapshot
}

func (m *Module) markCleanupPersisted(vchannel string, owner *vChannelView) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.pendingCleanupSnapshots, vchannel)
	if m.views[vchannel] == owner {
		delete(m.views, vchannel)
	}
}

func (m *Module) persistedPhysicalTimeTicks() (uint64, uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.persistedMetaPhysicalTT, m.persistedDataPhysicalTT
}

func (m *Module) logInconsistency(msg message.ImmutableMessage, msgText string, fields ...mlog.Field) {
	if m.logger == nil {
		return
	}
	fields = append(fields, mlog.FieldMessage(msg))
	m.logger.Warn(context.TODO(), "inconsistent vchannel observe state", append([]mlog.Field{mlog.String("reason", msgText)}, fields...)...)
}

func emptyObserveResult() moduleapi.ObserveResult {
	return moduleapi.ObserveResult{}
}

var (
	_ moduleapi.Module                      = (*Module)(nil)
	_ moduleapi.CheckpointPersistedObserver = (*Module)(nil)
	_ SchemaProvider                        = (*Module)(nil)
)
