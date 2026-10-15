package vchannel

import (
	"context"
	"maps"
	"sort"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/l0materializer"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/segment"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walsummary"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type PChannelManagerConfig struct {
	PChannel string

	VChannelMetas map[string]*streamingpb.VChannelMeta
	Segments      map[int64]*streamingpb.SegmentAssignmentMeta

	Runtime           moduleapi.Runtime
	Logger            *mlog.Logger
	SegmentLifecycle  segment.Lifecycle
	SegmentPackWriter segment.PackWriter
	// SummaryManager accepts durable materialization and GC frontiers.
	SummaryManager *walsummary.Manager
	// L0Materializer writes and registers L0 output.
	L0Materializer     l0materializer.Materializer
	L0MaterializeRows  uint64
	L0MaterializeBytes uint64

	// Deprecated: GetRecoveryCheckpoint and CoordinatorBroker wire the
	// temporary channel-checkpoint reporting (PChannelCheckpointUpdater) that
	// mirrors the removed flusher's DataCoord.UpdateChannelCheckpoint calls.
	// When both are non-nil the manager runs the updater; remove them
	// together with the updater once the new checkpoint-propagation path
	// lands.
	GetRecoveryCheckpoint func() *utility.WALCheckpoint
	CoordinatorBroker     checkpointReporter
}

// PChannelRecoveryManager owns all vchannel recovery modules on one pchannel.
type PChannelRecoveryManager struct {
	pchannel string
	modules  *typeutil.ConcurrentMap[string, *VChannelRecoveryModule]

	segmentsByVChannel map[string]map[int64]*streamingpb.SegmentAssignmentMeta
	dirtyMu            sync.Mutex
	dirtyModules       map[string]*VChannelRecoveryModule
	cleanupModules     map[string]*VChannelRecoveryModule

	// Deprecated: periodic DataCoord channel-checkpoint reporting (see
	// PChannelCheckpointUpdater). Non-nil only when the config wires it.
	checkpointUpdater *PChannelCheckpointUpdater

	closeCh   chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
	config    PChannelManagerConfig
}

func NewPChannelRecoveryManager(config PChannelManagerConfig) (*PChannelRecoveryManager, error) {
	if config.PChannel == "" {
		return nil, merr.WrapErrServiceInternalMsg("pchannel recovery manager pchannel is empty")
	}
	segmentsByVChannel := groupSegmentsByVChannel(config.Segments)
	manager := &PChannelRecoveryManager{
		pchannel:           config.PChannel,
		modules:            typeutil.NewConcurrentMap[string, *VChannelRecoveryModule](),
		segmentsByVChannel: segmentsByVChannel,
		dirtyModules:       make(map[string]*VChannelRecoveryModule),
		config:             config,
		closeCh:            make(chan struct{}),
	}
	if config.GetRecoveryCheckpoint != nil && config.CoordinatorBroker != nil {
		manager.checkpointUpdater = newPChannelCheckpointUpdater(
			config.PChannel,
			manager.activeVChannels,
			config.GetRecoveryCheckpoint,
			config.CoordinatorBroker,
		)
	}
	for _, vchannel := range manager.initialVChannels(config) {
		module, err := manager.newModule(vchannel)
		if err != nil {
			manager.Close()
			return nil, err
		}
		manager.modules.Insert(vchannel, module)
	}
	if config.SummaryManager != nil {
		config.SummaryManager.RestoreTransformGCTimeTicks(config.VChannelMetas)
	}
	manager.releaseInitialState()
	return manager, nil
}

// activeVChannels returns the currently active vchannels of the pchannel.
func (m *PChannelRecoveryManager) activeVChannels() []string {
	vchannels := make([]string, 0, m.modules.Len())
	m.modules.Range(func(vchannel string, _ *VChannelRecoveryModule) bool {
		vchannels = append(vchannels, vchannel)
		return true
	})
	sort.Strings(vchannels)
	return vchannels
}

func (m *PChannelRecoveryManager) initialVChannels(config PChannelManagerConfig) []string {
	index := make(map[string]struct{})
	for vchannel := range config.VChannelMetas {
		index[vchannel] = struct{}{}
	}
	for vchannel := range m.segmentsByVChannel {
		index[vchannel] = struct{}{}
	}
	vchannels := make([]string, 0, len(index))
	for vchannel := range index {
		vchannels = append(vchannels, vchannel)
	}
	sort.Strings(vchannels)
	return vchannels
}

func groupSegmentsByVChannel(segments map[int64]*streamingpb.SegmentAssignmentMeta) map[string]map[int64]*streamingpb.SegmentAssignmentMeta {
	grouped := make(map[string]map[int64]*streamingpb.SegmentAssignmentMeta)
	for id, meta := range segments {
		vchannel := meta.GetVchannel()
		if vchannel == "" {
			continue
		}
		if grouped[vchannel] == nil {
			grouped[vchannel] = make(map[int64]*streamingpb.SegmentAssignmentMeta)
		}
		grouped[vchannel][id] = meta
	}
	return grouped
}

func (m *PChannelRecoveryManager) releaseInitialState() {
	m.config.VChannelMetas = nil
	m.config.Segments = nil
	m.segmentsByVChannel = nil
}

func (m *PChannelRecoveryManager) ObserveMessage(
	ctx context.Context,
	retained message.RetainedImmutableMessage,
) {
	if m == nil {
		return
	}
	msg := retained.Message()
	if funcutil.IsControlChannel(msg.VChannel()) && !msg.IsPChannelLevel() {
		return
	}
	if m.shouldBroadcast(msg) {
		m.observeBroadcastMessage(ctx, retained)
		return
	}
	for {
		module := m.moduleForMessage(msg)
		if module == nil {
			return
		}
		if !module.ObserveMessage(ctx, retained) {
			continue
		}
		m.markModuleUpdated(module)
		return
	}
}

func (m *PChannelRecoveryManager) RecoverySnapshot() *moduleapi.WritePathRecoveryModuleSnapshot {
	snapshot := &moduleapi.WritePathRecoveryModuleSnapshot{
		VChannels:       make(map[string]moduleapi.VChannelWritePathRecoveryState),
		GrowingSegments: make(map[int64]moduleapi.SegmentWritePathRecoveryState),
	}
	if m == nil {
		return snapshot
	}
	m.modules.Range(func(_ string, module *VChannelRecoveryModule) bool {
		moduleSnapshot := module.RecoverySnapshot()
		maps.Copy(snapshot.VChannels, moduleSnapshot.VChannels)
		maps.Copy(snapshot.GrowingSegments, moduleSnapshot.GrowingSegments)
		return true
	})
	return snapshot
}

func (m *PChannelRecoveryManager) ConsumeDirtySnapshots() []moduleapi.DirtySnapshot {
	if m == nil {
		return nil
	}
	snapshots := make([]moduleapi.DirtySnapshot, 0)
	for _, module := range m.takeDirtyModules() {
		dirty := module.ConsumeDirtySnapshots()
		snapshots = append(snapshots, dirty...)
		if module.HasCleanupCandidates() {
			m.markCleanupCandidate(module)
		}
	}
	return snapshots
}

func (m *PChannelRecoveryManager) ConsumeCleanupSnapshots(cleanup moduleapi.CleanupContext) []moduleapi.DirtySnapshot {
	m.dirtyMu.Lock()
	candidates := m.cleanupModules
	m.cleanupModules = nil
	m.dirtyMu.Unlock()
	var snapshots []moduleapi.DirtySnapshot
	for _, module := range candidates {
		snapshots = append(snapshots, module.ConsumeCleanupSnapshots(cleanup)...)
		if module.HasCleanupCandidates() {
			m.markCleanupCandidate(module)
		}
	}
	return snapshots
}

func (m *PChannelRecoveryManager) markCleanupCandidate(module *VChannelRecoveryModule) {
	m.dirtyMu.Lock()
	if m.cleanupModules == nil {
		m.cleanupModules = make(map[string]*VChannelRecoveryModule)
	}
	m.cleanupModules[module.vchannel] = module
	m.dirtyMu.Unlock()
}

func (m *PChannelRecoveryManager) Module(vchannel string) *VChannelRecoveryModule {
	module, _ := m.modules.Get(vchannel)
	return module
}

// RequestPersistThrough schedules persistence for buffered data of one
// VChannel through targetTimeTick.
func (m *PChannelRecoveryManager) RequestPersistThrough(vchannel string, targetTimeTick uint64) {
	if m == nil || vchannel == "" {
		return
	}
	module := m.Module(vchannel)
	if module == nil {
		return
	}
	module.RequestPersistThrough(targetTimeTick)
}

// Start runs the legacy checkpoint reporter and one shared L0 age check.
func (m *PChannelRecoveryManager) Start() {
	if m.checkpointUpdater != nil {
		m.wg.Add(1)
		go func() { defer m.wg.Done(); m.checkpointUpdater.Start() }()
	}
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-m.closeCh:
				return
			case now := <-ticker.C:
				maxAge := paramtable.Get().DataNodeCfg.SyncPeriod.GetAsDuration(time.Second)
				m.modules.Range(func(_ string, module *VChannelRecoveryModule) bool {
					module.l0Materializer.FlushStale(now, maxAge)
					return true
				})
			}
		}
	}()
}

func (m *PChannelRecoveryManager) Close() {
	m.closeOnce.Do(func() { close(m.closeCh) })
	if m.checkpointUpdater != nil {
		m.checkpointUpdater.Close()
	}
	m.wg.Wait()
}

func (m *PChannelRecoveryManager) shouldBroadcast(msg message.ImmutableMessage) bool {
	return msg.VChannel() == "" || msg.IsPChannelLevel()
}

func (m *PChannelRecoveryManager) observeBroadcastMessage(
	ctx context.Context,
	retained message.RetainedImmutableMessage,
) {
	m.modules.Range(func(_ string, module *VChannelRecoveryModule) bool {
		dispatch := retained.Clone()
		observed := module.ObserveMessage(ctx, dispatch)
		dispatch.Release()
		if observed {
			m.markModuleUpdated(module)
		}
		return true
	})
}

func (m *PChannelRecoveryManager) moduleForMessage(
	msg message.ImmutableMessage,
) *VChannelRecoveryModule {
	vchannel := msg.VChannel()
	if vchannel == "" {
		return nil
	}
	module, _ := m.modules.Get(vchannel)
	if module != nil || msg.MessageType() != message.MessageTypeCreateCollection {
		return module
	}
	module, err := m.newModule(vchannel)
	if err != nil {
		return nil
	}
	module, _ = m.modules.GetOrInsert(vchannel, module)
	return module
}

func (m *PChannelRecoveryManager) newModule(vchannel string) (*VChannelRecoveryModule, error) {
	runtime := m.config.Runtime
	runtime.Notifier = &dirtyTrackingNotifier{
		inner: runtime.Notifier,
		onDirty: func() {
			m.markModuleUpdatedByVChannel(vchannel)
		},
	}
	module, err := newModuleFromOwnedRecoveryState(ModuleConfig{
		PChannel:           m.pchannel,
		VChannel:           vchannel,
		VChannelMeta:       m.config.VChannelMetas[vchannel],
		Segments:           m.segmentsByVChannel[vchannel],
		Runtime:            runtime,
		Logger:             m.config.Logger,
		SegmentLifecycle:   m.config.SegmentLifecycle,
		SegmentPackWriter:  m.config.SegmentPackWriter,
		L0Materializer:     m.config.L0Materializer,
		L0MaterializeRows:  m.config.L0MaterializeRows,
		L0MaterializeBytes: m.config.L0MaterializeBytes,
		OnCleanup:          m.removeModule,
		OnL0Materialized: func(through uint64) {
			if m.config.SummaryManager != nil {
				m.config.SummaryManager.ReportMaterialized(vchannel, through)
			}
		},
	})
	if err != nil {
		return nil, err
	}
	if module.HasCleanupCandidates() {
		m.markCleanupCandidate(module)
	}
	return module, nil
}

func (m *PChannelRecoveryManager) removeModule(module *VChannelRecoveryModule) {
	if module == nil {
		return
	}
	current, ok := m.modules.Get(module.vchannel)
	if !ok || current != module {
		return
	}
	m.modules.Remove(module.vchannel)
	m.dirtyMu.Lock()
	if m.dirtyModules[module.vchannel] == module {
		delete(m.dirtyModules, module.vchannel)
	}
	if m.cleanupModules[module.vchannel] == module {
		delete(m.cleanupModules, module.vchannel)
	}
	m.dirtyMu.Unlock()
	// Report the GC boundary to the summary: the dropped vchannel's cleanup
	// snapshot is durable, so its records — staged or already chunked — may
	// be released by retention GC.
	if m.config.SummaryManager != nil {
		m.config.SummaryManager.AdvanceGCTimeTick(module.vchannel, walsummary.DroppedVChannelTimeTick)
	}
}

func (m *PChannelRecoveryManager) markModuleUpdatedByVChannel(vchannel string) {
	if module := m.Module(vchannel); module != nil {
		m.markModuleUpdated(module)
	}
}

func (m *PChannelRecoveryManager) markModuleUpdated(module *VChannelRecoveryModule) {
	if module == nil {
		return
	}
	m.markModuleDirty(module)
}

func (m *PChannelRecoveryManager) markModuleDirty(module *VChannelRecoveryModule) {
	m.dirtyMu.Lock()
	defer m.dirtyMu.Unlock()
	if current, ok := m.modules.Get(module.vchannel); ok && current == module {
		m.dirtyModules[module.vchannel] = module
	}
}

func (m *PChannelRecoveryManager) takeDirtyModules() map[string]*VChannelRecoveryModule {
	m.dirtyMu.Lock()
	dirty := m.dirtyModules
	m.dirtyModules = make(map[string]*VChannelRecoveryModule)
	m.dirtyMu.Unlock()
	return dirty
}

type dirtyTrackingNotifier struct {
	inner   moduleapi.ModuleNotifier
	onDirty func()
}

func (n *dirtyTrackingNotifier) NotifyModuleUpdated(name moduleapi.ModuleName) {
	n.onDirty()
	if n.inner != nil {
		n.inner.NotifyModuleUpdated(name)
	}
}
