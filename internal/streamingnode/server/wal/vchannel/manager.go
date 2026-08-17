package vchannel

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/segment"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/transformlog"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type PChannelManagerConfig struct {
	PChannel string
	// DataCheckpointTimeTick is the persisted data-observed frontier used to
	// initialize WAL views before live data replay resumes.
	DataCheckpointTimeTick uint64

	VChannelMetas     map[string]*streamingpb.VChannelMeta
	Segments          map[int64]*streamingpb.SegmentAssignmentMeta
	TransformLogMetas map[string]*streamingpb.VChannelTransformLogMeta

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
}

// PChannelRecoveryManager owns all vchannel recovery modules on one pchannel.
type PChannelRecoveryManager struct {
	pchannel string
	modules  *typeutil.ConcurrentMap[string, *VChannelRecoveryModule]

	segmentsByVChannel map[string]map[int64]*streamingpb.SegmentAssignmentMeta
	dirtyMu            sync.Mutex
	dirtyModules       map[string]*VChannelRecoveryModule
	cleanupModules     map[string]*VChannelRecoveryModule

	config      PChannelManagerConfig
	metaAndData atomic.Bool
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
	}
	for _, vchannel := range manager.initialVChannels(config) {
		module, err := manager.newModule(vchannel)
		if err != nil {
			manager.Close()
			return nil, err
		}
		manager.modules.Insert(vchannel, module)
	}
	manager.releaseInitialState()
	return manager, nil
}

func (m *PChannelRecoveryManager) initialVChannels(config PChannelManagerConfig) []string {
	index := make(map[string]struct{})
	for vchannel := range config.VChannelMetas {
		index[vchannel] = struct{}{}
	}
	for vchannel := range m.segmentsByVChannel {
		index[vchannel] = struct{}{}
	}
	for vchannel := range config.TransformLogMetas {
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
	m.config.TransformLogMetas = nil
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

func (m *PChannelRecoveryManager) SwitchIntoMetaAndData() moduleapi.ModuleSnapshot {
	if m == nil {
		return nil
	}
	m.metaAndData.Store(true)
	snapshots := make([]moduleapi.ModuleSnapshot, 0, m.modules.Len()*3)
	m.modules.Range(func(_ string, module *VChannelRecoveryModule) bool {
		snapshots = append(snapshots, moduleapi.FlattenModuleSnapshot(module.SwitchIntoMetaAndData())...)
		return true
	})
	return aggregateModuleSnapshots(snapshots)
}

func aggregateModuleSnapshots(snapshots []moduleapi.ModuleSnapshot) moduleapi.ModuleSnapshot {
	vchannels := make(map[string]*streamingpb.VChannelMeta)
	segments := make(map[int64]*streamingpb.SegmentAssignmentMeta)
	transformLogs := make(map[string]*streamingpb.VChannelTransformLogMeta)
	writePathVChannels := make(map[string]moduleapi.VChannelWritePathRecoveryState)
	writePathSegments := make(map[int64]moduleapi.SegmentWritePathRecoveryState)
	others := make(moduleapi.CompositeModuleSnapshot, 0)

	for _, snapshot := range snapshots {
		switch typed := snapshot.(type) {
		case *moduleapi.WritePathRecoveryModuleSnapshot:
			for vchannel, state := range typed.VChannels {
				writePathVChannels[vchannel] = state
			}
			for segmentID, state := range typed.GrowingSegments {
				writePathSegments[segmentID] = state
			}
		case *moduleapi.VChannelModuleSnapshot:
			for vchannel, meta := range typed.VChannels {
				vchannels[vchannel] = meta
			}
		case *moduleapi.SegmentModuleSnapshot:
			for segmentID, meta := range typed.Segments {
				segments[segmentID] = meta
			}
		case *moduleapi.TransformLogModuleSnapshot:
			for vchannel, meta := range typed.TransformLogs {
				transformLogs[vchannel] = meta
			}
		default:
			if snapshot != nil {
				others = append(others, snapshot)
			}
		}
	}

	result := make(moduleapi.CompositeModuleSnapshot, 0, 3+len(others))
	if len(writePathVChannels) > 0 || len(writePathSegments) > 0 {
		result = append(result, &moduleapi.WritePathRecoveryModuleSnapshot{
			VChannels:       writePathVChannels,
			GrowingSegments: writePathSegments,
		})
	}
	if len(vchannels) > 0 {
		result = append(result, &moduleapi.VChannelModuleSnapshot{VChannels: vchannels})
	}
	if len(segments) > 0 {
		result = append(result, &moduleapi.SegmentModuleSnapshot{
			Segments: segments,
		})
	}
	if len(transformLogs) > 0 {
		result = append(result, &moduleapi.TransformLogModuleSnapshot{TransformLogs: transformLogs})
	}
	result = append(result, others...)
	return result
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
		if len(dirty) > 0 {
			m.markModuleDirty(module)
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
	for vchannel, module := range candidates {
		snapshots = append(snapshots, module.ConsumeCleanupSnapshots(cleanup)...)
		if module.HasCleanupCandidates() {
			m.dirtyMu.Lock()
			if m.cleanupModules == nil {
				m.cleanupModules = make(map[string]*VChannelRecoveryModule)
			}
			m.cleanupModules[vchannel] = module
			m.dirtyMu.Unlock()
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

func (m *PChannelRecoveryManager) Close() {}

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

func (m *PChannelRecoveryManager) moduleForMessage(msg message.ImmutableMessage) *VChannelRecoveryModule {
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
	switched := false
	if m.metaAndData.Load() {
		module.SwitchIntoMetaAndData()
		switched = true
	}
	module, loaded := m.modules.GetOrInsert(vchannel, module)
	if !loaded && !switched && m.metaAndData.Load() {
		module.SwitchIntoMetaAndData()
	}
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
		PChannel:                  m.pchannel,
		VChannel:                  vchannel,
		VChannelMeta:              m.config.VChannelMetas[vchannel],
		Segments:                  m.segmentsByVChannel[vchannel],
		TransformLogMeta:          m.config.TransformLogMetas[vchannel],
		Runtime:                   runtime,
		Logger:                    m.config.Logger,
		SegmentLifecycle:          m.config.SegmentLifecycle,
		SegmentPackWriter:         m.config.SegmentPackWriter,
		TransformLogStore:         m.config.TransformLogStore,
		TransformLogMaterializer:  m.config.TransformLogMaterializer,
		TransformLogMaxRows:       m.config.TransformLogMaxRows,
		TransformLogMaxBytes:      m.config.TransformLogMaxBytes,
		TransformLogMaterialRows:  m.config.TransformLogMaterialRows,
		TransformLogMaterialBytes: m.config.TransformLogMaterialBytes,
		DataObservedTimeTick:      m.config.DataCheckpointTimeTick,
		OnCleanup:                 m.removeModule,
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
