package vchannel

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	walcheckpoint "github.com/milvus-io/milvus/internal/streamingnode/server/wal/checkpoint"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/queryresource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/segment"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/transformlog"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type PChannelManagerConfig struct {
	PChannel string

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
	OnSegmentSealed           func(walview.SegmentSealedEvent)

	QueryRuntimeModuleBuilders []queryresource.QueryRuntimeModuleBuilder
	QueryViewLoadInfoProvider  queryresource.LoadInfoProvider
	NodeScheduler              nodescheduler.Scheduler
}

// PChannelRecoveryManager owns all vchannel recovery modules on one pchannel.
type PChannelRecoveryManager struct {
	pchannel string
	modules  *typeutil.ConcurrentMap[string, *VChannelRecoveryModule]

	segmentsByVChannel    map[string]map[int64]*streamingpb.SegmentAssignmentMeta
	dirtyMu               sync.Mutex
	dirtyModules          map[string]*VChannelRecoveryModule
	cleanupModules        map[string]*VChannelRecoveryModule
	durableFrontiers      *minimumFrontierIndex[string]
	materializedFrontiers *minimumFrontierIndex[string]

	config                  PChannelManagerConfig
	metaAndData             atomic.Bool
	streamManager           *transformlog.StreamManager
	queryTransformLogStream wal.TransformLogStream
	queryDispatcher         *queryresource.Dispatcher
}

func NewPChannelRecoveryManager(config PChannelManagerConfig) (*PChannelRecoveryManager, error) {
	if config.PChannel == "" {
		return nil, merr.WrapErrServiceInternalMsg("pchannel recovery manager pchannel is empty")
	}
	segmentsByVChannel := groupSegmentsByVChannel(config.Segments)
	manager := &PChannelRecoveryManager{
		pchannel:              config.PChannel,
		modules:               typeutil.NewConcurrentMap[string, *VChannelRecoveryModule](),
		segmentsByVChannel:    segmentsByVChannel,
		dirtyModules:          make(map[string]*VChannelRecoveryModule),
		durableFrontiers:      newMinimumFrontierIndex[string](),
		materializedFrontiers: newMinimumFrontierIndex[string](),
		config:                config,
		streamManager:         transformlog.NewStreamManager(config.PChannel),
		queryDispatcher:       queryresource.NewDispatcher(4),
	}
	queryTransformLogStream, err := manager.streamManager.AcquireStream(context.Background(), config.PChannel)
	if err != nil {
		manager.queryDispatcher.Close()
		return nil, err
	}
	manager.queryTransformLogStream = queryTransformLogStream
	for _, vchannel := range manager.initialVChannels(config) {
		module, err := manager.newModule(vchannel)
		if err != nil {
			manager.Close()
			return nil, err
		}
		manager.modules.Insert(vchannel, module)
		manager.refreshModuleFrontiers(module)
		manager.syncTransformLogStream(module)
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

func (m *PChannelRecoveryManager) Name() moduleapi.ModuleName {
	return moduleapi.ModuleNameVChannel
}

func (m *PChannelRecoveryManager) ObserveMessage(ctx context.Context, msg message.ImmutableMessage) moduleapi.ObserveResult {
	if m == nil || msg == nil {
		return moduleapi.ObserveResult{}
	}
	if funcutil.IsControlChannel(msg.VChannel()) && !msg.IsPChannelLevel() {
		return moduleapi.ObserveResult{}
	}
	if m.shouldBroadcast(msg) {
		return m.observeBroadcastMessage(ctx, msg)
	}
	module := m.moduleForMessage(msg)
	if module == nil {
		return moduleapi.ObserveResult{}
	}
	result := module.ObserveMessage(ctx, msg)
	m.markModuleUpdated(module)
	m.syncTransformLogStream(module)
	return result
}

func (m *PChannelRecoveryManager) SwitchIntoMetaAndData() moduleapi.ModuleSnapshot {
	if m == nil {
		return nil
	}
	m.metaAndData.Store(true)
	snapshots := make([]moduleapi.ModuleSnapshot, 0, m.modules.Len()*3)
	m.modules.Range(func(_ string, module *VChannelRecoveryModule) bool {
		snapshots = append(snapshots, moduleapi.FlattenModuleSnapshot(module.SwitchIntoMetaAndData())...)
		m.refreshModuleFrontiers(module)
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

func (m *PChannelRecoveryManager) HasPendingCleanup() bool {
	m.dirtyMu.Lock()
	defer m.dirtyMu.Unlock()
	return len(m.cleanupModules) > 0
}

func (m *PChannelRecoveryManager) markCleanupCandidate(module *VChannelRecoveryModule) {
	m.dirtyMu.Lock()
	if m.cleanupModules == nil {
		m.cleanupModules = make(map[string]*VChannelRecoveryModule)
	}
	m.cleanupModules[module.vchannel] = module
	m.dirtyMu.Unlock()
}

func (m *PChannelRecoveryManager) DataFrontier(scope moduleapi.Scope) walcheckpoint.Barrier {
	if m == nil {
		return nil
	}
	if scope.Type == moduleapi.ScopeAll {
		if scope.Kind == moduleapi.DataProgressMaterialized {
			return walcheckpoint.BarrierFunc(m.materializedFrontiers.Minimum)
		}
		return walcheckpoint.BarrierFunc(m.durableFrontiers.Minimum)
	}
	if scope.VChannel != "" && (scope.Type == moduleapi.ScopeVChannel || scope.Type == moduleapi.ScopePartition) {
		if module := m.Module(scope.VChannel); module != nil {
			return module.DataFrontier(scope)
		}
		return nil
	}
	barriers := make([]walcheckpoint.Barrier, 0)
	m.modules.Range(func(_ string, module *VChannelRecoveryModule) bool {
		if barrier := module.DataFrontier(scope); barrier != nil {
			barriers = append(barriers, barrier)
		}
		return true
	})
	return walcheckpoint.NewCompositeBarrier(barriers...)
}

func (m *PChannelRecoveryManager) Module(vchannel string) *VChannelRecoveryModule {
	module, _ := m.modules.Get(vchannel)
	return module
}

func (m *PChannelRecoveryManager) AcquireStream(ctx context.Context, pchannel string) (wal.TransformLogStream, error) {
	return m.streamManager.AcquireStream(ctx, pchannel)
}

func (m *PChannelRecoveryManager) Acquire(req snview.AcquireResource) {
	if m == nil || req.Meta == nil {
		panic("query resource acquire misses meta")
	}
	module := m.Module(req.Meta.GetVchannel())
	if module == nil {
		panic("query resource acquire misses vchannel module")
	}
	module.AcquireQueryResource(req)
}

func (m *PChannelRecoveryManager) Release(req snview.ReleaseResource) {
	if m == nil {
		return
	}
	module := m.Module(req.Key.ShardID.VChannel)
	if module == nil {
		go func() {
			if req.OnDropped != nil {
				req.OnDropped()
			}
		}()
		return
	}
	module.ReleaseQueryResource(req)
}

func (m *PChannelRecoveryManager) QueryRuntime(key qviews.QueryViewKey) (snview.QueryRuntime, bool) {
	runtime, ok := m.GetQueryRuntime(key)
	if !ok {
		return nil, false
	}
	return runtime, true
}

func (m *PChannelRecoveryManager) GetQueryRuntime(key qviews.QueryViewKey) (*queryresource.QueryRuntime, bool) {
	if m == nil {
		return nil, false
	}
	module := m.Module(key.ShardID.VChannel)
	if module == nil {
		return nil, false
	}
	return module.QueryRuntime(key)
}

func (m *PChannelRecoveryManager) Close() {
	if m == nil {
		return
	}
	m.modules.Range(func(_ string, module *VChannelRecoveryModule) bool {
		module.CloseQueryResources()
		return true
	})
	if m.queryTransformLogStream != nil {
		_ = m.queryTransformLogStream.Close()
	}
	if m.queryDispatcher != nil {
		m.queryDispatcher.Close()
	}
}

func (m *PChannelRecoveryManager) shouldBroadcast(msg message.ImmutableMessage) bool {
	return msg.VChannel() == "" || msg.IsPChannelLevel()
}

func (m *PChannelRecoveryManager) observeBroadcastMessage(ctx context.Context, msg message.ImmutableMessage) moduleapi.ObserveResult {
	results := make([]moduleapi.ObserveResult, 0, m.modules.Len())
	m.modules.Range(func(_ string, module *VChannelRecoveryModule) bool {
		result := module.ObserveMessage(ctx, msg)
		m.markModuleUpdated(module)
		m.syncTransformLogStream(module)
		results = append(results, result)
		return true
	})
	return moduleapi.ComposeBarriers(results)
}

func (m *PChannelRecoveryManager) syncTransformLogStream(module *VChannelRecoveryModule) {
	if module == nil {
		return
	}
	if module.IsActive() {
		m.streamManager.Register(module.vchannel, module.transformLog)
		return
	}
	m.streamManager.Remove(module.vchannel)
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
	if !loaded {
		m.refreshModuleFrontiers(module)
		m.syncTransformLogStream(module)
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
		PChannel:                   m.pchannel,
		VChannel:                   vchannel,
		VChannelMeta:               m.config.VChannelMetas[vchannel],
		Segments:                   m.segmentsByVChannel[vchannel],
		TransformLogMeta:           m.config.TransformLogMetas[vchannel],
		Runtime:                    runtime,
		Logger:                     m.config.Logger,
		SegmentLifecycle:           m.config.SegmentLifecycle,
		SegmentPackWriter:          m.config.SegmentPackWriter,
		TransformLogStore:          m.config.TransformLogStore,
		TransformLogMaterializer:   m.config.TransformLogMaterializer,
		TransformLogMaxRows:        m.config.TransformLogMaxRows,
		TransformLogMaxBytes:       m.config.TransformLogMaxBytes,
		TransformLogMaterialRows:   m.config.TransformLogMaterialRows,
		TransformLogMaterialBytes:  m.config.TransformLogMaterialBytes,
		OnSegmentSealed:            m.config.OnSegmentSealed,
		TransformLogStream:         m.queryTransformLogStream,
		QueryRuntimeModuleBuilders: m.config.QueryRuntimeModuleBuilders,
		QueryViewLoadInfoProvider:  m.config.QueryViewLoadInfoProvider,
		NodeScheduler:              m.config.NodeScheduler,
		QueryRuntimeDispatcher:     m.queryDispatcher,
		OnFrontierUpdated:          func() { m.refreshModuleFrontiersByVChannel(vchannel) },
	})
	if err != nil {
		return nil, err
	}
	if module.HasCleanupCandidates() {
		m.markCleanupCandidate(module)
	}
	if module.vchannelView != nil && module.vchannelView.SegmentDataVersionSummary().GT(module.vchannelView.PersistedSegmentDataVersionSummary()) {
		m.markModuleDirty(module)
	}
	return module, nil
}

func (m *PChannelRecoveryManager) markModuleUpdatedByVChannel(vchannel string) {
	if module := m.Module(vchannel); module != nil {
		m.markModuleUpdated(module)
	}
}

func (m *PChannelRecoveryManager) refreshModuleFrontiersByVChannel(vchannel string) {
	if module := m.Module(vchannel); module != nil {
		m.refreshModuleFrontiers(module)
	}
}

func (m *PChannelRecoveryManager) markModuleUpdated(module *VChannelRecoveryModule) {
	if module == nil {
		return
	}
	m.markModuleDirty(module)
	m.refreshModuleFrontiers(module)
}

func (m *PChannelRecoveryManager) markModuleDirty(module *VChannelRecoveryModule) {
	m.dirtyMu.Lock()
	m.dirtyModules[module.vchannel] = module
	m.dirtyMu.Unlock()
}

func (m *PChannelRecoveryManager) takeDirtyModules() map[string]*VChannelRecoveryModule {
	m.dirtyMu.Lock()
	dirty := m.dirtyModules
	m.dirtyModules = make(map[string]*VChannelRecoveryModule)
	m.dirtyMu.Unlock()
	return dirty
}

func (m *PChannelRecoveryManager) refreshModuleFrontiers(module *VChannelRecoveryModule) {
	if module == nil {
		return
	}
	m.durableFrontiers.Update(module.vchannel, module.dataFrontierTimeTick(moduleapi.DataProgressDurable))
	m.materializedFrontiers.Update(module.vchannel, module.dataFrontierTimeTick(moduleapi.DataProgressMaterialized))
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

func (n *dirtyTrackingNotifier) NotifyBarrierUpdated() {
	n.onDirty()
	if n.inner != nil {
		n.inner.NotifyBarrierUpdated()
	}
}

var (
	_ moduleapi.Module                    = (*PChannelRecoveryManager)(nil)
	_ moduleapi.PendingCleanupModule      = (*PChannelRecoveryManager)(nil)
	_ moduleapi.DataFrontierProvider      = (*PChannelRecoveryManager)(nil)
	_ wal.TransformLogStreamManager       = (*PChannelRecoveryManager)(nil)
	_ snview.StreamingNodeResourceManager = (*PChannelRecoveryManager)(nil)
	_ snview.QueryRuntimeProvider         = (*PChannelRecoveryManager)(nil)
)
