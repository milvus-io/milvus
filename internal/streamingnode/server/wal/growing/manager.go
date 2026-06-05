package growing

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	scheduler "github.com/milvus-io/milvus/pkg/v3/syncutil/preconditioned"
)

type Manager struct {
	vchannelViews map[string]*vChannelView
	segmentViews  map[int64]*segmentView
	lifecycle     segmentLifecycle
	packWriter    packWriter
	onDataUpdated func()
	channelName   string
	catalog       recoveryCatalog
	logger        *mlog.Logger
	runtime       moduleapi.Runtime
	metaAndData   bool
	transformRows uint64

	lastPersistTask scheduler.TaskHandle
	lastCleanupTask scheduler.TaskHandle
}

type managerOption func(*Manager)

type runtimeConfig struct {
	lifecycle     segmentLifecycle
	packWriter    packWriter
	runtime       moduleapi.Runtime
	onDataUpdated func()
	flushPolicy   flushPolicy
	metaAndData   bool
	transformRows uint64
}

func firstRuntimeConfig(configs []runtimeConfig) runtimeConfig {
	if len(configs) == 0 {
		return runtimeConfig{}
	}
	return configs[0]
}

func WithPackWriter(writer packWriter) managerOption {
	return func(manager *Manager) {
		manager.packWriter = writer
	}
}

func WithDataBarrierUpdatedCallback(callback func()) managerOption {
	return func(manager *Manager) {
		manager.onDataUpdated = callback
	}
}

func WithRecoveryCatalog(channelName string, catalog recoveryCatalog) managerOption {
	return func(manager *Manager) {
		manager.channelName = channelName
		manager.catalog = catalog
	}
}

func WithModuleRuntime(logger *mlog.Logger, runtime moduleapi.Runtime) managerOption {
	return func(manager *Manager) {
		manager.logger = logger
		manager.runtime = runtime
	}
}

func WithTransformLogBufferMaxRows(maxRows uint64) managerOption {
	return func(manager *Manager) {
		manager.transformRows = maxRows
	}
}

func NewManager(
	vchannels map[string]*streamingpb.VChannelMeta,
	segments map[int64]*streamingpb.SegmentAssignmentMeta,
	lifecycle segmentLifecycle,
	opts ...managerOption,
) *Manager {
	if vchannels == nil {
		vchannels = make(map[string]*streamingpb.VChannelMeta)
	}
	if segments == nil {
		segments = make(map[int64]*streamingpb.SegmentAssignmentMeta)
	}
	manager := &Manager{
		vchannelViews: make(map[string]*vChannelView, len(vchannels)),
		segmentViews:  make(map[int64]*segmentView, len(segments)),
		lifecycle:     lifecycle,
	}
	for _, opt := range opts {
		opt(manager)
	}
	manager.initializeRuntimeInfos(vchannels, segments)
	return manager
}

func (m *Manager) runtimeConfig() runtimeConfig {
	return runtimeConfig{
		lifecycle:     m.lifecycle,
		packWriter:    m.packWriter,
		runtime:       m.runtime,
		onDataUpdated: m.onDataUpdated,
		metaAndData:   m.metaAndData,
		transformRows: m.transformRows,
	}
}

func (m *Manager) initializeRuntimeInfos(
	vchannels map[string]*streamingpb.VChannelMeta,
	segments map[int64]*streamingpb.SegmentAssignmentMeta,
) {
	for vchannel, meta := range vchannels {
		m.vchannelViews[vchannel] = newVChannelViewFromMeta(meta, m.runtimeConfig())
	}
	for _, meta := range segments {
		vchannelManager := m.vchannelViews[meta.GetVchannel()]
		segment := newSegmentViewFromMeta(meta, segmentSchema(vchannelManager, meta), m.runtimeConfig())
		m.addSegmentView(segment)
	}
}

func segmentSchema(vchannel *vChannelView, meta *streamingpb.SegmentAssignmentMeta) *schemapb.CollectionSchema {
	if vchannel == nil {
		return nil
	}
	timetick := meta.GetStat().GetCreateSegmentTimeTick()
	if timetick == 0 {
		return nil
	}
	_, schema := vchannel.GetSchema(timetick)
	return schema
}

func (m *Manager) vChannel(vchannel string) *vChannelView {
	info := m.vchannelViews[vchannel]
	if info == nil || !info.IsActive() {
		return nil
	}
	return info
}

func (m *Manager) retainedVChannel(vchannel string) *vChannelView {
	return m.vchannelViews[vchannel]
}

func (m *Manager) vChannels() map[string]*vChannelView {
	return m.vchannelViews
}

func (m *Manager) addVChannel(meta *streamingpb.VChannelMeta) *vChannelView {
	info := newVChannelView(meta, 0, 0, true, m.runtimeConfig())
	m.vchannelViews[info.AssignmentMeta().GetVchannel()] = info
	m.attachRetainedSegments(info)
	return info
}

func (m *Manager) addSegmentView(segment *segmentView) *segmentView {
	segmentMeta := segment.AssignmentMeta()
	m.segmentViews[segmentMeta.GetSegmentId()] = segment
	vchannelManager := m.retainedVChannel(segmentMeta.GetVchannel())
	if vchannelManager == nil {
		return segment
	}
	vchannelMeta := vchannelManager.AssignmentMeta()
	if segmentMeta.GetCollectionId() != vchannelMeta.GetCollectionInfo().GetCollectionId() {
		return segment
	}
	vchannelManager.AddSegment(segment)
	return segment
}

func (m *Manager) attachRetainedSegments(vchannelManager *vChannelView) {
	vchannelMeta := vchannelManager.AssignmentMeta()
	for _, segment := range m.segmentViews {
		segmentMeta := segment.AssignmentMeta()
		if segmentMeta.GetVchannel() != vchannelMeta.GetVchannel() ||
			segmentMeta.GetCollectionId() != vchannelMeta.GetCollectionInfo().GetCollectionId() {
			continue
		}
		segment.SetSchema(segmentSchema(vchannelManager, segmentMeta))
		vchannelManager.AddSegment(segment)
	}
}

func (m *Manager) refreshRetainedSegmentSchemas(vchannelManager *vChannelView) {
	vchannelMeta := vchannelManager.AssignmentMeta()
	for _, segment := range m.segmentViews {
		segmentMeta := segment.AssignmentMeta()
		if segmentMeta.GetVchannel() != vchannelMeta.GetVchannel() ||
			segmentMeta.GetCollectionId() != vchannelMeta.GetCollectionInfo().GetCollectionId() {
			continue
		}
		segment.SetSchema(segmentSchema(vchannelManager, segmentMeta))
	}
}
