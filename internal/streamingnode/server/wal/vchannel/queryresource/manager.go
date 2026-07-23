package queryresource

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

type ViewBuilder func(meta *viewpb.QueryViewMeta) (walview.VChannelWALView, bool)

type Config struct {
	Builders         []QueryRuntimeModuleBuilder
	Scheduler        nodescheduler.Scheduler
	Dispatcher       *Dispatcher
	LoadInfoProvider LoadInfoProvider
}

type Manager struct {
	mu sync.Mutex

	builders         []QueryRuntimeModuleBuilder
	scheduler        nodescheduler.Scheduler
	dispatcher       *Dispatcher
	loadInfoProvider LoadInfoProvider

	refs    map[qviews.QueryViewKey]queryViewRef
	runtime *QueryRuntime
	task    *scheduledBuild
	err     error
	closed  bool
}

type queryViewRef struct {
	onReady func()
}

func NewManager(config Config) *Manager {
	return &Manager{
		builders:         defaultQueryRuntimeModuleBuilders(config.Builders),
		scheduler:        config.Scheduler,
		dispatcher:       config.Dispatcher,
		loadInfoProvider: config.LoadInfoProvider,
		refs:             make(map[qviews.QueryViewKey]queryViewRef),
	}
}

// AcquireLocked registers a query view reference and starts runtime building
// when needed. The caller should hold the owning VChannel state lock so build
// observes a consistent DataView snapshot.
func (m *Manager) AcquireLocked(req snview.AcquireResource, build ViewBuilder) {
	if req.Meta == nil || req.Meta.GetVersion() == nil || req.Meta.GetVersion().GetDataVersion() == nil {
		panic("query view meta version is nil")
	}
	var notifyReady bool
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		panic("vchannel query resource is closed")
	}
	if _, ok := m.refs[req.Key]; !ok {
		m.assertAcquireMonotonic(req.Key.QueryViewVersion.DataVersion)
		m.refs[req.Key] = queryViewRef{onReady: req.OnReady}
		if m.runtime == nil && m.task == nil {
			m.startBuildLocked(req.Meta, build)
		} else if m.runtime != nil && m.task == nil && m.err == nil {
			notifyReady = true
		}
	}
	m.mu.Unlock()
	if notifyReady {
		m.submitReady(req.Key, req.OnReady)
	}
}

func (m *Manager) Release(req snview.ReleaseResource) {
	var runtime *QueryRuntime
	var task *scheduledBuild
	var advanceRuntime *QueryRuntime
	var advance qviews.DataVersion
	var hasAdvance bool

	m.mu.Lock()
	if _, ok := m.refs[req.Key]; ok {
		delete(m.refs, req.Key)
		advance, hasAdvance = minQueryViewDataVersion(m.refs)
		advanceRuntime = m.runtime
	}
	if len(m.refs) == 0 {
		runtime, task = m.takeRuntimeLocked()
	}
	m.mu.Unlock()

	if hasAdvance && advanceRuntime != nil {
		advanceRuntime.Advance(advance)
	}
	cancelTask(task)
	closeRuntime(runtime)
	m.submitCallback(req.OnDropped)
}

func (m *Manager) QueryRuntime(key qviews.QueryViewKey) (*QueryRuntime, bool) {
	if m == nil {
		return nil, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.refs[key]; !ok {
		return nil, false
	}
	if m.task != nil || m.runtime == nil || m.err != nil {
		return nil, false
	}
	return m.runtime, true
}

func (m *Manager) Close() {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.closed = true
	runtime, task := m.takeRuntimeLocked()
	m.refs = make(map[qviews.QueryViewKey]queryViewRef)
	m.mu.Unlock()

	cancelTask(task)
	if task != nil {
		_, _ = task.Result()
	}
	closeRuntime(runtime)
}

func (m *Manager) ObserveEvent(ctx context.Context, event walview.VChannelResourceEvent) {
	m.mu.Lock()
	runtime := m.runtime
	m.mu.Unlock()
	if runtime != nil {
		runtime.ObserveEvent(ctx, event)
	}
}

func (m *Manager) assertAcquireMonotonic(version qviews.DataVersion) {
	for key := range m.refs {
		if key.QueryViewVersion.DataVersion.GT(version) {
			panic("non-monotonic query view acquire")
		}
	}
}

func (m *Manager) startBuildLocked(meta *viewpb.QueryViewMeta, build ViewBuilder) {
	if build == nil {
		panic("query resource view builder is nil")
	}
	if m.scheduler == nil {
		m.scheduler = nodescheduler.Get()
	}
	if m.dispatcher == nil {
		m.dispatcher = NewDispatcher(defaultLiveEventDispatchConcurrency)
	}
	runtime := newQueryRuntime(m.dispatcher, m.newModules()...)
	view, ok := build(meta)
	if !ok {
		panic("failed to build vchannel query resource view")
	}
	task := newResourceBuildTask(func(ctx context.Context) (*QueryRuntime, error) {
		resolved, err := m.resolveLoadInfo(ctx, view)
		if err != nil {
			return runtime, err
		}
		if err := runtime.Initialize(ctx, resolved); err != nil {
			return runtime, err
		}
		return runtime, nil
	})
	m.runtime = runtime
	m.task = scheduleResourceBuild(m.scheduler, task, m.finishBuild)
	m.err = nil
}

func (m *Manager) resolveLoadInfo(ctx context.Context, view walview.VChannelWALView) (walview.VChannelWALView, error) {
	if view.LoadInfoVersion == 0 || m.loadInfoProvider == nil {
		return view, nil
	}
	loadInfo, err := m.loadInfoProvider.QueryViewLoadInfo(ctx, view.CollectionID, view.LoadInfoVersion)
	if err != nil {
		return view, err
	}
	view.PartitionIDs = loadInfo.PartitionIDs
	view.LoadFields = loadInfo.LoadFields
	view.IndexInfos = loadInfo.IndexInfos
	return view, nil
}

func (m *Manager) newModules() []QueryRuntimeModule {
	modules := make([]QueryRuntimeModule, 0, len(m.builders))
	for _, builder := range m.builders {
		if builder == nil {
			continue
		}
		module, err := builder.NewRuntime()
		if err != nil {
			panic(errors.Wrap(err, "create query runtime module"))
		}
		if module != nil {
			modules = append(modules, module)
		}
	}
	return modules
}

func (m *Manager) finishBuild(task *scheduledBuild) {
	runtime, err := task.task.Result()
	ready := make(map[qviews.QueryViewKey]func())
	m.mu.Lock()
	if m.task != task {
		m.mu.Unlock()
		closeRuntime(runtime)
		return
	}
	m.task = nil
	if err != nil {
		if errors.Is(err, context.Canceled) {
			m.err = err
		} else {
			panic(errors.Wrap(err, "initialize query runtime"))
		}
	} else {
		m.runtime = runtime
		m.err = nil
		for key, ref := range m.refs {
			ready[key] = ref.onReady
		}
	}
	if len(m.refs) == 0 {
		runtime, task = m.takeRuntimeLocked()
	} else {
		runtime, task = nil, nil
	}
	m.mu.Unlock()

	cancelTask(task)
	closeRuntime(runtime)
	for key, onReady := range ready {
		m.notifyReady(key, onReady)
	}
}

func (m *Manager) submitReady(key qviews.QueryViewKey, onReady func()) {
	if onReady == nil {
		return
	}
	m.submitCallback(func() {
		m.notifyReady(key, onReady)
	})
}

func (m *Manager) notifyReady(key qviews.QueryViewKey, onReady func()) {
	if onReady == nil {
		return
	}
	m.mu.Lock()
	_, ok := m.refs[key]
	runtime := m.runtime
	advance, hasAdvance := minQueryViewDataVersion(m.refs)
	ready := ok && runtime != nil && m.task == nil && m.err == nil
	m.mu.Unlock()
	if !ready {
		return
	}
	if hasAdvance {
		runtime.Advance(advance)
	}
	onReady()
}

func (m *Manager) submitCallback(callback func()) {
	if callback == nil {
		return
	}
	m.mu.Lock()
	if m.scheduler == nil {
		m.scheduler = nodescheduler.Get()
	}
	scheduler := m.scheduler
	m.mu.Unlock()
	scheduler.Submit(resourceCallbackTask(callback))
}

func (m *Manager) takeRuntimeLocked() (*QueryRuntime, *scheduledBuild) {
	runtime, task := m.runtime, m.task
	m.runtime = nil
	m.task = nil
	m.err = nil
	return runtime, task
}
