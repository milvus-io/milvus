package queryresource

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

type ViewBuilder func(meta *viewpb.QueryViewMeta) (walview.VChannelWALView, bool)

type Config struct {
	Builders   []QueryRuntimeModuleBuilder
	Scheduler  Scheduler
	Dispatcher *Dispatcher
}

type Manager struct {
	mu sync.Mutex

	builders   []QueryRuntimeModuleBuilder
	scheduler  Scheduler
	dispatcher *Dispatcher

	refs    map[qviews.QueryViewKey]struct{}
	epoch   map[qviews.QueryViewKey]uint64
	runtime *QueryRuntime
	task    BuildTask
	err     error
	changed chan struct{}
	closed  bool
}

func NewManager(config Config) *Manager {
	return &Manager{
		builders:   defaultQueryRuntimeModuleBuilders(config.Builders),
		scheduler:  config.Scheduler,
		dispatcher: config.Dispatcher,
		refs:       make(map[qviews.QueryViewKey]struct{}),
		epoch:      make(map[qviews.QueryViewKey]uint64),
		changed:    make(chan struct{}),
	}
}

// AcquireLocked registers a query view reference and starts runtime building
// when needed. The caller should hold the owning VChannel state lock so build
// observes a consistent DataView snapshot.
func (m *Manager) AcquireLocked(req snview.AcquireResource, build ViewBuilder) uint64 {
	if req.Meta == nil || req.Meta.GetVersion() == nil || req.Meta.GetVersion().GetDataVersion() == nil {
		panic("query view meta version is nil")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		panic("vchannel query resource is closed")
	}
	if _, ok := m.refs[req.Key]; !ok {
		m.assertAcquireMonotonic(req.Key.QueryViewVersion.DataVersion)
		m.refs[req.Key] = struct{}{}
		m.epoch[req.Key]++
		if m.runtime == nil && m.task == nil {
			m.startBuildLocked(req.Meta, build)
		}
		m.notifyChangedLocked()
	}
	return m.epoch[req.Key]
}

func (m *Manager) Release(req snview.ReleaseResource) {
	var runtime *QueryRuntime
	var task BuildTask
	var advanceRuntime *QueryRuntime
	var advance qviews.DataVersion
	var hasAdvance bool

	m.mu.Lock()
	if _, ok := m.refs[req.Key]; ok {
		delete(m.refs, req.Key)
		m.notifyChangedLocked()
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
	go func() {
		if req.OnDropped != nil {
			req.OnDropped()
		}
	}()
	cancelTask(task)
	closeRuntime(runtime)
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
	m.notifyChangedLocked()
	runtime, task := m.takeRuntimeLocked()
	m.refs = make(map[qviews.QueryViewKey]struct{})
	m.epoch = make(map[qviews.QueryViewKey]uint64)
	m.mu.Unlock()

	cancelTask(task)
	closeRuntime(runtime)
}

func (m *Manager) WaitReady(key qviews.QueryViewKey, epoch uint64, onReady func()) {
	for {
		runtime, task, changed, ok := m.runtimeForRef(key, epoch)
		if !ok {
			return
		}
		if changed != nil {
			<-changed
			continue
		}
		if task != nil {
			<-task.Done()
			_, err := task.Result()
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				panic(errors.Wrap(err, "wait query runtime initialization"))
			}
			continue
		}
		advance, ok := m.oldestDataVersionForRef(key, epoch)
		if !ok {
			return
		}
		runtime.Advance(advance)
		if onReady != nil && m.hasRef(key, epoch) {
			onReady()
		}
		return
	}
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
		m.scheduler = NewScheduler(4)
	}
	if m.dispatcher == nil {
		m.dispatcher = NewDispatcher(defaultLiveEventDispatchConcurrency)
	}
	runtime := newQueryRuntime(m.dispatcher, m.newModules()...)
	view, ok := build(meta)
	if !ok {
		panic("failed to build vchannel query resource view")
	}
	task := newResourceBuildTask(context.Background(), func(ctx context.Context) (*QueryRuntime, error) {
		if err := runtime.Initialize(ctx, view); err != nil {
			return runtime, err
		}
		return runtime, nil
	})
	m.runtime = runtime
	m.task = task
	m.err = nil
	m.scheduler.Submit(task)
	go m.finishBuild(task)
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

func (m *Manager) finishBuild(task BuildTask) {
	runtime, err := task.Result()
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
	}
	m.notifyChangedLocked()
	if len(m.refs) == 0 {
		runtime, task = m.takeRuntimeLocked()
	} else {
		runtime, task = nil, nil
	}
	m.mu.Unlock()

	cancelTask(task)
	closeRuntime(runtime)
}

func (m *Manager) runtimeForRef(key qviews.QueryViewKey, epoch uint64) (*QueryRuntime, BuildTask, <-chan struct{}, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.epoch[key] != epoch {
		return nil, nil, nil, false
	}
	if _, ok := m.refs[key]; !ok {
		return nil, nil, nil, false
	}
	if m.err != nil && !errors.Is(m.err, context.Canceled) {
		panic(errors.Wrap(m.err, "query runtime initialization failed"))
	}
	if m.runtime == nil && m.task == nil {
		return nil, nil, m.changed, true
	}
	return m.runtime, m.task, nil, true
}

func (m *Manager) oldestDataVersionForRef(key qviews.QueryViewKey, epoch uint64) (qviews.DataVersion, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.epoch[key] != epoch {
		return qviews.DataVersion{}, false
	}
	if _, ok := m.refs[key]; !ok {
		return qviews.DataVersion{}, false
	}
	return minQueryViewDataVersion(m.refs)
}

func (m *Manager) hasRef(key qviews.QueryViewKey, epoch uint64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.epoch[key] != epoch {
		return false
	}
	_, ok := m.refs[key]
	return ok
}

func (m *Manager) takeRuntimeLocked() (*QueryRuntime, BuildTask) {
	runtime, task := m.runtime, m.task
	m.runtime = nil
	m.task = nil
	m.err = nil
	return runtime, task
}

func (m *Manager) notifyChangedLocked() {
	close(m.changed)
	m.changed = make(chan struct{})
}
