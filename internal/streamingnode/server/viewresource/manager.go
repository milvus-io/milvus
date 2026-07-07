package viewresource

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/viewresource/growingruntime"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
)

// SNQueryRuntimeManager prepares and owns StreamingNode query runtimes for one PChannel runtime.
type SNQueryRuntimeManager interface {
	walview.LoadConfigListener
	snview.StreamingNodeResourceManager

	Close()
}

type resourceState struct {
	initRef       bool
	queryViewRefs map[qviews.QueryViewKey]struct{}
	changed       chan struct{}

	runtime *QueryRuntime
	task    BuildTask
	err     error
}

// queryRuntimeManager is the concrete PChannel-local query runtime manager.
type queryRuntimeManager struct {
	mu             sync.Mutex
	moduleBuilders []QueryRuntimeModuleBuilder
	scheduler      Scheduler

	resources map[string]*resourceState
	refIndex  map[qviews.QueryViewKey]string
	refEpoch  map[qviews.QueryViewKey]uint64
	closed    bool
}

func NewManager(moduleBuilders ...QueryRuntimeModuleBuilder) SNQueryRuntimeManager {
	if len(moduleBuilders) == 0 {
		moduleBuilders = []QueryRuntimeModuleBuilder{NewGrowingRuntimeModuleBuilder(nil)}
	}
	return &queryRuntimeManager{
		moduleBuilders: append([]QueryRuntimeModuleBuilder(nil), moduleBuilders...),
		scheduler:      NewScheduler(4),
		resources:      make(map[string]*resourceState),
		refIndex:       make(map[qviews.QueryViewKey]string),
		refEpoch:       make(map[qviews.QueryViewKey]uint64),
	}
}

func newResourceState() *resourceState {
	return &resourceState{
		queryViewRefs: make(map[qviews.QueryViewKey]struct{}),
		changed:       make(chan struct{}),
	}
}

func (m *queryRuntimeManager) OnAlterLoadConfig(view walview.VChannelWALView) walview.VChannelLiveObserver {
	runtime := NewQueryRuntime(m.newModules()...)
	task := newResourceBuildTask(context.Background(), func(ctx context.Context) (*QueryRuntime, error) {
		if err := runtime.Initialize(ctx, view); err != nil {
			return runtime, err
		}
		return runtime, nil
	})

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		task.Cancel()
		runtime.Close()
		return nil
	}
	state := m.resources[view.VChannel]
	if state != nil && (state.runtime != nil || state.task != nil) {
		m.mu.Unlock()
		runtime.Close()
		return nil
	}
	if state == nil {
		state = newResourceState()
		state.initRef = true
		m.resources[view.VChannel] = state
	}
	state.runtime = runtime
	state.task = task
	state.err = nil
	m.notifyStateChangedLocked(state)
	m.mu.Unlock()

	m.scheduler.Submit(task)
	go m.finishBuild(view.VChannel, task)
	return runtime
}

func (m *queryRuntimeManager) newModules() []QueryRuntimeModule {
	modules := make([]QueryRuntimeModule, 0, len(m.moduleBuilders))
	for _, builder := range m.moduleBuilders {
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

func (m *queryRuntimeManager) OnDropLoadConfig(event walview.DropLoadConfigEvent) {
	m.mu.Lock()
	state := m.resources[event.VChannel]
	if state != nil {
		state.initRef = false
	}
	runtime, task := m.cleanupIfUnreferencedLocked(event.VChannel)
	m.mu.Unlock()

	cancelTask(task)
	closeRuntime(runtime)
}

func (m *queryRuntimeManager) finishBuild(vchannel string, task BuildTask) {
	runtime, err := task.Result()
	m.mu.Lock()
	state := m.resources[vchannel]
	if state == nil || state.task != task {
		m.mu.Unlock()
		closeRuntime(runtime)
		return
	}
	state.task = nil
	if err != nil {
		if errors.Is(err, context.Canceled) {
			state.err = err
		} else {
			m.mu.Unlock()
			panic(errors.Wrap(err, "initialize query runtime"))
		}
	} else {
		state.runtime = runtime
		state.err = nil
	}
	m.notifyStateChangedLocked(state)
	runtime, cleanupTask := m.cleanupIfUnreferencedLocked(vchannel)
	m.mu.Unlock()

	cancelTask(cleanupTask)
	closeRuntime(runtime)
}

func (m *queryRuntimeManager) Acquire(req snview.AcquireResource) {
	epoch := m.registerQueryViewRef(req)
	go m.waitRuntimeReady(req.Key, epoch, req.OnReady)
}

func (m *queryRuntimeManager) Release(req snview.ReleaseResource) {
	var runtime *QueryRuntime
	var task BuildTask
	var advanceRuntime *QueryRuntime
	var advance qviews.DataVersion
	var hasAdvance bool

	m.mu.Lock()
	vchannel, ok := m.refIndex[req.Key]
	if ok {
		delete(m.refIndex, req.Key)
		if state := m.resources[vchannel]; state != nil {
			delete(state.queryViewRefs, req.Key)
			m.notifyStateChangedLocked(state)
			advance, hasAdvance = minQueryViewDataVersion(state.queryViewRefs)
			advanceRuntime = state.runtime
		}
		runtime, task = m.cleanupIfUnreferencedLocked(vchannel)
	}
	m.mu.Unlock()

	if ok && hasAdvance && advanceRuntime != nil {
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

func (m *queryRuntimeManager) Close() {
	m.mu.Lock()
	remainingQueryViewRefs := m.queryViewRefCountLocked()
	m.closed = true
	runtimes := make([]*QueryRuntime, 0, len(m.resources))
	tasks := make([]BuildTask, 0, len(m.resources))
	for vchannel, state := range m.resources {
		m.notifyStateChangedLocked(state)
		if state.task != nil {
			tasks = append(tasks, state.task)
		}
		if state.runtime != nil {
			runtimes = append(runtimes, state.runtime)
		}
		delete(m.resources, vchannel)
	}
	m.refIndex = make(map[qviews.QueryViewKey]string)
	m.refEpoch = make(map[qviews.QueryViewKey]uint64)
	m.mu.Unlock()

	for _, task := range tasks {
		cancelTask(task)
	}
	for _, runtime := range runtimes {
		closeRuntime(runtime)
	}
	if m.scheduler != nil {
		m.scheduler.Close()
	}
	if remainingQueryViewRefs > 0 {
		panic(errors.Errorf("query runtime manager closed with %d query view references", remainingQueryViewRefs))
	}
}

func (m *queryRuntimeManager) registerQueryViewRef(req snview.AcquireResource) uint64 {
	if req.Meta == nil || req.Meta.GetVersion() == nil || req.Meta.GetVersion().GetDataVersion() == nil {
		panic("query view meta version is nil")
	}
	vchannel := req.Meta.GetVchannel()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		panic("query runtime manager is closed")
	}
	state := m.resources[vchannel]
	if state == nil {
		state = newResourceState()
		m.resources[vchannel] = state
	}
	if existing, ok := m.refIndex[req.Key]; ok && existing != vchannel {
		panic("query view already references a different runtime")
	}
	if state.queryViewRefs == nil {
		state.queryViewRefs = make(map[qviews.QueryViewKey]struct{})
	}
	if _, ok := state.queryViewRefs[req.Key]; !ok {
		m.assertMonotonicAcquireLocked(state, req.Key.QueryViewVersion.DataVersion)
		state.queryViewRefs[req.Key] = struct{}{}
		m.refIndex[req.Key] = vchannel
		m.refEpoch[req.Key]++
		state.initRef = false
		m.notifyStateChangedLocked(state)
	}
	return m.refEpoch[req.Key]
}

func (m *queryRuntimeManager) assertMonotonicAcquireLocked(state *resourceState, version qviews.DataVersion) {
	for key := range state.queryViewRefs {
		if key.QueryViewVersion.DataVersion.GT(version) {
			panic("non-monotonic query view acquire")
		}
	}
}

func (m *queryRuntimeManager) waitRuntimeReady(key qviews.QueryViewKey, epoch uint64, onReady func()) {
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
		if onReady != nil && m.hasQueryViewRef(key, epoch) {
			onReady()
		}
		return
	}
}

func (m *queryRuntimeManager) runtimeForRef(key qviews.QueryViewKey, epoch uint64) (*QueryRuntime, BuildTask, <-chan struct{}, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.refEpoch[key] != epoch {
		return nil, nil, nil, false
	}
	vchannel, ok := m.refIndex[key]
	if !ok {
		return nil, nil, nil, false
	}
	state := m.resources[vchannel]
	if state == nil {
		return nil, nil, nil, false
	}
	if state.err != nil && !errors.Is(state.err, context.Canceled) {
		panic(errors.Wrap(state.err, "query runtime initialization failed"))
	}
	if state.runtime == nil && state.task == nil {
		return nil, nil, state.changed, true
	}
	return state.runtime, state.task, nil, true
}

func (m *queryRuntimeManager) oldestDataVersionForRef(key qviews.QueryViewKey, epoch uint64) (qviews.DataVersion, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.refEpoch[key] != epoch {
		return qviews.DataVersion{}, false
	}
	vchannel, ok := m.refIndex[key]
	if !ok {
		return qviews.DataVersion{}, false
	}
	state := m.resources[vchannel]
	if state == nil {
		return qviews.DataVersion{}, false
	}
	return minQueryViewDataVersion(state.queryViewRefs)
}

func (m *queryRuntimeManager) hasQueryViewRef(key qviews.QueryViewKey, epoch uint64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.refEpoch[key] != epoch {
		return false
	}
	vchannel, ok := m.refIndex[key]
	if !ok {
		return false
	}
	state := m.resources[vchannel]
	if state == nil {
		return false
	}
	_, ok = state.queryViewRefs[key]
	return ok
}

func (m *queryRuntimeManager) cleanupIfUnreferencedLocked(vchannel string) (*QueryRuntime, BuildTask) {
	state := m.resources[vchannel]
	if state == nil {
		return nil, nil
	}
	if state.initRef || len(state.queryViewRefs) > 0 {
		return nil, nil
	}
	m.notifyStateChangedLocked(state)
	delete(m.resources, vchannel)
	return state.runtime, state.task
}

func (m *queryRuntimeManager) notifyStateChangedLocked(state *resourceState) {
	close(state.changed)
	state.changed = make(chan struct{})
}

func (m *queryRuntimeManager) queryViewRefCountLocked() int {
	count := 0
	for _, state := range m.resources {
		count += len(state.queryViewRefs)
	}
	return count
}

func minQueryViewDataVersion(refs map[qviews.QueryViewKey]struct{}) (qviews.DataVersion, bool) {
	var min qviews.DataVersion
	ok := false
	for key := range refs {
		version := key.QueryViewVersion.DataVersion
		if !ok || min.GT(version) {
			min = version
			ok = true
		}
	}
	return min, ok
}

func cancelTask(task BuildTask) {
	if task != nil {
		task.Cancel()
	}
}

func closeRuntime(runtime *QueryRuntime) {
	if runtime != nil {
		runtime.Close()
	}
}

type growingRuntimeModuleBuilder struct {
	builder growingruntime.Builder
}

func NewGrowingRuntimeModuleBuilder(builder growingruntime.Builder) QueryRuntimeModuleBuilder {
	if builder == nil {
		builder = growingruntime.SnapshotBuilder{}
	}
	return growingRuntimeModuleBuilder{builder: builder}
}

func (b growingRuntimeModuleBuilder) NewRuntime() (QueryRuntimeModule, error) {
	return b.builder.NewRuntime()
}
