package queryresource

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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
	// Admission watermark, distinct from the minimum retained reclamation version.
	latestDataVersion qviews.DataVersion
}

type queryViewRef struct {
	meta            *viewpb.QueryViewMeta
	onReady         func()
	onUnrecoverable func()
}

func NewManager(config Config) *Manager {
	return &Manager{
		builders:         defaultQueryRuntimeModuleBuilders(config.Builders),
		scheduler:        config.Scheduler,
		dispatcher:       config.Dispatcher,
		loadInfoProvider: config.LoadInfoProvider,
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
		version := req.Key.QueryViewVersion.DataVersion
		if m.err != nil || (len(m.refs) != 0 && m.latestDataVersion.GT(version)) {
			m.mu.Unlock()
			m.Reject(req)
			return
		}
		m.latestDataVersion = version
		if m.refs == nil {
			m.refs = make(map[qviews.QueryViewKey]queryViewRef)
		}
		m.refs[req.Key] = queryViewRef{
			meta:            proto.Clone(req.Meta).(*viewpb.QueryViewMeta),
			onReady:         req.OnReady,
			onUnrecoverable: req.OnUnrecoverable,
		}
		if m.runtime == nil && m.task == nil {
			m.startBuildLocked(m.oldestQueryViewMetaLocked(), build)
		} else if m.runtime != nil && m.task == nil && m.err == nil {
			notifyReady = true
		}
	}
	m.mu.Unlock()
	if notifyReady {
		m.submitReady(req.Key, req.OnReady)
	}
}

// TryBuildLocked retries a deferred query runtime build after the owning
// VChannel state changes. The caller should hold the owning VChannel lock.
func (m *Manager) TryBuildLocked(build ViewBuilder) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed || m.err != nil || len(m.refs) == 0 || m.runtime != nil || m.task != nil {
		return
	}
	m.startBuildLocked(m.oldestQueryViewMetaLocked(), build)
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
	// Keep reference removal and watermark delivery ordered across replicas.
	// BeforeRelease may read object storage and remains outside this lock.
	if hasAdvance && advanceRuntime != nil {
		advanceRuntime.Advance(advance)
	}
	m.mu.Unlock()
	cancelTask(task)
	closeRuntime(runtime)
	if hasAdvance && advanceRuntime != nil {
		m.scheduler.Submit(resourceReleaseTask{runtime: advanceRuntime, version: advance, onDropped: req.OnDropped})
	} else {
		m.submitCallback(req.OnDropped)
	}
}

// Reject completes an acquisition that cannot be reconstructed without
// registering a QueryView reference. The callback is submitted to the node
// scheduler to preserve the asynchronous ResourceManager contract.
func (m *Manager) Reject(req snview.AcquireResource) {
	m.submitCallback(req.OnUnrecoverable)
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

func (m *Manager) OldestDataVersion() (qviews.DataVersion, bool) {
	if m == nil {
		return qviews.DataVersion{}, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return minQueryViewDataVersion(m.refs)
}

func (m *Manager) Close() {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.closed = true
	runtime, task := m.takeRuntimeLocked()
	m.refs = nil
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
		if event.Message != nil {
			event.Message = walview.CopyMessage(event.Message)
		}
		runtime.ObserveEvent(ctx, event)
	}
}

func (m *Manager) startBuildLocked(meta *viewpb.QueryViewMeta, build ViewBuilder) bool {
	if build == nil {
		panic("query resource view builder is nil")
	}
	view, ok := build(meta)
	if !ok {
		return false
	}
	if m.scheduler == nil {
		m.scheduler = nodescheduler.Get()
	}
	if m.dispatcher == nil {
		panic("query resource dispatcher is nil")
	}
	runtime := newQueryRuntime(m.dispatcher, m.newModules()...)
	m.runtime = runtime
	withOwnerLock := view.WithResourceEventLock
	var task *resourceBuildTask
	task = newResourceBuildTask(func(ctx context.Context) (*QueryRuntime, error) {
		if err := ctx.Err(); err != nil {
			return runtime, err
		}
		if runtime == nil {
			// Each retry captures a fresh snapshot and installs its live observer in
			// the same owner critical section. Never Prepare a partially built module.
			var captureErr error
			capture := func() {
				m.mu.Lock()
				defer m.mu.Unlock()
				if m.closed || m.task == nil || m.task.task != task || len(m.refs) == 0 {
					captureErr = context.Canceled
					return
				}
				var ok bool
				view, ok = build(m.oldestQueryViewMetaLocked())
				if !ok {
					captureErr = nodescheduler.ErrDelay
					return
				}
				runtime = newQueryRuntime(m.dispatcher, m.newModules()...)
				m.runtime = runtime
			}
			if withOwnerLock != nil {
				withOwnerLock(capture)
			} else {
				capture()
			}
			if captureErr != nil {
				return nil, captureErr
			}
		}
		resolved, err := m.resolveLoadInfo(ctx, view)
		if err == nil {
			err = runtime.Initialize(ctx, resolved)
		}
		if err == nil {
			return runtime, nil
		}
		// Close outside the owner/manager locks, also waking a producer blocked on
		// the failed runtime's full event queue before the next snapshot capture.
		closeRuntime(runtime)
		m.mu.Lock()
		if m.runtime == runtime {
			m.runtime = nil
		}
		m.mu.Unlock()
		runtime = nil
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if errors.Is(err, wal.ErrTransformLogStartPointTruncated) || errors.Is(err, merr.ErrDataIntegrity) {
			return nil, err
		}
		mlog.Warn(ctx, "retry query runtime preparation", mlog.FieldVChannel(view.VChannel), mlog.Err(err))
		return nil, errors.Mark(merr.Wrap(err, "initialize query runtime"), nodescheduler.ErrDelay)
	})
	m.task = scheduleResourceBuild(m.scheduler, task, m.finishBuild)
	m.err = nil
	return true
}

func (m *Manager) oldestQueryViewMetaLocked() *viewpb.QueryViewMeta {
	var oldest qviews.QueryViewVersion
	var meta *viewpb.QueryViewMeta
	for key, ref := range m.refs {
		if meta == nil || oldest.GT(key.QueryViewVersion) {
			oldest = key.QueryViewVersion
			meta = ref.meta
		}
	}
	return meta
}

func (m *Manager) resolveLoadInfo(ctx context.Context, view walview.VChannelWALView) (walview.VChannelWALView, error) {
	if view.LoadInfoVersion == 0 || m.loadInfoProvider == nil {
		return view, nil
	}
	loadInfo, err := m.loadInfoProvider.QueryViewLoadInfo(ctx, view.CollectionID, view.LoadInfoVersion)
	if err != nil {
		return view, err
	}
	// A resolved empty list means no loaded partitions; nil in a WAL-only
	// snapshot retains the legacy unrestricted scope.
	view.PartitionIDs = append([]int64{}, loadInfo.PartitionIDs...)
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
			panic(merr.Wrap(err, "create query runtime module"))
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
	var unrecoverable []func()
	var failedRuntime *QueryRuntime
	m.mu.Lock()
	if m.task != task {
		m.mu.Unlock()
		closeRuntime(runtime)
		return
	}
	m.task = nil
	if err != nil {
		m.err = err
		failedRuntime, m.runtime = m.runtime, nil
		if !errors.Is(err, context.Canceled) {
			for _, ref := range m.refs {
				unrecoverable = append(unrecoverable, ref.onUnrecoverable)
			}
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
	closeRuntime(failedRuntime)
	for _, callback := range unrecoverable {
		m.submitCallback(callback)
	}
	for key, onReady := range ready {
		m.submitReady(key, onReady)
	}
}

func (m *Manager) submitReady(key qviews.QueryViewKey, onReady func()) {
	if onReady == nil {
		return
	}
	m.mu.Lock()
	if m.scheduler == nil {
		m.scheduler = nodescheduler.Get()
	}
	scheduler := m.scheduler
	m.mu.Unlock()
	scheduler.Submit(resourceReadyTask{
		manager: m,
		key:     key,
		onReady: onReady,
	})
}

func (m *Manager) prepareReady(ctx context.Context, key qviews.QueryViewKey, onReady func()) error {
	m.mu.Lock()
	_, ok := m.refs[key]
	runtime := m.runtime
	ready := ok && runtime != nil && m.task == nil && m.err == nil
	m.mu.Unlock()
	if !ready {
		return nil
	}
	// TODO(#40451): bind/reprepare resources when load_info_version or schema
	// changes. Reusing this runtime currently preserves its initial load scope.
	if err := runtime.PrepareQueryView(ctx, key.QueryViewVersion.DataVersion); err != nil {
		if errors.Is(err, context.Canceled) || ctx.Err() != nil {
			return err
		}
		return errors.Mark(err, nodescheduler.ErrDelay)
	}
	m.mu.Lock()
	_, ok = m.refs[key]
	ready = ok && m.runtime == runtime && m.task == nil && m.err == nil
	m.mu.Unlock()
	if !ready {
		return nil
	}
	onReady()
	return nil
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
