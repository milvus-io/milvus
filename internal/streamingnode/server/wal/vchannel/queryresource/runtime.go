package queryresource

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const defaultLiveEventBufferSize = 1024

// QueryRuntime is the vchannel singleton owned by VChannelRecoveryModule.
type QueryRuntime struct {
	views     map[*viewResources]struct{}
	closeOnce sync.Once
	mu        sync.Mutex
	cond      *sync.Cond
	state     queryRuntimeState
	modules   []QueryRuntimeModule

	pending      []walview.VChannelResourceEvent
	pendingLimit int

	dispatcher      *Dispatcher
	drainScheduled  bool
	applyMu         sync.Mutex
	indexMu         sync.Mutex         // serializes load-info resolution and index publication
	partitionScopes map[uint64][]int64 // immutable scopes retained for serving views
	indexVersion    uint64             // load-info identity, not an ordered version

	latestAdvance qviews.DataVersion
	hasAdvance    bool

	prepareView func(context.Context) error
	closed      chan struct{}
	ctx         context.Context
	cancel      context.CancelFunc
}

type queryRuntimeState int

const (
	queryRuntimePreparing queryRuntimeState = iota
	queryRuntimeReady
	queryRuntimeClosed
)

func NewQueryRuntime(modules ...QueryRuntimeModule) *QueryRuntime {
	return newQueryRuntime(nil, modules...)
}

func (r *QueryRuntime) RangeModules(fn func(QueryRuntimeModule) bool) {
	if r == nil || fn == nil {
		return
	}
	r.mu.Lock()
	modules := append([]QueryRuntimeModule(nil), r.modules...)
	r.mu.Unlock()
	for _, module := range modules {
		if !fn(module) {
			return
		}
	}
}

func newQueryRuntime(dispatcher *Dispatcher, modules ...QueryRuntimeModule) *QueryRuntime {
	ctx, cancel := context.WithCancel(context.Background())
	runtime := &QueryRuntime{
		ctx: ctx, cancel: cancel,
		dispatcher:   dispatcher,
		state:        queryRuntimePreparing,
		closed:       make(chan struct{}),
		pendingLimit: defaultLiveEventBufferSize,
		modules:      append([]QueryRuntimeModule(nil), modules...),
	}
	runtime.cond = sync.NewCond(&runtime.mu)
	return runtime
}

func (r *QueryRuntime) Initialize(ctx context.Context, view walview.VChannelWALView) error {
	if r == nil {
		return nil
	}
	view.RuntimeContext = r.ctx
	r.indexVersion = view.LoadInfoVersion
	r.partitionScopes = map[uint64][]int64{view.LoadInfoVersion: append([]int64{}, view.PartitionIDs...)}
	view.ResourceEventBarrier = func(ctx context.Context) error {
		return r.resourceEventBarrier(ctx, view.WithResourceEventLock, nil)
	}
	r.mu.Lock()
	r.prepareView = func(ctx context.Context) error {
		return r.resourceEventBarrier(ctx, view.WithResourceEventLock, view.PrepareQueryView)
	}
	r.mu.Unlock()
	for _, module := range r.modules {
		if module == nil {
			continue
		}
		if err := module.Prepare(ctx, view); err != nil {
			return err
		}
	}
	initial := r.takeInitialBatch()
	r.applyBatch(ctx, initial)
	// Serialize the initial watermark with Advance and module event application.
	r.applyMu.Lock()
	r.mu.Lock()
	if r.state == queryRuntimeClosed {
		r.mu.Unlock()
		r.applyMu.Unlock()
		if err := ctx.Err(); err != nil {
			return err
		}
		return context.Canceled
	}
	advance, hasAdvance := r.latestAdvance, r.hasAdvance
	r.state = queryRuntimeReady
	drain := r.markDrainScheduledLocked()
	r.cond.Broadcast()
	r.mu.Unlock()
	if hasAdvance {
		r.advanceModules(advance)
	}
	r.applyMu.Unlock()
	if drain {
		r.submitDrain()
	}
	return nil
}

func (r *QueryRuntime) ObserveEvent(ctx context.Context, event walview.VChannelResourceEvent) bool {
	if r == nil {
		return false
	}
	select {
	case <-ctx.Done():
		return false
	default:
	}
	r.mu.Lock()
	if r.state == queryRuntimeClosed {
		r.mu.Unlock()
		return false
	}
	for r.pendingLimit > 0 && len(r.pending) >= r.pendingLimit && r.state != queryRuntimeClosed {
		r.cond.Wait()
	}
	if r.state == queryRuntimeClosed {
		r.mu.Unlock()
		return false
	}
	r.pending = append(r.pending, event)
	drain := r.markDrainScheduledLocked()
	r.cond.Signal()
	r.mu.Unlock()
	if drain {
		r.submitDrain()
	}
	return true
}

func (r *QueryRuntime) Advance(oldestDataVersion qviews.DataVersion) {
	if r == nil {
		return
	}
	r.applyMu.Lock()
	defer r.applyMu.Unlock()
	r.mu.Lock()
	if r.state == queryRuntimeClosed {
		r.mu.Unlock()
		return
	}
	if r.hasAdvance && r.latestAdvance.GT(oldestDataVersion) {
		r.mu.Unlock()
		panic("non-monotonic query runtime advance")
	}
	r.latestAdvance = oldestDataVersion
	r.hasAdvance = true
	ready := r.state == queryRuntimeReady
	r.mu.Unlock()
	if ready {
		r.advanceModules(oldestDataVersion)
	}
}

// PrepareQueryView fences sealed handoffs before View resources are prepared.
func (r *QueryRuntime) PrepareQueryView(ctx context.Context) error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	if r.state != queryRuntimeReady {
		r.mu.Unlock()
		return context.Canceled
	}
	prepare := r.prepareView
	r.mu.Unlock()
	return prepare(ctx)
}

// PrepareIndexMeta refreshes a reused runtime before the new view can serve.
// Serialize preparation of the shared write resources. Exact LoadInfo reads are
// immutable references; read bindings prepare their own selected index metadata.
func (r *QueryRuntime) PrepareIndexMeta(ctx context.Context, meta *viewpb.QueryViewMeta, provider LoadInfoProvider) error {
	version := meta.GetLoadInfoVersion()
	if version == 0 || provider == nil {
		return nil
	}
	r.indexMu.Lock()
	defer r.indexMu.Unlock()
	r.mu.Lock()
	_, scopePrepared := r.partitionScopes[version]
	r.mu.Unlock()
	if r.indexVersion == version && scopePrepared {
		return nil
	}
	info, err := provider.QueryViewLoadInfo(ctx, meta.GetCollectionId(), version)
	if err != nil {
		return err
	}
	for _, module := range r.modules {
		if partitioned, ok := module.(interface {
			PreparePartitions(context.Context, *viewpb.QueryViewMeta, []int64) error
		}); ok {
			if err := partitioned.PreparePartitions(ctx, meta, info.PartitionIDs); err != nil {
				return err
			}
		}
	}
	// Do not hold applyMu during the RPC. WAL application and Close must remain
	// independent of coordinator availability.
	r.applyMu.Lock()
	defer r.applyMu.Unlock()
	r.mu.Lock()
	ready := r.state == queryRuntimeReady
	r.mu.Unlock()
	if !ready {
		return context.Canceled
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, module := range r.modules {
		if indexed, ok := module.(QueryRuntimeGrowingIndexModule); ok {
			if err := indexed.PrepareGrowingIndexes(ctx, info.IndexInfos); err != nil {
				return err
			}
		}
	}
	r.mu.Lock()
	if r.partitionScopes == nil {
		r.partitionScopes = make(map[uint64][]int64)
	}
	r.partitionScopes[version] = append([]int64{}, info.PartitionIDs...)
	r.mu.Unlock()
	r.indexVersion = version
	return nil
}

func (r *QueryRuntime) BeforeRelease(ctx context.Context, version qviews.DataVersion) error {
	r.mu.Lock()
	closed := r.state == queryRuntimeClosed
	r.mu.Unlock()
	if closed {
		return r.tryClose()
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(r.ctx, cancel)
	defer stop()

	var result error
	r.RangeModules(func(module QueryRuntimeModule) bool {
		if module, ok := module.(QueryRuntimeReleaseModule); ok {
			result = module.BeforeRelease(ctx, version)
		}
		return result == nil
	})
	return result
}

// resourceEventBarrier orders the optional owner check and preceding sealed/WAL
// events before readiness. Close also wakes waiters whose barrier was discarded.
func (r *QueryRuntime) resourceEventBarrier(ctx context.Context, withOwnerLock func(func()), prepare func() bool) error {
	done := make(chan struct{})
	var err error
	enqueue := func() {
		if prepare != nil && !prepare() {
			err = merr.WrapErrServiceNotReadyMsg("segment final commits are pending")
			return
		}
		if !r.ObserveEvent(ctx, walview.VChannelResourceEvent{Barrier: func() { close(done) }}) {
			err = context.Canceled
		}
	}
	if withOwnerLock != nil {
		withOwnerLock(enqueue)
	} else {
		enqueue()
	}
	if err != nil {
		return err
	}
	select {
	case <-done:
		return nil
	case <-r.closed:
		return context.Canceled
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *QueryRuntime) advanceModules(oldestDataVersion qviews.DataVersion) {
	r.mu.Lock()
	modules := append([]QueryRuntimeModule(nil), r.modules...)
	r.mu.Unlock()
	for _, module := range modules {
		if module != nil {
			module.Advance(oldestDataVersion)
		}
	}
}

// stop wakes producers and cancels operations without waiting under owner locks.
func (r *QueryRuntime) stop() []*viewResources {
	r.mu.Lock()
	if r.state != queryRuntimeClosed {
		r.state = queryRuntimeClosed
		close(r.closed)
		r.cancel()
		r.pending = nil
		r.drainScheduled = false
		r.cond.Broadcast()
	}
	views := make([]*viewResources, 0, len(r.views))
	for v := range r.views {
		views = append(views, v)
	}
	r.mu.Unlock()
	for _, v := range views {
		v.retire()
	}
	return views
}

func (r *QueryRuntime) tryClose() error {
	for _, v := range r.stop() {
		if err := v.release(); err != nil {
			return err
		}
	}
	r.Close()
	return nil
}

func (r *QueryRuntime) Close() {
	if r == nil {
		return
	}
	views := r.stop()
	r.closeOnce.Do(func() {
		for _, v := range views {
			<-v.drained
			_ = v.release()
			<-v.closed
		}
		r.applyMu.Lock()
		defer r.applyMu.Unlock()
		for i := len(r.modules) - 1; i >= 0; i-- {
			if r.modules[i] != nil {
				r.modules[i].Close()
			}
		}
	})
}

func (r *QueryRuntime) takeInitialBatch() []walview.VChannelResourceEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	batch := r.pending
	r.pending = nil
	r.cond.Broadcast()
	return batch
}

func (r *QueryRuntime) markDrainScheduledLocked() bool {
	if r.state != queryRuntimeReady || len(r.pending) == 0 || r.drainScheduled {
		return false
	}
	r.drainScheduled = true
	return true
}

func (r *QueryRuntime) submitDrain() {
	if r.dispatcher != nil && r.dispatcher.Submit(r) {
		return
	}
	r.drainReady()
}

func (r *QueryRuntime) drainReady() {
	for {
		batch, ok := r.takeDrainBatch()
		if !ok {
			return
		}
		r.applyBatch(context.Background(), batch)
	}
}

func (r *QueryRuntime) takeDrainBatch() ([]walview.VChannelResourceEvent, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state == queryRuntimeClosed || len(r.pending) == 0 {
		r.drainScheduled = false
		r.cond.Broadcast()
		return nil, false
	}
	batch := r.pending
	r.pending = nil
	r.cond.Broadcast()
	return batch, true
}

func (r *QueryRuntime) applyBatch(ctx context.Context, batch []walview.VChannelResourceEvent) {
	r.applyMu.Lock()
	defer r.applyMu.Unlock()
	r.mu.Lock()
	modules := append([]QueryRuntimeModule(nil), r.modules...)
	r.mu.Unlock()
	for _, event := range batch {
		if event.Barrier != nil {
			event.Barrier()
			continue
		}
		for _, module := range modules {
			if module != nil {
				module.ApplyLiveEvent(ctx, event)
			}
		}
	}
}

// CheckWriteSchemaVersion checks WAL application after the event barrier.
// Preparing an older read schema must not invalidate another view's resources.
func (r *QueryRuntime) CheckWriteSchemaVersion(version int32) error {
	var err error
	r.RangeModules(func(module QueryRuntimeModule) bool {
		if resource, ok := module.(QueryRuntimeWriteSchemaModule); ok {
			if applied := resource.WriteSchemaVersion(); applied < version {
				err = merr.WrapErrServiceNotReadyMsg("query runtime write schema %d has not reached %d", applied, version)
			}
		}
		return err == nil
	})
	return err
}
