package queryresource

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
)

const defaultLiveEventBufferSize = 1024

// QueryRuntime is the vchannel singleton owned by VChannelRecoveryModule.
type QueryRuntime struct {
	mu      sync.Mutex
	cond    *sync.Cond
	state   queryRuntimeState
	modules []QueryRuntimeModule

	pending      []walview.VChannelResourceEvent
	pendingLimit int

	dispatcher     *Dispatcher
	drainScheduled bool
	applyMu        sync.Mutex

	latestAdvance qviews.DataVersion
	hasAdvance    bool
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
	runtime := &QueryRuntime{
		dispatcher:   dispatcher,
		state:        queryRuntimePreparing,
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
	advance, hasAdvance := r.recordedAdvance()
	r.mu.Lock()
	if r.state == queryRuntimeClosed {
		r.mu.Unlock()
		if err := ctx.Err(); err != nil {
			return err
		}
		return context.Canceled
	}
	r.state = queryRuntimeReady
	drain := r.markDrainScheduledLocked()
	r.cond.Broadcast()
	r.mu.Unlock()
	if hasAdvance {
		r.advanceModules(advance)
	}
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
	r.mu.Lock()
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

func (r *QueryRuntime) PrepareDataVersion(ctx context.Context, dataVersion qviews.DataVersion) error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	if r.state != queryRuntimeReady {
		r.mu.Unlock()
		return context.Canceled
	}
	modules := append([]QueryRuntimeModule(nil), r.modules...)
	r.mu.Unlock()
	for _, module := range modules {
		versioned, ok := module.(QueryRuntimeVersionedModule)
		if !ok {
			continue
		}
		if err := versioned.PrepareDataVersion(ctx, dataVersion); err != nil {
			return err
		}
	}
	return nil
}

func (r *QueryRuntime) ReleaseDataVersion(dataVersion qviews.DataVersion) {
	if r == nil {
		return
	}
	r.mu.Lock()
	modules := append([]QueryRuntimeModule(nil), r.modules...)
	r.mu.Unlock()
	for _, module := range modules {
		if versioned, ok := module.(QueryRuntimeVersionedModule); ok {
			versioned.ReleaseDataVersion(dataVersion)
		}
	}
}

func (r *QueryRuntime) recordedAdvance() (qviews.DataVersion, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.latestAdvance, r.hasAdvance
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

func (r *QueryRuntime) Close() {
	if r == nil {
		return
	}
	r.mu.Lock()
	if r.state == queryRuntimeClosed {
		r.mu.Unlock()
		return
	}
	r.state = queryRuntimeClosed
	r.pending = nil
	r.drainScheduled = false
	r.cond.Broadcast()
	modules := append([]QueryRuntimeModule(nil), r.modules...)
	r.mu.Unlock()
	r.applyMu.Lock()
	defer r.applyMu.Unlock()
	for i := len(modules) - 1; i >= 0; i-- {
		if modules[i] != nil {
			modules[i].Close()
		}
	}
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
	if r.state == queryRuntimeClosed {
		r.drainScheduled = false
		r.cond.Broadcast()
		return nil, false
	}
	if len(r.pending) == 0 {
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
		for _, module := range modules {
			if module != nil {
				module.ApplyLiveEvent(ctx, event)
			}
		}
	}
}
