package viewresource

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
)

const defaultLiveEventBufferSize = 1024

// QueryRuntime is the vchannel singleton owned by SNQueryRuntimeManager.
type QueryRuntime struct {
	mu      sync.Mutex
	cond    *sync.Cond
	state   queryRuntimeState
	modules []QueryRuntimeModule

	pending      []walview.VChannelResourceEvent
	pendingLimit int

	drainWG sync.WaitGroup
	applyMu sync.Mutex

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
	runtime := &QueryRuntime{
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
	r.drainWG.Add(1)
	go func() {
		defer r.drainWG.Done()
		r.drainLoop()
	}()
	r.cond.Broadcast()
	r.mu.Unlock()
	if hasAdvance {
		r.advanceModules(advance)
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
	defer r.mu.Unlock()
	if r.state == queryRuntimeClosed {
		return false
	}
	for r.pendingLimit > 0 && len(r.pending) >= r.pendingLimit && r.state != queryRuntimeClosed {
		r.cond.Wait()
	}
	if r.state == queryRuntimeClosed {
		return false
	}
	r.pending = append(r.pending, event)
	r.cond.Signal()
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
	r.cond.Broadcast()
	modules := append([]QueryRuntimeModule(nil), r.modules...)
	r.mu.Unlock()
	r.drainWG.Wait()
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

func (r *QueryRuntime) drainLoop() {
	for {
		r.mu.Lock()
		for len(r.pending) == 0 && r.state != queryRuntimeClosed {
			r.cond.Wait()
		}
		if r.state == queryRuntimeClosed {
			r.mu.Unlock()
			return
		}
		batch := r.pending
		r.pending = nil
		r.cond.Broadcast()
		r.mu.Unlock()
		r.applyBatch(context.Background(), batch)
	}
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
