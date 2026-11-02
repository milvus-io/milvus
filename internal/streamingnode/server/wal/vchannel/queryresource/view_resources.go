package queryresource

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

// QueryViewResource is a View-owned reference, including while preparation is
// pending. Release runs only after that View's preparation and planning drain.
type QueryViewResource interface {
	Prepare(context.Context, LoadInfoProvider) error
	Release()
}

// QueryViewResourceModule binds immutable metadata under the owner lock. BindView
// only acquires ownership; snapshot and external I/O belong in Prepare.
type QueryViewResourceModule interface {
	BindView(*viewpb.QueryViewMeta) QueryViewResource
}

// viewResources follows the QN ViewResources ownership rule: withdrawing a View
// cancels its work, but its resource guards survive every operation borrowing them.
type viewResources struct {
	mu          sync.Mutex
	ctx         context.Context
	cancel      context.CancelFunc
	users       int
	released    bool
	closed      chan struct{}
	cleanupOnce sync.Once
	drained     chan struct{}
	releaseMu   sync.Mutex
	prepareMu   sync.Mutex
	modules     []QueryViewResource
	runtime     *QueryRuntime
}

func (r *QueryRuntime) bindView(meta *viewpb.QueryViewMeta) *viewResources {
	ctx, cancel := context.WithCancel(r.ctx)
	v := &viewResources{ctx: ctx, cancel: cancel, drained: make(chan struct{}), closed: make(chan struct{}), runtime: r}
	for _, module := range r.modules {
		if module, ok := module.(QueryViewResourceModule); ok {
			v.modules = append(v.modules, module.BindView(meta))
		}
	}
	r.mu.Lock()
	if r.views == nil {
		r.views = make(map[*viewResources]struct{})
	}
	r.views[v] = struct{}{}
	r.mu.Unlock()
	return v
}

func (v *viewResources) begin(ctx context.Context) (context.Context, func(), error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.released || v.ctx.Err() != nil {
		return ctx, nil, context.Canceled
	}
	v.users++
	ctx, cancel := contextutil.MergeContext(ctx, v.ctx)
	return ctx, func() {
		cancel()
		v.mu.Lock()
		defer v.mu.Unlock()
		v.users--
		if v.released && v.users == 0 {
			close(v.drained)
		}
	}, nil
}

func (v *viewResources) retire() {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.released {
		return
	}
	v.released = true
	v.cancel()
	if v.users == 0 {
		close(v.drained)
	}
}

func (v *viewResources) release() error {
	if v == nil {
		return nil
	}
	select {
	case <-v.drained:
	default:
		return nodescheduler.ErrDelay
	}
	if !v.releaseMu.TryLock() {
		return nodescheduler.ErrDelay
	}
	defer v.releaseMu.Unlock()
	for len(v.modules) > 0 {
		last := len(v.modules) - 1
		v.modules[last].Release()
		v.modules = v.modules[:last]
	}
	v.runtime.mu.Lock()
	delete(v.runtime.views, v)
	v.runtime.mu.Unlock()
	v.cleanupOnce.Do(func() { close(v.closed) })
	return nil
}

func (v *viewResources) prepare(ctx context.Context, provider LoadInfoProvider) error {
	if !v.prepareMu.TryLock() {
		return nodescheduler.ErrDelay
	}
	defer v.prepareMu.Unlock()
	for _, resource := range v.modules {
		if err := resource.Prepare(ctx, provider); err != nil {
			return err
		}
	}
	return nil
}
