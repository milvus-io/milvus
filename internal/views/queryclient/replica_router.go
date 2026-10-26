package queryclient

import (
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/views/replicadiscovery"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const replicaRetryInterval = 100 * time.Millisecond

// replicaRouter retains one subscription per encountered VChannel. Callbacks
// update local state only; the discovery client owns PChannel-shared workers.
// TODO: evict idle collection subscriptions together with proxy metadata GC.
type replicaRouter struct {
	mu       sync.Mutex
	source   QueryPlanClient
	channels map[string]*replicaRoute
	closed   bool
}

type replicaRoute struct {
	mu       sync.Mutex
	snapshot replicadiscovery.Snapshot
	changed  chan struct{}
	next     uint64
	sub      *replicadiscovery.Subscription
	closed   bool
}

func newReplicaRouter(source QueryPlanClient) *replicaRouter {
	return &replicaRouter{source: source, channels: make(map[string]*replicaRoute)}
}

func (r *replicaRouter) route(vc string) (*replicaRoute, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, merr.Wrap(merr.ErrServiceNotReady, "replica router closed")
	}
	if route := r.channels[vc]; route != nil {
		return route, nil
	}
	route := &replicaRoute{changed: make(chan struct{})}
	sub, err := r.source.SubscribeReplicas(vc, func(snapshot replicadiscovery.Snapshot) {
		route.mu.Lock()
		defer route.mu.Unlock()
		if route.closed {
			return
		}
		route.snapshot = snapshot
		close(route.changed)
		route.changed = make(chan struct{})
	})
	if err != nil {
		return nil, err
	}
	route.sub = sub
	r.channels[vc] = route
	return route, nil
}

func (r *replicaRouter) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	for _, route := range r.channels {
		route.mu.Lock()
		route.closed = true
		close(route.changed)
		route.mu.Unlock()
		route.sub.Close()
	}
	r.channels = nil
}

// nextReplica visits every candidate before retrying a temporarily failed one.
// A permanently unavailable candidate is retried only after its advertised
// maximum or QN session directory changes. The caller additionally rejects
// plans containing dead nodes.
func (r *replicaRoute) nextReplica(ctx context.Context, tried map[replicadiscovery.Replica]bool, sessionChanged <-chan struct{}) (replicadiscovery.Replica, bool, error) {
	for {
		select {
		case <-sessionChanged:
			return replicadiscovery.Replica{}, true, nil
		default:
		}
		if err := ctx.Err(); err != nil {
			return replicadiscovery.Replica{}, false, err
		}
		r.mu.Lock()
		if r.closed {
			r.mu.Unlock()
			return replicadiscovery.Replica{}, false, merr.Wrap(merr.ErrServiceNotReady, "replica router closed")
		}
		snapshot, changed := r.snapshot, r.changed
		retry := false
		if snapshot.Valid {
			for i := 0; i < len(snapshot.Replicas); i++ {
				candidate := snapshot.Replicas[(r.next+uint64(i))%uint64(len(snapshot.Replicas))]
				permanent, attempted := tried[candidate]
				if !attempted {
					r.next += uint64(i) + 1
					r.mu.Unlock()
					return candidate, false, nil
				}
				retry = retry || !permanent
			}
		}
		r.mu.Unlock()
		var timer *time.Timer
		var ready <-chan time.Time
		if retry {
			timer = time.NewTimer(replicaRetryInterval)
			ready = timer.C
		}
		select {
		case <-ctx.Done():
			if timer != nil {
				timer.Stop()
			}
			return replicadiscovery.Replica{}, false, ctx.Err()
		case <-sessionChanged:
			if timer != nil {
				timer.Stop()
			}
			return replicadiscovery.Replica{}, true, nil
		case <-changed:
			if timer != nil {
				timer.Stop()
			}
		case <-ready:
			for candidate, permanent := range tried {
				if !permanent {
					delete(tried, candidate)
				}
			}
		}
	}
}

func (r *replicaRoute) hasUntried(tried map[replicadiscovery.Replica]bool) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.snapshot.Valid {
		for _, candidate := range r.snapshot.Replicas {
			if _, ok := tried[candidate]; !ok {
				return true
			}
		}
	}
	return false
}
