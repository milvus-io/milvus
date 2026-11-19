package queryclient

import (
	"container/list"
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// QueryViewCircuitBreakerConfig bounds both routing memory and failure recovery.
// Zero values use defaults. Configuration is immutable after construction.
type QueryViewCircuitBreakerConfig struct {
	ViewExclusionTTL time.Duration
	SessionGoneTTL   time.Duration
	MaxRoutingWait   time.Duration
	MaxNodes         int
	MaxViews         int
}

// QueryViewCircuitBreaker shares exact View and confirmed session exclusions.
type QueryViewCircuitBreaker struct {
	mu      sync.Mutex
	cfg     QueryViewCircuitBreakerConfig
	nodes   map[int64]time.Time
	views   map[string]map[qviews.QueryViewKey]*list.Element
	viewTTL list.List
	changed chan struct{}
}

type excludedView struct {
	key     qviews.QueryViewKey
	expires time.Time
}

type routingAttemptKey struct{}

// A missing picker entry is avoided until the picker changes. A confirmed
// session removal is avoided until expiry, even if the shared table is full.
type localNodeExclusion struct {
	changed <-chan struct{}
	until   time.Time
}

type routingAttempt struct {
	constraints            *viewpb.QueryRoutingConstraints
	changed, pickerChanged <-chan struct{}
	failedNodes            map[int64]localNodeExclusion
	failedViews            map[qviews.QueryViewKey]time.Time
}

func NewQueryViewCircuitBreaker(cfg QueryViewCircuitBreakerConfig) *QueryViewCircuitBreaker {
	if cfg.ViewExclusionTTL <= 0 {
		cfg.ViewExclusionTTL = time.Minute
	}
	if cfg.SessionGoneTTL <= 0 {
		cfg.SessionGoneTTL = 5 * time.Minute
	}
	if cfg.MaxRoutingWait <= 0 {
		cfg.MaxRoutingWait = 5 * time.Second
	}
	if cfg.MaxNodes <= 0 {
		cfg.MaxNodes = 1024
	}
	if cfg.MaxViews <= 0 {
		cfg.MaxViews = 16384
	}
	return &QueryViewCircuitBreaker{cfg: cfg, nodes: make(map[int64]time.Time), views: make(map[string]map[qviews.QueryViewKey]*list.Element), changed: make(chan struct{})}
}

func (b *QueryViewCircuitBreaker) notifyLocked() {
	close(b.changed)
	b.changed = make(chan struct{})
}

func (b *QueryViewCircuitBreaker) removeViewLocked(e *list.Element, reason string) {
	v := e.Value.(excludedView)
	delete(b.views[v.key.ShardID.VChannel], v.key)
	if len(b.views[v.key.ShardID.VChannel]) == 0 {
		delete(b.views, v.key.ShardID.VChannel)
	}
	b.viewTTL.Remove(e)
	metrics.QueryViewCircuitBreakerEvents.WithLabelValues("view", reason).Inc()
}

func (b *QueryViewCircuitBreaker) expireLocked(now time.Time) {
	for e := b.viewTTL.Front(); e != nil && !e.Value.(excludedView).expires.After(now); e = b.viewTTL.Front() {
		b.removeViewLocked(e, "expired")
	}
	for id, until := range b.nodes {
		if !until.After(now) {
			delete(b.nodes, id)
			metrics.QueryViewCircuitBreakerEvents.WithLabelValues("node", "expired").Inc()
		}
	}
}

func (b *QueryViewCircuitBreaker) begin(ctx context.Context, vchannel string, failedNodes map[int64]localNodeExclusion, failedViews map[qviews.QueryViewKey]time.Time, availability func() ([]int64, <-chan struct{})) (context.Context, *routingAttempt) {
	a := &routingAttempt{constraints: &viewpb.QueryRoutingConstraints{}, failedNodes: failedNodes, failedViews: failedViews}
	var unavailable []int64
	if availability != nil {
		unavailable, a.pickerChanged = availability()
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	b.expireLocked(now)
	a.changed = b.changed
	nodes := make(map[int64]struct{})
	for _, id := range unavailable {
		nodes[id] = struct{}{}
	}
	for id, record := range failedNodes {
		select {
		case <-record.changed:
			delete(failedNodes, id)
			continue
		default:
		}
		if !record.until.IsZero() && !now.Before(record.until) {
			delete(failedNodes, id)
			continue
		}
		nodes[id] = struct{}{}
		if a.pickerChanged == nil {
			a.pickerChanged = record.changed
		}
	}
	for id := range b.nodes {
		nodes[id] = struct{}{}
	}
	for id := range nodes {
		a.constraints.ExcludedQueryNodeIds = append(a.constraints.ExcludedQueryNodeIds, id)
	}
	views := make(map[qviews.QueryViewKey]struct{})
	for key := range b.views[vchannel] {
		views[key] = struct{}{}
	}
	for key, until := range failedViews {
		if until.After(now) {
			views[key] = struct{}{}
		} else {
			delete(failedViews, key)
		}
	}
	for key := range views {
		a.constraints.ExcludedQueryViews = append(a.constraints.ExcludedQueryViews, &viewpb.QueryViewIdentity{ReplicaId: key.ShardID.ReplicaID, Version: key.QueryViewVersion.IntoProto()})
	}
	return context.WithValue(ctx, routingAttemptKey{}, a), a
}

func viewUnavailable(err error) bool {
	ve := viewerror.AsViewError(err)
	return ve != nil && (ve.IsViewNotFound() || ve.IsViewInvalidated())
}

func (b *QueryViewCircuitBreaker) excludeViewLocked(key qviews.QueryViewKey, now time.Time) {
	if existing := b.views[key.ShardID.VChannel][key]; existing != nil {
		if existing.Value.(excludedView).expires.After(now) {
			return
		}
		b.removeViewLocked(existing, "expired")
	}
	if b.viewTTL.Len() >= b.cfg.MaxViews {
		b.removeViewLocked(b.viewTTL.Front(), "capacity")
	}
	if b.views[key.ShardID.VChannel] == nil {
		b.views[key.ShardID.VChannel] = make(map[qviews.QueryViewKey]*list.Element)
	}
	e := b.viewTTL.PushBack(excludedView{key: key, expires: now.Add(b.cfg.ViewExclusionTTL)})
	b.views[key.ShardID.VChannel][key] = e
	metrics.QueryViewCircuitBreakerEvents.WithLabelValues("view", "excluded").Inc()
	b.notifyLocked()
}

func (b *QueryViewCircuitBreaker) observe(ctx context.Context, qn qviews.QueryNode, key qviews.QueryViewKey, started time.Time, err error) {
	if err == nil || errors.IsAny(err, context.Canceled, context.DeadlineExceeded) || status.Code(err) == codes.Canceled || status.Code(err) == codes.DeadlineExceeded || merr.GetErrorType(err) == merr.InputError {
		return
	}
	var missing *viewerror.NodeSessionUnavailable
	isViewFailure := viewUnavailable(err)
	if !isViewFailure && !errors.As(err, &missing) {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	a, _ := ctx.Value(routingAttemptKey{}).(*routingAttempt)
	if isViewFailure {
		if now.Sub(started) >= b.cfg.ViewExclusionTTL {
			return
		}
		b.excludeViewLocked(key, now)
		if a != nil {
			if _, recorded := a.failedViews[key]; !recorded {
				a.failedViews[key] = now.Add(b.cfg.ViewExclusionTTL)
			}
		}
		return
	}
	if !missing.Gone {
		if a != nil && ctx.Err() == nil {
			a.failedNodes[qn.ID] = localNodeExclusion{changed: missing.Changed}
		}
		return
	}
	if now.Sub(started) >= b.cfg.SessionGoneTTL {
		return
	}
	until := now.Add(b.cfg.SessionGoneTTL)
	if a != nil {
		a.failedNodes[qn.ID] = localNodeExclusion{until: until}
	}
	if b.nodes[qn.ID].After(now) {
		return
	}
	b.expireLocked(now)
	if len(b.nodes) >= b.cfg.MaxNodes {
		metrics.QueryViewCircuitBreakerEvents.WithLabelValues("node", "capacity").Inc()
		return
	}
	b.nodes[qn.ID] = until
	metrics.QueryViewCircuitBreakerEvents.WithLabelValues("node", "session_gone").Inc()
	b.notifyLocked()
}

// Periodic replanning also discovers newly published Views on the SN.
func (a *routingAttempt) wait(ctx context.Context) error {
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-a.changed:
	case <-a.pickerChanged:
	case <-timer.C:
	}
	return nil
}

func (a *routingAttempt) stale() bool {
	select {
	case <-a.changed:
	case <-a.pickerChanged:
	default:
		return false
	}
	return true
}
