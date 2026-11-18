package queryclient

import (
	"container/list"
	"context"
	"io"
	"math/rand/v2"
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
	NodeFailureThreshold int
	FailureWindow        time.Duration
	InitialBackoff       time.Duration
	MaxBackoff           time.Duration
	ViewExclusionTTL     time.Duration
	SessionGoneTTL       time.Duration
	MaxRoutingWait       time.Duration
	MaxNodes             int
	MaxViews             int
}

// QueryViewCircuitBreaker shares QueryNode observations across requests, never topology.
// Node connections recover through exclusive probes; gone sessions and invalid
// View identities are bounded negative caches. All state is protected by mu.
type QueryViewCircuitBreaker struct {
	mu      sync.Mutex
	cfg     QueryViewCircuitBreakerConfig
	nodes   map[int64]*nodeCircuit
	views   map[string]map[qviews.QueryViewKey]*list.Element
	viewTTL list.List
	changed chan struct{}
}

type nodeCircuit struct {
	failures   int
	window     time.Time
	expires    time.Time
	retryAt    time.Time
	backoff    time.Duration
	generation uint64
	probe      *routingAttempt
	gone       bool
}

type excludedView struct {
	key     qviews.QueryViewKey
	expires time.Time
}

type routingAttemptKey struct{}

// An attempt snapshots constraints before planning and reserves probes only
// after the plan arrives. Probe and observation state uses the breaker's mutex.
type routingAttempt struct {
	breaker     *QueryViewCircuitBreaker
	started     time.Time
	constraints *viewpb.QueryRoutingConstraints
	changed     <-chan struct{}
	probes      map[int64]*nodeCircuit
	failedNodes map[int64]time.Time
	failedViews map[qviews.QueryViewKey]time.Time
}

type nodeObservation struct {
	entry      *nodeCircuit
	generation uint64
	attempt    *routingAttempt
	started    time.Time
}

func NewQueryViewCircuitBreaker(cfg QueryViewCircuitBreakerConfig) *QueryViewCircuitBreaker {
	if cfg.NodeFailureThreshold <= 0 {
		cfg.NodeFailureThreshold = 3
	}
	if cfg.FailureWindow <= 0 {
		cfg.FailureWindow = 10 * time.Second
	}
	if cfg.InitialBackoff <= 0 {
		cfg.InitialBackoff = time.Second
	}
	if cfg.MaxBackoff < cfg.InitialBackoff {
		cfg.MaxBackoff = max(30*time.Second, cfg.InitialBackoff)
	}
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
	return &QueryViewCircuitBreaker{cfg: cfg, nodes: make(map[int64]*nodeCircuit), views: make(map[string]map[qviews.QueryViewKey]*list.Element), changed: make(chan struct{})}
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
	for id, entry := range b.nodes {
		if entry.probe == nil && !entry.expires.After(now) {
			delete(b.nodes, id)
			metrics.QueryViewCircuitBreakerEvents.WithLabelValues("node", "expired").Inc()
		}
	}
}

func (b *QueryViewCircuitBreaker) begin(ctx context.Context, vchannel string, failedNodes map[int64]time.Time, failedViews map[qviews.QueryViewKey]time.Time) (context.Context, *routingAttempt) {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	b.expireLocked(now)
	a := &routingAttempt{breaker: b, started: now, constraints: &viewpb.QueryRoutingConstraints{}, changed: b.changed, probes: make(map[int64]*nodeCircuit), failedNodes: failedNodes, failedViews: failedViews}
	nodes := make(map[int64]struct{})
	for id, until := range failedNodes {
		if until.After(now) {
			nodes[id] = struct{}{}
		} else {
			delete(failedNodes, id)
		}
	}
	for id, entry := range b.nodes {
		if entry.gone || entry.probe != nil || entry.retryAt.After(now) {
			nodes[id] = struct{}{}
		}
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

// acquireProbes rechecks shared exclusions and atomically reserves only the
// selected QNs. A competing attempt may have claimed a probe during planning;
// reject the entire plan without reserving a partial set in that case.
func (a *routingAttempt) acquireProbes(plan *viewpb.QueryPlan) bool {
	b := a.breaker
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	b.expireLocked(now)
	a.changed = b.changed
	key := qviews.QueryViewKey{ShardID: qviews.FromProtoShardID(plan.ShardId), QueryViewVersion: qviews.FromProtoQueryViewVersion(plan.Version)}
	if b.views[key.ShardID.VChannel][key] != nil {
		return false
	}
	for _, work := range plan.GetWorkNodes() {
		if node := work.GetQueryNode(); node != nil {
			if entry := b.nodes[node.NodeId]; entry != nil && (entry.gone || entry.probe != nil || entry.retryAt.After(now)) {
				return false
			}
		}
	}
	for _, work := range plan.GetWorkNodes() {
		if node := work.GetQueryNode(); node != nil {
			if entry := b.nodes[node.NodeId]; entry != nil && !entry.retryAt.IsZero() && entry.probe == nil {
				entry.probe = a
				a.probes[node.NodeId] = entry
				metrics.QueryViewCircuitBreakerEvents.WithLabelValues("node", "probe").Inc()
			}
		}
	}
	return true
}

// release runs after execution joins, including cancellation. An unobserved
// probe is not a failure and does not grow backoff. Wake admission waiters.
func (a *routingAttempt) release() {
	b := a.breaker
	b.mu.Lock()
	defer b.mu.Unlock()
	released := false
	for id, entry := range a.probes {
		if b.nodes[id] == entry && entry.probe == a {
			entry.probe = nil
			entry.retryAt = time.Now().Add(shardRetryInterval)
			released = true
		}
		delete(a.probes, id)
	}
	if released {
		b.notifyLocked()
	}
}

func (b *QueryViewCircuitBreaker) observeStart(ctx context.Context, qn qviews.QueryNode) nodeObservation {
	a, _ := ctx.Value(routingAttemptKey{}).(*routingAttempt)
	b.mu.Lock()
	defer b.mu.Unlock()
	token := nodeObservation{entry: b.nodes[qn.ID], attempt: a, started: time.Now()}
	if token.entry != nil {
		token.generation = token.entry.generation
	}
	return token
}

func viewUnavailable(err error) bool {
	ve := viewerror.AsViewError(err)
	return ve != nil && (ve.IsViewNotFound() || ve.IsViewInvalidated())
}

func nodeConnectionFailure(err error) bool {
	if err == nil || (merr.GetErrorType(err) == merr.InputError) {
		return false
	}
	if ve := viewerror.AsViewError(err); ve != nil && ve.Code != viewpb.ViewCode_VIEW_CODE_UNKNOWN {
		return false
	}
	return status.Code(err) == codes.Unavailable || errors.Is(err, io.EOF)
}

func (b *QueryViewCircuitBreaker) excludeViewLocked(key qviews.QueryViewKey, started, now time.Time) {
	// Old in-flight failures cannot repopulate an already expired generation.
	if now.Sub(started) >= b.cfg.ViewExclusionTTL {
		return
	}
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

func (b *QueryViewCircuitBreaker) observe(ctx context.Context, qn qviews.QueryNode, key qviews.QueryViewKey, token nodeObservation, err error) {
	if errors.IsAny(err, context.Canceled, context.DeadlineExceeded) || status.Code(err) == codes.Canceled || status.Code(err) == codes.DeadlineExceeded || (merr.GetErrorType(err) == merr.InputError) {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	started := now
	if token.attempt != nil {
		started = token.attempt.started
	}
	if viewUnavailable(err) {
		b.excludeViewLocked(key, started, now)
		if token.attempt != nil && now.Sub(started) < b.cfg.ViewExclusionTTL {
			if _, recorded := token.attempt.failedViews[key]; !recorded {
				token.attempt.failedViews[key] = now.Add(b.cfg.ViewExclusionTTL)
			}
		}
	}
	entry := b.nodes[qn.ID]
	var session *viewerror.NodeSessionUnavailable
	gone := errors.As(err, &session) && session.Gone
	connectionFailure := nodeConnectionFailure(err) && ctx.Err() == nil
	if !gone && !connectionFailure {
		// A valid View error proves reachability too. Local/unknown failures
		// and remote shutdown do not establish a successful connection probe.
		if err != nil && !viewUnavailable(err) {
			return
		}
		if entry != nil && !entry.gone && token.entry == entry && token.generation == entry.generation && (entry.retryAt.IsZero() || (entry.probe != nil && entry.probe == token.attempt)) {
			if entry.failures == 0 && entry.retryAt.IsZero() {
				return
			}
			// Retain a bounded generation fence so an older callback cannot
			// reopen a connection that a later probe has already recovered.
			entry.generation++
			entry.failures, entry.backoff, entry.probe = 0, 0, nil
			entry.retryAt = time.Time{}
			entry.expires = now.Add(b.cfg.SessionGoneTTL)
			metrics.QueryViewCircuitBreakerEvents.WithLabelValues("node", "recovered").Inc()
			b.notifyLocked()
		}
		return
	}
	if now.Sub(token.started) >= b.cfg.SessionGoneTTL {
		return
	}
	if token.attempt != nil {
		ttl := b.cfg.InitialBackoff
		if gone {
			ttl = b.cfg.SessionGoneTTL
		}
		token.attempt.failedNodes[qn.ID] = now.Add(ttl)
	}
	if entry != nil && entry.gone {
		return
	}
	if !gone && entry == nil && token.entry != nil {
		return
	}
	if !gone && entry != nil && entry.generation != 0 && (entry != token.entry || token.generation != entry.generation) {
		return
	}
	if !gone && entry != nil && !entry.retryAt.IsZero() && (entry != token.entry || token.generation != entry.generation || entry.probe != token.attempt || entry.probe == nil) {
		return
	}
	if entry == nil {
		b.expireLocked(now)
		if len(b.nodes) >= b.cfg.MaxNodes {
			metrics.QueryViewCircuitBreakerEvents.WithLabelValues("node", "capacity").Inc()
			return
		}
		entry = &nodeCircuit{window: now}
		b.nodes[qn.ID] = entry
	}
	entry.expires = now.Add(b.cfg.SessionGoneTTL)
	if gone {
		entry.gone = true
		entry.probe = nil
		entry.generation++
		metrics.QueryViewCircuitBreakerEvents.WithLabelValues("node", "session_gone").Inc()
		b.notifyLocked()
		return
	}
	if now.Sub(entry.window) >= b.cfg.FailureWindow {
		entry.window, entry.failures = now, 0
	}
	entry.failures++
	if entry.probe != nil || entry.failures >= b.cfg.NodeFailureThreshold {
		entry.backoff = min(b.cfg.MaxBackoff, max(b.cfg.InitialBackoff, 2*entry.backoff))
		entry.retryAt = now.Add(time.Duration(float64(entry.backoff) * (0.8 + 0.4*rand.Float64())))
		entry.probe = nil
		entry.generation++
		metrics.QueryViewCircuitBreakerEvents.WithLabelValues("node", "connection_open").Inc()
		b.notifyLocked()
	}
}

// Polling is bounded to notice newly published Views without a discovery stream.
// State changes wake the wait early; callers enforce a total routing wait budget.
func (a *routingAttempt) wait(ctx context.Context) error {
	b := a.breaker
	b.mu.Lock()
	delay := time.Second
	now := time.Now()
	for _, entry := range b.nodes {
		if !entry.gone && entry.probe == nil && entry.retryAt.After(now) {
			delay = min(delay, entry.retryAt.Sub(now))
		}
	}
	b.mu.Unlock()
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-a.changed:
		return nil
	case <-timer.C:
		return nil
	}
}
