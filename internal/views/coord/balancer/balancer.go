package balancer

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/errors"

	balancercache "github.com/milvus-io/milvus/internal/views/coord/balancer/cache"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

const defaultTickerInterval = 10 * time.Second

// Balancer is the scheduling controller that reconciles dirty shards into
// QueryView prepare/release operations.
type Balancer interface {
	Start(ctx context.Context)
	Stop()
	Trigger(scopes ...TriggerScope)
}

// DefaultBalancer owns the trigger queue and reconcile loop. Business
// decisions are delegated to BalancePolicy; this type reads the cache,
// drains dirty work, and applies the resulting BalancePlan.
type DefaultBalancer struct {
	cache          *balancercache.Cache
	reconcileMu    sync.Mutex
	viewRegistry   *coordview.ShardViewRegistry
	policy         BalancePolicy
	queue          *triggerQueue
	tickerInterval time.Duration

	mu     sync.Mutex
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewDefaultBalancer constructs the standard Balancer controller.
func NewDefaultBalancer(
	cache *balancercache.Cache,
	registry *coordview.ShardViewRegistry,
	policy BalancePolicy,
) *DefaultBalancer {
	if policy == nil {
		policy = NewDefaultBalancePolicy()
	}
	interval := defaultTickerInterval
	if cache != nil && cache.GetBalanceConfig().TickerInterval > 0 {
		interval = cache.GetBalanceConfig().TickerInterval
	}
	balancer := &DefaultBalancer{
		cache:          cache,
		viewRegistry:   registry,
		policy:         policy,
		queue:          newTriggerQueue(),
		tickerInterval: interval,
	}
	if cache != nil {
		cache.SetNotifier(func(scope TriggerScope) { balancer.Trigger(scope) })
	}
	return balancer
}

// Start launches the reconcile loop and enqueues an initial full scan.
func (b *DefaultBalancer) Start(ctx context.Context) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.cancel != nil {
		return
	}
	loopCtx, cancel := context.WithCancel(ctx)
	b.cancel = cancel
	b.wg.Add(1)
	go b.loop(loopCtx)
	b.queue.add()
}

// Stop cancels the reconcile loop and waits for it to exit.
func (b *DefaultBalancer) Stop() {
	b.mu.Lock()
	cancel := b.cancel
	b.cancel = nil
	b.mu.Unlock()

	if cancel != nil {
		cancel()
		b.wg.Wait()
	}
}

// Trigger enqueues affected shards. Calling Trigger with no scopes enqueues a
// full scan.
func (b *DefaultBalancer) Trigger(scopes ...TriggerScope) {
	b.queue.add(scopes...)
}

func (b *DefaultBalancer) loop(ctx context.Context) {
	defer b.wg.Done()

	ticker := time.NewTicker(b.tickerInterval)
	defer ticker.Stop()

	retryDelay := 100 * time.Millisecond
	for {
		interval := paramtable.Get().QueryCoordCfg.QueryViewFullReconsileInterval.GetAsDuration(time.Second)
		if b.cache != nil && b.cache.GetBalanceConfig().TickerInterval > 0 {
			interval = b.cache.GetBalanceConfig().TickerInterval
		}
		if interval != b.tickerInterval {
			b.tickerInterval = interval
			ticker.Reset(interval)
		}
		select {
		case <-ctx.Done():
			return
		case <-b.queue.signalCh():
		case <-ticker.C:
			b.queue.add()
			continue
		}

		if err := b.Reconcile(ctx); err != nil {
			mlog.Warn(ctx, "query view balance will retry", mlog.Err(err))
			timer := time.NewTimer(retryDelay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			retryDelay = min(5*time.Second, retryDelay*2)
		} else {
			retryDelay = 100 * time.Millisecond
		}
	}
}

// Reconcile runs one reconcile cycle. It is exported primarily for tests and
// for callers that want a synchronous controller pass during startup.
func (b *DefaultBalancer) Reconcile(ctx context.Context) error {
	b.reconcileMu.Lock()
	defer b.reconcileMu.Unlock()
	if b.cache == nil || b.viewRegistry == nil || b.policy == nil {
		return nil
	}
	if !b.cache.Ready() {
		return merr.WrapErrServiceNotReadyMsg("balancer cache sources have not completed initial replay")
	}
	pending := b.queue.takePending()
	if pending.empty() {
		return nil
	}
	reader := newPlanningContext(b.cache)
	dirty := resolveCacheScope(reader, pending)
	if len(dirty) == 0 {
		return nil
	}
	plan := b.policy.Plan(reader, dirty)
	return b.apply(ctx, plan)
}

func (b *DefaultBalancer) apply(ctx context.Context, plan *BalancePlan) error {
	if plan == nil {
		return nil
	}
	batch := b.viewRegistry.Begin()
	defer batch.Commit()
	var errs []error
	for _, shardID := range plan.Releases {
		mgr := b.viewRegistry.Get(shardID)
		if mgr == nil {
			continue
		}
		if err := mgr.RequestRelease(ctx); err != nil {
			errs = append(errs, err)
			b.Trigger(TriggerScope{DirtyShards: []qviews.ShardID{shardID}})
		}
	}
	for shardID, builder := range plan.Prepares {
		if builder == nil {
			continue
		}
		mgr := b.viewRegistry.Ensure(shardID)
		if err := mgr.AddPreparing(ctx, builder); err != nil {
			errs = append(errs, err)
			b.Trigger(TriggerScope{DirtyShards: []qviews.ShardID{shardID}})
		}
	}
	if len(plan.Retries) > 0 {
		b.Trigger(TriggerScope{DirtyShards: plan.Retries})
		errs = append(errs, merr.WrapErrServiceUnavailableMsg("balance inputs are not ready for %d shards", len(plan.Retries)))
	}
	var err error
	for _, e := range errs {
		err = errors.CombineErrors(err, e)
	}
	return err
}
