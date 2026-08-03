package balancer

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/qviews"
)

const defaultTickerInterval = 10 * time.Second

// Balancer is the scheduling controller that reconciles dirty shards into
// QueryView prepare/release operations.
type Balancer interface {
	Start(ctx context.Context)
	Stop()
	Trigger(scopes ...TriggerScope)
}

type snapshotSource interface {
	build(ctx context.Context, pending triggerBatch) (*BalancerSnapshot, []qviews.ShardID)
}

// DefaultBalancer owns the trigger queue and reconcile loop. Business
// decisions are delegated to BalancePolicy; this type only builds snapshots,
// drains dirty work, and applies the resulting BalancePlan.
type DefaultBalancer struct {
	snapshotBuilder snapshotSource
	viewRegistry    *coordview.ShardViewRegistry
	policy          BalancePolicy
	queue           *triggerQueue
	tickerInterval  time.Duration

	mu     sync.Mutex
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewDefaultBalancer constructs the standard Balancer controller.
func NewDefaultBalancer(
	builder *SnapshotBuilder,
	registry *coordview.ShardViewRegistry,
	policy BalancePolicy,
) *DefaultBalancer {
	if policy == nil {
		policy = NewDefaultBalancePolicy()
	}
	interval := defaultTickerInterval
	if builder != nil && builder.config != nil && builder.config.TickerInterval > 0 {
		interval = builder.config.TickerInterval
	}
	var source snapshotSource
	if builder != nil {
		source = builder
	}
	balancer := &DefaultBalancer{
		snapshotBuilder: source,
		viewRegistry:    registry,
		policy:          policy,
		queue:           newTriggerQueue(),
		tickerInterval:  interval,
	}
	if builder != nil {
		balancer.registerNodeChangedNotifier(builder.nodeProvider)
	}
	return balancer
}

func (b *DefaultBalancer) registerNodeChangedNotifier(provider NodeProvider) {
	notifier, ok := provider.(NodeChangedNotifier)
	if !ok {
		return
	}
	notifier.RegisterNodeChangedNotifier(func() {
		b.Trigger(TriggerScope{NodeChanged: true})
	})
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

	for {
		select {
		case <-ctx.Done():
			return
		case <-b.queue.signalCh():
		case <-ticker.C:
			b.queue.add()
			continue
		}

		_ = b.Reconcile(ctx)
	}
}

// Reconcile runs one reconcile cycle. It is exported primarily for tests and
// for callers that want a synchronous controller pass during startup.
func (b *DefaultBalancer) Reconcile(ctx context.Context) error {
	if b.snapshotBuilder == nil || b.viewRegistry == nil || b.policy == nil {
		return nil
	}
	// Take this cycle's work before building the snapshot so triggers arriving
	// during snapshot construction remain queued for the next cycle.
	pending := b.queue.takePending()
	if pending.empty() {
		return nil
	}
	snap, dirty := b.snapshotBuilder.build(ctx, pending)
	if len(dirty) == 0 {
		return nil
	}
	plan := b.policy.Plan(snap, dirty)
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
		}
	}
	for shardID, builder := range plan.Prepares {
		if builder == nil {
			continue
		}
		mgr := b.viewRegistry.Ensure(shardID)
		if err := mgr.AddPreparing(ctx, builder); err != nil {
			errs = append(errs, err)
		}
	}
	var err error
	for _, e := range errs {
		err = errors.CombineErrors(err, e)
	}
	return err
}
