package balancer

import (
	"context"
	"io"
	"slices"
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
	cache         *balancercache.Cache
	schemaVersion SchemaVersionProvider
	reconcileMu   sync.Mutex
	viewRegistry  *coordview.ShardViewRegistry
	policy        BalancePolicy
	queue         *triggerQueue
	scanner       balancercache.Scanner
	scanning      bool
	scanRequested bool
	configChanged chan struct{}

	mu     sync.Mutex
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// SchemaVersionProvider reads the authorized preparation schema identity.
// Schema identity is independent of the desired load configuration.
type SchemaVersionProvider func(context.Context, int64) (int32, error)

// NewDefaultBalancer constructs the standard Balancer controller.
func NewDefaultBalancer(
	cache *balancercache.Cache,
	registry *coordview.ShardViewRegistry,
	policy BalancePolicy,
	schemaVersion SchemaVersionProvider,
) *DefaultBalancer {
	if policy == nil {
		policy = NewDefaultBalancePolicy()
	}
	balancer := &DefaultBalancer{
		cache:         cache,
		schemaVersion: schemaVersion,
		viewRegistry:  registry,
		policy:        policy,
		queue:         newTriggerQueue(),
		configChanged: make(chan struct{}, 1),
	}
	if cache != nil {
		balancer.scanner = cache.NewScanner()
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

	unwatch := b.watchBalanceConfig(ctx, paramtable.Get())
	defer unwatch()
	interval := DefaultBalanceConfig().ReconcileInterval
	if b.cache != nil {
		interval = b.cache.GetBalanceConfig().ReconcileInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	resetTicker := func() {
		next := b.cache.GetBalanceConfig().ReconcileInterval
		if next != interval {
			interval = next
			ticker.Reset(interval)
		}
	}

	for {
		var retry <-chan time.Time
		var timer *time.Timer
		if delay, pending := b.queue.retryWait(); pending {
			timer = time.NewTimer(delay)
			retry = timer.C
		}
		select {
		case <-ctx.Done():
			if timer != nil {
				timer.Stop()
			}
			return
		case <-b.configChanged:
			if timer != nil {
				timer.Stop()
			}
			resetTicker()
			continue
		case <-b.queue.signalCh():
		case <-retry:
		case <-ticker.C:
			b.queue.add()
		}
		if timer != nil {
			timer.Stop()
		}
		if err := b.Reconcile(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			mlog.Warn(ctx, "query view balance will retry", mlog.Err(err))
		} else {
			b.queue.resetRetryDelay()
		}
	}
}

// Reconcile handles queued local work or one collection scan batch. A single
// call does not drain a full scan. It is exported primarily for tests and for
// callers that want a synchronous controller pass during startup.
// It waits for cache readiness before consuming queued work; ctx cancels that wait.
func (b *DefaultBalancer) Reconcile(ctx context.Context) error {
	if b.cache == nil || b.viewRegistry == nil || b.policy == nil {
		return nil
	}
	if err := b.cache.WaitForReady(ctx); err != nil {
		return err
	}
	b.reconcileMu.Lock()
	defer b.reconcileMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	pending := b.queue.takePending()
	b.scanRequested = b.scanRequested || pending.scan
	if !pending.hasScopes() && !b.scanning && !b.scanRequested {
		return nil
	}
	// A pass releases reconcileMu before another scan batch can start. Local
	// publications and management calls can therefore run between batches.
	defer func() {
		if b.scanning || b.scanRequested {
			b.queue.wake()
		}
	}()
	reader := newPlanningContext(b.cache)
	b.pruneLayouts(reader, pending.dirtyColls)
	var dirty []qviews.ShardID
	if pending.hasScopes() {
		dirty = resolveCacheScope(reader, pending)
	} else if b.scanning || b.scanRequested {
		if !b.scanning {
			b.scanning, b.scanRequested = true, false
		}
		dirty = b.scanBatch(reader)
	}
	if len(dirty) == 0 {
		return nil
	}
	dirty, err := b.readSchemaVersions(ctx, reader, dirty)
	plan := b.policy.Plan(reader, dirty)
	if plan != nil {
		// A suspended replica may shed serving capacity, but must retain the
		// published schema's recovery base while a newer schema is pending.
		// Otherwise resuming it can only plan the unmaterialized target. An
		// explicit release/removal still tears down its complete obligation.
		plan.Releases = slices.DeleteFunc(plan.Releases, func(id qviews.ShardID) bool {
			cfg, manager := reader.ConfigForShard(id), b.viewRegistry.Get(id)
			return cfg != nil && manager != nil && manager.PublishedRecovery(reader.SchemaVersion(cfg.CollectionID)) != nil
		})
	}
	applyErr := b.apply(ctx, plan)
	recoveryErr := b.recoverPublishedSchemas(ctx, reader, dirty)
	return errors.CombineErrors(err, errors.CombineErrors(applyErr, recoveryErr))
}

const (
	scanShardThreshold = 100
	scanSlotBudget     = 256
)

func (b *DefaultBalancer) scanBatch(reader *planningContext) []qviews.ShardID {
	targets := make(map[qviews.ShardID]struct{})
	collections := make(map[int64]struct{})
	err := b.scanner.Scan(scanSlotBudget, func(collection *balancercache.CollectionEntry) bool {
		collections[collection.ID()] = struct{}{}
		// The scanner pins directory membership only. Re-read current contents
		// through this batch's planning context, just as for a local event.
		addCollectionShards(targets, reader.GetCollection(collection.ID()))
		return len(targets) <= scanShardThreshold
	})
	if err == io.EOF {
		b.scanning = false
	}
	b.pruneLayouts(reader, collections)
	return shardKeys(targets)
}

// Configuration removal also works after the collection has left the cache;
// do not scan every retained layout at the start of each background batch.
func (b *DefaultBalancer) pruneLayouts(reader balancercache.Reader, collections map[int64]struct{}) {
	if policy, ok := b.policy.(*DefaultBalancePolicy); ok && len(collections) > 0 {
		policy.mu.Lock()
		defer policy.mu.Unlock()
		for id := range collections {
			if c := reader.GetCollection(id); c == nil || c.LoadConfig() == nil {
				delete(policy.layouts.domains, id)
			}
		}
	}
}

func (b *DefaultBalancer) apply(ctx context.Context, plan *BalancePlan) error {
	if plan == nil {
		return nil
	}
	var errs []error
	// Planning is batched, but applying a view may wait for its DataView ref.
	// Do not hold a registry-wide flush window across that wait. Each manager
	// submits one ordered shard event; the scheduler still coalesces pending work.
	for _, shardID := range plan.Releases {
		mgr := b.viewRegistry.Get(shardID)
		if mgr == nil {
			continue
		}
		if err := mgr.RequestRelease(ctx); err != nil {
			errs = append(errs, err)
			b.queue.addRetry(TriggerScope{DirtyShards: []qviews.ShardID{shardID}})
		}
	}
	for shardID, builder := range plan.Prepares {
		if builder == nil {
			continue
		}
		mgr := b.viewRegistry.Ensure(shardID)
		if err := mgr.AddPreparing(ctx, builder); err != nil {
			errs = append(errs, err)
			b.queue.addRetry(TriggerScope{DirtyShards: []qviews.ShardID{shardID}})
		}
	}
	if len(plan.Continues) > 0 {
		b.Trigger(TriggerScope{DirtyShards: plan.Continues})
	}
	if len(plan.Retries) > 0 {
		b.queue.addRetry(TriggerScope{DirtyShards: plan.Retries})
		errs = append(errs, merr.WrapErrServiceUnavailableMsg("balance inputs are not ready for %d shards", len(plan.Retries)))
	}
	var err error
	for _, e := range errs {
		err = errors.CombineErrors(err, e)
	}
	return err
}

// Read once per affected collection per pass, including startup recovery and
// periodic reconciliation. Failed metadata reads leave serving views intact.
func (b *DefaultBalancer) readSchemaVersions(ctx context.Context, reader *planningContext, dirty []qviews.ShardID) ([]qviews.ShardID, error) {
	if b.schemaVersion == nil {
		return dirty, nil
	}
	reader.schemaVersions = make(map[int64]int32)
	failed := make(map[int64]error)
	var result error
	ready := make([]qviews.ShardID, 0, len(dirty))
	for _, shard := range dirty {
		cfg := reader.ConfigForShard(shard)
		if cfg == nil {
			ready = append(ready, shard) // Releases need no schema.
			continue
		}
		id := cfg.CollectionID
		if _, read := reader.schemaVersions[id]; !read && failed[id] == nil {
			version, err := b.schemaVersion(ctx, id)
			if err != nil {
				failed[id] = err
				result = errors.CombineErrors(result, err)
				b.queue.addRetry(TriggerScope{DirtyCollections: []int64{id}})
			} else {
				reader.schemaVersions[id] = version
			}
		}
		if failed[id] == nil {
			ready = append(ready, shard)
		}
	}
	return ready, result
}
