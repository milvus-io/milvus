package balancer

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
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

// DefaultBalancer discovers dirty shards, takes priority batches, and applies
// the plans produced by BalancePolicy.
type DefaultBalancer struct {
	cache         *balancercache.Cache
	schemaVersion SchemaVersionProvider
	reconcileMu   sync.Mutex
	viewRegistry  *coordview.ShardViewRegistry
	policy        BalancePolicy
	discovery     *discovery
	running       atomic.Bool
	work          chan struct{}
	submitMu      sync.Mutex
	ready         shardQueue
	scoreRevision atomic.Uint64
	configChanged chan struct{}

	mu     sync.Mutex
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// SchemaVersionProvider reads the authorized DDL target for admission and
// published-view recovery. New QueryViews bind the selected DataView schema.
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

		work:          make(chan struct{}, 1),
		ready:         shardQueue{items: make(map[qviews.ShardID]*queuedShard)},
		configChanged: make(chan struct{}, 1),
	}
	balancer.discovery = newDiscovery()
	if cache != nil {
		cache.SetScheduler(balancer.publishScope)
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
	b.running.Store(true)
	b.wg.Go(func() { b.run(loopCtx) })
	wakeBalancer(b.work) // Resume queued work even if the startup scan is empty.
	b.Trigger()
}

// Stop cancels the reconcile loop and waits for it to exit.
func (b *DefaultBalancer) Stop() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.cancel != nil {
		b.cancel()
		b.wg.Wait()
		b.discovery.suspend()
		b.cancel = nil
		b.running.Store(false)
	}
}

// Trigger enqueues affected shards. Calling Trigger with no scopes enqueues a
// full scan.
func (b *DefaultBalancer) Trigger(scopes ...TriggerScope) {
	if len(scopes) == 0 {
		b.discovery.add(periodicScan, TriggerScope{NodeChanged: true})
		return
	}
	for _, s := range scopes {
		if len(s.DirtyShards) > 0 && len(s.DirtyShards) <= discoveryShardLimit && b.cache != nil && b.cache.Ready() {
			b.submitShards(s.DirtyShards)
			s.DirtyShards = nil
		}
		b.publishScope(s)
	}
}

// Cache callbacks can run under the registry writer lock. Only record the
// affected scope here; the producer reads inputs after publication.
func (b *DefaultBalancer) publishScope(s TriggerScope) {
	if len(s.DirtyShards) > 0 {
		b.discovery.add(localShards, TriggerScope{DirtyShards: slices.Clone(s.DirtyShards)})
	}
	if len(s.DirtyCollections) > 0 {
		b.discovery.add(localChanges, TriggerScope{DirtyCollections: slices.Clone(s.DirtyCollections)})
	}
	for _, replica := range s.RetiringReplicas {
		b.discovery.add(localChanges, TriggerScope{RetiringReplicas: []int64{replica}})
	}
	if len(s.DirtyNodes) > 0 {
		b.discovery.add(topologyChanges, TriggerScope{DirtyNodes: slices.Clone(s.DirtyNodes)})
	} else if s.NodeChanged {
		b.discovery.add(periodicScan, TriggerScope{NodeChanged: true})
	}
	for _, group := range s.DirtyResourceGroups {
		b.discovery.add(topologyChanges, TriggerScope{DirtyResourceGroups: []string{group}})
	}
}

func wakeBalancer(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// All producers score before taking the queue lock. An overlapping, slower
// notification cannot overwrite a newer priority already in the queue.
func (b *DefaultBalancer) submitShards(ids []qviews.ShardID) {
	if len(ids) == 0 {
		return
	}
	revision := b.scoreRevision.Add(1)
	reader := b.scoringContext()
	scored := make([]scoredShard, 0, len(ids))
	seen := make(map[qviews.ShardID]struct{}, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		if collection := reader.collectionForShard(id); collection == nil || collection.LoadConfig() == nil {
			continue
		}
		scored = append(scored, scoredShard{id: id, priority: reader.preparationPriority(id)})
	}
	if len(scored) == 0 {
		return
	}
	b.submitMu.Lock()
	b.ready.submit(scored, revision)
	b.submitMu.Unlock()
	wakeBalancer(b.work)
}

func (b *DefaultBalancer) run(ctx context.Context) {
	if b.cache == nil {
		return
	}
	unwatch := b.watchBalanceConfig(ctx, paramtable.Get())
	defer unwatch()
	if b.cache.WaitForReady(ctx) != nil {
		return
	}
	var workers sync.WaitGroup
	for source := range producerCount {
		workers.Go(func() { b.produce(ctx, source) })
	}
	workers.Go(func() { b.loop(ctx) })
	defer workers.Wait()
	interval := b.cache.GetBalanceConfig().ReconcileInterval
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-b.configChanged:
			if next := b.cache.GetBalanceConfig().ReconcileInterval; next != interval {
				interval = next
				ticker.Reset(interval)
			}
		case <-ticker.C:
			b.Trigger()
		}
	}
}

func (b *DefaultBalancer) produce(ctx context.Context, source discoverySource) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-b.discovery.queues[source].signal:
			b.dispatch(source)
		}
	}
}

func (b *DefaultBalancer) loop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-b.work:
			if err := b.Reconcile(ctx); err != nil && ctx.Err() == nil {
				mlog.Warn(ctx, "query view balance pass failed", mlog.Err(err))
			}
		}
	}
}

// Reconcile waits for cache readiness and runs one priority batch. Before
// Start, synchronous callers also drive discovery.
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
	// Synchronous callers drive the same producer without starting background loops.
	if !b.running.Load() {
		for remaining := discoveryShardLimit; remaining > 0; {
			n := b.dispatch(anySource)
			if n < 0 {
				break
			}
			remaining -= max(1, n) // Empty scopes also consume the discovery budget.
		}
	}
	b.submitMu.Lock()
	dirty := b.ready.take(int(b.cache.GetBalanceConfig().ShardBatchSize))
	more := len(b.ready.heap) > 0
	b.submitMu.Unlock()
	if more {
		wakeBalancer(b.work)
	}
	if len(dirty) == 0 {
		return nil
	}
	// A different producer may admit a shard before its node walk reaches it.
	// Repair this batch against live eligibility before pinning planning inputs.
	updating := b.discovery.refreshingNodes.Load() > 0
	if updating {
		for _, id := range dirty {
			b.cache.RefreshShardTarget(id)
		}
	}
	reader := newPlanningContext(b.cache)
	reader.deferOptimization = updating
	reader.boundedBatch = true
	dirty, err := b.readSchemaVersions(ctx, reader, dirty)
	plan := b.policy.Plan(reader, dirty)
	applyErr := b.apply(ctx, reader, plan, dirty)
	recoveryErr := b.recoverPublishedSchemas(ctx, reader, dirty)
	return errors.CombineErrors(err, errors.CombineErrors(applyErr, recoveryErr))
}

func (b *DefaultBalancer) apply(ctx context.Context, reader *planningContext, plan *BalancePlan, dirty []qviews.ShardID) error {
	if plan == nil {
		return nil
	}
	var result error
	// Planning is batched, but applying a view may wait for its DataView ref.
	// Do not hold a registry-wide flush window across that wait. Each manager
	// submits one ordered shard event; the scheduler still coalesces pending work.
	for _, shardID := range plan.Releases {
		mgr := b.viewRegistry.Get(shardID)
		planned := reader.collectionForShard(shardID)
		if mgr == nil || planned == nil || planned.DataView() == nil {
			continue
		}
		result = errors.CombineErrors(result, mgr.RequestReplicaRetirement(ctx, planned.DataView().SchemaVersion, func() bool {
			live := b.cache.GetCollection(planned.ID())
			return planned.SameLoad(live) && planned.DataView() == live.DataView() && live.Planning().Replica(shardID.ReplicaID) == nil && live.CanRetireReplicas(b.cache)
		}))
	}
	for _, shardID := range dirty {
		builder := plan.Prepares[shardID]
		if builder == nil {
			continue
		}
		collection, version := builder.LoadIdentity()
		mgr := b.viewRegistry.EnsureIf(shardID, func() bool {
			current := b.cache.GetCollection(collection)
			return current != nil && current.LoadConfig() != nil && current.ConfigVersion() == version && findReplica(current.LoadConfig(), shardID.ReplicaID) != nil
		})
		if mgr == nil {
			continue // The load was withdrawn or replaced after planning.
		}
		result = errors.CombineErrors(result, mgr.AddPreparing(ctx, builder))
	}
	if len(plan.RescanCollections) > 0 {
		b.discovery.layout(plan.RescanCollections, dirty)
	}
	if len(plan.Retries) > 0 {
		result = errors.CombineErrors(result, merr.WrapErrServiceUnavailableMsg("balance inputs are not ready for %d shards", len(plan.Retries)))
	}
	return result
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
		collection := reader.collectionForShard(shard)
		if collection == nil || collection.LoadConfig() == nil {
			ready = append(ready, shard)
			continue
		}
		id := collection.ID()
		if _, read := reader.schemaVersions[id]; !read && failed[id] == nil {
			version, err := b.schemaVersion(ctx, id)
			if err != nil {
				failed[id] = err
				result = errors.CombineErrors(result, err)
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
