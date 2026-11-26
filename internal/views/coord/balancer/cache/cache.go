package cache

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/milvus-io/milvus/internal/views/coord/balancer/api"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

// Reader returns immutable objects. Separate calls need not observe one instant.
// Published facts are read-only. Replica planning handles have their own lifetime and synchronization.
type Reader interface {
	GetCollection(int64) *CollectionEntry
	GetNode(int64) *NodeEntry
	GetResourceGroup(string) *ResourceGroupEntry
	GetBalanceConfig() *api.BalanceConfig
	CollectionForReplica(int64) (int64, bool)
	RangeCollectionIDs(func(int64) bool)
	RangeNodeIDs(func(int64) bool)
}

// CollectionEntry shares independently published desired and actual children.
type CollectionEntry struct {
	id            int64
	config        *loadmgr.LoadConfig
	configVersion uint64
	currentUps    int // Desired shards Up at the current load, schema and data versions.
	planning      *CollectionPlanning
	data          *api.CollectionDataView
	shards        immutableIndex[*ShardEntry]
	placements    immutableIndex[*PlacementNode]
	suspended     immutableIndex[bool]
	// Preserve Up history while a desired replica temporarily has no shard manager.
	releasedUpShards   immutableIndex[bool]
	targetLoads        immutableIndex[targetLoad]
	loadedPartitions   map[int64]struct{}
	loadsAllPartitions bool
	loadedRows         int64
	loadedShardRows    map[string]int64
}

func (c *CollectionEntry) ID() int64                         { return c.id }
func (c *CollectionEntry) LoadConfig() *loadmgr.LoadConfig   { return c.config }
func (c *CollectionEntry) ConfigVersion() uint64             { return c.configVersion }
func (c *CollectionEntry) DataView() *api.CollectionDataView { return c.data }
func (c *CollectionEntry) GetShard(id qviews.ShardID) *ShardEntry {
	shard, _ := c.shards.getShard(id)
	return shard
}

func (c *CollectionEntry) currentUp(id qviews.ShardID, stats *coordview.ShardStats) bool {
	return stats != nil && stats.UpVersion != nil && stats.UpLoadInfoVersion == c.configVersion &&
		c.planning.Replica(id.ReplicaID) != nil && c.data != nil && c.data.Shard(id.VChannel) != nil &&
		stats.UpSchemaVersion == c.data.SchemaVersion && stats.UpVersion.DataVersion.GTE(c.data.DataVersion)
}

func (c *CollectionEntry) allCurrentUp() bool {
	return c.currentUps > 0 && c.currentUps == len(c.config.Replicas)*len(c.data.Shards)
}

// CanRetireReplicas checks the complete desired placement against live topology.
// Placement footprints aggregate all shard Up references by node and replica.
func (c *CollectionEntry) CanRetireReplicas(reader Reader) bool {
	if !c.allCurrentUp() {
		return false
	}
	for node := range c.RangePlacementNodes {
		n := reader.GetNode(node.ID())
		for ref := range node.RangeReplicas {
			if r := c.planning.Replica(ref.ReplicaID); r != nil && ref.Up > 0 &&
				!n.eligibleFor(r.group) {
				return false
			}
		}
	}
	return true
}

// A placement-only handoff can unblock retirement while all versions stay current.
func (c *CollectionEntry) clearedRetirementBlocker(reader Reader, old, next *ShardEntry) bool {
	if old == nil || (next != nil && slices.Equal(old.upNodes, next.upNodes)) {
		return false
	}
	replica := c.planning.Replica(old.id.ReplicaID)
	if replica == nil {
		return false
	}
	key := idKey(old.id.ReplicaID)
	for _, node := range old.upNodes {
		if placement := c.PlacementNode(node); placement != nil {
			ref, _ := placement.replicas.get(key[:])
			if ref.Up > 0 {
				continue
			}
		}
		if !reader.GetNode(node).eligibleFor(replica.group) {
			return true
		}
	}
	return false
}

func (c *CollectionEntry) MaybeUped(id qviews.ShardID) bool {
	if shard := c.GetShard(id); shard != nil && shard.stats.MaybeUped {
		return true
	}
	_, ok := c.releasedUpShards.getShard(id)
	return ok
}
func (c *CollectionEntry) RangeShards(fn func(*ShardEntry) bool) { c.shards.each(fn) }

type ShardEntry struct {
	id                                     qviews.ShardID
	stats                                  *coordview.ShardStats
	rows                                   ShardSizeStats
	target                                 *coordview.ViewPlacement
	targetRows                             map[int64]int64
	upNodes, preparingNodes, residentNodes []int64
}

func (s *ShardEntry) ID() qviews.ShardID           { return s.id }
func (s *ShardEntry) Stats() *coordview.ShardStats { return s.stats }

type nodeContribution struct {
	shard qviews.ShardID
	rows  NodeSizeStats
}

// NodeEntry publishes totals and the exact contributions included in them.
type NodeEntry struct {
	info          api.BalanceNode
	contributions immutableIndex[nodeContribution]
	targets       immutableIndex[targetContribution]
}

func (n *NodeEntry) eligibleFor(group string) bool {
	return n != nil && n.info.Alive && !n.info.Stopping && n.info.ResourceGroup == group
}

func (n *NodeEntry) Info() *api.BalanceNode { return &n.info }
func (n *NodeEntry) Contribution(id qviews.ShardID) NodeSizeStats {
	value, _ := n.contributions.getShard(id)
	return value.rows
}

func (n *NodeEntry) RangeShards(fn func(qviews.ShardID) bool) {
	n.contributions.each(func(c nodeContribution) bool { return fn(c.shard) })
}

type ResourceGroupEntry struct {
	nodes       immutableIndex[int64]
	collections immutableIndex[int64]
	demand      immutableIndex[demandBucket]
}

func (r *ResourceGroupEntry) RangeNodes(fn func(int64) bool)       { r.nodes.each(fn) }
func (r *ResourceGroupEntry) RangeCollections(fn func(int64) bool) { r.collections.each(fn) }

type collectionSlot struct {
	mu       sync.Mutex
	value    atomic.Pointer[CollectionEntry]
	scanSlot *collectionScanSlot
	// Writer-owned historical fallback, retained only while latest/resident data needs it.
	rows        map[int64]int64
	uses        map[int64]int
	replicaUses map[int64]int
	unknown     map[qviews.ShardID]struct{}
}
type nodeSlot struct {
	mu    sync.Mutex
	value atomic.Pointer[NodeEntry]
}

// Cache is a resident read model. Directory locks cover keys/pointers only;
// segment work is serialized per collection, and contribution writes per node.
// Sources invoke publication synchronously and never call back from cache to source.
type Cache struct {
	mu               sync.RWMutex
	collections      map[int64]*collectionSlot
	scanIndex        collectionScanIndex
	nodes            map[int64]*nodeSlot
	replicas         map[int64]int64
	groups           map[string]*ResourceGroupEntry
	notify           func(api.TriggerScope)
	asyncNodeTargets bool
	config           atomic.Pointer[api.BalanceConfig]
	readyCh          chan struct{}
	readyOnce        sync.Once
	subscriptions    []func()
}

func New(config *api.BalanceConfig) *Cache {
	c := &Cache{collections: make(map[int64]*collectionSlot), nodes: make(map[int64]*nodeSlot), replicas: make(map[int64]int64), groups: make(map[string]*ResourceGroupEntry), readyCh: make(chan struct{})}
	c.config.Store(api.DefaultBalanceConfig())
	c.UpdateBalanceConfig(config)
	return c
}

// NodeListener publishes committed node identity/RG facts; nil means removal.
// Implementations serialize initial replay with all subsequent publications.
type (
	NodeListener  func(int64, *api.NodeInfo)
	NodePublisher interface{ RegisterNodeListener(NodeListener) func() }
)

// NewFromSources attaches replaying hooks before declaring the cache ready.
// It is component assembly only; production owners must supply the node/RG publisher.
func NewFromSources(config *api.BalanceConfig, configs *loadmgr.LoadConfigStore, data api.DataViewPublisher, registry *coordview.ShardViewRegistry, nodes NodePublisher) *Cache {
	c := New(config)
	c.subscriptions = append(c.subscriptions, configs.RegisterLoadConfigListener(c.PublishLoadConfig))
	c.subscriptions = append(c.subscriptions, data.RegisterDataViewListener(c.PublishDataView))
	c.subscriptions = append(c.subscriptions, registry.RegisterPublicationListener(c.PublishShard))
	c.subscriptions = append(c.subscriptions, nodes.RegisterNodeListener(c.PublishNode))
	c.MarkReady()
	return c
}

// Close detaches source hooks and joins directory maintenance. Call after
// stopping the controller.
func (c *Cache) Close() {
	c.mu.Lock()
	c.scanIndex.closed = true
	c.mu.Unlock()
	for _, unsubscribe := range c.subscriptions {
		unsubscribe()
	}
	c.scanIndex.workers.Wait()
}

// MarkReady opens the one-way initialization barrier after all sources have
// replayed their recovered state. Repeated calls are safe.
func (c *Cache) MarkReady() {
	c.readyOnce.Do(func() { close(c.readyCh) })
	c.changed(api.TriggerScope{NodeChanged: true})
}

func (c *Cache) Ready() bool {
	select {
	case <-c.readyCh:
		return true
	default:
		return false
	}
}

// WaitForReady blocks until source initialization completes or ctx is canceled.
// Closing the barrier wakes every waiter, including callers arriving later.
func (c *Cache) WaitForReady(ctx context.Context) error {
	select {
	case <-c.readyCh:
		return ctx.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}

// SetNotifier registers an observer. Target accounting completes on publication.
func (c *Cache) SetNotifier(fn func(api.TriggerScope)) { c.setNotifier(fn, false) }

// SetScheduler installs the controller, which refreshes node targets during discovery.
func (c *Cache) SetScheduler(fn func(api.TriggerScope)) { c.setNotifier(fn, true) }

func (c *Cache) setNotifier(fn func(api.TriggerScope), asyncNodeTargets bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.notify, c.asyncNodeTargets = fn, asyncNodeTargets && fn != nil
}

func (c *Cache) changed(scope api.TriggerScope) {
	c.mu.RLock()
	fn := c.notify
	c.mu.RUnlock()
	if fn != nil {
		fn(scope)
	}
}
func (c *Cache) GetBalanceConfig() *api.BalanceConfig { return c.config.Load() }

// UpdateBalanceConfig publishes a valid changed config. It returns false for
// invalid or identical values. Interval-only changes do not enqueue a full pass;
// the config source separately wakes the controller to reset its timer.
func (c *Cache) UpdateBalanceConfig(config *api.BalanceConfig) bool {
	if config == nil {
		config = api.DefaultBalanceConfig()
	}
	copy := *config
	if !copy.Valid() {
		return false
	}
	for {
		previous := c.config.Load()
		if *previous == copy {
			return false
		}
		if c.config.CompareAndSwap(previous, &copy) {
			policy := copy
			policy.ReconcileInterval = previous.ReconcileInterval
			policy.ShardBatchSize = previous.ShardBatchSize
			if policy != *previous {
				c.changed(api.TriggerScope{NodeChanged: true})
			}
			return true
		}
	}
}

func (c *Cache) collection(id int64) *collectionSlot {
	c.mu.RLock()
	slot := c.collections[id]
	c.mu.RUnlock()
	if slot != nil {
		return slot
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if slot = c.collections[id]; slot == nil {
		slot = &collectionSlot{rows: make(map[int64]int64), uses: make(map[int64]int), replicaUses: make(map[int64]int), unknown: make(map[qviews.ShardID]struct{})}
		slot.value.Store(&CollectionEntry{id: id})
		c.collections[id] = slot
		c.registerCollectionLocked(slot)
	}
	return slot
}

func (c *Cache) node(id int64) *nodeSlot {
	c.mu.RLock()
	slot := c.nodes[id]
	c.mu.RUnlock()
	if slot != nil {
		return slot
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if slot = c.nodes[id]; slot == nil {
		slot = &nodeSlot{}
		slot.value.Store(&NodeEntry{info: api.BalanceNode{NodeID: id}})
		c.nodes[id] = slot
	}
	return slot
}

func (c *Cache) GetCollection(id int64) *CollectionEntry {
	c.mu.RLock()
	slot := c.collections[id]
	c.mu.RUnlock()
	if slot == nil {
		return nil
	}
	return slot.value.Load()
}

func (c *Cache) GetNode(id int64) *NodeEntry {
	c.mu.RLock()
	slot := c.nodes[id]
	c.mu.RUnlock()
	if slot == nil {
		return nil
	}
	return slot.value.Load()
}

func (c *Cache) GetResourceGroup(name string) *ResourceGroupEntry {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.groups[name]
}

func (c *Cache) CollectionForReplica(id int64) (int64, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	collection, ok := c.replicas[id]
	return collection, ok
}

func (c *Cache) RangeCollectionIDs(fn func(int64) bool) {
	c.mu.RLock()
	ids := make([]int64, 0, len(c.collections))
	for id := range c.collections {
		ids = append(ids, id)
	}
	c.mu.RUnlock()
	for _, id := range ids {
		if !fn(id) {
			return
		}
	}
}

func (c *Cache) RangeNodeIDs(fn func(int64) bool) {
	c.mu.RLock()
	ids := make([]int64, 0, len(c.nodes))
	for id := range c.nodes {
		ids = append(ids, id)
	}
	c.mu.RUnlock()
	for _, id := range ids {
		if !fn(id) {
			return
		}
	}
}

func (c *Cache) groupLocked(name string) ResourceGroupEntry {
	if group := c.groups[name]; group != nil {
		return *group
	}
	return ResourceGroupEntry{}
}

func (c *Cache) storeGroupLocked(name string, group ResourceGroupEntry) {
	if group.nodes.len() == 0 && group.collections.len() == 0 && group.demand.len() == 0 {
		delete(c.groups, name)
	} else {
		c.groups[name] = &group
	}
}

// PublishLoadConfig takes a source-owned immutable config. It changes no DataView
// or actual placement fields. Callers must not mutate cfg after publication.
func (c *Cache) PublishLoadConfig(id int64, cfg *loadmgr.LoadConfig, version uint64) {
	slot := c.lockCollection(id)
	defer c.finishCollection(id, slot)
	next := *slot.value.Load()
	previous := next
	// AutoLoad-only publications retain the requirements version but replace
	// cfg. Compare both so subsequent planning sees the latest scheduling input.
	if next.config == cfg && next.configVersion == version {
		return
	}
	key := idKey(id)
	c.mu.Lock()
	if next.config != nil {
		for _, replica := range next.config.Replicas {
			group := c.groupLocked(replica.ResourceGroup)
			group.collections = group.collections.remove(key[:])
			c.storeGroupLocked(replica.ResourceGroup, group)
			// Keep a replica-to-collection binding while residual shards still use it.
			if slot.replicaUses[replica.ReplicaID] == 0 {
				delete(c.replicas, replica.ReplicaID)
			}
		}
	}
	if cfg != nil {
		for _, replica := range cfg.Replicas {
			c.replicas[replica.ReplicaID] = id
			group := c.groupLocked(replica.ResourceGroup)
			group.collections = group.collections.set(key[:], id)
			c.storeGroupLocked(replica.ResourceGroup, group)
		}
	}
	c.mu.Unlock()
	membershipChanged := previous.config != nil && (cfg == nil || !slices.EqualFunc(previous.config.Replicas, cfg.Replicas, func(a, b *loadmgr.ReplicaAssignment) bool {
		return a.ReplicaID == b.ReplicaID && a.ResourceGroup == b.ResourceGroup
	}))
	if cfg == nil {
		next.planning = nil
		next.releasedUpShards = immutableIndex[bool]{}
		next.suspended = immutableIndex[bool]{}
	} else if previous.config == nil || membershipChanged {
		next.planning = previous.planning.withMembership(cfg)
	}
	if cfg != nil && membershipChanged && (next.suspended.len() > 0 || next.releasedUpShards.len() > 0) {
		for _, replica := range previous.config.Replicas {
			if !hasReplica(cfg, replica.ReplicaID) {
				prefix := idKey(replica.ReplicaID)
				next.suspended = next.suspended.remove(prefix[:])
				if next.releasedUpShards.tree != nil {
					next.releasedUpShards.tree, _ = next.releasedUpShards.tree.DeletePrefix(prefix[:])
				}
			}
		}
	}
	next.config, next.configVersion = cfg, version
	next.refreshLoadRows()
	c.replaceDemand(&previous, &next)
	c.refreshTargets(&next, slot.rows)
	slot.value.Store(&next)
	c.changed(api.TriggerScope{DirtyCollections: []int64{id}})
}

// PublishDataView receives a finalized immutable collection, not a request to
// fetch it later. Row summaries were built by the source at publication.
func (c *Cache) PublishDataView(id int64, data *api.CollectionDataView) {
	slot := c.lockCollection(id)
	defer c.finishCollection(id, slot)
	next := *slot.value.Load()
	if next.data == data {
		return
	}
	previous := next.data
	previousEntry := next
	next.data = data
	if data != nil {
		for _, shard := range data.Shards {
			for _, part := range shard.Partitions {
				for _, seg := range part.Segments {
					slot.rows[seg.SegmentID] = seg.LogicalSize
				}
			}
		}
	}
	if previous != nil {
		for _, shard := range previous.Shards {
			for _, part := range shard.Partitions {
				for _, seg := range part.Segments {
					slot.pruneRow(&next, seg.SegmentID)
				}
			}
		}
	}
	// Known exact-version stats do not depend on the planning fallback.
	for shardID := range slot.unknown {
		old := next.GetShard(shardID)
		rows := shardRows(old.stats, slot.rows)
		c.replaceContributions(shardID, old.rows, rows)
		updated := *old
		updated.rows = rows
		next.shards = next.shards.set(shardKey(shardID), &updated)
	}
	next.refreshLoadRows()
	c.replaceDemand(&previousEntry, &next)
	c.refreshTargets(&next, slot.rows)
	slot.value.Store(&next)
	c.changed(api.TriggerScope{DirtyCollections: []int64{id}})
}

func (s *collectionSlot) pruneRow(next *CollectionEntry, id int64) {
	if s.uses[id] > 0 {
		return
	}
	if next.data != nil {
		if _, ok := next.data.Segment(id); ok {
			return
		}
	}
	delete(s.rows, id)
}

// PublishShard is called in source order, including seed and final removal.
func (c *Cache) PublishShard(id qviews.ShardID, stats *coordview.ShardStats) {
	collectionID, ok := c.collectionIDForShard(id)
	if !ok {
		return
	}
	slot := c.lockCollection(collectionID)
	defer c.finishCollection(collectionID, slot)
	next := *slot.value.Load()
	old := next.GetShard(id)
	if old != nil && old.stats == stats {
		return
	}
	wasReady := next.allCurrentUp()
	if next.currentUp(id, stats) {
		next.currentUps++
	}
	var previous ShardSizeStats
	if old != nil {
		if next.currentUp(id, old.stats) {
			next.currentUps--
		}
		previous = old.rows
		for segmentID := range old.stats.Segments {
			slot.uses[segmentID]--
		}
	}
	delete(slot.unknown, id)
	if stats != nil {
		for segmentID, segment := range stats.Segments {
			slot.uses[segmentID]++
			if segment.LostStats {
				slot.unknown[id] = struct{}{}
			}
		}
	}
	rows := shardRows(stats, slot.rows)
	c.replaceContributions(id, previous, rows)
	var replacement *ShardEntry
	if stats != nil {
		replacement = &ShardEntry{id: id, stats: stats, rows: rows, upNodes: stats.UpNodes, preparingNodes: stats.PreparingNodes, residentNodes: stats.ResidentNodes}
	}
	next.replacePlacements(id, old, replacement)
	c.replaceTarget(&next, id, old, replacement, slot.rows)
	if stats == nil {
		if old != nil {
			if old.stats.MaybeUped && next.config != nil && hasReplica(next.config, id.ReplicaID) {
				next.releasedUpShards = next.releasedUpShards.set(shardKey(id), true)
			}
			slot.replicaUses[id.ReplicaID]--
			if slot.replicaUses[id.ReplicaID] == 0 {
				delete(slot.replicaUses, id.ReplicaID)
			}
		}
		next.shards = next.shards.remove(shardKey(id))
	} else {
		if old == nil {
			slot.replicaUses[id.ReplicaID]++
		}
		next.shards = next.shards.set(shardKey(id), replacement)
	}
	if old != nil {
		for segmentID := range old.stats.Segments {
			slot.pruneRow(&next, segmentID)
			if slot.uses[segmentID] == 0 {
				delete(slot.uses, segmentID)
			}
		}
	}
	slot.value.Store(&next)
	retained := slot.replicaUses[id.ReplicaID] > 0 || (next.config != nil && hasReplica(next.config, id.ReplicaID))
	c.mu.Lock()
	if retained {
		c.replicas[id.ReplicaID] = collectionID
	} else if c.replicas[id.ReplicaID] == collectionID {
		delete(c.replicas, id.ReplicaID)
	}
	c.mu.Unlock()
	// Once all desired shards are current, each desired replica has a resident
	// shard. Extra replica IDs therefore identify collections awaiting retirement.
	if next.allCurrentUp() && len(slot.replicaUses) > len(next.config.Replicas) &&
		(!wasReady || next.clearedRetirementBlocker(c, old, replacement)) && next.CanRetireReplicas(c) {
		for resident := range slot.replicaUses {
			if next.planning.Replica(resident) == nil {
				c.changed(api.TriggerScope{RetiringReplicas: []int64{resident}})
			}
		}
	}
	if old != nil && stats != nil {
		// A schema publication can unblock retirement even for an empty manager.
		if next.config != nil && next.planning.Replica(id.ReplicaID) == nil &&
			!sameVersion(old.stats.PublishedSchema, stats.PublishedSchema) {
			c.changed(api.TriggerScope{DirtyShards: []qviews.ShardID{id}})
			return
		}
		if sameVersion(old.stats.UpVersion, stats.UpVersion) && sameVersion(old.stats.PreparingVersion, stats.PreparingVersion) {
			return
		}
		// An undesired shard's teardown is already driven by its manager and
		// syncer. Publish the accounting without scheduling another release.
		// Initial discovery, final removal and late Up/Preparing still notify.
		if stats.UpVersion == nil && stats.PreparingVersion == nil && (next.config == nil || !hasReplica(next.config, id.ReplicaID)) {
			return
		}
	}
	c.changed(api.TriggerScope{DirtyShards: []qviews.ShardID{id}})
}

func sameVersion[T comparable](a, b *T) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func shardRows(stats *coordview.ShardStats, fallback map[int64]int64) ShardSizeStats {
	rows := make(ShardSizeStats)
	if stats == nil {
		return rows
	}
	// A Preparing on an empty node can still select or invalidate a non-empty Up fallback.
	for _, node := range stats.UpNodes {
		rows[node] = NodeSizeStats{}
	}
	for _, node := range stats.PreparingNodes {
		rows[node] = NodeSizeStats{}
	}
	for _, segment := range stats.Segments {
		count := segment.LogicalSize
		if segment.LostStats {
			count = fallback[segment.SegmentID]
		}
		for nodeID, state := range segment.Nodes {
			entry := rows[nodeID]
			switch state {
			case coordview.SegmentStateUp:
				entry.UpLogicalSize += count
			case coordview.SegmentStatePreparing, coordview.SegmentStateReady:
				entry.PendingLogicalSize += count
			}
			rows[nodeID] = entry // Preserve zero-row and unrecoverable placements for node-loss scope.
		}
	}
	return rows
}

func (c *Cache) replaceContributions(id qviews.ShardID, old, next ShardSizeStats) {
	for nodeID := range old {
		if _, ok := next[nodeID]; !ok {
			c.replaceNodeContribution(nodeID, id, NodeSizeStats{}, false)
		}
	}
	for nodeID, rows := range next {
		if previous, ok := old[nodeID]; !ok || previous != rows {
			c.replaceNodeContribution(nodeID, id, rows, true)
		}
	}
}

func (c *Cache) replaceNodeContribution(nodeID int64, id qviews.ShardID, rows NodeSizeStats, present bool) {
	slot := c.lockNode(nodeID)
	defer c.finishNode(nodeID, slot)
	next := *slot.value.Load()
	previous := next.Contribution(id)
	next.info.UpLogicalSize += rows.UpLogicalSize - previous.UpLogicalSize
	next.info.PendingLogicalSize += rows.PendingLogicalSize - previous.PendingLogicalSize
	if present {
		next.contributions = next.contributions.set(shardKey(id), nodeContribution{shard: id, rows: rows})
	} else {
		next.contributions = next.contributions.remove(shardKey(id))
	}
	slot.value.Store(&next)
}

func (c *Cache) PublishNode(id int64, info *api.NodeInfo) {
	slot := c.lockNode(id)
	next := *slot.value.Load()
	old := next.info
	if info == nil {
		next.info.Alive = false
		next.info.Stopping = false
	} else {
		next.info.Alive, next.info.Stopping, next.info.ResourceGroup = info.Alive, info.Stopping, info.ResourceGroup
	}
	if old == next.info {
		c.finishNode(id, slot)
		return
	}
	key := idKey(id)
	c.mu.Lock()
	oldGroup := c.groupLocked(old.ResourceGroup)
	oldGroup.nodes = oldGroup.nodes.remove(key[:])
	c.storeGroupLocked(old.ResourceGroup, oldGroup)
	group := c.groupLocked(next.info.ResourceGroup)
	if next.info.Alive && !next.info.Stopping {
		group.nodes = group.nodes.set(key[:], id)
	}
	c.storeGroupLocked(next.info.ResourceGroup, group)
	c.mu.Unlock()
	slot.value.Store(&next)
	c.finishNode(id, slot)
	scope := api.TriggerScope{DirtyNodes: []int64{id}, DirtyResourceGroups: []string{old.ResourceGroup}}
	if next.info.ResourceGroup != old.ResourceGroup {
		scope.DirtyResourceGroups = append(scope.DirtyResourceGroups, next.info.ResourceGroup)
	}
	c.mu.RLock()
	notify, async := c.notify, c.asyncNodeTargets
	c.mu.RUnlock()
	if !async {
		// Standalone readers and initial replay have no discovery producer.
		for shard := range next.RangeShards {
			c.RefreshShardTarget(shard)
		}
	}
	if notify != nil {
		notify(scope)
	}
}

// RefreshShardTarget recomputes from current inputs under the collection writer lock.
func (c *Cache) RefreshShardTarget(shard qviews.ShardID) {
	collectionID, ok := c.collectionIDForShard(shard)
	if !ok {
		return
	}
	// Cold and already removed shards have no published target to repair.
	if entry := c.GetCollection(collectionID); entry == nil || entry.GetShard(shard) == nil {
		return
	}
	s := c.lockCollection(collectionID)
	defer c.finishCollection(collectionID, s)
	entry := *s.value.Load()
	if current := entry.GetShard(shard); current != nil && c.refreshShardTarget(&entry, current, s.rows, make(map[int64]int64)) {
		s.value.Store(&entry)
	}
}

// A publication that waited for a retired slot retries on the replacement;
// this allows reclaiming directories without publishing into detached objects.
func (c *Cache) lockCollection(id int64) *collectionSlot {
	for {
		slot := c.collection(id)
		slot.mu.Lock()
		c.mu.RLock()
		current := c.collections[id] == slot
		c.mu.RUnlock()
		if current {
			return slot
		}
		slot.mu.Unlock()
	}
}

func (c *Cache) finishCollection(id int64, slot *collectionSlot) {
	entry := slot.value.Load()
	if entry.config == nil && entry.data == nil && entry.shards.len() == 0 {
		c.mu.Lock()
		delete(c.collections, id)
		c.retireCollectionLocked(slot)
		c.mu.Unlock()
	}
	slot.mu.Unlock()
}

func (c *Cache) lockNode(id int64) *nodeSlot {
	for {
		slot := c.node(id)
		slot.mu.Lock()
		c.mu.RLock()
		current := c.nodes[id] == slot
		c.mu.RUnlock()
		if current {
			return slot
		}
		slot.mu.Unlock()
	}
}

func (c *Cache) finishNode(id int64, slot *nodeSlot) {
	entry := slot.value.Load()
	if !entry.info.Alive && entry.contributions.len() == 0 && entry.targets.len() == 0 {
		c.mu.Lock()
		delete(c.nodes, id)
		c.mu.Unlock()
	}
	slot.mu.Unlock()
}

func hasReplica(config *loadmgr.LoadConfig, id int64) bool {
	for _, replica := range config.Replicas {
		if replica.ReplicaID == id {
			return true
		}
	}
	return false
}

func (c *Cache) collectionIDForShard(shardID qviews.ShardID) (int64, bool) {
	_, id, _, err := funcutil.ParseVChannel(shardID.VChannel)
	if err != nil {
		return c.CollectionForReplica(shardID.ReplicaID)
	}
	return id, true
}
