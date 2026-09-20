package balancer

import (
	"sync"
	"sync/atomic"

	"github.com/milvus-io/milvus/internal/views/coord/balancer/api"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
)

// Reader returns immutable objects. Separate calls need not observe one instant.
// Returned objects and all their descendants are read-only.
type Reader interface {
	GetCollection(int64) *CollectionEntry
	GetNode(int64) *NodeEntry
	GetResourceGroup(string) *ResourceGroupEntry
	GetBalanceConfig() *BalanceConfig
	CollectionForReplica(int64) (int64, bool)
	RangeCollectionIDs(func(int64) bool)
	RangeNodeIDs(func(int64) bool)
}

// CollectionEntry shares independently published desired and actual children.
type CollectionEntry struct {
	id            int64
	config        *loadmgr.LoadConfig
	configVersion uint64
	data          *CollectionDataView
	shards        immutableIndex[*ShardEntry]
}

func (c *CollectionEntry) ID() int64                       { return c.id }
func (c *CollectionEntry) LoadConfig() *loadmgr.LoadConfig { return c.config }
func (c *CollectionEntry) ConfigVersion() uint64           { return c.configVersion }
func (c *CollectionEntry) DataView() *CollectionDataView   { return c.data }
func (c *CollectionEntry) GetShard(id qviews.ShardID) *ShardEntry {
	shard, _ := c.shards.get(shardKey(id))
	return shard
}
func (c *CollectionEntry) RangeShards(fn func(*ShardEntry) bool) { c.shards.each(fn) }

type ShardEntry struct {
	id    qviews.ShardID
	stats *coordview.ShardStats
	rows  ShardRowStats
}

func (s *ShardEntry) ID() qviews.ShardID           { return s.id }
func (s *ShardEntry) Stats() *coordview.ShardStats { return s.stats }

type nodeContribution struct {
	shard qviews.ShardID
	rows  NodeRowStats
}

// NodeEntry publishes totals and the exact contributions included in them.
type NodeEntry struct {
	info          BalanceNode
	contributions immutableIndex[nodeContribution]
}

func (n *NodeEntry) Info() *BalanceNode { return &n.info }
func (n *NodeEntry) Contribution(id qviews.ShardID) NodeRowStats {
	value, _ := n.contributions.get(shardKey(id))
	return value.rows
}

func (n *NodeEntry) RangeShards(fn func(qviews.ShardID) bool) {
	n.contributions.each(func(c nodeContribution) bool { return fn(c.shard) })
}

type ResourceGroupEntry struct {
	nodes       immutableIndex[int64]
	collections immutableIndex[int64]
}

func (r *ResourceGroupEntry) RangeNodes(fn func(int64) bool)       { r.nodes.each(fn) }
func (r *ResourceGroupEntry) RangeCollections(fn func(int64) bool) { r.collections.each(fn) }

type collectionSlot struct {
	mu    sync.Mutex
	value atomic.Pointer[CollectionEntry]
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
	mu            sync.RWMutex
	collections   map[int64]*collectionSlot
	nodes         map[int64]*nodeSlot
	replicas      map[int64]int64
	groups        map[string]*ResourceGroupEntry
	notify        func(TriggerScope)
	config        atomic.Pointer[BalanceConfig]
	ready         atomic.Bool
	subscriptions []func()
}

func NewCache(config *BalanceConfig) *Cache {
	c := &Cache{collections: make(map[int64]*collectionSlot), nodes: make(map[int64]*nodeSlot), replicas: make(map[int64]int64), groups: make(map[string]*ResourceGroupEntry)}
	c.UpdateBalanceConfig(config)
	return c
}

// NodeListener publishes committed node identity/RG facts; nil means removal.
// Implementations serialize initial replay with all subsequent publications.
type (
	NodeListener  func(int64, *NodeInfo)
	NodePublisher interface{ RegisterNodeListener(NodeListener) func() }
)

// NewCacheFromSources attaches replaying hooks before declaring the cache ready.
// It is component assembly only; production owners must supply the node/RG publisher.
func NewCacheFromSources(config *BalanceConfig, configs *loadmgr.LoadConfigStore, data api.DataViewPublisher, registry *coordview.ShardViewRegistry, nodes NodePublisher) *Cache {
	c := NewCache(config)
	c.subscriptions = append(c.subscriptions, configs.RegisterLoadConfigListener(c.PublishLoadConfig))
	c.subscriptions = append(c.subscriptions, data.RegisterDataViewListener(c.PublishDataView))
	c.subscriptions = append(c.subscriptions, registry.RegisterPublicationListener(c.PublishShard))
	c.subscriptions = append(c.subscriptions, nodes.RegisterNodeListener(c.PublishNode))
	c.MarkReady()
	return c
}

// Close detaches source hooks. Call after stopping the controller.
func (c *Cache) Close() {
	for _, unsubscribe := range c.subscriptions {
		unsubscribe()
	}
}
func (c *Cache) MarkReady()                        { c.ready.Store(true); c.changed(TriggerScope{NodeChanged: true}) }
func (c *Cache) Ready() bool                       { return c.ready.Load() }
func (c *Cache) SetNotifier(fn func(TriggerScope)) { c.mu.Lock(); c.notify = fn; c.mu.Unlock() }
func (c *Cache) changed(scope TriggerScope) {
	c.mu.RLock()
	fn := c.notify
	c.mu.RUnlock()
	if fn != nil {
		fn(scope)
	}
}
func (c *Cache) GetBalanceConfig() *BalanceConfig { return c.config.Load() }
func (c *Cache) UpdateBalanceConfig(config *BalanceConfig) {
	if config == nil {
		config = DefaultBalanceConfig()
	}
	copy := *config
	c.config.Store(&copy)
	c.changed(TriggerScope{NodeChanged: true})
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
		slot.value.Store(&NodeEntry{info: BalanceNode{NodeID: id}})
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
	if group.nodes.len() == 0 && group.collections.len() == 0 {
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
	if next.config == cfg && next.configVersion == version {
		return
	}
	c.mu.Lock()
	if next.config != nil {
		for _, replica := range next.config.Replicas {
			group := c.groupLocked(replica.ResourceGroup)
			group.collections = group.collections.remove(idKey(id))
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
			group.collections = group.collections.set(idKey(id), id)
			c.storeGroupLocked(replica.ResourceGroup, group)
		}
	}
	c.mu.Unlock()
	next.config, next.configVersion = cfg, version
	slot.value.Store(&next)
	c.changed(TriggerScope{DirtyCollections: []int64{id}})
}

// PublishDataView receives a finalized immutable collection, not a request to
// fetch it later. Row summaries were built by the source at publication.
func (c *Cache) PublishDataView(id int64, data *CollectionDataView) {
	slot := c.lockCollection(id)
	defer c.finishCollection(id, slot)
	next := *slot.value.Load()
	if next.data == data {
		return
	}
	previous := next.data
	next.data = data
	if data != nil {
		for _, shard := range data.Shards {
			for _, part := range shard.Partitions {
				for _, seg := range part.Segments {
					slot.rows[seg.SegmentID] = seg.RowNum
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
		next.shards = next.shards.set(shardKey(shardID), &ShardEntry{id: shardID, stats: old.stats, rows: rows})
	}
	slot.value.Store(&next)
	c.changed(TriggerScope{DirtyCollections: []int64{id}})
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
	collectionID, ok := parseShardCollection(id)
	if !ok {
		collectionID, ok = c.CollectionForReplica(id.ReplicaID)
	}
	if !ok {
		return
	} // Invalid legacy identity cannot be associated with a collection.
	slot := c.lockCollection(collectionID)
	defer c.finishCollection(collectionID, slot)
	next := *slot.value.Load()
	old := next.GetShard(id)
	if old != nil && old.stats == stats {
		return
	}
	var previous ShardRowStats
	if old != nil {
		previous = old.rows
		for segmentID := range old.stats.Segments {
			slot.uses[segmentID]--
		}
	}
	delete(slot.unknown, id)
	if stats != nil {
		for segmentID, segment := range stats.Segments {
			slot.uses[segmentID]++
			if !segment.HasRowNum {
				slot.unknown[id] = struct{}{}
			}
		}
	}
	rows := shardRows(stats, slot.rows)
	c.replaceContributions(id, previous, rows)
	if stats == nil {
		if old != nil {
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
		next.shards = next.shards.set(shardKey(id), &ShardEntry{id: id, stats: stats, rows: rows})
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
	retained := slot.replicaUses[id.ReplicaID] > 0 || (next.config != nil && findReplica(next.config, id.ReplicaID) != nil)
	c.mu.Lock()
	if retained {
		c.replicas[id.ReplicaID] = collectionID
	} else if c.replicas[id.ReplicaID] == collectionID {
		delete(c.replicas, id.ReplicaID)
	}
	c.mu.Unlock()
	if old == nil || stats == nil || !sameVersion(old.stats.UpVersion, stats.UpVersion) || !sameVersion(old.stats.PreparingVersion, stats.PreparingVersion) {
		c.changed(TriggerScope{DirtyShards: []qviews.ShardID{id}})
	}
}

func sameVersion(a, b *qviews.QueryViewVersion) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func shardRows(stats *coordview.ShardStats, fallback map[int64]int64) ShardRowStats {
	rows := make(ShardRowStats)
	if stats == nil {
		return rows
	}
	for _, segment := range stats.Segments {
		count := segment.RowNum
		if !segment.HasRowNum {
			count = fallback[segment.SegmentID]
		}
		for nodeID, state := range segment.Nodes {
			entry := rows[nodeID]
			switch state {
			case coordview.SegmentStateUp:
				entry.UpRowCount += count
			case coordview.SegmentStatePreparing, coordview.SegmentStateReady:
				entry.PendingRowCount += count
			}
			rows[nodeID] = entry // Preserve zero-row and unrecoverable placements for node-loss scope.
		}
	}
	return rows
}

func (c *Cache) replaceContributions(id qviews.ShardID, old, next ShardRowStats) {
	for nodeID := range old {
		if _, ok := next[nodeID]; !ok {
			c.replaceNodeContribution(nodeID, id, NodeRowStats{}, false)
		}
	}
	for nodeID, rows := range next {
		if previous, ok := old[nodeID]; !ok || previous != rows {
			c.replaceNodeContribution(nodeID, id, rows, true)
		}
	}
}

func (c *Cache) replaceNodeContribution(nodeID int64, id qviews.ShardID, rows NodeRowStats, present bool) {
	slot := c.lockNode(nodeID)
	defer c.finishNode(nodeID, slot)
	next := *slot.value.Load()
	previous := next.Contribution(id)
	next.info.UpRowCount += rows.UpRowCount - previous.UpRowCount
	next.info.PendingRowCount += rows.PendingRowCount - previous.PendingRowCount
	if present {
		next.contributions = next.contributions.set(shardKey(id), nodeContribution{shard: id, rows: rows})
	} else {
		next.contributions = next.contributions.remove(shardKey(id))
	}
	slot.value.Store(&next)
}

func (c *Cache) PublishNode(id int64, info *NodeInfo) {
	slot := c.lockNode(id)
	defer c.finishNode(id, slot)
	next := *slot.value.Load()
	old := next.info
	if info == nil {
		next.info.Alive = false
		next.info.Stopping = false
	} else {
		next.info.Alive, next.info.Stopping, next.info.ResourceGroup = info.Alive, info.Stopping, info.ResourceGroup
	}
	if old == next.info {
		return
	}
	c.mu.Lock()
	oldGroup := c.groupLocked(old.ResourceGroup)
	oldGroup.nodes = oldGroup.nodes.remove(idKey(id))
	c.storeGroupLocked(old.ResourceGroup, oldGroup)
	group := c.groupLocked(next.info.ResourceGroup)
	if next.info.Alive && !next.info.Stopping {
		group.nodes = group.nodes.set(idKey(id), id)
	}
	c.storeGroupLocked(next.info.ResourceGroup, group)
	c.mu.Unlock()
	slot.value.Store(&next)
	scope := TriggerScope{DirtyNodes: []int64{id}}
	if old.ResourceGroup != next.info.ResourceGroup || (next.info.Alive && !next.info.Stopping) {
		seen := make(map[int64]struct{})
		for _, name := range []string{old.ResourceGroup, next.info.ResourceGroup} {
			if rg := c.GetResourceGroup(name); rg != nil {
				rg.RangeCollections(func(id int64) bool { seen[id] = struct{}{}; return true })
			}
		}
		for id := range seen {
			scope.DirtyCollections = append(scope.DirtyCollections, id)
		}
	}
	c.changed(scope)
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
	if !entry.info.Alive && entry.contributions.len() == 0 {
		c.mu.Lock()
		delete(c.nodes, id)
		c.mu.Unlock()
	}
	slot.mu.Unlock()
}
