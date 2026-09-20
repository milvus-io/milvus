package balancer

import (
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
)

// balanceInput is the object-local input consumed by allocation helpers.
// It exposes no mutable manager state or globally materialized snapshot.
type balanceInput interface {
	ConfigForShard(qviews.ShardID) *loadmgr.LoadConfig
	DataViewForShard(qviews.ShardID) *ShardDataView
	DataVersionForCollection(int64) (qviews.DataVersion, bool)
	GetShardStats(qviews.ShardID) *coordview.ShardStats
	ConfigVersion(int64) uint64
	GetBalanceConfig() *BalanceConfig
	NodesMap() map[int64]*BalanceNode
	CandidateNodes(string) []int64
	CurrentRows(qviews.ShardID) map[int64]int64
}

type planningContext struct {
	Reader
	collections map[int64]*CollectionEntry
	nodeEntries map[int64]*NodeEntry
	nodes       map[int64]*BalanceNode
	groups      map[string][]int64
	config      *BalanceConfig
}

func newPlanningContext(reader Reader) *planningContext {
	if current, ok := reader.(*planningContext); ok {
		return current
	}
	p := &planningContext{Reader: reader, collections: make(map[int64]*CollectionEntry), nodeEntries: make(map[int64]*NodeEntry), nodes: make(map[int64]*BalanceNode), groups: make(map[string][]int64), config: reader.GetBalanceConfig()}
	if p.config == nil {
		p.config = DefaultBalanceConfig()
	}
	// Capture every baseline before accepting any candidate. Later cache updates
	// cannot enter the same batch a second time through actual Preparing rows.
	reader.RangeNodeIDs(func(id int64) bool {
		if node := reader.GetNode(id); node != nil {
			p.nodeEntries[id] = node
			p.nodes[id] = node.Info()
		}
		return true
	})
	return p
}

func (p *planningContext) GetCollection(id int64) *CollectionEntry {
	if collection, ok := p.collections[id]; ok {
		return collection
	}
	collection := p.Reader.GetCollection(id)
	p.collections[id] = collection
	return collection
}
func (p *planningContext) GetNode(id int64) *NodeEntry      { return p.nodeEntries[id] }
func (p *planningContext) GetBalanceConfig() *BalanceConfig { return p.config }
func (p *planningContext) NodesMap() map[int64]*BalanceNode { return p.nodes }
func (p *planningContext) collectionForShard(id qviews.ShardID) *CollectionEntry {
	collectionID, ok := parseShardCollection(id)
	if !ok {
		collectionID, ok = p.CollectionForReplica(id.ReplicaID)
	}
	if !ok {
		return nil
	}
	return p.GetCollection(collectionID)
}

func (p *planningContext) ConfigForShard(id qviews.ShardID) *loadmgr.LoadConfig {
	if collection := p.collectionForShard(id); collection != nil && collection.config != nil && findReplica(collection.config, id.ReplicaID) != nil {
		return collection.config
	}
	return nil
}

func (p *planningContext) DataViewForShard(id qviews.ShardID) *ShardDataView {
	if collection := p.collectionForShard(id); collection != nil && collection.data != nil {
		return collection.data.Shard(id.VChannel)
	}
	return nil
}

func (p *planningContext) DataVersionForCollection(id int64) (qviews.DataVersion, bool) {
	if collection := p.GetCollection(id); collection != nil && collection.data != nil {
		return collection.data.DataVersion, true
	}
	return qviews.DataVersion{}, false
}

func (p *planningContext) GetShardStats(id qviews.ShardID) *coordview.ShardStats {
	if collection := p.collectionForShard(id); collection != nil {
		if shard := collection.GetShard(id); shard != nil {
			return shard.stats
		}
	}
	return nil
}

func (p *planningContext) ConfigVersion(id int64) uint64 {
	if collection := p.GetCollection(id); collection != nil {
		return collection.configVersion
	}
	return 0
}

func (p *planningContext) CandidateNodes(name string) []int64 {
	if nodes, ok := p.groups[name]; ok {
		return nodes
	}
	var nodes []int64
	if group := p.GetResourceGroup(name); group != nil {
		group.RangeNodes(func(id int64) bool {
			if node := p.nodes[id]; node != nil && node.ResourceGroup == name && node.Alive && !node.Stopping {
				nodes = append(nodes, id)
			}
			return true
		})
	}
	p.groups[name] = nodes
	return nodes
}

func (p *planningContext) CurrentRows(id qviews.ShardID) map[int64]int64 {
	rows := make(map[int64]int64)
	for nodeID, node := range p.nodeEntries {
		contribution := node.Contribution(id)
		if total := contribution.UpRowCount + contribution.PendingRowCount; total != 0 {
			rows[nodeID] = total
		}
	}
	return rows
}

// resolveCacheScope enumerates only keys and immutable references. Full passes
// use the same cached statistics as scoped passes and never rebuild accounting.
func resolveCacheScope(reader Reader, pending triggerBatch) []qviews.ShardID {
	targets := make(map[qviews.ShardID]struct{})
	addCollection := func(id int64) {
		collection := reader.GetCollection(id)
		if collection == nil {
			return
		}
		collection.RangeShards(func(shard *ShardEntry) bool { targets[shard.id] = struct{}{}; return true })
		if collection.config != nil && collection.data != nil {
			for _, replica := range collection.config.Replicas {
				for _, shard := range collection.data.Shards {
					targets[qviews.ShardID{ReplicaID: replica.ReplicaID, VChannel: shard.VChannel}] = struct{}{}
				}
			}
		}
	}
	if pending.full {
		reader.RangeCollectionIDs(func(id int64) bool { addCollection(id); return true })
	} else {
		for id := range pending.dirtyColls {
			addCollection(id)
		}
		for id := range pending.dirtyShards {
			targets[id] = struct{}{}
		}
		for id := range pending.dirtyNodes {
			if node := reader.GetNode(id); node != nil {
				node.RangeShards(func(id qviews.ShardID) bool { targets[id] = struct{}{}; return true })
			}
		}
	}
	shards := make([]qviews.ShardID, 0, len(targets))
	for id := range targets {
		shards = append(shards, id)
	}
	return shards
}
