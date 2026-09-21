package loadmgr

import "github.com/milvus-io/milvus/internal/views/qviews"

// CollectionDiscoveryUpdate replaces a collection's discoverable shards with
// the healthy serving coverage selected by one serialized Balancer. Revision
// is monotonic for the lifetime of that controller and this load manager.
type CollectionDiscoveryUpdate struct {
	CollectionID  int64
	ConfigVersion uint64
	Revision      uint64
	Shards        []qviews.ShardID
}

// UpdateCollectionDiscovery supports suspension, restoration and replica
// removal. Once managed, legacy ObserveShardUp callbacks cannot reinsert keys;
// only a newer authoritative update can publish them again.
func (m *CollectionLoadManager) UpdateCollectionDiscovery(update CollectionDiscoveryUpdate) bool {
	m.store.mu.RLock()
	cfg := m.store.configs[update.CollectionID]
	if (cfg == nil && len(update.Shards) != 0) || (cfg != nil && m.store.versions[update.CollectionID] != update.ConfigVersion) {
		m.store.mu.RUnlock()
		return false
	}
	shards := make(map[qviews.ShardID]discoverableShard, len(update.Shards))
	for _, id := range update.Shards {
		shard, valid := newDiscoverableShard(id)
		if !valid || shard.collectionID != update.CollectionID {
			m.store.mu.RUnlock()
			return false
		}
		found := false
		if cfg != nil {
			for _, r := range cfg.Replicas {
				if r.ReplicaID == id.ReplicaID {
					found = true
					break
				}
			}
		}
		if !found {
			m.store.mu.RUnlock()
			return false
		}
		shards[id] = shard
	}
	m.mu.Lock()
	if update.Revision <= m.discoveryRevisions[update.CollectionID] {
		m.mu.Unlock()
		m.store.mu.RUnlock()
		return false
	}
	m.discoveryRevisions[update.CollectionID] = update.Revision
	changed := false
	for id, shard := range m.discoverableShards {
		if shard.collectionID == update.CollectionID {
			if _, exists := shards[id]; !exists {
				delete(m.discoverableShards, id)
				changed = true
			}
		}
	}
	for id, shard := range shards {
		if _, exists := m.discoverableShards[id]; !exists {
			m.discoverableShards[id] = shard
			changed = true
		}
	}
	m.mu.Unlock()
	m.store.mu.RUnlock()
	if changed {
		m.notifyShardAssignmentsChanged()
	}
	return true
}
