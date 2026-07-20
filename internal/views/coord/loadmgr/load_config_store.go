package loadmgr

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

// LoadConfigStore persists and serves the per-collection desired load state.
// It is the in-memory + ETCD-backed source of truth for "what collections
// should be loaded, and which resource group constrains each replica".
//
// LoadConfigStore does NOT know about ShardID, QueryViews, or Node topology.
//
// # Copy-On-Write semantics
//
// Snapshot returns pointers into the store's copy-on-write state for zero-copy
// efficiency in this read-heavy path. Callers MUST treat the returned
// LoadConfig / ReplicaAssignment values as read-only.
//
// Put internally clones its input, so callers may reuse or mutate their own
// copy after Put returns.
//
// All methods are safe for concurrent use.
type LoadConfigStore struct {
	mu      sync.RWMutex
	catalog metastore.QueryCoordCatalog
	version uint64

	// configs keeps the live in-memory snapshot per collection.
	configs  map[int64]*LoadConfig
	versions map[int64]uint64

	// snapshot is the resident immutable view returned to Balancer.
	snapshot *LoadConfigSnapshot
}

// RecoverLoadConfigStore constructs a LoadConfigStore and rebuilds its
// in-memory state from ETCD via the catalog. It is the sole constructor:
// the store is always fully recovered before any operation.
func RecoverLoadConfigStore(ctx context.Context, catalog metastore.QueryCoordCatalog) (*LoadConfigStore, error) {
	collections, err := catalog.GetCollections(ctx)
	if err != nil {
		return nil, err
	}

	collectionIDs := make([]int64, 0, len(collections))
	for _, c := range collections {
		collectionIDs = append(collectionIDs, c.GetCollectionID())
	}

	partitions, err := catalog.GetPartitions(ctx, collectionIDs)
	if err != nil {
		return nil, err
	}

	replicas, err := catalog.GetReplicas(ctx)
	if err != nil {
		return nil, err
	}

	// Group replicas by collection for efficient lookup.
	replicasByColl := make(map[int64][]*querypb.Replica, len(collections))
	for _, r := range replicas {
		replicasByColl[r.GetCollectionID()] = append(replicasByColl[r.GetCollectionID()], r)
	}

	configs := make(map[int64]*LoadConfig, len(collections))
	versions := make(map[int64]uint64, len(collections))
	for _, info := range collections {
		collID := info.GetCollectionID()
		cfg := buildFromPersisted(info, partitions[collID], replicasByColl[collID])
		configs[collID] = cfg
		versions[collID] = 1
	}

	store := &LoadConfigStore{
		catalog:  catalog,
		version:  1,
		configs:  configs,
		versions: versions,
	}
	return store, nil
}

// Put persists the full LoadConfig to ETCD and updates the live in-memory state.
// Orphan partitions / replicas (present in the previous state but not in cfg)
// are deleted.
func (s *LoadConfigStore) Put(ctx context.Context, cfg *LoadConfig) error {
	if cfg == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	existing := s.configs[cfg.CollectionID]

	// Delete orphans (items present in existing but absent from cfg) first,
	// so stale ETCD keys do not linger after partition / replica removal.
	if existing != nil {
		if removedParts := diffInt64Set(existing.PartitionIDs, cfg.PartitionIDs); len(removedParts) > 0 {
			if err := s.catalog.ReleasePartition(ctx, cfg.CollectionID, removedParts...); err != nil {
				return err
			}
		}
		if removedReplicas := diffRemovedReplicaIDs(existing.Replicas, cfg.Replicas); len(removedReplicas) > 0 {
			if err := s.catalog.ReleaseReplica(ctx, cfg.CollectionID, removedReplicas...); err != nil {
				return err
			}
		}
	}

	// Save the full collection (including all partitions) and all replicas.
	if err := s.catalog.SaveCollection(ctx,
		cfg.toCollectionLoadInfoProto(),
		cfg.toPartitionLoadInfoProtos()...,
	); err != nil {
		return err
	}
	if len(cfg.Replicas) > 0 {
		replicaProtos := make([]*querypb.Replica, 0, len(cfg.Replicas))
		for _, r := range cfg.Replicas {
			replicaProtos = append(replicaProtos, r.toReplicaProto(cfg.CollectionID))
		}
		if err := s.catalog.SaveReplica(ctx, replicaProtos...); err != nil {
			return err
		}
	}

	s.replaceInMemoryLocked(cfg)
	s.version++
	s.versions[cfg.CollectionID] = s.version
	return nil
}

// Remove deletes all persisted state for a collection
// (CollectionLoadInfo + PartitionLoadInfo keys + all Replicas).
// No-op if the collection is not present.
func (s *LoadConfigStore) Remove(ctx context.Context, collectionID int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing := s.configs[collectionID]
	if existing == nil {
		return nil
	}

	// Remove replicas first, then collection (includes partitions via ReleaseCollection).
	if err := s.catalog.ReleaseReplicas(ctx, collectionID); err != nil {
		return err
	}
	if err := s.catalog.ReleaseCollection(ctx, collectionID); err != nil {
		return err
	}

	delete(s.configs, collectionID)
	s.version++
	delete(s.versions, collectionID)
	return nil
}

// Snapshot returns the current immutable load-config view. It refreshes the
// resident snapshot lazily when the live version has advanced. The returned
// maps point at the store's copy-on-write snapshots and must be treated as
// read-only.
func (s *LoadConfigStore) Snapshot() *LoadConfigSnapshot {
	s.mu.RLock()
	snapshot := s.snapshot
	if snapshot != nil && snapshot.Version() == s.version {
		s.mu.RUnlock()
		return snapshot
	}
	s.mu.RUnlock()

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.snapshot == nil || s.snapshot.Version() != s.version {
		s.publishSnapshotLocked()
	}
	return s.snapshot
}

func (s *LoadConfigStore) publishSnapshotLocked() {
	s.snapshot = NewLoadConfigSnapshotWithVersions(s.version, s.configs, s.versions)
}

// replaceInMemoryLocked swaps the live in-memory config for a collection.
// Must be called under s.mu.
func (s *LoadConfigStore) replaceInMemoryLocked(next *LoadConfig) {
	clone := next.Clone()
	s.configs[next.CollectionID] = clone
}

// diffRemovedReplicaIDs returns replicaIDs that appear in prev but not in next.
func diffRemovedReplicaIDs(prev, next []*ReplicaAssignment) []int64 {
	nextIDs := make(map[int64]struct{}, len(next))
	for _, r := range next {
		nextIDs[r.ReplicaID] = struct{}{}
	}
	var removed []int64
	for _, r := range prev {
		if _, keep := nextIDs[r.ReplicaID]; !keep {
			removed = append(removed, r.ReplicaID)
		}
	}
	return removed
}

// diffInt64Set returns elements in a that are not in b.
func diffInt64Set(a, b []int64) []int64 {
	bset := make(map[int64]struct{}, len(b))
	for _, v := range b {
		bset[v] = struct{}{}
	}
	var out []int64
	for _, v := range a {
		if _, ok := bset[v]; !ok {
			out = append(out, v)
		}
	}
	return out
}
