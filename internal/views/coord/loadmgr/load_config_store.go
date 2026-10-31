package loadmgr

import (
	"context"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/kv/queryview"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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
	// mu protects the shared in-memory state.
	mu sync.RWMutex
	// collectionLocks serializes persistence and in-memory commits per collection.
	collectionLocks *lock.KeyLock[int64]
	catalog         queryview.CollectionLoadInfoCatalog
	allocateVersion func(context.Context) (uint64, error)
	indexes         func(int64, ...int64) []*indexpb.IndexInfo
	schema          func(context.Context, int64) (*schemapb.CollectionSchema, error)
	history         map[int64]map[uint64]*LoadConfig
	references      map[loadInfoKey]int
	gcEnabled       bool
	version         uint64
	listeners       map[uint64]LoadConfigListener
	nextListener    uint64

	// configs keeps the live in-memory snapshot per collection.
	configs  map[int64]*LoadConfig
	versions map[int64]uint64

	// snapshot is the lazily built immutable compatibility/management view.
	// The balancer receives per-collection publication events directly.
	snapshot *LoadConfigSnapshot

	observers []func(collectionID int64, released bool)
}

// LoadConfigEntry captures an immutable config and its version in one read.
// Config is nil and ConfigVersion is zero when the collection is absent.
type LoadConfigEntry struct {
	Config        *LoadConfig
	ConfigVersion uint64
}

// LoadConfigStoreDependencies supplies persistence and metadata for publication.
// Schema supplies defaults during publication; Indexes resolves exact
// selected IDs for reads as well as current IDs for publication.
type LoadConfigStoreDependencies struct {
	Catalog         queryview.CollectionLoadInfoCatalog
	AllocateVersion func(context.Context) (uint64, error)
	Indexes         func(int64, ...int64) []*indexpb.IndexInfo
	Schema          func(context.Context, int64) (*schemapb.CollectionSchema, error)
}

// RecoverLoadConfigStore loads all immutable versions before any View can acquire
// them. GC remains disabled until the registry has rebuilt every reference.
func RecoverLoadConfigStore(ctx context.Context, deps LoadConfigStoreDependencies) (*LoadConfigStore, error) {
	s := &LoadConfigStore{
		collectionLocks: lock.NewKeyLock[int64](), catalog: deps.Catalog,
		allocateVersion: deps.AllocateVersion, indexes: deps.Indexes, schema: deps.Schema,
		version: 1, configs: make(map[int64]*LoadConfig), versions: make(map[int64]uint64),
		history: make(map[int64]map[uint64]*LoadConfig), references: make(map[loadInfoKey]int),
	}
	records, err := deps.Catalog.List(ctx)
	if err != nil {
		return nil, err
	}

	for _, info := range records {
		if info.GetVersion() == 0 || len(info.GetRequiredFields()) == 0 {
			return nil, merr.WrapErrDataIntegrityMsg("invalid collection load info, collection=%d version=%d", info.GetCollectionID(), info.GetVersion())
		}
		cfg := buildFromPersisted(info)
		versions := s.history[cfg.CollectionID]
		if versions == nil {
			versions = make(map[uint64]*LoadConfig)
			s.history[cfg.CollectionID] = versions
		}
		versions[cfg.Version] = cfg
		if cfg.Version > s.versions[cfg.CollectionID] {
			s.configs[cfg.CollectionID], s.versions[cfg.CollectionID] = cfg, cfg.Version
		}
	}
	// Durable DDL callbacks replay after startup. Release withdraws this
	// recovered intent again, drains any newly planned Views, and only then
	// removes the retained records. Collection locking serializes a later Load.
	return s, nil
}

// Put publishes a new immutable version after persistence. The caller holds the
// collection DDL guard; the local guard also serializes reference GC and release.
func (s *LoadConfigStore) Put(ctx context.Context, cfg *LoadConfig) error {
	if cfg == nil {
		return nil
	}
	s.collectionLocks.Lock(cfg.CollectionID)
	defer s.collectionLocks.Unlock(cfg.CollectionID)
	return s.putLocked(ctx, cfg)
}

func (s *LoadConfigStore) putLocked(ctx context.Context, cfg *LoadConfig) error {
	cfg, err := s.normalize(ctx, cfg)
	if err != nil {
		return err
	}
	s.mu.RLock()
	previous := s.configs[cfg.CollectionID]
	s.mu.RUnlock()
	if sameLoadRequirements(previous, cfg) {
		return nil
	}
	version, err := s.allocateVersion(ctx)
	if err != nil {
		return err
	}
	if version == 0 || (previous != nil && version <= previous.Version) {
		return merr.WrapErrServiceInternalMsg("non-increasing collection load version %d", version)
	}
	cfg.Version = version
	if err := s.catalog.Save(ctx, cfg.toCollectionLoadConfigProto()); err != nil {
		return err
	}
	s.mu.Lock()
	versions := s.history[cfg.CollectionID]
	if versions == nil {
		versions = make(map[uint64]*LoadConfig)
		s.history[cfg.CollectionID] = versions
	}
	versions[version] = cfg
	s.configs[cfg.CollectionID], s.versions[cfg.CollectionID] = cfg, version
	s.version++
	s.publishConfigLocked(cfg.CollectionID)
	s.mu.Unlock()
	s.notifyObservers(cfg.CollectionID, false)
	return nil
}

// StopPlanning withdraws desired state but retains all metadata for node cleanup.
// The durable Release callback must drain reconciliation and wait for final View
// removal before calling Remove. The durable callback repeats this after restart.
func (s *LoadConfigStore) StopPlanning(collectionID int64) {
	s.collectionLocks.Lock(collectionID)
	defer s.collectionLocks.Unlock(collectionID)
	s.mu.Lock()
	delete(s.configs, collectionID)
	delete(s.versions, collectionID)
	s.version++
	s.publishConfigLocked(collectionID)
	s.mu.Unlock()
	s.notifyObservers(collectionID, true)
}

// Remove completes Release after all View references have been returned. It is
// idempotent across a crash after deletion and before the durable callback ACK.
func (s *LoadConfigStore) Remove(ctx context.Context, collectionID int64) error {
	s.collectionLocks.Lock(collectionID)
	defer s.collectionLocks.Unlock(collectionID)
	s.mu.RLock()
	active := s.configs[collectionID] != nil
	referenced := false
	for version := range s.history[collectionID] {
		if s.references[loadInfoKey{collectionID, version}] != 0 {
			referenced = true
			break
		}
	}
	s.mu.RUnlock()
	if active || referenced {
		return merr.WrapErrServiceNotReadyMsg("collection %d still owns load requirements", collectionID)
	}
	if err := s.catalog.RemoveCollection(ctx, collectionID); err != nil {
		return err
	}
	s.mu.Lock()
	delete(s.history, collectionID)
	s.mu.Unlock()
	return nil
}

// sameLoadRequirements compares canonical immutable content, not its identity.
// It makes durable callback replay idempotent without reusing version IDs after
// Release. Index selections and replica priorities participate in equality.
func sameLoadRequirements(a, b *LoadConfig) bool {
	if a == nil || b == nil {
		return a == b
	}
	left, right := a.toCollectionLoadConfigProto(), b.toCollectionLoadConfigProto()
	left.Version, right.Version = 0, 0
	return proto.Equal(left, right)
}

// RegisterObserver observes future committed changes. Callbacks run under the
// collection guard so a release notification precedes any subsequent reload.
// They must only signal waiters, never perform I/O or reenter the store.
func (s *LoadConfigStore) RegisterObserver(observer func(collectionID int64, released bool)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.observers = append(s.observers, observer)
}

func (s *LoadConfigStore) notifyObservers(collectionID int64, released bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, observer := range s.observers {
		observer(collectionID, released)
	}
}

// Get reads one collection and its version without materializing a snapshot.
func (s *LoadConfigStore) Get(collectionID int64) LoadConfigEntry {
	config, version := s.GetConfigWithVersion(collectionID)
	return LoadConfigEntry{Config: config, ConfigVersion: version}
}

// Contains reports whether a collection has a live load config without
// materializing the full balancer snapshot.
func (s *LoadConfigStore) Contains(collectionID int64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.configs[collectionID] != nil
}

// GetConfig returns the live load config for a single collection without
// materializing the full balancer snapshot. Entries are immutable after Put
// (replaced, never mutated in place), so the returned pointer can be read
// safely by the caller. Unlike Snapshot(), this read is O(1) and never
// triggers an O(N) snapshot rebuild, so it is the correct accessor for hot
// single-collection paths (e.g. the load RPC current-config check).
//
// Precondition: callers must treat the returned *LoadConfig as read-only.
func (s *LoadConfigStore) GetConfig(collectionID int64) *LoadConfig {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.configs[collectionID]
}

// GetConfigVersion returns the stable content identity of one live config
// without materializing the full management snapshot. It is the
// O(1) counterpart of LoadConfigSnapshot.ConfigVersion. Use GetConfigWithVersion
// when a caller needs a config and its version atomically.
func (s *LoadConfigStore) GetConfigVersion(collectionID int64) uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.versions[collectionID]
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

// GetConfigWithVersion returns one immutable configuration and its matching version.
func (s *LoadConfigStore) GetConfigWithVersion(collectionID int64) (*LoadConfig, uint64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.configs[collectionID], s.versions[collectionID]
}

// LoadConfigListener receives immutable committed configs and their durable
// version; nil and version zero denote withdrawal of load intent.
// It runs under the store lock and must not re-enter the store or perform I/O.
type LoadConfigListener func(collectionID int64, config *LoadConfig, version uint64)

// RegisterLoadConfigListener installs a listener and replays state atomically
// with respect to source publications. The return value unregisters it.
func (s *LoadConfigStore) RegisterLoadConfigListener(listener LoadConfigListener) func() {
	s.mu.Lock()
	if s.listeners == nil {
		s.listeners = make(map[uint64]LoadConfigListener)
	}
	s.nextListener++
	id := s.nextListener
	s.listeners[id] = listener
	for collectionID, config := range s.configs {
		listener(collectionID, config, s.versions[collectionID])
	}
	s.mu.Unlock()
	return func() { s.mu.Lock(); delete(s.listeners, id); s.mu.Unlock() }
}

func (s *LoadConfigStore) publishConfigLocked(collectionID int64) {
	for _, listener := range s.listeners {
		listener(collectionID, s.configs[collectionID], s.versions[collectionID])
	}
}
