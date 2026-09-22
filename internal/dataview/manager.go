// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dataview

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/views/coord/balancer/api"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type Catalog interface {
	SaveDataView(ctx context.Context, dataView *viewpb.DataViewOfCollection) error
	ListAllDataViews(ctx context.Context) ([]*viewpb.DataViewOfCollection, error)
	DropDataView(ctx context.Context, collectionID int64, dataVersion *viewpb.DataVersion) error
	DropDataViews(ctx context.Context, collectionID int64) error
}

type CollectionRecoveryValidator func(ctx context.Context, collectionID int64) (recover bool, err error)

// Projector computes the loadable Segment projection of a Collection from its
// SegmentMeta. It is injected by the Coordinator (datacoord) and must be
// callable while the Collection's DataView lock is held: it reads SegmentMeta
// only (no DataView calls, no catalog writes), is non-blocking, and must not
// re-enter the Manager.
type Projector func(ctx context.Context, collectionID int64) ([]LoadableSegment, error)

type Manager interface {
	api.DataViewPublisher
	OnCreateCollection(ctx context.Context, event CreateCollectionDataViewEvent) (*viewpb.DataVersion, error)
	OnBootstrapCollection(ctx context.Context, event BootstrapCollectionDataViewEvent) (*viewpb.DataVersion, error)
	// PrepareFlush builds the post-flush snapshot under the Collection lock
	// for the flush atomic txn (see implementation comment for the
	// commit/abort contract).
	PrepareFlush(ctx context.Context, event FlushDataViewEvent) (view *viewpb.DataViewOfCollection, commit func(), abort func(), err error)
	// Recompute asynchronously requests a reconciliation of the Collection
	// snapshot against the latest SegmentMeta projection (injected at
	// construction). It is a non-blocking, deduplicated request: a Collection
	// with a pending request is not queued again, and the manager's worker
	// rebuilds the snapshot from the freshest projection, so multiple pending
	// mutations collapse into one snapshot write. A request may be dropped
	// when the queue is full; the recovery rebuild converges. Returns an error
	// when the manager is not usable (e.g. no projector injected).
	Recompute(ctx context.Context, collectionID int64) error
	// RecomputeNow synchronously reconciles the Collection snapshot against
	// project and returns the resulting DataVersion. It is the sync primitive
	// behind recovery reconciliation (RecoverManager performs the recovery
	// pass at construction time) and is exposed for tests and callers that
	// need a deterministic reconciliation. Same semantics as the async
	// worker's step.
	RecomputeNow(ctx context.Context, collectionID int64, project Projector) (*viewpb.DataVersion, error)
	OnDropCollection(ctx context.Context, collectionID int64) (*viewpb.DataVersion, error)

	Latest(ctx context.Context, collectionID int64) (DataViewRef, error)
	Get(ctx context.Context, collectionID int64, dataVersion *viewpb.DataVersion) (DataViewRef, error)
	GarbageCollect(ctx context.Context, collectionID int64, retainLatest int) error
}

// DataViewRef is the read-only reference to one DataView version shared out
// by the Manager. It is defined in qviews with an immutable precondition: the
// caller must not mutate the returned DataView / SegmentStats data.
type DataViewRef = qviews.DataViewRef

type LoadableSegment struct {
	SegmentID       int64
	VChannel        string
	PartitionID     int64
	ManifestVersion int64
	// RowNum is the segment's published row count, projected from SegmentMeta
	// by the Coordinator. It is maintained in memory only (never persisted,
	// never re-read from SegmentMeta) and exposed via DataViewRef.Stats.
	RowNum int64
}

type CreateCollectionDataViewEvent struct {
	CollectionID int64
	VChannels    []string
}

// BootstrapCollectionDataViewEvent seeds the initial DataView snapshot for a
// Collection that predates DataView management (upgrade bootstrap). It is a
// one-time migration: if the Collection already has a persisted snapshot, the
// event is a no-op and returns the existing latest version.
type BootstrapCollectionDataViewEvent struct {
	CollectionID int64
	VChannels    []string
	Segments     []LoadableSegment
}

type FlushDataViewEvent struct {
	CollectionID int64
	Segments     []LoadableSegment
}

type dataViewAdvance int

const (
	dataViewAdvanceStreaming dataViewAdvance = iota
	dataViewAdvanceCompact
)

// SegmentStats is the per-segment load footprint published by the Manager for
// one DataView version. It lives in qviews so DataViewRef (also defined
// there) can expose it through the Stats API.
type SegmentStats = qviews.SegmentStats

type versionEntry struct {
	view *viewpb.DataViewOfCollection
	// stats is the per-segment published footprint (segmentID -> SegmentStats)
	// of this version's snapshot. Immutable after publication: readers holding
	// a ref to the entry may access it without the collection lock.
	stats map[int64]SegmentStats
	refs  *versionRefCounter
	// isTombstone marks an entry whose etcd snapshot key is being dropped by
	// GarbageCollect while the per-collection lock is released for the
	// DropDataView round trips. Get refuses tombstoned entries so no ref can
	// be acquired to a version whose snapshot key no longer exists.
	isTombstone bool
}

type versionRefCounter struct {
	count int
}

type collectionState struct {
	mu       sync.Mutex
	id       int64
	latest   *versionEntry
	versions map[qviews.DataVersion]*versionEntry
	// Retain only the latest native projection, not one for every historical version.
	publishedEntry *versionEntry
	native         *api.CollectionDataView
}

// newCollectionState constructs a Collection state with an empty version
// registry keyed by DataVersion struct.
func newCollectionState(collectionID int64) *collectionState {
	return &collectionState{id: collectionID, versions: make(map[qviews.DataVersion]*versionEntry)}
}

type dataViewManager struct {
	publicationMu sync.Mutex
	published     map[int64]*api.CollectionDataView
	listeners     map[uint64]api.DataViewListener
	nextListener  uint64
	mu            sync.RWMutex
	catalog       Catalog
	states        map[int64]*collectionState
	// dropped records Collections whose DataViews were removed by
	// OnDropCollection. Late mutations (in-flight flush, queued recompute)
	// consult it and no-op instead of resurrecting the state or persisting
	// orphan keys for a dropped Collection.
	dropped map[int64]struct{}

	// projector is the SegmentMeta projection injected at construction; the
	// async Recompute worker runs against it. nil means the manager is not
	// wired for async reconciliation (Recompute returns an error).
	projector Projector
	// workerCtx bounds the async Recompute worker's lifetime. NewManager uses
	// a process-lifetime context; RecoverManager derives it from the caller's
	// ctx so the worker stops when the coordinator does.
	workerCtx context.Context
	queue     *dataViewRecomputeQueue
	startOnce sync.Once
}

type dataViewRef struct {
	once  sync.Once
	state *collectionState
	entry *versionEntry
}

// NewManager constructs a DataView manager with the SegmentMeta projection
// wired for async Recompute reconciliation. The async worker starts
// immediately (bounded by the process lifetime). Pass nil as project when the
// manager only serves synchronous operations (PrepareFlush, RecomputeNow,
// bootstrap); Recompute requests then return an error.
func NewManager(catalog Catalog, project Projector) Manager {
	m := newManager(context.Background(), catalog, project)
	m.startWorker()
	return m
}

func newManager(ctx context.Context, catalog Catalog, project Projector) *dataViewManager {
	m := &dataViewManager{
		catalog:   catalog,
		states:    make(map[int64]*collectionState),
		dropped:   make(map[int64]struct{}),
		projector: project,
		workerCtx: ctx,
	}
	m.queue = newDataViewRecomputeQueue(m)
	return m
}

// startWorker launches the async Recompute worker exactly once; it stops when
// the manager's workerCtx is canceled.
func (m *dataViewManager) startWorker() {
	m.startOnce.Do(func() {
		go m.queue.run(m.workerCtx)
	})
}

// getProjector returns the injected SegmentMeta projection, or nil when none
// was set.
func (m *dataViewManager) getProjector() Projector {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.projector
}

// Recompute asynchronously requests a reconciliation of the Collection
// snapshot against the injected SegmentMeta projection. See the Manager
// interface comment for the queue contract.
func (m *dataViewManager) Recompute(ctx context.Context, collectionID int64) error {
	m.mu.RLock()
	projector := m.projector
	queue := m.queue
	m.mu.RUnlock()
	if projector == nil {
		return merr.WrapErrServiceInternalMsg(
			"DataView Recompute requested without an injected projection for collection %d",
			collectionID)
	}
	return queue.Enqueue(collectionID)
}

// RecoverManager constructs a DataView manager and performs the full recovery
// pass in one call, so the Coordinator only wires the constructor and has no
// post-construction setup to remember:
//
//   - persisted DataView snapshots are loaded and validated (existing logic);
//   - every live Collection (liveCollectionIDs) whose validator allows
//     recovery is synchronously reconciled against the SegmentMeta projection
//     (project) - SegmentMeta is the source of truth, so this both upgrades a
//     Collection that predates DataView management (no snapshot yet) and
//     converges snapshots that missed an event; recompute is a no-op when the
//     snapshot already matches;
//   - a Collection without any persisted snapshot gets its first snapshot
//     through the declared vchannel skeleton (collectionVChannels), so empty
//     channels and channels without loadable segments are present instead of
//     waiting for the first loadable Segment;
//   - the async Recompute worker is started, bounded by ctx (canceling ctx
//     stops the worker).
func RecoverManager(
	ctx context.Context,
	catalog Catalog,
	validator CollectionRecoveryValidator,
	project Projector,
	liveCollectionIDs []int64,
	collectionVChannels map[int64][]string,
) (Manager, error) {
	if validator == nil {
		return nil, merr.WrapErrServiceInternalMsg("DataView recovery requires a Collection recovery validator")
	}
	views, err := catalog.ListAllDataViews(ctx)
	if err != nil {
		return nil, err
	}

	viewsByCollection := make(map[int64][]*viewpb.DataViewOfCollection)
	for _, view := range views {
		if view == nil || view.GetDataVersion() == nil {
			mlog.Warn(ctx, "skip persisted DataView without DataVersion during recovery")
			continue
		}
		if view.GetCollectionId() == 0 || view.GetDataVersion().GetStreamingVersion() == 0 {
			mlog.Warn(ctx, "skip persisted DataView with invalid identity during recovery",
				mlog.Int64("collectionID", view.GetCollectionId()),
				mlog.String("version", dataVersionKey(view.GetDataVersion())))
			continue
		}
		if err := validatePersistedSegmentManifestVersions(view); err != nil {
			mlog.Warn(ctx, "skip malformed persisted DataView during recovery",
				mlog.Int64("collectionID", view.GetCollectionId()),
				mlog.String("version", dataVersionKey(view.GetDataVersion())),
				mlog.Err(err))
			continue
		}
		viewsByCollection[view.GetCollectionId()] = append(viewsByCollection[view.GetCollectionId()], view)
	}

	collectionIDs := make([]int64, 0, len(viewsByCollection))
	for collectionID := range viewsByCollection {
		collectionIDs = append(collectionIDs, collectionID)
	}
	sort.Slice(collectionIDs, func(i, j int) bool { return collectionIDs[i] < collectionIDs[j] })

	recoverCollection := make(map[int64]bool, len(collectionIDs))
	for _, collectionID := range collectionIDs {
		recover, err := validator(ctx, collectionID)
		if err != nil {
			return nil, err
		}
		recoverCollection[collectionID] = recover
	}

	manager := newManager(ctx, catalog, project)
	for _, collectionID := range collectionIDs {
		if !recoverCollection[collectionID] {
			continue
		}
		for _, view := range viewsByCollection[collectionID] {
			state := manager.getOrCreateState(view.GetCollectionId())
			state.mu.Lock()
			persisted := canonicalDataViewClone(view)
			key := protoVersionToStruct(view.GetDataVersion())
			if existing := state.versions[key]; existing != nil {
				if !proto.Equal(existing.view, persisted) {
					state.mu.Unlock()
					mlog.Warn(ctx, "skip persisted DataView with conflicting snapshots under one version during recovery",
						mlog.Int64("collectionID", view.GetCollectionId()),
						mlog.String("version", key.String()))
					continue
				}
				state.mu.Unlock()
				continue
			}
			entry := newVersionEntry(persisted)
			state.versions[key] = entry
			if state.latest == nil || compareDataVersion(entry.view.GetDataVersion(), state.latest.view.GetDataVersion()) > 0 {
				state.latest = entry
			}
			state.mu.Unlock()
		}
	}
	for _, collectionID := range collectionIDs {
		if recoverCollection[collectionID] {
			continue
		}
		if err := catalog.DropDataViews(ctx, collectionID); err != nil {
			return nil, err
		}
	}
	// Recovery reconciliation: SegmentMeta is the source of truth. Rebuild
	// every recoverable live Collection's snapshot from the projection so a
	// Collection that predates DataView management gets its first snapshot and
	// a stale one converges. recomputeNow is a no-op when the rebuilt snapshot
	// matches the persisted one, which also makes re-running this pass inside
	// the initMeta retry safe.
	for _, collectionID := range liveCollectionIDs {
		recover, err := validator(ctx, collectionID)
		if err != nil {
			return nil, err
		}
		if !recover {
			continue
		}
		if _, hasSnapshot := viewsByCollection[collectionID]; !hasSnapshot {
			// First snapshot for a Collection that predates DataView
			// management: seed it through the declared vchannel skeleton so
			// empty channels and channels without loadable segments are
			// present (recomputeNow could only discover shards from
			// projected segments).
			segments, err := project(ctx, collectionID)
			if err != nil {
				mlog.Warn(ctx, "failed to project segments for DataView bootstrap",
					mlog.Int64("collectionID", collectionID), mlog.Err(err))
				continue
			}
			if _, err := manager.OnBootstrapCollection(ctx, BootstrapCollectionDataViewEvent{
				CollectionID: collectionID,
				VChannels:    collectionVChannels[collectionID],
				Segments:     segments,
			}); err != nil {
				mlog.Warn(ctx, "failed to bootstrap DataView for collection during recovery",
					mlog.Int64("collectionID", collectionID), mlog.Err(err))
			}
			continue
		}
		if _, err := manager.recomputeNow(ctx, collectionID, project); err != nil {
			mlog.Warn(ctx, "failed to rebuild DataView for collection during recovery",
				mlog.Int64("collectionID", collectionID), mlog.Err(err))
		}
	}
	// Failed recovery attempts must not leave a worker retaining their
	// snapshots and SegmentMeta projection until the server context ends.
	for _, state := range manager.states {
		state.mu.Lock()
		manager.publishDataViewLocked(state)
		state.mu.Unlock()
	}
	manager.startWorker()
	return manager, nil
}

func (m *dataViewManager) OnCreateCollection(ctx context.Context, event CreateCollectionDataViewEvent) (*viewpb.DataVersion, error) {
	state, unlock := m.lockStateForMutation(event.CollectionID)
	if state == nil {
		return nil, nil
	}
	defer unlock()
	if state.latest != nil {
		return cloneDataVersion(state.latest.view.GetDataVersion()), nil
	}

	view := buildEmptyDataView(event.CollectionID, event.VChannels)
	view.DataVersion = &viewpb.DataVersion{StreamingVersion: 1}
	if err := m.persistLocked(ctx, state, view); err != nil {
		return nil, err
	}
	return cloneDataVersion(view.GetDataVersion()), nil
}

func (m *dataViewManager) OnBootstrapCollection(ctx context.Context, event BootstrapCollectionDataViewEvent) (*viewpb.DataVersion, error) {
	state, unlock := m.lockStateForMutation(event.CollectionID)
	if state == nil {
		return nil, nil
	}
	defer unlock()
	if state.latest != nil {
		return cloneDataVersion(state.latest.view.GetDataVersion()), nil
	}

	view := buildEmptyDataView(event.CollectionID, event.VChannels)
	view.DataVersion = &viewpb.DataVersion{StreamingVersion: 1}
	if err := addSegments(view, event.Segments); err != nil {
		return nil, err
	}
	if err := m.persistLockedWithStats(ctx, state, view, buildSegmentRowStats(event.Segments)); err != nil {
		return nil, err
	}
	return cloneDataVersion(view.GetDataVersion()), nil
}

// RecomputeNow synchronously reconciles the Collection snapshot against
// project. It rebuilds membership (and Manifest versions) from scratch, so it
// absorbs every pending mutation since the last write: compaction input
// retirement, output publication, L0 manifest bumps, import, copy, refresh,
// truncate and partition drops are all expressed by the projection's return
// value. A snapshot whose content is unchanged is not persisted and the
// current DataVersion is returned (idempotent no-op).
func (m *dataViewManager) RecomputeNow(ctx context.Context, collectionID int64, project Projector) (*viewpb.DataVersion, error) {
	return m.recomputeNow(ctx, collectionID, project)
}

func (m *dataViewManager) recomputeNow(ctx context.Context, collectionID int64, project Projector) (*viewpb.DataVersion, error) {
	if project == nil {
		return nil, merr.WrapErrServiceInternalMsg("DataView Recompute requires a projection function")
	}
	state, unlock := m.lockStateForMutation(collectionID)
	if state == nil {
		return nil, nil
	}
	defer unlock()

	base := latestView(state)
	segments, err := project(ctx, collectionID)
	if err != nil {
		return nil, err
	}
	next := canonicalDataViewClone(base)
	if next == nil {
		next = &viewpb.DataViewOfCollection{CollectionId: collectionID}
	}
	if err := rebuildSegments(next, segments); err != nil {
		return nil, err
	}
	canonicalizeDataView(next)
	if dataViewMembershipEqual(base, next) {
		// Recovery path: a persisted entry loaded from the catalog carries no
		// stats (they are never serialized), so a restart leaves the latest
		// entry with an empty footprint. Initialize it from the projection
		// once: segment RowNum is immutable, so the fill is idempotent and
		// never overwrites a published value (a normally-published entry is
		// never empty).
		if entry := state.latest; entry != nil && len(entry.stats) == 0 {
			if stats := buildSegmentRowStats(segments); len(stats) > 0 {
				updated := *entry
				updated.stats = stats
				state.versions[protoVersionToStruct(entry.view.GetDataVersion())] = &updated
				state.latest = &updated
			}
		}
		m.publishDataViewLocked(state)
		return dataVersionFromView(base), nil
	}
	next.DataVersion = nextDataVersion(base, dataViewAdvanceCompact)
	if err := m.persistLockedWithStats(ctx, state, next, buildSegmentRowStats(segments)); err != nil {
		return nil, err
	}
	return cloneDataVersion(next.GetDataVersion()), nil
}

func (m *dataViewManager) OnDropCollection(ctx context.Context, collectionID int64) (*viewpb.DataVersion, error) {
	m.mu.Lock()
	// Tombstone the Collection first so concurrent lockStateForMutation
	// callers observe the drop and no-op instead of recreating the state.
	m.dropped[collectionID] = struct{}{}
	state := m.states[collectionID]
	if state != nil {
		delete(m.states, collectionID)
	}
	// Never acquire state.mu while holding the manager-global lock:
	// lockStateForMutation takes state.mu first and only then m.mu.RLock
	// (re-validation), so holding m.mu across state.mu.Lock would invert the
	// order and deadlock the whole coordinator against an in-flight flush or
	// recompute on the same Collection.
	m.mu.Unlock()
	m.publishDataViewDrop(collectionID)
	if state != nil {
		state.mu.Lock()
		defer state.mu.Unlock()
	}
	// The catalog prefix delete must run under the per-collection state lock
	// only: holding the manager-global mutex across the etcd round trip would
	// block every membership event of every other collection for its duration.
	if err := m.catalog.DropDataViews(ctx, collectionID); err != nil {
		return nil, err
	}
	return nil, nil
}

func (m *dataViewManager) Latest(_ context.Context, collectionID int64) (DataViewRef, error) {
	state := m.getState(collectionID)
	if state == nil {
		return nil, nil
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	return acquireRefLocked(state, state.latest), nil
}

func (m *dataViewManager) Get(_ context.Context, collectionID int64, version *viewpb.DataVersion) (DataViewRef, error) {
	if version == nil {
		return nil, nil
	}
	state := m.getState(collectionID)
	if state == nil {
		return nil, nil
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	return acquireRefLocked(state, state.versions[protoVersionToStruct(version)]), nil
}

func (m *dataViewManager) GarbageCollect(ctx context.Context, collectionID int64, retainLatest int) error {
	state := m.getState(collectionID)
	if state == nil {
		return nil
	}
	if retainLatest < 1 {
		retainLatest = 1
	}

	// Snapshot the collectable versions under the per-collection lock, then
	// drop their etcd keys outside it: holding state.mu across one serial
	// DropDataView round trip per collected version would stall the single
	// cluster-wide recompute worker (recomputeNow -> lockStateForMutation) and
	// the flush path for the whole sweep.
	type collectableVersion struct {
		version *viewpb.DataVersion
		key     qviews.DataVersion
	}
	state.mu.Lock()
	entries := make([]*versionEntry, 0, len(state.versions))
	for _, entry := range state.versions {
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool {
		return compareDataVersion(entries[i].view.GetDataVersion(), entries[j].view.GetDataVersion()) > 0
	})
	collectable := make([]collectableVersion, 0, len(entries))
	for idx, entry := range entries {
		if idx < retainLatest || entry == state.latest || entry.refs.count > 0 {
			continue
		}
		version := entry.view.GetDataVersion()
		// Tombstone the entry while the lock is still held: the etcd drop
		// happens below with the lock released, and the marker closes the
		// window where a concurrent Get could otherwise acquire a ref to a
		// version whose snapshot key is about to be gone.
		entry.isTombstone = true
		collectable = append(collectable, collectableVersion{
			version: version,
			key:     protoVersionToStruct(version),
		})
	}
	state.mu.Unlock()

	for i, candidate := range collectable {
		if err := m.catalog.DropDataView(ctx, collectionID, candidate.version); err != nil {
			// The drop failed. Candidates up to i already lost their etcd key
			// and stay tombstoned (refused by Get, reclaimed on a later GC
			// retry by the cleanup loop); the remaining candidates kept their
			// key, so clear their tombstones to serve them again.
			state.mu.Lock()
			for _, c := range collectable[i:] {
				if entry := state.versions[c.key]; entry != nil {
					entry.isTombstone = false
				}
			}
			state.mu.Unlock()
			return err
		}
	}

	// Re-acquire the lock to remove the collected versions from the in-memory
	// table, re-checking that nothing changed while the lock was released: a
	// concurrent Get/Latest may have taken a ref, or a flush may have advanced
	// state.latest onto an entry that was collectable a moment ago.
	state.mu.Lock()
	defer state.mu.Unlock()
	for _, candidate := range collectable {
		entry := state.versions[candidate.key]
		if entry == nil || entry == state.latest || entry.refs.count > 0 {
			continue
		}
		delete(state.versions, candidate.key)
	}
	return nil
}

// collectionDataViewLocked builds the native collection projection from the
// latest version and its matching row statistics. Caller holds state.mu.
func (m *dataViewManager) collectionDataViewLocked(state *collectionState) *api.CollectionDataView {
	m.mu.RLock()
	current := m.states[state.id] == state
	m.mu.RUnlock()
	if !current {
		return nil
	}

	entry := state.latest
	if entry == nil || entry.isTombstone {
		return nil
	}
	view := entry.view

	coll := &api.CollectionDataView{
		CollectionID: view.GetCollectionId(),
		DataVersion:  qviews.FromProtoDataVersion(view.GetDataVersion()),
		Shards:       make([]*api.ShardDataView, 0, len(view.GetShards())),
	}
	for _, shard := range view.GetShards() {
		if shard == nil {
			continue
		}
		nativeShard := &api.ShardDataView{
			VChannel:   shard.GetVchannel(),
			Partitions: make([]*api.PartitionDataView, 0, len(shard.GetPartitions())),
		}
		for _, partition := range shard.GetPartitions() {
			if partition == nil {
				continue
			}
			segments := make([]*api.SegmentDataView, 0, len(partition.GetSegmentIds()))
			for _, segmentID := range partition.GetSegmentIds() {
				segment := &api.SegmentDataView{
					SegmentID:   segmentID,
					PartitionID: partition.GetPartitionId(),
				}
				if stats, ok := entry.stats[segmentID]; ok {
					segment.RowNum = stats.RowNum
				}
				segments = append(segments, segment)
			}
			nativeShard.Partitions = append(nativeShard.Partitions, &api.PartitionDataView{
				PartitionID: partition.GetPartitionId(),
				Segments:    segments,
			})
		}
		coll.Shards = append(coll.Shards, nativeShard)
	}
	return coll
}

// persistLocked persists a snapshot and loads it into memory. A nil stats
// map is normalized to an empty one so every published entry carries a
// non-nil (possibly empty) footprint — the invariant the recovery fill and
// Stats rely on.
func (m *dataViewManager) persistLocked(ctx context.Context, state *collectionState, view *viewpb.DataViewOfCollection) error {
	return m.persistLockedWithStats(ctx, state, view, make(map[int64]SegmentStats))
}

// persistLockedWithStats persists a snapshot and loads it into memory with
// the per-segment stats of the projection that produced it. A nil stats
// map publishes an empty footprint (Stats returns false).
func (m *dataViewManager) persistLockedWithStats(ctx context.Context, state *collectionState, view *viewpb.DataViewOfCollection, stats map[int64]SegmentStats) error {
	persisted := canonicalDataViewClone(view)
	key := protoVersionToStruct(persisted.GetDataVersion())
	if existing := state.versions[key]; existing != nil {
		if !proto.Equal(existing.view, persisted) {
			return merr.WrapErrDataIntegrityMsg(
				"DataView version %s of collection %d has conflicting snapshots",
				key.String(), state.id,
			)
		}
		state.latest = existing
		m.publishDataViewLocked(state)
		return nil
	}
	if err := m.catalog.SaveDataView(ctx, persisted); err != nil {
		return err
	}
	return m.persistMemoryLockedWithStats(state, persisted, stats)
}

// persistMemoryLocked loads a snapshot that is already persisted in the
// catalog into the in-memory version registry (used by the flush atomic-txn
// path, where the catalog write happened inside the same txn as SegmentMeta).
// The caller holds the Collection lock.
func (m *dataViewManager) persistMemoryLocked(state *collectionState, view *viewpb.DataViewOfCollection) error {
	return m.persistMemoryLockedWithStats(state, view, nil)
}

// persistMemoryLockedWithStats loads a snapshot that is already persisted in
// the catalog into the in-memory version registry (used by the flush
// atomic-txn path, where the catalog write happened inside the same txn as
// SegmentMeta). The caller holds the Collection lock. stats carries the
// per-segment RowNum footprint of the projection that produced the snapshot.
func (m *dataViewManager) persistMemoryLockedWithStats(state *collectionState, view *viewpb.DataViewOfCollection, stats map[int64]SegmentStats) error {
	persisted := canonicalDataViewClone(view)
	key := protoVersionToStruct(persisted.GetDataVersion())
	if existing := state.versions[key]; existing != nil {
		if !proto.Equal(existing.view, persisted) {
			return merr.WrapErrDataIntegrityMsg(
				"DataView version %s of collection %d has conflicting snapshots",
				key.String(), state.id,
			)
		}
		state.latest = existing
		m.publishDataViewLocked(state)
		return nil
	}
	entry := newVersionEntry(persisted)
	// Normalize nil to empty so every published entry carries a non-nil
	// (possibly empty) footprint — the invariant the recovery fill and
	// Stats rely on.
	if stats == nil {
		stats = make(map[int64]SegmentStats)
	}
	entry.stats = stats
	state.versions[key] = entry
	state.latest = entry
	m.publishDataViewLocked(state)
	return nil
}

// PrepareFlush builds the post-flush DataView snapshot while holding the
// Collection lock. It is the manager side of the flush atomic txn: the caller
// composes the returned snapshot into the same catalog.Update as the
// SegmentMeta actions (see meta.UpdateSegmentsInfoAndDataView), then calls
// commit when the composite txn was persisted (loads the snapshot into memory
// and releases the lock), or abort when the txn failed and the error is
// surfaced to the caller (releases the lock without touching memory). When
// nothing was persisted (the txn short-circuited because the Collection's
// DataViews were dropped, PrepareFlush returns nil with no-op callbacks), the
// caller may skip both callbacks. All callbacks are idempotent.
func (m *dataViewManager) PrepareFlush(ctx context.Context, event FlushDataViewEvent) (*viewpb.DataViewOfCollection, func(), func(), error) {
	state, unlock := m.lockStateForMutation(event.CollectionID)
	if state == nil {
		// The Collection's DataViews were dropped while this flush was in
		// flight: publish nothing. The caller composes a nil snapshot, which
		// commits SegmentMeta alone (meta.UpdateSegmentsInfoAndDataView).
		var once sync.Once
		noop := func() { once.Do(func() {}) }
		return nil, noop, noop, nil
	}

	base := latestView(state)
	next := canonicalDataViewClone(base)
	if next == nil {
		next = &viewpb.DataViewOfCollection{CollectionId: event.CollectionID}
	}
	if err := addSegments(next, event.Segments); err != nil {
		unlock()
		return nil, nil, nil, err
	}
	canonicalizeDataView(next)
	if dataViewMembershipEqual(base, next) {
		// Idempotent replay of an already-seen flush (e.g. a retried flush
		// RPC): no new snapshot to persist, return the current one.
		view := base
		if view == nil {
			view = next
		}
		var once sync.Once
		commit := func() { once.Do(unlock) }
		abort := func() { once.Do(unlock) }
		return view, commit, abort, nil
	}
	next.DataVersion = nextDataVersion(base, dataViewAdvanceStreaming)

	// The flushed snapshot is incremental: next carries the base version's
	// segments plus the flushed ones, so its stats index must merge the base
	// entry's footprint with the flushed segments' RowNum — otherwise segments
	// carried over from the base version would lose their published Stats.
	stats := make(map[int64]SegmentStats)
	if baseEntry := state.latest; baseEntry != nil {
		for segmentID, rows := range baseEntry.stats {
			stats[segmentID] = rows
		}
	}
	for segmentID, rows := range buildSegmentRowStats(event.Segments) {
		stats[segmentID] = rows
	}

	var once sync.Once
	commit := func() {
		once.Do(func() {
			defer unlock()
			if err := m.persistMemoryLockedWithStats(state, next, stats); err != nil {
				mlog.Warn(ctx, "failed to load prepared flush snapshot into DataView memory",
					mlog.Int64("collectionID", event.CollectionID),
					mlog.Err(err))
			}
		})
	}
	abort := func() {
		once.Do(unlock)
	}
	return next, commit, abort, nil
}

func (m *dataViewManager) getState(collectionID int64) *collectionState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.states[collectionID]
}

// lockStateForMutation returns the per-collection state with its lock held
// for a mutation, or (nil, nil) when the Collection's DataViews were dropped
// (late events must no-op). The manager-global lock is never held while
// waiting on the per-collection lock: state.mu is held across unbounded
// catalog I/O by PrepareFlush and recomputeNow, so acquiring it under m.mu
// would stall every other Collection's read path (getState, getProjector)
// for the whole RTT. The state is re-validated under state.mu so a drop that
// lands while we wait turns the mutation into a no-op instead of
// resurrecting a dropped Collection's state.
func (m *dataViewManager) lockStateForMutation(collectionID int64) (*collectionState, func()) {
	for {
		m.mu.RLock()
		if _, dropped := m.dropped[collectionID]; dropped {
			m.mu.RUnlock()
			return nil, nil
		}
		state := m.states[collectionID]
		m.mu.RUnlock()
		if state == nil {
			m.mu.Lock()
			if _, dropped := m.dropped[collectionID]; dropped {
				m.mu.Unlock()
				return nil, nil
			}
			state = m.states[collectionID]
			if state == nil {
				state = newCollectionState(collectionID)
				m.states[collectionID] = state
			}
			m.mu.Unlock()
		}
		state.mu.Lock()
		m.mu.RLock()
		if _, dropped := m.dropped[collectionID]; dropped {
			m.mu.RUnlock()
			state.mu.Unlock()
			return nil, nil
		}
		current := m.states[collectionID]
		m.mu.RUnlock()
		if current == state {
			return state, state.mu.Unlock
		}
		state.mu.Unlock()
	}
}

func (m *dataViewManager) getOrCreateState(collectionID int64) *collectionState {
	m.mu.Lock()
	defer m.mu.Unlock()
	state := m.states[collectionID]
	if state == nil {
		state = newCollectionState(collectionID)
		m.states[collectionID] = state
	}
	return state
}

func acquireRefLocked(state *collectionState, entry *versionEntry) DataViewRef {
	if entry == nil || entry.isTombstone {
		return nil
	}
	entry.refs.count++
	return &dataViewRef{state: state, entry: entry}
}

func (r *dataViewRef) DataView() *viewpb.DataViewOfCollection {
	if r == nil || r.entry == nil {
		return nil
	}
	return canonicalDataViewClone(r.entry.view)
}

func (r *dataViewRef) Version() *viewpb.DataVersion {
	if r == nil || r.entry == nil {
		return nil
	}
	return cloneDataVersion(r.entry.view.GetDataVersion())
}

// Stats returns the published SegmentStats of one segment in the referenced
// version. It is a lock-free map lookup on the immutable versionEntry.stats;
// ok is false when the segment has no published footprint in this version.
func (r *dataViewRef) Stats(segmentID int64) (SegmentStats, bool) {
	if r == nil || r.entry == nil {
		return SegmentStats{}, false
	}
	stat, ok := r.entry.stats[segmentID]
	return stat, ok
}

func (r *dataViewRef) Deref() {
	if r == nil {
		return
	}
	r.once.Do(func() {
		r.state.mu.Lock()
		defer r.state.mu.Unlock()
		if r.entry.refs.count > 0 {
			r.entry.refs.count--
		}
	})
}

func newVersionEntry(view *viewpb.DataViewOfCollection) *versionEntry {
	return &versionEntry{
		view:  view,
		stats: make(map[int64]SegmentStats),
		refs:  &versionRefCounter{},
	}
}

func latestView(state *collectionState) *viewpb.DataViewOfCollection {
	if state.latest == nil {
		return nil
	}
	return state.latest.view
}

func nextDataVersion(base *viewpb.DataViewOfCollection, advance dataViewAdvance) *viewpb.DataVersion {
	if base == nil || base.GetDataVersion() == nil || base.GetDataVersion().GetStreamingVersion() == 0 {
		return &viewpb.DataVersion{StreamingVersion: 1}
	}
	current := base.GetDataVersion()
	if advance == dataViewAdvanceStreaming {
		return &viewpb.DataVersion{StreamingVersion: current.GetStreamingVersion() + 1}
	}
	return &viewpb.DataVersion{
		StreamingVersion: current.GetStreamingVersion(),
		CompactVersion:   current.GetCompactVersion() + 1,
	}
}

func addSegments(view *viewpb.DataViewOfCollection, segments []LoadableSegment) error {
	slots := dataViewSegmentSlots(view)
	for _, segment := range segments {
		if segment.SegmentID == 0 || segment.VChannel == "" || segment.ManifestVersion < 0 {
			return merr.WrapErrServiceInternalMsg(
				"invalid loadable Segment descriptor: segment=%d vchannel=%q manifestVersion=%d",
				segment.SegmentID,
				segment.VChannel,
				segment.ManifestVersion,
			)
		}
		location := segmentLocation{vchannel: segment.VChannel, partitionID: segment.PartitionID}
		if known, ok := slots[segment.SegmentID]; ok {
			if known.location != location {
				return merr.WrapErrDataIntegrityMsg("Segment %d has conflicting DataView locations", segment.SegmentID)
			}
			if segment.ManifestVersion == 0 {
				// Zero is a resolution mode, not a comparable data revision
				// (see data_view.md): it means "the producer does not know the
				// manifest version, leave it alone". Preserve the stored version
				// so an idempotent replay of a membership event after an L0
				// manifest bump stays a no-op instead of a spurious regression.
				continue
			}
			currentVersion := known.partition.SegmentManifestVersions[known.index]
			if segment.ManifestVersion < currentVersion {
				// A lower positive version on an already-published Segment is
				// an idempotent replay, not a regression: the stored version
				// was advanced after this flush was first prepared (e.g. an
				// L0 compact manifest bump recompute), and the replayed
				// SaveBinlogPaths (double flush or recovery re-flush) carries
				// the manifest path known at its own flush time. Preserve the
				// stored version so the replay is a membership no-op instead
				// of failing the flush forever with "cannot regress" — the
				// SegmentMeta side treats the replay as already-flushed and
				// skips it the same way.
				continue
			}
			if segment.ManifestVersion > currentVersion {
				known.partition.SegmentManifestVersions[known.index] = segment.ManifestVersion
			}
			continue
		}
		shard := findOrCreateShard(view, segment.VChannel)
		partition := findOrCreatePartition(shard, segment.PartitionID)
		partition.SegmentIds = append(partition.SegmentIds, segment.SegmentID)
		partition.SegmentManifestVersions = append(partition.SegmentManifestVersions, segment.ManifestVersion)
		slots[segment.SegmentID] = segmentSlot{
			location:  location,
			partition: partition,
			index:     len(partition.SegmentIds) - 1,
		}
	}
	return nil
}

// rebuildSegments replaces every Segment of the snapshot with the supplied
// projection. The Collection's vchannel skeleton is preserved (empty shards
// stay), while every partition's Segment list is rebuilt from scratch, so
// segments absent from the projection (compacted away, truncated, partition
// dropped) disappear and Manifest versions are taken from the projector.
//
// Manifest version resolution: a projected ManifestVersion of 0 means "the
// producer does not know the version" (see data_view.md) and preserves the
// stored version, so an idempotent Recompute after an L0 manifest bump stays a
// no-op instead of spuriously regressing the version to 0. A positive
// projected version may only advance the stored version, never regress it.
func rebuildSegments(view *viewpb.DataViewOfCollection, segments []LoadableSegment) error {
	known := make(map[int64]int64)
	for _, shard := range view.GetShards() {
		for _, partition := range shard.GetPartitions() {
			for idx, segmentID := range partition.GetSegmentIds() {
				known[segmentID] = partition.GetSegmentManifestVersions()[idx]
			}
		}
	}
	for _, shard := range view.GetShards() {
		shard.Partitions = nil
	}
	for i := range segments {
		if segments[i].SegmentID == 0 {
			continue
		}
		stored, exists := known[segments[i].SegmentID]
		if segments[i].ManifestVersion == 0 {
			if exists {
				segments[i].ManifestVersion = stored
			}
			continue
		}
		if exists && segments[i].ManifestVersion < stored {
			return merr.WrapErrDataIntegrityMsg(
				"Segment %d Manifest version cannot regress from %d to %d",
				segments[i].SegmentID,
				stored,
				segments[i].ManifestVersion,
			)
		}
	}
	return addSegments(view, segments)
}

func validatePersistedSegmentManifestVersions(view *viewpb.DataViewOfCollection) error {
	for _, shard := range view.GetShards() {
		for _, partition := range shard.GetPartitions() {
			versions := partition.GetSegmentManifestVersions()
			if len(versions) != 0 && len(versions) != len(partition.GetSegmentIds()) {
				return merr.WrapErrDataIntegrityMsg(
					"persisted DataView has misaligned Segment arrays: collection=%d vchannel=%q partition=%d segments=%d manifestVersions=%d",
					view.GetCollectionId(),
					shard.GetVchannel(),
					partition.GetPartitionId(),
					len(partition.GetSegmentIds()),
					len(versions),
				)
			}
			for _, version := range versions {
				if version < 0 {
					return merr.WrapErrDataIntegrityMsg(
						"persisted DataView has negative Manifest version: collection=%d segmentManifestVersion=%d",
						view.GetCollectionId(),
						version,
					)
				}
			}
		}
	}
	return nil
}

type segmentLocation struct {
	vchannel    string
	partitionID int64
}

type segmentSlot struct {
	location  segmentLocation
	partition *viewpb.DataViewOfPartition
	index     int
}

func dataViewSegmentSlots(view *viewpb.DataViewOfCollection) map[int64]segmentSlot {
	slots := make(map[int64]segmentSlot)
	for _, shard := range view.GetShards() {
		for _, partition := range shard.GetPartitions() {
			for idx, segmentID := range partition.GetSegmentIds() {
				slots[segmentID] = segmentSlot{
					location:  segmentLocation{vchannel: shard.GetVchannel(), partitionID: partition.GetPartitionId()},
					partition: partition,
					index:     idx,
				}
			}
		}
	}
	return slots
}

func findOrCreateShard(view *viewpb.DataViewOfCollection, vchannel string) *viewpb.DataViewOfShard {
	for _, shard := range view.GetShards() {
		if shard.GetVchannel() == vchannel {
			return shard
		}
	}
	shard := &viewpb.DataViewOfShard{Vchannel: vchannel}
	view.Shards = append(view.Shards, shard)
	return shard
}

func findOrCreatePartition(shard *viewpb.DataViewOfShard, partitionID int64) *viewpb.DataViewOfPartition {
	for _, partition := range shard.GetPartitions() {
		if partition.GetPartitionId() == partitionID {
			return partition
		}
	}
	partition := &viewpb.DataViewOfPartition{PartitionId: partitionID}
	shard.Partitions = append(shard.Partitions, partition)
	return partition
}

func buildEmptyDataView(collectionID int64, vchannels []string) *viewpb.DataViewOfCollection {
	view := &viewpb.DataViewOfCollection{CollectionId: collectionID}
	seen := make(map[string]struct{}, len(vchannels))
	for _, vchannel := range vchannels {
		if vchannel == "" {
			continue
		}
		if _, ok := seen[vchannel]; ok {
			continue
		}
		seen[vchannel] = struct{}{}
		view.Shards = append(view.Shards, &viewpb.DataViewOfShard{Vchannel: vchannel})
	}
	canonicalizeDataView(view)
	return view
}

func canonicalDataViewClone(view *viewpb.DataViewOfCollection) *viewpb.DataViewOfCollection {
	if view == nil {
		return nil
	}
	clone := proto.Clone(view).(*viewpb.DataViewOfCollection)
	canonicalizeDataView(clone)
	return clone
}

func canonicalizeDataView(view *viewpb.DataViewOfCollection) {
	if view == nil {
		return
	}
	sort.Slice(view.Shards, func(i, j int) bool {
		return view.Shards[i].GetVchannel() < view.Shards[j].GetVchannel()
	})
	for _, shard := range view.GetShards() {
		sort.Slice(shard.Partitions, func(i, j int) bool {
			return shard.Partitions[i].GetPartitionId() < shard.Partitions[j].GetPartitionId()
		})
		for _, partition := range shard.GetPartitions() {
			canonicalizePartitionSegments(partition)
		}
	}
}

type canonicalSegment struct {
	id              int64
	manifestVersion int64
}

func canonicalizePartitionSegments(partition *viewpb.DataViewOfPartition) {
	segments := make([]canonicalSegment, len(partition.GetSegmentIds()))
	versions := partition.GetSegmentManifestVersions()
	for idx, segmentID := range partition.GetSegmentIds() {
		segments[idx].id = segmentID
		if idx < len(versions) {
			segments[idx].manifestVersion = versions[idx]
		}
	}
	sort.Slice(segments, func(i, j int) bool {
		return segments[i].id < segments[j].id
	})

	partition.SegmentIds = make([]int64, 0, len(segments))
	partition.SegmentManifestVersions = make([]int64, 0, len(segments))
	for _, segment := range segments {
		last := len(partition.SegmentIds) - 1
		if last >= 0 && partition.SegmentIds[last] == segment.id {
			if segment.manifestVersion > partition.SegmentManifestVersions[last] {
				partition.SegmentManifestVersions[last] = segment.manifestVersion
			}
			continue
		}
		partition.SegmentIds = append(partition.SegmentIds, segment.id)
		partition.SegmentManifestVersions = append(partition.SegmentManifestVersions, segment.manifestVersion)
	}
}

func dataViewMembershipEqual(left, right *viewpb.DataViewOfCollection) bool {
	left = canonicalDataViewClone(left)
	right = canonicalDataViewClone(right)
	if left != nil {
		left.DataVersion = nil
	}
	if right != nil {
		right.DataVersion = nil
	}
	return proto.Equal(left, right)
}

func dataVersionFromView(view *viewpb.DataViewOfCollection) *viewpb.DataVersion {
	if view == nil {
		return nil
	}
	return cloneDataVersion(view.GetDataVersion())
}

func cloneDataVersion(version *viewpb.DataVersion) *viewpb.DataVersion {
	if version == nil {
		return nil
	}
	return proto.Clone(version).(*viewpb.DataVersion)
}

func compareDataVersion(left, right *viewpb.DataVersion) int {
	leftStreaming, leftCompact := int64(0), int64(0)
	if left != nil {
		leftStreaming = left.GetStreamingVersion()
		leftCompact = left.GetCompactVersion()
	}
	rightStreaming, rightCompact := int64(0), int64(0)
	if right != nil {
		rightStreaming = right.GetStreamingVersion()
		rightCompact = right.GetCompactVersion()
	}
	if leftStreaming != rightStreaming {
		if leftStreaming > rightStreaming {
			return 1
		}
		return -1
	}
	if leftCompact != rightCompact {
		if leftCompact > rightCompact {
			return 1
		}
		return -1
	}
	return 0
}

// protoVersionToStruct converts a proto DataVersion to the struct key used by
// collectionState.versions. nil maps to the zero struct.
func protoVersionToStruct(v *viewpb.DataVersion) qviews.DataVersion {
	if v == nil {
		return qviews.DataVersion{}
	}
	return qviews.DataVersion{
		StreamingVersion: v.GetStreamingVersion(),
		CompactVersion:   v.GetCompactVersion(),
	}
}

// buildSegmentRowStats extracts the per-segment footprint from a projection.
// It feeds versionEntry.stats at every publication point so a DataViewRef can
// answer Stats for the exact version it references.
func buildSegmentRowStats(segments []LoadableSegment) map[int64]SegmentStats {
	stats := make(map[int64]SegmentStats, len(segments))
	for _, seg := range segments {
		if seg.SegmentID != 0 {
			stats[seg.SegmentID] = SegmentStats{RowNum: seg.RowNum}
		}
	}
	return stats
}

func dataVersionKey(version *viewpb.DataVersion) string {
	if version == nil {
		return "0/0"
	}
	return fmt.Sprintf(
		"%d/%d",
		version.GetStreamingVersion(),
		version.GetCompactVersion(),
	)
}

// RegisterDataViewListener serializes initial replay with all publications.
// The callback receives immutable values and must not re-enter this manager.
func (m *dataViewManager) RegisterDataViewListener(listener api.DataViewListener) func() {
	m.publicationMu.Lock()
	if m.listeners == nil {
		m.listeners = make(map[uint64]api.DataViewListener)
	}
	m.nextListener++
	id := m.nextListener
	m.listeners[id] = listener
	for collectionID, view := range m.published {
		listener(collectionID, view)
	}
	m.publicationMu.Unlock()
	return func() { m.publicationMu.Lock(); delete(m.listeners, id); m.publicationMu.Unlock() }
}

// Caller holds state.mu. Publication never acquires another collection lock.
func (m *dataViewManager) publishDataViewLocked(state *collectionState) {
	entry := state.latest
	if entry == nil {
		return
	}
	if state.publishedEntry != entry {
		view := m.collectionDataViewLocked(state)
		if view == nil {
			return
		}
		state.native = api.PrepareCollectionDataView(view)
		state.publishedEntry = entry
	}
	m.publicationMu.Lock()
	defer m.publicationMu.Unlock()
	// Drop removes source membership before waiting for state.mu. Revalidate
	// inside the publication critical section so a late commit cannot resurrect it.
	m.mu.RLock()
	current := m.states[state.id] == state
	m.mu.RUnlock()
	if !current || m.published[state.id] == state.native {
		return
	}
	if m.published == nil {
		m.published = make(map[int64]*api.CollectionDataView)
	}
	m.published[state.id] = state.native
	for _, listener := range m.listeners {
		listener(state.id, state.native)
	}
}

func (m *dataViewManager) publishDataViewDrop(collectionID int64) {
	m.publicationMu.Lock()
	defer m.publicationMu.Unlock()
	delete(m.published, collectionID)
	for _, listener := range m.listeners {
		listener(collectionID, nil)
	}
}
