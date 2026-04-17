package datacoord

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// Snapshot Metadata Management
//
// This file implements the metadata layer for collection snapshot management in DataCoord.
// Snapshots are point-in-time captures of collection state used for backup and restore operations.
//
// ARCHITECTURE:
// - snapshotMeta: Central metadata manager coordinating catalog persistence and S3 storage
// - snapshotID2Info: In-memory cache of snapshot metadata from etcd (immediately available)
// - snapshotID2RefIndex: Segment/index references from S3 (async loaded with blocking access)
// - Catalog: Persistent storage of snapshot metadata in etcd/KV store
// - Reader/Writer: S3 operations for reading/writing complete snapshot data
//
// METADATA ORGANIZATION:
// - Memory: Two maps - snapshotID2Info (etcd data) and snapshotID2RefIndex (S3 data)
// - Catalog (etcd): Collection snapshots metadata (name, ID, collection, partitions, S3 location)
// - S3: Complete snapshot data (segments, indexes, schema) at S3Location path
//
// LIFECYCLE:
// 1. Create: Save complete snapshot data to S3, then save metadata to catalog
// 2. Read: Get snapshot info from memory, read complete data from S3 if needed
// 3. List: Query memory cache with optional collection/partition filtering
// 4. Drop: Remove from memory, delete from catalog, delete from S3
// 5. Reload: Restore memory cache from catalog on DataCoord restart
//
// REFERENCE TRACKING:
// - Track which snapshots reference each segment/index ID
// - Enable safe GC by preventing deletion of segments/indexes in active snapshots
// - Support queries like "which snapshots contain this segment?"
//
// CONCURRENCY:
// - Thread-safe memory cache using ConcurrentMap
// - Catalog operations are atomic via etcd transactions
// - S3 operations use unique paths (collection_id/snapshot_id) to prevent conflicts

// RefIndexLoadState represents the loading state of a SnapshotRefIndex.
type RefIndexLoadState int

const (
	// RefIndexStatePending: Data not yet loaded from S3.
	RefIndexStatePending RefIndexLoadState = iota
	// RefIndexStateLoaded: Data successfully loaded from S3.
	RefIndexStateLoaded
	// RefIndexStateFailed: Loading from S3 failed.
	// The background loader will retry loading periodically.
	RefIndexStateFailed
)

// SnapshotRefIndex holds segment and build IDs loaded from S3.
// Loading state is managed per-snapshot to support retry on failure.
//
// CONCURRENCY: Protected by RWMutex since the async loader writes via SetLoaded/SetFailed
// while snapshotMeta.rebuildAllSegmentProtection reads via GetSegmentIDs/GetBuildIDs.
type SnapshotRefIndex struct {
	mu         sync.RWMutex
	loadState  RefIndexLoadState
	segmentIDs typeutil.UniqueSet
	buildIDs   typeutil.UniqueSet
}

// NewSnapshotRefIndex creates a new empty SnapshotRefIndex.
// Used during reload when data needs to be loaded from S3 asynchronously.
func NewSnapshotRefIndex() *SnapshotRefIndex {
	return &SnapshotRefIndex{}
}

// NewLoadedSnapshotRefIndex creates a SnapshotRefIndex with pre-loaded data.
// Used for newly created snapshots where data is already available.
func NewLoadedSnapshotRefIndex(segmentIDs, buildIDs []int64) *SnapshotRefIndex {
	return &SnapshotRefIndex{
		loadState:  RefIndexStateLoaded,
		segmentIDs: typeutil.NewUniqueSet(segmentIDs...),
		buildIDs:   typeutil.NewUniqueSet(buildIDs...),
	}
}

// SetLoaded sets the segment and build IDs and marks the RefIndex as loaded.
// This is called by the async loader after reading data from S3.
func (r *SnapshotRefIndex) SetLoaded(segmentIDs, buildIDs []int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.loadState = RefIndexStateLoaded
	r.segmentIDs = typeutil.NewUniqueSet(segmentIDs...)
	r.buildIDs = typeutil.NewUniqueSet(buildIDs...)
}

// ContainsSegment checks if segment exists.
// Used by GC to check if a segment is referenced by this snapshot.
func (r *SnapshotRefIndex) ContainsSegment(segmentID UniqueID) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.segmentIDs != nil && r.segmentIDs.Contain(segmentID)
}

// ContainsBuildID checks if build ID exists.
// Used by GC to check if a specific index build is referenced by this snapshot.
func (r *SnapshotRefIndex) ContainsBuildID(buildID UniqueID) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.buildIDs != nil && r.buildIDs.Contain(buildID)
}

// SetFailed marks the RefIndex as failed to load from S3.
// GC should skip this snapshot, and the background loader will retry loading periodically.
func (r *SnapshotRefIndex) SetFailed() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.loadState = RefIndexStateFailed
}

// IsLoaded returns true if data was successfully loaded from S3.
func (r *SnapshotRefIndex) IsLoaded() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.loadState == RefIndexStateLoaded
}

// IsFailed returns true if loading from S3 failed.
func (r *SnapshotRefIndex) IsFailed() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.loadState == RefIndexStateFailed
}

// GetSegmentIDs returns a copy of segment IDs referenced by this snapshot.
func (r *SnapshotRefIndex) GetSegmentIDs() []int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.segmentIDs == nil {
		return nil
	}
	return r.segmentIDs.Collect()
}

// GetBuildIDs returns a copy of the index buildIDs referenced by this snapshot.
// Used by rebuildAllSegmentProtection to populate buildIDReferencedByGC.
func (r *SnapshotRefIndex) GetBuildIDs() []int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.buildIDs == nil {
		return nil
	}
	return r.buildIDs.Collect()
}

// snapshotMeta manages snapshot metadata both in memory and persistent storage.
//
// This is the central coordinator for snapshot operations, maintaining:
//   - In-memory cache for fast lookup and reference tracking
//   - Catalog persistence for durability across restarts
//   - S3 reader/writer for complete snapshot data
//
// Data is split into two separate maps:
//   - snapshotID2Info: snapshot metadata from etcd (immediately available)
//   - snapshotID2RefIndex: segment/index references from S3 (async loaded)
type snapshotMeta struct {
	catalog metastore.DataCoordCatalog // Persistent storage in etcd/KV store for snapshot metadata

	// Primary indexes: snapshot ID -> data
	// Split into two maps for different data sources and availability
	snapshotID2Info     *typeutil.ConcurrentMap[UniqueID, *datapb.SnapshotInfo] // From etcd, immediately available
	snapshotID2RefIndex *typeutil.ConcurrentMap[UniqueID, *SnapshotRefIndex]    // From S3, async loaded

	// Secondary indexes for O(1) lookup performance
	// snapshotName2ID: collection ID -> (snapshot name -> snapshot ID) mapping for fast per-collection name-based queries
	snapshotName2ID *typeutil.ConcurrentMap[UniqueID, *typeutil.ConcurrentMap[string, UniqueID]]
	// collectionID2Snapshots: collection ID -> set of snapshot IDs for fast collection-based listing
	collectionID2Snapshots *typeutil.ConcurrentMap[UniqueID, typeutil.UniqueSet]
	// collectionIndexMu protects read-modify-write operations on collectionID2Snapshots
	// since UniqueSet (map) is not thread-safe for concurrent modifications.
	// Uses RWMutex to allow concurrent reads (ListSnapshots) while blocking writes.
	collectionIndexMu sync.RWMutex

	// Protection state, maintained by rebuildAllSegmentProtection under segmentProtectionMu.
	//
	// Compaction and GC share the same fail-closed → converge pattern (coarse collection-level
	// block at startup, progressively narrowed to precise segment/buildID sets as the loader
	// goroutine reads RefIndexes from S3). They differ in TTL semantics:
	//
	//   - Compaction protection is TTL-bound: only snapshots whose CompactionExpireTime is
	//     still in the future contribute. Once TTL expires, the segment is eligible for
	//     compaction again even if the snapshot still exists.
	//
	//   - GC protection is unconditional: any existing snapshot's referenced segments and
	//     buildIDs must not be deleted, because the snapshot needs them for PIT recovery
	//     regardless of CompactionExpireTime.
	//
	// These are therefore maintained as two independent sets of precise state under the
	// same lock, rebuilt atomically by rebuildAllSegmentProtection.
	segmentProtectionMu sync.RWMutex
	// ===== Compaction protection (TTL-bound) =====
	// segmentProtectionUntil: segmentID -> latest active protection expiry (Unix seconds).
	// When multiple active-TTL snapshots reference the same segment, the latest expiry wins.
	segmentProtectionUntil map[int64]uint64
	// compactionBlockedCollections: collections with active compaction-protected snapshots
	// whose RefIndex hasn't been loaded yet. Fail-closed coarse block.
	compactionBlockedCollections typeutil.UniqueSet
	// ===== GC protection (unconditional on TTL) =====
	// gcBlockedCollections: collections with any snapshot whose RefIndex hasn't been loaded.
	// Fail-closed coarse block for GC queries: we cannot know which segments/buildIDs an
	// unloaded RefIndex references, so every candidate in the collection must be kept.
	gcBlockedCollections typeutil.UniqueSet
	// segmentReferencedByGC: set of segment IDs referenced by at least one loaded snapshot.
	// Precise-grained GC protection: once we know a snapshot's referenced segments, we can
	// point-query this set instead of scanning all snapshots per candidate.
	segmentReferencedByGC typeutil.UniqueSet
	// buildIDReferencedByGC: set of index buildIDs referenced by at least one loaded snapshot.
	buildIDReferencedByGC typeutil.UniqueSet

	// snapshotPendingCollections: collections currently in the process of creating a snapshot.
	// Blocks compaction commit for these collections to prevent segment state changes
	// between GenSnapshot (segment list capture) and SaveSnapshot (protection setup).
	// This is independent of the rebuild pattern above — it is a short-lived flag set by
	// CreateSnapshot and cleared by its defer.
	snapshotPendingCollections typeutil.UniqueSet

	// pinMu protects the snapshotID2Info pointer swap and pinID2SnapshotID index.
	// Critical sections are short — no etcd IO is done under this lock. Actual
	// serialization of concurrent writers on the same snapshot is done via
	// per-snapshot locks from pinLocks (see getPinLock). Readers (HasActivePins)
	// take the read lock to read the swap target; they see either the pre- or
	// post-swap pointer but never a torn state because the pointer is swapped
	// atomically under the write lock.
	pinMu sync.RWMutex

	// pinLocks serializes concurrent Pin/Unpin/cleanExpired writers that target
	// the same snapshot. Each entry is lazily created via getPinLock and removed
	// when the snapshot is dropped. A per-snapshot lock lets etcd IO on snapshot
	// A run in parallel with etcd IO on snapshot B, which pinMu alone (a global
	// lock previously held during etcd IO) could not.
	pinLocks *typeutil.ConcurrentMap[UniqueID, *sync.Mutex]

	// pinID2SnapshotID is a reverse index for O(1) Unpin lookup. Maintained
	// alongside SnapshotInfo.PinIds under pinMu. Populated on reload from
	// persisted PinIds, and updated on every Pin/Unpin/Drop/cleanExpired.
	pinID2SnapshotID *typeutil.ConcurrentMap[int64, UniqueID]

	// pinMapsInit guards lazy initialization of pinLocks/pinID2SnapshotID so
	// tests that construct snapshotMeta literally (without newSnapshotMeta)
	// can still exercise Pin/Unpin without panicking on nil maps.
	pinMapsInit sync.Once

	// Background RefIndex loader goroutine control
	loaderCtx    context.Context
	loaderCancel context.CancelFunc
	loaderWg     sync.WaitGroup

	reader *SnapshotReader // Reads complete snapshot data from S3
	writer *SnapshotWriter // Writes complete snapshot data to S3
}

// newSnapshotMeta creates a new snapshot metadata manager and initializes it from catalog.
//
// This is called during DataCoord startup to restore snapshot metadata state.
//
// Process flow:
//  1. Create snapshotMeta instance with catalog and S3 reader/writer
//  2. Initialize empty in-memory cache
//  3. Reload all snapshots from catalog to populate cache
//  4. Verify snapshot data integrity by reading from S3
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - catalog: DataCoord catalog for persistent metadata storage
//   - chunkManager: S3/storage manager for reading/writing snapshot data
//
// Returns:
//   - *snapshotMeta: Initialized snapshot manager with populated cache
//   - error: Error if catalog reload or S3 verification fails
func newSnapshotMeta(ctx context.Context, catalog metastore.DataCoordCatalog, chunkManager storage.ChunkManager) (*snapshotMeta, error) {
	loaderCtx, loaderCancel := context.WithCancel(context.Background())
	sm := &snapshotMeta{
		catalog:                      catalog,
		snapshotID2Info:              typeutil.NewConcurrentMap[UniqueID, *datapb.SnapshotInfo](),
		snapshotID2RefIndex:          typeutil.NewConcurrentMap[UniqueID, *SnapshotRefIndex](),
		snapshotName2ID:              typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[string, UniqueID]](),
		collectionID2Snapshots:       typeutil.NewConcurrentMap[UniqueID, typeutil.UniqueSet](),
		segmentProtectionUntil:       make(map[int64]uint64),
		compactionBlockedCollections: typeutil.NewUniqueSet(),
		gcBlockedCollections:         typeutil.NewUniqueSet(),
		segmentReferencedByGC:        typeutil.NewUniqueSet(),
		buildIDReferencedByGC:        typeutil.NewUniqueSet(),
		snapshotPendingCollections:   typeutil.NewUniqueSet(),
		pinLocks:                     typeutil.NewConcurrentMap[UniqueID, *sync.Mutex](),
		pinID2SnapshotID:             typeutil.NewConcurrentMap[int64, UniqueID](),
		loaderCtx:                    loaderCtx,
		loaderCancel:                 loaderCancel,
		reader:                       NewSnapshotReader(chunkManager),
		writer:                       NewSnapshotWriter(chunkManager),
	}

	// Reload all snapshots from catalog to populate in-memory cache
	if err := sm.reload(ctx); err != nil {
		loaderCancel()
		log.Error("failed to reload snapshot meta from kv", zap.Error(err))
		return nil, err
	}

	// Synchronously populate compaction protection state before exposing snapshotMeta
	// to the rest of DataCoord. This is a pure in-memory operation:
	//
	// At this point every RefIndex is in Pending state (freshly created by reload()),
	// so rebuildAllSegmentProtection will conservatively imprint a collection-level
	// block for every snapshot with an active CompactionExpireTime (fail-closed).
	// Segment-level precision is not required on startup — the coarser collection-level
	// block is a strict superset of the correct segment-level protection.
	//
	// The background loader goroutine below will then read RefIndexes from S3 and
	// progressively transition each collection from coarse collection-level block to
	// precise segment-level protection. This keeps startup O(N) in-memory instead of
	// O(N × S3_RTT) I/O-bound, while preserving fail-closed semantics.
	sm.rebuildAllSegmentProtection()

	// Start background RefIndex loader goroutine. It runs an immediate first load
	// (to narrow the coarse collection-level blocks to precise segment-level protection
	// as soon as possible) and then periodically retries any RefIndex still in Failed state.
	sm.loaderWg.Add(1)
	go sm.refIndexLoaderLoop()

	return sm, nil
}

// reload rebuilds the in-memory cache from catalog during DataCoord startup.
//
// This function is critical for recovering snapshot state after DataCoord restarts.
// It reads snapshot metadata from catalog (etcd) and builds the in-memory cache placeholders.
// The actual segment/index ID sets are loaded by the background refIndexLoaderLoop.
//
// Process flow:
//  1. List all snapshots from catalog (persistent storage)
//  2. For each committed snapshot:
//     a. Insert placeholder into in-memory cache (without S3 data)
//     b. Build secondary indexes
//
// This design allows reload() to return quickly without waiting for S3 I/O,
// significantly improving DataCoord startup time when there are many snapshots.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//
// Returns:
//   - error: Error if catalog list fails
func (sm *snapshotMeta) reload(ctx context.Context) error {
	snapshots, err := sm.catalog.ListSnapshots(ctx)
	if err != nil {
		log.Info("failed to list snapshots from kv", zap.Error(err))
		return err
	}

	for _, snapshot := range snapshots {
		if snapshot.GetState() == datapb.SnapshotState_SnapshotStatePending ||
			snapshot.GetState() == datapb.SnapshotState_SnapshotStateDeleting {
			log.Warn("skipping snapshot during reload, will be cleaned by GC",
				zap.String("name", snapshot.GetName()),
				zap.Int64("id", snapshot.GetId()),
				zap.String("state", snapshot.GetState().String()))
			continue
		}

		// Insert snapshot info (immediately available from etcd)
		sm.snapshotID2Info.Insert(snapshot.GetId(), snapshot)

		// Create pending RefIndex (will be loaded by background goroutine)
		sm.snapshotID2RefIndex.Insert(snapshot.GetId(), NewSnapshotRefIndex())

		// Build secondary indexes for O(1) lookup
		sm.addToSecondaryIndexes(snapshot)

		// Rebuild pin reverse index from persisted PinIds so Unpin lookups
		// are O(1) after restart.
		for _, pid := range snapshot.GetPinIds() {
			sm.pinID2SnapshotID.Insert(pid, snapshot.GetId())
		}

		log.Info("loaded snapshot metadata from catalog",
			zap.String("name", snapshot.GetName()),
			zap.Int64("id", snapshot.GetId()))
	}

	return nil
}

// refIndexLoaderLoop loads unloaded RefIndexes in the background.
//
// newSnapshotMeta imprints a coarse collection-level block for every protected
// snapshot before returning (fail-closed, no I/O). This loop is responsible for
// converging from that coarse state to precise segment-level protection:
//   - first tick: runs immediately, loads RefIndexes from S3, narrows the blocks
//   - subsequent ticks: periodically retry any RefIndex still in Failed state
//
// It runs until loaderCtx is canceled.
func (sm *snapshotMeta) refIndexLoaderLoop() {
	defer sm.loaderWg.Done()

	// Note: SnapshotRefIndexLoadInterval is refreshable.
	// Re-read it after each tick to apply updates without restarting DataCoord.
	getInterval := func() time.Duration {
		interval := paramtable.Get().DataCoordCfg.SnapshotRefIndexLoadInterval.GetAsDurationByParse()
		if interval <= 0 {
			log.Warn("invalid snapshot RefIndex load interval, fallback to 60s",
				zap.Duration("interval", interval))
			return 60 * time.Second
		}
		return interval
	}

	runOnce := func() {
		if sm.loadUnloadedRefIndexes() {
			sm.rebuildAllSegmentProtection()
		}
	}

	// Run immediately on startup so convergence from coarse collection-level blocks
	// to precise segment-level protection does not wait a full interval.
	runOnce()

	timer := time.NewTimer(getInterval())
	defer timer.Stop()

	for {
		select {
		case <-sm.loaderCtx.Done():
			log.Info("RefIndex loader goroutine stopped")
			return
		case <-timer.C:
			runOnce()
			// Reset using the latest refreshable interval.
			timer.Reset(getInterval())
		}
	}
}

// loadUnloadedRefIndexes loads all RefIndexes that are Pending or Failed.
// Returns true if any RefIndex state changed (loaded or failed), indicating
// that callers should rebuild dependent state like segment protection.
//
// Each ReadSnapshot call is bounded by a per-call timeout (dataCoord.snapshot.refIndexLoadTimeout)
// derived from sm.loaderCtx. WITHOUT this timeout, a single hung S3 read would block
// the entire Range, no other RefIndex would ever be loaded, rebuildAllSegmentProtection
// would never be triggered, and every collection with a snapshot would stay in the
// startup fail-closed coarse block — leaking storage on every collection with snapshots
// until DataCoord restarts. On timeout the RefIndex is marked Failed and will be
// retried on the next loader tick.
func (sm *snapshotMeta) loadUnloadedRefIndexes() bool {
	changed := false
	timeout := paramtable.Get().DataCoordCfg.SnapshotRefIndexLoadTimeout.GetAsDurationByParse()
	sm.snapshotID2RefIndex.Range(func(id UniqueID, refIndex *SnapshotRefIndex) bool {
		if refIndex.IsLoaded() {
			return true // Already loaded, skip
		}

		info, exists := sm.snapshotID2Info.Get(id)
		if !exists {
			return true // Snapshot deleted
		}

		readCtx, cancel := context.WithTimeout(sm.loaderCtx, timeout)
		snapshotData, err := sm.reader.ReadSnapshot(readCtx, info.GetS3Location(), false)
		cancel()
		if err != nil {
			log.Warn("failed to load RefIndex from S3",
				zap.String("name", info.GetName()),
				zap.Int64("id", id),
				zap.Duration("timeout", timeout),
				zap.Error(err))
			refIndex.SetFailed()
		} else {
			refIndex.SetLoaded(snapshotData.SegmentIDs, snapshotData.BuildIDs)
			log.Info("loaded RefIndex from S3",
				zap.String("name", info.GetName()),
				zap.Int64("id", id))
		}
		changed = true

		return true
	})
	return changed
}

// Close stops the background RefIndex loader goroutine.
// Should be called when snapshotMeta is no longer needed.
func (sm *snapshotMeta) Close() {
	if sm.loaderCancel != nil {
		sm.loaderCancel()
		sm.loaderWg.Wait()
	}
}

// SaveSnapshot persists a new snapshot to both S3 and catalog using 2PC (Two-Phase Commit).
//
// This is the main entry point for creating a new snapshot. It uses a two-phase commit
// approach to ensure atomic creation and enable GC cleanup of orphan files.
//
// Process flow (2PC):
//  1. Phase 1 (Prepare): Save PENDING state to catalog (snapshot ID is used for file paths)
//  2. Write complete snapshot data to S3 (each segment to separate manifest file)
//  3. Phase 2 (Commit): Update catalog to COMMITTED state
//  4. Insert into in-memory cache
//
// Failure handling:
//   - If Phase 1 fails: No changes made, return error
//   - If S3 write fails: PENDING record remains in catalog, GC will cleanup S3 files
//   - If Phase 2 fails: PENDING record remains in catalog, GC will cleanup S3 files
//   - GC uses snapshot ID to compute and delete orphan S3 files without S3 list operations
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - snapshot: Complete snapshot data including segments, indexes, and schema
//
// Returns:
//   - error: Error if any phase fails
func (sm *snapshotMeta) SaveSnapshot(ctx context.Context, snapshot *SnapshotData) error {
	log := log.Ctx(ctx).With(
		zap.String("snapshotName", snapshot.SnapshotInfo.GetName()),
		zap.Int64("snapshotID", snapshot.SnapshotInfo.GetId()),
		zap.Int64("collectionID", snapshot.SnapshotInfo.GetCollectionId()),
	)

	// Step 1: Extract segment IDs and build IDs for reference tracking
	// Also populate SnapshotData fields so they are persisted to S3 metadata.json.
	// Without this, after restart the RefIndex loaded from S3 would have nil segmentIDs,
	// causing GC to skip snapshot protection and delete referenced files.
	segmentIDs := make([]int64, 0, len(snapshot.Segments))
	buildIDs := make([]int64, 0)
	for _, segment := range snapshot.Segments {
		segmentIDs = append(segmentIDs, segment.GetSegmentId())
		for _, indexFile := range segment.GetIndexFiles() {
			buildIDs = append(buildIDs, indexFile.GetBuildID())
		}
		for _, textIndex := range segment.GetTextIndexFiles() {
			if textIndex.GetBuildID() != 0 {
				buildIDs = append(buildIDs, textIndex.GetBuildID())
			}
		}
		for _, jsonKeyIndex := range segment.GetJsonKeyIndexFiles() {
			if jsonKeyIndex.GetBuildID() != 0 {
				buildIDs = append(buildIDs, jsonKeyIndex.GetBuildID())
			}
		}
	}
	snapshot.SegmentIDs = segmentIDs
	snapshot.BuildIDs = buildIDs

	// Step 2: Phase 1 (Prepare) - Save PENDING state to catalog
	// This enables GC to cleanup orphan S3 files if subsequent steps fail
	// Snapshot ID is used for computing S3 file paths
	snapshot.SnapshotInfo.State = datapb.SnapshotState_SnapshotStatePending
	snapshot.SnapshotInfo.PendingStartTime = time.Now().UnixMilli()

	if err := sm.catalog.SaveSnapshot(ctx, snapshot.SnapshotInfo); err != nil {
		log.Error("failed to save pending snapshot to catalog", zap.Error(err))
		return fmt.Errorf("failed to save pending snapshot to catalog: %w", err)
	}
	log.Info("saved pending snapshot to catalog")

	// Step 3: Write S3 files using snapshot ID for path computation
	metadataFilePath, err := sm.writer.Save(ctx, snapshot)
	if err != nil {
		// S3 write failed, leave PENDING record for GC to clean up
		log.Error("failed to save snapshot to S3, pending record left for GC cleanup",
			zap.Error(err))
		return fmt.Errorf("failed to save snapshot to S3: %w", err)
	}
	snapshot.SnapshotInfo.S3Location = metadataFilePath
	log.Info("saved snapshot data to S3", zap.String("s3Location", metadataFilePath))

	// Step 4: Phase 2 (Commit) - Update catalog to COMMITTED state
	snapshot.SnapshotInfo.State = datapb.SnapshotState_SnapshotStateCommitted

	// Update catalog first (catalog is source of truth). Memory cache and
	// protection state are only populated after the catalog write succeeds, so
	// a failed commit leaves no in-memory residue to roll back.
	if err := sm.catalog.SaveSnapshot(ctx, snapshot.SnapshotInfo); err != nil {
		// Phase 2 failed, but S3 data and PENDING record are already written.
		// GC will eventually cleanup via the PENDING marker.
		log.Error("failed to update snapshot to committed state, pending record left for GC cleanup",
			zap.Error(err))
		return fmt.Errorf("failed to update snapshot to committed state: %w", err)
	}

	// Catalog committed — safe to insert into in-memory cache and register protection.
	sm.snapshotID2Info.Insert(snapshot.SnapshotInfo.GetId(), snapshot.SnapshotInfo)
	sm.snapshotID2RefIndex.Insert(snapshot.SnapshotInfo.GetId(),
		NewLoadedSnapshotRefIndex(segmentIDs, buildIDs))
	sm.addToSecondaryIndexes(snapshot.SnapshotInfo)

	// Register both compaction protection (TTL-bound) and GC protection (unconditional)
	// for the referenced segments/buildIDs. Must run for ANY snapshot, including those
	// with CompactionExpireTime==0, because GC protection has no TTL — otherwise the
	// freshly-created snapshot's referenced files could be deleted by GC.
	sm.registerSnapshotProtection(snapshot.SnapshotInfo, segmentIDs, buildIDs)

	log.Info("snapshot saved successfully with 2PC",
		zap.String("s3Location", metadataFilePath),
		zap.Int("numSegments", len(snapshot.Segments)),
		zap.Int("numIndexes", len(snapshot.Indexes)))

	return nil
}

// DropSnapshot removes a snapshot from memory, catalog, and S3.
//
// This implements a two-phase delete to prevent orphaned S3 files:
//  1. Mark snapshot as Deleting in catalog (persistent, survives restart)
//  2. Remove from in-memory cache (user immediately sees deletion)
//  3. Delete S3 data (may fail, GC will retry)
//  4. Delete catalog record (final cleanup)
//
// If S3 deletion fails, the function returns success. The snapshot is already
// invisible to users (removed from memory), and GC will clean up the S3 data
// by finding snapshots in Deleting state.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - collectionID: Collection ID to scope the snapshot lookup
//   - snapshotName: Name of the snapshot to delete (unique within collection)
//
// Returns:
//   - error: Error if snapshot not found or catalog update fails
func (sm *snapshotMeta) DropSnapshot(ctx context.Context, collectionID int64, snapshotName string) error {
	log := log.Ctx(ctx).With(zap.String("snapshotName", snapshotName), zap.Int64("collectionID", collectionID))

	// Step 1: Lookup snapshot by name within collection
	snapshot, err := sm.getSnapshotByName(ctx, collectionID, snapshotName)
	if err != nil {
		log.Error("failed to get snapshot by name", zap.Error(err))
		return err
	}

	sm.ensurePinMaps()

	// Serialize against concurrent Pin/Unpin/cleanExpired on the same snapshot
	// via the per-snapshot lock. Pin readers (HasActivePins) take pinMu.RLock
	// and see either the pre- or post-swap pointer atomically.
	lock := sm.getPinLock(snapshot.GetId())
	lock.Lock()
	defer lock.Unlock()

	// Re-read under per-snapshot lock in case a concurrent writer just swapped.
	sm.pinMu.RLock()
	current, ok := sm.snapshotID2Info.Get(snapshot.GetId())
	sm.pinMu.RUnlock()
	if !ok {
		// Already dropped.
		return nil
	}

	// Reject if any pin is still active.
	if activePins := countActivePins(current); activePins > 0 {
		return merr.WrapErrSnapshotPinned(snapshotName,
			fmt.Sprintf("%d active pins, unpin before dropping", activePins))
	}

	// Clone-and-swap: drop expired pins AND mark Deleting in a single
	// persisted state so the in-memory view can never disagree with etcd
	// during the IO window.
	updated := cloneSnapshotInfo(current)
	expiredPinIDs := updated.PinIds // all are expired — activePins == 0 above
	updated.PinIds = nil
	updated.PinExpireAtMs = nil
	updated.State = datapb.SnapshotState_SnapshotStateDeleting

	if err := sm.catalog.SaveSnapshot(ctx, updated); err != nil {
		log.Error("failed to mark snapshot as deleting", zap.Error(err))
		return err
	}

	// Swap and drop reverse-index entries for the cleared expired pins.
	sm.pinMu.Lock()
	sm.swapSnapshotInfoLocked(current, updated)
	sm.pinMu.Unlock()

	// Step 3: Remove from in-memory cache and secondary indexes (user immediately sees deletion)
	sm.snapshotID2Info.Remove(updated.GetId())
	sm.snapshotID2RefIndex.Remove(updated.GetId())
	sm.removeFromSecondaryIndexes(updated)
	for _, pid := range expiredPinIDs {
		sm.pinID2SnapshotID.Remove(pid)
	}
	sm.pinLocks.Remove(updated.GetId())
	// From here on, reassign so downstream code references the swapped snapshot.
	snapshot = updated

	// Step 4: Rebuild protection state UNCONDITIONALLY. Must not be gated on
	// CompactionExpireTime > 0 because:
	//
	//   1. GC protection (segmentReferencedByGC/buildIDReferencedByGC) has no TTL —
	//      even a TTL=0 snapshot contributes, so every drop can leave stale entries.
	//   2. Even a TTL>0 snapshot with a Failed/Pending RefIndex imprinted a coarse
	//      collection-level block (compactionBlockedCollections or gcBlockedCollections)
	//      that a targeted rebuild cannot clear.
	//
	// A single rebuildAllSegmentProtection under one lock recomputes all five pieces
	// of state consistently. Incremental "unregister" is avoided on purpose because
	// it cannot tell whether another snapshot still references the same segment/buildID.
	sm.rebuildAllSegmentProtection()

	// Step 5: Delete S3 data (may fail, GC will retry)
	if err := sm.writer.Drop(ctx, snapshot.GetS3Location()); err != nil {
		log.Warn("S3 delete failed, will be cleaned by GC",
			zap.Int64("snapshotID", snapshot.GetId()),
			zap.Error(err))
		// Return success - snapshot is already invisible to users
		// GC will clean up S3 data by finding Deleting state snapshots
		return nil
	}

	// Step 6: Delete catalog record (final cleanup)
	if err := sm.catalog.DropSnapshot(ctx, snapshot.GetCollectionId(), snapshot.GetId()); err != nil {
		log.Warn("failed to drop snapshot from catalog after S3 cleanup",
			zap.Int64("snapshotID", snapshot.GetId()),
			zap.Error(err))
		// Return success - S3 data is already deleted
		// GC will clean up catalog record
		return nil
	}

	log.Info("snapshot deleted successfully", zap.Int64("snapshotID", snapshot.GetId()))
	return nil
}

// DropSnapshotsByCollection deletes all snapshots belonging to a collection.
// Used during drop collection cascade cleanup. Each snapshot goes through the
// same delete flow as individual DropSnapshot.
//
// Returns the names of snapshots that were successfully dropped so callers
// (manager layer) can clear per-snapshot metric series. Snapshots that were
// skipped (pinned, not-found) or failed are NOT included.
//
// Note: a concurrent SaveSnapshot between Step 1 (snapshot ID copy) and Step 3 (drop loop)
// could create a snapshot that is missed by this cascade. The GC orphan detection serves
// as a fallback to clean up such snapshots.
func (sm *snapshotMeta) DropSnapshotsByCollection(ctx context.Context, collectionID int64) ([]string, error) {
	log := log.Ctx(ctx).With(zap.Int64("collectionID", collectionID))

	// Step 1: Get all snapshot IDs for this collection.
	// Copy IDs under lock since UniqueSet (raw map) is not thread-safe for concurrent read/write.
	sm.collectionIndexMu.RLock()
	snapshotIDs, ok := sm.collectionID2Snapshots.Get(collectionID)
	var ids []int64
	if ok {
		ids = snapshotIDs.Collect()
	}
	sm.collectionIndexMu.RUnlock()
	if len(ids) == 0 {
		log.Info("no snapshots found for collection, skip")
		return nil, nil
	}

	// Step 2: Collect snapshot names before deletion (using copied IDs, safe without lock)
	var snapshotNames []string
	for _, id := range ids {
		if info, exists := sm.snapshotID2Info.Get(id); exists {
			snapshotNames = append(snapshotNames, info.GetName())
		}
	}

	// Step 3: Delete each snapshot through the standard flow.
	// DropSnapshot handles pin check internally (with expired-pin cleanup under pinMu).
	// Pinned snapshots will return error — treat as non-fatal skip.
	var errs []error
	var dropped []string
	for _, name := range snapshotNames {
		if err := sm.DropSnapshot(ctx, collectionID, name); err != nil {
			if errors.Is(err, merr.ErrSnapshotPinned) {
				log.Info("skip pinned snapshot during collection cleanup, GC will retry after TTL/unpin",
					zap.String("snapshotName", name),
					zap.Error(err))
				continue
			}
			if errors.Is(err, merr.ErrSnapshotNotFound) {
				log.Info("skip not-found snapshot during collection cleanup",
					zap.String("snapshotName", name),
					zap.Error(err))
				continue
			}
			errs = append(errs, err)
			log.Warn("failed to drop snapshot during collection cleanup, continuing",
				zap.String("snapshotName", name),
				zap.Error(err))
			continue
		}
		dropped = append(dropped, name)
		log.Info("dropped snapshot during collection cleanup",
			zap.String("snapshotName", name))
	}

	if len(errs) > 0 {
		return dropped, fmt.Errorf("failed to drop %d/%d snapshots for collection %d: %w",
			len(errs), len(snapshotNames), collectionID, errors.Join(errs...))
	}

	return dropped, nil
}

// ListSnapshots returns snapshot names filtered by collection and/or partition.
//
// This provides fast snapshot listing by querying the in-memory cache without
// accessing catalog or S3. Supports flexible filtering:
//   - collectionID <= 0: List all collections
//   - collectionID > 0: List only this collection (uses O(M) index lookup where M=snapshots in collection)
//   - partitionID <= 0: List all partitions in the collection
//   - partitionID > 0: List only snapshots containing this partition
//
// When collectionID is specified, uses the collectionID2Snapshots index for
// efficient lookup instead of scanning all snapshots.
//
// Parameters:
//   - ctx: Context for cancellation (currently unused but reserved for future)
//   - collectionID: Filter by collection ID, or <= 0 for all collections
//   - partitionID: Filter by partition ID, or <= 0 for all partitions
//
// Returns:
//   - []string: List of snapshot names matching the filter criteria
//   - error: Always returns nil (reserved for future error handling)
func (sm *snapshotMeta) ListSnapshots(ctx context.Context, collectionID int64, partitionID int64) ([]string, error) {
	ret := make([]string, 0)

	// If collectionID is specified, use index for O(M) lookup instead of O(N)
	if collectionID > 0 {
		// Acquire read lock to protect UniqueSet iteration from concurrent modifications
		sm.collectionIndexMu.RLock()
		snapshotIDs, ok := sm.collectionID2Snapshots.Get(collectionID)
		if !ok {
			sm.collectionIndexMu.RUnlock()
			return ret, nil // No snapshots for this collection
		}

		// Iterate only snapshots in this collection
		snapshotIDs.Range(func(snapshotID int64) bool {
			info, ok := sm.snapshotID2Info.Get(snapshotID)
			if !ok {
				return true // Continue iteration (index inconsistency, skip)
			}

			// Check partition filter
			if partitionID <= 0 || slices.Contains(info.GetPartitionIds(), partitionID) {
				ret = append(ret, info.GetName())
			}
			return true
		})
		sm.collectionIndexMu.RUnlock()
		return ret, nil
	}

	// No collectionID filter: scan all snapshots
	sm.snapshotID2Info.Range(func(id UniqueID, info *datapb.SnapshotInfo) bool {
		// Check partition filter (snapshot contains this partition)
		partitionMatch := partitionID <= 0 || slices.Contains(info.GetPartitionIds(), partitionID)

		if partitionMatch {
			ret = append(ret, info.GetName())
		}
		return true // Continue iteration
	})

	return ret, nil
}

// GetSnapshot returns basic snapshot metadata by name.
//
// This is a lightweight operation that only accesses the in-memory cache,
// returning the SnapshotInfo metadata without reading the complete snapshot
// data from S3. Use this when you only need snapshot metadata (ID, collection,
// partitions, S3 location) and not the full segment/index data.
//
// For complete snapshot data including segments and indexes, use ReadSnapshotData instead.
//
// Parameters:
//   - ctx: Context for cancellation (currently unused but reserved for future)
//   - snapshotName: Unique name of the snapshot to retrieve
//
// Returns:
//   - *datapb.SnapshotInfo: Basic snapshot metadata (name, ID, collection, partitions, S3 location)
//   - error: Error if snapshot not found
func (sm *snapshotMeta) GetSnapshot(ctx context.Context, collectionID int64, snapshotName string) (*datapb.SnapshotInfo, error) {
	snapshot, err := sm.getSnapshotByName(ctx, collectionID, snapshotName)
	if err != nil {
		return nil, err
	}
	return snapshot, nil
}

// ReadSnapshotData reads the complete snapshot data including segments and indexes from S3.
//
// This is a heavyweight operation that:
//  1. Looks up snapshot metadata in memory cache
//  2. Reads complete snapshot data from S3 (segments, indexes, schema)
//  3. Returns the full SnapshotData structure
//
// Use this when you need the complete snapshot data for operations like restore.
// For just metadata, use GetSnapshot instead to avoid the S3 read cost.
//
// Process flow:
//  1. Lookup snapshot by name in memory cache to get S3 location
//  2. Read complete snapshot data from S3 using the location
//  3. Merge S3 location from memory into the returned data
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - snapshotName: Unique name of the snapshot to read
//   - includeSegments: If true, reads segment manifest files from S3; if false, only reads metadata.json
//
// Returns:
//   - *SnapshotData: Snapshot data (segments only populated when includeSegments=true;
//     SegmentIDs always populated for new format snapshots)
//   - error: Error if snapshot not found or S3 read fails
func (sm *snapshotMeta) ReadSnapshotData(ctx context.Context, collectionID int64, snapshotName string, includeSegments bool) (*SnapshotData, error) {
	log := log.Ctx(ctx).With(zap.String("snapshotName", snapshotName), zap.Int64("collectionID", collectionID))

	// Step 1: Get snapshot metadata from memory to find S3 location
	snapshotInfo, err := sm.getSnapshotByName(ctx, collectionID, snapshotName)
	if err != nil {
		log.Error("failed to get snapshot by name", zap.Error(err))
		return nil, err
	}

	log.Info("got snapshot from memory before ReadSnapshot",
		zap.String("name", snapshotInfo.GetName()),
		zap.Int64("id", snapshotInfo.GetId()),
		zap.String("s3_location_from_memory", snapshotInfo.GetS3Location()))

	// Step 2: Read snapshot data from S3 using the known metadata path directly
	snapshotData, err := sm.reader.ReadSnapshot(ctx, snapshotInfo.GetS3Location(), includeSegments)
	if err != nil {
		log.Error("failed to read snapshot data from S3", zap.Error(err))
		return nil, err
	}

	// Step 3: Merge S3 location from memory into returned data
	snapshotData.SnapshotInfo.S3Location = snapshotInfo.S3Location

	return snapshotData, nil
}

// getSnapshotByName is an internal helper that looks up snapshot metadata by name within a collection.
//
// This uses the per-collection snapshotName2ID index for O(1) lookup instead of scanning.
//
// Parameters:
//   - ctx: Context for cancellation (currently unused but reserved for future)
//   - collectionID: Collection ID to scope the name lookup
//   - snapshotName: Name of the snapshot to find (unique within collection)
//
// Returns:
//   - *datapb.SnapshotInfo: Snapshot metadata if found
//   - error: Error if snapshot not found
func (sm *snapshotMeta) getSnapshotByName(ctx context.Context, collectionID int64, snapshotName string) (*datapb.SnapshotInfo, error) {
	// O(1) lookup using per-collection name index
	nameMap, ok := sm.snapshotName2ID.Get(collectionID)
	if !ok {
		return nil, merr.WrapErrSnapshotNotFound(snapshotName, fmt.Sprintf("collection %d", collectionID))
	}

	snapshotID, ok := nameMap.Get(snapshotName)
	if !ok {
		return nil, merr.WrapErrSnapshotNotFound(snapshotName, fmt.Sprintf("collection %d", collectionID))
	}

	// O(1) lookup from primary index
	info, ok := sm.snapshotID2Info.Get(snapshotID)
	if !ok {
		// Index inconsistency: clean up orphan name index entry
		nameMap.Remove(snapshotName)
		return nil, merr.WrapErrSnapshotNotFound(snapshotName, fmt.Sprintf("collection %d", collectionID))
	}

	return info, nil
}

// GetPendingSnapshots returns all snapshots that are in PENDING state.
// This is used by GC to find orphaned snapshots that need cleanup.
// Only returns snapshots that have exceeded the pending timeout.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - pendingTimeout: Duration after which a pending snapshot is considered orphaned
//
// Returns:
//   - []*datapb.SnapshotInfo: List of pending snapshots that have exceeded timeout
//   - error: Error if catalog list fails
func (sm *snapshotMeta) GetPendingSnapshots(ctx context.Context, pendingTimeout time.Duration) ([]*datapb.SnapshotInfo, error) {
	// Get all snapshots from catalog (etcd)
	snapshots, err := sm.catalog.ListSnapshots(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list snapshots from catalog: %w", err)
	}

	now := time.Now().UnixMilli()
	pendingSnapshots := make([]*datapb.SnapshotInfo, 0)

	for _, snapshot := range snapshots {
		// Only process PENDING snapshots
		if snapshot.GetState() != datapb.SnapshotState_SnapshotStatePending {
			continue
		}

		// Check if pending timeout exceeded
		if now-snapshot.GetPendingStartTime() < pendingTimeout.Milliseconds() {
			continue // Still within timeout, might be in progress
		}

		pendingSnapshots = append(pendingSnapshots, snapshot)
	}

	return pendingSnapshots, nil
}

// CleanupPendingSnapshot removes a pending snapshot from catalog.
// This is called by GC after S3 files have been cleaned up.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - snapshot: The pending snapshot to remove from catalog
//
// Returns:
//   - error: Error if catalog delete fails
func (sm *snapshotMeta) CleanupPendingSnapshot(ctx context.Context, snapshot *datapb.SnapshotInfo) error {
	return sm.catalog.DropSnapshot(ctx, snapshot.GetCollectionId(), snapshot.GetId())
}

// GetDeletingSnapshots returns all snapshots in DELETING state.
//
// These are snapshots that were marked for deletion but the S3 cleanup
// was not completed (e.g., due to network failure or process crash).
// GC should clean up these snapshots by deleting S3 data and catalog records.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//
// Returns:
//   - []*datapb.SnapshotInfo: List of snapshots in DELETING state
//   - error: Error if catalog query fails
func (sm *snapshotMeta) GetDeletingSnapshots(ctx context.Context) ([]*datapb.SnapshotInfo, error) {
	snapshots, err := sm.catalog.ListSnapshots(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list snapshots from catalog: %w", err)
	}

	deletingSnapshots := make([]*datapb.SnapshotInfo, 0)
	for _, snapshot := range snapshots {
		if snapshot.GetState() == datapb.SnapshotState_SnapshotStateDeleting {
			deletingSnapshots = append(deletingSnapshots, snapshot)
		}
	}

	return deletingSnapshots, nil
}

// CleanupDeletingSnapshot removes a deleting snapshot from catalog.
// This is called by GC after S3 files have been cleaned up.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - snapshot: The deleting snapshot to remove from catalog
//
// Returns:
//   - error: Error if catalog delete fails
func (sm *snapshotMeta) CleanupDeletingSnapshot(ctx context.Context, snapshot *datapb.SnapshotInfo) error {
	return sm.catalog.DropSnapshot(ctx, snapshot.GetCollectionId(), snapshot.GetId())
}

// addToSecondaryIndexes adds a snapshot to the secondary indexes (name and collection).
// This should be called after inserting into the primary indexes (snapshotID2Info and snapshotID2RefIndex).
func (sm *snapshotMeta) addToSecondaryIndexes(snapshotInfo *datapb.SnapshotInfo) {
	// Add to name index (per-collection, thread-safe via ConcurrentMap)
	collID := snapshotInfo.GetCollectionId()
	nameMap, _ := sm.snapshotName2ID.GetOrInsert(collID, typeutil.NewConcurrentMap[string, UniqueID]())
	nameMap.Insert(snapshotInfo.GetName(), snapshotInfo.GetId())

	// Add to collection index (requires lock since UniqueSet is not thread-safe)
	sm.collectionIndexMu.Lock()
	defer sm.collectionIndexMu.Unlock()

	collectionID := snapshotInfo.GetCollectionId()
	snapshotIDs, ok := sm.collectionID2Snapshots.Get(collectionID)
	if !ok {
		snapshotIDs = typeutil.NewUniqueSet()
	}
	snapshotIDs.Insert(snapshotInfo.GetId())
	sm.collectionID2Snapshots.Insert(collectionID, snapshotIDs)
}

// removeFromSecondaryIndexes removes a snapshot from the secondary indexes (name and collection).
// This should be called when removing from the primary indexes (snapshotID2Info and snapshotID2RefIndex).
func (sm *snapshotMeta) removeFromSecondaryIndexes(snapshotInfo *datapb.SnapshotInfo) {
	// Remove from name index (per-collection, thread-safe via ConcurrentMap).
	// Do NOT remove empty nameMap to avoid TOCTOU race with concurrent addToSecondaryIndexes.
	collID := snapshotInfo.GetCollectionId()
	if nameMap, ok := sm.snapshotName2ID.Get(collID); ok {
		nameMap.Remove(snapshotInfo.GetName())
	}

	// Remove from collection index (requires lock since UniqueSet is not thread-safe)
	sm.collectionIndexMu.Lock()
	defer sm.collectionIndexMu.Unlock()

	collectionID := snapshotInfo.GetCollectionId()
	snapshotIDs, ok := sm.collectionID2Snapshots.Get(collectionID)
	if !ok {
		return
	}
	snapshotIDs.Remove(snapshotInfo.GetId())
	// Clean up empty collection entry or update the map
	if snapshotIDs.Len() == 0 {
		sm.collectionID2Snapshots.Remove(collectionID)
	} else {
		sm.collectionID2Snapshots.Insert(collectionID, snapshotIDs)
	}
}

// IsCollectionCompactionBlocked checks if compaction is blocked for a collection.
// Returns true if:
//   - A protected snapshot's RefIndex hasn't been loaded yet (fail-closed), OR
//   - The collection is currently in the process of creating a snapshot (intent-based blocking).
func (sm *snapshotMeta) IsCollectionCompactionBlocked(collectionID int64) bool {
	sm.segmentProtectionMu.RLock()
	defer sm.segmentProtectionMu.RUnlock()
	return sm.compactionBlockedCollections.Contain(collectionID) ||
		sm.snapshotPendingCollections.Contain(collectionID)
}

// GetActiveCollectionIDs returns the set of collection IDs that have non-Deleting snapshots.
// Used by GC for orphan snapshot detection.
func (sm *snapshotMeta) GetActiveCollectionIDs() []int64 {
	collections := typeutil.NewSet[int64]()
	sm.snapshotID2Info.Range(func(id UniqueID, info *datapb.SnapshotInfo) bool {
		if info.GetState() != datapb.SnapshotState_SnapshotStateDeleting {
			collections.Insert(info.GetCollectionId())
		}
		return true
	})
	return collections.Collect()
}

// GetSnapshotByBuildID returns all snapshot IDs that reference a given build ID.
//
// This is used for garbage collection safety: before deleting index files for a build,
// check if any snapshots reference it. If snapshots exist, the files cannot be deleted.
//
// BuildID identifies a specific index build task, providing precise file-level protection.
//
// Caller should check IsAllRefIndexLoaded or IsRefIndexLoadedForCollection first.
//
// Parameters:
//   - buildID: Build ID to search for in snapshots
//
// Returns:
//   - []UniqueID: List of snapshot IDs that contain this build ID (empty if none)
func (sm *snapshotMeta) GetSnapshotByBuildID(buildID UniqueID) []UniqueID {
	snapshotIDs := make([]UniqueID, 0)

	sm.snapshotID2RefIndex.Range(func(id UniqueID, refIndex *SnapshotRefIndex) bool {
		if refIndex.ContainsBuildID(buildID) {
			snapshotIDs = append(snapshotIDs, id)
		}
		return true
	})

	return snapshotIDs
}

// SetSnapshotPending marks a collection as having a pending snapshot creation.
// This blocks compaction commit for the collection to prevent segment state changes
// during the window between GenSnapshot and SaveSnapshot.
func (sm *snapshotMeta) SetSnapshotPending(collectionID int64) {
	sm.segmentProtectionMu.Lock()
	defer sm.segmentProtectionMu.Unlock()
	sm.snapshotPendingCollections.Insert(collectionID)
	log.Info("collection marked as snapshot pending, compaction blocked",
		zap.Int64("collectionID", collectionID))
}

// ClearSnapshotPending removes the pending snapshot mark for a collection.
// Called after SaveSnapshot completes (success or failure) to unblock compaction.
func (sm *snapshotMeta) ClearSnapshotPending(collectionID int64) {
	sm.segmentProtectionMu.Lock()
	defer sm.segmentProtectionMu.Unlock()
	sm.snapshotPendingCollections.Remove(collectionID)
	log.Info("collection snapshot pending mark cleared, compaction unblocked",
		zap.Int64("collectionID", collectionID))
}

// isProtectionActive returns true if the given expiry timestamp represents
// active (non-zero and not yet expired) compaction protection.
func isProtectionActive(protectionUntil uint64, now uint64) bool {
	return protectionUntil > 0 && now < protectionUntil
}

// upsertMaxProtection sets the protection expiry for a segment to the maximum
// of the existing and new values. Must be called with segmentProtectionMu held.
func (sm *snapshotMeta) upsertMaxProtection(segID int64, protectionUntil uint64) {
	if existing, ok := sm.segmentProtectionUntil[segID]; !ok || protectionUntil > existing {
		sm.segmentProtectionUntil[segID] = protectionUntil
	}
}

// IsSegmentCompactionProtected checks if a segment is protected from compaction
// by any active snapshot. Returns true if the segment should be excluded from compaction.
func (sm *snapshotMeta) IsSegmentCompactionProtected(segmentID int64) bool {
	sm.segmentProtectionMu.RLock()
	defer sm.segmentProtectionMu.RUnlock()
	expiryTs, exists := sm.segmentProtectionUntil[segmentID]
	if !exists {
		return false
	}
	return uint64(time.Now().Unix()) < expiryTs
}

// IsSegmentGCBlocked reports whether a segment must be kept by GC because of snapshot
// references, without requiring a separate "is loaded" check by the caller.
//
// Fail-closed semantics, layered from coarse to precise:
//  1. If collectionID is non-negative and that collection is in gcBlockedCollections
//     (some snapshot's RefIndex has not been loaded from S3 yet), return true — we
//     cannot know the precise referenced set, so we keep the segment.
//  2. If collectionID is negative (orphan file walk with no collection context), any
//     unloaded RefIndex in any collection triggers a fail-closed return.
//  3. Otherwise, return whether the segment is in segmentReferencedByGC.
//
// collectionID == 0 takes the non-negative branch (same as any positive ID). Since
// Milvus never assigns 0 as a real collection ID, that branch will never match a real
// snapshot and simply falls through to the precise segmentReferencedByGC check. If the
// caller truly has no collection context, pass a negative value to hit branch 2.
//
// GC semantics are unconditional on TTL: snapshots whose CompactionExpireTime has
// elapsed still contribute to the precise set, because PIT recovery still needs the
// underlying files.
//
// Cost: O(1) — all state is precomputed by rebuildAllSegmentProtection.
func (sm *snapshotMeta) IsSegmentGCBlocked(collectionID, segmentID int64) bool {
	sm.segmentProtectionMu.RLock()
	defer sm.segmentProtectionMu.RUnlock()
	if collectionID < 0 {
		if sm.gcBlockedCollections.Len() > 0 {
			return true
		}
	} else if sm.gcBlockedCollections.Contain(collectionID) {
		return true
	}
	return sm.segmentReferencedByGC.Contain(segmentID)
}

// IsBuildIDGCBlocked reports whether an index buildID must be kept by GC because of
// snapshot references. Same fail-closed layering as IsSegmentGCBlocked, including
// the collectionID semantics: < 0 means "orphan walk, fail-closed globally", and any
// non-negative value (including 0) narrows the fail-closed path to that collection.
//
// BuildIDs are global (an index build is identified by its unique buildID, not scoped
// to a specific collection). Pass collectionID < 0 when the caller is walking orphan
// buildIDs without a collection context; pass a specific collectionID when the caller
// already has per-segment-index context (in which case only that collection's load
// state gates the fail-closed path).
//
// Cost: O(1).
func (sm *snapshotMeta) IsBuildIDGCBlocked(collectionID, buildID int64) bool {
	sm.segmentProtectionMu.RLock()
	defer sm.segmentProtectionMu.RUnlock()
	if collectionID < 0 {
		if sm.gcBlockedCollections.Len() > 0 {
			return true
		}
	} else if sm.gcBlockedCollections.Contain(collectionID) {
		return true
	}
	return sm.buildIDReferencedByGC.Contain(buildID)
}

// registerSnapshotProtection incrementally registers a newly saved snapshot into both
// protection dimensions under a single lock:
//
//   - GC protection (unconditional on TTL): the snapshot's segments and buildIDs are
//     inserted into segmentReferencedByGC / buildIDReferencedByGC so the underlying
//     files are pinned against GC, regardless of CompactionExpireTime.
//   - Compaction protection (TTL-bound): if CompactionExpireTime is still active, the
//     segments' entries in segmentProtectionUntil are upserted to the max expiry.
//
// This is the incremental counterpart to rebuildAllSegmentProtection — we use it on
// the SaveSnapshot success path to avoid a full O(N) rebuild for a single new snapshot
// while still keeping both dimensions in sync. rebuildAllSegmentProtection remains the
// canonical path when state needs to be recomputed (startup, loader progress, rollback,
// drop), because incremental updates cannot remove another snapshot's contribution.
func (sm *snapshotMeta) registerSnapshotProtection(
	info *datapb.SnapshotInfo, segmentIDs, buildIDs []int64,
) {
	sm.segmentProtectionMu.Lock()
	defer sm.segmentProtectionMu.Unlock()

	// GC protection (unconditional) — runs for every snapshot, TTL=0 included.
	for _, segID := range segmentIDs {
		sm.segmentReferencedByGC.Insert(segID)
	}
	for _, buildID := range buildIDs {
		sm.buildIDReferencedByGC.Insert(buildID)
	}

	// Compaction protection (TTL-bound) — only snapshots with active CompactionExpireTime.
	protectionUntil := info.GetCompactionExpireTime()
	if isProtectionActive(protectionUntil, uint64(time.Now().Unix())) {
		for _, segID := range segmentIDs {
			sm.upsertMaxProtection(segID, protectionUntil)
		}
	}

	log.Info("registered snapshot protection",
		zap.Int64("snapshotID", info.GetId()),
		zap.Int64("collectionID", info.GetCollectionId()),
		zap.Uint64("protectionUntil", protectionUntil),
		zap.Int("numSegments", len(segmentIDs)),
		zap.Int("numBuildIDs", len(buildIDs)))
}

// rebuildAllSegmentProtection atomically rebuilds BOTH compaction protection and GC
// protection state from all snapshots. Called whenever snapshot state changes (on
// startup, after RefIndex loader makes progress, after SaveSnapshot rollback, after
// DropSnapshot).
//
// A single Range over snapshotID2Info produces 5 pieces of state under one lock,
// keeping the two protection dimensions consistent:
//
//  1. segmentProtectionUntil — TTL-bound segment compaction protection
//  2. compactionBlockedCollections — fail-closed coarse block for compaction
//  3. segmentReferencedByGC — precise segment GC protection (no TTL)
//  4. buildIDReferencedByGC — precise buildID GC protection (no TTL)
//  5. gcBlockedCollections — fail-closed coarse block for GC
//
// Fail-closed design (both dimensions): if a snapshot's RefIndex hasn't been loaded,
// we don't know which segments/buildIDs it references. In that case the entire
// collection is coarsely blocked until the loader goroutine finishes, at which point
// the next rebuild will narrow the coarse block to precise per-segment/buildID sets.
//
// TTL asymmetry between the two dimensions:
//   - Compaction only protects snapshots whose CompactionExpireTime is still active.
//     Expired-TTL snapshots contribute nothing to segmentProtectionUntil /
//     compactionBlockedCollections.
//   - GC protects segments/buildIDs referenced by ANY existing snapshot, regardless
//     of TTL, because a snapshot always needs its underlying files to serve PIT
//     recovery. Expired-TTL snapshots still contribute to the GC sets.
func (sm *snapshotMeta) rebuildAllSegmentProtection() {
	sm.segmentProtectionMu.Lock()
	defer sm.segmentProtectionMu.Unlock()

	// Reset all protection state — the rebuild produces a complete, consistent snapshot.
	// NOTE: snapshotPendingCollections is intentionally NOT reset here. It is a short-lived
	// intent flag owned by SetSnapshotPending/ClearSnapshotPending (scoped to a single
	// CreateSnapshot call), independent from the snapshot→collection state that this
	// rebuild derives from snapshotID2Info. Clearing it here would let a concurrent
	// compaction slip through the GenSnapshot→SaveSnapshot window.
	sm.segmentProtectionUntil = make(map[int64]uint64)
	compactionBlocked := typeutil.NewUniqueSet()
	gcBlocked := typeutil.NewUniqueSet()
	segRefGC := typeutil.NewUniqueSet()
	buildRefGC := typeutil.NewUniqueSet()

	now := uint64(time.Now().Unix())
	sm.snapshotID2Info.Range(func(id UniqueID, info *datapb.SnapshotInfo) bool {
		refIndex, exists := sm.snapshotID2RefIndex.Get(id)
		loaded := exists && refIndex.IsLoaded()

		// ===== GC protection (unconditional on TTL) =====
		if !loaded {
			// Fail-closed: we don't know which segments/buildIDs this snapshot references.
			// Coarse-block the entire collection for GC until the RefIndex loads.
			gcBlocked.Insert(info.GetCollectionId())
		} else {
			// Precise: record every referenced segment/buildID for O(1) lookups.
			for _, segID := range refIndex.GetSegmentIDs() {
				segRefGC.Insert(segID)
			}
			for _, buildID := range refIndex.GetBuildIDs() {
				buildRefGC.Insert(buildID)
			}
		}

		// ===== Compaction protection (TTL-bound) =====
		protectionUntil := info.GetCompactionExpireTime()
		if !isProtectionActive(protectionUntil, now) {
			// TTL expired or never set — no compaction protection contribution.
			return true
		}

		if !loaded {
			// Fail-closed: active TTL but RefIndex not loaded.
			compactionBlocked.Insert(info.GetCollectionId())
			log.Info("blocking compaction for collection due to unloaded protected snapshot RefIndex",
				zap.Int64("snapshotID", id),
				zap.Int64("collectionID", info.GetCollectionId()),
				zap.Uint64("protectionUntil", protectionUntil))
			return true
		}

		for _, segID := range refIndex.GetSegmentIDs() {
			sm.upsertMaxProtection(segID, protectionUntil)
		}
		return true
	})

	sm.compactionBlockedCollections = compactionBlocked
	sm.gcBlockedCollections = gcBlocked
	sm.segmentReferencedByGC = segRefGC
	sm.buildIDReferencedByGC = buildRefGC
	log.Info("rebuilt all snapshot protection state",
		zap.Int("compactionProtectedSegments", len(sm.segmentProtectionUntil)),
		zap.Int("compactionBlockedCollections", sm.compactionBlockedCollections.Len()),
		zap.Int("gcReferencedSegments", sm.segmentReferencedByGC.Len()),
		zap.Int("gcReferencedBuildIDs", sm.buildIDReferencedByGC.Len()),
		zap.Int("gcBlockedCollections", sm.gcBlockedCollections.Len()))
}

// PinCleanupResult summarizes the per-snapshot outcome of a pin-modifying
// meta operation. It carries the information the manager layer needs to emit
// the active-pins gauge without itself touching pinMu or SnapshotInfo.
type PinCleanupResult struct {
	CollectionID int64
	SnapshotName string
	ActivePins   int // pins remaining after the operation completes
}

// cloneSnapshotInfo deep-copies a SnapshotInfo so the caller can mutate the
// clone without racing against readers holding the original pointer. Needed
// for the clone-and-swap pattern: Pin/Unpin/cleanExpired/DropSnapshot mutate
// the clone, persist it to etcd OUTSIDE of pinMu, then swap the pointer back
// into snapshotID2Info atomically.
func cloneSnapshotInfo(s *datapb.SnapshotInfo) *datapb.SnapshotInfo {
	if s == nil {
		return nil
	}
	return proto.Clone(s).(*datapb.SnapshotInfo)
}

// ensurePinMaps lazily initializes pinLocks/pinID2SnapshotID for tests that
// construct snapshotMeta via struct-literal. newSnapshotMeta always pre-seeds
// them; in that case this is a no-op.
func (sm *snapshotMeta) ensurePinMaps() {
	sm.pinMapsInit.Do(func() {
		if sm.pinLocks == nil {
			sm.pinLocks = typeutil.NewConcurrentMap[UniqueID, *sync.Mutex]()
		}
		if sm.pinID2SnapshotID == nil {
			sm.pinID2SnapshotID = typeutil.NewConcurrentMap[int64, UniqueID]()
		}
	})
}

// getPinLock returns (creating if needed) the per-snapshot serialization
// lock. The lock gates concurrent writers on the same snapshot so clone-and-
// swap remains linearizable, while letting writers on different snapshots
// proceed in parallel. Cleaned up on DropSnapshot.
func (sm *snapshotMeta) getPinLock(snapshotID UniqueID) *sync.Mutex {
	sm.ensurePinMaps()
	if l, ok := sm.pinLocks.Get(snapshotID); ok {
		return l
	}
	actual, _ := sm.pinLocks.GetOrInsert(snapshotID, &sync.Mutex{})
	return actual
}

// swapSnapshotInfoLocked atomically replaces the live snapshot pointer and
// reconciles the pinID2SnapshotID reverse index with the new PinIds. Caller
// must hold pinMu.Lock.
func (sm *snapshotMeta) swapSnapshotInfoLocked(old, updated *datapb.SnapshotInfo) {
	sm.ensurePinMaps()
	sm.snapshotID2Info.Insert(updated.GetId(), updated)
	// Reconcile reverse index: any pinID present in old but not in new is removed,
	// any pinID present in new is inserted. Small lists — linear diff is fine.
	oldPins := make(map[int64]struct{}, len(old.GetPinIds()))
	for _, p := range old.GetPinIds() {
		oldPins[p] = struct{}{}
	}
	newPins := make(map[int64]struct{}, len(updated.GetPinIds()))
	for _, p := range updated.GetPinIds() {
		newPins[p] = struct{}{}
	}
	for p := range oldPins {
		if _, ok := newPins[p]; !ok {
			sm.pinID2SnapshotID.Remove(p)
		}
	}
	for p := range newPins {
		sm.pinID2SnapshotID.Insert(p, updated.GetId())
	}
}

// PinSnapshot generates a crypto-random pin ID, appends it to the snapshot's
// pin_ids list with optional TTL, and persists to catalog. Returns the pin ID
// for later Unpin along with the resulting active-pin count (for metric
// emission at the caller layer).
//
// Uses the clone-and-swap pattern: per-snapshot lock serializes writers on
// the same snapshot; etcd IO runs OUTSIDE pinMu so Pin/Unpin on other
// snapshots are not blocked on this write.
func (sm *snapshotMeta) PinSnapshot(ctx context.Context, collectionID int64, name string, ttlSeconds int64) (int64, int, error) {
	sm.pinMu.RLock()
	snapshot, err := sm.getSnapshotByName(ctx, collectionID, name)
	sm.pinMu.RUnlock()
	if err != nil {
		return 0, 0, err
	}

	lock := sm.getPinLock(snapshot.GetId())
	lock.Lock()
	defer lock.Unlock()

	// Re-read the current pointer under the per-snapshot lock — another
	// writer may have just swapped it.
	sm.pinMu.RLock()
	current, ok := sm.snapshotID2Info.Get(snapshot.GetId())
	sm.pinMu.RUnlock()
	if !ok {
		return 0, 0, merr.WrapErrSnapshotNotFound(name, "snapshot removed before pin")
	}
	if current.GetState() == datapb.SnapshotState_SnapshotStateDeleting {
		return 0, 0, merr.WrapErrSnapshotNotFound(name, "snapshot is being deleted")
	}

	pinID, err := sm.generatePinID(current)
	if err != nil {
		return 0, 0, err
	}

	updated := cloneSnapshotInfo(current)
	updated.PinIds = append(updated.PinIds, pinID)
	if updated.PinExpireAtMs == nil {
		updated.PinExpireAtMs = make(map[int64]int64)
	}
	if ttlSeconds > 0 {
		updated.PinExpireAtMs[pinID] = time.Now().UnixMilli() + ttlSeconds*1000
	}

	if err := sm.catalog.SaveSnapshot(ctx, updated); err != nil {
		return 0, 0, err
	}

	sm.pinMu.Lock()
	sm.swapSnapshotInfoLocked(current, updated)
	sm.pinMu.Unlock()

	active := countActivePins(updated)
	log.Ctx(ctx).Info("pinned snapshot",
		zap.String("name", name),
		zap.Int64("collectionID", collectionID),
		zap.Int64("pinID", pinID),
		zap.Int64("ttlSeconds", ttlSeconds),
		zap.Int("pinCount", len(updated.GetPinIds())),
		zap.Int("activePins", active))
	return pinID, active, nil
}

// HasActivePins reports whether the named snapshot currently has any
// non-expired pins. Takes pinMu.RLock just long enough to snapshot the
// pointer — no etcd IO under lock.
//
// Used by service-layer DropSnapshot as a pre-broadcast pin check. Combined
// with PinSnapshotData acquiring the shared (collectionID, snapshotName)
// resource key lock, this gives DropSnapshot an authoritative view of active
// pins at the moment the exclusive broadcast lock is held — preventing a
// concurrent Pin from slipping in between the check and the callback.
func (sm *snapshotMeta) HasActivePins(ctx context.Context, collectionID int64, snapshotName string) (bool, error) {
	sm.pinMu.RLock()
	snapshot, err := sm.getSnapshotByName(ctx, collectionID, snapshotName)
	sm.pinMu.RUnlock()
	if err != nil {
		return false, err
	}
	return countActivePins(snapshot) > 0, nil
}

// countActivePins returns the number of non-expired pins on a snapshot.
// A pin is active if it has no expiry (permanent) or its expiry is in the
// future. Pure function over the passed-in SnapshotInfo — the caller is
// responsible for ensuring the SnapshotInfo pointer is stable (typically by
// either holding a clone, or operating under pinMu).
func countActivePins(snapshot *datapb.SnapshotInfo) int {
	if len(snapshot.GetPinIds()) == 0 {
		return 0
	}
	count := 0
	now := time.Now().UnixMilli()
	for _, pid := range snapshot.GetPinIds() {
		expireAt, ok := snapshot.GetPinExpireAtMs()[pid]
		if !ok || expireAt <= 0 || expireAt >= now {
			count++ // permanent pin or not yet expired
		}
	}
	return count
}

// UnpinSnapshot removes a pin ID from the snapshot's pin_ids list and
// persists. O(1) lookup via the pinID2SnapshotID reverse index; idempotent
// if pinID is not found (already unpinned or TTL-expired).
//
// Returns (collectionID, snapshotName, activePinCount, err) so the caller
// layer can emit metrics without touching SnapshotInfo directly.
func (sm *snapshotMeta) UnpinSnapshot(ctx context.Context, pinID int64) (int64, string, int, error) {
	sm.ensurePinMaps()
	snapshotID, ok := sm.pinID2SnapshotID.Get(pinID)
	if !ok {
		return 0, "", 0, nil // idempotent
	}

	lock := sm.getPinLock(snapshotID)
	lock.Lock()
	defer lock.Unlock()

	sm.pinMu.RLock()
	current, ok := sm.snapshotID2Info.Get(snapshotID)
	sm.pinMu.RUnlock()
	if !ok {
		// Snapshot was dropped underneath us; reverse index will have been
		// cleaned by DropSnapshot. Idempotent.
		sm.pinID2SnapshotID.Remove(pinID)
		return 0, "", 0, nil
	}

	// Pin may have been removed by a concurrent cleanExpired / Drop between
	// the reverse index lookup and the per-snapshot lock acquisition.
	if !slices.Contains(current.GetPinIds(), pinID) {
		sm.pinID2SnapshotID.Remove(pinID)
		return current.GetCollectionId(), current.GetName(), countActivePins(current), nil
	}

	updated := cloneSnapshotInfo(current)
	newPinIds := make([]int64, 0, len(updated.PinIds)-1)
	for _, pid := range updated.PinIds {
		if pid != pinID {
			newPinIds = append(newPinIds, pid)
		}
	}
	updated.PinIds = newPinIds
	delete(updated.PinExpireAtMs, pinID)

	if err := sm.catalog.SaveSnapshot(ctx, updated); err != nil {
		return current.GetCollectionId(), current.GetName(), 0, err
	}

	sm.pinMu.Lock()
	sm.swapSnapshotInfoLocked(current, updated)
	sm.pinMu.Unlock()

	active := countActivePins(updated)
	log.Ctx(ctx).Info("unpinned snapshot",
		zap.String("name", updated.GetName()),
		zap.Int64("collectionID", updated.GetCollectionId()),
		zap.Int64("pinID", pinID),
		zap.Int("pinCount", len(updated.GetPinIds())),
		zap.Int("activePins", active))
	return updated.GetCollectionId(), updated.GetName(), active, nil
}

// cleanExpiredPinsForCollection removes expired pins from snapshots of a
// collection. Only removes pins whose TTL has expired; permanent pins (no
// TTL) are left untouched. Returns per-snapshot outcomes so the caller can
// update metric gauges without re-reading SnapshotInfo.
//
// Uses the same clone-and-swap pattern as Pin/Unpin: one per-snapshot lock
// + one etcd write per snapshot, no global pinMu held across snapshots.
func (sm *snapshotMeta) cleanExpiredPinsForCollection(ctx context.Context, collectionID int64) []PinCleanupResult {
	sm.collectionIndexMu.RLock()
	snapshotIDs, ok := sm.collectionID2Snapshots.Get(collectionID)
	var ids []int64
	if ok {
		ids = snapshotIDs.Collect()
	}
	sm.collectionIndexMu.RUnlock()
	if len(ids) == 0 {
		return nil
	}

	var results []PinCleanupResult
	now := time.Now().UnixMilli()
	for _, snapshotID := range ids {
		updatedInfo, changed := sm.cleanExpiredPinsForSnapshot(ctx, snapshotID, now)
		if changed {
			results = append(results, PinCleanupResult{
				CollectionID: updatedInfo.GetCollectionId(),
				SnapshotName: updatedInfo.GetName(),
				ActivePins:   countActivePins(updatedInfo),
			})
		}
	}
	return results
}

// cleanExpiredPinsForSnapshot applies clone-and-swap to a single snapshot.
// Returns the (possibly new) SnapshotInfo pointer and whether any state
// actually changed.
func (sm *snapshotMeta) cleanExpiredPinsForSnapshot(ctx context.Context, snapshotID UniqueID, now int64) (*datapb.SnapshotInfo, bool) {
	lock := sm.getPinLock(snapshotID)
	lock.Lock()
	defer lock.Unlock()

	sm.pinMu.RLock()
	current, exists := sm.snapshotID2Info.Get(snapshotID)
	sm.pinMu.RUnlock()
	if !exists || len(current.GetPinIds()) == 0 {
		return current, false
	}

	var expired []int64
	for _, pinID := range current.GetPinIds() {
		expireAt, hasExpiry := current.GetPinExpireAtMs()[pinID]
		if hasExpiry && expireAt > 0 && expireAt < now {
			expired = append(expired, pinID)
		}
	}
	if len(expired) == 0 {
		return current, false
	}

	expiredSet := make(map[int64]struct{}, len(expired))
	for _, pid := range expired {
		expiredSet[pid] = struct{}{}
	}
	updated := cloneSnapshotInfo(current)
	newPinIds := make([]int64, 0, len(updated.PinIds)-len(expired))
	for _, pid := range updated.PinIds {
		if _, isExpired := expiredSet[pid]; !isExpired {
			newPinIds = append(newPinIds, pid)
		}
	}
	updated.PinIds = newPinIds
	for _, pid := range expired {
		delete(updated.PinExpireAtMs, pid)
	}

	if err := sm.catalog.SaveSnapshot(ctx, updated); err != nil {
		log.Warn("failed to persist expired pin cleanup, will retry",
			zap.String("name", current.GetName()), zap.Error(err))
		return current, false
	}

	sm.pinMu.Lock()
	sm.swapSnapshotInfoLocked(current, updated)
	sm.pinMu.Unlock()

	log.Info("cleaned expired pins from snapshot",
		zap.String("name", updated.GetName()),
		zap.Int64("collectionID", updated.GetCollectionId()),
		zap.Int("expiredCount", len(expired)),
		zap.Int("remainingPins", len(updated.GetPinIds())))
	return updated, true
}

// generatePinID generates a cryptographically random positive int64 that
// doesn't collide with any existing pin ID in the snapshot.
func (sm *snapshotMeta) generatePinID(snapshot *datapb.SnapshotInfo) (int64, error) {
	existing := make(map[int64]struct{}, len(snapshot.GetPinIds()))
	for _, pid := range snapshot.GetPinIds() {
		existing[pid] = struct{}{}
	}
	for i := 0; i < 3; i++ {
		var b [8]byte
		if _, err := cryptorand.Read(b[:]); err != nil {
			return 0, fmt.Errorf("failed to generate random pin ID: %w", err)
		}
		id := int64(binary.BigEndian.Uint64(b[:]) >> 1)
		if id == 0 {
			id = 1
		}
		if _, exists := existing[id]; !exists {
			return id, nil
		}
	}
	return 0, fmt.Errorf("failed to generate unique pin ID after 3 attempts")
}
