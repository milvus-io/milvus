package datacoord

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
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

// SnapshotRefIndex holds segment and index IDs loaded from S3.
// Loading state is managed per-snapshot to support retry on failure.
//
// CONCURRENCY: Protected by RWMutex since SetLoaded (async loader) and
// ContainsSegment/ContainsIndex (GC) may run concurrently.
type SnapshotRefIndex struct {
	mu         sync.RWMutex
	loadState  RefIndexLoadState
	segmentIDs typeutil.UniqueSet
	indexIDs   typeutil.UniqueSet
}

// NewSnapshotRefIndex creates a new empty SnapshotRefIndex.
// Used during reload when data needs to be loaded from S3 asynchronously.
func NewSnapshotRefIndex() *SnapshotRefIndex {
	return &SnapshotRefIndex{}
}

// NewLoadedSnapshotRefIndex creates a SnapshotRefIndex with pre-loaded data.
// Used for newly created snapshots where data is already available.
func NewLoadedSnapshotRefIndex(segmentIDs, indexIDs []int64) *SnapshotRefIndex {
	return &SnapshotRefIndex{
		loadState:  RefIndexStateLoaded,
		segmentIDs: typeutil.NewUniqueSet(segmentIDs...),
		indexIDs:   typeutil.NewUniqueSet(indexIDs...),
	}
}

// SetLoaded sets the segment and index IDs and marks the RefIndex as loaded.
// This is called by the async loader after reading data from S3.
func (r *SnapshotRefIndex) SetLoaded(segmentIDs, indexIDs []int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.loadState = RefIndexStateLoaded
	r.segmentIDs = typeutil.NewUniqueSet(segmentIDs...)
	r.indexIDs = typeutil.NewUniqueSet(indexIDs...)
}

// ContainsSegment checks if segment exists.
// Used by GC to check if a segment is referenced by this snapshot.
func (r *SnapshotRefIndex) ContainsSegment(segmentID UniqueID) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.segmentIDs != nil && r.segmentIDs.Contain(segmentID)
}

// ContainsIndex checks if index exists.
// Used by GC to check if an index is referenced by this snapshot.
func (r *SnapshotRefIndex) ContainsIndex(indexID UniqueID) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.indexIDs != nil && r.indexIDs.Contain(indexID)
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
	// snapshotName2ID: snapshot name -> snapshot ID mapping for fast name-based queries
	snapshotName2ID *typeutil.ConcurrentMap[string, UniqueID]
	// collectionID2Snapshots: collection ID -> set of snapshot IDs for fast collection-based listing
	collectionID2Snapshots *typeutil.ConcurrentMap[UniqueID, typeutil.UniqueSet]
	// collectionIndexMu protects read-modify-write operations on collectionID2Snapshots
	// since UniqueSet (map) is not thread-safe for concurrent modifications.
	// Uses RWMutex to allow concurrent reads (ListSnapshots) while blocking writes.
	collectionIndexMu sync.RWMutex

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
		catalog:                catalog,
		snapshotID2Info:        typeutil.NewConcurrentMap[UniqueID, *datapb.SnapshotInfo](),
		snapshotID2RefIndex:    typeutil.NewConcurrentMap[UniqueID, *SnapshotRefIndex](),
		snapshotName2ID:        typeutil.NewConcurrentMap[string, UniqueID](),
		collectionID2Snapshots: typeutil.NewConcurrentMap[UniqueID, typeutil.UniqueSet](),
		loaderCtx:              loaderCtx,
		loaderCancel:           loaderCancel,
		reader:                 NewSnapshotReader(chunkManager),
		writer:                 NewSnapshotWriter(chunkManager),
	}

	// Reload all snapshots from catalog to populate in-memory cache
	if err := sm.reload(ctx); err != nil {
		loaderCancel()
		log.Error("failed to reload snapshot meta from kv", zap.Error(err))
		return nil, err
	}

	// Start background RefIndex loader goroutine
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

		log.Info("loaded snapshot metadata from catalog",
			zap.String("name", snapshot.GetName()),
			zap.Int64("id", snapshot.GetId()))
	}

	return nil
}

// refIndexLoaderLoop periodically loads unloaded RefIndexes in background.
// It runs until loaderCtx is cancelled.
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

	// Load immediately on startup.
	sm.loadUnloadedRefIndexes()

	timer := time.NewTimer(getInterval())
	defer timer.Stop()

	for {
		select {
		case <-sm.loaderCtx.Done():
			log.Info("RefIndex loader goroutine stopped")
			return
		case <-timer.C:
			sm.loadUnloadedRefIndexes()
			// Reset using the latest refreshable interval.
			timer.Reset(getInterval())
		}
	}
}

// loadUnloadedRefIndexes loads all RefIndexes that are Pending or Failed.
func (sm *snapshotMeta) loadUnloadedRefIndexes() {
	sm.snapshotID2RefIndex.Range(func(id UniqueID, refIndex *SnapshotRefIndex) bool {
		if refIndex.IsLoaded() {
			return true // Already loaded, skip
		}

		info, exists := sm.snapshotID2Info.Get(id)
		if !exists {
			return true // Snapshot deleted
		}

		snapshotData, err := sm.reader.ReadSnapshot(sm.loaderCtx, info.GetS3Location(), false)
		if err != nil {
			log.Warn("failed to load RefIndex from S3",
				zap.String("name", info.GetName()),
				zap.Int64("id", id),
				zap.Error(err))
			refIndex.SetFailed()
		} else {
			refIndex.SetLoaded(snapshotData.SegmentIDs, snapshotData.IndexIDs)
			log.Info("loaded RefIndex from S3",
				zap.String("name", info.GetName()),
				zap.Int64("id", id))
		}

		return true
	})
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

	// Step 1: Extract segment IDs and index IDs for reference tracking
	segmentIDs := make([]int64, 0, len(snapshot.Segments))
	indexIDs := make([]int64, 0, len(snapshot.Indexes))
	for _, segment := range snapshot.Segments {
		segmentIDs = append(segmentIDs, segment.GetSegmentId())
	}
	for _, index := range snapshot.Indexes {
		indexIDs = append(indexIDs, index.GetIndexID())
	}

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

	// Insert into in-memory cache (two maps)
	sm.snapshotID2Info.Insert(snapshot.SnapshotInfo.GetId(), snapshot.SnapshotInfo)
	sm.snapshotID2RefIndex.Insert(snapshot.SnapshotInfo.GetId(),
		NewLoadedSnapshotRefIndex(segmentIDs, indexIDs))

	// Build secondary indexes for O(1) lookup
	sm.addToSecondaryIndexes(snapshot.SnapshotInfo)

	// Update catalog with COMMITTED state
	if err := sm.catalog.SaveSnapshot(ctx, snapshot.SnapshotInfo); err != nil {
		// Phase 2 failed, but S3 data is written. GC will eventually cleanup.
		// Remove from memory cache and secondary indexes since commit failed
		sm.snapshotID2Info.Remove(snapshot.SnapshotInfo.GetId())
		sm.snapshotID2RefIndex.Remove(snapshot.SnapshotInfo.GetId())
		sm.removeFromSecondaryIndexes(snapshot.SnapshotInfo)
		log.Error("failed to update snapshot to committed state, pending record left for GC cleanup",
			zap.Error(err))
		return fmt.Errorf("failed to update snapshot to committed state: %w", err)
	}

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
//   - snapshotName: Unique name of the snapshot to delete
//
// Returns:
//   - error: Error if snapshot not found or catalog update fails
func (sm *snapshotMeta) DropSnapshot(ctx context.Context, snapshotName string) error {
	log := log.Ctx(ctx).With(zap.String("snapshotName", snapshotName))

	// Step 1: Lookup snapshot by name
	snapshot, err := sm.getSnapshotByName(ctx, snapshotName)
	if err != nil {
		log.Error("failed to get snapshot by name", zap.Error(err))
		return err
	}

	// Step 2: Mark as Deleting in catalog (two-phase delete - first phase)
	// This ensures the snapshot can be cleaned up by GC if S3 deletion fails
	snapshot.State = datapb.SnapshotState_SnapshotStateDeleting
	if err := sm.catalog.SaveSnapshot(ctx, snapshot); err != nil {
		log.Error("failed to mark snapshot as deleting", zap.Error(err))
		return err
	}

	// Step 3: Remove from in-memory cache and secondary indexes (user immediately sees deletion)
	sm.snapshotID2Info.Remove(snapshot.GetId())
	sm.snapshotID2RefIndex.Remove(snapshot.GetId())
	sm.removeFromSecondaryIndexes(snapshot)

	// Step 4: Delete S3 data (may fail, GC will retry)
	if err := sm.writer.Drop(ctx, snapshot.GetS3Location()); err != nil {
		log.Warn("S3 delete failed, will be cleaned by GC",
			zap.Int64("snapshotID", snapshot.GetId()),
			zap.Error(err))
		// Return success - snapshot is already invisible to users
		// GC will clean up S3 data by finding Deleting state snapshots
		return nil
	}

	// Step 5: Delete catalog record (final cleanup)
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

// updateMetrics reports snapshot inventory gauge grouped by (collectionID, dbName).
// The getDBName callback resolves collection ID to database name, keeping snapshotMeta
// decoupled from the broader meta layer.
func (sm *snapshotMeta) updateMetrics(getDBName func(collectionID int64) string) {
	type collDBKey struct {
		collectionID string
		dbName       string
	}
	counts := make(map[collDBKey]int)

	sm.snapshotID2Info.Range(func(_ int64, info *datapb.SnapshotInfo) bool {
		collIDStr := strconv.FormatInt(info.GetCollectionId(), 10)
		counts[collDBKey{collIDStr, getDBName(info.GetCollectionId())}]++
		return true
	})

	metrics.DataCoordSnapshotTotal.Reset()
	for key, count := range counts {
		metrics.DataCoordSnapshotTotal.WithLabelValues(key.collectionID, key.dbName).Set(float64(count))
	}
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
func (sm *snapshotMeta) GetSnapshot(ctx context.Context, snapshotName string) (*datapb.SnapshotInfo, error) {
	snapshot, err := sm.getSnapshotByName(ctx, snapshotName)
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
//     SegmentIDs/IndexIDs always populated for new format snapshots)
//   - error: Error if snapshot not found or S3 read fails
func (sm *snapshotMeta) ReadSnapshotData(ctx context.Context, snapshotName string, includeSegments bool) (*SnapshotData, error) {
	log := log.Ctx(ctx).With(zap.String("snapshotName", snapshotName))

	// Step 1: Get snapshot metadata from memory to find S3 location
	snapshotInfo, err := sm.getSnapshotByName(ctx, snapshotName)
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

// getSnapshotByName is an internal helper that looks up snapshot metadata by name.
//
// This uses the snapshotName2ID index for O(1) lookup instead of scanning.
//
// Parameters:
//   - ctx: Context for cancellation (currently unused but reserved for future)
//   - snapshotName: Unique name of the snapshot to find
//
// Returns:
//   - *datapb.SnapshotInfo: Snapshot metadata if found
//   - error: Error if snapshot not found
func (sm *snapshotMeta) getSnapshotByName(ctx context.Context, snapshotName string) (*datapb.SnapshotInfo, error) {
	// O(1) lookup using name index
	snapshotID, ok := sm.snapshotName2ID.Get(snapshotName)
	if !ok {
		return nil, fmt.Errorf("snapshot %s not found", snapshotName)
	}

	// O(1) lookup from primary index
	info, ok := sm.snapshotID2Info.Get(snapshotID)
	if !ok {
		// Index inconsistency: clean up orphan name index entry
		sm.snapshotName2ID.Remove(snapshotName)
		return nil, fmt.Errorf("snapshot %s not found", snapshotName)
	}

	return info, nil
}

// GetSnapshotBySegment returns all snapshot IDs that reference a given segment.
//
// This is used for garbage collection safety: before deleting a segment, check if
// any snapshots reference it. If snapshots exist, the segment cannot be deleted.
//
// IMPORTANT: Caller should check IsRefIndexLoadedForCollection(collectionID) first.
// If RefIndexes are not loaded, results may be incomplete (returns empty for
// snapshots whose data is not available), which could lead to unsafe deletions.
//
// The lookup is O(N) where N is the number of snapshots. Each ContainsSegment()
// call is non-blocking and returns false if data is not yet loaded.
//
// Parameters:
//   - ctx: Context for cancellation (currently unused but reserved for future)
//   - collectionID: Collection ID to filter snapshots (only check this collection)
//   - segmentID: Segment ID to search for in snapshots
//
// Returns:
//   - []UniqueID: List of snapshot IDs that contain this segment (empty if none)
func (sm *snapshotMeta) GetSnapshotBySegment(ctx context.Context, collectionID, segmentID UniqueID) []UniqueID {
	snapshotIDs := make([]UniqueID, 0)

	// Scan snapshots and check if segment is in the precomputed ID set
	sm.snapshotID2Info.Range(func(id UniqueID, info *datapb.SnapshotInfo) bool {
		if info.GetCollectionId() != collectionID {
			return true // Skip, different collection
		}

		refIndex, exists := sm.snapshotID2RefIndex.Get(id)
		if !exists {
			return true // RefIndex not found (should not happen)
		}

		// ContainsSegment is non-blocking and returns false if data is not yet loaded
		if refIndex.ContainsSegment(segmentID) {
			snapshotIDs = append(snapshotIDs, id)
		}
		return true // Continue iteration
	})

	return snapshotIDs
}

// GetSnapshotByIndex returns all snapshot IDs that reference a given index.
//
// This is used for garbage collection safety: before deleting an index, check if
// any snapshots reference it. If snapshots exist, the index cannot be deleted.
//
// IMPORTANT: Caller should check IsRefIndexLoadedForCollection(collectionID) first.
// If RefIndexes are not loaded, results may be incomplete (returns empty for
// snapshots whose data is not available), which could lead to unsafe deletions.
//
// The lookup is O(N) where N is the number of snapshots. Each ContainsIndex()
// call is non-blocking and returns false if data is not yet loaded.
//
// Parameters:
//   - ctx: Context for cancellation (currently unused but reserved for future)
//   - collectionID: Collection ID to filter snapshots (only check this collection)
//   - indexID: Index ID to search for in snapshots
//
// Returns:
//   - []UniqueID: List of snapshot IDs that contain this index (empty if none)
func (sm *snapshotMeta) GetSnapshotByIndex(ctx context.Context, collectionID, indexID UniqueID) []UniqueID {
	snapshotIDs := make([]UniqueID, 0)

	// Scan snapshots and check if index is in the precomputed ID set
	sm.snapshotID2Info.Range(func(id UniqueID, info *datapb.SnapshotInfo) bool {
		if info.GetCollectionId() != collectionID {
			return true // Skip, different collection
		}

		refIndex, exists := sm.snapshotID2RefIndex.Get(id)
		if !exists {
			return true // RefIndex not found (should not happen)
		}

		// ContainsIndex is non-blocking and returns false if data is not yet loaded
		if refIndex.ContainsIndex(indexID) {
			snapshotIDs = append(snapshotIDs, id)
		}
		return true // Continue iteration
	})

	return snapshotIDs
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
	// Add to name index (thread-safe via ConcurrentMap)
	sm.snapshotName2ID.Insert(snapshotInfo.GetName(), snapshotInfo.GetId())

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
	// Remove from name index (thread-safe via ConcurrentMap)
	sm.snapshotName2ID.Remove(snapshotInfo.GetName())

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

// IsRefIndexLoadedForCollection checks if RefIndexes for a collection are loaded.
// Used by GC to check if it's safe to query snapshot references for a specific collection.
func (sm *snapshotMeta) IsRefIndexLoadedForCollection(collectionID int64) bool {
	sm.collectionIndexMu.RLock()
	snapshotIDs, ok := sm.collectionID2Snapshots.Get(collectionID)
	if !ok {
		sm.collectionIndexMu.RUnlock()
		return true // No snapshots for this collection, consider as loaded
	}

	allLoaded := true
	snapshotIDs.Range(func(snapshotID int64) bool {
		refIndex, exists := sm.snapshotID2RefIndex.Get(snapshotID)
		// Safety first:
		// - If snapshotInfo exists but refIndex is missing/not loaded, treat as not loaded to avoid unsafe GC.
		// - If snapshotInfo doesn't exist (already deleted/cleaned), ignore this snapshotID.
		if !exists {
			if _, ok := sm.snapshotID2Info.Get(snapshotID); ok {
				allLoaded = false
				return false
			}
			return true
		}

		if !refIndex.IsLoaded() {
			allLoaded = false
			return false // Stop iteration
		}
		return true
	})
	sm.collectionIndexMu.RUnlock()
	return allLoaded
}
