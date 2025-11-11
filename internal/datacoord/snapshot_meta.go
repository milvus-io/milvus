package datacoord

import (
	"context"
	"fmt"
	"slices"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// Snapshot Metadata Management
//
// This file implements the metadata layer for collection snapshot management in DataCoord.
// Snapshots are point-in-time captures of collection state used for backup and restore operations.
//
// ARCHITECTURE:
// - snapshotMeta: Central metadata manager coordinating catalog persistence and S3 storage
// - SnapshotDataInfo: In-memory cache of snapshot metadata with fast segment/index lookup
// - Catalog: Persistent storage of snapshot metadata in etcd/KV store
// - Reader/Writer: S3 operations for reading/writing complete snapshot data
//
// METADATA ORGANIZATION:
// - Memory: snapshotID -> SnapshotDataInfo (with segment/index ID sets for fast lookup)
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

// SnapshotDataInfo holds cached snapshot metadata with precomputed ID sets for fast lookup.
//
// This structure is used in the in-memory cache to enable efficient queries like:
//   - Which snapshots contain segment X?
//   - Which snapshots reference index Y?
//
// The ID sets are populated during snapshot creation and reload, avoiding the need
// to scan segment/index lists on every query.
type SnapshotDataInfo struct {
	snapshotInfo *datapb.SnapshotInfo // Basic snapshot metadata (name, ID, collection, partitions, S3 location)
	SegmentIDs   typeutil.UniqueSet   // Set of all segment IDs in this snapshot (for fast lookup)
	IndexIDs     typeutil.UniqueSet   // Set of all index IDs in this snapshot (for fast lookup)
}

// snapshotMeta manages snapshot metadata both in memory and persistent storage.
//
// This is the central coordinator for snapshot operations, maintaining:
//   - In-memory cache for fast lookup and reference tracking
//   - Catalog persistence for durability across restarts
//   - S3 reader/writer for complete snapshot data
type snapshotMeta struct {
	catalog metastore.DataCoordCatalog // Persistent storage in etcd/KV store for snapshot metadata

	// In-memory cache: snapshot ID -> snapshot reference info
	// Enables fast lookup and reference tracking (which snapshots contain a given segment/index)
	// Thread-safe for concurrent access
	snapshotID2DataInfo *typeutil.ConcurrentMap[UniqueID, *SnapshotDataInfo]

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
	sm := &snapshotMeta{
		catalog:             catalog,
		snapshotID2DataInfo: typeutil.NewConcurrentMap[UniqueID, *SnapshotDataInfo](),
		reader:              NewSnapshotReader(chunkManager),
		writer:              NewSnapshotWriter(chunkManager),
	}

	// Reload all snapshots from catalog to populate in-memory cache
	if err := sm.reload(ctx); err != nil {
		log.Error("failed to reload snapshot meta from kv", zap.Error(err))
		return nil, err
	}

	return sm, nil
}

// reload rebuilds the in-memory cache from catalog and S3 during DataCoord startup.
//
// This function is critical for recovering snapshot state after DataCoord restarts.
// It reads snapshot metadata from catalog (etcd) and builds the in-memory cache
// with segment/index ID sets for fast lookup.
//
// Process flow:
//  1. List all snapshots from catalog (persistent storage)
//  2. For each snapshot:
//     a. Read complete snapshot data from S3 to get segment/index lists
//     b. Extract segment IDs and index IDs
//     c. Build SnapshotDataInfo with ID sets for fast lookup
//     d. Insert into in-memory cache
//
// Note: We must read from S3 to get the complete segment/index lists for building
// the reference tracking sets. The catalog only stores basic metadata (name, ID, S3 location).
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//
// Returns:
//   - error: Error if catalog list fails or any S3 read fails
func (sm *snapshotMeta) reload(ctx context.Context) error {
	// Step 1: List all snapshots from catalog
	snapshots, err := sm.catalog.ListSnapshots(ctx)
	if err != nil {
		log.Info("failed to list snapshots from kv", zap.Error(err))
		return err
	}

	// Step 2: For each snapshot, read from S3 and build in-memory cache
	for _, snapshot := range snapshots {
		log.Info("reload snapshot from catalog",
			zap.String("name", snapshot.GetName()),
			zap.Int64("id", snapshot.GetId()),
			zap.String("s3_location", snapshot.GetS3Location()))

		// Read complete snapshot data from S3 to get segment/index lists
		snapshotData, err := sm.reader.ReadSnapshot(ctx, snapshot.GetCollectionId(), snapshot.GetId(), true)
		if err != nil {
			log.Error("failed to read snapshot from s3", zap.Error(err))
			return err
		}

		// Extract segment IDs and index IDs for reference tracking
		segmentIDs := make([]int64, 0)
		indexIDs := make([]int64, 0)
		for _, segment := range snapshotData.Segments {
			segmentIDs = append(segmentIDs, segment.GetSegmentId())
		}
		for _, index := range snapshotData.Indexes {
			indexIDs = append(indexIDs, index.GetIndexID())
		}

		// Insert into in-memory cache with precomputed ID sets
		sm.snapshotID2DataInfo.Insert(snapshot.GetId(), &SnapshotDataInfo{
			snapshotInfo: snapshot,
			SegmentIDs:   typeutil.NewUniqueSet(segmentIDs...),
			IndexIDs:     typeutil.NewUniqueSet(indexIDs...),
		})
	}

	return nil
}

// SaveSnapshot persists a new snapshot to both S3 and catalog.
//
// This is the main entry point for creating a new snapshot. It follows a two-phase
// approach: first save the complete data to S3, then save the metadata to catalog.
// This ordering ensures that catalog never references non-existent S3 data.
//
// Process flow:
//  1. Save complete snapshot data (segments, indexes, schema) to S3
//  2. Record S3 location in snapshot metadata
//  3. Extract segment IDs and index IDs for reference tracking
//  4. Insert into in-memory cache with ID sets
//  5. Save snapshot metadata to catalog (persistent storage)
//
// Transaction semantics:
//   - S3 write is durable before catalog write
//   - Catalog write is atomic (etcd transaction)
//   - If catalog write fails, S3 data becomes orphaned but doesn't affect correctness
//   - Recovery: orphaned S3 data will be cleaned by GC
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - snapshot: Complete snapshot data including segments, indexes, and schema
//
// Returns:
//   - error: Error if S3 write or catalog write fails
func (sm *snapshotMeta) SaveSnapshot(ctx context.Context, snapshot *SnapshotData) error {
	// Step 1: Save complete snapshot data to S3 first (durable storage)
	metadataFilePath, err := sm.writer.Save(ctx, snapshot)
	if err != nil {
		log.Error("failed to save snapshot to s3", zap.Error(err))
		return err
	}
	snapshot.SnapshotInfo.S3Location = metadataFilePath

	// Step 2: Extract segment IDs and index IDs for reference tracking
	segmentIDs := make([]int64, 0)
	indexIDs := make([]int64, 0)
	for _, segment := range snapshot.Segments {
		segmentIDs = append(segmentIDs, segment.GetSegmentId())
	}
	for _, index := range snapshot.Indexes {
		indexIDs = append(indexIDs, index.GetIndexID())
	}

	// Step 3: Insert into in-memory cache with precomputed ID sets
	sm.snapshotID2DataInfo.Insert(snapshot.SnapshotInfo.GetId(), &SnapshotDataInfo{
		snapshotInfo: snapshot.SnapshotInfo,
		SegmentIDs:   typeutil.NewUniqueSet(segmentIDs...),
		IndexIDs:     typeutil.NewUniqueSet(indexIDs...),
	})

	// Step 4: Save metadata to catalog (atomic, persistent)
	return sm.catalog.SaveSnapshot(ctx, snapshot.SnapshotInfo)
}

// DropSnapshot removes a snapshot from memory, catalog, and S3.
//
// This completely deletes a snapshot and all its associated data. The operation
// removes data in reverse order of SaveSnapshot to maintain consistency.
//
// Process flow:
//  1. Lookup snapshot by name in memory cache
//  2. Remove from in-memory cache (fast, prevents new references)
//  3. Delete metadata from catalog (persistent storage)
//  4. Delete snapshot data from S3 (complete data including segments/indexes)
//
// Error handling:
//   - If catalog delete fails, in-memory state is inconsistent but will be fixed on restart
//   - If S3 delete fails, orphaned data remains but won't be referenced
//   - Both failures are logged but don't prevent operation completion
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - snapshotName: Unique name of the snapshot to delete
//
// Returns:
//   - error: Error if snapshot not found, catalog delete fails, or S3 delete fails
func (sm *snapshotMeta) DropSnapshot(ctx context.Context, snapshotName string) error {
	log := log.Ctx(ctx).With(zap.String("snapshotName", snapshotName))

	// Step 1: Lookup snapshot by name
	snapshot, err := sm.getSnapshotByName(ctx, snapshotName)
	if err != nil {
		log.Error("failed to get snapshot by name", zap.Error(err))
		return err
	}

	// Step 2: Remove from in-memory cache first (prevents new references)
	sm.snapshotID2DataInfo.Remove(snapshot.GetId())

	// Step 3: Delete metadata from catalog (persistent storage)
	err = sm.catalog.DropSnapshot(ctx, snapshot.GetCollectionId(), snapshot.GetId())
	if err != nil {
		log.Error("failed to drop snapshot from catalog", zap.Error(err))
		return err
	}

	// Step 4: Delete complete snapshot data from S3
	err = sm.writer.Drop(ctx, snapshot.GetCollectionId(), snapshot.GetId())
	if err != nil {
		log.Error("failed to drop snapshot from s3", zap.Error(err))
		return err
	}

	return nil
}

// ListSnapshots returns snapshot names filtered by collection and/or partition.
//
// This provides fast snapshot listing by querying the in-memory cache without
// accessing catalog or S3. Supports flexible filtering:
//   - collectionID <= 0: List all collections
//   - collectionID > 0: List only this collection
//   - partitionID <= 0: List all partitions in the collection
//   - partitionID > 0: List only snapshots containing this partition
//
// The function scans all snapshots in memory and applies filter criteria.
// This is efficient because the cache is typically small (hundreds of snapshots).
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

	// Scan in-memory cache and apply filters
	sm.snapshotID2DataInfo.Range(func(key UniqueID, value *SnapshotDataInfo) bool {
		snapshotInfo := value.snapshotInfo

		// Check collection filter
		collectionMatch := snapshotInfo.GetCollectionId() == collectionID || collectionID <= 0

		// Check partition filter (snapshot contains this partition)
		partitionMatch := partitionID <= 0 || slices.Contains(snapshotInfo.GetPartitionIds(), partitionID)

		if collectionMatch && partitionMatch {
			ret = append(ret, snapshotInfo.GetName())
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
//
// Returns:
//   - *SnapshotData: Complete snapshot data with segments, indexes, and schema
//   - error: Error if snapshot not found or S3 read fails
func (sm *snapshotMeta) ReadSnapshotData(ctx context.Context, snapshotName string) (*SnapshotData, error) {
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

	// Step 2: Read complete snapshot data from S3
	snapshotData, err := sm.reader.ReadSnapshot(ctx, snapshotInfo.GetCollectionId(), snapshotInfo.GetId(), true)
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
// This scans the in-memory cache to find a snapshot with matching name.
// The scan is typically fast because the cache is small (hundreds of snapshots).
//
// Parameters:
//   - ctx: Context for cancellation (currently unused but reserved for future)
//   - snapshotName: Unique name of the snapshot to find
//
// Returns:
//   - *datapb.SnapshotInfo: Snapshot metadata if found
//   - error: Error if snapshot not found
func (sm *snapshotMeta) getSnapshotByName(ctx context.Context, snapshotName string) (*datapb.SnapshotInfo, error) {
	var ret *datapb.SnapshotInfo

	// Scan in-memory cache for matching name
	sm.snapshotID2DataInfo.Range(func(key UniqueID, value *SnapshotDataInfo) bool {
		if value.snapshotInfo.GetName() == snapshotName {
			ret = value.snapshotInfo
			return false // Stop iteration
		}
		return true // Continue iteration
	})

	if ret == nil {
		return nil, fmt.Errorf("snapshot %s not found", snapshotName)
	}
	return ret, nil
}

// GetSnapshotBySegment returns all snapshot IDs that reference a given segment.
//
// This is used for garbage collection safety: before deleting a segment, check if
// any snapshots reference it. If snapshots exist, the segment cannot be deleted.
//
// The lookup is O(N) where N is the number of snapshots, but uses the precomputed
// SegmentIDs sets in SnapshotDataInfo for fast membership testing (O(1) per snapshot).
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
	sm.snapshotID2DataInfo.Range(func(key UniqueID, value *SnapshotDataInfo) bool {
		if value.snapshotInfo.GetCollectionId() == collectionID && value.SegmentIDs.Contain(segmentID) {
			snapshotIDs = append(snapshotIDs, key)
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
// The lookup is O(N) where N is the number of snapshots, but uses the precomputed
// IndexIDs sets in SnapshotDataInfo for fast membership testing (O(1) per snapshot).
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
	sm.snapshotID2DataInfo.Range(func(key UniqueID, value *SnapshotDataInfo) bool {
		if value.snapshotInfo.GetCollectionId() == collectionID && value.IndexIDs.Contain(indexID) {
			snapshotIDs = append(snapshotIDs, key)
		}
		return true // Continue iteration
	})

	return snapshotIDs
}
