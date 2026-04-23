package shards

import (
	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/policy"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message/messageutil"
)

// CheckIfCollectionCanBeCreated checks if a collection can be created.
// It returns false if the collection cannot be created.
func (m *shardManagerImpl) CheckIfCollectionCanBeCreated(collectionID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.checkIfCollectionCanBeCreated(collectionID)
}

// checkIfCollectionCanBeCreated checks if a collection can be created.
func (m *shardManagerImpl) checkIfCollectionCanBeCreated(collectionID int64) error {
	if _, ok := m.collections[collectionID]; ok {
		return ErrCollectionExists
	}
	return nil
}

// CheckIfCollectionExists checks if a collection can be dropped.
func (m *shardManagerImpl) CheckIfCollectionExists(collectionID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.checkIfCollectionExists(collectionID)
}

// checkIfCollectionExists checks if a collection exists.
func (m *shardManagerImpl) checkIfCollectionExists(collectionID int64) error {
	if _, ok := m.collections[collectionID]; !ok {
		return ErrCollectionNotFound
	}
	return nil
}

// CreateCollection creates a new partition manager when create collection message is written into wal.
// After CreateCollection is called, the ddl and dml on the collection can be applied.
func (m *shardManagerImpl) CreateCollection(msg message.ImmutableCreateCollectionMessageV1) {
	collectionID := msg.Header().CollectionId
	partitionIDs := msg.Header().PartitionIds
	vchannel := msg.VChannel()
	timetick := msg.TimeTick()
	schema := msg.MustBody().GetCollectionSchema()
	logger := m.Logger().With(log.FieldMessage(msg))

	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.checkIfCollectionCanBeCreated(collectionID); err != nil {
		logger.Warn("collection already exists")
		return
	}

	collectionInfo := newCollectionInfo(vchannel, partitionIDs)
	// Set schema when creating collection
	if schema != nil {
		collectionInfo.Schema = &streamingpb.CollectionSchemaOfVChannel{
			Schema:             schema,
			CheckpointTimeTick: timetick,
			State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
		}
	}
	m.collections[collectionID] = collectionInfo

	for partitionID := range collectionInfo.PartitionIDs {
		uniqueKey := PartitionUniqueKey{CollectionID: collectionID, PartitionID: partitionID}
		if _, ok := m.partitionManagers[uniqueKey]; ok {
			logger.Warn("partition already exists", zap.Int64("partitionID", partitionID))
			continue
		}
		m.partitionManagers[uniqueKey] = newPartitionSegmentManager(
			m.ctx,
			m.Logger(),
			m.wal,
			m.pchannel,
			vchannel,
			collectionID,
			partitionID,
			make(map[int64]*segmentAllocManager),
			m.txnManager,
			timetick,
			m.metrics,
		)
	}
	logger.Info("collection created in segment assignment service", zap.Int64s("partitionIDs", partitionIDs))
	m.updateMetrics()
}

// DropCollection drops the collection and all the partitions and segments belong to it when drop collection message is written into wal.
// After DropCollection is called, no more segments can be assigned to the collection.
// Any dml and ddl for the collection will be rejected.
func (m *shardManagerImpl) DropCollection(msg message.ImmutableDropCollectionMessageV1) {
	collectionID := msg.Header().CollectionId
	logger := m.Logger().With(log.FieldMessage(msg))

	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.checkIfCollectionExists(collectionID); err != nil {
		logger.Warn("collection not exists")
		return
	}

	collectionInfo := m.collections[collectionID]
	delete(m.collections, collectionID)
	// remove all partition and segment
	partitionIDs := make([]int64, 0, len(collectionInfo.PartitionIDs))
	segmentIDs := make([]int64, 0, len(collectionInfo.PartitionIDs))
	for partitionID := range collectionInfo.PartitionIDs {
		uniqueKey := PartitionUniqueKey{CollectionID: collectionID, PartitionID: partitionID}
		pm, ok := m.partitionManagers[uniqueKey]
		if !ok {
			logger.Warn("partition not exists", zap.Int64("partitionID", partitionID))
			continue
		}
		// Flush all segments and fence assign to the partition manager.
		segments := pm.FlushAndDropPartition(policy.PolicyCollectionRemoved())
		partitionIDs = append(partitionIDs, partitionID)
		segmentIDs = append(segmentIDs, segments...)
		delete(m.partitionManagers, uniqueKey)
	}
	logger.Info("collection removed", zap.Int64s("partitionIDs", partitionIDs), zap.Int64s("segmentIDs", segmentIDs))
	m.updateMetrics()
}

// AlterCollection handles the alter collection message.
// It updates the schema if present, all within one critical region before WAL append.
func (m *shardManagerImpl) AlterCollection(msg message.MutableAlterCollectionMessageV2) ([]int64, error) {
	header := msg.Header()
	collectionID := header.CollectionId
	timetick := msg.TimeTick()
	logger := m.Logger().With(log.FieldMessage(msg))

	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.checkIfCollectionExists(collectionID); err != nil {
		logger.Warn("collection not found when altering collection", zap.Int64("collectionID", collectionID))
		return nil, err
	}

	// For schema changes: flush/fence segment allocation and update in-memory schema
	// atomically within this critical region. Both operations share the same
	// IsSchemaChange gate so live path and recovery replay stay consistent.
	var segmentIDs []int64
	if messageutil.IsSchemaChange(header) {
		var err error
		segmentIDs, err = m.flushAndFenceSegmentAllocUntil(collectionID, timetick)
		if err != nil {
			return nil, err
		}
		logger.Info("flushed segments on schema change", zap.Int64s("segmentIDs", segmentIDs))

		schema := msg.MustBody().Updates.Schema
		if schema == nil {
			// UpdateMask says schema changed but the body carries no schema —
			// malformed message; fail fast to avoid nil-pointer dereferences
			// in downstream GetSchema paths.
			logger.Error("schema change indicated by UpdateMask but schema body is nil",
				zap.Int64("collectionID", collectionID))
			return nil, errors.New("schema change message has nil schema body")
		}
		collectionInfo := m.collections[collectionID]
		collectionInfo.Schema = &streamingpb.CollectionSchemaOfVChannel{
			Schema:             schema,
			CheckpointTimeTick: timetick,
			State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
		}
		logger.Info("updated collection schema in shard manager",
			zap.Int64("collectionID", collectionID),
			zap.Int32("schemaVersion", schema.GetVersion()),
			zap.Uint64("checkpointTimeTick", timetick))
	}

	return segmentIDs, nil
}

func (m *shardManagerImpl) CheckIfCollectionSchemaVersionMatch(collectionID int64, schemaVersion int32) (int32, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.checkIfCollectionSchemaVersionMatch(collectionID, schemaVersion)
}

func (m *shardManagerImpl) checkIfCollectionSchemaVersionMatch(collectionID int64, schemaVersion int32) (int32, error) {
	collectionInfo, ok := m.collections[collectionID]
	if !ok {
		m.Logger().Warn("collection not found", zap.Int64("collectionID", collectionID))
		return -1, ErrCollectionNotFound
	}
	// Input schemaVersion 0 means the proxy did not set it (old proxy or old SDK).
	// Skip the schema presence and version checks for backward compatibility during rolling
	// upgrades, where a legacy collection may still have Schema == nil when an old proxy writes.
	if schemaVersion == 0 {
		return collectionInfo.SchemaVersion(), nil
	}
	if collectionInfo.Schema == nil || collectionInfo.Schema.GetSchema() == nil {
		m.Logger().Warn("collection schema not found", zap.Int64("collectionID", collectionID))
		return -1, ErrCollectionSchemaNotFound
	}
	collectionSchemaVersion := collectionInfo.SchemaVersion()
	if collectionSchemaVersion != schemaVersion {
		m.Logger().Warn("collection schema version not match", zap.Int64("collectionID", collectionID),
			zap.Int32("schemaVersion", schemaVersion),
			zap.Int32("collectionSchemaVersion", collectionSchemaVersion))
		return collectionSchemaVersion, ErrCollectionSchemaVersionNotMatch
	}
	return collectionSchemaVersion, nil
}
