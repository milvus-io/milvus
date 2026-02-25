package shards

import (
	"fmt"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/policy"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
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
// It updates the schema if present. Schema updates are handled after WAL append.
func (m *shardManagerImpl) AlterCollection(msg message.ImmutableAlterCollectionMessageV2) error {
	header := msg.Header()
	collectionID := header.CollectionId
	timetick := msg.TimeTick()
	logger := m.Logger().With(log.FieldMessage(msg))

	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.checkIfCollectionExists(collectionID); err != nil {
		logger.Warn("collection not found when altering collection", zap.Int64("collectionID", collectionID))
		return err
	}

	// Update schema if present
	schema := msg.MustBody().Updates.Schema
	if schema != nil {
		collectionInfo, ok := m.collections[collectionID]
		if !ok {
			logger.Warn("collection not found when updating schema", zap.Int64("collectionID", collectionID))
			return ErrCollectionNotFound
		}

		collectionInfo.Schema = &streamingpb.CollectionSchemaOfVChannel{
			Schema:             schema,
			CheckpointTimeTick: timetick,
			State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
		}
		log.Info("UpdatedCollectionSchema", zap.Any("schema", schema), zap.Int32("schemaVersion", schema.GetVersion()))
	}

	return nil
}

func (m *shardManagerImpl) AppendNewCollectionSchema(msg message.ImmutableAlterCollectionMessageV2) error {
	header := msg.Header()
	collectionID := header.CollectionId
	schema := msg.MustBody().Updates.Schema
	timetick := msg.TimeTick()
	vchannel := msg.VChannel()

	if schema == nil {
		log.Error("schema is nil when appending collection schema",
			zap.Int64("collectionID", collectionID),
			zap.String("vchannel", vchannel),
			zap.Uint64("timetick", timetick),
			zap.String("messageType", msg.MessageType().String()))
		return fmt.Errorf("collection schema cannot be nil when appending schema for collection %d on vchannel %s", collectionID, vchannel)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	collectionInfo, ok := m.collections[collectionID]
	if !ok {
		log.Warn("collection not found when updating schema", zap.Int64("collectionID", collectionID))
		return ErrCollectionNotFound
	}

	collectionInfo.Schema = &streamingpb.CollectionSchemaOfVChannel{
		Schema:             schema,
		CheckpointTimeTick: timetick,
		State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
	}
	log.Info("UpdatedCollectionSchema", zap.Any("schema", schema), zap.Int32("schemaVersion", schema.GetVersion()))
	return nil
}

func (m *shardManagerImpl) CheckIfCollectionSchemaVersionMatch(collectionID int64, schemaVersion int32) (int32, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.checkIfCollectionSchemaVersionMatch(collectionID, schemaVersion)
}

func (m *shardManagerImpl) checkIfCollectionSchemaVersionMatch(collectionID int64, schemaVersion int32) (int32, error) {
	if _, ok := m.collections[collectionID]; !ok {
		log.Warn("collection not found", zap.Int64("collectionID", collectionID))
		return -1, ErrCollectionNotFound
	}
	collectionInfo := m.collections[collectionID]
	if collectionInfo.Schema == nil {
		log.Warn("collection schema not found", zap.Int64("collectionID", collectionID))
		return -1, ErrCollectionSchemaNotFound
	}
	collectionSchemaVersion := collectionInfo.Schema.GetSchema().GetVersion()
	if collectionSchemaVersion != schemaVersion {
		log.Warn("collection schema version not match", zap.Int64("collectionID", collectionID),
			zap.Int32("schemaVersion", schemaVersion),
			zap.Int32("collectionSchemaVersion", collectionSchemaVersion))
		return -1, ErrCollectionSchemaVersionNotMatch
	}
	log.Info("collection schema version match", zap.Int64("collectionID", collectionID), zap.Int32("schemaVersion", schemaVersion), zap.Int32("collectionSchemaVersion", collectionSchemaVersion))
	return collectionSchemaVersion, nil
}
