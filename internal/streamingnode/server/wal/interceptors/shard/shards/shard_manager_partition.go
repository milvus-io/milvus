package shards

import (
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/policy"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// CheckIfPartitionCanBeCreated checks if a partition can be created.
func (m *shardManagerImpl) CheckIfPartitionCanBeCreated(uniquePartitionKey PartitionUniqueKey) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.checkIfPartitionCanBeCreated(uniquePartitionKey)
}

// checkIfPartitionCanBeCreated checks if a partition can be created.
func (m *shardManagerImpl) checkIfPartitionCanBeCreated(uniquePartitionKey PartitionUniqueKey) error {
	if _, ok := m.collections[uniquePartitionKey.CollectionID]; !ok {
		return ErrCollectionNotFound
	}

	if _, ok := m.collections[uniquePartitionKey.CollectionID].PartitionIDs[uniquePartitionKey.PartitionID]; ok {
		return ErrPartitionExists
	}
	return nil
}

// CheckIfPartitionExists checks if a partition can be dropped.
func (m *shardManagerImpl) CheckIfPartitionExists(uniquePartitionKey PartitionUniqueKey) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.checkIfPartitionExists(uniquePartitionKey)
}

// checkIfPartitionExists checks if a partition can be dropped.
func (m *shardManagerImpl) checkIfPartitionExists(uniquePartitionKey PartitionUniqueKey) error {
	if _, ok := m.collections[uniquePartitionKey.CollectionID]; !ok {
		return ErrCollectionNotFound
	}
	if _, ok := m.collections[uniquePartitionKey.CollectionID].PartitionIDs[uniquePartitionKey.PartitionID]; !ok {
		return ErrPartitionNotFound
	}
	return nil
}

// CreatePartition creates a new partition manager when create partition message is written into wal.
// After CreatePartition is called, the dml on the partition can be applied.
func (m *shardManagerImpl) CreatePartition(msg message.ImmutableCreatePartitionMessageV1) {
	collectionID := msg.Header().CollectionId
	partitionID := msg.Header().PartitionId
	tiemtick := msg.TimeTick()
	logger := m.Logger().With(mlog.FieldMessage(msg))

	m.mu.Lock()
	defer m.mu.Unlock()

	uniquePartitionKey := PartitionUniqueKey{CollectionID: collectionID, PartitionID: partitionID}
	if err := m.checkIfPartitionCanBeCreated(uniquePartitionKey); err != nil {
		logger.Warn(m.ctx, "partition can not be created", mlog.Err(err))
		return
	}

	m.collections[collectionID].PartitionIDs[partitionID] = struct{}{}
	if _, ok := m.partitionManagers[uniquePartitionKey]; ok {
		logger.Warn(m.ctx, "partition manager already exists")
		return
	}
	m.partitionManagers[uniquePartitionKey] = newPartitionSegmentManager(
		m.ctx,
		m.Logger(),
		m.wal,
		m.pchannel,
		m.collections[collectionID].VChannel,
		collectionID,
		partitionID,
		make(map[int64]*segmentAllocManager),
		m.txnManager,
		tiemtick,
		m.metrics,
	)
	m.Logger().Info(m.ctx, "partition created")
	m.updateMetrics()
}

// DropPartition drops a partition manager when drop partition message is written into wal.
// After DropPartition is called, the dml on the partition can not be applied.
func (m *shardManagerImpl) DropPartition(msg message.ImmutableDropPartitionMessageV1) {
	collectionID := msg.Header().CollectionId
	partitionID := msg.Header().PartitionId
	logger := m.Logger().With(mlog.FieldMessage(msg))

	m.mu.Lock()
	defer m.mu.Unlock()

	uniquePartitionKey := PartitionUniqueKey{CollectionID: collectionID, PartitionID: partitionID}
	if err := m.checkIfPartitionExists(uniquePartitionKey); err != nil {
		logger.Warn(m.ctx, "partition can not be dropped", mlog.Err(err))
		return
	}
	delete(m.collections[collectionID].PartitionIDs, partitionID)

	pm, ok := m.partitionManagers[uniquePartitionKey]
	if !ok {
		logger.Warn(m.ctx, "partition not exists", mlog.FieldCollectionID(collectionID), mlog.FieldPartitionID(partitionID))
		return
	}

	delete(m.partitionManagers, uniquePartitionKey)
	segmentIDs := pm.FlushAndDropPartition(policy.PolicyPartitionRemoved())
	m.Logger().Info(m.ctx, "partition removed", mlog.Int64s("segmentIDs", segmentIDs))
	m.updateMetrics()
}
