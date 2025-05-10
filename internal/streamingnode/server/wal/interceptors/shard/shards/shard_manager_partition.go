package shards

import (
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/policy"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

// CheckIfPartitionCanBeCreated checks if a partition can be created.
func (m *ShardManager) CheckIfPartitionCanBeCreated(collectionID int64, partitionID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.checkIfPartitionCanBeCreated(collectionID, partitionID)
}

// checkIfPartitionCanBeCreated checks if a partition can be created.
func (m *ShardManager) checkIfPartitionCanBeCreated(collectionID int64, partitionID int64) error {
	if _, ok := m.collections[collectionID]; !ok {
		return ErrCollectionNotFound
	}

	if _, ok := m.collections[collectionID].PartitionIDs[partitionID]; ok {
		return ErrPartitionExists
	}
	return nil
}

// CheckIfPartitionExists checks if a partition can be dropped.
func (m *ShardManager) CheckIfPartitionExists(collectionID int64, partitionID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.checkIfPartitionExists(collectionID, partitionID)
}

// checkIfPartitionExists checks if a partition can be dropped.
func (m *ShardManager) checkIfPartitionExists(collectionID int64, partitionID int64) error {
	if _, ok := m.collections[collectionID]; !ok {
		return ErrCollectionNotFound
	}
	if _, ok := m.collections[collectionID].PartitionIDs[partitionID]; !ok {
		return ErrPartitionNotFound
	}
	return nil
}

// CreatePartition creates a new partition manager when create partition message is written into wal.
// After CreatePartition is called, the dml on the partition can be applied.
func (m *ShardManager) CreatePartition(msg message.ImmutableCreatePartitionMessageV1) {
	collectionID := msg.Header().CollectionId
	partitionID := msg.Header().PartitionId
	tiemtick := msg.TimeTick()
	logger := m.Logger().With(log.FieldMessage(msg))

	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.checkIfPartitionCanBeCreated(collectionID, partitionID); err != nil {
		logger.Warn("partition can not be created", zap.Error(err))
		return
	}

	m.collections[collectionID].PartitionIDs[partitionID] = struct{}{}
	if _, ok := m.managers[partitionID]; ok {
		logger.Warn("partition manager already exists")
		return
	}
	m.managers[partitionID] = newPartitionSegmentManager(
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
	)
	m.Logger().Info("partition created")
	m.updateMetrics()
}

// DropPartition drops a partition manager when drop partition message is written into wal.
// After DropPartition is called, the dml on the partition can not be applied.
func (m *ShardManager) DropPartition(msg message.ImmutableDropPartitionMessageV1) {
	collectionID := msg.Header().CollectionId
	partitionID := msg.Header().PartitionId
	logger := m.Logger().With(log.FieldMessage(msg))

	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.checkIfPartitionExists(collectionID, partitionID); err != nil {
		logger.Warn("partition can not be dropped", zap.Error(err))
		return
	}
	delete(m.collections[collectionID].PartitionIDs, partitionID)

	pm, ok := m.managers[partitionID]
	if !ok {
		logger.Warn("partition not exists", zap.Int64("collectionID", collectionID), zap.Int64("partitionID", partitionID))
		return
	}

	delete(m.managers, partitionID)
	segmentIDs := pm.FlushAndDropPartition(policy.PolicyPartitionRemoved())
	m.Logger().Info("partition removed", zap.Int64s("segmentIDs", segmentIDs))
	m.updateMetrics()
}
