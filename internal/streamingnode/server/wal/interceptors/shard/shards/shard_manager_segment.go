package shards

import (
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/stats"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/utils"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

// AssignSegmentRequest is a request to allocate segment.
type AssignSegmentRequest struct {
	CollectionID  int64
	PartitionID   int64
	InsertMetrics stats.InsertMetrics
	TimeTick      uint64
	TxnSession    TxnSession
}

// AssignSegmentResult is a result of segment allocation.
// The sum of Results.Row is equal to InserMetrics.NumRows.
type AssignSegmentResult struct {
	SegmentID   int64
	Acknowledge *atomic.Int32 // used to ack the segment assign result has been consumed
}

// Ack acks the segment assign result has been consumed.
// Must be only call once after the segment assign result has been consumed.
func (r *AssignSegmentResult) Ack() {
	r.Acknowledge.Dec()
}

// CheckIfSegmentCanBeCreated checks if a segment can be created for the specified collection and partition.
func (m *shardManagerImpl) CheckIfSegmentCanBeCreated(collectionID int64, partitionID int64, segmentID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.checkIfSegmentCanBeCreated(collectionID, partitionID, segmentID)
}

// checkIfSegmentCanBeCreated checks if a segment can be created for the specified collection and partition.
func (m *shardManagerImpl) checkIfSegmentCanBeCreated(collectionID int64, partitionID int64, segmentID int64) error {
	// segment can be created only if the collection and partition exists.
	if err := m.checkIfPartitionExists(collectionID, partitionID); err != nil {
		return err
	}

	if m := m.partitionManagers[partitionID].GetSegmentManager(segmentID); m != nil {
		return ErrSegmentExists
	}
	return nil
}

// CheckIfSegmentCanBeDropped checks if a segment can be flushed.
func (m *shardManagerImpl) CheckIfSegmentCanBeFlushed(collecionID int64, partitionID int64, segmentID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.checkIfSegmentCanBeFlushed(collecionID, partitionID, segmentID)
}

// checkIfSegmentCanBeFlushed checks if a segment can be flushed.
func (m *shardManagerImpl) checkIfSegmentCanBeFlushed(collecionID int64, partitionID int64, segmentID int64) error {
	if err := m.checkIfPartitionExists(collecionID, partitionID); err != nil {
		return err
	}

	// segment can be flushed only if the segment exists, and its state is flushed.
	// pm must exists, because we have checked the partition exists.
	pm := m.partitionManagers[partitionID]
	sm := pm.GetSegmentManager(segmentID)
	if sm == nil {
		return ErrSegmentNotFound
	}
	if !sm.IsFlushed() {
		return ErrSegmentOnGrowing
	}
	return nil
}

// CreateSegment creates a new segment manager when create segment message is written into wal.
func (m *shardManagerImpl) CreateSegment(msg message.ImmutableCreateSegmentMessageV2) {
	logger := m.Logger().With(log.FieldMessage(msg))

	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkIfSegmentCanBeCreated(msg.Header().CollectionId, msg.Header().PartitionId, msg.Header().SegmentId); err != nil {
		logger.Warn("segment already exists")
		return
	}

	s := newSegmentAllocManager(m.pchannel, msg)
	pm, ok := m.partitionManagers[s.GetPartitionID()]
	if !ok {
		panic("critical error: partition manager not found when a segment is created")
	}
	pm.AddSegment(s)
}

// FlushSegment flushes the segment when flush message is written into wal.
func (m *shardManagerImpl) FlushSegment(msg message.ImmutableFlushMessageV2) {
	collectionID := msg.Header().CollectionId
	partitionID := msg.Header().PartitionId
	segmentID := msg.Header().SegmentId
	logger := m.Logger().With(log.FieldMessage(msg))

	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkIfSegmentCanBeFlushed(collectionID, partitionID, segmentID); err != nil {
		logger.Warn("segment can not be flushed", zap.Error(err))
		return
	}

	pm := m.partitionManagers[partitionID]
	pm.MustRemoveFlushedSegment(segmentID)
}

// AssignSegment assigns a segment for a assign segment request.
func (m *shardManagerImpl) AssignSegment(req *AssignSegmentRequest) (*AssignSegmentResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	pm, ok := m.partitionManagers[req.PartitionID]
	if !ok {
		return nil, ErrPartitionNotFound
	}
	result, err := pm.AssignSegment(req)
	if err == nil {
		m.metrics.ObserveInsert(req.InsertMetrics.Rows, req.InsertMetrics.BinarySize)
		return result, nil
	}
	return nil, err
}

// ApplyDelete: TODO move the L0 flush operation here.
func (m *shardManagerImpl) ApplyDelete(msg message.MutableDeleteMessageV1) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.metrics.ObserveDelete(msg.Header().GetRows())
	return nil
}

// WaitUntilGrowingSegmentReady waits until the growing segment is ready.
func (m *shardManagerImpl) WaitUntilGrowingSegmentReady(collectionID int64, partitonID int64) (<-chan struct{}, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.checkIfPartitionExists(collectionID, partitonID); err != nil {
		return nil, err
	}
	return m.partitionManagers[partitonID].WaitPendingGrowingSegmentReady(), nil
}

// FlushAndFenceSegmentAllocUntil flush all segment that contains the message which timetick is less than the incoming timetick.
// It will be used for message like ManualFlush, SchemaChange operations that want the exists segment to be flushed.
// !!! The returned segmentIDs may be is on-flushing state(which is on-flushing, a segmentFlushWorker is running, but not send into wal yet)
// !!! The caller should promise the returned segmentIDs to be flushed.
func (m *shardManagerImpl) FlushAndFenceSegmentAllocUntil(collectionID int64, timetick uint64) ([]int64, error) {
	logger := m.Logger().With(zap.Int64("collectionID", collectionID), zap.Uint64("timetick", timetick))
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.checkIfCollectionExists(collectionID); err != nil {
		logger.Warn("collection not found when FlushAndFenceSegmentAllocUntil", zap.Error(err))
		return nil, err
	}

	collectionInfo := m.collections[collectionID]
	segmentIDs := make([]int64, 0, len(collectionInfo.PartitionIDs))
	// collect all partitions
	for partitionID := range collectionInfo.PartitionIDs {
		// Seal all segments and fence assign to the partition manager.
		pm, ok := m.partitionManagers[partitionID]
		if !ok {
			logger.Warn("partition not found when FlushAndFenceSegmentAllocUntil", zap.Int64("partitionID", partitionID))
			continue
		}
		newSealedSegments := pm.FlushAndFenceSegmentUntil(timetick)
		segmentIDs = append(segmentIDs, newSealedSegments...)
	}
	logger.Info("segments should be flushed when FlushAndFenceSegmentAllocUntil", zap.Int64s("segmentIDs", segmentIDs))
	return segmentIDs, nil
}

// AsyncFlushSegment triggers the segment to be flushed when flush message is written into wal.
func (m *shardManagerImpl) AsyncFlushSegment(signal utils.SealSegmentSignal) {
	logger := m.Logger().With(
		zap.Int64("collectionID", signal.SegmentBelongs.CollectionID),
		zap.Int64("partitionID", signal.SegmentBelongs.PartitionID),
		zap.Int64("segmentID", signal.SegmentBelongs.SegmentID),
	)
	m.mu.Lock()
	defer m.mu.Unlock()

	pm, ok := m.partitionManagers[signal.SegmentBelongs.PartitionID]
	if !ok {
		logger.Warn("partition not found when AsyncMustSeal, may be already dropped")
		return
	}
	if err := pm.AsyncFlushSegment(signal); err != nil {
		logger.Warn("segment not found when AsyncMustSeal, may be already sealed", zap.Error(err))
	}
}
