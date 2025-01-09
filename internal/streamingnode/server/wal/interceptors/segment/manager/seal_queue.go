package manager

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
)

// newSealQueue creates a new seal helper queue.
func newSealQueue(
	logger *log.MLogger,
	wal *syncutil.Future[wal.WAL],
	waitForSealed []*segmentAllocManager,
	metrics *metricsutil.SegmentAssignMetrics,
) *sealQueue {
	return &sealQueue{
		cond:          syncutil.NewContextCond(&sync.Mutex{}),
		logger:        logger,
		wal:           wal,
		waitForSealed: waitForSealed,
		waitCounter:   len(waitForSealed),
		metrics:       metrics,
	}
}

// sealQueue is a helper to seal segments.
type sealQueue struct {
	cond          *syncutil.ContextCond
	logger        *log.MLogger
	wal           *syncutil.Future[wal.WAL]
	waitForSealed []*segmentAllocManager
	waitCounter   int // wait counter count the real wait segment count, it is not equal to waitForSealed length.
	// some segments may be in sealing process.
	metrics *metricsutil.SegmentAssignMetrics
}

// AsyncSeal adds a segment into the queue, and will be sealed at next time.
func (q *sealQueue) AsyncSeal(manager ...*segmentAllocManager) {
	if q.logger.Level().Enabled(zap.DebugLevel) {
		for _, m := range manager {
			q.logger.Debug("segment is added into seal queue",
				zap.Int("collectionID", int(m.GetCollectionID())),
				zap.Int("partitionID", int(m.GetPartitionID())),
				zap.Int("segmentID", int(m.GetSegmentID())),
				zap.String("policy", string(m.SealPolicy())))
		}
	}

	q.cond.LockAndBroadcast()
	defer q.cond.L.Unlock()

	q.waitForSealed = append(q.waitForSealed, manager...)
	q.waitCounter += len(manager)
}

// SealAllWait seals all segments in the queue.
// If the operation is failure, the segments will be collected and will be retried at next time.
// Return true if all segments are sealed, otherwise return false.
func (q *sealQueue) SealAllWait(ctx context.Context) {
	q.cond.L.Lock()
	segments := q.waitForSealed
	q.waitForSealed = make([]*segmentAllocManager, 0)
	q.cond.L.Unlock()

	q.tryToSealSegments(ctx, segments...)
}

// IsEmpty returns whether the queue is empty.
func (q *sealQueue) IsEmpty() bool {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	return q.waitCounter == 0
}

// WaitCounter returns the wait counter.
func (q *sealQueue) WaitCounter() int {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	return q.waitCounter
}

// WaitUntilNoWaitSeal waits until no segment in the queue.
func (q *sealQueue) WaitUntilNoWaitSeal(ctx context.Context) error {
	// wait until the wait counter becomes 0.
	q.cond.L.Lock()
	for q.waitCounter > 0 {
		if err := q.cond.Wait(ctx); err != nil {
			return err
		}
	}
	q.cond.L.Unlock()
	return nil
}

// tryToSealSegments tries to seal segments, return the undone segments.
func (q *sealQueue) tryToSealSegments(ctx context.Context, segments ...*segmentAllocManager) {
	if len(segments) == 0 {
		return
	}
	undone, sealedSegments := q.transferSegmentStateIntoSealed(ctx, segments...)

	// send flush message into wal.
	for collectionID, vchannelSegments := range sealedSegments {
		for vchannel, segments := range vchannelSegments {
			if err := q.sendFlushSegmentsMessageIntoWAL(ctx, collectionID, vchannel, segments); err != nil {
				q.logger.Warn("fail to send flush message into wal", zap.String("vchannel", vchannel), zap.Int64("collectionID", collectionID), zap.Error(err))
				undone = append(undone, segments...)
				continue
			}
			for _, segment := range segments {
				tx := segment.BeginModification()
				tx.IntoFlushed()
				if err := tx.Commit(ctx); err != nil {
					q.logger.Warn("flushed segment failed at commit, maybe sent repeated flush message into wal", zap.Int64("segmentID", segment.GetSegmentID()), zap.Error(err))
					undone = append(undone, segment)
					continue
				}
				q.metrics.ObserveSegmentFlushed(
					string(segment.SealPolicy()),
					int64(segment.GetStat().Insert.BinarySize))
				q.logger.Info("segment has been flushed",
					zap.Int64("collectionID", segment.GetCollectionID()),
					zap.Int64("partitionID", segment.GetPartitionID()),
					zap.String("vchannel", segment.GetVChannel()),
					zap.Int64("segmentID", segment.GetSegmentID()),
					zap.String("sealPolicy", string(segment.SealPolicy())))
			}
		}
	}

	q.cond.LockAndBroadcast()
	q.waitForSealed = append(q.waitForSealed, undone...)
	// the undone one should be retried at next time, so the counter should not decrease.
	q.waitCounter -= (len(segments) - len(undone))
	q.cond.L.Unlock()
}

// transferSegmentStateIntoSealed transfers the segment state into sealed.
func (q *sealQueue) transferSegmentStateIntoSealed(ctx context.Context, segments ...*segmentAllocManager) ([]*segmentAllocManager, map[int64]map[string][]*segmentAllocManager) {
	// undone sealed segment should be done at next time.
	undone := make([]*segmentAllocManager, 0)
	sealedSegments := make(map[int64]map[string][]*segmentAllocManager)
	for _, segment := range segments {
		logger := q.logger.With(
			zap.Int64("collectionID", segment.GetCollectionID()),
			zap.Int64("partitionID", segment.GetPartitionID()),
			zap.String("vchannel", segment.GetVChannel()),
			zap.Int64("segmentID", segment.GetSegmentID()),
			zap.String("sealPolicy", string(segment.SealPolicy())))

		if segment.GetState() == streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING {
			tx := segment.BeginModification()
			tx.IntoSealed()
			if err := tx.Commit(ctx); err != nil {
				logger.Warn("seal segment failed at commit", zap.Error(err))
				undone = append(undone, segment)
				continue
			}
		}
		// assert here.
		if segment.GetState() != streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_SEALED {
			panic("unreachable code: segment should be sealed here")
		}

		// if there'are flying acks, wait them acked, delay the sealed at next retry.
		ackSem := segment.AckSem()
		if ackSem > 0 {
			undone = append(undone, segment)
			logger.Info("segment has been sealed, but there are flying acks, delay it", zap.Int32("ackSem", ackSem))
			continue
		}

		txnSem := segment.TxnSem()
		if txnSem > 0 {
			undone = append(undone, segment)
			logger.Info("segment has been sealed, but there are flying txns, delay it", zap.Int32("txnSem", txnSem))
			continue
		}

		// collect all sealed segments and no flying ack segment.
		if _, ok := sealedSegments[segment.GetCollectionID()]; !ok {
			sealedSegments[segment.GetCollectionID()] = make(map[string][]*segmentAllocManager)
		}
		if _, ok := sealedSegments[segment.GetCollectionID()][segment.GetVChannel()]; !ok {
			sealedSegments[segment.GetCollectionID()][segment.GetVChannel()] = make([]*segmentAllocManager, 0)
		}
		sealedSegments[segment.GetCollectionID()][segment.GetVChannel()] = append(sealedSegments[segment.GetCollectionID()][segment.GetVChannel()], segment)
		logger.Info("segment has been mark as sealed, can be flushed")
	}
	return undone, sealedSegments
}

// sendFlushSegmentsMessageIntoWAL sends a flush message into wal.
func (m *sealQueue) sendFlushSegmentsMessageIntoWAL(ctx context.Context, collectionID int64, vchannel string, segments []*segmentAllocManager) error {
	segmentIDs := make([]int64, 0, len(segments))
	for _, segment := range segments {
		segmentIDs = append(segmentIDs, segment.GetSegmentID())
	}
	msg, err := message.NewFlushMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.FlushMessageHeader{}).
		WithBody(&message.FlushMessageBody{
			CollectionId: collectionID,
			SegmentId:    segmentIDs,
		}).BuildMutable()
	if err != nil {
		return errors.Wrap(err, "at create new flush segments message")
	}

	msgID, err := m.wal.Get().Append(ctx, msg)
	if err != nil {
		m.logger.Warn("send flush message into wal failed", zap.Int64("collectionID", collectionID), zap.String("vchannel", vchannel), zap.Int64s("segmentIDs", segmentIDs), zap.Error(err))
		return err
	}
	m.logger.Info("send flush message into wal", zap.Int64("collectionID", collectionID), zap.String("vchannel", vchannel), zap.Int64s("segmentIDs", segmentIDs), zap.Any("msgID", msgID))
	return nil
}
