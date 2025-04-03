package manager

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var ErrSealWorkerClosed = errors.New("seal worker is closed")

// newSealQueue creates a new seal worker.
func newSealQueue(
	logger *log.MLogger,
	wal *syncutil.Future[wal.WAL],
	metrics *metricsutil.SegmentAssignMetrics,
) *sealQueue {
	w := &sealQueue{
		backgroundTask: syncutil.NewAsyncTaskNotifier[struct{}](),
		gracefulClose:  make(chan struct{}),
		reqCh:          make(chan *asyncSealRequest),
		logger:         logger,
		wal:            wal,
		metrics:        metrics,
	}
	go w.background()
	return w
}

// sealQueue is a worker that seals segments asynchronously.
type sealQueue struct {
	backgroundTask *syncutil.AsyncTaskNotifier[struct{}]
	pendings       []*asyncSealRequest
	gracefulClose  chan struct{}
	reqCh          chan *asyncSealRequest
	logger         *log.MLogger
	wal            *syncutil.Future[wal.WAL]
	metrics        *metricsutil.SegmentAssignMetrics
}

// asyncSealRequest is a request to seal segments asynchronously.
type asyncSealRequest struct {
	segments []*segmentAllocManager
	result   *syncutil.Future[error]
}

// Seal seals the segments in the queue.
func (q *sealQueue) Seal(ctx context.Context, manager []*segmentAllocManager) error {
	if len(manager) == 0 {
		return nil
	}
	resultFuture := q.AsyncSeal(ctx, manager)
	result, err := resultFuture.GetWithContext(ctx)
	if err != nil {
		return err
	}
	return result
}

// AsyncSeal adds a segment into the queue, and will be sealed at next time.
func (q *sealQueue) AsyncSeal(ctx context.Context, managers []*segmentAllocManager) *syncutil.Future[error] {
	if q.logger.Level().Enabled(zap.DebugLevel) {
		for _, m := range managers {
			policy := m.SealPolicy()
			q.logger.Debug("segment is added into seal queue",
				zap.Int("collectionID", int(m.GetCollectionID())),
				zap.Int("partitionID", int(m.GetPartitionID())),
				zap.Int("segmentID", int(m.GetSegmentID())),
				zap.String("policy", string(policy.Policy)),
				zap.Any("policyExtra", policy.Extra),
			)
		}
	}
	result := syncutil.NewFuture[error]()
	r := &asyncSealRequest{
		segments: managers,
		result:   result,
	}
	select {
	case <-q.backgroundTask.Context().Done():
		result.Set(ErrSealWorkerClosed)
	case <-ctx.Done():
		result.Set(ctx.Err())
	case q.reqCh <- r:
	}
	return result
}

// background is a background working that will run in a separate goroutine.
func (q *sealQueue) background() {
	defer func() {
		q.flushAllPendings()
		if len(q.pendings) > 0 {
			segmentIDs := q.getPendingSegmentIDs()
			q.logger.Warn("there're some segments in pending queue after graceful closing", zap.Int("segmentCount", len(segmentIDs)), zap.Int64s("segmentIDs", segmentIDs))
			for _, req := range q.pendings {
				req.result.Set(ErrSealWorkerClosed)
			}
		}
		q.backgroundTask.Finish(struct{}{})
	}()

	backoff := typeutil.NewBackoffTimer(typeutil.BackoffTimerConfig{
		Default: time.Second,
		Backoff: typeutil.BackoffConfig{
			InitialInterval: 20 * time.Millisecond,
			Multiplier:      2,
			MaxInterval:     time.Second,
		},
	})
	// backoffTimer will enabled if there's any pending segments that's need to be flushed.
	var backoffTimer <-chan time.Time
	for {
		select {
		case <-q.backgroundTask.Context().Done():
			return
		case <-q.gracefulClose:
			q.logger.Info("seal worker is on graceful closing")
			return
		case req := <-q.reqCh:
			q.handleSealRequest(q.backgroundTask.Context(), req)
			if len(req.segments) > 0 {
				// if there's still any segments need to be sealed, add it into pending queue and wait backoff.
				q.pendings = append(q.pendings, req)
			}
		case <-backoffTimer:
			q.flushAllPendings()
		}
		backoffTimer = q.startBackoffOrNot(backoff)
	}
}

// Close closes the seal worker.
func (q *sealQueue) Close() {
	q.logger.Info("seal worker is on graceful closing")
	// perform a graceful close first.
	close(q.gracefulClose)

	// wait for the background task to finish.
	select {
	case <-time.After(gracefulCloseTimeout):
		q.logger.Warn("seal worker is not closed after graceful close, force to close it")
	case <-q.backgroundTask.FinishChan():
	}
	q.backgroundTask.Cancel()
	q.backgroundTask.BlockUntilFinish()
	q.logger.Info("seal worker is closed")
}

// startBackoffOrNot starts the backoff timer or not.
func (q *sealQueue) startBackoffOrNot(backoff *typeutil.BackoffTimer) <-chan time.Time {
	if len(q.pendings) > 0 {
		backoff.EnableBackoff()
		backOffTimer, nextInterval := backoff.NextTimer()
		q.logger.Info("there're some segment need to be sealed in pending queue",
			zap.Duration("nextInterval", nextInterval),
			zap.Int("pendingCount", len(q.pendings)),
			zap.Int64s("segmentIDs", q.getPendingSegmentIDs()),
		)
		return backOffTimer
	}
	return nil
}

// flushAllPendings flushes all pendings.
func (q *sealQueue) flushAllPendings() {
	newPendings := make([]*asyncSealRequest, 0, len(q.pendings))
	for _, req := range q.pendings {
		q.handleSealRequest(q.backgroundTask.Context(), req)
		if len(req.segments) > 0 {
			newPendings = append(newPendings, req)
		}
	}
	q.pendings = newPendings
}

// getPendingSegmentIDs returns the pending segment IDs.
func (q *sealQueue) getPendingSegmentIDs() []int64 {
	segmentIDs := make([]int64, 0, len(q.pendings))
	for _, req := range q.pendings {
		for _, segment := range req.segments {
			segmentIDs = append(segmentIDs, segment.GetSegmentID())
		}
	}
	return segmentIDs
}

// handleSealRequest handles the seal request.
func (q *sealQueue) handleSealRequest(ctx context.Context, req *asyncSealRequest) {
	// try to seal segments, return the undone segments.
	req.segments = q.tryToSealSegments(ctx, req.segments...)
	if len(req.segments) == 0 {
		req.result.Set(nil)
	}
}

// tryToSealSegments tries to seal segments, return the undone segments.
func (q *sealQueue) tryToSealSegments(ctx context.Context, segments ...*segmentAllocManager) []*segmentAllocManager {
	if len(segments) == 0 {
		return nil
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
				policy := segment.SealPolicy()
				q.metrics.ObserveSegmentFlushed(
					string(policy.Policy),
					int64(segment.GetStat().Insert.BinarySize))
				q.logger.Info("segment has been flushed",
					zap.Int64("collectionID", segment.GetCollectionID()),
					zap.Int64("partitionID", segment.GetPartitionID()),
					zap.String("vchannel", segment.GetVChannel()),
					zap.Int64("segmentID", segment.GetSegmentID()),
					zap.String("sealPolicy", string(policy.Policy)),
					zap.Any("sealPolicyExtra", policy.Extra),
				)
			}
		}
	}
	return undone
}

// transferSegmentStateIntoSealed transfers the segment state into sealed.
func (q *sealQueue) transferSegmentStateIntoSealed(ctx context.Context, segments ...*segmentAllocManager) ([]*segmentAllocManager, map[int64]map[string][]*segmentAllocManager) {
	// undone sealed segment should be done at next time.
	undone := make([]*segmentAllocManager, 0)
	sealedSegments := make(map[int64]map[string][]*segmentAllocManager)
	for _, segment := range segments {
		policy := segment.SealPolicy()
		logger := q.logger.With(
			zap.Int64("collectionID", segment.GetCollectionID()),
			zap.Int64("partitionID", segment.GetPartitionID()),
			zap.String("vchannel", segment.GetVChannel()),
			zap.Int64("segmentID", segment.GetSegmentID()),
			zap.String("sealPolicy", string(policy.Policy)),
			zap.Any("sealPolicyExtra", policy.Extra),
		)
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
