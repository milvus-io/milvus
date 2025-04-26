package manager

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var ErrSealWorkerClosed = errors.New("seal worker is closed")

// newSealQueue creates a new seal worker.
func newSealQueue(
	logger *log.MLogger,
	wal *syncutil.Future[wal.WAL],
	txnManager *txn.TxnManager,
	metrics *metricsutil.SegmentAssignMetrics,
) *sealQueue {
	w := &sealQueue{
		backgroundTask: syncutil.NewAsyncTaskNotifier[struct{}](),
		gracefulClose:  make(chan struct{}),
		reqCh:          make(chan *asyncSealRequest),
		logger:         logger,
		wal:            wal,
		txnManager:     txnManager,
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
	txnManager     *txn.TxnManager
	metrics        *metricsutil.SegmentAssignMetrics
}

// asyncSealRequest is a request to seal segments asynchronously.
type asyncSealRequest struct {
	segments []*segmentAllocManager
}

// AsyncSeal adds a segment into the queue, and will be sealed at next time.
// The seal operation does not block the caller, and it may not success if node crash or graceful shutdown happens.
func (q *sealQueue) AsyncSeal(managers []*segmentAllocManager) error {
	if len(managers) == 0 {
		return nil
	}
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
	r := &asyncSealRequest{
		segments: managers,
	}
	select {
	case <-q.backgroundTask.Context().Done():
		return ErrSealWorkerClosed
	case q.reqCh <- r:
		return nil
	}
}

// background is a background working that will run in a separate goroutine.
func (q *sealQueue) background() {
	defer q.backgroundTask.Finish(struct{}{})
	if err := q.prepare(); err != nil {
		return
	}
	q.execute()
}

// prepare prepares the seal worker.
func (q *sealQueue) prepare() (err error) {
	pendings := q.getPendingSegmentIDs()
	defer func() {
		if err != nil {
			segmentIDs := q.getPendingSegmentIDs()
			q.logger.Error("seal worker prepare failed", zap.Int64s("pendingSegmentIDs", segmentIDs), zap.Error(err))
			return
		}
		q.logger.Info("seal worker prepare done", zap.Int64s("pendingSegmentIDs", pendings))
	}()

	// The segment assignment manager lost the txnSem for the recovered txn message,
	// So the seal worker should wait for all the recovered txn to be done.
	// Otherwise, the flush message may be sent into wal before the txn is done.
	// Break the wal consistency: All insert message is writen into wal before the flush message.
	//
	for {
		select {
		case <-q.backgroundTask.Context().Done():
			return ErrSealWorkerClosed
		case <-q.gracefulClose:
			return ErrSealWorkerClosed
		case req := <-q.reqCh:
			// before the txn manager recover is done,
			// we only collect the pending segments that need to be sealed.
			q.pendings = append(q.pendings, req)
		case <-q.txnManager.RecoverDone():
			// the txn manager is recovered, so we can start seal worker and seal all pending segments.
			q.flushAllPendings()
			return nil
		}
	}
}

// background is a background working that will run in a separate goroutine.
func (q *sealQueue) execute() {
	defer func() {
		q.flushAllPendings()
		if len(q.pendings) > 0 {
			segmentIDs := q.getPendingSegmentIDs()
			q.logger.Warn("there're some segments in pending queue after graceful closing", zap.Int("segmentCount", len(segmentIDs)), zap.Int64s("segmentIDs", segmentIDs))
		}
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
			q.pendings = append(q.pendings, req)
			q.flushAllPendings()
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
	backoff.DisableBackoff()
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
}

// tryToSealSegments tries to seal segments, return the undone segments.
func (q *sealQueue) tryToSealSegments(ctx context.Context, segments ...*segmentAllocManager) []*segmentAllocManager {
	if len(segments) == 0 {
		return nil
	}
	undone, sealedSegments := q.collectFlushableSegments(ctx, segments...)

	// send flush message into wal.
	for collectionID, vchannelSegments := range sealedSegments {
		for vchannel, segments := range vchannelSegments {
			if err := q.sendFlushSegmentsMessageIntoWAL(ctx, collectionID, vchannel, segments); err != nil {
				q.logger.Warn("fail to send flush message into wal", zap.String("vchannel", vchannel), zap.Int64("collectionID", collectionID), zap.Error(err))
				undone = append(undone, segments...)
			}
			for _, segment := range segments {
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

// collectFlushableSegments collects all flushable segments from the given segments.
func (q *sealQueue) collectFlushableSegments(ctx context.Context, segments ...*segmentAllocManager) ([]*segmentAllocManager, map[int64]map[string][]*segmentAllocManager) {
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
		// if there'are flying acks, wait them acked, delay the sealed at next retry.
		ackSem := segment.AckSem()
		if ackSem > 0 {
			undone = append(undone, segment)
			logger.Info("segment has flying acks, delay it", zap.Int32("ackSem", ackSem))
			continue
		}

		txnSem := segment.TxnSem()
		if txnSem > 0 {
			undone = append(undone, segment)
			logger.Info("segment has flying txns, delay it", zap.Int32("txnSem", txnSem))
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
		logger.Info("all message of segment has been commited, ready to flush")
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
		WithHeader(&message.FlushMessageHeader{
			CollectionId: collectionID,
			SegmentIds:   segmentIDs,
		}).
		WithBody(&message.FlushMessageBody{}).BuildMutable()
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
