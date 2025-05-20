package shards

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

var errDelayFlush = errors.New("delay flush")

// asyncFlushSegment flushes the segments into the wal asynchronously.
func (m *partitionManager) asyncFlushSegment(
	ctx context.Context,
	segment *segmentAllocManager,
) {
	// create a new segment flush worker.
	w := &segmentFlushWorker{
		txnManager:   m.txnManager,
		ctx:          ctx,
		collectionID: m.collectionID,
		vchannel:     m.vchannel,
		segment:      segment,
		wal:          m.wal.Get(),
		msg:          nil,
	}
	w.SetLogger(m.Logger())
	go w.do()
}

// segmentFlusherWorker is the worker that flushes segments into the WAL.
type segmentFlushWorker struct {
	log.Binder
	txnManager   TxnManager
	ctx          context.Context
	collectionID int64
	vchannel     string
	segment      *segmentAllocManager // the segment is belong to one collection
	wal          wal.WAL
	msg          message.MutableMessage
}

// do is the main loop of the segment flush worker.
func (w *segmentFlushWorker) do() {
	backoff := backoff.NewExponentialBackOff()
	backoff.InitialInterval = 10 * time.Millisecond
	backoff.MaxInterval = 1 * time.Second
	backoff.MaxElapsedTime = 0
	backoff.Reset()

	// waitForTxnManagerRecoverReady waits for the txn manager to be ready for recovery.
	// The segment assignment manager lost the txnSem for the recovered txn message,
	// So the seal worker should wait for all the recovered txn to be done.
	// Otherwise, the flush message may be sent into wal before the txn is done.
	// Break the wal consistency: All insert message is written into wal before the flush message.
	if err := w.waitForTxnManagerRecoverDone(); err != nil {
		w.Logger().Error("failed to wait for txn manager recover ready", zap.Error(err))
		return
	}

	for {
		err := w.doOnce()
		if err == nil {
			return
		}
		if e := status.AsStreamingError(err); e.IsUnrecoverable() {
			w.Logger().Warn("flush growing segement with unrecoverable error, stop retrying", zap.Error(err))
			return
		}

		nextInterval := backoff.NextBackOff()
		w.Logger().Info("failed to allocate new growing segment, retrying", zap.Duration("nextInterval", nextInterval), zap.Error(err))
		select {
		case <-w.ctx.Done():
			w.Logger().Info("flush segment canceled", zap.Error(w.ctx.Err()))
			return
		case <-w.wal.Available():
			// wal is unavailable, stop the worker.
			w.Logger().Warn("wal is unavailable, stop flush segment")
			return
		case <-time.After(backoff.NextBackOff()):
		}
	}
}

// waitForTxnManagerRecoverDone waits for the txn manager to be recovery done.
func (w *segmentFlushWorker) waitForTxnManagerRecoverDone() error {
	select {
	case <-w.txnManager.RecoverDone():
		// txn manager is ready, continue to do the flush.
		return nil
	case <-w.ctx.Done():
		w.Logger().Info("flush segment canceled", zap.Error(w.ctx.Err()))
		return w.ctx.Err()
	case <-w.wal.Available():
		return errors.New("wal is unavailable")
	}
}

// doOnce performs the flush operation once.
func (w *segmentFlushWorker) doOnce() error {
	if !w.checkIfReady() {
		return errDelayFlush
	}
	w.generateFlushMessage()

	result, err := w.wal.Append(w.ctx, w.msg)
	if err != nil {
		w.Logger().Error("failed to append flush message", zap.Error(err))
		return err
	}
	policy := w.segment.SealPolicy()
	w.Logger().Info("segment has been flushed",
		zap.String("policy", string(policy.Policy)),
		zap.Any("extras", policy.Extra),
		zap.Any("stats", w.segment.GetFlushedStat()),
		zap.String("messageID", result.MessageID.String()),
		zap.Uint64("timetick", result.TimeTick))
	return nil
}

// checkIfReady checks if the segments are ready to be flushed.
func (w *segmentFlushWorker) checkIfReady() bool {
	// if there're flying acks, wait them acked, delay the flush at next retry.
	if ackSem := w.segment.AckSem(); ackSem > 0 {
		w.Logger().Info("segment has flying insert operation, delay it", zap.Int32("ackSem", ackSem), zap.Int64("segmentID", w.segment.GetSegmentID()))
		return false
	}
	// if there're flying txns, wait them committed, delay the flush at next retry.
	if txnSem := w.segment.TxnSem(); txnSem > 0 {
		w.Logger().Info("segment has flying txns, delay it", zap.Int32("txnSem", txnSem), zap.Int64("segmentID", w.segment.GetSegmentID()))
		return false
	}
	return true
}

// generateFlushMessage generates the flush message for the segments.
func (w *segmentFlushWorker) generateFlushMessage() {
	if w.msg != nil {
		return
	}
	w.msg = message.NewFlushMessageBuilderV2().
		WithVChannel(w.vchannel).
		WithHeader(&message.FlushMessageHeader{
			CollectionId: w.segment.GetCollectionID(),
			PartitionId:  w.segment.GetPartitionID(),
			SegmentId:    w.segment.GetSegmentID(),
		}).
		WithBody(&message.FlushMessageBody{}).MustBuildMutable()
}
