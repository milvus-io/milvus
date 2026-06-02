package shards

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
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
	}
	w.SetLogger(m.Logger())
	go w.do()
}

// segmentFlusherWorker is the worker that flushes segments into the WAL.
type segmentFlushWorker struct {
	mlog.Binder
	txnManager   TxnManager
	ctx          context.Context
	collectionID int64
	vchannel     string
	segment      *segmentAllocManager // the segment is belong to one collection
	wal          wal.WAL
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
		w.Logger().Error(w.ctx,

			"failed to wait for txn manager recover ready", mlog.Err(err))
		return
	}

	for {
		err := w.doOnce()
		if err == nil {
			return
		}
		if e := status.AsStreamingError(err); e.IsUnrecoverable() {
			w.Logger().Warn(w.ctx,

				"flush growing segement with unrecoverable error, stop retrying", mlog.Err(err))
			return
		}

		nextInterval := backoff.NextBackOff()
		w.Logger().Info(w.ctx,

			"failed to flush new growing segment, retrying", mlog.Duration("nextInterval", nextInterval), mlog.Err(err))
		select {
		case <-w.ctx.Done():
			w.Logger().Info(w.ctx,

				"flush segment canceled", mlog.Err(w.ctx.Err()))
			return
		case <-w.wal.Available():
			// wal is unavailable, stop the worker.
			w.Logger().Warn(w.ctx,

				"wal is unavailable, stop flush segment")
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
		w.Logger().Info(w.ctx,

			"flush segment canceled", mlog.Err(w.ctx.Err()))
		return w.ctx.Err()
	case <-w.wal.Available():
		return status.NewOnShutdownError("wal is unavailable")
	}
}

// doOnce performs the flush operation once.
func (w *segmentFlushWorker) doOnce() error {
	if !w.checkIfReady() {
		return errDelayFlush
	}

	// Build a fresh message each time to avoid reusing a contaminated message.
	// After a failed WAL append, the message may have internal state set (e.g., WAL term)
	// that would cause a panic if reused.
	msg := message.NewFlushMessageBuilderV2().
		WithVChannel(w.vchannel).
		WithHeader(&message.FlushMessageHeader{
			CollectionId: w.segment.GetCollectionID(),
			PartitionId:  w.segment.GetPartitionID(),
			SegmentId:    w.segment.GetSegmentID(),
		}).
		WithBody(&message.FlushMessageBody{}).MustBuildMutable()

	result, err := w.wal.Append(w.ctx, msg)
	if err != nil {
		w.Logger().Error(w.ctx,

			"failed to append flush message", mlog.FieldMessage(msg), mlog.Err(err))
		return err
	}
	policy := w.segment.SealPolicy()
	w.Logger().Info(w.ctx,

		"segment has been flushed",
		mlog.FieldMessage(msg),
		mlog.String("policy", string(policy.Policy)),
		mlog.Any("extras", policy.Extra),
		mlog.Any("stats", w.segment.GetFlushedStat()),
		mlog.String("messageID", result.MessageID.String()),
		mlog.Uint64("timetick", result.TimeTick))
	return nil
}

// checkIfReady checks if the segments are ready to be flushed.
func (w *segmentFlushWorker) checkIfReady() bool {
	// if there're flying acks, wait them acked, delay the flush at next retry.
	if ackSem := w.segment.AckSem(); ackSem > 0 {
		w.Logger().Info(w.ctx,

			"segment has flying insert operation, delay it", mlog.Int32("ackSem", ackSem), mlog.FieldSegmentID(w.segment.GetSegmentID()))
		return false
	}
	// if there're flying txns, wait them committed, delay the flush at next retry.
	if txnSem := w.segment.TxnSem(); txnSem > 0 {
		w.Logger().Info(w.ctx,

			"segment has flying txns, delay it", mlog.Int32("txnSem", txnSem), mlog.FieldSegmentID(w.segment.GetSegmentID()))
		return false
	}
	return true
}
