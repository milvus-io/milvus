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
	go func() {
		l, err := m.wal.GetWithContext(ctx)
		if err != nil {
			m.Logger().Info(ctx, "stop flushing segment before wal is ready",
				mlog.FieldVChannel(m.vchannel),
				mlog.FieldCollectionID(m.collectionID),
				mlog.FieldPartitionID(m.partitionID),
				mlog.FieldSegmentID(segment.GetSegmentID()),
				mlog.Err(err))
			return
		}

		// create a new segment flush worker.
		w := &segmentFlushWorker{
			txnManager:   m.txnManager,
			ctx:          ctx,
			collectionID: m.collectionID,
			partitionID:  m.partitionID,
			vchannel:     m.vchannel,
			segment:      segment,
			wal:          l,
		}
		w.SetLogger(m.Logger())
		w.do()
	}()
}

// segmentFlusherWorker is the worker that flushes segments into the WAL.
type segmentFlushWorker struct {
	mlog.Binder
	txnManager   TxnManager
	ctx          context.Context
	collectionID int64
	partitionID  int64
	vchannel     string
	segment      *segmentAllocManager // the segment is belong to one collection
	wal          wal.WAL
}

// do is the main loop of the segment flush worker.
func (w *segmentFlushWorker) do() {
	retryBackoff := backoff.NewExponentialBackOff()
	retryBackoff.InitialInterval = 10 * time.Millisecond
	retryBackoff.MaxInterval = time.Second
	retryBackoff.MaxElapsedTime = 0
	retryBackoff.Reset()

	// The recovered segment assignment state does not include txnSem. Wait for
	// recovered transactions before writing a Flush message, so all inserts stay
	// ordered before the flush in the WAL.
	if err := w.waitForTxnManagerRecoverDone(); err != nil {
		w.Logger().Error(w.ctx, "failed to wait for txn manager recover ready",
			mlog.FieldVChannel(w.vchannel),
			mlog.FieldCollectionID(w.collectionID),
			mlog.FieldPartitionID(w.partitionID),
			mlog.Err(err),
		)
		return
	}

	for {
		err := w.doOnce()
		if err == nil {
			return
		}
		if status.AsStreamingError(err).IsUnrecoverable() {
			w.Logger().Warn(w.ctx, "flush growing segment with unrecoverable error, stop retrying",
				mlog.FieldVChannel(w.vchannel),
				mlog.FieldCollectionID(w.collectionID),
				mlog.FieldPartitionID(w.partitionID),
				mlog.Err(err),
			)
			return
		}
		nextInterval := retryBackoff.NextBackOff()
		w.Logger().Info(w.ctx, "failed to flush growing segment, retrying",
			mlog.FieldVChannel(w.vchannel),
			mlog.FieldCollectionID(w.collectionID),
			mlog.FieldPartitionID(w.partitionID),
			mlog.Duration("nextInterval", nextInterval),
			mlog.Err(err),
		)
		select {
		case <-w.ctx.Done():
			w.Logger().Info(w.ctx, "flush segment canceled",
				mlog.FieldVChannel(w.vchannel),
				mlog.FieldCollectionID(w.collectionID),
				mlog.FieldPartitionID(w.partitionID),
				mlog.Err(w.ctx.Err()),
			)
			return
		case <-w.wal.Available():
			w.Logger().Warn(w.ctx, "wal is unavailable, stop flush segment",
				mlog.FieldVChannel(w.vchannel),
				mlog.FieldCollectionID(w.collectionID),
				mlog.FieldPartitionID(w.partitionID),
			)
			return
		case <-time.After(nextInterval):
		}
	}
}

// waitForTxnManagerRecoverDone waits until transaction recovery is complete.
func (w *segmentFlushWorker) waitForTxnManagerRecoverDone() error {
	select {
	case <-w.txnManager.RecoverDone():
		return nil
	case <-w.ctx.Done():
		w.Logger().Info(w.ctx, "flush segment canceled",
			mlog.FieldVChannel(w.vchannel),
			mlog.FieldCollectionID(w.collectionID),
			mlog.FieldPartitionID(w.partitionID),
			mlog.Err(w.ctx.Err()),
		)
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
		w.Logger().Error(w.ctx, "failed to append flush message",
			mlog.FieldVChannel(w.vchannel),
			mlog.FieldCollectionID(w.collectionID),
			mlog.FieldPartitionID(w.partitionID),
			mlog.FieldMessage(msg),
			mlog.Err(err),
		)
		return err
	}
	policy := w.segment.SealPolicy()
	w.Logger().Info(w.ctx,
		"segment has been flushed",
		mlog.FieldVChannel(w.vchannel),
		mlog.FieldCollectionID(w.collectionID),
		mlog.FieldPartitionID(w.partitionID),
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
		w.Logger().Info(w.ctx, "segment has flying insert operation, delay it",
			mlog.FieldVChannel(w.vchannel),
			mlog.FieldCollectionID(w.collectionID),
			mlog.FieldPartitionID(w.partitionID),
			mlog.FieldSegmentID(w.segment.GetSegmentID()),
			mlog.Int32("ackSem", ackSem),
		)
		return false
	}
	// if there're flying txns, wait them committed, delay the flush at next retry.
	if txnSem := w.segment.TxnSem(); txnSem > 0 {
		w.Logger().Info(w.ctx, "segment has flying txns, delay it",
			mlog.FieldVChannel(w.vchannel),
			mlog.FieldCollectionID(w.collectionID),
			mlog.FieldPartitionID(w.partitionID),
			mlog.FieldSegmentID(w.segment.GetSegmentID()),
			mlog.Int32("txnSem", txnSem),
		)
		return false
	}
	return true
}
