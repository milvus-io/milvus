package shards

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
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
				mlog.FieldSegmentID(segment.GetSegmentID()),
				mlog.Err(err))
			return
		}

		// create a new segment flush worker.
		w := &segmentFlushWorker{
			txnManager:   m.txnManager,
			ctx:          ctx,
			collectionID: m.collectionID,
			vchannel:     m.vchannel,
			segment:      segment,
			wal:          l,
		}
		w.SetLogger(m.Logger())
		m.scheduler.Submit(w)
	}()
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

func (w *segmentFlushWorker) Execute(schedulerCtx context.Context) error {
	ctx, cancel := mergeSegmentTaskContext(schedulerCtx, w.ctx)
	defer cancel()
	if segmentTaskStopped(ctx, w.wal) {
		return nil
	}
	if !txnManagerRecovered(w.txnManager) {
		return nodescheduler.ErrDelay
	}
	if !w.checkIfReady() {
		return nodescheduler.ErrDelay
	}
	if err := w.doOnceWithContext(ctx); err != nil {
		if segmentTaskStopped(ctx, w.wal) {
			return nil
		}
		if status.AsStreamingError(err).IsUnrecoverable() {
			return err
		}
		return nodescheduler.ErrDelay
	}
	return nil
}

func txnManagerRecovered(txnManager TxnManager) bool {
	select {
	case <-txnManager.RecoverDone():
		return true
	default:
		return false
	}
}

// doOnce performs the flush operation once.
func (w *segmentFlushWorker) doOnce() error {
	return w.doOnceWithContext(w.ctx)
}

func (w *segmentFlushWorker) doOnceWithContext(ctx context.Context) error {
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

	result, err := w.wal.Append(ctx, msg)
	if err != nil {
		w.Logger().Error(ctx, "failed to append flush message", mlog.FieldMessage(msg), mlog.Err(err))
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

func mergeSegmentTaskContext(schedulerCtx, taskCtx context.Context) (context.Context, context.CancelFunc) {
	if taskCtx == nil {
		taskCtx = context.Background()
	}
	ctx, cancel := context.WithCancel(taskCtx)
	stop := context.AfterFunc(schedulerCtx, cancel)
	return ctx, func() {
		stop()
		cancel()
	}
}

func segmentTaskStopped(ctx context.Context, wal wal.WAL) bool {
	select {
	case <-ctx.Done():
		return true
	case <-wal.Available():
		return true
	default:
		return false
	}
}

var _ nodescheduler.Task = (*segmentFlushWorker)(nil)

// checkIfReady checks if the segments are ready to be flushed.
func (w *segmentFlushWorker) checkIfReady() bool {
	// if there're flying acks, wait them acked, delay the flush at next retry.
	if ackSem := w.segment.AckSem(); ackSem > 0 {
		w.Logger().Info(w.ctx, "segment has flying insert operation, delay it", mlog.Int32("ackSem", ackSem), mlog.FieldSegmentID(w.segment.GetSegmentID()))
		return false
	}
	// if there're flying txns, wait them committed, delay the flush at next retry.
	if txnSem := w.segment.TxnSem(); txnSem > 0 {
		w.Logger().Info(w.ctx, "segment has flying txns, delay it", mlog.Int32("txnSem", txnSem), mlog.FieldSegmentID(w.segment.GetSegmentID()))
		return false
	}
	return true
}
