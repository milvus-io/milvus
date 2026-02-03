package shards

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

// asyncAllocSegment allocates a new growing segment asynchronously.
func (m *partitionManager) asyncAllocSegment() {
	if m.onAllocating != nil {
		m.Logger().Debug("segment alloc worker is already on allocating")
		// manager is already on allocating.
		return
	}
	// Create a notifier to notify the waiter when the allocation is done.
	m.onAllocating = make(chan struct{})
	w := &segmentAllocWorker{
		ctx:          m.ctx,
		collectionID: m.collectionID,
		partitionID:  m.partitionID,
		vchannel:     m.vchannel,
		wal:          m.wal.Get(),
	}
	w.SetLogger(m.Logger())
	// It should always done asynchronously.
	// Otherwise, a dead lock may happens when wal is on writing.
	go w.do()
}

// segmentAllocWorker is a worker that allocates new growing segments asynchronously.
type segmentAllocWorker struct {
	log.Binder
	ctx          context.Context
	collectionID int64
	partitionID  int64
	vchannel     string
	wal          wal.WAL
	// The following fields are preserved across retries to ensure the same segment
	// configuration is used when rebuilding the message after a failed append.
	segmentID      uint64            // allocated segment ID
	storageVersion int64             // storage version determined at first attempt
	limitation     segmentLimitation // segment limitation determined at first attempt
}

// do is the main loop of the segment allocation worker.
func (w *segmentAllocWorker) do() {
	backoff := backoff.NewExponentialBackOff()
	backoff.InitialInterval = 10 * time.Millisecond
	backoff.MaxInterval = 1 * time.Second
	backoff.MaxElapsedTime = 0
	backoff.Reset()

	for {
		err := w.doOnce()
		if err == nil {
			return
		}
		if e := status.AsStreamingError(err); e.IsUnrecoverable() {
			w.Logger().Warn("allocate new growing segement with unrecoverable error, stop retrying", zap.Error(err))
			return
		}
		nextInterval := backoff.NextBackOff()
		w.Logger().Info("failed to allocate new growing segment, retrying", zap.Duration("nextInterval", nextInterval), zap.Error(err))
		select {
		case <-w.ctx.Done():
			w.Logger().Info("segment allocation canceled", zap.Error(w.ctx.Err()))
			return
		case <-w.wal.Available():
			// wal is unavailable, stop the worker.
			w.Logger().Warn("wal is unavailable, stop alloc new segment")
			return
		case <-time.After(backoff.NextBackOff()):
		}
	}
}

// doOnce executes the segment allocation operation.
func (w *segmentAllocWorker) doOnce() error {
	// Initialize segment configuration on first attempt.
	// These values are preserved across retries to ensure consistency.
	if err := w.initSegmentConfig(); err != nil {
		return err
	}

	// Build a fresh message each time to avoid reusing a contaminated message.
	// After a failed WAL append, the message may have internal state set (e.g., WAL term)
	// that would cause a panic if reused.
	msg := message.NewCreateSegmentMessageBuilderV2().
		WithVChannel(w.vchannel).
		WithHeader(&message.CreateSegmentMessageHeader{
			CollectionId:   w.collectionID,
			PartitionId:    w.partitionID,
			SegmentId:      int64(w.segmentID),
			StorageVersion: w.storageVersion,
			MaxRows:        w.limitation.SegmentRows,
			MaxSegmentSize: w.limitation.SegmentSize,
			Level:          datapb.SegmentLevel_L1,
		}).
		WithBody(&message.CreateSegmentMessageBody{}).
		MustBuildMutable()

	result, err := w.wal.Append(w.ctx, msg)
	if err != nil {
		w.Logger().Warn("failed to append create segment message", log.FieldMessage(msg), zap.Error(err))
		return err
	}
	w.Logger().Info("append create segment message", log.FieldMessage(msg), zap.String("messageID", result.MessageID.String()), zap.Uint64("timetick", result.TimeTick))
	return nil
}

// initSegmentConfig initializes the segment configuration (segmentID, storageVersion, limitation).
// These values are only set once and preserved across retries to ensure consistency.
func (w *segmentAllocWorker) initSegmentConfig() error {
	// Skip if already initialized.
	if w.segmentID != 0 {
		return nil
	}

	// Allocate new segment id.
	segmentID, err := resource.Resource().IDAllocator().Allocate(w.ctx)
	if err != nil {
		w.Logger().Warn("failed to allocate segment id", zap.Error(err))
		return err
	}
	w.segmentID = segmentID

	// Determine storage version.
	w.storageVersion = storage.StorageV2
	// Generate growing segment limitation.
	w.limitation = getSegmentLimitationPolicy().GenerateLimitation(datapb.SegmentLevel_L1)
	return nil
}
