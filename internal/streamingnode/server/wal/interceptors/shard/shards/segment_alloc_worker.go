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
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
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
	msg          message.MutableMessage
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
	if err := w.generateNewGrowingSegmentMessage(); err != nil {
		return err
	}
	result, err := w.wal.Append(w.ctx, w.msg)
	if err != nil {
		w.Logger().Warn("failed to append create segment message", log.FieldMessage(w.msg), zap.Error(err))
		return err
	}
	w.Logger().Info("append create segment message", log.FieldMessage(w.msg), zap.String("messageID", result.MessageID.String()), zap.Uint64("timetick", result.TimeTick))
	return nil
}

// generateNewGrowingSegmentMessage generates a new growing segment message.
func (w *segmentAllocWorker) generateNewGrowingSegmentMessage() error {
	if w.msg != nil {
		return nil
	}

	// Allocate new segment id and create ts from remote.
	segmentID, err := resource.Resource().IDAllocator().Allocate(w.ctx)
	if err != nil {
		w.Logger().Warn("failed to allocate segment id", zap.Error(err))
		return err
	}
	storageVersion := storage.StorageV1
	if paramtable.Get().CommonCfg.EnableStorageV2.GetAsBool() {
		storageVersion = storage.StorageV2
	}
	// Getnerate growing segment limitation.
	limitation := getSegmentLimitationPolicy().GenerateLimitation()
	// Create a new segment by sending a create segment message into wal directly.
	w.msg = message.NewCreateSegmentMessageBuilderV2().
		WithVChannel(w.vchannel).
		WithHeader(&message.CreateSegmentMessageHeader{
			CollectionId:   w.collectionID,
			PartitionId:    w.partitionID,
			SegmentId:      int64(segmentID),
			StorageVersion: storageVersion,
			MaxSegmentSize: limitation.SegmentSize,
		}).
		WithBody(&message.CreateSegmentMessageBody{}).
		MustBuildMutable()
	w.SetLogger(w.Logger().With(log.FieldMessage(w.msg)))
	return nil
}
