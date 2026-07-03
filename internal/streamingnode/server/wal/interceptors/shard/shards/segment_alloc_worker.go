package shards

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// asyncAllocSegment allocates a new growing segment asynchronously.
func (m *partitionManager) asyncAllocSegment(schemaVersion int32, useGrowingSourceFlush bool) {
	if m.onAllocating != nil {
		m.Logger().Debug(context.TODO(), "segment alloc worker is already on allocating")
		// manager is already on allocating.
		return
	}
	// Create a notifier to notify the waiter when the allocation is done.
	m.onAllocating = make(chan struct{})
	w := &segmentAllocWorker{
		ctx:                   m.ctx,
		collectionID:          m.collectionID,
		partitionID:           m.partitionID,
		vchannel:              m.vchannel,
		wal:                   m.wal.Get(),
		schemaVersion:         schemaVersion,
		useGrowingSourceFlush: useGrowingSourceFlush,
	}
	w.SetLogger(m.Logger())
	// It should always done asynchronously.
	// Otherwise, a dead lock may happens when wal is on writing.
	go w.do()
}

// segmentAllocWorker is a worker that allocates new growing segments asynchronously.
type segmentAllocWorker struct {
	mlog.Binder
	ctx          context.Context
	collectionID int64
	partitionID  int64
	vchannel     string
	wal          wal.WAL
	// The following fields are preserved across retries to ensure the same segment
	// configuration is used when rebuilding the message after a failed append.
	segmentID             uint64            // allocated segment ID
	storageVersion        int64             // storage version determined at first attempt
	limitation            segmentLimitation // segment limitation determined at first attempt
	schemaVersion         int32
	useGrowingSourceFlush bool
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
			w.Logger().Warn(w.ctx, "allocate new growing segement with unrecoverable error, stop retrying", mlog.Err(err))
			return
		}
		nextInterval := backoff.NextBackOff()
		w.Logger().Info(w.ctx, "failed to allocate new growing segment, retrying", mlog.Duration("nextInterval", nextInterval), mlog.Err(err))
		select {
		case <-w.ctx.Done():
			w.Logger().Info(w.ctx, "segment allocation canceled", mlog.Err(w.ctx.Err()))
			return
		case <-w.wal.Available():
			// wal is unavailable, stop the worker.
			w.Logger().Warn(w.ctx, "wal is unavailable, stop alloc new segment")
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
	// TODO: include SchemaVersion in CreateSegmentMessageHeader so that the flusher
	// can propagate it to DataCoord's AllocSegment RPC. Currently streaming-created
	// segments get SchemaVersion=0, causing unnecessary backfill triggers.
	// Tracked in companion PR: https://github.com/milvus-io/milvus/pull/48865
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
			SchemaVersion:  w.schemaVersion,
		}).
		WithBody(&message.CreateSegmentMessageBody{}).
		MustBuildMutable()

	result, err := w.wal.Append(w.ctx, msg)
	if err != nil {
		w.Logger().Warn(w.ctx, "failed to append create segment message", mlog.FieldMessage(msg), mlog.Err(err))
		return err
	}
	w.Logger().Info(w.ctx,
		"append create segment message", mlog.FieldMessage(msg), mlog.String("messageID", result.MessageID.String()), mlog.Uint64("timetick", result.TimeTick))
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
		w.Logger().Warn(w.ctx, "failed to allocate segment id", mlog.Err(err))
		return err
	}
	w.segmentID = segmentID

	// Determine storage version.
	w.storageVersion = storage.StorageV2
	if paramtable.Get().CommonCfg.UseLoonFFI.GetAsBool() {
		w.storageVersion = storage.StorageV3
	}

	// Generate growing segment limitation.
	w.limitation = getSegmentLimitationPolicy().GenerateLimitation(datapb.SegmentLevel_L1)
	return nil
}
