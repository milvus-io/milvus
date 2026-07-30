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
func (m *partitionManager) asyncAllocSegment(schemaVersion int32, requiresStorageV3 bool) {
	if m.onAllocating != nil {
		m.Logger().Debug(context.TODO(), "segment alloc worker is already on allocating",
			mlog.FieldVChannel(m.vchannel),
			mlog.FieldCollectionID(m.collectionID),
			mlog.FieldPartitionID(m.partitionID),
		)
		// manager is already on allocating.
		return
	}
	// Create a notifier to notify the waiter when the allocation is done.
	m.onAllocating = make(chan struct{})
	w := &segmentAllocWorker{
		ctx:               m.ctx,
		collectionID:      m.collectionID,
		partitionID:       m.partitionID,
		vchannel:          m.vchannel,
		wal:               m.wal.Get(),
		schemaVersion:     schemaVersion,
		requiresStorageV3: requiresStorageV3,
	}
	w.SetLogger(m.Logger())
	// It should always run asynchronously. Otherwise, a deadlock may happen
	// while the WAL is writing the CreateSegment message.
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
	segmentID         uint64            // allocated segment ID
	storageVersion    int64             // storage version determined at first attempt
	limitation        segmentLimitation // segment limitation determined at first attempt
	schemaVersion     int32
	requiresStorageV3 bool
}

// do is the main loop of the segment allocation worker.
func (w *segmentAllocWorker) do() {
	retryBackoff := backoff.NewExponentialBackOff()
	retryBackoff.InitialInterval = 10 * time.Millisecond
	retryBackoff.MaxInterval = time.Second
	retryBackoff.MaxElapsedTime = 0
	retryBackoff.Reset()

	for {
		err := w.doOnce()
		if err == nil {
			return
		}
		if status.AsStreamingError(err).IsUnrecoverable() {
			w.Logger().Warn(w.ctx, "allocate new growing segment with unrecoverable error, stop retrying",
				mlog.FieldVChannel(w.vchannel),
				mlog.FieldCollectionID(w.collectionID),
				mlog.FieldPartitionID(w.partitionID),
				mlog.Err(err),
			)
			return
		}
		nextInterval := retryBackoff.NextBackOff()
		w.Logger().Info(w.ctx, "failed to allocate new growing segment, retrying",
			mlog.FieldVChannel(w.vchannel),
			mlog.FieldCollectionID(w.collectionID),
			mlog.FieldPartitionID(w.partitionID),
			mlog.Duration("nextInterval", nextInterval),
			mlog.Err(err),
		)
		select {
		case <-w.ctx.Done():
			w.Logger().Info(w.ctx, "segment allocation canceled",
				mlog.FieldVChannel(w.vchannel),
				mlog.FieldCollectionID(w.collectionID),
				mlog.FieldPartitionID(w.partitionID),
				mlog.Err(w.ctx.Err()),
			)
			return
		case <-w.wal.Available():
			w.Logger().Warn(w.ctx, "wal is unavailable, stop alloc new segment",
				mlog.FieldVChannel(w.vchannel),
				mlog.FieldCollectionID(w.collectionID),
				mlog.FieldPartitionID(w.partitionID),
			)
			return
		case <-time.After(nextInterval):
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
		w.Logger().Warn(w.ctx, "failed to append create segment message",
			mlog.FieldVChannel(w.vchannel),
			mlog.FieldCollectionID(w.collectionID),
			mlog.FieldPartitionID(w.partitionID),
			mlog.FieldMessage(msg),
			mlog.Err(err),
		)
		return err
	}
	w.Logger().Info(w.ctx,
		"append create segment message",
		mlog.FieldVChannel(w.vchannel),
		mlog.FieldCollectionID(w.collectionID),
		mlog.FieldPartitionID(w.partitionID),
		mlog.FieldMessage(msg),
		mlog.String("messageID", result.MessageID.String()),
		mlog.Uint64("timetick", result.TimeTick),
	)
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
		w.Logger().Warn(w.ctx, "failed to allocate segment id",
			mlog.FieldVChannel(w.vchannel),
			mlog.FieldCollectionID(w.collectionID),
			mlog.FieldPartitionID(w.partitionID),
			mlog.Err(err),
		)
		return err
	}
	w.segmentID = segmentID

	// Determine storage version.
	w.storageVersion = storage.StorageV2
	if w.requiresStorageV3 || paramtable.Get().CommonCfg.UseLoonFFI.GetAsBool() {
		w.storageVersion = storage.StorageV3
	}

	// Generate growing segment limitation.
	w.limitation = getSegmentLimitationPolicy().GenerateLimitation(datapb.SegmentLevel_L1)
	return nil
}
