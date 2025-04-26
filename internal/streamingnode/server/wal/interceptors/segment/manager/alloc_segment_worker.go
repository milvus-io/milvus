package manager

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
	"go.uber.org/zap"
)

// newAllocWorker creates a new alloc segment worker.
func newAllocWorker(
	logger *log.MLogger,
	wal *syncutil.Future[wal.WAL],
) *allocSegmentWorker {
	w := &allocSegmentWorker{
		backgroundTask: syncutil.NewAsyncTaskNotifier[struct{}](),
		logger:         logger,
		reqCh:          make(chan *AsyncAllocRequest, 100),
		wal:            wal,
	}
	go w.background()
	return w
}

// allocSegmentWorker is a worker that allocates new growing segments asynchronously.
type allocSegmentWorker struct {
	backgroundTask *syncutil.AsyncTaskNotifier[struct{}]
	logger         *log.MLogger
	reqCh          chan *AsyncAllocRequest
	wal            *syncutil.Future[wal.WAL]
}

// AsyncAllocRequest is the request for allocating a new growing segment.
type AsyncAllocRequest struct {
	CollectionID int64
	PartitionID  int64
	VChannel     string
	msg          message.MutableMessage
	future       *syncutil.Future[message.ImmutableCreateSegmentMessageV2]
}

// AsyncAllocNewGrowingSegment allocates a new growing segment asynchronously.
// The alloc operation doesn't promise done.
func (q *allocSegmentWorker) AsyncAllocNewGrowingSegment(req *AsyncAllocRequest) *syncutil.Future[message.ImmutableCreateSegmentMessageV2] {
	future := syncutil.NewFuture[message.ImmutableCreateSegmentMessageV2]()
	req.future = future

	select {
	case <-q.backgroundTask.Context().Done():
	case q.reqCh <- req:
	}
	return future
}

// Close closes the alloc segment worker.
func (w *allocSegmentWorker) Close() {
	w.backgroundTask.Cancel()
	w.backgroundTask.BlockUntilFinish()
}

// background runs the background task of the alloc segment worker.
func (w *allocSegmentWorker) background() {
	defer func() {
		w.backgroundTask.Finish(struct{}{})
		w.logger.Info("alloc segment worker finished")
	}()
	w.logger.Info("alloc segment worker started")

	for {
		select {
		case <-w.backgroundTask.Context().Done():
			return
		case req := <-w.reqCh:
			w.allocNewGrowingSegment(req)
		}
	}
}

// retryRequest retries the request if the background task is not done.
func (w *allocSegmentWorker) retryRequest(req *AsyncAllocRequest) {
	select {
	case <-w.backgroundTask.Context().Done():
		w.logger.Warn("background task is done, cannot retry request, the create segment request is dropped",
			zap.Int64("collectionID", req.CollectionID),
			zap.Int64("partitionID", req.PartitionID),
		)
	case w.reqCh <- req:
	}
}

func (w *allocSegmentWorker) allocNewGrowingSegment(req *AsyncAllocRequest) {
	if w.backgroundTask.Context().Err() != nil {
		return
	}
	if req.msg == nil {
		var err error
		if req.msg, err = w.generateNewGrowingSegmentMessage(w.backgroundTask.Context(), req); err != nil {
			w.logger.Warn("failed to generate new growing segment message", zap.Int64("partitionID", req.PartitionID))
			go func() {
				w.retryRequest(req)
			}()
		}
	}
	// Send CreateSegmentMessage into wal.
	w.wal.Get().AppendAsync(w.backgroundTask.Context(), req.msg, func(ar *wal.AppendResult, err error) {
		if err != nil {
			w.retryRequest(req)
			w.logger.Warn("failed to send create segment message into wal",
				zap.Int64("collectionID", req.CollectionID),
				zap.Int64("partitionID", req.PartitionID),
				zap.Error(err))
			return
		}
		immutableMessage := req.msg.IntoImmutableMessage(ar.MessageID)
		req.future.Set(message.MustAsImmutableCreateSegmentMessageV2(immutableMessage))
	})
}

// generateNewGrowingSegmentMessage generates a new growing segment message.
func (w *allocSegmentWorker) generateNewGrowingSegmentMessage(ctx context.Context, req *AsyncAllocRequest) (message.MutableMessage, error) {
	// Allocate new segment id and create ts from remote.
	segmentID, err := resource.Resource().IDAllocator().Allocate(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to allocate segment id")
	}
	storageVersion := storage.StorageV1
	if paramtable.Get().CommonCfg.EnableStorageV2.GetAsBool() {
		storageVersion = storage.StorageV2
	}
	// Getnerate growing segment limitation.
	limitation := getSegmentLimitationPolicy().GenerateLimitation()
	// Create a new segment by sending a create segment message into wal directly.
	return message.NewCreateSegmentMessageBuilderV2().
		WithVChannel(req.VChannel).
		WithHeader(&message.CreateSegmentMessageHeader{}).
		WithBody(&message.CreateSegmentMessageBody{
			CollectionId: req.CollectionID,
			Segments: []*messagespb.CreateSegmentInfo{{
				// We only execute one segment creation operation at a time.
				// But in future, we need to modify the segment creation operation to support batch creation.
				// Because the partition-key based collection may create huge amount of segments at the same time.
				PartitionId:    req.PartitionID,
				SegmentId:      int64(segmentID),
				StorageVersion: storageVersion,
				MaxSegmentSize: limitation.SegmentSize,
			}},
		}).MustBuildMutable(), nil
}
