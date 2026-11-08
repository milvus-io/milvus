package redo

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
)

var (
	_       interceptors.Interceptor                  = (*redoAppendInterceptor)(nil)
	_       interceptors.InterceptorWithGracefulClose = (*redoAppendInterceptor)(nil)
	ErrRedo                                           = errors.New("redo")
)

// redoAppendInterceptor is an append interceptor to retry the append operation if needed.
// It's useful when the append operation want to refresh the append context (such as timetick belong to the message)
type redoAppendInterceptor struct {
	shardManager shards.ShardManager
	gracefulStop chan struct{}
}

func (r *redoAppendInterceptor) DoAppend(ctx context.Context, msg message.MutableMessage, append interceptors.Append) (msgID message.MessageID, err error) {
	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if err := r.waitUntilGrowingSegmentReady(ctx, msg); err != nil {
			return nil, err
		}

		msgID, err = append(ctx, msg)
		// If the error is ErrRedo, we should redo the append operation.
		if errors.Is(err, ErrRedo) {
			continue
		}
		var preparation *function.PreparationRequiredError
		if errors.As(err, &preparation) {
			// The inner chain has released its locks, TimeTick acker and
			// transaction admission before joining preparation.
			if err := prepareRunner(ctx, preparation); err != nil {
				return nil, err
			}
			continue
		}
		return msgID, err
	}
}

type callerContextKey struct{}

// WithCallerContext preserves the caller's cancellation before the WAL detaches
// it for mutation safety. Only preparation uses it; accepted writes still use
// the WAL-owned context.
func WithCallerContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, callerContextKey{}, ctx)
}

func prepareRunner(ctx context.Context, preparation *function.PreparationRequiredError) error {
	if caller, ok := ctx.Value(callerContextKey{}).(context.Context); ok {
		var cancel context.CancelFunc
		ctx, cancel = contextutil.MergeContext(caller, ctx)
		defer cancel()
	}
	err := preparation.Prepare(ctx)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err != nil {
		// Transient resource failures retry inside Prepare. Preserve the
		// existing terminal append policy for permanent/native failures.
		return status.NewUnrecoverableError("failed to prepare function fields before WAL append: %s", err.Error())
	}
	return nil
}

// waitUntilGrowingSegmentReady waits until the growing segment is ready if msg is insert.
func (r *redoAppendInterceptor) waitUntilGrowingSegmentReady(ctx context.Context, msg message.MutableMessage) error {
	if msg.MessageType() == message.MessageTypeInsert {
		insertMessage := message.MustAsMutableInsertMessageV1(msg)
		h := insertMessage.Header()
		if len(h.Partitions) != 1 {
			// TODO: We will support multi-partition insert in the future.
			panic("insert message should only have one partition")
		}
		for _, partition := range h.Partitions {
			uniqueKey := shards.PartitionUniqueKey{CollectionID: h.CollectionId, PartitionID: partition.PartitionId}
			ready, err := r.shardManager.WaitUntilGrowingSegmentReady(uniqueKey)
			if err != nil {
				if errors.IsAny(err, shards.ErrCollectionNotFound, shards.ErrPartitionNotFound) {
					return status.NewUnrecoverableError("fail to wait growing segment ready, %s", err.Error())
				}
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ready:
				// do nothing
				return nil
			case <-r.gracefulStop:
				return status.NewOnShutdownError("redo interceptor is on shutdown")
			}
		}
	}
	return nil
}

func (r *redoAppendInterceptor) GracefulClose() {
	close(r.gracefulStop)
}

func (r *redoAppendInterceptor) Close() {}
