package redo

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
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

// TODO: should be removed after lock-based before timetick is applied.
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
		return msgID, err
	}
}

// waitUntilGrowingSegmentReady waits until the growing segment is ready if msg is insert.
func (r *redoAppendInterceptor) waitUntilGrowingSegmentReady(ctx context.Context, msg message.MutableMessage) error {
	if msg.MessageType() == message.MessageTypeInsert {
		insertMessage := message.MustAsMutableInsertMessageV1(msg)
		h := insertMessage.Header()
		for _, partition := range h.Partitions {
			ready, err := r.shardManager.WaitUntilGrowingSegmentReady(h.CollectionId, partition.PartitionId)
			if err != nil {
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
