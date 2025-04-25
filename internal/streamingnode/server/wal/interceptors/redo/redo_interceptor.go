package redo

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
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
	gracefulStop chan struct{}
}

// TODO: should be removed after lock-based before timetick is applied.
func (r *redoAppendInterceptor) DoAppend(ctx context.Context, msg message.MutableMessage, append interceptors.Append) (msgID message.MessageID, err error) {
	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		msgID, err = append(ctx, msg)
		// If the error is ErrRedo, we should redo the append operation.
		if errors.Is(err, ErrRedo) {
			redoWait := utility.GetRedoWait(ctx)
			if redoWait == nil {
				continue
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-r.gracefulStop:
				return nil, status.NewOnShutdownError("redo interceptor is on shutdown")
			case <-redoWait:
				continue
			}
		}
		return msgID, err
	}
}

func (r *redoAppendInterceptor) GracefulClose() {
	close(r.gracefulStop)
}

func (r *redoAppendInterceptor) Close() {}
