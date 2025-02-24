package redo

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

var (
	_       interceptors.Interceptor = (*redoAppendInterceptor)(nil)
	ErrRedo                          = errors.New("redo")
)

// redoAppendInterceptor is an append interceptor to retry the append operation if needed.
// It's useful when the append operation want to refresh the append context (such as timetick belong to the message)
type redoAppendInterceptor struct{}

func (r *redoAppendInterceptor) DoAppend(ctx context.Context, msg message.MutableMessage, append interceptors.Append) (msgID message.MessageID, err error) {
	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		msgID, err = append(ctx, msg)
		// If the error is ErrRedo, we should redo the append operation.
		if errors.Is(err, ErrRedo) {
			continue
		}
		return msgID, err
	}
}

func (r *redoAppendInterceptor) Close() {}
