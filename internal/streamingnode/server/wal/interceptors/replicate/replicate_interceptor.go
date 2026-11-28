package replicate

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/replicate/replicates"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

const interceptorName = "replicate"

type replicateInterceptor struct {
	replicateManager replicates.ReplicatesManager
}

func (impl *replicateInterceptor) Name() string {
	return interceptorName
}

func (impl *replicateInterceptor) DoAppend(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (msgID message.MessageID, err error) {
	// Begin to replicate the message.
	acker, err := impl.replicateManager.BeginReplicateMessage(ctx, msg)
	if errors.Is(err, replicates.ErrNotHandledByReplicateManager) {
		// the message is not handled by replicate manager, write it into wal directly.
		return appendOp(ctx, msg)
	}
	if err != nil {
		return nil, err
	}

	defer func() {
		acker.Ack(err)
	}()
	return appendOp(ctx, msg)
}

func (impl *replicateInterceptor) Close() {
}
