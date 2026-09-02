package streaming

import (
	"context"

	resumabletransformlog "github.com/milvus-io/milvus/internal/distributed/streaming/internal/transformlog"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func (w *walAccesserImpl) TransformLogStreamManager() wal.TransformLogStreamManager {
	return transformLogStreamManager{w: w}
}

type transformLogStreamManager struct {
	w *walAccesserImpl
}

func (m transformLogStreamManager) AcquireStream(ctx context.Context, pchannel string) (wal.TransformLogStream, error) {
	if !m.w.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, ErrWALAccesserClosed
	}
	defer m.w.lifetime.Done()

	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(m.w.transformCtx, cancel)
	stream := resumabletransformlog.NewResumableStream(ctx, pchannel, func(ctx context.Context, pchannel string) (wal.TransformLogStream, error) {
		return handler.AcquireTransformLogStream(ctx, m.w.handlerClient, pchannel)
	})
	go func() { <-stream.Done(); stop(); cancel() }()
	return stream, nil
}

// TransformLogStreamManager returns the resumable subscription entrance.
func TransformLogStreamManager() wal.TransformLogStreamManager {
	if provider, ok := singleton.(interface {
		TransformLogStreamManager() wal.TransformLogStreamManager
	}); ok {
		return provider.TransformLogStreamManager()
	}
	return wal.NewTransformLogErrorAccesser(status.NewUnrecoverableError("streaming WAL is not initialized for transform subscriptions"))
}
