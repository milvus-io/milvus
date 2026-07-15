package streaming

import (
	"context"

	resumabletransformlog "github.com/milvus-io/milvus/internal/distributed/streaming/internal/transformlog"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
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

	return resumabletransformlog.NewResumableStream(ctx, pchannel, m.w.handlerClient.AcquireTransformLogStream), nil
}
