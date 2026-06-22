package streaming

import (
	"context"

	resumabletransformlog "github.com/milvus-io/milvus/internal/distributed/streaming/internal/transformlog"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func (w *walAccesserImpl) TransformLog() wal.TransformLogAccesser {
	return transformLogAccesser{w: w}
}

type transformLogAccesser struct {
	w *walAccesserImpl
}

func (a transformLogAccesser) Read(ctx context.Context, opts wal.TransformLogReadOption) wal.TransformLogScanner {
	if !a.w.lifetime.Add(typeutil.LifetimeStateWorking) {
		return wal.NewTransformLogErrorScanner(opts.Name, ErrWALAccesserClosed)
	}
	defer a.w.lifetime.Done()

	if opts.VChannel == "" {
		return wal.NewTransformLogErrorScanner(opts.Name, wal.ErrTransformLogInvalidReadOption)
	}
	return resumabletransformlog.NewResumableScanner(ctx, a.w.handlerClient.ReadTransformLog, opts)
}
