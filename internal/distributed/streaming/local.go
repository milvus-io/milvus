package streaming

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type localServiceImpl struct {
	*walAccesserImpl
}

func (w localServiceImpl) GetLatestMVCCTimestampIfLocal(ctx context.Context, vchannel string) (uint64, error) {
	if !w.lifetime.Add(typeutil.LifetimeStateWorking) {
		return 0, ErrWALAccesserClosed
	}
	defer w.lifetime.Done()

	return w.handlerClient.GetLatestMVCCTimestampIfLocal(ctx, vchannel)
}

// GetMetrics gets the metrics of the wal.
func (w localServiceImpl) GetMetricsIfLocal(ctx context.Context) (*types.StreamingNodeMetrics, error) {
	if !w.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, ErrWALAccesserClosed
	}
	defer w.lifetime.Done()

	return w.handlerClient.GetWALMetricsIfLocal(ctx)
}
