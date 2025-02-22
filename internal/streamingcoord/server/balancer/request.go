package balancer

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

// request is a operation request.
type request struct {
	ctx    context.Context
	apply  requestApply
	future *syncutil.Future[error]
}

// requestApply is a request operation to be executed.
type requestApply func(impl *balancerImpl)

// newOpMarkAsUnavailable is a operation to mark some channels as unavailable.
func newOpMarkAsUnavailable(ctx context.Context, pChannels []types.PChannelInfo) *request {
	future := syncutil.NewFuture[error]()
	return &request{
		ctx: ctx,
		apply: func(impl *balancerImpl) {
			future.Set(impl.channelMetaManager.MarkAsUnavailable(ctx, pChannels))
		},
		future: future,
	}
}

// newOpTrigger is a operation to trigger a re-balance operation.
func newOpTrigger(ctx context.Context) *request {
	future := syncutil.NewFuture[error]()
	return &request{
		ctx: ctx,
		apply: func(impl *balancerImpl) {
			future.Set(nil)
		},
		future: future,
	}
}
