package balancer

import (
	"context"
	"strconv"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

type response struct {
	resp any
	err  error
}

// request is a operation request.
type request struct {
	ctx    context.Context
	apply  requestApply
	future *syncutil.Future[response]
}

// requestApply is a request operation to be executed.
type requestApply func(impl *balancerImpl)

// newOpUpdateBalancePolicy is a operation to update the balance policy.
func newOpUpdateBalancePolicy(ctx context.Context, req *types.UpdateWALBalancePolicyRequest) *request {
	future := syncutil.NewFuture[response]()
	return &request{
		ctx: ctx,
		apply: func(impl *balancerImpl) {
			if req.UpdateMask != nil {
				// if there's a update mask, only update the fields in the update mask.
				for _, field := range req.UpdateMask.Paths {
					switch field {
					case types.UpdateMaskPathWALBalancePolicyAllowRebalance:
						updateAllowRebalance(impl, req.GetConfig().GetAllowRebalance())
					}
				}
			} else {
				// otherwise update all fields.
				updateAllowRebalance(impl, req.GetConfig().GetAllowRebalance())
			}
			// apply the freeze streaming nodes.
			if len(req.GetNodes().GetFreezeNodeIds()) > 0 || len(req.GetNodes().GetDefreezeNodeIds()) > 0 {
				impl.Logger().Info("update freeze nodes", zap.Int64s("freezeNodeIDs", req.GetNodes().GetFreezeNodeIds()), zap.Int64s("defreezeNodeIDs", req.GetNodes().GetDefreezeNodeIds()))
				impl.freezeNodes.Insert(req.GetNodes().GetFreezeNodeIds()...)
				impl.freezeNodes.Remove(req.GetNodes().GetDefreezeNodeIds()...)
			}
			future.Set(response{resp: &types.UpdateWALBalancePolicyResponse{
				Config: &streamingpb.WALBalancePolicyConfig{
					AllowRebalance: paramtable.Get().StreamingCfg.WALBalancerPolicyAllowRebalance.GetAsBool(),
				},
				FreezeNodeIds: impl.freezeNodes.Collect(),
			}, err: nil})
		},
		future: future,
	}
}

// updateAllowRebalance update the allow rebalance.
func updateAllowRebalance(impl *balancerImpl, allowRebalance bool) {
	old := paramtable.Get().StreamingCfg.WALBalancerPolicyAllowRebalance.SwapTempValue(strconv.FormatBool(allowRebalance))
	impl.Logger().Info("update allow_rebalance", zap.Bool("new", allowRebalance), zap.String("old", old))
}

// newOpMarkAsUnavailable is a operation to mark some channels as unavailable.
func newOpMarkAsUnavailable(ctx context.Context, pChannels []types.PChannelInfo) *request {
	future := syncutil.NewFuture[response]()
	return &request{
		ctx: ctx,
		apply: func(impl *balancerImpl) {
			err := impl.channelMetaManager.MarkAsUnavailable(ctx, pChannels)
			future.Set(response{err: err})
		},
		future: future,
	}
}

// newOpTrigger is a operation to trigger a re-balance operation.
func newOpTrigger(ctx context.Context) *request {
	future := syncutil.NewFuture[response]()
	return &request{
		ctx: ctx,
		apply: func(impl *balancerImpl) {
			future.Set(response{})
		},
		future: future,
	}
}
