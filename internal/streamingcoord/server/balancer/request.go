package balancer

import (
	"context"
	"slices"
	"strconv"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
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
			if err := ctx.Err(); err != nil {
				future.Set(response{err: err})
				return
			}
			// Validate every node before applying any part of the request. This
			// runs in the balance queue, including for callers other than REST.
			if len(req.GetNodes().GetFreezeNodeIds()) > 0 || len(req.GetNodes().GetDefreezeNodeIds()) > 0 {
				nodes, err := impl.GetAllStreamingNodes(ctx)
				if err != nil {
					future.Set(response{err: err})
					return
				}
				for _, ids := range [][]int64{req.GetNodes().GetFreezeNodeIds(), req.GetNodes().GetDefreezeNodeIds()} {
					for _, id := range ids {
						if _, ok := nodes[id]; !ok {
							future.Set(response{err: merr.WrapErrNodeNotFound(id, "StreamingNode not found")})
							return
						}
					}
				}
			}
			if req.UpdateMask == nil || slices.Contains(req.UpdateMask.Paths, types.UpdateMaskPathWALBalancePolicyAllowRebalance) {
				if err := SetRebalanceEnabled(req.GetConfig().GetAllowRebalance()); err != nil {
					future.Set(response{err: err})
					return
				}
			}
			// apply the freeze streaming nodes.
			if len(req.GetNodes().GetFreezeNodeIds()) > 0 || len(req.GetNodes().GetDefreezeNodeIds()) > 0 {
				impl.Logger().Info(ctx, "update freeze nodes", mlog.Int64s("freezeNodeIDs", req.GetNodes().GetFreezeNodeIds()), mlog.Int64s("defreezeNodeIDs", req.GetNodes().GetDefreezeNodeIds()))
				impl.freezeNodes.Upsert(req.GetNodes().GetFreezeNodeIds()...)
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

// SetRebalanceEnabled uses AlterConfig's durable write and synchronous local
// refresh. No temporary override may shadow later config writes or resets.
func SetRebalanceEnabled(enabled bool) error {
	manager := paramtable.GetBaseTable().Manager()
	source, ok := manager.GetEtcdSource()
	if !ok {
		return merr.WrapErrServiceUnavailableMsg("cannot alter WAL balance configuration: etcd source is not enabled")
	}
	return manager.UpdateConfigInEtcd(source, paramtable.Get().StreamingCfg.WALBalancerPolicyAllowRebalance.Key, strconv.FormatBool(enabled))
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
