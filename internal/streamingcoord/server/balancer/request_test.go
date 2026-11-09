package balancer

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestUpdateBalancePolicyValidatesNodes(t *testing.T) {
	paramtable.Init()
	mockey.PatchConvey("validate all SN identities before mutation", t, func() {
		b := &balancerImpl{freezeNodes: typeutil.NewConcurrentSet[int64]()}
		b.SetLogger(mlog.With())
		mockey.Mock((*balancerImpl).GetAllStreamingNodes).Return(map[int64]*types.StreamingNodeInfoWithResourceGroup{1: {}, 2: {}}, nil).Build()
		var configured []bool
		var configErr error
		mockey.Mock(SetRebalanceEnabled).To(func(enabled bool) error { configured = append(configured, enabled); return configErr }).Build()
		apply := func(req *streamingpb.UpdateWALBalancePolicyRequest) error {
			r := newOpUpdateBalancePolicy(t.Context(), req)
			r.apply(b)
			return r.future.Get().err
		}
		for _, nodes := range []*streamingpb.WALBalancePolicyNodes{{FreezeNodeIds: []int64{1, 99}}, {DefreezeNodeIds: []int64{99}}} {
			require.ErrorIs(t, apply(&streamingpb.UpdateWALBalancePolicyRequest{Nodes: nodes}), merr.ErrNodeNotFound)
			require.Empty(t, b.freezeNodes.Collect())
			require.Empty(t, configured)
		}
		require.NoError(t, apply(&streamingpb.UpdateWALBalancePolicyRequest{UpdateMask: &fieldmaskpb.FieldMask{}, Nodes: &streamingpb.WALBalancePolicyNodes{FreezeNodeIds: []int64{1}}}))
		require.Equal(t, []int64{1}, b.freezeNodes.Collect())
		require.Empty(t, configured, "freeze must not touch balance config")
		configErr = merr.ErrServiceUnavailable
		require.ErrorIs(t, apply(&streamingpb.UpdateWALBalancePolicyRequest{Nodes: &streamingpb.WALBalancePolicyNodes{DefreezeNodeIds: []int64{1}}}), configErr)
		require.Equal(t, []int64{1}, b.freezeNodes.Collect())
		configErr = nil
		require.NoError(t, apply(&streamingpb.UpdateWALBalancePolicyRequest{Config: &streamingpb.WALBalancePolicyConfig{AllowRebalance: true}, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{types.UpdateMaskPathWALBalancePolicyAllowRebalance}}, Nodes: &streamingpb.WALBalancePolicyNodes{DefreezeNodeIds: []int64{1}}}))
		require.Empty(t, b.freezeNodes.Collect())
		require.Equal(t, []bool{false, true}, configured)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		r := newOpUpdateBalancePolicy(ctx, &streamingpb.UpdateWALBalancePolicyRequest{})
		r.apply(b)
		require.ErrorIs(t, r.future.Get().err, context.Canceled)
		require.Len(t, configured, 2)
	})
}
