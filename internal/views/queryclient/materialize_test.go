package queryclient

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/util/materialize"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/resource"
)

func retainedMaterializeFixture() (*legacyClient, *LegacyQueryRequest) {
	sn, qn := qviews.StreamingNode{PChannel: "p0"}, qviews.NewQueryNode(11)
	req := &LegacyQueryRequest{Req: &internalpb.RetrieveRequest{CollectionID: 100, SchemaVersion: 7, MvccTimestamp: 999}, Plans: []ShardPlan{{
		ShardID:    qviews.ShardID{ReplicaID: 3, VChannel: "p0_100v0"},
		Version:    &viewpb.QueryViewVersion{DataVersion: &viewpb.DataVersion{StreamingVersion: 5}, QueryVersion: 8},
		Mvcc:       &viewpb.QueryPlanMVCC{GrowingTimetick: 101, TransformingTimetick: 99},
		WorkNodes:  []qviews.WorkNode{sn, qn},
		Selections: []*viewpb.QueryPlanWorkNode{{Node: &viewpb.QueryPlanWorkNode_QueryNode{QueryNode: &viewpb.QueryWorkNode{NodeId: 11}}, SegmentSelection: &viewpb.SegmentSelection{Mode: viewpb.SegmentSelection_Include, SegmentIds: []int64{42}}}},
	}}}
	return &legacyClient{shardClient: &shardViewQueryClient{queryServiceClient: NewCompositeViewQueryServiceClient(nil, nil)}}, req
}

func TestMaterializeRetainedDispatchPreservesSnapshot(t *testing.T) {
	client, req := retainedMaterializeFixture()
	var mu sync.Mutex
	got := map[string]*viewpb.MaterializeOnViewRequest{}
	patch := mockey.Mock((*compositeViewQueryServiceClient).MaterializeOnView).To(func(_ *compositeViewQueryServiceClient, _ context.Context, node qviews.WorkNode, request *viewpb.MaterializeOnViewRequest) (*internalpb.MaterializeResults, error) {
		mu.Lock()
		got[node.String()] = proto.Clone(request).(*viewpb.MaterializeOnViewRequest)
		mu.Unlock()
		return &internalpb.MaterializeResults{Header: &internalpb.MaterializeHeader{Status: merr.Success(), FormatVersion: materialize.FormatVersion, ResultSize: 3}}, nil
	}).Build()
	defer patch.UnPatch()
	var received atomic.Int32
	err := client.Materialize(t.Context(), req, 123, 10, func(plan ShardPlan, node qviews.WorkNode, result *internalpb.MaterializeResults) error {
		received.Add(1)
		require.Equal(t, uint64(101), result.Header.MvccTimestamp)
		resource.ReleaseMaterializeBody(result)
		return nil
	})
	require.NoError(t, err)
	require.EqualValues(t, 2, received.Load())
	for _, node := range req.Plans[0].WorkNodes {
		request := got[node.String()]
		require.True(t, proto.Equal(req.Plans[0].Version, request.GetQueryByPk().Version))
		require.True(t, proto.Equal(req.Plans[0].Mvcc, request.GetQueryByPk().Mvcc))
		require.Equal(t, int32(7), request.GetQueryByPk().LegacyReq.SchemaVersion)
		require.Equal(t, legacyMVCCForNode(req.Plans[0].Mvcc, node), request.GetQueryByPk().LegacyReq.MvccTimestamp)
		require.Equal(t, uint64(123), request.GetIpcOptions().GetSchemaFingerprint())
		require.True(t, proto.Equal(qviews.WorkNodeSelection(&viewpb.QueryPlan{WorkNodes: req.Plans[0].Selections}, node).GetSegmentSelection(), request.GetQueryByPk().SegmentSelection))
	}
	require.Equal(t, uint64(999), req.Req.MvccTimestamp)
}

func TestMaterializeRetainedFailureAndSizeGuard(t *testing.T) {
	for _, outcome := range []string{"unsupported_request", "view_retired", "malformed", "oversized", "consumer_unsupported"} {
		t.Run(outcome, func(t *testing.T) {
			client, req := retainedMaterializeFixture()
			req.Plans[0].WorkNodes = req.Plans[0].WorkNodes[:1]
			patch := mockey.Mock((*compositeViewQueryServiceClient).MaterializeOnView).To(func(_ *compositeViewQueryServiceClient, _ context.Context, _ qviews.WorkNode, _ *viewpb.MaterializeOnViewRequest) (*internalpb.MaterializeResults, error) {
				switch outcome {
				case "unsupported_request":
					return nil, merr.ErrServiceUnimplemented
				case "view_retired":
					return nil, viewerror.NewViewInvalidated("retired")
				case "malformed":
					return &internalpb.MaterializeResults{}, nil
				}
				size := int64(11)
				if outcome == "consumer_unsupported" {
					size = 1
				}
				return &internalpb.MaterializeResults{Header: &internalpb.MaterializeHeader{Status: merr.Success(), ResultSize: size}}, nil
			}).Build()
			defer patch.UnPatch()
			var received atomic.Int32
			err := client.Materialize(t.Context(), req, 0, 10, func(_ ShardPlan, _ qviews.WorkNode, result *internalpb.MaterializeResults) error {
				received.Add(1)
				resource.ReleaseMaterializeBody(result)
				return merr.ErrServiceUnimplemented
			})
			switch outcome {
			case "unsupported_request":
				require.ErrorIs(t, err, merr.ErrServiceUnimplemented)
			case "view_retired":
				require.ErrorIs(t, err, merr.ErrQueryViewNotReady)
			case "malformed":
				require.ErrorIs(t, err, merr.ErrServiceInternal)
			case "oversized":
				require.ErrorIs(t, err, merr.ErrServiceUnimplemented)
			case "consumer_unsupported":
				require.ErrorIs(t, err, merr.ErrServiceUnimplemented)
			}
			if outcome == "consumer_unsupported" {
				require.EqualValues(t, 1, received.Load())
			} else {
				require.Zero(t, received.Load())
			}
		})
	}
}

func TestMaterializeRetainedJoinsLateReplyBeforeReturningFailure(t *testing.T) {
	client, req := retainedMaterializeFixture()
	entered, joined := make(chan struct{}), make(chan struct{})
	patch := mockey.Mock((*compositeViewQueryServiceClient).MaterializeOnView).To(func(_ *compositeViewQueryServiceClient, ctx context.Context, node qviews.WorkNode, _ *viewpb.MaterializeOnViewRequest) (*internalpb.MaterializeResults, error) {
		if node.NodeType() == qviews.NodeTypeStreamingNode {
			<-entered
			return nil, viewerror.NewViewInvalidated("retired")
		}
		close(entered)
		<-ctx.Done()
		close(joined)
		return &internalpb.MaterializeResults{Header: &internalpb.MaterializeHeader{Status: merr.Success()}}, nil
	}).Build()
	defer patch.UnPatch()
	err := client.Materialize(t.Context(), req, 0, 0, func(_ ShardPlan, _ qviews.WorkNode, result *internalpb.MaterializeResults) error {
		resource.ReleaseMaterializeBody(result)
		return nil
	})
	require.ErrorIs(t, err, merr.ErrQueryViewNotReady)
	select {
	case <-joined:
	default:
		t.Fatal("returned before late RPC completed")
	}
}

func TestMaterializeRetainedEmptySnapshots(t *testing.T) {
	client, req := retainedMaterializeFixture()
	patch := mockey.Mock((*compositeViewQueryServiceClient).MaterializeOnView).Return(nil, merr.ErrServiceInternal).Build()
	defer patch.UnPatch()
	req.Plans[0].WorkNodes = nil
	calls := 0
	err := client.Materialize(t.Context(), req, 0, 10, func(plan ShardPlan, node qviews.WorkNode, result *internalpb.MaterializeResults) error {
		calls++
		require.Nil(t, node)
		require.EqualValues(t, 101, result.Header.MvccTimestamp)
		require.Equal(t, materialize.FormatVersion, result.Header.FormatVersion)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, calls)
	require.Zero(t, patch.Times())
	req.Plans = []ShardPlan{}
	require.NoError(t, client.Materialize(t.Context(), req, 0, 10, nil))
	req.Plans = nil
	require.ErrorIs(t, client.Materialize(t.Context(), req, 0, 10, nil), merr.ErrServiceInternal)
}

func TestMaterializeRetainedSizeBudgetRequestsFallback(t *testing.T) {
	client, req := retainedMaterializeFixture()
	patch := mockey.Mock((*compositeViewQueryServiceClient).MaterializeOnView).To(func(_ *compositeViewQueryServiceClient, _ context.Context, _ qviews.WorkNode, _ *viewpb.MaterializeOnViewRequest) (*internalpb.MaterializeResults, error) {
		return &internalpb.MaterializeResults{Header: &internalpb.MaterializeHeader{Status: merr.Success(), ResultSize: 6}}, nil
	}).Build()
	defer patch.UnPatch()
	var received atomic.Int32
	err := client.Materialize(t.Context(), req, 0, 10, func(_ ShardPlan, _ qviews.WorkNode, result *internalpb.MaterializeResults) error {
		received.Add(1)
		resource.ReleaseMaterializeBody(result)
		return nil
	})
	require.ErrorIs(t, err, merr.ErrServiceUnimplemented)
	require.EqualValues(t, 1, received.Load())
}

func TestMaterializeRetainedSizeBudgetIsIndependentPerShard(t *testing.T) {
	client, req := retainedMaterializeFixture()
	second := req.Plans[0]
	second.ShardID.VChannel = "p0_100v1"
	second.WorkNodes = second.WorkNodes[1:]
	req.Plans[0].WorkNodes = req.Plans[0].WorkNodes[:1]
	req.Plans = append(req.Plans, second)
	patch := mockey.Mock((*compositeViewQueryServiceClient).MaterializeOnView).To(func(_ *compositeViewQueryServiceClient, _ context.Context, _ qviews.WorkNode, _ *viewpb.MaterializeOnViewRequest) (*internalpb.MaterializeResults, error) {
		return &internalpb.MaterializeResults{Header: &internalpb.MaterializeHeader{Status: merr.Success(), ResultSize: 6}}, nil
	}).Build()
	defer patch.UnPatch()
	var received atomic.Int32
	err := client.Materialize(t.Context(), req, 0, 10, func(_ ShardPlan, _ qviews.WorkNode, result *internalpb.MaterializeResults) error {
		received.Add(1)
		resource.ReleaseMaterializeBody(result)
		return nil
	})
	require.NoError(t, err)
	require.EqualValues(t, 2, received.Load())
}

func TestMaterializeRetainedUnsupportedWinsOverSiblingFailure(t *testing.T) {
	client, req := retainedMaterializeFixture()
	patch := mockey.Mock((*compositeViewQueryServiceClient).MaterializeOnView).To(func(_ *compositeViewQueryServiceClient, _ context.Context, node qviews.WorkNode, _ *viewpb.MaterializeOnViewRequest) (*internalpb.MaterializeResults, error) {
		if node.NodeType() == qviews.NodeTypeStreamingNode {
			return nil, merr.ErrServiceUnimplemented
		}
		return nil, viewerror.NewViewInvalidated("retired")
	}).Build()
	defer patch.UnPatch()
	require.ErrorIs(t, client.Materialize(t.Context(), req, 0, 10, nil), merr.ErrServiceUnimplemented)
}
