package adaptor

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/queryresource"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestQueryPlanPrunesSNUsingSelectedViewDataVersion(t *testing.T) {
	sealedAt := qviews.DataVersion{StreamingVersion: 10, CompactVersion: 2}
	patch := mockey.Mock((*queryresource.QueryRuntime).MayHaveVisibleGrowingSegments).To(func(_ *queryresource.QueryRuntime, version qviews.DataVersion, growing uint64, transform uint64, partitions []int64) bool {
		require.Equal(t, uint64(100), growing)
		require.Equal(t, uint64(90), transform)
		require.Equal(t, []int64{100}, partitions)
		return sealedAt.GT(version)
	}).Build()
	defer patch.UnPatch()
	options := queryPlanWorkNodeOptions{runtime: &queryresource.QueryRuntime{}, partitionIDs: []int64{100}, mvcc: &viewpb.QueryPlanMVCC{GrowingTimetick: 100, TransformingTimetick: 90}}
	view := &viewpb.QueryViewOfShard{Meta: &viewpb.QueryViewMeta{Vchannel: "p_1v0", Version: &viewpb.QueryViewVersion{DataVersion: &viewpb.DataVersion{StreamingVersion: 9}}}, StreamingNode: &viewpb.QueryViewOfStreamingNode{}}
	require.True(t, queryPlanIncludesStreamingNode(view, options))
	view.Meta.Version.DataVersion = sealedAt.IntoProto()
	require.False(t, queryPlanIncludesStreamingNode(view, options))
}

func TestQueryPlanRenewsLeaseOnlyOnSuccessfulReturn(t *testing.T) {
	view := &viewpb.QueryViewOfShard{Meta: &viewpb.QueryViewMeta{CollectionId: 1, Vchannel: "p_1v0", ReplicaId: 1, Version: &viewpb.QueryViewVersion{DataVersion: &viewpb.DataVersion{StreamingVersion: 1}, QueryVersion: 1}}}
	for _, valid := range []bool{false, true} {
		renewed, released := false, false
		lease := &snview.QueryViewLease{Meta: view.Meta, View: view, Version: qviews.FromProtoQueryViewVersion(view.Meta.Version), Renew: func() { require.False(t, released); renewed = true }, Release: func() { released = true }}
		patch := mockey.Mock((*snview.SNQueryViewHandler).AcquireLatestUpView).Return(lease, nil).Build()
		w := &walAdaptorImpl{roWALAdaptorImpl: &roWALAdaptorImpl{lifetime: typeutil.NewLifetime()}, queryViewHandler: &snview.SNQueryViewHandler{}}
		req := &viewpb.GetQueryPlanRequest{ShardId: qviews.NewShardIDFromQVMeta(view.Meta).IntoProto(), Mvcc: &viewpb.GetQueryPlanRequest_QueryPlanMvcc{QueryPlanMvcc: &viewpb.QueryPlanMVCC{GrowingTimetick: 1}}}
		if valid {
			req.Request = &viewpb.GetQueryPlanRequest_LegacyRetrieveRequest{LegacyRetrieveRequest: &internalpb.RetrieveRequest{CollectionID: 1}}
		}
		_, err := w.GetQueryPlan(context.Background(), req)
		patch.UnPatch()
		if valid {
			require.NoError(t, err)
		} else {
			require.Error(t, err)
		}
		require.Equal(t, valid, renewed)
		require.True(t, released)
	}
}
