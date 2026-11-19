package adaptor

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestFencedSourceCannotPlanAgainstPreLineageGeneration(t *testing.T) {
	old := &viewpb.QueryViewMeta{ShardSetEpoch: 0}
	require.NoError(t, requireSplitReadLineage(false, old))
	err := requireSplitReadLineage(true, old)
	require.True(t, viewerror.AsViewError(err).IsViewInvalidated())
	next := &viewpb.QueryViewMeta{ShardSetEpoch: 2, Lineage: &viewpb.QueryViewLineage{Vchannels: []string{"t1", "t2"}, SwitchTimetick: 100}}
	require.NoError(t, requireSplitReadLineage(true, next))
}

func TestSourceFenceInstalledWhileResolvingBarrierInvalidatesG0Plan(t *testing.T) {
	mockey.PatchConvey("append fence arrives while explicit B is being established", t, func() {
		fenced, released := false, false
		checks := 0
		mockey.Mock(shards.IsSplitFenced).To(func(shards.ShardManager, string) bool { checks++; return fenced }).Build()
		lease := &snview.QueryViewLease{Meta: &viewpb.QueryViewMeta{CollectionId: 100, Vchannel: "p_100v0", ReplicaId: 1}, Release: func() { released = true }}
		mockey.Mock((*snview.SNQueryViewHandler).AcquireUpViewAtGeneration).Return(lease, nil).Build()
		mockey.Mock((*walAdaptorImpl).resolveQueryPlanMVCC).To(func(_ *walAdaptorImpl, _ context.Context, _ *viewpb.GetQueryPlanRequest, _ string) (*viewpb.QueryPlanMVCC, error) {
			require.Equal(t, 1, checks)
			fenced = true // source append installed T_switch=100, before through covers B=150
			return &viewpb.QueryPlanMVCC{GrowingTimetick: 150, TransformingTimetick: 150}, nil
		}).Build()
		w := &walAdaptorImpl{roWALAdaptorImpl: &roWALAdaptorImpl{availableCtx: context.Background(), lifetime: typeutil.NewLifetime()}, param: &interceptors.InterceptorBuildParam{}, queryViewHandler: &snview.SNQueryViewHandler{}}
		response, err := w.GetQueryPlan(t.Context(), &viewpb.GetQueryPlanRequest{CollectionId: 100, ShardId: qviews.NewShardIDFromQVMeta(lease.Meta).IntoProto(), Mvcc: &viewpb.GetQueryPlanRequest_BarrierMvcc{BarrierMvcc: 150}, Request: &viewpb.GetQueryPlanRequest_LegacyRetrieveRequest{LegacyRetrieveRequest: &internalpb.RetrieveRequest{}}})
		require.Nil(t, response)
		require.True(t, viewerror.AsViewError(err).IsViewInvalidated())
		require.Equal(t, 2, checks)
		require.True(t, released)
	})
}
