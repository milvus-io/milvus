package adaptor

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestPrewarmPlanBorrowsExactTopologyWithoutQueryResources(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(map[bool]string{false: "matching", true: "reconfigured"}[changed], func(t *testing.T) {
			shard := qviews.ShardID{ReplicaID: 3, VChannel: "p_100v0"}
			version := qviews.QueryViewVersion{QueryVersion: 9}
			view := &viewpb.QueryViewOfShard{
				Meta:          &viewpb.QueryViewMeta{CollectionId: 100, ReplicaId: 3, Vchannel: shard.VChannel, Version: version.IntoProto(), LoadInfoVersion: 7},
				StreamingNode: &viewpb.QueryViewOfStreamingNode{},
				QueryNode:     []*viewpb.QueryViewOfQueryNode{{NodeId: 11, Partitions: []*viewpb.QueryViewOfPartition{{SegmentIds: []int64{101}}}}, {NodeId: 12, Partitions: []*viewpb.QueryViewOfPartition{{SegmentIds: []int64{102}}}}},
			}
			released, renewed := false, false
			lease := &snview.QueryViewLease{Meta: view.Meta, View: view, Version: version, Release: func() { released = true }, Renew: func() { require.False(t, released); renewed = true }}
			patch := mockey.Mock((*snview.SNQueryViewHandler).AcquireUpView).To(func(_ *snview.SNQueryViewHandler, _ context.Context, id qviews.ShardID, v qviews.QueryViewVersion) (*snview.QueryViewLease, error) {
				require.Equal(t, shard, id)
				require.Equal(t, version, v)
				return lease, nil
			}).Build()
			defer patch.UnPatch()
			// No MVCC manager, query runtime, optimizer or segment manager is installed.
			w := &walAdaptorImpl{roWALAdaptorImpl: &roWALAdaptorImpl{availableCtx: t.Context(), lifetime: typeutil.NewLifetime()}, queryViewHandler: &snview.SNQueryViewHandler{}}
			req := &viewpb.GetPrewarmPlanRequest{CollectionId: 100, ShardId: shard.IntoProto(), Version: version.IntoProto(), LoadInfoVersion: 7}
			if changed {
				req.LoadInfoVersion = 8
			}
			resp, err := w.GetPrewarmPlan(t.Context(), req)
			require.True(t, released)
			require.Equal(t, !changed, renewed)
			if changed {
				require.True(t, viewerror.AsViewError(err).IsViewInvalidated())
				return
			}
			require.NoError(t, err)
			require.Equal(t, version.IntoProto(), resp.Plan.Version)
			require.Len(t, resp.Plan.WorkNodes, 3)
			require.Nil(t, resp.Plan.Mvcc)
			require.Nil(t, resp.Plan.Request)
		})
	}
}
