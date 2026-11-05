package snview

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestAcquireUpViewAtExactSchema(t *testing.T) {
	cat, resources, reports := newMockCatalog(), newMockResourceManager(), &reportCollector{}
	h := recoverSNQueryViewHandler(t.Context(), testPChannel, cat, resources, nil)
	for _, pair := range []struct {
		view   int64
		schema int32
	}{{1, 0}, {2, 1}, {3, 0}} {
		meta := buildHandlerTestMeta(pair.view)
		meta.SchemaVersion = pair.schema
		view := qviews.NewQueryViewAtStreamingNode(meta, &viewpb.QueryViewOfStreamingNode{})
		h.ApplyViews([]handler.ApplyView{{View: view, OnReport: reports.onReport}})
		acquired, ok := resources.getAcquired(view.QueryViewKey())
		require.True(t, ok)
		acquired.OnReady()
		next := buildHandlerTestMeta(pair.view)
		next.SchemaVersion, next.State = pair.schema, viewpb.QueryViewState_QueryViewStateUp
		h.ApplyViews([]handler.ApplyView{{View: qviews.NewQueryViewAtStreamingNode(next, &viewpb.QueryViewOfStreamingNode{}), OnReport: reports.onReport}})
	}
	shard := qviews.ShardID{ReplicaID: testReplicaID, VChannel: testVChannel}
	for _, replica := range []int64{testReplicaID, qviews.UnknownReplicaID} {
		shard.ReplicaID = replica
		for schema, expected := range map[int32]int64{0: 3, 1: 2} {
			lease, err := h.AcquireUpViewAtSchema(t.Context(), shard, schema)
			require.NoError(t, err)
			require.Equal(t, schema, lease.Meta.GetSchemaVersion())
			require.Equal(t, expected, lease.Version.QueryVersion)
			lease.Release()
		}
		_, err := h.AcquireUpViewAtSchema(t.Context(), shard, 2)
		require.True(t, viewerror.AsViewError(err).IsViewNotFound())
	}
}
