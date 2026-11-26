package querynodev2

import (
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestQueryViewResourceWatermarksNodeLifecycle(t *testing.T) {
	mockey.PatchConvey("sample only between initialization and shutdown", t, func() {
		paramtable.Init()
		node := NewQueryNode(t.Context(), nil)
		require.Nil(t, node.QueryViewResourceWatermarks())
		node.loader = &segments.MockLoader{}
		want := &viewpb.NodeResourceWatermarks{Memory: 70, Disk: 40}
		patch := mockey.Mock((*segments.MockLoader).GetResourceWatermarks).Return(want).Build()
		node.UpdateStateCode(commonpb.StateCode_Healthy)
		require.Same(t, want, node.QueryViewResourceWatermarks())
		node.UpdateStateCode(commonpb.StateCode_Stopping)
		require.Nil(t, node.QueryViewResourceWatermarks())
		require.Equal(t, 1, patch.Times())
	})
}
