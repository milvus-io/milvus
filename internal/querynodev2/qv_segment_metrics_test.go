package querynodev2

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/querynodev2/qvresource"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
)

func TestQueryViewSegmentMetricsRegistration(t *testing.T) {
	node := &QueryNode{serverID: 5, manager: &segments.Manager{Segment: segments.NewSegmentManager()}}
	// An unavailable factory must return a nil interface, not a typed nil.
	require.Nil(t, node.NewQueryViewSegmentManager(t.Context(), nil, nil, nil))
	require.Nil(t, node.queryViewSegments.Load())

	mockey.PatchConvey("metrics reads the same resource manager used by QueryViews", t, func() {
		resources := &qnview.SegmentResourceManager{}
		mockey.Mock(qvresource.NewQueryViewSegmentManager).Return(resources).Build()
		require.Same(t, resources, node.NewQueryViewSegmentManager(t.Context(), nil, nil, nil))
		require.Same(t, resources, node.queryViewSegments.Load())

		mockey.Mock((*qnview.SegmentResourceManager).SegmentMetrics).To(func(m *qnview.SegmentResourceManager, collectionID, nodeID int64) []*metricsinfo.Segment {
			require.Same(t, resources, m)
			require.EqualValues(t, 1, collectionID)
			require.EqualValues(t, 5, nodeID)
			return []*metricsinfo.Segment{{SegmentID: 10, CollectionID: collectionID, NodeID: nodeID, MemSize: 50}}
		}).Build()

		var result []*metricsinfo.Segment
		require.NoError(t, json.Unmarshal([]byte(getSegmentJSON(node, 1)), &result))
		require.Len(t, result, 1, "QV resources must be visible even when the legacy segment manager is empty")
		require.EqualValues(t, 10, result[0].SegmentID)
		require.EqualValues(t, 50, result[0].MemSize)
	})

	// The published manager remains readable after all physical resources retire.
	var result []*metricsinfo.Segment
	require.NoError(t, json.Unmarshal([]byte(getSegmentJSON(node, 1)), &result))
	require.Empty(t, result)
	var absent *QueryNode
	require.Nil(t, absent.NewQueryViewSegmentManager(context.Background(), nil, nil, nil))
}
