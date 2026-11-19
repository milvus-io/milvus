package viewquery

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestSplitResidualRequestsReadAtCommonPoint(t *testing.T) {
	for _, point := range []uint64{105, 115} {
		mvcc := &viewpb.QueryPlanMVCC{GrowingTimetick: 100, TransformingTimetick: 100, ReadTimetick: point, LineageTransformingTimeticks: map[string]uint64{"target": 110}}
		search := &internalpb.SearchRequest{MvccTimestamp: 999}
		query := &internalpb.RetrieveRequest{MvccTimestamp: 999}
		require.Equal(t, point, buildSNSearchRequest(search, mvcc, "source", nil).GetReq().GetMvccTimestamp())
		require.Equal(t, point, buildSNQueryRequest(query, mvcc, "source", nil).GetReq().GetMvccTimestamp())
		require.Equal(t, uint64(999), search.GetMvccTimestamp())
		require.Equal(t, uint64(999), query.GetMvccTimestamp())
	}
	ordinary := &viewpb.QueryPlanMVCC{GrowingTimetick: 90, TransformingTimetick: 80}
	require.Equal(t, uint64(90), buildSNSearchRequest(&internalpb.SearchRequest{}, ordinary, "ordinary", nil).GetReq().GetMvccTimestamp())
	require.Equal(t, uint64(90), buildSNQueryRequest(&internalpb.RetrieveRequest{}, ordinary, "ordinary", nil).GetReq().GetMvccTimestamp())
}
