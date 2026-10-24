package qviews

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestQueryPlanPayloadRoundTrip(t *testing.T) {
	for _, hybrid := range []bool{false, true} {
		input := &internalpb.SearchRequest{PlaceholderGroup: bytes.Repeat([]byte{7}, 1<<20), SerializedExprPlan: []byte("expression"), PartitionIDs: []int64{1}}
		if hybrid {
			input.SubReqs = []*internalpb.SubSearchRequest{
				{PlaceholderGroup: bytes.Repeat([]byte{3}, 1<<20), SerializedExprPlan: []byte("unchanged")},
				{PlaceholderGroup: []byte("BM25 input"), SerializedExprPlan: []byte("before")},
			}
		}
		original := &viewpb.GetQueryPlanRequest{Request: &viewpb.GetQueryPlanRequest_LegacySearchRequest{LegacySearchRequest: input}}
		optimized := proto.Clone(input).(*internalpb.SearchRequest)
		optimized.PartitionIDs = []int64{1, 2}
		if hybrid {
			optimized.SubReqs[1].PlaceholderGroup = []byte("BM25 rewritten")
			optimized.SubReqs[1].SerializedExprPlan = nil // Explicit clearing must survive.
			optimized.SubReqs[1].Skip = true
		}
		want := proto.Clone(optimized)
		before := proto.Clone(original)
		plan := &viewpb.QueryPlan{Request: &viewpb.QueryPlan_LegacySearchRequest{LegacySearchRequest: optimized}}
		OmitUnchangedQueryPayloads(plan, original)
		require.Less(t, proto.Size(plan), 256)
		wire, err := proto.Marshal(plan)
		require.NoError(t, err)
		received := new(viewpb.QueryPlan)
		require.NoError(t, proto.Unmarshal(wire, received))
		require.NoError(t, RestoreQueryPlanPayloads(received, original))
		require.True(t, proto.Equal(want, received.GetLegacySearchRequest()))
		require.True(t, proto.Equal(before, original))
	}
}

func TestQueryPlanRetrievePayloadRoundTrip(t *testing.T) {
	input := &internalpb.RetrieveRequest{SerializedExprPlan: bytes.Repeat([]byte{1}, 1<<20)}
	original := &viewpb.GetQueryPlanRequest{Request: &viewpb.GetQueryPlanRequest_LegacyRetrieveRequest{LegacyRetrieveRequest: input}}
	for _, clear := range []bool{false, true} {
		optimized := proto.Clone(input).(*internalpb.RetrieveRequest)
		if clear {
			optimized.SerializedExprPlan = nil
		}
		want := proto.Clone(optimized)
		plan := &viewpb.QueryPlan{Request: &viewpb.QueryPlan_LegacyRetrieveRequest{LegacyRetrieveRequest: optimized}}
		OmitUnchangedQueryPayloads(plan, original)
		require.Less(t, proto.Size(plan), 64)
		require.NoError(t, RestoreQueryPlanPayloads(plan, original))
		require.True(t, proto.Equal(want, plan.GetLegacyRetrieveRequest()))
	}
}
