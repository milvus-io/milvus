package dql

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
)

func TestRangeAndIterativeFilterStreamingEligibility(t *testing.T) {
	tests := []struct {
		name       string
		params     string
		hints      string
		missing    bool
		function   bool
		subChain   bool
		searchType internalpb.SearchType
		want       bool
	}{
		{name: "range", params: `{"radius":10}`, want: true},
		{name: "range interval", params: `{"radius":10,"range_filter":1}`, want: true},
		{name: "iterative hint", params: `{}`, hints: iterativeFilterKey, want: true},
		{name: "iterative JSON hint", params: `{"hints":"iterative_filter"}`, want: true},
		{name: "unknown DEFAULT", params: `{}`},
		{name: "unrelated hint string", params: `{"comment":"iterative_filter"}`},
		{name: "missing query info", params: `{"radius":10}`, missing: true},
		{name: "function rerank", params: `{"radius":10}`, function: true},
		{name: "sub-search function chain", params: `{"radius":10}`, subChain: true},
		{name: "unknown type", params: `{"radius":10}`, searchType: internalpb.SearchType(99)},
	}
	for _, hybrid := range []bool{false, true} {
		for _, test := range tests {
			t.Run(map[bool]string{false: "ordinary/", true: "hybrid/"}[hybrid]+test.name, func(t *testing.T) {
				params := []*commonpb.KeyValuePair{
					{Key: TopKKey, Value: "129"},
					{Key: ParamsKey, Value: test.params},
					{Key: common.HintsKey, Value: test.hints},
				}
				info, err := parseSearchInfo(params, nil, nil, false)
				require.NoError(t, err)
				if test.want {
					require.Equal(t, internalpb.SearchType_DEFAULT, info.DetermineSearchType(false))
					require.Equal(t, internalpb.SearchType_DEFAULT, info.DetermineSearchType(true))
				}
				task := &SearchTask{
					SearchRequest: &internalpb.SearchRequest{Nq: 1, Topk: 129, SearchType: test.searchType},
					request:       &milvuspb.SearchRequest{},
					queryInfos:    []*planpb.QueryInfo{info.planInfo},
				}
				if hybrid {
					task.IsAdvanced = true
					task.rerankMeta = &legacyRerankMeta{}
					task.SubReqs = []*internalpb.SubSearchRequest{
						{SearchType: internalpb.SearchType_PURE_ANN_SEARCH_NO_FILTER},
						{SearchType: test.searchType},
					}
					task.queryInfos = []*planpb.QueryInfo{{}, info.planInfo}
					task.request.SubReqs = []*milvuspb.SubSearchRequest{{}, {}}
				}
				if test.missing {
					task.queryInfos = nil
				}
				if test.function || (test.subChain && !hybrid) {
					task.request.FunctionScore = &schemapb.FunctionScore{}
				}
				if test.subChain && hybrid {
					task.request.SubReqs[1].FunctionChains = []*schemapb.FunctionChain{{}}
				}
				require.Equal(t, test.want, task.supportsStreamingReduce())
				require.Equal(t, test.searchType, task.GetSearchType())
			})
		}
	}
}
