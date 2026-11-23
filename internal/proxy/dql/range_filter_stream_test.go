package dql

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

func TestRangeAndIterativeFilterSearchType(t *testing.T) {
	for _, tc := range []struct {
		name   string
		params string
		hints  string
	}{
		{name: "range", params: `{"radius":10}`},
		{name: "range interval", params: `{"radius":10,"range_filter":1}`},
		{name: "iterative hint", params: `{}`, hints: iterativeFilterKey},
		{name: "iterative JSON hint", params: `{"hints":"iterative_filter"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			info, err := parseSearchInfo([]*commonpb.KeyValuePair{
				{Key: TopKKey, Value: "129"},
				{Key: ParamsKey, Value: tc.params},
				{Key: common.HintsKey, Value: tc.hints},
			}, nil, nil, false)
			require.NoError(t, err)
			require.Equal(t, internalpb.SearchType_DEFAULT, info.DetermineSearchType(false))
			require.Equal(t, internalpb.SearchType_DEFAULT, info.DetermineSearchType(true))
		})
	}
}
