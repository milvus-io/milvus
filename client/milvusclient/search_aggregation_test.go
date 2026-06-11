// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package milvusclient

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSearchAggregationProtoBasic(t *testing.T) {
	agg := NewSearchAggregation([]string{"brand", "color"}, 5).
		WithSearchSize(20).
		WithMetric("avg_price", "AVG", "price").
		WithMetric("max_score", "max", "_score").
		WithMetric("doc_count", "count", "*").
		WithOrder("avg_price", "DESC").
		WithTopHits(NewTopHits(2).WithSort("_score", "ASC"))

	pb, err := agg.protoMessage()
	require.NoError(t, err)
	require.Equal(t, []string{"brand", "color"}, pb.GetFields())
	require.EqualValues(t, 5, pb.GetSize())
	require.EqualValues(t, 20, pb.GetSearchSize())
	require.Equal(t, "avg", pb.GetMetrics()["avg_price"].GetOp())
	require.Equal(t, "price", pb.GetMetrics()["avg_price"].GetFieldName())
	require.Equal(t, "max", pb.GetMetrics()["max_score"].GetOp())
	require.Equal(t, "_score", pb.GetMetrics()["max_score"].GetFieldName())
	require.Equal(t, "count", pb.GetMetrics()["doc_count"].GetOp())
	require.Equal(t, "*", pb.GetMetrics()["doc_count"].GetFieldName())
	require.Equal(t, "avg_price", pb.GetOrder()[0].GetKey())
	require.Equal(t, "desc", pb.GetOrder()[0].GetDirection())
	require.EqualValues(t, 2, pb.GetTopHits().GetSize())
	require.Equal(t, "_score", pb.GetTopHits().GetSort()[0].GetFieldName())
	require.Equal(t, "asc", pb.GetTopHits().GetSort()[0].GetDirection())
}

func TestSearchAggregationProtoNested(t *testing.T) {
	agg := NewSearchAggregation([]string{"category"}, 3).
		WithSubAggregation(
			NewSearchAggregation([]string{"brand", "color"}, 2).
				WithMetric("avg_rating", "avg", "rating").
				WithTopHits(NewTopHits(1).WithSort("price", "asc")),
		)

	pb, err := agg.protoMessage()
	require.NoError(t, err)
	require.Equal(t, []string{"brand", "color"}, pb.GetSubAggregation().GetFields())
	require.EqualValues(t, 2, pb.GetSubAggregation().GetSize())
	require.EqualValues(t, 1, pb.GetSubAggregation().GetTopHits().GetSize())
}

func TestSearchAggregationProtoBuildsDeepNesting(t *testing.T) {
	agg := NewSearchAggregation([]string{"level_0"}, 1)
	for i := 1; i < 6; i++ {
		agg = NewSearchAggregation([]string{fmt.Sprintf("level_%d", i)}, 1).
			WithSubAggregation(agg)
	}
	pb, err := agg.protoMessage()
	require.NoError(t, err)

	depth := 1
	for cur := pb; cur.GetSubAggregation() != nil; cur = cur.GetSubAggregation() {
		depth++
	}
	require.Equal(t, 6, depth)
}

func TestSearchAggregationValidate(t *testing.T) {
	cases := []struct {
		name string
		agg  *SearchAggregation
	}{
		{name: "empty fields", agg: NewSearchAggregation(nil, 3)},
		{name: "empty field", agg: NewSearchAggregation([]string{""}, 3)},
		{name: "json path field", agg: NewSearchAggregation([]string{`meta["region"]`}, 3)},
		{name: "zero size", agg: NewSearchAggregation([]string{"brand"}, 0)},
		{name: "negative size", agg: NewSearchAggregation([]string{"brand"}, -1)},
		{name: "search size less than size", agg: NewSearchAggregation([]string{"brand"}, 4).WithSearchSize(3)},
		{name: "negative search size", agg: NewSearchAggregation([]string{"brand"}, 4).WithSearchSize(-1)},
		{name: "empty metric alias", agg: NewSearchAggregation([]string{"brand"}, 3).WithMetric("", "avg", "price")},
		{name: "unsupported metric op", agg: NewSearchAggregation([]string{"brand"}, 3).WithMetric("m", "median", "price")},
		{name: "empty metric field", agg: NewSearchAggregation([]string{"brand"}, 3).WithMetric("m", "avg", "")},
		{name: "json metric field", agg: NewSearchAggregation([]string{"brand"}, 3).WithMetric("m", "avg", `meta["price"]`)},
		{name: "star for non count", agg: NewSearchAggregation([]string{"brand"}, 3).WithMetric("m", "avg", "*")},
		{name: "unknown order key", agg: NewSearchAggregation([]string{"brand"}, 3).WithOrder("missing", "desc")},
		{name: "empty order direction", agg: NewSearchAggregation([]string{"brand"}, 3).WithOrder("_count", "")},
		{name: "bad order direction", agg: NewSearchAggregation([]string{"brand"}, 3).WithOrder("_count", "down")},
		{name: "bad top hits size", agg: NewSearchAggregation([]string{"brand"}, 3).WithTopHits(NewTopHits(0))},
		{name: "empty top hits sort field", agg: NewSearchAggregation([]string{"brand"}, 3).WithTopHits(NewTopHits(1).WithSort("", "asc"))},
		{name: "json top hits sort field", agg: NewSearchAggregation([]string{"brand"}, 3).WithTopHits(NewTopHits(1).WithSort(`meta["price"]`, "asc"))},
		{name: "bad top hits sort direction", agg: NewSearchAggregation([]string{"brand"}, 3).WithTopHits(NewTopHits(1).WithSort("price", "up"))},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Error(t, tc.agg.Validate())
		})
	}
}
