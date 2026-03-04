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

package sqlparser

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestColumnRefMilvusExpr(t *testing.T) {
	tests := []struct {
		name     string
		col      ColumnRef
		expected string
	}{
		{
			name:     "simple field",
			col:      ColumnRef{FieldName: "id"},
			expected: "id",
		},
		{
			name:     "one level JSON path",
			col:      ColumnRef{FieldName: "data", NestedPath: []string{"kind"}, IsText: true},
			expected: `data["kind"]`,
		},
		{
			name: "two level JSON path",
			col: ColumnRef{
				FieldName:  "data",
				NestedPath: []string{"commit", "collection"},
				IsText:     true,
			},
			expected: `data["commit"]["collection"]`,
		},
		{
			name: "three level JSON path",
			col: ColumnRef{
				FieldName:  "data",
				NestedPath: []string{"a", "b", "c"},
				IsText:     false,
			},
			expected: `data["a"]["b"]["c"]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.col.MilvusExpr())
		})
	}
}

func TestVectorDistOpMetricType(t *testing.T) {
	assert.Equal(t, "L2", VectorDistL2.MetricType())
	assert.Equal(t, "COSINE", VectorDistCosine.MetricType())
	assert.Equal(t, "IP", VectorDistIP.MetricType())
}

// TestSqlComponentsQ1 tests that SqlComponents can represent JSONBench Q1.
//
// Q1: SELECT data->'commit'->>'collection' AS event, count(*) AS count
//
//	FROM bluesky GROUP BY event ORDER BY count DESC
func TestSqlComponentsQ1(t *testing.T) {
	q1 := SqlComponents{
		Collection: "bluesky",
		SelectItems: []SelectItem{
			{
				Type: SelectColumn,
				Column: &ColumnRef{
					FieldName:  "data",
					NestedPath: []string{"commit", "collection"},
					IsText:     true,
				},
				Alias: "event",
			},
			{
				Type: SelectAgg,
				AggFunc: &AggFuncCall{
					FuncName: "count",
					Arg:      nil, // count(*)
				},
				Alias: "count",
			},
		},
		GroupBy: []GroupByItem{
			{Alias: "event"},
		},
		OrderBy: []OrderItem{
			{Ref: "count", Desc: true},
		},
		Limit:     -1,
		QueryType: QueryTypeQuery,
	}

	assert.Equal(t, "bluesky", q1.Collection)
	assert.Len(t, q1.SelectItems, 2)
	assert.Len(t, q1.GroupBy, 1)
	assert.Len(t, q1.OrderBy, 1)
	assert.Equal(t, int64(-1), q1.Limit)
	assert.Nil(t, q1.VectorSearch)
	assert.Equal(t, QueryTypeQuery, q1.QueryType)
}

// TestSqlComponentsQ2 tests that SqlComponents can represent JSONBench Q2.
//
// Q2 has WHERE, count(DISTINCT ...), and 3 select items.
func TestSqlComponentsQ2(t *testing.T) {
	q2 := SqlComponents{
		Collection: "bluesky",
		SelectItems: []SelectItem{
			{
				Type: SelectColumn,
				Column: &ColumnRef{
					FieldName:  "data",
					NestedPath: []string{"commit", "collection"},
					IsText:     true,
				},
				Alias: "event",
			},
			{
				Type:    SelectAgg,
				AggFunc: &AggFuncCall{FuncName: "count", Arg: nil},
				Alias:   "count",
			},
			{
				Type: SelectAgg,
				AggFunc: &AggFuncCall{
					FuncName: "count",
					Arg:      &ColumnRef{FieldName: "data", NestedPath: []string{"did"}, IsText: true},
					Distinct: true,
				},
				Alias: "users",
			},
		},
		Where: &WhereClause{
			MilvusExpr: `data["kind"] == "commit" && data["commit"]["operation"] == "create"`,
		},
		GroupBy: []GroupByItem{
			{Alias: "event"},
		},
		OrderBy: []OrderItem{
			{Ref: "count", Desc: true},
		},
		Limit:     -1,
		QueryType: QueryTypeQuery,
	}

	assert.Equal(t, "bluesky", q2.Collection)
	assert.Len(t, q2.SelectItems, 3)
	assert.NotNil(t, q2.Where)
	assert.True(t, q2.SelectItems[2].AggFunc.Distinct)
}

// TestSqlComponentsQ3 tests Q3 with computed expressions.
func TestSqlComponentsQ3(t *testing.T) {
	q3 := SqlComponents{
		Collection: "bluesky",
		SelectItems: []SelectItem{
			{
				Type: SelectColumn,
				Column: &ColumnRef{
					FieldName:  "data",
					NestedPath: []string{"commit", "collection"},
					IsText:     true,
				},
				Alias: "event",
			},
			{
				Type: SelectComputed,
				Computed: &FunctionCall{
					FuncName: "extract",
					Args: []interface{}{
						"hour",
						&FunctionCall{
							FuncName: "to_timestamp",
							Args: []interface{}{
								&ColumnRef{
									FieldName:  "data",
									NestedPath: []string{"time_us"},
									IsText:     true,
									CastType:   "bigint",
								},
							},
						},
					},
				},
				Alias: "hour_of_day",
			},
			{
				Type:    SelectAgg,
				AggFunc: &AggFuncCall{FuncName: "count", Arg: nil},
				Alias:   "count",
			},
		},
		Where: &WhereClause{
			MilvusExpr: `data["kind"] == "commit" && data["commit"]["operation"] == "create" && data["commit"]["collection"] in ["app.bsky.feed.post", "app.bsky.feed.repost", "app.bsky.feed.like"]`,
		},
		GroupBy: []GroupByItem{
			{Alias: "event"},
			{Alias: "hour_of_day"},
		},
		OrderBy: []OrderItem{
			{Ref: "hour_of_day", Desc: false},
			{Ref: "event", Desc: false},
		},
		Limit:     -1,
		QueryType: QueryTypeQuery,
	}

	assert.Len(t, q3.SelectItems, 3)
	assert.Equal(t, SelectComputed, q3.SelectItems[1].Type)
	assert.Equal(t, "extract", q3.SelectItems[1].Computed.FuncName)
	assert.Len(t, q3.GroupBy, 2)
	assert.Len(t, q3.OrderBy, 2)
}

// TestSqlComponentsQ4 tests Q4 with LIMIT.
func TestSqlComponentsQ4(t *testing.T) {
	q4 := SqlComponents{
		Collection: "bluesky",
		SelectItems: []SelectItem{
			{
				Type: SelectColumn,
				Column: &ColumnRef{
					FieldName:  "data",
					NestedPath: []string{"did"},
					IsText:     true,
					CastType:   "text",
				},
				Alias: "user_id",
			},
			{
				Type: SelectAgg,
				AggFunc: &AggFuncCall{
					FuncName: "min",
					Arg: &ColumnRef{
						FieldName:  "data",
						NestedPath: []string{"time_us"},
						IsText:     true,
						CastType:   "bigint",
					},
				},
				Alias: "first_post_ts",
			},
		},
		Where: &WhereClause{
			MilvusExpr: `data["kind"] == "commit" && data["commit"]["operation"] == "create" && data["commit"]["collection"] == "app.bsky.feed.post"`,
		},
		GroupBy: []GroupByItem{
			{Alias: "user_id"},
		},
		OrderBy: []OrderItem{
			{Ref: "first_post_ts", Desc: false},
		},
		Limit:     3,
		QueryType: QueryTypeQuery,
	}

	assert.Equal(t, int64(3), q4.Limit)
	assert.Equal(t, "text", q4.SelectItems[0].Column.CastType)
}

// TestSqlComponentsVectorSearch tests that SqlComponents can represent
// a vector search query.
func TestSqlComponentsVectorSearch(t *testing.T) {
	vs := SqlComponents{
		Collection: "articles",
		SelectItems: []SelectItem{
			{
				Type:   SelectColumn,
				Column: &ColumnRef{FieldName: "id"},
				Alias:  "",
			},
			{
				Type:   SelectColumn,
				Column: &ColumnRef{FieldName: "title"},
				Alias:  "",
			},
			{
				Type: SelectVectorDist,
				Distance: &VectorDistExpr{
					Column:   &ColumnRef{FieldName: "embedding"},
					Operator: VectorDistL2,
					Vector:   []float32{0.1, 0.2, 0.3},
				},
				Alias: "distance",
			},
		},
		OrderBy: []OrderItem{
			{Ref: "distance", Desc: false, IsDistance: true},
		},
		Limit: 10,
		VectorSearch: &VectorSearchDef{
			FieldName:  "embedding",
			MetricType: "L2",
			Vector:     []float32{0.1, 0.2, 0.3},
			TopK:       10,
			DistAlias:  "distance",
		},
		QueryType: QueryTypeSearch,
	}

	assert.Equal(t, QueryTypeSearch, vs.QueryType)
	assert.NotNil(t, vs.VectorSearch)
	assert.Equal(t, "L2", vs.VectorSearch.MetricType)
	assert.Equal(t, int64(10), vs.VectorSearch.TopK)
	assert.Len(t, vs.VectorSearch.Vector, 3)
}

// TestSqlComponentsHybridSearch tests vector search + scalar filter.
func TestSqlComponentsHybridSearch(t *testing.T) {
	hs := SqlComponents{
		Collection: "articles",
		SelectItems: []SelectItem{
			{
				Type:   SelectColumn,
				Column: &ColumnRef{FieldName: "id"},
			},
			{
				Type: SelectVectorDist,
				Distance: &VectorDistExpr{
					Column:   &ColumnRef{FieldName: "embedding"},
					Operator: VectorDistCosine,
					Vector:   []float32{0.5, 0.5},
				},
				Alias: "score",
			},
		},
		Where: &WhereClause{
			MilvusExpr: `category == "tech"`,
		},
		OrderBy: []OrderItem{
			{Ref: "score", Desc: false, IsDistance: true},
		},
		Limit: 5,
		VectorSearch: &VectorSearchDef{
			FieldName:  "embedding",
			MetricType: "COSINE",
			Vector:     []float32{0.5, 0.5},
			TopK:       5,
			DistAlias:  "score",
		},
		QueryType: QueryTypeHybrid,
	}

	assert.Equal(t, QueryTypeHybrid, hs.QueryType)
	assert.NotNil(t, hs.VectorSearch)
	assert.NotNil(t, hs.Where)
	assert.Equal(t, "COSINE", hs.VectorSearch.MetricType)
}
