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
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"

	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// buildTestSchema creates a schema matching the JSONBench data model:
// - id (Int64, PK)
// - data (JSON) — the main JSON document field
func buildTestSchema(t *testing.T) *typeutil.SchemaHelper {
	fields := []*schemapb.FieldSchema{
		{FieldID: 1, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 2, Name: "data", DataType: schemapb.DataType_JSON},
	}
	schema := &schemapb.CollectionSchema{
		Name:   "github_events",
		AutoID: false,
		Fields: fields,
	}
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	return helper
}

// TestBuildQueryPlan_SimpleQuery tests a basic SELECT with WHERE and LIMIT.
func TestBuildQueryPlan_SimpleQuery(t *testing.T) {
	schema := buildTestSchema(t)

	comp := &SqlComponents{
		Collection: "github_events",
		SelectItems: []SelectItem{
			{Type: SelectColumn, Column: &ColumnRef{FieldName: "data", NestedPath: []string{"commit"}, IsText: true}},
		},
		Where: &WhereClause{
			MilvusExpr: `data["kind"] == "PushEvent"`,
		},
		Limit: 10,
	}

	plan, err := BuildQueryPlan(comp, schema)
	require.NoError(t, err)
	require.NotNil(t, plan)

	query := plan.GetQuery()
	require.NotNil(t, query)
	assert.NotNil(t, query.Predicates, "should have predicates from WHERE")
	assert.Equal(t, int64(10), query.Limit)
	assert.Nil(t, query.AggregateNode, "no aggregation in simple query")
	assert.Nil(t, query.SortNode, "no ORDER BY")

	// Output should include data field
	assert.Contains(t, plan.OutputFieldIds, int64(2))
}

// TestBuildQueryPlan_NoWhere tests a query without WHERE clause.
func TestBuildQueryPlan_NoWhere(t *testing.T) {
	schema := buildTestSchema(t)

	comp := &SqlComponents{
		Collection: "github_events",
		SelectItems: []SelectItem{
			{Type: SelectColumn, Column: &ColumnRef{FieldName: "data"}},
		},
		Limit: -1,
	}

	plan, err := BuildQueryPlan(comp, schema)
	require.NoError(t, err)
	require.NotNil(t, plan)

	query := plan.GetQuery()
	assert.Nil(t, query.Predicates)
	assert.Equal(t, int64(0), query.Limit) // -1 becomes 0 (no limit set)
}

// TestBuildQueryPlan_Q1 tests JSONBench Q1 pattern:
// SELECT data->>'commit' as k, count(*) as cnt
// FROM github_events
// WHERE data->>'kind' = 'PushEvent'
// GROUP BY data->>'commit'
// ORDER BY cnt DESC
// LIMIT 20
func TestBuildQueryPlan_Q1(t *testing.T) {
	schema := buildTestSchema(t)

	comp := &SqlComponents{
		Collection: "github_events",
		SelectItems: []SelectItem{
			{
				Type:   SelectColumn,
				Column: &ColumnRef{FieldName: "data", NestedPath: []string{"commit"}, IsText: true},
				Alias:  "k",
			},
			{
				Type:    SelectAgg,
				AggFunc: &AggFuncCall{FuncName: "count"},
				Alias:   "cnt",
			},
		},
		Where: &WhereClause{
			MilvusExpr: `data["kind"] == "PushEvent"`,
		},
		GroupBy: []GroupByItem{
			{Column: &ColumnRef{FieldName: "data", NestedPath: []string{"commit"}, IsText: true}},
		},
		OrderBy: []OrderItem{
			{Ref: "cnt", Desc: true},
		},
		Limit: 20,
	}

	plan, err := BuildQueryPlan(comp, schema)
	require.NoError(t, err)
	require.NotNil(t, plan)

	query := plan.GetQuery()
	require.NotNil(t, query)

	// WHERE
	assert.NotNil(t, query.Predicates)

	// AggregateNode
	aggNode := query.AggregateNode
	require.NotNil(t, aggNode)

	// GROUP BY: data->>'commit'
	require.Len(t, aggNode.GroupBy, 1)
	assert.Equal(t, int64(2), aggNode.GroupBy[0].FieldId) // data field
	assert.Equal(t, []string{"commit"}, aggNode.GroupBy[0].NestedPath)

	// Aggregates: count(*)
	require.Len(t, aggNode.Aggregates, 1)
	assert.Equal(t, planpb.AggregateOp_count, aggNode.Aggregates[0].Op)
	assert.Equal(t, int64(0), aggNode.Aggregates[0].FieldId) // count(*) has no field

	// SortNode: cnt DESC
	sortNode := query.SortNode
	require.NotNil(t, sortNode)
	require.Len(t, sortNode.Items, 1)
	assert.Equal(t, "cnt", sortNode.Items[0].Field)
	assert.True(t, sortNode.Items[0].Desc)

	// LIMIT
	assert.Equal(t, int64(20), query.Limit)
}

// TestBuildQueryPlan_Q2 tests JSONBench Q2 pattern:
// SELECT count(distinct data->>'author') FROM github_events WHERE ...
func TestBuildQueryPlan_Q2(t *testing.T) {
	schema := buildTestSchema(t)

	comp := &SqlComponents{
		Collection: "github_events",
		SelectItems: []SelectItem{
			{
				Type:    SelectAgg,
				AggFunc: &AggFuncCall{FuncName: "count", Arg: &ColumnRef{FieldName: "data", NestedPath: []string{"author"}, IsText: true}, Distinct: true},
			},
		},
		Where: &WhereClause{
			MilvusExpr: `data["kind"] == "PushEvent"`,
		},
		Limit: -1,
	}

	plan, err := BuildQueryPlan(comp, schema)
	require.NoError(t, err)

	aggNode := plan.GetQuery().GetAggregateNode()
	require.NotNil(t, aggNode)
	require.Len(t, aggNode.Aggregates, 1)

	agg := aggNode.Aggregates[0]
	assert.Equal(t, planpb.AggregateOp_count_distinct, agg.Op)
	assert.Equal(t, int64(2), agg.FieldId) // data field
	assert.Equal(t, []string{"author"}, agg.NestedPath)
}

// TestBuildQueryPlan_Q3 tests JSONBench Q3 pattern:
// SELECT data->>'repo' as repo, count(*) as cnt, sum((data->>'commits')::int) as total
// FROM github_events WHERE ... GROUP BY data->>'repo' ORDER BY total DESC LIMIT 10
func TestBuildQueryPlan_Q3(t *testing.T) {
	schema := buildTestSchema(t)

	comp := &SqlComponents{
		Collection: "github_events",
		SelectItems: []SelectItem{
			{
				Type:   SelectColumn,
				Column: &ColumnRef{FieldName: "data", NestedPath: []string{"repo"}, IsText: true},
				Alias:  "repo",
			},
			{
				Type:    SelectAgg,
				AggFunc: &AggFuncCall{FuncName: "count"},
				Alias:   "cnt",
			},
			{
				Type:    SelectAgg,
				AggFunc: &AggFuncCall{FuncName: "sum", Arg: &ColumnRef{FieldName: "data", NestedPath: []string{"commits"}, IsText: true, CastType: "int"}},
				Alias:   "total",
			},
		},
		Where: &WhereClause{
			MilvusExpr: `data["kind"] == "PushEvent"`,
		},
		GroupBy: []GroupByItem{
			{Column: &ColumnRef{FieldName: "data", NestedPath: []string{"repo"}, IsText: true}},
		},
		OrderBy: []OrderItem{
			{Ref: "total", Desc: true},
		},
		Limit: 10,
	}

	plan, err := BuildQueryPlan(comp, schema)
	require.NoError(t, err)

	query := plan.GetQuery()
	aggNode := query.AggregateNode
	require.NotNil(t, aggNode)

	// GROUP BY
	require.Len(t, aggNode.GroupBy, 1)
	assert.Equal(t, []string{"repo"}, aggNode.GroupBy[0].NestedPath)

	// Aggregates: count(*), sum(data->>'commits')
	require.Len(t, aggNode.Aggregates, 2)
	assert.Equal(t, planpb.AggregateOp_count, aggNode.Aggregates[0].Op)
	assert.Equal(t, planpb.AggregateOp_sum, aggNode.Aggregates[1].Op)
	assert.Equal(t, []string{"commits"}, aggNode.Aggregates[1].NestedPath)

	// ORDER BY total DESC
	require.Len(t, query.SortNode.Items, 1)
	assert.Equal(t, "total", query.SortNode.Items[0].Field)
	assert.True(t, query.SortNode.Items[0].Desc)

	// LIMIT
	assert.Equal(t, int64(10), query.Limit)
}

// TestBuildQueryPlan_GroupByAlias tests GROUP BY referencing a SELECT alias.
func TestBuildQueryPlan_GroupByAlias(t *testing.T) {
	schema := buildTestSchema(t)

	comp := &SqlComponents{
		Collection: "github_events",
		SelectItems: []SelectItem{
			{
				Type:   SelectColumn,
				Column: &ColumnRef{FieldName: "data", NestedPath: []string{"kind"}, IsText: true},
				Alias:  "k",
			},
			{
				Type:    SelectAgg,
				AggFunc: &AggFuncCall{FuncName: "count"},
				Alias:   "cnt",
			},
		},
		GroupBy: []GroupByItem{
			{Alias: "k"}, // references SELECT alias
		},
		Limit: -1,
	}

	plan, err := BuildQueryPlan(comp, schema)
	require.NoError(t, err)

	aggNode := plan.GetQuery().GetAggregateNode()
	require.NotNil(t, aggNode)
	require.Len(t, aggNode.GroupBy, 1)
	assert.Equal(t, int64(2), aggNode.GroupBy[0].FieldId)
	assert.Equal(t, []string{"kind"}, aggNode.GroupBy[0].NestedPath)
}

// TestBuildQueryPlan_AggOnly tests aggregation without GROUP BY.
func TestBuildQueryPlan_AggOnly(t *testing.T) {
	schema := buildTestSchema(t)

	comp := &SqlComponents{
		Collection: "github_events",
		SelectItems: []SelectItem{
			{
				Type:    SelectAgg,
				AggFunc: &AggFuncCall{FuncName: "min", Arg: &ColumnRef{FieldName: "data", NestedPath: []string{"created_at"}, IsText: true}},
			},
		},
		Where: &WhereClause{
			MilvusExpr: `data["kind"] == "PushEvent"`,
		},
		Limit: -1,
	}

	plan, err := BuildQueryPlan(comp, schema)
	require.NoError(t, err)

	aggNode := plan.GetQuery().GetAggregateNode()
	require.NotNil(t, aggNode)
	assert.Len(t, aggNode.GroupBy, 0) // no GROUP BY
	require.Len(t, aggNode.Aggregates, 1)
	assert.Equal(t, planpb.AggregateOp_min, aggNode.Aggregates[0].Op)
}

// TestBuildQueryPlan_AllAggOps tests all aggregate operations.
func TestBuildQueryPlan_AllAggOps(t *testing.T) {
	schema := buildTestSchema(t)

	comp := &SqlComponents{
		Collection: "github_events",
		SelectItems: []SelectItem{
			{Type: SelectAgg, AggFunc: &AggFuncCall{FuncName: "count"}},
			{Type: SelectAgg, AggFunc: &AggFuncCall{FuncName: "sum", Arg: &ColumnRef{FieldName: "data", NestedPath: []string{"v"}}}},
			{Type: SelectAgg, AggFunc: &AggFuncCall{FuncName: "avg", Arg: &ColumnRef{FieldName: "data", NestedPath: []string{"v"}}}},
			{Type: SelectAgg, AggFunc: &AggFuncCall{FuncName: "min", Arg: &ColumnRef{FieldName: "data", NestedPath: []string{"v"}}}},
			{Type: SelectAgg, AggFunc: &AggFuncCall{FuncName: "max", Arg: &ColumnRef{FieldName: "data", NestedPath: []string{"v"}}}},
			{Type: SelectAgg, AggFunc: &AggFuncCall{FuncName: "count", Arg: &ColumnRef{FieldName: "data", NestedPath: []string{"v"}}, Distinct: true}},
		},
		Limit: -1,
	}

	plan, err := BuildQueryPlan(comp, schema)
	require.NoError(t, err)

	aggs := plan.GetQuery().GetAggregateNode().GetAggregates()
	require.Len(t, aggs, 6)

	expected := []planpb.AggregateOp{
		planpb.AggregateOp_count,
		planpb.AggregateOp_sum,
		planpb.AggregateOp_avg,
		planpb.AggregateOp_min,
		planpb.AggregateOp_max,
		planpb.AggregateOp_count_distinct,
	}
	for i, agg := range aggs {
		assert.Equal(t, expected[i], agg.Op, "agg[%d] op mismatch", i)
	}
}

// TestBuildQueryPlan_OutputFieldIDs tests that output_field_ids are correctly resolved.
func TestBuildQueryPlan_OutputFieldIDs(t *testing.T) {
	schema := buildTestSchema(t)

	comp := &SqlComponents{
		Collection: "github_events",
		SelectItems: []SelectItem{
			{Type: SelectColumn, Column: &ColumnRef{FieldName: "id"}},
			{Type: SelectColumn, Column: &ColumnRef{FieldName: "data", NestedPath: []string{"name"}}},
		},
		Limit: -1,
	}

	plan, err := BuildQueryPlan(comp, schema)
	require.NoError(t, err)

	// Should contain both field IDs (1=id, 2=data), deduplicated
	assert.Contains(t, plan.OutputFieldIds, int64(1))
	assert.Contains(t, plan.OutputFieldIds, int64(2))
}

// TestBuildQueryPlan_InvalidField tests error on unknown field.
func TestBuildQueryPlan_InvalidField(t *testing.T) {
	schema := buildTestSchema(t)

	comp := &SqlComponents{
		Collection: "github_events",
		SelectItems: []SelectItem{
			{Type: SelectAgg, AggFunc: &AggFuncCall{FuncName: "sum", Arg: &ColumnRef{FieldName: "nonexistent"}}},
		},
		Limit: -1,
	}

	_, err := BuildQueryPlan(comp, schema)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "nonexistent")
}

// TestBuildQueryPlan_InvalidWhere tests error on invalid WHERE expression.
func TestBuildQueryPlan_InvalidWhere(t *testing.T) {
	schema := buildTestSchema(t)

	comp := &SqlComponents{
		Collection: "github_events",
		Where: &WhereClause{
			MilvusExpr: "this is not valid !!!",
		},
		Limit: -1,
	}

	_, err := BuildQueryPlan(comp, schema)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "WHERE")
}

// TestAggFuncNameToOp tests the aggregate function name to proto op mapping.
func TestAggFuncNameToOp(t *testing.T) {
	tests := []struct {
		name     string
		distinct bool
		want     planpb.AggregateOp
	}{
		{"count", false, planpb.AggregateOp_count},
		{"COUNT", false, planpb.AggregateOp_count},
		{"count", true, planpb.AggregateOp_count_distinct},
		{"sum", false, planpb.AggregateOp_sum},
		{"avg", false, planpb.AggregateOp_avg},
		{"min", false, planpb.AggregateOp_min},
		{"max", false, planpb.AggregateOp_max},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := aggFuncNameToOp(tt.name, tt.distinct)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}

	// Invalid function
	_, err := aggFuncNameToOp("median", false)
	assert.Error(t, err)
}

// buildBlueskySchema creates a schema matching the JSONBench bluesky data model.
func buildBlueskySchema(t *testing.T) *typeutil.SchemaHelper {
	fields := []*schemapb.FieldSchema{
		{FieldID: 1, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 2, Name: "data", DataType: schemapb.DataType_JSON},
	}
	schema := &schemapb.CollectionSchema{
		Name:   "bluesky",
		AutoID: false,
		Fields: fields,
	}
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	return helper
}

// TestSqlToQueryPlan_Convenience tests the one-step convenience function.
func TestSqlToQueryPlan_Convenience(t *testing.T) {
	schema := buildTestSchema(t)

	plan, err := SqlToQueryPlan(
		`SELECT count(*) FROM github_events WHERE data->>'kind' = 'PushEvent'`,
		schema,
	)
	require.NoError(t, err)
	require.NotNil(t, plan.GetQuery().GetAggregateNode())
	assert.Equal(t, planpb.AggregateOp_count, plan.GetQuery().GetAggregateNode().GetAggregates()[0].Op)
}

// ==================== End-to-End: JSONBench Q1-Q5 ====================

// TestE2E_Q1 tests the full pipeline for JSONBench Q1:
// SELECT data->'commit'->>'collection' AS event, count(*) AS count
// FROM bluesky GROUP BY event ORDER BY count DESC
func TestE2E_Q1(t *testing.T) {
	schema := buildBlueskySchema(t)
	plan, err := SqlToQueryPlan(jsonBenchQueries["Q1"], schema)
	require.NoError(t, err)

	query := plan.GetQuery()
	// Q1 has no WHERE
	assert.Nil(t, query.Predicates)

	// AggregateNode
	aggNode := query.AggregateNode
	require.NotNil(t, aggNode)

	// GROUP BY: data->'commit'->>'collection' (alias "event")
	require.Len(t, aggNode.GroupBy, 1)
	assert.Equal(t, int64(2), aggNode.GroupBy[0].FieldId)
	assert.Equal(t, []string{"commit", "collection"}, aggNode.GroupBy[0].NestedPath)

	// Aggregates: count(*)
	require.Len(t, aggNode.Aggregates, 1)
	assert.Equal(t, planpb.AggregateOp_count, aggNode.Aggregates[0].Op)

	// SortNode: count DESC
	require.NotNil(t, query.SortNode)
	require.Len(t, query.SortNode.Items, 1)
	assert.Equal(t, "count", query.SortNode.Items[0].Field)
	assert.True(t, query.SortNode.Items[0].Desc)

	// No LIMIT in Q1
	assert.Equal(t, int64(0), query.Limit)
}

// TestE2E_Q2 tests the full pipeline for JSONBench Q2:
// SELECT event, count(*), count(DISTINCT data->>'did') AS users
// FROM bluesky WHERE ... GROUP BY event ORDER BY count DESC
func TestE2E_Q2(t *testing.T) {
	schema := buildBlueskySchema(t)
	plan, err := SqlToQueryPlan(jsonBenchQueries["Q2"], schema)
	require.NoError(t, err)

	query := plan.GetQuery()
	// Q2 has WHERE
	assert.NotNil(t, query.Predicates)

	aggNode := query.AggregateNode
	require.NotNil(t, aggNode)

	// GROUP BY: event alias
	require.Len(t, aggNode.GroupBy, 1)
	assert.Equal(t, int64(2), aggNode.GroupBy[0].FieldId)

	// Aggregates: count(*) + count(DISTINCT data->>'did')
	require.Len(t, aggNode.Aggregates, 2)
	assert.Equal(t, planpb.AggregateOp_count, aggNode.Aggregates[0].Op)
	assert.Equal(t, planpb.AggregateOp_count_distinct, aggNode.Aggregates[1].Op)
	assert.Equal(t, []string{"did"}, aggNode.Aggregates[1].NestedPath)
}

// TestE2E_Q3 tests the full pipeline for JSONBench Q3:
// SELECT event, extract(hour FROM ...) AS hour_of_day, count(*)
// FROM bluesky WHERE ... GROUP BY event, hour_of_day ORDER BY hour_of_day, event
func TestE2E_Q3(t *testing.T) {
	schema := buildBlueskySchema(t)
	plan, err := SqlToQueryPlan(jsonBenchQueries["Q3"], schema)
	require.NoError(t, err)

	query := plan.GetQuery()
	assert.NotNil(t, query.Predicates)

	aggNode := query.AggregateNode
	require.NotNil(t, aggNode)

	// GROUP BY: event (column alias) + hour_of_day (computed alias → extract innermost column)
	// event resolves to data->'commit'->>'collection' → field 2
	assert.GreaterOrEqual(t, len(aggNode.GroupBy), 1)
	assert.Equal(t, int64(2), aggNode.GroupBy[0].FieldId)

	// Aggregates: count(*)
	require.Len(t, aggNode.Aggregates, 1)
	assert.Equal(t, planpb.AggregateOp_count, aggNode.Aggregates[0].Op)

	// SortNode: 2 items
	require.NotNil(t, query.SortNode)
	assert.Len(t, query.SortNode.Items, 2)

	// ProjectNode: hour_of_day is a computed expression (extract)
	require.NotNil(t, query.ProjectNode)
	assert.GreaterOrEqual(t, len(query.ProjectNode.Items), 1)
}

// TestE2E_Q4 tests the full pipeline for JSONBench Q4:
// SELECT (data->>'did')::text AS user_id, min(to_timestamp(...)) AS first_post_ts
// FROM bluesky WHERE ... GROUP BY user_id ORDER BY first_post_ts ASC LIMIT 3
func TestE2E_Q4(t *testing.T) {
	schema := buildBlueskySchema(t)
	plan, err := SqlToQueryPlan(jsonBenchQueries["Q4"], schema)
	require.NoError(t, err)

	query := plan.GetQuery()
	assert.NotNil(t, query.Predicates)

	aggNode := query.AggregateNode
	require.NotNil(t, aggNode)

	// GROUP BY: user_id alias → (data->>'did')::text → field 2, nested ["did"]
	require.Len(t, aggNode.GroupBy, 1)
	assert.Equal(t, int64(2), aggNode.GroupBy[0].FieldId)
	assert.Equal(t, []string{"did"}, aggNode.GroupBy[0].NestedPath)

	// Aggregates: min(to_timestamp(...)) → deep extract → field 2, nested ["time_us"]
	require.Len(t, aggNode.Aggregates, 1)
	assert.Equal(t, planpb.AggregateOp_min, aggNode.Aggregates[0].Op)
	assert.Equal(t, int64(2), aggNode.Aggregates[0].FieldId)
	assert.Equal(t, []string{"time_us"}, aggNode.Aggregates[0].NestedPath)

	// SortNode: first_post_ts ASC
	require.NotNil(t, query.SortNode)
	assert.Equal(t, "first_post_ts", query.SortNode.Items[0].Field)
	assert.False(t, query.SortNode.Items[0].Desc)

	// LIMIT 3
	assert.Equal(t, int64(3), query.Limit)
}

// TestE2E_Q5 tests the full pipeline for JSONBench Q5:
// SELECT user_id, extract(epoch FROM (max(...) - min(...))) * 1000 AS activity_span
// FROM bluesky WHERE ... GROUP BY user_id ORDER BY activity_span DESC LIMIT 3
func TestE2E_Q5(t *testing.T) {
	schema := buildBlueskySchema(t)
	plan, err := SqlToQueryPlan(jsonBenchQueries["Q5"], schema)
	require.NoError(t, err)

	query := plan.GetQuery()
	assert.NotNil(t, query.Predicates)

	aggNode := query.AggregateNode
	require.NotNil(t, aggNode)

	// GROUP BY: user_id → data->>'did'
	require.Len(t, aggNode.GroupBy, 1)
	assert.Equal(t, int64(2), aggNode.GroupBy[0].FieldId)

	// Nested aggregates: max(data["time_us"]) and min(data["time_us"])
	// extracted from the computed expression and registered in AggregateNode
	require.Len(t, aggNode.Aggregates, 2)
	aggOps := map[planpb.AggregateOp]bool{}
	for _, a := range aggNode.Aggregates {
		aggOps[a.Op] = true
		assert.Equal(t, int64(2), a.FieldId)
		assert.Equal(t, []string{"time_us"}, a.NestedPath)
	}
	assert.True(t, aggOps[planpb.AggregateOp_max], "should have max aggregate")
	assert.True(t, aggOps[planpb.AggregateOp_min], "should have min aggregate")

	// SortNode: activity_span DESC
	require.NotNil(t, query.SortNode)
	assert.Equal(t, "activity_span", query.SortNode.Items[0].Field)
	assert.True(t, query.SortNode.Items[0].Desc)

	// LIMIT 3
	assert.Equal(t, int64(3), query.Limit)

	// activity_span is a computed expression with nested aggregates (max/min).
	// The buildProjectNode intentionally skips computed items that contain nested
	// aggregates, because they are evaluated by ApplyProjection on the proxy side.
	// So ProjectNode is nil for Q5.
	assert.Nil(t, query.ProjectNode, "Q5 computed with nested aggs should NOT have ProjectNode")
}

// TestExtractDeepColumnRef tests the recursive column extraction from nested expressions.
func TestExtractDeepColumnRef(t *testing.T) {
	// Parse: min(to_timestamp((data->>'time_us')::bigint / 1000000.0))
	sql := `SELECT min(to_timestamp((data->>'time_us')::bigint / 1000000.0)) FROM bluesky`
	comp, err := ExtractSqlComponents(sql)
	require.NoError(t, err)

	si := comp.SelectItems[0]
	require.Equal(t, SelectAgg, si.Type)

	// Arg is nil because the arg is a FuncExpr (to_timestamp), not a column ref
	assert.Nil(t, si.AggFunc.Arg)

	// But extractDeepColumnRef on the raw expression should find data->>'time_us'
	col := extractDeepColumnRef(si.RawExpr)
	require.NotNil(t, col, "should extract deep column from nested functions")
	assert.Equal(t, "data", col.FieldName)
	assert.Equal(t, []string{"time_us"}, col.NestedPath)
}

// ==================== Search Plan Tests ====================

// buildVectorSchema creates a schema with a primary key, JSON field, and vector field.
func buildVectorSchema(t *testing.T) *typeutil.SchemaHelper {
	fields := []*schemapb.FieldSchema{
		{FieldID: 1, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 2, Name: "data", DataType: schemapb.DataType_JSON},
		{FieldID: 3, Name: "embedding", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "128"}}},
	}
	schema := &schemapb.CollectionSchema{
		Name:   "articles",
		AutoID: false,
		Fields: fields,
	}
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	return helper
}

func TestBuildSearchPlan_Basic(t *testing.T) {
	schema := buildVectorSchema(t)

	comp := &SqlComponents{
		Collection: "articles",
		SelectItems: []SelectItem{
			{Type: SelectColumn, Column: &ColumnRef{FieldName: "id"}},
			{Type: SelectVectorDist, Distance: &VectorDistExpr{
				Column:   &ColumnRef{FieldName: "embedding"},
				Operator: VectorDistL2,
				ParamRef: "$1",
			}, Alias: "distance"},
		},
		VectorSearch: &VectorSearchDef{
			FieldName:  "embedding",
			MetricType: "L2",
			ParamRef:   "$1",
			TopK:       10,
			DistAlias:  "distance",
		},
		Limit:     10,
		QueryType: QueryTypeSearch,
	}

	plan, err := BuildSearchPlan(comp, schema)
	require.NoError(t, err)
	require.NotNil(t, plan)

	va := plan.GetVectorAnns()
	require.NotNil(t, va, "should have VectorANNS node")
	assert.Equal(t, planpb.VectorType_FloatVector, va.VectorType)
	assert.Equal(t, int64(3), va.FieldId) // embedding field
	assert.Equal(t, "$0", va.PlaceholderTag)
	assert.Nil(t, va.Predicates, "no WHERE → no predicates")

	qi := va.QueryInfo
	require.NotNil(t, qi)
	assert.Equal(t, int64(10), qi.Topk)
	assert.Equal(t, "L2", qi.MetricType)

	// Output fields should include id (PK)
	assert.Contains(t, plan.OutputFieldIds, int64(1))
}

func TestBuildSearchPlan_WithFilter(t *testing.T) {
	schema := buildVectorSchema(t)

	comp := &SqlComponents{
		Collection: "articles",
		SelectItems: []SelectItem{
			{Type: SelectColumn, Column: &ColumnRef{FieldName: "id"}},
			{Type: SelectVectorDist, Distance: &VectorDistExpr{
				Column:   &ColumnRef{FieldName: "embedding"},
				Operator: VectorDistCosine,
				ParamRef: "$1",
			}, Alias: "dist"},
		},
		Where: &WhereClause{
			MilvusExpr: `data["category"] == "tech"`,
		},
		VectorSearch: &VectorSearchDef{
			FieldName:  "embedding",
			MetricType: "COSINE",
			ParamRef:   "$1",
			TopK:       5,
			DistAlias:  "dist",
		},
		Limit:     5,
		QueryType: QueryTypeHybrid,
	}

	plan, err := BuildSearchPlan(comp, schema)
	require.NoError(t, err)

	va := plan.GetVectorAnns()
	require.NotNil(t, va)
	assert.Equal(t, "COSINE", va.QueryInfo.MetricType)
	assert.Equal(t, int64(5), va.QueryInfo.Topk)
	assert.NotNil(t, va.Predicates, "should have predicates from WHERE")
}

func TestBuildSearchPlan_NoVectorSearch(t *testing.T) {
	schema := buildVectorSchema(t)

	comp := &SqlComponents{
		Collection:   "articles",
		VectorSearch: nil,
	}

	_, err := BuildSearchPlan(comp, schema)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no vector search definition")
}

func TestBuildSearchPlan_FieldIDResolved(t *testing.T) {
	schema := buildVectorSchema(t)

	comp := &SqlComponents{
		Collection: "articles",
		SelectItems: []SelectItem{
			{Type: SelectVectorDist, Distance: &VectorDistExpr{
				Column:   &ColumnRef{FieldName: "embedding"},
				Operator: VectorDistIP,
				ParamRef: "$1",
			}, Alias: "score"},
		},
		VectorSearch: &VectorSearchDef{
			FieldName:  "embedding",
			MetricType: "IP",
			ParamRef:   "$1",
			TopK:       20,
			DistAlias:  "score",
		},
		Limit:     20,
		QueryType: QueryTypeSearch,
	}

	_, err := BuildSearchPlan(comp, schema)
	require.NoError(t, err)

	// FieldID should be resolved back into VectorSearchDef
	assert.Equal(t, int64(3), comp.VectorSearch.FieldID)
}

func TestBuildPlaceholderGroup(t *testing.T) {
	vectors := [][]float32{{1.0, 2.0, 3.0}}
	data, err := BuildPlaceholderGroup(vectors)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	// Should be valid protobuf
	assert.True(t, len(data) > 0)
}

func TestBuildPlaceholderGroup_Empty(t *testing.T) {
	_, err := BuildPlaceholderGroup(nil)
	assert.Error(t, err)
}

func TestDataTypeToVectorType(t *testing.T) {
	tests := []struct {
		dt   schemapb.DataType
		want planpb.VectorType
	}{
		{schemapb.DataType_FloatVector, planpb.VectorType_FloatVector},
		{schemapb.DataType_BinaryVector, planpb.VectorType_BinaryVector},
		{schemapb.DataType_Float16Vector, planpb.VectorType_Float16Vector},
		{schemapb.DataType_SparseFloatVector, planpb.VectorType_SparseFloatVector},
	}

	for _, tt := range tests {
		got, err := dataTypeToVectorType(tt.dt)
		require.NoError(t, err)
		assert.Equal(t, tt.want, got)
	}

	// Non-vector type should error
	_, err := dataTypeToVectorType(schemapb.DataType_Int64)
	assert.Error(t, err)
}
