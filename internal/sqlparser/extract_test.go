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
)

// ==================== F1.1: SELECT/FROM ====================

func TestExtractSimpleSelect(t *testing.T) {
	comp, err := ExtractSqlComponents("SELECT id FROM articles")
	require.NoError(t, err)

	assert.Equal(t, "articles", comp.Collection)
	assert.Len(t, comp.SelectItems, 1)
	assert.Equal(t, SelectColumn, comp.SelectItems[0].Type)
	assert.Equal(t, "id", comp.SelectItems[0].Column.FieldName)
}

func TestExtractMultiColumnWithAlias(t *testing.T) {
	comp, err := ExtractSqlComponents("SELECT id, title AS t, category FROM articles")
	require.NoError(t, err)

	assert.Equal(t, "articles", comp.Collection)
	assert.Len(t, comp.SelectItems, 3)

	assert.Equal(t, "id", comp.SelectItems[0].Column.FieldName)
	assert.Equal(t, "", comp.SelectItems[0].Alias)

	assert.Equal(t, "title", comp.SelectItems[1].Column.FieldName)
	assert.Equal(t, "t", comp.SelectItems[1].Alias)

	assert.Equal(t, "category", comp.SelectItems[2].Column.FieldName)
}

func TestExtractFromRequired(t *testing.T) {
	_, err := ExtractSqlComponents("SELECT 1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "FROM clause is required")
}

func TestExtractJoinNotSupported(t *testing.T) {
	_, err := ExtractSqlComponents("SELECT a.id FROM articles a JOIN users u ON a.uid = u.id")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not supported")
}

func TestExtractNonSelect(t *testing.T) {
	_, err := ExtractSqlComponents("INSERT INTO foo VALUES (1)")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "expected SELECT")
}

// ==================== F1.2: JSON Operators ====================

func TestExtractJSONSingleLevel(t *testing.T) {
	comp, err := ExtractSqlComponents("SELECT data->>'kind' FROM bluesky")
	require.NoError(t, err)

	require.Len(t, comp.SelectItems, 1)
	col := comp.SelectItems[0].Column
	require.NotNil(t, col)
	assert.Equal(t, "data", col.FieldName)
	assert.Equal(t, []string{"kind"}, col.NestedPath)
	assert.True(t, col.IsText)
}

func TestExtractJSONTwoLevels(t *testing.T) {
	comp, err := ExtractSqlComponents("SELECT data->'commit'->>'collection' FROM bluesky")
	require.NoError(t, err)

	col := comp.SelectItems[0].Column
	assert.Equal(t, "data", col.FieldName)
	assert.Equal(t, []string{"commit", "collection"}, col.NestedPath)
	assert.True(t, col.IsText)
}

func TestExtractJSONThreeLevels(t *testing.T) {
	comp, err := ExtractSqlComponents("SELECT data->'commit'->'record'->>'text' FROM bluesky")
	require.NoError(t, err)

	col := comp.SelectItems[0].Column
	assert.Equal(t, "data", col.FieldName)
	assert.Equal(t, []string{"commit", "record", "text"}, col.NestedPath)
	assert.True(t, col.IsText)
}

func TestExtractMultipleJSONColumns(t *testing.T) {
	comp, err := ExtractSqlComponents("SELECT data->>'kind', data->'commit'->>'operation' FROM bluesky")
	require.NoError(t, err)

	assert.Len(t, comp.SelectItems, 2)

	col1 := comp.SelectItems[0].Column
	assert.Equal(t, "data", col1.FieldName)
	assert.Equal(t, []string{"kind"}, col1.NestedPath)

	col2 := comp.SelectItems[1].Column
	assert.Equal(t, "data", col2.FieldName)
	assert.Equal(t, []string{"commit", "operation"}, col2.NestedPath)
}

func TestExtractJSONFetchVal(t *testing.T) {
	// -> (not ->>) should set IsText=false
	comp, err := ExtractSqlComponents("SELECT data->'commit' FROM bluesky")
	require.NoError(t, err)

	col := comp.SelectItems[0].Column
	assert.Equal(t, []string{"commit"}, col.NestedPath)
	assert.False(t, col.IsText, "-> should set IsText=false")
}

// ==================== F1.3: WHERE ====================

func TestExtractWhereSimpleEqual(t *testing.T) {
	comp, err := ExtractSqlComponents("SELECT id FROM tbl WHERE data->>'kind' = 'commit'")
	require.NoError(t, err)
	require.NotNil(t, comp.Where)
	assert.Equal(t, `data["kind"] == "commit"`, comp.Where.MilvusExpr)
}

func TestExtractWhereAND(t *testing.T) {
	comp, err := ExtractSqlComponents(`SELECT id FROM tbl WHERE data->>'kind' = 'commit' AND data->'commit'->>'operation' = 'create'`)
	require.NoError(t, err)
	require.NotNil(t, comp.Where)
	assert.Equal(t, `(data["kind"] == "commit") and (data["commit"]["operation"] == "create")`, comp.Where.MilvusExpr)
}

func TestExtractWhereIN(t *testing.T) {
	comp, err := ExtractSqlComponents(`SELECT id FROM tbl WHERE data->'commit'->>'collection' IN ('app.bsky.feed.post', 'app.bsky.feed.repost')`)
	require.NoError(t, err)
	require.NotNil(t, comp.Where)
	assert.Equal(t, `data["commit"]["collection"] in ["app.bsky.feed.post", "app.bsky.feed.repost"]`, comp.Where.MilvusExpr)
}

func TestExtractWhereNumericComparison(t *testing.T) {
	comp, err := ExtractSqlComponents("SELECT id FROM tbl WHERE price > 100 AND stock >= 0")
	require.NoError(t, err)
	require.NotNil(t, comp.Where)
	assert.Equal(t, "(price > 100) and (stock >= 0)", comp.Where.MilvusExpr)
}

func TestExtractWhereNOT(t *testing.T) {
	comp, err := ExtractSqlComponents("SELECT id FROM tbl WHERE NOT (status = 'deleted')")
	require.NoError(t, err)
	require.NotNil(t, comp.Where)
	assert.Equal(t, `not (status == "deleted")`, comp.Where.MilvusExpr)
}

func TestExtractWhereOR(t *testing.T) {
	comp, err := ExtractSqlComponents("SELECT id FROM tbl WHERE status = 'active' OR priority > 5")
	require.NoError(t, err)
	require.NotNil(t, comp.Where)
	assert.Equal(t, `(status == "active") or (priority > 5)`, comp.Where.MilvusExpr)
}

func TestExtractWhereBETWEEN(t *testing.T) {
	comp, err := ExtractSqlComponents("SELECT id FROM tbl WHERE price BETWEEN 10 AND 100")
	require.NoError(t, err)
	require.NotNil(t, comp.Where)
	assert.Equal(t, "(price >= 10 and price <= 100)", comp.Where.MilvusExpr)
}

func TestExtractWhereLIKE(t *testing.T) {
	comp, err := ExtractSqlComponents("SELECT id FROM tbl WHERE name LIKE 'alice%'")
	require.NoError(t, err)
	require.NotNil(t, comp.Where)
	assert.Equal(t, `name like "alice%"`, comp.Where.MilvusExpr)
}

func TestExtractNoWhere(t *testing.T) {
	comp, err := ExtractSqlComponents("SELECT id FROM tbl")
	require.NoError(t, err)
	assert.Nil(t, comp.Where)
}

// ==================== F1.4: Aggregation Functions ====================

func TestExtractCountStar(t *testing.T) {
	comp, err := ExtractSqlComponents("SELECT count(*) AS cnt FROM tbl")
	require.NoError(t, err)

	require.Len(t, comp.SelectItems, 1)
	item := comp.SelectItems[0]
	assert.Equal(t, SelectAgg, item.Type)
	assert.Equal(t, "count", item.AggFunc.FuncName)
	assert.Nil(t, item.AggFunc.Arg)
	assert.False(t, item.AggFunc.Distinct)
	assert.Equal(t, "cnt", item.Alias)
}

func TestExtractCountField(t *testing.T) {
	comp, err := ExtractSqlComponents("SELECT count(id) FROM tbl")
	require.NoError(t, err)

	agg := comp.SelectItems[0].AggFunc
	assert.Equal(t, "count", agg.FuncName)
	assert.NotNil(t, agg.Arg)
	assert.Equal(t, "id", agg.Arg.FieldName)
	assert.False(t, agg.Distinct)
}

func TestExtractCountDistinct(t *testing.T) {
	comp, err := ExtractSqlComponents("SELECT count(DISTINCT data->>'did') AS users FROM tbl")
	require.NoError(t, err)

	agg := comp.SelectItems[0].AggFunc
	assert.Equal(t, "count", agg.FuncName)
	assert.True(t, agg.Distinct)
	assert.Equal(t, "data", agg.Arg.FieldName)
	assert.Equal(t, []string{"did"}, agg.Arg.NestedPath)
	assert.Equal(t, "users", comp.SelectItems[0].Alias)
}

func TestExtractMinMax(t *testing.T) {
	comp, err := ExtractSqlComponents("SELECT min(data->>'time_us'), max(data->>'time_us') FROM tbl")
	require.NoError(t, err)

	assert.Len(t, comp.SelectItems, 2)
	assert.Equal(t, "min", comp.SelectItems[0].AggFunc.FuncName)
	assert.Equal(t, "max", comp.SelectItems[1].AggFunc.FuncName)
}

func TestExtractMixedAggAndColumn(t *testing.T) {
	comp, err := ExtractSqlComponents("SELECT data->>'did', count(*), sum(data->>'score') FROM tbl")
	require.NoError(t, err)

	assert.Len(t, comp.SelectItems, 3)
	assert.Equal(t, SelectColumn, comp.SelectItems[0].Type)
	assert.Equal(t, SelectAgg, comp.SelectItems[1].Type)
	assert.Equal(t, SelectAgg, comp.SelectItems[2].Type)
	assert.Equal(t, "sum", comp.SelectItems[2].AggFunc.FuncName)
}

func TestExtractNestedAggError(t *testing.T) {
	_, err := ExtractSqlComponents("SELECT count(sum(x)) FROM tbl")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "nested aggregation")
}

// ==================== F1.5: GROUP BY ====================

func TestExtractGroupBySingleAlias(t *testing.T) {
	comp, err := ExtractSqlComponents("SELECT data->>'kind' AS k, count(*) FROM tbl GROUP BY k")
	require.NoError(t, err)

	require.Len(t, comp.GroupBy, 1)
	assert.Equal(t, "k", comp.GroupBy[0].Alias)
}

func TestExtractGroupByMultiple(t *testing.T) {
	comp, err := ExtractSqlComponents(`
		SELECT data->'commit'->>'collection' AS event,
		       data->>'kind' AS kind,
		       count(*) AS count
		FROM bluesky
		GROUP BY event, kind`)
	require.NoError(t, err)

	assert.Len(t, comp.GroupBy, 2)
}

func TestExtractGroupByJSONPath(t *testing.T) {
	comp, err := ExtractSqlComponents("SELECT data->'commit'->>'collection', count(*) FROM bluesky GROUP BY data->'commit'->>'collection'")
	require.NoError(t, err)

	require.Len(t, comp.GroupBy, 1)
	require.NotNil(t, comp.GroupBy[0].Column)
	assert.Equal(t, "data", comp.GroupBy[0].Column.FieldName)
	assert.Equal(t, []string{"commit", "collection"}, comp.GroupBy[0].Column.NestedPath)
}

func TestExtractNoGroupBy(t *testing.T) {
	comp, err := ExtractSqlComponents("SELECT id FROM tbl")
	require.NoError(t, err)
	assert.Nil(t, comp.GroupBy)
}

// ==================== F1.6: ORDER BY ====================

func TestExtractOrderByDesc(t *testing.T) {
	comp, err := ExtractSqlComponents("SELECT id FROM tbl ORDER BY id DESC")
	require.NoError(t, err)

	require.Len(t, comp.OrderBy, 1)
	assert.Equal(t, "id", comp.OrderBy[0].Ref)
	assert.True(t, comp.OrderBy[0].Desc)
}

func TestExtractOrderByAsc(t *testing.T) {
	comp, err := ExtractSqlComponents("SELECT id FROM tbl ORDER BY name ASC")
	require.NoError(t, err)

	require.Len(t, comp.OrderBy, 1)
	assert.Equal(t, "name", comp.OrderBy[0].Ref)
	assert.False(t, comp.OrderBy[0].Desc)
}

func TestExtractOrderByDefault(t *testing.T) {
	// Default direction is ASC
	comp, err := ExtractSqlComponents("SELECT id FROM tbl ORDER BY name")
	require.NoError(t, err)

	require.Len(t, comp.OrderBy, 1)
	assert.False(t, comp.OrderBy[0].Desc)
}

func TestExtractOrderByMultiple(t *testing.T) {
	comp, err := ExtractSqlComponents("SELECT id FROM tbl ORDER BY hour_of_day, event")
	require.NoError(t, err)

	assert.Len(t, comp.OrderBy, 2)
	assert.Equal(t, "hour_of_day", comp.OrderBy[0].Ref)
	assert.Equal(t, "event", comp.OrderBy[1].Ref)
}

func TestExtractNoOrderBy(t *testing.T) {
	comp, err := ExtractSqlComponents("SELECT id FROM tbl")
	require.NoError(t, err)
	assert.Nil(t, comp.OrderBy)
}

// ==================== F1.7: LIMIT ====================

func TestExtractLimit(t *testing.T) {
	comp, err := ExtractSqlComponents("SELECT id FROM tbl LIMIT 10")
	require.NoError(t, err)
	assert.Equal(t, int64(10), comp.Limit)
}

func TestExtractLimit3(t *testing.T) {
	comp, err := ExtractSqlComponents("SELECT id FROM tbl ORDER BY name LIMIT 3")
	require.NoError(t, err)
	assert.Equal(t, int64(3), comp.Limit)
}

func TestExtractNoLimit(t *testing.T) {
	comp, err := ExtractSqlComponents("SELECT id FROM tbl")
	require.NoError(t, err)
	assert.Equal(t, int64(-1), comp.Limit)
}

// ==================== F1.8: Aliases ====================

func TestExtractAliasesPreserved(t *testing.T) {
	comp, err := ExtractSqlComponents(`
		SELECT data->'commit'->>'collection' AS event,
		       count(*) AS count
		FROM bluesky
		GROUP BY event
		ORDER BY count DESC`)
	require.NoError(t, err)

	// SELECT aliases
	assert.Equal(t, "event", comp.SelectItems[0].Alias)
	assert.Equal(t, "count", comp.SelectItems[1].Alias)

	// GROUP BY should reference alias
	assert.Equal(t, "event", comp.GroupBy[0].Alias)

	// ORDER BY should reference alias
	assert.Equal(t, "count", comp.OrderBy[0].Ref)
	assert.True(t, comp.OrderBy[0].Desc)
}

// ==================== F1.9: Type Cast ====================

func TestExtractTypeCast(t *testing.T) {
	comp, err := ExtractSqlComponents("SELECT (data->>'did')::text AS user_id FROM tbl")
	require.NoError(t, err)

	require.Len(t, comp.SelectItems, 1)
	col := comp.SelectItems[0].Column
	require.NotNil(t, col)
	assert.Equal(t, "data", col.FieldName)
	assert.Equal(t, []string{"did"}, col.NestedPath)
	assert.Equal(t, "text", col.CastType)
	assert.Equal(t, "user_id", comp.SelectItems[0].Alias)
}

// ==================== End-to-end: JSONBench Q1-Q5 ====================

func TestExtractJSONBenchQ1(t *testing.T) {
	comp, err := ExtractSqlComponents(jsonBenchQueries["Q1"])
	require.NoError(t, err)

	assert.Equal(t, "bluesky", comp.Collection)
	assert.Len(t, comp.SelectItems, 2)

	// event = data->'commit'->>'collection'
	assert.Equal(t, SelectColumn, comp.SelectItems[0].Type)
	assert.Equal(t, "event", comp.SelectItems[0].Alias)
	assert.Equal(t, "data", comp.SelectItems[0].Column.FieldName)
	assert.Equal(t, []string{"commit", "collection"}, comp.SelectItems[0].Column.NestedPath)

	// count(*)
	assert.Equal(t, SelectAgg, comp.SelectItems[1].Type)
	assert.Equal(t, "count", comp.SelectItems[1].AggFunc.FuncName)
	assert.Nil(t, comp.SelectItems[1].AggFunc.Arg)
	assert.Equal(t, "count", comp.SelectItems[1].Alias)

	// GROUP BY event
	assert.Len(t, comp.GroupBy, 1)
	assert.Equal(t, "event", comp.GroupBy[0].Alias)

	// ORDER BY count DESC
	assert.Len(t, comp.OrderBy, 1)
	assert.Equal(t, "count", comp.OrderBy[0].Ref)
	assert.True(t, comp.OrderBy[0].Desc)

	// No LIMIT
	assert.Equal(t, int64(-1), comp.Limit)

	// No WHERE
	assert.Nil(t, comp.Where)

	// QueryType = Query
	assert.Equal(t, QueryTypeQuery, comp.QueryType)
}

func TestExtractJSONBenchQ2(t *testing.T) {
	comp, err := ExtractSqlComponents(jsonBenchQueries["Q2"])
	require.NoError(t, err)

	assert.Equal(t, "bluesky", comp.Collection)
	assert.Len(t, comp.SelectItems, 3)

	// count(DISTINCT data->>'did') AS users
	usersItem := comp.SelectItems[2]
	assert.Equal(t, SelectAgg, usersItem.Type)
	assert.True(t, usersItem.AggFunc.Distinct)
	assert.Equal(t, "count", usersItem.AggFunc.FuncName)
	assert.Equal(t, "data", usersItem.AggFunc.Arg.FieldName)
	assert.Equal(t, []string{"did"}, usersItem.AggFunc.Arg.NestedPath)

	// WHERE clause present
	require.NotNil(t, comp.Where)
	assert.Contains(t, comp.Where.MilvusExpr, `data["kind"] == "commit"`)
	assert.Contains(t, comp.Where.MilvusExpr, `data["commit"]["operation"] == "create"`)
}

func TestExtractJSONBenchQ3(t *testing.T) {
	comp, err := ExtractSqlComponents(jsonBenchQueries["Q3"])
	require.NoError(t, err)

	assert.Equal(t, "bluesky", comp.Collection)
	assert.Len(t, comp.SelectItems, 3)

	// Second item should be a computed expression (extract(...))
	hourItem := comp.SelectItems[1]
	assert.Equal(t, "hour_of_day", hourItem.Alias)
	// extract() is a FuncExpr → either SelectComputed or SelectAgg
	// It's not an aggregate, so it should be SelectComputed
	assert.Equal(t, SelectComputed, hourItem.Type)

	// WHERE includes IN clause
	require.NotNil(t, comp.Where)
	assert.Contains(t, comp.Where.MilvusExpr, "in [")

	// GROUP BY has 2 items
	assert.Len(t, comp.GroupBy, 2)

	// ORDER BY has 2 items
	assert.Len(t, comp.OrderBy, 2)
}

func TestExtractJSONBenchQ4(t *testing.T) {
	comp, err := ExtractSqlComponents(jsonBenchQueries["Q4"])
	require.NoError(t, err)

	assert.Equal(t, "bluesky", comp.Collection)
	assert.Len(t, comp.SelectItems, 2)

	// user_id with type cast
	userItem := comp.SelectItems[0]
	assert.Equal(t, "user_id", userItem.Alias)
	assert.Equal(t, SelectColumn, userItem.Type)
	assert.Equal(t, "text", userItem.Column.CastType)

	// min(to_timestamp(...)) as first_post_ts
	// Note: min has a FuncExpr arg, so Arg is nil but AggFunc is still extracted
	minItem := comp.SelectItems[1]
	assert.Equal(t, SelectAgg, minItem.Type)
	assert.Equal(t, "min", minItem.AggFunc.FuncName)
	assert.Equal(t, "first_post_ts", minItem.Alias)

	// LIMIT 3
	assert.Equal(t, int64(3), comp.Limit)

	// ORDER BY first_post_ts ASC
	require.Len(t, comp.OrderBy, 1)
	assert.Equal(t, "first_post_ts", comp.OrderBy[0].Ref)
	assert.False(t, comp.OrderBy[0].Desc)
}

func TestExtractJSONBenchQ5(t *testing.T) {
	comp, err := ExtractSqlComponents(jsonBenchQueries["Q5"])
	require.NoError(t, err)

	assert.Equal(t, "bluesky", comp.Collection)
	assert.Len(t, comp.SelectItems, 2)

	// activity_span is a computed expression (extract * 1000)
	spanItem := comp.SelectItems[1]
	assert.Equal(t, "activity_span", spanItem.Alias)

	// LIMIT 3
	assert.Equal(t, int64(3), comp.Limit)

	// ORDER BY activity_span DESC
	require.Len(t, comp.OrderBy, 1)
	assert.True(t, comp.OrderBy[0].Desc)
}

// ==================== Vector Distance Functions ====================

func TestExtractVectorDistL2(t *testing.T) {
	sql := `SELECT id, l2_distance(embedding, $1) AS distance FROM articles ORDER BY distance LIMIT 10`
	comp, err := ExtractSqlComponents(sql)
	require.NoError(t, err)

	assert.Equal(t, "articles", comp.Collection)
	assert.Equal(t, QueryTypeSearch, comp.QueryType)
	require.NotNil(t, comp.VectorSearch)
	assert.Equal(t, "embedding", comp.VectorSearch.FieldName)
	assert.Equal(t, "L2", comp.VectorSearch.MetricType)
	assert.Equal(t, "$1", comp.VectorSearch.ParamRef)
	assert.Equal(t, int64(10), comp.VectorSearch.TopK)
	assert.Equal(t, "distance", comp.VectorSearch.DistAlias)

	// SELECT list should have column + vector dist
	require.Len(t, comp.SelectItems, 2)
	assert.Equal(t, SelectColumn, comp.SelectItems[0].Type)
	assert.Equal(t, SelectVectorDist, comp.SelectItems[1].Type)
	assert.Equal(t, "distance", comp.SelectItems[1].Alias)
}

func TestExtractVectorDistCosine(t *testing.T) {
	sql := `SELECT id, cosine_distance(vec, $1) AS dist FROM items LIMIT 5`
	comp, err := ExtractSqlComponents(sql)
	require.NoError(t, err)

	assert.Equal(t, QueryTypeSearch, comp.QueryType)
	require.NotNil(t, comp.VectorSearch)
	assert.Equal(t, "vec", comp.VectorSearch.FieldName)
	assert.Equal(t, "COSINE", comp.VectorSearch.MetricType)
	assert.Equal(t, int64(5), comp.VectorSearch.TopK)
}

func TestExtractVectorDistIP(t *testing.T) {
	sql := `SELECT ip_distance(embedding, $1) AS score FROM docs LIMIT 20`
	comp, err := ExtractSqlComponents(sql)
	require.NoError(t, err)

	assert.Equal(t, QueryTypeSearch, comp.QueryType)
	require.NotNil(t, comp.VectorSearch)
	assert.Equal(t, "embedding", comp.VectorSearch.FieldName)
	assert.Equal(t, "IP", comp.VectorSearch.MetricType)
	assert.Equal(t, int64(20), comp.VectorSearch.TopK)
}

func TestExtractVectorDistOperatorLiteral(t *testing.T) {
	sql := `SELECT id, embedding <-> '[0.1, -2, 3.5e-1]'::vector AS distance FROM articles ORDER BY distance LIMIT 10`
	comp, err := ExtractSqlComponents(sql)
	require.NoError(t, err)

	assert.Equal(t, QueryTypeSearch, comp.QueryType)
	require.NotNil(t, comp.VectorSearch)
	assert.Equal(t, "embedding", comp.VectorSearch.FieldName)
	assert.Equal(t, "L2", comp.VectorSearch.MetricType)
	assert.Empty(t, comp.VectorSearch.ParamRef)
	assert.InDeltaSlice(t, []float32{0.1, -2, 0.35}, comp.VectorSearch.Vector, 1e-6)

	require.Len(t, comp.SelectItems, 2)
	require.NotNil(t, comp.SelectItems[1].Distance)
	assert.InDeltaSlice(t, []float32{0.1, -2, 0.35}, comp.SelectItems[1].Distance.Vector, 1e-6)
}

func TestExtractVectorDistFunctionLiteral(t *testing.T) {
	sql := `SELECT l2_distance(embedding, '[1, 2, 3]') AS distance FROM articles LIMIT 5`
	comp, err := ExtractSqlComponents(sql)
	require.NoError(t, err)

	require.NotNil(t, comp.VectorSearch)
	assert.Empty(t, comp.VectorSearch.ParamRef)
	assert.Equal(t, []float32{1, 2, 3}, comp.VectorSearch.Vector)
	assert.Equal(t, int64(5), comp.VectorSearch.TopK)
}

func TestExtractVectorDistInvalidLiteral(t *testing.T) {
	sql := `SELECT embedding <-> '[1, bad, 3]'::vector AS distance FROM articles`
	_, err := ExtractSqlComponents(sql)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid vector literal")
}

func TestExtractVectorDistWithFilter(t *testing.T) {
	sql := `SELECT id, l2_distance(embedding, $1) AS distance FROM articles WHERE category = 'tech' ORDER BY distance LIMIT 10`
	comp, err := ExtractSqlComponents(sql)
	require.NoError(t, err)

	assert.Equal(t, QueryTypeHybrid, comp.QueryType)
	require.NotNil(t, comp.VectorSearch)
	assert.Equal(t, "L2", comp.VectorSearch.MetricType)
	require.NotNil(t, comp.Where)
	assert.Contains(t, comp.Where.MilvusExpr, "category")
}

func TestExtractVectorDistDefaultTopK(t *testing.T) {
	// No LIMIT → default topK of 10
	sql := `SELECT l2_distance(embedding, $1) AS distance FROM articles`
	comp, err := ExtractSqlComponents(sql)
	require.NoError(t, err)

	require.NotNil(t, comp.VectorSearch)
	assert.Equal(t, int64(10), comp.VectorSearch.TopK)
}

func TestExtractVectorDistWrongArgCount(t *testing.T) {
	sql := `SELECT l2_distance(embedding) AS distance FROM articles`
	_, err := ExtractSqlComponents(sql)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expects 2 arguments")
}
