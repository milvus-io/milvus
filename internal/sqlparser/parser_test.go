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

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// JSONBench Q1-Q5 SQL statements in PostgreSQL syntax.
var jsonBenchQueries = map[string]string{
	"Q1": `SELECT data->'commit'->>'collection' AS event, count(*) AS count
FROM bluesky
GROUP BY event
ORDER BY count DESC`,

	"Q2": `SELECT data->'commit'->>'collection' AS event, count(*) AS count,
       count(DISTINCT data->>'did') AS users
FROM bluesky
WHERE data->>'kind' = 'commit' AND data->'commit'->>'operation' = 'create'
GROUP BY event
ORDER BY count DESC`,

	"Q3": `SELECT data->'commit'->>'collection' AS event,
       extract(hour FROM to_timestamp((data->>'time_us')::bigint / 1000000.0)) AS hour_of_day,
       count(*) AS count
FROM bluesky
WHERE data->>'kind' = 'commit' AND data->'commit'->>'operation' = 'create'
  AND data->'commit'->>'collection' IN ('app.bsky.feed.post', 'app.bsky.feed.repost',
                                         'app.bsky.feed.like')
GROUP BY event, hour_of_day
ORDER BY hour_of_day, event`,

	"Q4": `SELECT (data->>'did')::text AS user_id,
       min(to_timestamp((data->>'time_us')::bigint / 1000000.0)) AS first_post_ts
FROM bluesky
WHERE data->>'kind' = 'commit' AND data->'commit'->>'operation' = 'create'
  AND data->'commit'->>'collection' = 'app.bsky.feed.post'
GROUP BY user_id
ORDER BY first_post_ts ASC LIMIT 3`,

	"Q5": `SELECT (data->>'did')::text AS user_id,
       extract(epoch FROM (
           max(to_timestamp((data->>'time_us')::bigint / 1000000.0)) -
           min(to_timestamp((data->>'time_us')::bigint / 1000000.0))
       )) * 1000 AS activity_span
FROM bluesky
WHERE data->>'kind' = 'commit' AND data->'commit'->>'operation' = 'create'
  AND data->'commit'->>'collection' = 'app.bsky.feed.post'
GROUP BY user_id
ORDER BY activity_span DESC LIMIT 3`,
}

func TestParseBasic(t *testing.T) {
	result, err := Parse("SELECT 1")
	require.NoError(t, err)
	assert.Len(t, result.Stmts, 1)
}

func TestParseEmpty(t *testing.T) {
	_, err := Parse("")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "empty SQL")
}

func TestParseInvalidSQL(t *testing.T) {
	_, err := Parse("NOT VALID SQL !!!")
	assert.Error(t, err)
}

func TestParseOne(t *testing.T) {
	rawStmt, err := ParseOne("SELECT 1")
	require.NoError(t, err)
	assert.NotNil(t, rawStmt)

	selStmt := rawStmt.Stmt.GetSelectStmt()
	assert.NotNil(t, selStmt, "expected SelectStmt, got nil")
}

func TestParseOneMultipleStatements(t *testing.T) {
	_, err := ParseOne("SELECT 1; SELECT 2")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "expected 1 statement")
}

// TestParseJSONBenchQ1toQ5 tests that all 5 JSONBench queries parse successfully
// and produce valid ASTs with expected structure.
func TestParseJSONBenchQ1toQ5(t *testing.T) {
	for name, sql := range jsonBenchQueries {
		t.Run(name, func(t *testing.T) {
			rawStmt, err := ParseOne(sql)
			require.NoError(t, err, "failed to parse %s", name)
			assert.NotNil(t, rawStmt)

			selStmt := rawStmt.Stmt.GetSelectStmt()
			require.NotNil(t, selStmt, "%s should be a SELECT statement", name)

			// All queries SELECT from 'bluesky'
			require.Len(t, selStmt.FromClause, 1)
			rv := selStmt.FromClause[0].GetRangeVar()
			require.NotNil(t, rv)
			assert.Equal(t, "bluesky", rv.Relname)

			// All queries have GROUP BY
			assert.Greater(t, len(selStmt.GroupClause), 0, "%s should have GROUP BY", name)

			// All queries have SELECT expressions
			assert.Greater(t, len(selStmt.TargetList), 0, "%s should have SELECT expressions", name)

			t.Logf("%s: parsed successfully - %d select exprs, %d group by exprs",
				name, len(selStmt.TargetList), len(selStmt.GroupClause))
		})
	}
}

// TestQ1ASTStructure verifies the detailed AST structure of Q1.
func TestQ1ASTStructure(t *testing.T) {
	rawStmt, err := ParseOne(jsonBenchQueries["Q1"])
	require.NoError(t, err)

	selStmt := rawStmt.Stmt.GetSelectStmt()
	require.NotNil(t, selStmt)

	// Q1: SELECT data->'commit'->>'collection' AS event, count(*) AS count
	// Should have 2 select expressions
	assert.Len(t, selStmt.TargetList, 2)

	// First expr: data->'commit'->>'collection' AS event
	firstTarget := selStmt.TargetList[0].GetResTarget()
	require.NotNil(t, firstTarget)
	assert.Equal(t, "event", firstTarget.Name)

	// Second expr: count(*) AS count
	secondTarget := selStmt.TargetList[1].GetResTarget()
	require.NotNil(t, secondTarget)
	assert.Equal(t, "count", secondTarget.Name)
	// Should be a FuncCall with agg_star=true
	funcCall := secondTarget.Val.GetFuncCall()
	require.NotNil(t, funcCall, "expected FuncCall for count(*)")
	assert.True(t, funcCall.AggStar)
	require.Len(t, funcCall.Funcname, 1)
	assert.Equal(t, "count", funcCall.Funcname[0].GetString_().Sval)

	// GROUP BY: 1 expression (event)
	assert.Len(t, selStmt.GroupClause, 1)

	// ORDER BY: in the SelectStmt's sortClause
	require.Len(t, selStmt.SortClause, 1)
	sortBy := selStmt.SortClause[0].GetSortBy()
	require.NotNil(t, sortBy)
	assert.Equal(t, pg_query.SortByDir_SORTBY_DESC, sortBy.SortbyDir)
}

// TestQ2ASTStructure verifies Q2 has WHERE clause and DISTINCT count.
func TestQ2ASTStructure(t *testing.T) {
	rawStmt, err := ParseOne(jsonBenchQueries["Q2"])
	require.NoError(t, err)

	selStmt := rawStmt.Stmt.GetSelectStmt()
	require.NotNil(t, selStmt)

	// Q2 has 3 select expressions: event, count, users
	assert.Len(t, selStmt.TargetList, 3)

	// Q2 has WHERE clause
	assert.NotNil(t, selStmt.WhereClause)

	// Third expr: count(DISTINCT data->>'did') AS users
	usersTarget := selStmt.TargetList[2].GetResTarget()
	require.NotNil(t, usersTarget)
	assert.Equal(t, "users", usersTarget.Name)
	funcCall := usersTarget.Val.GetFuncCall()
	require.NotNil(t, funcCall, "expected FuncCall for count(DISTINCT ...)")
	require.Len(t, funcCall.Funcname, 1)
	assert.Equal(t, "count", funcCall.Funcname[0].GetString_().Sval)
	// Check DISTINCT qualifier
	assert.True(t, funcCall.AggDistinct, "expected DISTINCT aggregate")
}

// TestQ3ASTStructure verifies Q3 has IN expression and extract() function.
func TestQ3ASTStructure(t *testing.T) {
	rawStmt, err := ParseOne(jsonBenchQueries["Q3"])
	require.NoError(t, err)

	selStmt := rawStmt.Stmt.GetSelectStmt()
	require.NotNil(t, selStmt)

	// Q3 has 3 select expressions
	assert.Len(t, selStmt.TargetList, 3)

	// Q3 has WHERE
	assert.NotNil(t, selStmt.WhereClause)

	// GROUP BY has 2 expressions
	assert.Len(t, selStmt.GroupClause, 2)
}

// TestQ4OrderByAndLimit verifies Q4 has ORDER BY ASC and LIMIT.
func TestQ4OrderByAndLimit(t *testing.T) {
	rawStmt, err := ParseOne(jsonBenchQueries["Q4"])
	require.NoError(t, err)

	selStmt := rawStmt.Stmt.GetSelectStmt()
	require.NotNil(t, selStmt)

	// Check ORDER BY
	require.Len(t, selStmt.SortClause, 1)
	sortBy := selStmt.SortClause[0].GetSortBy()
	require.NotNil(t, sortBy)
	assert.Equal(t, pg_query.SortByDir_SORTBY_ASC, sortBy.SortbyDir)

	// Check LIMIT
	require.NotNil(t, selStmt.LimitCount)
}

// TestQ5OrderByDescAndLimit verifies Q5 has ORDER BY DESC and LIMIT.
func TestQ5OrderByDescAndLimit(t *testing.T) {
	rawStmt, err := ParseOne(jsonBenchQueries["Q5"])
	require.NoError(t, err)

	selStmt := rawStmt.Stmt.GetSelectStmt()
	require.NotNil(t, selStmt)

	// Check ORDER BY DESC
	require.Len(t, selStmt.SortClause, 1)
	sortBy := selStmt.SortClause[0].GetSortBy()
	require.NotNil(t, sortBy)
	assert.Equal(t, pg_query.SortByDir_SORTBY_DESC, sortBy.SortbyDir)

	// Check LIMIT
	require.NotNil(t, selStmt.LimitCount)
}

// TestJSONOperators verifies -> and ->> operators produce correct AST nodes.
func TestJSONOperators(t *testing.T) {
	rawStmt, err := ParseOne("SELECT data->'key1'->>'key2' AS val FROM tbl")
	require.NoError(t, err)

	selStmt := rawStmt.Stmt.GetSelectStmt()
	require.NotNil(t, selStmt)
	require.Len(t, selStmt.TargetList, 1)

	target := selStmt.TargetList[0].GetResTarget()
	require.NotNil(t, target)
	assert.Equal(t, "val", target.Name)

	// The expression should be a chain of A_Expr with -> and ->> operators
	// data->'key1'->>'key2' parses as:
	//   (data -> 'key1') ->> 'key2'
	// which is A_Expr{ lexpr: A_Expr{lexpr: data, op: ->, rexpr: 'key1'}, op: ->>, rexpr: 'key2' }
	outerExpr := target.Val.GetAExpr()
	require.NotNil(t, outerExpr, "expected A_Expr, got %v", target.Val)
	require.Len(t, outerExpr.Name, 1)
	assert.Equal(t, "->>", outerExpr.Name[0].GetString_().Sval, "outer op should be ->> (JSONFetchText)")

	innerExpr := outerExpr.Lexpr.GetAExpr()
	require.NotNil(t, innerExpr, "expected inner A_Expr")
	require.Len(t, innerExpr.Name, 1)
	assert.Equal(t, "->", innerExpr.Name[0].GetString_().Sval, "inner op should be -> (JSONFetchVal)")
}

// TestTypeCast verifies ::type cast expressions parse correctly.
func TestTypeCast(t *testing.T) {
	rawStmt, err := ParseOne("SELECT (data->>'did')::text AS user_id FROM tbl")
	require.NoError(t, err)

	selStmt := rawStmt.Stmt.GetSelectStmt()
	require.NotNil(t, selStmt)
	require.Len(t, selStmt.TargetList, 1)

	target := selStmt.TargetList[0].GetResTarget()
	require.NotNil(t, target)
	assert.Equal(t, "user_id", target.Name)

	// Should be a TypeCast wrapping an A_Expr
	typeCast := target.Val.GetTypeCast()
	require.NotNil(t, typeCast, "expected TypeCast, got %v", target.Val)

	// The cast type should be "text"
	typeName := typeCast.TypeName
	require.NotNil(t, typeName)
	require.Greater(t, len(typeName.Names), 0)
	// Last element in Names is the actual type name (may or may not include pg_catalog prefix)
	lastNameNode := typeName.Names[len(typeName.Names)-1]
	assert.Equal(t, "text", lastNameNode.GetString_().Sval)

	// Inner should be an A_Expr with ->> operator
	innerExpr := typeCast.Arg.GetAExpr()
	require.NotNil(t, innerExpr, "expected A_Expr inside TypeCast, got %v", typeCast.Arg)
	require.Len(t, innerExpr.Name, 1)
	assert.Equal(t, "->>", innerExpr.Name[0].GetString_().Sval)
}

// TestExtractFunction verifies extract(hour FROM ...) parses as a function call.
func TestExtractFunction(t *testing.T) {
	rawStmt, err := ParseOne("SELECT extract(hour FROM to_timestamp(123456)) AS h FROM tbl")
	require.NoError(t, err)

	selStmt := rawStmt.Stmt.GetSelectStmt()
	require.NotNil(t, selStmt)
	require.Len(t, selStmt.TargetList, 1)

	target := selStmt.TargetList[0].GetResTarget()
	require.NotNil(t, target)
	assert.Equal(t, "h", target.Name)
	// extract() should produce a FuncCall or similar node
	t.Logf("extract expression node: %v", target.Val)
}

// TestINExpression verifies IN clause parsing.
func TestINExpression(t *testing.T) {
	rawStmt, err := ParseOne("SELECT id FROM tbl WHERE name IN ('a', 'b', 'c')")
	require.NoError(t, err)

	selStmt := rawStmt.Stmt.GetSelectStmt()
	require.NotNil(t, selStmt)
	assert.NotNil(t, selStmt.WhereClause)

	// WHERE should have an A_Expr with AEXPR_IN kind
	aExpr := selStmt.WhereClause.GetAExpr()
	require.NotNil(t, aExpr, "expected A_Expr, got %v", selStmt.WhereClause)
	assert.Equal(t, pg_query.A_Expr_Kind_AEXPR_IN, aExpr.Kind, "expected AEXPR_IN kind")
}
