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
	"math"
	"testing"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- FieldData helpers for tests ---

func makeInt64Field(name string, data []int64) *schemapb.FieldData {
	return newInt64FieldData(name, data)
}

func makeFloat64Field(name string, data []float64) *schemapb.FieldData {
	return newFloat64FieldData(name, data)
}

func makeStringField(name string, data []string) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_VarChar,
		FieldName: name,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{Data: data},
				},
			},
		},
	}
}

// --- fieldDataToFloat64 tests ---

func TestFieldDataToFloat64_Int64(t *testing.T) {
	fd := makeInt64Field("x", []int64{10, 20, 30})
	result := fieldDataToFloat64(fd)
	assert.Equal(t, []float64{10, 20, 30}, result)
}

func TestFieldDataToFloat64_Double(t *testing.T) {
	fd := makeFloat64Field("x", []float64{1.5, 2.5, 3.5})
	result := fieldDataToFloat64(fd)
	assert.Equal(t, []float64{1.5, 2.5, 3.5}, result)
}

func TestFieldDataToFloat64_String(t *testing.T) {
	fd := makeStringField("x", []string{"100", "200", "bad"})
	result := fieldDataToFloat64(fd)
	assert.Equal(t, []float64{100, 200, 0}, result)
}

func TestFieldDataToFloat64_Nil(t *testing.T) {
	result := fieldDataToFloat64(nil)
	assert.Nil(t, result)
}

// --- extractTimeField tests ---

func TestExtractTimeField_Hour(t *testing.T) {
	// 2024-01-15 14:30:00 UTC = 1705327800
	ts := float64(time.Date(2024, 1, 15, 14, 30, 0, 0, time.UTC).Unix())
	assert.Equal(t, float64(14), extractTimeField("hour", ts))
}

func TestExtractTimeField_Epoch(t *testing.T) {
	ts := 1705327800.5
	assert.Equal(t, 1705327800.5, extractTimeField("epoch", ts))
}

func TestExtractTimeField_Day(t *testing.T) {
	ts := float64(time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC).Unix())
	assert.Equal(t, float64(15), extractTimeField("day", ts))
}

func TestExtractTimeField_Month(t *testing.T) {
	ts := float64(time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC).Unix())
	assert.Equal(t, float64(3), extractTimeField("month", ts))
}

func TestExtractTimeField_Year(t *testing.T) {
	ts := float64(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Unix())
	assert.Equal(t, float64(2024), extractTimeField("year", ts))
}

func TestExtractTimeField_Unknown(t *testing.T) {
	assert.Equal(t, float64(0), extractTimeField("unknown_field", 12345.0))
}

// --- broadcast tests ---

func TestBroadcast(t *testing.T) {
	result := broadcast(5.0, 3)
	assert.Equal(t, []float64{5, 5, 5}, result)
}

func TestBroadcastPair(t *testing.T) {
	a := []float64{1}
	b := []float64{10, 20, 30}
	a, b = broadcastPair(a, b)
	assert.Equal(t, []float64{1, 1, 1}, a)
	assert.Equal(t, []float64{10, 20, 30}, b)
}

// --- toFieldData tests ---

func TestToFieldData_AllIntegers(t *testing.T) {
	fd := toFieldData("hours", []float64{1, 2, 3})
	assert.Equal(t, schemapb.DataType_Int64, fd.GetType())
	assert.Equal(t, "hours", fd.GetFieldName())
	assert.Equal(t, []int64{1, 2, 3}, fd.GetScalars().GetLongData().GetData())
}

func TestToFieldData_HasFractions(t *testing.T) {
	fd := toFieldData("val", []float64{1.5, 2.0, 3.7})
	assert.Equal(t, schemapb.DataType_Double, fd.GetType())
	assert.Equal(t, "val", fd.GetFieldName())
	assert.Equal(t, []float64{1.5, 2.0, 3.7}, fd.GetScalars().GetDoubleData().GetData())
}

// --- evalContext basic tests ---

func TestEvalContext_ColumnLookup(t *testing.T) {
	fields := []*schemapb.FieldData{
		makeInt64Field("col_a", []int64{10, 20, 30}),
		makeFloat64Field("col_b", []float64{1.5, 2.5, 3.5}),
	}
	ctx := newEvalContext(fields)

	assert.Equal(t, 3, ctx.rowCount)
	assert.Contains(t, ctx.columns, "col_a")
	assert.Contains(t, ctx.columns, "col_b")
}

func TestEvalContext_WithOverride(t *testing.T) {
	fields := []*schemapb.FieldData{
		makeInt64Field("x", []int64{1, 2, 3}),
	}
	ctx := newEvalContext(fields)
	overridden := ctx.withOverride("x", []float64{100, 200, 300})

	// Original unchanged
	assert.Equal(t, []float64{1, 2, 3}, ctx.columns["x"])
	// Override applied
	assert.Equal(t, []float64{100, 200, 300}, overridden.columns["x"])
}

// --- Expression evaluation via SQL parsing ---

func TestApplyProjection_NoComputed(t *testing.T) {
	// When there are no computed items, pass through unchanged
	comp := &SqlComponents{
		SelectItems: []SelectItem{
			{Type: SelectColumn, Column: &ColumnRef{FieldName: "name"}, Alias: "name"},
			{Type: SelectAgg, AggFunc: &AggFuncCall{FuncName: "count"}, Alias: "cnt"},
		},
	}
	fields := []*schemapb.FieldData{
		makeStringField("name", []string{"a", "b"}),
		makeInt64Field("count(*)", []int64{10, 20}),
	}
	result, err := ApplyProjection(fields, comp)
	require.NoError(t, err)
	assert.Equal(t, fields, result) // exact same slice, no copy
}

func TestApplyProjection_Q3Style_ExtractHour(t *testing.T) {
	// Q3 pattern: SELECT event, extract(hour FROM to_timestamp(time_us / 1e6)) AS hour_of_day, count(*)
	// Reducer output: [event_col, count_col, time_us_col (hidden group-by)]
	sql := jsonBenchQueries["Q3"]
	comp, err := ExtractSqlComponents(sql)
	require.NoError(t, err)

	// Simulate reducer output:
	// - Index 0: event (group-by key for data["commit"]["collection"])
	// - Index 1: count(*) (aggregate)
	// - Index 2: data["time_us"] (hidden group-by key, raw microsecond values)
	ts1 := time.Date(2024, 1, 15, 14, 30, 0, 0, time.UTC).UnixMicro() // hour=14
	ts2 := time.Date(2024, 1, 15, 8, 0, 0, 0, time.UTC).UnixMicro()   // hour=8
	ts3 := time.Date(2024, 1, 15, 22, 15, 0, 0, time.UTC).UnixMicro() // hour=22

	reducerFields := []*schemapb.FieldData{
		makeStringField(`data["commit"]["collection"]`, []string{"app.bsky.feed.post", "app.bsky.feed.like", "app.bsky.feed.repost"}),
		makeInt64Field("count(*)", []int64{100, 50, 75}),
		makeInt64Field(`data["time_us"]`, []int64{ts1, ts2, ts3}),
	}

	result, err := ApplyProjection(reducerFields, comp)
	require.NoError(t, err)
	require.Len(t, result, 3, "should have 3 output columns: event, hour_of_day, count")

	// Column 0: event (pass-through, renamed)
	assert.Equal(t, "event", result[0].GetFieldName())
	assert.Equal(t, []string{"app.bsky.feed.post", "app.bsky.feed.like", "app.bsky.feed.repost"},
		result[0].GetScalars().GetStringData().GetData())

	// Column 1: hour_of_day (computed via extract(hour, ...))
	assert.Equal(t, "hour_of_day", result[1].GetFieldName())
	hourData := fieldDataToFloat64(result[1])
	require.Len(t, hourData, 3)
	assert.Equal(t, float64(14), hourData[0])
	assert.Equal(t, float64(8), hourData[1])
	assert.Equal(t, float64(22), hourData[2])

	// Column 2: count (pass-through, renamed)
	assert.Equal(t, "count", result[2].GetFieldName())
	assert.Equal(t, []int64{100, 50, 75}, result[2].GetScalars().GetLongData().GetData())
}

func TestApplyProjection_Q5Style_AggArithmetic(t *testing.T) {
	// Q5 pattern: SELECT user_id, extract(epoch FROM (max(...) - min(...))) * 1000 AS activity_span
	// Reducer output: [user_id_col, max_col (hidden), min_col (hidden)]
	sql := jsonBenchQueries["Q5"]
	comp, err := ExtractSqlComponents(sql)
	require.NoError(t, err)

	// Simulate reducer output:
	// user_id group-by values
	// max/min aggregate results (raw microsecond values)
	// User 1: active from hour 10 to hour 14 → span = 4 hours = 14400 seconds = 14400000 ms
	// User 2: active from hour 8 to hour 8:30 → span = 0.5 hours = 1800 seconds = 1800000 ms
	min1 := int64(time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC).UnixMicro())
	max1 := int64(time.Date(2024, 1, 15, 14, 0, 0, 0, time.UTC).UnixMicro())
	min2 := int64(time.Date(2024, 1, 15, 8, 0, 0, 0, time.UTC).UnixMicro())
	max2 := int64(time.Date(2024, 1, 15, 8, 30, 0, 0, time.UTC).UnixMicro())

	reducerFields := []*schemapb.FieldData{
		makeStringField(`data["did"]`, []string{"user1", "user2"}),
		makeInt64Field(`max(data["time_us"])`, []int64{max1, max2}),
		makeInt64Field(`min(data["time_us"])`, []int64{min1, min2}),
	}

	result, err := ApplyProjection(reducerFields, comp)
	require.NoError(t, err)
	require.Len(t, result, 2, "should have 2 output columns: user_id, activity_span")

	// Column 0: user_id (pass-through)
	assert.Equal(t, "user_id", result[0].GetFieldName())

	// Column 1: activity_span = extract(epoch, (max_ts - min_ts)) * 1000
	// User 1: (max1 - min1) / 1e6 = 14400 seconds, * 1000 = 14400000
	// User 2: (max2 - min2) / 1e6 = 1800 seconds, * 1000 = 1800000
	assert.Equal(t, "activity_span", result[1].GetFieldName())
	spanData := fieldDataToFloat64(result[1])
	require.Len(t, spanData, 2)
	assert.InDelta(t, 14400000.0, spanData[0], 1.0)
	assert.InDelta(t, 1800000.0, spanData[1], 1.0)
}

// --- ExtractNestedAggregates tests ---

func TestExtractNestedAggregates_Q5(t *testing.T) {
	sql := jsonBenchQueries["Q5"]
	comp, err := ExtractSqlComponents(sql)
	require.NoError(t, err)

	// The second SELECT item is the computed expression with max/min inside
	require.True(t, len(comp.SelectItems) >= 2)
	si := comp.SelectItems[1]
	require.Equal(t, SelectComputed, si.Type)
	require.NotNil(t, si.RawExpr)

	aggs := ExtractNestedAggregates(si.RawExpr)
	require.Len(t, aggs, 2, "should find max and min nested aggregates")

	names := map[string]bool{}
	for _, a := range aggs {
		names[a.FuncName] = true
		assert.Equal(t, "data", a.Column.FieldName)
		assert.Equal(t, []string{"time_us"}, a.Column.NestedPath)
	}
	assert.True(t, names["max"])
	assert.True(t, names["min"])
}

func TestExtractNestedAggregates_Q3_NoNestedAgg(t *testing.T) {
	sql := jsonBenchQueries["Q3"]
	comp, err := ExtractSqlComponents(sql)
	require.NoError(t, err)

	// The second SELECT item is the computed extract(hour...) - no aggregate inside
	require.True(t, len(comp.SelectItems) >= 2)
	si := comp.SelectItems[1]
	require.Equal(t, SelectComputed, si.Type)
	require.NotNil(t, si.RawExpr)

	aggs := ExtractNestedAggregates(si.RawExpr)
	assert.Empty(t, aggs, "extract(hour...) has no nested aggregates")
}

func TestExtractNestedAggregates_SimpleColumn(t *testing.T) {
	sql := `SELECT data->>'name' AS name FROM tbl`
	comp, err := ExtractSqlComponents(sql)
	require.NoError(t, err)
	require.Len(t, comp.SelectItems, 1)
	// No computed items, no aggregates
	aggs := ExtractNestedAggregates(comp.SelectItems[0].RawExpr)
	assert.Empty(t, aggs)
}

// --- ExtractDeepColumnRef export test ---

func TestExtractDeepColumnRef_Export(t *testing.T) {
	sql := `SELECT min(to_timestamp((data->>'time_us')::bigint / 1000000.0)) FROM bluesky`
	comp, err := ExtractSqlComponents(sql)
	require.NoError(t, err)

	si := comp.SelectItems[0]
	col := ExtractDeepColumnRef(si.RawExpr)
	require.NotNil(t, col)
	assert.Equal(t, "data", col.FieldName)
	assert.Equal(t, []string{"time_us"}, col.NestedPath)
}

// --- isDirectColumnRef tests ---

func TestIsDirectColumnRef(t *testing.T) {
	sql := `SELECT data->>'name', to_timestamp(x) FROM tbl`
	comp, err := ExtractSqlComponents(sql)
	require.NoError(t, err)

	// data->>'name' is a direct column ref (JSON path)
	assert.True(t, isDirectColumnRef(comp.SelectItems[0].RawExpr))

	// to_timestamp(x) is NOT direct
	assert.False(t, isDirectColumnRef(comp.SelectItems[1].RawExpr))
}

// --- Arithmetic evaluation via parsed SQL ---

func TestEval_BasicArithmetic(t *testing.T) {
	// Parse: SELECT (a + b) * 2.0 FROM tbl
	// Provide a and b in the context
	sql := `SELECT (col_a + col_b) * 2.0 AS result FROM tbl`
	comp, err := ExtractSqlComponents(sql)
	require.NoError(t, err)

	fields := []*schemapb.FieldData{
		makeFloat64Field("col_a", []float64{1, 2, 3}),
		makeFloat64Field("col_b", []float64{10, 20, 30}),
	}
	ctx := newEvalContext(fields)

	si := comp.SelectItems[0]
	require.Equal(t, SelectComputed, si.Type)

	values, err := ctx.eval(si.RawExpr)
	require.NoError(t, err)
	assert.Equal(t, []float64{22, 44, 66}, values)
}

func TestEval_DivisionByZero(t *testing.T) {
	sql := `SELECT x / y AS result FROM tbl`
	comp, err := ExtractSqlComponents(sql)
	require.NoError(t, err)

	fields := []*schemapb.FieldData{
		makeFloat64Field("x", []float64{10, 20}),
		makeFloat64Field("y", []float64{0, 5}),
	}
	ctx := newEvalContext(fields)

	values, err := ctx.eval(comp.SelectItems[0].RawExpr)
	require.NoError(t, err)
	assert.Equal(t, float64(0), values[0], "division by zero should return 0")
	assert.Equal(t, float64(4), values[1])
}

// --- shallowCopyFieldData test ---

func TestShallowCopyFieldData(t *testing.T) {
	fd := makeInt64Field("original", []int64{1, 2, 3})
	cp := shallowCopyFieldData(fd)

	cp.FieldName = "renamed"
	assert.Equal(t, "original", fd.GetFieldName(), "original should be unchanged")
	assert.Equal(t, "renamed", cp.GetFieldName())
	// Same underlying data
	assert.Equal(t, fd.GetScalars().GetLongData().GetData(), cp.GetScalars().GetLongData().GetData())
}

// --- Edge cases ---

func TestApplyProjection_EmptyComputed(t *testing.T) {
	comp := &SqlComponents{
		SelectItems: []SelectItem{
			{Type: SelectComputed, Alias: "bad"},
		},
	}
	_, err := ApplyProjection([]*schemapb.FieldData{}, comp)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no RawExpr")
}

// --- Verify Q3 extract produces correct hour for edge cases ---

func TestExtractTimeField_Midnight(t *testing.T) {
	ts := float64(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Unix())
	assert.Equal(t, float64(0), extractTimeField("hour", ts))
}

func TestExtractTimeField_EndOfDay(t *testing.T) {
	ts := float64(time.Date(2024, 1, 1, 23, 59, 59, 0, time.UTC).Unix())
	assert.Equal(t, float64(23), extractTimeField("hour", ts))
}

func TestExtractTimeField_FractionalSeconds(t *testing.T) {
	ts := float64(time.Date(2024, 1, 1, 12, 30, 45, 500000000, time.UTC).UnixNano()) / 1e9
	sec := extractTimeField("second", ts)
	assert.InDelta(t, 45.5, sec, 0.01)
}

// --- Verify the evalContext doesn't crash on large values ---

func TestEval_LargeTimestamps(t *testing.T) {
	// Typical bluesky time_us values (microseconds since epoch)
	ts := int64(time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC).UnixMicro())

	fields := []*schemapb.FieldData{
		makeInt64Field(`data["time_us"]`, []int64{ts}),
	}
	ctx := newEvalContext(fields)

	vals, ok := ctx.columns[`data["time_us"]`]
	require.True(t, ok)
	require.Len(t, vals, 1)

	// Convert to seconds and extract hour
	seconds := vals[0] / 1e6
	hour := extractTimeField("hour", seconds)
	assert.Equal(t, float64(12), hour)
}

func TestApplyProjection_PassthroughAliasRename(t *testing.T) {
	comp := &SqlComponents{
		SelectItems: []SelectItem{
			{Type: SelectColumn, Column: &ColumnRef{FieldName: "name"}, Alias: "user_name"},
		},
	}
	fields := []*schemapb.FieldData{
		makeStringField("name", []string{"alice", "bob"}),
	}
	result, err := ApplyProjection(fields, comp)
	require.NoError(t, err)
	require.Len(t, result, 1)
	// With no computed items, should return original unchanged (no rename needed)
	assert.Equal(t, "name", result[0].GetFieldName())
}

func TestApplyProjection_MixedWithComputed_AliasApplied(t *testing.T) {
	// When there IS a computed item, non-computed items get alias applied
	sql := `SELECT name AS user_name, age * 2 AS doubled_age FROM tbl`
	comp, err := ExtractSqlComponents(sql)
	require.NoError(t, err)

	fields := []*schemapb.FieldData{
		makeStringField("name", []string{"alice", "bob"}),
		makeFloat64Field("age", []float64{25, 30}),
	}

	result, err := ApplyProjection(fields, comp)
	require.NoError(t, err)
	require.Len(t, result, 2)

	assert.Equal(t, "user_name", result[0].GetFieldName())
	assert.Equal(t, "doubled_age", result[1].GetFieldName())

	doubled := fieldDataToFloat64(result[1])
	assert.Equal(t, []float64{50, 60}, doubled)
}

func TestApplyProjection_FloatResult(t *testing.T) {
	sql := `SELECT x / 3.0 AS ratio FROM tbl`
	comp, err := ExtractSqlComponents(sql)
	require.NoError(t, err)

	fields := []*schemapb.FieldData{
		makeFloat64Field("x", []float64{10}),
	}

	result, err := ApplyProjection(fields, comp)
	require.NoError(t, err)
	require.Len(t, result, 1)

	assert.Equal(t, schemapb.DataType_Double, result[0].GetType())
	ratio := result[0].GetScalars().GetDoubleData().GetData()
	assert.InDelta(t, 10.0/3.0, ratio[0], 0.001)
}

// Suppress unused import warning for math
var _ = math.MaxInt64
