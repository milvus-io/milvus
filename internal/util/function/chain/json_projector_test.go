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

package chain

import (
	"math"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func jsonProjectorTestResult(topks []int64, fields ...*schemapb.FieldData) *schemapb.SearchResultData {
	totalRows := 0
	for _, topk := range topks {
		totalRows += int(topk)
	}
	ids := make([]int64, totalRows)
	scores := make([]float32, totalRows)
	for i := range totalRows {
		ids[i] = int64(i + 1)
		scores[i] = float32(totalRows-i) / float32(totalRows)
	}
	return &schemapb.SearchResultData{
		NumQueries: int64(len(topks)),
		Topks:      topks,
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{Data: ids},
		}},
		Scores:     scores,
		FieldsData: fields,
	}
}

func jsonProjectorTestJSONField(fieldID int64, name string, rows []string, valid []bool) *schemapb.FieldData {
	data := make([][]byte, len(rows))
	for i, row := range rows {
		data[i] = []byte(row)
	}
	return &schemapb.FieldData{
		Type:      schemapb.DataType_JSON,
		FieldName: name,
		FieldId:   fieldID,
		ValidData: valid,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{Data: data}},
		}},
	}
}

func jsonProjectorTestInt64Field(fieldID int64, name string, values []int64) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: name,
		FieldId:   fieldID,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: values}},
		}},
	}
}

func testDataFrameInputPlan(resultData *schemapb.SearchResultData, fieldNames ...string) *DataFrameInputPlan {
	if len(fieldNames) == 0 {
		return nil
	}
	plan := &DataFrameInputPlan{Inputs: make([]ResolvedChainInput, 0, len(fieldNames))}
	for _, fieldName := range fieldNames {
		for _, fieldData := range resultData.GetFieldsData() {
			if fieldData.GetFieldName() != fieldName {
				continue
			}
			hint := schemapb.DataType_None
			if fieldData.GetType() == schemapb.DataType_JSON {
				hint = schemapb.DataType_JSON
			}
			plan.Inputs = append(plan.Inputs, ResolvedChainInput{
				LogicalName:   fieldName,
				SourceFieldID: fieldData.GetFieldId(),
				FieldName:     fieldName,
				DataType:      fieldData.GetType(),
				DataTypeHint:  hint,
			})
		}
	}
	return plan
}

func TestFromSearchResultDataProjectsJSON(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer func() { pool.AssertSize(t, 0) }()

	result := jsonProjectorTestResult(
		[]int64{2, 1},
		jsonProjectorTestInt64Field(101, "", []int64{10, 20, 30}),
		jsonProjectorTestJSONField(103, "metadata", []string{
			`{"price":1,"category":"a","nested":{"enabled":true}}`,
			`{"price":2.5,"category":"b","nested":{"enabled":false}}`,
			`{"price":null,"category":"c","nested":{}}`,
		}, nil),
		jsonProjectorTestJSONField(104, common.MetaFieldName, []string{
			`{"ctr":10}`,
			`{"ctr":20}`,
			`{"ctr":30}`,
		}, nil),
	)
	plan := &DataFrameInputPlan{Inputs: []ResolvedChainInput{
		{LogicalName: "age", SourceFieldID: 101, FieldName: "age", DataType: schemapb.DataType_Int64},
		{
			LogicalName: `metadata["price"]`, SourceFieldID: 103, FieldName: "metadata",
			DataType: schemapb.DataType_JSON, NestedPath: []string{"price"}, DataTypeHint: schemapb.DataType_Double,
		},
		{
			LogicalName: `metadata["category"]`, SourceFieldID: 103, FieldName: "metadata",
			DataType: schemapb.DataType_JSON, NestedPath: []string{"category"}, DataTypeHint: schemapb.DataType_VarChar,
		},
		{
			LogicalName: `metadata["nested"]["enabled"]`, SourceFieldID: 103, FieldName: "metadata",
			DataType: schemapb.DataType_JSON, NestedPath: []string{"nested", "enabled"}, DataTypeHint: schemapb.DataType_Bool,
		},
		{
			LogicalName: `$meta["ctr"]`, SourceFieldID: 104, FieldName: common.MetaFieldName,
			DataType: schemapb.DataType_JSON, NestedPath: []string{"ctr"}, DataTypeHint: schemapb.DataType_Int64,
		},
	}}

	df, err := FromSearchResultData(result, pool, plan)
	require.NoError(t, err)
	defer df.Release()

	assert.Equal(t, []int64{2, 1}, df.ChunkSizes())
	assert.Equal(t, []string{
		types.IDFieldName,
		types.ScoreFieldName,
		"age",
		`metadata["price"]`,
		`metadata["category"]`,
		`metadata["nested"]["enabled"]`,
		`$meta["ctr"]`,
	}, df.ColumnNames())
	assert.Equal(t, int64(101), mustFieldID(t, df, "age"))
	_, hasJSONPathFieldID := df.FieldID(`metadata["price"]`)
	assert.False(t, hasJSONPathFieldID)

	price := df.Column(`metadata["price"]`)
	require.IsType(t, &array.Float64{}, price.Chunk(0))
	priceChunk0 := price.Chunk(0).(*array.Float64)
	assert.Equal(t, 1.0, priceChunk0.Value(0))
	assert.Equal(t, 2.5, priceChunk0.Value(1))
	assert.True(t, price.Chunk(1).IsNull(0))
	assert.Equal(t, schemapb.DataType_Double, mustFieldType(t, df, `metadata["price"]`))

	category := df.Column(`metadata["category"]`).Chunk(0).(*array.String)
	assert.Equal(t, "a", category.Value(0))
	assert.Equal(t, "b", category.Value(1))

	enabled := df.Column(`metadata["nested"]["enabled"]`)
	assert.True(t, enabled.Chunk(0).(*array.Boolean).Value(0))
	assert.False(t, enabled.Chunk(0).(*array.Boolean).Value(1))
	assert.True(t, enabled.Chunk(1).IsNull(0))

	ctr := df.Column(`$meta["ctr"]`).Chunk(1).(*array.Int64)
	assert.Equal(t, int64(30), ctr.Value(0))
}

func TestFromSearchResultDataProjectsAllSupportedJSONScalarTypes(t *testing.T) {
	tests := []struct {
		name      string
		rows      []string
		hint      schemapb.DataType
		checkData func(*testing.T, arrow.Array)
	}{
		{
			name: "bool", rows: []string{`{"value":true}`, `{"value":false}`}, hint: schemapb.DataType_Bool,
			checkData: func(t *testing.T, data arrow.Array) {
				values := data.(*array.Boolean)
				assert.True(t, values.Value(0))
				assert.False(t, values.Value(1))
			},
		},
		{
			name: "int64 boundaries", rows: []string{`{"value":-9223372036854775808}`, `{"value":9223372036854775807}`}, hint: schemapb.DataType_Int64,
			checkData: func(t *testing.T, data arrow.Array) {
				values := data.(*array.Int64)
				assert.Equal(t, int64(-9223372036854775807-1), values.Value(0))
				assert.Equal(t, int64(9223372036854775807), values.Value(1))
			},
		},
		{
			name: "double", rows: []string{`{"value":1.25}`, `{"value":-2.5}`}, hint: schemapb.DataType_Double,
			checkData: func(t *testing.T, data arrow.Array) {
				values := data.(*array.Float64)
				assert.Equal(t, 1.25, values.Value(0))
				assert.Equal(t, -2.5, values.Value(1))
			},
		},
		{
			name: "varchar", rows: []string{`{"value":"a"}`, `{"value":"b"}`}, hint: schemapb.DataType_VarChar,
			checkData: func(t *testing.T, data arrow.Array) {
				values := data.(*array.String)
				assert.Equal(t, "a", values.Value(0))
				assert.Equal(t, "b", values.Value(1))
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer func() { pool.AssertSize(t, 0) }()
			result := jsonProjectorTestResult(
				[]int64{2},
				jsonProjectorTestJSONField(103, "metadata", test.rows, nil),
			)
			plan := &DataFrameInputPlan{Inputs: []ResolvedChainInput{{
				LogicalName: `metadata["value"]`, SourceFieldID: 103, FieldName: "metadata",
				DataType: schemapb.DataType_JSON, NestedPath: []string{"value"}, DataTypeHint: test.hint,
			}}}

			df, err := FromSearchResultData(result, pool, plan)
			require.NoError(t, err)
			defer df.Release()
			column := df.Column(`metadata["value"]`)
			require.NotNil(t, column)
			require.Zero(t, column.NullN())
			test.checkData(t, column.Chunk(0))
		})
	}
}

func TestFromSearchResultDataComplexValuesBecomeNull(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer func() { pool.AssertSize(t, 0) }()

	result := jsonProjectorTestResult(
		[]int64{3},
		jsonProjectorTestJSONField(103, "metadata", []string{
			`{"items":[1,"two"]}`,
			`{"items":{"nested":true}}`,
			`{"items":1e-400}`,
		}, nil),
	)
	plan := &DataFrameInputPlan{Inputs: []ResolvedChainInput{{
		LogicalName: `metadata["items"]`, SourceFieldID: 103, FieldName: "metadata",
		DataType: schemapb.DataType_JSON, NestedPath: []string{"items"}, DataTypeHint: schemapb.DataType_VarChar,
	}}}

	df, err := FromSearchResultData(result, pool, plan)
	require.NoError(t, err)
	defer df.Release()

	items := df.Column(`metadata["items"]`).Chunk(0).(*array.String)
	assert.Equal(t, 3, items.NullN())
}

func TestFromSearchResultDataProjectsJSONArrayIndex(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer func() { pool.AssertSize(t, 0) }()

	result := jsonProjectorTestResult(
		[]int64{2},
		jsonProjectorTestJSONField(103, "metadata", []string{
			`{"items":[{"name":"first"},{"name":"second"}]}`,
			`{"items":[]}`,
		}, nil),
	)
	plan := &DataFrameInputPlan{Inputs: []ResolvedChainInput{{
		LogicalName: `metadata["items"][1]["name"]`, SourceFieldID: 103, FieldName: "metadata",
		DataType: schemapb.DataType_JSON, NestedPath: []string{"items", "1", "name"},
		DataTypeHint: schemapb.DataType_VarChar,
	}}}

	df, err := FromSearchResultData(result, pool, plan)
	require.NoError(t, err)
	defer df.Release()

	values := df.Column(`metadata["items"][1]["name"]`).Chunk(0).(*array.String)
	assert.Equal(t, "second", values.Value(0))
	assert.True(t, values.IsNull(1))
}

func TestLookupJSONPathArrayIndexSyntax(t *testing.T) {
	document := map[string]any{
		"items": []any{"zero", "one"},
		"object": map[string]any{
			"01": "leading-zero key",
		},
	}

	tests := []struct {
		name     string
		path     []string
		expected any
	}{
		{name: "zero", path: []string{"items", "0"}, expected: "zero"},
		{name: "non-zero", path: []string{"items", "1"}, expected: "one"},
		{name: "leading zero", path: []string{"items", "01"}, expected: nil},
		{name: "leading plus", path: []string{"items", "+1"}, expected: nil},
		{name: "negative", path: []string{"items", "-1"}, expected: nil},
		{name: "overflow", path: []string{"items", "999999999999999999999999"}, expected: nil},
		{name: "object key keeps leading zero", path: []string{"object", "01"}, expected: "leading-zero key"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expected, lookupJSONPath(document, test.path))
		})
	}
}

func TestFromSearchResultDataPreservesGroupByValueAlongsideJSONPath(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer func() { pool.AssertSize(t, 0) }()

	result := jsonProjectorTestResult(
		[]int64{2},
		jsonProjectorTestJSONField(103, "metadata", []string{
			`{"price":10,"category":"a"}`,
			`{"price":20,"category":"b"}`,
		}, nil),
	)
	result.GroupByFieldValues = []*schemapb.FieldData{{
		Type: schemapb.DataType_VarChar, FieldName: "metadata", FieldId: 103,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"a", "b"}}},
		}},
	}}
	plan := &DataFrameInputPlan{Inputs: []ResolvedChainInput{{
		LogicalName: `metadata["price"]`, SourceFieldID: 103, FieldName: "metadata",
		DataType: schemapb.DataType_JSON, NestedPath: []string{"price"}, DataTypeHint: schemapb.DataType_Int64,
	}}}

	df, err := FromSearchResultData(result, pool, plan)
	require.NoError(t, err)
	defer df.Release()

	assert.True(t, df.HasColumn(`metadata["price"]`))
	groupBy := df.Column("metadata").Chunk(0).(*array.String)
	assert.Equal(t, "a", groupBy.Value(0))
	assert.Equal(t, "b", groupBy.Value(1))
}

func TestFromSearchResultDataAllMissingUsesDeclaredType(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer func() { pool.AssertSize(t, 0) }()

	result := jsonProjectorTestResult(
		[]int64{2},
		jsonProjectorTestJSONField(103, "metadata", []string{`{}`, `{"other":1}`}, nil),
	)
	plan := &DataFrameInputPlan{Inputs: []ResolvedChainInput{{
		LogicalName: `metadata["value"]`, SourceFieldID: 103, FieldName: "metadata",
		DataType: schemapb.DataType_JSON, NestedPath: []string{"value"}, DataTypeHint: schemapb.DataType_Int64,
	}}}

	df, err := FromSearchResultData(result, pool, plan)
	require.NoError(t, err)
	defer df.Release()

	values := df.Column(`metadata["value"]`).Chunk(0)
	assert.IsType(t, &array.Int64{}, values)
	assert.Equal(t, 2, values.NullN())
}

func TestFromSearchResultDataZeroRowsMaterializesAllPlannedColumns(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer func() { pool.AssertSize(t, 0) }()

	result := jsonProjectorTestResult([]int64{0, 0})
	plan := &DataFrameInputPlan{Inputs: []ResolvedChainInput{
		{
			LogicalName: "age", SourceFieldID: 101, FieldName: "age",
			DataType: schemapb.DataType_Int64, Nullable: true,
		},
		{
			LogicalName: `metadata["price"]`, SourceFieldID: 103, FieldName: "metadata",
			DataType: schemapb.DataType_JSON, NestedPath: []string{"price"}, DataTypeHint: schemapb.DataType_Double,
		},
		{
			LogicalName: `$meta["category"]`, SourceFieldID: 104, FieldName: common.MetaFieldName,
			DataType: schemapb.DataType_JSON, NestedPath: []string{"category"}, DataTypeHint: schemapb.DataType_VarChar,
		},
	}}

	df, err := FromSearchResultData(result, pool, plan)
	require.NoError(t, err)
	defer df.Release()

	assert.Equal(t, []int64{0, 0}, df.ChunkSizes())
	age := df.Column("age")
	require.NotNil(t, age)
	assert.IsType(t, &array.Int64{}, age.Chunk(0))
	assert.Len(t, age.Chunks(), 2)
	assert.Zero(t, age.Len())
	assert.Equal(t, int64(101), mustFieldID(t, df, "age"))
	assert.Equal(t, schemapb.DataType_Int64, mustFieldType(t, df, "age"))
	assert.True(t, df.fieldNullables["age"])
	price := df.Column(`metadata["price"]`)
	require.NotNil(t, price)
	assert.IsType(t, &array.Float64{}, price.Chunk(0))
	assert.Len(t, price.Chunks(), 2)
	assert.Zero(t, price.Len())
	category := df.Column(`$meta["category"]`)
	require.NotNil(t, category)
	assert.IsType(t, &array.String{}, category.Chunk(0))
	assert.Len(t, category.Chunks(), 2)
	assert.Zero(t, category.Len())
}

func TestFromSearchResultDataNullableJSONRoot(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer func() { pool.AssertSize(t, 0) }()

	result := jsonProjectorTestResult(
		[]int64{2},
		jsonProjectorTestJSONField(103, "metadata", []string{
			`{"value":1}`,
			`not parsed because the root is null`,
		}, []bool{true, false}),
	)
	plan := &DataFrameInputPlan{Inputs: []ResolvedChainInput{{
		LogicalName: `metadata["value"]`, SourceFieldID: 103, FieldName: "metadata",
		DataType: schemapb.DataType_JSON, NestedPath: []string{"value"}, DataTypeHint: schemapb.DataType_Int64,
	}}}

	df, err := FromSearchResultData(result, pool, plan)
	require.NoError(t, err)
	defer df.Release()

	values := df.Column(`metadata["value"]`).Chunk(0).(*array.Int64)
	assert.Equal(t, int64(1), values.Value(0))
	assert.True(t, values.IsNull(1))
}

func TestFromSearchResultDataProjectsMaxInt64(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer func() { pool.AssertSize(t, 0) }()

	result := jsonProjectorTestResult(
		[]int64{1},
		jsonProjectorTestJSONField(103, "metadata", []string{
			`{"value":9223372036854775807}`,
		}, nil),
	)
	plan := &DataFrameInputPlan{Inputs: []ResolvedChainInput{{
		LogicalName: `metadata["value"]`, SourceFieldID: 103, FieldName: "metadata",
		DataType: schemapb.DataType_JSON, NestedPath: []string{"value"}, DataTypeHint: schemapb.DataType_Int64,
	}}}

	df, err := FromSearchResultData(result, pool, plan)
	require.NoError(t, err)
	defer df.Release()

	values := df.Column(`metadata["value"]`).Chunk(0).(*array.Int64)
	assert.Equal(t, int64(9223372036854775807), values.Value(0))
}

func TestFromSearchResultDataTypeMismatchBecomesNull(t *testing.T) {
	tests := []struct {
		name      string
		rows      []string
		hint      schemapb.DataType
		nullCount int
	}{
		{
			name: "mixed declared types", rows: []string{`{"value":1}`, `{"value":"2"}`},
			hint: schemapb.DataType_Double, nullCount: 1,
		},
		{
			name: "floating value incompatible with int64", rows: []string{`{"value":1.5}`},
			hint: schemapb.DataType_Int64, nullCount: 1,
		},
		{
			name: "uint64 incompatible with int64", rows: []string{`{"value":18446744073709551615}`},
			hint: schemapb.DataType_Int64, nullCount: 1,
		},
		{
			name: "float64 overflow", rows: []string{`{"value":1e400}`},
			hint: schemapb.DataType_Double, nullCount: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer func() { pool.AssertSize(t, 0) }()
			result := jsonProjectorTestResult(
				[]int64{int64(len(test.rows))},
				jsonProjectorTestJSONField(103, "metadata", test.rows, nil),
			)
			plan := &DataFrameInputPlan{Inputs: []ResolvedChainInput{{
				LogicalName: `metadata["value"]`, SourceFieldID: 103, FieldName: "metadata",
				DataType: schemapb.DataType_JSON, NestedPath: []string{"value"}, DataTypeHint: test.hint,
			}}}

			df, err := FromSearchResultData(result, pool, plan)
			require.NoError(t, err)
			defer df.Release()
			assert.Equal(t, test.nullCount, df.Column(`metadata["value"]`).NullN())
		})
	}
}

func TestFromSearchResultDataTypeMismatchPreservesPositionsAcrossChunks(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer func() { pool.AssertSize(t, 0) }()
	result := jsonProjectorTestResult(
		[]int64{2, 2},
		jsonProjectorTestJSONField(103, "metadata", []string{
			`{"value":1}`,
			`{"value":"bad"}`,
			`{"value":3}`,
			`{"value":null}`,
		}, nil),
	)
	plan := &DataFrameInputPlan{Inputs: []ResolvedChainInput{{
		LogicalName: `metadata["value"]`, SourceFieldID: 103, FieldName: "metadata",
		DataType: schemapb.DataType_JSON, NestedPath: []string{"value"}, DataTypeHint: schemapb.DataType_Double,
	}}}

	df, err := FromSearchResultData(result, pool, plan)
	require.NoError(t, err)
	defer df.Release()
	column := df.Column(`metadata["value"]`)
	require.Len(t, column.Chunks(), 2)
	first := column.Chunk(0).(*array.Float64)
	assert.Equal(t, 1.0, first.Value(0))
	assert.True(t, first.IsNull(1))
	second := column.Chunk(1).(*array.Float64)
	assert.Equal(t, 3.0, second.Value(0))
	assert.True(t, second.IsNull(1))
}

func TestFromSearchResultDataJSONProjectionPreservesEmptyChunk(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer func() { pool.AssertSize(t, 0) }()
	result := jsonProjectorTestResult(
		[]int64{1, 0, 1},
		jsonProjectorTestJSONField(103, "metadata", []string{
			`{"value":1}`,
			`{"value":2}`,
		}, nil),
	)
	plan := &DataFrameInputPlan{Inputs: []ResolvedChainInput{{
		LogicalName: `metadata["value"]`, SourceFieldID: 103, FieldName: "metadata",
		DataType: schemapb.DataType_JSON, NestedPath: []string{"value"}, DataTypeHint: schemapb.DataType_Int64,
	}}}

	df, err := FromSearchResultData(result, pool, plan)
	require.NoError(t, err)
	defer df.Release()

	column := df.Column(`metadata["value"]`)
	require.Len(t, column.Chunks(), 3)
	assert.Equal(t, int64(1), column.Chunk(0).(*array.Int64).Value(0))
	assert.Zero(t, column.Chunk(1).Len())
	assert.Equal(t, int64(2), column.Chunk(2).(*array.Int64).Value(0))
}

func TestFromSearchResultDataRejectsMalformedJSON(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer func() { pool.AssertSize(t, 0) }()
	result := jsonProjectorTestResult(
		[]int64{1, 1},
		jsonProjectorTestJSONField(103, "metadata", []string{`{"value":"valid"}`, `{"value":`}, nil),
	)
	plan := &DataFrameInputPlan{Inputs: []ResolvedChainInput{{
		LogicalName: `metadata["value"]`, SourceFieldID: 103, FieldName: "metadata",
		DataType: schemapb.DataType_JSON, NestedPath: []string{"value"}, DataTypeHint: schemapb.DataType_VarChar,
	}}}

	df, err := FromSearchResultData(result, pool, plan)
	require.Error(t, err)
	assert.Nil(t, df)
	assert.ErrorContains(t, err, "invalid JSON")
	assert.True(t, errors.Is(err, merr.ErrDataIntegrity))
}

func TestFromSearchResultDataValidatesPhysicalField(t *testing.T) {
	tests := []struct {
		name       string
		field      *schemapb.FieldData
		expectText string
	}{
		{name: "missing", expectText: "is missing from search result"},
		{
			name:       "wrong type",
			field:      jsonProjectorTestInt64Field(103, "metadata", []int64{1}),
			expectText: "type mismatch",
		},
		{
			name:       "wrong name",
			field:      jsonProjectorTestJSONField(103, "other", []string{`{"value":1}`}, nil),
			expectText: "name mismatch",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer func() { pool.AssertSize(t, 0) }()
			fields := make([]*schemapb.FieldData, 0, 1)
			if test.field != nil {
				fields = append(fields, test.field)
			}
			result := jsonProjectorTestResult([]int64{1}, fields...)
			plan := &DataFrameInputPlan{Inputs: []ResolvedChainInput{{
				LogicalName: `metadata["value"]`, SourceFieldID: 103, FieldName: "metadata",
				DataType: schemapb.DataType_JSON, NestedPath: []string{"value"}, DataTypeHint: schemapb.DataType_Int64,
			}}}

			df, err := FromSearchResultData(result, pool, plan)
			require.Error(t, err)
			assert.Nil(t, df)
			assert.ErrorContains(t, err, test.expectText)
		})
	}
}

func mustFieldType(t *testing.T, df *DataFrame, name string) schemapb.DataType {
	t.Helper()
	dataType, ok := df.FieldType(name)
	require.True(t, ok)
	return dataType
}

func mustFieldID(t *testing.T, df *DataFrame, name string) int64 {
	t.Helper()
	fieldID, ok := df.FieldID(name)
	require.True(t, ok)
	return fieldID
}

func TestFromSearchResultDataMaterializesNarrowIntegers(t *testing.T) {
	for _, dataType := range []schemapb.DataType{schemapb.DataType_Int8, schemapb.DataType_Int16} {
		t.Run(dataType.String(), func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)
			minValue, maxValue := int32(-128), int32(127)
			if dataType == schemapb.DataType_Int16 {
				minValue, maxValue = -32768, 32767
			}
			input := ResolvedChainInput{
				LogicalName: "value", FieldName: "value", SourceFieldID: 101,
				DataType: dataType, Nullable: true,
			}
			plan := &DataFrameInputPlan{Inputs: []ResolvedChainInput{input}}
			for _, valid := range [][]bool{{true, false, true}, {false, false, false}, {}} {
				values := []int32{minValue, 0, maxValue}[:len(valid)]
				field := &schemapb.FieldData{
					Type: dataType, FieldName: "value", FieldId: 101, ValidData: valid,
					Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: values}},
					}},
				}
				df, err := FromSearchResultData(jsonProjectorTestResult([]int64{int64(len(valid))}, field), pool, plan)
				require.NoError(t, err)
				func() {
					defer df.Release()
					require.NoError(t, ValidateMaterializedInput(df, input))
					column := df.Column("value").Chunk(0)
					for i, isValid := range valid {
						assert.Equal(t, !isValid, column.IsNull(i))
						if isValid {
							if dataType == schemapb.DataType_Int8 {
								assert.Equal(t, int8(values[i]), column.(*array.Int8).Value(i))
							} else {
								assert.Equal(t, int16(values[i]), column.(*array.Int16).Value(i))
							}
						}
					}
				}()
			}
		})
	}
}

func TestFromSearchResultDataPreservesUnderflowAndSignedZero(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)
	result := jsonProjectorTestResult([]int64{2, 2},
		jsonProjectorTestJSONField(103, "metadata", []string{
			`{"value":1e-400}`, `{"value":-1e-400}`,
			`{"value":0.0}`, `{"value":-0.0}`,
		}, nil))
	plan := &DataFrameInputPlan{Inputs: []ResolvedChainInput{{
		LogicalName: `metadata["value"]`, SourceFieldID: 103, FieldName: "metadata",
		DataType: schemapb.DataType_JSON, NestedPath: []string{"value"}, DataTypeHint: schemapb.DataType_Double,
	}}}
	df, err := FromSearchResultData(result, pool, plan)
	require.NoError(t, err)
	defer df.Release()
	column := df.Column(`metadata["value"]`)
	require.Equal(t, 2, len(column.Chunks()))
	for _, chunk := range column.Chunks() {
		values := chunk.(*array.Float64)
		assert.Zero(t, values.NullN())
		assert.Zero(t, values.Value(0))
		assert.Zero(t, values.Value(1))
		assert.False(t, math.Signbit(values.Value(0)))
		assert.True(t, math.Signbit(values.Value(1)))
	}
}

func TestFromSearchResultDataUsesSingleJSONDecode(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)
	result := jsonProjectorTestResult([]int64{1},
		jsonProjectorTestJSONField(103, "metadata", []string{`{"value":7} {"unused":2}`}, nil))
	plan := &DataFrameInputPlan{Inputs: []ResolvedChainInput{{
		LogicalName: `metadata["value"]`, SourceFieldID: 103, FieldName: "metadata",
		DataType: schemapb.DataType_JSON, NestedPath: []string{"value"}, DataTypeHint: schemapb.DataType_Int64,
	}}}
	df, err := FromSearchResultData(result, pool, plan)
	require.NoError(t, err)
	defer df.Release()
	values := df.Column(`metadata["value"]`).Chunk(0).(*array.Int64)
	assert.Zero(t, values.NullN())
	assert.Equal(t, int64(7), values.Value(0))
}

func TestJSONDecodeRejectsNonJSONNumbers(t *testing.T) {
	// Conversion only receives json.Number values from the decoder; NaN/Inf
	// are rejected here and do not need another check in convertJSONDouble.
	for _, raw := range []string{`{"value":NaN}`, `{"value":Infinity}`, `{"value":-Infinity}`} {
		_, err := decodeJSONDocument([]byte(raw))
		require.Error(t, err, raw)
	}
}
