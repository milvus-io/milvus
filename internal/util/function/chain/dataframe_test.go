/*
 * # Licensed to the LF AI & Data foundation under one
 * # or more contributor license agreements. See the NOTICE file
 * # distributed with this work for additional information
 * # regarding copyright ownership. The ASF licenses this file
 * # to you under the Apache License, Version 2.0 (the
 * # "License"); you may not use this file except in compliance
 * # with the License. You may obtain a copy of the License at
 * #
 * #     http://www.apache.org/licenses/LICENSE-2.0
 * #
 * # Unless required by applicable law or agreed to in writing, software
 * # distributed under the License is distributed on an "AS IS" BASIS,
 * # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * # See the License for the specific language governing permissions and
 * # limitations under the License.
 */

package chain

import (
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

// =============================================================================
// DataFrame Test Suite
// =============================================================================

type DataFrameSuite struct {
	suite.Suite
	pool    *memory.CheckedAllocator
	rawPool *memory.GoAllocator
}

func (s *DataFrameSuite) SetupTest() {
	s.rawPool = memory.NewGoAllocator()
	s.pool = memory.NewCheckedAllocator(s.rawPool)
}

func (s *DataFrameSuite) TearDownTest() {
	s.pool.AssertSize(s.T(), 0)
}

// =============================================================================
// Column Access Tests
// =============================================================================

func (s *DataFrameSuite) TestColumnAccess() {
	df := s.createTestDataFrame()
	defer df.Release()

	col := df.Column("int_col")
	s.NotNil(col)

	col = df.Column("nonexistent")
	s.Nil(col)

	s.True(df.HasColumn("int_col"))
	s.False(df.HasColumn("nonexistent"))

	dt, ok := df.FieldType("int_col")
	s.True(ok)
	s.Equal(schemapb.DataType_Int64, dt)

	id, ok := df.FieldID("int_col")
	s.True(ok)
	s.Equal(int64(1), id)

	names := df.ColumnNames()
	s.Len(names, 3) // $id, int_col, str_col
}

func (s *DataFrameSuite) TestColumnNames_NilSchema() {
	builder := NewDataFrameBuilder()
	df := builder.Build()
	defer df.Release()

	names := df.ColumnNames()
	s.Nil(names)
}

// =============================================================================
// DataFrameBuilder Tests
// =============================================================================

func (s *DataFrameSuite) TestDataFrameBuilder_Basic() {
	builder := NewDataFrameBuilder()
	defer builder.Release()

	builder.SetChunkSizes([]int64{3, 2})

	b := array.NewInt64Builder(s.pool)
	b.AppendValues([]int64{1, 2, 3}, nil)
	arr1 := b.NewArray()
	b.AppendValues([]int64{4, 5}, nil)
	arr2 := b.NewArray()
	b.Release()

	err := builder.AddColumnFromChunks("col1", []arrow.Array{arr1, arr2})
	s.Require().NoError(err)

	df := builder.Build()
	s.NotNil(df)
	defer df.Release()

	s.Equal(2, df.NumChunks())
	s.Equal(int64(5), df.NumRows())
	s.True(df.HasColumn("col1"))
}

func (s *DataFrameSuite) TestDataFrameBuilder_AddColumns_Success() {
	builder := NewDataFrameBuilder()
	defer builder.Release()

	builder.SetChunkSizes([]int64{2})

	b1 := array.NewInt64Builder(s.pool)
	b1.AppendValues([]int64{1, 2}, nil)
	arr1 := b1.NewArray()
	b1.Release()
	chunked1 := arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{arr1})
	arr1.Release()

	b2 := array.NewStringBuilder(s.pool)
	b2.AppendValues([]string{"a", "b"}, nil)
	arr2 := b2.NewArray()
	b2.Release()
	chunked2 := arrow.NewChunked(arrow.BinaryTypes.String, []arrow.Array{arr2})
	arr2.Release()

	err := builder.AddColumns([]string{"col1", "col2"}, []*arrow.Chunked{chunked1, chunked2})
	s.Require().NoError(err)

	df := builder.Build()
	s.Equal(2, df.NumColumns())
	s.True(df.HasColumn("col1"))
	s.True(df.HasColumn("col2"))
	df.Release()
}

func (s *DataFrameSuite) TestDataFrameBuilder_AddColumns_DuplicateName() {
	builder := NewDataFrameBuilder()
	defer builder.Release()

	builder.SetChunkSizes([]int64{2})

	b0 := array.NewInt64Builder(s.pool)
	b0.AppendValues([]int64{0, 0}, nil)
	arr0 := b0.NewArray()
	b0.Release()
	chunked0 := arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{arr0})
	arr0.Release()
	err := builder.AddColumns([]string{"existing"}, []*arrow.Chunked{chunked0})
	s.Require().NoError(err)

	b1 := array.NewInt64Builder(s.pool)
	b1.AppendValues([]int64{1, 2}, nil)
	arr1 := b1.NewArray()
	b1.Release()
	chunked1 := arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{arr1})
	arr1.Release()

	b2 := array.NewInt64Builder(s.pool)
	b2.AppendValues([]int64{3, 4}, nil)
	arr2 := b2.NewArray()
	b2.Release()
	chunked2 := arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{arr2})
	arr2.Release()

	err = builder.AddColumns([]string{"new", "existing"}, []*arrow.Chunked{chunked1, chunked2})
	s.Error(err)
	s.Contains(err.Error(), "already exists")
}

func (s *DataFrameSuite) TestDataFrameBuilder_AddColumns_NilColumn() {
	builder := NewDataFrameBuilder()
	defer builder.Release()

	builder.SetChunkSizes([]int64{2})

	b1 := array.NewInt64Builder(s.pool)
	b1.AppendValues([]int64{1, 2}, nil)
	arr1 := b1.NewArray()
	b1.Release()
	chunked1 := arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{arr1})
	arr1.Release()

	err := builder.AddColumns([]string{"col1", "col2"}, []*arrow.Chunked{chunked1, nil})
	s.Error(err)
	s.Contains(err.Error(), "nil")
}

func (s *DataFrameSuite) TestDataFrameBuilder_AddColumns_LengthMismatch() {
	builder := NewDataFrameBuilder()
	defer builder.Release()

	b1 := array.NewInt64Builder(s.pool)
	b1.AppendValues([]int64{1, 2}, nil)
	arr1 := b1.NewArray()
	b1.Release()
	chunked1 := arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{arr1})
	arr1.Release()

	err := builder.AddColumns([]string{"col1", "col2"}, []*arrow.Chunked{chunked1})
	s.Error(err)
	s.Contains(err.Error(), "count")
}

func (s *DataFrameSuite) TestDataFrameBuilder_SetFieldNullable() {
	builder := NewDataFrameBuilder()
	defer builder.Release()

	builder.SetFieldNullable("col1", true)
	builder.SetFieldNullable("col2", false)

	df := builder.Build()
	defer df.Release()

	s.True(df.fieldNullables["col1"])
	s.False(df.fieldNullables["col2"])
	s.False(df.fieldNullables["col3"])
}

func (s *DataFrameSuite) TestCopyFieldMetadata_IncludesNullable() {
	resultData := &schemapb.SearchResultData{
		NumQueries: 1,
		TopK:       2,
		Topks:      []int64{2},
		Scores:     []float32{0.9, 0.8},
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{1, 2}},
			},
		},
		FieldsData: []*schemapb.FieldData{
			{
				Type:      schemapb.DataType_Int64,
				FieldName: "nullable_col",
				FieldId:   100,
				ValidData: []bool{true, false},
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: []int64{10, 0}},
						},
					},
				},
			},
		},
	}

	source, err := FromSearchResultData(resultData, s.pool)
	s.Require().NoError(err)
	defer source.Release()

	builder := NewDataFrameBuilder()
	defer builder.Release()
	builder.SetChunkSizes(source.ChunkSizes())

	err = builder.AddColumnFrom(source, "nullable_col")
	s.Require().NoError(err)

	df := builder.Build()
	defer df.Release()

	s.True(df.fieldNullables["nullable_col"])

	ft, ok := df.FieldType("nullable_col")
	s.True(ok)
	s.Equal(schemapb.DataType_Int64, ft)

	fid, ok := df.FieldID("nullable_col")
	s.True(ok)
	s.Equal(int64(100), fid)
}

// =============================================================================
// Helper Functions
// =============================================================================

func (s *DataFrameSuite) createTestDataFrame() *DataFrame {
	resultData := &schemapb.SearchResultData{
		NumQueries: 2,
		TopK:       3,
		Topks:      []int64{3, 2},
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: []int64{1, 2, 3, 4, 5},
				},
			},
		},
		FieldsData: []*schemapb.FieldData{
			{
				Type:      schemapb.DataType_Int64,
				FieldName: "int_col",
				FieldId:   1,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{
								Data: []int64{1, 2, 3, 4, 5},
							},
						},
					},
				},
			},
			{
				Type:      schemapb.DataType_VarChar,
				FieldName: "str_col",
				FieldId:   2,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{
								Data: []string{"a", "b", "c", "d", "e"},
							},
						},
					},
				},
			},
		},
	}

	df, err := FromSearchResultData(resultData, s.pool)
	s.Require().NoError(err)
	return df
}

func TestDataFrameSuite(t *testing.T) {
	suite.Run(t, new(DataFrameSuite))
}

// =============================================================================
// Standalone Tests (non-suite based)
// =============================================================================

func TestNewDataFrameBuilder(t *testing.T) {
	builder := NewDataFrameBuilder()
	df := builder.Build()
	assert.NotNil(t, df)
	assert.Equal(t, 0, df.NumChunks())
	assert.Equal(t, int64(0), df.NumRows())
	assert.Equal(t, 0, df.NumColumns())
	df.Release()
}

func TestDataFrameRelease(t *testing.T) {
	builder := NewDataFrameBuilder()
	df := builder.Build()
	df.Release()
	assert.Nil(t, df.columns)
	assert.Nil(t, df.schema)
}

func TestFieldType(t *testing.T) {
	builder := NewDataFrameBuilder()
	builder.SetFieldType("test_field", schemapb.DataType_Int64)
	df := builder.Build()
	defer df.Release()

	dt, ok := df.FieldType("test_field")
	assert.True(t, ok)
	assert.Equal(t, schemapb.DataType_Int64, dt)

	_, ok = df.FieldType("nonexistent")
	assert.False(t, ok)
}

func TestFieldID(t *testing.T) {
	builder := NewDataFrameBuilder()
	builder.SetFieldID("test_field", 123)
	df := builder.Build()
	defer df.Release()

	id, ok := df.FieldID("test_field")
	assert.True(t, ok)
	assert.Equal(t, int64(123), id)

	_, ok = df.FieldID("nonexistent")
	assert.False(t, ok)
}
