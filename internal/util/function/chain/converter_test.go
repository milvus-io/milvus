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
	"runtime"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
)

// =============================================================================
// Converter Test Suite
// =============================================================================

type ConverterSuite struct {
	suite.Suite
	pool    *memory.CheckedAllocator
	rawPool *memory.GoAllocator
}

func (s *ConverterSuite) SetupTest() {
	s.rawPool = memory.NewGoAllocator()
	s.pool = memory.NewCheckedAllocator(s.rawPool)
}

func (s *ConverterSuite) TearDownTest() {
	s.pool.AssertSize(s.T(), 0)
}

// =============================================================================
// Type Mapping Tests
// =============================================================================

func (s *ConverterSuite) TestToArrowType() {
	testCases := []struct {
		milvusType schemapb.DataType
		arrowType  arrow.DataType
		expectErr  bool
	}{
		{schemapb.DataType_Bool, arrow.FixedWidthTypes.Boolean, false},
		{schemapb.DataType_Int8, arrow.PrimitiveTypes.Int8, false},
		{schemapb.DataType_Int16, arrow.PrimitiveTypes.Int16, false},
		{schemapb.DataType_Int32, arrow.PrimitiveTypes.Int32, false},
		{schemapb.DataType_Int64, arrow.PrimitiveTypes.Int64, false},
		{schemapb.DataType_Float, arrow.PrimitiveTypes.Float32, false},
		{schemapb.DataType_Double, arrow.PrimitiveTypes.Float64, false},
		{schemapb.DataType_String, arrow.BinaryTypes.String, false},
		{schemapb.DataType_VarChar, arrow.BinaryTypes.String, false},
		{schemapb.DataType_Text, arrow.BinaryTypes.String, false},
		{schemapb.DataType_JSON, nil, true},        // Unsupported
		{schemapb.DataType_FloatVector, nil, true}, // Unsupported
	}

	for _, tc := range testCases {
		result, err := ToArrowType(tc.milvusType)
		if tc.expectErr {
			s.Error(err)
		} else {
			s.NoError(err)
			s.Equal(tc.arrowType.ID(), result.ID())
		}
	}
}

func (s *ConverterSuite) TestToMilvusType() {
	testCases := []struct {
		arrowType  arrow.DataType
		milvusType schemapb.DataType
		expectErr  bool
	}{
		{arrow.FixedWidthTypes.Boolean, schemapb.DataType_Bool, false},
		{arrow.PrimitiveTypes.Int8, schemapb.DataType_Int8, false},
		{arrow.PrimitiveTypes.Int16, schemapb.DataType_Int16, false},
		{arrow.PrimitiveTypes.Int32, schemapb.DataType_Int32, false},
		{arrow.PrimitiveTypes.Int64, schemapb.DataType_Int64, false},
		{arrow.PrimitiveTypes.Float32, schemapb.DataType_Float, false},
		{arrow.PrimitiveTypes.Float64, schemapb.DataType_Double, false},
		{arrow.BinaryTypes.String, schemapb.DataType_VarChar, false},
		{arrow.BinaryTypes.Binary, schemapb.DataType_None, true}, // Unsupported
	}

	for _, tc := range testCases {
		result, err := ToMilvusType(tc.arrowType)
		if tc.expectErr {
			s.Error(err)
		} else {
			s.NoError(err)
			s.Equal(tc.milvusType, result)
		}
	}
}

// =============================================================================
// Import Tests
// =============================================================================

func (s *ConverterSuite) TestFromSearchResultData_Basic() {
	resultData := &schemapb.SearchResultData{
		NumQueries: 2,
		TopK:       3,
		Topks:      []int64{3, 2},
		Scores:     []float32{0.9, 0.8, 0.7, 0.6, 0.5},
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: []int64{1, 2, 3, 4, 5},
				},
			},
		},
		FieldsData: []*schemapb.FieldData{
			{
				Type:      schemapb.DataType_VarChar,
				FieldName: "name",
				FieldId:   100,
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
			{
				Type:      schemapb.DataType_Int64,
				FieldName: "age",
				FieldId:   101,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{
								Data: []int64{20, 30, 40, 50, 60},
							},
						},
					},
				},
			},
		},
	}

	df, err := FromSearchResultData(resultData, s.pool)
	s.Require().NoError(err)
	defer df.Release()

	s.Equal(2, df.NumChunks())
	s.Equal(int64(5), df.NumRows())
	s.Equal([]int64{3, 2}, df.ChunkSizes())

	s.True(df.HasColumn(types.IDFieldName))
	s.True(df.HasColumn(types.ScoreFieldName))
	s.True(df.HasColumn("name"))
	s.True(df.HasColumn("age"))

	idCol := df.Column(types.IDFieldName)
	s.NotNil(idCol)
	s.Equal(3, idCol.Chunk(0).Len())
	s.Equal(2, idCol.Chunk(1).Len())

	scoreCol := df.Column(types.ScoreFieldName)
	s.NotNil(scoreCol)
	s.Equal(3, scoreCol.Chunk(0).Len())
	s.Equal(2, scoreCol.Chunk(1).Len())
}

func (s *ConverterSuite) TestFromSearchResultData_EmptyResult() {
	resultData := &schemapb.SearchResultData{
		NumQueries: 0,
		TopK:       0,
		Topks:      []int64{},
	}

	df, err := FromSearchResultData(resultData, s.pool)
	s.Require().NoError(err)
	defer df.Release()

	s.Equal(0, df.NumChunks())
	s.Equal(int64(0), df.NumRows())
}

func (s *ConverterSuite) TestFromSearchResultData_NilResult() {
	df, err := FromSearchResultData(nil, s.pool)
	s.Error(err)
	s.Nil(df)
	s.Contains(err.Error(), "resultData is nil")
}

func (s *ConverterSuite) TestFromSearchResultData_NilAllocator() {
	resultData := &schemapb.SearchResultData{
		NumQueries: 1,
		TopK:       1,
		Topks:      []int64{1},
	}
	df, err := FromSearchResultData(resultData, nil)
	s.Error(err)
	s.Nil(df)
	s.Contains(err.Error(), "alloc is nil")
}

func (s *ConverterSuite) TestFromSearchResultData_StringIDs() {
	resultData := &schemapb.SearchResultData{
		NumQueries: 1,
		TopK:       2,
		Topks:      []int64{2},
		Scores:     []float32{0.9, 0.8},
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_StrId{
				StrId: &schemapb.StringArray{
					Data: []string{"id1", "id2"},
				},
			},
		},
	}

	df, err := FromSearchResultData(resultData, s.pool)
	s.Require().NoError(err)
	defer df.Release()

	s.True(df.HasColumn(types.IDFieldName))
	idType, ok := df.FieldType(types.IDFieldName)
	s.True(ok)
	s.Equal(schemapb.DataType_VarChar, idType)
}

// =============================================================================
// Export Tests
// =============================================================================

func (s *ConverterSuite) TestToSearchResultData() {
	resultData := &schemapb.SearchResultData{
		NumQueries: 2,
		TopK:       3,
		Topks:      []int64{3, 2},
		Scores:     []float32{0.9, 0.8, 0.7, 0.6, 0.5},
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: []int64{1, 2, 3, 4, 5},
				},
			},
		},
		FieldsData: []*schemapb.FieldData{
			{
				Type:      schemapb.DataType_VarChar,
				FieldName: "name",
				FieldId:   100,
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
	defer df.Release()

	exported, err := ToSearchResultData(df)
	s.Require().NoError(err)

	s.Equal(int64(2), exported.NumQueries)
	s.Equal(int64(3), exported.TopK)
	s.Equal([]int64{3, 2}, exported.Topks)
	s.Equal([]float32{0.9, 0.8, 0.7, 0.6, 0.5}, exported.Scores)
	s.Equal([]int64{1, 2, 3, 4, 5}, exported.Ids.GetIntId().GetData())
	s.Len(exported.FieldsData, 1)
	s.Equal("name", exported.FieldsData[0].FieldName)
	s.Equal([]string{"a", "b", "c", "d", "e"}, exported.FieldsData[0].GetScalars().GetStringData().GetData())
}

func (s *ConverterSuite) TestImportExport_AllDataTypes() {
	resultData := &schemapb.SearchResultData{
		NumQueries: 1,
		TopK:       3,
		Topks:      []int64{3},
		Scores:     []float32{0.9, 0.8, 0.7},
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}},
			},
		},
		FieldsData: []*schemapb.FieldData{
			{
				Type:      schemapb.DataType_Bool,
				FieldName: "bool_col",
				FieldId:   1,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_BoolData{
							BoolData: &schemapb.BoolArray{Data: []bool{true, false, true}},
						},
					},
				},
			},
			{
				Type:      schemapb.DataType_Int8,
				FieldName: "int8_col",
				FieldId:   2,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_IntData{
							IntData: &schemapb.IntArray{Data: []int32{1, 2, 3}},
						},
					},
				},
			},
			{
				Type:      schemapb.DataType_Int16,
				FieldName: "int16_col",
				FieldId:   3,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_IntData{
							IntData: &schemapb.IntArray{Data: []int32{100, 200, 300}},
						},
					},
				},
			},
			{
				Type:      schemapb.DataType_Int32,
				FieldName: "int32_col",
				FieldId:   4,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_IntData{
							IntData: &schemapb.IntArray{Data: []int32{1000, 2000, 3000}},
						},
					},
				},
			},
			{
				Type:      schemapb.DataType_Int64,
				FieldName: "int64_col",
				FieldId:   5,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: []int64{10000, 20000, 30000}},
						},
					},
				},
			},
			{
				Type:      schemapb.DataType_Float,
				FieldName: "float_col",
				FieldId:   6,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_FloatData{
							FloatData: &schemapb.FloatArray{Data: []float32{1.1, 2.2, 3.3}},
						},
					},
				},
			},
			{
				Type:      schemapb.DataType_Double,
				FieldName: "double_col",
				FieldId:   7,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_DoubleData{
							DoubleData: &schemapb.DoubleArray{Data: []float64{1.11, 2.22, 3.33}},
						},
					},
				},
			},
			{
				Type:      schemapb.DataType_VarChar,
				FieldName: "varchar_col",
				FieldId:   8,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{Data: []string{"a", "b", "c"}},
						},
					},
				},
			},
		},
	}

	df, err := FromSearchResultData(resultData, s.pool)
	s.Require().NoError(err)
	defer df.Release()

	s.True(df.HasColumn("bool_col"))
	s.True(df.HasColumn("int8_col"))
	s.True(df.HasColumn("int16_col"))
	s.True(df.HasColumn("int32_col"))
	s.True(df.HasColumn("int64_col"))
	s.True(df.HasColumn("float_col"))
	s.True(df.HasColumn("double_col"))
	s.True(df.HasColumn("varchar_col"))

	exported, err := ToSearchResultData(df)
	s.Require().NoError(err)

	s.Len(exported.FieldsData, 8)
	s.Equal([]float32{0.9, 0.8, 0.7}, exported.Scores)
	s.Equal([]int64{1, 2, 3}, exported.Ids.GetIntId().GetData())
}

func (s *ConverterSuite) TestImportExport_StringIDs() {
	resultData := &schemapb.SearchResultData{
		NumQueries: 1,
		TopK:       2,
		Topks:      []int64{2},
		Scores:     []float32{0.9, 0.8},
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_StrId{
				StrId: &schemapb.StringArray{Data: []string{"id1", "id2"}},
			},
		},
	}

	df, err := FromSearchResultData(resultData, s.pool)
	s.Require().NoError(err)
	defer df.Release()

	exported, err := ToSearchResultData(df)
	s.Require().NoError(err)

	s.Equal([]string{"id1", "id2"}, exported.Ids.GetStrId().GetData())
}

// =============================================================================
// Memory Leak Tests
// =============================================================================

func (s *ConverterSuite) TestMemoryLeak_ImportExport() {
	resultData := &schemapb.SearchResultData{
		NumQueries: 2,
		TopK:       3,
		Topks:      []int64{3, 2},
		Scores:     []float32{0.9, 0.8, 0.7, 0.6, 0.5},
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: []int64{1, 2, 3, 4, 5},
				},
			},
		},
		FieldsData: []*schemapb.FieldData{
			{
				Type:      schemapb.DataType_VarChar,
				FieldName: "name",
				FieldId:   100,
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

	for range 10 {
		df, err := FromSearchResultData(resultData, s.pool)
		s.Require().NoError(err)

		_, err = ToSearchResultData(df)
		s.Require().NoError(err)

		df.Release()
	}
}

// =============================================================================
// Nullable Field Tests
// =============================================================================

func (s *ConverterSuite) TestFromSearchResultData_NullableField_Int64() {
	resultData := &schemapb.SearchResultData{
		NumQueries: 1,
		TopK:       5,
		Topks:      []int64{5},
		Scores:     []float32{0.9, 0.8, 0.7, 0.6, 0.5},
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4, 5}},
			},
		},
		FieldsData: []*schemapb.FieldData{
			{
				Type:      schemapb.DataType_Int64,
				FieldName: "nullable_col",
				FieldId:   100,
				ValidData: []bool{true, false, true, false, true},
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: []int64{10, 0, 30, 0, 50}},
						},
					},
				},
			},
		},
	}

	df, err := FromSearchResultData(resultData, s.pool)
	s.Require().NoError(err)
	defer df.Release()

	s.True(df.fieldNullables["nullable_col"])
	s.False(df.fieldNullables[types.IDFieldName])
	s.False(df.fieldNullables[types.ScoreFieldName])

	col := df.Column("nullable_col")
	s.Require().NotNil(col)
	chunk := col.Chunk(0).(*array.Int64)

	s.True(chunk.IsValid(0))
	s.False(chunk.IsValid(1))
	s.True(chunk.IsValid(2))
	s.False(chunk.IsValid(3))
	s.True(chunk.IsValid(4))

	s.Equal(int64(10), chunk.Value(0))
	s.Equal(int64(30), chunk.Value(2))
	s.Equal(int64(50), chunk.Value(4))
}

func (s *ConverterSuite) TestFromSearchResultData_NullableField_String() {
	resultData := &schemapb.SearchResultData{
		NumQueries: 1,
		TopK:       4,
		Topks:      []int64{4},
		Scores:     []float32{0.9, 0.8, 0.7, 0.6},
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4}},
			},
		},
		FieldsData: []*schemapb.FieldData{
			{
				Type:      schemapb.DataType_VarChar,
				FieldName: "nullable_str",
				FieldId:   100,
				ValidData: []bool{true, true, false, true},
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{Data: []string{"a", "b", "", "d"}},
						},
					},
				},
			},
		},
	}

	df, err := FromSearchResultData(resultData, s.pool)
	s.Require().NoError(err)
	defer df.Release()

	col := df.Column("nullable_str")
	chunk := col.Chunk(0).(*array.String)

	s.True(chunk.IsValid(0))
	s.True(chunk.IsValid(1))
	s.False(chunk.IsValid(2))
	s.True(chunk.IsValid(3))

	s.Equal("a", chunk.Value(0))
	s.Equal("b", chunk.Value(1))
	s.Equal("d", chunk.Value(3))
}

func (s *ConverterSuite) TestFromSearchResultData_NullableField_Int8() {
	resultData := &schemapb.SearchResultData{
		NumQueries: 1,
		TopK:       3,
		Topks:      []int64{3},
		Scores:     []float32{0.9, 0.8, 0.7},
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}},
			},
		},
		FieldsData: []*schemapb.FieldData{
			{
				Type:      schemapb.DataType_Int8,
				FieldName: "nullable_int8",
				FieldId:   100,
				ValidData: []bool{false, true, false},
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_IntData{
							IntData: &schemapb.IntArray{Data: []int32{0, 20, 0}},
						},
					},
				},
			},
		},
	}

	df, err := FromSearchResultData(resultData, s.pool)
	s.Require().NoError(err)
	defer df.Release()

	col := df.Column("nullable_int8")
	chunk := col.Chunk(0).(*array.Int8)

	s.False(chunk.IsValid(0))
	s.True(chunk.IsValid(1))
	s.False(chunk.IsValid(2))

	s.Equal(int8(20), chunk.Value(1))
}

func (s *ConverterSuite) TestFromSearchResultData_NullableField_Int16() {
	resultData := &schemapb.SearchResultData{
		NumQueries: 1,
		TopK:       3,
		Topks:      []int64{3},
		Scores:     []float32{0.9, 0.8, 0.7},
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}},
			},
		},
		FieldsData: []*schemapb.FieldData{
			{
				Type:      schemapb.DataType_Int16,
				FieldName: "nullable_int16",
				FieldId:   100,
				ValidData: []bool{true, false, true},
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_IntData{
							IntData: &schemapb.IntArray{Data: []int32{100, 0, 300}},
						},
					},
				},
			},
		},
	}

	df, err := FromSearchResultData(resultData, s.pool)
	s.Require().NoError(err)
	defer df.Release()

	col := df.Column("nullable_int16")
	chunk := col.Chunk(0).(*array.Int16)

	s.True(chunk.IsValid(0))
	s.False(chunk.IsValid(1))
	s.True(chunk.IsValid(2))

	s.Equal(int16(100), chunk.Value(0))
	s.Equal(int16(300), chunk.Value(2))
}

func (s *ConverterSuite) TestFromSearchResultData_NullableField_MultipleChunks() {
	resultData := &schemapb.SearchResultData{
		NumQueries: 2,
		TopK:       3,
		Topks:      []int64{3, 2},
		Scores:     []float32{0.9, 0.8, 0.7, 0.6, 0.5},
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4, 5}},
			},
		},
		FieldsData: []*schemapb.FieldData{
			{
				Type:      schemapb.DataType_Float,
				FieldName: "nullable_float",
				FieldId:   100,
				ValidData: []bool{true, false, true, false, true},
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_FloatData{
							FloatData: &schemapb.FloatArray{Data: []float32{1.1, 0, 3.3, 0, 5.5}},
						},
					},
				},
			},
		},
	}

	df, err := FromSearchResultData(resultData, s.pool)
	s.Require().NoError(err)
	defer df.Release()

	col := df.Column("nullable_float")

	chunk0 := col.Chunk(0).(*array.Float32)
	s.Equal(3, chunk0.Len())
	s.True(chunk0.IsValid(0))
	s.False(chunk0.IsValid(1))
	s.True(chunk0.IsValid(2))
	s.InDelta(float32(1.1), chunk0.Value(0), 0.01)
	s.InDelta(float32(3.3), chunk0.Value(2), 0.01)

	chunk1 := col.Chunk(1).(*array.Float32)
	s.Equal(2, chunk1.Len())
	s.False(chunk1.IsValid(0))
	s.True(chunk1.IsValid(1))
	s.InDelta(float32(5.5), chunk1.Value(1), 0.01)
}

func (s *ConverterSuite) TestFromSearchResultData_NullableField_AllTypes() {
	resultData := &schemapb.SearchResultData{
		NumQueries: 1,
		TopK:       3,
		Topks:      []int64{3},
		Scores:     []float32{0.9, 0.8, 0.7},
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}},
			},
		},
		FieldsData: []*schemapb.FieldData{
			{
				Type:      schemapb.DataType_Bool,
				FieldName: "nullable_bool",
				FieldId:   1,
				ValidData: []bool{true, false, true},
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_BoolData{
							BoolData: &schemapb.BoolArray{Data: []bool{true, false, false}},
						},
					},
				},
			},
			{
				Type:      schemapb.DataType_Int32,
				FieldName: "nullable_int32",
				FieldId:   2,
				ValidData: []bool{false, true, true},
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_IntData{
							IntData: &schemapb.IntArray{Data: []int32{0, 200, 300}},
						},
					},
				},
			},
			{
				Type:      schemapb.DataType_Double,
				FieldName: "nullable_double",
				FieldId:   3,
				ValidData: []bool{true, true, false},
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_DoubleData{
							DoubleData: &schemapb.DoubleArray{Data: []float64{1.11, 2.22, 0}},
						},
					},
				},
			},
		},
	}

	df, err := FromSearchResultData(resultData, s.pool)
	s.Require().NoError(err)
	defer df.Release()

	s.True(df.fieldNullables["nullable_bool"])
	s.True(df.fieldNullables["nullable_int32"])
	s.True(df.fieldNullables["nullable_double"])

	boolCol := df.Column("nullable_bool").Chunk(0).(*array.Boolean)
	s.True(boolCol.IsValid(0))
	s.False(boolCol.IsValid(1))
	s.True(boolCol.IsValid(2))

	int32Col := df.Column("nullable_int32").Chunk(0).(*array.Int32)
	s.False(int32Col.IsValid(0))
	s.True(int32Col.IsValid(1))
	s.True(int32Col.IsValid(2))
	s.Equal(int32(200), int32Col.Value(1))
	s.Equal(int32(300), int32Col.Value(2))

	doubleCol := df.Column("nullable_double").Chunk(0).(*array.Float64)
	s.True(doubleCol.IsValid(0))
	s.True(doubleCol.IsValid(1))
	s.False(doubleCol.IsValid(2))
	s.InDelta(1.11, doubleCol.Value(0), 0.001)
	s.InDelta(2.22, doubleCol.Value(1), 0.001)
}

func (s *ConverterSuite) TestFromSearchResultData_NonNullableField() {
	resultData := &schemapb.SearchResultData{
		NumQueries: 1,
		TopK:       3,
		Topks:      []int64{3},
		Scores:     []float32{0.9, 0.8, 0.7},
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}},
			},
		},
		FieldsData: []*schemapb.FieldData{
			{
				Type:      schemapb.DataType_Int64,
				FieldName: "non_nullable_col",
				FieldId:   100,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: []int64{10, 20, 30}},
						},
					},
				},
			},
		},
	}

	df, err := FromSearchResultData(resultData, s.pool)
	s.Require().NoError(err)
	defer df.Release()

	s.False(df.fieldNullables["non_nullable_col"])

	col := df.Column("non_nullable_col")
	chunk := col.Chunk(0).(*array.Int64)
	for i := 0; i < chunk.Len(); i++ {
		s.True(chunk.IsValid(i))
	}
}

func TestConverterSuite(t *testing.T) {
	suite.Run(t, new(ConverterSuite))
}

// =============================================================================
// Standalone Tests (non-suite based)
// =============================================================================

func TestFromSearchResultData_MemoryLeakOnError(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	resultData := &schemapb.SearchResultData{
		NumQueries: 2,
		TopK:       3,
		Topks:      []int64{3, 2},
		Scores:     []float32{0.9, 0.8, 0.7, 0.6, 0.5},
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: []int64{1, 2, 3, 4, 5},
				},
			},
		},
		FieldsData: []*schemapb.FieldData{
			{
				Type:      schemapb.DataType_VarChar,
				FieldName: "name",
				FieldId:   100,
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
			{
				Type:      schemapb.DataType_FloatVector, // Unsupported!
				FieldName: "vector",
				FieldId:   101,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Dim: 4,
						Data: &schemapb.VectorField_FloatVector{
							FloatVector: &schemapb.FloatArray{
								Data: make([]float32, 20),
							},
						},
					},
				},
			},
		},
	}

	df, err := FromSearchResultData(resultData, pool)
	assert.Error(t, err)
	assert.Nil(t, df)
	assert.Contains(t, err.Error(), "unsupported")
}

func TestFromSearchResultData_MemoryLeakOnError_WithCheckedAllocator(t *testing.T) {
	rawPool := memory.NewGoAllocator()
	checkedPool := memory.NewCheckedAllocator(rawPool)

	resultData := &schemapb.SearchResultData{
		NumQueries: 2,
		TopK:       3,
		Topks:      []int64{3, 2},
		Scores:     []float32{0.9, 0.8, 0.7, 0.6, 0.5},
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
				FieldName: "col1",
				FieldId:   100,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{
								Data: []int64{10, 20, 30, 40, 50},
							},
						},
					},
				},
			},
			{
				Type:      schemapb.DataType_JSON, // Unsupported!
				FieldName: "json_col",
				FieldId:   101,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_JsonData{
							JsonData: &schemapb.JSONArray{
								Data: [][]byte{[]byte(`{}`), []byte(`{}`), []byte(`{}`), []byte(`{}`), []byte(`{}`)},
							},
						},
					},
				},
			},
		},
	}

	df, err := FromSearchResultData(resultData, checkedPool)
	assert.Error(t, err)
	assert.Nil(t, df)

	runtime.GC()
	checkedPool.AssertSize(t, 0)
}

func TestFromSearchResultData_NoLeakOnSuccess(t *testing.T) {
	rawPool := memory.NewGoAllocator()
	checkedPool := memory.NewCheckedAllocator(rawPool)

	resultData := &schemapb.SearchResultData{
		NumQueries: 2,
		TopK:       3,
		Topks:      []int64{3, 2},
		Scores:     []float32{0.9, 0.8, 0.7, 0.6, 0.5},
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: []int64{1, 2, 3, 4, 5},
				},
			},
		},
		FieldsData: []*schemapb.FieldData{
			{
				Type:      schemapb.DataType_VarChar,
				FieldName: "name",
				FieldId:   100,
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

	for i := 0; i < 10; i++ {
		df, err := FromSearchResultData(resultData, checkedPool)
		require.NoError(t, err)
		require.NotNil(t, df)
		df.Release()
	}

	runtime.GC()
	checkedPool.AssertSize(t, 0)
}

func TestFromSearchResultData_MemoryLeakQuantification(t *testing.T) {
	rawPool := memory.NewGoAllocator()
	checkedPool := memory.NewCheckedAllocator(rawPool)

	resultData := &schemapb.SearchResultData{
		NumQueries: 1,
		TopK:       100,
		Topks:      []int64{100},
		Scores:     make([]float32, 100),
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: make([]int64, 100),
				},
			},
		},
		FieldsData: []*schemapb.FieldData{
			{
				Type:      schemapb.DataType_Double,
				FieldName: "large_col",
				FieldId:   100,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_DoubleData{
							DoubleData: &schemapb.DoubleArray{
								Data: make([]float64, 100),
							},
						},
					},
				},
			},
			{
				Type:      schemapb.DataType_FloatVector,
				FieldName: "vector",
				FieldId:   101,
				Field:     nil,
			},
		},
	}

	for i := 0; i < 100; i++ {
		resultData.Scores[i] = float32(i)
		resultData.Ids.GetIntId().Data[i] = int64(i)
	}

	iterations := 100
	for i := 0; i < iterations; i++ {
		df, err := FromSearchResultData(resultData, checkedPool)
		assert.Error(t, err)
		assert.Nil(t, df)
	}

	runtime.GC()
	checkedPool.AssertSize(t, 0)
}

func TestMemoryLeakStress(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	resultData := &schemapb.SearchResultData{
		NumQueries: 5,
		TopK:       10,
		Topks:      []int64{10, 10, 10, 10, 10},
		Scores:     make([]float32, 50),
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: make([]int64, 50),
				},
			},
		},
		FieldsData: []*schemapb.FieldData{
			{
				Type:      schemapb.DataType_Int64,
				FieldName: "col1",
				FieldId:   1,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{
								Data: make([]int64, 50),
							},
						},
					},
				},
			},
			{
				Type:      schemapb.DataType_Double,
				FieldName: "col2",
				FieldId:   2,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_DoubleData{
							DoubleData: &schemapb.DoubleArray{
								Data: make([]float64, 50),
							},
						},
					},
				},
			},
			{
				Type:      schemapb.DataType_VarChar,
				FieldName: "col3",
				FieldId:   3,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{
								Data: make([]string, 50),
							},
						},
					},
				},
			},
		},
	}

	for i := range resultData.Scores {
		resultData.Scores[i] = float32(i) * 0.1
	}
	for i := range resultData.Ids.GetIntId().Data {
		resultData.Ids.GetIntId().Data[i] = int64(i)
	}

	for range 100 {
		df, err := FromSearchResultData(resultData, pool)
		assert.NoError(t, err)

		_ = df.Column(types.IDFieldName)
		_ = df.Column(types.ScoreFieldName)

		_, err = ToSearchResultData(df)
		assert.NoError(t, err)

		df.Release()
	}
}

func TestSchema(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	resultData := &schemapb.SearchResultData{
		NumQueries: 1,
		TopK:       2,
		Topks:      []int64{2},
		Scores:     []float32{0.9, 0.8},
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: []int64{1, 2},
				},
			},
		},
	}

	df, err := FromSearchResultData(resultData, pool)
	assert.NoError(t, err)
	defer df.Release()

	schema := df.Schema()
	assert.NotNil(t, schema)
	assert.Equal(t, 2, schema.NumFields()) // $id and $score
}

// =============================================================================
// Benchmark
// =============================================================================

func BenchmarkFromSearchResultData_ErrorPath(b *testing.B) {
	pool := memory.NewGoAllocator()
	resultData := &schemapb.SearchResultData{
		NumQueries: 2,
		TopK:       10,
		Topks:      []int64{10, 10},
		Scores:     make([]float32, 20),
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: make([]int64, 20),
				},
			},
		},
		FieldsData: []*schemapb.FieldData{
			{
				Type:      schemapb.DataType_Int64,
				FieldName: "col1",
				FieldId:   100,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{
								Data: make([]int64, 20),
							},
						},
					},
				},
			},
			{
				Type:      schemapb.DataType_FloatVector,
				FieldName: "vector",
				FieldId:   101,
				Field:     nil,
			},
		},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		df, _ := FromSearchResultData(resultData, pool)
		if df != nil {
			df.Release()
		}
	}
}
