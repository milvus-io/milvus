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

package queryutil

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
)

// makeNullableSchema builds a minimal CollectionSchema marking the given
// fieldID as nullable with the specified DataType. For dense vectors, pass
// dim > 0; for sparse or scalars, pass dim = 0.
func makeNullableSchema(fieldID int64, dataType schemapb.DataType, dim int64) *schemapb.CollectionSchema {
	fs := &schemapb.FieldSchema{
		FieldID:  fieldID,
		DataType: dataType,
		Nullable: true,
	}
	if dim > 0 {
		fs.TypeParams = []*commonpb.KeyValuePair{
			{Key: "dim", Value: fmt.Sprintf("%d", dim)},
		}
	}
	return &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{fs}}
}

// =========================================================================
// buildMergedFieldData: ValidData preservation
// =========================================================================

func TestBuildMergedFieldData_ValidData(t *testing.T) {
	// Two results with nullable int64 fields
	r1 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2}}}},
		FieldsData: []*schemapb.FieldData{
			{
				Type: schemapb.DataType_Int64, FieldId: 100,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{10, 20}}},
				}},
				ValidData: []bool{true, false}, // row 0 valid, row 1 null
			},
		},
	}
	r2 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{3}}}},
		FieldsData: []*schemapb.FieldData{
			{
				Type: schemapb.DataType_Int64, FieldId: 100,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{30}}},
				}},
				ValidData: []bool{true},
			},
		},
	}

	results := []*internalpb.RetrieveResults{r1, r2}
	selectedRows := []rowRef{
		{resultIdx: 0, rowIdx: 0}, // r1 row 0 (valid)
		{resultIdx: 0, rowIdx: 1}, // r1 row 1 (null)
		{resultIdx: 1, rowIdx: 0}, // r2 row 0 (valid)
	}

	merged, err := buildMergedRetrieveResults(results, selectedRows, makeNullableSchema(100, schemapb.DataType_Int64, 0))
	require.NoError(t, err)
	require.Len(t, merged.FieldsData, 1)

	fd := merged.FieldsData[0]
	assert.Equal(t, []bool{true, false, true}, fd.ValidData)
	assert.Equal(t, []int64{10, 20, 30}, fd.GetScalars().GetLongData().GetData())
}

func TestBuildMergedFieldData_ValidData_MixedNullable(t *testing.T) {
	// r1 has ValidData (nullable), r2 has explicit ValidData=[true] (all valid)
	// This tests that explicit all-valid ValidData is preserved correctly.
	r1 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}},
		FieldsData: []*schemapb.FieldData{
			{
				Type: schemapb.DataType_Int64, FieldId: 100,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{10}}},
				}},
				ValidData: []bool{false}, // null
			},
		},
	}
	r2 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{2}}}},
		FieldsData: []*schemapb.FieldData{
			{
				Type: schemapb.DataType_Int64, FieldId: 100,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{20}}},
				}},
				ValidData: []bool{true}, // explicit valid (segcore always includes ValidData for nullable fields)
			},
		},
	}

	results := []*internalpb.RetrieveResults{r1, r2}
	selectedRows := []rowRef{
		{resultIdx: 0, rowIdx: 0}, // r1 (null)
		{resultIdx: 1, rowIdx: 0}, // r2 (valid=true)
	}

	merged, err := buildMergedRetrieveResults(results, selectedRows, makeNullableSchema(100, schemapb.DataType_Int64, 0))
	require.NoError(t, err)

	fd := merged.FieldsData[0]
	assert.Equal(t, []bool{false, true}, fd.ValidData)
}

// =========================================================================
// buildMergedScalarField: ArrayData ElementType preservation
// =========================================================================

func TestBuildMergedScalarField_ArrayElementType(t *testing.T) {
	r1 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2}}}},
		FieldsData: []*schemapb.FieldData{
			{
				Type: schemapb.DataType_Array, FieldId: 200,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
						Data: []*schemapb.ScalarField{
							{Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: []float32{1.1, 2.2}}}},
							{Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: []float32{3.3}}}},
						},
						ElementType: schemapb.DataType_Float,
					}},
				}},
			},
		},
	}

	results := []*internalpb.RetrieveResults{r1}
	selectedRows := []rowRef{{resultIdx: 0, rowIdx: 0}, {resultIdx: 0, rowIdx: 1}}

	merged, err := buildMergedRetrieveResults(results, selectedRows, nil)
	require.NoError(t, err)

	arrayField := merged.FieldsData[0].GetScalars().GetArrayData()
	assert.Equal(t, schemapb.DataType_Float, arrayField.GetElementType())
	assert.Len(t, arrayField.GetData(), 2)
}

// =========================================================================
// buildMergedScalarField: Geometry types
// =========================================================================

func TestBuildMergedScalarField_GeometryWktData(t *testing.T) {
	r1 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2}}}},
		FieldsData: []*schemapb.FieldData{
			{
				Type: schemapb.DataType_Geometry, FieldId: 300,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_GeometryWktData{GeometryWktData: &schemapb.GeometryWktArray{
						Data: []string{"POINT(0 0)", "POINT(1 1)"},
					}},
				}},
			},
		},
	}

	results := []*internalpb.RetrieveResults{r1}
	selectedRows := []rowRef{{resultIdx: 0, rowIdx: 1}}

	merged, err := buildMergedRetrieveResults(results, selectedRows, nil)
	require.NoError(t, err)

	wktData := merged.FieldsData[0].GetScalars().GetGeometryWktData().GetData()
	assert.Equal(t, []string{"POINT(1 1)"}, wktData)
}

func TestBuildMergedScalarField_GeometryData(t *testing.T) {
	r1 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}},
		FieldsData: []*schemapb.FieldData{
			{
				Type: schemapb.DataType_Geometry, FieldId: 300,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_GeometryData{GeometryData: &schemapb.GeometryArray{
						Data: [][]byte{{0x01, 0x02}, {0x03, 0x04}},
					}},
				}},
			},
		},
	}

	results := []*internalpb.RetrieveResults{r1}
	selectedRows := []rowRef{{resultIdx: 0, rowIdx: 0}}

	merged, err := buildMergedRetrieveResults(results, selectedRows, nil)
	require.NoError(t, err)

	geoData := merged.FieldsData[0].GetScalars().GetGeometryData().GetData()
	assert.Equal(t, [][]byte{{0x01, 0x02}}, geoData)
}

// =========================================================================
// buildMergedScalarField: Timestamptz and Mol types
// =========================================================================

func TestBuildMergedScalarField_TimestamptzData(t *testing.T) {
	r1 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2}}}},
		FieldsData: []*schemapb.FieldData{
			{
				Type: schemapb.DataType_Timestamptz, FieldId: 400,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_TimestamptzData{TimestamptzData: &schemapb.TimestamptzArray{
						Data: []int64{1000, 2000},
					}},
				}},
			},
		},
	}

	results := []*internalpb.RetrieveResults{r1}
	selectedRows := []rowRef{{resultIdx: 0, rowIdx: 0}, {resultIdx: 0, rowIdx: 1}}

	merged, err := buildMergedRetrieveResults(results, selectedRows, nil)
	require.NoError(t, err)

	tsData := merged.FieldsData[0].GetScalars().GetTimestamptzData().GetData()
	assert.Equal(t, []int64{1000, 2000}, tsData)
}

func TestBuildMergedScalarField_MolData(t *testing.T) {
	r1 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}},
		FieldsData: []*schemapb.FieldData{
			{
				Type: schemapb.DataType_Mol, FieldId: 500,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_MolData{MolData: &schemapb.MolArray{
						Data: [][]byte{{0xAA, 0xBB}},
					}},
				}},
			},
		},
	}

	results := []*internalpb.RetrieveResults{r1}
	selectedRows := []rowRef{{resultIdx: 0, rowIdx: 0}}

	merged, err := buildMergedRetrieveResults(results, selectedRows, nil)
	require.NoError(t, err)

	molData := merged.FieldsData[0].GetScalars().GetMolData().GetData()
	assert.Equal(t, [][]byte{{0xAA, 0xBB}}, molData)
}

// =========================================================================
// buildMergedVectorField: Int8Vector
// =========================================================================

func TestBuildMergedVectorField_Int8Vector(t *testing.T) {
	dim := 4
	r1 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2}}}},
		FieldsData: []*schemapb.FieldData{
			{
				Type: schemapb.DataType_Int8Vector, FieldId: 600,
				Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
					Dim:  int64(dim),
					Data: &schemapb.VectorField_Int8Vector{Int8Vector: []byte{1, 2, 3, 4, 5, 6, 7, 8}},
				}},
			},
		},
	}

	results := []*internalpb.RetrieveResults{r1}
	selectedRows := []rowRef{{resultIdx: 0, rowIdx: 1}} // select row 1 only

	merged, err := buildMergedRetrieveResults(results, selectedRows, nil)
	require.NoError(t, err)

	vecData := merged.FieldsData[0].GetVectors().GetInt8Vector()
	assert.Equal(t, []byte{5, 6, 7, 8}, vecData)
}

// =========================================================================
// rangeSliceScalarField: new types
// =========================================================================

func TestRangeSliceScalarField_ArrayElementType(t *testing.T) {
	fd := &schemapb.FieldData{
		Type: schemapb.DataType_Array, FieldId: 200,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
				Data: []*schemapb.ScalarField{
					{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{1}}}},
					{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{2}}}},
					{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{3}}}},
				},
				ElementType: schemapb.DataType_Int32,
			}},
		}},
	}

	sliced := rangeSliceFieldData(fd, 1, 3)
	arrayField := sliced.GetScalars().GetArrayData()
	assert.Equal(t, schemapb.DataType_Int32, arrayField.GetElementType())
	assert.Len(t, arrayField.GetData(), 2)
}

func TestRangeSliceScalarField_GeometryWkt(t *testing.T) {
	fd := &schemapb.FieldData{
		Type: schemapb.DataType_Geometry, FieldId: 300,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_GeometryWktData{GeometryWktData: &schemapb.GeometryWktArray{
				Data: []string{"POINT(0 0)", "POINT(1 1)", "POINT(2 2)"},
			}},
		}},
	}

	sliced := rangeSliceFieldData(fd, 1, 3)
	assert.Equal(t, []string{"POINT(1 1)", "POINT(2 2)"}, sliced.GetScalars().GetGeometryWktData().GetData())
}

func TestRangeSliceScalarField_Timestamptz(t *testing.T) {
	fd := &schemapb.FieldData{
		Type: schemapb.DataType_Timestamptz, FieldId: 400,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_TimestamptzData{TimestamptzData: &schemapb.TimestamptzArray{
				Data: []int64{100, 200, 300},
			}},
		}},
	}

	sliced := rangeSliceFieldData(fd, 0, 2)
	assert.Equal(t, []int64{100, 200}, sliced.GetScalars().GetTimestamptzData().GetData())
}

// =========================================================================
// sliceScalarField (index-based): new types
// =========================================================================

func TestSliceScalarField_ArrayElementType(t *testing.T) {
	fd := &schemapb.FieldData{
		Type: schemapb.DataType_Array, FieldId: 200,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
				Data: []*schemapb.ScalarField{
					{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{10}}}},
					{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{20}}}},
					{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{30}}}},
				},
				ElementType: schemapb.DataType_Int64,
			}},
		}},
	}

	sliced := sliceFieldData(fd, []int{2, 0})
	arrayField := sliced.GetScalars().GetArrayData()
	assert.Equal(t, schemapb.DataType_Int64, arrayField.GetElementType())
	assert.Len(t, arrayField.GetData(), 2)
	assert.Equal(t, []int64{30}, arrayField.GetData()[0].GetLongData().GetData())
	assert.Equal(t, []int64{10}, arrayField.GetData()[1].GetLongData().GetData())
}

func TestSliceScalarField_GeometryWkt(t *testing.T) {
	fd := &schemapb.FieldData{
		Type: schemapb.DataType_Geometry, FieldId: 300,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_GeometryWktData{GeometryWktData: &schemapb.GeometryWktArray{
				Data: []string{"POINT(0 0)", "POINT(1 1)", "POINT(2 2)"},
			}},
		}},
	}

	sliced := sliceFieldData(fd, []int{2, 0})
	assert.Equal(t, []string{"POINT(2 2)", "POINT(0 0)"}, sliced.GetScalars().GetGeometryWktData().GetData())
}

// =========================================================================
// calcFieldElementSize: new types
// =========================================================================

func TestCalcFieldElementSize_Geometry(t *testing.T) {
	fd := &schemapb.FieldData{
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_GeometryWktData{GeometryWktData: &schemapb.GeometryWktArray{
				Data: []string{"POINT(0 0)", "LINESTRING(0 0, 1 1)"},
			}},
		}},
	}
	assert.Equal(t, int64(len("POINT(0 0)")), calcFieldElementSize(fd, 0))
	assert.Equal(t, int64(len("LINESTRING(0 0, 1 1)")), calcFieldElementSize(fd, 1))
}

func TestCalcFieldElementSize_Timestamptz(t *testing.T) {
	fd := &schemapb.FieldData{
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_TimestamptzData{TimestamptzData: &schemapb.TimestamptzArray{
				Data: []int64{1000},
			}},
		}},
	}
	assert.Equal(t, int64(8), calcFieldElementSize(fd, 0))
}

func TestCalcFieldElementSize_Int8Vector(t *testing.T) {
	fd := &schemapb.FieldData{
		Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
			Dim:  128,
			Data: &schemapb.VectorField_Int8Vector{Int8Vector: make([]byte, 256)},
		}},
	}
	assert.Equal(t, int64(128), calcFieldElementSize(fd, 0))
}

// =========================================================================
// ElementIndices propagation through merge
// =========================================================================

func TestBuildMergedRetrieveResults_ElementIndices(t *testing.T) {
	r1 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2}}}},
		FieldsData: []*schemapb.FieldData{
			{
				Type: schemapb.DataType_Int64, FieldId: 100,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{10, 20}}},
				}},
			},
		},
		ElementLevel:   true,
		ElementIndices: []*internalpb.ElementIndices{{Indices: []int32{0, 1}}, {Indices: []int32{2}}},
	}

	results := []*internalpb.RetrieveResults{r1}
	selectedRows := []rowRef{{resultIdx: 0, rowIdx: 1}, {resultIdx: 0, rowIdx: 0}}

	merged, err := buildMergedRetrieveResults(results, selectedRows, nil)
	require.NoError(t, err)

	assert.True(t, merged.GetElementLevel())
	require.Len(t, merged.GetElementIndices(), 2)
	assert.Equal(t, []int32{2}, merged.GetElementIndices()[0].GetIndices())
	assert.Equal(t, []int32{0, 1}, merged.GetElementIndices()[1].GetIndices())
}

// =========================================================================
// rangeSliceRetrieveResults: ElementIndices propagation
// =========================================================================

func TestRangeSliceRetrieveResults_ElementIndices(t *testing.T) {
	result := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}}}},
		FieldsData: []*schemapb.FieldData{
			{
				Type: schemapb.DataType_Int64, FieldId: 100,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{10, 20, 30}}},
				}},
			},
		},
		ElementLevel: true,
		ElementIndices: []*internalpb.ElementIndices{
			{Indices: []int32{0, 1}},
			{Indices: []int32{2}},
			{Indices: []int32{0}},
		},
	}

	sliced := rangeSliceRetrieveResults(result, 1, 3)

	assert.True(t, sliced.GetElementLevel())
	require.Len(t, sliced.GetElementIndices(), 2)
	assert.Equal(t, []int32{2}, sliced.GetElementIndices()[0].GetIndices())
	assert.Equal(t, []int32{0}, sliced.GetElementIndices()[1].GetIndices())
}

// =========================================================================
// ConcatAndCheckPKOperator
// =========================================================================

func TestConcatAndCheckPKOperator_NoDuplicate(t *testing.T) {
	op := NewConcatAndCheckPKOperator(nil)
	ctx := context.Background()

	r1 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2}}}},
		FieldsData: []*schemapb.FieldData{
			{
				Type: schemapb.DataType_Int64, FieldId: 100,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{10, 20}}},
				}},
			},
		},
	}
	r2 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{3, 4}}}},
		FieldsData: []*schemapb.FieldData{
			{
				Type: schemapb.DataType_Int64, FieldId: 100,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{30, 40}}},
				}},
			},
		},
	}

	outputs, err := op.Run(ctx, nil, []*internalpb.RetrieveResults{r1, r2})
	require.NoError(t, err)

	result := outputs[0].(*internalpb.RetrieveResults)
	ids := result.GetIds().GetIntId().GetData()
	// Concatenated in order: 1,2,3,4
	assert.Equal(t, []int64{1, 2, 3, 4}, ids)
}

func TestConcatAndCheckPKOperator_DuplicateFails(t *testing.T) {
	op := NewConcatAndCheckPKOperator(nil)
	ctx := context.Background()

	r1 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2}}}},
		FieldsData: []*schemapb.FieldData{
			{
				Type: schemapb.DataType_Int64, FieldId: 100,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{10, 20}}},
				}},
			},
		},
	}
	r2 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{2, 3}}}}, // PK=2 duplicate
		FieldsData: []*schemapb.FieldData{
			{
				Type: schemapb.DataType_Int64, FieldId: 100,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{21, 30}}},
				}},
			},
		},
	}

	_, err := op.Run(ctx, nil, []*internalpb.RetrieveResults{r1, r2})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate PK")
}

// =========================================================================
// buildMergedRetrieveResults: common scalar types (LongData, DoubleData, StringData)
// =========================================================================

func TestBuildMergedFieldData_CommonScalarTypes(t *testing.T) {
	// Merge two results with Int64 + Double + String fields
	makeDoubleField := func(id int64, name string, vals []float64) *schemapb.FieldData {
		return &schemapb.FieldData{
			FieldId: id, FieldName: name, Type: schemapb.DataType_Double,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: vals}},
			}},
		}
	}
	makeStringField := func(id int64, name string, vals []string) *schemapb.FieldData {
		return &schemapb.FieldData{
			FieldId: id, FieldName: name, Type: schemapb.DataType_VarChar,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: vals}},
			}},
		}
	}

	r1 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2}}}},
		FieldsData: []*schemapb.FieldData{
			{FieldId: 100, Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{10, 20}}},
			}}},
			makeDoubleField(101, "price", []float64{1.1, 2.2}),
			makeStringField(102, "name", []string{"alice", "bob"}),
		},
	}
	r2 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{3}}}},
		FieldsData: []*schemapb.FieldData{
			{FieldId: 100, Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{30}}},
			}}},
			makeDoubleField(101, "price", []float64{3.3}),
			makeStringField(102, "name", []string{"charlie"}),
		},
	}

	selectedRows := []rowRef{{0, 0}, {1, 0}, {0, 1}} // r1[0], r2[0], r1[1]
	merged, err := buildMergedRetrieveResults([]*internalpb.RetrieveResults{r1, r2}, selectedRows, nil)
	require.NoError(t, err)

	assert.Equal(t, []int64{1, 3, 2}, merged.GetIds().GetIntId().GetData())
	assert.Equal(t, []int64{10, 30, 20}, merged.GetFieldsData()[0].GetScalars().GetLongData().GetData())
	assert.Equal(t, []float64{1.1, 3.3, 2.2}, merged.GetFieldsData()[1].GetScalars().GetDoubleData().GetData())
	assert.Equal(t, []string{"alice", "charlie", "bob"}, merged.GetFieldsData()[2].GetScalars().GetStringData().GetData())
}

// =========================================================================
// buildMergedRetrieveResults: FloatVector merge
// =========================================================================

func TestBuildMergedFieldData_FloatVector(t *testing.T) {
	dim := 4
	makeVecField := func(id int64, data []float32) *schemapb.FieldData {
		return &schemapb.FieldData{
			FieldId: id, Type: schemapb.DataType_FloatVector,
			Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Dim:  int64(dim),
				Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: data}},
			}},
		}
	}

	r1 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2}}}},
		FieldsData: []*schemapb.FieldData{
			makeVecField(100, []float32{1, 2, 3, 4, 5, 6, 7, 8}), // 2 rows × dim=4
		},
	}
	r2 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{3}}}},
		FieldsData: []*schemapb.FieldData{
			makeVecField(100, []float32{9, 10, 11, 12}), // 1 row × dim=4
		},
	}

	selectedRows := []rowRef{{1, 0}, {0, 1}} // r2[0], r1[1]
	merged, err := buildMergedRetrieveResults([]*internalpb.RetrieveResults{r1, r2}, selectedRows, nil)
	require.NoError(t, err)

	vecData := merged.GetFieldsData()[0].GetVectors().GetFloatVector().GetData()
	assert.Equal(t, []float32{9, 10, 11, 12, 5, 6, 7, 8}, vecData)
}

// =========================================================================
// calcRowSize
// =========================================================================

func TestCalcRowSize_BasicTypes(t *testing.T) {
	r := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}},
		FieldsData: []*schemapb.FieldData{
			// Int64: 8 bytes
			{FieldId: 100, Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{42}}},
			}}},
			// Double: 8 bytes
			{FieldId: 101, Type: schemapb.DataType_Double, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: []float64{3.14}}},
			}}},
			// String: len("hello") = 5 bytes
			{FieldId: 102, Type: schemapb.DataType_VarChar, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"hello"}}},
			}}},
		},
	}

	size := calcRowSize(r, 0)
	assert.Equal(t, int64(8+8+5), size) // Int64(8) + Double(8) + String("hello"=5)
}

func TestCalcRowSize_EmptyString(t *testing.T) {
	r := &internalpb.RetrieveResults{
		FieldsData: []*schemapb.FieldData{
			{FieldId: 100, Type: schemapb.DataType_VarChar, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{""}}},
			}}},
		},
	}
	size := calcRowSize(r, 0)
	assert.Equal(t, int64(0), size)
}

func TestCalcRowSize_FloatVector(t *testing.T) {
	dim := 8
	r := &internalpb.RetrieveResults{
		FieldsData: []*schemapb.FieldData{
			{
				FieldId: 100, Type: schemapb.DataType_FloatVector,
				Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
					Dim:  int64(dim),
					Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: make([]float32, dim*2)}}, // 2 rows
				}},
			},
		},
	}
	// FloatVector: dim * 4 bytes per row = 8 * 4 = 32
	size := calcRowSize(r, 0)
	assert.Equal(t, int64(dim*4), size)
}

func TestCalcRowSize_MultipleFieldTypes(t *testing.T) {
	r := &internalpb.RetrieveResults{
		FieldsData: []*schemapb.FieldData{
			// Bool: 1 byte
			{
				FieldId: 100, Type: schemapb.DataType_Bool,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: []bool{true}}},
				}},
			},
			// Float: 4 bytes
			{
				FieldId: 101, Type: schemapb.DataType_Float,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: []float32{1.0}}},
				}},
			},
			// Int32: 4 bytes
			{
				FieldId: 102, Type: schemapb.DataType_Int32,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{42}}},
				}},
			},
		},
	}
	size := calcRowSize(r, 0)
	assert.Equal(t, int64(1+4+4), size) // Bool(1) + Float(4) + Int32(4)
}

// =========================================================================
// validateElementLevelConsistency
// =========================================================================

func TestValidateElementLevelConsistency_Consistent(t *testing.T) {
	r1 := &internalpb.RetrieveResults{
		Ids:          &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}},
		ElementLevel: true,
		ElementIndices: []*internalpb.ElementIndices{
			{Indices: []int32{0, 1}},
		},
	}
	r2 := &internalpb.RetrieveResults{
		Ids:          &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{2}}}},
		ElementLevel: true,
		ElementIndices: []*internalpb.ElementIndices{
			{Indices: []int32{3}},
		},
	}
	err := validateElementLevelConsistency([]*internalpb.RetrieveResults{r1, r2}, nil)
	assert.NoError(t, err)
}

// makeStructArrayFieldData creates a FieldData wrapping a StructArrayField with scalar sub-fields.
// Used by TestGetRowCount and slice/reconstruct tests below.
func makeStructArrayFieldData(fieldID int64, name string, subFields []*schemapb.FieldData) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Array,
		FieldId:   fieldID,
		FieldName: name,
		Field: &schemapb.FieldData_StructArrays{
			StructArrays: &schemapb.StructArrayField{
				Fields: subFields,
			},
		},
	}
}

// =========================================================================
// rangeSliceStructArrayField
// =========================================================================

func TestRangeSliceStructArrayField(t *testing.T) {
	sa := &schemapb.StructArrayField{
		Fields: []*schemapb.FieldData{
			{
				Type: schemapb.DataType_Int64, FieldId: 101, FieldName: "age",
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{10, 20, 30, 40}}},
				}},
			},
			{
				Type: schemapb.DataType_VarChar, FieldId: 102, FieldName: "name",
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"a", "b", "c", "d"}}},
				}},
			},
		},
	}

	sliced := rangeSliceStructArrayField(sa, 1, 3)
	require.Len(t, sliced.GetFields(), 2)
	assert.Equal(t, []int64{20, 30}, sliced.GetFields()[0].GetScalars().GetLongData().GetData())
	assert.Equal(t, []string{"b", "c"}, sliced.GetFields()[1].GetScalars().GetStringData().GetData())
}

func TestRangeSliceStructArrayField_Nil(t *testing.T) {
	assert.Nil(t, rangeSliceStructArrayField(nil, 0, 1))
}

// =========================================================================
// sliceStructArrayField
// =========================================================================

func TestSliceStructArrayField(t *testing.T) {
	sa := &schemapb.StructArrayField{
		Fields: []*schemapb.FieldData{
			{
				Type: schemapb.DataType_Int64, FieldId: 101, FieldName: "val",
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{100, 200, 300, 400}}},
				}},
			},
			{
				Type: schemapb.DataType_VarChar, FieldId: 102, FieldName: "tag",
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"x", "y", "z", "w"}}},
				}},
			},
		},
	}

	// Select indices 3, 0, 2 (out of order)
	sliced := sliceStructArrayField(sa, []int{3, 0, 2})
	require.Len(t, sliced.GetFields(), 2)
	assert.Equal(t, []int64{400, 100, 300}, sliced.GetFields()[0].GetScalars().GetLongData().GetData())
	assert.Equal(t, []string{"w", "x", "z"}, sliced.GetFields()[1].GetScalars().GetStringData().GetData())
}

func TestSliceStructArrayField_Nil(t *testing.T) {
	assert.Nil(t, sliceStructArrayField(nil, []int{0}))
}

// =========================================================================
// buildMergedVectorField: VectorArray (used in StructArray vector sub-fields)
// =========================================================================

func TestBuildMergedVectorField_VectorArray(t *testing.T) {
	dim := int64(2)
	v1 := &schemapb.VectorField{Dim: dim, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1.0, 2.0}}}}
	v2 := &schemapb.VectorField{Dim: dim, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{3.0, 4.0}}}}
	v3 := &schemapb.VectorField{Dim: dim, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{5.0, 6.0}}}}

	r1 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2}}}},
		FieldsData: []*schemapb.FieldData{
			{
				Type: schemapb.DataType_FloatVector, FieldId: 700,
				Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
					Dim: dim,
					Data: &schemapb.VectorField_VectorArray{VectorArray: &schemapb.VectorArray{
						Dim:         dim,
						Data:        []*schemapb.VectorField{v1, v2},
						ElementType: schemapb.DataType_FloatVector,
					}},
				}},
			},
		},
	}
	r2 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{3}}}},
		FieldsData: []*schemapb.FieldData{
			{
				Type: schemapb.DataType_FloatVector, FieldId: 700,
				Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
					Dim: dim,
					Data: &schemapb.VectorField_VectorArray{VectorArray: &schemapb.VectorArray{
						Dim:         dim,
						Data:        []*schemapb.VectorField{v3},
						ElementType: schemapb.DataType_FloatVector,
					}},
				}},
			},
		},
	}

	results := []*internalpb.RetrieveResults{r1, r2}
	selectedRows := []rowRef{{resultIdx: 1, rowIdx: 0}, {resultIdx: 0, rowIdx: 0}}

	merged, err := buildMergedRetrieveResults(results, selectedRows, nil)
	require.NoError(t, err)

	va := merged.FieldsData[0].GetVectors().GetVectorArray()
	require.Len(t, va.GetData(), 2)
	assert.Equal(t, []float32{5.0, 6.0}, va.GetData()[0].GetFloatVector().GetData())
	assert.Equal(t, []float32{1.0, 2.0}, va.GetData()[1].GetFloatVector().GetData())
}

// =========================================================================
// sliceVectorField & rangeSliceVectorField: VectorArray branch
// =========================================================================

func TestSliceVectorField_VectorArray(t *testing.T) {
	dim := int64(2)
	v1 := &schemapb.VectorField{Dim: dim, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1.0, 2.0}}}}
	v2 := &schemapb.VectorField{Dim: dim, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{3.0, 4.0}}}}
	v3 := &schemapb.VectorField{Dim: dim, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{5.0, 6.0}}}}

	vf := &schemapb.VectorField{
		Dim: dim,
		Data: &schemapb.VectorField_VectorArray{VectorArray: &schemapb.VectorArray{
			Dim:         dim,
			Data:        []*schemapb.VectorField{v1, v2, v3},
			ElementType: schemapb.DataType_FloatVector,
		}},
	}

	sliced := sliceVectorField(vf, []int{2, 0}, nil)
	va := sliced.GetVectorArray()
	require.Len(t, va.GetData(), 2)
	assert.Equal(t, []float32{5.0, 6.0}, va.GetData()[0].GetFloatVector().GetData())
	assert.Equal(t, []float32{1.0, 2.0}, va.GetData()[1].GetFloatVector().GetData())
	assert.Equal(t, schemapb.DataType_FloatVector, va.GetElementType())
}

func TestRangeSliceVectorField_VectorArray(t *testing.T) {
	dim := int64(2)
	v1 := &schemapb.VectorField{Dim: dim, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1.0, 2.0}}}}
	v2 := &schemapb.VectorField{Dim: dim, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{3.0, 4.0}}}}
	v3 := &schemapb.VectorField{Dim: dim, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{5.0, 6.0}}}}

	vf := &schemapb.VectorField{
		Dim: dim,
		Data: &schemapb.VectorField_VectorArray{VectorArray: &schemapb.VectorArray{
			Dim:         dim,
			Data:        []*schemapb.VectorField{v1, v2, v3},
			ElementType: schemapb.DataType_FloatVector,
		}},
	}

	sliced := rangeSliceVectorField(vf, 1, 3, nil)
	va := sliced.GetVectorArray()
	require.Len(t, va.GetData(), 2)
	assert.Equal(t, []float32{3.0, 4.0}, va.GetData()[0].GetFloatVector().GetData())
	assert.Equal(t, []float32{5.0, 6.0}, va.GetData()[1].GetFloatVector().GetData())
}

// =========================================================================
// comparePK: string PK
// =========================================================================

func TestComparePK_StringPK(t *testing.T) {
	assert.Equal(t, -1, comparePK("apple", "banana"))
	assert.Equal(t, 0, comparePK("same", "same"))
	assert.Equal(t, 1, comparePK("zebra", "alpha"))
}

func TestComparePK_Int64PK(t *testing.T) {
	assert.Equal(t, -1, comparePK(int64(1), int64(2)))
	assert.Equal(t, 0, comparePK(int64(5), int64(5)))
	assert.Equal(t, 1, comparePK(int64(10), int64(3)))
}

// =========================================================================
// buildMergedScalarField: additional type branches
// =========================================================================

func TestBuildMergedScalarField_BoolData(t *testing.T) {
	r1 := makeInternalResultIntPK([]int64{1, 2}, &schemapb.FieldData{
		Type: schemapb.DataType_Bool, FieldId: 100,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: []bool{true, false}}},
		}},
	})
	selectedRows := []rowRef{{resultIdx: 0, rowIdx: 1}, {resultIdx: 0, rowIdx: 0}}
	merged, err := buildMergedRetrieveResults([]*internalpb.RetrieveResults{r1}, selectedRows, nil)
	require.NoError(t, err)
	assert.Equal(t, []bool{false, true}, merged.FieldsData[0].GetScalars().GetBoolData().GetData())
}

func TestBuildMergedScalarField_IntData(t *testing.T) {
	r1 := makeInternalResultIntPK([]int64{1, 2}, &schemapb.FieldData{
		Type: schemapb.DataType_Int32, FieldId: 100,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{10, 20}}},
		}},
	})
	selectedRows := []rowRef{{resultIdx: 0, rowIdx: 1}}
	merged, err := buildMergedRetrieveResults([]*internalpb.RetrieveResults{r1}, selectedRows, nil)
	require.NoError(t, err)
	assert.Equal(t, []int32{20}, merged.FieldsData[0].GetScalars().GetIntData().GetData())
}

func TestBuildMergedScalarField_FloatData(t *testing.T) {
	r1 := makeInternalResultIntPK([]int64{1}, &schemapb.FieldData{
		Type: schemapb.DataType_Float, FieldId: 100,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: []float32{1.5, 2.5}}},
		}},
	})
	selectedRows := []rowRef{{resultIdx: 0, rowIdx: 0}}
	merged, err := buildMergedRetrieveResults([]*internalpb.RetrieveResults{r1}, selectedRows, nil)
	require.NoError(t, err)
	assert.Equal(t, []float32{1.5}, merged.FieldsData[0].GetScalars().GetFloatData().GetData())
}

func TestBuildMergedScalarField_DoubleData(t *testing.T) {
	r1 := makeInternalResultIntPK([]int64{1, 2}, &schemapb.FieldData{
		Type: schemapb.DataType_Double, FieldId: 100,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: []float64{3.14, 2.71}}},
		}},
	})
	selectedRows := []rowRef{{resultIdx: 0, rowIdx: 1}}
	merged, err := buildMergedRetrieveResults([]*internalpb.RetrieveResults{r1}, selectedRows, nil)
	require.NoError(t, err)
	assert.Equal(t, []float64{2.71}, merged.FieldsData[0].GetScalars().GetDoubleData().GetData())
}

func TestBuildMergedScalarField_BytesData(t *testing.T) {
	r1 := makeInternalResultIntPK([]int64{1, 2}, &schemapb.FieldData{
		Type: schemapb.DataType_BinaryVector, FieldId: 100,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_BytesData{BytesData: &schemapb.BytesArray{Data: [][]byte{{0x01}, {0x02}}}},
		}},
	})
	selectedRows := []rowRef{{resultIdx: 0, rowIdx: 0}, {resultIdx: 0, rowIdx: 1}}
	merged, err := buildMergedRetrieveResults([]*internalpb.RetrieveResults{r1}, selectedRows, nil)
	require.NoError(t, err)
	assert.Equal(t, [][]byte{{0x01}, {0x02}}, merged.FieldsData[0].GetScalars().GetBytesData().GetData())
}

func TestBuildMergedScalarField_JsonData(t *testing.T) {
	r1 := makeInternalResultIntPK([]int64{1, 2}, &schemapb.FieldData{
		Type: schemapb.DataType_JSON, FieldId: 100,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{Data: [][]byte{[]byte(`{"a":1}`), []byte(`{"b":2}`)}}},
		}},
	})
	selectedRows := []rowRef{{resultIdx: 0, rowIdx: 1}}
	merged, err := buildMergedRetrieveResults([]*internalpb.RetrieveResults{r1}, selectedRows, nil)
	require.NoError(t, err)
	assert.Equal(t, [][]byte{[]byte(`{"b":2}`)}, merged.FieldsData[0].GetScalars().GetJsonData().GetData())
}

// =========================================================================
// buildMergedIDs: string PK
// =========================================================================

func TestBuildMergedIDs_StringPK(t *testing.T) {
	r1 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"a", "b"}}}},
		FieldsData: []*schemapb.FieldData{
			makeStringField(100, "pk", []string{"a", "b"}),
		},
	}
	selectedRows := []rowRef{{resultIdx: 0, rowIdx: 1}, {resultIdx: 0, rowIdx: 0}}
	merged, err := buildMergedRetrieveResults([]*internalpb.RetrieveResults{r1}, selectedRows, nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"b", "a"}, merged.GetIds().GetStrId().GetData())
}

// =========================================================================
// buildMergedVectorField: additional vector type branches
// =========================================================================

func TestBuildMergedVectorField_BinaryVector(t *testing.T) {
	dim := 16 // 2 bytes per row
	r1 := makeInternalResultIntPK([]int64{1, 2}, &schemapb.FieldData{
		Type: schemapb.DataType_BinaryVector, FieldId: 100,
		Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
			Dim:  int64(dim),
			Data: &schemapb.VectorField_BinaryVector{BinaryVector: []byte{0xAA, 0xBB, 0xCC, 0xDD}},
		}},
	})
	selectedRows := []rowRef{{resultIdx: 0, rowIdx: 1}}
	merged, err := buildMergedRetrieveResults([]*internalpb.RetrieveResults{r1}, selectedRows, nil)
	require.NoError(t, err)
	assert.Equal(t, []byte{0xCC, 0xDD}, merged.FieldsData[0].GetVectors().GetBinaryVector())
}

func TestBuildMergedVectorField_Float16Vector(t *testing.T) {
	dim := 2 // 4 bytes per row (dim*2)
	r1 := makeInternalResultIntPK([]int64{1, 2}, &schemapb.FieldData{
		Type: schemapb.DataType_Float16Vector, FieldId: 100,
		Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
			Dim:  int64(dim),
			Data: &schemapb.VectorField_Float16Vector{Float16Vector: []byte{1, 2, 3, 4, 5, 6, 7, 8}},
		}},
	})
	selectedRows := []rowRef{{resultIdx: 0, rowIdx: 1}}
	merged, err := buildMergedRetrieveResults([]*internalpb.RetrieveResults{r1}, selectedRows, nil)
	require.NoError(t, err)
	assert.Equal(t, []byte{5, 6, 7, 8}, merged.FieldsData[0].GetVectors().GetFloat16Vector())
}

func TestBuildMergedVectorField_BFloat16Vector(t *testing.T) {
	dim := 2
	r1 := makeInternalResultIntPK([]int64{1, 2}, &schemapb.FieldData{
		Type: schemapb.DataType_BFloat16Vector, FieldId: 100,
		Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
			Dim:  int64(dim),
			Data: &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: []byte{10, 20, 30, 40, 50, 60, 70, 80}},
		}},
	})
	selectedRows := []rowRef{{resultIdx: 0, rowIdx: 0}}
	merged, err := buildMergedRetrieveResults([]*internalpb.RetrieveResults{r1}, selectedRows, nil)
	require.NoError(t, err)
	assert.Equal(t, []byte{10, 20, 30, 40}, merged.FieldsData[0].GetVectors().GetBfloat16Vector())
}

func TestBuildMergedVectorField_SparseFloatVector(t *testing.T) {
	r1 := makeInternalResultIntPK([]int64{1, 2}, &schemapb.FieldData{
		Type: schemapb.DataType_SparseFloatVector, FieldId: 100,
		Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
			Dim: 100,
			Data: &schemapb.VectorField_SparseFloatVector{SparseFloatVector: &schemapb.SparseFloatArray{
				Contents: [][]byte{{0x01, 0x02}, {0x03, 0x04}},
				Dim:      100,
			}},
		}},
	})
	selectedRows := []rowRef{{resultIdx: 0, rowIdx: 1}}
	merged, err := buildMergedRetrieveResults([]*internalpb.RetrieveResults{r1}, selectedRows, nil)
	require.NoError(t, err)
	assert.Equal(t, [][]byte{{0x03, 0x04}}, merged.FieldsData[0].GetVectors().GetSparseFloatVector().GetContents())
}

// =========================================================================
// buildCompactIndices / getVecDataIdx: nullable vector
// =========================================================================

func TestBuildCompactIndices_NullableVector(t *testing.T) {
	r1 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}}}},
		FieldsData: []*schemapb.FieldData{
			{
				Type: schemapb.DataType_FloatVector, FieldId: 100,
				Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
					Dim:  2,
					Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4}}},
				}},
				ValidData: []bool{true, false, true}, // row0=valid(idx0), row1=null, row2=valid(idx1)
			},
		},
	}
	indices, err := buildCompactIndices([]*internalpb.RetrieveResults{r1}, 0, true)
	require.NoError(t, err)
	require.NotNil(t, indices)
	assert.Equal(t, []int{0, -1, 1}, indices[0])
}

func TestBuildCompactIndices_NonNullable(t *testing.T) {
	r1 := &internalpb.RetrieveResults{
		FieldsData: []*schemapb.FieldData{
			{
				Type: schemapb.DataType_FloatVector, FieldId: 100,
				Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
					Dim:  2,
					Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4}}},
				}},
				// No ValidData
			},
		},
	}
	indices, err := buildCompactIndices([]*internalpb.RetrieveResults{r1}, 0, false)
	require.NoError(t, err)
	assert.Nil(t, indices) // non-nullable → nil
}

func TestBuildCompactIndices_FailFast_EmptyValidData(t *testing.T) {
	// nullable field with numRows > 0 but empty ValidData → must error
	r1 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2}}}},
		FieldsData: []*schemapb.FieldData{
			{
				Type: schemapb.DataType_SparseFloatVector, FieldId: 100,
				// ValidData intentionally empty — segcore contract violation
			},
		},
	}
	_, err := buildCompactIndices([]*internalpb.RetrieveResults{r1}, 0, true)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty ValidData")
}

func TestBuildCompactIndices_FailFast_LengthMismatch(t *testing.T) {
	// nullable field with len(ValidData) != numRows → must error
	r1 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}}}},
		FieldsData: []*schemapb.FieldData{
			{
				Type: schemapb.DataType_FloatVector, FieldId: 100,
				ValidData: []bool{true, false}, // 2 entries but 3 rows → mismatch
			},
		},
	}
	_, err := buildCompactIndices([]*internalpb.RetrieveResults{r1}, 0, true)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "len(ValidData)")
}

func TestGetVecDataIdx(t *testing.T) {
	// nil compactIndices → returns rowIdx directly
	assert.Equal(t, 3, getVecDataIdx(nil, rowRef{resultIdx: 0, rowIdx: 3}))

	// With compact indices
	compactIdx := [][]int{
		{0, -1, 1}, // result 0: row0=0, row1=null, row2=1
		nil,        // result 1: non-nullable
	}
	assert.Equal(t, 0, getVecDataIdx(compactIdx, rowRef{resultIdx: 0, rowIdx: 0}))
	assert.Equal(t, -1, getVecDataIdx(compactIdx, rowRef{resultIdx: 0, rowIdx: 1}))
	assert.Equal(t, 1, getVecDataIdx(compactIdx, rowRef{resultIdx: 0, rowIdx: 2}))
	assert.Equal(t, 5, getVecDataIdx(compactIdx, rowRef{resultIdx: 1, rowIdx: 5})) // nil slice → rowIdx

	// Out of range → panics (validated upstream by buildCompactIndices)
	assert.Panics(t, func() { getVecDataIdx(compactIdx, rowRef{resultIdx: 0, rowIdx: 10}) })
}

// =========================================================================
// buildMergedVectorField: truncated data error paths
// =========================================================================

func TestBuildMergedVectorField_FloatVector_TruncatedData(t *testing.T) {
	// FloatVector with dim=2 but only 2 floats (enough for 1 row, not 2)
	r1 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2}}}},
		FieldsData: []*schemapb.FieldData{{
			FieldId: 100, Type: schemapb.DataType_FloatVector,
			Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Dim:  2,
				Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2}}}, // only 1 row
			}},
		}},
	}
	selectedRows := []rowRef{{resultIdx: 0, rowIdx: 0}, {resultIdx: 0, rowIdx: 1}}
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "2"}}},
	}}

	_, err := buildMergedRetrieveResults([]*internalpb.RetrieveResults{r1}, selectedRows, schema)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "truncated data")
}

func TestBuildMergedVectorField_BinaryVector_TruncatedData(t *testing.T) {
	// BinaryVector with dim=16 (2 bytes/row) but only 2 bytes (1 row, not 2)
	r1 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2}}}},
		FieldsData: []*schemapb.FieldData{{
			FieldId: 100, Type: schemapb.DataType_BinaryVector,
			Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Dim:  16,
				Data: &schemapb.VectorField_BinaryVector{BinaryVector: []byte{0xFF, 0x00}}, // 1 row
			}},
		}},
	}
	selectedRows := []rowRef{{resultIdx: 0, rowIdx: 0}, {resultIdx: 0, rowIdx: 1}}
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, DataType: schemapb.DataType_BinaryVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "16"}}},
	}}

	_, err := buildMergedRetrieveResults([]*internalpb.RetrieveResults{r1}, selectedRows, schema)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "truncated data")
}

func TestBuildMergedVectorField_SparseVector_TruncatedContents(t *testing.T) {
	// Sparse with ValidData=[true, true] but Contents has only 1 entry
	r1 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2}}}},
		FieldsData: []*schemapb.FieldData{{
			FieldId: 100, Type: schemapb.DataType_SparseFloatVector,
			Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Data: &schemapb.VectorField_SparseFloatVector{SparseFloatVector: &schemapb.SparseFloatArray{
					Contents: [][]byte{makeTestSparseVec(1, 0.5)}, // only 1 content
				}},
			}},
			ValidData: []bool{true, true}, // claims 2 valid rows
		}},
	}
	selectedRows := []rowRef{{resultIdx: 0, rowIdx: 0}, {resultIdx: 0, rowIdx: 1}}

	_, err := buildMergedRetrieveResults([]*internalpb.RetrieveResults{r1}, selectedRows,
		makeNullableSchema(100, schemapb.DataType_SparseFloatVector, 0))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "truncated data")
}

func TestBuildMergedVectorField_NullableCompact_BinaryVector(t *testing.T) {
	// BinaryVector dim=8 (1 byte/row), nullable: row0=valid, row1=null, row2=valid
	// Compact data: 2 bytes (only valid rows)
	r1 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}}}},
		FieldsData: []*schemapb.FieldData{{
			FieldId: 100, Type: schemapb.DataType_BinaryVector,
			Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Dim:  8,
				Data: &schemapb.VectorField_BinaryVector{BinaryVector: []byte{0xAA, 0xBB}}, // 2 compact rows
			}},
			ValidData: []bool{true, false, true},
		}},
	}
	selectedRows := []rowRef{{resultIdx: 0, rowIdx: 2}, {resultIdx: 0, rowIdx: 0}, {resultIdx: 0, rowIdx: 1}}

	merged, err := buildMergedRetrieveResults([]*internalpb.RetrieveResults{r1}, selectedRows,
		makeNullableSchema(100, schemapb.DataType_BinaryVector, 8))
	require.NoError(t, err)

	// row2 → compact idx 1 → 0xBB, row0 → compact idx 0 → 0xAA, row1 → null (skipped)
	assert.Equal(t, []byte{0xBB, 0xAA}, merged.FieldsData[0].GetVectors().GetBinaryVector())
	assert.Equal(t, []bool{true, true, false}, merged.FieldsData[0].ValidData)
}

func TestBuildMergedVectorField_NullableCompact_Float16Vector(t *testing.T) {
	// Float16 dim=1 (2 bytes/row), nullable: row0=null, row1=valid
	// Compact data: 2 bytes (1 valid row)
	r1 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2}}}},
		FieldsData: []*schemapb.FieldData{{
			FieldId: 100, Type: schemapb.DataType_Float16Vector,
			Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Dim:  1,
				Data: &schemapb.VectorField_Float16Vector{Float16Vector: []byte{0x01, 0x02}}, // 1 compact row
			}},
			ValidData: []bool{false, true},
		}},
	}
	selectedRows := []rowRef{{resultIdx: 0, rowIdx: 1}, {resultIdx: 0, rowIdx: 0}}

	merged, err := buildMergedRetrieveResults([]*internalpb.RetrieveResults{r1}, selectedRows,
		makeNullableSchema(100, schemapb.DataType_Float16Vector, 1))
	require.NoError(t, err)

	// row1 → compact idx 0 → [0x01, 0x02], row0 → null (skipped)
	assert.Equal(t, []byte{0x01, 0x02}, merged.FieldsData[0].GetVectors().GetFloat16Vector())
	assert.Equal(t, []bool{true, false}, merged.FieldsData[0].ValidData)
}

func TestBuildMergedVectorField_Float16Vector_TruncatedData(t *testing.T) {
	// Float16 dim=2 (4 bytes/row) but only 4 bytes provided (1 row, not 2)
	r1 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2}}}},
		FieldsData: []*schemapb.FieldData{{
			FieldId: 100, Type: schemapb.DataType_Float16Vector,
			Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Dim:  2,
				Data: &schemapb.VectorField_Float16Vector{Float16Vector: []byte{0x01, 0x02, 0x03, 0x04}}, // 1 row
			}},
		}},
	}
	selectedRows := []rowRef{{resultIdx: 0, rowIdx: 0}, {resultIdx: 0, rowIdx: 1}}
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, DataType: schemapb.DataType_Float16Vector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "2"}}},
	}}

	_, err := buildMergedRetrieveResults([]*internalpb.RetrieveResults{r1}, selectedRows, schema)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "truncated data")
}

func TestBuildMergedVectorField_NullableCompact_BFloat16Vector(t *testing.T) {
	// BFloat16 dim=1 (2 bytes/row), nullable: row0=valid, row1=null, row2=valid
	// Compact data: 4 bytes (2 valid rows)
	r1 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}}}},
		FieldsData: []*schemapb.FieldData{{
			FieldId: 100, Type: schemapb.DataType_BFloat16Vector,
			Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Dim:  1,
				Data: &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: []byte{0xAA, 0xBB, 0xCC, 0xDD}},
			}},
			ValidData: []bool{true, false, true},
		}},
	}
	// Select row1(null), row2(valid), row0(valid)
	selectedRows := []rowRef{{resultIdx: 0, rowIdx: 1}, {resultIdx: 0, rowIdx: 2}, {resultIdx: 0, rowIdx: 0}}

	merged, err := buildMergedRetrieveResults([]*internalpb.RetrieveResults{r1}, selectedRows,
		makeNullableSchema(100, schemapb.DataType_BFloat16Vector, 1))
	require.NoError(t, err)

	// row1 → null (skipped), row2 → compact idx 1 → [0xCC,0xDD], row0 → compact idx 0 → [0xAA,0xBB]
	assert.Equal(t, []byte{0xCC, 0xDD, 0xAA, 0xBB}, merged.FieldsData[0].GetVectors().GetBfloat16Vector())
	assert.Equal(t, []bool{false, true, true}, merged.FieldsData[0].ValidData)
}

func TestBuildMergedVectorField_BFloat16Vector_TruncatedData(t *testing.T) {
	// BFloat16 dim=2 (4 bytes/row) but only 4 bytes (1 row, not 2)
	r1 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2}}}},
		FieldsData: []*schemapb.FieldData{{
			FieldId: 100, Type: schemapb.DataType_BFloat16Vector,
			Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Dim:  2,
				Data: &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: []byte{0x01, 0x02, 0x03, 0x04}},
			}},
		}},
	}
	selectedRows := []rowRef{{resultIdx: 0, rowIdx: 0}, {resultIdx: 0, rowIdx: 1}}
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, DataType: schemapb.DataType_BFloat16Vector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "2"}}},
	}}

	_, err := buildMergedRetrieveResults([]*internalpb.RetrieveResults{r1}, selectedRows, schema)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "truncated data")
}

func TestBuildMergedVectorField_NullableCompact_Int8Vector(t *testing.T) {
	// Int8Vector dim=2 (2 bytes/row), nullable: row0=null, row1=valid
	// Compact data: 2 bytes (1 valid row)
	r1 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2}}}},
		FieldsData: []*schemapb.FieldData{{
			FieldId: 100, Type: schemapb.DataType_Int8Vector,
			Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Dim:  2,
				Data: &schemapb.VectorField_Int8Vector{Int8Vector: []byte{0x11, 0x22}},
			}},
			ValidData: []bool{false, true},
		}},
	}
	selectedRows := []rowRef{{resultIdx: 0, rowIdx: 0}, {resultIdx: 0, rowIdx: 1}}

	merged, err := buildMergedRetrieveResults([]*internalpb.RetrieveResults{r1}, selectedRows,
		makeNullableSchema(100, schemapb.DataType_Int8Vector, 2))
	require.NoError(t, err)

	// row0 → null (skipped), row1 → compact idx 0 → [0x11,0x22]
	assert.Equal(t, []byte{0x11, 0x22}, merged.FieldsData[0].GetVectors().GetInt8Vector())
	assert.Equal(t, []bool{false, true}, merged.FieldsData[0].ValidData)
}

func TestBuildMergedVectorField_Int8Vector_TruncatedData(t *testing.T) {
	// Int8Vector dim=3 (3 bytes/row) but only 3 bytes (1 row, not 2)
	r1 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2}}}},
		FieldsData: []*schemapb.FieldData{{
			FieldId: 100, Type: schemapb.DataType_Int8Vector,
			Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Dim:  3,
				Data: &schemapb.VectorField_Int8Vector{Int8Vector: []byte{0x01, 0x02, 0x03}},
			}},
		}},
	}
	selectedRows := []rowRef{{resultIdx: 0, rowIdx: 0}, {resultIdx: 0, rowIdx: 1}}
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, DataType: schemapb.DataType_Int8Vector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "3"}}},
	}}

	_, err := buildMergedRetrieveResults([]*internalpb.RetrieveResults{r1}, selectedRows, schema)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "truncated data")
}

// Note: VectorArray `di < 0` branch (default case with compact mode) is architecturally
// unreachable. To get compact mode, fieldSchema must be non-nil (nullable=true). But non-nil
// fieldSchema provides a concrete DataType that matches one of the explicit switch cases
// (FloatVector, BinaryVector, etc.), so the default/VectorArray branch is only entered via
// nil-schema fallback where isNullable=false and compact indices are nil.
// This leaves 1 line (VectorArray di<0) permanently uncovered — accepted as dead code.

func TestBuildMergedVectorField_VectorArray_TruncatedData(t *testing.T) {
	dim := int64(2)
	v1 := &schemapb.VectorField{Dim: dim, Data: &schemapb.VectorField_FloatVector{
		FloatVector: &schemapb.FloatArray{Data: []float32{1.0, 2.0}},
	}}

	// Result has 2 IDs but VectorArray has only 1 entry
	r1 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2}}}},
		FieldsData: []*schemapb.FieldData{{
			FieldId: 100, Type: schemapb.DataType_FloatVector,
			Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Dim: dim,
				Data: &schemapb.VectorField_VectorArray{VectorArray: &schemapb.VectorArray{
					Dim:         dim,
					Data:        []*schemapb.VectorField{v1}, // only 1 entry
					ElementType: schemapb.DataType_FloatVector,
				}},
			}},
		}},
	}
	selectedRows := []rowRef{{resultIdx: 0, rowIdx: 0}, {resultIdx: 0, rowIdx: 1}}

	// nil schema → fallback infers VectorArray from data
	_, err := buildMergedRetrieveResults([]*internalpb.RetrieveResults{r1}, selectedRows, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "truncated data")
}

// =========================================================================
// rangeSlice / slice: string IDs
// =========================================================================

func TestRangeSliceIDs_StringID(t *testing.T) {
	ids := &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"a", "b", "c", "d"}}}}
	sliced := rangeSliceIDs(ids, 1, 3)
	assert.Equal(t, []string{"b", "c"}, sliced.GetStrId().GetData())
}

func TestSliceIDs_StringID(t *testing.T) {
	ids := &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"x", "y", "z"}}}}
	sliced := sliceIDs(ids, []int{2, 0})
	assert.Equal(t, []string{"z", "x"}, sliced.GetStrId().GetData())
}

// =========================================================================
// rangeSliceVectorField: additional vector types
// =========================================================================

func TestRangeSliceVectorField_FloatVector(t *testing.T) {
	vf := &schemapb.VectorField{
		Dim:  2,
		Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4, 5, 6}}},
	}
	sliced := rangeSliceVectorField(vf, 1, 3, nil)
	assert.Equal(t, []float32{3, 4, 5, 6}, sliced.GetFloatVector().GetData())
}

func TestRangeSliceVectorField_BinaryVector(t *testing.T) {
	vf := &schemapb.VectorField{
		Dim:  16, // 2 bytes per row
		Data: &schemapb.VectorField_BinaryVector{BinaryVector: []byte{0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF}},
	}
	sliced := rangeSliceVectorField(vf, 0, 2, nil)
	assert.Equal(t, []byte{0xAA, 0xBB, 0xCC, 0xDD}, sliced.GetBinaryVector())
}

func TestRangeSliceVectorField_Float16Vector(t *testing.T) {
	vf := &schemapb.VectorField{
		Dim:  2, // 4 bytes per row
		Data: &schemapb.VectorField_Float16Vector{Float16Vector: []byte{1, 2, 3, 4, 5, 6, 7, 8}},
	}
	sliced := rangeSliceVectorField(vf, 1, 2, nil)
	assert.Equal(t, []byte{5, 6, 7, 8}, sliced.GetFloat16Vector())
}

func TestRangeSliceVectorField_SparseFloatVector(t *testing.T) {
	vf := &schemapb.VectorField{
		Dim: 100,
		Data: &schemapb.VectorField_SparseFloatVector{SparseFloatVector: &schemapb.SparseFloatArray{
			Contents: [][]byte{{0x01}, {0x02}, {0x03}},
			Dim:      100,
		}},
	}
	sliced := rangeSliceVectorField(vf, 1, 3, nil)
	assert.Equal(t, [][]byte{{0x02}, {0x03}}, sliced.GetSparseFloatVector().GetContents())
}

// =========================================================================
// sliceVectorField: additional vector types
// =========================================================================

func TestSliceVectorField_FloatVector(t *testing.T) {
	vf := &schemapb.VectorField{
		Dim:  2,
		Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4, 5, 6}}},
	}
	sliced := sliceVectorField(vf, []int{2, 0}, nil)
	assert.Equal(t, []float32{5, 6, 1, 2}, sliced.GetFloatVector().GetData())
}

func TestSliceVectorField_BinaryVector(t *testing.T) {
	vf := &schemapb.VectorField{
		Dim:  16, // 2 bytes per row
		Data: &schemapb.VectorField_BinaryVector{BinaryVector: []byte{0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF}},
	}
	sliced := sliceVectorField(vf, []int{2, 0}, nil)
	assert.Equal(t, []byte{0xEE, 0xFF, 0xAA, 0xBB}, sliced.GetBinaryVector())
}

func TestSliceVectorField_Float16Vector(t *testing.T) {
	vf := &schemapb.VectorField{
		Dim:  2, // 4 bytes per row
		Data: &schemapb.VectorField_Float16Vector{Float16Vector: []byte{1, 2, 3, 4, 5, 6, 7, 8}},
	}
	sliced := sliceVectorField(vf, []int{1}, nil)
	assert.Equal(t, []byte{5, 6, 7, 8}, sliced.GetFloat16Vector())
}

func TestSliceVectorField_BFloat16Vector(t *testing.T) {
	vf := &schemapb.VectorField{
		Dim:  2, // 4 bytes per row
		Data: &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: []byte{10, 20, 30, 40, 50, 60, 70, 80}},
	}
	sliced := sliceVectorField(vf, []int{1, 0}, nil)
	assert.Equal(t, []byte{50, 60, 70, 80, 10, 20, 30, 40}, sliced.GetBfloat16Vector())
}

func TestSliceVectorField_Int8Vector(t *testing.T) {
	vf := &schemapb.VectorField{
		Dim:  3,
		Data: &schemapb.VectorField_Int8Vector{Int8Vector: []byte{1, 2, 3, 4, 5, 6}},
	}
	sliced := sliceVectorField(vf, []int{1}, nil)
	assert.Equal(t, []byte{4, 5, 6}, sliced.GetInt8Vector())
}

func TestSliceVectorField_SparseFloatVector(t *testing.T) {
	vf := &schemapb.VectorField{
		Dim: 100,
		Data: &schemapb.VectorField_SparseFloatVector{SparseFloatVector: &schemapb.SparseFloatArray{
			Contents: [][]byte{{0x01}, {0x02}, {0x03}},
			Dim:      100,
		}},
	}
	sliced := sliceVectorField(vf, []int{2, 0}, nil)
	assert.Equal(t, [][]byte{{0x03}, {0x01}}, sliced.GetSparseFloatVector().GetContents())
}

// =========================================================================
// rangeSliceFieldData: vector branch
// =========================================================================

func TestRangeSliceFieldData_Vector(t *testing.T) {
	fd := &schemapb.FieldData{
		Type: schemapb.DataType_FloatVector, FieldId: 100, FieldName: "vec",
		Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
			Dim:  2,
			Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4, 5, 6}}},
		}},
	}
	sliced := rangeSliceFieldData(fd, 1, 3)
	assert.Equal(t, []float32{3, 4, 5, 6}, sliced.GetVectors().GetFloatVector().GetData())
}

func TestRangeSliceFieldData_Nil(t *testing.T) {
	assert.Nil(t, rangeSliceFieldData(nil, 0, 1))
}

// =========================================================================
// calcFieldElementSize: various types
// =========================================================================

func TestCalcFieldElementSize(t *testing.T) {
	tests := []struct {
		name     string
		fd       *schemapb.FieldData
		rowIdx   int
		expected int64
	}{
		{
			name: "bool",
			fd: &schemapb.FieldData{Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: []bool{true}}},
			}}},
			expected: 1,
		},
		{
			name: "int32",
			fd: &schemapb.FieldData{Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{42}}},
			}}},
			expected: 4,
		},
		{
			name: "int64",
			fd: &schemapb.FieldData{Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{42}}},
			}}},
			expected: 8,
		},
		{
			name: "float32",
			fd: &schemapb.FieldData{Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: []float32{1.0}}},
			}}},
			expected: 4,
		},
		{
			name: "float64",
			fd: &schemapb.FieldData{Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: []float64{1.0}}},
			}}},
			expected: 8,
		},
		{
			name: "string",
			fd: &schemapb.FieldData{Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"hello"}}},
			}}},
			expected: 5,
		},
		{
			name: "json",
			fd: &schemapb.FieldData{Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{Data: [][]byte{[]byte(`{"a":1}`)}}},
			}}},
			expected: 7,
		},
		{
			name: "bytes",
			fd: &schemapb.FieldData{Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_BytesData{BytesData: &schemapb.BytesArray{Data: [][]byte{{0x01, 0x02, 0x03}}}},
			}}},
			expected: 3,
		},
		{
			name: "timestamptz",
			fd: &schemapb.FieldData{Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_TimestamptzData{TimestamptzData: &schemapb.TimestamptzArray{Data: []int64{1000}}},
			}}},
			expected: 8,
		},
		{
			name: "float_vector",
			fd: &schemapb.FieldData{Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Dim:  4,
				Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4}}},
			}}},
			expected: 16, // 4 * 4
		},
		{
			name: "binary_vector",
			fd: &schemapb.FieldData{Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Dim:  16,
				Data: &schemapb.VectorField_BinaryVector{BinaryVector: []byte{0, 0}},
			}}},
			expected: 2, // 16/8
		},
		{
			name: "float16_vector",
			fd: &schemapb.FieldData{Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Dim:  4,
				Data: &schemapb.VectorField_Float16Vector{Float16Vector: []byte{0, 0, 0, 0, 0, 0, 0, 0}},
			}}},
			expected: 8, // 4*2
		},
		{
			name: "int8_vector",
			fd: &schemapb.FieldData{Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Dim:  3,
				Data: &schemapb.VectorField_Int8Vector{Int8Vector: []byte{1, 2, 3}},
			}}},
			expected: 3,
		},
		{
			name: "sparse_float_vector",
			fd: &schemapb.FieldData{Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Dim: 100,
				Data: &schemapb.VectorField_SparseFloatVector{SparseFloatVector: &schemapb.SparseFloatArray{
					Contents: [][]byte{{0x01, 0x02, 0x03, 0x04}},
					Dim:      100,
				}},
			}}},
			expected: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, calcFieldElementSize(tt.fd, tt.rowIdx))
		})
	}
}

// =========================================================================
// getFieldValue: various types
// =========================================================================

func TestGetFieldValue(t *testing.T) {
	tests := []struct {
		name     string
		fd       *schemapb.FieldData
		rowIdx   int
		expected any
		isNull   bool
	}{
		{
			name: "bool",
			fd: &schemapb.FieldData{Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: []bool{true, false}}},
			}}},
			rowIdx: 1, expected: false,
		},
		{
			name: "int32",
			fd: &schemapb.FieldData{Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{10, 20}}},
			}}},
			rowIdx: 0, expected: int32(10),
		},
		{
			name: "float32",
			fd: &schemapb.FieldData{Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: []float32{1.5}}},
			}}},
			rowIdx: 0, expected: float32(1.5),
		},
		{
			name: "float64",
			fd: &schemapb.FieldData{Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: []float64{3.14}}},
			}}},
			rowIdx: 0, expected: float64(3.14),
		},
		{
			name: "nullable_null",
			fd: &schemapb.FieldData{
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{99}}},
				}},
				ValidData: []bool{false},
			},
			rowIdx: 0, expected: nil, isNull: true,
		},
		{
			name:   "nil_scalars",
			fd:     &schemapb.FieldData{},
			rowIdx: 0, expected: nil, isNull: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, isNull := getFieldValue(tt.fd, tt.rowIdx)
			assert.Equal(t, tt.isNull, isNull)
			if !tt.isNull {
				assert.Equal(t, tt.expected, val)
			}
		})
	}
}

// =========================================================================
// getRowCount: various types
// =========================================================================

func TestGetRowCount(t *testing.T) {
	tests := []struct {
		name     string
		result   *internalpb.RetrieveResults
		expected int
	}{
		{
			name:     "from IDs",
			result:   makeInternalResultIntPK([]int64{1, 2, 3}),
			expected: 3,
		},
		{
			name: "from bool field",
			result: &internalpb.RetrieveResults{FieldsData: []*schemapb.FieldData{
				{Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: []bool{true, false}}},
				}}},
			}},
			expected: 2,
		},
		{
			name: "from int32 field",
			result: &internalpb.RetrieveResults{FieldsData: []*schemapb.FieldData{
				{Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{1, 2, 3}}},
				}}},
			}},
			expected: 3,
		},
		{
			name: "from float field",
			result: &internalpb.RetrieveResults{FieldsData: []*schemapb.FieldData{
				{Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: []float32{1.0}}},
				}}},
			}},
			expected: 1,
		},
		{
			name: "from double field",
			result: &internalpb.RetrieveResults{FieldsData: []*schemapb.FieldData{
				{Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: []float64{1.0, 2.0}}},
				}}},
			}},
			expected: 2,
		},
		{
			name: "from json field",
			result: &internalpb.RetrieveResults{FieldsData: []*schemapb.FieldData{
				{Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{Data: [][]byte{[]byte(`{}`)}}},
				}}},
			}},
			expected: 1,
		},
		{
			name: "from float vector",
			result: &internalpb.RetrieveResults{FieldsData: []*schemapb.FieldData{
				{Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
					Dim:  2,
					Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4, 5, 6}}},
				}}},
			}},
			expected: 3,
		},
		{
			name: "from binary vector",
			result: &internalpb.RetrieveResults{FieldsData: []*schemapb.FieldData{
				{Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
					Dim:  16,
					Data: &schemapb.VectorField_BinaryVector{BinaryVector: []byte{0, 0, 0, 0}},
				}}},
			}},
			expected: 2,
		},
		{
			name: "from sparse float vector",
			result: &internalpb.RetrieveResults{FieldsData: []*schemapb.FieldData{
				{Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
					Dim: 100,
					Data: &schemapb.VectorField_SparseFloatVector{SparseFloatVector: &schemapb.SparseFloatArray{
						Contents: [][]byte{{0x01}, {0x02}},
					}},
				}}},
			}},
			expected: 2,
		},
		{
			name: "from struct array",
			result: &internalpb.RetrieveResults{FieldsData: []*schemapb.FieldData{
				makeStructArrayFieldData(100, "info", []*schemapb.FieldData{
					makeInt64Field(101, "age", []int64{10, 20, 30}),
				}),
			}},
			expected: 3,
		},
		{
			name:     "empty",
			result:   &internalpb.RetrieveResults{},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, getRowCount(tt.result))
		})
	}
}

// =========================================================================
// compareValues: various types
// =========================================================================

func TestCompareValues(t *testing.T) {
	tests := []struct {
		name     string
		a, b     any
		dataType schemapb.DataType
		expected int
	}{
		{"bool_lt", false, true, schemapb.DataType_Bool, -1},
		{"bool_gt", true, false, schemapb.DataType_Bool, 1},
		{"bool_eq", true, true, schemapb.DataType_Bool, 0},
		{"int32_lt", int32(1), int32(2), schemapb.DataType_Int32, -1},
		{"int32_gt", int32(5), int32(3), schemapb.DataType_Int32, 1},
		{"int32_eq", int32(7), int32(7), schemapb.DataType_Int32, 0},
		{"int64_lt", int64(10), int64(20), schemapb.DataType_Int64, -1},
		{"int64_eq", int64(5), int64(5), schemapb.DataType_Int64, 0},
		{"float32_lt", float32(1.0), float32(2.0), schemapb.DataType_Float, -1},
		{"float32_gt", float32(3.0), float32(1.0), schemapb.DataType_Float, 1},
		{"float32_eq", float32(1.5), float32(1.5), schemapb.DataType_Float, 0},
		{"float64_lt", float64(1.0), float64(2.0), schemapb.DataType_Double, -1},
		{"float64_gt", float64(9.0), float64(1.0), schemapb.DataType_Double, 1},
		{"float64_eq", float64(3.14), float64(3.14), schemapb.DataType_Double, 0},
		{"string_lt", "apple", "banana", schemapb.DataType_VarChar, -1},
		{"string_gt", "zebra", "alpha", schemapb.DataType_VarChar, 1},
		{"string_eq", "same", "same", schemapb.DataType_VarChar, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, compareValues(tt.a, tt.b, tt.dataType))
		})
	}
}

// =========================================================================
// rangeSliceScalarField / sliceScalarField: more type branches
// =========================================================================

func TestRangeSliceScalarField_AllTypes(t *testing.T) {
	tests := []struct {
		name  string
		sf    *schemapb.ScalarField
		check func(t *testing.T, sf *schemapb.ScalarField)
	}{
		{
			name: "bool",
			sf:   &schemapb.ScalarField{Data: &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: []bool{true, false, true}}}},
			check: func(t *testing.T, sf *schemapb.ScalarField) {
				assert.Equal(t, []bool{false}, sf.GetBoolData().GetData())
			},
		},
		{
			name: "int32",
			sf:   &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{10, 20, 30}}}},
			check: func(t *testing.T, sf *schemapb.ScalarField) {
				assert.Equal(t, []int32{20}, sf.GetIntData().GetData())
			},
		},
		{
			name: "float",
			sf:   &schemapb.ScalarField{Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: []float32{1.0, 2.0, 3.0}}}},
			check: func(t *testing.T, sf *schemapb.ScalarField) {
				assert.Equal(t, []float32{2.0}, sf.GetFloatData().GetData())
			},
		},
		{
			name: "double",
			sf:   &schemapb.ScalarField{Data: &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: []float64{1.0, 2.0, 3.0}}}},
			check: func(t *testing.T, sf *schemapb.ScalarField) {
				assert.Equal(t, []float64{2.0}, sf.GetDoubleData().GetData())
			},
		},
		{
			name: "bytes",
			sf:   &schemapb.ScalarField{Data: &schemapb.ScalarField_BytesData{BytesData: &schemapb.BytesArray{Data: [][]byte{{1}, {2}, {3}}}}},
			check: func(t *testing.T, sf *schemapb.ScalarField) {
				assert.Equal(t, [][]byte{{2}}, sf.GetBytesData().GetData())
			},
		},
		{
			name: "json",
			sf:   &schemapb.ScalarField{Data: &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{Data: [][]byte{[]byte(`{}`), []byte(`[]`), []byte(`""`)}}}},
			check: func(t *testing.T, sf *schemapb.ScalarField) {
				assert.Equal(t, [][]byte{[]byte(`[]`)}, sf.GetJsonData().GetData())
			},
		},
		{
			name: "timestamptz",
			sf:   &schemapb.ScalarField{Data: &schemapb.ScalarField_TimestamptzData{TimestamptzData: &schemapb.TimestamptzArray{Data: []int64{100, 200, 300}}}},
			check: func(t *testing.T, sf *schemapb.ScalarField) {
				assert.Equal(t, []int64{200}, sf.GetTimestamptzData().GetData())
			},
		},
		{
			name: "mol",
			sf:   &schemapb.ScalarField{Data: &schemapb.ScalarField_MolData{MolData: &schemapb.MolArray{Data: [][]byte{{0xAA}, {0xBB}, {0xCC}}}}},
			check: func(t *testing.T, sf *schemapb.ScalarField) {
				assert.Equal(t, [][]byte{{0xBB}}, sf.GetMolData().GetData())
			},
		},
	}

	for _, tt := range tests {
		t.Run("rangeSlice_"+tt.name, func(t *testing.T) {
			sliced := rangeSliceScalarField(tt.sf, 1, 2)
			tt.check(t, sliced)
		})
		t.Run("slice_"+tt.name, func(t *testing.T) {
			sliced := sliceScalarField(tt.sf, []int{1})
			tt.check(t, sliced)
		})
	}
}

// =========================================================================
// rangeSliceVectorField: more vector types
// =========================================================================

func TestRangeSliceVectorField_BFloat16(t *testing.T) {
	vf := &schemapb.VectorField{
		Dim:  2,
		Data: &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: []byte{10, 20, 30, 40, 50, 60, 70, 80}},
	}
	sliced := rangeSliceVectorField(vf, 1, 2, nil)
	assert.Equal(t, []byte{50, 60, 70, 80}, sliced.GetBfloat16Vector())
}

func TestRangeSliceVectorField_Int8(t *testing.T) {
	vf := &schemapb.VectorField{
		Dim:  3,
		Data: &schemapb.VectorField_Int8Vector{Int8Vector: []byte{1, 2, 3, 4, 5, 6}},
	}
	sliced := rangeSliceVectorField(vf, 0, 1, nil)
	assert.Equal(t, []byte{1, 2, 3}, sliced.GetInt8Vector())
}

// =========================================================================
// getRowCount: additional vector + scalar types
// =========================================================================

func TestGetRowCount_AdditionalTypes(t *testing.T) {
	tests := []struct {
		name     string
		result   *internalpb.RetrieveResults
		expected int
	}{
		{
			name: "string field",
			result: &internalpb.RetrieveResults{FieldsData: []*schemapb.FieldData{
				{Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"a", "b"}}},
				}}},
			}},
			expected: 2,
		},
		{
			name: "array field",
			result: &internalpb.RetrieveResults{FieldsData: []*schemapb.FieldData{
				{Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{Data: []*schemapb.ScalarField{nil, nil, nil}}},
				}}},
			}},
			expected: 3,
		},
		{
			name: "geometry wkt field",
			result: &internalpb.RetrieveResults{FieldsData: []*schemapb.FieldData{
				{Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_GeometryWktData{GeometryWktData: &schemapb.GeometryWktArray{Data: []string{"POINT(0 0)"}}},
				}}},
			}},
			expected: 1,
		},
		{
			name: "timestamptz field",
			result: &internalpb.RetrieveResults{FieldsData: []*schemapb.FieldData{
				{Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_TimestamptzData{TimestamptzData: &schemapb.TimestamptzArray{Data: []int64{1, 2}}},
				}}},
			}},
			expected: 2,
		},
		{
			name: "mol field",
			result: &internalpb.RetrieveResults{FieldsData: []*schemapb.FieldData{
				{Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_MolData{MolData: &schemapb.MolArray{Data: [][]byte{{1}}}},
				}}},
			}},
			expected: 1,
		},
		{
			name: "float16 vector",
			result: &internalpb.RetrieveResults{FieldsData: []*schemapb.FieldData{
				{Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
					Dim:  2,
					Data: &schemapb.VectorField_Float16Vector{Float16Vector: []byte{1, 2, 3, 4, 5, 6, 7, 8}},
				}}},
			}},
			expected: 2,
		},
		{
			name: "bfloat16 vector",
			result: &internalpb.RetrieveResults{FieldsData: []*schemapb.FieldData{
				{Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
					Dim:  2,
					Data: &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: []byte{1, 2, 3, 4}},
				}}},
			}},
			expected: 1,
		},
		{
			name: "int8 vector",
			result: &internalpb.RetrieveResults{FieldsData: []*schemapb.FieldData{
				{Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
					Dim:  3,
					Data: &schemapb.VectorField_Int8Vector{Int8Vector: []byte{1, 2, 3, 4, 5, 6}},
				}}},
			}},
			expected: 2,
		},
		{
			name: "vector array",
			result: &internalpb.RetrieveResults{FieldsData: []*schemapb.FieldData{
				{Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
					Dim: 2,
					Data: &schemapb.VectorField_VectorArray{VectorArray: &schemapb.VectorArray{
						Data: []*schemapb.VectorField{nil, nil},
					}},
				}}},
			}},
			expected: 2,
		},
		{
			name: "geometry field",
			result: &internalpb.RetrieveResults{FieldsData: []*schemapb.FieldData{
				{Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_GeometryData{GeometryData: &schemapb.GeometryArray{Data: [][]byte{{1}, {2}}}},
				}}},
			}},
			expected: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, getRowCount(tt.result))
		})
	}
}

// =========================================================================
// calcFieldElementSize: remaining branches (geometry, array, mol, vectorArray)
// =========================================================================

func TestCalcFieldElementSize_RemainingBranches(t *testing.T) {
	tests := []struct {
		name     string
		fd       *schemapb.FieldData
		rowIdx   int
		expected int64
	}{
		{
			name: "geometry_data",
			fd: &schemapb.FieldData{Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_GeometryData{GeometryData: &schemapb.GeometryArray{Data: [][]byte{{1, 2, 3}}}},
			}}},
			expected: 3,
		},
		{
			name: "geometry_wkt_data",
			fd: &schemapb.FieldData{Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_GeometryWktData{GeometryWktData: &schemapb.GeometryWktArray{Data: []string{"POINT(0 0)"}}},
			}}},
			expected: 10,
		},
		{
			name: "mol_data",
			fd: &schemapb.FieldData{Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_MolData{MolData: &schemapb.MolArray{Data: [][]byte{{0xAA, 0xBB}}}},
			}}},
			expected: 2,
		},
		{
			name: "array_data",
			fd: &schemapb.FieldData{Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
					Data: []*schemapb.ScalarField{
						{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{1, 2}}}},
					},
					ElementType: schemapb.DataType_Int32,
				}},
			}}},
			expected: 10, // proto.Size of the ScalarField
		},
		{
			name: "bfloat16_vector",
			fd: &schemapb.FieldData{Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Dim:  4,
				Data: &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: []byte{0, 0, 0, 0, 0, 0, 0, 0}},
			}}},
			expected: 8, // 4*2
		},
		{
			name: "vector_array",
			fd: &schemapb.FieldData{Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Dim: 2,
				Data: &schemapb.VectorField_VectorArray{VectorArray: &schemapb.VectorArray{
					Data: []*schemapb.VectorField{
						{Dim: 2, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2}}}},
					},
				}},
			}}},
		},
		{
			name:     "nil_field",
			fd:       &schemapb.FieldData{},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := calcFieldElementSize(tt.fd, tt.rowIdx)
			if tt.name == "vector_array" {
				assert.True(t, result > 0, "vector_array size should be > 0")
			} else if tt.name == "array_data" {
				assert.True(t, result > 0, "array_data size should be > 0")
			} else {
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

// =========================================================================
// sliceFieldData / rangeSliceFieldData: vector + structArray branches
// =========================================================================

func TestSliceFieldData_Vector(t *testing.T) {
	fd := &schemapb.FieldData{
		Type: schemapb.DataType_FloatVector, FieldId: 100,
		Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
			Dim:  2,
			Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4, 5, 6}}},
		}},
	}
	sliced := sliceFieldData(fd, []int{2, 0})
	assert.Equal(t, []float32{5, 6, 1, 2}, sliced.GetVectors().GetFloatVector().GetData())
}

func TestSliceFieldData_NilOrEmpty(t *testing.T) {
	assert.Nil(t, sliceFieldData(nil, []int{0}))
	assert.Nil(t, sliceFieldData(&schemapb.FieldData{}, nil))
}

func TestSliceFieldData_ValidData(t *testing.T) {
	fd := &schemapb.FieldData{
		Type: schemapb.DataType_Int64, FieldId: 100,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{10, 20, 30}}},
		}},
		ValidData: []bool{true, false, true},
	}
	sliced := sliceFieldData(fd, []int{2, 0})
	assert.Equal(t, []bool{true, true}, sliced.GetValidData())
}

func TestRangeSliceFieldData_ValidData(t *testing.T) {
	fd := &schemapb.FieldData{
		Type: schemapb.DataType_Int64, FieldId: 100,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{10, 20, 30}}},
		}},
		ValidData: []bool{true, false, true},
	}
	sliced := rangeSliceFieldData(fd, 0, 2)
	assert.Equal(t, []bool{true, false}, sliced.GetValidData())
}

// =========================================================================
// sliceScalarField: remaining branches (geometry, geometryWkt)
// =========================================================================

func TestSliceScalarField_GeometryData(t *testing.T) {
	sf := &schemapb.ScalarField{
		Data: &schemapb.ScalarField_GeometryData{GeometryData: &schemapb.GeometryArray{Data: [][]byte{{1}, {2}, {3}}}},
	}
	sliced := sliceScalarField(sf, []int{2, 0})
	assert.Equal(t, [][]byte{{3}, {1}}, sliced.GetGeometryData().GetData())
}

func TestSliceScalarField_GeometryWktData(t *testing.T) {
	sf := &schemapb.ScalarField{
		Data: &schemapb.ScalarField_GeometryWktData{GeometryWktData: &schemapb.GeometryWktArray{Data: []string{"P1", "P2", "P3"}}},
	}
	sliced := sliceScalarField(sf, []int{1})
	assert.Equal(t, []string{"P2"}, sliced.GetGeometryWktData().GetData())
}

// =========================================================================
// rangeSliceScalarField: remaining branches (geometry)
// =========================================================================

func TestRangeSliceScalarField_GeometryData(t *testing.T) {
	sf := &schemapb.ScalarField{
		Data: &schemapb.ScalarField_GeometryData{GeometryData: &schemapb.GeometryArray{Data: [][]byte{{1}, {2}, {3}}}},
	}
	sliced := rangeSliceScalarField(sf, 0, 2)
	assert.Equal(t, [][]byte{{1}, {2}}, sliced.GetGeometryData().GetData())
}

// =========================================================================
// sliceVectorField / rangeSliceVectorField: nullable compact mode branches
// =========================================================================

func TestSliceVectorField_NullableCompact(t *testing.T) {
	// 3 logical rows, but only 2 valid rows in compact data
	vf := &schemapb.VectorField{
		Dim:  2,
		Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4}}}, // 2 valid rows
	}
	validData := []bool{true, false, true} // row0=valid(data0), row1=null, row2=valid(data1)

	sliced := sliceVectorField(vf, []int{2, 0}, validData)
	assert.Equal(t, []float32{3, 4, 1, 2}, sliced.GetFloatVector().GetData())
}

func TestSliceVectorField_NullableCompact_BinaryVector(t *testing.T) {
	vf := &schemapb.VectorField{
		Dim:  16,                                                                               // 2 bytes per row
		Data: &schemapb.VectorField_BinaryVector{BinaryVector: []byte{0xAA, 0xBB, 0xCC, 0xDD}}, // 2 valid rows
	}
	validData := []bool{true, false, true}

	sliced := sliceVectorField(vf, []int{2, 0}, validData)
	assert.Equal(t, []byte{0xCC, 0xDD, 0xAA, 0xBB}, sliced.GetBinaryVector())
}

func TestSliceVectorField_NullableCompact_Float16(t *testing.T) {
	vf := &schemapb.VectorField{
		Dim:  2, // 4 bytes per row
		Data: &schemapb.VectorField_Float16Vector{Float16Vector: []byte{1, 2, 3, 4, 5, 6, 7, 8}},
	}
	validData := []bool{true, false, true}

	sliced := sliceVectorField(vf, []int{0}, validData)
	assert.Equal(t, []byte{1, 2, 3, 4}, sliced.GetFloat16Vector())
}

func TestSliceVectorField_NullableCompact_BFloat16(t *testing.T) {
	vf := &schemapb.VectorField{
		Dim:  2,
		Data: &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: []byte{10, 20, 30, 40, 50, 60, 70, 80}},
	}
	validData := []bool{true, false, true}

	sliced := sliceVectorField(vf, []int{2}, validData)
	assert.Equal(t, []byte{50, 60, 70, 80}, sliced.GetBfloat16Vector())
}

func TestSliceVectorField_NullableCompact_Int8(t *testing.T) {
	vf := &schemapb.VectorField{
		Dim:  3,
		Data: &schemapb.VectorField_Int8Vector{Int8Vector: []byte{1, 2, 3, 4, 5, 6}},
	}
	validData := []bool{true, false, true}

	sliced := sliceVectorField(vf, []int{2}, validData)
	assert.Equal(t, []byte{4, 5, 6}, sliced.GetInt8Vector())
}

func TestSliceVectorField_NullableCompact_Sparse(t *testing.T) {
	vf := &schemapb.VectorField{
		Dim: 100,
		Data: &schemapb.VectorField_SparseFloatVector{SparseFloatVector: &schemapb.SparseFloatArray{
			Contents: [][]byte{{0x01}, {0x02}}, // 2 valid rows
			Dim:      100,
		}},
	}
	validData := []bool{true, false, true}

	sliced := sliceVectorField(vf, []int{2}, validData)
	assert.Equal(t, [][]byte{{0x02}}, sliced.GetSparseFloatVector().GetContents())
}

func TestSliceVectorField_NullableCompact_VectorArray(t *testing.T) {
	dim := int64(2)
	v1 := &schemapb.VectorField{Dim: dim, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2}}}}
	v2 := &schemapb.VectorField{Dim: dim, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{3, 4}}}}

	vf := &schemapb.VectorField{
		Dim: dim,
		Data: &schemapb.VectorField_VectorArray{VectorArray: &schemapb.VectorArray{
			Dim:         dim,
			Data:        []*schemapb.VectorField{v1, v2},
			ElementType: schemapb.DataType_FloatVector,
		}},
	}
	validData := []bool{true, false, true}

	sliced := sliceVectorField(vf, []int{2}, validData)
	assert.Len(t, sliced.GetVectorArray().GetData(), 1)
	assert.Equal(t, []float32{3, 4}, sliced.GetVectorArray().GetData()[0].GetFloatVector().GetData())
}

func TestSliceVectorField_NullableCompact_SkipNull(t *testing.T) {
	// Select a null row — it should be skipped
	vf := &schemapb.VectorField{
		Dim:  2,
		Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2}}},
	}
	validData := []bool{true, false}

	sliced := sliceVectorField(vf, []int{1}, validData) // row 1 is null
	assert.Empty(t, sliced.GetFloatVector().GetData())
}

func TestRangeSliceVectorField_NullableCompact(t *testing.T) {
	// 3 logical rows: valid, null, valid → 2 data entries in compact mode
	vf := &schemapb.VectorField{
		Dim:  2,
		Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4}}},
	}
	validData := []bool{true, false, true}

	// Range [0, 2) = logical row 0 and 1. Row 0 is valid(data0), row 1 is null → only data0
	sliced := rangeSliceVectorField(vf, 0, 2, validData)
	assert.Equal(t, []float32{1, 2}, sliced.GetFloatVector().GetData())
}

func TestRangeSliceVectorField_NullableCompact_BinaryVector(t *testing.T) {
	vf := &schemapb.VectorField{
		Dim:  16,
		Data: &schemapb.VectorField_BinaryVector{BinaryVector: []byte{0xAA, 0xBB, 0xCC, 0xDD}},
	}
	validData := []bool{true, false, true}

	sliced := rangeSliceVectorField(vf, 0, 2, validData)
	assert.Equal(t, []byte{0xAA, 0xBB}, sliced.GetBinaryVector())
}

func TestRangeSliceVectorField_NullableCompact_Float16(t *testing.T) {
	vf := &schemapb.VectorField{
		Dim:  2,
		Data: &schemapb.VectorField_Float16Vector{Float16Vector: []byte{1, 2, 3, 4, 5, 6, 7, 8}},
	}
	validData := []bool{true, false, true}

	sliced := rangeSliceVectorField(vf, 2, 3, validData)
	assert.Equal(t, []byte{5, 6, 7, 8}, sliced.GetFloat16Vector())
}

func TestRangeSliceVectorField_NullableCompact_BFloat16(t *testing.T) {
	vf := &schemapb.VectorField{
		Dim:  2,
		Data: &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: []byte{10, 20, 30, 40, 50, 60, 70, 80}},
	}
	validData := []bool{true, false, true}

	sliced := rangeSliceVectorField(vf, 0, 1, validData)
	assert.Equal(t, []byte{10, 20, 30, 40}, sliced.GetBfloat16Vector())
}

func TestRangeSliceVectorField_NullableCompact_Int8(t *testing.T) {
	vf := &schemapb.VectorField{
		Dim:  3,
		Data: &schemapb.VectorField_Int8Vector{Int8Vector: []byte{1, 2, 3, 4, 5, 6}},
	}
	validData := []bool{true, false, true}

	sliced := rangeSliceVectorField(vf, 2, 3, validData)
	assert.Equal(t, []byte{4, 5, 6}, sliced.GetInt8Vector())
}

func TestRangeSliceVectorField_NullableCompact_Sparse(t *testing.T) {
	vf := &schemapb.VectorField{
		Dim: 100,
		Data: &schemapb.VectorField_SparseFloatVector{SparseFloatVector: &schemapb.SparseFloatArray{
			Contents: [][]byte{{0x01}, {0x02}},
			Dim:      100,
		}},
	}
	validData := []bool{true, false, true}

	sliced := rangeSliceVectorField(vf, 2, 3, validData)
	assert.Equal(t, [][]byte{{0x02}}, sliced.GetSparseFloatVector().GetContents())
}

func TestRangeSliceVectorField_NullableCompact_VectorArray(t *testing.T) {
	dim := int64(2)
	v1 := &schemapb.VectorField{Dim: dim, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2}}}}
	v2 := &schemapb.VectorField{Dim: dim, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{3, 4}}}}

	vf := &schemapb.VectorField{
		Dim: dim,
		Data: &schemapb.VectorField_VectorArray{VectorArray: &schemapb.VectorArray{
			Dim:         dim,
			Data:        []*schemapb.VectorField{v1, v2},
			ElementType: schemapb.DataType_FloatVector,
		}},
	}
	validData := []bool{true, false, true}

	sliced := rangeSliceVectorField(vf, 0, 1, validData)
	assert.Len(t, sliced.GetVectorArray().GetData(), 1)
	assert.Equal(t, []float32{1, 2}, sliced.GetVectorArray().GetData()[0].GetFloatVector().GetData())
}

// =========================================================================
// buildMergedVectorField: nullable compact mode — null row skip
// =========================================================================

func TestBuildMergedVectorField_NullableCompact_FloatVector(t *testing.T) {
	// FloatVector dim=2, nullable: row0=valid(compact idx 0), row1=null, row2=valid(compact idx 1)
	// Compact data: [1,2, 3,4] — 2 valid rows only
	r1 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}}}},
		FieldsData: []*schemapb.FieldData{
			{
				Type: schemapb.DataType_FloatVector, FieldId: 100,
				Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
					Dim:  2,
					Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4}}},
				}},
				ValidData: []bool{true, false, true},
			},
		},
	}
	// Include the null row (row1) in selectedRows to exercise the di < 0 skip path
	selectedRows := []rowRef{{resultIdx: 0, rowIdx: 2}, {resultIdx: 0, rowIdx: 1}, {resultIdx: 0, rowIdx: 0}}
	merged, err := buildMergedRetrieveResults([]*internalpb.RetrieveResults{r1}, selectedRows, makeNullableSchema(100, schemapb.DataType_FloatVector, 2))
	require.NoError(t, err)
	// row2 → compact idx 1 → [3,4], row1 → null (skipped), row0 → compact idx 0 → [1,2]
	assert.Equal(t, []float32{3, 4, 1, 2}, merged.FieldsData[0].GetVectors().GetFloatVector().GetData())
	assert.Equal(t, []bool{true, false, true}, merged.FieldsData[0].ValidData)
}

// =========================================================================
// getFieldValue: remaining branches (string, out-of-range)
// =========================================================================

func TestGetFieldValue_String(t *testing.T) {
	fd := &schemapb.FieldData{Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
		Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"hello", "world"}}},
	}}}
	val, isNull := getFieldValue(fd, 1)
	assert.False(t, isNull)
	assert.Equal(t, "world", val)
}

func TestGetFieldValue_OutOfRange(t *testing.T) {
	fd := &schemapb.FieldData{Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
		Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1}}},
	}}}
	_, isNull := getFieldValue(fd, 5) // out of range
	assert.True(t, isNull)
}

// =========================================================================
// rangeSliceRetrieveResults: element-level metadata propagation
// =========================================================================

func TestRangeSliceRetrieveResults_EmptyRange(t *testing.T) {
	result := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2}}}},
	}
	sliced := rangeSliceRetrieveResults(result, 1, 1) // start == end
	assert.Nil(t, sliced.GetIds())
}

func TestRangeSliceRetrieveResults_ElementLevel(t *testing.T) {
	result := &internalpb.RetrieveResults{
		Ids:          &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}}}},
		ElementLevel: true,
		ElementIndices: []*internalpb.ElementIndices{
			{Indices: []int32{0}},
			{Indices: []int32{1, 2}},
			{Indices: []int32{3}},
		},
		FieldsData: []*schemapb.FieldData{
			makeInt64Field(100, "val", []int64{10, 20, 30}),
		},
	}
	sliced := rangeSliceRetrieveResults(result, 1, 3)
	assert.True(t, sliced.GetElementLevel())
	assert.Len(t, sliced.GetElementIndices(), 2)
	assert.Equal(t, []int32{1, 2}, sliced.GetElementIndices()[0].GetIndices())
}

func TestValidateElementLevelConsistency_InconsistentFlag(t *testing.T) {
	r1 := &internalpb.RetrieveResults{
		ElementLevel:   true,
		Ids:            &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}},
		ElementIndices: []*internalpb.ElementIndices{{Indices: []int32{0}}},
	}
	r2 := &internalpb.RetrieveResults{
		ElementLevel: false,
		Ids:          &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{2}}}},
	}
	err := validateElementLevelConsistency([]*internalpb.RetrieveResults{r1, r2}, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "inconsistent element-level flag")
}

func TestValidateElementLevelConsistency_LengthMismatch(t *testing.T) {
	r := &internalpb.RetrieveResults{
		Ids:            &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2}}}},
		ElementLevel:   true,
		ElementIndices: []*internalpb.ElementIndices{{Indices: []int32{0}}}, // 1 != 2 ids
	}
	err := validateElementLevelConsistency([]*internalpb.RetrieveResults{r}, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "element_indices length")
}

// =========================================================================
// Nullable vector / scalar merge correctness tests
// =========================================================================

// makeTestIntIDs is a helper to build IDs for test results.
func makeTestIntIDs(ids ...int64) *schemapb.IDs {
	return &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: ids}}}
}

// makeTestSparseVec builds a knowhere sparse vector binary: [dim uint32][val float32], little-endian.
func makeTestSparseVec(dim uint32, val float32) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint32(b[0:4], dim)
	binary.LittleEndian.PutUint32(b[4:8], math.Float32bits(val))
	return b
}

// TestBuildMergedVectorField_NullableSparseVector_AllNull_EmptyValidData validates Bug 1 fix:
// when a nullable sparse vector field has empty ValidData + empty Contents, merge must not panic,
// and the merged output must correctly mark those rows as null (ValidData=false).
func TestBuildMergedVectorField_NullableSparse_RejectsEmptyValidData(t *testing.T) {
	sparseSchema := makeNullableSchema(100, schemapb.DataType_SparseFloatVector, 0)

	// rA: 1 row, has valid data
	rA := &internalpb.RetrieveResults{
		Ids: makeTestIntIDs(1),
		FieldsData: []*schemapb.FieldData{{
			FieldId: 100,
			Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Data: &schemapb.VectorField_SparseFloatVector{
					SparseFloatVector: &schemapb.SparseFloatArray{
						Contents: [][]byte{makeTestSparseVec(1, 0.5)},
					},
				},
			}},
			ValidData: []bool{true},
		}},
	}

	// rB: 1 row, ValidData absent, Contents absent (segcore get_vector early return).
	// This is now a contract violation: nullable field with numRows > 0 must have ValidData.
	// The old code silently treated this as "all null"; the new code fails fast.
	rB := &internalpb.RetrieveResults{
		Ids: makeTestIntIDs(2),
		FieldsData: []*schemapb.FieldData{{
			FieldId: 100,
			Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Data: &schemapb.VectorField_SparseFloatVector{
					SparseFloatVector: &schemapb.SparseFloatArray{Contents: nil},
				},
			}},
			ValidData: nil,
		}},
	}

	results := []*internalpb.RetrieveResults{rA, rB}
	selectedRows := []rowRef{{resultIdx: 0, rowIdx: 0}, {resultIdx: 1, rowIdx: 0}}

	_, err := buildMergedRetrieveResults(results, selectedRows, sparseSchema)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty ValidData")
}

// TestBuildMergedVectorField_NullableSparse_RejectsMultipleRowsWithEmptyValidData validates that
// multiple rows from a result with absent ValidData trigger fail-fast error.
func TestBuildMergedVectorField_NullableSparse_RejectsMultipleRowsWithEmptyValidData(t *testing.T) {
	sparseSchema := makeNullableSchema(100, schemapb.DataType_SparseFloatVector, 0)

	rA := &internalpb.RetrieveResults{
		Ids: makeTestIntIDs(1),
		FieldsData: []*schemapb.FieldData{{
			FieldId: 100,
			Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Data: &schemapb.VectorField_SparseFloatVector{
					SparseFloatVector: &schemapb.SparseFloatArray{
						Contents: [][]byte{makeTestSparseVec(1, 0.1)},
					},
				},
			}},
			ValidData: []bool{true},
		}},
	}

	rB := &internalpb.RetrieveResults{
		Ids: makeTestIntIDs(2, 3),
		FieldsData: []*schemapb.FieldData{{
			FieldId: 100,
			Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Data: &schemapb.VectorField_SparseFloatVector{
					SparseFloatVector: &schemapb.SparseFloatArray{Contents: nil},
				},
			}},
			ValidData: nil,
		}},
	}

	results := []*internalpb.RetrieveResults{rA, rB}
	selectedRows := []rowRef{
		{resultIdx: 0, rowIdx: 0},
		{resultIdx: 1, rowIdx: 0},
		{resultIdx: 1, rowIdx: 1},
	}

	// rB has 2 rows but no ValidData → contract violation, must error
	_, err := buildMergedRetrieveResults(results, selectedRows, sparseSchema)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty ValidData")
}

// TestBuildMergedVectorField_NullableSparseVector_AllNullWithValidData validates compact path
// when ValidData is present and all false (explicit all-null).
func TestBuildMergedVectorField_NullableSparseVector_AllNullWithValidData(t *testing.T) {
	sparseSchema := makeNullableSchema(100, schemapb.DataType_SparseFloatVector, 0)
	r := &internalpb.RetrieveResults{
		Ids: makeTestIntIDs(1, 2),
		FieldsData: []*schemapb.FieldData{{
			FieldId: 100,
			Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Data: &schemapb.VectorField_SparseFloatVector{
					SparseFloatVector: &schemapb.SparseFloatArray{Contents: nil},
				},
			}},
			ValidData: []bool{false, false},
		}},
	}
	results := []*internalpb.RetrieveResults{r}
	selectedRows := []rowRef{{resultIdx: 0, rowIdx: 0}, {resultIdx: 0, rowIdx: 1}}

	merged, err := buildMergedRetrieveResults(results, selectedRows, sparseSchema)
	require.NoError(t, err)
	assert.Equal(t, []bool{false, false}, merged.FieldsData[0].ValidData)
	assert.Empty(t, merged.FieldsData[0].GetVectors().GetSparseFloatVector().GetContents())
}

// TestBuildMergedVectorField_NullableSparseVector_Mixed validates compact index mapping when
// some rows are valid and some are null within the same result.
func TestBuildMergedVectorField_NullableSparseVector_Mixed(t *testing.T) {
	sparseSchema := makeNullableSchema(100, schemapb.DataType_SparseFloatVector, 0)
	// 3 rows: ValidData=[true,false,true], Contents=[vec0, vec2] (compact)
	r := &internalpb.RetrieveResults{
		Ids: makeTestIntIDs(1, 2, 3),
		FieldsData: []*schemapb.FieldData{{
			FieldId: 100,
			Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Data: &schemapb.VectorField_SparseFloatVector{
					SparseFloatVector: &schemapb.SparseFloatArray{
						Contents: [][]byte{makeTestSparseVec(1, 0.1), makeTestSparseVec(3, 0.3)},
					},
				},
			}},
			ValidData: []bool{true, false, true},
		}},
	}
	results := []*internalpb.RetrieveResults{r}
	// Select row 0 (valid) and row 1 (null)
	selectedRows := []rowRef{{resultIdx: 0, rowIdx: 0}, {resultIdx: 0, rowIdx: 1}}

	merged, err := buildMergedRetrieveResults(results, selectedRows, sparseSchema)
	require.NoError(t, err)
	assert.Equal(t, []bool{true, false}, merged.FieldsData[0].ValidData)
	// Only 1 non-null row in output
	assert.Len(t, merged.FieldsData[0].GetVectors().GetSparseFloatVector().GetContents(), 1)
}

// TestBuildMergedFieldData_NullableScalar_EmptyValidData_FailFast validates that
// a nullable field with rows but absent ValidData triggers a fail-fast error,
// not a silent fallback to all-null. This catches segcore contract violations
// early instead of letting them propagate to downstream panics.
func TestBuildMergedVectorField_NullableVector_RejectsEmptyValidData(t *testing.T) {
	// Note: buildCompactIndices only validates vector fields. For scalar fields,
	// the ValidData check is still lenient (scalar fields are not compacted).
	// This test uses a vector schema to trigger the vector-path validation.
	sparseSchema := makeNullableSchema(100, schemapb.DataType_SparseFloatVector, 0)

	rA := &internalpb.RetrieveResults{
		Ids: makeTestIntIDs(1),
		FieldsData: []*schemapb.FieldData{{
			FieldId: 100,
			Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Data: &schemapb.VectorField_SparseFloatVector{
					SparseFloatVector: &schemapb.SparseFloatArray{
						Contents: [][]byte{makeTestSparseVec(1, 0.5)},
					},
				},
			}},
			ValidData: []bool{true},
		}},
	}
	// rB: ValidData absent → contract violation for nullable vector
	rB := &internalpb.RetrieveResults{
		Ids: makeTestIntIDs(2),
		FieldsData: []*schemapb.FieldData{{
			FieldId: 100,
			Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Data: &schemapb.VectorField_SparseFloatVector{
					SparseFloatVector: &schemapb.SparseFloatArray{Contents: nil},
				},
			}},
			ValidData: nil,
		}},
	}

	results := []*internalpb.RetrieveResults{rA, rB}
	selectedRows := []rowRef{{resultIdx: 0, rowIdx: 0}, {resultIdx: 1, rowIdx: 0}}

	_, err := buildMergedRetrieveResults(results, selectedRows, sparseSchema)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty ValidData")
}
