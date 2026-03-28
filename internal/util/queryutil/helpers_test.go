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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
)

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

	merged, err := buildMergedRetrieveResults(results, selectedRows)
	require.NoError(t, err)
	require.Len(t, merged.FieldsData, 1)

	fd := merged.FieldsData[0]
	assert.Equal(t, []bool{true, false, true}, fd.ValidData)
	assert.Equal(t, []int64{10, 20, 30}, fd.GetScalars().GetLongData().GetData())
}

func TestBuildMergedFieldData_ValidData_MixedNullable(t *testing.T) {
	// r1 has ValidData (nullable), r2 does not (non-nullable)
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
				// No ValidData — non-nullable
			},
		},
	}

	results := []*internalpb.RetrieveResults{r1, r2}
	selectedRows := []rowRef{
		{resultIdx: 0, rowIdx: 0}, // r1 (null)
		{resultIdx: 1, rowIdx: 0}, // r2 (no ValidData → valid=true)
	}

	merged, err := buildMergedRetrieveResults(results, selectedRows)
	require.NoError(t, err)

	fd := merged.FieldsData[0]
	// r1 row is null, r2 row has no ValidData so defaults to true
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

	merged, err := buildMergedRetrieveResults(results, selectedRows)
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

	merged, err := buildMergedRetrieveResults(results, selectedRows)
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

	merged, err := buildMergedRetrieveResults(results, selectedRows)
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

	merged, err := buildMergedRetrieveResults(results, selectedRows)
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

	merged, err := buildMergedRetrieveResults(results, selectedRows)
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

	merged, err := buildMergedRetrieveResults(results, selectedRows)
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

	merged, err := buildMergedRetrieveResults(results, selectedRows)
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
	op := NewConcatAndCheckPKOperator()
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
	op := NewConcatAndCheckPKOperator()
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
	merged, err := buildMergedRetrieveResults([]*internalpb.RetrieveResults{r1, r2}, selectedRows)
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
	merged, err := buildMergedRetrieveResults([]*internalpb.RetrieveResults{r1, r2}, selectedRows)
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

// =========================================================================
// buildMergedStructArrayField
// =========================================================================

// makeStructArrayFieldData creates a FieldData wrapping a StructArrayField with scalar sub-fields.
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

func TestBuildMergedStructArrayField_ScalarSubFields(t *testing.T) {
	// Two results, each with a struct array field containing two scalar sub-fields:
	// sub-field 0: int64 "age", sub-field 1: string "name"
	r1 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2}}}},
		FieldsData: []*schemapb.FieldData{
			makeStructArrayFieldData(100, "info", []*schemapb.FieldData{
				makeInt64Field(101, "age", []int64{25, 30}),
				makeStringField(102, "name", []string{"alice", "bob"}),
			}),
		},
	}
	r2 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{3}}}},
		FieldsData: []*schemapb.FieldData{
			makeStructArrayFieldData(100, "info", []*schemapb.FieldData{
				makeInt64Field(101, "age", []int64{35}),
				makeStringField(102, "name", []string{"charlie"}),
			}),
		},
	}

	results := []*internalpb.RetrieveResults{r1, r2}
	// Select: r1 row1, r2 row0, r1 row0
	selectedRows := []rowRef{
		{resultIdx: 0, rowIdx: 1},
		{resultIdx: 1, rowIdx: 0},
		{resultIdx: 0, rowIdx: 0},
	}

	merged := buildMergedStructArrayField(results, selectedRows, 0)
	require.Len(t, merged.GetFields(), 2)

	// Sub-field 0: age
	ages := merged.GetFields()[0].GetScalars().GetLongData().GetData()
	assert.Equal(t, []int64{30, 35, 25}, ages)

	// Sub-field 1: name
	names := merged.GetFields()[1].GetScalars().GetStringData().GetData()
	assert.Equal(t, []string{"bob", "charlie", "alice"}, names)
}

func TestBuildMergedStructArrayField_WithValidData(t *testing.T) {
	// Struct array sub-field with ValidData (nullable sub-field)
	r1 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2}}}},
		FieldsData: []*schemapb.FieldData{
			makeStructArrayFieldData(100, "info", []*schemapb.FieldData{
				{
					Type: schemapb.DataType_Int64, FieldId: 101, FieldName: "age",
					Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{25, 30}}},
					}},
					ValidData: []bool{true, false}, // row 1 is null
				},
			}),
		},
	}

	results := []*internalpb.RetrieveResults{r1}
	selectedRows := []rowRef{
		{resultIdx: 0, rowIdx: 0},
		{resultIdx: 0, rowIdx: 1},
	}

	merged := buildMergedStructArrayField(results, selectedRows, 0)
	require.Len(t, merged.GetFields(), 1)

	subFd := merged.GetFields()[0]
	assert.Equal(t, []int64{25, 30}, subFd.GetScalars().GetLongData().GetData())
	assert.Equal(t, []bool{true, false}, subFd.GetValidData())
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

	merged, err := buildMergedRetrieveResults(results, selectedRows)
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
// buildMergedStructArrayField with vector sub-field
// =========================================================================

func TestBuildMergedStructArrayField_VectorSubField(t *testing.T) {
	dim := int64(2)
	r1 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2}}}},
		FieldsData: []*schemapb.FieldData{
			makeStructArrayFieldData(100, "info", []*schemapb.FieldData{
				makeInt64Field(101, "id", []int64{10, 20}),
				{
					Type: schemapb.DataType_FloatVector, FieldId: 102, FieldName: "vec",
					Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
						Dim:  dim,
						Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4}}},
					}},
				},
			}),
		},
	}

	results := []*internalpb.RetrieveResults{r1}
	selectedRows := []rowRef{{resultIdx: 0, rowIdx: 1}} // select row 1 only

	merged := buildMergedStructArrayField(results, selectedRows, 0)
	require.Len(t, merged.GetFields(), 2)

	// Sub-field 0: scalar id
	assert.Equal(t, []int64{20}, merged.GetFields()[0].GetScalars().GetLongData().GetData())

	// Sub-field 1: vector
	vecData := merged.GetFields()[1].GetVectors().GetFloatVector().GetData()
	assert.Equal(t, []float32{3, 4}, vecData)
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
	merged, err := buildMergedRetrieveResults([]*internalpb.RetrieveResults{r1}, selectedRows)
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
	merged, err := buildMergedRetrieveResults([]*internalpb.RetrieveResults{r1}, selectedRows)
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
	merged, err := buildMergedRetrieveResults([]*internalpb.RetrieveResults{r1}, selectedRows)
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
	merged, err := buildMergedRetrieveResults([]*internalpb.RetrieveResults{r1}, selectedRows)
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
	merged, err := buildMergedRetrieveResults([]*internalpb.RetrieveResults{r1}, selectedRows)
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
	merged, err := buildMergedRetrieveResults([]*internalpb.RetrieveResults{r1}, selectedRows)
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
	merged, err := buildMergedRetrieveResults([]*internalpb.RetrieveResults{r1}, selectedRows)
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
	merged, err := buildMergedRetrieveResults([]*internalpb.RetrieveResults{r1}, selectedRows)
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
	merged, err := buildMergedRetrieveResults([]*internalpb.RetrieveResults{r1}, selectedRows)
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
	merged, err := buildMergedRetrieveResults([]*internalpb.RetrieveResults{r1}, selectedRows)
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
	merged, err := buildMergedRetrieveResults([]*internalpb.RetrieveResults{r1}, selectedRows)
	require.NoError(t, err)
	assert.Equal(t, [][]byte{{0x03, 0x04}}, merged.FieldsData[0].GetVectors().GetSparseFloatVector().GetContents())
}

// =========================================================================
// buildCompactIndices / getVecDataIdx: nullable vector
// =========================================================================

func TestBuildCompactIndices_NullableVector(t *testing.T) {
	r1 := &internalpb.RetrieveResults{
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
	indices := buildCompactIndices([]*internalpb.RetrieveResults{r1}, 0)
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
	indices := buildCompactIndices([]*internalpb.RetrieveResults{r1}, 0)
	assert.Nil(t, indices) // non-nullable → nil
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
	selectedRows := []rowRef{{resultIdx: 0, rowIdx: 2}, {resultIdx: 0, rowIdx: 0}}
	merged, err := buildMergedRetrieveResults([]*internalpb.RetrieveResults{r1}, selectedRows)
	require.NoError(t, err)
	assert.Equal(t, []float32{3, 4, 1, 2}, merged.FieldsData[0].GetVectors().GetFloatVector().GetData())
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
