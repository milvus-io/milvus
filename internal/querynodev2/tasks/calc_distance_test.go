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

package tasks

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func floatFieldData(fieldID int64, name string, dim int64, rows []float32) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_FloatVector,
		FieldId:   fieldID,
		FieldName: name,
		Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
			Dim: dim,
			Data: &schemapb.VectorField_FloatVector{
				FloatVector: &schemapb.FloatArray{Data: rows},
			},
		}},
	}
}

func rawBytesFieldData(fieldID int64, name string, dataType schemapb.DataType, dim int64, data []byte) *schemapb.FieldData {
	var vd schemapb.VectorField_Data
	switch dataType {
	case schemapb.DataType_Int8Vector:
		vd = &schemapb.VectorField_Int8Vector{Int8Vector: data}
	case schemapb.DataType_Float16Vector:
		vd = &schemapb.VectorField_Float16Vector{Float16Vector: data}
	case schemapb.DataType_BFloat16Vector:
		vd = &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: data}
	case schemapb.DataType_BinaryVector:
		vd = &schemapb.VectorField_BinaryVector{BinaryVector: data}
	default:
		panic("unsupported raw byte vector type")
	}
	return &schemapb.FieldData{
		Type:      dataType,
		FieldId:   fieldID,
		FieldName: name,
		Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
			Dim:  dim,
			Data: vd,
		}},
	}
}

func sparseFieldData(fieldID int64, name string, contents [][]byte) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_SparseFloatVector,
		FieldId:   fieldID,
		FieldName: name,
		Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
			Data: &schemapb.VectorField_SparseFloatVector{
				SparseFloatVector: &schemapb.SparseFloatArray{Contents: contents},
			},
		}},
	}
}

func newRetrieveResult(fieldsData ...*schemapb.FieldData) *segcorepb.RetrieveResults {
	return &segcorepb.RetrieveResults{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{IntId: &schemapb.IDArray{Data: []int64{1, 2}}},
		},
		FieldsData: fieldsData,
	}
}

// distanceColumn asserts the appended synthetic column and returns its values.
func distanceColumn(t *testing.T, result *segcorepb.RetrieveResults, name string) []float32 {
	t.Helper()
	require.NotEmpty(t, result.GetFieldsData())
	col := result.GetFieldsData()[len(result.GetFieldsData())-1]
	require.Equal(t, schemapb.DataType_Float, col.GetType())
	require.Equal(t, reservedFieldID, col.GetFieldId())
	require.Equal(t, name, col.GetFieldName())
	return col.GetScalars().GetFloatData().GetData()
}

func TestCalcDistanceColumn_FloatVector(t *testing.T) {
	const fieldID int64 = 101
	const dim int64 = 4
	ref := []float32{1, 2, 3, 4}
	rows := []float32{1, 2, 3, 4, 5, 6, 7, 8}

	run := func(metric string) []float32 {
		result := newRetrieveResult(floatFieldData(fieldID, "vec", dim, rows))
		err := calcDistanceColumn(result, typeutil.Float32ArrayToBytes(ref), schemapb.DataType_FloatVector,
			metric, fieldID, "distance", []int64{fieldID}, dim)
		require.NoError(t, err)
		return distanceColumn(t, result, "distance")
	}

	// L2: [0, 64]
	assert.Equal(t, []float32{0, 64}, run("L2"))
	// IP: [30, 70]
	assert.Equal(t, []float32{30, 70}, run("IP"))
	// COSINE: [1, 70 / sqrt(30*174)]
	cos := run("COSINE")
	require.Len(t, cos, 2)
	assert.Equal(t, float32(1), cos[0])
	assert.InEpsilon(t, 70/float32(math.Sqrt(30*174)), cos[1], 1e-5)

	// unknown metric
	err := calcDistanceColumn(newRetrieveResult(floatFieldData(fieldID, "vec", dim, rows)),
		typeutil.Float32ArrayToBytes(ref), schemapb.DataType_FloatVector, "L1", fieldID, "distance", nil, dim)
	require.Error(t, err)
}

func TestCalcDistanceColumn_DropsVectorColumn(t *testing.T) {
	const fieldID int64 = 101
	const dim int64 = 2
	ref := []float32{1, 1}
	rows := []float32{1, 1, 2, 2}

	// user did not request the vector field: it must be dropped, leaving only
	// the synthetic distance column
	result := newRetrieveResult(floatFieldData(fieldID, "vec", dim, rows))
	err := calcDistanceColumn(result, typeutil.Float32ArrayToBytes(ref), schemapb.DataType_FloatVector,
		"IP", fieldID, "distance", []int64{100}, dim)
	require.NoError(t, err)
	require.Len(t, result.GetFieldsData(), 1)
	assert.Equal(t, []float32{2, 8}, distanceColumn(t, result, "distance"))

	// user requested the vector field: both stay, distance last
	result = newRetrieveResult(floatFieldData(fieldID, "vec", dim, rows))
	err = calcDistanceColumn(result, typeutil.Float32ArrayToBytes(ref), schemapb.DataType_FloatVector,
		"IP", fieldID, "distance", []int64{fieldID}, dim)
	require.NoError(t, err)
	require.Len(t, result.GetFieldsData(), 2)
	assert.Equal(t, fieldID, result.GetFieldsData()[0].GetFieldId())
	assert.Equal(t, []float32{2, 8}, distanceColumn(t, result, "distance"))
}

func TestCalcDistanceColumn_DenseByteTypes(t *testing.T) {
	const fieldID int64 = 101
	const dim int64 = 4
	refF32 := []float32{1, 2, 3, 4}
	rowsF32 := []float32{1, 2, 3, 4, 5, 6, 7, 8}

	cases := []struct {
		dataType schemapb.DataType
		refBytes []byte
		rowBytes []byte
		wantIP   []float32
	}{
		{
			dataType: schemapb.DataType_Int8Vector,
			refBytes: typeutil.Int8ArrayToBytes([]int8{1, 2, 3, 4}),
			rowBytes: []byte{1, 2, 3, 4, 5, 6, 7, 8},
			wantIP:   []float32{30, 70},
		},
		{
			dataType: schemapb.DataType_Float16Vector,
			refBytes: typeutil.Float32ArrayToFloat16Bytes(refF32),
			rowBytes: typeutil.Float32ArrayToFloat16Bytes(rowsF32),
			wantIP:   []float32{30, 70},
		},
		{
			dataType: schemapb.DataType_BFloat16Vector,
			refBytes: typeutil.Float32ArrayToBFloat16Bytes(refF32),
			rowBytes: typeutil.Float32ArrayToBFloat16Bytes(rowsF32),
			wantIP:   []float32{30, 70},
		},
	}
	for _, tc := range cases {
		t.Run(tc.dataType.String(), func(t *testing.T) {
			result := newRetrieveResult(rawBytesFieldData(fieldID, "vec", tc.dataType, dim, tc.rowBytes))
			err := calcDistanceColumn(result, tc.refBytes, tc.dataType, "IP", fieldID, "distance", nil, dim)
			require.NoError(t, err)
			assert.Equal(t, tc.wantIP, distanceColumn(t, result, "distance"))
		})
	}
}

func TestCalcDistanceColumn_BinaryVector(t *testing.T) {
	const fieldID int64 = 101
	const dim int64 = 16
	// ref = 0b00001111 0b10101010
	ref := []byte{0b00001111, 0b10101010}
	// two rows: identical, and all-ones/all-bit-flipped
	rows := []byte{0b00001111, 0b10101010, 0b11111111, 0b01010101}

	result := newRetrieveResult(rawBytesFieldData(fieldID, "vec", schemapb.DataType_BinaryVector, dim, rows))
	err := calcDistanceColumn(result, ref, schemapb.DataType_BinaryVector, "HAMMING", fieldID, "distance", nil, dim)
	require.NoError(t, err)
	// hamming(ref, row0) = 0
	// hamming(ref, row1) = popcount(0x0f^0xff)=4 + popcount(0xaa^0x55)=8 = 12
	assert.Equal(t, []float32{0, 12}, distanceColumn(t, result, "distance"))
}

func TestCalcDistanceColumn_SparseVector(t *testing.T) {
	const fieldID int64 = 101
	// ref = {0:1, 2:2, 5:3}
	ref := typeutil.CreateSparseFloatRow([]uint32{0, 2, 5}, []float32{1, 2, 3})
	contents := [][]byte{
		typeutil.CreateSparseFloatRow([]uint32{0, 2, 5}, []float32{1, 2, 3}),
		typeutil.CreateSparseFloatRow([]uint32{2, 7}, []float32{2, 10}),
		typeutil.CreateSparseFloatRow([]uint32{100}, []float32{5}),
	}

	result := newRetrieveResult(sparseFieldData(fieldID, "vec", contents))
	err := calcDistanceColumn(result, ref, schemapb.DataType_SparseFloatVector, "IP", fieldID, "distance", nil, 0)
	require.NoError(t, err)
	assert.Equal(t, []float32{14, 4, 0}, distanceColumn(t, result, "distance"))

	result = newRetrieveResult(sparseFieldData(fieldID, "vec", contents))
	err = calcDistanceColumn(result, ref, schemapb.DataType_SparseFloatVector, "COSINE", fieldID, "distance", nil, 0)
	require.NoError(t, err)
	distances := distanceColumn(t, result, "distance")
	normA := float32(math.Sqrt(1*1 + 2*2 + 3*3))
	require.Len(t, distances, 3)
	assert.InEpsilon(t, 14/(normA*normA), distances[0], 1e-5)
	assert.InEpsilon(t, 4/(normA*float32(math.Sqrt(4+100))), distances[1], 1e-5)
	assert.Equal(t, float32(0), distances[2])
}

func TestCalcDistanceColumn_VectorFieldNotFound(t *testing.T) {
	err := calcDistanceColumn(newRetrieveResult(floatFieldData(101, "vec", 2, []float32{1, 2})),
		typeutil.Float32ArrayToBytes([]float32{1, 1}), schemapb.DataType_FloatVector,
		"IP", 999, "distance", nil, 2)
	require.Error(t, err)
}

func TestCalcDistances_UnsupportedType(t *testing.T) {
	_, err := calcDistances(nil, schemapb.DataType_VarChar, "IP",
		&schemapb.VectorField{}, 0)
	require.Error(t, err)
}
