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

package testutils

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"strings"

	"github.com/x448/float16"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

const elemCountOfArray = 10

// generate data
func GenerateBoolArray(numRows int) []bool {
	ret := make([]bool, 0, numRows)
	for i := 0; i < numRows; i++ {
		ret = append(ret, i%2 == 0)
	}
	return ret
}

func GenerateInt8Array(numRows int) []int8 {
	ret := make([]int8, 0, numRows)
	for i := 0; i < numRows; i++ {
		ret = append(ret, int8(i))
	}
	return ret
}

func GenerateInt16Array(numRows int) []int16 {
	ret := make([]int16, 0, numRows)
	for i := 0; i < numRows; i++ {
		ret = append(ret, int16(i))
	}
	return ret
}

func GenerateInt32Array(numRows int) []int32 {
	ret := make([]int32, 0, numRows)
	for i := 0; i < numRows; i++ {
		ret = append(ret, int32(i))
	}
	return ret
}

func GenerateInt64Array(numRows int) []int64 {
	ret := make([]int64, 0, numRows)
	for i := 0; i < numRows; i++ {
		ret = append(ret, int64(i))
	}
	return ret
}

func GenerateUint64Array(numRows int) []uint64 {
	ret := make([]uint64, 0, numRows)
	for i := 0; i < numRows; i++ {
		ret = append(ret, uint64(i))
	}
	return ret
}

func GenerateFloat32Array(numRows int) []float32 {
	ret := make([]float32, 0, numRows)
	for i := 0; i < numRows; i++ {
		ret = append(ret, float32(i))
	}
	return ret
}

func GenerateFloat64Array(numRows int) []float64 {
	ret := make([]float64, 0, numRows)
	for i := 0; i < numRows; i++ {
		ret = append(ret, float64(i))
	}
	return ret
}

func GenerateVarCharArray(numRows int, maxLen int) []string {
	ret := make([]string, numRows)
	for i := 0; i < numRows; i++ {
		ret[i] = funcutil.RandomString(rand.Intn(maxLen))
	}
	return ret
}

func GenerateStringArray(numRows int) []string {
	ret := make([]string, 0, numRows)

	genSentence := func() string {
		words := []string{"hello", "world", "this", "is", "a", "test", "sentence", "milvus", "vector", "database", "search", "engine", "fast", "efficient", "scalable"}
		selectedWords := make([]string, rand.Intn(6)+5) // 5 to 10 words
		for i := range selectedWords {
			selectedWords[i] = words[rand.Intn(len(words))]
		}
		rand.Shuffle(len(selectedWords), func(i, j int) {
			selectedWords[i], selectedWords[j] = selectedWords[j], selectedWords[i]
		})
		return strings.Join(selectedWords, " ")
	}

	for i := 0; i < numRows; i++ {
		ret = append(ret, genSentence())
	}
	return ret
}

func GenerateJSONArray(numRows int) [][]byte {
	ret := make([][]byte, 0, numRows)
	for i := 0; i < numRows; i++ {
		if i%4 == 0 {
			v, _ := json.Marshal("{\"a\": \"%s\", \"b\": %d}")
			ret = append(ret, v)
		} else if i%4 == 1 {
			v, _ := json.Marshal(i)
			ret = append(ret, v)
		} else if i%4 == 2 {
			v, _ := json.Marshal(float32(i) * 0.1)
			ret = append(ret, v)
		} else if i%4 == 3 {
			v, _ := json.Marshal(strconv.Itoa(i))
			ret = append(ret, v)
		}
	}
	return ret
}

func GenerateArrayOfBoolArray(numRows int) []*schemapb.ScalarField {
	ret := make([]*schemapb.ScalarField, 0, numRows)
	for i := 0; i < numRows; i++ {
		ret = append(ret, &schemapb.ScalarField{
			Data: &schemapb.ScalarField_BoolData{
				BoolData: &schemapb.BoolArray{
					Data: GenerateBoolArray(elemCountOfArray),
				},
			},
		})
	}
	return ret
}

func GenerateArrayOfIntArray(numRows int) []*schemapb.ScalarField {
	ret := make([]*schemapb.ScalarField, 0, numRows)
	for i := 0; i < numRows; i++ {
		ret = append(ret, &schemapb.ScalarField{
			Data: &schemapb.ScalarField_IntData{
				IntData: &schemapb.IntArray{
					Data: GenerateInt32Array(elemCountOfArray),
				},
			},
		})
	}
	return ret
}

func GenerateArrayOfLongArray(numRows int) []*schemapb.ScalarField {
	ret := make([]*schemapb.ScalarField, 0, numRows)
	for i := 0; i < numRows; i++ {
		ret = append(ret, &schemapb.ScalarField{
			Data: &schemapb.ScalarField_LongData{
				LongData: &schemapb.LongArray{
					Data: GenerateInt64Array(elemCountOfArray),
				},
			},
		})
	}
	return ret
}

func GenerateArrayOfFloatArray(numRows int) []*schemapb.ScalarField {
	ret := make([]*schemapb.ScalarField, 0, numRows)
	for i := 0; i < numRows; i++ {
		ret = append(ret, &schemapb.ScalarField{
			Data: &schemapb.ScalarField_FloatData{
				FloatData: &schemapb.FloatArray{
					Data: GenerateFloat32Array(elemCountOfArray),
				},
			},
		})
	}
	return ret
}

func GenerateArrayOfDoubleArray(numRows int) []*schemapb.ScalarField {
	ret := make([]*schemapb.ScalarField, 0, numRows)
	for i := 0; i < numRows; i++ {
		ret = append(ret, &schemapb.ScalarField{
			Data: &schemapb.ScalarField_DoubleData{
				DoubleData: &schemapb.DoubleArray{
					Data: GenerateFloat64Array(elemCountOfArray),
				},
			},
		})
	}
	return ret
}

func GenerateArrayOfStringArray(numRows int) []*schemapb.ScalarField {
	ret := make([]*schemapb.ScalarField, 0, numRows)
	for i := 0; i < numRows; i++ {
		ret = append(ret, &schemapb.ScalarField{
			Data: &schemapb.ScalarField_StringData{
				StringData: &schemapb.StringArray{
					Data: GenerateStringArray(elemCountOfArray),
				},
			},
		})
	}
	return ret
}

func GenerateBytesArray(numRows int) [][]byte {
	ret := make([][]byte, 0, numRows)
	for i := 0; i < numRows; i++ {
		ret = append(ret, []byte(fmt.Sprint(rand.Int())))
	}
	return ret
}

func GenerateBinaryVectors(numRows, dim int) []byte {
	total := (numRows * dim) / 8
	ret := make([]byte, total)
	_, err := rand.Read(ret)
	if err != nil {
		panic(err)
	}
	return ret
}

func GenerateFloatVectors(numRows, dim int) []float32 {
	total := numRows * dim
	ret := make([]float32, 0, total)
	for i := 0; i < total; i++ {
		ret = append(ret, rand.Float32())
	}
	return ret
}

func GenerateFloat16Vectors(numRows, dim int) []byte {
	total := numRows * dim
	ret := make([]byte, 0, total*2)
	for i := 0; i < total; i++ {
		f := (rand.Float32() - 0.5) * 100
		ret = append(ret, typeutil.Float32ToFloat16Bytes(f)...)
	}
	return ret
}

func GenerateBFloat16Vectors(numRows, dim int) []byte {
	total := numRows * dim
	ret := make([]byte, 0, total*2)
	for i := 0; i < total; i++ {
		f := (rand.Float32() - 0.5) * 100
		ret = append(ret, typeutil.Float32ToBFloat16Bytes(f)...)
	}
	return ret
}

func GenerateInt8Vectors(numRows, dim int) []int8 {
	total := numRows * dim
	ret := make([]int8, 0, total)
	for i := 0; i < total; i++ {
		ret = append(ret, int8(rand.Intn(256)-128))
	}
	return ret
}

func GenerateBFloat16VectorsWithInvalidData(numRows, dim int) []byte {
	total := numRows * dim
	ret16 := make([]uint16, 0, total)
	for i := 0; i < total; i++ {
		var f float32
		if i%2 == 0 {
			f = float32(math.NaN())
		} else {
			f = float32(math.Inf(1))
		}
		bits := math.Float32bits(f)
		bits >>= 16
		bits &= 0x7FFF
		ret16 = append(ret16, uint16(bits))
	}
	ret := make([]byte, len(ret16)*2)
	for i, value := range ret16 {
		binary.LittleEndian.PutUint16(ret[i*2:], value)
	}
	return ret
}

func GenerateFloat16VectorsWithInvalidData(numRows, dim int) []byte {
	total := numRows * dim
	ret := make([]byte, total*2)
	for i := 0; i < total; i++ {
		if i%2 == 0 {
			binary.LittleEndian.PutUint16(ret[i*2:], uint16(float16.Inf(1)))
		} else {
			binary.LittleEndian.PutUint16(ret[i*2:], uint16(float16.NaN()))
		}
	}
	return ret
}

func GenerateSparseFloatVectorsData(numRows int) ([][]byte, int64) {
	dim := 700
	avgNnz := 20
	var contents [][]byte
	maxDim := 0

	uniqueAndSort := func(indices []uint32) []uint32 {
		seen := make(map[uint32]bool)
		var result []uint32
		for _, value := range indices {
			if _, ok := seen[value]; !ok {
				seen[value] = true
				result = append(result, value)
			}
		}
		sort.Slice(result, func(i, j int) bool {
			return result[i] < result[j]
		})
		return result
	}

	for i := 0; i < numRows; i++ {
		nnz := rand.Intn(avgNnz*2) + 1
		indices := make([]uint32, 0, nnz)
		for j := 0; j < nnz; j++ {
			indices = append(indices, uint32(rand.Intn(dim)))
		}
		indices = uniqueAndSort(indices)
		values := make([]float32, 0, len(indices))
		for j := 0; j < len(indices); j++ {
			values = append(values, rand.Float32())
		}
		if len(indices) > 0 && int(indices[len(indices)-1])+1 > maxDim {
			maxDim = int(indices[len(indices)-1]) + 1
		}
		rowBytes := typeutil.CreateSparseFloatRow(indices, values)

		contents = append(contents, rowBytes)
	}
	return contents, int64(maxDim)
}

func GenerateSparseFloatVectors(numRows int) *schemapb.SparseFloatArray {
	dim := 700
	avgNnz := 20
	var contents [][]byte
	maxDim := 0

	uniqueAndSort := func(indices []uint32) []uint32 {
		seen := make(map[uint32]bool)
		var result []uint32
		for _, value := range indices {
			if _, ok := seen[value]; !ok {
				seen[value] = true
				result = append(result, value)
			}
		}
		sort.Slice(result, func(i, j int) bool {
			return result[i] < result[j]
		})
		return result
	}

	for i := 0; i < numRows; i++ {
		nnz := rand.Intn(avgNnz*2) + 1
		indices := make([]uint32, 0, nnz)
		for j := 0; j < nnz; j++ {
			indices = append(indices, uint32(rand.Intn(dim)))
		}
		indices = uniqueAndSort(indices)
		values := make([]float32, 0, len(indices))
		for j := 0; j < len(indices); j++ {
			values = append(values, rand.Float32())
		}
		if len(indices) > 0 && int(indices[len(indices)-1])+1 > maxDim {
			maxDim = int(indices[len(indices)-1]) + 1
		}
		rowBytes := typeutil.CreateSparseFloatRow(indices, values)

		contents = append(contents, rowBytes)
	}
	return &schemapb.SparseFloatArray{
		Dim:      int64(maxDim),
		Contents: contents,
	}
}

func GenerateHashKeys(numRows int) []uint32 {
	ret := make([]uint32, 0, numRows)
	for i := 0; i < numRows; i++ {
		ret = append(ret, rand.Uint32())
	}
	return ret
}

// generate FieldData
func NewBoolFieldData(fieldName string, numRows int) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Bool,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_BoolData{
					BoolData: &schemapb.BoolArray{
						Data: GenerateBoolArray(numRows),
					},
				},
			},
		},
	}
}

func NewBoolFieldDataWithValue(fieldName string, fieldValue interface{}) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Bool,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_BoolData{
					BoolData: &schemapb.BoolArray{
						Data: fieldValue.([]bool),
					},
				},
			},
		},
	}
}

func NewInt8FieldData(fieldName string, numRows int) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Int8,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_IntData{
					IntData: &schemapb.IntArray{
						Data: GenerateInt32Array(numRows),
					},
				},
			},
		},
	}
}

func NewInt16FieldData(fieldName string, numRows int) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Int16,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_IntData{
					IntData: &schemapb.IntArray{
						Data: GenerateInt32Array(numRows),
					},
				},
			},
		},
	}
}

func NewInt32FieldData(fieldName string, numRows int) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Int32,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_IntData{
					IntData: &schemapb.IntArray{
						Data: GenerateInt32Array(numRows),
					},
				},
			},
		},
	}
}

func NewInt32FieldDataWithValue(fieldName string, fieldValue interface{}) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Int32,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_IntData{
					IntData: &schemapb.IntArray{
						Data: fieldValue.([]int32),
					},
				},
			},
		},
	}
}

func NewInt64FieldData(fieldName string, numRows int) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{
						Data: GenerateInt64Array(numRows),
					},
				},
			},
		},
	}
}

func NewInt64FieldDataWithValue(fieldName string, fieldValue interface{}) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{
						Data: fieldValue.([]int64),
					},
				},
			},
		},
	}
}

func NewFloatFieldData(fieldName string, numRows int) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Float,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_FloatData{
					FloatData: &schemapb.FloatArray{
						Data: GenerateFloat32Array(numRows),
					},
				},
			},
		},
	}
}

func NewFloatFieldDataWithValue(fieldName string, fieldValue interface{}) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Float,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_FloatData{
					FloatData: &schemapb.FloatArray{
						Data: fieldValue.([]float32),
					},
				},
			},
		},
	}
}

func NewDoubleFieldData(fieldName string, numRows int) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Double,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_DoubleData{
					DoubleData: &schemapb.DoubleArray{
						Data: GenerateFloat64Array(numRows),
					},
				},
			},
		},
	}
}

func NewDoubleFieldDataWithValue(fieldName string, fieldValue interface{}) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Double,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_DoubleData{
					DoubleData: &schemapb.DoubleArray{
						Data: fieldValue.([]float64),
					},
				},
			},
		},
	}
}

func NewVarCharFieldData(fieldName string, numRows int) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_VarChar,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{
						Data: GenerateVarCharArray(numRows, 10),
					},
				},
			},
		},
	}
}

func NewVarCharFieldDataWithValue(fieldName string, fieldValue interface{}) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_VarChar,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{
						Data: fieldValue.([]string),
					},
				},
			},
		},
	}
}

func NewStringFieldData(fieldName string, numRows int) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_String,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{
						Data: GenerateStringArray(numRows),
					},
				},
			},
		},
	}
}

func NewJSONFieldData(fieldName string, numRows int) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_JSON,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_JsonData{
					JsonData: &schemapb.JSONArray{
						Data: GenerateJSONArray(numRows),
					},
				},
			},
		},
	}
}

func NewJSONFieldDataWithValue(fieldName string, fieldValue interface{}) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_JSON,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_JsonData{
					JsonData: &schemapb.JSONArray{
						Data: fieldValue.([][]byte),
					},
				},
			},
		},
	}
}

func NewArrayFieldData(fieldName string, numRows int) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Array,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_ArrayData{
					ArrayData: &schemapb.ArrayArray{
						Data: GenerateArrayOfIntArray(numRows),
					},
				},
			},
		},
	}
}

func NewArrayFieldDataWithValue(fieldName string, fieldValue interface{}) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Array,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_ArrayData{
					ArrayData: &schemapb.ArrayArray{
						Data: fieldValue.([]*schemapb.ScalarField),
					},
				},
			},
		},
	}
}

func NewBinaryVectorFieldData(fieldName string, numRows, dim int) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_BinaryVector,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: int64(dim),
				Data: &schemapb.VectorField_BinaryVector{
					BinaryVector: GenerateBinaryVectors(numRows, dim),
				},
			},
		},
	}
}

func NewBinaryVectorFieldDataWithValue(fieldName string, fieldValue interface{}, dim int) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_BinaryVector,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: int64(dim),
				Data: &schemapb.VectorField_BinaryVector{
					BinaryVector: fieldValue.([]byte),
				},
			},
		},
	}
}

func NewFloatVectorFieldData(fieldName string, numRows, dim int) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_FloatVector,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: int64(dim),
				Data: &schemapb.VectorField_FloatVector{
					FloatVector: &schemapb.FloatArray{
						Data: GenerateFloatVectors(numRows, dim),
					},
				},
			},
		},
	}
}

func NewFloatVectorFieldDataWithValue(fieldName string, fieldValue interface{}, dim int) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_FloatVector,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: int64(dim),
				Data: &schemapb.VectorField_FloatVector{
					FloatVector: &schemapb.FloatArray{
						Data: fieldValue.([]float32),
					},
				},
			},
		},
	}
}

func NewFloat16VectorFieldData(fieldName string, numRows, dim int) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Float16Vector,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: int64(dim),
				Data: &schemapb.VectorField_Float16Vector{
					Float16Vector: GenerateFloat16Vectors(numRows, dim),
				},
			},
		},
	}
}

func NewFloat16VectorFieldDataWithValue(fieldName string, fieldValue interface{}, dim int) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Float16Vector,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: int64(dim),
				Data: &schemapb.VectorField_Float16Vector{
					Float16Vector: fieldValue.([]byte),
				},
			},
		},
	}
}

func NewBFloat16VectorFieldData(fieldName string, numRows, dim int) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_BFloat16Vector,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: int64(dim),
				Data: &schemapb.VectorField_Bfloat16Vector{
					Bfloat16Vector: GenerateBFloat16Vectors(numRows, dim),
				},
			},
		},
	}
}

func NewBFloat16VectorFieldDataWithValue(fieldName string, fieldValue interface{}, dim int) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_BFloat16Vector,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: int64(dim),
				Data: &schemapb.VectorField_Bfloat16Vector{
					Bfloat16Vector: fieldValue.([]byte),
				},
			},
		},
	}
}

func NewSparseFloatVectorFieldData(fieldName string, numRows int) *schemapb.FieldData {
	sparseData := GenerateSparseFloatVectors(numRows)
	return &schemapb.FieldData{
		Type:      schemapb.DataType_SparseFloatVector,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: sparseData.Dim,
				Data: &schemapb.VectorField_SparseFloatVector{
					SparseFloatVector: &schemapb.SparseFloatArray{
						Dim:      sparseData.Dim,
						Contents: sparseData.Contents,
					},
				},
			},
		},
	}
}

func NewInt8VectorFieldData(fieldName string, numRows, dim int) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Int8Vector,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: int64(dim),
				Data: &schemapb.VectorField_Int8Vector{
					Int8Vector: typeutil.Int8ArrayToBytes(GenerateInt8Vectors(numRows, dim)),
				},
			},
		},
	}
}

func NewInt8VectorFieldDataWithValue(fieldName string, fieldValue interface{}, dim int) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Int8Vector,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: int64(dim),
				Data: &schemapb.VectorField_Int8Vector{
					Int8Vector: fieldValue.([]byte),
				},
			},
		},
	}
}

func GenerateScalarFieldData(dType schemapb.DataType, fieldName string, numRows int) *schemapb.FieldData {
	switch dType {
	case schemapb.DataType_Bool:
		return NewBoolFieldData(fieldName, numRows)
	case schemapb.DataType_Int8:
		return NewInt8FieldData(fieldName, numRows)
	case schemapb.DataType_Int16:
		return NewInt16FieldData(fieldName, numRows)
	case schemapb.DataType_Int32:
		return NewInt32FieldData(fieldName, numRows)
	case schemapb.DataType_Int64:
		return NewInt64FieldData(fieldName, numRows)
	case schemapb.DataType_Float:
		return NewFloatFieldData(fieldName, numRows)
	case schemapb.DataType_Double:
		return NewDoubleFieldData(fieldName, numRows)
	case schemapb.DataType_VarChar:
		return NewVarCharFieldData(fieldName, numRows)
	case schemapb.DataType_String:
		return NewStringFieldData(fieldName, numRows)
	case schemapb.DataType_Array:
		return NewArrayFieldData(fieldName, numRows)
	case schemapb.DataType_JSON:
		return NewJSONFieldData(fieldName, numRows)
	default:
		panic("unsupported data type")
	}
}

func GenerateScalarFieldDataWithID(dType schemapb.DataType, fieldName string, fieldID int64, numRows int) *schemapb.FieldData {
	fieldData := GenerateScalarFieldData(dType, fieldName, numRows)
	fieldData.FieldId = fieldID
	return fieldData
}

func GenerateScalarFieldDataWithValue(dType schemapb.DataType, fieldName string, fieldID int64, fieldValue interface{}) *schemapb.FieldData {
	var fieldData *schemapb.FieldData
	switch dType {
	case schemapb.DataType_Bool:
		fieldData = NewBoolFieldDataWithValue(fieldName, fieldValue)
	case schemapb.DataType_Int32:
		fieldData = NewInt32FieldDataWithValue(fieldName, fieldValue)
	case schemapb.DataType_Int64:
		fieldData = NewInt64FieldDataWithValue(fieldName, fieldValue)
	case schemapb.DataType_Float:
		fieldData = NewFloatFieldDataWithValue(fieldName, fieldValue)
	case schemapb.DataType_Double:
		fieldData = NewDoubleFieldDataWithValue(fieldName, fieldValue)
	case schemapb.DataType_VarChar:
		fieldData = NewVarCharFieldDataWithValue(fieldName, fieldValue)
	case schemapb.DataType_Array:
		fieldData = NewArrayFieldDataWithValue(fieldName, fieldValue)
	case schemapb.DataType_JSON:
		fieldData = NewJSONFieldDataWithValue(fieldName, fieldValue)
	default:
		panic("unsupported data type")
	}
	fieldData.FieldId = fieldID
	return fieldData
}

func GenerateVectorFieldData(dType schemapb.DataType, fieldName string, numRows int, dim int) *schemapb.FieldData {
	switch dType {
	case schemapb.DataType_BinaryVector:
		return NewBinaryVectorFieldData(fieldName, numRows, dim)
	case schemapb.DataType_FloatVector:
		return NewFloatVectorFieldData(fieldName, numRows, dim)
	case schemapb.DataType_Float16Vector:
		return NewFloat16VectorFieldData(fieldName, numRows, dim)
	case schemapb.DataType_BFloat16Vector:
		return NewBFloat16VectorFieldData(fieldName, numRows, dim)
	case schemapb.DataType_SparseFloatVector:
		return NewSparseFloatVectorFieldData(fieldName, numRows)
	case schemapb.DataType_Int8Vector:
		return NewInt8VectorFieldData(fieldName, numRows, dim)
	default:
		panic("unsupported data type")
	}
}

func GenerateVectorFieldDataWithID(dType schemapb.DataType, fieldName string, fieldID int64, numRows int, dim int) *schemapb.FieldData {
	fieldData := GenerateVectorFieldData(dType, fieldName, numRows, dim)
	fieldData.FieldId = fieldID
	return fieldData
}

func GenerateVectorFieldDataWithValue(dType schemapb.DataType, fieldName string, fieldID int64, fieldValue interface{}, dim int) *schemapb.FieldData {
	var fieldData *schemapb.FieldData
	switch dType {
	case schemapb.DataType_BinaryVector:
		fieldData = NewBinaryVectorFieldDataWithValue(fieldName, fieldValue, dim)
	case schemapb.DataType_FloatVector:
		fieldData = NewFloatVectorFieldDataWithValue(fieldName, fieldValue, dim)
	case schemapb.DataType_Float16Vector:
		fieldData = NewFloat16VectorFieldDataWithValue(fieldName, fieldValue, dim)
	case schemapb.DataType_BFloat16Vector:
		fieldData = NewBFloat16VectorFieldDataWithValue(fieldName, fieldValue, dim)
	case schemapb.DataType_Int8Vector:
		fieldData = NewInt8VectorFieldDataWithValue(fieldName, fieldValue, dim)
	default:
		panic("unsupported data type")
	}
	fieldData.FieldId = fieldID
	return fieldData
}
