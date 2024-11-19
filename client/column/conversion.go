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

package column

import (
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/client/v2/entity"
)

func slice2Scalar[T any](values []T, elementType entity.FieldType) *schemapb.ScalarField {
	var ok bool
	scalarField := &schemapb.ScalarField{}
	switch elementType {
	case entity.FieldTypeBool:
		var bools []bool
		bools, ok = any(values).([]bool)
		scalarField.Data = &schemapb.ScalarField_BoolData{
			BoolData: &schemapb.BoolArray{
				Data: bools,
			},
		}
	case entity.FieldTypeInt8:
		var int8s []int8
		int8s, ok = any(values).([]int8)
		int32s := lo.Map(int8s, func(i8 int8, _ int) int32 { return int32(i8) })
		scalarField.Data = &schemapb.ScalarField_IntData{
			IntData: &schemapb.IntArray{
				Data: int32s,
			},
		}
	case entity.FieldTypeInt16:
		var int16s []int16
		int16s, ok = any(values).([]int16)
		int32s := lo.Map(int16s, func(i16 int16, _ int) int32 { return int32(i16) })
		scalarField.Data = &schemapb.ScalarField_IntData{
			IntData: &schemapb.IntArray{
				Data: int32s,
			},
		}
	case entity.FieldTypeInt32:
		var int32s []int32
		int32s, ok = any(values).([]int32)
		scalarField.Data = &schemapb.ScalarField_IntData{
			IntData: &schemapb.IntArray{
				Data: int32s,
			},
		}
	case entity.FieldTypeInt64:
		var int64s []int64
		int64s, ok = any(values).([]int64)
		scalarField.Data = &schemapb.ScalarField_LongData{
			LongData: &schemapb.LongArray{
				Data: int64s,
			},
		}
	case entity.FieldTypeFloat:
		var floats []float32
		floats, ok = any(values).([]float32)
		scalarField.Data = &schemapb.ScalarField_FloatData{
			FloatData: &schemapb.FloatArray{
				Data: floats,
			},
		}
	case entity.FieldTypeDouble:
		var doubles []float64
		doubles, ok = any(values).([]float64)
		scalarField.Data = &schemapb.ScalarField_DoubleData{
			DoubleData: &schemapb.DoubleArray{
				Data: doubles,
			},
		}
	case entity.FieldTypeVarChar, entity.FieldTypeString:
		var strings []string
		strings, ok = any(values).([]string)
		scalarField.Data = &schemapb.ScalarField_StringData{
			StringData: &schemapb.StringArray{
				Data: strings,
			},
		}
	}

	if !ok {
		panic(fmt.Sprintf("unexpected values type(%T) of fieldType %v", values, elementType))
	}
	return scalarField
}

func values2FieldData[T any](values []T, fieldType entity.FieldType, dim int) *schemapb.FieldData {
	fd := &schemapb.FieldData{}
	switch fieldType {
	// scalars
	case entity.FieldTypeBool,
		entity.FieldTypeFloat,
		entity.FieldTypeDouble,
		entity.FieldTypeInt8,
		entity.FieldTypeInt16,
		entity.FieldTypeInt32,
		entity.FieldTypeInt64,
		entity.FieldTypeVarChar,
		entity.FieldTypeString,
		entity.FieldTypeJSON:
		fd.Field = &schemapb.FieldData_Scalars{
			Scalars: values2Scalars(values, fieldType), // scalars,
		}
	// vectors
	case entity.FieldTypeFloatVector,
		entity.FieldTypeFloat16Vector,
		entity.FieldTypeBFloat16Vector,
		entity.FieldTypeBinaryVector,
		entity.FieldTypeSparseVector:
		fd.Field = &schemapb.FieldData_Vectors{
			Vectors: values2Vectors(values, fieldType, int64(dim)),
		}
	default:
		panic(fmt.Sprintf("unexpected values type(%T) of fieldType %v", values, fieldType))
	}
	return fd
}

func values2Scalars[T any](values []T, fieldType entity.FieldType) *schemapb.ScalarField {
	scalars := &schemapb.ScalarField{}
	var ok bool
	switch fieldType {
	case entity.FieldTypeBool:
		var bools []bool
		bools, ok = any(values).([]bool)
		scalars.Data = &schemapb.ScalarField_BoolData{
			BoolData: &schemapb.BoolArray{Data: bools},
		}
	case entity.FieldTypeInt8:
		var int8s []int8
		int8s, ok = any(values).([]int8)
		int32s := lo.Map(int8s, func(i8 int8, _ int) int32 { return int32(i8) })
		scalars.Data = &schemapb.ScalarField_IntData{
			IntData: &schemapb.IntArray{Data: int32s},
		}
	case entity.FieldTypeInt16:
		var int16s []int16
		int16s, ok = any(values).([]int16)
		int32s := lo.Map(int16s, func(i16 int16, _ int) int32 { return int32(i16) })
		scalars.Data = &schemapb.ScalarField_IntData{
			IntData: &schemapb.IntArray{Data: int32s},
		}
	case entity.FieldTypeInt32:
		var int32s []int32
		int32s, ok = any(values).([]int32)
		scalars.Data = &schemapb.ScalarField_IntData{
			IntData: &schemapb.IntArray{Data: int32s},
		}
	case entity.FieldTypeInt64:
		var int64s []int64
		int64s, ok = any(values).([]int64)
		scalars.Data = &schemapb.ScalarField_LongData{
			LongData: &schemapb.LongArray{Data: int64s},
		}
	case entity.FieldTypeVarChar, entity.FieldTypeString:
		var strVals []string
		strVals, ok = any(values).([]string)
		scalars.Data = &schemapb.ScalarField_StringData{
			StringData: &schemapb.StringArray{Data: strVals},
		}
	case entity.FieldTypeFloat:
		var floats []float32
		floats, ok = any(values).([]float32)
		scalars.Data = &schemapb.ScalarField_FloatData{
			FloatData: &schemapb.FloatArray{Data: floats},
		}
	case entity.FieldTypeDouble:
		var data []float64
		data, ok = any(values).([]float64)
		scalars.Data = &schemapb.ScalarField_DoubleData{
			DoubleData: &schemapb.DoubleArray{Data: data},
		}
	case entity.FieldTypeJSON:
		var data [][]byte
		data, ok = any(values).([][]byte)
		scalars.Data = &schemapb.ScalarField_JsonData{
			JsonData: &schemapb.JSONArray{
				Data: data,
			},
		}
	}
	// shall not be accessed
	if !ok {
		panic(fmt.Sprintf("unexpected values type(%T) of fieldType %v", values, fieldType))
	}
	return scalars
}

func values2Vectors[T any](values []T, fieldType entity.FieldType, dim int64) *schemapb.VectorField {
	vectorField := &schemapb.VectorField{
		Dim: dim,
	}
	var ok bool
	switch fieldType {
	case entity.FieldTypeFloatVector:
		var vectors []entity.FloatVector
		vectors, ok = any(values).([]entity.FloatVector)
		data := make([]float32, 0, int64(len(vectors))*dim)
		for _, vector := range vectors {
			data = append(data, vector...)
		}
		vectorField.Data = &schemapb.VectorField_FloatVector{
			FloatVector: &schemapb.FloatArray{
				Data: data,
			},
		}
	case entity.FieldTypeFloat16Vector:
		var vectors []entity.Float16Vector
		vectors, ok = any(values).([]entity.Float16Vector)
		data := make([]byte, 0, int64(len(vectors))*dim*2)
		for _, vector := range vectors {
			data = append(data, vector.Serialize()...)
		}
		vectorField.Data = &schemapb.VectorField_Float16Vector{
			Float16Vector: data,
		}
	case entity.FieldTypeBFloat16Vector:
		var vectors []entity.BFloat16Vector
		vectors, ok = any(values).([]entity.BFloat16Vector)
		data := make([]byte, 0, int64(len(vectors))*dim*2)
		for _, vector := range vectors {
			data = append(data, vector.Serialize()...)
		}
		vectorField.Data = &schemapb.VectorField_Bfloat16Vector{
			Bfloat16Vector: data,
		}
	case entity.FieldTypeBinaryVector:
		var vectors []entity.BinaryVector
		vectors, ok = any(values).([]entity.BinaryVector)
		data := make([]byte, 0, int64(len(vectors))*dim/8)
		for _, vector := range vectors {
			data = append(data, vector.Serialize()...)
		}
		vectorField.Data = &schemapb.VectorField_BinaryVector{
			BinaryVector: data,
		}
	case entity.FieldTypeSparseVector:
		var vectors []entity.SparseEmbedding
		vectors, ok = any(values).([]entity.SparseEmbedding)
		data := lo.Map(vectors, func(row entity.SparseEmbedding, _ int) []byte {
			return row.Serialize()
		})
		vectorField.Data = &schemapb.VectorField_SparseFloatVector{
			SparseFloatVector: &schemapb.SparseFloatArray{
				Contents: data,
			},
		}
	}

	if !ok {
		panic(fmt.Sprintf("unexpected values type(%T) of fieldType %v", values, fieldType))
	}
	return vectorField
}

func value2Type[T any, U any](v T) (U, error) {
	var z U
	switch v := any(v).(type) {
	case U:
		return v, nil
	default:
		return z, errors.Newf("cannot automatically convert %T to %T", v, z)
	}
}
