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

// Column interface field type for column-based data frame
type Column interface {
	Name() string
	Type() entity.FieldType
	Len() int
	Slice(int, int) Column
	FieldData() *schemapb.FieldData
	AppendValue(interface{}) error
	Get(int) (interface{}, error)
	GetAsInt64(int) (int64, error)
	GetAsString(int) (string, error)
	GetAsDouble(int) (float64, error)
	GetAsBool(int) (bool, error)
	// nullable related API
	AppendNull() error
	IsNull(int) (bool, error)
	Nullable() bool
	SetNullable(bool)
	ValidateNullable() error
	CompactNullableValues()
}

var errFieldDataTypeNotMatch = errors.New("FieldData type not matched")

// IDColumns converts schemapb.IDs to corresponding column
// currently Int64 / string may be in IDs
func IDColumns(schema *entity.Schema, ids *schemapb.IDs, begin, end int) (Column, error) {
	var idColumn Column
	pkField := schema.PKField()
	if pkField == nil {
		return nil, errors.New("PK Field not found")
	}
	switch pkField.DataType {
	case entity.FieldTypeInt64:
		data := ids.GetIntId().GetData()
		if data == nil {
			return NewColumnInt64(pkField.Name, nil), nil
		}
		if end >= 0 {
			idColumn = NewColumnInt64(pkField.Name, data[begin:end])
		} else {
			idColumn = NewColumnInt64(pkField.Name, data[begin:])
		}
	case entity.FieldTypeVarChar, entity.FieldTypeString:
		data := ids.GetStrId().GetData()
		if data == nil {
			return NewColumnVarChar(pkField.Name, nil), nil
		}
		if end >= 0 {
			idColumn = NewColumnVarChar(pkField.Name, data[begin:end])
		} else {
			idColumn = NewColumnVarChar(pkField.Name, data[begin:])
		}
	default:
		return nil, fmt.Errorf("unsupported id type %v", pkField.DataType)
	}
	return idColumn, nil
}

func parseScalarData[T any, COL Column, NCOL Column](
	name string,
	data []T,
	start, end int,
	validData []bool,
	creator func(string, []T) COL,
	nullableCreator func(string, []T, []bool, ...ColumnOption[T]) (NCOL, error),
) (Column, error) {
	if end < 0 {
		end = len(data)
	}
	data = data[start:end]
	if len(validData) > 0 {
		validData = validData[start:end]
		ncol, err := nullableCreator(name, data, validData, WithSparseNullableMode[T](true))
		return ncol, err
	}

	return creator(name, data), nil
}

func parseArrayData(fieldName string, elementType schemapb.DataType, fieldDataList []*schemapb.ScalarField, validData []bool, begin, end int) (Column, error) {
	switch elementType {
	case schemapb.DataType_Bool:
		data := lo.Map(fieldDataList, func(fd *schemapb.ScalarField, _ int) []bool {
			return fd.GetBoolData().GetData()
		})
		return parseScalarData(fieldName, data, begin, end, validData, NewColumnBoolArray, NewNullableColumnBoolArray)

	case schemapb.DataType_Int8:
		data := lo.Map(fieldDataList, func(fd *schemapb.ScalarField, _ int) []int8 {
			return int32ToType[int8](fd.GetIntData().GetData())
		})
		return parseScalarData(fieldName, data, begin, end, validData, NewColumnInt8Array, NewNullableColumnInt8Array)

	case schemapb.DataType_Int16:
		data := lo.Map(fieldDataList, func(fd *schemapb.ScalarField, _ int) []int16 {
			return int32ToType[int16](fd.GetIntData().GetData())
		})
		return parseScalarData(fieldName, data, begin, end, validData, NewColumnInt16Array, NewNullableColumnInt16Array)

	case schemapb.DataType_Int32:
		data := lo.Map(fieldDataList, func(fd *schemapb.ScalarField, _ int) []int32 {
			return fd.GetIntData().GetData()
		})
		return parseScalarData(fieldName, data, begin, end, validData, NewColumnInt32Array, NewNullableColumnInt32Array)

	case schemapb.DataType_Int64:
		data := lo.Map(fieldDataList, func(fd *schemapb.ScalarField, _ int) []int64 {
			return fd.GetLongData().GetData()
		})
		return parseScalarData(fieldName, data, begin, end, validData, NewColumnInt64Array, NewNullableColumnInt64Array)

	case schemapb.DataType_Float:
		data := lo.Map(fieldDataList, func(fd *schemapb.ScalarField, _ int) []float32 {
			return fd.GetFloatData().GetData()
		})
		return parseScalarData(fieldName, data, begin, end, validData, NewColumnFloatArray, NewNullableColumnFloatArray)

	case schemapb.DataType_Double:
		data := lo.Map(fieldDataList, func(fd *schemapb.ScalarField, _ int) []float64 {
			return fd.GetDoubleData().GetData()
		})
		return parseScalarData(fieldName, data, begin, end, validData, NewColumnDoubleArray, NewNullableColumnDoubleArray)

	case schemapb.DataType_VarChar, schemapb.DataType_String:
		data := lo.Map(fieldDataList, func(fd *schemapb.ScalarField, _ int) []string {
			return fd.GetStringData().GetData()
		})
		return parseScalarData(fieldName, data, begin, end, validData, NewColumnVarCharArray, NewNullableColumnVarCharArray)

	default:
		return nil, fmt.Errorf("unsupported element type %s", elementType)
	}
}

func int32ToType[T ~int8 | int16](data []int32) []T {
	return lo.Map(data, func(i32 int32, _ int) T {
		return T(i32)
	})
}

// FieldDataColumn converts schemapb.FieldData to Column, used int search result conversion logic
// begin, end specifies the start and end positions
func FieldDataColumn(fd *schemapb.FieldData, begin, end int) (Column, error) {
	validData := fd.GetValidData()

	switch fd.GetType() {
	case schemapb.DataType_Bool:
		return parseScalarData(fd.GetFieldName(), fd.GetScalars().GetBoolData().GetData(), begin, end, validData, NewColumnBool, NewNullableColumnBool)

	case schemapb.DataType_Int8:
		data := int32ToType[int8](fd.GetScalars().GetIntData().GetData())
		return parseScalarData(fd.GetFieldName(), data, begin, end, validData, NewColumnInt8, NewNullableColumnInt8)

	case schemapb.DataType_Int16:
		data := int32ToType[int16](fd.GetScalars().GetIntData().GetData())
		return parseScalarData(fd.GetFieldName(), data, begin, end, validData, NewColumnInt16, NewNullableColumnInt16)

	case schemapb.DataType_Int32:
		return parseScalarData(fd.GetFieldName(), fd.GetScalars().GetIntData().GetData(), begin, end, validData, NewColumnInt32, NewNullableColumnInt32)

	case schemapb.DataType_Int64:
		return parseScalarData(fd.GetFieldName(), fd.GetScalars().GetLongData().GetData(), begin, end, validData, NewColumnInt64, NewNullableColumnInt64)

	case schemapb.DataType_Float:
		return parseScalarData(fd.GetFieldName(), fd.GetScalars().GetFloatData().GetData(), begin, end, validData, NewColumnFloat, NewNullableColumnFloat)

	case schemapb.DataType_Double:
		return parseScalarData(fd.GetFieldName(), fd.GetScalars().GetDoubleData().GetData(), begin, end, validData, NewColumnDouble, NewNullableColumnDouble)

	case schemapb.DataType_String:
		return parseScalarData(fd.GetFieldName(), fd.GetScalars().GetStringData().GetData(), begin, end, validData, NewColumnString, NewNullableColumnString)

	case schemapb.DataType_VarChar:
		return parseScalarData(fd.GetFieldName(), fd.GetScalars().GetStringData().GetData(), begin, end, validData, NewColumnVarChar, NewNullableColumnVarChar)

	case schemapb.DataType_Array:
		data := fd.GetScalars().GetArrayData()
		return parseArrayData(fd.GetFieldName(), data.GetElementType(), data.GetData(), validData, begin, end)

	case schemapb.DataType_JSON:
		return parseScalarData(fd.GetFieldName(), fd.GetScalars().GetJsonData().GetData(), begin, end, validData, NewColumnJSONBytes, NewNullableColumnJSONBytes)

	case schemapb.DataType_FloatVector:
		vectors := fd.GetVectors()
		x, ok := vectors.GetData().(*schemapb.VectorField_FloatVector)
		if !ok {
			return nil, errFieldDataTypeNotMatch
		}
		data := x.FloatVector.GetData()
		dim := int(vectors.GetDim())
		if end < 0 {
			end = len(data) / dim
		}
		vector := make([][]float32, 0, end-begin) // shall not have remanunt
		for i := begin; i < end; i++ {
			v := make([]float32, dim)
			copy(v, data[i*dim:(i+1)*dim])
			vector = append(vector, v)
		}
		return NewColumnFloatVector(fd.GetFieldName(), dim, vector), nil

	case schemapb.DataType_BinaryVector:
		vectors := fd.GetVectors()
		x, ok := vectors.GetData().(*schemapb.VectorField_BinaryVector)
		if !ok {
			return nil, errFieldDataTypeNotMatch
		}
		data := x.BinaryVector
		if data == nil {
			return nil, errFieldDataTypeNotMatch
		}
		dim := int(vectors.GetDim())
		blen := dim / 8
		if end < 0 {
			end = len(data) / blen
		}
		vector := make([][]byte, 0, end-begin)
		for i := begin; i < end; i++ {
			v := make([]byte, blen)
			copy(v, data[i*blen:(i+1)*blen])
			vector = append(vector, v)
		}
		return NewColumnBinaryVector(fd.GetFieldName(), dim, vector), nil

	case schemapb.DataType_Float16Vector:
		vectors := fd.GetVectors()
		x, ok := vectors.GetData().(*schemapb.VectorField_Float16Vector)
		if !ok {
			return nil, errFieldDataTypeNotMatch
		}
		data := x.Float16Vector
		dim := int(vectors.GetDim())
		if end < 0 {
			end = len(data) / dim / 2
		}
		vector := make([][]byte, 0, end-begin)
		for i := begin; i < end; i++ {
			v := make([]byte, dim*2)
			copy(v, data[i*dim*2:(i+1)*dim*2])
			vector = append(vector, v)
		}
		return NewColumnFloat16Vector(fd.GetFieldName(), dim, vector), nil

	case schemapb.DataType_BFloat16Vector:
		vectors := fd.GetVectors()
		x, ok := vectors.GetData().(*schemapb.VectorField_Bfloat16Vector)
		if !ok {
			return nil, errFieldDataTypeNotMatch
		}
		data := x.Bfloat16Vector
		dim := int(vectors.GetDim())
		if end < 0 {
			end = len(data) / dim / 2
		}
		vector := make([][]byte, 0, end-begin) // shall not have remanunt
		for i := begin; i < end; i++ {
			v := make([]byte, dim*2)
			copy(v, data[i*dim*2:(i+1)*dim*2])
			vector = append(vector, v)
		}
		return NewColumnBFloat16Vector(fd.GetFieldName(), dim, vector), nil
	case schemapb.DataType_SparseFloatVector:
		sparseVectors := fd.GetVectors().GetSparseFloatVector()
		if sparseVectors == nil {
			return nil, errFieldDataTypeNotMatch
		}
		data := sparseVectors.Contents
		if end < 0 {
			end = len(data)
		}
		data = data[begin:end]
		vectors := make([]entity.SparseEmbedding, 0, len(data))
		for _, bs := range data {
			vector, err := entity.DeserializeSliceSparseEmbedding(bs)
			if err != nil {
				return nil, err
			}
			vectors = append(vectors, vector)
		}
		return NewColumnSparseVectors(fd.GetFieldName(), vectors), nil
	default:
		return nil, fmt.Errorf("unsupported data type %s", fd.GetType())
	}
}

// getIntData get int32 slice from result field data
// also handles LongData bug (see also https://github.com/milvus-io/milvus/issues/23850)
func getIntData(fd *schemapb.FieldData) (*schemapb.ScalarField_IntData, bool) {
	switch data := fd.GetScalars().GetData().(type) {
	case *schemapb.ScalarField_IntData:
		return data, true
	case *schemapb.ScalarField_LongData:
		// only alway empty LongData for backward compatibility
		if len(data.LongData.GetData()) == 0 {
			return &schemapb.ScalarField_IntData{
				IntData: &schemapb.IntArray{},
			}, true
		}
		return nil, false
	default:
		return nil, false
	}
}
