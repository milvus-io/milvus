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

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/client/v2/entity"
)

//go:generate go run gen/gen.go

// Column interface field type for column-based data frame
type Column interface {
	Name() string
	Type() entity.FieldType
	Len() int
	FieldData() *schemapb.FieldData
	AppendValue(interface{}) error
	Get(int) (interface{}, error)
	GetAsInt64(int) (int64, error)
	GetAsString(int) (string, error)
	GetAsDouble(int) (float64, error)
	GetAsBool(int) (bool, error)
}

// ColumnBase adds conversion methods support for fixed-type columns.
type ColumnBase struct{}

func (b ColumnBase) GetAsInt64(_ int) (int64, error) {
	return 0, errors.New("conversion between fixed-type column not support")
}

func (b ColumnBase) GetAsString(_ int) (string, error) {
	return "", errors.New("conversion between fixed-type column not support")
}

func (b ColumnBase) GetAsDouble(_ int) (float64, error) {
	return 0, errors.New("conversion between fixed-type column not support")
}

func (b ColumnBase) GetAsBool(_ int) (bool, error) {
	return false, errors.New("conversion between fixed-type column not support")
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
	if ids == nil {
		return nil, errors.New("nil Ids from response")
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

// FieldDataColumn converts schemapb.FieldData to Column, used int search result conversion logic
// begin, end specifies the start and end positions
func FieldDataColumn(fd *schemapb.FieldData, begin, end int) (Column, error) {
	switch fd.GetType() {
	case schemapb.DataType_Bool:
		data, ok := fd.GetScalars().GetData().(*schemapb.ScalarField_BoolData)
		if !ok {
			return nil, errFieldDataTypeNotMatch
		}
		if end < 0 {
			return NewColumnBool(fd.GetFieldName(), data.BoolData.GetData()[begin:]), nil
		}
		return NewColumnBool(fd.GetFieldName(), data.BoolData.GetData()[begin:end]), nil

	case schemapb.DataType_Int8:
		data, ok := getIntData(fd)
		if !ok {
			return nil, errFieldDataTypeNotMatch
		}
		values := make([]int8, 0, len(data.IntData.GetData()))
		for _, v := range data.IntData.GetData() {
			values = append(values, int8(v))
		}

		if end < 0 {
			return NewColumnInt8(fd.GetFieldName(), values[begin:]), nil
		}

		return NewColumnInt8(fd.GetFieldName(), values[begin:end]), nil

	case schemapb.DataType_Int16:
		data, ok := getIntData(fd)
		if !ok {
			return nil, errFieldDataTypeNotMatch
		}
		values := make([]int16, 0, len(data.IntData.GetData()))
		for _, v := range data.IntData.GetData() {
			values = append(values, int16(v))
		}
		if end < 0 {
			return NewColumnInt16(fd.GetFieldName(), values[begin:]), nil
		}

		return NewColumnInt16(fd.GetFieldName(), values[begin:end]), nil

	case schemapb.DataType_Int32:
		data, ok := getIntData(fd)
		if !ok {
			return nil, errFieldDataTypeNotMatch
		}
		if end < 0 {
			return NewColumnInt32(fd.GetFieldName(), data.IntData.GetData()[begin:]), nil
		}
		return NewColumnInt32(fd.GetFieldName(), data.IntData.GetData()[begin:end]), nil

	case schemapb.DataType_Int64:
		data, ok := fd.GetScalars().GetData().(*schemapb.ScalarField_LongData)
		if !ok {
			return nil, errFieldDataTypeNotMatch
		}
		if end < 0 {
			return NewColumnInt64(fd.GetFieldName(), data.LongData.GetData()[begin:]), nil
		}
		return NewColumnInt64(fd.GetFieldName(), data.LongData.GetData()[begin:end]), nil

	case schemapb.DataType_Float:
		data, ok := fd.GetScalars().GetData().(*schemapb.ScalarField_FloatData)
		if !ok {
			return nil, errFieldDataTypeNotMatch
		}
		if end < 0 {
			return NewColumnFloat(fd.GetFieldName(), data.FloatData.GetData()[begin:]), nil
		}
		return NewColumnFloat(fd.GetFieldName(), data.FloatData.GetData()[begin:end]), nil

	case schemapb.DataType_Double:
		data, ok := fd.GetScalars().GetData().(*schemapb.ScalarField_DoubleData)
		if !ok {
			return nil, errFieldDataTypeNotMatch
		}
		if end < 0 {
			return NewColumnDouble(fd.GetFieldName(), data.DoubleData.GetData()[begin:]), nil
		}
		return NewColumnDouble(fd.GetFieldName(), data.DoubleData.GetData()[begin:end]), nil

	case schemapb.DataType_String:
		data, ok := fd.GetScalars().GetData().(*schemapb.ScalarField_StringData)
		if !ok {
			return nil, errFieldDataTypeNotMatch
		}
		if end < 0 {
			return NewColumnString(fd.GetFieldName(), data.StringData.GetData()[begin:]), nil
		}
		return NewColumnString(fd.GetFieldName(), data.StringData.GetData()[begin:end]), nil

	case schemapb.DataType_VarChar:
		data, ok := fd.GetScalars().GetData().(*schemapb.ScalarField_StringData)
		if !ok {
			return nil, errFieldDataTypeNotMatch
		}
		if end < 0 {
			return NewColumnVarChar(fd.GetFieldName(), data.StringData.GetData()[begin:]), nil
		}
		return NewColumnVarChar(fd.GetFieldName(), data.StringData.GetData()[begin:end]), nil

	case schemapb.DataType_Array:
		data := fd.GetScalars().GetArrayData()
		if data == nil {
			return nil, errFieldDataTypeNotMatch
		}
		var arrayData []*schemapb.ScalarField
		if end < 0 {
			arrayData = data.GetData()[begin:]
		} else {
			arrayData = data.GetData()[begin:end]
		}

		return parseArrayData(fd.GetFieldName(), data.GetElementType(), arrayData)

	case schemapb.DataType_JSON:
		data, ok := fd.GetScalars().GetData().(*schemapb.ScalarField_JsonData)
		isDynamic := fd.GetIsDynamic()
		if !ok {
			return nil, errFieldDataTypeNotMatch
		}
		if end < 0 {
			return NewColumnJSONBytes(fd.GetFieldName(), data.JsonData.GetData()[begin:]).WithIsDynamic(isDynamic), nil
		}
		return NewColumnJSONBytes(fd.GetFieldName(), data.JsonData.GetData()[begin:end]).WithIsDynamic(isDynamic), nil

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
			v := make([]byte, dim)
			copy(v, data[i*dim*2:(i+1)*dim*2])
			vector = append(vector, v)
		}
		return NewColumnBFloat16Vector(fd.GetFieldName(), dim, vector), nil
	default:
		return nil, fmt.Errorf("unsupported data type %s", fd.GetType())
	}
}

func parseArrayData(fieldName string, elementType schemapb.DataType, fieldDataList []*schemapb.ScalarField) (Column, error) {
	switch elementType {
	case schemapb.DataType_Bool:
		var data [][]bool
		for _, fd := range fieldDataList {
			data = append(data, fd.GetBoolData().GetData())
		}
		return NewColumnBoolArray(fieldName, data), nil

	case schemapb.DataType_Int8:
		var data [][]int8
		for _, fd := range fieldDataList {
			raw := fd.GetIntData().GetData()
			row := make([]int8, 0, len(raw))
			for _, item := range raw {
				row = append(row, int8(item))
			}
			data = append(data, row)
		}
		return NewColumnInt8Array(fieldName, data), nil

	case schemapb.DataType_Int16:
		var data [][]int16
		for _, fd := range fieldDataList {
			raw := fd.GetIntData().GetData()
			row := make([]int16, 0, len(raw))
			for _, item := range raw {
				row = append(row, int16(item))
			}
			data = append(data, row)
		}
		return NewColumnInt16Array(fieldName, data), nil

	case schemapb.DataType_Int32:
		var data [][]int32
		for _, fd := range fieldDataList {
			data = append(data, fd.GetIntData().GetData())
		}
		return NewColumnInt32Array(fieldName, data), nil

	case schemapb.DataType_Int64:
		var data [][]int64
		for _, fd := range fieldDataList {
			data = append(data, fd.GetLongData().GetData())
		}
		return NewColumnInt64Array(fieldName, data), nil

	case schemapb.DataType_Float:
		var data [][]float32
		for _, fd := range fieldDataList {
			data = append(data, fd.GetFloatData().GetData())
		}
		return NewColumnFloatArray(fieldName, data), nil

	case schemapb.DataType_Double:
		var data [][]float64
		for _, fd := range fieldDataList {
			data = append(data, fd.GetDoubleData().GetData())
		}
		return NewColumnDoubleArray(fieldName, data), nil

	case schemapb.DataType_VarChar, schemapb.DataType_String:
		var data [][][]byte
		for _, fd := range fieldDataList {
			strs := fd.GetStringData().GetData()
			bytesData := make([][]byte, 0, len(strs))
			for _, str := range strs {
				bytesData = append(bytesData, []byte(str))
			}
			data = append(data, bytesData)
		}

		return NewColumnVarCharArray(fieldName, data), nil

	default:
		return nil, fmt.Errorf("unsupported element type %s", elementType)
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

// FieldDataColumn converts schemapb.FieldData to vector Column
func FieldDataVector(fd *schemapb.FieldData) (Column, error) {
	switch fd.GetType() {
	case schemapb.DataType_FloatVector:
		vectors := fd.GetVectors()
		x, ok := vectors.GetData().(*schemapb.VectorField_FloatVector)
		if !ok {
			return nil, errFieldDataTypeNotMatch
		}
		data := x.FloatVector.GetData()
		dim := int(vectors.GetDim())
		vector := make([][]float32, 0, len(data)/dim) // shall not have remanunt
		for i := 0; i < len(data)/dim; i++ {
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
		vector := make([][]byte, 0, len(data)/blen)
		for i := 0; i < len(data)/blen; i++ {
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
		vector := make([][]byte, 0, len(data)/dim) // shall not have remanunt
		for i := 0; i < len(data)/dim; i++ {
			v := make([]byte, dim)
			copy(v, data[i*dim:(i+1)*dim])
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
		vector := make([][]byte, 0, len(data)/dim) // shall not have remanunt
		for i := 0; i < len(data)/dim; i++ {
			v := make([]byte, dim)
			copy(v, data[i*dim:(i+1)*dim])
			vector = append(vector, v)
		}
		return NewColumnBFloat16Vector(fd.GetFieldName(), dim, vector), nil
	default:
		return nil, errors.New("unsupported data type")
	}
}

// defaultValueColumn will return the empty scalars column which will be fill with default value
func DefaultValueColumn(name string, dataType entity.FieldType) (Column, error) {
	switch dataType {
	case entity.FieldTypeBool:
		return NewColumnBool(name, nil), nil
	case entity.FieldTypeInt8:
		return NewColumnInt8(name, nil), nil
	case entity.FieldTypeInt16:
		return NewColumnInt16(name, nil), nil
	case entity.FieldTypeInt32:
		return NewColumnInt32(name, nil), nil
	case entity.FieldTypeInt64:
		return NewColumnInt64(name, nil), nil
	case entity.FieldTypeFloat:
		return NewColumnFloat(name, nil), nil
	case entity.FieldTypeDouble:
		return NewColumnDouble(name, nil), nil
	case entity.FieldTypeString:
		return NewColumnString(name, nil), nil
	case entity.FieldTypeVarChar:
		return NewColumnVarChar(name, nil), nil
	case entity.FieldTypeJSON:
		return NewColumnJSONBytes(name, nil), nil

	default:
		return nil, fmt.Errorf("default value unsupported data type %s", dataType)
	}
}
