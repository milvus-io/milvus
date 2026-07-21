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

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/client/v3/entity"
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
	ValidCount() int
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
	logicalLen := len(data)
	if len(validData) > 0 {
		logicalLen = len(validData)
	}
	if start < 0 {
		start = 0
	}
	if start > logicalLen {
		start = logicalLen
	}
	if end < 0 || end > logicalLen {
		end = logicalLen
	}
	if start > end {
		start = end
	}

	if len(validData) > 0 {
		validCount := countValid(validData)
		sparseMode := false
		switch len(data) {
		case logicalLen:
			sparseMode = true
		case validCount:
		default:
			return nil, fmt.Errorf("scalar field %q payload row count %d does not match logical row count %d or valid count %d",
				name, len(data), logicalLen, validCount)
		}

		selectedValidData := validData[start:end]
		if sparseMode {
			data = data[start:end]
		} else {
			valueStart := countValid(validData[:start])
			valueEnd := valueStart + countValid(selectedValidData)
			data = data[valueStart:valueEnd]
		}
		ncol, err := nullableCreator(name, data, selectedValidData, WithSparseNullableMode[T](sparseMode))
		if err != nil {
			return nil, err
		}
		// An empty nullable slice has no validity bits, but it must retain the
		// nullable schema state for its parent struct array.
		ncol.SetNullable(true)
		return ncol, ncol.ValidateNullable()
	}

	data = data[start:end]
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

func parseStructArrayData(fieldName string, structArray *schemapb.StructArrayField, begin, end int) (Column, error) {
	var fields []Column
	for _, field := range structArray.GetFields() {
		field, err := FieldDataColumn(field, begin, end)
		if err != nil {
			return nil, err
		}
		fields = append(fields, field)
	}
	column := NewColumnStructArray(fieldName, fields)
	if err := column.ValidateNullable(); err != nil {
		return nil, errors.Wrapf(err, "invalid struct array %q", fieldName)
	}
	return column, nil
}

// parseVectorArrayData converts schemapb.VectorArray (per-row list of vectors) into the
// matching ColumnXxxVectorArray. Used for ArrayOfVector sub-fields of struct arrays.
func parseVectorArrayData(fieldName string, va *schemapb.VectorArray, validData []bool, begin, end int) (Column, error) {
	rows := va.GetData()
	// VectorArray.Dim may be 0 in server search responses; fall back to inner VectorField.Dim.
	dim := int(va.GetDim())
	if dim == 0 {
		for _, vf := range rows {
			if d := int(vf.GetDim()); d > 0 {
				dim = d
				break
			}
		}
	}
	if dim == 0 {
		return nil, fmt.Errorf("vector array %q has unknown dim", fieldName)
	}

	nullable := validData != nil
	sparseMode := false
	logicalLen := len(rows)
	if nullable {
		logicalLen = len(validData)
		switch len(rows) {
		case logicalLen:
			// Query results are row-dense and keep an empty placeholder for null rows.
			sparseMode = true
		case countValid(validData):
			// Insert FieldData keeps only valid payload rows.
			sparseMode = false
		default:
			return nil, fmt.Errorf("vector array %q payload row count %d does not match logical row count %d or valid count %d",
				fieldName, len(rows), logicalLen, countValid(validData))
		}
	}
	if begin < 0 {
		begin = 0
	}
	if begin > logicalLen {
		begin = logicalLen
	}
	if end < 0 || end > logicalLen {
		end = logicalLen
	}
	if begin > end {
		begin = end
	}
	selectedValidData := validData
	if nullable {
		selectedValidData = validData[begin:end]
		if sparseMode {
			rows = rows[begin:end]
		} else {
			valueBegin := countValid(validData[:begin])
			valueEnd := valueBegin + countValid(selectedValidData)
			rows = rows[valueBegin:valueEnd]
		}
	} else {
		rows = rows[begin:end]
	}
	finish := func(column Column) (Column, error) {
		if !nullable {
			return column, nil
		}
		vectorColumn, ok := column.(interface {
			setNullableData([]bool, bool) error
		})
		if !ok {
			return nil, fmt.Errorf("vector array %q column does not support nullable data", fieldName)
		}
		if err := vectorColumn.setNullableData(selectedValidData, sparseMode); err != nil {
			return nil, errors.Wrapf(err, "invalid vector array %q nullable data", fieldName)
		}
		return column, nil
	}

	switch va.GetElementType() {
	case schemapb.DataType_FloatVector:
		out, err := splitVectorArrayRows(fieldName, rows, dim, func(vf *schemapb.VectorField) []float32 {
			return vf.GetFloatVector().GetData()
		}, func(data []float32, j, w int) []float32 {
			v := make([]float32, w)
			copy(v, data[j:j+w])
			return v
		})
		if err != nil {
			return nil, err
		}
		return finish(NewColumnFloatVectorArray(fieldName, dim, out))

	case schemapb.DataType_Float16Vector:
		out, err := splitVectorArrayRows(fieldName, rows, dim*2, func(vf *schemapb.VectorField) []byte {
			return vf.GetFloat16Vector()
		}, copyByteVector)
		if err != nil {
			return nil, err
		}
		return finish(NewColumnFloat16VectorArray(fieldName, dim, out))

	case schemapb.DataType_BFloat16Vector:
		out, err := splitVectorArrayRows(fieldName, rows, dim*2, func(vf *schemapb.VectorField) []byte {
			return vf.GetBfloat16Vector()
		}, copyByteVector)
		if err != nil {
			return nil, err
		}
		return finish(NewColumnBFloat16VectorArray(fieldName, dim, out))

	case schemapb.DataType_BinaryVector:
		if dim%8 != 0 {
			return nil, fmt.Errorf("binary vector array %q requires dim multiple of 8, got %d", fieldName, dim)
		}
		out, err := splitVectorArrayRows(fieldName, rows, dim/8, func(vf *schemapb.VectorField) []byte {
			return vf.GetBinaryVector()
		}, copyByteVector)
		if err != nil {
			return nil, err
		}
		return finish(NewColumnBinaryVectorArray(fieldName, dim, out))

	case schemapb.DataType_Int8Vector:
		out, err := splitVectorArrayRows(fieldName, rows, dim, func(vf *schemapb.VectorField) []byte {
			return vf.GetInt8Vector()
		}, func(data []byte, j, w int) []int8 {
			v := make([]int8, w)
			for k := 0; k < w; k++ {
				v[k] = int8(data[j+k])
			}
			return v
		})
		if err != nil {
			return nil, err
		}
		return finish(NewColumnInt8VectorArray(fieldName, dim, out))

	default:
		return nil, fmt.Errorf("unsupported vector array element type %s", va.GetElementType())
	}
}

// splitVectorArrayRows extracts per-row flat payloads via `get` and splits each by `width` using
// `copyVector`. It validates that the flat payload length is a positive multiple of `width`, so
// that server-side protocol errors surface as clear errors instead of silent truncation or panics.
// The result is shaped as [row][vector][element].
func splitVectorArrayRows[D any, E any](
	fieldName string,
	rows []*schemapb.VectorField,
	width int,
	get func(*schemapb.VectorField) []D,
	copyVector func(data []D, offset, width int) []E,
) ([][][]E, error) {
	if width <= 0 {
		return nil, fmt.Errorf("vector array %q has invalid row width %d", fieldName, width)
	}
	out := make([][][]E, 0, len(rows))
	for i, vf := range rows {
		if vf == nil {
			return nil, fmt.Errorf("vector array %q row %d is nil", fieldName, i)
		}
		data := get(vf)
		if len(data)%width != 0 {
			return nil, fmt.Errorf("vector array %q row %d payload length %d not a multiple of row width %d",
				fieldName, i, len(data), width)
		}
		row := make([][]E, 0, len(data)/width)
		for j := 0; j+width <= len(data); j += width {
			row = append(row, copyVector(data, j, width))
		}
		out = append(out, row)
	}
	return out, nil
}

func copyByteVector(data []byte, offset, width int) []byte {
	v := make([]byte, width)
	copy(v, data[offset:offset+width])
	return v
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

	case schemapb.DataType_Timestamptz:
		return parseScalarData(fd.GetFieldName(), fd.GetScalars().GetStringData().GetData(), begin, end, validData, NewColumnTimestamptzIsoString, NewNullableColumnTimestamptzIsoString)

	case schemapb.DataType_String:
		return parseScalarData(fd.GetFieldName(), fd.GetScalars().GetStringData().GetData(), begin, end, validData, NewColumnString, NewNullableColumnString)

	case schemapb.DataType_VarChar:
		return parseScalarData(fd.GetFieldName(), fd.GetScalars().GetStringData().GetData(), begin, end, validData, NewColumnVarChar, NewNullableColumnVarChar)

	case schemapb.DataType_Array:
		// handle struct array field (legacy server may use DataType_Array as top-level)
		if fd.GetStructArrays() != nil {
			return parseStructArrayData(fd.GetFieldName(), fd.GetStructArrays(), begin, end)
		}
		data := fd.GetScalars().GetArrayData()
		return parseArrayData(fd.GetFieldName(), data.GetElementType(), data.GetData(), validData, begin, end)

	case schemapb.DataType_ArrayOfStruct:
		return parseStructArrayData(fd.GetFieldName(), fd.GetStructArrays(), begin, end)

	case schemapb.DataType_ArrayOfVector:
		vectors := fd.GetVectors()
		va := vectors.GetVectorArray()
		if va == nil {
			return nil, errFieldDataTypeNotMatch
		}
		return parseVectorArrayData(fd.GetFieldName(), va, validData, begin, end)

	case schemapb.DataType_JSON:
		return parseScalarData(fd.GetFieldName(), fd.GetScalars().GetJsonData().GetData(), begin, end, validData, NewColumnJSONBytes, NewNullableColumnJSONBytes)

	case schemapb.DataType_Geometry:
		return parseScalarData(fd.GetFieldName(), fd.GetScalars().GetGeometryWktData().GetData(), begin, end, validData, NewColumnGeometryWKT, NewNullableColumnGeometryWKT)

	case schemapb.DataType_FloatVector:
		vectors := fd.GetVectors()
		x, ok := vectors.GetData().(*schemapb.VectorField_FloatVector)
		if !ok {
			return nil, errFieldDataTypeNotMatch
		}
		data := x.FloatVector.GetData()
		dim := int(vectors.GetDim())

		if len(validData) > 0 {
			if end < 0 {
				end = len(validData)
			}
			vector := make([][]float32, 0, end-begin)
			dataIdx := 0
			for i := 0; i < begin; i++ {
				if validData[i] {
					dataIdx++
				}
			}
			for i := begin; i < end; i++ {
				if validData[i] {
					v := make([]float32, dim)
					copy(v, data[dataIdx*dim:(dataIdx+1)*dim])
					vector = append(vector, v)
					dataIdx++
				} else {
					vector = append(vector, nil)
				}
			}
			col := NewColumnFloatVector(fd.GetFieldName(), dim, vector)
			col.withValidData(validData[begin:end])
			col.nullable = true
			col.sparseMode = true
			return col, nil
		}

		if end < 0 {
			end = len(data) / dim
		}
		vector := make([][]float32, 0, end-begin)
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

		if len(validData) > 0 {
			if end < 0 {
				end = len(validData)
			}
			vector := make([][]byte, 0, end-begin)
			dataIdx := 0
			for i := 0; i < begin; i++ {
				if validData[i] {
					dataIdx++
				}
			}
			for i := begin; i < end; i++ {
				if validData[i] {
					v := make([]byte, blen)
					copy(v, data[dataIdx*blen:(dataIdx+1)*blen])
					vector = append(vector, v)
					dataIdx++
				} else {
					vector = append(vector, nil)
				}
			}
			col := NewColumnBinaryVector(fd.GetFieldName(), dim, vector)
			col.withValidData(validData[begin:end])
			col.nullable = true
			col.sparseMode = true
			return col, nil
		}

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
		bytePerRow := dim * 2

		if len(validData) > 0 {
			if end < 0 {
				end = len(validData)
			}
			vector := make([][]byte, 0, end-begin)
			dataIdx := 0
			for i := 0; i < begin; i++ {
				if validData[i] {
					dataIdx++
				}
			}
			for i := begin; i < end; i++ {
				if validData[i] {
					v := make([]byte, bytePerRow)
					copy(v, data[dataIdx*bytePerRow:(dataIdx+1)*bytePerRow])
					vector = append(vector, v)
					dataIdx++
				} else {
					vector = append(vector, nil)
				}
			}
			col := NewColumnFloat16Vector(fd.GetFieldName(), dim, vector)
			col.withValidData(validData[begin:end])
			col.nullable = true
			col.sparseMode = true
			return col, nil
		}

		if end < 0 {
			end = len(data) / bytePerRow
		}
		vector := make([][]byte, 0, end-begin)
		for i := begin; i < end; i++ {
			v := make([]byte, bytePerRow)
			copy(v, data[i*bytePerRow:(i+1)*bytePerRow])
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
		bytePerRow := dim * 2

		if len(validData) > 0 {
			if end < 0 {
				end = len(validData)
			}
			vector := make([][]byte, 0, end-begin)
			dataIdx := 0
			for i := 0; i < begin; i++ {
				if validData[i] {
					dataIdx++
				}
			}
			for i := begin; i < end; i++ {
				if validData[i] {
					v := make([]byte, bytePerRow)
					copy(v, data[dataIdx*bytePerRow:(dataIdx+1)*bytePerRow])
					vector = append(vector, v)
					dataIdx++
				} else {
					vector = append(vector, nil)
				}
			}
			col := NewColumnBFloat16Vector(fd.GetFieldName(), dim, vector)
			col.withValidData(validData[begin:end])
			col.nullable = true
			col.sparseMode = true
			return col, nil
		}

		if end < 0 {
			end = len(data) / bytePerRow
		}
		vector := make([][]byte, 0, end-begin)
		for i := begin; i < end; i++ {
			v := make([]byte, bytePerRow)
			copy(v, data[i*bytePerRow:(i+1)*bytePerRow])
			vector = append(vector, v)
		}
		return NewColumnBFloat16Vector(fd.GetFieldName(), dim, vector), nil

	case schemapb.DataType_SparseFloatVector:
		sparseVectors := fd.GetVectors().GetSparseFloatVector()
		if sparseVectors == nil {
			return nil, errFieldDataTypeNotMatch
		}
		data := sparseVectors.Contents

		if len(validData) > 0 {
			if end < 0 {
				end = len(validData)
			}
			vectors := make([]entity.SparseEmbedding, 0, end-begin)
			dataIdx := 0
			for i := 0; i < begin; i++ {
				if validData[i] {
					dataIdx++
				}
			}
			for i := begin; i < end; i++ {
				if validData[i] {
					vector, err := entity.DeserializeSliceSparseEmbedding(data[dataIdx])
					if err != nil {
						return nil, err
					}
					vectors = append(vectors, vector)
					dataIdx++
				} else {
					vectors = append(vectors, nil)
				}
			}
			col := NewColumnSparseVectors(fd.GetFieldName(), vectors)
			col.withValidData(validData[begin:end])
			col.nullable = true
			col.sparseMode = true
			return col, nil
		}

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

	case schemapb.DataType_Int8Vector:
		vectors := fd.GetVectors()
		x, ok := vectors.GetData().(*schemapb.VectorField_Int8Vector)
		if !ok {
			return nil, errFieldDataTypeNotMatch
		}
		data := x.Int8Vector
		dim := int(vectors.GetDim())

		if len(validData) > 0 {
			if end < 0 {
				end = len(validData)
			}
			vector := make([][]int8, 0, end-begin)
			dataIdx := 0
			for i := 0; i < begin; i++ {
				if validData[i] {
					dataIdx++
				}
			}
			for i := begin; i < end; i++ {
				if validData[i] {
					v := make([]int8, dim)
					for j := 0; j < dim; j++ {
						v[j] = int8(data[dataIdx*dim+j])
					}
					vector = append(vector, v)
					dataIdx++
				} else {
					vector = append(vector, nil)
				}
			}
			col := NewColumnInt8Vector(fd.GetFieldName(), dim, vector)
			col.withValidData(validData[begin:end])
			col.nullable = true
			col.sparseMode = true
			return col, nil
		}

		if end < 0 {
			end = len(data) / dim
		}
		vector := make([][]int8, 0, end-begin)
		for i := begin; i < end; i++ {
			v := make([]int8, dim)
			for j := 0; j < dim; j++ {
				v[j] = int8(data[i*dim+j])
			}
			vector = append(vector, v)
		}
		return NewColumnInt8Vector(fd.GetFieldName(), dim, vector), nil

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
