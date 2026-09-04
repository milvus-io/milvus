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

package segcore

import (
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// ArrowFieldsToProto converts the columns of an Arrow RecordBatch into proto
// FieldData. Each Arrow column carries milvus.field_id metadata; the column
// is matched to its FieldSchema via fieldSchemaMap. Columns whose field_id
// is not in the map are skipped (the field may have been dropped).
func ArrowFieldsToProto(rec arrow.Record, fieldSchemaMap map[int64]*schemapb.FieldSchema) ([]*schemapb.FieldData, error) {
	numCols := int(rec.NumCols())
	numRows := int(rec.NumRows())
	result := make([]*schemapb.FieldData, 0, numCols)

	for i := 0; i < numCols; i++ {
		field := rec.Schema().Field(i)
		md := field.Metadata
		fidStr, ok := md.GetValue("milvus.field_id")
		if !ok {
			continue
		}
		fid, err := strconv.ParseInt(fidStr, 10, 64)
		if err != nil {
			continue
		}
		schema, ok := fieldSchemaMap[fid]
		if !ok {
			continue
		}
		fd, err := arrowColumnToFieldData(rec.Column(i), schema, numRows)
		if err != nil {
			return nil, merr.WrapErrServiceInternal(
				fmt.Sprintf("failed to convert Arrow column %q (field %d)",
					schema.GetName(), schema.GetFieldID()),
				err.Error(),
			)
		}
		result = append(result, fd)
	}
	return result, nil
}

func arrowColumnToFieldData(col arrow.Array, schema *schemapb.FieldSchema, numRows int) (*schemapb.FieldData, error) {
	fd := &schemapb.FieldData{
		Type:      schema.GetDataType(),
		FieldName: schema.GetName(),
		FieldId:   schema.GetFieldID(),
	}

	switch schema.GetDataType() {
	case schemapb.DataType_Bool:
		arr := col.(*array.Boolean)
		data := make([]bool, numRows)
		for j := 0; j < numRows; j++ {
			data[j] = arr.Value(j)
		}
		fd.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_BoolData{
					BoolData: &schemapb.BoolArray{Data: data},
				},
			},
		}

	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		arr := col.(*array.Int32)
		data := make([]int32, numRows)
		copy(data, arr.Int32Values())
		fd.Field = intFieldData(data)

	case schemapb.DataType_Int64, schemapb.DataType_Timestamptz:
		arr := col.(*array.Int64)
		data := make([]int64, numRows)
		copy(data, arr.Int64Values())
		if schema.GetDataType() == schemapb.DataType_Timestamptz {
			fd.Field = &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_TimestamptzData{
						TimestamptzData: &schemapb.TimestamptzArray{Data: data},
					},
				},
			}
		} else {
			fd.Field = &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{Data: data},
					},
				},
			}
		}

	case schemapb.DataType_Float:
		arr := col.(*array.Float32)
		data := make([]float32, numRows)
		copy(data, arr.Float32Values())
		fd.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_FloatData{
					FloatData: &schemapb.FloatArray{Data: data},
				},
			},
		}

	case schemapb.DataType_Double:
		arr := col.(*array.Float64)
		data := make([]float64, numRows)
		copy(data, arr.Float64Values())
		fd.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_DoubleData{
					DoubleData: &schemapb.DoubleArray{Data: data},
				},
			},
		}

	case schemapb.DataType_VarChar, schemapb.DataType_String, schemapb.DataType_Text:
		arr := col.(*array.String)
		data := make([]string, numRows)
		for j := 0; j < numRows; j++ {
			data[j] = arr.Value(j)
		}
		fd.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{Data: data},
				},
			},
		}

	case schemapb.DataType_FloatVector:
		arr := col.(*array.FixedSizeBinary)
		dim := resolveVectorDim(schema, arr)
		floatData := compactFloatVector(arr, numRows, int(dim))
		fd.Field = &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: dim,
				Data: &schemapb.VectorField_FloatVector{
					FloatVector: &schemapb.FloatArray{Data: floatData},
				},
			},
		}

	case schemapb.DataType_BinaryVector, schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector:
		fd.Field = bytesVectorFieldData(col.(*array.FixedSizeBinary), schema, numRows)

	case schemapb.DataType_JSON:
		arr := col.(*array.Binary)
		rows := make([][]byte, numRows)
		for j := 0; j < numRows; j++ {
			rows[j] = append([]byte{}, arr.Value(j)...)
		}
		fd.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_JsonData{
					JsonData: &schemapb.JSONArray{Data: rows},
				},
			},
		}

	case schemapb.DataType_SparseFloatVector:
		arr := col.(*array.Binary)
		contents, maxDim := compactSparseVector(arr, numRows)
		fd.Field = &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: maxDim,
				Data: &schemapb.VectorField_SparseFloatVector{
					SparseFloatVector: &schemapb.SparseFloatArray{
						Contents: contents,
						Dim:      maxDim,
					},
				},
			},
		}

	case schemapb.DataType_Geometry:
		arr := col.(*array.Binary)
		rows := make([][]byte, numRows)
		for j := 0; j < numRows; j++ {
			rows[j] = append([]byte{}, arr.Value(j)...)
		}
		fd.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_GeometryData{
					GeometryData: &schemapb.GeometryArray{Data: rows},
				},
			},
		}

	case schemapb.DataType_Int8Vector:
		arr := col.(*array.FixedSizeBinary)
		dim := resolveVectorDim(schema, arr)
		data := compactBytesVector(arr, numRows)
		fd.Field = &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim:  dim,
				Data: &schemapb.VectorField_Int8Vector{Int8Vector: data},
			},
		}

	case schemapb.DataType_ArrayOfVector:
		fieldsData, err := arrowListToVectorArray(col, schema, numRows)
		if err != nil {
			return nil, err
		}
		fd.Field = fieldsData

	case schemapb.DataType_Array:
		arr := col.(*array.Binary)
		data := make([]*schemapb.ScalarField, numRows)
		for j := 0; j < numRows; j++ {
			sf := &schemapb.ScalarField{}
			if err := proto.Unmarshal(arr.Value(j), sf); err != nil {
				return nil, merr.WrapErrServiceInternalMsg("failed to unmarshal Array element at row %d for field %q: %s",
					j, schema.GetName(), err)
			}
			data[j] = sf
		}
		fd.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_ArrayData{
					ArrayData: &schemapb.ArrayArray{
						Data:        data,
						ElementType: schema.GetElementType(),
					},
				},
			},
		}

	default:
		return nil, merr.WrapErrServiceInternalMsg("unsupported data type %s for field %q (id=%d) in Arrow-to-proto conversion",
			schema.GetDataType().String(), schema.GetName(), schema.GetFieldID())
	}

	setArrowValidData(fd, col, numRows, schema.GetNullable())
	return fd, nil
}

func setArrowValidData(fd *schemapb.FieldData, col arrow.Array, numRows int, nullable bool) {
	if col.NullN() > 0 {
		validData := make([]bool, numRows)
		for j := 0; j < numRows; j++ {
			validData[j] = col.IsValid(j)
		}
		typeutil.SetFieldDataValidData(fd, validData)
		return
	}
	if nullable {
		validData := make([]bool, numRows)
		for j := range validData {
			validData[j] = true
		}
		typeutil.SetFieldDataValidData(fd, validData)
	}
}

// resolveVectorDim returns the vector dimension for a FixedSizeBinary vector
// column, preferring the FieldSchema's declared dim. If the schema doesn't
// carry a usable dim (typeutil.GetDim fails), it falls back to deriving the
// dim from the Arrow column's own byte width.
func resolveVectorDim(schema *schemapb.FieldSchema, arr *array.FixedSizeBinary) int64 {
	dim, err := typeutil.GetDim(schema)
	if err == nil {
		return dim
	}

	byteWidth := arr.DataType().(*arrow.FixedSizeBinaryType).ByteWidth
	var fallback int64
	switch schema.GetDataType() {
	case schemapb.DataType_FloatVector:
		fallback = int64(byteWidth / 4)
	case schemapb.DataType_BinaryVector:
		fallback = int64(byteWidth * 8)
	case schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector:
		fallback = int64(byteWidth / 2)
	}
	return fallback
}

func bytesVectorFieldData(arr *array.FixedSizeBinary, schema *schemapb.FieldSchema, numRows int) *schemapb.FieldData_Vectors {
	dim := resolveVectorDim(schema, arr)
	data := compactBytesVector(arr, numRows)

	vf := &schemapb.VectorField{Dim: dim}
	switch schema.GetDataType() {
	case schemapb.DataType_BinaryVector:
		vf.Data = &schemapb.VectorField_BinaryVector{BinaryVector: data}
	case schemapb.DataType_Float16Vector:
		vf.Data = &schemapb.VectorField_Float16Vector{Float16Vector: data}
	case schemapb.DataType_BFloat16Vector:
		vf.Data = &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: data}
	}
	return &schemapb.FieldData_Vectors{Vectors: vf}
}

// compactFloatVector copies only valid rows from an Arrow FixedSizeBinary
// column into a compact float32 slice.  Milvus FieldData pairs ValidData
// with a compact payload containing only valid rows.
func compactFloatVector(arr *array.FixedSizeBinary, numRows, dim int) []float32 {
	if arr.NullN() == 0 {
		raw := fixedSizeBinaryBytes(arr, numRows)
		floatData := make([]float32, numRows*dim)
		copy(floatData, arrow.Float32Traits.CastFromBytes(raw))
		return floatData
	}
	validCount := numRows - arr.NullN()
	floatData := make([]float32, validCount*dim)
	physical := 0
	for j := 0; j < numRows; j++ {
		if arr.IsValid(j) {
			src := arr.Value(j)
			copy(floatData[physical*dim:(physical+1)*dim], arrow.Float32Traits.CastFromBytes(src))
			physical++
		}
	}
	return floatData
}

// compactSparseVector copies only valid rows from an Arrow Binary column
// into a compact contents slice for sparse float vectors.
func compactSparseVector(arr *array.Binary, numRows int) ([][]byte, int64) {
	validCount := numRows - arr.NullN()
	contents := make([][]byte, validCount)
	var maxDim int64
	physical := 0
	for j := 0; j < numRows; j++ {
		if !arr.IsValid(j) {
			continue
		}
		row := arr.Value(j)
		contents[physical] = append([]byte{}, row...)
		numPairs := len(row) / 8
		for k := 0; k < numPairs; k++ {
			idx := int64(binary.LittleEndian.Uint32(row[k*8:])) + 1
			if idx > maxDim {
				maxDim = idx
			}
		}
		physical++
	}
	return contents, maxDim
}

// compactBytesVector copies only valid rows from an Arrow FixedSizeBinary
// column into a compact byte slice for binary/float16/bfloat16 vectors.
func compactBytesVector(arr *array.FixedSizeBinary, numRows int) []byte {
	if arr.NullN() == 0 {
		raw := fixedSizeBinaryBytes(arr, numRows)
		data := make([]byte, len(raw))
		copy(data, raw)
		return data
	}
	byteWidth := arr.DataType().(*arrow.FixedSizeBinaryType).ByteWidth
	validCount := numRows - arr.NullN()
	data := make([]byte, validCount*byteWidth)
	physical := 0
	for j := 0; j < numRows; j++ {
		if arr.IsValid(j) {
			copy(data[physical*byteWidth:(physical+1)*byteWidth], arr.Value(j))
			physical++
		}
	}
	return data
}

// arrowListToVectorArray converts an Arrow List(FixedSizeBinary) column into
// a proto VectorField_VectorArray.  Each list element is a single vector.
func arrowListToVectorArray(col arrow.Array, schema *schemapb.FieldSchema, numRows int) (*schemapb.FieldData_Vectors, error) {
	listArr := col.(*array.List)
	elemType := schema.GetElementType()
	dim, _ := typeutil.GetDim(schema)

	vectors := make([]*schemapb.VectorField, numRows)
	for j := 0; j < numRows; j++ {
		if !listArr.IsValid(j) {
			vectors[j] = &schemapb.VectorField{Dim: dim}
			continue
		}
		start, end := listArr.ValueOffsets(j)
		innerArr := listArr.ListValues().(*array.FixedSizeBinary)

		vf := &schemapb.VectorField{Dim: dim}
		length := int(end - start)
		switch elemType {
		case schemapb.DataType_FloatVector:
			floatData := make([]float32, 0, length*int(dim))
			for k := int(start); k < int(end); k++ {
				floatData = append(floatData, arrow.Float32Traits.CastFromBytes(innerArr.Value(k))...)
			}
			vf.Data = &schemapb.VectorField_FloatVector{
				FloatVector: &schemapb.FloatArray{Data: floatData},
			}
		case schemapb.DataType_BinaryVector,
			schemapb.DataType_Float16Vector,
			schemapb.DataType_BFloat16Vector,
			schemapb.DataType_Int8Vector:
			byteWidth := innerArr.DataType().(*arrow.FixedSizeBinaryType).ByteWidth
			raw := make([]byte, 0, length*byteWidth)
			for k := int(start); k < int(end); k++ {
				raw = append(raw, innerArr.Value(k)...)
			}
			switch elemType {
			case schemapb.DataType_BinaryVector:
				vf.Data = &schemapb.VectorField_BinaryVector{BinaryVector: raw}
			case schemapb.DataType_Float16Vector:
				vf.Data = &schemapb.VectorField_Float16Vector{Float16Vector: raw}
			case schemapb.DataType_BFloat16Vector:
				vf.Data = &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: raw}
			case schemapb.DataType_Int8Vector:
				vf.Data = &schemapb.VectorField_Int8Vector{Int8Vector: raw}
			}
		default:
			return nil, merr.WrapErrServiceInternalMsg(
				"unsupported VectorArray element type %s", elemType.String(),
			)
		}
		vectors[j] = vf
	}

	return &schemapb.FieldData_Vectors{
		Vectors: &schemapb.VectorField{
			Dim: dim,
			Data: &schemapb.VectorField_VectorArray{
				VectorArray: &schemapb.VectorArray{
					Dim:         dim,
					Data:        vectors,
					ElementType: elemType,
				},
			},
		},
	}, nil
}

func intFieldData(data []int32) *schemapb.FieldData_Scalars {
	return &schemapb.FieldData_Scalars{
		Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_IntData{
				IntData: &schemapb.IntArray{Data: data},
			},
		},
	}
}

// fixedSizeBinaryBytes returns the raw bytes backing the first numRows values
// of a FixedSizeBinary array, honoring the array's offset into its buffer.
// The returned slice aliases Arrow's underlying buffer and must not be
// retained beyond the lifetime of arr; callers copy out of it immediately.
func fixedSizeBinaryBytes(arr *array.FixedSizeBinary, numRows int) []byte {
	byteWidth := arr.DataType().(*arrow.FixedSizeBinaryType).ByteWidth
	buf := arr.Data().Buffers()[1]
	if buf == nil {
		return nil
	}
	offset := arr.Data().Offset() * byteWidth
	return buf.Bytes()[offset : offset+numRows*byteWidth]
}
