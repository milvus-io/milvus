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

package arrowconv

import (
	"encoding/binary"
	"math"
	"strconv"
	"strings"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// ArrowFieldsToProto converts the columns of an Arrow RecordBatch into proto
// FieldData. Each Arrow column carries milvus.field_id metadata; the column
// is matched to its FieldSchema via fieldSchemaMap. Columns whose field_id
// is not in the map are skipped (the field may have been dropped).
// Column lengths must match rec.NumRows(); each column is exported in full.
func ArrowFieldsToProto(rec arrow.Record, fieldSchemaMap map[int64]*schemapb.FieldSchema) ([]*schemapb.FieldData, error) {
	numCols := int(rec.NumCols())
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
		fd, err := arrowColumnToFieldData(rec.Column(i), schema)
		if err != nil {
			// Wrapf, not WrapErrServiceInternal(..., err.Error()):
			// arrowColumnToFieldData returns merr-coded errors of its own, and
			// folding one into a format string discards its code.
			return nil, merr.Wrapf(err,
				"failed to convert Arrow column %q (field %d)",
				schema.GetName(), schema.GetFieldID())
		}
		result = append(result, fd)
	}
	return result, nil
}

// ArrowFieldsToProtoOrdered is ArrowFieldsToProto for records whose columns
// carry NO per-field milvus.field_id metadata: the ids come from fieldIDs,
// positionally, one per column.
// Column lengths must match rec.NumRows(); each column is exported in full.
//
// This exists because the metadata is expensive out of proportion to what it
// carries. Profiling the retrieve transport attributed ~44 of ~126 extra
// allocations per call to it: cdata.decodeCMetadata builds an arrow.Metadata
// per field from the C strings, and arrow.StructOf then clones every one again
// while assembling the struct type the C Data Interface uses to represent a
// batch. That is a third of the Arrow path's fixed overhead, spent to transmit
// one integer per column that the caller already knows.
//
// Unlike ArrowFieldsToProto this does NOT skip columns. A length mismatch or
// an id absent from fieldSchemaMap is an error: positional matching has no way
// to resynchronise after a skip, so a silent one would misattribute every
// subsequent column's data.
func ArrowFieldsToProtoOrdered(
	rec arrow.Record,
	fieldIDs []int64,
	fieldSchemaMap map[int64]*schemapb.FieldSchema,
) ([]*schemapb.FieldData, error) {
	numCols := int(rec.NumCols())
	if len(fieldIDs) != numCols {
		return nil, merr.WrapErrServiceInternalMsg(
			"arrow record has %d columns but %d field ids were supplied",
			numCols, len(fieldIDs))
	}

	result := make([]*schemapb.FieldData, 0, numCols)
	for i := 0; i < numCols; i++ {
		schema, ok := fieldSchemaMap[fieldIDs[i]]
		if !ok {
			return nil, merr.WrapErrServiceInternalMsg(
				"arrow column %d has field id %d, which is not in the schema",
				i, fieldIDs[i])
		}
		fd, err := arrowColumnToFieldData(rec.Column(i), schema)
		if err != nil {
			// Wrapf, not WrapErrServiceInternal(..., err.Error()), per the
			// repo's error-handling rule 2.
			//
			// Measured, so as not to over-claim: today this changes NO code and
			// NO errors.Is behavior. Every error arrowColumnToFieldData returns
			// is already WrapErrServiceInternalMsg, so both forms yield
			// ServiceInternal, and both satisfy errors.Is against the inner
			// error because it unwraps to the same merr sentinel. What the
			// stringify form does do is read "service internal error: service
			// internal error: ..." and lose the cause's position in the message.
			// The reason to use Wrapf is that it stays correct if a differently
			// coded error is ever added here -- not a defect being fixed.
			return nil, merr.Wrapf(err,
				"failed to convert Arrow column %q (field %d)",
				schema.GetName(), schema.GetFieldID())
		}
		result = append(result, fd)
	}
	return result, nil
}

// NativeColumnType returns the Arrow physical type used by native Query output.
// It describes the layout without allocating an array or its buffers.
func NativeColumnType(schema *schemapb.FieldSchema) (arrow.DataType, error) {
	if schema == nil {
		return nil, merr.WrapErrServiceInternalMsg("missing empty output field schema")
	}
	var dtype arrow.DataType
	switch schema.DataType {
	case schemapb.DataType_Bool:
		dtype = arrow.FixedWidthTypes.Boolean
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		dtype = arrow.PrimitiveTypes.Int32
	case schemapb.DataType_Int64, schemapb.DataType_Timestamptz:
		dtype = arrow.PrimitiveTypes.Int64
	case schemapb.DataType_Float:
		dtype = arrow.PrimitiveTypes.Float32
	case schemapb.DataType_Double:
		dtype = arrow.PrimitiveTypes.Float64
	case schemapb.DataType_VarChar, schemapb.DataType_Text:
		dtype = arrow.BinaryTypes.String
	case schemapb.DataType_JSON, schemapb.DataType_Array, schemapb.DataType_Geometry, schemapb.DataType_SparseFloatVector:
		dtype = arrow.BinaryTypes.Binary
	case schemapb.DataType_FloatVector, schemapb.DataType_BinaryVector, schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector, schemapb.DataType_Int8Vector, schemapb.DataType_ArrayOfVector:
		dim, err := typeutil.GetDim(schema)
		if err != nil {
			return nil, merr.Wrap(err, "empty native output column")
		}
		element := schema.DataType
		if element == schemapb.DataType_ArrayOfVector {
			element = schema.ElementType
		}
		width := dim
		switch element {
		case schemapb.DataType_FloatVector:
			width *= 4
		case schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector:
			width *= 2
		case schemapb.DataType_BinaryVector:
			width /= 8
		case schemapb.DataType_Int8Vector:
		default:
			return nil, merr.WrapErrServiceInternalMsg("unsupported empty vector element type %s", element)
		}
		if width <= 0 || int64(int(width)) != width {
			return nil, merr.WrapErrServiceInternalMsg("invalid empty vector byte width %d", width)
		}
		dtype = &arrow.FixedSizeBinaryType{ByteWidth: int(width)}
		if schema.DataType == schemapb.DataType_ArrayOfVector {
			dtype = arrow.ListOf(dtype)
		}
		// The physical type is known by construction. Check only dimensions,
		// including truncated binary widths and conflicting declarations.
		if _, err := resolveVectorDim(dtype, schema); err != nil {
			return nil, err
		}
	default:
		return nil, merr.WrapErrServiceInternalMsg("unsupported empty native output type %s", schema.DataType)
	}
	return dtype, nil
}

// ArrowChunkedColumnToFieldData converts all chunks into one owned protobuf
// payload. Output storage is allocated once, without intermediate Arrow arrays
// or per-chunk FieldData objects. Returned payloads use Go-managed memory.
func ArrowChunkedColumnToFieldData(col *arrow.Chunked, schema *schemapb.FieldSchema) (*schemapb.FieldData, error) {
	if col == nil || schema == nil {
		return nil, merr.WrapErrServiceInternalMsg("missing Arrow column or field schema")
	}
	dim, err := validateArrowColumnType(col.DataType(), schema)
	if err != nil {
		return nil, err
	}
	return arrowColumnsToFieldDataValidated(col.Chunks(), schema, dim, col.Len(), col.NullN())
}

// validateArrowColumnType checks layouts even when there are no payload rows.
func validateArrowColumnType(dtype arrow.DataType, schema *schemapb.FieldSchema) (int64, error) {
	var expected arrow.Type
	switch schema.DataType {
	case schemapb.DataType_Bool:
		expected = arrow.BOOL
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		expected = arrow.INT32
		// Native retrieval uses Int32; DataFrame operators also use narrow integers.
		if dtype.ID() == arrow.INT8 || dtype.ID() == arrow.INT16 {
			expected = dtype.ID()
		}
	case schemapb.DataType_Int64, schemapb.DataType_Timestamptz:
		expected = arrow.INT64
	case schemapb.DataType_Float:
		expected = arrow.FLOAT32
	case schemapb.DataType_Double:
		expected = arrow.FLOAT64
	case schemapb.DataType_VarChar, schemapb.DataType_String, schemapb.DataType_Text:
		expected = arrow.STRING
	case schemapb.DataType_JSON, schemapb.DataType_Array, schemapb.DataType_SparseFloatVector:
		expected = arrow.BINARY
	case schemapb.DataType_Geometry:
		expected = arrow.BINARY
		if dtype.ID() == arrow.STRING {
			expected = arrow.STRING
		}
	case schemapb.DataType_FloatVector, schemapb.DataType_BinaryVector, schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector, schemapb.DataType_Int8Vector:
		expected = arrow.FIXED_SIZE_BINARY
	case schemapb.DataType_ArrayOfVector:
		expected = arrow.LIST
		if list, ok := dtype.(*arrow.ListType); !ok || list.Elem().ID() != arrow.FIXED_SIZE_BINARY {
			return 0, merr.WrapErrServiceInternalMsg("unsupported type %s for native Arrow vector array layout %s", schema.DataType, dtype)
		}
	}
	if dtype.ID() != expected || expected == arrow.NULL {
		return 0, merr.WrapErrServiceInternalMsg("unsupported type %s for native Arrow layout %s", schema.DataType, dtype)
	}
	var dim int64
	switch schema.DataType {
	case schemapb.DataType_FloatVector, schemapb.DataType_BinaryVector, schemapb.DataType_Float16Vector,
		schemapb.DataType_BFloat16Vector, schemapb.DataType_Int8Vector, schemapb.DataType_ArrayOfVector:
		var err error
		dim, err = resolveVectorDim(dtype, schema)
		if err != nil {
			return 0, err
		}
	}
	return dim, nil
}

// emptyColumnFieldData preserves the converter's protobuf layout without
// constructing temporary Arrow arrays. dim has already been validated/inferred.
func emptyColumnFieldData(schema *schemapb.FieldSchema, dim int64) (*schemapb.FieldData, error) {
	definition := &schemapb.FieldSchema{
		Name: schema.Name, FieldID: schema.FieldID, DataType: schema.DataType, ElementType: schema.ElementType,
	}
	if dim > 0 {
		definition.TypeParams = []*commonpb.KeyValuePair{{Key: "dim", Value: strconv.FormatInt(dim, 10)}}
	}
	field, err := typeutil.GenEmptyFieldData(definition)
	if err != nil {
		return nil, merr.Wrap(err, "export empty Arrow column")
	}
	if schema.DataType == schemapb.DataType_Timestamptz {
		// The shared helper retains its historical LongData representation.
		// Arrow export uses TimestamptzData for both empty and populated columns.
		field.GetScalars().Data = &schemapb.ScalarField_TimestamptzData{TimestamptzData: &schemapb.TimestamptzArray{}}
	}
	return field, nil
}

// arrowColumnToFieldData converts one Arrow column using its declared field
// schema, including native retrieval and DataFrame scalar layouts. It owns all returned payloads and borrows the array.
func arrowColumnToFieldData(col arrow.Array, schema *schemapb.FieldSchema) (*schemapb.FieldData, error) {
	if col == nil || schema == nil {
		return nil, merr.WrapErrServiceInternalMsg("missing Arrow column or field schema")
	}
	dim, err := validateArrowColumnType(col.DataType(), schema)
	if err != nil {
		return nil, err
	}
	return arrowColumnsToFieldDataValidated([]arrow.Array{col}, schema, dim, col.Len(), col.NullN())
}

// arrowColumnsToFieldDataValidated shares one writer for single-array and
// chunked columns. All chunks have the validated physical type and dimension.
func arrowColumnsToFieldDataValidated(chunks []arrow.Array, schema *schemapb.FieldSchema, dim int64, numRows, numNulls int) (*schemapb.FieldData, error) {
	if numRows == 0 {
		return emptyColumnFieldData(schema, dim)
	}
	fd := &schemapb.FieldData{
		Type:      schema.GetDataType(),
		FieldName: schema.GetName(),
		FieldId:   schema.GetFieldID(),
	}

	switch schema.GetDataType() {
	case schemapb.DataType_Bool:
		data := make([]bool, numRows)
		offset := 0
		for _, chunk := range chunks {
			arr := chunk.(*array.Boolean)
			for j := 0; j < arr.Len(); j++ {
				data[offset+j] = arr.Value(j)
			}
			offset += arr.Len()
		}
		fd.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_BoolData{
					BoolData: &schemapb.BoolArray{Data: data},
				},
			},
		}

	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		// All integer widths share protobuf int_data.
		data := make([]int32, numRows)
		offset := 0
		switch chunks[0].(type) {
		case *array.Int8:
			for _, chunk := range chunks {
				arr := chunk.(*array.Int8)
				for j := 0; j < arr.Len(); j++ {
					data[offset+j] = int32(arr.Value(j))
				}
				offset += arr.Len()
			}
		case *array.Int16:
			for _, chunk := range chunks {
				arr := chunk.(*array.Int16)
				for j := 0; j < arr.Len(); j++ {
					data[offset+j] = int32(arr.Value(j))
				}
				offset += arr.Len()
			}
		case *array.Int32:
			for _, chunk := range chunks {
				offset += copy(data[offset:], chunk.(*array.Int32).Int32Values())
			}
		default:
			return nil, merr.WrapErrServiceInternalMsg("unsupported integer Arrow layout %s", chunks[0].DataType())
		}
		fd.Field = intFieldData(data)

	case schemapb.DataType_Int64, schemapb.DataType_Timestamptz:
		data := make([]int64, numRows)
		offset := 0
		for _, chunk := range chunks {
			offset += copy(data[offset:], chunk.(*array.Int64).Int64Values())
		}
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
		data := make([]float32, numRows)
		offset := 0
		for _, chunk := range chunks {
			offset += copy(data[offset:], chunk.(*array.Float32).Float32Values())
		}
		fd.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_FloatData{
					FloatData: &schemapb.FloatArray{Data: data},
				},
			},
		}

	case schemapb.DataType_Double:
		data := make([]float64, numRows)
		offset := 0
		for _, chunk := range chunks {
			offset += copy(data[offset:], chunk.(*array.Float64).Float64Values())
		}
		fd.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_DoubleData{
					DoubleData: &schemapb.DoubleArray{Data: data},
				},
			},
		}

	case schemapb.DataType_VarChar, schemapb.DataType_String, schemapb.DataType_Text:
		data := make([]string, numRows)
		offset := 0
		for _, chunk := range chunks {
			arr := chunk.(*array.String)
			for j := 0; j < arr.Len(); j++ {
				// Arrow strings alias its buffers; output must survive Release.
				data[offset+j] = strings.Clone(arr.Value(j))
			}
			offset += arr.Len()
		}
		fd.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{Data: data},
				},
			},
		}

	case schemapb.DataType_FloatVector:
		floatData := compactFloatVector(chunks, numRows-numNulls, int(dim))
		fd.Field = &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: dim,
				Data: &schemapb.VectorField_FloatVector{
					FloatVector: &schemapb.FloatArray{Data: floatData},
				},
			},
		}

	case schemapb.DataType_BinaryVector, schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector:
		fd.Field = bytesVectorFieldData(chunks, schema, numRows-numNulls, dim)

	case schemapb.DataType_JSON:
		rows := make([][]byte, numRows)
		offset := 0
		for _, chunk := range chunks {
			arr := chunk.(*array.Binary)
			for j := 0; j < arr.Len(); j++ {
				rows[offset+j] = append([]byte{}, arr.Value(j)...)
			}
			offset += arr.Len()
		}
		fd.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_JsonData{
					JsonData: &schemapb.JSONArray{Data: rows},
				},
			},
		}

	case schemapb.DataType_SparseFloatVector:
		contents, maxDim := compactSparseVector(chunks, numRows-numNulls)
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
		rows := make([][]byte, numRows)
		offset := 0
		switch chunks[0].(type) {
		case *array.String:
			for _, chunk := range chunks {
				arr := chunk.(*array.String)
				for j := 0; j < arr.Len(); j++ {
					rows[offset+j] = []byte(arr.Value(j))
				}
				offset += arr.Len()
			}
		case *array.Binary:
			for _, chunk := range chunks {
				arr := chunk.(*array.Binary)
				for j := 0; j < arr.Len(); j++ {
					rows[offset+j] = append([]byte{}, arr.Value(j)...)
				}
				offset += arr.Len()
			}
		default:
			return nil, merr.WrapErrServiceInternalMsg("unsupported geometry Arrow layout %s", chunks[0].DataType())
		}
		fd.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_GeometryData{
					GeometryData: &schemapb.GeometryArray{Data: rows},
				},
			},
		}

	case schemapb.DataType_Int8Vector:
		data := compactBytesVector(chunks, numRows-numNulls)
		fd.Field = &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim:  dim,
				Data: &schemapb.VectorField_Int8Vector{Int8Vector: data},
			},
		}

	case schemapb.DataType_ArrayOfVector:
		fieldsData, err := arrowListToVectorArray(chunks, schema, numRows, dim)
		if err != nil {
			return nil, err
		}
		fd.Field = fieldsData

	case schemapb.DataType_Array:
		data := make([]*schemapb.ScalarField, numRows)
		offset := 0
		for _, chunk := range chunks {
			arr := chunk.(*array.Binary)
			for j := 0; j < arr.Len(); j++ {
				sf := &schemapb.ScalarField{}
				if err := proto.Unmarshal(arr.Value(j), sf); err != nil {
					return nil, merr.WrapErrServiceInternalMsg("failed to unmarshal Array element at row %d for field %q: %s", offset+j, schema.GetName(), err)
				}
				data[offset+j] = sf
			}
			offset += arr.Len()
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

	setArrowValidData(fd, chunks, numRows, numNulls, schema.GetNullable())
	return fd, nil
}

func setArrowValidData(fd *schemapb.FieldData, chunks []arrow.Array, numRows, numNulls int, nullable bool) {
	if numNulls == 0 && !nullable {
		return
	}
	validData := make([]bool, numRows)
	offset := 0
	for _, chunk := range chunks {
		if chunk.NullN() == 0 {
			for j := 0; j < chunk.Len(); j++ {
				validData[offset+j] = true
			}
		} else {
			for j := 0; j < chunk.Len(); j++ {
				validData[offset+j] = chunk.IsValid(j)
			}
		}
		offset += chunk.Len()
	}
	typeutil.SetFieldDataValidData(fd, validData)
}

// resolveVectorDim validates declared dimensions against physical byte width.
// If no dimension is declared, the unambiguous physical layout supplies it.
// Invalid declarations never fall back to inference or pad/truncate payloads.
func resolveVectorDim(dtype arrow.DataType, schema *schemapb.FieldSchema) (int64, error) {
	logical := schema.DataType
	if logical == schemapb.DataType_ArrayOfVector {
		logical = schema.ElementType
		if list, ok := dtype.(*arrow.ListType); ok {
			dtype = list.Elem()
		}
	}
	fixed, ok := dtype.(*arrow.FixedSizeBinaryType)
	if !ok || fixed.ByteWidth <= 0 {
		return 0, merr.WrapErrServiceInternalMsg("invalid vector layout for %s", schema.Name)
	}
	width := int64(fixed.ByteWidth)
	var physical int64
	switch logical {
	case schemapb.DataType_FloatVector:
		if width%4 == 0 {
			physical = width / 4
		}
	case schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector:
		if width%2 == 0 {
			physical = width / 2
		}
	case schemapb.DataType_BinaryVector:
		if width <= math.MaxInt64/8 {
			physical = width * 8
		}
	case schemapb.DataType_Int8Vector:
		physical = width
	}
	if physical == 0 {
		return 0, merr.WrapErrServiceInternalMsg("vector type %s does not match byte width %d", logical, width)
	}
	// Type parameters take precedence over index parameters, as in GetDim.
	for _, params := range [][]*commonpb.KeyValuePair{schema.TypeParams, schema.IndexParams} {
		found := false
		for _, param := range params {
			if param.GetKey() != "dim" {
				continue
			}
			dim, err := strconv.ParseInt(param.GetValue(), 10, 64)
			if found || err != nil || dim != physical {
				return 0, merr.WrapErrServiceInternalMsg("vector %s declared dimension %q does not match physical dimension %d", schema.Name, param.GetValue(), physical)
			}
			found = true
		}
		if found {
			return physical, nil
		}
	}
	return physical, nil
}

func bytesVectorFieldData(chunks []arrow.Array, schema *schemapb.FieldSchema, validRows int, dim int64) *schemapb.FieldData_Vectors {
	data := compactBytesVector(chunks, validRows)

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

// Vector payloads contain only valid rows; ValidData covers all logical rows.
func compactFloatVector(chunks []arrow.Array, validRows, dim int) []float32 {
	data := make([]float32, validRows*dim)
	offset := 0
	for _, chunk := range chunks {
		arr := chunk.(*array.FixedSizeBinary)
		if arr.NullN() == 0 {
			offset += copy(data[offset:], arrow.Float32Traits.CastFromBytes(fixedSizeBinaryBytes(arr, arr.Len())))
		} else {
			for j := 0; j < arr.Len(); j++ {
				if arr.IsValid(j) {
					offset += copy(data[offset:], arrow.Float32Traits.CastFromBytes(arr.Value(j)))
				}
			}
		}
	}
	return data
}

func compactSparseVector(chunks []arrow.Array, validRows int) ([][]byte, int64) {
	contents := make([][]byte, validRows)
	var maxDim int64
	physical := 0
	for _, chunk := range chunks {
		arr := chunk.(*array.Binary)
		for j := 0; j < arr.Len(); j++ {
			if !arr.IsValid(j) {
				continue
			}
			row := arr.Value(j)
			contents[physical] = append([]byte{}, row...)
			for k := 0; k < len(row)/8; k++ {
				idx := int64(binary.LittleEndian.Uint32(row[k*8:])) + 1
				if idx > maxDim {
					maxDim = idx
				}
			}
			physical++
		}
	}
	return contents, maxDim
}

func compactBytesVector(chunks []arrow.Array, validRows int) []byte {
	byteWidth := chunks[0].DataType().(*arrow.FixedSizeBinaryType).ByteWidth
	data := make([]byte, validRows*byteWidth)
	offset := 0
	for _, chunk := range chunks {
		arr := chunk.(*array.FixedSizeBinary)
		if arr.NullN() == 0 {
			offset += copy(data[offset:], fixedSizeBinaryBytes(arr, arr.Len()))
		} else {
			for j := 0; j < arr.Len(); j++ {
				if arr.IsValid(j) {
					offset += copy(data[offset:], arr.Value(j))
				}
			}
		}
	}
	return data
}

// arrowListToVectorArray converts an Arrow List(FixedSizeBinary) column into
// a proto VectorField_VectorArray.  Each list element is a single vector.
func arrowListToVectorArray(chunks []arrow.Array, schema *schemapb.FieldSchema, numRows int, dim int64) (*schemapb.FieldData_Vectors, error) {
	elemType := schema.GetElementType()
	vectors := make([]*schemapb.VectorField, numRows)
	offset := 0
	for _, chunk := range chunks {
		listArr := chunk.(*array.List)
		for j := 0; j < listArr.Len(); j++ {
			if !listArr.IsValid(j) {
				vectors[offset+j] = &schemapb.VectorField{Dim: dim}
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
			vectors[offset+j] = vf
		}
		offset += listArr.Len()
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
