/*
 * # Licensed to the LF AI & Data foundation under one
 * # or more contributor license agreements. See the NOTICE file
 * # distributed with this work for additional information
 * # regarding copyright ownership. The ASF licenses this file
 * # to you under the Apache License, Version 2.0 (the
 * # "License"); you may not use this file except in compliance
 * # with the License. You may obtain a copy of the License at
 * #
 * #     http://www.apache.org/licenses/LICENSE-2.0
 * #
 * # Unless required by applicable law or agreed to in writing, software
 * # distributed under the License is distributed on an "AS IS" BASIS,
 * # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * # See the License for the specific language governing permissions and
 * # limitations under the License.
 */

package chain

import (
	"strconv"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Vector columns are an internal, input-only representation used by
// operators that need stored vectors (currently MMR). Every vector type is
// held as an Arrow Binary array with one opaque value per row:
//
//   - fixed-width vectors (float, float16, bfloat16, int8, binary): the row's
//     raw bytes in the Milvus wire layout (float32 is little-endian)
//   - SparseFloatVector: the row's sparse content bytes
//   - ArrayOfVector: the raw bytes of the row's element vectors, concatenated
//
// The Milvus data type, dim, and element type live in the column's field
// schema metadata. Row selection copies the bytes without interpreting them.
// The proxy runs rerank chains with column pruning, so the column is dropped
// after its last reader and never exported.

// ValidateMMRInputField rejects fields that MMR cannot read: non-vector
// fields, and fields whose raw data cannot be retrieved (BM25 function
// outputs), since MMR fetches the stored vectors of its candidates.
func ValidateMMRInputField(schemaHelper *typeutil.SchemaHelper, field *schemapb.FieldSchema) error {
	if !typeutil.IsVectorType(field.GetDataType()) {
		return merr.WrapErrParameterInvalidMsg("mmr: input field %q is not a vector field", field.GetName())
	}
	if !schemaHelper.CanRetrieveRawFieldData(field) {
		return merr.WrapErrParameterInvalidMsg(
			"mmr: the vectors of input field %q cannot be retrieved (BM25 function output); use a dense vector field", field.GetName())
	}
	return nil
}

// vectorByteWidth is the byte size of one fixed-width vector, or 0 for types
// without a fixed width.
func vectorByteWidth(dataType schemapb.DataType, dim int64) int64 {
	switch dataType {
	case schemapb.DataType_FloatVector:
		return dim * 4
	case schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector:
		return dim * 2
	case schemapb.DataType_Int8Vector:
		return dim
	case schemapb.DataType_BinaryVector:
		return dim / 8
	default:
		return 0
	}
}

// fixedVectorBytes returns the raw payload of a fixed-width VectorField
// without copying.
func fixedVectorBytes(dataType schemapb.DataType, vf *schemapb.VectorField) []byte {
	switch dataType {
	case schemapb.DataType_FloatVector:
		return arrow.Float32Traits.CastToBytes(vf.GetFloatVector().GetData())
	case schemapb.DataType_Float16Vector:
		return vf.GetFloat16Vector()
	case schemapb.DataType_BFloat16Vector:
		return vf.GetBfloat16Vector()
	case schemapb.DataType_Int8Vector:
		return vf.GetInt8Vector()
	case schemapb.DataType_BinaryVector:
		return vf.GetBinaryVector()
	default:
		return nil
	}
}

// vectorRows splits a vector FieldData payload into per-row byte slices
// without copying, and returns the ArrayOfVector element type.
func vectorRows(dataType schemapb.DataType, vectors *schemapb.VectorField, fieldName string) ([][]byte, schemapb.DataType, error) {
	switch dataType {
	case schemapb.DataType_SparseFloatVector:
		return vectors.GetSparseFloatVector().GetContents(), schemapb.DataType_None, nil
	case schemapb.DataType_ArrayOfVector:
		va := vectors.GetVectorArray()
		rows := make([][]byte, len(va.GetData()))
		for i, vf := range va.GetData() {
			rows[i] = fixedVectorBytes(va.GetElementType(), vf)
		}
		return rows, va.GetElementType(), nil
	}
	data := fixedVectorBytes(dataType, vectors)
	if len(data) == 0 {
		// All rows are null; the payload may omit the dim.
		return nil, schemapb.DataType_None, nil
	}
	width := vectorByteWidth(dataType, vectors.GetDim())
	if width <= 0 || int64(len(data))%width != 0 {
		return nil, schemapb.DataType_None, merr.WrapErrServiceInternalMsg(
			"field %s: %s payload of %d bytes does not match dim %d", fieldName, dataType, len(data), vectors.GetDim())
	}
	rows := make([][]byte, int64(len(data))/width)
	for i := range rows {
		rows[i] = data[int64(i)*width : int64(i+1)*width]
	}
	return rows, schemapb.DataType_None, nil
}

// importVectorFieldData converts a vector FieldData into per-chunk opaque
// Binary arrays. Nullable vector fields may carry their payload either densely
// (one entry per logical row) or compacted (one entry per valid row).
func importVectorFieldData(
	fieldData *schemapb.FieldData,
	fieldName string,
	offsets []int64,
	validData []bool,
	alloc memory.Allocator,
) ([]arrow.Array, *schemapb.FieldSchema, error) {
	vectors := fieldData.GetVectors()
	if vectors == nil {
		return nil, nil, merr.WrapErrServiceInternalMsg("field %s: vectors is nil", fieldName)
	}
	totalRows := offsets[len(offsets)-1]
	rows, elementType, err := vectorRows(fieldData.GetType(), vectors, fieldName)
	if err != nil {
		return nil, nil, err
	}

	validCount := totalRows
	if len(validData) > 0 {
		validCount = int64(lo.Count(validData[:totalRows], true))
	}
	physicalRows := int64(len(rows))
	compacted := physicalRows != totalRows
	if compacted && physicalRows != validCount {
		return nil, nil, merr.WrapErrServiceInternalMsg(
			"field %s: vector row count (%d) matches neither totalRows (%d) nor valid rows (%d)",
			fieldName, physicalRows, totalRows, validCount)
	}

	chunks := make([]arrow.Array, 0, len(offsets)-1)
	physical := 0
	for chunkIdx := 0; chunkIdx < len(offsets)-1; chunkIdx++ {
		builder := array.NewBinaryBuilder(alloc, arrow.BinaryTypes.Binary)
		for row := offsets[chunkIdx]; row < offsets[chunkIdx+1]; row++ {
			if len(validData) > 0 && !validData[row] {
				builder.AppendNull()
				if !compacted {
					physical++
				}
				continue
			}
			builder.Append(rows[physical])
			physical++
		}
		chunks = append(chunks, builder.NewArray())
		builder.Release()
	}

	schema := &schemapb.FieldSchema{
		Name:        fieldName,
		FieldID:     fieldData.GetFieldId(),
		DataType:    fieldData.GetType(),
		Nullable:    len(validData) > 0,
		ElementType: elementType,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: strconv.FormatInt(vectors.GetDim(), 10)},
		},
	}
	return chunks, schema, nil
}
