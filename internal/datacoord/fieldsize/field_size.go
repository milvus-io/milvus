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

package fieldsize

import (
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Field-width and schema-size helpers used by DataCoord's task memory
// estimates. Metadata size estimation also reuses FixedFieldWidth; legacy
// scalar-slot estimates use meta.EstimateFieldsReadSize and its own reader
// projection policy, not SchemaFieldSize or taskresource.EstimateFieldSize.
//
// The binlog size recorded in meta is a measured input. In storage v2/v3 it can
// cover a whole column group rather than one field. The taskresource caller
// caps VarChar at its schema upper bound and projected non-nullable
// fixed-width fields at their exact width. This package supplies the bounds.
//
// The schema's upper bound also supplies a fallback when meta has no binlog
// size for the field (a storage v3 segment after a DataCoord restart: its
// per-field binlogs live in the manifest and are not persisted in meta) or the
// recorded size is not positive. Nullable fields include a validity bitmap,
// and nullable vectors include offsets. Payload is bounded by rows x width for
// a fixed-width type, or rows x the per-row limit the write path enforces for a
// variable-width one -- max_length for a varchar, common.JSONMaxLength for a
// JSON value, max_capacity x element width for an array. Text, geometry and
// the other types the write path does not bound have no schema bound at all.
//
// The rule is the same for every segment, internal or external: only the data
// recorded in meta decides which step answers.

// arrowOffsetBytes is the width of an offset in an Arrow binary or string array.
const arrowOffsetBytes = 4

// SchemaFieldSize returns the schema's upper bound on the bytes that rows
// values of field occupy. exact is true for non-nullable fixed-width fields.
// ok is false when the schema does not bound the type (text, geometry, sparse
// and array-of-vector fields, a varchar without max_length, an array without
// max_capacity, a vector without dim).
//
// Every bound here is one the write path enforces, so it holds for data already
// in a segment. It is never an average: an average that a caller mistakes for a
// bound under-prices a task, and the worker books the under-priced number while
// reading the real column.
//
// Text and geometry have no bound. proxy.maxTextLength is not applied on insert
// (checkTextFieldData returns without a length check), and geometry is only
// checked for WKB convertibility, so neither limit can be relied on here.
func SchemaFieldSize(field *schemapb.FieldSchema, rows int64) (size int64, exact bool, ok bool) {
	if rows <= 0 {
		return 0, false, false
	}
	validity := int64(0)
	if field.GetNullable() {
		validity = (rows + 7) / 8
	}
	if width := FixedFieldWidth(field); width > 0 {
		size := rows*width + validity
		if field.GetNullable() && typeutil.IsFixDimVectorType(field.GetDataType()) {
			// Nullable vectors use Arrow binary arrays with a terminal offset.
			size += (rows + 1) * arrowOffsetBytes
		}
		return size, !field.GetNullable(), true
	}
	switch field.GetDataType() {
	case schemapb.DataType_VarChar:
		maxLength := typeParamInt(field, common.MaxLengthKey)
		if maxLength <= 0 {
			return 0, false, false
		}
		return rows*(maxLength+arrowOffsetBytes) + validity, false, true
	case schemapb.DataType_JSON:
		// The proxy rejects a JSON value longer than common.JSONMaxLength
		// (validate_util.go checkJSONFieldData), so that is a real per-row bound.
		maxLength := paramtable.Get().CommonCfg.JSONMaxLength.GetAsInt64()
		if maxLength <= 0 {
			return 0, false, false
		}
		return rows*(maxLength+arrowOffsetBytes) + validity, false, true
	case schemapb.DataType_Array:
		// The proxy rejects an array longer than max_capacity
		// (validate_util.go verifyCapacityPerRow), so capacity times the widest
		// element bounds one row.
		element := arrayElementWidth(field)
		capacity := typeParamInt(field, common.MaxCapacityKey)
		if element <= 0 || capacity <= 0 {
			return 0, false, false
		}
		return rows*capacity*element + validity, false, true
	}
	return 0, false, false
}

// arrayElementWidth is the bytes of one array element: its fixed width, or the
// varchar bound for a varchar element. 0 when the element type has no bound.
func arrayElementWidth(field *schemapb.FieldSchema) int64 {
	switch field.GetElementType() {
	case schemapb.DataType_VarChar:
		maxLength := typeParamInt(field, common.MaxLengthKey)
		if maxLength <= 0 {
			return 0
		}
		return maxLength + arrowOffsetBytes
	default:
		return FixedFieldWidth(&schemapb.FieldSchema{DataType: field.GetElementType()})
	}
}

// FixedFieldWidth returns the bytes per row of a fixed-width field, or 0 for
// a variable-width one or a vector whose dim is unknown.
func FixedFieldWidth(field *schemapb.FieldSchema) int64 {
	switch field.GetDataType() {
	case schemapb.DataType_Bool, schemapb.DataType_Int8:
		return 1
	case schemapb.DataType_Int16:
		return 2
	case schemapb.DataType_Int32, schemapb.DataType_Float:
		return 4
	case schemapb.DataType_Int64, schemapb.DataType_Double, schemapb.DataType_Timestamptz:
		return 8
	case schemapb.DataType_FloatVector, schemapb.DataType_BinaryVector,
		schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector, schemapb.DataType_Int8Vector:
		dim, err := typeutil.GetDim(field)
		if err != nil || dim <= 0 {
			return 0
		}
		return int64(float64(dim) * typeutil.VectorTypeSize(field.GetDataType()))
	default:
		return 0
	}
}

// EstimateFieldSize sizes rows values of field: binlogSize when it is
// positive, otherwise the schema's upper bound. It returns 0 when neither is
// known, which the caller must answer with a coarser bound, never with "free".
func EstimateFieldSize(field *schemapb.FieldSchema, rows int64, binlogSize int64) int64 {
	if binlogSize > 0 {
		return binlogSize
	}
	if bound, _, ok := SchemaFieldSize(field, rows); ok && bound > 0 {
		return bound
	}
	return 0
}

func typeParamInt(field *schemapb.FieldSchema, key string) int64 {
	for _, kv := range field.GetTypeParams() {
		if kv.GetKey() != key {
			continue
		}
		if v, err := strconv.ParseInt(kv.GetValue(), 10, 64); err == nil {
			return v
		}
	}
	return 0
}
