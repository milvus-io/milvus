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

package taskcommon

import (
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Field size estimation shared by every place that sizes one field of a
// segment for a task: DataCoord's memory estimate and the scalar task slot
// derived from the same size, and the accepting DataNode's correction.
//
// A field's size is the smaller of two upper bounds:
//   - the schema's: rows x width for a fixed-width type, rows x (max_length +
//     offset) for a varchar, since the proxy rejects a varchar longer than
//     max_length bytes;
//   - the container's: the memory size of the binlogs that hold the field. In
//     storage v2/v3 a binlog holds a whole column group, so this bounds the
//     field by everything it shares the group with.
//
// Neither bound alone is the field: the schema cannot see that a varchar is
// short, and a column group cannot tell its fields apart.

const (
	// SystemFieldsBytesPerRow is RowID plus Timestamp, both int64. Every
	// segment stores them, whether or not the schema at hand lists them.
	SystemFieldsBytesPerRow = 16
	// varCharOffsetBytes is the per-value offset of an Arrow string array.
	varCharOffsetBytes = 4
)

// SchemaFieldSize returns the schema's upper bound on the bytes that rows
// values of field occupy. exact is true for a fixed-width type, whose bound is
// its size. ok is false when the schema does not bound the type (json, text,
// array, geometry, sparse and array-of-vector fields, a varchar without
// max_length, a vector without dim).
func SchemaFieldSize(field *schemapb.FieldSchema, rows int64) (size int64, exact bool, ok bool) {
	if field == nil || rows <= 0 {
		return 0, false, false
	}
	validity := int64(0)
	if field.GetNullable() {
		validity = (rows + 7) / 8
	}
	if width := FixedFieldWidth(field); width > 0 {
		return rows*width + validity, true, true
	}
	if field.GetDataType() == schemapb.DataType_VarChar {
		maxLength := typeParamInt(field, common.MaxLengthKey)
		if maxLength <= 0 {
			return 0, false, false
		}
		return rows*(maxLength+varCharOffsetBytes) + validity, false, true
	}
	return 0, false, false
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

// ColumnGroupSize sums the memory size of the binlogs that hold fieldID: its
// own FieldBinlog in storage v1, or the column group listing it as a child in
// storage v2/v3. Zero when the binlogs are not known (a v3 segment after a
// DataCoord restart).
func ColumnGroupSize(binlogs []*datapb.FieldBinlog, fieldID int64) int64 {
	var size int64
	for _, fieldBinlog := range binlogs {
		holds := fieldBinlog.GetFieldID() == fieldID
		for _, child := range fieldBinlog.GetChildFields() {
			holds = holds || child == fieldID
		}
		if !holds {
			continue
		}
		for _, binlog := range fieldBinlog.GetBinlogs() {
			size += binlog.GetMemorySize()
		}
	}
	return size
}

// EstimateFieldSize is min(schema bound, container) for rows values of field.
// container <= 0 means the container is unknown. It returns 0 when neither
// bound is known.
func EstimateFieldSize(field *schemapb.FieldSchema, rows int64, container int64) int64 {
	bound, _, bounded := SchemaFieldSize(field, rows)
	switch {
	case bounded && container > 0:
		return min(bound, container)
	case bounded:
		return bound
	case container > 0:
		return container
	default:
		return 0
	}
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
