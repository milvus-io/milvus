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

package storage

import (
	"fmt"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func appendValueAt(builder array.Builder, a arrow.Array, idx int, defaultValue *schemapb.ValueField) (uint64, error) {
	switch b := builder.(type) {
	case *array.BooleanBuilder:
		if a == nil {
			if defaultValue != nil {
				b.Append(defaultValue.GetBoolData())
				return 1, nil
			} else {
				b.AppendNull()
				return 0, nil
			}
		}
		ba, ok := a.(*array.Boolean)
		if !ok {
			return 0, fmt.Errorf("invalid value type %T, expect %T", a.DataType(), builder.Type())
		}
		if ba.IsNull(idx) {
			b.AppendNull()
			return 0, nil
		} else {
			b.Append(ba.Value(idx))
			return 1, nil
		}
	case *array.Int8Builder:
		if a == nil {
			if defaultValue != nil {
				b.Append(int8(defaultValue.GetIntData()))
				return 1, nil
			} else {
				b.AppendNull()
				return 0, nil
			}
		}
		ia, ok := a.(*array.Int8)
		if !ok {
			return 0, fmt.Errorf("invalid value type %T, expect %T", a.DataType(), builder.Type())
		}
		if ia.IsNull(idx) {
			b.AppendNull()
			return 0, nil
		} else {
			b.Append(ia.Value(idx))
			return 1, nil
		}
	case *array.Int16Builder:
		if a == nil {
			if defaultValue != nil {
				b.Append(int16(defaultValue.GetIntData()))
				return 2, nil
			} else {
				b.AppendNull()
				return 0, nil
			}
		}
		ia, ok := a.(*array.Int16)
		if !ok {
			return 0, fmt.Errorf("invalid value type %T, expect %T", a.DataType(), builder.Type())
		}
		if ia.IsNull(idx) {
			b.AppendNull()
			return 0, nil
		} else {
			b.Append(ia.Value(idx))
			return 2, nil
		}
	case *array.Int32Builder:
		if a == nil {
			if defaultValue != nil {
				b.Append(defaultValue.GetIntData())
				return 4, nil
			} else {
				b.AppendNull()
				return 0, nil
			}
		}
		ia, ok := a.(*array.Int32)
		if !ok {
			return 0, fmt.Errorf("invalid value type %T, expect %T", a.DataType(), builder.Type())
		}
		if ia.IsNull(idx) {
			b.AppendNull()
			return 0, nil
		} else {
			b.Append(ia.Value(idx))
			return 4, nil
		}
	case *array.Int64Builder:
		if a == nil {
			if defaultValue != nil {
				b.Append(defaultValue.GetLongData())
				return 8, nil
			} else {
				b.AppendNull()
				return 0, nil
			}
		}
		ia, ok := a.(*array.Int64)
		if !ok {
			return 0, fmt.Errorf("invalid value type %T, expect %T", a.DataType(), builder.Type())
		}
		if ia.IsNull(idx) {
			b.AppendNull()
			return 0, nil
		} else {
			b.Append(ia.Value(idx))
			return 8, nil
		}
	case *array.Float32Builder:
		if a == nil {
			if defaultValue != nil {
				b.Append(defaultValue.GetFloatData())
				return 4, nil
			} else {
				b.AppendNull()
				return 0, nil
			}
		}
		fa, ok := a.(*array.Float32)
		if !ok {
			return 0, fmt.Errorf("invalid value type %T, expect %T", a.DataType(), builder.Type())
		}
		if fa.IsNull(idx) {
			b.AppendNull()
			return 0, nil
		} else {
			b.Append(fa.Value(idx))
			return 4, nil
		}
	case *array.Float64Builder:
		if a == nil {
			if defaultValue != nil {
				b.Append(defaultValue.GetDoubleData())
				return 8, nil
			} else {
				b.AppendNull()
				return 0, nil
			}
		}
		fa, ok := a.(*array.Float64)
		if !ok {
			return 0, fmt.Errorf("invalid value type %T, expect %T", a.DataType(), builder.Type())
		}
		if fa.IsNull(idx) {
			b.AppendNull()
			return 0, nil
		} else {
			b.Append(fa.Value(idx))
			return 8, nil
		}
	case *array.StringBuilder:
		if a == nil {
			if defaultValue != nil {
				val := defaultValue.GetStringData()
				b.Append(val)
				return uint64(len(val)), nil
			} else {
				b.AppendNull()
				return 0, nil
			}
		}
		sa, ok := a.(*array.String)
		if !ok {
			return 0, fmt.Errorf("invalid value type %T, expect %T", a.DataType(), builder.Type())
		}
		if sa.IsNull(idx) {
			b.AppendNull()
			return 0, nil
		} else {
			val := sa.Value(idx)
			b.Append(val)
			return uint64(len(val)), nil
		}
	case *array.BinaryBuilder:
		// array type, not support defaultValue now
		if a == nil {
			b.AppendNull()
			return 0, nil
		}
		ba, ok := a.(*array.Binary)
		if !ok {
			return 0, fmt.Errorf("invalid value type %T, expect %T", a.DataType(), builder.Type())
		}
		if ba.IsNull(idx) {
			b.AppendNull()
			return 0, nil
		} else {
			val := ba.Value(idx)
			b.Append(val)
			return uint64(len(val)), nil
		}
	case *array.FixedSizeBinaryBuilder:
		ba, ok := a.(*array.FixedSizeBinary)
		if !ok {
			return 0, fmt.Errorf("invalid value type %T, expect %T", a.DataType(), builder.Type())
		}
		if ba.IsNull(idx) {
			b.AppendNull()
			return 0, nil
		} else {
			val := ba.Value(idx)
			b.Append(val)
			return uint64(len(val)), nil
		}
	case *array.ListBuilder:
		// Handle ListBuilder for ArrayOfVector type
		if a == nil {
			b.AppendNull()
			return 0, nil
		}
		la, ok := a.(*array.List)
		if !ok {
			return 0, fmt.Errorf("invalid value type %T, expect %T", a.DataType(), builder.Type())
		}
		if la.IsNull(idx) {
			b.AppendNull()
			return 0, nil
		}

		start, end := la.ValueOffsets(idx)
		b.Append(true)

		valuesArray := la.ListValues()
		valueBuilder := b.ValueBuilder()

		var totalSize uint64 = 0
		switch vb := valueBuilder.(type) {
		case *array.Float32Builder:
			if floatArray, ok := valuesArray.(*array.Float32); ok {
				for i := start; i < end; i++ {
					vb.Append(floatArray.Value(int(i)))
					totalSize += 4
				}
			}
		default:
			return 0, fmt.Errorf("unsupported value builder type in ListBuilder: %T", valueBuilder)
		}

		return totalSize, nil
	default:
		return 0, fmt.Errorf("unsupported builder type: %T", builder)
	}
}

// GenerateEmptyArrayFromSchema generate empty array from schema
// If schema has default value, the array will bef filled with it.
// Otherwise, null will be used instead.
// If input schema is not nullable, an error will be returned.
func GenerateEmptyArrayFromSchema(schema *schemapb.FieldSchema, numRows int) (arrow.Array, error) {
	// if not nullable, return error
	if !schema.GetNullable() {
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("missing field data %s", schema.Name))
	}
	dim, _ := typeutil.GetDim(schema)

	elementType := schemapb.DataType_None
	if schema.GetDataType() == schemapb.DataType_ArrayOfVector {
		elementType = schema.GetElementType()
	}
	builder := array.NewBuilder(memory.DefaultAllocator, serdeMap[schema.GetDataType()].arrowType(int(dim), elementType)) // serdeEntry[schema.GetDataType()].newBuilder()
	if schema.GetDefaultValue() != nil {
		switch schema.GetDataType() {
		case schemapb.DataType_Bool:
			bd := builder.(*array.BooleanBuilder)
			bd.AppendValues(
				lo.RepeatBy(numRows, func(_ int) bool { return schema.GetDefaultValue().GetBoolData() }),
				nil)
		case schemapb.DataType_Int8:
			bd := builder.(*array.Int8Builder)
			bd.AppendValues(
				lo.RepeatBy(numRows, func(_ int) int8 { return int8(schema.GetDefaultValue().GetIntData()) }),
				nil)
		case schemapb.DataType_Int16:
			bd := builder.(*array.Int16Builder)
			bd.AppendValues(
				lo.RepeatBy(numRows, func(_ int) int16 { return int16(schema.GetDefaultValue().GetIntData()) }),
				nil)
		case schemapb.DataType_Int32:
			bd := builder.(*array.Int32Builder)
			bd.AppendValues(
				lo.RepeatBy(numRows, func(_ int) int32 { return schema.GetDefaultValue().GetIntData() }),
				nil)
		case schemapb.DataType_Int64:
			bd := builder.(*array.Int64Builder)
			bd.AppendValues(
				lo.RepeatBy(numRows, func(_ int) int64 { return schema.GetDefaultValue().GetLongData() }),
				nil)
		case schemapb.DataType_Float:
			bd := builder.(*array.Float32Builder)
			bd.AppendValues(
				lo.RepeatBy(numRows, func(_ int) float32 { return schema.GetDefaultValue().GetFloatData() }),
				nil)
		case schemapb.DataType_Double:
			bd := builder.(*array.Float64Builder)
			bd.AppendValues(
				lo.RepeatBy(numRows, func(_ int) float64 { return schema.GetDefaultValue().GetDoubleData() }),
				nil)

		case schemapb.DataType_Timestamptz:
			bd := builder.(*array.Int64Builder)
			bd.AppendValues(
				lo.RepeatBy(numRows, func(_ int) int64 { return schema.GetDefaultValue().GetTimestamptzData() }),
				nil)

		case schemapb.DataType_VarChar, schemapb.DataType_String:
			bd := builder.(*array.StringBuilder)
			bd.AppendValues(
				lo.RepeatBy(numRows, func(_ int) string { return schema.GetDefaultValue().GetStringData() }),
				nil)
		default:
			return nil, merr.WrapErrServiceInternal(fmt.Sprintf("Unexpected default value type: %s", schema.GetDataType().String()))
		}
	} else {
		builder.AppendNulls(numRows)
	}

	return builder.NewArray(), nil
}

// RecordBuilder is a helper to build arrow record.
// Due to current arrow impl (v12), the write performance is largely dependent on the batch size,
// small batch size will cause write performance degradation. To work around this issue, we accumulate
// records and write them in batches. This requires additional memory copy.
type RecordBuilder struct {
	fields   []*schemapb.FieldSchema
	builders []array.Builder

	nRows int
	size  uint64
}

func (b *RecordBuilder) Append(rec Record, start, end int) error {
	for offset := start; offset < end; offset++ {
		for i, builder := range b.builders {
			f := b.fields[i]
			col := rec.Column(f.FieldID)
			size, err := appendValueAt(builder, col, offset, f.GetDefaultValue())
			if err != nil {
				return fmt.Errorf("failed to append value at offset %d for field %s: %w", offset, f.GetName(), err)
			}
			b.size += size
		}
	}
	b.nRows += (end - start)
	return nil
}

func (b *RecordBuilder) GetRowNum() int {
	return b.nRows
}

func (b *RecordBuilder) GetSize() uint64 {
	return b.size
}

func (b *RecordBuilder) Build() Record {
	arrays := make([]arrow.Array, len(b.builders))
	fields := make([]arrow.Field, len(b.builders))
	field2Col := make(map[FieldID]int, len(b.builders))
	for c, builder := range b.builders {
		arrays[c] = builder.NewArray()
		f := b.fields[c]
		fid := f.FieldID
		fields[c] = arrow.Field{
			Name:     f.GetName(),
			Type:     arrays[c].DataType(),
			Nullable: f.Nullable,
		}
		field2Col[fid] = c
	}

	rec := NewSimpleArrowRecord(array.NewRecord(arrow.NewSchema(fields, nil), arrays, int64(b.nRows)), field2Col)
	b.nRows = 0
	b.size = 0
	return rec
}

func NewRecordBuilder(schema *schemapb.CollectionSchema) *RecordBuilder {
	// assumes 5 sub fields per StructArrayField
	fields := make([]*schemapb.FieldSchema, 0, len(schema.Fields)+len(schema.StructArrayFields)*5)
	fields = append(fields, schema.Fields...)
	for _, sf := range schema.StructArrayFields {
		fields = append(fields, sf.Fields...)
	}

	builders := make([]array.Builder, len(fields))
	for i, field := range fields {
		dim, _ := typeutil.GetDim(field)

		elementType := schemapb.DataType_None
		if field.DataType == schemapb.DataType_ArrayOfVector {
			elementType = field.GetElementType()
		}
		arrowType := serdeMap[field.DataType].arrowType(int(dim), elementType)
		builders[i] = array.NewBuilder(memory.DefaultAllocator, arrowType)
	}

	return &RecordBuilder{
		fields:   fields,
		builders: builders,
	}
}
