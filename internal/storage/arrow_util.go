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

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
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
	default:
		return 0, fmt.Errorf("unsupported builder type: %T", builder)
	}
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
	builders := make([]array.Builder, len(schema.Fields))
	for i, field := range schema.Fields {
		dim, _ := typeutil.GetDim(field)
		builders[i] = array.NewBuilder(memory.DefaultAllocator, serdeMap[field.DataType].arrowType(int(dim)))
	}
	return &RecordBuilder{
		fields:   schema.Fields,
		builders: builders,
	}
}
