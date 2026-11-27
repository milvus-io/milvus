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

// Package deltacodec encodes mutation fragments as typed Arrow batches.
package deltacodec

import (
	"math"
	"slices"
	"strconv"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type Operation uint8

const (
	Delete Operation = 1
	Update Operation = 2
)

// Row carries shared assignments once, irrespective of the number of keys.
type Row struct {
	TimeTick      uint64
	SourceOrdinal uint32
	Operation     Operation
	PrimaryKeys   *schemapb.IDs
	Assignments   []*messagespb.UpdateAssignment
}

type Field struct {
	ID              int64
	Type            schemapb.DataType
	Element         schemapb.DataType
	ElementNullable *bool
}

type Layout struct {
	CollectionID  int64
	VChannel      string
	PartitionID   int64
	SchemaVersion int32
	PKType        schemapb.DataType
	Fields        []Field
}

// ValidateIdentity checks the scope supplied by the materialization task.
func (l Layout) ValidateIdentity() error {
	if l.CollectionID <= 0 {
		return merr.WrapErrDataIntegrityMsg("invalid delta collection ID %d", l.CollectionID)
	}
	if l.VChannel == "" {
		return merr.WrapErrDataIntegrityMsg("empty delta channel")
	}
	if l.PartitionID < common.AllPartitionsID {
		return merr.WrapErrDataIntegrityMsg("invalid delta partition scope %d", l.PartitionID)
	}
	return nil
}

func physical(t schemapb.DataType) (arrow.DataType, protoreflect.Name, error) {
	switch t {
	case schemapb.DataType_Bool:
		return arrow.FixedWidthTypes.Boolean, "bool_data", nil
	case schemapb.DataType_Int8:
		return arrow.PrimitiveTypes.Int8, "int_data", nil
	case schemapb.DataType_Int16:
		return arrow.PrimitiveTypes.Int16, "int_data", nil
	case schemapb.DataType_Int32:
		return arrow.PrimitiveTypes.Int32, "int_data", nil
	case schemapb.DataType_Int64:
		return arrow.PrimitiveTypes.Int64, "long_data", nil
	case schemapb.DataType_Float:
		return arrow.PrimitiveTypes.Float32, "float_data", nil
	case schemapb.DataType_Double:
		return arrow.PrimitiveTypes.Float64, "double_data", nil
	case schemapb.DataType_VarChar:
		return arrow.BinaryTypes.String, "string_data", nil
	case schemapb.DataType_Timestamptz:
		return &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, "timestamptz_data", nil
	default:
		return nil, "", merr.WrapErrDataIntegrityMsg("invalid delta type %s", t)
	}
}

func deltaField(name string, id int64, dataType arrow.DataType, nullable bool) arrow.Field {
	return arrow.Field{Name: name, Type: dataType, Nullable: nullable, Metadata: arrow.MetadataFrom(map[string]string{"PARQUET:field_id": strconv.FormatInt(id, 10)})}
}

func (l Layout) Schema() (*arrow.Schema, error) {
	if l.PKType != schemapb.DataType_Int64 && l.PKType != schemapb.DataType_VarChar {
		return nil, merr.WrapErrDataIntegrityMsg("invalid delta PK type %s", l.PKType)
	}
	pk, _, _ := physical(l.PKType)
	fields := []arrow.Field{
		deltaField("mutation_ts", 0, arrow.PrimitiveTypes.Uint64, false), deltaField("op_type", 1, arrow.PrimitiveTypes.Uint8, false),
		deltaField("primary_keys", 2, arrow.ListOfNonNullable(pk), false), deltaField("source_ordinal", 3, arrow.PrimitiveTypes.Uint32, false),
	}
	seen := map[int64]bool{}
	for _, f := range l.Fields {
		if f.ID < 100 || seen[f.ID] {
			return nil, merr.WrapErrDataIntegrityMsg("invalid or repeated delta field ID %d", f.ID)
		}
		seen[f.ID] = true
		t := f.Type
		if t == schemapb.DataType_Array {
			t = f.Element
		}
		dt, _, err := physical(t)
		if err != nil {
			return nil, err
		}
		if f.Type == schemapb.DataType_Array {
			dt = arrow.ListOfField(arrow.Field{Name: "item", Type: dt, Nullable: f.ElementNullable == nil || *f.ElementNullable})
		}
		fields = append(fields, deltaField("field_"+strconv.FormatInt(f.ID, 10), f.ID, arrow.StructOf(arrow.Field{Name: "value", Type: dt, Nullable: true}), true))
	}
	return arrow.NewSchema(fields, nil), nil
}

func Encode(l Layout, rows []Row) (arrow.Record, error) {
	schema, err := l.Schema()
	if err != nil {
		return nil, err
	}
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	index := map[int64]int{}
	for i, f := range l.Fields {
		index[f.ID] = i
	}
	for _, r := range rows {
		if r.Operation != Delete && r.Operation != Update || (r.Operation == Delete) != (len(r.Assignments) == 0) {
			return nil, merr.WrapErrDataIntegrityMsg("invalid delta operation or assignments at %d", r.TimeTick)
		}
		b.Field(0).(*array.Uint64Builder).Append(r.TimeTick)
		b.Field(1).(*array.Uint8Builder).Append(uint8(r.Operation))
		keys := b.Field(2).(*array.ListBuilder)
		keys.Append(true)
		if l.PKType == schemapb.DataType_Int64 {
			data := r.PrimaryKeys.GetIntId().GetData()
			if len(data) == 0 {
				return nil, merr.WrapErrDataIntegrityMsg("delta row requires int64 keys")
			}
			keys.ValueBuilder().(*array.Int64Builder).AppendValues(data, nil)
		} else {
			data := r.PrimaryKeys.GetStrId().GetData()
			if len(data) == 0 {
				return nil, merr.WrapErrDataIntegrityMsg("delta row requires varchar keys")
			}
			keys.ValueBuilder().(*array.StringBuilder).AppendValues(data, nil)
		}
		b.Field(3).(*array.Uint32Builder).Append(r.SourceOrdinal)
		for _, a := range r.Assignments {
			i, ok := index[a.GetFieldId()]
			if !ok {
				return nil, merr.WrapErrDataIntegrityMsg("unknown delta field %d", a.GetFieldId())
			}
			parent := b.Field(i + 4).(*array.StructBuilder)
			if parent.Len() == keys.Len() {
				return nil, merr.WrapErrDataIntegrityMsg("repeated delta field %d", a.GetFieldId())
			}
			parent.Append(true)
			if err := AppendLiteral(parent.FieldBuilder(0), l.Fields[i], a.GetLiteral()); err != nil {
				return nil, err
			}
		}
		for i := range l.Fields {
			parent := b.Field(i + 4)
			if parent.Len() < keys.Len() {
				parent.AppendNull()
			}
		}
	}
	return b.NewRecord(), nil
}

// AppendLiteral appends a decoded literal to its typed Arrow value builder.
func AppendLiteral(b array.Builder, f Field, literal *schemapb.UpdateLiteral) error {
	if literal.GetValue() == nil {
		return merr.WrapErrDataIntegrityMsg("delta field %d has no literal", f.ID)
	}
	if n, ok := literal.GetValue().(*schemapb.UpdateLiteral_SetNull); ok {
		if !n.SetNull {
			return merr.WrapErrDataIntegrityMsg("delta field %d has set_null=false", f.ID)
		}
		b.AppendNull()
		return nil
	}
	if f.Type == schemapb.DataType_Array {
		msg := literal.GetArrayValue().ProtoReflect()
		_, name, _ := physical(f.Element)
		desc := msg.Descriptor().Fields().ByName(name)
		if desc == nil || !msg.Has(desc) {
			return merr.WrapErrDataIntegrityMsg("delta field %d requires an array of %s", f.ID, f.Element)
		}
		elems := msg.Get(desc).Message()
		values := elems.Get(elems.Descriptor().Fields().ByName("data")).List()
		validity := literal.GetArrayValue().GetValidData()
		if f.ElementNullable != nil && !*f.ElementNullable && slices.Contains(validity, false) {
			return merr.WrapErrDataIntegrityMsg("NULL element in required delta array field %d", f.ID)
		}
		n := values.Len()
		compact := len(validity) > 0 && len(validity) != n
		if compact {
			writes := 0
			for _, v := range validity {
				if v {
					writes++
				}
			}
			if writes != n {
				return merr.WrapErrDataIntegrityMsg("invalid delta array validity for field %d", f.ID)
			}
		}
		if len(validity) > 0 {
			n = len(validity)
		}
		list := b.(*array.ListBuilder)
		list.Append(true)
		source := 0
		for i := 0; i < n; i++ {
			if len(validity) > 0 && !validity[i] {
				if !compact {
					source++
				}
				list.ValueBuilder().AppendNull()
				continue
			}
			value := values.Get(source)
			source++
			if err := appendPrimitive(list.ValueBuilder(), value); err != nil {
				return err
			}
		}
		return nil
	}
	msg := literal.GetScalarValue().ProtoReflect()
	_, name, _ := physical(f.Type)
	desc := msg.Descriptor().Fields().ByName(name)
	if desc == nil || !msg.Has(desc) {
		return merr.WrapErrDataIntegrityMsg("delta field %d requires a scalar of type %s", f.ID, f.Type)
	}
	return appendPrimitive(b, msg.Get(desc))
}

func appendPrimitive(b array.Builder, v protoreflect.Value) error {
	switch b := b.(type) {
	case *array.BooleanBuilder:
		b.Append(v.Bool())
	case *array.Int8Builder:
		if v.Int() < math.MinInt8 || v.Int() > math.MaxInt8 {
			return merr.WrapErrDataIntegrityMsg("int8 delta out of range")
		}
		b.Append(int8(v.Int()))
	case *array.Int16Builder:
		if v.Int() < math.MinInt16 || v.Int() > math.MaxInt16 {
			return merr.WrapErrDataIntegrityMsg("int16 delta out of range")
		}
		b.Append(int16(v.Int()))
	case *array.Int32Builder:
		b.Append(int32(v.Int()))
	case *array.Int64Builder:
		b.Append(v.Int())
	case *array.Float32Builder:
		b.Append(float32(v.Float()))
	case *array.Float64Builder:
		b.Append(v.Float())
	case *array.StringBuilder:
		b.Append(v.String())
	case *array.TimestampBuilder:
		b.Append(arrow.Timestamp(v.Int()))
	default:
		return merr.WrapErrDataIntegrityMsg("unexpected primitive delta builder %T", b)
	}
	return nil
}
