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

package deltacodec

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
)

func TestSparseTypedMutation(t *testing.T) {
	keys := &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"one", "two"}}}}
	layout := Layout{PKType: schemapb.DataType_VarChar, Fields: []Field{{ID: 105, Type: schemapb.DataType_Int8}, {ID: 106, Type: schemapb.DataType_Array, Element: schemapb.DataType_VarChar}}}
	small := &messagespb.UpdateAssignment{FieldId: 105, Literal: &schemapb.UpdateLiteral{Value: &schemapb.UpdateLiteral_ScalarValue{ScalarValue: &schemapb.ValueField{Data: &schemapb.ValueField_IntData{IntData: -4}}}}}
	null := &messagespb.UpdateAssignment{FieldId: 105, Literal: &schemapb.UpdateLiteral{Value: &schemapb.UpdateLiteral_NullValue{}}}
	arr := &messagespb.UpdateAssignment{FieldId: 106, Literal: &schemapb.UpdateLiteral{Value: &schemapb.UpdateLiteral_ArrayValue{ArrayValue: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"", "snow"}}}}}}}
	rows := []Row{{TimeTick: 100, Operation: Update, PrimaryKeys: keys, Assignments: []*messagespb.UpdateAssignment{small}}, {TimeTick: 110, Operation: Update, SourceOrdinal: 2, PrimaryKeys: keys, Assignments: []*messagespb.UpdateAssignment{arr}}, {TimeTick: 110, Operation: Update, SourceOrdinal: 3, PrimaryKeys: keys, Assignments: []*messagespb.UpdateAssignment{null}}, {TimeTick: 120, Operation: Delete, PrimaryKeys: keys}}
	rec, err := Encode(layout, rows)
	require.NoError(t, err)
	got, err := Decode(layout, rec)
	require.NoError(t, err)
	rec.Release()
	for i, r := range rows {
		require.Equal(t, r.Operation, got[i].Operation)
		require.Equal(t, r.SourceOrdinal, got[i].SourceOrdinal)
		require.True(t, proto.Equal(r.PrimaryKeys, got[i].PrimaryKeys))
		require.Len(t, got[i].Assignments, len(r.Assignments))
		for j, a := range r.Assignments {
			require.True(t, proto.Equal(a, got[i].Assignments[j]))
		}
	}
	require.Len(t, layout.Project([]int64{}).Fields, 0)
	bad := rows[0]
	bad.Assignments = []*messagespb.UpdateAssignment{small, small}
	_, err = Encode(layout, []Row{bad})
	require.ErrorContains(t, err, "repeated delta field")
	small.Literal.GetScalarValue().Data = &schemapb.ValueField_IntData{IntData: 512}
	_, err = Encode(layout, rows)
	require.ErrorContains(t, err, "out of range")
}

func TestMutationAssignmentLogicalTypes(t *testing.T) {
	cases := []struct {
		typ   schemapb.DataType
		field protoreflect.Name
		value protoreflect.Value
	}{
		{schemapb.DataType_Bool, "bool_data", protoreflect.ValueOfBool(false)},
		{schemapb.DataType_Int8, "int_data", protoreflect.ValueOfInt32(-128)},
		{schemapb.DataType_Int16, "int_data", protoreflect.ValueOfInt32(32767)},
		{schemapb.DataType_Int32, "int_data", protoreflect.ValueOfInt32(-2147483648)},
		{schemapb.DataType_Int64, "long_data", protoreflect.ValueOfInt64(-9007199254740999)},
		{schemapb.DataType_VarChar, "string_data", protoreflect.ValueOfString("雪")},
		{schemapb.DataType_Timestamptz, "timestamptz_data", protoreflect.ValueOfInt64(1732161708901777)},
		{schemapb.DataType_Float, "float_data", protoreflect.ValueOfFloat32(0.5)},
		{schemapb.DataType_Double, "double_data", protoreflect.ValueOfFloat64(0.25)},
	}
	keys := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}}
	for _, c := range cases {
		t.Run(c.typ.String(), func(t *testing.T) {
			v := &schemapb.ValueField{}
			msg := v.ProtoReflect()
			msg.Set(msg.Descriptor().Fields().ByName(c.field), c.value)
			check := func(f Field, literal *schemapb.UpdateLiteral) *schemapb.UpdateLiteral {
				layout := Layout{PKType: schemapb.DataType_Int64, Fields: []Field{f}}
				r := Row{TimeTick: 21, PrimaryKeys: keys, Operation: Update, Assignments: []*messagespb.UpdateAssignment{{FieldId: f.ID, Literal: literal}}}
				record, err := Encode(layout, []Row{r})
				require.NoError(t, err)
				got, err := Decode(layout, record)
				require.NoError(t, err)
				record.Release()
				return got[0].Assignments[0].Literal
			}
			if c.typ != schemapb.DataType_Float && c.typ != schemapb.DataType_Double {
				literal := &schemapb.UpdateLiteral{Value: &schemapb.UpdateLiteral_ScalarValue{ScalarValue: v}}
				require.True(t, proto.Equal(literal, check(Field{ID: 105, Type: c.typ}, literal)))
			}
			if c.typ == schemapb.DataType_Timestamptz {
				return
			}
			arr := &schemapb.ScalarField{}
			msg = arr.ProtoReflect()
			values := msg.Mutable(msg.Descriptor().Fields().ByName(c.field)).Message()
			data := values.Mutable(values.Descriptor().Fields().ByName("data")).List()
			literal := &schemapb.UpdateLiteral{Value: &schemapb.UpdateLiteral_ArrayValue{ArrayValue: arr}}
			field := Field{ID: 106, Type: schemapb.DataType_Array, Element: c.typ}
			require.True(t, proto.Equal(literal, check(field, literal)), "empty list is a SET value")
			data.Append(c.value)
			field.ElementNullable = proto.Bool(false)
			require.True(t, proto.Equal(literal, check(field, literal)))
			field.ElementNullable = proto.Bool(true)
			arr.ValidData = []bool{true}
			require.True(t, proto.Equal(literal, check(field, literal)))
			arr.ValidData = []bool{false, true}
			decoded := check(field, literal)
			require.Equal(t, []bool{false, true}, decoded.GetArrayValue().ValidData)
			vals := decoded.GetArrayValue().ProtoReflect().Get(msg.Descriptor().Fields().ByName(c.field)).Message()
			require.Equal(t, c.value.Interface(), vals.Get(vals.Descriptor().Fields().ByName("data")).List().Get(1).Interface())
		})
	}
}

func TestDeltaReplayIdentityAcrossFragmentCuts(t *testing.T) {
	layout := Layout{PKType: schemapb.DataType_Int64, Fields: []Field{{ID: 108, Type: schemapb.DataType_Int64}}}
	fragment := func(index uint32, keys ...int64) Row {
		val := &schemapb.UpdateLiteral{Value: &schemapb.UpdateLiteral_ScalarValue{ScalarValue: &schemapb.ValueField{Data: &schemapb.ValueField_LongData{LongData: int64(index)}}}}
		return Row{TimeTick: 21, SourceOrdinal: index, Operation: Update, PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: keys}}}, Assignments: []*messagespb.UpdateAssignment{{FieldId: 108, Literal: val}}}
	}
	original := []Row{fragment(1, 1, 2), fragment(3, 1, 2)}
	retry := []Row{fragment(3, 1), fragment(3, 2), fragment(1, 1), fragment(1, 2)}
	type identity struct {
		tt    uint64
		index uint32
		pk    int64
	}
	seen := map[identity]int{}
	for _, rs := range [][]Row{original, retry} {
		r, err := Encode(layout, rs)
		require.NoError(t, err)
		owned, err := Decode(layout, r)
		r.Release()
		require.NoError(t, err)
		for _, row := range owned {
			for _, pk := range row.PrimaryKeys.GetIntId().Data {
				seen[identity{row.TimeTick, row.SourceOrdinal, pk}]++
			}
		}
	}
	require.Len(t, seen, 4)
	for _, n := range seen {
		require.Equal(t, 2, n)
	}
}
