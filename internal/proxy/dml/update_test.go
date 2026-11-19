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

package dml

import (
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func updateScalar(v *schemapb.ValueField) *schemapb.UpdateLiteral {
	return &schemapb.UpdateLiteral{Value: &schemapb.UpdateLiteral_ScalarValue{ScalarValue: v}}
}

func updateNull() *schemapb.UpdateLiteral {
	return &schemapb.UpdateLiteral{Value: &schemapb.UpdateLiteral_NullValue{NullValue: structpb.NullValue_NULL_VALUE}}
}

func updateTestSchema(field *schemapb.FieldSchema) *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{Name: "people", Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}, field,
	}}
}

func TestPKUpdateLiteralValidation(t *testing.T) {
	paramtable.Init()
	for _, tc := range []struct {
		typ   schemapb.DataType
		value *schemapb.ValueField
	}{
		{schemapb.DataType_Bool, &schemapb.ValueField{Data: &schemapb.ValueField_BoolData{BoolData: false}}},
		{schemapb.DataType_Int8, &schemapb.ValueField{Data: &schemapb.ValueField_IntData{IntData: -128}}},
		{schemapb.DataType_Int16, &schemapb.ValueField{Data: &schemapb.ValueField_IntData{IntData: 32767}}},
		{schemapb.DataType_Int32, &schemapb.ValueField{Data: &schemapb.ValueField_IntData{IntData: 0}}},
		{schemapb.DataType_Int64, &schemapb.ValueField{Data: &schemapb.ValueField_LongData{LongData: 0}}},
		{schemapb.DataType_Timestamptz, &schemapb.ValueField{Data: &schemapb.ValueField_TimestamptzData{TimestamptzData: 123456789}}},
		{schemapb.DataType_VarChar, &schemapb.ValueField{Data: &schemapb.ValueField_StringData{StringData: ""}}},
	} {
		t.Run(tc.typ.String(), func(t *testing.T) {
			field := &schemapb.FieldSchema{FieldID: 101, Name: "value", DataType: tc.typ, Nullable: true, TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "32"}}}
			schema := updateTestSchema(field)
			for _, literal := range []*schemapb.UpdateLiteral{updateScalar(tc.value), updateNull()} {
				original := proto.Clone(literal)
				got, err := validateUpdateAssignments(schema, []*milvuspb.UpdateAssignment{{FieldName: "value", Literal: literal}})
				require.NoError(t, err)
				require.EqualValues(t, 101, got[0].GetFieldId())
				require.True(t, proto.Equal(literal, original), "validation must not change the request")
				require.True(t, proto.Equal(literal, got[0].GetLiteral()))
			}
		})
	}
}

func TestPKUpdateArrayValidation(t *testing.T) {
	paramtable.Init()
	for _, tc := range []struct {
		typ  schemapb.DataType
		data *schemapb.ScalarField
	}{
		{schemapb.DataType_Bool, &schemapb.ScalarField{Data: &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: []bool{false, true}}}}},
		{schemapb.DataType_Int8, &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{-128, 127}}}}},
		{schemapb.DataType_Int16, &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{-32768, 32767}}}}},
		{schemapb.DataType_Int32, &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{0, 1}}}}},
		{schemapb.DataType_Int64, &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1, 2}}}}},
		{schemapb.DataType_Float, &schemapb.ScalarField{Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: []float32{1.5}}}}},
		{schemapb.DataType_Double, &schemapb.ScalarField{Data: &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: []float64{2.5}}}}},
		{schemapb.DataType_VarChar, &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{}}}},
	} {
		t.Run(tc.typ.String(), func(t *testing.T) {
			field := &schemapb.FieldSchema{FieldID: 101, Name: "value", DataType: schemapb.DataType_Array, ElementType: tc.typ, Nullable: true, TypeParams: []*commonpb.KeyValuePair{{Key: "max_capacity", Value: "8"}, {Key: "max_length", Value: "32"}}}
			literal := &schemapb.UpdateLiteral{Value: &schemapb.UpdateLiteral_ArrayValue{ArrayValue: tc.data}}
			got, err := validateUpdateAssignments(updateTestSchema(field), []*milvuspb.UpdateAssignment{{FieldName: "value", Literal: literal}})
			require.NoError(t, err)
			require.True(t, proto.Equal(literal, got[0].GetLiteral()))
		})
	}
}

func TestPKUpdateRejectsInvalidAssignments(t *testing.T) {
	paramtable.Init()
	valid := updateScalar(&schemapb.ValueField{Data: &schemapb.ValueField_LongData{LongData: 18}})
	for _, name := range []string{"primary", "partition", "clustering", "function_output", "function_input", "ttl", "unsupported", "unknown", "duplicate", "absent", "type_mismatch", "null", "overflow", "length", "nested_array"} {
		t.Run(name, func(t *testing.T) {
			field := &schemapb.FieldSchema{FieldID: 101, Name: "value", DataType: schemapb.DataType_Int64}
			schema := updateTestSchema(field)
			assignments := []*milvuspb.UpdateAssignment{{FieldName: "value", Literal: valid}}
			switch name {
			case "primary":
				field.IsPrimaryKey = true
			case "partition":
				field.IsPartitionKey = true
			case "clustering":
				field.IsClusteringKey = true
			case "function_output":
				field.IsFunctionOutput = true
			case "function_input":
				schema.Functions = []*schemapb.FunctionSchema{{InputFieldNames: []string{"value"}}}
			case "ttl":
				schema.Properties = []*commonpb.KeyValuePair{{Key: common.CollectionTTLFieldKey, Value: "value"}}
			case "unsupported":
				field.DataType = schemapb.DataType_Double
			case "unknown":
				assignments[0].FieldName = "missing"
			case "duplicate":
				assignments = append(assignments, assignments[0])
			case "absent":
				assignments[0].Literal = &schemapb.UpdateLiteral{}
			case "type_mismatch":
				field.DataType = schemapb.DataType_Bool
			case "null":
				assignments[0].Literal = updateNull()
			case "overflow":
				field.DataType = schemapb.DataType_Int8
				assignments[0].Literal = updateScalar(&schemapb.ValueField{Data: &schemapb.ValueField_IntData{IntData: 128}})
			case "length":
				field.DataType = schemapb.DataType_VarChar
				field.TypeParams = []*commonpb.KeyValuePair{{Key: "max_length", Value: "1"}}
				assignments[0].Literal = updateScalar(&schemapb.ValueField{Data: &schemapb.ValueField_StringData{StringData: "too long"}})
			case "nested_array":
				field.DataType = schemapb.DataType_Array
				field.ElementType = schemapb.DataType_Array
			}
			_, err := validateUpdateAssignments(schema, assignments)
			require.Error(t, err)
		})
	}
}

func TestPKUpdateMessageSizeAndRouting(t *testing.T) {
	ids := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}}}}
	assignments := []*messagespb.UpdateAssignment{{FieldId: 101, Literal: updateNull()}, {FieldId: 102, Literal: updateScalar(&schemapb.ValueField{Data: &schemapb.ValueField_StringData{StringData: strings.Repeat("x", 32)}})}}
	channels := []string{"p0_10v0", "p1_10v1"}
	one, err := buildUpdateMessages(10, 7, -1, channels[:1], sliceUpdatePKs(ids, 0, 1), assignments, 4096, nil)
	require.NoError(t, err)
	limit := proto.Size(one[0].IntoMessageProto())
	msgs, err := buildUpdateMessages(10, 7, -1, channels, ids, assignments, limit, nil)
	require.NoError(t, err)
	require.Greater(t, len(msgs), 2)
	seen := map[int64]bool{}
	hashes, err := typeutil.HashPK2Channels(ids, channels)
	require.NoError(t, err)
	for _, msg := range msgs {
		require.LessOrEqual(t, proto.Size(msg.IntoMessageProto()), limit)
		u := message.MustAsMutableUpdateMessageV2(msg)
		require.EqualValues(t, 7, u.Header().GetSchemaVersion())
		b := u.MustBody()
		require.Len(t, b.Assignments, 2)
		require.True(t, proto.Equal(b.Assignments[0], assignments[0]))
		for _, pk := range b.GetPrimaryKeys().GetIntId().GetData() {
			require.False(t, seen[pk])
			seen[pk] = true
			require.Equal(t, channels[hashes[pk-1]], msg.VChannel())
		}
	}
	require.Len(t, seen, 10)
	msgs, err = buildUpdateMessages(10, 7, -1, channels, ids, assignments, limit-1, nil)
	require.ErrorIs(t, err, merr.ErrParameterTooLarge)
	require.Nil(t, msgs, "an indivisible operation must fail before any append")
	stringsIDs := &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"a", strings.Repeat("b", 4096)}}}}
	msgs, err = buildUpdateMessages(10, 7, -1, channels, stringsIDs, assignments, 256, nil)
	require.ErrorIs(t, err, merr.ErrParameterTooLarge)
	require.Nil(t, msgs, "a late oversized PK must discard the entire prepared request")
}

func TestPKUpdatePrimaryKeyValidation(t *testing.T) {
	schema := updateTestSchema(&schemapb.FieldSchema{FieldID: 101, Name: "age", DataType: schemapb.DataType_Int64})
	require.Error(t, validateUpdatePKs(nil, schema))
	require.Error(t, validateUpdatePKs(&schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"x"}}}}, schema))
	require.NoError(t, validateUpdatePKs(&schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{0, -1, 100}}}}, schema))
}

func TestPKUpdateInvalidArrayValues(t *testing.T) {
	paramtable.Init()
	for _, tc := range []struct {
		name string
		typ  schemapb.DataType
		data *schemapb.ScalarField
	}{
		{"nan", schemapb.DataType_Float, &schemapb.ScalarField{Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: []float32{float32(math.NaN())}}}}},
		{"inf", schemapb.DataType_Double, &schemapb.ScalarField{Data: &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: []float64{math.Inf(1)}}}}},
		{"utf8", schemapb.DataType_VarChar, &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{string([]byte{0xff})}}}}},
		{"type", schemapb.DataType_Int64, &schemapb.ScalarField{Data: &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: []bool{true}}}}},
		{"overflow", schemapb.DataType_Int8, &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{128}}}}},
		{"capacity", schemapb.DataType_Int64, &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1, 2, 3}}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			field := &schemapb.FieldSchema{FieldID: 101, Name: "value", DataType: schemapb.DataType_Array, ElementType: tc.typ, TypeParams: []*commonpb.KeyValuePair{{Key: "max_capacity", Value: "2"}, {Key: "max_length", Value: "32"}}}
			_, err := validateUpdateAssignments(updateTestSchema(field), []*milvuspb.UpdateAssignment{{FieldName: "value", Literal: &schemapb.UpdateLiteral{Value: &schemapb.UpdateLiteral_ArrayValue{ArrayValue: tc.data}}}})
			require.Error(t, err)
		})
	}
}
