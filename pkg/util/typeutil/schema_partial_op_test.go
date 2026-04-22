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

package typeutil

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

// boolRow/intRow/... helpers build a ScalarField carrying a single row's
// typed-array payload. They keep tests concise and match how ArrayArray
// stores per-row data.

func boolRow(vs ...bool) *schemapb.ScalarField {
	return &schemapb.ScalarField{Data: &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: vs}}}
}

func intRow(vs ...int32) *schemapb.ScalarField {
	return &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: vs}}}
}

func longRow(vs ...int64) *schemapb.ScalarField {
	return &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: vs}}}
}

func floatRow(vs ...float32) *schemapb.ScalarField {
	return &schemapb.ScalarField{Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: vs}}}
}

func doubleRow(vs ...float64) *schemapb.ScalarField {
	return &schemapb.ScalarField{Data: &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: vs}}}
}

func stringRow(vs ...string) *schemapb.ScalarField {
	return &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: vs}}}
}

func TestApplyArrayRowOp_Replace(t *testing.T) {
	base := longRow(1, 2, 3)
	update := longRow(9, 8)
	got, err := ApplyArrayRowOp(base, update, schemapb.FieldPartialUpdateOp_REPLACE, schemapb.DataType_Int64, -1)
	require.NoError(t, err)
	assert.Equal(t, []int64{9, 8}, got.GetLongData().GetData())
}

func TestApplyArrayRowOp_UnsupportedOp(t *testing.T) {
	got, err := ApplyArrayRowOp(longRow(1), longRow(2), schemapb.FieldPartialUpdateOp_OpType(999), schemapb.DataType_Int64, -1)
	assert.Error(t, err)
	assert.Nil(t, got)
}

func TestApplyArrayRowOp_AppendBool(t *testing.T) {
	got, err := ApplyArrayRowOp(boolRow(true, false), boolRow(true), schemapb.FieldPartialUpdateOp_ARRAY_APPEND, schemapb.DataType_Bool, -1)
	require.NoError(t, err)
	assert.Equal(t, []bool{true, false, true}, got.GetBoolData().GetData())
}

func TestApplyArrayRowOp_AppendInt32(t *testing.T) {
	for _, et := range []schemapb.DataType{schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32} {
		got, err := ApplyArrayRowOp(intRow(1, 2), intRow(3, 4), schemapb.FieldPartialUpdateOp_ARRAY_APPEND, et, -1)
		require.NoError(t, err, et.String())
		assert.Equal(t, []int32{1, 2, 3, 4}, got.GetIntData().GetData(), et.String())
	}
}

func TestApplyArrayRowOp_AppendInt64(t *testing.T) {
	got, err := ApplyArrayRowOp(longRow(1, 2), longRow(3, 4), schemapb.FieldPartialUpdateOp_ARRAY_APPEND, schemapb.DataType_Int64, -1)
	require.NoError(t, err)
	assert.Equal(t, []int64{1, 2, 3, 4}, got.GetLongData().GetData())
}

func TestApplyArrayRowOp_AppendFloat(t *testing.T) {
	got, err := ApplyArrayRowOp(floatRow(1.5, 2.5), floatRow(3.5), schemapb.FieldPartialUpdateOp_ARRAY_APPEND, schemapb.DataType_Float, -1)
	require.NoError(t, err)
	assert.Equal(t, []float32{1.5, 2.5, 3.5}, got.GetFloatData().GetData())
}

func TestApplyArrayRowOp_AppendDouble(t *testing.T) {
	got, err := ApplyArrayRowOp(doubleRow(1.25), doubleRow(2.5, 3.5), schemapb.FieldPartialUpdateOp_ARRAY_APPEND, schemapb.DataType_Double, -1)
	require.NoError(t, err)
	assert.Equal(t, []float64{1.25, 2.5, 3.5}, got.GetDoubleData().GetData())
}

func TestApplyArrayRowOp_AppendVarChar(t *testing.T) {
	for _, et := range []schemapb.DataType{schemapb.DataType_VarChar, schemapb.DataType_String} {
		got, err := ApplyArrayRowOp(stringRow("a"), stringRow("b", "c"), schemapb.FieldPartialUpdateOp_ARRAY_APPEND, et, -1)
		require.NoError(t, err, et.String())
		assert.Equal(t, []string{"a", "b", "c"}, got.GetStringData().GetData(), et.String())
	}
}

func TestApplyArrayRowOp_AppendEmptyBase(t *testing.T) {
	got, err := ApplyArrayRowOp(longRow(), longRow(1, 2), schemapb.FieldPartialUpdateOp_ARRAY_APPEND, schemapb.DataType_Int64, -1)
	require.NoError(t, err)
	assert.Equal(t, []int64{1, 2}, got.GetLongData().GetData())
}

func TestApplyArrayRowOp_AppendEmptyUpdate(t *testing.T) {
	got, err := ApplyArrayRowOp(longRow(1, 2), longRow(), schemapb.FieldPartialUpdateOp_ARRAY_APPEND, schemapb.DataType_Int64, -1)
	require.NoError(t, err)
	assert.Equal(t, []int64{1, 2}, got.GetLongData().GetData())
}

func TestApplyArrayRowOp_AppendBothEmpty(t *testing.T) {
	got, err := ApplyArrayRowOp(longRow(), longRow(), schemapb.FieldPartialUpdateOp_ARRAY_APPEND, schemapb.DataType_Int64, -1)
	require.NoError(t, err)
	assert.Empty(t, got.GetLongData().GetData())
}

func TestApplyArrayRowOp_AppendOverflowsCapacity(t *testing.T) {
	_, err := ApplyArrayRowOp(longRow(1, 2), longRow(3), schemapb.FieldPartialUpdateOp_ARRAY_APPEND, schemapb.DataType_Int64, 2)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "max_capacity")
}

func TestApplyArrayRowOp_AppendCapacityExactlyMet(t *testing.T) {
	got, err := ApplyArrayRowOp(longRow(1, 2), longRow(3), schemapb.FieldPartialUpdateOp_ARRAY_APPEND, schemapb.DataType_Int64, 3)
	require.NoError(t, err)
	assert.Equal(t, []int64{1, 2, 3}, got.GetLongData().GetData())
}

func TestApplyArrayRowOp_AppendNegativeCapacitySkipsCheck(t *testing.T) {
	got, err := ApplyArrayRowOp(longRow(1, 2, 3, 4, 5), longRow(6), schemapb.FieldPartialUpdateOp_ARRAY_APPEND, schemapb.DataType_Int64, -1)
	require.NoError(t, err)
	assert.Len(t, got.GetLongData().GetData(), 6)
}

func TestApplyArrayRowOp_AppendCapacityBoolInt32FloatDoubleString(t *testing.T) {
	cases := []struct {
		et   schemapb.DataType
		base *schemapb.ScalarField
		upd  *schemapb.ScalarField
	}{
		{schemapb.DataType_Bool, boolRow(true), boolRow(false)},
		{schemapb.DataType_Int32, intRow(1), intRow(2)},
		{schemapb.DataType_Float, floatRow(1), floatRow(2)},
		{schemapb.DataType_Double, doubleRow(1), doubleRow(2)},
		{schemapb.DataType_VarChar, stringRow("a"), stringRow("b")},
	}
	for _, tc := range cases {
		_, err := ApplyArrayRowOp(tc.base, tc.upd, schemapb.FieldPartialUpdateOp_ARRAY_APPEND, tc.et, 1)
		assert.Error(t, err, tc.et.String())
	}
}

func TestApplyArrayRowOp_AppendUnsupportedElementType(t *testing.T) {
	_, err := ApplyArrayRowOp(longRow(), longRow(), schemapb.FieldPartialUpdateOp_ARRAY_APPEND, schemapb.DataType_JSON, -1)
	assert.Error(t, err)
}

func TestApplyArrayRowOp_RemoveBool(t *testing.T) {
	got, err := ApplyArrayRowOp(boolRow(true, false, true, false), boolRow(true), schemapb.FieldPartialUpdateOp_ARRAY_REMOVE, schemapb.DataType_Bool, -1)
	require.NoError(t, err)
	assert.Equal(t, []bool{false, false}, got.GetBoolData().GetData())
}

func TestApplyArrayRowOp_RemoveInt32(t *testing.T) {
	for _, et := range []schemapb.DataType{schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32} {
		got, err := ApplyArrayRowOp(intRow(1, 2, 3, 2, 1), intRow(2), schemapb.FieldPartialUpdateOp_ARRAY_REMOVE, et, -1)
		require.NoError(t, err, et.String())
		assert.Equal(t, []int32{1, 3, 1}, got.GetIntData().GetData(), et.String())
	}
}

func TestApplyArrayRowOp_RemoveInt64(t *testing.T) {
	got, err := ApplyArrayRowOp(longRow(1, 2, 3, 4), longRow(2, 4), schemapb.FieldPartialUpdateOp_ARRAY_REMOVE, schemapb.DataType_Int64, -1)
	require.NoError(t, err)
	assert.Equal(t, []int64{1, 3}, got.GetLongData().GetData())
}

func TestApplyArrayRowOp_RemoveFloat(t *testing.T) {
	got, err := ApplyArrayRowOp(floatRow(1.5, 2.5, 3.5, 2.5), floatRow(2.5), schemapb.FieldPartialUpdateOp_ARRAY_REMOVE, schemapb.DataType_Float, -1)
	require.NoError(t, err)
	assert.Equal(t, []float32{1.5, 3.5}, got.GetFloatData().GetData())
}

func TestApplyArrayRowOp_RemoveDouble(t *testing.T) {
	got, err := ApplyArrayRowOp(doubleRow(1.1, 2.2, 3.3), doubleRow(2.2), schemapb.FieldPartialUpdateOp_ARRAY_REMOVE, schemapb.DataType_Double, -1)
	require.NoError(t, err)
	assert.Equal(t, []float64{1.1, 3.3}, got.GetDoubleData().GetData())
}

func TestApplyArrayRowOp_RemoveVarChar(t *testing.T) {
	for _, et := range []schemapb.DataType{schemapb.DataType_VarChar, schemapb.DataType_String} {
		got, err := ApplyArrayRowOp(stringRow("a", "b", "a", "c"), stringRow("a"), schemapb.FieldPartialUpdateOp_ARRAY_REMOVE, et, -1)
		require.NoError(t, err, et.String())
		assert.Equal(t, []string{"b", "c"}, got.GetStringData().GetData(), et.String())
	}
}

func TestApplyArrayRowOp_RemoveMultipleValues(t *testing.T) {
	got, err := ApplyArrayRowOp(longRow(1, 2, 3, 4, 5), longRow(2, 4), schemapb.FieldPartialUpdateOp_ARRAY_REMOVE, schemapb.DataType_Int64, -1)
	require.NoError(t, err)
	assert.Equal(t, []int64{1, 3, 5}, got.GetLongData().GetData())
}

func TestApplyArrayRowOp_RemoveNoMatch(t *testing.T) {
	got, err := ApplyArrayRowOp(longRow(1, 2, 3), longRow(99), schemapb.FieldPartialUpdateOp_ARRAY_REMOVE, schemapb.DataType_Int64, -1)
	require.NoError(t, err)
	assert.Equal(t, []int64{1, 2, 3}, got.GetLongData().GetData())
}

func TestApplyArrayRowOp_RemoveEmptyBase(t *testing.T) {
	base := longRow()
	got, err := ApplyArrayRowOp(base, longRow(1, 2), schemapb.FieldPartialUpdateOp_ARRAY_REMOVE, schemapb.DataType_Int64, -1)
	require.NoError(t, err)
	assert.Same(t, base, got)
}

func TestApplyArrayRowOp_RemoveEmptyUpdate(t *testing.T) {
	base := longRow(1, 2, 3)
	got, err := ApplyArrayRowOp(base, longRow(), schemapb.FieldPartialUpdateOp_ARRAY_REMOVE, schemapb.DataType_Int64, -1)
	require.NoError(t, err)
	assert.Same(t, base, got)
}

// TestApplyArrayRowOp_RemoveEmptyAcrossTypes exercises the early-exit
// "empty base or update" branches for every supported element type.
func TestApplyArrayRowOp_RemoveEmptyAcrossTypes(t *testing.T) {
	type rowCase struct {
		et         schemapb.DataType
		emptyBase  *schemapb.ScalarField
		emptyUpd   *schemapb.ScalarField
		valuedBase *schemapb.ScalarField
		valuedUpd  *schemapb.ScalarField
	}
	cases := []rowCase{
		{schemapb.DataType_Bool, boolRow(), boolRow(), boolRow(true), boolRow(true)},
		{schemapb.DataType_Int8, intRow(), intRow(), intRow(1), intRow(1)},
		{schemapb.DataType_Int16, intRow(), intRow(), intRow(1), intRow(1)},
		{schemapb.DataType_Int32, intRow(), intRow(), intRow(1), intRow(1)},
		{schemapb.DataType_Float, floatRow(), floatRow(), floatRow(1), floatRow(1)},
		{schemapb.DataType_Double, doubleRow(), doubleRow(), doubleRow(1), doubleRow(1)},
		{schemapb.DataType_VarChar, stringRow(), stringRow(), stringRow("a"), stringRow("a")},
		{schemapb.DataType_String, stringRow(), stringRow(), stringRow("a"), stringRow("a")},
	}
	for _, tc := range cases {
		// empty base
		got, err := ApplyArrayRowOp(tc.emptyBase, tc.valuedUpd, schemapb.FieldPartialUpdateOp_ARRAY_REMOVE, tc.et, -1)
		require.NoError(t, err, tc.et.String())
		assert.Same(t, tc.emptyBase, got, tc.et.String())
		// empty update
		got, err = ApplyArrayRowOp(tc.valuedBase, tc.emptyUpd, schemapb.FieldPartialUpdateOp_ARRAY_REMOVE, tc.et, -1)
		require.NoError(t, err, tc.et.String())
		assert.Same(t, tc.valuedBase, got, tc.et.String())
	}
}

func TestApplyArrayRowOp_RemoveDuplicatesInUpdate(t *testing.T) {
	got, err := ApplyArrayRowOp(longRow(1, 2, 3, 2), longRow(2, 2, 2), schemapb.FieldPartialUpdateOp_ARRAY_REMOVE, schemapb.DataType_Int64, -1)
	require.NoError(t, err)
	assert.Equal(t, []int64{1, 3}, got.GetLongData().GetData())
}

func TestApplyArrayRowOp_RemoveFloatNaN(t *testing.T) {
	nan32 := float32(math.NaN())
	got, err := ApplyArrayRowOp(floatRow(1.0, nan32, 2.0), floatRow(nan32), schemapb.FieldPartialUpdateOp_ARRAY_REMOVE, schemapb.DataType_Float, -1)
	require.NoError(t, err)
	// NaN != NaN → base NaN retained
	out := got.GetFloatData().GetData()
	require.Len(t, out, 3)
	assert.Equal(t, float32(1.0), out[0])
	assert.True(t, math.IsNaN(float64(out[1])))
	assert.Equal(t, float32(2.0), out[2])
}

func TestApplyArrayRowOp_RemoveDoubleNaN(t *testing.T) {
	got, err := ApplyArrayRowOp(doubleRow(math.NaN(), 1.0), doubleRow(math.NaN()), schemapb.FieldPartialUpdateOp_ARRAY_REMOVE, schemapb.DataType_Double, -1)
	require.NoError(t, err)
	out := got.GetDoubleData().GetData()
	require.Len(t, out, 2)
	assert.True(t, math.IsNaN(out[0]))
	assert.Equal(t, 1.0, out[1])
}

func TestApplyArrayRowOp_RemoveUnsupportedElementType(t *testing.T) {
	_, err := ApplyArrayRowOp(longRow(), longRow(), schemapb.FieldPartialUpdateOp_ARRAY_REMOVE, schemapb.DataType_JSON, -1)
	assert.Error(t, err)
}

func TestContainsFloat32(t *testing.T) {
	assert.True(t, containsFloat32([]float32{1, 2, 3}, 2))
	assert.False(t, containsFloat32([]float32{1, 2, 3}, 9))
	assert.False(t, containsFloat32(nil, 1))
	// NaN behavior
	nan := float32(math.NaN())
	assert.False(t, containsFloat32([]float32{nan}, nan))
}

func TestContainsFloat64(t *testing.T) {
	assert.True(t, containsFloat64([]float64{1, 2, 3}, 2))
	assert.False(t, containsFloat64([]float64{1, 2, 3}, 9))
	assert.False(t, containsFloat64(nil, 1))
	assert.False(t, containsFloat64([]float64{math.NaN()}, math.NaN()))
}

func arrayField(rows []*schemapb.ScalarField, et schemapb.DataType) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type: schemapb.DataType_Array,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
				Data:        rows,
				ElementType: et,
			}},
		}},
	}
}

func TestUpdateArrayFieldByColumnWithOp_Append(t *testing.T) {
	base := arrayField([]*schemapb.ScalarField{longRow(1), longRow(10, 20)}, schemapb.DataType_Int64)
	update := arrayField([]*schemapb.ScalarField{longRow(2, 3), longRow(30)}, schemapb.DataType_Int64)

	err := UpdateArrayFieldByColumnWithOp(base, update, []int64{0, 1}, []int64{0, 1},
		schemapb.FieldPartialUpdateOp_ARRAY_APPEND, -1)
	require.NoError(t, err)

	rows := base.GetScalars().GetArrayData().GetData()
	assert.Equal(t, []int64{1, 2, 3}, rows[0].GetLongData().GetData())
	assert.Equal(t, []int64{10, 20, 30}, rows[1].GetLongData().GetData())
}

func TestUpdateArrayFieldByColumnWithOp_Remove(t *testing.T) {
	base := arrayField([]*schemapb.ScalarField{longRow(1, 2, 3, 2), longRow(10)}, schemapb.DataType_Int64)
	update := arrayField([]*schemapb.ScalarField{longRow(2), longRow(99)}, schemapb.DataType_Int64)

	err := UpdateArrayFieldByColumnWithOp(base, update, []int64{0, 1}, []int64{0, 1},
		schemapb.FieldPartialUpdateOp_ARRAY_REMOVE, -1)
	require.NoError(t, err)

	rows := base.GetScalars().GetArrayData().GetData()
	assert.Equal(t, []int64{1, 3}, rows[0].GetLongData().GetData())
	assert.Equal(t, []int64{10}, rows[1].GetLongData().GetData())
}

func TestUpdateArrayFieldByColumnWithOp_ReplaceFallback(t *testing.T) {
	base := arrayField([]*schemapb.ScalarField{longRow(1, 2)}, schemapb.DataType_Int64)
	update := arrayField([]*schemapb.ScalarField{longRow(9)}, schemapb.DataType_Int64)

	err := UpdateArrayFieldByColumnWithOp(base, update, []int64{0}, []int64{0},
		schemapb.FieldPartialUpdateOp_REPLACE, -1)
	require.NoError(t, err)
	assert.Equal(t, []int64{9}, base.GetScalars().GetArrayData().GetData()[0].GetLongData().GetData())
}

func TestUpdateArrayFieldByColumnWithOp_RejectsNonArray(t *testing.T) {
	base := &schemapb.FieldData{
		Type: schemapb.DataType_Int64,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1}}},
		}},
	}
	update := arrayField([]*schemapb.ScalarField{longRow(2)}, schemapb.DataType_Int64)

	err := UpdateArrayFieldByColumnWithOp(base, update, []int64{0}, []int64{0},
		schemapb.FieldPartialUpdateOp_ARRAY_APPEND, -1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Array field")
}

func TestUpdateArrayFieldByColumnWithOp_RejectsIndexLenMismatch(t *testing.T) {
	base := arrayField([]*schemapb.ScalarField{longRow()}, schemapb.DataType_Int64)
	update := arrayField([]*schemapb.ScalarField{longRow()}, schemapb.DataType_Int64)
	err := UpdateArrayFieldByColumnWithOp(base, update, []int64{0}, []int64{0, 1},
		schemapb.FieldPartialUpdateOp_ARRAY_APPEND, -1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "length mismatch")
}

func TestUpdateArrayFieldByColumnWithOp_NilInputsReturnNil(t *testing.T) {
	assert.NoError(t, UpdateArrayFieldByColumnWithOp(nil, nil, nil, nil,
		schemapb.FieldPartialUpdateOp_ARRAY_APPEND, -1))
	base := arrayField([]*schemapb.ScalarField{longRow()}, schemapb.DataType_Int64)
	assert.NoError(t, UpdateArrayFieldByColumnWithOp(base, nil, nil, nil,
		schemapb.FieldPartialUpdateOp_ARRAY_APPEND, -1))
}

func TestUpdateArrayFieldByColumnWithOp_PropagatesMergeError(t *testing.T) {
	// Capacity exceeded on APPEND triggers ApplyArrayRowOp's error path.
	base := arrayField([]*schemapb.ScalarField{longRow(1, 2)}, schemapb.DataType_Int64)
	update := arrayField([]*schemapb.ScalarField{longRow(3)}, schemapb.DataType_Int64)
	err := UpdateArrayFieldByColumnWithOp(base, update, []int64{0}, []int64{0},
		schemapb.FieldPartialUpdateOp_ARRAY_APPEND, 2)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "max_capacity")
}

func TestNewArrayCapacityError(t *testing.T) {
	err := newArrayCapacityError(10, 5)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "10")
	assert.Contains(t, err.Error(), "5")
}
