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

package agg

import (
	"context"
	"math"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestGroupedMergeCollisionOwnershipAndLimit(t *testing.T) {
	// Force every key, including NULL, into the same hash bucket. Groups must
	// still compare all keys and keep independent accumulator storage.
	patch := mockey.Mock((*StringFieldAccessor).Hash).To(func(*StringFieldAccessor, int) uint64 { return 1 }).Build()
	defer patch.UnPatch()
	keys, err := genEmptyFieldDataByType(schemapb.DataType_VarChar)
	require.NoError(t, err)
	keys.GetScalars().GetStringData().Data = []string{"a", "b", "", "ignored"}
	typeutil.SetFieldDataValidData(keys, []bool{true, true, true, false})
	values := genEmptyLongFieldData(schemapb.DataType_Int64, []int64{1, 10, 100, 1000})
	originalKeys, originalValues := proto.Clone(keys), proto.Clone(values)
	for _, limit := range []int64{-1, 2} {
		r := makeGroupAggReducer()
		r.groupLimit = limit
		partial, err := r.DecodePartial([]*schemapb.FieldData{keys, values}, 4)
		require.NoError(t, err)
		for attempt := 0; attempt < 2; attempt++ {
			merged, err := r.Merge(t.Context(), []*PartialAggregationResult{partial, partial})
			require.NoError(t, err)
			buffer := r.NewBuffer()
			require.NoError(t, buffer.Merge([]*PartialAggregationResult{partial}))
			require.NoError(t, buffer.Merge([]*PartialAggregationResult{partial}))
			incremental, err := buffer.Result()
			require.NoError(t, err)
			for i, field := range merged.fieldDatas {
				require.True(t, proto.Equal(field, incremental.fieldDatas[i]))
			}
			actual := make(map[any]int64)
			outKeys, outValues := merged.fieldDatas[0], merged.fieldDatas[1]
			for i, value := range outValues.GetScalars().GetLongData().Data {
				var key any
				if typeutil.GetFieldDataValidData(outKeys)[i] {
					key = outKeys.GetScalars().GetStringData().Data[i]
				}
				actual[key] = value
			}
			want := map[any]int64{"a": 2, "b": 20}
			if limit == -1 {
				want[""], want[nil] = 200, 2000
			}
			require.Equal(t, want, actual)
			require.EqualValues(t, 8, merged.allRetrieveCount)
			require.True(t, proto.Equal(originalKeys, keys))
			require.True(t, proto.Equal(originalValues, values))
		}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, err = r.Merge(ctx, []*PartialAggregationResult{partial, partial})
		require.ErrorIs(t, err, context.Canceled)
	}
}

func TestGroupedMergeScalarKeys(t *testing.T) {
	for _, tc := range []struct {
		typ    schemapb.DataType
		values []any
	}{
		{schemapb.DataType_Bool, []any{true, false}},
		{schemapb.DataType_Int32, []any{int32(-1), int32(1)}},
		{schemapb.DataType_Int64, []any{int64(9007199254740992), int64(9007199254740993)}},
		{schemapb.DataType_Timestamptz, []any{int64(-500000), int64(500001)}},
		{schemapb.DataType_Float, []any{float32(1.25), float32(-2.5)}},
		{schemapb.DataType_Double, []any{float64(1.25), float64(-2.5)}},
	} {
		t.Run(tc.typ.String(), func(t *testing.T) {
			schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
				{FieldID: 1, Name: "key", DataType: tc.typ, Nullable: true},
				{FieldID: 2, Name: "count", DataType: schemapb.DataType_Int64},
			}}
			r := NewGroupAggReducer([]int64{1}, []*planpb.Aggregate{{Op: planpb.AggregateOp_count, FieldId: 2}}, -1, schema)
			keys, err := typeutil.GenEmptyFieldData(&schemapb.FieldSchema{DataType: tc.typ, Nullable: true})
			require.NoError(t, err)
			if tc.typ == schemapb.DataType_Timestamptz {
				keys, err = genEmptyFieldDataByType(tc.typ)
				require.NoError(t, err)
			}
			for _, value := range tc.values {
				require.NoError(t, AssembleSingleValue(NewFieldValue(value), keys))
			}
			require.NoError(t, AssembleSingleValue(NewNullFieldValue(), keys))
			partial, err := r.DecodePartial([]*schemapb.FieldData{keys, genEmptyLongFieldData(schemapb.DataType_Int64, []int64{1, 2, 3})}, 6)
			require.NoError(t, err)
			merged, err := r.Merge(t.Context(), []*PartialAggregationResult{partial, partial})
			require.NoError(t, err)
			require.Equal(t, []int64{2, 4, 6}, merged.fieldDatas[1].GetScalars().GetLongData().Data)
			require.Equal(t, []bool{true, true, false}, typeutil.GetFieldDataValidData(merged.fieldDatas[0]))
		})
	}
	// Retain the existing IEEE equality contract: NaNs never equal one another,
	// even when the hash bits are identical and they share a collision chain.
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 1, Name: "key", DataType: schemapb.DataType_Double}, {FieldID: 2, Name: "count", DataType: schemapb.DataType_Int64}}}
	r := NewGroupAggReducer([]int64{1}, []*planpb.Aggregate{{Op: planpb.AggregateOp_count, FieldId: 2}}, -1, schema)
	keys, err := genEmptyFieldDataByType(schemapb.DataType_Double)
	require.NoError(t, err)
	keys.GetScalars().GetDoubleData().Data = []float64{math.NaN(), 1}
	partial, err := r.DecodePartial([]*schemapb.FieldData{keys, genEmptyLongFieldData(schemapb.DataType_Int64, []int64{1, 1})}, 2)
	require.NoError(t, err)
	merged, err := r.Merge(t.Context(), []*PartialAggregationResult{partial, partial})
	require.NoError(t, err)
	require.ElementsMatch(t, []int64{1, 2, 1}, merged.fieldDatas[1].GetScalars().GetLongData().Data)
}

type groupedCancelContext struct {
	context.Context
	calls, after int
	err          error
}

func (c *groupedCancelContext) Err() error {
	c.calls++
	if c.calls > c.after {
		return c.err
	}
	return nil
}

func TestGroupedMergeCancellationDuringWork(t *testing.T) {
	const n = 2048
	keys, values := make([]int64, n), make([]int64, n)
	for i := range keys {
		keys[i], values[i] = int64(i), 1
	}
	r := NewGroupAggReducer([]int64{100}, []*planpb.Aggregate{{Op: planpb.AggregateOp_sum, FieldId: 101}}, -1, buildTestSchema())
	partial, err := r.DecodePartial([]*schemapb.FieldData{
		genEmptyLongFieldData(schemapb.DataType_Int64, keys),
		genEmptyLongFieldData(schemapb.DataType_Int64, values),
	}, n)
	require.NoError(t, err)
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
		ctx := &groupedCancelContext{Context: t.Context(), after: 3, err: cause}
		_, err := r.Merge(ctx, []*PartialAggregationResult{partial, partial})
		require.ErrorIs(t, err, cause)

		buffer := r.NewBuffer()
		require.NoError(t, buffer.Merge([]*PartialAggregationResult{partial}))
		ctx = &groupedCancelContext{Context: t.Context(), after: 2, err: cause}
		_, err = buffer.result(ctx)
		require.ErrorIs(t, err, cause)
	}
}
