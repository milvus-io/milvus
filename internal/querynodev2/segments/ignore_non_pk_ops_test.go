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

package segments

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/segcorepb"
)

func makeSegcoreIntIDs(ids []int64) *schemapb.IDs {
	return &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: ids}}}
}

func makeSegcoreStrIDs(ids []string) *schemapb.IDs {
	return &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: ids}}}
}

func makeSegcoreTsField(ts []int64) *schemapb.FieldData {
	return &schemapb.FieldData{
		FieldId: common.TimeStampField,
		Type:    schemapb.DataType_Int64,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: ts}},
			},
		},
	}
}

func TestMergeByPKWithOffsetsOperator(t *testing.T) {
	ctx := context.Background()

	t.Run("basic merge", func(t *testing.T) {
		op := NewMergeByPKWithOffsetsOperator(10, reduce.IReduceNoOrder)
		res1 := &segcorepb.RetrieveResults{Ids: makeSegcoreIntIDs([]int64{1, 3, 5}), Offset: []int64{10, 30, 50}, FieldsData: []*schemapb.FieldData{makeSegcoreTsField([]int64{1, 1, 1})}}
		res2 := &segcorepb.RetrieveResults{Ids: makeSegcoreIntIDs([]int64{2, 4, 6}), Offset: []int64{20, 40, 60}, FieldsData: []*schemapb.FieldData{makeSegcoreTsField([]int64{1, 1, 1})}}

		outs, err := op.Run(ctx, nil, []*segcorepb.RetrieveResults{res1, res2})
		require.NoError(t, err)
		merged := outs[0].(*MergedResultWithOffsets)
		assert.Equal(t, []int64{1, 2, 3, 4, 5, 6}, merged.IDs.GetIntId().GetData())
		assert.Len(t, merged.Selections, 6)
	})

	t.Run("topk truncation", func(t *testing.T) {
		op := NewMergeByPKWithOffsetsOperator(3, reduce.IReduceNoOrder)
		res1 := &segcorepb.RetrieveResults{Ids: makeSegcoreIntIDs([]int64{1, 3, 5}), Offset: []int64{10, 30, 50}, FieldsData: []*schemapb.FieldData{makeSegcoreTsField([]int64{1, 1, 1})}}
		res2 := &segcorepb.RetrieveResults{Ids: makeSegcoreIntIDs([]int64{2, 4, 6}), Offset: []int64{20, 40, 60}, FieldsData: []*schemapb.FieldData{makeSegcoreTsField([]int64{1, 1, 1})}}

		outs, err := op.Run(ctx, nil, []*segcorepb.RetrieveResults{res1, res2})
		require.NoError(t, err)
		merged := outs[0].(*MergedResultWithOffsets)
		assert.Equal(t, []int64{1, 2, 3}, merged.IDs.GetIntId().GetData())
	})

	t.Run("pk dedup", func(t *testing.T) {
		op := NewMergeByPKWithOffsetsOperator(10, reduce.IReduceNoOrder)
		res1 := &segcorepb.RetrieveResults{Ids: makeSegcoreIntIDs([]int64{1, 2, 3}), Offset: []int64{11, 12, 13}, FieldsData: []*schemapb.FieldData{makeSegcoreTsField([]int64{1, 1, 1})}}
		res2 := &segcorepb.RetrieveResults{Ids: makeSegcoreIntIDs([]int64{2, 3, 4}), Offset: []int64{22, 23, 24}, FieldsData: []*schemapb.FieldData{makeSegcoreTsField([]int64{1, 1, 1})}}

		outs, err := op.Run(ctx, nil, []*segcorepb.RetrieveResults{res1, res2})
		require.NoError(t, err)
		merged := outs[0].(*MergedResultWithOffsets)
		assert.Equal(t, []int64{1, 2, 3, 4}, merged.IDs.GetIntId().GetData())
		assert.Len(t, merged.Selections, 4)
	})

	t.Run("timestamp dedup keeps latest", func(t *testing.T) {
		op := NewMergeByPKWithOffsetsOperator(10, reduce.IReduceNoOrder)
		res1 := &segcorepb.RetrieveResults{Ids: makeSegcoreIntIDs([]int64{1, 2}), Offset: []int64{100, 200}, FieldsData: []*schemapb.FieldData{makeSegcoreTsField([]int64{100, 100})}}
		res2 := &segcorepb.RetrieveResults{Ids: makeSegcoreIntIDs([]int64{1, 3}), Offset: []int64{101, 300}, FieldsData: []*schemapb.FieldData{makeSegcoreTsField([]int64{200, 100})}}

		outs, err := op.Run(ctx, nil, []*segcorepb.RetrieveResults{res1, res2})
		require.NoError(t, err)
		merged := outs[0].(*MergedResultWithOffsets)
		assert.Equal(t, []int64{1, 2, 3}, merged.IDs.GetIntId().GetData())
		assert.Equal(t, int64(101), merged.Selections[0].Offset)
	})

	t.Run("single result", func(t *testing.T) {
		op := NewMergeByPKWithOffsetsOperator(10, reduce.IReduceNoOrder)
		res := &segcorepb.RetrieveResults{Ids: makeSegcoreIntIDs([]int64{1, 2}), Offset: []int64{10, 20}, FieldsData: []*schemapb.FieldData{makeSegcoreTsField([]int64{1, 1})}}
		outs, err := op.Run(ctx, nil, []*segcorepb.RetrieveResults{res})
		require.NoError(t, err)
		merged := outs[0].(*MergedResultWithOffsets)
		assert.Equal(t, []int64{1, 2}, merged.IDs.GetIntId().GetData())
	})

	t.Run("empty input", func(t *testing.T) {
		op := NewMergeByPKWithOffsetsOperator(10, reduce.IReduceNoOrder)
		outs, err := op.Run(ctx, nil, []*segcorepb.RetrieveResults{})
		require.NoError(t, err)
		merged := outs[0].(*MergedResultWithOffsets)
		assert.Nil(t, merged.IDs.GetIdField())
		assert.Empty(t, merged.Selections)
	})

	t.Run("string pk", func(t *testing.T) {
		op := NewMergeByPKWithOffsetsOperator(10, reduce.IReduceNoOrder)
		res1 := &segcorepb.RetrieveResults{Ids: makeSegcoreStrIDs([]string{"a", "c", "e"}), Offset: []int64{1, 3, 5}, FieldsData: []*schemapb.FieldData{makeSegcoreTsField([]int64{1, 1, 1})}}
		res2 := &segcorepb.RetrieveResults{Ids: makeSegcoreStrIDs([]string{"b", "d", "f"}), Offset: []int64{2, 4, 6}, FieldsData: []*schemapb.FieldData{makeSegcoreTsField([]int64{1, 1, 1})}}
		outs, err := op.Run(ctx, nil, []*segcorepb.RetrieveResults{res1, res2})
		require.NoError(t, err)
		merged := outs[0].(*MergedResultWithOffsets)
		assert.Equal(t, []string{"a", "b", "c", "d", "e", "f"}, merged.IDs.GetStrId().GetData())
	})
}

func TestFetchFieldsDataOperator_EmptySelections(t *testing.T) {
	op := NewFetchFieldsDataOperator(nil, nil, nil)
	outs, err := op.Run(context.Background(), nil, &MergedResultWithOffsets{IDs: makeSegcoreIntIDs([]int64{1, 2}), Selections: nil})
	require.NoError(t, err)
	out := outs[0].(*segcorepb.RetrieveResults)
	assert.Equal(t, []int64{1, 2}, out.GetIds().GetIntId().GetData())
	assert.Empty(t, out.GetFieldsData())
}

func TestFetchFieldsDataOperator_SingleSegment(t *testing.T) {
	seg := NewMockSegment(t)
	seg.EXPECT().DatabaseName().Return("default").Maybe()
	seg.EXPECT().ResourceGroup().Return("rg").Maybe()
	seg.EXPECT().RetrieveByOffsets(mock.Anything, mock.AnythingOfType("*segcore.RetrievePlanWithOffsets")).
		RunAndReturn(func(ctx context.Context, plan *segcore.RetrievePlanWithOffsets) (*segcorepb.RetrieveResults, error) {
			assert.Equal(t, []int64{101, 103, 102}, plan.Offsets)
			return &segcorepb.RetrieveResults{
				FieldsData: []*schemapb.FieldData{
					{
						FieldId:   101,
						FieldName: "age",
						Type:      schemapb.DataType_Int64,
						Field: &schemapb.FieldData_Scalars{
							Scalars: &schemapb.ScalarField{
								Data: &schemapb.ScalarField_LongData{
									LongData: &schemapb.LongArray{Data: []int64{30, 10, 20}},
								},
							},
						},
					},
				},
			}, nil
		}).Once()

	op := NewFetchFieldsDataOperator([]Segment{seg}, nil, nil)
	merged := &MergedResultWithOffsets{
		IDs: makeSegcoreIntIDs([]int64{1, 2, 3}),
		Selections: []OffsetSelection{
			{SegmentIndex: 0, Offset: 101},
			{SegmentIndex: 0, Offset: 103},
			{SegmentIndex: 0, Offset: 102},
		},
	}

	outs, err := op.Run(context.Background(), nil, merged)
	require.NoError(t, err)
	out := outs[0].(*segcorepb.RetrieveResults)
	assert.Equal(t, []int64{1, 2, 3}, out.GetIds().GetIntId().GetData())
	assert.Equal(t, []int64{30, 10, 20}, out.GetFieldsData()[0].GetScalars().GetLongData().GetData())
}

func TestFetchFieldsDataOperator_MultipleSegments(t *testing.T) {
	seg0 := NewMockSegment(t)
	seg1 := NewMockSegment(t)
	seg0.EXPECT().DatabaseName().Return("default").Maybe()
	seg0.EXPECT().ResourceGroup().Return("rg").Maybe()
	seg1.EXPECT().DatabaseName().Return("default").Maybe()
	seg1.EXPECT().ResourceGroup().Return("rg").Maybe()

	seg0.EXPECT().RetrieveByOffsets(mock.Anything, mock.AnythingOfType("*segcore.RetrievePlanWithOffsets")).
		RunAndReturn(func(ctx context.Context, plan *segcore.RetrievePlanWithOffsets) (*segcorepb.RetrieveResults, error) {
			assert.Equal(t, []int64{10, 11}, plan.Offsets)
			return &segcorepb.RetrieveResults{
				FieldsData: []*schemapb.FieldData{
					{
						FieldId:   101,
						FieldName: "age",
						Type:      schemapb.DataType_Int64,
						Field: &schemapb.FieldData_Scalars{
							Scalars: &schemapb.ScalarField{
								Data: &schemapb.ScalarField_LongData{
									LongData: &schemapb.LongArray{Data: []int64{100, 110}},
								},
							},
						},
					},
				},
			}, nil
		}).Once()

	seg1.EXPECT().RetrieveByOffsets(mock.Anything, mock.AnythingOfType("*segcore.RetrievePlanWithOffsets")).
		RunAndReturn(func(ctx context.Context, plan *segcore.RetrievePlanWithOffsets) (*segcorepb.RetrieveResults, error) {
			assert.Equal(t, []int64{20, 21}, plan.Offsets)
			return &segcorepb.RetrieveResults{
				FieldsData: []*schemapb.FieldData{
					{
						FieldId:   101,
						FieldName: "age",
						Type:      schemapb.DataType_Int64,
						Field: &schemapb.FieldData_Scalars{
							Scalars: &schemapb.ScalarField{
								Data: &schemapb.ScalarField_LongData{
									LongData: &schemapb.LongArray{Data: []int64{200, 210}},
								},
							},
						},
					},
				},
			}, nil
		}).Once()

	op := NewFetchFieldsDataOperator([]Segment{seg0, seg1}, nil, nil)
	merged := &MergedResultWithOffsets{
		IDs: makeSegcoreIntIDs([]int64{1, 2, 3, 4}),
		Selections: []OffsetSelection{
			{SegmentIndex: 0, Offset: 10},
			{SegmentIndex: 1, Offset: 20},
			{SegmentIndex: 0, Offset: 11},
			{SegmentIndex: 1, Offset: 21},
		},
	}

	outs, err := op.Run(context.Background(), nil, merged)
	require.NoError(t, err)
	out := outs[0].(*segcorepb.RetrieveResults)
	assert.Equal(t, []int64{1, 2, 3, 4}, out.GetIds().GetIntId().GetData())
	assert.Equal(t, []int64{100, 200, 110, 210}, out.GetFieldsData()[0].GetScalars().GetLongData().GetData())
}
