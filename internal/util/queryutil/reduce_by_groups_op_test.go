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

package queryutil

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
)

func testGroupBySchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name: "group_test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 200, Name: "color", DataType: schemapb.DataType_VarChar},
			{FieldID: 300, Name: "price", DataType: schemapb.DataType_Int64},
			{FieldID: 500, Name: "count_alias", DataType: schemapb.DataType_Int64},
		},
	}
}

func TestDeduplicateByGroupsOperator(t *testing.T) {
	ctx := context.Background()
	schema := testGroupBySchema()

	t.Run("basic count aggregation", func(t *testing.T) {
		op := NewDeduplicateByGroupsOperator(schema, []int64{200}, []*planpb.Aggregate{{Op: planpb.AggregateOp_count, FieldId: 500}}, 10)
		res1 := &internalpb.RetrieveResults{FieldsData: []*schemapb.FieldData{
			makeStringField(200, "color", []string{"blue", "red"}),
			makeInt64Field(500, "count", []int64{1, 2}),
		}}
		res2 := &internalpb.RetrieveResults{FieldsData: []*schemapb.FieldData{
			makeStringField(200, "color", []string{"blue", "green"}),
			makeInt64Field(500, "count", []int64{3, 4}),
		}}

		outs, err := op.Run(ctx, nil, []*internalpb.RetrieveResults{res1, res2})
		require.NoError(t, err)
		out := outs[0].(*internalpb.RetrieveResults)

		groups := out.GetFieldsData()[0].GetScalars().GetStringData().GetData()
		counts := out.GetFieldsData()[1].GetScalars().GetLongData().GetData()
		require.Equal(t, len(groups), len(counts))

		actual := map[string]int64{}
		for i := range groups {
			actual[groups[i]] = counts[i]
		}
		assert.Equal(t, int64(4), actual["blue"])
		assert.Equal(t, int64(2), actual["red"])
		assert.Equal(t, int64(4), actual["green"])
	})

	t.Run("group limit truncation", func(t *testing.T) {
		op := NewDeduplicateByGroupsOperator(schema, []int64{200}, []*planpb.Aggregate{{Op: planpb.AggregateOp_count, FieldId: 500}}, 2)
		res1 := &internalpb.RetrieveResults{FieldsData: []*schemapb.FieldData{
			makeStringField(200, "color", []string{"a", "b", "c", "d", "e"}),
			makeInt64Field(500, "count", []int64{1, 1, 1, 1, 1}),
		}}
		res2 := &internalpb.RetrieveResults{FieldsData: []*schemapb.FieldData{
			makeStringField(200, "color", []string{"a", "b", "c", "d", "e"}),
			makeInt64Field(500, "count", []int64{1, 1, 1, 1, 1}),
		}}

		outs, err := op.Run(ctx, nil, []*internalpb.RetrieveResults{res1, res2})
		require.NoError(t, err)
		out := outs[0].(*internalpb.RetrieveResults)
		assert.Len(t, out.GetFieldsData()[0].GetScalars().GetStringData().GetData(), 2)
	})

	t.Run("group limit unlimited", func(t *testing.T) {
		op := NewDeduplicateByGroupsOperator(schema, []int64{200}, []*planpb.Aggregate{{Op: planpb.AggregateOp_count, FieldId: 500}}, -1)
		res1 := &internalpb.RetrieveResults{FieldsData: []*schemapb.FieldData{
			makeStringField(200, "color", []string{"a", "b", "c", "d", "e"}),
			makeInt64Field(500, "count", []int64{1, 1, 1, 1, 1}),
		}}
		res2 := &internalpb.RetrieveResults{FieldsData: []*schemapb.FieldData{
			makeStringField(200, "color", []string{"a", "b", "c", "d", "e"}),
			makeInt64Field(500, "count", []int64{1, 1, 1, 1, 1}),
		}}

		outs, err := op.Run(ctx, nil, []*internalpb.RetrieveResults{res1, res2})
		require.NoError(t, err)
		out := outs[0].(*internalpb.RetrieveResults)
		assert.Len(t, out.GetFieldsData()[0].GetScalars().GetStringData().GetData(), 5)
	})

	t.Run("empty input", func(t *testing.T) {
		op := NewDeduplicateByGroupsOperator(schema, []int64{200}, []*planpb.Aggregate{{Op: planpb.AggregateOp_count, FieldId: 500}}, 10)
		outs, err := op.Run(ctx, nil, []*internalpb.RetrieveResults{})
		require.NoError(t, err)
		out := outs[0].(*internalpb.RetrieveResults)
		require.Len(t, out.GetFieldsData(), 2)
		assert.Len(t, out.GetFieldsData()[0].GetScalars().GetStringData().GetData(), 0)
		assert.Equal(t, []int64{0}, out.GetFieldsData()[1].GetScalars().GetLongData().GetData())
	})

	t.Run("multiple aggregate columns", func(t *testing.T) {
		op := NewDeduplicateByGroupsOperator(
			schema,
			[]int64{200},
			[]*planpb.Aggregate{{Op: planpb.AggregateOp_count, FieldId: 500}, {Op: planpb.AggregateOp_sum, FieldId: 300}},
			10,
		)
		res1 := &internalpb.RetrieveResults{FieldsData: []*schemapb.FieldData{
			makeStringField(200, "color", []string{"blue", "red"}),
			makeInt64Field(500, "count", []int64{1, 1}),
			makeInt64Field(300, "sum", []int64{10, 20}),
		}}
		res2 := &internalpb.RetrieveResults{FieldsData: []*schemapb.FieldData{
			makeStringField(200, "color", []string{"blue", "red"}),
			makeInt64Field(500, "count", []int64{1, 2}),
			makeInt64Field(300, "sum", []int64{5, 7}),
		}}

		outs, err := op.Run(ctx, nil, []*internalpb.RetrieveResults{res1, res2})
		require.NoError(t, err)
		out := outs[0].(*internalpb.RetrieveResults)

		groups := out.GetFieldsData()[0].GetScalars().GetStringData().GetData()
		counts := out.GetFieldsData()[1].GetScalars().GetLongData().GetData()
		sums := out.GetFieldsData()[2].GetScalars().GetLongData().GetData()
		actualCount := map[string]int64{}
		actualSum := map[string]int64{}
		for i := range groups {
			actualCount[groups[i]] = counts[i]
			actualSum[groups[i]] = sums[i]
		}

		assert.Equal(t, int64(2), actualCount["blue"])
		assert.Equal(t, int64(3), actualCount["red"])
		assert.Equal(t, int64(15), actualSum["blue"])
		assert.Equal(t, int64(27), actualSum["red"])
	})

	t.Run("min aggregation", func(t *testing.T) {
		op := NewDeduplicateByGroupsOperator(schema, []int64{200}, []*planpb.Aggregate{{Op: planpb.AggregateOp_min, FieldId: 300}}, 10)
		res1 := &internalpb.RetrieveResults{FieldsData: []*schemapb.FieldData{
			makeStringField(200, "color", []string{"blue", "red"}),
			makeInt64Field(300, "min_price", []int64{10, 20}),
		}}
		res2 := &internalpb.RetrieveResults{FieldsData: []*schemapb.FieldData{
			makeStringField(200, "color", []string{"blue", "red"}),
			makeInt64Field(300, "min_price", []int64{5, 25}),
		}}

		outs, err := op.Run(ctx, nil, []*internalpb.RetrieveResults{res1, res2})
		require.NoError(t, err)
		out := outs[0].(*internalpb.RetrieveResults)

		groups := out.GetFieldsData()[0].GetScalars().GetStringData().GetData()
		mins := out.GetFieldsData()[1].GetScalars().GetLongData().GetData()
		actual := map[string]int64{}
		for i := range groups {
			actual[groups[i]] = mins[i]
		}
		assert.Equal(t, int64(5), actual["blue"])
		assert.Equal(t, int64(20), actual["red"])
	})

	t.Run("max aggregation", func(t *testing.T) {
		op := NewDeduplicateByGroupsOperator(schema, []int64{200}, []*planpb.Aggregate{{Op: planpb.AggregateOp_max, FieldId: 300}}, 10)
		res1 := &internalpb.RetrieveResults{FieldsData: []*schemapb.FieldData{
			makeStringField(200, "color", []string{"blue", "red"}),
			makeInt64Field(300, "max_price", []int64{10, 20}),
		}}
		res2 := &internalpb.RetrieveResults{FieldsData: []*schemapb.FieldData{
			makeStringField(200, "color", []string{"blue", "red"}),
			makeInt64Field(300, "max_price", []int64{15, 8}),
		}}

		outs, err := op.Run(ctx, nil, []*internalpb.RetrieveResults{res1, res2})
		require.NoError(t, err)
		out := outs[0].(*internalpb.RetrieveResults)

		groups := out.GetFieldsData()[0].GetScalars().GetStringData().GetData()
		maxes := out.GetFieldsData()[1].GetScalars().GetLongData().GetData()
		actual := map[string]int64{}
		for i := range groups {
			actual[groups[i]] = maxes[i]
		}
		assert.Equal(t, int64(15), actual["blue"])
		assert.Equal(t, int64(20), actual["red"])
	})

	t.Run("sum aggregation standalone", func(t *testing.T) {
		op := NewDeduplicateByGroupsOperator(schema, []int64{200}, []*planpb.Aggregate{{Op: planpb.AggregateOp_sum, FieldId: 300}}, 10)
		res1 := &internalpb.RetrieveResults{FieldsData: []*schemapb.FieldData{
			makeStringField(200, "color", []string{"blue", "red"}),
			makeInt64Field(300, "sum_price", []int64{10, 20}),
		}}
		res2 := &internalpb.RetrieveResults{FieldsData: []*schemapb.FieldData{
			makeStringField(200, "color", []string{"blue"}),
			makeInt64Field(300, "sum_price", []int64{7}),
		}}

		outs, err := op.Run(ctx, nil, []*internalpb.RetrieveResults{res1, res2})
		require.NoError(t, err)
		out := outs[0].(*internalpb.RetrieveResults)

		groups := out.GetFieldsData()[0].GetScalars().GetStringData().GetData()
		sums := out.GetFieldsData()[1].GetScalars().GetLongData().GetData()
		actual := map[string]int64{}
		for i := range groups {
			actual[groups[i]] = sums[i]
		}
		assert.Equal(t, int64(17), actual["blue"])
		assert.Equal(t, int64(20), actual["red"])
	})
}
