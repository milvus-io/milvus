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

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/internal/util/reduce/orderby"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
)

func makeInt64Field(fieldID int64, name string, vals []int64) *schemapb.FieldData {
	return &schemapb.FieldData{
		FieldId:   fieldID,
		FieldName: name,
		Type:      schemapb.DataType_Int64,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{Data: vals},
				},
			},
		},
	}
}

func makeStringField(fieldID int64, name string, vals []string) *schemapb.FieldData {
	return &schemapb.FieldData{
		FieldId:   fieldID,
		FieldName: name,
		Type:      schemapb.DataType_VarChar,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{Data: vals},
				},
			},
		},
	}
}

func makeInternalResultIntPK(ids []int64, fields ...*schemapb.FieldData) *internalpb.RetrieveResults {
	return &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: ids}},
		},
		FieldsData: fields,
	}
}

func testSchemaForPipelineBuilders(pkType schemapb.DataType) *schemapb.CollectionSchema {
	pk := &schemapb.FieldSchema{
		FieldID:      100,
		Name:         "pk",
		DataType:     pkType,
		IsPrimaryKey: true,
	}
	return &schemapb.CollectionSchema{
		Name:   "test_coll",
		Fields: []*schemapb.FieldSchema{pk, {FieldID: 101, Name: "age", DataType: schemapb.DataType_Int64}, {FieldID: 200, Name: "color", DataType: schemapb.DataType_VarChar}},
	}
}

func TestComputeGroupByOrderPositions(t *testing.T) {
	tests := []struct {
		name     string
		groupBy  []int64
		aggs     []*planpb.Aggregate
		orderBy  []*orderby.OrderByField
		expected []int
	}{
		{
			name:     "order by group column",
			groupBy:  []int64{100, 200},
			aggs:     []*planpb.Aggregate{{Op: planpb.AggregateOp_sum, FieldId: 301}},
			orderBy:  []*orderby.OrderByField{{FieldID: 200}},
			expected: []int{1},
		},
		{
			name:    "order by aggregate column",
			groupBy: []int64{100},
			aggs: []*planpb.Aggregate{
				{Op: planpb.AggregateOp_count, FieldId: 500},
				{Op: planpb.AggregateOp_sum, FieldId: 301},
			},
			orderBy:  []*orderby.OrderByField{{FieldID: 301}},
			expected: []int{2},
		},
		{
			name:     "order by mixed columns",
			groupBy:  []int64{100, 200},
			aggs:     []*planpb.Aggregate{{Op: planpb.AggregateOp_count, FieldId: 500}},
			orderBy:  []*orderby.OrderByField{{FieldID: 100}, {FieldID: 500}},
			expected: []int{0, 2},
		},
		{
			name:     "empty order by",
			groupBy:  []int64{100},
			aggs:     []*planpb.Aggregate{{Op: planpb.AggregateOp_count, FieldId: 500}},
			orderBy:  nil,
			expected: []int{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ComputeGroupByOrderPositions(tt.orderBy, tt.groupBy, tt.aggs)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestBuildQueryReducePipeline(t *testing.T) {
	schema := testSchemaForPipelineBuilders(schemapb.DataType_Int64)
	ctx := context.Background()

	t.Run("plain", func(t *testing.T) {
		pipeline, err := BuildQueryReducePipeline(
			"plain", schema, 3, reduce.IReduceNoOrder,
			nil, nil, nil, 0,
		)
		require.NoError(t, err)
		assert.Contains(t, pipeline.String(), OpReduceByPKTS)
		assert.NotContains(t, pipeline.String(), OpOrderByLimit)

		// Plain query: ReduceByPK does k-way merge (PK-sorted inputs → PK-sorted output)
		res1 := makeInternalResultIntPK(
			[]int64{1, 3},
			makeInt64Field(999, "dummy", []int64{10, 30}),
		)
		res2 := makeInternalResultIntPK(
			[]int64{2, 3},
			makeInt64Field(999, "dummy", []int64{20, 300}),
		)

		msg, err := pipeline.Run(ctx, nil, OpMsg{PipelineInput: []*internalpb.RetrieveResults{res1, res2}})
		require.NoError(t, err)
		out := msg[PipelineOutput].(*internalpb.RetrieveResults)
		assert.Equal(t, []int64{1, 2, 3}, out.GetIds().GetIntId().GetData())
	})

	t.Run("order by", func(t *testing.T) {
		orderBy := []*orderby.OrderByField{orderby.NewOrderByField(101, "age", schemapb.DataType_Int64)}
		pipeline, err := BuildQueryReducePipeline(
			"orderby", schema, 3, reduce.IReduceNoOrder,
			orderBy, nil, nil, 0,
		)
		require.NoError(t, err)
		assert.Contains(t, pipeline.String(), OpDeduplicatePK)
		assert.Contains(t, pipeline.String(), OpOrderByLimit)

		res1 := makeInternalResultIntPK(
			[]int64{1, 3},
			makeInt64Field(100, "pk", []int64{1, 3}),
			makeInt64Field(101, "age", []int64{30, 10}),
		)
		res2 := makeInternalResultIntPK(
			[]int64{2, 4},
			makeInt64Field(100, "pk", []int64{2, 4}),
			makeInt64Field(101, "age", []int64{20, 40}),
		)

		msg, err := pipeline.Run(ctx, nil, OpMsg{PipelineInput: []*internalpb.RetrieveResults{res1, res2}})
		require.NoError(t, err)
		out := msg[PipelineOutput].(*internalpb.RetrieveResults)
		assert.Equal(t, []int64{3, 2, 1}, out.GetIds().GetIntId().GetData())
		assert.Equal(t, []int64{10, 20, 30}, out.GetFieldsData()[1].GetScalars().GetLongData().GetData())
	})

	t.Run("group by", func(t *testing.T) {
		aggs := []*planpb.Aggregate{{Op: planpb.AggregateOp_count, FieldId: 500}}
		// topK=2 means groupLimit=2: only 2 groups are kept, but existing groups
		// must still accumulate correctly across all results.
		pipeline, err := BuildQueryReducePipeline(
			"groupby", schema, 2, reduce.IReduceNoOrder,
			nil, []int64{200}, aggs, 0,
		)
		require.NoError(t, err)
		assert.Contains(t, pipeline.String(), OpReduceByGroups)
		assert.NotContains(t, pipeline.String(), OpOrderByLimit)

		res1 := &internalpb.RetrieveResults{FieldsData: []*schemapb.FieldData{
			makeStringField(200, "color", []string{"blue", "red"}),
			makeInt64Field(500, "count", []int64{1, 2}),
		}}
		res2 := &internalpb.RetrieveResults{FieldsData: []*schemapb.FieldData{
			makeStringField(200, "color", []string{"blue", "green"}),
			makeInt64Field(500, "count", []int64{3, 4}),
		}}

		msg, err := pipeline.Run(ctx, nil, OpMsg{PipelineInput: []*internalpb.RetrieveResults{res1, res2}})
		require.NoError(t, err)
		out := msg[PipelineOutput].(*internalpb.RetrieveResults)
		colors := out.GetFieldsData()[0].GetScalars().GetStringData().GetData()
		counts := out.GetFieldsData()[1].GetScalars().GetLongData().GetData()
		// groupLimit=2: blue and red are kept (first 2 groups from res1), green is new and dropped.
		// blue must accumulate across results: 1+3=4. red stays 2.
		require.Len(t, colors, 2)
		require.Len(t, counts, 2)
		actual := map[string]int64{}
		for i := range colors {
			actual[colors[i]] = counts[i]
		}
		assert.Equal(t, int64(4), actual["blue"])
		assert.Equal(t, int64(2), actual["red"])
	})

	t.Run("group by order by", func(t *testing.T) {
		aggs := []*planpb.Aggregate{{Op: planpb.AggregateOp_count, FieldId: 500}}
		orderBy := []*orderby.OrderByField{orderby.NewOrderByField(500, "count", schemapb.DataType_Int64, orderby.WithAscending(false))}
		pipeline, err := BuildQueryReducePipeline(
			"groupby-orderby", schema, 2, reduce.IReduceNoOrder,
			orderBy, []int64{200}, aggs, 0,
		)
		require.NoError(t, err)
		assert.Contains(t, pipeline.String(), OpReduceByGroups)
		assert.Contains(t, pipeline.String(), OpOrderByLimit)

		res1 := &internalpb.RetrieveResults{FieldsData: []*schemapb.FieldData{
			makeStringField(200, "color", []string{"blue", "red"}),
			makeInt64Field(500, "count", []int64{1, 5}),
		}}
		res2 := &internalpb.RetrieveResults{FieldsData: []*schemapb.FieldData{
			makeStringField(200, "color", []string{"blue", "green"}),
			makeInt64Field(500, "count", []int64{3, 2}),
		}}

		msg, err := pipeline.Run(ctx, nil, OpMsg{PipelineInput: []*internalpb.RetrieveResults{res1, res2}})
		require.NoError(t, err)
		out := msg[PipelineOutput].(*internalpb.RetrieveResults)
		colors := out.GetFieldsData()[0].GetScalars().GetStringData().GetData()
		counts := out.GetFieldsData()[1].GetScalars().GetLongData().GetData()
		// Aggregation: blue=1+3=4, red=5, green=2
		// ORDER BY count DESC, topK=2 → red(5), blue(4)
		require.Len(t, counts, 2)
		assert.Equal(t, "red", colors[0])
		assert.Equal(t, int64(5), counts[0])
		assert.Equal(t, "blue", colors[1])
		assert.Equal(t, int64(4), counts[1])
	})
}
