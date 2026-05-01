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
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/queryutil"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
)

func makeInternalIntIDs(ids []int64) *schemapb.IDs {
	return &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: ids}}}
}

func makeInt64Field(fieldID int64, name string, vals []int64) *schemapb.FieldData {
	return &schemapb.FieldData{
		FieldId:   fieldID,
		FieldName: name,
		Type:      schemapb.DataType_Int64,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: vals}},
			},
		},
	}
}

func makeSegcoreResult(ids []int64, allCnt int64) *segcorepb.RetrieveResults {
	return &segcorepb.RetrieveResults{Ids: makeInternalIntIDs(ids), AllRetrieveCount: allCnt}
}

func buildQueryReqForDelegator(limit int64, outputFieldIDs []int64, orderBy []*planpb.OrderByField, groupBy []int64, aggs []*planpb.Aggregate) *querypb.QueryRequest {
	planNode := &planpb.PlanNode{
		Node: &planpb.PlanNode_Query{
			Query: &planpb.QueryPlanNode{
				OrderByFields:   orderBy,
				GroupByFieldIds: groupBy,
				Aggregates:      aggs,
			},
		},
	}
	serialized, _ := proto.Marshal(planNode)
	return &querypb.QueryRequest{
		Req: &internalpb.RetrieveRequest{
			Limit:              limit,
			SerializedExprPlan: serialized,
			OutputFieldsId:     outputFieldIDs,
			GroupByFieldIds:    groupBy,
			Aggregates:         aggs,
			OrderByFields:      orderBy,
		},
	}
}

func testQueryPipelineSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name: "query_pipeline",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "age", DataType: schemapb.DataType_Int64},
			{FieldID: 200, Name: "color", DataType: schemapb.DataType_VarChar},
		},
	}
}

func TestAllSegcoreResultsEmpty(t *testing.T) {
	assert.True(t, allSegcoreResultsEmpty([]*segcorepb.RetrieveResults{nil, nil}))
	assert.False(t, allSegcoreResultsEmpty([]*segcorepb.RetrieveResults{nil, makeSegcoreResult([]int64{1, 2}, 2)}))
	assert.True(t, allSegcoreResultsEmpty([]*segcorepb.RetrieveResults{{Ids: &schemapb.IDs{}}}))
	assert.True(t, allSegcoreResultsEmpty(nil))
}

func TestRunDelegatorQueryPipeline(t *testing.T) {
	schema := testQueryPipelineSchema()
	ctx := context.Background()

	t.Run("plain query dedup and stats", func(t *testing.T) {
		req := buildQueryReqForDelegator(3, []int64{100, 101}, nil, nil, nil)
		res1 := &internalpb.RetrieveResults{
			Ids:              makeInternalIntIDs([]int64{1, 3}),
			FieldsData:       []*schemapb.FieldData{makeInt64Field(999, "dummy", []int64{10, 30}), makeInt64Field(100, "pk_sort_col", []int64{1, 3})},
			AllRetrieveCount: 2,
		}
		res2 := &internalpb.RetrieveResults{
			Ids:              makeInternalIntIDs([]int64{2, 3}),
			FieldsData:       []*schemapb.FieldData{makeInt64Field(999, "dummy", []int64{20, 300}), makeInt64Field(100, "pk_sort_col", []int64{2, 3})},
			AllRetrieveCount: 3,
		}
		out, err := RunDelegatorQueryPipeline(ctx, req, schema, []*internalpb.RetrieveResults{res1, res2})
		require.NoError(t, err)
		assert.Equal(t, []int64{1, 2, 3}, out.GetIds().GetIntId().GetData())
		assert.Equal(t, int64(5), out.GetAllRetrieveCount())
	})

	t.Run("order by query", func(t *testing.T) {
		req := buildQueryReqForDelegator(3, []int64{100, 101}, []*planpb.OrderByField{{FieldId: 101, Ascending: true, NullsFirst: false}}, nil, nil)
		res1 := &internalpb.RetrieveResults{
			Ids:        makeInternalIntIDs([]int64{1, 3}),
			FieldsData: []*schemapb.FieldData{makeInt64Field(100, "pk", []int64{1, 3}), makeInt64Field(101, "age", []int64{30, 10})},
		}
		res2 := &internalpb.RetrieveResults{
			Ids:        makeInternalIntIDs([]int64{2, 4}),
			FieldsData: []*schemapb.FieldData{makeInt64Field(100, "pk", []int64{2, 4}), makeInt64Field(101, "age", []int64{20, 40})},
		}
		out, err := RunDelegatorQueryPipeline(ctx, req, schema, []*internalpb.RetrieveResults{res1, res2})
		require.NoError(t, err)
		assert.Equal(t, []int64{3, 2, 1}, out.GetIds().GetIntId().GetData())
		assert.Equal(t, []int64{10, 20, 30}, out.GetFieldsData()[1].GetScalars().GetLongData().GetData())
	})

	t.Run("order by with duplicate PKs", func(t *testing.T) {
		// PK=3 appears in both results; should be deduped, then sorted by age ASC
		req := buildQueryReqForDelegator(3, []int64{100, 101}, []*planpb.OrderByField{{FieldId: 101, Ascending: true, NullsFirst: false}}, nil, nil)
		res1 := &internalpb.RetrieveResults{
			Ids:        makeInternalIntIDs([]int64{1, 3}),
			FieldsData: []*schemapb.FieldData{makeInt64Field(100, "pk", []int64{1, 3}), makeInt64Field(101, "age", []int64{30, 10})},
		}
		res2 := &internalpb.RetrieveResults{
			Ids:        makeInternalIntIDs([]int64{2, 3}),
			FieldsData: []*schemapb.FieldData{makeInt64Field(100, "pk", []int64{2, 3}), makeInt64Field(101, "age", []int64{20, 10})},
		}
		out, err := RunDelegatorQueryPipeline(ctx, req, schema, []*internalpb.RetrieveResults{res1, res2})
		require.NoError(t, err)
		// After dedup: PK=1(age=30), PK=2(age=20), PK=3(age=10)
		// After ORDER BY age ASC: PK=3(10), PK=2(20), PK=1(30)
		assert.Equal(t, []int64{3, 2, 1}, out.GetIds().GetIntId().GetData())
		ages := out.GetFieldsData()[1].GetScalars().GetLongData().GetData()
		assert.Equal(t, []int64{10, 20, 30}, ages)
	})

	t.Run("empty input", func(t *testing.T) {
		req := buildQueryReqForDelegator(3, []int64{100}, nil, nil, nil)
		out, err := RunDelegatorQueryPipeline(ctx, req, schema, nil)
		require.NoError(t, err)
		assert.NotNil(t, out)
		assert.NotNil(t, out.GetFieldsData())
	})
}

func TestRunDelegatorQueryPipeline_GroupBy(t *testing.T) {
	schema := testQueryPipelineSchema()
	ctx := context.Background()

	req := buildQueryReqForDelegator(10, nil,
		nil,          // no ORDER BY
		[]int64{200}, // GROUP BY color
		[]*planpb.Aggregate{{Op: planpb.AggregateOp_count, FieldId: 500}},
	)

	// Two QN results with GROUP BY layout: [group_col(color), agg_col(count)]
	res1 := &internalpb.RetrieveResults{
		FieldsData: []*schemapb.FieldData{
			{
				FieldId: 200, FieldName: "color", Type: schemapb.DataType_VarChar,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"red", "blue"}}},
				}},
			},
			makeInt64Field(500, "count", []int64{3, 5}),
		},
	}
	res2 := &internalpb.RetrieveResults{
		FieldsData: []*schemapb.FieldData{
			{
				FieldId: 200, FieldName: "color", Type: schemapb.DataType_VarChar,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"red", "green"}}},
				}},
			},
			makeInt64Field(500, "count", []int64{2, 4}),
		},
	}

	out, err := RunDelegatorQueryPipeline(ctx, req, schema, []*internalpb.RetrieveResults{res1, res2})
	require.NoError(t, err)
	assert.NotNil(t, out)
	// Should have 2 columns: color and count
	require.Len(t, out.GetFieldsData(), 2)
	// Should have 3 groups: red(5), blue(5), green(4)
	groups := out.GetFieldsData()[0].GetScalars().GetStringData().GetData()
	assert.Len(t, groups, 3)
}

func TestExtractSegcoreResult(t *testing.T) {
	req := &querypb.QueryRequest{Req: &internalpb.RetrieveRequest{OutputFieldsId: []int64{100}}}
	schema := testQueryPipelineSchema()
	segStats := []*segcorepb.RetrieveResults{
		{AllRetrieveCount: 2, HasMoreResult: true, ScannedRemoteBytes: 10, ScannedTotalBytes: 20},
		{AllRetrieveCount: 3, HasMoreResult: false, ScannedRemoteBytes: 20, ScannedTotalBytes: 30},
	}

	t.Run("segcore output", func(t *testing.T) {
		msg := queryutil.OpMsg{queryutil.PipelineOutput: &segcorepb.RetrieveResults{Ids: makeInternalIntIDs([]int64{1, 2})}}
		out, err := extractSegcoreResult(msg, segStats, req, schema)
		require.NoError(t, err)
		assert.Equal(t, []int64{1, 2}, out.GetIds().GetIntId().GetData())
		assert.Equal(t, int64(5), out.GetAllRetrieveCount())
		assert.True(t, out.GetHasMoreResult())
	})

	t.Run("internal output", func(t *testing.T) {
		msg := queryutil.OpMsg{queryutil.PipelineOutput: &internalpb.RetrieveResults{Ids: makeInternalIntIDs([]int64{3, 4})}}
		out, err := extractSegcoreResult(msg, segStats, req, schema)
		require.NoError(t, err)
		assert.Equal(t, []int64{3, 4}, out.GetIds().GetIntId().GetData())
	})

	t.Run("unexpected type", func(t *testing.T) {
		msg := queryutil.OpMsg{queryutil.PipelineOutput: "bad_output"}
		_, err := extractSegcoreResult(msg, segStats, req, schema)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unexpected pipeline output type")
	})
}

func TestRunQNQueryPipeline(t *testing.T) {
	schema := testQueryPipelineSchema()
	ctx := context.Background()
	plan := &planpb.PlanNode{Node: &planpb.PlanNode_Query{Query: &planpb.QueryPlanNode{}}}

	t.Run("standard non ignore non pk", func(t *testing.T) {
		req := &querypb.QueryRequest{Req: &internalpb.RetrieveRequest{Limit: 3, OutputFieldsId: []int64{100, 101}}}
		rp := &segcore.RetrievePlan{}

		segcoreResults := []*segcorepb.RetrieveResults{
			{
				Ids:              makeInternalIntIDs([]int64{1, 3}),
				FieldsData:       []*schemapb.FieldData{makeInt64Field(999, "dummy", []int64{10, 30}), makeInt64Field(100, "pk_sort_col", []int64{1, 3})},
				AllRetrieveCount: 2,
			},
			{
				Ids:              makeInternalIntIDs([]int64{2, 3}),
				FieldsData:       []*schemapb.FieldData{makeInt64Field(999, "dummy", []int64{20, 300}), makeInt64Field(100, "pk_sort_col", []int64{2, 3})},
				AllRetrieveCount: 3,
			},
		}

		out, err := RunQNQueryPipeline(ctx, req, schema, plan, segcoreResults, nil, nil, rp)
		require.NoError(t, err)
		assert.Equal(t, []int64{1, 2, 3}, out.GetIds().GetIntId().GetData())
		assert.Equal(t, int64(5), out.GetAllRetrieveCount())
	})

	t.Run("ignore non pk path", func(t *testing.T) {
		req := &querypb.QueryRequest{Req: &internalpb.RetrieveRequest{Limit: 2, OutputFieldsId: []int64{100, 101}}}
		rp := &segcore.RetrievePlan{}
		rp.SetIgnoreNonPk(true)

		seg0 := NewMockSegment(t)
		seg1 := NewMockSegment(t)
		seg0.EXPECT().DatabaseName().Return("default").Maybe()
		seg0.EXPECT().ResourceGroup().Return("rg").Maybe()
		seg1.EXPECT().DatabaseName().Return("default").Maybe()
		seg1.EXPECT().ResourceGroup().Return("rg").Maybe()

		seg0.EXPECT().RetrieveByOffsets(mock.Anything, mock.AnythingOfType("*segcore.RetrievePlanWithOffsets")).
			RunAndReturn(func(ctx context.Context, plan *segcore.RetrievePlanWithOffsets) (*segcorepb.RetrieveResults, error) {
				assert.Equal(t, []int64{10}, plan.Offsets)
				return &segcorepb.RetrieveResults{
					FieldsData: []*schemapb.FieldData{
						makeInt64Field(101, "age", []int64{100}),
					},
				}, nil
			}).Once()
		seg1.EXPECT().RetrieveByOffsets(mock.Anything, mock.AnythingOfType("*segcore.RetrievePlanWithOffsets")).
			RunAndReturn(func(ctx context.Context, plan *segcore.RetrievePlanWithOffsets) (*segcorepb.RetrieveResults, error) {
				assert.Equal(t, []int64{20}, plan.Offsets)
				return &segcorepb.RetrieveResults{
					FieldsData: []*schemapb.FieldData{
						makeInt64Field(101, "age", []int64{200}),
					},
				}, nil
			}).Once()

		segcoreResults := []*segcorepb.RetrieveResults{
			{
				Ids:              makeInternalIntIDs([]int64{1}),
				Offset:           []int64{10},
				FieldsData:       []*schemapb.FieldData{makeSegcoreTsField([]int64{100})},
				AllRetrieveCount: 2,
			},
			{
				Ids:              makeInternalIntIDs([]int64{2}),
				Offset:           []int64{20},
				FieldsData:       []*schemapb.FieldData{makeSegcoreTsField([]int64{200})},
				AllRetrieveCount: 3,
			},
		}

		out, err := RunQNQueryPipeline(ctx, req, schema, plan, segcoreResults, []Segment{seg0, seg1}, nil, rp)
		require.NoError(t, err)
		assert.Equal(t, []int64{1, 2}, out.GetIds().GetIntId().GetData())
		assert.Equal(t, []int64{100, 200}, out.GetFieldsData()[0].GetScalars().GetLongData().GetData())
		assert.Equal(t, int64(5), out.GetAllRetrieveCount())
	})

	t.Run("all empty segcore results", func(t *testing.T) {
		req := &querypb.QueryRequest{Req: &internalpb.RetrieveRequest{Limit: 2, OutputFieldsId: []int64{100}}}
		rp := &segcore.RetrievePlan{}

		out, err := RunQNQueryPipeline(ctx, req, schema, plan, []*segcorepb.RetrieveResults{nil, {Ids: &schemapb.IDs{}}}, nil, nil, rp)
		require.NoError(t, err)
		assert.NotNil(t, out)
		assert.NotNil(t, out.GetFieldsData())
	})

	// Regression test for https://github.com/milvus-io/milvus/issues/48603
	// When sparse filter prunes all segments on a QN worker, emptySegcoreResult
	// must return a valid aggregation result (e.g., count=0) instead of empty FieldsData.
	t.Run("all empty segcore results with count aggregation", func(t *testing.T) {
		countAgg := []*planpb.Aggregate{{Op: planpb.AggregateOp_count}}
		req := buildQueryReqForDelegator(-1, nil, nil, nil, countAgg)
		rp := &segcore.RetrievePlan{}

		out, err := RunQNQueryPipeline(ctx, req, schema, plan,
			[]*segcorepb.RetrieveResults{nil, {Ids: &schemapb.IDs{}}}, nil, nil, rp)
		require.NoError(t, err)
		require.Len(t, out.GetFieldsData(), 1, "count(*) should produce exactly 1 fieldData")
		countVal := out.GetFieldsData()[0].GetScalars().GetLongData().GetData()
		require.Len(t, countVal, 1)
		assert.Equal(t, int64(0), countVal[0], "count(*) on empty segments should be 0")
	})
}

func TestEmptySegcoreResultAggregation(t *testing.T) {
	schema := testQueryPipelineSchema()

	t.Run("count aggregation returns count=0", func(t *testing.T) {
		countAgg := []*planpb.Aggregate{{Op: planpb.AggregateOp_count}}
		req := buildQueryReqForDelegator(-1, nil, nil, nil, countAgg)

		result, err := emptySegcoreResult(req, schema)
		require.NoError(t, err)
		require.Len(t, result.GetFieldsData(), 1)
		assert.Equal(t, schemapb.DataType_Int64, result.GetFieldsData()[0].GetType())
		countVal := result.GetFieldsData()[0].GetScalars().GetLongData().GetData()
		require.Len(t, countVal, 1)
		assert.Equal(t, int64(0), countVal[0])
	})

	t.Run("group by with count aggregation returns empty group + count=0", func(t *testing.T) {
		countAgg := []*planpb.Aggregate{{Op: planpb.AggregateOp_count}}
		groupBy := []int64{101} // group by "age" (Int64)
		req := buildQueryReqForDelegator(-1, nil, nil, groupBy, countAgg)

		result, err := emptySegcoreResult(req, schema)
		require.NoError(t, err)
		// 1 group-by column + 1 count column = 2 fieldDatas
		require.Len(t, result.GetFieldsData(), 2)
	})

	t.Run("non-aggregation returns regular empty result", func(t *testing.T) {
		req := buildQueryReqForDelegator(10, []int64{100, 101}, nil, nil, nil)

		result, err := emptySegcoreResult(req, schema)
		require.NoError(t, err)
		// Regular empty result uses FillRetrieveResultIfEmpty with OutputFieldsId
		assert.Len(t, result.GetFieldsData(), 2)
	})
}

// Regression test: Delegator pipeline must handle a mix of empty and non-empty
// worker results for count(*) aggregation (issue #48603).
func TestDelegatorPipelineCountWithEmptyWorkerResult(t *testing.T) {
	schema := testQueryPipelineSchema()
	ctx := context.Background()
	countAgg := []*planpb.Aggregate{{Op: planpb.AggregateOp_count}}
	req := buildQueryReqForDelegator(-1, nil, nil, nil, countAgg)

	// Simulate: worker1 returned count=5, worker2 returned count=0 (from emptySegcoreResult fix)
	worker1 := &internalpb.RetrieveResults{
		FieldsData: []*schemapb.FieldData{makeInt64Field(0, "", []int64{5})},
	}
	worker2 := &internalpb.RetrieveResults{
		FieldsData: []*schemapb.FieldData{makeInt64Field(0, "", []int64{0})},
	}

	out, err := RunDelegatorQueryPipeline(ctx, req, schema, []*internalpb.RetrieveResults{worker1, worker2})
	require.NoError(t, err)
	require.Len(t, out.GetFieldsData(), 1)
	countVal := out.GetFieldsData()[0].GetScalars().GetLongData().GetData()
	require.Len(t, countVal, 1)
	assert.Equal(t, int64(5), countVal[0], "should sum counts from both workers")
}
