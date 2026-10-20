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

package dql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/agg"
	"github.com/milvus-io/milvus/internal/util/queryutil"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/internal/util/reduce/orderby"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
)

func testSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "val", DataType: schemapb.DataType_Int64},
			{FieldID: common.TimeStampField, Name: "timestamp", DataType: schemapb.DataType_Int64},
		},
	}
}

func testSchemaWithStructArray() *schemapb.CollectionSchema {
	schema := testSchema()
	schema.StructArrayFields = []*schemapb.StructArrayFieldSchema{
		{
			FieldID: 200,
			Name:    "items",
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:     201,
					Name:        "items[tag]",
					DataType:    schemapb.DataType_Array,
					ElementType: schemapb.DataType_VarChar,
				},
			},
		},
	}
	return schema
}

func makeTestIntIDs(ids []int64) *schemapb.IDs {
	return &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: ids}}}
}

func makeTestInt64Field(fieldID int64, name string, vals []int64) *schemapb.FieldData {
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

// =========================================================================
// Plain query pipeline
// =========================================================================

func TestNewQueryPipeline_Plain(t *testing.T) {
	schema := testSchema()
	pipeline, err := NewQueryPipeline(
		schema, 3, 0, reduce.IReduceNoOrder,
		nil, nil, nil, nil,
		[]int64{100, 101},
	)
	require.NoError(t, err)
	require.NotNil(t, pipeline)

	// Two results sorted by PK
	r1 := &internalpb.RetrieveResults{
		Ids: makeTestIntIDs([]int64{1, 3}),
		FieldsData: []*schemapb.FieldData{
			makeTestInt64Field(100, "pk", []int64{1, 3}),
			makeTestInt64Field(101, "val", []int64{10, 30}),
			makeTestInt64Field(common.TimeStampField, "timestamp", []int64{100, 100}),
		},
	}
	r2 := &internalpb.RetrieveResults{
		Ids: makeTestIntIDs([]int64{2, 4}),
		FieldsData: []*schemapb.FieldData{
			makeTestInt64Field(100, "pk", []int64{2, 4}),
			makeTestInt64Field(101, "val", []int64{20, 40}),
			makeTestInt64Field(common.TimeStampField, "timestamp", []int64{100, 100}),
		},
	}

	result, err := pipeline.Execute(context.Background(), []*internalpb.RetrieveResults{r1, r2})
	require.NoError(t, err)
	assert.NotNil(t, result)
	// Should have 2 field columns (pk, val) — timestamp is dropped by complementFieldOperator
	assert.Equal(t, 2, len(result.GetFieldsData()))
}

func TestNewQueryPipeline_Plain_ComplementFields(t *testing.T) {
	schema := testSchema()
	pipeline, err := NewQueryPipeline(
		schema, 2, 0, reduce.IReduceNoOrder,
		nil, nil, nil, nil,
		[]int64{100, 101},
	)
	require.NoError(t, err)

	// Input without FieldName set (simulates segcore output)
	r := &internalpb.RetrieveResults{
		Ids: makeTestIntIDs([]int64{1, 2}),
		FieldsData: []*schemapb.FieldData{
			makeTestInt64Field(100, "", []int64{1, 2}),
			makeTestInt64Field(101, "", []int64{10, 20}),
			makeTestInt64Field(common.TimeStampField, "", []int64{100, 100}),
		},
	}
	result, err := pipeline.Execute(context.Background(), []*internalpb.RetrieveResults{r})
	require.NoError(t, err)

	// complementFieldOperator should set FieldName from schema and drop timestamp
	for _, fd := range result.GetFieldsData() {
		assert.NotEmpty(t, fd.GetFieldName(), "FieldName should be set by complementFieldOperator")
		assert.NotEqual(t, int64(common.TimeStampField), fd.GetFieldId(), "timestamp should be dropped")
	}
}

// =========================================================================
// ORDER BY pipeline
// =========================================================================

func TestNewQueryPipeline_OrderBy(t *testing.T) {
	schema := testSchema()
	orderByFields := []*orderby.OrderByField{
		orderby.NewOrderByField(101, "val", schemapb.DataType_Int64),
	}
	pipeline, err := NewQueryPipeline(
		schema, 3, 0, reduce.IReduceNoOrder,
		orderByFields, nil, nil, nil,
		[]int64{100, 101},
	)
	require.NoError(t, err)

	// Two shard results, unsorted by val
	r1 := &internalpb.RetrieveResults{
		Ids: makeTestIntIDs([]int64{1, 2}),
		FieldsData: []*schemapb.FieldData{
			makeTestInt64Field(100, "pk", []int64{1, 2}),
			makeTestInt64Field(101, "val", []int64{30, 10}),
			makeTestInt64Field(common.TimeStampField, "timestamp", []int64{100, 100}),
		},
	}
	r2 := &internalpb.RetrieveResults{
		Ids: makeTestIntIDs([]int64{3, 4}),
		FieldsData: []*schemapb.FieldData{
			makeTestInt64Field(100, "pk", []int64{3, 4}),
			makeTestInt64Field(101, "val", []int64{20, 5}),
			makeTestInt64Field(common.TimeStampField, "timestamp", []int64{100, 100}),
		},
	}

	result, err := pipeline.Execute(context.Background(), []*internalpb.RetrieveResults{r1, r2})
	require.NoError(t, err)
	assert.NotNil(t, result)
	// Timestamp should be dropped
	for _, fd := range result.GetFieldsData() {
		assert.NotEqual(t, int64(common.TimeStampField), fd.GetFieldId())
	}
}

func TestNewQueryPipeline_OrderBy_WithOffset(t *testing.T) {
	schema := testSchema()
	orderByFields := []*orderby.OrderByField{
		orderby.NewOrderByField(101, "val", schemapb.DataType_Int64),
	}
	pipeline, err := NewQueryPipeline(
		schema, 2, 1, reduce.IReduceNoOrder, // limit=2, offset=1
		orderByFields, nil, nil, nil,
		[]int64{100, 101},
	)
	require.NoError(t, err)

	r := &internalpb.RetrieveResults{
		Ids: makeTestIntIDs([]int64{1, 2, 3}),
		FieldsData: []*schemapb.FieldData{
			makeTestInt64Field(100, "pk", []int64{1, 2, 3}),
			makeTestInt64Field(101, "val", []int64{30, 10, 20}),
			makeTestInt64Field(common.TimeStampField, "timestamp", []int64{100, 100, 100}),
		},
	}

	result, err := pipeline.Execute(context.Background(), []*internalpb.RetrieveResults{r})
	require.NoError(t, err)
	assert.NotNil(t, result)
}

// =========================================================================
// Empty results
// =========================================================================

func TestNewQueryPipeline_EmptyResult(t *testing.T) {
	schema := testSchema()
	pipeline, err := NewQueryPipeline(
		schema, 10, 0, reduce.IReduceNoOrder,
		nil, nil, nil, nil,
		[]int64{100, 101},
	)
	require.NoError(t, err)

	// Empty results
	result, err := pipeline.Execute(context.Background(), []*internalpb.RetrieveResults{})
	require.NoError(t, err)
	assert.NotNil(t, result)
	// FillRetrieveResultIfEmpty should fill empty field data with schema
	assert.True(t, len(result.GetFieldsData()) > 0, "empty result should have field schema filled")
}

// =========================================================================
// GROUP BY pipeline
// =========================================================================

func TestNewQueryPipeline_GroupBy(t *testing.T) {
	schema := testSchema()

	// Build outputMap: user wants ["val", "count(*)"]
	// GROUP BY val, count(*) aggregation
	countAggs, err := agg.NewAggregate("count", 500, "count(*)", 0)
	require.NoError(t, err)
	aggsBases := make([]agg.AggregateBase, len(countAggs))
	copy(aggsBases, countAggs)
	outputMap, err := agg.NewAggregationFieldMap(
		[]string{"val", "count(*)"},
		[]string{"val"},
		aggsBases,
	)
	require.NoError(t, err)

	pipeline, err := NewQueryPipeline(
		schema, 10, 0, reduce.IReduceNoOrder,
		nil,          // no ORDER BY
		[]int64{101}, // group by val
		[]*planpb.Aggregate{{Op: planpb.AggregateOp_count, FieldId: 500}},
		outputMap,
		nil, // outputFieldIDs not used for GROUP BY
	)
	require.NoError(t, err)

	// Two shard results with GROUP BY layout: [group_col(val), agg_col(count)]
	r1 := &internalpb.RetrieveResults{
		FieldsData: []*schemapb.FieldData{
			makeTestInt64Field(101, "val", []int64{10, 20}),
			makeTestInt64Field(500, "count", []int64{3, 5}),
		},
	}
	r2 := &internalpb.RetrieveResults{
		FieldsData: []*schemapb.FieldData{
			makeTestInt64Field(101, "val", []int64{10, 30}),
			makeTestInt64Field(500, "count", []int64{2, 4}),
		},
	}

	result, err := pipeline.Execute(context.Background(), []*internalpb.RetrieveResults{r1, r2})
	require.NoError(t, err)
	assert.NotNil(t, result)
	// Should have 2 field columns: val and count(*)
	assert.Equal(t, 2, len(result.GetFieldsData()))
}

func TestNewQueryPipeline_GroupByCountStarWithStructSchema(t *testing.T) {
	schema := testSchemaWithStructArray()

	countAggs, err := agg.NewAggregate("count", 0, "count(*)", schemapb.DataType_None)
	require.NoError(t, err)
	aggsBases := make([]agg.AggregateBase, len(countAggs))
	copy(aggsBases, countAggs)
	outputMap, err := agg.NewAggregationFieldMap(
		[]string{"pk", "count(*)"},
		[]string{"pk"},
		aggsBases,
	)
	require.NoError(t, err)

	pipeline, err := NewQueryPipeline(
		schema, 10, 0, reduce.IReduceNoOrder,
		nil,
		[]int64{100},
		[]*planpb.Aggregate{{Op: planpb.AggregateOp_count, FieldId: 0}},
		outputMap,
		nil,
	)
	require.NoError(t, err)

	r1 := &internalpb.RetrieveResults{
		FieldsData: []*schemapb.FieldData{
			makeTestInt64Field(0, "", []int64{1, 2}),
			makeTestInt64Field(0, "", []int64{2, 1}),
		},
	}
	r2 := &internalpb.RetrieveResults{
		FieldsData: []*schemapb.FieldData{
			makeTestInt64Field(0, "", []int64{1, 3}),
			makeTestInt64Field(0, "", []int64{3, 4}),
		},
	}

	result, err := pipeline.Execute(context.Background(), []*internalpb.RetrieveResults{r1, r2})
	require.NoError(t, err)
	require.NotNil(t, result)

	result.OutputFields = []string{"pk", "count(*)"}
	reconstructStructFieldDataForQuery(result, schema)

	require.Len(t, result.GetFieldsData(), 2)
	assert.Equal(t, "pk", result.GetFieldsData()[0].GetFieldName())
	assert.Equal(t, "count(*)", result.GetFieldsData()[1].GetFieldName())
	assert.Equal(t, int64(0), result.GetFieldsData()[1].GetFieldId())
	assert.Equal(t, []string{"pk", "count(*)"}, result.GetOutputFields())

	pks := result.GetFieldsData()[0].GetScalars().GetLongData().GetData()
	counts := result.GetFieldsData()[1].GetScalars().GetLongData().GetData()
	require.Len(t, pks, len(counts))

	countByPK := make(map[int64]int64, len(pks))
	for i, pk := range pks {
		countByPK[pk] = counts[i]
	}
	assert.Equal(t, map[int64]int64{1: 5, 2: 1, 3: 4}, countByPK)
}

func TestNewQueryPipeline_GroupByOrderBy(t *testing.T) {
	schema := testSchema()

	countAggs, err := agg.NewAggregate("count", 500, "count(*)", 0)
	require.NoError(t, err)
	aggsBases := make([]agg.AggregateBase, len(countAggs))
	copy(aggsBases, countAggs)
	outputMap, err := agg.NewAggregationFieldMap(
		[]string{"val", "count(*)"},
		[]string{"val"},
		aggsBases,
	)
	require.NoError(t, err)

	// ORDER BY count(*) descending
	orderByFields := []*orderby.OrderByField{
		orderby.NewOrderByField(500, "count(*)", schemapb.DataType_Int64, orderby.WithAscending(false)),
	}

	pipeline, err := NewQueryPipeline(
		schema, 10, 0, reduce.IReduceNoOrder,
		orderByFields,
		[]int64{101},
		[]*planpb.Aggregate{{Op: planpb.AggregateOp_count, FieldId: 500}},
		outputMap,
		nil,
	)
	require.NoError(t, err)

	// Raw reducer layout: [group_col(val=101), agg_col(count=500)]
	r1 := &internalpb.RetrieveResults{
		FieldsData: []*schemapb.FieldData{
			makeTestInt64Field(101, "val", []int64{10, 20}),
			makeTestInt64Field(500, "count", []int64{3, 5}),
		},
	}
	r2 := &internalpb.RetrieveResults{
		FieldsData: []*schemapb.FieldData{
			makeTestInt64Field(101, "val", []int64{10, 30}),
			makeTestInt64Field(500, "count", []int64{2, 4}),
		},
	}

	result, err := pipeline.Execute(context.Background(), []*internalpb.RetrieveResults{r1, r2})
	require.NoError(t, err)
	assert.NotNil(t, result)
	// After agg remap: should have user's output_fields [val, count(*)]
	assert.Equal(t, 2, len(result.GetFieldsData()))
}

func TestGroupByLimitPreservesAggregateAcrossWorkers(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 201, Name: "region", DataType: schemapb.DataType_Int64},
		{FieldID: 202, Name: "city", DataType: schemapb.DataType_Int64},
		{FieldID: 203, Name: "value", DataType: schemapb.DataType_Int64},
	}}
	groupBy := []int64{201, 202}
	aggregates := []*planpb.Aggregate{{Op: planpb.AggregateOp_sum, FieldId: 203}}
	sumAggs, err := agg.NewAggregate("sum", 203, "sum(value)", schemapb.DataType_Int64)
	require.NoError(t, err)
	outputMap, err := agg.NewAggregationFieldMap(
		[]string{"region", "city", "sum(value)"},
		[]string{"region", "city"}, sumAggs,
	)
	require.NoError(t, err)

	partial := func(cities ...int64) *internalpb.RetrieveResults {
		regions := make([]int64, len(cities))
		values := make([]int64, len(cities))
		for i := range cities {
			regions[i], values[i] = 1, 1
		}
		return &internalpb.RetrieveResults{FieldsData: []*schemapb.FieldData{
			makeTestInt64Field(201, "region", regions),
			makeTestInt64Field(202, "city", cities),
			makeTestInt64Field(203, "sum(value)", values),
		}}
	}
	partialOrder := []*orderby.OrderByField{
		orderby.NewOrderByField(201, "region", schemapb.DataType_Int64),
	}
	completeOrder := []*orderby.OrderByField{
		partialOrder[0],
		orderby.NewOrderByField(202, "city", schemapb.DataType_Int64),
	}

	for _, tc := range []struct {
		name    string
		orderBy []*orderby.OrderByField
		workers [][]*internalpb.RetrieveResults
	}{
		{
			name: "without order by",
			workers: [][]*internalpb.RetrieveResults{
				{partial(2), partial(1)},
				{partial(1), partial(2)},
			},
		},
		{
			name:    "order by subset of group keys",
			orderBy: partialOrder,
			workers: [][]*internalpb.RetrieveResults{
				{partial(2, 1)},
				{partial(1, 2)},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			workerResults := make([]*internalpb.RetrieveResults, len(tc.workers))
			for i, segments := range tc.workers {
				worker, err := queryutil.BuildQueryReducePipeline(
					"worker", schema, 1, reduce.IReduceNoOrder,
					tc.orderBy, groupBy, aggregates, 0, nil, nil,
				)
				require.NoError(t, err)
				msg, err := worker.Run(context.Background(), nil,
					queryutil.OpMsg{queryutil.PipelineInput: segments})
				require.NoError(t, err)
				workerResults[i] = msg[queryutil.PipelineOutput].(*internalpb.RetrieveResults)
			}

			proxyOrder := tc.orderBy
			if len(proxyOrder) > 0 {
				proxyOrder = completeOrder // QueryTask passes the order completed during plan creation.
			}
			proxy, err := NewQueryPipeline(schema, 1, 0, reduce.IReduceNoOrder,
				proxyOrder, groupBy, aggregates, outputMap, nil)
			require.NoError(t, err)
			result, err := proxy.Execute(context.Background(), workerResults)
			require.NoError(t, err)
			require.Len(t, result.GetFieldsData(), 3)
			require.Equal(t, []int64{2}, result.GetFieldsData()[2].GetScalars().GetLongData().GetData())
			if len(tc.orderBy) > 0 {
				require.Equal(t, []int64{1}, result.GetFieldsData()[1].GetScalars().GetLongData().GetData())
			}
		})
	}

	// Proxy must apply the same tie order even when a worker returns both
	// groups; otherwise it can select a group pruned on another worker.
	proxy, err := NewQueryPipeline(schema, 1, 0, reduce.IReduceNoOrder,
		completeOrder,
		groupBy, aggregates, outputMap, nil)
	require.NoError(t, err)
	result, err := proxy.Execute(context.Background(), []*internalpb.RetrieveResults{partial(2, 1)})
	require.NoError(t, err)
	require.Equal(t, []int64{1}, result.GetFieldsData()[1].GetScalars().GetLongData().GetData())
}

// =========================================================================
// Element-level (element_filter) pipeline
// =========================================================================

func TestNewQueryPipeline_Plain_ElementIndices(t *testing.T) {
	schema := testSchema()
	pipeline, err := NewQueryPipeline(
		schema, 3, 0, reduce.IReduceNoOrder,
		nil, nil, nil, nil,
		[]int64{100, 101},
	)
	require.NoError(t, err)

	// Simulate element_filter results: same PK appears multiple times with different offsets
	r := &internalpb.RetrieveResults{
		Ids: makeTestIntIDs([]int64{1, 1, 2}),
		FieldsData: []*schemapb.FieldData{
			makeTestInt64Field(100, "pk", []int64{1, 1, 2}),
			makeTestInt64Field(101, "val", []int64{10, 10, 20}),
			makeTestInt64Field(common.TimeStampField, "timestamp", []int64{100, 100, 100}),
		},
		ElementLevel: true,
		ElementIndices: []*internalpb.ElementIndices{
			{Indices: []int32{3}},
			{Indices: []int32{5}},
			{Indices: []int32{0}},
		},
	}

	result, err := pipeline.Execute(context.Background(), []*internalpb.RetrieveResults{r})
	require.NoError(t, err)

	// ElementIndices should be propagated to the output
	require.Len(t, result.GetElementIndices(), 3)
	assert.Equal(t, []int64{3}, result.GetElementIndices()[0].GetIndices().GetData())
	assert.Equal(t, []int64{5}, result.GetElementIndices()[1].GetIndices().GetData())
	assert.Equal(t, []int64{0}, result.GetElementIndices()[2].GetIndices().GetData())

	// Timestamp should still be dropped
	for _, fd := range result.GetFieldsData() {
		assert.NotEqual(t, int64(common.TimeStampField), fd.GetFieldId())
	}
}

func TestNewQueryPipeline_Plain_NoElementLevel(t *testing.T) {
	schema := testSchema()
	pipeline, err := NewQueryPipeline(
		schema, 3, 0, reduce.IReduceNoOrder,
		nil, nil, nil, nil,
		[]int64{100, 101},
	)
	require.NoError(t, err)

	// Normal (non-element-level) results should have no ElementIndices
	r := &internalpb.RetrieveResults{
		Ids: makeTestIntIDs([]int64{1, 2}),
		FieldsData: []*schemapb.FieldData{
			makeTestInt64Field(100, "pk", []int64{1, 2}),
			makeTestInt64Field(101, "val", []int64{10, 20}),
			makeTestInt64Field(common.TimeStampField, "timestamp", []int64{100, 100}),
		},
		ElementLevel: false,
	}

	result, err := pipeline.Execute(context.Background(), []*internalpb.RetrieveResults{r})
	require.NoError(t, err)
	assert.Empty(t, result.GetElementIndices())
}

// =========================================================================
// Error handling
// =========================================================================

func TestNewQueryPipeline_GroupByOrderBy_InvalidField(t *testing.T) {
	schema := testSchema()
	// ORDER BY on a field that doesn't exist in GROUP BY or aggregates
	orderByFields := []*orderby.OrderByField{
		orderby.NewOrderByField(999, "nonexistent", schemapb.DataType_Int64),
	}
	_, err := NewQueryPipeline(
		schema, 10, 0, reduce.IReduceNoOrder,
		orderByFields,
		[]int64{101}, // group by val
		nil,          // no aggregates
		nil,          // outputMap (nil ok since we expect error before use)
		nil,
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}
