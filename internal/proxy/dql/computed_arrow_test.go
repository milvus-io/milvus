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
	"strings"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain"
	"github.com/milvus-io/milvus/internal/util/queryutil"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

func TestQueryComputedArrowColumnsThroughReducers(t *testing.T) {
	ctx := context.Background()
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "label", Type: arrow.BinaryTypes.String},
		{Name: "tags", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64)},
		{Name: "details", Type: arrow.ListOf(arrow.StructOf(arrow.Field{Name: "values", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64)}))},
	}, nil)
	makeInput := func(ids []int64, timestamp int64, labels string) *internalpb.RetrieveResults {
		builder := chain.NewDataFrameBuilder().SetChunkSizes([]int64{2})
		defer builder.Release()
		for i, input := range []string{labels, `[[10],[20,30]]`, `[[{"values":[1]}],[{"values":[2,3]}]]`} {
			values, _, err := array.FromJSON(pool, schema.Field(i).Type, strings.NewReader(input))
			require.NoError(t, err)
			require.NoError(t, builder.AddColumnFromChunks(schema.Field(i).Name, []arrow.Array{values}))
		}
		pk := array.NewInt64Builder(pool)
		pk.AppendValues(ids, nil)
		require.NoError(t, builder.AddColumnFromChunks("$id", []arrow.Array{pk.NewArray()}))
		pk.Release()
		ts := array.NewInt64Builder(pool)
		ts.AppendValues([]int64{timestamp, timestamp}, nil)
		require.NoError(t, builder.AddColumnFromChunks("$timestamp", []arrow.Array{ts.NewArray()}))
		ts.Release()
		frame := builder.Build()
		defer frame.Release()
		exported, err := chain.ToRetrieveResults(frame)
		require.NoError(t, err)
		require.Len(t, exported.FieldsData, 1)
		require.Equal(t, int64(common.TimeStampField), exported.FieldsData[0].FieldId)
		return &internalpb.RetrieveResults{Ids: exported.Ids, FieldsData: append(
			[]*schemapb.FieldData{makeTestInt64Field(100, "pk", ids)}, exported.FieldsData...),
			ComputedResults: exported.ComputedResults}

	}
	a := makeInput([]int64{1, 3}, 1, `["one","old"]`)
	b := makeInput([]int64{2, 3}, 2, `["two","latest"]`)
	pool.AssertSize(t, 0) // Both reducers operate after native/Arrow owners are gone.
	delegator, err := queryutil.BuildQueryReducePipeline(queryutil.PipelineNameDelegator, testSchema(), 3, reduce.IReduceNoOrder, nil, nil, nil, 1<<20, nil, nil)
	require.NoError(t, err)
	msg, err := delegator.Run(ctx, nil, queryutil.OpMsg{queryutil.PipelineInput: []*internalpb.RetrieveResults{a, b}})
	require.NoError(t, err)
	merged := msg[queryutil.PipelineOutput].(*internalpb.RetrieveResults)
	require.Equal(t, []string{"one", "two", "latest"}, merged.ComputedResults[0].GetScalars().GetStringData().Data)
	proxy, err := NewQueryPipeline(testSchema(), 2, 1, reduce.IReduceNoOrder, nil, nil, nil, nil, []int64{100})
	require.NoError(t, err)
	result, err := proxy.Execute(ctx, []*internalpb.RetrieveResults{merged})
	require.NoError(t, err)
	require.Equal(t, []int64{2, 3}, result.FieldsData[0].GetScalars().GetLongData().Data)
	columns := result.ComputedResults
	require.Equal(t, []string{"two", "latest"}, columns[0].GetScalars().GetStringData().Data)
	tags := columns[1].GetScalars().GetArrayData().Data
	require.Equal(t, []int64{10}, tags[0].GetLongData().Data)
	require.Equal(t, []int64{20, 30}, tags[1].GetLongData().Data)
	details := columns[2].GetStructArrays().Fields[0].GetScalars().GetArrayData().Data
	require.Equal(t, []int64{2, 3}, details[1].GetArrayData().Data[0].GetLongData().Data)
}
