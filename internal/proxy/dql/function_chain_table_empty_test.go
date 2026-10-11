// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/proxy/taskmodel"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func postTableStringArrayField(id int64, name string, values ...[]string) *schemapb.FieldData {
	arrays := make([]*schemapb.ScalarField, len(values))
	for index, value := range values {
		arrays[index] = &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: value}}}
	}
	return &schemapb.FieldData{
		FieldId: id, FieldName: name, Type: schemapb.DataType_Array,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{
			ArrayData: &schemapb.ArrayArray{ElementType: schemapb.DataType_VarChar, Data: arrays},
		}}},
	}
}

func TestHybridANNPostTableEntireEmptyBranchHasTypedStoredFields(t *testing.T) {
	schema := proto.Clone(newFunctionChainTestSchema().CollectionSchema).(*schemapb.CollectionSchema)
	schema.Fields = append(schema.Fields,
		&schemapb.FieldSchema{FieldID: 106, Name: "tags", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_VarChar},
		&schemapb.FieldSchema{FieldID: 107, Name: "metadata", DataType: schemapb.DataType_JSON},
	)
	schema.StructArrayFields = []*schemapb.StructArrayFieldSchema{{FieldID: 200, Name: "sections", Fields: []*schemapb.FieldSchema{
		{FieldID: 201, Name: "sections[text]", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_VarChar},
		{FieldID: 202, Name: "sections[label]", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_VarChar},
	}}}
	for _, test := range []struct {
		name                    string
		emptyFirst, emptySecond bool
		wantANN                 []int64
	}{
		{name: "empty first branch", emptyFirst: true, wantANN: []int64{1, 1}},
		{name: "empty second branch", emptySecond: true, wantANN: []int64{0, 0}},
		{name: "all empty", emptyFirst: true, emptySecond: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			plan := postTableTestPlan(t, true, &schemapb.FunctionChainOp{Op: "concat"},
				transformTestSelect(t, "tag", "tags", "metadata", "sections", postANNIndexColumn))
			task := postTableHybridTask(t, plan, mustNewSchemaInfo(schema))
			task.needRequery = true
			calls := 0
			task.node = &postTableQueryRunner{
				namespaceRequeryMockNode: &namespaceRequeryMockNode{},
				execute: func(context.Context, taskmodel.Task, trace.Span) (*milvuspb.QueryResults, segcore.StorageCost, error) {
					calls++
					return &milvuspb.QueryResults{Status: merr.Success(), FieldsData: []*schemapb.FieldData{
						postTablePKField(10, 30), postTableStringField(102, "tag", "A", "C"),
						postTableStringArrayField(106, "tags", []string{"one", "two"}, []string{"three"}),
						{
							FieldId: 107, FieldName: "metadata", Type: schemapb.DataType_JSON,
							Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{Data: [][]byte{[]byte(`{"kind":1}`), []byte(`{"kind":2}`)}}}}},
						},
						postTableStringArrayField(201, "sections[text]", []string{"first"}, []string{"second"}),
						postTableStringArrayField(202, "sections[label]", []string{"a"}, []string{"c"}),
					}}, segcore.StorageCost{}, nil
				},
			}
			source := postTableHybridSource(t)
			for index, empty := range []bool{test.emptyFirst, test.emptySecond} {
				data := transformTestResults([]int64{1, 1}).GetResults()
				data.Ids.GetIntId().Data = []int64{10, 30}
				if index == 1 {
					data.Scores = []float32{-0.4, -0.6}
				}
				if empty {
					data = transformTestResults([]int64{0, 0}).GetResults()
				}
				data.TopK = 3
				source[0].SubResults[index].ResultData = data
			}
			pipeline, err := newSearchPipeline(task)
			require.NoError(t, err)
			result, _, err := pipeline.Run(context.Background(), trace.SpanFromContext(context.Background()), source, segcore.StorageCost{})
			require.NoError(t, err)
			table := decodePostTableTest(t, result.GetPostProcessResult())
			require.Equal(t, int64(len(test.wantANN)), table.NumRows())
			require.Equal(t, []string{"tag", "tags", "metadata", "sections", postANNIndexColumn}, []string{
				table.Schema().Field(0).Name, table.Schema().Field(1).Name, table.Schema().Field(2).Name, table.Schema().Field(3).Name, table.Schema().Field(4).Name,
			})
			require.Equal(t, arrow.BinaryTypes.String, table.Schema().Field(0).Type)
			require.Equal(t, arrow.LIST, table.Schema().Field(1).Type.ID())
			require.Equal(t, arrow.BINARY, table.Schema().Field(2).Type.ID())
			require.Equal(t, arrow.LIST, table.Schema().Field(3).Type.ID())
			if len(test.wantANN) > 0 {
				require.Equal(t, 1, calls)
				require.Equal(t, test.wantANN, postTableIntColumn(t, table, postANNIndexColumn))
				require.Equal(t, "A", table.Column(0).Data().Chunk(0).(*array.String).Value(0))
			} else {
				require.Equal(t, 0, calls)
			}
		})
	}
}

func TestPostTableEmptySourceCompletionDoesNotMaskMalformedPayload(t *testing.T) {
	plan := postTableTestPlan(t, false, transformTestSelect(t, "tag"))
	task := transformTestTask(&milvuspb.SearchRequest{Nq: 1, FunctionChains: []*schemapb.FunctionChain{plan}})
	require.NoError(t, task.prepareSearchTransforms())
	for _, rows := range []int64{0, 1} {
		result := transformTestResults([]int64{rows})
		field := postTableStringField(102, "tag", "unexpected")
		if rows > 0 {
			field.Field = nil
		}
		result.Results.FieldsData = []*schemapb.FieldData{field}
		err := task.executeSearchPostProcess(context.Background(), result)
		require.ErrorIs(t, err, merr.ErrServiceInternal)
		require.Equal(t, merr.SystemError, merr.GetErrorType(err))
	}
}
