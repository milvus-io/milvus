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

package tasks

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestQueryTaskChecksChainExecution(t *testing.T) {
	paramtable.Init()
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 101, Name: "message", DataType: schemapb.DataType_Text},
	}}
	patch := mockey.Mock((*segments.Collection).Schema).Return(schema).Build()
	defer patch.UnPatch()
	pb := &schemapb.FunctionChain{
		Stage: schemapb.FunctionChainStage_FunctionChainStageQueryScan,
		Ops: []*schemapb.FunctionChainOp{{
			Op: "grep", Inputs: []string{"message"}, Outputs: []string{"matches"},
			Params: map[string]*schemapb.FunctionParamValue{
				"arguments": {Value: &schemapb.FunctionParamValue_StringValue{StringValue: `-e timeout`}},
				"limit":     {Value: &schemapb.FunctionParamValue_Int64Value{Int64Value: 10}},
			},
		}},
	}
	plan := &planpb.PlanNode{
		Node:                    &planpb.PlanNode_Query{Query: &planpb.QueryPlanNode{}},
		QuerynodeFunctionChains: []*schemapb.FunctionChain{pb},
	}
	run := func(p *planpb.PlanNode) error {
		data, err := proto.Marshal(p)
		require.NoError(t, err)
		req := &querypb.QueryRequest{Req: &internalpb.RetrieveRequest{SerializedExprPlan: data}}
		task := NewQueryTask(context.Background(), &segments.Collection{}, nil, req)
		defer task.scheduleSpan.End()
		return task.PreExecute()
	}
	err := run(plan)
	require.NoError(t, err)
	plan.QuerynodeFunctionChains[0].Ops[0].Op = "unsupported"
	require.ErrorIs(t, run(plan), merr.ErrParameterInvalid)
	plan.Node = nil
	require.ErrorIs(t, run(plan), merr.ErrServiceInternal)
	plan.QuerynodeFunctionChains = nil
	plan.Node = &planpb.PlanNode_Query{Query: &planpb.QueryPlanNode{}}
	require.NoError(t, run(plan))
}
