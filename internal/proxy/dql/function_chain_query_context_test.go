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
	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain/transform"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/internal/util/function/pyudf"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func queryContextUDFOp(output, input string) *schemapb.FunctionChainOp {
	op := mapOp(output, "py_udf", columnArg(input))
	op.Expr.Params = map[string]*schemapb.FunctionParamValue{"resource_name": chainStringParam("query_context_test")}
	return op
}

func TestSearchQueryContextFlowsThroughPreRerankAndPost(t *testing.T) {
	registerSearchTransformTest(t)
	defer mockey.Mock(pyudf.CheckEnabled).Return(nil).Build().UnPatch()
	defer mockey.Mock(pyudf.ResolveResourcePath).Return("/tmp/query-context.whl", nil).Build().UnPatch()
	defer mockey.Mock(pyudf.RuntimeConfig).Return(pyudf.Config{}, nil).Build().UnPatch()
	client := &pyudf.Client{}
	defer mockey.Mock(pyudf.NewClient).Return(client, nil).Build().UnPatch()
	var observed []pyudf.ExecuteRequest
	defer mockey.Mock((*pyudf.Client).ExecuteWithSchema).To(func(_ *pyudf.Client, _ context.Context, request pyudf.ExecuteRequest) (*arrow.Schema, []*arrow.Chunked, error) {
		observed = append(observed, request)
		request.Inputs[0].Retain()
		return request.InputSchema, []*arrow.Chunked{request.Inputs[0]}, nil
	}).Build().UnPatch()
	pre, err := transform.PreProcess("translate", transformTestMap("query", "query", "CN:"))
	require.NoError(t, err)
	post, err := transform.PostProcess("annotate", queryContextUDFOp("annotation", "tag"))
	require.NoError(t, err)
	l2 := &schemapb.FunctionChain{Stage: schemapb.FunctionChainStage_FunctionChainStageL2Rerank, Ops: []*schemapb.FunctionChainOp{queryContextUDFOp("$score", "$score")}}
	request := &milvuspb.SearchRequest{
		Nq: 2, SearchInput: &milvuspb.SearchRequest_PlaceholderGroup{PlaceholderGroup: transformTestPlaceholder(t, "one", "two")},
		FunctionChains: []*schemapb.FunctionChain{pre, l2, post},
	}
	original := proto.Clone(request)
	task := transformTestTask(request)
	task.ctx = context.Background()
	task.queryInfos = []*planpb.QueryInfo{{}}
	task.Topk = 2
	require.NoError(t, task.prepareSearchTransforms())
	require.NoError(t, task.executeSearchPreProcess(context.Background()))
	require.True(t, proto.Equal(original, request))
	require.Equal(t, [][]byte{[]byte("one"), []byte("two")}, task.queryContext.OriginalQueries[0].Values)
	require.Equal(t, [][]byte{[]byte("CN:ONE"), []byte("CN:TWO")}, task.queryContext.Queries[0].Values)
	task.rerankMeta, err = newFunctionChainRerankMeta([]*schemapb.FunctionChain{l2}, task.schema)
	require.NoError(t, err)
	op, err := newRerankOperator(task, nil)
	require.NoError(t, err)
	_, err = op.run(context.Background(), trace.SpanFromContext(context.Background()), []*milvuspb.SearchResults{transformTestResults([]int64{2, 1})}, []string{"COSINE"})
	require.NoError(t, err)
	require.NoError(t, task.executeSearchPostProcess(context.Background(), transformTestResults([]int64{2, 1})))
	require.Len(t, observed, 2)
	require.Equal(t, types.StageL2Rerank, observed[0].Stage)
	require.Equal(t, types.StagePostProcess, observed[1].Stage)
	for _, call := range observed {
		require.Same(t, task.queryContext, call.QueryContext)
		require.Len(t, call.Inputs, 1)
	}
	retained := task.queryContext
	require.NoError(t, task.executeSearchPreProcess(context.Background()))
	require.Same(t, retained, task.queryContext, "retrying the same task must not change query snapshots")
}

func TestHybridQueryContextRetainsEveryANNInput(t *testing.T) {
	registerSearchTransformTest(t)
	post, err := transform.PostProcess("post", queryContextUDFOp("annotation", "tag"))
	require.NoError(t, err)
	request := &milvuspb.SearchRequest{Nq: 2, FunctionChains: []*schemapb.FunctionChain{post}}
	for _, prefix := range []string{"DENSE:", "SPARSE:"} {
		pre, err := transform.PreProcess("translate", transformTestMap("query", "query", prefix))
		require.NoError(t, err)
		request.SubReqs = append(request.SubReqs, &milvuspb.SubSearchRequest{Nq: 2, PlaceholderGroup: transformTestPlaceholder(t, prefix+"one", prefix+"two"), FunctionChains: []*schemapb.FunctionChain{pre}})
	}
	task := transformTestTask(request)
	require.NoError(t, task.prepareSearchTransforms())
	require.NoError(t, task.executeSearchPreProcess(context.Background()))
	require.Len(t, task.queryContext.Queries, 2)
	require.Equal(t, []byte("DENSE:one"), task.queryContext.OriginalQueries[0].Values[0])
	require.Equal(t, []byte("SPARSE:two"), task.queryContext.OriginalQueries[1].Values[1])
	require.Equal(t, []byte("DENSE:DENSE:ONE"), task.queryContext.Queries[0].Values[0])
	require.Equal(t, []byte("SPARSE:SPARSE:TWO"), task.queryContext.Queries[1].Values[1])
}

func TestQueryContextRejectsMalformedRequestBeforeModelExecution(t *testing.T) {
	request := &milvuspb.SearchRequest{Nq: 2, FunctionChains: []*schemapb.FunctionChain{{Stage: schemapb.FunctionChainStage_FunctionChainStageL2Rerank, Ops: []*schemapb.FunctionChainOp{queryContextUDFOp("$score", "$score")}}}}
	for _, raw := range [][]byte{nil, {0xff}, transformTestPlaceholder(t, "only one"), transformTestPlaceholder(t, "valid", string([]byte{0xff}))} {
		request.SearchInput = &milvuspb.SearchRequest_PlaceholderGroup{PlaceholderGroup: raw}
		err := transformTestTask(request).executeSearchPreProcess(context.Background())
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
	}
}
