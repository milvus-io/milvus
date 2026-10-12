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
	"github.com/milvus-io/milvus/internal/proxy/requestcontext"
	"github.com/milvus-io/milvus/internal/util/function/chain/transform"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/internal/util/function/pyudf"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func scopeTestUDFOp(output, input string) *schemapb.FunctionChainOp {
	op := queryContextUDFOp(output, input)
	op.Expr.Params["udf_params"] = &schemapb.FunctionParamValue{Value: &schemapb.FunctionParamValue_ObjectValue{ObjectValue: &schemapb.FunctionParamObject{Fields: map[string]*schemapb.FunctionParamValue{
		"user": chainStringParam("spoof_user"), "database": chainStringParam("spoof_db"), "collection": chainStringParam("spoof_collection"), "resource_name": chainStringParam("spoof_operator"),
	}}}}
	return op
}

func mockRequestScopeUDF(t *testing.T, observe func(pyudf.ExecuteRequest)) {
	t.Helper()
	paramtable.Init()
	for _, mocker := range []*mockey.Mocker{
		mockey.Mock(pyudf.CheckEnabled).Return(nil).Build(),
		mockey.Mock(pyudf.ResolveResourcePath).Return("/tmp/request-scope.whl", nil).Build(),
		mockey.Mock(pyudf.RuntimeConfig).Return(pyudf.Config{}, nil).Build(),
		mockey.Mock(pyudf.NewClient).Return(&pyudf.Client{}, nil).Build(),
		mockey.Mock((*pyudf.Client).ExecuteWithSchema).To(func(_ *pyudf.Client, _ context.Context, call pyudf.ExecuteRequest) (*arrow.Schema, []*arrow.Chunked, error) {
			observe(call)
			call.Inputs[0].Retain()
			return call.InputSchema, []*arrow.Chunked{call.Inputs[0]}, nil
		}).Build(),
	} {
		t.Cleanup(func() { mocker.UnPatch() })
	}
}

func TestSearchRequestScopeReachesPreL2AndPostWithoutSpoofing(t *testing.T) {
	var observed []types.RequestContext
	var stages []string
	mockRequestScopeUDF(t, func(call pyudf.ExecuteRequest) {
		require.NotNil(t, call.RequestContext)
		observed = append(observed, *call.RequestContext)
		stages = append(stages, call.Stage)
		require.Equal(t, "spoof_user", call.Params.GetFields()["user"].GetStringValue(), "params remain separate from host scope")
		require.Contains(t, call.RequestContext.ResourceName, "/step/0/Map")
		require.NotEqual(t, "spoof_operator", call.RequestContext.ResourceName)
	})
	for _, user := range []string{"alice", "bob"} {
		ctx := requestcontext.WithAuthenticatedUser(context.Background(), user)
		ctx = types.WithRequestContext(ctx, types.RequestContext{Database: "spoof_db", Collection: "spoof_collection", User: "spoof_user", ResourceName: "spoof_operator"})
		pre, err := transform.PreProcess("prepare_scope", scopeTestUDFOp("query", "query"))
		require.NoError(t, err)
		post, err := transform.PostProcess("post_scope", scopeTestUDFOp("annotation", "tag"))
		require.NoError(t, err)
		l2 := &schemapb.FunctionChain{Name: "rank_scope", Stage: schemapb.FunctionChainStage_FunctionChainStageL2Rerank, Ops: []*schemapb.FunctionChainOp{scopeTestUDFOp("$score", "$score")}}
		task := transformTestTask(&milvuspb.SearchRequest{
			DbName: "caller_db", CollectionName: "alias", Nq: 2,
			SearchInput:    &milvuspb.SearchRequest_PlaceholderGroup{PlaceholderGroup: transformTestPlaceholder(t, "one", "two")},
			FunctionChains: []*schemapb.FunctionChain{pre, l2, post},
		})
		task.ctx, task.rlsDBName = ctx, "resolved_"+user
		schema := proto.Clone(task.schema.CollectionSchema).(*schemapb.CollectionSchema)
		schema.Name = "canonical_" + user
		task.schema = mustNewSchemaInfo(schema)
		task.queryInfos, task.Topk = []*planpb.QueryInfo{{}}, 2
		require.NoError(t, task.prepareSearchTransforms())
		require.NoError(t, task.executeSearchPreProcess(ctx))
		task.rerankMeta, err = newFunctionChainRerankMeta([]*schemapb.FunctionChain{l2}, task.schema)
		require.NoError(t, err)
		op, err := newRerankOperator(task, nil)
		require.NoError(t, err)
		_, err = op.run(ctx, trace.SpanFromContext(ctx), []*milvuspb.SearchResults{transformTestResults([]int64{1, 1})}, []string{"COSINE"})
		require.NoError(t, err)
		require.NoError(t, task.executeSearchPostProcess(ctx, transformTestResults([]int64{1, 1})))
		for _, scope := range observed[len(observed)-3:] {
			require.Equal(t, "resolved_"+user, scope.Database)
			require.Equal(t, "canonical_"+user, scope.Collection)
			require.Equal(t, user, scope.User)
		}
		require.Equal(t, "spoof_user", types.GetRequestContext(ctx).User, "adapter scope does not mutate its caller")
	}
	require.Equal(t, []string{types.StagePreProcess, types.StageL2Rerank, types.StagePostProcess, types.StagePreProcess, types.StageL2Rerank, types.StagePostProcess}, stages)
	require.Equal(t, "alice", observed[0].User, "later requests cannot mutate prior snapshots")
}

func TestHybridANNPostTableRequestScopeUsesResolvedTarget(t *testing.T) {
	observed := false
	mockRequestScopeUDF(t, func(call pyudf.ExecuteRequest) {
		observed = true
		require.Equal(t, types.StagePostProcess, call.Stage)
		require.NotNil(t, call.RequestContext)
		require.Equal(t, "resolved_db", call.RequestContext.Database)
		require.Equal(t, "canonical_collection", call.RequestContext.Collection)
		require.Equal(t, "alice", call.RequestContext.User)
		require.Contains(t, call.RequestContext.ResourceName, "/step/0/Map")
	})
	plan := postTableTestPlan(t, true, scopeTestUDFOp("annotation", types.IDFieldName), transformTestSelect(t, "annotation"))
	task := postTableHybridTask(t, plan)
	ctx := requestcontext.WithAuthenticatedUser(context.Background(), "alice")
	task.ctx, task.rlsDBName = ctx, "resolved_db"
	task.schema.CollectionSchema.Name = "canonical_collection"
	task.request.CollectionName = "alias"
	pipeline, err := newSearchPipeline(task)
	require.NoError(t, err)
	_, _, err = pipeline.Run(ctx, trace.SpanFromContext(ctx), postTableHybridSource(t), segcore.StorageCost{})
	require.NoError(t, err)
	require.True(t, observed)
}

func TestQueryPostTableRequestScopeUsesCanonicalAndDefaultDatabase(t *testing.T) {
	var observed []types.RequestContext
	mockRequestScopeUDF(t, func(call pyudf.ExecuteRequest) {
		require.NotNil(t, call.RequestContext)
		observed = append(observed, *call.RequestContext)
	})
	for _, db := range []string{"resolved_db", ""} {
		plan := postTableTestPlan(t, false, scopeTestUDFOp("annotation", types.IDFieldName))
		schema := proto.Clone(newFunctionChainTestSchema().CollectionSchema).(*schemapb.CollectionSchema)
		schema.Name = "canonical_collection"
		task := &QueryTask{
			schema: mustNewSchemaInfo(schema), functionDBName: db,
			request: &milvuspb.QueryRequest{CollectionName: "alias", FunctionChains: []*schemapb.FunctionChain{plan}}, queryParams: &queryParams{},
			result: &milvuspb.QueryResults{FieldsData: []*schemapb.FieldData{postTablePKField(11, 12)}},
		}
		require.NoError(t, task.prepareQueryPostProcess())
		require.NoError(t, task.executeQueryPostProcess(requestcontext.WithAuthenticatedUser(context.Background(), "alice")))
	}
	require.Len(t, observed, 2)
	require.Equal(t, "resolved_db", observed[0].Database)
	require.Equal(t, util.DefaultDBName, observed[1].Database)
	for _, scope := range observed {
		require.Equal(t, "canonical_collection", scope.Collection)
		require.Equal(t, "alice", scope.User)
		require.Contains(t, scope.ResourceName, "/step/0/Map")
	}
}
