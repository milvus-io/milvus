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

package dml

import (
	"context"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/proxy/requestcontext"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/internal/util/function/pyudf"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func requestScopeStringParam(value string) *schemapb.FunctionParamValue {
	return &schemapb.FunctionParamValue{Value: &schemapb.FunctionParamValue_StringValue{StringValue: value}}
}

func TestInsertIngestionRequestScopeIsAuthenticatedAndRequestOwned(t *testing.T) {
	paramtable.Init()
	defer mockey.Mock(pyudf.CheckEnabled).Return(nil).Build().UnPatch()
	defer mockey.Mock(pyudf.ResolveResourcePath).Return("/tmp/request-scope.whl", nil).Build().UnPatch()
	defer mockey.Mock(pyudf.RuntimeConfig).Return(pyudf.Config{}, nil).Build().UnPatch()
	defer mockey.Mock(pyudf.NewClient).Return(&pyudf.Client{}, nil).Build().UnPatch()
	var scopes []types.RequestContext
	defer mockey.Mock((*pyudf.Client).ExecuteWithSchema).To(func(_ *pyudf.Client, _ context.Context, call pyudf.ExecuteRequest) (*arrow.Schema, []*arrow.Chunked, error) {
		require.Equal(t, types.StageIngestion, call.Stage)
		require.NotNil(t, call.RequestContext)
		scopes = append(scopes, *call.RequestContext)
		require.Equal(t, "spoof_user", call.Params.GetFields()["user"].GetStringValue())
		require.Equal(t, 3, call.Inputs[0].Len(), "scope accompanies expanded insertion rows")
		call.Inputs[0].Retain()
		return call.InputSchema, []*arrow.Chunked{call.Inputs[0]}, nil
	}).Build().UnPatch()
	for _, test := range []struct{ database, user string }{{database: "tenant_a", user: "alice"}, {database: "", user: "bob"}} {
		schema := ingestionTestSchema()
		schema.Name = "canonical_collection"
		udf := &schemapb.FunctionChainOp{Op: "map", Outputs: []string{"doc_id"}, Expr: &schemapb.FunctionChainExpr{
			Name: "py_udf", Args: []*schemapb.FunctionChainExprArg{{Arg: &schemapb.FunctionChainExprArg_Column{Column: &schemapb.FunctionChainColumnArg{Name: "doc_id"}}}},
			Params: map[string]*schemapb.FunctionParamValue{
				"resource_name": requestScopeStringParam("same_cached_wheel"),
				"udf_params":    {Value: &schemapb.FunctionParamValue_ObjectValue{ObjectValue: &schemapb.FunctionParamObject{Fields: map[string]*schemapb.FunctionParamValue{"user": requestScopeStringParam("spoof_user")}}}},
			},
		}}
		schema.FunctionChains[0].Ops = append(schema.FunctionChains[0].Ops[:1], udf, schema.FunctionChains[0].Ops[1])
		task := ingestionTestTask()
		task.insertMsg.DbName, task.insertMsg.CollectionName = test.database, "alias"
		ctx := requestcontext.WithAuthenticatedUser(context.Background(), test.user)
		ctx = types.WithRequestContext(ctx, types.RequestContext{Database: "spoof_db", Collection: "spoof_collection", User: "spoof_user"})
		require.NoError(t, task.applyIngestion(ctx, mustNewSchemaInfo(schema)))
		require.Empty(t, task.insertMsg.GetRowIDs(), "identity is attached before ID allocation and WAL")
		require.Equal(t, "spoof_user", types.GetRequestContext(ctx).User, "adapter leaves the caller's scope unchanged")
	}
	require.Len(t, scopes, 2)
	require.Equal(t, "tenant_a", scopes[0].Database)
	require.Equal(t, util.DefaultDBName, scopes[1].Database)
	require.Equal(t, "alice", scopes[0].User)
	require.Equal(t, "bob", scopes[1].User)
	for _, scope := range scopes {
		require.Equal(t, "canonical_collection", scope.Collection)
		require.Equal(t, "chunk/step/1/Map", scope.ResourceName)
	}
}
