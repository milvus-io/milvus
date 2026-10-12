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

package httpserver

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/internal/proxy/requestcontext"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestRESTL2WrapperPreservesVerifiedScopeInsteadOfGinClaims(t *testing.T) {
	ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ctx := requestcontext.WithAuthenticatedUser(context.Background(), "alice")
	ginCtx.Request = httptest.NewRequest(http.MethodPost, "/v2/vectordb/entities/search", nil).WithContext(ctx)
	ginCtx.Set(ContextUsername, "spoof_user")
	ctx = proxy.NewContextWithMetadata(ctx, "spoof_user", "tenant_db")
	chains, err := genFunctionChains([]FunctionChainReq{{
		Stage: "FunctionChainStageL2Rerank", Ops: []FunctionChainOpReq{{Op: "map", Outputs: []string{"$score"}, Expr: &FunctionChainExprReq{Name: "py_udf", Params: map[string]interface{}{"resource_name": "wheel"}}}},
	}})
	require.NoError(t, err, "legacy L2 remains a reachable REST stage")
	require.Equal(t, schemapb.FunctionChainStage_FunctionChainStageL2Rerank, chains[0].GetStage())
	request := &milvuspb.SearchRequest{DbName: "tenant_db", CollectionName: "alias", FunctionChains: chains}
	handler := &HandlersV2{}
	called := false
	_, err = handler.wrapperProxy(ctx, ginCtx, request, false, false, milvuspb.MilvusService_Search_FullMethodName, func(reqCtx context.Context, _ interface{}) (interface{}, error) {
		called = true
		scope := types.GetRequestContext(requestcontext.WithFunctionScope(reqCtx, "tenant_db", "canonical_collection"))
		require.Equal(t, "alice", scope.User)
		require.Equal(t, "tenant_db", scope.Database)
		require.Equal(t, "canonical_collection", scope.Collection)
		return &milvuspb.SearchResults{Status: merr.Success()}, nil
	})
	require.NoError(t, err)
	require.True(t, called)
}
