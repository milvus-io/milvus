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

package expr

import (
	"context"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/internal/util/function/pyudf"
)

func TestPyUDFExprRequestScopeIsIndependentOfWheelResourceAndUserParams(t *testing.T) {
	enablePyUDFForTest(t)
	mockPyUDFPath(t, nil)
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	input := newPyUDFFloat32Chunked(pool, [][]float32{{1, 2}})
	defer input.Release()
	parent, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Minute))
	defer cancel()
	scope := types.RequestContext{Database: "host_database", Collection: "documents", User: "alice", ResourceName: "rank_chain/step/0/Map"}
	ctx := types.WithRequestContext(parent, scope)
	deadline, _ := parent.Deadline()
	params := &schemapb.FunctionParamObject{Fields: map[string]*schemapb.FunctionParamValue{
		"user": stringParam("forged_user"), "database": stringParam("forged_database"), "resource_name": stringParam("forged_resource"),
	}}
	calls := 0
	client := &fakePyUDFClient{execute: func(call context.Context, req pyudf.ExecuteRequest) ([]*arrow.Chunked, error) {
		calls++
		require.Equal(t, "registered_rank_wheel", req.ResourceName)
		require.Equal(t, "/tmp/rank.whl", req.UDFPath)
		require.Same(t, params, req.Params)
		actualDeadline, _ := call.Deadline()
		require.Equal(t, deadline, actualDeadline)
		if calls == 1 {
			require.NotNil(t, req.RequestContext)
			require.Equal(t, scope, *req.RequestContext)
			req.RequestContext.User = "mutated transport snapshot"
			require.Equal(t, "alice", types.GetRequestContext(call).User)
		} else {
			require.Nil(t, req.RequestContext, "a later unscoped call must not inherit the cached expression's previous scope")
		}
		input.Retain()
		return []*arrow.Chunked{input}, nil
	}}
	function, err := NewPyUDFExpr("registered_rank_wheel", params, client)
	require.NoError(t, err)
	outputs, err := function.Execute(types.NewFuncContextFull(ctx, pool, types.StageL2Rerank), []*arrow.Chunked{input})
	require.NoError(t, err)
	releasePyUDFOutputs(outputs)
	outputs, err = function.Execute(types.NewFuncContextFull(parent, pool, types.StageL2Rerank), []*arrow.Chunked{input})
	require.NoError(t, err)
	releasePyUDFOutputs(outputs)
	require.Equal(t, 2, calls)
	require.Equal(t, scope, types.GetRequestContext(ctx))
}
