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

package pyudf

import (
	"context"
	"strconv"
	"sync"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/proto/pyudfpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestClientRequestScopePreservesExpandedSchemaAndIgnoresParams(t *testing.T) {
	request := clientRequest(t)
	request.Stage, request.TableResult = types.StagePostProcess, true
	request.Inputs = clientInput(t, 1, 0, 2)
	request.InputSchema = arrow.NewSchema([]arrow.Field{{Name: "score", Type: arrow.PrimitiveTypes.Float64}}, nil)
	request.QueryContext = &types.QueryContext{
		OriginalQueries: []*commonpb.PlaceholderValue{expansionQuery("a", "b")},
		Queries:         []*commonpb.PlaceholderValue{expansionQuery("a0", "a1", "b0")},
		QueryOrigins:    [][]int64{{0, 0, 1}},
	}
	request.RequestContext = &types.RequestContext{Database: "数据库", Collection: "docs", User: "alice", ResourceName: "pipeline/step/1/Map"}
	request.Params = &schemapb.FunctionParamObject{Fields: map[string]*schemapb.FunctionParamValue{
		"user":          {Value: &schemapb.FunctionParamValue_StringValue{StringValue: "mallory"}},
		"database":      {Value: &schemapb.FunctionParamValue_StringValue{StringValue: "another"}},
		"resource_name": {Value: &schemapb.FunctionParamValue_StringValue{StringValue: "spoof"}},
	}}
	wire, err := encodeExecuteRequest(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, requestContextExecutionVersion, wire.ExecutionVersion)
	require.Equal(t, "alice", wire.RequestContext.User)
	require.Equal(t, "数据库", wire.RequestContext.Database)
	require.Equal(t, "pipeline/step/1/Map", wire.RequestContext.ResourceName)
	require.Equal(t, request.ResourceName, wire.ResourceName, "wheel artifact name remains independent of the operator label")
	require.Equal(t, []int64{0, 0, 1}, wire.QueryContext.Queries[0].OriginalQueryIndices)
	schema, columns, err := decodeOutputsWithSchema(context.Background(), wire.Inputs)
	require.NoError(t, err)
	defer releaseColumns(columns)
	require.True(t, request.InputSchema.Equal(schema))
	request.RequestContext.User = "bob"
	require.Equal(t, "alice", wire.RequestContext.User, "encoded metadata is a value snapshot")

	request.QueryContext = nil
	request.TableResult, request.Stage = false, types.StageIngestion
	request.RequestContext = &types.RequestContext{}
	wire, err = encodeExecuteRequest(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, requestContextExecutionVersion, wire.ExecutionVersion)
	require.NotNil(t, wire.RequestContext, "an explicitly empty scope still requires acknowledgement")
	request.RequestContext = nil
	wire, err = encodeExecuteRequest(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, schemaExecutionVersion, wire.ExecutionVersion)
	require.Nil(t, wire.RequestContext)
}

func TestClientRequestScopeRejectsEveryOldSuccessAcknowledgement(t *testing.T) {
	for _, ack := range []uint32{0, 2, 3, 4, 5} {
		t.Run(strconv.Itoa(int(ack)), func(t *testing.T) {
			cfg, _ := startClientTestServer(t, func(_ context.Context, request *pyudfpb.ExecuteRequest) (*pyudfpb.ExecuteResponse, error) {
				require.Equal(t, requestContextExecutionVersion, request.ExecutionVersion)
				return &pyudfpb.ExecuteResponse{ExecutionVersion: ack, Result: &pyudfpb.ExecuteResponse_Outputs{Outputs: request.Inputs}}, nil
			})
			client := newTestClient(t, cfg)
			request := clientRequest(t)
			request.RequestContext = &types.RequestContext{Database: "db", Collection: "docs", User: "alice", ResourceName: "operator"}
			outputs, err := client.Execute(context.Background(), request)
			require.ErrorIs(t, err, merr.ErrServiceUnimplemented)
			require.Nil(t, outputs)
		})
	}
}

func TestClientRequestScopesRemainIsolatedAcrossConcurrentRPCs(t *testing.T) {
	cfg, _ := startClientTestServer(t, func(_ context.Context, request *pyudfpb.ExecuteRequest) (*pyudfpb.ExecuteResponse, error) {
		scope := request.GetRequestContext()
		require.Equal(t, "db-"+scope.User, scope.Database)
		require.Equal(t, "docs-"+scope.User, scope.Collection)
		require.Equal(t, "op-"+scope.User, scope.ResourceName)
		return &pyudfpb.ExecuteResponse{ExecutionVersion: requestContextExecutionVersion, Result: &pyudfpb.ExecuteResponse_Outputs{Outputs: request.Inputs}}, nil
	})
	client := newTestClient(t, cfg)
	request := clientRequest(t)
	var calls sync.WaitGroup
	for index := range 16 {
		calls.Add(1)
		go func() {
			defer calls.Done()
			user := strconv.Itoa(index)
			call := request
			call.RequestContext = &types.RequestContext{Database: "db-" + user, Collection: "docs-" + user, User: user, ResourceName: "op-" + user}
			outputs, err := client.Execute(context.Background(), call)
			require.NoError(t, err)
			releaseColumns(outputs)
		}()
	}
	calls.Wait()
}
