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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/proto/pyudfpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestClientQueryContextAndExecutionVersion(t *testing.T) {
	queries := &types.QueryContext{
		OriginalQueries: []*commonpb.PlaceholderValue{{Type: commonpb.PlaceholderType_VarChar, Tag: "$0", Values: [][]byte{[]byte("first"), []byte("second")}}},
		Queries:         []*commonpb.PlaceholderValue{{Type: commonpb.PlaceholderType_VarChar, Tag: "$0", Values: [][]byte{[]byte("FIRST"), []byte("SECOND")}}},
	}
	for _, stage := range []string{types.StagePreProcess, types.StageL2Rerank, types.StagePostProcess} {
		t.Run(stage, func(t *testing.T) {
			request := clientRequest(t)
			request.Stage, request.QueryContext = stage, queries
			if stage == types.StagePreProcess {
				request.Inputs = clientInput(t, 2)
			}
			wire, err := encodeExecuteRequest(context.Background(), request)
			require.NoError(t, err)
			require.Equal(t, executionVersion, wire.GetExecutionVersion())
			require.Equal(t, "VarChar", wire.QueryContext.Queries[0].DataType)
			require.Equal(t, "$0", wire.QueryContext.Queries[0].Tag)
			require.Equal(t, queries.OriginalQueries[0].Values, wire.QueryContext.OriginalQueries[0].Values)
			require.Equal(t, queries.Queries[0].Values, wire.QueryContext.Queries[0].Values)
		})
	}
	request := clientRequest(t)
	request.QueryContext = &types.QueryContext{OriginalQueries: queries.OriginalQueries, Queries: nil}
	_, err := encodeExecuteRequest(context.Background(), request)
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	request.QueryContext = &types.QueryContext{OriginalQueries: queries.OriginalQueries, Queries: []*commonpb.PlaceholderValue{{Type: commonpb.PlaceholderType_VarChar, Values: [][]byte{[]byte("wrong count")}}}}
	_, err = encodeExecuteRequest(context.Background(), request)
	require.ErrorIs(t, err, merr.ErrServiceInternal)
}

func TestClientRejectsWorkerWithoutQueryContextContract(t *testing.T) {
	cfg, _ := startClientTestServer(t, func(ctx context.Context, request *pyudfpb.ExecuteRequest) (*pyudfpb.ExecuteResponse, error) {
		response, err := echoExecute(ctx, request)
		if response != nil {
			response.ExecutionVersion = 0 // An old worker ignores the request extension.
		}
		return response, err
	})
	client := newTestClient(t, cfg)
	outputs, err := client.Execute(context.Background(), clientRequest(t))
	require.ErrorIs(t, err, merr.ErrServiceUnimplemented)
	require.Nil(t, outputs)
	require.Equal(t, merr.SystemError, merr.GetErrorType(err))
}

func TestClientTableContextPreservesOriginalNQAfterGrouping(t *testing.T) {
	request := clientRequest(t)
	request.Stage, request.TableResult, request.Reduction = types.StagePostProcess, true, true
	request.Inputs = clientInput(t, 4)
	query := &commonpb.PlaceholderValue{Type: commonpb.PlaceholderType_VarChar, Values: [][]byte{[]byte("a"), []byte("b"), []byte("c")}}
	request.QueryContext = &types.QueryContext{OriginalQueries: []*commonpb.PlaceholderValue{query}, Queries: []*commonpb.PlaceholderValue{query}}
	wire, err := encodeExecuteRequest(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, tableExecutionVersion, wire.GetExecutionVersion())
	require.True(t, wire.GetTableResult())
	require.True(t, wire.GetReduction())
	require.Len(t, wire.GetQueryContext().GetQueries()[0].GetValues(), 3)
	request.QueryContext = nil // Query TABLE has no ANN input.
	_, err = encodeExecuteRequest(context.Background(), request)
	require.NoError(t, err)
	request.TableResult = false
	_, err = encodeExecuteRequest(context.Background(), request)
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	request.TableResult, request.Stage = true, types.StageIngestion
	_, err = encodeExecuteRequest(context.Background(), request)
	require.ErrorIs(t, err, merr.ErrServiceInternal)
}

func TestClientRejectsHitWorkerSuccessForTableRequest(t *testing.T) {
	cfg, _ := startClientTestServer(t, echoExecute) // Fixture acknowledges only v2.
	client := newTestClient(t, cfg)
	request := clientRequest(t)
	request.Stage, request.TableResult = types.StagePostProcess, true
	outputs, err := client.Execute(context.Background(), request)
	require.ErrorIs(t, err, merr.ErrServiceUnimplemented)
	require.Nil(t, outputs)
	require.Equal(t, merr.SystemError, merr.GetErrorType(err))
}
