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

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/proto/pyudfpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func expansionQuery(values ...string) *commonpb.PlaceholderValue {
	result := &commonpb.PlaceholderValue{Type: commonpb.PlaceholderType_VarChar, Tag: "$0"}
	for _, value := range values {
		result.Values = append(result.Values, []byte(value))
	}
	return result
}

func TestClientExpandedQueryContextRequiresVersionFive(t *testing.T) {
	request := clientRequest(t)
	request.Stage, request.TableResult = types.StagePostProcess, true
	request.Inputs = clientInput(t, 1, 0, 2)
	request.InputSchema = arrow.NewSchema([]arrow.Field{{Name: "score", Type: arrow.PrimitiveTypes.Float64}}, nil)
	request.QueryContext = &types.QueryContext{
		OriginalQueries: []*commonpb.PlaceholderValue{expansionQuery("a", "b"), expansionQuery("a", "b")},
		Queries:         []*commonpb.PlaceholderValue{expansionQuery("a0", "a1", "b0"), expansionQuery("b0")},
		QueryOrigins:    [][]int64{{0, 0, 1}, {1}},
	}
	wire, err := encodeExecuteRequest(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, queryExpansionExecutionVersion, wire.ExecutionVersion)
	require.Len(t, wire.QueryContext.OriginalQueries[0].Values, 2)
	require.Equal(t, []int64{0, 0, 1}, wire.QueryContext.Queries[0].OriginalQueryIndices)
	require.Equal(t, []int64{1}, wire.QueryContext.Queries[1].OriginalQueryIndices)

	request.Stage, request.TableResult = types.StagePreProcess, false
	request.Inputs = clientInput(t, 3)
	request.QueryContext.Queries = request.QueryContext.OriginalQueries
	request.QueryContext.QueryOrigins = nil
	request.QueryContext.RowQueryOrigins = []int64{0, 0, 1}
	wire, err = encodeExecuteRequest(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, queryExpansionExecutionVersion, wire.ExecutionVersion)
	require.Equal(t, []int64{0, 0, 1}, wire.QueryContext.RowQueryOrigins)
	require.Len(t, wire.QueryContext.Queries[0].Values, 2, "PRE snapshots stay at stage entry")
	request.Inputs = clientInput(t, 0)
	request.QueryContext.RowQueryOrigins = []int64{}
	wire, err = encodeExecuteRequest(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, queryExpansionExecutionVersion, wire.ExecutionVersion, "typed empty intermediate rows retain the provenance contract")

	request.Inputs = clientInput(t, 2)
	request.QueryContext.RowQueryOrigins = nil
	request.QueryContext.QueryOrigins = [][]int64{{0, 1}, {0, 1}}
	wire, err = encodeExecuteRequest(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, schemaExecutionVersion, wire.ExecutionVersion, "identity maps must not upgrade legacy schema requests")
}

func TestClientExpandedQueryContextRejectsBadMappings(t *testing.T) {
	for _, origins := range [][][]int64{nil, {{0}}, {{0, 2, 1}}, {{0, 1, 0}}, {{0, -1, 1}}, {{0, 0, 1}, {0}}} {
		request := clientRequest(t)
		request.Stage, request.TableResult = types.StagePostProcess, true
		request.Inputs = clientInput(t, 1, 1, 1)
		request.QueryContext = &types.QueryContext{
			OriginalQueries: []*commonpb.PlaceholderValue{expansionQuery("a", "b")},
			Queries:         []*commonpb.PlaceholderValue{expansionQuery("a0", "a1", "b0")},
			QueryOrigins:    origins,
		}
		_, err := encodeExecuteRequest(context.Background(), request)
		require.ErrorIs(t, err, merr.ErrServiceInternal)
	}
	request := clientRequest(t)
	request.Stage = types.StageL2Rerank
	request.QueryContext = &types.QueryContext{
		OriginalQueries: []*commonpb.PlaceholderValue{expansionQuery("a", "b")},
		Queries:         []*commonpb.PlaceholderValue{expansionQuery("a0", "a1", "b0")},
		QueryOrigins:    [][]int64{{0, 0, 1}},
	}
	_, err := encodeExecuteRequest(context.Background(), request)
	require.ErrorIs(t, err, merr.ErrServiceInternal, "candidate group count must match prepared NQ")
	request.QueryContext.Queries = request.QueryContext.OriginalQueries
	request.QueryContext.QueryOrigins = nil
	request.QueryContext.RowQueryOrigins = []int64{0, 1}
	_, err = encodeExecuteRequest(context.Background(), request)
	require.ErrorIs(t, err, merr.ErrServiceInternal, "current row maps are PRE-only")
}

func TestClientRejectsVersionFourSuccessForExpandedQuery(t *testing.T) {
	cfg, _ := startClientTestServer(t, func(ctx context.Context, request *pyudfpb.ExecuteRequest) (*pyudfpb.ExecuteResponse, error) {
		require.Equal(t, queryExpansionExecutionVersion, request.ExecutionVersion)
		return &pyudfpb.ExecuteResponse{ExecutionVersion: schemaExecutionVersion, Result: &pyudfpb.ExecuteResponse_Outputs{Outputs: request.Inputs}}, nil
	})
	client := newTestClient(t, cfg)
	request := clientRequest(t)
	request.Stage, request.TableResult = types.StagePostProcess, true
	request.QueryContext = &types.QueryContext{
		OriginalQueries: []*commonpb.PlaceholderValue{expansionQuery("a", "b")},
		Queries:         []*commonpb.PlaceholderValue{expansionQuery("a0", "a1", "b0")},
		QueryOrigins:    [][]int64{{0, 0, 1}},
	}
	outputs, err := client.Execute(context.Background(), request)
	require.ErrorIs(t, err, merr.ErrServiceUnimplemented)
	require.Nil(t, outputs)
}
