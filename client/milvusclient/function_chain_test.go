// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package milvusclient

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/client/v3/column"
	"github.com/milvus-io/milvus/client/v3/entity"
	fc "github.com/milvus-io/milvus/client/v3/functionchain"
)

func TestFunctionChainSearch(t *testing.T) {
	c := fc.NewFunctionChain(fc.L2Rerank).Map("$score", fc.RoundDecimal(fc.Col("$score"), 3)).Limit(10)
	opt := NewSearchOption("docs", 10, nil).WithFunctionChains(c)
	req, err := opt.Request()
	require.NoError(t, err)
	require.Len(t, req.FunctionChains, 1)
	require.Equal(t, fc.L2Rerank, req.FunctionChains[0].Stage)
	_, err = opt.WithFunctionScore(entity.NewFunctionScore()).Request()
	require.Error(t, err)
	_, err = NewSearchOption("docs", 10, nil).WithFunctionChains(fc.NewFunctionChain(fc.Unspecified)).Request()
	require.Error(t, err)
	_, err = NewSearchOption("docs", 10, nil).WithFunctionChains(nil).Request()
	require.Error(t, err)
	req, err = NewSearchByIDsOption("docs", 10, column.NewColumnInt64("id", []int64{1})).WithFunctionChains(c).Request()
	require.NoError(t, err)
	require.Len(t, req.FunctionChains, 1)
	req, err = NewSearchOption("docs", 10, nil).WithFunctionChains(c).WithFunctionChains().Request()
	require.NoError(t, err)
	require.Empty(t, req.FunctionChains)
}

func TestFunctionChainHybrid(t *testing.T) {
	chain := func(stage fc.Stage) *fc.FunctionChain { return fc.NewFunctionChain(stage).Limit(10) }
	req, err := NewHybridSearchOption("docs", 10, NewAnnRequest("vec", 10).WithFunctionChains(chain(fc.L0Rerank))).WithFunctionChains(fc.NewFunctionChain(fc.L2Rerank).Merge("rrf")).HybridRequest()
	require.NoError(t, err)
	require.Len(t, req.FunctionChains, 1)
	require.Len(t, req.Requests[0].FunctionChains, 1)
	_, err = NewHybridSearchOption("docs", 10).WithFunctionChains(chain(fc.L2Rerank)).WithReranker(NewRRFReranker()).HybridRequest()
	require.Error(t, err)
	_, err = NewHybridSearchOption("docs", 10).WithFunctionChains(chain(fc.L2Rerank)).WithFunctionScore(entity.NewFunctionScore()).HybridRequest()
	require.Error(t, err)
	// Nil chains remain invalid even though stage/count rules belong to the server.
	_, err = NewHybridSearchOption("docs", 10).WithFunctionChains(nil).HybridRequest()
	require.Error(t, err)
	for _, chains := range [][]*fc.FunctionChain{{chain(fc.L0Rerank), chain(fc.L0Rerank)}, {fc.NewFunctionChain(fc.L0Rerank)}} {
		_, err := NewHybridSearchOption("docs", 10, NewAnnRequest("vec", 10).WithFunctionChains(chains...)).HybridRequest()
		require.Error(t, err)
	}
	_, err = NewHybridSearchOption("docs", 10, NewAnnRequest("vec", 10).WithFunctionChains(chain(fc.L0Rerank))).WithFunctionScore(entity.NewFunctionScore()).HybridRequest()
	require.ErrorContains(t, err, "sub-search")
	// A legacy merge ranker may still combine sub-searches using their own chains.
	_, err = NewHybridSearchOption("docs", 10, NewAnnRequest("vec", 10).WithFunctionChains(chain(fc.L0Rerank))).WithReranker(NewRRFReranker()).HybridRequest()
	require.NoError(t, err)
}

func TestFunctionChainQuery(t *testing.T) {
	c := fc.NewFunctionChain(fc.QueryScan).Grep("text", "-i timeout")
	req, err := NewQueryOption("docs").WithFunctionChains(c).WithLimit(20).WithOffset(2).Request()
	require.NoError(t, err)
	require.Len(t, req.FunctionChains, 1)
	require.Empty(t, req.FunctionChains[0].OutputColumns)
	for _, chains := range [][]*fc.FunctionChain{{c, c}, {fc.NewFunctionChain(fc.QueryScan)}, {fc.NewFunctionChain(fc.L2Rerank).Limit(1)}, {nil}} {
		_, err := NewQueryOption("docs").WithFunctionChains(chains...).Request()
		require.Error(t, err)
	}
	req, err = NewGrepOption("docs", "text", "-i -e 'timeout'", "details").WithFilter("id > 0").WithPartitions("p").WithOutputFields("id").Request()
	require.NoError(t, err)
	require.Equal(t, "id > 0", req.Expr)
	require.Equal(t, []string{"p"}, req.PartitionNames)
	require.Equal(t, []string{"details"}, req.FunctionChains[0].OutputColumns)
	require.Equal(t, "-i -e 'timeout'", req.FunctionChains[0].Ops[0].Params["arguments"].GetStringValue())
	require.Contains(t, req.QueryParams, &commonpb.KeyValuePair{Key: "limit", Value: "100"})
	_, err = NewGrepOption("docs", "text", "x", "a", "b").Request()
	require.Error(t, err)
}

func TestFunctionChainQueryOutput(t *testing.T) {
	sch := entity.NewSchema().WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true))
	matches := column.NewColumnJSONBytes("matches", [][]byte{[]byte(`[{"content":"timeout","line_number":1,"starts":[0],"ends":[7]}]`)})
	client := &Client{}
	result, err := client.parseQueryResult(sch, []string{"id"}, &milvuspb.QueryResults{FieldsData: []*schemapb.FieldData{column.NewColumnInt64("id", []int64{1}).FieldData(), matches.FieldData()}})
	require.NoError(t, err)
	require.Equal(t, 1, result.ResultCount)
	require.NotNil(t, result.GetColumn("matches"))
	got, err := result.GetColumn("matches").Get(0)
	require.NoError(t, err)
	require.Equal(t, matches.Data()[0], got)
}

func (s *ReadSuite) TestFunctionChainGrepRPC() {
	s.setupCache("chain_docs", s.schema)
	s.mock.EXPECT().Query(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *milvuspb.QueryRequest) (*milvuspb.QueryResults, error) {
		s.Require().Len(req.FunctionChains, 1)
		s.Equal(fc.QueryScan, req.FunctionChains[0].Stage)
		s.Equal("-i timeout", req.FunctionChains[0].Ops[0].Params["arguments"].GetStringValue())
		return &milvuspb.QueryResults{FieldsData: []*schemapb.FieldData{
			column.NewColumnInt64("ID", []int64{7}).FieldData(),
			column.NewColumnJSONBytes("matches", [][]byte{[]byte(`[{"content":"timeout"}]`)}).FieldData(),
		}}, nil
	}).Once()
	result, err := s.client.Grep(context.Background(), NewGrepOption("chain_docs", "text", "-i timeout").WithOutputFields("ID"))
	s.Require().NoError(err)
	s.Equal(1, result.ResultCount)
	s.NotNil(result.GetColumn("matches"))
}

// These cases check request serialization, not whether a particular server
// release supports executing the supplied top-level chain combination.
func TestFunctionChainHybridSerializesCombinations(t *testing.T) {
	for _, tc := range []struct {
		name   string
		stages []fc.Stage
	}{
		{"none", nil},
		{"l2", []fc.Stage{fc.L2Rerank}},
		{"l0", []fc.Stage{fc.L0Rerank}},
		{"preprocess only", []fc.Stage{fc.PreProcess}},
		{"multiple l2", []fc.Stage{fc.L2Rerank, fc.L2Rerank}},
		{"preprocess rerank postprocess", []fc.Stage{fc.PreProcess, fc.L2Rerank, fc.PostProcess}},
		{"rerank postprocess", []fc.Stage{fc.L2Rerank, fc.PostProcess}},
		{"unspecified", []fc.Stage{fc.Unspecified}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			chains := make([]*fc.FunctionChain, len(tc.stages))
			for i, stage := range tc.stages {
				chains[i] = fc.NewFunctionChain(stage).Limit(i + 1)
			}
			req, err := NewHybridSearchOption("docs", 10).WithFunctionChains(chains...).HybridRequest()
			require.NoError(t, err)
			require.Len(t, req.FunctionChains, len(chains))
			for i, stage := range tc.stages {
				require.Equal(t, stage, req.FunctionChains[i].GetStage())
				require.Len(t, req.FunctionChains[i].GetOps(), 1)
				require.Equal(t, "limit", req.FunctionChains[i].Ops[0].GetOp())
				require.EqualValues(t, i+1, req.FunctionChains[i].Ops[0].Params["limit"].GetInt64Value())
			}
		})
	}
}
