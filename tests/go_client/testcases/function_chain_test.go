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

package testcases

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/client/v3/column"
	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/index"
	client "github.com/milvus-io/milvus/client/v3/milvusclient"
	"github.com/milvus-io/milvus/tests/go_client/base"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

// A tiny exact FLAT fixture makes scores and pagination independently predictable.
func createFunctionChainCollection(ctx context.Context, t *testing.T, mc *base.MilvusClient) string {
	name := common.GenRandomString("function_chain_", 8)
	schema := entity.NewSchema().WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("vector").WithDataType(entity.FieldTypeFloatVector).WithDim(2)).
		WithField(entity.NewField().WithName("boost").WithDataType(entity.FieldTypeDouble)).
		WithField(entity.NewField().WithName("meta").WithDataType(entity.FieldTypeJSON)).
		WithField(entity.NewField().WithName("text").WithDataType(entity.FieldTypeVarChar).WithMaxLength(512))
	common.CheckErr(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(name, schema).WithConsistencyLevel(entity.ClStrong)), true)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		require.NoError(t, mc.DropCollection(cleanupCtx, client.NewDropCollectionOption(name)))
	})
	_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(name,
		column.NewColumnInt64("id", []int64{1, 2, 3, 4}),
		column.NewColumnFloatVector("vector", 2, [][]float32{{1, 0}, {2, 0}, {3, 0}, {4, 0}}),
		column.NewColumnDouble("boost", []float64{4, 3, 2, 1}),
		column.NewColumnJSONBytes("meta", [][]byte{[]byte(`{"boost":4}`), []byte(`{"boost":3}`), []byte(`{"boost":2}`), []byte(`{"boost":1}`)}),
		column.NewColumnVarChar("text", []string{"timeout first", "healthy", "TIMEOUT third", "normal"})))
	common.CheckErr(t, err, true)
	flush, err := mc.Flush(ctx, client.NewFlushOption(name))
	common.CheckErr(t, err, true)
	common.CheckErr(t, flush.Await(ctx), true)
	idx, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(name, "vector", index.NewFlatIndex(entity.IP)))
	common.CheckErr(t, err, true)
	common.CheckErr(t, idx.Await(ctx), true)
	load, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(name))
	common.CheckErr(t, err, true)
	common.CheckErr(t, load.Await(ctx), true)
	return name
}

func TestFunctionChainSearchStagesAndJSON(t *testing.T) {
	t.Parallel()
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	name := createFunctionChainCollection(ctx, t, mc)
	for _, stage := range []entity.FunctionChainStage{entity.FunctionChainStageL0Rerank, entity.FunctionChainStageL1Rerank, entity.FunctionChainStageL2Rerank} {
		for _, jsonInput := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s_json_%t", stage, jsonInput), func(t *testing.T) {
				input := entity.Col("boost")
				if jsonInput {
					input = entity.Col(`meta["boost"]`).WithDataType(entity.FieldTypeDouble)
				}
				chain := entity.NewFunctionChain(stage).Map("$score", entity.NewNumCombineExpr("weighted", entity.Col("$score"), input).WithParam("weights", []float64{0, 1}))
				if stage != entity.FunctionChainStageL0Rerank {
					chain.Sort(entity.Col("$score"), true, entity.Col("$id")).Limit(2, 1)
				}
				result, err := mc.Search(ctx, client.NewSearchOption(name, 4, []entity.Vector{entity.FloatVector{1, 0}, entity.FloatVector{1, 0}}).
					WithANNSField("vector").WithFunctionChains(chain).WithOutputFields("text").WithConsistencyLevel(entity.ClStrong))
				common.CheckErr(t, err, true)
				require.Len(t, result, 2)
				wantIDs, wantScores := []int64{1, 2, 3, 4}, []float32{4, 3, 2, 1}
				if stage != entity.FunctionChainStageL0Rerank {
					wantIDs, wantScores = []int64{2, 3}, []float32{3, 2}
				}
				for _, rs := range result {
					require.NoError(t, rs.Err)
					require.Equal(t, wantIDs, rs.IDs.(*column.ColumnInt64).Data())
					require.Equal(t, wantScores, rs.Scores)
					require.Nil(t, rs.GetColumn("boost"), "internally fetched fields must stay hidden")
					require.Nil(t, rs.GetColumn("meta"))
					require.Equal(t, len(wantIDs), rs.GetColumn("text").Len())
				}
			})
		}
	}
	t.Run("composed_stages", func(t *testing.T) {
		l0 := entity.NewFunctionChain(entity.FunctionChainStageL0Rerank).Map("$score", entity.NewNumCombineExpr("weighted", entity.Col("$score"), entity.Col("boost")).WithParam("weights", []float64{0, 1}))
		l1 := entity.NewFunctionChain(entity.FunctionChainStageL1Rerank).Map("$score", entity.NewNumCombineExpr("sum", entity.Col("$score"), entity.Col("boost")))
		l2 := entity.NewFunctionChain(entity.FunctionChainStageL2Rerank).Map("$score", entity.NewRoundDecimalExpr(entity.Col("$score"), 2)).Sort(entity.Col("$score"), true).Limit(2, 0)
		results, err := mc.Search(ctx, client.NewSearchOption(name, 4, []entity.Vector{entity.FloatVector{1, 0}}).WithANNSField("vector").WithFunctionChains(l2, l1, l0))
		common.CheckErr(t, err, true)
		require.NoError(t, results[0].Err)
		require.Equal(t, []int64{1, 2}, results[0].IDs.(*column.ColumnInt64).Data())
		require.Equal(t, []float32{8, 6}, results[0].Scores)
	})
	t.Run("server_error", func(t *testing.T) {
		chain := entity.NewFunctionChain(entity.FunctionChainStageL2Rerank).Map("$score", entity.NewRoundDecimalExpr(entity.Col("absent"), 2))
		_, err := mc.Search(ctx, client.NewSearchOption(name, 4, []entity.Vector{entity.FloatVector{1, 0}}).WithANNSField("vector").WithFunctionChains(chain))
		require.Error(t, err)
		require.Contains(t, err.Error(), "absent")
	})
}

func TestFunctionChainHybridMergeAndNestedStages(t *testing.T) {
	t.Parallel()
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	name := createFunctionChainCollection(ctx, t, mc)
	for _, strategy := range []string{"rrf", "weighted", "max", "sum", "avg"} {
		t.Run(strategy, func(t *testing.T) {
			params := map[string]any{}
			if strategy == "weighted" || strategy == "rrf" {
				params["weights"] = []float64{0.25, 0.75}
			}
			if strategy != "rrf" {
				params["norm_score"] = false
			}
			chain := entity.NewFunctionChain(entity.FunctionChainStageL2Rerank).Merge(strategy, params).Sort(entity.Col("$score"), true, entity.Col("$id")).Limit(2, 1)
			a := client.NewAnnRequest("vector", 4, entity.FloatVector{1, 0}).WithFunctionChains(entity.NewFunctionChain(entity.FunctionChainStageL0Rerank).Map("$score", entity.NewNumCombineExpr("weighted", entity.Col("$score"), entity.Col("boost")).WithParam("weights", []float64{0, 1})))
			b := client.NewAnnRequest("vector", 4, entity.FloatVector{1, 0}).WithFunctionChains(entity.NewFunctionChain(entity.FunctionChainStageL1Rerank).Map("$score", entity.NewNumCombineExpr("weighted", entity.Col("$score"), entity.Col("boost")).WithParam("weights", []float64{0, 1})))
			results, err := mc.HybridSearch(ctx, client.NewHybridSearchOption(name, 4, a, b).WithFunctionChains(chain).WithOutputFields("text"))
			common.CheckErr(t, err, true)
			require.Len(t, results, 1)
			require.NoError(t, results[0].Err)
			require.Equal(t, []int64{2, 3}, results[0].IDs.(*column.ColumnInt64).Data())
			expected := []float64{3, 2}
			if strategy == "sum" {
				expected = []float64{6, 4}
			}
			if strategy == "rrf" {
				expected = []float64{1.0 / 62, 1.0 / 63}
			}
			for i, score := range expected {
				require.InDelta(t, score, results[0].Scores[i], 1e-6)
			}
			require.Equal(t, []string{"healthy", "TIMEOUT third"}, results[0].GetColumn("text").(*column.ColumnVarChar).Data())
		})
	}
}

func TestFunctionChainQueryGrepComputedResults(t *testing.T) {
	t.Parallel()
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	name := createFunctionChainCollection(ctx, t, mc)
	for _, pattern := range []string{"timeout", "no_such_token"} {
		t.Run(pattern, func(t *testing.T) {
			chain := entity.NewFunctionChain(entity.FunctionChainStageQueryScan).WithName("scan").WithOutputColumns("matches").Grep(entity.Col("text"), "-i -e "+pattern, "matches")
			result, err := mc.Query(ctx, client.NewQueryOption(name).WithFilter("id > 0").WithOutputFields("id", "text").WithFunctionChains(chain).WithLimit(4))
			common.CheckErr(t, err, true)
			matches := result.GetColumn("matches")
			require.NotNil(t, matches, "computed column must survive empty results")
			if pattern == "no_such_token" {
				require.Zero(t, result.ResultCount)
				require.Zero(t, matches.Len())
				return
			}
			require.Equal(t, []int64{1, 3}, result.GetColumn("id").(*column.ColumnInt64).Data())
			for i, raw := range matches.(*column.ColumnJSONBytes).Data() {
				var lines []struct {
					Content    string `json:"content"`
					LineNumber int    `json:"line_number"`
					Starts     []int  `json:"starts"`
					Ends       []int  `json:"ends"`
				}
				require.NoError(t, json.Unmarshal(raw, &lines))
				require.Len(t, lines, 1)
				require.Equal(t, []string{"timeout first", "TIMEOUT third"}[i], lines[0].Content)
				require.Equal(t, 1, lines[0].LineNumber)
				require.Equal(t, []int{0}, lines[0].Starts)
				require.Equal(t, []int{7}, lines[0].Ends)
			}
		})
	}
}
