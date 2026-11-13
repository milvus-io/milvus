package testcases

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/client/v3/column"
	"github.com/milvus-io/milvus/client/v3/entity"
	fc "github.com/milvus-io/milvus/client/v3/functionchain"
	"github.com/milvus-io/milvus/client/v3/index"
	client "github.com/milvus-io/milvus/client/v3/milvusclient"
	"github.com/milvus-io/milvus/tests/go_client/base"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

// Fixed signed inputs make expected scores independent of server implementation.
// FLAT and topK == row count prevent candidate recall from hiding chain errors.
func prepareFunctionChainCollection(t *testing.T, ctx context.Context, mc *base.MilvusClient) string {
	t.Helper()
	name := common.GenRandomString("gosdk_chain_", 8)
	schema := entity.NewSchema().WithName(name).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("vector").WithDataType(entity.FieldTypeFloatVector).WithDim(2)).
		WithField(entity.NewField().WithName("a").WithDataType(entity.FieldTypeDouble)).
		WithField(entity.NewField().WithName("b").WithDataType(entity.FieldTypeDouble)).
		WithField(entity.NewField().WithName("text").WithDataType(entity.FieldTypeVarChar).WithMaxLength(256))
	require.NoError(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(name, schema).WithConsistencyLevel(entity.ClStrong)))
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		require.NoError(t, mc.DropCollection(cleanup, client.NewDropCollectionOption(name)))
	})
	_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(name).WithColumns(
		column.NewColumnInt64("id", []int64{1, 2, 3, 4}),
		column.NewColumnFloatVector("vector", 2, [][]float32{{1, 1}, {2, 1}, {3, 1}, {4, 1}}),
		column.NewColumnDouble("a", []float64{8, -4, 0, 1.5}),
		column.NewColumnDouble("b", []float64{2, 6, -2, 0.5}),
		column.NewColumnVarChar("text", []string{"前言\nTIMEOUT timeout", "healthy", "timeout", "no match"}),
	))
	require.NoError(t, err)
	flush, err := mc.Flush(ctx, client.NewFlushOption(name))
	require.NoError(t, err)
	require.NoError(t, flush.Await(ctx))
	task, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(name, "vector", index.NewFlatIndex(entity.IP)))
	require.NoError(t, err)
	require.NoError(t, task.Await(ctx))
	load, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(name))
	require.NoError(t, err)
	require.NoError(t, load.Await(ctx))
	return name
}

func requireFunctionChainHits(t *testing.T, results []client.ResultSet, nq int, ids []int64, scores map[int64]float64) {
	t.Helper()
	require.Len(t, results, nq)
	for _, r := range results {
		require.NoError(t, r.Err)
		require.Equal(t, len(ids), r.ResultCount)
		require.Len(t, r.Scores, len(ids))
		require.NotNil(t, r.IDs)
		require.Len(t, r.Fields, 1) // temporary/input columns must not leak into projection
		require.Equal(t, "text", r.Fields[0].Name())
		for i, id := range ids {
			got, err := r.IDs.GetAsInt64(i)
			require.NoError(t, err)
			require.Equal(t, id, got)
			require.InDelta(t, scores[id], r.Scores[i], 1e-5)
		}
	}
}

func TestFunctionChainSearchIntegration(t *testing.T) {
	ctx := hp.CreateContext(t, 180*time.Second)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	name := prepareFunctionChainCollection(t, ctx, mc)
	search := func(c *fc.FunctionChain) ([]client.ResultSet, error) {
		return mc.Search(ctx, client.NewSearchOption(name, 4, []entity.Vector{entity.FloatVector{1, 0}, entity.FloatVector{0, 1}}).WithANNSField("vector").WithOutputFields("text").WithFunctionChains(c))
	}
	modes := map[string][]float64{"sum": {10, 2, -2, 2}, "multiply": {16, -24, 0, 0.75}, "min": {2, -4, -2, 0.5}, "max": {8, 6, 0, 1.5}, "avg": {5, 1, -1, 1}, "weighted": {15, -11, 1, 2.75}}
	for _, stage := range []fc.Stage{fc.L0Rerank, fc.L1Rerank, fc.L2Rerank} {
		for mode, values := range modes {
			t.Run(fmt.Sprintf("%s/%s", stage, mode), func(t *testing.T) {
				expr := fc.NumCombine(fc.Col("a"), fc.Col("b")).WithParam("mode", mode)
				if mode == "weighted" {
					expr.WithParam("weights", []float64{2, -0.5})
				}
				c := fc.NewFunctionChain(stage).Map("$score", expr)
				// L0 is a per-row stage; sorting belongs to L1/L2.
				if stage != fc.L0Rerank {
					c.SortBy("$score", true, "$id")
				}
				result, err := search(c)
				require.NoError(t, err)
				ids := []int64{1, 2, 3, 4}
				sort.Slice(ids, func(i, j int) bool {
					a, b := values[ids[i]-1], values[ids[j]-1]
					if a == b {
						return ids[i] < ids[j]
					}
					return a > b
				})
				scores := map[int64]float64{}
				for i, v := range values {
					scores[int64(i+1)] = v
				}
				// L0 ties have no explicit tie-break contract; verify scores by ID.
				if stage == fc.L0Rerank {
					require.Len(t, result, 2)
					for _, r := range result {
						require.NoError(t, r.Err)
						require.Equal(t, 4, r.ResultCount)
						require.Len(t, r.Scores, 4)
						gotIDs := []int64{}
						for i := range r.Scores {
							id, err := r.IDs.GetAsInt64(i)
							require.NoError(t, err)
							gotIDs = append(gotIDs, id)
							require.InDelta(t, scores[id], r.Scores[i], 1e-5)
							if i > 0 {
								require.GreaterOrEqual(t, r.Scores[i-1], r.Scores[i])
							}
						}
						require.ElementsMatch(t, ids, gotIDs)
					}
				} else {
					requireFunctionChainHits(t, result, 2, ids, scores)
				}
			})
		}
	}
	t.Run("decay_round_sort_limit", func(t *testing.T) {
		c := fc.NewFunctionChain(fc.L2Rerank).Map("freshness", fc.Decay(fc.Col("id"), "linear", 1, 2)).Map("$score", fc.RoundDecimal(fc.Col("freshness"), 3)).Sort("$score").Limit(2, 1)
		r, err := search(c)
		require.NoError(t, err)
		requireFunctionChainHits(t, r, 2, []int64{2, 3}, map[int64]float64{2: 0.75, 3: 0.5})
	})
	t.Run("server_rejection_and_recovery", func(t *testing.T) {
		// Generic expressions deliberately pass SDK validation and reach the server.
		_, err := search(fc.NewFunctionChain(fc.L2Rerank).Map("$score", fc.NewExpr("gosdk_nonexistent_function", fc.Col("a"))))
		require.Error(t, err)
		require.Contains(t, err.Error(), "gosdk_nonexistent_function")
		r, err := search(fc.NewFunctionChain(fc.L2Rerank).Map("$score", fc.NumCombine(fc.Col("a"), fc.Col("b"))).SortBy("$score", true, "$id"))
		require.NoError(t, err)
		requireFunctionChainHits(t, r, 2, []int64{1, 2, 4, 3}, map[int64]float64{1: 10, 2: 2, 3: -2, 4: 2})
	})
}

func TestFunctionChainHybridIntegration(t *testing.T) {
	ctx := hp.CreateContext(t, 180*time.Second)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	name := prepareFunctionChainCollection(t, ctx, mc)
	requests := func() []*client.AnnRequest {
		return []*client.AnnRequest{client.NewAnnRequest("vector", 4, entity.FloatVector{1, 0}), client.NewAnnRequest("vector", 4, entity.FloatVector{-1, 0})}
	}
	t.Run("rrf_matches_legacy", func(t *testing.T) {
		legacy, err := mc.HybridSearch(ctx, client.NewHybridSearchOption(name, 4, requests()...).WithReranker(client.NewRRFReranker()).WithOutputFields("text"))
		require.NoError(t, err)
		result, err := mc.HybridSearch(ctx, client.NewHybridSearchOption(name, 4, requests()...).WithFunctionChains(fc.NewFunctionChain(fc.L2Rerank).Merge("rrf")).WithOutputFields("text"))
		require.NoError(t, err)
		require.Len(t, legacy, 1)
		require.Len(t, result, 1)
		require.Equal(t, 4, result[0].ResultCount)
		scores := map[int64]float32{}
		for i, s := range legacy[0].Scores {
			id, err := legacy[0].IDs.GetAsInt64(i)
			require.NoError(t, err)
			scores[id] = s
		}
		seen := map[int64]bool{}
		for i, s := range result[0].Scores {
			id, err := result[0].IDs.GetAsInt64(i)
			require.NoError(t, err)
			require.Contains(t, scores, id)
			require.False(t, seen[id])
			seen[id] = true
			require.InDelta(t, scores[id], s, 1e-6)
		}
		require.Len(t, seen, 4)
	})
	t.Run("subsearch_chains_and_weighted_merge", func(t *testing.T) {
		reqs := requests()
		for _, r := range reqs {
			r.WithFunctionChains(fc.NewFunctionChain(fc.L0Rerank).Map("$score", fc.NumCombine(fc.Col("a"), fc.Col("b"))))
		}
		norm := false
		c := fc.NewFunctionChain(fc.L2Rerank).Merge("weighted", fc.MergeOptions{Weights: []float64{0.25, 0.75}, NormScore: &norm}).SortBy("$score", true, "$id").Limit(3)
		result, err := mc.HybridSearch(ctx, client.NewHybridSearchOption(name, 3, reqs...).WithOutputFields("text").WithFunctionChains(c))
		require.NoError(t, err)
		require.Len(t, result, 1)
		requireFunctionChainHits(t, result, 1, []int64{1, 2, 4}, map[int64]float64{1: 10, 2: 2, 4: 2})
	})
}

func TestFunctionChainGrepIntegration(t *testing.T) {
	ctx := hp.CreateContext(t, 180*time.Second)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	name := prepareFunctionChainCollection(t, ctx, mc)
	t.Run("utf8_offsets_and_multiple_matches", func(t *testing.T) {
		r, err := mc.Grep(ctx, client.NewGrepOption(name, "text", "-i -e timeout", "details").WithFilter("id == 1").WithOutputFields("id"))
		require.NoError(t, err)
		require.Equal(t, 1, r.ResultCount)
		require.NotNil(t, r.GetColumn("details"))
		raw, err := r.GetColumn("details").Get(0)
		require.NoError(t, err)
		data, ok := raw.([]byte)
		require.True(t, ok)
		var lines []struct {
			Content         string `json:"content"`
			LineNumber      int    `json:"line_number"`
			LineStartOffset int    `json:"line_start_offset"`
			Starts          []int  `json:"starts"`
			Ends            []int  `json:"ends"`
		}
		require.NoError(t, json.Unmarshal(data, &lines))
		require.Len(t, lines, 1)
		require.Equal(t, "TIMEOUT timeout", lines[0].Content)
		require.Equal(t, 2, lines[0].LineNumber)
		require.Equal(t, 7, lines[0].LineStartOffset)
		require.Equal(t, []int{0, 8}, lines[0].Starts)
		require.Equal(t, []int{7, 15}, lines[0].Ends)
	})
	t.Run("filter_only_and_pagination", func(t *testing.T) {
		c := fc.NewFunctionChain(fc.QueryScan).Grep("text", "-i -e timeout")
		query := func(offset int) (client.ResultSet, error) {
			return mc.Query(ctx, client.NewQueryOption(name).WithOutputFields("id").WithLimit(1).WithOffset(offset).WithFunctionChains(c))
		}
		ids := []int64{}
		for offset := 0; offset < 2; offset++ {
			r, err := query(offset)
			require.NoError(t, err)
			require.Equal(t, 1, r.ResultCount)
			require.Nil(t, r.GetColumn("matches"))
			id, err := r.GetColumn("id").GetAsInt64(0)
			require.NoError(t, err)
			ids = append(ids, id)
		}
		require.ElementsMatch(t, []int64{1, 3}, ids)
		r, err := query(2)
		require.NoError(t, err)
		require.Zero(t, r.ResultCount)
	})
	t.Run("no_matches", func(t *testing.T) {
		r, err := mc.Grep(ctx, client.NewGrepOption(name, "text", "-e definitely_absent").WithOutputFields("id"))
		require.NoError(t, err)
		require.Zero(t, r.ResultCount)
	})
}
