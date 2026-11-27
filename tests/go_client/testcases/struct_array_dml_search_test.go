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
	"fmt"
	"slices"
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

func structArrayModelCreate(t *testing.T, ctx context.Context, mc *base.MilvusClient, varchar bool) (string, *entity.StructSchema) {
	t.Helper()
	name := common.GenRandomString("struct_contract", 8)
	inner := entity.NewStructSchema().
		WithField(entity.NewField().WithName("value").WithDataType(entity.FieldTypeInt64)).
		WithField(entity.NewField().WithName("tag").WithDataType(entity.FieldTypeVarChar).WithMaxLength(128)).
		WithField(entity.NewField().WithName("emb").WithDataType(entity.FieldTypeFloatVector).WithDim(4))
	pk := entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	if varchar {
		pk.WithDataType(entity.FieldTypeVarChar).WithMaxLength(128)
	}
	schema := entity.NewSchema().WithName(name).WithField(pk).
		WithField(entity.NewField().WithName("v").WithDataType(entity.FieldTypeFloatVector).WithDim(4)).
		WithField(entity.NewField().WithName("rev").WithDataType(entity.FieldTypeInt64)).
		WithField(entity.NewField().WithName("clips").WithDataType(entity.FieldTypeArray).
			WithElementType(entity.FieldTypeStruct).WithMaxCapacity(8).WithNullable(true).WithStructSchema(inner))
	common.CheckErr(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(name, schema).WithConsistencyLevel(entity.ClStrong)), true)
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		require.NoError(t, mc.DropCollection(cleanup, client.NewDropCollectionOption(name)))
	})
	return name, inner
}

func structArrayModelPK(varchar bool, ids []int64) column.Column {
	if !varchar {
		return column.NewColumnInt64("id", ids)
	}
	values := make([]string, len(ids))
	for i, id := range ids {
		values[i] = fmt.Sprintf("主键_%03d", id)
	}
	return column.NewColumnVarChar("id", values)
}

func structArrayModelRow(id, rev int64, size int) map[string]any {
	values, tags, vectors := make([]int64, size), make([]string, size), make([][]float32, size)
	for j := range size {
		values[j] = id*10 + int64(j)
		tags[j] = fmt.Sprintf("中文_%d_%d_%d", id, rev, j)
		vectors[j] = []float32{float32(id + 1), float32(j + 1), float32(rev + 1), 1}
	}
	return map[string]any{"value": values, "tag": tags, "emb": vectors}
}

func structArrayModelLoad(t *testing.T, ctx context.Context, mc *base.MilvusClient, name string, metric entity.MetricType) {
	t.Helper()
	for _, field := range []string{"v", "clips[emb]"} {
		task, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(name, field, index.NewFlatIndex(metric)))
		common.CheckErr(t, err, true)
		common.CheckErr(t, task.Await(ctx), true)
	}
	task, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(name))
	common.CheckErr(t, err, true)
	common.CheckErr(t, task.Await(ctx), true)
}

func TestStructArrayMutationOptionsRoundTrip(t *testing.T) {
	for _, varchar := range []bool{false, true} {
		for _, allNull := range []bool{false, true} {
			t.Run(fmt.Sprintf("varchar_%t_all_null_%t", varchar, allNull), func(t *testing.T) {
				ctx := hp.CreateContext(t, 5*time.Minute)
				mc := hp.CreateDefaultMilvusClient(ctx, t)
				name, inner := structArrayModelCreate(t, ctx, mc, varchar)
				expected := map[int64]map[string]any{}
				revisions := map[int64]int64{}
				write := func(ids []int64, rev int64, kind string) {
					rows, vectors, revs := make([]map[string]any, len(ids)), make([][]float32, len(ids)), make([]int64, len(ids))
					for i, id := range ids {
						vectors[i], revs[i] = []float32{float32(id), 1, 2, 3}, rev
						if kind != "null" && !(kind == "insert" && (allNull || id%3 == 0)) {
							rows[i] = structArrayModelRow(id, rev, int(id%4))
						}
					}
					opt := client.NewColumnBasedInsertOption(name).WithColumns(structArrayModelPK(varchar, ids)).
						WithFloatVectorColumn("v", 4, vectors).WithInt64Column("rev", revs)
					if kind != "omit" {
						opt.WithStructArrayColumn("clips", inner, rows)
					}
					if kind == "insert" {
						_, err := mc.Insert(ctx, opt)
						common.CheckErr(t, err, true)
					} else {
						_, err := mc.Upsert(ctx, opt.WithPartialUpdate(kind != "full"))
						common.CheckErr(t, err, true)
					}
					for i, id := range ids {
						revisions[id] = rev
						if kind != "omit" {
							expected[id] = rows[i]
						} else if _, ok := expected[id]; !ok {
							expected[id] = nil
						}
					}
				}
				verify := func() {
					result, err := mc.Query(ctx, client.NewQueryOption(name).WithFilter("rev >= 0").WithOutputFields("*").WithConsistencyLevel(entity.ClStrong))
					common.CheckErr(t, err, true)
					require.Equal(t, len(expected), result.ResultCount)
					seen := map[any]bool{}
					for i := range result.ResultCount {
						pk, err := result.GetColumn("id").Get(i)
						require.NoError(t, err)
						require.False(t, seen[pk])
						seen[pk] = true
						var id int64
						if varchar {
							_, err = fmt.Sscanf(pk.(string), "主键_%03d", &id)
							require.NoError(t, err)
						} else {
							id = pk.(int64)
						}
						want, ok := expected[id]
						require.True(t, ok)
						rev, err := result.GetColumn("rev").GetAsInt64(i)
						require.NoError(t, err)
						require.Equal(t, revisions[id], rev)
						got, err := result.GetColumn("clips").Get(i)
						require.NoError(t, err)
						null, err := result.GetColumn("clips").IsNull(i)
						require.NoError(t, err)
						require.Equal(t, want == nil, null)
						if want == nil {
							require.Nil(t, got)
							continue
						}
						value := got.(map[string]any)
						require.True(t, slices.Equal(want["value"].([]int64), value["value"].([]int64)))
						require.True(t, slices.Equal(want["tag"].([]string), value["tag"].([]string)))
						embs := value["emb"].([]entity.FloatVector)
						expectedEmb := want["emb"].([][]float32)
						require.Len(t, embs, len(expectedEmb))
						for j, vector := range embs {
							require.Equal(t, expectedEmb[j], []float32(vector))
						}
					}
				}
				write([]int64{0, 1, 2, 3, 4, 5}, 0, "insert")
				structArrayModelLoad(t, ctx, mc, name, entity.L2)
				verify()
				write([]int64{0, 1, 6}, 1, "omit")
				verify()
				write([]int64{0, 2, 7}, 2, "null")
				verify()
				write([]int64{1, 3, 7}, 3, "full")
				verify()
				_, err := mc.Delete(ctx, client.NewDeleteOption(name).WithExpr(fmt.Sprintf("rev == %d && %s", revisions[4], func() string {
					if varchar {
						return `id == "主键_004"`
					}
					return "id == 4"
				}())))
				common.CheckErr(t, err, true)
				delete(expected, 4)
				delete(revisions, 4)
				verify()
				write([]int64{4}, 3, "full")
				verify()
			})
		}
	}
}

// Exercise public result decoding and slicing with two independent query vectors.
func TestStructArraySearchResultOffsetsRoundTrip(t *testing.T) {
	ctx := hp.CreateContext(t, time.Minute*3)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	name, inner := structArrayModelCreate(t, ctx, mc, false)
	rows := []map[string]any{
		{"value": []int64{10, 11}, "tag": []string{"first", "second"}, "emb": [][]float32{{1, 0, 0, 0}, {4, 0, 0, 0}}},
		{"value": []int64{20}, "tag": []string{"third"}, "emb": [][]float32{{9, 0, 0, 0}}},
	}
	_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(name).
		WithInt64Column("id", []int64{10, 20}).WithInt64Column("rev", []int64{0, 0}).
		WithFloatVectorColumn("v", 4, [][]float32{{1, 0, 0, 0}, {9, 0, 0, 0}}).
		WithStructArrayColumn("clips", inner, rows))
	common.CheckErr(t, err, true)
	structArrayModelLoad(t, ctx, mc, name, entity.L2)
	results, err := mc.Search(ctx, client.NewSearchOption(name, 2, []entity.Vector{
		entity.FloatVector([]float32{1, 0, 0, 0}), entity.FloatVector([]float32{9, 0, 0, 0}),
	}).WithANNSField("clips[emb]").WithSearchParam("metric_type", "L2").
		WithFilter("element_filter(clips, $[value] >= 0)").WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	require.Len(t, results, 2)
	for q, result := range results {
		require.NoError(t, result.Err)
		require.Equal(t, 2, result.ResultCount)
		require.Equal(t, []int64{0, 1}, result.ElementOffsets)
		require.Equal(t, [][]float32{{0, 9}, {0, 25}}[q], result.Scores)
		for i, expected := range [][]int64{{10, 10}, {20, 10}}[q] {
			id, err := result.IDs.GetAsInt64(i)
			require.NoError(t, err)
			require.Equal(t, expected, id)
		}
		sliced := result.Slice(1, 2)
		require.Equal(t, 1, sliced.ResultCount)
		require.Equal(t, []int64{1}, sliced.ElementOffsets)
		require.Equal(t, result.Scores[1:], sliced.Scores)
		id, err := sliced.IDs.GetAsInt64(0)
		require.NoError(t, err)
		require.Equal(t, int64(10), id)
	}
}
