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
	"fmt"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/index"
	client "github.com/milvus-io/milvus/client/v3/milvusclient"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

// specialized reads verify application-owned state after each mutation.
func TestRLSSchemaSearchVariants(t *testing.T) {
	for _, vector := range []string{"float32", "binary", "float16", "bfloat16", "int8", "sparse"} {
		for _, varchar := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/varchar_%v", vector, varchar), func(t *testing.T) {
				t.Parallel()
				ctx := hp.CreateContext(t, 5*time.Minute)
				mc := hp.CreateDefaultMilvusClient(ctx, t).Client
				tc := rlsSchemaCase{vector: vector, varchar: varchar}
				name := prepareRLSSchema(ctx, t, mc, tc)
				all := map[any]rlsSchemaRow{}
				var fields []string
				for _, c := range rlsSchemaColumns(t, tc) {
					fields = append(fields, c.Name())
				}
				qv, _, _, _ := rlsSchemaVector(t, vector, 1)
				for i := 1; i <= 8; i++ {
					owner := "alice"
					if i%2 == 0 {
						owner = "bob"
					}
					r := rlsSchemaData(t, tc, i, 0, owner)
					_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(name, rlsSchemaWriteColumns(t, tc, r)...).WithRLSPrincipal(owner))
					require.NoError(t, err)
					all[r["id"]] = r
				}
				for _, stage := range []string{"insert", "full_upsert", "partial_upsert", "delete", "flushed"} {
					t.Run(stage, func(t *testing.T) {
						changed := rlsSchemaData(t, tc, 1, 1, "alice")
						switch stage {
						case "full_upsert":
							_, err := mc.Upsert(ctx, client.NewColumnBasedInsertOption(name, rlsSchemaWriteColumns(t, tc, changed)...).WithRLSPrincipal("alice"))
							require.NoError(t, err)
							all[changed["id"]] = changed
						case "partial_upsert":
							patch := rlsSchemaRow{"id": changed["id"], "f64": float64(99.125)}
							_, err := mc.Upsert(ctx, client.NewColumnBasedInsertOption(name, rlsSchemaWriteColumns(t, tc, patch)...).WithPartialUpdate(true).WithRLSPrincipal("alice"))
							require.NoError(t, err)
							all[changed["id"]]["f64"] = patch["f64"]
						case "delete":
							_, err := mc.Delete(ctx, client.NewDeleteOption(name).WithExpr("id == {target}").WithTemplateParam("target", changed["id"]).WithRLSPrincipal("alice"))
							require.NoError(t, err)
							delete(all, changed["id"])
						case "flushed":
							rlsFlush(ctx, t, mc, name)
						}
						for _, principal := range []string{"alice", "bob", "nobody", ""} {
							t.Run("principal_"+principal, func(t *testing.T) {
								want := maps.Clone(all)
								maps.DeleteFunc(want, func(_ any, r rlsSchemaRow) bool { return principal != "" && r["owner"] != principal })
								for _, mode := range []string{"range", "group", "aggregation", "ordered_query", "template_query"} {
									t.Run(mode, func(t *testing.T) {
										if mode == "ordered_query" || mode == "template_query" {
											o := client.NewQueryOption(name).WithFilter("i32 >= {minimum}").WithTemplateParam("minimum", int64(100000)).WithOutputFields(fields...).WithRLSPrincipal(principal).WithSkipRLS(principal == "").WithConsistencyLevel(entity.ClStrong)
											if mode == "ordered_query" {
												o.WithOrderByFields("f64:desc").WithLimit(100)
											}
											r, err := mc.Query(ctx, o)
											require.NoError(t, err)
											ids := checkRLSSchemaRows(t, r, want, fields)
											require.ElementsMatch(t, slices.Collect(maps.Keys(want)), ids)
											if mode == "ordered_query" {
												for i := 1; i < len(ids); i++ {
													require.Greater(t, want[ids[i-1]]["f64"].(float64), want[ids[i]]["f64"].(float64))
												}
											}
											return
										}
										o := client.NewSearchOption(name, 100, []entity.Vector{qv}).WithANNSField("vector").WithOutputFields(fields...).WithRLSPrincipal(principal).WithSkipRLS(principal == "").WithConsistencyLevel(entity.ClStrong)
										rangeWant := maps.Clone(want)
										switch mode {
										case "range":
											params := index.NewCustomAnnParam()
											threshold := float32(9)
											if vector == "binary" {
												threshold = 2
											}
											if vector == "sparse" {
												params.WithRadius(8.5)
												params.WithRangeFilter(100)
												maps.DeleteFunc(rangeWant, func(_ any, r rlsSchemaRow) bool {
													d := rlsSchemaDistance(vector, r["vector"])
													return d <= 8.5 || d > 100
												})
											} else {
												params.WithRadius(float64(threshold))
												params.WithRangeFilter(0)
												maps.DeleteFunc(rangeWant, func(_ any, r rlsSchemaRow) bool { return rlsSchemaDistance(vector, r["vector"]) >= threshold })
											}
											o.WithAnnParam(params)
										case "group":
											o.WithGroupByField("owner")
										case "aggregation":
											o.WithOutputFields("owner", "f64").WithSearchAggregation(client.NewSearchAggregation([]string{"owner"}, 100).WithMetric("n", "count", "*").WithMetric("total", "sum", "f64").WithTopHits(client.NewTopHits(100)))
										}
										rs, err := mc.Search(ctx, o)
										if mode == "group" && vector == "binary" {
											require.ErrorContains(t, err, "not support search_group_by operation based on binary vector column")
											return // Binary vectors reject legacy group-by search.
										}
										require.NoError(t, err)
										require.Len(t, rs, 1)
										r := rs[0]
										require.NoError(t, r.Err)
										if mode == "range" {
											require.ElementsMatch(t, slices.Collect(maps.Keys(rangeWant)), checkRLSSchemaRows(t, r, rangeWant, fields))
											return
										}
										owners := map[string][]any{}
										for id, row := range want {
											owner := row["owner"].(string)
											owners[owner] = append(owners[owner], id)
										}
										if mode == "group" {
											ids := checkRLSSchemaRows(t, r, want, fields)
											require.Len(t, ids, len(owners))
											seen := map[string]bool{}
											for i, id := range ids {
												owner := want[id]["owner"].(string)
												require.False(t, seen[owner])
												seen[owner] = true
												g, e := r.GroupByValue.Get(i)
												require.NoError(t, e)
												require.Equal(t, owner, g)
											}
											return
										}
										require.Len(t, r.AggregationBuckets, len(owners))
										var returned []any
										for _, b := range r.AggregationBuckets {
											require.Len(t, b.Key, 1)
											owner := b.Key[0].Value.(string)
											require.Contains(t, owners, owner)
											require.EqualValues(t, len(owners[owner]), b.Count)
											require.EqualValues(t, b.Count, b.Metrics["n"])
											var sum float64
											for _, id := range owners[owner] {
												sum += want[id]["f64"].(float64)
											}
											require.InDelta(t, sum, b.Metrics["total"], 0.001)
											var ids []any
											for _, h := range b.Hits {
												require.Contains(t, want, h.PK)
												require.Equal(t, owner, h.Fields["owner"])
												require.Equal(t, want[h.PK]["f64"], h.Fields["f64"])
												ids = append(ids, h.PK)
											}
											require.ElementsMatch(t, owners[owner], ids)
											returned = append(returned, ids...)
										}
										require.ElementsMatch(t, slices.Collect(maps.Keys(want)), returned)
									})
								}
							})
						}
					})
				}
			})
		}
	}
}
