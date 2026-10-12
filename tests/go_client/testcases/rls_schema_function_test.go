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
	"io"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/client/v3/column"
	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/index"
	client "github.com/milvus-io/milvus/client/v3/milvusclient"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

// generated sparse vectors must follow authorized source-text updates.
func TestRLSSchemaBM25(t *testing.T) {
	for _, mode := range []string{"column", "row"} {
		t.Run(mode, func(t *testing.T) {
			ctx := hp.CreateContext(t, 8*time.Minute)
			mc := hp.CreateDefaultMilvusClient(ctx, t).Client
			name := common.GenRandomString("rls_bm25_", 8)
			schema := entity.NewSchema().WithName(name).WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).WithField(entity.NewField().WithName("owner").WithDataType(entity.FieldTypeVarChar).WithMaxLength(256)).WithField(entity.NewField().WithName("text").WithDataType(entity.FieldTypeVarChar).WithMaxLength(1024).WithEnableAnalyzer(true).WithEnableMatch(true)).WithField(entity.NewField().WithName("vector").WithDataType(entity.FieldTypeSparseVector)).WithFunction(hp.TNewBM25Function("text", "vector"))
			require.NoError(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(name, schema).WithProperty("rls.enabled", true).WithConsistencyLevel(entity.ClStrong)))
			t.Cleanup(func() {
				cleanup, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()
				require.NoError(t, mc.DropCollection(cleanup, client.NewDropCollectionOption(name)))
			})
			createRLSPolicy(ctx, t, mc, name, "owner", "owner == $current_principal", "owner == $current_principal", rlsActions...)
			task, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(name, "vector", index.NewSparseInvertedIndex(entity.BM25, 0)))
			require.NoError(t, err)
			require.NoError(t, task.Await(ctx))
			load, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(name))
			require.NoError(t, err)
			require.NoError(t, load.Await(ctx))
			all := map[any]rlsSchemaRow{}
			write := func(id int64, owner, text string, upsert, partial bool) error {
				r := map[string]any{"id": id, "owner": owner, "text": text}
				cols := []column.Column{column.NewColumnInt64("id", []int64{id}), column.NewColumnVarChar("text", []string{text})}
				if partial {
					delete(r, "owner")
				} else {
					cols = append(cols, column.NewColumnVarChar("owner", []string{owner}))
				}
				if mode == "row" {
					o := client.NewRowBasedInsertOption(name, r).WithRLSPrincipal(owner).WithPartialUpdate(partial)
					if upsert {
						_, e := mc.Upsert(ctx, o)
						return e
					}
					_, e := mc.Insert(ctx, o)
					return e
				}
				o := client.NewColumnBasedInsertOption(name, cols...).WithRLSPrincipal(owner).WithPartialUpdate(partial)
				if upsert {
					_, e := mc.Upsert(ctx, o)
					return e
				}
				_, e := mc.Insert(ctx, o)
				return e
			}
			check := func(t *testing.T) {
				for _, principal := range []string{"alice", "bob", "nobody", ""} {
					t.Run("principal_"+principal, func(t *testing.T) {
						want := map[any]rlsSchemaRow{}
						for id, r := range all {
							if principal == "" || r["owner"] == principal {
								want[id] = r
							}
						}
						fields := []string{"id", "owner", "text"}
						keys := slices.Collect(maps.Keys(want))
						r, e := mc.Query(ctx, client.NewQueryOption(name).WithFilter("id>=0").WithOutputFields(fields...).WithRLSPrincipal(principal).WithSkipRLS(principal == "").WithConsistencyLevel(entity.ClStrong))
						require.NoError(t, e)
						require.ElementsMatch(t, keys, checkRLSSchemaRows(t, r, want, fields))
						for _, word := range []string{"common", "changed"} {
							t.Run(word, func(t *testing.T) {
								matches := maps.Clone(want)
								maps.DeleteFunc(matches, func(_ any, r rlsSchemaRow) bool { return r["text"] != word })
								expected := slices.Collect(maps.Keys(matches))
								opt := client.NewSearchOption(name, 100, []entity.Vector{entity.Text(word)}).WithANNSField("vector").WithOutputFields(fields...).WithRLSPrincipal(principal).WithSkipRLS(principal == "").WithConsistencyLevel(entity.ClStrong)
								rs, e := mc.Search(ctx, opt)
								require.NoError(t, e)
								require.ElementsMatch(t, expected, checkRLSSchemaRows(t, rs[0], matches, fields))
								hs, e := mc.HybridSearch(ctx, client.NewHybridSearchOption(name, 100, client.NewAnnRequest("vector", 100, entity.Text(word))).WithReranker(client.NewRRFReranker()).WithOutputFields(fields...).WithRLSPrincipal(principal).WithSkipRLS(principal == "").WithConsistencyLevel(entity.ClStrong))
								require.NoError(t, e)
								require.ElementsMatch(t, expected, checkRLSSchemaRows(t, hs[0], matches, fields))
								it, e := mc.SearchIterator(ctx, client.NewSearchIteratorOption(name, entity.Text(word)).WithANNSField("vector").WithBatchSize(1).WithOutputFields(fields...).WithRLSPrincipal(principal).WithSkipRLS(principal == "").WithConsistencyLevel(entity.ClStrong))
								require.NoError(t, e)
								var got []any
								ended := false
								for range len(all) + 2 {
									r, e := it.Next(ctx)
									if errors.Is(e, io.EOF) {
										ended = true
										break
									}
									require.NoError(t, e)
									got = append(got, checkRLSSchemaRows(t, r, matches, fields)...)
								}
								require.True(t, ended)
								require.ElementsMatch(t, expected, got)
							})
						}
					})
				}
			}
			for i, owner := range []string{"alice", "bob", "alice", "bob"} {
				id := int64(i + 1)
				require.NoError(t, write(id, owner, "common", false, false))
				all[id] = rlsSchemaRow{"id": id, "owner": owner, "text": "common"}
			}
			t.Run("insert", check)
			for _, partial := range []bool{false, true} {
				t.Run(fmt.Sprintf("upsert_partial_%v", partial), func(t *testing.T) {
					word := "changed"
					if partial {
						word = "common"
					}
					require.NoError(t, write(1, "alice", word, true, partial))
					all[int64(1)]["text"] = word
					require.ErrorContains(t, write(2, "alice", "changed", true, partial), "RLS")
					check(t)
				})
			}
			_, err = mc.Delete(ctx, client.NewDeleteOption(name).WithExpr("text_match(text, 'common')").WithRLSPrincipal("alice"))
			require.NoError(t, err)
			delete(all, int64(1))
			delete(all, int64(3))
			t.Run("delete", check)
			rlsFlush(ctx, t, mc, name)
			t.Run("flushed", check)
		})
	}
}
