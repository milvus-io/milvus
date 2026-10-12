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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/client/v3/column"
	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/index"
	client "github.com/milvus-io/milvus/client/v3/milvusclient"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

// compare successful empty wire responses with high-level SDK decoding.
func TestRLSSchemaEmptyDynamicResult(t *testing.T) {
	ctx := hp.CreateContext(t, 3*time.Minute)
	mc := hp.CreateDefaultMilvusClient(ctx, t).Client
	tc := rlsSchemaCase{vector: "float32", dynamic: true}
	name := prepareRLSSchema(ctx, t, mc, tc)
	r := rlsSchemaData(t, tc, 1, 0, "alice")
	_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(name, rlsSchemaWriteColumns(t, tc, r)...).WithRLSPrincipal("alice"))
	require.NoError(t, err)
	for _, skip := range []bool{false, true} {
		t.Run(fmt.Sprintf("skip_%v", skip), func(t *testing.T) {
			o := client.NewSearchOption(name, 10, []entity.Vector{entity.FloatVector{1, 1, 2, 3, 4, 5, 6, 7}}).WithANNSField("vector").WithOutputFields("id", "vector", "extra").WithRLSPrincipal("nobody").WithSkipRLS(skip).WithConsistencyLevel(entity.ClStrong)
			if skip {
				o.WithFilter("id == -1")
			}
			request, err := o.Request()
			require.NoError(t, err)
			response, err := mc.GetService().Search(ctx, request)
			require.NoError(t, err)
			require.Zero(t, response.GetStatus().GetCode())
			require.Zero(t, int(response.GetStatus().GetErrorCode()))
			require.EqualValues(t, 1, response.GetResults().GetNumQueries())
			require.Equal(t, []int64{0}, response.GetResults().GetTopks())
			t.Logf("EMPTY_RESPONSE skip=%v fields=%v", skip, response.GetResults().GetFieldsData())
			result, err := mc.Search(ctx, o)
			require.NoError(t, err)
			require.Len(t, result, 1)
			require.NoError(t, result[0].Err)
			require.Zero(t, result[0].Len())
		})
	}
}

// struct reconstruction must retain requested dynamic output names.
func TestRLSSchemaStructuredDynamicProjection(t *testing.T) {
	ctx := hp.CreateContext(t, 3*time.Minute)
	mc := hp.CreateDefaultMilvusClient(ctx, t).Client
	connected, err := mc.GetService().Connect(ctx, &milvuspb.ConnectRequest{ClientInfo: &commonpb.ClientInfo{SdkType: "GoMilvusClient", User: "root"}})
	require.NoError(t, err)
	require.Zero(t, connected.GetStatus().GetCode())
	t.Logf("RUNTIME_BASELINE %v", connected.GetServerInfo())
	tc := rlsSchemaCase{vector: "float32", dynamic: true, structured: true}
	name := prepareRLSSchema(ctx, t, mc, tc)
	r := rlsSchemaData(t, tc, 1, 0, "alice")
	_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(name, rlsSchemaWriteColumns(t, tc, r)...).WithRLSPrincipal("alice"))
	require.NoError(t, err)
	for _, partial := range []bool{false, true} {
		if partial {
			_, err = mc.Upsert(ctx, client.NewColumnBasedInsertOption(name, column.NewColumnInt64("id", []int64{1}), column.NewColumnInt64Array("a_i64", [][]int64{{1, 2}})).WithPartialUpdate(true).WithRLSPrincipal("alice"))
			require.NoError(t, err)
			r["a_i64"] = []int64{1, 2}
		}
		for _, skip := range []bool{false, true} {
			for _, structOutput := range []bool{false, true} {
				t.Run(fmt.Sprintf("partial_%v/skip_%v/struct_%v", partial, skip, structOutput), func(t *testing.T) {
					fields := []string{"id", "extra", "a_i64"}
					if structOutput {
						fields = append(fields, "objects")
					}
					o := client.NewQueryOption(name).WithFilter("id == 1").WithOutputFields(fields...).WithRLSPrincipal("alice").WithSkipRLS(skip).WithConsistencyLevel(entity.ClStrong)
					request, err := o.Request()
					require.NoError(t, err)
					response, err := mc.GetService().Query(ctx, request)
					require.NoError(t, err)
					require.Zero(t, response.GetStatus().GetCode())
					require.Zero(t, int(response.GetStatus().GetErrorCode()))
					t.Logf("STRUCT_DYNAMIC_RESPONSE outputs=%v fields=%v", response.GetOutputFields(), response.GetFieldsData())
					result, err := mc.Query(ctx, o)
					require.NoError(t, err)
					require.ElementsMatch(t, []any{int64(1)}, checkRLSSchemaRows(t, result, map[any]rlsSchemaRow{int64(1): r}, fields))
				})
			}
		}
	}
}

// recover only to report SDK panics as test failures, never as accepted behavior.
func TestRLSSchemaNullableRowEncoding(t *testing.T) {
	for _, kind := range []string{"json", "vector"} {
		for _, skip := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/skip_%v", kind, skip), func(t *testing.T) {
				ctx := hp.CreateContext(t, 2*time.Minute)
				mc := hp.CreateDefaultMilvusClient(ctx, t).Client
				name := common.GenRandomString("rls_nullable_row_", 8)
				schema := entity.NewSchema().WithName(name).WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).WithField(entity.NewField().WithName("owner").WithDataType(entity.FieldTypeVarChar).WithMaxLength(64)).WithField(entity.NewField().WithName("vector").WithDataType(entity.FieldTypeFloatVector).WithDim(4).WithNullable(kind == "vector"))
				if kind == "json" {
					schema.WithField(entity.NewField().WithName("doc").WithDataType(entity.FieldTypeJSON).WithNullable(true))
				}
				require.NoError(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(name, schema).WithProperty("rls.enabled", true).WithConsistencyLevel(entity.ClStrong)))
				t.Cleanup(func() {
					cleanup, cancel := context.WithTimeout(context.Background(), time.Minute)
					defer cancel()
					require.NoError(t, mc.DropCollection(cleanup, client.NewDropCollectionOption(name)))
				})
				createRLSPolicy(ctx, t, mc, name, "owner", "owner == $current_principal", "owner == $current_principal", rlsActions...)
				task, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(name, "vector", index.NewFlatIndex(entity.L2)))
				require.NoError(t, err)
				require.NoError(t, task.Await(ctx))
				load, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(name))
				require.NoError(t, err)
				require.NoError(t, load.Await(ctx))
				for _, mode := range []string{"map", "struct"} {
					t.Run(mode, func(t *testing.T) {
						var rows []any
						if mode == "map" {
							a := map[string]any{"id": int64(1), "owner": "alice", "vector": []float32{1, 2, 3, 4}}
							b := map[string]any{"id": int64(2), "owner": "alice", "vector": []float32{2, 3, 4, 5}}
							if kind == "json" {
								a["doc"] = []byte(`{"n":1}`)
								b["doc"] = nil
							} else {
								b["vector"] = nil
							}
							rows = []any{a, b}
						} else if kind == "json" {
							type row struct {
								ID     int64     `milvus:"name:id"`
								Owner  string    `milvus:"name:owner"`
								Vector []float32 `milvus:"name:vector"`
								Doc    *[]byte   `milvus:"name:doc"`
							}
							rows = []any{&row{3, "alice", []float32{1, 2, 3, 4}, new([]byte(`{"n":1}`))}, &row{4, "alice", []float32{2, 3, 4, 5}, nil}}
						} else {
							type row struct {
								ID     int64      `milvus:"name:id"`
								Owner  string     `milvus:"name:owner"`
								Vector *[]float32 `milvus:"name:vector"`
							}
							rows = []any{&row{3, "alice", new([]float32{1, 2, 3, 4})}, &row{4, "alice", nil}}
						}
						var result client.InsertResult
						require.NotPanics(t, func() {
							result, err = mc.Insert(ctx, client.NewRowBasedInsertOption(name, rows...).WithRLSPrincipal("alice").WithSkipRLS(skip))
						})
						require.NoError(t, err)
						require.EqualValues(t, 2, result.InsertCount)
					})
				}
			})
		}
	}
}

// an unrestricted hybrid branch must retain every authorized partition.
func TestRLSSchemaPartitionHybridBranches(t *testing.T) {
	ctx := hp.CreateContext(t, 3*time.Minute)
	mc := hp.CreateDefaultMilvusClient(ctx, t).Client
	name := prepareRLSCollection(ctx, t, mc, "", true)
	createRLSPolicy(ctx, t, mc, name, "owner", "owner == $current_principal", "owner == $current_principal", rlsActions...)
	for _, skip := range []bool{false, true} {
		for _, reverse := range []bool{false, true} {
			t.Run(fmt.Sprintf("skip_%v/reverse_%v", skip, reverse), func(t *testing.T) {
				want := []int64{1, 3}
				if skip {
					want = []int64{1, 2, 3, 4}
				}
				control, err := mc.Search(ctx, client.NewSearchOption(name, 10, []entity.Vector{entity.FloatVector{1, 1, 2, 3}}).WithANNSField("vector").WithRLSPrincipal("alice").WithSkipRLS(skip).WithConsistencyLevel(entity.ClStrong))
				require.NoError(t, err)
				checkRLSIDs(t, control[0].IDs, want...)
				branches := []*client.AnnRequest{client.NewAnnRequest("vector", 10, entity.FloatVector{1, 1, 2, 3}), client.NewAnnRequest("vector", 10, entity.FloatVector{1, 1, 2, 3}).WithFilter("owner == 'bob'")}
				if reverse {
					branches[0], branches[1] = branches[1], branches[0]
				}
				results, err := mc.HybridSearch(ctx, client.NewHybridSearchOption(name, 10, branches...).WithReranker(client.NewRRFReranker()).WithRLSPrincipal("alice").WithSkipRLS(skip).WithConsistencyLevel(entity.ClStrong))
				require.NoError(t, err)
				checkRLSIDs(t, results[0].IDs, want...)
			})
		}
	}
}
