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

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/client/v3/column"
	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/index"
	client "github.com/milvus-io/milvus/client/v3/milvusclient"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

func rlsFlush(ctx context.Context, t *testing.T, mc *client.Client, name string) {
	t.Helper()
	// Flush has a low administrative rate limit on the dedicated instance.
	deadline := time.Now().Add(30 * time.Second)
	for {
		task, err := mc.Flush(ctx, client.NewFlushOption(name))
		if err == nil {
			require.NoError(t, task.Await(ctx))
			return
		}
		if client.ErrorCode(err) != merr.Code(merr.ErrServiceRateLimit) || time.Now().After(deadline) {
			require.NoError(t, err)
			return
		}
		select {
		case <-ctx.Done():
			require.NoError(t, ctx.Err())
			return
		case <-time.After(time.Second):
		}
	}
}

// release/reload, partial field loading, and mmap preserve RLS.
func TestRLSStorageLifecycle(t *testing.T) {
	ctx := hp.CreateContext(t, 5*time.Minute)
	mc := hp.CreateDefaultMilvusClient(ctx, t).Client
	tc := rlsSchemaCase{vector: "float32", dim: 128}
	name, all := prepareRLSMixedSegments(ctx, t, mc, tc)
	for _, mode := range []string{"full_reload", "partial_load", "mmap", "reindex"} {
		t.Run(mode, func(t *testing.T) {
			require.NoError(t, mc.ReleaseCollection(ctx, client.NewReleaseCollectionOption(name)))
			if mode == "reindex" {
				require.NoError(t, mc.DropIndex(ctx, client.NewDropIndexOption(name, "vector")))
				idx, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(name, "vector", index.NewHNSWIndex(entity.L2, 8, 64)))
				require.NoError(t, err)
				require.NoError(t, idx.Await(ctx))
			}
			if mode == "mmap" {
				require.NoError(t, mc.AlterCollectionProperties(ctx, client.NewAlterCollectionPropertiesOption(name).WithProperty("mmap.enabled", true)))
			}
			opt := client.NewLoadCollectionOption(name)
			if mode == "partial_load" {
				opt.WithLoadFields("id", "vector", "owner")
			}
			task, err := mc.LoadCollection(ctx, opt)
			require.NoError(t, err)
			require.NoError(t, task.Await(ctx))
			checkRLSMixedSegments(ctx, t, mc, name)
			for _, owner := range []string{"alice", "bob"} {
				var expected []int64
				want := map[any]rlsSchemaRow{}
				for id, row := range all {
					if row["owner"] == owner {
						expected = append(expected, id.(int64))
						want[id] = row
					}
				}
				queryRLS(ctx, t, mc, name, owner, false, expected...)
				result, err := mc.Search(ctx, client.NewSearchOption(name, 20, []entity.Vector{entity.FloatVector(common.GenFloatVector(tc.vectorDim()))}).WithOutputFields("id", "owner").WithRLSPrincipal(owner).WithConsistencyLevel(entity.ClStrong))
				require.NoError(t, err)
				require.Len(t, result, 1)
				require.Len(t, checkRLSSchemaRows(t, result[0], want, []string{"id", "owner"}), 20)
			}
		})
	}
}

// explicit partitions and their recreation cannot bypass policies.
func TestRLSPartitionLifecycle(t *testing.T) {
	ctx := hp.CreateContext(t, 5*time.Minute)
	mc := hp.CreateDefaultMilvusClient(ctx, t).Client
	name := prepareRLSCollection(ctx, t, mc, "", false)
	createRLSPolicy(ctx, t, mc, name, "owner", "owner == $current_principal", "owner == $current_principal", rlsActions...)
	for _, part := range []string{"p_a", "p_b"} {
		require.NoError(t, mc.CreatePartition(ctx, client.NewCreatePartitionOption(name, part)))
		_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(name, rlsColumns([]int64{10, 11}, []string{"alice", "bob"})...).WithPartition(part).WithSkipRLS(true))
		require.NoError(t, err)
	}
	load, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(name))
	require.NoError(t, err)
	require.NoError(t, load.Await(ctx))
	for _, part := range []string{"p_a", "p_b"} {
		r, err := mc.Query(ctx, client.NewQueryOption(name).WithPartitions(part).WithFilter("id>=0").WithOutputFields("id").WithRLSPrincipal("alice").WithConsistencyLevel(entity.ClStrong))
		require.NoError(t, err)
		checkRLSIDs(t, r.GetColumn("id"), 10)
		_, err = mc.Delete(ctx, client.NewDeleteOption(name).WithPartition(part).WithExpr("id>=0").WithRLSPrincipal("alice"))
		require.NoError(t, err)
		r, err = mc.Query(ctx, client.NewQueryOption(name).WithPartitions(part).WithFilter("id>=0").WithOutputFields("id").WithRLSPrincipal("bob").WithConsistencyLevel(entity.ClStrong))
		require.NoError(t, err)
		checkRLSIDs(t, r.GetColumn("id"), 11)
	}
	require.NoError(t, mc.ReleaseCollection(ctx, client.NewReleaseCollectionOption(name)))
	require.NoError(t, mc.DropPartition(ctx, client.NewDropPartitionOption(name, "p_a")))
	require.NoError(t, mc.CreatePartition(ctx, client.NewCreatePartitionOption(name, "p_a")))
	_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(name, rlsColumns([]int64{12}, []string{"bob"})...).WithPartition("p_a").WithRLSPrincipal("alice"))
	require.Error(t, err)
	load, err = mc.LoadCollection(ctx, client.NewLoadCollectionOption(name))
	require.NoError(t, err)
	require.NoError(t, load.Await(ctx))
	queryRLS(ctx, t, mc, name, "alice", false, 1, 3)
	queryRLS(ctx, t, mc, name, "bob", false, 2, 4, 11)
}

// primary-key representation must not change enforcement.
func TestRLSPrimaryKeyVariants(t *testing.T) {
	for _, kind := range []string{"varchar", "auto_id"} {
		t.Run(kind, func(t *testing.T) {
			ctx := hp.CreateContext(t, 3*time.Minute)
			mc := hp.CreateDefaultMilvusClient(ctx, t).Client
			name := common.GenRandomString("rls_pk_", 8)
			pk := entity.NewField().WithName("id").WithIsPrimaryKey(true).WithDataType(entity.FieldTypeInt64).WithIsAutoID(kind == "auto_id")
			if kind == "varchar" {
				pk.WithDataType(entity.FieldTypeVarChar).WithMaxLength(64)
			}
			schema := entity.NewSchema().WithName(name).WithField(pk).WithField(entity.NewField().WithName("owner").WithDataType(entity.FieldTypeVarChar).WithMaxLength(64)).WithField(entity.NewField().WithName("vector").WithDataType(entity.FieldTypeFloatVector).WithDim(4))
			require.NoError(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(name, schema).WithProperty("rls.enabled", true)))
			t.Cleanup(func() {
				cleanup, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()
				require.NoError(t, mc.DropCollection(cleanup, client.NewDropCollectionOption(name)))
			})
			createRLSPolicy(ctx, t, mc, name, "owner", "owner == $current_principal", "owner == $current_principal", rlsActions...)
			cols := []column.Column{column.NewColumnVarChar("owner", []string{"alice", "bob"}), column.NewColumnFloatVector("vector", 4, [][]float32{{1, 2, 3, 4}, {2, 3, 4, 5}})}
			if kind == "varchar" {
				cols = append(cols, column.NewColumnVarChar("id", []string{"α/alice", "用户/bob"}))
			}
			inserted, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(name, cols...).WithSkipRLS(true))
			require.NoError(t, err)
			task, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(name, "vector", index.NewFlatIndex(entity.L2)))
			require.NoError(t, err)
			require.NoError(t, task.Await(ctx))
			load, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(name))
			require.NoError(t, err)
			require.NoError(t, load.Await(ctx))
			for i, principal := range []string{"alice", "bob"} {
				r, err := mc.Query(ctx, client.NewQueryOption(name).WithFilter("owner != ''").WithOutputFields("id").WithRLSPrincipal(principal).WithConsistencyLevel(entity.ClStrong))
				require.NoError(t, err)
				require.Equal(t, 1, r.ResultCount)
				expected, err := inserted.IDs.Get(i)
				require.NoError(t, err)
				actual, err := r.GetColumn("id").Get(0)
				require.NoError(t, err)
				require.Equal(t, expected, actual)
			}
			_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(name, cols...).WithRLSPrincipal("alice"))
			require.Error(t, err)
			_, err = mc.Delete(ctx, client.NewDeleteOption(name).WithExpr("owner != ''").WithRLSPrincipal("alice"))
			require.NoError(t, err)
			r, err := mc.Query(ctx, client.NewQueryOption(name).WithFilter("owner != ''").WithOutputFields("id").WithSkipRLS(true).WithConsistencyLevel(entity.ClStrong))
			require.NoError(t, err)
			require.Equal(t, 1, r.ResultCount)
			want, err := inserted.IDs.Get(1)
			require.NoError(t, err)
			got, err := r.GetColumn("id").Get(0)
			require.NoError(t, err)
			require.Equal(t, want, got)
		})
	}
}

// multi-query ANN, offset, groups and weighted hybrid branches.
func TestRLSAdvancedSearchPaths(t *testing.T) {
	ctx := hp.CreateContext(t, 3*time.Minute)
	mc := hp.CreateDefaultMilvusClient(ctx, t).Client
	name := prepareRLSCollection(ctx, t, mc, "", false)
	createRLSPolicy(ctx, t, mc, name, "owner", "owner == $current_principal", "", milvuspb.RowPolicyAction_Search, milvuspb.RowPolicyAction_HybridSearch)
	for _, mode := range []string{"multi_nq", "offset", "group", "range"} {
		t.Run(mode, func(t *testing.T) {
			opt := client.NewSearchOption(name, 4, []entity.Vector{entity.FloatVector{0, 1, 2, 3}, entity.FloatVector{9, 1, 2, 3}}).WithRLSPrincipal("alice").WithConsistencyLevel(entity.ClStrong)
			if mode == "offset" {
				opt.WithOffset(1)
			}
			if mode == "group" {
				opt.WithGroupByField("active")
			}
			if mode == "range" {
				params := index.NewCustomAnnParam()
				params.WithRadius(100)
				params.WithRangeFilter(0)
				opt.WithAnnParam(params)
			}
			results, err := mc.Search(ctx, opt)
			require.NoError(t, err)
			require.Len(t, results, 2)
			for i, r := range results {
				expected := []int64{1, 3}
				if mode == "offset" {
					expected = []int64{3}
					if i == 1 {
						expected = []int64{1}
					}
				}
				checkRLSIDs(t, r.IDs, expected...)
			}
		})
	}
	result, err := mc.HybridSearch(ctx, client.NewHybridSearchOption(name, 4, client.NewAnnRequest("vector", 4, entity.FloatVector{0, 1, 2, 3}).WithFilter("active == true"), client.NewAnnRequest("vector", 4, entity.FloatVector{9, 1, 2, 3})).WithReranker(client.NewWeightedReranker([]float64{0.3, 0.7})).WithRLSPrincipal("alice").WithConsistencyLevel(entity.ClStrong))
	require.NoError(t, err)
	checkRLSIDs(t, result[0].IDs, 1, 3)
	t.Run("multiple_vector_fields", func(t *testing.T) {
		multi := prepareRLSNullableMultiVector(ctx, t, mc)
		createRLSPolicy(ctx, t, mc, multi, "owner", "owner == $current_principal", "", milvuspb.RowPolicyAction_Search, milvuspb.RowPolicyAction_HybridSearch)
		res, err := mc.HybridSearch(ctx, client.NewHybridSearchOption(multi, 4, client.NewAnnRequest("vector", 4, entity.FloatVector{0, 1, 2, 3}), client.NewAnnRequest("vector2", 4, entity.FloatVector{9, 1, 2, 3})).WithReranker(client.NewRRFReranker()).WithRLSPrincipal("alice").WithOutputFields("owner").WithConsistencyLevel(entity.ClStrong))
		require.NoError(t, err)
		checkRLSIDs(t, res[0].IDs, 1, 3)
		for i := range res[0].IDs.Len() {
			owner, err := res[0].GetColumn("owner").GetAsString(i)
			require.NoError(t, err)
			require.Equal(t, "alice", owner)
		}
	})
}

func prepareRLSNullableMultiVector(ctx context.Context, t *testing.T, mc *client.Client) string {
	t.Helper()
	name := common.GenRandomString("rls_nullable_multi_", 8)
	schema := entity.NewSchema().WithName(name).WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).WithField(entity.NewField().WithName("owner").WithDataType(entity.FieldTypeVarChar).WithMaxLength(64).WithNullable(true))
	for _, field := range []string{"vector", "vector2"} {
		schema.WithField(entity.NewField().WithName(field).WithDataType(entity.FieldTypeFloatVector).WithDim(4))
	}
	require.NoError(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(name, schema).WithProperty("rls.enabled", true).WithConsistencyLevel(entity.ClStrong)))
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		require.NoError(t, mc.DropCollection(cleanup, client.NewDropCollectionOption(name)))
	})
	owners, err := column.NewNullableColumnVarChar("owner", []string{"alice", "bob", "alice"}, []bool{true, true, true, false})
	require.NoError(t, err)
	cols := []column.Column{column.NewColumnInt64("id", []int64{1, 2, 3, 4}), owners}
	for _, field := range []string{"vector", "vector2"} {
		cols = append(cols, column.NewColumnFloatVector(field, 4, [][]float32{{1, 1, 2, 3}, {2, 1, 2, 3}, {3, 1, 2, 3}, {4, 1, 2, 3}}))
	}
	_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(name, cols...).WithSkipRLS(true))
	require.NoError(t, err)
	for _, field := range []string{"vector", "vector2"} {
		idx, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(name, field, index.NewFlatIndex(entity.L2)))
		require.NoError(t, err)
		require.NoError(t, idx.Await(ctx))
	}
	load, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(name))
	require.NoError(t, err)
	require.NoError(t, load.Await(ctx))
	return name
}

// violations at every batch boundary must reject the entire write.
func TestRLSLargeBatchAtomicity(t *testing.T) {
	ctx := hp.CreateContext(t, 5*time.Minute)
	mc := hp.CreateDefaultMilvusClient(ctx, t).Client
	name := prepareRLSCollection(ctx, t, mc, "", false)
	createRLSPolicy(ctx, t, mc, name, "owner", "owner == $current_principal", "owner == $current_principal", rlsActions...)
	for _, bad := range []int{0, 2048, 4095} {
		for _, operation := range []string{"insert", "upsert"} {
			t.Run(fmt.Sprintf("%s_bad_%d", operation, bad), func(t *testing.T) {
				ids := make([]int64, 4096)
				owners := make([]string, 4096)
				for i := range ids {
					ids[i] = int64(100 + i)
					owners[i] = "alice"
				}
				owners[bad] = "bob"
				opt := client.NewColumnBasedInsertOption(name, rlsColumns(ids, owners)...).WithRLSPrincipal("alice")
				var err error
				if operation == "insert" {
					_, err = mc.Insert(ctx, opt)
				} else {
					_, err = mc.Upsert(ctx, opt)
				}
				require.ErrorContains(t, err, "RLS")
				queryRLS(ctx, t, mc, name, "", true, 1, 2, 3, 4)
			})
		}
	}
}

// independent truth table for OR permissive / AND restrictive.
func TestRLSPolicyCombinationOracle(t *testing.T) {
	ctx := hp.CreateContext(t, 5*time.Minute)
	mc := hp.CreateDefaultMilvusClient(ctx, t).Client
	name := prepareRLSCollection(ctx, t, mc, "", false)
	for mask := range 8 {
		for i, expr := range []string{"owner == 'alice'", "department == 'sales'", "active == true"} {
			policy := fmt.Sprintf("rule_%d", i)
			if mask&(1<<i) == 0 {
				continue
			}
			kind := milvuspb.RowPolicyType_RowPolicyTypePermissive
			if i == 2 {
				kind = milvuspb.RowPolicyType_RowPolicyTypeRestrictive
			}
			require.NoError(t, mc.CreateRowPolicy(ctx, client.NewCreateRowPolicyOption(name, policy, kind, milvuspb.RowPolicyAction_Query, milvuspb.RowPolicyAction_Search, milvuspb.RowPolicyAction_Insert).WithUsingExpr(expr).WithCheckExpr(expr)))
		}
		var expected []int64
		for _, id := range []int64{1, 2, 3, 4} {
			permissive := (mask&1 != 0 && (id == 1 || id == 3)) || (mask&2 != 0 && id == 2)
			restrictive := mask&4 == 0 || id != 3
			if permissive && restrictive {
				expected = append(expected, id)
			}
		}
		if mask == 0 {
			_, err := mc.Query(ctx, client.NewQueryOption(name).WithFilter("id>=0").WithRLSPrincipal("alice"))
			require.ErrorContains(t, err, "RLS")
			_, err = mc.Search(ctx, client.NewSearchOption(name, 4, []entity.Vector{entity.FloatVector{0, 1, 2, 3}}).WithRLSPrincipal("alice"))
			require.ErrorContains(t, err, "RLS")
		} else {
			queryRLS(ctx, t, mc, name, "alice", false, expected...)
			results, err := mc.Search(ctx, client.NewSearchOption(name, 4, []entity.Vector{entity.FloatVector{0, 1, 2, 3}}).WithRLSPrincipal("alice").WithConsistencyLevel(entity.ClStrong))
			require.NoError(t, err)
			checkRLSIDs(t, results[0].IDs, expected...)
		}
		for candidate := range 4 {
			owner, department, active := "bob", "eng", true
			if candidate == 0 || candidate == 2 {
				owner = "alice"
			}
			if candidate == 1 {
				department = "sales"
			}
			if candidate == 2 {
				active = false
			}
			allowed := ((mask&1 != 0 && owner == "alice") || (mask&2 != 0 && department == "sales")) && (mask&4 == 0 || active)
			cols := rlsColumns([]int64{100}, []string{owner})
			cols[2] = column.NewColumnVarChar("department", []string{department})
			cols[3] = column.NewColumnBool("active", []bool{active})
			_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(name, cols...).WithRLSPrincipal("alice"))
			if allowed {
				require.NoError(t, err)
				_, err = mc.Delete(ctx, client.NewDeleteOption(name).WithExpr("id==100").WithSkipRLS(true))
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "RLS")
			}
		}
		for i := range 3 {
			if mask&(1<<i) != 0 {
				require.NoError(t, mc.DropRowPolicy(ctx, client.NewDropRowPolicyOption(name, fmt.Sprintf("rule_%d", i))))
			}
		}
	}
	t.Run("null_not_action_check", func(t *testing.T) {
		nullable := prepareRLSNullableMultiVector(ctx, t, mc)
		for _, expr := range []string{"owner is null", "owner == 'alice' or owner == 'bob'"} {
			err := mc.CreateRowPolicy(ctx, client.NewCreateRowPolicyOption(nullable, "unsupported", milvuspb.RowPolicyType_RowPolicyTypePermissive, milvuspb.RowPolicyAction_Query).WithUsingExpr(expr))
			require.ErrorContains(t, err, "invalid RLS")
		}
		// NULL remains unknown under NOT and cannot authorize a read or a write.
		createRLSPolicy(ctx, t, mc, nullable, "owner", "not (owner == 'bob')", "not (owner == 'bob')", milvuspb.RowPolicyAction_Query, milvuspb.RowPolicyAction_Insert)
		require.NoError(t, mc.CreateRowPolicy(ctx, client.NewCreateRowPolicyOption(nullable, "identity", milvuspb.RowPolicyType_RowPolicyTypeRestrictive, milvuspb.RowPolicyAction_Query).WithUsingExpr("owner == $current_principal")))
		queryRLS(ctx, t, mc, nullable, "alice", false, 1, 3)
		queryRLS(ctx, t, mc, nullable, "auditor", false)
		queryRLS(ctx, t, mc, nullable, "bob", false)
		_, err := mc.Search(ctx, client.NewSearchOption(nullable, 4, []entity.Vector{entity.FloatVector{0, 1, 2, 3}}).WithANNSField("vector").WithRLSPrincipal("alice"))
		require.ErrorContains(t, err, "RLS")
		for _, owner := range []string{"alice", "bob", "null"} {
			values := []string{owner}
			valid := []bool{true}
			if owner == "null" {
				values = nil
				valid[0] = false
			}
			owners, err := column.NewNullableColumnVarChar("owner", values, valid)
			require.NoError(t, err)
			_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(nullable, column.NewColumnInt64("id", []int64{100}), owners, column.NewColumnFloatVector("vector", 4, [][]float32{{1, 2, 3, 4}}), column.NewColumnFloatVector("vector2", 4, [][]float32{{1, 2, 3, 4}})).WithRLSPrincipal("alice"))
			if owner == "alice" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "RLS")
			}
		}
		queryRLS(ctx, t, mc, nullable, "alice", false, 1, 3, 100)
		queryRLS(ctx, t, mc, nullable, "auditor", false)
	})
}

// cancellation must not contaminate the next request's identity.
func TestRLSCancellationIsolation(t *testing.T) {
	ctx := hp.CreateContext(t, 3*time.Minute)
	mc := hp.CreateDefaultMilvusClient(ctx, t).Client
	name := prepareRLSCollection(ctx, t, mc, "", false)
	createRLSPolicy(ctx, t, mc, name, "owner", "owner == $current_principal", "", milvuspb.RowPolicyAction_Query, milvuspb.RowPolicyAction_Search)
	for round := range 50 {
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		_, writeErr := mc.Insert(canceled, client.NewColumnBasedInsertOption(name, rlsColumns([]int64{999}, []string{"bob"})...).WithSkipRLS(true))
		require.Error(t, writeErr)
		_, err := mc.Query(canceled, client.NewQueryOption(name).WithFilter("id>=0").WithSkipRLS(true))
		require.Error(t, err)
		deadline, stop := context.WithTimeout(ctx, time.Nanosecond)
		<-deadline.Done()
		_, err = mc.Search(deadline, client.NewSearchOption(name, 4, []entity.Vector{entity.FloatVector{0, 1, 2, 3}}).WithRLSPrincipal("bob"))
		stop()
		require.Error(t, err)
		queryRLS(ctx, t, mc, name, "alice", false, 1, 3)
		if round%10 == 0 {
			_, err = mc.Query(ctx, client.NewQueryOption(name).WithFilter("id>=0"))
			require.ErrorContains(t, err, "rls_principal")
		}
	}
	queryRLS(ctx, t, mc, name, "", true, 1, 2, 3, 4)
}
