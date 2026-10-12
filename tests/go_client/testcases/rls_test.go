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
	"slices"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/client/v3/column"
	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/index"
	client "github.com/milvus-io/milvus/client/v3/milvusclient"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

var rlsActions = []milvuspb.RowPolicyAction{
	milvuspb.RowPolicyAction_Query, milvuspb.RowPolicyAction_Search,
	milvuspb.RowPolicyAction_QueryIterator, milvuspb.RowPolicyAction_SearchIterator,
	milvuspb.RowPolicyAction_HybridSearch, milvuspb.RowPolicyAction_Insert,
	milvuspb.RowPolicyAction_Upsert, milvuspb.RowPolicyAction_Delete,
}

// The explicit fixture keeps authorization expectations independent of random data.
func prepareRLSCollection(ctx context.Context, t *testing.T, mc *client.Client, name string, partitionKey bool) string {
	t.Helper()
	return prepareRLSCollectionWithEnforcement(ctx, t, mc, name, partitionKey, true)
}

func prepareRLSCollectionWithEnforcement(ctx context.Context, t *testing.T, mc *client.Client, name string, partitionKey, enabled bool) string {
	t.Helper()
	if name == "" {
		name = common.GenRandomString("rls_security_", 8)
	}
	schema := entity.NewSchema().WithName(name).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("owner").WithDataType(entity.FieldTypeVarChar).WithMaxLength(256).WithIsPartitionKey(partitionKey)).
		WithField(entity.NewField().WithName("department").WithDataType(entity.FieldTypeVarChar).WithMaxLength(256)).
		WithField(entity.NewField().WithName("active").WithDataType(entity.FieldTypeBool)).
		WithField(entity.NewField().WithName("number").WithDataType(entity.FieldTypeInt64)).
		WithField(entity.NewField().WithName("vector").WithDataType(entity.FieldTypeFloatVector).WithDim(4))
	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(name, schema).WithConsistencyLevel(entity.ClStrong).WithProperty("rls.enabled", enabled))
	common.CheckErr(t, err, true)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		require.NoError(t, mc.DropCollection(cleanupCtx, client.NewDropCollectionOption(name)))
	})
	_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(name, rlsColumns([]int64{1, 2, 3, 4}, []string{"alice", "bob", "alice", "bob"})...).WithSkipRLS(true))
	common.CheckErr(t, err, true)
	idx, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(name, "vector", index.NewFlatIndex(entity.L2)))
	common.CheckErr(t, err, true)
	common.CheckErr(t, idx.Await(ctx), true)
	load, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(name))
	common.CheckErr(t, err, true)
	common.CheckErr(t, load.Await(ctx), true)
	return name
}

func rlsColumns(ids []int64, owners []string) []column.Column {
	departments, active, vectors := make([]string, len(ids)), make([]bool, len(ids)), make([][]float32, len(ids))
	for i, id := range ids {
		departments[i], active[i], vectors[i] = "eng", id != 3, []float32{float32(id), 1, 2, 3}
		if id == 2 {
			departments[i] = "sales"
		}
	}
	return []column.Column{
		column.NewColumnInt64("id", ids), column.NewColumnVarChar("owner", owners),
		column.NewColumnVarChar("department", departments), column.NewColumnBool("active", active),
		column.NewColumnInt64("number", ids), column.NewColumnFloatVector("vector", 4, vectors),
	}
}

func createRLSPolicy(ctx context.Context, t *testing.T, mc *client.Client, name, policy, using, check string, actions ...milvuspb.RowPolicyAction) {
	t.Helper()
	common.CheckErr(t, mc.CreateRowPolicy(ctx, client.NewCreateRowPolicyOption(name, policy, milvuspb.RowPolicyType_RowPolicyTypePermissive, actions...).WithUsingExpr(using).WithCheckExpr(check)), true)
}

func checkRLSIDs(t *testing.T, col column.Column, expected ...int64) {
	t.Helper()
	var got []int64
	if col != nil {
		for i := range col.Len() {
			id, err := col.GetAsInt64(i)
			require.NoError(t, err)
			got = append(got, id)
		}
	}
	require.ElementsMatch(t, expected, got)
}

func queryRLS(ctx context.Context, t *testing.T, mc *client.Client, name, principal string, skip bool, expected ...int64) {
	t.Helper()
	r, err := mc.Query(ctx, client.NewQueryOption(name).WithFilter("id >= 0").WithOutputFields("id").WithRLSPrincipal(principal).WithSkipRLS(skip).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	checkRLSIDs(t, r.GetColumn("id"), expected...)
}

// Each operation uses its own action, including the two iterator actions.
func runRLSAction(ctx context.Context, mc *client.Client, name, principal string, skip bool, action milvuspb.RowPolicyAction) error {
	switch action {
	case milvuspb.RowPolicyAction_Query:
		_, err := mc.Query(ctx, client.NewQueryOption(name).WithFilter("id >= 0").WithRLSPrincipal(principal).WithSkipRLS(skip))
		return err
	case milvuspb.RowPolicyAction_Search:
		_, err := mc.Search(ctx, client.NewSearchOption(name, 4, []entity.Vector{entity.FloatVector{0, 1, 2, 3}}).WithRLSPrincipal(principal).WithSkipRLS(skip))
		return err
	case milvuspb.RowPolicyAction_HybridSearch:
		_, err := mc.HybridSearch(ctx, client.NewHybridSearchOption(name, 4, client.NewAnnRequest("vector", 4, entity.FloatVector{0, 1, 2, 3})).WithReranker(client.NewRRFReranker()).WithRLSPrincipal(principal).WithSkipRLS(skip))
		return err
	case milvuspb.RowPolicyAction_QueryIterator:
		it, err := mc.QueryIterator(ctx, client.NewQueryIteratorOption(name).WithBatchSize(1).WithRLSPrincipal(principal).WithSkipRLS(skip))
		if err == nil {
			_, err = it.Next(ctx)
		}
		return err
	case milvuspb.RowPolicyAction_SearchIterator:
		it, err := mc.SearchIterator(ctx, client.NewSearchIteratorOption(name, entity.FloatVector{0, 1, 2, 3}).WithBatchSize(1).WithRLSPrincipal(principal).WithSkipRLS(skip))
		if err == nil {
			_, err = it.Next(ctx)
		}
		return err
	case milvuspb.RowPolicyAction_Insert:
		_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(name, rlsColumns([]int64{100}, []string{"alice"})...).WithRLSPrincipal(principal).WithSkipRLS(skip))
		return err
	case milvuspb.RowPolicyAction_Upsert:
		_, err := mc.Upsert(ctx, client.NewColumnBasedInsertOption(name, rlsColumns([]int64{101}, []string{"alice"})...).WithRLSPrincipal(principal).WithSkipRLS(skip))
		return err
	case milvuspb.RowPolicyAction_Delete:
		_, err := mc.Delete(ctx, client.NewDeleteOption(name).WithInt64IDs("id", []int64{1, 2}).WithRLSPrincipal(principal).WithSkipRLS(skip))
		return err
	}
	panic("unhandled RLS action")
}

func TestRLSDefaultDenyAndActionIsolation(t *testing.T) {
	ctx := hp.CreateContext(t, 3*time.Minute)
	mc := hp.CreateDefaultMilvusClient(ctx, t).Client
	name := prepareRLSCollection(ctx, t, mc, "", false)
	for _, action := range rlsActions {
		t.Run("no_policy_"+action.String(), func(t *testing.T) {
			common.CheckErr(t, runRLSAction(ctx, mc, name, "alice", false, action), false, "no applicable")
		})
	}
	for _, granted := range rlsActions {
		t.Run("only_"+granted.String(), func(t *testing.T) {
			isolated := prepareRLSCollection(ctx, t, mc, "", false)
			using, check := "owner == $current_principal", ""
			if granted == milvuspb.RowPolicyAction_Insert || granted == milvuspb.RowPolicyAction_Upsert {
				check = using
			}
			if granted == milvuspb.RowPolicyAction_Insert {
				using = ""
			}
			createRLSPolicy(ctx, t, mc, isolated, "owner", using, check, granted)
			for _, action := range rlsActions {
				if action != granted {
					t.Run("denies_"+action.String(), func(t *testing.T) {
						common.CheckErr(t, runRLSAction(ctx, mc, isolated, "alice", false, action), false, "no applicable")
					})
				}
			}
			queryRLS(ctx, t, mc, isolated, "", true, 1, 2, 3, 4)
			common.CheckErr(t, runRLSAction(ctx, mc, isolated, "alice", false, granted), true)
		})
	}
	createRLSPolicy(ctx, t, mc, name, "owner", "owner == $current_principal", "owner == $current_principal", rlsActions...)
	for _, action := range rlsActions {
		for _, principal := range []string{"", " \t"} {
			t.Run("missing_principal_"+action.String()+fmt.Sprintf("_%q", principal), func(t *testing.T) {
				common.CheckErr(t, runRLSAction(ctx, mc, name, principal, false, action), false, "rls_principal")
			})
		}
	}
	common.CheckErr(t, mc.AlterCollectionProperties(ctx, client.NewAlterCollectionPropertiesOption(name).WithProperty("rls.force", true)), true)
	for _, action := range rlsActions {
		t.Run("force_rejects_root_skip_"+action.String(), func(t *testing.T) {
			common.CheckErr(t, runRLSAction(ctx, mc, name, "alice", true, action), false, "rls.force")
		})
	}
	common.CheckErr(t, mc.AlterCollectionProperties(ctx, client.NewAlterCollectionPropertiesOption(name).WithProperty("rls.force", false)), true)
	queryRLS(ctx, t, mc, name, "", true, 1, 2, 3, 4)
}

func TestRLSReadPathsAndPrincipalBinding(t *testing.T) {
	ctx := hp.CreateContext(t, 4*time.Minute)
	mc := hp.CreateDefaultMilvusClient(ctx, t).Client
	name := prepareRLSCollection(ctx, t, mc, "", false)
	createRLSPolicy(ctx, t, mc, name, "owner", "owner == $current_principal", "owner == $current_principal", rlsActions...)
	for _, principal := range []string{"alice", "bob", "unknown", "alice", "bob", `alice' or true or owner == 'bob`, "alice\x00bob", " alice", "ALICE"} {
		t.Run(fmt.Sprintf("principal_%q", principal), func(t *testing.T) {
			var expected []int64
			if principal == "alice" {
				expected = []int64{1, 3}
			}
			if principal == "bob" {
				expected = []int64{2, 4}
			}
			queryRLS(ctx, t, mc, name, principal, false, expected...)
		})
	}
	for _, expr := range []string{"", "id >= 0", "owner == 'bob' or id >= 0", "not (owner == 'alice')", "id in [1,2,3,4]"} {
		t.Run("filter_"+expr, func(t *testing.T) {
			expected := []int64{1, 3}
			if expr == "not (owner == 'alice')" {
				expected = nil
			}
			r, err := mc.Query(ctx, client.NewQueryOption(name).WithFilter(expr).WithLimit(20).WithOutputFields("id").WithRLSPrincipal("alice"))
			common.CheckErr(t, err, true)
			checkRLSIDs(t, r.GetColumn("id"), expected...)
		})
	}
	t.Run("random_sample", func(t *testing.T) {
		for range 10 {
			r, err := mc.Query(ctx, client.NewQueryOption(name).WithFilter("random_sample(0.5)").WithOutputFields("id").WithRLSPrincipal("alice"))
			common.CheckErr(t, err, true)
			for i := range r.Len() {
				id, err := r.GetColumn("id").GetAsInt64(i)
				require.NoError(t, err)
				require.Contains(t, []int64{1, 3}, id)
			}
		}
	})
	t.Run("count", func(t *testing.T) {
		r, err := mc.Query(ctx, client.NewQueryOption(name).WithOutputFields("count(*)").WithRLSPrincipal("alice"))
		common.CheckErr(t, err, true)
		count, err := r.GetColumn("count(*)").GetAsInt64(0)
		require.NoError(t, err)
		require.EqualValues(t, 2, count)
	})
	t.Run("template_cannot_override_principal", func(t *testing.T) {
		r, err := mc.Query(ctx, client.NewQueryOption(name).WithFilter("owner == {who}").WithTemplateParam("who", "bob").WithTemplateParam("current_principal", "bob").WithOutputFields("id").WithRLSPrincipal("alice"))
		common.CheckErr(t, err, true)
		checkRLSIDs(t, r.GetColumn("id"))
	})
	for _, grouped := range []bool{false, true} {
		t.Run(fmt.Sprintf("search_output_group_%v", grouped), func(t *testing.T) {
			opt := client.NewSearchOption(name, 4, []entity.Vector{entity.FloatVector{2, 1, 2, 3}}).WithOutputFields("*").WithRLSPrincipal("alice").WithConsistencyLevel(entity.ClStrong)
			if grouped {
				opt.WithGroupByField("active")
			}
			r, err := mc.Search(ctx, opt)
			common.CheckErr(t, err, true)
			checkRLSIDs(t, r[0].IDs, 1, 3)
			for i := range r[0].Len() {
				owner, err := r[0].GetColumn("owner").GetAsString(i)
				require.NoError(t, err)
				require.Equal(t, "alice", owner)
			}
		})
	}
	t.Run("hidden_search_seed", func(t *testing.T) {
		_, hiddenErr := mc.Search(ctx, client.NewSearchByIDsOption(name, 4, column.NewColumnInt64("id", []int64{2})).WithANNSField("vector").WithRLSPrincipal("alice"))
		_, missingErr := mc.Search(ctx, client.NewSearchByIDsOption(name, 4, column.NewColumnInt64("id", []int64{9999})).WithANNSField("vector").WithRLSPrincipal("alice"))
		common.CheckErr(t, hiddenErr, false, "do not exist")
		common.CheckErr(t, missingErr, false, "do not exist")
	})
	t.Run("visible_search_seed", func(t *testing.T) {
		r, err := mc.Search(ctx, client.NewSearchByIDsOption(name, 4, column.NewColumnInt64("id", []int64{1})).WithANNSField("vector").WithRLSPrincipal("alice"))
		common.CheckErr(t, err, true)
		checkRLSIDs(t, r[0].IDs, 1, 3)
	})
	t.Run("hybrid_conflicting_filter", func(t *testing.T) {
		r, err := mc.HybridSearch(ctx, client.NewHybridSearchOption(name, 4,
			client.NewAnnRequest("vector", 4, entity.FloatVector{2, 1, 2, 3}).WithFilter("owner == 'bob'"),
			client.NewAnnRequest("vector", 4, entity.FloatVector{0, 1, 2, 3})).WithReranker(client.NewRRFReranker()).WithOutputFields("*").WithRLSPrincipal("alice"))
		common.CheckErr(t, err, true)
		checkRLSIDs(t, r[0].IDs, 1, 3)
	})
	t.Run("iterators", func(t *testing.T) {
		qi, err := mc.QueryIterator(ctx, client.NewQueryIteratorOption(name).WithBatchSize(1).WithOutputFields("id").WithRLSPrincipal("alice"))
		common.CheckErr(t, err, true)
		var got []int64
		ended := false
		for range 10 {
			r, err := qi.Next(ctx)
			if errors.Is(err, io.EOF) {
				ended = true
				break
			}
			common.CheckErr(t, err, true)
			for i := range r.Len() {
				id, err := r.GetColumn("id").GetAsInt64(i)
				require.NoError(t, err)
				got = append(got, id)
			}
		}
		require.True(t, ended, "query iterator did not reach EOF")
		require.ElementsMatch(t, []int64{1, 3}, got)
		si, err := mc.SearchIterator(ctx, client.NewSearchIteratorOption(name, entity.FloatVector{0, 1, 2, 3}).WithBatchSize(1).WithRLSPrincipal("alice"))
		common.CheckErr(t, err, true)
		got = nil
		ended = false
		for range 10 {
			r, err := si.Next(ctx)
			if errors.Is(err, io.EOF) {
				ended = true
				break
			}
			common.CheckErr(t, err, true)
			for i := range r.Len() {
				id, err := r.IDs.GetAsInt64(i)
				require.NoError(t, err)
				got = append(got, id)
			}
		}
		require.True(t, ended, "search iterator did not reach EOF")
		require.ElementsMatch(t, []int64{1, 3}, got)
	})
}

func TestRLSWriteChecksAndPartialUpsert(t *testing.T) {
	ctx := hp.CreateContext(t, 4*time.Minute)
	mc := hp.CreateDefaultMilvusClient(ctx, t).Client
	name := prepareRLSCollection(ctx, t, mc, "", false)
	createRLSPolicy(ctx, t, mc, name, "owner", "owner == $current_principal", "owner == $current_principal", rlsActions...)
	for _, partial := range []bool{false, true} {
		for _, test := range []struct {
			name   string
			ids    []int64
			owners []string
		}{
			{"hidden_pk", []int64{2}, []string{"alice"}},
			{"transfer_owner", []int64{1}, []string{"bob"}},
			{"mixed_existing", []int64{1, 2}, []string{"alice", "alice"}},
			{"mixed_new", []int64{5, 2}, []string{"alice", "alice"}},
			{"mixed_check", []int64{5, 6}, []string{"alice", "bob"}},
		} {
			t.Run(fmt.Sprintf("%s_partial_%v", test.name, partial), func(t *testing.T) {
				cols := rlsColumns(test.ids, test.owners)
				if partial && test.name != "mixed_new" && test.name != "mixed_check" {
					cols = cols[:2]
				}
				_, err := mc.Upsert(ctx, client.NewColumnBasedInsertOption(name, cols...).WithPartialUpdate(partial).WithRLSPrincipal("alice"))
				common.CheckErr(t, err, false, "RLS")
				queryRLS(ctx, t, mc, name, "", true, 1, 2, 3, 4)
				queryRLS(ctx, t, mc, name, "bob", false, 2, 4)
			})
		}
	}
	t.Run("partial_omitted_policy_field", func(t *testing.T) {
		_, err := mc.Upsert(ctx, client.NewColumnBasedInsertOption(name, column.NewColumnInt64("id", []int64{1}), column.NewColumnVarChar("department", []string{"updated"})).WithPartialUpdate(true).WithRLSPrincipal("alice"))
		common.CheckErr(t, err, true)
		_, err = mc.Upsert(ctx, client.NewColumnBasedInsertOption(name, column.NewColumnInt64("id", []int64{2}), column.NewColumnVarChar("department", []string{"stolen"})).WithPartialUpdate(true).WithRLSPrincipal("alice"))
		common.CheckErr(t, err, false, "RLS")
		r, err := mc.Query(ctx, client.NewQueryOption(name).WithFilter("id == 2").WithOutputFields("department").WithSkipRLS(true))
		common.CheckErr(t, err, true)
		department, err := r.GetColumn("department").GetAsString(0)
		require.NoError(t, err)
		require.Equal(t, "sales", department)
	})
	t.Run("insert_rejects_entire_batch", func(t *testing.T) {
		_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(name, rlsColumns([]int64{7, 8}, []string{"alice", "bob"})...).WithRLSPrincipal("alice"))
		common.CheckErr(t, err, false, "RLS")
		queryRLS(ctx, t, mc, name, "", true, 1, 2, 3, 4)
	})
	t.Run("delete_filters_hidden_rows", func(t *testing.T) {
		_, err := mc.Delete(ctx, client.NewDeleteOption(name).WithExpr("id >= 0").WithRLSPrincipal("alice"))
		common.CheckErr(t, err, true)
		queryRLS(ctx, t, mc, name, "", true, 2, 4)
	})
}

func TestRLSTagBindingAndPolicyComposition(t *testing.T) {
	ctx := hp.CreateContext(t, 4*time.Minute)
	mc := hp.CreateDefaultMilvusClient(ctx, t).Client
	name := prepareRLSCollection(ctx, t, mc, "", false)
	createRLSPolicy(ctx, t, mc, name, "department", "department == $current_principal_tags['department']", "", milvuspb.RowPolicyAction_Query)
	queryRLS(ctx, t, mc, name, "alice", false)
	for _, test := range []struct {
		name, tags string
		expected   []int64
	}{
		{"grant", `{"department":"eng","other":"keep"}`, []int64{1, 3, 4}},
		{"preserve_unspecified", `{"other":"updated"}`, []int64{1, 3, 4}},
		{"replace", `{"department":"sales"}`, []int64{2}},
		{"type_mismatch", `{"department":1}`, nil},
		{"injection", `{"department":"eng' or true or department == 'sales"}`, nil},
		{"restore", `{"department":"eng"}`, []int64{1, 3, 4}},
	} {
		t.Run(test.name, func(t *testing.T) {
			common.CheckErr(t, mc.SetRLSPrincipalTags(ctx, client.NewSetRLSPrincipalTagsOption(name, "alice", test.tags)), true)
			queryRLS(ctx, t, mc, name, "alice", false, test.expected...)
		})
	}
	common.CheckErr(t, mc.SetRLSPrincipalTags(ctx, client.NewSetRLSPrincipalTagsOption(name, "alice", `{"department":[]}`)), true)
	queryRLS(ctx, t, mc, name, "alice", false)
	common.CheckErr(t, mc.SetRLSPrincipalTags(ctx, client.NewSetRLSPrincipalTagsOption(name, "alice", `{"department":"eng"}`)), true)
	queryRLS(ctx, t, mc, name, "alice", false, 1, 3, 4)
	for _, tags := range []string{`{"department":null}`, `{"department":true}`, `{"department":{}}`, `{"department":1e9999}`, `[]`, `{broken`} {
		t.Run("invalid_"+tags, func(t *testing.T) {
			err := mc.SetRLSPrincipalTags(ctx, client.NewSetRLSPrincipalTagsOption(name, "alice", tags))
			require.Error(t, err)
			stored, err := mc.GetRLSPrincipalTags(ctx, client.NewGetRLSPrincipalTagsOption(name, "alice"))
			common.CheckErr(t, err, true)
			require.JSONEq(t, `{"department":"eng","other":"updated"}`, stored)
			queryRLS(ctx, t, mc, name, "alice", false, 1, 3, 4)
		})
	}
	common.CheckErr(t, mc.DeleteRLSPrincipalTags(ctx, client.NewDeleteRLSPrincipalTagsOption(name, "alice", "department", "department")), true)
	queryRLS(ctx, t, mc, name, "alice", false)
	tags, err := mc.GetRLSPrincipalTags(ctx, client.NewGetRLSPrincipalTagsOption(name, "alice"))
	common.CheckErr(t, err, true)
	require.JSONEq(t, `{"other":"updated"}`, tags)
	createRLSPolicy(ctx, t, mc, name, "owner", "owner == $current_principal", "", milvuspb.RowPolicyAction_Query)
	common.CheckErr(t, mc.SetRLSPrincipalTags(ctx, client.NewSetRLSPrincipalTagsOption(name, "alice", `{"department":"sales"}`)), true)
	queryRLS(ctx, t, mc, name, "alice", false, 1, 2, 3)
	common.CheckErr(t, mc.CreateRowPolicy(ctx, client.NewCreateRowPolicyOption(name, "active", milvuspb.RowPolicyType_RowPolicyTypeRestrictive, milvuspb.RowPolicyAction_Query).WithUsingExpr("active == true")), true)
	queryRLS(ctx, t, mc, name, "alice", false, 1, 2)
	common.CheckErr(t, mc.DropRowPolicy(ctx, client.NewDropRowPolicyOption(name, "owner")), true)
	common.CheckErr(t, mc.DropRowPolicy(ctx, client.NewDropRowPolicyOption(name, "department")), true)
	queryRLS(ctx, t, mc, name, "alice", false)
}

func TestRLSNumericTagsAndLiteralBinding(t *testing.T) {
	ctx := hp.CreateContext(t, 4*time.Minute)
	mc := hp.CreateDefaultMilvusClient(ctx, t).Client
	name := prepareRLSCollection(ctx, t, mc, "", false)
	cols := rlsColumns([]int64{5, 6}, []string{"alice", "alice"})
	cols[4] = column.NewColumnInt64("number", []int64{9007199254740992, 9007199254740993})
	_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(name, cols...).WithSkipRLS(true))
	common.CheckErr(t, err, true)
	createRLSPolicy(ctx, t, mc, name, "number", "number == $current_principal_tags['number']", "number == $current_principal_tags['number']", rlsActions...)
	for _, test := range []struct {
		value    string
		expected []int64
	}{
		{`3`, []int64{3}},
		{`3.0`, []int64{3}},
		{`3e0`, []int64{3}},
		{`3.5`, nil},
		{`"3"`, nil},
		{`9007199254740993`, []int64{6}},
		{`9007199254740992`, []int64{5}},
		{`9223372036854775808`, nil},
	} {
		t.Run(test.value, func(t *testing.T) {
			common.CheckErr(t, mc.SetRLSPrincipalTags(ctx, client.NewSetRLSPrincipalTagsOption(name, "alice", `{"number":`+test.value+`}`)), true)
			queryRLS(ctx, t, mc, name, "alice", false, test.expected...)
			for _, n := range []int64{3, 9007199254740992, 9007199254740993} {
				cols := rlsColumns([]int64{100}, []string{"alice"})
				cols[4] = column.NewColumnInt64("number", []int64{n})
				_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(name, cols...).WithRLSPrincipal("alice"))
				allowed := slices.Contains(test.expected, int64(3)) && n == 3 || slices.Contains(test.expected, int64(5)) && n == 9007199254740992 || slices.Contains(test.expected, int64(6)) && n == 9007199254740993
				if allowed {
					common.CheckErr(t, err, true)
					_, err = mc.Delete(ctx, client.NewDeleteOption(name).WithInt64IDs("id", []int64{100}).WithSkipRLS(true))
					common.CheckErr(t, err, true)
				} else {
					common.CheckErr(t, err, false, "RLS")
				}
			}
		})
	}
	common.CheckErr(t, mc.DropRowPolicy(ctx, client.NewDropRowPolicyOption(name, "number")), true)
	createRLSPolicy(ctx, t, mc, name, "owner", "owner == $current_principal", "owner == $current_principal", rlsActions...)
	for i, principal := range []string{`alice' or true`, `" or true`, `$current_principal`, "租户甲", "alice\\bob"} {
		t.Run(principal, func(t *testing.T) {
			id := int64(200 + i)
			_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(name, rlsColumns([]int64{id}, []string{principal})...).WithRLSPrincipal(principal))
			common.CheckErr(t, err, true)
			queryRLS(ctx, t, mc, name, principal, false, id)
		})
	}
	// A quoted RLS variable is literal data, not a template.
	common.CheckErr(t, mc.UpdateRowPolicy(ctx, client.NewUpdateRowPolicyOption(name, "owner", milvuspb.RowPolicyType_RowPolicyTypePermissive, milvuspb.RowPolicyAction_Query).WithUsingExpr(`owner == "$current_principal"`)), true)
	queryRLS(ctx, t, mc, name, "alice", false, 202)
}

func TestRLSAliasAndCollectionCacheIsolation(t *testing.T) {
	ctx := hp.CreateContext(t, 5*time.Minute)
	mc := hp.CreateDefaultMilvusClient(ctx, t).Client
	a, b := prepareRLSCollection(ctx, t, mc, "", false), prepareRLSCollection(ctx, t, mc, "", false)
	for _, name := range []string{a, b} {
		createRLSPolicy(ctx, t, mc, name, "department", "department == $current_principal_tags['department']", "", milvuspb.RowPolicyAction_Query)
	}
	common.CheckErr(t, mc.SetRLSPrincipalTags(ctx, client.NewSetRLSPrincipalTagsOption(a, "alice", `{"department":"eng"}`)), true)
	common.CheckErr(t, mc.SetRLSPrincipalTags(ctx, client.NewSetRLSPrincipalTagsOption(b, "alice", `{"department":"sales"}`)), true)
	alias := common.GenRandomString("rls_alias_", 8)
	common.CheckErr(t, mc.CreateAlias(ctx, client.NewCreateAliasOption(a, alias)), true)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		require.NoError(t, mc.DropAlias(cleanupCtx, client.NewDropAliasOption(alias)))
	})
	for range 3 {
		queryRLS(ctx, t, mc, a, "alice", false, 1, 3, 4)
		queryRLS(ctx, t, mc, b, "alice", false, 2)
		queryRLS(ctx, t, mc, alias, "alice", false, 1, 3, 4)
	}
	common.CheckErr(t, mc.AlterAlias(ctx, client.NewAlterAliasOption(alias, b)), true)
	queryRLS(ctx, t, mc, alias, "alice", false, 2)
	common.CheckErr(t, mc.SetRLSPrincipalTags(ctx, client.NewSetRLSPrincipalTagsOption(alias, "alice", `{"department":"eng"}`)), true)
	queryRLS(ctx, t, mc, b, "alice", false, 1, 3, 4)
	common.CheckErr(t, mc.DeleteRLSPrincipalTags(ctx, client.NewDeleteRLSPrincipalTagsOption(b, "alice")), true)
	queryRLS(ctx, t, mc, alias, "alice", false)
	queryRLS(ctx, t, mc, a, "alice", false, 1, 3, 4)
}

func TestRLSPartitionKeyUpsertFailsClosed(t *testing.T) {
	ctx := hp.CreateContext(t, 4*time.Minute)
	mc := hp.CreateDefaultMilvusClient(ctx, t).Client
	name := prepareRLSCollection(ctx, t, mc, "", true)
	createRLSPolicy(ctx, t, mc, name, "owner", "owner == $current_principal", "owner == $current_principal", rlsActions...)
	_, err := mc.Upsert(ctx, client.NewColumnBasedInsertOption(name, rlsColumns([]int64{1}, []string{"alice"})...).WithRLSPrincipal("alice"))
	common.CheckErr(t, err, false, "partition-key")
	_, err = mc.Upsert(ctx, client.NewColumnBasedInsertOption(name, rlsColumns([]int64{2}, []string{"alice"})...).WithRLSPrincipal("alice"))
	common.CheckErr(t, err, false, "RLS")
	queryRLS(ctx, t, mc, name, "bob", false, 2, 4)
	_, err = mc.Upsert(ctx, client.NewColumnBasedInsertOption(name, rlsColumns([]int64{5}, []string{"alice"})...).WithRLSPrincipal("alice"))
	common.CheckErr(t, err, true)
	queryRLS(ctx, t, mc, name, "alice", false, 1, 3, 5)
}

func TestRLSIteratorRevocationBetweenPages(t *testing.T) {
	ctx := hp.CreateContext(t, 4*time.Minute)
	mc := hp.CreateDefaultMilvusClient(ctx, t).Client
	name := prepareRLSCollection(ctx, t, mc, "", false)
	createRLSPolicy(ctx, t, mc, name, "department", "department == $current_principal_tags['department']", "",
		milvuspb.RowPolicyAction_QueryIterator, milvuspb.RowPolicyAction_SearchIterator)
	grant := func() {
		common.CheckErr(t, mc.SetRLSPrincipalTags(ctx,
			client.NewSetRLSPrincipalTagsOption(name, "alice", `{"department":"eng"}`)), true)
	}
	revoke := func() {
		common.CheckErr(t, mc.DeleteRLSPrincipalTags(ctx,
			client.NewDeleteRLSPrincipalTagsOption(name, "alice")), true)
	}
	grant()
	queryIterator, err := mc.QueryIterator(ctx, client.NewQueryIteratorOption(name).
		WithBatchSize(1).WithOutputFields("id").WithRLSPrincipal("alice").WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	first, err := queryIterator.Next(ctx)
	common.CheckErr(t, err, true)
	require.Equal(t, 1, first.Len())
	revoke()
	second, err := queryIterator.Next(ctx)
	require.ErrorIs(t, err, io.EOF)
	require.Zero(t, second.Len())
	grant()
	searchIterator, err := mc.SearchIterator(ctx, client.NewSearchIteratorOption(name,
		entity.FloatVector{0, 1, 2, 3}).WithBatchSize(1).WithRLSPrincipal("alice").WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	first, err = searchIterator.Next(ctx)
	common.CheckErr(t, err, true)
	require.Equal(t, 1, first.Len())
	revoke()
	second, err = searchIterator.Next(ctx)
	require.ErrorIs(t, err, io.EOF)
	require.Zero(t, second.Len())
}

// Metadata updates must replace warmed read and write decisions after acknowledgement.
func TestRLSPolicyUpdateReadAndWriteDecisions(t *testing.T) {
	ctx := hp.CreateContext(t, 5*time.Minute)
	admin := hp.CreateDefaultMilvusClient(ctx, t).Client
	name := prepareRLSCollection(ctx, t, admin, "", false)
	createRLSPolicy(ctx, t, admin, name, "owner", "owner == 'alice'", "owner == 'alice'", rlsActions...)
	app := admin
	checkReads := func(expected ...int64) {
		for _, consistency := range []entity.ConsistencyLevel{entity.ClStrong, entity.ClBounded, entity.ClEventually} {
			r, err := app.Query(ctx, client.NewQueryOption(name).WithFilter("id >= 0").WithOutputFields("id").WithRLSPrincipal("probe").WithConsistencyLevel(consistency))
			common.CheckErr(t, err, true)
			checkRLSIDs(t, r.GetColumn("id"), expected...)
			results, err := app.Search(ctx, client.NewSearchOption(name, 10, []entity.Vector{entity.FloatVector{0, 1, 2, 3}}).WithRLSPrincipal("probe").WithConsistencyLevel(consistency))
			common.CheckErr(t, err, true)
			checkRLSIDs(t, results[0].IDs, expected...)
		}
	}
	// Warm every write action, then establish the data watermark before weaker reads.
	_, err := app.Insert(ctx, client.NewColumnBasedInsertOption(name, rlsColumns([]int64{10}, []string{"alice"})...).WithRLSPrincipal("probe"))
	common.CheckErr(t, err, true)
	_, err = app.Upsert(ctx, client.NewColumnBasedInsertOption(name, rlsColumns([]int64{10}, []string{"alice"})...).WithRLSPrincipal("probe"))
	common.CheckErr(t, err, true)
	_, err = app.Delete(ctx, client.NewDeleteOption(name).WithInt64IDs("id", []int64{10}).WithRLSPrincipal("probe"))
	common.CheckErr(t, err, true)
	queryRLS(ctx, t, app, name, "probe", false, 1, 3)
	checkReads(1, 3)
	for _, owner := range []string{"bob", "alice", "bob"} {
		common.CheckErr(t, admin.UpdateRowPolicy(ctx, client.NewUpdateRowPolicyOption(name, "owner", milvuspb.RowPolicyType_RowPolicyTypePermissive, rlsActions...).WithUsingExpr("owner == '"+owner+"'").WithCheckExpr("owner == '"+owner+"'")), true)
		if owner == "alice" {
			checkReads(1, 3)
		} else {
			checkReads(2, 4)
		}
	}
	// A failed replacement must leave the previous policy intact.
	require.Error(t, admin.UpdateRowPolicy(ctx, client.NewUpdateRowPolicyOption(name, "owner", milvuspb.RowPolicyType_RowPolicyTypePermissive, rlsActions...).WithUsingExpr("missing_field == 1").WithCheckExpr("true")))
	checkReads(2, 4)
	_, err = app.Insert(ctx, client.NewColumnBasedInsertOption(name, rlsColumns([]int64{11}, []string{"alice"})...).WithRLSPrincipal("probe"))
	common.CheckErr(t, err, false, "RLS")
	_, err = app.Upsert(ctx, client.NewColumnBasedInsertOption(name, rlsColumns([]int64{1}, []string{"alice"})...).WithRLSPrincipal("probe"))
	common.CheckErr(t, err, false, "RLS")
	_, err = app.Upsert(ctx, client.NewColumnBasedInsertOption(name, rlsColumns([]int64{12}, []string{"alice"})...).WithRLSPrincipal("probe"))
	common.CheckErr(t, err, false, "RLS")
	_, err = app.Insert(ctx, client.NewColumnBasedInsertOption(name, rlsColumns([]int64{20}, []string{"bob"})...).WithRLSPrincipal("probe"))
	common.CheckErr(t, err, true)
	_, err = app.Upsert(ctx, client.NewColumnBasedInsertOption(name, column.NewColumnInt64("id", []int64{20}), column.NewColumnVarChar("department", []string{"changed"})).WithPartialUpdate(true).WithRLSPrincipal("probe"))
	common.CheckErr(t, err, true)
	_, err = app.Delete(ctx, client.NewDeleteOption(name).WithInt64IDs("id", []int64{1, 2}).WithRLSPrincipal("probe"))
	common.CheckErr(t, err, true)
	queryRLS(ctx, t, admin, name, "", true, 1, 3, 4, 20)
	// Updating only CHECK must affect the next partial upsert and reject the whole write.
	common.CheckErr(t, admin.UpdateRowPolicy(ctx, client.NewUpdateRowPolicyOption(name, "owner", milvuspb.RowPolicyType_RowPolicyTypePermissive, rlsActions...).WithUsingExpr("owner == 'bob'").WithCheckExpr("department == 'sales'")), true)
	_, err = app.Upsert(ctx, client.NewColumnBasedInsertOption(name, column.NewColumnInt64("id", []int64{20}), column.NewColumnVarChar("department", []string{"changed_again"})).WithPartialUpdate(true).WithRLSPrincipal("probe"))
	common.CheckErr(t, err, false, "RLS")
	r, err := admin.Query(ctx, client.NewQueryOption(name).WithFilter("id == 20").WithOutputFields("department").WithSkipRLS(true).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	value, err := r.GetColumn("department").GetAsString(0)
	require.NoError(t, err)
	require.Equal(t, "changed", value)
	// Removing Search from the action list must invalidate its compiled policy.
	common.CheckErr(t, admin.UpdateRowPolicy(ctx, client.NewUpdateRowPolicyOption(name, "owner", milvuspb.RowPolicyType_RowPolicyTypePermissive, milvuspb.RowPolicyAction_Query).WithUsingExpr("owner == 'bob'")), true)
	common.CheckErr(t, runRLSAction(ctx, app, name, "probe", false, milvuspb.RowPolicyAction_Search), false, "no applicable")
	queryRLS(ctx, t, app, name, "probe", false, 4, 20)
	common.CheckErr(t, admin.DropRowPolicy(ctx, client.NewDropRowPolicyOption(name, "owner")), true)
	common.CheckErr(t, runRLSAction(ctx, app, name, "probe", false, milvuspb.RowPolicyAction_Query), false, "no applicable")
	createRLSPolicy(ctx, t, admin, name, "owner", "owner == 'alice'", "", milvuspb.RowPolicyAction_Query)
	queryRLS(ctx, t, app, name, "probe", false, 1, 3)
}

func TestRLSPolicyUpdateIteratorPages(t *testing.T) {
	for _, action := range []milvuspb.RowPolicyAction{milvuspb.RowPolicyAction_QueryIterator, milvuspb.RowPolicyAction_SearchIterator} {
		t.Run(action.String(), func(t *testing.T) {
			ctx := hp.CreateContext(t, 3*time.Minute)
			admin := hp.CreateDefaultMilvusClient(ctx, t).Client
			name := prepareRLSCollection(ctx, t, admin, "", false)
			createRLSPolicy(ctx, t, admin, name, "owner", "owner == 'alice'", "", action)
			app := admin
			var next func() (int, error)
			if action == milvuspb.RowPolicyAction_QueryIterator {
				it, err := app.QueryIterator(ctx, client.NewQueryIteratorOption(name).WithBatchSize(1).WithOutputFields("id").WithRLSPrincipal("probe").WithConsistencyLevel(entity.ClStrong))
				common.CheckErr(t, err, true)
				next = func() (int, error) { r, err := it.Next(ctx); return r.Len(), err }
			} else {
				it, err := app.SearchIterator(ctx, client.NewSearchIteratorOption(name, entity.FloatVector{0, 1, 2, 3}).WithBatchSize(1).WithRLSPrincipal("probe").WithConsistencyLevel(entity.ClStrong))
				common.CheckErr(t, err, true)
				next = func() (int, error) { r, err := it.Next(ctx); return r.Len(), err }
			}
			n, err := next()
			common.CheckErr(t, err, true)
			require.Equal(t, 1, n)
			common.CheckErr(t, admin.UpdateRowPolicy(ctx, client.NewUpdateRowPolicyOption(name, "owner", milvuspb.RowPolicyType_RowPolicyTypePermissive, action).WithUsingExpr("false")), true)
			n, err = next()
			require.ErrorIs(t, err, io.EOF)
			require.Zero(t, n)
		})
	}
}

func TestRLSNullableDefaultAndArrayPredicates(t *testing.T) {
	ctx := hp.CreateContext(t, 5*time.Minute)
	mc := hp.CreateDefaultMilvusClient(ctx, t).Client
	name := common.GenRandomString("rls_nullable_", 8)
	schema := entity.NewSchema().WithName(name).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("owner").WithDataType(entity.FieldTypeVarChar).WithMaxLength(32).WithNullable(true).WithDefaultValueString("bob")).
		WithField(entity.NewField().WithName("groups").WithDataType(entity.FieldTypeArray).WithElementType(entity.FieldTypeInt64).WithMaxCapacity(16)).
		WithField(entity.NewField().WithName("vector").WithDataType(entity.FieldTypeFloatVector).WithDim(4))
	common.CheckErr(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(name, schema).WithProperty("rls.enabled", true).WithConsistencyLevel(entity.ClStrong)), true)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		require.NoError(t, mc.DropCollection(cleanupCtx, client.NewDropCollectionOption(name)))
	})
	cols := func(id int64, groups []int64) []column.Column {
		return []column.Column{column.NewColumnInt64("id", []int64{id}), column.NewColumnInt64Array("groups", [][]int64{groups}), column.NewColumnFloatVector("vector", 4, [][]float32{{float32(id), 1, 2, 3}})}
	}
	createRLSPolicy(ctx, t, mc, name, "owner", "owner == $current_principal", "owner == $current_principal", rlsActions...)
	t.Run("default_is_checked", func(t *testing.T) {
		_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(name, cols(1, []int64{1, 2})...).WithRLSPrincipal("alice"))
		common.CheckErr(t, err, false, "RLS")
		_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(name, cols(2, []int64{2})...).WithRLSPrincipal("bob"))
		common.CheckErr(t, err, true)
	})
	t.Run("explicit_null_uses_default", func(t *testing.T) {
		owner, err := column.NewNullableColumnVarChar("owner", nil, []bool{false})
		require.NoError(t, err)
		_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(name, append(cols(3, nil), owner)...).WithRLSPrincipal("alice"))
		common.CheckErr(t, err, false, "RLS")
		_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(name, append(cols(3, nil), owner)...).WithSkipRLS(true))
		common.CheckErr(t, err, true)
	})
	_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(name, append(cols(4, []int64{1, 2}), column.NewColumnVarChar("owner", []string{"alice"}))...).WithRLSPrincipal("alice"))
	common.CheckErr(t, err, true)
	idx, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(name, "vector", index.NewFlatIndex(entity.L2)))
	common.CheckErr(t, err, true)
	common.CheckErr(t, idx.Await(ctx), true)
	load, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(name))
	common.CheckErr(t, err, true)
	common.CheckErr(t, load.Await(ctx), true)
	queryRLS(ctx, t, mc, name, "alice", false, 4)
	queryRLS(ctx, t, mc, name, "bob", false, 2, 3)
	queryRLS(ctx, t, mc, name, "", true, 2, 3, 4)
	defaulted, err := mc.Query(ctx, client.NewQueryOption(name).WithFilter("id == 3").WithOutputFields("owner").WithSkipRLS(true))
	common.CheckErr(t, err, true)
	require.Equal(t, 1, defaulted.Len())
	isNull, err := defaulted.GetColumn("owner").IsNull(0)
	require.NoError(t, err)
	require.False(t, isNull)
	defaultOwner, err := defaulted.GetColumn("owner").GetAsString(0)
	require.NoError(t, err)
	require.Equal(t, "bob", defaultOwner)
	for _, test := range []struct {
		expr     string
		expected []int64
	}{
		{`array_contains(groups, 1)`, []int64{4}},
		{`array_contains_all(groups, [1,2])`, []int64{4}},
		{`array_contains_any(groups, [1,2])`, []int64{2, 4}},
		{`owner in ["alice"]`, []int64{4}},
	} {
		t.Run(test.expr, func(t *testing.T) {
			common.CheckErr(t, mc.UpdateRowPolicy(ctx, client.NewUpdateRowPolicyOption(name, "owner", milvuspb.RowPolicyType_RowPolicyTypePermissive, rlsActions...).WithUsingExpr(test.expr).WithCheckExpr(test.expr)), true)
			queryRLS(ctx, t, mc, name, "alice", false, test.expected...)
			_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(name, cols(10, []int64{9})...).WithRLSPrincipal("alice"))
			common.CheckErr(t, err, false, "RLS")
			_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(name, append(cols(11, []int64{1, 2}), column.NewColumnVarChar("owner", []string{"alice"}))...).WithRLSPrincipal("alice"))
			common.CheckErr(t, err, true)
			_, err = mc.Delete(ctx, client.NewDeleteOption(name).WithInt64IDs("id", []int64{11}).WithSkipRLS(true))
			common.CheckErr(t, err, true)
		})
	}
}

func TestRLSNullableOwnerWithoutDefault(t *testing.T) {
	ctx := hp.CreateContext(t, 3*time.Minute)
	mc := hp.CreateDefaultMilvusClient(ctx, t).Client
	name := common.GenRandomString("rls_null_owner_", 8)
	schema := entity.NewSchema().WithName(name).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("owner").WithDataType(entity.FieldTypeVarChar).WithMaxLength(32).WithNullable(true)).
		WithField(entity.NewField().WithName("vector").WithDataType(entity.FieldTypeFloatVector).WithDim(4))
	common.CheckErr(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(name, schema).WithProperty("rls.enabled", true).WithConsistencyLevel(entity.ClStrong)), true)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		require.NoError(t, mc.DropCollection(cleanupCtx, client.NewDropCollectionOption(name)))
	})
	createRLSPolicy(ctx, t, mc, name, "owner", "owner == $current_principal", "owner == $current_principal", rlsActions...)
	nullOwner, err := column.NewNullableColumnVarChar("owner", []string{"alice", "bob"}, []bool{true, true, false})
	require.NoError(t, err)
	_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(name,
		column.NewColumnInt64("id", []int64{1, 2, 3}), nullOwner,
		column.NewColumnFloatVector("vector", 4, [][]float32{{1, 0, 0, 0}, {2, 0, 0, 0}, {3, 0, 0, 0}}),
	).WithSkipRLS(true))
	common.CheckErr(t, err, true)
	idx, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(name, "vector", index.NewFlatIndex(entity.L2)))
	common.CheckErr(t, err, true)
	common.CheckErr(t, idx.Await(ctx), true)
	load, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(name))
	common.CheckErr(t, err, true)
	common.CheckErr(t, load.Await(ctx), true)
	queryRLS(ctx, t, mc, name, "alice", false, 1)
	queryRLS(ctx, t, mc, name, "bob", false, 2)
	queryRLS(ctx, t, mc, name, "", true, 1, 2, 3)
	_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(name,
		column.NewColumnInt64("id", []int64{4}),
		column.NewColumnFloatVector("vector", 4, [][]float32{{4, 0, 0, 0}}),
	).WithRLSPrincipal("alice"))
	common.CheckErr(t, err, false, "RLS")
}

func TestRLSArrayTagsAndNotFailClosed(t *testing.T) {
	ctx := hp.CreateContext(t, 4*time.Minute)
	mc := hp.CreateDefaultMilvusClient(ctx, t).Client
	name := common.GenRandomString("rls_array_tags_", 8)
	schema := entity.NewSchema().WithName(name).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("groups").WithDataType(entity.FieldTypeArray).WithElementType(entity.FieldTypeInt64).WithMaxCapacity(8).WithNullable(true)).
		WithField(entity.NewField().WithName("vector").WithDataType(entity.FieldTypeFloatVector).WithDim(4))
	common.CheckErr(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(name, schema).WithProperty("rls.enabled", true).WithConsistencyLevel(entity.ClStrong)), true)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		require.NoError(t, mc.DropCollection(cleanupCtx, client.NewDropCollectionOption(name)))
	})
	cols := func(id int64, groups []int64) []column.Column {
		return []column.Column{
			column.NewColumnInt64("id", []int64{id}),
			column.NewColumnInt64Array("groups", [][]int64{groups}),
			column.NewColumnFloatVector("vector", 4, [][]float32{{float32(id), 1, 2, 3}}),
		}
	}
	for _, row := range []struct {
		id     int64
		groups []int64
	}{{1, []int64{1}}, {2, []int64{2}}, {3, []int64{3}}, {4, []int64{}}} {
		_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(name, cols(row.id, row.groups)...).WithSkipRLS(true))
		common.CheckErr(t, err, true)
	}
	nullGroups, err := column.NewNullableColumnInt64Array("groups", nil, []bool{false})
	require.NoError(t, err)
	_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(name,
		column.NewColumnInt64("id", []int64{5}), nullGroups,
		column.NewColumnFloatVector("vector", 4, [][]float32{{5, 1, 2, 3}}),
	).WithSkipRLS(true))
	common.CheckErr(t, err, true)
	idx, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(name, "vector", index.NewFlatIndex(entity.L2)))
	common.CheckErr(t, err, true)
	common.CheckErr(t, idx.Await(ctx), true)
	load, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(name))
	common.CheckErr(t, err, true)
	common.CheckErr(t, load.Await(ctx), true)
	using := "array_contains_any(groups, $current_principal_tags['groups'])"
	createRLSPolicy(ctx, t, mc, name, "group_read", using, "", milvuspb.RowPolicyAction_Query)
	common.CheckErr(t, mc.SetRLSPrincipalTags(ctx, client.NewSetRLSPrincipalTagsOption(name, "alice", `{"groups":[1,3]}`)), true)
	queryRLS(ctx, t, mc, name, "alice", false, 1, 3)
	common.CheckErr(t, mc.SetRLSPrincipalTags(ctx, client.NewSetRLSPrincipalTagsOption(name, "alice", `{"groups":[1.0,3]}`)), true)
	queryRLS(ctx, t, mc, name, "alice", false, 1, 3)
	require.Error(t, mc.SetRLSPrincipalTags(ctx, client.NewSetRLSPrincipalTagsOption(name, "alice", `{"groups":[9007199254740993,1.0]}`)))
	queryRLS(ctx, t, mc, name, "alice", false, 1, 3)
	require.Error(t, mc.SetRLSPrincipalTags(ctx, client.NewSetRLSPrincipalTagsOption(name, "alice", `{"groups":["1",2]}`)))
	queryRLS(ctx, t, mc, name, "alice", false, 1, 3)
	common.CheckErr(t, mc.UpdateRowPolicy(ctx, client.NewUpdateRowPolicyOption(name, "group_read", milvuspb.RowPolicyType_RowPolicyTypePermissive, milvuspb.RowPolicyAction_Query).WithUsingExpr("not "+using)), true)
	common.CheckErr(t, mc.SetRLSPrincipalTags(ctx, client.NewSetRLSPrincipalTagsOption(name, "alice", `{"groups":[1]}`)), true)
	queryRLS(ctx, t, mc, name, "alice", false, 2, 3, 4)
	common.CheckErr(t, mc.DeleteRLSPrincipalTags(ctx, client.NewDeleteRLSPrincipalTagsOption(name, "alice")), true)
	queryRLS(ctx, t, mc, name, "alice", false)
	common.CheckErr(t, mc.SetRLSPrincipalTags(ctx, client.NewSetRLSPrincipalTagsOption(name, "alice", `{"groups":[1]}`)), true)
	createRLSPolicy(ctx, t, mc, name, "group_write", "", using, milvuspb.RowPolicyAction_Insert)
	_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(name, cols(6, []int64{1})...).WithRLSPrincipal("alice"))
	common.CheckErr(t, err, true)
	_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(name, cols(7, []int64{2})...).WithRLSPrincipal("alice"))
	common.CheckErr(t, err, false, "RLS")
	common.CheckErr(t, mc.UpdateRowPolicy(ctx, client.NewUpdateRowPolicyOption(name, "group_write", milvuspb.RowPolicyType_RowPolicyTypePermissive, milvuspb.RowPolicyAction_Insert).WithCheckExpr("not "+using)), true)
	_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(name, cols(8, []int64{2})...).WithRLSPrincipal("alice"))
	common.CheckErr(t, err, true)
	_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(name, cols(9, []int64{1})...).WithRLSPrincipal("alice"))
	common.CheckErr(t, err, false, "RLS")
	queryRLS(ctx, t, mc, name, "", true, 1, 2, 3, 4, 5, 6, 8)
}
