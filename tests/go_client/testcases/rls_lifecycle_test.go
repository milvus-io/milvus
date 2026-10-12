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
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/client/v3/entity"
	client "github.com/milvus-io/milvus/client/v3/milvusclient"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

// rejected writes must leave the listed and enforced policy unchanged.
func TestRLSPolicyLifecycle(t *testing.T) {
	ctx := hp.CreateContext(t, 3*time.Minute)
	mc := hp.CreateDefaultMilvusClient(ctx, t).Client
	connected, err := mc.GetService().Connect(ctx, &milvuspb.ConnectRequest{ClientInfo: &commonpb.ClientInfo{SdkType: "Go", SdkVersion: "RLS E2E"}})
	common.CheckErr(t, err, true)
	require.Zero(t, connected.GetStatus().GetCode())
	require.Equal(t, commonpb.ErrorCode_Success, connected.GetStatus().GetErrorCode())
	require.NotEmpty(t, connected.GetServerInfo().GetGitCommit())
	t.Logf("server identity: %s", connected.GetServerInfo())
	name := prepareRLSCollection(ctx, t, mc, "", false)
	policies, err := mc.ListRowPolicies(ctx, client.NewListRowPoliciesOption(name))
	common.CheckErr(t, err, true)
	require.Empty(t, policies)
	create := client.NewCreateRowPolicyOption(name, "owner", milvuspb.RowPolicyType_RowPolicyTypePermissive, milvuspb.RowPolicyAction_Query).
		WithUsingExpr("owner == $current_principal").WithDescription("initial")
	common.CheckErr(t, mc.CreateRowPolicy(ctx, create), true)
	common.CheckErr(t, mc.CreateRowPolicy(ctx, create), false, "already exists")
	common.CheckErr(t, mc.UpdateRowPolicy(ctx, client.NewUpdateRowPolicyOption(name, "missing", milvuspb.RowPolicyType_RowPolicyTypePermissive, milvuspb.RowPolicyAction_Query).WithUsingExpr("true")), false, "does not exist")
	policies, err = mc.ListRowPolicies(ctx, client.NewListRowPoliciesOption(name))
	common.CheckErr(t, err, true)
	require.Len(t, policies, 1)
	require.Equal(t, "owner", policies[0].GetPolicyName())
	require.Equal(t, "initial", policies[0].GetDescription())
	require.Equal(t, "owner == $current_principal", policies[0].GetUsingExpr())
	require.Equal(t, []milvuspb.RowPolicyAction{milvuspb.RowPolicyAction_Query}, policies[0].GetActions())
	queryRLS(ctx, t, mc, name, "alice", false, 1, 3)
	common.CheckErr(t, mc.UpdateRowPolicy(ctx, client.NewUpdateRowPolicyOption(name, "owner", milvuspb.RowPolicyType_RowPolicyTypePermissive, milvuspb.RowPolicyAction_Query).WithUsingExpr("owner == 'bob'").WithDescription("replacement")), true)
	policies, err = mc.ListRowPolicies(ctx, client.NewListRowPoliciesOption(name))
	common.CheckErr(t, err, true)
	require.Len(t, policies, 1)
	require.Equal(t, "replacement", policies[0].GetDescription())
	require.Equal(t, "owner == 'bob'", policies[0].GetUsingExpr())
	queryRLS(ctx, t, mc, name, "alice", false, 2, 4)
	for range 2 {
		common.CheckErr(t, mc.DropRowPolicy(ctx, client.NewDropRowPolicyOption(name, "owner")), true)
	}
	policies, err = mc.ListRowPolicies(ctx, client.NewListRowPoliciesOption(name))
	common.CheckErr(t, err, true)
	require.Empty(t, policies)
	common.CheckErr(t, runRLSAction(ctx, mc, name, "alice", false, milvuspb.RowPolicyAction_Query), false, "no applicable")
}

// an empty tag binding is removed; a tagless principal remains valid.
func TestRLSPrincipalTagLifecycle(t *testing.T) {
	ctx := hp.CreateContext(t, 3*time.Minute)
	mc := hp.CreateDefaultMilvusClient(ctx, t).Client
	name := prepareRLSCollection(ctx, t, mc, "", false)
	createRLSPolicy(ctx, t, mc, name, "department", "department == $current_principal_tags['department']", "", milvuspb.RowPolicyAction_Query)
	queryRLS(ctx, t, mc, name, "alice", false)
	for _, principal := range []string{"alice", "bob"} {
		common.CheckErr(t, mc.SetRLSPrincipalTags(ctx, client.NewSetRLSPrincipalTagsOption(name, principal, `{"department":"eng","keep":"value"}`)), true)
	}
	queryRLS(ctx, t, mc, name, "alice", false, 1, 3, 4)
	principals, err := mc.ListRLSPrincipals(ctx, client.NewListRLSPrincipalsOption(name))
	common.CheckErr(t, err, true)
	require.ElementsMatch(t, []string{"alice", "bob"}, principals)
	common.CheckErr(t, mc.DeleteRLSPrincipalTags(ctx, client.NewDeleteRLSPrincipalTagsOption(name, "alice", "department", "department", "missing")), true)
	queryRLS(ctx, t, mc, name, "alice", false)
	tags, err := mc.GetRLSPrincipalTags(ctx, client.NewGetRLSPrincipalTagsOption(name, "alice"))
	common.CheckErr(t, err, true)
	require.JSONEq(t, `{"keep":"value"}`, tags)
	for range 2 {
		common.CheckErr(t, mc.DeleteRLSPrincipalTags(ctx, client.NewDeleteRLSPrincipalTagsOption(name, "alice")), true)
	}
	principals, err = mc.ListRLSPrincipals(ctx, client.NewListRLSPrincipalsOption(name))
	common.CheckErr(t, err, true)
	require.Equal(t, []string{"bob"}, principals)
	// Management lookup reports a missing binding; runtime principal use remains valid.
	_, err = mc.GetRLSPrincipalTags(ctx, client.NewGetRLSPrincipalTagsOption(name, "alice"))
	common.CheckErr(t, err, false, "does not exist")
	common.CheckErr(t, mc.SetRLSPrincipalTags(ctx, client.NewSetRLSPrincipalTagsOption(name, "alice", `{"department":"sales"}`)), true)
	queryRLS(ctx, t, mc, name, "alice", false, 2)
	common.CheckErr(t, mc.DeleteRLSPrincipalTags(ctx, client.NewDeleteRLSPrincipalTagsOption(name, "alice")), true)
	common.CheckErr(t, mc.UpdateRowPolicy(ctx, client.NewUpdateRowPolicyOption(name, "department", milvuspb.RowPolicyType_RowPolicyTypePermissive, milvuspb.RowPolicyAction_Query).WithUsingExpr("owner == $current_principal")), true)
	queryRLS(ctx, t, mc, name, "alice", false, 1, 3)
}

// invalid definitions must not replace an already valid policy.
func TestRLSPolicyExpressionValidation(t *testing.T) {
	ctx := hp.CreateContext(t, 3*time.Minute)
	mc := hp.CreateDefaultMilvusClient(ctx, t).Client
	name := prepareRLSCollection(ctx, t, mc, "", false)
	createRLSPolicy(ctx, t, mc, name, "owner", "owner == $current_principal", "", milvuspb.RowPolicyAction_Query)
	for _, test := range []struct{ name, expr string }{
		{"ordered", "number > 1"},
		{"field_comparison", "owner == department"},
		{"conjunction", "owner == 'alice' and active == true"},
		{"disjunction", "owner == 'alice' or active == true"},
		{"unknown_field", "missing == 1"},
		{"vector", "vector == 1"},
		{"raw_template", "owner == {who}"},
		{"empty", ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := mc.UpdateRowPolicy(ctx, client.NewUpdateRowPolicyOption(name, "owner", milvuspb.RowPolicyType_RowPolicyTypePermissive, milvuspb.RowPolicyAction_Query).WithUsingExpr(test.expr))
			require.Error(t, err)
			policies, err := mc.ListRowPolicies(ctx, client.NewListRowPoliciesOption(name))
			common.CheckErr(t, err, true)
			require.Len(t, policies, 1)
			require.Equal(t, "owner == $current_principal", policies[0].GetUsingExpr())
			queryRLS(ctx, t, mc, name, "alice", false, 1, 3)
		})
	}
	for _, test := range []struct {
		name string
		opt  client.CreateRowPolicyOption
	}{
		{"blank_name", client.NewCreateRowPolicyOption(name, " ", milvuspb.RowPolicyType_RowPolicyTypePermissive, milvuspb.RowPolicyAction_Query).WithUsingExpr("true")},
		{"no_action", client.NewCreateRowPolicyOption(name, "bad", milvuspb.RowPolicyType_RowPolicyTypePermissive).WithUsingExpr("true")},
		{"invalid_action", client.NewCreateRowPolicyOption(name, "bad", milvuspb.RowPolicyType_RowPolicyTypePermissive, milvuspb.RowPolicyAction(999)).WithUsingExpr("true")},
		{"invalid_type", client.NewCreateRowPolicyOption(name, "bad", milvuspb.RowPolicyType(999), milvuspb.RowPolicyAction_Query).WithUsingExpr("true")},
	} {
		t.Run(test.name, func(t *testing.T) { require.Error(t, mc.CreateRowPolicy(ctx, test.opt)) })
	}
	queryRLS(ctx, t, mc, name, "alice", false, 1, 3)
}

// enforcement can change without changing metadata ownership.
func TestRLSCollectionProperties(t *testing.T) {
	ctx := hp.CreateContext(t, 3*time.Minute)
	mc := hp.CreateDefaultMilvusClient(ctx, t).Client
	name := prepareRLSCollection(ctx, t, mc, "", false)
	for _, value := range []bool{true, false} {
		common.CheckErr(t, mc.AlterCollectionProperties(ctx, client.NewAlterCollectionPropertiesOption(name).WithProperty("rls.enabled", value)), true)
	}
	queryRLS(ctx, t, mc, name, "", false, 1, 2, 3, 4)
	common.CheckErr(t, mc.DropCollectionProperties(ctx, client.NewDropCollectionPropertiesOption(name, "rls.enabled")), true)
	queryRLS(ctx, t, mc, name, "", false, 1, 2, 3, 4)
	common.CheckErr(t, mc.AlterCollectionProperties(ctx, client.NewAlterCollectionPropertiesOption(name).WithProperty("rls.enabled", true)), true)
	common.CheckErr(t, mc.AlterCollectionProperties(ctx, client.NewAlterCollectionPropertiesOption(name).WithProperty("RLS.ENABLED", false)), false, "invalid property key")
	common.CheckErr(t, runRLSAction(ctx, mc, name, "alice", false, milvuspb.RowPolicyAction_Query), false, "no applicable")
	info, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(name))
	common.CheckErr(t, err, true)
	disabled := common.GenRandomString("rls_disabled_", 8)
	common.CheckErr(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(disabled, info.Schema).WithConsistencyLevel(entity.ClStrong)), true)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		require.NoError(t, mc.DropCollection(cleanupCtx, client.NewDropCollectionOption(disabled)))
	})
	_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(disabled, rlsColumns([]int64{1}, []string{"alice"})...))
	common.CheckErr(t, err, true)
	common.CheckErr(t, mc.AlterCollectionProperties(ctx, client.NewAlterCollectionPropertiesOption(disabled).WithProperty("rls.force", true)), false, "requires")
	common.CheckErr(t, mc.AlterCollectionProperties(ctx, client.NewAlterCollectionPropertiesOption(disabled).WithProperty("rls.enabled", true)), true)
	for _, properties := range []map[string]any{
		{"rls.enabled": "invalid"}, {"rls.force": true}, {"RLS.ENABLED": true},
	} {
		invalid := common.GenRandomString("rls_invalid_", 8)
		// Register cleanup even if a regression unexpectedly accepts the request.
		t.Cleanup(func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			require.NoError(t, mc.DropCollection(cleanupCtx, client.NewDropCollectionOption(invalid)))
		})
		opt := client.NewCreateCollectionOption(invalid, info.Schema)
		for key, value := range properties {
			opt.WithProperty(key, value)
		}
		require.Error(t, mc.CreateCollection(ctx, opt))
	}
}

// names are not authorization identities across drop/recreate.
func TestRLSCollectionRecreation(t *testing.T) {
	ctx := hp.CreateContext(t, 4*time.Minute)
	mc := hp.CreateDefaultMilvusClient(ctx, t).Client
	name := prepareRLSCollection(ctx, t, mc, "", false)
	createRLSPolicy(ctx, t, mc, name, "department", "department == $current_principal_tags['department']", "", milvuspb.RowPolicyAction_Query)
	common.CheckErr(t, mc.SetRLSPrincipalTags(ctx, client.NewSetRLSPrincipalTagsOption(name, "alice", `{"department":"eng"}`)), true)
	queryRLS(ctx, t, mc, name, "alice", false, 1, 3, 4)
	old, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(name))
	common.CheckErr(t, err, true)
	common.CheckErr(t, mc.DropCollection(ctx, client.NewDropCollectionOption(name)), true)
	prepareRLSCollection(ctx, t, mc, name, false)
	fresh, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(name))
	common.CheckErr(t, err, true)
	require.NotEqual(t, old.ID, fresh.ID)
	policies, err := mc.ListRowPolicies(ctx, client.NewListRowPoliciesOption(name))
	common.CheckErr(t, err, true)
	require.Empty(t, policies)
	principals, err := mc.ListRLSPrincipals(ctx, client.NewListRLSPrincipalsOption(name))
	common.CheckErr(t, err, true)
	require.Empty(t, principals)
	common.CheckErr(t, runRLSAction(ctx, mc, name, "alice", false, milvuspb.RowPolicyAction_Query), false, "no applicable")
	createRLSPolicy(ctx, t, mc, name, "department", "department == $current_principal_tags['department']", "", milvuspb.RowPolicyAction_Query)
	queryRLS(ctx, t, mc, name, "alice", false)
	common.CheckErr(t, mc.SetRLSPrincipalTags(ctx, client.NewSetRLSPrincipalTagsOption(name, "alice", `{"department":"sales"}`)), true)
	queryRLS(ctx, t, mc, name, "alice", false, 2)
}

// reject schema changes that remove a policy dependency.
func TestRLSPolicySchemaDependencies(t *testing.T) {
	ctx := hp.CreateContext(t, 4*time.Minute)
	mc := hp.CreateDefaultMilvusClient(ctx, t).Client
	name := prepareRLSCollection(ctx, t, mc, "", false)
	createRLSPolicy(ctx, t, mc, name, "owner", "owner == $current_principal", "", milvuspb.RowPolicyAction_Query)
	common.CheckErr(t, mc.ReleaseCollection(ctx, client.NewReleaseCollectionOption(name)), true)
	common.CheckErr(t, mc.DropCollectionField(ctx, client.NewDropCollectionFieldOption(name, "owner")), false, "RLS")
	info, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(name))
	common.CheckErr(t, err, true)
	require.True(t, slices.ContainsFunc(info.Schema.Fields, func(f *entity.Field) bool { return f.Name == "owner" }))
	common.CheckErr(t, mc.DropRowPolicy(ctx, client.NewDropRowPolicyOption(name, "owner")), true)
	common.CheckErr(t, mc.DropCollectionField(ctx, client.NewDropCollectionFieldOption(name, "owner")), true)
	info, err = mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(name))
	common.CheckErr(t, err, true)
	require.False(t, slices.ContainsFunc(info.Schema.Fields, func(f *entity.Field) bool { return f.Name == "owner" }))
}

// identically named collections and principals in different databases are isolated.
func TestRLSDatabaseIsolation(t *testing.T) {
	ctx := hp.CreateContext(t, 5*time.Minute)
	admin := hp.CreateDefaultMilvusClient(ctx, t).Client
	name := common.GenRandomString("rls_db_collection_", 8)
	var clients []*client.Client
	for i := range 2 {
		db := common.GenRandomString("rls_db_", 8)
		common.CheckErr(t, admin.CreateDatabase(ctx, client.NewCreateDatabaseOption(db)), true)
		t.Cleanup(func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			require.NoError(t, admin.DropDatabase(cleanupCtx, client.NewDropDatabaseOption(db)))
		})
		mc := hp.CreateMilvusClient(ctx, t, &client.ClientConfig{DBName: db}).Client
		prepareRLSCollection(ctx, t, mc, name, false)
		createRLSPolicy(ctx, t, mc, name, "department", "department == $current_principal_tags['department']", "", milvuspb.RowPolicyAction_Query)
		tags := `{"department":"eng"}`
		if i == 1 {
			tags = `{"department":"sales"}`
		}
		common.CheckErr(t, mc.SetRLSPrincipalTags(ctx, client.NewSetRLSPrincipalTagsOption(name, "alice", tags)), true)
		clients = append(clients, mc)
	}
	for range 3 {
		queryRLS(ctx, t, clients[0], name, "alice", false, 1, 3, 4)
		queryRLS(ctx, t, clients[1], name, "alice", false, 2)
	}
	common.CheckErr(t, clients[0].DeleteRLSPrincipalTags(ctx, client.NewDeleteRLSPrincipalTagsOption(name, "alice")), true)
	queryRLS(ctx, t, clients[0], name, "alice", false)
	queryRLS(ctx, t, clients[1], name, "alice", false, 2)
}

// this case requires the default RLS admission quotas.
func TestRLSQuotaBoundaries(t *testing.T) {
	ctx := hp.CreateContext(t, 5*time.Minute)
	mc := hp.CreateDefaultMilvusClient(ctx, t).Client
	name := prepareRLSCollection(ctx, t, mc, "", false)
	policyName := strings.Repeat("p", 255)
	createRLSPolicy(ctx, t, mc, name, policyName, "owner == $current_principal", "", milvuspb.RowPolicyAction_Query)
	common.CheckErr(t, mc.CreateRowPolicy(ctx, client.NewCreateRowPolicyOption(name, policyName+"p", milvuspb.RowPolicyType_RowPolicyTypePermissive, milvuspb.RowPolicyAction_Query).WithUsingExpr("true")), false, "max length")
	// UTF-8 limits count bytes: 85 three-byte characters fit, 86 do not.
	principal := strings.Repeat("租", 85)
	common.CheckErr(t, mc.SetRLSPrincipalTags(ctx, client.NewSetRLSPrincipalTagsOption(name, principal, `{"a":"b"}`)), true)
	common.CheckErr(t, mc.SetRLSPrincipalTags(ctx, client.NewSetRLSPrincipalTagsOption(name, principal+"租", `{"a":"b"}`)), false, "max length")
	tags := make(map[string]string, 50)
	for i := range 50 {
		tags[fmt.Sprintf("key_%d", i)] = "value"
	}
	payload, err := json.Marshal(tags)
	require.NoError(t, err)
	common.CheckErr(t, mc.SetRLSPrincipalTags(ctx, client.NewSetRLSPrincipalTagsOption(name, "alice", string(payload))), true)
	common.CheckErr(t, mc.SetRLSPrincipalTags(ctx, client.NewSetRLSPrincipalTagsOption(name, "alice", `{"overflow":"value"}`)), false, "limit")
	stored, err := mc.GetRLSPrincipalTags(ctx, client.NewGetRLSPrincipalTagsOption(name, "alice"))
	common.CheckErr(t, err, true)
	require.JSONEq(t, string(payload), stored)
	common.CheckErr(t, mc.SetRLSPrincipalTags(ctx, client.NewSetRLSPrincipalTagsOption(name, "alice", `{"key_0":"replacement"}`)), true)
	// All policies use the same narrow predicate, so none grants broader access.
	for i := range 99 {
		createRLSPolicy(ctx, t, mc, name, fmt.Sprintf("policy_%d", i), "owner == $current_principal", "", milvuspb.RowPolicyAction_Query)
	}
	common.CheckErr(t, mc.CreateRowPolicy(ctx, client.NewCreateRowPolicyOption(name, "overflow", milvuspb.RowPolicyType_RowPolicyTypePermissive, milvuspb.RowPolicyAction_Query).WithUsingExpr("true")), false, "limit")
	policies, err := mc.ListRowPolicies(ctx, client.NewListRowPoliciesOption(name))
	common.CheckErr(t, err, true)
	require.Len(t, policies, 100)
	queryRLS(ctx, t, mc, name, "alice", false, 1, 3)
	common.CheckErr(t, mc.DropRowPolicy(ctx, client.NewDropRowPolicyOption(name, "policy_0")), true)
	createRLSPolicy(ctx, t, mc, name, "replacement", "owner == $current_principal", "", milvuspb.RowPolicyAction_Query)
	queryRLS(ctx, t, mc, name, "alice", false, 1, 3)
}
