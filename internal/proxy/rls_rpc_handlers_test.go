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

package proxy

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proxy/privilege"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/crypto"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// contextWithUser creates an incoming gRPC context that carries the given username.
func contextWithUser(username string) context.Context {
	authKey := strings.ToLower(util.HeaderAuthorize)
	authValue := crypto.Base64Encode(username + util.CredentialSeparator + "password")
	md := metadata.New(map[string]string{authKey: authValue})
	return metadata.NewIncomingContext(context.Background(), md)
}

// ---------- convertMilvuspbToInternalCreateRowPolicy ----------

func TestConvertMilvuspbToInternalCreateRowPolicy_ValidInput(t *testing.T) {
	req := &milvuspb.CreateRowPolicyRequest{
		Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_Undefined},
		DbName:         "testdb",
		PolicyName:     "policy1",
		CollectionName: "coll1",
		Actions:        []milvuspb.RowPolicyAction{milvuspb.RowPolicyAction_Query, milvuspb.RowPolicyAction_Search},
		Roles:          []string{"role1", "role2"},
		UsingExpr:      "id > 10",
		CheckExpr:      "status == 'active'",
		Description:    "test policy",
	}

	result := convertMilvuspbToInternalCreateRowPolicy(req)
	require.NotNil(t, result)
	assert.Equal(t, "testdb", result.GetDbName())
	assert.NotNil(t, result.GetPolicy())

	policy := result.GetPolicy()
	assert.Equal(t, "policy1", policy.GetPolicyName())
	assert.Equal(t, "coll1", policy.GetCollectionName())
	assert.Equal(t, messagespb.RLSPolicyType_PERMISSIVE, policy.GetPolicyType())
	assert.Equal(t, []string{string(model.RLSActionQuery), string(model.RLSActionSearch)}, policy.GetActions())
	assert.Equal(t, []string{"role1", "role2"}, policy.GetRoles())
	assert.Equal(t, "id > 10", policy.GetUsingExpr())
	assert.Equal(t, "status == 'active'", policy.GetCheckExpr())
	assert.Equal(t, "test policy", policy.GetDescription())
}

func TestConvertMilvuspbToInternalCreateRowPolicy_AllActions(t *testing.T) {
	req := &milvuspb.CreateRowPolicyRequest{
		DbName:     "db",
		PolicyName: "p",
		Actions: []milvuspb.RowPolicyAction{
			milvuspb.RowPolicyAction_Query,
			milvuspb.RowPolicyAction_Search,
			milvuspb.RowPolicyAction_Insert,
			milvuspb.RowPolicyAction_Delete,
			milvuspb.RowPolicyAction_Upsert,
		},
	}

	result := convertMilvuspbToInternalCreateRowPolicy(req)
	require.NotNil(t, result)
	expected := []string{
		string(model.RLSActionQuery),
		string(model.RLSActionSearch),
		string(model.RLSActionInsert),
		string(model.RLSActionDelete),
		string(model.RLSActionUpsert),
	}
	assert.Equal(t, expected, result.GetPolicy().GetActions())
}

func TestConvertMilvuspbToInternalCreateRowPolicy_UnknownActionsIgnored(t *testing.T) {
	req := &milvuspb.CreateRowPolicyRequest{
		DbName:     "db",
		PolicyName: "p",
		// Use a numeric value that does not map to any known action
		Actions: []milvuspb.RowPolicyAction{milvuspb.RowPolicyAction(999)},
	}

	result := convertMilvuspbToInternalCreateRowPolicy(req)
	require.NotNil(t, result)
	assert.Empty(t, result.GetPolicy().GetActions())
}

func TestConvertMilvuspbToInternalCreateRowPolicy_NilInput(t *testing.T) {
	result := convertMilvuspbToInternalCreateRowPolicy(nil)
	assert.Nil(t, result)
}

func TestConvertMilvuspbToInternalCreateRowPolicy_EmptyFields(t *testing.T) {
	req := &milvuspb.CreateRowPolicyRequest{}

	result := convertMilvuspbToInternalCreateRowPolicy(req)
	require.NotNil(t, result)
	assert.Equal(t, "", result.GetDbName())
	assert.NotNil(t, result.GetPolicy())
	assert.Equal(t, "", result.GetPolicy().GetPolicyName())
	assert.Empty(t, result.GetPolicy().GetActions())
	assert.Empty(t, result.GetPolicy().GetRoles())
}

// ---------- convertMilvuspbToInternalDropRowPolicy ----------

func TestConvertMilvuspbToInternalDropRowPolicy_ValidInput(t *testing.T) {
	req := &milvuspb.DropRowPolicyRequest{
		Base:           &commonpb.MsgBase{},
		DbName:         "testdb",
		CollectionName: "coll1",
		PolicyName:     "policy1",
	}

	result := convertMilvuspbToInternalDropRowPolicy(req)
	require.NotNil(t, result)
	assert.Equal(t, "testdb", result.GetDbName())
	assert.Equal(t, "coll1", result.GetCollectionName())
	assert.Equal(t, "policy1", result.GetPolicyName())
}

func TestConvertMilvuspbToInternalDropRowPolicy_NilInput(t *testing.T) {
	result := convertMilvuspbToInternalDropRowPolicy(nil)
	assert.Nil(t, result)
}

func TestConvertMilvuspbToInternalDropRowPolicy_EmptyFields(t *testing.T) {
	req := &milvuspb.DropRowPolicyRequest{}

	result := convertMilvuspbToInternalDropRowPolicy(req)
	require.NotNil(t, result)
	assert.Equal(t, "", result.GetDbName())
	assert.Equal(t, "", result.GetCollectionName())
	assert.Equal(t, "", result.GetPolicyName())
}

// ---------- convertMilvuspbToInternalListRowPolicies ----------

func TestConvertMilvuspbToInternalListRowPolicies_ValidInput(t *testing.T) {
	req := &milvuspb.ListRowPoliciesRequest{
		Base:           &commonpb.MsgBase{},
		DbName:         "testdb",
		CollectionName: "coll1",
	}

	result := convertMilvuspbToInternalListRowPolicies(req)
	require.NotNil(t, result)
	assert.Equal(t, "testdb", result.GetDbName())
	assert.Equal(t, "coll1", result.GetCollectionName())
}

func TestConvertMilvuspbToInternalListRowPolicies_NilInput(t *testing.T) {
	result := convertMilvuspbToInternalListRowPolicies(nil)
	assert.Nil(t, result)
}

func TestConvertMilvuspbToInternalListRowPolicies_EmptyFields(t *testing.T) {
	req := &milvuspb.ListRowPoliciesRequest{}

	result := convertMilvuspbToInternalListRowPolicies(req)
	require.NotNil(t, result)
	assert.Equal(t, "", result.GetDbName())
	assert.Equal(t, "", result.GetCollectionName())
}

// ---------- convertInternalPolicyToMilvuspbPolicy ----------

func TestConvertInternalPolicyToMilvuspbPolicy_ValidPolicy(t *testing.T) {
	internal := &messagespb.RLSPolicy{
		PolicyName:  "policy1",
		Actions:     []string{"Query", "Search"},
		Roles:       []string{"role1"},
		UsingExpr:   "id > 10",
		CheckExpr:   "status == 'ok'",
		Description: "a policy",
		CreatedAt:   12345,
	}

	result := convertInternalPolicyToMilvuspbPolicy(internal)
	require.NotNil(t, result)
	assert.Equal(t, "policy1", result.GetPolicyName())
	assert.Equal(t, []milvuspb.RowPolicyAction{
		milvuspb.RowPolicyAction_Query,
		milvuspb.RowPolicyAction_Search,
	}, result.GetActions())
	assert.Equal(t, []string{"role1"}, result.GetRoles())
	assert.Equal(t, "id > 10", result.GetUsingExpr())
	assert.Equal(t, "status == 'ok'", result.GetCheckExpr())
	assert.Equal(t, "a policy", result.GetDescription())
	assert.Equal(t, int64(12345), result.GetCreatedAt())
}

func TestConvertInternalPolicyToMilvuspbPolicy_NilPolicy(t *testing.T) {
	result := convertInternalPolicyToMilvuspbPolicy(nil)
	assert.Nil(t, result)
}

func TestConvertInternalPolicyToMilvuspbPolicy_AllActionTypes(t *testing.T) {
	tests := []struct {
		name     string
		action   string
		expected milvuspb.RowPolicyAction
	}{
		{"query lowercase", "query", milvuspb.RowPolicyAction_Query},
		{"Query capitalized", "Query", milvuspb.RowPolicyAction_Query},
		{"search lowercase", "search", milvuspb.RowPolicyAction_Search},
		{"Search capitalized", "Search", milvuspb.RowPolicyAction_Search},
		{"insert lowercase", "insert", milvuspb.RowPolicyAction_Insert},
		{"Insert capitalized", "Insert", milvuspb.RowPolicyAction_Insert},
		{"delete lowercase", "delete", milvuspb.RowPolicyAction_Delete},
		{"Delete capitalized", "Delete", milvuspb.RowPolicyAction_Delete},
		{"upsert lowercase", "upsert", milvuspb.RowPolicyAction_Upsert},
		{"Upsert capitalized", "Upsert", milvuspb.RowPolicyAction_Upsert},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			internal := &messagespb.RLSPolicy{
				PolicyName: "p",
				Actions:    []string{tt.action},
			}
			result := convertInternalPolicyToMilvuspbPolicy(internal)
			require.NotNil(t, result)
			require.Len(t, result.GetActions(), 1)
			assert.Equal(t, tt.expected, result.GetActions()[0])
		})
	}
}

func TestConvertInternalPolicyToMilvuspbPolicy_UnknownActionsSkipped(t *testing.T) {
	internal := &messagespb.RLSPolicy{
		PolicyName: "p",
		Actions:    []string{"unknown_action", "QUERY"},
	}

	result := convertInternalPolicyToMilvuspbPolicy(internal)
	require.NotNil(t, result)
	// Both "unknown_action" and "QUERY" (wrong case) should be skipped
	assert.Empty(t, result.GetActions())
}

// ---------- convertInternalToMilvuspbListRowPoliciesResponse ----------

func TestConvertInternalToMilvuspbListRowPoliciesResponse_ValidResponse(t *testing.T) {
	resp := &messagespb.ListRowPoliciesResponse{
		Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		Policies: []*messagespb.RLSPolicy{
			{
				PolicyName: "p1",
				Actions:    []string{"Query"},
				Roles:      []string{"admin"},
				UsingExpr:  "a > 1",
			},
			{
				PolicyName: "p2",
				Actions:    []string{"Search", "Delete"},
				Roles:      []string{"reader"},
				UsingExpr:  "b < 10",
			},
		},
	}

	result := convertInternalToMilvuspbListRowPoliciesResponse(resp, "mydb", "mycoll")
	require.NotNil(t, result)
	assert.Equal(t, commonpb.ErrorCode_Success, result.GetStatus().GetErrorCode())
	assert.Equal(t, "mydb", result.GetDbName())
	assert.Equal(t, "mycoll", result.GetCollectionName())
	require.Len(t, result.GetPolicies(), 2)
	assert.Equal(t, "p1", result.GetPolicies()[0].GetPolicyName())
	assert.Equal(t, "p2", result.GetPolicies()[1].GetPolicyName())
}

func TestConvertInternalToMilvuspbListRowPoliciesResponse_EmptyPolicies(t *testing.T) {
	resp := &messagespb.ListRowPoliciesResponse{
		Status:   &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		Policies: []*messagespb.RLSPolicy{},
	}

	result := convertInternalToMilvuspbListRowPoliciesResponse(resp, "db", "coll")
	require.NotNil(t, result)
	assert.Empty(t, result.GetPolicies())
	assert.Equal(t, "db", result.GetDbName())
	assert.Equal(t, "coll", result.GetCollectionName())
}

func TestConvertInternalToMilvuspbListRowPoliciesResponse_NilResponse(t *testing.T) {
	result := convertInternalToMilvuspbListRowPoliciesResponse(nil, "db", "coll")
	require.NotNil(t, result)
	// Returns an empty response, not nil
	assert.Empty(t, result.GetPolicies())
}

func TestConvertInternalToMilvuspbListRowPoliciesResponse_NilPoliciesInList(t *testing.T) {
	resp := &messagespb.ListRowPoliciesResponse{
		Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		Policies: []*messagespb.RLSPolicy{
			nil,
			{PolicyName: "p1", Actions: []string{"Query"}},
			nil,
		},
	}

	result := convertInternalToMilvuspbListRowPoliciesResponse(resp, "db", "coll")
	require.NotNil(t, result)
	// nil policies should be skipped
	require.Len(t, result.GetPolicies(), 1)
	assert.Equal(t, "p1", result.GetPolicies()[0].GetPolicyName())
}

// ---------- convertProtoToModelRLSPolicy ----------

func TestConvertProtoToModelRLSPolicy_ValidPermissive(t *testing.T) {
	proto := &messagespb.RLSPolicy{
		PolicyName: "policy1",
		PolicyType: messagespb.RLSPolicyType_PERMISSIVE,
		Actions:    []string{"query", "search"},
		Roles:      []string{"role1"},
		UsingExpr:  "id > 10",
		CheckExpr:  "status == 'ok'",
	}

	result := convertProtoToModelRLSPolicy(proto, 100, 200)
	require.NotNil(t, result)
	assert.Equal(t, "policy1", result.PolicyName)
	assert.Equal(t, int64(100), result.CollectionID)
	assert.Equal(t, int64(200), result.DBID)
	assert.Equal(t, model.RLSPolicyTypePermissive, result.PolicyType)
	assert.Equal(t, []string{"query", "search"}, result.Actions)
	assert.Equal(t, []string{"role1"}, result.Roles)
	assert.Equal(t, "id > 10", result.UsingExpr)
	assert.Equal(t, "status == 'ok'", result.CheckExpr)
}

func TestConvertProtoToModelRLSPolicy_ValidRestrictive(t *testing.T) {
	proto := &messagespb.RLSPolicy{
		PolicyName: "policy2",
		PolicyType: messagespb.RLSPolicyType_RESTRICTIVE,
		Actions:    []string{"insert"},
	}

	result := convertProtoToModelRLSPolicy(proto, 1, 2)
	require.NotNil(t, result)
	assert.Equal(t, model.RLSPolicyTypeRestrictive, result.PolicyType)
}

func TestConvertProtoToModelRLSPolicy_NilProto(t *testing.T) {
	result := convertProtoToModelRLSPolicy(nil, 100, 200)
	assert.Nil(t, result)
}

// ---------- requireAdminForRLS ----------

func TestRequireAdminForRLS_AuthDisabled(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(Params.CommonCfg.AuthorizationEnabled.Key, "false")
	defer paramtable.Get().Reset(Params.CommonCfg.AuthorizationEnabled.Key)

	// When authorization is disabled, anyone should be allowed
	err := requireAdminForRLS(context.Background())
	assert.NoError(t, err)
}

func TestRequireAdminForRLS_NoUserInContext(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
	defer paramtable.Get().Reset(Params.CommonCfg.AuthorizationEnabled.Key)

	// No user metadata in context should fail
	err := requireAdminForRLS(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "authentication")
}

func TestRequireAdminForRLS_RootUser(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
	defer paramtable.Get().Reset(Params.CommonCfg.AuthorizationEnabled.Key)

	ctx := contextWithUser(util.UserRoot)
	err := requireAdminForRLS(ctx)
	assert.NoError(t, err)
}

func TestRequireAdminForRLS_AdminRole(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
	defer paramtable.Get().Reset(Params.CommonCfg.AuthorizationEnabled.Key)

	// Set up a privilege cache with admin role for "testuser"
	pc := privilege.NewPrivilegeCache(nil)
	pc.InitPolicyInfo(nil, []string{"testuser/" + util.RoleAdmin})
	defer privilege.ResetPrivilegeCacheForTest()

	ctx := contextWithUser("testuser")
	err := requireAdminForRLS(ctx)
	assert.NoError(t, err)
}

func TestRequireAdminForRLS_NonAdminUser(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
	defer paramtable.Get().Reset(Params.CommonCfg.AuthorizationEnabled.Key)

	// Set up a privilege cache with a non-admin role
	pc := privilege.NewPrivilegeCache(nil)
	pc.InitPolicyInfo(nil, []string{"normaluser/reader"})
	defer privilege.ResetPrivilegeCacheForTest()

	ctx := contextWithUser("normaluser")
	err := requireAdminForRLS(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not authorized")
}

func TestRequireAdminForRLS_NoCacheAvailable(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
	defer paramtable.Get().Reset(Params.CommonCfg.AuthorizationEnabled.Key)

	// Ensure no privilege cache is set
	privilege.ResetPrivilegeCacheForTest()

	ctx := contextWithUser("someuser")
	err := requireAdminForRLS(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get roles")
}

func TestRequireAdminForRLS_EmptyUserInContext(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
	defer paramtable.Get().Reset(Params.CommonCfg.AuthorizationEnabled.Key)

	// Create a context with metadata but no valid user (empty auth value)
	md := metadata.New(map[string]string{
		strings.ToLower(util.HeaderAuthorize): crypto.Base64Encode(""),
	})
	ctx := metadata.NewIncomingContext(context.Background(), md)

	err := requireAdminForRLS(ctx)
	assert.Error(t, err)
}
