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

//go:build test
// +build test

package proxy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// --- helpers ---

// setupRLSTestEnv initializes paramtable, sets AuthorizationEnabled=true,
// creates a privilege cache so GetRole works, and sets up the global RLS
// manager with interceptors. It returns a cleanup function.
func setupRLSTestEnv(t *testing.T) func() {
	t.Helper()

	paramtable.Init()
	Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")

	// Use the test-only helper to set up globalMetaCache + privilege cache.
	InitEmptyGlobalCache()

	// Give the root user the admin role so GetRole("root") returns ["admin"].
	AddRootUserToAdminRole()

	origConfig := GetRLSConfig()
	origManager := GetRLSManager()

	return func() {
		RemoveRootUserFromAdminRole()
		Params.Reset(Params.CommonCfg.AuthorizationEnabled.Key)
		SetRLSConfig(origConfig)
		// Restore original global manager.
		globalRLSManagerMu.Lock()
		globalRLSManager = origManager
		globalRLSManagerMu.Unlock()
	}
}

// enabledPermissiveConfig returns an RLSConfig with mode=permissive, enabled.
func enabledPermissiveConfig() *RLSConfig {
	return &RLSConfig{
		Enabled:                  true,
		Mode:                     RLSModePermissive,
		AuditEnabled:             false,
		CacheExpirationSeconds:   3600,
		MaxCacheEntries:          10000,
		MaxPoliciesPerCollection: 100,
		MaxUserTags:              50,
		MaxExpressionLength:      4096,
	}
}

// enabledStrictConfig returns an RLSConfig with mode=strict, enabled.
func enabledStrictConfig() *RLSConfig {
	return &RLSConfig{
		Enabled:                  true,
		Mode:                     RLSModeStrict,
		AuditEnabled:             false,
		CacheExpirationSeconds:   3600,
		MaxCacheEntries:          10000,
		MaxPoliciesPerCollection: 100,
		MaxUserTags:              50,
		MaxExpressionLength:      4096,
	}
}

// initManagerWithPolicies sets up a global RLS manager with the given context
// provider and policies for dbID=1, collectionID=100.
func initManagerWithPolicies(provider ContextProvider, policies []*model.RLSPolicy) *RLSManager {
	mgr := InitRLSManager()
	mgr.SetContextProvider(provider)
	if policies != nil {
		mgr.GetCache().UpdatePolicies(1, 100, policies)
	}
	return mgr
}

// ctxForUser builds a context carrying user identity metadata so that
// GetCurUserFromContext returns the given username.
func ctxForUser(username string) context.Context {
	return NewContextWithMetadata(context.Background(), username, "default")
}

// --- isRLSExemptUser tests ---

func TestIsRLSExemptUser_RootUserIsExempt(t *testing.T) {
	cleanup := setupRLSTestEnv(t)
	defer cleanup()

	ctx := ctxForUser(util.UserRoot)
	assert.True(t, isRLSExemptUser(ctx), "root user should be exempt")
}

func TestIsRLSExemptUser_AdminRoleIsExempt(t *testing.T) {
	cleanup := setupRLSTestEnv(t)
	defer cleanup()

	// root already has the admin role from setupRLSTestEnv.
	ctx := ctxForUser(util.UserRoot)
	assert.True(t, isRLSExemptUser(ctx))
}

func TestIsRLSExemptUser_EmptyContext(t *testing.T) {
	cleanup := setupRLSTestEnv(t)
	defer cleanup()

	// A bare context has no user identity.
	assert.False(t, isRLSExemptUser(context.Background()))
}

func TestIsRLSExemptUser_NonAdminUser(t *testing.T) {
	cleanup := setupRLSTestEnv(t)
	defer cleanup()

	// "alice" exists in the context but has no admin role.
	ctx := ctxForUser("alice")
	assert.False(t, isRLSExemptUser(ctx), "non-admin user should not be exempt")
}

// --- applyRLSFilter tests ---

func TestApplyRLSFilter_NilManager(t *testing.T) {
	cleanup := setupRLSTestEnv(t)
	defer cleanup()

	// Clear global manager.
	globalRLSManagerMu.Lock()
	globalRLSManager = nil
	globalRLSManagerMu.Unlock()

	result, err := applyRLSFilter(context.Background(), "default", "coll", 100, "age > 18")
	require.NoError(t, err)
	assert.Equal(t, "age > 18", result)
}

func TestApplyRLSFilter_Disabled(t *testing.T) {
	cleanup := setupRLSTestEnv(t)
	defer cleanup()

	SetRLSConfig(DefaultRLSConfig()) // disabled by default
	initManagerWithPolicies(NewSimpleContextProvider("alice", []string{"PUBLIC"}), nil)

	result, err := applyRLSFilter(ctxForUser("alice"), "default", "coll", 100, "age > 18")
	require.NoError(t, err)
	assert.Equal(t, "age > 18", result)
}

func TestApplyRLSFilter_AdminBypass(t *testing.T) {
	cleanup := setupRLSTestEnv(t)
	defer cleanup()

	SetRLSConfig(enabledPermissiveConfig())
	policies := []*model.RLSPolicy{
		model.NewRLSPolicy("p1", 100, 1, model.RLSPolicyTypePermissive,
			[]string{"query"}, []string{"PUBLIC"},
			"owner_id == $current_user_name", "", "policy"),
	}
	initManagerWithPolicies(NewSimpleContextProvider(util.UserRoot, []string{"admin"}), policies)

	// root user should bypass and get original expression back.
	result, err := applyRLSFilter(ctxForUser(util.UserRoot), "default", "coll", 100, "age > 18")
	require.NoError(t, err)
	assert.Equal(t, "age > 18", result)
}

func TestApplyRLSFilter_SuccessfulFilter(t *testing.T) {
	cleanup := setupRLSTestEnv(t)
	defer cleanup()

	SetRLSConfig(enabledPermissiveConfig())
	policies := []*model.RLSPolicy{
		model.NewRLSPolicy("p1", 100, 1, model.RLSPolicyTypePermissive,
			[]string{"query"}, []string{"PUBLIC"},
			"owner_id == $current_user_name", "", "policy"),
	}
	initManagerWithPolicies(NewSimpleContextProvider("alice", []string{"PUBLIC"}), policies)

	result, err := applyRLSFilter(ctxForUser("alice"), "default", "coll", 100, "age > 18")
	require.NoError(t, err)
	assert.Contains(t, result, "age > 18")
	assert.Contains(t, result, "owner_id == 'alice'")
}

func TestApplyRLSFilter_NoPolicies_DenyDefault(t *testing.T) {
	cleanup := setupRLSTestEnv(t)
	defer cleanup()

	SetRLSConfig(enabledPermissiveConfig())
	// No policies configured for this collection.
	initManagerWithPolicies(NewSimpleContextProvider("alice", []string{"PUBLIC"}), nil)

	result, err := applyRLSFilter(ctxForUser("alice"), "default", "coll", 100, "age > 18")
	require.NoError(t, err)
	// With no policies the interceptor returns "false" (deny by default).
	assert.Equal(t, "false", result)
}

func TestApplyRLSFilter_NilInterceptor(t *testing.T) {
	cleanup := setupRLSTestEnv(t)
	defer cleanup()

	SetRLSConfig(enabledPermissiveConfig())
	// Manager without interceptors (no SetContextProvider call).
	mgr := InitRLSManager()
	_ = mgr

	result, err := applyRLSFilter(ctxForUser("alice"), "default", "coll", 100, "age > 18")
	require.NoError(t, err)
	assert.Equal(t, "age > 18", result)
}

// --- applyRLSDeleteFilter tests ---

func TestApplyRLSDeleteFilter_Disabled(t *testing.T) {
	cleanup := setupRLSTestEnv(t)
	defer cleanup()

	SetRLSConfig(DefaultRLSConfig())
	initManagerWithPolicies(NewSimpleContextProvider("alice", []string{"PUBLIC"}), nil)

	result, err := applyRLSDeleteFilter(ctxForUser("alice"), "default", "coll", 100, "status == 'old'")
	require.NoError(t, err)
	assert.Equal(t, "status == 'old'", result)
}

func TestApplyRLSDeleteFilter_SuccessfulFilter(t *testing.T) {
	cleanup := setupRLSTestEnv(t)
	defer cleanup()

	SetRLSConfig(enabledPermissiveConfig())
	policies := []*model.RLSPolicy{
		model.NewRLSPolicy("dp1", 100, 1, model.RLSPolicyTypePermissive,
			[]string{"delete"}, []string{"PUBLIC"},
			"owner_id == $current_user_name", "", "delete policy"),
	}
	initManagerWithPolicies(NewSimpleContextProvider("alice", []string{"PUBLIC"}), policies)

	result, err := applyRLSDeleteFilter(ctxForUser("alice"), "default", "coll", 100, "status == 'old'")
	require.NoError(t, err)
	assert.Contains(t, result, "status == 'old'")
	assert.Contains(t, result, "owner_id == 'alice'")
}

func TestApplyRLSDeleteFilter_AdminBypass(t *testing.T) {
	cleanup := setupRLSTestEnv(t)
	defer cleanup()

	SetRLSConfig(enabledPermissiveConfig())
	policies := []*model.RLSPolicy{
		model.NewRLSPolicy("dp1", 100, 1, model.RLSPolicyTypePermissive,
			[]string{"delete"}, []string{"PUBLIC"},
			"owner_id == $current_user_name", "", "delete policy"),
	}
	initManagerWithPolicies(NewSimpleContextProvider(util.UserRoot, []string{"admin"}), policies)

	result, err := applyRLSDeleteFilter(ctxForUser(util.UserRoot), "default", "coll", 100, "status == 'old'")
	require.NoError(t, err)
	assert.Equal(t, "status == 'old'", result)
}

// --- applyRLSSearchFilter tests ---

func TestApplyRLSSearchFilter_Disabled(t *testing.T) {
	cleanup := setupRLSTestEnv(t)
	defer cleanup()

	SetRLSConfig(DefaultRLSConfig())
	initManagerWithPolicies(NewSimpleContextProvider("alice", []string{"PUBLIC"}), nil)

	result, err := applyRLSSearchFilter(ctxForUser("alice"), "default", "coll", 100, "color == 'red'")
	require.NoError(t, err)
	assert.Equal(t, "color == 'red'", result)
}

func TestApplyRLSSearchFilter_SuccessfulFilter(t *testing.T) {
	cleanup := setupRLSTestEnv(t)
	defer cleanup()

	SetRLSConfig(enabledPermissiveConfig())
	policies := []*model.RLSPolicy{
		model.NewRLSPolicy("sp1", 100, 1, model.RLSPolicyTypePermissive,
			[]string{"search"}, []string{"PUBLIC"},
			"owner_id == $current_user_name", "", "search policy"),
	}
	initManagerWithPolicies(NewSimpleContextProvider("alice", []string{"PUBLIC"}), policies)

	result, err := applyRLSSearchFilter(ctxForUser("alice"), "default", "coll", 100, "color == 'red'")
	require.NoError(t, err)
	assert.Contains(t, result, "color == 'red'")
	assert.Contains(t, result, "owner_id == 'alice'")
}

// --- applyRLSInsertCheck tests ---

func TestApplyRLSInsertCheck_Disabled(t *testing.T) {
	cleanup := setupRLSTestEnv(t)
	defer cleanup()

	SetRLSConfig(DefaultRLSConfig())
	initManagerWithPolicies(NewSimpleContextProvider("alice", []string{"PUBLIC"}), nil)

	err := applyRLSInsertCheck(ctxForUser("alice"), "default", "coll", 100)
	assert.NoError(t, err)
}

func TestApplyRLSInsertCheck_AdminBypass(t *testing.T) {
	cleanup := setupRLSTestEnv(t)
	defer cleanup()

	SetRLSConfig(enabledPermissiveConfig())
	// Policy denies all non-admin inserts.
	policies := []*model.RLSPolicy{
		model.NewRLSPolicy("ip1", 100, 1, model.RLSPolicyTypePermissive,
			[]string{"insert"}, []string{"admin"},
			"", "true", "admin only insert"),
	}
	initManagerWithPolicies(NewSimpleContextProvider(util.UserRoot, []string{"admin"}), policies)

	err := applyRLSInsertCheck(ctxForUser(util.UserRoot), "default", "coll", 100)
	assert.NoError(t, err)
}

func TestApplyRLSInsertCheck_Allowed(t *testing.T) {
	cleanup := setupRLSTestEnv(t)
	defer cleanup()

	SetRLSConfig(enabledPermissiveConfig())
	policies := []*model.RLSPolicy{
		model.NewRLSPolicy("ip1", 100, 1, model.RLSPolicyTypePermissive,
			[]string{"insert"}, []string{"PUBLIC"},
			"", "owner_id == $current_user_name", "insert policy"),
	}
	initManagerWithPolicies(NewSimpleContextProvider("alice", []string{"PUBLIC"}), policies)

	err := applyRLSInsertCheck(ctxForUser("alice"), "default", "coll", 100)
	assert.NoError(t, err)
}

func TestApplyRLSInsertCheck_Denied_StrictMode(t *testing.T) {
	cleanup := setupRLSTestEnv(t)
	defer cleanup()

	SetRLSConfig(enabledStrictConfig())
	// Policy only allows admin role; alice has "employee" role.
	policies := []*model.RLSPolicy{
		model.NewRLSPolicy("ip1", 100, 1, model.RLSPolicyTypePermissive,
			[]string{"insert"}, []string{"admin"},
			"", "true", "admin only insert"),
	}
	initManagerWithPolicies(NewSimpleContextProvider("alice", []string{"employee"}), policies)

	err := applyRLSInsertCheck(ctxForUser("alice"), "default", "coll", 100)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "denied")
}

func TestApplyRLSInsertCheck_Denied_PermissiveMode(t *testing.T) {
	cleanup := setupRLSTestEnv(t)
	defer cleanup()

	SetRLSConfig(enabledPermissiveConfig())
	// Policy only allows admin role; alice has "employee" role.
	// The interceptor returns an error, but permissive mode swallows it.
	policies := []*model.RLSPolicy{
		model.NewRLSPolicy("ip1", 100, 1, model.RLSPolicyTypePermissive,
			[]string{"insert"}, []string{"admin"},
			"", "true", "admin only insert"),
	}
	initManagerWithPolicies(NewSimpleContextProvider("alice", []string{"employee"}), policies)

	err := applyRLSInsertCheck(ctxForUser("alice"), "default", "coll", 100)
	// Permissive mode: the interceptor denies (no matching role), but the
	// denial itself is a policy outcome, not a runtime error. The interceptor
	// returns an error for denied inserts, and permissive mode swallows it.
	assert.NoError(t, err)
}

func TestApplyRLSInsertCheck_NilManager(t *testing.T) {
	cleanup := setupRLSTestEnv(t)
	defer cleanup()

	globalRLSManagerMu.Lock()
	globalRLSManager = nil
	globalRLSManagerMu.Unlock()

	err := applyRLSInsertCheck(ctxForUser("alice"), "default", "coll", 100)
	assert.NoError(t, err)
}

// --- applyRLSUpsertCheck tests ---

func TestApplyRLSUpsertCheck_Disabled(t *testing.T) {
	cleanup := setupRLSTestEnv(t)
	defer cleanup()

	SetRLSConfig(DefaultRLSConfig())
	initManagerWithPolicies(NewSimpleContextProvider("alice", []string{"PUBLIC"}), nil)

	err := applyRLSUpsertCheck(ctxForUser("alice"), "default", "coll", 100)
	assert.NoError(t, err)
}

func TestApplyRLSUpsertCheck_AdminBypass(t *testing.T) {
	cleanup := setupRLSTestEnv(t)
	defer cleanup()

	SetRLSConfig(enabledPermissiveConfig())
	policies := []*model.RLSPolicy{
		model.NewRLSPolicy("up1", 100, 1, model.RLSPolicyTypePermissive,
			[]string{"insert"}, []string{"admin"},
			"", "true", "admin only upsert"),
	}
	initManagerWithPolicies(NewSimpleContextProvider(util.UserRoot, []string{"admin"}), policies)

	err := applyRLSUpsertCheck(ctxForUser(util.UserRoot), "default", "coll", 100)
	assert.NoError(t, err)
}

func TestApplyRLSUpsertCheck_Denied_StrictMode(t *testing.T) {
	cleanup := setupRLSTestEnv(t)
	defer cleanup()

	SetRLSConfig(enabledStrictConfig())
	policies := []*model.RLSPolicy{
		model.NewRLSPolicy("up1", 100, 1, model.RLSPolicyTypePermissive,
			[]string{"insert"}, []string{"admin"},
			"", "true", "admin only upsert"),
	}
	initManagerWithPolicies(NewSimpleContextProvider("alice", []string{"employee"}), policies)

	err := applyRLSUpsertCheck(ctxForUser("alice"), "default", "coll", 100)
	assert.Error(t, err)
}

func TestApplyRLSUpsertCheck_NilManager(t *testing.T) {
	cleanup := setupRLSTestEnv(t)
	defer cleanup()

	globalRLSManagerMu.Lock()
	globalRLSManager = nil
	globalRLSManagerMu.Unlock()

	err := applyRLSUpsertCheck(ctxForUser("alice"), "default", "coll", 100)
	assert.NoError(t, err)
}

// --- Strict vs Permissive mode for query filter ---

func TestApplyRLSFilter_StrictMode_NoPolicies_Denied(t *testing.T) {
	cleanup := setupRLSTestEnv(t)
	defer cleanup()

	SetRLSConfig(enabledStrictConfig())
	// No policies => interceptor returns "false".
	initManagerWithPolicies(NewSimpleContextProvider("alice", []string{"PUBLIC"}), nil)

	result, err := applyRLSFilter(ctxForUser("alice"), "default", "coll", 100, "age > 18")
	require.NoError(t, err)
	// "false" is the deny-by-default from the interceptor (not an error).
	assert.Equal(t, "false", result)
}

func TestApplyRLSFilter_EmptyOriginalExpr(t *testing.T) {
	cleanup := setupRLSTestEnv(t)
	defer cleanup()

	SetRLSConfig(enabledPermissiveConfig())
	policies := []*model.RLSPolicy{
		model.NewRLSPolicy("p1", 100, 1, model.RLSPolicyTypePermissive,
			[]string{"query"}, []string{"PUBLIC"},
			"owner_id == $current_user_name", "", "policy"),
	}
	initManagerWithPolicies(NewSimpleContextProvider("alice", []string{"PUBLIC"}), policies)

	result, err := applyRLSFilter(ctxForUser("alice"), "default", "coll", 100, "")
	require.NoError(t, err)
	assert.Contains(t, result, "owner_id == 'alice'")
}

// --- getDBIDForRLS tests ---

func TestGetDBIDForRLS_NilMetaCache(t *testing.T) {
	oldCache := globalMetaCache
	globalMetaCache = nil
	defer func() { globalMetaCache = oldCache }()

	assert.Equal(t, int64(0), getDBIDForRLS(context.Background(), "default"))
}

func TestGetDBIDForRLS_WithMetaCache(t *testing.T) {
	cleanup := setupRLSTestEnv(t)
	defer cleanup()

	// InitEmptyGlobalCache sets up a mock that returns errors for most calls.
	// getDBIDForRLS should gracefully return 0 on error.
	dbID := getDBIDForRLS(context.Background(), "nonexistent_db")
	assert.Equal(t, int64(0), dbID)
}

// --- Restrictive policy interaction ---

func TestApplyRLSFilter_RestrictivePolicy(t *testing.T) {
	cleanup := setupRLSTestEnv(t)
	defer cleanup()

	SetRLSConfig(enabledPermissiveConfig())
	policies := []*model.RLSPolicy{
		model.NewRLSPolicy("restrict1", 100, 1, model.RLSPolicyTypeRestrictive,
			[]string{"query"}, []string{"PUBLIC"},
			"archived == false", "", "no archived"),
	}
	initManagerWithPolicies(NewSimpleContextProvider("alice", []string{"PUBLIC"}), policies)

	result, err := applyRLSFilter(ctxForUser("alice"), "default", "coll", 100, "status == 'active'")
	require.NoError(t, err)
	assert.Contains(t, result, "status == 'active'")
	assert.Contains(t, result, "archived == false")
}

// --- User tag substitution through the full path ---

func TestApplyRLSFilter_UserTagSubstitution(t *testing.T) {
	cleanup := setupRLSTestEnv(t)
	defer cleanup()

	SetRLSConfig(enabledPermissiveConfig())
	policies := []*model.RLSPolicy{
		model.NewRLSPolicy("tag_policy", 100, 1, model.RLSPolicyTypePermissive,
			[]string{"query"}, []string{"PUBLIC"},
			"department == $current_user_tags['department']", "", "dept filter"),
	}
	mgr := initManagerWithPolicies(NewSimpleContextProvider("charlie", []string{"PUBLIC"}), policies)
	mgr.GetCache().UpdateUserTags("charlie", map[string]string{"department": "sales"})

	result, err := applyRLSFilter(ctxForUser("charlie"), "default", "coll", 100, "")
	require.NoError(t, err)
	assert.Contains(t, result, "department == 'sales'")
}
