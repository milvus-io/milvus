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

package rls

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/util/rlsutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type managerTestCoordClient struct {
	getRLSMetadata func(context.Context, *rootcoordpb.GetRLSMetadataRequest) (*rootcoordpb.GetRLSMetadataResponse, error)
}

var _ CoordClient = (*managerTestCoordClient)(nil)

func (c *managerTestCoordClient) GetRLSMetadata(ctx context.Context, req *rootcoordpb.GetRLSMetadataRequest, opts ...grpc.CallOption) (*rootcoordpb.GetRLSMetadataResponse, error) {
	return c.getRLSMetadata(ctx, req)
}

func TestToTemplateExprPreservesQuotedVariables(t *testing.T) {
	expr := `dept == "$current_principal" and owner == "$current_principal_tags['owner']"`
	templateExpr, needsPrincipal, tagVariables := toTemplateExpr(expr)
	assert.Equal(t, expr, templateExpr)
	assert.False(t, needsPrincipal)
	assert.Empty(t, tagVariables)
}

func TestReferencedFieldIDs(t *testing.T) {
	helper := newManagerTestPrincipalSchemaHelper(t)
	expr, err := planparserv2.ParseExpr(helper, `dept in ["sales", "support"] and owner == "alice"`, nil)
	require.NoError(t, err)
	assert.Equal(t, []int64{101, 102}, ReferencedFieldIDs(expr))
}

func TestManagerPolicyCombination(t *testing.T) {
	ctx := context.Background()
	manager := newManager()
	helper := newManagerTestSchemaHelper(t)

	require.True(t, manager.setRLSPolicySnapshot("db", 100, policySnapshot{
		Version: 1,
		Policies: []*rlsutil.RowPolicy{
			{
				PolicyName: "engineering",
				PolicyType: rlsutil.PolicyTypePermissive,
				Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
				UsingExpr:  `dept == "engineering"`,
			},
			{
				PolicyName: "sales",
				PolicyType: rlsutil.PolicyTypePermissive,
				Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
				UsingExpr:  `dept == "sales"`,
			},
			{
				PolicyName: "existing_ids",
				PolicyType: rlsutil.PolicyTypeRestrictive,
				Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
				UsingExpr:  `id in [1, 2]`,
			},
			{
				PolicyName: "search_only",
				PolicyType: rlsutil.PolicyTypeRestrictive,
				Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionSearch},
				UsingExpr:  `dept == "ignored"`,
			},
		},
	}))

	expr, err := manager.GetRLSUsingPredicate(ctx, 100, "alice", rlsutil.PolicyActionQuery, true, helper, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.NoError(t, ValidateRowsByPredicate(ctx, managerTestFieldsDataWithID(1, "sales"), 1, expr, "query", "using"))
	require.NoError(t, ValidateRowsByPredicate(ctx, managerTestFieldsDataWithID(2, "engineering"), 1, expr, "query", "using"))
	require.Error(t, ValidateRowsByPredicate(ctx, managerTestFieldsDataWithID(3, "sales"), 1, expr, "query", "using"))
	require.Error(t, ValidateRowsByPredicate(ctx, managerTestFieldsDataWithID(1, "product"), 1, expr, "query", "using"))

	_, err = manager.GetRLSUsingPredicate(ctx, 100, "alice", rlsutil.PolicyActionDelete, true, helper, nil)
	require.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
}

func TestManagerRestrictiveOnlyIsFalse(t *testing.T) {
	ctx := context.Background()
	manager := newManager()
	helper := newManagerTestSchemaHelper(t)

	require.True(t, manager.setRLSPolicySnapshot("db", 100, policySnapshot{
		Version: 1,
		Policies: []*rlsutil.RowPolicy{
			{
				PolicyName: "restrictive_only",
				PolicyType: rlsutil.PolicyTypeRestrictive,
				Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
				UsingExpr:  `dept == "sales"`,
			},
		},
	}))

	expr, err := manager.GetRLSUsingPredicate(ctx, 100, "alice", rlsutil.PolicyActionQuery, true, helper, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.Error(t, ValidateRowsByPredicate(ctx, managerTestFieldsData("sales"), 1, expr, "query", "using"))
}

func TestManagerPolicyTagsAndPrincipal(t *testing.T) {
	ctx := context.Background()
	manager := newManager()
	helper := newManagerTestPrincipalSchemaHelper(t)

	require.True(t, manager.setRLSPolicySnapshot("db", 100, policySnapshot{
		Version: 1,
		Policies: []*rlsutil.RowPolicy{
			{
				PolicyName: "dept",
				PolicyType: rlsutil.PolicyTypePermissive,
				Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
				UsingExpr:  "dept == $current_principal_tags['dept']",
			},
			{
				PolicyName: "owner_region",
				PolicyType: rlsutil.PolicyTypeRestrictive,
				Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
				UsingExpr:  "owner == $current_principal and region == $current_principal_tags['region']",
			},
		},
	}))
	require.True(t, manager.setRLSPrincipalTagsSnapshot("db", 100, principalTagsSnapshot{
		Version: 2,
		PrincipalTags: map[string]map[string]string{
			"alice": {"dept": "sales", "region": "us"},
		},
	}))

	expr, err := manager.GetRLSUsingPredicate(ctx, 100, "alice", rlsutil.PolicyActionQuery, true, helper, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.NoError(t, ValidateRowsByPredicate(ctx, managerTestPrincipalFieldsData("sales", "alice", "us"), 1, expr, "query", "using"))
	require.Error(t, ValidateRowsByPredicate(ctx, managerTestPrincipalFieldsData("sales", "bob", "us"), 1, expr, "query", "using"))

	require.True(t, manager.setRLSPrincipalTagsSnapshot("db", 100, principalTagsSnapshot{
		Version: 3,
		PrincipalTags: map[string]map[string]string{
			"alice": {"dept": "sales"},
		},
	}))
	expr, err = manager.GetRLSUsingPredicate(ctx, 100, "alice", rlsutil.PolicyActionQuery, true, helper, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.Error(t, ValidateRowsByPredicate(ctx, managerTestPrincipalFieldsData("sales", "alice", "us"), 1, expr, "query", "using"))
}

func TestManagerMissingTagOnlyDeniesReferencingPolicy(t *testing.T) {
	ctx := context.Background()
	manager := newManager()
	helper := newManagerTestSchemaHelper(t)

	require.True(t, manager.setRLSPolicySnapshot("db", 100, policySnapshot{
		Version: 1,
		Policies: []*rlsutil.RowPolicy{
			{
				PolicyName: "principal_dept",
				PolicyType: rlsutil.PolicyTypePermissive,
				Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
				UsingExpr:  "dept == $current_principal_tags['dept']",
			},
			{
				PolicyName: "public_dept",
				PolicyType: rlsutil.PolicyTypePermissive,
				Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
				UsingExpr:  `dept == "public"`,
			},
		},
	}))
	require.True(t, manager.setRLSPrincipalTagsSnapshot("db", 100, principalTagsSnapshot{
		Version: 2,
		PrincipalTags: map[string]map[string]string{
			"alice": {},
		},
	}))

	expr, err := manager.GetRLSUsingPredicate(ctx, 100, "alice", rlsutil.PolicyActionQuery, true, helper, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.NoError(t, ValidateRowsByPredicate(ctx, managerTestFieldsData("public"), 1, expr, "query", "using"))
	require.Error(t, ValidateRowsByPredicate(ctx, managerTestFieldsData("sales"), 1, expr, "query", "using"))
}

func TestManagerSnapshotReplace(t *testing.T) {
	ctx := context.Background()
	manager := newManager()
	helper := newManagerTestSchemaHelper(t)
	policy := &rlsutil.RowPolicy{
		PolicyName: "p1",
		PolicyType: rlsutil.PolicyTypePermissive,
		Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
		UsingExpr:  "dept == $current_principal_tags['dept']",
	}
	tags := map[string]string{"dept": "sales"}

	require.True(t, manager.setRLSPolicySnapshot("db", 100, policySnapshot{
		Version:  1,
		Policies: []*rlsutil.RowPolicy{policy},
	}))
	require.True(t, manager.setRLSPrincipalTagsSnapshot("db", 100, principalTagsSnapshot{
		Version: 2,
		PrincipalTags: map[string]map[string]string{
			"alice": tags,
		},
	}))

	expr, err := manager.GetRLSUsingPredicate(ctx, 100, "alice", rlsutil.PolicyActionQuery, true, helper, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.NoError(t, ValidateRowsByPredicate(ctx, managerTestFieldsData("sales"), 1, expr, "query", "using"))

	require.True(t, manager.setRLSPrincipalTagsSnapshot("db", 100, principalTagsSnapshot{
		Version:       3,
		PrincipalTags: map[string]map[string]string{},
	}))
	expr, err = manager.GetRLSUsingPredicate(ctx, 100, "alice", rlsutil.PolicyActionQuery, true, helper, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.Error(t, ValidateRowsByPredicate(ctx, managerTestFieldsData("sales"), 1, expr, "query", "using"))

	require.True(t, manager.setRLSPolicySnapshot("db", 100, policySnapshot{
		Version:  4,
		Policies: nil,
	}))
	expr, err = manager.GetRLSUsingPredicate(ctx, 100, "alice", rlsutil.PolicyActionQuery, true, helper, nil)
	require.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
	assert.Nil(t, expr)
}

func TestManagerEmptySnapshotFailsClosed(t *testing.T) {
	ctx := context.Background()
	manager := newManager()
	helper := newManagerTestSchemaHelper(t)

	require.True(t, manager.setRLSPolicySnapshot("db", 100, policySnapshot{Version: 0}))
	require.True(t, manager.setRLSPrincipalTagsSnapshot("db", 100, principalTagsSnapshot{Version: 0}))

	expr, err := manager.GetRLSUsingPredicate(ctx, 100, "alice", rlsutil.PolicyActionQuery, true, helper, nil)
	require.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
	require.Nil(t, expr)
}

func TestManagerDisabledIgnoresSnapshots(t *testing.T) {
	ctx := context.Background()
	manager := newManager()
	helper := newManagerTestSchemaHelper(t)

	require.True(t, manager.setRLSPolicySnapshot("db", 100, policySnapshot{
		Version: 1,
		Policies: []*rlsutil.RowPolicy{
			{
				PolicyName: "p1",
				PolicyType: rlsutil.PolicyTypePermissive,
				Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
				UsingExpr:  "dept == $current_principal_tags['dept']",
			},
		},
	}))
	require.True(t, manager.setRLSPrincipalTagsSnapshot("db", 100, principalTagsSnapshot{
		Version: 2,
		PrincipalTags: map[string]map[string]string{
			"alice": {"dept": "sales"},
		},
	}))

	expr, err := manager.GetRLSUsingPredicate(ctx, 100, "alice", rlsutil.PolicyActionQuery, false, helper, nil)
	require.NoError(t, err)
	require.Nil(t, expr)

	expr, err = manager.GetRLSUsingPredicate(ctx, 100, "alice", rlsutil.PolicyActionQuery, true, helper, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
}

func TestManagerCombinedExpressionLengthLimit(t *testing.T) {
	ctx := context.Background()
	manager := newManager()
	helper := newManagerTestSchemaHelper(t)

	paramtable.Get().Save(paramtable.Get().ProxyCfg.RLSMaxCombinedExpressionLength.Key, "8")
	t.Cleanup(func() {
		paramtable.Get().Reset(paramtable.Get().ProxyCfg.RLSMaxCombinedExpressionLength.Key)
	})

	require.True(t, manager.setRLSPolicySnapshot("db", 100, policySnapshot{
		Version: 1,
		Policies: []*rlsutil.RowPolicy{
			{
				PolicyName: "p1",
				PolicyType: rlsutil.PolicyTypePermissive,
				Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
				UsingExpr:  "dept == 'sales'",
			},
		},
	}))

	expr, err := manager.GetRLSUsingPredicate(ctx, 100, "alice", rlsutil.PolicyActionQuery, true, helper, nil)
	require.ErrorIs(t, err, merr.ErrServiceQuotaExceeded)
	require.Nil(t, expr)
}

func TestManagerAlwaysTruePredicateReturnsNil(t *testing.T) {
	ctx := context.Background()
	manager := newManager()
	helper := newManagerTestSchemaHelper(t)

	require.True(t, manager.setRLSPolicySnapshot("db", 100, policySnapshot{
		Version: 1,
		Policies: []*rlsutil.RowPolicy{
			{
				PolicyName: "full_access",
				PolicyType: rlsutil.PolicyTypePermissive,
				Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
				UsingExpr:  "true",
			},
		},
	}))

	expr, err := manager.GetRLSUsingPredicate(ctx, 100, "alice", rlsutil.PolicyActionQuery, true, helper, nil)
	require.NoError(t, err)
	require.Nil(t, expr)
}

func TestManagerRequestPathLoadsMissingStartupState(t *testing.T) {
	ctx := context.Background()
	const collectionID = UniqueID(104)

	manager := newManager()
	helper := newManagerTestSchemaHelper(t)
	coord := &managerTestCoordClient{
		getRLSMetadata: func(ctx context.Context, req *rootcoordpb.GetRLSMetadataRequest) (*rootcoordpb.GetRLSMetadataResponse, error) {
			require.NotNil(t, req)
			require.Equal(t, int64(collectionID), req.GetCollectionId())
			return &rootcoordpb.GetRLSMetadataResponse{
				Status:         merr.Success(),
				DbName:         "db",
				CollectionName: "coll",
				CollectionId:   collectionID,
				Policies: []*rootcoordpb.RLSPolicyInfo{
					{
						PolicyName: "dept",
						PolicyType: milvuspb.RowPolicyType(rlsutil.PolicyTypePermissive),
						Actions:    []milvuspb.RowPolicyAction{milvuspb.RowPolicyAction(rlsutil.PolicyActionQuery)},
						UsingExpr:  "dept == $current_principal_tags['dept']",
					},
				},
				Principals: []*rootcoordpb.RLSPrincipalInfo{
					{
						PrincipalName: "alice",
						Tags:          map[string]string{"dept": "sales"},
					},
				},
			}, nil
		},
	}

	require.NoError(t, manager.Init(ctx, coord, func(context.Context) (uint64, error) {
		return 10, nil
	}))

	expr, err := manager.GetRLSUsingPredicate(ctx, collectionID, "alice", rlsutil.PolicyActionQuery, true, helper, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.NoError(t, ValidateRowsByPredicate(ctx, managerTestFieldsData("sales"), 1, expr, "query", "using"))
}

func TestManagerRequestPathRefreshFailsClosed(t *testing.T) {
	ctx := context.Background()
	const collectionID = UniqueID(105)

	manager := newManager()
	helper := newManagerTestSchemaHelper(t)
	staleRefresh := time.Now().Add(-2 * time.Hour)
	require.True(t, manager.setRLSPolicySnapshot("db", collectionID, policySnapshot{
		Version:     1,
		RefreshedAt: staleRefresh,
		Policies: []*rlsutil.RowPolicy{
			{
				PolicyName: "stale-allow",
				PolicyType: rlsutil.PolicyTypePermissive,
				Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
				UsingExpr:  `dept == "sales"`,
			},
		},
	}))
	require.True(t, manager.setRLSPrincipalTagsSnapshot("db", collectionID, principalTagsSnapshot{
		Version:     1,
		RefreshedAt: staleRefresh,
	}))
	coord := &managerTestCoordClient{
		getRLSMetadata: func(ctx context.Context, req *rootcoordpb.GetRLSMetadataRequest) (*rootcoordpb.GetRLSMetadataResponse, error) {
			return &rootcoordpb.GetRLSMetadataResponse{
				Status: merr.Status(merr.WrapErrServiceUnavailableMsg("rootcoord unavailable")),
			}, nil
		},
	}

	require.NoError(t, manager.Init(ctx, coord, func(context.Context) (uint64, error) {
		return 10, nil
	}))
	expr, err := manager.GetRLSUsingPredicate(ctx, collectionID, "alice", rlsutil.PolicyActionQuery, true, helper, nil)
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	require.Nil(t, expr)
}

func TestManagerCollectionStateIsScopedByCollectionID(t *testing.T) {
	ctx := context.Background()
	manager := newManager()
	helper := newManagerTestSchemaHelper(t)

	require.True(t, manager.setRLSPolicySnapshot("db1", 100, policySnapshot{
		Version: 1,
		Policies: []*rlsutil.RowPolicy{
			{
				PolicyName: "dept",
				PolicyType: rlsutil.PolicyTypePermissive,
				Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
				UsingExpr:  `dept == "sales"`,
			},
		},
	}))
	require.True(t, manager.setRLSPrincipalTagsSnapshot("db1", 100, principalTagsSnapshot{Version: 1}))

	expr, err := manager.GetRLSUsingPredicate(ctx, 100, "alice", rlsutil.PolicyActionQuery, true, helper, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)

	expr, err = manager.GetRLSUsingPredicate(ctx, 101, "alice", rlsutil.PolicyActionQuery, true, helper, nil)
	require.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
	require.Nil(t, expr)
}

func TestManagerCollectionPredicateLocksAreIndependent(t *testing.T) {
	ctx := context.Background()
	manager := newManager()
	helper := newManagerTestSchemaHelper(t)
	for _, collectionID := range []UniqueID{100, 200} {
		require.True(t, manager.setRLSPolicySnapshot("db", collectionID, policySnapshot{
			Version: 1,
			Policies: []*rlsutil.RowPolicy{
				{
					PolicyName: "dept",
					PolicyType: rlsutil.PolicyTypePermissive,
					Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
					UsingExpr:  `dept == "sales"`,
				},
			},
		}))
		require.True(t, manager.setRLSPrincipalTagsSnapshot("db", collectionID, principalTagsSnapshot{Version: 1}))
	}

	state := manager.getCollectionState(newCollectionKey(100))
	require.NotNil(t, state)
	state.mu.Lock()
	firstDone := make(chan error, 1)
	go func() {
		_, err := manager.GetRLSUsingPredicate(ctx, 100, "alice", rlsutil.PolicyActionQuery, true, helper, nil)
		firstDone <- err
	}()
	select {
	case err := <-firstDone:
		state.mu.Unlock()
		require.Failf(t, "predicate unexpectedly bypassed its collection lock", "error: %v", err)
		return
	case <-time.After(20 * time.Millisecond):
	}

	secondDone := make(chan error, 1)
	go func() {
		_, err := manager.GetRLSUsingPredicate(ctx, 200, "alice", rlsutil.PolicyActionQuery, true, helper, nil)
		secondDone <- err
	}()
	select {
	case err := <-secondDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		state.mu.Unlock()
		t.Fatal("predicate for one collection waited for another collection's lock")
	}

	state.mu.Unlock()
	require.NoError(t, <-firstDone)
}

func TestManagerDefaultDatabaseNameDoesNotAffectLookup(t *testing.T) {
	ctx := context.Background()
	manager := newManager()
	helper := newManagerTestSchemaHelper(t)

	require.True(t, manager.setRLSPolicySnapshot("default", 100, policySnapshot{
		Version: 1,
		Policies: []*rlsutil.RowPolicy{
			{
				PolicyName: "dept",
				PolicyType: rlsutil.PolicyTypePermissive,
				Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
				UsingExpr:  `dept == "sales"`,
			},
		},
	}))
	require.True(t, manager.setRLSPrincipalTagsSnapshot("default", 100, principalTagsSnapshot{Version: 1}))

	expr, err := manager.GetRLSUsingPredicate(ctx, 100, "alice", rlsutil.PolicyActionQuery, true, helper, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.NoError(t, ValidateRowsByPredicate(ctx, managerTestFieldsData("sales"), 1, expr, "query", "using"))
}

func TestManagerMissingEntriesFailClosed(t *testing.T) {
	ctx := context.Background()
	manager := newManager()
	helper := newManagerTestSchemaHelper(t)

	expr, err := manager.GetRLSUsingPredicate(ctx, 100, "alice", rlsutil.PolicyActionQuery, true, helper, nil)
	require.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
	assert.Nil(t, expr)

	require.True(t, manager.setRLSPolicySnapshot("db", 100, policySnapshot{
		Version: 1,
		Policies: []*rlsutil.RowPolicy{
			{
				PolicyName: "p1",
				PolicyType: rlsutil.PolicyTypePermissive,
				Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
				UsingExpr:  "dept == $current_principal_tags['dept']",
			},
		},
	}))

	expr, err = manager.GetRLSUsingPredicate(ctx, 100, "alice", rlsutil.PolicyActionQuery, true, helper, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.Error(t, ValidateRowsByPredicate(ctx, managerTestFieldsData("sales"), 1, expr, "query", "using"))

	require.True(t, manager.setRLSPrincipalTagsSnapshot("db", 100, principalTagsSnapshot{
		Version: 2,
		PrincipalTags: map[string]map[string]string{
			"alice": {"region": "us"},
		},
	}))
	expr, err = manager.GetRLSUsingPredicate(ctx, 100, "alice", rlsutil.PolicyActionQuery, true, helper, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.Error(t, ValidateRowsByPredicate(ctx, managerTestFieldsData("sales"), 1, expr, "query", "using"))

	require.True(t, manager.setRLSPrincipalTagsSnapshot("db", 100, principalTagsSnapshot{
		Version: 3,
		PrincipalTags: map[string]map[string]string{
			"alice": {"dept": "sales"},
		},
	}))
	expr, err = manager.GetRLSUsingPredicate(ctx, 100, "alice", rlsutil.PolicyActionQuery, true, helper, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.NoError(t, ValidateRowsByPredicate(ctx, managerTestFieldsData("sales"), 1, expr, "query", "using"))
}

func TestManagerPolicySnapshotReplacesByName(t *testing.T) {
	ctx := context.Background()
	manager := newManager()
	helper := newManagerTestSchemaHelper(t)

	require.True(t, manager.setRLSPolicySnapshot("db", 100, policySnapshot{
		Version: 1,
		Policies: []*rlsutil.RowPolicy{
			{
				PolicyName: "sales",
				PolicyType: rlsutil.PolicyTypePermissive,
				Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
				UsingExpr:  `dept == "sales"`,
			},
			{
				PolicyName: "engineering",
				PolicyType: rlsutil.PolicyTypePermissive,
				Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
				UsingExpr:  `dept == "engineering"`,
			},
		},
	}))

	expr, err := manager.GetRLSUsingPredicate(ctx, 100, "alice", rlsutil.PolicyActionQuery, true, helper, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.NoError(t, ValidateRowsByPredicate(ctx, managerTestFieldsData("sales"), 1, expr, "query", "using"))
	require.NoError(t, ValidateRowsByPredicate(ctx, managerTestFieldsData("engineering"), 1, expr, "query", "using"))

	require.True(t, manager.setRLSPolicySnapshot("db", 100, policySnapshot{
		Version: 2,
		Policies: []*rlsutil.RowPolicy{
			{
				PolicyName: "sales",
				PolicyType: rlsutil.PolicyTypePermissive,
				Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
				UsingExpr:  `dept == "sales"`,
			},
			{
				PolicyName: "engineering",
				PolicyType: rlsutil.PolicyTypePermissive,
				Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
				UsingExpr:  `dept == "product"`,
			},
		},
	}))

	expr, err = manager.GetRLSUsingPredicate(ctx, 100, "alice", rlsutil.PolicyActionQuery, true, helper, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.NoError(t, ValidateRowsByPredicate(ctx, managerTestFieldsData("sales"), 1, expr, "query", "using"))
	require.Error(t, ValidateRowsByPredicate(ctx, managerTestFieldsData("engineering"), 1, expr, "query", "using"))
	require.NoError(t, ValidateRowsByPredicate(ctx, managerTestFieldsData("product"), 1, expr, "query", "using"))

	require.True(t, manager.setRLSPolicySnapshot("db", 100, policySnapshot{
		Version: 3,
		Policies: []*rlsutil.RowPolicy{
			{
				PolicyName: "engineering",
				PolicyType: rlsutil.PolicyTypePermissive,
				Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
				UsingExpr:  `dept == "product"`,
			},
		},
	}))
	expr, err = manager.GetRLSUsingPredicate(ctx, 100, "alice", rlsutil.PolicyActionQuery, true, helper, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.Error(t, ValidateRowsByPredicate(ctx, managerTestFieldsData("sales"), 1, expr, "query", "using"))
	require.NoError(t, ValidateRowsByPredicate(ctx, managerTestFieldsData("product"), 1, expr, "query", "using"))

	require.True(t, manager.setRLSPolicySnapshot("db", 100, policySnapshot{
		Version:  4,
		Policies: nil,
	}))
	expr, err = manager.GetRLSUsingPredicate(ctx, 100, "alice", rlsutil.PolicyActionQuery, true, helper, nil)
	require.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
	assert.Nil(t, expr)
}

func TestResolveRuntimePrincipal(t *testing.T) {
	principal, enforce, err := ResolveRuntimePrincipal(false, "", "query")
	require.NoError(t, err)
	assert.Empty(t, principal)
	assert.False(t, enforce)

	_, _, err = ResolveRuntimePrincipal(true, "", "query")
	require.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
	assert.Contains(t, err.Error(), "rls_principal")
	_, _, err = ResolveRuntimePrincipal(true, " \t ", "query")
	require.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
	assert.Contains(t, err.Error(), "rls_principal")

	principal, enforce, err = ResolveRuntimePrincipal(true, "alice", "query")
	require.NoError(t, err)
	assert.Equal(t, "alice", principal)
	assert.True(t, enforce)

	paramtable.Get().Save(paramtable.Get().ProxyCfg.RLSMaxPrincipalNameLength.Key, "3")
	t.Cleanup(func() {
		paramtable.Get().Reset(paramtable.Get().ProxyCfg.RLSMaxPrincipalNameLength.Key)
	})
	principal, enforce, err = ResolveRuntimePrincipal(true, "alice", "query")
	require.NoError(t, err)
	assert.Equal(t, "alice", principal)
	assert.True(t, enforce)

	_, _, err = ResolveRuntimePrincipal(true, strings.Repeat("a", rlsutil.MaxTransportIdentifierLength+1), "query")
	require.ErrorIs(t, err, merr.ErrParameterTooLarge)
}

func TestValidateCheckForWriteUsesSchemaTimezone(t *testing.T) {
	ctx := context.Background()
	const collectionID = int64(987654322)
	defaultManager.removeCollection(ctx, collectionID)
	t.Cleanup(func() {
		defaultManager.removeCollection(ctx, collectionID)
	})

	schema := &schemapb.CollectionSchema{
		Name:       "rls_timestamptz_test",
		Properties: []*commonpb.KeyValuePair{{Key: common.TimezoneKey, Value: "Asia/Shanghai"}},
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "ts", DataType: schemapb.DataType_Timestamptz},
		},
	}
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	require.True(t, defaultManager.setRLSPolicySnapshot("db", collectionID, policySnapshot{
		Version: 1,
		Policies: []*rlsutil.RowPolicy{
			{
				PolicyName: "insert_at_midnight",
				PolicyType: rlsutil.PolicyTypePermissive,
				Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionInsert},
				CheckExpr:  "ts == ISO '2025-01-01 00:00:00'",
			},
		},
	}))

	fieldsData := []*schemapb.FieldData{
		{
			FieldId:   100,
			FieldName: "id",
			Type:      schemapb.DataType_Int64,
			Field:     &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1}}}}},
		},
		{
			FieldId:   101,
			FieldName: "ts",
			Type:      schemapb.DataType_Timestamptz,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_TimestamptzData{
				TimestamptzData: &schemapb.TimestamptzArray{Data: []int64{1735660800000000}},
			}}},
		},
	}
	require.NoError(t, ValidateCheckForWrite(ctx, collectionID, "alice", rlsutil.PolicyActionInsert, true, fieldsData, helper, 1, "insert"))
}

func TestManagerReadPredicateUsesSchemaTimezone(t *testing.T) {
	ctx := context.Background()
	manager := newManager()
	const collectionID = int64(987654323)

	schema := &schemapb.CollectionSchema{
		Name:       "rls_read_timestamptz_test",
		Properties: []*commonpb.KeyValuePair{{Key: common.TimezoneKey, Value: "Asia/Shanghai"}},
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "ts", DataType: schemapb.DataType_Timestamptz},
		},
	}
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	require.True(t, manager.setRLSPolicySnapshot("db", collectionID, policySnapshot{
		Version: 1,
		Policies: []*rlsutil.RowPolicy{
			{
				PolicyName: "read_at_midnight",
				PolicyType: rlsutil.PolicyTypePermissive,
				Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
				UsingExpr:  "ts == ISO '2025-01-01 00:00:00'",
			},
		},
	}))

	expr, err := manager.GetRLSUsingPredicate(
		ctx,
		collectionID,
		"alice",
		rlsutil.PolicyActionQuery,
		true,
		helper,
		&planparserv2.ParserVisitorArgs{Timezone: "UTC"},
	)
	require.NoError(t, err)

	fieldsData := []*schemapb.FieldData{
		{
			FieldId:   101,
			FieldName: "ts",
			Type:      schemapb.DataType_Timestamptz,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_TimestamptzData{
				TimestamptzData: &schemapb.TimestamptzArray{Data: []int64{1735660800000000}},
			}}},
		},
	}
	require.NoError(t, ValidateRowsByPredicate(ctx, fieldsData, 1, expr, "query", "using"))
}

func TestManagerCompiledPredicateCacheUsesSchemaContext(t *testing.T) {
	ctx := context.Background()
	manager := newManager()
	const collectionID = int64(987654324)

	newHelper := func(version int32, timezone string) *typeutil.SchemaHelper {
		helper, err := typeutil.CreateSchemaHelper(&schemapb.CollectionSchema{
			Name:       "rls_compiled_cache_schema_context_test",
			Version:    version,
			Properties: []*commonpb.KeyValuePair{{Key: common.TimezoneKey, Value: timezone}},
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 101, Name: "ts", DataType: schemapb.DataType_Timestamptz},
			},
		})
		require.NoError(t, err)
		return helper
	}

	require.True(t, manager.setRLSPolicySnapshot("db", collectionID, policySnapshot{
		Version: 1,
		Policies: []*rlsutil.RowPolicy{
			{
				PolicyName: "read_at_midnight",
				PolicyType: rlsutil.PolicyTypePermissive,
				Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
				UsingExpr:  "ts == ISO '2025-01-01 00:00:00'",
			},
		},
	}))

	getPredicateValue := func(helper *typeutil.SchemaHelper) int64 {
		expr, err := manager.GetRLSUsingPredicate(
			ctx,
			collectionID,
			"alice",
			rlsutil.PolicyActionQuery,
			true,
			helper,
			&planparserv2.ParserVisitorArgs{Timezone: "America/Los_Angeles"},
		)
		require.NoError(t, err)
		require.NotNil(t, expr.GetUnaryRangeExpr())
		return expr.GetUnaryRangeExpr().GetValue().GetInt64Val()
	}

	shanghaiV1 := newHelper(1, "Asia/Shanghai")
	assert.Equal(t, int64(1735660800000000), getPredicateValue(shanghaiV1))

	// A real schema evolution must use a distinct compiled entry even when the
	// collection timezone remains unchanged.
	shanghaiV2 := newHelper(2, "Asia/Shanghai")
	assert.Equal(t, int64(1735660800000000), getPredicateValue(shanghaiV2))

	// Collection property alters, including timezone changes, do not increment
	// schema version. Timezone therefore has to be part of the cache identity.
	utcV2 := newHelper(2, "UTC")
	assert.Equal(t, int64(1735689600000000), getPredicateValue(utcV2))

	state := manager.getCollectionState(newCollectionKey(collectionID))
	require.NotNil(t, state)
	state.mu.RLock()
	defer state.mu.RUnlock()
	require.Len(t, state.compiled, 1)
	entry := state.compiled[compiledKey{action: rlsutil.PolicyActionQuery, kind: usingExprKind}]
	require.NotNil(t, entry)
	assert.Equal(t, int32(2), entry.schemaVersion)
	assert.Equal(t, "UTC", entry.timezone)
	require.NotNil(t, entry.expression)
}

func TestValidateRowsUsesFieldPrecisionForFloat(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "rls_float_test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "score", DataType: schemapb.DataType_Float},
			{FieldID: 101, Name: "scores", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Float},
		},
	}
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)

	t.Run("scalar", func(t *testing.T) {
		fieldsData := []*schemapb.FieldData{{
			FieldId:   100,
			FieldName: "score",
			Type:      schemapb.DataType_Float,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_FloatData{
				FloatData: &schemapb.FloatArray{Data: []float32{0.1}},
			}}},
		}}
		require.NoError(t, validateRows(context.Background(), fieldsData, helper, 1, "score == 0.1", "insert", "check"))
	})

	t.Run("array", func(t *testing.T) {
		fieldsData := []*schemapb.FieldData{{
			FieldId:   101,
			FieldName: "scores",
			Type:      schemapb.DataType_Array,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{
				ArrayData: &schemapb.ArrayArray{
					ElementType: schemapb.DataType_Float,
					Data: []*schemapb.ScalarField{{
						Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: []float32{0.1}}},
					}},
				},
			}}},
		}}
		require.NoError(t, validateRows(context.Background(), fieldsData, helper, 1, "array_contains(scores, 0.1)", "insert", "check"))
	})
}

func TestValidateRowsTreatsEmptyValidDataAsDense(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "rls_default_value_test",
		Fields: []*schemapb.FieldSchema{{
			FieldID:  100,
			Name:     "score",
			DataType: schemapb.DataType_Float,
			DefaultValue: &schemapb.ValueField{
				Data: &schemapb.ValueField_FloatData{FloatData: 0.1},
			},
		}},
	}
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)

	fieldsData := []*schemapb.FieldData{{
		FieldId:   100,
		FieldName: "score",
		Type:      schemapb.DataType_Float,
		ValidData: []bool{},
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_FloatData{
			FloatData: &schemapb.FloatArray{Data: []float32{0.1}},
		}}},
	}}

	require.NotPanics(t, func() {
		err = validateRows(context.Background(), fieldsData, helper, 1, "score == 0.1", "insert", "check")
	})
	require.NoError(t, err)
}

func TestManagerPredicateEvaluationUsesSeparateSnapshotWatermarks(t *testing.T) {
	ctx := context.Background()
	manager := newManager()
	helper := newManagerTestSchemaHelper(t)

	require.True(t, manager.setRLSPrincipalTagsSnapshot("db", 100, principalTagsSnapshot{
		Version: 20,
		PrincipalTags: map[string]map[string]string{
			"alice": {"dept": "sales"},
		},
	}))
	require.True(t, manager.setRLSPolicySnapshot("db", 100, policySnapshot{
		Version: 10,
		Policies: []*rlsutil.RowPolicy{
			{
				PolicyName: "dept",
				PolicyType: rlsutil.PolicyTypePermissive,
				Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
				UsingExpr:  "dept == $current_principal_tags['dept']",
			},
		},
	}))

	expr, err := manager.GetRLSUsingPredicate(ctx, 100, "alice", rlsutil.PolicyActionQuery, true, helper, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.NoError(t, ValidateRowsByPredicate(ctx, managerTestFieldsData("sales"), 1, expr, "query", "using"))

	require.False(t, manager.setRLSPolicySnapshot("db", 100, policySnapshot{
		Version: 9,
		Policies: []*rlsutil.RowPolicy{
			{
				PolicyName: "dept",
				PolicyType: rlsutil.PolicyTypePermissive,
				Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
				UsingExpr:  `dept == "engineering"`,
			},
		},
	}))
	expr, err = manager.GetRLSUsingPredicate(ctx, 100, "alice", rlsutil.PolicyActionQuery, true, helper, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.NoError(t, ValidateRowsByPredicate(ctx, managerTestFieldsData("sales"), 1, expr, "query", "using"))
	require.Error(t, ValidateRowsByPredicate(ctx, managerTestFieldsData("engineering"), 1, expr, "query", "using"))

	require.True(t, manager.setRLSPrincipalTagsSnapshot("db", 100, principalTagsSnapshot{
		Version:       21,
		PrincipalTags: map[string]map[string]string{},
	}))
	expr, err = manager.GetRLSUsingPredicate(ctx, 100, "alice", rlsutil.PolicyActionQuery, true, helper, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.Error(t, ValidateRowsByPredicate(ctx, managerTestFieldsData("sales"), 1, expr, "query", "using"))
}

func newManagerTestSchemaHelper(t *testing.T) *typeutil.SchemaHelper {
	t.Helper()

	schema := &schemapb.CollectionSchema{
		Name: "rls_manager_test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "dept", DataType: schemapb.DataType_VarChar},
		},
	}
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	return helper
}

func newManagerTestPrincipalSchemaHelper(t *testing.T) *typeutil.SchemaHelper {
	t.Helper()

	schema := &schemapb.CollectionSchema{
		Name: "rls_manager_principal_test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "dept", DataType: schemapb.DataType_VarChar},
			{FieldID: 102, Name: "owner", DataType: schemapb.DataType_VarChar},
			{FieldID: 103, Name: "region", DataType: schemapb.DataType_VarChar},
		},
	}
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	return helper
}

func managerTestFieldsData(dept string) []*schemapb.FieldData {
	return managerTestFieldsDataWithID(1, dept)
}

func managerTestFieldsDataWithID(id int64, dept string) []*schemapb.FieldData {
	return []*schemapb.FieldData{
		{
			FieldId:   100,
			FieldName: "id",
			Type:      schemapb.DataType_Int64,
			Field:     &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{id}}}}},
		},
		{
			FieldId:   101,
			FieldName: "dept",
			Type:      schemapb.DataType_VarChar,
			Field:     &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{dept}}}}},
		},
	}
}

func managerTestPrincipalFieldsData(dept string, owner string, region string) []*schemapb.FieldData {
	return []*schemapb.FieldData{
		{
			FieldId:   100,
			FieldName: "id",
			Type:      schemapb.DataType_Int64,
			Field:     &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1}}}}},
		},
		{
			FieldId:   101,
			FieldName: "dept",
			Type:      schemapb.DataType_VarChar,
			Field:     &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{dept}}}}},
		},
		{
			FieldId:   102,
			FieldName: "owner",
			Type:      schemapb.DataType_VarChar,
			Field:     &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{owner}}}}},
		},
		{
			FieldId:   103,
			FieldName: "region",
			Type:      schemapb.DataType_VarChar,
			Field:     &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{region}}}}},
		},
	}
}

func TestMergePredicateToPlan(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "rls_plan_test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "owner", DataType: schemapb.DataType_VarChar},
			{FieldID: 102, Name: "age", DataType: schemapb.DataType_Int64},
			{
				FieldID:  103,
				Name:     "vec",
				DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: "4"},
				},
			},
		},
	}
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	visitorArgs := &planparserv2.ParserVisitorArgs{}

	retrievePlan, err := planparserv2.CreateRetrievePlanArgs(helper, "age > 18", nil, visitorArgs)
	require.NoError(t, err)
	rlsPredicate, err := planparserv2.ParseExpr(helper, `owner == "alice"`, nil)
	require.NoError(t, err)
	require.NoError(t, MergePredicateToPlan(retrievePlan, rlsPredicate))
	assertPredicateMerged(t, retrievePlan.GetQuery().GetPredicates())

	searchPlan, err := planparserv2.CreateSearchPlanArgs(helper, "age > 18", "vec", &planpb.QueryInfo{
		Topk:           10,
		MetricType:     "L2",
		SearchParams:   "{}",
		GroupByFieldId: -1,
	}, nil, nil, visitorArgs)
	require.NoError(t, err)
	rlsPredicate, err = planparserv2.ParseExpr(helper, `owner == "alice"`, nil)
	require.NoError(t, err)
	require.NoError(t, MergePredicateToPlan(searchPlan, rlsPredicate))
	assertPredicateMerged(t, searchPlan.GetVectorAnns().GetPredicates())
}

func TestManagerApplyRLSUsingPredicate(t *testing.T) {
	ctx := context.Background()
	manager := newManager()
	helper := newManagerTestSchemaHelper(t)
	visitorArgs := &planparserv2.ParserVisitorArgs{}

	require.True(t, manager.setRLSPolicySnapshot("db", 100, policySnapshot{
		Version: 1,
		Policies: []*rlsutil.RowPolicy{
			{
				PolicyName: "dept",
				PolicyType: rlsutil.PolicyTypePermissive,
				Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
				UsingExpr:  "dept == $current_principal_tags['dept']",
			},
		},
	}))
	require.True(t, manager.setRLSPrincipalTagsSnapshot("db", 100, principalTagsSnapshot{
		Version: 2,
		PrincipalTags: map[string]map[string]string{
			"alice": {"dept": "sales"},
		},
	}))

	plan, err := planparserv2.CreateRetrievePlanArgs(helper, "id == 1", nil, visitorArgs)
	require.NoError(t, err)
	err = manager.ApplyRLSUsingPredicate(ctx, 100, "alice", rlsutil.PolicyActionQuery, true, helper, visitorArgs, plan)
	require.NoError(t, err)
	assertPredicateMerged(t, plan.GetQuery().GetPredicates())
	require.NoError(t, ValidateRowsByPredicate(ctx, managerTestFieldsData("sales"), 1, plan.GetQuery().GetPredicates(), "query", "using"))
	require.Error(t, ValidateRowsByPredicate(ctx, managerTestFieldsData("engineering"), 1, plan.GetQuery().GetPredicates(), "query", "using"))
}

func assertPredicateMerged(t *testing.T, expr *planpb.Expr) {
	t.Helper()

	binaryExpr := expr.GetBinaryExpr()
	require.NotNil(t, binaryExpr)
	assert.Equal(t, planpb.BinaryExpr_LogicalAnd, binaryExpr.GetOp())
	assert.NotNil(t, binaryExpr.GetLeft())
	assert.NotNil(t, binaryExpr.GetRight())
}

func TestValidateRowsByParsedExpression(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "rls_test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "owner", DataType: schemapb.DataType_VarChar},
			{FieldID: 102, Name: "age", DataType: schemapb.DataType_Int64},
			{FieldID: 103, Name: "tags", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_VarChar},
		},
	}
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	fieldsData := []*schemapb.FieldData{
		{
			FieldId:   100,
			FieldName: "id",
			Type:      schemapb.DataType_Int64,
			Field:     &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1, 2}}}}},
		},
		{
			FieldId:   101,
			FieldName: "owner",
			Type:      schemapb.DataType_VarChar,
			Field:     &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"alice", "alice"}}}}},
		},
		{
			FieldId:   102,
			FieldName: "age",
			Type:      schemapb.DataType_Int64,
			Field:     &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{18, 19}}}}},
		},
		{
			FieldId:   103,
			FieldName: "tags",
			Type:      schemapb.DataType_Array,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
				ElementType: schemapb.DataType_VarChar,
				Data: []*schemapb.ScalarField{
					{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"red", "blue"}}}},
					{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"red"}}}},
				},
			}}}},
		},
	}

	err = validateRows(context.Background(), fieldsData, helper, 2, `owner == "alice" and age in [18, 19] and array_contains(tags, "red")`, "insert", "check")
	require.NoError(t, err)

	err = validateRows(context.Background(), fieldsData, helper, 2, `age == 18`, "insert", "check")
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
	assert.Contains(t, err.Error(), "row 1")
}

func TestValidateWritePredicatesUseThreeValuedLogic(t *testing.T) {
	ctx := context.Background()
	const collectionID = int64(987654323)
	defaultManager.removeCollection(ctx, collectionID)
	t.Cleanup(func() {
		defaultManager.removeCollection(ctx, collectionID)
	})

	schema := &schemapb.CollectionSchema{
		Name: "rls_three_valued_logic_test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "dept", DataType: schemapb.DataType_VarChar, Nullable: true},
			{FieldID: 102, Name: "tags", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_VarChar, Nullable: true},
		},
	}
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	require.True(t, defaultManager.setRLSPolicySnapshot("db", collectionID, policySnapshot{
		Version: 1,
		Policies: []*rlsutil.RowPolicy{
			{
				PolicyName: "not_blocked",
				PolicyType: rlsutil.PolicyTypePermissive,
				Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionInsert},
				CheckExpr:  `not (dept == "blocked")`,
			},
		},
	}))

	newFieldsData := func(fullSize bool) []*schemapb.FieldData {
		deptData := []string(nil)
		arrayData := []*schemapb.ScalarField(nil)
		if fullSize {
			deptData = []string{""}
			arrayData = []*schemapb.ScalarField{{
				Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{}},
			}}
		}
		return []*schemapb.FieldData{
			{
				FieldId:   100,
				FieldName: "id",
				Type:      schemapb.DataType_Int64,
				Field:     &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1}}}}},
			},
			{
				FieldId:   101,
				FieldName: "dept",
				Type:      schemapb.DataType_VarChar,
				ValidData: []bool{false},
				Field:     &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: deptData}}}},
			},
			{
				FieldId:   102,
				FieldName: "tags",
				Type:      schemapb.DataType_Array,
				ValidData: []bool{false},
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
					ElementType: schemapb.DataType_VarChar,
					Data:        arrayData,
				}}}},
			},
		}
	}

	for _, fullSize := range []bool{false, true} {
		name := "compact"
		if fullSize {
			name = "full_size"
		}
		t.Run(name, func(t *testing.T) {
			fieldsData := newFieldsData(fullSize)
			err := ValidateCheckForWrite(ctx, collectionID, "alice", rlsutil.PolicyActionInsert, true, fieldsData, helper, 1, "insert")
			require.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
		})
	}

	fieldsData := newFieldsData(false)
	rowData := newRowData(fieldsData)
	tests := []struct {
		name     string
		expr     string
		expected truthValue
	}{
		{name: "not unknown", expr: `not (dept == "blocked")`, expected: truthUnknown},
		{name: "unknown and false", expr: `dept == "blocked" and false`, expected: truthFalse},
		{name: "unknown and true", expr: `dept == "blocked" and true`, expected: truthUnknown},
		{name: "unknown or true", expr: `dept == "blocked" or true`, expected: truthTrue},
		{name: "unknown or false", expr: `dept == "blocked" or false`, expected: truthUnknown},
		{name: "not contains unknown", expr: `not array_contains(tags, "red")`, expected: truthUnknown},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parsedExpr, err := planparserv2.ParseExpr(helper, test.expr, nil)
			require.NoError(t, err)
			result, err := evalExpr(parsedExpr, rowData, 0)
			require.NoError(t, err)
			require.Equal(t, test.expected, result)

			err = ValidateUsingPredicateForExistingRows(ctx, fieldsData, 1, "upsert", parsedExpr)
			if test.expected == truthTrue {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
		})
	}
}

func TestValidateRowsInternalRowShapeErrorsAreSystemErrors(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "rls_test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "age", DataType: schemapb.DataType_Int64},
		},
	}
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)

	fieldsData := []*schemapb.FieldData{
		{
			FieldId:   100,
			FieldName: "id",
			Type:      schemapb.DataType_Int64,
			Field:     &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1}}}}},
		},
	}

	err = validateRows(context.Background(), fieldsData, helper, 1, `age == 18`, "insert", "check")
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrServiceInternal)
	assert.NotErrorIs(t, err, merr.ErrParameterInvalid)
}
