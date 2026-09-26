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
	"encoding/json"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proxy/privilege"
	"github.com/milvus-io/milvus/internal/util/rlsutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestProxyRLSAPIsForwardToMixCoord(t *testing.T) {
	const (
		requestDBName           = "source_db"
		canonicalDBName         = "target_db"
		requestCollectionName   = "coll_alias"
		canonicalCollectionName = "coll"
	)
	ctx := context.Background()
	mixCoord := &mocks.MockMixCoordClient{}
	t.Cleanup(func() {
		mixCoord.AssertExpectations(t)
	})
	cache := NewMockCache(t)
	cache.EXPECT().GetCollectionID(mock.Anything, requestDBName, requestCollectionName).Return(int64(1), nil).Times(8)
	cache.EXPECT().GetCollectionInfo(mock.Anything, requestDBName, "", int64(1)).Return(&collectionInfo{
		CollID: int64(1),
		DBName: canonicalDBName,
		Schema: &schemaInfo{CollectionSchema: &schemapb.CollectionSchema{Name: canonicalCollectionName}},
	}, nil).Times(8)

	node := &Proxy{mixCoord: mixCoord, metaCache: cache}
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	mixCoord.EXPECT().CreateRowPolicy(mock.Anything, mock.MatchedBy(func(req *milvuspb.CreateRowPolicyRequest) bool {
		return req != nil &&
			req.GetBase().GetMsgType() == commonpb.MsgType_CreateRowPolicy &&
			req.GetDbName() == canonicalDBName &&
			req.GetCollectionName() == canonicalCollectionName &&
			req.GetPolicyType() == milvuspb.RowPolicyType_RowPolicyTypePermissive
	})).Return(merr.Success(), nil).Once()
	status, err := node.CreateRowPolicy(ctx, &milvuspb.CreateRowPolicyRequest{
		DbName:         requestDBName,
		CollectionName: requestCollectionName,
		PolicyName:     "policy",
		Actions:        []milvuspb.RowPolicyAction{milvuspb.RowPolicyAction_Query},
		UsingExpr:      "true",
	})
	require.NoError(t, err)
	require.Equal(t, commonpb.ErrorCode_Success, status.GetErrorCode())

	mixCoord.EXPECT().UpdateRowPolicy(mock.Anything, mock.MatchedBy(func(req *milvuspb.UpdateRowPolicyRequest) bool {
		return req != nil && req.GetBase().GetMsgType() == commonpb.MsgType_UpdateRowPolicy &&
			req.GetDbName() == canonicalDBName && req.GetCollectionName() == canonicalCollectionName
	})).Return(merr.Success(), nil).Once()
	status, err = node.UpdateRowPolicy(ctx, &milvuspb.UpdateRowPolicyRequest{
		DbName:         requestDBName,
		CollectionName: requestCollectionName,
		PolicyName:     "policy",
		PolicyType:     milvuspb.RowPolicyType_RowPolicyTypePermissive,
		Actions:        []milvuspb.RowPolicyAction{milvuspb.RowPolicyAction_Query},
		UsingExpr:      "true",
	})
	require.NoError(t, err)
	require.Equal(t, commonpb.ErrorCode_Success, status.GetErrorCode())

	mixCoord.EXPECT().DropRowPolicy(mock.Anything, mock.MatchedBy(func(req *milvuspb.DropRowPolicyRequest) bool {
		return req != nil && req.GetBase().GetMsgType() == commonpb.MsgType_DropRowPolicy &&
			req.GetDbName() == canonicalDBName && req.GetCollectionName() == canonicalCollectionName
	})).Return(merr.Success(), nil).Once()
	status, err = node.DropRowPolicy(ctx, &milvuspb.DropRowPolicyRequest{
		DbName:         requestDBName,
		CollectionName: requestCollectionName,
		PolicyName:     "policy",
	})
	require.NoError(t, err)
	require.Equal(t, commonpb.ErrorCode_Success, status.GetErrorCode())

	mixCoord.EXPECT().ListRowPolicies(mock.Anything, mock.MatchedBy(func(req *milvuspb.ListRowPoliciesRequest) bool {
		return req != nil && req.GetBase().GetMsgType() == commonpb.MsgType_ListRowPolicies &&
			req.GetDbName() == canonicalDBName && req.GetCollectionName() == canonicalCollectionName
	})).Return(&milvuspb.ListRowPoliciesResponse{
		Status:         merr.Success(),
		DbName:         canonicalDBName,
		CollectionName: "coll",
		Policies:       []*milvuspb.RowPolicy{{PolicyName: "policy"}},
	}, nil).Once()
	listPoliciesResp, err := node.ListRowPolicies(ctx, &milvuspb.ListRowPoliciesRequest{
		DbName:         requestDBName,
		CollectionName: requestCollectionName,
	})
	require.NoError(t, err)
	require.Equal(t, canonicalDBName, listPoliciesResp.GetDbName())
	require.Equal(t, "coll", listPoliciesResp.GetCollectionName())
	require.Equal(t, "policy", listPoliciesResp.GetPolicies()[0].GetPolicyName())
	require.Equal(t, commonpb.ErrorCode_Success, listPoliciesResp.GetStatus().GetErrorCode())

	mixCoord.EXPECT().SetRLSPrincipalTags(mock.Anything, mock.MatchedBy(func(req *milvuspb.SetRLSPrincipalTagsRequest) bool {
		return req != nil && req.GetBase().GetMsgType() == commonpb.MsgType_SetRLSPrincipalTags &&
			req.GetDbName() == canonicalDBName && req.GetCollectionName() == canonicalCollectionName
	})).Return(merr.Success(), nil).Once()
	status, err = node.SetRLSPrincipalTags(ctx, &milvuspb.SetRLSPrincipalTagsRequest{
		DbName:         requestDBName,
		CollectionName: requestCollectionName,
		PrincipalName:  "alice",
		Tags:           `{"team":"search"}`,
	})
	require.NoError(t, err)
	require.Equal(t, commonpb.ErrorCode_Success, status.GetErrorCode())

	mixCoord.EXPECT().GetRLSPrincipalTags(mock.Anything, mock.MatchedBy(func(req *milvuspb.GetRLSPrincipalTagsRequest) bool {
		return req != nil && req.GetBase().GetMsgType() == commonpb.MsgType_GetRLSPrincipalTags &&
			req.GetDbName() == canonicalDBName && req.GetCollectionName() == canonicalCollectionName
	})).Return(&milvuspb.GetRLSPrincipalTagsResponse{
		Status:         merr.Success(),
		DbName:         canonicalDBName,
		CollectionName: "coll",
		PrincipalName:  "alice",
		Tags:           `{"dept":"engineering"}`,
	}, nil).Once()
	getTagsResp, err := node.GetRLSPrincipalTags(ctx, &milvuspb.GetRLSPrincipalTagsRequest{
		DbName:         requestDBName,
		CollectionName: requestCollectionName,
		PrincipalName:  "alice",
	})
	require.NoError(t, err)
	require.Equal(t, canonicalDBName, getTagsResp.GetDbName())
	require.Equal(t, "coll", getTagsResp.GetCollectionName())
	require.Equal(t, "alice", getTagsResp.GetPrincipalName())
	require.JSONEq(t, `{"dept":"engineering"}`, getTagsResp.GetTags())
	require.Equal(t, commonpb.ErrorCode_Success, getTagsResp.GetStatus().GetErrorCode())

	mixCoord.EXPECT().ListRLSPrincipals(mock.Anything, mock.MatchedBy(func(req *milvuspb.ListRLSPrincipalsRequest) bool {
		return req != nil && req.GetBase().GetMsgType() == commonpb.MsgType_ListRLSPrincipals &&
			req.GetDbName() == canonicalDBName && req.GetCollectionName() == canonicalCollectionName
	})).Return(&milvuspb.ListRLSPrincipalsResponse{
		Status:         merr.Success(),
		DbName:         canonicalDBName,
		CollectionName: "coll",
		PrincipalNames: []string{"alice"},
	}, nil).Once()
	listPrincipalsResp, err := node.ListRLSPrincipals(ctx, &milvuspb.ListRLSPrincipalsRequest{
		DbName:         requestDBName,
		CollectionName: requestCollectionName,
	})
	require.NoError(t, err)
	require.Equal(t, canonicalDBName, listPrincipalsResp.GetDbName())
	require.Equal(t, "coll", listPrincipalsResp.GetCollectionName())
	require.Equal(t, []string{"alice"}, listPrincipalsResp.GetPrincipalNames())
	require.Equal(t, commonpb.ErrorCode_Success, listPrincipalsResp.GetStatus().GetErrorCode())

	mixCoord.EXPECT().DeleteRLSPrincipalTags(mock.Anything, mock.MatchedBy(func(req *milvuspb.DeleteRLSPrincipalTagsRequest) bool {
		return req != nil &&
			req.GetBase().GetMsgType() == commonpb.MsgType_DeleteRLSPrincipalTags &&
			req.GetDbName() == canonicalDBName &&
			req.GetCollectionName() == canonicalCollectionName &&
			len(req.GetTagKeys()) == 1 && req.GetTagKeys()[0] == "dept"
	})).Return(merr.Success(), nil).Once()
	status, err = node.DeleteRLSPrincipalTags(ctx, &milvuspb.DeleteRLSPrincipalTagsRequest{
		DbName:         requestDBName,
		CollectionName: requestCollectionName,
		PrincipalName:  "alice",
		TagKeys:        []string{"dept", "dept"},
	})
	require.NoError(t, err)
	require.Equal(t, commonpb.ErrorCode_Success, status.GetErrorCode())
}

func TestProxyRLSAPIReauthorizesCanonicalTarget(t *testing.T) {
	const (
		oldDBName     = "old_db"
		newDBName     = "new_db"
		aliasName     = "rls_alias"
		oldCollection = "old_collection"
		newCollection = "new_collection"
		roleName      = "rls_admin"
		username      = "alice"
	)
	require.NoError(t, Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "true"))
	require.NoError(t, Params.Save(Params.ProxyCfg.ResolveAliasForPrivilege.Key, "true"))
	privilege.ResetPrivilegeCacheForTest()
	t.Cleanup(func() {
		Params.Reset(Params.CommonCfg.AuthorizationEnabled.Key)
		Params.Reset(Params.ProxyCfg.ResolveAliasForPrivilege.Key)
		privilege.ResetPrivilegeCacheForTest()
	})

	policyCoord := mocks.NewMockMixCoordClient(t)
	policyCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
		PolicyInfos: []string{
			funcutil.PolicyForPrivilege(roleName, commonpb.ObjectType_Collection.String(),
				oldCollection, commonpb.ObjectPrivilege_PrivilegeManageRLS.String(), oldDBName),
			// This stale-database grant proves the second authorization uses newDBName,
			// not merely the canonical collection name with the request database.
			funcutil.PolicyForPrivilege(roleName, commonpb.ObjectType_Collection.String(),
				newCollection, commonpb.ObjectPrivilege_PrivilegeManageRLS.String(), oldDBName),
		},
		UserRoles: []string{funcutil.EncodeUserRoleCache(username, roleName)},
	}, nil).Once()
	require.NoError(t, privilege.InitPrivilegeCache(context.Background(), policyCoord))

	cache := NewMockCache(t)
	cache.EXPECT().ResolveCollectionAlias(mock.Anything, oldDBName, aliasName).Return(oldCollection, nil).Once()
	req := &milvuspb.CreateRowPolicyRequest{
		DbName:         oldDBName,
		CollectionName: aliasName,
		PolicyName:     "policy",
		Actions:        []milvuspb.RowPolicyAction{milvuspb.RowPolicyAction_Query},
		UsingExpr:      "true",
	}
	ctx, err := PrivilegeInterceptorWithMetaCache(func() Cache { return cache })(GetContext(context.Background(), username+":password"), req)
	require.NoError(t, err)

	cache.EXPECT().GetCollectionID(mock.Anything, oldDBName, aliasName).Return(int64(2), nil).Once()
	cache.EXPECT().GetCollectionInfo(mock.Anything, oldDBName, "", int64(2)).Return(&collectionInfo{
		CollID: int64(2),
		DBName: newDBName,
		Schema: &schemaInfo{CollectionSchema: &schemapb.CollectionSchema{Name: newCollection}},
	}, nil).Once()
	cache.EXPECT().ResolveCollectionAlias(mock.Anything, newDBName, newCollection).Return(newCollection, nil).Once()

	mixCoord := &mocks.MockMixCoordClient{}
	node := &Proxy{mixCoord: mixCoord, metaCache: cache}
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	status, err := node.CreateRowPolicy(ctx, req)
	require.NoError(t, err)
	require.ErrorIs(t, merr.Error(status), merr.ErrPrivilegeNotPermitted)
	mixCoord.AssertNotCalled(t, "CreateRowPolicy", mock.Anything, mock.Anything)
}

func TestProxyRLSAPIStopsOnTargetResolutionError(t *testing.T) {
	ctx := context.Background()
	targetErr := merr.WrapErrAsInputError(merr.WrapErrCollectionNotFound("coll"))
	cache := NewMockCache(t)
	cache.EXPECT().GetCollectionID(mock.Anything, "db", "coll").Return(int64(0), targetErr).Once()
	mixCoord := &mocks.MockMixCoordClient{}
	node := &Proxy{mixCoord: mixCoord, metaCache: cache}
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	status, err := node.CreateRowPolicy(ctx, &milvuspb.CreateRowPolicyRequest{
		DbName:         "db",
		CollectionName: "coll",
		PolicyName:     "policy",
		Actions:        []milvuspb.RowPolicyAction{milvuspb.RowPolicyAction_Query},
		UsingExpr:      "true",
	})
	require.NoError(t, err)
	require.ErrorIs(t, merr.Error(status), merr.ErrCollectionNotFound)
	require.Equal(t, merr.InputError, merr.GetErrorType(merr.Error(status)))
	mixCoord.AssertNotCalled(t, "CreateRowPolicy", mock.Anything, mock.Anything)
}

func TestProxyRLSAPIsRejectInvalidPayloadBeforeForwarding(t *testing.T) {
	ctx := context.Background()
	mixCoord := &mocks.MockMixCoordClient{}
	node := &Proxy{mixCoord: mixCoord}
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	assertErrorStatus := func(t *testing.T, status *commonpb.Status, target error) {
		t.Helper()
		require.NotNil(t, status)
		require.ErrorIs(t, merr.Error(status), target, status.GetReason())
	}

	maxPolicyNameLength := paramtable.Get().ProxyCfg.RLSMaxPolicyNameLength.GetAsInt()
	status, err := node.CreateRowPolicy(ctx, &milvuspb.CreateRowPolicyRequest{
		PolicyName: strings.Repeat("p", maxPolicyNameLength+1),
		PolicyType: milvuspb.RowPolicyType_RowPolicyTypePermissive.Enum(),
		Actions:    []milvuspb.RowPolicyAction{milvuspb.RowPolicyAction_Query},
		UsingExpr:  "true",
	})
	require.NoError(t, err)
	assertErrorStatus(t, status, merr.ErrParameterInvalid)

	status, err = node.CreateRowPolicy(ctx, &milvuspb.CreateRowPolicyRequest{
		PolicyName: "legacy-role-policy",
		Actions:    []milvuspb.RowPolicyAction{milvuspb.RowPolicyAction_Query},
		Roles:      []string{"reader"},
		UsingExpr:  "true",
	})
	require.NoError(t, err)
	assertErrorStatus(t, status, merr.ErrParameterInvalid)

	maxDescriptionLength := paramtable.Get().ProxyCfg.RLSMaxPolicyDescriptionLength.GetAsInt()
	status, err = node.UpdateRowPolicy(ctx, &milvuspb.UpdateRowPolicyRequest{
		PolicyName:  "policy",
		PolicyType:  milvuspb.RowPolicyType_RowPolicyTypePermissive,
		Actions:     []milvuspb.RowPolicyAction{milvuspb.RowPolicyAction_Query},
		UsingExpr:   "true",
		Description: strings.Repeat("d", maxDescriptionLength+1),
	})
	require.NoError(t, err)
	assertErrorStatus(t, status, merr.ErrParameterInvalid)

	status, err = node.UpdateRowPolicy(ctx, &milvuspb.UpdateRowPolicyRequest{
		PolicyName: "legacy-role-policy",
		PolicyType: milvuspb.RowPolicyType_RowPolicyTypePermissive,
		Actions:    []milvuspb.RowPolicyAction{milvuspb.RowPolicyAction_Query},
		Roles:      []string{"reader"},
		UsingExpr:  "true",
	})
	require.NoError(t, err)
	assertErrorStatus(t, status, merr.ErrParameterInvalid)

	maxTags := paramtable.Get().ProxyCfg.RLSMaxTagsPerPrincipal.GetAsInt()
	tags := make(map[string]string, maxTags+1)
	for i := 0; i <= maxTags; i++ {
		tags[string(rune(i+1))] = "value"
	}
	payload, marshalErr := json.Marshal(tags)
	require.NoError(t, marshalErr)
	status, err = node.SetRLSPrincipalTags(ctx, &milvuspb.SetRLSPrincipalTagsRequest{
		PrincipalName: "alice",
		Tags:          string(payload),
	})
	require.NoError(t, err)
	assertErrorStatus(t, status, merr.ErrServiceQuotaExceeded)

	require.NoError(t, paramtable.Get().Save(paramtable.Get().ProxyCfg.RLSMaxPrincipalCacheBytes.Key, "1"))
	t.Cleanup(func() {
		require.NoError(t, paramtable.Get().Reset(paramtable.Get().ProxyCfg.RLSMaxPrincipalCacheBytes.Key))
	})
	status, err = node.SetRLSPrincipalTags(ctx, &milvuspb.SetRLSPrincipalTagsRequest{
		PrincipalName: "alice",
		Tags:          `{"key":"` + strings.Repeat("x", rlsutil.MaxTransportIdentifierLength) + `"}`,
	})
	require.NoError(t, err)
	assertErrorStatus(t, status, merr.ErrParameterTooLarge)

	status, err = node.DeleteRLSPrincipalTags(ctx, &milvuspb.DeleteRLSPrincipalTagsRequest{
		PrincipalName: "alice",
		TagKeys:       make([]string, rlsutil.MaxTransportTagKeys+1),
	})
	require.NoError(t, err)
	assertErrorStatus(t, status, merr.ErrParameterTooLarge)

	oversizedIdentifier := strings.Repeat("x", rlsutil.MaxTransportIdentifierLength+1)
	status, err = node.DropRowPolicy(ctx, &milvuspb.DropRowPolicyRequest{PolicyName: oversizedIdentifier})
	require.NoError(t, err)
	assertErrorStatus(t, status, merr.ErrParameterTooLarge)

	status, err = node.DeleteRLSPrincipalTags(ctx, &milvuspb.DeleteRLSPrincipalTagsRequest{PrincipalName: oversizedIdentifier})
	require.NoError(t, err)
	assertErrorStatus(t, status, merr.ErrParameterTooLarge)

	getResp, err := node.GetRLSPrincipalTags(ctx, &milvuspb.GetRLSPrincipalTagsRequest{PrincipalName: oversizedIdentifier})
	require.NoError(t, err)
	assertErrorStatus(t, getResp.GetStatus(), merr.ErrParameterTooLarge)

	listResp, err := node.ListRowPolicies(ctx, &milvuspb.ListRowPoliciesRequest{CollectionName: oversizedIdentifier})
	require.NoError(t, err)
	assertErrorStatus(t, listResp.GetStatus(), merr.ErrParameterTooLarge)

	getResp, err = node.GetRLSPrincipalTags(ctx, &milvuspb.GetRLSPrincipalTagsRequest{})
	require.NoError(t, err)
	assertErrorStatus(t, getResp.GetStatus(), merr.ErrParameterInvalid)
	mixCoord.AssertNotCalled(t, "CreateRowPolicy", mock.Anything, mock.Anything)
	mixCoord.AssertNotCalled(t, "UpdateRowPolicy", mock.Anything, mock.Anything)
	mixCoord.AssertNotCalled(t, "SetRLSPrincipalTags", mock.Anything, mock.Anything)
	mixCoord.AssertNotCalled(t, "DeleteRLSPrincipalTags", mock.Anything, mock.Anything)
	mixCoord.AssertNotCalled(t, "GetRLSPrincipalTags", mock.Anything, mock.Anything)
	mixCoord.AssertNotCalled(t, "DropRowPolicy", mock.Anything, mock.Anything)
	mixCoord.AssertNotCalled(t, "ListRowPolicies", mock.Anything, mock.Anything)
}

func TestProxySetRLSPrincipalTagsDefersCreationLimitToRootCoord(t *testing.T) {
	ctx := context.Background()
	mixCoord := &mocks.MockMixCoordClient{}
	cache := NewMockCache(t)
	cache.EXPECT().GetCollectionID(mock.Anything, "db", "coll").Return(int64(1), nil).Once()
	cache.EXPECT().GetCollectionInfo(mock.Anything, "db", "", int64(1)).Return(&collectionInfo{
		CollID: int64(1),
		Schema: &schemaInfo{CollectionSchema: &schemapb.CollectionSchema{Name: "coll"}},
	}, nil).Once()
	node := &Proxy{mixCoord: mixCoord, metaCache: cache}
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	paramtable.Get().Save(paramtable.Get().ProxyCfg.RLSMaxPrincipalNameLength.Key, "3")
	t.Cleanup(func() {
		paramtable.Get().Reset(paramtable.Get().ProxyCfg.RLSMaxPrincipalNameLength.Key)
		mixCoord.AssertExpectations(t)
	})

	mixCoord.EXPECT().SetRLSPrincipalTags(mock.Anything, mock.MatchedBy(func(req *milvuspb.SetRLSPrincipalTagsRequest) bool {
		return req.GetPrincipalName() == "alice" && req.GetTags() == `{"dept":"support"}`
	})).Return(merr.Success(), nil).Once()
	status, err := node.SetRLSPrincipalTags(ctx, &milvuspb.SetRLSPrincipalTagsRequest{
		DbName:         "db",
		CollectionName: "coll",
		PrincipalName:  "alice",
		Tags:           `{"dept":"support"}`,
	})
	require.NoError(t, err)
	require.NoError(t, merr.Error(status))
}

func TestProxyUpdateRowPolicyIgnoresCreationNameLimit(t *testing.T) {
	ctx := context.Background()
	mixCoord := &mocks.MockMixCoordClient{}
	cache := NewMockCache(t)
	cache.EXPECT().GetCollectionID(mock.Anything, "db", "coll").Return(int64(1), nil).Once()
	cache.EXPECT().GetCollectionInfo(mock.Anything, "db", "", int64(1)).Return(&collectionInfo{
		CollID: int64(1),
		Schema: &schemaInfo{CollectionSchema: &schemapb.CollectionSchema{Name: "coll"}},
	}, nil).Once()
	node := &Proxy{mixCoord: mixCoord, metaCache: cache}
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	paramtable.Get().Save(paramtable.Get().ProxyCfg.RLSMaxPolicyNameLength.Key, "1")
	t.Cleanup(func() {
		paramtable.Get().Reset(paramtable.Get().ProxyCfg.RLSMaxPolicyNameLength.Key)
		mixCoord.AssertExpectations(t)
	})

	mixCoord.EXPECT().UpdateRowPolicy(mock.Anything, mock.MatchedBy(func(req *milvuspb.UpdateRowPolicyRequest) bool {
		return req.GetPolicyName() == "existing-policy"
	})).Return(merr.Success(), nil).Once()
	status, err := node.UpdateRowPolicy(ctx, &milvuspb.UpdateRowPolicyRequest{
		DbName:         "db",
		CollectionName: "coll",
		PolicyName:     "existing-policy",
		PolicyType:     milvuspb.RowPolicyType_RowPolicyTypePermissive,
		Actions:        []milvuspb.RowPolicyAction{milvuspb.RowPolicyAction_Query},
		UsingExpr:      "true",
	})
	require.NoError(t, err)
	require.NoError(t, merr.Error(status))
}

func TestProxyRLSAPIsRejectNilRequest(t *testing.T) {
	ctx := context.Background()
	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	assertParameterInvalidStatus := func(t *testing.T, status *commonpb.Status) {
		t.Helper()
		require.NotNil(t, status)
		require.True(t, errors.Is(merr.Error(status), merr.ErrParameterInvalid), status.GetReason())
	}

	status, err := node.CreateRowPolicy(ctx, nil)
	require.NoError(t, err)
	assertParameterInvalidStatus(t, status)

	status, err = node.UpdateRowPolicy(ctx, nil)
	require.NoError(t, err)
	assertParameterInvalidStatus(t, status)

	status, err = node.DropRowPolicy(ctx, nil)
	require.NoError(t, err)
	assertParameterInvalidStatus(t, status)

	listPoliciesResp, err := node.ListRowPolicies(ctx, nil)
	require.NoError(t, err)
	assertParameterInvalidStatus(t, listPoliciesResp.GetStatus())

	status, err = node.SetRLSPrincipalTags(ctx, nil)
	require.NoError(t, err)
	assertParameterInvalidStatus(t, status)

	getTagsResp, err := node.GetRLSPrincipalTags(ctx, nil)
	require.NoError(t, err)
	assertParameterInvalidStatus(t, getTagsResp.GetStatus())

	listPrincipalsResp, err := node.ListRLSPrincipals(ctx, nil)
	require.NoError(t, err)
	assertParameterInvalidStatus(t, listPrincipalsResp.GetStatus())

	status, err = node.DeleteRLSPrincipalTags(ctx, nil)
	require.NoError(t, err)
	assertParameterInvalidStatus(t, status)
}
