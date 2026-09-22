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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/proxy/metacache"
	"github.com/milvus-io/milvus/internal/proxy/privilege"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type metadataPrivilegeCoordinator struct {
	types.MixCoordClient
	policies      []string
	describeCalls int
	metadataCalls int
	collectionIDs []int64
}

func (c *metadataPrivilegeCoordinator) ListPolicy(context.Context, *internalpb.ListPolicyRequest, ...grpc.CallOption) (*internalpb.ListPolicyResponse, error) {
	return &internalpb.ListPolicyResponse{
		Status: merr.Success(), PolicyInfos: c.policies,
		UserRoles: []string{funcutil.EncodeUserRoleCache("reader", "metadata_reader")},
	}, nil
}

func (c *metadataPrivilegeCoordinator) DescribeCollection(_ context.Context, req *milvuspb.DescribeCollectionRequest, _ ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
	c.describeCalls++
	if req.GetCollectionID() != 41 && req.GetCollectionName() != "records" && req.GetCollectionName() != "records_alias" {
		return &milvuspb.DescribeCollectionResponse{Status: merr.Status(merr.WrapErrCollectionNotFound("unknown"))}, nil
	}
	return &milvuspb.DescribeCollectionResponse{
		Status: merr.Success(), CollectionID: 41, DbName: "tenant_b",
		Schema: &schemapb.CollectionSchema{Name: "records"}, Aliases: []string{"records_alias"},
	}, nil
}

func (c *metadataPrivilegeCoordinator) GetSegmentsByStates(_ context.Context, req *datapb.GetSegmentsByStatesRequest, _ ...grpc.CallOption) (*datapb.GetSegmentsByStatesResponse, error) {
	c.metadataCalls++
	c.collectionIDs = append(c.collectionIDs, req.GetCollectionID())
	return &datapb.GetSegmentsByStatesResponse{Status: merr.Success()}, nil
}

func (c *metadataPrivilegeCoordinator) GetSegmentInfo(context.Context, *datapb.GetSegmentInfoRequest, ...grpc.CallOption) (*datapb.GetSegmentInfoResponse, error) {
	c.metadataCalls++
	return &datapb.GetSegmentInfoResponse{Status: merr.Success()}, nil
}

func (c *metadataPrivilegeCoordinator) GetLoadSegmentInfo(_ context.Context, req *querypb.GetSegmentInfoRequest, _ ...grpc.CallOption) (*querypb.GetSegmentInfoResponse, error) {
	c.metadataCalls++
	c.collectionIDs = append(c.collectionIDs, req.GetCollectionID())
	return &querypb.GetSegmentInfoResponse{Status: merr.Success()}, nil
}

func (c *metadataPrivilegeCoordinator) GetReplicas(_ context.Context, req *milvuspb.GetReplicasRequest, _ ...grpc.CallOption) (*milvuspb.GetReplicasResponse, error) {
	c.metadataCalls++
	c.collectionIDs = append(c.collectionIDs, req.GetCollectionID())
	return &milvuspb.GetReplicasResponse{Status: merr.Success()}, nil
}

func setupMetadataPrivileges(t *testing.T) (*metadataPrivilegeCoordinator, func(string, ...commonpb.ObjectPrivilege)) {
	t.Helper()
	paramtable.Init()
	for key, value := range map[string]string{
		Params.CommonCfg.AuthorizationEnabled.Key:    "true",
		Params.CommonCfg.RootShouldBindRole.Key:      "false",
		Params.ProxyCfg.ResolveAliasForPrivilege.Key: "false",
	} {
		previous, err := paramtable.GetBaseTable().Load(key)
		require.NoError(t, err)
		require.NoError(t, Params.Save(key, value))
		t.Cleanup(func() { Params.Save(key, previous) })
	}
	privilege.InitPrivilegeGroups()
	t.Cleanup(privilege.ResetPrivilegeCacheForTest)
	t.Cleanup(privilege.CleanPrivilegeCache)
	c := &metadataPrivilegeCoordinator{}
	setPolicy := func(db string, actions ...commonpb.ObjectPrivilege) {
		c.policies = nil
		for _, action := range actions {
			c.policies = append(c.policies, funcutil.PolicyForPrivilege("metadata_reader", commonpb.ObjectType_Collection.String(), "records", action.String(), db))
		}
		require.NoError(t, privilege.InitPrivilegeCache(context.Background(), c))
	}
	setPolicy("") // Start without grants.
	return c, setPolicy
}

func TestCollectionMetadataPrivilegeMappings(t *testing.T) {
	for _, tc := range []struct {
		req    interface{}
		action commonpb.ObjectPrivilege
	}{
		{&milvuspb.GetPersistentSegmentInfoRequest{DbName: "tenant_b", CollectionName: "records"}, commonpb.ObjectPrivilege_PrivilegeGetStatistics},
		{&milvuspb.GetQuerySegmentInfoRequest{DbName: "tenant_b", CollectionName: "records"}, commonpb.ObjectPrivilege_PrivilegeGetStatistics},
		{&milvuspb.GetReplicasRequest{DbName: "tenant_b", CollectionName: "records"}, commonpb.ObjectPrivilege_PrivilegeGetLoadState},
	} {
		mapped := collectionMetadataPrivilegeRequest(tc.req)
		ext, err := funcutil.GetPrivilegeExtObj(mapped)
		require.NoError(t, err)
		assert.Equal(t, commonpb.ObjectType_Collection, ext.ObjectType)
		assert.Equal(t, tc.action, ext.ObjectPrivilege)
		assert.Equal(t, "records", funcutil.GetObjectName(mapped, ext.ObjectNameIndex))
		assert.Equal(t, "tenant_b", GetCurDBNameFromRequestOrContext(context.Background(), mapped))
	}
	show := &milvuspb.ShowCollectionsRequest{}
	assert.Same(t, show, collectionMetadataPrivilegeRequest(show))
}

func TestCollectionMetadataPrivileges(t *testing.T) {
	_, setPolicy := setupMetadataPrivileges(t)
	ctx := NewContextWithMetadata(context.Background(), "reader", "tenant_a")
	check := PrivilegeInterceptorWithMetaCache(func() Cache { return nil })
	for _, tc := range []struct {
		name   string
		req    interface{}
		action commonpb.ObjectPrivilege
	}{
		{"persistent", &milvuspb.GetPersistentSegmentInfoRequest{DbName: "tenant_b", CollectionName: "records"}, commonpb.ObjectPrivilege_PrivilegeGetStatistics},
		{"query", &milvuspb.GetQuerySegmentInfoRequest{DbName: "tenant_b", CollectionName: "records"}, commonpb.ObjectPrivilege_PrivilegeGetStatistics},
		// The handler uses the name when both selectors are supplied.
		{"replicas_name_and_id", &milvuspb.GetReplicasRequest{DbName: "tenant_b", CollectionName: "records", CollectionID: 999, WithShardNodes: true}, commonpb.ObjectPrivilege_PrivilegeGetLoadState},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setPolicy("")
			_, err := check(ctx, tc.req)
			require.Equal(t, codes.PermissionDenied, status.Code(err))
			_, err = check(NewContextWithMetadata(context.Background(), "no_roles", "tenant_b"), tc.req)
			require.Equal(t, codes.PermissionDenied, status.Code(err))
			setPolicy("tenant_a", tc.action)
			_, err = check(ctx, tc.req)
			require.Equal(t, codes.PermissionDenied, status.Code(err))
			setPolicy("tenant_b", tc.action)
			_, err = check(ctx, tc.req)
			require.NoError(t, err)
			setPolicy("tenant_b", commonpb.ObjectPrivilege_PrivilegeGroupReadOnly)
			_, err = check(ctx, tc.req)
			require.NoError(t, err)
			setPolicy("") // Revoke the cached allow.
			_, err = check(ctx, tc.req)
			require.Equal(t, codes.PermissionDenied, status.Code(err))
			_, err = check(context.Background(), tc.req)
			require.Error(t, err)
			_, err = check(NewContextWithMetadata(context.Background(), util.UserRoot, "tenant_b"), tc.req)
			require.NoError(t, err)
		})
	}
	_, err := check(ctx, &milvuspb.ShowCollectionsRequest{})
	require.NoError(t, err, "result-filtered ShowCollections must keep its existing contract")
	_, err = check(ctx, &milvuspb.ConnectRequest{})
	require.NoError(t, err, "public connection setup must not acquire an object privilege")
}

func TestReplicaPrivilegeUsesResolvedIdentity(t *testing.T) {
	for _, warm := range []bool{false, true} {
		t.Run(map[bool]string{false: "cold", true: "warm"}[warm], func(t *testing.T) {
			coord, setPolicy := setupMetadataPrivileges(t)
			cache, err := metacache.NewMetaCache(coord)
			require.NoError(t, err)
			t.Cleanup(cache.Close)
			if warm {
				_, err = cache.GetCollectionInfo(context.Background(), "tenant_b", "", 41)
				require.NoError(t, err)
			}
			check := PrivilegeInterceptorWithMetaCache(func() Cache { return cache })
			ctx := NewContextWithMetadata(context.Background(), "reader", "tenant_a")
			req := &milvuspb.GetReplicasRequest{DbName: "tenant_a", CollectionID: 41, WithShardNodes: true}
			setPolicy("tenant_a", commonpb.ObjectPrivilege_PrivilegeGetLoadState)
			_, err = check(ctx, req)
			require.Equal(t, codes.PermissionDenied, status.Code(err))
			setPolicy("tenant_b", commonpb.ObjectPrivilege_PrivilegeGetLoadState)
			_, err = check(ctx, req)
			require.NoError(t, err)
			assert.Equal(t, 1, coord.describeCalls, "authorization must also run for the cached identity")
			assert.Equal(t, "tenant_a", req.GetDbName(), "do not rewrite the execution request")
			assert.Empty(t, req.GetCollectionName())
			_, err = check(ctx, &milvuspb.GetReplicasRequest{DbName: "tenant_b", CollectionName: "records_alias"})
			require.Equal(t, codes.PermissionDenied, status.Code(err), "alias resolution remains opt-in")
			require.NoError(t, Params.Save(Params.ProxyCfg.ResolveAliasForPrivilege.Key, "true"))
			_, err = check(ctx, &milvuspb.GetReplicasRequest{DbName: "tenant_b", CollectionName: "records_alias"})
			require.NoError(t, err, "canonical collection grants must cover aliases when configured")
		})
	}
}

type incompleteMetadataCache struct {
	Cache
	info *collectionInfo
	err  error
}

func (c incompleteMetadataCache) GetCollectionInfo(context.Context, string, string, int64) (*collectionInfo, error) {
	return c.info, c.err
}

func TestReplicaPrivilegeFailsClosed(t *testing.T) {
	_, setPolicy := setupMetadataPrivileges(t)
	setPolicy("tenant_b", commonpb.ObjectPrivilege_PrivilegeGetLoadState)
	ctx := NewContextWithMetadata(context.Background(), "reader", "tenant_b")
	for _, cache := range []Cache{
		nil,
		incompleteMetadataCache{},
		incompleteMetadataCache{info: &collectionInfo{CollID: 41}},
		incompleteMetadataCache{info: &collectionInfo{CollID: 41, DBName: "tenant_b"}},
		incompleteMetadataCache{info: &collectionInfo{CollID: 41, Schema: &schemaInfo{CollectionSchema: &schemapb.CollectionSchema{Name: "records"}}}},
		incompleteMetadataCache{info: &collectionInfo{CollID: 41, DBName: "tenant_b", Schema: &schemaInfo{CollectionSchema: &schemapb.CollectionSchema{}}}},
		incompleteMetadataCache{info: &collectionInfo{CollID: 42, DBName: "tenant_b", Schema: &schemaInfo{CollectionSchema: &schemapb.CollectionSchema{Name: "records"}}}},
		incompleteMetadataCache{err: merr.WrapErrServiceUnavailable("coordinator unavailable")},
	} {
		_, err := PrivilegeInterceptorWithMetaCache(func() Cache { return cache })(ctx, &milvuspb.GetReplicasRequest{CollectionID: 41})
		require.Error(t, err)
	}
	check := PrivilegeInterceptorWithMetaCache(func() Cache { return nil })
	_, err := check(ctx, &milvuspb.GetReplicasRequest{})
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	_, err = check(NewContextWithMetadata(context.Background(), util.UserRoot, ""), &milvuspb.GetReplicasRequest{CollectionID: 41})
	require.NoError(t, err, "the existing root exemption must not require a metadata lookup")
	require.NoError(t, Params.Save(Params.CommonCfg.RootShouldBindRole.Key, "true"))
	_, err = check(NewContextWithMetadata(context.Background(), util.UserRoot, ""), &milvuspb.GetReplicasRequest{CollectionName: "records", DbName: "tenant_b"})
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	require.NoError(t, Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "false"))
	_, err = check(context.Background(), &milvuspb.GetReplicasRequest{CollectionID: 41})
	require.NoError(t, err)
}

func TestProxyCollectionMetadataRejectsBeforeCoordinator(t *testing.T) {
	setupMetadataPrivileges(t)
	node := &Proxy{getMetaCache: func() Cache { return nil }}
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	ctx := NewContextWithMetadata(context.Background(), "no_roles", "tenant_b")
	// mixCoord is deliberately nil: a denied direct call must not dispatch to it.
	persistent, err := node.GetPersistentSegmentInfo(ctx, &milvuspb.GetPersistentSegmentInfoRequest{DbName: "tenant_b", CollectionName: "records"})
	require.NoError(t, err)
	require.ErrorIs(t, merr.Error(persistent.GetStatus()), merr.ErrPrivilegeNotPermitted)
	query, err := node.GetQuerySegmentInfo(ctx, &milvuspb.GetQuerySegmentInfoRequest{DbName: "tenant_b", CollectionName: "records"})
	require.NoError(t, err)
	require.ErrorIs(t, merr.Error(query.GetStatus()), merr.ErrPrivilegeNotPermitted)
	replicas, err := node.GetReplicas(ctx, &milvuspb.GetReplicasRequest{DbName: "tenant_b", CollectionName: "records"})
	require.NoError(t, err)
	require.ErrorIs(t, merr.Error(replicas.GetStatus()), merr.ErrPrivilegeNotPermitted)
}

func TestProxyCollectionMetadataAuthorizedCalls(t *testing.T) {
	coord, setPolicy := setupMetadataPrivileges(t)
	setPolicy("tenant_b", commonpb.ObjectPrivilege_PrivilegeGetStatistics, commonpb.ObjectPrivilege_PrivilegeGetLoadState)
	cache, err := metacache.NewMetaCache(coord)
	require.NoError(t, err)
	t.Cleanup(cache.Close)
	node := &Proxy{getMetaCache: func() Cache { return cache }, mixCoord: coord}
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	ctx := NewContextWithMetadata(context.Background(), "reader", "tenant_b")
	persistent, err := node.GetPersistentSegmentInfo(ctx, &milvuspb.GetPersistentSegmentInfoRequest{DbName: "tenant_b", CollectionName: "records"})
	require.NoError(t, err)
	require.NoError(t, merr.Error(persistent.GetStatus()))
	query, err := node.GetQuerySegmentInfo(ctx, &milvuspb.GetQuerySegmentInfoRequest{DbName: "tenant_b", CollectionName: "records"})
	require.NoError(t, err)
	require.NoError(t, merr.Error(query.GetStatus()))
	replicas, err := node.GetReplicas(ctx, &milvuspb.GetReplicasRequest{CollectionID: 41, DbName: "tenant_a"})
	require.NoError(t, err)
	require.NoError(t, merr.Error(replicas.GetStatus()))
	require.Equal(t, 4, coord.metadataCalls)
	require.Equal(t, []int64{41, 41, 41}, coord.collectionIDs)
}
