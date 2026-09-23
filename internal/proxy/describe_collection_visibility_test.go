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
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v3/util/crypto"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestDescribeCollectionCachedVisibility(t *testing.T) {
	require.NoError(t, Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "true"))
	require.NoError(t, Params.Save(Params.ProxyCfg.EnableCachedServiceProvider.Key, "true"))
	t.Cleanup(func() {
		Params.Reset(Params.CommonCfg.AuthorizationEnabled.Key)
		Params.Reset(Params.ProxyCfg.EnableCachedServiceProvider.Key)
	})

	for _, prewarm := range []bool{false, true} {
		for _, lookup := range []string{"name", "alias", "id in another database"} {
			t.Run(lookup+"/prewarm="+strconv.FormatBool(prewarm), func(t *testing.T) {
				const collectionID int64 = 77
				mix := mocks.NewMockMixCoordClient(t)
				describes := 0
				allowed := false
				visibilityChecks := 0
				mix.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(
					func(ctx context.Context, req *milvuspb.DescribeCollectionRequest, _ ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
						resp := &milvuspb.DescribeCollectionResponse{
							Status: merr.Success(), CollectionID: collectionID, DbId: 9, DbName: "tenant_b",
							Aliases: []string{"orders_alias"},
							Schema: &schemapb.CollectionSchema{Name: "orders", Fields: []*schemapb.FieldSchema{
								{FieldID: common.StartOfUserFieldID, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
							}},
						}
						if req.GetCollectionID() == 0 || req.GetCollectionName() == "" {
							// Shared cache fills are internal metadata reads. The user
							// must still pass the separate authoritative check below.
							// Model a real RPC: an outgoing user would make RootCoord
							// apply that user's grants even during a cache fill.
							md, _ := metadata.FromOutgoingContext(ctx)
							if len(md.Get(util.HeaderAuthorize)) != 0 {
								return &milvuspb.DescribeCollectionResponse{Status: merr.Status(merr.ErrPrivilegeNotPermitted)}, nil
							}
							describes++
							return resp, nil
						}
						visibilityChecks++
						require.Equal(t, "tenant_b", req.GetDbName(), "authorize the resolved database, not the ID lookup hint")
						require.Equal(t, "orders", req.GetCollectionName(), "authorize the canonical name, including aliases")
						md, _ := metadata.FromOutgoingContext(ctx)
						require.Len(t, md.Get(util.HeaderAuthorize), 1)
						user, err := contextutil.GetCurUserFromContext(metadata.NewIncomingContext(context.Background(), md))
						require.NoError(t, err)
						if user != util.UserRoot && !allowed {
							require.Equal(t, "alice", user)
							return &milvuspb.DescribeCollectionResponse{Status: merr.Status(merr.ErrPrivilegeNotPermitted)}, nil
						}
						return resp, nil
					})
				cache := mustNewMetaCacheForTest(mix)
				t.Cleanup(cache.Close)
				node := &Proxy{mixCoord: mix, metaCache: cache}
				node.UpdateStateCode(commonpb.StateCode_Healthy)
				if prewarm {
					resp, err := node.DescribeCollection(NewContextWithMetadata(context.Background(), util.UserRoot, "tenant_b"),
						&milvuspb.DescribeCollectionRequest{DbName: "tenant_b", CollectionName: "orders"})
					require.NoError(t, err)
					require.NoError(t, merr.Error(resp.GetStatus()))
				}
				req := &milvuspb.DescribeCollectionRequest{DbName: "tenant_b", CollectionName: "orders"}
				switch lookup {
				case "alias":
					req.CollectionName = "orders_alias"
				case "id in another database":
					req = &milvuspb.DescribeCollectionRequest{DbName: "tenant_a", CollectionID: collectionID}
				}
				original := proto.Clone(req)
				ctx := NewContextWithMetadata(context.Background(), "alice", "tenant_a")
				// Existing outgoing metadata must not override the authenticated user.
				ctx = metadata.AppendToOutgoingContext(ctx, util.HeaderAuthorize, crypto.Base64Encode("stale_user:stale_user"))
				for _, grant := range []bool{true, false, true, false} {
					allowed = grant
					before := visibilityChecks
					resp, err := node.DescribeCollection(ctx, req)
					require.NoError(t, err)
					if grant {
						require.NoError(t, merr.Error(resp.GetStatus()))
						require.Equal(t, collectionID, resp.GetCollectionID())
						require.Equal(t, "tenant_b", resp.GetDbName())
					} else {
						require.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrPrivilegeNotPermitted)
						require.Nil(t, resp.GetSchema())
						require.Zero(t, resp.GetCollectionID())
					}
					require.Equal(t, before+1, visibilityChecks, "check each response, including after revoke on the same hot cache")
					require.True(t, proto.Equal(original, req), "authorization must not mutate the request")
				}
				require.Equal(t, 1, describes, "metadata remains shared and cached; authorization does not")
			})
		}
	}
}

func TestDescribeCollectionRequiresIdentity(t *testing.T) {
	require.NoError(t, Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "true"))
	t.Cleanup(func() { Params.Reset(Params.CommonCfg.AuthorizationEnabled.Key) })
	for _, ctx := range []context.Context{
		context.Background(),
		metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-user", "root")),
		metadata.NewIncomingContext(context.Background(), metadata.Pairs(util.HeaderAuthorize, "malformed")),
		metadata.NewIncomingContext(context.Background(), metadata.Pairs(util.HeaderAuthorize, crypto.Base64Encode(":password"))),
	} {
		// No cache or coordinator is installed: denial must precede either lookup.
		resp, err := (&Proxy{}).DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{CollectionID: 77})
		require.NoError(t, err)
		require.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrPrivilegeNotPermitted)
		require.Nil(t, resp.GetSchema())
	}
}

func TestDescribeCollectionVisibilityFailsClosed(t *testing.T) {
	require.NoError(t, Params.Save(Params.ProxyCfg.EnableCachedServiceProvider.Key, "true"))
	t.Cleanup(func() { Params.Reset(Params.ProxyCfg.EnableCachedServiceProvider.Key) })
	require.NoError(t, Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "true"))
	t.Cleanup(func() { Params.Reset(Params.CommonCfg.AuthorizationEnabled.Key) })
	ctx := NewContextWithMetadata(context.Background(), "alice", "tenant_a")
	collection := &collectionInfo{CollID: 77, DBName: "tenant_b", Schema: mustNewSchemaInfo(&schemapb.CollectionSchema{Name: "orders"})}
	for _, tc := range []struct {
		name string
		resp *milvuspb.DescribeCollectionResponse
		err  error
		want error
	}{
		{"same name with a different id", &milvuspb.DescribeCollectionResponse{Status: merr.Success(), CollectionName: "orders", CollectionID: 78}, nil, merr.ErrServiceUnavailable},
		{"coordinator status failure", &milvuspb.DescribeCollectionResponse{Status: merr.Status(merr.ErrServiceUnavailable)}, nil, merr.ErrServiceUnavailable},
		{"transport failure", nil, merr.ErrServiceUnavailable, merr.ErrServiceUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mix := mocks.NewMockMixCoordClient(t)
			mix.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(tc.resp, tc.err).Once()
			cache := NewMockCache(t)
			cache.EXPECT().GetCollectionID(mock.Anything, "tenant_b", "orders").Return(int64(77), nil).Once()
			cache.EXPECT().GetCollectionInfo(mock.Anything, "tenant_b", "orders", int64(77)).Return(collection, nil).Once()
			node := &Proxy{mixCoord: mix, metaCache: cache}
			node.UpdateStateCode(commonpb.StateCode_Healthy)
			resp, err := node.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{DbName: "tenant_b", CollectionName: "orders"})
			require.NoError(t, err)
			require.ErrorIs(t, merr.Error(resp.GetStatus()), tc.want)
			require.Nil(t, resp.GetSchema())
			require.Zero(t, resp.GetCollectionID())
		})
	}
	unknownDB := *collection
	unknownDB.DBName = ""
	provider := &CachedProxyServiceProvider{Proxy: &Proxy{}}
	require.ErrorIs(t, provider.checkCollectionVisibility(ctx, &unknownDB), merr.ErrServiceUnavailable)
	// Authorization-disabled deployments retain their existing metadata behavior.
	require.NoError(t, Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "false"))
	require.NoError(t, provider.checkCollectionVisibility(context.Background(), &unknownDB))
}

func TestDescribeCollectionRemoteForwardsAuthenticatedIdentity(t *testing.T) {
	mix := NewMixCoordMock()
	mix.SetDescribeCollectionFunc(func(ctx context.Context, req *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
		md, _ := metadata.FromOutgoingContext(ctx)
		require.Equal(t, []string{crypto.Base64Encode("alice:alice")}, md.Get(util.HeaderAuthorize))
		return &milvuspb.DescribeCollectionResponse{Status: merr.Status(merr.ErrPrivilegeNotPermitted)}, nil
	})
	ctx := NewContextWithMetadata(context.Background(), "alice", "tenant_a")
	ctx = metadata.AppendToOutgoingContext(ctx, util.HeaderAuthorize, crypto.Base64Encode("root:root"))
	task := &describeCollectionTask{
		mixCoord: mix,
		DescribeCollectionRequest: &milvuspb.DescribeCollectionRequest{
			CollectionID: 77, DbName: "tenant_a",
		},
	}
	require.NoError(t, task.Execute(ctx))
	require.ErrorIs(t, merr.Error(task.result.GetStatus()), merr.ErrPrivilegeNotPermitted)
	require.Nil(t, task.result.GetSchema())
}

func TestDescribeCollectionRPCContextPreservesRequest(t *testing.T) {
	type requestKey struct{}
	parent, cancel := context.WithTimeout(context.WithValue(context.Background(), requestKey{}, "request-value"), time.Minute)
	defer cancel()
	ctx := NewContextWithMetadata(parent, "alice", "tenant_a")
	staleIdentity := crypto.Base64Encode("root:root")
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs(
		util.HeaderAuthorize, staleIdentity,
		"request-id", "describe-request",
	))

	rpcCtx := describeCollectionRPCContext(ctx)
	md, ok := metadata.FromOutgoingContext(rpcCtx)
	require.True(t, ok)
	require.Equal(t, []string{crypto.Base64Encode("alice:alice")}, md.Get(util.HeaderAuthorize))
	require.Equal(t, []string{"describe-request"}, md.Get("request-id"))
	require.Equal(t, "request-value", rpcCtx.Value(requestKey{}))
	wantDeadline, _ := parent.Deadline()
	deadline, ok := rpcCtx.Deadline()
	require.True(t, ok)
	require.Equal(t, wantDeadline, deadline)

	// A forwarded identity must not mutate metadata held by another user of
	// the parent context, and disconnects must still cancel the coordinator RPC.
	original, _ := metadata.FromOutgoingContext(ctx)
	require.Equal(t, []string{staleIdentity}, original.Get(util.HeaderAuthorize))
	cancel()
	require.ErrorIs(t, rpcCtx.Err(), context.Canceled)
}
