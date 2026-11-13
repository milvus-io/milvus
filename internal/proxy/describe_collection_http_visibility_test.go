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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	mhttp "github.com/milvus-io/milvus/internal/http"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proxy/privilege"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestConsoleDescribeCollectionVisibility(t *testing.T) {
	require.NoError(t, Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "true"))
	require.NoError(t, Params.Save(Params.CommonCfg.RootShouldBindRole.Key, "true"))
	t.Cleanup(func() {
		Params.Reset(Params.CommonCfg.AuthorizationEnabled.Key)
		Params.Reset(Params.CommonCfg.RootShouldBindRole.Key)
	})
	for _, principal := range []string{"alice", "missing", "verified administrator"} {
		t.Run(principal, func(t *testing.T) {
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Request = httptest.NewRequest(http.MethodGet, "/?db_name=tenant_b&collection_name=orders", nil)
			c.Request.Header.Set("x-user", "root")
			if principal == "alice" {
				c.Set("username", "alice")
			}
			if principal == "verified administrator" {
				// Produce the real, unforgeable marker through management authentication.
				mhttp.RegisterManagementVerifier(mhttp.VerifierSlotProxy, func(context.Context, string, string) error { return nil })
				t.Cleanup(func() { mhttp.RegisterManagementVerifier(mhttp.VerifierSlotProxy, nil) })
				c.Request.SetBasicAuth(util.UserRoot, "test-only-password")
				c.Request.Header.Set(mhttp.AdminRequestHeader, "true")
				decision := mhttp.CheckAdminRequest(c.Request, mhttp.CollectionDescPath, false)
				require.True(t, decision.Allowed())
				c.Request = decision.AuthenticatedRequest(c.Request)
			}
			mix := mocks.NewMockMixCoordClient(t)
			if principal != "missing" {
				mix.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(
					func(ctx context.Context, req *milvuspb.DescribeCollectionRequest, _ ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
						require.Equal(t, "tenant_b", req.GetDbName())
						md, _ := metadata.FromOutgoingContext(ctx)
						if principal == "alice" {
							user, err := contextutil.GetCurUserFromContext(metadata.NewIncomingContext(context.Background(), md))
							require.NoError(t, err)
							require.Equal(t, "alice", user)
							return &milvuspb.DescribeCollectionResponse{Status: merr.Status(merr.ErrPrivilegeNotPermitted)}, nil
						}
						require.Empty(t, md.Get(util.HeaderAuthorize), "the verified management view does not depend on data-plane grants")
						return &milvuspb.DescribeCollectionResponse{Status: merr.Success(), CollectionID: 77, CollectionName: "orders", Schema: &schemapb.CollectionSchema{Name: "orders"}}, nil
					}).Once()
			}
			if principal == "verified administrator" {
				mix.EXPECT().ShowPartitions(mock.Anything, mock.Anything).Return(&milvuspb.ShowPartitionsResponse{Status: merr.Success()}, nil).Once()
			}
			describeCollection(&Proxy{mixCoord: mix})(c)
			if principal == "verified administrator" {
				require.Equal(t, http.StatusOK, w.Code)
				require.Contains(t, w.Body.String(), "orders")
			} else {
				require.NotEqual(t, http.StatusOK, w.Code)
				require.NotContains(t, w.Body.String(), "orders")
				require.NotContains(t, w.Body.String(), "fields")
			}
		})
	}
}

func TestConsoleLocalCollectionVisibility(t *testing.T) {
	for key, value := range map[string]string{
		Params.CommonCfg.AuthorizationEnabled.Key:           "true",
		Params.ProxyCfg.EnableLocalCollectionVisibility.Key: "true",
	} {
		require.NoError(t, Params.Save(key, value))
		t.Cleanup(func() { Params.Reset(key) })
	}
	t.Cleanup(privilege.ResetPrivilegeCacheForTest)
	for _, tc := range []struct {
		name, db string
		allow    bool
		status   int
	}{
		{"allow", "tenant_b", true, http.StatusOK},
		{"deny", "tenant_b", false, http.StatusForbidden},
		{"missing resolved database", "", true, http.StatusInternalServerError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mix := mocks.NewMockMixCoordClient(t)
			role := &internalpb.RoleCollectionVisibility{Role: "reader"}
			if tc.allow {
				role.Rules = []*internalpb.CollectionVisibilityRule{{DbName: "tenant_b", CollectionName: "orders"}}
			}
			mix.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
				Status: merr.Success(), UserRoles: []string{funcutil.EncodeUserRoleCache("alice", "reader")},
				CollectionVisibility: &internalpb.CollectionVisibilitySnapshot{FormatVersion: 1, Roles: []*internalpb.RoleCollectionVisibility{role}},
			}, nil).Once()
			require.NoError(t, privilege.InitPrivilegeCache(context.Background(), mix))
			mix.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, _ *milvuspb.DescribeCollectionRequest, _ ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
				md, _ := metadata.FromOutgoingContext(ctx)
				require.Empty(t, md.Get(util.HeaderAuthorize), "lookup is internal; authorization follows locally")
				return &milvuspb.DescribeCollectionResponse{Status: merr.Success(), DbName: tc.db, CollectionID: 77, Schema: &schemapb.CollectionSchema{Name: "orders"}}, nil
			}).Once()
			if tc.status == http.StatusOK {
				mix.EXPECT().ShowPartitions(mock.Anything, mock.Anything).Return(&milvuspb.ShowPartitionsResponse{Status: merr.Success()}, nil).Once()
			}
			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			ctx.Request = httptest.NewRequest(http.MethodGet, "/?db_name=tenant_b&collection_name=orders", nil)
			ctx.Set("username", "alice")
			describeCollection(&Proxy{mixCoord: mix})(ctx)
			require.Equal(t, tc.status, recorder.Code)
			if tc.status != http.StatusOK {
				require.NotContains(t, recorder.Body.String(), "collection_id")
			}
		})
	}
}
