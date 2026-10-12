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
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
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
