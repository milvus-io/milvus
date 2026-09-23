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

package httpserver

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type describeIdentityProxy struct {
	types.ProxyComponent
	describe func(context.Context, *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error)
}

func (p *describeIdentityProxy) DescribeCollection(ctx context.Context, req *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	return p.describe(ctx, req)
}

func TestLegacyDescribeCollectionForwardsVerifiedIdentity(t *testing.T) {
	for _, username := range []string{"alice", ""} {
		t.Run("user="+username, func(t *testing.T) {
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c.Request = httptest.NewRequest(http.MethodGet, "/api/v1/collection", strings.NewReader(`{"db_name":"tenant_b","collection_name":"orders"}`)).WithContext(ctx)
			c.Request.Header.Set("Content-Type", "application/json")
			c.Request.Header.Set("x-user", "root")
			c.Request.SetBasicAuth("root", "unverified")
			if username != "" {
				c.Set(ContextUsername, username)
			}
			cancel()
			called := false
			h := NewHandlers(&describeIdentityProxy{describe: func(ctx context.Context, req *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
				called = true
				require.Equal(t, "tenant_b", req.GetDbName())
				require.Equal(t, "orders", req.GetCollectionName())
				require.Equal(t, "tenant_b", proxy.GetCurDBNameFromContextOrDefault(ctx))
				user, err := contextutil.GetCurUserFromContext(ctx)
				if username == "" {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
					require.Equal(t, username, user)
				}
				require.ErrorIs(t, ctx.Err(), context.Canceled)
				return &milvuspb.DescribeCollectionResponse{Status: merr.Success()}, nil
			}})
			_, err := h.handleDescribeCollection(c)
			require.NoError(t, err)
			require.True(t, called)
		})
	}
}
