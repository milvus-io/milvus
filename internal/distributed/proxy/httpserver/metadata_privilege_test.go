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
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
)

type metadataIdentityProxy struct {
	types.ProxyComponent
	ctx context.Context
}

func (p *metadataIdentityProxy) GetPersistentSegmentInfo(ctx context.Context, _ *milvuspb.GetPersistentSegmentInfoRequest) (*milvuspb.GetPersistentSegmentInfoResponse, error) {
	p.ctx = ctx
	return &milvuspb.GetPersistentSegmentInfoResponse{}, nil
}

func (p *metadataIdentityProxy) GetQuerySegmentInfo(ctx context.Context, _ *milvuspb.GetQuerySegmentInfoRequest) (*milvuspb.GetQuerySegmentInfoResponse, error) {
	p.ctx = ctx
	return &milvuspb.GetQuerySegmentInfoResponse{}, nil
}

func (p *metadataIdentityProxy) GetReplicas(ctx context.Context, _ *milvuspb.GetReplicasRequest) (*milvuspb.GetReplicasResponse, error) {
	p.ctx = ctx
	return &milvuspb.GetReplicasResponse{}, nil
}

func TestMetadataHandlersForwardVerifiedPrincipal(t *testing.T) {
	for _, username := range []string{"root", "reader", ""} {
		for _, method := range []string{"persistent", "query", "replicas"} {
			t.Run(method+"/"+username, func(t *testing.T) {
				p := &metadataIdentityProxy{}
				h := &Handlers{proxy: p}
				c, _ := gin.CreateTestContext(httptest.NewRecorder())
				requestCtx, cancel := context.WithCancel(context.Background())
				defer cancel()
				c.Request = httptest.NewRequest(http.MethodGet, "/", strings.NewReader(`{"db_name":"tenant_b","collection_name":"records"}`)).WithContext(requestCtx)
				c.Request.Header.Set("Content-Type", "application/json")
				// The handler must use the verified middleware identity, never this
				// caller-controlled credential header or Gin's context fallback.
				c.Request.SetBasicAuth("forged_user", "password")
				if username != "" {
					c.Set(ContextUsername, username)
				}
				var err error
				switch method {
				case "persistent":
					_, err = h.handleGetPersistentSegmentInfo(c)
				case "query":
					_, err = h.handleGetQuerySegmentInfo(c)
				case "replicas":
					_, err = h.handleGetReplicas(c)
				}
				require.NoError(t, err)
				require.NotNil(t, p.ctx)
				actual, _, authErr := contextutil.GetAuthInfoFromContext(p.ctx)
				if username == "" {
					require.Error(t, authErr)
				} else {
					require.NoError(t, authErr)
					require.Equal(t, username, actual)
				}
				cancel()
				require.ErrorIs(t, p.ctx.Err(), context.Canceled)
			})
		}
	}
}
