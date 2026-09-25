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
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v3/util/crypto"
)

type metadataIdentityProxy struct {
	types.ProxyComponent
	ctx            context.Context
	dbName         string
	collectionName string
}

func (p *metadataIdentityProxy) GetPersistentSegmentInfo(ctx context.Context, req *milvuspb.GetPersistentSegmentInfoRequest) (*milvuspb.GetPersistentSegmentInfoResponse, error) {
	p.ctx = ctx
	p.dbName, p.collectionName = req.GetDbName(), req.GetCollectionName()
	return &milvuspb.GetPersistentSegmentInfoResponse{}, nil
}

func (p *metadataIdentityProxy) GetQuerySegmentInfo(ctx context.Context, req *milvuspb.GetQuerySegmentInfoRequest) (*milvuspb.GetQuerySegmentInfoResponse, error) {
	p.ctx = ctx
	p.dbName, p.collectionName = req.GetDbName(), req.GetCollectionName()
	return &milvuspb.GetQuerySegmentInfoResponse{}, nil
}

func (p *metadataIdentityProxy) GetReplicas(ctx context.Context, req *milvuspb.GetReplicasRequest) (*milvuspb.GetReplicasResponse, error) {
	p.ctx = ctx
	p.dbName, p.collectionName = req.GetDbName(), req.GetCollectionName()
	return &milvuspb.GetReplicasResponse{}, nil
}

func TestMetadataHandlersForwardVerifiedPrincipal(t *testing.T) {
	for _, inherited := range []bool{false, true} {
		for _, username := range []string{"root", "reader", ""} {
			for _, method := range []string{"persistent", "query", "replicas"} {
				t.Run(fmt.Sprintf("%s/%s/inherited=%v", method, username, inherited), func(t *testing.T) {
					p := &metadataIdentityProxy{}
					h := &Handlers{proxy: p}
					c, _ := gin.CreateTestContext(httptest.NewRecorder())
					// Existing incoming metadata must not override the principal
					// verified by HTTP middleware, including when it is missing.
					originalMD := metadata.Pairs("request-id", "metadata-request")
					if inherited {
						originalMD.Set(util.HeaderAuthorize, crypto.Base64Encode("unverified:password"))
						originalMD.Set(util.HeaderDBName, "unverified_db")
					}
					requestCtx, cancel := context.WithTimeout(metadata.NewIncomingContext(context.Background(), originalMD), time.Minute)
					defer cancel()
					body := `{"dbName":"tenant_b","collectionName":"records"}`
					if method == "replicas" {
						body = `{"db_name":"tenant_b","collection_name":"records"}`
					}
					c.Request = httptest.NewRequest(http.MethodGet, "/", strings.NewReader(body)).WithContext(requestCtx)
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
					require.Equal(t, "tenant_b", p.dbName)
					require.Equal(t, "records", p.collectionName)
					forwardedMD, ok := metadata.FromIncomingContext(p.ctx)
					require.True(t, ok)
					require.Equal(t, []string{"tenant_b"}, forwardedMD.Get(util.HeaderDBName))
					require.Equal(t, []string{"metadata-request"}, forwardedMD.Get("request-id"))
					if inherited {
						require.Equal(t, []string{"unverified_db"}, originalMD.Get(util.HeaderDBName), "do not mutate the parent metadata")
					}
					deadline, ok := p.ctx.Deadline()
					require.True(t, ok)
					requestDeadline, _ := requestCtx.Deadline()
					require.Equal(t, requestDeadline, deadline)
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
}
