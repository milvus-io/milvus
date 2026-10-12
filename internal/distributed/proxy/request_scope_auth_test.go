// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package grpcproxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/internal/proxy/requestcontext"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestHTTPAuthenticationRequestScopeUsesVerifiedSubject(t *testing.T) {
	for _, mode := range []string{"password", "apikey", "rejected"} {
		t.Run(mode, func(t *testing.T) {
			ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
			parent, cancel := context.WithCancel(requestcontext.WithAuthenticatedUser(context.Background(), "previous_user"))
			defer cancel()
			ctx.Request = httptest.NewRequest(http.MethodPost, "/v2/vectordb/entities/search", nil).WithContext(parent)
			want := "alice"
			if mode == "apikey" {
				ctx.Request.Header.Set("Authorization", "Bearer test_api_key")
				want = "mapped_user"
				defer mockey.Mock(proxy.VerifyAPIKey).Return(want, nil).Build().UnPatch()
			} else {
				ctx.Request.SetBasicAuth("alice", "test_password")
				defer mockey.Mock(proxy.PasswordVerify).Return(mode != "rejected").Build().UnPatch()
				if mode == "rejected" {
					want = ""
					defer mockey.Mock(proxy.VerifyAPIKey).Return("", merr.ErrNeedAuthenticate).Build().UnPatch()
				}
			}
			authenticate(ctx)
			require.Equal(t, want, requestcontext.AuthenticatedUser(ctx.Request.Context()))
			require.Equal(t, mode == "rejected", ctx.IsAborted())
			require.Equal(t, "previous_user", requestcontext.AuthenticatedUser(parent), "authentication does not mutate its parent")
			cancel()
			require.ErrorIs(t, ctx.Request.Context().Err(), context.Canceled)
			if mode == "rejected" {
				require.Equal(t, http.StatusUnauthorized, ctx.Writer.Status())
			}
		})
	}
}
