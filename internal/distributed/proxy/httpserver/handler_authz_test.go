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
	"sync/atomic"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/internal/proxy/privilege"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// authzProbeProxy counts how often a privileged operation is reached.
type authzProbeProxy struct {
	types.ProxyComponent

	createCredentialCalls atomic.Int32
}

func (m *authzProbeProxy) CreateCredential(ctx context.Context, req *milvuspb.CreateCredentialRequest) (*commonpb.Status, error) {
	m.createCredentialCalls.Add(1)
	return testStatus, nil
}

// newLowLevelServerForAuthzTest builds the low-level REST server the way the
// metrics port serves it, with an authentication middleware that only records
// the parsed username, so the test exercises the authorization added on top.
func newLowLevelServerForAuthzTest(t *testing.T, pxy types.ProxyComponent) *gin.Engine {
	t.Helper()
	h := NewHandlers(pxy)
	ginHandler := gin.Default()
	apiv1 := ginHandler.Group("/api/v1", func(c *gin.Context) {
		username, _, ok := ParseUsernamePassword(c)
		if !ok || username == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{HTTPReturnCode: merr.Code(merr.ErrNeedAuthenticate), HTTPReturnMessage: merr.ErrNeedAuthenticate.Error()})
			return
		}
		c.Set(ContextUsername, username)
	})
	h.RegisterRoutesTo(apiv1)
	return ginHandler
}

// TestLowLevelRESTAuthorization verifies that the low-level REST API enforces
// the same RBAC rules as the gRPC path instead of letting any authenticated
// user reach privileged proxy operations.
func TestLowLevelRESTAuthorization(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(proxy.Params.CommonCfg.AuthorizationEnabled.Key, "true")
	t.Cleanup(func() { paramtable.Get().Reset(proxy.Params.CommonCfg.AuthorizationEnabled.Key) })

	cache := proxy.InitEmptyMetaCacheForTest()
	require.NotNil(t, cache)

	mp := &authzProbeProxy{}
	testEngine := newLowLevelServerForAuthzTest(t, proxyComponentWithMetaCache{ProxyComponent: mp, metaCache: cache})

	postCreateCredential := func(t *testing.T, user string) *httptest.ResponseRecorder {
		t.Helper()
		body := `{"username":"backdoor","password":"QmFja2Rvb3IjMTIz"}`
		req := httptest.NewRequest(http.MethodPost, "/api/v1/credential", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		if user != "" {
			req.SetBasicAuth(user, "pwd")
		}
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		return w
	}

	t.Run("user without grants is denied", func(t *testing.T) {
		w := postCreateCredential(t, "lowpriv")
		assert.Equal(t, http.StatusForbidden, w.Code, w.Body.String())
		assert.Equal(t, int32(0), mp.createCredentialCalls.Load())
	})

	t.Run("request without credentials is rejected", func(t *testing.T) {
		w := postCreateCredential(t, "")
		assert.Equal(t, http.StatusUnauthorized, w.Code, w.Body.String())
		assert.Equal(t, int32(0), mp.createCredentialCalls.Load())
	})

	t.Run("granted user reaches the operation", func(t *testing.T) {
		privCache := privilege.GetPrivilegeCache()
		require.NotNil(t, privCache)
		require.NoError(t, privCache.RefreshPolicyInfo(typeutil.CacheOp{
			OpType: typeutil.CacheGrantPrivilege,
			OpKey:  funcutil.PolicyForPrivilege(util.RolePublic, commonpb.ObjectType_Global.String(), util.AnyWord, commonpb.ObjectPrivilege_PrivilegeCreateUser.String(), util.AnyWord),
		}))

		w := postCreateCredential(t, "lowpriv")
		assert.Equal(t, http.StatusOK, w.Code, w.Body.String())
		assert.Equal(t, int32(1), mp.createCredentialCalls.Load())
	})

	t.Run("authorization disabled keeps the legacy passthrough", func(t *testing.T) {
		paramtable.Get().Save(proxy.Params.CommonCfg.AuthorizationEnabled.Key, "false")
		t.Cleanup(func() { paramtable.Get().Reset(proxy.Params.CommonCfg.AuthorizationEnabled.Key) })

		w := postCreateCredential(t, "")
		assert.Equal(t, http.StatusOK, w.Code, w.Body.String())
		assert.Equal(t, int32(2), mp.createCredentialCalls.Load())
	})
}
