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

package grpcproxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	mhttp "github.com/milvus-io/milvus/internal/http"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/internal/proxy/privilege"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// diagnosticsConsoleProxy mounts one console metrics route behind the
// production gate.
type diagnosticsConsoleProxy struct {
	types.ProxyComponent
}

func (diagnosticsConsoleProxy) RegisterRestRouter(router gin.IRouter) {
	router.GET(mhttp.ClusterInfoPath, proxy.ConsoleDiagnosticsAuthMiddleware(), func(c *gin.Context) {
		c.String(http.StatusOK, "ok")
	})
}

// With the management gate off, the metrics port authenticates ordinary users
// and the console metrics gate must see who they are. If the gate could not
// read the caller the port verified, every user would be refused as
// unauthenticated (401) rather than judged on their grants.
func TestConsoleMetricsGateSeesPortAuthenticatedCaller(t *testing.T) {
	previousMode := gin.Mode()
	gin.SetMode(gin.TestMode)
	t.Cleanup(func() { gin.SetMode(previousMode) })
	params := paramtable.Get()
	require.NoError(t, params.Save(params.CommonCfg.AdminAuthEnabled.Key, "false"))
	require.NoError(t, params.Save(params.CommonCfg.AuthorizationEnabled.Key, "true"))
	require.NoError(t, params.Save(params.CommonCfg.RootShouldBindRole.Key, "false"))
	t.Cleanup(func() {
		params.Reset(params.CommonCfg.AdminAuthEnabled.Key)
		params.Reset(params.CommonCfg.AuthorizationEnabled.Key)
		params.Reset(params.CommonCfg.RootShouldBindRole.Key)
	})
	passwordMock := mockey.Mock(proxy.PasswordVerify).Return(true).Build()
	defer passwordMock.UnPatch()
	// alice exists and holds no grant.
	mixCoord := mocks.NewMockMixCoordClient(t)
	mixCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status:    merr.Success(),
		UserRoles: []string{funcutil.EncodeUserRoleCache("alice", util.RolePublic)},
	}, nil).Maybe()
	require.NoError(t, privilege.InitPrivilegeCache(context.Background(), mixCoord))
	t.Cleanup(privilege.ResetPrivilegeCacheForTest)
	t.Cleanup(privilege.CleanPrivilegeCache)

	engine := newMetricsPortEngine(gin.New(), diagnosticsConsoleProxy{mocks.NewMockProxy(t)})
	request := func(user string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, apiPathPrefix+mhttp.ClusterInfoPath, nil)
		if user != "" {
			req.SetBasicAuth(user, "password")
		}
		engine.ServeHTTP(w, req)
		return w
	}

	assert.Equal(t, http.StatusUnauthorized, request("").Code, "the port refuses an anonymous caller")
	assert.Equal(t, http.StatusOK, request(util.UserRoot).Code, "root, as verified by the port, passes")
	w := request("alice")
	assert.Equal(t, http.StatusForbidden, w.Code, "an ordinary user is judged on grants: %s", w.Body.String())
	assert.NotContains(t, w.Body.String(), "ok")
}
