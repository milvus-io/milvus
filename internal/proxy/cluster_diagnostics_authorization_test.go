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
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	mhttp "github.com/milvus-io/milvus/internal/http"
	"github.com/milvus-io/milvus/internal/proxy/privilege"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
)

// diagnosticsCoordinator lets "reader" hold the grants a test sets and
// "operator" hold the built-in admin role.
type diagnosticsCoordinator struct {
	*metadataPrivilegeCoordinator
	quotaCalls   int
	metricsCalls int
}

func (c *diagnosticsCoordinator) GetMetrics(context.Context, *milvuspb.GetMetricsRequest, ...grpc.CallOption) (*milvuspb.GetMetricsResponse, error) {
	c.metricsCalls++
	return &milvuspb.GetMetricsResponse{Status: merr.Success(), Response: `{"segments":[]}`}, nil
}

func (c *diagnosticsCoordinator) ListPolicy(context.Context, *internalpb.ListPolicyRequest, ...grpc.CallOption) (*internalpb.ListPolicyResponse, error) {
	return &internalpb.ListPolicyResponse{
		Status: merr.Success(), PolicyInfos: c.policies,
		UserRoles: []string{
			funcutil.EncodeUserRoleCache("reader", "metadata_reader"),
			funcutil.EncodeUserRoleCache("operator", util.RoleAdmin),
		},
	}, nil
}

func (c *diagnosticsCoordinator) GetQuotaMetrics(context.Context, *internalpb.GetQuotaMetricsRequest, ...grpc.CallOption) (*internalpb.GetQuotaMetricsResponse, error) {
	c.quotaCalls++
	return &internalpb.GetQuotaMetricsResponse{Status: merr.Success(), MetricsInfo: `{"collections":{"41":{"rows":10}}}`}, nil
}

// builtinPrivileges lists the privileges of a built-in privilege group, which
// RootCoord expands before Proxy sees them.
func builtinPrivileges(group []string) []commonpb.ObjectPrivilege {
	privileges := make([]commonpb.ObjectPrivilege, 0, len(group))
	for _, name := range group {
		value, ok := commonpb.ObjectPrivilege_value[util.PrivilegeNameForMetastore(name)]
		if !ok {
			continue // Not an object privilege (the deprecated Expr); nothing checks it.
		}
		privileges = append(privileges, commonpb.ObjectPrivilege(value))
	}
	return privileges
}

// grantDiagnostics replaces the grants of metadata_reader with privileges on
// the global object, or, for collection-level ones, on tenant_b.records.
func (c *diagnosticsCoordinator) grantDiagnostics(t *testing.T, global []commonpb.ObjectPrivilege, collection []commonpb.ObjectPrivilege) {
	c.policies = nil
	for _, p := range global {
		c.policies = append(c.policies, funcutil.PolicyForPrivilege("metadata_reader",
			commonpb.ObjectType_Global.String(), util.AnyWord, p.String(), util.AnyWord))
	}
	for _, p := range collection {
		c.policies = append(c.policies, funcutil.PolicyForPrivilege("metadata_reader",
			commonpb.ObjectType_Collection.String(), "records", p.String(), "tenant_b"))
	}
	require.NoError(t, privilege.InitPrivilegeCache(context.Background(), c))
}

func TestClusterDiagnosticsRequireClusterAdmin(t *testing.T) {
	base, _ := setupMetadataPrivileges(t)
	coord := &diagnosticsCoordinator{metadataPrivilegeCoordinator: base}
	node := &Proxy{mixCoord: coord}
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	userCtx := func(user string) context.Context {
		return NewContextWithMetadata(context.Background(), user, "tenant_b")
	}
	// An unknown metric type fails only after authorization, which tells the
	// two outcomes apart without assembling real system metrics.
	metrics := func(ctx context.Context) error {
		resp, err := node.GetMetrics(ctx, &milvuspb.GetMetricsRequest{Request: `{"metric_type": "unknown_metrics"}`})
		require.NoError(t, err)
		return merr.Error(resp.GetStatus())
	}
	quota := func(ctx context.Context) error {
		resp, err := node.GetQuotaMetrics(ctx, &internalpb.GetQuotaMetricsRequest{})
		require.NoError(t, err)
		return merr.Error(resp.GetStatus())
	}

	for _, tc := range []struct {
		name       string
		user       string
		global     []commonpb.ObjectPrivilege
		collection []commonpb.ObjectPrivilege
	}{
		{"no_grants", "no_roles", nil, nil},
		{"collection_admin", "reader", nil, builtinPrivileges(util.CollectionAdminPrivileges)},
		{"cluster_read_write", "reader", builtinPrivileges(util.ClusterReadWritePrivileges), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			coord.grantDiagnostics(t, tc.global, tc.collection)
			calls := coord.quotaCalls
			assert.ErrorIs(t, metrics(userCtx(tc.user)), merr.ErrPrivilegeNotPermitted)
			assert.ErrorIs(t, quota(userCtx(tc.user)), merr.ErrPrivilegeNotPermitted)
			assert.Equal(t, calls, coord.quotaCalls, "denied callers must not reach the coordinator")
		})
	}
	assert.ErrorIs(t, quota(context.Background()), merr.ErrPrivilegeNotAuthenticated)

	for _, tc := range []struct {
		name   string
		user   string
		global []commonpb.ObjectPrivilege
	}{
		{"root", util.UserRoot, nil},
		{"admin_role", "operator", nil},
		{"cluster_admin_group", "reader", builtinPrivileges(util.ClusterAdminPrivileges)},
		{"manage_ownership", "reader", []commonpb.ObjectPrivilege{commonpb.ObjectPrivilege_PrivilegeManageOwnership}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			coord.grantDiagnostics(t, tc.global, nil)
			assert.ErrorIs(t, metrics(userCtx(tc.user)), merr.ErrMetricNotFound)
			require.NoError(t, quota(userCtx(tc.user)))
		})
	}

	require.NoError(t, Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "false"))
	t.Cleanup(func() { Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "true") })
	assert.ErrorIs(t, metrics(context.Background()), merr.ErrMetricNotFound)
	require.NoError(t, quota(context.Background()))
}

// consoleMetricsPaths are the web console routes that read coordinator
// metrics.
var consoleMetricsPaths = []string{
	mhttp.ClusterInfoPath,
	mhttp.QCTargetPath, mhttp.QCDistPath, mhttp.QCReplicaPath, mhttp.QCResourceGroupPath, mhttp.QCAllTasksPath, mhttp.QCSegmentsPath,
	mhttp.QNSegmentsPath, mhttp.QNChannelsPath,
	mhttp.DCDistPath, mhttp.DCCompactionTasksPath, mhttp.DCImportTasksPath, mhttp.DCBuildIndexTasksPath, mhttp.IndexListPath, mhttp.DCSegmentsPath,
	mhttp.DNSyncTasksPath, mhttp.DNSegmentsPath, mhttp.DNChannelsPath,
}

// The console reaches the same cluster-wide data as GetMetrics, and any of
// these routes can pick another metrics action through its query string. With
// the management gate off, the console admits every authenticated user, so
// each route needs the cluster admin grant itself.
func TestConsoleMetricsRequireClusterAdmin(t *testing.T) {
	gin.SetMode(gin.TestMode)
	base, _ := setupMetadataPrivileges(t)
	coord := &diagnosticsCoordinator{metadataPrivilegeCoordinator: base}
	node := &Proxy{mixCoord: coord, metricsCacheManager: metricsinfo.NewMetricsCacheManager()}
	// Serve /_cluster/info from the cache for the whole test.
	node.metricsCacheManager.SetRetention(time.Hour)
	node.metricsCacheManager.UpdateSystemInfoMetrics(&milvuspb.GetMetricsResponse{Status: merr.Success(), Response: `{"nodes_info":[]}`})

	// verified stands in for the metrics port authentication, which stores
	// the caller it verified under the "username" key.
	serve := func(verified string, path string, prepare func(*http.Request) *http.Request) *httptest.ResponseRecorder {
		router := gin.New()
		group := router.Group("/api/v1", func(c *gin.Context) {
			if verified != "" {
				c.Set("username", verified)
			}
		})
		node.RegisterRestRouter(group)
		req := httptest.NewRequest(http.MethodGet, "/api/v1"+path+"?req_type=dist", nil)
		if prepare != nil {
			req = prepare(req)
		}
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		return w
	}

	for _, tc := range []struct {
		name     string
		verified string
		global   []commonpb.ObjectPrivilege
		coll     []commonpb.ObjectPrivilege
		status   int
	}{
		{"no_verified_caller", "", nil, nil, http.StatusUnauthorized},
		{"no_grants", "no_roles", nil, nil, http.StatusForbidden},
		{"collection_admin", "reader", nil, builtinPrivileges(util.CollectionAdminPrivileges), http.StatusForbidden},
		{"cluster_read_write", "reader", builtinPrivileges(util.ClusterReadWritePrivileges), nil, http.StatusForbidden},
		{"root", util.UserRoot, nil, nil, http.StatusOK},
		{"admin_role", "operator", nil, nil, http.StatusOK},
		{"cluster_admin_group", "reader", builtinPrivileges(util.ClusterAdminPrivileges), nil, http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			coord.grantDiagnostics(t, tc.global, tc.coll)
			for _, path := range consoleMetricsPaths {
				calls := coord.metricsCalls
				w := serve(tc.verified, path, nil)
				require.Equal(t, tc.status, w.Code, "%s: %s", path, w.Body.String())
				if tc.status != http.StatusOK {
					assert.Equal(t, calls, coord.metricsCalls, "%s: a refused caller must not reach the coordinator", path)
					assert.NotContains(t, w.Body.String(), "segments")
					assert.NotContains(t, w.Body.String(), "nodes_info")
				}
			}
		})
	}

	t.Run("request_cannot_name_the_caller", func(t *testing.T) {
		coord.grantDiagnostics(t, nil, nil)
		rootToken := base64.StdEncoding.EncodeToString([]byte(util.UserRoot + util.CredentialSeparator + "Milvus"))
		for _, path := range consoleMetricsPaths {
			w := serve("no_roles", path, func(req *http.Request) *http.Request {
				req.Header.Set("Authorization", "Basic "+rootToken)
				req.Header.Set(util.HeaderAuthorize, rootToken)
				return req
			})
			assert.Equal(t, http.StatusForbidden, w.Code, path)
		}
	})

	t.Run("management_gate", func(t *testing.T) {
		require.NoError(t, Params.Save(Params.CommonCfg.AdminAuthEnabled.Key, "true"))
		mhttp.RegisterManagementVerifier(mhttp.VerifierSlotProxy, func(context.Context, string, string) error { return nil })
		t.Cleanup(func() {
			Params.Reset(Params.CommonCfg.AdminAuthEnabled.Key)
			mhttp.RegisterManagementVerifier(mhttp.VerifierSlotProxy, nil)
		})
		coord.grantDiagnostics(t, nil, nil)
		for _, path := range consoleMetricsPaths {
			// Mounted without the gate's middleware, a route refuses.
			assert.Equal(t, http.StatusUnauthorized, serve("", path, nil).Code, path)
			// A request the gate verified as root passes.
			w := serve("", path, func(req *http.Request) *http.Request {
				req.SetBasicAuth(util.UserRoot, "test-only-password")
				req.Header.Set(mhttp.AdminRequestHeader, "true")
				decision := mhttp.CheckAdminRequest(req, path, false)
				require.True(t, decision.Allowed())
				return decision.AuthenticatedRequest(req)
			})
			assert.Equal(t, http.StatusOK, w.Code, path)
		}
	})

	t.Run("authorization_disabled", func(t *testing.T) {
		require.NoError(t, Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "false"))
		t.Cleanup(func() { Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "true") })
		for _, path := range consoleMetricsPaths {
			assert.Equal(t, http.StatusOK, serve("", path, nil).Code, path)
		}
	})
}
