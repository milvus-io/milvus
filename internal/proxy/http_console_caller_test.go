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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	mhttp "github.com/milvus-io/milvus/internal/http"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proxy/connection"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v3/util/crypto"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
)

// consoleCallerCase describes who reaches a console route.
type consoleCallerCase struct {
	name string
	// authorization is common.security.authorizationEnabled.
	authorization bool
	// username is what the metrics-port authentication stored on the request.
	username string
	// admin marks a request that passed the management gate.
	admin bool
	// wantUser is the identity RootCoord must see; "" means none is forwarded.
	wantUser string
	// rejected means the handler must answer 403 without calling a coordinator.
	rejected bool
}

var consoleCallers = []consoleCallerCase{
	{name: "authenticated user", authorization: true, username: "alice", wantUser: "alice"},
	{name: "authenticated root", authorization: true, username: util.UserRoot, wantUser: util.UserRoot},
	{name: "missing user", authorization: true, rejected: true},
	{name: "management administrator", authorization: true, admin: true},
	{name: "authorization disabled", authorization: false},
}

func newConsoleRequest(t *testing.T, caller consoleCallerCase, target, route string) (*gin.Context, *httptest.ResponseRecorder) {
	t.Helper()
	require.NoError(t, Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, map[bool]string{true: "true", false: "false"}[caller.authorization]))
	t.Cleanup(func() { Params.Reset(Params.CommonCfg.AuthorizationEnabled.Key) })

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, target, nil)
	// Raw HTTP headers must never choose the identity forwarded to RootCoord.
	c.Request.Header.Set(util.HeaderAuthorize, crypto.Base64Encode("root:root"))
	c.Request.Header.Set("x-user", util.UserRoot)
	if caller.username != "" {
		c.Set("username", caller.username)
	}
	if caller.admin {
		// Produce the real, unforgeable marker through management authentication.
		mhttp.RegisterManagementVerifier(mhttp.VerifierSlotProxy, func(context.Context, string, string) error { return nil })
		t.Cleanup(func() { mhttp.RegisterManagementVerifier(mhttp.VerifierSlotProxy, nil) })
		c.Request.SetBasicAuth(util.UserRoot, "test-only-password")
		c.Request.Header.Set(mhttp.AdminRequestHeader, "true")
		decision := mhttp.CheckAdminRequest(c.Request, route, false)
		require.True(t, decision.Allowed())
		c.Request = decision.AuthenticatedRequest(c.Request)
	}
	return c, w
}

func requireUser(t *testing.T, md metadata.MD, want string) {
	t.Helper()
	if want == "" {
		require.Empty(t, md.Get(util.HeaderAuthorize), "no data-plane identity may be forwarded")
		return
	}
	require.Len(t, md.Get(util.HeaderAuthorize), 1)
	user, err := contextutil.GetCurUserFromContext(metadata.NewIncomingContext(context.Background(), md))
	require.NoError(t, err)
	require.Equal(t, want, user)
}

func requireRejected(t *testing.T, w *httptest.ResponseRecorder) {
	t.Helper()
	require.Equal(t, http.StatusForbidden, w.Code)
	require.Contains(t, w.Body.String(), "requires an authenticated user")
}

func TestConsoleListCollectionForwardsCaller(t *testing.T) {
	for _, caller := range consoleCallers {
		t.Run(caller.name, func(t *testing.T) {
			c, w := newConsoleRequest(t, caller, "/?db_name=tenant_a", mhttp.CollectionListPath)
			mix := mocks.NewMockMixCoordClient(t)
			if !caller.rejected {
				mix.EXPECT().ShowCollections(mock.Anything, mock.Anything).RunAndReturn(
					func(ctx context.Context, req *milvuspb.ShowCollectionsRequest, _ ...grpc.CallOption) (*milvuspb.ShowCollectionsResponse, error) {
						require.Equal(t, "tenant_a", req.GetDbName())
						md, _ := metadata.FromOutgoingContext(ctx)
						requireUser(t, md, caller.wantUser)
						return &milvuspb.ShowCollectionsResponse{
							Status:               merr.Success(),
							CollectionIds:        []int64{7},
							CollectionNames:      []string{"orders"},
							CreatedUtcTimestamps: []uint64{1633046400000},
						}, nil
					}).Once()
				mix.EXPECT().ShowLoadCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{
					Status:                merr.Success(),
					CollectionIDs:         []int64{7, 8},
					InMemoryPercentages:   []int64{100, 50},
					QueryServiceAvailable: []bool{true, true},
				}, nil).Once()
			}
			listCollection(&Proxy{mixCoord: mix})(c)
			if caller.rejected {
				requireRejected(t, w)
				return
			}
			require.Equal(t, http.StatusOK, w.Code)
			require.Contains(t, w.Body.String(), "orders")
			// Load state is reported only for collections RootCoord returned.
			require.NotContains(t, w.Body.String(), `"8"`)
			require.NotContains(t, w.Body.String(), `"50"`)
		})
	}
}

func TestConsoleListDatabaseForwardsCaller(t *testing.T) {
	for _, caller := range consoleCallers {
		t.Run(caller.name, func(t *testing.T) {
			c, w := newConsoleRequest(t, caller, "/", mhttp.DatabaseListPath)
			node := mocks.NewMockProxy(t)
			if !caller.rejected {
				node.EXPECT().ListDatabases(mock.Anything, mock.Anything).RunAndReturn(
					func(ctx context.Context, _ *milvuspb.ListDatabasesRequest) (*milvuspb.ListDatabasesResponse, error) {
						md, _ := metadata.FromIncomingContext(ctx)
						requireUser(t, md, caller.wantUser)
						// The Proxy task forwards the same identity to RootCoord.
						out, _ := metadata.FromOutgoingContext(AppendUserInfoForRPC(ctx))
						requireUser(t, out, caller.wantUser)
						return &milvuspb.ListDatabasesResponse{Status: merr.Success(), DbNames: []string{"tenant_a"}}, nil
					}).Once()
			}
			listDatabase(node)(c)
			if caller.rejected {
				requireRejected(t, w)
				return
			}
			require.Equal(t, http.StatusOK, w.Code)
			require.Contains(t, w.Body.String(), "tenant_a")
		})
	}
}

func TestConsoleDescribeDatabaseForwardsCaller(t *testing.T) {
	for _, caller := range consoleCallers {
		t.Run(caller.name, func(t *testing.T) {
			c, w := newConsoleRequest(t, caller, "/?db_name=tenant_b", mhttp.DatabaseDescPath)
			node := mocks.NewMockProxy(t)
			if !caller.rejected {
				node.EXPECT().DescribeDatabase(mock.Anything, mock.Anything).RunAndReturn(
					func(ctx context.Context, req *milvuspb.DescribeDatabaseRequest) (*milvuspb.DescribeDatabaseResponse, error) {
						require.Equal(t, "tenant_b", req.GetDbName())
						md, _ := metadata.FromIncomingContext(ctx)
						requireUser(t, md, caller.wantUser)
						out, _ := metadata.FromOutgoingContext(AppendUserInfoForRPC(ctx))
						requireUser(t, out, caller.wantUser)
						if caller.wantUser == "alice" {
							return &milvuspb.DescribeDatabaseResponse{Status: merr.Status(merr.WrapErrPrivilegeNotPermitted("not allowed to access database"))}, nil
						}
						return &milvuspb.DescribeDatabaseResponse{Status: merr.Success(), DbName: "tenant_b", DbID: 9}, nil
					}).Once()
			}
			describeDatabase(node)(c)
			switch {
			case caller.rejected:
				requireRejected(t, w)
			case caller.wantUser == "alice":
				require.NotEqual(t, http.StatusOK, w.Code)
				require.NotContains(t, w.Body.String(), `"dbID"`)
			default:
				require.Equal(t, http.StatusOK, w.Code)
				require.Contains(t, w.Body.String(), "tenant_b")
			}
		})
	}
}

// consoleSeesEveryone is whether the caller is shown every user's entries in
// what the Proxy records itself.
func consoleSeesEveryone(caller consoleCallerCase) bool {
	return caller.admin || !caller.authorization || caller.username == util.UserRoot
}

func TestConsoleSlowQueryScopedToCaller(t *testing.T) {
	for _, caller := range consoleCallers {
		t.Run(caller.name, func(t *testing.T) {
			c, w := newConsoleRequest(t, caller, "/", mhttp.SlowQueryPath)
			node := &Proxy{slowQueries: expirable.NewLRU[Timestamp, *metricsinfo.SlowQuery](20, nil, time.Minute)}
			node.slowQueries.Add(1, &metricsinfo.SlowQuery{User: "alice", Database: "tenant_a", Collection: "alice_orders"})
			node.slowQueries.Add(2, &metricsinfo.SlowQuery{
				User: "bob", Database: "tenant_b", Collection: "bob_orders",
				QueryParams: &metricsinfo.QueryParams{Expr: "owner == 'bob'"},
			})
			getSlowQuery(node)(c)
			if caller.rejected {
				requireRejected(t, w)
				return
			}
			require.Equal(t, http.StatusOK, w.Code)
			var got []*metricsinfo.SlowQuery
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
			users := lo.Map(got, func(q *metricsinfo.SlowQuery, _ int) string { return q.User })
			if consoleSeesEveryone(caller) {
				require.ElementsMatch(t, []string{"alice", "bob"}, users)
				return
			}
			require.Equal(t, []string{caller.username}, users)
			require.NotContains(t, w.Body.String(), "tenant_b")
			require.NotContains(t, w.Body.String(), "bob_orders")
			require.NotContains(t, w.Body.String(), "owner == 'bob'")
		})
	}
}

func TestConsoleSlowQueryRootBoundToRole(t *testing.T) {
	require.NoError(t, Params.Save(Params.CommonCfg.RootShouldBindRole.Key, "true"))
	t.Cleanup(func() { Params.Reset(Params.CommonCfg.RootShouldBindRole.Key) })
	caller := consoleCallerCase{authorization: true, username: util.UserRoot}
	c, w := newConsoleRequest(t, caller, "/", mhttp.SlowQueryPath)
	node := &Proxy{slowQueries: expirable.NewLRU[Timestamp, *metricsinfo.SlowQuery](20, nil, time.Minute)}
	node.slowQueries.Add(1, &metricsinfo.SlowQuery{User: "bob", Collection: "bob_orders"})
	getSlowQuery(node)(c)
	require.Equal(t, http.StatusOK, w.Code)
	require.NotContains(t, w.Body.String(), "bob_orders")
}

func TestConsoleConnectedClientsScopedToCaller(t *testing.T) {
	manager := connection.GetManager()
	manager.Register(context.Background(), 990001, &commonpb.ClientInfo{SdkType: "Golang", User: "alice", Host: "alice-host"})
	manager.Register(context.Background(), 990002, &commonpb.ClientInfo{SdkType: "Golang", User: "bob", Host: "bob-host"})
	for _, caller := range consoleCallers {
		t.Run(caller.name, func(t *testing.T) {
			c, w := newConsoleRequest(t, caller, "/", mhttp.ClusterClientsPath)
			getConnectedClients(c)
			if caller.rejected {
				requireRejected(t, w)
				return
			}
			require.Equal(t, http.StatusOK, w.Code)
			if consoleSeesEveryone(caller) {
				require.Contains(t, w.Body.String(), "alice-host")
				require.Contains(t, w.Body.String(), "bob-host")
				return
			}
			var got []*commonpb.ClientInfo
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
			for _, client := range got {
				require.Equal(t, caller.username, client.GetUser())
			}
			require.NotContains(t, w.Body.String(), "bob-host")
		})
	}
}

func TestConsoleTelemetryCommandsRequireRoot(t *testing.T) {
	require.NoError(t, Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "true"))
	t.Cleanup(func() { Params.Reset(Params.CommonCfg.AuthorizationEnabled.Key) })
	for _, user := range []string{"alice", util.UserRoot} {
		t.Run(user, func(t *testing.T) {
			mix := mocks.NewMockMixCoordClient(t)
			if user == util.UserRoot {
				mix.EXPECT().ListClientCommands(mock.Anything, mock.Anything).Return(&rootcoordpb.ListClientCommandsResponse{
					Status:   merr.Success(),
					Commands: []*commonpb.ClientCommand{{CommandId: "cmd-1", CommandType: "push_config", TargetScope: "database:tenant_b"}},
				}, nil).Once()
			}
			node := &Proxy{mixCoord: mix}
			node.UpdateStateCode(commonpb.StateCode_Healthy)
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			// TelemetryAuthMiddleware stores the verified caller this way.
			c.Request = httptest.NewRequest(http.MethodGet, "/", nil).WithContext(NewContextWithMetadata(context.Background(), user, ""))
			listTelemetryCommands(node)(c)
			if user == util.UserRoot {
				require.Equal(t, http.StatusOK, w.Code)
				require.Contains(t, w.Body.String(), "database:tenant_b")
				return
			}
			require.NotEqual(t, http.StatusOK, w.Code)
			require.NotContains(t, w.Body.String(), "tenant_b")
		})
	}
}
