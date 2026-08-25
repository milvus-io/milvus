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
	"testing"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/proxy/privilege"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestAuthorizeWALRead(t *testing.T) {
	t.Run("authorization disabled allows", func(t *testing.T) {
		Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "false")
		defer Params.Reset(Params.CommonCfg.AuthorizationEnabled.Key)

		_, err := authorizeWALRead(context.Background())
		assert.NoError(t, err)
	})

	t.Run("authorization enabled", func(t *testing.T) {
		Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
		defer Params.Reset(Params.CommonCfg.AuthorizationEnabled.Key)
		defer privilege.CleanPrivilegeCache()
		privilege.InitPrivilegeGroups()

		t.Run("root is exempt", func(t *testing.T) {
			_, err := authorizeWALRead(GetContext(context.Background(), "root:pwd"))
			assert.NoError(t, err)
		})

		client := &MockMixCoordClientInterface{}
		client.listPolicy = func(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
			return &internalpb.ListPolicyResponse{
				Status: merr.Success(),
				UserRoles: []string{
					funcutil.EncodeUserRoleCache("dbadmin", util.RoleAdmin),
					funcutil.EncodeUserRoleCache("bob", "role_readonly"),
				},
			}, nil
		}
		_, err := initMetaCache(context.Background(), client)
		require.NoError(t, err)

		t.Run("admin role is allowed", func(t *testing.T) {
			_, err := authorizeWALRead(GetContext(context.Background(), "dbadmin:pwd"))
			assert.NoError(t, err)
		})

		t.Run("custom superuser with Global PrivilegeGroupAdmin is allowed", func(t *testing.T) {
			superClient := &MockMixCoordClientInterface{}
			superClient.listPolicy = func(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
				return &internalpb.ListPolicyResponse{
					Status: merr.Success(),
					PolicyInfos: []string{
						// Cluster-level PrivilegeGroupAdmin grant at db="*",
						// the way rootcoord normalizes cluster-level grants.
						funcutil.PolicyForPrivilege("role_super", commonpb.ObjectType_Global.String(), "*", commonpb.ObjectPrivilege_PrivilegeGroupAdmin.String(), util.AnyWord),
					},
					UserRoles: []string{
						funcutil.EncodeUserRoleCache("super", "role_super"),
					},
				}, nil
			}
			_, err := initMetaCache(context.Background(), superClient)
			require.NoError(t, err)

			_, err = authorizeWALRead(GetContext(context.Background(), "super:pwd"))
			assert.NoError(t, err)
		})

		t.Run("custom superuser with Global PrivilegeManageOwnership is allowed (v2 ClusterAdmin group materialization)", func(t *testing.T) {
			superClient := &MockMixCoordClientInterface{}
			superClient.listPolicy = func(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
				return &internalpb.ListPolicyResponse{
					Status: merr.Success(),
					PolicyInfos: []string{
						funcutil.PolicyForPrivilege("role_clusadmin", commonpb.ObjectType_Global.String(), "*", commonpb.ObjectPrivilege_PrivilegeManageOwnership.String(), util.AnyWord),
					},
					UserRoles: []string{
						funcutil.EncodeUserRoleCache("clusadmin", "role_clusadmin"),
					},
				}, nil
			}
			_, err := initMetaCache(context.Background(), superClient)
			require.NoError(t, err)

			_, err = authorizeWALRead(GetContext(context.Background(), "clusadmin:pwd"))
			assert.NoError(t, err)
		})

		t.Run("custom superuser with Global PrivilegeAll is allowed", func(t *testing.T) {
			superClient := &MockMixCoordClientInterface{}
			superClient.listPolicy = func(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
				return &internalpb.ListPolicyResponse{
					Status: merr.Success(),
					PolicyInfos: []string{
						funcutil.PolicyForPrivilege("role_all", commonpb.ObjectType_Global.String(), "*", commonpb.ObjectPrivilege_PrivilegeAll.String(), util.AnyWord),
					},
					UserRoles: []string{
						funcutil.EncodeUserRoleCache("alluser", "role_all"),
					},
				}, nil
			}
			_, err := initMetaCache(context.Background(), superClient)
			require.NoError(t, err)

			_, err = authorizeWALRead(GetContext(context.Background(), "alluser:pwd"))
			assert.NoError(t, err)
		})

		t.Run("role with only replicate-config grant is denied", func(t *testing.T) {
			// Guardrail: replication rights (PrivilegeUpdateReplicateConfiguration)
			// must NOT imply raw WAL dump rights. A role holding only that narrow
			// grant can create a replicate stream but is denied DumpMessages.
			replClient := &MockMixCoordClientInterface{}
			replClient.listPolicy = func(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
				return &internalpb.ListPolicyResponse{
					Status: merr.Success(),
					PolicyInfos: []string{
						funcutil.PolicyForPrivilege("role_repl", commonpb.ObjectType_Global.String(), "*", commonpb.ObjectPrivilege_PrivilegeUpdateReplicateConfiguration.String(), util.AnyWord),
					},
					UserRoles: []string{
						funcutil.EncodeUserRoleCache("repluser", "role_repl"),
					},
				}, nil
			}
			_, err := initMetaCache(context.Background(), replClient)
			require.NoError(t, err)

			_, err = authorizeWALRead(GetContext(context.Background(), "repluser:pwd"))
			require.Error(t, err)
			assert.Equal(t, codes.PermissionDenied, status.Code(err))
		})

		t.Run("non-admin role is denied", func(t *testing.T) {
			_, err := authorizeWALRead(GetContext(context.Background(), "bob:pwd"))
			require.Error(t, err)
			assert.Equal(t, codes.PermissionDenied, status.Code(err))
		})

		t.Run("missing authorization is denied", func(t *testing.T) {
			// GetAuthInfoFromContext fails with a merr error (no auth metadata),
			// which is fail-closed rather than a PermissionDenied status.
			_, err := authorizeWALRead(context.Background())
			assert.Error(t, err)
		})

		t.Run("root requires admin role when bind role enabled", func(t *testing.T) {
			Params.Save(Params.CommonCfg.RootShouldBindRole.Key, "true")
			defer Params.Reset(Params.CommonCfg.RootShouldBindRole.Key)

			rootClient := &MockMixCoordClientInterface{}
			rootClient.listPolicy = func(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
				return &internalpb.ListPolicyResponse{
					Status: merr.Success(),
					UserRoles: []string{
						funcutil.EncodeUserRoleCache("root", util.RoleAdmin),
						funcutil.EncodeUserRoleCache("plain", "role_readonly"),
					},
				}, nil
			}
			_, err := initMetaCache(context.Background(), rootClient)
			require.NoError(t, err)

			_, err = authorizeWALRead(GetContext(context.Background(), "root:pwd"))
			assert.NoError(t, err)
			_, err = authorizeWALRead(GetContext(context.Background(), "plain:pwd"))
			require.Error(t, err)
			assert.Equal(t, codes.PermissionDenied, status.Code(err))
		})
	})
}

func TestDumpMessages_UnauthorizedUserDenied(t *testing.T) {
	Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
	defer Params.Reset(Params.CommonCfg.AuthorizationEnabled.Key)
	defer privilege.CleanPrivilegeCache()

	client := &MockMixCoordClientInterface{}
	client.listPolicy = func(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
		return &internalpb.ListPolicyResponse{
			Status: merr.Success(),
			UserRoles: []string{
				funcutil.EncodeUserRoleCache("mockUser", "role_readonly"),
			},
		}, nil
	}
	_, err := initMetaCache(context.Background(), client)
	require.NoError(t, err)

	// The DumpMessages stream is authorized at the gRPC interceptor layer
	// (GrpcAuthStreamInterceptor + PrivilegeStreamInterceptor), so exercise that
	// path directly rather than the handler: an unauthorized user must be
	// rejected before the handler observes the stream.
	handlerCalled := false
	interceptor := PrivilegeStreamInterceptor(StreamPrivilegeInterceptor)
	stream := &mockDumpMessagesServer{ctx: GetContext(context.Background(), "mockUser:mockPass")}
	err = interceptor(nil, stream, &grpc.StreamServerInfo{
		FullMethod: milvuspb.MilvusService_DumpMessages_FullMethodName,
	}, func(srv interface{}, ss grpc.ServerStream) error {
		handlerCalled = true
		return nil
	})
	require.Error(t, err)
	assert.Equal(t, codes.PermissionDenied, status.Code(err))
	assert.False(t, handlerCalled)
}

func TestDumpMessages_UnauthenticatedUserDenied(t *testing.T) {
	Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
	defer Params.Reset(Params.CommonCfg.AuthorizationEnabled.Key)
	defer privilege.CleanPrivilegeCache()

	client := &MockMixCoordClientInterface{}
	_, err := initMetaCache(context.Background(), client)
	require.NoError(t, err)

	// The DumpMessages stream is authenticated at the gRPC interceptor layer
	// (GrpcAuthStreamInterceptor) before authorization runs, so exercise that
	// path directly: a request without valid credentials must be rejected
	// before the privilege interceptor or the handler runs.
	handlerCalled := false
	authInterceptor := GrpcAuthStreamInterceptor(AuthenticationInterceptorWithMetaCache(func() Cache { return InitEmptyMetaCacheForTest() }))
	stream := &mockDumpMessagesServer{ctx: GetContext(context.Background(), "mockUser:wrongPass")}
	err = authInterceptor(nil, stream, &grpc.StreamServerInfo{
		FullMethod: milvuspb.MilvusService_DumpMessages_FullMethodName,
	}, func(srv interface{}, ss grpc.ServerStream) error {
		handlerCalled = true
		return nil
	})
	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
	assert.False(t, handlerCalled)
}

// TestStreamPrivilegeInterceptor_CreateReplicateStream exercises the casbin
// authorization path for CreateReplicateStream: a role granted the
// cluster-level PrivilegeUpdateReplicateConfiguration at the global (db="*")
// scope is allowed regardless of the connection namespace, matching the unary
// interceptor's handling of cluster-level grants.
func TestStreamPrivilegeInterceptor_CreateReplicateStream(t *testing.T) {
	paramtable.Init()
	Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
	defer Params.Reset(Params.CommonCfg.AuthorizationEnabled.Key)
	defer privilege.CleanPrivilegeCache()

	client := &MockMixCoordClientInterface{}
	client.listPolicy = func(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
		return &internalpb.ListPolicyResponse{
			Status: merr.Success(),
			PolicyInfos: []string{
				// Cluster-level grant stored with db="*", the way rootcoord
				// normalizes cluster-level privileges (see #52386 review).
				funcutil.PolicyForPrivilege("role_replicate", commonpb.ObjectType_Global.String(), "*", commonpb.ObjectPrivilege_PrivilegeUpdateReplicateConfiguration.String(), util.AnyWord),
			},
			UserRoles: []string{
				funcutil.EncodeUserRoleCache("carol", "role_replicate"),
				funcutil.EncodeUserRoleCache("dave", "role_readonly"),
			},
		}, nil
	}
	_, err := initMetaCache(context.Background(), client)
	require.NoError(t, err)

	interceptor := PrivilegeStreamInterceptor(StreamPrivilegeInterceptor)

	t.Run("role with cluster grant is allowed", func(t *testing.T) {
		handlerCalled := false
		stream := &mockDumpMessagesServer{ctx: GetContextWithDB(context.Background(), "carol:pwd", "some_namespace")}
		err := interceptor(nil, stream, &grpc.StreamServerInfo{
			FullMethod: milvuspb.MilvusService_CreateReplicateStream_FullMethodName,
		}, func(srv interface{}, ss grpc.ServerStream) error {
			handlerCalled = true
			return nil
		})
		require.NoError(t, err)
		assert.True(t, handlerCalled)
	})

	t.Run("role without grant is denied", func(t *testing.T) {
		handlerCalled := false
		stream := &mockDumpMessagesServer{ctx: GetContext(context.Background(), "dave:pwd")}
		err := interceptor(nil, stream, &grpc.StreamServerInfo{
			FullMethod: milvuspb.MilvusService_CreateReplicateStream_FullMethodName,
		}, func(srv interface{}, ss grpc.ServerStream) error {
			handlerCalled = true
			return nil
		})
		require.Error(t, err)
		assert.Equal(t, codes.PermissionDenied, status.Code(err))
		assert.False(t, handlerCalled)
	})

	t.Run("root is exempt", func(t *testing.T) {
		handlerCalled := false
		stream := &mockDumpMessagesServer{ctx: GetContext(context.Background(), "root:pwd")}
		err := interceptor(nil, stream, &grpc.StreamServerInfo{
			FullMethod: milvuspb.MilvusService_CreateReplicateStream_FullMethodName,
		}, func(srv interface{}, ss grpc.ServerStream) error {
			handlerCalled = true
			return nil
		})
		require.NoError(t, err)
		assert.True(t, handlerCalled)
	})
}

// TestStreamPrivilegeInterceptor_FailClosed covers the fail-closed default:
// a streaming method not present in streamMethodAuthorizers is denied, while
// the gRPC health service (used for infrastructure liveness probing) is
// exempted.
func TestStreamPrivilegeInterceptor_FailClosed(t *testing.T) {
	paramtable.Init()
	Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
	defer Params.Reset(Params.CommonCfg.AuthorizationEnabled.Key)

	interceptor := PrivilegeStreamInterceptor(StreamPrivilegeInterceptor)

	t.Run("unregistered stream method is denied", func(t *testing.T) {
		handlerCalled := false
		stream := &mockDumpMessagesServer{ctx: GetContext(context.Background(), "root:pwd")}
		err := interceptor(nil, stream, &grpc.StreamServerInfo{
			FullMethod: "/milvus.proto.milvus.MilvusService/FutureStreamRPC",
		}, func(srv interface{}, ss grpc.ServerStream) error {
			handlerCalled = true
			return nil
		})
		require.Error(t, err)
		assert.Equal(t, codes.PermissionDenied, status.Code(err))
		assert.False(t, handlerCalled)
	})

	t.Run("health watch is exempt", func(t *testing.T) {
		handlerCalled := false
		stream := &mockDumpMessagesServer{ctx: context.Background()}
		err := interceptor(nil, stream, &grpc.StreamServerInfo{
			FullMethod: grpc_health_v1.Health_Watch_FullMethodName,
		}, func(srv interface{}, ss grpc.ServerStream) error {
			handlerCalled = true
			return nil
		})
		require.NoError(t, err)
		assert.True(t, handlerCalled)
	})
}

// TestStreamHealthWatch_FullChain verifies the exemption semantics in the real
// chain: the health service is exempt from RBAC (so an authenticated probe
// passes) but NOT from authentication (an unauthenticated probe is rejected
// before the exemption is reached), matching the unary health.Check behavior.
func TestStreamHealthWatch_FullChain(t *testing.T) {
	paramtable.Init()
	Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
	defer Params.Reset(Params.CommonCfg.AuthorizationEnabled.Key)
	defer privilege.CleanPrivilegeCache()

	chain := grpc_middleware.ChainStreamServer(
		GrpcAuthStreamInterceptor(AuthenticationInterceptorWithMetaCache(func() Cache { return InitEmptyMetaCacheForTest() })),
		PrivilegeStreamInterceptor(StreamPrivilegeInterceptor),
	)

	t.Run("unauthenticated health watch is rejected", func(t *testing.T) {
		handlerCalled := false
		stream := &mockDumpMessagesServer{ctx: metadata.NewIncomingContext(context.Background(), metadata.MD{})}
		err := chain(nil, stream, &grpc.StreamServerInfo{
			FullMethod: grpc_health_v1.Health_Watch_FullMethodName,
		}, func(srv interface{}, ss grpc.ServerStream) error {
			handlerCalled = true
			return nil
		})
		require.Error(t, err)
		assert.Equal(t, codes.Unauthenticated, status.Code(err))
		assert.False(t, handlerCalled)
	})

	t.Run("authenticated health watch passes the RBAC exemption", func(t *testing.T) {
		handlerCalled := false
		stream := &mockDumpMessagesServer{ctx: GetContext(context.Background(), "root:pwd")}
		err := chain(nil, stream, &grpc.StreamServerInfo{
			FullMethod: grpc_health_v1.Health_Watch_FullMethodName,
		}, func(srv interface{}, ss grpc.ServerStream) error {
			handlerCalled = true
			return nil
		})
		require.NoError(t, err)
		assert.True(t, handlerCalled)
	})
}

// TestGrpcAuthStreamInterceptorChain verifies the external stream interceptor
// chain (authentication -> authorization) propagates the context end-to-end:
// auth runs first, authorization sees the resolved user, and the handler
// receives the wrapped context. Authorization is disabled here so the chain
// exercises the full pass-through path.
func TestGrpcAuthStreamInterceptorChain(t *testing.T) {
	Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "false")
	defer Params.Reset(Params.CommonCfg.AuthorizationEnabled.Key)
	_, err := initMetaCache(context.Background(), &MockMixCoordClientInterface{})
	require.NoError(t, err)

	chain := grpc_middleware.ChainStreamServer(
		GrpcAuthStreamInterceptor(AuthenticationInterceptorWithMetaCache(func() Cache { return InitEmptyMetaCacheForTest() })),
		PrivilegeStreamInterceptor(StreamPrivilegeInterceptor),
	)
	stream := &mockDumpMessagesServer{ctx: GetContext(context.Background(), "root:pwd")}
	handlerCalled := false
	err = chain(nil, stream, &grpc.StreamServerInfo{
		FullMethod: milvuspb.MilvusService_DumpMessages_FullMethodName,
	}, func(srv interface{}, ss grpc.ServerStream) error {
		handlerCalled = true
		return nil
	})
	require.NoError(t, err)
	assert.True(t, handlerCalled)
}

// TestGrpcAuthStreamInterceptorChain_Authorized verifies the full chain with
// authorization enabled: the handler receives a context that still carries the
// authenticated user (incoming metadata). The mock credential store only knows
// "mockUser:mockPass", so exercise the DumpMessages admin path by binding
// mockUser to the admin role.
func TestGrpcAuthStreamInterceptorChain_Authorized(t *testing.T) {
	paramtable.Init()
	Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
	defer Params.Reset(Params.CommonCfg.AuthorizationEnabled.Key)
	defer privilege.CleanPrivilegeCache()

	client := &MockMixCoordClientInterface{}
	client.listPolicy = func(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
		return &internalpb.ListPolicyResponse{
			Status: merr.Success(),
			UserRoles: []string{
				funcutil.EncodeUserRoleCache("mockUser", util.RoleAdmin),
			},
		}, nil
	}
	cache, err := initMetaCache(context.Background(), client)
	require.NoError(t, err)

	chain := grpc_middleware.ChainStreamServer(
		GrpcAuthStreamInterceptor(AuthenticationInterceptorWithMetaCache(func() Cache { return cache })),
		PrivilegeStreamInterceptor(StreamPrivilegeInterceptor),
	)
	stream := &mockDumpMessagesServer{ctx: GetContext(context.Background(), "mockUser:mockPass")}
	var handlerCtx context.Context
	err = chain(nil, stream, &grpc.StreamServerInfo{
		FullMethod: milvuspb.MilvusService_DumpMessages_FullMethodName,
	}, func(srv interface{}, ss grpc.ServerStream) error {
		handlerCtx = ss.Context()
		return nil
	})
	require.NoError(t, err)

	// The handler context must still carry the authenticated user so downstream
	// checks can resolve identity from incoming metadata.
	username, _, err := contextutil.GetAuthInfoFromContext(handlerCtx)
	require.NoError(t, err)
	assert.Equal(t, "mockUser", username)
}
