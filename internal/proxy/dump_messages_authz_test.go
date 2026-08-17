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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/mocks/distributed/mock_streaming"
	"github.com/milvus-io/milvus/internal/proxy/privilege"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestAuthorizeWALRead(t *testing.T) {
	t.Run("authorization disabled allows", func(t *testing.T) {
		Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "false")
		defer Params.Reset(Params.CommonCfg.AuthorizationEnabled.Key)

		assert.NoError(t, authorizeWALRead(context.Background()))
	})

	t.Run("authorization enabled", func(t *testing.T) {
		Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
		defer Params.Reset(Params.CommonCfg.AuthorizationEnabled.Key)
		defer privilege.CleanPrivilegeCache()

		t.Run("root is exempt", func(t *testing.T) {
			assert.NoError(t, authorizeWALRead(GetContext(context.Background(), "root:pwd")))
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
		require.NoError(t, InitMetaCache(context.Background(), client))

		t.Run("admin role is allowed", func(t *testing.T) {
			assert.NoError(t, authorizeWALRead(GetContext(context.Background(), "dbadmin:pwd")))
		})

		t.Run("non-admin role is denied", func(t *testing.T) {
			err := authorizeWALRead(GetContext(context.Background(), "bob:pwd"))
			require.Error(t, err)
			assert.Equal(t, codes.PermissionDenied, status.Code(err))
		})

		t.Run("missing authorization is denied", func(t *testing.T) {
			// GetAuthInfoFromContext fails with a merr error (no auth metadata),
			// which is fail-closed rather than a PermissionDenied status.
			assert.Error(t, authorizeWALRead(context.Background()))
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
			require.NoError(t, InitMetaCache(context.Background(), rootClient))

			assert.NoError(t, authorizeWALRead(GetContext(context.Background(), "root:pwd")))
			err := authorizeWALRead(GetContext(context.Background(), "plain:pwd"))
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
	require.NoError(t, InitMetaCache(context.Background(), client))

	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	mockWAL := mock_streaming.NewMockWALAccesser(t)
	prevWAL := streaming.WAL()
	streaming.SetWALForTest(mockWAL)
	defer streaming.SetWALForTest(prevWAL)

	stream := &mockDumpMessagesServer{ctx: GetContext(context.Background(), "mockUser:mockPass")}
	err := node.DumpMessages(&milvuspb.DumpMessagesRequest{
		Pchannel:       "test-channel",
		StartMessageId: testStartMessageID(),
	}, stream)
	require.Error(t, err)
	assert.Equal(t, codes.PermissionDenied, status.Code(err))
	assert.Empty(t, stream.getSent())
	mockWAL.AssertNotCalled(t, "Read", mock.Anything, mock.Anything)
}

func TestDumpMessages_UnauthenticatedUserDenied(t *testing.T) {
	Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
	defer Params.Reset(Params.CommonCfg.AuthorizationEnabled.Key)
	defer privilege.CleanPrivilegeCache()

	client := &MockMixCoordClientInterface{}
	require.NoError(t, InitMetaCache(context.Background(), client))

	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	mockWAL := mock_streaming.NewMockWALAccesser(t)
	prevWAL := streaming.WAL()
	streaming.SetWALForTest(mockWAL)
	defer streaming.SetWALForTest(prevWAL)

	stream := &mockDumpMessagesServer{ctx: GetContext(context.Background(), "mockUser:wrongPass")}
	err := node.DumpMessages(&milvuspb.DumpMessagesRequest{
		Pchannel:       "test-channel",
		StartMessageId: testStartMessageID(),
	}, stream)
	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
	assert.Empty(t, stream.getSent())
	mockWAL.AssertNotCalled(t, "Read", mock.Anything, mock.Anything)
}
