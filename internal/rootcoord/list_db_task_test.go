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

package rootcoord

import (
	"context"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"
	"github.com/milvus-io/milvus/pkg/util"
	"github.com/milvus-io/milvus/pkg/util/crypto"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func Test_ListDBTask(t *testing.T) {
	paramtable.Init()
	t.Run("list db fails", func(t *testing.T) {
		core := newTestCore(withInvalidMeta())
		task := &listDatabaseTask{
			baseTask: newBaseTask(context.TODO(), core),
			Req: &milvuspb.ListDatabasesRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_ListDatabases,
				},
			},
			Resp: &milvuspb.ListDatabasesResponse{},
		}

		err := task.Prepare(context.Background())
		assert.NoError(t, err)

		err = task.Execute(context.Background())
		assert.Error(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, task.Resp.GetStatus().GetErrorCode())
	})

	t.Run("ok", func(t *testing.T) {
		ret := []*model.Database{model.NewDefaultDatabase(nil)}
		meta := mockrootcoord.NewIMetaTable(t)
		meta.On("ListDatabases",
			mock.Anything,
			mock.Anything).
			Return(ret, nil)

		core := newTestCore(withMeta(meta))
		task := &listDatabaseTask{
			baseTask: newBaseTask(context.TODO(), core),
			Req: &milvuspb.ListDatabasesRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_ListDatabases,
				},
			},
			Resp: &milvuspb.ListDatabasesResponse{},
		}

		err := task.Prepare(context.Background())
		assert.NoError(t, err)

		err = task.Execute(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, 1, len(task.Resp.GetDbNames()))
		assert.Equal(t, ret[0].Name, task.Resp.GetDbNames()[0])
		assert.Equal(t, commonpb.ErrorCode_Success, task.Resp.GetStatus().GetErrorCode())
	})

	t.Run("list db with auth", func(t *testing.T) {
		Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
		defer Params.Reset(Params.CommonCfg.AuthorizationEnabled.Key)
		ret := []*model.Database{model.NewDefaultDatabase(nil)}
		meta := mockrootcoord.NewIMetaTable(t)

		core := newTestCore(withMeta(meta))
		getTask := func() *listDatabaseTask {
			return &listDatabaseTask{
				baseTask: newBaseTask(context.TODO(), core),
				Req: &milvuspb.ListDatabasesRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_ListDatabases,
					},
				},
				Resp: &milvuspb.ListDatabasesResponse{},
			}
		}

		{
			// inner node
			meta.EXPECT().ListDatabases(mock.Anything, mock.Anything).Return(ret, nil).Once()

			task := getTask()
			err := task.Execute(context.Background())
			assert.NoError(t, err)
			assert.Equal(t, 1, len(task.Resp.GetDbNames()))
			assert.Equal(t, ret[0].Name, task.Resp.GetDbNames()[0])
			assert.Equal(t, commonpb.ErrorCode_Success, task.Resp.GetStatus().GetErrorCode())
		}

		{
			// proxy node with root user
			meta.EXPECT().ListDatabases(mock.Anything, mock.Anything).Return(ret, nil).Once()

			ctx := GetContext(context.Background(), "root:root")
			task := getTask()
			err := task.Execute(ctx)
			assert.NoError(t, err)
			assert.Equal(t, 1, len(task.Resp.GetDbNames()))
			assert.Equal(t, ret[0].Name, task.Resp.GetDbNames()[0])
			assert.Equal(t, commonpb.ErrorCode_Success, task.Resp.GetStatus().GetErrorCode())
		}

		{
			// proxy node with root user, root user should bind role
			Params.Save(Params.CommonCfg.RootShouldBindRole.Key, "true")
			meta.EXPECT().SelectUser(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				Return([]*milvuspb.UserResult{
					{
						User: &milvuspb.UserEntity{
							Name: "root",
						},
						Roles: []*milvuspb.RoleEntity{},
					},
				}, nil).Once()

			ctx := GetContext(context.Background(), "root:root")
			task := getTask()
			err := task.Execute(ctx)
			assert.NoError(t, err)
			assert.Equal(t, 0, len(task.Resp.GetDbNames()))
			assert.Equal(t, commonpb.ErrorCode_Success, task.Resp.GetStatus().GetErrorCode())
			Params.Reset(Params.CommonCfg.RootShouldBindRole.Key)
		}

		{
			// select role fail
			meta.EXPECT().SelectUser(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				Return(nil, errors.New("mock select user error")).Once()
			ctx := GetContext(context.Background(), "foo:root")
			task := getTask()
			err := task.Execute(ctx)
			assert.Error(t, err)
		}

		{
			// select role, empty result
			meta.EXPECT().SelectUser(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				Return([]*milvuspb.UserResult{}, nil).Once()
			ctx := GetContext(context.Background(), "foo:root")
			task := getTask()
			err := task.Execute(ctx)
			assert.NoError(t, err)
			assert.Equal(t, 0, len(task.Resp.GetDbNames()))
		}

		{
			// select role, the user is added to admin role
			meta.EXPECT().SelectUser(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				Return([]*milvuspb.UserResult{
					{
						User: &milvuspb.UserEntity{
							Name: "foo",
						},
						Roles: []*milvuspb.RoleEntity{
							{
								Name: "admin",
							},
						},
					},
				}, nil).Once()
			meta.EXPECT().ListDatabases(mock.Anything, mock.Anything).Return(ret, nil).Once()
			ctx := GetContext(context.Background(), "foo:root")
			task := getTask()
			err := task.Execute(ctx)
			assert.NoError(t, err)
			assert.Equal(t, 1, len(task.Resp.GetDbNames()))
		}

		{
			// select grant fail
			meta.EXPECT().SelectUser(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				Return([]*milvuspb.UserResult{
					{
						User: &milvuspb.UserEntity{
							Name: "foo",
						},
						Roles: []*milvuspb.RoleEntity{
							{
								Name: "hoo",
							},
						},
					},
				}, nil).Once()
			meta.EXPECT().SelectGrant(mock.Anything, mock.Anything, mock.Anything).
				Return(nil, errors.New("mock select grant error")).Once()
			ctx := GetContext(context.Background(), "foo:root")
			task := getTask()
			err := task.Execute(ctx)
			assert.Error(t, err)
		}

		{
			// normal user
			meta.EXPECT().SelectUser(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				Return([]*milvuspb.UserResult{
					{
						User: &milvuspb.UserEntity{
							Name: "foo",
						},
						Roles: []*milvuspb.RoleEntity{
							{
								Name: "hoo",
							},
						},
					},
				}, nil).Once()
			meta.EXPECT().ListDatabases(mock.Anything, mock.Anything).Return([]*model.Database{
				{
					Name: "fooDB",
				},
				{
					Name: "default",
				},
			}, nil).Once()
			meta.EXPECT().SelectGrant(mock.Anything, mock.Anything, mock.Anything).
				Return([]*milvuspb.GrantEntity{
					{
						DbName: "fooDB",
					},
				}, nil).Once()
			ctx := GetContext(context.Background(), "foo:root")
			task := getTask()
			err := task.Execute(ctx)
			assert.NoError(t, err)
			assert.Equal(t, 1, len(task.Resp.GetDbNames()))
			assert.Equal(t, "fooDB", task.Resp.GetDbNames()[0])
		}

		{
			// normal user and public role
			meta.EXPECT().SelectUser(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				Return([]*milvuspb.UserResult{
					{
						User: &milvuspb.UserEntity{
							Name: "foo",
						},
						Roles: []*milvuspb.RoleEntity{
							{
								Name: "public",
							},
						},
					},
				}, nil).Once()
			ctx := GetContext(context.Background(), "foo:root")
			task := getTask()
			err := task.Execute(ctx)
			assert.NoError(t, err)
			assert.Equal(t, 0, len(task.Resp.GetDbNames()))
		}

		{
			// normal user with any db privilege
			meta.EXPECT().SelectUser(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				Return([]*milvuspb.UserResult{
					{
						User: &milvuspb.UserEntity{
							Name: "foo",
						},
						Roles: []*milvuspb.RoleEntity{
							{
								Name: "hoo",
							},
						},
					},
				}, nil).Once()
			meta.EXPECT().ListDatabases(mock.Anything, mock.Anything).Return([]*model.Database{
				{
					Name: "fooDB",
				},
				{
					Name: "default",
				},
			}, nil).Once()
			meta.EXPECT().SelectGrant(mock.Anything, mock.Anything, mock.Anything).
				Return([]*milvuspb.GrantEntity{
					{
						DbName: "*",
					},
				}, nil).Once()
			ctx := GetContext(context.Background(), "foo:root")
			task := getTask()
			err := task.Execute(ctx)
			assert.NoError(t, err)
			assert.Equal(t, 2, len(task.Resp.GetDbNames()))
		}
	})
}

func GetContext(ctx context.Context, originValue string) context.Context {
	authKey := strings.ToLower(util.HeaderAuthorize)
	authValue := crypto.Base64Encode(originValue)
	contextMap := map[string]string{
		authKey: authValue,
	}
	md := metadata.New(contextMap)
	return metadata.NewIncomingContext(ctx, md)
}
