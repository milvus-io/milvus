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
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func Test_describeDatabaseTask_Execute(t *testing.T) {
	t.Run("failed to get database by name", func(t *testing.T) {
		core := newTestCore(withInvalidMeta())
		task := &describeDBTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &rootcoordpb.DescribeDatabaseRequest{
				DbName: "testDB",
			},
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
		assert.NotNil(t, task.Rsp)
		assert.NotNil(t, task.Rsp.Status)
	})

	t.Run("describe with empty database name", func(t *testing.T) {
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().GetDatabaseByName(mock.Anything, mock.Anything, mock.Anything).
			Return(model.NewDefaultDatabase(nil), nil)
		core := newTestCore(withMeta(meta))

		task := &describeDBTask{
			baseTask: newBaseTask(context.Background(), core),
			Req:      &rootcoordpb.DescribeDatabaseRequest{},
		}
		err := task.Execute(context.Background())
		assert.NoError(t, err)
		assert.NotNil(t, task.Rsp)
		assert.Equal(t, task.Rsp.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
		assert.Equal(t, util.DefaultDBName, task.Rsp.GetDbName())
		assert.Equal(t, util.DefaultDBID, task.Rsp.GetDbID())
	})

	t.Run("describe with specified database name", func(t *testing.T) {
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().GetDatabaseByName(mock.Anything, mock.Anything, mock.Anything).
			Return(&model.Database{
				Name:        "db1",
				ID:          100,
				CreatedTime: 1,
			}, nil)
		core := newTestCore(withMeta(meta))

		task := &describeDBTask{
			baseTask: newBaseTask(context.Background(), core),
			Req:      &rootcoordpb.DescribeDatabaseRequest{DbName: "db1"},
		}
		err := task.Execute(context.Background())
		assert.NoError(t, err)
		assert.NotNil(t, task.Rsp)
		assert.Equal(t, task.Rsp.GetStatus().GetCode(), int32(commonpb.ErrorCode_Success))
		assert.Equal(t, "db1", task.Rsp.GetDbName())
		assert.Equal(t, int64(100), task.Rsp.GetDbID())
		assert.Equal(t, uint64(1), task.Rsp.GetCreatedTimestamp())
	})
}

func Test_describeDBTask_WithAuth(t *testing.T) {
	paramtable.Init()
	Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
	defer Params.Reset(Params.CommonCfg.AuthorizationEnabled.Key)
	meta := mockrootcoord.NewIMetaTable(t)

	core := newTestCore(withMeta(meta))
	getTask := func() *describeDBTask {
		return &describeDBTask{
			baseTask: newBaseTask(context.Background(), core),
			Req:      &rootcoordpb.DescribeDatabaseRequest{DbName: "db1"},
		}
	}

	{
		// inner node
		meta.EXPECT().GetDatabaseByName(mock.Anything, mock.Anything, mock.Anything).
			Return(&model.Database{
				Name:        "db1",
				ID:          100,
				CreatedTime: 1,
			}, nil).Once()

		task := getTask()
		err := task.Execute(context.Background())
		assert.NoError(t, err)
		assert.NotNil(t, task.Rsp)
		assert.Equal(t, task.Rsp.GetStatus().GetCode(), int32(commonpb.ErrorCode_Success))
		assert.Equal(t, "db1", task.Rsp.GetDbName())
		assert.Equal(t, int64(100), task.Rsp.GetDbID())
		assert.Equal(t, uint64(1), task.Rsp.GetCreatedTimestamp())
	}

	{
		// proxy node with root user
		meta.EXPECT().GetDatabaseByName(mock.Anything, mock.Anything, mock.Anything).
			Return(&model.Database{
				Name:        "db1",
				ID:          100,
				CreatedTime: 1,
			}, nil).Once()

		ctx := GetContext(context.Background(), "root:root")
		task := getTask()
		err := task.Execute(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, task.Rsp)
		assert.Equal(t, task.Rsp.GetStatus().GetCode(), int32(commonpb.ErrorCode_Success))
		assert.Equal(t, "db1", task.Rsp.GetDbName())
		assert.Equal(t, int64(100), task.Rsp.GetDbID())
		assert.Equal(t, uint64(1), task.Rsp.GetCreatedTimestamp())
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
		assert.Error(t, err)
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
		assert.Error(t, err)
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
		meta.EXPECT().GetDatabaseByName(mock.Anything, mock.Anything, mock.Anything).
			Return(&model.Database{
				Name:        "db1",
				ID:          100,
				CreatedTime: 1,
			}, nil).Once()
		ctx := GetContext(context.Background(), "foo:root")
		task := getTask()
		err := task.Execute(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, task.Rsp)
		assert.Equal(t, task.Rsp.GetStatus().GetCode(), int32(commonpb.ErrorCode_Success))
		assert.Equal(t, "db1", task.Rsp.GetDbName())
		assert.Equal(t, int64(100), task.Rsp.GetDbID())
		assert.Equal(t, uint64(1), task.Rsp.GetCreatedTimestamp())
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
		meta.EXPECT().SelectGrant(mock.Anything, mock.Anything, mock.Anything).
			Return([]*milvuspb.GrantEntity{
				{
					DbName: "db1",
				},
			}, nil).Once()
		meta.EXPECT().GetDatabaseByName(mock.Anything, mock.Anything, mock.Anything).
			Return(&model.Database{
				Name:        "db1",
				ID:          100,
				CreatedTime: 1,
			}, nil).Once()
		ctx := GetContext(context.Background(), "foo:root")
		task := getTask()
		err := task.Execute(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, task.Rsp)
		assert.Equal(t, task.Rsp.GetStatus().GetCode(), int32(commonpb.ErrorCode_Success))
		assert.Equal(t, "db1", task.Rsp.GetDbName())
		assert.Equal(t, int64(100), task.Rsp.GetDbID())
		assert.Equal(t, uint64(1), task.Rsp.GetCreatedTimestamp())
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
		assert.Error(t, err)
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
		meta.EXPECT().SelectGrant(mock.Anything, mock.Anything, mock.Anything).
			Return([]*milvuspb.GrantEntity{
				{
					DbName: "*",
				},
			}, nil).Once()
		meta.EXPECT().GetDatabaseByName(mock.Anything, mock.Anything, mock.Anything).
			Return(&model.Database{
				Name:        "db1",
				ID:          100,
				CreatedTime: 1,
			}, nil).Once()
		ctx := GetContext(context.Background(), "foo:root")
		task := getTask()
		err := task.Execute(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, task.Rsp)
		assert.Equal(t, task.Rsp.GetStatus().GetCode(), int32(commonpb.ErrorCode_Success))
		assert.Equal(t, "db1", task.Rsp.GetDbName())
		assert.Equal(t, int64(100), task.Rsp.GetDbID())
		assert.Equal(t, uint64(1), task.Rsp.GetCreatedTimestamp())
	}
}
