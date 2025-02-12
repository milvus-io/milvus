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
	"github.com/milvus-io/milvus/pkg/util"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
)

func Test_showCollectionTask_Prepare(t *testing.T) {
	paramtable.Init()
	t.Run("invalid msg type", func(t *testing.T) {
		task := &showCollectionTask{
			Req: &milvuspb.ShowCollectionsRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_Undefined,
				},
			},
		}
		err := task.Prepare(context.Background())
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		task := &showCollectionTask{
			Req: &milvuspb.ShowCollectionsRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_ShowCollections,
				},
			},
		}
		err := task.Prepare(context.Background())
		assert.NoError(t, err)
	})
}

func Test_showCollectionTask_Execute(t *testing.T) {
	paramtable.Init()
	t.Run("failed to list collections", func(t *testing.T) {
		core := newTestCore(withInvalidMeta())
		task := &showCollectionTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &milvuspb.ShowCollectionsRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_ShowCollections,
				},
			},
			Rsp: &milvuspb.ShowCollectionsResponse{},
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
	})

	t.Run("success", func(t *testing.T) {
		meta := newMockMetaTable()
		meta.ListCollectionsFunc = func(ctx context.Context, ts Timestamp) ([]*model.Collection, error) {
			return []*model.Collection{
				{
					Name: "test coll",
				},
				{
					Name: "test coll2",
				},
			}, nil
		}
		core := newTestCore(withMeta(meta))
		task := &showCollectionTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &milvuspb.ShowCollectionsRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_ShowCollections,
				},
			},
			Rsp: &milvuspb.ShowCollectionsResponse{},
		}
		err := task.Execute(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, task.Rsp.GetStatus().GetErrorCode())
		assert.Equal(t, 2, len(task.Rsp.GetCollectionNames()))
	})
}

func TestShowCollectionsAuth(t *testing.T) {
	paramtable.Init()

	t.Run("no auth", func(t *testing.T) {
		Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "false")
		defer Params.Reset(Params.CommonCfg.AuthorizationEnabled.Key)
		meta := mockrootcoord.NewIMetaTable(t)
		core := newTestCore(withMeta(meta))

		meta.EXPECT().ListCollections(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*model.Collection{
			{
				DBID:         1,
				CollectionID: 100,
				Name:         "foo",
				CreateTime:   tsoutil.GetCurrentTime(),
			},
		}, nil).Once()

		task := &showCollectionTask{
			baseTask: newBaseTask(context.Background(), core),
			Req:      &milvuspb.ShowCollectionsRequest{DbName: "default"},
			Rsp:      &milvuspb.ShowCollectionsResponse{},
		}

		err := task.Execute(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, 1, len(task.Rsp.GetCollectionNames()))
		assert.Equal(t, "foo", task.Rsp.GetCollectionNames()[0])
	})

	t.Run("empty ctx", func(t *testing.T) {
		Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
		defer Params.Reset(Params.CommonCfg.AuthorizationEnabled.Key)
		meta := mockrootcoord.NewIMetaTable(t)
		core := newTestCore(withMeta(meta))

		meta.EXPECT().ListCollections(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*model.Collection{
			{
				DBID:         1,
				CollectionID: 100,
				Name:         "foo",
				CreateTime:   tsoutil.GetCurrentTime(),
			},
		}, nil).Once()

		task := &showCollectionTask{
			baseTask: newBaseTask(context.Background(), core),
			Req:      &milvuspb.ShowCollectionsRequest{DbName: "default"},
			Rsp:      &milvuspb.ShowCollectionsResponse{},
		}

		err := task.Execute(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, 1, len(task.Rsp.GetCollectionNames()))
		assert.Equal(t, "foo", task.Rsp.GetCollectionNames()[0])
	})

	t.Run("root user", func(t *testing.T) {
		Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
		defer Params.Reset(Params.CommonCfg.AuthorizationEnabled.Key)
		meta := mockrootcoord.NewIMetaTable(t)
		core := newTestCore(withMeta(meta))

		meta.EXPECT().ListCollections(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*model.Collection{
			{
				DBID:         1,
				CollectionID: 100,
				Name:         "foo",
				CreateTime:   tsoutil.GetCurrentTime(),
			},
		}, nil).Once()

		task := &showCollectionTask{
			baseTask: newBaseTask(context.Background(), core),
			Req:      &milvuspb.ShowCollectionsRequest{DbName: "default"},
			Rsp:      &milvuspb.ShowCollectionsResponse{},
		}

		ctx := GetContext(context.Background(), "root:root")
		err := task.Execute(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(task.Rsp.GetCollectionNames()))
		assert.Equal(t, "foo", task.Rsp.GetCollectionNames()[0])
	})

	t.Run("root user, should bind role", func(t *testing.T) {
		Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
		Params.Save(Params.CommonCfg.RootShouldBindRole.Key, "true")
		defer Params.Reset(Params.CommonCfg.AuthorizationEnabled.Key)
		defer Params.Reset(Params.CommonCfg.RootShouldBindRole.Key)
		meta := mockrootcoord.NewIMetaTable(t)
		core := newTestCore(withMeta(meta))

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

		task := &showCollectionTask{
			baseTask: newBaseTask(context.Background(), core),
			Req:      &milvuspb.ShowCollectionsRequest{DbName: "default"},
			Rsp:      &milvuspb.ShowCollectionsResponse{},
		}

		ctx := GetContext(context.Background(), "root:root")
		err := task.Execute(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(task.Rsp.GetCollectionNames()))
	})

	t.Run("fail to select user", func(t *testing.T) {
		Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
		defer Params.Reset(Params.CommonCfg.AuthorizationEnabled.Key)
		meta := mockrootcoord.NewIMetaTable(t)
		core := newTestCore(withMeta(meta))

		meta.EXPECT().SelectUser(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(nil, errors.New("mock error: select user")).Once()

		task := &showCollectionTask{
			baseTask: newBaseTask(context.Background(), core),
			Req:      &milvuspb.ShowCollectionsRequest{DbName: "default"},
			Rsp:      &milvuspb.ShowCollectionsResponse{},
		}

		ctx := GetContext(context.Background(), "foo:root")
		err := task.Execute(ctx)
		assert.Error(t, err)
	})

	t.Run("no user", func(t *testing.T) {
		Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
		defer Params.Reset(Params.CommonCfg.AuthorizationEnabled.Key)
		meta := mockrootcoord.NewIMetaTable(t)
		core := newTestCore(withMeta(meta))

		meta.EXPECT().SelectUser(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return([]*milvuspb.UserResult{}, nil).Once()

		task := &showCollectionTask{
			baseTask: newBaseTask(context.Background(), core),
			Req:      &milvuspb.ShowCollectionsRequest{DbName: "default"},
			Rsp:      &milvuspb.ShowCollectionsResponse{},
		}

		ctx := GetContext(context.Background(), "foo:root")
		err := task.Execute(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(task.Rsp.GetCollectionNames()))
	})

	t.Run("admin role", func(t *testing.T) {
		Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
		defer Params.Reset(Params.CommonCfg.AuthorizationEnabled.Key)
		meta := mockrootcoord.NewIMetaTable(t)
		core := newTestCore(withMeta(meta))

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
		meta.EXPECT().ListCollections(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*model.Collection{
			{
				DBID:         1,
				CollectionID: 100,
				Name:         "foo",
				CreateTime:   tsoutil.GetCurrentTime(),
			},
		}, nil).Once()

		task := &showCollectionTask{
			baseTask: newBaseTask(context.Background(), core),
			Req:      &milvuspb.ShowCollectionsRequest{DbName: "default"},
			Rsp:      &milvuspb.ShowCollectionsResponse{},
		}
		ctx := GetContext(context.Background(), "foo:root")
		err := task.Execute(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(task.Rsp.GetCollectionNames()))
		assert.Equal(t, "foo", task.Rsp.GetCollectionNames()[0])
	})

	t.Run("select grant error", func(t *testing.T) {
		Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
		defer Params.Reset(Params.CommonCfg.AuthorizationEnabled.Key)
		meta := mockrootcoord.NewIMetaTable(t)
		core := newTestCore(withMeta(meta))

		meta.EXPECT().SelectUser(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return([]*milvuspb.UserResult{
				{
					User: &milvuspb.UserEntity{
						Name: "foo",
					},
					Roles: []*milvuspb.RoleEntity{
						{
							Name: "hoooo",
						},
					},
				},
			}, nil).Once()
		meta.EXPECT().SelectGrant(mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("mock error: select grant")).Once()

		task := &showCollectionTask{
			baseTask: newBaseTask(context.Background(), core),
			Req:      &milvuspb.ShowCollectionsRequest{DbName: "default"},
			Rsp:      &milvuspb.ShowCollectionsResponse{},
		}
		ctx := GetContext(context.Background(), "foo:root")
		err := task.Execute(ctx)
		assert.Error(t, err)
	})

	t.Run("global all privilege", func(t *testing.T) {
		Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
		defer Params.Reset(Params.CommonCfg.AuthorizationEnabled.Key)
		meta := mockrootcoord.NewIMetaTable(t)
		core := newTestCore(withMeta(meta))

		meta.EXPECT().SelectUser(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return([]*milvuspb.UserResult{
				{
					User: &milvuspb.UserEntity{
						Name: "foo",
					},
					Roles: []*milvuspb.RoleEntity{
						{
							Name: "hoooo",
						},
					},
				},
			}, nil).Once()
		meta.EXPECT().SelectGrant(mock.Anything, mock.Anything, mock.Anything).Return([]*milvuspb.GrantEntity{
			{
				Object: &milvuspb.ObjectEntity{Name: commonpb.ObjectType_Global.String()},
				Grantor: &milvuspb.GrantorEntity{
					Privilege: &milvuspb.PrivilegeEntity{
						Name: util.PrivilegeNameForAPI(commonpb.ObjectPrivilege_PrivilegeAll.String()),
					},
				},
			},
		}, nil).Once()
		meta.EXPECT().ListCollections(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*model.Collection{
			{
				DBID:         1,
				CollectionID: 100,
				Name:         "foo",
				CreateTime:   tsoutil.GetCurrentTime(),
			},
		}, nil).Once()

		task := &showCollectionTask{
			baseTask: newBaseTask(context.Background(), core),
			Req:      &milvuspb.ShowCollectionsRequest{DbName: "default"},
			Rsp:      &milvuspb.ShowCollectionsResponse{},
		}
		ctx := GetContext(context.Background(), "foo:root")
		err := task.Execute(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(task.Rsp.GetCollectionNames()))
		assert.Equal(t, "foo", task.Rsp.GetCollectionNames()[0])
	})

	t.Run("collection level privilege group", func(t *testing.T) {
		Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
		defer Params.Reset(Params.CommonCfg.AuthorizationEnabled.Key)
		meta := mockrootcoord.NewIMetaTable(t)
		core := newTestCore(withMeta(meta))

		meta.EXPECT().SelectUser(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return([]*milvuspb.UserResult{
				{
					User: &milvuspb.UserEntity{
						Name: "foo",
					},
					Roles: []*milvuspb.RoleEntity{
						{
							Name: "hoooo",
						},
					},
				},
			}, nil).Once()
		meta.EXPECT().SelectGrant(mock.Anything, mock.Anything, mock.Anything).Return([]*milvuspb.GrantEntity{
			{
				Object: &milvuspb.ObjectEntity{Name: commonpb.ObjectType_Global.String()},
				Grantor: &milvuspb.GrantorEntity{
					Privilege: &milvuspb.PrivilegeEntity{
						Name: util.PrivilegeNameForAPI(commonpb.ObjectPrivilege_PrivilegeGroupCollectionReadOnly.String()),
					},
				},
				ObjectName: util.AnyWord,
			},
		}, nil).Once()
		meta.EXPECT().ListCollections(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*model.Collection{
			{
				DBID:         1,
				CollectionID: 100,
				Name:         "foo",
				CreateTime:   tsoutil.GetCurrentTime(),
			},
		}, nil).Once()

		task := &showCollectionTask{
			baseTask: newBaseTask(context.Background(), core),
			Req:      &milvuspb.ShowCollectionsRequest{DbName: "default"},
			Rsp:      &milvuspb.ShowCollectionsResponse{},
		}
		ctx := GetContext(context.Background(), "foo:root")
		err := task.Execute(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(task.Rsp.GetCollectionNames()))
		assert.Equal(t, "foo", task.Rsp.GetCollectionNames()[0])
	})

	t.Run("all collection", func(t *testing.T) {
		Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
		defer Params.Reset(Params.CommonCfg.AuthorizationEnabled.Key)
		meta := mockrootcoord.NewIMetaTable(t)
		core := newTestCore(withMeta(meta))

		meta.EXPECT().SelectUser(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return([]*milvuspb.UserResult{
				{
					User: &milvuspb.UserEntity{
						Name: "foo",
					},
					Roles: []*milvuspb.RoleEntity{
						{
							Name: "hoooo",
						},
					},
				},
			}, nil).Once()
		meta.EXPECT().SelectGrant(mock.Anything, mock.Anything, mock.Anything).Return([]*milvuspb.GrantEntity{
			{
				Object:     &milvuspb.ObjectEntity{Name: commonpb.ObjectType_Collection.String()},
				ObjectName: util.AnyWord,
			},
		}, nil).Once()
		meta.EXPECT().ListCollections(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*model.Collection{
			{
				DBID:         1,
				CollectionID: 100,
				Name:         "foo",
				CreateTime:   tsoutil.GetCurrentTime(),
			},
		}, nil).Once()

		task := &showCollectionTask{
			baseTask: newBaseTask(context.Background(), core),
			Req:      &milvuspb.ShowCollectionsRequest{DbName: "default"},
			Rsp:      &milvuspb.ShowCollectionsResponse{},
		}
		ctx := GetContext(context.Background(), "foo:root")
		err := task.Execute(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(task.Rsp.GetCollectionNames()))
		assert.Equal(t, "foo", task.Rsp.GetCollectionNames()[0])
	})
	t.Run("normal", func(t *testing.T) {
		Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
		defer Params.Reset(Params.CommonCfg.AuthorizationEnabled.Key)
		meta := mockrootcoord.NewIMetaTable(t)
		core := newTestCore(withMeta(meta))

		meta.EXPECT().SelectUser(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return([]*milvuspb.UserResult{
				{
					User: &milvuspb.UserEntity{
						Name: "foo",
					},
					Roles: []*milvuspb.RoleEntity{
						{
							Name: "hoooo",
						},
					},
				},
			}, nil).Once()
		meta.EXPECT().SelectGrant(mock.Anything, mock.Anything, mock.Anything).Return([]*milvuspb.GrantEntity{
			{
				Object:     &milvuspb.ObjectEntity{Name: commonpb.ObjectType_Collection.String()},
				ObjectName: "a",
			},
			{
				Object: &milvuspb.ObjectEntity{Name: commonpb.ObjectType_Global.String()},
			},
			{
				Object:     &milvuspb.ObjectEntity{Name: commonpb.ObjectType_Collection.String()},
				ObjectName: "b",
			},
		}, nil).Once()
		meta.EXPECT().ListCollections(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*model.Collection{
			{
				DBID:         1,
				CollectionID: 100,
				Name:         "foo",
				CreateTime:   tsoutil.GetCurrentTime(),
			},
			{
				DBID:         1,
				CollectionID: 200,
				Name:         "a",
				CreateTime:   tsoutil.GetCurrentTime(),
			},
		}, nil).Once()

		task := &showCollectionTask{
			baseTask: newBaseTask(context.Background(), core),
			Req:      &milvuspb.ShowCollectionsRequest{DbName: "default"},
			Rsp:      &milvuspb.ShowCollectionsResponse{},
		}
		ctx := GetContext(context.Background(), "foo:root")
		err := task.Execute(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(task.Rsp.GetCollectionNames()))
		assert.Equal(t, "a", task.Rsp.GetCollectionNames()[0])
	})
}
