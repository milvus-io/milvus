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
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func Test_describeCollectionTask_Prepare(t *testing.T) {
	t.Run("invalid msg type", func(t *testing.T) {
		task := &describeCollectionTask{
			Req: &milvuspb.DescribeCollectionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_DropCollection,
				},
			},
		}
		err := task.Prepare(context.Background())
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		task := &describeCollectionTask{
			Req: &milvuspb.DescribeCollectionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_DescribeCollection,
				},
			},
		}
		err := task.Prepare(context.Background())
		assert.NoError(t, err)
	})
}

func Test_describeCollectionTask_Execute(t *testing.T) {
	t.Run("failed to get collection by name", func(t *testing.T) {
		core := newTestCore(withInvalidMeta())
		task := &describeCollectionTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &milvuspb.DescribeCollectionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_DescribeCollection,
				},
				CollectionName: "test coll",
			},
			Rsp: &milvuspb.DescribeCollectionResponse{},
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
	})

	t.Run("failed to get collection by id", func(t *testing.T) {
		core := newTestCore(withInvalidMeta())
		task := &describeCollectionTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &milvuspb.DescribeCollectionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_DescribeCollection,
				},
				CollectionID: 1,
			},
			Rsp: &milvuspb.DescribeCollectionResponse{},
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
	})

	t.Run("success", func(t *testing.T) {
		alias1, alias2 := funcutil.GenRandomStr(), funcutil.GenRandomStr()
		meta := mockrootcoord.NewIMetaTable(t)
		meta.On("GetCollectionByID",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(&model.Collection{
			CollectionID: 1,
			Name:         "test coll",
			DBID:         1,
		}, nil)
		meta.On("ListAliasesByID",
			mock.Anything,
			mock.Anything,
		).Return([]string{alias1, alias2})
		meta.EXPECT().GetDatabaseByID(mock.Anything, mock.Anything, mock.Anything).Return(&model.Database{
			ID:   1,
			Name: "test db",
		}, nil)

		core := newTestCore(withMeta(meta))
		task := &describeCollectionTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &milvuspb.DescribeCollectionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_DescribeCollection,
				},
				CollectionID: 1,
			},
			Rsp: &milvuspb.DescribeCollectionResponse{},
		}
		err := task.Execute(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, task.Rsp.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
		assert.Equal(t, "test db", task.Rsp.GetDbName())
		assert.ElementsMatch(t, []string{alias1, alias2}, task.Rsp.GetAliases())
	})
}

func TestDescribeCollectionsAuth(t *testing.T) {
	paramtable.Init()

	getTask := func(core *Core) *describeCollectionTask {
		return &describeCollectionTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &milvuspb.DescribeCollectionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_DescribeCollection,
				},
				DbName:         "default",
				CollectionName: "test coll",
			},
			Rsp: &milvuspb.DescribeCollectionResponse{},
		}
	}

	t.Run("no auth", func(t *testing.T) {
		Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "false")
		defer Params.Reset(Params.CommonCfg.AuthorizationEnabled.Key)
		meta := mockrootcoord.NewIMetaTable(t)
		core := newTestCore(withMeta(meta))

		meta.EXPECT().GetCollectionByName(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&model.Collection{
			CollectionID: 1,
			Name:         "test coll",
			DBID:         1,
		}, nil).Once()
		meta.EXPECT().ListAliasesByID(mock.Anything, mock.Anything).Return([]string{}).Once()
		meta.EXPECT().GetDatabaseByID(mock.Anything, mock.Anything, mock.Anything).Return(&model.Database{
			ID:   1,
			Name: "test db",
		}, nil).Once()

		task := getTask(core)

		err := task.Execute(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, int32(0), task.Rsp.GetStatus().GetCode())
		assert.Equal(t, "test db", task.Rsp.GetDbName())
		assert.Equal(t, int64(1), task.Rsp.GetDbId())
		assert.Equal(t, "test coll", task.Rsp.GetCollectionName())
		assert.Equal(t, int64(1), task.Rsp.GetCollectionID())
	})

	t.Run("empty ctx", func(t *testing.T) {
		Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
		defer Params.Reset(Params.CommonCfg.AuthorizationEnabled.Key)
		meta := mockrootcoord.NewIMetaTable(t)
		core := newTestCore(withMeta(meta))

		meta.EXPECT().GetCollectionByName(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&model.Collection{
			CollectionID: 1,
			Name:         "test coll",
			DBID:         1,
		}, nil).Once()
		meta.EXPECT().ListAliasesByID(mock.Anything, mock.Anything).Return([]string{}).Once()
		meta.EXPECT().GetDatabaseByID(mock.Anything, mock.Anything, mock.Anything).Return(&model.Database{
			ID:   1,
			Name: "test db",
		}, nil).Once()

		task := getTask(core)

		err := task.Execute(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, int32(0), task.Rsp.GetStatus().GetCode())
		assert.Equal(t, "test db", task.Rsp.GetDbName())
		assert.Equal(t, int64(1), task.Rsp.GetDbId())
		assert.Equal(t, "test coll", task.Rsp.GetCollectionName())
		assert.Equal(t, int64(1), task.Rsp.GetCollectionID())
	})

	t.Run("root user", func(t *testing.T) {
		Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
		defer Params.Reset(Params.CommonCfg.AuthorizationEnabled.Key)
		meta := mockrootcoord.NewIMetaTable(t)
		core := newTestCore(withMeta(meta))

		meta.EXPECT().GetCollectionByName(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&model.Collection{
			CollectionID: 1,
			Name:         "test coll",
			DBID:         1,
		}, nil).Once()
		meta.EXPECT().ListAliasesByID(mock.Anything, mock.Anything).Return([]string{}).Once()
		meta.EXPECT().GetDatabaseByID(mock.Anything, mock.Anything, mock.Anything).Return(&model.Database{
			ID:   1,
			Name: "test db",
		}, nil).Once()

		task := getTask(core)

		ctx := GetContext(context.Background(), "root:root")
		err := task.Execute(ctx)
		assert.NoError(t, err)
		assert.Equal(t, int32(0), task.Rsp.GetStatus().GetCode())
		assert.Equal(t, "test db", task.Rsp.GetDbName())
		assert.Equal(t, int64(1), task.Rsp.GetDbId())
		assert.Equal(t, "test coll", task.Rsp.GetCollectionName())
		assert.Equal(t, int64(1), task.Rsp.GetCollectionID())
	})

	t.Run("root user, should bind role", func(t *testing.T) {
		Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
		Params.Save(Params.CommonCfg.RootShouldBindRole.Key, "true")
		defer Params.Reset(Params.CommonCfg.AuthorizationEnabled.Key)
		defer Params.Reset(Params.CommonCfg.RootShouldBindRole.Key)
		meta := mockrootcoord.NewIMetaTable(t)
		core := newTestCore(withMeta(meta))

		meta.EXPECT().GetCollectionByName(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&model.Collection{
			CollectionID: 1,
			Name:         "test coll",
			DBID:         1,
		}, nil).Once()
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

		task := getTask(core)

		ctx := GetContext(context.Background(), "root:root")
		err := task.Execute(ctx)
		assert.Error(t, err)
	})

	t.Run("fail to select user", func(t *testing.T) {
		Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
		defer Params.Reset(Params.CommonCfg.AuthorizationEnabled.Key)
		meta := mockrootcoord.NewIMetaTable(t)
		core := newTestCore(withMeta(meta))

		meta.EXPECT().GetCollectionByName(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&model.Collection{
			CollectionID: 1,
			Name:         "test coll",
			DBID:         1,
		}, nil).Once()
		meta.EXPECT().SelectUser(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(nil, errors.New("mock error: select user")).Once()

		task := getTask(core)

		ctx := GetContext(context.Background(), "foo:root")
		err := task.Execute(ctx)
		assert.Error(t, err)
	})

	t.Run("no user", func(t *testing.T) {
		Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
		defer Params.Reset(Params.CommonCfg.AuthorizationEnabled.Key)
		meta := mockrootcoord.NewIMetaTable(t)
		core := newTestCore(withMeta(meta))

		meta.EXPECT().GetCollectionByName(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&model.Collection{
			CollectionID: 1,
			Name:         "test coll",
			DBID:         1,
		}, nil).Once()
		meta.EXPECT().SelectUser(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return([]*milvuspb.UserResult{}, nil).Once()

		task := getTask(core)

		ctx := GetContext(context.Background(), "foo:root")
		err := task.Execute(ctx)
		assert.Error(t, err)
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
		meta.EXPECT().GetCollectionByName(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&model.Collection{
			CollectionID: 1,
			Name:         "test coll",
			DBID:         1,
		}, nil).Once()
		meta.EXPECT().ListAliasesByID(mock.Anything, mock.Anything).Return([]string{}).Once()
		meta.EXPECT().GetDatabaseByID(mock.Anything, mock.Anything, mock.Anything).Return(&model.Database{
			ID:   1,
			Name: "test db",
		}, nil).Once()

		task := getTask(core)
		ctx := GetContext(context.Background(), "foo:root")
		err := task.Execute(ctx)
		assert.NoError(t, err)
		assert.Equal(t, int32(0), task.Rsp.GetStatus().GetCode())
		assert.Equal(t, "test db", task.Rsp.GetDbName())
		assert.Equal(t, int64(1), task.Rsp.GetDbId())
		assert.Equal(t, "test coll", task.Rsp.GetCollectionName())
		assert.Equal(t, int64(1), task.Rsp.GetCollectionID())
	})

	t.Run("select grant error", func(t *testing.T) {
		Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
		defer Params.Reset(Params.CommonCfg.AuthorizationEnabled.Key)
		meta := mockrootcoord.NewIMetaTable(t)
		core := newTestCore(withMeta(meta))

		meta.EXPECT().GetCollectionByName(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&model.Collection{
			CollectionID: 1,
			Name:         "test coll",
			DBID:         1,
		}, nil).Once()
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

		task := getTask(core)
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
		meta.EXPECT().GetCollectionByName(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&model.Collection{
			CollectionID: 1,
			Name:         "test coll",
			DBID:         1,
		}, nil).Once()
		meta.EXPECT().ListAliasesByID(mock.Anything, mock.Anything).Return([]string{}).Once()
		meta.EXPECT().GetDatabaseByID(mock.Anything, mock.Anything, mock.Anything).Return(&model.Database{
			ID:   1,
			Name: "test db",
		}, nil).Once()

		task := getTask(core)
		ctx := GetContext(context.Background(), "foo:root")
		err := task.Execute(ctx)
		assert.NoError(t, err)
		assert.Equal(t, int32(0), task.Rsp.GetStatus().GetCode())
		assert.Equal(t, "test db", task.Rsp.GetDbName())
		assert.Equal(t, int64(1), task.Rsp.GetDbId())
		assert.Equal(t, "test coll", task.Rsp.GetCollectionName())
		assert.Equal(t, int64(1), task.Rsp.GetCollectionID())
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
		meta.EXPECT().GetCollectionByName(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&model.Collection{
			CollectionID: 1,
			Name:         "test coll",
			DBID:         1,
		}, nil).Once()
		meta.EXPECT().ListAliasesByID(mock.Anything, mock.Anything).Return([]string{}).Once()
		meta.EXPECT().GetDatabaseByID(mock.Anything, mock.Anything, mock.Anything).Return(&model.Database{
			ID:   1,
			Name: "test db",
		}, nil).Once()
		meta.EXPECT().IsCustomPrivilegeGroup(mock.Anything, util.PrivilegeNameForAPI(commonpb.ObjectPrivilege_PrivilegeGroupCollectionReadOnly.String())).Return(false, nil).Once()

		task := getTask(core)
		ctx := GetContext(context.Background(), "foo:root")
		err := task.Execute(ctx)
		assert.NoError(t, err)
		assert.Equal(t, int32(0), task.Rsp.GetStatus().GetCode())
		assert.Equal(t, "test db", task.Rsp.GetDbName())
		assert.Equal(t, int64(1), task.Rsp.GetDbId())
		assert.Equal(t, "test coll", task.Rsp.GetCollectionName())
		assert.Equal(t, int64(1), task.Rsp.GetCollectionID())
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
		meta.EXPECT().GetCollectionByName(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&model.Collection{
			CollectionID: 1,
			Name:         "test coll",
			DBID:         1,
		}, nil).Once()
		meta.EXPECT().ListAliasesByID(mock.Anything, mock.Anything).Return([]string{}).Once()
		meta.EXPECT().GetDatabaseByID(mock.Anything, mock.Anything, mock.Anything).Return(&model.Database{
			ID:   1,
			Name: "test db",
		}, nil).Once()

		task := getTask(core)
		ctx := GetContext(context.Background(), "foo:root")
		err := task.Execute(ctx)
		assert.NoError(t, err)
		assert.Equal(t, int32(0), task.Rsp.GetStatus().GetCode())
		assert.Equal(t, "test db", task.Rsp.GetDbName())
		assert.Equal(t, int64(1), task.Rsp.GetDbId())
		assert.Equal(t, "test coll", task.Rsp.GetCollectionName())
		assert.Equal(t, int64(1), task.Rsp.GetCollectionID())
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
				ObjectName: "test coll",
			},
			{
				Object: &milvuspb.ObjectEntity{Name: commonpb.ObjectType_Global.String()},
			},
			{
				Object:     &milvuspb.ObjectEntity{Name: commonpb.ObjectType_Collection.String()},
				ObjectName: "b",
			},
		}, nil).Once()
		meta.EXPECT().GetCollectionByName(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&model.Collection{
			CollectionID: 1,
			Name:         "test coll",
			DBID:         1,
		}, nil).Once()
		meta.EXPECT().ListAliasesByID(mock.Anything, mock.Anything).Return([]string{}).Once()
		meta.EXPECT().GetDatabaseByID(mock.Anything, mock.Anything, mock.Anything).Return(&model.Database{
			ID:   1,
			Name: "test db",
		}, nil).Once()
		meta.EXPECT().IsCustomPrivilegeGroup(mock.Anything, mock.Anything).Return(false, nil).Once()

		task := getTask(core)
		ctx := GetContext(context.Background(), "foo:root")
		err := task.Execute(ctx)
		assert.NoError(t, err)
		assert.Equal(t, int32(0), task.Rsp.GetStatus().GetCode())
		assert.Equal(t, "test db", task.Rsp.GetDbName())
		assert.Equal(t, int64(1), task.Rsp.GetDbId())
		assert.Equal(t, "test coll", task.Rsp.GetCollectionName())
		assert.Equal(t, int64(1), task.Rsp.GetCollectionID())
	})

	t.Run("custom privilege group", func(t *testing.T) {
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
				Object: &milvuspb.ObjectEntity{Name: "custom_type"},
				Grantor: &milvuspb.GrantorEntity{
					Privilege: &milvuspb.PrivilegeEntity{
						Name: "privilege_group",
					},
				},
				ObjectName: "test coll",
			},
		}, nil).Once()
		meta.EXPECT().IsCustomPrivilegeGroup(mock.Anything, "privilege_group").Return(true, nil).Once()
		meta.EXPECT().GetCollectionByName(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&model.Collection{
			CollectionID: 1,
			Name:         "test coll",
			DBID:         1,
		}, nil).Once()
		meta.EXPECT().ListAliasesByID(mock.Anything, mock.Anything).Return([]string{}).Once()
		meta.EXPECT().GetDatabaseByID(mock.Anything, mock.Anything, mock.Anything).Return(&model.Database{
			ID:   1,
			Name: "test db",
		}, nil).Once()

		task := getTask(core)
		ctx := GetContext(context.Background(), "foo:root")
		err := task.Execute(ctx)
		assert.NoError(t, err)
		assert.Equal(t, int32(0), task.Rsp.GetStatus().GetCode())
		assert.Equal(t, "test db", task.Rsp.GetDbName())
		assert.Equal(t, int64(1), task.Rsp.GetDbId())
		assert.Equal(t, "test coll", task.Rsp.GetCollectionName())
		assert.Equal(t, int64(1), task.Rsp.GetCollectionID())
	})
}
