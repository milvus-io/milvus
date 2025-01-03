/*
 * Licensed to the LF AI & Data foundation under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package rootcoord

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"
)

func TestLockerKey(t *testing.T) {
	clusterLock := NewClusterLockerKey(true)
	assert.Equal(t, clusterLock.IsWLock(), true)
	assert.Equal(t, clusterLock.Level(), ClusterLock)
	assert.Equal(t, clusterLock.LockKey(), "$")

	dbLock := NewDatabaseLockerKey("foo", true)
	assert.Equal(t, dbLock.IsWLock(), true)
	assert.Equal(t, dbLock.Level(), DatabaseLock)
	assert.Equal(t, dbLock.LockKey(), "foo")

	collectionLock := NewCollectionLockerKey("foo", true)
	assert.Equal(t, collectionLock.IsWLock(), true)
	assert.Equal(t, collectionLock.Level(), CollectionLock)
	assert.Equal(t, collectionLock.LockKey(), "foo")

	{
		lockerChain := NewLockerKeyChain(nil)
		assert.Nil(t, lockerChain)
	}

	{
		lockerChain := NewLockerKeyChain(dbLock)
		assert.Nil(t, lockerChain)
	}

	{
		lockerChain := NewLockerKeyChain(clusterLock, collectionLock, dbLock)
		assert.Nil(t, lockerChain)
	}

	{
		lockerChain := NewLockerKeyChain(clusterLock, dbLock, collectionLock)
		assert.NotNil(t, lockerChain)
		assert.Equal(t, lockerChain.Next(), dbLock)
		assert.Equal(t, lockerChain.Next().Next(), collectionLock)
	}
}

func TestGetLockerKey(t *testing.T) {
	t.Run("alter alias task locker key", func(t *testing.T) {
		tt := &alterAliasTask{
			Req: &milvuspb.AlterAliasRequest{
				DbName:         "foo",
				CollectionName: "bar",
			},
		}
		key := tt.GetLockerKey()
		assert.Equal(t, GetLockerKeyString(key), "$-0-false|foo-1-false|bar-2-true")
	})
	t.Run("alter collection task locker key", func(t *testing.T) {
		metaMock := mockrootcoord.NewIMetaTable(t)
		metaMock.EXPECT().GetCollectionByName(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, s string, s2 string, u uint64) (*model.Collection, error) {
				return &model.Collection{
					Name:         s2,
					CollectionID: 111,
				}, nil
			})
		c := &Core{
			meta: metaMock,
		}
		tt := &alterCollectionTask{
			baseTask: baseTask{
				core: c,
			},
			Req: &milvuspb.AlterCollectionRequest{
				DbName:         "foo",
				CollectionName: "bar",
			},
		}
		key := tt.GetLockerKey()
		assert.Equal(t, GetLockerKeyString(key), "$-0-false|foo-1-false|111-2-true")
	})
	t.Run("alter collection task locker key by ID", func(t *testing.T) {
		metaMock := mockrootcoord.NewIMetaTable(t)
		c := &Core{
			meta: metaMock,
		}
		tt := &alterCollectionTask{
			baseTask: baseTask{
				core: c,
			},
			Req: &milvuspb.AlterCollectionRequest{
				DbName:         "foo",
				CollectionName: "",
				CollectionID:   111,
			},
		}
		key := tt.GetLockerKey()
		assert.Equal(t, GetLockerKeyString(key), "$-0-false|foo-1-false|111-2-true")
	})
	t.Run("alter database task locker key", func(t *testing.T) {
		tt := &alterDatabaseTask{
			Req: &rootcoordpb.AlterDatabaseRequest{
				DbName: "foo",
			},
		}
		key := tt.GetLockerKey()
		assert.Equal(t, GetLockerKeyString(key), "$-0-false|foo-1-true")
	})
	t.Run("create alias task locker key", func(t *testing.T) {
		metaMock := mockrootcoord.NewIMetaTable(t)
		c := &Core{
			meta: metaMock,
		}
		tt := &createAliasTask{
			baseTask: baseTask{
				core: c,
			},
			Req: &milvuspb.CreateAliasRequest{
				DbName:         "foo",
				CollectionName: "bar",
			},
		}
		key := tt.GetLockerKey()
		assert.Equal(t, GetLockerKeyString(key), "$-0-false|foo-1-false|bar-2-true")
	})
	t.Run("create collection task locker key", func(t *testing.T) {
		tt := &createCollectionTask{
			Req: &milvuspb.CreateCollectionRequest{
				DbName:         "foo",
				CollectionName: "bar",
			},
			collID: 10,
		}
		key := tt.GetLockerKey()
		assert.Equal(t, GetLockerKeyString(key), "$-0-false|foo-1-false|10-2-true")
	})
	t.Run("create database task locker key", func(t *testing.T) {
		tt := &createDatabaseTask{
			Req: &milvuspb.CreateDatabaseRequest{
				DbName: "foo",
			},
		}
		key := tt.GetLockerKey()
		assert.Equal(t, GetLockerKeyString(key), "$-0-true")
	})
	t.Run("create partition task locker key", func(t *testing.T) {
		metaMock := mockrootcoord.NewIMetaTable(t)
		metaMock.EXPECT().GetCollectionByName(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, s string, s2 string, u uint64) (*model.Collection, error) {
				return &model.Collection{
					Name:         "real" + s2,
					CollectionID: 111,
				}, nil
			})
		c := &Core{
			meta: metaMock,
		}
		tt := &createPartitionTask{
			baseTask: baseTask{core: c},
			Req: &milvuspb.CreatePartitionRequest{
				DbName:         "foo",
				CollectionName: "bar",
				PartitionName:  "baz",
			},
		}
		key := tt.GetLockerKey()
		assert.Equal(t, GetLockerKeyString(key), "$-0-false|foo-1-false|111-2-true")
	})
	t.Run("describe collection task locker key", func(t *testing.T) {
		metaMock := mockrootcoord.NewIMetaTable(t)
		metaMock.EXPECT().GetCollectionByName(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, s string, s2 string, u uint64) (*model.Collection, error) {
				return nil, errors.New("not found")
			})
		c := &Core{
			meta: metaMock,
		}
		tt := &describeCollectionTask{
			baseTask: baseTask{core: c},
			Req: &milvuspb.DescribeCollectionRequest{
				DbName:         "foo",
				CollectionName: "bar",
			},
		}
		key := tt.GetLockerKey()
		assert.Equal(t, GetLockerKeyString(key), "$-0-false|foo-1-false|-1-2-false")
	})
	t.Run("describe collection task locker key by ID", func(t *testing.T) {
		metaMock := mockrootcoord.NewIMetaTable(t)
		c := &Core{
			meta: metaMock,
		}
		tt := &describeCollectionTask{
			baseTask: baseTask{core: c},
			Req: &milvuspb.DescribeCollectionRequest{
				DbName:         "foo",
				CollectionName: "",
				CollectionID:   111,
			},
		}
		key := tt.GetLockerKey()
		assert.Equal(t, GetLockerKeyString(key), "$-0-false|foo-1-false|111-2-false")
	})
	t.Run("describe database task locker key", func(t *testing.T) {
		tt := &describeDBTask{
			Req: &rootcoordpb.DescribeDatabaseRequest{
				DbName: "foo",
			},
		}
		key := tt.GetLockerKey()
		assert.Equal(t, GetLockerKeyString(key), "$-0-false|foo-1-false")
	})
	t.Run("drop alias task locker key", func(t *testing.T) {
		metaMock := mockrootcoord.NewIMetaTable(t)
		metaMock.EXPECT().GetCollectionByName(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, s string, s2 string, u uint64) (*model.Collection, error) {
				return &model.Collection{
					Name:         "real" + s2,
					CollectionID: 111,
				}, nil
			})
		c := &Core{
			meta: metaMock,
		}
		tt := &dropAliasTask{
			baseTask: baseTask{core: c},
			Req: &milvuspb.DropAliasRequest{
				DbName: "foo",
				Alias:  "bar",
			},
		}
		key := tt.GetLockerKey()
		assert.Equal(t, GetLockerKeyString(key), "$-0-false|foo-1-false|111-2-true")
	})
	t.Run("drop collection task locker key", func(t *testing.T) {
		metaMock := mockrootcoord.NewIMetaTable(t)
		metaMock.EXPECT().GetCollectionByName(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, s string, s2 string, u uint64) (*model.Collection, error) {
				return &model.Collection{
					Name:         "bar",
					CollectionID: 111,
				}, nil
			})
		c := &Core{
			meta: metaMock,
		}
		tt := &dropCollectionTask{
			baseTask: baseTask{core: c},
			Req: &milvuspb.DropCollectionRequest{
				DbName:         "foo",
				CollectionName: "bar",
			},
		}
		key := tt.GetLockerKey()
		assert.Equal(t, GetLockerKeyString(key), "$-0-false|foo-1-false|111-2-true")
	})
	t.Run("drop database task locker key", func(t *testing.T) {
		tt := &dropDatabaseTask{
			Req: &milvuspb.DropDatabaseRequest{
				DbName: "foo",
			},
		}
		key := tt.GetLockerKey()
		assert.Equal(t, GetLockerKeyString(key), "$-0-true")
	})
	t.Run("drop partition task locker key", func(t *testing.T) {
		metaMock := mockrootcoord.NewIMetaTable(t)
		metaMock.EXPECT().GetCollectionByName(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, s string, s2 string, u uint64) (*model.Collection, error) {
				return &model.Collection{
					Name:         "real" + s2,
					CollectionID: 111,
				}, nil
			})
		c := &Core{
			meta: metaMock,
		}
		tt := &dropPartitionTask{
			baseTask: baseTask{core: c},
			Req: &milvuspb.DropPartitionRequest{
				DbName:         "foo",
				CollectionName: "bar",
				PartitionName:  "baz",
			},
		}
		key := tt.GetLockerKey()
		assert.Equal(t, GetLockerKeyString(key), "$-0-false|foo-1-false|111-2-true")
	})
	t.Run("has collection task locker key", func(t *testing.T) {
		tt := &hasCollectionTask{
			Req: &milvuspb.HasCollectionRequest{
				DbName:         "foo",
				CollectionName: "bar",
			},
		}
		key := tt.GetLockerKey()
		assert.Equal(t, GetLockerKeyString(key), "$-0-false|foo-1-false")
	})
	t.Run("has partition task locker key", func(t *testing.T) {
		metaMock := mockrootcoord.NewIMetaTable(t)
		metaMock.EXPECT().GetCollectionByName(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, s string, s2 string, u uint64) (*model.Collection, error) {
				return &model.Collection{
					Name:         "real" + s2,
					CollectionID: 111,
				}, nil
			})
		c := &Core{
			meta: metaMock,
		}
		tt := &hasPartitionTask{
			baseTask: baseTask{core: c},
			Req: &milvuspb.HasPartitionRequest{
				DbName:         "foo",
				CollectionName: "bar",
				PartitionName:  "baz",
			},
		}
		key := tt.GetLockerKey()
		assert.Equal(t, GetLockerKeyString(key), "$-0-false|foo-1-false|111-2-false")
	})
	t.Run("list db task locker key", func(t *testing.T) {
		tt := &listDatabaseTask{}
		key := tt.GetLockerKey()
		assert.Equal(t, GetLockerKeyString(key), "$-0-false")
	})
	t.Run("rename collection task locker key", func(t *testing.T) {
		tt := &renameCollectionTask{
			Req: &milvuspb.RenameCollectionRequest{
				DbName:  "foo",
				OldName: "bar",
				NewName: "baz",
			},
		}
		key := tt.GetLockerKey()
		assert.Equal(t, GetLockerKeyString(key), "$-0-true")
	})
	t.Run("show collection task locker key", func(t *testing.T) {
		tt := &showCollectionTask{
			Req: &milvuspb.ShowCollectionsRequest{
				DbName: "foo",
			},
		}
		key := tt.GetLockerKey()
		assert.Equal(t, GetLockerKeyString(key), "$-0-false|foo-1-false")
	})
	t.Run("show partition task locker key", func(t *testing.T) {
		metaMock := mockrootcoord.NewIMetaTable(t)
		metaMock.EXPECT().GetCollectionByName(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, s string, s2 string, u uint64) (*model.Collection, error) {
				return &model.Collection{
					Name:         "real" + s2,
					CollectionID: 111,
				}, nil
			})
		c := &Core{
			meta: metaMock,
		}
		tt := &showPartitionTask{
			baseTask: baseTask{core: c},
			Req: &milvuspb.ShowPartitionsRequest{
				DbName:         "foo",
				CollectionName: "bar",
			},
		}
		key := tt.GetLockerKey()
		assert.Equal(t, GetLockerKeyString(key), "$-0-false|foo-1-false|111-2-false")
	})
	t.Run("show partition task locker key by ID", func(t *testing.T) {
		metaMock := mockrootcoord.NewIMetaTable(t)
		c := &Core{
			meta: metaMock,
		}
		tt := &showPartitionTask{
			baseTask: baseTask{core: c},
			Req: &milvuspb.ShowPartitionsRequest{
				DbName:         "foo",
				CollectionName: "",
				CollectionID:   111,
			},
		}
		key := tt.GetLockerKey()
		assert.Equal(t, GetLockerKeyString(key), "$-0-false|foo-1-false|111-2-false")
	})
}
