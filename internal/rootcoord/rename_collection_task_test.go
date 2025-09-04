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
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
)

func Test_renameCollectionTask_Prepare(t *testing.T) {
	t.Run("invalid msg type", func(t *testing.T) {
		task := &renameCollectionTask{
			Req: &milvuspb.RenameCollectionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_Undefined,
				},
			},
		}
		err := task.Prepare(context.Background())
		assert.Error(t, err)
	})

	t.Run("normal case same database", func(t *testing.T) {
		task := &renameCollectionTask{
			Req: &milvuspb.RenameCollectionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_RenameCollection,
				},
				DbName:  "db1",
				OldName: "old_collection",
				NewName: "new_collection",
			},
		}
		err := task.Prepare(context.Background())
		assert.NoError(t, err)
	})

	t.Run("cross database with encryption enabled", func(t *testing.T) {
		oldDB := &model.Database{
			Name: "db1",
			ID:   1,
			Properties: []*commonpb.KeyValuePair{
				{Key: "milvus.storage.encryption.enabled", Value: "true"},
			},
		}
		newDB := &model.Database{
			Name: "db2",
			ID:   2,
		}

		meta := mockrootcoord.NewIMetaTable(t)
		meta.On("GetDatabaseByName",
			mock.Anything,
			"db1",
			mock.Anything,
		).Return(oldDB, nil)
		meta.On("GetDatabaseByName",
			mock.Anything,
			"db2",
			mock.Anything,
		).Return(newDB, nil)

		core := newTestCore(withMeta(meta))
		task := &renameCollectionTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &milvuspb.RenameCollectionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_RenameCollection,
				},
				DbName:    "db1",
				OldName:   "old_collection",
				NewDBName: "db2",
				NewName:   "new_collection",
			},
		}
		err := task.Prepare(context.Background())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "deny to change collection databases due to at least one database enabled encryption")
	})

	t.Run("cross database without encryption", func(t *testing.T) {
		oldDB := &model.Database{
			Name: "db1",
			ID:   1,
		}
		newDB := &model.Database{
			Name: "db2",
			ID:   2,
		}

		meta := mockrootcoord.NewIMetaTable(t)
		meta.On("GetDatabaseByName",
			mock.Anything,
			"db1",
			mock.Anything,
		).Return(oldDB, nil)
		meta.On("GetDatabaseByName",
			mock.Anything,
			"db2",
			mock.Anything,
		).Return(newDB, nil)

		core := newTestCore(withMeta(meta))
		task := &renameCollectionTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &milvuspb.RenameCollectionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_RenameCollection,
				},
				DbName:    "db1",
				OldName:   "old_collection",
				NewDBName: "db2",
				NewName:   "new_collection",
			},
		}
		err := task.Prepare(context.Background())
		assert.NoError(t, err)
	})
}

func Test_renameCollectionTask_Execute(t *testing.T) {
	t.Run("collection not found", func(t *testing.T) {
		oldName := funcutil.GenRandomStr()
		newName := funcutil.GenRandomStr()
		
		meta := mockrootcoord.NewIMetaTable(t)
		meta.On("GetCollectionID",
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(int64(0))
		
		core := newTestCore(withMeta(meta))
		task := &renameCollectionTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &milvuspb.RenameCollectionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_RenameCollection,
				},
				OldName: oldName,
				NewName: newName,
			},
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
	})

	t.Run("rename step failed", func(t *testing.T) {
		oldName := funcutil.GenRandomStr()
		newName := funcutil.GenRandomStr()
		collectionID := int64(111)
		
		meta := mockrootcoord.NewIMetaTable(t)
		meta.On("GetCollectionID",
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(collectionID)
		meta.On("ListAliasesByID",
			mock.Anything,
			mock.Anything,
		).Return([]string{})
		meta.On("RenameCollection",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(errors.New("failed to rename collection"))
		
		core := newTestCore(withValidProxyManager(), withMeta(meta))
		task := &renameCollectionTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &milvuspb.RenameCollectionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_RenameCollection,
				},
				OldName: oldName,
				NewName: newName,
			},
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
	})

	t.Run("expire cache failed", func(t *testing.T) {
		oldName := funcutil.GenRandomStr()
		newName := funcutil.GenRandomStr()
		collectionID := int64(111)
		
		meta := mockrootcoord.NewIMetaTable(t)
		meta.On("GetCollectionID",
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(collectionID)
		meta.On("ListAliasesByID",
			mock.Anything,
			mock.Anything,
		).Return([]string{})
		meta.On("RenameCollection",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil)
		
		core := newTestCore(withInvalidProxyManager(), withMeta(meta))
		task := &renameCollectionTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &milvuspb.RenameCollectionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_RenameCollection,
				},
				OldName: oldName,
				NewName: newName,
			},
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
	})

	t.Run("rename with aliases within same database", func(t *testing.T) {
		oldName := funcutil.GenRandomStr()
		newName := funcutil.GenRandomStr()
		collectionID := int64(111)
		aliases := []string{"alias1", "alias2"}
		
		meta := mockrootcoord.NewIMetaTable(t)
		meta.On("IsAlias",
			mock.Anything,
			mock.Anything,
			oldName,
		).Return(false)
		meta.On("GetCollectionID",
			mock.Anything,
			mock.Anything,
			oldName,
		).Return(collectionID)
		meta.On("IsAlias",
			mock.Anything,
			mock.Anything,
			newName,
		).Return(false)
		meta.On("GetCollectionID",
			mock.Anything,
			mock.Anything,
			newName,
		).Return(int64(0))
		meta.On("ListAliasesByID",
			mock.Anything,
			mock.Anything,
		).Return(aliases)
		meta.On("RenameCollection",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil)
		
		core := newTestCore(withValidProxyManager(), withMeta(meta))
		task := &renameCollectionTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &milvuspb.RenameCollectionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_RenameCollection,
				},
				DbName:  "db1",
				OldName: oldName,
				NewName: newName,
			},
		}
		err := task.Execute(context.Background())
		assert.NoError(t, err)
	})

	t.Run("rename with aliases across databases should fail", func(t *testing.T) {
		oldName := funcutil.GenRandomStr()
		newName := funcutil.GenRandomStr()
		collectionID := int64(111)
		aliases := []string{"alias1", "alias2"}
		
		meta := mockrootcoord.NewIMetaTable(t)
		meta.On("IsAlias",
			mock.Anything,
			"db1",
			oldName,
		).Return(false)
		meta.On("GetCollectionID",
			mock.Anything,
			"db1",
			oldName,
		).Return(collectionID)
		meta.On("IsAlias",
			mock.Anything,
			"db2",
			newName,
		).Return(false)
		meta.On("GetCollectionID",
			mock.Anything,
			"db2",
			newName,
		).Return(int64(0))
		meta.On("ListAliasesByID",
			mock.Anything,
			mock.Anything,
		).Return(aliases)
		
		core := newTestCore(withMeta(meta))
		task := &renameCollectionTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &milvuspb.RenameCollectionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_RenameCollection,
				},
				DbName:    "db1",
				OldName:   oldName,
				NewDBName: "db2",
				NewName:   newName,
			},
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "must drop all aliases")
	})

	t.Run("rename using alias as old name should fail", func(t *testing.T) {
		oldName := funcutil.GenRandomStr()
		newName := funcutil.GenRandomStr()
		
		meta := mockrootcoord.NewIMetaTable(t)
		meta.On("IsAlias",
			mock.Anything,
			mock.Anything,
			oldName,
		).Return(true)
		
		core := newTestCore(withMeta(meta))
		task := &renameCollectionTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &milvuspb.RenameCollectionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_RenameCollection,
				},
				OldName: oldName,
				NewName: newName,
			},
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported use an alias to rename collection")
	})

	t.Run("rename to existing alias should fail", func(t *testing.T) {
		oldName := funcutil.GenRandomStr()
		newName := funcutil.GenRandomStr()
		collectionID := int64(111)
		
		meta := mockrootcoord.NewIMetaTable(t)
		meta.On("IsAlias",
			mock.Anything,
			mock.Anything,
			oldName,
		).Return(false)
		meta.On("GetCollectionID",
			mock.Anything,
			mock.Anything,
			oldName,
		).Return(collectionID)
		meta.On("IsAlias",
			mock.Anything,
			mock.Anything,
			newName,
		).Return(true)
		
		core := newTestCore(withMeta(meta))
		task := &renameCollectionTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &milvuspb.RenameCollectionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_RenameCollection,
				},
				OldName: oldName,
				NewName: newName,
			},
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cannot rename collection to an existing alias")
	})

	t.Run("rename to existing collection should fail", func(t *testing.T) {
		oldName := funcutil.GenRandomStr()
		newName := funcutil.GenRandomStr()
		collectionID := int64(111)
		existingCollID := int64(222)
		
		meta := mockrootcoord.NewIMetaTable(t)
		meta.On("IsAlias",
			mock.Anything,
			mock.Anything,
			oldName,
		).Return(false)
		meta.On("GetCollectionID",
			mock.Anything,
			mock.Anything,
			oldName,
		).Return(collectionID)
		meta.On("IsAlias",
			mock.Anything,
			mock.Anything,
			newName,
		).Return(false)
		meta.On("GetCollectionID",
			mock.Anything,
			mock.Anything,
			newName,
		).Return(existingCollID)
		
		core := newTestCore(withMeta(meta))
		task := &renameCollectionTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &milvuspb.RenameCollectionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_RenameCollection,
				},
				OldName: oldName,
				NewName: newName,
			},
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "duplicated new collection name")
	})

	t.Run("rename across databases without aliases", func(t *testing.T) {
		oldName := funcutil.GenRandomStr()
		newName := funcutil.GenRandomStr()
		oldDB := "db1"
		newDB := "db2"
		collectionID := int64(111)
		
		meta := mockrootcoord.NewIMetaTable(t)
		meta.On("IsAlias",
			mock.Anything,
			oldDB,
			oldName,
		).Return(false)
		meta.On("GetCollectionID",
			mock.Anything,
			oldDB,
			oldName,
		).Return(collectionID)
		meta.On("IsAlias",
			mock.Anything,
			newDB,
			newName,
		).Return(false)
		meta.On("GetCollectionID",
			mock.Anything,
			newDB,
			newName,
		).Return(int64(0))
		meta.On("ListAliasesByID",
			mock.Anything,
			mock.Anything,
		).Return([]string{})
		meta.On("RenameCollection",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil)
		
		core := newTestCore(withValidProxyManager(), withMeta(meta))
		task := &renameCollectionTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &milvuspb.RenameCollectionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_RenameCollection,
				},
				DbName:    oldDB,
				OldName:   oldName,
				NewDBName: newDB,
				NewName:   newName,
			},
		}
		err := task.Execute(context.Background())
		assert.NoError(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		oldName := funcutil.GenRandomStr()
		newName := funcutil.GenRandomStr()
		collectionID := int64(111)
		
		meta := mockrootcoord.NewIMetaTable(t)
		meta.On("IsAlias",
			mock.Anything,
			mock.Anything,
			oldName,
		).Return(false)
		meta.On("GetCollectionID",
			mock.Anything,
			mock.Anything,
			oldName,
		).Return(collectionID)
		meta.On("IsAlias",
			mock.Anything,
			mock.Anything,
			newName,
		).Return(false)
		meta.On("GetCollectionID",
			mock.Anything,
			mock.Anything,
			newName,
		).Return(int64(0))
		meta.On("ListAliasesByID",
			mock.Anything,
			mock.Anything,
		).Return([]string{})
		meta.On("RenameCollection",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil)
		
		core := newTestCore(withValidProxyManager(), withMeta(meta))
		task := &renameCollectionTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &milvuspb.RenameCollectionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_RenameCollection,
				},
				OldName: oldName,
				NewName: newName,
			},
		}
		err := task.Execute(context.Background())
		assert.NoError(t, err)
	})
}

func Test_renameCollectionTask_GetLockerKey(t *testing.T) {
	oldName := funcutil.GenRandomStr()
	newName := funcutil.GenRandomStr()
	collectionID := int64(111)
	
	meta := mockrootcoord.NewIMetaTable(t)
	meta.On("GetCollectionID",
		mock.Anything,
		mock.Anything,
		mock.Anything,
	).Return(collectionID)
	
	core := newTestCore(withMeta(meta))
	task := &renameCollectionTask{
		baseTask: newBaseTask(context.Background(), core),
		Req: &milvuspb.RenameCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_RenameCollection,
			},
			DbName:  "db1",
			OldName: oldName,
			NewName: newName,
		},
	}
	
	key := task.GetLockerKey()
	assert.NotNil(t, key)
}
