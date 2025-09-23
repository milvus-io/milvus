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
				{Key: "cipher.enabled", Value: "true"},
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
			mock.AnythingOfType("uint64"),
		).Return(oldDB, nil)
		meta.On("GetDatabaseByName",
			mock.Anything,
			"db2",
			mock.AnythingOfType("uint64"),
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
		// First call Prepare to set default values
		err := task.Prepare(context.Background())
		assert.NoError(t, err)
		// Then call Execute where encryption validation happens
		err = task.Execute(context.Background())
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
		collectionID := int64(111)

		meta := mockrootcoord.NewIMetaTable(t)
		meta.On("GetDatabaseByName",
			mock.Anything,
			"db1",
			mock.AnythingOfType("uint64"),
		).Return(oldDB, nil)
		meta.On("GetDatabaseByName",
			mock.Anything,
			"db2",
			mock.AnythingOfType("uint64"),
		).Return(newDB, nil)
		// Mock additional methods called in Execute after encryption check
		meta.On("IsAlias",
			mock.Anything,
			"db1",
			"old_collection",
		).Return(false)
		meta.On("GetCollectionByName",
			mock.Anything,
			"db1",
			"old_collection",
			mock.AnythingOfType("uint64"),
		).Return(&model.Collection{
			CollectionID: collectionID,
			Name:         "old_collection",
		}, nil)
		meta.On("ListAliasesByID",
			mock.Anything,
			collectionID,
		).Return([]string{})
		meta.On("IsAlias",
			mock.Anything,
			"db2",
			"new_collection",
		).Return(false)
		meta.On("GetCollectionByName",
			mock.Anything,
			"db2",
			"new_collection",
			mock.AnythingOfType("uint64"),
		).Return(nil, errors.New("not found"))
		meta.On("RenameCollection",
			mock.Anything,
			"db1",
			"old_collection",
			"db2",
			"new_collection",
			mock.Anything,
		).Return(nil)

		core := newTestCore(withValidProxyManager(), withMeta(meta))
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
		// First call Prepare to set default values
		err := task.Prepare(context.Background())
		assert.NoError(t, err)
		// Then call Execute - should pass encryption check and proceed
		err = task.Execute(context.Background())
		assert.NoError(t, err)
	})
}

func Test_renameCollectionTask_Execute(t *testing.T) {
	t.Run("collection not found", func(t *testing.T) {
		oldName := funcutil.GenRandomStr()
		newName := funcutil.GenRandomStr()

		meta := mockrootcoord.NewIMetaTable(t)
		meta.On("IsAlias",
			mock.Anything,
			mock.Anything,
			oldName,
		).Return(false)
		meta.On("GetCollectionByName",
			mock.Anything,
			mock.Anything,
			oldName,
			mock.AnythingOfType("uint64"),
		).Return(nil, errors.New("collection not found"))

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
		// Call Prepare to set default values
		err := task.Prepare(context.Background())
		assert.NoError(t, err)
		err = task.Execute(context.Background())
		assert.Error(t, err)
	})

	t.Run("rename step failed", func(t *testing.T) {
		oldName := funcutil.GenRandomStr()
		newName := funcutil.GenRandomStr()
		collectionID := int64(111)

		meta := mockrootcoord.NewIMetaTable(t)
		meta.On("IsAlias",
			mock.Anything,
			mock.Anything,
			oldName,
		).Return(false)
		meta.On("GetCollectionByName",
			mock.Anything,
			mock.Anything,
			oldName,
			mock.AnythingOfType("uint64"),
		).Return(&model.Collection{
			CollectionID: collectionID,
			Name:         oldName,
		}, nil)
		meta.On("ListAliasesByID",
			mock.Anything,
			collectionID,
		).Return([]string{})
		meta.On("IsAlias",
			mock.Anything,
			mock.Anything,
			newName,
		).Return(false)
		meta.On("GetCollectionByName",
			mock.Anything,
			mock.Anything,
			newName,
			mock.AnythingOfType("uint64"),
		).Return(nil, errors.New("not found"))
		meta.On("RenameCollection",
			mock.Anything,
			mock.Anything,
			oldName,
			mock.Anything,
			newName,
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
		// Call Prepare to set default values
		err := task.Prepare(context.Background())
		assert.NoError(t, err)
		err = task.Execute(context.Background())
		assert.Error(t, err)
	})

	t.Run("expire cache failed", func(t *testing.T) {
		oldName := funcutil.GenRandomStr()
		newName := funcutil.GenRandomStr()
		collectionID := int64(111)

		meta := mockrootcoord.NewIMetaTable(t)
		meta.On("IsAlias",
			mock.Anything,
			mock.Anything,
			oldName,
		).Return(false)
		meta.On("GetCollectionByName",
			mock.Anything,
			mock.Anything,
			oldName,
			mock.AnythingOfType("uint64"),
		).Return(&model.Collection{
			CollectionID: collectionID,
			Name:         oldName,
		}, nil)
		meta.On("ListAliasesByID",
			mock.Anything,
			collectionID,
		).Return([]string{})
		meta.On("IsAlias",
			mock.Anything,
			mock.Anything,
			newName,
		).Return(false)
		meta.On("GetCollectionByName",
			mock.Anything,
			mock.Anything,
			newName,
			mock.AnythingOfType("uint64"),
		).Return(nil, errors.New("not found"))
		meta.On("RenameCollection",
			mock.Anything,
			mock.Anything,
			oldName,
			mock.Anything,
			newName,
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
		// Call Prepare to set default values
		err := task.Prepare(context.Background())
		assert.NoError(t, err)
		err = task.Execute(context.Background())
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
		meta.On("GetCollectionByName",
			mock.Anything,
			mock.Anything,
			oldName,
			mock.AnythingOfType("uint64"),
		).Return(&model.Collection{
			CollectionID: collectionID,
			Name:         oldName,
		}, nil)
		meta.On("IsAlias",
			mock.Anything,
			mock.Anything,
			newName,
		).Return(false)
		meta.On("GetCollectionByName",
			mock.Anything,
			mock.Anything,
			newName,
			mock.AnythingOfType("uint64"),
		).Return(nil, errors.New("not found"))
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
		// Call Prepare to set default values
		err := task.Prepare(context.Background())
		assert.NoError(t, err)
		err = task.Execute(context.Background())
		assert.NoError(t, err)
	})

	t.Run("rename with aliases across databases should fail", func(t *testing.T) {
		oldName := funcutil.GenRandomStr()
		newName := funcutil.GenRandomStr()
		collectionID := int64(111)
		aliases := []string{"alias1", "alias2"}

		oldDB := &model.Database{
			Name: "db1",
			ID:   1,
		}
		newDB := &model.Database{
			Name: "db2",
			ID:   2,
		}

		meta := mockrootcoord.NewIMetaTable(t)
		// Mock for encryption check
		meta.On("GetDatabaseByName",
			mock.Anything,
			"db1",
			mock.AnythingOfType("uint64"),
		).Return(oldDB, nil)
		meta.On("GetDatabaseByName",
			mock.Anything,
			"db2",
			mock.AnythingOfType("uint64"),
		).Return(newDB, nil)
		// Mock for collection checks
		meta.On("IsAlias",
			mock.Anything,
			"db1",
			oldName,
		).Return(false)
		meta.On("GetCollectionByName",
			mock.Anything,
			"db1",
			oldName,
			mock.AnythingOfType("uint64"),
		).Return(&model.Collection{
			CollectionID: collectionID,
			Name:         oldName,
		}, nil)
		meta.On("ListAliasesByID",
			mock.Anything,
			collectionID,
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
		// Call Prepare to set default values
		err := task.Prepare(context.Background())
		assert.NoError(t, err)
		err = task.Execute(context.Background())
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
		// Call Prepare to set default values
		err := task.Prepare(context.Background())
		assert.NoError(t, err)
		err = task.Execute(context.Background())
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
		meta.On("GetCollectionByName",
			mock.Anything,
			mock.Anything,
			oldName,
			mock.AnythingOfType("uint64"),
		).Return(&model.Collection{
			CollectionID: collectionID,
			Name:         oldName,
		}, nil)
		meta.On("ListAliasesByID",
			mock.Anything,
			collectionID,
		).Return([]string{})
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
		err := task.Prepare(context.Background())
		assert.NoError(t, err)
		err = task.Execute(context.Background())
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
		meta.On("GetCollectionByName",
			mock.Anything,
			mock.Anything,
			oldName,
			mock.AnythingOfType("uint64"),
		).Return(&model.Collection{
			CollectionID: collectionID,
			Name:         oldName,
		}, nil)
		meta.On("ListAliasesByID",
			mock.Anything,
			collectionID,
		).Return([]string{})
		meta.On("IsAlias",
			mock.Anything,
			mock.Anything,
			newName,
		).Return(false)
		meta.On("GetCollectionByName",
			mock.Anything,
			mock.Anything,
			newName,
			mock.AnythingOfType("uint64"),
		).Return(&model.Collection{
			CollectionID: existingCollID,
			Name:         newName,
		}, nil)

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
		err := task.Prepare(context.Background())
		assert.NoError(t, err)
		err = task.Execute(context.Background())
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
		// Mock for encryption check
		meta.On("GetDatabaseByName",
			mock.Anything,
			oldDB,
			mock.AnythingOfType("uint64"),
		).Return(&model.Database{
			ID:         1,
			Name:       oldDB,
			Properties: []*commonpb.KeyValuePair{},
		}, nil)
		meta.On("GetDatabaseByName",
			mock.Anything,
			newDB,
			mock.AnythingOfType("uint64"),
		).Return(&model.Database{
			ID:         2,
			Name:       newDB,
			Properties: []*commonpb.KeyValuePair{},
		}, nil)
		meta.On("IsAlias",
			mock.Anything,
			oldDB,
			oldName,
		).Return(false)
		meta.On("GetCollectionByName",
			mock.Anything,
			oldDB,
			oldName,
			mock.AnythingOfType("uint64"),
		).Return(&model.Collection{
			CollectionID: collectionID,
			Name:         oldName,
		}, nil)
		meta.On("IsAlias",
			mock.Anything,
			newDB,
			newName,
		).Return(false)
		meta.On("GetCollectionByName",
			mock.Anything,
			newDB,
			newName,
			mock.AnythingOfType("uint64"),
		).Return(nil, errors.New("not found"))
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
		meta.On("GetCollectionByName",
			mock.Anything,
			mock.Anything,
			oldName,
			mock.AnythingOfType("uint64"),
		).Return(&model.Collection{
			CollectionID: collectionID,
			Name:         oldName,
		}, nil)
		meta.On("IsAlias",
			mock.Anything,
			mock.Anything,
			newName,
		).Return(false)
		meta.On("GetCollectionByName",
			mock.Anything,
			mock.Anything,
			newName,
			mock.AnythingOfType("uint64"),
		).Return(nil, errors.New("not found"))
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
		err := task.Prepare(context.Background())
		assert.NoError(t, err)
		err = task.Execute(context.Background())
		assert.NoError(t, err)
	})
}

func Test_renameCollectionTask_GetLockerKey(t *testing.T) {
	oldName := funcutil.GenRandomStr()
	newName := funcutil.GenRandomStr()

	core := newTestCore()
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
