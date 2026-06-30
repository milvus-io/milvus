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
	"math/rand"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	mocktso "github.com/milvus-io/milvus/pkg/v3/coordmeta/rootcoord/mocktso"
	"github.com/milvus-io/milvus/pkg/v3/metastore"
	"github.com/milvus-io/milvus/pkg/v3/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v3/metastore/model"
	pb "github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestMetaTable_getCollectionByIDInternal(t *testing.T) {
	t.Run("failed to get from catalog", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("GetCollectionByID",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil, errors.New("error mock GetCollectionByID"))
		meta := &MetaTable{
			catalog: catalog,
			dbName2Meta: map[string]*model.Database{
				util.DefaultDBName: model.NewDefaultDatabase(nil),
			},
			collID2Meta: map[typeutil.UniqueID]*model.Collection{},
		}
		ctx := context.Background()
		_, err := meta.getCollectionByIDInternal(ctx, util.DefaultDBName, 100, 101, false)
		assert.Error(t, err)
	})

	t.Run("collection not available", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("GetCollectionByID",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(&model.Collection{State: pb.CollectionState_CollectionDropped}, nil)
		meta := &MetaTable{
			catalog: catalog,
			dbName2Meta: map[string]*model.Database{
				util.DefaultDBName: model.NewDefaultDatabase(nil),
			},
			collID2Meta: map[typeutil.UniqueID]*model.Collection{},
		}
		ctx := context.Background()
		_, err := meta.getCollectionByIDInternal(ctx, util.DefaultDBName, 100, 101, false)
		assert.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrCollectionNotFound)
		coll, err := meta.getCollectionByIDInternal(ctx, util.DefaultDBName, 100, 101, true)
		assert.NoError(t, err)
		assert.False(t, coll.Available())
	})

	t.Run("normal case, filter unavailable partitions", func(t *testing.T) {
		meta := &MetaTable{
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				100: {
					State:      pb.CollectionState_CollectionCreated,
					CreateTime: 99,
					Partitions: []*model.Partition{
						{PartitionID: 11, PartitionName: Params.CommonCfg.DefaultPartitionName.GetValue(), State: pb.PartitionState_PartitionCreated},
						{PartitionID: 22, PartitionName: "dropped", State: pb.PartitionState_PartitionDropped},
					},
				},
			},
		}
		ctx := context.Background()
		coll, err := meta.getCollectionByIDInternal(ctx, "", 100, 101, false)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(coll.Partitions))
		assert.Equal(t, UniqueID(11), coll.Partitions[0].PartitionID)
		assert.Equal(t, Params.CommonCfg.DefaultPartitionName.GetValue(), coll.Partitions[0].PartitionName)
	})

	t.Run("get latest version", func(t *testing.T) {
		meta := &MetaTable{
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				100: {
					State:      pb.CollectionState_CollectionCreated,
					CreateTime: 99,
					Partitions: []*model.Partition{
						{PartitionID: 11, PartitionName: Params.CommonCfg.DefaultPartitionName.GetValue(), State: pb.PartitionState_PartitionCreated},
						{PartitionID: 22, PartitionName: "dropped", State: pb.PartitionState_PartitionDropped},
					},
				},
			},
		}
		ctx := context.Background()
		coll, err := meta.getCollectionByIDInternal(ctx, "", 100, typeutil.MaxTimestamp, false)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(coll.Partitions))
		assert.Equal(t, UniqueID(11), coll.Partitions[0].PartitionID)
		assert.Equal(t, Params.CommonCfg.DefaultPartitionName.GetValue(), coll.Partitions[0].PartitionName)
	})

	t.Run("UpdateTimestamp > ts triggers catalog fallback (time-travel correctness)", func(t *testing.T) {
		// Regression test for the bug fix in getCollectionByIDInternal:
		// cache invalidation was changed from CreateTime to UpdateTimestamp.
		// Scenario: collection created at T=50, schema altered at T=100.
		// A time-travel query at ts=80 (50 < 80 < 100) must NOT use the in-memory
		// cache (which holds the post-alteration schema) — it must fall back to catalog.
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("GetCollectionByID",
			mock.Anything,
			mock.Anything,
			uint64(80),
			int64(100),
		).Return(&model.Collection{
			State:           pb.CollectionState_CollectionCreated,
			CreateTime:      50,
			UpdateTimestamp: 100,
			Partitions: []*model.Partition{
				{PartitionID: 11, PartitionName: Params.CommonCfg.DefaultPartitionName.GetValue(), State: pb.PartitionState_PartitionCreated},
			},
		}, nil)

		meta := &MetaTable{
			catalog: catalog,
			dbName2Meta: map[string]*model.Database{
				util.DefaultDBName: model.NewDefaultDatabase(nil),
			},
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				100: {
					State:           pb.CollectionState_CollectionCreated,
					CreateTime:      50,
					UpdateTimestamp: 100, // schema was altered at T=100
					Partitions: []*model.Partition{
						{PartitionID: 11, PartitionName: Params.CommonCfg.DefaultPartitionName.GetValue(), State: pb.PartitionState_PartitionCreated},
					},
				},
			},
		}
		ctx := context.Background()

		// ts=80 is between CreateTime(50) and UpdateTimestamp(100):
		// UpdateTimestamp(100) > ts(80) → cache bypass → catalog must be called.
		coll, err := meta.getCollectionByIDInternal(ctx, util.DefaultDBName, 100, 80, false)
		assert.NoError(t, err)
		assert.NotNil(t, coll)
		catalog.AssertCalled(t, "GetCollectionByID", mock.Anything, mock.Anything, uint64(80), int64(100))
	})

	t.Run("UpdateTimestamp <= ts uses in-memory cache (no catalog call)", func(t *testing.T) {
		// ts=150 >= UpdateTimestamp(100) → the in-memory cache is fresh enough → no catalog call.
		catalog := mocks.NewRootCoordCatalog(t)
		// No expectations set — testify mock will fail if GetCollectionByID is called.

		meta := &MetaTable{
			catalog: catalog,
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				100: {
					State:           pb.CollectionState_CollectionCreated,
					CreateTime:      50,
					UpdateTimestamp: 100,
					Partitions: []*model.Partition{
						{PartitionID: 11, PartitionName: Params.CommonCfg.DefaultPartitionName.GetValue(), State: pb.PartitionState_PartitionCreated},
					},
				},
			},
		}
		ctx := context.Background()

		coll, err := meta.getCollectionByIDInternal(ctx, "", 100, 150, false)
		assert.NoError(t, err)
		assert.NotNil(t, coll)
		catalog.AssertNotCalled(t, "GetCollectionByID")
	})
}

func TestMetaTable_GetCollectionByName(t *testing.T) {
	t.Run("db not found", func(t *testing.T) {
		meta := &MetaTable{
			aliases: newNameDb(),
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				100: {
					State:      pb.CollectionState_CollectionCreated,
					CreateTime: 99,
					Partitions: []*model.Partition{},
				},
			},
			dbName2Meta: map[string]*model.Database{
				util.DefaultDBName: model.NewDefaultDatabase(nil),
			},
		}
		ctx := context.Background()
		_, err := meta.GetCollectionByName(ctx, "not_exist", "name", 101, false)
		assert.Error(t, err)
	})
	t.Run("get by alias", func(t *testing.T) {
		meta := &MetaTable{
			aliases: newNameDb(),
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				100: {
					State:      pb.CollectionState_CollectionCreated,
					CreateTime: 99,
					Partitions: []*model.Partition{
						{PartitionID: 11, PartitionName: Params.CommonCfg.DefaultPartitionName.GetValue(), State: pb.PartitionState_PartitionCreated},
						{PartitionID: 22, PartitionName: "dropped", State: pb.PartitionState_PartitionDropped},
					},
				},
			},
			dbName2Meta: map[string]*model.Database{
				util.DefaultDBName: model.NewDefaultDatabase(nil),
			},
		}
		meta.aliases.insert(util.DefaultDBName, "alias", 100)
		ctx := context.Background()
		coll, err := meta.GetCollectionByName(ctx, "", "alias", 101, false)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(coll.Partitions))
		assert.Equal(t, UniqueID(11), coll.Partitions[0].PartitionID)
		assert.Equal(t, Params.CommonCfg.DefaultPartitionName.GetValue(), coll.Partitions[0].PartitionName)
	})

	t.Run("get by name", func(t *testing.T) {
		meta := &MetaTable{
			names:   newNameDb(),
			aliases: newNameDb(),
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				100: {
					State:      pb.CollectionState_CollectionCreated,
					CreateTime: 99,
					Partitions: []*model.Partition{
						{PartitionID: 11, PartitionName: Params.CommonCfg.DefaultPartitionName.GetValue(), State: pb.PartitionState_PartitionCreated},
						{PartitionID: 22, PartitionName: "dropped", State: pb.PartitionState_PartitionDropped},
					},
				},
			},
			dbName2Meta: map[string]*model.Database{
				util.DefaultDBName: model.NewDefaultDatabase(nil),
			},
		}
		meta.names.insert(util.DefaultDBName, "name", 100)
		ctx := context.Background()
		coll, err := meta.GetCollectionByName(ctx, "", "name", 101, false)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(coll.Partitions))
		assert.Equal(t, UniqueID(11), coll.Partitions[0].PartitionID)
		assert.Equal(t, Params.CommonCfg.DefaultPartitionName.GetValue(), coll.Partitions[0].PartitionName)
	})

	t.Run("failed to get from catalog", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("GetCollectionByName",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil, errors.New("error mock GetCollectionByName"))
		meta := &MetaTable{
			dbName2Meta: map[string]*model.Database{
				util.DefaultDBName: model.NewDefaultDatabase(nil),
			},
			names:   newNameDb(),
			aliases: newNameDb(),
			catalog: catalog,
		}
		ctx := context.Background()
		_, err := meta.GetCollectionByName(ctx, util.DefaultDBName, "name", 101, false)
		assert.Error(t, err)
	})

	t.Run("collection not available", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("GetCollectionByName",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(&model.Collection{State: pb.CollectionState_CollectionDropped}, nil)
		meta := &MetaTable{
			dbName2Meta: map[string]*model.Database{
				util.DefaultDBName: model.NewDefaultDatabase(nil),
			},
			names:   newNameDb(),
			aliases: newNameDb(),
			catalog: catalog,
		}
		ctx := context.Background()
		_, err := meta.GetCollectionByName(ctx, util.DefaultDBName, "name", 101, false)
		assert.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrCollectionNotFound)
	})

	t.Run("normal case, filter unavailable partitions", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("GetCollectionByName",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(&model.Collection{
			State:      pb.CollectionState_CollectionCreated,
			CreateTime: 99,
			Partitions: []*model.Partition{
				{PartitionID: 11, PartitionName: Params.CommonCfg.DefaultPartitionName.GetValue(), State: pb.PartitionState_PartitionCreated},
				{PartitionID: 22, PartitionName: "dropped", State: pb.PartitionState_PartitionDropped},
			},
		}, nil)

		meta := &MetaTable{
			dbName2Meta: map[string]*model.Database{
				util.DefaultDBName: model.NewDefaultDatabase(nil),
			},
			names:   newNameDb(),
			aliases: newNameDb(),
			catalog: catalog,
		}
		ctx := context.Background()
		coll, err := meta.GetCollectionByName(ctx, util.DefaultDBName, "name", 101, false)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(coll.Partitions))
		assert.Equal(t, UniqueID(11), coll.Partitions[0].PartitionID)
		assert.Equal(t, Params.CommonCfg.DefaultPartitionName.GetValue(), coll.Partitions[0].PartitionName)
	})

	t.Run("get latest version", func(t *testing.T) {
		ctx := context.Background()
		meta := &MetaTable{
			dbName2Meta: map[string]*model.Database{
				util.DefaultDBName: model.NewDefaultDatabase(nil),
			},
			names:   newNameDb(),
			aliases: newNameDb(),
		}
		_, err := meta.GetCollectionByName(ctx, "", "not_exist", typeutil.MaxTimestamp, false)
		assert.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrCollectionNotFound)
	})
}

/*
func TestMetaTable_AlterCollection(t *testing.T) {
	t.Run("alter metastore fail", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("AlterCollection",
			mock.Anything, // context.Context
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(errors.New("error"))
		meta := &MetaTable{
			catalog:     catalog,
			collID2Meta: map[typeutil.UniqueID]*model.Collection{},
		}
		ctx := context.Background()
		err := meta.AlterCollection(ctx, nil, nil, 0, false, false)
		assert.Error(t, err)
	})

	t.Run("new name is already an alias", func(t *testing.T) {
		meta := &MetaTable{
			dbName2Meta: map[string]*model.Database{
				util.DefaultDBName: model.NewDefaultDatabase(nil),
			},
			names:   newNameDb(),
			aliases: newNameDb(),
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				1: {
					CollectionID: 1,
					Name:         "old",
					DBID:         util.DefaultDBID,
					State:        pb.CollectionState_CollectionCreated,
				},
			},
		}
		meta.names.insert(util.DefaultDBName, "old", 1)
		meta.aliases.insert(util.DefaultDBName, "new", 1)

		err := meta.RenameCollection(context.TODO(), util.DefaultDBName, "old", util.DefaultDBName, "new", typeutil.MaxTimestamp)
		assert.Error(t, err)
	})

	t.Run("alter collection ok", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("AlterCollection",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil)
		meta := &MetaTable{
			catalog:     catalog,
			collID2Meta: map[typeutil.UniqueID]*model.Collection{},
		}
		ctx := context.Background()

		oldColl := &model.Collection{CollectionID: 1}
		newColl := &model.Collection{CollectionID: 1}
		err := meta.AlterCollection(ctx, oldColl, newColl, 0, false, false)
		assert.NoError(t, err)
		assert.Equal(t, meta.collID2Meta[1], newColl)
	})
}
*/

func TestMetaTable_DescribeAlias(t *testing.T) {
	t.Run("metatable describe alias ok", func(t *testing.T) {
		var collectionID int64 = 100
		collectionName := "test_metatable_describe_alias"
		aliasName := "a_alias"
		meta := &MetaTable{
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				collectionID: {
					CollectionID: collectionID,
					Name:         collectionName,
				},
			},
			names:   newNameDb(),
			aliases: newNameDb(),
		}
		meta.names.insert("", collectionName, collectionID)
		meta.aliases.insert("", aliasName, collectionID)

		ctx := context.Background()
		descCollectionName, err := meta.DescribeAlias(ctx, "", aliasName, 0)
		assert.NoError(t, err)
		assert.Equal(t, collectionName, descCollectionName)
	})

	t.Run("metatable describe not exist alias", func(t *testing.T) {
		var collectionID int64 = 100
		aliasName1 := "a_alias"
		aliasName2 := "a_alias2"
		meta := &MetaTable{
			names:   newNameDb(),
			aliases: newNameDb(),
		}
		meta.aliases.insert("", aliasName1, collectionID)
		ctx := context.Background()
		descCollectionName, err := meta.DescribeAlias(ctx, "", aliasName2, 0)
		assert.Error(t, err)
		assert.Equal(t, "", descCollectionName)
	})

	t.Run("metatable describe not exist database", func(t *testing.T) {
		aliasName := "a_alias"
		meta := &MetaTable{
			names:   newNameDb(),
			aliases: newNameDb(),
		}
		ctx := context.Background()
		descCollectionName, err := meta.DescribeAlias(ctx, "", aliasName, 0)
		assert.Error(t, err)
		assert.Equal(t, "", descCollectionName)
	})

	t.Run("metatable describe alias fail", func(t *testing.T) {
		var collectionID int64 = 100
		collectionName := "test_metatable_describe_alias"
		aliasName := "a_alias"
		meta := &MetaTable{
			names:   newNameDb(),
			aliases: newNameDb(),
		}
		meta.names.insert("", collectionName, collectionID)
		meta.aliases.insert("", aliasName, collectionID)
		ctx := context.Background()
		_, err := meta.DescribeAlias(ctx, "", aliasName, 0)
		assert.Error(t, err)
	})

	t.Run("metatable describe alias dropped collection", func(t *testing.T) {
		var collectionID int64 = 100
		collectionName := "test_metatable_describe_alias"
		aliasName := "a_alias"
		meta := &MetaTable{
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				collectionID: {
					CollectionID: collectionID,
					Name:         collectionName,
				},
			},
			names:   newNameDb(),
			aliases: newNameDb(),
		}
		meta.names.insert("", collectionName, collectionID)
		meta.aliases.insert("", aliasName, collectionID)

		ctx := context.Background()
		meta.collID2Meta[collectionID] = &model.Collection{State: pb.CollectionState_CollectionDropped}
		alias, err := meta.DescribeAlias(ctx, "", aliasName, 0)
		assert.Equal(t, "", alias)
		assert.Error(t, err)
	})
}

func TestMetaTable_ListAliases(t *testing.T) {
	t.Run("metatable list alias ok", func(t *testing.T) {
		var collectionID1 int64 = 101
		collectionName1 := "test_metatable_list_alias1"
		aliasName1 := "a_alias"
		var collectionID2 int64 = 102
		collectionName2 := "test_metatable_list_alias2"
		aliasName2 := "a_alias2"
		var collectionID3 int64 = 103
		collectionName3 := "test_metatable_list_alias3"
		aliasName3 := "a_alias3"
		aliasName4 := "a_alias4"
		meta := &MetaTable{
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				collectionID1: {
					CollectionID: collectionID1,
					Name:         collectionName1,
				},
				collectionID1: {
					CollectionID: collectionID2,
					Name:         collectionName2,
				},
			},
			names:   newNameDb(),
			aliases: newNameDb(),
		}
		meta.names.insert("", collectionName1, collectionID1)
		meta.names.insert("", collectionName2, collectionID2)
		meta.names.insert("db2", collectionName3, collectionID3)

		meta.aliases.insert("", aliasName1, collectionID1)
		meta.aliases.insert("", aliasName2, collectionID2)
		meta.aliases.insert("db2", aliasName3, collectionID3)
		meta.aliases.insert("db2", aliasName4, collectionID3)

		meta.collID2Meta[collectionID1] = &model.Collection{State: pb.CollectionState_CollectionCreated}
		meta.collID2Meta[collectionID2] = &model.Collection{State: pb.CollectionState_CollectionCreated}
		meta.collID2Meta[collectionID3] = &model.Collection{State: pb.CollectionState_CollectionCreated}

		ctx := context.Background()
		aliases, err := meta.ListAliases(ctx, "", "", 0)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(aliases))

		aliases2, err := meta.ListAliases(ctx, "", collectionName1, 0)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(aliases2))

		aliases3, err := meta.ListAliases(ctx, "db2", "", 0)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(aliases3))

		aliases4, err := meta.ListAliases(ctx, "db2", collectionName3, 0)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(aliases4))
	})

	t.Run("metatable list alias in not exist database", func(t *testing.T) {
		aliasName := "a_alias"
		meta := &MetaTable{
			names:   newNameDb(),
			aliases: newNameDb(),
		}
		ctx := context.Background()
		aliases, err := meta.ListAliases(ctx, "", aliasName, 0)
		assert.Error(t, err)
		assert.Equal(t, 0, len(aliases))
	})

	t.Run("metatable list alias error", func(t *testing.T) {
		var collectionID1 int64 = 101
		collectionName1 := "test_metatable_list_alias1"
		aliasName1 := "a_alias"
		var collectionID2 int64 = 102
		collectionName2 := "test_metatable_list_alias2"
		aliasName2 := "a_alias2"
		meta := &MetaTable{
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				collectionID1: {
					CollectionID: collectionID1,
					Name:         collectionName1,
				},
				collectionID1: {
					CollectionID: collectionID2,
					Name:         collectionName2,
				},
			},
			names:   newNameDb(),
			aliases: newNameDb(),
		}
		meta.aliases.insert("", aliasName1, collectionID1)
		meta.aliases.insert("", aliasName2, collectionID2)
		ctx := context.Background()
		_, err := meta.ListAliases(ctx, "", collectionName1, 0)
		assert.Error(t, err)
	})

	t.Run("metatable list alias Dropping collection", func(t *testing.T) {
		ctx := context.Background()

		var collectionID1 int64 = 101
		collectionName1 := "test_metatable_list_alias1"
		aliasName1 := "a_alias"
		var collectionID2 int64 = 102
		collectionName2 := "test_metatable_list_alias2"
		aliasName2 := "a_alias2"
		meta := &MetaTable{
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				collectionID1: {
					CollectionID: collectionID1,
					Name:         collectionName1,
				},
				collectionID1: {
					CollectionID: collectionID2,
					Name:         collectionName2,
				},
			},
			names:   newNameDb(),
			aliases: newNameDb(),
		}
		meta.names.insert("", collectionName1, collectionID1)
		meta.names.insert("", collectionName2, collectionID2)
		meta.aliases.insert("", aliasName1, collectionID1)
		meta.aliases.insert("", aliasName2, collectionID2)
		meta.collID2Meta[collectionID1] = &model.Collection{State: pb.CollectionState_CollectionCreated}
		meta.collID2Meta[collectionID2] = &model.Collection{State: pb.CollectionState_CollectionDropped}

		aliases, err := meta.ListAliases(ctx, "", "", 0)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(aliases))

		aliases2, err := meta.ListAliases(ctx, "", collectionName1, 0)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(aliases2))
	})
}

func Test_filterUnavailable(t *testing.T) {
	coll := &model.Collection{}
	nPartition := 10
	nAvailablePartition := 0
	for i := 0; i < nPartition; i++ {
		partition := &model.Partition{
			State: pb.PartitionState_PartitionDropping,
		}
		if rand.Int()%2 == 0 {
			partition.State = pb.PartitionState_PartitionCreated
			nAvailablePartition++
		}
		coll.Partitions = append(coll.Partitions, partition)
	}
	clone := filterUnavailablePartition(coll)
	assert.Equal(t, nAvailablePartition, len(clone.Partitions))
	for _, p := range clone.Partitions {
		assert.True(t, p.Available())
	}
}

func TestMetaTable_getLatestCollectionByIDInternal(t *testing.T) {
	t.Run("not exist", func(t *testing.T) {
		ctx := context.Background()
		mt := &MetaTable{collID2Meta: nil}
		_, err := mt.getLatestCollectionByIDInternal(ctx, 100, false)
		assert.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrCollectionNotFound)
	})

	t.Run("nil case", func(t *testing.T) {
		ctx := context.Background()
		mt := &MetaTable{collID2Meta: map[typeutil.UniqueID]*model.Collection{
			100: nil,
		}}
		_, err := mt.getLatestCollectionByIDInternal(ctx, 100, false)
		assert.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrCollectionNotFound)
	})

	t.Run("unavailable", func(t *testing.T) {
		ctx := context.Background()
		mt := &MetaTable{collID2Meta: map[typeutil.UniqueID]*model.Collection{
			100: {State: pb.CollectionState_CollectionDropping},
		}}
		_, err := mt.getLatestCollectionByIDInternal(ctx, 100, false)
		assert.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrCollectionNotFound)
		coll, err := mt.getLatestCollectionByIDInternal(ctx, 100, true)
		assert.NoError(t, err)
		assert.False(t, coll.Available())
	})

	t.Run("normal case", func(t *testing.T) {
		ctx := context.Background()
		mt := &MetaTable{collID2Meta: map[typeutil.UniqueID]*model.Collection{
			100: {
				State: pb.CollectionState_CollectionCreated,
				Partitions: []*model.Partition{
					{State: pb.PartitionState_PartitionCreated},
					{State: pb.PartitionState_PartitionDropping},
				},
			},
		}}
		coll, err := mt.getLatestCollectionByIDInternal(ctx, 100, false)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(coll.Partitions))
	})
}

func TestMetaTable_RemoveCollection(t *testing.T) {
	t.Run("catalog error", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("DropCollection",
			mock.Anything, // context.Context
			mock.Anything, // model.Collection
			mock.AnythingOfType("uint64"),
		).Return(errors.New("error mock DropCollection"))

		meta := &MetaTable{
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				100: {
					CollectionID: 100,
					DBID:         int64(100),
				},
			},
			names:   newNameDb(),
			aliases: newNameDb(),
			catalog: catalog,
		}

		ctx := context.Background()
		err := meta.RemoveCollection(ctx, 100, 9999)
		assert.Error(t, err)

		meta.collID2Meta[100].State = pb.CollectionState_CollectionDropping
		err = meta.RemoveCollection(ctx, 100, 9999)
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("DropCollection",
			mock.Anything, // context.Context
			mock.Anything, // model.Collection
			mock.AnythingOfType("uint64"),
		).Return(nil)
		catalog.On("DeleteGrantByCollectionName",
			mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		).Return(nil)
		meta := &MetaTable{
			catalog: catalog,
			names:   newNameDb(),
			aliases: newNameDb(),
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				100: {Name: "collection", State: pb.CollectionState_CollectionDropping},
			},
		}
		meta.names.insert("", "collection", 100)
		meta.names.insert("", "alias1", 100)
		meta.names.insert("", "alias2", 100)
		ctx := context.Background()
		err := meta.RemoveCollection(ctx, 100, 9999)
		assert.NoError(t, err)
	})
}

func TestMetaTable_RemoveCollection_GrantDeleteBestEffort(t *testing.T) {
	// When DeleteGrantByCollectionName fails, RemoveCollection should still succeed (best-effort)
	catalog := mocks.NewRootCoordCatalog(t)
	catalog.On("DropCollection",
		mock.Anything,
		mock.Anything,
		mock.AnythingOfType("uint64"),
	).Return(nil)
	catalog.On("DeleteGrantByCollectionName",
		mock.Anything, mock.Anything, mock.Anything, mock.Anything,
	).Return(errors.New("grant delete failed"))

	meta := &MetaTable{
		catalog:            catalog,
		names:              newNameDb(),
		aliases:            newNameDb(),
		fileResourceRefCnt: make(map[int64]int),
		collID2Meta: map[typeutil.UniqueID]*model.Collection{
			100: {Name: "collection", State: pb.CollectionState_CollectionDropping},
		},
	}
	meta.names.insert("", "collection", 100)
	ctx := context.Background()
	err := meta.RemoveCollection(ctx, 100, 9999)
	assert.NoError(t, err)
}

func TestMetaTable_DropCollection_GrantCleanup(t *testing.T) {
	t.Run("grant cleanup on drop", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("AlterCollection",
			mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		).Return(nil)
		catalog.On("DeleteGrantByCollectionName",
			mock.Anything, mock.Anything, "testdb", "collection",
		).Return(nil)

		meta := &MetaTable{
			catalog: catalog,
			names:   newNameDb(),
			aliases: newNameDb(),
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				100: {Name: "collection", DBID: 1, State: pb.CollectionState_CollectionCreated},
			},
			dbName2Meta: map[string]*model.Database{
				"testdb": {ID: 1, Name: "testdb"},
			},
			fileResourceRefCnt: make(map[int64]int),
		}
		meta.names.insert("testdb", "collection", 100)
		ctx := context.Background()
		err := meta.DropCollection(ctx, 100, 9999)
		assert.NoError(t, err)
		catalog.AssertCalled(t, "DeleteGrantByCollectionName", mock.Anything, mock.Anything, "testdb", "collection")
	})

	t.Run("grant cleanup best-effort on drop", func(t *testing.T) {
		// When DeleteGrantByCollectionName fails, DropCollection should still succeed
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("AlterCollection",
			mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		).Return(nil)
		catalog.On("DeleteGrantByCollectionName",
			mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		).Return(errors.New("grant delete failed"))

		meta := &MetaTable{
			catalog: catalog,
			names:   newNameDb(),
			aliases: newNameDb(),
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				100: {Name: "collection", DBID: 1, State: pb.CollectionState_CollectionCreated},
			},
			dbName2Meta: map[string]*model.Database{
				"default": {ID: 1, Name: "default"},
			},
			fileResourceRefCnt: make(map[int64]int),
		}
		meta.names.insert("default", "collection", 100)
		ctx := context.Background()
		err := meta.DropCollection(ctx, 100, 9999)
		assert.NoError(t, err)
	})
}

func TestMetaTable_DropPartition_CopyOnWrite(t *testing.T) {
	catalog := mocks.NewRootCoordCatalog(t)
	originalPart := &model.Partition{
		PartitionID:   100,
		PartitionName: "p1",
		CollectionID:  100,
		State:         pb.PartitionState_PartitionCreated,
	}
	catalog.On("AlterPartition",
		mock.Anything,
		int64(10),
		originalPart,
		mock.MatchedBy(func(newPart *model.Partition) bool {
			return newPart != nil &&
				newPart != originalPart &&
				newPart.PartitionID == originalPart.PartitionID &&
				newPart.PartitionName == originalPart.PartitionName &&
				newPart.CollectionID == originalPart.CollectionID &&
				newPart.State == pb.PartitionState_PartitionDropping
		}),
		metastore.MODIFY,
		uint64(9999),
	).Return(nil).Once()

	meta := &MetaTable{
		catalog: catalog,
		collID2Meta: map[typeutil.UniqueID]*model.Collection{
			100: {
				CollectionID: 100,
				DBID:         10,
				State:        pb.CollectionState_CollectionCreated,
				Partitions:   []*model.Partition{originalPart},
			},
		},
		partitionName2ID: map[int64]map[string]int64{
			100: {"p1": 100},
		},
	}

	snapshot, err := meta.GetCollectionByID(context.Background(), "", 100, typeutil.MaxTimestamp, true)
	require.NoError(t, err)
	require.Same(t, originalPart, snapshot.Partitions[0])

	err = meta.DropPartition(context.Background(), 100, 100, 9999)
	require.NoError(t, err)

	require.Same(t, originalPart, snapshot.Partitions[0])
	assert.Equal(t, pb.PartitionState_PartitionCreated, snapshot.Partitions[0].State)

	require.Len(t, meta.collID2Meta[100].Partitions, 1)
	assert.NotSame(t, originalPart, meta.collID2Meta[100].Partitions[0])
	assert.Equal(t, pb.PartitionState_PartitionDropping, meta.collID2Meta[100].Partitions[0].State)
	assert.Equal(t, pb.PartitionState_PartitionCreated, originalPart.State)
	assert.NotContains(t, meta.partitionName2ID[100], "p1")
}

func TestMetaTable_RemovePartition(t *testing.T) {
	t.Run("catalog error", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("DropPartition",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.AnythingOfType("uint64"),
		).Return(errors.New("error mock AlterPartition"))

		meta := &MetaTable{
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				100: {
					CollectionID: 100,
					DBID:         int64(100),
					Partitions: []*model.Partition{
						{PartitionID: 100, State: pb.PartitionState_PartitionCreated},
					},
				},
			},
			names:   newNameDb(),
			aliases: newNameDb(),
			catalog: catalog,
		}

		ctx := context.Background()
		err := meta.RemovePartition(ctx, 100, 100, 9999)
		assert.Error(t, err)

		meta.collID2Meta[100].Partitions[0].State = pb.PartitionState_PartitionDropping
		err = meta.RemovePartition(ctx, 100, 100, 9999)
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("DropPartition",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.AnythingOfType("uint64"),
		).Return(nil)
		meta := &MetaTable{
			catalog: catalog,
			names:   newNameDb(),
			aliases: newNameDb(),
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				100: {
					Name: "collection",
					Partitions: []*model.Partition{
						{PartitionID: 100, State: pb.PartitionState_PartitionDropping},
					},
				},
			},
		}
		meta.names.insert("", "collection", 100)
		meta.names.insert("", "alias1", 100)
		meta.names.insert("", "alias2", 100)
		ctx := context.Background()
		err := meta.RemovePartition(ctx, 100, 100, 9999)
		assert.NoError(t, err)
	})
}

func TestMetaTable_reload(t *testing.T) {
	createMetaTableFn := func(catalogOpts ...func(*mocks.RootCoordCatalog)) *MetaTable {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("ListDatabases",
			mock.Anything,
			mock.Anything,
		).Return(make([]*model.Database, 0), nil)
		catalog.On("CreateDatabase",
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil)

		tso := mocktso.NewAllocator(t)
		tso.On("GenerateTSO",
			mock.Anything,
		).Return(uint64(1), nil)

		for _, opt := range catalogOpts {
			opt(catalog)
		}
		return &MetaTable{
			names:        newNameDb(),
			aliases:      newNameDb(),
			catalog:      catalog,
			tsoAllocator: tso,
		}
	}

	t.Run("list db fail", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("ListDatabases",
			mock.Anything,
			mock.Anything,
		).Return(nil, errors.New("error mock ListDatabases"))

		meta := &MetaTable{catalog: catalog}
		err := meta.reload()
		assert.Error(t, err)
		assert.Empty(t, meta.collID2Meta)

		assert.Equal(t, 0, len(meta.names.listDB()))
		assert.Equal(t, 0, len(meta.aliases.listDB()))
	})

	t.Run("failed to list collections", func(t *testing.T) {
		meta := createMetaTableFn(func(catalog *mocks.RootCoordCatalog) {
			catalog.On("ListCollections",
				mock.Anything,
				mock.Anything,
				mock.Anything,
			).Return(nil, errors.New("error mock ListCollections"))
		})

		err := meta.reload()
		assert.Error(t, err)
		assert.Empty(t, meta.collID2Meta)
	})

	t.Run("failed to list aliases", func(t *testing.T) {
		meta := createMetaTableFn(
			func(catalog *mocks.RootCoordCatalog) {
				catalog.On("ListCollections",
					mock.Anything,
					mock.Anything,
					mock.Anything,
				).Return(
					[]*model.Collection{{CollectionID: 100, Name: "test"}},
					nil)
			},
			func(catalog *mocks.RootCoordCatalog) {
				catalog.On("ListAliases",
					mock.Anything,
					mock.Anything,
					mock.Anything,
				).Return(nil, errors.New("error mock ListAliases"))
			},
		)

		err := meta.reload()
		assert.Error(t, err)
	})

	t.Run("no collections and default db doesn't exists", func(t *testing.T) {
		meta := createMetaTableFn(
			func(catalog *mocks.RootCoordCatalog) {
				catalog.On("ListCollections",
					mock.Anything,
					mock.Anything,
					mock.Anything,
				).Return(make([]*model.Collection, 0), nil)
			},
			func(catalog *mocks.RootCoordCatalog) {
				catalog.On("ListAliases",
					mock.Anything,
					mock.Anything,
					mock.Anything,
				).Return(
					[]*model.Alias{},
					nil)
			},
			func(catalog *mocks.RootCoordCatalog) {
				catalog.On("ListFileResource",
					mock.Anything,
				).Return(nil, uint64(0), nil)
			},
		)
		err := meta.reload()
		assert.NoError(t, err)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(meta.collID2Meta))

		assert.Equal(t, 1, len(meta.names.listDB()))
		assert.True(t, meta.names.exist("default"))
		assert.Equal(t, 1, len(meta.aliases.listDB()))
		assert.True(t, meta.aliases.exist("default"))
	})

	t.Run("no collections and default db already exists", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("ListDatabases",
			mock.Anything,
			mock.Anything,
		).Return([]*model.Database{model.NewDefaultDatabase(nil)}, nil)
		catalog.On("ListCollections",
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(
			[]*model.Collection{{CollectionID: 100, Name: "test", State: pb.CollectionState_CollectionCreated}},
			nil)
		catalog.On("ListAliases",
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(
			[]*model.Alias{{Name: "alias", CollectionID: 100}},
			nil)
		catalog.On("ListFileResource",
			mock.Anything,
		).Return(nil, uint64(0), nil)

		meta := &MetaTable{catalog: catalog}
		err := meta.reload()
		assert.NoError(t, err)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(meta.collID2Meta))

		assert.Equal(t, 1, len(meta.names.listDB()))
		assert.True(t, meta.names.exist(util.DefaultDBName))
		assert.Equal(t, 1, len(meta.aliases.listDB()))
		assert.True(t, meta.aliases.exist(util.DefaultDBName))
	})

	t.Run("ok", func(t *testing.T) {
		meta := createMetaTableFn(
			func(catalog *mocks.RootCoordCatalog) {
				catalog.On("ListCollections",
					mock.Anything,
					mock.Anything,
					mock.Anything,
				).Return(
					[]*model.Collection{{CollectionID: 100, Name: "test", State: pb.CollectionState_CollectionCreated}},
					nil)
			},
			func(catalog *mocks.RootCoordCatalog) {
				catalog.On("ListAliases",
					mock.Anything,
					mock.Anything,
					mock.Anything,
				).Return(
					[]*model.Alias{{Name: "alias", CollectionID: 100}},
					nil)
			},
			func(catalog *mocks.RootCoordCatalog) {
				catalog.On("ListFileResource",
					mock.Anything,
				).Return(nil, uint64(0), nil)
			},
		)

		err := meta.reload()
		assert.NoError(t, err)
		assert.Equal(t, 1, len(meta.collID2Meta))

		assert.Equal(t, 1, len(meta.names.listDB()))
		assert.True(t, meta.names.exist(util.DefaultDBName))
		assert.Equal(t, 1, len(meta.aliases.listDB()))
		assert.True(t, meta.aliases.exist(util.DefaultDBName))

		colls, err := meta.names.listCollectionID(util.DefaultDBName)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(colls))
		assert.Equal(t, int64(100), colls[0])

		colls, err = meta.aliases.listCollectionID(util.DefaultDBName)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(colls))
		assert.Equal(t, int64(100), colls[0])
	})
}

func TestMetaTable_ListAllAvailCollections(t *testing.T) {
	meta := &MetaTable{
		dbName2Meta: map[string]*model.Database{
			util.DefaultDBName: {
				ID: util.DefaultDBID,
			},
			"db2": {
				ID: 11,
			},
			"db3": {
				ID: 2,
			},
			"db4": {
				ID: 1111,
			},
		},
		collID2Meta: map[typeutil.UniqueID]*model.Collection{
			111: {
				CollectionID: 111,
				DBID:         1111,
				State:        pb.CollectionState_CollectionDropped,
			},
			2: {
				CollectionID: 2,
				DBID:         11,
				State:        pb.CollectionState_CollectionCreated,
			},
			3: {
				CollectionID: 3,
				DBID:         11,
				State:        pb.CollectionState_CollectionCreated,
			},
			4: {
				CollectionID: 4,
				DBID:         2,
				State:        pb.CollectionState_CollectionCreated,
			},
			5: {
				CollectionID: 5,
				DBID:         util.NonDBID,
				State:        pb.CollectionState_CollectionCreated,
			},
		},
	}

	ret := meta.ListAllAvailCollections(context.TODO())
	assert.Equal(t, 4, len(ret))
	db0, ok := ret[util.DefaultDBID]
	assert.True(t, ok)
	assert.Equal(t, 1, len(db0))
	db1, ok := ret[11]
	assert.True(t, ok)
	assert.Equal(t, 2, len(db1))
	db2, ok2 := ret[2]
	assert.True(t, ok2)
	assert.Equal(t, 1, len(db2))
	db3, ok := ret[1111]
	assert.True(t, ok)
	assert.Equal(t, 0, len(db3))
}

func TestMetaTable_AddPartition(t *testing.T) {
	t.Run("collection not available", func(t *testing.T) {
		meta := &MetaTable{}
		err := meta.AddPartition(context.TODO(), &model.Partition{CollectionID: 100})
		assert.Error(t, err)
	})

	t.Run("add not-created partition", func(t *testing.T) {
		meta := &MetaTable{
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				100: {
					Name:         "test",
					CollectionID: 100,
				},
			},
		}
		err := meta.AddPartition(context.TODO(), &model.Partition{CollectionID: 100, State: pb.PartitionState_PartitionDropping})
		assert.Error(t, err)
	})

	t.Run("failed to create partition", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("CreatePartition",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(errors.New("error mock CreatePartition"))
		meta := &MetaTable{
			catalog: catalog,
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				100: {Name: "test", CollectionID: 100},
			},
		}
		err := meta.AddPartition(context.TODO(), &model.Partition{CollectionID: 100, State: pb.PartitionState_PartitionCreated})
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("CreatePartition",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil)
		meta := &MetaTable{
			catalog: catalog,
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				100: {Name: "test", CollectionID: 100},
			},
			partitionName2ID: make(map[int64]map[string]int64),
		}
		err := meta.AddPartition(context.TODO(), &model.Partition{CollectionID: 100, State: pb.PartitionState_PartitionCreated})
		assert.NoError(t, err)
	})
}

/*
func TestMetaTable_RenameCollection(t *testing.T) {
	t.Run("unsupported use a alias to rename collection", func(t *testing.T) {
		meta := &MetaTable{
			names:   newNameDb(),
			aliases: newNameDb(),
		}
		meta.names.insert("", "alias", 1)
		err := meta.RenameCollection(context.TODO(), "", "alias", "", "new", typeutil.MaxTimestamp)
		assert.Error(t, err)
	})

	t.Run("target db doesn't exist", func(t *testing.T) {
		meta := &MetaTable{
			names:   newNameDb(),
			aliases: newNameDb(),
		}
		err := meta.RenameCollection(context.TODO(), "", "non-exists", "non-exists", "new", typeutil.MaxTimestamp)
		assert.Error(t, err)
	})

	t.Run("collection name not exist", func(t *testing.T) {
		meta := &MetaTable{
			names:   newNameDb(),
			aliases: newNameDb(),
		}
		err := meta.RenameCollection(context.TODO(), "", "non-exists", "", "new", typeutil.MaxTimestamp)
		assert.Error(t, err)
	})

	t.Run("collection id not exist", func(t *testing.T) {
		meta := &MetaTable{
			names:   newNameDb(),
			aliases: newNameDb(),
		}
		meta.names.insert("", "old", 1)
		err := meta.RenameCollection(context.TODO(), "", "old", "", "new", typeutil.MaxTimestamp)
		assert.Error(t, err)
	})

	t.Run("new collection name already exist-1", func(t *testing.T) {
		meta := &MetaTable{
			dbName2Meta: map[string]*model.Database{
				util.DefaultDBName: model.NewDefaultDatabase(nil),
			},
			names:   newNameDb(),
			aliases: newNameDb(),
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				util.DefaultDBID: {
					CollectionID: 1,
					Name:         "old",
					State:        pb.CollectionState_CollectionCreated,
				},
			},
		}
		meta.names.insert(util.DefaultDBName, "old", 1)
		err := meta.RenameCollection(context.TODO(), util.DefaultDBName, "old", util.DefaultDBName, "old", 1000)
		assert.Error(t, err)
	})

	t.Run("new collection name already exist-2", func(t *testing.T) {
		meta := &MetaTable{
			dbName2Meta: map[string]*model.Database{
				util.DefaultDBName: model.NewDefaultDatabase(nil),
			},
			names:   newNameDb(),
			aliases: newNameDb(),
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				1: {
					CollectionID: 1,
					Name:         "old",
					State:        pb.CollectionState_CollectionCreated,
				},
				2: {
					CollectionID: 2,
					Name:         "new",
					State:        pb.CollectionState_CollectionCreated,
				},
			},
		}
		meta.names.insert(util.DefaultDBName, "old", 1)
		meta.names.insert(util.DefaultDBName, "new", 2)
		err := meta.RenameCollection(context.TODO(), util.DefaultDBName, "old", util.DefaultDBName, "new", 1000)
		assert.Error(t, err)
	})

	t.Run("alter collection fail", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("AlterCollection",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(errors.New("fail"))

		meta := &MetaTable{
			dbName2Meta: map[string]*model.Database{
				util.DefaultDBName: model.NewDefaultDatabase(nil),
			},
			catalog: catalog,
			names:   newNameDb(),
			aliases: newNameDb(),
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				1: {
					CollectionID: 1,
					Name:         "old",
					DBID:         util.DefaultDBID,
					State:        pb.CollectionState_CollectionCreated,
				},
			},
		}
		meta.names.insert(util.DefaultDBName, "old", 1)
		err := meta.RenameCollection(context.TODO(), util.DefaultDBName, "old", util.DefaultDBName, "new", typeutil.MaxTimestamp)
		assert.Error(t, err)
	})

	t.Run("new name is already an alias", func(t *testing.T) {
		meta := &MetaTable{
			dbName2Meta: map[string]*model.Database{
				util.DefaultDBName: model.NewDefaultDatabase(nil),
			},
			names:   newNameDb(),
			aliases: newNameDb(),
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				1: {
					CollectionID: 1,
					Name:         "old",
					DBID:         util.DefaultDBID,
					State:        pb.CollectionState_CollectionCreated,
				},
			},
		}
		meta.names.insert(util.DefaultDBName, "old", 1)
		meta.aliases.insert(util.DefaultDBName, "new", 1)

		err := meta.RenameCollection(context.TODO(), util.DefaultDBName, "old", util.DefaultDBName, "new", typeutil.MaxTimestamp)
		assert.Error(t, err)
	})

	t.Run("alter collection ok", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("AlterCollectionDB",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil)
		catalog.On("GetCollectionByName",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil, merr.WrapErrCollectionNotFound("error"))
		meta := &MetaTable{
			dbName2Meta: map[string]*model.Database{
				util.DefaultDBName: model.NewDefaultDatabase(nil),
			},
			catalog: catalog,
			names:   newNameDb(),
			aliases: newNameDb(),
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				1: {
					CollectionID: 1,
					Name:         "old",
				},
			},
		}
		meta.names.insert(util.DefaultDBName, "old", 1)
		err := meta.RenameCollection(context.TODO(), util.DefaultDBName, "old", util.DefaultDBName, "new", 1000)
		assert.NoError(t, err)

		id, ok := meta.names.get(util.DefaultDBName, "new")
		assert.True(t, ok)
		assert.Equal(t, int64(1), id)

		coll, ok := meta.collID2Meta[1]
		assert.True(t, ok)
		assert.Equal(t, "new", coll.Name)
	})

	t.Run("rename collection ok", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("AlterCollection",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil)
		catalog.On("GetCollectionByName",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil, merr.WrapErrCollectionNotFound("error"))
		meta := &MetaTable{
			dbName2Meta: map[string]*model.Database{
				util.DefaultDBName: model.NewDefaultDatabase(nil),
			},
			catalog: catalog,
			names:   newNameDb(),
			aliases: newNameDb(),
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				1: {
					CollectionID: 1,
					DBID:         1,
					Name:         "old",
				},
			},
		}
		meta.names.insert(util.DefaultDBName, "old", 1)
		err := meta.RenameCollection(context.TODO(), util.DefaultDBName, "old", util.DefaultDBName, "new", 1000)
		assert.NoError(t, err)

		id, ok := meta.names.get(util.DefaultDBName, "new")
		assert.True(t, ok)
		assert.Equal(t, int64(1), id)

		coll, ok := meta.collID2Meta[1]
		assert.True(t, ok)
		assert.Equal(t, "new", coll.Name)
	})

	t.Run("rename collection rename collName with db encryption on", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("AlterCollection",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil)
		catalog.On("GetCollectionByName",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil, merr.WrapErrCollectionNotFound("error"))

		// Create database with encryption enabled
		encryptedDBProperties := []*commonpb.KeyValuePair{
			{Key: "cipher.enabled", Value: "true"},
		}
		encryptedDB := model.NewDatabase(1, util.DefaultDBName, pb.DatabaseState_DatabaseCreated, encryptedDBProperties)

		meta := &MetaTable{
			dbName2Meta: map[string]*model.Database{
				util.DefaultDBName: encryptedDB,
			},
			catalog: catalog,
			names:   newNameDb(),
			aliases: newNameDb(),
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				1: {
					CollectionID: 1,
					DBID:         1,
					Name:         "old",
				},
			},
		}
		meta.names.insert(util.DefaultDBName, "old", 1)

		// Should succeed when renaming within the same encrypted database
		err := meta.RenameCollection(context.TODO(), util.DefaultDBName, "old", util.DefaultDBName, "new", 1000)
		assert.NoError(t, err)

		// Verify the collection name was updated
		id, ok := meta.names.get(util.DefaultDBName, "new")
		assert.True(t, ok)
		assert.Equal(t, int64(1), id)

		coll, ok := meta.collID2Meta[1]
		assert.True(t, ok)
		assert.Equal(t, "new", coll.Name)
	})
}
*/

func TestMetaTable_CreateDatabase(t *testing.T) {
	db := model.NewDatabase(1, "exist", pb.DatabaseState_DatabaseCreated, nil)
	t.Run("database already exist", func(t *testing.T) {
		meta := &MetaTable{
			names:       newNameDb(),
			aliases:     newNameDb(),
			dbName2Meta: make(map[string]*model.Database),
		}
		meta.names.insert("exist", "collection", 100)

		err := meta.CheckIfDatabaseCreatable(context.TODO(), &milvuspb.CreateDatabaseRequest{
			DbName: "exist",
		})
		assert.Error(t, err)
	})

	t.Run("database not persistent", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("CreateDatabase",
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(errors.New("error mock CreateDatabase"))
		meta := &MetaTable{
			names:   newNameDb(),
			aliases: newNameDb(),
			catalog: catalog,
		}
		err := meta.CreateDatabase(context.TODO(), db, 10000)
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("CreateDatabase",
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil)
		meta := &MetaTable{
			dbName2Meta: make(map[string]*model.Database),
			names:       newNameDb(),
			aliases:     newNameDb(),
			catalog:     catalog,
		}
		err := meta.CheckIfDatabaseCreatable(context.TODO(), &milvuspb.CreateDatabaseRequest{
			DbName: "exist",
		})
		assert.NoError(t, err)
		err = meta.CreateDatabase(context.TODO(), db, 10000)
		assert.NoError(t, err)
		assert.True(t, meta.names.exist("exist"))
		assert.True(t, meta.aliases.exist("exist"))
		assert.True(t, meta.names.empty("exist"))
		assert.True(t, meta.aliases.empty("exist"))
	})
}

func TestCreateDefaultDb(t *testing.T) {

	// Save original config and restore after test
	originalDefaultKey := paramtable.GetCipherParams().DefaultRootKey.GetValue()
	defer func() {
		paramtable.GetCipherParams().Save("cipherPlugin.kms.defaultKey", originalDefaultKey)
	}()

	t.Run("default db without encryption when defaultKey is empty", func(t *testing.T) {
		paramtable.GetCipherParams().Save("cipherPlugin.kms.defaultKey", "")

		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("CreateDatabase",
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil)

		tsoAllocator := mocktso.NewAllocator(t)
		tsoAllocator.On("GenerateTSO", mock.Anything).Return(uint64(100), nil)

		meta := &MetaTable{
			ctx:          context.Background(),
			dbName2Meta:  make(map[string]*model.Database),
			names:        newNameDb(),
			aliases:      newNameDb(),
			catalog:      catalog,
			tsoAllocator: tsoAllocator,
			cipherHelper: fakeCipherHelper{},
		}

		err := meta.createDefaultDb()
		assert.NoError(t, err)

		// Verify default database was created
		db, ok := meta.dbName2Meta[util.DefaultDBName]
		assert.True(t, ok)
		assert.Equal(t, util.DefaultDBName, db.Name)

		// Verify no encryption properties
		hasEncryption := isDBEncrypted(db.Properties)
		assert.False(t, hasEncryption, "default DB should not be encrypted when defaultKey is empty")
	})

	t.Run("default db with encryption when defaultKey is set", func(t *testing.T) {
		paramtable.GetCipherParams().Save("cipherPlugin.kms.defaultKey", "default-test-key")

		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("CreateDatabase",
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil)

		tsoAllocator := mocktso.NewAllocator(t)
		tsoAllocator.On("GenerateTSO", mock.Anything).Return(uint64(200), nil)

		meta := &MetaTable{
			ctx:          context.Background(),
			dbName2Meta:  make(map[string]*model.Database),
			names:        newNameDb(),
			aliases:      newNameDb(),
			catalog:      catalog,
			tsoAllocator: tsoAllocator,
			cipherHelper: fakeCipherHelper{},
		}

		err := meta.createDefaultDb()
		assert.NoError(t, err)

		// Verify default database was created
		db, ok := meta.dbName2Meta[util.DefaultDBName]
		assert.True(t, ok)
		assert.Equal(t, util.DefaultDBName, db.Name)

		// Verify encryption properties are present
		hasEzID := false
		hasRootKey := false
		for _, prop := range db.Properties {
			if prop.Key == common.EncryptionEzIDKey {
				hasEzID = true
				assert.Equal(t, prop.GetValue(), "199")
			}
			if prop.Key == common.EncryptionRootKeyKey && prop.Value == "default-test-key" {
				hasRootKey = true
			}
		}
		assert.True(t, hasRootKey, "default DB should have root key when encrypted")
		assert.True(t, hasEzID, "default DB should have ezID when encrypted")
	})

	t.Run("TSO allocation failure", func(t *testing.T) {
		tsoAllocator := mocktso.NewAllocator(t)
		tsoAllocator.On("GenerateTSO", mock.Anything).Return(uint64(0), errors.New("TSO allocation failed"))

		meta := &MetaTable{
			ctx:          context.Background(),
			dbName2Meta:  make(map[string]*model.Database),
			tsoAllocator: tsoAllocator,
		}

		err := meta.createDefaultDb()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "TSO allocation failed")
	})

	t.Run("catalog CreateDatabase failure", func(t *testing.T) {
		paramtable.GetCipherParams().Save("cipherPlugin.kms.defaultKey", "")

		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("CreateDatabase",
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(errors.New("catalog error"))

		tsoAllocator := mocktso.NewAllocator(t)
		tsoAllocator.On("GenerateTSO", mock.Anything).Return(uint64(300), nil)

		meta := &MetaTable{
			ctx:          context.Background(),
			dbName2Meta:  make(map[string]*model.Database),
			names:        newNameDb(),
			aliases:      newNameDb(),
			catalog:      catalog,
			tsoAllocator: tsoAllocator,
			cipherHelper: fakeCipherHelper{},
		}

		err := meta.createDefaultDb()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "catalog error")
	})
}

func TestAlterDatabase(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("AlterDatabase",
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil)

		db := model.NewDatabase(1, "db1", pb.DatabaseState_DatabaseCreated, nil)

		meta := &MetaTable{
			dbName2Meta: map[string]*model.Database{
				"db1": db,
			},
			names:   newNameDb(),
			aliases: newNameDb(),
			catalog: catalog,
		}
		newDB := db.Clone()
		db.Properties = []*commonpb.KeyValuePair{
			{
				Key:   "key1",
				Value: "value1",
			},
		}
		err := meta.AlterDatabase(context.TODO(), newDB, typeutil.ZeroTimestamp)
		assert.NoError(t, err)
	})

	t.Run("access catalog failed", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		mockErr := errors.New("access catalog failed")
		catalog.On("AlterDatabase",
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(mockErr)

		db := model.NewDatabase(1, "db1", pb.DatabaseState_DatabaseCreated, nil)

		meta := &MetaTable{
			dbName2Meta: map[string]*model.Database{
				"db1": db,
			},
			names:   newNameDb(),
			aliases: newNameDb(),
			catalog: catalog,
		}
		newDB := db.Clone()
		db.Properties = []*commonpb.KeyValuePair{
			{
				Key:   "key1",
				Value: "value1",
			},
		}
		err := meta.AlterDatabase(context.TODO(), newDB, typeutil.ZeroTimestamp)
		assert.ErrorIs(t, err, mockErr)
	})
}

func TestMetaTable_EmtpyDatabaseName(t *testing.T) {
	t.Run("getDatabaseByNameInternal with empty db", func(t *testing.T) {
		mt := &MetaTable{
			dbName2Meta: map[string]*model.Database{
				util.DefaultDBName: {ID: 1},
			},
		}

		ret, err := mt.getDatabaseByNameInternal(context.TODO(), "", typeutil.MaxTimestamp)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), ret.ID)
	})

	t.Run("getCollectionByNameInternal with empty db", func(t *testing.T) {
		mt := &MetaTable{
			aliases: newNameDb(),
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				1: {CollectionID: 1},
			},
			dbName2Meta: map[string]*model.Database{
				util.DefaultDBName: model.NewDefaultDatabase(nil),
			},
		}

		mt.aliases.insert(util.DefaultDBName, "aliases", 1)
		ret, err := mt.getCollectionByNameInternal(context.TODO(), "", "aliases", typeutil.MaxTimestamp, false)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), ret.CollectionID)
	})

	t.Run("listCollectionFromCache with empty db", func(t *testing.T) {
		mt := &MetaTable{
			names: newNameDb(),
			dbName2Meta: map[string]*model.Database{
				util.DefaultDBName: model.NewDefaultDatabase(nil),
				"db2":              model.NewDatabase(2, "db2", pb.DatabaseState_DatabaseCreated, nil),
			},
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				1: {
					CollectionID: 1,
					State:        pb.CollectionState_CollectionCreated,
				},
				2: {
					CollectionID: 2,
					State:        pb.CollectionState_CollectionDropping,
					DBID:         util.DefaultDBID,
				},
				3: {
					CollectionID: 3,
					State:        pb.CollectionState_CollectionCreated,
					DBID:         2,
				},
			},
		}

		ret, err := mt.listCollectionFromCache(context.TODO(), "none", false)
		assert.Error(t, err)
		assert.Nil(t, ret)

		ret, err = mt.listCollectionFromCache(context.TODO(), "", false)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(ret))
		assert.Contains(t, []int64{1, 2}, ret[0].CollectionID)
		assert.Contains(t, []int64{1, 2}, ret[1].CollectionID)

		ret, err = mt.listCollectionFromCache(context.TODO(), "db2", false)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(ret))
		assert.Equal(t, int64(3), ret[0].CollectionID)
	})

	t.Run("CreateAlias with empty db", func(t *testing.T) {
		mt := &MetaTable{
			names: newNameDb(),
		}

		mt.names.insert(util.DefaultDBName, "name", 1)
		err := mt.CheckIfAliasCreatable(context.TODO(), "", "name", "name")
		assert.Error(t, err)
	})
}

func TestMetaTable_DropDatabase(t *testing.T) {
	t.Run("can't drop default database", func(t *testing.T) {
		mt := &MetaTable{}
		err := mt.CheckIfDatabaseDroppable(context.TODO(), &milvuspb.DropDatabaseRequest{
			DbName: util.DefaultDBName,
		})
		assert.Error(t, err)
	})

	t.Run("database not exist", func(t *testing.T) {
		mt := &MetaTable{
			names:   newNameDb(),
			aliases: newNameDb(),
		}
		err := mt.CheckIfDatabaseDroppable(context.TODO(), &milvuspb.DropDatabaseRequest{
			DbName: "not_exist",
		})
		assert.True(t, errors.Is(err, merr.ErrDatabaseNotFound))
	})

	t.Run("database not empty", func(t *testing.T) {
		mt := &MetaTable{
			dbName2Meta: map[string]*model.Database{
				"not_empty": model.NewDatabase(1, "not_empty", pb.DatabaseState_DatabaseCreated, nil),
			},
			names:   newNameDb(),
			aliases: newNameDb(),
			collID2Meta: map[int64]*model.Collection{
				10000000: {
					DBID:         1,
					CollectionID: 10000000,
					Name:         "collection",
				},
			},
		}
		mt.names.insert("not_empty", "collection", 10000000)
		err := mt.CheckIfDatabaseDroppable(context.TODO(), &milvuspb.DropDatabaseRequest{
			DbName: "not_empty",
		})
		assert.Error(t, err)
	})

	t.Run("not commit", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("DropDatabase",
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(errors.New("error mock DropDatabase"))
		mt := &MetaTable{
			dbName2Meta: map[string]*model.Database{
				"not_commit": model.NewDatabase(1, "not_commit", pb.DatabaseState_DatabaseCreated, nil),
			},
			names:   newNameDb(),
			aliases: newNameDb(),
			catalog: catalog,
		}
		mt.names.createDbIfNotExist("not_commit")
		mt.aliases.createDbIfNotExist("not_commit")
		err := mt.CheckIfDatabaseDroppable(context.TODO(), &milvuspb.DropDatabaseRequest{
			DbName: "not_commit",
		})
		assert.NoError(t, err)
		err = mt.DropDatabase(context.TODO(), "not_commit", 10000)
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("DropDatabase",
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil)
		mt := &MetaTable{
			dbName2Meta: map[string]*model.Database{
				"not_commit": model.NewDatabase(1, "not_commit", pb.DatabaseState_DatabaseCreated, nil),
			},
			names:   newNameDb(),
			aliases: newNameDb(),
			catalog: catalog,
		}
		mt.names.createDbIfNotExist("not_commit")
		mt.aliases.createDbIfNotExist("not_commit")
		err := mt.CheckIfDatabaseDroppable(context.TODO(), &milvuspb.DropDatabaseRequest{
			DbName: "not_commit",
		})
		assert.NoError(t, err)
		err = mt.DropDatabase(context.TODO(), "not_commit", 10000)
		assert.NoError(t, err)
		assert.False(t, mt.names.exist("not_commit"))
		assert.False(t, mt.aliases.exist("not_commit"))
	})
}

func TestMetaTable_BackupRBAC(t *testing.T) {
	catalog := mocks.NewRootCoordCatalog(t)
	catalog.EXPECT().BackupRBAC(mock.Anything, mock.Anything).Return(&milvuspb.RBACMeta{}, nil)
	mt := &MetaTable{
		dbName2Meta: map[string]*model.Database{
			"not_commit": model.NewDatabase(1, "not_commit", pb.DatabaseState_DatabaseCreated, nil),
		},
		names:   newNameDb(),
		aliases: newNameDb(),
		catalog: catalog,
	}
	_, err := mt.BackupRBAC(context.TODO(), util.DefaultTenant)
	assert.NoError(t, err)

	catalog.ExpectedCalls = nil
	catalog.EXPECT().BackupRBAC(mock.Anything, mock.Anything).Return(nil, errors.New("error mock BackupRBAC"))
	_, err = mt.BackupRBAC(context.TODO(), util.DefaultTenant)
	assert.Error(t, err)
}

func TestMetaTable_RestoreRBAC(t *testing.T) {
	catalog := mocks.NewRootCoordCatalog(t)
	catalog.EXPECT().RestoreRBAC(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mt := &MetaTable{
		dbName2Meta: map[string]*model.Database{
			"not_commit": model.NewDatabase(1, "not_commit", pb.DatabaseState_DatabaseCreated, nil),
		},
		names:   newNameDb(),
		aliases: newNameDb(),
		catalog: catalog,
	}

	err := mt.RestoreRBAC(context.TODO(), util.DefaultTenant, &milvuspb.RBACMeta{})
	assert.NoError(t, err)

	catalog.ExpectedCalls = nil
	catalog.EXPECT().RestoreRBAC(mock.Anything, mock.Anything, mock.Anything).Return(errors.New("error mock RestoreRBAC"))
	err = mt.RestoreRBAC(context.TODO(), util.DefaultTenant, &milvuspb.RBACMeta{})
	assert.Error(t, err)
}

func TestMetaTable_CheckIfRBACRestorable_Wildcard(t *testing.T) {
	catalog := mocks.NewRootCoordCatalog(t)
	catalog.EXPECT().ListRole(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, nil)
	catalog.EXPECT().ListPrivilegeGroups(mock.Anything).
		Return(nil, nil)
	catalog.EXPECT().ListUser(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, nil)

	mt := &MetaTable{
		dbName2Meta: map[string]*model.Database{
			"not_commit": model.NewDatabase(1, "not_commit", pb.DatabaseState_DatabaseCreated, nil),
		},
		names:   newNameDb(),
		aliases: newNameDb(),
		catalog: catalog,
	}

	req := &milvuspb.RestoreRBACMetaRequest{
		RBACMeta: &milvuspb.RBACMeta{
			Roles: []*milvuspb.RoleEntity{{Name: "wildcard_role"}},
			Grants: []*milvuspb.GrantEntity{
				{
					Role:       &milvuspb.RoleEntity{Name: "wildcard_role"},
					Object:     &milvuspb.ObjectEntity{Name: commonpb.ObjectType_Global.String()},
					ObjectName: util.AnyWord,
					DbName:     util.AnyWord,
					Grantor: &milvuspb.GrantorEntity{
						User:      &milvuspb.UserEntity{Name: util.UserRoot},
						Privilege: &milvuspb.PrivilegeEntity{Name: util.AnyWord},
					},
				},
			},
		},
	}
	assert.NoError(t, mt.CheckIfRBACRestorable(context.TODO(), req))
}

func TestMetaTable_PrivilegeGroup(t *testing.T) {
	catalog := mocks.NewRootCoordCatalog(t)
	catalog.EXPECT().ListPrivilegeGroups(mock.Anything).Return([]*milvuspb.PrivilegeGroupInfo{
		{
			GroupName:  "pg1",
			Privileges: []*milvuspb.PrivilegeEntity{{Name: "CreateCollection"}, {Name: "DescribeCollection"}},
		},
	}, nil)
	catalog.EXPECT().ListRole(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	catalog.EXPECT().SavePrivilegeGroup(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().DropPrivilegeGroup(mock.Anything, mock.Anything).Return(nil)
	mt := &MetaTable{
		dbName2Meta: map[string]*model.Database{
			"not_commit": model.NewDatabase(1, "not_commit", pb.DatabaseState_DatabaseCreated, nil),
		},
		names:   newNameDb(),
		aliases: newNameDb(),
		catalog: catalog,
	}
	err := mt.CheckIfPrivilegeGroupCreatable(context.TODO(), &milvuspb.CreatePrivilegeGroupRequest{
		GroupName: "pg1",
	})
	assert.Error(t, err)
	err = mt.CheckIfPrivilegeGroupCreatable(context.TODO(), &milvuspb.CreatePrivilegeGroupRequest{
		GroupName: "",
	})
	assert.Error(t, err)
	err = mt.CheckIfPrivilegeGroupCreatable(context.TODO(), &milvuspb.CreatePrivilegeGroupRequest{
		GroupName: "Insert",
	})
	assert.Error(t, err)
	err = mt.CheckIfPrivilegeGroupCreatable(context.TODO(), &milvuspb.CreatePrivilegeGroupRequest{
		GroupName: "pg2",
	})
	assert.NoError(t, err)
	err = mt.CreatePrivilegeGroup(context.TODO(), "pg1")
	assert.NoError(t, err)
	// idempotency
	err = mt.CreatePrivilegeGroup(context.TODO(), "pg1")
	assert.NoError(t, err)

	err = mt.CheckIfPrivilegeGroupDropable(context.TODO(), &milvuspb.DropPrivilegeGroupRequest{
		GroupName: "",
	})
	assert.Error(t, err)
	err = mt.CheckIfPrivilegeGroupDropable(context.TODO(), &milvuspb.DropPrivilegeGroupRequest{
		GroupName: "pg1",
	})
	assert.NoError(t, err)
	err = mt.DropPrivilegeGroup(context.TODO(), "pg1")
	assert.NoError(t, err)
	err = mt.CheckIfPrivilegeGroupAlterable(context.TODO(), &milvuspb.OperatePrivilegeGroupRequest{
		GroupName:  "",
		Privileges: []*milvuspb.PrivilegeEntity{},
		Type:       milvuspb.OperatePrivilegeGroupType_AddPrivilegesToGroup,
	})
	assert.Error(t, err)
	err = mt.CheckIfPrivilegeGroupAlterable(context.TODO(), &milvuspb.OperatePrivilegeGroupRequest{
		GroupName:  "ClusterReadOnly",
		Privileges: []*milvuspb.PrivilegeEntity{},
		Type:       milvuspb.OperatePrivilegeGroupType_AddPrivilegesToGroup,
	})
	assert.Error(t, err)
	err = mt.CheckIfPrivilegeGroupAlterable(context.TODO(), &milvuspb.OperatePrivilegeGroupRequest{
		GroupName:  "pg3",
		Privileges: []*milvuspb.PrivilegeEntity{},
		Type:       milvuspb.OperatePrivilegeGroupType_AddPrivilegesToGroup,
	})
	assert.Error(t, err)
	_, err = mt.GetPrivilegeGroupRoles(context.TODO(), "")
	assert.Error(t, err)
	_, err = mt.ListPrivilegeGroups(context.TODO())
	assert.NoError(t, err)
}
