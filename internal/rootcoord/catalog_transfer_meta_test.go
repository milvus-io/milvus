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

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	pb "github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func newTransferMetaTableForTest(catalog *mocks.RootCoordCatalog) *MetaTable {
	meta := &MetaTable{
		catalog: catalog,
		dbName2Meta: map[string]*model.Database{
			util.DefaultDBName: model.NewDefaultDatabase(nil),
		},
		collID2Meta:          map[typeutil.UniqueID]*model.Collection{},
		partitionName2ID:     map[int64]map[string]int64{},
		fileResourceRefCnt:   map[int64]int{},
		fileResourceRefHolds: map[int64]map[int64]int{},
		names:                newNameDb(),
		aliases:              newNameDb(),
	}
	meta.names.createDbIfNotExist(util.DefaultDBName)
	meta.aliases.createDbIfNotExist(util.DefaultDBName)
	return meta
}

func TestMetaTableApplyTransferredCollectionLoadsLiveIndexes(t *testing.T) {
	const (
		collectionID = int64(100)
		partitionID  = int64(200)
		resourceID   = int64(300)
	)

	channel.RecoverPChannelStatsManager([]string{})
	catalog := mocks.NewRootCoordCatalog(t)
	meta := newTransferMetaTableForTest(catalog)

	coll := &model.Collection{
		DBID:                util.DefaultDBID,
		DBName:              util.DefaultDBName,
		CollectionID:        collectionID,
		Name:                "transferred_collection",
		State:               pb.CollectionState_CollectionCreated,
		ShardsNum:           2,
		VirtualChannelNames: []string{"transfer-vchan-1", "transfer-vchan-2"},
		FileResourceIds:     []int64{resourceID},
		Aliases:             []string{"transferred_alias"},
		Partitions: []*model.Partition{
			{
				PartitionID:   partitionID,
				PartitionName: "p_transfer",
				State:         pb.PartitionState_PartitionCreated,
			},
			{
				PartitionID:   partitionID + 1,
				PartitionName: "p_dropping",
				State:         pb.PartitionState_PartitionDropping,
			},
		},
	}

	require.NoError(t, meta.ApplyTransferredCollection(context.Background(), coll))

	byName, err := meta.GetCollectionByName(context.Background(), util.DefaultDBName, coll.Name, typeutil.MaxTimestamp, false)
	require.NoError(t, err)
	require.Equal(t, collectionID, byName.CollectionID)

	byAlias, err := meta.GetCollectionByName(context.Background(), util.DefaultDBName, "transferred_alias", typeutil.MaxTimestamp, false)
	require.NoError(t, err)
	require.Equal(t, collectionID, byAlias.CollectionID)

	gotPartitionID, ok := meta.GetPartitionIDByName(collectionID, "p_transfer")
	require.True(t, ok)
	require.Equal(t, partitionID, gotPartitionID)
	_, ok = meta.GetPartitionIDByName(collectionID, "p_dropping")
	require.False(t, ok)

	require.Equal(t, 1, meta.fileResourceRefCnt[resourceID])
	require.Equal(t, 2*1, meta.GetGeneralCount(context.Background()))

	require.NoError(t, meta.ApplyTransferredCollection(context.Background(), coll))
	require.Equal(t, 1, meta.fileResourceRefCnt[resourceID])
	require.Equal(t, 2*1, meta.GetGeneralCount(context.Background()))
	catalog.AssertNotCalled(t, "CreateCollection")
	catalog.AssertNotCalled(t, "AlterCollection")
}

func TestMetaTableDeactivateTransferredCollectionRemovesLiveIndexesWithoutCatalogWrite(t *testing.T) {
	const (
		collectionID = int64(100)
		partitionID  = int64(200)
		resourceID   = int64(300)
	)

	channel.RecoverPChannelStatsManager([]string{})
	catalog := mocks.NewRootCoordCatalog(t)
	meta := newTransferMetaTableForTest(catalog)
	coll := &model.Collection{
		DBID:                util.DefaultDBID,
		DBName:              util.DefaultDBName,
		CollectionID:        collectionID,
		Name:                "source_collection",
		State:               pb.CollectionState_CollectionCreated,
		ShardsNum:           1,
		VirtualChannelNames: []string{"source-vchan"},
		FileResourceIds:     []int64{resourceID},
		Aliases:             []string{"source_alias"},
		Partitions: []*model.Partition{
			{
				PartitionID:   partitionID,
				PartitionName: "p_source",
				State:         pb.PartitionState_PartitionCreated,
			},
		},
	}
	require.NoError(t, meta.ApplyTransferredCollection(context.Background(), coll))

	require.NoError(t, meta.DeactivateTransferredCollection(context.Background(), collectionID))

	_, err := meta.GetCollectionByName(context.Background(), util.DefaultDBName, "source_collection", typeutil.MaxTimestamp, false)
	require.Error(t, err)
	_, err = meta.GetCollectionByName(context.Background(), util.DefaultDBName, "source_alias", typeutil.MaxTimestamp, false)
	require.Error(t, err)
	_, err = meta.GetCollectionByID(context.Background(), util.DefaultDBName, collectionID, typeutil.MaxTimestamp, false)
	require.Error(t, err)
	_, ok := meta.GetPartitionIDByName(collectionID, "p_source")
	require.False(t, ok)
	require.Zero(t, meta.fileResourceRefCnt[resourceID])
	require.Zero(t, meta.GetGeneralCount(context.Background()))
	catalog.AssertNotCalled(t, "DropCollection")
	catalog.AssertNotCalled(t, "AlterCollection")
	catalog.AssertNotCalled(t, "Update")
}

func TestMetaTableApplyTransferredCollectionRejectsCollectionIDIdentityChange(t *testing.T) {
	const collectionID = int64(100)

	channel.RecoverPChannelStatsManager([]string{})
	meta := newTransferMetaTableForTest(nil)

	require.NoError(t, meta.ApplyTransferredCollection(context.Background(), &model.Collection{
		DBID:         util.DefaultDBID,
		DBName:       util.DefaultDBName,
		CollectionID: collectionID,
		Name:         "first",
		State:        pb.CollectionState_CollectionCreated,
		ShardsNum:    1,
		Partitions: []*model.Partition{
			{
				PartitionID:   200,
				PartitionName: "p",
				State:         pb.PartitionState_PartitionCreated,
			},
		},
	}))

	err := meta.ApplyTransferredCollection(context.Background(), &model.Collection{
		DBID:         util.DefaultDBID,
		DBName:       util.DefaultDBName,
		CollectionID: collectionID,
		Name:         "second",
		State:        pb.CollectionState_CollectionCreated,
		ShardsNum:    1,
	})
	require.Error(t, err)

	coll, err := meta.GetCollectionByID(context.Background(), util.DefaultDBName, collectionID, typeutil.MaxTimestamp, false)
	require.NoError(t, err)
	require.Equal(t, "first", coll.Name)
}

func TestMetaTableApplyTransferredCollectionRejectsUnknownDatabaseWithoutSideEffects(t *testing.T) {
	const collectionID = int64(100)

	channel.RecoverPChannelStatsManager([]string{})
	meta := newTransferMetaTableForTest(nil)

	err := meta.ApplyTransferredCollection(context.Background(), &model.Collection{
		DBID:         999,
		DBName:       "missing_db",
		CollectionID: collectionID,
		Name:         "missing_db_collection",
		State:        pb.CollectionState_CollectionCreated,
		ShardsNum:    1,
		FileResourceIds: []int64{
			300,
		},
		Partitions: []*model.Partition{
			{
				PartitionID:   200,
				PartitionName: "p",
				State:         pb.PartitionState_PartitionCreated,
			},
		},
	})
	require.Error(t, err)
	require.Empty(t, meta.collID2Meta)
	require.Empty(t, meta.partitionName2ID)
	require.Empty(t, meta.fileResourceRefCnt)
	require.Zero(t, meta.GetGeneralCount(context.Background()))
	require.Equal(t, InvalidCollectionID, meta.GetCollectionID(context.Background(), "missing_db", "missing_db_collection"))
}

func TestMetaTableApplyTransferredCollectionLoadsMissingDatabaseFromCatalog(t *testing.T) {
	const collectionID = int64(100)
	const dbID = int64(999)

	channel.RecoverPChannelStatsManager([]string{})
	catalog := mocks.NewRootCoordCatalog(t)
	meta := newTransferMetaTableForTest(catalog)
	catalog.EXPECT().ListDatabases(mock.Anything, typeutil.MaxTimestamp).Return([]*model.Database{
		{ID: dbID, Name: "target_db", State: pb.DatabaseState_DatabaseCreated},
	}, nil)

	err := meta.ApplyTransferredCollection(context.Background(), &model.Collection{
		DBID:         dbID,
		DBName:       "target_db",
		CollectionID: collectionID,
		Name:         "target_collection",
		State:        pb.CollectionState_CollectionCreated,
		ShardsNum:    1,
		Partitions: []*model.Partition{
			{
				PartitionID:   200,
				PartitionName: "p",
				State:         pb.PartitionState_PartitionCreated,
			},
		},
	})
	require.NoError(t, err)

	db, err := meta.GetDatabaseByName(context.Background(), "target_db", typeutil.MaxTimestamp)
	require.NoError(t, err)
	require.Equal(t, dbID, db.ID)
	byName, err := meta.GetCollectionByName(context.Background(), "target_db", "target_collection", typeutil.MaxTimestamp, false)
	require.NoError(t, err)
	require.Equal(t, collectionID, byName.CollectionID)
	catalog.AssertNotCalled(t, "CreateDatabase")
}

func TestMetaTableApplyTransferredCollectionRejectsOperationalMetadataChange(t *testing.T) {
	const collectionID = int64(100)

	channel.RecoverPChannelStatsManager([]string{})
	meta := newTransferMetaTableForTest(nil)

	base := &model.Collection{
		DBID:                util.DefaultDBID,
		DBName:              util.DefaultDBName,
		CollectionID:        collectionID,
		Name:                "same_identity",
		State:               pb.CollectionState_CollectionCreated,
		ShardsNum:           1,
		VirtualChannelNames: []string{"vchan-1"},
		FileResourceIds:     []int64{300},
		Aliases:             []string{"alias_1"},
		Partitions: []*model.Partition{
			{
				PartitionID:   200,
				PartitionName: "p1",
				State:         pb.PartitionState_PartitionCreated,
			},
		},
	}
	require.NoError(t, meta.ApplyTransferredCollection(context.Background(), base))

	changed := base.Clone()
	changed.Aliases = []string{"alias_2"}
	changed.Partitions = append(changed.Partitions, &model.Partition{
		PartitionID:   201,
		PartitionName: "p2",
		State:         pb.PartitionState_PartitionCreated,
	})
	changed.ShardsNum = 2
	changed.VirtualChannelNames = []string{"vchan-2"}
	changed.FileResourceIds = []int64{301}

	err := meta.ApplyTransferredCollection(context.Background(), changed)
	require.Error(t, err)

	require.True(t, meta.IsAlias(context.Background(), util.DefaultDBName, "alias_1"))
	require.False(t, meta.IsAlias(context.Background(), util.DefaultDBName, "alias_2"))
	_, ok := meta.GetPartitionIDByName(collectionID, "p2")
	require.False(t, ok)
	require.Equal(t, 1, meta.fileResourceRefCnt[int64(300)])
	require.Zero(t, meta.fileResourceRefCnt[int64(301)])
	require.Equal(t, 1, meta.GetGeneralCount(context.Background()))
}

func TestMetaTableApplyTransferredCollectionAllowsCanonicalEquivalentRetry(t *testing.T) {
	const collectionID = int64(100)

	channel.RecoverPChannelStatsManager([]string{})
	meta := newTransferMetaTableForTest(nil)

	base := &model.Collection{
		DBID:                util.DefaultDBID,
		DBName:              util.DefaultDBName,
		CollectionID:        collectionID,
		Name:                "canonical_retry",
		State:               pb.CollectionState_CollectionCreated,
		ShardsNum:           1,
		VirtualChannelNames: []string{"vchan-b", "vchan-a"},
		FileResourceIds:     []int64{301, 300},
		Aliases:             []string{"alias_b", "alias_a"},
		Partitions: []*model.Partition{
			{
				PartitionID:   201,
				PartitionName: "p2",
				State:         pb.PartitionState_PartitionCreated,
			},
			{
				PartitionID:   200,
				PartitionName: "p1",
				State:         pb.PartitionState_PartitionCreated,
			},
		},
	}
	require.NoError(t, meta.ApplyTransferredCollection(context.Background(), base))

	retry := base.Clone()
	retry.VirtualChannelNames = []string{"vchan-a", "vchan-b"}
	retry.FileResourceIds = []int64{300, 301}
	retry.Aliases = []string{"alias_a", "alias_b"}
	retry.Partitions = []*model.Partition{
		base.Partitions[1].Clone(),
		base.Partitions[0].Clone(),
	}

	require.NoError(t, meta.ApplyTransferredCollection(context.Background(), retry))
	require.Equal(t, 1, meta.fileResourceRefCnt[int64(300)])
	require.Equal(t, 1, meta.fileResourceRefCnt[int64(301)])
	require.Equal(t, 2, meta.GetGeneralCount(context.Background()))
}

func TestMetaTableAddCollectionDoesNotDoubleCountReservedFileResourceRefs(t *testing.T) {
	const (
		collectionID = int64(100)
		resourceID   = int64(300)
	)

	channel.RecoverPChannelStatsManager([]string{})
	catalog := mocks.NewRootCoordCatalog(t)
	catalog.On("CreateCollection", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	meta := newTransferMetaTableForTest(catalog)
	meta.fileResourceRefCnt[resourceID] = 1

	err := meta.AddCollection(context.Background(), &model.Collection{
		DBID:            util.DefaultDBID,
		DBName:          util.DefaultDBName,
		CollectionID:    collectionID,
		Name:            "created_collection",
		State:           pb.CollectionState_CollectionCreated,
		ShardsNum:       1,
		FileResourceIds: []int64{resourceID},
		Partitions: []*model.Partition{
			{
				PartitionID:   200,
				PartitionName: "p",
				State:         pb.PartitionState_PartitionCreated,
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, meta.fileResourceRefCnt[resourceID])
}
