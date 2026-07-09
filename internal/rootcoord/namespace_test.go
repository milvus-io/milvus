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
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/common"
	pb "github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	namespaceTestDBID       = int64(10)
	namespaceTestCollection = int64(100)
	namespaceTestDefault    = int64(101)
	namespaceTestNextPartID = int64(102)
)

func newNamespaceTestCore(t *testing.T, dbName, collectionName string, enableNamespace bool, mode string) *Core {
	t.Helper()

	properties := []*commonpb.KeyValuePair(nil)
	if mode != "" {
		properties = []*commonpb.KeyValuePair{{Key: common.NamespaceModeKey, Value: mode}}
	}

	names := newNameDb()
	names.insert(dbName, collectionName, namespaceTestCollection)
	defaultPartition := &model.Partition{
		PartitionID:               namespaceTestDefault,
		PartitionName:             Params.CommonCfg.DefaultPartitionName.GetValue(),
		PartitionCreatedTimestamp: tsoutil.ComposeTSByTime(time.Now(), 0),
		CollectionID:              namespaceTestCollection,
		State:                     pb.PartitionState_PartitionCreated,
	}
	return newTestCore(
		withHealthyCode(),
		withMeta(&MetaTable{
			names:       names,
			aliases:     newNameDb(),
			dbName2Meta: map[string]*model.Database{dbName: model.NewDatabase(namespaceTestDBID, dbName, pb.DatabaseState_DatabaseCreated, nil)},
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				namespaceTestCollection: {
					DBID:                namespaceTestDBID,
					CollectionID:        namespaceTestCollection,
					Name:                collectionName,
					DBName:              dbName,
					State:               pb.CollectionState_CollectionCreated,
					EnableNamespace:     enableNamespace,
					Properties:          properties,
					Partitions:          []*model.Partition{defaultPartition},
					ShardsNum:           1,
					VirtualChannelNames: []string{"by-dev-rootcoord-dml_0v0"},
				},
			},
			partitionName2ID: map[int64]map[string]int64{
				namespaceTestCollection: {defaultPartition.PartitionName: defaultPartition.PartitionID},
			},
		}),
	)
}

func patchNamespaceBroadcasts(t *testing.T) {
	t.Helper()
	nextPartitionID := namespaceTestNextPartID
	createGuard := mockey.Mock((*Core).broadcastCreatePartition).To(func(c *Core, ctx context.Context, in *milvuspb.CreatePartitionRequest) (int64, error) {
		coll, err := c.meta.GetCollectionByName(ctx, in.GetDbName(), in.GetCollectionName(), typeutil.MaxTimestamp, true)
		if err != nil {
			return 0, err
		}
		if partitionID, exists := c.meta.GetPartitionIDByName(coll.CollectionID, in.GetPartitionName()); exists {
			return partitionID, errIgnoerdCreatePartition
		}

		partitionID := nextPartitionID
		nextPartitionID++
		addNamespaceTestPartition(c.meta.(*MetaTable), coll.CollectionID, partitionID, in.GetPartitionName(), pb.PartitionState_PartitionCreated)
		return partitionID, nil
	}).Build()
	dropGuard := mockey.Mock((*Core).broadcastDropPartition).To(func(c *Core, ctx context.Context, in *milvuspb.DropPartitionRequest) error {
		coll, err := c.meta.GetCollectionByName(ctx, in.GetDbName(), in.GetCollectionName(), typeutil.MaxTimestamp, true)
		if err != nil {
			return err
		}

		mt := c.meta.(*MetaTable)
		mt.ddLock.Lock()
		defer mt.ddLock.Unlock()
		for _, partition := range mt.collID2Meta[coll.CollectionID].Partitions {
			if partition.PartitionName == in.GetPartitionName() {
				partition.State = pb.PartitionState_PartitionDropping
				delete(mt.partitionName2ID[coll.CollectionID], partition.PartitionName)
				return nil
			}
		}
		return errIgnoredDropPartition
	}).Build()
	t.Cleanup(func() {
		dropGuard.UnPatch()
		createGuard.UnPatch()
	})
}

func addNamespaceTestPartition(mt *MetaTable, collectionID, partitionID int64, partitionName string, state pb.PartitionState) {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()
	mt.collID2Meta[collectionID].Partitions = append(mt.collID2Meta[collectionID].Partitions, &model.Partition{
		PartitionID:               partitionID,
		PartitionName:             partitionName,
		PartitionCreatedTimestamp: tsoutil.ComposeTSByTime(time.Now(), 0),
		CollectionID:              collectionID,
		State:                     state,
	})
	if mt.partitionName2ID[collectionID] == nil {
		mt.partitionName2ID[collectionID] = make(map[string]int64)
	}
	mt.partitionName2ID[collectionID][partitionName] = partitionID
}

func TestNamespaceLifecyclePartitionMode(t *testing.T) {
	mockey.PatchConvey("namespace lifecycle partition mode", t, func() {
		patchNamespaceBroadcasts(t)
		ctx := context.Background()
		dbName := "ns_db_" + funcutil.RandomString(8)
		collectionName := "ns_coll_" + funcutil.RandomString(8)
		core := newNamespaceTestCore(t, dbName, collectionName, true, common.NamespaceModePartition)

		createResp, err := core.CreateNamespace(ctx, &milvuspb.CreateNamespaceRequest{
			DbName:         dbName,
			CollectionName: collectionName,
			NamespaceName:  "tenant_b",
		})
		require.NoError(t, merr.CheckRPCCall(createResp.GetStatus(), err))
		require.Equal(t, collectionName, createResp.GetNamespace().GetCollectionName())
		require.Equal(t, "tenant_b", createResp.GetNamespace().GetNamespaceName())
		require.Equal(t, "Ready", createResp.GetNamespace().GetState())
		require.NotZero(t, createResp.GetNamespace().GetCreatedTimestamp())
		require.NotZero(t, createResp.GetNamespace().GetCreatedUtcTimestamp())

		dupResp, err := core.CreateNamespace(ctx, &milvuspb.CreateNamespaceRequest{
			DbName:         dbName,
			CollectionName: collectionName,
			NamespaceName:  "tenant_b",
		})
		require.ErrorIs(t, merr.CheckRPCCall(dupResp.GetStatus(), err), merr.ErrNamespaceAlreadyExists)

		createResp, err = core.CreateNamespace(ctx, &milvuspb.CreateNamespaceRequest{
			DbName:         dbName,
			CollectionName: collectionName,
			NamespaceName:  "tenant_a",
		})
		require.NoError(t, merr.CheckRPCCall(createResp.GetStatus(), err))
		createResp, err = core.CreateNamespace(ctx, &milvuspb.CreateNamespaceRequest{
			DbName:         dbName,
			CollectionName: collectionName,
			NamespaceName:  "tenant_c",
		})
		require.NoError(t, merr.CheckRPCCall(createResp.GetStatus(), err))
		createResp, err = core.CreateNamespace(ctx, &milvuspb.CreateNamespaceRequest{
			DbName:         dbName,
			CollectionName: collectionName,
			NamespaceName:  "other",
		})
		require.NoError(t, merr.CheckRPCCall(createResp.GetStatus(), err))

		describeResp, err := core.DescribeNamespace(ctx, &milvuspb.DescribeNamespaceRequest{
			DbName:         dbName,
			CollectionName: collectionName,
			NamespaceName:  "tenant_b",
		})
		require.NoError(t, merr.CheckRPCCall(describeResp.GetStatus(), err))
		require.Equal(t, "tenant_b", describeResp.GetNamespace().GetNamespaceName())

		listResp, err := core.ListNamespaces(ctx, &milvuspb.ListNamespacesRequest{
			DbName:         dbName,
			CollectionName: collectionName,
			Prefix:         "tenant_",
			PageSize:       2,
		})
		require.NoError(t, merr.CheckRPCCall(listResp.GetStatus(), err))
		require.Equal(t, []string{"tenant_a", "tenant_b"}, namespaceNames(listResp.GetNamespaces()))
		require.Equal(t, "2", listResp.GetNextPageToken())

		listResp, err = core.ListNamespaces(ctx, &milvuspb.ListNamespacesRequest{
			DbName:         dbName,
			CollectionName: collectionName,
			Prefix:         "tenant_",
			PageSize:       2,
			PageToken:      listResp.GetNextPageToken(),
		})
		require.NoError(t, merr.CheckRPCCall(listResp.GetStatus(), err))
		require.Equal(t, []string{"tenant_c"}, namespaceNames(listResp.GetNamespaces()))
		require.Empty(t, listResp.GetNextPageToken())
		require.NotContains(t, namespaceNames(listResp.GetNamespaces()), Params.CommonCfg.DefaultPartitionName.GetValue())

		hasResp, err := core.HasNamespace(ctx, &milvuspb.HasNamespaceRequest{
			DbName:         dbName,
			CollectionName: collectionName,
			NamespaceName:  "tenant_b",
		})
		require.NoError(t, merr.CheckRPCCall(hasResp.GetStatus(), err))
		require.True(t, hasResp.GetValue())

		dropResp, err := core.DropNamespace(ctx, &milvuspb.DropNamespaceRequest{
			DbName:         dbName,
			CollectionName: collectionName,
			NamespaceName:  "tenant_b",
		})
		require.NoError(t, merr.CheckRPCCall(dropResp.GetStatus(), err))
		require.Equal(t, "tenant_b", dropResp.GetNamespace().GetNamespaceName())

		hasResp, err = core.HasNamespace(ctx, &milvuspb.HasNamespaceRequest{
			DbName:         dbName,
			CollectionName: collectionName,
			NamespaceName:  "tenant_b",
		})
		require.NoError(t, merr.CheckRPCCall(hasResp.GetStatus(), err))
		require.False(t, hasResp.GetValue())
	})
}

func TestNamespaceMissingAndModeErrors(t *testing.T) {
	mockey.PatchConvey("namespace missing and mode errors", t, func() {
		ctx := context.Background()
		dbName := "ns_db_" + funcutil.RandomString(8)
		collectionName := "ns_coll_" + funcutil.RandomString(8)
		core := newNamespaceTestCore(t, dbName, collectionName, true, common.NamespaceModePartition)

		describeResp, err := core.DescribeNamespace(ctx, &milvuspb.DescribeNamespaceRequest{
			DbName:         dbName,
			CollectionName: collectionName,
			NamespaceName:  "missing",
		})
		require.ErrorIs(t, merr.CheckRPCCall(describeResp.GetStatus(), err), merr.ErrNamespaceNotFound)

		dropResp, err := core.DropNamespace(ctx, &milvuspb.DropNamespaceRequest{
			DbName:         dbName,
			CollectionName: collectionName,
			NamespaceName:  "missing",
		})
		require.ErrorIs(t, merr.CheckRPCCall(dropResp.GetStatus(), err), merr.ErrNamespaceNotFound)

		disabledDB := "ns_db_" + funcutil.RandomString(8)
		disabledColl := "ns_coll_" + funcutil.RandomString(8)
		disabledCore := newNamespaceTestCore(t, disabledDB, disabledColl, false, "")
		partitionKeyDB := "ns_db_" + funcutil.RandomString(8)
		partitionKeyColl := "ns_coll_" + funcutil.RandomString(8)
		partitionKeyCore := newNamespaceTestCore(t, partitionKeyDB, partitionKeyColl, true, common.NamespaceModePartitionKey)
		createResp, err := partitionKeyCore.CreateNamespace(ctx, &milvuspb.CreateNamespaceRequest{
			DbName:         partitionKeyDB,
			CollectionName: partitionKeyColl,
			NamespaceName:  "tenant_a",
		})
		require.ErrorIs(t, merr.CheckRPCCall(createResp.GetStatus(), err), merr.ErrParameterInvalid)

		createResp, err = disabledCore.CreateNamespace(ctx, &milvuspb.CreateNamespaceRequest{
			DbName:         disabledDB,
			CollectionName: disabledColl,
			NamespaceName:  "tenant_a",
		})
		require.ErrorIs(t, merr.CheckRPCCall(createResp.GetStatus(), err), merr.ErrParameterInvalid)
	})
}

func namespaceNames(infos []*milvuspb.NamespaceInfo) []string {
	names := make([]string, 0, len(infos))
	for _, info := range infos {
		names = append(names, info.GetNamespaceName())
	}
	return names
}
