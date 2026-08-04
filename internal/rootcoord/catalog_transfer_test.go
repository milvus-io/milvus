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
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_broadcaster"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestCoreCatalogTransferPrepareFreezesAndDrains(t *testing.T) {
	ctx := context.Background()
	core := newTestCore()
	core.UpdateStateCode(commonpb.StateCode_Healthy)

	done, err := core.transferGate.BeginUserOperation(100, 0)
	require.NoError(t, err)

	errCh := make(chan error, 1)
	go func() {
		status, err := core.CatalogTransferPrepare(ctx, &rootcoordpb.CatalogTransferPrepareRequest{
			TransferId:     "transfer-1",
			TransferEpoch:  10,
			CollectionId:   100,
			DrainTimeoutMs: 500,
		})
		errCh <- merr.CheckRPCCall(status, err)
	}()

	require.Eventually(t, func() bool {
		err := core.transferGate.AllowUserOperation(100, 0)
		return err == errCollectionTransferring
	}, time.Second, 10*time.Millisecond)

	select {
	case err := <-errCh:
		require.Failf(t, "prepare returned before drain finished", "err: %v", err)
	default:
	}

	done()
	require.NoError(t, <-errCh)
	require.ErrorIs(t, core.transferGate.AllowUserOperation(100, 0), errCollectionTransferring)
}

func TestCoreCatalogTransferPrepareHoldsBroadcastDrainLockUntilTransferGateDrains(t *testing.T) {
	mockey.PatchConvey("prepare holds db broadcast lock while waiting for in-flight operations", t, func() {
		ctx := context.Background()
		core := newTestCore()
		core.UpdateStateCode(commonpb.StateCode_Healthy)

		inFlightDone, err := core.transferGate.BeginUserOperation(100, 0)
		require.NoError(t, err)

		lockClosed := make(chan struct{})
		broadcastLock := mock_broadcaster.NewMockBroadcastAPI(t)
		broadcastLock.EXPECT().Close().Run(func() {
			close(lockClosed)
		}).Once()
		mockey.Mock(startBroadcastWithDatabaseLock).Return(broadcastLock, nil).Build()

		errCh := make(chan error, 1)
		go func() {
			status, err := core.CatalogTransferPrepare(ctx, &rootcoordpb.CatalogTransferPrepareRequest{
				TransferId:     "transfer-1",
				TransferEpoch:  10,
				CollectionId:   100,
				DbName:         "db",
				CollectionName: "collection",
				DrainTimeoutMs: 500,
			})
			errCh <- merr.CheckRPCCall(status, err)
		}()

		require.Eventually(t, func() bool {
			return core.transferGate.AllowUserOperation(100, 0) == errCollectionTransferring
		}, time.Second, 10*time.Millisecond)
		select {
		case <-lockClosed:
			require.Fail(t, "broadcast drain lock closed before transfer gate drained")
		default:
		}

		inFlightDone()
		require.NoError(t, <-errCh)
		select {
		case <-lockClosed:
		case <-time.After(time.Second):
			require.Fail(t, "broadcast drain lock was not closed after prepare")
		}
	})
}

func TestCoreCatalogTransferDeactivateInvalidatesSourceCache(t *testing.T) {
	const collectionID = int64(100)

	ctx := context.Background()
	core := newTestCore()
	core.UpdateStateCode(commonpb.StateCode_Healthy)
	core.proxyClientManager = proxyutil.NewProxyClientManager(proxyutil.DefaultProxyCreator)
	require.NoError(t, core.transferGate.Freeze(collectionID, "transfer-1", 10))

	var invalidatedNames []string
	proxy := newMockProxy()
	proxy.InvalidateCollectionMetaCacheFunc = func(ctx context.Context, req *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error) {
		require.Equal(t, "db", req.GetDbName())
		require.Equal(t, collectionID, req.GetCollectionID())
		require.Equal(t, uint64(1234), req.GetBase().GetTimestamp())
		invalidatedNames = append(invalidatedNames, req.GetCollectionName())
		return merr.Success(), nil
	}
	core.proxyClientManager.GetProxyClients().Insert(TestProxyID, proxy)

	status, err := core.CatalogTransferDeactivate(ctx, &rootcoordpb.CatalogTransferDeactivateRequest{
		TransferId:     "transfer-1",
		TransferEpoch:  10,
		CollectionId:   collectionID,
		DbName:         "db",
		CollectionName: "collection",
		Aliases:        []string{"alias-1", "collection", "alias-1"},
		CacheExpireTs:  1234,
	})
	require.NoError(t, merr.CheckRPCCall(status, err))
	require.Equal(t, []string{"collection", "alias-1"}, invalidatedNames)
	require.ErrorIs(t, core.transferGate.AllowUserOperation(collectionID, 0), errCollectionTransferredOut)
}

func TestCoreCatalogTransferRejectsTruncateWhenCollectionFrozen(t *testing.T) {
	mockey.PatchConvey("truncate is blocked by transfer gate", t, func() {
		core := newTestCore()
		core.UpdateStateCode(commonpb.StateCode_Healthy)
		require.NoError(t, core.transferGate.Freeze(100, "transfer-1", 10))
		core.meta = mockMetaTable{
			GetCollectionByNameFunc: func(ctx context.Context, collectionName string, ts Timestamp, allowUnavailable bool) (*model.Collection, error) {
				return &model.Collection{
					CollectionID:        100,
					DBID:                1,
					DBName:              "db",
					Name:                "collection",
					VirtualChannelNames: []string{"by-dev-rootcoord-dml_0_100v0"},
				}, nil
			},
		}
		broadcastLock := mock_broadcaster.NewMockBroadcastAPI(t)
		broadcastLock.EXPECT().Close().Maybe()
		mockey.Mock((*Core).startBroadcastWithCollectionLock).Return(broadcastLock, nil).Build()

		err := core.broadcastTruncateCollection(context.Background(), &milvuspb.TruncateCollectionRequest{
			DbName:         "db",
			CollectionName: "collection",
		})
		require.ErrorIs(t, err, errCollectionTransferring)
	})
}

func TestCoreCatalogTransferRejectsDynamicSchemaAlterWhenCollectionFrozen(t *testing.T) {
	mockey.PatchConvey("dynamic schema alter is blocked by transfer gate", t, func() {
		core := newTestCore()
		core.UpdateStateCode(commonpb.StateCode_Healthy)
		require.NoError(t, core.transferGate.Freeze(100, "transfer-1", 10))
		core.meta = mockMetaTable{
			GetCollectionByNameFunc: func(ctx context.Context, collectionName string, ts Timestamp, allowUnavailable bool) (*model.Collection, error) {
				return &model.Collection{
					CollectionID:        100,
					DBID:                1,
					DBName:              "db",
					Name:                "collection",
					VirtualChannelNames: []string{"by-dev-rootcoord-dml_0_100v0"},
				}, nil
			},
		}
		broadcastLock := mock_broadcaster.NewMockBroadcastAPI(t)
		broadcastLock.EXPECT().Close().Maybe()
		mockey.Mock((*Core).startBroadcastWithCollectionLock).Return(broadcastLock, nil).Build()

		err := core.broadcastAlterCollectionForAlterDynamicField(context.Background(), &milvuspb.AlterCollectionRequest{
			DbName:         "db",
			CollectionName: "collection",
			Properties: []*commonpb.KeyValuePair{{
				Key:   common.EnableDynamicSchemaKey,
				Value: "true",
			}},
		}, true)
		require.ErrorIs(t, err, errCollectionTransferring)
	})
}

func TestCoreCatalogTransferDescribeAliasRejectsFrozenCollection(t *testing.T) {
	core := newTestCore()
	core.UpdateStateCode(commonpb.StateCode_Healthy)
	require.NoError(t, core.transferGate.Freeze(100, "transfer-1", 10))
	core.meta = mockMetaTable{
		DescribeAliasFunc: func(ctx context.Context, dbName, alias string, ts Timestamp) (string, error) {
			return "collection", nil
		},
		GetCollectionByNameFunc: func(ctx context.Context, collectionName string, ts Timestamp, allowUnavailable bool) (*model.Collection, error) {
			return &model.Collection{CollectionID: 100, DBName: "db", Name: "collection"}, nil
		},
	}

	resp, err := core.DescribeAlias(context.Background(), &milvuspb.DescribeAliasRequest{
		DbName: "db",
		Alias:  "alias-1",
	})
	require.ErrorContains(t, merr.CheckRPCCall(resp.GetStatus(), err), errCollectionTransferring.Error())
}

func TestCoreCatalogTransferListAliasesFiltersFrozenCollections(t *testing.T) {
	core := newTestCore()
	core.UpdateStateCode(commonpb.StateCode_Healthy)
	require.NoError(t, core.transferGate.Freeze(100, "transfer-1", 10))
	core.meta = mockMetaTable{
		ListAliasesFunc: func(ctx context.Context, dbName, collectionName string, ts Timestamp) ([]string, error) {
			return []string{"alias-1", "alias-2"}, nil
		},
		DescribeAliasFunc: func(ctx context.Context, dbName, alias string, ts Timestamp) (string, error) {
			if alias == "alias-1" {
				return "collection-1", nil
			}
			return "collection-2", nil
		},
		GetCollectionByNameFunc: func(ctx context.Context, collectionName string, ts Timestamp, allowUnavailable bool) (*model.Collection, error) {
			if collectionName == "collection-1" {
				return &model.Collection{CollectionID: 100, DBName: "db", Name: collectionName}, nil
			}
			return &model.Collection{CollectionID: 200, DBName: "db", Name: collectionName}, nil
		},
	}

	resp, err := core.ListAliases(context.Background(), &milvuspb.ListAliasesRequest{DbName: "db"})
	require.NoError(t, merr.CheckRPCCall(resp.GetStatus(), err))
	require.Equal(t, []string{"alias-2"}, resp.GetAliases())
}

func TestCoreCatalogTransferDeactivateRemovesSourceLiveMeta(t *testing.T) {
	const collectionID = int64(100)

	ctx := context.Background()
	core := newTestCore()
	core.UpdateStateCode(commonpb.StateCode_Healthy)
	core.proxyClientManager = proxyutil.NewProxyClientManager(proxyutil.DefaultProxyCreator)
	require.NoError(t, core.transferGate.Freeze(collectionID, "transfer-1", 10))

	var deactivatedCollectionID int64
	core.meta = mockMetaTable{
		DeactivateTransferredCollectionFunc: func(ctx context.Context, collectionID int64) error {
			deactivatedCollectionID = collectionID
			return nil
		},
	}

	status, err := core.CatalogTransferDeactivate(ctx, &rootcoordpb.CatalogTransferDeactivateRequest{
		TransferId:     "transfer-1",
		TransferEpoch:  10,
		CollectionId:   collectionID,
		DbName:         "db",
		CollectionName: "collection",
		CacheExpireTs:  1234,
	})
	require.NoError(t, merr.CheckRPCCall(status, err))
	require.Equal(t, collectionID, deactivatedCollectionID)
}

func TestCoreCatalogTransferApplyLoadsTransferredCollectionAndInvalidatesTargetCache(t *testing.T) {
	const collectionID = int64(100)

	ctx := context.Background()
	core := newTestCore()
	core.UpdateStateCode(commonpb.StateCode_Healthy)
	core.proxyClientManager = proxyutil.NewProxyClientManager(proxyutil.DefaultProxyCreator)

	var applied *model.Collection
	var verified *model.Collection
	core.meta = mockMetaTable{
		VerifyTransferredCollectionFunc: func(ctx context.Context, coll *model.Collection) error {
			verified = coll.Clone()
			return nil
		},
		ApplyTransferredCollectionFunc: func(ctx context.Context, coll *model.Collection) error {
			applied = coll.Clone()
			return nil
		},
	}

	var invalidatedNames []string
	proxy := newMockProxy()
	proxy.InvalidateCollectionMetaCacheFunc = func(ctx context.Context, req *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error) {
		require.Equal(t, "db", req.GetDbName())
		require.Equal(t, collectionID, req.GetCollectionID())
		require.Equal(t, uint64(1234), req.GetBase().GetTimestamp())
		invalidatedNames = append(invalidatedNames, req.GetCollectionName())
		return merr.Success(), nil
	}
	core.proxyClientManager.GetProxyClients().Insert(TestProxyID, proxy)

	status, err := core.CatalogTransferApply(ctx, &rootcoordpb.CatalogTransferApplyRequest{
		TransferId:    "transfer-1",
		TransferEpoch: 10,
		Collection: &etcdpb.CollectionInfo{
			ID:   collectionID,
			DbId: 1,
			Schema: &schemapb.CollectionSchema{
				Name:   "collection",
				DbName: "db",
			},
			State:                etcdpb.CollectionState_CollectionCreated,
			ShardsNum:            1,
			VirtualChannelNames:  []string{"by-dev-rootcoord-dml_0_100v0"},
			PhysicalChannelNames: []string{"by-dev-rootcoord-dml_0"},
		},
		Partitions: []*etcdpb.PartitionInfo{
			{
				PartitionID:   101,
				PartitionName: "_default",
				CollectionId:  collectionID,
				State:         etcdpb.PartitionState_PartitionCreated,
			},
		},
		Aliases: []*etcdpb.AliasInfo{
			{
				AliasName:    "alias-1",
				CollectionId: collectionID,
				State:        etcdpb.AliasState_AliasCreated,
			},
			{
				AliasName:    "alias-dropped",
				CollectionId: collectionID,
				State:        etcdpb.AliasState_AliasDropped,
			},
		},
		CacheExpireTs: 1234,
	})
	require.NoError(t, merr.CheckRPCCall(status, err))
	require.NotNil(t, verified)
	require.Equal(t, collectionID, verified.CollectionID)
	require.NotNil(t, applied)
	require.Equal(t, collectionID, applied.CollectionID)
	require.Equal(t, "db", applied.DBName)
	require.Equal(t, []*model.Partition{
		{
			PartitionID:   101,
			PartitionName: "_default",
			CollectionID:  collectionID,
			State:         etcdpb.PartitionState_PartitionCreated,
		},
	}, applied.Partitions)
	require.Equal(t, []string{"alias-1"}, applied.Aliases)
	require.Equal(t, []string{"collection", "alias-1"}, invalidatedNames)
}

func TestCoreCatalogTransferApplyRejectsDurableCatalogMismatch(t *testing.T) {
	ctx := context.Background()
	core := newTestCore()
	core.UpdateStateCode(commonpb.StateCode_Healthy)
	core.proxyClientManager = proxyutil.NewProxyClientManager(proxyutil.DefaultProxyCreator)

	applied := false
	core.meta = mockMetaTable{
		VerifyTransferredCollectionFunc: func(ctx context.Context, coll *model.Collection) error {
			return assertTransferGuardErr("durable target mismatch")
		},
		ApplyTransferredCollectionFunc: func(ctx context.Context, coll *model.Collection) error {
			applied = true
			return nil
		},
	}

	status, err := core.CatalogTransferApply(ctx, &rootcoordpb.CatalogTransferApplyRequest{
		TransferId:    "transfer-1",
		TransferEpoch: 10,
		Collection: &etcdpb.CollectionInfo{
			ID:   100,
			DbId: 1,
			Schema: &schemapb.CollectionSchema{
				Name:   "collection",
				DbName: "db",
			},
			State:     etcdpb.CollectionState_CollectionCreated,
			ShardsNum: 1,
		},
		CacheExpireTs: 1234,
	})
	require.ErrorContains(t, merr.CheckRPCCall(status, err), "durable target mismatch")
	require.False(t, applied)
}

func TestCoreCatalogTransferApplyRequiresTransferIdentity(t *testing.T) {
	core := newTestCore()
	core.UpdateStateCode(commonpb.StateCode_Healthy)

	status, err := core.CatalogTransferApply(context.Background(), &rootcoordpb.CatalogTransferApplyRequest{
		Collection: &etcdpb.CollectionInfo{
			ID: 100,
			Schema: &schemapb.CollectionSchema{
				Name:   "collection",
				DbName: "db",
			},
			State: etcdpb.CollectionState_CollectionCreated,
		},
	})
	require.ErrorIs(t, merr.CheckRPCCall(status, err), merr.ErrParameterInvalid)
}

func TestCoreCatalogTransferAbortUnfreezesCollection(t *testing.T) {
	ctx := context.Background()
	core := newTestCore()
	core.UpdateStateCode(commonpb.StateCode_Healthy)
	require.NoError(t, core.transferGate.Freeze(100, "transfer-1", 10))

	status, err := core.CatalogTransferAbort(ctx, &rootcoordpb.CatalogTransferAbortRequest{
		TransferId:    "transfer-1",
		TransferEpoch: 10,
		CollectionId:  100,
	})
	require.NoError(t, merr.CheckRPCCall(status, err))
	require.NoError(t, core.transferGate.AllowUserOperation(100, 0))
}

func TestCoreCatalogTransferRejectsUnhealthyRootCoord(t *testing.T) {
	core := newTestCore()
	core.UpdateStateCode(commonpb.StateCode_Abnormal)

	status, err := core.CatalogTransferPrepare(context.Background(), &rootcoordpb.CatalogTransferPrepareRequest{
		TransferId:     "transfer-1",
		TransferEpoch:  10,
		CollectionId:   100,
		DrainTimeoutMs: 1,
	})
	require.ErrorIs(t, merr.CheckRPCCall(status, err), merr.ErrServiceNotReady)
}
