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
	"os"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	pb "github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/tso"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

const (
	TestProxyID     = 100
	TestRootCoordID = 200
)

type mockMetaTable struct {
	IMetaTable
	ListDatabasesFunc                func(ctx context.Context, ts Timestamp) ([]*model.Database, error)
	ListCollectionsFunc              func(ctx context.Context, ts Timestamp) ([]*model.Collection, error)
	AddCollectionFunc                func(ctx context.Context, coll *model.Collection) error
	GetCollectionByNameFunc          func(ctx context.Context, collectionName string, ts Timestamp) (*model.Collection, error)
	GetCollectionByIDFunc            func(ctx context.Context, collectionID UniqueID, ts Timestamp, allowUnavailable bool) (*model.Collection, error)
	ChangeCollectionStateFunc        func(ctx context.Context, collectionID UniqueID, state pb.CollectionState, ts Timestamp) error
	RemoveCollectionFunc             func(ctx context.Context, collectionID UniqueID, ts Timestamp) error
	AddPartitionFunc                 func(ctx context.Context, partition *model.Partition) error
	ChangePartitionStateFunc         func(ctx context.Context, collectionID UniqueID, partitionID UniqueID, state pb.PartitionState, ts Timestamp) error
	RemovePartitionFunc              func(ctx context.Context, collectionID UniqueID, partitionID UniqueID, ts Timestamp) error
	CreateAliasFunc                  func(ctx context.Context, dbName string, alias string, collectionName string, ts Timestamp) error
	AlterAliasFunc                   func(ctx context.Context, dbName string, alias string, collectionName string, ts Timestamp) error
	DropAliasFunc                    func(ctx context.Context, dbName string, alias string, ts Timestamp) error
	IsAliasFunc                      func(dbName, name string) bool
	ListAliasesByIDFunc              func(collID UniqueID) []string
	GetCollectionIDByNameFunc        func(name string) (UniqueID, error)
	GetPartitionByNameFunc           func(collID UniqueID, partitionName string, ts Timestamp) (UniqueID, error)
	GetCollectionVirtualChannelsFunc func(colID int64) []string
	AlterCollectionFunc              func(ctx context.Context, oldColl *model.Collection, newColl *model.Collection, ts Timestamp) error
	RenameCollectionFunc             func(ctx context.Context, oldName string, newName string, ts Timestamp) error
}

func (m mockMetaTable) ListDatabases(ctx context.Context, ts typeutil.Timestamp) ([]*model.Database, error) {
	return m.ListDatabasesFunc(ctx, ts)
}

func (m mockMetaTable) ListCollections(ctx context.Context, dbName string, ts Timestamp, onlyAvail bool) ([]*model.Collection, error) {
	return m.ListCollectionsFunc(ctx, ts)
}

func (m mockMetaTable) AddCollection(ctx context.Context, coll *model.Collection) error {
	return m.AddCollectionFunc(ctx, coll)
}

func (m mockMetaTable) GetCollectionByName(ctx context.Context, dbName string, collectionName string, ts Timestamp) (*model.Collection, error) {
	return m.GetCollectionByNameFunc(ctx, collectionName, ts)
}

func (m mockMetaTable) GetCollectionByID(ctx context.Context, dbName string, collectionID UniqueID, ts Timestamp, allowUnavailable bool) (*model.Collection, error) {
	return m.GetCollectionByIDFunc(ctx, collectionID, ts, allowUnavailable)
}

func (m mockMetaTable) ChangeCollectionState(ctx context.Context, collectionID UniqueID, state pb.CollectionState, ts Timestamp) error {
	return m.ChangeCollectionStateFunc(ctx, collectionID, state, ts)
}

func (m mockMetaTable) RemoveCollection(ctx context.Context, collectionID UniqueID, ts Timestamp) error {
	return m.RemoveCollectionFunc(ctx, collectionID, ts)
}

func (m mockMetaTable) AddPartition(ctx context.Context, partition *model.Partition) error {
	return m.AddPartitionFunc(ctx, partition)
}

func (m mockMetaTable) ChangePartitionState(ctx context.Context, collectionID UniqueID, partitionID UniqueID, state pb.PartitionState, ts Timestamp) error {
	return m.ChangePartitionStateFunc(ctx, collectionID, partitionID, state, ts)
}

func (m mockMetaTable) RemovePartition(ctx context.Context, dbID int64, collectionID UniqueID, partitionID UniqueID, ts Timestamp) error {
	return m.RemovePartitionFunc(ctx, collectionID, partitionID, ts)
}

func (m mockMetaTable) CreateAlias(ctx context.Context, dbName string, alias string, collectionName string, ts Timestamp) error {
	return m.CreateAliasFunc(ctx, dbName, alias, collectionName, ts)
}

func (m mockMetaTable) AlterAlias(ctx context.Context, dbName, alias string, collectionName string, ts Timestamp) error {
	return m.AlterAliasFunc(ctx, dbName, alias, collectionName, ts)
}

func (m mockMetaTable) DropAlias(ctx context.Context, dbName, alias string, ts Timestamp) error {
	return m.DropAliasFunc(ctx, dbName, alias, ts)
}

func (m mockMetaTable) IsAlias(dbName, name string) bool {
	return m.IsAliasFunc(dbName, name)
}

func (m mockMetaTable) ListAliasesByID(collID UniqueID) []string {
	return m.ListAliasesByIDFunc(collID)
}

func (m mockMetaTable) AlterCollection(ctx context.Context, oldColl *model.Collection, newColl *model.Collection, ts Timestamp) error {
	return m.AlterCollectionFunc(ctx, oldColl, newColl, ts)
}

func (m *mockMetaTable) RenameCollection(ctx context.Context, dbName string, oldName string, newName string, ts Timestamp) error {
	return m.RenameCollectionFunc(ctx, oldName, newName, ts)
}

func (m mockMetaTable) GetCollectionIDByName(name string) (UniqueID, error) {
	return m.GetCollectionIDByNameFunc(name)
}

func (m mockMetaTable) GetPartitionByName(collID UniqueID, partitionName string, ts Timestamp) (UniqueID, error) {
	return m.GetPartitionByNameFunc(collID, partitionName, ts)
}

func (m mockMetaTable) GetCollectionVirtualChannels(colID int64) []string {
	return m.GetCollectionVirtualChannelsFunc(colID)
}

func newMockMetaTable() *mockMetaTable {
	return &mockMetaTable{}
}

//type mockIndexCoord struct {
//	types.IndexCoord
//	GetComponentStatesFunc   func(ctx context.Context) (*milvuspb.ComponentStates, error)
//	GetSegmentIndexStateFunc func(ctx context.Context, req *indexpb.GetSegmentIndexStateRequest) (*indexpb.GetSegmentIndexStateResponse, error)
//	DropIndexFunc            func(ctx context.Context, req *indexpb.DropIndexRequest) (*commonpb.Status, error)
//}
//
//func newMockIndexCoord() *mockIndexCoord {
//	return &mockIndexCoord{}
//}
//
//func (m mockIndexCoord) GetComponentStates(ctx context.Context) (*milvuspb.ComponentStates, error) {
//	return m.GetComponentStatesFunc(ctx)
//}
//
//func (m mockIndexCoord) GetSegmentIndexState(ctx context.Context, req *indexpb.GetSegmentIndexStateRequest) (*indexpb.GetSegmentIndexStateResponse, error) {
//	return m.GetSegmentIndexStateFunc(ctx, req)
//}
//
//func (m mockIndexCoord) DropIndex(ctx context.Context, req *indexpb.DropIndexRequest) (*commonpb.Status, error) {
//	return m.DropIndexFunc(ctx, req)
//}

type mockDataCoord struct {
	types.DataCoord
	GetComponentStatesFunc         func(ctx context.Context) (*milvuspb.ComponentStates, error)
	WatchChannelsFunc              func(ctx context.Context, req *datapb.WatchChannelsRequest) (*datapb.WatchChannelsResponse, error)
	FlushFunc                      func(ctx context.Context, req *datapb.FlushRequest) (*datapb.FlushResponse, error)
	ImportFunc                     func(ctx context.Context, req *datapb.ImportTaskRequest) (*datapb.ImportTaskResponse, error)
	UnsetIsImportingStateFunc      func(ctx context.Context, req *datapb.UnsetIsImportingStateRequest) (*commonpb.Status, error)
	broadCastAlteredCollectionFunc func(ctx context.Context, req *datapb.AlterCollectionRequest) (*commonpb.Status, error)
	GetSegmentIndexStateFunc       func(ctx context.Context, req *indexpb.GetSegmentIndexStateRequest) (*indexpb.GetSegmentIndexStateResponse, error)
	DropIndexFunc                  func(ctx context.Context, req *indexpb.DropIndexRequest) (*commonpb.Status, error)
}

func newMockDataCoord() *mockDataCoord {
	return &mockDataCoord{}
}

func (m *mockDataCoord) GetComponentStates(ctx context.Context) (*milvuspb.ComponentStates, error) {
	return m.GetComponentStatesFunc(ctx)
}

func (m *mockDataCoord) WatchChannels(ctx context.Context, req *datapb.WatchChannelsRequest) (*datapb.WatchChannelsResponse, error) {
	return m.WatchChannelsFunc(ctx, req)
}

func (m *mockDataCoord) Flush(ctx context.Context, req *datapb.FlushRequest) (*datapb.FlushResponse, error) {
	return m.FlushFunc(ctx, req)
}

func (m *mockDataCoord) Import(ctx context.Context, req *datapb.ImportTaskRequest) (*datapb.ImportTaskResponse, error) {
	return m.ImportFunc(ctx, req)
}

func (m *mockDataCoord) UnsetIsImportingState(ctx context.Context, req *datapb.UnsetIsImportingStateRequest) (*commonpb.Status, error) {
	return m.UnsetIsImportingStateFunc(ctx, req)
}

func (m *mockDataCoord) BroadcastAlteredCollection(ctx context.Context, req *datapb.AlterCollectionRequest) (*commonpb.Status, error) {
	return m.broadCastAlteredCollectionFunc(ctx, req)
}

func (m *mockDataCoord) CheckHealth(ctx context.Context, req *milvuspb.CheckHealthRequest) (*milvuspb.CheckHealthResponse, error) {
	return &milvuspb.CheckHealthResponse{
		IsHealthy: true,
	}, nil
}

func (m *mockDataCoord) GetSegmentIndexState(ctx context.Context, req *indexpb.GetSegmentIndexStateRequest) (*indexpb.GetSegmentIndexStateResponse, error) {
	return m.GetSegmentIndexStateFunc(ctx, req)
}

func (m *mockDataCoord) DropIndex(ctx context.Context, req *indexpb.DropIndexRequest) (*commonpb.Status, error) {
	return m.DropIndexFunc(ctx, req)
}

type mockQueryCoord struct {
	types.QueryCoord
	GetSegmentInfoFunc     func(ctx context.Context, req *querypb.GetSegmentInfoRequest) (*querypb.GetSegmentInfoResponse, error)
	GetComponentStatesFunc func(ctx context.Context) (*milvuspb.ComponentStates, error)
	ReleaseCollectionFunc  func(ctx context.Context, req *querypb.ReleaseCollectionRequest) (*commonpb.Status, error)
}

func (m mockQueryCoord) GetSegmentInfo(ctx context.Context, req *querypb.GetSegmentInfoRequest) (*querypb.GetSegmentInfoResponse, error) {
	return m.GetSegmentInfoFunc(ctx, req)
}

func (m mockQueryCoord) GetComponentStates(ctx context.Context) (*milvuspb.ComponentStates, error) {
	return m.GetComponentStatesFunc(ctx)
}

func (m mockQueryCoord) ReleaseCollection(ctx context.Context, req *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	return m.ReleaseCollectionFunc(ctx, req)
}

func newMockQueryCoord() *mockQueryCoord {
	return &mockQueryCoord{}
}

func newMockIDAllocator() *allocator.MockGIDAllocator {
	r := allocator.NewMockGIDAllocator()
	r.AllocF = func(count uint32) (UniqueID, UniqueID, error) {
		return 0, 0, nil
	}
	r.AllocOneF = func() (UniqueID, error) {
		return 0, nil
	}
	return r
}

func newMockTsoAllocator() *tso.MockAllocator {
	r := tso.NewMockAllocator()
	r.GenerateTSOF = func(count uint32) (uint64, error) {
		return 0, nil
	}
	return r
}

type mockProxy struct {
	types.Proxy
	InvalidateCollectionMetaCacheFunc func(ctx context.Context, request *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error)
	InvalidateCredentialCacheFunc     func(ctx context.Context, request *proxypb.InvalidateCredCacheRequest) (*commonpb.Status, error)
	RefreshPolicyInfoCacheFunc        func(ctx context.Context, request *proxypb.RefreshPolicyInfoCacheRequest) (*commonpb.Status, error)
	GetComponentStatesFunc            func(ctx context.Context) (*milvuspb.ComponentStates, error)
}

func (m mockProxy) InvalidateCollectionMetaCache(ctx context.Context, request *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error) {
	return m.InvalidateCollectionMetaCacheFunc(ctx, request)
}

func (m mockProxy) InvalidateCredentialCache(ctx context.Context, request *proxypb.InvalidateCredCacheRequest) (*commonpb.Status, error) {
	return m.InvalidateCredentialCacheFunc(ctx, request)
}

func (m mockProxy) RefreshPolicyInfoCache(ctx context.Context, request *proxypb.RefreshPolicyInfoCacheRequest) (*commonpb.Status, error) {
	return m.RefreshPolicyInfoCacheFunc(ctx, request)
}

func (m mockProxy) GetComponentStates(ctx context.Context) (*milvuspb.ComponentStates, error) {
	return m.GetComponentStatesFunc(ctx)
}

func newMockProxy() *mockProxy {
	r := &mockProxy{}
	r.InvalidateCollectionMetaCacheFunc = func(ctx context.Context, request *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error) {
		return succStatus(), nil
	}
	return r
}

func newTestCore(opts ...Opt) *Core {
	c := &Core{
		session: &sessionutil.Session{ServerID: TestRootCoordID},
	}
	executor := newMockStepExecutor()
	executor.AddStepsFunc = func(s *stepStack) {
		// no schedule, execute directly.
		s.Execute(context.Background())
	}
	executor.StopFunc = func() {}
	c.stepExecutor = executor
	for _, opt := range opts {
		opt(c)
	}
	return c
}

func withValidProxyManager() Opt {
	return func(c *Core) {
		c.proxyClientManager = &proxyClientManager{
			proxyClient: make(map[UniqueID]types.Proxy),
		}
		p := newMockProxy()
		p.InvalidateCollectionMetaCacheFunc = func(ctx context.Context, request *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error) {
			return succStatus(), nil
		}
		p.GetComponentStatesFunc = func(ctx context.Context) (*milvuspb.ComponentStates, error) {
			return &milvuspb.ComponentStates{
				State:  &milvuspb.ComponentInfo{StateCode: commonpb.StateCode_Healthy},
				Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			}, nil
		}
		c.proxyClientManager.proxyClient[TestProxyID] = p
	}
}

func withInvalidProxyManager() Opt {
	return func(c *Core) {
		c.proxyClientManager = &proxyClientManager{
			proxyClient: make(map[UniqueID]types.Proxy),
		}
		p := newMockProxy()
		p.InvalidateCollectionMetaCacheFunc = func(ctx context.Context, request *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error) {
			return succStatus(), errors.New("error mock InvalidateCollectionMetaCache")
		}
		p.GetComponentStatesFunc = func(ctx context.Context) (*milvuspb.ComponentStates, error) {
			return &milvuspb.ComponentStates{
				State:  &milvuspb.ComponentInfo{StateCode: commonpb.StateCode_Abnormal},
				Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			}, nil
		}
		c.proxyClientManager.proxyClient[TestProxyID] = p
	}
}

func withDropIndex() Opt {
	return func(c *Core) {
		c.broker = &mockBroker{
			DropCollectionIndexFunc: func(ctx context.Context, collID UniqueID, partIDs []UniqueID) error {
				return nil
			},
		}
	}
}

func withMeta(meta IMetaTable) Opt {
	return func(c *Core) {
		c.meta = meta
	}
}

func withInvalidMeta() Opt {
	meta := newMockMetaTable()
	meta.ListDatabasesFunc = func(ctx context.Context, ts Timestamp) ([]*model.Database, error) {
		return nil, errors.New("error mock ListDatabases")
	}
	meta.ListCollectionsFunc = func(ctx context.Context, ts Timestamp) ([]*model.Collection, error) {
		return nil, errors.New("error mock ListCollections")
	}
	meta.GetCollectionByNameFunc = func(ctx context.Context, collectionName string, ts Timestamp) (*model.Collection, error) {
		return nil, errors.New("error mock GetCollectionByName")
	}
	meta.GetCollectionByIDFunc = func(ctx context.Context, collectionID typeutil.UniqueID, ts Timestamp, allowUnavailable bool) (*model.Collection, error) {
		return nil, errors.New("error mock GetCollectionByID")
	}
	meta.AddPartitionFunc = func(ctx context.Context, partition *model.Partition) error {
		return errors.New("error mock AddPartition")
	}
	meta.ChangePartitionStateFunc = func(ctx context.Context, collectionID UniqueID, partitionID UniqueID, state pb.PartitionState, ts Timestamp) error {
		return errors.New("error mock ChangePartitionState")
	}
	meta.CreateAliasFunc = func(ctx context.Context, dbName string, alias string, collectionName string, ts Timestamp) error {
		return errors.New("error mock CreateAlias")
	}
	meta.AlterAliasFunc = func(ctx context.Context, dbName string, alias string, collectionName string, ts Timestamp) error {
		return errors.New("error mock AlterAlias")
	}
	meta.DropAliasFunc = func(ctx context.Context, dbName string, alias string, ts Timestamp) error {
		return errors.New("error mock DropAlias")
	}
	return withMeta(meta)
}

func withIDAllocator(idAllocator allocator.Interface) Opt {
	return func(c *Core) {
		c.idAllocator = idAllocator
	}
}

func withValidIDAllocator() Opt {
	idAllocator := newMockIDAllocator()
	idAllocator.AllocOneF = func() (UniqueID, error) {
		return rand.Int63(), nil
	}
	return withIDAllocator(idAllocator)
}

func withInvalidIDAllocator() Opt {
	idAllocator := newMockIDAllocator()
	idAllocator.AllocOneF = func() (UniqueID, error) {
		return -1, errors.New("error mock AllocOne")
	}
	idAllocator.AllocF = func(count uint32) (UniqueID, UniqueID, error) {
		return -1, -1, errors.New("error mock Alloc")
	}
	return withIDAllocator(idAllocator)
}

func withQueryCoord(qc types.QueryCoord) Opt {
	return func(c *Core) {
		c.queryCoord = qc
	}
}

func withUnhealthyQueryCoord() Opt {
	qc := &mocks.MockQueryCoord{}
	qc.EXPECT().GetComponentStates(mock.Anything).Return(
		&milvuspb.ComponentStates{
			State:  &milvuspb.ComponentInfo{StateCode: commonpb.StateCode_Abnormal},
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "error mock GetComponentStates"),
		}, retry.Unrecoverable(errors.New("error mock GetComponentStates")),
	)
	return withQueryCoord(qc)
}

func withInvalidQueryCoord() Opt {
	qc := &mocks.MockQueryCoord{}
	qc.EXPECT().GetComponentStates(mock.Anything).Return(
		&milvuspb.ComponentStates{
			State:  &milvuspb.ComponentInfo{StateCode: commonpb.StateCode_Healthy},
			Status: succStatus(),
		}, nil,
	)
	qc.EXPECT().ReleaseCollection(mock.Anything, mock.Anything).Return(
		nil, errors.New("error mock ReleaseCollection"),
	)

	qc.EXPECT().GetSegmentInfo(mock.Anything, mock.Anything).Return(
		nil, errors.New("error mock GetSegmentInfo"),
	)

	return withQueryCoord(qc)
}

func withFailedQueryCoord() Opt {
	qc := &mocks.MockQueryCoord{}
	qc.EXPECT().GetComponentStates(mock.Anything).Return(
		&milvuspb.ComponentStates{
			State:  &milvuspb.ComponentInfo{StateCode: commonpb.StateCode_Healthy},
			Status: succStatus(),
		}, nil,
	)
	qc.EXPECT().ReleaseCollection(mock.Anything, mock.Anything).Return(
		failStatus(commonpb.ErrorCode_UnexpectedError, "mock release collection error"), nil,
	)

	qc.EXPECT().GetSegmentInfo(mock.Anything, mock.Anything).Return(
		&querypb.GetSegmentInfoResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "mock get segment info error"),
		}, nil,
	)

	return withQueryCoord(qc)
}

func withValidQueryCoord() Opt {
	qc := &mocks.MockQueryCoord{}
	qc.EXPECT().GetComponentStates(mock.Anything).Return(
		&milvuspb.ComponentStates{
			State:  &milvuspb.ComponentInfo{StateCode: commonpb.StateCode_Healthy},
			Status: succStatus(),
		}, nil,
	)
	qc.EXPECT().ReleaseCollection(mock.Anything, mock.Anything).Return(
		succStatus(), nil,
	)

	qc.EXPECT().ReleasePartitions(mock.Anything, mock.Anything).Return(
		succStatus(), nil,
	)

	qc.EXPECT().GetSegmentInfo(mock.Anything, mock.Anything).Return(
		&querypb.GetSegmentInfoResponse{
			Status: succStatus(),
		}, nil,
	)

	qc.EXPECT().SyncNewCreatedPartition(mock.Anything, mock.Anything).Return(
		succStatus(), nil,
	)

	return withQueryCoord(qc)
}

// cleanTestEnv clean test environment, for example, files generated by rocksmq.
func cleanTestEnv() {
	path := "/tmp/milvus"
	if err := os.RemoveAll(path); err != nil {
		log.Warn("failed to clean test directories", zap.Error(err), zap.String("path", path))
	}
	log.Debug("clean test environment", zap.String("path", path))
}

func withTtSynchronizer(ticker *timetickSync) Opt {
	return func(c *Core) {
		c.chanTimeTick = ticker
	}
}

func newRocksMqTtSynchronizer() *timetickSync {
	Params.Init()
	paramtable.Get().Save(Params.RootCoordCfg.DmlChannelNum.Key, "4")
	ctx := context.Background()
	factory := dependency.NewDefaultFactory(true)
	chans := map[UniqueID][]string{}
	ticker := newTimeTickSync(ctx, TestRootCoordID, factory, chans)
	return ticker
}

// cleanTestEnv should be called if tested with this option.
func withRocksMqTtSynchronizer() Opt {
	ticker := newRocksMqTtSynchronizer()
	return withTtSynchronizer(ticker)
}

func withDataCoord(dc types.DataCoord) Opt {
	return func(c *Core) {
		c.dataCoord = dc
	}
}

func withUnhealthyDataCoord() Opt {
	dc := newMockDataCoord()
	dc.GetComponentStatesFunc = func(ctx context.Context) (*milvuspb.ComponentStates, error) {
		return &milvuspb.ComponentStates{
			State:  &milvuspb.ComponentInfo{StateCode: commonpb.StateCode_Abnormal},
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "error mock GetComponentStates"),
		}, retry.Unrecoverable(errors.New("error mock GetComponentStates"))
	}
	return withDataCoord(dc)
}

func withInvalidDataCoord() Opt {
	dc := newMockDataCoord()
	dc.GetComponentStatesFunc = func(ctx context.Context) (*milvuspb.ComponentStates, error) {
		return &milvuspb.ComponentStates{
			State:  &milvuspb.ComponentInfo{StateCode: commonpb.StateCode_Healthy},
			Status: succStatus(),
		}, nil
	}
	dc.WatchChannelsFunc = func(ctx context.Context, req *datapb.WatchChannelsRequest) (*datapb.WatchChannelsResponse, error) {
		return nil, errors.New("error mock WatchChannels")
	}
	dc.WatchChannelsFunc = func(ctx context.Context, req *datapb.WatchChannelsRequest) (*datapb.WatchChannelsResponse, error) {
		return nil, errors.New("error mock WatchChannels")
	}
	dc.FlushFunc = func(ctx context.Context, req *datapb.FlushRequest) (*datapb.FlushResponse, error) {
		return nil, errors.New("error mock Flush")
	}
	dc.ImportFunc = func(ctx context.Context, req *datapb.ImportTaskRequest) (*datapb.ImportTaskResponse, error) {
		return nil, errors.New("error mock Import")
	}
	dc.UnsetIsImportingStateFunc = func(ctx context.Context, req *datapb.UnsetIsImportingStateRequest) (*commonpb.Status, error) {
		return nil, errors.New("error mock UnsetIsImportingState")
	}
	dc.broadCastAlteredCollectionFunc = func(ctx context.Context, req *datapb.AlterCollectionRequest) (*commonpb.Status, error) {
		return nil, errors.New("error mock broadCastAlteredCollection")
	}
	dc.GetSegmentIndexStateFunc = func(ctx context.Context, req *indexpb.GetSegmentIndexStateRequest) (*indexpb.GetSegmentIndexStateResponse, error) {
		return nil, errors.New("error mock GetSegmentIndexStateFunc")
	}
	dc.DropIndexFunc = func(ctx context.Context, req *indexpb.DropIndexRequest) (*commonpb.Status, error) {
		return nil, errors.New("error mock DropIndexFunc")
	}
	return withDataCoord(dc)
}

func withFailedDataCoord() Opt {
	dc := newMockDataCoord()
	dc.GetComponentStatesFunc = func(ctx context.Context) (*milvuspb.ComponentStates, error) {
		return &milvuspb.ComponentStates{
			State:  &milvuspb.ComponentInfo{StateCode: commonpb.StateCode_Healthy},
			Status: succStatus(),
		}, nil
	}
	dc.WatchChannelsFunc = func(ctx context.Context, req *datapb.WatchChannelsRequest) (*datapb.WatchChannelsResponse, error) {
		return &datapb.WatchChannelsResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "mock watch channels error"),
		}, nil
	}
	dc.FlushFunc = func(ctx context.Context, req *datapb.FlushRequest) (*datapb.FlushResponse, error) {
		return &datapb.FlushResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "mock flush error"),
		}, nil
	}
	dc.ImportFunc = func(ctx context.Context, req *datapb.ImportTaskRequest) (*datapb.ImportTaskResponse, error) {
		return &datapb.ImportTaskResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "mock import error"),
		}, nil
	}
	dc.UnsetIsImportingStateFunc = func(ctx context.Context, req *datapb.UnsetIsImportingStateRequest) (*commonpb.Status, error) {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "mock UnsetIsImportingState error",
		}, nil
	}
	dc.broadCastAlteredCollectionFunc = func(ctx context.Context, req *datapb.AlterCollectionRequest) (*commonpb.Status, error) {
		return failStatus(commonpb.ErrorCode_UnexpectedError, "mock broadcast altered collection error"), nil
	}
	dc.GetSegmentIndexStateFunc = func(ctx context.Context, req *indexpb.GetSegmentIndexStateRequest) (*indexpb.GetSegmentIndexStateResponse, error) {
		return &indexpb.GetSegmentIndexStateResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "mock GetSegmentIndexStateFunc fail"),
		}, nil
	}
	dc.DropIndexFunc = func(ctx context.Context, req *indexpb.DropIndexRequest) (*commonpb.Status, error) {
		return failStatus(commonpb.ErrorCode_UnexpectedError, "mock DropIndexFunc fail"), nil
	}
	return withDataCoord(dc)
}

func withValidDataCoord() Opt {
	dc := newMockDataCoord()
	dc.GetComponentStatesFunc = func(ctx context.Context) (*milvuspb.ComponentStates, error) {
		return &milvuspb.ComponentStates{
			State:  &milvuspb.ComponentInfo{StateCode: commonpb.StateCode_Healthy},
			Status: succStatus(),
		}, nil
	}
	dc.WatchChannelsFunc = func(ctx context.Context, req *datapb.WatchChannelsRequest) (*datapb.WatchChannelsResponse, error) {
		return &datapb.WatchChannelsResponse{
			Status: succStatus(),
		}, nil
	}
	dc.FlushFunc = func(ctx context.Context, req *datapb.FlushRequest) (*datapb.FlushResponse, error) {
		return &datapb.FlushResponse{
			Status: succStatus(),
		}, nil
	}
	dc.ImportFunc = func(ctx context.Context, req *datapb.ImportTaskRequest) (*datapb.ImportTaskResponse, error) {
		return &datapb.ImportTaskResponse{
			Status: succStatus(),
		}, nil
	}
	dc.UnsetIsImportingStateFunc = func(ctx context.Context, req *datapb.UnsetIsImportingStateRequest) (*commonpb.Status, error) {
		return succStatus(), nil
	}
	dc.broadCastAlteredCollectionFunc = func(ctx context.Context, req *datapb.AlterCollectionRequest) (*commonpb.Status, error) {
		return succStatus(), nil
	}
	dc.GetSegmentIndexStateFunc = func(ctx context.Context, req *indexpb.GetSegmentIndexStateRequest) (*indexpb.GetSegmentIndexStateResponse, error) {
		return &indexpb.GetSegmentIndexStateResponse{
			Status: succStatus(),
		}, nil
	}
	dc.DropIndexFunc = func(ctx context.Context, req *indexpb.DropIndexRequest) (*commonpb.Status, error) {
		return succStatus(), nil
	}
	return withDataCoord(dc)
}

func withStateCode(code commonpb.StateCode) Opt {
	return func(c *Core) {
		c.UpdateStateCode(code)
	}
}

func withHealthyCode() Opt {
	return withStateCode(commonpb.StateCode_Healthy)
}

func withAbnormalCode() Opt {
	return withStateCode(commonpb.StateCode_Abnormal)
}

type mockScheduler struct {
	IScheduler
	AddTaskFunc     func(t task) error
	GetMinDdlTsFunc func() Timestamp
	StopFunc        func()
	minDdlTs        Timestamp
}

func newMockScheduler() *mockScheduler {
	return &mockScheduler{}
}

func (m mockScheduler) AddTask(t task) error {
	if m.AddTaskFunc != nil {
		return m.AddTaskFunc(t)
	}
	return nil
}

func (m mockScheduler) GetMinDdlTs() Timestamp {
	if m.GetMinDdlTsFunc != nil {
		return m.GetMinDdlTsFunc()
	}
	return m.minDdlTs
}

func (m mockScheduler) Stop() {
	if m.StopFunc != nil {
		m.StopFunc()
	}
}

func withScheduler(sched IScheduler) Opt {
	return func(c *Core) {
		c.scheduler = sched
	}
}

func withValidScheduler() Opt {
	sched := newMockScheduler()
	sched.AddTaskFunc = func(t task) error {
		t.NotifyDone(nil)
		return nil
	}
	sched.StopFunc = func() {}
	return withScheduler(sched)
}

func withInvalidScheduler() Opt {
	sched := newMockScheduler()
	sched.AddTaskFunc = func(t task) error {
		return errors.New("error mock AddTask")
	}
	return withScheduler(sched)
}

func withTaskFailScheduler() Opt {
	sched := newMockScheduler()
	sched.AddTaskFunc = func(t task) error {
		err := errors.New("error mock task fail")
		t.NotifyDone(err)
		return nil
	}
	return withScheduler(sched)
}

func withTsoAllocator(alloc tso.Allocator) Opt {
	return func(c *Core) {
		c.tsoAllocator = alloc
	}
}

func withInvalidTsoAllocator() Opt {
	alloc := newMockTsoAllocator()
	alloc.GenerateTSOF = func(count uint32) (uint64, error) {
		return 0, errors.New("error mock GenerateTSO")
	}
	return withTsoAllocator(alloc)
}

func withMetricsCacheManager() Opt {
	return func(c *Core) {
		m := metricsinfo.NewMetricsCacheManager()
		c.metricsCacheManager = m
	}
}

type mockBroker struct {
	Broker

	ReleaseCollectionFunc       func(ctx context.Context, collectionID UniqueID) error
	ReleasePartitionsFunc       func(ctx context.Context, collectionID UniqueID, partitionIDs ...UniqueID) error
	SyncNewCreatedPartitionFunc func(ctx context.Context, collectionID UniqueID, partitionID UniqueID) error
	GetQuerySegmentInfoFunc     func(ctx context.Context, collectionID int64, segIDs []int64) (retResp *querypb.GetSegmentInfoResponse, retErr error)

	WatchChannelsFunc     func(ctx context.Context, info *watchInfo) error
	UnwatchChannelsFunc   func(ctx context.Context, info *watchInfo) error
	AddSegRefLockFunc     func(ctx context.Context, taskID int64, segIDs []int64) error
	ReleaseSegRefLockFunc func(ctx context.Context, taskID int64, segIDs []int64) error
	FlushFunc             func(ctx context.Context, cID int64, segIDs []int64) error
	ImportFunc            func(ctx context.Context, req *datapb.ImportTaskRequest) (*datapb.ImportTaskResponse, error)

	DropCollectionIndexFunc  func(ctx context.Context, collID UniqueID, partIDs []UniqueID) error
	DescribeIndexFunc        func(ctx context.Context, colID UniqueID) (*indexpb.DescribeIndexResponse, error)
	GetSegmentIndexStateFunc func(ctx context.Context, collID UniqueID, indexName string, segIDs []UniqueID) ([]*indexpb.SegmentIndexState, error)

	BroadcastAlteredCollectionFunc func(ctx context.Context, req *milvuspb.AlterCollectionRequest) error

	GCConfirmFunc func(ctx context.Context, collectionID, partitionID UniqueID) bool
}

func newMockBroker() *mockBroker {
	return &mockBroker{}
}

func (b mockBroker) WatchChannels(ctx context.Context, info *watchInfo) error {
	return b.WatchChannelsFunc(ctx, info)
}

func (b mockBroker) UnwatchChannels(ctx context.Context, info *watchInfo) error {
	return b.UnwatchChannelsFunc(ctx, info)
}

func (b mockBroker) ReleaseCollection(ctx context.Context, collectionID UniqueID) error {
	return b.ReleaseCollectionFunc(ctx, collectionID)
}

func (b mockBroker) ReleasePartitions(ctx context.Context, collectionID UniqueID, partitionIDs ...UniqueID) error {
	return b.ReleasePartitionsFunc(ctx, collectionID)
}

func (b mockBroker) SyncNewCreatedPartition(ctx context.Context, collectionID UniqueID, partitionID UniqueID) error {
	return b.SyncNewCreatedPartitionFunc(ctx, collectionID, partitionID)
}

func (b mockBroker) DropCollectionIndex(ctx context.Context, collID UniqueID, partIDs []UniqueID) error {
	return b.DropCollectionIndexFunc(ctx, collID, partIDs)
}

func (b mockBroker) DescribeIndex(ctx context.Context, colID UniqueID) (*indexpb.DescribeIndexResponse, error) {
	return b.DescribeIndexFunc(ctx, colID)
}

func (b mockBroker) GetSegmentIndexState(ctx context.Context, collID UniqueID, indexName string, segIDs []UniqueID) ([]*indexpb.SegmentIndexState, error) {
	return b.GetSegmentIndexStateFunc(ctx, collID, indexName, segIDs)
}

func (b mockBroker) BroadcastAlteredCollection(ctx context.Context, req *milvuspb.AlterCollectionRequest) error {
	return b.BroadcastAlteredCollectionFunc(ctx, req)
}

func (b mockBroker) GcConfirm(ctx context.Context, collectionID, partitionID UniqueID) bool {
	return b.GCConfirmFunc(ctx, collectionID, partitionID)
}

func withBroker(b Broker) Opt {
	return func(c *Core) {
		c.broker = b
	}
}

type mockGarbageCollector struct {
	GarbageCollector
	GcCollectionDataFunc func(ctx context.Context, coll *model.Collection) (Timestamp, error)
	GcPartitionDataFunc  func(ctx context.Context, pChannels []string, partition *model.Partition) (Timestamp, error)
}

func (m mockGarbageCollector) GcCollectionData(ctx context.Context, coll *model.Collection) (Timestamp, error) {
	return m.GcCollectionDataFunc(ctx, coll)
}

func (m mockGarbageCollector) GcPartitionData(ctx context.Context, pChannels []string, partition *model.Partition) (Timestamp, error) {
	return m.GcPartitionDataFunc(ctx, pChannels, partition)
}

func newMockGarbageCollector() *mockGarbageCollector {
	return &mockGarbageCollector{}
}

func withGarbageCollector(gc GarbageCollector) Opt {
	return func(c *Core) {
		c.garbageCollector = gc
	}
}

func newMockFailStream() *msgstream.WastedMockMsgStream {
	stream := msgstream.NewWastedMockMsgStream()
	stream.BroadcastFunc = func(pack *msgstream.MsgPack) error {
		return errors.New("error mock Broadcast")
	}
	stream.BroadcastMarkFunc = func(pack *msgstream.MsgPack) (map[string][]msgstream.MessageID, error) {
		return nil, errors.New("error mock BroadcastMark")
	}
	stream.AsProducerFunc = func(channels []string) {
	}
	return stream
}

func newMockFailStreamFactory() *msgstream.MockMqFactory {
	f := msgstream.NewMockMqFactory()
	f.NewMsgStreamFunc = func(ctx context.Context) (msgstream.MsgStream, error) {
		return newMockFailStream(), nil
	}
	return f
}

func newTickerWithMockFailStream() *timetickSync {
	factory := newMockFailStreamFactory()
	return newTickerWithFactory(factory)
}

func newMockNormalStream() *msgstream.WastedMockMsgStream {
	stream := msgstream.NewWastedMockMsgStream()
	stream.BroadcastFunc = func(pack *msgstream.MsgPack) error {
		return nil
	}
	stream.BroadcastMarkFunc = func(pack *msgstream.MsgPack) (map[string][]msgstream.MessageID, error) {
		return map[string][]msgstream.MessageID{}, nil
	}
	stream.AsProducerFunc = func(channels []string) {
	}
	return stream
}

func newMockNormalStreamFactory() *msgstream.MockMqFactory {
	f := msgstream.NewMockMqFactory()
	f.NewMsgStreamFunc = func(ctx context.Context) (msgstream.MsgStream, error) {
		return newMockNormalStream(), nil
	}
	return f
}

func newTickerWithMockNormalStream() *timetickSync {
	factory := newMockNormalStreamFactory()
	return newTickerWithFactory(factory)
}

func newTickerWithFactory(factory msgstream.Factory) *timetickSync {
	Params.Init()
	paramtable.Get().Save(Params.RootCoordCfg.DmlChannelNum.Key, "4")
	ctx := context.Background()
	chans := map[UniqueID][]string{}
	ticker := newTimeTickSync(ctx, TestRootCoordID, factory, chans)
	return ticker
}

type mockDdlTsLockManager struct {
	DdlTsLockManager
	GetMinDdlTsFunc func() Timestamp
}

func (m mockDdlTsLockManager) GetMinDdlTs() Timestamp {
	if m.GetMinDdlTsFunc != nil {
		return m.GetMinDdlTsFunc()
	}
	return 100
}

func newMockDdlTsLockManager() *mockDdlTsLockManager {
	return &mockDdlTsLockManager{}
}

func withDdlTsLockManager(m DdlTsLockManager) Opt {
	return func(c *Core) {
		c.ddlTsLockManager = m
	}
}

type mockStepExecutor struct {
	StepExecutor
	StartFunc    func()
	StopFunc     func()
	AddStepsFunc func(s *stepStack)
}

func newMockStepExecutor() *mockStepExecutor {
	return &mockStepExecutor{}
}

func (m mockStepExecutor) Start() {
	if m.StartFunc != nil {
		m.StartFunc()
	} else {
	}
}

func (m mockStepExecutor) Stop() {
	if m.StopFunc != nil {
		m.StopFunc()
	} else {
	}
}

func (m mockStepExecutor) AddSteps(s *stepStack) {
	if m.AddStepsFunc != nil {
		m.AddStepsFunc(s)
	} else {
	}
}

func withStepExecutor(executor StepExecutor) Opt {
	return func(c *Core) {
		c.stepExecutor = executor
	}
}
