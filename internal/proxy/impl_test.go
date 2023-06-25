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

package proxy

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func TestProxy_InvalidateCollectionMetaCache_remove_stream(t *testing.T) {
	paramtable.Init()
	cache := globalMetaCache
	globalMetaCache = nil
	defer func() { globalMetaCache = cache }()

	chMgr := newMockChannelsMgr()
	chMgr.removeDMLStreamFuncType = func(collectionID UniqueID) error {
		log.Debug("TestProxy_InvalidateCollectionMetaCache_remove_stream, remove dml stream")
		return nil
	}

	node := &Proxy{chMgr: chMgr}
	node.stateCode.Store(commonpb.StateCode_Healthy)

	ctx := context.Background()
	req := &proxypb.InvalidateCollMetaCacheRequest{
		Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection},
	}

	status, err := node.InvalidateCollectionMetaCache(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, status.GetErrorCode())
}

func TestProxy_CheckHealth(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		node := &Proxy{session: &sessionutil.Session{ServerID: 1}}
		node.multiRateLimiter = NewMultiRateLimiter()
		node.stateCode.Store(commonpb.StateCode_Abnormal)
		ctx := context.Background()
		resp, err := node.CheckHealth(ctx, &milvuspb.CheckHealthRequest{})
		assert.NoError(t, err)
		assert.Equal(t, false, resp.IsHealthy)
		assert.Equal(t, 1, len(resp.Reasons))
	})

	t.Run("proxy health check is ok", func(t *testing.T) {
		qc := &types.MockQueryCoord{}
		qc.EXPECT().CheckHealth(mock.Anything, mock.Anything).Return(&milvuspb.CheckHealthResponse{IsHealthy: true}, nil)
		node := &Proxy{
			rootCoord:  NewRootCoordMock(),
			queryCoord: qc,
			dataCoord:  NewDataCoordMock(),
			session:    &sessionutil.Session{ServerID: 1},
		}
		node.multiRateLimiter = NewMultiRateLimiter()
		node.stateCode.Store(commonpb.StateCode_Healthy)
		ctx := context.Background()
		resp, err := node.CheckHealth(ctx, &milvuspb.CheckHealthRequest{})
		assert.NoError(t, err)
		assert.Equal(t, true, resp.IsHealthy)
		assert.Empty(t, resp.Reasons)
	})

	t.Run("proxy health check is fail", func(t *testing.T) {
		checkHealthFunc1 := func(ctx context.Context,
			req *milvuspb.CheckHealthRequest) (*milvuspb.CheckHealthResponse, error) {
			return &milvuspb.CheckHealthResponse{
				IsHealthy: false,
				Reasons:   []string{"unHealth"},
			}, nil
		}

		dataCoordMock := NewDataCoordMock()
		dataCoordMock.checkHealthFunc = checkHealthFunc1

		qc := &types.MockQueryCoord{}
		qc.EXPECT().CheckHealth(mock.Anything, mock.Anything).Return(nil, errors.New("test"))
		node := &Proxy{
			session: &sessionutil.Session{ServerID: 1},
			rootCoord: NewRootCoordMock(func(mock *RootCoordMock) {
				mock.checkHealthFunc = checkHealthFunc1
			}),
			queryCoord: qc,
			dataCoord:  dataCoordMock}
		node.multiRateLimiter = NewMultiRateLimiter()
		node.stateCode.Store(commonpb.StateCode_Healthy)
		ctx := context.Background()
		resp, err := node.CheckHealth(ctx, &milvuspb.CheckHealthRequest{})
		assert.NoError(t, err)
		assert.Equal(t, false, resp.IsHealthy)
		assert.Equal(t, 3, len(resp.Reasons))
	})

	t.Run("check quota state", func(t *testing.T) {
		qc := &types.MockQueryCoord{}
		qc.EXPECT().CheckHealth(mock.Anything, mock.Anything).Return(&milvuspb.CheckHealthResponse{IsHealthy: true}, nil)
		node := &Proxy{
			rootCoord:  NewRootCoordMock(),
			dataCoord:  NewDataCoordMock(),
			queryCoord: qc,
		}
		node.multiRateLimiter = NewMultiRateLimiter()
		node.stateCode.Store(commonpb.StateCode_Healthy)
		resp, err := node.CheckHealth(context.Background(), &milvuspb.CheckHealthRequest{})
		assert.NoError(t, err)
		assert.Equal(t, true, resp.IsHealthy)
		assert.Equal(t, 0, len(resp.GetQuotaStates()))
		assert.Equal(t, 0, len(resp.GetReasons()))

		states := []milvuspb.QuotaState{milvuspb.QuotaState_DenyToWrite, milvuspb.QuotaState_DenyToRead}
		codes := []commonpb.ErrorCode{commonpb.ErrorCode_MemoryQuotaExhausted, commonpb.ErrorCode_ForceDeny}
		node.multiRateLimiter.SetRates([]*proxypb.CollectionRate{
			{
				Collection: 1,
				States:     states,
				Codes:      codes,
			},
		})
		resp, err = node.CheckHealth(context.Background(), &milvuspb.CheckHealthRequest{})
		assert.NoError(t, err)
		assert.Equal(t, true, resp.IsHealthy)
		assert.Equal(t, 2, len(resp.GetQuotaStates()))
		assert.Equal(t, 2, len(resp.GetReasons()))
	})
}

func TestProxyRenameCollection(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		node := &Proxy{session: &sessionutil.Session{ServerID: 1}}
		node.stateCode.Store(commonpb.StateCode_Abnormal)
		ctx := context.Background()
		resp, err := node.RenameCollection(ctx, &milvuspb.RenameCollectionRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetErrorCode())
	})

	t.Run("rename with illegal new collection name", func(t *testing.T) {
		node := &Proxy{session: &sessionutil.Session{ServerID: 1}}
		node.stateCode.Store(commonpb.StateCode_Healthy)
		ctx := context.Background()
		resp, err := node.RenameCollection(ctx, &milvuspb.RenameCollectionRequest{NewName: "$#^%#&#$*!)#@!"})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_IllegalCollectionName, resp.GetErrorCode())
	})

	t.Run("rename fail", func(t *testing.T) {
		rc := mocks.NewRootCoord(t)
		rc.On("RenameCollection", mock.Anything, mock.Anything).
			Return(nil, errors.New("fail"))
		node := &Proxy{
			session:   &sessionutil.Session{ServerID: 1},
			rootCoord: rc,
		}
		node.stateCode.Store(commonpb.StateCode_Healthy)
		ctx := context.Background()

		resp, err := node.RenameCollection(ctx, &milvuspb.RenameCollectionRequest{NewName: "new"})
		assert.Error(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetErrorCode())
	})

	t.Run("rename ok", func(t *testing.T) {
		rc := mocks.NewRootCoord(t)
		rc.On("RenameCollection", mock.Anything, mock.Anything).
			Return(&commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			}, nil)
		node := &Proxy{
			session:   &sessionutil.Session{ServerID: 1},
			rootCoord: rc,
		}
		node.stateCode.Store(commonpb.StateCode_Healthy)
		ctx := context.Background()

		resp, err := node.RenameCollection(ctx, &milvuspb.RenameCollectionRequest{NewName: "new"})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})
}

func TestProxy_ResourceGroup(t *testing.T) {
	factory := dependency.NewDefaultFactory(true)
	ctx := context.Background()

	node, err := NewProxy(ctx, factory)
	assert.NoError(t, err)
	node.multiRateLimiter = NewMultiRateLimiter()
	node.stateCode.Store(commonpb.StateCode_Healthy)

	qc := types.NewMockQueryCoord(t)
	node.SetQueryCoordClient(qc)

	tsoAllocatorIns := newMockTsoAllocator()
	node.sched, err = newTaskScheduler(node.ctx, tsoAllocatorIns, node.factory)
	assert.NoError(t, err)
	node.sched.Start()
	defer node.sched.Close()

	rc := &MockRootCoordClientInterface{}
	mgr := newShardClientMgr()
	InitMetaCache(ctx, rc, qc, mgr)

	successStatus := &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}

	t.Run("create resource group", func(t *testing.T) {
		qc.EXPECT().CreateResourceGroup(mock.Anything, mock.Anything).Return(successStatus, nil)
		resp, err := node.CreateResourceGroup(ctx, &milvuspb.CreateResourceGroupRequest{
			ResourceGroup: "rg",
		})
		assert.NoError(t, err)
		assert.Equal(t, resp.ErrorCode, commonpb.ErrorCode_Success)
	})

	t.Run("drop resource group", func(t *testing.T) {
		qc.EXPECT().DropResourceGroup(mock.Anything, mock.Anything).Return(successStatus, nil)
		resp, err := node.DropResourceGroup(ctx, &milvuspb.DropResourceGroupRequest{
			ResourceGroup: "rg",
		})
		assert.NoError(t, err)
		assert.Equal(t, resp.ErrorCode, commonpb.ErrorCode_Success)
	})

	t.Run("transfer node", func(t *testing.T) {
		qc.EXPECT().TransferNode(mock.Anything, mock.Anything).Return(successStatus, nil)
		resp, err := node.TransferNode(ctx, &milvuspb.TransferNodeRequest{
			SourceResourceGroup: "rg1",
			TargetResourceGroup: "rg2",
			NumNode:             1,
		})
		assert.NoError(t, err)
		assert.Equal(t, resp.ErrorCode, commonpb.ErrorCode_Success)
	})

	t.Run("transfer replica", func(t *testing.T) {
		qc.EXPECT().TransferReplica(mock.Anything, mock.Anything).Return(successStatus, nil)
		resp, err := node.TransferReplica(ctx, &milvuspb.TransferReplicaRequest{
			SourceResourceGroup: "rg1",
			TargetResourceGroup: "rg2",
			NumReplica:          1,
			CollectionName:      "collection1",
		})
		assert.NoError(t, err)
		assert.Equal(t, resp.ErrorCode, commonpb.ErrorCode_Success)
	})

	t.Run("list resource group", func(t *testing.T) {
		qc.EXPECT().ListResourceGroups(mock.Anything, mock.Anything).Return(&milvuspb.ListResourceGroupsResponse{Status: successStatus}, nil)
		resp, err := node.ListResourceGroups(ctx, &milvuspb.ListResourceGroupsRequest{})
		assert.NoError(t, err)
		assert.Equal(t, resp.Status.ErrorCode, commonpb.ErrorCode_Success)
	})

	t.Run("describe resource group", func(t *testing.T) {
		qc.EXPECT().DescribeResourceGroup(mock.Anything, mock.Anything).Return(&querypb.DescribeResourceGroupResponse{
			Status: successStatus,
			ResourceGroup: &querypb.ResourceGroupInfo{
				Name:             "rg",
				Capacity:         1,
				NumAvailableNode: 1,
				NumLoadedReplica: nil,
				NumOutgoingNode:  nil,
				NumIncomingNode:  nil,
			},
		}, nil)
		resp, err := node.DescribeResourceGroup(ctx, &milvuspb.DescribeResourceGroupRequest{
			ResourceGroup: "rg",
		})
		assert.NoError(t, err)
		assert.Equal(t, resp.Status.ErrorCode, commonpb.ErrorCode_Success)
	})
}

func TestProxy_InvalidResourceGroupName(t *testing.T) {
	factory := dependency.NewDefaultFactory(true)
	ctx := context.Background()

	node, err := NewProxy(ctx, factory)
	assert.NoError(t, err)
	node.multiRateLimiter = NewMultiRateLimiter()
	node.stateCode.Store(commonpb.StateCode_Healthy)

	qc := types.NewMockQueryCoord(t)
	node.SetQueryCoordClient(qc)
	qc.EXPECT().DropResourceGroup(mock.Anything, mock.Anything).Return(&commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil)

	tsoAllocatorIns := newMockTsoAllocator()
	node.sched, err = newTaskScheduler(node.ctx, tsoAllocatorIns, node.factory)
	assert.NoError(t, err)
	node.sched.Start()
	defer node.sched.Close()

	rc := &MockRootCoordClientInterface{}
	mgr := newShardClientMgr()
	InitMetaCache(ctx, rc, qc, mgr)

	t.Run("create resource group", func(t *testing.T) {
		resp, err := node.CreateResourceGroup(ctx, &milvuspb.CreateResourceGroupRequest{
			ResourceGroup: "...",
		})
		assert.NoError(t, err)
		assert.Equal(t, resp.ErrorCode, commonpb.ErrorCode_IllegalArgument)
	})

	t.Run("drop resource group", func(t *testing.T) {
		resp, err := node.DropResourceGroup(ctx, &milvuspb.DropResourceGroupRequest{
			ResourceGroup: "...",
		})
		assert.NoError(t, err)
		assert.Equal(t, resp.ErrorCode, commonpb.ErrorCode_Success)
	})

	t.Run("transfer node", func(t *testing.T) {
		resp, err := node.TransferNode(ctx, &milvuspb.TransferNodeRequest{
			SourceResourceGroup: "...",
			TargetResourceGroup: "!!!",
			NumNode:             1,
		})
		assert.NoError(t, err)
		assert.Equal(t, resp.ErrorCode, commonpb.ErrorCode_IllegalArgument)
	})

	t.Run("transfer replica", func(t *testing.T) {
		resp, err := node.TransferReplica(ctx, &milvuspb.TransferReplicaRequest{
			SourceResourceGroup: "...",
			TargetResourceGroup: "!!!",
			NumReplica:          1,
			CollectionName:      "collection1",
		})
		assert.NoError(t, err)
		assert.Equal(t, resp.ErrorCode, commonpb.ErrorCode_IllegalArgument)
	})
}

func TestProxy_FlushAll(t *testing.T) {
	factory := dependency.NewDefaultFactory(true)
	ctx := context.Background()
	paramtable.Init()

	node, err := NewProxy(ctx, factory)
	assert.NoError(t, err)
	node.stateCode.Store(commonpb.StateCode_Healthy)
	node.tsoAllocator = &timestampAllocator{
		tso: newMockTimestampAllocatorInterface(),
	}

	Params.Save(Params.ProxyCfg.MaxTaskNum.Key, "1000")
	node.sched, err = newTaskScheduler(ctx, node.tsoAllocator, node.factory)
	assert.NoError(t, err)
	err = node.sched.Start()
	assert.NoError(t, err)
	defer node.sched.Close()
	node.dataCoord = mocks.NewDataCoord(t)
	node.rootCoord = mocks.NewRootCoord(t)

	// set expectations
	cache := NewMockCache(t)
	cache.On("GetCollectionID",
		mock.Anything, // context.Context
		mock.AnythingOfType("string"),
		mock.AnythingOfType("string"),
	).Return(UniqueID(0), nil).Once()

	cache.On("RemoveDatabase",
		mock.Anything, // context.Context
		mock.AnythingOfType("string"),
	).Maybe()

	globalMetaCache = cache
	successStatus := &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}
	node.dataCoord.(*mocks.DataCoord).EXPECT().Flush(mock.Anything, mock.Anything).
		Return(&datapb.FlushResponse{Status: successStatus}, nil).Maybe()
	node.rootCoord.(*mocks.RootCoord).EXPECT().ShowCollections(mock.Anything, mock.Anything).
		Return(&milvuspb.ShowCollectionsResponse{Status: successStatus, CollectionNames: []string{"col-0"}}, nil).Maybe()
	node.rootCoord.(*mocks.RootCoord).EXPECT().ListDatabases(mock.Anything, mock.Anything).
		Return(&milvuspb.ListDatabasesResponse{Status: successStatus, DbNames: []string{"default"}}, nil).Maybe()

	t.Run("FlushAll", func(t *testing.T) {
		resp, err := node.FlushAll(ctx, &milvuspb.FlushAllRequest{})
		assert.NoError(t, err)
		assert.Equal(t, resp.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
	})

	t.Run("FlushAll failed, server is abnormal", func(t *testing.T) {
		node.stateCode.Store(commonpb.StateCode_Abnormal)
		resp, err := node.FlushAll(ctx, &milvuspb.FlushAllRequest{})
		assert.NoError(t, err)
		assert.Equal(t, resp.GetStatus().GetErrorCode(), commonpb.ErrorCode_UnexpectedError)
		node.stateCode.Store(commonpb.StateCode_Healthy)
	})

	t.Run("FlushAll failed, get id failed", func(t *testing.T) {
		globalMetaCache.(*MockCache).On("GetCollectionID",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(UniqueID(0), errors.New("mock error")).Once()
		resp, err := node.FlushAll(ctx, &milvuspb.FlushAllRequest{})
		assert.NoError(t, err)
		assert.Equal(t, resp.GetStatus().GetErrorCode(), commonpb.ErrorCode_UnexpectedError)
		globalMetaCache.(*MockCache).On("GetCollectionID",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(UniqueID(0), nil).Once()
	})

	t.Run("FlushAll failed, DataCoord flush failed", func(t *testing.T) {
		node.dataCoord.(*mocks.DataCoord).ExpectedCalls = nil
		node.dataCoord.(*mocks.DataCoord).EXPECT().Flush(mock.Anything, mock.Anything).
			Return(&datapb.FlushResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    "mock err",
				},
			}, nil).Maybe()
		resp, err := node.FlushAll(ctx, &milvuspb.FlushAllRequest{})
		assert.NoError(t, err)
		assert.Equal(t, resp.GetStatus().GetErrorCode(), commonpb.ErrorCode_UnexpectedError)
	})

	t.Run("FlushAll failed, RootCoord showCollections failed", func(t *testing.T) {
		node.rootCoord.(*mocks.RootCoord).ExpectedCalls = nil
		node.rootCoord.(*mocks.RootCoord).EXPECT().ListDatabases(mock.Anything, mock.Anything).
			Return(&milvuspb.ListDatabasesResponse{Status: successStatus, DbNames: []string{"default"}}, nil).Maybe()
		node.rootCoord.(*mocks.RootCoord).EXPECT().ShowCollections(mock.Anything, mock.Anything).
			Return(&milvuspb.ShowCollectionsResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    "mock err",
				},
			}, nil).Maybe()
		resp, err := node.FlushAll(ctx, &milvuspb.FlushAllRequest{})
		assert.NoError(t, err)
		assert.Equal(t, resp.GetStatus().GetErrorCode(), commonpb.ErrorCode_UnexpectedError)
	})

	t.Run("FlushAll failed, RootCoord showCollections failed", func(t *testing.T) {
		node.rootCoord.(*mocks.RootCoord).ExpectedCalls = nil
		node.rootCoord.(*mocks.RootCoord).EXPECT().ListDatabases(mock.Anything, mock.Anything).
			Return(&milvuspb.ListDatabasesResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    "mock err",
				},
			}, nil).Maybe()
		resp, err := node.FlushAll(ctx, &milvuspb.FlushAllRequest{})
		assert.NoError(t, err)
		assert.Equal(t, resp.GetStatus().GetErrorCode(), commonpb.ErrorCode_UnexpectedError)
	})
}

func TestProxy_GetFlushAllState(t *testing.T) {
	factory := dependency.NewDefaultFactory(true)
	ctx := context.Background()

	node, err := NewProxy(ctx, factory)
	assert.NoError(t, err)
	node.stateCode.Store(commonpb.StateCode_Healthy)
	node.tsoAllocator = &timestampAllocator{
		tso: newMockTimestampAllocatorInterface(),
	}
	node.dataCoord = mocks.NewDataCoord(t)
	node.rootCoord = mocks.NewRootCoord(t)

	// set expectations
	successStatus := &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}
	node.dataCoord.(*mocks.DataCoord).EXPECT().GetFlushAllState(mock.Anything, mock.Anything).
		Return(&milvuspb.GetFlushAllStateResponse{Status: successStatus}, nil).Maybe()

	t.Run("GetFlushAllState success", func(t *testing.T) {
		resp, err := node.GetFlushAllState(ctx, &milvuspb.GetFlushAllStateRequest{})
		assert.NoError(t, err)
		assert.Equal(t, resp.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
	})

	t.Run("GetFlushAllState failed, server is abnormal", func(t *testing.T) {
		node.stateCode.Store(commonpb.StateCode_Abnormal)
		resp, err := node.GetFlushAllState(ctx, &milvuspb.GetFlushAllStateRequest{})
		assert.NoError(t, err)
		assert.Equal(t, resp.GetStatus().GetErrorCode(), commonpb.ErrorCode_UnexpectedError)
		node.stateCode.Store(commonpb.StateCode_Healthy)
	})

	t.Run("DataCoord GetFlushAllState failed", func(t *testing.T) {
		node.dataCoord.(*mocks.DataCoord).ExpectedCalls = nil
		node.dataCoord.(*mocks.DataCoord).EXPECT().GetFlushAllState(mock.Anything, mock.Anything).
			Return(&milvuspb.GetFlushAllStateResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    "mock err",
				},
			}, nil)
		resp, err := node.GetFlushAllState(ctx, &milvuspb.GetFlushAllStateRequest{})
		assert.NoError(t, err)
		assert.Equal(t, resp.GetStatus().GetErrorCode(), commonpb.ErrorCode_UnexpectedError)
	})
}

func TestProxy_GetReplicas(t *testing.T) {
	factory := dependency.NewDefaultFactory(true)
	ctx := context.Background()

	node, err := NewProxy(ctx, factory)
	assert.NoError(t, err)
	node.stateCode.Store(commonpb.StateCode_Healthy)
	node.tsoAllocator = &timestampAllocator{
		tso: newMockTimestampAllocatorInterface(),
	}
	mockQC := types.NewMockQueryCoord(t)
	mockRC := mocks.NewRootCoord(t)
	node.queryCoord = mockQC
	node.rootCoord = mockRC

	// set expectations
	successStatus := &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}
	t.Run("success", func(t *testing.T) {
		mockQC.EXPECT().GetReplicas(mock.Anything, mock.AnythingOfType("*milvuspb.GetReplicasRequest")).Return(&milvuspb.GetReplicasResponse{Status: successStatus}, nil)
		resp, err := node.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
			CollectionID: 1000,
		})
		assert.NoError(t, err)
		assert.Equal(t, resp.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
	})

	t.Run("proxy_not_healthy", func(t *testing.T) {
		node.stateCode.Store(commonpb.StateCode_Abnormal)
		resp, err := node.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
			CollectionID: 1000,
		})
		assert.NoError(t, err)
		assert.Equal(t, resp.GetStatus().GetErrorCode(), commonpb.ErrorCode_UnexpectedError)
		node.stateCode.Store(commonpb.StateCode_Healthy)
	})

	t.Run("QueryCoordClient_returnsError", func(t *testing.T) {
		mockQC.ExpectedCalls = nil
		mockQC.EXPECT().GetReplicas(mock.Anything, mock.AnythingOfType("*milvuspb.GetReplicasRequest")).Return(nil, errors.New("mocked"))

		resp, err := node.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
			CollectionID: 1000,
		})
		assert.NoError(t, err)
		assert.Equal(t, resp.GetStatus().GetErrorCode(), commonpb.ErrorCode_UnexpectedError)
	})
}

func TestProxy_Connect(t *testing.T) {
	t.Run("proxy unhealthy", func(t *testing.T) {
		node := &Proxy{}
		node.UpdateStateCode(commonpb.StateCode_Abnormal)

		resp, err := node.Connect(context.TODO(), nil)
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("failed to allocate ts", func(t *testing.T) {
		m := newMockTimestampAllocator(t)
		m.On("AllocTimestamp",
			mock.Anything,
			mock.Anything,
		).Return(nil, errors.New("error mock AllocateTimestamp"))
		alloc, _ := newTimestampAllocator(m, 199)
		node := Proxy{
			tsoAllocator: alloc,
		}
		node.UpdateStateCode(commonpb.StateCode_Healthy)
		resp, err := node.Connect(context.TODO(), nil)
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("normal case", func(t *testing.T) {
		m := newMockTimestampAllocator(t)
		m.On("AllocTimestamp",
			mock.Anything,
			mock.Anything,
		).Return(&rootcoordpb.AllocTimestampResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
			Timestamp: 20230518,
			Count:     1,
		}, nil)
		alloc, _ := newTimestampAllocator(m, 199)
		node := Proxy{
			tsoAllocator: alloc,
		}
		node.UpdateStateCode(commonpb.StateCode_Healthy)
		resp, err := node.Connect(context.TODO(), &milvuspb.ConnectRequest{
			ClientInfo: &commonpb.ClientInfo{},
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})
}

func TestProxy_ListClientInfos(t *testing.T) {
	t.Run("proxy unhealthy", func(t *testing.T) {
		node := &Proxy{}
		node.UpdateStateCode(commonpb.StateCode_Abnormal)

		resp, err := node.ListClientInfos(context.TODO(), nil)
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("normal case", func(t *testing.T) {
		node := Proxy{}
		node.UpdateStateCode(commonpb.StateCode_Healthy)

		resp, err := node.ListClientInfos(context.TODO(), nil)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())

	})
}

func TestProxyCreateDatabase(t *testing.T) {
	paramtable.Init()

	t.Run("not healthy", func(t *testing.T) {
		node := &Proxy{session: &sessionutil.Session{ServerID: 1}}
		node.stateCode.Store(commonpb.StateCode_Abnormal)
		ctx := context.Background()
		resp, err := node.CreateDatabase(ctx, &milvuspb.CreateDatabaseRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetErrorCode())
	})

	factory := dependency.NewDefaultFactory(true)
	ctx := context.Background()

	node, err := NewProxy(ctx, factory)
	assert.NoError(t, err)
	node.tsoAllocator = &timestampAllocator{
		tso: newMockTimestampAllocatorInterface(),
	}
	node.multiRateLimiter = NewMultiRateLimiter()
	node.stateCode.Store(commonpb.StateCode_Healthy)
	node.sched, err = newTaskScheduler(ctx, node.tsoAllocator, node.factory)
	node.sched.ddQueue.setMaxTaskNum(10)
	assert.NoError(t, err)
	err = node.sched.Start()
	assert.NoError(t, err)
	defer node.sched.Close()

	t.Run("create database fail", func(t *testing.T) {
		rc := mocks.NewRootCoord(t)
		rc.On("CreateDatabase", mock.Anything, mock.Anything).
			Return(nil, errors.New("fail"))
		node.rootCoord = rc
		ctx := context.Background()
		resp, err := node.CreateDatabase(ctx, &milvuspb.CreateDatabaseRequest{DbName: "db"})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetErrorCode())
	})

	t.Run("create database ok", func(t *testing.T) {
		rc := mocks.NewRootCoord(t)
		rc.On("CreateDatabase", mock.Anything, mock.Anything).
			Return(&commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			}, nil)
		node.rootCoord = rc
		node.stateCode.Store(commonpb.StateCode_Healthy)
		ctx := context.Background()

		resp, err := node.CreateDatabase(ctx, &milvuspb.CreateDatabaseRequest{DbName: "db"})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})
}

func TestProxyDropDatabase(t *testing.T) {
	paramtable.Init()

	t.Run("not healthy", func(t *testing.T) {
		node := &Proxy{session: &sessionutil.Session{ServerID: 1}}
		node.stateCode.Store(commonpb.StateCode_Abnormal)
		ctx := context.Background()
		resp, err := node.DropDatabase(ctx, &milvuspb.DropDatabaseRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetErrorCode())
	})

	factory := dependency.NewDefaultFactory(true)
	ctx := context.Background()

	node, err := NewProxy(ctx, factory)
	assert.NoError(t, err)
	node.tsoAllocator = &timestampAllocator{
		tso: newMockTimestampAllocatorInterface(),
	}
	node.multiRateLimiter = NewMultiRateLimiter()
	node.stateCode.Store(commonpb.StateCode_Healthy)
	node.sched, err = newTaskScheduler(ctx, node.tsoAllocator, node.factory)
	node.sched.ddQueue.setMaxTaskNum(10)
	assert.NoError(t, err)
	err = node.sched.Start()
	assert.NoError(t, err)
	defer node.sched.Close()

	t.Run("drop database fail", func(t *testing.T) {
		rc := mocks.NewRootCoord(t)
		rc.On("DropDatabase", mock.Anything, mock.Anything).
			Return(nil, errors.New("fail"))
		node.rootCoord = rc
		ctx := context.Background()
		resp, err := node.DropDatabase(ctx, &milvuspb.DropDatabaseRequest{DbName: "db"})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetErrorCode())
	})

	t.Run("drop database ok", func(t *testing.T) {
		rc := mocks.NewRootCoord(t)
		rc.On("DropDatabase", mock.Anything, mock.Anything).
			Return(&commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			}, nil)
		node.rootCoord = rc
		node.stateCode.Store(commonpb.StateCode_Healthy)
		ctx := context.Background()

		resp, err := node.DropDatabase(ctx, &milvuspb.DropDatabaseRequest{DbName: "db"})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})
}

func TestProxyListDatabase(t *testing.T) {
	paramtable.Init()

	t.Run("not healthy", func(t *testing.T) {
		node := &Proxy{session: &sessionutil.Session{ServerID: 1}}
		node.stateCode.Store(commonpb.StateCode_Abnormal)
		ctx := context.Background()
		resp, err := node.ListDatabases(ctx, &milvuspb.ListDatabasesRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
	})

	factory := dependency.NewDefaultFactory(true)
	ctx := context.Background()

	node, err := NewProxy(ctx, factory)
	assert.NoError(t, err)
	node.tsoAllocator = &timestampAllocator{
		tso: newMockTimestampAllocatorInterface(),
	}
	node.multiRateLimiter = NewMultiRateLimiter()
	node.stateCode.Store(commonpb.StateCode_Healthy)
	node.sched, err = newTaskScheduler(ctx, node.tsoAllocator, node.factory)
	node.sched.ddQueue.setMaxTaskNum(10)
	assert.NoError(t, err)
	err = node.sched.Start()
	assert.NoError(t, err)
	defer node.sched.Close()

	t.Run("list database fail", func(t *testing.T) {
		rc := mocks.NewRootCoord(t)
		rc.On("ListDatabases", mock.Anything, mock.Anything).
			Return(nil, errors.New("fail"))
		node.rootCoord = rc
		ctx := context.Background()
		resp, err := node.ListDatabases(ctx, &milvuspb.ListDatabasesRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
	})

	t.Run("list database ok", func(t *testing.T) {
		rc := mocks.NewRootCoord(t)
		rc.On("ListDatabases", mock.Anything, mock.Anything).
			Return(&milvuspb.ListDatabasesResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
				}}, nil)
		node.rootCoord = rc
		node.stateCode.Store(commonpb.StateCode_Healthy)
		ctx := context.Background()

		resp, err := node.ListDatabases(ctx, &milvuspb.ListDatabasesRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})
}
