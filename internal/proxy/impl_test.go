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
	"encoding/base64"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	mqcommon "github.com/milvus-io/milvus/pkg/mq/common"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/ratelimitutil"
	"github.com/milvus-io/milvus/pkg/util/resource"
)

func TestProxy_InvalidateCollectionMetaCache_remove_stream(t *testing.T) {
	paramtable.Init()
	cache := globalMetaCache
	globalMetaCache = nil
	defer func() { globalMetaCache = cache }()

	chMgr := NewMockChannelsMgr(t)
	chMgr.EXPECT().removeDMLStream(mock.Anything).Return()

	node := &Proxy{chMgr: chMgr}
	_ = node.initRateCollector()
	node.UpdateStateCode(commonpb.StateCode_Healthy)

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
		node := &Proxy{session: &sessionutil.Session{SessionRaw: sessionutil.SessionRaw{ServerID: 1}}}
		node.simpleLimiter = NewSimpleLimiter(0, 0)
		node.UpdateStateCode(commonpb.StateCode_Abnormal)
		ctx := context.Background()
		resp, err := node.CheckHealth(ctx, &milvuspb.CheckHealthRequest{})
		assert.NoError(t, err)
		assert.Equal(t, false, resp.IsHealthy)
		assert.Equal(t, 1, len(resp.Reasons))
	})

	t.Run("proxy health check is ok", func(t *testing.T) {
		qc := &mocks.MockQueryCoordClient{}
		qc.EXPECT().CheckHealth(mock.Anything, mock.Anything).Return(&milvuspb.CheckHealthResponse{IsHealthy: true}, nil)
		node := &Proxy{
			rootCoord:  NewRootCoordMock(),
			queryCoord: qc,
			dataCoord:  NewDataCoordMock(),
			session:    &sessionutil.Session{SessionRaw: sessionutil.SessionRaw{ServerID: 1}},
		}
		node.simpleLimiter = NewSimpleLimiter(0, 0)
		node.UpdateStateCode(commonpb.StateCode_Healthy)
		ctx := context.Background()
		resp, err := node.CheckHealth(ctx, &milvuspb.CheckHealthRequest{})
		assert.NoError(t, err)
		assert.Equal(t, true, resp.IsHealthy)
		assert.Empty(t, resp.Reasons)
	})

	t.Run("proxy health check is fail", func(t *testing.T) {
		checkHealthFunc1 := func(ctx context.Context,
			req *milvuspb.CheckHealthRequest,
			opts ...grpc.CallOption,
		) (*milvuspb.CheckHealthResponse, error) {
			return &milvuspb.CheckHealthResponse{
				IsHealthy: false,
				Reasons:   []string{"unHealth"},
			}, nil
		}

		dataCoordMock := NewDataCoordMock()
		dataCoordMock.checkHealthFunc = checkHealthFunc1

		qc := &mocks.MockQueryCoordClient{}
		qc.EXPECT().CheckHealth(mock.Anything, mock.Anything).Return(nil, errors.New("test"))
		node := &Proxy{
			session: &sessionutil.Session{SessionRaw: sessionutil.SessionRaw{ServerID: 1}},
			rootCoord: NewRootCoordMock(func(mock *RootCoordMock) {
				mock.checkHealthFunc = checkHealthFunc1
			}),
			queryCoord: qc,
			dataCoord:  dataCoordMock,
		}
		node.simpleLimiter = NewSimpleLimiter(0, 0)
		node.UpdateStateCode(commonpb.StateCode_Healthy)
		ctx := context.Background()
		resp, err := node.CheckHealth(ctx, &milvuspb.CheckHealthRequest{})
		assert.NoError(t, err)
		assert.Equal(t, false, resp.IsHealthy)
		assert.Equal(t, 3, len(resp.Reasons))
	})

	t.Run("check quota state", func(t *testing.T) {
		qc := &mocks.MockQueryCoordClient{}
		qc.EXPECT().CheckHealth(mock.Anything, mock.Anything).Return(&milvuspb.CheckHealthResponse{IsHealthy: true}, nil)
		node := &Proxy{
			rootCoord:  NewRootCoordMock(),
			dataCoord:  NewDataCoordMock(),
			queryCoord: qc,
		}
		node.simpleLimiter = NewSimpleLimiter(0, 0)
		node.UpdateStateCode(commonpb.StateCode_Healthy)
		resp, err := node.CheckHealth(context.Background(), &milvuspb.CheckHealthRequest{})
		assert.NoError(t, err)
		assert.Equal(t, true, resp.IsHealthy)
		assert.Equal(t, 0, len(resp.GetQuotaStates()))
		assert.Equal(t, 0, len(resp.GetReasons()))

		states := []milvuspb.QuotaState{milvuspb.QuotaState_DenyToWrite, milvuspb.QuotaState_DenyToRead}
		codes := []commonpb.ErrorCode{commonpb.ErrorCode_MemoryQuotaExhausted, commonpb.ErrorCode_ForceDeny}
		err = node.simpleLimiter.SetRates(&proxypb.LimiterNode{
			Limiter: &proxypb.Limiter{},
			// db level
			Children: map[int64]*proxypb.LimiterNode{
				1: {
					Limiter: &proxypb.Limiter{},
					// collection level
					Children: map[int64]*proxypb.LimiterNode{
						100: {
							Limiter: &proxypb.Limiter{
								States: states,
								Codes:  codes,
							},
							Children: make(map[int64]*proxypb.LimiterNode),
						},
					},
				},
			},
		})
		assert.NoError(t, err)

		resp, err = node.CheckHealth(context.Background(), &milvuspb.CheckHealthRequest{})
		assert.NoError(t, err)
		assert.Equal(t, true, resp.IsHealthy)
	})
}

func TestProxyRenameCollection(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		node := &Proxy{session: &sessionutil.Session{SessionRaw: sessionutil.SessionRaw{ServerID: 1}}}
		node.UpdateStateCode(commonpb.StateCode_Abnormal)
		ctx := context.Background()
		resp, err := node.RenameCollection(ctx, &milvuspb.RenameCollectionRequest{})
		assert.NoError(t, err)
		assert.ErrorIs(t, merr.Error(resp), merr.ErrServiceNotReady)
	})

	t.Run("rename with illegal new collection name", func(t *testing.T) {
		node := &Proxy{session: &sessionutil.Session{SessionRaw: sessionutil.SessionRaw{ServerID: 1}}}
		node.UpdateStateCode(commonpb.StateCode_Healthy)
		ctx := context.Background()
		resp, err := node.RenameCollection(ctx, &milvuspb.RenameCollectionRequest{NewName: "$#^%#&#$*!)#@!"})
		assert.NoError(t, err)
		assert.ErrorIs(t, merr.Error(resp), merr.ErrParameterInvalid)
	})

	t.Run("rename fail", func(t *testing.T) {
		rc := mocks.NewMockRootCoordClient(t)
		rc.On("RenameCollection", mock.Anything, mock.Anything).
			Return(nil, errors.New("fail"))
		node := &Proxy{
			session:   &sessionutil.Session{SessionRaw: sessionutil.SessionRaw{ServerID: 1}},
			rootCoord: rc,
		}
		node.UpdateStateCode(commonpb.StateCode_Healthy)
		ctx := context.Background()

		resp, err := node.RenameCollection(ctx, &milvuspb.RenameCollectionRequest{NewName: "new"})
		assert.Error(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetErrorCode())
	})

	t.Run("rename ok", func(t *testing.T) {
		rc := mocks.NewMockRootCoordClient(t)
		rc.On("RenameCollection", mock.Anything, mock.Anything).
			Return(merr.Success(), nil)
		node := &Proxy{
			session:   &sessionutil.Session{SessionRaw: sessionutil.SessionRaw{ServerID: 1}},
			rootCoord: rc,
		}
		node.UpdateStateCode(commonpb.StateCode_Healthy)
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
	node.simpleLimiter = NewSimpleLimiter(0, 0)
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	qc := mocks.NewMockQueryCoordClient(t)
	node.SetQueryCoordClient(qc)

	qc.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{}, nil).Maybe()

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
		assert.True(t, merr.Ok(resp.GetStatus()))
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
		assert.True(t, merr.Ok(resp.GetStatus()))
	})
}

func TestProxy_InvalidResourceGroupName(t *testing.T) {
	factory := dependency.NewDefaultFactory(true)
	ctx := context.Background()

	node, err := NewProxy(ctx, factory)
	assert.NoError(t, err)
	node.simpleLimiter = NewSimpleLimiter(0, 0)
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	qc := mocks.NewMockQueryCoordClient(t)
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
		assert.ErrorIs(t, merr.Error(resp), merr.ErrParameterInvalid)
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
		assert.ErrorIs(t, merr.Error(resp), merr.ErrParameterInvalid)
	})

	t.Run("transfer replica", func(t *testing.T) {
		resp, err := node.TransferReplica(ctx, &milvuspb.TransferReplicaRequest{
			SourceResourceGroup: "...",
			TargetResourceGroup: "!!!",
			NumReplica:          1,
			CollectionName:      "collection1",
		})
		assert.NoError(t, err)
		assert.ErrorIs(t, merr.Error(resp), merr.ErrParameterInvalid)
	})
}

func TestProxy_FlushAll_DbCollection(t *testing.T) {
	tests := []struct {
		testName        string
		FlushRequest    *milvuspb.FlushAllRequest
		ExpectedSuccess bool
	}{
		{"flushAll", &milvuspb.FlushAllRequest{}, true},
		{"flushAll set db", &milvuspb.FlushAllRequest{DbName: "default"}, true},
		{"flushAll set db, db not exist", &milvuspb.FlushAllRequest{DbName: "default2"}, false},
	}

	cacheBak := globalMetaCache
	defer func() { globalMetaCache = cacheBak }()
	// set expectations
	cache := NewMockCache(t)
	cache.On("GetCollectionID",
		mock.Anything, // context.Context
		mock.AnythingOfType("string"),
		mock.AnythingOfType("string"),
	).Return(UniqueID(0), nil).Maybe()

	cache.On("RemoveDatabase",
		mock.Anything, // context.Context
		mock.AnythingOfType("string"),
	).Maybe()

	globalMetaCache = cache

	for _, test := range tests {
		t.Run(test.testName, func(t *testing.T) {
			factory := dependency.NewDefaultFactory(true)
			ctx := context.Background()
			paramtable.Init()

			node, err := NewProxy(ctx, factory)
			assert.NoError(t, err)
			node.UpdateStateCode(commonpb.StateCode_Healthy)
			node.tsoAllocator = &timestampAllocator{
				tso: newMockTimestampAllocatorInterface(),
			}
			rpcRequestChannel := Params.CommonCfg.ReplicateMsgChannel.GetValue()
			node.replicateMsgStream, err = node.factory.NewMsgStream(node.ctx)
			assert.NoError(t, err)
			node.replicateMsgStream.AsProducer([]string{rpcRequestChannel})

			Params.Save(Params.ProxyCfg.MaxTaskNum.Key, "1000")
			node.sched, err = newTaskScheduler(ctx, node.tsoAllocator, node.factory)
			assert.NoError(t, err)
			err = node.sched.Start()
			assert.NoError(t, err)
			defer node.sched.Close()
			node.dataCoord = mocks.NewMockDataCoordClient(t)
			node.rootCoord = mocks.NewMockRootCoordClient(t)
			successStatus := &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}
			node.dataCoord.(*mocks.MockDataCoordClient).EXPECT().Flush(mock.Anything, mock.Anything).
				Return(&datapb.FlushResponse{Status: successStatus}, nil).Maybe()
			node.rootCoord.(*mocks.MockRootCoordClient).EXPECT().ShowCollections(mock.Anything, mock.Anything).
				Return(&milvuspb.ShowCollectionsResponse{Status: successStatus, CollectionNames: []string{"col-0"}}, nil).Maybe()
			node.rootCoord.(*mocks.MockRootCoordClient).EXPECT().ListDatabases(mock.Anything, mock.Anything).
				Return(&milvuspb.ListDatabasesResponse{Status: successStatus, DbNames: []string{"default"}}, nil).Maybe()

			resp, err := node.FlushAll(ctx, test.FlushRequest)
			assert.NoError(t, err)
			if test.ExpectedSuccess {
				assert.True(t, merr.Ok(resp.GetStatus()))
			} else {
				assert.NotEqual(t, resp.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
			}
		})
	}
}

func TestProxy_FlushAll(t *testing.T) {
	factory := dependency.NewDefaultFactory(true)
	ctx := context.Background()
	paramtable.Init()

	node, err := NewProxy(ctx, factory)
	assert.NoError(t, err)
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	node.tsoAllocator = &timestampAllocator{
		tso: newMockTimestampAllocatorInterface(),
	}
	rpcRequestChannel := Params.CommonCfg.ReplicateMsgChannel.GetValue()
	node.replicateMsgStream, err = node.factory.NewMsgStream(node.ctx)
	assert.NoError(t, err)
	node.replicateMsgStream.AsProducer([]string{rpcRequestChannel})

	Params.Save(Params.ProxyCfg.MaxTaskNum.Key, "1000")
	node.sched, err = newTaskScheduler(ctx, node.tsoAllocator, node.factory)
	assert.NoError(t, err)
	err = node.sched.Start()
	assert.NoError(t, err)
	defer node.sched.Close()
	node.dataCoord = mocks.NewMockDataCoordClient(t)
	node.rootCoord = mocks.NewMockRootCoordClient(t)

	cacheBak := globalMetaCache
	defer func() { globalMetaCache = cacheBak }()

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
	node.dataCoord.(*mocks.MockDataCoordClient).EXPECT().Flush(mock.Anything, mock.Anything).
		Return(&datapb.FlushResponse{Status: successStatus}, nil).Maybe()
	node.rootCoord.(*mocks.MockRootCoordClient).EXPECT().ShowCollections(mock.Anything, mock.Anything).
		Return(&milvuspb.ShowCollectionsResponse{Status: successStatus, CollectionNames: []string{"col-0"}}, nil).Maybe()
	node.rootCoord.(*mocks.MockRootCoordClient).EXPECT().ListDatabases(mock.Anything, mock.Anything).
		Return(&milvuspb.ListDatabasesResponse{Status: successStatus, DbNames: []string{"default"}}, nil).Maybe()

	t.Run("FlushAll", func(t *testing.T) {
		resp, err := node.FlushAll(ctx, &milvuspb.FlushAllRequest{})
		assert.NoError(t, err)
		assert.True(t, merr.Ok(resp.GetStatus()))
	})

	t.Run("FlushAll failed, server is abnormal", func(t *testing.T) {
		node.UpdateStateCode(commonpb.StateCode_Abnormal)
		resp, err := node.FlushAll(ctx, &milvuspb.FlushAllRequest{})
		assert.NoError(t, err)
		assert.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrServiceNotReady)
		node.UpdateStateCode(commonpb.StateCode_Healthy)
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
		node.dataCoord.(*mocks.MockDataCoordClient).ExpectedCalls = nil
		node.dataCoord.(*mocks.MockDataCoordClient).EXPECT().Flush(mock.Anything, mock.Anything).
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
		node.rootCoord.(*mocks.MockRootCoordClient).ExpectedCalls = nil
		node.rootCoord.(*mocks.MockRootCoordClient).EXPECT().ListDatabases(mock.Anything, mock.Anything).
			Return(&milvuspb.ListDatabasesResponse{Status: successStatus, DbNames: []string{"default"}}, nil).Maybe()
		node.rootCoord.(*mocks.MockRootCoordClient).EXPECT().ShowCollections(mock.Anything, mock.Anything).
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
		node.rootCoord.(*mocks.MockRootCoordClient).ExpectedCalls = nil
		node.rootCoord.(*mocks.MockRootCoordClient).EXPECT().ListDatabases(mock.Anything, mock.Anything).
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
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	node.tsoAllocator = &timestampAllocator{
		tso: newMockTimestampAllocatorInterface(),
	}
	node.dataCoord = mocks.NewMockDataCoordClient(t)
	node.rootCoord = mocks.NewMockRootCoordClient(t)

	// set expectations
	successStatus := &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}
	node.dataCoord.(*mocks.MockDataCoordClient).EXPECT().GetFlushAllState(mock.Anything, mock.Anything).
		Return(&milvuspb.GetFlushAllStateResponse{Status: successStatus}, nil).Maybe()

	t.Run("GetFlushAllState success", func(t *testing.T) {
		resp, err := node.GetFlushAllState(ctx, &milvuspb.GetFlushAllStateRequest{})
		assert.NoError(t, err)
		assert.True(t, merr.Ok(resp.GetStatus()))
	})

	t.Run("GetFlushAllState failed, server is abnormal", func(t *testing.T) {
		node.UpdateStateCode(commonpb.StateCode_Abnormal)
		resp, err := node.GetFlushAllState(ctx, &milvuspb.GetFlushAllStateRequest{})
		assert.NoError(t, err)
		assert.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrServiceNotReady)
		node.UpdateStateCode(commonpb.StateCode_Healthy)
	})

	t.Run("DataCoord GetFlushAllState failed", func(t *testing.T) {
		node.dataCoord.(*mocks.MockDataCoordClient).ExpectedCalls = nil
		node.dataCoord.(*mocks.MockDataCoordClient).EXPECT().GetFlushAllState(mock.Anything, mock.Anything).
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

func TestProxy_GetFlushState(t *testing.T) {
	factory := dependency.NewDefaultFactory(true)
	ctx := context.Background()

	node, err := NewProxy(ctx, factory)
	assert.NoError(t, err)
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	node.tsoAllocator = &timestampAllocator{
		tso: newMockTimestampAllocatorInterface(),
	}
	node.dataCoord = mocks.NewMockDataCoordClient(t)
	node.rootCoord = mocks.NewMockRootCoordClient(t)

	// set expectations
	successStatus := &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}
	node.dataCoord.(*mocks.MockDataCoordClient).EXPECT().GetFlushState(mock.Anything, mock.Anything, mock.Anything).
		Return(&milvuspb.GetFlushStateResponse{Status: successStatus}, nil).Maybe()

	t.Run("GetFlushState success", func(t *testing.T) {
		resp, err := node.GetFlushState(ctx, &milvuspb.GetFlushStateRequest{})
		assert.NoError(t, err)
		assert.Equal(t, resp.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
	})

	t.Run("GetFlushState failed, server is abnormal", func(t *testing.T) {
		node.UpdateStateCode(commonpb.StateCode_Abnormal)
		resp, err := node.GetFlushState(ctx, &milvuspb.GetFlushStateRequest{})
		assert.NoError(t, err)
		assert.Equal(t, resp.GetStatus().GetErrorCode(), commonpb.ErrorCode_NotReadyServe)
		node.UpdateStateCode(commonpb.StateCode_Healthy)
	})

	t.Run("GetFlushState with collection name", func(t *testing.T) {
		resp, err := node.GetFlushState(ctx, &milvuspb.GetFlushStateRequest{
			CollectionName: "*",
		})
		assert.NoError(t, err)
		assert.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrParameterInvalid)

		cacheBak := globalMetaCache
		defer func() { globalMetaCache = cacheBak }()
		cache := NewMockCache(t)
		cache.On("GetCollectionID",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(UniqueID(0), nil).Maybe()
		globalMetaCache = cache

		resp, err = node.GetFlushState(ctx, &milvuspb.GetFlushStateRequest{
			CollectionName: "collection1",
		})
		assert.NoError(t, err)
		assert.Equal(t, resp.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
	})

	t.Run("DataCoord GetFlushState failed", func(t *testing.T) {
		node.dataCoord.(*mocks.MockDataCoordClient).ExpectedCalls = nil
		node.dataCoord.(*mocks.MockDataCoordClient).EXPECT().GetFlushState(mock.Anything, mock.Anything, mock.Anything).
			Return(&milvuspb.GetFlushStateResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    "mock err",
				},
			}, nil)
		resp, err := node.GetFlushState(ctx, &milvuspb.GetFlushStateRequest{})
		assert.NoError(t, err)
		assert.Equal(t, resp.GetStatus().GetErrorCode(), commonpb.ErrorCode_UnexpectedError)
	})

	t.Run("GetFlushState return error", func(t *testing.T) {
		node.dataCoord.(*mocks.MockDataCoordClient).ExpectedCalls = nil
		node.dataCoord.(*mocks.MockDataCoordClient).EXPECT().GetFlushState(mock.Anything, mock.Anything, mock.Anything).
			Return(nil, errors.New("fake error"))
		resp, err := node.GetFlushState(ctx, &milvuspb.GetFlushStateRequest{})
		assert.NoError(t, err)
		assert.Equal(t, resp.GetStatus().GetErrorCode(), commonpb.ErrorCode_UnexpectedError)
	})
}

func TestProxy_GetReplicas(t *testing.T) {
	factory := dependency.NewDefaultFactory(true)
	ctx := context.Background()

	node, err := NewProxy(ctx, factory)
	assert.NoError(t, err)
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	node.tsoAllocator = &timestampAllocator{
		tso: newMockTimestampAllocatorInterface(),
	}
	mockQC := mocks.NewMockQueryCoordClient(t)
	mockRC := mocks.NewMockRootCoordClient(t)
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
		assert.True(t, merr.Ok(resp.GetStatus()))
	})

	t.Run("proxy_not_healthy", func(t *testing.T) {
		node.UpdateStateCode(commonpb.StateCode_Abnormal)
		resp, err := node.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
			CollectionID: 1000,
		})
		assert.NoError(t, err)
		assert.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrServiceNotReady)
		node.UpdateStateCode(commonpb.StateCode_Healthy)
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

	t.Run("failed to list database", func(t *testing.T) {
		r := mocks.NewMockRootCoordClient(t)
		r.On("ListDatabases",
			mock.Anything,
			mock.Anything,
		).Return(nil, errors.New("error mock ListDatabases"))

		node := &Proxy{rootCoord: r}
		node.UpdateStateCode(commonpb.StateCode_Healthy)

		resp, err := node.Connect(context.TODO(), nil)
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("list database error", func(t *testing.T) {
		r := mocks.NewMockRootCoordClient(t)
		r.On("ListDatabases",
			mock.Anything,
			mock.Anything,
		).Return(&milvuspb.ListDatabasesResponse{
			Status: merr.Status(merr.WrapErrServiceNotReady(paramtable.GetRole(), paramtable.GetNodeID(), "initialization")),
		}, nil)

		node := &Proxy{rootCoord: r}
		node.UpdateStateCode(commonpb.StateCode_Healthy)

		resp, err := node.Connect(context.TODO(), nil)
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("database not found", func(t *testing.T) {
		md := metadata.New(map[string]string{
			"dbName": "20230525",
		})
		ctx := metadata.NewIncomingContext(context.TODO(), md)

		r := mocks.NewMockRootCoordClient(t)
		r.On("ListDatabases",
			mock.Anything,
			mock.Anything,
		).Return(&milvuspb.ListDatabasesResponse{
			Status:  merr.Success(),
			DbNames: []string{},
		}, nil)

		node := &Proxy{rootCoord: r}
		node.UpdateStateCode(commonpb.StateCode_Healthy)

		resp, err := node.Connect(ctx, nil)
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("failed to allocate ts", func(t *testing.T) {
		md := metadata.New(map[string]string{
			"dbName": "20230525",
		})
		ctx := metadata.NewIncomingContext(context.TODO(), md)

		r := mocks.NewMockRootCoordClient(t)
		r.On("ListDatabases",
			mock.Anything,
			mock.Anything,
		).Return(&milvuspb.ListDatabasesResponse{
			Status:  merr.Success(),
			DbNames: []string{"20230525"},
		}, nil)

		m := newMockTimestampAllocator(t)
		m.On("AllocTimestamp",
			mock.Anything,
			mock.Anything,
		).Return(nil, errors.New("error mock AllocateTimestamp"))
		alloc, _ := newTimestampAllocator(m, 199)
		node := Proxy{
			tsoAllocator: alloc,
			rootCoord:    r,
		}
		node.UpdateStateCode(commonpb.StateCode_Healthy)
		resp, err := node.Connect(ctx, nil)
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("normal case", func(t *testing.T) {
		md := metadata.New(map[string]string{
			"dbName": "20230525",
		})
		ctx := metadata.NewIncomingContext(context.TODO(), md)

		r := mocks.NewMockRootCoordClient(t)
		r.On("ListDatabases",
			mock.Anything,
			mock.Anything,
		).Return(&milvuspb.ListDatabasesResponse{
			Status:  merr.Success(),
			DbNames: []string{"20230525"},
		}, nil)

		m := newMockTimestampAllocator(t)
		m.On("AllocTimestamp",
			mock.Anything,
			mock.Anything,
		).Return(&rootcoordpb.AllocTimestampResponse{
			Status:    merr.Success(),
			Timestamp: 20230518,
			Count:     1,
		}, nil)
		alloc, _ := newTimestampAllocator(m, 199)
		node := Proxy{
			tsoAllocator: alloc,
			rootCoord:    r,
		}
		node.UpdateStateCode(commonpb.StateCode_Healthy)
		resp, err := node.Connect(ctx, &milvuspb.ConnectRequest{
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
		node := &Proxy{session: &sessionutil.Session{SessionRaw: sessionutil.SessionRaw{ServerID: 1}}}
		node.UpdateStateCode(commonpb.StateCode_Abnormal)
		ctx := context.Background()
		resp, err := node.CreateDatabase(ctx, &milvuspb.CreateDatabaseRequest{})
		assert.NoError(t, err)
		assert.ErrorIs(t, merr.Error(resp), merr.ErrServiceNotReady)
	})

	factory := dependency.NewDefaultFactory(true)
	ctx := context.Background()

	node, err := NewProxy(ctx, factory)
	assert.NoError(t, err)
	node.tsoAllocator = &timestampAllocator{
		tso: newMockTimestampAllocatorInterface(),
	}
	node.simpleLimiter = NewSimpleLimiter(0, 0)
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	node.sched, err = newTaskScheduler(ctx, node.tsoAllocator, node.factory)
	node.sched.ddQueue.setMaxTaskNum(10)
	assert.NoError(t, err)
	err = node.sched.Start()
	assert.NoError(t, err)
	defer node.sched.Close()

	rpcRequestChannel := Params.CommonCfg.ReplicateMsgChannel.GetValue()
	node.replicateMsgStream, err = node.factory.NewMsgStream(node.ctx)
	assert.NoError(t, err)
	node.replicateMsgStream.AsProducer([]string{rpcRequestChannel})

	t.Run("create database fail", func(t *testing.T) {
		rc := mocks.NewMockRootCoordClient(t)
		rc.On("CreateDatabase", mock.Anything, mock.Anything).
			Return(nil, errors.New("fail"))
		node.rootCoord = rc
		ctx := context.Background()
		resp, err := node.CreateDatabase(ctx, &milvuspb.CreateDatabaseRequest{DbName: "db"})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetErrorCode())
	})

	t.Run("create database ok", func(t *testing.T) {
		rc := mocks.NewMockRootCoordClient(t)
		rc.On("CreateDatabase", mock.Anything, mock.Anything).
			Return(merr.Success(), nil)
		node.rootCoord = rc
		node.UpdateStateCode(commonpb.StateCode_Healthy)
		ctx := context.Background()

		resp, err := node.CreateDatabase(ctx, &milvuspb.CreateDatabaseRequest{DbName: "db"})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})
}

func TestProxyDropDatabase(t *testing.T) {
	paramtable.Init()

	t.Run("not healthy", func(t *testing.T) {
		node := &Proxy{session: &sessionutil.Session{SessionRaw: sessionutil.SessionRaw{ServerID: 1}}}
		node.UpdateStateCode(commonpb.StateCode_Abnormal)
		ctx := context.Background()
		resp, err := node.DropDatabase(ctx, &milvuspb.DropDatabaseRequest{})
		assert.NoError(t, err)
		assert.ErrorIs(t, merr.Error(resp), merr.ErrServiceNotReady)
	})

	factory := dependency.NewDefaultFactory(true)
	ctx := context.Background()

	node, err := NewProxy(ctx, factory)
	node.initRateCollector()
	assert.NoError(t, err)
	node.tsoAllocator = &timestampAllocator{
		tso: newMockTimestampAllocatorInterface(),
	}
	node.simpleLimiter = NewSimpleLimiter(0, 0)
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	node.sched, err = newTaskScheduler(ctx, node.tsoAllocator, node.factory)
	node.sched.ddQueue.setMaxTaskNum(10)
	assert.NoError(t, err)
	err = node.sched.Start()
	assert.NoError(t, err)
	defer node.sched.Close()

	rpcRequestChannel := Params.CommonCfg.ReplicateMsgChannel.GetValue()
	node.replicateMsgStream, err = node.factory.NewMsgStream(node.ctx)
	assert.NoError(t, err)
	node.replicateMsgStream.AsProducer([]string{rpcRequestChannel})

	t.Run("drop database fail", func(t *testing.T) {
		rc := mocks.NewMockRootCoordClient(t)
		rc.On("DropDatabase", mock.Anything, mock.Anything).
			Return(nil, errors.New("fail"))
		node.rootCoord = rc
		ctx := context.Background()
		resp, err := node.DropDatabase(ctx, &milvuspb.DropDatabaseRequest{DbName: "db"})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetErrorCode())
	})

	t.Run("drop database ok", func(t *testing.T) {
		rc := mocks.NewMockRootCoordClient(t)
		rc.On("DropDatabase", mock.Anything, mock.Anything).
			Return(merr.Success(), nil)
		node.rootCoord = rc
		node.UpdateStateCode(commonpb.StateCode_Healthy)
		ctx := context.Background()

		resp, err := node.DropDatabase(ctx, &milvuspb.DropDatabaseRequest{DbName: "db"})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})
}

func TestProxyListDatabase(t *testing.T) {
	paramtable.Init()

	t.Run("not healthy", func(t *testing.T) {
		node := &Proxy{session: &sessionutil.Session{SessionRaw: sessionutil.SessionRaw{ServerID: 1}}}
		node.UpdateStateCode(commonpb.StateCode_Abnormal)
		ctx := context.Background()
		resp, err := node.ListDatabases(ctx, &milvuspb.ListDatabasesRequest{})
		assert.NoError(t, err)
		assert.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrServiceNotReady)
	})

	factory := dependency.NewDefaultFactory(true)
	ctx := context.Background()

	node, err := NewProxy(ctx, factory)
	assert.NoError(t, err)
	node.tsoAllocator = &timestampAllocator{
		tso: newMockTimestampAllocatorInterface(),
	}
	node.simpleLimiter = NewSimpleLimiter(0, 0)
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	node.sched, err = newTaskScheduler(ctx, node.tsoAllocator, node.factory)
	node.sched.ddQueue.setMaxTaskNum(10)
	assert.NoError(t, err)
	err = node.sched.Start()
	assert.NoError(t, err)
	defer node.sched.Close()

	t.Run("list database fail", func(t *testing.T) {
		rc := mocks.NewMockRootCoordClient(t)
		rc.On("ListDatabases", mock.Anything, mock.Anything).
			Return(nil, errors.New("fail"))
		node.rootCoord = rc
		ctx := context.Background()
		resp, err := node.ListDatabases(ctx, &milvuspb.ListDatabasesRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
	})

	t.Run("list database ok", func(t *testing.T) {
		rc := mocks.NewMockRootCoordClient(t)
		rc.On("ListDatabases", mock.Anything, mock.Anything).
			Return(&milvuspb.ListDatabasesResponse{
				Status: merr.Success(),
			}, nil)
		node.rootCoord = rc
		node.UpdateStateCode(commonpb.StateCode_Healthy)
		ctx := context.Background()

		resp, err := node.ListDatabases(ctx, &milvuspb.ListDatabasesRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})
}

func TestProxyAlterDatabase(t *testing.T) {
	paramtable.Init()

	t.Run("not healthy", func(t *testing.T) {
		node := &Proxy{session: &sessionutil.Session{SessionRaw: sessionutil.SessionRaw{ServerID: 1}}}
		node.UpdateStateCode(commonpb.StateCode_Abnormal)
		ctx := context.Background()
		resp, err := node.AlterDatabase(ctx, &milvuspb.AlterDatabaseRequest{})
		assert.NoError(t, err)
		assert.ErrorIs(t, merr.Error(resp), merr.ErrServiceNotReady)
	})

	factory := dependency.NewDefaultFactory(true)
	ctx := context.Background()

	node, err := NewProxy(ctx, factory)
	assert.NoError(t, err)
	node.tsoAllocator = &timestampAllocator{
		tso: newMockTimestampAllocatorInterface(),
	}
	node.simpleLimiter = NewSimpleLimiter(0, 0)
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	node.sched, err = newTaskScheduler(ctx, node.tsoAllocator, node.factory)
	node.sched.ddQueue.setMaxTaskNum(10)
	assert.NoError(t, err)
	err = node.sched.Start()
	assert.NoError(t, err)
	defer node.sched.Close()

	t.Run("alter database fail", func(t *testing.T) {
		rc := mocks.NewMockRootCoordClient(t)
		rc.On("AlterDatabase", mock.Anything, mock.Anything).Return(nil, errors.New("fail"))
		node.rootCoord = rc
		ctx := context.Background()
		resp, err := node.AlterDatabase(ctx, &milvuspb.AlterDatabaseRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetErrorCode())
	})

	t.Run("alter database ok", func(t *testing.T) {
		rc := mocks.NewMockRootCoordClient(t)
		rc.On("AlterDatabase", mock.Anything, mock.Anything).
			Return(merr.Success(), nil)
		node.rootCoord = rc
		node.UpdateStateCode(commonpb.StateCode_Healthy)
		ctx := context.Background()

		resp, err := node.AlterDatabase(ctx, &milvuspb.AlterDatabaseRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})
}

func TestProxyDescribeDatabase(t *testing.T) {
	paramtable.Init()

	t.Run("not healthy", func(t *testing.T) {
		node := &Proxy{session: &sessionutil.Session{SessionRaw: sessionutil.SessionRaw{ServerID: 1}}}
		node.UpdateStateCode(commonpb.StateCode_Abnormal)
		ctx := context.Background()
		resp, err := node.DescribeDatabase(ctx, &milvuspb.DescribeDatabaseRequest{})
		assert.NoError(t, err)
		assert.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrServiceNotReady)
	})

	factory := dependency.NewDefaultFactory(true)
	ctx := context.Background()

	node, err := NewProxy(ctx, factory)
	assert.NoError(t, err)
	node.tsoAllocator = &timestampAllocator{
		tso: newMockTimestampAllocatorInterface(),
	}
	node.simpleLimiter = NewSimpleLimiter(0, 0)
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	node.sched, err = newTaskScheduler(ctx, node.tsoAllocator, node.factory)
	node.sched.ddQueue.setMaxTaskNum(10)
	assert.NoError(t, err)
	err = node.sched.Start()
	assert.NoError(t, err)
	defer node.sched.Close()

	t.Run("describe database fail", func(t *testing.T) {
		rc := mocks.NewMockRootCoordClient(t)
		rc.On("DescribeDatabase", mock.Anything, mock.Anything).Return(nil, errors.New("fail"))
		node.rootCoord = rc
		ctx := context.Background()
		resp, err := node.DescribeDatabase(ctx, &milvuspb.DescribeDatabaseRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
	})

	t.Run("describe database ok", func(t *testing.T) {
		rc := mocks.NewMockRootCoordClient(t)
		rc.On("DescribeDatabase", mock.Anything, mock.Anything).Return(&rootcoordpb.DescribeDatabaseResponse{Status: merr.Success()}, nil)
		node.rootCoord = rc
		node.UpdateStateCode(commonpb.StateCode_Healthy)
		ctx := context.Background()

		resp, err := node.DescribeDatabase(ctx, &milvuspb.DescribeDatabaseRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})
}

func TestProxy_AllocTimestamp(t *testing.T) {
	t.Run("proxy unhealthy", func(t *testing.T) {
		node := &Proxy{}
		node.UpdateStateCode(commonpb.StateCode_Abnormal)

		resp, err := node.AllocTimestamp(context.TODO(), nil)
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("success", func(t *testing.T) {
		node := &Proxy{}
		node.UpdateStateCode(commonpb.StateCode_Healthy)

		node.tsoAllocator = &timestampAllocator{
			tso: newMockTimestampAllocatorInterface(),
		}

		resp, err := node.AllocTimestamp(context.TODO(), nil)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("failed", func(t *testing.T) {
		m := newMockTimestampAllocator(t)
		m.On("AllocTimestamp",
			mock.Anything,
			mock.Anything,
		).Return(&rootcoordpb.AllocTimestampResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "failed",
			},
			Timestamp: 20230518,
			Count:     1,
		}, nil)

		alloc, _ := newTimestampAllocator(m, 199)
		node := Proxy{
			tsoAllocator: alloc,
		}
		node.UpdateStateCode(commonpb.StateCode_Healthy)

		resp, err := node.AllocTimestamp(context.TODO(), nil)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
	})
}

func TestProxy_Delete(t *testing.T) {
	collectionName := "test_delete"
	collectionID := int64(111)
	partitionName := "default"
	partitionID := int64(222)
	channels := []string{"test_vchannel"}
	dbName := "test_1"
	collSchema := &schemapb.CollectionSchema{
		Name:        collectionName,
		Description: "",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      common.StartOfUserFieldID,
				Name:         "pk",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:      common.StartOfUserFieldID + 1,
				Name:         "non_pk",
				IsPrimaryKey: false,
				DataType:     schemapb.DataType_Int64,
			},
		},
	}
	schema := newSchemaInfo(collSchema)
	paramtable.Init()

	t.Run("delete run failed", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		chMgr := NewMockChannelsMgr(t)

		req := &milvuspb.DeleteRequest{
			CollectionName: collectionName,
			DbName:         dbName,
			PartitionName:  partitionName,
			Expr:           "pk in [1, 2, 3]",
		}
		cache := NewMockCache(t)
		cache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(&databaseInfo{dbID: 0}, nil)
		cache.On("GetCollectionID",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(collectionID, nil)
		cache.On("GetCollectionSchema",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(schema, nil)
		cache.On("GetPartitionID",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(partitionID, nil)
		chMgr.On("getVChannels", mock.Anything).Return(channels, nil)
		chMgr.On("getChannels", mock.Anything).Return(nil, fmt.Errorf("mock error"))
		globalMetaCache = cache
		rc := mocks.NewMockRootCoordClient(t)
		tsoAllocator := &mockTsoAllocator{}
		idAllocator, err := allocator.NewIDAllocator(ctx, rc, 0)
		assert.NoError(t, err)

		queue, err := newTaskScheduler(ctx, tsoAllocator, nil)
		assert.NoError(t, err)

		node := &Proxy{chMgr: chMgr, rowIDAllocator: idAllocator, sched: queue}
		node.UpdateStateCode(commonpb.StateCode_Healthy)
		resp, err := node.Delete(ctx, req)
		assert.NoError(t, err)
		assert.Error(t, merr.Error(resp.GetStatus()))
	})
}

func TestProxy_ReplicateMessage(t *testing.T) {
	paramtable.Init()
	defer paramtable.Get().Save(paramtable.Get().CommonCfg.TTMsgEnabled.Key, "true")
	t.Run("proxy unhealthy", func(t *testing.T) {
		node := &Proxy{}
		node.UpdateStateCode(commonpb.StateCode_Abnormal)

		resp, err := node.ReplicateMessage(context.TODO(), nil)
		assert.NoError(t, err)
		assert.NotEqual(t, 0, resp.GetStatus().GetCode())
	})

	t.Run("not backup instance", func(t *testing.T) {
		node := &Proxy{}
		node.UpdateStateCode(commonpb.StateCode_Healthy)

		resp, err := node.ReplicateMessage(context.TODO(), nil)
		assert.NoError(t, err)
		assert.NotEqual(t, 0, resp.GetStatus().GetCode())
	})

	t.Run("empty channel name", func(t *testing.T) {
		node := &Proxy{}
		node.UpdateStateCode(commonpb.StateCode_Healthy)
		paramtable.Get().Save(paramtable.Get().CommonCfg.TTMsgEnabled.Key, "false")

		resp, err := node.ReplicateMessage(context.TODO(), nil)
		assert.NoError(t, err)
		assert.NotEqual(t, 0, resp.GetStatus().GetCode())
	})

	t.Run("fail to get msg stream", func(t *testing.T) {
		factory := newMockMsgStreamFactory()
		factory.f = func(ctx context.Context) (msgstream.MsgStream, error) {
			return nil, errors.New("mock error: get msg stream")
		}
		resourceManager := resource.NewManager(time.Second, 2*time.Second, nil)
		manager := NewReplicateStreamManager(context.Background(), factory, resourceManager)

		node := &Proxy{
			replicateStreamManager: manager,
		}
		node.UpdateStateCode(commonpb.StateCode_Healthy)
		paramtable.Get().Save(paramtable.Get().CommonCfg.TTMsgEnabled.Key, "false")

		resp, err := node.ReplicateMessage(context.TODO(), &milvuspb.ReplicateMessageRequest{ChannelName: "unit_test_replicate_message"})
		assert.NoError(t, err)
		assert.NotEqual(t, 0, resp.GetStatus().GetCode())
	})

	t.Run("get latest position", func(t *testing.T) {
		base64DecodeMsgPosition := func(position string) (*msgstream.MsgPosition, error) {
			decodeBytes, err := base64.StdEncoding.DecodeString(position)
			if err != nil {
				log.Warn("fail to decode the position", zap.Error(err))
				return nil, err
			}
			msgPosition := &msgstream.MsgPosition{}
			err = proto.Unmarshal(decodeBytes, msgPosition)
			if err != nil {
				log.Warn("fail to unmarshal the position", zap.Error(err))
				return nil, err
			}
			return msgPosition, nil
		}

		paramtable.Get().Save(paramtable.Get().CommonCfg.TTMsgEnabled.Key, "false")
		defer paramtable.Get().Save(paramtable.Get().CommonCfg.TTMsgEnabled.Key, "true")

		factory := dependency.NewMockFactory(t)
		stream := msgstream.NewMockMsgStream(t)
		mockMsgID := mqcommon.NewMockMessageID(t)

		factory.EXPECT().NewMsgStream(mock.Anything).Return(stream, nil).Once()
		mockMsgID.EXPECT().Serialize().Return([]byte("mock")).Once()
		stream.EXPECT().AsConsumer(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		stream.EXPECT().GetLatestMsgID(mock.Anything).Return(mockMsgID, nil).Once()
		stream.EXPECT().Close().Return()
		node := &Proxy{
			factory: factory,
		}
		node.UpdateStateCode(commonpb.StateCode_Healthy)
		resp, err := node.ReplicateMessage(context.TODO(), &milvuspb.ReplicateMessageRequest{
			ChannelName: Params.CommonCfg.ReplicateMsgChannel.GetValue(),
		})
		assert.NoError(t, err)
		assert.EqualValues(t, 0, resp.GetStatus().GetCode())
		{
			p, err := base64DecodeMsgPosition(resp.GetPosition())
			assert.NoError(t, err)
			assert.Equal(t, []byte("mock"), p.MsgID)
		}

		factory.EXPECT().NewMsgStream(mock.Anything).Return(nil, errors.New("mock")).Once()
		resp, err = node.ReplicateMessage(context.TODO(), &milvuspb.ReplicateMessageRequest{
			ChannelName: Params.CommonCfg.ReplicateMsgChannel.GetValue(),
		})
		assert.NoError(t, err)
		assert.NotEqualValues(t, 0, resp.GetStatus().GetCode())
	})

	t.Run("invalid msg pack", func(t *testing.T) {
		node := &Proxy{
			replicateStreamManager: NewReplicateStreamManager(context.Background(), nil, nil),
		}
		node.UpdateStateCode(commonpb.StateCode_Healthy)
		paramtable.Get().Save(paramtable.Get().CommonCfg.TTMsgEnabled.Key, "false")
		{
			resp, err := node.ReplicateMessage(context.TODO(), &milvuspb.ReplicateMessageRequest{
				ChannelName: "unit_test_replicate_message",
				Msgs:        [][]byte{{1, 2, 3}},
			})
			assert.NoError(t, err)
			assert.NotEqual(t, 0, resp.GetStatus().GetCode())
		}

		{
			timeTickMsg := &msgstream.TimeTickMsg{
				BaseMsg: msgstream.BaseMsg{
					BeginTimestamp: 1,
					EndTimestamp:   10,
					HashValues:     []uint32{0},
				},
				TimeTickMsg: &msgpb.TimeTickMsg{},
			}
			msgBytes, _ := timeTickMsg.Marshal(timeTickMsg)
			resp, err := node.ReplicateMessage(context.TODO(), &milvuspb.ReplicateMessageRequest{
				ChannelName: "unit_test_replicate_message",
				Msgs:        [][]byte{msgBytes.([]byte)},
			})
			assert.NoError(t, err)
			log.Info("resp", zap.Any("resp", resp))
			assert.NotEqual(t, 0, resp.GetStatus().GetCode())
		}

		{
			timeTickMsg := &msgstream.TimeTickMsg{
				BaseMsg: msgstream.BaseMsg{
					BeginTimestamp: 1,
					EndTimestamp:   10,
					HashValues:     []uint32{0},
				},
				TimeTickMsg: &msgpb.TimeTickMsg{
					Base: commonpbutil.NewMsgBase(
						commonpbutil.WithMsgType(commonpb.MsgType(-1)),
						commonpbutil.WithTimeStamp(10),
						commonpbutil.WithSourceID(-1),
					),
				},
			}
			msgBytes, _ := timeTickMsg.Marshal(timeTickMsg)
			resp, err := node.ReplicateMessage(context.TODO(), &milvuspb.ReplicateMessageRequest{
				ChannelName: "unit_test_replicate_message",
				Msgs:        [][]byte{msgBytes.([]byte)},
			})
			assert.NoError(t, err)
			log.Info("resp", zap.Any("resp", resp))
			assert.NotEqual(t, 0, resp.GetStatus().GetCode())
		}
	})

	t.Run("success", func(t *testing.T) {
		paramtable.Init()
		factory := newMockMsgStreamFactory()
		msgStreamObj := msgstream.NewMockMsgStream(t)
		msgStreamObj.EXPECT().SetRepackFunc(mock.Anything).Return()
		msgStreamObj.EXPECT().AsProducer(mock.Anything).Return()
		msgStreamObj.EXPECT().EnableProduce(mock.Anything).Return()
		msgStreamObj.EXPECT().Close().Return()
		mockMsgID1 := mqcommon.NewMockMessageID(t)
		mockMsgID2 := mqcommon.NewMockMessageID(t)
		mockMsgID2.EXPECT().Serialize().Return([]byte("mock message id 2"))
		broadcastMock := msgStreamObj.EXPECT().Broadcast(mock.Anything).Return(map[string][]mqcommon.MessageID{
			"unit_test_replicate_message": {mockMsgID1, mockMsgID2},
		}, nil)

		factory.f = func(ctx context.Context) (msgstream.MsgStream, error) {
			return msgStreamObj, nil
		}
		resourceManager := resource.NewManager(time.Second, 2*time.Second, nil)
		manager := NewReplicateStreamManager(context.Background(), factory, resourceManager)

		ctx := context.Background()
		dataCoord := &mockDataCoord{}
		dataCoord.expireTime = Timestamp(1000)
		segAllocator, err := newSegIDAssigner(ctx, dataCoord, getLastTick1)
		assert.NoError(t, err)
		segAllocator.Start()

		node := &Proxy{
			replicateStreamManager: manager,
			segAssigner:            segAllocator,
		}
		node.UpdateStateCode(commonpb.StateCode_Healthy)
		paramtable.Get().Save(paramtable.Get().CommonCfg.TTMsgEnabled.Key, "false")

		insertMsg := &msgstream.InsertMsg{
			BaseMsg: msgstream.BaseMsg{
				BeginTimestamp: 4,
				EndTimestamp:   10,
				HashValues:     []uint32{0},
				MsgPosition: &msgstream.MsgPosition{
					ChannelName: "unit_test_replicate_message",
					MsgID:       []byte("mock message id 2"),
				},
			},
			InsertRequest: &msgpb.InsertRequest{
				Base: &commonpb.MsgBase{
					MsgType:   commonpb.MsgType_Insert,
					MsgID:     10001,
					Timestamp: 10,
					SourceID:  -1,
				},
				ShardName:      "unit_test_replicate_message_v1",
				DbName:         "default",
				CollectionName: "foo_collection",
				PartitionName:  "_default",
				DbID:           1,
				CollectionID:   11,
				PartitionID:    22,
				SegmentID:      33,
				Timestamps:     []uint64{10},
				RowIDs:         []int64{66},
				NumRows:        1,
			},
		}
		msgBytes, _ := insertMsg.Marshal(insertMsg)

		replicateRequest := &milvuspb.ReplicateMessageRequest{
			ChannelName: "unit_test_replicate_message",
			BeginTs:     1,
			EndTs:       10,
			Msgs:        [][]byte{msgBytes.([]byte)},
			StartPositions: []*msgpb.MsgPosition{
				{ChannelName: "unit_test_replicate_message", MsgID: []byte("mock message id 1")},
			},
			EndPositions: []*msgpb.MsgPosition{
				{ChannelName: "unit_test_replicate_message", MsgID: []byte("mock message id 2")},
			},
		}
		resp, err := node.ReplicateMessage(context.TODO(), replicateRequest)
		assert.NoError(t, err)
		assert.EqualValues(t, 0, resp.GetStatus().GetCode())
		assert.Equal(t, base64.StdEncoding.EncodeToString([]byte("mock message id 2")), resp.GetPosition())

		res := resourceManager.Delete(ReplicateMsgStreamTyp, replicateRequest.GetChannelName())
		assert.NotNil(t, res)
		time.Sleep(2 * time.Second)

		{
			broadcastMock.Unset()
			broadcastMock = msgStreamObj.EXPECT().Broadcast(mock.Anything).Return(nil, errors.New("mock error: broadcast"))
			resp, err := node.ReplicateMessage(context.TODO(), replicateRequest)
			assert.NoError(t, err)
			assert.NotEqualValues(t, 0, resp.GetStatus().GetCode())
			resourceManager.Delete(ReplicateMsgStreamTyp, replicateRequest.GetChannelName())
			time.Sleep(2 * time.Second)
		}
		{
			broadcastMock.Unset()
			broadcastMock = msgStreamObj.EXPECT().Broadcast(mock.Anything).Return(map[string][]mqcommon.MessageID{
				"unit_test_replicate_message": {},
			}, nil)
			resp, err := node.ReplicateMessage(context.TODO(), replicateRequest)
			assert.NoError(t, err)
			assert.EqualValues(t, 0, resp.GetStatus().GetCode())
			assert.Empty(t, resp.GetPosition())
			resourceManager.Delete(ReplicateMsgStreamTyp, replicateRequest.GetChannelName())
			time.Sleep(2 * time.Second)
			broadcastMock.Unset()
		}
	})
}

func TestProxy_ImportV2(t *testing.T) {
	ctx := context.Background()
	mockErr := errors.New("mock error")

	cache := globalMetaCache
	defer func() { globalMetaCache = cache }()

	t.Run("ImportV2", func(t *testing.T) {
		// server is not healthy
		node := &Proxy{}
		node.UpdateStateCode(commonpb.StateCode_Abnormal)
		rsp, err := node.ImportV2(ctx, nil)
		assert.NoError(t, err)
		assert.NotEqual(t, int32(0), rsp.GetStatus().GetCode())
		node.UpdateStateCode(commonpb.StateCode_Healthy)

		// no such collection
		mc := NewMockCache(t)
		mc.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(0, mockErr)
		globalMetaCache = mc
		rsp, err = node.ImportV2(ctx, &internalpb.ImportRequest{CollectionName: "aaa"})
		assert.NoError(t, err)
		assert.NotEqual(t, int32(0), rsp.GetStatus().GetCode())

		// get schema failed
		mc = NewMockCache(t)
		mc.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(0, nil)
		mc.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything, mock.Anything).Return(nil, mockErr)
		globalMetaCache = mc
		rsp, err = node.ImportV2(ctx, &internalpb.ImportRequest{CollectionName: "aaa"})
		assert.NoError(t, err)
		assert.NotEqual(t, int32(0), rsp.GetStatus().GetCode())

		// get channel failed
		mc = NewMockCache(t)
		mc.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(0, nil)
		mc.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything, mock.Anything).Return(&schemaInfo{
			CollectionSchema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
				{IsPartitionKey: true},
			}},
		}, nil)
		globalMetaCache = mc
		chMgr := NewMockChannelsMgr(t)
		chMgr.EXPECT().getVChannels(mock.Anything).Return(nil, mockErr)
		node.chMgr = chMgr
		rsp, err = node.ImportV2(ctx, &internalpb.ImportRequest{CollectionName: "aaa"})
		assert.NoError(t, err)
		assert.NotEqual(t, int32(0), rsp.GetStatus().GetCode())

		// set partition name and with partition key
		chMgr = NewMockChannelsMgr(t)
		chMgr.EXPECT().getVChannels(mock.Anything).Return([]string{"ch0"}, nil)
		node.chMgr = chMgr
		rsp, err = node.ImportV2(ctx, &internalpb.ImportRequest{CollectionName: "aaa", PartitionName: "bbb"})
		assert.NoError(t, err)
		assert.NotEqual(t, int32(0), rsp.GetStatus().GetCode())

		// get partitions failed
		mc = NewMockCache(t)
		mc.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(0, nil)
		mc.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything, mock.Anything).Return(&schemaInfo{
			CollectionSchema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
				{IsPartitionKey: true},
			}},
		}, nil)
		mc.EXPECT().GetPartitions(mock.Anything, mock.Anything, mock.Anything).Return(nil, mockErr)
		globalMetaCache = mc
		rsp, err = node.ImportV2(ctx, &internalpb.ImportRequest{CollectionName: "aaa"})
		assert.NoError(t, err)
		assert.NotEqual(t, int32(0), rsp.GetStatus().GetCode())

		// get partitionID failed
		mc = NewMockCache(t)
		mc.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(0, nil)
		mc.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything, mock.Anything).Return(&schemaInfo{
			CollectionSchema: &schemapb.CollectionSchema{},
		}, nil)
		mc.EXPECT().GetPartitionID(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(0, mockErr)
		globalMetaCache = mc
		rsp, err = node.ImportV2(ctx, &internalpb.ImportRequest{CollectionName: "aaa", PartitionName: "bbb"})
		assert.NoError(t, err)
		assert.NotEqual(t, int32(0), rsp.GetStatus().GetCode())

		// no file
		mc = NewMockCache(t)
		mc.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(0, nil)
		mc.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything, mock.Anything).Return(&schemaInfo{
			CollectionSchema: &schemapb.CollectionSchema{},
		}, nil)
		mc.EXPECT().GetPartitionID(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(0, nil)
		globalMetaCache = mc
		rsp, err = node.ImportV2(ctx, &internalpb.ImportRequest{CollectionName: "aaa", PartitionName: "bbb"})
		assert.NoError(t, err)
		assert.NotEqual(t, int32(0), rsp.GetStatus().GetCode())

		// illegal file type
		rsp, err = node.ImportV2(ctx, &internalpb.ImportRequest{
			CollectionName: "aaa",
			PartitionName:  "bbb",
			Files: []*internalpb.ImportFile{{
				Id:    1,
				Paths: []string{"a.cpp"},
			}},
		})
		assert.NoError(t, err)
		assert.NotEqual(t, int32(0), rsp.GetStatus().GetCode())

		// normal case
		dataCoord := mocks.NewMockDataCoordClient(t)
		dataCoord.EXPECT().ImportV2(mock.Anything, mock.Anything).Return(nil, nil)
		node.dataCoord = dataCoord
		rsp, err = node.ImportV2(ctx, &internalpb.ImportRequest{
			CollectionName: "aaa",
			Files: []*internalpb.ImportFile{{
				Id:    1,
				Paths: []string{"a.json"},
			}},
		})
		assert.NoError(t, err)
		assert.Equal(t, int32(0), rsp.GetStatus().GetCode())
	})

	t.Run("GetImportProgress", func(t *testing.T) {
		// server is not healthy
		node := &Proxy{}
		node.UpdateStateCode(commonpb.StateCode_Abnormal)
		rsp, err := node.GetImportProgress(ctx, nil)
		assert.NoError(t, err)
		assert.NotEqual(t, int32(0), rsp.GetStatus().GetCode())
		node.UpdateStateCode(commonpb.StateCode_Healthy)

		// normal case
		dataCoord := mocks.NewMockDataCoordClient(t)
		dataCoord.EXPECT().GetImportProgress(mock.Anything, mock.Anything).Return(nil, nil)
		node.dataCoord = dataCoord
		rsp, err = node.GetImportProgress(ctx, &internalpb.GetImportProgressRequest{})
		assert.NoError(t, err)
		assert.Equal(t, int32(0), rsp.GetStatus().GetCode())
	})

	t.Run("ListImports", func(t *testing.T) {
		// server is not healthy
		node := &Proxy{}
		node.UpdateStateCode(commonpb.StateCode_Abnormal)
		rsp, err := node.ListImports(ctx, nil)
		assert.NoError(t, err)
		assert.NotEqual(t, int32(0), rsp.GetStatus().GetCode())
		node.UpdateStateCode(commonpb.StateCode_Healthy)

		// normal case
		mc := NewMockCache(t)
		mc.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(0, nil)
		globalMetaCache = mc
		dataCoord := mocks.NewMockDataCoordClient(t)
		dataCoord.EXPECT().ListImports(mock.Anything, mock.Anything).Return(nil, nil)
		node.dataCoord = dataCoord
		rsp, err = node.ListImports(ctx, &internalpb.ListImportsRequest{
			CollectionName: "col",
		})
		assert.NoError(t, err)
		assert.Equal(t, int32(0), rsp.GetStatus().GetCode())
	})
}

func TestGetCollectionRateSubLabel(t *testing.T) {
	d := "db1"
	collectionName := "test1"

	t.Run("normal", func(t *testing.T) {
		subLabel := GetCollectionRateSubLabel(&milvuspb.QueryRequest{
			DbName:         d,
			CollectionName: collectionName,
		})
		assert.Equal(t, ratelimitutil.GetCollectionSubLabel(d, collectionName), subLabel)
	})

	t.Run("fail", func(t *testing.T) {
		{
			subLabel := GetCollectionRateSubLabel(&milvuspb.QueryRequest{
				DbName:         "",
				CollectionName: collectionName,
			})
			assert.Equal(t, "", subLabel)
		}
		{
			subLabel := GetCollectionRateSubLabel(&milvuspb.QueryRequest{
				DbName:         d,
				CollectionName: "",
			})
			assert.Equal(t, "", subLabel)
		}
	})
}

func TestProxy_InvalidateShardLeaderCache(t *testing.T) {
	t.Run("proxy unhealthy", func(t *testing.T) {
		node := &Proxy{}
		node.UpdateStateCode(commonpb.StateCode_Abnormal)

		resp, err := node.InvalidateShardLeaderCache(context.TODO(), nil)
		assert.NoError(t, err)
		assert.False(t, merr.Ok(resp))
	})

	t.Run("success", func(t *testing.T) {
		node := &Proxy{}
		node.UpdateStateCode(commonpb.StateCode_Healthy)

		cacheBak := globalMetaCache
		defer func() { globalMetaCache = cacheBak }()
		// set expectations
		cache := NewMockCache(t)
		cache.EXPECT().InvalidateShardLeaderCache(mock.Anything)
		globalMetaCache = cache

		resp, err := node.InvalidateShardLeaderCache(context.TODO(), &proxypb.InvalidateShardLeaderCacheRequest{})
		assert.NoError(t, err)
		assert.True(t, merr.Ok(resp))
	})
}
