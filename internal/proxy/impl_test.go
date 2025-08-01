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
	"math/rand"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	grpcmixcoordclient "github.com/milvus-io/milvus/internal/distributed/mixcoord/client"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	mhttp "github.com/milvus-io/milvus/internal/http"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/mocks/distributed/mock_streaming"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	mqcommon "github.com/milvus-io/milvus/pkg/v2/mq/common"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/ratelimitutil"
	"github.com/milvus-io/milvus/pkg/v2/util/resource"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
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
		mixc := &mocks.MockMixCoordClient{}
		mixc.EXPECT().CheckHealth(mock.Anything, mock.Anything).Return(&milvuspb.CheckHealthResponse{IsHealthy: true}, nil)
		node := &Proxy{
			mixCoord: mixc,
			session:  &sessionutil.Session{SessionRaw: sessionutil.SessionRaw{ServerID: 1}},
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
		qc := &mocks.MockQueryCoordClient{}
		qc.EXPECT().CheckHealth(mock.Anything, mock.Anything).Return(nil, errors.New("test"))
		node := &Proxy{
			session: &sessionutil.Session{SessionRaw: sessionutil.SessionRaw{ServerID: 1}},
			mixCoord: NewMixCoordMock(func(mock *MixCoordMock) {
				mock.checkHealthFunc = checkHealthFunc1
			}),
		}
		node.simpleLimiter = NewSimpleLimiter(0, 0)
		node.UpdateStateCode(commonpb.StateCode_Healthy)
		ctx := context.Background()
		resp, err := node.CheckHealth(ctx, &milvuspb.CheckHealthRequest{})
		assert.NoError(t, err)
		assert.Equal(t, false, resp.IsHealthy)
		assert.Equal(t, 1, len(resp.Reasons))
	})

	t.Run("check quota state", func(t *testing.T) {
		node := &Proxy{
			mixCoord: NewMixCoordMock(),
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
		rc := mocks.NewMockMixCoordClient(t)
		rc.On("RenameCollection", mock.Anything, mock.Anything).
			Return(nil, errors.New("fail"))
		node := &Proxy{
			session:  &sessionutil.Session{SessionRaw: sessionutil.SessionRaw{ServerID: 1}},
			mixCoord: rc,
		}
		node.UpdateStateCode(commonpb.StateCode_Healthy)
		ctx := context.Background()

		resp, err := node.RenameCollection(ctx, &milvuspb.RenameCollectionRequest{NewName: "new"})
		assert.Error(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetErrorCode())
	})

	t.Run("rename ok", func(t *testing.T) {
		rc := mocks.NewMockMixCoordClient(t)
		rc.On("RenameCollection", mock.Anything, mock.Anything).
			Return(merr.Success(), nil)
		node := &Proxy{
			session:  &sessionutil.Session{SessionRaw: sessionutil.SessionRaw{ServerID: 1}},
			mixCoord: rc,
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

	qc := mocks.NewMockMixCoordClient(t)
	node.SetMixCoordClient(qc)
	qc.EXPECT().ListPolicy(ctx, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()
	qc.EXPECT().ShowLoadCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{}, nil).Maybe()

	tsoAllocatorIns := newMockTsoAllocator()
	node.sched, err = newTaskScheduler(node.ctx, tsoAllocatorIns, node.factory)
	assert.NoError(t, err)
	node.sched.Start()
	defer node.sched.Close()

	mgr := newShardClientMgr()
	InitMetaCache(ctx, qc, mgr)

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

	qc := mocks.NewMockMixCoordClient(t)
	node.SetMixCoordClient(qc)
	qc.EXPECT().DropResourceGroup(mock.Anything, mock.Anything).Return(&commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil)
	qc.EXPECT().ListPolicy(ctx, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()
	tsoAllocatorIns := newMockTsoAllocator()
	node.sched, err = newTaskScheduler(node.ctx, tsoAllocatorIns, node.factory)
	assert.NoError(t, err)
	node.sched.Start()
	defer node.sched.Close()

	mgr := newShardClientMgr()
	InitMetaCache(ctx, qc, mgr)

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

// createTestProxy creates a test proxy instance with all necessary setup
func createTestProxy() *Proxy {
	factory := dependency.NewDefaultFactory(true)
	ctx := context.Background()

	node, _ := NewProxy(ctx, factory)
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	node.tsoAllocator = &timestampAllocator{
		tso: newMockTimestampAllocatorInterface(),
	}

	rpcRequestChannel := Params.CommonCfg.ReplicateMsgChannel.GetValue()
	node.replicateMsgStream, _ = node.factory.NewMsgStream(node.ctx)
	node.replicateMsgStream.AsProducer(ctx, []string{rpcRequestChannel})

	node.sched, _ = newTaskScheduler(ctx, node.tsoAllocator, node.factory)
	node.sched.Start()

	return node
}

func TestProxy_FlushAll_NoDatabase(t *testing.T) {
	mockey.PatchConvey("TestProxy_FlushAll_NoDatabase", t, func() {
		// Mock global meta cache methods
		globalMetaCache = &MetaCache{}
		mockey.Mock(globalMetaCache.GetCollectionID).To(func(ctx context.Context, dbName, collectionName string) (UniqueID, error) {
			return UniqueID(0), nil
		}).Build()
		mockey.Mock(globalMetaCache.RemoveDatabase).To(func(ctx context.Context, dbName string) error {
			return nil
		}).Build()

		// Mock paramtable initialization
		mockey.Mock(paramtable.Init).Return().Build()
		mockey.Mock((*paramtable.ComponentParam).Save).Return().Build()

		// Mock grpc mix coord client FlushAll method
		successStatus := &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}
		mockey.Mock((*grpcmixcoordclient.Client).FlushAll).To(func(ctx context.Context, req *datapb.FlushAllRequest, opts ...grpc.CallOption) (*datapb.FlushAllResponse, error) {
			return &datapb.FlushAllResponse{Status: successStatus}, nil
		}).Build()

		// Act: Execute test
		node := createTestProxy()
		defer node.sched.Close()

		mixcoord := &grpcmixcoordclient.Client{}
		node.mixCoord = mixcoord

		resp, err := node.FlushAll(context.Background(), &milvuspb.FlushAllRequest{})

		// Assert: Verify results
		assert.NoError(t, err)
		assert.True(t, merr.Ok(resp.GetStatus()))
	})
}

func TestProxy_FlushAll_WithDefaultDatabase(t *testing.T) {
	mockey.PatchConvey("TestProxy_FlushAll_WithDefaultDatabase", t, func() {
		// Mock global meta cache methods
		globalMetaCache = &MetaCache{}
		mockey.Mock(globalMetaCache.GetCollectionID).To(func(ctx context.Context, dbName, collectionName string) (UniqueID, error) {
			return UniqueID(0), nil
		}).Build()
		mockey.Mock(globalMetaCache.RemoveDatabase).To(func(ctx context.Context, dbName string) error {
			return nil
		}).Build()

		// Mock paramtable initialization
		mockey.Mock(paramtable.Init).Return().Build()
		mockey.Mock((*paramtable.ComponentParam).Save).Return().Build()

		// Mock grpc mix coord client FlushAll method for default database
		successStatus := &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}
		mockey.Mock((*grpcmixcoordclient.Client).FlushAll).To(func(ctx context.Context, req *datapb.FlushAllRequest, opts ...grpc.CallOption) (*datapb.FlushAllResponse, error) {
			return &datapb.FlushAllResponse{Status: successStatus}, nil
		}).Build()

		// Act: Execute test
		node := createTestProxy()
		defer node.sched.Close()

		mixcoord := &grpcmixcoordclient.Client{}
		node.mixCoord = mixcoord

		resp, err := node.FlushAll(context.Background(), &milvuspb.FlushAllRequest{DbName: "default"})

		// Assert: Verify results
		assert.NoError(t, err)
		assert.True(t, merr.Ok(resp.GetStatus()))
	})
}

func TestProxy_FlushAll_DatabaseNotExist(t *testing.T) {
	mockey.PatchConvey("TestProxy_FlushAll_DatabaseNotExist", t, func() {
		// Mock global meta cache methods
		globalMetaCache = &MetaCache{}
		mockey.Mock(globalMetaCache.GetCollectionID).To(func(ctx context.Context, dbName, collectionName string) (UniqueID, error) {
			return UniqueID(0), nil
		}).Build()
		mockey.Mock(globalMetaCache.RemoveDatabase).To(func(ctx context.Context, dbName string) error {
			return nil
		}).Build()

		// Mock paramtable initialization
		mockey.Mock(paramtable.Init).Return().Build()
		mockey.Mock((*paramtable.ComponentParam).Save).Return().Build()

		// Mock grpc mix coord client FlushAll method for non-existent database
		mockey.Mock((*grpcmixcoordclient.Client).FlushAll).To(func(ctx context.Context, req *datapb.FlushAllRequest, opts ...grpc.CallOption) (*datapb.FlushAllResponse, error) {
			return &datapb.FlushAllResponse{Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_MetaFailed}}, nil
		}).Build()

		// Act: Execute test
		node := createTestProxy()
		defer node.sched.Close()

		mixcoord := &grpcmixcoordclient.Client{}
		node.mixCoord = mixcoord

		resp, err := node.FlushAll(context.Background(), &milvuspb.FlushAllRequest{DbName: "default2"})

		// Assert: Verify results
		assert.NoError(t, err)
		assert.NotEqual(t, resp.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
	})
}

func TestProxy_FlushAll_ServerAbnormal(t *testing.T) {
	mockey.PatchConvey("TestProxy_FlushAll_ServerAbnormal", t, func() {
		// Mock global meta cache methods
		globalMetaCache = &MetaCache{}
		mockey.Mock(globalMetaCache.GetCollectionID).To(func(ctx context.Context, dbName, collectionName string) (UniqueID, error) {
			return UniqueID(0), nil
		}).Build()
		mockey.Mock(globalMetaCache.RemoveDatabase).To(func(ctx context.Context, dbName string) error {
			return nil
		}).Build()

		// Mock paramtable initialization
		mockey.Mock(paramtable.Init).Return().Build()
		mockey.Mock((*paramtable.ComponentParam).Save).Return().Build()

		// Act: Execute test
		node := createTestProxy()
		defer node.sched.Close()

		mixcoord := &grpcmixcoordclient.Client{}
		node.mixCoord = mixcoord

		// Set node state to abnormal
		node.UpdateStateCode(commonpb.StateCode_Abnormal)
		resp, err := node.FlushAll(context.Background(), &milvuspb.FlushAllRequest{})

		// Assert: Verify results
		assert.NoError(t, err)
		assert.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrServiceNotReady)
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
	node.mixCoord = mocks.NewMockMixCoordClient(t)

	// set expectations
	successStatus := &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}
	node.mixCoord.(*mocks.MockMixCoordClient).EXPECT().GetFlushAllState(mock.Anything, mock.Anything).
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
		node.mixCoord.(*mocks.MockMixCoordClient).ExpectedCalls = nil
		node.mixCoord.(*mocks.MockMixCoordClient).EXPECT().GetFlushAllState(mock.Anything, mock.Anything).
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
	node.mixCoord = mocks.NewMockMixCoordClient(t)

	// set expectations
	successStatus := &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}
	node.mixCoord.(*mocks.MockMixCoordClient).EXPECT().GetFlushState(mock.Anything, mock.Anything, mock.Anything).
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
		node.mixCoord.(*mocks.MockMixCoordClient).ExpectedCalls = nil
		node.mixCoord.(*mocks.MockMixCoordClient).EXPECT().GetFlushState(mock.Anything, mock.Anything, mock.Anything).
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
		node.mixCoord.(*mocks.MockMixCoordClient).ExpectedCalls = nil
		node.mixCoord.(*mocks.MockMixCoordClient).EXPECT().GetFlushState(mock.Anything, mock.Anything, mock.Anything).
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
	mockQC := mocks.NewMockMixCoordClient(t)
	node.mixCoord = mockQC

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
		r := mocks.NewMockMixCoordClient(t)
		r.On("ListDatabases",
			mock.Anything,
			mock.Anything,
		).Return(nil, errors.New("error mock ListDatabases"))

		node := &Proxy{mixCoord: r}
		node.UpdateStateCode(commonpb.StateCode_Healthy)

		resp, err := node.Connect(context.TODO(), nil)
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("list database error", func(t *testing.T) {
		r := mocks.NewMockMixCoordClient(t)
		r.On("ListDatabases",
			mock.Anything,
			mock.Anything,
		).Return(&milvuspb.ListDatabasesResponse{
			Status: merr.Status(merr.WrapErrServiceNotReady(paramtable.GetRole(), paramtable.GetNodeID(), "initialization")),
		}, nil)

		node := &Proxy{mixCoord: r}
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

		r := mocks.NewMockMixCoordClient(t)
		r.On("ListDatabases",
			mock.Anything,
			mock.Anything,
		).Return(&milvuspb.ListDatabasesResponse{
			Status:  merr.Success(),
			DbNames: []string{},
		}, nil)

		node := &Proxy{mixCoord: r}
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

		r := mocks.NewMockMixCoordClient(t)
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
			mixCoord:     r,
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

		r := mocks.NewMockMixCoordClient(t)
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
			mixCoord:     r,
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
	node.replicateMsgStream.AsProducer(ctx, []string{rpcRequestChannel})

	t.Run("create database fail", func(t *testing.T) {
		mixc := mocks.NewMockMixCoordClient(t)
		mixc.On("CreateDatabase", mock.Anything, mock.Anything).
			Return(nil, errors.New("fail"))
		node.mixCoord = mixc
		ctx := context.Background()
		resp, err := node.CreateDatabase(ctx, &milvuspb.CreateDatabaseRequest{DbName: "db"})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetErrorCode())
	})

	t.Run("create database ok", func(t *testing.T) {
		mixc := mocks.NewMockMixCoordClient(t)
		mixc.On("CreateDatabase", mock.Anything, mock.Anything).
			Return(merr.Success(), nil)
		node.mixCoord = mixc
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
	node.replicateMsgStream.AsProducer(ctx, []string{rpcRequestChannel})

	t.Run("drop database fail", func(t *testing.T) {
		mixc := mocks.NewMockMixCoordClient(t)
		mixc.On("DropDatabase", mock.Anything, mock.Anything).
			Return(nil, errors.New("fail"))
		node.mixCoord = mixc
		ctx := context.Background()
		resp, err := node.DropDatabase(ctx, &milvuspb.DropDatabaseRequest{DbName: "db"})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetErrorCode())
	})

	t.Run("drop database ok", func(t *testing.T) {
		mix := mocks.NewMockMixCoordClient(t)
		mix.EXPECT().DropDatabase(mock.Anything, mock.Anything).Return(merr.Success(), nil)
		node.mixCoord = mix
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
		mixc := mocks.NewMockMixCoordClient(t)
		mixc.On("ListDatabases", mock.Anything, mock.Anything).
			Return(nil, errors.New("fail"))
		node.mixCoord = mixc
		ctx := context.Background()
		resp, err := node.ListDatabases(ctx, &milvuspb.ListDatabasesRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
	})

	t.Run("list database ok", func(t *testing.T) {
		mixc := mocks.NewMockMixCoordClient(t)
		mixc.On("ListDatabases", mock.Anything, mock.Anything).
			Return(&milvuspb.ListDatabasesResponse{
				Status: merr.Success(),
			}, nil)
		node.mixCoord = mixc
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
		mixc := mocks.NewMockMixCoordClient(t)
		mixc.On("AlterDatabase", mock.Anything, mock.Anything).Return(nil, errors.New("fail"))
		node.mixCoord = mixc
		ctx := context.Background()
		resp, err := node.AlterDatabase(ctx, &milvuspb.AlterDatabaseRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetErrorCode())
	})

	t.Run("alter database ok", func(t *testing.T) {
		mixc := mocks.NewMockMixCoordClient(t)
		mixc.On("AlterDatabase", mock.Anything, mock.Anything).
			Return(merr.Success(), nil)
		node.mixCoord = mixc
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
		mixc := mocks.NewMockMixCoordClient(t)
		mixc.On("DescribeDatabase", mock.Anything, mock.Anything).Return(nil, errors.New("fail"))
		node.mixCoord = mixc
		ctx := context.Background()
		resp, err := node.DescribeDatabase(ctx, &milvuspb.DescribeDatabaseRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
	})

	t.Run("describe database ok", func(t *testing.T) {
		mixc := mocks.NewMockMixCoordClient(t)
		mixc.On("DescribeDatabase", mock.Anything, mock.Anything).Return(&rootcoordpb.DescribeDatabaseResponse{Status: merr.Success()}, nil)
		node.mixCoord = mixc
		node.UpdateStateCode(commonpb.StateCode_Healthy)
		ctx := context.Background()

		resp, err := node.DescribeDatabase(ctx, &milvuspb.DescribeDatabaseRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})
}

func TestProxyDescribeCollection(t *testing.T) {
	paramtable.Init()
	node := &Proxy{session: &sessionutil.Session{SessionRaw: sessionutil.SessionRaw{ServerID: 1}}}
	ctx := context.Background()
	mixCoord := mocks.NewMockMixCoordClient(t)
	mixCoord.On("DescribeCollection", mock.Anything, mock.MatchedBy(func(req *milvuspb.DescribeCollectionRequest) bool {
		return req.DbName == "test_1" && req.CollectionName == "test_collection"
	})).Return(&milvuspb.DescribeCollectionResponse{
		Status:       merr.Success(),
		CollectionID: 1,
		Schema: &schemapb.CollectionSchema{
			Name: "test_collection",
			Fields: []*schemapb.FieldSchema{
				{Name: "pk", DataType: schemapb.DataType_Int64},
			},
		},
	}, nil).Maybe()
	mixCoord.On("ShowPartitions", mock.Anything, mock.Anything).Return(&milvuspb.ShowPartitionsResponse{
		Status:               merr.Success(),
		PartitionNames:       []string{"default"},
		CreatedTimestamps:    []uint64{1},
		CreatedUtcTimestamps: []uint64{1},
		PartitionIDs:         []int64{1},
	}, nil).Maybe()
	mixCoord.On("ShowLoadCollections", mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{
		Status: merr.Success(),
	}, nil).Maybe()
	mixCoord.On("DescribeCollection", mock.Anything, mock.Anything).Return(nil, merr.ErrCollectionNotFound).Maybe()
	var err error
	globalMetaCache, err = NewMetaCache(mixCoord, nil)
	assert.NoError(t, err)

	t.Run("not healthy", func(t *testing.T) {
		node.UpdateStateCode(commonpb.StateCode_Abnormal)
		defer node.UpdateStateCode(commonpb.StateCode_Healthy)
		resp, err := node.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("collection not exists", func(t *testing.T) {
		resp, err := node.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
			DbName:         "test_1",
			CollectionName: "test_not_exists",
		})
		assert.NoError(t, err)
		assert.Contains(t, resp.GetStatus().GetReason(), "can't find collection[database=test_1][collection=test_not_exists]")
		assert.Equal(t, commonpb.ErrorCode_CollectionNotExists, resp.GetStatus().GetErrorCode())
	})

	t.Run("collection id not exists", func(t *testing.T) {
		resp, err := node.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
			DbName:       "test_1",
			CollectionID: 1000,
		})
		assert.NoError(t, err)
		assert.Contains(t, resp.GetStatus().GetReason(), "can't find collection[database=test_1][collection=]")
		assert.Equal(t, commonpb.ErrorCode_CollectionNotExists, resp.GetStatus().GetErrorCode())
	})

	t.Run("db not exists", func(t *testing.T) {
		resp, err := node.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
			DbName:         "db_not_exists",
			CollectionName: "test_collection",
		})
		assert.NoError(t, err)
		assert.Contains(t, resp.GetStatus().GetReason(), "can't find collection[database=db_not_exists][collection=test_collection]")
		assert.Equal(t, commonpb.ErrorCode_CollectionNotExists, resp.GetStatus().GetErrorCode())
	})

	t.Run("describe collection ok", func(t *testing.T) {
		resp, err := node.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
			DbName:         "test_1",
			CollectionName: "test_collection",
		})
		assert.NoError(t, err)
		assert.Empty(t, resp.GetStatus().GetReason())
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
	basicInfo := &collectionInfo{
		collID: collectionID,
	}
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
		cache.On("GetCollectionInfo", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(basicInfo, nil)
		chMgr.On("getVChannels", mock.Anything).Return(channels, nil)
		chMgr.On("getChannels", mock.Anything).Return(nil, errors.New("mock error"))
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
		msgStreamObj.EXPECT().AsProducer(mock.Anything, mock.Anything).Return()
		msgStreamObj.EXPECT().ForceEnableProduce(mock.Anything).Return()
		msgStreamObj.EXPECT().Close().Return()
		mockMsgID1 := mqcommon.NewMockMessageID(t)
		mockMsgID2 := mqcommon.NewMockMessageID(t)
		mockMsgID2.EXPECT().Serialize().Return([]byte("mock message id 2"))
		broadcastMock := msgStreamObj.EXPECT().Broadcast(mock.Anything, mock.Anything).Return(map[string][]mqcommon.MessageID{
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
			broadcastMock = msgStreamObj.EXPECT().Broadcast(mock.Anything, mock.Anything).Return(nil, errors.New("mock error: broadcast"))
			resp, err := node.ReplicateMessage(context.TODO(), replicateRequest)
			assert.NoError(t, err)
			assert.NotEqualValues(t, 0, resp.GetStatus().GetCode())
			resourceManager.Delete(ReplicateMsgStreamTyp, replicateRequest.GetChannelName())
			time.Sleep(2 * time.Second)
		}
		{
			broadcastMock.Unset()
			broadcastMock = msgStreamObj.EXPECT().Broadcast(mock.Anything, mock.Anything).Return(map[string][]mqcommon.MessageID{
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
	wal := mock_streaming.NewMockWALAccesser(t)
	b := mock_streaming.NewMockBroadcast(t)
	wal.EXPECT().Broadcast().Return(b).Maybe()
	b.EXPECT().Append(mock.Anything, mock.Anything).Return(&types.BroadcastAppendResult{}, nil).Maybe()
	streaming.SetWALForTest(wal)
	defer streaming.RecoverWALForTest()
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

		factory := dependency.NewDefaultFactory(true)
		node, err = NewProxy(ctx, factory)
		assert.NoError(t, err)
		node.UpdateStateCode(commonpb.StateCode_Healthy)
		node.tsoAllocator = &timestampAllocator{
			tso: newMockTimestampAllocatorInterface(),
		}
		scheduler, err := newTaskScheduler(ctx, node.tsoAllocator, factory)
		assert.NoError(t, err)
		node.sched = scheduler
		err = node.sched.Start()
		assert.NoError(t, err)
		chMgr := NewMockChannelsMgr(t)
		node.chMgr = chMgr

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
		chMgr.EXPECT().getVChannels(mock.Anything).Return(nil, mockErr).Once()
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
		rc := mocks.NewMockRootCoordClient(t)
		rc.EXPECT().AllocID(mock.Anything, mock.Anything).Return(&rootcoordpb.AllocIDResponse{
			ID:    rand.Int63(),
			Count: 1,
		}, nil).Once()
		idAllocator, err := allocator.NewIDAllocator(ctx, rc, 0)
		assert.NoError(t, err)
		node.rowIDAllocator = idAllocator
		err = idAllocator.Start()
		assert.NoError(t, err)

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
		mixCoord := mocks.NewMockMixCoordClient(t)
		mixCoord.EXPECT().GetImportProgress(mock.Anything, mock.Anything).Return(nil, nil)
		node.mixCoord = mixCoord
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
		mixCoord := mocks.NewMockMixCoordClient(t)
		mixCoord.EXPECT().ListImports(mock.Anything, mock.Anything).Return(nil, nil)
		node.mixCoord = mixCoord
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

func TestRegisterRestRouter(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()

	mixc := mocks.NewMockMixCoordClient(t)
	mixc.EXPECT().GetMetrics(mock.Anything, mock.Anything).Return(nil, errors.New("error"))

	proxy := &Proxy{
		mixCoord: mixc,
	}
	proxy.RegisterRestRouter(router)

	tests := []struct {
		path       string
		statusCode int
	}{
		{path: mhttp.QCTargetPath, statusCode: http.StatusInternalServerError},
		{path: mhttp.QCDistPath, statusCode: http.StatusInternalServerError},
		{path: mhttp.QCAllTasksPath, statusCode: http.StatusInternalServerError},
		{path: mhttp.DNSyncTasksPath, statusCode: http.StatusInternalServerError},
		{path: mhttp.DCCompactionTasksPath, statusCode: http.StatusInternalServerError},
		{path: mhttp.DCImportTasksPath, statusCode: http.StatusInternalServerError},
		{path: mhttp.DCBuildIndexTasksPath, statusCode: http.StatusInternalServerError},
		{path: mhttp.DNSyncTasksPath, statusCode: http.StatusInternalServerError},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			req, _ := http.NewRequest("GET", tt.path, nil)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			assert.Equal(t, tt.statusCode, w.Code)
		})
	}
}

func TestReplicateMessageForCollectionMode(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	insertMsg := &msgstream.InsertMsg{
		BaseMsg: msgstream.BaseMsg{
			BeginTimestamp: 10,
			EndTimestamp:   10,
			HashValues:     []uint32{0},
			MsgPosition: &msgstream.MsgPosition{
				ChannelName: "foo",
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
			ShardName:      "foo_v1",
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
	insertMsgBytes, _ := insertMsg.Marshal(insertMsg)
	deleteMsg := &msgstream.DeleteMsg{
		BaseMsg: msgstream.BaseMsg{
			BeginTimestamp: 20,
			EndTimestamp:   20,
			HashValues:     []uint32{0},
			MsgPosition: &msgstream.MsgPosition{
				ChannelName: "foo",
				MsgID:       []byte("mock message id 2"),
			},
		},
		DeleteRequest: &msgpb.DeleteRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Delete,
				MsgID:     10002,
				Timestamp: 20,
				SourceID:  -1,
			},
			ShardName:      "foo_v1",
			DbName:         "default",
			CollectionName: "foo_collection",
			PartitionName:  "_default",
			DbID:           1,
			CollectionID:   11,
			PartitionID:    22,
		},
	}
	deleteMsgBytes, _ := deleteMsg.Marshal(deleteMsg)

	cache := globalMetaCache
	defer func() { globalMetaCache = cache }()

	t.Run("replicate message in the replicate collection mode", func(t *testing.T) {
		defer func() {
			paramtable.Get().Reset(paramtable.Get().CommonCfg.TTMsgEnabled.Key)
			paramtable.Get().Reset(paramtable.Get().CommonCfg.CollectionReplicateEnable.Key)
		}()

		{
			paramtable.Get().Save(paramtable.Get().CommonCfg.TTMsgEnabled.Key, "true")
			paramtable.Get().Save(paramtable.Get().CommonCfg.CollectionReplicateEnable.Key, "false")
			p := &Proxy{}
			p.UpdateStateCode(commonpb.StateCode_Healthy)
			r, err := p.ReplicateMessage(ctx, &milvuspb.ReplicateMessageRequest{
				ChannelName: "foo",
			})
			assert.NoError(t, err)
			assert.Error(t, merr.Error(r.Status))
		}

		{
			paramtable.Get().Save(paramtable.Get().CommonCfg.TTMsgEnabled.Key, "false")
			paramtable.Get().Save(paramtable.Get().CommonCfg.CollectionReplicateEnable.Key, "true")
			p := &Proxy{}
			p.UpdateStateCode(commonpb.StateCode_Healthy)
			r, err := p.ReplicateMessage(ctx, &milvuspb.ReplicateMessageRequest{
				ChannelName: "foo",
			})
			assert.NoError(t, err)
			assert.Error(t, merr.Error(r.Status))
		}
	})

	t.Run("replicate message for the replicate collection mode", func(t *testing.T) {
		paramtable.Get().Save(paramtable.Get().CommonCfg.TTMsgEnabled.Key, "true")
		paramtable.Get().Save(paramtable.Get().CommonCfg.CollectionReplicateEnable.Key, "true")
		defer func() {
			paramtable.Get().Reset(paramtable.Get().CommonCfg.TTMsgEnabled.Key)
			paramtable.Get().Reset(paramtable.Get().CommonCfg.CollectionReplicateEnable.Key)
		}()

		mockCache := NewMockCache(t)
		mockCache.EXPECT().GetCollectionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&collectionInfo{}, nil).Twice()
		mockCache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(&databaseInfo{}, nil).Twice()
		globalMetaCache = mockCache

		{
			p := &Proxy{
				replicateStreamManager: NewReplicateStreamManager(context.Background(), nil, nil),
			}
			p.UpdateStateCode(commonpb.StateCode_Healthy)
			r, err := p.ReplicateMessage(ctx, &milvuspb.ReplicateMessageRequest{
				ChannelName: "foo",
				Msgs:        [][]byte{insertMsgBytes.([]byte)},
			})
			assert.NoError(t, err)
			assert.EqualValues(t, r.GetStatus().GetCode(), merr.Code(merr.ErrCollectionReplicateMode))
		}

		{
			p := &Proxy{
				replicateStreamManager: NewReplicateStreamManager(context.Background(), nil, nil),
			}
			p.UpdateStateCode(commonpb.StateCode_Healthy)
			r, err := p.ReplicateMessage(ctx, &milvuspb.ReplicateMessageRequest{
				ChannelName: "foo",
				Msgs:        [][]byte{deleteMsgBytes.([]byte)},
			})
			assert.NoError(t, err)
			assert.EqualValues(t, r.GetStatus().GetCode(), merr.Code(merr.ErrCollectionReplicateMode))
		}
	})
}

func TestAlterCollectionReplicateProperty(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().CommonCfg.TTMsgEnabled.Key, "true")
	paramtable.Get().Save(paramtable.Get().CommonCfg.CollectionReplicateEnable.Key, "true")
	defer func() {
		paramtable.Get().Reset(paramtable.Get().CommonCfg.TTMsgEnabled.Key)
		paramtable.Get().Reset(paramtable.Get().CommonCfg.CollectionReplicateEnable.Key)
	}()
	cache := globalMetaCache
	defer func() { globalMetaCache = cache }()
	mockCache := NewMockCache(t)
	mockCache.EXPECT().GetCollectionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&collectionInfo{
		replicateID: "local-milvus",
	}, nil).Maybe()
	mockCache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(1, nil).Maybe()
	mockCache.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything, mock.Anything).Return(&schemaInfo{}, nil)
	globalMetaCache = mockCache

	factory := newMockMsgStreamFactory()
	msgStreamObj := msgstream.NewMockMsgStream(t)
	msgStreamObj.EXPECT().SetRepackFunc(mock.Anything).Return().Maybe()
	msgStreamObj.EXPECT().AsProducer(mock.Anything, mock.Anything).Return().Maybe()
	msgStreamObj.EXPECT().ForceEnableProduce(mock.Anything).Return().Maybe()
	msgStreamObj.EXPECT().Close().Return().Maybe()
	mockMsgID1 := mqcommon.NewMockMessageID(t)
	mockMsgID2 := mqcommon.NewMockMessageID(t)
	mockMsgID2.EXPECT().Serialize().Return([]byte("mock message id 2")).Maybe()
	msgStreamObj.EXPECT().Broadcast(mock.Anything, mock.Anything).Return(map[string][]mqcommon.MessageID{
		"alter_property": {mockMsgID1, mockMsgID2},
	}, nil).Maybe()

	factory.f = func(ctx context.Context) (msgstream.MsgStream, error) {
		return msgStreamObj, nil
	}
	resourceManager := resource.NewManager(time.Second, 2*time.Second, nil)
	manager := NewReplicateStreamManager(context.Background(), factory, resourceManager)

	ctx := context.Background()
	var startTt uint64 = 10
	startTime := time.Now()
	dataCoord := &mockDataCoord{}
	dataCoord.expireTime = Timestamp(1000)
	segAllocator, err := newSegIDAssigner(ctx, dataCoord, func() Timestamp {
		return Timestamp(time.Since(startTime).Seconds()) + startTt
	})
	assert.NoError(t, err)
	segAllocator.Start()

	mockMixcoord := mocks.NewMockMixCoordClient(t)
	mockMixcoord.EXPECT().AllocTimestamp(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, request *rootcoordpb.AllocTimestampRequest, option ...grpc.CallOption) (*rootcoordpb.AllocTimestampResponse, error) {
		return &rootcoordpb.AllocTimestampResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
			Timestamp: Timestamp(time.Since(startTime).Seconds()) + startTt,
		}, nil
	})
	mockMixcoord.EXPECT().AlterCollection(mock.Anything, mock.Anything).Return(&commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}, nil)

	p := &Proxy{
		ctx:                    ctx,
		replicateStreamManager: manager,
		segAssigner:            segAllocator,
		mixCoord:               mockMixcoord,
	}
	tsoAllocatorIns := newMockTsoAllocator()
	p.sched, err = newTaskScheduler(p.ctx, tsoAllocatorIns, p.factory)
	assert.NoError(t, err)
	p.sched.Start()
	defer p.sched.Close()
	p.UpdateStateCode(commonpb.StateCode_Healthy)

	getInsertMsgBytes := func(channel string, ts uint64) []byte {
		insertMsg := &msgstream.InsertMsg{
			BaseMsg: msgstream.BaseMsg{
				BeginTimestamp: ts,
				EndTimestamp:   ts,
				HashValues:     []uint32{0},
				MsgPosition: &msgstream.MsgPosition{
					ChannelName: channel,
					MsgID:       []byte("mock message id 2"),
				},
			},
			InsertRequest: &msgpb.InsertRequest{
				Base: &commonpb.MsgBase{
					MsgType:   commonpb.MsgType_Insert,
					MsgID:     10001,
					Timestamp: ts,
					SourceID:  -1,
				},
				ShardName:      channel + "_v1",
				DbName:         "default",
				CollectionName: "foo_collection",
				PartitionName:  "_default",
				DbID:           1,
				CollectionID:   11,
				PartitionID:    22,
				SegmentID:      33,
				Timestamps:     []uint64{ts},
				RowIDs:         []int64{66},
				NumRows:        1,
			},
		}
		insertMsgBytes, _ := insertMsg.Marshal(insertMsg)
		return insertMsgBytes.([]byte)
	}
	getDeleteMsgBytes := func(channel string, ts uint64) []byte {
		deleteMsg := &msgstream.DeleteMsg{
			BaseMsg: msgstream.BaseMsg{
				BeginTimestamp: ts,
				EndTimestamp:   ts,
				HashValues:     []uint32{0},
				MsgPosition: &msgstream.MsgPosition{
					ChannelName: "foo",
					MsgID:       []byte("mock message id 2"),
				},
			},
			DeleteRequest: &msgpb.DeleteRequest{
				Base: &commonpb.MsgBase{
					MsgType:   commonpb.MsgType_Delete,
					MsgID:     10002,
					Timestamp: ts,
					SourceID:  -1,
				},
				ShardName:      channel + "_v1",
				DbName:         "default",
				CollectionName: "foo_collection",
				PartitionName:  "_default",
				DbID:           1,
				CollectionID:   11,
				PartitionID:    22,
			},
		}
		deleteMsgBytes, _ := deleteMsg.Marshal(deleteMsg)
		return deleteMsgBytes.([]byte)
	}

	go func() {
		// replicate message
		var replicateResp *milvuspb.ReplicateMessageResponse
		var err error
		replicateResp, err = p.ReplicateMessage(ctx, &milvuspb.ReplicateMessageRequest{
			ChannelName: "alter_property_1",
			Msgs:        [][]byte{getInsertMsgBytes("alter_property_1", startTt+5)},
		})
		assert.NoError(t, err)
		assert.True(t, merr.Ok(replicateResp.Status), replicateResp.Status.Reason)

		replicateResp, err = p.ReplicateMessage(ctx, &milvuspb.ReplicateMessageRequest{
			ChannelName: "alter_property_2",
			Msgs:        [][]byte{getDeleteMsgBytes("alter_property_2", startTt+5)},
		})
		assert.NoError(t, err)
		assert.True(t, merr.Ok(replicateResp.Status), replicateResp.Status.Reason)

		time.Sleep(time.Second)

		replicateResp, err = p.ReplicateMessage(ctx, &milvuspb.ReplicateMessageRequest{
			ChannelName: "alter_property_1",
			Msgs:        [][]byte{getInsertMsgBytes("alter_property_1", startTt+10)},
		})
		assert.NoError(t, err)
		assert.False(t, merr.Ok(replicateResp.Status), replicateResp.Status.Reason)

		replicateResp, err = p.ReplicateMessage(ctx, &milvuspb.ReplicateMessageRequest{
			ChannelName: "alter_property_2",
			Msgs:        [][]byte{getInsertMsgBytes("alter_property_2", startTt+10)},
		})
		assert.NoError(t, err)
		assert.False(t, merr.Ok(replicateResp.Status), replicateResp.Status.Reason)
	}()
	time.Sleep(200 * time.Millisecond)

	// alter collection property
	statusResp, err := p.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{
		DbName:         "default",
		CollectionName: "foo_collection",
		Properties: []*commonpb.KeyValuePair{
			{
				Key:   "replicate.endTS",
				Value: "1",
			},
		},
	})
	assert.NoError(t, err)
	assert.True(t, merr.Ok(statusResp))
}

func TestRunAnalyzer(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	cache := globalMetaCache
	globalMetaCache = nil
	defer func() { globalMetaCache = cache }()

	p := &Proxy{}

	tsoAllocatorIns := newMockTsoAllocator()
	sched, err := newTaskScheduler(ctx, tsoAllocatorIns, p.factory)
	require.NoError(t, err)
	sched.Start()
	defer sched.Close()

	p.sched = sched

	t.Run("run analyzer err with node not healthy", func(t *testing.T) {
		p.UpdateStateCode(commonpb.StateCode_Abnormal)
		resp, err := p.RunAnalyzer(context.Background(), &milvuspb.RunAnalyzerRequest{
			Placeholder: [][]byte{[]byte("test doc")},
		})
		require.NoError(t, err)
		require.Error(t, merr.Error(resp.GetStatus()))
	})

	p.UpdateStateCode(commonpb.StateCode_Healthy)
	t.Run("run analyzer with default params", func(t *testing.T) {
		resp, err := p.RunAnalyzer(context.Background(), &milvuspb.RunAnalyzerRequest{
			Placeholder: [][]byte{[]byte("test doc")},
		})
		require.NoError(t, err)
		require.NoError(t, merr.Error(resp.GetStatus()))
		assert.Equal(t, len(resp.GetResults()[0].GetTokens()), 2)
	})

	t.Run("run analyzer with invalid params", func(t *testing.T) {
		resp, err := p.RunAnalyzer(context.Background(), &milvuspb.RunAnalyzerRequest{
			Placeholder:    [][]byte{[]byte("test doc")},
			AnalyzerParams: "invalid json",
		})
		require.NoError(t, err)
		require.Error(t, merr.Error(resp.GetStatus()))
	})

	t.Run("run analyzer from loaded collection field", func(t *testing.T) {
		mockCache := NewMockCache(t)
		globalMetaCache = mockCache

		fieldMap := &typeutil.ConcurrentMap[string, int64]{}
		fieldMap.Insert("test_text", 100)
		mockCache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, "test_collection").Return(1, nil)
		mockCache.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything, "test_collection").Return(&schemaInfo{
			CollectionSchema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{{
					FieldID: 100,
					Name:    "test_text",
				}},
			},
			fieldMap: fieldMap,
		}, nil)

		lb := NewMockLBPolicy(t)
		lb.EXPECT().ExecuteOneChannel(mock.Anything, mock.Anything).Return(nil)
		p.lbPolicy = lb

		resp, err := p.RunAnalyzer(context.Background(), &milvuspb.RunAnalyzerRequest{
			Placeholder:    [][]byte{[]byte("test doc")},
			CollectionName: "test_collection",
			FieldName:      "test_text",
		})

		require.NoError(t, err)
		require.NoError(t, merr.Error(resp.GetStatus()))
	})
}

func Test_GetSegmentsInfo(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		mockMixCoord := mocks.NewMockMixCoordClient(t)
		mockMixCoord.EXPECT().GetSegmentInfo(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, request *datapb.GetSegmentInfoRequest, opts ...grpc.CallOption) (*datapb.GetSegmentInfoResponse, error) {
			segmentInfos := make([]*datapb.SegmentInfo, 0)
			for _, segID := range request.SegmentIDs {
				segmentInfos = append(segmentInfos, &datapb.SegmentInfo{
					ID:            segID,
					CollectionID:  1,
					PartitionID:   2,
					InsertChannel: "ch-1",
					NumOfRows:     1024,
					State:         commonpb.SegmentState_Flushed,
					MaxRowNum:     65535,
					Binlogs: []*datapb.FieldBinlog{
						{
							FieldID: 0,
							Binlogs: []*datapb.Binlog{
								{
									LogID: 1,
								},
								{
									LogID: 5,
								},
							},
						},
						{
							FieldID: 1,
							Binlogs: []*datapb.Binlog{
								{
									LogID: 2,
								},
								{
									LogID: 6,
								},
							},
						},
						{
							FieldID: 100,
							Binlogs: []*datapb.Binlog{
								{
									LogID: 3,
								},
								{
									LogID: 7,
								},
							},
						},
						{
							FieldID: 101,
							Binlogs: []*datapb.Binlog{
								{
									LogID: 4,
								},
								{
									LogID: 8,
								},
							},
						},
					},
					Statslogs: nil,
					Deltalogs: nil,
					Level:     datapb.SegmentLevel_L1,
					IsSorted:  true,
				})
			}

			return &datapb.GetSegmentInfoResponse{
				Status: merr.Success(),
				Infos:  segmentInfos,
			}, nil
		})

		ctx := context.Background()
		p := &Proxy{
			ctx:      ctx,
			mixCoord: mockMixCoord,
		}
		p.UpdateStateCode(commonpb.StateCode_Healthy)

		resp, err := p.GetSegmentsInfo(ctx, &internalpb.GetSegmentsInfoRequest{
			CollectionID: 1,
			SegmentIDs:   []int64{4, 5, 6},
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.GetErrorCode())
		assert.Equal(t, 3, len(resp.GetSegmentInfos()))
		assert.Equal(t, 4, len(resp.GetSegmentInfos()[0].GetInsertLogs()))
		assert.Equal(t, int64(1), resp.GetSegmentInfos()[0].GetCollectionID())
		assert.Equal(t, int64(2), resp.GetSegmentInfos()[0].GetPartitionID())
		assert.Equal(t, "ch-1", resp.GetSegmentInfos()[0].GetVChannel())
		assert.ElementsMatch(t, []int64{1, 5}, resp.GetSegmentInfos()[0].GetInsertLogs()[0].GetLogIDs())
		assert.ElementsMatch(t, []int64{2, 6}, resp.GetSegmentInfos()[0].GetInsertLogs()[1].GetLogIDs())
		assert.ElementsMatch(t, []int64{3, 7}, resp.GetSegmentInfos()[0].GetInsertLogs()[2].GetLogIDs())
		assert.ElementsMatch(t, []int64{4, 8}, resp.GetSegmentInfos()[0].GetInsertLogs()[3].GetLogIDs())
	})
}
