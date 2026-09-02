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

package grpcquerynode

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

type distributedQueryNodeTaskFunc func(context.Context) error

func (f distributedQueryNodeTaskFunc) Execute(ctx context.Context) error {
	return f(ctx)
}

func TestLazyQNSegmentManagerNilCallbacksUseNodeScheduler(t *testing.T) {
	scheduler := nodescheduler.New(1)
	t.Cleanup(scheduler.Close)

	started := make(chan struct{})
	releaseBlocker := make(chan struct{})
	blocker := scheduler.Submit(distributedQueryNodeTaskFunc(func(context.Context) error {
		close(started)
		<-releaseBlocker
		return nil
	}))
	<-started

	mgr := &lazyQNSegmentManager{scheduler: scheduler}
	unrecoverable := make(chan struct{}, 1)
	dropped := make(chan struct{}, 1)
	mgr.Acquire(qnview.AcquireSegments{OnUnrecoverable: func() { unrecoverable <- struct{}{} }})
	mgr.Release(qnview.ReleaseSegments{OnDropped: func() { dropped <- struct{}{} }})

	select {
	case <-unrecoverable:
		t.Fatal("acquire callback bypassed node scheduler")
	case <-dropped:
		t.Fatal("release callback bypassed node scheduler")
	case <-time.After(20 * time.Millisecond):
	}

	close(releaseBlocker)
	require.NoError(t, blocker.Wait(context.Background()))
	select {
	case <-unrecoverable:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for acquire callback")
	}
	select {
	case <-dropped:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for release callback")
	}
}

type MockRootCoord struct {
	types.RootCoord
	initErr  error
	startErr error
	regErr   error
	stopErr  error
	stateErr commonpb.ErrorCode
}

func (m *MockRootCoord) Init() error {
	return m.initErr
}

func (m *MockRootCoord) Start() error {
	return m.startErr
}

func (m *MockRootCoord) Stop() error {
	return m.stopErr
}

func (m *MockRootCoord) Register() error {
	return m.regErr
}

func (m *MockRootCoord) SetEtcdClient(client *clientv3.Client) {
}

func (m *MockRootCoord) GetComponentStates(ctx context.Context, req *milvuspb.GetComponentStatesRequest) (*milvuspb.ComponentStates, error) {
	return &milvuspb.ComponentStates{
		State:  &milvuspb.ComponentInfo{StateCode: commonpb.StateCode_Healthy},
		Status: &commonpb.Status{ErrorCode: m.stateErr},
	}, nil
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func TestMain(m *testing.M) {
	paramtable.Init()
	code := m.Run()
	os.Exit(code)
}

func Test_NewServer(t *testing.T) {
	ctx := context.Background()
	server, err := NewServer(ctx, nil)
	assert.NoError(t, err)
	assert.NotNil(t, server)

	mockQN := mocks.NewMockQueryNode(t)
	mockQN.EXPECT().Start().Return(nil).Maybe()
	mockQN.EXPECT().Stop().Return(nil).Maybe()
	mockQN.EXPECT().Register().Return(nil).Maybe()
	mockQN.EXPECT().SetEtcdClient(mock.Anything).Maybe()
	mockQN.EXPECT().SetAddress(mock.Anything).Maybe()
	mockQN.EXPECT().UpdateStateCode(mock.Anything).Maybe()
	mockQN.EXPECT().Init().Return(nil).Maybe()
	mockQN.EXPECT().GetNodeID().Return(2).Maybe()
	server.querynode = mockQN

	t.Run("Run", func(t *testing.T) {
		err = server.Prepare()
		assert.NoError(t, err)
		err = server.Run()
		assert.NoError(t, err)
	})

	t.Run("GetComponentStates", func(t *testing.T) {
		mockQN.EXPECT().GetComponentStates(mock.Anything, mock.Anything).Return(&milvuspb.ComponentStates{
			State: &milvuspb.ComponentInfo{
				StateCode: commonpb.StateCode_Healthy,
			},
		}, nil)
		req := &milvuspb.GetComponentStatesRequest{}
		states, err := server.GetComponentStates(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.StateCode_Healthy, states.State.StateCode)
	})

	t.Run("GetStatisticsChannel", func(t *testing.T) {
		mockQN.EXPECT().GetStatisticsChannel(mock.Anything, mock.Anything).Return(&milvuspb.StringResponse{Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}}, nil)
		req := &internalpb.GetStatisticsChannelRequest{}
		resp, err := server.GetStatisticsChannel(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("GetTimeTickChannel", func(t *testing.T) {
		mockQN.EXPECT().GetTimeTickChannel(mock.Anything, mock.Anything).Return(&milvuspb.StringResponse{Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}}, nil)
		req := &internalpb.GetTimeTickChannelRequest{}
		resp, err := server.GetTimeTickChannel(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("WatchDmChannels", func(t *testing.T) {
		mockQN.EXPECT().WatchDmChannels(mock.Anything, mock.Anything).Return(&commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil)
		req := &querypb.WatchDmChannelsRequest{}
		resp, err := server.WatchDmChannels(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("LoadSegments", func(t *testing.T) {
		mockQN.EXPECT().LoadSegments(mock.Anything, mock.Anything).Return(&commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil)
		req := &querypb.LoadSegmentsRequest{}
		resp, err := server.LoadSegments(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("ReleaseCollection", func(t *testing.T) {
		mockQN.EXPECT().ReleaseCollection(mock.Anything, mock.Anything).Return(&commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil)
		req := &querypb.ReleaseCollectionRequest{}
		resp, err := server.ReleaseCollection(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("LoadPartitions", func(t *testing.T) {
		mockQN.EXPECT().LoadPartitions(mock.Anything, mock.Anything).Return(&commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil)
		req := &querypb.LoadPartitionsRequest{}
		resp, err := server.LoadPartitions(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("ReleasePartitions", func(t *testing.T) {
		mockQN.EXPECT().ReleasePartitions(mock.Anything, mock.Anything).Return(&commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil)
		req := &querypb.ReleasePartitionsRequest{}
		resp, err := server.ReleasePartitions(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("ReleaseSegments", func(t *testing.T) {
		mockQN.EXPECT().ReleaseSegments(mock.Anything, mock.Anything).Return(&commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil)
		req := &querypb.ReleaseSegmentsRequest{}
		resp, err := server.ReleaseSegments(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("GetSegmentInfo", func(t *testing.T) {
		mockQN.EXPECT().GetSegmentInfo(mock.Anything, mock.Anything).Return(&querypb.GetSegmentInfoResponse{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		}, nil)
		req := &querypb.GetSegmentInfoRequest{}
		resp, err := server.GetSegmentInfo(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("GetMetrics", func(t *testing.T) {
		mockQN.EXPECT().GetMetrics(mock.Anything, mock.Anything).Return(
			&milvuspb.GetMetricsResponse{Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}}, nil)
		req := &milvuspb.GetMetricsRequest{
			Request: "",
		}
		resp, err := server.GetMetrics(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("Search", func(t *testing.T) {
		mockQN.EXPECT().Search(mock.Anything, mock.Anything).Return(&internalpb.SearchResults{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		}, nil)
		req := &querypb.SearchRequest{}
		resp, err := server.Search(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("SearchSegments", func(t *testing.T) {
		mockQN.EXPECT().SearchSegments(mock.Anything, mock.Anything).Return(&internalpb.SearchResults{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		}, nil)
		req := &querypb.SearchRequest{}
		resp, err := server.SearchSegments(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("Query", func(t *testing.T) {
		mockQN.EXPECT().Query(mock.Anything, mock.Anything).Return(&internalpb.RetrieveResults{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		}, nil)
		req := &querypb.QueryRequest{}
		resp, err := server.Query(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("QueryStream", func(t *testing.T) {
		mockQN.EXPECT().QueryStream(mock.Anything, mock.Anything).Return(nil)
		ret := server.QueryStream(nil, nil)
		assert.Nil(t, ret)
	})

	t.Run("QuerySegments", func(t *testing.T) {
		mockQN.EXPECT().QuerySegments(mock.Anything, mock.Anything).Return(&internalpb.RetrieveResults{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		}, nil)
		req := &querypb.QueryRequest{}
		resp, err := server.QuerySegments(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("QueryStreamSegments", func(t *testing.T) {
		mockQN.EXPECT().QueryStreamSegments(mock.Anything, mock.Anything).Return(nil)
		ret := server.QueryStreamSegments(nil, nil)
		assert.Nil(t, ret)
	})

	t.Run("SyncReplicaSegments", func(t *testing.T) {
		mockQN.EXPECT().SyncReplicaSegments(mock.Anything, mock.Anything).Return(&commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil)
		req := &querypb.SyncReplicaSegmentsRequest{}
		resp, err := server.SyncReplicaSegments(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("ShowConfigurtaions", func(t *testing.T) {
		mockQN.EXPECT().ShowConfigurations(mock.Anything, mock.Anything).Return(&internalpb.ShowConfigurationsResponse{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		}, nil)
		req := &internalpb.ShowConfigurationsRequest{
			Pattern: "Cache",
		}
		resp, err := server.ShowConfigurations(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("DeleteBatch", func(t *testing.T) {
		mockQN.EXPECT().DeleteBatch(mock.Anything, mock.Anything).Return(&querypb.DeleteBatchResponse{
			Status: merr.Success(),
		}, nil)

		resp, err := server.DeleteBatch(ctx, &querypb.DeleteBatchRequest{})
		assert.NoError(t, merr.CheckRPCCall(resp, err))
	})

	t.Run("RunAnalyzer", func(t *testing.T) {
		mockQN.EXPECT().RunAnalyzer(mock.Anything, mock.Anything).Return(&milvuspb.RunAnalyzerResponse{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		}, nil)
		req := &querypb.RunAnalyzerRequest{}
		resp, err := server.RunAnalyzer(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("GetHighlight", func(t *testing.T) {
		mockQN.EXPECT().GetHighlight(mock.Anything, mock.Anything).Return(&querypb.GetHighlightResponse{
			Status: merr.Success(),
		}, nil)

		resp, err := server.GetHighlight(ctx, &querypb.GetHighlightRequest{
			Channel: "test-channel",
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("ValidateAnalyzer", func(t *testing.T) {
		mockQN.EXPECT().ValidateAnalyzer(mock.Anything, mock.Anything).Return(&querypb.ValidateAnalyzerResponse{Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}}, nil)
		req := &querypb.ValidateAnalyzerRequest{}
		resp, err := server.ValidateAnalyzer(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	err = server.Stop()
	assert.NoError(t, err)
}

func TestRegisterQueryViewSyncServer(t *testing.T) {
	server := grpc.NewServer()
	registerQueryViewSyncServer(server, noopQNSegmentManager{})

	_, ok := server.GetServiceInfo()["milvus.proto.view.ViewSyncService"]
	assert.True(t, ok)
}

func TestRegisterQueryViewServers(t *testing.T) {
	server := grpc.NewServer()
	registerQueryViewServers(server, noopQNSegmentManager{}, 100)

	services := server.GetServiceInfo()
	_, ok := services["milvus.proto.view.ViewSyncService"]
	assert.True(t, ok)
	_, ok = services["milvus.proto.view.ViewQueryService"]
	assert.True(t, ok)
}

type noopQNSegmentManager struct{}

func (noopQNSegmentManager) Acquire(qnview.AcquireSegments) {}

func (noopQNSegmentManager) Release(qnview.ReleaseSegments) {}

func (noopQNSegmentManager) AcquireSealedSegmentHandles(context.Context, qviews.QueryViewKey, *viewpb.QueryViewOfQueryNode) ([]qnview.SealedSegmentHandle, error) {
	return nil, nil
}

func (noopQNSegmentManager) WaitTransformVisible(context.Context, qviews.QueryViewKey, uint64) error {
	return nil
}

func TestQueryViewTransformLogStreamManagerNilWhenWALNotReady(t *testing.T) {
	streaming.SetWALForTest(nil)
	defer streaming.SetupNoopWALForTest()

	assert.Nil(t, queryViewTransformLogStreamManager())
}

type fakeQueryViewMetadataMixCoordClient struct {
	types.MixCoordClient

	describeReqs  []*milvuspb.DescribeCollectionRequest
	describeResps []*milvuspb.DescribeCollectionResponse
	describeErrs  []error

	getQVCollectionLoadInfoReqs  []*querypb.GetQueryViewLoadInfoRequest
	getQVCollectionLoadInfoResps []*querypb.GetQueryViewLoadInfoResponse
	getQVCollectionLoadInfoErrs  []error
}

func (c *fakeQueryViewMetadataMixCoordClient) DescribeCollection(_ context.Context, req *milvuspb.DescribeCollectionRequest, _ ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
	c.describeReqs = append(c.describeReqs, req)
	idx := len(c.describeReqs) - 1
	if idx < len(c.describeErrs) && c.describeErrs[idx] != nil {
		return nil, c.describeErrs[idx]
	}
	if idx < len(c.describeResps) {
		return c.describeResps[idx], nil
	}
	return c.describeResps[len(c.describeResps)-1], nil
}

func (c *fakeQueryViewMetadataMixCoordClient) GetQueryViewLoadInfo(_ context.Context, req *querypb.GetQueryViewLoadInfoRequest, _ ...grpc.CallOption) (*querypb.GetQueryViewLoadInfoResponse, error) {
	c.getQVCollectionLoadInfoReqs = append(c.getQVCollectionLoadInfoReqs, req)
	idx := len(c.getQVCollectionLoadInfoReqs) - 1
	if idx < len(c.getQVCollectionLoadInfoErrs) && c.getQVCollectionLoadInfoErrs[idx] != nil {
		return nil, c.getQVCollectionLoadInfoErrs[idx]
	}
	if idx < len(c.getQVCollectionLoadInfoResps) {
		return c.getQVCollectionLoadInfoResps[idx], nil
	}
	return c.getQVCollectionLoadInfoResps[len(c.getQVCollectionLoadInfoResps)-1], nil
}

func newTestQueryViewLoadMetadataProvider(client types.MixCoordClient) *lazyQueryViewLoadMetadataProvider {
	future := syncutil.NewFuture[types.MixCoordClient]()
	future.Set(client)
	return &lazyQueryViewLoadMetadataProvider{mixCoord: future}
}

func TestLazyQueryViewLoadMetadataProvider_GetQueryViewLoadInfo(t *testing.T) {
	indexes := []*indexpb.IndexInfo{{CollectionID: 100, FieldID: 101, IndexName: "vec_idx"}}
	client := &fakeQueryViewMetadataMixCoordClient{
		getQVCollectionLoadInfoResps: []*querypb.GetQueryViewLoadInfoResponse{{
			Status:        &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			CollectionID:  100,
			Version:       7,
			PartitionIDs:  []int64{10, 20},
			LoadFields:    []*messagespb.LoadFieldConfig{{FieldId: 100}, {FieldId: 101, IndexId: 1001}},
			IndexInfoList: indexes,
		}},
	}
	provider := newTestQueryViewLoadMetadataProvider(client)

	info, err := provider.GetQueryViewLoadInfo(context.Background(), 100, qnview.QueryViewLoadInfoVersion(7))

	require.NoError(t, err)
	assert.Equal(t, int64(100), info.CollectionID)
	assert.Equal(t, qnview.QueryViewLoadInfoVersion(7), info.Version)
	assert.Equal(t, []int64{10, 20}, info.PartitionIDs)
	assert.Equal(t, []*messagespb.LoadFieldConfig{{FieldId: 100}, {FieldId: 101, IndexId: 1001}}, info.LoadFields)
	assert.Equal(t, indexes, info.IndexInfos)
	require.Len(t, client.getQVCollectionLoadInfoReqs, 1)
	assert.Equal(t, int64(100), client.getQVCollectionLoadInfoReqs[0].GetCollectionID())
	assert.Equal(t, uint64(7), client.getQVCollectionLoadInfoReqs[0].GetVersion())
}

func TestLazyQueryViewLoadMetadataProvider_DescribeCollectionAttemptsRecoverableErrorOnce(t *testing.T) {
	client := &fakeQueryViewMetadataMixCoordClient{
		describeErrs: []error{merr.WrapErrNodeNotMatch(1, 2)},
		describeResps: []*milvuspb.DescribeCollectionResponse{{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			Schema: &schemapb.CollectionSchema{
				Name: "qv",
			},
		}},
	}
	provider := newTestQueryViewLoadMetadataProvider(client)

	resp, err := provider.DescribeCollection(context.Background(), 100)

	require.ErrorIs(t, err, merr.ErrNodeNotMatch)
	assert.Nil(t, resp)
	require.Len(t, client.describeReqs, 1)
	assert.Equal(t, int64(100), client.describeReqs[0].GetCollectionID())
}

func TestLazyQueryViewLoadMetadataProvider_DescribeCollectionDoesNotRetryPermanentError(t *testing.T) {
	client := &fakeQueryViewMetadataMixCoordClient{
		describeErrs: []error{merr.WrapErrCollectionNotFound(100)},
	}
	provider := newTestQueryViewLoadMetadataProvider(client)

	_, err := provider.DescribeCollection(context.Background(), 100)

	require.ErrorIs(t, err, merr.ErrCollectionNotFound)
	assert.NotContains(t, err.Error(), "unrecoverable error")
	require.Len(t, client.describeReqs, 1)
	assert.Equal(t, int64(100), client.describeReqs[0].GetCollectionID())
}

func Test_Run(t *testing.T) {
	ctx := context.Background()
	server, err := NewServer(ctx, nil)
	assert.NoError(t, err)
	assert.NotNil(t, server)

	mockQN := mocks.NewMockQueryNode(t)
	mockQN.EXPECT().Start().Return(errors.New("Failed")).Maybe()
	mockQN.EXPECT().Stop().Return(errors.New("Failed")).Maybe()
	mockQN.EXPECT().Register().Return(errors.New("Failed")).Maybe()
	mockQN.EXPECT().SetEtcdClient(mock.Anything).Maybe()
	mockQN.EXPECT().SetAddress(mock.Anything).Maybe()
	mockQN.EXPECT().UpdateStateCode(mock.Anything).Maybe()
	mockQN.EXPECT().Init().Return(nil).Maybe()
	mockQN.EXPECT().GetNodeID().Return(2).Maybe()
	server.querynode = mockQN
	err = server.Prepare()
	assert.NoError(t, err)
	err = server.Run()
	assert.Error(t, err)

	err = server.Run()
	assert.Error(t, err)

	err = server.Stop()
	assert.Error(t, err)
}
