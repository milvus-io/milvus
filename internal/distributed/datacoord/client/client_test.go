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

package grpcdatacoordclient

import (
	"context"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func TestMain(m *testing.M) {
	// init embed etcd
	embedetcdServer, tempDir, err := etcd.StartTestEmbedEtcdServer()
	if err != nil {
		log.Fatal("failed to start embed etcd server", zap.Error(err))
	}
	defer os.RemoveAll(tempDir)
	defer embedetcdServer.Close()

	addrs := etcd.GetEmbedEtcdEndpoints(embedetcdServer)

	paramtable.Init()
	paramtable.Get().Save(Params.EtcdCfg.Endpoints.Key, strings.Join(addrs, ","))

	rand.Seed(time.Now().UnixNano())
	os.Exit(m.Run())
}

func Test_NewClient(t *testing.T) {
	ctx := context.Background()

	client, err := NewClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)

	err = client.Close()
	assert.NoError(t, err)
}

func Test_GetComponentStates(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	client, err := NewClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	mockProxy := mocks.NewMockDataCoordClient(t)
	mockGrpcClient := mocks.NewMockGrpcClient[datapb.DataCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().ReCall(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, f func(datapb.DataCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockProxy)
	})
	client.grpcClient = mockGrpcClient

	// test success
	mockProxy.EXPECT().GetComponentStates(mock.Anything, mock.Anything).Return(&milvuspb.ComponentStates{
		Status: merr.Success(),
	}, nil)
	_, err = client.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
	assert.Nil(t, err)

	// test return error code
	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().GetComponentStates(mock.Anything, mock.Anything).Return(&milvuspb.ComponentStates{
		Status: merr.Status(merr.ErrServiceNotReady),
	}, nil)

	_, err = client.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
	assert.Nil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_GetTimeTickChannel(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	client, err := NewClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	mockProxy := mocks.NewMockDataCoordClient(t)
	mockGrpcClient := mocks.NewMockGrpcClient[datapb.DataCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().ReCall(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, f func(datapb.DataCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockProxy)
	})
	client.grpcClient = mockGrpcClient

	// test success
	mockProxy.EXPECT().GetTimeTickChannel(mock.Anything, mock.Anything).Return(&milvuspb.StringResponse{
		Status: merr.Success(),
	}, nil)
	_, err = client.GetTimeTickChannel(ctx, &internalpb.GetTimeTickChannelRequest{})
	assert.Nil(t, err)

	// test return error code
	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().GetTimeTickChannel(mock.Anything, mock.Anything).Return(&milvuspb.StringResponse{
		Status: merr.Status(merr.ErrServiceNotReady),
	}, nil)

	_, err = client.GetTimeTickChannel(ctx, &internalpb.GetTimeTickChannelRequest{})
	assert.Nil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.GetTimeTickChannel(ctx, &internalpb.GetTimeTickChannelRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_GetStatisticsChannel(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	client, err := NewClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	mockProxy := mocks.NewMockDataCoordClient(t)
	mockGrpcClient := mocks.NewMockGrpcClient[datapb.DataCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().ReCall(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, f func(datapb.DataCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockProxy)
	})
	client.grpcClient = mockGrpcClient

	// test success
	mockProxy.EXPECT().GetStatisticsChannel(mock.Anything, mock.Anything).Return(&milvuspb.StringResponse{
		Status: merr.Success(),
	}, nil)
	_, err = client.GetStatisticsChannel(ctx, &internalpb.GetStatisticsChannelRequest{})
	assert.Nil(t, err)

	// test return error code
	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().GetStatisticsChannel(mock.Anything, mock.Anything).Return(&milvuspb.StringResponse{
		Status: merr.Status(merr.ErrServiceNotReady),
	}, nil)

	_, err = client.GetStatisticsChannel(ctx, &internalpb.GetStatisticsChannelRequest{})
	assert.Nil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.GetStatisticsChannel(ctx, &internalpb.GetStatisticsChannelRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_Flush(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	client, err := NewClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	mockProxy := mocks.NewMockDataCoordClient(t)
	mockGrpcClient := mocks.NewMockGrpcClient[datapb.DataCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().GetNodeID().Return(1)
	mockGrpcClient.EXPECT().ReCall(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, f func(datapb.DataCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockProxy)
	})
	client.grpcClient = mockGrpcClient

	// test success
	mockProxy.EXPECT().Flush(mock.Anything, mock.Anything).Return(&datapb.FlushResponse{
		Status: merr.Success(),
	}, nil)
	_, err = client.Flush(ctx, &datapb.FlushRequest{})
	assert.Nil(t, err)

	// test return error code
	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().Flush(mock.Anything, mock.Anything).Return(&datapb.FlushResponse{
		Status: merr.Status(merr.ErrServiceNotReady),
	}, nil)

	_, err = client.Flush(ctx, &datapb.FlushRequest{})
	assert.Nil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.Flush(ctx, &datapb.FlushRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_AssignSegmentID(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	client, err := NewClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	mockProxy := mocks.NewMockDataCoordClient(t)
	mockGrpcClient := mocks.NewMockGrpcClient[datapb.DataCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().ReCall(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, f func(datapb.DataCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockProxy)
	})
	client.grpcClient = mockGrpcClient

	// test success
	mockProxy.EXPECT().AssignSegmentID(mock.Anything, mock.Anything).Return(&datapb.AssignSegmentIDResponse{
		Status: merr.Success(),
	}, nil)
	_, err = client.AssignSegmentID(ctx, &datapb.AssignSegmentIDRequest{})
	assert.Nil(t, err)

	// test return error code
	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().AssignSegmentID(mock.Anything, mock.Anything).Return(&datapb.AssignSegmentIDResponse{
		Status: merr.Status(merr.ErrServiceNotReady),
	}, nil)

	_, err = client.AssignSegmentID(ctx, &datapb.AssignSegmentIDRequest{})
	assert.Nil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.AssignSegmentID(ctx, &datapb.AssignSegmentIDRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_GetSegmentStates(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	client, err := NewClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	mockProxy := mocks.NewMockDataCoordClient(t)
	mockGrpcClient := mocks.NewMockGrpcClient[datapb.DataCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().GetNodeID().Return(1)
	mockGrpcClient.EXPECT().ReCall(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, f func(datapb.DataCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockProxy)
	})
	client.grpcClient = mockGrpcClient

	// test success
	mockProxy.EXPECT().GetSegmentStates(mock.Anything, mock.Anything).Return(&datapb.GetSegmentStatesResponse{
		Status: merr.Success(),
	}, nil)
	_, err = client.GetSegmentStates(ctx, &datapb.GetSegmentStatesRequest{})
	assert.Nil(t, err)

	// test return error code
	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().GetSegmentStates(mock.Anything, mock.Anything).Return(&datapb.GetSegmentStatesResponse{
		Status: merr.Status(merr.ErrServiceNotReady),
	}, nil)

	_, err = client.GetSegmentStates(ctx, &datapb.GetSegmentStatesRequest{})
	assert.Nil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.GetSegmentStates(ctx, &datapb.GetSegmentStatesRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_GetInsertBinlogPaths(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	client, err := NewClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	mockProxy := mocks.NewMockDataCoordClient(t)
	mockGrpcClient := mocks.NewMockGrpcClient[datapb.DataCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().GetNodeID().Return(1)
	mockGrpcClient.EXPECT().ReCall(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, f func(datapb.DataCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockProxy)
	})
	client.grpcClient = mockGrpcClient

	// test success
	mockProxy.EXPECT().GetInsertBinlogPaths(mock.Anything, mock.Anything).Return(&datapb.GetInsertBinlogPathsResponse{
		Status: merr.Success(),
	}, nil)
	_, err = client.GetInsertBinlogPaths(ctx, &datapb.GetInsertBinlogPathsRequest{})
	assert.Nil(t, err)

	// test return error code
	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().GetInsertBinlogPaths(mock.Anything, mock.Anything).Return(&datapb.GetInsertBinlogPathsResponse{
		Status: merr.Status(merr.ErrServiceNotReady),
	}, nil)

	_, err = client.GetInsertBinlogPaths(ctx, &datapb.GetInsertBinlogPathsRequest{})
	assert.Nil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.GetInsertBinlogPaths(ctx, &datapb.GetInsertBinlogPathsRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_GetCollectionStatistics(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	client, err := NewClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	mockProxy := mocks.NewMockDataCoordClient(t)
	mockGrpcClient := mocks.NewMockGrpcClient[datapb.DataCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().GetNodeID().Return(1)
	mockGrpcClient.EXPECT().ReCall(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, f func(datapb.DataCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockProxy)
	})
	client.grpcClient = mockGrpcClient

	// test success
	mockProxy.EXPECT().GetCollectionStatistics(mock.Anything, mock.Anything).Return(&datapb.GetCollectionStatisticsResponse{
		Status: merr.Success(),
	}, nil)
	_, err = client.GetCollectionStatistics(ctx, &datapb.GetCollectionStatisticsRequest{})
	assert.Nil(t, err)

	// test return error code
	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().GetCollectionStatistics(mock.Anything, mock.Anything).Return(&datapb.GetCollectionStatisticsResponse{
		Status: merr.Status(merr.ErrServiceNotReady),
	}, nil)

	_, err = client.GetCollectionStatistics(ctx, &datapb.GetCollectionStatisticsRequest{})
	assert.Nil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.GetCollectionStatistics(ctx, &datapb.GetCollectionStatisticsRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_GetPartitionStatistics(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	client, err := NewClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	mockProxy := mocks.NewMockDataCoordClient(t)
	mockGrpcClient := mocks.NewMockGrpcClient[datapb.DataCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().GetNodeID().Return(1)
	mockGrpcClient.EXPECT().ReCall(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, f func(datapb.DataCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockProxy)
	})
	client.grpcClient = mockGrpcClient

	// test success
	mockProxy.EXPECT().GetPartitionStatistics(mock.Anything, mock.Anything).Return(&datapb.GetPartitionStatisticsResponse{
		Status: merr.Success(),
	}, nil)
	_, err = client.GetPartitionStatistics(ctx, &datapb.GetPartitionStatisticsRequest{})
	assert.Nil(t, err)

	// test return error code
	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().GetPartitionStatistics(mock.Anything, mock.Anything).Return(&datapb.GetPartitionStatisticsResponse{
		Status: merr.Status(merr.ErrServiceNotReady),
	}, nil)

	_, err = client.GetPartitionStatistics(ctx, &datapb.GetPartitionStatisticsRequest{})
	assert.Nil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.GetPartitionStatistics(ctx, &datapb.GetPartitionStatisticsRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_GetSegmentInfoChannel(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	client, err := NewClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	mockProxy := mocks.NewMockDataCoordClient(t)
	mockGrpcClient := mocks.NewMockGrpcClient[datapb.DataCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().ReCall(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, f func(datapb.DataCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockProxy)
	})
	client.grpcClient = mockGrpcClient

	// test success
	mockProxy.EXPECT().GetSegmentInfoChannel(mock.Anything, mock.Anything).Return(&milvuspb.StringResponse{
		Status: merr.Success(),
	}, nil)
	_, err = client.GetSegmentInfoChannel(ctx, &datapb.GetSegmentInfoChannelRequest{})
	assert.Nil(t, err)

	// test return error code
	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().GetSegmentInfoChannel(mock.Anything, mock.Anything).Return(&milvuspb.StringResponse{
		Status: merr.Status(merr.ErrServiceNotReady),
	}, nil)

	_, err = client.GetSegmentInfoChannel(ctx, &datapb.GetSegmentInfoChannelRequest{})
	assert.Nil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.GetSegmentInfoChannel(ctx, &datapb.GetSegmentInfoChannelRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_GetSegmentInfo(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	client, err := NewClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	mockProxy := mocks.NewMockDataCoordClient(t)
	mockGrpcClient := mocks.NewMockGrpcClient[datapb.DataCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().GetNodeID().Return(1)
	mockGrpcClient.EXPECT().ReCall(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, f func(datapb.DataCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockProxy)
	})
	client.grpcClient = mockGrpcClient

	// test success
	mockProxy.EXPECT().GetSegmentInfo(mock.Anything, mock.Anything).Return(&datapb.GetSegmentInfoResponse{
		Status: merr.Success(),
	}, nil)
	_, err = client.GetSegmentInfo(ctx, &datapb.GetSegmentInfoRequest{})
	assert.Nil(t, err)

	// test return error code
	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().GetSegmentInfo(mock.Anything, mock.Anything).Return(&datapb.GetSegmentInfoResponse{
		Status: merr.Status(merr.ErrServiceNotReady),
	}, nil)

	_, err = client.GetSegmentInfo(ctx, &datapb.GetSegmentInfoRequest{})
	assert.Nil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.GetSegmentInfo(ctx, &datapb.GetSegmentInfoRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_SaveBinlogPaths(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	client, err := NewClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	mockProxy := mocks.NewMockDataCoordClient(t)
	mockGrpcClient := mocks.NewMockGrpcClient[datapb.DataCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().GetNodeID().Return(1)
	mockGrpcClient.EXPECT().ReCall(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, f func(datapb.DataCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockProxy)
	})
	client.grpcClient = mockGrpcClient

	// test success
	mockProxy.EXPECT().GetSegmentInfo(mock.Anything, mock.Anything).Return(&datapb.GetSegmentInfoResponse{
		Status: merr.Success(),
	}, nil)
	_, err = client.GetSegmentInfo(ctx, &datapb.GetSegmentInfoRequest{})
	assert.Nil(t, err)

	// test return error code
	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().GetSegmentInfo(mock.Anything, mock.Anything).Return(&datapb.GetSegmentInfoResponse{
		Status: merr.Status(merr.ErrServiceNotReady),
	}, nil)

	_, err = client.GetSegmentInfo(ctx, &datapb.GetSegmentInfoRequest{})
	assert.Nil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.GetSegmentInfo(ctx, &datapb.GetSegmentInfoRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_GetRecoveryInfo(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	client, err := NewClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	mockProxy := mocks.NewMockDataCoordClient(t)
	mockGrpcClient := mocks.NewMockGrpcClient[datapb.DataCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().GetNodeID().Return(1)
	mockGrpcClient.EXPECT().ReCall(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, f func(datapb.DataCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockProxy)
	})
	client.grpcClient = mockGrpcClient

	// test success
	mockProxy.EXPECT().GetRecoveryInfo(mock.Anything, mock.Anything).Return(&datapb.GetRecoveryInfoResponse{
		Status: merr.Success(),
	}, nil)
	_, err = client.GetRecoveryInfo(ctx, &datapb.GetRecoveryInfoRequest{})
	assert.Nil(t, err)

	// test return error code
	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().GetRecoveryInfo(mock.Anything, mock.Anything).Return(&datapb.GetRecoveryInfoResponse{
		Status: merr.Status(merr.ErrServiceNotReady),
	}, nil)

	_, err = client.GetRecoveryInfo(ctx, &datapb.GetRecoveryInfoRequest{})
	assert.Nil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.GetRecoveryInfo(ctx, &datapb.GetRecoveryInfoRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_GetRecoveryInfoV2(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	client, err := NewClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	mockProxy := mocks.NewMockDataCoordClient(t)
	mockGrpcClient := mocks.NewMockGrpcClient[datapb.DataCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().ReCall(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, f func(datapb.DataCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockProxy)
	})
	client.grpcClient = mockGrpcClient

	// test success
	mockProxy.EXPECT().GetRecoveryInfoV2(mock.Anything, mock.Anything).Return(&datapb.GetRecoveryInfoResponseV2{
		Status: merr.Success(),
	}, nil)
	_, err = client.GetRecoveryInfoV2(ctx, &datapb.GetRecoveryInfoRequestV2{})
	assert.Nil(t, err)

	// test return error code
	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().GetRecoveryInfoV2(mock.Anything, mock.Anything).Return(&datapb.GetRecoveryInfoResponseV2{
		Status: merr.Status(merr.ErrServiceNotReady),
	}, nil)

	_, err = client.GetRecoveryInfoV2(ctx, &datapb.GetRecoveryInfoRequestV2{})
	assert.Nil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.GetRecoveryInfoV2(ctx, &datapb.GetRecoveryInfoRequestV2{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_GetFlushedSegments(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	client, err := NewClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	mockProxy := mocks.NewMockDataCoordClient(t)
	mockGrpcClient := mocks.NewMockGrpcClient[datapb.DataCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().GetNodeID().Return(1)
	mockGrpcClient.EXPECT().ReCall(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, f func(datapb.DataCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockProxy)
	})
	client.grpcClient = mockGrpcClient

	// test success
	mockProxy.EXPECT().GetFlushedSegments(mock.Anything, mock.Anything).Return(&datapb.GetFlushedSegmentsResponse{
		Status: merr.Success(),
	}, nil)
	_, err = client.GetFlushedSegments(ctx, &datapb.GetFlushedSegmentsRequest{})
	assert.Nil(t, err)

	// test return error code
	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().GetFlushedSegments(mock.Anything, mock.Anything).Return(&datapb.GetFlushedSegmentsResponse{
		Status: merr.Status(merr.ErrServiceNotReady),
	}, nil)

	_, err = client.GetFlushedSegments(ctx, &datapb.GetFlushedSegmentsRequest{})
	assert.Nil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.GetFlushedSegments(ctx, &datapb.GetFlushedSegmentsRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_GetSegmentsByStates(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	client, err := NewClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	mockProxy := mocks.NewMockDataCoordClient(t)
	mockGrpcClient := mocks.NewMockGrpcClient[datapb.DataCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().GetNodeID().Return(1)
	mockGrpcClient.EXPECT().ReCall(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, f func(datapb.DataCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockProxy)
	})
	client.grpcClient = mockGrpcClient

	// test success
	mockProxy.EXPECT().GetSegmentsByStates(mock.Anything, mock.Anything).Return(&datapb.GetSegmentsByStatesResponse{
		Status: merr.Success(),
	}, nil)
	_, err = client.GetSegmentsByStates(ctx, &datapb.GetSegmentsByStatesRequest{})
	assert.Nil(t, err)

	// test return error code
	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().GetSegmentsByStates(mock.Anything, mock.Anything).Return(&datapb.GetSegmentsByStatesResponse{
		Status: merr.Status(merr.ErrServiceNotReady),
	}, nil)

	_, err = client.GetSegmentsByStates(ctx, &datapb.GetSegmentsByStatesRequest{})
	assert.Nil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.GetSegmentsByStates(ctx, &datapb.GetSegmentsByStatesRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_ShowConfigurations(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	client, err := NewClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	mockProxy := mocks.NewMockDataCoordClient(t)
	mockGrpcClient := mocks.NewMockGrpcClient[datapb.DataCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().GetNodeID().Return(1)
	mockGrpcClient.EXPECT().ReCall(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, f func(datapb.DataCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockProxy)
	})
	client.grpcClient = mockGrpcClient

	// test success
	mockProxy.EXPECT().ShowConfigurations(mock.Anything, mock.Anything).Return(&internalpb.ShowConfigurationsResponse{
		Status: merr.Success(),
	}, nil)
	_, err = client.ShowConfigurations(ctx, &internalpb.ShowConfigurationsRequest{})
	assert.Nil(t, err)

	// test return error code
	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().ShowConfigurations(mock.Anything, mock.Anything).Return(&internalpb.ShowConfigurationsResponse{
		Status: merr.Status(merr.ErrServiceNotReady),
	}, nil)

	_, err = client.ShowConfigurations(ctx, &internalpb.ShowConfigurationsRequest{})
	assert.Nil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.ShowConfigurations(ctx, &internalpb.ShowConfigurationsRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_GetMetrics(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	client, err := NewClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	mockProxy := mocks.NewMockDataCoordClient(t)
	mockGrpcClient := mocks.NewMockGrpcClient[datapb.DataCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().GetNodeID().Return(1)
	mockGrpcClient.EXPECT().ReCall(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, f func(datapb.DataCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockProxy)
	})
	client.grpcClient = mockGrpcClient

	// test success
	mockProxy.EXPECT().GetMetrics(mock.Anything, mock.Anything).Return(&milvuspb.GetMetricsResponse{
		Status: merr.Success(),
	}, nil)
	_, err = client.GetMetrics(ctx, &milvuspb.GetMetricsRequest{})
	assert.Nil(t, err)

	// test return error code
	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().GetMetrics(mock.Anything, mock.Anything).Return(&milvuspb.GetMetricsResponse{
		Status: merr.Status(merr.ErrServiceNotReady),
	}, nil)

	_, err = client.GetMetrics(ctx, &milvuspb.GetMetricsRequest{})
	assert.Nil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.GetMetrics(ctx, &milvuspb.GetMetricsRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_ManualCompaction(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	client, err := NewClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	mockProxy := mocks.NewMockDataCoordClient(t)
	mockGrpcClient := mocks.NewMockGrpcClient[datapb.DataCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().ReCall(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, f func(datapb.DataCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockProxy)
	})
	client.grpcClient = mockGrpcClient

	// test success
	mockProxy.EXPECT().ManualCompaction(mock.Anything, mock.Anything).Return(&milvuspb.ManualCompactionResponse{
		Status: merr.Success(),
	}, nil)
	_, err = client.ManualCompaction(ctx, &milvuspb.ManualCompactionRequest{})
	assert.Nil(t, err)

	// test return error code
	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().ManualCompaction(mock.Anything, mock.Anything).Return(&milvuspb.ManualCompactionResponse{
		Status: merr.Status(merr.ErrServiceNotReady),
	}, nil)

	_, err = client.ManualCompaction(ctx, &milvuspb.ManualCompactionRequest{})
	assert.Nil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.ManualCompaction(ctx, &milvuspb.ManualCompactionRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_GetCompactionState(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	client, err := NewClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	mockProxy := mocks.NewMockDataCoordClient(t)
	mockGrpcClient := mocks.NewMockGrpcClient[datapb.DataCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().ReCall(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, f func(datapb.DataCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockProxy)
	})
	client.grpcClient = mockGrpcClient

	// test success
	mockProxy.EXPECT().GetCompactionState(mock.Anything, mock.Anything).Return(&milvuspb.GetCompactionStateResponse{
		Status: merr.Success(),
	}, nil)
	_, err = client.GetCompactionState(ctx, &milvuspb.GetCompactionStateRequest{})
	assert.Nil(t, err)

	// test return error code
	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().GetCompactionState(mock.Anything, mock.Anything).Return(&milvuspb.GetCompactionStateResponse{
		Status: merr.Status(merr.ErrServiceNotReady),
	}, nil)

	_, err = client.GetCompactionState(ctx, &milvuspb.GetCompactionStateRequest{})
	assert.Nil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.GetCompactionState(ctx, &milvuspb.GetCompactionStateRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_GetCompactionStateWithPlans(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	client, err := NewClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	mockProxy := mocks.NewMockDataCoordClient(t)
	mockGrpcClient := mocks.NewMockGrpcClient[datapb.DataCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().ReCall(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, f func(datapb.DataCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockProxy)
	})
	client.grpcClient = mockGrpcClient

	// test success
	mockProxy.EXPECT().GetCompactionStateWithPlans(mock.Anything, mock.Anything).Return(&milvuspb.GetCompactionPlansResponse{
		Status: merr.Success(),
	}, nil)
	_, err = client.GetCompactionStateWithPlans(ctx, &milvuspb.GetCompactionPlansRequest{})
	assert.Nil(t, err)

	// test return error code
	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().GetCompactionStateWithPlans(mock.Anything, mock.Anything).Return(&milvuspb.GetCompactionPlansResponse{
		Status: merr.Status(merr.ErrServiceNotReady),
	}, nil)

	_, err = client.GetCompactionStateWithPlans(ctx, &milvuspb.GetCompactionPlansRequest{})
	assert.Nil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.GetCompactionStateWithPlans(ctx, &milvuspb.GetCompactionPlansRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_WatchChannels(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	client, err := NewClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	mockProxy := mocks.NewMockDataCoordClient(t)
	mockGrpcClient := mocks.NewMockGrpcClient[datapb.DataCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().ReCall(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, f func(datapb.DataCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockProxy)
	})
	client.grpcClient = mockGrpcClient

	// test success
	mockProxy.EXPECT().WatchChannels(mock.Anything, mock.Anything).Return(&datapb.WatchChannelsResponse{
		Status: merr.Success(),
	}, nil)
	_, err = client.WatchChannels(ctx, &datapb.WatchChannelsRequest{})
	assert.Nil(t, err)

	// test return error code
	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().WatchChannels(mock.Anything, mock.Anything).Return(&datapb.WatchChannelsResponse{
		Status: merr.Status(merr.ErrServiceNotReady),
	}, nil)

	_, err = client.WatchChannels(ctx, &datapb.WatchChannelsRequest{})
	assert.Nil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.WatchChannels(ctx, &datapb.WatchChannelsRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_GetFlushState(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	client, err := NewClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	mockProxy := mocks.NewMockDataCoordClient(t)
	mockGrpcClient := mocks.NewMockGrpcClient[datapb.DataCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().ReCall(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, f func(datapb.DataCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockProxy)
	})
	client.grpcClient = mockGrpcClient

	// test success
	mockProxy.EXPECT().GetFlushState(mock.Anything, mock.Anything).Return(&milvuspb.GetFlushStateResponse{
		Status: merr.Success(),
	}, nil)
	_, err = client.GetFlushState(ctx, &datapb.GetFlushStateRequest{})
	assert.Nil(t, err)

	// test return error code
	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().GetFlushState(mock.Anything, mock.Anything).Return(&milvuspb.GetFlushStateResponse{
		Status: merr.Status(merr.ErrServiceNotReady),
	}, nil)

	_, err = client.GetFlushState(ctx, &datapb.GetFlushStateRequest{})
	assert.Nil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.GetFlushState(ctx, &datapb.GetFlushStateRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_GetFlushAllState(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	client, err := NewClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	mockProxy := mocks.NewMockDataCoordClient(t)
	mockGrpcClient := mocks.NewMockGrpcClient[datapb.DataCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().ReCall(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, f func(datapb.DataCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockProxy)
	})
	client.grpcClient = mockGrpcClient

	// test success
	mockProxy.EXPECT().GetFlushAllState(mock.Anything, mock.Anything).Return(&milvuspb.GetFlushAllStateResponse{
		Status: merr.Success(),
	}, nil)
	_, err = client.GetFlushAllState(ctx, &milvuspb.GetFlushAllStateRequest{})
	assert.Nil(t, err)

	// test return error code
	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().GetFlushAllState(mock.Anything, mock.Anything).Return(&milvuspb.GetFlushAllStateResponse{
		Status: merr.Status(merr.ErrServiceNotReady),
	}, nil)

	_, err = client.GetFlushAllState(ctx, &milvuspb.GetFlushAllStateRequest{})
	assert.Nil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.GetFlushAllState(ctx, &milvuspb.GetFlushAllStateRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_DropVirtualChannel(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	client, err := NewClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	mockProxy := mocks.NewMockDataCoordClient(t)
	mockGrpcClient := mocks.NewMockGrpcClient[datapb.DataCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().GetNodeID().Return(1)
	mockGrpcClient.EXPECT().ReCall(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, f func(datapb.DataCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockProxy)
	})
	client.grpcClient = mockGrpcClient

	// test success
	mockProxy.EXPECT().DropVirtualChannel(mock.Anything, mock.Anything).Return(&datapb.DropVirtualChannelResponse{
		Status: merr.Success(),
	}, nil)
	_, err = client.DropVirtualChannel(ctx, &datapb.DropVirtualChannelRequest{})
	assert.Nil(t, err)

	// test return error code
	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().DropVirtualChannel(mock.Anything, mock.Anything).Return(&datapb.DropVirtualChannelResponse{
		Status: merr.Status(merr.ErrServiceNotReady),
	}, nil)

	_, err = client.DropVirtualChannel(ctx, &datapb.DropVirtualChannelRequest{})
	assert.Nil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.DropVirtualChannel(ctx, &datapb.DropVirtualChannelRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_SetSegmentState(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	client, err := NewClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	mockProxy := mocks.NewMockDataCoordClient(t)
	mockGrpcClient := mocks.NewMockGrpcClient[datapb.DataCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().GetNodeID().Return(1)
	mockGrpcClient.EXPECT().ReCall(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, f func(datapb.DataCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockProxy)
	})
	client.grpcClient = mockGrpcClient

	// test success
	mockProxy.EXPECT().SetSegmentState(mock.Anything, mock.Anything).Return(&datapb.SetSegmentStateResponse{
		Status: merr.Success(),
	}, nil)
	_, err = client.SetSegmentState(ctx, &datapb.SetSegmentStateRequest{})
	assert.Nil(t, err)

	// test return error code
	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().SetSegmentState(mock.Anything, mock.Anything).Return(&datapb.SetSegmentStateResponse{
		Status: merr.Status(merr.ErrServiceNotReady),
	}, nil)

	_, err = client.SetSegmentState(ctx, &datapb.SetSegmentStateRequest{})
	assert.Nil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.SetSegmentState(ctx, &datapb.SetSegmentStateRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_Import(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	client, err := NewClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	mockProxy := mocks.NewMockDataCoordClient(t)
	mockGrpcClient := mocks.NewMockGrpcClient[datapb.DataCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().GetNodeID().Return(1)
	mockGrpcClient.EXPECT().ReCall(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, f func(datapb.DataCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockProxy)
	})
	client.grpcClient = mockGrpcClient

	// test success
	mockProxy.EXPECT().Import(mock.Anything, mock.Anything).Return(&datapb.ImportTaskResponse{
		Status: merr.Success(),
	}, nil)
	_, err = client.Import(ctx, &datapb.ImportTaskRequest{})
	assert.Nil(t, err)

	// test return error code
	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().Import(mock.Anything, mock.Anything).Return(&datapb.ImportTaskResponse{
		Status: merr.Status(merr.ErrServiceNotReady),
	}, nil)

	_, err = client.Import(ctx, &datapb.ImportTaskRequest{})
	assert.Nil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.Import(ctx, &datapb.ImportTaskRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_UpdateSegmentStatistics(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	client, err := NewClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	mockProxy := mocks.NewMockDataCoordClient(t)
	mockGrpcClient := mocks.NewMockGrpcClient[datapb.DataCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().GetNodeID().Return(1)
	mockGrpcClient.EXPECT().ReCall(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, f func(datapb.DataCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockProxy)
	})
	client.grpcClient = mockGrpcClient

	// test success
	mockProxy.EXPECT().UpdateSegmentStatistics(mock.Anything, mock.Anything).Return(merr.Success(), nil)
	_, err = client.UpdateSegmentStatistics(ctx, &datapb.UpdateSegmentStatisticsRequest{})
	assert.Nil(t, err)

	// test return error code
	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().UpdateSegmentStatistics(mock.Anything, mock.Anything).Return(merr.Status(merr.ErrServiceNotReady), nil)

	_, err = client.UpdateSegmentStatistics(ctx, &datapb.UpdateSegmentStatisticsRequest{})
	assert.Nil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.UpdateSegmentStatistics(ctx, &datapb.UpdateSegmentStatisticsRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_UpdateChannelCheckpoint(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	client, err := NewClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	mockProxy := mocks.NewMockDataCoordClient(t)
	mockGrpcClient := mocks.NewMockGrpcClient[datapb.DataCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().GetNodeID().Return(1)
	mockGrpcClient.EXPECT().ReCall(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, f func(datapb.DataCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockProxy)
	})
	client.grpcClient = mockGrpcClient

	// test success
	mockProxy.EXPECT().UpdateChannelCheckpoint(mock.Anything, mock.Anything).Return(merr.Success(), nil)
	_, err = client.UpdateChannelCheckpoint(ctx, &datapb.UpdateChannelCheckpointRequest{})
	assert.Nil(t, err)

	// test return error code
	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().UpdateChannelCheckpoint(mock.Anything, mock.Anything).Return(merr.Status(merr.ErrServiceNotReady), nil)

	_, err = client.UpdateChannelCheckpoint(ctx, &datapb.UpdateChannelCheckpointRequest{})
	assert.Nil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.UpdateChannelCheckpoint(ctx, &datapb.UpdateChannelCheckpointRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_SaveImportSegment(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	client, err := NewClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	mockProxy := mocks.NewMockDataCoordClient(t)
	mockGrpcClient := mocks.NewMockGrpcClient[datapb.DataCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().GetNodeID().Return(1)
	mockGrpcClient.EXPECT().ReCall(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, f func(datapb.DataCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockProxy)
	})
	client.grpcClient = mockGrpcClient

	// test success
	mockProxy.EXPECT().SaveImportSegment(mock.Anything, mock.Anything).Return(merr.Success(), nil)
	_, err = client.SaveImportSegment(ctx, &datapb.SaveImportSegmentRequest{})
	assert.Nil(t, err)

	// test return error code
	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().SaveImportSegment(mock.Anything, mock.Anything).Return(merr.Status(merr.ErrServiceNotReady), nil)

	_, err = client.SaveImportSegment(ctx, &datapb.SaveImportSegmentRequest{})
	assert.Nil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.SaveImportSegment(ctx, &datapb.SaveImportSegmentRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_UnsetIsImportingState(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	client, err := NewClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	mockProxy := mocks.NewMockDataCoordClient(t)
	mockGrpcClient := mocks.NewMockGrpcClient[datapb.DataCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().GetNodeID().Return(1)
	mockGrpcClient.EXPECT().ReCall(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, f func(datapb.DataCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockProxy)
	})
	client.grpcClient = mockGrpcClient

	// test success
	mockProxy.EXPECT().UnsetIsImportingState(mock.Anything, mock.Anything).Return(merr.Success(), nil)
	_, err = client.UnsetIsImportingState(ctx, &datapb.UnsetIsImportingStateRequest{})
	assert.Nil(t, err)

	// test return error code
	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().UnsetIsImportingState(mock.Anything, mock.Anything).Return(merr.Status(merr.ErrServiceNotReady), nil)

	_, err = client.UnsetIsImportingState(ctx, &datapb.UnsetIsImportingStateRequest{})
	assert.Nil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.UnsetIsImportingState(ctx, &datapb.UnsetIsImportingStateRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_MarkSegmentsDropped(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	client, err := NewClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	mockProxy := mocks.NewMockDataCoordClient(t)
	mockGrpcClient := mocks.NewMockGrpcClient[datapb.DataCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().GetNodeID().Return(1)
	mockGrpcClient.EXPECT().ReCall(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, f func(datapb.DataCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockProxy)
	})
	client.grpcClient = mockGrpcClient

	// test success
	mockProxy.EXPECT().MarkSegmentsDropped(mock.Anything, mock.Anything).Return(merr.Success(), nil)
	_, err = client.MarkSegmentsDropped(ctx, &datapb.MarkSegmentsDroppedRequest{})
	assert.Nil(t, err)

	// test return error code
	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().MarkSegmentsDropped(mock.Anything, mock.Anything).Return(merr.Status(merr.ErrServiceNotReady), nil)

	_, err = client.MarkSegmentsDropped(ctx, &datapb.MarkSegmentsDroppedRequest{})
	assert.Nil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.MarkSegmentsDropped(ctx, &datapb.MarkSegmentsDroppedRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_BroadcastAlteredCollection(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	client, err := NewClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	mockProxy := mocks.NewMockDataCoordClient(t)
	mockGrpcClient := mocks.NewMockGrpcClient[datapb.DataCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().ReCall(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, f func(datapb.DataCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockProxy)
	})
	client.grpcClient = mockGrpcClient

	// test success
	mockProxy.EXPECT().BroadcastAlteredCollection(mock.Anything, mock.Anything).Return(merr.Success(), nil)
	_, err = client.BroadcastAlteredCollection(ctx, &datapb.AlterCollectionRequest{})
	assert.Nil(t, err)

	// test return error code
	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().BroadcastAlteredCollection(mock.Anything, mock.Anything).Return(merr.Status(merr.ErrServiceNotReady), nil)

	_, err = client.BroadcastAlteredCollection(ctx, &datapb.AlterCollectionRequest{})
	assert.Nil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.BroadcastAlteredCollection(ctx, &datapb.AlterCollectionRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_CheckHealth(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	client, err := NewClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	mockProxy := mocks.NewMockDataCoordClient(t)
	mockGrpcClient := mocks.NewMockGrpcClient[datapb.DataCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().ReCall(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, f func(datapb.DataCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockProxy)
	})
	client.grpcClient = mockGrpcClient

	// test success
	mockProxy.EXPECT().CheckHealth(mock.Anything, mock.Anything).Return(&milvuspb.CheckHealthResponse{Status: merr.Success()}, nil)
	_, err = client.CheckHealth(ctx, &milvuspb.CheckHealthRequest{})
	assert.Nil(t, err)

	// test return error code
	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().CheckHealth(mock.Anything, mock.Anything).Return(&milvuspb.CheckHealthResponse{Status: merr.Status(merr.ErrServiceNotReady)}, nil)

	_, err = client.CheckHealth(ctx, &milvuspb.CheckHealthRequest{})
	assert.Nil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.CheckHealth(ctx, &milvuspb.CheckHealthRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_GcConfirm(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	client, err := NewClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	mockProxy := mocks.NewMockDataCoordClient(t)
	mockGrpcClient := mocks.NewMockGrpcClient[datapb.DataCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().ReCall(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, f func(datapb.DataCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockProxy)
	})
	client.grpcClient = mockGrpcClient

	// test success
	mockProxy.EXPECT().GcConfirm(mock.Anything, mock.Anything).Return(&datapb.GcConfirmResponse{Status: merr.Success()}, nil)
	_, err = client.GcConfirm(ctx, &datapb.GcConfirmRequest{})
	assert.Nil(t, err)

	// test return error code
	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().GcConfirm(mock.Anything, mock.Anything).Return(&datapb.GcConfirmResponse{Status: merr.Status(merr.ErrServiceNotReady)}, nil)

	_, err = client.GcConfirm(ctx, &datapb.GcConfirmRequest{})
	assert.Nil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.GcConfirm(ctx, &datapb.GcConfirmRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_CreateIndex(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	client, err := NewClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	mockProxy := mocks.NewMockDataCoordClient(t)
	mockGrpcClient := mocks.NewMockGrpcClient[datapb.DataCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().ReCall(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, f func(datapb.DataCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockProxy)
	})
	client.grpcClient = mockGrpcClient

	// test success
	mockProxy.EXPECT().CreateIndex(mock.Anything, mock.Anything).Return(merr.Success(), nil)
	_, err = client.CreateIndex(ctx, &indexpb.CreateIndexRequest{})
	assert.Nil(t, err)

	// test return error code
	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().CreateIndex(mock.Anything, mock.Anything).Return(merr.Status(merr.ErrServiceNotReady), nil)

	_, err = client.CreateIndex(ctx, &indexpb.CreateIndexRequest{})
	assert.Nil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.CreateIndex(ctx, &indexpb.CreateIndexRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_GetSegmentIndexState(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	client, err := NewClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	mockProxy := mocks.NewMockDataCoordClient(t)
	mockGrpcClient := mocks.NewMockGrpcClient[datapb.DataCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().ReCall(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, f func(datapb.DataCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockProxy)
	})
	client.grpcClient = mockGrpcClient

	// test success
	mockProxy.EXPECT().GetSegmentIndexState(mock.Anything, mock.Anything).Return(&indexpb.GetSegmentIndexStateResponse{Status: merr.Success()}, nil)
	_, err = client.GetSegmentIndexState(ctx, &indexpb.GetSegmentIndexStateRequest{})
	assert.Nil(t, err)

	// test return error code
	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().GetSegmentIndexState(mock.Anything, mock.Anything).Return(&indexpb.GetSegmentIndexStateResponse{Status: merr.Status(err)}, nil)

	_, err = client.GetSegmentIndexState(ctx, &indexpb.GetSegmentIndexStateRequest{})
	assert.Nil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.GetSegmentIndexState(ctx, &indexpb.GetSegmentIndexStateRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_GetIndexState(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	client, err := NewClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	mockProxy := mocks.NewMockDataCoordClient(t)
	mockGrpcClient := mocks.NewMockGrpcClient[datapb.DataCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().ReCall(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, f func(datapb.DataCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockProxy)
	})
	client.grpcClient = mockGrpcClient

	// test success
	mockProxy.EXPECT().GetIndexState(mock.Anything, mock.Anything).Return(&indexpb.GetIndexStateResponse{Status: merr.Success()}, nil)
	_, err = client.GetIndexState(ctx, &indexpb.GetIndexStateRequest{})
	assert.Nil(t, err)

	// test return error code
	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().GetIndexState(mock.Anything, mock.Anything).Return(&indexpb.GetIndexStateResponse{Status: merr.Status(err)}, nil)

	_, err = client.GetIndexState(ctx, &indexpb.GetIndexStateRequest{})
	assert.Nil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.GetIndexState(ctx, &indexpb.GetIndexStateRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_GetIndexInfos(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	client, err := NewClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	mockProxy := mocks.NewMockDataCoordClient(t)
	mockGrpcClient := mocks.NewMockGrpcClient[datapb.DataCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().ReCall(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, f func(datapb.DataCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockProxy)
	})
	client.grpcClient = mockGrpcClient

	// test success
	mockProxy.EXPECT().GetIndexInfos(mock.Anything, mock.Anything).Return(&indexpb.GetIndexInfoResponse{Status: merr.Success()}, nil)
	_, err = client.GetIndexInfos(ctx, &indexpb.GetIndexInfoRequest{})
	assert.Nil(t, err)

	// test return error code
	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().GetIndexInfos(mock.Anything, mock.Anything).Return(&indexpb.GetIndexInfoResponse{Status: merr.Status(err)}, nil)

	_, err = client.GetIndexInfos(ctx, &indexpb.GetIndexInfoRequest{})
	assert.Nil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.GetIndexInfos(ctx, &indexpb.GetIndexInfoRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_DescribeIndex(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	client, err := NewClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	mockProxy := mocks.NewMockDataCoordClient(t)
	mockGrpcClient := mocks.NewMockGrpcClient[datapb.DataCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().ReCall(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, f func(datapb.DataCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockProxy)
	})
	client.grpcClient = mockGrpcClient

	// test success
	mockProxy.EXPECT().DescribeIndex(mock.Anything, mock.Anything).Return(&indexpb.DescribeIndexResponse{Status: merr.Success()}, nil)
	_, err = client.DescribeIndex(ctx, &indexpb.DescribeIndexRequest{})
	assert.Nil(t, err)

	// test return error code
	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().DescribeIndex(mock.Anything, mock.Anything).Return(&indexpb.DescribeIndexResponse{Status: merr.Status(err)}, nil)

	_, err = client.DescribeIndex(ctx, &indexpb.DescribeIndexRequest{})
	assert.Nil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.DescribeIndex(ctx, &indexpb.DescribeIndexRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_GetIndexStatistics(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	client, err := NewClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	mockProxy := mocks.NewMockDataCoordClient(t)
	mockGrpcClient := mocks.NewMockGrpcClient[datapb.DataCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().ReCall(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, f func(datapb.DataCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockProxy)
	})
	client.grpcClient = mockGrpcClient

	// test success
	mockProxy.EXPECT().GetIndexStatistics(mock.Anything, mock.Anything).Return(&indexpb.GetIndexStatisticsResponse{Status: merr.Success()}, nil)
	_, err = client.GetIndexStatistics(ctx, &indexpb.GetIndexStatisticsRequest{})
	assert.Nil(t, err)

	// test return error code
	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().GetIndexStatistics(mock.Anything, mock.Anything).Return(&indexpb.GetIndexStatisticsResponse{Status: merr.Status(err)}, nil)

	_, err = client.GetIndexStatistics(ctx, &indexpb.GetIndexStatisticsRequest{})
	assert.Nil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.GetIndexStatistics(ctx, &indexpb.GetIndexStatisticsRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_GetIndexBuildProgress(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	client, err := NewClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	mockProxy := mocks.NewMockDataCoordClient(t)
	mockGrpcClient := mocks.NewMockGrpcClient[datapb.DataCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().ReCall(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, f func(datapb.DataCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockProxy)
	})
	client.grpcClient = mockGrpcClient

	// test success
	mockProxy.EXPECT().GetIndexBuildProgress(mock.Anything, mock.Anything).Return(&indexpb.GetIndexBuildProgressResponse{Status: merr.Success()}, nil)
	_, err = client.GetIndexBuildProgress(ctx, &indexpb.GetIndexBuildProgressRequest{})
	assert.Nil(t, err)

	// test return error code
	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().GetIndexBuildProgress(mock.Anything, mock.Anything).Return(&indexpb.GetIndexBuildProgressResponse{Status: merr.Status(err)}, nil)

	_, err = client.GetIndexBuildProgress(ctx, &indexpb.GetIndexBuildProgressRequest{})
	assert.Nil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.GetIndexBuildProgress(ctx, &indexpb.GetIndexBuildProgressRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_DropIndex(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	client, err := NewClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	mockProxy := mocks.NewMockDataCoordClient(t)
	mockGrpcClient := mocks.NewMockGrpcClient[datapb.DataCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().ReCall(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, f func(datapb.DataCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockProxy)
	})
	client.grpcClient = mockGrpcClient

	// test success
	mockProxy.EXPECT().DropIndex(mock.Anything, mock.Anything).Return(merr.Success(), nil)
	_, err = client.DropIndex(ctx, &indexpb.DropIndexRequest{})
	assert.Nil(t, err)

	// test return error code
	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().DropIndex(mock.Anything, mock.Anything).Return(merr.Status(err), nil)

	_, err = client.DropIndex(ctx, &indexpb.DropIndexRequest{})
	assert.Nil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.DropIndex(ctx, &indexpb.DropIndexRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_ReportDataNodeTtMsgs(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	client, err := NewClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	mockProxy := mocks.NewMockDataCoordClient(t)
	mockGrpcClient := mocks.NewMockGrpcClient[datapb.DataCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().ReCall(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, f func(datapb.DataCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockProxy)
	})
	client.grpcClient = mockGrpcClient

	// test success
	mockProxy.EXPECT().ReportDataNodeTtMsgs(mock.Anything, mock.Anything).Return(merr.Success(), nil)
	_, err = client.ReportDataNodeTtMsgs(ctx, &datapb.ReportDataNodeTtMsgsRequest{})
	assert.Nil(t, err)

	// test return error code
	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().ReportDataNodeTtMsgs(mock.Anything, mock.Anything).Return(merr.Status(err), nil)

	_, err = client.ReportDataNodeTtMsgs(ctx, &datapb.ReportDataNodeTtMsgsRequest{})
	assert.Nil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.ReportDataNodeTtMsgs(ctx, &datapb.ReportDataNodeTtMsgsRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_GcControl(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	client, err := NewClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	mockProxy := mocks.NewMockDataCoordClient(t)
	mockGrpcClient := mocks.NewMockGrpcClient[datapb.DataCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().ReCall(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, f func(datapb.DataCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockProxy)
	})
	client.grpcClient = mockGrpcClient

	// test success
	mockProxy.EXPECT().GcControl(mock.Anything, mock.Anything).Return(merr.Success(), nil)
	_, err = client.GcControl(ctx, &datapb.GcControlRequest{})
	assert.Nil(t, err)

	// test return error code
	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().GcControl(mock.Anything, mock.Anything).Return(merr.Status(err), nil)

	_, err = client.GcControl(ctx, &datapb.GcControlRequest{})
	assert.Nil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.GcControl(ctx, &datapb.GcControlRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}
