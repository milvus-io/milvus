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

package grpcmixcoordclient

import (
	"context"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	mock1 "github.com/stretchr/testify/mock"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/util/mock"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/etcd"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

var mockErr = errors.New("mock grpc err")

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

	checkFunc := func(retNotNil bool) {
		retCheck := func(notNil bool, ret interface{}, err error) {
			if notNil {
				assert.NotNil(t, ret)
				assert.NoError(t, err)
			} else {
				assert.Nil(t, ret)
				assert.Error(t, err)
			}
		}

		{
			r, err := client.GetComponentStates(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.GetTimeTickChannel(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.GetStatisticsChannel(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.CreateCollection(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.DropCollection(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.HasCollection(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.DescribeCollection(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.ShowCollections(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.ShowCollectionIDs(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.CreatePartition(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.DropPartition(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.HasPartition(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.ShowPartitions(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.AllocTimestamp(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.AllocID(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.UpdateChannelTimeTick(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.ShowSegments(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.GetPChannelInfo(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.GetMetrics(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.CreateAlias(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.DropAlias(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.AlterAlias(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.DescribeAlias(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.ListAliases(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.CreateCredential(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.GetCredential(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.UpdateCredential(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.DeleteCredential(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.ListCredUsers(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.InvalidateCollectionMetaCache(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.CreateRole(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.DropRole(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.OperateUserRole(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.SelectRole(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.SelectUser(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.OperatePrivilege(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.SelectGrant(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.ListPolicy(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.ShowConfigurations(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.CheckHealth(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.CreateDatabase(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.DropDatabase(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.ListDatabases(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.AlterCollection(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.AlterDatabase(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.BackupRBAC(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.RestoreRBAC(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.CreatePrivilegeGroup(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.DropPrivilegeGroup(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.ListPrivilegeGroups(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.OperatePrivilegeGroup(ctx, nil)
			retCheck(retNotNil, r, err)
		}

		r4, err := client.ShowLoadCollections(ctx, nil)
		retCheck(retNotNil, r4, err)

		r5, err := client.ShowLoadPartitions(ctx, nil)
		retCheck(retNotNil, r5, err)

		r6, err := client.LoadPartitions(ctx, nil)
		retCheck(retNotNil, r6, err)

		r7, err := client.ReleasePartitions(ctx, nil)
		retCheck(retNotNil, r7, err)

		r7, err = client.SyncNewCreatedPartition(ctx, nil)
		retCheck(retNotNil, r7, err)

		r8, err := client.ShowLoadCollections(ctx, nil)
		retCheck(retNotNil, r8, err)

		r9, err := client.LoadCollection(ctx, nil)
		retCheck(retNotNil, r9, err)

		r10, err := client.ReleaseCollection(ctx, nil)
		retCheck(retNotNil, r10, err)

		r12, err := client.ShowPartitions(ctx, nil)
		retCheck(retNotNil, r12, err)

		r13, err := client.GetPartitionStates(ctx, nil)
		retCheck(retNotNil, r13, err)

		r14, err := client.GetLoadSegmentInfo(ctx, nil)
		retCheck(retNotNil, r14, err)

		r16, err := client.LoadBalance(ctx, nil)
		retCheck(retNotNil, r16, err)

		r17, err := client.GetReplicas(ctx, nil)
		retCheck(retNotNil, r17, err)

		r18, err := client.GetShardLeaders(ctx, nil)
		retCheck(retNotNil, r18, err)

		r21, err := client.CreateResourceGroup(ctx, nil)
		retCheck(retNotNil, r21, err)

		r22, err := client.DropResourceGroup(ctx, nil)
		retCheck(retNotNil, r22, err)

		r23, err := client.TransferNode(ctx, nil)
		retCheck(retNotNil, r23, err)

		r24, err := client.TransferReplica(ctx, nil)
		retCheck(retNotNil, r24, err)

		r26, err := client.ListResourceGroups(ctx, nil)
		retCheck(retNotNil, r26, err)

		r27, err := client.DescribeResourceGroup(ctx, nil)
		retCheck(retNotNil, r27, err)

		r28, err := client.ListCheckers(ctx, nil)
		retCheck(retNotNil, r28, err)

		r29, err := client.ActivateChecker(ctx, nil)
		retCheck(retNotNil, r29, err)

		r30, err := client.DeactivateChecker(ctx, nil)
		retCheck(retNotNil, r30, err)

		r31, err := client.ListQueryNode(ctx, nil)
		retCheck(retNotNil, r31, err)

		r32, err := client.GetQueryNodeDistribution(ctx, nil)
		retCheck(retNotNil, r32, err)

		r33, err := client.SuspendBalance(ctx, nil)
		retCheck(retNotNil, r33, err)

		r34, err := client.ResumeBalance(ctx, nil)
		retCheck(retNotNil, r34, err)

		r35, err := client.SuspendNode(ctx, nil)
		retCheck(retNotNil, r35, err)

		r36, err := client.ResumeNode(ctx, nil)
		retCheck(retNotNil, r36, err)

		r37, err := client.TransferSegment(ctx, nil)
		retCheck(retNotNil, r37, err)

		r38, err := client.TransferChannel(ctx, nil)
		retCheck(retNotNil, r38, err)

		r39, err := client.CheckQueryNodeDistribution(ctx, nil)
		retCheck(retNotNil, r39, err)

		r40, err := client.CheckBalanceStatus(ctx, nil)
		retCheck(retNotNil, r40, err)
	}

	client.(*Client).grpcClient = &mock.GRPCClientBase[MixCoordClient]{
		GetGrpcClientErr: errors.New("dummy"),
	}

	newFunc1 := func(cc *grpc.ClientConn) MixCoordClient {
		return MixCoordClient{
			RootCoordClient:  &mock.GrpcRootCoordClient{Err: nil},
			QueryCoordClient: &mock.GrpcQueryCoordClient{Err: nil},
		}
	}
	client.(*Client).grpcClient.SetNewGrpcClientFunc(newFunc1)

	checkFunc(false)

	client.(*Client).grpcClient = &mock.GRPCClientBase[MixCoordClient]{
		GetGrpcClientErr: nil,
	}

	newFunc2 := func(cc *grpc.ClientConn) MixCoordClient {
		return MixCoordClient{
			RootCoordClient:  &mock.GrpcRootCoordClient{Err: errors.New("dummy")},
			QueryCoordClient: &mock.GrpcQueryCoordClient{Err: errors.New("dummy")},
		}
	}

	client.(*Client).grpcClient.SetNewGrpcClientFunc(newFunc2)

	checkFunc(false)

	client.(*Client).grpcClient = &mock.GRPCClientBase[MixCoordClient]{
		GetGrpcClientErr: nil,
	}

	newFunc3 := func(cc *grpc.ClientConn) MixCoordClient {
		return MixCoordClient{
			RootCoordClient:  &mock.GrpcRootCoordClient{Err: nil},
			QueryCoordClient: &mock.GrpcQueryCoordClient{Err: nil},
		}
	}
	client.(*Client).grpcClient.SetNewGrpcClientFunc(newFunc3)

	checkFunc(true)

	// timeout
	timeout := time.Nanosecond
	shortCtx, shortCancel := context.WithTimeout(ctx, timeout)
	defer shortCancel()
	time.Sleep(timeout)

	retCheck := func(ret interface{}, err error) {
		assert.Nil(t, ret)
		assert.Error(t, err)
	}
	{
		rTimeout, err := client.GetComponentStates(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.GetTimeTickChannel(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.GetStatisticsChannel(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.CreateCollection(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.DropCollection(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.HasCollection(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.DescribeCollection(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.ShowCollections(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.ShowCollectionIDs(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.CreatePartition(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.DropPartition(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.HasPartition(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.ShowPartitions(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.AllocTimestamp(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.AllocID(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.UpdateChannelTimeTick(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.ShowSegments(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.GetPChannelInfo(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.GetMetrics(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.CreateAlias(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.DropAlias(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.AlterAlias(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.DescribeAlias(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.ListAliases(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.CreateCredential(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.GetCredential(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.UpdateCredential(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.DeleteCredential(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.ListCredUsers(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.InvalidateCollectionMetaCache(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.CreateRole(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.DropRole(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.OperateUserRole(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.SelectRole(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.SelectUser(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.OperatePrivilege(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.SelectGrant(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.ListPolicy(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.CheckHealth(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.CreateDatabase(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.DropDatabase(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.ListDatabases(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	// clean up
	err = client.Close()
	assert.NoError(t, err)
}

func Test_Flush(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	client, err := NewClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	mockDC := mocks.NewMockDataCoordClient(t)
	mockmix := MixCoordClient{
		DataCoordClient: mockDC,
	}
	mockGrpcClient := mocks.NewMockGrpcClient[MixCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().GetNodeID().Return(1)
	mockGrpcClient.EXPECT().ReCall(mock1.Anything, mock1.Anything).RunAndReturn(func(ctx context.Context, f func(MixCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockmix)
	})
	client.(*Client).grpcClient = mockGrpcClient

	// test success
	mockDC.EXPECT().Flush(mock1.Anything, mock1.Anything).Return(&datapb.FlushResponse{
		Status: merr.Success(),
	}, nil)
	_, err = client.Flush(ctx, &datapb.FlushRequest{})
	assert.Nil(t, err)

	// test return error status
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().Flush(mock1.Anything, mock1.Anything).Return(&datapb.FlushResponse{
		Status: merr.Status(merr.ErrServiceNotReady),
	}, nil)

	rsp, err := client.Flush(ctx, &datapb.FlushRequest{})
	assert.NotEqual(t, int32(0), rsp.GetStatus().GetCode())
	assert.Nil(t, err)

	// test return error
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().Flush(mock1.Anything, mock1.Anything).Return(&datapb.FlushResponse{
		Status: merr.Success(),
	}, mockErr)

	_, err = client.Flush(ctx, &datapb.FlushRequest{})
	assert.NotNil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.Flush(ctx, &datapb.FlushRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_GetSegmentStates(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	client, err := NewClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	mockDC := mocks.NewMockDataCoordClient(t)
	mockmix := MixCoordClient{
		DataCoordClient: mockDC,
	}
	mockGrpcClient := mocks.NewMockGrpcClient[MixCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().GetNodeID().Return(1)
	mockGrpcClient.EXPECT().ReCall(mock1.Anything, mock1.Anything).RunAndReturn(func(ctx context.Context, f func(MixCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockmix)
	})
	client.(*Client).grpcClient = mockGrpcClient
	// test success
	mockDC.EXPECT().GetSegmentStates(mock1.Anything, mock1.Anything).Return(&datapb.GetSegmentStatesResponse{
		Status: merr.Success(),
	}, nil)
	_, err = client.GetSegmentStates(ctx, &datapb.GetSegmentStatesRequest{})
	assert.Nil(t, err)

	// test return error status
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().GetSegmentStates(mock1.Anything, mock1.Anything).Return(&datapb.GetSegmentStatesResponse{
		Status: merr.Status(merr.ErrServiceNotReady),
	}, nil)

	rsp, err := client.GetSegmentStates(ctx, &datapb.GetSegmentStatesRequest{})
	assert.NotEqual(t, int32(0), rsp.GetStatus().GetCode())
	assert.Nil(t, err)

	// test return error
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().GetSegmentStates(mock1.Anything, mock1.Anything).Return(&datapb.GetSegmentStatesResponse{
		Status: merr.Success(),
	}, mockErr)

	_, err = client.GetSegmentStates(ctx, &datapb.GetSegmentStatesRequest{})
	assert.NotNil(t, err)

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

	mockDC := mocks.NewMockDataCoordClient(t)
	mockmix := MixCoordClient{
		DataCoordClient: mockDC,
	}
	mockGrpcClient := mocks.NewMockGrpcClient[MixCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().GetNodeID().Return(1)
	mockGrpcClient.EXPECT().ReCall(mock1.Anything, mock1.Anything).RunAndReturn(func(ctx context.Context, f func(MixCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockmix)
	})
	client.(*Client).grpcClient = mockGrpcClient
	// test success
	mockDC.EXPECT().GetInsertBinlogPaths(mock1.Anything, mock1.Anything).Return(&datapb.GetInsertBinlogPathsResponse{
		Status: merr.Success(),
	}, nil)
	_, err = client.GetInsertBinlogPaths(ctx, &datapb.GetInsertBinlogPathsRequest{})
	assert.Nil(t, err)

	// test return error status
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().GetInsertBinlogPaths(mock1.Anything, mock1.Anything).Return(&datapb.GetInsertBinlogPathsResponse{
		Status: merr.Status(merr.ErrServiceNotReady),
	}, nil)

	rsp, err := client.GetInsertBinlogPaths(ctx, &datapb.GetInsertBinlogPathsRequest{})
	assert.NotEqual(t, int32(0), rsp.GetStatus().GetCode())
	assert.Nil(t, err)

	// test return error
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().GetInsertBinlogPaths(mock1.Anything, mock1.Anything).Return(&datapb.GetInsertBinlogPathsResponse{
		Status: merr.Success(),
	}, mockErr)

	_, err = client.GetInsertBinlogPaths(ctx, &datapb.GetInsertBinlogPathsRequest{})
	assert.NotNil(t, err)

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

	mockDC := mocks.NewMockDataCoordClient(t)
	mockmix := MixCoordClient{
		DataCoordClient: mockDC,
	}
	mockGrpcClient := mocks.NewMockGrpcClient[MixCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().GetNodeID().Return(1)
	mockGrpcClient.EXPECT().ReCall(mock1.Anything, mock1.Anything).RunAndReturn(func(ctx context.Context, f func(MixCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockmix)
	})
	client.(*Client).grpcClient = mockGrpcClient
	// test success
	mockDC.EXPECT().GetCollectionStatistics(mock1.Anything, mock1.Anything).Return(&datapb.GetCollectionStatisticsResponse{
		Status: merr.Success(),
	}, nil)
	_, err = client.GetCollectionStatistics(ctx, &datapb.GetCollectionStatisticsRequest{})
	assert.Nil(t, err)

	// test return error status
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().GetCollectionStatistics(mock1.Anything, mock1.Anything).Return(&datapb.GetCollectionStatisticsResponse{
		Status: merr.Status(merr.ErrServiceNotReady),
	}, nil)

	rsp, err := client.GetCollectionStatistics(ctx, &datapb.GetCollectionStatisticsRequest{})
	assert.NotEqual(t, int32(0), rsp.GetStatus().GetCode())
	assert.Nil(t, err)

	// test return error
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().GetCollectionStatistics(mock1.Anything, mock1.Anything).Return(&datapb.GetCollectionStatisticsResponse{
		Status: merr.Success(),
	}, mockErr)

	_, err = client.GetCollectionStatistics(ctx, &datapb.GetCollectionStatisticsRequest{})
	assert.NotNil(t, err)

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

	mockDC := mocks.NewMockDataCoordClient(t)
	mockmix := MixCoordClient{
		DataCoordClient: mockDC,
	}
	mockGrpcClient := mocks.NewMockGrpcClient[MixCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().GetNodeID().Return(1)
	mockGrpcClient.EXPECT().ReCall(mock1.Anything, mock1.Anything).RunAndReturn(func(ctx context.Context, f func(MixCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockmix)
	})
	client.(*Client).grpcClient = mockGrpcClient
	// test success
	mockDC.EXPECT().GetPartitionStatistics(mock1.Anything, mock1.Anything).Return(&datapb.GetPartitionStatisticsResponse{
		Status: merr.Success(),
	}, nil)
	_, err = client.GetPartitionStatistics(ctx, &datapb.GetPartitionStatisticsRequest{})
	assert.Nil(t, err)

	// test return error status
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().GetPartitionStatistics(mock1.Anything, mock1.Anything).Return(&datapb.GetPartitionStatisticsResponse{
		Status: merr.Status(merr.ErrServiceNotReady),
	}, nil)

	rsp, err := client.GetPartitionStatistics(ctx, &datapb.GetPartitionStatisticsRequest{})
	assert.NotEqual(t, int32(0), rsp.GetStatus().GetCode())
	assert.Nil(t, err)

	// test return error
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().GetPartitionStatistics(mock1.Anything, mock1.Anything).Return(&datapb.GetPartitionStatisticsResponse{
		Status: merr.Success(),
	}, mockErr)

	_, err = client.GetPartitionStatistics(ctx, &datapb.GetPartitionStatisticsRequest{})
	assert.NotNil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.GetPartitionStatistics(ctx, &datapb.GetPartitionStatisticsRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_GetSegmentInfo(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	client, err := NewClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	mockDC := mocks.NewMockDataCoordClient(t)
	mockmix := MixCoordClient{
		DataCoordClient: mockDC,
	}
	mockGrpcClient := mocks.NewMockGrpcClient[MixCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().GetNodeID().Return(1)
	mockGrpcClient.EXPECT().ReCall(mock1.Anything, mock1.Anything).RunAndReturn(func(ctx context.Context, f func(MixCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockmix)
	})
	client.(*Client).grpcClient = mockGrpcClient

	// test success
	mockDC.EXPECT().GetSegmentInfo(mock1.Anything, mock1.Anything).Return(&datapb.GetSegmentInfoResponse{
		Status: merr.Success(),
	}, nil)
	_, err = client.GetSegmentInfo(ctx, &datapb.GetSegmentInfoRequest{})
	assert.Nil(t, err)

	// test return error status
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().GetSegmentInfo(mock1.Anything, mock1.Anything).Return(&datapb.GetSegmentInfoResponse{
		Status: merr.Status(merr.ErrServiceNotReady),
	}, nil)

	rsp, err := client.GetSegmentInfo(ctx, &datapb.GetSegmentInfoRequest{})
	assert.NotEqual(t, int32(0), rsp.GetStatus().GetCode())
	assert.Nil(t, err)

	// test return error
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().GetSegmentInfo(mock1.Anything, mock1.Anything).Return(&datapb.GetSegmentInfoResponse{
		Status: merr.Success(),
	}, mockErr)

	_, err = client.GetSegmentInfo(ctx, &datapb.GetSegmentInfoRequest{})
	assert.NotNil(t, err)

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

	mockDC := mocks.NewMockDataCoordClient(t)
	mockmix := MixCoordClient{
		DataCoordClient: mockDC,
	}
	mockGrpcClient := mocks.NewMockGrpcClient[MixCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().GetNodeID().Return(1)
	mockGrpcClient.EXPECT().ReCall(mock1.Anything, mock1.Anything).RunAndReturn(func(ctx context.Context, f func(MixCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockmix)
	})
	client.(*Client).grpcClient = mockGrpcClient

	// test success
	mockDC.EXPECT().GetSegmentInfo(mock1.Anything, mock1.Anything).Return(&datapb.GetSegmentInfoResponse{
		Status: merr.Success(),
	}, nil)
	_, err = client.GetSegmentInfo(ctx, &datapb.GetSegmentInfoRequest{})
	assert.Nil(t, err)

	// test return error status
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().GetSegmentInfo(mock1.Anything, mock1.Anything).Return(&datapb.GetSegmentInfoResponse{
		Status: merr.Status(merr.ErrServiceNotReady),
	}, nil)

	rsp, err := client.GetSegmentInfo(ctx, &datapb.GetSegmentInfoRequest{})
	assert.NotEqual(t, int32(0), rsp.GetStatus().GetCode())
	assert.Nil(t, err)

	// test return error
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().GetSegmentInfo(mock1.Anything, mock1.Anything).Return(&datapb.GetSegmentInfoResponse{
		Status: merr.Success(),
	}, mockErr)

	_, err = client.GetSegmentInfo(ctx, &datapb.GetSegmentInfoRequest{})
	assert.NotNil(t, err)

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

	mockDC := mocks.NewMockDataCoordClient(t)
	mockmix := MixCoordClient{
		DataCoordClient: mockDC,
	}
	mockGrpcClient := mocks.NewMockGrpcClient[MixCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().GetNodeID().Return(1)
	mockGrpcClient.EXPECT().ReCall(mock1.Anything, mock1.Anything).RunAndReturn(func(ctx context.Context, f func(MixCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockmix)
	})
	client.(*Client).grpcClient = mockGrpcClient

	// test success
	mockDC.EXPECT().GetRecoveryInfo(mock1.Anything, mock1.Anything).Return(&datapb.GetRecoveryInfoResponse{
		Status: merr.Success(),
	}, nil)
	_, err = client.GetRecoveryInfo(ctx, &datapb.GetRecoveryInfoRequest{})
	assert.Nil(t, err)

	// test return error status
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().GetRecoveryInfo(mock1.Anything, mock1.Anything).Return(&datapb.GetRecoveryInfoResponse{
		Status: merr.Status(merr.ErrServiceNotReady),
	}, nil)

	rsp, err := client.GetRecoveryInfo(ctx, &datapb.GetRecoveryInfoRequest{})
	assert.NotEqual(t, int32(0), rsp.GetStatus().GetCode())
	assert.Nil(t, err)

	// test return error
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().GetRecoveryInfo(mock1.Anything, mock1.Anything).Return(&datapb.GetRecoveryInfoResponse{
		Status: merr.Success(),
	}, mockErr)

	_, err = client.GetRecoveryInfo(ctx, &datapb.GetRecoveryInfoRequest{})
	assert.NotNil(t, err)

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

	mockDC := mocks.NewMockDataCoordClient(t)
	mockmix := MixCoordClient{
		DataCoordClient: mockDC,
	}
	mockGrpcClient := mocks.NewMockGrpcClient[MixCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().ReCall(mock1.Anything, mock1.Anything).RunAndReturn(func(ctx context.Context, f func(MixCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockmix)
	})
	client.(*Client).grpcClient = mockGrpcClient

	// test success
	mockDC.EXPECT().GetRecoveryInfoV2(mock1.Anything, mock1.Anything).Return(&datapb.GetRecoveryInfoResponseV2{
		Status: merr.Success(),
	}, nil)
	_, err = client.GetRecoveryInfoV2(ctx, &datapb.GetRecoveryInfoRequestV2{})
	assert.Nil(t, err)

	// test return error status
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().GetRecoveryInfoV2(mock1.Anything, mock1.Anything).Return(&datapb.GetRecoveryInfoResponseV2{
		Status: merr.Status(merr.ErrServiceNotReady),
	}, nil)

	rsp, err := client.GetRecoveryInfoV2(ctx, &datapb.GetRecoveryInfoRequestV2{})
	assert.NotEqual(t, int32(0), rsp.GetStatus().GetCode())
	assert.Nil(t, err)

	// test return error
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().GetRecoveryInfoV2(mock1.Anything, mock1.Anything).Return(&datapb.GetRecoveryInfoResponseV2{
		Status: merr.Success(),
	}, mockErr)

	_, err = client.GetRecoveryInfoV2(ctx, &datapb.GetRecoveryInfoRequestV2{})
	assert.NotNil(t, err)

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

	mockDC := mocks.NewMockDataCoordClient(t)
	mockmix := MixCoordClient{
		DataCoordClient: mockDC,
	}
	mockGrpcClient := mocks.NewMockGrpcClient[MixCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().GetNodeID().Return(1)
	mockGrpcClient.EXPECT().ReCall(mock1.Anything, mock1.Anything).RunAndReturn(func(ctx context.Context, f func(MixCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockmix)
	})
	client.(*Client).grpcClient = mockGrpcClient

	// test success
	mockDC.EXPECT().GetFlushedSegments(mock1.Anything, mock1.Anything).Return(&datapb.GetFlushedSegmentsResponse{
		Status: merr.Success(),
	}, nil)
	_, err = client.GetFlushedSegments(ctx, &datapb.GetFlushedSegmentsRequest{})
	assert.Nil(t, err)

	// test return error status
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().GetFlushedSegments(mock1.Anything, mock1.Anything).Return(&datapb.GetFlushedSegmentsResponse{
		Status: merr.Status(merr.ErrServiceNotReady),
	}, nil)

	rsp, err := client.GetFlushedSegments(ctx, &datapb.GetFlushedSegmentsRequest{})
	assert.NotEqual(t, int32(0), rsp.GetStatus().GetCode())
	assert.Nil(t, err)

	// test return error
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().GetFlushedSegments(mock1.Anything, mock1.Anything).Return(&datapb.GetFlushedSegmentsResponse{
		Status: merr.Success(),
	}, mockErr)

	_, err = client.GetFlushedSegments(ctx, &datapb.GetFlushedSegmentsRequest{})
	assert.NotNil(t, err)

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

	mockDC := mocks.NewMockDataCoordClient(t)
	mockmix := MixCoordClient{
		DataCoordClient: mockDC,
	}
	mockGrpcClient := mocks.NewMockGrpcClient[MixCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().GetNodeID().Return(1)
	mockGrpcClient.EXPECT().ReCall(mock1.Anything, mock1.Anything).RunAndReturn(func(ctx context.Context, f func(MixCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockmix)
	})
	client.(*Client).grpcClient = mockGrpcClient

	// test success
	mockDC.EXPECT().GetSegmentsByStates(mock1.Anything, mock1.Anything).Return(&datapb.GetSegmentsByStatesResponse{
		Status: merr.Success(),
	}, nil)
	_, err = client.GetSegmentsByStates(ctx, &datapb.GetSegmentsByStatesRequest{})
	assert.Nil(t, err)

	// test return error status
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().GetSegmentsByStates(mock1.Anything, mock1.Anything).Return(&datapb.GetSegmentsByStatesResponse{
		Status: merr.Status(merr.ErrServiceNotReady),
	}, nil)

	rsp, err := client.GetSegmentsByStates(ctx, &datapb.GetSegmentsByStatesRequest{})
	assert.NotEqual(t, int32(0), rsp.GetStatus().GetCode())
	assert.Nil(t, err)

	// test return error
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().GetSegmentsByStates(mock1.Anything, mock1.Anything).Return(&datapb.GetSegmentsByStatesResponse{
		Status: merr.Success(),
	}, mockErr)

	_, err = client.GetSegmentsByStates(ctx, &datapb.GetSegmentsByStatesRequest{})
	assert.NotNil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.GetSegmentsByStates(ctx, &datapb.GetSegmentsByStatesRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_ManualCompaction(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	client, err := NewClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	mockDC := mocks.NewMockDataCoordClient(t)
	mockmix := MixCoordClient{
		DataCoordClient: mockDC,
	}
	mockGrpcClient := mocks.NewMockGrpcClient[MixCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().ReCall(mock1.Anything, mock1.Anything).RunAndReturn(func(ctx context.Context, f func(MixCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockmix)
	})
	client.(*Client).grpcClient = mockGrpcClient

	// test success
	mockDC.EXPECT().ManualCompaction(mock1.Anything, mock1.Anything).Return(&milvuspb.ManualCompactionResponse{
		Status: merr.Success(),
	}, nil)
	_, err = client.ManualCompaction(ctx, &milvuspb.ManualCompactionRequest{})
	assert.Nil(t, err)

	// test return error status
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().ManualCompaction(mock1.Anything, mock1.Anything).Return(&milvuspb.ManualCompactionResponse{
		Status: merr.Status(merr.ErrServiceNotReady),
	}, nil)

	rsp, err := client.ManualCompaction(ctx, &milvuspb.ManualCompactionRequest{})
	assert.NotEqual(t, int32(0), rsp.GetStatus().GetCode())
	assert.Nil(t, err)

	// test return error
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().ManualCompaction(mock1.Anything, mock1.Anything).Return(&milvuspb.ManualCompactionResponse{
		Status: merr.Success(),
	}, mockErr)

	_, err = client.ManualCompaction(ctx, &milvuspb.ManualCompactionRequest{})
	assert.NotNil(t, err)

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

	mockDC := mocks.NewMockDataCoordClient(t)
	mockmix := MixCoordClient{
		DataCoordClient: mockDC,
	}
	mockGrpcClient := mocks.NewMockGrpcClient[MixCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().ReCall(mock1.Anything, mock1.Anything).RunAndReturn(func(ctx context.Context, f func(MixCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockmix)
	})
	client.(*Client).grpcClient = mockGrpcClient

	// test success
	mockDC.EXPECT().GetCompactionState(mock1.Anything, mock1.Anything).Return(&milvuspb.GetCompactionStateResponse{
		Status: merr.Success(),
	}, nil)
	_, err = client.GetCompactionState(ctx, &milvuspb.GetCompactionStateRequest{})
	assert.Nil(t, err)

	// test return error status
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().GetCompactionState(mock1.Anything, mock1.Anything).Return(&milvuspb.GetCompactionStateResponse{
		Status: merr.Status(merr.ErrServiceNotReady),
	}, nil)

	rsp, err := client.GetCompactionState(ctx, &milvuspb.GetCompactionStateRequest{})
	assert.NotEqual(t, int32(0), rsp.GetStatus().GetCode())
	assert.Nil(t, err)

	// test return error
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().GetCompactionState(mock1.Anything, mock1.Anything).Return(&milvuspb.GetCompactionStateResponse{
		Status: merr.Success(),
	}, mockErr)

	_, err = client.GetCompactionState(ctx, &milvuspb.GetCompactionStateRequest{})
	assert.NotNil(t, err)

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

	mockDC := mocks.NewMockDataCoordClient(t)
	mockmix := MixCoordClient{
		DataCoordClient: mockDC,
	}
	mockGrpcClient := mocks.NewMockGrpcClient[MixCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().ReCall(mock1.Anything, mock1.Anything).RunAndReturn(func(ctx context.Context, f func(MixCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockmix)
	})
	client.(*Client).grpcClient = mockGrpcClient

	// test success
	mockDC.EXPECT().GetCompactionStateWithPlans(mock1.Anything, mock1.Anything).Return(&milvuspb.GetCompactionPlansResponse{
		Status: merr.Success(),
	}, nil)
	_, err = client.GetCompactionStateWithPlans(ctx, &milvuspb.GetCompactionPlansRequest{})
	assert.Nil(t, err)

	// test return error status
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().GetCompactionStateWithPlans(mock1.Anything, mock1.Anything).Return(&milvuspb.GetCompactionPlansResponse{
		Status: merr.Status(merr.ErrServiceNotReady),
	}, nil)

	rsp, err := client.GetCompactionStateWithPlans(ctx, &milvuspb.GetCompactionPlansRequest{})
	assert.NotEqual(t, int32(0), rsp.GetStatus().GetCode())
	assert.Nil(t, err)

	// test return error
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().GetCompactionStateWithPlans(mock1.Anything, mock1.Anything).Return(&milvuspb.GetCompactionPlansResponse{
		Status: merr.Success(),
	}, mockErr)

	_, err = client.GetCompactionStateWithPlans(ctx, &milvuspb.GetCompactionPlansRequest{})
	assert.NotNil(t, err)

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

	mockDC := mocks.NewMockDataCoordClient(t)
	mockmix := MixCoordClient{
		DataCoordClient: mockDC,
	}
	mockGrpcClient := mocks.NewMockGrpcClient[MixCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().ReCall(mock1.Anything, mock1.Anything).RunAndReturn(func(ctx context.Context, f func(MixCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockmix)
	})
	client.(*Client).grpcClient = mockGrpcClient

	// test success
	mockDC.EXPECT().WatchChannels(mock1.Anything, mock1.Anything).Return(&datapb.WatchChannelsResponse{
		Status: merr.Success(),
	}, nil)
	_, err = client.WatchChannels(ctx, &datapb.WatchChannelsRequest{})
	assert.Nil(t, err)

	// test return error status
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().WatchChannels(mock1.Anything, mock1.Anything).Return(&datapb.WatchChannelsResponse{
		Status: merr.Status(merr.ErrServiceNotReady),
	}, nil)

	rsp, err := client.WatchChannels(ctx, &datapb.WatchChannelsRequest{})
	assert.NotEqual(t, int32(0), rsp.GetStatus().GetCode())
	assert.Nil(t, err)

	// test return error
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().WatchChannels(mock1.Anything, mock1.Anything).Return(&datapb.WatchChannelsResponse{
		Status: merr.Success(),
	}, mockErr)

	_, err = client.WatchChannels(ctx, &datapb.WatchChannelsRequest{})
	assert.NotNil(t, err)

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

	mockDC := mocks.NewMockDataCoordClient(t)
	mockmix := MixCoordClient{
		DataCoordClient: mockDC,
	}
	mockGrpcClient := mocks.NewMockGrpcClient[MixCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().ReCall(mock1.Anything, mock1.Anything).RunAndReturn(func(ctx context.Context, f func(MixCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockmix)
	})
	client.(*Client).grpcClient = mockGrpcClient

	// test success
	mockDC.EXPECT().GetFlushState(mock1.Anything, mock1.Anything).Return(&milvuspb.GetFlushStateResponse{
		Status: merr.Success(),
	}, nil)
	_, err = client.GetFlushState(ctx, &datapb.GetFlushStateRequest{})
	assert.Nil(t, err)

	// test return error status
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().GetFlushState(mock1.Anything, mock1.Anything).Return(&milvuspb.GetFlushStateResponse{
		Status: merr.Status(merr.ErrServiceNotReady),
	}, nil)

	rsp, err := client.GetFlushState(ctx, &datapb.GetFlushStateRequest{})
	assert.NotEqual(t, int32(0), rsp.GetStatus().GetCode())
	assert.Nil(t, err)

	// test return error
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().GetFlushState(mock1.Anything, mock1.Anything).Return(&milvuspb.GetFlushStateResponse{
		Status: merr.Success(),
	}, mockErr)

	_, err = client.GetFlushState(ctx, &datapb.GetFlushStateRequest{})
	assert.NotNil(t, err)

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

	mockDC := mocks.NewMockDataCoordClient(t)
	mockmix := MixCoordClient{
		DataCoordClient: mockDC,
	}
	mockGrpcClient := mocks.NewMockGrpcClient[MixCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().ReCall(mock1.Anything, mock1.Anything).RunAndReturn(func(ctx context.Context, f func(MixCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockmix)
	})
	client.(*Client).grpcClient = mockGrpcClient

	// test success
	mockDC.EXPECT().GetFlushAllState(mock1.Anything, mock1.Anything).Return(&milvuspb.GetFlushAllStateResponse{
		Status: merr.Success(),
	}, nil)
	_, err = client.GetFlushAllState(ctx, &milvuspb.GetFlushAllStateRequest{})
	assert.Nil(t, err)

	// test return error status
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().GetFlushAllState(mock1.Anything, mock1.Anything).Return(&milvuspb.GetFlushAllStateResponse{
		Status: merr.Status(merr.ErrServiceNotReady),
	}, nil)

	rsp, err := client.GetFlushAllState(ctx, &milvuspb.GetFlushAllStateRequest{})
	assert.NotEqual(t, int32(0), rsp.GetStatus().GetCode())
	assert.Nil(t, err)

	// test return error
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().GetFlushAllState(mock1.Anything, mock1.Anything).Return(&milvuspb.GetFlushAllStateResponse{
		Status: merr.Success(),
	}, mockErr)

	_, err = client.GetFlushAllState(ctx, &milvuspb.GetFlushAllStateRequest{})
	assert.NotNil(t, err)

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

	mockDC := mocks.NewMockDataCoordClient(t)
	mockmix := MixCoordClient{
		DataCoordClient: mockDC,
	}
	mockGrpcClient := mocks.NewMockGrpcClient[MixCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().GetNodeID().Return(1)
	mockGrpcClient.EXPECT().ReCall(mock1.Anything, mock1.Anything).RunAndReturn(func(ctx context.Context, f func(MixCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockmix)
	})
	client.(*Client).grpcClient = mockGrpcClient

	// test success
	mockDC.EXPECT().DropVirtualChannel(mock1.Anything, mock1.Anything).Return(&datapb.DropVirtualChannelResponse{
		Status: merr.Success(),
	}, nil)
	_, err = client.DropVirtualChannel(ctx, &datapb.DropVirtualChannelRequest{})
	assert.Nil(t, err)

	// test return error status
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().DropVirtualChannel(mock1.Anything, mock1.Anything).Return(&datapb.DropVirtualChannelResponse{
		Status: merr.Status(merr.ErrServiceNotReady),
	}, nil)

	rsp, err := client.DropVirtualChannel(ctx, &datapb.DropVirtualChannelRequest{})
	assert.NotEqual(t, int32(0), rsp.GetStatus().GetCode())
	assert.Nil(t, err)

	// test return error
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().DropVirtualChannel(mock1.Anything, mock1.Anything).Return(&datapb.DropVirtualChannelResponse{
		Status: merr.Success(),
	}, mockErr)

	_, err = client.DropVirtualChannel(ctx, &datapb.DropVirtualChannelRequest{})
	assert.NotNil(t, err)

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

	mockDC := mocks.NewMockDataCoordClient(t)
	mockmix := MixCoordClient{
		DataCoordClient: mockDC,
	}
	mockGrpcClient := mocks.NewMockGrpcClient[MixCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().GetNodeID().Return(1)
	mockGrpcClient.EXPECT().ReCall(mock1.Anything, mock1.Anything).RunAndReturn(func(ctx context.Context, f func(MixCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockmix)
	})
	client.(*Client).grpcClient = mockGrpcClient

	// test success
	mockDC.EXPECT().SetSegmentState(mock1.Anything, mock1.Anything).Return(&datapb.SetSegmentStateResponse{
		Status: merr.Success(),
	}, nil)
	_, err = client.SetSegmentState(ctx, &datapb.SetSegmentStateRequest{})
	assert.Nil(t, err)

	// test return error status
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().SetSegmentState(mock1.Anything, mock1.Anything).Return(&datapb.SetSegmentStateResponse{
		Status: merr.Status(merr.ErrServiceNotReady),
	}, nil)

	rsp, err := client.SetSegmentState(ctx, &datapb.SetSegmentStateRequest{})
	assert.NotEqual(t, int32(0), rsp.GetStatus().GetCode())
	assert.Nil(t, err)

	// test return error
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().SetSegmentState(mock1.Anything, mock1.Anything).Return(&datapb.SetSegmentStateResponse{
		Status: merr.Success(),
	}, mockErr)

	_, err = client.SetSegmentState(ctx, &datapb.SetSegmentStateRequest{})
	assert.NotNil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.SetSegmentState(ctx, &datapb.SetSegmentStateRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_UpdateSegmentStatistics(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	client, err := NewClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	mockDC := mocks.NewMockDataCoordClient(t)
	mockmix := MixCoordClient{
		DataCoordClient: mockDC,
	}
	mockGrpcClient := mocks.NewMockGrpcClient[MixCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().GetNodeID().Return(1)
	mockGrpcClient.EXPECT().ReCall(mock1.Anything, mock1.Anything).RunAndReturn(func(ctx context.Context, f func(MixCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockmix)
	})
	client.(*Client).grpcClient = mockGrpcClient

	// test success
	mockDC.EXPECT().UpdateSegmentStatistics(mock1.Anything, mock1.Anything).Return(merr.Success(), nil)
	_, err = client.UpdateSegmentStatistics(ctx, &datapb.UpdateSegmentStatisticsRequest{})
	assert.Nil(t, err)

	// test return error status
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().UpdateSegmentStatistics(mock1.Anything, mock1.Anything).Return(merr.Status(merr.ErrServiceNotReady), nil)

	rsp, err := client.UpdateSegmentStatistics(ctx, &datapb.UpdateSegmentStatisticsRequest{})
	assert.NotEqual(t, int32(0), rsp.GetCode())
	assert.Nil(t, err)

	// test return error
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().UpdateSegmentStatistics(mock1.Anything, mock1.Anything).Return(merr.Success(), mockErr)

	_, err = client.UpdateSegmentStatistics(ctx, &datapb.UpdateSegmentStatisticsRequest{})
	assert.NotNil(t, err)

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

	mockDC := mocks.NewMockDataCoordClient(t)
	mockmix := MixCoordClient{
		DataCoordClient: mockDC,
	}
	mockGrpcClient := mocks.NewMockGrpcClient[MixCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().GetNodeID().Return(1)
	mockGrpcClient.EXPECT().ReCall(mock1.Anything, mock1.Anything).RunAndReturn(func(ctx context.Context, f func(MixCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockmix)
	})
	client.(*Client).grpcClient = mockGrpcClient

	// test success
	mockDC.EXPECT().UpdateChannelCheckpoint(mock1.Anything, mock1.Anything).Return(merr.Success(), nil)
	_, err = client.UpdateChannelCheckpoint(ctx, &datapb.UpdateChannelCheckpointRequest{})
	assert.Nil(t, err)

	// test return error status
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().UpdateChannelCheckpoint(mock1.Anything, mock1.Anything).Return(merr.Status(merr.ErrServiceNotReady), nil)

	rsp, err := client.UpdateChannelCheckpoint(ctx, &datapb.UpdateChannelCheckpointRequest{})
	assert.NotEqual(t, int32(0), rsp.GetCode())
	assert.Nil(t, err)

	// test return error
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().UpdateChannelCheckpoint(mock1.Anything, mock1.Anything).Return(merr.Success(), mockErr)

	_, err = client.UpdateChannelCheckpoint(ctx, &datapb.UpdateChannelCheckpointRequest{})
	assert.NotNil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.UpdateChannelCheckpoint(ctx, &datapb.UpdateChannelCheckpointRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_MarkSegmentsDropped(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	client, err := NewClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	mockDC := mocks.NewMockDataCoordClient(t)
	mockmix := MixCoordClient{
		DataCoordClient: mockDC,
	}
	mockGrpcClient := mocks.NewMockGrpcClient[MixCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().GetNodeID().Return(1)
	mockGrpcClient.EXPECT().ReCall(mock1.Anything, mock1.Anything).RunAndReturn(func(ctx context.Context, f func(MixCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockmix)
	})
	client.(*Client).grpcClient = mockGrpcClient

	// test success
	mockDC.EXPECT().MarkSegmentsDropped(mock1.Anything, mock1.Anything).Return(merr.Success(), nil)
	_, err = client.MarkSegmentsDropped(ctx, &datapb.MarkSegmentsDroppedRequest{})
	assert.Nil(t, err)

	// test return error status
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().MarkSegmentsDropped(mock1.Anything, mock1.Anything).Return(merr.Status(merr.ErrServiceNotReady), nil)

	rsp, err := client.MarkSegmentsDropped(ctx, &datapb.MarkSegmentsDroppedRequest{})
	assert.NotEqual(t, int32(0), rsp.GetCode())
	assert.Nil(t, err)

	// test return error
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().MarkSegmentsDropped(mock1.Anything, mock1.Anything).Return(
		merr.Success(), mockErr)

	_, err = client.MarkSegmentsDropped(ctx, &datapb.MarkSegmentsDroppedRequest{})
	assert.NotNil(t, err)

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

	mockDC := mocks.NewMockDataCoordClient(t)
	mockmix := MixCoordClient{
		DataCoordClient: mockDC,
	}
	mockGrpcClient := mocks.NewMockGrpcClient[MixCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().ReCall(mock1.Anything, mock1.Anything).RunAndReturn(func(ctx context.Context, f func(MixCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockmix)
	})
	client.(*Client).grpcClient = mockGrpcClient

	// test success
	mockDC.EXPECT().BroadcastAlteredCollection(mock1.Anything, mock1.Anything).Return(merr.Success(), nil)
	_, err = client.BroadcastAlteredCollection(ctx, &datapb.AlterCollectionRequest{})
	assert.Nil(t, err)

	// test return error status
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().BroadcastAlteredCollection(mock1.Anything, mock1.Anything).Return(merr.Status(merr.ErrServiceNotReady), nil)

	rsp, err := client.BroadcastAlteredCollection(ctx, &datapb.AlterCollectionRequest{})
	assert.NotEqual(t, int32(0), rsp.GetCode())
	assert.Nil(t, err)

	// test return error
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().BroadcastAlteredCollection(mock1.Anything, mock1.Anything).Return(
		merr.Success(), mockErr)

	_, err = client.BroadcastAlteredCollection(ctx, &datapb.AlterCollectionRequest{})
	assert.NotNil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.BroadcastAlteredCollection(ctx, &datapb.AlterCollectionRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_GcConfirm(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	client, err := NewClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	mockDC := mocks.NewMockDataCoordClient(t)
	mockmix := MixCoordClient{
		DataCoordClient: mockDC,
	}
	mockGrpcClient := mocks.NewMockGrpcClient[MixCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().ReCall(mock1.Anything, mock1.Anything).RunAndReturn(func(ctx context.Context, f func(MixCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockmix)
	})
	client.(*Client).grpcClient = mockGrpcClient

	// test success
	mockDC.EXPECT().GcConfirm(mock1.Anything, mock1.Anything).Return(&datapb.GcConfirmResponse{Status: merr.Success()}, nil)
	_, err = client.GcConfirm(ctx, &datapb.GcConfirmRequest{})
	assert.Nil(t, err)

	// test return error status
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().GcConfirm(mock1.Anything, mock1.Anything).Return(&datapb.GcConfirmResponse{Status: merr.Status(merr.ErrServiceNotReady)}, nil)

	rsp, err := client.GcConfirm(ctx, &datapb.GcConfirmRequest{})
	assert.NotEqual(t, int32(0), rsp.GetStatus().GetCode())
	assert.Nil(t, err)

	// test return error
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().GcConfirm(mock1.Anything, mock1.Anything).Return(&datapb.GcConfirmResponse{
		Status: merr.Success(),
	}, mockErr)

	_, err = client.GcConfirm(ctx, &datapb.GcConfirmRequest{})
	assert.NotNil(t, err)

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

	mockDC := mocks.NewMockDataCoordClient(t)
	mockmix := MixCoordClient{
		DataCoordClient: mockDC,
	}
	mockGrpcClient := mocks.NewMockGrpcClient[MixCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().ReCall(mock1.Anything, mock1.Anything).RunAndReturn(func(ctx context.Context, f func(MixCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockmix)
	})
	client.(*Client).grpcClient = mockGrpcClient

	// test success
	mockDC.EXPECT().CreateIndex(mock1.Anything, mock1.Anything).Return(merr.Success(), nil)
	_, err = client.CreateIndex(ctx, &indexpb.CreateIndexRequest{})
	assert.Nil(t, err)

	// test compatible with 2.2.x
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().CreateIndex(mock1.Anything, mock1.Anything).Return(nil, merr.ErrServiceUnimplemented).Times(1)
	mockDC.EXPECT().CreateIndex(mock1.Anything, mock1.Anything).Return(merr.Success(), nil)
	_, err = client.CreateIndex(ctx, &indexpb.CreateIndexRequest{})
	assert.Nil(t, err)

	// test return error status
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().CreateIndex(mock1.Anything, mock1.Anything).Return(merr.Status(merr.ErrServiceNotReady), nil)

	rsp, err := client.CreateIndex(ctx, &indexpb.CreateIndexRequest{})
	assert.NotEqual(t, int32(0), rsp.GetCode())
	assert.Nil(t, err)

	// test return error
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().CreateIndex(mock1.Anything, mock1.Anything).Return(merr.Success(), mockErr)

	_, err = client.CreateIndex(ctx, &indexpb.CreateIndexRequest{})
	assert.NotNil(t, err)

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

	mockDC := mocks.NewMockDataCoordClient(t)
	mockmix := MixCoordClient{
		DataCoordClient: mockDC,
	}
	mockGrpcClient := mocks.NewMockGrpcClient[MixCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().ReCall(mock1.Anything, mock1.Anything).RunAndReturn(func(ctx context.Context, f func(MixCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockmix)
	})
	client.(*Client).grpcClient = mockGrpcClient

	// test success
	mockDC.EXPECT().GetSegmentIndexState(mock1.Anything, mock1.Anything).Return(&indexpb.GetSegmentIndexStateResponse{Status: merr.Success()}, nil)
	_, err = client.GetSegmentIndexState(ctx, &indexpb.GetSegmentIndexStateRequest{})
	assert.Nil(t, err)

	// test compatible with 2.2.x
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().GetSegmentIndexState(mock1.Anything, mock1.Anything).Return(nil, merr.ErrServiceUnimplemented).Times(1)
	mockDC.EXPECT().GetSegmentIndexState(mock1.Anything, mock1.Anything).Return(&indexpb.GetSegmentIndexStateResponse{}, nil)
	_, err = client.GetSegmentIndexState(ctx, &indexpb.GetSegmentIndexStateRequest{})
	assert.Nil(t, err)

	// test return error status
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().GetSegmentIndexState(mock1.Anything, mock1.Anything).Return(&indexpb.GetSegmentIndexStateResponse{
		Status: merr.Status(merr.ErrServiceNotReady),
	}, nil)

	rsp, err := client.GetSegmentIndexState(ctx, &indexpb.GetSegmentIndexStateRequest{})
	assert.NotEqual(t, int32(0), rsp.GetStatus().GetCode())
	assert.Nil(t, err)

	// test return error
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().GetSegmentIndexState(mock1.Anything, mock1.Anything).Return(&indexpb.GetSegmentIndexStateResponse{
		Status: merr.Success(),
	}, mockErr)

	_, err = client.GetSegmentIndexState(ctx, &indexpb.GetSegmentIndexStateRequest{})
	assert.NotNil(t, err)

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

	mockDC := mocks.NewMockDataCoordClient(t)
	mockmix := MixCoordClient{
		DataCoordClient: mockDC,
	}
	mockGrpcClient := mocks.NewMockGrpcClient[MixCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().ReCall(mock1.Anything, mock1.Anything).RunAndReturn(func(ctx context.Context, f func(MixCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockmix)
	})
	client.(*Client).grpcClient = mockGrpcClient

	// test success
	mockDC.EXPECT().GetIndexState(mock1.Anything, mock1.Anything).Return(&indexpb.GetIndexStateResponse{Status: merr.Success()}, nil)
	_, err = client.GetIndexState(ctx, &indexpb.GetIndexStateRequest{})
	assert.Nil(t, err)

	// test compatible with 2.2.x
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().GetIndexState(mock1.Anything, mock1.Anything).Return(nil, merr.ErrServiceUnimplemented).Times(1)
	mockDC.EXPECT().GetIndexState(mock1.Anything, mock1.Anything).Return(&indexpb.GetIndexStateResponse{}, nil)
	_, err = client.GetIndexState(ctx, &indexpb.GetIndexStateRequest{})
	assert.Nil(t, err)

	// test return error status
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().GetIndexState(mock1.Anything, mock1.Anything).Return(&indexpb.GetIndexStateResponse{
		Status: merr.Status(merr.ErrServiceNotReady),
	}, nil)

	rsp, err := client.GetIndexState(ctx, &indexpb.GetIndexStateRequest{})
	assert.NotEqual(t, int32(0), rsp.GetStatus().GetCode())
	assert.Nil(t, err)

	// test return error
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().GetIndexState(mock1.Anything, mock1.Anything).Return(&indexpb.GetIndexStateResponse{
		Status: merr.Success(),
	}, mockErr)
	_, err = client.GetIndexState(ctx, &indexpb.GetIndexStateRequest{})
	assert.NotNil(t, err)

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

	mockDC := mocks.NewMockDataCoordClient(t)
	mockmix := MixCoordClient{
		DataCoordClient: mockDC,
	}
	mockGrpcClient := mocks.NewMockGrpcClient[MixCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().ReCall(mock1.Anything, mock1.Anything).RunAndReturn(func(ctx context.Context, f func(MixCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockmix)
	})
	client.(*Client).grpcClient = mockGrpcClient

	// test success
	mockDC.EXPECT().GetIndexInfos(mock1.Anything, mock1.Anything).Return(&indexpb.GetIndexInfoResponse{Status: merr.Success()}, nil)
	_, err = client.GetIndexInfos(ctx, &indexpb.GetIndexInfoRequest{})
	assert.Nil(t, err)

	// test compatible with 2.2.x
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().GetIndexInfos(mock1.Anything, mock1.Anything).Return(nil, merr.ErrServiceUnimplemented).Times(1)
	mockDC.EXPECT().GetIndexInfos(mock1.Anything, mock1.Anything).Return(&indexpb.GetIndexInfoResponse{}, nil)
	_, err = client.GetIndexInfos(ctx, &indexpb.GetIndexInfoRequest{})
	assert.Nil(t, err)

	// test return error status
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().GetIndexInfos(mock1.Anything, mock1.Anything).Return(&indexpb.GetIndexInfoResponse{
		Status: merr.Status(merr.ErrServiceNotReady),
	}, nil)

	rsp, err := client.GetIndexInfos(ctx, &indexpb.GetIndexInfoRequest{})
	assert.NotEqual(t, int32(0), rsp.GetStatus().GetCode())
	assert.Nil(t, err)

	// test return error
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().GetIndexInfos(mock1.Anything, mock1.Anything).Return(&indexpb.GetIndexInfoResponse{
		Status: merr.Success(),
	}, mockErr)

	_, err = client.GetIndexInfos(ctx, &indexpb.GetIndexInfoRequest{})
	assert.NotNil(t, err)

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

	mockDC := mocks.NewMockDataCoordClient(t)
	mockmix := MixCoordClient{
		DataCoordClient: mockDC,
	}
	mockGrpcClient := mocks.NewMockGrpcClient[MixCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().ReCall(mock1.Anything, mock1.Anything).RunAndReturn(func(ctx context.Context, f func(MixCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockmix)
	})
	client.(*Client).grpcClient = mockGrpcClient

	// test success
	mockDC.EXPECT().DescribeIndex(mock1.Anything, mock1.Anything).Return(&indexpb.DescribeIndexResponse{Status: merr.Success()}, nil)
	_, err = client.DescribeIndex(ctx, &indexpb.DescribeIndexRequest{})
	assert.Nil(t, err)

	// test compatible with 2.2.x
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().DescribeIndex(mock1.Anything, mock1.Anything).Return(nil, merr.ErrServiceUnimplemented).Times(1)
	mockDC.EXPECT().DescribeIndex(mock1.Anything, mock1.Anything).Return(&indexpb.DescribeIndexResponse{}, nil)
	_, err = client.DescribeIndex(ctx, &indexpb.DescribeIndexRequest{})
	assert.Nil(t, err)

	// test return error status
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().DescribeIndex(mock1.Anything, mock1.Anything).Return(&indexpb.DescribeIndexResponse{
		Status: merr.Status(merr.ErrServiceNotReady),
	}, nil)

	rsp, err := client.DescribeIndex(ctx, &indexpb.DescribeIndexRequest{})
	assert.NotEqual(t, int32(0), rsp.GetStatus().GetCode())
	assert.Nil(t, err)

	// test return error
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().DescribeIndex(mock1.Anything, mock1.Anything).Return(&indexpb.DescribeIndexResponse{
		Status: merr.Success(),
	}, mockErr)

	_, err = client.DescribeIndex(ctx, &indexpb.DescribeIndexRequest{})
	assert.NotNil(t, err)

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

	mockDC := mocks.NewMockDataCoordClient(t)
	mockmix := MixCoordClient{
		DataCoordClient: mockDC,
	}
	mockGrpcClient := mocks.NewMockGrpcClient[MixCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().ReCall(mock1.Anything, mock1.Anything).RunAndReturn(func(ctx context.Context, f func(MixCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockmix)
	})
	client.(*Client).grpcClient = mockGrpcClient

	// test success
	mockDC.EXPECT().GetIndexStatistics(mock1.Anything, mock1.Anything).Return(&indexpb.GetIndexStatisticsResponse{Status: merr.Success()}, nil)
	_, err = client.GetIndexStatistics(ctx, &indexpb.GetIndexStatisticsRequest{})
	assert.Nil(t, err)

	// test compatible with 2.2.x
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().GetIndexStatistics(mock1.Anything, mock1.Anything).Return(nil, merr.ErrServiceUnimplemented).Times(1)
	mockDC.EXPECT().GetIndexStatistics(mock1.Anything, mock1.Anything).Return(&indexpb.GetIndexStatisticsResponse{}, nil)
	_, err = client.GetIndexStatistics(ctx, &indexpb.GetIndexStatisticsRequest{})
	assert.Nil(t, err)

	// test return error status
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().GetIndexStatistics(mock1.Anything, mock1.Anything).Return(&indexpb.GetIndexStatisticsResponse{
		Status: merr.Status(merr.ErrServiceNotReady),
	}, nil)

	rsp, err := client.GetIndexStatistics(ctx, &indexpb.GetIndexStatisticsRequest{})
	assert.NotEqual(t, int32(0), rsp.GetStatus().GetCode())
	assert.Nil(t, err)

	// test return error
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().GetIndexStatistics(mock1.Anything, mock1.Anything).Return(&indexpb.GetIndexStatisticsResponse{
		Status: merr.Success(),
	}, mockErr)

	_, err = client.GetIndexStatistics(ctx, &indexpb.GetIndexStatisticsRequest{})
	assert.NotNil(t, err)

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

	mockDC := mocks.NewMockDataCoordClient(t)
	mockmix := MixCoordClient{
		DataCoordClient: mockDC,
	}
	mockGrpcClient := mocks.NewMockGrpcClient[MixCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().ReCall(mock1.Anything, mock1.Anything).RunAndReturn(func(ctx context.Context, f func(MixCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockmix)
	})
	client.(*Client).grpcClient = mockGrpcClient

	// test success
	mockDC.EXPECT().GetIndexBuildProgress(mock1.Anything, mock1.Anything).Return(&indexpb.GetIndexBuildProgressResponse{Status: merr.Success()}, nil)
	_, err = client.GetIndexBuildProgress(ctx, &indexpb.GetIndexBuildProgressRequest{})
	assert.Nil(t, err)

	// test compatible with 2.2.x
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().GetIndexBuildProgress(mock1.Anything, mock1.Anything).Return(nil, merr.ErrServiceUnimplemented).Times(1)
	mockDC.EXPECT().GetIndexBuildProgress(mock1.Anything, mock1.Anything).Return(&indexpb.GetIndexBuildProgressResponse{}, nil)
	_, err = client.GetIndexBuildProgress(ctx, &indexpb.GetIndexBuildProgressRequest{})
	assert.Nil(t, err)

	// test return error status
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().GetIndexBuildProgress(mock1.Anything, mock1.Anything).Return(&indexpb.GetIndexBuildProgressResponse{
		Status: merr.Status(merr.ErrServiceNotReady),
	}, nil)

	rsp, err := client.GetIndexBuildProgress(ctx, &indexpb.GetIndexBuildProgressRequest{})
	assert.NotEqual(t, int32(0), rsp.GetStatus().GetCode())
	assert.Nil(t, err)

	// test return error
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().GetIndexBuildProgress(mock1.Anything, mock1.Anything).Return(&indexpb.GetIndexBuildProgressResponse{
		Status: merr.Success(),
	}, mockErr)

	_, err = client.GetIndexBuildProgress(ctx, &indexpb.GetIndexBuildProgressRequest{})
	assert.NotNil(t, err)

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

	mockDC := mocks.NewMockDataCoordClient(t)
	mockmix := MixCoordClient{
		DataCoordClient: mockDC,
	}
	mockGrpcClient := mocks.NewMockGrpcClient[MixCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().ReCall(mock1.Anything, mock1.Anything).RunAndReturn(func(ctx context.Context, f func(MixCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockmix)
	})
	client.(*Client).grpcClient = mockGrpcClient

	// test success
	mockDC.EXPECT().DropIndex(mock1.Anything, mock1.Anything).Return(merr.Success(), nil)
	_, err = client.DropIndex(ctx, &indexpb.DropIndexRequest{})
	assert.Nil(t, err)

	// test compatible with 2.2.x
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().DropIndex(mock1.Anything, mock1.Anything).Return(nil, merr.ErrServiceUnimplemented).Times(1)
	mockDC.EXPECT().DropIndex(mock1.Anything, mock1.Anything).Return(&commonpb.Status{}, nil)
	_, err = client.DropIndex(ctx, &indexpb.DropIndexRequest{})
	assert.Nil(t, err)

	// test return error status
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().DropIndex(mock1.Anything, mock1.Anything).Return(
		merr.Status(merr.ErrServiceNotReady), nil)

	rsp, err := client.DropIndex(ctx, &indexpb.DropIndexRequest{})
	assert.NotEqual(t, int32(0), rsp.GetCode())
	assert.Nil(t, err)

	// test return error
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().DropIndex(mock1.Anything, mock1.Anything).Return(merr.Success(), mockErr)

	_, err = client.DropIndex(ctx, &indexpb.DropIndexRequest{})
	assert.NotNil(t, err)

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

	mockDC := mocks.NewMockDataCoordClient(t)
	mockmix := MixCoordClient{
		DataCoordClient: mockDC,
	}
	mockGrpcClient := mocks.NewMockGrpcClient[MixCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().ReCall(mock1.Anything, mock1.Anything).RunAndReturn(func(ctx context.Context, f func(MixCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockmix)
	})
	client.(*Client).grpcClient = mockGrpcClient

	// test success
	mockDC.EXPECT().ReportDataNodeTtMsgs(mock1.Anything, mock1.Anything).Return(merr.Success(), nil)
	_, err = client.ReportDataNodeTtMsgs(ctx, &datapb.ReportDataNodeTtMsgsRequest{})
	assert.Nil(t, err)

	// test return error status
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().ReportDataNodeTtMsgs(mock1.Anything, mock1.Anything).Return(
		merr.Status(merr.ErrServiceNotReady), nil)

	rsp, err := client.ReportDataNodeTtMsgs(ctx, &datapb.ReportDataNodeTtMsgsRequest{})
	assert.NotEqual(t, int32(0), rsp.GetCode())
	assert.Nil(t, err)

	// test return error
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().ReportDataNodeTtMsgs(mock1.Anything, mock1.Anything).Return(merr.Success(), mockErr)

	_, err = client.ReportDataNodeTtMsgs(ctx, &datapb.ReportDataNodeTtMsgsRequest{})
	assert.NotNil(t, err)

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

	mockDC := mocks.NewMockDataCoordClient(t)
	mockmix := MixCoordClient{
		DataCoordClient: mockDC,
	}
	mockGrpcClient := mocks.NewMockGrpcClient[MixCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().ReCall(mock1.Anything, mock1.Anything).RunAndReturn(func(ctx context.Context, f func(MixCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockmix)
	})
	client.(*Client).grpcClient = mockGrpcClient

	// test success
	mockDC.EXPECT().GcControl(mock1.Anything, mock1.Anything).Return(merr.Success(), nil)
	_, err = client.GcControl(ctx, &datapb.GcControlRequest{})
	assert.Nil(t, err)

	// test return error status
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().GcControl(mock1.Anything, mock1.Anything).Return(
		merr.Status(merr.ErrServiceNotReady), nil)

	rsp, err := client.GcControl(ctx, &datapb.GcControlRequest{})
	assert.NotEqual(t, int32(0), rsp.GetCode())
	assert.Nil(t, err)

	// test return error
	mockDC.ExpectedCalls = nil
	mockDC.EXPECT().GcControl(mock1.Anything, mock1.Anything).Return(merr.Success(), mockErr)

	_, err = client.GcControl(ctx, &datapb.GcControlRequest{})
	assert.NotNil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.GcControl(ctx, &datapb.GcControlRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_ListIndexes(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	client, err := NewClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	mockDC := mocks.NewMockDataCoordClient(t)
	mockmix := MixCoordClient{
		DataCoordClient: mockDC,
	}
	mockGrpcClient := mocks.NewMockGrpcClient[MixCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().ReCall(mock1.Anything, mock1.Anything).RunAndReturn(func(ctx context.Context, f func(MixCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockmix)
	})
	client.(*Client).grpcClient = mockGrpcClient

	// test success
	mockDC.EXPECT().ListIndexes(mock1.Anything, mock1.Anything).Return(&indexpb.ListIndexesResponse{
		Status: merr.Success(),
	}, nil).Once()
	_, err = client.ListIndexes(ctx, &indexpb.ListIndexesRequest{})
	assert.Nil(t, err)

	// test return error status
	mockDC.EXPECT().ListIndexes(mock1.Anything, mock1.Anything).Return(
		&indexpb.ListIndexesResponse{
			Status: merr.Status(merr.ErrServiceNotReady),
		}, nil).Once()

	rsp, err := client.ListIndexes(ctx, &indexpb.ListIndexesRequest{})

	assert.Nil(t, err)
	assert.False(t, merr.Ok(rsp.GetStatus()))

	// test return error
	mockDC.EXPECT().ListIndexes(mock1.Anything, mock1.Anything).Return(nil, mockErr).Once()

	_, err = client.ListIndexes(ctx, &indexpb.ListIndexesRequest{})
	assert.Error(t, err)

	// test ctx done
	ctx, cancel := context.WithCancel(ctx)
	cancel()
	_, err = client.ListIndexes(ctx, &indexpb.ListIndexesRequest{})
	assert.ErrorIs(t, err, context.Canceled)
}

func Test_GetChannelRecoveryInfo(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	client, err := NewClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	mockDC := mocks.NewMockDataCoordClient(t)
	mockmix := MixCoordClient{
		DataCoordClient: mockDC,
	}
	mockGrpcClient := mocks.NewMockGrpcClient[MixCoordClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().ReCall(mock1.Anything, mock1.Anything).RunAndReturn(func(ctx context.Context, f func(MixCoordClient) (interface{}, error)) (interface{}, error) {
		return f(mockmix)
	})
	client.(*Client).grpcClient = mockGrpcClient

	// test success
	mockDC.EXPECT().GetChannelRecoveryInfo(mock1.Anything, mock1.Anything).Return(&datapb.GetChannelRecoveryInfoResponse{
		Status: merr.Success(),
	}, nil).Once()
	_, err = client.GetChannelRecoveryInfo(ctx, &datapb.GetChannelRecoveryInfoRequest{})
	assert.Nil(t, err)

	// test return error status
	mockDC.EXPECT().GetChannelRecoveryInfo(mock1.Anything, mock1.Anything).Return(
		&datapb.GetChannelRecoveryInfoResponse{
			Status: merr.Status(merr.ErrServiceNotReady),
		}, nil).Once()

	rsp, err := client.GetChannelRecoveryInfo(ctx, &datapb.GetChannelRecoveryInfoRequest{})

	assert.Nil(t, err)
	assert.False(t, merr.Ok(rsp.GetStatus()))

	// test return error
	mockDC.EXPECT().GetChannelRecoveryInfo(mock1.Anything, mock1.Anything).Return(nil, mockErr).Once()
	_, err = client.GetChannelRecoveryInfo(ctx, &datapb.GetChannelRecoveryInfoRequest{})
	assert.Error(t, err)

	// test ctx done
	ctx, cancel := context.WithCancel(ctx)
	cancel()
	_, err = client.GetChannelRecoveryInfo(ctx, &datapb.GetChannelRecoveryInfoRequest{})
	assert.ErrorIs(t, err, context.Canceled)
}
