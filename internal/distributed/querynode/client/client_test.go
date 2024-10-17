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

package grpcquerynodeclient

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/utils"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/mock"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/stretchr/testify/assert"
	testify_mock "github.com/stretchr/testify/mock"
	"google.golang.org/grpc"
)

func Test_NewClient(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	client, err := NewClient(ctx, "", 1)
	assert.Nil(t, client)
	assert.Error(t, err)

	client, err = NewClient(ctx, "test", 2)
	assert.NoError(t, err)
	assert.NotNil(t, client)

	ctx, cancel := context.WithCancel(ctx)

	checkFunc := func(retNotNil bool) {
		retCheck := func(notNil bool, ret any, err error) {
			if notNil {
				assert.NotNil(t, ret)
				assert.NoError(t, err)
			} else {
				assert.Nil(t, ret)
				assert.Error(t, err)
			}
		}

		r1, err := client.GetComponentStates(ctx, nil)
		retCheck(retNotNil, r1, err)

		r2, err := client.GetTimeTickChannel(ctx, nil)
		retCheck(retNotNil, r2, err)

		r3, err := client.GetStatisticsChannel(ctx, nil)
		retCheck(retNotNil, r3, err)

		r6, err := client.WatchDmChannels(ctx, nil)
		retCheck(retNotNil, r6, err)

		r7, err := client.LoadSegments(ctx, nil)
		retCheck(retNotNil, r7, err)

		r8, err := client.ReleaseCollection(ctx, nil)
		retCheck(retNotNil, r8, err)

		r8, err = client.LoadPartitions(ctx, nil)
		retCheck(retNotNil, r8, err)

		r9, err := client.ReleasePartitions(ctx, nil)
		retCheck(retNotNil, r9, err)

		r10, err := client.ReleaseSegments(ctx, nil)
		retCheck(retNotNil, r10, err)

		r11, err := client.GetSegmentInfo(ctx, nil)
		retCheck(retNotNil, r11, err)

		r12, err := client.GetMetrics(ctx, nil)
		retCheck(retNotNil, r12, err)

		r14, err := client.Search(ctx, nil)
		retCheck(retNotNil, r14, err)

		r15, err := client.Query(ctx, nil)
		retCheck(retNotNil, r15, err)

		r16, err := client.SyncReplicaSegments(ctx, nil)
		retCheck(retNotNil, r16, err)

		r17, err := client.GetStatistics(ctx, nil)
		retCheck(retNotNil, r17, err)

		r18, err := client.ShowConfigurations(ctx, nil)
		retCheck(retNotNil, r18, err)

		r19, err := client.QuerySegments(ctx, nil)
		retCheck(retNotNil, r19, err)

		r20, err := client.SearchSegments(ctx, nil)
		retCheck(retNotNil, r20, err)

		// stream rpc
		client, err := client.QueryStream(ctx, nil)
		retCheck(retNotNil, client, err)
	}

	client.(*Client).grpcClient = &mock.GRPCClientBase[querypb.QueryNodeClient]{
		GetGrpcClientErr: errors.New("dummy"),
	}

	newFunc1 := func(cc *grpc.ClientConn) querypb.QueryNodeClient {
		return &mock.GrpcQueryNodeClient{Err: nil}
	}
	client.(*Client).grpcClient.SetNewGrpcClientFunc(newFunc1)

	checkFunc(false)

	client.(*Client).grpcClient = &mock.GRPCClientBase[querypb.QueryNodeClient]{
		GetGrpcClientErr: nil,
	}

	newFunc2 := func(cc *grpc.ClientConn) querypb.QueryNodeClient {
		return &mock.GrpcQueryNodeClient{Err: errors.New("dummy")}
	}

	client.(*Client).grpcClient.SetNewGrpcClientFunc(newFunc2)

	checkFunc(false)

	client.(*Client).grpcClient = &mock.GRPCClientBase[querypb.QueryNodeClient]{
		GetGrpcClientErr: nil,
	}

	newFunc3 := func(cc *grpc.ClientConn) querypb.QueryNodeClient {
		return &mock.GrpcQueryNodeClient{Err: nil}
	}
	client.(*Client).grpcClient.SetNewGrpcClientFunc(newFunc3)

	checkFunc(true)

	// ctx canceled
	client.(*Client).grpcClient = &mock.GRPCClientBase[querypb.QueryNodeClient]{
		GetGrpcClientErr: nil,
	}
	client.(*Client).grpcClient.SetNewGrpcClientFunc(newFunc1)
	cancel() // make context canceled
	checkFunc(false)

	err = client.Close()
	assert.NoError(t, err)
}

func Test_InternalTLS(t *testing.T) {
	paramtable.Init()
	validPath := "../../../../configs/cert1/ca.pem"
	ctx := context.Background()
	client, err := NewClient(ctx, "test", 1)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	mockQN := mocks.NewMockQueryNodeClient(t)
	mockGrpcClient := mocks.NewMockGrpcClient[querypb.QueryNodeClient](t)

	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().GetNodeID().Return(1)
	mockGrpcClient.EXPECT().ReCall(testify_mock.Anything, testify_mock.Anything).RunAndReturn(func(ctx context.Context, f func(querypb.QueryNodeClient) (interface{}, error)) (interface{}, error) {
		return f(mockQN)
	})

	t.Run("NoCertPool", func(t *testing.T) {
		var ErrNoCertPool = errors.New("no cert pool")
		mockGrpcClient.EXPECT().SetInternalTLSCertPool(testify_mock.Anything).Return().Once()
		client.(*Client).grpcClient = mockGrpcClient
		client.(*Client).grpcClient.GetNodeID()
		client.(*Client).grpcClient.SetInternalTLSCertPool(nil)

		mockQN.EXPECT().GetComponentStates(testify_mock.Anything, testify_mock.Anything).Return(nil, ErrNoCertPool)

		_, err := client.GetComponentStates(ctx, nil)
		assert.Error(t, err)
		assert.Equal(t, ErrNoCertPool, err)
	})

	// Sub-test for invalid certificate path
	t.Run("InvalidCertPath", func(t *testing.T) {
		invalidCAPath := "invalid/path/to/ca.pem"
		cp, err := utils.CreateCertPoolforClient(invalidCAPath, "querynode")
		assert.NotNil(t, err)
		assert.Nil(t, cp)
	})

	// Sub-test for TLS handshake failure
	t.Run("TlsHandshakeFailed", func(t *testing.T) {
		cp, err := utils.CreateCertPoolforClient(validPath, "querynode")
		assert.Nil(t, err)
		mockQN.ExpectedCalls = nil

		mockGrpcClient.EXPECT().SetInternalTLSCertPool(cp).Return().Once()
		mockGrpcClient.EXPECT().GetNodeID().Return(1)
		mockQN.EXPECT().GetComponentStates(testify_mock.Anything, testify_mock.Anything).Return(nil, errors.New("TLS handshake failed"))

		client.(*Client).grpcClient.GetNodeID()
		client.(*Client).grpcClient.SetInternalTLSCertPool(cp)

		_, err = client.GetComponentStates(ctx, nil)
		assert.NotNil(t, err)
		assert.EqualError(t, err, "TLS handshake failed")
	})

	t.Run("TlsHandshakeSuccess", func(t *testing.T) {
		cp, err := utils.CreateCertPoolforClient(validPath, "querynode")
		assert.Nil(t, err)
		mockQN.ExpectedCalls = nil

		mockGrpcClient.EXPECT().SetInternalTLSCertPool(cp).Return().Once()
		mockGrpcClient.EXPECT().GetNodeID().Return(1)
		mockQN.EXPECT().GetComponentStates(testify_mock.Anything, testify_mock.Anything).Return(&milvuspb.ComponentStates{}, nil)

		client.(*Client).grpcClient.GetNodeID()
		client.(*Client).grpcClient.SetInternalTLSCertPool(cp)

		componentStates, err := client.GetComponentStates(ctx, nil)
		assert.Nil(t, err)
		assert.NotNil(t, componentStates)
		assert.IsType(t, &milvuspb.ComponentStates{}, componentStates)
	})

	t.Run("ContextDeadlineExceeded", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
		defer cancel()
		time.Sleep(20 * time.Millisecond)

		_, err := client.GetComponentStates(ctx, nil)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	})
}
