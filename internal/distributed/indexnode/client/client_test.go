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

package grpcindexnodeclient

import (
	"context"
	"errors"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/utils"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	testify_mock "github.com/stretchr/testify/mock"
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
	client, err := NewClient(ctx, "", 1, false)
	assert.Nil(t, client)
	assert.Error(t, err)

	client, err = NewClient(ctx, "localhost:1234", 1, false)
	assert.NotNil(t, client)
	assert.NoError(t, err)

	err = client.Close()
	assert.NoError(t, err)
}

func TestIndexNodeClient(t *testing.T) {
	ctx := context.Background()
	client, err := NewClient(ctx, "localhost:1234", 1, false)
	assert.NoError(t, err)
	assert.NotNil(t, client)

	mockIN := mocks.NewMockIndexNodeClient(t)

	mockGrpcClient := mocks.NewMockGrpcClient[workerpb.IndexNodeClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().ReCall(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, f func(nodeClient workerpb.IndexNodeClient) (interface{}, error)) (interface{}, error) {
		return f(mockIN)
	})
	client.(*Client).grpcClient = mockGrpcClient

	t.Run("GetComponentStates", func(t *testing.T) {
		mockIN.EXPECT().GetComponentStates(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := client.GetComponentStates(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("GetStatisticsChannel", func(t *testing.T) {
		mockIN.EXPECT().GetStatisticsChannel(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := client.GetStatisticsChannel(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("CreatJob", func(t *testing.T) {
		mockIN.EXPECT().CreateJob(mock.Anything, mock.Anything).Return(nil, nil)

		req := &workerpb.CreateJobRequest{
			ClusterID: "0",
			BuildID:   0,
		}
		_, err := client.CreateJob(ctx, req)
		assert.NoError(t, err)
	})

	t.Run("QueryJob", func(t *testing.T) {
		mockIN.EXPECT().QueryJobs(mock.Anything, mock.Anything).Return(nil, nil)

		req := &workerpb.QueryJobsRequest{}
		_, err := client.QueryJobs(ctx, req)
		assert.NoError(t, err)
	})

	t.Run("DropJob", func(t *testing.T) {
		mockIN.EXPECT().DropJobs(mock.Anything, mock.Anything).Return(nil, nil)

		req := &workerpb.DropJobsRequest{}
		_, err := client.DropJobs(ctx, req)
		assert.NoError(t, err)
	})

	t.Run("ShowConfigurations", func(t *testing.T) {
		mockIN.EXPECT().ShowConfigurations(mock.Anything, mock.Anything).Return(nil, nil)

		req := &internalpb.ShowConfigurationsRequest{
			Pattern: "",
		}
		_, err := client.ShowConfigurations(ctx, req)
		assert.NoError(t, err)
	})

	t.Run("GetMetrics", func(t *testing.T) {
		mockIN.EXPECT().GetMetrics(mock.Anything, mock.Anything).Return(nil, nil)

		req, err := metricsinfo.ConstructRequestByMetricType(metricsinfo.SystemInfoMetrics)
		assert.NoError(t, err)
		_, err = client.GetMetrics(ctx, req)
		assert.NoError(t, err)
	})

	t.Run("GetJobStats", func(t *testing.T) {
		mockIN.EXPECT().GetJobStats(mock.Anything, mock.Anything).Return(nil, nil)

		req := &workerpb.GetJobStatsRequest{}
		_, err := client.GetJobStats(ctx, req)
		assert.NoError(t, err)
	})

	t.Run("CreateJobV2", func(t *testing.T) {
		mockIN.EXPECT().CreateJobV2(mock.Anything, mock.Anything).Return(nil, nil)

		req := &workerpb.CreateJobV2Request{}
		_, err := client.CreateJobV2(ctx, req)
		assert.NoError(t, err)
	})

	t.Run("QueryJobsV2", func(t *testing.T) {
		mockIN.EXPECT().QueryJobsV2(mock.Anything, mock.Anything).Return(nil, nil)

		req := &workerpb.QueryJobsV2Request{}
		_, err := client.QueryJobsV2(ctx, req)
		assert.NoError(t, err)
	})

	t.Run("DropJobsV2", func(t *testing.T) {
		mockIN.EXPECT().DropJobsV2(mock.Anything, mock.Anything).Return(nil, nil)

		req := &workerpb.DropJobsV2Request{}
		_, err := client.DropJobsV2(ctx, req)
		assert.NoError(t, err)
	})

	err = client.Close()
	assert.NoError(t, err)
}

func Test_InternalTLS_IndexNode(t *testing.T) {
	paramtable.Init()
	validPath := "../../../../configs/cert1/ca.pem"
	ctx := context.Background()
	client, err := NewClient(ctx, "test", 1, false)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	mockIndexNode := mocks.NewMockIndexNodeClient(t)
	mockGrpcClient := mocks.NewMockGrpcClient[indexpb.IndexNodeClient](t)

	mockGrpcClient.EXPECT().Close().Return(nil)
	mockGrpcClient.EXPECT().GetNodeID().Return(1)
	mockGrpcClient.EXPECT().ReCall(testify_mock.Anything, testify_mock.Anything).RunAndReturn(func(ctx context.Context, f func(indexpb.IndexNodeClient) (interface{}, error)) (interface{}, error) {
		return f(mockIndexNode)
	})

	t.Run("NoCertPool", func(t *testing.T) {
		var ErrNoCertPool = errors.New("no cert pool")
		mockGrpcClient.EXPECT().SetInternalTLSCertPool(testify_mock.Anything).Return().Once()
		client.(*Client).grpcClient = mockGrpcClient
		client.(*Client).grpcClient.GetNodeID()
		client.(*Client).grpcClient.SetInternalTLSCertPool(nil)

		mockIndexNode.EXPECT().GetComponentStates(testify_mock.Anything, testify_mock.Anything).Return(nil, ErrNoCertPool)

		_, err := client.GetComponentStates(ctx, nil)
		assert.Error(t, err)
		assert.Equal(t, ErrNoCertPool, err)
	})

	// Sub-test for invalid certificate path
	t.Run("InvalidCertPath", func(t *testing.T) {
		invalidCAPath := "invalid/path/to/ca.pem"
		cp, err := utils.CreateCertPoolforClient(invalidCAPath, "indexnode")
		assert.NotNil(t, err)
		assert.Nil(t, cp)
	})

	// Sub-test for TLS handshake failure
	t.Run("TlsHandshakeFailed", func(t *testing.T) {
		cp, err := utils.CreateCertPoolforClient(validPath, "indexnode")
		assert.Nil(t, err)
		mockIndexNode.ExpectedCalls = nil

		mockGrpcClient.EXPECT().SetInternalTLSCertPool(cp).Return().Once()
		mockGrpcClient.EXPECT().GetNodeID().Return(1)
		mockIndexNode.EXPECT().GetComponentStates(testify_mock.Anything, testify_mock.Anything).Return(nil, errors.New("TLS handshake failed"))

		client.(*Client).grpcClient.GetNodeID()
		client.(*Client).grpcClient.SetInternalTLSCertPool(cp)

		_, err = client.GetComponentStates(ctx, nil)
		assert.NotNil(t, err)
		assert.EqualError(t, err, "TLS handshake failed")
	})

	t.Run("TlsHandshakeSuccess", func(t *testing.T) {
		cp, err := utils.CreateCertPoolforClient(validPath, "indexnode")
		assert.Nil(t, err)
		mockIndexNode.ExpectedCalls = nil

		mockGrpcClient.EXPECT().SetInternalTLSCertPool(cp).Return().Once()
		mockGrpcClient.EXPECT().GetNodeID().Return(1)
		mockIndexNode.EXPECT().GetComponentStates(testify_mock.Anything, testify_mock.Anything).Return(&milvuspb.ComponentStates{}, nil)

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
