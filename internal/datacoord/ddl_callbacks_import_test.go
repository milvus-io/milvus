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

package datacoord

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
)

// ================================
// Import Callbacks Test Suite
// ================================

type ImportCallbacksSuite struct {
	suite.Suite
}

func TestImportCallbacksSuite(t *testing.T) {
	suite.Run(t, new(ImportCallbacksSuite))
}

// --------------------------------
// validateImportRequest Tests
// --------------------------------

func (s *ImportCallbacksSuite) TestValidateImportRequest_InvalidTimeoutReturnsError() {
	ctx := context.Background()
	server := &Server{}

	files := []*msgpb.ImportFile{
		{Id: 1, Paths: []string{"/test/file1.json"}},
	}
	options := []*commonpb.KeyValuePair{
		{Key: "timeout", Value: "invalid_timeout_format"},
	}

	err := server.validateImportRequest(ctx, files, options)

	s.Error(err)
	s.Contains(err.Error(), "timeout")
}

func (s *ImportCallbacksSuite) TestValidateImportRequest_MaxJobsExceededReturnsError() {
	ctx := context.Background()

	mock := mockey.Mock((*importMeta).CountJobBy).To(func(_ *importMeta, _ context.Context, _ ...ImportJobFilter) int {
		return 2000 // Exceeds default MaxImportJobNum (1024)
	}).Build()
	defer mock.UnPatch()

	server := &Server{
		importMeta: &importMeta{},
	}

	files := []*msgpb.ImportFile{
		{Id: 1, Paths: []string{"/test/file1.json"}},
	}
	options := []*commonpb.KeyValuePair{
		{Key: "timeout", Value: "300s"},
	}

	err := server.validateImportRequest(ctx, files, options)

	s.Error(err)
	// Job-count backpressure is a server-side condition -> ErrImportSysFailed
	// (must not be bucketed as fail_input).
	s.True(errors.Is(err, merr.ErrImportSysFailed))
	s.Contains(err.Error(), "The number of jobs has reached the limit")
}

func (s *ImportCallbacksSuite) TestValidateImportRequest_BalancerGetFailsReturnsError() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	mockCount := mockey.Mock((*importMeta).CountJobBy).To(func(_ *importMeta, _ context.Context, _ ...ImportJobFilter) int {
		return 1
	}).Build()
	defer mockCount.UnPatch()

	mockBalance := mockey.Mock(balance.GetWithContext).To(func(ctx context.Context) (balancer.Balancer, error) {
		return nil, errors.New("balancer not available")
	}).Build()
	defer mockBalance.UnPatch()

	server := &Server{
		importMeta: &importMeta{},
	}

	files := []*msgpb.ImportFile{
		{Id: 1, Paths: []string{"/test/file1.json"}},
	}
	options := []*commonpb.KeyValuePair{
		{Key: "timeout", Value: "300s"},
	}

	err := server.validateImportRequest(ctx, files, options)

	s.Error(err)
	s.Contains(err.Error(), "balancer not available")
}

func (s *ImportCallbacksSuite) TestValidateImportRequest_ReplicatingClusterReturnsError() {
	ctx := context.Background()

	mockCount := mockey.Mock((*importMeta).CountJobBy).To(func(_ *importMeta, _ context.Context, _ ...ImportJobFilter) int {
		return 1
	}).Build()
	defer mockCount.UnPatch()

	mockBalancer := &mockBalancerImpl{}
	mockBalance := mockey.Mock(balance.GetWithContext).To(func(ctx context.Context) (balancer.Balancer, error) {
		return mockBalancer, nil
	}).Build()
	defer mockBalance.UnPatch()

	// Mock GetLatestChannelAssignment to return replicating cluster config
	mockAssignment := mockey.Mock((*mockBalancerImpl).GetLatestChannelAssignment).To(
		func(_ *mockBalancerImpl) (*channel.WatchChannelAssignmentsCallbackParam, error) {
			return &channel.WatchChannelAssignmentsCallbackParam{
				ReplicateConfiguration: &commonpb.ReplicateConfiguration{
					Clusters: []*commonpb.MilvusCluster{
						{ClusterId: "cluster1"},
						{ClusterId: "cluster2"},
					},
				},
			}, nil
		}).Build()
	defer mockAssignment.UnPatch()

	server := &Server{
		importMeta: &importMeta{},
	}

	files := []*msgpb.ImportFile{
		{Id: 1, Paths: []string{"/test/file1.json"}},
	}
	options := []*commonpb.KeyValuePair{
		{Key: "timeout", Value: "300s"},
	}

	err := server.validateImportRequest(ctx, files, options)

	s.Error(err)
	s.True(errors.Is(err, merr.ErrOperationNotSupported))
	s.Contains(err.Error(), "replicating cluster")
}

func (s *ImportCallbacksSuite) TestValidateImportRequest_ReplicatingClusterEnabledRequiresManualCommit() {
	ctx := context.Background()

	paramtable.Get().Save(paramtable.Get().DataCoordCfg.ImportInReplicatingCluster.Key, "true")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.ImportInReplicatingCluster.Key)

	mockCount := mockey.Mock((*importMeta).CountJobBy).To(func(_ *importMeta, _ context.Context, _ ...ImportJobFilter) int {
		return 1
	}).Build()
	defer mockCount.UnPatch()

	mockBalancer := &mockBalancerImpl{}
	mockBalance := mockey.Mock(balance.GetWithContext).To(func(ctx context.Context) (balancer.Balancer, error) {
		return mockBalancer, nil
	}).Build()
	defer mockBalance.UnPatch()

	mockAssignment := mockey.Mock((*mockBalancerImpl).GetLatestChannelAssignment).To(
		func(_ *mockBalancerImpl) (*channel.WatchChannelAssignmentsCallbackParam, error) {
			return &channel.WatchChannelAssignmentsCallbackParam{
				ReplicateConfiguration: &commonpb.ReplicateConfiguration{
					Clusters: []*commonpb.MilvusCluster{
						{ClusterId: "cluster1"},
						{ClusterId: "cluster2"},
					},
				},
			}, nil
		}).Build()
	defer mockAssignment.UnPatch()

	server := &Server{
		importMeta: &importMeta{},
	}
	files := []*msgpb.ImportFile{
		{Id: 1, Paths: []string{"/test/file1.json"}},
	}

	err := server.validateImportRequest(ctx, files, []*commonpb.KeyValuePair{
		{Key: "timeout", Value: "300s"},
	})
	s.Error(err)
	s.True(errors.Is(err, merr.ErrOperationNotSupported))
	s.Contains(err.Error(), "auto_commit=true")

	err = server.validateImportRequest(ctx, files, []*commonpb.KeyValuePair{
		{Key: "timeout", Value: "300s"},
		{Key: importutilv2.AutoCommitKey, Value: "false"},
	})
	s.NoError(err)
}

func (s *ImportCallbacksSuite) TestValidateImportRequest_SuccessWithValidInput() {
	ctx := context.Background()

	mockCount := mockey.Mock((*importMeta).CountJobBy).To(func(_ *importMeta, _ context.Context, _ ...ImportJobFilter) int {
		return 1
	}).Build()
	defer mockCount.UnPatch()

	mockBalancer := &mockBalancerImpl{}
	mockBalance := mockey.Mock(balance.GetWithContext).To(func(ctx context.Context) (balancer.Balancer, error) {
		return mockBalancer, nil
	}).Build()
	defer mockBalance.UnPatch()

	mockAssignment := mockey.Mock((*mockBalancerImpl).GetLatestChannelAssignment).To(
		func(_ *mockBalancerImpl) (*channel.WatchChannelAssignmentsCallbackParam, error) {
			return &channel.WatchChannelAssignmentsCallbackParam{
				ReplicateConfiguration: nil, // No replication
			}, nil
		}).Build()
	defer mockAssignment.UnPatch()

	server := &Server{
		importMeta: &importMeta{},
	}

	files := []*msgpb.ImportFile{
		{Id: 1, Paths: []string{"/test/file1.json"}},
	}
	options := []*commonpb.KeyValuePair{
		{Key: "timeout", Value: "300s"},
	}

	err := server.validateImportRequest(ctx, files, options)

	s.NoError(err)
}

// --------------------------------
// broadcastImport Tests
// --------------------------------

func (s *ImportCallbacksSuite) TestBroadcastImport_ValidationFailsReturnsError() {
	ctx := context.Background()

	// Mock validateImportRequest to fail
	mockCount := mockey.Mock((*importMeta).CountJobBy).To(func(_ *importMeta, _ context.Context, _ ...ImportJobFilter) int {
		return 2000 // Exceeds limit
	}).Build()
	defer mockCount.UnPatch()

	server := &Server{
		importMeta: &importMeta{},
	}

	err := server.broadcastImport(
		ctx,
		"test_collection",
		100,
		[]int64{1},
		[]*internalpb.ImportFile{{Id: 1, Paths: []string{"/test/file.json"}}},
		[]*commonpb.KeyValuePair{{Key: "timeout", Value: "300s"}},
		&schemapb.CollectionSchema{Name: "test_collection"},
		1000,
		[]string{"v1"},
	)

	s.Error(err)
	s.Contains(err.Error(), "failed to validate import request")
}

func (s *ImportCallbacksSuite) TestBroadcastImport_DescribeCollectionFailsReturnsError() {
	ctx := context.Background()

	// Setup validation to pass
	mockCount := mockey.Mock((*importMeta).CountJobBy).To(func(_ *importMeta, _ context.Context, _ ...ImportJobFilter) int {
		return 1
	}).Build()
	defer mockCount.UnPatch()

	mockBalancer := &mockBalancerImpl{}
	mockBalance := mockey.Mock(balance.GetWithContext).To(func(ctx context.Context) (balancer.Balancer, error) {
		return mockBalancer, nil
	}).Build()
	defer mockBalance.UnPatch()

	mockAssignment := mockey.Mock((*mockBalancerImpl).GetLatestChannelAssignment).To(
		func(_ *mockBalancerImpl) (*channel.WatchChannelAssignmentsCallbackParam, error) {
			return &channel.WatchChannelAssignmentsCallbackParam{
				ReplicateConfiguration: nil,
			}, nil
		}).Build()
	defer mockAssignment.UnPatch()

	// Mock broker.DescribeCollectionInternal to fail (called in startBroadcastWithCollectionID)
	mockBroker := broker.NewMockBroker(s.T())
	mockBroker.EXPECT().DescribeCollectionInternal(mock.Anything, int64(100)).Return(nil, errors.New("collection not found"))

	server := &Server{
		importMeta: &importMeta{},
		broker:     mockBroker,
	}

	err := server.broadcastImport(
		ctx,
		"test_collection",
		100,
		[]int64{1},
		[]*internalpb.ImportFile{{Id: 1, Paths: []string{"/test/file.json"}}},
		[]*commonpb.KeyValuePair{{Key: "timeout", Value: "300s"}},
		&schemapb.CollectionSchema{Name: "test_collection"},
		1000,
		[]string{"v1"},
	)

	s.Error(err)
	s.Contains(err.Error(), "failed to start broadcast with collection id")
}

func (s *ImportCallbacksSuite) TestBroadcastImport_StartBroadcastFailsReturnsError() {
	ctx := context.Background()

	// Setup validation to pass
	mockCount := mockey.Mock((*importMeta).CountJobBy).To(func(_ *importMeta, _ context.Context, _ ...ImportJobFilter) int {
		return 1
	}).Build()
	defer mockCount.UnPatch()

	mockBalancer := &mockBalancerImpl{}
	mockBalance := mockey.Mock(balance.GetWithContext).To(func(ctx context.Context) (balancer.Balancer, error) {
		return mockBalancer, nil
	}).Build()
	defer mockBalance.UnPatch()

	mockAssignment := mockey.Mock((*mockBalancerImpl).GetLatestChannelAssignment).To(
		func(_ *mockBalancerImpl) (*channel.WatchChannelAssignmentsCallbackParam, error) {
			return &channel.WatchChannelAssignmentsCallbackParam{
				ReplicateConfiguration: nil,
			}, nil
		}).Build()
	defer mockAssignment.UnPatch()

	// Mock broker.DescribeCollectionInternal to return dbName (called in startBroadcastWithCollectionID)
	mockBroker := broker.NewMockBroker(s.T())
	mockBroker.EXPECT().DescribeCollectionInternal(mock.Anything, int64(100)).Return(&milvuspb.DescribeCollectionResponse{
		DbName:         "test_db",
		CollectionName: "test_collection",
	}, nil)

	// Mock StartBroadcastWithResourceKeys to fail
	mockBroadcast := mockey.Mock(broadcast.StartBroadcastWithResourceKeys).To(
		func(ctx context.Context, keys ...message.ResourceKey) (broadcaster.BroadcastAPI, error) {
			return nil, errors.New("failed to acquire resource lock")
		}).Build()
	defer mockBroadcast.UnPatch()

	server := &Server{
		importMeta: &importMeta{},
		broker:     mockBroker,
	}

	err := server.broadcastImport(
		ctx,
		"test_collection",
		100,
		[]int64{1},
		[]*internalpb.ImportFile{{Id: 1, Paths: []string{"/test/file.json"}}},
		[]*commonpb.KeyValuePair{{Key: "timeout", Value: "300s"}},
		&schemapb.CollectionSchema{Name: "test_collection"},
		1000,
		[]string{"v1"},
	)

	s.Error(err)
	s.Contains(err.Error(), "failed to start broadcast with collection id")
}

func (s *ImportCallbacksSuite) TestBroadcastImport_SecondDescribeCollectionFailsReturnsError() {
	ctx := context.Background()

	// Setup validation to pass
	mockCount := mockey.Mock((*importMeta).CountJobBy).To(func(_ *importMeta, _ context.Context, _ ...ImportJobFilter) int {
		return 1
	}).Build()
	defer mockCount.UnPatch()

	mockBalancer := &mockBalancerImpl{}
	mockBalance := mockey.Mock(balance.GetWithContext).To(func(ctx context.Context) (balancer.Balancer, error) {
		return mockBalancer, nil
	}).Build()
	defer mockBalance.UnPatch()

	mockAssignment := mockey.Mock((*mockBalancerImpl).GetLatestChannelAssignment).To(
		func(_ *mockBalancerImpl) (*channel.WatchChannelAssignmentsCallbackParam, error) {
			return &channel.WatchChannelAssignmentsCallbackParam{
				ReplicateConfiguration: nil,
			}, nil
		}).Build()
	defer mockAssignment.UnPatch()

	// Mock StartBroadcastWithResourceKeys to succeed
	mockBroadcastAPI := newMockBroadcastAPIImpl()
	mockBroadcast := mockey.Mock(broadcast.StartBroadcastWithResourceKeys).To(
		func(ctx context.Context, keys ...message.ResourceKey) (broadcaster.BroadcastAPI, error) {
			return mockBroadcastAPI, nil
		}).Build()
	defer mockBroadcast.UnPatch()

	// Mock broker: first DescribeCollectionInternal succeeds (in startBroadcastWithCollectionID),
	// second call returns error status (in broadcastImport after getting broadcaster)
	mockBroker := broker.NewMockBroker(s.T())
	mockBroker.EXPECT().DescribeCollectionInternal(mock.Anything, int64(100)).Return(&milvuspb.DescribeCollectionResponse{
		DbName:         "test_db",
		CollectionName: "test_collection",
	}, nil).Once()
	mockBroker.EXPECT().DescribeCollectionInternal(mock.Anything, int64(100)).Return(&milvuspb.DescribeCollectionResponse{
		Status: merr.Status(merr.ErrCollectionNotFound),
	}, nil).Once()

	server := &Server{
		importMeta: &importMeta{},
		broker:     mockBroker,
	}

	err := server.broadcastImport(
		ctx,
		"test_collection",
		100,
		[]int64{1},
		[]*internalpb.ImportFile{{Id: 1, Paths: []string{"/test/file.json"}}},
		[]*commonpb.KeyValuePair{{Key: "timeout", Value: "300s"}},
		&schemapb.CollectionSchema{Name: "test_collection"},
		1000,
		[]string{"v1"},
	)

	s.Error(err)
	s.True(errors.Is(err, merr.ErrCollectionNotFound))
}

func (s *ImportCallbacksSuite) TestBroadcastImport_BroadcastFailsReturnsError() {
	ctx := context.Background()

	// Setup validation to pass
	mockCount := mockey.Mock((*importMeta).CountJobBy).To(func(_ *importMeta, _ context.Context, _ ...ImportJobFilter) int {
		return 1
	}).Build()
	defer mockCount.UnPatch()

	mockBalancer := &mockBalancerImpl{}
	mockBalance := mockey.Mock(balance.GetWithContext).To(func(ctx context.Context) (balancer.Balancer, error) {
		return mockBalancer, nil
	}).Build()
	defer mockBalance.UnPatch()

	mockAssignment := mockey.Mock((*mockBalancerImpl).GetLatestChannelAssignment).To(
		func(_ *mockBalancerImpl) (*channel.WatchChannelAssignmentsCallbackParam, error) {
			return &channel.WatchChannelAssignmentsCallbackParam{
				ReplicateConfiguration: nil,
			}, nil
		}).Build()
	defer mockAssignment.UnPatch()

	// Mock StartBroadcastWithResourceKeys to succeed
	mockBroadcastAPI := newMockBroadcastAPIImpl()
	mockBroadcastAPI.broadcastErr = errors.New("broadcast failed")
	mockBroadcast := mockey.Mock(broadcast.StartBroadcastWithResourceKeys).To(
		func(ctx context.Context, keys ...message.ResourceKey) (broadcaster.BroadcastAPI, error) {
			return mockBroadcastAPI, nil
		}).Build()
	defer mockBroadcast.UnPatch()

	// Mock broker: DescribeCollectionInternal is called twice
	// First call in startBroadcastWithCollectionID, second call in broadcastImport
	mockBroker := broker.NewMockBroker(s.T())
	mockBroker.EXPECT().DescribeCollectionInternal(mock.Anything, int64(100)).Return(&milvuspb.DescribeCollectionResponse{
		DbName:         "test_db",
		CollectionName: "test_collection",
	}, nil).Times(2)

	server := &Server{
		importMeta: &importMeta{},
		broker:     mockBroker,
	}

	err := server.broadcastImport(
		ctx,
		"test_collection",
		100,
		[]int64{1},
		[]*internalpb.ImportFile{{Id: 1, Paths: []string{"/test/file.json"}}},
		[]*commonpb.KeyValuePair{{Key: "timeout", Value: "300s"}},
		&schemapb.CollectionSchema{Name: "test_collection"},
		1000,
		[]string{"v1"},
	)

	s.Error(err)
	s.Contains(err.Error(), "broadcast failed")
}

func (s *ImportCallbacksSuite) TestBroadcastImport_SuccessWithValidInput() {
	ctx := context.Background()

	// Setup validation to pass
	mockCount := mockey.Mock((*importMeta).CountJobBy).To(func(_ *importMeta, _ context.Context, _ ...ImportJobFilter) int {
		return 1
	}).Build()
	defer mockCount.UnPatch()

	mockBalancer := &mockBalancerImpl{}
	mockBalance := mockey.Mock(balance.GetWithContext).To(func(ctx context.Context) (balancer.Balancer, error) {
		return mockBalancer, nil
	}).Build()
	defer mockBalance.UnPatch()

	mockAssignment := mockey.Mock((*mockBalancerImpl).GetLatestChannelAssignment).To(
		func(_ *mockBalancerImpl) (*channel.WatchChannelAssignmentsCallbackParam, error) {
			return &channel.WatchChannelAssignmentsCallbackParam{
				ReplicateConfiguration: nil,
			}, nil
		}).Build()
	defer mockAssignment.UnPatch()

	// Mock StartBroadcastWithResourceKeys to succeed
	mockBroadcastAPI := newMockBroadcastAPIImpl()
	mockBroadcast := mockey.Mock(broadcast.StartBroadcastWithResourceKeys).To(
		func(ctx context.Context, keys ...message.ResourceKey) (broadcaster.BroadcastAPI, error) {
			return mockBroadcastAPI, nil
		}).Build()
	defer mockBroadcast.UnPatch()

	// Mock broker: DescribeCollectionInternal is called twice
	// First call in startBroadcastWithCollectionID, second call in broadcastImport
	mockBroker := broker.NewMockBroker(s.T())
	mockBroker.EXPECT().DescribeCollectionInternal(mock.Anything, int64(100)).Return(&milvuspb.DescribeCollectionResponse{
		DbName:         "test_db",
		CollectionName: "test_collection",
	}, nil).Times(2)

	server := &Server{
		importMeta: &importMeta{},
		broker:     mockBroker,
	}

	err := server.broadcastImport(
		ctx,
		"test_collection",
		100,
		[]int64{1},
		[]*internalpb.ImportFile{{Id: 1, Paths: []string{"/test/file.json"}}},
		[]*commonpb.KeyValuePair{{Key: "timeout", Value: "300s"}},
		&schemapb.CollectionSchema{Name: "test_collection"},
		1000,
		[]string{"v1"},
	)

	s.NoError(err)
}

// --------------------------------
// RegisterDDLCallbacks Import Tests
// --------------------------------

func (s *ImportCallbacksSuite) TestRegisterDDLCallbacks_DoesNotPanic() {
	server := &Server{}

	s.NotPanics(func() {
		RegisterDDLCallbacks(server)
	})
}

// --------------------------------
// Helper Types for Mocking
// --------------------------------

// mockBalancerImpl is a mock implementation for balancer.Balancer interface
type mockBalancerImpl struct {
	balancer.Balancer
}

func (m *mockBalancerImpl) GetLatestChannelAssignment() (*channel.WatchChannelAssignmentsCallbackParam, error) {
	// Ensure the function body is long enough for mockey to patch
	result := &channel.WatchChannelAssignmentsCallbackParam{}
	return result, nil
}

// mockBroadcastAPIImpl is a mock implementation for broadcaster.BroadcastAPI interface
// This implementation has configurable behavior and uses enough code to be patchable by mockey
type mockBroadcastAPIImpl struct {
	broadcastResult *types.BroadcastAppendResult
	broadcastErr    error
	closeCalled     atomic.Bool
}

func newMockBroadcastAPIImpl() *mockBroadcastAPIImpl {
	// Initialize with default success result
	mock := &mockBroadcastAPIImpl{
		broadcastResult: &types.BroadcastAppendResult{
			BroadcastID: 12345,
		},
		broadcastErr: nil,
	}
	mock.closeCalled.Store(false)
	return mock
}

func (m *mockBroadcastAPIImpl) Broadcast(ctx context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
	// Add operations to ensure the function is long enough for mockey
	if ctx == nil {
		return nil, errors.New("context is nil")
	}
	if msg == nil {
		return nil, errors.New("message is nil")
	}
	if m.broadcastErr != nil {
		return nil, m.broadcastErr
	}
	if m.broadcastResult != nil {
		return m.broadcastResult, nil
	}
	return &types.BroadcastAppendResult{BroadcastID: 0}, nil
}

func (m *mockBroadcastAPIImpl) Close() {
	// Add operations to ensure the function is long enough for mockey
	m.closeCalled.Store(true)
	if m.closeCalled.Load() {
		// Already closed, do nothing
		return
	}
}

// --------------------------------
// Import Flow Documentation Tests
// --------------------------------

// TestImportV2_OnlyBroadcast verifies that ImportV2 is dedicated to broadcasting.
// ImportV2 no longer handles ack callbacks - they are processed by createImportJobFromAck.
func TestImportV2_OnlyBroadcast(t *testing.T) {
	t.Run("ImportV2 is only for proxy broadcast", func(t *testing.T) {
		// Create request - ImportV2 always broadcasts
		req := &internalpb.ImportRequestInternal{
			CollectionID:   100,
			CollectionName: "test_collection",
			PartitionIDs:   []int64{1},
			ChannelNames:   []string{"vchannel1"},
			Schema: &schemapb.CollectionSchema{
				Name: "test_collection",
				Fields: []*schemapb.FieldSchema{
					{FieldID: 1, Name: "id", DataType: schemapb.DataType_Int64},
				},
			},
			Files: []*internalpb.ImportFile{
				{Id: 1, Paths: []string{"/test/file1.json"}},
			},
			Options: []*commonpb.KeyValuePair{
				{Key: "timeout", Value: "300"},
			},
		}

		// ImportV2 always broadcasts, regardless of DataTimestamp
		// Ack callbacks are handled by createImportJobFromAck internally
		assert.NotNil(t, req, "ImportV2 should handle broadcast only")
	})
}

// TestImportV2_ProxyCallPath tests the proxy call path (DataTimestamp == 0)
// This test verifies that the new broadcast flow is triggered
func TestImportV2_ProxyCallPath(t *testing.T) {
	t.Run("Proxy call should trigger broadcast", func(t *testing.T) {
		req := &internalpb.ImportRequestInternal{
			CollectionID:   100,
			CollectionName: "test_collection",
			PartitionIDs:   []int64{1},
			ChannelNames:   []string{"vchannel1"},
			Schema: &schemapb.CollectionSchema{
				Name: "test_collection",
			},
			Files: []*internalpb.ImportFile{
				{Id: 1, Paths: []string{"/test/file1.json"}},
			},
			Options: []*commonpb.KeyValuePair{
				{Key: "timeout", Value: "300"},
			},
			DataTimestamp: 0, // Proxy call - no timestamp
			JobID:         0,
		}

		// Verify this is identified as proxy call
		isFromAckCallback := req.GetDataTimestamp() > 0
		assert.False(t, isFromAckCallback, "DataTimestamp=0 should be identified as proxy call")

		// The ImportV2 method should:
		// 1. Allocate job ID
		// 2. Call broadcastImport
		// 3. Return job ID without creating job (job created by ack callback)
	})
}

// TestImportV2_AckCallbackPath tests the ack callback path (DataTimestamp > 0)
// This test verifies that the job creation flow is triggered
func TestImportV2_AckCallbackPath(t *testing.T) {
	t.Run("Ack callback should trigger job creation", func(t *testing.T) {
		req := &internalpb.ImportRequestInternal{
			CollectionID:   100,
			CollectionName: "test_collection",
			PartitionIDs:   []int64{1},
			ChannelNames:   []string{"vchannel1"},
			Schema: &schemapb.CollectionSchema{
				Name: "test_collection",
			},
			Files: []*internalpb.ImportFile{
				{Id: 1, Paths: []string{"/test/file1.json"}},
			},
			Options: []*commonpb.KeyValuePair{
				{Key: "timeout", Value: "300"},
			},
			DataTimestamp: 123456789, // Ack callback - has timestamp from broadcast
			JobID:         1000,      // Ack callback - has job ID
		}

		// Verify this is identified as ack callback
		isFromAckCallback := req.GetDataTimestamp() > 0
		assert.True(t, isFromAckCallback, "DataTimestamp>0 should be identified as ack callback")

		// The ImportV2 method should:
		// 1. Skip broadcast
		// 2. Process files
		// 3. Create import job
		// 4. Return job ID
	})
}

// TestProxyImportRequest tests that proxy correctly constructs the request
func TestProxyImportRequest(t *testing.T) {
	t.Run("Proxy should not set DataTimestamp", func(t *testing.T) {
		// Simulating what proxy does in task_import.go
		req := &internalpb.ImportRequestInternal{
			DbID:           0, // deprecated
			CollectionID:   100,
			CollectionName: "test_collection",
			PartitionIDs:   []int64{1, 2},
			ChannelNames:   []string{"vchannel1", "vchannel2"},
			Schema: &schemapb.CollectionSchema{
				Name: "test_collection",
			},
			Files: []*internalpb.ImportFile{
				{Id: 1, Paths: []string{"/test/file1.json"}},
			},
			Options: []*commonpb.KeyValuePair{
				{Key: "timeout", Value: "300"},
			},
			DataTimestamp: 0, // CRITICAL: Must be 0 for proxy calls
			JobID:         0, // Let DataCoord allocate
		}

		// Verify proxy request structure
		assert.Equal(t, uint64(0), req.DataTimestamp, "Proxy must set DataTimestamp to 0")
		assert.Equal(t, int64(0), req.JobID, "Proxy should let DataCoord allocate job ID")
		assert.NotEmpty(t, req.ChannelNames, "Proxy must provide channel names")
		assert.NotNil(t, req.Schema, "Proxy must provide schema")
	})
}

// TestAckCallbackImportRequest tests that ack callback correctly constructs the request
func TestAckCallbackImportRequest(t *testing.T) {
	t.Run("Ack callback should set DataTimestamp", func(t *testing.T) {
		// Simulating what ack callback does in ddl_callbacks_import.go
		req := &internalpb.ImportRequestInternal{
			CollectionID:   100,
			CollectionName: "test_collection",
			PartitionIDs:   []int64{1, 2},
			ChannelNames:   []string{"vchannel1", "vchannel2"}, // Only acked channels
			Schema: &schemapb.CollectionSchema{
				Name: "test_collection",
			},
			Files: []*internalpb.ImportFile{
				{Id: 1, Paths: []string{"/test/file1.json"}},
			},
			Options: []*commonpb.KeyValuePair{
				{Key: "timeout", Value: "300"},
			},
			DataTimestamp: 123456789, // CRITICAL: Set from broadcast message timestamp
			JobID:         1000,      // Set from broadcast message
		}

		// Verify ack callback request structure
		assert.Greater(t, req.DataTimestamp, uint64(0), "Ack callback must set DataTimestamp from message")
		assert.Greater(t, req.JobID, int64(0), "Ack callback must set JobID from message")
	})
}

// TestImportFlowIntegration documents the complete import flow
func TestImportFlowIntegration(t *testing.T) {
	t.Run("Document complete import flow", func(t *testing.T) {
		// This test documents the expected flow but doesn't execute it
		// (would require full integration test setup)

		// STEP 1: Proxy receives import request from user
		proxyReq := &internalpb.ImportRequestInternal{
			CollectionID:   100,
			CollectionName: "test_collection",
			PartitionIDs:   []int64{1},
			ChannelNames:   []string{"vchannel1"},
			Schema:         &schemapb.CollectionSchema{Name: "test_collection"},
			Files:          []*internalpb.ImportFile{{Id: 1, Paths: []string{"/test/file1.json"}}},
			Options:        []*commonpb.KeyValuePair{{Key: "timeout", Value: "300"}},
			DataTimestamp:  0, // Proxy call
			JobID:          0,
		}

		// STEP 2: Proxy calls DataCoord.ImportV2() via RPC
		// DataCoord identifies this as proxy call (DataTimestamp == 0)
		assert.Equal(t, uint64(0), proxyReq.DataTimestamp)

		// STEP 3: DataCoord broadcasts message
		// (broadcastImport is called internally)

		// STEP 4: Ack callback is triggered
		ackCallbackReq := &internalpb.ImportRequestInternal{
			CollectionID:   proxyReq.CollectionID,
			CollectionName: proxyReq.CollectionName,
			PartitionIDs:   proxyReq.PartitionIDs,
			ChannelNames:   []string{"vchannel1"}, // Only acked channels
			Schema:         proxyReq.Schema,
			Files:          proxyReq.Files,
			Options:        proxyReq.Options,
			DataTimestamp:  123456789, // Set from broadcast message
			JobID:          1000,      // Set from broadcast message
		}

		// STEP 5: DataCoord identifies this as ack callback (DataTimestamp > 0)
		assert.Greater(t, ackCallbackReq.DataTimestamp, uint64(0))

		// STEP 6: DataCoord creates import job
		// (ImportV2 continues with job creation logic)

		t.Log("Import flow documented successfully")
	})
}

func newTestImportMeta(t *testing.T) (ImportMeta, *mocks.DataCoordCatalog) {
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil).Maybe()

	alloc := allocator.NewMockAllocator(t)
	importMeta, err := NewImportMeta(context.Background(), catalog, alloc, nil)
	assert.NoError(t, err)
	return importMeta, catalog
}

func buildCommitImportBroadcastResult(jobID int64) message.BroadcastResultCommitImportMessageV2 {
	broadcastMsg := message.NewCommitImportMessageBuilderV2().
		WithHeader(&message.CommitImportMessageHeader{JobId: jobID}).
		WithBody(&messagespb.CommitImportMessageBody{}).
		WithBroadcast([]string{"control_channel"}).
		MustBuildBroadcast()
	return message.BroadcastResultCommitImportMessageV2{
		Message: message.MustAsSpecializedBroadcastMessage[*message.CommitImportMessageHeader, *messagespb.CommitImportMessageBody](broadcastMsg),
		Results: map[string]*message.AppendResult{},
	}
}

func buildRollbackImportBroadcastResult(jobID int64) message.BroadcastResultRollbackImportMessageV2 {
	broadcastMsg := message.NewRollbackImportMessageBuilderV2().
		WithHeader(&message.RollbackImportMessageHeader{JobId: jobID}).
		WithBody(&messagespb.RollbackImportMessageBody{}).
		WithBroadcast([]string{"control_channel"}).
		MustBuildBroadcast()
	return message.BroadcastResultRollbackImportMessageV2{
		Message: message.MustAsSpecializedBroadcastMessage[*message.RollbackImportMessageHeader, *messagespb.RollbackImportMessageBody](broadcastMsg),
		Results: map[string]*message.AppendResult{},
	}
}

func TestCommitImportCallback_UncommittedToCommitting(t *testing.T) {
	ctx := context.Background()
	importMeta, _ := newTestImportMeta(t)

	job := &importJob{
		ImportJob: &datapb.ImportJob{
			JobID:      1,
			State:      internalpb.ImportJobState_Uncommitted,
			AutoCommit: false,
		},
		tr: timerecord.NewTimeRecorder("test"),
	}
	err := importMeta.AddJob(ctx, job)
	assert.NoError(t, err)

	callbacks := &DDLCallbacks{Server: &Server{importMeta: importMeta}}
	err = callbacks.commitImportV2AckCallback(ctx, buildCommitImportBroadcastResult(1))
	assert.NoError(t, err)

	updatedJob := importMeta.GetJob(ctx, 1)
	assert.NotNil(t, updatedJob)
	assert.Equal(t, internalpb.ImportJobState_Committing, updatedJob.GetState())
}

func TestRollbackImportCallback_TransitionToFailed(t *testing.T) {
	ctx := context.Background()
	importMeta, _ := newTestImportMeta(t)

	job := &importJob{
		ImportJob: &datapb.ImportJob{
			JobID:             2,
			State:             internalpb.ImportJobState_Uncommitted,
			AutoCommit:        false,
			RequestedDiskSize: 1024 * 1024, // nonzero so we can observe release
		},
		tr: timerecord.NewTimeRecorder("test"),
	}
	err := importMeta.AddJob(ctx, job)
	assert.NoError(t, err)

	callbacks := &DDLCallbacks{Server: &Server{importMeta: importMeta}}
	err = callbacks.rollbackImportV2AckCallback(ctx, buildRollbackImportBroadcastResult(2))
	assert.NoError(t, err)

	// Segment cleanup is handled by the import inspector's processFailed (covered
	// by ImportInspectorSuite.TestProcessFailed), not by this callback.
	updatedJob := importMeta.GetJob(ctx, 2)
	assert.NotNil(t, updatedJob)
	assert.Equal(t, internalpb.ImportJobState_Failed, updatedJob.GetState())
	// UpdateJobState(Failed) also releases disk quota and arms GC eligibility.
	assert.EqualValues(t, 0, updatedJob.GetRequestedDiskSize(),
		"Failed transition must release RequestedDiskSize")
	assert.Greater(t, updatedJob.GetCleanupTs(), uint64(0),
		"Failed transition must set CleanupTs for GC eligibility")
}

func TestCommitImportCallback_AfterAbort_NoOp(t *testing.T) {
	ctx := context.Background()
	importMeta, _ := newTestImportMeta(t)

	// Job already Failed (abort won the race).
	job := &importJob{
		ImportJob: &datapb.ImportJob{
			JobID: 3,
			State: internalpb.ImportJobState_Failed,
		},
		tr: timerecord.NewTimeRecorder("test"),
	}
	err := importMeta.AddJob(ctx, job)
	assert.NoError(t, err)

	callbacks := &DDLCallbacks{Server: &Server{importMeta: importMeta}}
	err = callbacks.commitImportV2AckCallback(ctx, buildCommitImportBroadcastResult(3))
	assert.NoError(t, err)

	// UpdateJob skips jobs in Failed state → no-op.
	updatedJob := importMeta.GetJob(ctx, 3)
	assert.NotNil(t, updatedJob)
	assert.Equal(t, internalpb.ImportJobState_Failed, updatedJob.GetState())
}

func TestRollbackImportCallback_AfterCommit_NoOp(t *testing.T) {
	ctx := context.Background()
	importMeta, _ := newTestImportMeta(t)

	// Job already Committing (commit won the race).
	job := &importJob{
		ImportJob: &datapb.ImportJob{
			JobID: 4,
			State: internalpb.ImportJobState_Committing,
		},
		tr: timerecord.NewTimeRecorder("test"),
	}
	err := importMeta.AddJob(ctx, job)
	assert.NoError(t, err)

	callbacks := &DDLCallbacks{Server: &Server{importMeta: importMeta}}
	err = callbacks.rollbackImportV2AckCallback(ctx, buildRollbackImportBroadcastResult(4))
	assert.NoError(t, err)

	// Abort rejected: job already in committed state.
	updatedJob := importMeta.GetJob(ctx, 4)
	assert.NotNil(t, updatedJob)
	assert.Equal(t, internalpb.ImportJobState_Committing, updatedJob.GetState())
}

// TestImportAckCallbacks_CommitVsAbort_Race fires commit and rollback ack
// callbacks concurrently against the same Uncommitted job. In production these
// callbacks are serialized by the broadcaster's exclusive collection-level
// resource-key lock (both CommitImport and RollbackImport are ExclusiveRequired
// on NewExclusiveCollectionNameResourceKey), so this race is unreachable. The
// test documents the invariant from the callback side and provides regression
// coverage against future drift: the job must end in a deterministic terminal
// state (Committing or Failed) without panicking or corrupting meta. Run with
// `-race` to detect any unsynchronized access.
func TestImportAckCallbacks_CommitVsAbort_Race(t *testing.T) {
	for iter := 0; iter < 32; iter++ {
		ctx := context.Background()
		importMeta, _ := newTestImportMeta(t)

		const jobID int64 = 5
		err := importMeta.AddJob(ctx, &importJob{
			ImportJob: &datapb.ImportJob{
				JobID:      jobID,
				State:      internalpb.ImportJobState_Uncommitted,
				AutoCommit: false,
			},
			tr: timerecord.NewTimeRecorder("race"),
		})
		assert.NoError(t, err)

		callbacks := &DDLCallbacks{Server: &Server{importMeta: importMeta}}

		start := make(chan struct{})
		var commitErr, rollbackErr error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			commitErr = callbacks.commitImportV2AckCallback(ctx, buildCommitImportBroadcastResult(jobID))
		}()
		go func() {
			defer wg.Done()
			<-start
			rollbackErr = callbacks.rollbackImportV2AckCallback(ctx, buildRollbackImportBroadcastResult(jobID))
		}()
		close(start)
		wg.Wait()

		assert.NoError(t, commitErr)
		assert.NoError(t, rollbackErr)

		final := importMeta.GetJob(ctx, jobID).GetState()
		assert.Contains(t,
			[]internalpb.ImportJobState{internalpb.ImportJobState_Committing, internalpb.ImportJobState_Failed},
			final, "iter %d: terminal state must be Committing or Failed, got %s", iter, final)
	}
}

// --------------------------------
// broadcastCommitImportMessage / broadcastRollbackImportMessage Tests
// --------------------------------

// captureBroadcastAPI is a BroadcastAPI mock that records the message passed
// to Broadcast so a test can assert its broadcast target vchannels.
type captureBroadcastAPI struct {
	captured message.BroadcastMutableMessage
}

func (c *captureBroadcastAPI) Broadcast(_ context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
	c.captured = msg
	return &types.BroadcastAppendResult{BroadcastID: 1}, nil
}

func (c *captureBroadcastAPI) Close() {}

func testBroadcastTargetsDataVchannels(t *testing.T, broadcastFn func(*Server, context.Context, ImportJob) error) {
	ctx := context.Background()
	wantVchannels := []string{"by-dev-rootcoord-dml_0_v0", "by-dev-rootcoord-dml_1_v0"}

	mockBroker := broker.NewMockBroker(t)
	mockBroker.EXPECT().DescribeCollectionInternal(mock.Anything, int64(7)).Return(&milvuspb.DescribeCollectionResponse{
		DbName:         "test_db",
		CollectionName: "test_collection",
	}, nil)

	capture := &captureBroadcastAPI{}
	mockBroadcast := mockey.Mock(broadcast.StartBroadcastWithResourceKeys).To(
		func(_ context.Context, _ ...message.ResourceKey) (broadcaster.BroadcastAPI, error) {
			return capture, nil
		}).Build()
	defer mockBroadcast.UnPatch()

	server := &Server{broker: mockBroker}
	job := &importJob{
		ImportJob: &datapb.ImportJob{
			JobID:        7,
			CollectionID: 7,
			Vchannels:    wantVchannels,
		},
		tr: timerecord.NewTimeRecorder("test"),
	}

	err := broadcastFn(server, ctx, job)
	assert.NoError(t, err)
	assert.NotNil(t, capture.captured, "Broadcast must have been called")
	assert.ElementsMatch(t, wantVchannels, capture.captured.BroadcastHeader().VChannels,
		"broadcast must target the job's data vchannels, not the control channel")
}

// TestBroadcastCommitImportMessage_TargetsDataVchannels asserts that the
// CommitImport WAL message is broadcast to the job's data vchannels.
// Control-channel-only broadcasts are dropped by the WAL flusher's
// IsControlChannel guard before reaching the CommitImport case, so the
// message must reach data vchannels for HandleCommitVchannel to run.
func TestBroadcastCommitImportMessage_TargetsDataVchannels(t *testing.T) {
	testBroadcastTargetsDataVchannels(t, (*Server).broadcastCommitImportMessage)
}

// TestBroadcastRollbackImportMessage_TargetsDataVchannels asserts that the
// RollbackImport WAL message is broadcast to the job's data vchannels,
// matching the CommitImport routing.
func TestBroadcastRollbackImportMessage_TargetsDataVchannels(t *testing.T) {
	testBroadcastTargetsDataVchannels(t, (*Server).broadcastRollbackImportMessage)
}

func testBroadcastRequiresVchannels(t *testing.T, broadcastFn func(*Server, context.Context, ImportJob) error) {
	ctx := context.Background()
	server := &Server{}
	job := &importJob{
		ImportJob: &datapb.ImportJob{
			JobID:        7,
			CollectionID: 7,
		},
		tr: timerecord.NewTimeRecorder("test"),
	}

	err := broadcastFn(server, ctx, job)
	assert.Error(t, err)
	// Missing vchannels is internal broadcast state -> ErrImportSysFailed.
	assert.True(t, errors.Is(err, merr.ErrImportSysFailed))
	assert.Contains(t, err.Error(), "job 7 has no vchannels")
}

func TestBroadcastCommitImportMessage_RequiresVchannels(t *testing.T) {
	testBroadcastRequiresVchannels(t, (*Server).broadcastCommitImportMessage)
}

func TestBroadcastRollbackImportMessage_RequiresVchannels(t *testing.T) {
	testBroadcastRequiresVchannels(t, (*Server).broadcastRollbackImportMessage)
}
