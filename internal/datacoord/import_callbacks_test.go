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
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
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
	// ValidateMaxImportJobExceed returns WrapErrImportFailed, not ErrServiceQuotaExceeded
	s.True(errors.Is(err, merr.ErrImportFailed))
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
	s.True(errors.Is(err, merr.ErrImportFailed))
	s.Contains(err.Error(), "replicating cluster")
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
		"test_db",
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

	// Mock StartBroadcastWithResourceKeys to fail
	mockBroadcast := mockey.Mock(broadcast.StartBroadcastWithResourceKeys).To(
		func(ctx context.Context, keys ...message.ResourceKey) (broadcaster.BroadcastAPI, error) {
			return nil, errors.New("failed to acquire resource lock")
		}).Build()
	defer mockBroadcast.UnPatch()

	server := &Server{
		importMeta: &importMeta{},
	}

	err := server.broadcastImport(
		ctx,
		"test_db",
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
	s.Contains(err.Error(), "failed to start broadcast with resource lock")
}

func (s *ImportCallbacksSuite) TestBroadcastImport_CollectionNotExistsReturnsError() {
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

	// Mock broker.HasCollection to return false (collection not exists)
	mockBroker := broker.NewMockBroker(s.T())
	mockBroker.EXPECT().HasCollection(ctx, int64(100)).Return(false, nil)

	server := &Server{
		importMeta: &importMeta{},
		broker:     mockBroker,
	}

	err := server.broadcastImport(
		ctx,
		"test_db",
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

	// Mock broker.HasCollection to return true
	mockBroker := broker.NewMockBroker(s.T())
	mockBroker.EXPECT().HasCollection(ctx, int64(100)).Return(true, nil)

	server := &Server{
		importMeta: &importMeta{},
		broker:     mockBroker,
	}

	err := server.broadcastImport(
		ctx,
		"test_db",
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

	// Mock broker.HasCollection to return true
	mockBroker := broker.NewMockBroker(s.T())
	mockBroker.EXPECT().HasCollection(ctx, int64(100)).Return(true, nil)

	server := &Server{
		importMeta: &importMeta{},
		broker:     mockBroker,
	}

	err := server.broadcastImport(
		ctx,
		"test_db",
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
// RegisterImportCallbacks Tests
// --------------------------------

func (s *ImportCallbacksSuite) TestRegisterImportCallbacks_DoesNotPanic() {
	server := &Server{}

	s.NotPanics(func() {
		RegisterImportCallbacks(server)
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
	// Add some operations to ensure the function is long enough for mockey to patch
	result := &channel.WatchChannelAssignmentsCallbackParam{}
	if result != nil {
		return result, nil
	}
	return nil, nil
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
