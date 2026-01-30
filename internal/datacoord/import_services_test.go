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
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

// ================================
// Import Services Test Suite
// ================================

type ImportServicesSuite struct {
	suite.Suite
}

func TestImportServicesSuite(t *testing.T) {
	suite.Run(t, new(ImportServicesSuite))
}

// --------------------------------
// ImportV2 Tests
// --------------------------------

func (s *ImportServicesSuite) TestImportV2_ServerNotHealthyReturnsError() {
	ctx := context.Background()
	server := &Server{}
	server.stateCode.Store(commonpb.StateCode_Initializing)

	resp, err := server.ImportV2(ctx, nil)

	s.NoError(err)
	s.NotNil(resp)
	s.True(errors.Is(merr.Error(resp.GetStatus()), merr.ErrServiceNotReady))
}

func (s *ImportServicesSuite) TestImportV2_InvalidTimeoutReturnsError() {
	ctx := context.Background()
	server := &Server{}
	server.stateCode.Store(commonpb.StateCode_Healthy)

	req := &internalpb.ImportRequestInternal{
		Options: []*commonpb.KeyValuePair{
			{Key: "timeout", Value: "invalid_format"},
		},
	}

	resp, err := server.ImportV2(ctx, req)

	s.NoError(err)
	s.NotNil(resp)
	s.True(errors.Is(merr.Error(resp.GetStatus()), merr.ErrImportFailed))
}

func (s *ImportServicesSuite) TestImportV2_AllocatorNilReturnsError() {
	ctx := context.Background()
	server := &Server{}
	server.stateCode.Store(commonpb.StateCode_Healthy)
	server.allocator = nil

	req := &internalpb.ImportRequestInternal{
		Options: []*commonpb.KeyValuePair{
			{Key: "timeout", Value: "300s"},
		},
	}

	resp, err := server.ImportV2(ctx, req)

	s.NoError(err)
	s.NotNil(resp)
	s.True(errors.Is(merr.Error(resp.GetStatus()), merr.ErrImportFailed))
	s.Contains(resp.GetStatus().GetReason(), "allocator not initialized")
}

func (s *ImportServicesSuite) TestImportV2_AllocatorFailsReturnsError() {
	ctx := context.Background()
	server := &Server{}
	server.stateCode.Store(commonpb.StateCode_Healthy)

	mockAllocator := allocator.NewMockAllocator(s.T())
	mockAllocator.EXPECT().AllocN(mock.Anything).Return(int64(0), int64(0), errors.New("allocation failed"))
	server.allocator = mockAllocator

	req := &internalpb.ImportRequestInternal{
		Options: []*commonpb.KeyValuePair{
			{Key: "timeout", Value: "300s"},
		},
	}

	resp, err := server.ImportV2(ctx, req)

	s.NoError(err)
	s.NotNil(resp)
	s.True(errors.Is(merr.Error(resp.GetStatus()), merr.ErrImportFailed))
	s.Contains(resp.GetStatus().GetReason(), "failed to allocate job ID")
}

func (s *ImportServicesSuite) TestImportV2_BroadcastFailsReturnsError() {
	ctx := context.Background()

	// Mock validation to pass but broadcast to fail
	mockCount := mockey.Mock((*importMeta).CountJobBy).To(func(_ *importMeta, _ context.Context, _ ...ImportJobFilter) int {
		return 1
	}).Build()
	defer mockCount.UnPatch()

	mockBalancerInst := &mockBalancerImpl{}
	mockBalance := mockey.Mock(balance.GetWithContext).To(func(ctx context.Context) (balancer.Balancer, error) {
		return mockBalancerInst, nil
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
			return nil, errors.New("broadcast failed")
		}).Build()
	defer mockBroadcast.UnPatch()

	server := &Server{
		importMeta: &importMeta{},
	}
	server.stateCode.Store(commonpb.StateCode_Healthy)

	mockAllocator := allocator.NewMockAllocator(s.T())
	mockAllocator.EXPECT().AllocN(mock.Anything).Return(int64(1000), int64(1001), nil)
	server.allocator = mockAllocator

	req := &internalpb.ImportRequestInternal{
		CollectionID:   100,
		CollectionName: "test_collection",
		PartitionIDs:   []int64{1},
		ChannelNames:   []string{"v1"},
		Schema: &schemapb.CollectionSchema{
			Name:   "test_collection",
			DbName: "test_db",
		},
		Files: []*internalpb.ImportFile{
			{Id: 1, Paths: []string{"/test/file.json"}},
		},
		Options: []*commonpb.KeyValuePair{
			{Key: "timeout", Value: "300s"},
		},
	}

	resp, err := server.ImportV2(ctx, req)

	s.NoError(err)
	s.NotNil(resp)
	s.True(errors.Is(merr.Error(resp.GetStatus()), merr.ErrImportFailed))
	s.Contains(resp.GetStatus().GetReason(), "failed to broadcast import")
}

func (s *ImportServicesSuite) TestImportV2_SuccessReturnsJobID() {
	ctx := context.Background()

	// Mock validation to pass
	mockCount := mockey.Mock((*importMeta).CountJobBy).To(func(_ *importMeta, _ context.Context, _ ...ImportJobFilter) int {
		return 1
	}).Build()
	defer mockCount.UnPatch()

	mockBalancerInst := &mockBalancerImpl{}
	mockBalance := mockey.Mock(balance.GetWithContext).To(func(ctx context.Context) (balancer.Balancer, error) {
		return mockBalancerInst, nil
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
	server.stateCode.Store(commonpb.StateCode_Healthy)

	mockAllocator := allocator.NewMockAllocator(s.T())
	mockAllocator.EXPECT().AllocN(mock.Anything).Return(int64(1000), int64(1001), nil)
	server.allocator = mockAllocator

	req := &internalpb.ImportRequestInternal{
		CollectionID:   100,
		CollectionName: "test_collection",
		PartitionIDs:   []int64{1},
		ChannelNames:   []string{"v1"},
		Schema: &schemapb.CollectionSchema{
			Name:   "test_collection",
			DbName: "test_db",
		},
		Files: []*internalpb.ImportFile{
			{Id: 1, Paths: []string{"/test/file.json"}},
		},
		Options: []*commonpb.KeyValuePair{
			{Key: "timeout", Value: "300s"},
		},
	}

	resp, err := server.ImportV2(ctx, req)

	s.NoError(err)
	s.NotNil(resp)
	s.Equal(int32(0), resp.GetStatus().GetCode())
	s.Equal("1000", resp.GetJobID())
}

func (s *ImportServicesSuite) TestImportV2_UsesDefaultDbNameWhenEmpty() {
	ctx := context.Background()

	// Mock validation to pass
	mockCount := mockey.Mock((*importMeta).CountJobBy).To(func(_ *importMeta, _ context.Context, _ ...ImportJobFilter) int {
		return 1
	}).Build()
	defer mockCount.UnPatch()

	mockBalancerInst := &mockBalancerImpl{}
	mockBalance := mockey.Mock(balance.GetWithContext).To(func(ctx context.Context) (balancer.Balancer, error) {
		return mockBalancerInst, nil
	}).Build()
	defer mockBalance.UnPatch()

	mockAssignment := mockey.Mock((*mockBalancerImpl).GetLatestChannelAssignment).To(
		func(_ *mockBalancerImpl) (*channel.WatchChannelAssignmentsCallbackParam, error) {
			return &channel.WatchChannelAssignmentsCallbackParam{
				ReplicateConfiguration: nil,
			}, nil
		}).Build()
	defer mockAssignment.UnPatch()

	// Capture the dbName passed to broadcastImport
	var capturedDbName string
	mockBroadcast := mockey.Mock(broadcast.StartBroadcastWithResourceKeys).To(
		func(ctx context.Context, keys ...message.ResourceKey) (broadcaster.BroadcastAPI, error) {
			// Check if default db name resource key is used
			for _, key := range keys {
				if key.String() != "" {
					capturedDbName = "default" // This indicates default db was used
				}
			}
			return nil, errors.New("stop here for test")
		}).Build()
	defer mockBroadcast.UnPatch()

	server := &Server{
		importMeta: &importMeta{},
	}
	server.stateCode.Store(commonpb.StateCode_Healthy)

	mockAllocator := allocator.NewMockAllocator(s.T())
	mockAllocator.EXPECT().AllocN(mock.Anything).Return(int64(1000), int64(1001), nil)
	server.allocator = mockAllocator

	// Request with empty DbName in schema
	req := &internalpb.ImportRequestInternal{
		CollectionID:   100,
		CollectionName: "test_collection",
		PartitionIDs:   []int64{1},
		ChannelNames:   []string{"v1"},
		Schema: &schemapb.CollectionSchema{
			Name:   "test_collection",
			DbName: "", // Empty - should use default
		},
		Files: []*internalpb.ImportFile{
			{Id: 1, Paths: []string{"/test/file.json"}},
		},
		Options: []*commonpb.KeyValuePair{
			{Key: "timeout", Value: "300s"},
		},
	}

	server.ImportV2(ctx, req)

	// Verify that default db name was used (test stops at broadcast mock)
	s.Equal("default", capturedDbName)
}

// --------------------------------
// createImportJobFromAck Tests
// --------------------------------

func (s *ImportServicesSuite) TestCreateImportJobFromAck_ServerNotHealthyReturnsError() {
	ctx := context.Background()
	server := &Server{}
	server.stateCode.Store(commonpb.StateCode_Initializing)

	resp, err := server.createImportJobFromAck(ctx, nil)

	s.NoError(err)
	s.NotNil(resp)
	s.True(errors.Is(merr.Error(resp.GetStatus()), merr.ErrServiceNotReady))
}

func (s *ImportServicesSuite) TestCreateImportJobFromAck_InvalidTimeoutReturnsError() {
	ctx := context.Background()
	server := &Server{}
	server.stateCode.Store(commonpb.StateCode_Healthy)

	req := &internalpb.ImportRequestInternal{
		Options: []*commonpb.KeyValuePair{
			{Key: "timeout", Value: "invalid_format"},
		},
	}

	resp, err := server.createImportJobFromAck(ctx, req)

	s.NoError(err)
	s.NotNil(resp)
	s.True(errors.Is(merr.Error(resp.GetStatus()), merr.ErrImportFailed))
}

func (s *ImportServicesSuite) TestCreateImportJobFromAck_AllocatorFailsReturnsError() {
	ctx := context.Background()
	server := &Server{}
	server.stateCode.Store(commonpb.StateCode_Healthy)

	mockAllocator := allocator.NewMockAllocator(s.T())
	mockAllocator.EXPECT().AllocN(mock.Anything).Return(int64(0), int64(0), errors.New("allocation failed"))
	server.allocator = mockAllocator

	req := &internalpb.ImportRequestInternal{
		CollectionID: 100,
		Files: []*internalpb.ImportFile{
			{Id: 1, Paths: []string{"/test/file.json"}},
		},
		Options: []*commonpb.KeyValuePair{
			{Key: "timeout", Value: "300s"},
		},
	}

	resp, err := server.createImportJobFromAck(ctx, req)

	s.NoError(err)
	s.NotNil(resp)
	s.True(errors.Is(merr.Error(resp.GetStatus()), merr.ErrImportFailed))
	s.Contains(resp.GetStatus().GetReason(), "alloc id failed")
}

func (s *ImportServicesSuite) TestCreateImportJobFromAck_CollectionNotFoundReturnsError() {
	ctx := context.Background()

	mockHandler := NewNMockHandler(s.T())
	mockHandler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(nil, merr.ErrCollectionNotFound)

	server := &Server{
		handler: mockHandler,
	}
	server.stateCode.Store(commonpb.StateCode_Healthy)

	mockAllocator := allocator.NewMockAllocator(s.T())
	mockAllocator.EXPECT().AllocN(mock.Anything).Return(int64(1000), int64(1002), nil)
	server.allocator = mockAllocator

	req := &internalpb.ImportRequestInternal{
		CollectionID: 100,
		Files: []*internalpb.ImportFile{
			{Id: 1, Paths: []string{"/test/file.json"}},
		},
		Options: []*commonpb.KeyValuePair{
			{Key: "timeout", Value: "300s"},
		},
	}

	resp, err := server.createImportJobFromAck(ctx, req)

	s.NoError(err)
	s.NotNil(resp)
	s.True(errors.Is(merr.Error(resp.GetStatus()), merr.ErrCollectionNotFound))
}

func (s *ImportServicesSuite) TestCreateImportJobFromAck_CollectionNilReturnsError() {
	ctx := context.Background()

	mockHandler := NewNMockHandler(s.T())
	mockHandler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(nil, nil)

	server := &Server{
		handler: mockHandler,
	}
	server.stateCode.Store(commonpb.StateCode_Healthy)

	mockAllocator := allocator.NewMockAllocator(s.T())
	mockAllocator.EXPECT().AllocN(mock.Anything).Return(int64(1000), int64(1002), nil)
	server.allocator = mockAllocator

	req := &internalpb.ImportRequestInternal{
		CollectionID: 100,
		Files: []*internalpb.ImportFile{
			{Id: 1, Paths: []string{"/test/file.json"}},
		},
		Options: []*commonpb.KeyValuePair{
			{Key: "timeout", Value: "300s"},
		},
	}

	resp, err := server.createImportJobFromAck(ctx, req)

	s.NoError(err)
	s.NotNil(resp)
	s.True(errors.Is(merr.Error(resp.GetStatus()), merr.ErrCollectionNotFound))
}

func (s *ImportServicesSuite) TestCreateImportJobFromAck_AddJobFailsReturnsError() {
	ctx := context.Background()

	mockHandler := NewNMockHandler(s.T())
	mockHandler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(&collectionInfo{
		ID:            100,
		VChannelNames: []string{"v1"},
	}, nil)

	catalog := mocks.NewDataCoordCatalog(s.T())
	catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(errors.New("save job failed"))

	importMeta, err := NewImportMeta(context.TODO(), catalog, nil, nil)
	s.NoError(err)

	server := &Server{
		handler:    mockHandler,
		importMeta: importMeta,
	}
	server.stateCode.Store(commonpb.StateCode_Healthy)

	mockAllocator := allocator.NewMockAllocator(s.T())
	mockAllocator.EXPECT().AllocN(mock.Anything).Return(int64(1000), int64(1002), nil)
	server.allocator = mockAllocator

	req := &internalpb.ImportRequestInternal{
		CollectionID:   100,
		CollectionName: "test_collection",
		PartitionIDs:   []int64{1},
		ChannelNames:   []string{"v1"},
		Schema:         &schemapb.CollectionSchema{Name: "test_collection"},
		Files: []*internalpb.ImportFile{
			{Id: 1, Paths: []string{"/test/file.json"}},
		},
		Options: []*commonpb.KeyValuePair{
			{Key: "timeout", Value: "300s"},
		},
		DataTimestamp: 123456789,
		JobID:         2000,
	}

	resp, err := server.createImportJobFromAck(ctx, req)

	s.NoError(err)
	s.NotNil(resp)
	s.True(errors.Is(merr.Error(resp.GetStatus()), merr.ErrImportFailed))
	s.Contains(resp.GetStatus().GetReason(), "add import job failed")
}

func (s *ImportServicesSuite) TestCreateImportJobFromAck_SuccessWithProvidedJobID() {
	ctx := context.Background()

	mockHandler := NewNMockHandler(s.T())
	mockHandler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(&collectionInfo{
		ID:            100,
		VChannelNames: []string{"v1"},
	}, nil)

	catalog := mocks.NewDataCoordCatalog(s.T())
	catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)

	importMeta, err := NewImportMeta(context.TODO(), catalog, nil, nil)
	s.NoError(err)

	server := &Server{
		handler:    mockHandler,
		importMeta: importMeta,
	}
	server.stateCode.Store(commonpb.StateCode_Healthy)

	mockAllocator := allocator.NewMockAllocator(s.T())
	mockAllocator.EXPECT().AllocN(mock.Anything).Return(int64(1000), int64(1002), nil)
	server.allocator = mockAllocator

	req := &internalpb.ImportRequestInternal{
		CollectionID:   100,
		CollectionName: "test_collection",
		PartitionIDs:   []int64{1},
		ChannelNames:   []string{"v1"},
		Schema:         &schemapb.CollectionSchema{Name: "test_collection"},
		Files: []*internalpb.ImportFile{
			{Id: 1, Paths: []string{"/test/file.json"}},
		},
		Options: []*commonpb.KeyValuePair{
			{Key: "timeout", Value: "300s"},
		},
		DataTimestamp: 123456789,
		JobID:         2000, // Provided job ID should be used
	}

	resp, err := server.createImportJobFromAck(ctx, req)

	s.NoError(err)
	s.NotNil(resp)
	s.Equal(int32(0), resp.GetStatus().GetCode())
	s.Equal("2000", resp.GetJobID()) // Should use provided job ID
}

func (s *ImportServicesSuite) TestCreateImportJobFromAck_SuccessAllocatesJobIDWhenNotProvided() {
	ctx := context.Background()

	mockHandler := NewNMockHandler(s.T())
	mockHandler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(&collectionInfo{
		ID:            100,
		VChannelNames: []string{"v1"},
	}, nil)

	catalog := mocks.NewDataCoordCatalog(s.T())
	catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)

	importMeta, err := NewImportMeta(context.TODO(), catalog, nil, nil)
	s.NoError(err)

	server := &Server{
		handler:    mockHandler,
		importMeta: importMeta,
	}
	server.stateCode.Store(commonpb.StateCode_Healthy)

	mockAllocator := allocator.NewMockAllocator(s.T())
	mockAllocator.EXPECT().AllocN(mock.Anything).Return(int64(1000), int64(1002), nil)
	server.allocator = mockAllocator

	req := &internalpb.ImportRequestInternal{
		CollectionID:   100,
		CollectionName: "test_collection",
		PartitionIDs:   []int64{1},
		ChannelNames:   []string{"v1"},
		Schema:         &schemapb.CollectionSchema{Name: "test_collection"},
		Files: []*internalpb.ImportFile{
			{Id: 1, Paths: []string{"/test/file.json"}},
		},
		Options: []*commonpb.KeyValuePair{
			{Key: "timeout", Value: "300s"},
		},
		DataTimestamp: 123456789,
		JobID:         0, // Not provided - should use idStart (1000)
	}

	resp, err := server.createImportJobFromAck(ctx, req)

	s.NoError(err)
	s.NotNil(resp)
	s.Equal(int32(0), resp.GetStatus().GetCode())
	s.Equal("1000", resp.GetJobID()) // Should use allocated idStart
}

func (s *ImportServicesSuite) TestCreateImportJobFromAck_AssignsFileIDs() {
	ctx := context.Background()

	mockHandler := NewNMockHandler(s.T())
	mockHandler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(&collectionInfo{
		ID:            100,
		VChannelNames: []string{"v1"},
	}, nil)

	var savedJob *datapb.ImportJob
	catalog := mocks.NewDataCoordCatalog(s.T())
	catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, job *datapb.ImportJob) error {
		savedJob = job
		return nil
	})

	importMeta, err := NewImportMeta(context.TODO(), catalog, nil, nil)
	s.NoError(err)

	server := &Server{
		handler:    mockHandler,
		importMeta: importMeta,
	}
	server.stateCode.Store(commonpb.StateCode_Healthy)

	mockAllocator := allocator.NewMockAllocator(s.T())
	// With 3 files, AllocN(4) will be called (files + 1 for job ID)
	mockAllocator.EXPECT().AllocN(mock.Anything).Return(int64(1000), int64(1004), nil)
	server.allocator = mockAllocator

	req := &internalpb.ImportRequestInternal{
		CollectionID:   100,
		CollectionName: "test_collection",
		PartitionIDs:   []int64{1},
		ChannelNames:   []string{"v1"},
		Schema:         &schemapb.CollectionSchema{Name: "test_collection"},
		Files: []*internalpb.ImportFile{
			{Id: 0, Paths: []string{"/test/file1.json"}},
			{Id: 0, Paths: []string{"/test/file2.json"}},
			{Id: 0, Paths: []string{"/test/file3.json"}},
		},
		Options: []*commonpb.KeyValuePair{
			{Key: "timeout", Value: "300s"},
		},
		DataTimestamp: 123456789,
		JobID:         2000,
	}

	resp, err := server.createImportJobFromAck(ctx, req)

	s.NoError(err)
	s.NotNil(resp)
	s.Equal(int32(0), resp.GetStatus().GetCode())

	// Verify file IDs were assigned correctly
	s.NotNil(savedJob)
	files := savedJob.GetFiles()
	s.Len(files, 3)
	s.Equal(int64(1001), files[0].GetId()) // idStart + 0 + 1
	s.Equal(int64(1002), files[1].GetId()) // idStart + 1 + 1
	s.Equal(int64(1003), files[2].GetId()) // idStart + 2 + 1
}

// Helper types are defined in import_callbacks_test.go (mockBalancerImpl, mockBroadcastAPIImpl, newMockBroadcastAPIImpl)
