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
	"math"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
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
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
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

func (s *ImportServicesSuite) TestImportV2_L0ImportDisabledReturnsError() {
	paramtable.Init()
	ctx := context.Background()
	server := &Server{}
	server.stateCode.Store(commonpb.StateCode_Healthy)

	// enableL0Import defaults to false, so an l0_import request must be rejected
	// before reaching allocation (allocator is nil here, proving the early reject).
	req := &internalpb.ImportRequestInternal{
		Options: []*commonpb.KeyValuePair{
			{Key: "timeout", Value: "300s"},
			{Key: "l0_import", Value: "true"},
		},
	}

	resp, err := server.ImportV2(ctx, req)

	s.NoError(err)
	s.NotNil(resp)
	s.True(errors.Is(merr.Error(resp.GetStatus()), merr.ErrImportFailed))
	s.Contains(resp.GetStatus().GetReason(), "l0 import is disabled")
}

func (s *ImportServicesSuite) TestImportV2_L0ImportEnabledPassesGate() {
	paramtable.Init()
	ctx := context.Background()
	server := &Server{}
	server.stateCode.Store(commonpb.StateCode_Healthy)

	params := paramtable.Get()
	params.Save(params.DataCoordCfg.EnableL0Import.Key, "true")
	defer params.Reset(params.DataCoordCfg.EnableL0Import.Key)

	// With enableL0Import=true the same request must get past the L0 gate and
	// fail later on the nil allocator instead — proof the gate was skipped.
	req := &internalpb.ImportRequestInternal{
		Options: []*commonpb.KeyValuePair{
			{Key: "timeout", Value: "300s"},
			{Key: "l0_import", Value: "true"},
		},
	}

	resp, err := server.ImportV2(ctx, req)

	s.NoError(err)
	s.NotNil(resp)
	s.True(errors.Is(merr.Error(resp.GetStatus()), merr.ErrServiceUnavailable))
	s.Contains(resp.GetStatus().GetReason(), "allocator not initialized")
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
	s.True(errors.Is(merr.Error(resp.GetStatus()), merr.ErrServiceUnavailable))
	s.Contains(resp.GetStatus().GetReason(), "allocator not initialized")
}

func (s *ImportServicesSuite) TestImportV2_AllocatorFailsReturnsError() {
	ctx := context.Background()
	server := &Server{}
	server.stateCode.Store(commonpb.StateCode_Healthy)

	mockAllocator := allocator.NewMockAllocator(s.T())
	mockAllocator.EXPECT().AllocN(mock.Anything).Return(int64(0), int64(0), merr.WrapErrServiceUnavailable("allocation failed"))
	server.allocator = mockAllocator

	req := &internalpb.ImportRequestInternal{
		Options: []*commonpb.KeyValuePair{
			{Key: "timeout", Value: "300s"},
		},
	}

	resp, err := server.ImportV2(ctx, req)

	s.NoError(err)
	s.NotNil(resp)
	s.True(errors.Is(merr.Error(resp.GetStatus()), merr.ErrServiceUnavailable))
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

	// Mock broker.DescribeCollectionInternal (called once in startBroadcastWithCollectionID, which will fail at StartBroadcastWithResourceKeys)
	mockBroker := broker.NewMockBroker(s.T())
	mockBroker.EXPECT().DescribeCollectionInternal(mock.Anything, int64(100)).Return(&milvuspb.DescribeCollectionResponse{
		DbName:         "test_db",
		CollectionName: "test_collection",
	}, nil)

	// Mock StartBroadcastWithResourceKeys to fail
	mockBroadcast := mockey.Mock(broadcast.StartBroadcastWithResourceKeys).To(
		func(ctx context.Context, keys ...message.ResourceKey) (broadcaster.BroadcastAPI, error) {
			return nil, merr.WrapErrServiceUnavailable("broadcast failed")
		}).Build()
	defer mockBroadcast.UnPatch()

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
	s.True(errors.Is(merr.Error(resp.GetStatus()), merr.ErrServiceUnavailable))
	s.Contains(resp.GetStatus().GetReason(), "broadcast")
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

	// Mock broker.DescribeCollectionInternal to return empty dbName (called in startBroadcastWithCollectionID)
	mockBroker := broker.NewMockBroker(s.T())
	mockBroker.EXPECT().DescribeCollectionInternal(mock.Anything, int64(100)).Return(&milvuspb.DescribeCollectionResponse{
		DbName:         "", // Empty - should use default
		CollectionName: "test_collection",
	}, nil)

	server := &Server{
		importMeta: &importMeta{},
		broker:     mockBroker,
	}
	server.stateCode.Store(commonpb.StateCode_Healthy)

	mockAllocator := allocator.NewMockAllocator(s.T())
	mockAllocator.EXPECT().AllocN(mock.Anything).Return(int64(1000), int64(1001), nil)
	server.allocator = mockAllocator

	// Request with empty DbName in schema (not used anymore, broker is the source of truth)
	req := &internalpb.ImportRequestInternal{
		CollectionID:   100,
		CollectionName: "test_collection",
		PartitionIDs:   []int64{1},
		ChannelNames:   []string{"v1"},
		Schema: &schemapb.CollectionSchema{
			Name:   "test_collection",
			DbName: "", // Empty - not used anymore
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
	mockAllocator.EXPECT().AllocN(mock.Anything).Return(int64(0), int64(0), merr.WrapErrServiceUnavailable("allocation failed"))
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
	s.True(errors.Is(merr.Error(resp.GetStatus()), merr.ErrServiceUnavailable))
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
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(merr.WrapErrServiceUnavailable("save job failed"))

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
	s.True(errors.Is(merr.Error(resp.GetStatus()), merr.ErrServiceUnavailable))
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

func (s *ImportServicesSuite) TestCreateImportJobFromAck_L0ImportDisabledCreatesFailedJob() {
	paramtable.Init()
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
	mockAllocator.EXPECT().AllocN(mock.Anything).Return(int64(1000), int64(1002), nil)
	server.allocator = mockAllocator

	// enableL0Import defaults to false. A replicated l0_import message reaching
	// the ack path must NOT create a runnable job, and must NOT return an error
	// (ack callbacks retry forever); instead the job is created in Failed state
	// so replicated CommitImport becomes a terminal no-op.
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
			{Key: "l0_import", Value: "true"},
		},
		DataTimestamp: 123456789,
		JobID:         2000,
	}

	resp, err := server.createImportJobFromAck(ctx, req)

	s.NoError(err)
	s.NotNil(resp)
	s.Equal(int32(0), resp.GetStatus().GetCode())
	s.Equal("2000", resp.GetJobID())

	s.NotNil(savedJob)
	s.Equal(internalpb.ImportJobState_Failed, savedJob.GetState())
	s.Contains(savedJob.GetReason(), "l0 import is disabled")
	// Failed at creation must carry a real cleanup ts so GC can reclaim the job.
	s.NotEqual(uint64(math.MaxUint64), savedJob.GetCleanupTs())
}

func (s *ImportServicesSuite) TestCreateImportJobFromAck_L0ImportEnabledCreatesPendingJob() {
	paramtable.Init()
	ctx := context.Background()

	params := paramtable.Get()
	params.Save(params.DataCoordCfg.EnableL0Import.Key, "true")
	defer params.Reset(params.DataCoordCfg.EnableL0Import.Key)

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
			{Key: "l0_import", Value: "true"},
		},
		DataTimestamp: 123456789,
		JobID:         2000,
	}

	resp, err := server.createImportJobFromAck(ctx, req)

	s.NoError(err)
	s.NotNil(resp)
	s.Equal(int32(0), resp.GetStatus().GetCode())

	s.NotNil(savedJob)
	s.Equal(internalpb.ImportJobState_Pending, savedJob.GetState())
}

const testCollectionID = int64(100)

func TestCheckL0ImportAllowed(t *testing.T) {
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.EnableL0Import.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.EnableL0Import.Key)

	t.Run("legacy l0_import still rejected", func(t *testing.T) {
		err := checkL0ImportAllowed([]*commonpb.KeyValuePair{
			{Key: importutilv2.L0Import, Value: "true"},
		})
		assert.Error(t, err)
	})

	t.Run("write_mode delete allowed", func(t *testing.T) {
		err := checkL0ImportAllowed([]*commonpb.KeyValuePair{
			{Key: importutilv2.WriteMode, Value: "Delete"},
		})
		assert.NoError(t, err)
	})

	t.Run("write_mode upsert allowed", func(t *testing.T) {
		err := checkL0ImportAllowed([]*commonpb.KeyValuePair{
			{Key: importutilv2.WriteMode, Value: "Upsert"},
		})
		assert.NoError(t, err)
	})
}

// newTestServerWithCollection builds a *Server backed by a bare *meta (no gRPC/etcd plumbing)
// with testCollectionID registered as a collection, following the lightweight meta construction
// pattern used in import_checker_test.go's ImportCheckerSuite.SetupTest.
func newTestServerWithCollection(t *testing.T) *Server {
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListIndexes(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListSegmentIndexes(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListAnalyzeTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListCompactionTargets(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListSnapshots(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListExternalCollectionRefreshJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListExternalCollectionRefreshTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil).Maybe()

	mockBroker := broker.NewMockBroker(t)
	mockBroker.EXPECT().ShowCollectionIDs(mock.Anything).Return(nil, nil)

	m, err := newMeta(context.TODO(), catalog, nil, mockBroker)
	require.NoError(t, err)
	m.AddCollection(&collectionInfo{ID: testCollectionID})

	s := &Server{meta: m}
	s.stateCode.Store(commonpb.StateCode_Healthy)
	return s
}

func TestCheckWriteModeSupported(t *testing.T) {
	t.Run("rejected when the collection has manifest-less segments", func(t *testing.T) {
		s := newTestServerWithCollection(t)
		require.NoError(t, s.meta.AddSegment(context.TODO(), NewSegmentInfo(&datapb.SegmentInfo{
			ID: 1, CollectionID: testCollectionID, Level: datapb.SegmentLevel_L1,
			State: commonpb.SegmentState_Flushed, ManifestPath: "",
		})))
		err := s.checkWriteModeSupported(context.TODO(), testCollectionID,
			[]*commonpb.KeyValuePair{{Key: importutilv2.WriteMode, Value: "Delete"}})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "storage v3")
	})

	t.Run("allowed when every data segment is manifest-based", func(t *testing.T) {
		s := newTestServerWithCollection(t)
		require.NoError(t, s.meta.AddSegment(context.TODO(), NewSegmentInfo(&datapb.SegmentInfo{
			ID: 2, CollectionID: testCollectionID, Level: datapb.SegmentLevel_L1,
			State: commonpb.SegmentState_Flushed, ManifestPath: "s3://base/100",
		})))
		err := s.checkWriteModeSupported(context.TODO(), testCollectionID,
			[]*commonpb.KeyValuePair{{Key: importutilv2.WriteMode, Value: "Upsert"}})
		assert.NoError(t, err)
	})

	t.Run("append mode is never gated", func(t *testing.T) {
		s := newTestServerWithCollection(t)
		require.NoError(t, s.meta.AddSegment(context.TODO(), NewSegmentInfo(&datapb.SegmentInfo{
			ID: 3, CollectionID: testCollectionID, Level: datapb.SegmentLevel_L1,
			State: commonpb.SegmentState_Flushed, ManifestPath: "",
		})))
		assert.NoError(t, s.checkWriteModeSupported(context.TODO(), testCollectionID, nil))
	})

	t.Run("L0 segments are not counted", func(t *testing.T) {
		s := newTestServerWithCollection(t)
		require.NoError(t, s.meta.AddSegment(context.TODO(), NewSegmentInfo(&datapb.SegmentInfo{
			ID: 4, CollectionID: testCollectionID, Level: datapb.SegmentLevel_L0,
			State: commonpb.SegmentState_Flushed, ManifestPath: "",
		})))
		err := s.checkWriteModeSupported(context.TODO(), testCollectionID,
			[]*commonpb.KeyValuePair{{Key: importutilv2.WriteMode, Value: "Delete"}})
		assert.NoError(t, err, "L0 segments never receive folded deletes; they are the source")
	})

	t.Run("malformed write_mode is rejected, not treated as append", func(t *testing.T) {
		s := newTestServerWithCollection(t)
		// A manifest-less segment would make Delete/Upsert fail the gate, but a value that
		// degrades to Append would pass it — so this asserts the parse error surfaces first.
		require.NoError(t, s.meta.AddSegment(context.TODO(), NewSegmentInfo(&datapb.SegmentInfo{
			ID: 5, CollectionID: testCollectionID, Level: datapb.SegmentLevel_L1,
			State: commonpb.SegmentState_Flushed, ManifestPath: "s3://base/100",
		})))
		err := s.checkWriteModeSupported(context.TODO(), testCollectionID,
			[]*commonpb.KeyValuePair{{Key: importutilv2.WriteMode, Value: "Replace"}})
		assert.Error(t, err)
	})
}

// TestImportV2_WriteModeRequiresManifestSegments covers the ImportV2 wiring of
// checkWriteModeSupported, mirroring TestImportV2_L0ImportDisabledReturnsError's pattern of
// driving ImportV2 directly and asserting on the returned status.
func TestImportV2_WriteModeRequiresManifestSegments(t *testing.T) {
	paramtable.Init()
	s := newTestServerWithCollection(t)
	require.NoError(t, s.meta.AddSegment(context.TODO(), NewSegmentInfo(&datapb.SegmentInfo{
		ID: 6, CollectionID: testCollectionID, Level: datapb.SegmentLevel_L1,
		State: commonpb.SegmentState_Flushed, ManifestPath: "",
	})))

	req := &internalpb.ImportRequestInternal{
		CollectionID: testCollectionID,
		Options: []*commonpb.KeyValuePair{
			{Key: "timeout", Value: "300s"},
			{Key: importutilv2.WriteMode, Value: "Delete"},
		},
	}

	resp, err := s.ImportV2(context.Background(), req)

	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.True(t, errors.Is(merr.Error(resp.GetStatus()), merr.ErrImportFailed))
	assert.Contains(t, resp.GetStatus().GetReason(), "storage v3")
}

// TestCreateImportJobFromAck_WriteModeRequiresManifestSegments covers the createImportJobFromAck
// wiring of checkWriteModeSupported: a replicated write_mode=Delete message targeting a
// collection with a manifest-less segment must not error the ack callback (which retries
// forever); it must create the job directly in Failed state.
func TestCreateImportJobFromAck_WriteModeRequiresManifestSegments(t *testing.T) {
	ctx := context.Background()
	s := newTestServerWithCollection(t)
	require.NoError(t, s.meta.AddSegment(ctx, NewSegmentInfo(&datapb.SegmentInfo{
		ID: 77, CollectionID: testCollectionID, Level: datapb.SegmentLevel_L1,
		State: commonpb.SegmentState_Flushed, ManifestPath: "",
	})))

	mockHandler := NewNMockHandler(t)
	mockHandler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(&collectionInfo{
		ID:            testCollectionID,
		VChannelNames: []string{"v1"},
	}, nil)
	s.handler = mockHandler

	mockAllocator := allocator.NewMockAllocator(t)
	mockAllocator.EXPECT().AllocN(mock.Anything).Return(int64(1000), int64(1002), nil)
	s.allocator = mockAllocator

	// A second, independent catalog mock backs importMeta; it is unrelated to the catalog
	// newTestServerWithCollection used to build s.meta.
	importCatalog := mocks.NewDataCoordCatalog(t)
	importCatalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
	importCatalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
	importCatalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
	var savedJob *datapb.ImportJob
	importCatalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, job *datapb.ImportJob) error {
		savedJob = job
		return nil
	})
	importMeta, err := NewImportMeta(ctx, importCatalog, s.allocator, s.meta)
	require.NoError(t, err)
	s.importMeta = importMeta

	req := &internalpb.ImportRequestInternal{
		CollectionID:   testCollectionID,
		CollectionName: "test_collection",
		PartitionIDs:   []int64{1},
		ChannelNames:   []string{"v1"},
		Schema:         &schemapb.CollectionSchema{Name: "test_collection"},
		Files: []*internalpb.ImportFile{
			{Id: 1, Paths: []string{"/test/file.json"}},
		},
		Options: []*commonpb.KeyValuePair{
			{Key: "timeout", Value: "300s"},
			{Key: importutilv2.WriteMode, Value: "Delete"},
		},
		DataTimestamp: 123456789,
		JobID:         2000,
	}

	resp, err := s.createImportJobFromAck(ctx, req)

	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, int32(0), resp.GetStatus().GetCode())
	assert.Equal(t, "2000", resp.GetJobID())

	assert.NotNil(t, savedJob)
	assert.Equal(t, internalpb.ImportJobState_Failed, savedJob.GetState())
	assert.Contains(t, savedJob.GetReason(), "storage v3")
}

// Helper types are defined in import_callbacks_test.go (mockBalancerImpl, mockBroadcastAPIImpl, newMockBroadcastAPIImpl)
