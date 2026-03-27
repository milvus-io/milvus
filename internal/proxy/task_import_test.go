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
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

// Note: mockey is not used in this file since we use testify/mock for generated mocks

// ================================
// ImportTask Test Suite
// ================================

type ImportTaskSuite struct {
	suite.Suite
}

func TestImportTaskSuite(t *testing.T) {
	suite.Run(t, new(ImportTaskSuite))
}

// --------------------------------
// Execute Tests
// --------------------------------

func (s *ImportTaskSuite) TestExecute_GetDatabaseInfoFailsReturnsError() {
	ctx := context.Background()

	// Mock globalMetaCache.GetDatabaseInfo to fail
	mockCache := NewMockCache(s.T())
	mockCache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(nil, errors.New("database not found"))

	oldCache := globalMetaCache
	globalMetaCache = mockCache
	defer func() { globalMetaCache = oldCache }()

	task := &importTask{
		ctx: ctx,
		req: &internalpb.ImportRequest{
			DbName:         "test_db",
			CollectionName: "test_collection",
		},
		resp: &internalpb.ImportResponse{},
	}

	err := task.Execute(ctx)

	s.Error(err)
	s.Contains(err.Error(), "database not found")
}

func (s *ImportTaskSuite) TestExecute_ImportV2RPCFailsReturnsError() {
	ctx := context.Background()

	// Mock globalMetaCache.GetDatabaseInfo to succeed
	mockCache := NewMockCache(s.T())
	mockCache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(&databaseInfo{
		dbID: 1,
	}, nil)

	oldCache := globalMetaCache
	globalMetaCache = mockCache
	defer func() { globalMetaCache = oldCache }()

	// Mock MixCoordClient to return RPC error
	mockMixCoord := mocks.NewMockMixCoordClient(s.T())
	mockMixCoord.EXPECT().ImportV2(mock.Anything, mock.Anything).Return(nil, errors.New("rpc error"))

	task := &importTask{
		ctx:      ctx,
		mixCoord: mockMixCoord,
		req: &internalpb.ImportRequest{
			DbName:         "test_db",
			CollectionName: "test_collection",
			Files: []*internalpb.ImportFile{
				{Id: 1, Paths: []string{"/test/file.json"}},
			},
		},
		collectionID: 100,
		partitionIDs: []int64{1},
		vchannels:    []string{"v1"},
		schema: &schemaInfo{
			CollectionSchema: &schemapb.CollectionSchema{
				Name: "test_collection",
			},
		},
		resp: &internalpb.ImportResponse{},
	}

	err := task.Execute(ctx)

	s.Error(err)
	s.Contains(err.Error(), "rpc error")
}

func (s *ImportTaskSuite) TestExecute_ImportV2ReturnsErrorStatusReturnsError() {
	ctx := context.Background()

	// Mock globalMetaCache.GetDatabaseInfo to succeed
	mockCache := NewMockCache(s.T())
	mockCache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(&databaseInfo{
		dbID: 1,
	}, nil)

	oldCache := globalMetaCache
	globalMetaCache = mockCache
	defer func() { globalMetaCache = oldCache }()

	// Mock MixCoordClient to return error status
	mockMixCoord := mocks.NewMockMixCoordClient(s.T())
	mockMixCoord.EXPECT().ImportV2(mock.Anything, mock.Anything).Return(&internalpb.ImportResponse{
		Status: merr.Status(merr.WrapErrImportFailed("validation failed")),
	}, nil)

	task := &importTask{
		ctx:      ctx,
		mixCoord: mockMixCoord,
		req: &internalpb.ImportRequest{
			DbName:         "test_db",
			CollectionName: "test_collection",
			Files: []*internalpb.ImportFile{
				{Id: 1, Paths: []string{"/test/file.json"}},
			},
		},
		collectionID: 100,
		partitionIDs: []int64{1},
		vchannels:    []string{"v1"},
		schema: &schemaInfo{
			CollectionSchema: &schemapb.CollectionSchema{
				Name: "test_collection",
			},
		},
		resp: &internalpb.ImportResponse{},
	}

	err := task.Execute(ctx)

	s.Error(err)
	s.True(errors.Is(err, merr.ErrImportFailed))
}

func (s *ImportTaskSuite) TestExecute_SuccessSetsJobID() {
	ctx := context.Background()

	// Mock globalMetaCache.GetDatabaseInfo to succeed
	mockCache := NewMockCache(s.T())
	mockCache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(&databaseInfo{
		dbID: 1,
	}, nil)

	oldCache := globalMetaCache
	globalMetaCache = mockCache
	defer func() { globalMetaCache = oldCache }()

	// Mock MixCoordClient to return success
	mockMixCoord := mocks.NewMockMixCoordClient(s.T())
	mockMixCoord.EXPECT().ImportV2(mock.Anything, mock.Anything).Return(&internalpb.ImportResponse{
		Status: merr.Success(),
		JobID:  "12345",
	}, nil)

	resp := &internalpb.ImportResponse{}
	task := &importTask{
		ctx:      ctx,
		mixCoord: mockMixCoord,
		req: &internalpb.ImportRequest{
			DbName:         "test_db",
			CollectionName: "test_collection",
			Files: []*internalpb.ImportFile{
				{Id: 1, Paths: []string{"/test/file.json"}},
			},
		},
		collectionID: 100,
		partitionIDs: []int64{1},
		vchannels:    []string{"v1"},
		schema: &schemaInfo{
			CollectionSchema: &schemapb.CollectionSchema{
				Name: "test_collection",
			},
		},
		resp: resp,
	}

	err := task.Execute(ctx)

	s.NoError(err)
	s.Equal("12345", resp.JobID)
}

func (s *ImportTaskSuite) TestExecute_PassesCorrectRequestParameters() {
	ctx := context.Background()

	// Mock globalMetaCache.GetDatabaseInfo to succeed
	mockCache := NewMockCache(s.T())
	mockCache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(&databaseInfo{
		dbID: 42,
	}, nil)

	oldCache := globalMetaCache
	globalMetaCache = mockCache
	defer func() { globalMetaCache = oldCache }()

	// Capture the request to verify parameters
	var capturedReq *internalpb.ImportRequestInternal
	mockMixCoord := mocks.NewMockMixCoordClient(s.T())
	mockMixCoord.EXPECT().ImportV2(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *internalpb.ImportRequestInternal, opts ...grpc.CallOption) (*internalpb.ImportResponse, error) {
			capturedReq = req
			return &internalpb.ImportResponse{
				Status: merr.Success(),
				JobID:  "12345",
			}, nil
		})

	task := &importTask{
		ctx:      ctx,
		mixCoord: mockMixCoord,
		req: &internalpb.ImportRequest{
			DbName:         "test_db",
			CollectionName: "test_collection",
			Files: []*internalpb.ImportFile{
				{Id: 1, Paths: []string{"/test/file.json"}},
			},
			Options: []*commonpb.KeyValuePair{
				{Key: "timeout", Value: "300s"},
			},
		},
		collectionID: 100,
		partitionIDs: []int64{1, 2},
		vchannels:    []string{"v1", "v2"},
		schema: &schemaInfo{
			CollectionSchema: &schemapb.CollectionSchema{
				Name: "test_collection",
			},
		},
		resp: &internalpb.ImportResponse{},
	}

	err := task.Execute(ctx)

	s.NoError(err)
	s.NotNil(capturedReq)
	s.Equal(int64(42), capturedReq.DbID)
	s.Equal(int64(100), capturedReq.CollectionID)
	s.Equal("test_collection", capturedReq.CollectionName)
	s.Equal([]int64{1, 2}, capturedReq.PartitionIDs)
	s.Equal([]string{"v1", "v2"}, capturedReq.ChannelNames)
	s.Equal(uint64(0), capturedReq.DataTimestamp) // Must be 0 for proxy call
	s.Equal(int64(0), capturedReq.JobID)          // Let DataCoord allocate
}

// --------------------------------
// GetImportFiles Tests
// --------------------------------

func (s *ImportTaskSuite) TestGetImportFiles_ConvertsCorrectly() {
	internals := []*internalpb.ImportFile{
		{Id: 1, Paths: []string{"/test/file1.json"}},
		{Id: 2, Paths: []string{"/test/file2.json", "/test/file2_part2.json"}},
		{Id: 3, Paths: []string{}},
	}

	result := GetImportFiles(internals)

	s.Len(result, 3)
	s.Equal(int64(1), result[0].Id)
	s.Equal([]string{"/test/file1.json"}, result[0].Paths)
	s.Equal(int64(2), result[1].Id)
	s.Equal([]string{"/test/file2.json", "/test/file2_part2.json"}, result[1].Paths)
	s.Equal(int64(3), result[2].Id)
	s.Empty(result[2].Paths)
}

func (s *ImportTaskSuite) TestGetImportFiles_EmptyInput() {
	result := GetImportFiles([]*internalpb.ImportFile{})
	s.Empty(result)
}

func (s *ImportTaskSuite) TestGetImportFiles_NilInput() {
	result := GetImportFiles(nil)
	s.Empty(result)
}

// --------------------------------
// Basic Task Methods Tests
// --------------------------------

func (s *ImportTaskSuite) TestTaskBasicMethods() {
	ctx := context.Background()
	task := &importTask{
		ctx:    ctx,
		msgID:  123,
		taskTS: 456,
	}

	s.Equal(ctx, task.TraceCtx())
	s.Equal(UniqueID(123), task.ID())

	task.SetID(789)
	s.Equal(UniqueID(789), task.ID())

	s.Equal("ImportTask", task.Name())
	s.Equal(commonpb.MsgType_Import, task.Type())
	s.Equal(Timestamp(456), task.BeginTs())
	s.Equal(Timestamp(456), task.EndTs())

	task.SetTs(999)
	s.Equal(Timestamp(999), task.BeginTs())

	s.NoError(task.OnEnqueue())
	s.NoError(task.PostExecute(ctx))
}

func (s *ImportTaskSuite) TestSetChannels_ReturnsNil() {
	task := &importTask{}
	s.NoError(task.setChannels())
}

func (s *ImportTaskSuite) TestGetChannels_ReturnsNil() {
	task := &importTask{}
	s.Nil(task.getChannels())
}

// --------------------------------
// DataTimestamp Verification Tests
// --------------------------------

func (s *ImportTaskSuite) TestExecute_DataTimestampIsAlwaysZero() {
	ctx := context.Background()

	// This test verifies the critical requirement that DataTimestamp must be 0
	// for proxy calls. This distinguishes proxy calls from ack callbacks.

	mockCache := NewMockCache(s.T())
	mockCache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(&databaseInfo{
		dbID: 1,
	}, nil)

	oldCache := globalMetaCache
	globalMetaCache = mockCache
	defer func() { globalMetaCache = oldCache }()

	var capturedReq *internalpb.ImportRequestInternal
	mockMixCoord := mocks.NewMockMixCoordClient(s.T())
	mockMixCoord.EXPECT().ImportV2(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *internalpb.ImportRequestInternal, opts ...grpc.CallOption) (*internalpb.ImportResponse, error) {
			capturedReq = req
			return &internalpb.ImportResponse{
				Status: merr.Success(),
				JobID:  "12345",
			}, nil
		})

	task := &importTask{
		ctx:      ctx,
		mixCoord: mockMixCoord,
		req: &internalpb.ImportRequest{
			DbName:         "test_db",
			CollectionName: "test_collection",
			Files: []*internalpb.ImportFile{
				{Id: 1, Paths: []string{"/test/file.json"}},
			},
		},
		collectionID: 100,
		partitionIDs: []int64{1},
		vchannels:    []string{"v1"},
		schema: &schemaInfo{
			CollectionSchema: &schemapb.CollectionSchema{
				Name: "test_collection",
			},
		},
		resp: &internalpb.ImportResponse{},
	}

	task.Execute(ctx)

	// Critical assertion: DataTimestamp must be 0 for proxy calls
	s.Equal(uint64(0), capturedReq.DataTimestamp,
		"DataTimestamp must be 0 for proxy calls to distinguish from ack callbacks")
	s.Equal(int64(0), capturedReq.JobID,
		"JobID must be 0 to let DataCoord allocate")
}

// --------------------------------
// PreExecute Tests
// --------------------------------

func (s *ImportTaskSuite) TestPreExecute_GetCollectionIDFailsReturnsError() {
	ctx := context.Background()

	// Use NewMockCache which is generated by mockery
	mockCache := NewMockCache(s.T())
	mockCache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(int64(0), errors.New("collection not found"))

	oldCache := globalMetaCache
	globalMetaCache = mockCache
	defer func() { globalMetaCache = oldCache }()

	task := &importTask{
		ctx: ctx,
		req: &internalpb.ImportRequest{
			DbName:         "test_db",
			CollectionName: "test_collection",
		},
	}

	err := task.PreExecute(ctx)

	s.Error(err)
	s.Contains(err.Error(), "collection not found")
}
