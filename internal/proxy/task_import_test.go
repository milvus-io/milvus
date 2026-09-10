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

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proxy/channelmgr"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/interceptor"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestImportTaskPreExecuteTargetPartitions(t *testing.T) {
	paramtable.Init()
	for _, tc := range []struct {
		name         string
		snapshot     bool
		backup       bool
		partitionKey bool
		partition    string
		wantName     string
		wantIDs      []int64
		wantErr      string
	}{
		{name: "snapshot_default", snapshot: true, backup: true, wantName: "_default", wantIDs: []int64{10}},
		{name: "snapshot_named", snapshot: true, backup: true, partition: "custom", wantName: "custom", wantIDs: []int64{20}},
		{name: "snapshot_missing_partition", snapshot: true, backup: true, partition: "missing", wantErr: "partition not found"},
		{name: "snapshot_partition_key", snapshot: true, backup: true, partitionKey: true, wantIDs: []int64{20, 10}},
		{name: "snapshot_partition_key_explicit", snapshot: true, backup: true, partitionKey: true, partition: "_default_0", wantErr: "not allow to set partition name"},
		{name: "legacy_backup_requires_partition", backup: true, wantErr: "partition not specified"},
		{name: "legacy_backup_named", backup: true, partition: "custom", wantName: "custom", wantIDs: []int64{20}},
		{name: "ordinary_default", wantName: "_default", wantIDs: []int64{10}},
		{name: "ordinary_partition_key", partitionKey: true, wantIDs: []int64{20, 10}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			schema := &schemaInfo{CollectionSchema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 101, Name: "part", DataType: schemapb.DataType_Int64, IsPartitionKey: tc.partitionKey},
			}}}
			cache := &MetaCache{}
			idPatch := mockey.Mock((*MetaCache).GetCollectionID).Return(int64(100), nil).Build()
			defer idPatch.UnPatch()
			schemaPatch := mockey.Mock((*MetaCache).GetCollectionSchema).Return(schema, nil).Build()
			defer schemaPatch.UnPatch()
			partitionPatch := mockey.Mock((*MetaCache).GetPartitionID).To(
				func(_ *MetaCache, _ context.Context, _, _, name string) (int64, error) {
					switch name {
					case "_default":
						return 10, nil
					case "custom":
						return 20, nil
					default:
						return 0, errors.New("partition not found")
					}
				}).Build()
			defer partitionPatch.UnPatch()
			// IDs deliberately differ from name order: both import phases must
			// receive the stable partition-key index order, not map or ID order.
			partitionsPatch := mockey.Mock((*MetaCache).GetPartitions).Return(map[string]int64{
				"_default_1": 10, "_default_0": 20,
			}, nil).Build()
			defer partitionsPatch.UnPatch()
			type testChannels struct{ channelmgr.ChannelsMgr }
			channels := &testChannels{}
			channelPatch := mockey.Mock((*testChannels).GetVChannels).Return([]string{"v1"}, nil).Build()
			defer channelPatch.UnPatch()
			req := &internalpb.ImportRequest{
				DbName: "default", CollectionName: "target", PartitionName: tc.partition,
				Files: []*internalpb.ImportFile{{Paths: []string{"data.json"}}},
			}
			if tc.backup {
				req.Options = append(req.Options, &commonpb.KeyValuePair{Key: "backup", Value: "true"})
			}
			if tc.snapshot {
				req.Options = append(req.Options, &commonpb.KeyValuePair{Key: "source_type", Value: "snapshot"})
				req.Files[0].Paths = []string{"s3://source/root/snapshots/1/metadata/2.json"}
			}
			task := &importTask{req: req, node: &Proxy{chMgr: channels}}
			task.MetaCache = cache
			err := task.PreExecute(context.Background())
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantName, req.PartitionName)
			require.Equal(t, tc.wantIDs, task.partitionIDs)
		})
	}
}

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

	// Mock database info lookup to fail
	mockCache := NewMockCache(s.T())
	mockCache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(nil, errors.New("database not found"))
	task := &importTask{
		ctx: ctx,
		req: &internalpb.ImportRequest{
			DbName:         "test_db",
			CollectionName: "test_collection",
		},
		resp: &internalpb.ImportResponse{},
	}
	task.MetaCache = mockCache

	err := task.Execute(ctx)

	s.Error(err)
	s.Contains(err.Error(), "database not found")
}

func (s *ImportTaskSuite) TestExecute_ImportV2RPCFailsReturnsError() {
	ctx := context.Background()

	// Mock database info lookup to succeed
	mockCache := NewMockCache(s.T())
	mockCache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(&databaseInfo{
		DBID: 1,
	}, nil)
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
	task.MetaCache = mockCache

	err := task.Execute(ctx)

	s.Error(err)
	s.Contains(err.Error(), "rpc error")
}

func (s *ImportTaskSuite) TestExecute_ImportV2ReturnsErrorStatusReturnsError() {
	ctx := context.Background()

	// Mock database info lookup to succeed
	mockCache := NewMockCache(s.T())
	mockCache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(&databaseInfo{
		DBID: 1,
	}, nil)
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
	task.MetaCache = mockCache

	err := task.Execute(ctx)

	s.Error(err)
	s.True(errors.Is(err, merr.ErrImportFailed))
}

func (s *ImportTaskSuite) TestExecute_SuccessSetsJobID() {
	ctx := context.Background()

	// Mock database info lookup to succeed
	mockCache := NewMockCache(s.T())
	mockCache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(&databaseInfo{
		DBID: 1,
	}, nil)
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
	task.MetaCache = mockCache

	err := task.Execute(ctx)

	s.NoError(err)
	s.Equal("12345", resp.JobID)
}

func (s *ImportTaskSuite) TestExecute_PassesCorrectRequestParameters() {
	ctx := context.Background()

	// Mock database info lookup to succeed
	mockCache := NewMockCache(s.T())
	mockCache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(&databaseInfo{
		DBID: 42,
	}, nil)
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
	task.MetaCache = mockCache

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
	s.NoError(task.SetChannels())
}

func (s *ImportTaskSuite) TestGetChannels_ReturnsNil() {
	task := &importTask{}
	s.Nil(task.GetChannels())
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
		DBID: 1,
	}, nil)
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
	task.MetaCache = mockCache

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
	task := &importTask{
		ctx: ctx,
		req: &internalpb.ImportRequest{
			DbName:         "test_db",
			CollectionName: "test_collection",
		},
	}
	task.MetaCache = mockCache

	err := task.PreExecute(ctx)

	s.Error(err)
	s.Contains(err.Error(), "collection not found")
}

// The idempotency key rides the gRPC metadata of the context, not the request
// body, so Execute must hand the coordinator client the very context the request
// arrived on. A context rebuilt or detached here would strip the key and the
// client interceptor would have nothing to propagate.
func (s *ImportTaskSuite) TestExecute_PassesTheRequestContextToMixCoord() {
	ctx := metadata.NewIncomingContext(context.Background(),
		metadata.Pairs(util.HeaderIdempotencyKey, "run-1-batch-1"))

	mockCache := NewMockCache(s.T())
	mockCache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(&databaseInfo{
		DBID: 42,
	}, nil)

	var capturedCtx context.Context
	mockMixCoord := mocks.NewMockMixCoordClient(s.T())
	mockMixCoord.EXPECT().ImportV2(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *internalpb.ImportRequestInternal, opts ...grpc.CallOption) (*internalpb.ImportResponse, error) {
			capturedCtx = ctx
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
		},
		collectionID: 100,
		schema: &schemaInfo{
			CollectionSchema: &schemapb.CollectionSchema{
				Name: "test_collection",
			},
		},
		resp: &internalpb.ImportResponse{},
	}
	task.MetaCache = mockCache

	err := task.Execute(ctx)

	s.NoError(err)
	s.Require().NotNil(capturedCtx)
	s.Equal("run-1-batch-1", interceptor.IdempotencyKeyFromContext(capturedCtx))
}
