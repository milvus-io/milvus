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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proxy/privilege"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
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
	task.metaCache = mockCache

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
	task.metaCache = mockCache

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
	task.metaCache = mockCache

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
	task.metaCache = mockCache

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
	task.metaCache = mockCache

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
	task.metaCache = mockCache

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
	task.metaCache = mockCache

	err := task.PreExecute(ctx)

	s.Error(err)
	s.Contains(err.Error(), "collection not found")
}

// newImportTaskForPreExecute builds an importTask whose dependencies are mocked
// just far enough for PreExecute to reach the option-driven checks. The checks
// under test sit at both ends of PreExecute -- the duplicate-key rejection runs
// before anything is resolved, the privilege gate runs after the schema and
// vchannels are -- so the mocks must satisfy everything in between.
func newImportTaskForPreExecute(t *testing.T, options []*commonpb.KeyValuePair) *importTask {
	mockCache := NewMockCache(t)
	mockCache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).
		Return(int64(100), nil).Maybe()
	mockCache.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything, mock.Anything).
		Return(&schemaInfo{
			CollectionSchema: &schemapb.CollectionSchema{
				Name: "test_collection",
				Fields: []*schemapb.FieldSchema{
					{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				},
			},
		}, nil).Maybe()
	globalMetaCache = mockCache

	// Only reached by the ordinary-import case, which runs past the gate into
	// partition resolution.
	mockCache.EXPECT().GetPartitionID(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(int64(200), nil).Maybe()

	chMgr := NewMockChannelsMgr(t)
	chMgr.EXPECT().getVChannels(mock.Anything).Return([]string{"v1"}, nil).Maybe()

	return &importTask{
		ctx:  context.Background(),
		node: &Proxy{chMgr: chMgr},
		req: &internalpb.ImportRequest{
			DbName:         "test_db",
			CollectionName: "test_collection",
			Files:          []*internalpb.ImportFile{{Id: 1, Paths: []string{"staging/file.json"}}},
			Options:        options,
		},
		resp: &internalpb.ImportResponse{},
	}
}

// TestImportTask_PreExecuteRequiresImportBinlogPrivilege drives the gate through
// PreExecute rather than calling CheckClusterPrivilege directly: the helper
// already has its own coverage in privilege_interceptor_test.go, and what is
// untested is the wiring -- that PreExecute calls it, and only for the options
// that read Milvus's internal storage layout.
func TestImportTask_PreExecuteRequiresImportBinlogPrivilege(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
	paramtable.Get().Save(Params.CommonCfg.RootShouldBindRole.Key, "false")
	defer paramtable.Get().Reset(Params.CommonCfg.AuthorizationEnabled.Key)
	defer paramtable.Get().Reset(Params.CommonCfg.RootShouldBindRole.Key)

	oldCache := globalMetaCache
	defer func() { globalMetaCache = oldCache }()

	// CheckClusterPrivilege resolves roles via privilege.GetPrivilegeCache(), a
	// process-wide singleton normally populated once at Proxy startup. Seed it
	// with an empty policy set (following the same pattern as
	// privilege_interceptor_test.go's InitEmptyGlobalCache) so the "ordinary
	// user" case below reaches the actual privilege decision instead of
	// failing earlier with ErrServiceUnavailable because the cache is nil.
	mixcoord := mocks.NewMockMixCoordClient(t)
	mixcoord.EXPECT().ListPolicy(mock.Anything, mock.Anything, mock.Anything).
		Return(&internalpb.ListPolicyResponse{Status: merr.Success()}, nil)
	require.NoError(t, privilege.InitPrivilegeCache(context.Background(), mixcoord))

	backupOptions := []*commonpb.KeyValuePair{{Key: "backup", Value: "true"}}
	l0Options := []*commonpb.KeyValuePair{{Key: "l0_import", Value: "true"}}

	t.Run("backup import by a user without the privilege is refused", func(t *testing.T) {
		it := newImportTaskForPreExecute(t, backupOptions)
		err := it.PreExecute(GetContext(context.Background(), "alice:123456"))
		assert.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
	})

	t.Run("l0 import by a user without the privilege is refused", func(t *testing.T) {
		it := newImportTaskForPreExecute(t, l0Options)
		err := it.PreExecute(GetContext(context.Background(), "alice:123456"))
		assert.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
	})

	t.Run("root passes the gate", func(t *testing.T) {
		it := newImportTaskForPreExecute(t, backupOptions)
		err := it.PreExecute(GetContext(context.Background(), "root:123456"))
		// A backup import still fails afterwards on the unset partition name;
		// what matters here is that it is no longer the privilege that refuses it.
		assert.NotErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
	})

	t.Run("ordinary import does not require the privilege", func(t *testing.T) {
		it := newImportTaskForPreExecute(t, nil)
		err := it.PreExecute(GetContext(context.Background(), "alice:123456"))
		assert.NotErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
	})
}

// TestImportTask_PreExecuteRejectsDuplicateOptionKeys pins the same wiring for
// the duplicate-key check. It must reject before any option is read, because
// validation reads options as a repeated KV (first match wins) while the
// broadcast body folds them into a map (last value wins).
func TestImportTask_PreExecuteRejectsDuplicateOptionKeys(t *testing.T) {
	paramtable.Init()

	oldCache := globalMetaCache
	defer func() { globalMetaCache = oldCache }()

	it := newImportTaskForPreExecute(t, []*commonpb.KeyValuePair{
		{Key: "backup", Value: "false"},
		{Key: "backup", Value: "true"},
	})

	err := it.PreExecute(context.Background())

	assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	assert.Contains(t, err.Error(), "duplicate import option key: backup")
}
