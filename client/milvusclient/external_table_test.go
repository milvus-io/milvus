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

package milvusclient

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestRefreshExternalCollectionOption(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		opt := NewRefreshExternalCollectionOption("test_collection")
		req := opt.Request()

		assert.Equal(t, "test_collection", req.GetCollectionName())
		assert.Empty(t, req.GetExternalSource())
		assert.Empty(t, req.GetExternalSpec())
	})

	t.Run("with_external_source", func(t *testing.T) {
		opt := NewRefreshExternalCollectionOption("test_collection").
			WithExternalSource("s3://bucket/path")
		req := opt.Request()

		assert.Equal(t, "test_collection", req.GetCollectionName())
		assert.Equal(t, "s3://bucket/path", req.GetExternalSource())
	})

	t.Run("with_external_spec", func(t *testing.T) {
		opt := NewRefreshExternalCollectionOption("test_collection").
			WithExternalSpec("iceberg")
		req := opt.Request()

		assert.Equal(t, "test_collection", req.GetCollectionName())
		assert.Equal(t, "iceberg", req.GetExternalSpec())
	})

	t.Run("with_all_options", func(t *testing.T) {
		opt := NewRefreshExternalCollectionOption("test_collection").
			WithExternalSource("s3://bucket/path").
			WithExternalSpec("iceberg")
		req := opt.Request()

		assert.Equal(t, "test_collection", req.GetCollectionName())
		assert.Equal(t, "s3://bucket/path", req.GetExternalSource())
		assert.Equal(t, "iceberg", req.GetExternalSpec())
	})
}

func TestGetRefreshExternalCollectionProgressOption(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		opt := NewGetRefreshExternalCollectionProgressOption(123)
		req := opt.Request()

		assert.Equal(t, int64(123), req.GetJobId())
	})
}

func TestListRefreshExternalCollectionJobsOption(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		opt := NewListRefreshExternalCollectionJobsOption("test_collection")
		req := opt.Request()

		assert.Equal(t, "test_collection", req.GetCollectionName())
	})
}

func TestConvertToEntityJobInfo(t *testing.T) {
	t.Run("nil_input", func(t *testing.T) {
		result := convertToEntityJobInfo(nil)
		assert.Nil(t, result)
	})

	t.Run("valid_input", func(t *testing.T) {
		info := &milvuspb.RefreshExternalCollectionJobInfo{
			JobId:          123,
			CollectionName: "test_collection",
			State:          milvuspb.RefreshExternalCollectionState_RefreshInProgress,
			Progress:       50,
			Reason:         "",
			ExternalSource: "s3://bucket/path",
			ExternalSpec:   `{"format":"parquet"}`,
			StartTime:      1000,
			EndTime:        0,
		}

		result := convertToEntityJobInfo(info)
		assert.NotNil(t, result)
		assert.Equal(t, int64(123), result.JobID)
		assert.Equal(t, "test_collection", result.CollectionName)
		assert.Equal(t, entity.RefreshStateInProgress, result.State)
		assert.Equal(t, int64(50), result.Progress)
		assert.Equal(t, "", result.Reason)
		assert.Equal(t, "s3://bucket/path", result.ExternalSource)
		assert.Equal(t, `{"format":"parquet"}`, result.ExternalSpec)
		assert.Equal(t, int64(1000), result.StartTime)
		assert.Equal(t, int64(0), result.EndTime)
	})

	t.Run("completed_state", func(t *testing.T) {
		info := &milvuspb.RefreshExternalCollectionJobInfo{
			JobId:    123,
			State:    milvuspb.RefreshExternalCollectionState_RefreshCompleted,
			Progress: 100,
			EndTime:  2000,
		}

		result := convertToEntityJobInfo(info)
		assert.NotNil(t, result)
		assert.Equal(t, entity.RefreshStateCompleted, result.State)
		assert.Equal(t, int64(100), result.Progress)
		assert.Equal(t, int64(2000), result.EndTime)
	})

	t.Run("failed_state", func(t *testing.T) {
		info := &milvuspb.RefreshExternalCollectionJobInfo{
			JobId:    123,
			State:    milvuspb.RefreshExternalCollectionState_RefreshFailed,
			Progress: 30,
			Reason:   "connection timeout",
			EndTime:  2000,
		}

		result := convertToEntityJobInfo(info)
		assert.NotNil(t, result)
		assert.Equal(t, entity.RefreshStateFailed, result.State)
		assert.Equal(t, int64(30), result.Progress)
		assert.Equal(t, "connection timeout", result.Reason)
	})

	t.Run("pending_state", func(t *testing.T) {
		info := &milvuspb.RefreshExternalCollectionJobInfo{
			JobId: 123,
			State: milvuspb.RefreshExternalCollectionState_RefreshPending,
		}

		result := convertToEntityJobInfo(info)
		assert.NotNil(t, result)
		assert.Equal(t, entity.RefreshStatePending, result.State)
	})
}

func TestRefreshExternalCollectionOption_Updates(t *testing.T) {
	t.Run("update_external_source", func(t *testing.T) {
		opt := NewRefreshExternalCollectionOption("test_collection").
			WithExternalSource("s3://new-bucket")
		req := opt.Request()
		assert.Equal(t, "s3://new-bucket", req.GetExternalSource())
	})

	t.Run("update_external_spec", func(t *testing.T) {
		opt := NewRefreshExternalCollectionOption("test_collection").
			WithExternalSpec("new-spec")
		req := opt.Request()
		assert.Equal(t, "new-spec", req.GetExternalSpec())
	})
}

func TestGetRefreshExternalCollectionProgressOption_Validation(t *testing.T) {
	t.Run("different_job_ids", func(t *testing.T) {
		opt1 := NewGetRefreshExternalCollectionProgressOption(123)
		opt2 := NewGetRefreshExternalCollectionProgressOption(456)

		assert.NotEqual(t, opt1.Request().GetJobId(), opt2.Request().GetJobId())
		assert.Equal(t, int64(123), opt1.Request().GetJobId())
		assert.Equal(t, int64(456), opt2.Request().GetJobId())
	})
}

func TestListRefreshExternalCollectionJobsOption_Validation(t *testing.T) {
	t.Run("different_collections", func(t *testing.T) {
		opt1 := NewListRefreshExternalCollectionJobsOption("collection_1")
		opt2 := NewListRefreshExternalCollectionJobsOption("collection_2")

		assert.NotEqual(t, opt1.Request().GetCollectionName(), opt2.Request().GetCollectionName())
		assert.Equal(t, "collection_1", opt1.Request().GetCollectionName())
		assert.Equal(t, "collection_2", opt2.Request().GetCollectionName())
	})
}

func TestConvertToEntityJobInfo_AllStates(t *testing.T) {
	t.Run("unknown_state", func(t *testing.T) {
		info := &milvuspb.RefreshExternalCollectionJobInfo{
			JobId: 123,
			State: 999, // Invalid state
		}

		result := convertToEntityJobInfo(info)
		assert.NotNil(t, result)
		assert.Equal(t, int64(123), result.JobID)
	})

	t.Run("with_collection_info", func(t *testing.T) {
		info := &milvuspb.RefreshExternalCollectionJobInfo{
			JobId:          123,
			CollectionName: "test_collection",
			State:          milvuspb.RefreshExternalCollectionState_RefreshInProgress,
			Progress:       75,
		}

		result := convertToEntityJobInfo(info)
		assert.NotNil(t, result)
		assert.Equal(t, int64(123), result.JobID)
		assert.Equal(t, "test_collection", result.CollectionName)
		assert.Equal(t, int64(75), result.Progress)
	})
}

func TestAlterCollectionSchemaOption(t *testing.T) {
	t.Run("add_function", func(t *testing.T) {
		field := entity.NewField().
			WithName("bm25_sparse").
			WithDataType(entity.FieldTypeSparseVector)
		fn := entity.NewFunction().
			WithName("bm25_fn").
			WithInputFields("text").
			WithOutputFields("bm25_sparse").
			WithType(entity.FunctionTypeBM25)

		req := NewAlterCollectionSchemaAddFunctionOption("external_coll", fn, field).
			WithDbName("default").
			Request()

		assert.Equal(t, "default", req.GetDbName())
		assert.Equal(t, "external_coll", req.GetCollectionName())
		add := req.GetAction().GetAddRequest()
		require.NotNil(t, add)
		require.False(t, add.GetDoPhysicalBackfill())
		require.Len(t, add.GetFieldInfos(), 1)
		require.Equal(t, "bm25_sparse", add.GetFieldInfos()[0].GetFieldSchema().GetName())
		require.Equal(t, schemapb.DataType_SparseFloatVector, add.GetFieldInfos()[0].GetFieldSchema().GetDataType())
		require.Len(t, add.GetFuncSchema(), 1)
		require.Equal(t, "bm25_fn", add.GetFuncSchema()[0].GetName())
		require.Equal(t, []string{"text"}, add.GetFuncSchema()[0].GetInputFieldNames())
		require.Equal(t, []string{"bm25_sparse"}, add.GetFuncSchema()[0].GetOutputFieldNames())
	})

	t.Run("add_field", func(t *testing.T) {
		field := entity.NewField().
			WithName("category").
			WithDataType(entity.FieldTypeVarChar).
			WithMaxLength(1024).
			WithExternalField("text_zh").
			WithNullable(true)

		req := NewAlterCollectionSchemaAddFieldOption("external_coll", field).
			WithDbName("default").
			Request()

		assert.Equal(t, "default", req.GetDbName())
		assert.Equal(t, "external_coll", req.GetCollectionName())
		add := req.GetAction().GetAddRequest()
		require.NotNil(t, add)
		require.False(t, add.GetDoPhysicalBackfill())
		require.Empty(t, add.GetFuncSchema())
		require.Len(t, add.GetFieldInfos(), 1)
		fieldSchema := add.GetFieldInfos()[0].GetFieldSchema()
		require.NotNil(t, fieldSchema)
		require.Equal(t, "category", fieldSchema.GetName())
		require.Equal(t, schemapb.DataType_VarChar, fieldSchema.GetDataType())
		require.Equal(t, "text_zh", fieldSchema.GetExternalField())
		require.True(t, fieldSchema.GetNullable())
	})

	t.Run("drop_field", func(t *testing.T) {
		req := NewAlterCollectionSchemaDropFieldOption("external_coll", "obsolete").Request()
		drop := req.GetAction().GetDropRequest()
		require.NotNil(t, drop)
		require.Equal(t, "obsolete", drop.GetFieldName())
	})

	t.Run("drop_function", func(t *testing.T) {
		req := NewAlterCollectionSchemaDropFunctionOption("external_coll", "bm25_fn").Request()
		drop := req.GetAction().GetDropRequest()
		require.NotNil(t, drop)
		require.Equal(t, "bm25_fn", drop.GetFunctionName())
		require.False(t, drop.GetDropFunctionOutputFields())
	})

	t.Run("drop_function_output_fields", func(t *testing.T) {
		req := NewAlterCollectionSchemaDropFunctionOption("external_coll", "bm25_fn").
			WithDropOutputFields(true).
			Request()
		drop := req.GetAction().GetDropRequest()
		require.NotNil(t, drop)
		require.Equal(t, "bm25_fn", drop.GetFunctionName())
		require.True(t, drop.GetDropFunctionOutputFields())
	})
}

type alterCollectionSchemaTestServer struct {
	milvuspb.UnimplementedMilvusServiceServer
	lastReq *milvuspb.AlterCollectionSchemaRequest
	resp    *milvuspb.AlterCollectionSchemaResponse
	err     error
}

// AlterCollectionSchema captures alter schema requests for client wrapper tests.
func (s *alterCollectionSchemaTestServer) AlterCollectionSchema(
	ctx context.Context,
	req *milvuspb.AlterCollectionSchemaRequest,
) (*milvuspb.AlterCollectionSchemaResponse, error) {
	s.lastReq = req
	return s.resp, s.err
}

// newAlterCollectionSchemaTestClient creates a bufconn-backed client and cleanup hook.
func newAlterCollectionSchemaTestClient(
	t *testing.T,
	server *alterCollectionSchemaTestServer,
) (*Client, func()) {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	milvuspb.RegisterMilvusServiceServer(grpcServer, server)
	go func() {
		_ = grpcServer.Serve(listener)
	}()

	dialer := func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}
	conn, err := grpc.DialContext(
		context.Background(),
		"bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)

	client := &Client{service: milvuspb.NewMilvusServiceClient(conn)}
	cleanup := func() {
		_ = conn.Close()
		grpcServer.Stop()
		_ = listener.Close()
	}
	return client, cleanup
}

func TestClientAlterCollectionSchema(t *testing.T) {
	server := &alterCollectionSchemaTestServer{
		resp: &milvuspb.AlterCollectionSchemaResponse{AlterStatus: merr.Success()},
	}
	client, cleanup := newAlterCollectionSchemaTestClient(t, server)
	defer cleanup()

	field := entity.NewField().
		WithName("bm25_sparse").
		WithDataType(entity.FieldTypeSparseVector)
	fn := entity.NewFunction().
		WithName("bm25_fn").
		WithInputFields("text").
		WithOutputFields("bm25_sparse").
		WithType(entity.FunctionTypeBM25)

	err := client.AlterCollectionSchema(
		context.Background(),
		NewAlterCollectionSchemaAddFunctionOption("external_coll", fn, field),
	)
	require.NoError(t, err)
	require.NotNil(t, server.lastReq)
	require.Equal(t, "external_coll", server.lastReq.GetCollectionName())
	require.NotNil(t, server.lastReq.GetAction().GetAddRequest())
}

func TestClientAlterCollectionSchemaFailure(t *testing.T) {
	server := &alterCollectionSchemaTestServer{
		resp: &milvuspb.AlterCollectionSchemaResponse{
			AlterStatus: merr.Status(merr.WrapErrParameterInvalidMsg("bad alter request")),
		},
	}
	client, cleanup := newAlterCollectionSchemaTestClient(t, server)
	defer cleanup()

	err := client.AlterCollectionSchema(
		context.Background(),
		NewAlterCollectionSchemaDropFunctionOption("external_coll", "bm25_fn"),
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "bad alter request")

	server.resp = nil
	server.err = status.Error(codes.Internal, "rpc failure")
	err = client.AlterCollectionSchema(
		context.Background(),
		NewAlterCollectionSchemaDropFunctionOption("external_coll", "bm25_fn"),
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "rpc failure")
}
