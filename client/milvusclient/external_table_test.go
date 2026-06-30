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
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/internal/merr"
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
		field.IndexParams = map[string]string{
			"index_type":  "SPARSE_INVERTED_INDEX",
			"metric_type": "BM25",
		}
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
		require.Equal(t, field.IndexParams, entity.KvPairsMap(add.GetFieldInfos()[0].GetExtraParams()))
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
		require.True(t, drop.GetDropFunctionOutputFields())
	})
}

type alterCollectionSchemaClient struct{ milvuspb.MilvusServiceClient }

func TestClientAlterCollectionSchema(t *testing.T) {
	field := entity.NewField().
		WithName("bm25_sparse").
		WithDataType(entity.FieldTypeSparseVector)
	fn := entity.NewFunction().
		WithName("bm25_fn").
		WithInputFields("text").
		WithOutputFields("bm25_sparse").
		WithType(entity.FunctionTypeBM25)

	fake := &alterCollectionSchemaClient{}
	var lastReq *milvuspb.AlterCollectionSchemaRequest
	mockAlter := mockey.Mock((*alterCollectionSchemaClient).AlterCollectionSchema).
		To(func(_ *alterCollectionSchemaClient, _ context.Context, req *milvuspb.AlterCollectionSchemaRequest, _ ...grpc.CallOption) (*milvuspb.AlterCollectionSchemaResponse, error) {
			lastReq = req
			return &milvuspb.AlterCollectionSchemaResponse{AlterStatus: merr.Success()}, nil
		}).Build()
	defer mockAlter.UnPatch()
	client := &Client{service: fake, collCache: NewCollectionCache(nil)}
	client.collCache.collections.Insert("external_coll", &entity.Collection{})
	err := client.AlterCollectionSchema(
		context.Background(),
		NewAlterCollectionSchemaAddFunctionOption("external_coll", fn, field),
	)
	require.NoError(t, err)
	require.NotNil(t, lastReq)
	require.Equal(t, "external_coll", lastReq.GetCollectionName())
	require.NotNil(t, lastReq.GetAction().GetAddRequest())
	_, cached := client.collCache.collections.Get("external_coll")
	require.False(t, cached)
}

func TestClientAlterCollectionSchemaFailure(t *testing.T) {
	fake := &alterCollectionSchemaClient{}
	resp := &milvuspb.AlterCollectionSchemaResponse{
		AlterStatus: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_IllegalArgument,
			Reason:    "bad alter request",
		},
	}
	var rpcErr error
	mockAlter := mockey.Mock((*alterCollectionSchemaClient).AlterCollectionSchema).
		To(func(_ *alterCollectionSchemaClient, _ context.Context, _ *milvuspb.AlterCollectionSchemaRequest, _ ...grpc.CallOption) (*milvuspb.AlterCollectionSchemaResponse, error) {
			return resp, rpcErr
		}).Build()
	defer mockAlter.UnPatch()
	client := &Client{service: fake, collCache: NewCollectionCache(nil)}
	client.collCache.collections.Insert("external_coll", &entity.Collection{})

	err := client.AlterCollectionSchema(
		context.Background(),
		NewAlterCollectionSchemaDropFunctionOption("external_coll", "bm25_fn"),
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "bad alter request")
	_, cached := client.collCache.collections.Get("external_coll")
	require.True(t, cached)

	resp = nil
	rpcErr = status.Error(codes.Internal, "rpc failure")
	err = client.AlterCollectionSchema(
		context.Background(),
		NewAlterCollectionSchemaDropFunctionOption("external_coll", "bm25_fn"),
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "rpc failure")
	_, cached = client.collCache.collections.Get("external_coll")
	require.True(t, cached)
}
