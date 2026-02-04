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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/client/v2/entity"
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
