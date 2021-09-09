// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package indexnode

import (
	"context"
	"strconv"
	"testing"

	"github.com/golang/protobuf/proto"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/stretchr/testify/assert"
)

func TestIndexNodeMock(t *testing.T) {
	Params.Init()
	inm := Mock{
		Build: true,
	}
	err := inm.Register()
	assert.Nil(t, err)
	err = inm.Init()
	assert.Nil(t, err)
	err = inm.Start()
	assert.Nil(t, err)
	ctx := context.Background()

	t.Run("GetComponentStates", func(t *testing.T) {
		states, err := inm.GetComponentStates(ctx)
		assert.Nil(t, err)
		assert.Equal(t, internalpb.StateCode_Healthy, states.State.StateCode)
	})

	t.Run("GetTimeTickChannel", func(t *testing.T) {
		resp, err := inm.GetTimeTickChannel(ctx)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("GetStatisticsChannel", func(t *testing.T) {
		resp, err := inm.GetStatisticsChannel(ctx)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("CreateIndex", func(t *testing.T) {
		req := &indexpb.CreateIndexRequest{
			IndexBuildID: 0,
			IndexID:      0,
			DataPaths:    []string{},
		}
		resp, err := inm.CreateIndex(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("GetMetrics", func(t *testing.T) {
		req := &milvuspb.GetMetricsRequest{
			Request: "",
		}
		resp, err := inm.GetMetrics(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	err = inm.Stop()
	assert.Nil(t, err)
}

func TestIndexNodeMockError(t *testing.T) {
	inm := Mock{
		Failure: false,
		Build:   false,
		Err:     true,
	}

	ctx := context.Background()
	err := inm.Register()
	assert.NotNil(t, err)

	err = inm.Init()
	assert.NotNil(t, err)

	err = inm.Start()
	assert.NotNil(t, err)

	t.Run("GetComponentStates error", func(t *testing.T) {
		resp, err := inm.GetComponentStates(ctx)
		assert.NotNil(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.Status.ErrorCode)
	})

	t.Run("GetStatisticsChannel error", func(t *testing.T) {
		resp, err := inm.GetStatisticsChannel(ctx)
		assert.NotNil(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.Status.ErrorCode)
	})

	t.Run("GetTimeTickChannel error", func(t *testing.T) {
		resp, err := inm.GetTimeTickChannel(ctx)
		assert.NotNil(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.Status.ErrorCode)
	})

	t.Run("CreateIndex error", func(t *testing.T) {
		resp, err := inm.CreateIndex(ctx, &indexpb.CreateIndexRequest{})
		assert.NotNil(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.ErrorCode)
	})

	t.Run("GetMetrics error", func(t *testing.T) {
		req := &milvuspb.GetMetricsRequest{}
		resp, err := inm.GetMetrics(ctx, req)

		assert.NotNil(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.Status.ErrorCode)
	})

	err = inm.Stop()
	assert.NotNil(t, err)
}

func TestIndexNodeMockFiled(t *testing.T) {
	inm := Mock{
		Failure: true,
		Build:   true,
		Err:     false,
	}

	err := inm.Register()
	assert.Nil(t, err)
	err = inm.Init()
	assert.Nil(t, err)
	err = inm.Start()
	assert.Nil(t, err)
	ctx := context.Background()

	t.Run("CreateIndex failed", func(t *testing.T) {
		req := &indexpb.CreateIndexRequest{
			IndexBuildID: 0,
			IndexID:      0,
			DataPaths:    []string{},
		}
		key := "/indexes/" + strconv.FormatInt(10, 10)
		indexMeta := &indexpb.IndexMeta{
			IndexBuildID: 10,
			State:        commonpb.IndexState_InProgress,
			Version:      0,
		}

		value := proto.MarshalTextString(indexMeta)
		err := inm.etcdKV.Save(key, value)
		assert.Nil(t, err)
		resp, err := inm.CreateIndex(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
		err = inm.etcdKV.RemoveWithPrefix(key)
		assert.Nil(t, err)
	})
	t.Run("GetMetrics failed", func(t *testing.T) {
		req := &milvuspb.GetMetricsRequest{}
		resp, err := inm.GetMetrics(ctx, req)

		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.Status.ErrorCode)
	})

	err = inm.Stop()
	assert.Nil(t, err)
}
