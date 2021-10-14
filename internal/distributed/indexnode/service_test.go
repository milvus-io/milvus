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

package grpcindexnode

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus/internal/indexnode"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/stretchr/testify/assert"
)

func TestIndexNodeServer(t *testing.T) {
	ctx := context.Background()
	server, err := NewServer(ctx)
	assert.Nil(t, err)
	assert.NotNil(t, server)

	inm := &indexnode.Mock{}
	err = server.SetClient(inm)
	assert.Nil(t, err)

	err = server.Run()
	assert.Nil(t, err)

	t.Run("GetComponentStates", func(t *testing.T) {
		req := &internalpb.GetComponentStatesRequest{}
		states, err := server.GetComponentStates(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, internalpb.StateCode_Healthy, states.State.StateCode)
	})

	t.Run("GetTimeTickChannel", func(t *testing.T) {
		req := &internalpb.GetTimeTickChannelRequest{}
		resp, err := server.GetTimeTickChannel(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("GetStatisticsChannel", func(t *testing.T) {
		req := &internalpb.GetStatisticsChannelRequest{}
		resp, err := server.GetStatisticsChannel(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("CreateIndex", func(t *testing.T) {
		req := &indexpb.CreateIndexRequest{
			IndexBuildID: 0,
			IndexID:      0,
			DataPaths:    []string{},
		}
		resp, err := server.CreateIndex(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("GetMetrics", func(t *testing.T) {
		req := &milvuspb.GetMetricsRequest{
			Request: "",
		}
		resp, err := server.GetMetrics(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	err = server.Stop()
	assert.Nil(t, err)
}
