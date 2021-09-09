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

package grpcindexnodeclient

import (
	"context"
	"testing"

	grpcindexnode "github.com/milvus-io/milvus/internal/distributed/indexnode"
	"github.com/milvus-io/milvus/internal/indexnode"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/stretchr/testify/assert"
)

func TestIndexNodeClient(t *testing.T) {
	ctx := context.Background()

	ins, err := grpcindexnode.NewServer(ctx)
	assert.Nil(t, err)
	assert.NotNil(t, ins)

	inm := &indexnode.Mock{}
	err = ins.SetClient(inm)
	assert.Nil(t, err)

	err = ins.Run()
	assert.Nil(t, err)

	inc, err := NewClient(ctx, "localhost:21121")
	assert.Nil(t, err)
	assert.NotNil(t, inc)

	err = inc.Init()
	assert.Nil(t, err)

	err = inc.Start()
	assert.Nil(t, err)

	t.Run("GetComponentStates", func(t *testing.T) {
		states, err := inc.GetComponentStates(ctx)
		assert.Nil(t, err)
		assert.Equal(t, internalpb.StateCode_Healthy, states.State.StateCode)
		assert.Equal(t, commonpb.ErrorCode_Success, states.Status.ErrorCode)
	})

	t.Run("GetTimeTickChannel", func(t *testing.T) {
		resp, err := inc.GetTimeTickChannel(ctx)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("GetStatisticsChannel", func(t *testing.T) {
		resp, err := inc.GetStatisticsChannel(ctx)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("CreateIndex", func(t *testing.T) {
		req := &indexpb.CreateIndexRequest{
			IndexBuildID: 0,
			IndexID:      0,
		}
		resp, err := inc.CreateIndex(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("GetMetrics", func(t *testing.T) {
		req := &milvuspb.GetMetricsRequest{}
		resp, err := inc.GetMetrics(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	err = ins.Stop()
	assert.Nil(t, err)

	err = inc.Stop()
	assert.Nil(t, err)
}
