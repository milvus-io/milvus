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

package grpcindexcoordclient

import (
	"context"
	"testing"

	grpcindexcoord "github.com/milvus-io/milvus/internal/distributed/indexcoord"
	"github.com/milvus-io/milvus/internal/indexcoord"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/stretchr/testify/assert"
)

func TestIndexCoordClient(t *testing.T) {
	ctx := context.Background()
	server, err := grpcindexcoord.NewServer(ctx)
	assert.Nil(t, err)
	icm := &indexcoord.Mock{}
	err = server.SetClient(icm)
	assert.Nil(t, err)

	err = server.Run()
	assert.Nil(t, err)

	icc, err := NewClient(ctx, indexcoord.Params.MetaRootPath, indexcoord.Params.EtcdEndpoints)
	assert.Nil(t, err)
	assert.NotNil(t, icc)

	err = icc.Init()
	assert.Nil(t, err)

	err = icc.Register()
	assert.Nil(t, err)

	err = icc.Start()
	assert.Nil(t, err)

	t.Run("GetComponentStates", func(t *testing.T) {
		states, err := icc.GetComponentStates(ctx)
		assert.Nil(t, err)
		assert.Equal(t, internalpb.StateCode_Healthy, states.State.StateCode)
		assert.Equal(t, commonpb.ErrorCode_Success, states.Status.ErrorCode)
	})

	t.Run("GetTimeTickChannel", func(t *testing.T) {
		resp, err := icc.GetTimeTickChannel(ctx)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("GetStatisticsChannel", func(t *testing.T) {
		resp, err := icc.GetStatisticsChannel(ctx)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("BuildIndex", func(t *testing.T) {
		req := &indexpb.BuildIndexRequest{
			IndexBuildID: 0,
			IndexID:      0,
		}
		resp, err := icc.BuildIndex(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("DropIndex", func(t *testing.T) {
		req := &indexpb.DropIndexRequest{
			IndexID: 0,
		}
		resp, err := icc.DropIndex(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("GetIndexStates", func(t *testing.T) {
		req := &indexpb.GetIndexStatesRequest{
			IndexBuildIDs: []int64{0},
		}
		resp, err := icc.GetIndexStates(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.Equal(t, len(req.IndexBuildIDs), len(resp.States))
		assert.Equal(t, commonpb.IndexState_Finished, resp.States[0].State)
	})

	t.Run("GetIndexFilePaths", func(t *testing.T) {
		req := &indexpb.GetIndexFilePathsRequest{
			IndexBuildIDs: []int64{0},
		}
		resp, err := icc.GetIndexFilePaths(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.Equal(t, len(req.IndexBuildIDs), len(resp.FilePaths))
	})

	t.Run("GetMetrics", func(t *testing.T) {
		req := &milvuspb.GetMetricsRequest{}
		resp, err := icc.GetMetrics(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	err = server.Stop()
	assert.Nil(t, err)

	err = icc.Stop()
	assert.Nil(t, err)
}
