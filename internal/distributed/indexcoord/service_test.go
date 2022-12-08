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

package grpcindexcoord

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus/internal/indexcoord"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

func TestIndexCoordinateServer(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	factory := dependency.NewDefaultFactory(true)
	server, err := NewServer(ctx, factory)
	assert.NoError(t, err)
	assert.NotNil(t, server)

	indexCoordClient := indexcoord.NewIndexCoordMock()
	err = server.SetClient(indexCoordClient)
	assert.NoError(t, err)

	rcm := indexcoord.NewRootCoordMock()
	server.rootCoord = rcm
	dcm := indexcoord.NewDataCoordMock()
	server.dataCoord = dcm
	err = server.Run()
	assert.NoError(t, err)

	t.Run("GetComponentStates", func(t *testing.T) {
		req := &milvuspb.GetComponentStatesRequest{}
		states, err := server.GetComponentStates(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.StateCode_Healthy, states.State.StateCode)
	})

	t.Run("GetStatisticsChannel", func(t *testing.T) {
		req := &internalpb.GetStatisticsChannelRequest{}
		resp, err := server.GetStatisticsChannel(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("CreateIndex", func(t *testing.T) {
		req := &indexpb.CreateIndexRequest{
			CollectionID: 0,
			FieldID:      0,
			IndexName:    "index",
		}
		resp, err := server.CreateIndex(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("GetIndexState", func(t *testing.T) {
		req := &indexpb.GetIndexStateRequest{
			CollectionID: 0,
			IndexName:    "index",
		}
		resp, err := server.GetIndexState(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.IndexState_Finished, resp.State)
	})

	t.Run("GetSegmentIndexState", func(t *testing.T) {
		req := &indexpb.GetSegmentIndexStateRequest{
			CollectionID: 1,
			IndexName:    "index",
			SegmentIDs:   []UniqueID{1, 2},
		}
		resp, err := server.GetSegmentIndexState(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, len(req.SegmentIDs), len(resp.States))
	})

	t.Run("GetIndexInfos", func(t *testing.T) {
		req := &indexpb.GetIndexInfoRequest{
			CollectionID: 0,
			SegmentIDs:   []UniqueID{0},
			IndexName:    "index",
		}
		resp, err := server.GetIndexInfos(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, len(req.SegmentIDs), len(resp.SegmentInfo))
	})

	t.Run("DescribeIndex", func(t *testing.T) {
		req := &indexpb.DescribeIndexRequest{
			CollectionID: 1,
			IndexName:    "",
		}
		resp, err := server.DescribeIndex(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(resp.IndexInfos))
	})

	t.Run("GetIndexBuildProgress", func(t *testing.T) {
		req := &indexpb.GetIndexBuildProgressRequest{
			CollectionID: 1,
			IndexName:    "default",
		}
		resp, err := server.GetIndexBuildProgress(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.Equal(t, resp.TotalRows, resp.IndexedRows)
	})

	t.Run("DropIndex", func(t *testing.T) {
		req := &indexpb.DropIndexRequest{
			CollectionID: 0,
			IndexName:    "default",
		}
		resp, err := server.DropIndex(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("ShowConfigurations", func(t *testing.T) {
		req := &internalpb.ShowConfigurationsRequest{
			Pattern: "",
		}
		resp, err := server.ShowConfigurations(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("GetMetrics", func(t *testing.T) {
		req := &milvuspb.GetMetricsRequest{
			Request: "",
		}
		resp, err := server.GetMetrics(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.Equal(t, typeutil.IndexCoordRole, resp.ComponentName)
	})

	t.Run("CheckHealth", func(t *testing.T) {
		ret, err := server.CheckHealth(ctx, nil)
		assert.Nil(t, err)
		assert.Equal(t, true, ret.IsHealthy)
	})

	err = server.Stop()
	assert.NoError(t, err)
}
