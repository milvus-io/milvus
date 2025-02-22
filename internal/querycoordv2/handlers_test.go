// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package querycoordv2

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/tidwall/gjson"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/metricsinfo"
)

func TestGetChannelsFromQueryNode(t *testing.T) {
	mockCluster := session.NewMockCluster(t)
	nodeManager := session.NewNodeManager()
	nodeManager.Add(session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: 1}))
	server := &Server{cluster: mockCluster, nodeMgr: nodeManager}
	req := &milvuspb.GetMetricsRequest{}
	expectedChannels := []*metricsinfo.Channel{
		{
			Name:           "channel1",
			WatchState:     "Healthy",
			LatestTimeTick: "1",
			NodeID:         int64(1),
			CollectionID:   int64(100),
		},
		{
			Name:           "channel2",
			WatchState:     "Healthy",
			LatestTimeTick: "2",
			NodeID:         int64(2),
			CollectionID:   int64(200),
		},
	}
	resp := &milvuspb.GetMetricsResponse{
		Response: func() string {
			data, _ := json.Marshal(expectedChannels)
			return string(data)
		}(),
	}
	mockCluster.EXPECT().GetMetrics(mock.Anything, mock.Anything, req).Return(resp, nil)
	result, err := server.getChannelsFromQueryNode(context.Background(), req)
	assert.NoError(t, err)

	var actualChannels []*metricsinfo.Channel
	err = json.Unmarshal([]byte(result), &actualChannels)
	assert.NoError(t, err)
	assert.Equal(t, expectedChannels, actualChannels)
}

func TestGetSegmentsFromQueryNode(t *testing.T) {
	mockCluster := session.NewMockCluster(t)
	nodeManager := session.NewNodeManager()
	nodeManager.Add(session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: 1}))
	server := &Server{cluster: mockCluster, nodeMgr: nodeManager}
	expectedSegments := []*metricsinfo.Segment{
		{
			SegmentID:            1,
			PartitionID:          1,
			Channel:              "channel1",
			ResourceGroup:        "default",
			MemSize:              int64(1024),
			LoadedInsertRowCount: 100,
		},
		{
			SegmentID:            2,
			PartitionID:          1,
			Channel:              "channel2",
			ResourceGroup:        "default",
			MemSize:              int64(1024),
			LoadedInsertRowCount: 200,
		},
	}
	resp := &milvuspb.GetMetricsResponse{
		Response: func() string {
			data, _ := json.Marshal(expectedSegments)
			return string(data)
		}(),
	}
	req := &milvuspb.GetMetricsRequest{}
	mockCluster.EXPECT().GetMetrics(mock.Anything, mock.Anything, req).Return(resp, nil)

	result, err := server.getSegmentsFromQueryNode(context.Background(), req)
	assert.NoError(t, err)

	var actualSegments []*metricsinfo.Segment
	err = json.Unmarshal([]byte(result), &actualSegments)
	assert.NoError(t, err)
	assert.Equal(t, expectedSegments, actualSegments)
}

func TestServer_getSegmentsJSON(t *testing.T) {
	mockCluster := session.NewMockCluster(t)
	nodeManager := session.NewNodeManager()
	nodeManager.Add(session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: 1}))
	server := &Server{cluster: mockCluster, nodeMgr: nodeManager}
	expectedSegments := []*metricsinfo.Segment{
		{
			SegmentID:            1,
			PartitionID:          1,
			Channel:              "channel1",
			ResourceGroup:        "default",
			MemSize:              int64(1024),
			LoadedInsertRowCount: 100,
		},
		{
			SegmentID:            2,
			PartitionID:          1,
			Channel:              "channel2",
			ResourceGroup:        "default",
			MemSize:              int64(1024),
			LoadedInsertRowCount: 200,
		},
	}
	resp := &milvuspb.GetMetricsResponse{
		Response: func() string {
			data, _ := json.Marshal(expectedSegments)
			return string(data)
		}(),
	}
	req := &milvuspb.GetMetricsRequest{}
	mockCluster.EXPECT().GetMetrics(mock.Anything, mock.Anything, req).Return(resp, nil)

	server.dist = meta.NewDistributionManager()
	server.dist.SegmentDistManager.Update(1, meta.SegmentFromInfo(&datapb.SegmentInfo{
		ID:            1,
		CollectionID:  1,
		PartitionID:   1,
		InsertChannel: "dmc0",
	}))

	ctx := context.TODO()

	t.Run("valid request in dc", func(t *testing.T) {
		jsonReq := gjson.Parse(`{"in": "qc", "collection_id": 1}`)
		result, err := server.getSegmentsJSON(ctx, req, jsonReq)
		assert.NoError(t, err)
		assert.NotEmpty(t, result)
	})

	t.Run("invalid request", func(t *testing.T) {
		jsonReq := gjson.Parse(`{"in": "invalid"}`)
		result, err := server.getSegmentsJSON(ctx, req, jsonReq)
		assert.Error(t, err)
		assert.Empty(t, result)
	})

	t.Run("valid request in qn", func(t *testing.T) {
		jsonReq := gjson.Parse(`{"in": "qn"}`)
		result, err := server.getSegmentsJSON(ctx, req, jsonReq)
		assert.NoError(t, err)
		assert.NotEmpty(t, result)
	})

	t.Run("valid request in qc", func(t *testing.T) {
		jsonReq := gjson.Parse(`{"in": "qc", "collection_id": 1}`)
		result, err := server.getSegmentsJSON(ctx, req, jsonReq)
		assert.NoError(t, err)
		assert.NotEmpty(t, result)
	})
}
