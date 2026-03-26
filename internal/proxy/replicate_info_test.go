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

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/mocks/distributed/mock_streaming"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

func TestProxy_GetReplicateInfo_NodeUnhealthy(t *testing.T) {
	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Abnormal)

	resp, err := node.GetReplicateInfo(context.Background(), &milvuspb.GetReplicateInfoRequest{
		TargetPchannel: "test-pchannel",
	})
	assert.Error(t, err)
	assert.True(t, errors.Is(err, merr.ErrServiceNotReady))
	assert.Nil(t, resp)
}

func TestProxy_GetReplicateInfo_GetCheckpointError(t *testing.T) {
	replicateService := mock_streaming.NewMockReplicateService(t)
	replicateService.EXPECT().GetReplicateCheckpoint(mock.Anything, "test-pchannel").
		Return(nil, errors.New("checkpoint error"))

	mockWAL := mock_streaming.NewMockWALAccesser(t)
	mockWAL.EXPECT().Replicate().Return(replicateService)
	streaming.SetWALForTest(mockWAL)
	defer streaming.SetWALForTest(nil)

	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	resp, err := node.GetReplicateInfo(context.Background(), &milvuspb.GetReplicateInfoRequest{
		TargetPchannel: "test-pchannel",
	})
	assert.EqualError(t, err, "checkpoint error")
	assert.Nil(t, resp)
}

func TestProxy_GetReplicateInfo_GetSalvageCheckpointError(t *testing.T) {
	replicateService := mock_streaming.NewMockReplicateService(t)
	replicateService.EXPECT().GetReplicateCheckpoint(mock.Anything, "test-pchannel").
		Return(&utility.ReplicateCheckpoint{ClusterID: "cluster-a", TimeTick: 100}, nil)
	replicateService.EXPECT().GetSalvageCheckpoint(mock.Anything, "test-pchannel").
		Return(nil, errors.New("salvage error"))

	mockWAL := mock_streaming.NewMockWALAccesser(t)
	mockWAL.EXPECT().Replicate().Return(replicateService)
	streaming.SetWALForTest(mockWAL)
	defer streaming.SetWALForTest(nil)

	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	resp, err := node.GetReplicateInfo(context.Background(), &milvuspb.GetReplicateInfoRequest{
		TargetPchannel:  "test-pchannel",
		SourceClusterId: "source-cluster",
	})
	assert.EqualError(t, err, "salvage error")
	assert.Nil(t, resp)
}

func TestProxy_GetReplicateInfo_Success_NoSalvageCheckpoints(t *testing.T) {
	replicateService := mock_streaming.NewMockReplicateService(t)
	replicateService.EXPECT().GetReplicateCheckpoint(mock.Anything, "test-pchannel").
		Return(&utility.ReplicateCheckpoint{ClusterID: "cluster-a", PChannel: "test-pchannel", TimeTick: 100}, nil)
	replicateService.EXPECT().GetSalvageCheckpoint(mock.Anything, "test-pchannel").
		Return(nil, nil)

	mockWAL := mock_streaming.NewMockWALAccesser(t)
	mockWAL.EXPECT().Replicate().Return(replicateService)
	streaming.SetWALForTest(mockWAL)
	defer streaming.SetWALForTest(nil)

	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	resp, err := node.GetReplicateInfo(context.Background(), &milvuspb.GetReplicateInfoRequest{
		TargetPchannel:  "test-pchannel",
		SourceClusterId: "source-cluster",
	})
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, "cluster-a", resp.GetCheckpoint().GetClusterId())
	assert.Equal(t, uint64(100), resp.GetCheckpoint().GetTimeTick())
	assert.Nil(t, resp.GetSalvageCheckpoint())
}

func TestProxy_GetReplicateInfo_Success_MatchingSourceCluster(t *testing.T) {
	salvageCPs := []*utility.ReplicateCheckpoint{
		{ClusterID: "other-cluster", PChannel: "other-pchannel", TimeTick: 50},
		{ClusterID: "source-cluster", PChannel: "source-pchannel", TimeTick: 200},
	}
	replicateService := mock_streaming.NewMockReplicateService(t)
	replicateService.EXPECT().GetReplicateCheckpoint(mock.Anything, "test-pchannel").
		Return(&utility.ReplicateCheckpoint{ClusterID: "cluster-a", PChannel: "test-pchannel", TimeTick: 100}, nil)
	replicateService.EXPECT().GetSalvageCheckpoint(mock.Anything, "test-pchannel").
		Return(salvageCPs, nil)

	mockWAL := mock_streaming.NewMockWALAccesser(t)
	mockWAL.EXPECT().Replicate().Return(replicateService)
	streaming.SetWALForTest(mockWAL)
	defer streaming.SetWALForTest(nil)

	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	resp, err := node.GetReplicateInfo(context.Background(), &milvuspb.GetReplicateInfoRequest{
		TargetPchannel:  "test-pchannel",
		SourceClusterId: "source-cluster",
	})
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, "cluster-a", resp.GetCheckpoint().GetClusterId())
	assert.NotNil(t, resp.GetSalvageCheckpoint())
	assert.Equal(t, "source-cluster", resp.GetSalvageCheckpoint().GetClusterId())
	assert.Equal(t, "source-pchannel", resp.GetSalvageCheckpoint().GetPchannel())
	assert.Equal(t, uint64(200), resp.GetSalvageCheckpoint().GetTimeTick())
}

func TestProxy_GetReplicateInfo_Success_NoMatchingSourceCluster(t *testing.T) {
	salvageCPs := []*utility.ReplicateCheckpoint{
		{ClusterID: "other-cluster", PChannel: "other-pchannel", TimeTick: 50},
	}
	replicateService := mock_streaming.NewMockReplicateService(t)
	replicateService.EXPECT().GetReplicateCheckpoint(mock.Anything, "test-pchannel").
		Return(&utility.ReplicateCheckpoint{ClusterID: "cluster-a", PChannel: "test-pchannel", TimeTick: 100}, nil)
	replicateService.EXPECT().GetSalvageCheckpoint(mock.Anything, "test-pchannel").
		Return(salvageCPs, nil)

	mockWAL := mock_streaming.NewMockWALAccesser(t)
	mockWAL.EXPECT().Replicate().Return(replicateService)
	streaming.SetWALForTest(mockWAL)
	defer streaming.SetWALForTest(nil)

	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	resp, err := node.GetReplicateInfo(context.Background(), &milvuspb.GetReplicateInfoRequest{
		TargetPchannel:  "test-pchannel",
		SourceClusterId: "source-cluster", // not in salvage list
	})
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, "cluster-a", resp.GetCheckpoint().GetClusterId())
	assert.Nil(t, resp.GetSalvageCheckpoint()) // no match
}

func TestProxy_GetReplicateInfo_Success_NoSourceClusterIDFilter(t *testing.T) {
	// When SourceClusterId is empty, no salvage checkpoint is returned
	salvageCPs := []*utility.ReplicateCheckpoint{
		{ClusterID: "some-cluster", PChannel: "some-pchannel", TimeTick: 75},
	}
	replicateService := mock_streaming.NewMockReplicateService(t)
	replicateService.EXPECT().GetReplicateCheckpoint(mock.Anything, "test-pchannel").
		Return(&utility.ReplicateCheckpoint{ClusterID: "cluster-a", PChannel: "test-pchannel", TimeTick: 100}, nil)
	replicateService.EXPECT().GetSalvageCheckpoint(mock.Anything, "test-pchannel").
		Return(salvageCPs, nil)

	mockWAL := mock_streaming.NewMockWALAccesser(t)
	mockWAL.EXPECT().Replicate().Return(replicateService)
	streaming.SetWALForTest(mockWAL)
	defer streaming.SetWALForTest(nil)

	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	resp, err := node.GetReplicateInfo(context.Background(), &milvuspb.GetReplicateInfoRequest{
		TargetPchannel: "test-pchannel",
		// SourceClusterId intentionally empty
	})
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, "cluster-a", resp.GetCheckpoint().GetClusterId())
	assert.Nil(t, resp.GetSalvageCheckpoint()) // no source cluster filter → no match
}
