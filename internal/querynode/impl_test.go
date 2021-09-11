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

package querynode

import (
	"context"
	"encoding/json"
	"math/rand"
	"testing"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	queryPb "github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/stretchr/testify/assert"
)

func TestImpl_GetComponentStates(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node, err := genSimpleQueryNode(ctx)
	assert.NoError(t, err)

	rsp, err := node.GetComponentStates(ctx)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, rsp.Status.ErrorCode)

	node.UpdateStateCode(internalpb.StateCode_Abnormal)
	rsp, err = node.GetComponentStates(ctx)
	assert.Error(t, err)
	assert.Equal(t, commonpb.ErrorCode_UnexpectedError, rsp.Status.ErrorCode)
}

func TestImpl_GetTimeTickChannel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node, err := genSimpleQueryNode(ctx)
	assert.NoError(t, err)

	rsp, err := node.GetTimeTickChannel(ctx)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, rsp.Status.ErrorCode)
}

func TestImpl_GetStatisticsChannel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node, err := genSimpleQueryNode(ctx)
	assert.NoError(t, err)

	rsp, err := node.GetStatisticsChannel(ctx)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, rsp.Status.ErrorCode)
}

func TestImpl_AddQueryChannel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node, err := genSimpleQueryNode(ctx)
	assert.NoError(t, err)

	req := &queryPb.AddQueryChannelRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_WatchQueryChannels,
			MsgID:   rand.Int63(),
		},
		NodeID:           0,
		CollectionID:     defaultCollectionID,
		RequestChannelID: genQueryChannel(),
		ResultChannelID:  genQueryResultChannel(),
	}

	status, err := node.AddQueryChannel(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)

	node.UpdateStateCode(internalpb.StateCode_Abnormal)
	status, err = node.AddQueryChannel(ctx, req)
	assert.Error(t, err)
	assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.ErrorCode)
}

func TestImpl_RemoveQueryChannel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node, err := genSimpleQueryNode(ctx)
	assert.NoError(t, err)

	status, err := node.RemoveQueryChannel(ctx, nil)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
}

func TestImpl_WatchDmChannels(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node, err := genSimpleQueryNode(ctx)
	assert.NoError(t, err)

	_, schema := genSimpleSchema()

	req := &queryPb.WatchDmChannelsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_WatchQueryChannels,
			MsgID:   rand.Int63(),
		},
		NodeID:       0,
		CollectionID: defaultCollectionID,
		PartitionID:  defaultPartitionID,
		Schema:       schema,
	}

	status, err := node.WatchDmChannels(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)

	node.UpdateStateCode(internalpb.StateCode_Abnormal)
	status, err = node.WatchDmChannels(ctx, req)
	assert.Error(t, err)
	assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.ErrorCode)
}

func TestImpl_LoadSegments(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node, err := genSimpleQueryNode(ctx)
	assert.NoError(t, err)

	_, schema := genSimpleSchema()

	req := &queryPb.LoadSegmentsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_WatchQueryChannels,
			MsgID:   rand.Int63(),
		},
		NodeID:        0,
		Schema:        schema,
		LoadCondition: queryPb.TriggerCondition_grpcRequest,
	}

	status, err := node.LoadSegments(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)

	node.UpdateStateCode(internalpb.StateCode_Abnormal)
	status, err = node.LoadSegments(ctx, req)
	assert.Error(t, err)
	assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.ErrorCode)
}

func TestImpl_ReleaseCollection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node, err := genSimpleQueryNode(ctx)
	assert.NoError(t, err)

	req := &queryPb.ReleaseCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_WatchQueryChannels,
			MsgID:   rand.Int63(),
		},
		NodeID:       0,
		CollectionID: defaultCollectionID,
	}

	status, err := node.ReleaseCollection(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)

	node.UpdateStateCode(internalpb.StateCode_Abnormal)
	status, err = node.ReleaseCollection(ctx, req)
	assert.Error(t, err)
	assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.ErrorCode)
}

func TestImpl_ReleasePartitions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node, err := genSimpleQueryNode(ctx)
	assert.NoError(t, err)

	req := &queryPb.ReleasePartitionsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_WatchQueryChannels,
			MsgID:   rand.Int63(),
		},
		NodeID:       0,
		CollectionID: defaultCollectionID,
		PartitionIDs: []UniqueID{defaultPartitionID},
	}

	status, err := node.ReleasePartitions(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)

	node.UpdateStateCode(internalpb.StateCode_Abnormal)
	status, err = node.ReleasePartitions(ctx, req)
	assert.Error(t, err)
	assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.ErrorCode)
}

func TestImpl_GetSegmentInfo(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node, err := genSimpleQueryNode(ctx)
	assert.NoError(t, err)

	req := &queryPb.GetSegmentInfoRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_WatchQueryChannels,
			MsgID:   rand.Int63(),
		},
		SegmentIDs: []UniqueID{defaultSegmentID},
	}

	rsp, err := node.GetSegmentInfo(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, rsp.Status.ErrorCode)

	node.UpdateStateCode(internalpb.StateCode_Abnormal)
	rsp, err = node.GetSegmentInfo(ctx, req)
	assert.Error(t, err)
	assert.Equal(t, commonpb.ErrorCode_UnexpectedError, rsp.Status.ErrorCode)
}

func TestImpl_isHealthy(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node, err := genSimpleQueryNode(ctx)
	assert.NoError(t, err)

	isHealthy := node.isHealthy()
	assert.True(t, isHealthy)
}

func TestImpl_GetMetrics(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("test GetMetrics", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)

		node.session = sessionutil.NewSession(node.queryNodeLoopCtx, Params.MetaRootPath, Params.EtcdEndpoints)

		metricReq := make(map[string]string)
		metricReq[metricsinfo.MetricTypeKey] = "system_info"
		mReq, err := json.Marshal(metricReq)
		assert.NoError(t, err)

		req := &milvuspb.GetMetricsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_WatchQueryChannels,
				MsgID:   rand.Int63(),
			},
			Request: string(mReq),
		}

		_, err = node.GetMetrics(ctx, req)
		assert.NoError(t, err)
	})

	t.Run("test ParseMetricType failed", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)

		req := &milvuspb.GetMetricsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_WatchQueryChannels,
				MsgID:   rand.Int63(),
			},
		}

		_, err = node.GetMetrics(ctx, req)
		assert.NoError(t, err)

		node.UpdateStateCode(internalpb.StateCode_Abnormal)
		_, err = node.GetMetrics(ctx, req)
		assert.NoError(t, err)
	})
}

func TestImpl_ReleaseSegments(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("test valid", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)

		req := &queryPb.ReleaseSegmentsRequest{
			Base:         genCommonMsgBase(commonpb.MsgType_ReleaseSegments),
			CollectionID: defaultCollectionID,
			PartitionIDs: []UniqueID{defaultPartitionID},
			SegmentIDs:   []UniqueID{defaultSegmentID},
		}

		_, err = node.ReleaseSegments(ctx, req)
		assert.NoError(t, err)
	})

	t.Run("test invalid query node", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)

		req := &queryPb.ReleaseSegmentsRequest{
			Base:         genCommonMsgBase(commonpb.MsgType_ReleaseSegments),
			CollectionID: defaultCollectionID,
			PartitionIDs: []UniqueID{defaultPartitionID},
			SegmentIDs:   []UniqueID{defaultSegmentID},
		}

		node.UpdateStateCode(internalpb.StateCode_Abnormal)
		_, err = node.ReleaseSegments(ctx, req)
		assert.Error(t, err)
	})

	t.Run("test segment not exists", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)

		req := &queryPb.ReleaseSegmentsRequest{
			Base:         genCommonMsgBase(commonpb.MsgType_ReleaseSegments),
			CollectionID: defaultCollectionID,
			PartitionIDs: []UniqueID{defaultPartitionID},
			SegmentIDs:   []UniqueID{defaultSegmentID},
		}

		err = node.historical.replica.removeSegment(defaultSegmentID)
		assert.NoError(t, err)

		err = node.streaming.replica.removeSegment(defaultSegmentID)
		assert.NoError(t, err)

		status, err := node.ReleaseSegments(ctx, req)
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, status.ErrorCode)
	})
}
