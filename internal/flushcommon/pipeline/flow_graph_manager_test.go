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

package pipeline

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datanode/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/broker"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache/pkoracle"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/flushcommon/util"
	"github.com/milvus-io/milvus/internal/flushcommon/writebuffer"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/util/metrics"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/mq/msgdispatcher"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func TestMain(t *testing.M) {
	paramtable.Init()
	code := t.Run()
	os.Exit(code)
}

func TestFlowGraphManager(t *testing.T) {
	mockBroker := broker.NewMockBroker(t)
	mockBroker.EXPECT().ReportTimeTick(mock.Anything, mock.Anything).Return(nil).Maybe()
	mockBroker.EXPECT().SaveBinlogPaths(mock.Anything, mock.Anything).Return(nil).Maybe()
	mockBroker.EXPECT().GetSegmentInfo(mock.Anything, mock.Anything).Return([]*datapb.SegmentInfo{}, nil).Maybe()
	mockBroker.EXPECT().DropVirtualChannel(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	mockBroker.EXPECT().UpdateChannelCheckpoint(mock.Anything, mock.Anything).Return(nil).Maybe()

	wbm := writebuffer.NewMockBufferManager(t)
	wbm.EXPECT().Register(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	dispClient := msgdispatcher.NewMockClient(t)
	dispClient.EXPECT().Register(mock.Anything, mock.Anything).Return(make(chan *msgstream.MsgPack), nil)
	dispClient.EXPECT().Deregister(mock.Anything)

	pipelineParams := &util.PipelineParams{
		Ctx:                context.TODO(),
		Broker:             mockBroker,
		Session:            &sessionutil.Session{SessionRaw: sessionutil.SessionRaw{ServerID: 0}},
		CheckpointUpdater:  util.NewChannelCheckpointUpdater(mockBroker),
		SyncMgr:            syncmgr.NewMockSyncManager(t),
		WriteBufferManager: wbm,
		Allocator:          allocator.NewMockAllocator(t),
		DispClient:         dispClient,
	}

	fm := NewFlowgraphManager()

	chanWatchInfo := generateChannelWatchInfo()
	ds, err := NewDataSyncService(
		context.TODO(),
		pipelineParams,
		chanWatchInfo,
		util.NewTickler(),
	)
	assert.NoError(t, err)

	fm.AddFlowgraph(ds)
	assert.True(t, fm.HasFlowgraph(chanWatchInfo.Vchan.ChannelName))
	ds, ret := fm.GetFlowgraphService(chanWatchInfo.Vchan.ChannelName)
	assert.True(t, ret)
	assert.Equal(t, chanWatchInfo.Vchan.ChannelName, ds.vchannelName)

	fm.RemoveFlowgraph(chanWatchInfo.Vchan.ChannelName)
	assert.False(t, fm.HasFlowgraph(chanWatchInfo.Vchan.ChannelName))

	fm.ClearFlowgraphs()
	assert.Equal(t, fm.GetFlowgraphCount(), 0)
}

func generateChannelWatchInfo() *datapb.ChannelWatchInfo {
	collectionID := int64(1)
	dmChannelName := fmt.Sprintf("%s_%d", "fake-ch-", collectionID)
	schema := &schemapb.CollectionSchema{
		Name: fmt.Sprintf("%s_%d", "collection_", collectionID),
		Fields: []*schemapb.FieldSchema{
			{FieldID: common.RowIDField, Name: common.RowIDFieldName, DataType: schemapb.DataType_Int64},
			{FieldID: common.TimeStampField, Name: common.TimeStampFieldName, DataType: schemapb.DataType_Int64},
			{FieldID: common.StartOfUserFieldID, DataType: schemapb.DataType_Int64, IsPrimaryKey: true, Name: "pk"},
			{FieldID: common.StartOfUserFieldID + 1, DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{
				{Key: common.DimKey, Value: "128"},
			}},
		},
	}
	vchan := &datapb.VchannelInfo{
		CollectionID:        collectionID,
		ChannelName:         dmChannelName,
		UnflushedSegmentIds: []int64{},
		FlushedSegmentIds:   []int64{},
	}

	return &datapb.ChannelWatchInfo{
		Vchan:  vchan,
		State:  datapb.ChannelWatchState_WatchSuccess,
		Schema: schema,
	}
}

type mockTimeSender struct{}

func (m *mockTimeSender) Update(channel string, ts typeutil.Timestamp, stats []*commonpb.SegmentStats) {
	panic("implement me")
}

func (m *mockTimeSender) GetLatestTimestamp(channel string) typeutil.Timestamp {
	return 0
}

func newFlowGraphManager(t *testing.T) (string, FlowgraphManager) {
	mockBroker := broker.NewMockBroker(t)
	mockBroker.EXPECT().ReportTimeTick(mock.Anything, mock.Anything).Return(nil).Maybe()
	mockBroker.EXPECT().SaveBinlogPaths(mock.Anything, mock.Anything).Return(nil).Maybe()
	mockBroker.EXPECT().GetSegmentInfo(mock.Anything, mock.Anything).Return([]*datapb.SegmentInfo{}, nil).Maybe()

	wbm := writebuffer.NewMockBufferManager(t)
	wbm.EXPECT().Register(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	dispClient := msgdispatcher.NewMockClient(t)
	dispClient.EXPECT().Register(mock.Anything, mock.Anything).Return(make(chan *msgstream.MsgPack), nil)

	pipelineParams := &util.PipelineParams{
		Ctx:                context.TODO(),
		Session:            &sessionutil.Session{SessionRaw: sessionutil.SessionRaw{ServerID: 0}},
		Broker:             mockBroker,
		TimeTickSender:     &mockTimeSender{},
		DispClient:         dispClient,
		WriteBufferManager: wbm,
	}

	chanWatchInfo := generateChannelWatchInfo()
	ds, err := NewDataSyncService(
		context.TODO(),
		pipelineParams,
		chanWatchInfo,
		util.NewTickler(),
	)
	assert.NoError(t, err)

	fm := NewFlowgraphManager()
	fm.AddFlowgraph(ds)
	return ds.vchannelName, fm
}

func TestGetChannelsJSON(t *testing.T) {
	paramtable.SetNodeID(1)
	_, fm := newFlowGraphManager(t)
	obj := []*metricsinfo.Channel{
		{
			Name:           "fake-ch-_1",
			WatchState:     "Healthy",
			LatestTimeTick: tsoutil.PhysicalTimeFormat(0),
			NodeID:         paramtable.GetNodeID(),
			CollectionID:   1,
		},
	}

	expectedBytes, err := json.Marshal(obj)
	assert.NoError(t, err)
	expectedJSON := string(expectedBytes)

	jsonResult := fm.GetChannelsJSON(0)
	assert.JSONEq(t, expectedJSON, jsonResult)

	jsonResult = fm.GetChannelsJSON(10)
	var ret []*metricsinfo.Channel
	err = json.Unmarshal([]byte(jsonResult), &ret)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(ret))
}

func TestGetSegmentJSON(t *testing.T) {
	ch, fm := newFlowGraphManager(t)
	ds, ok := fm.GetFlowgraphService(ch)
	assert.True(t, ok)

	nodeID := paramtable.GetNodeID()
	paramtable.SetNodeID(1)
	defer paramtable.SetNodeID(nodeID)

	pkStatsFactory := func(*datapb.SegmentInfo) pkoracle.PkStat {
		return pkoracle.NewBloomFilterSet()
	}
	segment := &datapb.SegmentInfo{
		ID:           1,
		PartitionID:  10,
		State:        commonpb.SegmentState_Flushed,
		Level:        datapb.SegmentLevel_L1,
		NumOfRows:    10240,
		CollectionID: 1,
	}

	s := metrics.NewSegmentFrom(segment)
	s.NodeID = 1
	s.Channel = "fake-ch-_1"
	s.FlushedRows = 10240

	expectedBytes, err := json.Marshal([]*metricsinfo.Segment{s})
	assert.NoError(t, err)
	expectedJSON := string(expectedBytes)

	ds.metacache.AddSegment(segment, pkStatsFactory, metacache.NoneBm25StatsFactory)
	jsonResult := fm.GetSegmentsJSON(0)
	assert.JSONEq(t, expectedJSON, jsonResult)

	jsonResult = fm.GetSegmentsJSON(10)
	var ret []*metricsinfo.Segment
	err = json.Unmarshal([]byte(jsonResult), &ret)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(ret))
}
