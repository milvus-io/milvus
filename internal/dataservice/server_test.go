// Copyright (C) 2019-2020 Zilliz. All rights reserved.//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.
package dataservice

import (
	"context"
	"math"
	"path"
	"strconv"
	"testing"
	"time"

	"math/rand"

	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/clientv3"
)

func TestGetSegmentInfoChannel(t *testing.T) {
	svr := newTestServer(t, nil)
	defer closeTestServer(t, svr)
	t.Run("get segment info channel", func(t *testing.T) {
		resp, err := svr.GetSegmentInfoChannel(context.TODO())
		assert.Nil(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.EqualValues(t, Params.SegmentInfoChannelName, resp.Value)
	})
}

func TestAssignSegmentID(t *testing.T) {
	const collID = 100
	const collIDInvalid = 101
	const partID = 0
	const channel0 = "channel0"
	const channel1 = "channel1"

	svr := newTestServer(t, nil)
	defer closeTestServer(t, svr)
	schema := newTestSchema()
	svr.meta.AddCollection(&datapb.CollectionInfo{
		ID:         collID,
		Schema:     schema,
		Partitions: []int64{},
	})
	recordSize, err := typeutil.EstimateSizePerRecord(schema)
	assert.Nil(t, err)
	maxCount := int(Params.SegmentSize * 1024 * 1024 / float64(recordSize))

	cases := []struct {
		Description  string
		CollectionID UniqueID
		PartitionID  UniqueID
		ChannelName  string
		Count        uint32
		Success      bool
	}{
		{"assign segment normally", collID, partID, channel0, 1000, true},
		{"assign segment with invalid collection", collIDInvalid, partID, channel0, 1000, false},
		{"assign with max count", collID, partID, channel0, uint32(maxCount), true},
		{"assign with max uint32 count", collID, partID, channel1, math.MaxUint32, false},
	}

	for _, test := range cases {
		t.Run(test.Description, func(t *testing.T) {
			req := &datapb.SegmentIDRequest{
				Count:        test.Count,
				ChannelName:  test.ChannelName,
				CollectionID: test.CollectionID,
				PartitionID:  test.PartitionID,
			}

			resp, err := svr.AssignSegmentID(context.TODO(), &datapb.AssignSegmentIDRequest{
				NodeID:            0,
				PeerRole:          "",
				SegmentIDRequests: []*datapb.SegmentIDRequest{req},
			})
			assert.Nil(t, err)
			assert.EqualValues(t, 1, len(resp.SegIDAssignments))
			assign := resp.SegIDAssignments[0]
			if test.Success {
				assert.EqualValues(t, commonpb.ErrorCode_Success, assign.Status.ErrorCode)
				assert.EqualValues(t, test.CollectionID, assign.CollectionID)
				assert.EqualValues(t, test.PartitionID, assign.PartitionID)
				assert.EqualValues(t, test.ChannelName, assign.ChannelName)
				assert.EqualValues(t, test.Count, assign.Count)
			} else {
				assert.NotEqualValues(t, commonpb.ErrorCode_Success, assign.Status.ErrorCode)
			}
		})
	}
}

func TestShowSegments(t *testing.T) {
	svr := newTestServer(t, nil)
	defer closeTestServer(t, svr)
	segments := []struct {
		id           UniqueID
		collectionID UniqueID
		partitionID  UniqueID
	}{
		{0, 0, 0},
		{1, 0, 0},
		{2, 0, 1},
		{3, 1, 1},
	}
	for _, segment := range segments {
		err := svr.meta.AddSegment(&datapb.SegmentInfo{
			ID:           segment.id,
			CollectionID: segment.collectionID,
			PartitionID:  segment.partitionID,
		})
		assert.Nil(t, err)
	}
	cases := []struct {
		description  string
		collectionID UniqueID
		partitionID  UniqueID
		expected     []UniqueID
	}{
		{"show segments normally", 0, 0, []UniqueID{0, 1}},
		{"show non-existed segments", 1, 2, []UniqueID{}},
	}

	for _, test := range cases {
		t.Run(test.description, func(t *testing.T) {
			resp, err := svr.ShowSegments(context.TODO(), &datapb.ShowSegmentsRequest{
				Base: &commonpb.MsgBase{
					MsgType:   0,
					MsgID:     0,
					Timestamp: 0,
					SourceID:  0,
				},
				CollectionID: test.collectionID,
				PartitionID:  test.partitionID,
				DbID:         0,
			})
			assert.Nil(t, err)
			assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
			assert.ElementsMatch(t, test.expected, resp.SegmentIDs)
		})
	}
}

func TestFlush(t *testing.T) {
	svr := newTestServer(t, nil)
	defer closeTestServer(t, svr)
	schema := newTestSchema()
	err := svr.meta.AddCollection(&datapb.CollectionInfo{
		ID:         0,
		Schema:     schema,
		Partitions: []int64{},
	})
	assert.Nil(t, err)
	segID, _, expireTs, err := svr.segmentManager.AllocSegment(context.TODO(), 0, 1, "channel-1", 1)
	assert.Nil(t, err)
	resp, err := svr.Flush(context.TODO(), &datapb.FlushRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_Flush,
			MsgID:     0,
			Timestamp: 0,
			SourceID:  0,
		},
		DbID:         0,
		CollectionID: 0,
	})
	assert.Nil(t, err)
	assert.EqualValues(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	ids, err := svr.segmentManager.GetFlushableSegments(context.TODO(), "channel-1", expireTs)
	assert.Nil(t, err)
	assert.EqualValues(t, 1, len(ids))
	assert.EqualValues(t, segID, ids[0])
}

//func TestGetComponentStates(t *testing.T) {
//svr := newTestServer(t)
//defer closeTestServer(t, svr)
//cli := newMockDataNodeClient(1)
//err := cli.Init()
//assert.Nil(t, err)
//err = cli.Start()
//assert.Nil(t, err)

//err = svr.cluster.Register(&dataNode{
//id: 1,
//address: struct {
//ip   string
//port int64
//}{
//ip:   "",
//port: 0,
//},
//client:     cli,
//channelNum: 0,
//})
//assert.Nil(t, err)

//resp, err := svr.GetComponentStates(context.TODO())
//assert.Nil(t, err)
//assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
//assert.EqualValues(t, internalpb.StateCode_Healthy, resp.State.StateCode)
//assert.EqualValues(t, 1, len(resp.SubcomponentStates))
//assert.EqualValues(t, internalpb.StateCode_Healthy, resp.SubcomponentStates[0].StateCode)
//}

func TestGetTimeTickChannel(t *testing.T) {
	svr := newTestServer(t, nil)
	defer closeTestServer(t, svr)
	resp, err := svr.GetTimeTickChannel(context.TODO())
	assert.Nil(t, err)
	assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	assert.EqualValues(t, Params.TimeTickChannelName, resp.Value)
}

func TestGetStatisticsChannel(t *testing.T) {
	svr := newTestServer(t, nil)
	defer closeTestServer(t, svr)
	resp, err := svr.GetStatisticsChannel(context.TODO())
	assert.Nil(t, err)
	assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	assert.EqualValues(t, Params.StatisticsChannelName, resp.Value)
}

func TestGetSegmentStates(t *testing.T) {
	svr := newTestServer(t, nil)
	defer closeTestServer(t, svr)
	err := svr.meta.AddSegment(&datapb.SegmentInfo{
		ID:            1000,
		CollectionID:  100,
		PartitionID:   0,
		InsertChannel: "c1",
		NumOfRows:     0,
		State:         commonpb.SegmentState_Growing,
		StartPosition: &internalpb.MsgPosition{
			ChannelName: "c1",
			MsgID:       []byte{},
			MsgGroup:    "",
			Timestamp:   0,
		},
	})
	assert.Nil(t, err)

	cases := []struct {
		description   string
		id            UniqueID
		expected      bool
		expectedState commonpb.SegmentState
	}{
		{"get existed segment", 1000, true, commonpb.SegmentState_Growing},
		{"get non-existed segment", 10, false, commonpb.SegmentState_Growing},
	}

	for _, test := range cases {
		t.Run(test.description, func(t *testing.T) {
			resp, err := svr.GetSegmentStates(context.TODO(), &datapb.GetSegmentStatesRequest{
				Base: &commonpb.MsgBase{
					MsgType:   0,
					MsgID:     0,
					Timestamp: 0,
					SourceID:  0,
				},
				SegmentIDs: []int64{test.id},
			})
			assert.Nil(t, err)
			assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
			assert.EqualValues(t, 1, len(resp.States))
			if test.expected {
				assert.EqualValues(t, commonpb.ErrorCode_Success, resp.States[0].Status.ErrorCode)
				assert.EqualValues(t, test.expectedState, resp.States[0].State)
			}
		})
	}
}

func TestGetInsertBinlogPaths(t *testing.T) {
	svr := newTestServer(t, nil)
	defer closeTestServer(t, svr)

	req := &datapb.GetInsertBinlogPathsRequest{
		SegmentID: 0,
	}
	resp, err := svr.GetInsertBinlogPaths(svr.ctx, req)
	assert.Nil(t, err)
	assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
}

func TestGetCollectionStatistics(t *testing.T) {
	svr := newTestServer(t, nil)
	defer closeTestServer(t, svr)

	req := &datapb.GetCollectionStatisticsRequest{
		CollectionID: 0,
	}
	resp, err := svr.GetCollectionStatistics(svr.ctx, req)
	assert.Nil(t, err)
	assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
}

func TestGetSegmentInfo(t *testing.T) {
	svr := newTestServer(t, nil)
	defer closeTestServer(t, svr)

	segInfo := &datapb.SegmentInfo{
		ID: 0,
	}
	svr.meta.AddSegment(segInfo)

	req := &datapb.GetSegmentInfoRequest{
		SegmentIDs: []int64{0},
	}
	resp, err := svr.GetSegmentInfo(svr.ctx, req)
	assert.Nil(t, err)
	assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
}

func TestChannel(t *testing.T) {
	svr := newTestServer(t, nil)
	defer closeTestServer(t, svr)

	t.Run("Test StatsChannel", func(t *testing.T) {
		const segID = 0
		const rowNum = int64(100)

		segInfo := &datapb.SegmentInfo{
			ID: segID,
		}
		svr.meta.AddSegment(segInfo)

		stats := &internalpb.SegmentStatisticsUpdates{
			SegmentID: segID,
			NumRows:   rowNum,
		}
		genMsg := func(msgType commonpb.MsgType, t Timestamp) *msgstream.SegmentStatisticsMsg {
			return &msgstream.SegmentStatisticsMsg{
				BaseMsg: msgstream.BaseMsg{
					HashValues: []uint32{0},
				},
				SegmentStatistics: internalpb.SegmentStatistics{
					Base: &commonpb.MsgBase{
						MsgType:   msgType,
						MsgID:     0,
						Timestamp: t,
						SourceID:  0,
					},
					SegStats: []*internalpb.SegmentStatisticsUpdates{stats},
				},
			}
		}

		statsStream, _ := svr.msFactory.NewMsgStream(svr.ctx)
		statsStream.AsProducer([]string{Params.StatisticsChannelName})
		statsStream.Start()
		defer statsStream.Close()

		msgPack := msgstream.MsgPack{}
		msgPack.Msgs = append(msgPack.Msgs, genMsg(commonpb.MsgType_SegmentStatistics, 123))
		msgPack.Msgs = append(msgPack.Msgs, genMsg(commonpb.MsgType_SegmentInfo, 234))
		msgPack.Msgs = append(msgPack.Msgs, genMsg(commonpb.MsgType_SegmentStatistics, 345))
		err := statsStream.Produce(&msgPack)
		assert.Nil(t, err)
	})

	t.Run("Test SegmentFlushChannel", func(t *testing.T) {
		genMsg := func(msgType commonpb.MsgType, t Timestamp) *msgstream.FlushCompletedMsg {
			return &msgstream.FlushCompletedMsg{
				BaseMsg: msgstream.BaseMsg{
					HashValues: []uint32{0},
				},
				SegmentFlushCompletedMsg: internalpb.SegmentFlushCompletedMsg{
					Base: &commonpb.MsgBase{
						MsgType:   msgType,
						MsgID:     0,
						Timestamp: t,
						SourceID:  0,
					},
					SegmentID: 0,
				},
			}
		}

		segInfoStream, _ := svr.msFactory.NewMsgStream(svr.ctx)
		segInfoStream.AsProducer([]string{Params.SegmentInfoChannelName})
		segInfoStream.Start()
		defer segInfoStream.Close()

		msgPack := msgstream.MsgPack{}
		msgPack.Msgs = append(msgPack.Msgs, genMsg(commonpb.MsgType_SegmentFlushDone, 123))
		msgPack.Msgs = append(msgPack.Msgs, genMsg(commonpb.MsgType_SegmentInfo, 234))
		msgPack.Msgs = append(msgPack.Msgs, genMsg(commonpb.MsgType_SegmentFlushDone, 345))
		err := segInfoStream.Produce(&msgPack)
		assert.Nil(t, err)
		time.Sleep(time.Second)
	})

	t.Run("Test ProxyTimeTickChannel", func(t *testing.T) {
		genMsg := func(msgType commonpb.MsgType, t Timestamp) *msgstream.TimeTickMsg {
			return &msgstream.TimeTickMsg{
				BaseMsg: msgstream.BaseMsg{
					HashValues: []uint32{0},
				},
				TimeTickMsg: internalpb.TimeTickMsg{
					Base: &commonpb.MsgBase{
						MsgType:   msgType,
						MsgID:     0,
						Timestamp: t,
						SourceID:  0,
					},
				},
			}
		}

		timeTickStream, _ := svr.msFactory.NewMsgStream(svr.ctx)
		timeTickStream.AsProducer([]string{Params.ProxyTimeTickChannelName})
		timeTickStream.Start()
		defer timeTickStream.Close()

		msgPack := msgstream.MsgPack{}
		msgPack.Msgs = append(msgPack.Msgs, genMsg(commonpb.MsgType_TimeTick, 123))
		msgPack.Msgs = append(msgPack.Msgs, genMsg(commonpb.MsgType_SegmentInfo, 234))
		msgPack.Msgs = append(msgPack.Msgs, genMsg(commonpb.MsgType_TimeTick, 345))
		err := timeTickStream.Produce(&msgPack)
		assert.Nil(t, err)
		time.Sleep(time.Second)
	})
}

func TestSaveBinlogPaths(t *testing.T) {
	svr := newTestServer(t, nil)
	defer closeTestServer(t, svr)

	collections := []struct {
		ID         UniqueID
		Partitions []int64
	}{
		{0, []int64{0, 1}},
		{1, []int64{0, 1}},
	}

	for _, collection := range collections {
		err := svr.meta.AddCollection(&datapb.CollectionInfo{
			ID:         collection.ID,
			Schema:     nil,
			Partitions: collection.Partitions,
		})
		assert.Nil(t, err)
	}

	segments := []struct {
		id           UniqueID
		collectionID UniqueID
		partitionID  UniqueID
	}{
		{0, 0, 0},
		{1, 0, 0},
		{2, 0, 1},
		{3, 1, 1},
	}
	for _, segment := range segments {
		err := svr.meta.AddSegment(&datapb.SegmentInfo{
			ID:           segment.id,
			CollectionID: segment.collectionID,
			PartitionID:  segment.partitionID,
		})
		assert.Nil(t, err)
	}
	t.Run("Normal SaveRequest", func(t *testing.T) {
		ctx := context.Background()
		resp, err := svr.SaveBinlogPaths(ctx, &datapb.SaveBinlogPathsRequest{
			Base: &commonpb.MsgBase{
				Timestamp: uint64(time.Now().Unix()),
			},
			SegmentID:    2,
			CollectionID: 0,
			Field2BinlogPaths: []*datapb.ID2PathList{
				{
					ID: 1,
					Paths: []string{
						"/by-dev/test/0/1/2/1/Allo1",
						"/by-dev/test/0/1/2/1/Allo2",
					},
				},
			},
			CheckPoints: []*datapb.CheckPoint{
				{
					SegmentID: 0,
					Position: &internalpb.MsgPosition{
						ChannelName: "ch1",
						MsgID:       []byte{1, 2, 3},
						MsgGroup:    "",
						Timestamp:   0,
					},
					NumOfRows: 10,
				},
			},
			Flushed: false,
		})
		assert.Nil(t, err)
		assert.EqualValues(t, resp.ErrorCode, commonpb.ErrorCode_Success)

		metas, err := svr.getFieldBinlogMeta(2, 1)
		assert.Nil(t, err)
		if assert.EqualValues(t, 2, len(metas)) {
			assert.EqualValues(t, 1, metas[0].FieldID)
			assert.EqualValues(t, "/by-dev/test/0/1/2/1/Allo1", metas[0].BinlogPath)
			assert.EqualValues(t, 1, metas[1].FieldID)
			assert.EqualValues(t, "/by-dev/test/0/1/2/1/Allo2", metas[1].BinlogPath)
		}

		metas, err = svr.getSegmentBinlogMeta(2)
		assert.Nil(t, err)
		if assert.EqualValues(t, 2, len(metas)) {
			assert.EqualValues(t, 1, metas[0].FieldID)
			assert.EqualValues(t, "/by-dev/test/0/1/2/1/Allo1", metas[0].BinlogPath)
			assert.EqualValues(t, 1, metas[1].FieldID)
			assert.EqualValues(t, "/by-dev/test/0/1/2/1/Allo2", metas[1].BinlogPath)
		}

		segmentInfo, err := svr.meta.GetSegment(0)
		assert.Nil(t, err)
		assert.EqualValues(t, segmentInfo.DmlPosition.ChannelName, "ch1")
		assert.EqualValues(t, segmentInfo.DmlPosition.MsgID, []byte{1, 2, 3})
		assert.EqualValues(t, segmentInfo.NumOfRows, 10)
	})
	t.Run("Abnormal SaveRequest", func(t *testing.T) {
		ctx := context.Background()
		resp, err := svr.SaveBinlogPaths(ctx, &datapb.SaveBinlogPathsRequest{
			SegmentID:    10,
			CollectionID: 5,
			Field2BinlogPaths: []*datapb.ID2PathList{
				{
					ID:    1,
					Paths: []string{"/by-dev/test/0/1/2/1/Allo1", "/by-dev/test/0/1/2/1/Allo2"},
				},
			},
		})
		assert.Nil(t, err)
		assert.EqualValues(t, resp.ErrorCode, commonpb.ErrorCode_UnexpectedError)
	})
}

func TestDataNodeTtChannel(t *testing.T) {
	ch := make(chan interface{}, 1)
	svr := newTestServer(t, ch)
	defer closeTestServer(t, svr)

	svr.meta.AddCollection(&datapb.CollectionInfo{
		ID:         0,
		Schema:     newTestSchema(),
		Partitions: []int64{0},
	})

	ttMsgStream, err := svr.msFactory.NewMsgStream(context.TODO())
	assert.Nil(t, err)
	ttMsgStream.AsProducer([]string{Params.TimeTickChannelName})
	ttMsgStream.Start()
	defer ttMsgStream.Close()

	genMsg := func(msgType commonpb.MsgType, ch string, t Timestamp) *msgstream.DataNodeTtMsg {
		return &msgstream.DataNodeTtMsg{
			BaseMsg: msgstream.BaseMsg{
				HashValues: []uint32{0},
			},
			DataNodeTtMsg: datapb.DataNodeTtMsg{
				Base: &commonpb.MsgBase{
					MsgType:   msgType,
					MsgID:     0,
					Timestamp: t,
					SourceID:  0,
				},
				ChannelName: ch,
				Timestamp:   t,
			},
		}
	}

	svr.cluster.register(&datapb.DataNodeInfo{
		Address: "localhost:7777",
		Version: 0,
		Channels: []*datapb.ChannelStatus{
			{
				Name:  "ch-1",
				State: datapb.ChannelWatchState_Complete,
			},
		},
	})

	t.Run("Test segment flush after tt", func(t *testing.T) {
		resp, err := svr.AssignSegmentID(context.TODO(), &datapb.AssignSegmentIDRequest{
			NodeID:   0,
			PeerRole: "",
			SegmentIDRequests: []*datapb.SegmentIDRequest{
				{
					CollectionID: 0,
					PartitionID:  0,
					ChannelName:  "ch-1",
					Count:        100,
				},
			},
		})

		assert.Nil(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.EqualValues(t, 1, len(resp.SegIDAssignments))
		assign := resp.SegIDAssignments[0]

		resp2, err := svr.Flush(context.TODO(), &datapb.FlushRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Flush,
				MsgID:     0,
				Timestamp: 0,
				SourceID:  0,
			},
			DbID:         0,
			CollectionID: 0,
		})
		assert.Nil(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp2.ErrorCode)

		msgPack := msgstream.MsgPack{}
		msg := genMsg(commonpb.MsgType_DataNodeTt, "ch-1", assign.ExpireTime)
		msgPack.Msgs = append(msgPack.Msgs, msg)
		ttMsgStream.Produce(&msgPack)

		flushMsg := <-ch
		flushReq := flushMsg.(*datapb.FlushSegmentsRequest)
		assert.EqualValues(t, 1, len(flushReq.SegmentIDs))
		assert.EqualValues(t, assign.SegID, flushReq.SegmentIDs[0])
	})

}

func TestGetVChannelPos(t *testing.T) {
	svr := newTestServer(t, nil)
	defer closeTestServer(t, svr)
	schema := newTestSchema()
	err := svr.meta.AddCollection(&datapb.CollectionInfo{
		ID:     0,
		Schema: schema,
	})
	assert.Nil(t, err)
	err = svr.meta.AddSegment(&datapb.SegmentInfo{
		ID:            1,
		CollectionID:  0,
		PartitionID:   0,
		InsertChannel: "ch1",
		State:         commonpb.SegmentState_Flushed,
	})
	assert.Nil(t, err)
	err = svr.meta.AddSegment(&datapb.SegmentInfo{
		ID:            2,
		CollectionID:  0,
		PartitionID:   0,
		InsertChannel: "ch1",
		State:         commonpb.SegmentState_Growing,
		DmlPosition: &internalpb.MsgPosition{
			ChannelName: "ch1",
			MsgID:       []byte{1, 2, 3},
			MsgGroup:    "",
			Timestamp:   0,
		},
	})
	assert.Nil(t, err)
	err = svr.meta.AddSegment(&datapb.SegmentInfo{
		ID:            3,
		CollectionID:  0,
		PartitionID:   0,
		InsertChannel: "ch1",
		State:         commonpb.SegmentState_Growing,
	})
	assert.Nil(t, err)

	t.Run("get unexisted channel", func(t *testing.T) {
		pair, err := svr.GetVChanPositions([]vchannel{
			{
				CollectionID: 0,
				DmlChannel:   "chx1",
			},
		}, true)
		assert.Nil(t, err)
		assert.EqualValues(t, 1, len(pair))
		assert.Empty(t, pair[0].UnflushedSegments)
		assert.Empty(t, pair[0].FlushedSegments)
	})

	t.Run("get existed channel", func(t *testing.T) {
		pair, err := svr.GetVChanPositions([]vchannel{
			{
				CollectionID: 0,
				DmlChannel:   "ch1",
			},
		}, true)
		assert.Nil(t, err)
		assert.EqualValues(t, 1, len(pair))
		assert.EqualValues(t, 0, pair[0].CollectionID)
		assert.EqualValues(t, 1, len(pair[0].FlushedSegments))
		assert.EqualValues(t, 1, pair[0].FlushedSegments[0])
		assert.EqualValues(t, 1, len(pair[0].UnflushedSegments))
		assert.EqualValues(t, 2, pair[0].UnflushedSegments[0].ID)
		assert.EqualValues(t, []byte{1, 2, 3}, pair[0].UnflushedSegments[0].DmlPosition.MsgID)
	})
}

func TestGetRecoveryInfo(t *testing.T) {
	svr := newTestServer(t, nil)
	defer closeTestServer(t, svr)

	svr.masterClientCreator = func(addr string) (types.MasterService, error) {
		return newMockMasterService(), nil
	}

	t.Run("test get recovery info with no segments", func(t *testing.T) {
		req := &datapb.GetRecoveryInfoRequest{
			CollectionID: 0,
			PartitionID:  0,
		}
		resp, err := svr.GetRecoveryInfo(context.TODO(), req)
		assert.Nil(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.EqualValues(t, 0, len(resp.GetBinlogs()))
		assert.EqualValues(t, 1, len(resp.GetChannels()))
		assert.Nil(t, resp.GetChannels()[0].SeekPosition)
	})

	createSegment := func(id, collectionID, partitionID, numOfRows int64, posTs uint64,
		channel string, state commonpb.SegmentState) *datapb.SegmentInfo {
		return &datapb.SegmentInfo{
			ID:            id,
			CollectionID:  collectionID,
			PartitionID:   partitionID,
			InsertChannel: channel,
			NumOfRows:     numOfRows,
			State:         state,
			DmlPosition: &internalpb.MsgPosition{
				ChannelName: channel,
				MsgID:       []byte{},
				Timestamp:   posTs,
			},
			StartPosition: &internalpb.MsgPosition{
				ChannelName: "",
				MsgID:       []byte{},
				MsgGroup:    "",
				Timestamp:   0,
			},
		}
	}

	t.Run("test get largest position of flushed segments as seek position", func(t *testing.T) {
		seg1 := createSegment(0, 0, 0, 100, 10, "vchan1", commonpb.SegmentState_Flushed)
		seg2 := createSegment(1, 0, 0, 100, 20, "vchan1", commonpb.SegmentState_Flushed)
		err := svr.meta.AddSegment(seg1)
		assert.Nil(t, err)
		err = svr.meta.AddSegment(seg2)
		assert.Nil(t, err)

		req := &datapb.GetRecoveryInfoRequest{
			CollectionID: 0,
			PartitionID:  0,
		}
		resp, err := svr.GetRecoveryInfo(context.TODO(), req)
		assert.Nil(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.EqualValues(t, 1, len(resp.GetChannels()))
		assert.EqualValues(t, 0, len(resp.GetChannels()[0].GetUnflushedSegments()))
		assert.ElementsMatch(t, []UniqueID{0, 1}, resp.GetChannels()[0].GetFlushedSegments())
		assert.EqualValues(t, 20, resp.GetChannels()[0].GetSeekPosition().GetTimestamp())
	})

	t.Run("test get recovery of unflushed segments ", func(t *testing.T) {
		seg1 := createSegment(3, 0, 0, 100, 30, "vchan1", commonpb.SegmentState_Growing)
		seg2 := createSegment(4, 0, 0, 100, 40, "vchan1", commonpb.SegmentState_Growing)
		err := svr.meta.AddSegment(seg1)
		assert.Nil(t, err)
		err = svr.meta.AddSegment(seg2)
		assert.Nil(t, err)

		req := &datapb.GetRecoveryInfoRequest{
			CollectionID: 0,
			PartitionID:  0,
		}
		resp, err := svr.GetRecoveryInfo(context.TODO(), req)
		assert.Nil(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.EqualValues(t, 0, len(resp.GetBinlogs()))
		assert.EqualValues(t, 1, len(resp.GetChannels()))
		assert.NotNil(t, resp.GetChannels()[0].SeekPosition)
	})

	t.Run("test get binlogs", func(t *testing.T) {
		binlogReq := &datapb.SaveBinlogPathsRequest{
			SegmentID:    0,
			CollectionID: 0,
			Field2BinlogPaths: []*datapb.ID2PathList{
				{
					ID: 1,
					Paths: []string{
						"/binlog/file1",
						"/binlog/file2",
					},
				},
			},
		}
		meta, err := svr.prepareBinlog(binlogReq)
		assert.Nil(t, err)
		err = svr.kvClient.MultiSave(meta)
		assert.Nil(t, err)

		req := &datapb.GetRecoveryInfoRequest{
			CollectionID: 0,
			PartitionID:  0,
		}
		resp, err := svr.GetRecoveryInfo(context.TODO(), req)
		assert.Nil(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.EqualValues(t, 1, len(resp.GetBinlogs()))
		assert.EqualValues(t, 0, resp.GetBinlogs()[0].GetSegmentID())
		assert.EqualValues(t, 1, len(resp.GetBinlogs()[0].GetFieldBinlogs()))
		assert.EqualValues(t, 1, resp.GetBinlogs()[0].GetFieldBinlogs()[0].GetFieldID())
		assert.ElementsMatch(t, []string{"/binlog/file1", "/binlog/file2"}, resp.GetBinlogs()[0].GetFieldBinlogs()[0].GetBinlogs())
	})
}

func newTestServer(t *testing.T, receiveCh chan interface{}) *Server {
	Params.Init()
	Params.TimeTickChannelName += strconv.Itoa(rand.Int())
	Params.StatisticsChannelName += strconv.Itoa(rand.Int())
	var err error
	factory := msgstream.NewPmsFactory()
	m := map[string]interface{}{
		"pulsarAddress":  Params.PulsarAddress,
		"receiveBufSize": 1024,
		"pulsarBufSize":  1024,
	}
	err = factory.SetParams(m)
	assert.Nil(t, err)

	etcdCli, err := initEtcd(Params.EtcdEndpoints)
	assert.Nil(t, err)
	sessKey := path.Join(Params.MetaRootPath, sessionutil.DefaultServiceRoot)
	_, err = etcdCli.Delete(context.Background(), sessKey, clientv3.WithPrefix())
	assert.Nil(t, err)

	svr, err := CreateServer(context.TODO(), factory)
	assert.Nil(t, err)
	svr.dataClientCreator = func(addr string) (types.DataNode, error) {
		return newMockDataNodeClient(0, receiveCh)
	}
	svr.masterClientCreator = func(addr string) (types.MasterService, error) {
		return newMockMasterService(), nil
	}
	assert.Nil(t, err)
	err = svr.Register()
	assert.Nil(t, err)
	err = svr.Init()
	assert.Nil(t, err)
	err = svr.Start()
	assert.Nil(t, err)
	return svr
}

func closeTestServer(t *testing.T, svr *Server) {
	err := svr.Stop()
	assert.Nil(t, err)
	err = svr.CleanMeta()
	assert.Nil(t, err)
}

func initEtcd(etcdEndpoints []string) (*clientv3.Client, error) {
	var etcdCli *clientv3.Client
	connectEtcdFn := func() error {
		etcd, err := clientv3.New(clientv3.Config{Endpoints: etcdEndpoints, DialTimeout: 5 * time.Second})
		if err != nil {
			return err
		}
		etcdCli = etcd
		return nil
	}
	err := retry.Retry(100000, time.Millisecond*200, connectEtcdFn)
	if err != nil {
		return nil, err
	}
	return etcdCli, nil
}
