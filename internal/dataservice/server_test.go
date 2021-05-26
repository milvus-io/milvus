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
package dataservice

import (
	"context"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/log"
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
	"go.uber.org/zap"
)

func TestRegisterNode(t *testing.T) {
	svr := newTestServer(t)
	defer closeTestServer(t, svr)
	t.Run("register node", func(t *testing.T) {
		resp, err := svr.RegisterNode(context.TODO(), &datapb.RegisterNodeRequest{
			Base: &commonpb.MsgBase{
				MsgType:   0,
				MsgID:     0,
				Timestamp: 0,
				SourceID:  1000,
			},
			Address: &commonpb.Address{
				Ip:   "localhost",
				Port: 1000,
			},
		})
		assert.Nil(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.EqualValues(t, 1, svr.cluster.GetNumOfNodes())
		assert.EqualValues(t, []int64{1000}, svr.cluster.GetNodeIDs())
	})

}

func TestGetSegmentInfoChannel(t *testing.T) {
	svr := newTestServer(t)
	defer closeTestServer(t, svr)
	t.Run("get segment info channel", func(t *testing.T) {
		resp, err := svr.GetSegmentInfoChannel(context.TODO())
		assert.Nil(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.EqualValues(t, Params.SegmentInfoChannelName, resp.Value)
	})
}

func TestGetInsertChannels(t *testing.T) {
	svr := newTestServer(t)
	defer closeTestServer(t, svr)
	t.Run("get insert channels", func(t *testing.T) {
		resp, err := svr.GetInsertChannels(context.TODO(), &datapb.GetInsertChannelsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   0,
				MsgID:     0,
				Timestamp: 0,
				SourceID:  1000,
			},
			DbID:         0,
			CollectionID: 0,
		})
		assert.Nil(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.EqualValues(t, svr.getInsertChannels(), resp.Values)
	})
}

func TestAssignSegmentID(t *testing.T) {
	const collID = 100
	const collIDInvalid = 101
	const partID = 0
	const channel0 = "channel0"
	const channel1 = "channel1"

	svr := newTestServer(t)
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
	svr := newTestServer(t)
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
	svr := newTestServer(t)
	defer closeTestServer(t, svr)
	schema := newTestSchema()
	err := svr.meta.AddCollection(&datapb.CollectionInfo{
		ID:         0,
		Schema:     schema,
		Partitions: []int64{},
	})
	assert.Nil(t, err)
	segID, _, expireTs, err := svr.segAllocator.AllocSegment(context.TODO(), 0, 1, "channel-1", 1)
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
	ids, err := svr.segAllocator.GetFlushableSegments(context.TODO(), "channel-1", expireTs)
	assert.Nil(t, err)
	assert.EqualValues(t, 1, len(ids))
	assert.EqualValues(t, segID, ids[0])
}

func TestGetComponentStates(t *testing.T) {
	svr := newTestServer(t)
	defer closeTestServer(t, svr)
	cli, err := newMockDataNodeClient(1)
	assert.Nil(t, err)
	err = cli.Init()
	assert.Nil(t, err)
	err = cli.Start()
	assert.Nil(t, err)

	err = svr.cluster.Register(&dataNode{
		id: 1,
		address: struct {
			ip   string
			port int64
		}{
			ip:   "",
			port: 0,
		},
		client:     cli,
		channelNum: 0,
	})
	assert.Nil(t, err)

	resp, err := svr.GetComponentStates(context.TODO())
	assert.Nil(t, err)
	assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	assert.EqualValues(t, internalpb.StateCode_Healthy, resp.State.StateCode)
	assert.EqualValues(t, 1, len(resp.SubcomponentStates))
	assert.EqualValues(t, internalpb.StateCode_Healthy, resp.SubcomponentStates[0].StateCode)
}

func TestGetTimeTickChannel(t *testing.T) {
	svr := newTestServer(t)
	defer closeTestServer(t, svr)
	resp, err := svr.GetTimeTickChannel(context.TODO())
	assert.Nil(t, err)
	assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	assert.EqualValues(t, Params.TimeTickChannelName, resp.Value)
}

func TestGetStatisticsChannel(t *testing.T) {
	svr := newTestServer(t)
	defer closeTestServer(t, svr)
	resp, err := svr.GetStatisticsChannel(context.TODO())
	assert.Nil(t, err)
	assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	assert.EqualValues(t, Params.StatisticsChannelName, resp.Value)
}

func TestGetSegmentStates(t *testing.T) {
	svr := newTestServer(t)
	defer closeTestServer(t, svr)
	err := svr.meta.AddSegment(&datapb.SegmentInfo{
		ID:            1000,
		CollectionID:  100,
		PartitionID:   0,
		InsertChannel: "",
		NumRows:       0,
		State:         commonpb.SegmentState_Growing,
		StartPosition: &internalpb.MsgPosition{
			ChannelName: "",
			MsgID:       []byte{},
			MsgGroup:    "",
			Timestamp:   0,
		},
		EndPosition: &internalpb.MsgPosition{
			ChannelName: "",
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
	svr := newTestServer(t)
	defer closeTestServer(t, svr)

	req := &datapb.GetInsertBinlogPathsRequest{
		SegmentID: 0,
	}
	resp, err := svr.GetInsertBinlogPaths(svr.ctx, req)
	assert.Nil(t, err)
	assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
}

func TestGetCollectionStatistics(t *testing.T) {
	svr := newTestServer(t)
	defer closeTestServer(t, svr)

	req := &datapb.GetCollectionStatisticsRequest{
		CollectionID: 0,
	}
	resp, err := svr.GetCollectionStatistics(svr.ctx, req)
	assert.Nil(t, err)
	assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
}

func TestGetSegmentInfo(t *testing.T) {
	svr := newTestServer(t)
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
	svr := newTestServer(t)
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
	svr := newTestServer(t)
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
			Field2BinlogPaths: &datapb.ID2PathList{
				ID:    1,
				Paths: []string{"/by-dev/test/0/1/2/1/Allo1", "/by-dev/test/0/1/2/1/Allo2"},
			},
			DdlBinlogPaths: []*datapb.DDLBinlogMeta{
				{
					DdlBinlogPath: "/by-dev/test/0/ddl/Allo7",
					TsBinlogPath:  "/by-dev/test/0/ts/Allo5",
				},
				{
					DdlBinlogPath: "/by-dev/test/0/ddl/Allo9",
					TsBinlogPath:  "/by-dev/test/0/ts/Allo8",
				},
			},
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

		collMetas, err := svr.getDDLBinlogMeta(0)
		assert.Nil(t, err)
		if assert.EqualValues(t, 2, len(collMetas)) {
			assert.EqualValues(t, "/by-dev/test/0/ts/Allo5", collMetas[0].TsBinlogPath)
			assert.EqualValues(t, "/by-dev/test/0/ddl/Allo7", collMetas[0].DdlBinlogPath)
			assert.EqualValues(t, "/by-dev/test/0/ts/Allo8", collMetas[1].TsBinlogPath)
			assert.EqualValues(t, "/by-dev/test/0/ddl/Allo9", collMetas[1].DdlBinlogPath)
		}

	})
	t.Run("Abnormal SaveRequest", func(t *testing.T) {
		ctx := context.Background()
		resp, err := svr.SaveBinlogPaths(ctx, &datapb.SaveBinlogPathsRequest{
			SegmentID:    10,
			CollectionID: 5,
			Field2BinlogPaths: &datapb.ID2PathList{
				ID:    1,
				Paths: []string{"/by-dev/test/0/1/2/1/Allo1", "/by-dev/test/0/1/2/1/Allo2"},
			},
			DdlBinlogPaths: []*datapb.DDLBinlogMeta{
				{
					DdlBinlogPath: "/by-dev/test/0/ddl/Allo7",
					TsBinlogPath:  "/by-dev/test/0/ts/Allo5",
				},
				{
					DdlBinlogPath: "/by-dev/test/0/ddl/Allo9",
					TsBinlogPath:  "/by-dev/test/0/ts/Allo8",
				},
			},
		})
		assert.NotNil(t, err)
		assert.EqualValues(t, resp.ErrorCode, commonpb.ErrorCode_UnexpectedError)
	})
}

func TestDataNodeTtChannel(t *testing.T) {
	svr := newTestServer(t)
	defer closeTestServer(t, svr)

	svr.meta.AddCollection(&datapb.CollectionInfo{
		ID:         0,
		Schema:     newTestSchema(),
		Partitions: []int64{0},
	})

	ch := make(chan interface{}, 1)
	svr.createDataNodeClient = func(addr string, serverID int64) (types.DataNode, error) {
		cli, err := newMockDataNodeClient(0)
		assert.Nil(t, err)
		cli.ch = ch
		return cli, nil
	}

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

	resp, err := svr.RegisterNode(context.TODO(), &datapb.RegisterNodeRequest{
		Base: &commonpb.MsgBase{
			MsgType:   0,
			MsgID:     0,
			Timestamp: 0,
			SourceID:  0,
		},
		Address: &commonpb.Address{
			Ip:   "localhost:7777",
			Port: 8080,
		},
	})
	assert.Nil(t, err)
	assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)

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

func TestResumeChannel(t *testing.T) {
	Params.Init()

	segmentIDs := make([]int64, 0, 1000)

	t.Run("Prepare Resume test set", func(t *testing.T) {
		svr := newTestServer(t)
		defer svr.Stop()

		i := int64(-1)
		cnt := 0
		for ; cnt < 1000; i-- {
			svr.meta.RLock()
			_, has := svr.meta.segments[i]
			svr.meta.RUnlock()
			if has {
				continue
			}
			err := svr.meta.AddSegment(&datapb.SegmentInfo{
				ID:           i,
				CollectionID: -1,
			})
			assert.Nil(t, err)
			segmentIDs = append(segmentIDs, i)
			cnt++
		}
	})

	t.Run("Test ResumeSegmentStatsChannel", func(t *testing.T) {
		svr := newTestServer(t)

		segRows := rand.Int63n(1000)

		statsStream, _ := svr.msFactory.NewMsgStream(svr.ctx)
		statsStream.AsProducer([]string{Params.StatisticsChannelName})
		statsStream.Start()
		defer statsStream.Close()

		genMsg := func(msgType commonpb.MsgType, t Timestamp, stats *internalpb.SegmentStatisticsUpdates) *msgstream.SegmentStatisticsMsg {
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
		ch := make(chan struct{})

		go func() {
			for _, segID := range segmentIDs {
				stats := &internalpb.SegmentStatisticsUpdates{
					SegmentID: segID,
					NumRows:   segRows,
				}

				msgPack := msgstream.MsgPack{}
				msgPack.Msgs = append(msgPack.Msgs, genMsg(commonpb.MsgType_SegmentStatistics, uint64(time.Now().Unix()), stats))

				err := statsStream.Produce(&msgPack)
				assert.Nil(t, err)
				time.Sleep(time.Millisecond * 5)
			}
			ch <- struct{}{}
		}()

		time.Sleep(time.Second)

		svr.Stop()
		time.Sleep(time.Millisecond * 50)

		svr = newTestServer(t)
		defer svr.Stop()
		<-ch

		//wait for Server processing last messages
		time.Sleep(time.Second)

		svr.meta.RLock()
		defer svr.meta.RUnlock()
		for _, segID := range segmentIDs {
			seg, has := svr.meta.segments[segID]
			log.Debug("check segment in meta", zap.Any("id", seg.ID), zap.Any("has", has))
			assert.True(t, has)
			if has {
				assert.Equal(t, segRows, seg.NumRows)
			}
		}
	})

	t.Run("Clean up test segments", func(t *testing.T) {
		svr := newTestServer(t)
		defer closeTestServer(t, svr)
		var err error
		for _, segID := range segmentIDs {
			err = svr.meta.DropSegment(segID)
			assert.Nil(t, err)
		}
	})
}

func newTestServer(t *testing.T) *Server {
	Params.Init()
	var err error
	factory := msgstream.NewPmsFactory()
	m := map[string]interface{}{
		"pulsarAddress":  Params.PulsarAddress,
		"receiveBufSize": 1024,
		"pulsarBufSize":  1024,
	}
	err = factory.SetParams(m)
	assert.Nil(t, err)

	etcdCli, err := initEtcd(Params.EtcdAddress)
	assert.Nil(t, err)
	_, err = etcdCli.Delete(context.Background(), sessionutil.DefaultServiceRoot, clientv3.WithPrefix())
	assert.Nil(t, err)

	svr, err := CreateServer(context.TODO(), factory)
	assert.Nil(t, err)
	ms := newMockMasterService()
	err = ms.Init()
	assert.Nil(t, err)
	err = ms.Start()
	assert.Nil(t, err)
	defer ms.Stop()
	svr.SetMasterClient(ms)
	svr.createDataNodeClient = func(addr string, serverID int64) (types.DataNode, error) {
		return newMockDataNodeClient(0)
	}
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

func initEtcd(etcdAddress string) (*clientv3.Client, error) {
	var etcdCli *clientv3.Client
	connectEtcdFn := func() error {
		etcd, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddress}, DialTimeout: 5 * time.Second})
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
