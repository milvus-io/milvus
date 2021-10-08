// Copyright (C) 2019-2020 Zilliz. All rights reserved.//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.
package datacoord

import (
	"context"
	"errors"
	"math/rand"
	"os"
	"path"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"

	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/util/metricsinfo"

	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/stretchr/testify/assert"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestMain(m *testing.M) {
	rand.Seed(time.Now().UnixNano())
	os.Exit(m.Run())
}

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

	t.Run("assign segment normally", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)
		schema := newTestSchema()
		svr.meta.AddCollection(&datapb.CollectionInfo{
			ID:         collID,
			Schema:     schema,
			Partitions: []int64{},
		})
		req := &datapb.SegmentIDRequest{
			Count:        1000,
			ChannelName:  channel0,
			CollectionID: collID,
			PartitionID:  partID,
		}

		resp, err := svr.AssignSegmentID(context.TODO(), &datapb.AssignSegmentIDRequest{
			NodeID:            0,
			PeerRole:          "",
			SegmentIDRequests: []*datapb.SegmentIDRequest{req},
		})
		assert.Nil(t, err)
		assert.EqualValues(t, 1, len(resp.SegIDAssignments))
		assign := resp.SegIDAssignments[0]
		assert.EqualValues(t, commonpb.ErrorCode_Success, assign.Status.ErrorCode)
		assert.EqualValues(t, collID, assign.CollectionID)
		assert.EqualValues(t, partID, assign.PartitionID)
		assert.EqualValues(t, channel0, assign.ChannelName)
		assert.EqualValues(t, 1000, assign.Count)
	})

	t.Run("with closed server", func(t *testing.T) {
		req := &datapb.SegmentIDRequest{
			Count:        100,
			ChannelName:  channel0,
			CollectionID: collID,
			PartitionID:  partID,
		}
		svr := newTestServer(t, nil)
		closeTestServer(t, svr)
		resp, err := svr.AssignSegmentID(context.Background(), &datapb.AssignSegmentIDRequest{
			NodeID:            0,
			PeerRole:          "",
			SegmentIDRequests: []*datapb.SegmentIDRequest{req},
		})
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
		assert.Equal(t, serverNotServingErrMsg, resp.GetStatus().GetReason())
	})

	t.Run("assign segment with invalid collection", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)
		schema := newTestSchema()
		svr.meta.AddCollection(&datapb.CollectionInfo{
			ID:         collID,
			Schema:     schema,
			Partitions: []int64{},
		})
		req := &datapb.SegmentIDRequest{
			Count:        1000,
			ChannelName:  channel0,
			CollectionID: collIDInvalid,
			PartitionID:  partID,
		}

		resp, err := svr.AssignSegmentID(context.TODO(), &datapb.AssignSegmentIDRequest{
			NodeID:            0,
			PeerRole:          "",
			SegmentIDRequests: []*datapb.SegmentIDRequest{req},
		})
		assert.Nil(t, err)
		assert.EqualValues(t, 0, len(resp.SegIDAssignments))
	})
}

func TestFlush(t *testing.T) {
	req := &datapb.FlushRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_Flush,
			MsgID:     0,
			Timestamp: 0,
			SourceID:  0,
		},
		DbID:         0,
		CollectionID: 0,
	}
	t.Run("normal case", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)
		schema := newTestSchema()
		svr.meta.AddCollection(&datapb.CollectionInfo{ID: 0, Schema: schema, Partitions: []int64{}})
		allocations, err := svr.segmentManager.AllocSegment(context.TODO(), 0, 1, "channel-1", 1)
		assert.Nil(t, err)
		assert.EqualValues(t, 1, len(allocations))
		expireTs := allocations[0].ExpireTime
		segID := allocations[0].SegmentID

		resp, err := svr.Flush(context.TODO(), req)
		assert.Nil(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		ids, err := svr.segmentManager.GetFlushableSegments(context.TODO(), "channel-1", expireTs)
		assert.Nil(t, err)
		assert.EqualValues(t, 1, len(ids))
		assert.EqualValues(t, segID, ids[0])
	})

	t.Run("closed server", func(t *testing.T) {
		svr := newTestServer(t, nil)
		closeTestServer(t, svr)
		resp, err := svr.Flush(context.Background(), req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
		assert.Equal(t, serverNotServingErrMsg, resp.GetStatus().GetReason())
	})
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
	t.Run("normal cases", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)
		segment := &datapb.SegmentInfo{
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
		}
		err := svr.meta.AddSegment(NewSegmentInfo(segment))
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
	})

	t.Run("with closed server", func(t *testing.T) {
		svr := newTestServer(t, nil)
		closeTestServer(t, svr)
		resp, err := svr.GetSegmentStates(context.TODO(), &datapb.GetSegmentStatesRequest{
			Base: &commonpb.MsgBase{
				MsgType:   0,
				MsgID:     0,
				Timestamp: 0,
				SourceID:  0,
			},
			SegmentIDs: []int64{0},
		})
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
		assert.Equal(t, serverNotServingErrMsg, resp.GetStatus().GetReason())
	})
}

func TestGetInsertBinlogPaths(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)

		info := &datapb.SegmentInfo{
			ID: 0,
			Binlogs: []*datapb.FieldBinlog{
				{
					FieldID: 1,
					Binlogs: []string{
						"dev/datacoord/testsegment/1/part1",
						"dev/datacoord/testsegment/1/part2",
					},
				},
			},
		}
		err := svr.meta.AddSegment(NewSegmentInfo(info))
		assert.Nil(t, err)
		req := &datapb.GetInsertBinlogPathsRequest{
			SegmentID: 0,
		}
		resp, err := svr.GetInsertBinlogPaths(svr.ctx, req)
		assert.Nil(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("with invalid segment id", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)

		info := &datapb.SegmentInfo{
			ID: 0,
			Binlogs: []*datapb.FieldBinlog{
				{
					FieldID: 1,
					Binlogs: []string{
						"dev/datacoord/testsegment/1/part1",
						"dev/datacoord/testsegment/1/part2",
					},
				},
			},
		}
		err := svr.meta.AddSegment(NewSegmentInfo(info))
		assert.Nil(t, err)
		req := &datapb.GetInsertBinlogPathsRequest{
			SegmentID: 1,
		}
		resp, err := svr.GetInsertBinlogPaths(svr.ctx, req)
		assert.Nil(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())

	})

	t.Run("with closed server", func(t *testing.T) {
		svr := newTestServer(t, nil)
		closeTestServer(t, svr)
		resp, err := svr.GetInsertBinlogPaths(context.TODO(), &datapb.GetInsertBinlogPathsRequest{
			SegmentID: 0,
		})
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
		assert.Equal(t, serverNotServingErrMsg, resp.GetStatus().GetReason())
	})
}

func TestGetCollectionStatistics(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)

		req := &datapb.GetCollectionStatisticsRequest{
			CollectionID: 0,
		}
		resp, err := svr.GetCollectionStatistics(svr.ctx, req)
		assert.Nil(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)

	})
	t.Run("with closed server", func(t *testing.T) {
		svr := newTestServer(t, nil)
		closeTestServer(t, svr)
		resp, err := svr.GetCollectionStatistics(context.Background(), &datapb.GetCollectionStatisticsRequest{
			CollectionID: 0,
		})
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
		assert.Equal(t, serverNotServingErrMsg, resp.GetStatus().GetReason())
	})
}

func TestGetPartitionStatistics(t *testing.T) {
	t.Run("normal cases", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)

		req := &datapb.GetPartitionStatisticsRequest{
			CollectionID: 0,
			PartitionID:  0,
		}
		resp, err := svr.GetPartitionStatistics(context.Background(), req)
		assert.Nil(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})
	t.Run("with closed server", func(t *testing.T) {
		svr := newTestServer(t, nil)
		closeTestServer(t, svr)
		resp, err := svr.GetPartitionStatistics(context.Background(), &datapb.GetPartitionStatisticsRequest{})
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
		assert.Equal(t, serverNotServingErrMsg, resp.GetStatus().GetReason())
	})
}

func TestGetSegmentInfo(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)

		segInfo := &datapb.SegmentInfo{
			ID: 0,
		}
		err := svr.meta.AddSegment(NewSegmentInfo(segInfo))
		assert.Nil(t, err)

		req := &datapb.GetSegmentInfoRequest{
			SegmentIDs: []int64{0},
		}
		resp, err := svr.GetSegmentInfo(svr.ctx, req)
		assert.Nil(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})
	t.Run("with wrong segment id", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)

		segInfo := &datapb.SegmentInfo{
			ID: 0,
		}
		err := svr.meta.AddSegment(NewSegmentInfo(segInfo))
		assert.Nil(t, err)

		req := &datapb.GetSegmentInfoRequest{
			SegmentIDs: []int64{0, 1},
		}
		resp, err := svr.GetSegmentInfo(svr.ctx, req)
		assert.Nil(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_UnexpectedError, resp.Status.ErrorCode)
	})
	t.Run("with closed server", func(t *testing.T) {
		svr := newTestServer(t, nil)
		closeTestServer(t, svr)
		resp, err := svr.GetSegmentInfo(context.Background(), &datapb.GetSegmentInfoRequest{
			SegmentIDs: []int64{},
		})
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
		assert.Equal(t, serverNotServingErrMsg, resp.GetStatus().GetReason())
	})

}

func TestGetComponentStates(t *testing.T) {
	svr := &Server{}
	type testCase struct {
		state ServerState
		code  internalpb.StateCode
	}
	cases := []testCase{
		{state: ServerStateStopped, code: internalpb.StateCode_Abnormal},
		{state: ServerStateInitializing, code: internalpb.StateCode_Initializing},
		{state: ServerStateHealthy, code: internalpb.StateCode_Healthy},
	}
	for _, tc := range cases {
		atomic.StoreInt64(&svr.isServing, tc.state)
		resp, err := svr.GetComponentStates(context.Background())
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		assert.Equal(t, tc.code, resp.GetState().GetStateCode())
	}
}

func TestGetFlushedSegments(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)
		type testCase struct {
			collID            int64
			partID            int64
			searchPartID      int64
			flushedSegments   []int64
			unflushedSegments []int64
			expected          []int64
		}
		cases := []testCase{
			{
				collID:            1,
				partID:            1,
				searchPartID:      1,
				flushedSegments:   []int64{1, 2, 3},
				unflushedSegments: []int64{4},
				expected:          []int64{1, 2, 3},
			},
			{
				collID:            1,
				partID:            2,
				searchPartID:      2,
				flushedSegments:   []int64{5, 6},
				unflushedSegments: []int64{},
				expected:          []int64{5, 6},
			},
			{
				collID:            2,
				partID:            3,
				searchPartID:      3,
				flushedSegments:   []int64{11, 12},
				unflushedSegments: []int64{},
				expected:          []int64{11, 12},
			},
			{
				collID:       1,
				searchPartID: -1,
				expected:     []int64{1, 2, 3, 5, 6},
			},
			{
				collID:       2,
				searchPartID: -1,
				expected:     []int64{11, 12},
			},
		}
		for _, tc := range cases {
			for _, fs := range tc.flushedSegments {
				segInfo := &datapb.SegmentInfo{
					ID:           fs,
					CollectionID: tc.collID,
					PartitionID:  tc.partID,
					State:        commonpb.SegmentState_Flushed,
				}
				assert.Nil(t, svr.meta.AddSegment(NewSegmentInfo(segInfo)))
			}
			for _, us := range tc.unflushedSegments {
				segInfo := &datapb.SegmentInfo{
					ID:           us,
					CollectionID: tc.collID,
					PartitionID:  tc.partID,
					State:        commonpb.SegmentState_Growing,
				}
				assert.Nil(t, svr.meta.AddSegment(NewSegmentInfo(segInfo)))
			}

			resp, err := svr.GetFlushedSegments(context.Background(), &datapb.GetFlushedSegmentsRequest{
				CollectionID: tc.collID,
				PartitionID:  tc.searchPartID,
			})
			assert.Nil(t, err)
			assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())

			assert.ElementsMatch(t, tc.expected, resp.GetSegments())
		}
	})

	t.Run("with closed server", func(t *testing.T) {
		t.Run("with closed server", func(t *testing.T) {
			svr := newTestServer(t, nil)
			closeTestServer(t, svr)
			resp, err := svr.GetFlushedSegments(context.Background(), &datapb.GetFlushedSegmentsRequest{})
			assert.Nil(t, err)
			assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
			assert.Equal(t, serverNotServingErrMsg, resp.GetStatus().GetReason())
		})
	})
}

func TestServer_GetMetrics(t *testing.T) {
	svr := newTestServer(t, nil)
	defer closeTestServer(t, svr)

	var err error

	// server is closed
	stateSave := atomic.LoadInt64(&svr.isServing)
	atomic.StoreInt64(&svr.isServing, ServerStateInitializing)
	resp, err := svr.GetMetrics(svr.ctx, &milvuspb.GetMetricsRequest{})
	assert.Nil(t, err)
	assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	atomic.StoreInt64(&svr.isServing, stateSave)

	// failed to parse metric type
	invalidRequest := "invalid request"
	resp, err = svr.GetMetrics(svr.ctx, &milvuspb.GetMetricsRequest{
		Request: invalidRequest,
	})
	assert.Nil(t, err)
	assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)

	// unsupported metric type
	unsupportedMetricType := "unsupported"
	req, err := metricsinfo.ConstructRequestByMetricType(unsupportedMetricType)
	assert.Nil(t, err)
	resp, err = svr.GetMetrics(svr.ctx, req)
	assert.Nil(t, err)
	assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)

	// normal case
	req, err = metricsinfo.ConstructRequestByMetricType(metricsinfo.SystemInfoMetrics)
	assert.Nil(t, err)
	resp, err = svr.GetMetrics(svr.ctx, req)
	assert.Nil(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	log.Info("TestServer_GetMetrics",
		zap.String("name", resp.ComponentName),
		zap.String("response", resp.Response))
}

func TestServer_getSystemInfoMetrics(t *testing.T) {
	svr := newTestServer(t, nil)
	defer closeTestServer(t, svr)

	req, err := metricsinfo.ConstructRequestByMetricType(metricsinfo.SystemInfoMetrics)
	assert.Nil(t, err)
	resp, err := svr.getSystemInfoMetrics(svr.ctx, req)
	assert.Nil(t, err)
	log.Info("TestServer_getSystemInfoMetrics",
		zap.String("name", resp.ComponentName),
		zap.String("response", resp.Response))

	var coordTopology metricsinfo.DataCoordTopology
	err = metricsinfo.UnmarshalTopology(resp.Response, &coordTopology)
	assert.Nil(t, err)
	assert.Equal(t, len(svr.cluster.GetNodes()), len(coordTopology.Cluster.ConnectedNodes))
	for _, nodeMetrics := range coordTopology.Cluster.ConnectedNodes {
		assert.Equal(t, false, nodeMetrics.HasError)
		assert.Equal(t, 0, len(nodeMetrics.ErrorReason))
		_, err = metricsinfo.MarshalComponentInfos(nodeMetrics)
		assert.Nil(t, err)
	}
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
		err := svr.meta.AddSegment(NewSegmentInfo(segInfo))
		assert.Nil(t, err)

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
		err = statsStream.Produce(&msgPack)
		assert.Nil(t, err)
	})
}

func TestSaveBinlogPaths(t *testing.T) {
	t.Run("Normal SaveRequest", func(t *testing.T) {
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
			svr.meta.AddCollection(&datapb.CollectionInfo{
				ID:         collection.ID,
				Schema:     nil,
				Partitions: collection.Partitions,
			})
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
			s := &datapb.SegmentInfo{
				ID:           segment.id,
				CollectionID: segment.collectionID,
				PartitionID:  segment.partitionID,
			}
			err := svr.meta.AddSegment(NewSegmentInfo(s))
			assert.Nil(t, err)
		}

		ctx := context.Background()
		resp, err := svr.SaveBinlogPaths(ctx, &datapb.SaveBinlogPathsRequest{
			Base: &commonpb.MsgBase{
				Timestamp: uint64(time.Now().Unix()),
			},
			SegmentID:    2,
			CollectionID: 0,
			Field2BinlogPaths: []*datapb.FieldBinlog{
				{
					FieldID: 1,
					Binlogs: []string{
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

		segment := svr.meta.GetSegment(2)
		assert.NotNil(t, segment)
		binlogs := segment.GetBinlogs()
		assert.EqualValues(t, 1, len(binlogs))
		fieldBinlogs := binlogs[0]
		assert.NotNil(t, fieldBinlogs)
		assert.EqualValues(t, 2, len(fieldBinlogs.GetBinlogs()))
		assert.EqualValues(t, 1, fieldBinlogs.GetFieldID())
		assert.EqualValues(t, "/by-dev/test/0/1/2/1/Allo1", fieldBinlogs.GetBinlogs()[0])
		assert.EqualValues(t, "/by-dev/test/0/1/2/1/Allo2", fieldBinlogs.GetBinlogs()[1])

		segmentInfo := svr.meta.GetSegment(0)
		assert.NotNil(t, segmentInfo)
		assert.EqualValues(t, segmentInfo.DmlPosition.ChannelName, "ch1")
		assert.EqualValues(t, segmentInfo.DmlPosition.MsgID, []byte{1, 2, 3})
		assert.EqualValues(t, segmentInfo.NumOfRows, 10)
	})

	t.Run("with closed server", func(t *testing.T) {
		svr := newTestServer(t, nil)
		closeTestServer(t, svr)
		resp, err := svr.SaveBinlogPaths(context.Background(), &datapb.SaveBinlogPathsRequest{})
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetErrorCode())
		assert.Equal(t, serverNotServingErrMsg, resp.GetReason())
	})
}

func TestDataNodeTtChannel(t *testing.T) {
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
	t.Run("Test segment flush after tt", func(t *testing.T) {
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
		info := &datapb.DataNodeInfo{
			Address: "localhost:7777",
			Version: 0,
			Channels: []*datapb.ChannelStatus{
				{
					Name:  "ch-1",
					State: datapb.ChannelWatchState_Complete,
				},
			},
		}
		node := NewNodeInfo(context.TODO(), info)
		node.client, err = newMockDataNodeClient(1, ch)
		assert.Nil(t, err)
		svr.cluster.Register(node)

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
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp2.Status.ErrorCode)

		msgPack := msgstream.MsgPack{}
		msg := genMsg(commonpb.MsgType_DataNodeTt, "ch-1", assign.ExpireTime)
		msgPack.Msgs = append(msgPack.Msgs, msg)
		err = ttMsgStream.Produce(&msgPack)
		assert.Nil(t, err)

		flushMsg := <-ch
		flushReq := flushMsg.(*datapb.FlushSegmentsRequest)
		assert.EqualValues(t, 1, len(flushReq.SegmentIDs))
		assert.EqualValues(t, assign.SegID, flushReq.SegmentIDs[0])
	})

	t.Run("flush segment with different channels", func(t *testing.T) {
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
		info := &datapb.DataNodeInfo{
			Address: "localhost:7777",
			Version: 0,
			Channels: []*datapb.ChannelStatus{
				{
					Name:  "ch-1",
					State: datapb.ChannelWatchState_Complete,
				},
				{
					Name:  "ch-2",
					State: datapb.ChannelWatchState_Complete,
				},
			},
		}
		node := NewNodeInfo(context.TODO(), info)
		node.client, err = newMockDataNodeClient(1, ch)
		assert.Nil(t, err)
		svr.cluster.Register(node)
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
				{
					CollectionID: 0,
					PartitionID:  0,
					ChannelName:  "ch-2",
					Count:        100,
				},
			},
		})
		assert.Nil(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.EqualValues(t, 2, len(resp.SegIDAssignments))
		var assign *datapb.SegmentIDAssignment
		for _, segment := range resp.SegIDAssignments {
			if segment.GetChannelName() == "ch-1" {
				assign = segment
				break
			}
		}
		assert.NotNil(t, assign)
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
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp2.Status.ErrorCode)

		msgPack := msgstream.MsgPack{}
		msg := genMsg(commonpb.MsgType_DataNodeTt, "ch-1", assign.ExpireTime)
		msgPack.Msgs = append(msgPack.Msgs, msg)
		err = ttMsgStream.Produce(&msgPack)
		assert.Nil(t, err)
		flushMsg := <-ch
		flushReq := flushMsg.(*datapb.FlushSegmentsRequest)
		assert.EqualValues(t, 1, len(flushReq.SegmentIDs))
		assert.EqualValues(t, assign.SegID, flushReq.SegmentIDs[0])
	})

	t.Run("test expire allocation after receiving tt msg", func(t *testing.T) {
		ch := make(chan interface{}, 1)
		helper := ServerHelper{
			eventAfterHandleDataNodeTt: func() { ch <- struct{}{} },
		}
		svr := newTestServer(t, nil, SetServerHelper(helper))
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
		info := &datapb.DataNodeInfo{
			Address: "localhost:7777",
			Version: 0,
			Channels: []*datapb.ChannelStatus{
				{
					Name:  "ch-1",
					State: datapb.ChannelWatchState_Complete,
				},
			},
		}
		node := NewNodeInfo(context.TODO(), info)
		node.client, err = newMockDataNodeClient(1, ch)
		assert.Nil(t, err)
		svr.cluster.Register(node)

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

		assignedSegmentID := resp.SegIDAssignments[0].SegID
		segment := svr.meta.GetSegment(assignedSegmentID)
		assert.EqualValues(t, 1, len(segment.allocations))

		msgPack := msgstream.MsgPack{}
		msg := genMsg(commonpb.MsgType_DataNodeTt, "ch-1", resp.SegIDAssignments[0].ExpireTime)
		msgPack.Msgs = append(msgPack.Msgs, msg)
		err = ttMsgStream.Produce(&msgPack)
		assert.Nil(t, err)

		<-ch
		segment = svr.meta.GetSegment(assignedSegmentID)
		assert.EqualValues(t, 0, len(segment.allocations))
	})
}

func TestGetVChannelPos(t *testing.T) {
	svr := newTestServer(t, nil)
	defer closeTestServer(t, svr)
	schema := newTestSchema()
	svr.meta.AddCollection(&datapb.CollectionInfo{
		ID:     0,
		Schema: schema,
	})
	s1 := &datapb.SegmentInfo{
		ID:            1,
		CollectionID:  0,
		PartitionID:   0,
		InsertChannel: "ch1",
		State:         commonpb.SegmentState_Flushed,
	}
	err := svr.meta.AddSegment(NewSegmentInfo(s1))
	assert.Nil(t, err)
	s2 := &datapb.SegmentInfo{
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
	}
	err = svr.meta.AddSegment(NewSegmentInfo(s2))
	assert.Nil(t, err)
	s3 := &datapb.SegmentInfo{
		ID:            3,
		CollectionID:  0,
		PartitionID:   0,
		InsertChannel: "ch1",
		State:         commonpb.SegmentState_Growing,
	}
	err = svr.meta.AddSegment(NewSegmentInfo(s3))
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
		assert.EqualValues(t, s1, pair[0].FlushedSegments[0])
		assert.EqualValues(t, 1, len(pair[0].UnflushedSegments))
		assert.EqualValues(t, 2, pair[0].UnflushedSegments[0].ID)
		assert.EqualValues(t, []byte{1, 2, 3}, pair[0].UnflushedSegments[0].DmlPosition.MsgID)
	})
}

func TestGetRecoveryInfo(t *testing.T) {

	t.Run("test get recovery info with no segments", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)

		svr.rootCoordClientCreator = func(ctx context.Context, metaRootPath string, etcdEndpoints []string) (types.RootCoord, error) {
			return newMockRootCoordService(), nil
		}

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
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)

		svr.rootCoordClientCreator = func(ctx context.Context, metaRootPath string, etcdEndpoints []string) (types.RootCoord, error) {
			return newMockRootCoordService(), nil
		}

		seg1 := createSegment(0, 0, 0, 100, 10, "vchan1", commonpb.SegmentState_Flushed)
		seg2 := createSegment(1, 0, 0, 100, 20, "vchan1", commonpb.SegmentState_Flushed)
		err := svr.meta.AddSegment(NewSegmentInfo(seg1))
		assert.Nil(t, err)
		err = svr.meta.AddSegment(NewSegmentInfo(seg2))
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
		assert.ElementsMatch(t, []*datapb.SegmentInfo{seg1, seg2}, resp.GetChannels()[0].GetFlushedSegments())
		assert.EqualValues(t, 20, resp.GetChannels()[0].GetSeekPosition().GetTimestamp())
	})

	t.Run("test get recovery of unflushed segments ", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)

		svr.rootCoordClientCreator = func(ctx context.Context, metaRootPath string, etcdEndpoints []string) (types.RootCoord, error) {
			return newMockRootCoordService(), nil
		}

		seg1 := createSegment(3, 0, 0, 100, 30, "vchan1", commonpb.SegmentState_Growing)
		seg2 := createSegment(4, 0, 0, 100, 40, "vchan1", commonpb.SegmentState_Growing)
		err := svr.meta.AddSegment(NewSegmentInfo(seg1))
		assert.Nil(t, err)
		err = svr.meta.AddSegment(NewSegmentInfo(seg2))
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
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)

		svr.rootCoordClientCreator = func(ctx context.Context, metaRootPath string, etcdEndpoints []string) (types.RootCoord, error) {
			return newMockRootCoordService(), nil
		}

		binlogReq := &datapb.SaveBinlogPathsRequest{
			SegmentID:    0,
			CollectionID: 0,
			Field2BinlogPaths: []*datapb.FieldBinlog{
				{
					FieldID: 1,
					Binlogs: []string{
						"/binlog/file1",
						"/binlog/file2",
					},
				},
			},
		}
		segment := createSegment(0, 0, 0, 100, 10, "ch1", commonpb.SegmentState_Flushed)
		err := svr.meta.AddSegment(NewSegmentInfo(segment))
		assert.Nil(t, err)
		sResp, err := svr.SaveBinlogPaths(context.TODO(), binlogReq)
		assert.Nil(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, sResp.ErrorCode)

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

	t.Run("with closed server", func(t *testing.T) {
		svr := newTestServer(t, nil)
		closeTestServer(t, svr)
		resp, err := svr.GetRecoveryInfo(context.TODO(), &datapb.GetRecoveryInfoRequest{})
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
		assert.Equal(t, serverNotServingErrMsg, resp.GetStatus().GetReason())
	})
}

func TestOptions(t *testing.T) {
	t.Run("SetRootCoordCreator", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)
		var crt RootCoordCreatorFunc = func(ctx context.Context, metaRoot string, endpoints []string) (types.RootCoord, error) {
			return nil, errors.New("dummy")
		}
		opt := SetRootCoordCreator(crt)
		assert.NotNil(t, opt)
		svr.rootCoordClientCreator = nil
		opt(svr)
		// testify cannot compare function directly
		// the behavior is actually undefined
		assert.NotNil(t, crt)
		assert.NotNil(t, svr.rootCoordClientCreator)
	})
	t.Run("SetCluster", func(t *testing.T) {

		registerPolicy := newEmptyRegisterPolicy()
		ch := make(chan interface{})
		kv := memkv.NewMemoryKV()
		spyClusterStore := &SpyClusterStore{
			NodesInfo: NewNodesInfo(),
			ch:        ch,
		}
		cluster, err := NewCluster(context.TODO(), kv, spyClusterStore, dummyPosProvider{}, withRegisterPolicy(registerPolicy))
		assert.Nil(t, err)
		opt := SetCluster(cluster)
		assert.NotNil(t, opt)
		svr := newTestServer(t, nil, opt)
		defer closeTestServer(t, svr)

		assert.Equal(t, cluster, svr.cluster)
	})
	t.Run("SetDataNodeCreator", func(t *testing.T) {
		var target int64
		var val int64 = rand.Int63()
		opt := SetDataNodeCreator(func(context.Context, string) (types.DataNode, error) {
			target = val
			return nil, nil
		})
		assert.NotNil(t, opt)

		factory := msgstream.NewPmsFactory()

		svr, err := CreateServer(context.TODO(), factory, opt)
		assert.Nil(t, err)
		dn, err := svr.dataNodeCreator(context.Background(), "")
		assert.Nil(t, dn)
		assert.Nil(t, err)
		assert.Equal(t, target, val)
	})
}

func TestHandleSessionEvent(t *testing.T) {
	registerPolicy := newEmptyRegisterPolicy()
	unregisterPolicy := newEmptyUnregisterPolicy()
	ch := make(chan interface{})
	kv := memkv.NewMemoryKV()
	spyClusterStore := &SpyClusterStore{
		NodesInfo: NewNodesInfo(),
		ch:        ch,
	}
	cluster, err := NewCluster(context.TODO(), kv, spyClusterStore,
		dummyPosProvider{},
		withRegisterPolicy(registerPolicy),
		withUnregistorPolicy(unregisterPolicy))
	assert.Nil(t, err)
	defer cluster.Close()

	cluster.Startup(nil)

	svr := newTestServer(t, nil, SetCluster(cluster))
	defer closeTestServer(t, svr)
	t.Run("handle events", func(t *testing.T) {
		// None event
		evt := &sessionutil.SessionEvent{
			EventType: sessionutil.SessionNoneEvent,
			Session: &sessionutil.Session{
				ServerID:   0,
				ServerName: "",
				Address:    "",
				Exclusive:  false,
			},
		}
		svr.handleSessionEvent(context.Background(), evt)

		evt = &sessionutil.SessionEvent{
			EventType: sessionutil.SessionAddEvent,
			Session: &sessionutil.Session{
				ServerID:   101,
				ServerName: "DN101",
				Address:    "DN127.0.0.101",
				Exclusive:  false,
			},
		}
		svr.handleSessionEvent(context.Background(), evt)
		<-ch
		dataNodes := svr.cluster.GetNodes()
		assert.EqualValues(t, 1, len(dataNodes))
		assert.EqualValues(t, "DN127.0.0.101", dataNodes[0].Info.GetAddress())

		evt = &sessionutil.SessionEvent{
			EventType: sessionutil.SessionDelEvent,
			Session: &sessionutil.Session{
				ServerID:   101,
				ServerName: "DN101",
				Address:    "DN127.0.0.101",
				Exclusive:  false,
			},
		}
		svr.handleSessionEvent(context.Background(), evt)
		<-ch
		dataNodes = svr.cluster.GetNodes()
		assert.EqualValues(t, 0, len(dataNodes))
	})

	t.Run("nil evt", func(t *testing.T) {
		assert.NotPanics(t, func() {
			svr.handleSessionEvent(context.Background(), nil)
		})
	})
}

type rootCoordSegFlushComplete struct {
	mockRootCoordService
	flag bool
}

//SegmentFlushCompleted, override default behavior
func (rc *rootCoordSegFlushComplete) SegmentFlushCompleted(ctx context.Context, req *datapb.SegmentFlushCompletedMsg) (*commonpb.Status, error) {
	if rc.flag {
		return &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil
	}
	return &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError}, nil
}

func TestPostFlush(t *testing.T) {
	t.Run("segment not found", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)

		err := svr.postFlush(context.Background(), 1)
		assert.EqualValues(t, errors.New("segment not found"), err)
	})
	t.Run("failed to sync with Rootcoord", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)
		svr.rootCoordClient = &rootCoordSegFlushComplete{flag: false}

		err := svr.meta.AddSegment(NewSegmentInfo(&datapb.SegmentInfo{
			ID:           1,
			CollectionID: 1,
			PartitionID:  1,
			State:        commonpb.SegmentState_Flushing,
		}))

		assert.Nil(t, err)

		err = svr.postFlush(context.Background(), 1)
		assert.NotNil(t, err)
	})
	t.Run("success post flush", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)
		svr.rootCoordClient = &rootCoordSegFlushComplete{flag: true}

		err := svr.meta.AddSegment(NewSegmentInfo(&datapb.SegmentInfo{
			ID:           1,
			CollectionID: 1,
			PartitionID:  1,
			State:        commonpb.SegmentState_Flushing,
		}))

		assert.Nil(t, err)

		err = svr.postFlush(context.Background(), 1)
		assert.Nil(t, err)
	})
}

func newTestServer(t *testing.T, receiveCh chan interface{}, opts ...Option) *Server {
	Params.Init()
	Params.TimeTickChannelName = Params.TimeTickChannelName + strconv.Itoa(rand.Int())
	Params.StatisticsChannelName = Params.StatisticsChannelName + strconv.Itoa(rand.Int())
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

	svr, err := CreateServer(context.TODO(), factory, opts...)
	assert.Nil(t, err)
	svr.dataNodeCreator = func(ctx context.Context, addr string) (types.DataNode, error) {
		return newMockDataNodeClient(0, receiveCh)
	}
	svr.rootCoordClientCreator = func(ctx context.Context, metaRootPath string, etcdEndpoints []string) (types.RootCoord, error) {
		return newMockRootCoordService(), nil
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
	err := retry.Do(context.TODO(), connectEtcdFn, retry.Attempts(300))
	if err != nil {
		return nil, err
	}
	return etcdCli, nil
}
