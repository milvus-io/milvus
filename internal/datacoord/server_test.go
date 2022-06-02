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

package datacoord

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"path"
	"strconv"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/util/typeutil"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
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
		assert.EqualValues(t, Params.CommonCfg.DataCoordSegmentInfo, resp.Value)
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
		svr.rootCoordClient = &mockDescribeCollRoot{
			RootCoord: svr.rootCoordClient,
			collID:    collID,
		}
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

type mockDescribeCollRoot struct {
	types.RootCoord
	collID UniqueID
}

func (r *mockDescribeCollRoot) DescribeCollection(ctx context.Context, req *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	if req.CollectionID != r.collID {
		return &milvuspb.DescribeCollectionResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "Collection not found",
			},
		}, nil
	}
	return r.RootCoord.DescribeCollection(ctx, req)
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

		svr.meta.SetCurrentRows(segID, 1)
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
	assert.EqualValues(t, Params.CommonCfg.DataCoordTimeTick, resp.Value)
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
					Binlogs: []*datapb.Binlog{
						{
							LogPath: "dev/datacoord/testsegment/1/part1",
						},
						{
							LogPath: "dev/datacoord/testsegment/1/part2",
						},
					},
				},
			},
			State: commonpb.SegmentState_Growing,
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
					Binlogs: []*datapb.Binlog{
						{
							LogPath: "dev/datacoord/testsegment/1/part1",
						},
						{
							LogPath: "dev/datacoord/testsegment/1/part2",
						},
					},
				},
			},

			State: commonpb.SegmentState_Growing,
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
			ID:    0,
			State: commonpb.SegmentState_Flushed,
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
			ID:    0,
			State: commonpb.SegmentState_Flushed,
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
	resp, err := svr.GetComponentStates(context.Background())
	assert.Nil(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	assert.Equal(t, common.NotRegisteredID, resp.State.NodeID)
	svr.session = &sessionutil.Session{}
	svr.session.UpdateRegistered(true)
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

func TestService_WatchServices(t *testing.T) {
	sc := make(chan os.Signal, 1)
	signal.Notify(sc, syscall.SIGINT)
	defer signal.Reset(syscall.SIGINT)
	factory := dependency.NewDefaultFactory(true)
	svr := CreateServer(context.TODO(), factory)
	svr.session = &sessionutil.Session{
		TriggerKill: true,
	}
	svr.serverLoopWg.Add(1)

	ech := make(chan *sessionutil.SessionEvent)
	svr.dnEventCh = ech

	flag := false
	closed := false
	sigDone := make(chan struct{}, 1)
	sigQuit := make(chan struct{}, 1)

	go func() {
		svr.watchService(context.Background())
		flag = true
		sigDone <- struct{}{}
	}()
	go func() {
		<-sc
		closed = true
		sigQuit <- struct{}{}
	}()

	close(ech)
	<-sigDone
	<-sigQuit
	assert.True(t, flag)
	assert.True(t, closed)

	ech = make(chan *sessionutil.SessionEvent)

	flag = false
	svr.dnEventCh = ech
	ctx, cancel := context.WithCancel(context.Background())
	svr.serverLoopWg.Add(1)

	go func() {
		svr.watchService(ctx)
		flag = true
		sigDone <- struct{}{}
	}()

	ech <- nil
	cancel()
	<-sigDone
	assert.True(t, flag)
}

func TestServer_watchCoord(t *testing.T) {
	Params.Init()
	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	assert.Nil(t, err)
	etcdKV := etcdkv.NewEtcdKV(etcdCli, Params.EtcdCfg.MetaRootPath)
	assert.NotNil(t, etcdKV)
	factory := dependency.NewDefaultFactory(true)
	svr := CreateServer(context.TODO(), factory)
	svr.session = &sessionutil.Session{
		TriggerKill: true,
	}
	svr.kvClient = etcdKV

	dnCh := make(chan *sessionutil.SessionEvent)
	icCh := make(chan *sessionutil.SessionEvent)
	qcCh := make(chan *sessionutil.SessionEvent)

	svr.dnEventCh = dnCh
	svr.icEventCh = icCh
	svr.qcEventCh = qcCh

	segRefer, err := NewSegmentReferenceManager(etcdKV, nil)
	assert.NoError(t, err)
	assert.NotNil(t, segRefer)
	svr.segReferManager = segRefer

	sc := make(chan os.Signal, 1)
	signal.Notify(sc, syscall.SIGINT)
	defer signal.Reset(syscall.SIGINT)
	closed := false
	sigQuit := make(chan struct{}, 1)

	svr.serverLoopWg.Add(1)
	go func() {
		svr.watchService(context.Background())
	}()

	go func() {
		<-sc
		closed = true
		sigQuit <- struct{}{}
	}()

	icCh <- &sessionutil.SessionEvent{
		EventType: sessionutil.SessionAddEvent,
		Session: &sessionutil.Session{
			ServerID: 1,
		},
	}
	icCh <- &sessionutil.SessionEvent{
		EventType: sessionutil.SessionDelEvent,
		Session: &sessionutil.Session{
			ServerID: 1,
		},
	}
	close(icCh)
	<-sigQuit
	svr.serverLoopWg.Wait()
	assert.True(t, closed)
}

func TestServer_watchQueryCoord(t *testing.T) {
	Params.Init()
	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	assert.Nil(t, err)
	etcdKV := etcdkv.NewEtcdKV(etcdCli, Params.EtcdCfg.MetaRootPath)
	assert.NotNil(t, etcdKV)
	factory := dependency.NewDefaultFactory(true)
	svr := CreateServer(context.TODO(), factory)
	svr.session = &sessionutil.Session{
		TriggerKill: true,
	}
	svr.kvClient = etcdKV

	dnCh := make(chan *sessionutil.SessionEvent)
	icCh := make(chan *sessionutil.SessionEvent)
	qcCh := make(chan *sessionutil.SessionEvent)

	svr.dnEventCh = dnCh
	svr.icEventCh = icCh
	svr.qcEventCh = qcCh

	segRefer, err := NewSegmentReferenceManager(etcdKV, nil)
	assert.NoError(t, err)
	assert.NotNil(t, segRefer)
	svr.segReferManager = segRefer

	sc := make(chan os.Signal, 1)
	signal.Notify(sc, syscall.SIGINT)
	defer signal.Reset(syscall.SIGINT)
	closed := false
	sigQuit := make(chan struct{}, 1)

	svr.serverLoopWg.Add(1)
	go func() {
		svr.watchService(context.Background())
	}()

	go func() {
		<-sc
		closed = true
		sigQuit <- struct{}{}
	}()

	qcCh <- &sessionutil.SessionEvent{
		EventType: sessionutil.SessionAddEvent,
		Session: &sessionutil.Session{
			ServerID: 2,
		},
	}
	qcCh <- &sessionutil.SessionEvent{
		EventType: sessionutil.SessionDelEvent,
		Session: &sessionutil.Session{
			ServerID: 2,
		},
	}
	close(qcCh)
	<-sigQuit
	svr.serverLoopWg.Wait()
	assert.True(t, closed)
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
	assert.Equal(t, len(svr.cluster.GetSessions()), len(coordTopology.Cluster.ConnectedNodes))
	for _, nodeMetrics := range coordTopology.Cluster.ConnectedNodes {
		assert.Equal(t, false, nodeMetrics.HasError)
		assert.Equal(t, 0, len(nodeMetrics.ErrorReason))
		_, err = metricsinfo.MarshalComponentInfos(nodeMetrics)
		assert.Nil(t, err)
	}
}

type spySegmentManager struct {
	spyCh chan struct{}
}

// AllocSegment allocates rows and record the allocation.
func (s *spySegmentManager) AllocSegment(ctx context.Context, collectionID UniqueID, partitionID UniqueID, channelName string, requestRows int64) ([]*Allocation, error) {
	panic("not implemented") // TODO: Implement
}

// DropSegment drops the segment from manager.
func (s *spySegmentManager) DropSegment(ctx context.Context, segmentID UniqueID) {
}

// SealAllSegments seals all segments of collection with collectionID and return sealed segments
func (s *spySegmentManager) SealAllSegments(ctx context.Context, collectionID UniqueID, segIDs []UniqueID) ([]UniqueID, error) {
	panic("not implemented") // TODO: Implement
}

// GetFlushableSegments returns flushable segment ids
func (s *spySegmentManager) GetFlushableSegments(ctx context.Context, channel string, ts Timestamp) ([]UniqueID, error) {
	panic("not implemented") // TODO: Implement
}

// ExpireAllocations notifies segment status to expire old allocations
func (s *spySegmentManager) ExpireAllocations(channel string, ts Timestamp) error {
	panic("not implemented") // TODO: Implement
}

// DropSegmentsOfChannel drops all segments in a channel
func (s *spySegmentManager) DropSegmentsOfChannel(ctx context.Context, channel string) {
	s.spyCh <- struct{}{}
}

func TestSaveBinlogPaths(t *testing.T) {
	t.Run("Normal SaveRequest", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)

		svr.meta.AddCollection(&datapb.CollectionInfo{ID: 0})

		segments := []struct {
			id           UniqueID
			collectionID UniqueID
		}{
			{0, 0},
			{1, 0},
		}
		for _, segment := range segments {
			s := &datapb.SegmentInfo{
				ID:            segment.id,
				CollectionID:  segment.collectionID,
				InsertChannel: "ch1",
				State:         commonpb.SegmentState_Growing,
			}
			err := svr.meta.AddSegment(NewSegmentInfo(s))
			assert.Nil(t, err)
		}

		err := svr.channelManager.AddNode(0)
		assert.Nil(t, err)
		err = svr.channelManager.Watch(&channel{"ch1", 0})
		assert.Nil(t, err)

		ctx := context.Background()
		resp, err := svr.SaveBinlogPaths(ctx, &datapb.SaveBinlogPathsRequest{
			Base: &commonpb.MsgBase{
				Timestamp: uint64(time.Now().Unix()),
			},
			SegmentID:    1,
			CollectionID: 0,
			Field2BinlogPaths: []*datapb.FieldBinlog{
				{
					FieldID: 1,
					Binlogs: []*datapb.Binlog{
						{
							LogPath: "/by-dev/test/0/1/2/1/Allo1",
						},
						{
							LogPath: "/by-dev/test/0/1/2/1/Allo2",
						},
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

		segment := svr.meta.GetSegment(1)
		assert.NotNil(t, segment)
		binlogs := segment.GetBinlogs()
		assert.EqualValues(t, 1, len(binlogs))
		fieldBinlogs := binlogs[0]
		assert.NotNil(t, fieldBinlogs)
		assert.EqualValues(t, 2, len(fieldBinlogs.GetBinlogs()))
		assert.EqualValues(t, 1, fieldBinlogs.GetFieldID())
		assert.EqualValues(t, "/by-dev/test/0/1/2/1/Allo1", fieldBinlogs.GetBinlogs()[0].GetLogPath())
		assert.EqualValues(t, "/by-dev/test/0/1/2/1/Allo2", fieldBinlogs.GetBinlogs()[1].GetLogPath())

		segmentInfo := svr.meta.GetSegment(0)
		assert.NotNil(t, segmentInfo)
		assert.EqualValues(t, segmentInfo.DmlPosition.ChannelName, "ch1")
		assert.EqualValues(t, segmentInfo.DmlPosition.MsgID, []byte{1, 2, 3})
		assert.EqualValues(t, segmentInfo.NumOfRows, 10)
	})

	t.Run("with channel not matched", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)
		err := svr.channelManager.AddNode(0)
		require.Nil(t, err)
		err = svr.channelManager.Watch(&channel{"ch1", 0})
		require.Nil(t, err)
		s := &datapb.SegmentInfo{
			ID:            1,
			InsertChannel: "ch2",
			State:         commonpb.SegmentState_Growing,
		}
		svr.meta.AddSegment(NewSegmentInfo(s))

		resp, err := svr.SaveBinlogPaths(context.Background(), &datapb.SaveBinlogPathsRequest{
			SegmentID: 1,
		})
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_MetaFailed, resp.GetErrorCode())
	})

	t.Run("with closed server", func(t *testing.T) {
		svr := newTestServer(t, nil)
		closeTestServer(t, svr)
		resp, err := svr.SaveBinlogPaths(context.Background(), &datapb.SaveBinlogPathsRequest{})
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetErrorCode())
		assert.Equal(t, serverNotServingErrMsg, resp.GetReason())
	})
	/*
		t.Run("test save dropped segment and remove channel", func(t *testing.T) {
			spyCh := make(chan struct{}, 1)
			svr := newTestServer(t, nil, SetSegmentManager(&spySegmentManager{spyCh: spyCh}))
			defer closeTestServer(t, svr)

			svr.meta.AddCollection(&datapb.CollectionInfo{ID: 1})
			err := svr.meta.AddSegment(&SegmentInfo{
				SegmentInfo: &datapb.SegmentInfo{
					ID:            1,
					CollectionID:  1,
					InsertChannel: "ch1",
					State:         commonpb.SegmentState_Growing,
				},
			})
			assert.Nil(t, err)

			err = svr.channelManager.AddNode(0)
			assert.Nil(t, err)
			err = svr.channelManager.Watch(&channel{"ch1", 1})
			assert.Nil(t, err)

			_, err = svr.SaveBinlogPaths(context.TODO(), &datapb.SaveBinlogPathsRequest{
				SegmentID: 1,
				Dropped:   true,
			})
			assert.Nil(t, err)
			<-spyCh
		})*/
}

func TestDropVirtualChannel(t *testing.T) {
	t.Run("normal DropVirtualChannel", func(t *testing.T) {
		spyCh := make(chan struct{}, 1)
		svr := newTestServer(t, nil, SetSegmentManager(&spySegmentManager{spyCh: spyCh}))

		defer closeTestServer(t, svr)

		svr.meta.AddCollection(&datapb.CollectionInfo{ID: 0})
		type testSegment struct {
			id           UniqueID
			collectionID UniqueID
		}
		segments := make([]testSegment, 0, maxOperationsPerTxn) // test batch overflow
		for i := 0; i < maxOperationsPerTxn; i++ {
			segments = append(segments, testSegment{
				id:           int64(i),
				collectionID: 0,
			})
		}
		for idx, segment := range segments {
			s := &datapb.SegmentInfo{
				ID:            segment.id,
				CollectionID:  segment.collectionID,
				InsertChannel: "ch1",

				State: commonpb.SegmentState_Growing,
			}
			if idx%2 == 0 {
				s.Binlogs = []*datapb.FieldBinlog{
					{FieldID: 1},
				}
				s.Statslogs = []*datapb.FieldBinlog{
					{FieldID: 1},
				}
			}
			err := svr.meta.AddSegment(NewSegmentInfo(s))
			assert.Nil(t, err)
		}
		// add non matched segments
		os := &datapb.SegmentInfo{
			ID:            maxOperationsPerTxn + 100,
			CollectionID:  0,
			InsertChannel: "ch2",

			State: commonpb.SegmentState_Growing,
		}

		svr.meta.AddSegment(NewSegmentInfo(os))

		err := svr.channelManager.AddNode(0)
		require.Nil(t, err)
		err = svr.channelManager.Watch(&channel{"ch1", 0})
		require.Nil(t, err)

		ctx := context.Background()
		req := &datapb.DropVirtualChannelRequest{
			Base: &commonpb.MsgBase{
				Timestamp: uint64(time.Now().Unix()),
			},
			ChannelName: "ch1",
			Segments:    make([]*datapb.DropVirtualChannelSegment, 0, maxOperationsPerTxn),
		}
		for _, segment := range segments {
			seg2Drop := &datapb.DropVirtualChannelSegment{
				SegmentID:    segment.id,
				CollectionID: segment.collectionID,
				Field2BinlogPaths: []*datapb.FieldBinlog{
					{
						FieldID: 1,
						Binlogs: []*datapb.Binlog{
							{
								LogPath: "/by-dev/test/0/1/2/1/Allo1",
							},
							{
								LogPath: "/by-dev/test/0/1/2/1/Allo2",
							},
						},
					},
				},
				Field2StatslogPaths: []*datapb.FieldBinlog{
					{
						FieldID: 1,
						Binlogs: []*datapb.Binlog{
							{
								LogPath: "/by-dev/test/0/1/2/1/stats1",
							},
							{
								LogPath: "/by-dev/test/0/1/2/1/stats2",
							},
						},
					},
				},
				Deltalogs: []*datapb.FieldBinlog{
					{
						Binlogs: []*datapb.Binlog{
							{
								EntriesNum: 1,
								LogPath:    "/by-dev/test/0/1/2/1/delta1",
							},
						},
					},
				},
				CheckPoint: &internalpb.MsgPosition{
					ChannelName: "ch1",
					MsgID:       []byte{1, 2, 3},
					MsgGroup:    "",
					Timestamp:   0,
				},
				StartPosition: &internalpb.MsgPosition{
					ChannelName: "ch1",
					MsgID:       []byte{1, 2, 3},
					MsgGroup:    "",
					Timestamp:   0,
				},
				NumOfRows: 10,
			}
			req.Segments = append(req.Segments, seg2Drop)
		}
		resp, err := svr.DropVirtualChannel(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())

		<-spyCh

		err = svr.channelManager.Watch(&channel{"ch1", 0})
		require.Nil(t, err)

		//resend
		resp, err = svr.DropVirtualChannel(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())

	})

	t.Run("with channel not matched", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)
		err := svr.channelManager.AddNode(0)
		require.Nil(t, err)
		err = svr.channelManager.Watch(&channel{"ch1", 0})
		require.Nil(t, err)

		resp, err := svr.DropVirtualChannel(context.Background(), &datapb.DropVirtualChannelRequest{
			ChannelName: "ch2",
		})
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_MetaFailed, resp.GetStatus().GetErrorCode())
	})

	t.Run("with closed server", func(t *testing.T) {
		svr := newTestServer(t, nil)
		closeTestServer(t, svr)
		resp, err := svr.DropVirtualChannel(context.Background(), &datapb.DropVirtualChannelRequest{})
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
		assert.Equal(t, serverNotServingErrMsg, resp.GetStatus().GetReason())
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

		ttMsgStream, err := svr.factory.NewMsgStream(context.TODO())
		assert.Nil(t, err)
		ttMsgStream.AsProducer([]string{Params.CommonCfg.DataCoordTimeTick})
		ttMsgStream.Start()
		defer ttMsgStream.Close()
		info := &NodeInfo{
			Address: "localhost:7777",
			NodeID:  0,
		}
		err = svr.cluster.Register(info)
		assert.Nil(t, err)

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
		msg.SegmentsStats = append(msg.SegmentsStats, &datapb.SegmentStats{
			SegmentID: assign.GetSegID(),
			NumRows:   1,
		})
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
		ttMsgStream, err := svr.factory.NewMsgStream(context.TODO())
		assert.Nil(t, err)
		ttMsgStream.AsProducer([]string{Params.CommonCfg.DataCoordTimeTick})
		ttMsgStream.Start()
		defer ttMsgStream.Close()
		info := &NodeInfo{
			Address: "localhost:7777",
			NodeID:  0,
		}
		err = svr.cluster.Register(info)
		assert.Nil(t, err)
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
		msg.SegmentsStats = append(msg.SegmentsStats, &datapb.SegmentStats{
			SegmentID: assign.GetSegID(),
			NumRows:   1,
		})
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

		ttMsgStream, err := svr.factory.NewMsgStream(context.TODO())
		assert.Nil(t, err)
		ttMsgStream.AsProducer([]string{Params.CommonCfg.DataCoordTimeTick})
		ttMsgStream.Start()
		defer ttMsgStream.Close()
		node := &NodeInfo{
			NodeID:  0,
			Address: "localhost:7777",
		}
		err = svr.cluster.Register(node)
		assert.Nil(t, err)

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
		StartPositions: []*commonpb.KeyDataPair{
			{
				Key:  "ch1",
				Data: []byte{8, 9, 10},
			},
		},
	})
	svr.meta.AddCollection(&datapb.CollectionInfo{
		ID:     1,
		Schema: schema,
		StartPositions: []*commonpb.KeyDataPair{
			{
				Key:  "ch0",
				Data: []byte{8, 9, 10},
			},
		},
	})

	s1 := &datapb.SegmentInfo{
		ID:            1,
		CollectionID:  0,
		PartitionID:   0,
		InsertChannel: "ch1",
		State:         commonpb.SegmentState_Flushed,
		DmlPosition: &internalpb.MsgPosition{
			ChannelName: "ch1",
			MsgID:       []byte{1, 2, 3},
			MsgGroup:    "",
			Timestamp:   0,
		},
	}
	err := svr.meta.AddSegment(NewSegmentInfo(s1))
	assert.Nil(t, err)
	s2 := &datapb.SegmentInfo{
		ID:            2,
		CollectionID:  0,
		PartitionID:   0,
		InsertChannel: "ch1",
		State:         commonpb.SegmentState_Growing,
		StartPosition: &internalpb.MsgPosition{
			ChannelName: "ch1",
			MsgID:       []byte{8, 9, 10},
			MsgGroup:    "",
		},
		DmlPosition: &internalpb.MsgPosition{
			ChannelName: "ch1",
			MsgID:       []byte{1, 2, 3},
			MsgGroup:    "",
			Timestamp:   1,
		},
	}
	err = svr.meta.AddSegment(NewSegmentInfo(s2))
	assert.Nil(t, err)
	s3 := &datapb.SegmentInfo{
		ID:            3,
		CollectionID:  0,
		PartitionID:   1,
		InsertChannel: "ch1",
		State:         commonpb.SegmentState_Growing,
		StartPosition: &internalpb.MsgPosition{
			ChannelName: "ch1",
			MsgID:       []byte{8, 9, 10},
			MsgGroup:    "",
		},
		DmlPosition: &internalpb.MsgPosition{
			ChannelName: "ch1",
			MsgID:       []byte{11, 12, 13},
			MsgGroup:    "",
			Timestamp:   2,
		},
	}
	err = svr.meta.AddSegment(NewSegmentInfo(s3))
	assert.Nil(t, err)

	t.Run("get unexisted channel", func(t *testing.T) {
		vchan := svr.handler.GetVChanPositions("chx1", 0, allPartitionID)
		assert.Empty(t, vchan.UnflushedSegments)
		assert.Empty(t, vchan.FlushedSegments)
	})

	t.Run("get existed channel", func(t *testing.T) {
		vchan := svr.handler.GetVChanPositions("ch1", 0, allPartitionID)
		assert.EqualValues(t, 1, len(vchan.FlushedSegments))
		assert.EqualValues(t, 1, vchan.FlushedSegments[0].ID)
		assert.EqualValues(t, 2, len(vchan.UnflushedSegments))
		assert.EqualValues(t, []byte{1, 2, 3}, vchan.GetSeekPosition().GetMsgID())
	})

	t.Run("empty collection", func(t *testing.T) {
		infos := svr.handler.GetVChanPositions("ch0_suffix", 1, allPartitionID)
		assert.EqualValues(t, 1, infos.CollectionID)
		assert.EqualValues(t, 0, len(infos.FlushedSegments))
		assert.EqualValues(t, 0, len(infos.UnflushedSegments))
		assert.EqualValues(t, []byte{8, 9, 10}, infos.SeekPosition.MsgID)
	})

	t.Run("filter partition", func(t *testing.T) {
		infos := svr.handler.GetVChanPositions("ch1", 0, 1)
		assert.EqualValues(t, 0, infos.CollectionID)
		assert.EqualValues(t, 0, len(infos.FlushedSegments))
		assert.EqualValues(t, 1, len(infos.UnflushedSegments))
		assert.EqualValues(t, []byte{11, 12, 13}, infos.SeekPosition.MsgID)
	})
}

func TestShouldDropChannel(t *testing.T) {
	svr := newTestServer(t, nil)
	defer closeTestServer(t, svr)
	schema := newTestSchema()
	svr.meta.AddCollection(&datapb.CollectionInfo{
		ID:     0,
		Schema: schema,
		StartPositions: []*commonpb.KeyDataPair{
			{
				Key:  "ch1",
				Data: []byte{8, 9, 10},
			},
		},
	})
	svr.meta.AddCollection(&datapb.CollectionInfo{
		ID:     1,
		Schema: schema,
		StartPositions: []*commonpb.KeyDataPair{
			{
				Key:  "ch0",
				Data: []byte{8, 9, 10},
			},
		},
	})
	/*
		s1 := &datapb.SegmentInfo{
			ID:            1,
			CollectionID:  0,
			PartitionID:   0,
			InsertChannel: "ch1",
			State:         commonpb.SegmentState_Dropped,
			StartPosition: &internalpb.MsgPosition{
				ChannelName: "ch1",
				MsgID:       []byte{8, 9, 10},
				MsgGroup:    "",
				Timestamp:   0,
			},
			DmlPosition: &internalpb.MsgPosition{
				ChannelName: "ch1",
				MsgID:       []byte{1, 2, 3},
				MsgGroup:    "",
				Timestamp:   0,
			},
		}
		s2 := &datapb.SegmentInfo{
			ID:             2,
			CollectionID:   0,
			PartitionID:    0,
			InsertChannel:  "ch1",
			State:          commonpb.SegmentState_Flushed,
			CompactionFrom: []int64{4, 5},
			StartPosition: &internalpb.MsgPosition{
				ChannelName: "ch1",
				MsgID:       []byte{8, 9, 10},
				MsgGroup:    "",
			},
			DmlPosition: &internalpb.MsgPosition{
				ChannelName: "ch1",
				MsgID:       []byte{1, 2, 3},
				MsgGroup:    "",
				Timestamp:   1,
			},
		}
		s3 := &datapb.SegmentInfo{
			ID:            3,
			CollectionID:  0,
			PartitionID:   1,
			InsertChannel: "ch1",
			State:         commonpb.SegmentState_Growing,
			StartPosition: &internalpb.MsgPosition{
				ChannelName: "ch1",
				MsgID:       []byte{8, 9, 10},
				MsgGroup:    "",
			},
			DmlPosition: &internalpb.MsgPosition{
				ChannelName: "ch1",
				MsgID:       []byte{11, 12, 13},
				MsgGroup:    "",
				Timestamp:   2,
			},
		}
		s4 := &datapb.SegmentInfo{
			ID:            4,
			CollectionID:  0,
			PartitionID:   1,
			InsertChannel: "ch1",
			State:         commonpb.SegmentState_Growing,
		}*/
	/*
		t.Run("channel without segments", func(t *testing.T) {
			r := svr.handler.CheckShouldDropChannel("ch1")
			assert.True(t, r)

		})

		t.Run("channel with all dropped segments", func(t *testing.T) {
			err := svr.meta.AddSegment(NewSegmentInfo(s1))
			require.NoError(t, err)

			r := svr.handler.CheckShouldDropChannel("ch1")
			assert.True(t, r)
		})

		t.Run("channel with all dropped segments and flushed compacted segments", func(t *testing.T) {
			err := svr.meta.AddSegment(NewSegmentInfo(s2))
			require.Nil(t, err)

			r := svr.handler.CheckShouldDropChannel("ch1")
			assert.False(t, r)
		})

		t.Run("channel with other state segments", func(t *testing.T) {
			err := svr.meta.DropSegment(2)
			require.Nil(t, err)
			err = svr.meta.AddSegment(NewSegmentInfo(s3))
			require.Nil(t, err)

			r := svr.handler.CheckShouldDropChannel("ch1")
			assert.False(t, r)
		})

		t.Run("channel with dropped segment and with segment without start position", func(t *testing.T) {
			err := svr.meta.DropSegment(3)
			require.Nil(t, err)
			err = svr.meta.AddSegment(NewSegmentInfo(s4))
			require.Nil(t, err)

			r := svr.handler.CheckShouldDropChannel("ch1")
			assert.True(t, r)
		})
	*/
	t.Run("channel name not in kv", func(t *testing.T) {
		assert.False(t, svr.handler.CheckShouldDropChannel("ch99"))
	})

	t.Run("channel in remove flag", func(t *testing.T) {
		key := buildChannelRemovePath("ch1")
		err := svr.meta.client.Save(key, removeFlagTomestone)
		require.NoError(t, err)

		assert.True(t, svr.handler.CheckShouldDropChannel("ch1"))
	})

	t.Run("channel name not matched", func(t *testing.T) {
		assert.False(t, svr.handler.CheckShouldDropChannel("ch2"))
	})
}

func TestGetRecoveryInfo(t *testing.T) {

	t.Run("test get recovery info with no segments", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)

		svr.rootCoordClientCreator = func(ctx context.Context, metaRootPath string, etcdCli *clientv3.Client) (types.RootCoord, error) {
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

	t.Run("test get earliest position of flushed segments as seek position", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)

		svr.rootCoordClientCreator = func(ctx context.Context, metaRootPath string, etcdCli *clientv3.Client) (types.RootCoord, error) {
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
		assert.ElementsMatch(t, []*datapb.SegmentInfo{trimSegmentInfo(seg1), trimSegmentInfo(seg2)}, resp.GetChannels()[0].GetFlushedSegments())
		assert.EqualValues(t, 10, resp.GetChannels()[0].GetSeekPosition().GetTimestamp())
	})

	t.Run("test get recovery of unflushed segments ", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)

		svr.rootCoordClientCreator = func(ctx context.Context, metaRootPath string, etcdCli *clientv3.Client) (types.RootCoord, error) {
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
		assert.NotEqual(t, 0, resp.GetChannels()[0].GetSeekPosition().GetTimestamp())
	})

	t.Run("test get binlogs", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)

		svr.rootCoordClientCreator = func(ctx context.Context, metaRootPath string, etcdCli *clientv3.Client) (types.RootCoord, error) {
			return newMockRootCoordService(), nil
		}

		binlogReq := &datapb.SaveBinlogPathsRequest{
			SegmentID:    0,
			CollectionID: 0,
			Field2BinlogPaths: []*datapb.FieldBinlog{
				{
					FieldID: 1,
					Binlogs: []*datapb.Binlog{
						{
							LogPath: "/binlog/file1",
						},
						{
							LogPath: "/binlog/file2",
						},
					},
				},
			},
			Field2StatslogPaths: []*datapb.FieldBinlog{
				{
					FieldID: 1,
					Binlogs: []*datapb.Binlog{
						{
							LogPath: "/stats_log/file1",
						},
						{
							LogPath: "/stats_log/file2",
						},
					},
				},
			},
			Deltalogs: []*datapb.FieldBinlog{
				{
					Binlogs: []*datapb.Binlog{
						{
							TimestampFrom: 0,
							TimestampTo:   1,
							LogPath:       "/stats_log/file1",
							LogSize:       1,
						},
					},
				},
			},
		}
		segment := createSegment(0, 0, 0, 100, 10, "ch1", commonpb.SegmentState_Flushed)
		err := svr.meta.AddSegment(NewSegmentInfo(segment))
		assert.Nil(t, err)

		err = svr.channelManager.AddNode(0)
		assert.Nil(t, err)
		err = svr.channelManager.Watch(&channel{"ch1", 0})
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
		for i, binlog := range resp.GetBinlogs()[0].GetFieldBinlogs()[0].GetBinlogs() {
			assert.Equal(t, fmt.Sprintf("/binlog/file%d", i+1), binlog.GetLogPath())
		}
	})
	t.Run("with dropped segments", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)

		svr.rootCoordClientCreator = func(ctx context.Context, metaRootPath string, etcdCli *clientv3.Client) (types.RootCoord, error) {
			return newMockRootCoordService(), nil
		}

		seg1 := createSegment(7, 0, 0, 100, 30, "vchan1", commonpb.SegmentState_Growing)
		seg2 := createSegment(8, 0, 0, 100, 40, "vchan1", commonpb.SegmentState_Dropped)
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
		assert.NotEqual(t, 0, resp.GetChannels()[0].GetSeekPosition().GetTimestamp())
		assert.Len(t, resp.GetChannels()[0].GetDroppedSegments(), 1)
		assert.Equal(t, UniqueID(8), resp.GetChannels()[0].GetDroppedSegments()[0].GetID())
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

func TestGetCompactionState(t *testing.T) {
	Params.DataCoordCfg.EnableCompaction = true
	t.Run("test get compaction state with new compactionhandler", func(t *testing.T) {
		svr := &Server{}
		svr.isServing = ServerStateHealthy

		svr.compactionHandler = &mockCompactionHandler{
			methods: map[string]interface{}{
				"getCompactionTasksBySignalID": func(signalID int64) []*compactionTask {
					return []*compactionTask{
						{state: completed},
					}
				},
			},
		}

		resp, err := svr.GetCompactionState(context.Background(), &milvuspb.GetCompactionStateRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		assert.Equal(t, commonpb.CompactionState_Completed, resp.GetState())
	})
	t.Run("test get compaction state in running", func(t *testing.T) {
		svr := &Server{}
		svr.isServing = ServerStateHealthy

		svr.compactionHandler = &mockCompactionHandler{
			methods: map[string]interface{}{
				"getCompactionTasksBySignalID": func(signalID int64) []*compactionTask {
					return []*compactionTask{
						{state: executing},
						{state: executing},
						{state: executing},
						{state: completed},
						{state: completed},
						{state: timeout},
					}
				},
			},
		}

		resp, err := svr.GetCompactionState(context.Background(), &milvuspb.GetCompactionStateRequest{CompactionID: 1})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		assert.Equal(t, commonpb.CompactionState_Executing, resp.GetState())
		assert.EqualValues(t, 3, resp.GetExecutingPlanNo())
		assert.EqualValues(t, 2, resp.GetCompletedPlanNo())
		assert.EqualValues(t, 1, resp.GetTimeoutPlanNo())
	})

	t.Run("with closed server", func(t *testing.T) {
		svr := &Server{}
		svr.isServing = ServerStateStopped

		resp, err := svr.GetCompactionState(context.Background(), &milvuspb.GetCompactionStateRequest{})
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
		assert.Equal(t, msgDataCoordIsUnhealthy(Params.DataCoordCfg.GetNodeID()), resp.GetStatus().GetReason())
	})
}

func TestCompleteCompaction(t *testing.T) {
	Params.DataCoordCfg.EnableCompaction = true
	t.Run("test complete compaction successfully", func(t *testing.T) {
		svr := &Server{}
		svr.isServing = ServerStateHealthy

		svr.compactionHandler = &mockCompactionHandler{
			methods: map[string]interface{}{
				"completeCompaction": func(result *datapb.CompactionResult) error {
					return nil
				},
			},
		}
		status, err := svr.CompleteCompaction(context.TODO(), &datapb.CompactionResult{})
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
	})

	t.Run("test complete compaction failure", func(t *testing.T) {
		svr := &Server{}
		svr.isServing = ServerStateHealthy
		svr.compactionHandler = &mockCompactionHandler{
			methods: map[string]interface{}{
				"completeCompaction": func(result *datapb.CompactionResult) error {
					return errors.New("mock error")
				},
			},
		}
		status, err := svr.CompleteCompaction(context.TODO(), &datapb.CompactionResult{})
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.ErrorCode)
	})

	t.Run("with closed server", func(t *testing.T) {
		svr := &Server{}
		svr.isServing = ServerStateStopped

		resp, err := svr.CompleteCompaction(context.Background(), &datapb.CompactionResult{})
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetErrorCode())
		assert.Equal(t, msgDataCoordIsUnhealthy(Params.DataCoordCfg.GetNodeID()), resp.GetReason())
	})
}

func TestManualCompaction(t *testing.T) {
	Params.DataCoordCfg.EnableCompaction = true
	t.Run("test manual compaction successfully", func(t *testing.T) {
		svr := &Server{allocator: &MockAllocator{}}
		svr.isServing = ServerStateHealthy
		svr.compactionTrigger = &mockCompactionTrigger{
			methods: map[string]interface{}{
				"forceTriggerCompaction": func(collectionID int64, tt *timetravel) (UniqueID, error) {
					return 1, nil
				},
			},
		}

		resp, err := svr.ManualCompaction(context.TODO(), &milvuspb.ManualCompactionRequest{
			CollectionID: 1,
			Timetravel:   1,
		})
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("test manual compaction failure", func(t *testing.T) {
		svr := &Server{allocator: &MockAllocator{}}
		svr.isServing = ServerStateHealthy
		svr.compactionTrigger = &mockCompactionTrigger{
			methods: map[string]interface{}{
				"forceTriggerCompaction": func(collectionID int64, tt *timetravel) (UniqueID, error) {
					return 0, errors.New("mock error")
				},
			},
		}

		resp, err := svr.ManualCompaction(context.TODO(), &milvuspb.ManualCompactionRequest{
			CollectionID: 1,
			Timetravel:   1,
		})
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.Status.ErrorCode)
	})

	t.Run("test manual compaction with closed server", func(t *testing.T) {
		svr := &Server{}
		svr.isServing = ServerStateStopped
		svr.compactionTrigger = &mockCompactionTrigger{
			methods: map[string]interface{}{
				"forceTriggerCompaction": func(collectionID int64, tt *timetravel) (UniqueID, error) {
					return 1, nil
				},
			},
		}

		resp, err := svr.ManualCompaction(context.TODO(), &milvuspb.ManualCompactionRequest{
			CollectionID: 1,
			Timetravel:   1,
		})
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.Status.ErrorCode)
		assert.Equal(t, msgDataCoordIsUnhealthy(Params.DataCoordCfg.GetNodeID()), resp.Status.Reason)
	})
}

func TestGetCompactionStateWithPlans(t *testing.T) {
	t.Run("test get compaction state successfully", func(t *testing.T) {
		svr := &Server{}
		svr.isServing = ServerStateHealthy
		svr.compactionHandler = &mockCompactionHandler{
			methods: map[string]interface{}{
				"getCompactionTasksBySignalID": func(signalID int64) []*compactionTask {
					return []*compactionTask{
						{
							triggerInfo: &compactionSignal{id: 1},
							state:       executing,
						},
					}
				},
			},
		}

		resp, err := svr.GetCompactionStateWithPlans(context.TODO(), &milvuspb.GetCompactionPlansRequest{
			CompactionID: 1,
		})
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.Equal(t, commonpb.CompactionState_Executing, resp.State)
	})

	t.Run("test get compaction state with closed server", func(t *testing.T) {
		svr := &Server{}
		svr.isServing = ServerStateStopped
		svr.compactionHandler = &mockCompactionHandler{
			methods: map[string]interface{}{
				"getCompactionTasksBySignalID": func(signalID int64) []*compactionTask {
					return []*compactionTask{
						{
							triggerInfo: &compactionSignal{id: 1},
							state:       executing,
						},
					}
				},
			},
		}

		resp, err := svr.GetCompactionStateWithPlans(context.TODO(), &milvuspb.GetCompactionPlansRequest{
			CompactionID: 1,
		})
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.Status.ErrorCode)
		assert.Equal(t, msgDataCoordIsUnhealthy(Params.DataCoordCfg.GetNodeID()), resp.Status.Reason)
	})
}

func TestOptions(t *testing.T) {
	kv := getMetaKv(t)
	defer func() {
		kv.RemoveWithPrefix("")
		kv.Close()
	}()

	t.Run("SetRootCoordCreator", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)
		var crt rootCoordCreatorFunc = func(ctx context.Context, metaRoot string, etcdClient *clientv3.Client) (types.RootCoord, error) {
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
		defer kv.RemoveWithPrefix("")

		sessionManager := NewSessionManager()
		channelManager, err := NewChannelManager(kv, newMockHandler())
		assert.Nil(t, err)

		cluster := NewCluster(sessionManager, channelManager)
		assert.Nil(t, err)
		opt := SetCluster(cluster)
		assert.NotNil(t, opt)
		svr := newTestServer(t, nil, opt)
		defer closeTestServer(t, svr)

		assert.Same(t, cluster, svr.cluster)
	})
	t.Run("SetDataNodeCreator", func(t *testing.T) {
		var target int64
		var val = rand.Int63()
		opt := SetDataNodeCreator(func(context.Context, string) (types.DataNode, error) {
			target = val
			return nil, nil
		})
		assert.NotNil(t, opt)

		factory := dependency.NewDefaultFactory(true)

		svr := CreateServer(context.TODO(), factory, opt)
		dn, err := svr.dataNodeCreator(context.Background(), "")
		assert.Nil(t, dn)
		assert.Nil(t, err)
		assert.Equal(t, target, val)
	})
}

type mockPolicyFactory struct {
	ChannelPolicyFactoryV1
}

// NewRegisterPolicy create a new register policy
func (p *mockPolicyFactory) NewRegisterPolicy() RegisterPolicy {
	return EmptyRegister
}

// NewDeregisterPolicy create a new dereigster policy
func (p *mockPolicyFactory) NewDeregisterPolicy() DeregisterPolicy {
	return EmptyDeregisterPolicy
}

func TestHandleSessionEvent(t *testing.T) {
	kv := getMetaKv(t)
	defer func() {
		kv.RemoveWithPrefix("")
		kv.Close()
	}()
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	channelManager, err := NewChannelManager(kv, newMockHandler(), withFactory(&mockPolicyFactory{}))
	assert.Nil(t, err)
	sessionManager := NewSessionManager()
	cluster := NewCluster(sessionManager, channelManager)
	assert.Nil(t, err)

	err = cluster.Startup(ctx, nil)
	assert.Nil(t, err)
	defer cluster.Close()

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
		err = svr.handleSessionEvent(context.Background(), evt)
		assert.Nil(t, err)

		evt = &sessionutil.SessionEvent{
			EventType: sessionutil.SessionAddEvent,
			Session: &sessionutil.Session{
				ServerID:   101,
				ServerName: "DN101",
				Address:    "DN127.0.0.101",
				Exclusive:  false,
			},
		}
		err = svr.handleSessionEvent(context.Background(), evt)
		assert.Nil(t, err)
		dataNodes := svr.cluster.GetSessions()
		assert.EqualValues(t, 1, len(dataNodes))
		assert.EqualValues(t, "DN127.0.0.101", dataNodes[0].info.Address)

		evt = &sessionutil.SessionEvent{
			EventType: sessionutil.SessionDelEvent,
			Session: &sessionutil.Session{
				ServerID:   101,
				ServerName: "DN101",
				Address:    "DN127.0.0.101",
				Exclusive:  false,
			},
		}
		err = svr.handleSessionEvent(context.Background(), evt)
		assert.Nil(t, err)
		dataNodes = svr.cluster.GetSessions()
		assert.EqualValues(t, 0, len(dataNodes))
	})

	t.Run("nil evt", func(t *testing.T) {
		assert.NotPanics(t, func() {
			err = svr.handleSessionEvent(context.Background(), nil)
			assert.Nil(t, err)
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

func TestGetFlushState(t *testing.T) {
	t.Run("get flush state with all flushed segments", func(t *testing.T) {
		svr := &Server{
			isServing: ServerStateHealthy,
			meta: &meta{
				segments: &SegmentsInfo{
					segments: map[int64]*SegmentInfo{
						1: {
							SegmentInfo: &datapb.SegmentInfo{
								ID:    1,
								State: commonpb.SegmentState_Flushed,
							},
						},
						2: {
							SegmentInfo: &datapb.SegmentInfo{
								ID:    2,
								State: commonpb.SegmentState_Flushed,
							},
						},
					},
				},
			},
		}

		resp, err := svr.GetFlushState(context.TODO(), &milvuspb.GetFlushStateRequest{SegmentIDs: []int64{1, 2}})
		assert.Nil(t, err)
		assert.EqualValues(t, &milvuspb.GetFlushStateResponse{
			Status:  &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			Flushed: true,
		}, resp)
	})

	t.Run("get flush state with unflushed segments", func(t *testing.T) {
		svr := &Server{
			isServing: ServerStateHealthy,
			meta: &meta{
				segments: &SegmentsInfo{
					segments: map[int64]*SegmentInfo{
						1: {
							SegmentInfo: &datapb.SegmentInfo{
								ID:    1,
								State: commonpb.SegmentState_Flushed,
							},
						},
						2: {
							SegmentInfo: &datapb.SegmentInfo{
								ID:    2,
								State: commonpb.SegmentState_Sealed,
							},
						},
					},
				},
			},
		}

		resp, err := svr.GetFlushState(context.TODO(), &milvuspb.GetFlushStateRequest{SegmentIDs: []int64{1, 2}})
		assert.Nil(t, err)
		assert.EqualValues(t, &milvuspb.GetFlushStateResponse{
			Status:  &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			Flushed: false,
		}, resp)
	})

	t.Run("get flush state with compacted segments", func(t *testing.T) {
		svr := &Server{
			isServing: ServerStateHealthy,
			meta: &meta{
				segments: &SegmentsInfo{
					segments: map[int64]*SegmentInfo{
						1: {
							SegmentInfo: &datapb.SegmentInfo{
								ID:    1,
								State: commonpb.SegmentState_Flushed,
							},
						},
						2: {
							SegmentInfo: &datapb.SegmentInfo{
								ID:    2,
								State: commonpb.SegmentState_Dropped,
							},
						},
					},
				},
			},
		}

		resp, err := svr.GetFlushState(context.TODO(), &milvuspb.GetFlushStateRequest{SegmentIDs: []int64{1, 2}})
		assert.Nil(t, err)
		assert.EqualValues(t, &milvuspb.GetFlushStateResponse{
			Status:  &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			Flushed: true,
		}, resp)
	})
}

type mockTxnKVext struct {
	kv.MockTxnKV
}

func (m *mockTxnKVext) LoadWithPrefix(prefix string) ([]string, []string, error) {
	return []string{}, []string{}, nil
}

func (m *mockTxnKVext) MultiSave(kvs map[string]string) error {
	return errors.New("(testing only) injected error")
}

func TestDataCoordServer_SetSegmentState(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
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
		// Set segment state.
		svr.SetSegmentState(context.TODO(), &datapb.SetSegmentStateRequest{
			SegmentId: 1000,
			NewState:  commonpb.SegmentState_Flushed,
		})
		// Verify that the state has been updated.
		resp, err := svr.GetSegmentStates(context.TODO(), &datapb.GetSegmentStatesRequest{
			Base: &commonpb.MsgBase{
				MsgType:   0,
				MsgID:     0,
				Timestamp: 0,
				SourceID:  0,
			},
			SegmentIDs: []int64{1000},
		})
		assert.Nil(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.EqualValues(t, 1, len(resp.States))
		assert.EqualValues(t, commonpb.SegmentState_Flushed, resp.States[0].State)
	})

	t.Run("dataCoord meta set state error", func(t *testing.T) {
		svr := newTestServer(t, nil)
		svr.meta.Lock()
		func() {
			defer svr.meta.Unlock()
			svr.meta, _ = newMeta(&mockTxnKVext{})
		}()
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
		svr.meta.AddSegment(NewSegmentInfo(segment))
		// Set segment state.
		svr.SetSegmentState(context.TODO(), &datapb.SetSegmentStateRequest{
			SegmentId: 1000,
			NewState:  commonpb.SegmentState_Flushed,
		})
		// Verify that the state has been updated.
		resp, err := svr.GetSegmentStates(context.TODO(), &datapb.GetSegmentStatesRequest{
			Base: &commonpb.MsgBase{
				MsgType:   0,
				MsgID:     0,
				Timestamp: 0,
				SourceID:  0,
			},
			SegmentIDs: []int64{1000},
		})
		assert.Nil(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.EqualValues(t, 1, len(resp.States))
		assert.EqualValues(t, commonpb.SegmentState_Flushed, resp.States[0].State)
	})

	t.Run("with closed server", func(t *testing.T) {
		svr := newTestServer(t, nil)
		closeTestServer(t, svr)
		resp, err := svr.SetSegmentState(context.TODO(), &datapb.SetSegmentStateRequest{
			SegmentId: 1000,
			NewState:  commonpb.SegmentState_Flushed,
		})
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
		assert.Equal(t, serverNotServingErrMsg, resp.GetStatus().GetReason())
	})
}

func TestDataCoord_Import(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)

		err := svr.channelManager.AddNode(0)
		assert.Nil(t, err)
		err = svr.channelManager.Watch(&channel{"ch1", 0})
		assert.Nil(t, err)

		resp, err := svr.Import(svr.ctx, &datapb.ImportTaskRequest{
			ImportTask: &datapb.ImportTask{
				CollectionId: 100,
				PartitionId:  100,
			},
		})
		assert.Nil(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.GetErrorCode())
		etcd.StopEtcdServer()
	})

	t.Run("no free node", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)

		err := svr.channelManager.AddNode(0)
		assert.Nil(t, err)
		err = svr.channelManager.Watch(&channel{"ch1", 0})
		assert.Nil(t, err)

		resp, err := svr.Import(svr.ctx, &datapb.ImportTaskRequest{
			ImportTask: &datapb.ImportTask{
				CollectionId: 100,
				PartitionId:  100,
			},
			WorkingNodes: []int64{0},
		})
		assert.Nil(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_UnexpectedError, resp.Status.GetErrorCode())
		etcd.StopEtcdServer()
	})

	t.Run("no datanode available", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)

		resp, err := svr.Import(svr.ctx, &datapb.ImportTaskRequest{
			ImportTask: &datapb.ImportTask{
				CollectionId: 100,
				PartitionId:  100,
			},
		})
		assert.Nil(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_UnexpectedError, resp.Status.GetErrorCode())
		etcd.StopEtcdServer()
	})

	t.Run("with closed server", func(t *testing.T) {
		svr := newTestServer(t, nil)
		closeTestServer(t, svr)

		resp, err := svr.Import(svr.ctx, &datapb.ImportTaskRequest{
			ImportTask: &datapb.ImportTask{
				CollectionId: 100,
				PartitionId:  100,
			},
		})
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.Status.GetErrorCode())
		assert.Equal(t, msgDataCoordIsUnhealthy(Params.DataCoordCfg.GetNodeID()), resp.Status.GetReason())
	})

	t.Run("test update segment stat", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)

		status, err := svr.UpdateSegmentStatistics(context.TODO(), &datapb.UpdateSegmentStatisticsRequest{
			Stats: []*datapb.SegmentStats{{
				SegmentID: 100,
				NumRows:   int64(1),
			}},
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, status.GetErrorCode())
	})

	t.Run("test update segment stat w/ closed server", func(t *testing.T) {
		svr := newTestServer(t, nil)
		closeTestServer(t, svr)

		status, err := svr.UpdateSegmentStatistics(context.TODO(), &datapb.UpdateSegmentStatisticsRequest{
			Stats: []*datapb.SegmentStats{{
				SegmentID: 100,
				NumRows:   int64(1),
			}},
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.GetErrorCode())
	})

	t.Run("test acquire segment reference lock with closed server", func(t *testing.T) {
		svr := newTestServer(t, nil)
		closeTestServer(t, svr)

		status, err := svr.AcquireSegmentLock(context.TODO(), &datapb.AcquireSegmentLockRequest{
			SegmentIDs: []UniqueID{1, 2},
			NodeID:     UniqueID(1),
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.GetErrorCode())
	})

	t.Run("test release segment reference lock with closed server", func(t *testing.T) {
		svr := newTestServer(t, nil)
		closeTestServer(t, svr)

		status, err := svr.ReleaseSegmentLock(context.TODO(), &datapb.ReleaseSegmentLockRequest{
			SegmentIDs: []UniqueID{1, 2},
			NodeID:     UniqueID(1),
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.GetErrorCode())
	})
}

func TestDataCoord_AddSegment(t *testing.T) {
	t.Run("test add segment", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)

		err := svr.channelManager.AddNode(110)
		assert.Nil(t, err)
		err = svr.channelManager.Watch(&channel{"ch1", 100})
		assert.Nil(t, err)

		status, err := svr.AddSegment(context.TODO(), &datapb.AddSegmentRequest{
			SegmentId:    100,
			ChannelName:  "ch1",
			CollectionId: 100,
			PartitionId:  100,
			RowNum:       int64(1),
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, status.GetErrorCode())
	})

	t.Run("test add segment w/ bad channel name", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)

		err := svr.channelManager.AddNode(110)
		assert.Nil(t, err)
		err = svr.channelManager.Watch(&channel{"ch1", 100})
		assert.Nil(t, err)

		status, err := svr.AddSegment(context.TODO(), &datapb.AddSegmentRequest{
			SegmentId:    100,
			ChannelName:  "non-channel",
			CollectionId: 100,
			PartitionId:  100,
			RowNum:       int64(1),
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.GetErrorCode())
	})

	t.Run("test add segment w/ closed server", func(t *testing.T) {
		svr := newTestServer(t, nil)
		closeTestServer(t, svr)

		status, err := svr.AddSegment(context.TODO(), &datapb.AddSegmentRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.GetErrorCode())
	})
}

// https://github.com/milvus-io/milvus/issues/15659
func TestIssue15659(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{
		helper: ServerHelper{
			eventAfterHandleDataNodeTt: func() {},
		},
	}
	ms := &MockClosePanicMsgstream{}

	msgChan := make(chan *msgstream.MsgPack)
	go func() {
		msgChan <- &msgstream.MsgPack{}
	}()
	ms.On("Chan").Return(msgChan)

	ch := make(chan struct{})
	go func() {
		assert.NotPanics(t, func() {
			s.serverLoopWg.Add(1)
			s.handleDataNodeTimetickMsgstream(ctx, ms)
			close(ch)
		})
	}()
	cancel()
	<-ch
}

type MockClosePanicMsgstream struct {
	mock.Mock
	msgstream.MsgStream
}

func (ms *MockClosePanicMsgstream) Close() {
	panic("mocked close panic")
}

func (ms *MockClosePanicMsgstream) Chan() <-chan *msgstream.MsgPack {
	args := ms.Called()
	return args.Get(0).(chan *msgstream.MsgPack)
}

func newTestServer(t *testing.T, receiveCh chan interface{}, opts ...Option) *Server {
	var err error
	Params.Init()
	Params.CommonCfg.DataCoordTimeTick = Params.CommonCfg.DataCoordTimeTick + strconv.Itoa(rand.Int())
	factory := dependency.NewDefaultFactory(true)

	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	assert.Nil(t, err)
	sessKey := path.Join(Params.EtcdCfg.MetaRootPath, sessionutil.DefaultServiceRoot)
	_, err = etcdCli.Delete(context.Background(), sessKey, clientv3.WithPrefix())
	assert.Nil(t, err)

	svr := CreateServer(context.TODO(), factory, opts...)
	svr.SetEtcdClient(etcdCli)
	svr.dataNodeCreator = func(ctx context.Context, addr string) (types.DataNode, error) {
		return newMockDataNodeClient(0, receiveCh)
	}
	svr.rootCoordClientCreator = func(ctx context.Context, metaRootPath string, etcdCli *clientv3.Client) (types.RootCoord, error) {
		return newMockRootCoordService(), nil
	}

	err = svr.Init()
	assert.Nil(t, err)
	err = svr.Start()
	assert.Nil(t, err)
	err = svr.Register()
	assert.Nil(t, err)

	// Stop channal watch state watcher in tests
	if svr.channelManager != nil && svr.channelManager.stopChecker != nil {
		svr.channelManager.stopChecker()
	}

	return svr
}

func closeTestServer(t *testing.T, svr *Server) {
	err := svr.Stop()
	assert.Nil(t, err)
	err = svr.CleanMeta()
	assert.Nil(t, err)
}

func newTestServer2(t *testing.T, receiveCh chan interface{}, opts ...Option) *Server {
	var err error
	Params.Init()
	Params.CommonCfg.DataCoordTimeTick = Params.CommonCfg.DataCoordTimeTick + strconv.Itoa(rand.Int())
	factory := dependency.NewDefaultFactory(true)

	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	assert.Nil(t, err)
	sessKey := path.Join(Params.EtcdCfg.MetaRootPath, sessionutil.DefaultServiceRoot)
	_, err = etcdCli.Delete(context.Background(), sessKey, clientv3.WithPrefix())
	assert.Nil(t, err)

	icSession := sessionutil.NewSession(context.Background(), Params.EtcdCfg.MetaRootPath, etcdCli)
	icSession.Init(typeutil.IndexCoordRole, "localhost:31000", true, true)
	icSession.Register()

	qcSession := sessionutil.NewSession(context.Background(), Params.EtcdCfg.MetaRootPath, etcdCli)
	qcSession.Init(typeutil.QueryCoordRole, "localhost:19532", true, true)
	qcSession.Register()

	svr := CreateServer(context.TODO(), factory, opts...)
	svr.SetEtcdClient(etcdCli)
	svr.dataNodeCreator = func(ctx context.Context, addr string) (types.DataNode, error) {
		return newMockDataNodeClient(0, receiveCh)
	}
	svr.rootCoordClientCreator = func(ctx context.Context, metaRootPath string, etcdCli *clientv3.Client) (types.RootCoord, error) {
		return newMockRootCoordService(), nil
	}

	err = svr.Init()
	assert.Nil(t, err)
	err = svr.Start()
	assert.Nil(t, err)

	_, err = etcdCli.Delete(context.Background(), sessKey, clientv3.WithPrefix())
	assert.Nil(t, err)

	err = svr.Register()
	assert.Nil(t, err)

	// Stop channal watch state watcher in tests
	if svr.channelManager != nil && svr.channelManager.stopChecker != nil {
		svr.channelManager.stopChecker()
	}

	return svr
}

func Test_initServiceDiscovery(t *testing.T) {
	server := newTestServer2(t, nil)
	assert.NotNil(t, server)

	segmentID := rand.Int63()
	err := server.meta.AddSegment(&SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:           segmentID,
			CollectionID: rand.Int63(),
			PartitionID:  rand.Int63(),
			NumOfRows:    100,
		},
		currRows: 100,
	})
	assert.Nil(t, err)

	qcSession := sessionutil.NewSession(context.Background(), Params.EtcdCfg.MetaRootPath, server.etcdCli)
	qcSession.Init(typeutil.QueryCoordRole, "localhost:19532", true, true)
	qcSession.Register()
	req := &datapb.AcquireSegmentLockRequest{
		NodeID:     qcSession.ServerID,
		SegmentIDs: []UniqueID{segmentID},
	}
	resp, err := server.AcquireSegmentLock(context.TODO(), req)
	assert.Nil(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())

	sessKey := path.Join(Params.EtcdCfg.MetaRootPath, sessionutil.DefaultServiceRoot, typeutil.QueryCoordRole)
	_, err = server.etcdCli.Delete(context.Background(), sessKey, clientv3.WithPrefix())
	assert.Nil(t, err)

	for {
		if !server.segReferManager.HasSegmentLock(segmentID) {
			break
		}
	}

	closeTestServer(t, server)
}
