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
	"math/rand"
	"os"
	"os/signal"
	"path"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/proto/workerpb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/tikv"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func TestMain(m *testing.M) {
	// init embed etcd
	embedetcdServer, tempDir, err := etcd.StartTestEmbedEtcdServer()
	if err != nil {
		log.Fatal("failed to start embed etcd server", zap.Error(err))
	}
	defer os.RemoveAll(tempDir)
	defer embedetcdServer.Close()

	addrs := etcd.GetEmbedEtcdEndpoints(embedetcdServer)

	paramtable.Init()
	paramtable.Get().Save(Params.EtcdCfg.Endpoints.Key, strings.Join(addrs, ","))

	rand.Seed(time.Now().UnixNano())
	parameters := []string{"tikv", "etcd"}
	var code int
	for _, v := range parameters {
		paramtable.Get().Save(paramtable.Get().MetaStoreCfg.MetaStoreType.Key, v)
		code = m.Run()
	}
	os.Exit(code)
}

type mockRootCoord struct {
	types.RootCoordClient
	collID UniqueID
}

func (r *mockRootCoord) DescribeCollectionInternal(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
	if req.CollectionID != r.collID {
		return &milvuspb.DescribeCollectionResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "Collection not found",
			},
		}, nil
	}
	return r.RootCoordClient.DescribeCollection(ctx, req)
}

func TestGetTimeTickChannel(t *testing.T) {
	svr := newTestServer(t)
	defer closeTestServer(t, svr)
	resp, err := svr.GetTimeTickChannel(context.TODO(), nil)
	assert.NoError(t, err)
	assert.EqualValues(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	assert.EqualValues(t, Params.CommonCfg.DataCoordTimeTick.GetValue(), resp.Value)
}

func TestGetSegmentStates(t *testing.T) {
	t.Run("normal cases", func(t *testing.T) {
		svr := newTestServer(t)
		defer closeTestServer(t, svr)
		segment := &datapb.SegmentInfo{
			ID:            1000,
			CollectionID:  100,
			PartitionID:   0,
			InsertChannel: "c1",
			NumOfRows:     0,
			State:         commonpb.SegmentState_Growing,
			StartPosition: &msgpb.MsgPosition{
				ChannelName: "c1",
				MsgID:       []byte{},
				MsgGroup:    "",
				Timestamp:   0,
			},
		}
		err := svr.meta.AddSegment(context.TODO(), NewSegmentInfo(segment))
		assert.NoError(t, err)

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
				assert.NoError(t, err)
				assert.EqualValues(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
				assert.EqualValues(t, 1, len(resp.States))
				if test.expected {
					assert.EqualValues(t, test.expectedState, resp.States[0].State)
				}
			})
		}
	})

	t.Run("with closed server", func(t *testing.T) {
		svr := newTestServer(t)
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
		assert.NoError(t, err)
		assert.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrServiceNotReady)
	})
}

func TestGetInsertBinlogPaths(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		svr := newTestServer(t)
		defer closeTestServer(t, svr)

		info := &datapb.SegmentInfo{
			ID: 0,
			Binlogs: []*datapb.FieldBinlog{
				{
					FieldID: 1,
					Binlogs: []*datapb.Binlog{
						{
							LogID: 1,
						},
						{
							LogID: 2,
						},
					},
				},
			},
			State: commonpb.SegmentState_Growing,
		}
		err := svr.meta.AddSegment(context.TODO(), NewSegmentInfo(info))
		assert.NoError(t, err)
		req := &datapb.GetInsertBinlogPathsRequest{
			SegmentID: 0,
		}
		resp, err := svr.GetInsertBinlogPaths(svr.ctx, req)
		assert.NoError(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("with invalid segmentID", func(t *testing.T) {
		svr := newTestServer(t)
		defer closeTestServer(t, svr)

		info := &datapb.SegmentInfo{
			ID: 0,
			Binlogs: []*datapb.FieldBinlog{
				{
					FieldID: 1,
					Binlogs: []*datapb.Binlog{
						{
							LogID: 1,
						},
						{
							LogID: 2,
						},
					},
				},
			},

			State: commonpb.SegmentState_Growing,
		}
		err := svr.meta.AddSegment(context.TODO(), NewSegmentInfo(info))
		assert.NoError(t, err)
		req := &datapb.GetInsertBinlogPathsRequest{
			SegmentID: 1,
		}
		resp, err := svr.GetInsertBinlogPaths(svr.ctx, req)
		assert.NoError(t, err)
		assert.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrSegmentNotFound)
	})

	t.Run("with closed server", func(t *testing.T) {
		svr := newTestServer(t)
		closeTestServer(t, svr)
		resp, err := svr.GetInsertBinlogPaths(context.TODO(), &datapb.GetInsertBinlogPathsRequest{
			SegmentID: 0,
		})
		assert.NoError(t, err)
		assert.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrServiceNotReady)
	})
}

func TestGetCollectionStatistics(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		svr := newTestServer(t)
		defer closeTestServer(t, svr)

		req := &datapb.GetCollectionStatisticsRequest{
			CollectionID: 0,
		}
		resp, err := svr.GetCollectionStatistics(svr.ctx, req)
		assert.NoError(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})
	t.Run("with closed server", func(t *testing.T) {
		svr := newTestServer(t)
		closeTestServer(t, svr)
		resp, err := svr.GetCollectionStatistics(context.Background(), &datapb.GetCollectionStatisticsRequest{
			CollectionID: 0,
		})
		assert.NoError(t, err)
		assert.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrServiceNotReady)
	})
}

func TestGetPartitionStatistics(t *testing.T) {
	t.Run("normal cases", func(t *testing.T) {
		svr := newTestServer(t)
		defer closeTestServer(t, svr)

		req := &datapb.GetPartitionStatisticsRequest{
			CollectionID: 0,
			PartitionIDs: []int64{0},
		}
		resp, err := svr.GetPartitionStatistics(context.Background(), req)
		assert.NoError(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})
	t.Run("with closed server", func(t *testing.T) {
		svr := newTestServer(t)
		closeTestServer(t, svr)
		resp, err := svr.GetPartitionStatistics(context.Background(), &datapb.GetPartitionStatisticsRequest{})
		assert.NoError(t, err)
		assert.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrServiceNotReady)
	})
}

func TestGetSegmentInfo(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		svr := newTestServer(t)
		defer closeTestServer(t, svr)

		segInfo := &datapb.SegmentInfo{
			ID:        0,
			State:     commonpb.SegmentState_Flushed,
			NumOfRows: 100,
			Binlogs: []*datapb.FieldBinlog{
				{
					FieldID: 1,
					Binlogs: []*datapb.Binlog{
						{
							EntriesNum: 20,
							LogID:      801,
						},
						{
							EntriesNum: 20,
							LogID:      802,
						},
						{
							EntriesNum: 20,
							LogID:      803,
						},
					},
				},
			},
		}
		err := svr.meta.AddSegment(context.TODO(), NewSegmentInfo(segInfo))
		assert.NoError(t, err)

		req := &datapb.GetSegmentInfoRequest{
			SegmentIDs: []int64{0},
		}
		resp, err := svr.GetSegmentInfo(svr.ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(resp.GetInfos()))
		// Check that # of rows is corrected from 100 to 60.
		assert.EqualValues(t, 60, resp.GetInfos()[0].GetNumOfRows())
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})
	t.Run("with wrong segmentID", func(t *testing.T) {
		svr := newTestServer(t)
		defer closeTestServer(t, svr)

		segInfo := &datapb.SegmentInfo{
			ID:    0,
			State: commonpb.SegmentState_Flushed,
		}
		err := svr.meta.AddSegment(context.TODO(), NewSegmentInfo(segInfo))
		assert.NoError(t, err)

		req := &datapb.GetSegmentInfoRequest{
			SegmentIDs: []int64{0, 1},
		}
		resp, err := svr.GetSegmentInfo(svr.ctx, req)
		assert.NoError(t, err)
		assert.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrSegmentNotFound)
	})
	t.Run("with closed server", func(t *testing.T) {
		svr := newTestServer(t)
		closeTestServer(t, svr)
		resp, err := svr.GetSegmentInfo(context.Background(), &datapb.GetSegmentInfoRequest{
			SegmentIDs: []int64{},
		})
		assert.NoError(t, err)
		assert.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrServiceNotReady)
	})
	t.Run("with dropped segment", func(t *testing.T) {
		svr := newTestServer(t)
		defer closeTestServer(t, svr)

		segInfo := &datapb.SegmentInfo{
			ID:    0,
			State: commonpb.SegmentState_Dropped,
		}
		err := svr.meta.AddSegment(context.TODO(), NewSegmentInfo(segInfo))
		assert.NoError(t, err)

		req := &datapb.GetSegmentInfoRequest{
			SegmentIDs:       []int64{0},
			IncludeUnHealthy: false,
		}
		resp, err := svr.GetSegmentInfo(svr.ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(resp.Infos))

		req = &datapb.GetSegmentInfoRequest{
			SegmentIDs:       []int64{0},
			IncludeUnHealthy: true,
		}
		resp2, err := svr.GetSegmentInfo(svr.ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(resp2.Infos))
	})

	t.Run("with channel checkpoint", func(t *testing.T) {
		mockVChannel := "fake-by-dev-rootcoord-dml-1-testgetsegmentinfo-v0"
		mockPChannel := "fake-by-dev-rootcoord-dml-1"

		pos := &msgpb.MsgPosition{
			ChannelName: mockPChannel,
			MsgID:       []byte{0, 0, 0, 0, 0, 0, 0, 0},
			Timestamp:   1000,
		}

		svr := newTestServer(t)
		defer closeTestServer(t, svr)

		segInfo := &datapb.SegmentInfo{
			ID:    0,
			State: commonpb.SegmentState_Flushed,
		}
		err := svr.meta.AddSegment(context.TODO(), NewSegmentInfo(segInfo))
		assert.NoError(t, err)

		req := &datapb.GetSegmentInfoRequest{
			SegmentIDs: []int64{0},
		}
		// no channel checkpoint
		resp, err := svr.GetSegmentInfo(svr.ctx, req)
		assert.NoError(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		assert.Equal(t, 0, len(resp.GetChannelCheckpoint()))

		// with nil insert channel of segment
		err = svr.meta.UpdateChannelCheckpoint(mockVChannel, pos)
		assert.NoError(t, err)
		resp, err = svr.GetSegmentInfo(svr.ctx, req)
		assert.NoError(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		assert.Equal(t, 0, len(resp.GetChannelCheckpoint()))

		// normal test
		segInfo.InsertChannel = mockVChannel
		segInfo.ID = 2
		req.SegmentIDs = []int64{2}
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(segInfo))
		assert.NoError(t, err)
		resp, err = svr.GetSegmentInfo(svr.ctx, req)
		assert.NoError(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		assert.Equal(t, 1, len(resp.GetChannelCheckpoint()))
		assert.Equal(t, mockPChannel, resp.ChannelCheckpoint[mockVChannel].ChannelName)
		assert.Equal(t, Timestamp(1000), resp.ChannelCheckpoint[mockVChannel].Timestamp)
	})
}

func TestGetComponentStates(t *testing.T) {
	svr := &Server{}
	resp, err := svr.GetComponentStates(context.Background(), nil)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	assert.Equal(t, common.NotRegisteredID, resp.State.NodeID)
	svr.session = &sessionutil.Session{}
	svr.session.UpdateRegistered(true)
	type testCase struct {
		state commonpb.StateCode
		code  commonpb.StateCode
	}
	cases := []testCase{
		{state: commonpb.StateCode_Abnormal, code: commonpb.StateCode_Abnormal},
		{state: commonpb.StateCode_Initializing, code: commonpb.StateCode_Initializing},
		{state: commonpb.StateCode_Healthy, code: commonpb.StateCode_Healthy},
	}
	for _, tc := range cases {
		svr.stateCode.Store(tc.state)
		resp, err := svr.GetComponentStates(context.Background(), nil)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		assert.Equal(t, tc.code, resp.GetState().GetStateCode())
	}
}

func TestGetFlushedSegments(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		svr := newTestServer(t)
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
				assert.Nil(t, svr.meta.AddSegment(context.TODO(), NewSegmentInfo(segInfo)))
			}
			for _, us := range tc.unflushedSegments {
				segInfo := &datapb.SegmentInfo{
					ID:           us,
					CollectionID: tc.collID,
					PartitionID:  tc.partID,
					State:        commonpb.SegmentState_Growing,
				}
				assert.Nil(t, svr.meta.AddSegment(context.TODO(), NewSegmentInfo(segInfo)))
			}

			resp, err := svr.GetFlushedSegments(context.Background(), &datapb.GetFlushedSegmentsRequest{
				CollectionID: tc.collID,
				PartitionID:  tc.searchPartID,
			})
			assert.NoError(t, err)
			assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())

			assert.ElementsMatch(t, tc.expected, resp.GetSegments())
		}
	})

	t.Run("with closed server", func(t *testing.T) {
		t.Run("with closed server", func(t *testing.T) {
			svr := newTestServer(t)
			closeTestServer(t, svr)
			resp, err := svr.GetFlushedSegments(context.Background(), &datapb.GetFlushedSegmentsRequest{})
			assert.NoError(t, err)
			assert.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrServiceNotReady)
		})
	})
}

func TestGetSegmentsByStates(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		svr := newTestServer(t)
		defer closeTestServer(t, svr)
		type testCase struct {
			collID          int64
			partID          int64
			searchPartID    int64
			flushedSegments []int64
			sealedSegments  []int64
			growingSegments []int64
			expected        []int64
		}
		cases := []testCase{
			{
				collID:          1,
				partID:          1,
				searchPartID:    1,
				flushedSegments: []int64{1, 2, 3},
				sealedSegments:  []int64{4},
				growingSegments: []int64{5},
				expected:        []int64{1, 2, 3, 4},
			},
			{
				collID:          1,
				partID:          2,
				searchPartID:    2,
				flushedSegments: []int64{6, 7},
				sealedSegments:  []int64{},
				growingSegments: []int64{8},
				expected:        []int64{6, 7},
			},
			{
				collID:          2,
				partID:          3,
				searchPartID:    3,
				flushedSegments: []int64{9, 10},
				sealedSegments:  []int64{},
				growingSegments: []int64{11},
				expected:        []int64{9, 10},
			},
			{
				collID:       1,
				searchPartID: -1,
				expected:     []int64{1, 2, 3, 4, 6, 7},
			},
			{
				collID:       2,
				searchPartID: -1,
				expected:     []int64{9, 10},
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
				assert.Nil(t, svr.meta.AddSegment(context.TODO(), NewSegmentInfo(segInfo)))
			}
			for _, us := range tc.sealedSegments {
				segInfo := &datapb.SegmentInfo{
					ID:           us,
					CollectionID: tc.collID,
					PartitionID:  tc.partID,
					State:        commonpb.SegmentState_Sealed,
				}
				assert.Nil(t, svr.meta.AddSegment(context.TODO(), NewSegmentInfo(segInfo)))
			}
			for _, us := range tc.growingSegments {
				segInfo := &datapb.SegmentInfo{
					ID:           us,
					CollectionID: tc.collID,
					PartitionID:  tc.partID,
					State:        commonpb.SegmentState_Growing,
				}
				assert.Nil(t, svr.meta.AddSegment(context.TODO(), NewSegmentInfo(segInfo)))
			}

			resp, err := svr.GetSegmentsByStates(context.Background(), &datapb.GetSegmentsByStatesRequest{
				CollectionID: tc.collID,
				PartitionID:  tc.searchPartID,
				States:       []commonpb.SegmentState{commonpb.SegmentState_Sealed, commonpb.SegmentState_Flushed},
			})
			assert.NoError(t, err)
			assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())

			assert.ElementsMatch(t, tc.expected, resp.GetSegments())
		}
	})

	t.Run("with closed server", func(t *testing.T) {
		t.Run("with closed server", func(t *testing.T) {
			svr := newTestServer(t)
			closeTestServer(t, svr)
			resp, err := svr.GetSegmentsByStates(context.Background(), &datapb.GetSegmentsByStatesRequest{})
			assert.NoError(t, err)
			assert.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrServiceNotReady)
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
		SessionRaw: sessionutil.SessionRaw{TriggerKill: true},
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

func TestServer_ShowConfigurations(t *testing.T) {
	svr := newTestServer(t)
	defer closeTestServer(t, svr)
	pattern := "datacoord.Port"
	req := &internalpb.ShowConfigurationsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_WatchQueryChannels,
			MsgID:   rand.Int63(),
		},
		Pattern: pattern,
	}

	// server is closed
	stateSave := svr.stateCode.Load()
	svr.stateCode.Store(commonpb.StateCode_Initializing)
	resp, err := svr.ShowConfigurations(svr.ctx, req)
	assert.NoError(t, err)
	assert.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrServiceNotReady)

	// normal case
	svr.stateCode.Store(stateSave)

	resp, err = svr.ShowConfigurations(svr.ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	assert.Equal(t, 1, len(resp.Configuations))
	assert.Equal(t, "datacoord.port", resp.Configuations[0].Key)
}

func TestServer_GetMetrics(t *testing.T) {
	svr := newTestServer(t)
	defer closeTestServer(t, svr)

	var err error

	// server is closed
	stateSave := svr.stateCode.Load()
	svr.stateCode.Store(commonpb.StateCode_Initializing)
	resp, err := svr.GetMetrics(svr.ctx, &milvuspb.GetMetricsRequest{})
	assert.NoError(t, err)
	assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	svr.stateCode.Store(stateSave)

	// failed to parse metric type
	invalidRequest := "invalid request"
	resp, err = svr.GetMetrics(svr.ctx, &milvuspb.GetMetricsRequest{
		Request: invalidRequest,
	})
	assert.NoError(t, err)
	assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())

	// unsupported metric type
	unsupportedMetricType := "unsupported"
	req, err := metricsinfo.ConstructRequestByMetricType(unsupportedMetricType)
	assert.NoError(t, err)
	resp, err = svr.GetMetrics(svr.ctx, req)
	assert.NoError(t, err)
	assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())

	// normal case
	req, err = metricsinfo.ConstructRequestByMetricType(metricsinfo.SystemInfoMetrics)
	assert.NoError(t, err)
	resp, err = svr.GetMetrics(svr.ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	log.Info("TestServer_GetMetrics",
		zap.String("name", resp.ComponentName),
		zap.String("response", resp.Response))
}

func TestServer_getSystemInfoMetrics(t *testing.T) {
	svr := newTestServer(t)
	defer closeTestServer(t, svr)

	req, err := metricsinfo.ConstructRequestByMetricType(metricsinfo.SystemInfoMetrics)
	assert.NoError(t, err)
	resp, err := svr.getSystemInfoMetrics(svr.ctx, req)
	assert.NoError(t, err)
	log.Info("TestServer_getSystemInfoMetrics",
		zap.String("name", resp.ComponentName),
		zap.String("response", resp.Response))

	var coordTopology metricsinfo.DataCoordTopology
	err = metricsinfo.UnmarshalTopology(resp.Response, &coordTopology)
	assert.NoError(t, err)
	assert.Equal(t, len(svr.cluster.GetSessions()), len(coordTopology.Cluster.ConnectedDataNodes))
	for _, nodeMetrics := range coordTopology.Cluster.ConnectedDataNodes {
		assert.Equal(t, false, nodeMetrics.HasError)
		assert.Equal(t, 0, len(nodeMetrics.ErrorReason))
		_, err = metricsinfo.MarshalComponentInfos(nodeMetrics)
		assert.NoError(t, err)
	}
}

type spySegmentManager struct {
	spyCh chan struct{}
}

// AllocSegment allocates rows and record the allocation.
func (s *spySegmentManager) AllocSegment(ctx context.Context, collectionID UniqueID, partitionID UniqueID, channelName string, requestRows int64) ([]*Allocation, error) {
	return nil, nil
}

func (s *spySegmentManager) AllocNewGrowingSegment(ctx context.Context, collectionID, partitionID, segmentID UniqueID, channelName string) (*SegmentInfo, error) {
	return nil, nil
}

func (s *spySegmentManager) allocSegmentForImport(ctx context.Context, collectionID UniqueID, partitionID UniqueID, channelName string, requestRows int64, taskID int64) (*Allocation, error) {
	return nil, nil
}

func (s *spySegmentManager) AllocImportSegment(ctx context.Context, taskID int64, collectionID UniqueID, partitionID UniqueID, channelName string, level datapb.SegmentLevel) (*SegmentInfo, error) {
	return nil, nil
}

// DropSegment drops the segment from manager.
func (s *spySegmentManager) DropSegment(ctx context.Context, segmentID UniqueID) {
}

// FlushImportSegments set importing segment state to Flushed.
func (s *spySegmentManager) FlushImportSegments(ctx context.Context, collectionID UniqueID, segmentIDs []UniqueID) error {
	return nil
}

// SealAllSegments seals all segments of collection with collectionID and return sealed segments
func (s *spySegmentManager) SealAllSegments(ctx context.Context, collectionID UniqueID, segIDs []UniqueID) ([]UniqueID, error) {
	return nil, nil
}

// GetFlushableSegments returns flushable segment ids
func (s *spySegmentManager) GetFlushableSegments(ctx context.Context, channel string, ts Timestamp) ([]UniqueID, error) {
	return nil, nil
}

// ExpireAllocations notifies segment status to expire old allocations
func (s *spySegmentManager) ExpireAllocations(channel string, ts Timestamp) error {
	return nil
}

// DropSegmentsOfChannel drops all segments in a channel
func (s *spySegmentManager) DropSegmentsOfChannel(ctx context.Context, channel string) {
	s.spyCh <- struct{}{}
}

func TestDropVirtualChannel(t *testing.T) {
	t.Run("normal DropVirtualChannel", func(t *testing.T) {
		spyCh := make(chan struct{}, 1)
		svr := newTestServer(t, WithSegmentManager(&spySegmentManager{spyCh: spyCh}))

		defer closeTestServer(t, svr)

		vecFieldID := int64(201)
		svr.meta.AddCollection(&collectionInfo{
			ID: 0,
			Schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{
						FieldID:  vecFieldID,
						DataType: schemapb.DataType_FloatVector,
					},
				},
			},
		})
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
			err := svr.meta.AddSegment(context.TODO(), NewSegmentInfo(s))
			assert.NoError(t, err)
		}
		// add non matched segments
		os := &datapb.SegmentInfo{
			ID:            maxOperationsPerTxn + 100,
			CollectionID:  0,
			InsertChannel: "ch2",

			State: commonpb.SegmentState_Growing,
		}

		svr.meta.AddSegment(context.TODO(), NewSegmentInfo(os))

		ctx := context.Background()
		chanName := "ch1"
		mockChManager := NewMockChannelManager(t)
		mockChManager.EXPECT().Match(mock.Anything, mock.Anything).Return(true).Twice()
		mockChManager.EXPECT().Release(mock.Anything, chanName).Return(nil).Twice()
		svr.channelManager = mockChManager

		req := &datapb.DropVirtualChannelRequest{
			Base: &commonpb.MsgBase{
				Timestamp: uint64(time.Now().Unix()),
			},
			ChannelName: chanName,
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
				CheckPoint: &msgpb.MsgPosition{
					ChannelName: "ch1",
					MsgID:       []byte{1, 2, 3},
					MsgGroup:    "",
					Timestamp:   0,
				},
				StartPosition: &msgpb.MsgPosition{
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

		// resend
		resp, err = svr.DropVirtualChannel(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("with channel not matched", func(t *testing.T) {
		svr := newTestServer(t)
		defer closeTestServer(t, svr)
		mockChManager := NewMockChannelManager(t)
		mockChManager.EXPECT().Match(mock.Anything, mock.Anything).Return(false).Once()
		svr.channelManager = mockChManager

		resp, err := svr.DropVirtualChannel(context.Background(), &datapb.DropVirtualChannelRequest{
			ChannelName: "ch2",
		})
		assert.NoError(t, err)
		assert.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrChannelNotFound)
	})

	t.Run("with closed server", func(t *testing.T) {
		svr := newTestServer(t)
		closeTestServer(t, svr)
		resp, err := svr.DropVirtualChannel(context.Background(), &datapb.DropVirtualChannelRequest{})
		assert.NoError(t, err)
		assert.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrServiceNotReady)
	})
}

func TestGetChannelSeekPosition(t *testing.T) {
	startPos1 := []*commonpb.KeyDataPair{
		{
			Key:  "ch1",
			Data: []byte{1, 2, 3},
		},
	}
	startPosNonExist := []*commonpb.KeyDataPair{
		{
			Key:  "ch2",
			Data: []byte{4, 5, 6},
		},
	}
	msgID := []byte{0, 0, 0, 0, 0, 0, 0, 0}

	tests := []struct {
		testName     string
		channelCP    *msgpb.MsgPosition
		segDMLPos    []*msgpb.MsgPosition
		collStartPos []*commonpb.KeyDataPair
		channelName  string
		expectedPos  *msgpb.MsgPosition
	}{
		{
			"test-with-channelCP",
			&msgpb.MsgPosition{ChannelName: "ch1", Timestamp: 100, MsgID: msgID},
			[]*msgpb.MsgPosition{{ChannelName: "ch1", Timestamp: 50, MsgID: msgID}, {ChannelName: "ch1", Timestamp: 200, MsgID: msgID}},
			startPos1,
			"ch1", &msgpb.MsgPosition{ChannelName: "ch1", Timestamp: 100, MsgID: msgID},
		},

		{
			"test-with-segmentDMLPos",
			nil,
			[]*msgpb.MsgPosition{{ChannelName: "ch1", Timestamp: 50, MsgID: msgID}, {ChannelName: "ch1", Timestamp: 200, MsgID: msgID}},
			startPos1,
			"ch1", &msgpb.MsgPosition{ChannelName: "ch1", Timestamp: 50, MsgID: msgID},
		},

		{
			"test-with-collStartPos",
			nil,
			nil,
			startPos1,
			"ch1", &msgpb.MsgPosition{ChannelName: "ch1", MsgID: startPos1[0].Data},
		},

		{
			"test-non-exist-channel-1",
			nil,
			nil,
			startPosNonExist,
			"ch1", nil,
		},

		{
			"test-non-exist-channel-2",
			nil,
			nil,
			nil,
			"ch1", nil,
		},
	}
	for _, test := range tests {
		t.Run(test.testName, func(t *testing.T) {
			svr := newTestServer(t)
			defer closeTestServer(t, svr)
			schema := newTestSchema()
			if test.collStartPos != nil {
				svr.meta.AddCollection(&collectionInfo{
					ID:             0,
					Schema:         schema,
					StartPositions: test.collStartPos,
				})
			}
			for i, segPos := range test.segDMLPos {
				seg := &datapb.SegmentInfo{
					ID:            UniqueID(i),
					CollectionID:  0,
					PartitionID:   0,
					DmlPosition:   segPos,
					InsertChannel: "ch1",
				}
				err := svr.meta.AddSegment(context.TODO(), NewSegmentInfo(seg))
				assert.NoError(t, err)
			}
			if test.channelCP != nil {
				err := svr.meta.UpdateChannelCheckpoint(test.channelCP.ChannelName, test.channelCP)
				assert.NoError(t, err)
			}

			seekPos := svr.handler.(*ServerHandler).GetChannelSeekPosition(&channelMeta{
				Name:         test.channelName,
				CollectionID: 0,
			}, allPartitionID)
			if test.expectedPos == nil {
				assert.True(t, seekPos == nil)
			} else {
				assert.Equal(t, test.expectedPos.ChannelName, seekPos.ChannelName)
				assert.Equal(t, test.expectedPos.Timestamp, seekPos.Timestamp)
				assert.ElementsMatch(t, test.expectedPos.MsgID, seekPos.MsgID)
			}
		})
	}
}

func TestGetDataVChanPositions(t *testing.T) {
	svr := newTestServer(t)
	defer closeTestServer(t, svr)
	schema := newTestSchema()
	svr.meta.AddCollection(&collectionInfo{
		ID:     0,
		Schema: schema,
		StartPositions: []*commonpb.KeyDataPair{
			{
				Key:  "ch1",
				Data: []byte{8, 9, 10},
			},
		},
	})
	svr.meta.AddCollection(&collectionInfo{
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
		DmlPosition: &msgpb.MsgPosition{
			ChannelName: "ch1",
			MsgID:       []byte{1, 2, 3},
		},
	}
	err := svr.meta.AddSegment(context.TODO(), NewSegmentInfo(s1))
	require.Nil(t, err)
	s2 := &datapb.SegmentInfo{
		ID:            2,
		CollectionID:  0,
		PartitionID:   0,
		InsertChannel: "ch1",
		State:         commonpb.SegmentState_Growing,
		StartPosition: &msgpb.MsgPosition{
			ChannelName: "ch1",
			MsgID:       []byte{8, 9, 10},
		},
		DmlPosition: &msgpb.MsgPosition{
			ChannelName: "ch1",
			MsgID:       []byte{1, 2, 3},
			Timestamp:   1,
		},
	}
	err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(s2))
	require.Nil(t, err)
	s3 := &datapb.SegmentInfo{
		ID:            3,
		CollectionID:  0,
		PartitionID:   1,
		InsertChannel: "ch1",
		State:         commonpb.SegmentState_Growing,
		StartPosition: &msgpb.MsgPosition{
			ChannelName: "ch1",
			MsgID:       []byte{8, 9, 10},
		},
		DmlPosition: &msgpb.MsgPosition{
			ChannelName: "ch1",
			MsgID:       []byte{11, 12, 13},
			Timestamp:   2,
		},
	}
	err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(s3))
	require.Nil(t, err)

	t.Run("get unexisted channel", func(t *testing.T) {
		vchan := svr.handler.GetDataVChanPositions(&channelMeta{Name: "chx1", CollectionID: 0}, allPartitionID)
		assert.Empty(t, vchan.UnflushedSegmentIds)
		assert.Empty(t, vchan.FlushedSegmentIds)
	})

	t.Run("get existed channel", func(t *testing.T) {
		vchan := svr.handler.GetDataVChanPositions(&channelMeta{Name: "ch1", CollectionID: 0}, allPartitionID)
		assert.EqualValues(t, 1, len(vchan.FlushedSegmentIds))
		assert.EqualValues(t, 1, vchan.FlushedSegmentIds[0])
		assert.EqualValues(t, 2, len(vchan.UnflushedSegmentIds))
		assert.ElementsMatch(t, []int64{s2.ID, s3.ID}, vchan.UnflushedSegmentIds)
	})

	t.Run("empty collection", func(t *testing.T) {
		infos := svr.handler.GetDataVChanPositions(&channelMeta{Name: "ch0_suffix", CollectionID: 1}, allPartitionID)
		assert.EqualValues(t, 1, infos.CollectionID)
		assert.EqualValues(t, 0, len(infos.FlushedSegmentIds))
		assert.EqualValues(t, 0, len(infos.UnflushedSegmentIds))
	})

	t.Run("filter partition", func(t *testing.T) {
		infos := svr.handler.GetDataVChanPositions(&channelMeta{Name: "ch1", CollectionID: 0}, 1)
		assert.EqualValues(t, 0, infos.CollectionID)
		assert.EqualValues(t, 0, len(infos.FlushedSegmentIds))
		assert.EqualValues(t, 1, len(infos.UnflushedSegmentIds))
	})

	t.Run("empty collection with passed positions", func(t *testing.T) {
		vchannel := "ch_no_segment_1"
		pchannel := funcutil.ToPhysicalChannel(vchannel)
		infos := svr.handler.GetDataVChanPositions(&channelMeta{
			Name:           vchannel,
			CollectionID:   0,
			StartPositions: []*commonpb.KeyDataPair{{Key: pchannel, Data: []byte{14, 15, 16}}},
		}, allPartitionID)
		assert.EqualValues(t, 0, infos.CollectionID)
		assert.EqualValues(t, vchannel, infos.ChannelName)
	})
}

func TestGetQueryVChanPositions(t *testing.T) {
	svr := newTestServer(t)
	defer closeTestServer(t, svr)
	schema := newTestSchema()
	svr.meta.AddCollection(&collectionInfo{
		ID:     0,
		Schema: schema,
		StartPositions: []*commonpb.KeyDataPair{
			{
				Key:  "ch1",
				Data: []byte{8, 9, 10},
			},
		},
	})
	svr.meta.AddCollection(&collectionInfo{
		ID:     1,
		Schema: schema,
		StartPositions: []*commonpb.KeyDataPair{
			{
				Key:  "ch0",
				Data: []byte{8, 9, 10},
			},
		},
	})

	err := svr.meta.indexMeta.CreateIndex(&model.Index{
		TenantID:     "",
		CollectionID: 0,
		FieldID:      2,
		IndexID:      1,
	})
	assert.NoError(t, err)

	s1 := &datapb.SegmentInfo{
		ID:            1,
		CollectionID:  0,
		PartitionID:   0,
		InsertChannel: "ch1",
		State:         commonpb.SegmentState_Flushed,
		DmlPosition: &msgpb.MsgPosition{
			ChannelName: "ch1",
			MsgID:       []byte{1, 2, 3},
			MsgGroup:    "",
			Timestamp:   0,
		},
		NumOfRows: 2048,
	}
	err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(s1))
	assert.NoError(t, err)
	err = svr.meta.indexMeta.AddSegmentIndex(&model.SegmentIndex{
		SegmentID: 1,
		BuildID:   1,
		IndexID:   1,
	})
	assert.NoError(t, err)
	err = svr.meta.indexMeta.FinishTask(&workerpb.IndexTaskInfo{
		BuildID: 1,
		State:   commonpb.IndexState_Finished,
	})
	assert.NoError(t, err)
	s2 := &datapb.SegmentInfo{
		ID:            2,
		CollectionID:  0,
		PartitionID:   0,
		InsertChannel: "ch1",
		State:         commonpb.SegmentState_Growing,
		StartPosition: &msgpb.MsgPosition{
			ChannelName: "ch1",
			MsgID:       []byte{8, 9, 10},
			MsgGroup:    "",
		},
		DmlPosition: &msgpb.MsgPosition{
			ChannelName: "ch1",
			MsgID:       []byte{1, 2, 3},
			MsgGroup:    "",
			Timestamp:   1,
		},
	}
	err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(s2))
	assert.NoError(t, err)
	s3 := &datapb.SegmentInfo{
		ID:            3,
		CollectionID:  0,
		PartitionID:   1,
		InsertChannel: "ch1",
		State:         commonpb.SegmentState_Growing,
		StartPosition: &msgpb.MsgPosition{
			ChannelName: "ch1",
			MsgID:       []byte{8, 9, 10},
			MsgGroup:    "",
		},
		DmlPosition: &msgpb.MsgPosition{
			ChannelName: "ch1",
			MsgID:       []byte{11, 12, 13},
			MsgGroup:    "",
			Timestamp:   2,
		},
	}
	err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(s3))
	assert.NoError(t, err)

	s4 := &datapb.SegmentInfo{
		ID:            4,
		CollectionID:  0,
		PartitionID:   common.AllPartitionsID,
		InsertChannel: "ch1",
		State:         commonpb.SegmentState_Flushed,
		StartPosition: &msgpb.MsgPosition{
			ChannelName: "ch1",
			MsgID:       []byte{8, 9, 10},
			MsgGroup:    "",
		},
		DmlPosition: &msgpb.MsgPosition{
			ChannelName: "ch1",
			MsgID:       []byte{11, 12, 13},
			MsgGroup:    "",
			Timestamp:   2,
		},
		Level: datapb.SegmentLevel_L0,
	}
	err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(s4))
	assert.NoError(t, err)
	//mockResp := &indexpb.GetIndexInfoResponse{
	//	Status: &commonpb.Status{},
	//	SegmentInfo: map[int64]*indexpb.SegmentInfo{
	//		s1.ID: {
	//			CollectionID: s1.CollectionID,
	//			SegmentID:    s1.ID,
	//			EnableIndex:  true,
	//			IndexInfos: []*indexpb.IndexFilePathInfo{
	//				{
	//					SegmentID: s1.ID,
	//					FieldID:   2,
	//				},
	//			},
	//		},
	//	},
	//}

	t.Run("get unexisted channel", func(t *testing.T) {
		vchan := svr.handler.GetQueryVChanPositions(&channelMeta{Name: "chx1", CollectionID: 0})
		assert.Empty(t, vchan.UnflushedSegmentIds)
		assert.Empty(t, vchan.FlushedSegmentIds)
	})

	// t.Run("get existed channel", func(t *testing.T) {
	// vchan := svr.handler.GetQueryVChanPositions(&channelMeta{Name: "ch1", CollectionID: 0})
	// assert.EqualValues(t, 1, len(vchan.FlushedSegmentIds))
	// assert.ElementsMatch(t, []int64{1}, vchan.FlushedSegmentIds)
	// assert.EqualValues(t, 2, len(vchan.UnflushedSegmentIds))
	// assert.EqualValues(t, 1, len(vchan.GetLevelZeroSegmentIds()))
	// })

	t.Run("empty collection", func(t *testing.T) {
		infos := svr.handler.GetQueryVChanPositions(&channelMeta{Name: "ch0_suffix", CollectionID: 1})
		assert.EqualValues(t, 1, infos.CollectionID)
		assert.EqualValues(t, 0, len(infos.FlushedSegmentIds))
		assert.EqualValues(t, 0, len(infos.UnflushedSegmentIds))
		assert.EqualValues(t, 0, len(infos.GetLevelZeroSegmentIds()))
	})

	t.Run("filter partition", func(t *testing.T) {
		infos := svr.handler.GetQueryVChanPositions(&channelMeta{Name: "ch1", CollectionID: 0}, 1)
		assert.EqualValues(t, 0, infos.CollectionID)
		// assert.EqualValues(t, 0, len(infos.FlushedSegmentIds))
		// assert.EqualValues(t, 1, len(infos.UnflushedSegmentIds))
		assert.EqualValues(t, 1, len(infos.GetLevelZeroSegmentIds()))
	})

	t.Run("empty collection with passed positions", func(t *testing.T) {
		vchannel := "ch_no_segment_1"
		pchannel := funcutil.ToPhysicalChannel(vchannel)
		infos := svr.handler.GetQueryVChanPositions(&channelMeta{
			Name:           vchannel,
			CollectionID:   0,
			StartPositions: []*commonpb.KeyDataPair{{Key: pchannel, Data: []byte{14, 15, 16}}},
		})
		assert.EqualValues(t, 0, infos.CollectionID)
		assert.EqualValues(t, vchannel, infos.ChannelName)
		assert.EqualValues(t, 0, len(infos.GetLevelZeroSegmentIds()))
	})
}

func TestGetQueryVChanPositions_PartitionStats(t *testing.T) {
	svr := newTestServer(t)
	defer closeTestServer(t, svr)
	schema := newTestSchema()
	collectionID := int64(0)
	partitionID := int64(1)
	vchannel := "test_vchannel"
	version := int64(100)
	svr.meta.AddCollection(&collectionInfo{
		ID:     collectionID,
		Schema: schema,
	})
	svr.meta.partitionStatsMeta.partitionStatsInfos = map[string]map[int64]*partitionStatsInfo{
		vchannel: {
			partitionID: {
				currentVersion: version,
				infos: map[int64]*datapb.PartitionStatsInfo{
					version: {Version: version},
				},
			},
		},
	}
	partitionIDs := make([]UniqueID, 0)
	partitionIDs = append(partitionIDs, partitionID)
	vChannelInfo := svr.handler.GetQueryVChanPositions(&channelMeta{Name: vchannel, CollectionID: collectionID}, partitionIDs...)
	statsVersions := vChannelInfo.GetPartitionStatsVersions()
	assert.Equal(t, 1, len(statsVersions))
	assert.Equal(t, int64(100), statsVersions[partitionID])
}

func TestGetQueryVChanPositions_Retrieve_unIndexed(t *testing.T) {
	t.Run("ab GC-ed, cde unIndexed", func(t *testing.T) {
		svr := newTestServer(t)
		defer closeTestServer(t, svr)
		schema := newTestSchema()
		svr.meta.AddCollection(&collectionInfo{
			ID:     0,
			Schema: schema,
		})
		err := svr.meta.indexMeta.CreateIndex(&model.Index{
			TenantID:     "",
			CollectionID: 0,
			FieldID:      2,
			IndexID:      1,
		})
		assert.NoError(t, err)
		c := &datapb.SegmentInfo{
			ID:            1,
			CollectionID:  0,
			PartitionID:   0,
			InsertChannel: "ch1",
			State:         commonpb.SegmentState_Dropped,
			DmlPosition: &msgpb.MsgPosition{
				ChannelName: "ch1",
				MsgID:       []byte{1, 2, 3},
				MsgGroup:    "",
				Timestamp:   1,
			},
			CompactionFrom: []int64{99, 100}, // a, b which have been GC-ed
		}
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(c))
		assert.NoError(t, err)
		d := &datapb.SegmentInfo{
			ID:            2,
			CollectionID:  0,
			PartitionID:   0,
			InsertChannel: "ch1",
			State:         commonpb.SegmentState_Dropped,
			DmlPosition: &msgpb.MsgPosition{
				ChannelName: "ch1",
				MsgID:       []byte{1, 2, 3},
				MsgGroup:    "",
				Timestamp:   1,
			},
		}
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(d))
		assert.NoError(t, err)
		e := &datapb.SegmentInfo{
			ID:            3,
			CollectionID:  0,
			PartitionID:   1,
			InsertChannel: "ch1",
			State:         commonpb.SegmentState_Flushed,
			DmlPosition: &msgpb.MsgPosition{
				ChannelName: "ch1",
				MsgID:       []byte{1, 2, 3},
				MsgGroup:    "",
				Timestamp:   1,
			},
			CompactionFrom: []int64{1, 2}, // c, d
			NumOfRows:      2048,
		}

		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(e))
		assert.NoError(t, err)
		// vchan := svr.handler.GetQueryVChanPositions(&channelMeta{Name: "ch1", CollectionID: 0})
		// assert.EqualValues(t, 2, len(vchan.FlushedSegmentIds))
		// assert.EqualValues(t, 0, len(vchan.UnflushedSegmentIds))
		// assert.ElementsMatch(t, []int64{c.GetID(), d.GetID()}, vchan.FlushedSegmentIds) // expected c, d
	})

	t.Run("a GC-ed, bcde unIndexed", func(t *testing.T) {
		svr := newTestServer(t)
		defer closeTestServer(t, svr)
		schema := newTestSchema()
		svr.meta.AddCollection(&collectionInfo{
			ID:     0,
			Schema: schema,
		})
		err := svr.meta.indexMeta.CreateIndex(&model.Index{
			TenantID:     "",
			CollectionID: 0,
			FieldID:      2,
			IndexID:      1,
		})
		assert.NoError(t, err)
		a := &datapb.SegmentInfo{
			ID:            99,
			CollectionID:  0,
			PartitionID:   0,
			InsertChannel: "ch1",
			State:         commonpb.SegmentState_Dropped,
			DmlPosition: &msgpb.MsgPosition{
				ChannelName: "ch1",
				MsgID:       []byte{1, 2, 3},
				MsgGroup:    "",
				Timestamp:   1,
			},
		}
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(a))
		assert.NoError(t, err)

		c := &datapb.SegmentInfo{
			ID:            1,
			CollectionID:  0,
			PartitionID:   0,
			InsertChannel: "ch1",
			State:         commonpb.SegmentState_Dropped,
			DmlPosition: &msgpb.MsgPosition{
				ChannelName: "ch1",
				MsgID:       []byte{1, 2, 3},
				MsgGroup:    "",
				Timestamp:   1,
			},
			CompactionFrom: []int64{99, 100}, // a, b which have been GC-ed
		}
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(c))
		assert.NoError(t, err)
		d := &datapb.SegmentInfo{
			ID:            2,
			CollectionID:  0,
			PartitionID:   0,
			InsertChannel: "ch1",
			State:         commonpb.SegmentState_Dropped,
			DmlPosition: &msgpb.MsgPosition{
				ChannelName: "ch1",
				MsgID:       []byte{1, 2, 3},
				MsgGroup:    "",
				Timestamp:   1,
			},
		}
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(d))
		assert.NoError(t, err)
		e := &datapb.SegmentInfo{
			ID:            3,
			CollectionID:  0,
			PartitionID:   1,
			InsertChannel: "ch1",
			State:         commonpb.SegmentState_Flushed,
			DmlPosition: &msgpb.MsgPosition{
				ChannelName: "ch1",
				MsgID:       []byte{1, 2, 3},
				MsgGroup:    "",
				Timestamp:   1,
			},
			CompactionFrom: []int64{1, 2}, // c, d
			NumOfRows:      2048,
		}

		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(e))
		assert.NoError(t, err)
		// vchan := svr.handler.GetQueryVChanPositions(&channelMeta{Name: "ch1", CollectionID: 0})
		// assert.EqualValues(t, 2, len(vchan.FlushedSegmentIds))
		// assert.EqualValues(t, 0, len(vchan.UnflushedSegmentIds))
		// assert.ElementsMatch(t, []int64{c.GetID(), d.GetID()}, vchan.FlushedSegmentIds) // expected c, d
	})

	t.Run("ab GC-ed, c unIndexed, de indexed", func(t *testing.T) {
		svr := newTestServer(t)
		defer closeTestServer(t, svr)
		schema := newTestSchema()
		svr.meta.AddCollection(&collectionInfo{
			ID:     0,
			Schema: schema,
		})
		err := svr.meta.indexMeta.CreateIndex(&model.Index{
			TenantID:     "",
			CollectionID: 0,
			FieldID:      2,
			IndexID:      1,
		})
		assert.NoError(t, err)
		c := &datapb.SegmentInfo{
			ID:            1,
			CollectionID:  0,
			PartitionID:   0,
			InsertChannel: "ch1",
			State:         commonpb.SegmentState_Dropped,
			DmlPosition: &msgpb.MsgPosition{
				ChannelName: "ch1",
				MsgID:       []byte{1, 2, 3},
				MsgGroup:    "",
				Timestamp:   1,
			},
			CompactionFrom: []int64{99, 100}, // a, b which have been GC-ed
		}
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(c))
		assert.NoError(t, err)
		d := &datapb.SegmentInfo{
			ID:            2,
			CollectionID:  0,
			PartitionID:   0,
			InsertChannel: "ch1",
			State:         commonpb.SegmentState_Dropped,
			DmlPosition: &msgpb.MsgPosition{
				ChannelName: "ch1",
				MsgID:       []byte{1, 2, 3},
				MsgGroup:    "",
				Timestamp:   1,
			},
		}
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(d))
		assert.NoError(t, err)
		err = svr.meta.indexMeta.AddSegmentIndex(&model.SegmentIndex{
			SegmentID: 2,
			BuildID:   1,
			IndexID:   1,
		})
		assert.NoError(t, err)
		err = svr.meta.indexMeta.FinishTask(&workerpb.IndexTaskInfo{
			BuildID: 1,
			State:   commonpb.IndexState_Finished,
		})
		assert.NoError(t, err)
		e := &datapb.SegmentInfo{
			ID:            3,
			CollectionID:  0,
			PartitionID:   0,
			InsertChannel: "ch1",
			State:         commonpb.SegmentState_Flushed,
			DmlPosition: &msgpb.MsgPosition{
				ChannelName: "ch1",
				MsgID:       []byte{1, 2, 3},
				MsgGroup:    "",
				Timestamp:   1,
			},
			CompactionFrom: []int64{1, 2}, // c, d
			NumOfRows:      2048,
		}
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(e))
		assert.NoError(t, err)
		err = svr.meta.indexMeta.AddSegmentIndex(&model.SegmentIndex{
			SegmentID: 3,
			BuildID:   2,
			IndexID:   1,
		})
		assert.NoError(t, err)
		err = svr.meta.indexMeta.FinishTask(&workerpb.IndexTaskInfo{
			BuildID: 2,
			State:   commonpb.IndexState_Finished,
		})
		assert.NoError(t, err)

		// vchan := svr.handler.GetQueryVChanPositions(&channelMeta{Name: "ch1", CollectionID: 0})
		// assert.EqualValues(t, 1, len(vchan.FlushedSegmentIds))
		// assert.EqualValues(t, 0, len(vchan.UnflushedSegmentIds))
		// assert.ElementsMatch(t, []int64{e.GetID()}, vchan.FlushedSegmentIds) // expected e
	})
}

func TestShouldDropChannel(t *testing.T) {
	type myRootCoord struct {
		mocks.MockRootCoordClient
	}
	myRoot := &myRootCoord{}
	myRoot.EXPECT().AllocTimestamp(mock.Anything, mock.Anything).Return(&rootcoordpb.AllocTimestampResponse{
		Status:    merr.Success(),
		Timestamp: tsoutil.ComposeTSByTime(time.Now(), 0),
		Count:     1,
	}, nil)

	myRoot.EXPECT().AllocID(mock.Anything, mock.Anything).Return(&rootcoordpb.AllocIDResponse{
		Status: merr.Success(),
		ID:     int64(tsoutil.ComposeTSByTime(time.Now(), 0)),
		Count:  1,
	}, nil)

	svr := newTestServer(t)
	defer closeTestServer(t, svr)
	schema := newTestSchema()
	svr.meta.AddCollection(&collectionInfo{
		ID:     0,
		Schema: schema,
		StartPositions: []*commonpb.KeyDataPair{
			{
				Key:  "ch1",
				Data: []byte{8, 9, 10},
			},
		},
	})
	svr.meta.AddCollection(&collectionInfo{
		ID:     1,
		Schema: schema,
		StartPositions: []*commonpb.KeyDataPair{
			{
				Key:  "ch0",
				Data: []byte{8, 9, 10},
			},
		},
	})

	t.Run("channel name not in kv ", func(t *testing.T) {
		assert.False(t, svr.handler.CheckShouldDropChannel("ch99"))
	})

	t.Run("channel in remove flag", func(t *testing.T) {
		err := svr.meta.catalog.MarkChannelDeleted(context.TODO(), "ch1")
		require.NoError(t, err)
		assert.True(t, svr.handler.CheckShouldDropChannel("ch1"))
	})
}

func TestGetRecoveryInfo(t *testing.T) {
	t.Run("test get recovery info with no segments", func(t *testing.T) {
		svr := newTestServer(t)
		defer closeTestServer(t, svr)

		svr.rootCoordClientCreator = func(ctx context.Context) (types.RootCoordClient, error) {
			return newMockRootCoordClient(), nil
		}

		mockHandler := NewNMockHandler(t)
		mockHandler.EXPECT().GetQueryVChanPositions(mock.Anything, mock.Anything).Return(&datapb.VchannelInfo{})
		svr.handler = mockHandler

		req := &datapb.GetRecoveryInfoRequest{
			CollectionID: 0,
			PartitionID:  0,
		}
		resp, err := svr.GetRecoveryInfo(context.TODO(), req)
		assert.NoError(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		assert.EqualValues(t, 0, len(resp.GetBinlogs()))
		assert.EqualValues(t, 1, len(resp.GetChannels()))
		assert.Nil(t, resp.GetChannels()[0].SeekPosition)
	})

	createSegment := func(id, collectionID, partitionID, numOfRows int64, posTs uint64,
		channel string, state commonpb.SegmentState,
	) *datapb.SegmentInfo {
		return &datapb.SegmentInfo{
			ID:            id,
			CollectionID:  collectionID,
			PartitionID:   partitionID,
			InsertChannel: channel,
			NumOfRows:     numOfRows,
			State:         state,
			DmlPosition: &msgpb.MsgPosition{
				ChannelName: channel,
				MsgID:       []byte{},
				Timestamp:   posTs,
			},
			StartPosition: &msgpb.MsgPosition{
				ChannelName: "",
				MsgID:       []byte{},
				MsgGroup:    "",
				Timestamp:   0,
			},
		}
	}

	t.Run("test get earliest position of flushed segments as seek position", func(t *testing.T) {
		svr := newTestServer(t)
		defer closeTestServer(t, svr)

		svr.rootCoordClientCreator = func(ctx context.Context) (types.RootCoordClient, error) {
			return newMockRootCoordClient(), nil
		}

		svr.meta.AddCollection(&collectionInfo{
			Schema: newTestSchema(),
		})

		err := svr.meta.UpdateChannelCheckpoint("vchan1", &msgpb.MsgPosition{
			ChannelName: "vchan1",
			Timestamp:   10,
			MsgID:       []byte{0, 0, 0, 0, 0, 0, 0, 0},
		})
		assert.NoError(t, err)

		err = svr.meta.indexMeta.CreateIndex(&model.Index{
			TenantID:     "",
			CollectionID: 0,
			FieldID:      2,
			IndexID:      0,
			IndexName:    "",
		})
		assert.NoError(t, err)

		seg1 := createSegment(0, 0, 0, 100, 10, "vchan1", commonpb.SegmentState_Flushed)
		seg1.Binlogs = []*datapb.FieldBinlog{
			{
				FieldID: 1,
				Binlogs: []*datapb.Binlog{
					{
						EntriesNum: 20,
						LogID:      901,
					},
					{
						EntriesNum: 20,
						LogID:      902,
					},
					{
						EntriesNum: 20,
						LogID:      903,
					},
				},
			},
		}
		seg2 := createSegment(1, 0, 0, 100, 20, "vchan1", commonpb.SegmentState_Flushed)
		seg2.Binlogs = []*datapb.FieldBinlog{
			{
				FieldID: 1,
				Binlogs: []*datapb.Binlog{
					{
						EntriesNum: 30,
						LogID:      801,
					},
					{
						EntriesNum: 70,
						LogID:      802,
					},
				},
			},
		}
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(seg1))
		assert.NoError(t, err)
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(seg2))
		assert.NoError(t, err)
		err = svr.meta.indexMeta.AddSegmentIndex(&model.SegmentIndex{
			SegmentID: seg1.ID,
			BuildID:   seg1.ID,
		})
		assert.NoError(t, err)
		err = svr.meta.indexMeta.FinishTask(&workerpb.IndexTaskInfo{
			BuildID: seg1.ID,
			State:   commonpb.IndexState_Finished,
		})
		assert.NoError(t, err)
		err = svr.meta.indexMeta.AddSegmentIndex(&model.SegmentIndex{
			SegmentID: seg2.ID,
			BuildID:   seg2.ID,
		})
		assert.NoError(t, err)
		err = svr.meta.indexMeta.FinishTask(&workerpb.IndexTaskInfo{
			BuildID: seg2.ID,
			State:   commonpb.IndexState_Finished,
		})
		assert.NoError(t, err)

		mockHandler := NewNMockHandler(t)
		mockHandler.EXPECT().GetQueryVChanPositions(mock.Anything, mock.Anything).Return(&datapb.VchannelInfo{})
		svr.handler = mockHandler

		req := &datapb.GetRecoveryInfoRequest{
			CollectionID: 0,
			PartitionID:  0,
		}
		resp, err := svr.GetRecoveryInfo(context.TODO(), req)
		assert.NoError(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		assert.EqualValues(t, 1, len(resp.GetChannels()))
		assert.EqualValues(t, 0, len(resp.GetChannels()[0].GetUnflushedSegmentIds()))
		// assert.ElementsMatch(t, []int64{0, 1}, resp.GetChannels()[0].GetFlushedSegmentIds())
		// assert.EqualValues(t, 10, resp.GetChannels()[0].GetSeekPosition().GetTimestamp())
		// assert.EqualValues(t, 2, len(resp.GetBinlogs()))
		// Row count corrected from 100 + 100 -> 100 + 60.
		// assert.EqualValues(t, 160, resp.GetBinlogs()[0].GetNumOfRows()+resp.GetBinlogs()[1].GetNumOfRows())
	})

	t.Run("test get recovery of unflushed segments ", func(t *testing.T) {
		svr := newTestServer(t)
		defer closeTestServer(t, svr)

		svr.rootCoordClientCreator = func(ctx context.Context) (types.RootCoordClient, error) {
			return newMockRootCoordClient(), nil
		}

		svr.meta.AddCollection(&collectionInfo{
			ID:     0,
			Schema: newTestSchema(),
		})

		err := svr.meta.UpdateChannelCheckpoint("vchan1", &msgpb.MsgPosition{
			ChannelName: "vchan1",
			Timestamp:   0,
			MsgID:       []byte{0, 0, 0, 0, 0, 0, 0, 0},
		})
		assert.NoError(t, err)

		seg1 := createSegment(3, 0, 0, 100, 30, "vchan1", commonpb.SegmentState_Growing)
		seg1.Binlogs = []*datapb.FieldBinlog{
			{
				FieldID: 1,
				Binlogs: []*datapb.Binlog{
					{
						EntriesNum: 20,
						LogID:      901,
					},
					{
						EntriesNum: 20,
						LogID:      902,
					},
					{
						EntriesNum: 20,
						LogID:      903,
					},
				},
			},
		}
		seg2 := createSegment(4, 0, 0, 100, 40, "vchan1", commonpb.SegmentState_Growing)
		seg2.Binlogs = []*datapb.FieldBinlog{
			{
				FieldID: 1,
				Binlogs: []*datapb.Binlog{
					{
						EntriesNum: 30,
						LogID:      801,
					},
					{
						EntriesNum: 70,
						LogID:      802,
					},
				},
			},
		}
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(seg1))
		assert.NoError(t, err)
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(seg2))
		assert.NoError(t, err)
		// svr.indexCoord.(*mocks.MockIndexCoord).EXPECT().GetIndexInfos(mock.Anything, mock.Anything).Return(nil, nil)

		req := &datapb.GetRecoveryInfoRequest{
			CollectionID: 0,
			PartitionID:  0,
		}
		resp, err := svr.GetRecoveryInfo(context.TODO(), req)
		assert.NoError(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		assert.EqualValues(t, 0, len(resp.GetBinlogs()))
		assert.EqualValues(t, 1, len(resp.GetChannels()))
		assert.NotNil(t, resp.GetChannels()[0].SeekPosition)
		assert.NotEqual(t, 0, resp.GetChannels()[0].GetSeekPosition().GetTimestamp())
	})

	t.Run("test get binlogs", func(t *testing.T) {
		svr := newTestServer(t)
		defer closeTestServer(t, svr)

		svr.meta.AddCollection(&collectionInfo{
			Schema: newTestSchema(),
		})

		svr.rootCoordClientCreator = func(ctx context.Context) (types.RootCoordClient, error) {
			return newMockRootCoordClient(), nil
		}

		binlogReq := &datapb.SaveBinlogPathsRequest{
			SegmentID:    0,
			CollectionID: 0,
			Field2BinlogPaths: []*datapb.FieldBinlog{
				{
					FieldID: 1,
					Binlogs: []*datapb.Binlog{
						{
							LogPath: "/binlog/1",
						},
						{
							LogPath: "/binlog/2",
						},
					},
				},
			},
			Field2StatslogPaths: []*datapb.FieldBinlog{
				{
					FieldID: 1,
					Binlogs: []*datapb.Binlog{
						{
							LogPath: "/stats_log/1",
						},
						{
							LogPath: "/stats_log/2",
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
							LogPath:       "/stats_log/1",
							LogSize:       1,
						},
					},
				},
			},
		}
		segment := createSegment(0, 0, 1, 100, 10, "vchan1", commonpb.SegmentState_Flushed)
		err := svr.meta.AddSegment(context.TODO(), NewSegmentInfo(segment))
		assert.NoError(t, err)

		err = svr.meta.indexMeta.CreateIndex(&model.Index{
			TenantID:     "",
			CollectionID: 0,
			FieldID:      2,
			IndexID:      0,
			IndexName:    "",
		})
		assert.NoError(t, err)
		err = svr.meta.indexMeta.AddSegmentIndex(&model.SegmentIndex{
			SegmentID: segment.ID,
			BuildID:   segment.ID,
		})
		assert.NoError(t, err)
		err = svr.meta.indexMeta.FinishTask(&workerpb.IndexTaskInfo{
			BuildID: segment.ID,
			State:   commonpb.IndexState_Finished,
		})
		assert.NoError(t, err)

		sResp, err := svr.SaveBinlogPaths(context.TODO(), binlogReq)
		assert.NoError(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, sResp.ErrorCode)

		req := &datapb.GetRecoveryInfoRequest{
			CollectionID: 0,
			PartitionID:  1,
		}
		resp, err := svr.GetRecoveryInfo(context.TODO(), req)
		assert.NoError(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		assert.EqualValues(t, 1, len(resp.GetBinlogs()))
		assert.EqualValues(t, 0, resp.GetBinlogs()[0].GetSegmentID())
		assert.EqualValues(t, 1, len(resp.GetBinlogs()[0].GetFieldBinlogs()))
		assert.EqualValues(t, 1, resp.GetBinlogs()[0].GetFieldBinlogs()[0].GetFieldID())
		for _, binlog := range resp.GetBinlogs()[0].GetFieldBinlogs()[0].GetBinlogs() {
			assert.Equal(t, "", binlog.GetLogPath())
		}
		for i, binlog := range resp.GetBinlogs()[0].GetFieldBinlogs()[0].GetBinlogs() {
			assert.Equal(t, int64(i+1), binlog.GetLogID())
		}
	})
	t.Run("with dropped segments", func(t *testing.T) {
		svr := newTestServer(t)
		defer closeTestServer(t, svr)

		svr.rootCoordClientCreator = func(ctx context.Context) (types.RootCoordClient, error) {
			return newMockRootCoordClient(), nil
		}

		svr.meta.AddCollection(&collectionInfo{
			ID:     0,
			Schema: newTestSchema(),
		})

		err := svr.meta.UpdateChannelCheckpoint("vchan1", &msgpb.MsgPosition{
			ChannelName: "vchan1",
			Timestamp:   0,
			MsgID:       []byte{0, 0, 0, 0, 0, 0, 0, 0},
		})
		assert.NoError(t, err)

		seg1 := createSegment(7, 0, 0, 100, 30, "vchan1", commonpb.SegmentState_Growing)
		seg2 := createSegment(8, 0, 0, 100, 40, "vchan1", commonpb.SegmentState_Dropped)
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(seg1))
		assert.NoError(t, err)
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(seg2))
		assert.NoError(t, err)

		mockHandler := NewNMockHandler(t)
		mockHandler.EXPECT().GetQueryVChanPositions(mock.Anything, mock.Anything).Return(&datapb.VchannelInfo{})
		svr.handler = mockHandler

		req := &datapb.GetRecoveryInfoRequest{
			CollectionID: 0,
			PartitionID:  0,
		}
		resp, err := svr.GetRecoveryInfo(context.TODO(), req)
		assert.NoError(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		assert.EqualValues(t, 0, len(resp.GetBinlogs()))
		assert.EqualValues(t, 1, len(resp.GetChannels()))
		// assert.NotNil(t, resp.GetChannels()[0].SeekPosition)
		assert.NotEqual(t, 0, resp.GetChannels()[0].GetSeekPosition().GetTimestamp())
		// assert.Len(t, resp.GetChannels()[0].GetDroppedSegmentIds(), 1)
		// assert.Equal(t, UniqueID(8), resp.GetChannels()[0].GetDroppedSegmentIds()[0])
	})

	t.Run("with fake segments", func(t *testing.T) {
		svr := newTestServer(t)
		defer closeTestServer(t, svr)

		svr.rootCoordClientCreator = func(ctx context.Context) (types.RootCoordClient, error) {
			return newMockRootCoordClient(), nil
		}

		svr.meta.AddCollection(&collectionInfo{
			ID:     0,
			Schema: newTestSchema(),
		})

		err := svr.meta.UpdateChannelCheckpoint("vchan1", &msgpb.MsgPosition{
			ChannelName: "vchan1",
			Timestamp:   0,
			MsgID:       []byte{0, 0, 0, 0, 0, 0, 0, 0},
		})
		require.NoError(t, err)

		seg1 := createSegment(7, 0, 0, 100, 30, "vchan1", commonpb.SegmentState_Growing)
		seg2 := createSegment(8, 0, 0, 100, 40, "vchan1", commonpb.SegmentState_Flushed)
		seg2.IsFake = true
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(seg1))
		assert.NoError(t, err)
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(seg2))
		assert.NoError(t, err)

		req := &datapb.GetRecoveryInfoRequest{
			CollectionID: 0,
			PartitionID:  0,
		}
		resp, err := svr.GetRecoveryInfo(context.TODO(), req)
		assert.NoError(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		assert.EqualValues(t, 0, len(resp.GetBinlogs()))
		assert.EqualValues(t, 1, len(resp.GetChannels()))
		assert.NotNil(t, resp.GetChannels()[0].SeekPosition)
		assert.NotEqual(t, 0, resp.GetChannels()[0].GetSeekPosition().GetTimestamp())
	})

	t.Run("with continuous compaction", func(t *testing.T) {
		svr := newTestServer(t)
		defer closeTestServer(t, svr)

		svr.rootCoordClientCreator = func(ctx context.Context) (types.RootCoordClient, error) {
			return newMockRootCoordClient(), nil
		}

		svr.meta.AddCollection(&collectionInfo{
			ID:     0,
			Schema: newTestSchema(),
		})

		err := svr.meta.UpdateChannelCheckpoint("vchan1", &msgpb.MsgPosition{
			ChannelName: "vchan1",
			Timestamp:   0,
			MsgID:       []byte{0, 0, 0, 0, 0, 0, 0, 0},
		})
		assert.NoError(t, err)

		seg1 := createSegment(9, 0, 0, 2048, 30, "vchan1", commonpb.SegmentState_Dropped)
		seg2 := createSegment(10, 0, 0, 2048, 40, "vchan1", commonpb.SegmentState_Dropped)
		seg3 := createSegment(11, 0, 0, 2048, 40, "vchan1", commonpb.SegmentState_Dropped)
		seg3.CompactionFrom = []int64{9, 10}
		seg4 := createSegment(12, 0, 0, 2048, 40, "vchan1", commonpb.SegmentState_Dropped)
		seg5 := createSegment(13, 0, 0, 2048, 40, "vchan1", commonpb.SegmentState_Flushed)
		seg5.CompactionFrom = []int64{11, 12}
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(seg1))
		assert.NoError(t, err)
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(seg2))
		assert.NoError(t, err)
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(seg3))
		assert.NoError(t, err)
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(seg4))
		assert.NoError(t, err)
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(seg5))
		assert.NoError(t, err)
		err = svr.meta.indexMeta.CreateIndex(&model.Index{
			TenantID:        "",
			CollectionID:    0,
			FieldID:         2,
			IndexID:         0,
			IndexName:       "_default_idx_2",
			IsDeleted:       false,
			CreateTime:      0,
			TypeParams:      nil,
			IndexParams:     nil,
			IsAutoIndex:     false,
			UserIndexParams: nil,
		})
		assert.NoError(t, err)
		svr.meta.indexMeta.updateSegmentIndex(&model.SegmentIndex{
			SegmentID:     seg4.ID,
			CollectionID:  0,
			PartitionID:   0,
			NumRows:       100,
			IndexID:       0,
			BuildID:       0,
			NodeID:        0,
			IndexVersion:  1,
			IndexState:    commonpb.IndexState_Finished,
			FailReason:    "",
			IsDeleted:     false,
			CreateTime:    0,
			IndexFileKeys: nil,
			IndexSize:     0,
		})

		req := &datapb.GetRecoveryInfoRequest{
			CollectionID: 0,
			PartitionID:  0,
		}
		resp, err := svr.GetRecoveryInfo(context.TODO(), req)
		assert.NoError(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		assert.NotNil(t, resp.GetChannels()[0].SeekPosition)
		assert.NotEqual(t, 0, resp.GetChannels()[0].GetSeekPosition().GetTimestamp())
		assert.Len(t, resp.GetChannels()[0].GetDroppedSegmentIds(), 0)
		assert.ElementsMatch(t, []UniqueID{}, resp.GetChannels()[0].GetUnflushedSegmentIds())
		// assert.ElementsMatch(t, []UniqueID{9, 10, 12}, resp.GetChannels()[0].GetFlushedSegmentIds())
	})

	t.Run("with closed server", func(t *testing.T) {
		svr := newTestServer(t)
		closeTestServer(t, svr)
		resp, err := svr.GetRecoveryInfo(context.TODO(), &datapb.GetRecoveryInfoRequest{})
		assert.NoError(t, err)
		assert.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrServiceNotReady)
	})
}

func TestGetCompactionState(t *testing.T) {
	paramtable.Get().Save(Params.DataCoordCfg.EnableCompaction.Key, "true")
	defer paramtable.Get().Reset(Params.DataCoordCfg.EnableCompaction.Key)
	t.Run("test get compaction state with new compaction Handler", func(t *testing.T) {
		svr := &Server{}
		svr.stateCode.Store(commonpb.StateCode_Healthy)

		mockHandler := NewMockCompactionPlanContext(t)
		mockHandler.EXPECT().getCompactionInfo(mock.Anything).Return(&compactionInfo{
			state: commonpb.CompactionState_Completed,
		})
		svr.compactionHandler = mockHandler
		resp, err := svr.GetCompactionState(context.Background(), &milvuspb.GetCompactionStateRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		assert.Equal(t, commonpb.CompactionState_Completed, resp.GetState())
	})
	t.Run("test get compaction state in running", func(t *testing.T) {
		svr := &Server{}
		svr.stateCode.Store(commonpb.StateCode_Healthy)
		mockMeta := NewMockCompactionMeta(t)
		mockMeta.EXPECT().GetCompactionTasksByTriggerID(mock.Anything).Return(
			[]*datapb.CompactionTask{
				{State: datapb.CompactionTaskState_executing},
				{State: datapb.CompactionTaskState_executing},
				{State: datapb.CompactionTaskState_executing},
				{State: datapb.CompactionTaskState_completed},
				{State: datapb.CompactionTaskState_completed},
				{State: datapb.CompactionTaskState_failed, PlanID: 1},
				{State: datapb.CompactionTaskState_timeout, PlanID: 2},
				{State: datapb.CompactionTaskState_timeout},
				{State: datapb.CompactionTaskState_timeout},
				{State: datapb.CompactionTaskState_timeout},
			})
		mockHandler := newCompactionPlanHandler(nil, nil, nil, mockMeta, nil, nil, nil)
		svr.compactionHandler = mockHandler
		resp, err := svr.GetCompactionState(context.Background(), &milvuspb.GetCompactionStateRequest{CompactionID: 1})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		assert.Equal(t, commonpb.CompactionState_Executing, resp.GetState())
		assert.EqualValues(t, 3, resp.GetExecutingPlanNo())
		assert.EqualValues(t, 2, resp.GetCompletedPlanNo())
		assert.EqualValues(t, 1, resp.GetFailedPlanNo())
		assert.EqualValues(t, 4, resp.GetTimeoutPlanNo())
	})

	t.Run("with closed server", func(t *testing.T) {
		svr := &Server{}
		svr.stateCode.Store(commonpb.StateCode_Abnormal)

		resp, err := svr.GetCompactionState(context.Background(), &milvuspb.GetCompactionStateRequest{})
		assert.NoError(t, err)
		assert.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrServiceNotReady)
	})
}

func TestManualCompaction(t *testing.T) {
	paramtable.Get().Save(Params.DataCoordCfg.EnableCompaction.Key, "true")
	defer paramtable.Get().Reset(Params.DataCoordCfg.EnableCompaction.Key)
	t.Run("test manual compaction successfully", func(t *testing.T) {
		svr := &Server{allocator: allocator.NewMockAllocator(t)}
		svr.stateCode.Store(commonpb.StateCode_Healthy)
		svr.compactionTrigger = &mockCompactionTrigger{
			methods: map[string]interface{}{
				"triggerManualCompaction": func(collectionID int64) (UniqueID, error) {
					return 1, nil
				},
			},
		}

		mockHandler := NewMockCompactionPlanContext(t)
		mockHandler.EXPECT().getCompactionTasksNumBySignalID(mock.Anything).Return(1)
		svr.compactionHandler = mockHandler
		resp, err := svr.ManualCompaction(context.TODO(), &milvuspb.ManualCompactionRequest{
			CollectionID: 1,
			Timetravel:   1,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("test manual compaction failure", func(t *testing.T) {
		svr := &Server{allocator: allocator.NewMockAllocator(t)}
		svr.stateCode.Store(commonpb.StateCode_Healthy)
		svr.compactionTrigger = &mockCompactionTrigger{
			methods: map[string]interface{}{
				"triggerManualCompaction": func(collectionID int64) (UniqueID, error) {
					return 0, errors.New("mock error")
				},
			},
		}
		// mockMeta =:
		// mockHandler := newCompactionPlanHandler(nil, nil, nil, mockMeta, nil)
		// svr.compactionHandler = mockHandler
		resp, err := svr.ManualCompaction(context.TODO(), &milvuspb.ManualCompactionRequest{
			CollectionID: 1,
			Timetravel:   1,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
	})

	t.Run("test manual compaction with closed server", func(t *testing.T) {
		svr := &Server{}
		svr.stateCode.Store(commonpb.StateCode_Abnormal)
		svr.compactionTrigger = &mockCompactionTrigger{
			methods: map[string]interface{}{
				"triggerManualCompaction": func(collectionID int64) (UniqueID, error) {
					return 1, nil
				},
			},
		}

		resp, err := svr.ManualCompaction(context.TODO(), &milvuspb.ManualCompactionRequest{
			CollectionID: 1,
			Timetravel:   1,
		})
		assert.NoError(t, err)
		assert.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrServiceNotReady)
	})
}

func TestGetCompactionStateWithPlans(t *testing.T) {
	t.Run("test get compaction state successfully", func(t *testing.T) {
		svr := &Server{}
		svr.stateCode.Store(commonpb.StateCode_Healthy)

		mockHandler := NewMockCompactionPlanContext(t)
		mockHandler.EXPECT().getCompactionInfo(mock.Anything).Return(&compactionInfo{
			state:        commonpb.CompactionState_Executing,
			executingCnt: 1,
		})
		svr.compactionHandler = mockHandler

		resp, err := svr.GetCompactionStateWithPlans(context.TODO(), &milvuspb.GetCompactionPlansRequest{
			CompactionID: 1,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		assert.Equal(t, commonpb.CompactionState_Executing, resp.State)
	})

	t.Run("test get compaction state with closed server", func(t *testing.T) {
		svr := &Server{}
		svr.stateCode.Store(commonpb.StateCode_Abnormal)
		resp, err := svr.GetCompactionStateWithPlans(context.TODO(), &milvuspb.GetCompactionPlansRequest{
			CompactionID: 1,
		})
		assert.NoError(t, err)
		assert.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrServiceNotReady)
	})
}

func TestOptions(t *testing.T) {
	kv := getWatchKV(t)
	defer func() {
		kv.RemoveWithPrefix("")
		kv.Close()
	}()

	t.Run("WithRootCoordCreator", func(t *testing.T) {
		svr := newTestServer(t)
		defer closeTestServer(t, svr)
		var crt rootCoordCreatorFunc = func(ctx context.Context) (types.RootCoordClient, error) {
			return nil, errors.New("dummy")
		}
		opt := WithRootCoordCreator(crt)
		assert.NotNil(t, opt)
		svr.rootCoordClientCreator = nil
		opt(svr)
		// testify cannot compare function directly
		// the behavior is actually undefined
		assert.NotNil(t, crt)
		assert.NotNil(t, svr.rootCoordClientCreator)
	})
	t.Run("WithCluster", func(t *testing.T) {
		defer kv.RemoveWithPrefix("")

		sessionManager := session.NewDataNodeManagerImpl()
		channelManager, err := NewChannelManager(kv, newMockHandler(), sessionManager, allocator.NewMockAllocator(t))
		assert.NoError(t, err)

		cluster := NewClusterImpl(sessionManager, channelManager)
		assert.NoError(t, err)
		opt := WithCluster(cluster)
		assert.NotNil(t, opt)
		svr := newTestServer(t, opt)
		defer closeTestServer(t, svr)

		assert.Same(t, cluster, svr.cluster)
	})
	t.Run("WithDataNodeCreator", func(t *testing.T) {
		var target int64
		val := rand.Int63()
		opt := WithDataNodeCreator(func(context.Context, string, int64) (types.DataNodeClient, error) {
			target = val
			return nil, nil
		})
		assert.NotNil(t, opt)

		factory := dependency.NewDefaultFactory(true)

		svr := CreateServer(context.TODO(), factory, opt)
		dn, err := svr.dataNodeCreator(context.Background(), "", 1)
		assert.Nil(t, dn)
		assert.NoError(t, err)
		assert.Equal(t, target, val)
	})
}

func TestHandleSessionEvent(t *testing.T) {
	kv := getWatchKV(t)
	defer func() {
		kv.RemoveWithPrefix("")
		kv.Close()
	}()
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	alloc := allocator.NewMockAllocator(t)

	sessionManager := session.NewDataNodeManagerImpl()
	channelManager, err := NewChannelManager(kv, newMockHandler(), sessionManager, alloc)
	assert.NoError(t, err)

	cluster := NewClusterImpl(sessionManager, channelManager)
	assert.NoError(t, err)

	err = cluster.Startup(ctx, nil)
	assert.NoError(t, err)
	defer cluster.Close()

	svr := newTestServer(t, WithCluster(cluster))
	defer closeTestServer(t, svr)
	t.Run("handle events", func(t *testing.T) {
		// None event
		evt := &sessionutil.SessionEvent{
			EventType: sessionutil.SessionNoneEvent,
			Session: &sessionutil.Session{
				SessionRaw: sessionutil.SessionRaw{
					ServerID:   0,
					ServerName: "",
					Address:    "",
					Exclusive:  false,
				},
			},
		}
		err = svr.handleSessionEvent(context.Background(), typeutil.DataNodeRole, evt)
		assert.NoError(t, err)

		evt = &sessionutil.SessionEvent{
			EventType: sessionutil.SessionAddEvent,
			Session: &sessionutil.Session{
				SessionRaw: sessionutil.SessionRaw{
					ServerID:   101,
					ServerName: "DN101",
					Address:    "DN127.0.0.101",
					Exclusive:  false,
				},
			},
		}
		err = svr.handleSessionEvent(context.Background(), typeutil.DataNodeRole, evt)
		assert.NoError(t, err)
		dataNodes := svr.cluster.GetSessions()
		assert.EqualValues(t, 1, len(dataNodes))
		assert.EqualValues(t, "DN127.0.0.101", dataNodes[0].Address())

		evt = &sessionutil.SessionEvent{
			EventType: sessionutil.SessionDelEvent,
			Session: &sessionutil.Session{
				SessionRaw: sessionutil.SessionRaw{
					ServerID:   101,
					ServerName: "DN101",
					Address:    "DN127.0.0.101",
					Exclusive:  false,
				},
			},
		}
		err = svr.handleSessionEvent(context.Background(), typeutil.DataNodeRole, evt)
		assert.NoError(t, err)
		dataNodes = svr.cluster.GetSessions()
		assert.EqualValues(t, 0, len(dataNodes))
	})

	t.Run("nil evt", func(t *testing.T) {
		assert.NotPanics(t, func() {
			err = svr.handleSessionEvent(context.Background(), typeutil.DataNodeRole, nil)
			assert.NoError(t, err)
		})
	})
}

type rootCoordSegFlushComplete struct {
	mockRootCoordClient
	flag bool
}

// SegmentFlushCompleted, override default behavior
func (rc *rootCoordSegFlushComplete) SegmentFlushCompleted(ctx context.Context, req *datapb.SegmentFlushCompletedMsg) (*commonpb.Status, error) {
	if rc.flag {
		return merr.Success(), nil
	}
	return &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError}, nil
}

func TestPostFlush(t *testing.T) {
	t.Run("segment not found", func(t *testing.T) {
		svr := newTestServer(t)
		defer closeTestServer(t, svr)

		err := svr.postFlush(context.Background(), 1)
		assert.ErrorIs(t, err, merr.ErrSegmentNotFound)
	})

	t.Run("success post flush", func(t *testing.T) {
		svr := newTestServer(t)
		defer closeTestServer(t, svr)
		svr.rootCoordClient = &rootCoordSegFlushComplete{flag: true}

		err := svr.meta.AddSegment(context.TODO(), NewSegmentInfo(&datapb.SegmentInfo{
			ID:           1,
			CollectionID: 1,
			PartitionID:  1,
			State:        commonpb.SegmentState_Flushing,
			IsSorted:     true,
		}))

		assert.NoError(t, err)

		err = svr.postFlush(context.Background(), 1)
		assert.NoError(t, err)
	})
}

func TestGetFlushAllState(t *testing.T) {
	tests := []struct {
		testName                 string
		ChannelCPs               []Timestamp
		FlushAllTs               Timestamp
		ServerIsHealthy          bool
		ListDatabaseFailed       bool
		ShowCollectionFailed     bool
		DescribeCollectionFailed bool
		ExpectedSuccess          bool
		ExpectedFlushed          bool
	}{
		{
			"test FlushAll flushed",
			[]Timestamp{100, 200},
			99,
			true, false, false, false, true, true,
		},
		{
			"test FlushAll not flushed",
			[]Timestamp{100, 200},
			150,
			true, false, false, false, true, false,
		},
		{
			"test Sever is not healthy", nil, 0,
			false, false, false, false, false, false,
		},
		{
			"test ListDatabase failed", nil, 0,
			true, true, false, false, false, false,
		},
		{
			"test ShowCollections failed", nil, 0,
			true, false, true, false, false, false,
		},
		{
			"test DescribeCollection failed", nil, 0,
			true, false, false, true, false, false,
		},
	}
	for _, test := range tests {
		t.Run(test.testName, func(t *testing.T) {
			collection := UniqueID(0)
			vchannels := []string{"mock-vchannel-0", "mock-vchannel-1"}

			svr := &Server{}
			if test.ServerIsHealthy {
				svr.stateCode.Store(commonpb.StateCode_Healthy)
			}
			var err error
			svr.meta = &meta{}
			svr.rootCoordClient = mocks.NewMockRootCoordClient(t)
			svr.broker = broker.NewCoordinatorBroker(svr.rootCoordClient)
			if test.ListDatabaseFailed {
				svr.rootCoordClient.(*mocks.MockRootCoordClient).EXPECT().ListDatabases(mock.Anything, mock.Anything).
					Return(&milvuspb.ListDatabasesResponse{
						Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError},
					}, nil).Maybe()
			} else {
				svr.rootCoordClient.(*mocks.MockRootCoordClient).EXPECT().ListDatabases(mock.Anything, mock.Anything).
					Return(&milvuspb.ListDatabasesResponse{
						DbNames: []string{"db1"},
						Status:  merr.Success(),
					}, nil).Maybe()
			}

			if test.ShowCollectionFailed {
				svr.rootCoordClient.(*mocks.MockRootCoordClient).EXPECT().ShowCollections(mock.Anything, mock.Anything).
					Return(&milvuspb.ShowCollectionsResponse{
						Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError},
					}, nil).Maybe()
			} else {
				svr.rootCoordClient.(*mocks.MockRootCoordClient).EXPECT().ShowCollections(mock.Anything, mock.Anything).
					Return(&milvuspb.ShowCollectionsResponse{
						Status:        merr.Success(),
						CollectionIds: []int64{collection},
					}, nil).Maybe()
			}

			if test.DescribeCollectionFailed {
				svr.rootCoordClient.(*mocks.MockRootCoordClient).EXPECT().DescribeCollectionInternal(mock.Anything, mock.Anything).
					Return(&milvuspb.DescribeCollectionResponse{
						Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError},
					}, nil).Maybe()
			} else {
				svr.rootCoordClient.(*mocks.MockRootCoordClient).EXPECT().DescribeCollectionInternal(mock.Anything, mock.Anything).
					Return(&milvuspb.DescribeCollectionResponse{
						Status:              merr.Success(),
						VirtualChannelNames: vchannels,
					}, nil).Maybe()
			}

			svr.meta.channelCPs = newChannelCps()
			for i, ts := range test.ChannelCPs {
				channel := vchannels[i]
				svr.meta.channelCPs.checkpoints[channel] = &msgpb.MsgPosition{
					ChannelName: channel,
					Timestamp:   ts,
				}
			}

			resp, err := svr.GetFlushAllState(context.TODO(), &milvuspb.GetFlushAllStateRequest{FlushAllTs: test.FlushAllTs})
			assert.NoError(t, err)
			if test.ExpectedSuccess {
				assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
			} else if test.ServerIsHealthy {
				assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
			} else {
				assert.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrServiceNotReady)
			}
			assert.Equal(t, test.ExpectedFlushed, resp.GetFlushed())
		})
	}
}

func TestGetFlushAllStateWithDB(t *testing.T) {
	tests := []struct {
		testName        string
		FlushAllTs      Timestamp
		DbExist         bool
		ExpectedSuccess bool
		ExpectedFlushed bool
	}{
		{"test FlushAllWithDB, db exist", 99, true, true, true},
		{"test FlushAllWithDB, db not exist", 99, false, false, false},
	}
	for _, test := range tests {
		t.Run(test.testName, func(t *testing.T) {
			collectionID := UniqueID(0)
			dbName := "db"
			collectionName := "collection"
			vchannels := []string{"mock-vchannel-0", "mock-vchannel-1"}

			svr := &Server{}
			svr.stateCode.Store(commonpb.StateCode_Healthy)
			var err error
			svr.meta = &meta{}
			svr.rootCoordClient = mocks.NewMockRootCoordClient(t)
			svr.broker = broker.NewCoordinatorBroker(svr.rootCoordClient)

			if test.DbExist {
				svr.rootCoordClient.(*mocks.MockRootCoordClient).EXPECT().ListDatabases(mock.Anything, mock.Anything).
					Return(&milvuspb.ListDatabasesResponse{
						DbNames: []string{dbName},
						Status:  merr.Success(),
					}, nil).Maybe()
			} else {
				svr.rootCoordClient.(*mocks.MockRootCoordClient).EXPECT().ListDatabases(mock.Anything, mock.Anything).
					Return(&milvuspb.ListDatabasesResponse{
						DbNames: []string{},
						Status:  merr.Success(),
					}, nil).Maybe()
			}

			svr.rootCoordClient.(*mocks.MockRootCoordClient).EXPECT().ShowCollections(mock.Anything, mock.Anything).
				Return(&milvuspb.ShowCollectionsResponse{
					Status:        merr.Success(),
					CollectionIds: []int64{collectionID},
				}, nil).Maybe()

			svr.rootCoordClient.(*mocks.MockRootCoordClient).EXPECT().DescribeCollectionInternal(mock.Anything, mock.Anything).
				Return(&milvuspb.DescribeCollectionResponse{
					Status:              merr.Success(),
					VirtualChannelNames: vchannels,
					CollectionID:        collectionID,
					CollectionName:      collectionName,
				}, nil).Maybe()

			svr.meta.channelCPs = newChannelCps()
			channelCPs := []Timestamp{100, 200}
			for i, ts := range channelCPs {
				channel := vchannels[i]
				svr.meta.channelCPs.checkpoints[channel] = &msgpb.MsgPosition{
					ChannelName: channel,
					Timestamp:   ts,
				}
			}

			var resp *milvuspb.GetFlushAllStateResponse
			resp, err = svr.GetFlushAllState(context.TODO(), &milvuspb.GetFlushAllStateRequest{FlushAllTs: test.FlushAllTs, DbName: dbName})
			assert.NoError(t, err)
			if test.ExpectedSuccess {
				assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
			} else {
				assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
			}
			assert.Equal(t, test.ExpectedFlushed, resp.GetFlushed())
		})
	}
}

func TestDataCoordServer_SetSegmentState(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		svr := newTestServer(t)
		defer closeTestServer(t, svr)
		segment := &datapb.SegmentInfo{
			ID:            1000,
			CollectionID:  100,
			PartitionID:   0,
			InsertChannel: "c1",
			NumOfRows:     0,
			State:         commonpb.SegmentState_Growing,
			StartPosition: &msgpb.MsgPosition{
				ChannelName: "c1",
				MsgID:       []byte{},
				MsgGroup:    "",
				Timestamp:   0,
			},
		}
		err := svr.meta.AddSegment(context.TODO(), NewSegmentInfo(segment))
		assert.NoError(t, err)
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
		assert.NoError(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		assert.EqualValues(t, 1, len(resp.States))
		assert.EqualValues(t, commonpb.SegmentState_Flushed, resp.States[0].State)
	})

	t.Run("dataCoord meta set state not exists", func(t *testing.T) {
		meta, err := newMemoryMeta()
		assert.NoError(t, err)
		svr := newTestServer(t, WithMeta(meta))
		defer closeTestServer(t, svr)
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
		assert.NoError(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		assert.EqualValues(t, 1, len(resp.States))
		assert.EqualValues(t, commonpb.SegmentState_NotExist, resp.States[0].State)
	})

	t.Run("with closed server", func(t *testing.T) {
		svr := newTestServer(t)
		closeTestServer(t, svr)
		resp, err := svr.SetSegmentState(context.TODO(), &datapb.SetSegmentStateRequest{
			SegmentId: 1000,
			NewState:  commonpb.SegmentState_Flushed,
		})
		assert.NoError(t, err)
		assert.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrServiceNotReady)
	})
}

func TestDataCoord_SegmentStatistics(t *testing.T) {
	t.Run("test update imported segment stat", func(t *testing.T) {
		svr := newTestServer(t)

		seg1 := &datapb.SegmentInfo{
			ID:        100,
			Binlogs:   []*datapb.FieldBinlog{getFieldBinlogIDsWithEntry(101, 1, 1)},
			Statslogs: []*datapb.FieldBinlog{getFieldBinlogIDs(1, 2)},
			Deltalogs: []*datapb.FieldBinlog{getFieldBinlogIDs(1, 3)},
			State:     commonpb.SegmentState_Importing,
		}

		info := NewSegmentInfo(seg1)
		svr.meta.AddSegment(context.TODO(), info)

		status, err := svr.UpdateSegmentStatistics(context.TODO(), &datapb.UpdateSegmentStatisticsRequest{
			Stats: []*commonpb.SegmentStats{{
				SegmentID: 100,
				NumRows:   int64(1),
			}},
		})
		assert.NoError(t, err)

		assert.Equal(t, svr.meta.GetHealthySegment(100).currRows, int64(1))
		assert.Equal(t, commonpb.ErrorCode_Success, status.GetErrorCode())
		closeTestServer(t, svr)
	})

	t.Run("test update flushed segment stat", func(t *testing.T) {
		svr := newTestServer(t)

		seg1 := &datapb.SegmentInfo{
			ID:        100,
			Binlogs:   []*datapb.FieldBinlog{getFieldBinlogIDsWithEntry(101, 1, 1)},
			Statslogs: []*datapb.FieldBinlog{getFieldBinlogIDs(1, 2)},
			Deltalogs: []*datapb.FieldBinlog{getFieldBinlogIDs(1, 3)},
			State:     commonpb.SegmentState_Flushed,
		}

		info := NewSegmentInfo(seg1)
		svr.meta.AddSegment(context.TODO(), info)

		status, err := svr.UpdateSegmentStatistics(context.TODO(), &datapb.UpdateSegmentStatisticsRequest{
			Stats: []*commonpb.SegmentStats{{
				SegmentID: 100,
				NumRows:   int64(1),
			}},
		})
		assert.NoError(t, err)

		assert.Equal(t, svr.meta.GetHealthySegment(100).currRows, int64(0))
		assert.Equal(t, commonpb.ErrorCode_Success, status.GetErrorCode())
		closeTestServer(t, svr)
	})
}

func TestDataCoordServer_UpdateChannelCheckpoint(t *testing.T) {
	mockVChannel := "fake-by-dev-rootcoord-dml-1-testchannelcp-v0"

	t.Run("UpdateChannelCheckpoint_Success", func(t *testing.T) {
		svr := newTestServer(t)
		defer closeTestServer(t, svr)

		datanodeID := int64(1)
		channelManager := NewMockChannelManager(t)
		channelManager.EXPECT().Match(datanodeID, mockVChannel).Return(true)

		svr.channelManager = channelManager
		req := &datapb.UpdateChannelCheckpointRequest{
			Base: &commonpb.MsgBase{
				SourceID: datanodeID,
			},
			VChannel: mockVChannel,
			Position: &msgpb.MsgPosition{
				ChannelName: mockVChannel,
				Timestamp:   1000,
				MsgID:       []byte{0, 0, 0, 0, 0, 0, 0, 0},
			},
		}

		resp, err := svr.UpdateChannelCheckpoint(context.TODO(), req)
		assert.NoError(t, merr.CheckRPCCall(resp, err))

		cp := svr.meta.GetChannelCheckpoint(mockVChannel)
		assert.NotNil(t, cp)
		svr.meta.DropChannelCheckpoint(mockVChannel)

		req = &datapb.UpdateChannelCheckpointRequest{
			Base: &commonpb.MsgBase{
				SourceID: datanodeID,
			},
			VChannel: mockVChannel,
			ChannelCheckpoints: []*msgpb.MsgPosition{{
				ChannelName: mockVChannel,
				Timestamp:   1000,
				MsgID:       []byte{0, 0, 0, 0, 0, 0, 0, 0},
			}},
		}

		resp, err = svr.UpdateChannelCheckpoint(context.TODO(), req)
		assert.NoError(t, merr.CheckRPCCall(resp, err))
		cp = svr.meta.GetChannelCheckpoint(mockVChannel)
		assert.NotNil(t, cp)
	})

	t.Run("UpdateChannelCheckpoint_NodeNotMatch", func(t *testing.T) {
		svr := newTestServer(t)
		defer closeTestServer(t, svr)

		datanodeID := int64(1)
		channelManager := NewMockChannelManager(t)
		channelManager.EXPECT().Match(datanodeID, mockVChannel).Return(false)

		svr.channelManager = channelManager
		req := &datapb.UpdateChannelCheckpointRequest{
			Base: &commonpb.MsgBase{
				SourceID: datanodeID,
			},
			VChannel: mockVChannel,
			Position: &msgpb.MsgPosition{
				ChannelName: mockVChannel,
				Timestamp:   1000,
				MsgID:       []byte{0, 0, 0, 0, 0, 0, 0, 0},
			},
		}

		resp, err := svr.UpdateChannelCheckpoint(context.TODO(), req)
		assert.Error(t, merr.CheckRPCCall(resp, err))
		assert.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrChannelNotFound)
		cp := svr.meta.GetChannelCheckpoint(mockVChannel)
		assert.Nil(t, cp)

		req = &datapb.UpdateChannelCheckpointRequest{
			Base: &commonpb.MsgBase{
				SourceID: datanodeID,
			},
			VChannel: mockVChannel,
			ChannelCheckpoints: []*msgpb.MsgPosition{{
				ChannelName: mockVChannel,
				Timestamp:   1000,
				MsgID:       []byte{0, 0, 0, 0, 0, 0, 0, 0},
			}},
		}

		resp, err = svr.UpdateChannelCheckpoint(context.TODO(), req)
		assert.NoError(t, merr.CheckRPCCall(resp, err))
		cp = svr.meta.GetChannelCheckpoint(mockVChannel)
		assert.Nil(t, cp)
	})
}

var globalTestTikv = tikv.SetupLocalTxn()

func WithMeta(meta *meta) Option {
	return func(svr *Server) {
		svr.meta = meta

		svr.watchClient = etcdkv.NewEtcdKV(svr.etcdCli, Params.EtcdCfg.MetaRootPath.GetValue(),
			etcdkv.WithRequestTimeout(paramtable.Get().ServiceParam.EtcdCfg.RequestTimeout.GetAsDuration(time.Millisecond)))
		metaRootPath := Params.EtcdCfg.MetaRootPath.GetValue()
		svr.kv = etcdkv.NewEtcdKV(svr.etcdCli, metaRootPath,
			etcdkv.WithRequestTimeout(paramtable.Get().ServiceParam.EtcdCfg.RequestTimeout.GetAsDuration(time.Millisecond)))
	}
}

func newTestServer(t *testing.T, opts ...Option) *Server {
	var err error
	paramtable.Get().Save(Params.CommonCfg.DataCoordTimeTick.Key, Params.CommonCfg.DataCoordTimeTick.GetValue()+strconv.Itoa(rand.Int()))
	paramtable.Get().Save(Params.RocksmqCfg.CompressionTypes.Key, "0,0,0,0,0")
	factory := dependency.NewDefaultFactory(true)
	etcdCli, err := etcd.GetEtcdClient(
		Params.EtcdCfg.UseEmbedEtcd.GetAsBool(),
		Params.EtcdCfg.EtcdUseSSL.GetAsBool(),
		Params.EtcdCfg.Endpoints.GetAsStrings(),
		Params.EtcdCfg.EtcdTLSCert.GetValue(),
		Params.EtcdCfg.EtcdTLSKey.GetValue(),
		Params.EtcdCfg.EtcdTLSCACert.GetValue(),
		Params.EtcdCfg.EtcdTLSMinVersion.GetValue())
	assert.NoError(t, err)
	sessKey := path.Join(Params.EtcdCfg.MetaRootPath.GetValue(), sessionutil.DefaultServiceRoot)
	_, err = etcdCli.Delete(context.Background(), sessKey, clientv3.WithPrefix())
	assert.NoError(t, err)

	svr := CreateServer(context.TODO(), factory)
	svr.SetEtcdClient(etcdCli)
	svr.SetTiKVClient(globalTestTikv)

	svr.dataNodeCreator = func(ctx context.Context, addr string, nodeID int64) (types.DataNodeClient, error) {
		return newMockDataNodeClient(0, nil)
	}
	svr.rootCoordClientCreator = func(ctx context.Context) (types.RootCoordClient, error) {
		return newMockRootCoordClient(), nil
	}
	for _, opt := range opts {
		opt(svr)
	}

	err = svr.Init()
	assert.NoError(t, err)

	signal := make(chan struct{})
	if Params.DataCoordCfg.EnableActiveStandby.GetAsBool() {
		assert.Equal(t, commonpb.StateCode_StandBy, svr.stateCode.Load().(commonpb.StateCode))
		activateFunc := svr.activateFunc
		svr.activateFunc = func() error {
			defer func() {
				close(signal)
			}()
			var err error
			if activateFunc != nil {
				err = activateFunc()
			}
			return err
		}
	} else {
		assert.Equal(t, commonpb.StateCode_Initializing, svr.stateCode.Load().(commonpb.StateCode))
		close(signal)
	}

	err = svr.Register()
	assert.NoError(t, err)
	<-signal
	err = svr.Start()
	assert.NoError(t, err)
	assert.Equal(t, commonpb.StateCode_Healthy, svr.stateCode.Load().(commonpb.StateCode))

	return svr
}

func closeTestServer(t *testing.T, svr *Server) {
	err := svr.Stop()
	assert.NoError(t, err)
	err = svr.CleanMeta()
	assert.NoError(t, err)
	paramtable.Get().Reset(Params.CommonCfg.DataCoordTimeTick.Key)
}

func Test_CheckHealth(t *testing.T) {
	getSessionManager := func(isHealthy bool) *session.DataNodeManagerImpl {
		var client *mockDataNodeClient
		if isHealthy {
			client = &mockDataNodeClient{
				id:    1,
				state: commonpb.StateCode_Healthy,
			}
		} else {
			client = &mockDataNodeClient{
				id:    1,
				state: commonpb.StateCode_Abnormal,
			}
		}

		sm := session.NewDataNodeManagerImpl(session.WithDataNodeCreator(func(ctx context.Context, addr string, nodeID int64) (types.DataNodeClient, error) {
			return client, nil
		}))
		sm.AddSession(&session.NodeInfo{
			NodeID: 1,
		})
		return sm
	}

	getChannelManager := func(t *testing.T, findWatcherOk bool) ChannelManager {
		channelManager := NewMockChannelManager(t)
		if findWatcherOk {
			channelManager.EXPECT().FindWatcher(mock.Anything).Return(0, nil)
		} else {
			channelManager.EXPECT().FindWatcher(mock.Anything).Return(0, errors.New("error"))
		}
		return channelManager
	}

	collections := map[UniqueID]*collectionInfo{
		449684528748778322: {
			ID:            449684528748778322,
			VChannelNames: []string{"ch1", "ch2"},
		},
		2: nil,
	}

	t.Run("not healthy", func(t *testing.T) {
		ctx := context.Background()
		s := &Server{session: &sessionutil.Session{SessionRaw: sessionutil.SessionRaw{ServerID: 1}}}
		s.stateCode.Store(commonpb.StateCode_Abnormal)
		resp, err := s.CheckHealth(ctx, &milvuspb.CheckHealthRequest{})
		assert.NoError(t, err)
		assert.Equal(t, false, resp.IsHealthy)
		assert.NotEmpty(t, resp.Reasons)
	})

	t.Run("data node health check is fail", func(t *testing.T) {
		svr := &Server{session: &sessionutil.Session{SessionRaw: sessionutil.SessionRaw{ServerID: 1}}}
		svr.stateCode.Store(commonpb.StateCode_Healthy)
		svr.sessionManager = getSessionManager(false)
		ctx := context.Background()
		resp, err := svr.CheckHealth(ctx, &milvuspb.CheckHealthRequest{})
		assert.NoError(t, err)
		assert.Equal(t, false, resp.IsHealthy)
		assert.NotEmpty(t, resp.Reasons)
	})

	t.Run("check channel watched fail", func(t *testing.T) {
		svr := &Server{session: &sessionutil.Session{SessionRaw: sessionutil.SessionRaw{ServerID: 1}}}
		svr.stateCode.Store(commonpb.StateCode_Healthy)
		svr.sessionManager = getSessionManager(true)
		svr.channelManager = getChannelManager(t, false)
		svr.meta = &meta{collections: collections}
		ctx := context.Background()
		resp, err := svr.CheckHealth(ctx, &milvuspb.CheckHealthRequest{})
		assert.NoError(t, err)
		assert.Equal(t, false, resp.IsHealthy)
		assert.NotEmpty(t, resp.Reasons)
	})

	t.Run("check checkpoint fail", func(t *testing.T) {
		svr := &Server{session: &sessionutil.Session{SessionRaw: sessionutil.SessionRaw{ServerID: 1}}}
		svr.stateCode.Store(commonpb.StateCode_Healthy)
		svr.sessionManager = getSessionManager(true)
		svr.channelManager = getChannelManager(t, true)
		svr.meta = &meta{
			collections: collections,
			channelCPs: &channelCPs{
				checkpoints: map[string]*msgpb.MsgPosition{
					"cluster-id-rootcoord-dm_3_449684528748778322v0": {
						Timestamp: tsoutil.ComposeTSByTime(time.Now().Add(-1000*time.Hour), 0),
						MsgID:     []byte{1, 2, 3, 4},
					},
				},
			},
		}

		ctx := context.Background()
		resp, err := svr.CheckHealth(ctx, &milvuspb.CheckHealthRequest{})
		assert.NoError(t, err)
		assert.Equal(t, false, resp.IsHealthy)
		assert.NotEmpty(t, resp.Reasons)
	})

	t.Run("ok", func(t *testing.T) {
		svr := &Server{session: &sessionutil.Session{SessionRaw: sessionutil.SessionRaw{ServerID: 1}}}
		svr.stateCode.Store(commonpb.StateCode_Healthy)
		svr.sessionManager = getSessionManager(true)
		svr.channelManager = getChannelManager(t, true)
		svr.meta = &meta{
			collections: collections,
			channelCPs: &channelCPs{
				checkpoints: map[string]*msgpb.MsgPosition{
					"cluster-id-rootcoord-dm_3_449684528748778322v0": {
						Timestamp: tsoutil.ComposeTSByTime(time.Now(), 0),
						MsgID:     []byte{1, 2, 3, 4},
					},
					"cluster-id-rootcoord-dm_3_449684528748778323v0": {
						Timestamp: tsoutil.ComposeTSByTime(time.Now(), 0),
						MsgID:     []byte{1, 2, 3, 4},
					},
					"invalid-vchannel-name": {
						Timestamp: tsoutil.ComposeTSByTime(time.Now(), 0),
						MsgID:     []byte{1, 2, 3, 4},
					},
				},
			},
		}
		ctx := context.Background()
		resp, err := svr.CheckHealth(ctx, &milvuspb.CheckHealthRequest{})
		assert.NoError(t, err)
		assert.Equal(t, true, resp.IsHealthy)
		assert.Empty(t, resp.Reasons)
	})
}

func Test_newChunkManagerFactory(t *testing.T) {
	server := newTestServer(t)
	paramtable.Get().Save(Params.DataCoordCfg.EnableGarbageCollection.Key, "true")
	defer closeTestServer(t, server)

	t.Run("err_minio_bad_address", func(t *testing.T) {
		paramtable.Get().Save(Params.CommonCfg.StorageType.Key, "minio")
		paramtable.Get().Save(Params.MinioCfg.Address.Key, "host:9000:bad")
		defer paramtable.Get().Reset(Params.MinioCfg.Address.Key)
		storageCli, err := server.newChunkManagerFactory()
		assert.Nil(t, storageCli)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid port")
	})

	t.Run("local storage init", func(t *testing.T) {
		paramtable.Get().Save(Params.CommonCfg.StorageType.Key, "local")
		defer paramtable.Get().Reset(Params.CommonCfg.StorageType.Key)
		storageCli, err := server.newChunkManagerFactory()
		assert.NotNil(t, storageCli)
		assert.NoError(t, err)
	})
}

func Test_initGarbageCollection(t *testing.T) {
	paramtable.Get().Save(Params.DataCoordCfg.EnableGarbageCollection.Key, "true")
	defer paramtable.Get().Reset(Params.DataCoordCfg.EnableGarbageCollection.Key)

	server := newTestServer(t)
	defer closeTestServer(t, server)

	t.Run("ok", func(t *testing.T) {
		storageCli, err := server.newChunkManagerFactory()
		assert.NotNil(t, storageCli)
		assert.NoError(t, err)
		server.initGarbageCollection(storageCli)
	})
	t.Run("err_minio_bad_address", func(t *testing.T) {
		paramtable.Get().Save(Params.CommonCfg.StorageType.Key, "minio")
		paramtable.Get().Save(Params.MinioCfg.Address.Key, "host:9000:bad")
		defer paramtable.Get().Reset(Params.MinioCfg.Address.Key)
		storageCli, err := server.newChunkManagerFactory()
		assert.Nil(t, storageCli)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid port")
	})
}

func TestDataCoord_DisableActiveStandby(t *testing.T) {
	paramtable.Get().Save(Params.DataCoordCfg.EnableActiveStandby.Key, "false")
	defer paramtable.Get().Reset(Params.DataCoordCfg.EnableActiveStandby.Key)
	svr := newTestServer(t)
	closeTestServer(t, svr)
}

// make sure the main functions work well when EnableActiveStandby=true
func TestDataCoord_EnableActiveStandby(t *testing.T) {
	paramtable.Get().Save(Params.DataCoordCfg.EnableActiveStandby.Key, "true")
	defer paramtable.Get().Reset(Params.DataCoordCfg.EnableActiveStandby.Key)
	svr := newTestServer(t)
	defer closeTestServer(t, svr)
	assert.Eventually(t, func() bool {
		// return svr.
		return svr.GetStateCode() == commonpb.StateCode_Healthy
	}, time.Second*5, time.Millisecond*100)
}

func TestLoadCollectionFromRootCoord(t *testing.T) {
	broker := broker.NewMockBroker(t)
	s := &Server{
		broker: broker,
		meta:   &meta{collections: make(map[UniqueID]*collectionInfo)},
	}

	t.Run("has collection fail with error", func(t *testing.T) {
		broker.EXPECT().HasCollection(mock.Anything, mock.Anything).
			Return(false, errors.New("has collection error")).Once()
		err := s.loadCollectionFromRootCoord(context.TODO(), 0)
		assert.Error(t, err, "has collection error")
	})

	t.Run("has collection with not found", func(t *testing.T) {
		broker.EXPECT().HasCollection(mock.Anything, mock.Anything).Return(false, nil).Once()
		err := s.loadCollectionFromRootCoord(context.TODO(), 0)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, merr.ErrCollectionNotFound))
	})

	broker.EXPECT().HasCollection(mock.Anything, mock.Anything).Return(true, nil)

	t.Run("describeCollectionInternal fail", func(t *testing.T) {
		broker.EXPECT().DescribeCollectionInternal(mock.Anything, mock.Anything).
			Return(nil, errors.New("describeCollectionInternal error")).Once()
		err := s.loadCollectionFromRootCoord(context.TODO(), 0)
		assert.Error(t, err, "describeCollectionInternal error")
	})

	broker.EXPECT().DescribeCollectionInternal(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
		CollectionID: 1,
	}, nil).Twice()

	t.Run("ShowPartitionsInternal fail", func(t *testing.T) {
		broker.EXPECT().ShowPartitionsInternal(mock.Anything, mock.Anything).
			Return(nil, errors.New("ShowPartitionsInternal error")).Once()
		err := s.loadCollectionFromRootCoord(context.TODO(), 0)
		assert.Error(t, err, "ShowPartitionsInternal error")
	})

	broker.EXPECT().ShowPartitionsInternal(mock.Anything, mock.Anything).Return([]int64{2000}, nil).Once()
	t.Run("ok", func(t *testing.T) {
		err := s.loadCollectionFromRootCoord(context.TODO(), 0)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(s.meta.collections))
		_, ok := s.meta.collections[1]
		assert.True(t, ok)
	})
}

func TestUpdateAutoBalanceConfigLoop(t *testing.T) {
	Params.Save(Params.DataCoordCfg.CheckAutoBalanceConfigInterval.Key, "1")
	defer Params.Reset(Params.DataCoordCfg.CheckAutoBalanceConfigInterval.Key)

	t.Run("test old node exist", func(t *testing.T) {
		Params.Save(Params.DataCoordCfg.AutoBalance.Key, "false")
		defer Params.Reset(Params.DataCoordCfg.AutoBalance.Key)
		oldSessions := make(map[string]*sessionutil.Session)
		oldSessions["s1"] = sessionutil.NewSession(context.Background())

		server := &Server{}
		mockSession := sessionutil.NewMockSession(t)
		mockSession.EXPECT().GetSessionsWithVersionRange(mock.Anything, mock.Anything).Return(oldSessions, 0, nil).Maybe()
		server.session = mockSession

		ctx, cancel := context.WithCancel(context.Background())
		wg := &sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(1500 * time.Millisecond)
			server.updateBalanceConfigLoop(ctx)
		}()
		// old data node exist, disable auto balance
		assert.Eventually(t, func() bool {
			return !Params.DataCoordCfg.AutoBalance.GetAsBool()
		}, 3*time.Second, 1*time.Second)

		cancel()
		wg.Wait()
	})

	t.Run("test all old node down", func(t *testing.T) {
		Params.Save(Params.DataCoordCfg.AutoBalance.Key, "false")
		defer Params.Reset(Params.DataCoordCfg.AutoBalance.Key)
		server := &Server{}
		mockSession := sessionutil.NewMockSession(t)
		mockSession.EXPECT().GetSessionsWithVersionRange(mock.Anything, mock.Anything).Return(nil, 0, nil).Maybe()
		server.session = mockSession

		ctx, cancel := context.WithCancel(context.Background())
		wg := &sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			server.updateBalanceConfigLoop(ctx)
		}()
		// all old data node down, enable auto balance
		assert.Eventually(t, func() bool {
			return Params.DataCoordCfg.AutoBalance.GetAsBool()
		}, 3*time.Second, 1*time.Second)

		cancel()
		wg.Wait()
	})
}
