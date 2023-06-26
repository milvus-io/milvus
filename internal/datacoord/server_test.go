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
	"fmt"
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

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/indexnode"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metautil"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func TestMain(m *testing.M) {
	// init embed etcd
	embedetcdServer, tempDir, err := etcd.StartTestEmbedEtcdServer()
	if err != nil {
		log.Fatal(err.Error())
	}
	defer os.RemoveAll(tempDir)
	defer embedetcdServer.Close()

	addrs := etcd.GetEmbedEtcdEndpoints(embedetcdServer)

	paramtable.Init()
	paramtable.Get().Save(Params.EtcdCfg.Endpoints.Key, strings.Join(addrs, ","))

	rand.Seed(time.Now().UnixNano())
	os.Exit(m.Run())
}

func TestGetSegmentInfoChannel(t *testing.T) {
	svr := newTestServer(t, nil)
	defer closeTestServer(t, svr)
	t.Run("get segment info channel", func(t *testing.T) {
		resp, err := svr.GetSegmentInfoChannel(context.TODO())
		assert.NoError(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.EqualValues(t, Params.CommonCfg.DataCoordSegmentInfo.GetValue(), resp.Value)
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
		svr.meta.AddCollection(&collectionInfo{
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
		assert.NoError(t, err)
		assert.EqualValues(t, 1, len(resp.SegIDAssignments))
		assign := resp.SegIDAssignments[0]
		assert.EqualValues(t, commonpb.ErrorCode_Success, assign.Status.ErrorCode)
		assert.EqualValues(t, collID, assign.CollectionID)
		assert.EqualValues(t, partID, assign.PartitionID)
		assert.EqualValues(t, channel0, assign.ChannelName)
		assert.EqualValues(t, 1000, assign.Count)
	})

	t.Run("assign segment for bulkload", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)
		schema := newTestSchema()
		svr.meta.AddCollection(&collectionInfo{
			ID:         collID,
			Schema:     schema,
			Partitions: []int64{},
		})
		req := &datapb.SegmentIDRequest{
			Count:        1000,
			ChannelName:  channel0,
			CollectionID: collID,
			PartitionID:  partID,
			IsImport:     true,
		}

		resp, err := svr.AssignSegmentID(context.TODO(), &datapb.AssignSegmentIDRequest{
			NodeID:            0,
			PeerRole:          "",
			SegmentIDRequests: []*datapb.SegmentIDRequest{req},
		})
		assert.NoError(t, err)
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
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
		assert.Equal(t, serverNotServingErrMsg, resp.GetStatus().GetReason())
	})

	t.Run("assign segment with invalid collection", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)
		svr.rootCoordClient = &mockRootCoord{
			RootCoord: svr.rootCoordClient,
			collID:    collID,
		}
		schema := newTestSchema()
		svr.meta.AddCollection(&collectionInfo{
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
		assert.NoError(t, err)
		assert.EqualValues(t, 0, len(resp.SegIDAssignments))
	})
}

type mockRootCoord struct {
	types.RootCoord
	collID UniqueID
}

func (r *mockRootCoord) DescribeCollectionInternal(ctx context.Context, req *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
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

func (r *mockRootCoord) ReportImport(context.Context, *rootcoordpb.ImportResult) (*commonpb.Status, error) {
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
		Reason:    "something bad",
	}, nil
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
		svr.meta.AddCollection(&collectionInfo{ID: 0, Schema: schema, Partitions: []int64{}})
		allocations, err := svr.segmentManager.AllocSegment(context.TODO(), 0, 1, "channel-1", 1)
		assert.NoError(t, err)
		assert.EqualValues(t, 1, len(allocations))
		expireTs := allocations[0].ExpireTime
		segID := allocations[0].SegmentID

		resp, err := svr.Flush(context.TODO(), req)
		assert.NoError(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)

		svr.meta.SetCurrentRows(segID, 1)
		ids, err := svr.segmentManager.GetFlushableSegments(context.TODO(), "channel-1", expireTs)
		assert.NoError(t, err)
		assert.EqualValues(t, 1, len(ids))
		assert.EqualValues(t, segID, ids[0])
	})

	t.Run("bulkload segment", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)
		schema := newTestSchema()
		svr.meta.AddCollection(&collectionInfo{ID: 0, Schema: schema, Partitions: []int64{}})

		allocations, err := svr.segmentManager.allocSegmentForImport(context.TODO(), 0, 1, "channel-1", 1, 100)
		assert.NoError(t, err)
		expireTs := allocations.ExpireTime
		segID := allocations.SegmentID

		resp, err := svr.Flush(context.TODO(), req)
		assert.NoError(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.EqualValues(t, 0, len(resp.SegmentIDs))
		// should not flush anything since this is a normal flush
		svr.meta.SetCurrentRows(segID, 1)
		ids, err := svr.segmentManager.GetFlushableSegments(context.TODO(), "channel-1", expireTs)
		assert.NoError(t, err)
		assert.EqualValues(t, 0, len(ids))

		req := &datapb.FlushRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Flush,
				MsgID:     0,
				Timestamp: 0,
				SourceID:  0,
			},
			DbID:         0,
			CollectionID: 0,
			IsImport:     true,
		}

		resp, err = svr.Flush(context.TODO(), req)
		assert.NoError(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.EqualValues(t, 1, len(resp.SegmentIDs))

		ids, err = svr.segmentManager.GetFlushableSegments(context.TODO(), "channel-1", expireTs)
		assert.NoError(t, err)
		assert.EqualValues(t, 1, len(ids))
		assert.EqualValues(t, segID, ids[0])
	})

	t.Run("closed server", func(t *testing.T) {
		svr := newTestServer(t, nil)
		closeTestServer(t, svr)
		resp, err := svr.Flush(context.Background(), req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
		assert.Equal(t, serverNotServingErrMsg, resp.GetStatus().GetReason())
	})
}

//func TestGetComponentStates(t *testing.T) {
//svr := newTestServer(t)
//defer closeTestServer(t, svr)
//cli := newMockDataNodeClient(1)
//err := cli.Init()
//assert.NoError(t, err)
//err = cli.Start()
//assert.NoError(t, err)

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
//assert.NoError(t, err)

//resp, err := svr.GetComponentStates(context.TODO())
//assert.NoError(t, err)
//assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
//assert.EqualValues(t, commonpb.StateCode_Healthy, resp.State.StateCode)
//assert.EqualValues(t, 1, len(resp.SubcomponentStates))
//assert.EqualValues(t, commonpb.StateCode_Healthy, resp.SubcomponentStates[0].StateCode)
//}

func TestGetTimeTickChannel(t *testing.T) {
	svr := newTestServer(t, nil)
	defer closeTestServer(t, svr)
	resp, err := svr.GetTimeTickChannel(context.TODO())
	assert.NoError(t, err)
	assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	assert.EqualValues(t, Params.CommonCfg.DataCoordTimeTick.GetValue(), resp.Value)
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
			StartPosition: &msgpb.MsgPosition{
				ChannelName: "c1",
				MsgID:       []byte{},
				MsgGroup:    "",
				Timestamp:   0,
			},
		}
		err := svr.meta.AddSegment(NewSegmentInfo(segment))
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
		assert.NoError(t, err)
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
		assert.NoError(t, err)
		req := &datapb.GetInsertBinlogPathsRequest{
			SegmentID: 0,
		}
		resp, err := svr.GetInsertBinlogPaths(svr.ctx, req)
		assert.NoError(t, err)
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
		assert.NoError(t, err)
		req := &datapb.GetInsertBinlogPathsRequest{
			SegmentID: 1,
		}
		resp, err := svr.GetInsertBinlogPaths(svr.ctx, req)
		assert.NoError(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())

	})

	t.Run("with closed server", func(t *testing.T) {
		svr := newTestServer(t, nil)
		closeTestServer(t, svr)
		resp, err := svr.GetInsertBinlogPaths(context.TODO(), &datapb.GetInsertBinlogPathsRequest{
			SegmentID: 0,
		})
		assert.NoError(t, err)
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
		assert.NoError(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)

	})
	t.Run("with closed server", func(t *testing.T) {
		svr := newTestServer(t, nil)
		closeTestServer(t, svr)
		resp, err := svr.GetCollectionStatistics(context.Background(), &datapb.GetCollectionStatisticsRequest{
			CollectionID: 0,
		})
		assert.NoError(t, err)
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
			PartitionIDs: []int64{0},
		}
		resp, err := svr.GetPartitionStatistics(context.Background(), req)
		assert.NoError(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})
	t.Run("with closed server", func(t *testing.T) {
		svr := newTestServer(t, nil)
		closeTestServer(t, svr)
		resp, err := svr.GetPartitionStatistics(context.Background(), &datapb.GetPartitionStatisticsRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
		assert.Equal(t, serverNotServingErrMsg, resp.GetStatus().GetReason())
	})
}

func TestGetSegmentInfo(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		svr := newTestServer(t, nil)
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
							LogPath:    metautil.BuildInsertLogPath("a", 0, 0, 0, 1, 801),
						},
						{
							EntriesNum: 20,
							LogPath:    metautil.BuildInsertLogPath("a", 0, 0, 0, 1, 802),
						},
						{
							EntriesNum: 20,
							LogPath:    metautil.BuildInsertLogPath("a", 0, 0, 0, 1, 803),
						},
					},
				},
			},
		}
		err := svr.meta.AddSegment(NewSegmentInfo(segInfo))
		assert.NoError(t, err)

		req := &datapb.GetSegmentInfoRequest{
			SegmentIDs: []int64{0},
		}
		resp, err := svr.GetSegmentInfo(svr.ctx, req)
		assert.Equal(t, 1, len(resp.GetInfos()))
		// Check that # of rows is corrected from 100 to 60.
		assert.EqualValues(t, 60, resp.GetInfos()[0].GetNumOfRows())
		assert.NoError(t, err)
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
		assert.NoError(t, err)

		req := &datapb.GetSegmentInfoRequest{
			SegmentIDs: []int64{0, 1},
		}
		resp, err := svr.GetSegmentInfo(svr.ctx, req)
		assert.NoError(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_UnexpectedError, resp.Status.ErrorCode)
	})
	t.Run("with closed server", func(t *testing.T) {
		svr := newTestServer(t, nil)
		closeTestServer(t, svr)
		resp, err := svr.GetSegmentInfo(context.Background(), &datapb.GetSegmentInfoRequest{
			SegmentIDs: []int64{},
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
		assert.Equal(t, serverNotServingErrMsg, resp.GetStatus().GetReason())
	})
	t.Run("with dropped segment", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)

		segInfo := &datapb.SegmentInfo{
			ID:    0,
			State: commonpb.SegmentState_Dropped,
		}
		err := svr.meta.AddSegment(NewSegmentInfo(segInfo))
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
			MsgID:       []byte{},
			Timestamp:   1000,
		}

		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)

		segInfo := &datapb.SegmentInfo{
			ID:    0,
			State: commonpb.SegmentState_Flushed,
		}
		err := svr.meta.AddSegment(NewSegmentInfo(segInfo))
		assert.NoError(t, err)

		req := &datapb.GetSegmentInfoRequest{
			SegmentIDs: []int64{0},
		}
		// no channel checkpoint
		resp, err := svr.GetSegmentInfo(svr.ctx, req)
		assert.NoError(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.Equal(t, 0, len(resp.GetChannelCheckpoint()))

		// with nil insert channel of segment
		err = svr.meta.UpdateChannelCheckpoint(mockVChannel, pos)
		assert.NoError(t, err)
		resp, err = svr.GetSegmentInfo(svr.ctx, req)
		assert.NoError(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.Equal(t, 0, len(resp.GetChannelCheckpoint()))

		// normal test
		segInfo.InsertChannel = mockVChannel
		segInfo.ID = 2
		req.SegmentIDs = []int64{2}
		err = svr.meta.AddSegment(NewSegmentInfo(segInfo))
		assert.NoError(t, err)
		resp, err = svr.GetSegmentInfo(svr.ctx, req)
		assert.NoError(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.Equal(t, 1, len(resp.GetChannelCheckpoint()))
		assert.Equal(t, mockPChannel, resp.ChannelCheckpoint[mockVChannel].ChannelName)
		assert.Equal(t, Timestamp(1000), resp.ChannelCheckpoint[mockVChannel].Timestamp)
	})
}

func TestGetComponentStates(t *testing.T) {
	svr := &Server{}
	resp, err := svr.GetComponentStates(context.Background())
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
		resp, err := svr.GetComponentStates(context.Background())
		assert.NoError(t, err)
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
			assert.NoError(t, err)
			assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())

			assert.ElementsMatch(t, tc.expected, resp.GetSegments())
		}
	})

	t.Run("with closed server", func(t *testing.T) {
		t.Run("with closed server", func(t *testing.T) {
			svr := newTestServer(t, nil)
			closeTestServer(t, svr)
			resp, err := svr.GetFlushedSegments(context.Background(), &datapb.GetFlushedSegmentsRequest{})
			assert.NoError(t, err)
			assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
			assert.Equal(t, serverNotServingErrMsg, resp.GetStatus().GetReason())
		})
	})
}

func TestGetSegmentsByStates(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		svr := newTestServer(t, nil)
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
				assert.Nil(t, svr.meta.AddSegment(NewSegmentInfo(segInfo)))
			}
			for _, us := range tc.sealedSegments {
				segInfo := &datapb.SegmentInfo{
					ID:           us,
					CollectionID: tc.collID,
					PartitionID:  tc.partID,
					State:        commonpb.SegmentState_Sealed,
				}
				assert.Nil(t, svr.meta.AddSegment(NewSegmentInfo(segInfo)))
			}
			for _, us := range tc.growingSegments {
				segInfo := &datapb.SegmentInfo{
					ID:           us,
					CollectionID: tc.collID,
					PartitionID:  tc.partID,
					State:        commonpb.SegmentState_Growing,
				}
				assert.Nil(t, svr.meta.AddSegment(NewSegmentInfo(segInfo)))
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
			svr := newTestServer(t, nil)
			closeTestServer(t, svr)
			resp, err := svr.GetSegmentsByStates(context.Background(), &datapb.GetSegmentsByStatesRequest{})
			assert.NoError(t, err)
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

//func TestServer_watchCoord(t *testing.T) {
//	Params.Init()
//	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
//	assert.NoError(t, err)
//	etcdKV := etcdkv.NewEtcdKV(etcdCli, Params.EtcdCfg.MetaRootPath)
//	assert.NotNil(t, etcdKV)
//	factory := dependency.NewDefaultFactory(true)
//	svr := CreateServer(context.TODO(), factory)
//	svr.session = &sessionutil.Session{
//		TriggerKill: true,
//	}
//	svr.kvClient = etcdKV
//
//	dnCh := make(chan *sessionutil.SessionEvent)
//	//icCh := make(chan *sessionutil.SessionEvent)
//	qcCh := make(chan *sessionutil.SessionEvent)
//	rcCh := make(chan *sessionutil.SessionEvent)
//
//	svr.dnEventCh = dnCh
//	//svr.icEventCh = icCh
//	svr.qcEventCh = qcCh
//	svr.rcEventCh = rcCh
//
//	segRefer, err := NewSegmentReferenceManager(etcdKV, nil)
//	assert.NoError(t, err)
//	assert.NotNil(t, segRefer)
//	svr.segReferManager = segRefer
//
//	sc := make(chan os.Signal, 1)
//	signal.Notify(sc, syscall.SIGINT)
//	defer signal.Reset(syscall.SIGINT)
//	closed := false
//	sigQuit := make(chan struct{}, 1)
//
//	svr.serverLoopWg.Add(1)
//	go func() {
//		svr.watchService(context.Background())
//	}()
//
//	go func() {
//		<-sc
//		closed = true
//		sigQuit <- struct{}{}
//	}()
//
//	icCh <- &sessionutil.SessionEvent{
//		EventType: sessionutil.SessionAddEvent,
//		Session: &sessionutil.Session{
//			ServerID: 1,
//		},
//	}
//	icCh <- &sessionutil.SessionEvent{
//		EventType: sessionutil.SessionDelEvent,
//		Session: &sessionutil.Session{
//			ServerID: 1,
//		},
//	}
//	close(icCh)
//	<-sigQuit
//	svr.serverLoopWg.Wait()
//	assert.True(t, closed)
//}

//func TestServer_watchQueryCoord(t *testing.T) {
//	Params.Init()
//	etcdCli, err := etcd.GetEtcdClient(
//		Params.EtcdCfg.UseEmbedEtcd.GetAsBool(),
//		Params.EtcdCfg.EtcdUseSSL.GetAsBool(),
//		Params.EtcdCfg.Endpoints.GetAsStrings(),
//		Params.EtcdCfg.EtcdTLSCert.GetValue(),
//		Params.EtcdCfg.EtcdTLSKey.GetValue(),
//		Params.EtcdCfg.EtcdTLSCACert.GetValue(),
//		Params.EtcdCfg.EtcdTLSMinVersion.GetValue())
//	assert.NoError(t, err)
//	etcdKV := etcdkv.NewEtcdKV(etcdCli, Params.EtcdCfg.MetaRootPath.GetValue())
//	assert.NotNil(t, etcdKV)
//	factory := dependency.NewDefaultFactory(true)
//	svr := CreateServer(context.TODO(), factory)
//	svr.session = &sessionutil.Session{
//		TriggerKill: true,
//	}
//	svr.kvClient = etcdKV
//
//	dnCh := make(chan *sessionutil.SessionEvent)
//	//icCh := make(chan *sessionutil.SessionEvent)
//	qcCh := make(chan *sessionutil.SessionEvent)
//
//	svr.dnEventCh = dnCh
//
//	segRefer, err := NewSegmentReferenceManager(etcdKV, nil)
//	assert.NoError(t, err)
//	assert.NotNil(t, segRefer)
//
//	sc := make(chan os.Signal, 1)
//	signal.Notify(sc, syscall.SIGINT)
//	defer signal.Reset(syscall.SIGINT)
//	closed := false
//	sigQuit := make(chan struct{}, 1)
//
//	svr.serverLoopWg.Add(1)
//	go func() {
//		svr.watchService(context.Background())
//	}()
//
//	go func() {
//		<-sc
//		closed = true
//		sigQuit <- struct{}{}
//	}()
//
//	qcCh <- &sessionutil.SessionEvent{
//		EventType: sessionutil.SessionAddEvent,
//		Session: &sessionutil.Session{
//			ServerID: 2,
//		},
//	}
//	qcCh <- &sessionutil.SessionEvent{
//		EventType: sessionutil.SessionDelEvent,
//		Session: &sessionutil.Session{
//			ServerID: 2,
//		},
//	}
//	close(qcCh)
//	<-sigQuit
//	svr.serverLoopWg.Wait()
//	assert.True(t, closed)
//}

func TestServer_ShowConfigurations(t *testing.T) {
	svr := newTestServer(t, nil)
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
	assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.Status.ErrorCode)

	// normal case
	svr.stateCode.Store(stateSave)

	resp, err = svr.ShowConfigurations(svr.ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	assert.Equal(t, 1, len(resp.Configuations))
	assert.Equal(t, "datacoord.port", resp.Configuations[0].Key)
}

func TestServer_GetMetrics(t *testing.T) {
	svr := newTestServer(t, nil)
	defer closeTestServer(t, svr)

	var err error

	// server is closed
	stateSave := svr.stateCode.Load()
	svr.stateCode.Store(commonpb.StateCode_Initializing)
	resp, err := svr.GetMetrics(svr.ctx, &milvuspb.GetMetricsRequest{})
	assert.NoError(t, err)
	assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	svr.stateCode.Store(stateSave)

	// failed to parse metric type
	invalidRequest := "invalid request"
	resp, err = svr.GetMetrics(svr.ctx, &milvuspb.GetMetricsRequest{
		Request: invalidRequest,
	})
	assert.NoError(t, err)
	assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)

	// unsupported metric type
	unsupportedMetricType := "unsupported"
	req, err := metricsinfo.ConstructRequestByMetricType(unsupportedMetricType)
	assert.NoError(t, err)
	resp, err = svr.GetMetrics(svr.ctx, req)
	assert.NoError(t, err)
	assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)

	// normal case
	req, err = metricsinfo.ConstructRequestByMetricType(metricsinfo.SystemInfoMetrics)
	assert.NoError(t, err)
	resp, err = svr.GetMetrics(svr.ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	log.Info("TestServer_GetMetrics",
		zap.String("name", resp.ComponentName),
		zap.String("response", resp.Response))
}

func TestServer_getSystemInfoMetrics(t *testing.T) {
	svr := newTestServer(t, nil)
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
	panic("not implemented") // TODO: Implement
}

func (s *spySegmentManager) allocSegmentForImport(ctx context.Context, collectionID UniqueID, partitionID UniqueID, channelName string, requestRows int64, taskID int64) (*Allocation, error) {
	panic("not implemented") // TODO: Implement
}

// DropSegment drops the segment from manager.
func (s *spySegmentManager) DropSegment(ctx context.Context, segmentID UniqueID) {
}

// SealAllSegments seals all segments of collection with collectionID and return sealed segments
func (s *spySegmentManager) SealAllSegments(ctx context.Context, collectionID UniqueID, segIDs []UniqueID, isImport bool) ([]UniqueID, error) {
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

		// vecFieldID := int64(201)
		svr.meta.AddCollection(&collectionInfo{
			ID: 0,
		})

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
			assert.NoError(t, err)
		}

		err := svr.channelManager.AddNode(0)
		assert.NoError(t, err)
		err = svr.channelManager.Watch(&channel{Name: "ch1", CollectionID: 0})
		assert.NoError(t, err)

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
							LogPath:    "/by-dev/test/0/1/1/1/Allo1",
							EntriesNum: 5,
						},
						{
							LogPath:    "/by-dev/test/0/1/1/1/Allo2",
							EntriesNum: 5,
						},
					},
				},
			},
			CheckPoints: []*datapb.CheckPoint{
				{
					SegmentID: 1,
					Position: &msgpb.MsgPosition{
						ChannelName: "ch1",
						MsgID:       []byte{1, 2, 3},
						MsgGroup:    "",
						Timestamp:   0,
					},
					NumOfRows: 12,
				},
			},
			Flushed: false,
		})
		assert.NoError(t, err)
		assert.EqualValues(t, resp.ErrorCode, commonpb.ErrorCode_Success)

		segment := svr.meta.GetHealthySegment(1)
		assert.NotNil(t, segment)
		binlogs := segment.GetBinlogs()
		assert.EqualValues(t, 1, len(binlogs))
		fieldBinlogs := binlogs[0]
		assert.NotNil(t, fieldBinlogs)
		assert.EqualValues(t, 2, len(fieldBinlogs.GetBinlogs()))
		assert.EqualValues(t, 1, fieldBinlogs.GetFieldID())
		assert.EqualValues(t, "/by-dev/test/0/1/1/1/Allo1", fieldBinlogs.GetBinlogs()[0].GetLogPath())
		assert.EqualValues(t, "/by-dev/test/0/1/1/1/Allo2", fieldBinlogs.GetBinlogs()[1].GetLogPath())

		assert.EqualValues(t, segment.DmlPosition.ChannelName, "ch1")
		assert.EqualValues(t, segment.DmlPosition.MsgID, []byte{1, 2, 3})
		assert.EqualValues(t, segment.NumOfRows, 10)
	})

	t.Run("SaveDroppedSegment", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)

		// vecFieldID := int64(201)
		svr.meta.AddCollection(&collectionInfo{
			ID: 0,
		})

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
				State:         commonpb.SegmentState_Dropped,
			}
			err := svr.meta.AddSegment(NewSegmentInfo(s))
			assert.NoError(t, err)
		}

		err := svr.channelManager.AddNode(0)
		assert.NoError(t, err)
		err = svr.channelManager.Watch(&channel{Name: "ch1", CollectionID: 0})
		assert.NoError(t, err)

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
							LogPath:    "/by-dev/test/0/1/1/1/Allo1",
							EntriesNum: 5,
						},
						{
							LogPath:    "/by-dev/test/0/1/1/1/Allo2",
							EntriesNum: 5,
						},
					},
				},
			},
			CheckPoints: []*datapb.CheckPoint{
				{
					SegmentID: 1,
					Position: &msgpb.MsgPosition{
						ChannelName: "ch1",
						MsgID:       []byte{1, 2, 3},
						MsgGroup:    "",
						Timestamp:   0,
					},
					NumOfRows: 12,
				},
			},
			Flushed: false,
		})
		assert.NoError(t, err)
		assert.EqualValues(t, resp.ErrorCode, commonpb.ErrorCode_Success)

		segment := svr.meta.GetSegment(1)
		assert.NotNil(t, segment)
		binlogs := segment.GetBinlogs()
		assert.EqualValues(t, 0, len(binlogs))
		assert.EqualValues(t, segment.NumOfRows, 0)
	})

	t.Run("SaveUnhealthySegment", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)

		// vecFieldID := int64(201)
		svr.meta.AddCollection(&collectionInfo{
			ID: 0,
		})

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
				State:         commonpb.SegmentState_NotExist,
			}
			err := svr.meta.AddSegment(NewSegmentInfo(s))
			assert.NoError(t, err)
		}

		err := svr.channelManager.AddNode(0)
		assert.NoError(t, err)
		err = svr.channelManager.Watch(&channel{Name: "ch1", CollectionID: 0})
		assert.NoError(t, err)

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
							LogPath:    "/by-dev/test/0/1/1/1/Allo1",
							EntriesNum: 5,
						},
						{
							LogPath:    "/by-dev/test/0/1/1/1/Allo2",
							EntriesNum: 5,
						},
					},
				},
			},
			CheckPoints: []*datapb.CheckPoint{
				{
					SegmentID: 1,
					Position: &msgpb.MsgPosition{
						ChannelName: "ch1",
						MsgID:       []byte{1, 2, 3},
						MsgGroup:    "",
						Timestamp:   0,
					},
					NumOfRows: 12,
				},
			},
			Flushed: false,
		})
		assert.NoError(t, err)
		assert.EqualValues(t, resp.ErrorCode, commonpb.ErrorCode_SegmentNotFound)
	})

	t.Run("SaveNotExistSegment", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)

		// vecFieldID := int64(201)
		svr.meta.AddCollection(&collectionInfo{
			ID: 0,
		})

		err := svr.channelManager.AddNode(0)
		assert.NoError(t, err)
		err = svr.channelManager.Watch(&channel{Name: "ch1", CollectionID: 0})
		assert.NoError(t, err)

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
							LogPath:    "/by-dev/test/0/1/1/1/Allo1",
							EntriesNum: 5,
						},
						{
							LogPath:    "/by-dev/test/0/1/1/1/Allo2",
							EntriesNum: 5,
						},
					},
				},
			},
			CheckPoints: []*datapb.CheckPoint{
				{
					SegmentID: 1,
					Position: &msgpb.MsgPosition{
						ChannelName: "ch1",
						MsgID:       []byte{1, 2, 3},
						MsgGroup:    "",
						Timestamp:   0,
					},
					NumOfRows: 12,
				},
			},
			Flushed: false,
		})
		assert.NoError(t, err)
		assert.EqualValues(t, resp.ErrorCode, commonpb.ErrorCode_SegmentNotFound)
	})

	t.Run("with channel not matched", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)
		err := svr.channelManager.AddNode(0)
		require.Nil(t, err)
		err = svr.channelManager.Watch(&channel{Name: "ch1", CollectionID: 0})
		require.Nil(t, err)
		s := &datapb.SegmentInfo{
			ID:            1,
			InsertChannel: "ch2",
			State:         commonpb.SegmentState_Growing,
		}
		svr.meta.AddSegment(NewSegmentInfo(s))

		resp, err := svr.SaveBinlogPaths(context.Background(), &datapb.SaveBinlogPathsRequest{
			SegmentID: 1,
			Channel:   "test",
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_MetaFailed, resp.GetErrorCode())
	})

	t.Run("with closed server", func(t *testing.T) {
		svr := newTestServer(t, nil)
		closeTestServer(t, svr)
		resp, err := svr.SaveBinlogPaths(context.Background(), &datapb.SaveBinlogPathsRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetErrorCode())
		assert.Equal(t, serverNotServingErrMsg, resp.GetReason())
	})
	/*
		t.Run("test save dropped segment and remove channel", func(t *testing.T) {
			spyCh := make(chan struct{}, 1)
			svr := newTestServer(t, nil, WithSegmentManager(&spySegmentManager{spyCh: spyCh}))
			defer closeTestServer(t, svr)

			svr.meta.AddCollection(&collectionInfo{ID: 1})
			err := svr.meta.AddSegment(&SegmentInfo{
				Segment: &datapb.SegmentInfo{
					ID:            1,
					CollectionID:  1,
					InsertChannel: "ch1",
					State:         commonpb.SegmentState_Growing,
				},
			})
			assert.NoError(t, err)

			err = svr.channelManager.AddNode(0)
			assert.NoError(t, err)
			err = svr.channelManager.Watch(&channel{Name: "ch1", CollectionID: 1})
			assert.NoError(t, err)

			_, err = svr.SaveBinlogPaths(context.TODO(), &datapb.SaveBinlogPathsRequest{
				SegmentID: 1,
				Dropped:   true,
			})
			assert.NoError(t, err)
			<-spyCh
		})*/
}

func TestDropVirtualChannel(t *testing.T) {
	t.Run("normal DropVirtualChannel", func(t *testing.T) {
		spyCh := make(chan struct{}, 1)
		svr := newTestServer(t, nil, WithSegmentManager(&spySegmentManager{spyCh: spyCh}))

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
			err := svr.meta.AddSegment(NewSegmentInfo(s))
			assert.NoError(t, err)
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
		err = svr.channelManager.Watch(&channel{Name: "ch1", CollectionID: 0})
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

		err = svr.channelManager.Watch(&channel{Name: "ch1", CollectionID: 0})
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
		err = svr.channelManager.Watch(&channel{Name: "ch1", CollectionID: 0})
		require.Nil(t, err)

		resp, err := svr.DropVirtualChannel(context.Background(), &datapb.DropVirtualChannelRequest{
			ChannelName: "ch2",
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_MetaFailed, resp.GetStatus().GetErrorCode())
	})

	t.Run("with closed server", func(t *testing.T) {
		svr := newTestServer(t, nil)
		closeTestServer(t, svr)
		resp, err := svr.DropVirtualChannel(context.Background(), &datapb.DropVirtualChannelRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
		assert.Equal(t, serverNotServingErrMsg, resp.GetStatus().GetReason())
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

	tests := []struct {
		testName     string
		channelCP    *msgpb.MsgPosition
		segDMLPos    []*msgpb.MsgPosition
		collStartPos []*commonpb.KeyDataPair
		channelName  string
		expectedPos  *msgpb.MsgPosition
	}{
		{"test-with-channelCP",
			&msgpb.MsgPosition{ChannelName: "ch1", Timestamp: 100},
			[]*msgpb.MsgPosition{{ChannelName: "ch1", Timestamp: 50}, {ChannelName: "ch1", Timestamp: 200}},
			startPos1,
			"ch1", &msgpb.MsgPosition{ChannelName: "ch1", Timestamp: 100}},

		{"test-with-segmentDMLPos",
			nil,
			[]*msgpb.MsgPosition{{ChannelName: "ch1", Timestamp: 50}, {ChannelName: "ch1", Timestamp: 200}},
			startPos1,
			"ch1", &msgpb.MsgPosition{ChannelName: "ch1", Timestamp: 50}},

		{"test-with-collStartPos",
			nil,
			nil,
			startPos1,
			"ch1", &msgpb.MsgPosition{ChannelName: "ch1", MsgID: startPos1[0].Data}},

		{"test-non-exist-channel-1",
			nil,
			nil,
			startPosNonExist,
			"ch1", nil},

		{"test-non-exist-channel-2",
			nil,
			nil,
			nil,
			"ch1", nil},
	}
	for _, test := range tests {
		t.Run(test.testName, func(t *testing.T) {
			svr := newTestServer(t, nil)
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
				err := svr.meta.AddSegment(NewSegmentInfo(seg))
				assert.NoError(t, err)
			}
			if test.channelCP != nil {
				err := svr.meta.UpdateChannelCheckpoint(test.channelCP.ChannelName, test.channelCP)
				assert.NoError(t, err)
			}

			seekPos := svr.handler.(*ServerHandler).GetChannelSeekPosition(&channel{
				Name:         test.channelName,
				CollectionID: 0}, allPartitionID)
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

func TestDescribeCollection(t *testing.T) {
	t.Run("TestNotExistCollections", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)
		has, err := svr.handler.(*ServerHandler).HasCollection(context.TODO(), -1)
		assert.NoError(t, err)
		assert.False(t, has)
	})

	t.Run("TestExistCollections", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)
		has, err := svr.handler.(*ServerHandler).HasCollection(context.TODO(), 1314)
		assert.NoError(t, err)
		assert.True(t, has)
	})
}

func TestGetDataVChanPositions(t *testing.T) {
	svr := newTestServer(t, nil)
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
	err := svr.meta.AddSegment(NewSegmentInfo(s1))
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
	err = svr.meta.AddSegment(NewSegmentInfo(s2))
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
	err = svr.meta.AddSegment(NewSegmentInfo(s3))
	require.Nil(t, err)

	t.Run("get unexisted channel", func(t *testing.T) {
		vchan := svr.handler.GetDataVChanPositions(&channel{Name: "chx1", CollectionID: 0}, allPartitionID)
		assert.Empty(t, vchan.UnflushedSegmentIds)
		assert.Empty(t, vchan.FlushedSegmentIds)
	})

	t.Run("get existed channel", func(t *testing.T) {
		vchan := svr.handler.GetDataVChanPositions(&channel{Name: "ch1", CollectionID: 0}, allPartitionID)
		assert.EqualValues(t, 1, len(vchan.FlushedSegmentIds))
		assert.EqualValues(t, 1, vchan.FlushedSegmentIds[0])
		assert.EqualValues(t, 2, len(vchan.UnflushedSegmentIds))
		assert.ElementsMatch(t, []int64{s2.ID, s3.ID}, vchan.UnflushedSegmentIds)
	})

	t.Run("empty collection", func(t *testing.T) {
		infos := svr.handler.GetDataVChanPositions(&channel{Name: "ch0_suffix", CollectionID: 1}, allPartitionID)
		assert.EqualValues(t, 1, infos.CollectionID)
		assert.EqualValues(t, 0, len(infos.FlushedSegmentIds))
		assert.EqualValues(t, 0, len(infos.UnflushedSegmentIds))
	})

	t.Run("filter partition", func(t *testing.T) {
		infos := svr.handler.GetDataVChanPositions(&channel{Name: "ch1", CollectionID: 0}, 1)
		assert.EqualValues(t, 0, infos.CollectionID)
		assert.EqualValues(t, 0, len(infos.FlushedSegmentIds))
		assert.EqualValues(t, 1, len(infos.UnflushedSegmentIds))
	})

	t.Run("empty collection with passed positions", func(t *testing.T) {
		vchannel := "ch_no_segment_1"
		pchannel := funcutil.ToPhysicalChannel(vchannel)
		infos := svr.handler.GetDataVChanPositions(&channel{
			Name:           vchannel,
			CollectionID:   0,
			StartPositions: []*commonpb.KeyDataPair{{Key: pchannel, Data: []byte{14, 15, 16}}},
		}, allPartitionID)
		assert.EqualValues(t, 0, infos.CollectionID)
		assert.EqualValues(t, vchannel, infos.ChannelName)
	})
}

func TestGetQueryVChanPositions(t *testing.T) {
	svr := newTestServer(t, nil)
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

	err := svr.meta.CreateIndex(&model.Index{
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
	}
	err = svr.meta.AddSegment(NewSegmentInfo(s1))
	assert.NoError(t, err)
	err = svr.meta.AddSegmentIndex(&model.SegmentIndex{
		SegmentID: 1,
		BuildID:   1,
		IndexID:   1,
	})
	assert.NoError(t, err)
	err = svr.meta.FinishTask(&indexpb.IndexTaskInfo{
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
	err = svr.meta.AddSegment(NewSegmentInfo(s2))
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
	err = svr.meta.AddSegment(NewSegmentInfo(s3))
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
		vchan := svr.handler.GetQueryVChanPositions(&channel{Name: "chx1", CollectionID: 0}, allPartitionID)
		assert.Empty(t, vchan.UnflushedSegmentIds)
		assert.Empty(t, vchan.FlushedSegmentIds)
	})

	t.Run("get existed channel", func(t *testing.T) {
		vchan := svr.handler.GetQueryVChanPositions(&channel{Name: "ch1", CollectionID: 0}, allPartitionID)
		assert.EqualValues(t, 1, len(vchan.FlushedSegmentIds))
		assert.EqualValues(t, 1, vchan.FlushedSegmentIds[0])
		assert.EqualValues(t, 2, len(vchan.UnflushedSegmentIds))
		assert.ElementsMatch(t, []int64{s2.ID, s3.ID}, vchan.UnflushedSegmentIds)
	})

	t.Run("empty collection", func(t *testing.T) {
		infos := svr.handler.GetQueryVChanPositions(&channel{Name: "ch0_suffix", CollectionID: 1}, allPartitionID)
		assert.EqualValues(t, 1, infos.CollectionID)
		assert.EqualValues(t, 0, len(infos.FlushedSegmentIds))
		assert.EqualValues(t, 0, len(infos.UnflushedSegmentIds))
	})

	t.Run("filter partition", func(t *testing.T) {
		infos := svr.handler.GetQueryVChanPositions(&channel{Name: "ch1", CollectionID: 0}, 1)
		assert.EqualValues(t, 0, infos.CollectionID)
		assert.EqualValues(t, 0, len(infos.FlushedSegmentIds))
		assert.EqualValues(t, 1, len(infos.UnflushedSegmentIds))
	})

	t.Run("empty collection with passed positions", func(t *testing.T) {
		vchannel := "ch_no_segment_1"
		pchannel := funcutil.ToPhysicalChannel(vchannel)
		infos := svr.handler.GetQueryVChanPositions(&channel{
			Name:           vchannel,
			CollectionID:   0,
			StartPositions: []*commonpb.KeyDataPair{{Key: pchannel, Data: []byte{14, 15, 16}}},
		}, allPartitionID)
		assert.EqualValues(t, 0, infos.CollectionID)
		assert.EqualValues(t, vchannel, infos.ChannelName)
	})
}

func TestGetQueryVChanPositions_Retrieve_unIndexed(t *testing.T) {
	t.Run("ab GC-ed, cde unIndexed", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)
		schema := newTestSchema()
		svr.meta.AddCollection(&collectionInfo{
			ID:     0,
			Schema: schema,
		})
		err := svr.meta.CreateIndex(&model.Index{
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
		err = svr.meta.AddSegment(NewSegmentInfo(c))
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
		err = svr.meta.AddSegment(NewSegmentInfo(d))
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
		}

		err = svr.meta.AddSegment(NewSegmentInfo(e))
		assert.NoError(t, err)
		vchan := svr.handler.GetQueryVChanPositions(&channel{Name: "ch1", CollectionID: 0}, allPartitionID)
		assert.EqualValues(t, 0, len(vchan.FlushedSegmentIds))
		assert.EqualValues(t, 2, len(vchan.UnflushedSegmentIds))
		assert.ElementsMatch(t, []int64{c.GetID(), d.GetID()}, vchan.UnflushedSegmentIds) // expected c, d
	})

	t.Run("a GC-ed, bcde unIndexed", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)
		schema := newTestSchema()
		svr.meta.AddCollection(&collectionInfo{
			ID:     0,
			Schema: schema,
		})
		err := svr.meta.CreateIndex(&model.Index{
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
		err = svr.meta.AddSegment(NewSegmentInfo(a))
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
		err = svr.meta.AddSegment(NewSegmentInfo(c))
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
		err = svr.meta.AddSegment(NewSegmentInfo(d))
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
		}

		err = svr.meta.AddSegment(NewSegmentInfo(e))
		assert.NoError(t, err)
		vchan := svr.handler.GetQueryVChanPositions(&channel{Name: "ch1", CollectionID: 0}, allPartitionID)
		assert.EqualValues(t, 0, len(vchan.FlushedSegmentIds))
		assert.EqualValues(t, 2, len(vchan.UnflushedSegmentIds))
		assert.ElementsMatch(t, []int64{c.GetID(), d.GetID()}, vchan.UnflushedSegmentIds) // expected c, d
	})

	t.Run("ab GC-ed, c unIndexed, de indexed", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)
		schema := newTestSchema()
		svr.meta.AddCollection(&collectionInfo{
			ID:     0,
			Schema: schema,
		})
		err := svr.meta.CreateIndex(&model.Index{
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
		err = svr.meta.AddSegment(NewSegmentInfo(c))
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
		err = svr.meta.AddSegment(NewSegmentInfo(d))
		assert.NoError(t, err)
		err = svr.meta.AddSegmentIndex(&model.SegmentIndex{
			SegmentID: 2,
			BuildID:   1,
			IndexID:   1,
		})
		assert.NoError(t, err)
		err = svr.meta.FinishTask(&indexpb.IndexTaskInfo{
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
		}
		err = svr.meta.AddSegment(NewSegmentInfo(e))
		assert.NoError(t, err)
		err = svr.meta.AddSegmentIndex(&model.SegmentIndex{
			SegmentID: 3,
			BuildID:   2,
			IndexID:   1,
		})
		assert.NoError(t, err)
		err = svr.meta.FinishTask(&indexpb.IndexTaskInfo{
			BuildID: 2,
			State:   commonpb.IndexState_Finished,
		})
		assert.NoError(t, err)

		vchan := svr.handler.GetQueryVChanPositions(&channel{Name: "ch1", CollectionID: 0}, allPartitionID)
		assert.EqualValues(t, 1, len(vchan.FlushedSegmentIds))
		assert.EqualValues(t, 0, len(vchan.UnflushedSegmentIds))
		assert.ElementsMatch(t, []int64{e.GetID()}, vchan.FlushedSegmentIds) // expected e
	})
}

func TestShouldDropChannel(t *testing.T) {
	type myRootCoord struct {
		mocks.RootCoord
	}
	myRoot := &myRootCoord{}
	myRoot.EXPECT().Init().Return(nil)
	myRoot.EXPECT().Start().Return(nil)
	myRoot.EXPECT().AllocTimestamp(mock.Anything, mock.Anything).Return(&rootcoordpb.AllocTimestampResponse{
		Status:    &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		Timestamp: tsoutil.ComposeTSByTime(time.Now(), 0),
		Count:     1,
	}, nil)

	myRoot.EXPECT().AllocID(mock.Anything, mock.Anything).Return(&rootcoordpb.AllocIDResponse{
		Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		ID:     int64(tsoutil.ComposeTSByTime(time.Now(), 0)),
		Count:  1,
	}, nil)

	var crt rootCoordCreatorFunc = func(ctx context.Context, metaRoot string, etcdClient *clientv3.Client) (types.RootCoord, error) {
		return myRoot, nil
	}

	opt := WithRootCoordCreator(crt)
	svr := newTestServer(t, nil, opt)
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

	t.Run("channel name not in kv, collection not exist", func(t *testing.T) {
		//myRoot.code = commonpb.ErrorCode_CollectionNotExists
		myRoot.EXPECT().DescribeCollection(mock.Anything, mock.Anything).
			Return(&milvuspb.DescribeCollectionResponse{
				Status:       &commonpb.Status{ErrorCode: commonpb.ErrorCode_CollectionNotExists},
				CollectionID: -1,
			}, nil).Once()
		assert.True(t, svr.handler.CheckShouldDropChannel("ch99", -1))
	})

	t.Run("channel name not in kv, collection exist", func(t *testing.T) {
		myRoot.EXPECT().DescribeCollection(mock.Anything, mock.Anything).
			Return(&milvuspb.DescribeCollectionResponse{
				Status:       &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
				CollectionID: 0,
			}, nil).Once()
		assert.False(t, svr.handler.CheckShouldDropChannel("ch99", 0))
	})

	t.Run("collection name in kv, collection exist", func(t *testing.T) {
		myRoot.EXPECT().DescribeCollection(mock.Anything, mock.Anything).
			Return(&milvuspb.DescribeCollectionResponse{
				Status:       &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
				CollectionID: 0,
			}, nil).Once()
		assert.False(t, svr.handler.CheckShouldDropChannel("ch1", 0))
	})

	t.Run("collection name in kv, collection not exist", func(t *testing.T) {
		myRoot.EXPECT().DescribeCollection(mock.Anything, mock.Anything).
			Return(&milvuspb.DescribeCollectionResponse{
				Status:       &commonpb.Status{ErrorCode: commonpb.ErrorCode_CollectionNotExists},
				CollectionID: -1,
			}, nil).Once()
		assert.True(t, svr.handler.CheckShouldDropChannel("ch1", -1))
	})

	t.Run("channel in remove flag, collection exist", func(t *testing.T) {
		err := svr.meta.catalog.MarkChannelDeleted(context.TODO(), "ch1")
		require.NoError(t, err)
		myRoot.EXPECT().DescribeCollection(mock.Anything, mock.Anything).
			Return(&milvuspb.DescribeCollectionResponse{
				Status:       &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
				CollectionID: 0,
			}, nil).Once()
		assert.True(t, svr.handler.CheckShouldDropChannel("ch1", 0))
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
		assert.NoError(t, err)
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
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)

		svr.rootCoordClientCreator = func(ctx context.Context, metaRootPath string, etcdCli *clientv3.Client) (types.RootCoord, error) {
			return newMockRootCoordService(), nil
		}

		svr.meta.AddCollection(&collectionInfo{
			Schema: newTestSchema(),
		})

		err := svr.meta.UpdateChannelCheckpoint("vchan1", &msgpb.MsgPosition{
			ChannelName: "vchan1",
			Timestamp:   10,
		})
		assert.NoError(t, err)

		err = svr.meta.CreateIndex(&model.Index{
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
						LogPath:    metautil.BuildInsertLogPath("a", 0, 0, 0, 1, 901),
					},
					{
						EntriesNum: 20,
						LogPath:    metautil.BuildInsertLogPath("a", 0, 0, 0, 1, 902),
					},
					{
						EntriesNum: 20,
						LogPath:    metautil.BuildInsertLogPath("a", 0, 0, 0, 1, 903),
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
						LogPath:    metautil.BuildInsertLogPath("a", 0, 0, 1, 1, 801),
					},
					{
						EntriesNum: 70,
						LogPath:    metautil.BuildInsertLogPath("a", 0, 0, 1, 1, 802),
					},
				},
			},
		}
		err = svr.meta.AddSegment(NewSegmentInfo(seg1))
		assert.NoError(t, err)
		err = svr.meta.AddSegment(NewSegmentInfo(seg2))
		assert.NoError(t, err)
		err = svr.meta.AddSegmentIndex(&model.SegmentIndex{
			SegmentID: seg1.ID,
			BuildID:   seg1.ID,
		})
		assert.NoError(t, err)
		err = svr.meta.FinishTask(&indexpb.IndexTaskInfo{
			BuildID: seg1.ID,
			State:   commonpb.IndexState_Finished,
		})
		assert.NoError(t, err)
		err = svr.meta.AddSegmentIndex(&model.SegmentIndex{
			SegmentID: seg2.ID,
			BuildID:   seg2.ID,
		})
		assert.NoError(t, err)
		err = svr.meta.FinishTask(&indexpb.IndexTaskInfo{
			BuildID: seg2.ID,
			State:   commonpb.IndexState_Finished,
		})
		assert.NoError(t, err)

		req := &datapb.GetRecoveryInfoRequest{
			CollectionID: 0,
			PartitionID:  0,
		}
		resp, err := svr.GetRecoveryInfo(context.TODO(), req)
		assert.NoError(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.EqualValues(t, 1, len(resp.GetChannels()))
		assert.EqualValues(t, 0, len(resp.GetChannels()[0].GetUnflushedSegmentIds()))
		assert.ElementsMatch(t, []int64{0, 1}, resp.GetChannels()[0].GetFlushedSegmentIds())
		assert.EqualValues(t, 10, resp.GetChannels()[0].GetSeekPosition().GetTimestamp())
		assert.EqualValues(t, 2, len(resp.GetBinlogs()))
		// Row count corrected from 100 + 100 -> 100 + 60.
		assert.EqualValues(t, 160, resp.GetBinlogs()[0].GetNumOfRows()+resp.GetBinlogs()[1].GetNumOfRows())
	})

	t.Run("test get recovery of unflushed segments ", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)

		svr.rootCoordClientCreator = func(ctx context.Context, metaRootPath string, etcdCli *clientv3.Client) (types.RootCoord, error) {
			return newMockRootCoordService(), nil
		}

		svr.meta.AddCollection(&collectionInfo{
			ID:     0,
			Schema: newTestSchema(),
		})

		err := svr.meta.UpdateChannelCheckpoint("vchan1", &msgpb.MsgPosition{
			ChannelName: "vchan1",
			Timestamp:   0,
		})
		assert.NoError(t, err)

		seg1 := createSegment(3, 0, 0, 100, 30, "vchan1", commonpb.SegmentState_Growing)
		seg1.Binlogs = []*datapb.FieldBinlog{
			{
				FieldID: 1,
				Binlogs: []*datapb.Binlog{
					{
						EntriesNum: 20,
						LogPath:    metautil.BuildInsertLogPath("a", 0, 0, 3, 1, 901),
					},
					{
						EntriesNum: 20,
						LogPath:    metautil.BuildInsertLogPath("a", 0, 0, 3, 1, 902),
					},
					{
						EntriesNum: 20,
						LogPath:    metautil.BuildInsertLogPath("a", 0, 0, 3, 1, 903),
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
						LogPath:    metautil.BuildInsertLogPath("a", 0, 0, 4, 1, 801),
					},
					{
						EntriesNum: 70,
						LogPath:    metautil.BuildInsertLogPath("a", 0, 0, 4, 1, 802),
					},
				},
			},
		}
		err = svr.meta.AddSegment(NewSegmentInfo(seg1))
		assert.NoError(t, err)
		err = svr.meta.AddSegment(NewSegmentInfo(seg2))
		assert.NoError(t, err)
		//svr.indexCoord.(*mocks.MockIndexCoord).EXPECT().GetIndexInfos(mock.Anything, mock.Anything).Return(nil, nil)

		req := &datapb.GetRecoveryInfoRequest{
			CollectionID: 0,
			PartitionID:  0,
		}
		resp, err := svr.GetRecoveryInfo(context.TODO(), req)
		assert.NoError(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.EqualValues(t, 0, len(resp.GetBinlogs()))
		assert.EqualValues(t, 1, len(resp.GetChannels()))
		assert.NotNil(t, resp.GetChannels()[0].SeekPosition)
		assert.NotEqual(t, 0, resp.GetChannels()[0].GetSeekPosition().GetTimestamp())
	})

	t.Run("test get binlogs", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)

		svr.meta.AddCollection(&collectionInfo{
			Schema: newTestSchema(),
		})

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
		segment := createSegment(0, 0, 1, 100, 10, "vchan1", commonpb.SegmentState_Flushed)
		err := svr.meta.AddSegment(NewSegmentInfo(segment))
		assert.NoError(t, err)

		err = svr.meta.CreateIndex(&model.Index{
			TenantID:     "",
			CollectionID: 0,
			FieldID:      2,
			IndexID:      0,
			IndexName:    "",
		})
		assert.NoError(t, err)
		err = svr.meta.AddSegmentIndex(&model.SegmentIndex{
			SegmentID: segment.ID,
			BuildID:   segment.ID,
		})
		assert.NoError(t, err)
		err = svr.meta.FinishTask(&indexpb.IndexTaskInfo{
			BuildID: segment.ID,
			State:   commonpb.IndexState_Finished,
		})
		assert.NoError(t, err)

		err = svr.channelManager.AddNode(0)
		assert.NoError(t, err)
		err = svr.channelManager.Watch(&channel{Name: "vchan1", CollectionID: 0})
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

		svr.meta.AddCollection(&collectionInfo{
			ID:     0,
			Schema: newTestSchema(),
		})

		err := svr.meta.UpdateChannelCheckpoint("vchan1", &msgpb.MsgPosition{
			ChannelName: "vchan1",
			Timestamp:   0,
		})
		assert.NoError(t, err)

		seg1 := createSegment(7, 0, 0, 100, 30, "vchan1", commonpb.SegmentState_Growing)
		seg2 := createSegment(8, 0, 0, 100, 40, "vchan1", commonpb.SegmentState_Dropped)
		err = svr.meta.AddSegment(NewSegmentInfo(seg1))
		assert.NoError(t, err)
		err = svr.meta.AddSegment(NewSegmentInfo(seg2))
		assert.NoError(t, err)

		req := &datapb.GetRecoveryInfoRequest{
			CollectionID: 0,
			PartitionID:  0,
		}
		resp, err := svr.GetRecoveryInfo(context.TODO(), req)
		assert.NoError(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.EqualValues(t, 0, len(resp.GetBinlogs()))
		assert.EqualValues(t, 1, len(resp.GetChannels()))
		assert.NotNil(t, resp.GetChannels()[0].SeekPosition)
		assert.NotEqual(t, 0, resp.GetChannels()[0].GetSeekPosition().GetTimestamp())
		assert.Len(t, resp.GetChannels()[0].GetDroppedSegmentIds(), 1)
		assert.Equal(t, UniqueID(8), resp.GetChannels()[0].GetDroppedSegmentIds()[0])
	})

	t.Run("with fake segments", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)

		svr.rootCoordClientCreator = func(ctx context.Context, metaRootPath string, etcdCli *clientv3.Client) (types.RootCoord, error) {
			return newMockRootCoordService(), nil
		}

		svr.meta.AddCollection(&collectionInfo{
			ID:     0,
			Schema: newTestSchema(),
		})

		err := svr.meta.UpdateChannelCheckpoint("vchan1", &msgpb.MsgPosition{
			ChannelName: "vchan1",
			Timestamp:   0,
		})
		require.NoError(t, err)

		seg1 := createSegment(7, 0, 0, 100, 30, "vchan1", commonpb.SegmentState_Growing)
		seg2 := createSegment(8, 0, 0, 100, 40, "vchan1", commonpb.SegmentState_Flushed)
		seg2.IsFake = true
		err = svr.meta.AddSegment(NewSegmentInfo(seg1))
		assert.NoError(t, err)
		err = svr.meta.AddSegment(NewSegmentInfo(seg2))
		assert.NoError(t, err)

		req := &datapb.GetRecoveryInfoRequest{
			CollectionID: 0,
			PartitionID:  0,
		}
		resp, err := svr.GetRecoveryInfo(context.TODO(), req)
		assert.NoError(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.EqualValues(t, 0, len(resp.GetBinlogs()))
		assert.EqualValues(t, 1, len(resp.GetChannels()))
		assert.NotNil(t, resp.GetChannels()[0].SeekPosition)
		assert.NotEqual(t, 0, resp.GetChannels()[0].GetSeekPosition().GetTimestamp())
	})

	t.Run("with continuous compaction", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)

		svr.rootCoordClientCreator = func(ctx context.Context, metaRootPath string, etcdCli *clientv3.Client) (types.RootCoord, error) {
			return newMockRootCoordService(), nil
		}

		svr.meta.AddCollection(&collectionInfo{
			ID:     0,
			Schema: newTestSchema(),
		})

		err := svr.meta.UpdateChannelCheckpoint("vchan1", &msgpb.MsgPosition{
			ChannelName: "vchan1",
			Timestamp:   0,
		})
		assert.NoError(t, err)

		seg1 := createSegment(9, 0, 0, 100, 30, "vchan1", commonpb.SegmentState_Dropped)
		seg2 := createSegment(10, 0, 0, 100, 40, "vchan1", commonpb.SegmentState_Dropped)
		seg3 := createSegment(11, 0, 0, 100, 40, "vchan1", commonpb.SegmentState_Dropped)
		seg3.CompactionFrom = []int64{9, 10}
		seg4 := createSegment(12, 0, 0, 100, 40, "vchan1", commonpb.SegmentState_Dropped)
		seg5 := createSegment(13, 0, 0, 100, 40, "vchan1", commonpb.SegmentState_Flushed)
		seg5.CompactionFrom = []int64{11, 12}
		err = svr.meta.AddSegment(NewSegmentInfo(seg1))
		assert.NoError(t, err)
		err = svr.meta.AddSegment(NewSegmentInfo(seg2))
		assert.NoError(t, err)
		err = svr.meta.AddSegment(NewSegmentInfo(seg3))
		assert.NoError(t, err)
		err = svr.meta.AddSegment(NewSegmentInfo(seg4))
		assert.NoError(t, err)
		err = svr.meta.AddSegment(NewSegmentInfo(seg5))
		assert.NoError(t, err)
		err = svr.meta.CreateIndex(&model.Index{
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
		svr.meta.segments.SetSegmentIndex(seg4.ID, &model.SegmentIndex{
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
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.NotNil(t, resp.GetChannels()[0].SeekPosition)
		assert.NotEqual(t, 0, resp.GetChannels()[0].GetSeekPosition().GetTimestamp())
		assert.Len(t, resp.GetChannels()[0].GetDroppedSegmentIds(), 0)
		assert.ElementsMatch(t, []UniqueID{9, 10}, resp.GetChannels()[0].GetUnflushedSegmentIds())
		assert.ElementsMatch(t, []UniqueID{12}, resp.GetChannels()[0].GetFlushedSegmentIds())
	})

	t.Run("with closed server", func(t *testing.T) {
		svr := newTestServer(t, nil)
		closeTestServer(t, svr)
		resp, err := svr.GetRecoveryInfo(context.TODO(), &datapb.GetRecoveryInfoRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
		assert.Equal(t, serverNotServingErrMsg, resp.GetStatus().GetReason())
	})
}

func TestGetCompactionState(t *testing.T) {
	paramtable.Get().Save(Params.DataCoordCfg.EnableCompaction.Key, "true")
	defer paramtable.Get().Reset(Params.DataCoordCfg.EnableCompaction.Key)
	t.Run("test get compaction state with new compactionhandler", func(t *testing.T) {
		svr := &Server{}
		svr.stateCode.Store(commonpb.StateCode_Healthy)

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
		svr.stateCode.Store(commonpb.StateCode_Healthy)

		svr.compactionHandler = &mockCompactionHandler{
			methods: map[string]interface{}{
				"getCompactionTasksBySignalID": func(signalID int64) []*compactionTask {
					return []*compactionTask{
						{state: executing},
						{state: executing},
						{state: executing},
						{state: completed},
						{state: completed},
						{state: failed, plan: &datapb.CompactionPlan{PlanID: 1}},
						{state: timeout, plan: &datapb.CompactionPlan{PlanID: 2}},
						{state: timeout},
						{state: timeout},
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
		assert.EqualValues(t, 1, resp.GetFailedPlanNo())
		assert.EqualValues(t, 4, resp.GetTimeoutPlanNo())
	})

	t.Run("with closed server", func(t *testing.T) {
		svr := &Server{}
		svr.stateCode.Store(commonpb.StateCode_Abnormal)

		resp, err := svr.GetCompactionState(context.Background(), &milvuspb.GetCompactionStateRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
		assert.Equal(t, msgDataCoordIsUnhealthy(paramtable.GetNodeID()), resp.GetStatus().GetReason())
	})
}

func TestManualCompaction(t *testing.T) {
	paramtable.Get().Save(Params.DataCoordCfg.EnableCompaction.Key, "true")
	defer paramtable.Get().Reset(Params.DataCoordCfg.EnableCompaction.Key)
	t.Run("test manual compaction successfully", func(t *testing.T) {
		svr := &Server{allocator: &MockAllocator{}}
		svr.stateCode.Store(commonpb.StateCode_Healthy)
		svr.compactionTrigger = &mockCompactionTrigger{
			methods: map[string]interface{}{
				"forceTriggerCompaction": func(collectionID int64) (UniqueID, error) {
					return 1, nil
				},
			},
		}

		resp, err := svr.ManualCompaction(context.TODO(), &milvuspb.ManualCompactionRequest{
			CollectionID: 1,
			Timetravel:   1,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("test manual compaction failure", func(t *testing.T) {
		svr := &Server{allocator: &MockAllocator{}}
		svr.stateCode.Store(commonpb.StateCode_Healthy)
		svr.compactionTrigger = &mockCompactionTrigger{
			methods: map[string]interface{}{
				"forceTriggerCompaction": func(collectionID int64) (UniqueID, error) {
					return 0, errors.New("mock error")
				},
			},
		}

		resp, err := svr.ManualCompaction(context.TODO(), &milvuspb.ManualCompactionRequest{
			CollectionID: 1,
			Timetravel:   1,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.Status.ErrorCode)
	})

	t.Run("test manual compaction with closed server", func(t *testing.T) {
		svr := &Server{}
		svr.stateCode.Store(commonpb.StateCode_Abnormal)
		svr.compactionTrigger = &mockCompactionTrigger{
			methods: map[string]interface{}{
				"forceTriggerCompaction": func(collectionID int64) (UniqueID, error) {
					return 1, nil
				},
			},
		}

		resp, err := svr.ManualCompaction(context.TODO(), &milvuspb.ManualCompactionRequest{
			CollectionID: 1,
			Timetravel:   1,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.Status.ErrorCode)
		assert.Equal(t, msgDataCoordIsUnhealthy(paramtable.GetNodeID()), resp.Status.Reason)
	})
}

func TestGetCompactionStateWithPlans(t *testing.T) {
	t.Run("test get compaction state successfully", func(t *testing.T) {
		svr := &Server{}
		svr.stateCode.Store(commonpb.StateCode_Healthy)

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
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.Equal(t, commonpb.CompactionState_Executing, resp.State)
	})

	t.Run("test get compaction state with closed server", func(t *testing.T) {
		svr := &Server{}
		svr.stateCode.Store(commonpb.StateCode_Abnormal)
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
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.Status.ErrorCode)
		assert.Equal(t, msgDataCoordIsUnhealthy(paramtable.GetNodeID()), resp.Status.Reason)
	})
}

func TestOptions(t *testing.T) {
	kv := getWatchKV(t)
	defer func() {
		kv.RemoveWithPrefix("")
		kv.Close()
	}()

	t.Run("WithRootCoordCreator", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)
		var crt rootCoordCreatorFunc = func(ctx context.Context, metaRoot string, etcdClient *clientv3.Client) (types.RootCoord, error) {
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

		sessionManager := NewSessionManager()
		channelManager, err := NewChannelManager(kv, newMockHandler())
		assert.NoError(t, err)

		cluster := NewCluster(sessionManager, channelManager)
		assert.NoError(t, err)
		opt := WithCluster(cluster)
		assert.NotNil(t, opt)
		svr := newTestServer(t, nil, opt)
		defer closeTestServer(t, svr)

		assert.Same(t, cluster, svr.cluster)
	})
	t.Run("WithDataNodeCreator", func(t *testing.T) {
		var target int64
		var val = rand.Int63()
		opt := WithDataNodeCreator(func(context.Context, string) (types.DataNode, error) {
			target = val
			return nil, nil
		})
		assert.NotNil(t, opt)

		factory := dependency.NewDefaultFactory(true)

		svr := CreateServer(context.TODO(), factory, opt)
		dn, err := svr.dataNodeCreator(context.Background(), "")
		assert.Nil(t, dn)
		assert.NoError(t, err)
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
	kv := getWatchKV(t)
	defer func() {
		kv.RemoveWithPrefix("")
		kv.Close()
	}()
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	channelManager, err := NewChannelManager(kv, newMockHandler(), withFactory(&mockPolicyFactory{}))
	assert.NoError(t, err)
	sessionManager := NewSessionManager()
	cluster := NewCluster(sessionManager, channelManager)
	assert.NoError(t, err)

	err = cluster.Startup(ctx, nil)
	assert.NoError(t, err)
	defer cluster.Close()

	svr := newTestServer(t, nil, WithCluster(cluster))
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
		err = svr.handleSessionEvent(context.Background(), typeutil.DataNodeRole, evt)
		assert.NoError(t, err)

		evt = &sessionutil.SessionEvent{
			EventType: sessionutil.SessionAddEvent,
			Session: &sessionutil.Session{
				ServerID:   101,
				ServerName: "DN101",
				Address:    "DN127.0.0.101",
				Exclusive:  false,
			},
		}
		err = svr.handleSessionEvent(context.Background(), typeutil.DataNodeRole, evt)
		assert.NoError(t, err)
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
	mockRootCoordService
	flag bool
}

// SegmentFlushCompleted, override default behavior
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
		assert.ErrorIs(t, err, merr.ErrSegmentNotFound)
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

		assert.NoError(t, err)

		err = svr.postFlush(context.Background(), 1)
		assert.NoError(t, err)
	})
}

func TestGetFlushState(t *testing.T) {
	t.Run("get flush state with all flushed segments", func(t *testing.T) {
		svr := &Server{
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
		svr.stateCode.Store(commonpb.StateCode_Healthy)
		resp, err := svr.GetFlushState(context.TODO(), &milvuspb.GetFlushStateRequest{SegmentIDs: []int64{1, 2}})
		assert.NoError(t, err)
		assert.EqualValues(t, &milvuspb.GetFlushStateResponse{
			Status:  &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			Flushed: true,
		}, resp)
	})

	t.Run("get flush state with unflushed segments", func(t *testing.T) {
		svr := &Server{
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
		svr.stateCode.Store(commonpb.StateCode_Healthy)

		resp, err := svr.GetFlushState(context.TODO(), &milvuspb.GetFlushStateRequest{SegmentIDs: []int64{1, 2}})
		assert.NoError(t, err)
		assert.EqualValues(t, &milvuspb.GetFlushStateResponse{
			Status:  &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			Flushed: false,
		}, resp)
	})

	t.Run("get flush state with compacted segments", func(t *testing.T) {
		svr := &Server{
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
		svr.stateCode.Store(commonpb.StateCode_Healthy)

		resp, err := svr.GetFlushState(context.TODO(), &milvuspb.GetFlushStateRequest{SegmentIDs: []int64{1, 2}})
		assert.NoError(t, err)
		assert.EqualValues(t, &milvuspb.GetFlushStateResponse{
			Status:  &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			Flushed: true,
		}, resp)
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
		{"test FlushAll flushed", []Timestamp{100, 200}, 99,
			true, false, false, false, true, true},
		{"test FlushAll not flushed", []Timestamp{100, 200}, 150,
			true, false, false, false, true, false},
		{"test Sever is not healthy", nil, 0,
			false, false, false, false, false, false},
		{"test ListDatabase failed", nil, 0,
			true, true, false, false, false, false},
		{"test ShowCollections failed", nil, 0,
			true, false, true, false, false, false},
		{"test DescribeCollection failed", nil, 0,
			true, false, false, true, false, false},
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
			svr.rootCoordClient = mocks.NewRootCoord(t)
			if test.ListDatabaseFailed {
				svr.rootCoordClient.(*mocks.RootCoord).EXPECT().ListDatabases(mock.Anything, mock.Anything).
					Return(&milvuspb.ListDatabasesResponse{
						Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError},
					}, nil).Maybe()
			} else {
				svr.rootCoordClient.(*mocks.RootCoord).EXPECT().ListDatabases(mock.Anything, mock.Anything).
					Return(&milvuspb.ListDatabasesResponse{
						DbNames: []string{"db1"},
						Status:  &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
					}, nil).Maybe()
			}

			if test.ShowCollectionFailed {
				svr.rootCoordClient.(*mocks.RootCoord).EXPECT().ShowCollections(mock.Anything, mock.Anything).
					Return(&milvuspb.ShowCollectionsResponse{
						Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError},
					}, nil).Maybe()
			} else {
				svr.rootCoordClient.(*mocks.RootCoord).EXPECT().ShowCollections(mock.Anything, mock.Anything).
					Return(&milvuspb.ShowCollectionsResponse{
						Status:        &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
						CollectionIds: []int64{collection},
					}, nil).Maybe()
			}

			if test.DescribeCollectionFailed {
				svr.rootCoordClient.(*mocks.RootCoord).EXPECT().DescribeCollectionInternal(mock.Anything, mock.Anything).
					Return(&milvuspb.DescribeCollectionResponse{
						Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError},
					}, nil).Maybe()
			} else {
				svr.rootCoordClient.(*mocks.RootCoord).EXPECT().DescribeCollectionInternal(mock.Anything, mock.Anything).
					Return(&milvuspb.DescribeCollectionResponse{
						Status:              &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
						VirtualChannelNames: vchannels,
					}, nil).Maybe()
			}

			svr.meta.channelCPs = make(map[string]*msgpb.MsgPosition)
			for i, ts := range test.ChannelCPs {
				channel := vchannels[i]
				svr.meta.channelCPs[channel] = &msgpb.MsgPosition{
					ChannelName: channel,
					Timestamp:   ts,
				}
			}

			resp, err := svr.GetFlushAllState(context.TODO(), &milvuspb.GetFlushAllStateRequest{FlushAllTs: test.FlushAllTs})
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
		svr := newTestServer(t, nil)
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
		err := svr.meta.AddSegment(NewSegmentInfo(segment))
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
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.EqualValues(t, 1, len(resp.States))
		assert.EqualValues(t, commonpb.SegmentState_Flushed, resp.States[0].State)
	})

	t.Run("dataCoord meta set state not exists", func(t *testing.T) {
		meta, err := newMemoryMeta()
		assert.NoError(t, err)
		svr := newTestServerWithMeta(t, nil, meta)
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
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.EqualValues(t, 1, len(resp.States))
		assert.EqualValues(t, commonpb.SegmentState_NotExist, resp.States[0].State)
	})

	t.Run("with closed server", func(t *testing.T) {
		svr := newTestServer(t, nil)
		closeTestServer(t, svr)
		resp, err := svr.SetSegmentState(context.TODO(), &datapb.SetSegmentStateRequest{
			SegmentId: 1000,
			NewState:  commonpb.SegmentState_Flushed,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
		assert.Equal(t, serverNotServingErrMsg, resp.GetStatus().GetReason())
	})
}

func TestDataCoord_Import(t *testing.T) {
	storage.CheckBucketRetryAttempts = 2

	t.Run("normal case", func(t *testing.T) {
		svr := newTestServer(t, nil)
		svr.sessionManager.AddSession(&NodeInfo{
			NodeID:  0,
			Address: "localhost:8080",
		})
		err := svr.channelManager.AddNode(0)
		assert.NoError(t, err)
		err = svr.channelManager.Watch(&channel{Name: "ch1", CollectionID: 0})
		assert.NoError(t, err)

		resp, err := svr.Import(svr.ctx, &datapb.ImportTaskRequest{
			ImportTask: &datapb.ImportTask{
				CollectionId: 100,
				PartitionId:  100,
			},
		})
		assert.NoError(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.GetErrorCode())
		closeTestServer(t, svr)
	})

	t.Run("no free node", func(t *testing.T) {
		svr := newTestServer(t, nil)

		err := svr.channelManager.AddNode(0)
		assert.NoError(t, err)
		err = svr.channelManager.Watch(&channel{Name: "ch1", CollectionID: 0})
		assert.NoError(t, err)

		resp, err := svr.Import(svr.ctx, &datapb.ImportTaskRequest{
			ImportTask: &datapb.ImportTask{
				CollectionId: 100,
				PartitionId:  100,
			},
			WorkingNodes: []int64{0},
		})
		assert.NoError(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_UnexpectedError, resp.Status.GetErrorCode())
		closeTestServer(t, svr)
	})

	t.Run("no datanode available", func(t *testing.T) {
		svr := newTestServer(t, nil)
		Params.BaseTable.Save("minio.address", "minio:9000")
		defer Params.BaseTable.Reset("minio.address")
		resp, err := svr.Import(svr.ctx, &datapb.ImportTaskRequest{
			ImportTask: &datapb.ImportTask{
				CollectionId: 100,
				PartitionId:  100,
			},
		})
		assert.NoError(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_UnexpectedError, resp.Status.GetErrorCode())
		closeTestServer(t, svr)
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
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.Status.GetErrorCode())
		assert.Equal(t, msgDataCoordIsUnhealthy(paramtable.GetNodeID()), resp.Status.GetReason())
	})

	t.Run("test update segment stat", func(t *testing.T) {
		svr := newTestServer(t, nil)

		status, err := svr.UpdateSegmentStatistics(context.TODO(), &datapb.UpdateSegmentStatisticsRequest{
			Stats: []*commonpb.SegmentStats{{
				SegmentID: 100,
				NumRows:   int64(1),
			}},
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, status.GetErrorCode())
		closeTestServer(t, svr)
	})

	t.Run("test update segment stat w/ closed server", func(t *testing.T) {
		svr := newTestServer(t, nil)
		closeTestServer(t, svr)

		status, err := svr.UpdateSegmentStatistics(context.TODO(), &datapb.UpdateSegmentStatisticsRequest{
			Stats: []*commonpb.SegmentStats{{
				SegmentID: 100,
				NumRows:   int64(1),
			}},
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.GetErrorCode())
	})
}

func TestDataCoord_SegmentStatistics(t *testing.T) {
	t.Run("test update imported segment stat", func(t *testing.T) {
		svr := newTestServer(t, nil)

		seg1 := &datapb.SegmentInfo{
			ID:        100,
			Binlogs:   []*datapb.FieldBinlog{getFieldBinlogPathsWithEntry(101, 1, getInsertLogPath("log1", 100))},
			Statslogs: []*datapb.FieldBinlog{getFieldBinlogPaths(101, getStatsLogPath("log2", 100))},
			Deltalogs: []*datapb.FieldBinlog{getFieldBinlogPaths(101, getDeltaLogPath("log3", 100))},
			State:     commonpb.SegmentState_Importing,
		}

		info := NewSegmentInfo(seg1)
		svr.meta.AddSegment(info)

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
		svr := newTestServer(t, nil)

		seg1 := &datapb.SegmentInfo{
			ID:        100,
			Binlogs:   []*datapb.FieldBinlog{getFieldBinlogPathsWithEntry(101, 1, getInsertLogPath("log1", 100))},
			Statslogs: []*datapb.FieldBinlog{getFieldBinlogPaths(101, getStatsLogPath("log2", 100))},
			Deltalogs: []*datapb.FieldBinlog{getFieldBinlogPaths(101, getDeltaLogPath("log3", 100))},
			State:     commonpb.SegmentState_Flushed,
		}

		info := NewSegmentInfo(seg1)
		svr.meta.AddSegment(info)

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

func TestDataCoord_SaveImportSegment(t *testing.T) {
	t.Run("test add segment", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)
		svr.meta.AddCollection(&collectionInfo{
			ID: 100,
		})
		seg := buildSegment(100, 100, 100, "ch1", false)
		svr.meta.AddSegment(seg)
		svr.sessionManager.AddSession(&NodeInfo{
			NodeID:  110,
			Address: "localhost:8080",
		})
		err := svr.channelManager.AddNode(110)
		assert.NoError(t, err)
		err = svr.channelManager.Watch(&channel{Name: "ch1", CollectionID: 100})
		assert.NoError(t, err)

		status, err := svr.SaveImportSegment(context.TODO(), &datapb.SaveImportSegmentRequest{
			SegmentId:    100,
			ChannelName:  "ch1",
			CollectionId: 100,
			PartitionId:  100,
			RowNum:       int64(1),
			SaveBinlogPathReq: &datapb.SaveBinlogPathsRequest{
				Base: &commonpb.MsgBase{
					SourceID: paramtable.GetNodeID(),
				},
				SegmentID:    100,
				CollectionID: 100,
				Importing:    true,
				StartPositions: []*datapb.SegmentStartPosition{
					{
						StartPosition: &msgpb.MsgPosition{
							ChannelName: "ch1",
							Timestamp:   1,
						},
						SegmentID: 100,
					},
				},
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, status.GetErrorCode())
	})

	t.Run("test add segment w/ bad channel name", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)

		err := svr.channelManager.AddNode(110)
		assert.NoError(t, err)
		err = svr.channelManager.Watch(&channel{Name: "ch1", CollectionID: 100})
		assert.NoError(t, err)

		status, err := svr.SaveImportSegment(context.TODO(), &datapb.SaveImportSegmentRequest{
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

		status, err := svr.SaveImportSegment(context.TODO(), &datapb.SaveImportSegmentRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_DataCoordNA, status.GetErrorCode())
	})
}

func TestDataCoord_UnsetIsImportingState(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)
		seg := buildSegment(100, 100, 100, "ch1", false)
		svr.meta.AddSegment(seg)

		status, err := svr.UnsetIsImportingState(context.Background(), &datapb.UnsetIsImportingStateRequest{
			SegmentIds: []int64{100},
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, status.GetErrorCode())

		// Trying to unset state of a segment that does not exist.
		status, err = svr.UnsetIsImportingState(context.Background(), &datapb.UnsetIsImportingStateRequest{
			SegmentIds: []int64{999},
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.GetErrorCode())
	})
}

func TestDataCoordServer_UpdateChannelCheckpoint(t *testing.T) {
	mockVChannel := "fake-by-dev-rootcoord-dml-1-testchannelcp-v0"
	mockPChannel := "fake-by-dev-rootcoord-dml-1"

	t.Run("UpdateChannelCheckpoint", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)

		req := &datapb.UpdateChannelCheckpointRequest{
			Base: &commonpb.MsgBase{
				SourceID: paramtable.GetNodeID(),
			},
			VChannel: mockVChannel,
			Position: &msgpb.MsgPosition{
				ChannelName: mockPChannel,
				Timestamp:   1000,
			},
		}

		resp, err := svr.UpdateChannelCheckpoint(context.TODO(), req)
		assert.NoError(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		req.Position = nil
		resp, err = svr.UpdateChannelCheckpoint(context.TODO(), req)
		assert.NoError(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_UnexpectedError, resp.ErrorCode)
	})
}

func newTestServer(t *testing.T, receiveCh chan any, opts ...Option) *Server {
	var err error
	paramtable.Get().Save(Params.CommonCfg.DataCoordTimeTick.Key, Params.CommonCfg.DataCoordTimeTick.GetValue()+strconv.Itoa(rand.Int()))
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
	svr.dataNodeCreator = func(ctx context.Context, addr string) (types.DataNode, error) {
		return newMockDataNodeClient(0, receiveCh)
	}
	svr.rootCoordClientCreator = func(ctx context.Context, metaRootPath string, etcdCli *clientv3.Client) (types.RootCoord, error) {
		return newMockRootCoordService(), nil
	}

	for _, opt := range opts {
		opt(svr)
	}

	err = svr.Init()
	assert.NoError(t, err)
	if Params.DataCoordCfg.EnableActiveStandby.GetAsBool() {
		assert.Equal(t, commonpb.StateCode_StandBy, svr.stateCode.Load().(commonpb.StateCode))
	} else {
		assert.Equal(t, commonpb.StateCode_Initializing, svr.stateCode.Load().(commonpb.StateCode))
	}
	err = svr.Register()
	assert.NoError(t, err)
	err = svr.Start()
	assert.NoError(t, err)
	assert.Equal(t, commonpb.StateCode_Healthy, svr.stateCode.Load().(commonpb.StateCode))

	// Stop channal watch state watcher in tests
	if svr.channelManager != nil && svr.channelManager.stopChecker != nil {
		svr.channelManager.stopChecker()
	}

	return svr
}

func newTestServerWithMeta(t *testing.T, receiveCh chan any, meta *meta, opts ...Option) *Server {
	var err error
	paramtable.Get().Save(Params.CommonCfg.DataCoordTimeTick.Key, Params.CommonCfg.DataCoordTimeTick.GetValue()+strconv.Itoa(rand.Int()))
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

	svr := CreateServer(context.TODO(), factory, opts...)
	svr.SetEtcdClient(etcdCli)
	svr.dataNodeCreator = func(ctx context.Context, addr string) (types.DataNode, error) {
		return newMockDataNodeClient(0, receiveCh)
	}
	svr.rootCoordClientCreator = func(ctx context.Context, metaRootPath string, etcdCli *clientv3.Client) (types.RootCoord, error) {
		return newMockRootCoordService(), nil
	}
	//indexCoord := mocks.NewMockIndexCoord(t)
	//indexCoord.EXPECT().GetIndexInfos(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	//svr.indexCoord = indexCoord

	err = svr.Init()
	assert.NoError(t, err)
	svr.meta = meta

	err = svr.Start()
	assert.NoError(t, err)
	err = svr.Register()
	assert.NoError(t, err)

	// Stop channal watch state watcher in tests
	if svr.channelManager != nil && svr.channelManager.stopChecker != nil {
		svr.channelManager.stopChecker()
	}

	return svr
}

func closeTestServer(t *testing.T, svr *Server) {
	err := svr.Stop()
	assert.NoError(t, err)
	err = svr.CleanMeta()
	assert.NoError(t, err)
}

func newTestServer2(t *testing.T, receiveCh chan any, opts ...Option) *Server {
	var err error
	paramtable.Init()
	paramtable.Get().Save(Params.CommonCfg.DataCoordTimeTick.Key, Params.CommonCfg.DataCoordTimeTick.GetValue()+strconv.Itoa(rand.Int()))
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

	svr := CreateServer(context.TODO(), factory, opts...)
	svr.SetEtcdClient(etcdCli)
	svr.dataNodeCreator = func(ctx context.Context, addr string) (types.DataNode, error) {
		return newMockDataNodeClient(0, receiveCh)
	}
	svr.rootCoordClientCreator = func(ctx context.Context, metaRootPath string, etcdCli *clientv3.Client) (types.RootCoord, error) {
		return newMockRootCoordService(), nil
	}

	err = svr.Init()
	assert.NoError(t, err)
	err = svr.Start()
	assert.NoError(t, err)
	err = svr.Register()
	assert.NoError(t, err)

	// Stop channal watch state watcher in tests
	if svr.channelManager != nil && svr.channelManager.stopChecker != nil {
		svr.channelManager.stopChecker()
	}

	return svr
}

func Test_CheckHealth(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		ctx := context.Background()
		s := &Server{session: &sessionutil.Session{ServerID: 1}}
		s.stateCode.Store(commonpb.StateCode_Abnormal)
		resp, err := s.CheckHealth(ctx, &milvuspb.CheckHealthRequest{})
		assert.NoError(t, err)
		assert.Equal(t, false, resp.IsHealthy)
		assert.NotEmpty(t, resp.Reasons)
	})

	t.Run("data node health check is ok", func(t *testing.T) {
		svr := &Server{session: &sessionutil.Session{ServerID: 1}}
		svr.stateCode.Store(commonpb.StateCode_Healthy)
		healthClient := &mockDataNodeClient{
			id:    1,
			state: commonpb.StateCode_Healthy}
		sm := NewSessionManager()
		sm.sessions = struct {
			sync.RWMutex
			data map[int64]*Session
		}{data: map[int64]*Session{1: {
			client: healthClient,
			clientCreator: func(ctx context.Context, addr string) (types.DataNode, error) {
				return healthClient, nil
			},
		}}}

		svr.sessionManager = sm
		ctx := context.Background()
		resp, err := svr.CheckHealth(ctx, &milvuspb.CheckHealthRequest{})
		assert.NoError(t, err)
		assert.Equal(t, true, resp.IsHealthy)
		assert.Empty(t, resp.Reasons)
	})

	t.Run("data node health check is fail", func(t *testing.T) {
		svr := &Server{session: &sessionutil.Session{ServerID: 1}}
		svr.stateCode.Store(commonpb.StateCode_Healthy)
		unhealthClient := &mockDataNodeClient{
			id:    1,
			state: commonpb.StateCode_Abnormal}
		sm := NewSessionManager()
		sm.sessions = struct {
			sync.RWMutex
			data map[int64]*Session
		}{data: map[int64]*Session{1: {
			client: unhealthClient,
			clientCreator: func(ctx context.Context, addr string) (types.DataNode, error) {
				return unhealthClient, nil
			},
		}}}
		svr.sessionManager = sm
		ctx := context.Background()
		resp, err := svr.CheckHealth(ctx, &milvuspb.CheckHealthRequest{})
		assert.NoError(t, err)
		assert.Equal(t, false, resp.IsHealthy)
		assert.NotEmpty(t, resp.Reasons)
	})
}

//func Test_initServiceDiscovery(t *testing.T) {
//	server := newTestServer2(t, nil)
//	assert.NotNil(t, server)
//
//	segmentID := rand.Int63()
//	err := server.meta.AddSegment(&SegmentInfo{
//		SegmentInfo: &datapb.SegmentInfo{
//			ID:           segmentID,
//			CollectionID: rand.Int63(),
//			PartitionID:  rand.Int63(),
//			NumOfRows:    100,
//		},
//		currRows: 100,
//	})
//	assert.NoError(t, err)
//
//	qcSession := sessionutil.NewSession(context.Background(), Params.EtcdCfg.MetaRootPath.GetValue(), server.etcdCli)
//	qcSession.Init(typeutil.QueryCoordRole, "localhost:19532", true, true)
//	qcSession.Register()
//	//req := &datapb.AcquireSegmentLockRequest{
//	//	NodeID:     qcSession.ServerID,
//	//	SegmentIDs: []UniqueID{segmentID},
//	//}
//	//resp, err := server.AcquireSegmentLock(context.TODO(), req)
//	//assert.NoError(t, err)
//	//assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
//
//	sessKey := path.Join(Params.EtcdCfg.MetaRootPath.GetValue(), sessionutil.DefaultServiceRoot, typeutil.QueryCoordRole)
//	_, err = server.etcdCli.Delete(context.Background(), sessKey, clientv3.WithPrefix())
//	assert.NoError(t, err)
//
//	//for {
//	//	if !server.segReferManager.HasSegmentLock(segmentID) {
//	//		break
//	//	}
//	//}
//
//	closeTestServer(t, server)
//}

func Test_newChunkManagerFactory(t *testing.T) {
	server := newTestServer2(t, nil)
	paramtable.Get().Save(Params.DataCoordCfg.EnableGarbageCollection.Key, "true")
	defer closeTestServer(t, server)

	t.Run("err_minio_bad_address", func(t *testing.T) {
		paramtable.Get().Save(Params.CommonCfg.StorageType.Key, "minio")
		paramtable.Get().Save(Params.MinioCfg.Address.Key, "host:9000:bad")
		defer paramtable.Get().Reset(Params.MinioCfg.Address.Key)
		storageCli, err := server.newChunkManagerFactory()
		assert.Nil(t, storageCli)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "too many colons in address")
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

	server := newTestServer2(t, nil)
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
		assert.Contains(t, err.Error(), "too many colons in address")
	})
}

func testDataCoordBase(t *testing.T, opts ...Option) *Server {
	var err error
	paramtable.Get().Save(Params.CommonCfg.DataCoordTimeTick.Key, Params.CommonCfg.DataCoordTimeTick.GetValue()+strconv.Itoa(rand.Int()))
	factory := dependency.NewDefaultFactory(true)

	ctx := context.Background()
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
	_, err = etcdCli.Delete(ctx, sessKey, clientv3.WithPrefix())
	assert.NoError(t, err)

	svr := CreateServer(ctx, factory, opts...)
	svr.SetEtcdClient(etcdCli)
	svr.SetDataNodeCreator(func(ctx context.Context, addr string) (types.DataNode, error) {
		return newMockDataNodeClient(0, nil)
	})
	svr.SetIndexNodeCreator(func(ctx context.Context, addr string) (types.IndexNode, error) {
		return indexnode.NewMockIndexNodeComponent(ctx)
	})
	svr.SetRootCoord(newMockRootCoordService())

	err = svr.Init()
	assert.NoError(t, err)
	err = svr.Start()
	assert.NoError(t, err)
	err = svr.Register()
	assert.NoError(t, err)

	resp, err := svr.GetComponentStates(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	assert.Equal(t, commonpb.StateCode_Healthy, resp.GetState().GetStateCode())

	// stop channal watch state watcher in tests
	if svr.channelManager != nil && svr.channelManager.stopChecker != nil {
		svr.channelManager.stopChecker()
	}

	return svr
}

func TestDataCoord_DisableActiveStandby(t *testing.T) {
	paramtable.Get().Save(Params.DataCoordCfg.EnableActiveStandby.Key, "false")
	svr := testDataCoordBase(t)
	defer closeTestServer(t, svr)
}

// make sure the main functions work well when EnableActiveStandby=true
func TestDataCoord_EnableActiveStandby(t *testing.T) {
	paramtable.Get().Save(Params.DataCoordCfg.EnableActiveStandby.Key, "true")
	defer paramtable.Get().Reset(Params.DataCoordCfg.EnableActiveStandby.Key)
	svr := testDataCoordBase(t)
	defer closeTestServer(t, svr)
}
