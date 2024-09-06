package datacoord

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	mocks2 "github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/workerpb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metautil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type ServerSuite struct {
	suite.Suite

	testServer *Server
	mockChMgr  *MockChannelManager
}

func WithChannelManager(cm ChannelManager) Option {
	return func(svr *Server) {
		svr.sessionManager = session.NewDataNodeManagerImpl(session.WithDataNodeCreator(svr.dataNodeCreator))
		svr.channelManager = cm
		svr.cluster = NewClusterImpl(svr.sessionManager, svr.channelManager)
	}
}

func (s *ServerSuite) SetupTest() {
	s.mockChMgr = NewMockChannelManager(s.T())
	s.mockChMgr.EXPECT().Startup(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	s.mockChMgr.EXPECT().Close().Maybe()

	s.testServer = newTestServer(s.T(), WithChannelManager(s.mockChMgr))
	if s.testServer.channelManager != nil {
		s.testServer.channelManager.Close()
	}
}

func (s *ServerSuite) TearDownTest() {
	if s.testServer != nil {
		log.Info("ServerSuite tears down test", zap.String("name", s.T().Name()))
		closeTestServer(s.T(), s.testServer)
	}
}

func TestServerSuite(t *testing.T) {
	suite.Run(t, new(ServerSuite))
}

func genMsg(msgType commonpb.MsgType, ch string, t Timestamp, sourceID int64) *msgstream.DataNodeTtMsg {
	return &msgstream.DataNodeTtMsg{
		BaseMsg: msgstream.BaseMsg{
			HashValues: []uint32{0},
		},
		DataNodeTtMsg: &msgpb.DataNodeTtMsg{
			Base: &commonpb.MsgBase{
				MsgType:   msgType,
				Timestamp: t,
				SourceID:  sourceID,
			},
			ChannelName:   ch,
			Timestamp:     t,
			SegmentsStats: []*commonpb.SegmentStats{{SegmentID: 2, NumRows: 100}},
		},
	}
}

func (s *ServerSuite) TestGetFlushState_ByFlushTs() {
	s.mockChMgr.EXPECT().GetChannelsByCollectionID(int64(0)).
		Return([]RWChannel{&channelMeta{Name: "ch1", CollectionID: 0}}).Times(3)

	s.mockChMgr.EXPECT().GetChannelsByCollectionID(int64(1)).Return(nil).Times(1)
	tests := []struct {
		description string
		inTs        Timestamp

		expected bool
	}{
		{"channel cp > flush ts", 11, true},
		{"channel cp = flush ts", 12, true},
		{"channel cp < flush ts", 13, false},
	}

	err := s.testServer.meta.UpdateChannelCheckpoint("ch1", &msgpb.MsgPosition{
		MsgID:     []byte{1},
		Timestamp: 12,
	})
	s.Require().NoError(err)
	for _, test := range tests {
		s.Run(test.description, func() {
			resp, err := s.testServer.GetFlushState(context.TODO(), &datapb.GetFlushStateRequest{FlushTs: test.inTs})
			s.NoError(err)
			s.EqualValues(&milvuspb.GetFlushStateResponse{
				Status:  merr.Success(),
				Flushed: test.expected,
			}, resp)
		})
	}

	resp, err := s.testServer.GetFlushState(context.TODO(), &datapb.GetFlushStateRequest{CollectionID: 1, FlushTs: 13})
	s.NoError(err)
	s.EqualValues(&milvuspb.GetFlushStateResponse{
		Status:  merr.Success(),
		Flushed: true,
	}, resp)
}

func (s *ServerSuite) TestGetFlushState_BySegment() {
	s.mockChMgr.EXPECT().GetChannelsByCollectionID(mock.Anything).
		Return([]RWChannel{&channelMeta{Name: "ch1", CollectionID: 0}}).Times(3)

	tests := []struct {
		description string
		segID       int64
		state       commonpb.SegmentState

		expected bool
	}{
		{"flushed seg1", 1, commonpb.SegmentState_Flushed, true},
		{"flushed seg2", 2, commonpb.SegmentState_Flushed, true},
		{"sealed seg3", 3, commonpb.SegmentState_Sealed, false},
		{"compacted/dropped seg4", 4, commonpb.SegmentState_Dropped, true},
	}

	for _, test := range tests {
		s.Run(test.description, func() {
			err := s.testServer.meta.AddSegment(context.TODO(), &SegmentInfo{
				SegmentInfo: &datapb.SegmentInfo{
					ID:    test.segID,
					State: test.state,
				},
			})

			s.Require().NoError(err)
			err = s.testServer.meta.UpdateChannelCheckpoint("ch1", &msgpb.MsgPosition{
				MsgID:     []byte{1},
				Timestamp: 12,
			})
			s.Require().NoError(err)

			resp, err := s.testServer.GetFlushState(context.TODO(), &datapb.GetFlushStateRequest{SegmentIDs: []int64{test.segID}})
			s.NoError(err)
			s.EqualValues(&milvuspb.GetFlushStateResponse{
				Status:  merr.Success(),
				Flushed: test.expected,
			}, resp)
		})
	}
}

func (s *ServerSuite) TestSaveBinlogPath_ClosedServer() {
	s.TearDownTest()
	resp, err := s.testServer.SaveBinlogPaths(context.Background(), &datapb.SaveBinlogPathsRequest{
		SegmentID: 1,
		Channel:   "test",
	})
	s.NoError(err)
	s.ErrorIs(merr.Error(resp), merr.ErrServiceNotReady)
}

func (s *ServerSuite) TestSaveBinlogPath_ChannelNotMatch() {
	s.mockChMgr.EXPECT().Match(mock.Anything, mock.Anything).Return(false)
	resp, err := s.testServer.SaveBinlogPaths(context.Background(), &datapb.SaveBinlogPathsRequest{
		SegmentID: 1,
		Channel:   "test",
	})
	s.NoError(err)
	s.ErrorIs(merr.Error(resp), merr.ErrChannelNotFound)
}

func (s *ServerSuite) TestSaveBinlogPath_SaveUnhealthySegment() {
	s.mockChMgr.EXPECT().Match(int64(0), "ch1").Return(true)
	s.testServer.meta.AddCollection(&collectionInfo{ID: 0})

	segments := map[int64]commonpb.SegmentState{
		1: commonpb.SegmentState_NotExist,
		2: commonpb.SegmentState_Dropped,
	}
	for segID, state := range segments {
		info := &datapb.SegmentInfo{
			ID:            segID,
			InsertChannel: "ch1",
			State:         state,
		}
		err := s.testServer.meta.AddSegment(context.TODO(), NewSegmentInfo(info))
		s.Require().NoError(err)
	}

	tests := []struct {
		description string
		inSeg       int64

		expectedError error
	}{
		{"segment not exist", 1, merr.ErrSegmentNotFound},
		{"segment dropped", 2, nil},
		{"segment not in meta", 3, merr.ErrSegmentNotFound},
	}

	for _, test := range tests {
		s.Run(test.description, func() {
			ctx := context.Background()
			resp, err := s.testServer.SaveBinlogPaths(ctx, &datapb.SaveBinlogPathsRequest{
				Base: &commonpb.MsgBase{
					Timestamp: uint64(time.Now().Unix()),
				},
				SegmentID: test.inSeg,
				Channel:   "ch1",
			})
			s.NoError(err)
			s.ErrorIs(merr.Error(resp), test.expectedError)
		})
	}
}

func (s *ServerSuite) TestSaveBinlogPath_SaveDroppedSegment() {
	s.mockChMgr.EXPECT().Match(int64(0), "ch1").Return(true)
	s.testServer.meta.AddCollection(&collectionInfo{ID: 0})

	segments := map[int64]commonpb.SegmentState{
		0: commonpb.SegmentState_Flushed,
		1: commonpb.SegmentState_Sealed,
	}
	for segID, state := range segments {
		info := &datapb.SegmentInfo{
			ID:            segID,
			InsertChannel: "ch1",
			State:         state,
			Level:         datapb.SegmentLevel_L1,
		}
		err := s.testServer.meta.AddSegment(context.TODO(), NewSegmentInfo(info))
		s.Require().NoError(err)
	}

	tests := []struct {
		description string
		inSegID     int64
		inDropped   bool
		inFlushed   bool

		expectedState commonpb.SegmentState
	}{
		{"segID=0, flushed to dropped", 0, true, false, commonpb.SegmentState_Dropped},
		{"segID=1, sealed to flushing", 1, false, true, commonpb.SegmentState_Flushing},
	}

	paramtable.Get().Save(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key, "False")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key)
	for _, test := range tests {
		s.Run(test.description, func() {
			ctx := context.Background()
			resp, err := s.testServer.SaveBinlogPaths(ctx, &datapb.SaveBinlogPathsRequest{
				Base: &commonpb.MsgBase{
					Timestamp: uint64(time.Now().Unix()),
				},
				SegmentID: test.inSegID,
				Channel:   "ch1",
				Flushed:   test.inFlushed,
				Dropped:   test.inDropped,
			})
			s.NoError(err)
			s.EqualValues(resp.ErrorCode, commonpb.ErrorCode_Success)

			segment := s.testServer.meta.GetSegment(test.inSegID)
			s.NotNil(segment)
			s.EqualValues(0, len(segment.GetBinlogs()))
			s.EqualValues(segment.NumOfRows, 0)

			flushing := []commonpb.SegmentState{commonpb.SegmentState_Flushed, commonpb.SegmentState_Flushing}
			if lo.Contains(flushing, test.expectedState) {
				s.True(lo.Contains(flushing, segment.GetState()))
			} else {
				s.Equal(test.expectedState, segment.GetState())
			}
		})
	}
}

func (s *ServerSuite) TestSaveBinlogPath_L0Segment() {
	s.mockChMgr.EXPECT().Match(int64(0), "ch1").Return(true)
	s.testServer.meta.AddCollection(&collectionInfo{ID: 0})

	segment := s.testServer.meta.GetHealthySegment(1)
	s.Require().Nil(segment)
	ctx := context.Background()
	resp, err := s.testServer.SaveBinlogPaths(ctx, &datapb.SaveBinlogPathsRequest{
		Base: &commonpb.MsgBase{
			Timestamp: uint64(time.Now().Unix()),
		},
		SegmentID:    1,
		PartitionID:  1,
		CollectionID: 0,
		SegLevel:     datapb.SegmentLevel_L0,
		Channel:      "ch1",
		Deltalogs: []*datapb.FieldBinlog{
			{
				FieldID: 1,
				Binlogs: []*datapb.Binlog{
					{
						LogPath:    "/by-dev/test/0/1/1/1/1",
						EntriesNum: 5,
					},
					{
						LogPath:    "/by-dev/test/0/1/1/1/2",
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
		Flushed: true,
	})
	s.NoError(err)
	s.EqualValues(resp.ErrorCode, commonpb.ErrorCode_Success)

	segment = s.testServer.meta.GetHealthySegment(1)
	s.NotNil(segment)
	s.EqualValues(datapb.SegmentLevel_L0, segment.GetLevel())
}

func (s *ServerSuite) TestSaveBinlogPath_NormalCase() {
	s.mockChMgr.EXPECT().Match(int64(0), "ch1").Return(true)
	s.testServer.meta.AddCollection(&collectionInfo{ID: 0})

	segments := map[int64]int64{
		0: 0,
		1: 0,
	}
	for segID, collID := range segments {
		info := &datapb.SegmentInfo{
			ID:            segID,
			CollectionID:  collID,
			InsertChannel: "ch1",
			State:         commonpb.SegmentState_Growing,
		}
		err := s.testServer.meta.AddSegment(context.TODO(), NewSegmentInfo(info))
		s.Require().NoError(err)
	}

	ctx := context.Background()

	resp, err := s.testServer.SaveBinlogPaths(ctx, &datapb.SaveBinlogPathsRequest{
		Base: &commonpb.MsgBase{
			Timestamp: uint64(time.Now().Unix()),
		},
		SegmentID:    1,
		CollectionID: 0,
		Channel:      "ch1",
		Field2BinlogPaths: []*datapb.FieldBinlog{
			{
				FieldID: 1,
				Binlogs: []*datapb.Binlog{
					{
						LogPath:    "/by-dev/test/0/1/1/1/1",
						EntriesNum: 5,
					},
					{
						LogPath:    "/by-dev/test/0/1/1/1/2",
						EntriesNum: 5,
					},
				},
			},
		},
		Field2StatslogPaths: []*datapb.FieldBinlog{
			{
				FieldID: 1,
				Binlogs: []*datapb.Binlog{
					{
						LogPath:    "/by-dev/test_stats/0/1/1/1/1",
						EntriesNum: 5,
					},
					{
						LogPath:    "/by-dev/test_stats/0/1/1/1/2",
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
	s.NoError(err)
	s.EqualValues(resp.ErrorCode, commonpb.ErrorCode_Success)

	segment := s.testServer.meta.GetHealthySegment(1)
	s.NotNil(segment)
	binlogs := segment.GetBinlogs()
	s.EqualValues(1, len(binlogs))
	fieldBinlogs := binlogs[0]
	s.NotNil(fieldBinlogs)
	s.EqualValues(2, len(fieldBinlogs.GetBinlogs()))
	s.EqualValues(1, fieldBinlogs.GetFieldID())
	s.EqualValues("", fieldBinlogs.GetBinlogs()[0].GetLogPath())
	s.EqualValues(int64(1), fieldBinlogs.GetBinlogs()[0].GetLogID())
	s.EqualValues("", fieldBinlogs.GetBinlogs()[1].GetLogPath())
	s.EqualValues(int64(2), fieldBinlogs.GetBinlogs()[1].GetLogID())

	s.EqualValues(segment.DmlPosition.ChannelName, "ch1")
	s.EqualValues(segment.DmlPosition.MsgID, []byte{1, 2, 3})
	s.EqualValues(segment.NumOfRows, 10)
}

func (s *ServerSuite) TestFlush_NormalCase() {
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

	s.mockChMgr.EXPECT().GetNodeChannelsByCollectionID(mock.Anything).Return(map[int64][]string{
		1: {"channel-1"},
	})

	mockCluster := NewMockCluster(s.T())
	mockCluster.EXPECT().FlushChannels(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil)
	mockCluster.EXPECT().Close().Maybe()
	s.testServer.cluster = mockCluster

	schema := newTestSchema()
	s.testServer.meta.AddCollection(&collectionInfo{ID: 0, Schema: schema, Partitions: []int64{}})
	allocations, err := s.testServer.segmentManager.AllocSegment(context.TODO(), 0, 1, "channel-1", 1)
	s.NoError(err)
	s.EqualValues(1, len(allocations))
	expireTs := allocations[0].ExpireTime
	segID := allocations[0].SegmentID

	info, err := s.testServer.segmentManager.AllocNewGrowingSegment(context.TODO(), 0, 1, 1, "channel-1")
	s.NoError(err)
	s.NotNil(info)

	resp, err := s.testServer.Flush(context.TODO(), req)
	s.NoError(err)
	s.EqualValues(commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())

	s.testServer.meta.SetCurrentRows(segID, 1)
	ids, err := s.testServer.segmentManager.GetFlushableSegments(context.TODO(), "channel-1", expireTs)
	s.NoError(err)
	s.EqualValues(1, len(ids))
	s.EqualValues(segID, ids[0])
}

func (s *ServerSuite) TestFlush_CollectionNotExist() {
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

	resp, err := s.testServer.Flush(context.TODO(), req)
	s.NoError(err)
	s.EqualValues(commonpb.ErrorCode_CollectionNotExists, resp.GetStatus().GetErrorCode())

	mockHandler := NewNMockHandler(s.T())
	mockHandler.EXPECT().GetCollection(mock.Anything, mock.Anything).
		Return(nil, errors.New("mock error"))
	s.testServer.handler = mockHandler

	resp2, err2 := s.testServer.Flush(context.TODO(), req)
	s.NoError(err2)
	s.EqualValues(commonpb.ErrorCode_UnexpectedError, resp2.GetStatus().GetErrorCode())
}

func (s *ServerSuite) TestFlush_ClosedServer() {
	s.TearDownTest()
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
	resp, err := s.testServer.Flush(context.Background(), req)
	s.NoError(err)
	s.ErrorIs(merr.Error(resp.GetStatus()), merr.ErrServiceNotReady)
}

func (s *ServerSuite) TestFlush_RollingUpgrade() {
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
	mockCluster := NewMockCluster(s.T())
	mockCluster.EXPECT().FlushChannels(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(merr.WrapErrServiceUnimplemented(grpcStatus.Error(codes.Unimplemented, "mock grpc unimplemented error")))
	mockCluster.EXPECT().Close().Maybe()
	s.testServer.cluster = mockCluster
	s.testServer.meta.AddCollection(&collectionInfo{ID: 0})
	s.mockChMgr.EXPECT().GetNodeChannelsByCollectionID(mock.Anything).Return(map[int64][]string{
		1: {"channel-1"},
	}).Once()

	resp, err := s.testServer.Flush(context.TODO(), req)
	s.NoError(err)
	s.EqualValues(commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	s.EqualValues(0, resp.GetFlushTs())
}

func (s *ServerSuite) TestGetSegmentInfoChannel() {
	resp, err := s.testServer.GetSegmentInfoChannel(context.TODO(), nil)
	s.NoError(err)
	s.EqualValues(commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	s.EqualValues(Params.CommonCfg.DataCoordSegmentInfo.GetValue(), resp.Value)
}

func (s *ServerSuite) TestGetSegmentInfo() {
	testSegmentID := int64(1)
	s.testServer.meta.AddSegment(context.TODO(), &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:        1,
			Deltalogs: []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{LogID: 100}}}},
		},
	})

	s.testServer.meta.AddSegment(context.TODO(), &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             2,
			Deltalogs:      []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{LogID: 101}}}},
			CompactionFrom: []int64{1},
		},
	})

	resp, err := s.testServer.GetSegmentInfo(context.TODO(), &datapb.GetSegmentInfoRequest{
		SegmentIDs:       []int64{testSegmentID},
		IncludeUnHealthy: true,
	})
	s.NoError(err)
	s.EqualValues(2, len(resp.Infos[0].Deltalogs))
}

func (s *ServerSuite) TestAssignSegmentID() {
	s.TearDownTest()
	const collID = 100
	const collIDInvalid = 101
	const partID = 0
	const channel0 = "channel0"

	s.Run("assign segment normally", func() {
		s.SetupTest()
		defer s.TearDownTest()

		schema := newTestSchema()
		s.testServer.meta.AddCollection(&collectionInfo{
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

		resp, err := s.testServer.AssignSegmentID(context.TODO(), &datapb.AssignSegmentIDRequest{
			NodeID:            0,
			PeerRole:          "",
			SegmentIDRequests: []*datapb.SegmentIDRequest{req},
		})
		s.NoError(err)
		s.EqualValues(1, len(resp.SegIDAssignments))
		assign := resp.SegIDAssignments[0]
		s.EqualValues(commonpb.ErrorCode_Success, assign.GetStatus().GetErrorCode())
		s.EqualValues(collID, assign.CollectionID)
		s.EqualValues(partID, assign.PartitionID)
		s.EqualValues(channel0, assign.ChannelName)
		s.EqualValues(1000, assign.Count)
	})

	s.Run("with closed server", func() {
		s.SetupTest()
		s.TearDownTest()

		req := &datapb.SegmentIDRequest{
			Count:        100,
			ChannelName:  channel0,
			CollectionID: collID,
			PartitionID:  partID,
		}
		resp, err := s.testServer.AssignSegmentID(context.Background(), &datapb.AssignSegmentIDRequest{
			NodeID:            0,
			PeerRole:          "",
			SegmentIDRequests: []*datapb.SegmentIDRequest{req},
		})
		s.NoError(err)
		s.ErrorIs(merr.Error(resp.GetStatus()), merr.ErrServiceNotReady)
	})

	s.Run("assign segment with invalid collection", func() {
		s.SetupTest()
		defer s.TearDownTest()

		s.testServer.rootCoordClient = &mockRootCoord{
			RootCoordClient: s.testServer.rootCoordClient,
			collID:          collID,
		}

		schema := newTestSchema()
		s.testServer.meta.AddCollection(&collectionInfo{
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

		resp, err := s.testServer.AssignSegmentID(context.TODO(), &datapb.AssignSegmentIDRequest{
			NodeID:            0,
			PeerRole:          "",
			SegmentIDRequests: []*datapb.SegmentIDRequest{req},
		})
		s.NoError(err)
		s.EqualValues(1, len(resp.SegIDAssignments))
	})
}

func TestBroadcastAlteredCollection(t *testing.T) {
	t.Run("test server is closed", func(t *testing.T) {
		s := &Server{}
		s.stateCode.Store(commonpb.StateCode_Initializing)
		ctx := context.Background()
		resp, err := s.BroadcastAlteredCollection(ctx, nil)
		assert.NotNil(t, resp.Reason)
		assert.NoError(t, err)
	})

	t.Run("test meta non exist", func(t *testing.T) {
		s := &Server{meta: &meta{collections: make(map[UniqueID]*collectionInfo, 1)}}
		s.stateCode.Store(commonpb.StateCode_Healthy)
		ctx := context.Background()
		req := &datapb.AlterCollectionRequest{
			CollectionID: 1,
			PartitionIDs: []int64{1},
			Properties:   []*commonpb.KeyValuePair{{Key: "k", Value: "v"}},
		}
		resp, err := s.BroadcastAlteredCollection(ctx, req)
		assert.NotNil(t, resp)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(s.meta.collections))
	})

	t.Run("test update meta", func(t *testing.T) {
		s := &Server{meta: &meta{collections: map[UniqueID]*collectionInfo{
			1: {ID: 1},
		}}}
		s.stateCode.Store(commonpb.StateCode_Healthy)
		ctx := context.Background()
		req := &datapb.AlterCollectionRequest{
			CollectionID: 1,
			PartitionIDs: []int64{1},
			Properties:   []*commonpb.KeyValuePair{{Key: "k", Value: "v"}},
		}

		assert.Nil(t, s.meta.collections[1].Properties)
		resp, err := s.BroadcastAlteredCollection(ctx, req)
		assert.NotNil(t, resp)
		assert.NoError(t, err)
		assert.NotNil(t, s.meta.collections[1].Properties)
	})
}

func TestServer_GcConfirm(t *testing.T) {
	t.Run("closed server", func(t *testing.T) {
		s := &Server{}
		s.stateCode.Store(commonpb.StateCode_Initializing)
		resp, err := s.GcConfirm(context.TODO(), &datapb.GcConfirmRequest{CollectionId: 100, PartitionId: 10000})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("normal case", func(t *testing.T) {
		s := &Server{}
		s.stateCode.Store(commonpb.StateCode_Healthy)

		m := &meta{}
		catalog := mocks.NewDataCoordCatalog(t)
		m.catalog = catalog

		catalog.On("GcConfirm",
			mock.Anything,
			mock.AnythingOfType("int64"),
			mock.AnythingOfType("int64")).
			Return(false)

		s.meta = m

		resp, err := s.GcConfirm(context.TODO(), &datapb.GcConfirmRequest{CollectionId: 100, PartitionId: 10000})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		assert.False(t, resp.GetGcFinished())
	})
}

func TestGetRecoveryInfoV2(t *testing.T) {
	t.Run("test get recovery info with no segments", func(t *testing.T) {
		svr := newTestServer(t)
		defer closeTestServer(t, svr)

		svr.rootCoordClientCreator = func(ctx context.Context) (types.RootCoordClient, error) {
			return newMockRootCoordClient(), nil
		}

		req := &datapb.GetRecoveryInfoRequestV2{
			CollectionID: 0,
		}
		resp, err := svr.GetRecoveryInfoV2(context.TODO(), req)
		assert.NoError(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		assert.EqualValues(t, 0, len(resp.GetSegments()))
		assert.EqualValues(t, 0, len(resp.GetChannels()))
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

		ch := &channelMeta{Name: "vchan1", CollectionID: 0}
		svr.channelManager.AddNode(0)
		svr.channelManager.Watch(context.Background(), ch)

		req := &datapb.GetRecoveryInfoRequestV2{
			CollectionID: 0,
		}
		resp, err := svr.GetRecoveryInfoV2(context.TODO(), req)
		assert.NoError(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		assert.EqualValues(t, 1, len(resp.GetChannels()))
		assert.EqualValues(t, 0, len(resp.GetChannels()[0].GetUnflushedSegmentIds()))
		// assert.ElementsMatch(t, []int64{0, 1}, resp.GetChannels()[0].GetFlushedSegmentIds())
		assert.EqualValues(t, 10, resp.GetChannels()[0].GetSeekPosition().GetTimestamp())
		// assert.EqualValues(t, 2, len(resp.GetSegments()))
		// Row count corrected from 100 + 100 -> 100 + 60.
		// assert.EqualValues(t, 160, resp.GetSegments()[0].GetNumOfRows()+resp.GetSegments()[1].GetNumOfRows())
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

		ch := &channelMeta{Name: "vchan1", CollectionID: 0}
		svr.channelManager.AddNode(0)
		svr.channelManager.Watch(context.Background(), ch)

		req := &datapb.GetRecoveryInfoRequestV2{
			CollectionID: 0,
		}
		resp, err := svr.GetRecoveryInfoV2(context.TODO(), req)
		assert.NoError(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		assert.EqualValues(t, 0, len(resp.GetSegments()))
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
							LogID: 801,
						},
						{
							LogID: 801,
						},
					},
				},
			},
			Field2StatslogPaths: []*datapb.FieldBinlog{
				{
					FieldID: 1,
					Binlogs: []*datapb.Binlog{
						{
							LogID: 10000,
						},
						{
							LogID: 10000,
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
							LogPath:       metautil.BuildDeltaLogPath("a", 0, 100, 0, 100000),
							LogSize:       1,
							LogID:         100000,
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

		err = svr.channelManager.AddNode(0)
		assert.NoError(t, err)
		err = svr.channelManager.Watch(context.Background(), &channelMeta{Name: "vchan1", CollectionID: 0})
		assert.NoError(t, err)

		sResp, err := svr.SaveBinlogPaths(context.TODO(), binlogReq)
		assert.NoError(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, sResp.ErrorCode)

		req := &datapb.GetRecoveryInfoRequestV2{
			CollectionID: 0,
			PartitionIDs: []int64{1},
		}
		resp, err := svr.GetRecoveryInfoV2(context.TODO(), req)
		assert.NoError(t, err)
		assert.NoError(t, merr.Error(resp.Status))
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		assert.EqualValues(t, 1, len(resp.GetSegments()))
		assert.EqualValues(t, 0, resp.GetSegments()[0].GetID())
		assert.EqualValues(t, 0, len(resp.GetSegments()[0].GetBinlogs()))
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

		ch := &channelMeta{Name: "vchan1", CollectionID: 0}
		svr.channelManager.AddNode(0)
		svr.channelManager.Watch(context.Background(), ch)

		req := &datapb.GetRecoveryInfoRequestV2{
			CollectionID: 0,
		}
		resp, err := svr.GetRecoveryInfoV2(context.TODO(), req)
		assert.NoError(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		assert.EqualValues(t, 0, len(resp.GetSegments()))
		assert.EqualValues(t, 1, len(resp.GetChannels()))
		assert.NotNil(t, resp.GetChannels()[0].SeekPosition)
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

		ch := &channelMeta{Name: "vchan1", CollectionID: 0}
		svr.channelManager.AddNode(0)
		svr.channelManager.Watch(context.Background(), ch)

		req := &datapb.GetRecoveryInfoRequestV2{
			CollectionID: 0,
		}
		resp, err := svr.GetRecoveryInfoV2(context.TODO(), req)
		assert.NoError(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		assert.EqualValues(t, 0, len(resp.GetSegments()))
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

		ch := &channelMeta{Name: "vchan1", CollectionID: 0}
		svr.channelManager.AddNode(0)
		svr.channelManager.Watch(context.Background(), ch)

		req := &datapb.GetRecoveryInfoRequestV2{
			CollectionID: 0,
		}
		resp, err := svr.GetRecoveryInfoV2(context.TODO(), req)
		assert.NoError(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		assert.NotNil(t, resp.GetChannels()[0].SeekPosition)
		assert.NotEqual(t, 0, resp.GetChannels()[0].GetSeekPosition().GetTimestamp())
		assert.Len(t, resp.GetChannels()[0].GetDroppedSegmentIds(), 0)
		// assert.ElementsMatch(t, []UniqueID{}, resp.GetChannels()[0].GetUnflushedSegmentIds())
		// assert.ElementsMatch(t, []UniqueID{9, 10, 12}, resp.GetChannels()[0].GetFlushedSegmentIds())
	})

	t.Run("with closed server", func(t *testing.T) {
		svr := newTestServer(t)
		closeTestServer(t, svr)
		resp, err := svr.GetRecoveryInfoV2(context.TODO(), &datapb.GetRecoveryInfoRequestV2{})
		assert.NoError(t, err)
		err = merr.Error(resp.GetStatus())
		assert.ErrorIs(t, err, merr.ErrServiceNotReady)
	})
}

func TestImportV2(t *testing.T) {
	ctx := context.Background()
	mockErr := errors.New("mock err")

	t.Run("ImportV2", func(t *testing.T) {
		// server not healthy
		s := &Server{}
		s.stateCode.Store(commonpb.StateCode_Initializing)
		resp, err := s.ImportV2(ctx, nil)
		assert.NoError(t, err)
		assert.NotEqual(t, int32(0), resp.GetStatus().GetCode())
		s.stateCode.Store(commonpb.StateCode_Healthy)

		// parse timeout failed
		resp, err = s.ImportV2(ctx, &internalpb.ImportRequestInternal{
			Options: []*commonpb.KeyValuePair{
				{
					Key:   "timeout",
					Value: "@$#$%#%$",
				},
			},
		})
		assert.NoError(t, err)
		assert.True(t, errors.Is(merr.Error(resp.GetStatus()), merr.ErrImportFailed))

		// list binlog failed
		cm := mocks2.NewChunkManager(t)
		cm.EXPECT().WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(mockErr)
		s.meta = &meta{chunkManager: cm}
		resp, err = s.ImportV2(ctx, &internalpb.ImportRequestInternal{
			Files: []*internalpb.ImportFile{
				{
					Id:    1,
					Paths: []string{"mock_insert_prefix"},
				},
			},
			Options: []*commonpb.KeyValuePair{
				{
					Key:   "backup",
					Value: "true",
				},
			},
		})
		assert.NoError(t, err)
		assert.True(t, errors.Is(merr.Error(resp.GetStatus()), merr.ErrImportFailed))

		// alloc failed
		alloc := allocator.NewMockAllocator(t)
		alloc.EXPECT().AllocN(mock.Anything).Return(0, 0, mockErr)
		s.allocator = alloc
		resp, err = s.ImportV2(ctx, &internalpb.ImportRequestInternal{})
		assert.NoError(t, err)
		assert.True(t, errors.Is(merr.Error(resp.GetStatus()), merr.ErrImportFailed))
		alloc = allocator.NewMockAllocator(t)
		alloc.EXPECT().AllocN(mock.Anything).Return(0, 0, nil)
		s.allocator = alloc

		// add job failed
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().ListImportJobs().Return(nil, nil)
		catalog.EXPECT().ListPreImportTasks().Return(nil, nil)
		catalog.EXPECT().ListImportTasks().Return(nil, nil)
		catalog.EXPECT().SaveImportJob(mock.Anything).Return(mockErr)
		s.importMeta, err = NewImportMeta(catalog)
		assert.NoError(t, err)
		resp, err = s.ImportV2(ctx, &internalpb.ImportRequestInternal{
			Files: []*internalpb.ImportFile{
				{
					Id:    1,
					Paths: []string{"a.json"},
				},
			},
		})
		assert.NoError(t, err)
		assert.True(t, errors.Is(merr.Error(resp.GetStatus()), merr.ErrImportFailed))
		jobs := s.importMeta.GetJobBy()
		assert.Equal(t, 0, len(jobs))
		catalog.ExpectedCalls = lo.Filter(catalog.ExpectedCalls, func(call *mock.Call, _ int) bool {
			return call.Method != "SaveImportJob"
		})
		catalog.EXPECT().SaveImportJob(mock.Anything).Return(nil)

		// normal case
		resp, err = s.ImportV2(ctx, &internalpb.ImportRequestInternal{
			Files: []*internalpb.ImportFile{
				{
					Id:    1,
					Paths: []string{"a.json"},
				},
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, int32(0), resp.GetStatus().GetCode())
		jobs = s.importMeta.GetJobBy()
		assert.Equal(t, 1, len(jobs))
	})

	t.Run("GetImportProgress", func(t *testing.T) {
		// server not healthy
		s := &Server{}
		s.stateCode.Store(commonpb.StateCode_Initializing)
		resp, err := s.GetImportProgress(ctx, nil)
		assert.NoError(t, err)
		assert.NotEqual(t, int32(0), resp.GetStatus().GetCode())
		s.stateCode.Store(commonpb.StateCode_Healthy)

		// illegal jobID
		resp, err = s.GetImportProgress(ctx, &internalpb.GetImportProgressRequest{
			JobID: "@%$%$#%",
		})
		assert.NoError(t, err)
		assert.True(t, errors.Is(merr.Error(resp.GetStatus()), merr.ErrImportFailed))

		// job does not exist
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().ListImportJobs().Return(nil, nil)
		catalog.EXPECT().ListPreImportTasks().Return(nil, nil)
		catalog.EXPECT().ListImportTasks().Return(nil, nil)
		catalog.EXPECT().SaveImportJob(mock.Anything).Return(nil)
		s.importMeta, err = NewImportMeta(catalog)
		assert.NoError(t, err)
		resp, err = s.GetImportProgress(ctx, &internalpb.GetImportProgressRequest{
			JobID: "-1",
		})
		assert.NoError(t, err)
		assert.True(t, errors.Is(merr.Error(resp.GetStatus()), merr.ErrImportFailed))

		// normal case
		var job ImportJob = &importJob{
			ImportJob: &datapb.ImportJob{
				JobID:  0,
				Schema: &schemapb.CollectionSchema{},
				State:  internalpb.ImportJobState_Failed,
			},
		}
		err = s.importMeta.AddJob(job)
		assert.NoError(t, err)
		resp, err = s.GetImportProgress(ctx, &internalpb.GetImportProgressRequest{
			JobID: "0",
		})
		assert.NoError(t, err)
		assert.Equal(t, int32(0), resp.GetStatus().GetCode())
		assert.Equal(t, int64(0), resp.GetProgress())
		assert.Equal(t, internalpb.ImportJobState_Failed, resp.GetState())
	})

	t.Run("ListImports", func(t *testing.T) {
		// server not healthy
		s := &Server{}
		s.stateCode.Store(commonpb.StateCode_Initializing)
		resp, err := s.ListImports(ctx, nil)
		assert.NoError(t, err)
		assert.NotEqual(t, int32(0), resp.GetStatus().GetCode())
		s.stateCode.Store(commonpb.StateCode_Healthy)

		// normal case
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().ListImportJobs().Return(nil, nil)
		catalog.EXPECT().ListPreImportTasks().Return(nil, nil)
		catalog.EXPECT().ListImportTasks().Return(nil, nil)
		catalog.EXPECT().SaveImportJob(mock.Anything).Return(nil)
		catalog.EXPECT().SavePreImportTask(mock.Anything).Return(nil)
		s.importMeta, err = NewImportMeta(catalog)
		assert.NoError(t, err)
		var job ImportJob = &importJob{
			ImportJob: &datapb.ImportJob{
				JobID:        0,
				CollectionID: 1,
				Schema:       &schemapb.CollectionSchema{},
			},
		}
		err = s.importMeta.AddJob(job)
		assert.NoError(t, err)
		var task ImportTask = &preImportTask{
			PreImportTask: &datapb.PreImportTask{
				JobID:  0,
				TaskID: 1,
				State:  datapb.ImportTaskStateV2_Failed,
			},
		}
		err = s.importMeta.AddTask(task)
		assert.NoError(t, err)
		resp, err = s.ListImports(ctx, &internalpb.ListImportsRequestInternal{
			CollectionID: 1,
		})
		assert.NoError(t, err)
		assert.Equal(t, int32(0), resp.GetStatus().GetCode())
		assert.Equal(t, 1, len(resp.GetJobIDs()))
		assert.Equal(t, 1, len(resp.GetStates()))
		assert.Equal(t, 1, len(resp.GetReasons()))
		assert.Equal(t, 1, len(resp.GetProgresses()))
	})
}

func TestGetChannelRecoveryInfo(t *testing.T) {
	ctx := context.Background()

	// server not healthy
	s := &Server{}
	s.stateCode.Store(commonpb.StateCode_Initializing)
	resp, err := s.GetChannelRecoveryInfo(ctx, nil)
	assert.NoError(t, err)
	assert.NotEqual(t, int32(0), resp.GetStatus().GetCode())
	s.stateCode.Store(commonpb.StateCode_Healthy)

	// get collection failed
	handler := NewNMockHandler(t)
	handler.EXPECT().GetCollection(mock.Anything, mock.Anything).
		Return(nil, errors.New("mock err"))
	s.handler = handler
	assert.NoError(t, err)
	resp, err = s.GetChannelRecoveryInfo(ctx, &datapb.GetChannelRecoveryInfoRequest{
		Vchannel: "ch-1",
	})
	assert.NoError(t, err)
	assert.Error(t, merr.Error(resp.GetStatus()))

	// normal case
	channelInfo := &datapb.VchannelInfo{
		CollectionID:        0,
		ChannelName:         "ch-1",
		SeekPosition:        nil,
		UnflushedSegmentIds: []int64{1},
		FlushedSegmentIds:   []int64{2},
		DroppedSegmentIds:   []int64{3},
		IndexedSegmentIds:   []int64{4},
	}

	handler = NewNMockHandler(t)
	handler.EXPECT().GetCollection(mock.Anything, mock.Anything).
		Return(&collectionInfo{Schema: &schemapb.CollectionSchema{}}, nil)
	handler.EXPECT().GetDataVChanPositions(mock.Anything, mock.Anything).Return(channelInfo)
	s.handler = handler

	assert.NoError(t, err)
	resp, err = s.GetChannelRecoveryInfo(ctx, &datapb.GetChannelRecoveryInfoRequest{
		Vchannel: "ch-1",
	})
	assert.NoError(t, err)
	assert.Equal(t, int32(0), resp.GetStatus().GetCode())
	assert.NotNil(t, resp.GetSchema())
	assert.Equal(t, channelInfo, resp.GetInfo())
}

type GcControlServiceSuite struct {
	suite.Suite

	server *Server
}

func (s *GcControlServiceSuite) SetupTest() {
	s.server = newTestServer(s.T())
}

func (s *GcControlServiceSuite) TearDownTest() {
	if s.server != nil {
		closeTestServer(s.T(), s.server)
	}
}

func (s *GcControlServiceSuite) TestClosedServer() {
	closeTestServer(s.T(), s.server)
	resp, err := s.server.GcControl(context.TODO(), &datapb.GcControlRequest{})
	s.NoError(err)
	s.False(merr.Ok(resp))
	s.server = nil
}

func (s *GcControlServiceSuite) TestUnknownCmd() {
	resp, err := s.server.GcControl(context.TODO(), &datapb.GcControlRequest{
		Command: 0,
	})
	s.NoError(err)
	s.False(merr.Ok(resp))
}

func (s *GcControlServiceSuite) TestPause() {
	resp, err := s.server.GcControl(context.TODO(), &datapb.GcControlRequest{
		Command: datapb.GcCommand_Pause,
	})
	s.Nil(err)
	s.False(merr.Ok(resp))

	resp, err = s.server.GcControl(context.TODO(), &datapb.GcControlRequest{
		Command: datapb.GcCommand_Pause,
		Params: []*commonpb.KeyValuePair{
			{Key: "duration", Value: "not_int"},
		},
	})
	s.Nil(err)
	s.False(merr.Ok(resp))

	resp, err = s.server.GcControl(context.TODO(), &datapb.GcControlRequest{
		Command: datapb.GcCommand_Pause,
		Params: []*commonpb.KeyValuePair{
			{Key: "duration", Value: "60"},
		},
	})
	s.Nil(err)
	s.True(merr.Ok(resp))
}

func (s *GcControlServiceSuite) TestResume() {
	resp, err := s.server.GcControl(context.TODO(), &datapb.GcControlRequest{
		Command: datapb.GcCommand_Resume,
	})
	s.Nil(err)
	s.True(merr.Ok(resp))
}

func (s *GcControlServiceSuite) TestTimeoutCtx() {
	s.server.garbageCollector.close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	resp, err := s.server.GcControl(ctx, &datapb.GcControlRequest{
		Command: datapb.GcCommand_Resume,
	})
	s.Nil(err)
	s.False(merr.Ok(resp))

	resp, err = s.server.GcControl(ctx, &datapb.GcControlRequest{
		Command: datapb.GcCommand_Pause,
		Params: []*commonpb.KeyValuePair{
			{Key: "duration", Value: "60"},
		},
	})
	s.Nil(err)
	s.False(merr.Ok(resp))
}

func TestGcControlService(t *testing.T) {
	suite.Run(t, new(GcControlServiceSuite))
}
