package datacoord

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	globalIDAllocator "github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/coordinator/snmanager"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	mocks2 "github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/mocks/distributed/mock_streaming"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_balancer"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/internal/tso"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v2/kv"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metautil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type ServerSuite struct {
	suite.Suite

	testServer   *Server
	mockMixCoord *mocks2.MixCoord
}

func (s *ServerSuite) SetupSuite() {
	snmanager.ResetStreamingNodeManager()
	b := mock_balancer.NewMockBalancer(s.T())
	b.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cb balancer.WatchChannelAssignmentsCallback) error {
		<-ctx.Done()
		return ctx.Err()
	})
	b.EXPECT().GetLatestWALLocated(mock.Anything, mock.Anything).Return(0, true)
	balance.Register(b)
}

func (s *ServerSuite) SetupTest() {
	s.testServer = newTestServer(s.T())
	s.mockMixCoord = mocks2.NewMixCoord(s.T())
	s.testServer.mixCoord = s.mockMixCoord
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

func (s *ServerSuite) TestGetFlushState_ByFlushTs() {
	s.mockMixCoord.EXPECT().DescribeCollectionInternal(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
		if req.CollectionID == 0 {
			return &milvuspb.DescribeCollectionResponse{
				Status:              merr.Success(),
				CollectionID:        0,
				VirtualChannelNames: []string{"ch1"},
			}, nil
		}
		return &milvuspb.DescribeCollectionResponse{
			Status:       merr.Success(),
			CollectionID: 1,
		}, nil
	})
	tests := []struct {
		description string
		inTs        Timestamp

		expected bool
	}{
		{"channel cp > flush ts", 11, true},
		{"channel cp = flush ts", 12, true},
		{"channel cp < flush ts", 13, false},
	}

	err := s.testServer.meta.UpdateChannelCheckpoint(context.TODO(), "ch1", &msgpb.MsgPosition{
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
	s.mockMixCoord.EXPECT().DescribeCollectionInternal(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
		return &milvuspb.DescribeCollectionResponse{
			Status:              merr.Success(),
			VirtualChannelNames: []string{"ch1"},
		}, nil
	})
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
			err = s.testServer.meta.UpdateChannelCheckpoint(context.TODO(), "ch1", &msgpb.MsgPosition{
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
	resp, err := s.testServer.SaveBinlogPaths(context.Background(), &datapb.SaveBinlogPathsRequest{
		Base: &commonpb.MsgBase{
			SourceID: 1,
		},
		SegmentID: 1,
		Channel:   "test",
	})
	s.NoError(err)
	s.ErrorIs(merr.Error(resp), merr.ErrChannelNotFound)
}

func (s *ServerSuite) TestSaveBinlogPath_SaveUnhealthySegment() {
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
	s.testServer.meta.AddCollection(&collectionInfo{ID: 0})

	segments := map[int64]commonpb.SegmentState{
		0: commonpb.SegmentState_Flushed,
		1: commonpb.SegmentState_Sealed,
		2: commonpb.SegmentState_Sealed,
	}
	for segID, state := range segments {
		numOfRows := int64(100)
		if segID == 2 {
			numOfRows = 0
		}
		info := &datapb.SegmentInfo{
			ID:            segID,
			InsertChannel: "ch1",
			State:         state,
			Level:         datapb.SegmentLevel_L1,
			NumOfRows:     numOfRows,
		}
		err := s.testServer.meta.AddSegment(context.TODO(), NewSegmentInfo(info))
		s.Require().NoError(err)
	}

	tests := []struct {
		description string
		inSegID     int64
		inDropped   bool
		inFlushed   bool
		numOfRows   int64

		expectedState commonpb.SegmentState
	}{
		{"segID=0, flushed to dropped", 0, true, false, 100, commonpb.SegmentState_Dropped},
		{"segID=1, sealed to flushing", 1, false, true, 100, commonpb.SegmentState_Flushed},
		// empty segment flush should be dropped directly.
		{"segID=2, sealed to dropped", 2, false, true, 0, commonpb.SegmentState_Dropped},
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

			segment := s.testServer.meta.GetSegment(context.TODO(), test.inSegID)
			s.NotNil(segment)
			s.EqualValues(0, len(segment.GetBinlogs()))
			s.EqualValues(segment.NumOfRows, test.numOfRows)

			s.Equal(test.expectedState, segment.GetState())
		})
	}
}

func (s *ServerSuite) TestSaveBinlogPath_L0Segment() {
	s.testServer.meta.AddCollection(&collectionInfo{ID: 0})

	segment := s.testServer.meta.GetHealthySegment(context.TODO(), 1)
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

	segment = s.testServer.meta.GetHealthySegment(context.TODO(), 1)
	s.NotNil(segment)
	s.EqualValues(datapb.SegmentLevel_L0, segment.GetLevel())
}

func (s *ServerSuite) TestSaveBinlogPath_NormalCase() {
	s.testServer.meta.AddCollection(&collectionInfo{ID: 0})

	segments := map[int64]int64{
		0: 0,
		1: 0,
		2: 0,
		3: 0,
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

	segment := s.testServer.meta.GetHealthySegment(context.TODO(), 1)
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

	resp, err = s.testServer.SaveBinlogPaths(ctx, &datapb.SaveBinlogPathsRequest{
		Base: &commonpb.MsgBase{
			Timestamp: uint64(time.Now().Unix()),
		},
		SegmentID:           2,
		CollectionID:        0,
		Channel:             "ch1",
		Field2BinlogPaths:   []*datapb.FieldBinlog{},
		Field2StatslogPaths: []*datapb.FieldBinlog{},
		CheckPoints:         []*datapb.CheckPoint{},
		Flushed:             true,
	})
	s.NoError(err)
	s.EqualValues(resp.ErrorCode, commonpb.ErrorCode_Success)
	segment = s.testServer.meta.GetSegment(context.TODO(), 2)
	s.NotNil(segment)
	s.Equal(commonpb.SegmentState_Dropped, segment.GetState())

	resp, err = s.testServer.SaveBinlogPaths(ctx, &datapb.SaveBinlogPathsRequest{
		Base: &commonpb.MsgBase{
			Timestamp: uint64(time.Now().Unix()),
		},
		SegmentID:    3,
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
				SegmentID: 3,
				Position: &msgpb.MsgPosition{
					ChannelName: "ch1",
					MsgID:       []byte{1, 2, 3},
					MsgGroup:    "",
					Timestamp:   0,
				},
				NumOfRows: 12,
			},
		},
		Flushed:         false,
		WithFullBinlogs: true,
	})
	s.NoError(err)
	s.EqualValues(resp.ErrorCode, commonpb.ErrorCode_Success)

	segment = s.testServer.meta.GetHealthySegment(context.TODO(), 3)
	s.NotNil(segment)
	binlogs = segment.GetBinlogs()
	s.EqualValues(1, len(binlogs))
	fieldBinlogs = binlogs[0]
	s.NotNil(fieldBinlogs)
	s.EqualValues(2, len(fieldBinlogs.GetBinlogs()))
	s.EqualValues(1, fieldBinlogs.GetFieldID())
	s.EqualValues("", fieldBinlogs.GetBinlogs()[0].GetLogPath())
	s.EqualValues(int64(1), fieldBinlogs.GetBinlogs()[0].GetLogID())
	s.EqualValues("", fieldBinlogs.GetBinlogs()[1].GetLogPath())
	s.EqualValues(int64(2), fieldBinlogs.GetBinlogs()[1].GetLogID())

	resp, err = s.testServer.SaveBinlogPaths(ctx, &datapb.SaveBinlogPathsRequest{
		Base: &commonpb.MsgBase{
			Timestamp: uint64(time.Now().Unix()),
		},
		SegmentID:    3,
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
					{
						LogPath:    "/by-dev/test/0/1/1/1/3",
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
					{
						LogPath:    "/by-dev/test_stats/0/1/1/1/3",
						EntriesNum: 5,
					},
				},
			},
		},
		CheckPoints: []*datapb.CheckPoint{
			{
				SegmentID: 3,
				Position: &msgpb.MsgPosition{
					ChannelName: "ch1",
					MsgID:       []byte{1, 2, 3},
					MsgGroup:    "",
					Timestamp:   1,
				},
				NumOfRows: 12,
			},
		},
		Flushed:         false,
		WithFullBinlogs: true,
	})
	s.NoError(err)
	s.EqualValues(resp.ErrorCode, commonpb.ErrorCode_Success)

	segment = s.testServer.meta.GetHealthySegment(context.TODO(), 3)
	s.NotNil(segment)
	binlogs = segment.GetBinlogs()
	s.EqualValues(1, len(binlogs))
	fieldBinlogs = binlogs[0]
	s.NotNil(fieldBinlogs)
	s.EqualValues(3, len(fieldBinlogs.GetBinlogs()))
	s.EqualValues(1, fieldBinlogs.GetFieldID())
	s.EqualValues("", fieldBinlogs.GetBinlogs()[0].GetLogPath())
	s.EqualValues(int64(1), fieldBinlogs.GetBinlogs()[0].GetLogID())
	s.EqualValues("", fieldBinlogs.GetBinlogs()[1].GetLogPath())
	s.EqualValues(int64(2), fieldBinlogs.GetBinlogs()[1].GetLogID())
	s.EqualValues("", fieldBinlogs.GetBinlogs()[2].GetLogPath())
	s.EqualValues(int64(3), fieldBinlogs.GetBinlogs()[2].GetLogID())

	resp, err = s.testServer.SaveBinlogPaths(ctx, &datapb.SaveBinlogPathsRequest{
		Base: &commonpb.MsgBase{
			Timestamp: uint64(time.Now().Unix()),
		},
		SegmentID:           3,
		CollectionID:        0,
		Channel:             "ch1",
		Field2BinlogPaths:   []*datapb.FieldBinlog{},
		Field2StatslogPaths: []*datapb.FieldBinlog{},
		CheckPoints: []*datapb.CheckPoint{
			{
				SegmentID: 3,
				Position: &msgpb.MsgPosition{
					ChannelName: "ch1",
					MsgID:       []byte{1, 2, 3},
					MsgGroup:    "",
					Timestamp:   0,
				},
				NumOfRows: 12,
			},
		},
		Flushed:         false,
		WithFullBinlogs: true,
	})
	s.NoError(err)
	s.EqualValues(resp.ErrorCode, commonpb.ErrorCode_Success)

	segment = s.testServer.meta.GetHealthySegment(context.TODO(), 3)
	s.NotNil(segment)
	binlogs = segment.GetBinlogs()
	s.EqualValues(1, len(binlogs))
	fieldBinlogs = binlogs[0]
	s.NotNil(fieldBinlogs)
	s.EqualValues(3, len(fieldBinlogs.GetBinlogs()))
	s.EqualValues(1, fieldBinlogs.GetFieldID())
	s.EqualValues("", fieldBinlogs.GetBinlogs()[0].GetLogPath())
	s.EqualValues(int64(1), fieldBinlogs.GetBinlogs()[0].GetLogID())
	s.EqualValues("", fieldBinlogs.GetBinlogs()[1].GetLogPath())
	s.EqualValues(int64(2), fieldBinlogs.GetBinlogs()[1].GetLogID())
	s.EqualValues("", fieldBinlogs.GetBinlogs()[2].GetLogPath())
	s.EqualValues(int64(3), fieldBinlogs.GetBinlogs()[2].GetLogID())
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

	schema := newTestSchema()
	s.testServer.meta.AddCollection(&collectionInfo{ID: 0, Schema: schema, Partitions: []int64{}, VChannelNames: []string{"channel-1"}})
	allocations, err := s.testServer.segmentManager.AllocSegment(context.TODO(), 0, 1, "channel-1", 1, storage.StorageV1)
	s.NoError(err)
	s.EqualValues(1, len(allocations))
	expireTs := allocations[0].ExpireTime
	segID := allocations[0].SegmentID

	info, err := s.testServer.segmentManager.AllocNewGrowingSegment(context.TODO(), AllocNewGrowingSegmentRequest{
		CollectionID:         0,
		PartitionID:          1,
		SegmentID:            1,
		ChannelName:          "channel1-1",
		StorageVersion:       storage.StorageV1,
		IsCreatedByStreaming: true,
	})
	s.NoError(err)
	s.NotNil(info)

	resp, err := s.testServer.Flush(context.TODO(), req)
	s.NoError(err)
	s.EqualValues(commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())

	s.testServer.meta.SetRowCount(segID, 1)
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
		s := &Server{meta: &meta{collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()}}
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
		assert.Equal(t, 1, s.meta.collections.Len())
	})

	t.Run("test update meta", func(t *testing.T) {
		collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
		collections.Insert(1, &collectionInfo{ID: 1})
		s := &Server{meta: &meta{collections: collections}}
		s.stateCode.Store(commonpb.StateCode_Healthy)
		ctx := context.Background()
		req := &datapb.AlterCollectionRequest{
			CollectionID: 1,
			PartitionIDs: []int64{1},
			Properties:   []*commonpb.KeyValuePair{{Key: "k", Value: "v"}},
		}

		coll, ok := s.meta.collections.Get(1)
		assert.True(t, ok)
		assert.Nil(t, coll.Properties)
		resp, err := s.BroadcastAlteredCollection(ctx, req)
		assert.NotNil(t, resp)
		assert.NoError(t, err)
		coll, ok = s.meta.collections.Get(1)
		assert.True(t, ok)
		assert.NotNil(t, coll.Properties)
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
		svr.mixCoordCreator = func(ctx context.Context) (types.MixCoord, error) {
			return newMockMixCoord(), nil
		}
		req := &datapb.GetRecoveryInfoRequestV2{
			CollectionID: 0,
		}
		resp, err := svr.GetRecoveryInfoV2(context.TODO(), req)
		assert.NoError(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		assert.EqualValues(t, 0, len(resp.GetSegments()))
		assert.EqualValues(t, 1, len(resp.GetChannels()))
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

		svr.mixCoordCreator = func(ctx context.Context) (types.MixCoord, error) {
			return newMockMixCoord(), nil
		}

		svr.meta.AddCollection(&collectionInfo{
			Schema: newTestSchema(),
		})

		err := svr.meta.UpdateChannelCheckpoint(context.TODO(), "vchan1", &msgpb.MsgPosition{
			ChannelName: "vchan1",
			Timestamp:   10,
			MsgID:       []byte{0, 0, 0, 0, 0, 0, 0, 0},
		})
		assert.NoError(t, err)

		err = svr.meta.indexMeta.CreateIndex(context.TODO(), &model.Index{
			CollectionID: 0,
			FieldID:      2,
			IndexID:      rand.Int63n(1000),
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
		err = svr.meta.indexMeta.AddSegmentIndex(context.TODO(), &model.SegmentIndex{
			SegmentID: seg1.ID,
			BuildID:   seg1.ID,
		})
		assert.NoError(t, err)
		err = svr.meta.indexMeta.FinishTask(&workerpb.IndexTaskInfo{
			BuildID: seg1.ID,
			State:   commonpb.IndexState_Finished,
		})
		assert.NoError(t, err)
		err = svr.meta.indexMeta.AddSegmentIndex(context.TODO(), &model.SegmentIndex{
			SegmentID: seg2.ID,
			BuildID:   seg2.ID,
		})
		assert.NoError(t, err)
		err = svr.meta.indexMeta.FinishTask(&workerpb.IndexTaskInfo{
			BuildID: seg2.ID,
			State:   commonpb.IndexState_Finished,
		})
		assert.NoError(t, err)

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
		svr.mixCoordCreator = func(ctx context.Context) (types.MixCoord, error) {
			return newMockMixCoord(), nil
		}
		svr.meta.AddCollection(&collectionInfo{
			ID:     0,
			Schema: newTestSchema(),
		})

		err := svr.meta.UpdateChannelCheckpoint(context.TODO(), "vchan1", &msgpb.MsgPosition{
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
		svr.mixCoordCreator = func(ctx context.Context) (types.MixCoord, error) {
			return newMockMixCoord(), nil
		}
		svr.meta.AddCollection(&collectionInfo{
			Schema: newTestSchema(),
		})

		binlogReq := &datapb.SaveBinlogPathsRequest{
			SegmentID:    10087,
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
			Flushed: true,
		}
		segment := createSegment(binlogReq.SegmentID, 0, 1, 100, 10, "vchan1", commonpb.SegmentState_Growing)
		err := svr.meta.AddSegment(context.TODO(), NewSegmentInfo(segment))
		assert.NoError(t, err)

		err = svr.meta.indexMeta.CreateIndex(context.TODO(), &model.Index{
			CollectionID: 0,
			FieldID:      2,
			IndexID:      rand.Int63n(1000),
		})
		assert.NoError(t, err)
		err = svr.meta.indexMeta.AddSegmentIndex(context.TODO(), &model.SegmentIndex{
			SegmentID: segment.ID,
			BuildID:   segment.ID,
		})
		assert.NoError(t, err)
		err = svr.meta.indexMeta.FinishTask(&workerpb.IndexTaskInfo{
			BuildID: segment.ID,
			State:   commonpb.IndexState_Finished,
		})
		assert.NoError(t, err)

		paramtable.Get().Save(Params.DataCoordCfg.EnableSortCompaction.Key, "false")
		defer paramtable.Get().Reset(Params.DataCoordCfg.EnableSortCompaction.Key)

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
		assert.EqualValues(t, binlogReq.SegmentID, resp.GetSegments()[0].GetID())
		assert.EqualValues(t, 0, len(resp.GetSegments()[0].GetBinlogs()))
	})
	t.Run("with dropped segments", func(t *testing.T) {
		svr := newTestServer(t)
		defer closeTestServer(t, svr)

		svr.mixCoordCreator = func(ctx context.Context) (types.MixCoord, error) {
			return newMockMixCoord(), nil
		}

		svr.meta.AddCollection(&collectionInfo{
			ID:     0,
			Schema: newTestSchema(),
		})

		err := svr.meta.UpdateChannelCheckpoint(context.TODO(), "vchan1", &msgpb.MsgPosition{
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
		svr.mixCoordCreator = func(ctx context.Context) (types.MixCoord, error) {
			return newMockMixCoord(), nil
		}

		svr.meta.AddCollection(&collectionInfo{
			ID:     0,
			Schema: newTestSchema(),
		})

		err := svr.meta.UpdateChannelCheckpoint(context.TODO(), "vchan1", &msgpb.MsgPosition{
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

		svr.mixCoordCreator = func(ctx context.Context) (types.MixCoord, error) {
			return newMockMixCoord(), nil
		}

		svr.meta.AddCollection(&collectionInfo{
			ID:     0,
			Schema: newTestSchema(),
		})

		err := svr.meta.UpdateChannelCheckpoint(context.TODO(), "vchan1", &msgpb.MsgPosition{
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
		err = svr.meta.indexMeta.CreateIndex(context.TODO(), &model.Index{
			CollectionID: 0,
			FieldID:      2,
			IndexID:      rand.Int63n(1000),
			IndexName:    "_default_idx_2",
		})
		assert.NoError(t, err)
		svr.meta.indexMeta.updateSegmentIndex(&model.SegmentIndex{
			SegmentID:           seg4.ID,
			CollectionID:        0,
			PartitionID:         0,
			NumRows:             100,
			IndexID:             0,
			BuildID:             0,
			NodeID:              0,
			IndexVersion:        1,
			IndexState:          commonpb.IndexState_Finished,
			FailReason:          "",
			IsDeleted:           false,
			CreatedUTCTime:      0,
			IndexFileKeys:       nil,
			IndexSerializedSize: 0,
		})

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
		mockHandler := NewNMockHandler(t)
		mockHandler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(&collectionInfo{
			ID:            1000,
			VChannelNames: []string{"foo_1v1"},
		}, nil).Maybe()
		s.handler = mockHandler

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
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
		s.importMeta, err = NewImportMeta(context.TODO(), catalog, nil, nil)
		assert.NoError(t, err)
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
		catalog = mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(mockErr)
		s.importMeta, err = NewImportMeta(context.TODO(), catalog, nil, nil)
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
		jobs := s.importMeta.GetJobBy(context.TODO())
		assert.Equal(t, 0, len(jobs))
		catalog.ExpectedCalls = lo.Filter(catalog.ExpectedCalls, func(call *mock.Call, _ int) bool {
			return call.Method != "SaveImportJob"
		})
		catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)

		// normal case
		resp, err = s.ImportV2(ctx, &internalpb.ImportRequestInternal{
			Files: []*internalpb.ImportFile{
				{
					Id:    1,
					Paths: []string{"a.json"},
				},
			},
			ChannelNames: []string{"foo_1v1"},
		})
		assert.NoError(t, err)
		assert.Equal(t, int32(0), resp.GetStatus().GetCode())
		jobs = s.importMeta.GetJobBy(context.TODO())
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
		catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)
		wal := mock_streaming.NewMockWALAccesser(t)
		b := mock_streaming.NewMockBroadcast(t)
		wal.EXPECT().Broadcast().Return(b).Maybe()
		// streaming.SetWALForTest(wal)
		// defer streaming.RecoverWALForTest()

		s.importMeta, err = NewImportMeta(context.TODO(), catalog, nil, nil)
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
		err = s.importMeta.AddJob(context.TODO(), job)
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
		catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)
		catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).Return(nil)
		s.importMeta, err = NewImportMeta(context.TODO(), catalog, nil, nil)
		assert.NoError(t, err)
		var job ImportJob = &importJob{
			ImportJob: &datapb.ImportJob{
				JobID:        0,
				CollectionID: 1,
				Schema:       &schemapb.CollectionSchema{},
			},
		}
		err = s.importMeta.AddJob(context.TODO(), job)
		assert.NoError(t, err)
		taskProto := &datapb.PreImportTask{
			JobID:  0,
			TaskID: 1,
			State:  datapb.ImportTaskStateV2_Failed,
		}
		var task ImportTask = &preImportTask{}
		task.(*preImportTask).task.Store(taskProto)
		err = s.importMeta.AddTask(context.TODO(), task)
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
	broker := broker.NewMockBroker(t)
	s.broker = broker

	// normal case
	channelInfo := &datapb.VchannelInfo{
		CollectionID:        0,
		ChannelName:         "ch-1",
		SeekPosition:        &msgpb.MsgPosition{Timestamp: 10},
		UnflushedSegmentIds: []int64{1},
		FlushedSegmentIds:   []int64{2},
		DroppedSegmentIds:   []int64{3},
		IndexedSegmentIds:   []int64{4},
	}

	handler := NewNMockHandler(t)
	handler.EXPECT().GetDataVChanPositions(mock.Anything, mock.Anything).Return(channelInfo)
	s.handler = handler
	s.meta = &meta{
		segments: NewSegmentsInfo(),
	}
	s.meta.segments.segments[1] = NewSegmentInfo(&datapb.SegmentInfo{
		ID:                   1,
		CollectionID:         0,
		PartitionID:          0,
		State:                commonpb.SegmentState_Growing,
		IsCreatedByStreaming: false,
	})

	assert.NoError(t, err)
	resp, err = s.GetChannelRecoveryInfo(ctx, &datapb.GetChannelRecoveryInfoRequest{
		Vchannel: "ch-1",
	})
	assert.NoError(t, err)
	assert.Equal(t, int32(0), resp.GetStatus().GetCode())
	assert.Nil(t, resp.GetSchema())
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

func TestServer_AddFileResource(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		mockCatalog := mocks.NewDataCoordCatalog(t)
		mockAllocator := tso.NewMockAllocator()
		mockAllocator.GenerateTSOF = func(count uint32) (uint64, error) { return 100, nil }

		server := &Server{
			idAllocator: globalIDAllocator.NewTestGlobalIDAllocator(mockAllocator),
			mixCoord:    newMockMixCoord(),
			meta: &meta{
				resourceMeta: make(map[string]*internalpb.FileResourceInfo),
				catalog:      mockCatalog,
			},
		}
		server.stateCode.Store(commonpb.StateCode_Healthy)

		req := &milvuspb.AddFileResourceRequest{
			Base: &commonpb.MsgBase{},
			Name: "test_resource",
			Path: "/path/to/resource",
		}

		mockCatalog.EXPECT().SaveFileResource(mock.Anything, mock.MatchedBy(func(resource *internalpb.FileResourceInfo) bool {
			return resource.Name == "test_resource" && resource.Path == "/path/to/resource"
		}), mock.Anything).Return(nil)

		resp, err := server.AddFileResource(context.Background(), req)
		assert.NoError(t, err)
		assert.NoError(t, merr.Error(resp))
	})

	t.Run("server not healthy", func(t *testing.T) {
		server := &Server{}
		server.stateCode.Store(commonpb.StateCode_Abnormal)

		req := &milvuspb.AddFileResourceRequest{
			Name: "test_resource",
			Path: "/path/to/resource",
		}

		resp, err := server.AddFileResource(context.Background(), req)
		assert.NoError(t, err)
		assert.Error(t, merr.Error(resp))
	})

	t.Run("allocator error", func(t *testing.T) {
		mockCatalog := mocks.NewDataCoordCatalog(t)
		mockAllocator := tso.NewMockAllocator()
		mockAllocator.GenerateTSOF = func(count uint32) (uint64, error) { return 0, fmt.Errorf("mock error") }

		server := &Server{
			idAllocator: globalIDAllocator.NewTestGlobalIDAllocator(mockAllocator),
			meta: &meta{
				resourceMeta: make(map[string]*internalpb.FileResourceInfo),
				catalog:      mockCatalog,
			},
		}
		server.stateCode.Store(commonpb.StateCode_Healthy)

		req := &milvuspb.AddFileResourceRequest{
			Name: "test_resource",
			Path: "/path/to/resource",
		}

		resp, err := server.AddFileResource(context.Background(), req)
		assert.NoError(t, err)
		assert.Error(t, merr.Error(resp))
	})

	t.Run("catalog save error", func(t *testing.T) {
		mockCatalog := mocks.NewDataCoordCatalog(t)
		mockAllocator := tso.NewMockAllocator()
		mockAllocator.GenerateTSOF = func(count uint32) (uint64, error) { return 100, nil }

		server := &Server{
			idAllocator: globalIDAllocator.NewTestGlobalIDAllocator(mockAllocator),
			meta: &meta{
				resourceMeta: make(map[string]*internalpb.FileResourceInfo),
				catalog:      mockCatalog,
			},
		}
		server.stateCode.Store(commonpb.StateCode_Healthy)

		req := &milvuspb.AddFileResourceRequest{
			Name: "test_resource",
			Path: "/path/to/resource",
		}

		mockCatalog.EXPECT().SaveFileResource(mock.Anything, mock.Anything, mock.Anything).Return(errors.New("catalog error"))

		resp, err := server.AddFileResource(context.Background(), req)
		assert.NoError(t, err)
		assert.Error(t, merr.Error(resp))
	})

	t.Run("resource already exists", func(t *testing.T) {
		mockCatalog := mocks.NewDataCoordCatalog(t)
		mockAllocator := tso.NewMockAllocator()
		mockAllocator.GenerateTSOF = func(count uint32) (uint64, error) { return 100, nil }

		existingResource := &internalpb.FileResourceInfo{
			Id:   1,
			Name: "test_resource",
			Path: "/existing/path",
		}

		server := &Server{
			idAllocator: globalIDAllocator.NewTestGlobalIDAllocator(mockAllocator),
			meta: &meta{
				resourceMeta: map[string]*internalpb.FileResourceInfo{
					"test_resource": existingResource,
				},
				catalog: mockCatalog,
			},
		}
		server.stateCode.Store(commonpb.StateCode_Healthy)

		req := &milvuspb.AddFileResourceRequest{
			Name: "test_resource",
			Path: "/path/to/resource",
		}

		resp, err := server.AddFileResource(context.Background(), req)
		assert.NoError(t, err)
		assert.Error(t, merr.Error(resp))
		assert.Contains(t, resp.GetReason(), "resource name exist")
	})
}

func TestServer_RemoveFileResource(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		mockCatalog := mocks.NewDataCoordCatalog(t)

		existingResource := &internalpb.FileResourceInfo{
			Id:   1,
			Name: "test_resource",
			Path: "/path/to/resource",
		}

		server := &Server{
			meta: &meta{
				resourceMeta: map[string]*internalpb.FileResourceInfo{
					"test_resource": existingResource,
				},
				catalog: mockCatalog,
			},
			mixCoord: newMockMixCoord(),
		}
		server.stateCode.Store(commonpb.StateCode_Healthy)

		req := &milvuspb.RemoveFileResourceRequest{
			Base: &commonpb.MsgBase{},
			Name: "test_resource",
		}

		mockCatalog.EXPECT().RemoveFileResource(mock.Anything, mock.Anything, mock.Anything).Return(nil)

		resp, err := server.RemoveFileResource(context.Background(), req)
		assert.NoError(t, err)
		assert.NoError(t, merr.Error(resp))
	})

	t.Run("server not healthy", func(t *testing.T) {
		server := &Server{}
		server.stateCode.Store(commonpb.StateCode_Abnormal)

		req := &milvuspb.RemoveFileResourceRequest{
			Name: "test_resource",
		}

		resp, err := server.RemoveFileResource(context.Background(), req)
		assert.NoError(t, err)
		assert.Error(t, merr.Error(resp))
	})

	t.Run("resource not found", func(t *testing.T) {
		mockCatalog := mocks.NewDataCoordCatalog(t)

		server := &Server{
			meta: &meta{
				resourceMeta: make(map[string]*internalpb.FileResourceInfo),
				catalog:      mockCatalog,
			},
			mixCoord: newMockMixCoord(),
		}
		server.stateCode.Store(commonpb.StateCode_Healthy)

		req := &milvuspb.RemoveFileResourceRequest{
			Name: "non_existent_resource",
		}

		resp, err := server.RemoveFileResource(context.Background(), req)
		assert.NoError(t, err)
		assert.NoError(t, merr.Error(resp)) // Should succeed even if resource doesn't exist
	})

	t.Run("catalog remove error", func(t *testing.T) {
		mockCatalog := mocks.NewDataCoordCatalog(t)

		existingResource := &internalpb.FileResourceInfo{
			Id:   1,
			Name: "test_resource",
			Path: "/path/to/resource",
		}

		server := &Server{
			meta: &meta{
				resourceMeta: map[string]*internalpb.FileResourceInfo{
					"test_resource": existingResource,
				},
				catalog: mockCatalog,
			},
		}
		server.stateCode.Store(commonpb.StateCode_Healthy)

		req := &milvuspb.RemoveFileResourceRequest{
			Name: "test_resource",
		}

		mockCatalog.EXPECT().RemoveFileResource(mock.Anything, int64(1), mock.Anything).Return(errors.New("catalog error"))

		resp, err := server.RemoveFileResource(context.Background(), req)
		assert.NoError(t, err)
		assert.Error(t, merr.Error(resp))
	})
}

func TestServer_ListFileResources(t *testing.T) {
	t.Run("success with empty list", func(t *testing.T) {
		mockCatalog := mocks.NewDataCoordCatalog(t)

		server := &Server{
			meta: &meta{
				resourceMeta: make(map[string]*internalpb.FileResourceInfo),
				catalog:      mockCatalog,
			},
		}
		server.stateCode.Store(commonpb.StateCode_Healthy)

		req := &milvuspb.ListFileResourcesRequest{
			Base: &commonpb.MsgBase{},
		}

		resp, err := server.ListFileResources(context.Background(), req)
		assert.NoError(t, err)
		assert.NoError(t, merr.Error(resp.GetStatus()))
		assert.NotNil(t, resp.GetResources())
		assert.Equal(t, 0, len(resp.GetResources()))
	})

	t.Run("success with resources", func(t *testing.T) {
		mockCatalog := mocks.NewDataCoordCatalog(t)

		resource1 := &internalpb.FileResourceInfo{
			Id:   1,
			Name: "resource1",
			Path: "/path/to/resource1",
		}
		resource2 := &internalpb.FileResourceInfo{
			Id:   2,
			Name: "resource2",
			Path: "/path/to/resource2",
		}

		server := &Server{
			meta: &meta{
				resourceMeta: map[string]*internalpb.FileResourceInfo{
					"resource1": resource1,
					"resource2": resource2,
				},
				catalog: mockCatalog,
			},
		}
		server.stateCode.Store(commonpb.StateCode_Healthy)

		req := &milvuspb.ListFileResourcesRequest{}

		resp, err := server.ListFileResources(context.Background(), req)
		assert.NoError(t, err)
		assert.NoError(t, merr.Error(resp.GetStatus()))
		assert.NotNil(t, resp.GetResources())
		assert.Equal(t, 2, len(resp.GetResources()))

		// Check that both resources are returned
		resourceNames := make(map[string]bool)
		for _, resource := range resp.GetResources() {
			resourceNames[resource.GetName()] = true
		}
		assert.True(t, resourceNames["resource1"])
		assert.True(t, resourceNames["resource2"])
	})

	t.Run("server not healthy", func(t *testing.T) {
		server := &Server{}
		server.stateCode.Store(commonpb.StateCode_Abnormal)

		req := &milvuspb.ListFileResourcesRequest{}

		resp, err := server.ListFileResources(context.Background(), req)
		assert.NoError(t, err)
		assert.Error(t, merr.Error(resp.GetStatus()))
	})
}

// createTestFlushAllServer creates a test server for FlushAll tests
func createTestFlushAllServer() *Server {
	// Create a mock allocator that will be replaced by mockey
	mockAlloc := &allocator.MockAllocator{}
	mockBroker := &broker.MockBroker{}

	server := &Server{
		allocator: mockAlloc,
		broker:    mockBroker,
		meta: &meta{
			collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
			channelCPs:  newChannelCps(),
			segments:    NewSegmentsInfo(),
		},
		// handler will be set to a mock in individual tests when needed
	}
	server.stateCode.Store(commonpb.StateCode_Healthy)

	return server
}

func TestServer_FlushAll(t *testing.T) {
	t.Run("server not healthy", func(t *testing.T) {
		server := &Server{}
		server.stateCode.Store(commonpb.StateCode_Abnormal)

		req := &datapb.FlushAllRequest{}
		resp, err := server.FlushAll(context.Background(), req)

		assert.NoError(t, err)
		assert.Error(t, merr.Error(resp.GetStatus()))
	})

	t.Run("allocator error", func(t *testing.T) {
		server := createTestFlushAllServer()

		// Mock allocator AllocTimestamp to return error
		mockAllocTimestamp := mockey.Mock(mockey.GetMethod(server.allocator, "AllocTimestamp")).Return(uint64(0), errors.New("alloc error")).Build()
		defer mockAllocTimestamp.UnPatch()

		req := &datapb.FlushAllRequest{}
		resp, err := server.FlushAll(context.Background(), req)

		assert.Error(t, err)
		assert.Nil(t, resp)
	})

	t.Run("broker ListDatabases error", func(t *testing.T) {
		server := createTestFlushAllServer()

		// Mock allocator AllocTimestamp
		mockAllocTimestamp := mockey.Mock(mockey.GetMethod(server.allocator, "AllocTimestamp")).Return(uint64(12345), nil).Build()
		defer mockAllocTimestamp.UnPatch()

		// Mock broker ListDatabases to return error
		mockListDatabases := mockey.Mock(mockey.GetMethod(server.broker, "ListDatabases")).Return(nil, errors.New("list databases error")).Build()
		defer mockListDatabases.UnPatch()

		req := &datapb.FlushAllRequest{} // No specific targets, should list all databases

		resp, err := server.FlushAll(context.Background(), req)

		assert.NoError(t, err)
		assert.Error(t, merr.Error(resp.GetStatus()))
	})

	t.Run("broker ShowCollectionIDs error", func(t *testing.T) {
		server := createTestFlushAllServer()

		// Mock allocator AllocTimestamp
		mockAllocTimestamp := mockey.Mock(mockey.GetMethod(server.allocator, "AllocTimestamp")).Return(uint64(12345), nil).Build()
		defer mockAllocTimestamp.UnPatch()

		// Mock broker ShowCollectionIDs to return error
		mockShowCollectionIDs := mockey.Mock(mockey.GetMethod(server.broker, "ShowCollectionIDs")).Return(nil, errors.New("broker error")).Build()
		defer mockShowCollectionIDs.UnPatch()

		req := &datapb.FlushAllRequest{
			DbName: "test-db",
		}

		resp, err := server.FlushAll(context.Background(), req)

		assert.NoError(t, err)
		assert.Error(t, merr.Error(resp.GetStatus()))
	})

	t.Run("empty collections in database", func(t *testing.T) {
		server := createTestFlushAllServer()

		// Mock allocator AllocTimestamp
		mockAllocTimestamp := mockey.Mock(mockey.GetMethod(server.allocator, "AllocTimestamp")).Return(uint64(12345), nil).Build()
		defer mockAllocTimestamp.UnPatch()

		// Mock broker ShowCollectionIDs returns empty collections
		mockShowCollectionIDs := mockey.Mock(mockey.GetMethod(server.broker, "ShowCollectionIDs")).Return(&rootcoordpb.ShowCollectionIDsResponse{
			Status: merr.Success(),
			DbCollections: []*rootcoordpb.DBCollections{
				{
					DbName:        "empty-db",
					CollectionIDs: []int64{}, // Empty collections
				},
			},
		}, nil).Build()
		defer mockShowCollectionIDs.UnPatch()

		req := &datapb.FlushAllRequest{
			DbName: "empty-db",
		}

		resp, err := server.FlushAll(context.Background(), req)

		assert.NoError(t, err)
		assert.NoError(t, merr.Error(resp.GetStatus()))
		assert.Equal(t, uint64(12345), resp.GetFlushTs())
		assert.Equal(t, 0, len(resp.GetFlushResults()))
	})

	t.Run("flush specific database successfully", func(t *testing.T) {
		server := createTestFlushAllServer()
		server.handler = NewNMockHandler(t) // Initialize handler with testing.T

		// Mock allocator AllocTimestamp
		mockAllocTimestamp := mockey.Mock(mockey.GetMethod(server.allocator, "AllocTimestamp")).Return(uint64(12345), nil).Build()
		defer mockAllocTimestamp.UnPatch()

		// Mock broker ShowCollectionIDs
		mockShowCollectionIDs := mockey.Mock(mockey.GetMethod(server.broker, "ShowCollectionIDs")).Return(&rootcoordpb.ShowCollectionIDsResponse{
			Status: merr.Success(),
			DbCollections: []*rootcoordpb.DBCollections{
				{
					DbName:        "test-db",
					CollectionIDs: []int64{100, 101},
				},
			},
		}, nil).Build()
		defer mockShowCollectionIDs.UnPatch()

		// Add collections to server meta with collection names
		server.meta.AddCollection(&collectionInfo{
			ID: 100,
			Schema: &schemapb.CollectionSchema{
				Name: "collection1",
			},
			VChannelNames: []string{"channel1"},
		})
		server.meta.AddCollection(&collectionInfo{
			ID: 101,
			Schema: &schemapb.CollectionSchema{
				Name: "collection2",
			},
			VChannelNames: []string{"channel2"},
		})

		// Mock handler GetCollection to return collection info
		mockGetCollection := mockey.Mock(mockey.GetMethod(server.handler, "GetCollection")).To(func(ctx context.Context, collectionID int64) (*collectionInfo, error) {
			if collectionID == 100 {
				return &collectionInfo{
					ID: 100,
					Schema: &schemapb.CollectionSchema{
						Name: "collection1",
					},
				}, nil
			} else if collectionID == 101 {
				return &collectionInfo{
					ID: 101,
					Schema: &schemapb.CollectionSchema{
						Name: "collection2",
					},
				}, nil
			}
			return nil, errors.New("collection not found")
		}).Build()
		defer mockGetCollection.UnPatch()

		// Mock flushCollection to return success results
		mockFlushCollection := mockey.Mock(mockey.GetMethod(server, "flushCollection")).To(func(ctx context.Context, collectionID int64, flushTs uint64, toFlushSegments []int64) (*datapb.FlushResult, error) {
			var collectionName string
			if collectionID == 100 {
				collectionName = "collection1"
			} else if collectionID == 101 {
				collectionName = "collection2"
			}

			return &datapb.FlushResult{
				CollectionID:    collectionID,
				DbName:          "test-db",
				CollectionName:  collectionName,
				SegmentIDs:      []int64{1000 + collectionID, 2000 + collectionID},
				FlushSegmentIDs: []int64{1000 + collectionID, 2000 + collectionID},
				TimeOfSeal:      12300,
				FlushTs:         flushTs,
				ChannelCps:      make(map[string]*msgpb.MsgPosition),
			}, nil
		}).Build()
		defer mockFlushCollection.UnPatch()

		req := &datapb.FlushAllRequest{
			DbName: "test-db",
		}

		resp, err := server.FlushAll(context.Background(), req)

		assert.NoError(t, err)
		assert.NoError(t, merr.Error(resp.GetStatus()))
		assert.Equal(t, uint64(12345), resp.GetFlushTs())
		assert.Equal(t, 2, len(resp.GetFlushResults()))

		// Verify flush results
		resultMap := make(map[int64]*datapb.FlushResult)
		for _, result := range resp.GetFlushResults() {
			resultMap[result.GetCollectionID()] = result
		}

		assert.Contains(t, resultMap, int64(100))
		assert.Contains(t, resultMap, int64(101))
		assert.Equal(t, "test-db", resultMap[100].GetDbName())
		assert.Equal(t, "collection1", resultMap[100].GetCollectionName())
		assert.Equal(t, "collection2", resultMap[101].GetCollectionName())
	})

	t.Run("flush with specific flush targets successfully", func(t *testing.T) {
		server := createTestFlushAllServer()
		server.handler = NewNMockHandler(t) // Initialize handler with testing.T

		// Mock allocator AllocTimestamp
		mockAllocTimestamp := mockey.Mock(mockey.GetMethod(server.allocator, "AllocTimestamp")).Return(uint64(12345), nil).Build()
		defer mockAllocTimestamp.UnPatch()

		// Mock broker ShowCollectionIDs
		mockShowCollectionIDs := mockey.Mock(mockey.GetMethod(server.broker, "ShowCollectionIDs")).Return(&rootcoordpb.ShowCollectionIDsResponse{
			Status: merr.Success(),
			DbCollections: []*rootcoordpb.DBCollections{
				{
					DbName:        "test-db",
					CollectionIDs: []int64{100, 101},
				},
			},
		}, nil).Build()
		defer mockShowCollectionIDs.UnPatch()

		// Add collections to server meta with collection names
		server.meta.AddCollection(&collectionInfo{
			ID: 100,
			Schema: &schemapb.CollectionSchema{
				Name: "target-collection",
			},
			VChannelNames: []string{"channel1"},
		})
		server.meta.AddCollection(&collectionInfo{
			ID: 101,
			Schema: &schemapb.CollectionSchema{
				Name: "other-collection",
			},
			VChannelNames: []string{"channel2"},
		})

		// Mock handler GetCollection to return collection info
		mockGetCollection := mockey.Mock(mockey.GetMethod(server.handler, "GetCollection")).To(func(ctx context.Context, collectionID int64) (*collectionInfo, error) {
			if collectionID == 100 {
				return &collectionInfo{
					ID: 100,
					Schema: &schemapb.CollectionSchema{
						Name: "target-collection",
					},
				}, nil
			} else if collectionID == 101 {
				return &collectionInfo{
					ID: 101,
					Schema: &schemapb.CollectionSchema{
						Name: "other-collection",
					},
				}, nil
			}
			return nil, errors.New("collection not found")
		}).Build()
		defer mockGetCollection.UnPatch()

		// Mock flushCollection to return success result
		mockFlushCollection := mockey.Mock(mockey.GetMethod(server, "flushCollection")).To(func(ctx context.Context, collectionID int64, flushTs uint64, toFlushSegments []int64) (*datapb.FlushResult, error) {
			return &datapb.FlushResult{
				CollectionID:    collectionID,
				DbName:          "test-db",
				CollectionName:  "target-collection",
				SegmentIDs:      []int64{1100, 2100},
				FlushSegmentIDs: []int64{1100, 2100},
				TimeOfSeal:      12300,
				FlushTs:         flushTs,
				ChannelCps:      make(map[string]*msgpb.MsgPosition),
			}, nil
		}).Build()
		defer mockFlushCollection.UnPatch()

		req := &datapb.FlushAllRequest{
			FlushTargets: []*datapb.FlushAllTarget{
				{
					DbName:        "test-db",
					CollectionIds: []int64{100},
				},
			},
		}

		resp, err := server.FlushAll(context.Background(), req)

		assert.NoError(t, err)
		assert.NoError(t, merr.Error(resp.GetStatus()))
		assert.Equal(t, uint64(12345), resp.GetFlushTs())
		assert.Equal(t, 1, len(resp.GetFlushResults()))

		// Verify only the target collection was flushed
		result := resp.GetFlushResults()[0]
		assert.Equal(t, int64(100), result.GetCollectionID())
		assert.Equal(t, "test-db", result.GetDbName())
		assert.Equal(t, "target-collection", result.GetCollectionName())
		assert.Equal(t, []int64{1100, 2100}, result.GetSegmentIDs())
		assert.Equal(t, []int64{1100, 2100}, result.GetFlushSegmentIDs())
	})

	t.Run("flush all databases successfully", func(t *testing.T) {
		server := createTestFlushAllServer()
		server.handler = NewNMockHandler(t) // Initialize handler with testing.T

		// Mock allocator AllocTimestamp
		mockAllocTimestamp := mockey.Mock(mockey.GetMethod(server.allocator, "AllocTimestamp")).Return(uint64(12345), nil).Build()
		defer mockAllocTimestamp.UnPatch()

		// Mock broker ListDatabases
		mockListDatabases := mockey.Mock(mockey.GetMethod(server.broker, "ListDatabases")).Return(&milvuspb.ListDatabasesResponse{
			Status:  merr.Success(),
			DbNames: []string{"db1", "db2"},
		}, nil).Build()
		defer mockListDatabases.UnPatch()

		// Mock broker ShowCollectionIDs for different databases
		mockShowCollectionIDs := mockey.Mock(mockey.GetMethod(server.broker, "ShowCollectionIDs")).To(func(ctx context.Context, dbNames ...string) (*rootcoordpb.ShowCollectionIDsResponse, error) {
			if len(dbNames) == 0 {
				return nil, errors.New("no database names provided")
			}
			dbName := dbNames[0] // Use the first database name
			if dbName == "db1" {
				return &rootcoordpb.ShowCollectionIDsResponse{
					Status: merr.Success(),
					DbCollections: []*rootcoordpb.DBCollections{
						{
							DbName:        "db1",
							CollectionIDs: []int64{100},
						},
					},
				}, nil
			}
			if dbName == "db2" {
				return &rootcoordpb.ShowCollectionIDsResponse{
					Status: merr.Success(),
					DbCollections: []*rootcoordpb.DBCollections{
						{
							DbName:        "db2",
							CollectionIDs: []int64{200},
						},
					},
				}, nil
			}
			return nil, errors.New("unknown database")
		}).Build()
		defer mockShowCollectionIDs.UnPatch()

		// Add collections to server meta with collection names
		server.meta.AddCollection(&collectionInfo{
			ID: 100,
			Schema: &schemapb.CollectionSchema{
				Name: "collection1",
			},
			VChannelNames: []string{"channel1"},
		})
		server.meta.AddCollection(&collectionInfo{
			ID: 200,
			Schema: &schemapb.CollectionSchema{
				Name: "collection2",
			},
			VChannelNames: []string{"channel2"},
		})

		// Mock handler GetCollection to return collection info
		mockGetCollection := mockey.Mock(mockey.GetMethod(server.handler, "GetCollection")).To(func(ctx context.Context, collectionID int64) (*collectionInfo, error) {
			if collectionID == 100 {
				return &collectionInfo{
					ID: 100,
					Schema: &schemapb.CollectionSchema{
						Name: "collection1",
					},
				}, nil
			} else if collectionID == 200 {
				return &collectionInfo{
					ID: 200,
					Schema: &schemapb.CollectionSchema{
						Name: "collection2",
					},
				}, nil
			}
			return nil, errors.New("collection not found")
		}).Build()
		defer mockGetCollection.UnPatch()

		// Mock flushCollection for different collections
		mockFlushCollection := mockey.Mock(mockey.GetMethod(server, "flushCollection")).To(func(ctx context.Context, collectionID int64, flushTs uint64, toFlushSegments []int64) (*datapb.FlushResult, error) {
			var dbName, collectionName string
			if collectionID == 100 {
				dbName = "db1"
				collectionName = "collection1"
			} else if collectionID == 200 {
				dbName = "db2"
				collectionName = "collection2"
			}

			return &datapb.FlushResult{
				CollectionID:    collectionID,
				DbName:          dbName,
				CollectionName:  collectionName,
				SegmentIDs:      []int64{collectionID + 1000, collectionID + 2000},
				FlushSegmentIDs: []int64{collectionID + 1000, collectionID + 2000},
				TimeOfSeal:      12300,
				FlushTs:         flushTs,
				ChannelCps:      make(map[string]*msgpb.MsgPosition),
			}, nil
		}).Build()
		defer mockFlushCollection.UnPatch()

		req := &datapb.FlushAllRequest{} // No specific targets, flush all databases

		resp, err := server.FlushAll(context.Background(), req)

		assert.NoError(t, err)
		assert.NoError(t, merr.Error(resp.GetStatus()))
		assert.Equal(t, uint64(12345), resp.GetFlushTs())
		assert.Equal(t, 2, len(resp.GetFlushResults()))

		// Verify results from both databases
		resultMap := make(map[string]*datapb.FlushResult)
		for _, result := range resp.GetFlushResults() {
			resultMap[result.GetDbName()] = result
		}

		assert.Contains(t, resultMap, "db1")
		assert.Contains(t, resultMap, "db2")
		assert.Equal(t, int64(100), resultMap["db1"].GetCollectionID())
		assert.Equal(t, int64(200), resultMap["db2"].GetCollectionID())
	})
}

// createTestGetFlushAllStateServer creates a test server for GetFlushAllState tests
func createTestGetFlushAllStateServer() *Server {
	// Create a mock broker that will be replaced by mockey
	mockBroker := &broker.MockBroker{}

	server := &Server{
		broker: mockBroker,
		meta: &meta{
			channelCPs: newChannelCps(),
		},
	}
	server.stateCode.Store(commonpb.StateCode_Healthy)

	return server
}

func TestServer_GetFlushAllState(t *testing.T) {
	t.Run("server not healthy", func(t *testing.T) {
		server := &Server{}
		server.stateCode.Store(commonpb.StateCode_Abnormal)

		req := &milvuspb.GetFlushAllStateRequest{
			FlushAllTs: 12345,
		}
		resp, err := server.GetFlushAllState(context.Background(), req)

		assert.NoError(t, err)
		assert.Error(t, merr.Error(resp.GetStatus()))
	})

	t.Run("ListDatabases error", func(t *testing.T) {
		server := createTestGetFlushAllStateServer()

		// Mock ListDatabases error
		mockListDatabases := mockey.Mock(mockey.GetMethod(server.broker, "ListDatabases")).Return(nil, errors.New("list databases error")).Build()
		defer mockListDatabases.UnPatch()

		req := &milvuspb.GetFlushAllStateRequest{
			FlushAllTs: 12345,
		}

		resp, err := server.GetFlushAllState(context.Background(), req)

		assert.NoError(t, err)
		assert.Error(t, merr.Error(resp.GetStatus()))
	})

	t.Run("check all databases", func(t *testing.T) {
		server := createTestGetFlushAllStateServer()

		// Mock ListDatabases
		mockListDatabases := mockey.Mock(mockey.GetMethod(server.broker, "ListDatabases")).Return(&milvuspb.ListDatabasesResponse{
			Status:  merr.Success(),
			DbNames: []string{"db1", "db2"},
		}, nil).Build()
		defer mockListDatabases.UnPatch()

		// Mock ShowCollections for db1
		mockShowCollections := mockey.Mock(mockey.GetMethod(server.broker, "ShowCollections")).To(func(ctx context.Context, dbName string) (*milvuspb.ShowCollectionsResponse, error) {
			if dbName == "db1" {
				return &milvuspb.ShowCollectionsResponse{
					Status:          merr.Success(),
					CollectionIds:   []int64{100},
					CollectionNames: []string{"collection1"},
				}, nil
			}
			if dbName == "db2" {
				return &milvuspb.ShowCollectionsResponse{
					Status:          merr.Success(),
					CollectionIds:   []int64{200},
					CollectionNames: []string{"collection2"},
				}, nil
			}
			return nil, errors.New("unknown db")
		}).Build()
		defer mockShowCollections.UnPatch()

		// Mock DescribeCollectionInternal
		mockDescribeCollection := mockey.Mock(mockey.GetMethod(server.broker, "DescribeCollectionInternal")).To(func(ctx context.Context, collectionID int64) (*milvuspb.DescribeCollectionResponse, error) {
			if collectionID == 100 {
				return &milvuspb.DescribeCollectionResponse{
					Status:              merr.Success(),
					VirtualChannelNames: []string{"channel1"},
				}, nil
			}
			if collectionID == 200 {
				return &milvuspb.DescribeCollectionResponse{
					Status:              merr.Success(),
					VirtualChannelNames: []string{"channel2"},
				}, nil
			}
			return nil, errors.New("collection not found")
		}).Build()
		defer mockDescribeCollection.UnPatch()

		// Setup channel checkpoints - both flushed
		server.meta.channelCPs.checkpoints["channel1"] = &msgpb.MsgPosition{Timestamp: 15000}
		server.meta.channelCPs.checkpoints["channel2"] = &msgpb.MsgPosition{Timestamp: 15000}

		req := &milvuspb.GetFlushAllStateRequest{
			FlushAllTs: 12345, // No specific targets, check all databases
		}

		resp, err := server.GetFlushAllState(context.Background(), req)

		assert.NoError(t, err)
		assert.NoError(t, merr.Error(resp.GetStatus()))
		assert.Equal(t, 2, len(resp.GetFlushStates()))

		// Check both databases are present
		dbNames := make(map[string]bool)
		for _, flushState := range resp.GetFlushStates() {
			dbNames[flushState.GetDbName()] = true
		}
		assert.True(t, dbNames["db1"])
		assert.True(t, dbNames["db2"])
		assert.True(t, resp.GetFlushed()) // Overall flushed
	})

	t.Run("channel checkpoint not found", func(t *testing.T) {
		server := createTestGetFlushAllStateServer()

		// Mock ListDatabases
		mockListDatabases := mockey.Mock(mockey.GetMethod(server.broker, "ListDatabases")).Return(&milvuspb.ListDatabasesResponse{
			Status:  merr.Success(),
			DbNames: []string{"test-db"},
		}, nil).Build()
		defer mockListDatabases.UnPatch()

		// Mock ShowCollections
		mockShowCollections := mockey.Mock(mockey.GetMethod(server.broker, "ShowCollections")).Return(&milvuspb.ShowCollectionsResponse{
			Status:          merr.Success(),
			CollectionIds:   []int64{100},
			CollectionNames: []string{"collection1"},
		}, nil).Build()
		defer mockShowCollections.UnPatch()

		// Mock DescribeCollectionInternal
		mockDescribeCollection := mockey.Mock(mockey.GetMethod(server.broker, "DescribeCollectionInternal")).Return(&milvuspb.DescribeCollectionResponse{
			Status:              merr.Success(),
			VirtualChannelNames: []string{"channel1"},
		}, nil).Build()
		defer mockDescribeCollection.UnPatch()

		// No channel checkpoint set - should be considered not flushed

		req := &milvuspb.GetFlushAllStateRequest{
			FlushAllTs: 12345,
			DbName:     "test-db",
		}

		resp, err := server.GetFlushAllState(context.Background(), req)

		assert.NoError(t, err)
		assert.NoError(t, merr.Error(resp.GetStatus()))
		assert.Equal(t, 1, len(resp.GetFlushStates()))

		flushState := resp.GetFlushStates()[0]
		assert.Equal(t, "test-db", flushState.GetDbName())
		assert.Equal(t, 1, len(flushState.GetCollectionFlushStates()))
		assert.False(t, flushState.GetCollectionFlushStates()["collection1"]) // Not flushed
		assert.False(t, resp.GetFlushed())                                    // Overall not flushed
	})

	t.Run("channel checkpoint timestamp too low", func(t *testing.T) {
		server := createTestGetFlushAllStateServer()

		// Mock ListDatabases
		mockListDatabases := mockey.Mock(mockey.GetMethod(server.broker, "ListDatabases")).Return(&milvuspb.ListDatabasesResponse{
			Status:  merr.Success(),
			DbNames: []string{"test-db"},
		}, nil).Build()
		defer mockListDatabases.UnPatch()

		// Mock ShowCollections
		mockShowCollections := mockey.Mock(mockey.GetMethod(server.broker, "ShowCollections")).Return(&milvuspb.ShowCollectionsResponse{
			Status:          merr.Success(),
			CollectionIds:   []int64{100},
			CollectionNames: []string{"collection1"},
		}, nil).Build()
		defer mockShowCollections.UnPatch()

		// Mock DescribeCollectionInternal
		mockDescribeCollection := mockey.Mock(mockey.GetMethod(server.broker, "DescribeCollectionInternal")).Return(&milvuspb.DescribeCollectionResponse{
			Status:              merr.Success(),
			VirtualChannelNames: []string{"channel1"},
		}, nil).Build()
		defer mockDescribeCollection.UnPatch()

		// Setup channel checkpoint with timestamp lower than FlushAllTs
		server.meta.channelCPs.checkpoints["channel1"] = &msgpb.MsgPosition{Timestamp: 10000}

		req := &milvuspb.GetFlushAllStateRequest{
			FlushAllTs: 12345,
			DbName:     "test-db",
		}

		resp, err := server.GetFlushAllState(context.Background(), req)

		assert.NoError(t, err)
		assert.NoError(t, merr.Error(resp.GetStatus()))
		assert.Equal(t, 1, len(resp.GetFlushStates()))

		flushState := resp.GetFlushStates()[0]
		assert.Equal(t, "test-db", flushState.GetDbName())
		assert.Equal(t, 1, len(flushState.GetCollectionFlushStates()))
		assert.False(t, flushState.GetCollectionFlushStates()["collection1"]) // Not flushed
		assert.False(t, resp.GetFlushed())                                    // Overall not flushed
	})

	t.Run("specific database flushed successfully", func(t *testing.T) {
		server := createTestGetFlushAllStateServer()

		// Mock ListDatabases (called even when DbName is specified)
		mockListDatabases := mockey.Mock(mockey.GetMethod(server.broker, "ListDatabases")).Return(&milvuspb.ListDatabasesResponse{
			Status:  merr.Success(),
			DbNames: []string{"test-db"},
		}, nil).Build()
		defer mockListDatabases.UnPatch()

		// Mock ShowCollections for specific database
		mockShowCollections := mockey.Mock(mockey.GetMethod(server.broker, "ShowCollections")).Return(&milvuspb.ShowCollectionsResponse{
			Status:          merr.Success(),
			CollectionIds:   []int64{100, 101},
			CollectionNames: []string{"collection1", "collection2"},
		}, nil).Build()
		defer mockShowCollections.UnPatch()

		// Mock DescribeCollectionInternal
		mockDescribeCollection := mockey.Mock(mockey.GetMethod(server.broker, "DescribeCollectionInternal")).To(func(ctx context.Context, collectionID int64) (*milvuspb.DescribeCollectionResponse, error) {
			if collectionID == 100 {
				return &milvuspb.DescribeCollectionResponse{
					Status:              merr.Success(),
					VirtualChannelNames: []string{"channel1"},
				}, nil
			}
			if collectionID == 101 {
				return &milvuspb.DescribeCollectionResponse{
					Status:              merr.Success(),
					VirtualChannelNames: []string{"channel2"},
				}, nil
			}
			return nil, errors.New("collection not found")
		}).Build()
		defer mockDescribeCollection.UnPatch()

		// Setup channel checkpoints - both flushed (timestamps higher than FlushAllTs)
		server.meta.channelCPs.checkpoints["channel1"] = &msgpb.MsgPosition{Timestamp: 15000}
		server.meta.channelCPs.checkpoints["channel2"] = &msgpb.MsgPosition{Timestamp: 16000}

		req := &milvuspb.GetFlushAllStateRequest{
			FlushAllTs: 12345,
			DbName:     "test-db",
		}

		resp, err := server.GetFlushAllState(context.Background(), req)

		assert.NoError(t, err)
		assert.NoError(t, merr.Error(resp.GetStatus()))
		assert.Equal(t, 1, len(resp.GetFlushStates()))

		flushState := resp.GetFlushStates()[0]
		assert.Equal(t, "test-db", flushState.GetDbName())
		assert.Equal(t, 2, len(flushState.GetCollectionFlushStates()))
		assert.True(t, flushState.GetCollectionFlushStates()["collection1"]) // Flushed
		assert.True(t, flushState.GetCollectionFlushStates()["collection2"]) // Flushed
		assert.True(t, resp.GetFlushed())                                    // Overall flushed
	})

	t.Run("check with flush targets successfully", func(t *testing.T) {
		server := createTestGetFlushAllStateServer()

		// Mock ListDatabases (called even when FlushTargets are specified)
		mockListDatabases := mockey.Mock(mockey.GetMethod(server.broker, "ListDatabases")).Return(&milvuspb.ListDatabasesResponse{
			Status:  merr.Success(),
			DbNames: []string{"test-db"},
		}, nil).Build()
		defer mockListDatabases.UnPatch()

		// Mock ShowCollections for specific database
		mockShowCollections := mockey.Mock(mockey.GetMethod(server.broker, "ShowCollections")).Return(&milvuspb.ShowCollectionsResponse{
			Status:          merr.Success(),
			CollectionIds:   []int64{100, 101},
			CollectionNames: []string{"target-collection", "other-collection"},
		}, nil).Build()
		defer mockShowCollections.UnPatch()

		// Mock DescribeCollectionInternal
		mockDescribeCollection := mockey.Mock(mockey.GetMethod(server.broker, "DescribeCollectionInternal")).To(func(ctx context.Context, collectionID int64) (*milvuspb.DescribeCollectionResponse, error) {
			if collectionID == 100 {
				return &milvuspb.DescribeCollectionResponse{
					Status:              merr.Success(),
					VirtualChannelNames: []string{"channel1"},
				}, nil
			}
			if collectionID == 101 {
				return &milvuspb.DescribeCollectionResponse{
					Status:              merr.Success(),
					VirtualChannelNames: []string{"channel2"},
				}, nil
			}
			return nil, errors.New("collection not found")
		}).Build()
		defer mockDescribeCollection.UnPatch()

		// Setup channel checkpoints - target collection flushed, other not checked
		server.meta.channelCPs.checkpoints["channel1"] = &msgpb.MsgPosition{Timestamp: 15000}
		server.meta.channelCPs.checkpoints["channel2"] = &msgpb.MsgPosition{Timestamp: 10000} // Won't be checked due to filtering

		req := &milvuspb.GetFlushAllStateRequest{
			FlushAllTs: 12345,
			FlushTargets: []*milvuspb.FlushAllTarget{
				{
					DbName:          "test-db",
					CollectionNames: []string{"target-collection"},
				},
			},
		}

		resp, err := server.GetFlushAllState(context.Background(), req)

		assert.NoError(t, err)
		assert.NoError(t, merr.Error(resp.GetStatus()))
		assert.Equal(t, 1, len(resp.GetFlushStates()))

		flushState := resp.GetFlushStates()[0]
		assert.Equal(t, "test-db", flushState.GetDbName())
		assert.Equal(t, 1, len(flushState.GetCollectionFlushStates()))             // Only target collection checked
		assert.True(t, flushState.GetCollectionFlushStates()["target-collection"]) // Flushed
		assert.True(t, resp.GetFlushed())                                          // Overall flushed (only checking target collection)
	})

	t.Run("mixed flush states - partial success", func(t *testing.T) {
		server := createTestGetFlushAllStateServer()

		// Mock ListDatabases
		mockListDatabases := mockey.Mock(mockey.GetMethod(server.broker, "ListDatabases")).Return(&milvuspb.ListDatabasesResponse{
			Status:  merr.Success(),
			DbNames: []string{"db1", "db2"},
		}, nil).Build()
		defer mockListDatabases.UnPatch()

		// Mock ShowCollections for different databases
		mockShowCollections := mockey.Mock(mockey.GetMethod(server.broker, "ShowCollections")).To(func(ctx context.Context, dbName string) (*milvuspb.ShowCollectionsResponse, error) {
			if dbName == "db1" {
				return &milvuspb.ShowCollectionsResponse{
					Status:          merr.Success(),
					CollectionIds:   []int64{100},
					CollectionNames: []string{"collection1"},
				}, nil
			}
			if dbName == "db2" {
				return &milvuspb.ShowCollectionsResponse{
					Status:          merr.Success(),
					CollectionIds:   []int64{200},
					CollectionNames: []string{"collection2"},
				}, nil
			}
			return nil, errors.New("unknown db")
		}).Build()
		defer mockShowCollections.UnPatch()

		// Mock DescribeCollectionInternal
		mockDescribeCollection := mockey.Mock(mockey.GetMethod(server.broker, "DescribeCollectionInternal")).To(func(ctx context.Context, collectionID int64) (*milvuspb.DescribeCollectionResponse, error) {
			if collectionID == 100 {
				return &milvuspb.DescribeCollectionResponse{
					Status:              merr.Success(),
					VirtualChannelNames: []string{"channel1"},
				}, nil
			}
			if collectionID == 200 {
				return &milvuspb.DescribeCollectionResponse{
					Status:              merr.Success(),
					VirtualChannelNames: []string{"channel2"},
				}, nil
			}
			return nil, errors.New("collection not found")
		}).Build()
		defer mockDescribeCollection.UnPatch()

		// Setup channel checkpoints - db1 flushed, db2 not flushed
		server.meta.channelCPs.checkpoints["channel1"] = &msgpb.MsgPosition{Timestamp: 15000} // Flushed
		server.meta.channelCPs.checkpoints["channel2"] = &msgpb.MsgPosition{Timestamp: 10000} // Not flushed

		req := &milvuspb.GetFlushAllStateRequest{
			FlushAllTs: 12345, // Check all databases
		}

		resp, err := server.GetFlushAllState(context.Background(), req)

		assert.NoError(t, err)
		assert.NoError(t, merr.Error(resp.GetStatus()))
		assert.Equal(t, 2, len(resp.GetFlushStates()))

		// Verify mixed flush states
		stateMap := make(map[string]*milvuspb.FlushAllState)
		for _, state := range resp.GetFlushStates() {
			stateMap[state.GetDbName()] = state
		}

		assert.Contains(t, stateMap, "db1")
		assert.Contains(t, stateMap, "db2")
		assert.True(t, stateMap["db1"].GetCollectionFlushStates()["collection1"])  // db1 flushed
		assert.False(t, stateMap["db2"].GetCollectionFlushStates()["collection2"]) // db2 not flushed
		assert.False(t, resp.GetFlushed())                                         // Overall not flushed due to db2
	})
}

func getWatchKV(t *testing.T) kv.WatchKV {
	rootPath := "/etcd/test/root/" + t.Name()
	kv, err := etcdkv.NewWatchKVFactory(rootPath, &Params.EtcdCfg)
	require.NoError(t, err)

	return kv
}
