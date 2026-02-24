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
	"github.com/milvus-io/milvus/internal/coordinator/snmanager"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	mocks2 "github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/mocks/distributed/mock_streaming"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_balancer"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_broadcaster"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v2/kv"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	types2 "github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metautil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/retry"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
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

	t.Run("flush all successfully", func(t *testing.T) {
		server := createTestFlushAllServer()
		server.handler = NewNMockHandler(t)

		// Mock WAL
		wal := mock_streaming.NewMockWALAccesser(t)
		wal.EXPECT().ControlChannel().Return(funcutil.GetControlChannel("by-dev-rootcoord-dml_0")).Maybe()
		streaming.SetWALForTest(wal)

		// Mock broadcaster
		bapi := mock_broadcaster.NewMockBroadcastAPI(t)
		bapi.EXPECT().Broadcast(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, msg message.BroadcastMutableMessage) (*types2.BroadcastAppendResult, error) {
			results := make(map[string]*message.AppendResult)
			for _, vchannel := range msg.BroadcastHeader().VChannels {
				results[vchannel] = &message.AppendResult{
					MessageID:              rmq.NewRmqID(1),
					TimeTick:               tsoutil.ComposeTSByTime(time.Now(), 0),
					LastConfirmedMessageID: rmq.NewRmqID(1),
				}
			}
			msg.WithBroadcastID(1)
			retry.Do(context.Background(), func() error {
				log.Info("broadcast message", log.FieldMessage(msg))
				return registry.CallMessageAckCallback(context.Background(), msg, results)
			}, retry.AttemptAlways())
			return &types2.BroadcastAppendResult{
				BroadcastID: 1,
				AppendResults: lo.MapValues(results, func(result *message.AppendResult, vchannel string) *types2.AppendResult {
					return &types2.AppendResult{
						MessageID:              result.MessageID,
						TimeTick:               result.TimeTick,
						LastConfirmedMessageID: result.LastConfirmedMessageID,
					}
				}),
			}, nil
		})
		bapi.EXPECT().Close().Return()

		// Register mock broadcaster
		mb := mock_broadcaster.NewMockBroadcaster(t)
		mb.EXPECT().WithResourceKeys(mock.Anything, mock.Anything).Return(bapi, nil)
		mb.EXPECT().Close().Return().Maybe()
		broadcast.ResetBroadcaster()
		broadcast.Register(mb)

		// Register mock balancer
		snmanager.ResetStreamingNodeManager()
		b := mock_balancer.NewMockBalancer(t)
		b.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, callback balancer.WatchChannelAssignmentsCallback) error {
			<-ctx.Done()
			return ctx.Err()
		}).Maybe()
		b.EXPECT().GetLatestChannelAssignment().Return(&balancer.WatchChannelAssignmentsCallbackParam{
			PChannelView: &channel.PChannelView{
				Channels: map[channel.ChannelID]*channel.PChannelMeta{
					{Name: "by-dev-1"}: channel.NewPChannelMeta("by-dev-1", types2.AccessModeRW),
				},
			},
		}, nil)
		b.EXPECT().WaitUntilWALbasedDDLReady(mock.Anything).Return(nil)
		balance.Register(b)

		req := &datapb.FlushAllRequest{}
		resp, err := server.FlushAll(context.Background(), req)

		assert.NoError(t, err)
		assert.NoError(t, merr.Error(resp.GetStatus()))
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

		req := &milvuspb.GetFlushAllStateRequest{}
		resp, err := server.GetFlushAllState(context.Background(), req)

		assert.NoError(t, err)
		assert.Error(t, merr.Error(resp.GetStatus()))
	})

	t.Run("ListDatabases error", func(t *testing.T) {
		server := createTestGetFlushAllStateServer()

		// Mock ListDatabases error
		mockListDatabases := mockey.Mock(mockey.GetMethod(server.broker, "ListDatabases")).Return(nil, errors.New("list databases error")).Build()
		defer mockListDatabases.UnPatch()

		req := &milvuspb.GetFlushAllStateRequest{}

		resp, err := server.GetFlushAllState(context.Background(), req)

		assert.NoError(t, err)
		assert.Error(t, merr.Error(resp.GetStatus()))
	})

	t.Run("all flushed", func(t *testing.T) {
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
			FlushAllTss: map[string]uint64{
				"channel1": 15000,
				"channel2": 15000,
			},
		}

		resp, err := server.GetFlushAllState(context.Background(), req)

		assert.NoError(t, merr.CheckRPCCall(resp, err))
		assert.True(t, resp.GetFlushed())
	})

	t.Run("not flushed, channel checkpoint too old", func(t *testing.T) {
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
			FlushAllTss: map[string]uint64{
				"channel1": 15000,
			},
		}

		resp, err := server.GetFlushAllState(context.Background(), req)

		assert.NoError(t, merr.CheckRPCCall(resp, err))
		assert.False(t, resp.GetFlushed())
	})

	t.Run("test legacy FlushAllTs provided and flushed", func(t *testing.T) {
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

		// Setup channel checkpoint with timestamp >= deprecated FlushAllTs
		server.meta.channelCPs.checkpoints["channel1"] = &msgpb.MsgPosition{Timestamp: 15000}

		req := &milvuspb.GetFlushAllStateRequest{
			FlushAllTs: 15000, // deprecated field
		}

		resp, err := server.GetFlushAllState(context.Background(), req)

		assert.NoError(t, merr.CheckRPCCall(resp, err))
		assert.True(t, resp.GetFlushed())
	})

	t.Run("test legacy FlushAllTs provided and not flushed", func(t *testing.T) {
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

		// Setup channel checkpoint with timestamp < deprecated FlushAllTs
		server.meta.channelCPs.checkpoints["channel1"] = &msgpb.MsgPosition{Timestamp: 10000}

		req := &milvuspb.GetFlushAllStateRequest{
			FlushAllTs: 15000, // deprecated field
		}

		resp, err := server.GetFlushAllState(context.Background(), req)

		assert.NoError(t, merr.CheckRPCCall(resp, err))
		assert.False(t, resp.GetFlushed())
	})
}

func getWatchKV(t *testing.T) kv.WatchKV {
	rootPath := "/etcd/test/root/" + t.Name()
	kv, err := etcdkv.NewWatchKVFactory(rootPath, &Params.EtcdCfg)
	require.NoError(t, err)

	return kv
}

func TestServer_DropSegmentsByTime(t *testing.T) {
	ctx := context.Background()
	collectionID := int64(1)
	channelName := "test-channel"
	flushTs := uint64(1000)

	t.Run("server not healthy", func(t *testing.T) {
		s := &Server{}
		s.stateCode.Store(commonpb.StateCode_Abnormal)
		err := s.DropSegmentsByTime(ctx, collectionID, map[string]uint64{channelName: flushTs})
		assert.Error(t, err)
	})

	t.Run("watch channel checkpoint failed", func(t *testing.T) {
		s := &Server{}
		s.stateCode.Store(commonpb.StateCode_Healthy)

		meta, err := newMemoryMeta(t)
		assert.NoError(t, err)
		s.meta = meta

		// WatchChannelCheckpoint will wait indefinitely, so we use a context with timeout
		ctxWithTimeout, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()

		err = s.DropSegmentsByTime(ctxWithTimeout, collectionID, map[string]uint64{channelName: flushTs})
		assert.Error(t, err)
	})

	t.Run("success - drop segments", func(t *testing.T) {
		s := &Server{}
		s.stateCode.Store(commonpb.StateCode_Healthy)

		meta, err := newMemoryMeta(t)
		assert.NoError(t, err)
		s.meta = meta

		// Set channel checkpoint to satisfy WatchChannelCheckpoint
		pos := &msgpb.MsgPosition{
			ChannelName: channelName,
			MsgID:       []byte{0, 0, 0, 0, 0, 0, 0, 0},
			Timestamp:   flushTs,
		}
		err = meta.UpdateChannelCheckpoint(ctx, channelName, pos)
		assert.NoError(t, err)

		// Add segments to drop (timestamp <= flushTs)
		seg1 := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:           1,
				CollectionID: collectionID,
				State:        commonpb.SegmentState_Flushed,
				DmlPosition: &msgpb.MsgPosition{
					Timestamp: flushTs - 100, // less than flushTs
				},
			},
		}
		err = meta.AddSegment(ctx, seg1)
		assert.NoError(t, err)

		// Add segment that should not be dropped (timestamp > flushTs)
		seg2 := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:           2,
				CollectionID: collectionID,
				State:        commonpb.SegmentState_Flushed,
				DmlPosition: &msgpb.MsgPosition{
					Timestamp: flushTs + 100, // greater than flushTs
				},
			},
		}
		err = meta.AddSegment(ctx, seg2)
		assert.NoError(t, err)

		// Set segment channel
		seg1.InsertChannel = channelName
		seg2.InsertChannel = channelName
		meta.segments.SetSegment(seg1.ID, seg1)
		meta.segments.SetSegment(seg2.ID, seg2)

		err = s.DropSegmentsByTime(ctx, collectionID, map[string]uint64{channelName: flushTs})
		assert.NoError(t, err)

		// Verify segment 1 is dropped
		seg1After := meta.GetSegment(ctx, seg1.ID)
		assert.NotNil(t, seg1After)
		assert.Equal(t, commonpb.SegmentState_Dropped, seg1After.GetState())

		// Verify segment 2 is not dropped
		seg2After := meta.GetSegment(ctx, seg2.ID)
		assert.NotNil(t, seg2After)
		assert.NotEqual(t, commonpb.SegmentState_Dropped, seg2After.GetState())
	})
}

func TestGetSegmentInfo_WithCompaction(t *testing.T) {
	t.Run("use handler.GetDeltaLogFromCompactTo", func(t *testing.T) {
		svr := newTestServer(t)
		defer closeTestServer(t, svr)

		collID := int64(100)
		partID := int64(10)

		// Add collection
		svr.meta.AddCollection(&collectionInfo{
			ID:         collID,
			Partitions: []int64{partID},
		})

		// Create parent segment
		parent := NewSegmentInfo(&datapb.SegmentInfo{
			ID:           1000,
			CollectionID: collID,
			PartitionID:  partID,
			State:        commonpb.SegmentState_Dropped,
		})
		err := svr.meta.AddSegment(context.TODO(), parent)
		require.NoError(t, err)

		// Create child segment with delta logs
		child := NewSegmentInfo(&datapb.SegmentInfo{
			ID:             1001,
			CollectionID:   collID,
			PartitionID:    partID,
			State:          commonpb.SegmentState_Flushed,
			CompactionFrom: []int64{1000},
			NumOfRows:      100,
			Deltalogs: []*datapb.FieldBinlog{
				{
					FieldID: 0,
					Binlogs: []*datapb.Binlog{
						{LogID: 1, LogSize: 100},
					},
				},
			},
		})
		err = svr.meta.AddSegment(context.TODO(), child)
		require.NoError(t, err)

		// Test GetSegmentInfo
		req := &datapb.GetSegmentInfoRequest{
			SegmentIDs:       []int64{1000},
			IncludeUnHealthy: true,
		}

		resp, err := svr.GetSegmentInfo(context.Background(), req)
		assert.NoError(t, err)
		assert.NoError(t, merr.Error(resp.GetStatus()))
		assert.Len(t, resp.GetInfos(), 1)

		// Verify delta logs were merged from child
		info := resp.GetInfos()[0]
		assert.Equal(t, int64(1000), info.GetID())
		assert.NotEmpty(t, info.GetDeltalogs())
	})
}

func TestGetRestoreSnapshotState(t *testing.T) {
	t.Run("job not found", func(t *testing.T) {
		svr := newTestServer(t)
		defer closeTestServer(t, svr)

		req := &datapb.GetRestoreSnapshotStateRequest{
			JobId: 999,
		}

		resp, err := svr.GetRestoreSnapshotState(context.Background(), req)
		assert.NoError(t, err)
		assert.Error(t, merr.Error(resp.GetStatus()))
	})

	t.Run("job in progress", func(t *testing.T) {
		svr := newTestServer(t)
		defer closeTestServer(t, svr)

		// Add a copy job
		job := &copySegmentJob{
			CopySegmentJob: &datapb.CopySegmentJob{
				JobId:          100,
				CollectionId:   1,
				State:          datapb.CopySegmentJobState_CopySegmentJobExecuting,
				TotalSegments:  10,
				CopiedSegments: 5,
				SnapshotName:   "test_snapshot",
				StartTs:        uint64(time.Now().UnixNano()),
			},
			tr: timerecord.NewTimeRecorder("test job"),
		}
		err := svr.copySegmentMeta.AddJob(context.TODO(), job)
		require.NoError(t, err)

		req := &datapb.GetRestoreSnapshotStateRequest{
			JobId: 100,
		}

		resp, err := svr.GetRestoreSnapshotState(context.Background(), req)
		assert.NoError(t, err)
		assert.NoError(t, merr.Error(resp.GetStatus()))
		assert.NotNil(t, resp.GetInfo())
		assert.Equal(t, int64(100), resp.GetInfo().GetJobId())
		assert.Equal(t, "test_snapshot", resp.GetInfo().GetSnapshotName())
		assert.Equal(t, datapb.RestoreSnapshotState_RestoreSnapshotExecuting, resp.GetInfo().GetState())
		assert.Equal(t, int32(50), resp.GetInfo().GetProgress()) // 5/10 * 100 = 50%
	})

	t.Run("job completed", func(t *testing.T) {
		svr := newTestServer(t)
		defer closeTestServer(t, svr)

		startTime := time.Now()
		endTime := startTime.Add(10 * time.Second)

		job := &copySegmentJob{
			CopySegmentJob: &datapb.CopySegmentJob{
				JobId:          200,
				CollectionId:   1,
				State:          datapb.CopySegmentJobState_CopySegmentJobCompleted,
				TotalSegments:  10,
				CopiedSegments: 10,
				SnapshotName:   "completed_snapshot",
				StartTs:        uint64(startTime.UnixNano()),
				CompleteTs:     uint64(endTime.UnixNano()),
			},
			tr: timerecord.NewTimeRecorder("completed job"),
		}
		err := svr.copySegmentMeta.AddJob(context.TODO(), job)
		require.NoError(t, err)

		req := &datapb.GetRestoreSnapshotStateRequest{
			JobId: 200,
		}

		resp, err := svr.GetRestoreSnapshotState(context.Background(), req)
		assert.NoError(t, err)
		assert.NoError(t, merr.Error(resp.GetStatus()))
		assert.NotNil(t, resp.GetInfo())
		assert.Equal(t, datapb.RestoreSnapshotState_RestoreSnapshotCompleted, resp.GetInfo().GetState())
		assert.Equal(t, int32(100), resp.GetInfo().GetProgress()) // 10/10 * 100 = 100%
		assert.Greater(t, resp.GetInfo().GetTimeCost(), uint64(0))
	})
}

func TestListRestoreSnapshotJobs(t *testing.T) {
	t.Run("list all jobs", func(t *testing.T) {
		svr := newTestServer(t)
		defer closeTestServer(t, svr)

		// Add multiple jobs
		for i := 1; i <= 3; i++ {
			job := &copySegmentJob{
				CopySegmentJob: &datapb.CopySegmentJob{
					JobId:          int64(100 + i),
					CollectionId:   1,
					State:          datapb.CopySegmentJobState_CopySegmentJobExecuting,
					TotalSegments:  int64(i * 10),
					CopiedSegments: int64(i * 5),
					SnapshotName:   fmt.Sprintf("snapshot_%d", i),
				},
				tr: timerecord.NewTimeRecorder(fmt.Sprintf("job_%d", i)),
			}
			err := svr.copySegmentMeta.AddJob(context.TODO(), job)
			require.NoError(t, err)
		}

		req := &datapb.ListRestoreSnapshotJobsRequest{}

		resp, err := svr.ListRestoreSnapshotJobs(context.Background(), req)
		assert.NoError(t, err)
		assert.NoError(t, merr.Error(resp.GetStatus()))
		assert.Len(t, resp.GetJobs(), 3)
	})

	t.Run("filter by collection id", func(t *testing.T) {
		svr := newTestServer(t)
		defer closeTestServer(t, svr)

		// Add jobs for different collections
		job1 := &copySegmentJob{
			CopySegmentJob: &datapb.CopySegmentJob{
				JobId:        201,
				CollectionId: 100,
				State:        datapb.CopySegmentJobState_CopySegmentJobExecuting,
				SnapshotName: "snapshot_1",
			},
			tr: timerecord.NewTimeRecorder("job_1"),
		}
		err := svr.copySegmentMeta.AddJob(context.TODO(), job1)
		require.NoError(t, err)

		job2 := &copySegmentJob{
			CopySegmentJob: &datapb.CopySegmentJob{
				JobId:        202,
				CollectionId: 200,
				State:        datapb.CopySegmentJobState_CopySegmentJobExecuting,
				SnapshotName: "snapshot_2",
			},
			tr: timerecord.NewTimeRecorder("job_2"),
		}
		err = svr.copySegmentMeta.AddJob(context.TODO(), job2)
		require.NoError(t, err)

		req := &datapb.ListRestoreSnapshotJobsRequest{
			CollectionId: 100,
		}

		resp, err := svr.ListRestoreSnapshotJobs(context.Background(), req)
		assert.NoError(t, err)
		assert.NoError(t, merr.Error(resp.GetStatus()))
		assert.Equal(t, int64(201), resp.GetJobs()[0].GetJobId())
	})
}

func TestServer_CreateSnapshot_DuplicateName(t *testing.T) {
	t.Run("snapshot name already exists", func(t *testing.T) {
		ctx := context.Background()
		existingSnapshotName := "test_snapshot"

		// Mock snapshotManager.GetSnapshot to simulate existing snapshot
		mockGetSnapshot := mockey.Mock((*snapshotManager).GetSnapshot).To(func(
			sm *snapshotManager,
			ctx context.Context,
			name string,
		) (*datapb.SnapshotInfo, error) {
			// Return a snapshot to simulate it already exists
			return &datapb.SnapshotInfo{Name: name}, nil
		}).Build()
		defer mockGetSnapshot.UnPatch()

		server := &Server{
			snapshotManager: NewSnapshotManager(nil, nil, nil, nil, nil, nil, nil),
		}
		server.stateCode.Store(commonpb.StateCode_Healthy)

		// Try to create snapshot with duplicate name
		req := &datapb.CreateSnapshotRequest{
			Name:         existingSnapshotName,
			Description:  "duplicate snapshot",
			CollectionId: 200,
		}

		resp, err := server.CreateSnapshot(ctx, req)

		// Verify error response
		assert.NoError(t, err)
		assert.NotNil(t, resp)
		assert.Error(t, merr.Error(resp))
		assert.True(t, errors.Is(merr.Error(resp), merr.ErrParameterInvalid))
		assert.Contains(t, resp.GetReason(), "already exists")
		assert.Contains(t, resp.GetReason(), existingSnapshotName)
	})
}

// --- Test startBroadcastRestoreSnapshot index validation ---
// Note: Broker method tests (DescribeCollectionInternal, ShowPartitions, DropCollection)
// are covered in internal/datacoord/broker/coordinator_broker_test.go

func TestServer_StartBroadcastRestoreSnapshot_IndexValidation(t *testing.T) {
	t.Run("index_not_found", func(t *testing.T) {
		ctx := context.Background()

		// Mock GetIndexesForCollection to return empty (no indexes)
		mockGetIndexes := mockey.Mock((*indexMeta).GetIndexesForCollection).To(
			func(im *indexMeta, collectionID int64, indexName string) []*model.Index {
				return []*model.Index{} // No indexes exist
			}).Build()
		defer mockGetIndexes.UnPatch()

		// Mock broker methods
		mockDescribe := mockey.Mock(mockey.GetMethod(&broker.MockBroker{}, "DescribeCollectionInternal")).To(
			func(_ *broker.MockBroker, ctx context.Context, collectionID int64) (*milvuspb.DescribeCollectionResponse, error) {
				return &milvuspb.DescribeCollectionResponse{
					Status:         merr.Success(),
					CollectionID:   collectionID,
					CollectionName: "test_collection",
					DbName:         "default",
				}, nil
			}).Build()
		defer mockDescribe.UnPatch()

		mockShowPartitions := mockey.Mock(mockey.GetMethod(&broker.MockBroker{}, "ShowPartitions")).To(
			func(_ *broker.MockBroker, ctx context.Context, collectionID int64) (*milvuspb.ShowPartitionsResponse, error) {
				return &milvuspb.ShowPartitionsResponse{
					Status:         merr.Success(),
					PartitionNames: []string{"_default"},
					PartitionIDs:   []int64{1},
				}, nil
			}).Build()
		defer mockShowPartitions.UnPatch()

		mockBroker := broker.NewMockBroker(t)
		server := &Server{
			broker: mockBroker,
			meta:   &meta{indexMeta: &indexMeta{}},
		}

		// Snapshot has an index that should exist but doesn't
		snapshotData := &SnapshotData{
			Collection: &datapb.CollectionDescription{
				Partitions: map[string]int64{
					"_default": 1,
				},
			},
			Indexes: []*indexpb.IndexInfo{
				{
					IndexID:   1001,
					IndexName: "vector_index",
					FieldID:   100,
				},
			},
		}

		// Execute
		broadcaster, err := server.startBroadcastRestoreSnapshot(ctx, 100, snapshotData)

		// Verify
		assert.Error(t, err)
		assert.Nil(t, broadcaster)
		assert.Contains(t, err.Error(), "index vector_index for field 100 does not exist")
	})

	t.Run("index_found", func(t *testing.T) {
		ctx := context.Background()

		// Mock GetIndexesForCollection to return matching index
		mockGetIndexes := mockey.Mock((*indexMeta).GetIndexesForCollection).To(
			func(im *indexMeta, collectionID int64, indexName string) []*model.Index {
				return []*model.Index{
					{
						FieldID:   100,
						IndexName: "vector_index",
					},
				}
			}).Build()
		defer mockGetIndexes.UnPatch()

		// Mock broker methods
		mockDescribe := mockey.Mock(mockey.GetMethod(&broker.MockBroker{}, "DescribeCollectionInternal")).To(
			func(_ *broker.MockBroker, ctx context.Context, collectionID int64) (*milvuspb.DescribeCollectionResponse, error) {
				return &milvuspb.DescribeCollectionResponse{
					Status:         merr.Success(),
					CollectionID:   collectionID,
					CollectionName: "test_collection",
					DbName:         "default",
				}, nil
			}).Build()
		defer mockDescribe.UnPatch()

		mockShowPartitions := mockey.Mock(mockey.GetMethod(&broker.MockBroker{}, "ShowPartitions")).To(
			func(_ *broker.MockBroker, ctx context.Context, collectionID int64) (*milvuspb.ShowPartitionsResponse, error) {
				return &milvuspb.ShowPartitionsResponse{
					Status:         merr.Success(),
					PartitionNames: []string{"_default"},
					PartitionIDs:   []int64{1},
				}, nil
			}).Build()
		defer mockShowPartitions.UnPatch()

		// Mock StartBroadcastWithResourceKeys to avoid actual broadcast
		mockBroadcast := mockey.Mock(broadcast.StartBroadcastWithResourceKeys).To(
			func(ctx context.Context, keys ...message.ResourceKey) (broadcaster.BroadcastAPI, error) {
				return nil, errors.New("broadcast not needed for this test")
			}).Build()
		defer mockBroadcast.UnPatch()

		mockBroker := broker.NewMockBroker(t)
		server := &Server{
			broker: mockBroker,
			meta:   &meta{indexMeta: &indexMeta{}},
		}

		snapshotData := &SnapshotData{
			Collection: &datapb.CollectionDescription{
				Partitions: map[string]int64{
					"_default": 1,
				},
			},
			Indexes: []*indexpb.IndexInfo{
				{
					IndexID:   1001,
					IndexName: "vector_index",
					FieldID:   100,
				},
			},
		}

		// Execute - should pass validation but fail at broadcast (which we don't need to test)
		_, err := server.startBroadcastRestoreSnapshot(ctx, 100, snapshotData)

		// Verify that we got past index validation (error is from broadcast mock, not validation)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "broadcast not needed")
	})
}

// --- Test rollbackRestoreSnapshot ---
// Note: The actual DropCollection RPC is tested in internal/datacoord/broker/coordinator_broker_test.go

func TestServer_RollbackRestoreSnapshot(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		ctx := context.Background()

		dropCalled := false
		mockDrop := mockey.Mock(mockey.GetMethod(&broker.MockBroker{}, "DropCollection")).To(
			func(_ *broker.MockBroker, ctx context.Context, dbName, collectionName string) error {
				dropCalled = true
				assert.Equal(t, "test_db", dbName)
				assert.Equal(t, "test_collection", collectionName)
				return nil
			}).Build()
		defer mockDrop.UnPatch()

		mockBroker := broker.NewMockBroker(t)
		server := &Server{
			broker: mockBroker,
		}

		err := server.rollbackRestoreSnapshot(ctx, "test_db", "test_collection")

		assert.NoError(t, err)
		assert.True(t, dropCalled)
	})

	t.Run("drop_failed", func(t *testing.T) {
		ctx := context.Background()

		expectedErr := errors.New("drop collection failed")
		mockDrop := mockey.Mock(mockey.GetMethod(&broker.MockBroker{}, "DropCollection")).To(
			func(_ *broker.MockBroker, ctx context.Context, dbName, collectionName string) error {
				return expectedErr
			}).Build()
		defer mockDrop.UnPatch()

		mockBroker := broker.NewMockBroker(t)
		server := &Server{
			broker: mockBroker,
		}

		err := server.rollbackRestoreSnapshot(ctx, "test_db", "test_collection")

		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
	})
}

// --- Test DropSnapshot ---

func TestServer_DropSnapshot(t *testing.T) {
	t.Run("server_not_healthy", func(t *testing.T) {
		ctx := context.Background()

		server := &Server{}
		server.stateCode.Store(commonpb.StateCode_Abnormal)

		resp, err := server.DropSnapshot(ctx, &datapb.DropSnapshotRequest{
			Name: "test_snapshot",
		})

		assert.NoError(t, err)
		assert.Error(t, merr.Error(resp))
	})

	t.Run("snapshot_not_found_returns_success", func(t *testing.T) {
		ctx := context.Background()

		// Mock GetSnapshot to return not found error
		mockGetSnapshot := mockey.Mock((*snapshotManager).GetSnapshot).To(
			func(sm *snapshotManager, ctx context.Context, name string) (*datapb.SnapshotInfo, error) {
				return nil, errors.New("snapshot not found")
			}).Build()
		defer mockGetSnapshot.UnPatch()

		mockBroadCaster := &struct{ broadcaster.BroadcastAPI }{}
		mockClose := mockey.Mock((*struct{ broadcaster.BroadcastAPI }).Close).Return().Build()
		defer mockClose.UnPatch()

		mockBroadcast := mockey.Mock(broadcast.StartBroadcastWithResourceKeys).To(
			func(ctx context.Context, keys ...message.ResourceKey) (broadcaster.BroadcastAPI, error) {
				return mockBroadCaster, nil
			}).Build()
		defer mockBroadcast.UnPatch()

		server := &Server{
			snapshotManager: NewSnapshotManager(nil, nil, nil, nil, nil, nil, nil),
		}
		server.stateCode.Store(commonpb.StateCode_Healthy)

		resp, err := server.DropSnapshot(ctx, &datapb.DropSnapshotRequest{
			Name: "non_existent_snapshot",
		})

		assert.NoError(t, err)
		assert.NoError(t, merr.Error(resp))
	})

	t.Run("snapshot_dropped_between_check_and_lock", func(t *testing.T) {
		ctx := context.Background()

		// Mock GetSnapshot: first call returns success, second returns not found
		// This simulates another goroutine dropping the snapshot between the
		// pre-lock check and the post-lock double-check (TOCTOU pattern)
		callCount := 0
		mockGetSnapshot := mockey.Mock((*snapshotManager).GetSnapshot).To(
			func(sm *snapshotManager, ctx context.Context, name string) (*datapb.SnapshotInfo, error) {
				callCount++
				if callCount == 1 {
					return &datapb.SnapshotInfo{Name: name}, nil
				}
				return nil, errors.New("snapshot not found")
			}).Build()
		defer mockGetSnapshot.UnPatch()

		mockBroadCaster := &struct{ broadcaster.BroadcastAPI }{}
		mockClose := mockey.Mock((*struct{ broadcaster.BroadcastAPI }).Close).Return().Build()
		defer mockClose.UnPatch()

		mockBroadcast := mockey.Mock(broadcast.StartBroadcastWithResourceKeys).To(
			func(ctx context.Context, keys ...message.ResourceKey) (broadcaster.BroadcastAPI, error) {
				return mockBroadCaster, nil
			}).Build()
		defer mockBroadcast.UnPatch()

		server := &Server{
			snapshotManager: NewSnapshotManager(nil, nil, nil, nil, nil, nil, nil),
		}
		server.stateCode.Store(commonpb.StateCode_Healthy)

		resp, err := server.DropSnapshot(ctx, &datapb.DropSnapshotRequest{
			Name: "concurrent_dropped_snapshot",
		})

		assert.NoError(t, err)
		assert.NoError(t, merr.Error(resp))
		assert.Equal(t, 2, callCount, "GetSnapshot should be called exactly twice (pre-lock + post-lock)")
	})

	t.Run("snapshot_being_restored_returns_error", func(t *testing.T) {
		ctx := context.Background()

		// Mock GetSnapshot to return a valid snapshot (passes existence check)
		mockGetSnapshot := mockey.Mock((*snapshotManager).GetSnapshot).To(
			func(sm *snapshotManager, ctx context.Context, name string) (*datapb.SnapshotInfo, error) {
				return &datapb.SnapshotInfo{Name: name}, nil
			}).Build()
		defer mockGetSnapshot.UnPatch()

		// Mock GetSnapshotRestoreRefCount to return 1 (active restore in progress)
		mockGetRefCount := mockey.Mock((*snapshotManager).GetSnapshotRestoreRefCount).To(
			func(sm *snapshotManager, snapshotName string) int32 {
				return 1
			}).Build()
		defer mockGetRefCount.UnPatch()

		mockBroadCaster := &struct{ broadcaster.BroadcastAPI }{}
		mockClose := mockey.Mock((*struct{ broadcaster.BroadcastAPI }).Close).Return().Build()
		defer mockClose.UnPatch()

		mockBroadcast := mockey.Mock(broadcast.StartBroadcastWithResourceKeys).To(
			func(ctx context.Context, keys ...message.ResourceKey) (broadcaster.BroadcastAPI, error) {
				return mockBroadCaster, nil
			}).Build()
		defer mockBroadcast.UnPatch()

		server := &Server{
			snapshotManager: NewSnapshotManager(nil, nil, nil, nil, nil, nil, nil),
		}
		server.stateCode.Store(commonpb.StateCode_Healthy)

		resp, err := server.DropSnapshot(ctx, &datapb.DropSnapshotRequest{
			Name: "restoring_snapshot",
		})

		assert.NoError(t, err)
		assert.Error(t, merr.Error(resp))
	})
}

// --- Test DescribeSnapshot ---

func TestServer_DescribeSnapshot(t *testing.T) {
	t.Run("server_not_healthy", func(t *testing.T) {
		ctx := context.Background()

		server := &Server{}
		server.stateCode.Store(commonpb.StateCode_Abnormal)

		resp, err := server.DescribeSnapshot(ctx, &datapb.DescribeSnapshotRequest{
			Name: "test_snapshot",
		})

		assert.NoError(t, err)
		assert.Error(t, merr.Error(resp.GetStatus()))
	})

	t.Run("snapshot_not_found", func(t *testing.T) {
		ctx := context.Background()

		// Mock DescribeSnapshot to return error
		mockDescribe := mockey.Mock((*snapshotManager).DescribeSnapshot).To(
			func(sm *snapshotManager, ctx context.Context, name string) (*SnapshotData, error) {
				return nil, errors.New("snapshot not found: " + name)
			}).Build()
		defer mockDescribe.UnPatch()

		server := &Server{
			snapshotManager: NewSnapshotManager(nil, nil, nil, nil, nil, nil, nil),
		}
		server.stateCode.Store(commonpb.StateCode_Healthy)

		resp, err := server.DescribeSnapshot(ctx, &datapb.DescribeSnapshotRequest{
			Name: "non_existent_snapshot",
		})

		assert.NoError(t, err)
		assert.Error(t, merr.Error(resp.GetStatus()))
	})

	t.Run("success", func(t *testing.T) {
		ctx := context.Background()

		// Mock DescribeSnapshot to return snapshot data
		mockDescribe := mockey.Mock((*snapshotManager).DescribeSnapshot).To(
			func(sm *snapshotManager, ctx context.Context, name string) (*SnapshotData, error) {
				return &SnapshotData{
					SnapshotInfo: &datapb.SnapshotInfo{
						Name:         name,
						CollectionId: 100,
					},
					Collection: &datapb.CollectionDescription{
						Schema: &schemapb.CollectionSchema{
							Name: "test_collection",
						},
					},
				}, nil
			}).Build()
		defer mockDescribe.UnPatch()

		server := &Server{
			snapshotManager: NewSnapshotManager(nil, nil, nil, nil, nil, nil, nil),
		}
		server.stateCode.Store(commonpb.StateCode_Healthy)

		resp, err := server.DescribeSnapshot(ctx, &datapb.DescribeSnapshotRequest{
			Name: "test_snapshot",
		})

		assert.NoError(t, err)
		assert.NoError(t, merr.Error(resp.GetStatus()))
		assert.Equal(t, "test_snapshot", resp.GetSnapshotInfo().GetName())
		assert.Equal(t, int64(100), resp.GetSnapshotInfo().GetCollectionId())
	})

	t.Run("success_with_collection_info", func(t *testing.T) {
		ctx := context.Background()

		// Mock DescribeSnapshot to return snapshot data with collection info
		mockDescribe := mockey.Mock((*snapshotManager).DescribeSnapshot).To(
			func(sm *snapshotManager, ctx context.Context, name string) (*SnapshotData, error) {
				return &SnapshotData{
					SnapshotInfo: &datapb.SnapshotInfo{
						Name:         name,
						CollectionId: 100,
					},
					Collection: &datapb.CollectionDescription{
						Schema: &schemapb.CollectionSchema{
							Name: "test_collection",
						},
					},
					Indexes: []*indexpb.IndexInfo{
						{IndexID: 1, IndexName: "idx1"},
					},
				}, nil
			}).Build()
		defer mockDescribe.UnPatch()

		server := &Server{
			snapshotManager: NewSnapshotManager(nil, nil, nil, nil, nil, nil, nil),
		}
		server.stateCode.Store(commonpb.StateCode_Healthy)

		resp, err := server.DescribeSnapshot(ctx, &datapb.DescribeSnapshotRequest{
			Name:                  "test_snapshot",
			IncludeCollectionInfo: true,
		})

		assert.NoError(t, err)
		assert.NoError(t, merr.Error(resp.GetStatus()))
		assert.NotNil(t, resp.GetCollectionInfo())
		assert.Equal(t, "test_collection", resp.GetCollectionInfo().GetSchema().GetName())
		assert.Len(t, resp.GetIndexInfos(), 1)
	})
}

// --- Test ListSnapshots ---

func TestServer_ListSnapshots(t *testing.T) {
	t.Run("server_not_healthy", func(t *testing.T) {
		ctx := context.Background()

		server := &Server{}
		server.stateCode.Store(commonpb.StateCode_Abnormal)

		resp, err := server.ListSnapshots(ctx, &datapb.ListSnapshotsRequest{
			CollectionId: 100,
		})

		assert.NoError(t, err)
		assert.Error(t, merr.Error(resp.GetStatus()))
	})

	t.Run("success_empty_list", func(t *testing.T) {
		ctx := context.Background()

		// Mock ListSnapshots to return empty list
		mockList := mockey.Mock((*snapshotManager).ListSnapshots).To(
			func(sm *snapshotManager, ctx context.Context, collectionID, partitionID int64) ([]string, error) {
				return []string{}, nil
			}).Build()
		defer mockList.UnPatch()

		server := &Server{
			snapshotManager: NewSnapshotManager(nil, nil, nil, nil, nil, nil, nil),
		}
		server.stateCode.Store(commonpb.StateCode_Healthy)

		resp, err := server.ListSnapshots(ctx, &datapb.ListSnapshotsRequest{
			CollectionId: 100,
		})

		assert.NoError(t, err)
		assert.NoError(t, merr.Error(resp.GetStatus()))
		assert.Empty(t, resp.GetSnapshots())
	})

	t.Run("success_with_snapshots", func(t *testing.T) {
		ctx := context.Background()

		// Mock ListSnapshots to return list
		mockList := mockey.Mock((*snapshotManager).ListSnapshots).To(
			func(sm *snapshotManager, ctx context.Context, collectionID, partitionID int64) ([]string, error) {
				return []string{"snapshot1", "snapshot2"}, nil
			}).Build()
		defer mockList.UnPatch()

		server := &Server{
			snapshotManager: NewSnapshotManager(nil, nil, nil, nil, nil, nil, nil),
		}
		server.stateCode.Store(commonpb.StateCode_Healthy)

		resp, err := server.ListSnapshots(ctx, &datapb.ListSnapshotsRequest{
			CollectionId: 100,
		})

		assert.NoError(t, err)
		assert.NoError(t, merr.Error(resp.GetStatus()))
		assert.Len(t, resp.GetSnapshots(), 2)
	})

	t.Run("list_error", func(t *testing.T) {
		ctx := context.Background()

		// Mock ListSnapshots to return error
		mockList := mockey.Mock((*snapshotManager).ListSnapshots).To(
			func(sm *snapshotManager, ctx context.Context, collectionID, partitionID int64) ([]string, error) {
				return nil, errors.New("failed to list snapshots")
			}).Build()
		defer mockList.UnPatch()

		server := &Server{
			snapshotManager: NewSnapshotManager(nil, nil, nil, nil, nil, nil, nil),
		}
		server.stateCode.Store(commonpb.StateCode_Healthy)

		resp, err := server.ListSnapshots(ctx, &datapb.ListSnapshotsRequest{
			CollectionId: 100,
		})

		assert.NoError(t, err)
		assert.Error(t, merr.Error(resp.GetStatus()))
	})
}

// --- Test RestoreSnapshot ---

func TestServer_RestoreSnapshot(t *testing.T) {
	t.Run("server_not_healthy", func(t *testing.T) {
		ctx := context.Background()

		server := &Server{}
		server.stateCode.Store(commonpb.StateCode_Abnormal)

		resp, err := server.RestoreSnapshot(ctx, &datapb.RestoreSnapshotRequest{
			Name:           "test_snapshot",
			DbName:         "default",
			CollectionName: "new_collection",
		})

		assert.NoError(t, err)
		assert.Error(t, merr.Error(resp.GetStatus()))
	})

	t.Run("missing_snapshot_name", func(t *testing.T) {
		ctx := context.Background()

		server := &Server{}
		server.stateCode.Store(commonpb.StateCode_Healthy)

		resp, err := server.RestoreSnapshot(ctx, &datapb.RestoreSnapshotRequest{
			Name:           "",
			DbName:         "default",
			CollectionName: "new_collection",
		})

		assert.NoError(t, err)
		assert.Error(t, merr.Error(resp.GetStatus()))
		assert.True(t, errors.Is(merr.Error(resp.GetStatus()), merr.ErrParameterInvalid))
	})

	t.Run("missing_collection_name", func(t *testing.T) {
		ctx := context.Background()

		server := &Server{}
		server.stateCode.Store(commonpb.StateCode_Healthy)

		resp, err := server.RestoreSnapshot(ctx, &datapb.RestoreSnapshotRequest{
			Name:           "test_snapshot",
			DbName:         "default",
			CollectionName: "",
		})

		assert.NoError(t, err)
		assert.Error(t, merr.Error(resp.GetStatus()))
		assert.True(t, errors.Is(merr.Error(resp.GetStatus()), merr.ErrParameterInvalid))
	})

	t.Run("snapshot_not_found", func(t *testing.T) {
		ctx := context.Background()

		// Mock ReadSnapshotData to return error
		mockRead := mockey.Mock((*snapshotManager).ReadSnapshotData).To(
			func(sm *snapshotManager, ctx context.Context, name string) (*SnapshotData, error) {
				return nil, errors.New("snapshot not found: " + name)
			}).Build()
		defer mockRead.UnPatch()

		server := &Server{
			snapshotManager: NewSnapshotManager(nil, nil, nil, nil, nil, nil, nil),
		}
		server.stateCode.Store(commonpb.StateCode_Healthy)

		resp, err := server.RestoreSnapshot(ctx, &datapb.RestoreSnapshotRequest{
			Name:           "non_existent_snapshot",
			DbName:         "default",
			CollectionName: "new_collection",
		})

		assert.NoError(t, err)
		assert.Error(t, merr.Error(resp.GetStatus()))
	})
}

// --- Test CreateSnapshot additional cases ---

func TestServer_CreateSnapshot_AdditionalCases(t *testing.T) {
	t.Run("server_not_healthy", func(t *testing.T) {
		ctx := context.Background()

		server := &Server{}
		server.stateCode.Store(commonpb.StateCode_Abnormal)

		resp, err := server.CreateSnapshot(ctx, &datapb.CreateSnapshotRequest{
			Name:         "test_snapshot",
			CollectionId: 100,
		})

		assert.NoError(t, err)
		assert.Error(t, merr.Error(resp))
	})

	t.Run("snapshot_already_exists", func(t *testing.T) {
		ctx := context.Background()

		// Mock GetSnapshot to return existing snapshot (no error means it exists)
		mockGet := mockey.Mock((*snapshotManager).GetSnapshot).To(
			func(sm *snapshotManager, ctx context.Context, name string) (*datapb.SnapshotInfo, error) {
				return &datapb.SnapshotInfo{Name: name}, nil
			}).Build()
		defer mockGet.UnPatch()

		server := &Server{
			snapshotManager: NewSnapshotManager(nil, nil, nil, nil, nil, nil, nil),
		}
		server.stateCode.Store(commonpb.StateCode_Healthy)

		resp, err := server.CreateSnapshot(ctx, &datapb.CreateSnapshotRequest{
			Name:         "existing_snapshot",
			CollectionId: 100,
		})

		assert.NoError(t, err)
		assert.Error(t, merr.Error(resp))
		assert.True(t, errors.Is(merr.Error(resp), merr.ErrParameterInvalid))
		assert.Contains(t, resp.GetReason(), "already exists")
	})
}

// --- Test BatchUpdateManifest ---

func TestServer_BatchUpdateManifest(t *testing.T) {
	t.Run("server_not_healthy", func(t *testing.T) {
		ctx := context.Background()

		server := &Server{}
		server.stateCode.Store(commonpb.StateCode_Abnormal)

		resp, err := server.BatchUpdateManifest(ctx, &datapb.BatchUpdateManifestRequest{
			CollectionId: 100,
			Items: []*datapb.BatchUpdateManifestItem{
				{SegmentId: 1, ManifestVersion: 10},
			},
		})

		assert.NoError(t, err)
		assert.Error(t, merr.Error(resp))
	})

	t.Run("describe_collection_failed", func(t *testing.T) {
		ctx := context.Background()

		mockBroker := broker.NewMockBroker(t)
		mockBroker.EXPECT().DescribeCollectionInternal(mock.Anything, mock.Anything).
			Return(nil, errors.New("collection not found"))

		server := &Server{
			broker: mockBroker,
		}
		server.stateCode.Store(commonpb.StateCode_Healthy)

		resp, err := server.BatchUpdateManifest(ctx, &datapb.BatchUpdateManifestRequest{
			CollectionId: 100,
			Items: []*datapb.BatchUpdateManifestItem{
				{SegmentId: 1, ManifestVersion: 10},
			},
		})

		assert.NoError(t, err)
		assert.Error(t, merr.Error(resp))
	})

	t.Run("start_broadcast_failed", func(t *testing.T) {
		ctx := context.Background()

		mockBroker := broker.NewMockBroker(t)
		mockBroker.EXPECT().DescribeCollectionInternal(mock.Anything, mock.Anything).
			Return(&milvuspb.DescribeCollectionResponse{
				Status:         merr.Success(),
				DbName:         "default",
				CollectionName: "test_collection",
			}, nil)

		mockBroadcast := mockey.Mock(broadcast.StartBroadcastWithResourceKeys).To(
			func(ctx context.Context, keys ...message.ResourceKey) (broadcaster.BroadcastAPI, error) {
				return nil, errors.New("failed to start broadcast")
			}).Build()
		defer mockBroadcast.UnPatch()

		server := &Server{
			broker: mockBroker,
		}
		server.stateCode.Store(commonpb.StateCode_Healthy)

		resp, err := server.BatchUpdateManifest(ctx, &datapb.BatchUpdateManifestRequest{
			CollectionId: 100,
			Items: []*datapb.BatchUpdateManifestItem{
				{SegmentId: 1, ManifestVersion: 10},
			},
		})

		assert.NoError(t, err)
		assert.Error(t, merr.Error(resp))
	})

	t.Run("broadcast_send_failed", func(t *testing.T) {
		ctx := context.Background()

		mockBroker := broker.NewMockBroker(t)
		mockBroker.EXPECT().DescribeCollectionInternal(mock.Anything, mock.Anything).
			Return(&milvuspb.DescribeCollectionResponse{
				Status:         merr.Success(),
				DbName:         "default",
				CollectionName: "test_collection",
			}, nil)

		// Mock WAL
		wal := mock_streaming.NewMockWALAccesser(t)
		wal.EXPECT().ControlChannel().Return("by-dev-rootcoord-dml_0").Maybe()
		streaming.SetWALForTest(wal)

		// Mock broadcaster
		bapi := mock_broadcaster.NewMockBroadcastAPI(t)
		bapi.EXPECT().Broadcast(mock.Anything, mock.Anything).Return(nil, errors.New("broadcast send failed"))
		bapi.EXPECT().Close().Return()

		mockBroadcastPatch := mockey.Mock(broadcast.StartBroadcastWithResourceKeys).To(
			func(ctx context.Context, keys ...message.ResourceKey) (broadcaster.BroadcastAPI, error) {
				return bapi, nil
			}).Build()
		defer mockBroadcastPatch.UnPatch()

		server := &Server{
			broker: mockBroker,
		}
		server.stateCode.Store(commonpb.StateCode_Healthy)

		resp, err := server.BatchUpdateManifest(ctx, &datapb.BatchUpdateManifestRequest{
			CollectionId: 100,
			Items: []*datapb.BatchUpdateManifestItem{
				{SegmentId: 1, ManifestVersion: 10},
			},
		})

		assert.NoError(t, err)
		assert.Error(t, merr.Error(resp))
	})

	t.Run("success", func(t *testing.T) {
		ctx := context.Background()

		mockBroker := broker.NewMockBroker(t)
		mockBroker.EXPECT().DescribeCollectionInternal(mock.Anything, mock.Anything).
			Return(&milvuspb.DescribeCollectionResponse{
				Status:         merr.Success(),
				DbName:         "default",
				CollectionName: "test_collection",
			}, nil)

		// Mock WAL
		wal := mock_streaming.NewMockWALAccesser(t)
		wal.EXPECT().ControlChannel().Return("by-dev-rootcoord-dml_0").Maybe()
		streaming.SetWALForTest(wal)

		// Mock broadcaster
		bapi := mock_broadcaster.NewMockBroadcastAPI(t)
		bapi.EXPECT().Broadcast(mock.Anything, mock.Anything).Return(&types2.BroadcastAppendResult{
			BroadcastID: 1,
			AppendResults: map[string]*types2.AppendResult{
				"by-dev-rootcoord-dml_0": {
					MessageID:              rmq.NewRmqID(1),
					TimeTick:               tsoutil.ComposeTSByTime(time.Now(), 0),
					LastConfirmedMessageID: rmq.NewRmqID(1),
				},
			},
		}, nil)
		bapi.EXPECT().Close().Return()

		mockBroadcastPatch := mockey.Mock(broadcast.StartBroadcastWithResourceKeys).To(
			func(ctx context.Context, keys ...message.ResourceKey) (broadcaster.BroadcastAPI, error) {
				return bapi, nil
			}).Build()
		defer mockBroadcastPatch.UnPatch()

		server := &Server{
			broker: mockBroker,
		}
		server.stateCode.Store(commonpb.StateCode_Healthy)

		resp, err := server.BatchUpdateManifest(ctx, &datapb.BatchUpdateManifestRequest{
			CollectionId: 100,
			Items: []*datapb.BatchUpdateManifestItem{
				{SegmentId: 1, ManifestVersion: 10},
				{SegmentId: 2, ManifestVersion: 20},
			},
		})

		assert.NoError(t, err)
		assert.True(t, merr.Ok(resp))
	})
}

func TestServer_BatchUpdateManifest_Callback(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		ctx := context.Background()

		registry.ResetRegistration()

		mockUpdateSegmentsInfo := mockey.Mock((*meta).UpdateSegmentsInfo).To(
			func(m *meta, ctx context.Context, operators ...UpdateOperator) error {
				return nil
			}).Build()
		defer mockUpdateSegmentsInfo.UnPatch()

		server := &Server{
			ctx:  ctx,
			meta: &meta{segments: NewSegmentsInfo()},
		}
		server.stateCode.Store(commonpb.StateCode_Healthy)
		server.initMessageCallback()

		msg := message.NewBatchUpdateManifestMessageBuilderV2().
			WithHeader(&message.BatchUpdateManifestMessageHeader{
				CollectionId: 100,
			}).
			WithBody(&message.BatchUpdateManifestMessageBody{
				Items: []*messagespb.BatchUpdateManifestItem{
					{SegmentId: 1, ManifestVersion: 15},
					{SegmentId: 2, ManifestVersion: 25},
				},
			}).
			WithBroadcast([]string{"control_channel"}).
			MustBuildBroadcast()

		err := registry.CallMessageAckCallback(ctx, msg, map[string]*message.AppendResult{
			"control_channel": {
				MessageID:              rmq.NewRmqID(1),
				LastConfirmedMessageID: rmq.NewRmqID(1),
				TimeTick:               1,
			},
		})
		assert.NoError(t, err)
	})

	t.Run("empty_items", func(t *testing.T) {
		ctx := context.Background()

		registry.ResetRegistration()

		server := &Server{
			ctx:  ctx,
			meta: &meta{segments: NewSegmentsInfo()},
		}
		server.stateCode.Store(commonpb.StateCode_Healthy)
		server.initMessageCallback()

		msg := message.NewBatchUpdateManifestMessageBuilderV2().
			WithHeader(&message.BatchUpdateManifestMessageHeader{
				CollectionId: 100,
			}).
			WithBody(&message.BatchUpdateManifestMessageBody{}).
			WithBroadcast([]string{"control_channel"}).
			MustBuildBroadcast()

		err := registry.CallMessageAckCallback(ctx, msg, map[string]*message.AppendResult{
			"control_channel": {
				MessageID:              rmq.NewRmqID(1),
				LastConfirmedMessageID: rmq.NewRmqID(1),
				TimeTick:               1,
			},
		})
		assert.NoError(t, err)
	})

	t.Run("update_segments_info_error", func(t *testing.T) {
		ctx := context.Background()

		registry.ResetRegistration()

		mockUpdateSegmentsInfo := mockey.Mock((*meta).UpdateSegmentsInfo).To(
			func(m *meta, ctx context.Context, operators ...UpdateOperator) error {
				return errors.New("update segments info failed")
			}).Build()
		defer mockUpdateSegmentsInfo.UnPatch()

		server := &Server{
			ctx:  ctx,
			meta: &meta{segments: NewSegmentsInfo()},
		}
		server.stateCode.Store(commonpb.StateCode_Healthy)
		server.initMessageCallback()

		msg := message.NewBatchUpdateManifestMessageBuilderV2().
			WithHeader(&message.BatchUpdateManifestMessageHeader{
				CollectionId: 100,
			}).
			WithBody(&message.BatchUpdateManifestMessageBody{
				Items: []*messagespb.BatchUpdateManifestItem{
					{SegmentId: 1, ManifestVersion: 15},
				},
			}).
			WithBroadcast([]string{"control_channel"}).
			MustBuildBroadcast()

		err := registry.CallMessageAckCallback(ctx, msg, map[string]*message.AppendResult{
			"control_channel": {
				MessageID:              rmq.NewRmqID(1),
				LastConfirmedMessageID: rmq.NewRmqID(1),
				TimeTick:               1,
			},
		})
		assert.Error(t, err)
	})
}
