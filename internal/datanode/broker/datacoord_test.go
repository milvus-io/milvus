package broker

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
)

type dataCoordSuite struct {
	suite.Suite

	dc     *mocks.MockDataCoordClient
	broker Broker
}

func (s *dataCoordSuite) SetupSuite() {
	paramtable.Init()
}

func (s *dataCoordSuite) SetupTest() {
	s.dc = mocks.NewMockDataCoordClient(s.T())
	s.broker = NewCoordBroker(nil, s.dc)
}

func (s *dataCoordSuite) resetMock() {
	s.dc.AssertExpectations(s.T())
	s.dc.ExpectedCalls = nil
}

func (s *dataCoordSuite) TestAssignSegmentID() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reqs := []*datapb.SegmentIDRequest{
		{CollectionID: 100, Count: 1000},
		{CollectionID: 100, Count: 2000},
	}

	s.Run("normal_case", func() {
		s.dc.EXPECT().AssignSegmentID(mock.Anything, mock.Anything).
			Return(&datapb.AssignSegmentIDResponse{
				Status: merr.Status(nil),
				SegIDAssignments: lo.Map(reqs, func(req *datapb.SegmentIDRequest, _ int) *datapb.SegmentIDAssignment {
					return &datapb.SegmentIDAssignment{
						Status: merr.Status(nil),
						SegID:  10001,
						Count:  req.GetCount(),
					}
				}),
			}, nil)

		segmentIDs, err := s.broker.AssignSegmentID(ctx, reqs...)
		s.NoError(err)
		s.Equal(len(segmentIDs), len(reqs))
		s.resetMock()
	})

	s.Run("datacoord_return_error", func() {
		s.dc.EXPECT().AssignSegmentID(mock.Anything, mock.Anything).
			Return(nil, errors.New("mock"))

		_, err := s.broker.AssignSegmentID(ctx, reqs...)
		s.Error(err)
		s.resetMock()
	})

	s.Run("datacoord_return_failure_status", func() {
		s.dc.EXPECT().AssignSegmentID(mock.Anything, mock.Anything).
			Return(&datapb.AssignSegmentIDResponse{
				Status: merr.Status(errors.New("mock")),
			}, nil)

		_, err := s.broker.AssignSegmentID(ctx, reqs...)
		s.Error(err)
		s.resetMock()
	})
}

func (s *dataCoordSuite) TestReportTimeTick() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	msgs := []*msgpb.DataNodeTtMsg{
		{Timestamp: 1000, ChannelName: "dml_0"},
		{Timestamp: 2000, ChannelName: "dml_1"},
	}

	s.Run("normal_case", func() {
		s.dc.EXPECT().ReportDataNodeTtMsgs(mock.Anything, mock.Anything).
			Run(func(_ context.Context, req *datapb.ReportDataNodeTtMsgsRequest, _ ...grpc.CallOption) {
				s.Equal(msgs, req.GetMsgs())
			}).
			Return(merr.Status(nil), nil)

		err := s.broker.ReportTimeTick(ctx, msgs)
		s.NoError(err)
		s.resetMock()
	})

	s.Run("datacoord_return_error", func() {
		s.dc.EXPECT().ReportDataNodeTtMsgs(mock.Anything, mock.Anything).
			Return(merr.Status(errors.New("mock")), nil)

		err := s.broker.ReportTimeTick(ctx, msgs)
		s.Error(err)
		s.resetMock()
	})
}

func (s *dataCoordSuite) TestGetSegmentInfo() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	segmentIDs := []int64{1, 2, 3}

	s.Run("normal_case", func() {
		s.dc.EXPECT().GetSegmentInfo(mock.Anything, mock.Anything).
			Run(func(_ context.Context, req *datapb.GetSegmentInfoRequest, _ ...grpc.CallOption) {
				s.ElementsMatch(segmentIDs, req.GetSegmentIDs())
				s.True(req.GetIncludeUnHealthy())
			}).
			Return(&datapb.GetSegmentInfoResponse{
				Status: merr.Status(nil),
				Infos: lo.Map(segmentIDs, func(id int64, _ int) *datapb.SegmentInfo {
					return &datapb.SegmentInfo{ID: id}
				}),
			}, nil)
		infos, err := s.broker.GetSegmentInfo(ctx, segmentIDs)
		s.NoError(err)
		s.ElementsMatch(segmentIDs, lo.Map(infos, func(info *datapb.SegmentInfo, _ int) int64 { return info.GetID() }))
		s.resetMock()
	})

	s.Run("datacoord_return_error", func() {
		s.dc.EXPECT().GetSegmentInfo(mock.Anything, mock.Anything).
			Return(nil, errors.New("mock"))
		_, err := s.broker.GetSegmentInfo(ctx, segmentIDs)
		s.Error(err)
		s.resetMock()
	})

	s.Run("datacoord_return_failure_status", func() {
		s.dc.EXPECT().GetSegmentInfo(mock.Anything, mock.Anything).
			Return(&datapb.GetSegmentInfoResponse{
				Status: merr.Status(errors.New("mock")),
			}, nil)
		_, err := s.broker.GetSegmentInfo(ctx, segmentIDs)
		s.Error(err)
		s.resetMock()
	})
}

func (s *dataCoordSuite) TestUpdateChannelCheckpoint() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	channelName := "dml_0"
	checkpoint := &msgpb.MsgPosition{
		ChannelName: channelName,
		MsgID:       []byte{1, 2, 3},
		Timestamp:   tsoutil.ComposeTSByTime(time.Now(), 0),
	}

	s.Run("normal_case", func() {
		s.dc.EXPECT().UpdateChannelCheckpoint(mock.Anything, mock.Anything).
			Run(func(_ context.Context, req *datapb.UpdateChannelCheckpointRequest, _ ...grpc.CallOption) {
				s.Equal(channelName, req.GetVChannel())
				cp := req.GetPosition()
				s.Equal(checkpoint.MsgID, cp.GetMsgID())
				s.Equal(checkpoint.ChannelName, cp.GetChannelName())
				s.Equal(checkpoint.Timestamp, cp.GetTimestamp())
			}).
			Return(merr.Status(nil), nil)

		err := s.broker.UpdateChannelCheckpoint(ctx, channelName, checkpoint)
		s.NoError(err)
		s.resetMock()
	})

	s.Run("datacoord_return_error", func() {
		s.dc.EXPECT().UpdateChannelCheckpoint(mock.Anything, mock.Anything).
			Return(nil, errors.New("mock"))

		err := s.broker.UpdateChannelCheckpoint(ctx, channelName, checkpoint)
		s.Error(err)
		s.resetMock()
	})

	s.Run("datacoord_return_failure_status", func() {
		s.dc.EXPECT().UpdateChannelCheckpoint(mock.Anything, mock.Anything).
			Return(merr.Status(errors.New("mock")), nil)

		err := s.broker.UpdateChannelCheckpoint(ctx, channelName, checkpoint)
		s.Error(err)
		s.resetMock()
	})
}

func (s *dataCoordSuite) TestSaveBinlogPaths() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req := &datapb.SaveBinlogPathsRequest{
		Channel: "dml_0",
	}

	s.Run("normal_case", func() {
		s.dc.EXPECT().SaveBinlogPaths(mock.Anything, mock.Anything).
			Run(func(_ context.Context, req *datapb.SaveBinlogPathsRequest, _ ...grpc.CallOption) {
				s.Equal("dml_0", req.GetChannel())
			}).
			Return(merr.Status(nil), nil)
		err := s.broker.SaveBinlogPaths(ctx, req)
		s.NoError(err)
		s.resetMock()
	})

	s.Run("datacoord_return_error", func() {
		s.dc.EXPECT().SaveBinlogPaths(mock.Anything, mock.Anything).
			Return(nil, errors.New("mock"))
		err := s.broker.SaveBinlogPaths(ctx, req)
		s.Error(err)
		s.resetMock()
	})

	s.Run("datacoord_return_failure_status", func() {
		s.dc.EXPECT().SaveBinlogPaths(mock.Anything, mock.Anything).
			Return(merr.Status(errors.New("mock")), nil)
		err := s.broker.SaveBinlogPaths(ctx, req)
		s.Error(err)
		s.resetMock()
	})
}

func (s *dataCoordSuite) TestDropVirtualChannel() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req := &datapb.DropVirtualChannelRequest{
		ChannelName: "dml_0",
	}

	s.Run("normal_case", func() {
		s.dc.EXPECT().DropVirtualChannel(mock.Anything, mock.Anything).
			Run(func(_ context.Context, req *datapb.DropVirtualChannelRequest, _ ...grpc.CallOption) {
				s.Equal("dml_0", req.GetChannelName())
			}).
			Return(&datapb.DropVirtualChannelResponse{Status: merr.Status(nil)}, nil)
		_, err := s.broker.DropVirtualChannel(ctx, req)
		s.NoError(err)
		s.resetMock()
	})

	s.Run("datacoord_return_error", func() {
		s.dc.EXPECT().DropVirtualChannel(mock.Anything, mock.Anything).
			Return(nil, errors.New("mock"))
		_, err := s.broker.DropVirtualChannel(ctx, req)
		s.Error(err)
		s.resetMock()
	})

	s.Run("datacoord_return_failure_status", func() {
		s.dc.EXPECT().DropVirtualChannel(mock.Anything, mock.Anything).
			Return(&datapb.DropVirtualChannelResponse{Status: merr.Status(errors.New("mock"))}, nil)
		_, err := s.broker.DropVirtualChannel(ctx, req)
		s.Error(err)
		s.resetMock()
	})

	s.Run("datacoord_return_legacy_MetaFailed", func() {
		s.dc.EXPECT().DropVirtualChannel(mock.Anything, mock.Anything).
			Return(&datapb.DropVirtualChannelResponse{Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_MetaFailed}}, nil)
		_, err := s.broker.DropVirtualChannel(ctx, req)
		s.Error(err)
		s.ErrorIs(err, merr.ErrChannelNotFound)
		s.resetMock()
	})
}

func (s *dataCoordSuite) TestUpdateSegmentStatistics() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req := &datapb.UpdateSegmentStatisticsRequest{
		Stats: []*commonpb.SegmentStats{
			{}, {}, {},
		},
	}

	s.Run("normal_case", func() {
		s.dc.EXPECT().UpdateSegmentStatistics(mock.Anything, mock.Anything).
			Run(func(_ context.Context, r *datapb.UpdateSegmentStatisticsRequest, _ ...grpc.CallOption) {
				s.Equal(len(req.GetStats()), len(r.GetStats()))
			}).
			Return(merr.Status(nil), nil)
		err := s.broker.UpdateSegmentStatistics(ctx, req)
		s.NoError(err)
		s.resetMock()
	})

	s.Run("datacoord_return_failure_status", func() {
		s.dc.EXPECT().UpdateSegmentStatistics(mock.Anything, mock.Anything).
			Return(nil, errors.New("mock"))
		err := s.broker.UpdateSegmentStatistics(ctx, req)
		s.Error(err)
		s.resetMock()
	})

	s.Run("datacoord_return_failure_status", func() {
		s.dc.EXPECT().UpdateSegmentStatistics(mock.Anything, mock.Anything).
			Return(merr.Status(errors.New("mock")), nil)
		err := s.broker.UpdateSegmentStatistics(ctx, req)
		s.Error(err)
		s.resetMock()
	})
}

func (s *dataCoordSuite) TestSaveImportSegment() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	segmentID := int64(1001)
	collectionID := int64(100)

	req := &datapb.SaveImportSegmentRequest{
		SegmentId:    segmentID,
		CollectionId: collectionID,
	}

	s.Run("normal_case", func() {
		s.dc.EXPECT().SaveImportSegment(mock.Anything, mock.Anything).
			Run(func(_ context.Context, r *datapb.SaveImportSegmentRequest, _ ...grpc.CallOption) {
				s.Equal(collectionID, req.GetCollectionId())
				s.Equal(segmentID, req.GetSegmentId())
			}).
			Return(merr.Status(nil), nil)
		err := s.broker.SaveImportSegment(ctx, req)
		s.NoError(err)
		s.resetMock()
	})

	s.Run("datacoord_return_failure_status", func() {
		s.dc.EXPECT().SaveImportSegment(mock.Anything, mock.Anything).
			Return(nil, errors.New("mock"))
		err := s.broker.SaveImportSegment(ctx, req)
		s.Error(err)
		s.resetMock()
	})

	s.Run("datacoord_return_failure_status", func() {
		s.dc.EXPECT().SaveImportSegment(mock.Anything, mock.Anything).
			Return(merr.Status(errors.New("mock")), nil)
		err := s.broker.SaveImportSegment(ctx, req)
		s.Error(err)
		s.resetMock()
	})
}

func TestDataCoordBroker(t *testing.T) {
	suite.Run(t, new(dataCoordSuite))
}
