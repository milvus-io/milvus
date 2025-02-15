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
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
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
	s.broker = NewCoordBroker(s.dc, 1)
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
				cp := req.GetChannelCheckpoints()[0]
				s.Equal(checkpoint.MsgID, cp.GetMsgID())
				s.Equal(checkpoint.ChannelName, cp.GetChannelName())
				s.Equal(checkpoint.Timestamp, cp.GetTimestamp())
			}).
			Return(merr.Status(nil), nil)

		err := s.broker.UpdateChannelCheckpoint(ctx, []*msgpb.MsgPosition{checkpoint})
		s.NoError(err)
		s.resetMock()
	})

	s.Run("datacoord_return_error", func() {
		s.dc.EXPECT().UpdateChannelCheckpoint(mock.Anything, mock.Anything).
			Return(nil, errors.New("mock"))

		err := s.broker.UpdateChannelCheckpoint(ctx, []*msgpb.MsgPosition{checkpoint})
		s.Error(err)
		s.resetMock()
	})

	s.Run("datacoord_return_failure_status", func() {
		s.dc.EXPECT().UpdateChannelCheckpoint(mock.Anything, mock.Anything).
			Return(merr.Status(errors.New("mock")), nil)

		err := s.broker.UpdateChannelCheckpoint(ctx, []*msgpb.MsgPosition{checkpoint})
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

func (s *dataCoordSuite) TestImportV2() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := &internalpb.ImportRequestInternal{}

	s.Run("normal_case", func() {
		s.dc.EXPECT().ImportV2(mock.Anything, mock.Anything).
			Return(&internalpb.ImportResponse{Status: merr.Status(nil), JobID: "1000"}, nil)
		resp, err := s.broker.ImportV2(ctx, req)
		s.NoError(err)
		s.Equal("1000", resp.GetJobID())
		s.resetMock()
	})
	s.Run("datacoord_return_error", func() {
		s.dc.EXPECT().ImportV2(mock.Anything, mock.Anything).
			Return(nil, errors.New("mock"))
		_, err := s.broker.ImportV2(ctx, req)
		s.Error(err)
		s.resetMock()
	})
}

func TestDataCoordBroker(t *testing.T) {
	suite.Run(t, new(dataCoordSuite))
}
