package datacoord

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

func TestSessionManagerSuite(t *testing.T) {
	suite.Run(t, new(SessionManagerSuite))
}

type SessionManagerSuite struct {
	suite.Suite

	dn *mocks.MockDataNodeClient

	m SessionManager
}

func (s *SessionManagerSuite) SetupTest() {
	s.dn = mocks.NewMockDataNodeClient(s.T())

	s.m = NewSessionManagerImpl(withSessionCreator(func(ctx context.Context, addr string, nodeID int64) (types.DataNodeClient, error) {
		return s.dn, nil
	}))

	s.m.AddSession(&NodeInfo{1000, "addr-1"})
}

func (s *SessionManagerSuite) TestNotifyChannelOperation() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	info := &datapb.ChannelWatchInfo{
		Vchan: &datapb.VchannelInfo{},
		State: datapb.ChannelWatchState_ToWatch,
		OpID:  1,
	}

	req := &datapb.ChannelOperationsRequest{
		Infos: []*datapb.ChannelWatchInfo{info},
	}
	s.Run("no node", func() {
		err := s.m.NotifyChannelOperation(ctx, 100, req)
		s.Error(err)
	})

	s.Run("fail", func() {
		s.SetupTest()
		s.dn.EXPECT().NotifyChannelOperation(mock.Anything, mock.Anything).Return(nil, errors.New("mock"))

		err := s.m.NotifyChannelOperation(ctx, 1000, req)
		s.Error(err)
	})

	s.Run("normal", func() {
		s.SetupTest()
		s.dn.EXPECT().NotifyChannelOperation(mock.Anything, mock.Anything).Return(merr.Status(nil), nil)

		err := s.m.NotifyChannelOperation(ctx, 1000, req)
		s.NoError(err)
	})
}

func (s *SessionManagerSuite) TestCheckCHannelOperationProgress() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	info := &datapb.ChannelWatchInfo{
		Vchan: &datapb.VchannelInfo{},
		State: datapb.ChannelWatchState_ToWatch,
		OpID:  1,
	}

	s.Run("no node", func() {
		resp, err := s.m.CheckChannelOperationProgress(ctx, 100, info)
		s.Error(err)
		s.Nil(resp)
	})

	s.Run("fail", func() {
		s.SetupTest()
		s.dn.EXPECT().CheckChannelOperationProgress(mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("mock"))

		resp, err := s.m.CheckChannelOperationProgress(ctx, 1000, info)
		s.Error(err)
		s.Nil(resp)
	})

	s.Run("normal", func() {
		s.SetupTest()
		s.dn.EXPECT().CheckChannelOperationProgress(mock.Anything, mock.Anything, mock.Anything).
			Return(
				&datapb.ChannelOperationProgressResponse{
					Status:   merr.Status(nil),
					OpID:     info.OpID,
					State:    info.State,
					Progress: 100,
				},
				nil)

		resp, err := s.m.CheckChannelOperationProgress(ctx, 1000, info)
		s.NoError(err)
		s.Equal(resp.GetState(), info.State)
		s.Equal(resp.OpID, info.OpID)
		s.EqualValues(100, resp.Progress)
	})
}
