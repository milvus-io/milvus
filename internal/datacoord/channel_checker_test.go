package datacoord

import (
	"testing"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/stretchr/testify/suite"
	"golang.org/x/net/context"
)

func TestChannelChecker(t *testing.T) {
	suite.Run(t, new(ChannelCheckerSuite))
}

type ChannelCheckerSuite struct {
	suite.Suite
	c      *ChannelChecker
	nodeID UniqueID
}

func (s *ChannelCheckerSuite) SetupSuite() {
	session := NewSessionManager()
	session.sessions.data = map[int64]*Session{
		s.nodeID: {
			info:   &NodeInfo{s.nodeID, "mock_address"},
			client: getSuccessDNMock(),
		},
	}

	s.c = NewChannelChecker(session)
}

func (s *ChannelCheckerSuite) TearDownSuite() {
	if s.c != nil {
		s.c.Close()
	}
}

func (s *ChannelCheckerSuite) TestSubmit_SkipCheck() {
	tests := []struct {
		description string
		singleOp    *SingleChannelOp
		expected    *checkResult
	}{
		{description: "towatch",
			singleOp: &SingleChannelOp{
				Add, s.nodeID,
				&channel{Name: "channel1", CollectionID: 100},
				&datapb.ChannelWatchInfo{State: datapb.ChannelWatchState_ToWatch}},
			expected: &checkResult{channel: "channel1", state: datapb.ChannelWatchState_WatchFailure},
		},

		{description: "torelease",
			singleOp: &SingleChannelOp{
				Add,
				s.nodeID,
				&channel{Name: "channel1", CollectionID: 100},
				&datapb.ChannelWatchInfo{State: datapb.ChannelWatchState_ToRelease}},
			expected: &checkResult{channel: "channel1", state: datapb.ChannelWatchState_ReleaseFailure},
		}}

	for _, test := range tests {
		ctx := context.Background()
		s.Run(test.description, func() {
			s.c.Submit(ctx, test.singleOp, true)

			gotResult := <-s.c.Watcher()
			s.Equal(test.expected.channel, gotResult.channel)
			s.Equal(test.expected.state, gotResult.state)
			s.True(s.c.Empty())
		})
	}
}
