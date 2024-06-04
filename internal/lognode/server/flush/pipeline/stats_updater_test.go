package pipeline

import (
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/internal/lognode/server/flush"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
)

type MqStatsUpdaterSuite struct {
	suite.Suite

	producer *msgstream.MockMsgStream
	updater  *mqStatsUpdater
}

func (s *MqStatsUpdaterSuite) SetupTest() {
	s.producer = msgstream.NewMockMsgStream(s.T())
	s.updater = &mqStatsUpdater{
		stats:    make(map[int64]int64),
		producer: s.producer,
		config: &datanode.nodeConfig{
			vChannelName: "by-dev-rootcoord-dml_0v0",
		},
	}
}

func (s *MqStatsUpdaterSuite) TestSend() {
	s.Run("send_ok", func() {
		s.producer.EXPECT().Produce(mock.Anything).Return(nil)

		s.updater.mut.Lock()
		s.updater.stats[100] = 1024
		s.updater.mut.Unlock()

		err := s.updater.send(tsoutil.GetCurrentTime(), []int64{100})
		s.NoError(err)

		s.updater.mut.Lock()
		_, has := s.updater.stats[100]
		s.updater.mut.Unlock()
		s.False(has)
	})

	s.Run("send_error", func() {
		s.SetupTest()
		s.producer.EXPECT().Produce(mock.Anything).Return(errors.New("mocked"))

		s.updater.mut.Lock()
		s.updater.stats[100] = 1024
		s.updater.mut.Unlock()

		err := s.updater.send(tsoutil.GetCurrentTime(), []int64{100})
		s.Error(err)
	})
}

func TestMqStatsUpdater(t *testing.T) {
	suite.Run(t, new(MqStatsUpdaterSuite))
}
