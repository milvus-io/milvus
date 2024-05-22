package mqbased

import (
	"github.com/milvus-io/milvus/internal/lognode/server/wal"
	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/status"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"go.uber.org/zap"
)

var _ wal.Scanner = (*mqBasedScanner)(nil)

// newMQBasedScanner creates a new scanner based on message queue.
func newMQBasedScanner(channel *logpb.PChannelInfo, c mqwrapper.Consumer) *mqBasedScanner {
	s := &mqBasedScanner{
		channel:  channel,
		c:        c,
		err:      nil,
		ch:       make(chan message.ImmutableMessage, 1),
		closeCh:  make(chan struct{}),
		finishCh: make(chan struct{}),
		logger:   log.With(zap.Any("channel", channel), zap.String("scannerName", c.Subscription())),
		ackPool:  conc.NewPool[struct{}](5),
	}
	go s.execute()
	return s
}

// mqBasedScanner is a scanner based on message queue.
type mqBasedScanner struct {
	channel *logpb.PChannelInfo
	c       mqwrapper.Consumer

	err      error
	ch       chan message.ImmutableMessage
	closeCh  chan struct{}
	finishCh chan struct{}
	logger   *log.MLogger
	ackPool  *conc.Pool[struct{}]
}

// execute executes the scanner.
func (s *mqBasedScanner) execute() {
	defer func() {
		close(s.ch)
		close(s.finishCh)
	}()
	var lastMessageID message.MessageID

	for {
		select {
		case <-s.closeCh:
			return
		case msg, ok := <-s.c.Chan():
			if !ok {
				s.err = status.NewInner("mq consumer unexpected channel closed")
				return
			}
			// filter repeat sent message, which is possible for ack failure.
			incomingMessageID := msg.ID()
			if lastMessageID == nil || lastMessageID.LT(incomingMessageID) {
				s.ch <- message.NewBuilder().
					WithMessageID(msg.ID()).
					WithPayload(msg.Payload()).
					WithProperties(msg.Properties()).
					BuildImmutable()
				lastMessageID = incomingMessageID
			}
			// Ack right now
			s.ackPool.Submit(func() (struct{}, error) {
				s.c.Ack(msg)
				return struct{}{}, nil
			})
		}
	}
}

func (s *mqBasedScanner) Channel() *logpb.PChannelInfo {
	return s.channel
}

func (s *mqBasedScanner) GetLatestMessageID() (message.MessageID, error) {
	msgID, err := s.c.GetLatestMsgID()
	if err != nil {
		return nil, status.NewInner("get latest message id from mq consumer failed, %s", err.Error())
	}
	return msgID, nil
}

func (s *mqBasedScanner) Chan() <-chan message.ImmutableMessage {
	return s.ch
}

func (s *mqBasedScanner) Error() error {
	<-s.finishCh
	return s.err
}

func (s *mqBasedScanner) Done() <-chan struct{} {
	return s.finishCh
}

func (s *mqBasedScanner) Close() error {
	close(s.closeCh)
	<-s.finishCh
	s.c.Close()
	s.ackPool.Release()
	return s.err
}
