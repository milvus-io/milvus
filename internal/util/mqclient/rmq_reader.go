package mqclient

import (
	"context"

	"github.com/milvus-io/milvus/internal/util/rocksmq/client/rocksmq"
)

var _ Reader = (*rmqReader)(nil)

type rmqReader struct {
	r rocksmq.Reader
}

func (rr *rmqReader) Topic() string {
	return rr.r.Topic()
}

func (rr *rmqReader) Next(ctx context.Context) (Message, error) {
	rMsg, err := rr.r.Next(ctx)
	if err != nil {
		return nil, err
	}
	msg := &rmqMessage{msg: rMsg}
	return msg, nil
}

func (rr *rmqReader) HasNext() bool {
	return rr.r.HasNext()
}

func (rr *rmqReader) Seek(id MessageID) error {
	msgID := id.(*rmqID).messageID
	return rr.r.Seek(msgID)
}

func (rr *rmqReader) Close() {
	rr.r.Close()
}
