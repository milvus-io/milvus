package mqclient

import (
	"context"

	"github.com/milvus-io/milvus/internal/util/rocksmq/client/rocksmq"
)

var _ Reader = (*rmqReader)(nil)

// rmqReader contains a rocksmq reader
type rmqReader struct {
	r rocksmq.Reader
}

// Topic returns the topic name of a reader
func (rr *rmqReader) Topic() string {
	return rr.r.Topic()
}

// Next returns the next message of reader, blocking until a message is available
func (rr *rmqReader) Next(ctx context.Context) (Message, error) {
	rMsg, err := rr.r.Next(ctx)
	if err != nil {
		return nil, err
	}
	msg := &rmqMessage{msg: rMsg}
	return msg, nil
}

// HasNext returns whether reader has next message
func (rr *rmqReader) HasNext() bool {
	return rr.r.HasNext()
}

// Seek seeks the reader position to id
func (rr *rmqReader) Seek(id MessageID) error {
	msgID := id.(*rmqID).messageID
	return rr.r.Seek(msgID)
}

// Close closes the rocksmq reader
func (rr *rmqReader) Close() {
	rr.r.Close()
}
