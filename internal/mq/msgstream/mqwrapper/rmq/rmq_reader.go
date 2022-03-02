package rmq

import (
	"context"

	"github.com/milvus-io/milvus/internal/mq/mqimpl/rocksmq/client"

	"github.com/milvus-io/milvus/internal/mq/msgstream/mqwrapper"
)

var _ mqwrapper.Reader = (*rmqReader)(nil)

// rmqReader contains a rocksmq reader
type rmqReader struct {
	r client.Reader
}

// Topic returns the topic name of a reader
func (rr *rmqReader) Topic() string {
	return rr.r.Topic()
}

// Next returns the next message of reader, blocking until a message is available
func (rr *rmqReader) Next(ctx context.Context) (mqwrapper.Message, error) {
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
func (rr *rmqReader) Seek(id mqwrapper.MessageID) error {
	msgID := id.(*rmqID).messageID
	return rr.r.Seek(msgID)
}

// Close closes the rocksmq reader
func (rr *rmqReader) Close() {
	rr.r.Close()
}
