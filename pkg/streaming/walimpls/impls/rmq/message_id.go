package rmq

import (
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/streaming/util/message"
)

var _ message.MessageID = rmqID(0)

// UnmarshalMessageID unmarshal the message id.
func UnmarshalMessageID(data string) (message.MessageID, error) {
	id, err := unmarshalMessageID(data)
	if err != nil {
		return nil, err
	}
	return id, nil
}

// unmashalMessageID unmarshal the message id.
func unmarshalMessageID(data string) (rmqID, error) {
	v, err := message.DecodeUint64(data)
	if err != nil {
		return 0, errors.Wrapf(message.ErrInvalidMessageID, "decode rmqID fail with err: %s, id: %s", err.Error(), data)
	}
	return rmqID(v), nil
}

// rmqID is the message id for rmq.
type rmqID int64

// WALName returns the name of message id related wal.
func (id rmqID) WALName() string {
	return walName
}

// LT less than.
func (id rmqID) LT(other message.MessageID) bool {
	return id < other.(rmqID)
}

// LTE less than or equal to.
func (id rmqID) LTE(other message.MessageID) bool {
	return id <= other.(rmqID)
}

// EQ Equal to.
func (id rmqID) EQ(other message.MessageID) bool {
	return id == other.(rmqID)
}

// Marshal marshal the message id.
func (id rmqID) Marshal() string {
	return message.EncodeInt64(int64(id))
}
