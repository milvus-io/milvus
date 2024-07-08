package rmq

import (
	"encoding/base64"

	"github.com/cockroachdb/errors"
	"github.com/golang/protobuf/proto"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/milvus-io/milvus/pkg/streaming/util/message"
)

var _ message.MessageID = rmqID(0)

// UnmarshalMessageID unmarshal the message id.
func UnmarshalMessageID(data []byte) (message.MessageID, error) {
	id, err := unmarshalMessageID(data)
	if err != nil {
		return nil, err
	}
	return id, nil
}

// unmashalMessageID unmarshal the message id.
func unmarshalMessageID(data []byte) (rmqID, error) {
	v, n := proto.DecodeVarint(data)
	if n <= 0 || n != len(data) {
		return 0, errors.Wrapf(message.ErrInvalidMessageID, "rmqID: %s", base64.RawStdEncoding.EncodeToString(data))
	}
	return rmqID(protowire.DecodeZigZag(v)), nil
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
func (id rmqID) Marshal() []byte {
	return proto.EncodeVarint(protowire.EncodeZigZag(int64(id)))
}
