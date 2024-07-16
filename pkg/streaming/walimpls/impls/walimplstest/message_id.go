//go:build test
// +build test

package walimplstest

import (
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
)

var _ message.MessageID = testMessageID(0)

// NewTestMessageID create a new test message id.
func NewTestMessageID(id int64) message.MessageID {
	return testMessageID(id)
}

// UnmarshalTestMessageID unmarshal the message id.
func UnmarshalTestMessageID(data string) (message.MessageID, error) {
	id, err := unmarshalTestMessageID(data)
	if err != nil {
		return nil, err
	}
	return id, nil
}

// unmashalTestMessageID unmarshal the message id.
func unmarshalTestMessageID(data string) (testMessageID, error) {
	id, err := message.DecodeInt64(data)
	if err != nil {
		return 0, err
	}
	return testMessageID(id), nil
}

// testMessageID is the message id for rmq.
type testMessageID int64

// WALName returns the name of message id related wal.
func (id testMessageID) WALName() string {
	return WALName
}

// LT less than.
func (id testMessageID) LT(other message.MessageID) bool {
	return id < other.(testMessageID)
}

// LTE less than or equal to.
func (id testMessageID) LTE(other message.MessageID) bool {
	return id <= other.(testMessageID)
}

// EQ Equal to.
func (id testMessageID) EQ(other message.MessageID) bool {
	return id == other.(testMessageID)
}

// Marshal marshal the message id.
func (id testMessageID) Marshal() string {
	return message.EncodeInt64(int64(id))
}
