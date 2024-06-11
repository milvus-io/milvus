//go:build test
// +build test

package message

import (
	"strconv"
)

var _ MessageID = testMessageID(0)

const testWALName = "test"

func init() {
	RegisterMessageIDUnmsarshaler(testWALName, UnmarshalTestMessageID)
}

// NewTestMessageID create a new test message id.
func NewTestMessageID(id int64) MessageID {
	return testMessageID(id)
}

// UnmarshalTestMessageID unmarshal the message id.
func UnmarshalTestMessageID(data []byte) (MessageID, error) {
	id, err := unmarshalTestMessageID(data)
	if err != nil {
		return nil, err
	}
	return id, nil
}

// unmashalTestMessageID unmarshal the message id.
func unmarshalTestMessageID(data []byte) (testMessageID, error) {
	id, err := strconv.ParseInt(string(data), 10, 64)
	if err != nil {
		return 0, err
	}
	return testMessageID(id), nil
}

// testMessageID is the message id for rmq.
type testMessageID int64

// WALName returns the name of message id related wal.
func (id testMessageID) WALName() string {
	return testWALName
}

// LT less than.
func (id testMessageID) LT(other MessageID) bool {
	return id < other.(testMessageID)
}

// LTE less than or equal to.
func (id testMessageID) LTE(other MessageID) bool {
	return id <= other.(testMessageID)
}

// EQ Equal to.
func (id testMessageID) EQ(other MessageID) bool {
	return id == other.(testMessageID)
}

// Marshal marshal the message id.
func (id testMessageID) Marshal() []byte {
	return []byte(strconv.FormatInt(int64(id), 10))
}
