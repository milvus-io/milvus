package querynode

import (
	"errors"

	"github.com/milvus-io/milvus/internal/proto/internalpb"

	"github.com/milvus-io/milvus/internal/mq/msgstream"
)

type mockMessageID struct {
	msgstream.MessageID
	serialize          func() []byte
	atEarliestPosition bool
	lessOrEqualThan    func(msgID []byte) (bool, error)
}

func (m mockMessageID) Serialize() []byte {
	if m.serialize != nil {
		return m.serialize()
	}
	return nil
}

func (m mockMessageID) AtEarliestPosition() bool {
	return m.atEarliestPosition
}

func (m mockMessageID) LessOrEqualThan(msgID []byte) (bool, error) {
	if m.lessOrEqualThan != nil {
		return m.lessOrEqualThan(msgID)
	}
	return false, errors.New("mock")
}

func newMockMessageID() *mockMessageID {
	return &mockMessageID{}
}

type mockMsgStream struct {
	msgstream.MsgStream
	asProducer     func([]string)
	asConsumer     func([]string, string)
	seek           func([]*internalpb.MsgPosition) error
	setRepack      func(repackFunc msgstream.RepackFunc)
	getLatestMsgID func(channel string) (msgstream.MessageID, error)
	close          func()
}

func (m *mockMsgStream) AsProducer(producers []string) {
	if m.asProducer != nil {
		m.asProducer(producers)
	}
}

func (m *mockMsgStream) AsConsumer(consumers []string, subName string) {
	if m.asConsumer != nil {
		m.asConsumer(consumers, subName)
	}
}

func (m *mockMsgStream) Seek(position []*internalpb.MsgPosition) error {
	if m.seek != nil {
		return m.seek(position)
	}
	return errors.New("mock")
}

func (m *mockMsgStream) SetRepackFunc(repackFunc msgstream.RepackFunc) {
	if m.setRepack != nil {
		m.setRepack(repackFunc)
	}
}

func (m *mockMsgStream) GetLatestMsgID(channel string) (msgstream.MessageID, error) {
	if m.getLatestMsgID != nil {
		return m.getLatestMsgID(channel)
	}
	return nil, errors.New("mock")
}

func (m *mockMsgStream) Close() {
	if m.close != nil {
		m.close()
	}
}

func newMockMsgStream() *mockMsgStream {
	return &mockMsgStream{}
}
