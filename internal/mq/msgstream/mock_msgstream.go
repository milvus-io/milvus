package msgstream

type MockMsgStream struct {
	MsgStream
	AsProducerFunc    func(channels []string)
	BroadcastMarkFunc func(*MsgPack) (map[string][]MessageID, error)
	BroadcastFunc     func(*MsgPack) error
}

func NewMockMsgStream() *MockMsgStream {
	return &MockMsgStream{}
}

func (m MockMsgStream) AsProducer(channels []string) {
	m.AsProducerFunc(channels)
}

func (m MockMsgStream) BroadcastMark(pack *MsgPack) (map[string][]MessageID, error) {
	return m.BroadcastMarkFunc(pack)
}

func (m MockMsgStream) Broadcast(pack *MsgPack) error {
	return m.BroadcastFunc(pack)
}
