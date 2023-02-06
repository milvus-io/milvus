package msgstream

type WastedMockMsgStream struct {
	MsgStream
	AsProducerFunc    func(channels []string)
	BroadcastMarkFunc func(*MsgPack) (map[string][]MessageID, error)
	BroadcastFunc     func(*MsgPack) error
	ChanFunc          func() <-chan *MsgPack
}

func NewWastedMockMsgStream() *WastedMockMsgStream {
	return &WastedMockMsgStream{}
}

func (m WastedMockMsgStream) AsProducer(channels []string) {
	m.AsProducerFunc(channels)
}

func (m WastedMockMsgStream) Broadcast(pack *MsgPack) (map[string][]MessageID, error) {
	return m.BroadcastMarkFunc(pack)
}

func (m WastedMockMsgStream) Chan() <-chan *MsgPack {
	return m.ChanFunc()
}
