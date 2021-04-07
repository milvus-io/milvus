package msgstream

import "sync"

type SimpleMsgStream struct {
	msgChan chan *MsgPack

	msgCount    int
	msgCountMtx sync.RWMutex
}

func (ms *SimpleMsgStream) Start() {
}

func (ms *SimpleMsgStream) Close() {
}

func (ms *SimpleMsgStream) Chan() <-chan *MsgPack {
	return ms.msgChan
}

func (ms *SimpleMsgStream) AsProducer(channels []string) {
}

func (ms *SimpleMsgStream) AsConsumer(channels []string, subName string) {
}

func (ms *SimpleMsgStream) SetRepackFunc(repackFunc RepackFunc) {
}

func (ms *SimpleMsgStream) Produce(pack *MsgPack) error {
	ms.msgCountMtx.Lock()
	defer ms.msgCountMtx.Unlock()

	ms.msgChan <- pack
	ms.msgCount++

	return nil
}

func (ms *SimpleMsgStream) Broadcast(pack *MsgPack) error {
	return nil
}

func (ms *SimpleMsgStream) Consume() *MsgPack {
	ms.msgCountMtx.RLock()
	defer ms.msgCountMtx.RUnlock()

	if ms.msgCount <= 0 {
		return nil
	}

	return <-ms.msgChan
}

func (ms *SimpleMsgStream) Seek(offset *MsgPosition) error {
	return nil
}

func NewSimpleMsgStream() *SimpleMsgStream {
	return &SimpleMsgStream{
		msgChan:  make(chan *MsgPack, 1024),
		msgCount: 0,
	}
}
