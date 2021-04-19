package msgstream

import (
	"context"
	"sync"
)

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

func (ms *SimpleMsgStream) getMsgCount() int {
	ms.msgCountMtx.RLock()
	defer ms.msgCountMtx.RUnlock()

	return ms.msgCount
}

func (ms *SimpleMsgStream) increaseMsgCount(delta int) {
	ms.msgCountMtx.Lock()
	defer ms.msgCountMtx.Unlock()

	ms.msgCount += delta
}

func (ms *SimpleMsgStream) decreaseMsgCount(delta int) {
	ms.increaseMsgCount(-delta)
}

func (ms *SimpleMsgStream) Produce(pack *MsgPack) error {
	defer ms.increaseMsgCount(1)

	ms.msgChan <- pack

	return nil
}

func (ms *SimpleMsgStream) Broadcast(pack *MsgPack) error {
	return nil
}

func (ms *SimpleMsgStream) Consume() *MsgPack {
	if ms.getMsgCount() <= 0 {
		return nil
	}

	defer ms.decreaseMsgCount(1)

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

type SimpleMsgStreamFactory struct {
}

func (factory *SimpleMsgStreamFactory) SetParams(params map[string]interface{}) error {
	return nil
}

func (factory *SimpleMsgStreamFactory) NewMsgStream(ctx context.Context) (MsgStream, error) {
	return NewSimpleMsgStream(), nil
}

func (factory *SimpleMsgStreamFactory) NewTtMsgStream(ctx context.Context) (MsgStream, error) {
	return NewSimpleMsgStream(), nil
}

func (factory *SimpleMsgStreamFactory) NewQueryMsgStream(ctx context.Context) (MsgStream, error) {
	return NewSimpleMsgStream(), nil
}

func NewSimpleMsgStreamFactory() *SimpleMsgStreamFactory {
	return &SimpleMsgStreamFactory{}
}
