package rmqmsgstream

import "github.com/zilliztech/milvus-distributed/internal/msgstream"

type RmqMsgStream struct {
}

func NewRmqMsgStream() *RmqMsgStream {
	return nil
}

func (ms *RmqMsgStream) Start() {

}

func (ms *RmqMsgStream) Close() {

}

func (ms *RmqMsgStream) Produce(pack *msgstream.MsgPack) error {
	return nil
}

func (ms *RmqMsgStream) Consume() *msgstream.MsgPack {
	return nil
}

func (ms *RmqMsgStream) Chan() <-chan *msgstream.MsgPack {
	return nil
}
