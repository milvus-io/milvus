package rmqms

import (
	"context"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
)

type Factory struct {
	dispatcherFactory msgstream.ProtoUDFactory
	receiveBufSize    int64
	rmqBufSize        int64
}

func (f *Factory) NewMsgStream(ctx context.Context) (msgstream.MsgStream, error) {
	return newRmqMsgStream(ctx, f.receiveBufSize, f.rmqBufSize, f.dispatcherFactory.NewUnmarshalDispatcher())
}

func NewFactory(address string, receiveBufSize int64, pulsarBufSize int64) *Factory {
	f := &Factory{
		dispatcherFactory: msgstream.ProtoUDFactory{},
		receiveBufSize:    receiveBufSize,
	}
	return f
}
