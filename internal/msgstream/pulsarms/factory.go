package pulsarms

import (
	"context"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
)

type Factory struct {
	dispatcherFactory msgstream.ProtoUDFactory
	address           string
	receiveBufSize    int64
	pulsarBufSize     int64
}

func (f *Factory) NewMsgStream(ctx context.Context) (msgstream.MsgStream, error) {
	return newPulsarMsgStream(ctx, f.address, f.receiveBufSize, f.pulsarBufSize, f.dispatcherFactory.NewUnmarshalDispatcher())
}

func (f *Factory) NewTtMsgStream(ctx context.Context) (msgstream.MsgStream, error) {
	return NewPulsarTtMsgStream(ctx, f.address, f.receiveBufSize, f.pulsarBufSize, f.dispatcherFactory.NewUnmarshalDispatcher())
}

func NewFactory(address string, receiveBufSize int64, pulsarBufSize int64) *Factory {
	f := &Factory{
		dispatcherFactory: msgstream.ProtoUDFactory{},
		address:           address,
		receiveBufSize:    receiveBufSize,
		pulsarBufSize:     pulsarBufSize,
	}
	return f
}
