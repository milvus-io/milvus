package pulsarms

import (
	"context"

	"github.com/mitchellh/mapstructure"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
)

type Factory struct {
	dispatcherFactory msgstream.ProtoUDFactory
	// the following members must be public, so that mapstructure.Decode() can access them
	PulsarAddress  string
	ReceiveBufSize int64
	PulsarBufSize  int64
}

func (f *Factory) SetParams(params map[string]interface{}) error {
	err := mapstructure.Decode(params, f)
	if err != nil {
		return err
	}
	return nil
}

func (f *Factory) NewMsgStream(ctx context.Context) (msgstream.MsgStream, error) {
	return newPulsarMsgStream(ctx, f.PulsarAddress, f.ReceiveBufSize, f.PulsarBufSize, f.dispatcherFactory.NewUnmarshalDispatcher())
}

func (f *Factory) NewTtMsgStream(ctx context.Context) (msgstream.MsgStream, error) {
	return newPulsarTtMsgStream(ctx, f.PulsarAddress, f.ReceiveBufSize, f.PulsarBufSize, f.dispatcherFactory.NewUnmarshalDispatcher())
}

func (f *Factory) NewQueryMsgStream(ctx context.Context) (msgstream.MsgStream, error) {
	return f.NewMsgStream(ctx)
}

func NewFactory() msgstream.Factory {
	f := &Factory{
		dispatcherFactory: msgstream.ProtoUDFactory{},
		ReceiveBufSize:    64,
		PulsarBufSize:     64,
	}
	return f
}
