package pulsarms

import (
	"context"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/zilliztech/milvus-distributed/internal/msgstream/ms"
	pulsar2 "github.com/zilliztech/milvus-distributed/internal/util/mqclient"

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
	pulsarClient, err := pulsar2.NewPulsarClient(pulsar.ClientOptions{URL: f.PulsarAddress})
	if err != nil {
		return nil, err
	}
	return ms.NewMsgStream(ctx, f.ReceiveBufSize, f.PulsarBufSize, pulsarClient, f.dispatcherFactory.NewUnmarshalDispatcher())
}

func (f *Factory) NewTtMsgStream(ctx context.Context) (msgstream.MsgStream, error) {
	pulsarClient, err := pulsar2.NewPulsarClient(pulsar.ClientOptions{URL: f.PulsarAddress})
	if err != nil {
		return nil, err
	}
	return ms.NewTtMsgStream(ctx, f.ReceiveBufSize, f.PulsarBufSize, pulsarClient, f.dispatcherFactory.NewUnmarshalDispatcher())
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
