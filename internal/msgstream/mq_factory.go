package msgstream

import (
	"context"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/mitchellh/mapstructure"
	"github.com/zilliztech/milvus-distributed/internal/util/mqclient"
	"github.com/zilliztech/milvus-distributed/internal/util/rocksmq/client/rocksmq"
	rocksmqserver "github.com/zilliztech/milvus-distributed/internal/util/rocksmq/server/rocksmq"
)

type PmsFactory struct {
	dispatcherFactory ProtoUDFactory
	// the following members must be public, so that mapstructure.Decode() can access them
	PulsarAddress  string
	ReceiveBufSize int64
	PulsarBufSize  int64
}

func (f *PmsFactory) SetParams(params map[string]interface{}) error {
	err := mapstructure.Decode(params, f)
	if err != nil {
		return err
	}
	return nil
}

func (f *PmsFactory) NewMsgStream(ctx context.Context) (MsgStream, error) {
	pulsarClient, err := mqclient.NewPulsarClient(pulsar.ClientOptions{URL: f.PulsarAddress})
	if err != nil {
		return nil, err
	}
	return NewMqMsgStream(ctx, f.ReceiveBufSize, f.PulsarBufSize, pulsarClient, f.dispatcherFactory.NewUnmarshalDispatcher())
}

func (f *PmsFactory) NewTtMsgStream(ctx context.Context) (MsgStream, error) {
	pulsarClient, err := mqclient.NewPulsarClient(pulsar.ClientOptions{URL: f.PulsarAddress})
	if err != nil {
		return nil, err
	}
	return NewMqTtMsgStream(ctx, f.ReceiveBufSize, f.PulsarBufSize, pulsarClient, f.dispatcherFactory.NewUnmarshalDispatcher())
}

func (f *PmsFactory) NewQueryMsgStream(ctx context.Context) (MsgStream, error) {
	return f.NewMsgStream(ctx)
}

func NewPmsFactory() Factory {
	f := &PmsFactory{
		dispatcherFactory: ProtoUDFactory{},
		ReceiveBufSize:    64,
		PulsarBufSize:     64,
	}
	return f
}

type RmsFactory struct {
	dispatcherFactory ProtoUDFactory
	// the following members must be public, so that mapstructure.Decode() can access them
	ReceiveBufSize int64
	RmqBufSize     int64
}

func (f *RmsFactory) SetParams(params map[string]interface{}) error {
	err := mapstructure.Decode(params, f)
	if err != nil {
		return err
	}
	return nil
}

func (f *RmsFactory) NewMsgStream(ctx context.Context) (MsgStream, error) {
	rmqClient, err := mqclient.NewRmqClient(rocksmq.ClientOptions{Server: rocksmqserver.Rmq})
	if err != nil {
		return nil, err
	}
	return NewMqMsgStream(ctx, f.ReceiveBufSize, f.RmqBufSize, rmqClient, f.dispatcherFactory.NewUnmarshalDispatcher())
}

func (f *RmsFactory) NewTtMsgStream(ctx context.Context) (MsgStream, error) {
	rmqClient, err := mqclient.NewRmqClient(rocksmq.ClientOptions{Server: rocksmqserver.Rmq})
	if err != nil {
		return nil, err
	}
	return NewMqTtMsgStream(ctx, f.ReceiveBufSize, f.RmqBufSize, rmqClient, f.dispatcherFactory.NewUnmarshalDispatcher())
}

func (f *RmsFactory) NewQueryMsgStream(ctx context.Context) (MsgStream, error) {
	InitMmq()
	return NewMemMsgStream(ctx, f.ReceiveBufSize)
}

func NewRmsFactory() Factory {
	f := &RmsFactory{
		dispatcherFactory: ProtoUDFactory{},
		ReceiveBufSize:    1024,
		RmqBufSize:        1024,
	}

	rocksmqserver.InitRocksMQ("/tmp/milvus_rdb")
	return f
}
