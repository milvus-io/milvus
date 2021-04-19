package rmqms

import (
	"context"

	"github.com/zilliztech/milvus-distributed/internal/msgstream/ms"
	rocksmq2 "github.com/zilliztech/milvus-distributed/internal/util/mqclient"
	client "github.com/zilliztech/milvus-distributed/internal/util/rocksmq/client/rocksmq"

	"github.com/mitchellh/mapstructure"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/msgstream/memms"
	"github.com/zilliztech/milvus-distributed/internal/util/rocksmq/server/rocksmq"
)

type Factory struct {
	dispatcherFactory msgstream.ProtoUDFactory
	// the following members must be public, so that mapstructure.Decode() can access them
	ReceiveBufSize int64
	RmqBufSize     int64
}

func (f *Factory) SetParams(params map[string]interface{}) error {
	err := mapstructure.Decode(params, f)
	if err != nil {
		return err
	}
	return nil
}

func (f *Factory) NewMsgStream(ctx context.Context) (msgstream.MsgStream, error) {
	rmqClient, err := rocksmq2.NewRmqClient(client.ClientOptions{Server: rocksmq.Rmq})
	if err != nil {
		return nil, err
	}
	return ms.NewMsgStream(ctx, f.ReceiveBufSize, f.RmqBufSize, rmqClient, f.dispatcherFactory.NewUnmarshalDispatcher())
}

func (f *Factory) NewTtMsgStream(ctx context.Context) (msgstream.MsgStream, error) {
	rmqClient, err := rocksmq2.NewRmqClient(client.ClientOptions{Server: rocksmq.Rmq})
	if err != nil {
		return nil, err
	}
	return ms.NewTtMsgStream(ctx, f.ReceiveBufSize, f.RmqBufSize, rmqClient, f.dispatcherFactory.NewUnmarshalDispatcher())
}

func (f *Factory) NewQueryMsgStream(ctx context.Context) (msgstream.MsgStream, error) {
	memms.InitMmq()
	return memms.NewMemMsgStream(ctx, f.ReceiveBufSize)
}

func NewFactory() msgstream.Factory {
	f := &Factory{
		dispatcherFactory: msgstream.ProtoUDFactory{},
		ReceiveBufSize:    1024,
		RmqBufSize:        1024,
	}

	rocksmq.InitRocksMQ("/tmp/milvus_rdb")
	return f
}
