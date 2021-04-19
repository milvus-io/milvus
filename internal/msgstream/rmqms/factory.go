package rmqms

import (
	"context"

	"github.com/mitchellh/mapstructure"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/util/rocksmq"
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
	return newRmqMsgStream(ctx, f.ReceiveBufSize, f.RmqBufSize, f.dispatcherFactory.NewUnmarshalDispatcher())
}

func (f *Factory) NewTtMsgStream(ctx context.Context) (msgstream.MsgStream, error) {
	return newRmqTtMsgStream(ctx, f.ReceiveBufSize, f.RmqBufSize, f.dispatcherFactory.NewUnmarshalDispatcher())
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
