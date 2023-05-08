package msgstream

import (
	"context"
	"fmt"

	nmqimplserver "github.com/milvus-io/milvus/internal/mq/mqimpl/natsmq/server"
	rmqimplserver "github.com/milvus-io/milvus/internal/mq/mqimpl/rocksmq/server"
	"github.com/milvus-io/milvus/internal/mq/msgstream/mqwrapper/nmq"
	"github.com/milvus-io/milvus/internal/mq/msgstream/mqwrapper/rmq"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	"go.uber.org/zap"
)

const (
	// MsgStreamTypeRmq represents a message stream based on rocksdb.
	MsgStreamTypeRmq StreamType = 1

	// MsgStreamTypeNmq represents a message stream based on NATS-server.
	MsgStreamTypeNmq StreamType = 2
)

var (
	_ fmt.Stringer      = StreamType(1)
	_ msgstream.Factory = &Factory{}
)

// StreamType represents the type of message stream.
type StreamType int

func (t StreamType) String() string {
	switch t {
	case MsgStreamTypeRmq:
		return "rmq"
	case MsgStreamTypeNmq:
		return "nmq"
	default:
		return ""
	}
}

// Factory is a factory for creating message streams.
//
// It contains a function field named newer, which is a function that creates
// an mqwrapper.Client when called.
type Factory struct {
	streamType        StreamType
	newer             func() (mqwrapper.Client, error) // client constructor
	dispatcherFactory msgstream.ProtoUDFactory
	receiveBufSize    int64
	mqBufSize         int64
}

// NewFactory creates a new message stream factory of type t using the given path.
func NewFactory(t StreamType, path string) msgstream.Factory {
	var newer func() (mqwrapper.Client, error)
	var err error
	switch t {
	case MsgStreamTypeRmq:
		newer = rmq.NewClientWithDefaultOptions
		err = rmqimplserver.InitRocksMQ(path)
	case MsgStreamTypeNmq:
		newer = nmq.NewClientWithDefaultOptions
		err = nmqimplserver.InitNatsMQ(path)
	default:
		panic("unreachable msgstream type")
	}
	if err != nil {
		// Should be stop if fail to create local msgstream service.
		log.Fatal("init msgstream failed", zap.String("msgStreamType", t.String()), zap.Error(err))
	}
	log.Info("init local msgstream success", zap.String("msgStreamType", t.String()), zap.String("path", path))

	return &Factory{
		streamType:        t,
		newer:             newer,
		dispatcherFactory: msgstream.ProtoUDFactory{},
		receiveBufSize:    1024,
		mqBufSize:         1024,
	}
}

// StreamType returns the current stream type used by the factory.
func (f *Factory) StreamType() StreamType {
	return f.streamType
}

// NewMsgStream is used to generate a new Msgstream object
func (f *Factory) NewMsgStream(ctx context.Context) (msgstream.MsgStream, error) {
	cli, err := f.newer()
	if err != nil {
		return nil, err
	}
	return msgstream.NewMqMsgStream(ctx, f.receiveBufSize, f.mqBufSize, cli, f.dispatcherFactory.NewUnmarshalDispatcher())
}

// NewTtMsgStream is used to generate a new TtMsgstream object
func (f *Factory) NewTtMsgStream(ctx context.Context) (msgstream.MsgStream, error) {
	cli, err := f.newer()
	if err != nil {
		return nil, err
	}
	return msgstream.NewMqTtMsgStream(ctx, f.receiveBufSize, f.mqBufSize, cli, f.dispatcherFactory.NewUnmarshalDispatcher())
}

// NewQueryMsgStream is used to generate a new QueryMsgstream object
func (f *Factory) NewQueryMsgStream(ctx context.Context) (msgstream.MsgStream, error) {
	cli, err := f.newer()
	if err != nil {
		return nil, err
	}
	return msgstream.NewMqMsgStream(ctx, f.receiveBufSize, f.mqBufSize, cli, f.dispatcherFactory.NewUnmarshalDispatcher())
}

// NewMsgStreamDisposer returns a function that can be used to dispose of a message stream.
// The returned function takes a slice of channel names and a subscription name, and
// disposes of the message stream associated with those arguments.
func (f *Factory) NewMsgStreamDisposer(ctx context.Context) func([]string, string) error {
	return func(channels []string, subName string) error {
		msgstream, err := f.NewMsgStream(ctx)
		if err != nil {
			return err
		}
		msgstream.AsConsumer(channels, subName, mqwrapper.SubscriptionPositionUnknown)
		msgstream.Close()
		return nil
	}
}
