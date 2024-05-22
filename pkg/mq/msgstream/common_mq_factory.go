package msgstream

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

var _ Factory = &CommonFactory{}

// CommonFactory is a Factory for creating message streams with common logic.
//
// It contains a function field named newer, which is a function that creates
// an mqwrapper.Client when called.
type CommonFactory struct {
	Newer             func(context.Context) (mqwrapper.Client, error) // client constructor
	DispatcherFactory ProtoUDFactory
	CustomDisposer    func([]string, string) error
}

// NewCommonFactory creates a new CommonFactory.
func (f *CommonFactory) NewClient(ctx context.Context) (mqwrapper.Client, error) {
	return f.Newer(ctx)
}

// NewMsgStream is used to generate a new Msgstream object
func (f *CommonFactory) NewMsgStream(ctx context.Context) (ms MsgStream, err error) {
	defer wrapError(&err, "NewMsgStream")
	cli, err := f.Newer(ctx)
	if err != nil {
		return nil, err
	}
	receiveBufSize := paramtable.Get().MQCfg.ReceiveBufSize.GetAsInt64()
	mqBufSize := paramtable.Get().MQCfg.MQBufSize.GetAsInt64()
	return NewMqMsgStream(context.Background(), receiveBufSize, mqBufSize, cli, f.DispatcherFactory.NewUnmarshalDispatcher())
}

// NewTtMsgStream is used to generate a new TtMsgstream object
func (f *CommonFactory) NewTtMsgStream(ctx context.Context) (ms MsgStream, err error) {
	defer wrapError(&err, "NewTtMsgStream")
	cli, err := f.Newer(ctx)
	if err != nil {
		return nil, err
	}
	receiveBufSize := paramtable.Get().MQCfg.ReceiveBufSize.GetAsInt64()
	mqBufSize := paramtable.Get().MQCfg.MQBufSize.GetAsInt64()
	return NewMqTtMsgStream(context.Background(), receiveBufSize, mqBufSize, cli, f.DispatcherFactory.NewUnmarshalDispatcher())
}

// NewMsgStreamDisposer returns a function that can be used to dispose of a message stream.
// The returned function takes a slice of channel names and a subscription name, and
// disposes of the message stream associated with those arguments.
func (f *CommonFactory) NewMsgStreamDisposer(ctx context.Context) func([]string, string) error {
	if f.CustomDisposer != nil {
		return f.CustomDisposer
	}

	return func(channels []string, subName string) (err error) {
		defer wrapError(&err, "NewMsgStreamDisposer")
		msgs, err := f.NewMsgStream(ctx)
		if err != nil {
			return err
		}
		msgs.AsConsumer(ctx, channels, subName, mqwrapper.SubscriptionPositionUnknown)
		msgs.Close()
		return nil
	}
}

func wrapError(err *error, method string) {
	if *err != nil {
		*err = errors.Wrapf(*err, "in method: %s", method)
	}
}
