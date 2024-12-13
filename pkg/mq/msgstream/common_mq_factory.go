package msgstream

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/mq/common"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
)

var _ Factory = &CommonFactory{}

// CommonFactory is a Factory for creating message streams with common logic.
//
// It contains a function field named newer, which is a function that creates
// an mqwrapper.Client when called.
type CommonFactory struct {
	Newer             func(context.Context) (mqwrapper.Client, error) // client constructor
	DispatcherFactory ProtoUDFactory
	ReceiveBufSize    int64
	MQBufSize         int64
}

// NewMsgStream is used to generate a new Msgstream object
func (f *CommonFactory) NewMsgStream(initCtx context.Context) (ms MsgStream, err error) {
	defer wrapError(&err, "NewMsgStream")
	cli, err := f.Newer(context.TODO())
	if err != nil {
		return nil, err
	}
	return NewMqMsgStream(initCtx, f.ReceiveBufSize, f.MQBufSize, cli, f.DispatcherFactory.NewUnmarshalDispatcher())
}

// NewTtMsgStream is used to generate a new TtMsgstream object
func (f *CommonFactory) NewTtMsgStream(ctx context.Context) (ms MsgStream, err error) {
	defer wrapError(&err, "NewTtMsgStream")
	cli, err := f.Newer(ctx)
	if err != nil {
		return nil, err
	}
	return NewMqTtMsgStream(context.Background(), f.ReceiveBufSize, f.MQBufSize, cli, f.DispatcherFactory.NewUnmarshalDispatcher())
}

// NewMsgStreamDisposer returns a function that can be used to dispose of a message stream.
// The returned function takes a slice of channel names and a subscription name, and
// disposes of the message stream associated with those arguments.
func (f *CommonFactory) NewMsgStreamDisposer(ctx context.Context) func([]string, string) error {
	return func(channels []string, subName string) (err error) {
		defer wrapError(&err, "NewMsgStreamDisposer")
		msgs, err := f.NewMsgStream(ctx)
		if err != nil {
			return err
		}
		msgs.AsConsumer(ctx, channels, subName, common.SubscriptionPositionUnknown)
		msgs.Close()
		return nil
	}
}

func wrapError(err *error, method string) {
	if *err != nil {
		*err = errors.Wrapf(*err, "in method: %s", method)
	}
}
