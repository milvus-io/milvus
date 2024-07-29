package timetick

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/ack"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
)

var (
	_ interceptors.InterceptorWithReady           = (*timeTickAppendInterceptor)(nil)
	_ interceptors.InterceptorWithUnwrapMessageID = (*timeTickAppendInterceptor)(nil)
)

// timeTickAppendInterceptor is a append interceptor.
type timeTickAppendInterceptor struct {
	operator *timeTickSyncOperator
}

// Ready implements AppendInterceptor.
func (impl *timeTickAppendInterceptor) Ready() <-chan struct{} {
	return impl.operator.Ready()
}

// Do implements AppendInterceptor.
func (impl *timeTickAppendInterceptor) DoAppend(ctx context.Context, msg message.MutableMessage, append interceptors.Append) (message.MessageID, error) {
	var timetick uint64
	var msgID message.MessageID
	var err error
	if msg.MessageType() != message.MessageTypeTimeTick {
		// Allocate new acker for message.
		var acker *ack.Acker
		var err error
		if acker, err = impl.operator.AckManager().Allocate(ctx); err != nil {
			return nil, errors.Wrap(err, "allocate timestamp failed")
		}
		defer func() {
			acker.Ack(ack.OptError(err))
			impl.operator.AckManager().AdvanceLastConfirmedMessageID(msgID)
		}()

		// Assign timestamp to message and call append method.
		msg = msg.
			WithTimeTick(acker.Timestamp()).                  // message assigned with these timetick.
			WithLastConfirmed(acker.LastConfirmedMessageID()) // start consuming from these message id, the message which timetick greater than current timetick will never be lost.
		timetick = acker.Timestamp()
	} else {
		timetick = msg.TimeTick()
	}

	// append the message into wal.
	if msgID, err = append(ctx, msg); err != nil {
		return nil, err
	}

	// wrap message id with timetick.
	return wrapMessageIDWithTimeTick{
		MessageID: msgID,
		timetick:  timetick,
	}, nil
}

// Close implements AppendInterceptor.
func (impl *timeTickAppendInterceptor) Close() {
	resource.Resource().TimeTickInspector().UnregisterSyncOperator(impl.operator)
	impl.operator.Close()
}

func (impl *timeTickAppendInterceptor) UnwrapMessageID(r *wal.AppendResult) {
	m := r.MessageID.(wrapMessageIDWithTimeTick)
	r.MessageID = m.MessageID
	r.TimeTick = m.timetick
}

type wrapMessageIDWithTimeTick struct {
	message.MessageID
	timetick uint64
}
