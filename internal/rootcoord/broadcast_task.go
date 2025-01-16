package rootcoord

import (
	"context"

	"github.com/milvus-io/milvus/internal/util/streamingutil/util"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
)

var _ task = (*broadcastTask)(nil)

// newBroadcastTask creates a new broadcast task.
func newBroadcastTask(ctx context.Context, core *Core, msgs []message.MutableMessage) *broadcastTask {
	return &broadcastTask{
		baseTask: newBaseTask(ctx, core),
		msgs:     msgs,
	}
}

// BroadcastTask is used to implement the broadcast operation based on the msgstream
// by using the streaming service interface.
// msgstream will be deprecated since 2.6.0 with streaming service, so those code will be removed in the future version.
type broadcastTask struct {
	baseTask
	msgs         []message.MutableMessage // The message wait for broadcast
	walName      string
	resultFuture *syncutil.Future[types.AppendResponses]
}

func (b *broadcastTask) Execute(ctx context.Context) error {
	result := types.NewAppendResponseN(len(b.msgs))
	defer func() {
		b.resultFuture.Set(result)
	}()

	for idx, msg := range b.msgs {
		tsMsg, err := adaptor.NewMsgPackFromMutableMessageV1(msg)
		if err != nil {
			result.FillResponseAtIdx(types.AppendResponse{Error: err}, idx)
			return err
		}
		pchannel := funcutil.ToPhysicalChannel(msg.VChannel())
		msgID, err := b.core.chanTimeTick.broadcastMarkDmlChannels([]string{pchannel}, &msgstream.MsgPack{
			BeginTs: b.ts,
			EndTs:   b.ts,
			Msgs:    []msgstream.TsMsg{tsMsg},
		})
		if err != nil {
			result.FillResponseAtIdx(types.AppendResponse{Error: err}, idx)
			continue
		}
		result.FillResponseAtIdx(types.AppendResponse{
			AppendResult: &types.AppendResult{
				MessageID: adaptor.MustGetMessageIDFromMQWrapperIDBytes(b.walName, msgID[pchannel]),
				TimeTick:  b.ts,
			},
		}, idx)
	}
	return result.UnwrapFirstError()
}

func newMsgStreamAppendOperator(c *Core) *msgstreamAppendOperator {
	return &msgstreamAppendOperator{
		core:    c,
		walName: util.MustSelectWALName(),
	}
}

// msgstreamAppendOperator the code of streamingcoord to make broadcast available on the legacy msgstream.
// Because msgstream is bound to the rootcoord task, so we transfer each broadcast operation into a ddl task.
// to make sure the timetick rule.
// The Msgstream will be deprecated since 2.6.0, so we make a single module to hold it.
type msgstreamAppendOperator struct {
	core    *Core
	walName string
}

// AppendMessages implements the AppendOperator interface for broadcaster service at streaming service.
func (m *msgstreamAppendOperator) AppendMessages(ctx context.Context, msgs ...message.MutableMessage) types.AppendResponses {
	t := &broadcastTask{
		baseTask:     newBaseTask(ctx, m.core),
		msgs:         msgs,
		walName:      m.walName,
		resultFuture: syncutil.NewFuture[types.AppendResponses](),
	}

	if err := m.core.scheduler.AddTask(t); err != nil {
		resp := types.NewAppendResponseN(len(msgs))
		resp.FillAllError(err)
		return resp
	}

	result, err := t.resultFuture.GetWithContext(ctx)
	if err != nil {
		resp := types.NewAppendResponseN(len(msgs))
		resp.FillAllError(err)
		return resp
	}
	return result
}
