//go:build test

package broadcaster

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

type traceCaptureWALAccesser struct {
	streaming.WALAccesser
	traceID trace.TraceID
}

func (w *traceCaptureWALAccesser) AppendMessages(ctx context.Context, msgs ...message.MutableMessage) types.AppendResponses {
	w.traceID = trace.SpanContextFromContext(ctx).TraceID()
	resps := types.NewAppendResponseN(len(msgs))
	resps.FillAllError(errors.New("append failed"))
	return resps
}

func TestPendingBroadcastTaskExecuteRestoresTraceContextFromMessage(t *testing.T) {
	originTraceID, err := trace.TraceIDFromHex("0102030405060708090a0b0c0d0e0f10")
	require.NoError(t, err)
	originSpanID, err := trace.SpanIDFromHex("0102030405060708")
	require.NoError(t, err)
	originCtx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: originTraceID,
		SpanID:  originSpanID,
	}))

	msg := createNewBroadcastMsg([]string{"v1", "v2"}).WithBroadcastID(100)
	message.InjectTraceContext(originCtx, msg)
	proto := createNewWaitAckBroadcastTaskFromMessage(
		msg,
		streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING,
		[]byte{0x00, 0x00},
	)
	task := newBroadcastTaskFromProto(proto, newBroadcasterMetrics(), newAckCallbackScheduler(mlog.With()))
	task.SetLogger(mlog.With())

	wal := &traceCaptureWALAccesser{}
	oldWAL := streaming.WAL()
	streaming.SetWALForTest(wal)
	defer streaming.SetWALForTest(oldWAL)

	err = newPendingBroadcastTask(task).Execute(context.Background())
	assert.ErrorIs(t, err, errBroadcastTaskIsNotDone)
	assert.Equal(t, originTraceID, wal.traceID)
}
