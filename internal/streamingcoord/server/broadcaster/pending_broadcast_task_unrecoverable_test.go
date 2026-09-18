//go:build test

package broadcaster

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// refusingWALAccesser fails every append with the same error.
type refusingWALAccesser struct {
	streaming.WALAccesser
	err   error
	calls int
}

func (w *refusingWALAccesser) AppendMessages(ctx context.Context, msgs ...message.MutableMessage) types.AppendResponses {
	w.calls++
	resps := types.NewAppendResponseN(len(msgs))
	resps.FillAllError(w.err)
	return resps
}

// TestPendingBroadcastTaskCountsUnrecoverableAppends: an append refused with an
// unrecoverable streaming code is retried exactly like a transient failure --
// the task is still not done, every replica is still pending -- but it is
// counted, per message type and code, so a persisted task a StreamingNode will
// never accept is visible instead of looking like a slow one.
//
// Three shapes of the same refusal are exercised, because the counter is only
// as good as the classification of its INPUT: the *StreamingError an in-process
// append returns, the same error marked the way the resumable producer marks it,
// and the gRPC status a remote StreamingNode answers with, which crosses the
// process boundary as a StreamingClientStatus rather than a *StreamingError.
func TestPendingBroadcastTaskCountsUnrecoverableAppends(t *testing.T) {
	paramtable.Init()
	fenced := status.NewShardFenced("v1", 7, 3)
	overTheWire := status.ConvertStreamingError("append",
		status.NewGRPCStatusFromStreamingError(status.NewUnrecoverableError("refused by the node")).Err())

	cases := []struct {
		name string
		err  error
		code streamingpb.StreamingCode
	}{
		{"unrecoverable", status.NewUnrecoverableError("refused by the node"), streamingpb.StreamingCode_STREAMING_CODE_UNRECOVERABLE},
		{"shard fenced", fenced, streamingpb.StreamingCode_STREAMING_CODE_SHARD_FENCED},
		{"marked by the producer", errors.Mark(status.NewUnrecoverableError("refused"), errors.New("unrecoverable")), streamingpb.StreamingCode_STREAMING_CODE_UNRECOVERABLE},
		{"across the wire", overTheWire, streamingpb.StreamingCode_STREAMING_CODE_UNRECOVERABLE},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			task, wal := newRefusedPendingTask(t, tc.err)
			counter := metrics.StreamingCoordBroadcasterAppendUnrecoverableTotal.WithLabelValues(
				paramtable.GetStringNodeID(), task.msg.MessageType().String(), tc.code.String())
			before := testutil.ToFloat64(counter)

			pending := newPendingBroadcastTask(task)
			err := pending.Execute(context.Background())

			// Retry semantics are untouched: not done, nothing acked, both replicas pending.
			assert.ErrorIs(t, err, errBroadcastTaskIsNotDone)
			assert.Len(t, pending.pendingMessages, 2)
			assert.Equal(t, 1, wal.calls)
			assert.Equal(t, before+2, testutil.ToFloat64(counter), "one count per refused replica")
		})
	}
}

// TestPendingBroadcastTaskDoesNotCountTransientAppendFailures: a failure that a
// retry can clear is not counted, or the counter would fire on every restart
// of a StreamingNode.
func TestPendingBroadcastTaskDoesNotCountTransientAppendFailures(t *testing.T) {
	paramtable.Init()
	transient := []error{
		errors.New("connection refused"),
		status.NewOnShutdownError("node is shutting down"),
		status.NewInner("wal is busy"),
	}
	for _, cause := range transient {
		task, _ := newRefusedPendingTask(t, cause)
		msgType := task.msg.MessageType().String()
		nodeID := paramtable.GetStringNodeID()
		before := map[streamingpb.StreamingCode]float64{}
		for code := range streamingpb.StreamingCode_name {
			before[streamingpb.StreamingCode(code)] = testutil.ToFloat64(
				metrics.StreamingCoordBroadcasterAppendUnrecoverableTotal.WithLabelValues(nodeID, msgType, streamingpb.StreamingCode(code).String()))
		}

		err := newPendingBroadcastTask(task).Execute(context.Background())
		require.ErrorIs(t, err, errBroadcastTaskIsNotDone)

		for code, was := range before {
			assert.Equal(t, was, testutil.ToFloat64(
				metrics.StreamingCoordBroadcasterAppendUnrecoverableTotal.WithLabelValues(nodeID, msgType, code.String())),
				"transient %v must not be counted as %s", cause, code)
		}
	}
}

// newRefusedPendingTask builds a two-replica pending broadcast task whose WAL
// refuses every append with err, installed as the process-wide WAL for the
// test's lifetime.
func newRefusedPendingTask(t *testing.T, err error) (*broadcastTask, *refusingWALAccesser) {
	t.Helper()
	msg := createNewBroadcastMsg([]string{"v1", "v2"}).WithBroadcastID(105)
	proto := createNewWaitAckBroadcastTaskFromMessage(
		msg, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING, []byte{0x00, 0x00})
	task := newBroadcastTaskFromProto(proto, newBroadcasterMetrics(), newAckCallbackScheduler(mlog.With()))
	task.SetLogger(mlog.With())

	wal := &refusingWALAccesser{err: err}
	oldWAL := streaming.WAL()
	streaming.SetWALForTest(wal)
	t.Cleanup(func() { streaming.SetWALForTest(oldWAL) })
	return task, wal
}
