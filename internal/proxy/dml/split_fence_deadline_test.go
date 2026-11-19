package dml

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type unavailableSplitTargetWAL struct {
	*splitFenceTestWAL
	targetDeadline time.Time
}

func (w *unavailableSplitTargetWAL) AppendMessages(ctx context.Context, msgs ...message.MutableMessage) streaming.AppendResponses {
	return w.AppendMessagesWithOptions(ctx, msgs)
}

func (w *unavailableSplitTargetWAL) AppendMessagesWithOptions(ctx context.Context, msgs []message.MutableMessage, opts ...streaming.AppendOption) streaming.AppendResponses {
	if len(w.batches) == 0 {
		return w.splitFenceTestWAL.AppendMessagesWithOptions(ctx, msgs, opts...)
	}
	w.targetDeadline, _ = ctx.Deadline()
	<-ctx.Done()
	resp := streaming.AppendResponses{Responses: make([]streaming.AppendResponse, len(msgs))}
	resp.FillAllError(ctx.Err())
	return resp
}

func TestFenceDeadlineBoundsBlockedTargetAppend(t *testing.T) {
	old := paramtable.Get().ProxyCfg.ShardSplitMaxFenceRetryWait.SwapTempValue("350ms")
	t.Cleanup(func() { paramtable.Get().ProxyCfg.ShardSplitMaxFenceRetryWait.SwapTempValue(old) })
	for _, op := range []string{"insert", "delete", "upsert"} {
		t.Run(op, func(t *testing.T) {
			useSingleMessageRepack(t)
			pre, post := twoShardSplit()
			fixture := newSplitFenceFixture(t, pre, post)
			wal := &unavailableSplitTargetWAL{splitFenceTestWAL: installSplitFenceTestWAL(t, splitSource)}
			streaming.SetWALForTest(wal)
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			pks := seqPKs(16)
			start := time.Now()
			var err error
			switch op {
			case "insert":
				task := fixture.insertTask(pks)
				require.NoError(t, task.Execute(ctx))
				err = merr.Error(task.result.GetStatus())
			case "delete":
				err = fixture.deleteTask(pks).Execute(ctx)
			case "upsert":
				err = fixture.upsertTask(t, pks, pks).appendUpsertAttempt(ctx, nil)
			}
			require.ErrorContains(t, err, "partially applied")
			require.ErrorContains(t, err, "outcome is unknown")
			require.NoError(t, ctx.Err())
			require.False(t, wal.targetDeadline.IsZero())
			require.Less(t, wal.targetDeadline.Sub(start), time.Second)
		})
	}
}
