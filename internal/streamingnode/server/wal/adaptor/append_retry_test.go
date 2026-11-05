package adaptor

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestAppendRetriesBackendContextErrorsOnLiveWAL(t *testing.T) {
	for _, backendErr := range []error{context.Canceled, context.DeadlineExceeded} {
		t.Run(backendErr.Error(), func(t *testing.T) {
			attempts := 0
			inner := newFirstTimeTickWALImpls(func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
				require.NoError(t, ctx.Err())
				require.EqualValues(t, 100, msg.TimeTick(), "retries keep the same logical append")
				attempts++
				if attempts == 1 {
					return nil, errors.Wrap(backendErr, "backend attempt ended")
				}
				return finishFenceTestAppend(ctx, 10), nil
			})
			w := newFenceTestWAL(t, inner)
			msg := message.CreateTestEmptyInsertMesage(1, nil).WithTimeTick(100).WithLastConfirmedUseMessageID()
			result, err := w.Append(context.Background(), msg)
			require.NoError(t, err)
			require.True(t, result.MessageID.EQ(walimplstest.NewTestMessageID(10)))
			require.Equal(t, 2, attempts)
			require.True(t, w.IsAvailable())
		})
	}
}

func TestAppendRetryStopsOnOuterCancellationOrFence(t *testing.T) {
	for _, fenced := range []bool{false, true} {
		t.Run(map[bool]string{false: "outer cancellation", true: "writer fenced"}[fenced], func(t *testing.T) {
			cause := errors.New("WAL stopped")
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			attempts := 0
			inner := newFirstTimeTickWALImpls(func(context.Context, message.MutableMessage) (message.MessageID, error) {
				attempts++
				if fenced {
					return nil, errors.Wrap(walimpls.ErrFenced, "writer lost ownership")
				}
				cancel(cause)
				return nil, context.DeadlineExceeded // The outer cancellation determines the outcome.
			})
			w := newFenceTestWAL(t, inner)
			msg := message.CreateTestEmptyInsertMesage(1, nil).WithTimeTick(100).WithLastConfirmedUseMessageID()
			_, err := w.appendOneWithRetry(ctx, msg)
			if fenced {
				require.ErrorIs(t, err, walimpls.ErrFenced)
			} else {
				require.ErrorIs(t, err, cause)
			}
			require.Equal(t, 1, attempts)
		})
	}
}
