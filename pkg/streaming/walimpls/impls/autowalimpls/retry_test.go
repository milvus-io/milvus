package autowalimpls

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestRetryPreservesAttemptIdentity(t *testing.T) {
	mockey.PatchConvey("backend timeouts do not cancel a live writer", t, func() {
		backend := &physicalLog{name: message.WALNamePulsar}
		msg := ordinary(100, nil)
		attempts := 0
		id := walimplstest.NewTestMessageID(10)
		mockey.Mock((*physicalLog).Append).To(func(b *physicalLog, ctx context.Context, m message.MutableMessage) (message.MessageID, error) {
			require.Same(t, backend, b)
			require.Same(t, msg, m)
			require.EqualValues(t, 100, m.TimeTick())
			require.NoError(t, ctx.Err())
			attempts++
			switch attempts {
			case 1:
				return nil, context.Canceled
			case 2:
				return nil, context.DeadlineExceeded
			default:
				return id, nil
			}
		}).Build()
		w := New(backend, nil)
		got, err := w.Append(context.Background(), msg)
		require.NoError(t, err)
		require.True(t, id.EQ(got))
		require.Equal(t, 3, attempts)
	})
}

func TestRetryAfterDurableAppendLosesAcknowledgement(t *testing.T) {
	mockey.PatchConvey("a lost backend response is recovered inside the same logical append", t, func() {
		backend, _, open := installBackends(t)
		auto := New(backend, open)
		defer auto.Close()
		backend.failAfterAppend = true
		msg := ordinary(100, nil)
		id, err := auto.Append(t.Context(), msg)
		require.NoError(t, err)
		require.Len(t, backend.messages, 2, "the failed attempt must already have persisted a record")
		first, second := backend.messages[0], backend.messages[1]
		require.False(t, first.MessageID().EQ(second.MessageID()))
		require.True(t, id.EQ(second.MessageID()), "return the acknowledged retry's position")
		require.EqualValues(t, 100, first.TimeTick())
		require.Equal(t, first.TimeTick(), second.TimeTick())
		require.Equal(t, first.Payload(), second.Payload())
		require.Equal(t, first.Properties().ToRawMap(), second.Properties().ToRawMap())
		require.Equal(t, backend.name, auto.WALName())
	})
}

func TestRetryStopsOnWriterLoss(t *testing.T) {
	for _, phase := range []string{"before attempt", "during attempt", "during backoff", "fenced"} {
		t.Run(phase, func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			cause := merr.WrapErrServiceUnavailableMsg("writer closed")
			if phase == "before attempt" {
				cancel(cause)
			}
			attempts := 0
			err := retry(ctx, "retry-test", ordinary(100, nil), func() error {
				attempts++
				switch phase {
				case "during attempt":
					cancel(cause)
				case "during backoff":
					// The minimum first backoff is 5ms; cancel while it waits.
					time.AfterFunc(time.Millisecond, func() { cancel(cause) })
				case "fenced":
					return merr.Wrap(walimpls.ErrFenced, "writer lost ownership")
				}
				return context.DeadlineExceeded
			})
			if phase == "fenced" {
				require.ErrorIs(t, err, walimpls.ErrFenced)
			} else {
				require.ErrorIs(t, err, cause)
			}
			if phase == "before attempt" {
				require.Zero(t, attempts)
			} else {
				require.Equal(t, 1, attempts)
			}
		})
	}
}

func TestAlterWALRetriesOnlyIncompletePhysicalStage(t *testing.T) {
	mockey.PatchConvey("open, anchor and old marker retry independently", t, func() {
		p, w, open := installBackends(t)
		ctx := context.Background()
		opens := 0
		auto := New(p, func(ctx context.Context, name message.WALName, mode types.AccessMode) (walimpls.WALImpls, error) {
			opens++
			if opens == 1 {
				return nil, context.DeadlineExceeded
			}
			return open(ctx, name, mode)
		})
		first, err := auto.Append(ctx, ordinary(10, nil))
		require.NoError(t, err)
		w.failAfterAppend = true
		p.failAfterAppend = true
		p.onLostResponse = func() {
			require.Equal(t, p.name, auto.WALName())
			require.Len(t, w.messages, 2)
		}
		a := alter(20, w.name, first)
		boundary, err := auto.Append(ctx, a)
		require.NoError(t, err)
		require.Equal(t, 2, opens)
		require.Equal(t, w.name, auto.WALName())
		require.Len(t, w.messages, 2, "old-end retry must not append another anchor")
		require.Len(t, p.messages, 3)
		start, err := message.WALSwitchStart(a)
		require.NoError(t, err)
		require.True(t, start.EQ(w.messages[1].MessageID()))
		for _, record := range p.messages[1:] {
			linked, err := message.WALSwitchStart(record)
			require.NoError(t, err)
			require.True(t, start.EQ(linked))
			require.EqualValues(t, 20, record.TimeTick())
		}
		require.True(t, boundary.EQ(p.messages[2].MessageID()))
	})
}

func TestAlterWALWriterLossBeforePublication(t *testing.T) {
	for _, stage := range []string{"open", "anchor", "old marker"} {
		t.Run(stage, func(t *testing.T) {
			mockey.PatchConvey("fencing does not publish the new writer", t, func() {
				p := &physicalLog{name: message.WALNamePulsar}
				w := &physicalLog{name: message.WALNameWoodpecker}
				anchorID := walimplstest.NewTestMessageID(1)
				mockey.Mock((*physicalLog).Append).To(func(b *physicalLog, _ context.Context, _ message.MutableMessage) (message.MessageID, error) {
					if stage == "anchor" || b == p {
						return nil, walimpls.ErrFenced
					}
					return anchorID, nil
				}).Build()
				auto := New(p, func(context.Context, message.WALName, types.AccessMode) (walimpls.WALImpls, error) {
					if stage == "open" {
						return nil, walimpls.ErrFenced
					}
					return w, nil
				})
				_, err := auto.Append(context.Background(), alter(20, w.name, walimplstest.NewTestMessageID(0)))
				require.ErrorIs(t, err, walimpls.ErrFenced)
				require.Equal(t, p.name, auto.WALName())
			})
		})
	}
}
