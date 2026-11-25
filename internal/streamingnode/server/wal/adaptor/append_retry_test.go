package adaptor

import (
	"context"
	"testing"

	pulsarclient "github.com/apache/pulsar-client-go/pulsar"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	wplog "github.com/zilliztech/woodpecker/woodpecker/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/autowalimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/pulsar"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/wp"
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
			w := newFenceTestWAL(t, autowalimpls.New(inner, nil))
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
			w := newFenceTestWAL(t, autowalimpls.New(inner, nil))
			msg := message.CreateTestEmptyInsertMesage(1, nil).WithTimeTick(100).WithLastConfirmedUseMessageID()
			_, err := w.rwWALImpls.Append(ctx, msg)
			if fenced {
				require.ErrorIs(t, err, walimpls.ErrFenced)
			} else {
				require.ErrorIs(t, err, cause)
			}
			require.Equal(t, 1, attempts)
		})
	}
}

func TestAppendObservationEnclosesAutoRetries(t *testing.T) {
	resource.InitForTest(t)
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter), sdktrace.WithSampler(sdktrace.AlwaysSample()))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	defer otel.SetTracerProvider(previous)
	var attempts []trace.SpanContext
	inner := newFirstTimeTickWALImpls(func(ctx context.Context, _ message.MutableMessage) (message.MessageID, error) {
		attempts = append(attempts, trace.SpanContextFromContext(ctx))
		if len(attempts) == 1 {
			return nil, context.DeadlineExceeded
		}
		return walimplstest.NewTestMessageID(1), nil
	})
	w := &walAdaptorImpl{rwWALImpls: autowalimpls.New(inner, nil)}
	msg := message.CreateTestEmptyInsertMesage(1, nil).WithTimeTick(100).WithLastConfirmedUseMessageID()
	_, err := w.appendOne(context.Background(), msg)
	require.NoError(t, err)
	require.Len(t, attempts, 2)
	spans := exporter.GetSpans()
	require.Len(t, spans, 1, "one adaptor span covers every backend attempt")
	require.Equal(t, message.SpanNameWALAppendImpl, spans[0].Name)
	require.Equal(t, codes.Unset, spans[0].Status.Code)
	for _, attempt := range attempts {
		require.Equal(t, spans[0].SpanContext.SpanID(), attempt.SpanID())
	}
}

func TestAutoLiveWritesKeepAdaptorObservation(t *testing.T) {
	resource.InitForTest(t)
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter), sdktrace.WithSampler(sdktrace.AlwaysSample()))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	defer otel.SetTracerProvider(previous)
	appendID := func(id message.MessageID) walimpls.WALImpls {
		return &recoveryPhysicalWAL{name: id.WALName(), WALImpls: newFirstTimeTickWALImpls(func(context.Context, message.MutableMessage) (message.MessageID, error) {
			return id, nil
		})}
	}
	oldID := pulsar.NewPulsarID(pulsarclient.NewMessageID(1, 0, 0, 0))
	targetID := wp.NewWpID(&wplog.LogMessageId{SegmentId: 1, EntryId: 0})
	old, target := appendID(oldID), appendID(targetID)
	opens := 0
	live := autowalimpls.New(old, func(_ context.Context, name message.WALName, mode types.AccessMode) (walimpls.WALImpls, error) {
		require.Equal(t, message.WALNameWoodpecker, name)
		require.Equal(t, types.AccessModeRW, mode)
		opens++
		if opens == 1 {
			return nil, context.DeadlineExceeded
		}
		return target, nil
	})
	w := &walAdaptorImpl{rwWALImpls: live}
	marker := newFenceTestAlterWALMessage().WithLastConfirmed(oldID)
	_, err := w.appendOne(context.Background(), marker)
	require.NoError(t, err)
	_, err = w.appendOne(context.Background(), message.CreateTestEmptyInsertMesage(1, nil).WithTimeTick(101).WithLastConfirmed(targetID))
	require.NoError(t, err)
	require.Equal(t, 2, opens)
	// Each adaptor call produces one span, including the target open retry
	// and internal anchor append. Physical writers need no decoration.
	spans := exporter.GetSpans()
	require.Len(t, spans, 2)
	for _, span := range spans {
		require.Equal(t, message.SpanNameWALAppendImpl, span.Name)
	}
}
