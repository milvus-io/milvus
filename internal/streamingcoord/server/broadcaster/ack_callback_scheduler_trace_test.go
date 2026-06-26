//go:build test

package broadcaster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

func TestRunAckCallbackWithTrace_OpensChildSpan(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	defer otel.SetTracerProvider(prev)

	// Simulate a broadcast message with a persisted _tc pointing at the
	// original DDL caller's trace.
	originCtx, originSpan := otel.Tracer("test").Start(context.Background(), "ddl.caller")
	originTraceID := trace.SpanContextFromContext(originCtx).TraceID()
	originSpan.End()

	msg := buildTestBroadcastMessageForTrace(t)
	message.InjectTraceContext(originCtx, msg)

	var innerCalled bool
	var childTraceID trace.TraceID
	err := runAckCallbackWithTrace(context.Background(), msg, func(spanCtx context.Context) error {
		innerCalled = true
		childTraceID = trace.SpanContextFromContext(spanCtx).TraceID()
		return nil
	})

	assert.NoError(t, err)
	assert.True(t, innerCalled)
	assert.Equal(t, originTraceID, childTraceID, "ack callback span must share trace id with persisted parent")

	spans := exporter.GetSpans()
	var found bool
	for _, s := range spans {
		if s.Name == message.SpanNameWALBCCallback {
			assert.Equal(t, originTraceID, s.SpanContext.TraceID())
			found = true
		}
	}
	assert.True(t, found, "wal.bc_callback span must be emitted")
}

func TestRunAckCallbackWithTrace_PreservesBaseCancellation(t *testing.T) {
	originCtx, originSpan := otel.Tracer("test").Start(context.Background(), "ddl.caller")
	originTraceID := trace.SpanContextFromContext(originCtx).TraceID()
	originSpan.End()

	msg := buildTestBroadcastMessageForTrace(t)
	message.InjectTraceContext(originCtx, msg)

	baseCtx, cancel := context.WithCancel(context.Background())
	cancel()

	var childTraceID trace.TraceID
	err := runAckCallbackWithTrace(baseCtx, msg, func(spanCtx context.Context) error {
		childTraceID = trace.SpanContextFromContext(spanCtx).TraceID()
		return spanCtx.Err()
	})

	assert.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, originTraceID, childTraceID, "ack callback span must still use the persisted trace")
}
