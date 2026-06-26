package adaptor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestRetryAppendOverwritesTraceContextWithAppendImplSpan(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	defer otel.SetTracerProvider(prev)

	sourceCtx, sourceSpan := otel.Tracer("test").Start(context.Background(), "source")
	sourceSpan.End()

	msg := message.CreateTestEmptyInsertMesage(1, nil)
	message.InjectTraceContext(sourceCtx, msg)

	var capturedCtx context.Context
	w := &walAdaptorImpl{
		rwWALImpls: newFirstTimeTickWALImpls(func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			capturedCtx = ctx
			return walimplstest.NewTestMessageID(1), nil
		}),
	}

	_, err := w.retryAppendWhenRecoverableError(sourceCtx, msg)
	require.NoError(t, err)

	spans := exporter.GetSpans()
	var appendImpl tracetest.SpanStub
	for _, s := range spans {
		if s.Name == message.SpanNameWALAppendImpl {
			appendImpl = s
			break
		}
	}
	require.Equal(t, message.SpanNameWALAppendImpl, appendImpl.Name)

	capturedSC := trace.SpanContextFromContext(capturedCtx)
	assert.Equal(t, appendImpl.SpanContext.TraceID(), capturedSC.TraceID())
	assert.Equal(t, appendImpl.SpanContext.SpanID(), capturedSC.SpanID())

	msgSC := trace.SpanContextFromContext(message.ExtractTraceContext(context.Background(), msg))
	assert.Equal(t, appendImpl.SpanContext.TraceID(), msgSC.TraceID())
	assert.Equal(t, appendImpl.SpanContext.SpanID(), msgSC.SpanID())
}

func TestCatchupScannerOverwritesTraceContextWithConsumeSpan(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	defer otel.SetTracerProvider(prev)

	sourceCtx, sourceSpan := otel.Tracer("test").Start(context.Background(), "wal.appendimpl")
	sourceSpan.End()

	msgID := walimplstest.NewTestMessageID(1)
	mutableMsg := message.CreateTestEmptyInsertMesage(1, nil)
	mutableMsg.WithTimeTick(100)
	mutableMsg.WithLastConfirmed(msgID)
	message.InjectTraceContext(sourceCtx, mutableMsg)
	immutableMsg := mutableMsg.IntoImmutableMessage(msgID)

	scannerCh := make(chan message.ImmutableMessage, 1)
	scannerCh <- immutableMsg
	close(scannerCh)
	msgCh := make(chan message.ImmutableMessage, 1)
	scanner := &catchupScanner{
		switchableScannerImpl: switchableScannerImpl{
			scannerName: "trace-test",
			logger:      mlog.With(),
			innerWAL:    newFirstTimeTickWALImpls(nil),
			msgChan:     msgCh,
		},
	}
	_, err := scanner.consumeWithScanner(context.Background(), &traceTestScanner{ch: scannerCh})
	require.NoError(t, err)

	capturedMsg := <-msgCh
	spans := exporter.GetSpans()
	var consume tracetest.SpanStub
	for _, s := range spans {
		if s.Name == message.SpanNameWALConsume {
			consume = s
			break
		}
	}
	require.Equal(t, message.SpanNameWALConsume, consume.Name)

	msgSC := trace.SpanContextFromContext(message.ExtractTraceContext(context.Background(), capturedMsg))
	assert.Equal(t, consume.SpanContext.TraceID(), msgSC.TraceID())
	assert.Equal(t, consume.SpanContext.SpanID(), msgSC.SpanID())
}

type traceTestScanner struct {
	ch <-chan message.ImmutableMessage
}

func (s *traceTestScanner) Name() string {
	return "trace-test-scanner"
}

func (s *traceTestScanner) Chan() <-chan message.ImmutableMessage {
	return s.ch
}

func (s *traceTestScanner) Error() error {
	return nil
}

func (s *traceTestScanner) Done() <-chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}

func (s *traceTestScanner) Close() error {
	return nil
}
