package adaptor

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/helper"
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

func TestRetryAppendSkipsTraceForTimeTickMessage(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	defer otel.SetTracerProvider(prev)

	sourceCtx, sourceSpan := otel.Tracer("test").Start(context.Background(), "source")
	sourceSC := trace.SpanContextFromContext(sourceCtx)
	sourceSpan.End()

	msgID := walimplstest.NewTestMessageID(1)
	msg := message.CreateTestTimeTickSyncMessage(t, 1, 100, msgID)
	message.InjectTraceContext(sourceCtx, msg)

	var capturedCtx context.Context
	w := &walAdaptorImpl{
		rwWALImpls: newFirstTimeTickWALImpls(func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			capturedCtx = ctx
			return msgID, nil
		}),
	}

	_, err := w.retryAppendWhenRecoverableError(sourceCtx, msg)
	require.NoError(t, err)

	for _, s := range exporter.GetSpans() {
		assert.NotEqual(t, message.SpanNameWALAppendImpl, s.Name)
	}

	capturedSC := trace.SpanContextFromContext(capturedCtx)
	assert.Equal(t, sourceSC.TraceID(), capturedSC.TraceID())
	assert.Equal(t, sourceSC.SpanID(), capturedSC.SpanID())

	msgSC := trace.SpanContextFromContext(message.ExtractTraceContext(context.Background(), msg))
	assert.False(t, msgSC.IsValid())
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

	capturedMsgs := runTraceTestCatchupScanner(t, immutableMsg)
	require.Len(t, capturedMsgs, 1)

	spans := exporter.GetSpans()
	var consume tracetest.SpanStub
	for _, s := range spans {
		if s.Name == message.SpanNameWALCatchupConsume {
			consume = s
			break
		}
	}
	require.Equal(t, message.SpanNameWALCatchupConsume, consume.Name)

	msgSC := trace.SpanContextFromContext(message.ExtractTraceContext(context.Background(), capturedMsgs[0]))
	assert.Equal(t, consume.SpanContext.TraceID(), msgSC.TraceID())
	assert.Equal(t, consume.SpanContext.SpanID(), msgSC.SpanID())
}

func TestCatchupScannerSkipsTraceForTimeTickMessage(t *testing.T) {
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
	mutableMsg := message.CreateTestTimeTickSyncMessage(t, 1, 100, msgID)
	message.InjectTraceContext(sourceCtx, mutableMsg)
	immutableMsg := mutableMsg.IntoImmutableMessage(msgID)

	capturedMsgs := runTraceTestCatchupScanner(t, immutableMsg)
	require.Len(t, capturedMsgs, 1)
	for _, s := range exporter.GetSpans() {
		assert.NotEqual(t, message.SpanNameWALCatchupConsume, s.Name)
	}

	msgSC := trace.SpanContextFromContext(message.ExtractTraceContext(context.Background(), capturedMsgs[0]))
	assert.False(t, msgSC.IsValid())
}

func TestCatchupScannerStartsConsumeSpanOnlyOnTxnCommit(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	defer otel.SetTracerProvider(prev)

	sourceCtx, sourceSpan := otel.Tracer("test").Start(context.Background(), message.SpanNameWALTxn)
	sourceSC := trace.SpanContextFromContext(sourceCtx)
	sourceSpan.End()

	msgs := buildTraceTestTxnImmutableMessages(t, sourceCtx)
	for _, msg := range msgs {
		msgSC := trace.SpanContextFromContext(message.ExtractTraceContext(context.Background(), msg))
		assert.Equal(t, sourceSC.SpanID(), msgSC.SpanID())
	}
	scanner := newTraceTestScannerAdaptor()
	capturedMsgs := runTraceTestCatchupScanner(t, msgs...)
	require.Len(t, capturedMsgs, 3)
	for _, msg := range capturedMsgs {
		scanner.handleUpstream(msg)
	}
	timeTickMsg := message.CreateTestTimeTickSyncMessage(t, 1, 102, walimplstest.NewTestMessageID(4)).IntoImmutableMessage(walimplstest.NewTestMessageID(4))
	scanner.handleUpstream(timeTickMsg)

	consumes := findTraceTestSpansByName(exporter.GetSpans(), message.SpanNameWALCatchupConsume)
	require.Len(t, consumes, 1)
	assert.Equal(t, sourceSC.TraceID(), consumes[0].SpanContext.TraceID())
	assert.Equal(t, sourceSC.SpanID(), consumes[0].Parent.SpanID())

	capturedMsg := scanner.pendingQueue.Next()
	require.Equal(t, message.MessageTypeTxn, capturedMsg.MessageType())
	msgSC := trace.SpanContextFromContext(message.ExtractTraceContext(context.Background(), capturedMsg))
	assert.Equal(t, consumes[0].SpanContext.TraceID(), msgSC.TraceID())
	assert.Equal(t, consumes[0].SpanContext.SpanID(), msgSC.SpanID())
}

func TestScannerAdaptorSkipsConsumeSpanForTailingMessage(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	defer otel.SetTracerProvider(prev)

	sourceCtx, sourceSpan := otel.Tracer("test").Start(context.Background(), message.SpanNameWALAppendImpl)
	sourceSC := trace.SpanContextFromContext(sourceCtx)
	sourceSpan.End()

	msgID := walimplstest.NewTestMessageID(1)
	mutableMsg := message.CreateTestEmptyInsertMesage(1, nil)
	mutableMsg.WithTimeTick(100)
	mutableMsg.WithLastConfirmed(msgID)
	message.InjectTraceContext(sourceCtx, mutableMsg)
	immutableMsg := mutableMsg.IntoImmutableMessage(msgID)

	scanner := newTraceTestScannerAdaptor()
	scanner.handleUpstream(tailingImmutableMesasge{immutableMsg})
	timeTickMsg := message.CreateTestTimeTickSyncMessage(t, 1, 100, msgID).IntoImmutableMessage(msgID)
	scanner.handleUpstream(tailingImmutableMesasge{timeTickMsg})

	for _, s := range exporter.GetSpans() {
		assert.NotEqual(t, message.SpanNameWALCatchupConsume, s.Name)
	}

	capturedMsg := scanner.pendingQueue.Next()
	msgSC := trace.SpanContextFromContext(message.ExtractTraceContext(context.Background(), capturedMsg))
	assert.Equal(t, sourceSC.TraceID(), msgSC.TraceID())
	assert.Equal(t, sourceSC.SpanID(), msgSC.SpanID())
}

func newTraceTestScannerAdaptor() *scannerAdaptorImpl {
	logger := mlog.With()
	scanMetrics := metricsutil.NewScanMetrics(types.PChannelInfo{}).NewScannerMetrics()
	return &scannerAdaptorImpl{
		logger:          logger,
		filterFunc:      func(message.ImmutableMessage) bool { return true },
		reorderBuffer:   utility.NewReOrderBuffer(),
		pendingQueue:    utility.NewPendingQueue(),
		txnBuffer:       utility.NewTxnBuffer(logger, scanMetrics),
		ScannerHelper:   helper.NewScannerHelper("trace-test"),
		metrics:         scanMetrics,
		readRateCounter: utility.NewAverageRateCounter(10 * time.Second),
	}
}

func runTraceTestCatchupScanner(t *testing.T, msgs ...message.ImmutableMessage) []message.ImmutableMessage {
	t.Helper()

	scannerCh := make(chan message.ImmutableMessage, len(msgs))
	for _, msg := range msgs {
		scannerCh <- msg
	}
	close(scannerCh)

	msgCh := make(chan message.ImmutableMessage, len(msgs))
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

	capturedMsgs := make([]message.ImmutableMessage, 0, len(msgs))
	for len(msgCh) > 0 {
		capturedMsgs = append(capturedMsgs, <-msgCh)
	}
	return capturedMsgs
}

func buildTraceTestTxnImmutableMessages(t *testing.T, ctx context.Context) []message.ImmutableMessage {
	t.Helper()

	txnCtx := message.TxnContext{
		TxnID:     1,
		Keepalive: time.Second,
	}
	lastConfirmed := walimplstest.NewTestMessageID(0)

	begin := message.NewBeginTxnMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.BeginTxnMessageHeader{}).
		WithBody(&message.BeginTxnMessageBody{}).
		MustBuildMutable().
		WithTxnContext(txnCtx).
		WithTimeTick(100).
		WithLastConfirmed(lastConfirmed)
	message.InjectTraceContext(ctx, begin)

	body := message.CreateTestEmptyInsertMesage(1, nil).
		WithTxnContext(txnCtx).
		WithTimeTick(101).
		WithLastConfirmed(lastConfirmed)
	message.InjectTraceContext(ctx, body)

	commit := message.NewCommitTxnMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.CommitTxnMessageHeader{}).
		WithBody(&message.CommitTxnMessageBody{}).
		MustBuildMutable().
		WithTxnContext(txnCtx).
		WithTimeTick(102).
		WithLastConfirmed(lastConfirmed)
	message.InjectTraceContext(ctx, commit)

	return []message.ImmutableMessage{
		begin.IntoImmutableMessage(walimplstest.NewTestMessageID(1)),
		body.IntoImmutableMessage(walimplstest.NewTestMessageID(2)),
		commit.IntoImmutableMessage(walimplstest.NewTestMessageID(3)),
	}
}

func findTraceTestSpansByName(spans tracetest.SpanStubs, name string) []tracetest.SpanStub {
	result := make([]tracetest.SpanStub, 0)
	for _, s := range spans {
		if s.Name == name {
			result = append(result, s)
		}
	}
	return result
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
