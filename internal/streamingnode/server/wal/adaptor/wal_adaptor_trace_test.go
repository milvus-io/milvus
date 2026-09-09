package adaptor

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/helper"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type chunkRetryTestWALImpls struct {
	*firstTimeTickWALImpls
}

const testMinWALMessageSize = 256 * 1024

func (w *chunkRetryTestWALImpls) WALName() message.WALName {
	return message.WALNamePulsar
}

// newOversizedTestInsertMessage builds an insert message whose marshaled body
// is guaranteed to exceed budget bytes, so the chunking path is entered for a
// realistic (non-degenerate) backend limit.
func newOversizedTestInsertMessage(t *testing.T, budget int) message.MutableMessage {
	msg, err := message.NewInsertMessageBuilderV1().
		WithHeader(&message.InsertMessageHeader{
			CollectionId: 1,
			Partitions: []*message.PartitionSegmentAssignment{
				{PartitionId: 2, Rows: 1000, BinarySize: 1024 * 1024},
			},
		}).
		WithBody(&msgpb.InsertRequest{
			Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_Insert, MsgID: 1},
			CollectionName: strings.Repeat("x", budget*2),
		}).
		WithVChannel("v1").
		BuildMutable()
	require.NoError(t, err)
	return msg
}

func TestAppendWithOptionalChunkingUsesSuccessfulHeadIDOnDurableAssembly(t *testing.T) {
	resource.InitForTest(t)
	params := paramtable.Get()
	oldSplitChunkSN := params.StreamingCfg.SplitChunkSN.SwapTempValue("true")
	t.Cleanup(func() { params.StreamingCfg.SplitChunkSN.SwapTempValue(oldSplitChunkSN) })
	maxMessageSizeKey := params.PulsarCfg.MaxMessageSize.Key
	reserveSizeKey := params.PulsarCfg.MessageReserveSize.Key
	// The smallest budget a valid configuration can express: keep the WAL
	// message-size limit at its supported minimum and reserve all but one byte.
	require.NoError(t, params.Save(maxMessageSizeKey, strconv.Itoa(testMinWALMessageSize)))
	require.NoError(t, params.Save(reserveSizeKey, strconv.Itoa(testMinWALMessageSize-1)))
	t.Cleanup(func() {
		assert.NoError(t, params.Reset(reserveSizeKey))
		assert.NoError(t, params.Reset(maxMessageSizeKey))
	})

	var persisted []message.ImmutableMessage
	var nextID int64
	headAttempts := 0
	inner := newFirstTimeTickWALImpls(func(_ context.Context, msg message.MutableMessage) (message.MessageID, error) {
		id := walimplstest.NewTestMessageID(nextID)
		nextID++
		// Snapshot the record before returning the result: the first head attempt
		// models a broker-persisted record whose acknowledgement was lost.
		persisted = append(persisted, msg.IntoImmutableMessage(id))
		if message.ChunkIndex(msg) == 0 {
			headAttempts++
			if headAttempts == 1 {
				return nil, errors.New("head persisted but acknowledgement lost")
			}
		}
		return id, nil
	})
	walImpls := &chunkRetryTestWALImpls{firstTimeTickWALImpls: inner}
	roWAL := adaptImplsToROWAL(walImpls, func() {})
	defer roWAL.Close()
	writeMetrics := metricsutil.NewWriteMetrics(walImpls.Channel(), walImpls.WALName())
	defer writeMetrics.Close()
	w := &walAdaptorImpl{
		roWALAdaptorImpl: roWAL,
		rwWALImpls:       walImpls,
		writeMetrics:     writeMetrics,
	}

	msg := message.CreateTestEmptyInsertMesage(1, nil).
		WithTimeTick(100).
		WithLastConfirmedUseMessageID()
	require.Greater(t, len(msg.IntoMessageProto().GetPayload()), 1)

	acknowledgedID, err := w.appendWithOptionalChunking(context.Background(), msg)
	require.NoError(t, err)
	require.Equal(t, 2, headAttempts)
	require.Greater(t, len(persisted), 2)
	require.True(t, walimplstest.NewTestMessageID(0).EQ(persisted[0].MessageID()))
	require.True(t, walimplstest.NewTestMessageID(1).EQ(persisted[1].MessageID()))
	require.True(t, walimplstest.NewTestMessageID(1).EQ(acknowledgedID))

	var assembler message.ChunkAssembler
	var assembled message.ImmutableMessage
	for _, physical := range persisted {
		candidate, handled, err := assembler.Push(physical)
		require.NoError(t, err)
		require.True(t, handled)
		if candidate != nil {
			require.Nil(t, assembled)
			assembled = candidate
		}
	}
	require.NotNil(t, assembled)
	assert.True(t, acknowledgedID.EQ(assembled.MessageID()))
	assert.Equal(t, msg.IntoMessageProto().GetPayload(), assembled.IntoImmutableMessageProto().GetPayload())
}

func TestAppendWithOptionalChunkingDisabledUsesSingleRecord(t *testing.T) {
	resource.InitForTest(t)

	// A budget both messages below can be compared against: the oversized
	// insert must exceed it while an ordinary time tick stays well under.
	const chunkBudget = 2048
	params := paramtable.Get()
	oldSplitChunkSN := params.StreamingCfg.SplitChunkSN.SwapTempValue("false")
	t.Cleanup(func() { params.StreamingCfg.SplitChunkSN.SwapTempValue(oldSplitChunkSN) })
	maxMessageSizeKey := params.PulsarCfg.MaxMessageSize.Key
	reserveSizeKey := params.PulsarCfg.MessageReserveSize.Key
	require.NoError(t, params.Save(maxMessageSizeKey, strconv.Itoa(testMinWALMessageSize)))
	require.NoError(t, params.Save(reserveSizeKey, strconv.Itoa(testMinWALMessageSize-chunkBudget)))
	t.Cleanup(func() {
		assert.NoError(t, params.Reset(reserveSizeKey))
		assert.NoError(t, params.Reset(maxMessageSizeKey))
	})

	var persisted []message.MutableMessage
	inner := newFirstTimeTickWALImpls(func(_ context.Context, msg message.MutableMessage) (message.MessageID, error) {
		persisted = append(persisted, msg)
		return walimplstest.NewTestMessageID(int64(len(persisted))), nil
	})
	w := &walAdaptorImpl{
		rwWALImpls: &chunkRetryTestWALImpls{firstTimeTickWALImpls: inner},
	}
	msg := newOversizedTestInsertMessage(t, chunkBudget).
		WithTimeTick(100).
		WithLastConfirmedUseMessageID()
	require.Greater(t, len(msg.IntoMessageProto().GetPayload()), chunkBudget)

	_, err := w.appendWithOptionalChunking(context.Background(), msg)
	require.NoError(t, err)
	require.Len(t, persisted, 1)
	assert.False(t, message.IsChunkedPayload(persisted[0]))
}

func TestAppendWithOptionalChunkingObservesSplitChunkSNHotUpdate(t *testing.T) {
	resource.InitForTest(t)

	const chunkBudget = 2048
	params := paramtable.Get()
	oldSplitChunkSN := params.StreamingCfg.SplitChunkSN.SwapTempValue("false")
	t.Cleanup(func() { params.StreamingCfg.SplitChunkSN.SwapTempValue(oldSplitChunkSN) })
	maxMessageSizeKey := params.PulsarCfg.MaxMessageSize.Key
	reserveSizeKey := params.PulsarCfg.MessageReserveSize.Key
	require.NoError(t, params.Save(maxMessageSizeKey, strconv.Itoa(testMinWALMessageSize)))
	require.NoError(t, params.Save(reserveSizeKey, strconv.Itoa(testMinWALMessageSize-chunkBudget)))
	t.Cleanup(func() {
		assert.NoError(t, params.Reset(reserveSizeKey))
		assert.NoError(t, params.Reset(maxMessageSizeKey))
	})

	var persisted []message.MutableMessage
	inner := newFirstTimeTickWALImpls(func(_ context.Context, msg message.MutableMessage) (message.MessageID, error) {
		persisted = append(persisted, msg)
		return walimplstest.NewTestMessageID(int64(len(persisted))), nil
	})
	w := &walAdaptorImpl{
		rwWALImpls: &chunkRetryTestWALImpls{firstTimeTickWALImpls: inner},
	}

	appendWithSwitch := func(timeTick uint64, enabled bool) []message.MutableMessage {
		params.StreamingCfg.SplitChunkSN.SwapTempValue(strconv.FormatBool(enabled))
		start := len(persisted)
		msg := newOversizedTestInsertMessage(t, chunkBudget).
			WithTimeTick(timeTick).
			WithLastConfirmedUseMessageID()
		_, err := w.appendWithOptionalChunking(context.Background(), msg)
		require.NoError(t, err)
		return persisted[start:]
	}

	disabled := appendWithSwitch(100, false)
	require.Len(t, disabled, 1)
	assert.False(t, message.IsChunkedPayload(disabled[0]))

	enabled := appendWithSwitch(200, true)
	require.Greater(t, len(enabled), 1)
	for _, physical := range enabled {
		assert.True(t, message.IsChunkedPayload(physical))
	}

	disabledAgain := appendWithSwitch(300, false)
	require.Len(t, disabledAgain, 1)
	assert.False(t, message.IsChunkedPayload(disabledAgain[0]))
}

func TestAppendWithOptionalChunkingDoesNotChunkWithoutRecordLimit(t *testing.T) {
	resource.InitForTest(t)
	params := paramtable.Get()
	oldSplitChunkSN := params.StreamingCfg.SplitChunkSN.SwapTempValue("true")
	t.Cleanup(func() { params.StreamingCfg.SplitChunkSN.SwapTempValue(oldSplitChunkSN) })

	var persisted []message.MutableMessage
	w := &walAdaptorImpl{
		rwWALImpls: newFirstTimeTickWALImpls(func(_ context.Context, msg message.MutableMessage) (message.MessageID, error) {
			persisted = append(persisted, msg)
			return walimplstest.NewTestMessageID(1), nil
		}),
	}
	msg := message.CreateTestEmptyInsertMesage(1, nil).
		WithTimeTick(100).
		WithLastConfirmedUseMessageID()

	_, err := w.appendWithOptionalChunking(context.Background(), msg)
	require.NoError(t, err)
	require.Len(t, persisted, 1)
	assert.False(t, message.IsChunkedPayload(persisted[0]))
}

func TestAppendWithOptionalChunkingOverwritesTraceContextWithAppendImplSpan(t *testing.T) {
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

	_, err := w.appendWithOptionalChunking(sourceCtx, msg)
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

func TestAppendWithOptionalChunkingSkipsTraceForTimeTickMessage(t *testing.T) {
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

	_, err := w.appendWithOptionalChunking(sourceCtx, msg)
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
