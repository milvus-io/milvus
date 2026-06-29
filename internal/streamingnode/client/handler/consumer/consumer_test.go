package consumer

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/mocks/proto/mock_streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

func TestConsumer(t *testing.T) {
	resultCh := make(adaptor.ChanMessageHandler, 1)
	c := newMockedConsumerImpl(t, context.Background(), resultCh)

	mmsg, _ := message.NewInsertMessageBuilderV1().
		WithHeader(&message.InsertMessageHeader{}).
		WithBody(&msgpb.InsertRequest{}).
		WithVChannel("test-1").
		BuildMutable()
	c.recvCh <- newConsumeResponse(walimplstest.NewTestMessageID(1), mmsg)

	msg := <-resultCh
	assert.True(t, msg.MessageID().EQ(walimplstest.NewTestMessageID(1)))

	txnCtx := message.TxnContext{
		TxnID:     1,
		Keepalive: time.Second,
	}
	mmsg, _ = message.NewBeginTxnMessageBuilderV2().
		WithVChannel("test-1").
		WithHeader(&message.BeginTxnMessageHeader{}).
		WithBody(&message.BeginTxnMessageBody{}).
		BuildMutable()
	c.recvCh <- newConsumeResponse(walimplstest.NewTestMessageID(2), mmsg.WithTxnContext(txnCtx))

	mmsg, _ = message.NewInsertMessageBuilderV1().
		WithVChannel("test-1").
		WithHeader(&message.InsertMessageHeader{}).
		WithBody(&msgpb.InsertRequest{}).
		BuildMutable()
	c.recvCh <- newConsumeResponse(walimplstest.NewTestMessageID(3), mmsg.WithTxnContext(txnCtx))

	mmsg, _ = message.NewCommitTxnMessageBuilderV2().
		WithVChannel("test-1").
		WithHeader(&message.CommitTxnMessageHeader{}).
		WithBody(&message.CommitTxnMessageBody{}).
		BuildMutable()
	c.recvCh <- newConsumeResponse(walimplstest.NewTestMessageID(4), mmsg.WithTxnContext(txnCtx))

	msg = <-resultCh
	assert.True(t, msg.MessageID().EQ(walimplstest.NewTestMessageID(4)))
	assert.Equal(t, msg.TxnContext().TxnID, txnCtx.TxnID)
	assert.Equal(t, message.MessageTypeTxn, msg.MessageType())

	c.consumer.Close()
	<-c.consumer.Done()
	assert.NoError(t, c.consumer.Error())
}

func TestConsumerWithCancellation(t *testing.T) {
	resultCh := make(adaptor.ChanMessageHandler, 1)
	ctx, cancel := context.WithCancel(context.Background())
	c := newMockedConsumerImpl(t, ctx, resultCh)

	mmsg, _ := message.NewInsertMessageBuilderV1().
		WithHeader(&message.InsertMessageHeader{}).
		WithBody(&msgpb.InsertRequest{}).
		WithVChannel("test-1").
		BuildMutable()
	c.recvCh <- newConsumeResponse(walimplstest.NewTestMessageID(1), mmsg)
	// The recv goroutinue will be blocked until the context is canceled.
	mmsg, _ = message.NewInsertMessageBuilderV1().
		WithHeader(&message.InsertMessageHeader{}).
		WithBody(&msgpb.InsertRequest{}).
		WithVChannel("test-1").
		BuildMutable()
	c.recvCh <- newConsumeResponse(walimplstest.NewTestMessageID(1), mmsg)

	// The background recv loop should be started.
	time.Sleep(20 * time.Millisecond)

	go func() {
		c.consumer.Close()
	}()

	select {
	case <-c.consumer.Done():
		panic("should not reach here")
	case <-time.After(10 * time.Millisecond):
	}

	cancel()
	select {
	case <-c.consumer.Done():
	case <-time.After(20 * time.Millisecond):
		panic("should not reach here")
	}
	assert.ErrorIs(t, c.consumer.Error(), context.Canceled)
}

func TestRemoteConsumerOverwritesTraceContextWithDistConsumeSpan(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	defer otel.SetTracerProvider(prev)

	sourceCtx, sourceSpan := otel.Tracer("test").Start(context.Background(), message.SpanNameWALConsume)
	sourceSC := trace.SpanContextFromContext(sourceCtx)
	sourceSpan.End()

	h := &captureTraceHandler{ch: make(chan message.HandleParam, 1)}
	c := newMockedConsumerImpl(t, context.Background(), h)

	mmsg, _ := message.NewInsertMessageBuilderV1().
		WithHeader(&message.InsertMessageHeader{}).
		WithBody(&msgpb.InsertRequest{}).
		WithVChannel("test-1").
		BuildMutable()
	message.InjectTraceContext(sourceCtx, mmsg)
	c.recvCh <- newConsumeResponse(walimplstest.NewTestMessageID(1), mmsg)

	var param message.HandleParam
	select {
	case param = <-h.ch:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for consumed message")
	}

	c.consumer.Close()
	<-c.consumer.Done()
	require.NoError(t, c.consumer.Error())

	spans := exporter.GetSpans()
	var distConsume tracetest.SpanStub
	for _, s := range spans {
		if s.Name == message.SpanNameWALDistConsume {
			distConsume = s
			break
		}
	}
	require.Equal(t, message.SpanNameWALDistConsume, distConsume.Name)
	assert.Equal(t, sourceSC.TraceID(), distConsume.SpanContext.TraceID())
	assert.Equal(t, sourceSC.SpanID(), distConsume.Parent.SpanID())

	ctxSC := trace.SpanContextFromContext(param.Ctx)
	assert.Equal(t, distConsume.SpanContext.TraceID(), ctxSC.TraceID())
	assert.Equal(t, distConsume.SpanContext.SpanID(), ctxSC.SpanID())

	msgSC := trace.SpanContextFromContext(message.ExtractTraceContext(context.Background(), param.Message))
	assert.Equal(t, distConsume.SpanContext.TraceID(), msgSC.TraceID())
	assert.Equal(t, distConsume.SpanContext.SpanID(), msgSC.SpanID())
}

func TestRemoteConsumerSkipsTraceForTimeTickMessage(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	defer otel.SetTracerProvider(prev)

	sourceCtx, sourceSpan := otel.Tracer("test").Start(context.Background(), message.SpanNameWALConsume)
	sourceSpan.End()

	h := &captureTraceHandler{ch: make(chan message.HandleParam, 1)}
	c := newMockedConsumerImpl(t, context.Background(), h)

	msgID := walimplstest.NewTestMessageID(1)
	mmsg := message.CreateTestTimeTickSyncMessage(t, 1, 100, msgID)
	message.InjectTraceContext(sourceCtx, mmsg)
	c.recvCh <- newConsumeResponse(msgID, mmsg)

	var param message.HandleParam
	select {
	case param = <-h.ch:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for consumed message")
	}

	c.consumer.Close()
	<-c.consumer.Done()
	require.NoError(t, c.consumer.Error())

	for _, s := range exporter.GetSpans() {
		assert.NotEqual(t, message.SpanNameWALDistConsume, s.Name)
	}

	ctxSC := trace.SpanContextFromContext(param.Ctx)
	assert.False(t, ctxSC.IsValid())

	msgSC := trace.SpanContextFromContext(message.ExtractTraceContext(context.Background(), param.Message))
	assert.False(t, msgSC.IsValid())
}

func TestRemoteConsumerStartsDistConsumeSpanOnlyOnTxnCommit(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	defer otel.SetTracerProvider(prev)

	sourceCtx, sourceSpan := otel.Tracer("test").Start(context.Background(), message.SpanNameWALConsume)
	sourceSC := trace.SpanContextFromContext(sourceCtx)
	sourceSpan.End()

	h := &captureTraceHandler{ch: make(chan message.HandleParam, 1)}
	c := newMockedConsumerImpl(t, context.Background(), h)

	txnCtx := message.TxnContext{
		TxnID:     1,
		Keepalive: time.Second,
	}
	begin := message.NewBeginTxnMessageBuilderV2().
		WithVChannel("test-1").
		WithHeader(&message.BeginTxnMessageHeader{}).
		WithBody(&message.BeginTxnMessageBody{}).
		MustBuildMutable().
		WithTxnContext(txnCtx)
	message.InjectTraceContext(sourceCtx, begin)
	c.recvCh <- newConsumeResponse(walimplstest.NewTestMessageID(1), begin)

	body := message.CreateTestEmptyInsertMesage(1, nil).WithTxnContext(txnCtx)
	message.InjectTraceContext(sourceCtx, body)
	c.recvCh <- newConsumeResponse(walimplstest.NewTestMessageID(2), body)

	commit := message.NewCommitTxnMessageBuilderV2().
		WithVChannel("test-1").
		WithHeader(&message.CommitTxnMessageHeader{}).
		WithBody(&message.CommitTxnMessageBody{}).
		MustBuildMutable().
		WithTxnContext(txnCtx)
	message.InjectTraceContext(sourceCtx, commit)
	c.recvCh <- newConsumeResponse(walimplstest.NewTestMessageID(3), commit)

	var param message.HandleParam
	select {
	case param = <-h.ch:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for consumed txn message")
	}

	c.consumer.Close()
	<-c.consumer.Done()
	require.NoError(t, c.consumer.Error())

	distConsumes := findSpansByName(exporter.GetSpans(), message.SpanNameWALDistConsume)
	require.Len(t, distConsumes, 1)
	assert.Equal(t, sourceSC.TraceID(), distConsumes[0].SpanContext.TraceID())
	assert.Equal(t, sourceSC.SpanID(), distConsumes[0].Parent.SpanID())
	assert.Equal(t, message.MessageTypeTxn, param.Message.MessageType())

	ctxSC := trace.SpanContextFromContext(param.Ctx)
	assert.Equal(t, distConsumes[0].SpanContext.TraceID(), ctxSC.TraceID())
	assert.Equal(t, distConsumes[0].SpanContext.SpanID(), ctxSC.SpanID())

	msgSC := trace.SpanContextFromContext(message.ExtractTraceContext(context.Background(), param.Message))
	assert.Equal(t, distConsumes[0].SpanContext.TraceID(), msgSC.TraceID())
	assert.Equal(t, distConsumes[0].SpanContext.SpanID(), msgSC.SpanID())
}

func findSpansByName(spans tracetest.SpanStubs, name string) []tracetest.SpanStub {
	result := make([]tracetest.SpanStub, 0)
	for _, s := range spans {
		if s.Name == name {
			result = append(result, s)
		}
	}
	return result
}

type mockedConsumer struct {
	consumer Consumer
	recvCh   chan *streamingpb.ConsumeResponse
}

func newMockedConsumerImpl(t *testing.T, ctx context.Context, h message.Handler) *mockedConsumer {
	c := mock_streamingpb.NewMockStreamingNodeHandlerServiceClient(t)
	cc := mock_streamingpb.NewMockStreamingNodeHandlerService_ConsumeClient(t)
	recvCh := make(chan *streamingpb.ConsumeResponse, 10)
	cc.EXPECT().Recv().RunAndReturn(func() (*streamingpb.ConsumeResponse, error) {
		msg, ok := <-recvCh
		if !ok {
			return nil, io.EOF
		}
		return msg, nil
	})
	sendCh := make(chan *streamingpb.ConsumeRequest, 10)
	cc.EXPECT().Send(mock.Anything).RunAndReturn(func(cr *streamingpb.ConsumeRequest) error {
		sendCh <- cr
		return nil
	})
	c.EXPECT().Consume(mock.Anything, mock.Anything).Return(cc, nil)
	cc.EXPECT().CloseSend().RunAndReturn(func() error {
		recvCh <- &streamingpb.ConsumeResponse{Response: &streamingpb.ConsumeResponse_Close{}}
		close(recvCh)
		return nil
	})

	opts := &ConsumerOptions{
		VChannel: "test-1",
		Assignment: &types.PChannelInfoAssigned{
			Channel: types.PChannelInfo{Name: "test", Term: 1},
			Node:    types.StreamingNodeInfo{ServerID: 1, Address: "localhost"},
		},
		DeliverPolicy: options.DeliverPolicyAll(),
		DeliverFilters: []options.DeliverFilter{
			options.DeliverFilterTimeTickGT(100),
		},
		MessageHandler: h,
	}

	recvCh <- &streamingpb.ConsumeResponse{
		Response: &streamingpb.ConsumeResponse_Create{
			Create: &streamingpb.CreateConsumerResponse{},
		},
	}
	recvCh <- &streamingpb.ConsumeResponse{
		Response: &streamingpb.ConsumeResponse_CreateVchannel{
			CreateVchannel: &streamingpb.CreateVChannelConsumerResponse{
				Response: &streamingpb.CreateVChannelConsumerResponse_ConsumerId{
					ConsumerId: 1,
				},
			},
		},
	}
	consumer, err := CreateConsumer(ctx, opts, c)
	if err != nil {
		panic(err)
	}

	return &mockedConsumer{
		consumer: consumer,
		recvCh:   recvCh,
	}
}

func newConsumeResponse(id message.MessageID, msg message.MutableMessage) *streamingpb.ConsumeResponse {
	msg.WithTimeTick(tsoutil.ComposeTSByTime(time.Now()))
	msg.WithLastConfirmed(walimplstest.NewTestMessageID(0))
	immutableMsg := msg.IntoImmutableMessage(id)
	return &streamingpb.ConsumeResponse{
		Response: &streamingpb.ConsumeResponse_Consume{
			Consume: &streamingpb.ConsumeMessageReponse{
				Message: immutableMsg.IntoImmutableMessageProto(),
			},
		},
	}
}

type captureTraceHandler struct {
	ch chan message.HandleParam
}

func (h *captureTraceHandler) Handle(param message.HandleParam) message.HandleResult {
	h.ch <- param
	return message.HandleResult{MessageHandled: true}
}

func (h *captureTraceHandler) Close() {
}
