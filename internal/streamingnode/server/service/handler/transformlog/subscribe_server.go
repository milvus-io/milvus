package transformlog

import (
	"context"
	"io"
	"sync"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/walmanager"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

type SubscribeServer struct {
	walManager walmanager.Manager
	logStream  wal.TransformLogStream
	stream     streamingpb.StreamingNodeHandlerService_SubscribeTransformServer
	ctx        context.Context
	cancel     context.CancelFunc
	outgoing   chan response
	requestMu  sync.Mutex
	subsMu     sync.Mutex
	subs       map[int64]*serverSubscription
	pchannel   string
}

func CreateSubscribeServer(
	walManager walmanager.Manager,
	stream streamingpb.StreamingNodeHandlerService_SubscribeTransformServer,
) (*SubscribeServer, error) {
	createReq, err := contextutil.GetCreateTransformStream(stream.Context())
	if err != nil {
		return nil, status.NewInvalidArgument("create transform stream request is required")
	}
	if createReq.GetPchannel() == nil || createReq.GetPchannel().GetName() == "" {
		return nil, status.NewInvalidArgument("transform stream pchannel is required")
	}
	w, err := walManager.GetAvailableWAL(types.NewPChannelInfoFromProto(createReq.GetPchannel()))
	if err != nil {
		return nil, err
	}
	streamManager := wal.TransformLogFor(w)
	logStream, err := streamManager.AcquireStream(stream.Context(), createReq.GetPchannel().GetName())
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(stream.Context()) //nolint:gosec // Execute defers the stored cancel
	return &SubscribeServer{
		ctx: ctx, cancel: cancel, outgoing: make(chan response, 16),
		walManager: walManager,
		logStream:  logStream,
		stream:     stream,
		subs:       make(map[int64]*serverSubscription),
		pchannel:   createReq.GetPchannel().GetName(),
	}, nil
}

func (s *SubscribeServer) Execute() error {
	defer s.closeAll()
	defer s.cancel()
	if err := s.stream.SendHeader(metadata.Pairs("transform-stream-ready", "true")); err != nil {
		return err
	}
	result := make(chan error, 1)
	go func() { result <- s.receive() }()
	sendResult := make(chan error, 1)
	go func() { sendResult <- s.sendLoop() }()
	select {
	case err := <-result:
		return err
	case err := <-sendResult:
		return err
	case <-s.logStream.Done():
		return s.logStream.Error()
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

// Only this goroutine touches gRPC Send. A slow peer cannot prevent Execute
// from observing WAL shutdown and releasing the local provider's subscriptions.
func (s *SubscribeServer) sendLoop() error {
	for {
		select {
		case outgoing := <-s.outgoing:
			err := s.stream.Send(outgoing.message)
			outgoing.result <- err
			if err != nil {
				return err
			}
		case <-s.ctx.Done():
			return s.ctx.Err()
		}
	}
}

func (s *SubscribeServer) receive() error {
	for {
		req, err := s.stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := s.processRequest(req); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}

func (s *SubscribeServer) processRequest(request *streamingpb.TransformRequest) error {
	s.requestMu.Lock()
	defer s.requestMu.Unlock()
	if err := s.ctx.Err(); err != nil {
		return err
	}
	switch req := request.GetRequest().(type) {
	case *streamingpb.TransformRequest_Create:
		return s.createSubscription(req.Create)
	case *streamingpb.TransformRequest_CloseSubscription:
		id := req.CloseSubscription.GetSubscriptionId()
		vc := s.closeSubscription(id)
		return s.send(&streamingpb.TransformResponse{Response: &streamingpb.TransformResponse_CloseSubscription{CloseSubscription: &streamingpb.CloseTransformSubscriptionResponse{SubscriptionId: id, Vchannel: vc}}})
	case *streamingpb.TransformRequest_CloseStream:
		if err := s.send(&streamingpb.TransformResponse{Response: &streamingpb.TransformResponse_CloseStream{CloseStream: &streamingpb.CloseTransformStreamResponse{}}}); err != nil {
			return err
		}
		return io.EOF
	default:
		return status.NewInvalidRequestSeq("unknown transform request")
	}
}

type response struct {
	message *streamingpb.TransformResponse
	result  chan error
}

func (s *SubscribeServer) send(resp *streamingpb.TransformResponse) error {
	outgoing := response{message: resp, result: make(chan error, 1)}
	select {
	case s.outgoing <- outgoing:
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
	select {
	case err := <-outgoing.result:
		return err
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func (s *SubscribeServer) closeAll() {
	// Finish any in-flight provider call and reject requests received after shutdown.
	s.requestMu.Lock()
	defer s.requestMu.Unlock()
	s.subsMu.Lock()
	subs := s.subs
	s.subs = make(map[int64]*serverSubscription)
	s.subsMu.Unlock()
	for _, sub := range subs {
		sub.handler.Close()
		_ = sub.Close()
	}
	_ = s.logStream.Close()
}

func (s *SubscribeServer) closeSubscription(subscriptionID int64) string {
	s.subsMu.Lock()
	sub := s.subs[subscriptionID]
	delete(s.subs, subscriptionID)
	s.subsMu.Unlock()
	if sub != nil {
		sub.handler.Close()
		_ = sub.Close()
		return sub.VChannel()
	}
	return ""
}

func (s *SubscribeServer) createSubscription(req *streamingpb.CreateTransformSubscriptionRequest) error {
	if req == nil {
		return status.NewInvalidArgument("create transform subscription request is nil")
	}
	s.subsMu.Lock()
	exists := s.subs[req.GetSubscriptionId()] != nil
	s.subsMu.Unlock()
	if exists {
		return s.sendSubscriptionError(req.GetSubscriptionId(), req.GetVchannel(), wal.ErrTransformLogInvalidReadOption)
	}
	handler := newServerEventHandler(
		s.ctx, req.GetSubscriptionId(),
		req.GetVchannel(),
		s.sendSubscriptionEvent,
	)
	handler.onClose = func() { s.subsMu.Lock(); delete(s.subs, req.GetSubscriptionId()); s.subsMu.Unlock() }
	sub, err := s.logStream.Subscribe(s.ctx, wal.TransformLogSubscriptionOption{
		SubscriptionID:     req.GetSubscriptionId(),
		VChannel:           req.GetVchannel(),
		StartAfterTimeTick: req.GetStartAfterTimeTick(),
		EndTimeTick:        req.GetEndTimeTick(),
		Handler:            handler,
	})
	if err != nil {
		mlog.Debug(s.stream.Context(), "streamingnode transform log subscription create failed",
			mlog.FieldPChannel(s.pchannel),
			mlog.FieldVChannel(req.GetVchannel()),
			mlog.Int64("subscriptionID", req.GetSubscriptionId()),
			mlog.Uint64("startAfterTimeTick", req.GetStartAfterTimeTick()),
			mlog.Err(err),
		)
		return s.sendSubscriptionError(req.GetSubscriptionId(), req.GetVchannel(), err)
	}
	s.subsMu.Lock()
	s.subs[req.GetSubscriptionId()] = &serverSubscription{TransformLogSubscription: sub, handler: handler}
	s.subsMu.Unlock()
	mlog.Debug(s.stream.Context(), "streamingnode transform log subscription created",
		mlog.FieldPChannel(s.pchannel),
		mlog.FieldVChannel(req.GetVchannel()),
		mlog.Int64("subscriptionID", req.GetSubscriptionId()),
		mlog.Uint64("startAfterTimeTick", req.GetStartAfterTimeTick()),
		mlog.Uint64("endTimeTick", req.GetEndTimeTick()),
	)
	if err := s.send(&streamingpb.TransformResponse{
		Response: &streamingpb.TransformResponse_Create{
			Create: &streamingpb.CreateTransformSubscriptionResponse{
				SubscriptionId:     req.GetSubscriptionId(),
				Vchannel:           req.GetVchannel(),
				StartAfterTimeTick: req.GetStartAfterTimeTick(),
				EndTimeTick:        req.GetEndTimeTick(),
			},
		},
	}); err != nil {
		handler.Close()
		_ = sub.Close()
		return err
	}
	handler.markReady()
	return nil
}

func (s *SubscribeServer) sendSubscriptionError(subscriptionID int64, vchannel string, err error) error {
	return s.send(&streamingpb.TransformResponse{
		Response: &streamingpb.TransformResponse_SubscriptionError{
			SubscriptionError: &streamingpb.TransformSubscriptionError{
				SubscriptionId: subscriptionID,
				Vchannel:       vchannel,
				Error:          status.AsStreamingError(err).AsPBError(),
				Reason:         contextutil.TransformLogErrorReason(err),
			},
		},
	})
}

func (s *SubscribeServer) sendSubscriptionEvent(event wal.TransformLogStreamEvent) error {
	if event.Err != nil {
		// Local SN streams report canceled reads while shutting down. Those are
		// transport failures: reconnect rather than poison the QN subscription.
		if errors.Is(event.Err, context.Canceled) || errors.Is(event.Err, context.DeadlineExceeded) {
			select {
			case <-s.logStream.Done():
				s.cancel()
				return event.Err
			default:
			}
		}
		mlog.Debug(s.stream.Context(), "streamingnode transform log subscription failed",
			mlog.FieldPChannel(s.pchannel),
			mlog.FieldVChannel(event.VChannel),
			mlog.Int64("subscriptionID", event.SubscriptionID),
			mlog.Err(event.Err),
		)
		return s.sendSubscriptionError(event.SubscriptionID, event.VChannel, event.Err)
	}
	if event.Entry != nil {
		mlog.Debug(s.stream.Context(), "streamingnode transform log forward entry",
			mlog.FieldPChannel(s.pchannel),
			mlog.FieldVChannel(event.VChannel),
			mlog.Int64("subscriptionID", event.SubscriptionID),
			mlog.Uint64("timeTick", event.Entry.GetTimeTick()),
		)
		return s.send(&streamingpb.TransformResponse{
			Response: &streamingpb.TransformResponse_MessageBatch{
				MessageBatch: &streamingpb.TransformMessageBatch{
					SubscriptionId: event.SubscriptionID,
					Vchannel:       event.VChannel,
					Entries:        []*streamingpb.TransformLogEntry{event.Entry},
				},
			},
		})
	}
	if event.SyncUp != nil {
		mlog.Debug(s.stream.Context(), "streamingnode transform log forward sync-up",
			mlog.FieldPChannel(s.pchannel),
			mlog.FieldVChannel(event.VChannel),
			mlog.Int64("subscriptionID", event.SubscriptionID),
			mlog.Uint64("timeTick", event.SyncUp.TimeTick),
		)
		return s.send(&streamingpb.TransformResponse{
			Response: &streamingpb.TransformResponse_SyncUp{
				SyncUp: &streamingpb.TransformSubscriptionSyncUp{
					SubscriptionId: event.SubscriptionID,
					Vchannel:       event.VChannel,
					TimeTick:       event.SyncUp.TimeTick,
				},
			},
		})
	}
	return nil
}

type serverSubscription struct {
	wal.TransformLogSubscription
	handler *serverEventHandler
}

type serverEventHandler struct {
	ctx            context.Context
	subscriptionID int64
	vchannel       string
	ready          chan struct{}
	closed         chan struct{}
	send           func(wal.TransformLogStreamEvent) error
	readyOnce      sync.Once
	closeOnce      sync.Once
	onClose        func()
}

func newServerEventHandler(ctx context.Context, subscriptionID int64, vchannel string, send func(wal.TransformLogStreamEvent) error) *serverEventHandler {
	return &serverEventHandler{
		ctx:            ctx,
		subscriptionID: subscriptionID,
		vchannel:       vchannel,
		ready:          make(chan struct{}),
		closed:         make(chan struct{}),
		send:           send,
	}
}

func (h *serverEventHandler) Handle(event wal.TransformLogStreamEvent) error {
	select {
	case <-h.closed:
		return nil
	default:
	}
	select {
	case <-h.ready:
	case <-h.closed:
		return nil
	case <-h.ctx.Done():
		return h.ctx.Err()
	}
	return h.send(event)
}

func (h *serverEventHandler) Close() {
	h.closeOnce.Do(func() {
		close(h.closed)
		if h.onClose != nil {
			h.onClose()
		}
	})
}

func (h *serverEventHandler) markReady() {
	h.readyOnce.Do(func() {
		close(h.ready)
	})
}
