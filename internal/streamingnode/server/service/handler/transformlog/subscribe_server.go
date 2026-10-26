package transformlog

import (
	"context"
	"io"
	"sync"

	"github.com/cockroachdb/errors"

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
	events     chan wal.TransformLogStreamEvent
	subsMu     sync.Mutex
	subs       map[int64]wal.TransformLogSubscription
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
	w, err := walManager.GetAvailableWAL(types.NewPChannelInfoFromProto(createReq.GetPchannel()))
	if err != nil {
		return nil, err
	}
	streamManager := w.TransformLog()
	logStream, err := streamManager.AcquireStream(stream.Context(), createReq.GetPchannel().GetName())
	if err != nil {
		return nil, err
	}
	if err := stream.Send(&streamingpb.TransformResponse{
		Response: &streamingpb.TransformResponse_CreateStream{CreateStream: &streamingpb.CreateTransformStreamResponse{}},
	}); err != nil {
		_ = logStream.Close()
		return nil, err
	}
	return &SubscribeServer{
		walManager: walManager,
		logStream:  logStream,
		stream:     stream,
		subs:       make(map[int64]wal.TransformLogSubscription),
		pchannel:   createReq.GetPchannel().GetName(),
	}, nil
}

func (s *SubscribeServer) Execute() error {
	defer s.closeAll()
	s.events = make(chan wal.TransformLogStreamEvent)
	ctx, cancel := context.WithCancel(s.stream.Context())
	defer cancel()
	requests := make(chan *streamingpb.TransformRequest)
	recvErr := make(chan error, 1)
	// Recv may block while a Summary read fails. Keep request reception separate
	// so Done/Error can terminate the RPC without waiting for another request.
	go func() {
		for {
			req, err := s.stream.Recv()
			if err != nil {
				recvErr <- err
				return
			}
			select {
			case requests <- req:
			case <-ctx.Done():
				return
			}
		}
	}()
	for {
		var req *streamingpb.TransformRequest
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.logStream.Done():
			return s.logStream.Error()
		case err := <-recvErr:
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		case event := <-s.events:
			if err := s.sendSubscriptionEvent(event); err != nil {
				return err
			}
			continue
		case req = <-requests:
		}
		switch req := req.GetRequest().(type) {
		case *streamingpb.TransformRequest_Create:
			if err := s.createSubscription(req.Create); err != nil {
				return err
			}
		case *streamingpb.TransformRequest_CloseSubscription:
			subscriptionID := req.CloseSubscription.GetSubscriptionId()
			vchannel := s.closeSubscription(subscriptionID)
			if err := s.send(&streamingpb.TransformResponse{
				Response: &streamingpb.TransformResponse_CloseSubscription{
					CloseSubscription: &streamingpb.CloseTransformSubscriptionResponse{
						SubscriptionId: subscriptionID,
						Vchannel:       vchannel,
					},
				},
			}); err != nil {
				return err
			}
		case *streamingpb.TransformRequest_CloseStream:
			return s.send(&streamingpb.TransformResponse{
				Response: &streamingpb.TransformResponse_CloseStream{
					CloseStream: &streamingpb.CloseTransformStreamResponse{},
				},
			})
		default:
			return status.NewInvalidRequestSeq("unknown transform request")
		}
	}
}

func (s *SubscribeServer) send(resp *streamingpb.TransformResponse) error {
	return s.stream.Send(resp)
}

func (s *SubscribeServer) closeAll() {
	s.subsMu.Lock()
	subs := s.subs
	s.subs = make(map[int64]wal.TransformLogSubscription)
	s.subsMu.Unlock()
	for _, sub := range subs {
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
		_ = sub.Close()
		return sub.VChannel()
	}
	return ""
}

func (s *SubscribeServer) createSubscription(req *streamingpb.CreateTransformSubscriptionRequest) error {
	if req == nil {
		return status.NewInvalidArgument("create transform subscription request is nil")
	}
	handler := newServerEventHandler(
		req.GetSubscriptionId(),
		req.GetVchannel(),
		s.events,
	)
	sub, err := s.logStream.Subscribe(s.stream.Context(), wal.TransformLogSubscriptionOption{
		SubscriptionID:     req.GetSubscriptionId(),
		VChannel:           req.GetVchannel(),
		StartAfterTimeTick: req.GetStartAfterTimeTick(),
		EndTimeTick:        req.GetEndTimeTick(),
		Handler:            handler,
	})
	if err != nil {
		handler.Close()
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
	if old := s.subs[req.GetSubscriptionId()]; old != nil {
		_ = old.Close()
	}
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
		// Unblock callbacks before joining the worker: the failed response
		// leaves the ready gate closed, including for cancellation events.
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
			},
		},
	})
}

func (s *SubscribeServer) sendSubscriptionEvent(event wal.TransformLogStreamEvent) error {
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

// Stop event delivery before canceling/joining the Summary worker. Its internal
// cancellation is not a subscription failure when the client requested Close.
type serverSubscription struct {
	wal.TransformLogSubscription
	handler *serverEventHandler
}

func (s *serverSubscription) Close() error {
	s.handler.Close()
	return s.TransformLogSubscription.Close()
}

type serverEventHandler struct {
	subscriptionID int64
	vchannel       string
	ready          chan struct{}
	closed         chan struct{}
	events         chan<- wal.TransformLogStreamEvent
	readyOnce      sync.Once
	closeOnce      sync.Once
}

func newServerEventHandler(subscriptionID int64, vchannel string, events chan<- wal.TransformLogStreamEvent) *serverEventHandler {
	return &serverEventHandler{
		subscriptionID: subscriptionID,
		vchannel:       vchannel,
		ready:          make(chan struct{}),
		closed:         make(chan struct{}),
		events:         events,
	}
}

func (h *serverEventHandler) Handle(event wal.TransformLogStreamEvent) {
	select {
	case <-h.ready:
	case <-h.closed:
		return
	}
	// Execute owns sends and transport errors. Closing the handler releases a
	// worker blocked on delivery before Close joins that worker.
	select {
	case <-h.closed:
	case h.events <- event:
	}
}

func (h *serverEventHandler) Close() {
	h.closeOnce.Do(func() {
		close(h.closed)
	})
}

func (h *serverEventHandler) markReady() {
	h.readyOnce.Do(func() {
		close(h.ready)
	})
}
