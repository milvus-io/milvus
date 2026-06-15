package transformlog

import (
	"io"
	"sync"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/walmanager"
	transformlogapi "github.com/milvus-io/milvus/internal/streamingnode/transformlog"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

type SubscribeServer struct {
	walManager walmanager.Manager
	accesser   transformlogapi.Accesser
	stream     streamingpb.StreamingNodeHandlerService_SubscribeTransformServer
	sendMu     sync.Mutex
	scannersMu sync.Mutex
	scanners   map[int64]*subscription
}

type subscription struct {
	id       int64
	vchannel string
	scanner  transformlogapi.Scanner
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
	return &SubscribeServer{
		walManager: walManager,
		accesser:   w.TransformLog(),
		stream:     stream,
		scanners:   make(map[int64]*subscription),
	}, nil
}

func (s *SubscribeServer) Execute() error {
	for {
		req, err := s.stream.Recv()
		if errors.Is(err, io.EOF) {
			s.closeAll()
			return nil
		}
		if err != nil {
			s.closeAll()
			return err
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
			s.closeAll()
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
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	return s.stream.Send(resp)
}

func (s *SubscribeServer) closeAll() {
	s.scannersMu.Lock()
	scanners := s.scanners
	s.scanners = make(map[int64]*subscription)
	s.scannersMu.Unlock()
	for _, subscription := range scanners {
		_ = subscription.scanner.Close()
	}
}

func (s *SubscribeServer) closeSubscription(subscriptionID int64) string {
	s.scannersMu.Lock()
	subscription := s.scanners[subscriptionID]
	delete(s.scanners, subscriptionID)
	s.scannersMu.Unlock()
	if subscription != nil {
		_ = subscription.scanner.Close()
		return subscription.vchannel
	}
	return ""
}

func (s *SubscribeServer) createSubscription(req *streamingpb.CreateTransformSubscriptionRequest) error {
	if req == nil {
		return status.NewInvalidArgument("create transform subscription request is nil")
	}
	scanner := s.accesser.Read(s.stream.Context(), transformlogapi.ReadOption{
		Name:               req.GetVchannel(),
		VChannel:           req.GetVchannel(),
		StartAfterTimeTick: req.GetStartAfterTimeTick(),
	})
	if err := scanner.Error(); err != nil {
		return s.sendSubscriptionError(req.GetSubscriptionId(), req.GetVchannel(), err)
	}
	s.scannersMu.Lock()
	if old := s.scanners[req.GetSubscriptionId()]; old != nil {
		_ = old.scanner.Close()
	}
	s.scanners[req.GetSubscriptionId()] = &subscription{
		id:       req.GetSubscriptionId(),
		vchannel: req.GetVchannel(),
		scanner:  scanner,
	}
	s.scannersMu.Unlock()
	if err := s.send(&streamingpb.TransformResponse{
		Response: &streamingpb.TransformResponse_Create{
			Create: &streamingpb.CreateTransformSubscriptionResponse{
				SubscriptionId:     req.GetSubscriptionId(),
				Vchannel:           req.GetVchannel(),
				StartAfterTimeTick: req.GetStartAfterTimeTick(),
			},
		},
	}); err != nil {
		_ = scanner.Close()
		return err
	}
	go s.forwardSubscription(req.GetSubscriptionId(), req.GetVchannel(), scanner)
	return nil
}

func (s *SubscribeServer) forwardSubscription(subscriptionID int64, vchannel string, scanner transformlogapi.Scanner) {
	for {
		select {
		case event, ok := <-scanner.Chan():
			if !ok {
				s.closeSubscription(subscriptionID)
				return
			}
			if event.Entry != nil {
				if err := s.send(&streamingpb.TransformResponse{
					Response: &streamingpb.TransformResponse_MessageBatch{
						MessageBatch: &streamingpb.TransformMessageBatch{
							SubscriptionId: subscriptionID,
							Vchannel:       vchannel,
							Entries:        []*streamingpb.TransformLogEntry{event.Entry},
						},
					},
				}); err != nil {
					s.closeSubscription(subscriptionID)
					return
				}
			}
			if event.CaughtUp != nil {
				if err := s.send(&streamingpb.TransformResponse{
					Response: &streamingpb.TransformResponse_CaughtUp{
						CaughtUp: &streamingpb.TransformSubscriptionCaughtUp{
							SubscriptionId:     subscriptionID,
							Vchannel:           vchannel,
							StartAfterTimeTick: event.CaughtUp.StartAfterTimeTick,
						},
					},
				}); err != nil {
					s.closeSubscription(subscriptionID)
					return
				}
			}
		case <-scanner.Done():
			if err := scanner.Error(); err != nil {
				_ = s.sendSubscriptionError(subscriptionID, vchannel, err)
			}
			s.closeSubscription(subscriptionID)
			return
		case <-s.stream.Context().Done():
			s.closeSubscription(subscriptionID)
			return
		}
	}
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
