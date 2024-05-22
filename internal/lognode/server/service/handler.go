package service

import (
	"context"

	"github.com/milvus-io/milvus/internal/lognode/server/service/handler/consumer"
	"github.com/milvus-io/milvus/internal/lognode/server/service/handler/producer"
	"github.com/milvus-io/milvus/internal/lognode/server/walmanager"
	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

var _ HandlerService = (*handlerServiceImpl)(nil)

// NewHandlerService creates a new handler service.
func NewHandlerService(walManager walmanager.Manager) HandlerService {
	return &handlerServiceImpl{
		walManager: walManager,
	}
}

type HandlerService = logpb.LogNodeHandlerServiceServer

// handlerServiceImpl implements HandlerService.
// handlerServiceImpl is just a rpc level to handle incoming grpc.
// It should not handle any wal related logic, just
// 1. recv request and transfer param into wal
// 2. wait wal handling result and transform it into grpc response (convert error into grpc error)
// 3. send response to client.
type handlerServiceImpl struct {
	walManager walmanager.Manager
}

// Produce creates a new producer for the channel on this log node.
func (hs *handlerServiceImpl) Produce(streamServer logpb.LogNodeHandlerService_ProduceServer) error {
	metrics.LogNodeProducerTotal.WithLabelValues(paramtable.GetStringNodeID()).Inc()
	defer metrics.LogNodeProducerTotal.WithLabelValues(paramtable.GetStringNodeID()).Dec()

	p, err := producer.CreateProduceServer(hs.walManager, streamServer)
	if err != nil {
		return err
	}
	return p.Execute()
}

// Consume creates a new consumer for the channel on this log node.
func (hs *handlerServiceImpl) Consume(streamServer logpb.LogNodeHandlerService_ConsumeServer) error {
	metrics.LogNodeConsumerTotal.WithLabelValues(paramtable.GetStringNodeID()).Inc()
	defer metrics.LogNodeConsumerTotal.WithLabelValues(paramtable.GetStringNodeID()).Dec()

	c, err := consumer.CreateConsumeServer(hs.walManager, streamServer)
	if err != nil {
		return err
	}
	return c.Execute()
}

// GetLatestMessageID returns the latest message id of the channel.
func (hs *handlerServiceImpl) GetLatestMessageID(ctx context.Context, req *logpb.GetLatestMessageIDRequest) (*logpb.GetLatestMessageIDResponse, error) {
	l, err := hs.walManager.GetAvailableWAL(req.ChannelName, req.Term)
	if err != nil {
		return nil, err
	}
	msgID, err := l.GetLatestMessageID(ctx)
	if err != nil {
		return nil, err
	}
	return &logpb.GetLatestMessageIDResponse{
		Id: message.NewPBMessageIDFromMessageID(msgID),
	}, nil
}
