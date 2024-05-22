package producer

import (
	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
)

// produceGrpcClient is a wrapped producer server of log messages.
type produceGrpcClient struct {
	logpb.LogNodeHandlerService_ProduceClient
}

// SendProduceMessage sends the produce message to server.
func (p *produceGrpcClient) SendProduceMessage(requestID int64, msg message.MutableMessage) error {
	return p.Send(&logpb.ProduceRequest{
		Request: &logpb.ProduceRequest_Produce{
			Produce: &logpb.ProduceMessageRequest{
				RequestID: requestID,
				Message: &logpb.Message{
					Payload:    msg.Payload(),
					Properties: msg.Properties().ToRawMap(),
				},
			},
		},
	})
}

// SendClose sends the close request to server.
func (p *produceGrpcClient) SendClose() error {
	return p.Send(&logpb.ProduceRequest{
		Request: &logpb.ProduceRequest_Close{
			Close: &logpb.CloseProducerRequest{},
		},
	})
}
