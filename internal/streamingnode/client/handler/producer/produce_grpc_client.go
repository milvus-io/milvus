package producer

import (
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

// produceGrpcClient is a wrapped producer server of log messages.
type produceGrpcClient struct {
	streamingpb.StreamingNodeHandlerService_ProduceClient
}

// SendProduceMessage sends the produce message to server.
func (p *produceGrpcClient) SendProduceMessage(requestID int64, msg message.MutableMessage) error {
	return p.Send(&streamingpb.ProduceRequest{
		Request: &streamingpb.ProduceRequest_Produce{
			Produce: &streamingpb.ProduceMessageRequest{
				RequestId: requestID,
				Message: &messagespb.Message{
					Payload:    msg.Payload(),
					Properties: msg.Properties().ToRawMap(),
				},
			},
		},
	})
}

// SendClose sends the close request to server.
func (p *produceGrpcClient) SendClose() error {
	return p.Send(&streamingpb.ProduceRequest{
		Request: &streamingpb.ProduceRequest_Close{
			Close: &streamingpb.CloseProducerRequest{},
		},
	})
}
