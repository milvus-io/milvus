package producer

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// produceGrpcClient is a wrapped producer server of log messages.
type produceGrpcClient struct {
	streamingpb.StreamingNodeHandlerService_ProduceClient
}

// SendProduceMessage sends the produce message to server.
func (p *produceGrpcClient) SendProduceMessage(ctx context.Context, requestID int64, msg message.MutableMessage) error {
	message.OverwriteTraceContext(ctx, msg)
	return p.Send(&streamingpb.ProduceRequest{
		Request: &streamingpb.ProduceRequest_Produce{
			Produce: &streamingpb.ProduceMessageRequest{
				RequestId: requestID,
				Message:   msg.IntoMessageProto(),
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
