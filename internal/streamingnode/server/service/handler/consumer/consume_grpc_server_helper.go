package consumer

import "github.com/milvus-io/milvus/pkg/proto/streamingpb"

// consumeGrpcServerHelper is a wrapped consumer server of log messages.
type consumeGrpcServerHelper struct {
	streamingpb.StreamingNodeHandlerService_ConsumeServer
}

// SendConsumeMessage sends the consume result to client.
func (p *consumeGrpcServerHelper) SendConsumeMessage(resp *streamingpb.ConsumeMessageReponse) error {
	return p.Send(&streamingpb.ConsumeResponse{
		Response: &streamingpb.ConsumeResponse_Consume{
			Consume: resp,
		},
	})
}

// SendCreated sends the create response to client.
func (p *consumeGrpcServerHelper) SendCreated(resp *streamingpb.CreateConsumerResponse) error {
	return p.Send(&streamingpb.ConsumeResponse{
		Response: &streamingpb.ConsumeResponse_Create{
			Create: resp,
		},
	})
}

// SendCreated sends the create response to client.
func (p *consumeGrpcServerHelper) SendCreateVChannelConsumer(resp *streamingpb.CreateVChannelConsumerResponse) error {
	return p.Send(&streamingpb.ConsumeResponse{
		Response: &streamingpb.ConsumeResponse_CreateVchannel{
			CreateVchannel: resp,
		},
	})
}

// SendClosed sends the close response to client.
// no more message should be sent after sending close response.
func (p *consumeGrpcServerHelper) SendClosed() error {
	// wait for all consume messages are processed.
	return p.Send(&streamingpb.ConsumeResponse{
		Response: &streamingpb.ConsumeResponse_Close{
			Close: &streamingpb.CloseConsumerResponse{},
		},
	})
}
