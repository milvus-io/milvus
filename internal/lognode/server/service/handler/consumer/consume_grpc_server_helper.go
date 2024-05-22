package consumer

import "github.com/milvus-io/milvus/internal/proto/logpb"

// consumeGrpcServerHelper is a wrapped consumer server of log messages.
type consumeGrpcServerHelper struct {
	logpb.LogNodeHandlerService_ConsumeServer
}

// SendConsumeMessage sends the consume result to client.
func (p *consumeGrpcServerHelper) SendConsumeMessage(resp *logpb.ConsumeMessageReponse) error {
	return p.Send(&logpb.ConsumeResponse{
		Response: &logpb.ConsumeResponse_Consume{
			Consume: resp,
		},
	})
}

// SendCreated sends the create response to client.
func (p *consumeGrpcServerHelper) SendCreated(resp *logpb.CreateConsumerResponse) error {
	return p.Send(&logpb.ConsumeResponse{
		Response: &logpb.ConsumeResponse_Create{
			Create: resp,
		},
	})
}

// SendClosed sends the close response to client.
// no more message should be sent after sending close response.
func (p *consumeGrpcServerHelper) SendClosed() error {
	// wait for all consume messages are processed.
	return p.Send(&logpb.ConsumeResponse{
		Response: &logpb.ConsumeResponse_Close{
			Close: &logpb.CloseConsumerResponse{},
		},
	})
}
