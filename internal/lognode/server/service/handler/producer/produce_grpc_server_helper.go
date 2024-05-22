package producer

import (
	"github.com/milvus-io/milvus/internal/proto/logpb"
)

// produceGrpcServerHelper is a wrapped producer server of log messages.
type produceGrpcServerHelper struct {
	logpb.LogNodeHandlerService_ProduceServer
}

// SendProduceMessage sends the produce result to client.
func (p *produceGrpcServerHelper) SendProduceMessage(resp *logpb.ProduceMessageResponse) error {
	return p.Send(&logpb.ProduceResponse{
		Response: &logpb.ProduceResponse_Produce{
			Produce: resp,
		},
	})
}

// SendCreated sends the create response to client.
func (p *produceGrpcServerHelper) SendCreated() error {
	return p.Send(&logpb.ProduceResponse{
		Response: &logpb.ProduceResponse_Create{
			Create: &logpb.CreateProducerResponse{},
		},
	})
}

// SendClosed sends the close response to client.
// no more message should be sent after sending close response.
func (p *produceGrpcServerHelper) SendClosed() error {
	// wait for all produce messages are processed.
	return p.Send(&logpb.ProduceResponse{
		Response: &logpb.ProduceResponse_Close{
			Close: &logpb.CloseProducerResponse{},
		},
	})
}
