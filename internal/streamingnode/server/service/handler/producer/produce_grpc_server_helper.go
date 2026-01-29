package producer

import (
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/ratelimit"
)

// produceGrpcServerHelper is a wrapped producer server of log messages.
type produceGrpcServerHelper struct {
	streamingpb.StreamingNodeHandlerService_ProduceServer
}

// SendProduceMessage sends the produce result to client.
func (p *produceGrpcServerHelper) SendProduceMessage(resp *streamingpb.ProduceMessageResponse) error {
	return p.Send(&streamingpb.ProduceResponse{
		Response: &streamingpb.ProduceResponse_Produce{
			Produce: resp,
		},
	})
}

// SendProduceRateLimitMessage sends the rate limit message to client.
func (p *produceGrpcServerHelper) SendProduceRateLimitMessage(state ratelimit.RateLimitState) error {
	return p.Send(&streamingpb.ProduceResponse{
		Response: &streamingpb.ProduceResponse_RateLimit{
			RateLimit: &streamingpb.ProduceRateLimitResponse{
				State: state.State,
				Rate:  state.Rate,
			},
		},
	})
}

// SendCreated sends the create response to client.
func (p *produceGrpcServerHelper) SendCreated(resp *streamingpb.CreateProducerResponse) error {
	return p.Send(&streamingpb.ProduceResponse{
		Response: &streamingpb.ProduceResponse_Create{
			Create: resp,
		},
	})
}

// SendClosed sends the close response to client.
// no more message should be sent after sending close response.
func (p *produceGrpcServerHelper) SendClosed() error {
	// wait for all produce messages are processed.
	return p.Send(&streamingpb.ProduceResponse{
		Response: &streamingpb.ProduceResponse_Close{
			Close: &streamingpb.CloseProducerResponse{},
		},
	})
}
