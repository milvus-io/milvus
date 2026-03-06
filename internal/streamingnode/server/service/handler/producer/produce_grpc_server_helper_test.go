package producer

import (
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/pkg/v2/mocks/proto/mock_streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/ratelimit"
)

func TestProduceGrpcServerHelper_SendProduceMessage(t *testing.T) {
	grpcServer := mock_streamingpb.NewMockStreamingNodeHandlerService_ProduceServer(t)
	helper := &produceGrpcServerHelper{
		StreamingNodeHandlerService_ProduceServer: grpcServer,
	}

	resp := &streamingpb.ProduceMessageResponse{
		RequestId: 1,
		Response: &streamingpb.ProduceMessageResponse_Result{
			Result: &streamingpb.ProduceMessageResponseResult{},
		},
	}

	// Test success
	grpcServer.EXPECT().Send(mock.MatchedBy(func(pr *streamingpb.ProduceResponse) bool {
		return pr.GetProduce() != nil && pr.GetProduce().RequestId == 1
	})).Return(nil).Once()

	err := helper.SendProduceMessage(resp)
	assert.NoError(t, err)

	// Test error
	grpcServer.EXPECT().Send(mock.Anything).Return(errors.New("send error")).Once()

	err = helper.SendProduceMessage(resp)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "send error")
}

func TestProduceGrpcServerHelper_SendProduceRateLimitMessage(t *testing.T) {
	grpcServer := mock_streamingpb.NewMockStreamingNodeHandlerService_ProduceServer(t)
	helper := &produceGrpcServerHelper{
		StreamingNodeHandlerService_ProduceServer: grpcServer,
	}

	state := ratelimit.RateLimitState{
		State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_SLOWDOWN,
		Rate:  1024,
	}

	// Test success
	grpcServer.EXPECT().Send(mock.MatchedBy(func(pr *streamingpb.ProduceResponse) bool {
		rl := pr.GetRateLimit()
		return rl != nil && rl.State == streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_SLOWDOWN && rl.Rate == 1024
	})).Return(nil).Once()

	err := helper.SendProduceRateLimitMessage(state)
	assert.NoError(t, err)

	// Test error
	grpcServer.EXPECT().Send(mock.Anything).Return(errors.New("rate limit send error")).Once()

	err = helper.SendProduceRateLimitMessage(state)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "rate limit send error")
}

func TestProduceGrpcServerHelper_SendCreated(t *testing.T) {
	grpcServer := mock_streamingpb.NewMockStreamingNodeHandlerService_ProduceServer(t)
	helper := &produceGrpcServerHelper{
		StreamingNodeHandlerService_ProduceServer: grpcServer,
	}

	resp := &streamingpb.CreateProducerResponse{
		WalName: "test-wal",
	}

	// Test success
	grpcServer.EXPECT().Send(mock.MatchedBy(func(pr *streamingpb.ProduceResponse) bool {
		return pr.GetCreate() != nil && pr.GetCreate().WalName == "test-wal"
	})).Return(nil).Once()

	err := helper.SendCreated(resp)
	assert.NoError(t, err)

	// Test error
	grpcServer.EXPECT().Send(mock.Anything).Return(errors.New("create send error")).Once()

	err = helper.SendCreated(resp)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "create send error")
}

func TestProduceGrpcServerHelper_SendClosed(t *testing.T) {
	grpcServer := mock_streamingpb.NewMockStreamingNodeHandlerService_ProduceServer(t)
	helper := &produceGrpcServerHelper{
		StreamingNodeHandlerService_ProduceServer: grpcServer,
	}

	// Test success
	grpcServer.EXPECT().Send(mock.MatchedBy(func(pr *streamingpb.ProduceResponse) bool {
		return pr.GetClose() != nil
	})).Return(nil).Once()

	err := helper.SendClosed()
	assert.NoError(t, err)

	// Test error
	grpcServer.EXPECT().Send(mock.Anything).Return(errors.New("close send error")).Once()

	err = helper.SendClosed()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "close send error")
}
