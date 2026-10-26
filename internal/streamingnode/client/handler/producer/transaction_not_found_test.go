package producer

import (
	"io"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/mocks/proto/mock_streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func TestProducerReceivesTransactionNotFoundCode(t *testing.T) {
	response := &streamingpb.ProduceResponse{Response: &streamingpb.ProduceResponse_Produce{
		Produce: &streamingpb.ProduceMessageResponse{
			RequestId: 1,
			Response:  &streamingpb.ProduceMessageResponse_Error{Error: status.NewTransactionNotFound(42).AsPBError()},
		},
	}}
	wire, err := proto.Marshal(response)
	require.NoError(t, err)
	var decoded streamingpb.ProduceResponse
	require.NoError(t, proto.Unmarshal(wire, &decoded))
	received := false
	patch := mockey.Mock((*mock_streamingpb.MockStreamingNodeHandlerService_ProduceClient).Recv).To(func(*mock_streamingpb.MockStreamingNodeHandlerService_ProduceClient) (*streamingpb.ProduceResponse, error) {
		if received {
			return nil, io.EOF
		}
		received = true
		return &decoded, nil
	}).Build()
	defer patch.UnPatch()

	p := &producerImpl{
		grpcStreamClient: &produceGrpcClient{StreamingNodeHandlerService_ProduceClient: &mock_streamingpb.MockStreamingNodeHandlerService_ProduceClient{}},
		recvExitCh:       make(chan struct{}),
		logger:           mlog.With(),
	}
	request := &produceRequest{respCh: make(chan produceResponse, 1)}
	p.pendingRequests.Store(int64(1), request)
	require.NoError(t, p.recvLoop())
	select {
	case result := <-request.respCh:
		se := status.AsStreamingError(result.err)
		require.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_TRANSACTION_NOT_FOUND, se.Code)
		require.True(t, se.IsTxnNotFound())
		require.True(t, se.IsUnrecoverable())
		require.False(t, se.IsTxnExpired())
	default:
		t.Fatal("producer did not deliver the server error")
	}
}
