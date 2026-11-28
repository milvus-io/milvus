package status

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func TestTransactionNotFoundWireCompatibility(t *testing.T) {
	wire, err := proto.Marshal(NewTransactionNotFound(42).AsPBError())
	require.NoError(t, err)
	var decoded streamingpb.StreamingError
	require.NoError(t, proto.Unmarshal(wire, &decoded))

	current := New(decoded.Code, decoded.Cause)
	require.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_TRANSACTION_NOT_FOUND, current.Code)
	require.True(t, current.IsTxnNotFound())
	require.True(t, current.IsUnrecoverable())
	require.False(t, current.IsTxnExpired())
	require.False(t, current.IsPartialUpdateRetryableCAS())

	for _, other := range []*StreamingError{
		NewTransactionExpired("expired"),
		NewUnrecoverableError(current.Cause),
		NewUnknownError(current.Cause),
		NewInvalidTransactionState("commit", 0, 1),
	} {
		require.False(t, other.IsTxnNotFound())
	}
}
