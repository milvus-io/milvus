package merr

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
)

func TestWriteOutcomeUnknownWireContract(t *testing.T) {
	st := Status(ErrServiceWriteOutcomeUnknown)
	require.Equal(t, int32(13), st.GetCode())
	require.Equal(t, commonpb.ErrorCode_UnexpectedError, st.GetErrorCode())
	require.False(t, st.GetRetriable())
	require.Equal(t, SystemError, GetErrorType(ErrServiceWriteOutcomeUnknown))
	require.Contains(t, st.GetReason(), "write may have completed")
	require.Contains(t, st.GetReason(), "avoid duplicate writes")
	wire, err := proto.Marshal(st)
	require.NoError(t, err)
	var decoded commonpb.Status
	require.NoError(t, proto.Unmarshal(wire, &decoded))
	roundTrip := Error(&decoded)
	require.ErrorIs(t, roundTrip, ErrServiceWriteOutcomeUnknown)
	require.False(t, IsRetryableErr(roundTrip))
	require.Equal(t, SystemError, GetErrorType(roundTrip))
}
