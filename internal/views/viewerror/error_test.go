package viewerror

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestRetryClassificationSurvivesGRPCRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		err   *ViewError
		code  codes.Code
		retry bool
	}{
		{NewViewInvalidated("version %d is Down", 7), codes.FailedPrecondition, true},
		{NewViewNotFound("missing"), codes.NotFound, true},
		{NewOnShutdownError("closed"), codes.Unavailable, true},
		{NewNotPrimaryError("secondary"), codes.FailedPrecondition, true},
		{NewUnknownError("failed"), codes.Unknown, false},
		{New(viewpb.ViewCode(999), "future error"), codes.Unknown, false},
	} {
		t.Run(tc.err.Code.String(), func(t *testing.T) {
			wrapped := merr.Wrap(tc.err, "phase 2")
			require.Same(t, tc.err, AsViewError(wrapped))
			wire := NewGRPCStatusFromViewError(tc.err).Err()
			require.Equal(t, tc.code, status.Code(wire))
			client := ConvertViewError("SearchOnView", wire).(*ViewClientStatus)
			decoded := AsViewError(client)
			require.True(t, proto.Equal(tc.err.AsPBError(), decoded.AsPBError()))
			require.Equal(t, tc.retry, decoded.IsRetryable())
			require.Equal(t, tc.code, client.GRPCStatus().Code())
			require.Contains(t, client.Error(), tc.err.Cause)
			require.Contains(t, decoded.Error(), tc.err.Cause)
		})
	}
}

func TestViewErrorPreservesTransportCancellation(t *testing.T) {
	for _, err := range []error{nil, context.Canceled, context.DeadlineExceeded, io.EOF} {
		require.Equal(t, err, ConvertViewError("QueryOnView", err))
	}
	require.Nil(t, AsViewError(nil))
	require.Nil(t, NewGRPCStatusFromViewError(nil).Err())
	require.Nil(t, NewGRPCStatusFromViewError(New(viewpb.ViewCode_VIEW_CODE_OK, "")).Err())
	require.Nil(t, (*ViewClientStatus)(nil).GRPCStatus())
	require.Nil(t, (*ViewClientStatus)(nil).TryIntoViewError())
	plain := status.Error(codes.Unavailable, "transport unavailable")
	client := ConvertViewError("QueryOnView", plain).(*ViewClientStatus)
	require.Nil(t, client.TryIntoViewError())
	require.Contains(t, client.Error(), "transport unavailable")
	require.False(t, AsViewError(client).IsRetryable())
	require.Equal(t, plain.Error(), AsViewError(plain).Cause)
}
