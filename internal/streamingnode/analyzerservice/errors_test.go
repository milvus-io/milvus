package analyzerservice

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestAnalyzerErrors(t *testing.T) {
	for _, tc := range []struct {
		err   error
		retry bool
	}{
		{status.NewInvalidArgument("bad tokenizer"), false},
		{status.NewSchemaVersionMismatch("new schema"), false},
		{status.NewOnShutdownError("closing"), true},
		{status.NewInner("not ready"), true},
		{grpcstatus.Error(codes.Unavailable, "connection lost"), true},
		{grpcstatus.Error(codes.Unimplemented, "older SN"), false},
		{context.Canceled, false},
		{context.DeadlineExceeded, false},
		{nil, false},
	} {
		require.Equal(t, tc.retry, Retryable(tc.err), "%v", tc.err)
	}
	require.Nil(t, PublicError(nil))
	require.ErrorIs(t, PublicError(context.Canceled), context.Canceled)
	require.ErrorIs(t, PublicError(grpcstatus.Error(codes.Unimplemented, "older SN")), merr.ErrServiceUnimplemented)
	require.ErrorIs(t, PublicError(status.NewInvalidArgument("bad envelope")), merr.ErrParameterInvalid)
	require.ErrorIs(t, PublicError(status.NewOnShutdownError("closing")), merr.ErrServiceUnavailable)
}
