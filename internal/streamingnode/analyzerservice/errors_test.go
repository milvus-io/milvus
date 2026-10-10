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
	native := status.NewOnShutdownError("closing")
	require.Same(t, native, StreamingError(native))
	err := StreamingError(merr.WrapErrParameterInvalidMsg("bad names"))
	require.True(t, status.AsStreamingError(err).IsInvalidArgument())
	require.ErrorIs(t, PublicError(err), merr.ErrParameterInvalid)
	err = StreamingError(merr.ErrCollectionSchemaVersionNotReady)
	require.True(t, status.AsStreamingError(err).IsSchemaVersionMismatch())
	require.ErrorIs(t, PublicError(err), merr.ErrCollectionSchemaVersionNotReady)
	require.ErrorIs(t, PublicError(StreamingError(merr.ErrServiceUnavailable)), merr.ErrServiceUnavailable)
	require.ErrorIs(t, StreamingError(context.Canceled), context.Canceled)
}
