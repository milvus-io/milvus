package viewerror

import (
	"context"
	"io"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestErrorClassification(t *testing.T) {
	tests := []struct {
		err       *ViewError
		retryable bool
	}{
		{NewViewInvalidated("view %d invalid", 1), true},
		{NewViewNotFound("missing"), true},
		{NewOnShutdownError("stopping"), true},
		{NewNotPrimaryError("moved"), true},
		{NewUnknownError("unknown"), false},
	}
	for _, test := range tests {
		require.Equal(t, test.retryable, test.err.IsRetryable())
		require.Contains(t, test.err.Error(), test.err.Cause)
		require.Same(t, (*viewpb.ViewError)(test.err), test.err.AsPBError())
		require.Same(t, test.err, TryAsViewError(errors.Wrap(test.err, "outer")))
	}
	require.Nil(t, TryAsViewError(nil))
	require.Nil(t, TryAsViewError(errors.New("plain")))
	require.Equal(t, viewpb.ViewCode_VIEW_CODE_UNKNOWN, AsViewError(errors.New("plain")).Code)
	require.Nil(t, AsViewError(nil))
	var nilError *ViewError
	require.False(t, nilError.IsRetryable())
	require.Equal(t, "query view error is nil", nilError.Error())
}

func TestGRPCStatusRoundTrip(t *testing.T) {
	original := NewViewInvalidated("version %d", 10)
	serverStatus := NewGRPCStatusFromViewError(original)
	require.Equal(t, codes.FailedPrecondition, serverStatus.Code())

	converted := ConvertViewError("ViewQueryService/SearchOnView", serverStatus.Err())
	clientStatus, ok := converted.(*ViewClientStatus)
	require.True(t, ok)
	require.Equal(t, original.Code, clientStatus.TryIntoViewError().Code)
	require.Equal(t, original.Cause, clientStatus.TryIntoViewError().Cause)
	require.Contains(t, clientStatus.Error(), "SearchOnView")
	require.Equal(t, original.Code, TryAsViewError(converted).Code)
	require.Same(t, clientStatus.Status, clientStatus.GRPCStatus())

	require.Equal(t, codes.OK, NewGRPCStatusFromViewError(nil).Code())
	require.Equal(t, codes.OK, NewGRPCStatusFromViewError(New(viewpb.ViewCode_VIEW_CODE_OK, "")).Code())
	require.Equal(t, codes.Unknown, NewGRPCStatusFromViewError(New(viewpb.ViewCode(1000), "bad code")).Code())
}

func TestConvertPreservesLocalErrors(t *testing.T) {
	require.NoError(t, ConvertViewError("method", nil))
	require.ErrorIs(t, ConvertViewError("method", context.Canceled), context.Canceled)
	require.ErrorIs(t, ConvertViewError("method", context.DeadlineExceeded), context.DeadlineExceeded)
	require.ErrorIs(t, ConvertViewError("method", io.EOF), io.EOF)

	converted := ConvertViewError("method", status.Error(codes.Unavailable, "offline"))
	clientStatus, ok := converted.(*ViewClientStatus)
	require.True(t, ok)
	require.Nil(t, clientStatus.TryIntoViewError())
	require.Contains(t, clientStatus.Error(), "offline")
	require.Equal(t, codes.Unavailable, status.Code(errors.Unwrap(clientStatus)))

	var nilStatus *ViewClientStatus
	require.Nil(t, nilStatus.TryIntoViewError())
	require.Nil(t, nilStatus.GRPCStatus())
	require.NoError(t, nilStatus.Unwrap())
	require.Equal(t, "query view RPC status is nil", nilStatus.Error())
}
