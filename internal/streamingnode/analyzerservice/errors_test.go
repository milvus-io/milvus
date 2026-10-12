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
	require.Nil(t, PublicError(nil))
	require.ErrorIs(t, PublicError(context.Canceled), context.Canceled)
	require.ErrorIs(t, PublicError(grpcstatus.Error(codes.Unimplemented, "older SN")), merr.ErrServiceUnimplemented)
	require.ErrorIs(t, PublicError(status.NewInvalidArgument("bad envelope")), merr.ErrParameterInvalid)
	require.ErrorIs(t, PublicError(status.NewOnShutdownError("closing")), merr.ErrServiceUnavailable)
}
