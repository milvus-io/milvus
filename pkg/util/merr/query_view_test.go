package merr

import (
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestQueryViewNotReadyRemainsRetriableAcrossStatus(t *testing.T) {
	err := Wrap(ErrQueryViewNotReady, "view retired during handoff")
	require.EqualValues(t, 2202, Code(err))
	require.Equal(t, SystemError, GetErrorType(err))
	require.True(t, IsRetryableErr(err))
	require.False(t, errors.Is(err, ErrCollectionNotLoaded))
	wire := Status(err)
	require.True(t, wire.GetRetriable())
	restored := Error(wire)
	require.True(t, errors.Is(restored, ErrQueryViewNotReady))
	require.Equal(t, SystemError, GetErrorType(restored))
	require.True(t, IsRetryableErr(restored))
}
