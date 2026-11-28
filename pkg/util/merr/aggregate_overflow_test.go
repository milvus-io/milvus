package merr

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAggregateOverflowStatus(t *testing.T) {
	err := Wrapf(Wrap(ErrAggregateOverflow, "integer SUM result exceeds int64 range"), "metric %q", "sum_v")
	require.EqualValues(t, 2203, Code(err))
	require.Equal(t, SystemError, GetErrorType(err))
	require.False(t, IsRetryableErr(err))
	status := Status(err)
	require.EqualValues(t, 2203, status.GetCode())
	require.Contains(t, status.GetReason(), `metric "sum_v"`)
	require.False(t, status.GetRetriable())
	restored := Error(status)
	require.ErrorIs(t, restored, ErrAggregateOverflow)
	require.Equal(t, SystemError, GetErrorType(restored))
	require.False(t, IsRetryableErr(restored))
}
