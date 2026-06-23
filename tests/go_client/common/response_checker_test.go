package common

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestRetryOnTSafeStalled(t *testing.T) {
	t.Run("retries tsafe stalled", func(t *testing.T) {
		attempts := 0
		err := retryOnTSafeStalled(context.Background(), time.Second, time.Nanosecond, func() error {
			attempts++
			if attempts < 3 {
				return errors.New("fail to Query on QueryNode 1: channel tsafe stalled[channel=by-dev]")
			}
			return nil
		})

		require.NoError(t, err)
		require.Equal(t, 3, attempts)
	})

	t.Run("returns non tsafe error", func(t *testing.T) {
		expectedErr := errors.New("non-retriable query error")
		attempts := 0
		err := retryOnTSafeStalled(context.Background(), time.Second, time.Nanosecond, func() error {
			attempts++
			return expectedErr
		})

		require.ErrorIs(t, err, expectedErr)
		require.Equal(t, 1, attempts)
	})
}
