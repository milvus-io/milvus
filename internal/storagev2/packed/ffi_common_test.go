package packed

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
)

func TestLoonFFIErrorClassification(t *testing.T) {
	t.Run("transaction conflicts are transient", func(t *testing.T) {
		for _, code := range []int{loonTxnExhaustedRetry, loonTxnResolutionFailed} {
			err := loonFFIError(code, "txn conflict")
			assert.ErrorIs(t, err, ErrLoonFFI)
			assert.True(t, errors.Is(err, ErrLoonTransient), "code %d should be retryable", code)
		}
	})

	t.Run("logical errors are not transient", func(t *testing.T) {
		err := loonFFIError(loonLogicalError, "Column count mismatch")
		assert.ErrorIs(t, err, ErrLoonFFI)
		assert.False(t, errors.Is(err, ErrLoonTransient))
		assert.Contains(t, err.Error(), fmt.Sprintf("code=%d", loonLogicalError))
	})
}
