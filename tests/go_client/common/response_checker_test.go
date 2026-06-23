package common

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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

// TestCheckErrTriple validates that the structured error helpers read the
// (code, is_input_error, retriable) triple correctly off real merr errors,
// across a Status round-trip — the exact path a client observes: the server
// builds a commonpb.Status, the SDK reconstructs the error via merr.Error.
func TestCheckErrTriple(t *testing.T) {
	// roundTrip mimics the SDK path: error -> wire Status -> error.
	roundTrip := func(err error) error {
		return merr.Error(merr.Status(err))
	}

	t.Run("input_error_non_retriable", func(t *testing.T) {
		// ErrParameterInvalid: baked InputError, non-retriable.
		err := roundTrip(merr.WrapErrParameterInvalidMsg("bad param"))
		CheckErrTriple(t, err, merr.ErrParameterInvalid, true, false)
		CheckErrCode(t, err, merr.ErrParameterInvalid)
	})

	t.Run("optional_message_substring", func(t *testing.T) {
		// The optional expMsg pins the specific error on top of the triple.
		err := roundTrip(merr.WrapErrParameterInvalidMsg("dim out of range"))
		CheckErrTriple(t, err, merr.ErrParameterInvalid, true, false, "dim out of range")
	})

	t.Run("system_error_non_retriable", func(t *testing.T) {
		// ErrCollectionNotFound is SystemError by default (no proxy stamping here).
		err := roundTrip(merr.WrapErrCollectionNotFound("c1"))
		CheckErrTriple(t, err, merr.ErrCollectionNotFound, false, false)
	})

	t.Run("system_error_retriable", func(t *testing.T) {
		// ErrServiceUnavailable: SystemError, retriable=true.
		err := roundTrip(merr.WrapErrServiceUnavailable("not ready"))
		CheckErrTriple(t, err, merr.ErrServiceUnavailable, false, true)
	})

	t.Run("input_error_forces_non_retriable", func(t *testing.T) {
		// An InputError stamped onto an otherwise-retriable sentinel must come
		// back non-retriable: the server forces Retriable=false for input errors.
		base := merr.WrapErrServiceUnavailable("transient")
		err := roundTrip(merr.WrapErrAsInputError(base))
		// Code stays ErrServiceUnavailable, but type flips to input and
		// retriable is forced false.
		CheckErrTriple(t, err, merr.ErrServiceUnavailable, true, false)
	})

	t.Run("success", func(t *testing.T) {
		CheckErrTriple(t, nil, nil, false, false)
		CheckErrCode(t, nil, nil)
	})
}
