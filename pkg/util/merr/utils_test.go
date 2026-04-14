// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package merr

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
)

func TestIsNonRetryableErr(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		// Permanent errors - should NOT retry
		{"KeyNotFound", ErrIoKeyNotFound, true},
		{"PermissionDenied", ErrIoPermissionDenied, true},
		{"BucketNotFound", ErrIoBucketNotFound, true},
		{"InvalidCredentials", ErrIoInvalidCredentials, true},

		// Client validation errors - should NOT retry
		{"InvalidArgument", ErrIoInvalidArgument, true},
		{"InvalidRange", ErrIoInvalidRange, true},
		{"EntityTooLarge", ErrIoEntityTooLarge, true},

		// Transient errors - SHOULD retry (return false)
		{"UnexpectedEOF", ErrIoUnexpectEOF, false},
		{"TooManyRequests", ErrIoTooManyRequests, false},
		{"ServiceNotReady", ErrServiceNotReady, false},
		{"ServiceUnavailable", ErrServiceUnavailable, false},

		// Generic error - SHOULD retry (return false)
		{"GenericError", errors.New("network error"), false},

		// Nil error
		{"NilError", nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsNonRetryableErr(tt.err)
			assert.Equal(t, tt.expected, result,
				"IsNonRetryableErr(%v) = %v, want %v", tt.err, result, tt.expected)
		})
	}
}

func TestIsNonRetryableErr_WrappedErrors(t *testing.T) {
	// Test that wrapped errors are detected correctly
	wrappedPermDenied := fmt.Errorf("failed to read: %w", ErrIoPermissionDenied)
	assert.True(t, IsNonRetryableErr(wrappedPermDenied))

	wrappedInvalidArg := fmt.Errorf("validation failed: %w", ErrIoInvalidArgument)
	assert.True(t, IsNonRetryableErr(wrappedInvalidArg))

	wrappedRetryable := fmt.Errorf("network issue: %w", ErrIoUnexpectEOF)
	assert.False(t, IsNonRetryableErr(wrappedRetryable))
}

func TestIsMilvusError_WrappedChain(t *testing.T) {
	// Direct milvus error
	assert.True(t, IsMilvusError(ErrCollectionNotFound))

	// Wrapped via errors.Wrap — milvusError is root, both impls handle this
	assert.True(t, IsMilvusError(errors.Wrap(ErrCollectionNotFound, "context")))

	// Combined: plain error first, milvusError second.
	// errors.Cause: multiErrors has no Cause() method, returns multiErrors itself — type
	// assertion to milvusError fails. errors.As: multiErrors.Unwrap() returns errs[1] directly
	// (ErrCollectionNotFound), so errors.As finds it.
	// Known limitation: Combine(milvusErr, plain) is NOT covered — Unwrap exposes errs[1:] only,
	// so milvusError at index 0 would not be found by errors.As either.
	combined := Combine(errors.New("plain"), ErrCollectionNotFound)
	assert.True(t, IsMilvusError(combined), "milvusError at tail of Combine chain should be detected")

	// Non-milvus error
	assert.False(t, IsMilvusError(errors.New("plain error")))
	assert.False(t, IsMilvusError(nil))
}

func TestWrapErrCompactionBlocked(t *testing.T) {
	// Without extra message: only the reason is attached to ErrCompactionBlocked.
	err := WrapErrCompactionBlocked("collection 100 has pending snapshot")
	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrCompactionBlocked))
	assert.Contains(t, err.Error(), "collection 100 has pending snapshot")
	// Sanity: callers should NOT mistake this for a service-internal failure.
	assert.False(t, errors.Is(err, ErrServiceInternal))
	// Business-level rejection is marked retryable so compaction scheduler can back off.
	assert.False(t, IsNonRetryableErr(err))

	// With extra context message: errors.Wrap joins msg slices with "->".
	err2 := WrapErrCompactionBlocked("segment 42 protected", "planID=8001", "type=Merge")
	assert.Error(t, err2)
	assert.True(t, errors.Is(err2, ErrCompactionBlocked))
	assert.Contains(t, err2.Error(), "segment 42 protected")
	assert.Contains(t, err2.Error(), "planID=8001->type=Merge")
}
