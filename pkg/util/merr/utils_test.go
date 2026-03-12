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
