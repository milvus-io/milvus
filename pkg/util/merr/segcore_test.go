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
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
)

func TestSegcoreErrorClassification(t *testing.T) {
	// Sentinel identity must be preserved for the codes datanode/index
	// scheduler relies on via errors.Is.
	t.Run("pretend_finished_signal", func(t *testing.T) {
		// ClusterSkip(2033) and NotImplemented(2002) both surface as
		// pretend-finished; scheduler.go matches errors.Is on it.
		for _, code := range []int32{2002, 2033} {
			err := SegcoreError(code, "msg")
			assert.ErrorIs(t, err, ErrSegcorePretendFinished, "code %d", code)
			assert.True(t, IsSegcoreSignal(code), "code %d should be a signal", code)
		}
	})

	t.Run("unsupported_identity", func(t *testing.T) {
		// Unsupported(2003) must remain matchable as ErrSegcoreUnsupported
		// (scheduler.go:221).
		err := SegcoreError(2003, "msg")
		assert.ErrorIs(t, err, ErrSegcoreUnsupported)
		assert.False(t, IsSegcoreSignal(2003))
	})

	t.Run("named_sentinels", func(t *testing.T) {
		assert.ErrorIs(t, SegcoreError(2038, "x"), ErrSegcoreFollyCancel)
		assert.ErrorIs(t, SegcoreError(2039, "x"), ErrSegcoreOutOfRange)
		assert.ErrorIs(t, SegcoreError(2099, "x"), KnowhereError)
	})

	t.Run("input_error_classification", func(t *testing.T) {
		// DimNotMatch(2032) and ExprInvalid(2028) are clean caller-input
		// errors -> InputError.
		for _, code := range []int32{2028, 2032} {
			err := SegcoreError(code, "bad query")
			assert.Equal(t, InputError, GetErrorType(err), "code %d", code)
			assert.ErrorIs(t, err, ErrSegcore, "code %d", code)
			// input error must be non-retriable at the boundary
			assert.False(t, Status(err).GetRetriable(), "code %d", code)
		}
	})

	t.Run("system_error_default", func(t *testing.T) {
		// A plain segcore error is a system error.
		err := SegcoreError(2000, "x")
		assert.Equal(t, SystemError, GetErrorType(err))
		assert.ErrorIs(t, err, ErrSegcore)
	})

	t.Run("unknown_code_fallback", func(t *testing.T) {
		// An unregistered code must fall back to ErrSegcore safely, not be
		// dropped or panic.
		err := SegcoreError(2055, "future code")
		assert.ErrorIs(t, err, ErrSegcore)
		assert.Equal(t, SystemError, GetErrorType(err))
		assert.False(t, IsSegcoreSignal(2055))
	})

	t.Run("empty_message", func(t *testing.T) {
		err := SegcoreError(2000, "")
		assert.ErrorIs(t, err, ErrSegcore)
	})

	t.Run("message_wrapped", func(t *testing.T) {
		err := SegcoreError(2000, "boom detail")
		assert.Contains(t, err.Error(), "boom detail")
		// still matchable after message wrap
		assert.True(t, errors.Is(err, ErrSegcore))
	})
}
