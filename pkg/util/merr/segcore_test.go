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
		// Caller-input codes -> InputError, non-retriable by construction:
		// FieldIDInvalid, DataIsEmpty, JsonKeyInvalid, MetricTypeInvalid,
		// ExprInvalid, MetricTypeNotMatch, DimNotMatch, InvalidParameter.
		for _, code := range []int32{2020, 2023, 2025, 2026, 2028, 2031, 2032, 2042} {
			err := SegcoreError(code, "bad query")
			assert.Equal(t, InputError, GetErrorType(err), "code %d", code)
			assert.ErrorIs(t, err, ErrSegcore, "code %d", code)
			// input error must be non-retriable at the boundary
			assert.False(t, Status(err).GetRetriable(), "code %d", code)
		}
	})

	t.Run("retriable_system_classification", func(t *testing.T) {
		// Transient system codes (object storage / local IO / OOM / mmap /
		// folly / field-not-loaded / insufficient-resource) -> retriable
		// system errors, never InputError.
		for _, code := range []int32{2012, 2014, 2015, 2018, 2027, 2034, 2036, 2037, 2040, 2043} {
			err := SegcoreError(code, "transient failure")
			assert.Equal(t, SystemError, GetErrorType(err), "code %d", code)
			assert.True(t, Status(err).GetRetriable(), "code %d should be retriable", code)
		}
	})

	t.Run("permanent_system_classification", func(t *testing.T) {
		// Registered permanent system codes stay non-retriable system errors:
		// IndexBuildError, BucketInvalid, ObjectNotExist.
		for _, code := range []int32{2004, 2016, 2017} {
			err := SegcoreError(code, "permanent failure")
			assert.Equal(t, SystemError, GetErrorType(err), "code %d", code)
			assert.False(t, Status(err).GetRetriable(), "code %d should not be retriable", code)
		}
	})

	t.Run("system_error_default", func(t *testing.T) {
		// A plain segcore error is a non-retriable system error.
		err := SegcoreError(2000, "x")
		assert.Equal(t, SystemError, GetErrorType(err))
		assert.ErrorIs(t, err, ErrSegcore)
		assert.False(t, Status(err).GetRetriable())
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

// TestSegcoreCodeTableCoverage cross-checks segcoreCodeTable against the C++
// ErrorCode enum (common/EasyAssert.h). It guards two things:
//   - regression: codes we deliberately classified must stay registered with
//     their intended class (a silent edit that drops one fails here);
//   - drift: a C++ enum value not present in the table falls back to a plain
//     non-retriable ErrSegcore — t.Log lists those so a maintainer adding a new
//     C++ code is reminded to classify it here instead of letting it degrade.
func TestSegcoreCodeTableCoverage(t *testing.T) {
	// C++ ErrorCode enum values (common/EasyAssert.h, 2000-2099). Keep in sync
	// with the C++ side; a new value here that isn't in segcoreCodeTable is
	// reported below.
	cppCodes := []int32{
		2000, 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2009, 2010, 2011,
		2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022,
		2023, 2024, 2025, 2026, 2027, 2028, 2030, 2031, 2032, 2033, 2034,
		2035, 2036, 2037, 2038, 2039, 2040, 2041, 2042, 2043, 2099,
	}

	// Regression guard: the codes we classified on purpose must stay registered
	// with the intended property.
	wantInput := []int32{2020, 2023, 2025, 2026, 2028, 2031, 2032, 2042}
	wantRetriable := []int32{2012, 2014, 2015, 2018, 2027, 2034, 2036, 2037, 2040, 2043}
	for _, c := range wantInput {
		cls, ok := segcoreCodeTable[c]
		assert.True(t, ok && cls.inputError, "code %d must stay registered as inputError", c)
	}
	for _, c := range wantRetriable {
		cls, ok := segcoreCodeTable[c]
		assert.True(t, ok && cls.retriable, "code %d must stay registered as retriable", c)
	}

	// Drift report: C++ codes that fall back to the generic ErrSegcore.
	var unregistered []int32
	for _, c := range cppCodes {
		if _, ok := segcoreCodeTable[c]; !ok {
			unregistered = append(unregistered, c)
		}
	}
	if len(unregistered) > 0 {
		t.Logf("segcore C++ codes not registered in segcoreCodeTable (fall back to generic ErrSegcore, non-retriable): %v", unregistered)
	}
}
