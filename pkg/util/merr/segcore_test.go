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

func TestSegcoreErrorClassification(t *testing.T) {
	// Sentinel identity must be preserved for the codes datanode/index
	// scheduler relies on via errors.Is.
	t.Run("pretend_finished_signal", func(t *testing.T) {
		// Only C++ ClusterSkip(2033) is the pretend-finished signal scheduler.go
		// matches via errors.Is.
		err := SegcoreError(2033, "msg")
		assert.ErrorIs(t, err, ErrSegcorePretendFinished)
		assert.True(t, IsSegcoreSignal(2033))
	})

	t.Run("not_implemented_is_not_pretend_finished", func(t *testing.T) {
		// C++ NotImplemented(2002) must NOT map to the pretend-finished signal:
		// ErrSegcorePretendFinished's merr-code 2002 only coincides, but C++
		// NotImplemented is a real build failure. It must stay generic ErrSegcore
		// (system, non-signal) so getStateFromError retries it instead of
		// reporting JobStateFinished.
		err := SegcoreError(2002, "msg")
		assert.ErrorIs(t, err, ErrSegcore)
		assert.NotErrorIs(t, err, ErrSegcorePretendFinished)
		assert.False(t, IsSegcoreSignal(2002))
		assert.Equal(t, SystemError, GetErrorType(err))
	})

	t.Run("unsupported_identity", func(t *testing.T) {
		// Unsupported(2003) must remain matchable as ErrSegcoreUnsupported
		// (scheduler.go:221).
		err := SegcoreError(2003, "msg")
		assert.ErrorIs(t, err, ErrSegcoreUnsupported)
		assert.False(t, IsSegcoreSignal(2003))
	})

	t.Run("unexpected_error_is_not_unsupported", func(t *testing.T) {
		// C++ UnexpectedError(2001) is the generic catch-all the C++ core throws
		// for any unclassified exception; it must stay generic ErrSegcore (->
		// scheduler retry), NOT ErrSegcoreUnsupported (whose merr-code 2001 only
		// coincides and would make scheduler.go fail the task permanently).
		err := SegcoreError(2001, "msg")
		assert.ErrorIs(t, err, ErrSegcore)
		assert.NotErrorIs(t, err, ErrSegcoreUnsupported)
		assert.False(t, IsSegcoreSignal(2001))
		assert.Equal(t, SystemError, GetErrorType(err))
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
		assert.False(t, Status(err).GetRetriable())
		assert.False(t, IsSegcoreSignal(2055))
	})

	t.Run("unmapped_code_observer", func(t *testing.T) {
		// The drift observer fires only for codes absent from the table, with the
		// raw code, so the node side can bump a metric / log a warning.
		var got []int32
		RegisterUnmappedSegcoreCodeObserver(func(code int32) { got = append(got, code) })
		defer RegisterUnmappedSegcoreCodeObserver(nil)

		_ = SegcoreError(2056, "future code") // unregistered -> observed
		_ = SegcoreError(2042, "bad param")   // registered -> not observed
		assert.Equal(t, []int32{2056}, got)
	})

	t.Run("wire_code_projection", func(t *testing.T) {
		// Pins the client-visible contract: pass-through segcore codes collapse
		// to ErrSegcore's wire code on Status, with the original C++ code kept
		// in the Reason text. Anyone changing a sentinel's numeric code, the
		// Code() extraction, or promoting a table entry to its own sentinel
		// changes what clients receive — this test forces that to be explicit.
		st := Status(SegcoreError(2028, "expr bad"))
		assert.Equal(t, ErrSegcore.code(), st.GetCode())
		assert.Contains(t, st.GetReason(), "2028")

		// A named sentinel keeps its own distinct wire code.
		assert.Equal(t, ErrSegcoreUnsupported.code(), Status(SegcoreError(2003, "x")).GetCode())

		// An unregistered (future) code collapses safely as well.
		assert.Equal(t, ErrSegcore.code(), Status(SegcoreError(9999, "x")).GetCode())
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

// TestSegcoreCodeTableCoverage is the runtime backstop for the exhaustive
// classification switch. The SegcoreCode constants are generated from
// milvus-common's EasyAssert.h and classForCode is //exhaustive:enforce, so the
// `exhaustive` linter is the primary gate; this test still catches drift if the
// linter is skipped. It guards two things:
//   - regression: codes we deliberately classified keep their intended class (a
//     silent edit that drops one fails here);
//   - coverage: every generated SegcoreCode is classified by classForCode; a new
//     C++ code regenerated without a case is reported here (named).
func TestSegcoreCodeTableCoverage(t *testing.T) {
	// The SegcoreCode constants are generated from milvus-common's EasyAssert.h
	// (see internal/segcoregen / `make generate-segcore-codes`). classForCode is
	// marked //exhaustive:enforce, so the `exhaustive` linter is the primary gate
	// that every generated constant is classified. This test is the runtime
	// backstop (still catches drift if the linter is skipped) and pins the
	// regression classifications below.

	// Regression guard: the codes we classified on purpose keep their property.
	wantInput := []SegcoreCode{2007, 2020, 2021, 2022, 2023, 2025, 2026, 2028, 2031, 2032, 2042}
	wantRetriable := []SegcoreCode{2012, 2013, 2014, 2015, 2018, 2027, 2034, 2036, 2043, 2045}
	for _, c := range wantInput {
		cls, ok := classForCode(c)
		assert.True(t, ok && cls.inputError, "code %d must stay classified as inputError", int32(c))
	}
	for _, c := range wantRetriable {
		cls, ok := classForCode(c)
		assert.True(t, ok && cls.retriable, "code %d must stay classified as retriable", int32(c))
	}

	// Drift backstop: every generated SegcoreCode must be classified by
	// classForCode. A new C++ enum value regenerated without a case there is
	// reported here (and, before that, fails the exhaustive linter).
	var unclassified []string
	for code, name := range segcoreCodeNames {
		if _, ok := classForCode(code); !ok {
			unclassified = append(unclassified, fmt.Sprintf("%d(%s)", int32(code), name))
		}
	}
	assert.Empty(t, unclassified, "generated SegcoreCode constants not classified in classForCode "+
		"(pkg/util/merr/segcore.go): %v", unclassified)
}
