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
		assert.ErrorIs(t, SegcoreError(2046, "x"), ErrCollectionSchemaVersionNotReady)
		assert.ErrorIs(t, SegcoreError(2099, "x"), KnowhereError)
	})

	t.Run("input_error_classification", func(t *testing.T) {
		// Caller-input codes -> InputError, non-retriable by construction:
		// JsonKeyInvalid, MetricTypeInvalid, ExprInvalid, MetricTypeNotMatch,
		// DimNotMatch, InvalidParameter.
		for _, code := range []int32{2025, 2026, 2028, 2031, 2032, 2042} {
			err := SegcoreError(code, "bad query")
			assert.Equal(t, InputError, GetErrorType(err), "code %d", code)
			assert.ErrorIs(t, err, ErrSegcore, "code %d", code)
			// input error must be non-retriable at the boundary
			assert.False(t, Status(err).GetRetriable(), "code %d", code)
		}
	})

	t.Run("mixed_semantics_codes_stay_system", func(t *testing.T) {
		// DataTypeInvalid(2007) / FieldIDInvalid(2020) / FieldAlreadyExist(2021)
		// look like input validation but their producers are predominantly or
		// exclusively internal guards (see classForCode). They must NOT be
		// InputError: lb_policy aborts the cross-replica sweep on InputError,
		// so mislabeling an internal failure would stop rerouting to a healthy
		// replica. Locked here so a future re-classification is a conscious,
		// producer-audited decision.
		for _, code := range []int32{2007, 2020, 2021, 2022, 2023} {
			err := SegcoreError(code, "internal guard")
			assert.Equal(t, SystemError, GetErrorType(err), "code %d", code)
			assert.False(t, Status(err).GetRetriable(), "code %d", code)
		}
	})

	t.Run("retriable_system_classification", func(t *testing.T) {
		// Transient system codes (object storage / local IO / OOM / mmap /
		// folly / field-not-loaded / insufficient-resource) -> retriable
		// system errors, never InputError.
		for _, code := range []int32{2012, 2014, 2015, 2018, 2027, 2034, 2036, 2037, 2040, 2043, 2045} {
			err := SegcoreError(code, "transient failure")
			assert.Equal(t, SystemError, GetErrorType(err), "code %d", code)
			assert.True(t, Status(err).GetRetriable(), "code %d should be retriable", code)
		}
	})

	t.Run("permanent_system_classification", func(t *testing.T) {
		// Registered permanent system codes stay non-retriable system errors:
		// IndexBuildError, BucketInvalid, ObjectNotExist, StorageError.
		for _, code := range []int32{2004, 2016, 2017, 2044} {
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

	t.Run("named_sentinel_wire_transitions", func(t *testing.T) {
		// The renumbering is a versioned wire-contract change, pinned here:
		// ErrSegcoreUnsupported moved 2001 -> 2003 and ErrSegcorePretendFinished
		// 2002 -> 2033 (their old numbers squatted on C++ UnexpectedError /
		// NotImplemented). Clients matching the old wire values must migrate;
		// see docs/dev/error_handling_casebook.md.
		assert.Equal(t, int32(2003), ErrSegcoreUnsupported.code())
		assert.Equal(t, int32(2033), ErrSegcorePretendFinished.code())
		// The vacated numbers now mean the C++ codes themselves and must NOT
		// resurrect the old sentinel identities.
		assert.Equal(t, int32(2001), Status(SegcoreError(2001, "x")).GetCode())
		assert.NotErrorIs(t, SegcoreError(2001, "x"), ErrSegcoreUnsupported)
		assert.Equal(t, int32(2002), Status(SegcoreError(2002, "x")).GetCode())
		assert.NotErrorIs(t, SegcoreError(2002, "x"), ErrSegcorePretendFinished)
	})

	t.Run("wire_code_projection", func(t *testing.T) {
		// Pins the client-visible contract: the ORIGINAL C++ code passes
		// through to the wire (2028 stays 2028) instead of collapsing onto
		// ErrSegcore(2000), while the family sentinel stays matchable via
		// errors.Is for existing guards. Anyone changing the pass-through
		// rule or a sentinel's numeric code changes what clients receive —
		// this test forces that to be explicit.
		st := Status(SegcoreError(2028, "expr bad"))
		assert.Equal(t, int32(2028), st.GetCode())
		assert.ErrorIs(t, SegcoreError(2028, "expr bad"), ErrSegcore)

		// A code with a named sentinel also wires its C++ value; the named
		// sentinel identity is preserved for errors.Is.
		assert.Equal(t, int32(2003), Status(SegcoreError(2003, "x")).GetCode())
		assert.ErrorIs(t, SegcoreError(2003, "x"), ErrSegcoreUnsupported)

		// An unregistered but in-band (future) code passes through too, still
		// under the ErrSegcore umbrella.
		assert.Equal(t, int32(2055), Status(SegcoreError(2055, "x")).GetCode())
		assert.ErrorIs(t, SegcoreError(2055, "x"), ErrSegcore)

		// A garbage code outside the segcore band collapses to ErrSegcore's
		// wire code — never leak an arbitrary number to clients.
		assert.Equal(t, ErrSegcore.code(), Status(SegcoreError(9999, "x")).GetCode())

		// A cross-family mapping keeps its sentinel's wire code (deliberate
		// remapping, not a collapse).
		assert.Equal(t, ErrCollectionSchemaVersionNotReady.code(),
			Status(SegcoreError(2046, "x")).GetCode())
	})

	t.Run("data_format_broken_identity", func(t *testing.T) {
		err := SegcoreError(2024, "malformed vector data")
		assert.True(t, IsSegcoreDataFormatBroken(err))
		assert.False(t, IsSegcoreDataFormatBroken(SegcoreError(2001, "unexpected")))
		assert.ErrorIs(t, err, ErrSegcore)
		assert.Equal(t, ErrSegcore.code(), Status(err).GetCode())
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
	wantInput := []SegcoreCode{2025, 2026, 2028, 2031, 2032, 2042}
	wantRetriable := []SegcoreCode{2012, 2013, 2014, 2015, 2018, 2027, 2034, 2036, 2037, 2040, 2043, 2045, 2046}
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

// TestSegcoreOrigin pins the parser that turns a C++ message into the metric
// label. EasyAssertInfo appends " at <file>:<line>"; everything else must be
// reported as unknown rather than guessed at, and absolute build paths must
// collapse to one repo-relative series so the same site does not split across
// CI images.
func TestSegcoreOrigin(t *testing.T) {
	cases := []struct {
		name string
		msg  string
		want string
	}{
		{
			"absolute build path is trimmed to repo-relative",
			"assert failed at /home/runner/work/milvus/milvus/internal/core/src/index/FMIndex.h:75",
			"internal/core/src/index/FMIndex.h:75",
		},
		{
			"already relative path is kept",
			"boom at internal/core/src/exec/Driver.cpp:150",
			"internal/core/src/exec/Driver.cpp:150",
		},
		{
			// The body itself contains " at " and a colon; the scan runs from
			// the end so the real location still wins.
			"prose containing the marker does not confuse the scan",
			"failed at offset 3: bad at /src/internal/core/src/storage/Util.cpp:42",
			"internal/core/src/storage/Util.cpp:42",
		},
		{"no location at all", "plain failure with no location", ""},
		{"marker without a line number", "failed at the wrong time", ""},
		{"trailing colon is not a line number", "failed at /a/b.cpp:", ""},
		{"empty message", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, segcoreOrigin(tc.msg))
		})
	}
}

// The origin observer must fire for 2001 and only for 2001: every other code
// already names its failure, so labeling them by source location would add
// metric series with no decision attached.
func TestUnexpectedSegcoreOriginObserver(t *testing.T) {
	var got []string
	RegisterUnexpectedSegcoreOriginObserver(func(origin string) { got = append(got, origin) })
	defer RegisterUnexpectedSegcoreOriginObserver(nil)

	_ = SegcoreError(2001, "boom at internal/core/src/exec/Task.cpp:248")
	_ = SegcoreError(2001, "no location here")
	_ = SegcoreError(2024, "corrupt at internal/core/src/storage/Util.cpp:1")
	_ = SegcoreError(2034, "oom at internal/core/src/storage/Util.cpp:2")

	assert.Equal(t, []string{"internal/core/src/exec/Task.cpp:248", ""}, got)
}
