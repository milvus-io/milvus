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

import "github.com/cockroachdb/errors"

// onUnmappedSegcoreCode, if set, is invoked once per occurrence whenever a C++
// segcore code arrives that is not registered in segcoreCodeTable (classification
// drift). merr is a leaf package and cannot import pkg/metrics or pkg/mlog (both
// import merr, directly or transitively, which would create an import cycle), so
// the observability side-effect (a counter + a rate-limited WARN) is injected by
// a node-side package via RegisterUnmappedSegcoreCodeObserver. It is set once at
// init time and only read afterward, so no locking is needed.
var onUnmappedSegcoreCode func(code int32)

// RegisterUnmappedSegcoreCodeObserver installs the callback invoked for every
// unregistered segcore code seen by classifySegcoreError. Call it once at init
// from a package that may import metrics/logging.
func RegisterUnmappedSegcoreCodeObserver(fn func(code int32)) {
	onUnmappedSegcoreCode = fn
}

// segcore error codes are produced by the C++ core (milvus::ErrorCode, defined
// in milvus-common's EasyAssert.h, value range 2000-2099) and travel to Go via
// the CGO CStatus{error_code, error_msg} boundary. Historically two Go paths
// consumed them inconsistently:
//
//   - the direct path (SegcoreError) passed the raw C++ code straight through,
//     so merr.Code returned an opaque number with no sentinel identity;
//   - the wrapper path (the cgo helpers in analyzer/textmatch/index wrappers)
//     hand-wrote `if errorCode == 2003/2033` switches, an drift-prone source.
//
// classifySegcoreError is the single source of truth shared by both paths. It
// maps a C++ code to the right merr sentinel (so errors.Is works), carries the
// original code in the segcoreCode field (so the precise code is never lost),
// and applies the error-type classification. Codes not present in the table
// fall back to ErrSegcore, so an unknown / newly-added C++ code is always
// captured safely (non-retriable system error) rather than dropped — it is
// simply unclassified until registered here.

// segcoreClass describes how a single C++ ErrorCode is surfaced in Go.
type segcoreClass struct {
	// sentinel is the merr sentinel this code is mapped to. errors.Is against
	// it must keep working for existing callers.
	sentinel milvusError
	// inputError marks codes that are the caller's fault (malformed request),
	// so they are classified as InputError at the boundary.
	inputError bool
	// signal marks control-flow "errors" that the caller treats as a normal
	// outcome (e.g. pretend-finished / cluster-skip), not a failure. Callers
	// that need the signal semantics match on the sentinel directly.
	signal bool
	// retriable marks transient system failures where a retry — possibly
	// rerouted to another replica/node — can succeed: object-storage / local-IO
	// errors, OOM, and field-not-loaded. inputError codes are non-retriable by
	// construction and never set this; permanent system failures (corruption,
	// config, internal bug, missing object) leave it false.
	retriable bool
}

// segcoreCodeTable is the registry of known C++ segcore error codes. Codes
// absent here fall back to ErrSegcore (see classifySegcoreError).
//
// Codes carry two orthogonal classifications:
//   - inputError: the caller's fault (bad request) -> InputError, non-retriable
//     by construction.
//   - retriable: a transient system failure where a retry / reroute can succeed.
//
// They are mutually exclusive: a code is either the caller's fault (then a retry
// of the same request is pointless) or a server-side condition that is either
// transient (retriable) or permanent (neither flag). ConfigInvalid (2006) is a
// server-side yaml/config error (not the API caller's fault), so it is left as a
// plain system error until the C++ source splits its mixed user/server semantics.
var segcoreCodeTable = map[int32]segcoreClass{
	// Already-named segcore sentinels (identity preserved).
	2000: {sentinel: ErrSegcore},
	// C++ UnexpectedError(2001) is the generic catch-all the C++ core throws for
	// any unclassified std::exception (EasyAssert.h default). It must stay generic
	// ErrSegcore so the index/analyze scheduler retries it (master parity), NOT
	// ErrSegcoreUnsupported — whose merr-code 2001 only coincides and would make
	// scheduler.go fail the task permanently. The real C++ Unsupported is 2003.
	2001: {sentinel: ErrSegcore},
	// C++ NotImplemented(2002) is a real build/runtime failure, not a signal.
	// ErrSegcorePretendFinished's merr-code 2002 only coincides; the real
	// pretend-finished code is C++ ClusterSkip 2033. Keep generic so a failed
	// build retries instead of being reported as JobStateFinished.
	2002: {sentinel: ErrSegcore},
	2037: {sentinel: ErrSegcoreFollyOtherException, retriable: true}, // FollyOtherException (folly async failure; retry/reroute)
	2038: {sentinel: ErrSegcoreFollyCancel},                          // FollyCancel (cancellation; not a pretend-finished signal — sentinel identity preserved, scheduler retries)
	2039: {sentinel: ErrSegcoreOutOfRange},                           // OutOfRange (internal bounds bug, not a signal)
	2040: {sentinel: ErrSegcoreGCPNativeError, retriable: true},      // GcpNativeError (object storage; transient)
	2099: {sentinel: KnowhereError},                                  // KnowhereError

	// Wrapper-path special cases (preserve existing errors.Is behavior that
	// datanode/index/scheduler.go relies on):
	//   2003 Unsupported    -> ErrSegcoreUnsupported (scheduler.go:221 matches)
	//   2033 ClusterSkip     -> ErrSegcorePretendFinished signal (scheduler.go:224)
	2003: {sentinel: ErrSegcoreUnsupported},
	2033: {sentinel: ErrSegcorePretendFinished, signal: true},

	// Caller-input errors (errType=input => non-retriable by construction).
	2020: {sentinel: ErrSegcore, inputError: true}, // FieldIDInvalid: field id not in schema
	2023: {sentinel: ErrSegcore, inputError: true}, // DataIsEmpty: indexing empty/all-null source data
	2025: {sentinel: ErrSegcore, inputError: true}, // JsonKeyInvalid
	2026: {sentinel: ErrSegcore, inputError: true}, // MetricTypeInvalid
	2028: {sentinel: ErrSegcore, inputError: true}, // ExprInvalid: filter expression invalid
	2031: {sentinel: ErrSegcore, inputError: true}, // MetricTypeNotMatch
	2032: {sentinel: ErrSegcore, inputError: true}, // DimNotMatch: query vector dim != schema
	2042: {sentinel: ErrSegcore, inputError: true}, // InvalidParameter: rescorer params

	// Transient system errors (retriable: a retry / reroute to another replica
	// can succeed).
	2012: {sentinel: ErrSegcore, retriable: true}, // FileOpenFailed
	2014: {sentinel: ErrSegcore, retriable: true}, // FileReadFailed
	2015: {sentinel: ErrSegcore, retriable: true}, // FileWriteFailed
	2018: {sentinel: ErrSegcore, retriable: true}, // S3Error: object-storage transient (throttling/timeout)
	2027: {sentinel: ErrSegcore, retriable: true}, // FieldNotLoaded: another replica may have it loaded
	2034: {sentinel: ErrSegcore, retriable: true}, // MemAllocateFailed: OOM
	2036: {sentinel: ErrSegcore, retriable: true}, // MmapError
	2043: {sentinel: ErrSegcore, retriable: true}, // InsufficientResource

	// Permanent system errors registered explicitly so a future reader does not
	// mistake them for "unclassified" and flip them to retriable. They map to the
	// same non-retriable ErrSegcore as the fallback; the raw code is kept in
	// segcoreCode.
	2004: {sentinel: ErrSegcore}, // IndexBuildError: build failed (bad data / permanent)
	2016: {sentinel: ErrSegcore}, // BucketInvalid: misconfigured bucket (same on every replica)
	2017: {sentinel: ErrSegcore}, // ObjectNotExist: object missing in shared storage (reroute won't help)

	// Previously-unclassified C++ codes registered explicitly (review §2): an
	// unknown code still falls back to non-retriable ErrSegcore, but registering
	// them lets the drift-guard test fail on any genuinely new/unmapped code and
	// fixes the wrong-class fallback for the caller-input ones.
	2007: {sentinel: ErrSegcore, inputError: true}, // DataTypeInvalid: caller data type wrong
	2021: {sentinel: ErrSegcore, inputError: true}, // FieldAlreadyExist: caller adds a duplicate field
	2022: {sentinel: ErrSegcore, inputError: true}, // OpTypeInvalid: caller op type invalid
	2013: {sentinel: ErrSegcore, retriable: true},  // FileCreateFailed: transient IO (sibling of 2012/2014/2015)
	2005: {sentinel: ErrSegcore},                   // IndexAlreadyBuild: internal state (proxy already dedups)
	2006: {sentinel: ErrSegcore},                   // ConfigInvalid: mixed user/server config; default system, split later
	2009: {sentinel: ErrSegcore},                   // PathInvalid (storage; classify with storage PR)
	2010: {sentinel: ErrSegcore},                   // PathAlreadyExist (storage)
	2011: {sentinel: ErrSegcore},                   // PathNotExist (storage)
	2019: {sentinel: ErrSegcore},                   // RetrieveError: generic retrieve failure
	2024: {sentinel: ErrSegcore},                   // DataFormatBroken: data corruption (permanent)
	2030: {sentinel: ErrSegcore},                   // UnistdError: syscall failure
	2035: {sentinel: ErrSegcore},                   // MemAllocateSizeNotMatch: size logic bug (not OOM)
	2041: {sentinel: ErrSegcore},                   // TextIndexNotFound

	// milvus-storage generic fallbacks (a pair, each carrying exactly one retry
	// verdict; see milvus-common EasyAssert.h). milvus-storage's ToSegcoreErrorCode
	// maps a transient object-storage IO failure to StorageTransientError(2045)
	// and a permanent/internal storage error to StorageError(2044). They must be
	// classified consistently here: 2044 non-retriable, 2045 retriable.
	2044: {sentinel: ErrSegcore},                  // StorageError: permanent storage fallback
	2045: {sentinel: ErrSegcore, retriable: true}, // StorageTransientError: retriable storage IO/throttle/timeout
}

// classifySegcoreError converts a C++ segcore error code + message into a
// classified merr error. It is the shared entry point for both the direct
// (SegcoreError) and the wrapper cgo paths.
//
// The returned error:
//   - matches errors.Is against the mapped sentinel (ErrSegcore as fallback);
//   - carries the original C++ code in the segcoreCode field;
//   - is marked InputError when the code is an unambiguous caller-input error;
//   - is retriable only for transient system codes (object storage / IO / OOM /
//     field-not-loaded); all other codes stay non-retriable.
func classifySegcoreError(code int32, msg string) error {
	cls, ok := segcoreCodeTable[code]
	if !ok {
		// Runtime degrade (never panic): an unregistered C++ code falls back to a
		// generic non-retriable system error. Notify the observer (if installed)
		// so this classification drift is observable -- a growing counter means
		// the C++ side added an ErrorCode the table has not been taught about yet.
		cls = segcoreClass{sentinel: ErrSegcore}
		if onUnmappedSegcoreCode != nil {
			onUnmappedSegcoreCode(code)
		}
	}

	// Stamp the original C++ code into the segcoreCode field on the sentinel,
	// then optionally wrap the message. The InputError mark must be applied to
	// the milvusError *before* the errors.Wrap below, because WrapErrAsInputError
	// only recognizes a bare milvusError, not a wrapped one.
	base := cls.sentinel
	if cls.inputError {
		WithErrorType(InputError)(&base)
	}
	if cls.retriable {
		base.retriable = true
	}
	err := wrapFields(base, value("segcoreCode", code))
	if msg != "" {
		err = errors.Wrap(err, msg)
	}
	return err
}

// IsSegcoreSignal reports whether a segcore error code is a control-flow signal
// (pretend-finished / cluster-skip) that callers treat as a normal outcome
// rather than a failure.
func IsSegcoreSignal(code int32) bool {
	cls, ok := segcoreCodeTable[code]
	return ok && cls.signal
}
