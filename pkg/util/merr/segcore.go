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

// The segcore code list (generatedSegcoreCppCodes / segcore_codes_gen.go) is
// generated from milvus-common's enum ErrorCode. Regenerate with
// `make generate-segcore-codes` (which resolves the pinned header), or directly:
//go:generate sh -c "go run ./internal/segcoregen/main.go -header \"$MILVUS_COMMON_HEADER\" -out segcore_codes_gen.go"

import "github.com/cockroachdb/errors"

// onUnmappedSegcoreCode, if set, is invoked once per occurrence whenever a C++
// segcore code arrives that is not classified by classForCode (classification
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

// classForCode maps every SegcoreCode constant to its Go classification. It is
// the single source of the retry/ownership policy; segcore_codes_gen.go is the
// single source of the code list. The two are tied together by //exhaustive:enforce:
// the `exhaustive` linter fails the build when a generated SegcoreCode constant
// has no case here, so a code the C++ side adds cannot ship unclassified -- the
// near-compile-time analog of the C++ -Werror=switch, across the C++->Go seam.
//
// It is written as a switch with NO default plus a post-switch fallback: the
// absent default is what lets `exhaustive` demand every constant, while the
// post-switch return keeps an unknown raw int32 (a code not yet regenerated, or
// garbage) safe at runtime -- classifySegcoreError degrades it to a non-retriable
// ErrSegcore and reports it through the unmapped-code observer.
//
// inputError and retriable are mutually exclusive: a code is either the caller's
// fault (retrying the same request is pointless) or a server-side condition that
// is transient (retriable) or permanent (neither flag).
func classForCode(c SegcoreCode) (segcoreClass, bool) {
	//exhaustive:enforce
	switch c {
	// Named sentinels: identity preserved so existing errors.Is guards keep
	// working (datanode/index/scheduler.go matches Unsupported / ClusterSkip).
	case CodeUnsupported:
		return segcoreClass{sentinel: ErrSegcoreUnsupported}, true
	case CodeClusterSkip:
		return segcoreClass{sentinel: ErrSegcorePretendFinished, signal: true}, true
	case CodeFollyOtherException:
		return segcoreClass{sentinel: ErrSegcoreFollyOtherException, retriable: true}, true
	case CodeFollyCancel:
		return segcoreClass{sentinel: ErrSegcoreFollyCancel}, true
	case CodeOutOfRange:
		return segcoreClass{sentinel: ErrSegcoreOutOfRange}, true
	case CodeGcpNativeError:
		return segcoreClass{sentinel: ErrSegcoreGCPNativeError, retriable: true}, true
	case CodeKnowhereError:
		return segcoreClass{sentinel: KnowhereError}, true

	// Caller-input errors -> InputError (non-retriable by construction).
	case CodeDataTypeInvalid, CodeFieldIDInvalid, CodeFieldAlreadyExist, CodeOpTypeInvalid,
		CodeDataIsEmpty, CodeJsonKeyInvalid, CodeMetricTypeInvalid, CodeExprInvalid,
		CodeMetricTypeNotMatch, CodeDimNotMatch, CodeInvalidParameter:
		return segcoreClass{sentinel: ErrSegcore, inputError: true}, true

	// Transient system errors -> retriable (a retry / reroute to another replica
	// can succeed): object storage, local IO, OOM, mmap, field-not-loaded,
	// insufficient resource, and the retriable storage fallback (2045).
	case CodeFileOpenFailed, CodeFileCreateFailed, CodeFileReadFailed, CodeFileWriteFailed,
		CodeS3Error, CodeFieldNotLoaded, CodeMemAllocateFailed, CodeMmapError,
		CodeInsufficientResource, CodeStorageTransientError:
		return segcoreClass{sentinel: ErrSegcore, retriable: true}, true

	// Permanent system errors -> non-retriable ErrSegcore. UnexpectedError(2001)
	// and NotImplemented(2002) stay generic ErrSegcore on purpose: their merr-codes
	// only coincide with ErrSegcoreUnsupported/PretendFinished, and the index/analyze
	// scheduler must retry them rather than fail permanently. ConfigInvalid(2006) is
	// a server-side yaml/config error (not the API caller's fault). StorageError(2044)
	// is the permanent storage fallback.
	case CodeUnexpectedError, CodeNotImplemented, CodeIndexBuildError, CodeIndexAlreadyBuild,
		CodeConfigInvalid, CodePathInvalid, CodePathAlreadyExist, CodePathNotExist,
		CodeBucketInvalid, CodeObjectNotExist, CodeRetrieveError, CodeDataFormatBroken,
		CodeUnistdError, CodeMemAllocateSizeNotMatch, CodeTextIndexNotFound, CodeStorageError:
		return segcoreClass{sentinel: ErrSegcore}, true
	}
	return segcoreClass{}, false
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
	cls, ok := classForCode(SegcoreCode(code))
	if !ok {
		// Runtime degrade (never panic): an unclassified C++ code falls back to a
		// generic non-retriable system error. Notify the observer (if installed)
		// so this classification drift is observable -- a growing counter means
		// the C++ side added an ErrorCode classForCode has not been taught yet.
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
	cls, ok := classForCode(SegcoreCode(code))
	return ok && cls.signal
}
