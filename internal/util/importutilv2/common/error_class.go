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

package common

import (
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ioErrSentinels is the merr IO family. A file decoder can surface one of these
// through its own error chain -- npyio propagates the reader's error verbatim and
// Arrow wraps it with %w -- and the code has to survive the wrap: ErrImportFailed
// is 2100/InputError, so folding a transient outage into it reports the outage as
// bad input data and makes retry.Do give up (pkg/util/retry/retry.go:86).
//
// Enumerated rather than matched on the 1000-1012 code range, which also holds
// ErrSerializationFailed, ErrStorage and ErrDataIntegrity.
var ioErrSentinels = []error{
	merr.ErrIoKeyNotFound, merr.ErrIoFailed, merr.ErrIoUnexpectEOF,
	merr.ErrIoTooManyRequests, merr.ErrIoPermissionDenied, merr.ErrIoBucketNotFound,
	merr.ErrIoInvalidCredentials, merr.ErrIoInvalidArgument, merr.ErrIoInvalidRange,
	merr.ErrIoEntityTooLarge,
}

// IsTypedIOErr reports whether err carries a merr IO code anywhere in its chain.
func IsTypedIOErr(err error) bool {
	for _, sentinel := range ioErrSentinels {
		if errors.Is(err, sentinel) {
			return true
		}
	}
	return false
}

// WrapDecodeErr classifies a failure raised while decoding an import file.
//
// A reader in front of the decoder retries transient object-store faults, but
// every such layer gives up after a bounded number of attempts -- and the parquet
// footer is read through ReadAt/Seek, which retryableReader does not even override.
// So a persistent outage does reach the decoder. Keep a typed IO cause and its
// System classification; report anything else -- a real decode failure, which
// carries no merr code -- as input.
//
// UPGRADE CHECK -- this classification depends on the decoder preserving the
// cause. IsTypedIOErr walks the chain with errors.Is, so a decoder that
// stringifies its cause instead of wrapping it would turn a persistent storage
// outage into an InputError and abort the caller's retry.Do. Verified against
// the pinned versions: arrow-go v17 wraps with %w throughout
// parquet/file/file_reader.go, and npyio v0.6.0 returns reader errors unmodified.
// Re-verify both when either dependency is bumped; TestWrapDecodeErr covers the
// wrapping semantics but cannot detect a dependency that stops wrapping.
func WrapDecodeErr(err error, what string) error {
	if IsTypedIOErr(err) {
		return merr.Wrap(err, what)
	}
	return merr.WrapErrImportFailedMsg("%s, err=%v", what, err)
}

// IsTerminalImportV3Err decides whether an Import V3 error should immediately
// fail the job/task instead of retrying.
//
// Denylist, matching the V2 read paths (merr.IsNonRetryableErr): only provably
// permanent errors fail fast — the IO sentinels IsNonRetryableErr lists (key
// not found, permission denied, bad argument, ...), the import-specific
// terminal codes ErrDataIntegrity, ErrImportSysFailed and ErrImportFailed, and
// ErrParameterInvalid for caller-input defects (oversized import file,
// parquet type/dimension mismatch, string over max_length, invalid UTF-8):
// the request content itself forces these branches, so retrying cannot change
// the outcome — but each retry burns a fresh segment and log range with no
// retry cap. Everything else retries until the job timeout. That notably
// includes ErrIoFailed: mapObjectStorageError assigns it to every unenumerated
// S3/Azure/GCS code (InternalError, ServiceUnavailable, RequestTimeout, ...)
// and to unrecognized network errors, so honoring its retriable=false flag as
// a terminal verdict would let one transient 5xx fail a whole job.
//
// Shared by DataCoord (checker and task pollers) and DataNode (the Import V3
// task manager's Retry/Failed classifier) so the two sides cannot drift.
// Non-milvus errors carry no code and are treated as transient — on the worker
// this is load-bearing: RemoteChunkManager.Write does not map object-store
// errors, so manifest-write failures surface raw (and typically transient).
func IsTerminalImportV3Err(err error) bool {
	if err == nil {
		return false
	}
	if !merr.IsMilvusError(err) {
		return false
	}
	if merr.IsNonRetryableErr(err) {
		return true
	}
	return errors.Is(err, merr.ErrDataIntegrity) ||
		errors.Is(err, merr.ErrImportSysFailed) ||
		errors.Is(err, merr.ErrImportFailed) ||
		errors.Is(err, merr.ErrParameterInvalid)
}
