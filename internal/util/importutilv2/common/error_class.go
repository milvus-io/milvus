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
