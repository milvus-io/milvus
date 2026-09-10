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

// Package pkerr holds the error categories shared by the pkindex tree and the
// translation into merr at its boundaries.
//
// Code under internal/pkindex does not return merr. A caller inside the tree
// branches on what happened, and merr.Is matches on the numeric code alone, so
// distinct situations that share a code (an engine that is closed, a
// generation that is not draining) become indistinguishable once they are
// merr. Instead:
//
//   - A signal a caller branches on is a sentinel declared by the package that
//     returns it, for example engine.ErrClosed.
//   - How a failure should be reported to the outside is a category from this
//     package, attached with errors.Mark so that the original error chain and
//     its context survive.
//
// Both are translated once, at the boundary that leaves the tree: ToMerr for a
// coordinator's gRPC service or a worker's task result, and the streamingnode
// interceptor's own mapping to streaming status errors. This mirrors how the
// streaming package keeps walimpls.ErrFenced internal and translates at its
// interceptor boundary. An error that escapes untranslated crosses gRPC as
// code 65535.
package pkerr

import (
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// The error categories. They are attached with errors.Mark and tested with
// errors.Is; they are never returned bare.
//
// A mark is only visible to github.com/cockroachdb/errors.Is, which is what
// "errors" resolves to across this repository. The standard library's
// errors.Is does not see it and silently reports false, so a test helper that
// takes the standard library path (testify's assert.ErrorIs) cannot be used on
// these categories.
var (
	// ErrIO marks a failure to read or write local disk.
	ErrIO = errors.New("pkindex io failed")

	// ErrCorrupted marks stored bytes that do not match the expected layout.
	ErrCorrupted = errors.New("pkindex data corrupted")

	// ErrUnavailable marks a condition that a retry may clear, such as an
	// engine that has been closed.
	ErrUnavailable = errors.New("pkindex unavailable")
)

// MarkIO adds context to a failure of a plain file system call and marks it as
// ErrIO. It is the one form of errors.Mark shared widely enough to be worth a
// name; the other categories are attached at their single call sites.
func MarkIO(err error, format string, args ...any) error {
	return errors.Mark(errors.Wrapf(err, format, args...), ErrIO)
}

// ToMerr translates a pkindex error into a merr for a boundary that leaves the
// pkindex tree: a coordinator's gRPC service or a worker's task result. An
// error that is already a merr passes through, so a boundary may call it on a
// mixed error without checking first.
func ToMerr(err error) error {
	switch {
	case err == nil:
		return nil
	case merr.IsMilvusError(err):
		return err
	case errors.Is(err, ErrCorrupted):
		return merr.WrapErrDataIntegrity(err, "pkindex data corrupted")
	case errors.Is(err, ErrIO):
		// the only IO helper that carries an inner error's message; merr has no
		// wrapInner variant for ErrIoFailed
		return merr.WrapErrIoFailed("pkindex", err)
	case errors.Is(err, ErrUnavailable):
		return merr.WrapErrServiceUnavailableErr(err, "pkindex unavailable")
	default:
		// an uncategorized error is a pkindex bug, not a condition to retry
		return merr.WrapErrServiceInternalErr(err, "pkindex internal error")
	}
}
