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

// Package store provides object access using caller-owned storage clients.
package store

import "context"

// Object identifies a complete bucket-relative key. Local adapters use an
// empty bucket and an absolute path. Implementations must not prepend a root.
type Object struct {
	Bucket string
	Key    string
}

type CopyRequest struct {
	Source      Object
	Destination Object
}

// CopySizeError means the destination write completed but its size could not
// be obtained or validated. Callers must retain the destination for cleanup.
// Err preserves the underlying failure for error classification.
type CopySizeError struct{ Err error }

func (e *CopySizeError) Error() string {
	return "copy completed but size unavailable: " + e.Err.Error()
}
func (e *CopySizeError) Unwrap() error { return e.Err }

// ObjectReader reads complete objects. Implementations must support concurrent
// calls and preserve provider errors so callers can classify failures.
type ObjectReader interface {
	Read(context.Context, Object) ([]byte, error)
}

// ObjectStore owns access to the target and to Copy's source location. Source
// reader credentials are not forwarded to Copy. Copy reports only confirmed
// writes and their byte counts. The caller owns client lifetime and cleanup.
type ObjectStore interface {
	ObjectReader
	Write(context.Context, Object, []byte) error
	// Copy returns the nonnegative object size on success. If size lookup
	// fails after a confirmed write, Copy returns *CopySizeError. Other errors
	// do not guarantee that the destination is absent.
	Copy(context.Context, CopyRequest) (int64, error)
}
