// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package milvusclient

import "github.com/milvus-io/milvus/client/v3/internal/merr"

// RPCError is the structured error returned for a failed Milvus RPC.
type RPCError = merr.RPCError

// Client-side error sentinels. Errors returned from RPCs with the same Milvus
// code match these values through errors.Is.
var (
	ErrServiceNotReady          = merr.ErrServiceNotReady
	ErrCollectionNotFound       = merr.ErrCollectionNotFound
	ErrCollectionSchemaMismatch = merr.ErrCollectionSchemaMismatch
	ErrIndexNotFound            = merr.ErrIndexNotFound
)

// ErrorCode returns the Milvus numeric error code carried by err.
func ErrorCode(err error) int32 {
	return merr.Code(err)
}

// IsRetryableError reports whether Milvus marked err as retriable.
func IsRetryableError(err error) bool {
	return merr.IsRetryableErr(err)
}
