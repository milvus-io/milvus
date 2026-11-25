// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package grpcclient

import "github.com/cockroachdb/errors"

// requestNotDispatchedError marks a local context failure before ClientBase
// invoked any RPC callback. It preserves the original context error for callers.
// Once a callback is invoked, ClientBase cannot establish whether it sent data.
type requestNotDispatchedError struct{ cause error }

func (e *requestNotDispatchedError) Error() string { return e.cause.Error() }
func (e *requestNotDispatchedError) Unwrap() error { return e.cause }

func IsRequestNotDispatched(err error) bool {
	var local *requestNotDispatchedError
	return errors.As(err, &local)
}
