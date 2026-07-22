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

package session

import "errors"

type rpcNotSentError struct {
	cause error
}

func (err *rpcNotSentError) Error() string {
	return err.cause.Error()
}

func (err *rpcNotSentError) Unwrap() error {
	return err.cause
}

// NewRPCNotSentError marks an error that occurred before the QueryNode RPC was
// dispatched. Executors use this marker to distinguish definitive local
// failures from remote outcomes whose response may have been lost.
func NewRPCNotSentError(err error) error {
	if err == nil || IsRPCNotSentError(err) {
		return err
	}
	return &rpcNotSentError{cause: err}
}

// IsRPCNotSentError reports whether an error occurred before RPC dispatch.
func IsRPCNotSentError(err error) bool {
	var target *rpcNotSentError
	return errors.As(err, &target)
}
