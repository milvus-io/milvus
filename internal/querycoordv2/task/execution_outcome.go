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

package task

import "errors"

type ambiguousExecutionError struct {
	cause error
}

func (err *ambiguousExecutionError) Error() string {
	return err.cause.Error()
}

func (err *ambiguousExecutionError) Unwrap() error {
	return err.cause
}

// NewAmbiguousExecutionError marks an execution error whose remote side
// effect is unknown because the RPC returned without an application response.
func NewAmbiguousExecutionError(err error) error {
	if err == nil || IsAmbiguousExecutionError(err) {
		return err
	}
	return &ambiguousExecutionError{cause: err}
}

// IsAmbiguousExecutionError reports whether an execution may have taken
// effect even though the task observed a failure.
func IsAmbiguousExecutionError(err error) bool {
	var target *ambiguousExecutionError
	return errors.As(err, &target)
}
