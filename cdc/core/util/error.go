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

package util

import (
	"fmt"
	"strings"
)

type unrecoverableError struct {
	error
}

// Unrecoverable method wrap an error to unrecoverableError. This will make retry
// quick return.
func Unrecoverable(err error) error {
	return unrecoverableError{err}
}

// IsUnRecoverable is used to judge whether the error is wrapped by unrecoverableError.
func IsUnRecoverable(err error) bool {
	_, isUnrecoverable := err.(unrecoverableError)
	return isUnrecoverable
}

// ErrorList for print error log
type ErrorList []error

// Error method return an string representation of retry error list.
func (el ErrorList) Error() string {
	limit := 10
	var builder strings.Builder
	builder.WriteString("All attempts results:\n")
	for index, err := range el {
		// if early termination happens
		if err == nil {
			break
		}
		if index > limit {
			break
		}
		builder.WriteString(fmt.Sprintf("attempt #%d:%s\n", index+1, err.Error()))
	}
	return builder.String()
}
