// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package rocksmq

import "fmt"

// Result is the type of int and represent error result
type Result int

// constant value used in error struct
const (
	Ok Result = iota
	UnknownError
	InvalidConfiguration
)

// Error is a struct contains error msg and result
type Error struct {
	msg    string
	result Result
}

// Result returns the error result
func (e *Error) Result() Result {
	return e.result
}

func (e *Error) Error() string {
	return e.msg
}

func newError(result Result, msg string) error {
	return &Error{
		msg:    fmt.Sprintf("%s: %s", msg, getResultStr(result)),
		result: result,
	}
}

func getResultStr(r Result) string {
	switch r {
	case Ok:
		return "OK"
	case UnknownError:
		return "UnknownError"
	case InvalidConfiguration:
		return "InvalidConfiguration"
	default:
		return fmt.Sprintf("Result(%d)", r)
	}
}
