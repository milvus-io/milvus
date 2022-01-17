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

package trace

import (
	"fmt"
	"runtime"
)

const (
	numFuncsInStack = 10
	frameNumToSkip  = 2
)

// StackTraceMsg returns the stack information, which numFuncs means how many functions do you want to show in the stack
// information.
func StackTraceMsg(numFuncs uint) string {
	pc := make([]uintptr, numFuncs)
	n := runtime.Callers(frameNumToSkip, pc)
	frames := runtime.CallersFrames(pc[:n])

	ret := ""

	for {
		frame, more := frames.Next()
		ret += fmt.Sprintf("%s:%d %s\n", frame.File, frame.Line, frame.Function)
		if !more {
			break
		}
	}

	return ret
}

// StackTrace returns the stack trace information.
func StackTrace() string {
	return StackTraceMsg(numFuncsInStack)
}
