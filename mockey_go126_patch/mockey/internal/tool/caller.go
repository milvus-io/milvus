/*
 * Copyright 2022 ByteDance Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tool

import (
	"fmt"
	"runtime"
	"strings"
)

type CallerInfo runtime.Frame

func (c CallerInfo) String() string {
	return fmt.Sprintf("%s:%d", c.File, c.Line)
}

// OuterCaller gets non-current package caller of a function
// For example, assume we have 3 files: a/b/foo.go, a/c/bar.go and a/c/innerBar.go,
// a/b/foo.Foo calls a/c/bar.Bar, and  a/c/bar.Bar calls a/c/innerBar.innerBar.
// Here is how innerBar looks like:
//
//	func innerBar() CallerInfo { /*do some thing*/ return Caller() }
//
// The return value of innerBar should represent the line in a/b/foo.go where a/b/foo.Foo calls a/c/bar.Bar
func OuterCaller() (info CallerInfo) {
	defer func() {
		if err := recover(); err != nil {
			DebugPrintf("OuterCaller: get stack failed, err: %v\n", err)
			info = CallerInfo(runtime.Frame{File: "Nan"})
		}
	}()

	caller, _, _, _ := runtime.Caller(1)
	oriPkg, _ := getPackageAndFunction(caller)

	pc := make([]uintptr, 10)
	n := runtime.Callers(2, pc)
	pc = pc[:n]
	frames := runtime.CallersFrames(pc)
	for frame, more := frames.Next(); more; frame, more = frames.Next() {
		curPkg, _ := getPackageAndFunction(frame.PC)
		if curPkg != oriPkg {
			return CallerInfo(frame)
		}
	}
	return CallerInfo(runtime.Frame{File: "Nan"})
}

func Caller() CallerInfo {
	caller, _, _, _ := runtime.Caller(1)
	frame, _ := runtime.CallersFrames([]uintptr{caller}).Next()
	return CallerInfo(frame)
}

func getPackageAndFunction(pc uintptr) (string, string) {
	parts := strings.Split(runtime.FuncForPC(pc).Name(), ".")
	pl := len(parts)
	packageName := ""
	funcName := parts[pl-1]

	// if mock run in an anonymous function of a global variable,
	// the stack will looks like a.b.c.glob..func1(), so the
	// second last part of the caller stack would not be guaranteed
	// always to be non-empty.
	if len(parts[pl-2]) > 0 && parts[pl-2][0] == '(' {
		funcName = parts[pl-2] + "." + funcName
		packageName = strings.Join(parts[0:pl-2], ".")
	} else {
		packageName = strings.Join(parts[0:pl-1], ".")
	}
	return packageName, funcName
}
