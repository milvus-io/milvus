//go:build !mockey_disable_ss && go1.23 && !go1.27
// +build !mockey_disable_ss,go1.23,!go1.27

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

package sysmon

import (
	"reflect"
	"unsafe"

	"github.com/bytedance/mockey/internal/monkey/fn"
	"github.com/bytedance/mockey/internal/monkey/linkname"
)

func init() {
	usleepPC := linkname.FuncPCForName("runtime.usleep")
	usleep = func(usec uint32) { usleepTrampoline(usec, usleepPC) }
	lockPC := linkname.FuncPCForName("runtime.lock")
	lock = fn.MakeFunc(reflect.TypeOf(lock), lockPC).Interface().(func(unsafe.Pointer))
	unlockPC := linkname.FuncPCForName("runtime.unlock")
	unlock = fn.MakeFunc(reflect.TypeOf(unlock), unlockPC).Interface().(func(unsafe.Pointer))
}

// usleepTrampoline a trampoline for the function `runtime.usleep`. `runtime.usleep` is marked with the special tag
// `go:cgo_unsafe_args`, whose input parameters are passed via the stack instead of registers. Therefore, directly
// injecting the program counter value of the target function into other user-defined functions will result in undefined
// behavior due to argument mismatch.
func usleepTrampoline(usec uint32, pc uintptr)
