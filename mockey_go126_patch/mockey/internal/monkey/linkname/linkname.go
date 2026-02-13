//go:build !go1.27
// +build !go1.27

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

// Package linkname provides a way to link a function to a symbol without //go:linkname
// Special thanks to liuxinyu.0922 from Bytedance for his help.
package linkname

import (
	"reflect"
	"runtime"
	"unsafe"
)

func FuncPCForName(name string) uintptr {
	return nameMap[name]
}

func FuncList() []*runtime.Func {
	return funcs
}

var (
	nameMap = map[string]uintptr{}
	funcs   = make([]*runtime.Func, 0)
)

func init() {
	md := getMainModuleData()
	textStart := *(*uintptr)(unsafe.Pointer(uintptr(md) + uintptr(textOffset)))
	funcTabStart := *(**functab)(unsafe.Pointer(uintptr(md) + uintptr(funcTabOffset)))
	funcTabSize := *(*int)(unsafe.Pointer(uintptr(md) + uintptr(funcTabOffset) + unsafe.Sizeof(uintptr(0))))
	header := reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(funcTabStart)),
		Len:  funcTabSize,
		Cap:  funcTabSize,
	}
	funcTabs := *(*[]functab)(unsafe.Pointer(&header))
	for _, tab := range funcTabs {
		pc := textStart + uintptr(tab.entryoff)
		fun := runtime.FuncForPC(pc)
		nameMap[fun.Name()] = pc
		funcs = append(funcs, fun)
	}
}

const (
	funcTabOffset = 128
	textOffset    = 176
)

type functab struct {
	entryoff uint32
	funcoff  uint32
}

func getMainModuleData() unsafe.Pointer {
	var f = getMainModuleData
	entry := **(**uintptr)(unsafe.Pointer(&f))
	_, pointer := findfunc(entry)
	return pointer
}

//go:linkname findfunc runtime.findfunc
func findfunc(_ uintptr) (unsafe.Pointer, unsafe.Pointer)
