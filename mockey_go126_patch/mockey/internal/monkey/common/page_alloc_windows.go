//go:build windows
// +build windows

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

package common

import (
	"fmt"

	"golang.org/x/sys/windows"
)

const (
	_MEM_COMMIT   = 0x1000
	_MEM_RESERVE  = 0x2000
	_MEM_DECOMMIT = 0x4000

	_PAGE_READWRITE = 0x0004
)

var virtualAlloc, virtualFree *windows.LazyProc

func init() {
	kernel32 := windows.NewLazySystemDLL("kernel32.dll")
	virtualAlloc = kernel32.NewProc("VirtualAlloc")
	virtualFree = kernel32.NewProc("VirtualFree")
}

func allocate(n int) ([]byte, error) {
	ptr, _, _ := virtualAlloc.Call(
		0,
		uintptr(n),
		_MEM_COMMIT|_MEM_RESERVE,
		_PAGE_READWRITE,
	)
	if ptr == 0 {
		return nil, fmt.Errorf("VirtualAlloc failed: %d", ptr)
	}
	return BytesOf(ptr, n), nil
}

func free(b []byte) error {
	res, _, err := virtualFree.Call(
		PtrOf(b),
		uintptr(len(b)),
		_MEM_DECOMMIT,
	)
	if res == 0 {
		return fmt.Errorf("VirtualFree failed: (%d)%w", res, err)
	}

	return nil
}
