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

package prot

import (
	"reflect"
	"syscall"

	"github.com/bytedance/mockey/internal/monkey/common"
)

const (
	protectRWX = 0x40
)

var procVirtualProtect = syscall.NewLazyDLL("kernel32.dll").NewProc("VirtualProtect")

func MProtectRWX(addr uintptr) error {
	return mProtectPage(common.PageOf(addr), protectRWX)
}

func mProtectRX(b []byte) error {
	return MProtectRWX(common.PtrOf(b))
}

func mProtectPage(page, prot uintptr) error {
	var ori uint
	return virtualProtect(page, common.PageSize(), uint32(prot), common.PtrAt(reflect.ValueOf(&ori)))
}

func virtualProtect(lpAddress uintptr, dwSize int, flNewProtect uint32, lpflOldProtect uintptr) error {
	ret, _, _ := procVirtualProtect.Call(lpAddress, uintptr(dwSize), uintptr(flNewProtect), lpflOldProtect)
	if ret == 0 {
		return syscall.GetLastError()
	}
	return nil
}
