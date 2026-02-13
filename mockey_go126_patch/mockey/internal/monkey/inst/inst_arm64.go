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

package inst

import "unsafe"

func BranchTo(to uintptr) (res []byte) {
	res = append(res, x26MOV(to)...)                     // MOV x26, to // fake
	res = append(res, []byte{0x40, 0x03, 0x1f, 0xd6}...) // BR x26
	return
}

// BranchInto create a branch into command
//
// Go supports passing function arguments from go 1.17 (see https://go.dev/doc/go1.17).
// We could not use x0~x18 register. As an alternative, we use R19 register (see https://go.googlesource.com/go/+/refs/heads/master/src/cmd/compile/abi-internal.md).
func BranchInto(to uintptr) (res []byte) {
	res = append(res, x26MOV(to)...)                     // MOV x26, to // fake
	res = append(res, []byte{0x53, 0x03, 0x40, 0xf9}...) // LDR x19, [x26]
	res = append(res, []byte{0x60, 0x02, 0x1f, 0xd6}...) // BR x19
	return
}

const x26 uint32 = 0b11010

// x26MOV moves the 64bit value to x26 register, using the following four instructions:
// MOVZ x26, val[0:16]
// MOVK x26, val[16:32]
// MOVK x26, val[32:48]
// MOVK x26, val[48:64]
func x26MOV(val uintptr) (res []byte) {
	res = append(res, x26MOVZ(val)...)
	res = append(res, x26MOVK(val, 1)...)
	res = append(res, x26MOVK(val, 2)...)
	res = append(res, x26MOVK(val, 3)...)
	return res
}

// x26MOVZ see https://developer.arm.com/documentation/ddi0596/2021-12/Base-Instructions/MOVZ--Move-wide-with-zero-
func x26MOVZ(val uintptr) []byte {
	imm := uint32(val & 0xffff)
	inst := 0b1<<31 | 0b1010010100<<21 | imm<<5 | x26
	res := make([]byte, 4)
	*(*uint32)(unsafe.Pointer(&res[0])) = inst
	return res
}

// x26MOVK see https://developer.arm.com/documentation/ddi0596/2021-12/Base-Instructions/MOVK--Move-wide-with-keep-
func x26MOVK(val uintptr, shift int) []byte {
	imm := uint32((val >> (shift * 0x10)) & 0xffff)
	inst := 0b1<<31 | 0b111100101<<23 | uint32(shift)&0b11<<21 | imm<<5 | 26
	res := make([]byte, 4)
	*(*uint32)(unsafe.Pointer(&res[0])) = inst
	return res
}
