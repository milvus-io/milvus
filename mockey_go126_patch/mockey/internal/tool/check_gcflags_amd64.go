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
	"reflect"
	"unsafe"

	"golang.org/x/arch/x86/x86asm"
)

func fn() {
}

func fn2() {
	fn()
}

func IsGCFlagsSet() bool {
	var asm []byte
	header := (*reflect.SliceHeader)(unsafe.Pointer(&asm))
	header.Data = reflect.ValueOf(fn2).Pointer()
	header.Len = 1000
	header.Cap = 1000

	flag := false
	pos := 0
	for pos < len(asm) {
		inst, _ := x86asm.Decode(asm[pos:], 64)
		if inst.Op == x86asm.RET {
			break
		}
		if inst.Op == x86asm.CALL {
			flag = true
			break
		}
		pos += int(inst.Len)
	}
	return flag
}
