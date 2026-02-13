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

	"golang.org/x/arch/arm64/arm64asm"
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
		inst, _ := arm64asm.Decode(asm[pos:])
		if inst.Op == arm64asm.RET {
			break
		}
		if inst.Op == arm64asm.BL {
			flag = true
			break
		}
		pos += int(unsafe.Sizeof(inst.Enc))
	}
	return flag
}
