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

package fn

import (
	"unsafe"

	"github.com/bytedance/mockey/internal/monkey/common"
	"github.com/bytedance/mockey/internal/tool"
	"golang.org/x/arch/x86/x86asm"
)

func copyCode(targetCode, oriCode []byte) {
	var (
		inst    x86asm.Inst // current code
		n       int         // length of total codes
		callIdx []int       // index list of call instruction
		err     error
	)
	for inst.Op != x86asm.RET {
		inst, err = x86asm.Decode(oriCode[n:], 64)
		tool.Assert(err == nil, err)
		tool.DebugPrintf("copyCode: inst: %v\n", inst)
		if inst.Op == x86asm.CALL {
			callIdx = append(callIdx, n)
			tool.DebugPrintf("copyCode: call code: 0x%x\n", oriCode[n:n+5])
		}
		n += inst.Len
	}
	// copy function codes to the target
	i := copy(targetCode, oriCode[:n])
	tool.DebugPrintf("copyCode: oriCode len(%v), copied len(%v)\n", n, i)
	// replace the relative call addresses
	callOffset := int32(common.PtrOf(oriCode) - common.PtrOf(targetCode))
	tool.DebugPrintf("copyCode: offset: %x\n", callOffset)
	for _, idx := range callIdx {
		*(*int32)(unsafe.Pointer(&targetCode[idx+1])) += callOffset
	}
}
