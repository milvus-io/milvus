//go:build go1.26
// +build go1.26

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

import (
	"github.com/bytedance/mockey/internal/monkey/common"
	"github.com/bytedance/mockey/internal/monkey/linkname"
	"github.com/bytedance/mockey/internal/tool"
	"golang.org/x/arch/x86/x86asm"
)

// Go 1.26 no longer allows go:linkname to runtime.duffcopy/duffzero.
// Use linkname.FuncPCForName to resolve their addresses instead.
var duffcopyStart, duffcopyEnd, duffzeroStart, duffzeroEnd uintptr

func init() {
	if pc := linkname.FuncPCForName("runtime.duffcopy"); pc != 0 {
		duffcopyStart, duffcopyEnd = calcFnAddrRangePC("duffcopy", pc)
	}
	if pc := linkname.FuncPCForName("runtime.duffzero"); pc != 0 {
		duffzeroStart, duffzeroEnd = calcFnAddrRangePC("duffzero", pc)
	}
}

// calcFnAddrRangePC is like calcFnAddrRange but takes a PC directly
// instead of a func() value (since go:linkname is blocked in Go 1.26).
func calcFnAddrRangePC(name string, start uintptr) (uintptr, uintptr) {
	maxScan := 2000
	code := common.BytesOf(start, maxScan)
	pos := 0
	for pos < maxScan {
		inst, err := x86asm.Decode(code[pos:], 64)
		tool.Assert(err == nil, err)
		if inst.Op == x86asm.RET {
			return start, start + uintptr(pos)
		}
		pos += int(inst.Len)
	}
	tool.Assert(false, "%v ret not found", name)
	return 0, 0
}

// proxyCallRace exclude functions used for race check in unit test
var proxyCallRace = map[uintptr]string{}

// isGenericProxyCallExtra checks if generic function's proxy called an extra internal function
func isGenericProxyCallExtra(addr uintptr) (bool, string) {
	if duffcopyStart != 0 && addr >= duffcopyStart && addr <= duffcopyEnd {
		return true, "diffcopy"
	}
	if duffzeroStart != 0 && addr >= duffzeroStart && addr <= duffzeroEnd {
		return true, "duffzero"
	}
	if name, ok := proxyCallRace[addr]; ok {
		return true, name
	}
	return false, ""
}
