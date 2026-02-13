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
	"fmt"
	"reflect"
	"unsafe"

	"github.com/bytedance/mockey/internal/monkey/common"
	"github.com/bytedance/mockey/internal/tool"
	"golang.org/x/arch/arm64/arm64asm"
)

const (
	instLen = 4 // arm64 instruction length is 4 bytes
)

func calcFnAddrRange(name string, fn func()) (uintptr, uintptr) {
	v := reflect.ValueOf(fn)
	var start, end uintptr
	start = v.Pointer()
	maxScan := 2000
	code := common.BytesOf(start, 2000)
	pos := 0
	for pos < maxScan {
		inst, err := arm64asm.Decode(code[pos:])
		tool.Assert(err == nil, err)

		args := []interface{}{name, inst.Op}
		for i := range inst.Args {
			args = append(args, inst.Args[i])
		}

		if inst.Op == arm64asm.RET {
			end = start + uintptr(pos)
			return start, end
		}

		pos += int(unsafe.Sizeof(inst.Enc))
	}
	tool.Assert(false, "%v end not found", name)
	return 0, 0
}

func Disassemble(code []byte, required int, checkLen bool) int {
	var pos int
	var err error
	var inst arm64asm.Inst

	for pos < required {
		inst, err = arm64asm.Decode(code[pos:])
		tool.Assert(err == nil || !checkLen, err)
		tool.DebugPrintf("Disassemble: %3d\t0x%x\t%v\n", pos, common.PtrOf(code)+uintptr(pos), inst)
		tool.Assert(inst.Op != arm64asm.RET || !checkLen, "function is too short to patch")
		pos += instLen
	}
	return pos
}

func GetGenericAddr(addr uintptr, maxScan int) (jumpAddr, genericInfoAddr uintptr) {
	code := common.BytesOf(addr, maxScan)
	var (
		allJumpInsts []*genericJmpInst
		allInfoInsts []*genericInfoInst
		pos          int
	)
loop:
	for pos < maxScan {
		inst, err := arm64asm.Decode(code[pos:])
		tool.Assert(err == nil, err)
		args := []interface{}{inst.Op}
		for i := range inst.Args {
			args = append(args, inst.Args[i])
		}
		tool.DebugPrintf("GetGenericAddr: %3d\t0x%x\t%v\n", pos, addr+uintptr(pos), inst)

		switch inst.Op {
		case arm64asm.BL:
			allJumpInsts = append(allJumpInsts, newGenericJmpInst(addr, pos, inst))
		case arm64asm.ADRP:
			allInfoInsts = append(allInfoInsts, newGenericInfoInst(addr, pos, inst))
		case arm64asm.ADD:
			if len(allInfoInsts) > 0 {
				allInfoInsts[len(allInfoInsts)-1].putAddInst(pos, inst)
			}
		case arm64asm.RET:
			break loop
		}
		pos += instLen
	}

	var (
		jumpInst *genericJmpInst
		infoInst *genericInfoInst
	)
	// Find the only jumpInst and filter the extra call
	for _, cur := range allJumpInsts {
		if cur.isExtraCall {
			continue
		}
		tool.Assert(jumpInst == nil, "invalid jumpInsts: %v", allJumpInsts)
		jumpInst = cur
	}
	tool.Assert(jumpInst != nil, "invalid jumpInsts: %v", allJumpInsts)
	tool.DebugPrintf("jumpInst found: %v\n", jumpInst)

	// Find the latest infoInst before the jumpInst
	for _, cur := range allInfoInsts {
		if cur.matchJumpInst(jumpInst) {
			infoInst = cur
		}
	}
	// In some cases, genericInfoAddr needs to be calculated and cannot be directly obtained by analyzing instructions.
	if infoInst == nil {
		tool.DebugPrintf("infoInst not found!\n")
		return jumpInst.jumpAddr, 0
	}

	gi := infoInst.calcGenericInfoAddr()
	tool.DebugPrintf("infoInst found: %v, genericInfoAddr: 0x%x\n", infoInst, gi)
	return jumpInst.jumpAddr, gi
}

type posInst struct {
	pos  int
	addr uintptr
	inst arm64asm.Inst
}

func (pi *posInst) String() string {
	return fmt.Sprintf("{pos: %d, addr: 0x%x, inst: %v}", pi.pos, pi.addr, pi.inst)
}

func newGenericJmpInst(base uintptr, pos int, inst arm64asm.Inst) *genericJmpInst {
	ji := &genericJmpInst{
		posInst: &posInst{pos: pos, addr: base + uintptr(pos), inst: inst},
	}
	return ji.init()
}

type genericJmpInst struct {
	*posInst
	jumpAddr      uintptr
	isExtraCall   bool
	extraCallName string
}

func (g *genericJmpInst) String() string {
	return fmt.Sprintf("{posInst: %v, jumpAddr: 0x%x, isExtraCall: %v, extraCallName: %v}", g.posInst, g.jumpAddr, g.isExtraCall, g.extraCallName)
}

func (g *genericJmpInst) init() *genericJmpInst {
	g.jumpAddr = g.calcJumpAddr()
	g.isExtraCall, g.extraCallName = isGenericProxyCallExtra(g.jumpAddr)
	return g
}

func (g *genericJmpInst) calcJumpAddr() uintptr {
	return g.addr + uintptr(g.inst.Args[0].(arm64asm.PCRel))
}

func newGenericInfoInst(base uintptr, pos int, inst arm64asm.Inst) *genericInfoInst {
	return &genericInfoInst{
		adrp: &posInst{pos: pos, addr: base + uintptr(pos), inst: inst},
	}
}

type genericInfoInst struct {
	adrp *posInst
	add  *posInst
}

func (g *genericInfoInst) String() string {
	return fmt.Sprintf("{adrp: %v, add: %v}", g.adrp, g.add)
}

func (g *genericInfoInst) matchJumpInst(jumpInst *genericJmpInst) bool {
	return g.add != nil && g.add.pos < jumpInst.pos
}

func (g *genericInfoInst) putAddInst(pos int, inst arm64asm.Inst) {
	if g.adrp == nil || g.add != nil || g.adrp.pos+instLen != pos {
		return
	}
	base := g.adrp.addr - uintptr(g.adrp.pos)
	g.add = &posInst{pos: pos, addr: base + uintptr(pos), inst: inst}
}

// calcGenericInfoAddr calculates the genericInfo from the adrp and add instructions. Example:
// ADRP X0, .+0xb3000
// ADD X0, X0, #0xec0
func (g *genericInfoInst) calcGenericInfoAddr() uintptr {
	adrpInst := g.adrp.inst
	adrpReg := adrpInst.Args[0].(arm64asm.Reg)
	adrpRes := (g.adrp.addr &^ 0xFFF) + uintptr(adrpInst.Args[1].(arm64asm.PCRel))
	addInst := g.add.inst
	tool.Assert(addInst.Args[0].(arm64asm.RegSP) == (arm64asm.RegSP)(adrpReg), "invalid addInst: %v", addInst)
	tool.Assert(addInst.Args[1].(arm64asm.RegSP) == (arm64asm.RegSP)(adrpReg), "invalid addInst: %v", addInst)
	addImmShift0 := addInst.Args[2].(arm64asm.ImmShift)
	type immShift struct {
		imm   uint16
		shift uint8
	}
	addImmShift := (*immShift)(unsafe.Pointer(&addImmShift0))
	tool.Assert(addImmShift.shift == 0, "invalid addInst: %v", addInst)
	addRes := adrpRes + uintptr(addImmShift.imm)
	return addRes
}
