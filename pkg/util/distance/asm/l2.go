//go:build ignore

package main

import (
	. "github.com/mmcloughlin/avo/build"
	. "github.com/mmcloughlin/avo/operand"
	. "github.com/mmcloughlin/avo/reg"
)

var unroll = 4

// inspired by the avo example https://github.com/mmcloughlin/avo
// avo is a tool to generate go assembly
func main() {
	TEXT("L2", NOSPLIT, "func(x, y []float32) float32")
	Doc("squared l2 between x and y")
	x := Mem{Base: Load(Param("x").Base(), GP64())}
	y := Mem{Base: Load(Param("y").Base(), GP64())}
	n := Load(Param("x").Len(), GP64())

	acc := make([]VecVirtual, unroll)
	diff := make([]VecVirtual, unroll)
	for i := 0; i < unroll; i++ {
		acc[i] = YMM()
		diff[i] = YMM()
	}

	for i := 0; i < unroll; i++ {
		VXORPS(acc[i], acc[i], acc[i])
		VXORPS(diff[i], diff[i], diff[i])
	}

	blockitems := 8 * unroll
	blocksize := 4 * blockitems
	Label("blockloop")
	CMPQ(n, U32(blockitems))
	JL(LabelRef("tail"))

	// Load x
	xs := make([]VecVirtual, unroll)
	for i := 0; i < unroll; i++ {
		xs[i] = YMM()
	}

	for i := 0; i < unroll; i++ {
		VMOVUPS(x.Offset(32*i), xs[i])
	}

	for i := 0; i < unroll; i++ {
		VSUBPS(y.Offset(32*i), xs[i], diff[i])
	}

	for i := 0; i < unroll; i++ {
		VFMADD231PS(diff[i], diff[i], acc[i])
	}

	ADDQ(U32(blocksize), x.Base)
	ADDQ(U32(blocksize), y.Base)
	SUBQ(U32(blockitems), n)
	JMP(LabelRef("blockloop"))

	// Process any trailing entries
	Label("tail")
	tail := XMM()
	VXORPS(tail, tail, tail)

	Label("tailloop")
	CMPQ(n, U32(0))
	JE(LabelRef("reduce"))

	xt := XMM()
	VMOVSS(x, xt)

	difft := XMM()
	VSUBSS(y, xt, difft)

	VFMADD231SS(difft, difft, tail)

	ADDQ(U32(4), x.Base)
	ADDQ(U32(4), y.Base)
	DECQ(n)
	JMP(LabelRef("tailloop"))

	// Reduce the lanes to one
	Label("reduce")

	// Manual reduction
	VADDPS(acc[0], acc[1], acc[0])
	VADDPS(acc[2], acc[3], acc[2])
	VADDPS(acc[0], acc[2], acc[0])

	result := acc[0].AsX()
	top := XMM()
	VEXTRACTF128(U8(1), acc[0], top)
	VADDPS(result, top, result)
	VADDPS(result, tail, result)
	VHADDPS(result, result, result)
	VHADDPS(result, result, result)
	Store(result, ReturnIndex(0))

	RET()

	Generate()
}
