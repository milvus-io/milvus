//go:build amd64

#include "textflag.h"

// func HasF16C() bool
// CPUID leaf 1, ECX bit 20 (F16C).
TEXT ·HasF16C(SB), NOSPLIT, $0-8
	MOVL $1, AX
	XORL CX, CX
	CPUID
	BTQ  $20, CX
	SBBQ AX, AX
	ANDQ $1, AX
	MOVQ AX, ret+0(FP)
	RET

// func DecodeFP16ToFloat32(dst []float32, src []byte)
TEXT ·DecodeFP16ToFloat32(SB), NOSPLIT, $0-48
	MOVQ dst_base+0(FP), AX
	MOVQ src_base+24(FP), BX
	MOVQ src_len+32(FP), CX
	XORQ DX, DX
loop16:
	CMPQ DX, CX
	JGE  done
	LEAQ 16(DX), R8
	CMPQ R8, CX
	JG   tail
	VMOVDQU   (BX)(DX*1), X0
	VCVTPH2PS X0, Y1
	VMOVUPS   Y1, (AX)(DX*2)
	ADDQ $16, DX
	JMP  loop16
tail:
	MOVWLZX (BX)(DX*1), R8
	VMOVQ   R8, X2
	VCVTPH2PS X2, X3
	MOVSS   X3, (AX)(DX*2)
	ADDQ $2, DX
	JMP  loop16
done:
	RET

// func DecodeInt8ToFloat32(dst []float32, src []byte)
TEXT ·DecodeInt8ToFloat32(SB), NOSPLIT, $0-48
	MOVQ dst_base+0(FP), AX
	MOVQ src_base+24(FP), BX
	MOVQ src_len+32(FP), CX
	XORQ DX, DX
loop8:
	CMPQ DX, CX
	JGE  done
	LEAQ 8(DX), R8
	CMPQ R8, CX
	JG   tail
	VMOVQ     (BX)(DX*1), X0
	VPMOVSXBW X0, Y1
	VPMOVSXWD X1, Y2
	VCVTDQ2PS Y2, Y3
	VMOVUPS   Y3, (AX)(DX*4)
	ADDQ $8, DX
	JMP  loop8
tail:
	MOVBQSX    (BX)(DX*1), R8
	VCVTSI2SSQ R8, X4, X5
	MOVSS      X5, (AX)(DX*4)
	ADDQ $1, DX
	JMP  loop8
done:
	RET

// func DecodeBFloat16ToFloat32(dst []float32, src []byte)
TEXT ·DecodeBFloat16ToFloat32(SB), NOSPLIT, $0-48
	MOVQ dst_base+0(FP), AX
	MOVQ src_base+24(FP), BX
	MOVQ src_len+32(FP), CX
	XORQ DX, DX
loop16:
	CMPQ DX, CX
	JGE  done
	LEAQ 16(DX), R8
	CMPQ R8, CX
	JG   tail
	VMOVDQU   (BX)(DX*1), X0
	VPMOVZXWD X0, Y1
	VPSLLD    $16, Y1, Y2
	VMOVUPS   Y2, (AX)(DX*2)
	ADDQ $16, DX
	JMP  loop16
tail:
	MOVWLZX (BX)(DX*1), R8
	SHLQ    $16, R8
	VMOVQ   R8, X0
	MOVSS   X0, (AX)(DX*2)
	ADDQ $2, DX
	JMP  loop16
done:
	RET
