/*****************************************************************************
Copyright (c) 2011-2014, The OpenBLAS Project
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

   1. Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.

   2. Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in
      the documentation and/or other materials provided with the
      distribution.
   3. Neither the name of the OpenBLAS project nor the names of 
      its contributors may be used to endorse or promote products 
      derived from this software without specific prior written 
      permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

 **********************************************************************************/

#define ASSEMBLER
#include "common.h"

#define old_bm %rdi
#define old_bn %rsi
#define old_bk %rdx

#define bm %r13
#define bn %r14
#define bk %r15

#define ALPHA %xmm0
#define ba %rcx
#define bb %r8
#define C %r9
#define ldc %r10

#define i %r11
#define k %rax

#define ptrba %rdi
#define ptrbb %rsi
#define C0 %rbx
#define C1 %rbp

#define prebb %r12

#ifndef WINDOWS_ABI

#define STACKSIZE 128

#define old_ldc 8+STACKSIZE(%rsp)
#define old_offset 16+STACKSIZE(%rsp)
#define MEMALPHA 48(%rsp)
#define j 56(%rsp)
#define OFFSET 64(%rsp)
#define kk 72(%rsp)
#define kkk 80(%rsp)

#else

#define STACKSIZE 512

#define OLD_A		40 + STACKSIZE(%rsp)
#define OLD_B		48 + STACKSIZE(%rsp)
#define OLD_C		56 + STACKSIZE(%rsp)
#define old_ldc		64 + STACKSIZE(%rsp)
#define old_offset	72 + STACKSIZE(%rsp)

#define MEMALPHA	  224(%rsp)
#define j	  232(%rsp)
#define OFFSET	  240(%rsp)
#define kk	  248(%rsp)
#define kkk	  256(%rsp)

#endif

#define PREFETCH0 prefetcht0
#define PREFETCH1 prefetcht0
#define PREFETCH2 prefetcht2
#define PRESIZE	80

#define xvec0	%xmm0
#define xvec1	%xmm1
#define xvec2	%xmm2
#define xvec3	%xmm3
#define xvec4	%xmm4
#define xvec5	%xmm5
#define xvec6	%xmm6
#define xvec7	%xmm7
#define xvec8	%xmm8
#define xvec9	%xmm9
#define xvec10	%xmm10
#define xvec11	%xmm11
#define xvec12	%xmm12
#define xvec13	%xmm13
#define xvec14	%xmm14
#define xvec15	%xmm15

#define yvec0	%ymm0
#define yvec1	%ymm1
#define yvec2	%ymm2
#define yvec3	%ymm3
#define yvec4	%ymm4
#define yvec5	%ymm5
#define yvec6	%ymm6
#define yvec7	%ymm7
#define yvec8	%ymm8
#define yvec9	%ymm9
#define yvec10	%ymm10
#define yvec11	%ymm11
#define yvec12	%ymm12
#define yvec13	%ymm13
#define yvec14	%ymm14
#define yvec15	%ymm15

#define LEAQ	leaq
#define ADDQ	addq
#define MULQ	imulq
#define SARQ	sarq
#define SALQ	salq
#define ANDQ	andq
#define SUBQ	subq
#define DECQ	decq
#define JG		jg
#define JLE		jle
#define TEST	testq
#define OR		orq
#define JNE		jne
#define JMP		jmp
#define NOP
#define XOR		xorpd
#undef	MOVQ
#define MOVQ	movq

#define XOR_SY	vxorps
#define XOR_SX	vxorps

#define LD_SY	vmovaps
#define LD_SX	vmovaps
#define LDL_SX	vmovlps
#define LDL_SY	vmovlps
#define LDH_SX	vmovhps
#define LDH_SY	vmovhps

#define ST_SY	vmovaps
#define ST_SX	vmovaps
#define STL_SX	vmovlps
#define STL_SY	vmovlps
#define STH_SX	vmovhps
#define STH_SY	vmovhps

#define EDUP_SY	vmovsldup
#define ODUP_SY	vmovshdup
#define EDUP_SX	vmovsldup
#define ODUP_SX	vmovshdup

#define ADD_SY	vaddps
#define ADD_SX	vaddps

#define ADD1_DY	vaddpd
#define ADDSUB_SY	vaddsubps

#define MUL_SY	vmulps
#define MUL_SX	vmulps

#define SHUF_SY	vperm2f128
#define SHUF_DY	vperm2f128
#define SHUF_SX	vpshufd

#define VPERMILP_SY	vpermilps
#define VPERMILP_SX	vpermilps

#define BROAD_SY vbroadcastss
#define BROAD_SX vbroadcastss

#define MOV_SY vmovaps
#define MOV_SX vmovaps

#define REVS_SY	vshufps
#define REVS_SX vshufps

#define EXTRA_SY vextractf128


PROLOGUE

subq    $STACKSIZE, %rsp;
movq    %rbx,  0(%rsp);
movq    %rbp,  8(%rsp);
movq    %r12, 16(%rsp);
movq    %r13, 24(%rsp);
movq    %r14, 32(%rsp);
movq    %r15, 40(%rsp);

#ifdef WINDOWS_ABI
	movq	%rdi,    48(%rsp)
	movq	%rsi,    56(%rsp)
	movups	%xmm6,   64(%rsp)
	movups	%xmm7,   80(%rsp)
	movups	%xmm8,   96(%rsp)
	movups	%xmm9,  112(%rsp)
	movups	%xmm10, 128(%rsp)
	movups	%xmm11, 144(%rsp)
	movups	%xmm12, 160(%rsp)
	movups	%xmm13, 176(%rsp)
	movups	%xmm14, 192(%rsp)
	movups	%xmm15, 208(%rsp)

	movq	ARG1,      old_bm
	movq	ARG2,      old_bn
	movq	ARG3,      old_bk
	movq	OLD_A,     ba
	movq	OLD_B,     bb
	movq	OLD_C,     C
	movq	old_ldc,   ldc
#ifdef TRMMKERNEL
	movq	old_offset, %r11
#endif
	movaps	%xmm3, %xmm0
#else

movq	old_ldc, ldc
#ifdef	TRMMKERNEL
movq	old_offset, %r11
#endif
#endif

vzeroupper

vmovlps	ALPHA, MEMALPHA
movq	old_bm,	bm
movq	old_bn, bn
movq	old_bk, bk
leaq	(, ldc, SIZE), ldc
#ifdef	TRMMKERNEL
movq	%r11, OFFSET
#ifndef	LEFT
negq	%r11;
#endif
movq	%r11, kk
#endif
MOVQ bn,j;
SARQ $3,j;
JLE .L0_loopE;
ALIGN_4;
.L0_bodyB:;
#if defined(TRMMKERNEL) && defined(LEFT)
MOVQ OFFSET, %rax;
MOVQ %rax, kk;
#endif

MOVQ C,C0;
LEAQ (C,ldc,4),C1;
MOVQ bk, k;
SALQ $5, k;
LEAQ (bb, k, 1), prebb;
MOVQ ba,ptrba;
MOVQ bm,i;
SARQ $3,i;
JLE .L1_loopE;
ALIGN_4;
.L1_bodyB:;
#if !defined(TRMMKERNEL)||(defined(TRMMKERNEL) && defined(LEFT) && defined(TRANSA))||(defined(TRMMKERNEL)&&!defined(LEFT)&&!defined(TRANSA))
MOVQ bb, ptrbb;
#else
MOVQ bb, ptrbb;
MOVQ kk, %rax;
LEAQ (, %rax, SIZE), %rax;
LEAQ (ptrba, %rax, 8), ptrba;
LEAQ (ptrbb, %rax, 8), ptrbb;
#endif
#### Initial Results Register ####
XOR_SY yvec15, yvec15, yvec15;
PREFETCH0	0*SIZE(prebb);
XOR_SY yvec14, yvec14, yvec14;
PREFETCH0	16*SIZE(prebb);
XOR_SY yvec13, yvec13, yvec13;
PREFETCH0	32*SIZE(prebb);
XOR_SY yvec12, yvec12, yvec12;
ADDQ	$48*SIZE, prebb;
EDUP_SY	0*SIZE(ptrbb), yvec2;
LEAQ	(ldc, ldc, 2), %rax;
PREFETCH2	7*SIZE(C0);
PREFETCH2	7*SIZE(C1);
XOR_SY yvec11, yvec11, yvec11;
XOR_SY yvec10, yvec10, yvec10;
PREFETCH2	7*SIZE(C0, ldc, 1);
PREFETCH2	7*SIZE(C1, ldc, 1);
LD_SY	0*SIZE(ptrba), yvec0;
XOR_SY yvec9, yvec9, yvec9;
PREFETCH2	7*SIZE(C0, ldc, 2);
PREFETCH2	7*SIZE(C1, ldc, 2);
XOR_SY yvec8, yvec8, yvec8;
VPERMILP_SY	$0x4e, yvec2, yvec3;
PREFETCH2	7*SIZE(C0, %rax, 1);
PREFETCH2	7*SIZE(C1, %rax, 1);
#ifndef	TRMMKERNEL
MOVQ bk,k;
#elif (defined(LEFT) && !defined(TRANSA))||(!defined(LEFT)&&defined(TRANSA))
MOVQ bk, %rax;
SUBQ kk, %rax;
MOVQ %rax, kkk;
#else
MOVQ kk, %rax;
#ifdef LEFT
ADDQ $8, %rax;
#else
ADDQ $8, %rax;
#endif
MOVQ %rax, kkk;
#endif
SARQ $2,k;
JLE .L2_loopE;
ALIGN_4;
.L2_bodyB:;
# Computing kernel

#### Unroll times 1 ####
PREFETCH0	PRESIZE*SIZE(ptrba);
MUL_SY	yvec0, yvec2, yvec6;
SHUF_SY	$0x03, yvec2, yvec2, yvec4;
ODUP_SY	0*SIZE(ptrbb), yvec2
MUL_SY	yvec0, yvec3, yvec7;
SHUF_SY	$0x03, yvec3, yvec3, yvec5
ADD_SY	yvec15, yvec6, yvec15
ADD_SY	yvec13, yvec7, yvec13;

LD_SY	8*SIZE(ptrba), yvec1;
VPERMILP_SY	$0x4e, yvec2, yvec3;
MUL_SY	yvec0, yvec4, yvec6;
MUL_SY	yvec0, yvec5, yvec7;
ADD_SY	yvec11, yvec6, yvec11;
ADD_SY	yvec9, yvec7, yvec9;

MUL_SY	yvec0, yvec2, yvec6;
SHUF_SY	$0x03, yvec2, yvec2, yvec4;
EDUP_SY	8*SIZE(ptrbb), yvec2;
MUL_SY	yvec0, yvec3, yvec7;
SHUF_SY	$0x03, yvec3, yvec3, yvec5;
ADD_SY	yvec14, yvec6, yvec14;
ADD_SY	yvec12, yvec7, yvec12;

VPERMILP_SY	$0x4e, yvec2, yvec3;
MUL_SY	yvec0, yvec4, yvec6;
MUL_SY	yvec0, yvec5, yvec7;
ADD_SY	yvec10, yvec6, yvec10;
ADD_SY	yvec8, yvec7, yvec8;

#### Unroll times 2 ####
MUL_SY	yvec1, yvec2, yvec6;
SHUF_SY	$0x03, yvec2, yvec2, yvec4;
ODUP_SY	8*SIZE(ptrbb), yvec2
MUL_SY	yvec1, yvec3, yvec7;
SHUF_SY	$0x03, yvec3, yvec3, yvec5
ADD_SY	yvec15, yvec6, yvec15
ADD_SY	yvec13, yvec7, yvec13;

LD_SY	16*SIZE(ptrba), yvec0;
VPERMILP_SY	$0x4e, yvec2, yvec3;
MUL_SY	yvec1, yvec4, yvec6;
MUL_SY	yvec1, yvec5, yvec7;
ADD_SY	yvec11, yvec6, yvec11;
ADD_SY	yvec9, yvec7, yvec9;

MUL_SY	yvec1, yvec2, yvec6;
SHUF_SY	$0x03, yvec2, yvec2, yvec4;
EDUP_SY	16*SIZE(ptrbb), yvec2;
MUL_SY	yvec1, yvec3, yvec7;
SHUF_SY	$0x03, yvec3, yvec3, yvec5;
ADD_SY	yvec14, yvec6, yvec14;
ADD_SY	yvec12, yvec7, yvec12;

VPERMILP_SY	$0x4e, yvec2, yvec3;
MUL_SY	yvec1, yvec4, yvec6;
MUL_SY	yvec1, yvec5, yvec7;
ADD_SY	yvec10, yvec6, yvec10;
ADD_SY	yvec8, yvec7, yvec8;

#### Unroll times 3 ####
PREFETCH0	(PRESIZE+16)*SIZE(ptrba);
MUL_SY	yvec0, yvec2, yvec6;
SHUF_SY	$0x03, yvec2, yvec2, yvec4;
ODUP_SY	16*SIZE(ptrbb), yvec2
MUL_SY	yvec0, yvec3, yvec7;
SHUF_SY	$0x03, yvec3, yvec3, yvec5
ADD_SY	yvec15, yvec6, yvec15
ADD_SY	yvec13, yvec7, yvec13;

LD_SY	24*SIZE(ptrba), yvec1;
VPERMILP_SY	$0x4e, yvec2, yvec3;
MUL_SY	yvec0, yvec4, yvec6;
MUL_SY	yvec0, yvec5, yvec7;
ADD_SY	yvec11, yvec6, yvec11;
ADD_SY	yvec9, yvec7, yvec9;
ADDQ	$32*SIZE, ptrba;

MUL_SY	yvec0, yvec2, yvec6;
SHUF_SY	$0x03, yvec2, yvec2, yvec4;
MUL_SY	yvec0, yvec3, yvec7;
SHUF_SY	$0x03, yvec3, yvec3, yvec5;
EDUP_SY	24*SIZE(ptrbb), yvec2;
ADD_SY	yvec14, yvec6, yvec14;
ADD_SY	yvec12, yvec7, yvec12;

VPERMILP_SY	$0x4e, yvec2, yvec3;
MUL_SY	yvec0, yvec4, yvec6;
MUL_SY	yvec0, yvec5, yvec7;
ADD_SY	yvec10, yvec6, yvec10;
ADD_SY	yvec8, yvec7, yvec8;

#### Unroll times 4 ####
MUL_SY	yvec1, yvec2, yvec6;
SHUF_SY	$0x03, yvec2, yvec2, yvec4;
ODUP_SY	24*SIZE(ptrbb), yvec2
MUL_SY	yvec1, yvec3, yvec7;
SHUF_SY	$0x03, yvec3, yvec3, yvec5
ADDQ	$32*SIZE, ptrbb;
ADD_SY	yvec15, yvec6, yvec15
ADD_SY	yvec13, yvec7, yvec13;

LD_SY	0*SIZE(ptrba), yvec0;
VPERMILP_SY	$0x4e, yvec2, yvec3;
MUL_SY	yvec1, yvec4, yvec6;
MUL_SY	yvec1, yvec5, yvec7;
ADD_SY	yvec11, yvec6, yvec11;
ADD_SY	yvec9, yvec7, yvec9;

MUL_SY	yvec1, yvec2, yvec6;
SHUF_SY	$0x03, yvec2, yvec2, yvec4;
EDUP_SY	0*SIZE(ptrbb), yvec2;
MUL_SY	yvec1, yvec3, yvec7;
SHUF_SY	$0x03, yvec3, yvec3, yvec5;
ADD_SY	yvec14, yvec6, yvec14;
ADD_SY	yvec12, yvec7, yvec12;

VPERMILP_SY	$0x4e, yvec2, yvec3;
MUL_SY	yvec1, yvec4, yvec6;
MUL_SY	yvec1, yvec5, yvec7;
ADD_SY	yvec10, yvec6, yvec10;
ADD_SY	yvec8, yvec7, yvec8;
.L2_bodyE:;
DECQ k;
JG .L2_bodyB;
ALIGN_4
.L2_loopE:;
#ifndef	TRMMKERNEL
TEST	$2, bk;
#else
TEST	$2, kkk;
#endif
JLE		.L3_loopE;
ALIGN_4
.L3_loobB:
#### Unroll times 1 ####
MUL_SY	yvec0, yvec2, yvec6;
SHUF_SY	$0x03, yvec2, yvec2, yvec4;
ODUP_SY	0*SIZE(ptrbb), yvec2
MUL_SY	yvec0, yvec3, yvec7;
SHUF_SY	$0x03, yvec3, yvec3, yvec5
ADD_SY	yvec15, yvec6, yvec15
ADD_SY	yvec13, yvec7, yvec13;

LD_SY	8*SIZE(ptrba), yvec1;
VPERMILP_SY	$0x4e, yvec2, yvec3;
MUL_SY	yvec0, yvec4, yvec6;
MUL_SY	yvec0, yvec5, yvec7;
ADDQ	$16*SIZE, ptrba;
ADD_SY	yvec11, yvec6, yvec11;
ADD_SY	yvec9, yvec7, yvec9;

MUL_SY	yvec0, yvec2, yvec6;
SHUF_SY	$0x03, yvec2, yvec2, yvec4;
EDUP_SY	8*SIZE(ptrbb), yvec2;
MUL_SY	yvec0, yvec3, yvec7;
SHUF_SY	$0x03, yvec3, yvec3, yvec5;
ADD_SY	yvec14, yvec6, yvec14;
ADD_SY	yvec12, yvec7, yvec12;

VPERMILP_SY	$0x4e, yvec2, yvec3;
MUL_SY	yvec0, yvec4, yvec6;
MUL_SY	yvec0, yvec5, yvec7;
ADD_SY	yvec10, yvec6, yvec10;
ADD_SY	yvec8, yvec7, yvec8;

#### Unroll times 2 ####
MUL_SY	yvec1, yvec2, yvec6;
SHUF_SY	$0x03, yvec2, yvec2, yvec4;
ODUP_SY	8*SIZE(ptrbb), yvec2
MUL_SY	yvec1, yvec3, yvec7;
SHUF_SY	$0x03, yvec3, yvec3, yvec5
ADDQ	$16*SIZE, ptrbb
ADD_SY	yvec15, yvec6, yvec15
ADD_SY	yvec13, yvec7, yvec13;

LD_SY	0*SIZE(ptrba), yvec0;
VPERMILP_SY	$0x4e, yvec2, yvec3;
MUL_SY	yvec1, yvec4, yvec6;
MUL_SY	yvec1, yvec5, yvec7;
ADD_SY	yvec11, yvec6, yvec11;
ADD_SY	yvec9, yvec7, yvec9;

MUL_SY	yvec1, yvec2, yvec6;
SHUF_SY	$0x03, yvec2, yvec2, yvec4;
EDUP_SY	0*SIZE(ptrbb), yvec2;
MUL_SY	yvec1, yvec3, yvec7;
SHUF_SY	$0x03, yvec3, yvec3, yvec5;
ADD_SY	yvec14, yvec6, yvec14;
ADD_SY	yvec12, yvec7, yvec12;

VPERMILP_SY	$0x4e, yvec2, yvec3;
MUL_SY	yvec1, yvec4, yvec6;
MUL_SY	yvec1, yvec5, yvec7;
ADD_SY	yvec10, yvec6, yvec10;
ADD_SY	yvec8, yvec7, yvec8;
.L3_loopE:
#ifndef	TRMMKERNEL
TEST	$1, bk;
#else
TEST	$1, kkk;
#endif
JLE		.L4_loopE;
ALIGN_4
.L4_loopB:;
#### Unroll times 1 ####
MUL_SY	yvec0, yvec2, yvec6;
SHUF_SY	$0x03, yvec2, yvec2, yvec4;
ODUP_SY	0*SIZE(ptrbb), yvec2
MUL_SY	yvec0, yvec3, yvec7;
SHUF_SY	$0x03, yvec3, yvec3, yvec5
ADDQ	$8*SIZE, ptrba;
ADD_SY	yvec15, yvec6, yvec15
ADD_SY	yvec13, yvec7, yvec13;

VPERMILP_SY	$0x4e, yvec2, yvec3;
MUL_SY	yvec0, yvec4, yvec6;
MUL_SY	yvec0, yvec5, yvec7;
ADDQ	$8*SIZE, ptrbb;
ADD_SY	yvec11, yvec6, yvec11;
ADD_SY	yvec9, yvec7, yvec9;

MUL_SY	yvec0, yvec2, yvec6;
SHUF_SY	$0x03, yvec2, yvec2, yvec4;
MUL_SY	yvec0, yvec3, yvec7;
SHUF_SY	$0x03, yvec3, yvec3, yvec5;
ADD_SY	yvec14, yvec6, yvec14;
ADD_SY	yvec12, yvec7, yvec12;

MUL_SY	yvec0, yvec4, yvec6;
MUL_SY	yvec0, yvec5, yvec7;
ADD_SY	yvec10, yvec6, yvec10;
ADD_SY	yvec8, yvec7, yvec8;

.L4_loopE:;
#### Load Alpha ####
BROAD_SY MEMALPHA,yvec7;
MUL_SY yvec7,yvec15,yvec15;
MUL_SY yvec7,yvec14,yvec14;
MUL_SY yvec7,yvec13,yvec13;
MUL_SY yvec7,yvec12,yvec12;
MUL_SY yvec7,yvec11,yvec11;
MUL_SY yvec7,yvec10,yvec10;
MUL_SY yvec7,yvec9,yvec9;
MUL_SY yvec7,yvec8,yvec8;
MOV_SY yvec15,yvec7;
REVS_SY $0xe4,yvec13,yvec15,yvec15;
REVS_SY $0xe4,yvec7,yvec13,yvec13;
MOV_SY yvec14,yvec7;
REVS_SY $0xe4,yvec12,yvec14,yvec14;
REVS_SY $0xe4,yvec7,yvec12,yvec12;
MOV_SY yvec11,yvec7;
REVS_SY $0xe4,yvec9,yvec11,yvec11;
REVS_SY $0xe4,yvec7,yvec9,yvec9;
MOV_SY yvec10,yvec7;
REVS_SY $0xe4,yvec8,yvec10,yvec10;
REVS_SY $0xe4,yvec7,yvec8,yvec8;
##### Testing alignment #####
MOVQ	C0, %rax;
OR		ldc, %rax;
TEST	$15, %rax;
JNE		.L4_loopEx;
ALIGN_4
LEAQ (ldc,ldc,2),%rax;
EXTRA_SY $1,yvec15,xvec7;
EXTRA_SY $1,yvec14,xvec6;
EXTRA_SY $1,yvec13,xvec5;
EXTRA_SY $1,yvec12,xvec4;
EXTRA_SY $1,yvec11,xvec3;
EXTRA_SY $1,yvec10,xvec2;
EXTRA_SY $1,yvec9,xvec1;
EXTRA_SY $1,yvec8,xvec0;
#ifndef	TRMMKERNEL
ADD_SY 0*SIZE(C0), xvec15, xvec15;
ADD_SY 4*SIZE(C1), xvec7, xvec7;
ADD_SY 0*SIZE(C0,ldc,1), xvec14, xvec14;
ADD_SY 4*SIZE(C1,ldc,1), xvec6, xvec6;
ADD_SY 0*SIZE(C0,ldc,2), xvec13, xvec13;
ADD_SY 4*SIZE(C1,ldc,2), xvec5, xvec5;
ADD_SY 0*SIZE(C0,%rax,1), xvec12, xvec12;
ADD_SY 4*SIZE(C1,%rax,1), xvec4, xvec4;
ADD_SY 0*SIZE(C1), xvec11, xvec11;
ADD_SY 4*SIZE(C0), xvec3, xvec3;
ADD_SY 0*SIZE(C1,ldc,1), xvec10, xvec10;
ADD_SY 4*SIZE(C0,ldc,1), xvec2, xvec2;
ADD_SY 0*SIZE(C1,ldc,2), xvec9, xvec9;
ADD_SY 4*SIZE(C0,ldc,2), xvec1, xvec1;
ADD_SY 0*SIZE(C1,%rax,1), xvec8, xvec8;
ADD_SY 4*SIZE(C0,%rax,1), xvec0, xvec0;
#endif
ST_SY xvec15,0*SIZE(C0);
ST_SY xvec7,4*SIZE(C1);
ST_SY xvec14,0*SIZE(C0,ldc,1);
ST_SY xvec6,4*SIZE(C1,ldc,1);
ST_SY xvec13,0*SIZE(C0,ldc,2);
ST_SY xvec5,4*SIZE(C1,ldc,2);
ST_SY xvec12,0*SIZE(C0,%rax,1);
ST_SY xvec4,4*SIZE(C1,%rax,1);
ST_SY xvec11,0*SIZE(C1);
ST_SY xvec3,4*SIZE(C0);
ST_SY xvec10,0*SIZE(C1,ldc,1);
ST_SY xvec2,4*SIZE(C0,ldc,1);
ST_SY xvec9,0*SIZE(C1,ldc,2);
ST_SY xvec1,4*SIZE(C0,ldc,2);
ST_SY xvec8,0*SIZE(C1,%rax,1);
ST_SY xvec0,4*SIZE(C0,%rax,1);
#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) ||(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
MOVQ	bk, %rax;
SUBQ	kkk, %rax;
LEAQ 	(, %rax, SIZE), %rax;
LEAQ	(ptrba, %rax, 8), ptrba;
LEAQ 	(ptrbb, %rax, 8), ptrbb;
#endif
#if defined(TRMMKERNEL) && defined(LEFT)
ADDQ	$8, kk
#endif
ADDQ $8*SIZE,C0;
ADDQ $8*SIZE,C1;
.L1_bodyE:;
DECQ i;
JG .L1_bodyB;
JMP .L1_loopE;
ALIGN_4;
.L4_loopEx:
LEAQ (ldc,ldc,2),%rax;
EXTRA_SY $1, yvec15, xvec7;
#ifndef	TRMMKERNEL
LDL_SY	0*SIZE(C0), xvec6, xvec6;
LDH_SY	2*SIZE(C0), xvec6, xvec6;
ADD_SY	xvec6, xvec15, xvec15;
#endif
STL_SY	xvec15, 0*SIZE(C0);
STH_SY	xvec15, 2*SIZE(C0);
#ifndef	TRMMKERNEL
LDL_SY	4*SIZE(C1), xvec5, xvec5;
LDH_SY	6*SIZE(C1), xvec5, xvec5;
ADD_SY	xvec5, xvec7, xvec7;
#endif
STL_SY	xvec7, 4*SIZE(C1);
STH_SY	xvec7, 6*SIZE(C1);

EXTRA_SY $1, yvec14, xvec6;
#ifndef	TRMMKERNEL
LDL_SY	0*SIZE(C0, ldc, 1), xvec5, xvec5;
LDH_SY	2*SIZE(C0, ldc, 1), xvec5, xvec5;
ADD_SY	xvec5, xvec14, xvec14;
#endif
STL_SY	xvec14, 0*SIZE(C0, ldc, 1);
STH_SY	xvec14, 2*SIZE(C0, ldc, 1);
#ifndef	TRMMKERNEL
LDL_SY	4*SIZE(C1, ldc, 1), xvec4, xvec4;
LDH_SY	6*SIZE(C1, ldc, 1), xvec4, xvec4;
ADD_SY	xvec4, xvec6, xvec6;
#endif
STL_SY	xvec6, 4*SIZE(C1, ldc, 1);
STH_SY	xvec6, 6*SIZE(C1, ldc, 1);

EXTRA_SY $1, yvec13, xvec5;
#ifndef	TRMMKERNEL
LDL_SY	0*SIZE(C0, ldc, 2), xvec4, xvec4;
LDH_SY	2*SIZE(C0, ldc, 2), xvec4, xvec4;
ADD_SY	xvec4, xvec13, xvec13;
#endif
STL_SY	xvec13, 0*SIZE(C0, ldc, 2);
STH_SY	xvec13, 2*SIZE(C0, ldc, 2);
#ifndef	TRMMKERNEL
LDL_SY	4*SIZE(C1, ldc, 2), xvec3, xvec3;
LDH_SY	6*SIZE(C1, ldc, 2), xvec3, xvec3;
ADD_SY	xvec3, xvec5, xvec5;
#endif
STL_SY	xvec5, 4*SIZE(C1, ldc, 2);
STH_SY	xvec5, 6*SIZE(C1, ldc, 2);

EXTRA_SY $1, yvec12, xvec4;
#ifndef	TRMMKERNEL
LDL_SY	0*SIZE(C0, %rax, 1), xvec3, xvec3;
LDH_SY	2*SIZE(C0, %rax, 1), xvec3, xvec3;
ADD_SY	xvec3, xvec12, xvec12;
#endif
STL_SY	xvec12, 0*SIZE(C0, %rax, 1);
STH_SY	xvec12, 2*SIZE(C0, %rax, 1);
#ifndef	TRMMKERNEL
LDL_SY	4*SIZE(C1, %rax, 1), xvec2, xvec2;
LDH_SY	6*SIZE(C1, %rax, 1), xvec2, xvec2;
ADD_SY	xvec2, xvec4, xvec4;
#endif
STL_SY	xvec4, 4*SIZE(C1, %rax, 1);
STH_SY	xvec4, 6*SIZE(C1, %rax, 1);

EXTRA_SY $1, yvec11, xvec3;
#ifndef	TRMMKERNEL
LDL_SY	0*SIZE(C1), xvec2, xvec2;
LDH_SY	2*SIZE(C1), xvec2, xvec2;
ADD_SY	xvec2, xvec11, xvec11;
#endif
STL_SY	xvec11, 0*SIZE(C1);
STH_SY	xvec11, 2*SIZE(C1);
#ifndef	TRMMKERNEL
LDL_SY	4*SIZE(C0), xvec1, xvec1;
LDH_SY	6*SIZE(C0), xvec1, xvec1;
ADD_SY	xvec1, xvec3, xvec3;
#endif
STL_SY	xvec3, 4*SIZE(C0);
STH_SY	xvec3, 6*SIZE(C0);

EXTRA_SY $1, yvec10, xvec2;
#ifndef	TRMMKERNEL
LDL_SY	0*SIZE(C1, ldc, 1), xvec1, xvec1;
LDH_SY	2*SIZE(C1, ldc, 1), xvec1, xvec1;
ADD_SY	xvec1, xvec10, xvec10;
#endif
STL_SY	xvec10, 0*SIZE(C1, ldc, 1);
STH_SY	xvec10, 2*SIZE(C1, ldc, 1);
#ifndef	TRMMKERNEL
LDL_SY	4*SIZE(C0, ldc, 1), xvec0, xvec0;
LDH_SY	6*SIZE(C0, ldc, 1), xvec0, xvec0;
ADD_SY	xvec0, xvec2, xvec2;
#endif
STL_SY	xvec2, 4*SIZE(C0, ldc, 1);
STH_SY	xvec2, 6*SIZE(C0, ldc, 1);

EXTRA_SY $1, yvec9, xvec1;
#ifndef	TRMMKERNEL
LDL_SY	0*SIZE(C1, ldc, 2), xvec0, xvec0;
LDH_SY	2*SIZE(C1, ldc, 2), xvec0, xvec0;
ADD_SY	xvec0, xvec9, xvec9;
#endif
STL_SY	xvec9, 0*SIZE(C1, ldc, 2);
STH_SY	xvec9, 2*SIZE(C1, ldc, 2);
#ifndef	TRMMKERNEL
LDL_SY	4*SIZE(C0, ldc, 2), xvec7, xvec7;
LDH_SY	6*SIZE(C0, ldc, 2), xvec7, xvec7;
ADD_SY	xvec7, xvec1, xvec1;
#endif
STL_SY	xvec1, 4*SIZE(C0, ldc, 2);
STH_SY	xvec1, 6*SIZE(C0, ldc, 2);

EXTRA_SY $1, yvec8, xvec0;
#ifndef	TRMMKERNEL
LDL_SY	0*SIZE(C1, %rax, 1), xvec6, xvec6;
LDH_SY	2*SIZE(C1, %rax, 1), xvec6, xvec6;
ADD_SY	xvec6, xvec8, xvec8;
#endif
STL_SY	xvec8, 0*SIZE(C1, %rax, 1);
STH_SY	xvec8, 2*SIZE(C1, %rax, 1);
#ifndef	TRMMKERNEL
LDL_SY	4*SIZE(C0, %rax, 1), xvec5, xvec5;
LDH_SY	6*SIZE(C0, %rax, 1), xvec5, xvec5;
ADD_SY	xvec5, xvec0, xvec0;
#endif
STL_SY	xvec0, 4*SIZE(C0, %rax, 1);
STH_SY	xvec0, 6*SIZE(C0, %rax, 1);
#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) ||(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
MOVQ	bk, %rax;
SUBQ	kkk, %rax;
LEAQ 	(, %rax, SIZE), %rax;
LEAQ	(ptrba, %rax, 8), ptrba;
LEAQ 	(ptrbb, %rax, 8), ptrbb;
#endif
#if defined(TRMMKERNEL) && defined(LEFT)
ADDQ	$8, kk
#endif
ADDQ	$8*SIZE, C0;
ADDQ	$8*SIZE, C1;
DECQ i;
JG .L1_bodyB;
ALIGN_4
.L1_loopE:;
TEST $4, bm;
JLE	.L5_loopE;
ALIGN_4
.L5_bodyB:
#if !defined(TRMMKERNEL)||(defined(TRMMKERNEL)&&defined(LEFT)&&defined(TRANSA))||(defined(TRMMKERNEL)&&!defined(LEFT)&&!defined(TRANSA))
MOVQ bb, ptrbb;
#else
MOVQ bb, ptrbb;
MOVQ kk, %rax;
LEAQ (, %rax, SIZE), %rax;
LEAQ (ptrba, %rax, 4), ptrba;
LEAQ (ptrbb, %rax, 8), ptrbb;
#endif
#### Initial Results Register ####
XOR_SY	yvec15, yvec15, yvec15;
XOR_SY	yvec14, yvec14, yvec14;
XOR_SY	yvec13, yvec13, yvec13;
XOR_SY	yvec12, yvec12, yvec12;
LD_SX	0*SIZE(ptrba), xvec0;
XOR_SY	yvec11, yvec11, yvec11;
XOR_SY	yvec10, yvec10, yvec10;
EDUP_SX	0*SIZE(ptrbb), xvec2;
XOR_SY	yvec9, yvec9, yvec9;
XOR_SY	yvec8, yvec8, yvec8;
ODUP_SX	0*SIZE(ptrbb), xvec3;
#ifndef	TRMMKERNEL
MOVQ bk, k;
#elif (defined(LEFT)&&!defined(TRANSA))||(!defined(LEFT)&&defined(TRANSA))
MOVQ bk, %rax;
SUBQ kk, %rax;
MOVQ %rax, kkk;
#else
MOVQ kk, %rax;
#ifdef	LEFT
ADDQ $4, %rax;
#else
ADDQ $8, %rax;
#endif
MOVQ %rax, kkk;
#endif
SARQ $2, k;
JLE .L8_loopE;
ALIGN_4
.L8_bodyB:

#### Unroll time 1 ####
SHUF_SX	$0x4e, xvec2, xvec4;
MUL_SX xvec0, xvec2, xvec2;
ADD_SX xvec2, xvec15, xvec15;
SHUF_SX	$0x4e, xvec3, xvec5;
MUL_SX xvec0, xvec3, xvec3;
ADD_SX xvec3, xvec14, xvec14;

EDUP_SX	4*SIZE(ptrbb), xvec2;
MUL_SX xvec0, xvec4, xvec4;
ADD_SX xvec4, xvec13, xvec13;
ODUP_SX 4*SIZE(ptrbb), xvec3;
MUL_SX xvec0, xvec5, xvec5;
ADD_SX xvec5, xvec12, xvec12;

LD_SX	4*SIZE(ptrba), xvec1;
SHUF_SX	$0x4e, xvec2, xvec4;
MUL_SX xvec0, xvec2, xvec2;
ADD_SX xvec2, xvec11, xvec11;
SHUF_SX	$0x4e, xvec3, xvec5;
MUL_SX xvec0, xvec3, xvec3;
ADD_SX xvec3, xvec10, xvec10;

EDUP_SX	8*SIZE(ptrbb), xvec2;
MUL_SX xvec0, xvec4, xvec4;
ADD_SX xvec4, xvec9, xvec9;
ODUP_SX 8*SIZE(ptrbb), xvec3;
MUL_SX xvec0, xvec5, xvec5;
ADD_SX xvec5, xvec8, xvec8;

#### Unroll time 2 ####
SHUF_SX	$0x4e, xvec2, xvec4;
MUL_SX xvec1, xvec2, xvec2;
ADD_SX xvec2, xvec15, xvec15;
SHUF_SX	$0x4e, xvec3, xvec5;
MUL_SX xvec1, xvec3, xvec3;
ADD_SX xvec3, xvec14, xvec14;

EDUP_SX	12*SIZE(ptrbb), xvec2;
MUL_SX xvec1, xvec4, xvec4;
ADD_SX xvec4, xvec13, xvec13;
ODUP_SX	12*SIZE(ptrbb), xvec3;
MUL_SX xvec1, xvec5, xvec5;
ADD_SX xvec5, xvec12, xvec12;

LD_SX	8*SIZE(ptrba), xvec0;
SHUF_SX	$0x4e, xvec2, xvec4;
MUL_SX xvec1, xvec2, xvec2;
ADD_SX xvec2, xvec11, xvec11;
SHUF_SX	$0x4e, xvec3, xvec5;
MUL_SX xvec1, xvec3, xvec3;
ADD_SX xvec3, xvec10, xvec10;

EDUP_SX	16*SIZE(ptrbb), xvec2;
MUL_SX xvec1, xvec4, xvec4;
ADD_SX xvec4, xvec9, xvec9;
ODUP_SX	16*SIZE(ptrbb), xvec3;
MUL_SX xvec1, xvec5, xvec5;
ADD_SX xvec5, xvec8, xvec8;

#### Unroll time 3 ####
SHUF_SX	$0x4e, xvec2, xvec4;
MUL_SX xvec0, xvec2, xvec2;
ADD_SX xvec2, xvec15, xvec15;
SHUF_SX	$0x4e, xvec3, xvec5;
MUL_SX xvec0, xvec3, xvec3;
ADD_SX xvec3, xvec14, xvec14;

EDUP_SX	20*SIZE(ptrbb), xvec2;
MUL_SX xvec0, xvec4, xvec4;
ADD_SX xvec4, xvec13, xvec13;
ODUP_SX	20*SIZE(ptrbb), xvec3;
MUL_SX xvec0, xvec5, xvec5;
ADD_SX xvec5, xvec12, xvec12;

LD_SX	12*SIZE(ptrba), xvec1;
SHUF_SX	$0x4e, xvec2, xvec4;
MUL_SX xvec0, xvec2, xvec2;
ADD_SX xvec2, xvec11, xvec11;
SHUF_SX	$0x4e, xvec3, xvec5;
MUL_SX xvec0, xvec3, xvec3;
ADD_SX xvec3, xvec10, xvec10;

EDUP_SX	24*SIZE(ptrbb), xvec2;
MUL_SX xvec0, xvec4, xvec4;
ADD_SX xvec4, xvec9, xvec9;
ODUP_SX	24*SIZE(ptrbb), xvec3;
MUL_SX xvec0, xvec5, xvec5;
ADD_SX xvec5, xvec8, xvec8;
ADDQ	$16*SIZE, ptrba;

#### Unroll time 4 ####
SHUF_SX	$0x4e, xvec2, xvec4;
MUL_SX xvec1, xvec2, xvec2;
ADD_SX xvec2, xvec15, xvec15;
SHUF_SX	$0x4e, xvec3, xvec5;
MUL_SX xvec1, xvec3, xvec3;
ADD_SX xvec3, xvec14, xvec14;

EDUP_SX	28*SIZE(ptrbb), xvec2;
MUL_SX xvec1, xvec4, xvec4;
ADD_SX xvec4, xvec13, xvec13;
ODUP_SX	28*SIZE(ptrbb), xvec3;
MUL_SX xvec1, xvec5, xvec5;
ADD_SX xvec5, xvec12, xvec12;
ADDQ	$32*SIZE, ptrbb;

LD_SX	0*SIZE(ptrba), xvec0;
SHUF_SX	$0x4e, xvec2, xvec4;
MUL_SX xvec1, xvec2, xvec2;
ADD_SX xvec2, xvec11, xvec11;
SHUF_SX	$0x4e, xvec3, xvec5;
MUL_SX xvec1, xvec3, xvec3;
ADD_SX xvec3, xvec10, xvec10;

EDUP_SX	0*SIZE(ptrbb), xvec2;
MUL_SX xvec1, xvec4, xvec4;
ADD_SX xvec4, xvec9, xvec9;
ODUP_SX	0*SIZE(ptrbb), xvec3;
MUL_SX xvec1, xvec5, xvec5;
ADD_SX xvec5, xvec8, xvec8;
DECQ k;
JG .L8_bodyB;
ALIGN_4
.L8_loopE:
#ifndef	TRMMKERNEL
TEST $2, bk;
#else
TEST $2, kkk;
#endif
JLE .L9_loopE;
ALIGN_4
.L9_bodyB:
#### Unroll time 1 ####
SHUF_SX	$0x4e, xvec2, xvec4;
MUL_SX xvec0, xvec2, xvec2;
ADD_SX xvec2, xvec15, xvec15;
SHUF_SX	$0x4e, xvec3, xvec5;
MUL_SX xvec0, xvec3, xvec3;
ADD_SX xvec3, xvec14, xvec14;

EDUP_SX	4*SIZE(ptrbb), xvec2;
MUL_SX xvec0, xvec4, xvec4;
ADD_SX xvec4, xvec13, xvec13;
ODUP_SX	4*SIZE(ptrbb), xvec3;
MUL_SX xvec0, xvec5, xvec5;
ADD_SX xvec5, xvec12, xvec12;

LD_SX	4*SIZE(ptrba), xvec1;
SHUF_SX	$0x4e, xvec2, xvec4;
MUL_SX xvec0, xvec2, xvec2;
ADD_SX xvec2, xvec11, xvec11;
SHUF_SX	$0x4e, xvec3, xvec5;
MUL_SX xvec0, xvec3, xvec3;
ADD_SX xvec3, xvec10, xvec10;

EDUP_SX	8*SIZE(ptrbb), xvec2;
MUL_SX xvec0, xvec4, xvec4;
ADD_SX xvec4, xvec9, xvec9;
ODUP_SX	8*SIZE(ptrbb), xvec3;
MUL_SX xvec0, xvec5, xvec5;
ADD_SX xvec5, xvec8, xvec8;

#### Unroll time 2 ####
ADDQ	$8*SIZE, ptrba;
SHUF_SX	$0x4e, xvec2, xvec4;
MUL_SX xvec1, xvec2, xvec2;
ADD_SX xvec2, xvec15, xvec15;
SHUF_SX	$0x4e, xvec3, xvec5;
MUL_SX xvec1, xvec3, xvec3;
ADD_SX xvec3, xvec14, xvec14;

EDUP_SX	12*SIZE(ptrbb), xvec2;
MUL_SX xvec1, xvec4, xvec4;
ADD_SX xvec4, xvec13, xvec13;
ODUP_SX	12*SIZE(ptrbb), xvec3;
MUL_SX xvec1, xvec5, xvec5;
ADD_SX xvec5, xvec12, xvec12;
ADDQ	$16*SIZE, ptrbb;

LD_SX	0*SIZE(ptrba), xvec0;
SHUF_SX	$0x4e, xvec2, xvec4;
MUL_SX xvec1, xvec2, xvec2;
ADD_SX xvec2, xvec11, xvec11;
SHUF_SX	$0x4e, xvec3, xvec5;
MUL_SX xvec1, xvec3, xvec3;
ADD_SX xvec3, xvec10, xvec10;

EDUP_SX	0*SIZE(ptrbb), xvec2;
MUL_SX xvec1, xvec4, xvec4;
ADD_SX xvec4, xvec9, xvec9;
ODUP_SX	0*SIZE(ptrbb), xvec3;
MUL_SX xvec1, xvec5, xvec5;
ADD_SX xvec5, xvec8, xvec8;

.L9_loopE:
#ifndef	TRMMKERNEL
TEST $1, bk;
#else
TEST $1, kkk;
#endif
JLE .L10_loopE;
ALIGN_4
.L10_bodyB:
#### Unroll time 1 ####
SHUF_SX	$0x4e, xvec2, xvec4;
MUL_SX xvec0, xvec2, xvec2;
ADD_SX xvec2, xvec15, xvec15;
SHUF_SX	$0x4e, xvec3, xvec5;
MUL_SX xvec0, xvec3, xvec3;
ADD_SX xvec3, xvec14, xvec14;
ADDQ	$4*SIZE, ptrba;

EDUP_SX	4*SIZE(ptrbb), xvec2;
MUL_SX xvec0, xvec4, xvec4;
ADD_SX xvec4, xvec13, xvec13;
ODUP_SX 4*SIZE(ptrbb), xvec3;
MUL_SX xvec0, xvec5, xvec5;
ADD_SX xvec5, xvec12, xvec12;
ADDQ	$8*SIZE, ptrbb;

SHUF_SX	$0x4e, xvec2, xvec4;
MUL_SX xvec0, xvec2, xvec2;
ADD_SX xvec2, xvec11, xvec11;
SHUF_SX	$0x4e, xvec3, xvec5;
MUL_SX xvec0, xvec3, xvec3;
ADD_SX xvec3, xvec10, xvec10;

MUL_SX xvec0, xvec4, xvec4;
ADD_SX xvec4, xvec9, xvec9;
MUL_SX xvec0, xvec5, xvec5;
ADD_SX xvec5, xvec8, xvec8;

.L10_loopE:
#### Multiply Alpha ####
BROAD_SX MEMALPHA, xvec7;
MUL_SX xvec7, xvec15, xvec15;
MUL_SX xvec7, xvec14, xvec14;
MUL_SX xvec7, xvec13, xvec13;
MUL_SX xvec7, xvec12, xvec12;
MUL_SX xvec7, xvec11, xvec11;
MUL_SX xvec7, xvec10, xvec10;
MUL_SX xvec7, xvec9, xvec9;
MUL_SX xvec7, xvec8, xvec8;
#### Reverse Result ####
MOV_SX	xvec15, xvec7;
REVS_SX	$0xe4, xvec13, xvec15, xvec15;
REVS_SX	$0xe4, xvec7, xvec13, xvec13;
MOV_SX	xvec14, xvec7;
REVS_SX	$0xe4, xvec12, xvec14, xvec14;
REVS_SX	$0xe4, xvec7, xvec12, xvec12;
MOV_SX	xvec11, xvec7;
REVS_SX	$0xe4, xvec9, xvec11, xvec11;
REVS_SX	$0xe4, xvec7, xvec9, xvec9;
MOV_SX	xvec10, xvec7;
REVS_SX	$0xe4, xvec8, xvec10, xvec10;
REVS_SX	$0xe4, xvec7, xvec8, xvec8;
#### Testing Alignment ####
MOVQ	C0, %rax;
OR		ldc, %rax;
TEST	$15, %rax;
JNE		.L10_loopEx;
ALIGN_4
LEAQ (ldc,ldc,2),%rax;
#ifndef	TRMMKERNEL
ADD_SX 0*SIZE(C0), xvec15, xvec15;
ADD_SX 0*SIZE(C0, ldc,1), xvec14, xvec14;
ADD_SX 0*SIZE(C0, ldc,2), xvec13, xvec13;
ADD_SX 0*SIZE(C0, %rax,1), xvec12, xvec12;
ADD_SX 0*SIZE(C1), xvec11, xvec11;
ADD_SX 0*SIZE(C1, ldc,1), xvec10, xvec10;
ADD_SX 0*SIZE(C1, ldc,2), xvec9, xvec9;
ADD_SX 0*SIZE(C1, %rax,1), xvec8, xvec8;
#endif
ST_SX	xvec15, 0*SIZE(C0);
ST_SX	xvec14, 0*SIZE(C0, ldc, 1);
ST_SX	xvec13, 0*SIZE(C0, ldc, 2);
ST_SX	xvec12, 0*SIZE(C0, %rax, 1);
ST_SX	xvec11, 0*SIZE(C1);
ST_SX	xvec10, 0*SIZE(C1, ldc, 1);
ST_SX	xvec9, 0*SIZE(C1, ldc, 2);
ST_SX	xvec8, 0*SIZE(C1, %rax, 1);
#if	(defined(TRMMKERNEL)&&defined(LEFT)&&defined(TRANSA))||(defined(TRMMKERNEL)&&!defined(LEFT)&&!defined(TRANSA))
MOVQ	bk, %rax;
SUBQ	kkk, %rax;
LEAQ	(, %rax, SIZE), %rax;
LEAQ	(ptrba, %rax, 4), ptrba;
LEAQ	(ptrbb, %rax, 8), ptrbb;
#endif
#if	defined(TRMMKERNEL)&&defined(LEFT)
ADDQ	$4, kk
#endif
ADDQ	$4*SIZE, C0;
ADDQ	$4*SIZE, C1;
JMP	.L5_loopE;
ALIGN_4
.L10_loopEx:
LEAQ (ldc,ldc,2),%rax;
#ifndef	TRMMKERNEL
LDL_SX 0*SIZE(C0), xvec7, xvec7;
LDH_SX 2*SIZE(C0), xvec7, xvec7;
LDL_SX 0*SIZE(C0, ldc, 1), xvec6, xvec6;
LDH_SX 2*SIZE(C0, ldc, 1), xvec6, xvec6;
LDL_SX 0*SIZE(C0, ldc, 2), xvec5, xvec5;
LDH_SX 2*SIZE(C0, ldc, 2), xvec5, xvec5;
LDL_SX 0*SIZE(C0, %rax, 1), xvec4, xvec4;
LDH_SX 2*SIZE(C0, %rax, 1), xvec4, xvec4;
LDL_SX 0*SIZE(C1), xvec3, xvec3;
LDH_SX 2*SIZE(C1), xvec3, xvec3;
LDL_SX 0*SIZE(C1, ldc, 1), xvec2, xvec2;
LDH_SX 2*SIZE(C1, ldc, 1), xvec2, xvec2;
LDL_SX 0*SIZE(C1, ldc, 2), xvec1, xvec1;
LDH_SX 2*SIZE(C1, ldc, 2), xvec1, xvec1;
LDL_SX 0*SIZE(C1, %rax, 1), xvec0, xvec0;
LDH_SX 2*SIZE(C1, %rax, 1), xvec0, xvec0;
ADD_SX xvec7, xvec15, xvec15;
ADD_SX xvec6, xvec14, xvec14;
ADD_SX xvec5, xvec13, xvec13;
ADD_SX xvec4, xvec12, xvec12;
ADD_SX xvec3, xvec11, xvec11;
ADD_SX xvec2, xvec10, xvec10;
ADD_SX xvec1, xvec9, xvec9;
ADD_SX xvec0, xvec8, xvec8;
#endif
STL_SX	xvec15, 0*SIZE(C0);
STH_SX	xvec15, 2*SIZE(C0);
STL_SX	xvec14, 0*SIZE(C0, ldc, 1);
STH_SX	xvec14, 2*SIZE(C0, ldc, 1);
STL_SX	xvec13, 0*SIZE(C0, ldc, 2);
STH_SX	xvec13, 2*SIZE(C0, ldc, 2);
STL_SX 	xvec12, 0*SIZE(C0, %rax, 1);
STH_SX	xvec12, 2*SIZE(C0, %rax, 1);
STL_SX	xvec11, 0*SIZE(C1);
STH_SX	xvec11, 2*SIZE(C1);
STL_SX	xvec10, 0*SIZE(C1, ldc, 1);
STH_SX	xvec10, 2*SIZE(C1, ldc, 1);
STL_SX	xvec9, 0*SIZE(C1, ldc, 2);
STH_SX	xvec9, 2*SIZE(C1, ldc, 2);
STL_SX	xvec8, 0*SIZE(C1, %rax, 1);
STH_SX	xvec8, 2*SIZE(C1, %rax, 1);
#if	(defined(TRMMKERNEL)&&defined(LEFT)&&defined(TRANSA))||(defined(TRMMKERNEL)&&!defined(LEFT)&&!defined(TRANSA))
MOVQ	bk, %rax;
SUBQ	kkk, %rax;
LEAQ	(, %rax, SIZE), %rax;
LEAQ	(ptrba, %rax, 4), ptrba;
LEAQ	(ptrbb, %rax, 8), ptrbb;
#endif
#if	defined(TRMMKERNEL)&&defined(LEFT)
ADDQ	$4, kk
#endif
ADDQ	$4*SIZE, C0;
ADDQ	$4*SIZE, C1;
.L5_loopE:
TEST $2, bm;
JLE	.L6_loopE;
ALIGN_4
.L6_bodyB:
#if	!defined(TRMMKERNEL)||(defined(TRMMKERNEL)&&defined(LEFT)&&defined(TRANSA))||(defined(TRMMKERNEL)&&!defined(LEFT)&&!defined(TRANSA))
MOVQ bb, ptrbb;
#else
MOVQ bb, ptrbb;
MOVQ kk, %rax;
LEAQ (, %rax, SIZE), %rax;
LEAQ (ptrba, %rax, 2), ptrba;
LEAQ (ptrbb, %rax, 8), ptrbb
#endif
#### Initial Results Register ####
XOR_SY	yvec15, yvec15, yvec15;
XOR_SY	yvec14, yvec14, yvec14;
XOR_SY	yvec13, yvec13, yvec13;
XOR_SY	yvec12, yvec12, yvec12;
MOVQ bk, k;
#ifndef	TRMMKERNEL
MOVQ bk, k;
#elif (defined(LEFT)&&!defined(TRANSA))||(!defined(LEFT)&&defined(TRANSA))
MOVQ	bk, %rax;
SUBQ 	kk, %rax;
MOVQ 	%rax, kkk;
#else
MOVQ	kk, %rax;
#ifdef	LEFT
ADDQ	$2, %rax;
#else
ADDQ	$8, %rax;
#endif
MOVQ	%rax, kkk;
#endif
SARQ $2, k;
JLE .L11_loopE;
ALIGN_4
.L11_bodyB:
#### Computing kernel
LD_SX	0*SIZE(ptrba), xvec0;	# a1, a2, a3, a4
SHUF_SX	$0x44, xvec0, xvec1;	# a1, a2, a1, a2
EDUP_SX	0*SIZE(ptrbb), xvec2;
ODUP_SX	0*SIZE(ptrbb), xvec3;
MUL_SX xvec1, xvec2, xvec2;
ADD_SX xvec2, xvec15, xvec15;
MUL_SX xvec1, xvec3, xvec3;
ADD_SX xvec3, xvec14, xvec14;

EDUP_SX	4*SIZE(ptrbb), xvec4;
ODUP_SX	4*SIZE(ptrbb), xvec5;
MUL_SX xvec1, xvec4, xvec4;
ADD_SX xvec4, xvec13, xvec13;
MUL_SX xvec1, xvec5, xvec5;
ADD_SX xvec5, xvec12, xvec12;

SHUF_SX	$0xee, xvec0, xvec6;
EDUP_SX	8*SIZE(ptrbb), xvec2;
ODUP_SX	8*SIZE(ptrbb), xvec3;
MUL_SX xvec6, xvec2, xvec2;
ADD_SX xvec2, xvec15, xvec15;
MUL_SX xvec6, xvec3, xvec3;
ADD_SX xvec3, xvec14, xvec14;

EDUP_SX	12*SIZE(ptrbb), xvec4;
ODUP_SX	12*SIZE(ptrbb), xvec5;
MUL_SX xvec6, xvec4, xvec4;
ADD_SX xvec4, xvec13, xvec13;
MUL_SX xvec6, xvec5, xvec5;
ADD_SX xvec5, xvec12, xvec12;

LD_SX	4*SIZE(ptrba), xvec0;
SHUF_SX	$0x44, xvec0, xvec1;
EDUP_SX	16*SIZE(ptrbb), xvec2;
ODUP_SX	16*SIZE(ptrbb), xvec3;
MUL_SX xvec1, xvec2, xvec2;
ADD_SX xvec2, xvec15, xvec15;
MUL_SX xvec1, xvec3, xvec3;
ADD_SX xvec3, xvec14, xvec14;

EDUP_SX	20*SIZE(ptrbb), xvec4;
ODUP_SX	20*SIZE(ptrbb), xvec5;
MUL_SX xvec1, xvec4, xvec4;
ADD_SX xvec4, xvec13, xvec13;
MUL_SX xvec1, xvec5, xvec5;
ADD_SX xvec5, xvec12, xvec12;

SHUF_SX	$0xee, xvec0, xvec6;
EDUP_SX	24*SIZE(ptrbb), xvec2;
ODUP_SX	24*SIZE(ptrbb), xvec3;
MUL_SX xvec6, xvec2, xvec2;
ADD_SX xvec2, xvec15, xvec15;
MUL_SX xvec6, xvec3, xvec3;
ADD_SX xvec3, xvec14, xvec14;

EDUP_SX	28*SIZE(ptrbb), xvec4;
ODUP_SX	28*SIZE(ptrbb), xvec5;
MUL_SX xvec6, xvec4, xvec4;
ADD_SX xvec4, xvec13, xvec13;
MUL_SX xvec6, xvec5, xvec5;
ADD_SX xvec5, xvec12, xvec12;

ADDQ	$8*SIZE, ptrba;
ADDQ	$32*SIZE, ptrbb;
DECQ	k;
JG .L11_bodyB;
ALIGN_4
.L11_loopE:
#ifndef	TRMMKERNEL
TEST $2, bk;
#else
TEST $2, kkk;
#endif
JLE .L12_loopE;
ALIGN_4
.L12_bodyB:
LD_SX	0*SIZE(ptrba), xvec0;	# a1, a2, a3, a4
SHUF_SX	$0x44, xvec0, xvec1;	# a1, a2, a1, a2
EDUP_SX	0*SIZE(ptrbb), xvec2;
ODUP_SX	0*SIZE(ptrbb), xvec3;
MUL_SX xvec1, xvec2, xvec2;
ADD_SX xvec2, xvec15, xvec15;
MUL_SX xvec1, xvec3, xvec3;
ADD_SX xvec3, xvec14, xvec14;

EDUP_SX	4*SIZE(ptrbb), xvec4;
ODUP_SX	4*SIZE(ptrbb), xvec5;
MUL_SX xvec1, xvec4, xvec4;
ADD_SX xvec4, xvec13, xvec13;
MUL_SX xvec1, xvec5, xvec5;
ADD_SX xvec5, xvec12, xvec12;

SHUF_SX	$0xee, xvec0, xvec6;
EDUP_SX	8*SIZE(ptrbb), xvec2;
ODUP_SX	8*SIZE(ptrbb), xvec3;
MUL_SX xvec6, xvec2, xvec2;
ADD_SX xvec2, xvec15, xvec15;
MUL_SX xvec6, xvec3, xvec3;
ADD_SX xvec3, xvec14, xvec14;

EDUP_SX	12*SIZE(ptrbb), xvec4;
ODUP_SX	12*SIZE(ptrbb), xvec5;
MUL_SX xvec6, xvec4, xvec4;
ADD_SX xvec4, xvec13, xvec13;
MUL_SX xvec6, xvec5, xvec5;
ADD_SX xvec5, xvec12, xvec12;
ADDQ	$4*SIZE, ptrba;
ADDQ	$16*SIZE, ptrbb;

.L12_loopE:
#ifndef	TRMMKERNEL
TEST $1, bk;
#else
TEST $1, kkk;
#endif
JLE .L13_loopE;
ALIGN_4
.L13_bodyB:
LD_SX	0*SIZE(ptrba), xvec0;	# a1, a2, a3, a4
SHUF_SX	$0x44, xvec0, xvec1;	# a1, a2, a1, a2
EDUP_SX	0*SIZE(ptrbb), xvec2;
ODUP_SX	0*SIZE(ptrbb), xvec3;
MUL_SX xvec1, xvec2, xvec2;
ADD_SX xvec2, xvec15, xvec15;
MUL_SX xvec1, xvec3, xvec3;
ADD_SX xvec3, xvec14, xvec14;

EDUP_SX	4*SIZE(ptrbb), xvec4;
ODUP_SX	4*SIZE(ptrbb), xvec5;
MUL_SX xvec1, xvec4, xvec4;
ADD_SX xvec4, xvec13, xvec13;
MUL_SX xvec1, xvec5, xvec5;
ADD_SX xvec5, xvec12, xvec12;
ADDQ	$2*SIZE, ptrba;
ADDQ	$8*SIZE, ptrbb;
.L13_loopE:
LEAQ (ldc,ldc,2),%rax;
#### Multiply Alpha ####
BROAD_SX MEMALPHA, xvec7;
MUL_SX xvec7, xvec15, xvec15;
MUL_SX xvec7, xvec14, xvec14;
MUL_SX xvec7, xvec13, xvec13;
MUL_SX xvec7, xvec12, xvec12;
#ifndef	TRMMKERNEL
LDL_SX 0*SIZE(C0), xvec11, xvec11;
LDH_SX 0*SIZE(C0, ldc, 2), xvec11, xvec11;
LDL_SX 0*SIZE(C0, ldc, 1), xvec10, xvec10;
LDH_SX 0*SIZE(C0, %rax, 1), xvec10, xvec10;
LDL_SX 0*SIZE(C1), xvec9, xvec9;
LDH_SX 0*SIZE(C1, ldc, 2), xvec9, xvec9;
LDL_SX 0*SIZE(C1, ldc, 1), xvec8, xvec8;
LDH_SX 0*SIZE(C1, %rax,1), xvec8, xvec8;
ADD_SX xvec11, xvec15, xvec15;
ADD_SX xvec10, xvec14, xvec14;
ADD_SX xvec9, xvec13, xvec13;
ADD_SX xvec8, xvec12, xvec12;
#endif
STL_SX	xvec15, 0*SIZE(C0);
STH_SX	xvec15, 0*SIZE(C0, ldc, 2);
STL_SX	xvec14, 0*SIZE(C0, ldc, 1);
STH_SX	xvec14, 0*SIZE(C0, %rax, 1);
STL_SX	xvec13, 0*SIZE(C1);
STH_SX	xvec13, 0*SIZE(C1, ldc, 2);
STL_SX	xvec12, 0*SIZE(C1, ldc, 1);
STH_SX	xvec12, 0*SIZE(C1, %rax, 1);
#if (defined(TRMMKERNEL)&&defined(LEFT)&&defined(TRANSA))||(defined(TRMMKERNEL)&&!defined(LEFT)&&!defined(TRANSA))
MOVQ	bk, %rax;
SUBQ	kkk, %rax;
LEAQ	(,%rax, SIZE), %rax;
LEAQ	(ptrba, %rax, 2), ptrba;
LEAQ	(ptrbb, %rax, 8), ptrbb;
#endif
#if defined(TRMMKERNEL) && defined(LEFT)
ADDQ	$2, kk
#endif
ADDQ	$2*SIZE, C0;
ADDQ	$2*SIZE, C1;
#### Writing Back ####
.L6_loopE:
TEST $1, bm;
JLE .L7_loopE;
ALIGN_4
.L7_bodyB:
#if !defined(TRMMKERNEL)||(defined(TRMMKERNEL)&&defined(LEFT)&&defined(TRANSA))||(defined(TRMMKERNEL)&&!defined(LEFT)&&!defined(TRANSA))
MOVQ bb, ptrbb;
#else
MOVQ bb, ptrbb;
MOVQ kk, %rax;
LEAQ (,%rax, SIZE), %rax;
ADDQ	%rax, ptrba;
LEAQ (ptrbb, %rax, 8), ptrbb;
#endif
#### intitial ####
XOR_SY	yvec15, yvec15, yvec15;
XOR_SY	yvec14, yvec14, yvec14;
MOVQ bk, k;
#ifndef	TRMMKERNEL
MOVQ bk, k;
#elif (defined(LEFT)&&!defined(TRANSA))||(!defined(LEFT)&&defined(TRANSA))
MOVQ	bk, %rax;
SUBQ	kk, %rax;
MOVQ	%rax, kkk;
#else
MOVQ	kk, %rax;
#ifdef	LEFT
ADDQ	$1, %rax;
#else
ADDQ	$8, %rax;
#endif
MOVQ	%rax, kkk;
#endif
SARQ $2, k;
JLE .L14_loopE;
ALIGN_4
.L14_bodyB:
BROAD_SX 0*SIZE(ptrba), xvec0;
LD_SX	0*SIZE(ptrbb), xvec2;
MUL_SX xvec0, xvec2, xvec2;
ADD_SX xvec2, xvec15, xvec15;
LD_SX	4*SIZE(ptrbb), xvec3;
MUL_SX xvec0, xvec3, xvec3;
ADD_SX xvec3, xvec14, xvec14;

BROAD_SX 1*SIZE(ptrba), xvec1;
LD_SX	8*SIZE(ptrbb), xvec4;
MUL_SX xvec1, xvec4, xvec4;
ADD_SX xvec4, xvec15, xvec15;
LD_SX	12*SIZE(ptrbb), xvec5;
MUL_SX xvec1, xvec5, xvec5;
ADD_SX xvec5, xvec14, xvec14;

BROAD_SX 2*SIZE(ptrba), xvec0;
LD_SX	16*SIZE(ptrbb), xvec2;
MUL_SX xvec0, xvec2, xvec2;
ADD_SX xvec2, xvec15, xvec15;
LD_SX	20*SIZE(ptrbb), xvec3;
MUL_SX xvec0, xvec3, xvec3;
ADD_SX xvec3, xvec14, xvec14;

BROAD_SX 3*SIZE(ptrba), xvec1;
LD_SX	24*SIZE(ptrbb), xvec4;
MUL_SX xvec1, xvec4, xvec4;
ADD_SX xvec4, xvec15, xvec15;
LD_SX	28*SIZE(ptrbb), xvec5;
MUL_SX xvec1, xvec5, xvec5;
ADD_SX xvec5, xvec14, xvec14;

ADDQ	$4*SIZE, ptrba;
ADDQ	$32*SIZE, ptrbb;
DECQ	k;
JG .L14_bodyB;
ALIGN_4
.L14_loopE:
#ifndef	TRMMKERNEL
TEST $2, bk;
#else
TEST $2, kkk;
#endif
JLE .L15_loopE;
ALIGN_4
.L15_bodyB:
BROAD_SX 0*SIZE(ptrba), xvec0;
LD_SX	0*SIZE(ptrbb), xvec2;
MUL_SX xvec0, xvec2, xvec2;
ADD_SX xvec2, xvec15, xvec15;
LD_SX	4*SIZE(ptrbb), xvec3;
MUL_SX xvec0, xvec3, xvec3;
ADD_SX xvec3, xvec14, xvec14;

BROAD_SX 1*SIZE(ptrba), xvec1;
LD_SX	8*SIZE(ptrbb), xvec4;
MUL_SX xvec1, xvec4, xvec4;
ADD_SX xvec4, xvec15, xvec15;
LD_SX	12*SIZE(ptrbb), xvec5;
MUL_SX xvec1, xvec5, xvec5;
ADD_SX xvec5, xvec14, xvec14;
ADDQ	$2*SIZE, ptrba;
ADDQ	$16*SIZE, ptrbb;

.L15_loopE:
#ifndef	TRMMKERNEL
TEST $1, bk;
#else
TEST $1, kkk;
#endif
JLE	.L16_loopE;
ALIGN_4
.L16_bodyB:
BROAD_SX 0*SIZE(ptrba), xvec0;
LD_SX	0*SIZE(ptrbb), xvec2;
MUL_SX xvec0, xvec2, xvec2;
ADD_SX xvec2, xvec15, xvec15;
LD_SX	4*SIZE(ptrbb), xvec3;
MUL_SX xvec0, xvec3, xvec3;
ADD_SX xvec3, xvec14, xvec14;
ADDQ	$1, ptrba;
ADDQ	$4, ptrbb;

.L16_loopE:
BROAD_SX MEMALPHA, xvec7;
MUL_SX xvec7, xvec15, xvec15;
MUL_SX xvec7, xvec14, xvec14;

LEAQ (ldc,ldc,2),%rax;
SHUF_SX	$0xff, xvec15, xvec13;
SHUF_SX	$0xaa, xvec15, xvec12;
SHUF_SX	$0x55, xvec15, xvec11;
SHUF_SX	$0x00, xvec15, xvec10;

#ifndef	TRMMKERNEL
addss	0*SIZE(C0), xvec10;
addss	0*SIZE(C0, ldc, 1), xvec11;
addss	0*SIZE(C0, ldc, 2), xvec12;
addss	0*SIZE(C0, %rax, 1), xvec13;
#endif
movss	xvec10, 0*SIZE(C0);
movss	xvec11, 0*SIZE(C0, ldc, 1);
movss	xvec12, 0*SIZE(C0, ldc, 2);
movss	xvec13, 0*SIZE(C0, %rax, 1);

SHUF_SX	$0xff, xvec14, xvec9;
SHUF_SX	$0xaa, xvec14, xvec8;
SHUF_SX	$0x55, xvec14, xvec7;
SHUF_SX	$0x00, xvec14, xvec6;

#ifndef	TRMMKERNEL
addss	0*SIZE(C1), xvec6;
addss	0*SIZE(C1, ldc, 1), xvec7;
addss	0*SIZE(C1, ldc, 2), xvec8;
addss	0*SIZE(C1, %rax, 1), xvec9;
#endif
movss	xvec6, 0*SIZE(C1);
movss	xvec7, 0*SIZE(C1, ldc, 1);
movss	xvec8, 0*SIZE(C1, ldc, 2);
movss	xvec9, 0*SIZE(C1, %rax, 1);
#if (defined(TRMMKERNEL)&&defined(LEFT)&&defined(TRANSA))||(defined(TRMMKERNEL)&&!defined(LEFT)&&!defined(TRANSA))
MOVQ	bk, %rax;
SUBQ	kkk, %rax;
LEAQ	(,%rax, SIZE), %rax;
ADDQ	%rax, ptrba;
LEAQ	(ptrbb, %rax, 8), ptrbb;
#endif
#if defined(TRMMKERNEL)&&defined(LEFT)
ADDQ	$1, kk
#endif
ADDQ	$1*SIZE, C0;
ADDQ	$1*SIZE, C1;
#### Writing Back ####
.L7_loopE:
#if	defined(TRMMKERNEL)&&!defined(LEFT)
ADDQ	$8, kk
#endif
MOVQ bk,k;
SALQ $5,k;
ADDQ k,bb;
LEAQ (C,ldc,8),C;
.L0_bodyE:;
DECQ j;
JG .L0_bodyB;
ALIGN_4;
.L0_loopE:;
TEST $4, bn;					# Rn = 4
JLE	.L20_loopE;
ALIGN_4;
.L20_bodyB:
#if defined(TRMMKERNEL) && defined(LEFT)
MOVQ OFFSET, %rax;
MOVQ %rax, kk;
#endif

MOVQ C, C0;
LEAQ (C, ldc, 2), C1;
MOVQ ba, ptrba;
MOVQ bm, i;
SARQ $3, i;
JLE .L21_loopE;
ALIGN_4
.L21_bodyB:
#if !defined(TRMMKERNEL)||(defined(TRMMKERNEL) && defined(LEFT) && defined(TRANSA))||(defined(TRMMKERNEL)&&!defined(LEFT)&&!defined(TRANSA))
MOVQ bb, ptrbb;
#else
MOVQ bb, ptrbb;
MOVQ kk, %rax;
LEAQ (, %rax, SIZE), %rax;
LEAQ (ptrba, %rax, 8), ptrba;
LEAQ (ptrbb, %rax, 4), ptrbb;
#endif
#### Initial ####
XOR_SY yvec15, yvec15, yvec15;
XOR_SY yvec14, yvec14, yvec14;
XOR_SY yvec13, yvec13, yvec13;
XOR_SY yvec12, yvec12, yvec12;
EDUP_SX	0*SIZE(ptrbb), xvec2;
XOR_SY yvec11, yvec11, yvec11;
XOR_SY yvec10, yvec10, yvec10;
LD_SX	0*SIZE(ptrba), xvec0;
XOR_SY yvec9, yvec9, yvec9;
XOR_SY yvec8, yvec8, yvec8;
LD_SX	4*SIZE(ptrba), xvec1;
#ifndef	TRMMKERNEL
MOVQ bk,k;
#elif (defined(LEFT) && !defined(TRANSA))||(!defined(LEFT)&&defined(TRANSA))
MOVQ bk, %rax;
SUBQ kk, %rax;
MOVQ %rax, kkk;
#else
MOVQ kk, %rax;
#ifdef LEFT
ADDQ $8, %rax;
#else
ADDQ $4, %rax;
#endif
MOVQ %rax, kkk;
#endif
SARQ $2,k;
JLE .L211_loopE;
ALIGN_4
.L211_bodyB:
#### Unroll time 1 ####
ODUP_SX	0*SIZE(ptrbb), xvec3;
SHUF_SX	$0x4e, xvec2, xvec4;
MOV_SX	xvec2, xvec6;
MUL_SX xvec0, xvec2, xvec2;
ADD_SX xvec2, xvec15, xvec15;
MUL_SX xvec1, xvec6, xvec6;
ADD_SX xvec6, xvec14, xvec14;

SHUF_SX	$0x4e, xvec3, xvec5;
MOV_SX	xvec3, xvec7;
MUL_SX xvec0, xvec3, xvec3;
ADD_SX xvec3, xvec13, xvec13;
MUL_SX xvec1, xvec7, xvec7;
ADD_SX xvec7, xvec12, xvec12;

EDUP_SX	4*SIZE(ptrbb), xvec2;
MOV_SX	xvec4, xvec6;
MUL_SX xvec0, xvec4, xvec4;
ADD_SX xvec4, xvec11, xvec11;
MUL_SX xvec1, xvec6, xvec6;
ADD_SX xvec6, xvec10, xvec10;

MOV_SX	xvec5, xvec7;
MUL_SX xvec0, xvec5, xvec5;
ADD_SX xvec5, xvec9, xvec9;
LD_SX	8*SIZE(ptrba), xvec0;
MUL_SX xvec1, xvec7, xvec7;
ADD_SX xvec7, xvec8, xvec8;
LD_SX	12*SIZE(ptrba), xvec1;

#### Unroll time 2 ####
ODUP_SX	4*SIZE(ptrbb), xvec3;
SHUF_SX	$0x4e, xvec2, xvec4;
MOV_SX	xvec2, xvec6;
MUL_SX xvec0, xvec2, xvec2;
ADD_SX xvec2, xvec15, xvec15;
MUL_SX xvec1, xvec6, xvec6;
ADD_SX xvec6, xvec14, xvec14;

SHUF_SX	$0x4e, xvec3, xvec5;
MOV_SX	xvec3, xvec7;
MUL_SX xvec0, xvec3, xvec3;
ADD_SX xvec3, xvec13, xvec13;
MUL_SX xvec1, xvec7, xvec7;
ADD_SX xvec7, xvec12, xvec12;

EDUP_SX	8*SIZE(ptrbb), xvec2;
MOV_SX	xvec4, xvec6;
MUL_SX xvec0, xvec4, xvec4;
ADD_SX xvec4, xvec11, xvec11;
MUL_SX xvec1, xvec6, xvec6;
ADD_SX xvec6, xvec10, xvec10;

MOV_SX	xvec5, xvec7;
MUL_SX xvec0, xvec5, xvec5;
ADD_SX xvec5, xvec9, xvec9;
LD_SX	16*SIZE(ptrba), xvec0;
MUL_SX xvec1, xvec7, xvec7;
ADD_SX xvec7, xvec8, xvec8;
LD_SX	20*SIZE(ptrba), xvec1;

#### Unroll time 3 ####
ODUP_SX	8*SIZE(ptrbb), xvec3;
SHUF_SX	$0x4e, xvec2, xvec4;
MOV_SX	xvec2, xvec6;
MUL_SX xvec0, xvec2, xvec2;
ADD_SX xvec2, xvec15, xvec15;
MUL_SX xvec1, xvec6, xvec6;
ADD_SX xvec6, xvec14, xvec14;

SHUF_SX	$0x4e, xvec3, xvec5;
MOV_SX	xvec3, xvec7;
MUL_SX xvec0, xvec3, xvec3;
ADD_SX xvec3, xvec13, xvec13;
MUL_SX xvec1, xvec7, xvec7;
ADD_SX xvec7, xvec12, xvec12;

EDUP_SX	12*SIZE(ptrbb), xvec2;
MOV_SX	xvec4, xvec6;
ADDQ	$16*SIZE, ptrbb;

MUL_SX xvec0, xvec4, xvec4;
ADD_SX xvec4, xvec11, xvec11;
MUL_SX xvec1, xvec6, xvec6;
ADD_SX xvec6, xvec10, xvec10;

MOV_SX	xvec5, xvec7;
MUL_SX xvec0, xvec5, xvec5;
ADD_SX xvec5, xvec9, xvec9;
LD_SX	24*SIZE(ptrba), xvec0;

MUL_SX xvec1, xvec7, xvec7;
ADD_SX xvec7, xvec8, xvec8;
LD_SX	28*SIZE(ptrba), xvec1;
ADDQ	$32*SIZE, ptrba;

#### Unroll time 4 ####
ODUP_SX	-4*SIZE(ptrbb), xvec3;
SHUF_SX	$0x4e, xvec2, xvec4;
MOV_SX	xvec2, xvec6;
MUL_SX xvec0, xvec2, xvec2;
ADD_SX xvec2, xvec15, xvec15;
MUL_SX xvec1, xvec6, xvec6;
ADD_SX xvec6, xvec14, xvec14;

SHUF_SX	$0x4e, xvec3, xvec5;
MOV_SX	xvec3, xvec7;
MUL_SX xvec0, xvec3, xvec3;
ADD_SX xvec3, xvec13, xvec13;
MUL_SX xvec1, xvec7, xvec7;
ADD_SX xvec7, xvec12, xvec12;

EDUP_SX	0*SIZE(ptrbb), xvec2;
MOV_SX	xvec4, xvec6;
MUL_SX xvec0, xvec4, xvec4;
ADD_SX xvec4, xvec11, xvec11;
MUL_SX xvec1, xvec6, xvec6;
ADD_SX xvec6, xvec10, xvec10;

MOV_SX	xvec5, xvec7;
MUL_SX xvec0, xvec5, xvec5;
ADD_SX xvec5, xvec9, xvec9;
LD_SX	0*SIZE(ptrba), xvec0;

MUL_SX xvec1, xvec7, xvec7;
ADD_SX xvec7, xvec8, xvec8;
LD_SX	4*SIZE(ptrba), xvec1;
DECQ	k;
JG	.L211_bodyB;
ALIGN_4
.L211_loopE:
#ifndef	TRMMKERNEL
TEST $2, bk
#else
TEST $2, kkk;
#endif
JLE .L212_loopE;
ALIGN_4
.L212_bodyB:
#### Unroll time 1 ####
ODUP_SX	0*SIZE(ptrbb), xvec3;
SHUF_SX	$0x4e, xvec2, xvec4;
MOV_SX	xvec2, xvec6;
MUL_SX xvec0, xvec2, xvec2;
ADD_SX xvec2, xvec15, xvec15;
MUL_SX xvec1, xvec6, xvec6;
ADD_SX xvec6, xvec14, xvec14;

SHUF_SX	$0x4e, xvec3, xvec5;
MOV_SX	xvec3, xvec7;
MUL_SX xvec0, xvec3, xvec3;
ADD_SX xvec3, xvec13, xvec13;
MUL_SX xvec1, xvec7, xvec7;
ADD_SX xvec7, xvec12, xvec12;

EDUP_SX	4*SIZE(ptrbb), xvec2;
MOV_SX	xvec4, xvec6;
MUL_SX xvec0, xvec4, xvec4;
ADD_SX xvec4, xvec11, xvec11;
ADDQ	$8*SIZE, ptrbb;

MUL_SX xvec1, xvec6, xvec6;
ADD_SX xvec6, xvec10, xvec10;
MOV_SX	xvec5, xvec7;
MUL_SX xvec0, xvec5, xvec5;
ADD_SX xvec5, xvec9, xvec9;
LD_SX	8*SIZE(ptrba), xvec0;

MUL_SX xvec1, xvec7, xvec7;
ADD_SX xvec7, xvec8, xvec8;
LD_SX	12*SIZE(ptrba), xvec1;
ADDQ	$16*SIZE, ptrba;

#### Unroll time 2 ####
ODUP_SX	-4*SIZE(ptrbb), xvec3;
SHUF_SX	$0x4e, xvec2, xvec4;
MOV_SX	xvec2, xvec6;
MUL_SX xvec0, xvec2, xvec2;
ADD_SX xvec2, xvec15, xvec15;
MUL_SX xvec1, xvec6, xvec6;
ADD_SX xvec6, xvec14, xvec14;

SHUF_SX	$0x4e, xvec3, xvec5;
MOV_SX	xvec3, xvec7;
MUL_SX xvec0, xvec3, xvec3;
ADD_SX xvec3, xvec13, xvec13;
MUL_SX xvec1, xvec7, xvec7;
ADD_SX xvec7, xvec12, xvec12;

EDUP_SX	0*SIZE(ptrbb), xvec2;
MOV_SX	xvec4, xvec6;
MUL_SX xvec0, xvec4, xvec4;
ADD_SX xvec4, xvec11, xvec11;
MUL_SX xvec1, xvec6, xvec6;
ADD_SX xvec6, xvec10, xvec10;

MOV_SX	xvec5, xvec7;
MUL_SX xvec0, xvec5, xvec5;
ADD_SX xvec5, xvec9, xvec9;
LD_SX	0*SIZE(ptrba), xvec0;
MUL_SX xvec1, xvec7, xvec7;
ADD_SX xvec7, xvec8, xvec8;
LD_SX	4*SIZE(ptrba), xvec1;

.L212_loopE:
#ifndef	TRMMKERNEL
TEST $1, bk;
#else
TEST $1, kkk;
#endif
JLE .L213_loopE;
ALIGN_4
.L213_bodyB:
ODUP_SX	0*SIZE(ptrbb), xvec3;
SHUF_SX	$0x4e, xvec2, xvec4;
MOV_SX	xvec2, xvec6;
MUL_SX xvec0, xvec2, xvec2;
ADD_SX xvec2, xvec15, xvec15;
ADDQ	$4*SIZE, ptrbb;

SHUF_SX	$0x4e, xvec3, xvec5;
MUL_SX xvec1, xvec6, xvec6;
ADD_SX xvec6, xvec14, xvec14;
MOV_SX	xvec3, xvec7;
MUL_SX xvec0, xvec3, xvec3;
ADD_SX xvec3, xvec13, xvec13;

MUL_SX xvec1, xvec7, xvec7;
ADD_SX xvec7, xvec12, xvec12;
MOV_SX	xvec4, xvec6;
ADDQ	$8*SIZE, ptrba;

MUL_SX xvec0, xvec4, xvec4;
ADD_SX xvec4, xvec11, xvec11;
MUL_SX xvec1, xvec6, xvec6;
ADD_SX xvec6, xvec10, xvec10;

MOV_SX	xvec5, xvec7;
MUL_SX xvec0, xvec5, xvec5;
ADD_SX xvec5, xvec9, xvec9;
MUL_SX xvec1, xvec7, xvec7;
ADD_SX xvec7, xvec8, xvec8;

.L213_loopE:
#### Multiply Alpha ####
BROAD_SX MEMALPHA, xvec7;
MUL_SX xvec7, xvec15, xvec15;
MUL_SX xvec7, xvec14, xvec14;
MUL_SX xvec7, xvec13, xvec13;
MUL_SX xvec7, xvec12, xvec12;
MUL_SX xvec7, xvec11, xvec11;
MUL_SX xvec7, xvec10, xvec10;
MUL_SX xvec7, xvec9, xvec9;
MUL_SX xvec7, xvec8, xvec8;
#### Writing Back ####
#ifndef	TRMMKERNEL
LDL_SX 0*SIZE(C0), xvec0, xvec0;
LDH_SX 2*SIZE(C1), xvec0, xvec0;
LDL_SX 4*SIZE(C0), xvec1, xvec1;
LDH_SX 6*SIZE(C1), xvec1, xvec1;
LDL_SX 0*SIZE(C0, ldc, 1), xvec2, xvec2;
LDH_SX 2*SIZE(C1, ldc, 1), xvec2, xvec2;
LDL_SX 4*SIZE(C0, ldc, 1), xvec3, xvec3;
LDH_SX 6*SIZE(C1, ldc, 1), xvec3, xvec3;
LDL_SX 0*SIZE(C1), xvec4, xvec4;
LDH_SX 2*SIZE(C0), xvec4, xvec4;
LDL_SX 4*SIZE(C1), xvec5, xvec5;
LDH_SX 6*SIZE(C0), xvec5, xvec5;
LDL_SX 0*SIZE(C1, ldc, 1), xvec6, xvec6;
LDH_SX 2*SIZE(C0, ldc, 1), xvec6, xvec6;
LDL_SX 4*SIZE(C1, ldc, 1), xvec7, xvec7;
LDH_SX 6*SIZE(C0, ldc, 1), xvec7, xvec7;
ADD_SX xvec0, xvec15, xvec15;
ADD_SX xvec1, xvec14, xvec14;
ADD_SX xvec2, xvec13, xvec13;
ADD_SX xvec3, xvec12, xvec12;
ADD_SX xvec4, xvec11, xvec11;
ADD_SX xvec5, xvec10, xvec10;
ADD_SX xvec6, xvec9, xvec9;
ADD_SX xvec7, xvec8, xvec8;
#endif
STL_SX	xvec15, 0*SIZE(C0);
STH_SX	xvec15, 2*SIZE(C1);
STL_SX	xvec14, 4*SIZE(C0);
STH_SX	xvec14, 6*SIZE(C1);
STL_SX	xvec13, 0*SIZE(C0, ldc, 1);
STH_SX	xvec13, 2*SIZE(C1, ldc, 1);
STL_SX	xvec12, 4*SIZE(C0, ldc, 1);
STH_SX	xvec12, 6*SIZE(C1, ldc, 1);
STL_SX	xvec11, 0*SIZE(C1);
STH_SX	xvec11, 2*SIZE(C0);
STL_SX	xvec10, 4*SIZE(C1);
STH_SX	xvec10, 6*SIZE(C0);
STL_SX	xvec9, 0*SIZE(C1, ldc, 1);
STH_SX	xvec9, 2*SIZE(C0, ldc, 1);
STL_SX	xvec8, 4*SIZE(C1, ldc, 1);
STH_SX	xvec8, 6*SIZE(C0, ldc, 1);
#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) ||(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
MOVQ	bk, %rax;
SUBQ	kkk, %rax;
LEAQ 	(, %rax, SIZE), %rax;
LEAQ	(ptrba, %rax, 8), ptrba;
LEAQ 	(ptrbb, %rax, 4), ptrbb;
#endif
#if defined(TRMMKERNEL) && defined(LEFT)
ADDQ	$8, kk
#endif
ADDQ	$8*SIZE, C0;
ADDQ	$8*SIZE, C1;
DECQ	i;
JG	.L21_bodyB;
ALIGN_4
.L21_loopE:
TEST $4, bm;
JLE .L22_loopE;
ALIGN_4
.L22_bodyB:
#if !defined(TRMMKERNEL)||(defined(TRMMKERNEL)&&defined(LEFT)&&defined(TRANSA))||(defined(TRMMKERNEL)&&!defined(LEFT)&&!defined(TRANSA))
MOVQ bb, ptrbb;
#else
MOVQ bb, ptrbb;
MOVQ kk, %rax;
LEAQ (, %rax, SIZE), %rax;
LEAQ (ptrba, %rax, 4), ptrba;
LEAQ (ptrbb, %rax, 4), ptrbb;
#endif
#### Initial Results ####
XOR_SY	yvec15, yvec15, yvec15;
XOR_SY	yvec14, yvec14, yvec14;
XOR_SY	yvec13, yvec13, yvec13;
XOR_SY	yvec12, yvec12, yvec12;
#ifndef	TRMMKERNEL
MOVQ bk, k;
#elif (defined(LEFT)&&!defined(TRANSA))||(!defined(LEFT)&&defined(TRANSA))
MOVQ bk, %rax;
SUBQ kk, %rax;
MOVQ %rax, kkk;
#else
MOVQ kk, %rax;
#ifdef	LEFT
ADDQ $4, %rax;
#else
ADDQ $4, %rax;
#endif
MOVQ %rax, kkk;
#endif
SARQ $2, k;
JLE .L221_loopE;
ALIGN_4
.L221_bodyB:
LD_SX	0*SIZE(ptrba), xvec0;
EDUP_SX	0*SIZE(ptrbb), xvec2;
ODUP_SX	0*SIZE(ptrbb), xvec3;

SHUF_SX	$0x4e, xvec2, xvec4;
MUL_SX xvec0, xvec2, xvec2;
ADD_SX xvec2, xvec15, xvec15;
SHUF_SX	$0x4e, xvec3, xvec5;
MUL_SX xvec0, xvec3, xvec3;
ADD_SX xvec3, xvec14, xvec14;

MUL_SX xvec0, xvec4, xvec4;
ADD_SX xvec4, xvec13, xvec13;
MUL_SX xvec0, xvec5, xvec5;
ADD_SX xvec5, xvec12, xvec12;

LD_SX	4*SIZE(ptrba), xvec1;
EDUP_SX	4*SIZE(ptrbb), xvec2;
ODUP_SX	4*SIZE(ptrbb), xvec3;

SHUF_SX	$0x4e, xvec2, xvec4;
MUL_SX xvec1, xvec2, xvec2;
ADD_SX xvec2, xvec15, xvec15;
SHUF_SX	$0x4e, xvec3, xvec5;
MUL_SX xvec1, xvec3, xvec3;
ADD_SX xvec3, xvec14, xvec14;

MUL_SX xvec1, xvec4, xvec4;
ADD_SX xvec4, xvec13, xvec13;
MUL_SX xvec1, xvec5, xvec5;
ADD_SX xvec5, xvec12, xvec12;

LD_SX	8*SIZE(ptrba), xvec0;
EDUP_SX	8*SIZE(ptrbb), xvec2;
ODUP_SX	8*SIZE(ptrbb), xvec3;

SHUF_SX	$0x4e, xvec2, xvec4;
MUL_SX xvec0, xvec2, xvec2;
ADD_SX xvec2, xvec15, xvec15;
SHUF_SX	$0x4e, xvec3, xvec5;
MUL_SX xvec0, xvec3, xvec3;
ADD_SX xvec3, xvec14, xvec14;

MUL_SX xvec0, xvec4, xvec4;
ADD_SX xvec4, xvec13, xvec13;
MUL_SX xvec0, xvec5, xvec5;
ADD_SX xvec5, xvec12, xvec12;

LD_SX	12*SIZE(ptrba), xvec1;
EDUP_SX	12*SIZE(ptrbb), xvec2;
ODUP_SX	12*SIZE(ptrbb), xvec3;

SHUF_SX	$0x4e, xvec2, xvec4;
MUL_SX xvec1, xvec2, xvec2;
ADD_SX xvec2, xvec15, xvec15
SHUF_SX	$0x4e, xvec3, xvec5;
MUL_SX xvec1, xvec3, xvec3;
ADD_SX xvec3, xvec14, xvec14;

MUL_SX xvec1, xvec4, xvec4;
ADD_SX xvec4, xvec13, xvec13;
MUL_SX xvec1, xvec5, xvec5;
ADD_SX xvec5, xvec12, xvec12;
ADDQ	$16*SIZE, ptrba;
ADDQ	$16*SIZE, ptrbb;

DECQ	k;
JG	.L221_bodyB;
ALIGN_4
.L221_loopE:
#ifndef	TRMMKERNEL
TEST $2, bk;
#else
TEST $2, kkk;
#endif
JLE	.L222_loopE;
ALIGN_4
.L222_bodyB:
LD_SX	0*SIZE(ptrba), xvec0;
EDUP_SX	0*SIZE(ptrbb), xvec2;
ODUP_SX	0*SIZE(ptrbb), xvec3;

SHUF_SX	$0x4e, xvec2, xvec4;
MUL_SX xvec0, xvec2, xvec2;
ADD_SX xvec2, xvec15, xvec15;
SHUF_SX	$0x4e, xvec3, xvec5;
MUL_SX xvec0, xvec3, xvec3;
ADD_SX xvec3, xvec14, xvec14;

MUL_SX xvec0, xvec4, xvec4;
ADD_SX xvec4, xvec13, xvec13;
MUL_SX xvec0, xvec5, xvec5;
ADD_SX xvec5, xvec12, xvec12;

LD_SX	4*SIZE(ptrba), xvec1;
EDUP_SX	4*SIZE(ptrbb), xvec2;
ODUP_SX	4*SIZE(ptrbb), xvec3;

SHUF_SX	$0x4e, xvec2, xvec4;
MUL_SX xvec1, xvec2, xvec2;
ADD_SX xvec2, xvec15, xvec15;
SHUF_SX	$0x4e, xvec3, xvec5;
MUL_SX xvec1, xvec3, xvec3;
ADD_SX xvec3, xvec14, xvec14;

MUL_SX xvec1, xvec4, xvec4;
ADD_SX xvec4, xvec13, xvec13
MUL_SX xvec1, xvec5, xvec5;
ADD_SX xvec5, xvec12, xvec12;
ADDQ	$8*SIZE, ptrba;
ADDQ	$8*SIZE, ptrbb;
.L222_loopE:
#ifndef	TRMMKERNEL
TEST $1, bk;
#else
TEST $1, kkk;
#endif
JLE .L223_loopE;
ALIGN_4
.L223_bodyB:
LD_SX	0*SIZE(ptrba), xvec0;
EDUP_SX	0*SIZE(ptrbb), xvec2;
ODUP_SX	0*SIZE(ptrbb), xvec3;

SHUF_SX	$0x4e, xvec2, xvec4;
MUL_SX xvec0, xvec2, xvec2;
ADD_SX xvec2, xvec15, xvec15;
SHUF_SX	$0x4e, xvec3, xvec5;
MUL_SX xvec0, xvec3, xvec3;
ADD_SX xvec3, xvec14, xvec14;

MUL_SX xvec0, xvec4, xvec4;
ADD_SX xvec4, xvec13, xvec13;
MUL_SX xvec0, xvec5, xvec5;
ADD_SX xvec5, xvec12, xvec12;
ADDQ	$4*SIZE, ptrba;
ADDQ	$4*SIZE, ptrbb;
.L223_loopE:
#### Multiply Alpha ####
BROAD_SX MEMALPHA, xvec7;
MUL_SX xvec7, xvec15, xvec15;
MUL_SX xvec7, xvec14, xvec14;
MUL_SX xvec7, xvec13, xvec13;
MUL_SX xvec7, xvec12, xvec12;
#### Writing back ####
#ifndef	TRMMKERNEL
LDL_SX 0*SIZE(C0), xvec0, xvec0;
LDH_SX 2*SIZE(C1), xvec0, xvec0;
LDL_SX 0*SIZE(C0, ldc, 1), xvec1, xvec1;
LDH_SX 2*SIZE(C1, ldc, 1), xvec1, xvec1;
LDL_SX 0*SIZE(C1), xvec2, xvec2;
LDH_SX 2*SIZE(C0), xvec2, xvec2;
LDL_SX 0*SIZE(C1, ldc, 1), xvec3, xvec3;
LDH_SX 2*SIZE(C0, ldc, 1), xvec3, xvec3;
ADD_SX xvec0, xvec15, xvec15;
ADD_SX xvec1, xvec14, xvec14;
ADD_SX xvec2, xvec13, xvec13;
ADD_SX xvec3, xvec12, xvec12;
#endif
STL_SX	xvec15, 0*SIZE(C0);
STH_SX	xvec15, 2*SIZE(C1);
STL_SX	xvec14, 0*SIZE(C0, ldc, 1);
STH_SX	xvec14, 2*SIZE(C1, ldc, 1);
STL_SX	xvec13, 0*SIZE(C1);
STH_SX	xvec13, 2*SIZE(C0);
STL_SX	xvec12, 0*SIZE(C1, ldc, 1);
STH_SX	xvec12, 2*SIZE(C0, ldc, 1);
#if	(defined(TRMMKERNEL)&&defined(LEFT)&&defined(TRANSA))||(defined(TRMMKERNEL)&&!defined(LEFT)&&!defined(TRANSA))
MOVQ	bk, %rax;
SUBQ	kkk, %rax;
LEAQ	(, %rax, SIZE), %rax;
LEAQ	(ptrba, %rax, 4), ptrba;
LEAQ	(ptrbb, %rax, 4), ptrbb;
#endif
#if	defined(TRMMKERNEL)&&defined(LEFT)
ADDQ	$4, kk
#endif
ADDQ	$4*SIZE, C0;
ADDQ	$4*SIZE, C1;
.L22_loopE:
TEST $2, bm;
JLE .L23_loopE;
ALIGN_4
.L23_bodyB:
#if	!defined(TRMMKERNEL)||(defined(TRMMKERNEL)&&defined(LEFT)&&defined(TRANSA))||(defined(TRMMKERNEL)&&!defined(LEFT)&&!defined(TRANSA))
MOVQ bb, ptrbb;
#else
MOVQ bb, ptrbb;
MOVQ kk, %rax;
LEAQ (, %rax, SIZE), %rax;
LEAQ (ptrba, %rax, 2), ptrba;
LEAQ (ptrbb, %rax, 4), ptrbb
#endif
#### Initial ####
XOR_SY	yvec15, yvec15, yvec15;
XOR_SY	yvec14, yvec14, yvec14;
#ifndef	TRMMKERNEL
MOVQ bk, k;
#elif (defined(LEFT)&&!defined(TRANSA))||(!defined(LEFT)&&defined(TRANSA))
MOVQ	bk, %rax;
SUBQ 	kk, %rax;
MOVQ 	%rax, kkk;
#else
MOVQ	kk, %rax;
#ifdef	LEFT
ADDQ	$2, %rax;
#else
ADDQ	$4, %rax;
#endif
MOVQ	%rax, kkk;
#endif
SARQ $2, k;
JLE .L231_loopE;
ALIGN_4
.L231_bodyB:
LD_SX	0*SIZE(ptrba), xvec0;
EDUP_SX	0*SIZE(ptrbb), xvec4;
ODUP_SX	0*SIZE(ptrbb), xvec5;
SHUF_SX	$0x44, xvec0, xvec1;
MUL_SX xvec1, xvec4, xvec4;
ADD_SX xvec4, xvec15, xvec15;
MUL_SX xvec1, xvec5, xvec5;
ADD_SX xvec5, xvec14, xvec14;

SHUF_SX	$0xee, xvec0, xvec2;
EDUP_SX	4*SIZE(ptrbb), xvec6;
ODUP_SX	4*SIZE(ptrbb), xvec7;
MUL_SX xvec2, xvec6, xvec6;
ADD_SX xvec6, xvec15, xvec15;
MUL_SX xvec2, xvec7, xvec7;
ADD_SX xvec7, xvec14, xvec14;

LD_SX	4*SIZE(ptrba), xvec0;
EDUP_SX	8*SIZE(ptrbb), xvec4;
ODUP_SX	8*SIZE(ptrbb), xvec5;
SHUF_SX	$0x44, xvec0, xvec1;
MUL_SX xvec1, xvec4, xvec4;
ADD_SX xvec4, xvec15, xvec15;
MUL_SX xvec1, xvec5, xvec5;
ADD_SX xvec5, xvec14, xvec14;

SHUF_SX	$0xee, xvec0, xvec2;
EDUP_SX	12*SIZE(ptrbb), xvec6;
ODUP_SX	12*SIZE(ptrbb), xvec7;
MUL_SX xvec2, xvec6, xvec6;
ADD_SX xvec6, xvec15, xvec15;
MUL_SX xvec2, xvec7, xvec7;
ADD_SX xvec7, xvec14, xvec14;

ADDQ	$8*SIZE, ptrba;
ADDQ	$16*SIZE, ptrbb;
DECQ	k;
JG .L231_bodyB;
ALIGN_4
.L231_loopE:
#ifndef	TRMMKERNEL
TEST $2, bk;
#else
TEST $2, kkk;
#endif
JLE .L232_loopE;
ALIGN_4
.L232_bodyB:
LD_SX	0*SIZE(ptrba), xvec0;
EDUP_SX	0*SIZE(ptrbb), xvec4;
ODUP_SX	0*SIZE(ptrbb), xvec5;
SHUF_SX	$0x44, xvec0, xvec1;
MUL_SX xvec1, xvec4, xvec4;
ADD_SX xvec4, xvec15, xvec15;
MUL_SX xvec1, xvec5, xvec5;
ADD_SX xvec5, xvec14, xvec14;

SHUF_SX	$0xee, xvec0, xvec2;
EDUP_SX	4*SIZE(ptrbb), xvec6;
ODUP_SX	4*SIZE(ptrbb), xvec7;
MUL_SX xvec2, xvec6, xvec6;
ADD_SX xvec6, xvec15, xvec15;
MUL_SX xvec2, xvec7, xvec7;
ADD_SX xvec7, xvec14, xvec14;

ADDQ	$4*SIZE, ptrba;
ADDQ	$8*SIZE, ptrbb;
.L232_loopE:
#ifndef	TRMMKERNEL
TEST $1, bk;
#else
TEST $1, kkk;
#endif
JLE .L233_loopE;
ALIGN_4
.L233_bodyB:
LD_SX	0*SIZE(ptrba), xvec0;
EDUP_SX	0*SIZE(ptrbb), xvec4;
ODUP_SX	0*SIZE(ptrbb), xvec5;
SHUF_SX	$0x44, xvec0, xvec1;
MUL_SX xvec1, xvec4, xvec4;
ADD_SX xvec4, xvec15, xvec15;
MUL_SX xvec1, xvec5, xvec5;
ADD_SX xvec5, xvec14, xvec14;

ADDQ	$2*SIZE, ptrba;
ADDQ	$4*SIZE, ptrbb;
.L233_loopE:
#### Multiply Alpha ####
BROAD_SY MEMALPHA, yvec7;
MUL_SY	xvec7, xvec15, xvec15;
MUL_SY	xvec7, xvec14, xvec14;
#### Writing Back ####
SHUF_SX	$0xee, xvec15, xvec13;
SHUF_SX	$0xee, xvec14, xvec12;
#ifndef	TRMMKERNEL
ADD_SY	0*SIZE(C0), xvec15, xvec15;
ADD_SY	0*SIZE(C0, ldc, 1), xvec14, xvec14;
ADD_SY	0*SIZE(C1), xvec13, xvec13;
ADD_SY	0*SIZE(C1, ldc, 1), xvec12, xvec12;
#endif
STL_SY	xvec15, 0*SIZE(C0);
STL_SY	xvec14, 0*SIZE(C0, ldc, 1);
STL_SY	xvec13, 0*SIZE(C1);
STL_SY	xvec12, 0*SIZE(C1, ldc, 1);
#if (defined(TRMMKERNEL)&&defined(LEFT)&&defined(TRANSA))||(defined(TRMMKERNEL)&&!defined(LEFT)&&!defined(TRANSA))
MOVQ	bk, %rax;
SUBQ	kkk, %rax;
LEAQ	(,%rax, SIZE), %rax;
LEAQ	(ptrba, %rax, 2), ptrba;
LEAQ	(ptrbb, %rax, 4), ptrbb;
#endif
#if defined(TRMMKERNEL) && defined(LEFT)
ADDQ	$2, kk
#endif
ADDQ	$2*SIZE, C0;
ADDQ	$2*SIZE, C1;
.L23_loopE:
TEST $1, bm;
JLE .L24_loopE;
ALIGN_4
.L24_bodyB:
#if !defined(TRMMKERNEL)||(defined(TRMMKERNEL)&&defined(LEFT)&&defined(TRANSA))||(defined(TRMMKERNEL)&&!defined(LEFT)&&!defined(TRANSA))
MOVQ bb, ptrbb;
#else
MOVQ bb, ptrbb;
MOVQ kk, %rax;
LEAQ (,%rax, SIZE), %rax;
ADDQ	%rax, ptrba;
LEAQ (ptrbb, %rax, 4), ptrbb;
#endif
#### Initial ####
XOR_SY	yvec15, yvec15, yvec15;
#ifndef	TRMMKERNEL
MOVQ bk, k;
#elif (defined(LEFT)&&!defined(TRANSA))||(!defined(LEFT)&&defined(TRANSA))
MOVQ	bk, %rax;
SUBQ	kk, %rax;
MOVQ	%rax, kkk;
#else
MOVQ	kk, %rax;
#ifdef	LEFT
ADDQ	$1, %rax;
#else
ADDQ	$4, %rax;
#endif
MOVQ	%rax, kkk;
#endif
SARQ $2, k;
JLE .L241_loopE;
ALIGN_4
.L241_bodyB:
BROAD_SX 0*SIZE(ptrba), xvec0;
LD_SX	0*SIZE(ptrbb), xvec1;
MUL_SX xvec0, xvec1, xvec1;
ADD_SX xvec1, xvec15, xvec15;

BROAD_SX 1*SIZE(ptrba), xvec2;
LD_SX	4*SIZE(ptrbb), xvec3;
MUL_SX xvec2, xvec3, xvec3;
ADD_SX xvec3, xvec15, xvec15;

BROAD_SX 2*SIZE(ptrba), xvec4;
LD_SX	8*SIZE(ptrbb), xvec5;
MUL_SX xvec4, xvec5, xvec5;
ADD_SX xvec5, xvec15, xvec15;

BROAD_SX 3*SIZE(ptrba), xvec6;
LD_SX	12*SIZE(ptrbb), xvec7;
MUL_SX xvec6, xvec7, xvec7;
ADD_SX xvec7, xvec15, xvec15;
ADDQ	$4*SIZE, ptrba;
ADDQ	$16*SIZE, ptrbb;
DECQ	k;
JG .L241_bodyB;
ALIGN_4
.L241_loopE:
#ifndef	TRMMKERNEL
TEST $2, bk;
#else
TEST $2, kkk;
#endif
JLE	.L242_loopE;
ALIGN_4
.L242_bodyB:
BROAD_SX 0*SIZE(ptrba), xvec0;
LD_SX	0*SIZE(ptrbb), xvec1;
MUL_SX xvec0, xvec1, xvec1;
ADD_SX xvec1, xvec15, xvec15;

BROAD_SX 1*SIZE(ptrba), xvec2;
LD_SX	4*SIZE(ptrbb), xvec3;
MUL_SX xvec2, xvec3, xvec3;
ADD_SX xvec3, xvec15, xvec15;
ADDQ	$2*SIZE, ptrba;
ADDQ	$8*SIZE, ptrbb;

.L242_loopE:
#ifndef	TRMMKERNEL
TEST $1, bk;
#else
TEST $1, kkk;
#endif
JLE .L243_loopE;
ALIGN_4;
.L243_bodyB:
BROAD_SX 0*SIZE(ptrba), xvec0;
LD_SX	0*SIZE(ptrbb), xvec1;
MUL_SX xvec0, xvec1, xvec1;
ADD_SX xvec1, xvec15, xvec15;
ADDQ	$1*SIZE, ptrba;
ADDQ	$4*SIZE, ptrbb;
.L243_loopE:
#### Multiply Alpha ####
BROAD_SX MEMALPHA, xvec7;
MUL_SX xvec7, xvec15, xvec15;
SHUF_SX $0xff, xvec15, xvec14;
SHUF_SX $0xaa, xvec15, xvec13;
SHUF_SX $0x55, xvec15, xvec12;
SHUF_SX $0x00, xvec15, xvec11;

#ifndef	TRMMKERNEL
addss	0*SIZE(C0), xvec11;
addss	0*SIZE(C0, ldc, 1), xvec12;
addss	0*SIZE(C1), xvec13;
addss	0*SIZE(C1, ldc, 1), xvec14;
#endif

movss	xvec11, 0*SIZE(C0);
movss	xvec12, 0*SIZE(C0, ldc, 1);
movss	xvec13, 0*SIZE(C1);
movss	xvec14, 0*SIZE(C1, ldc, 1);
#if (defined(TRMMKERNEL)&&defined(LEFT)&&defined(TRANSA))||(defined(TRMMKERNEL)&&!defined(LEFT)&&!defined(TRANSA))
MOVQ	bk, %rax;
SUBQ	kkk, %rax;
LEAQ	(,%rax, SIZE), %rax;
ADDQ	%rax, ptrba;
LEAQ	(ptrbb, %rax, 4), ptrbb;
#endif
#if defined(TRMMKERNEL)&&defined(LEFT)
ADDQ	$1, kk
#endif
ADDQ	$1*SIZE, C0;
ADDQ	$1*SIZE, C1;
.L24_loopE:
#if	defined(TRMMKERNEL)&&!defined(LEFT)
ADDQ	$4, kk
#endif
MOVQ bk, k;
SALQ $4, k;
ADDQ k, bb;
LEAQ (C, ldc, 4), C;
.L20_loopE:
TEST $2, bn;
JLE	.L30_loopE;
ALIGN_4
.L30_bodyB:
#if defined(TRMMKERNEL) && defined(LEFT)
MOVQ	OFFSET, %rax;
MOVQ	%rax, kk
#endif
MOVQ C, C0;
LEAQ (C, ldc, 1), C1;
MOVQ ba, ptrba;
MOVQ bm, i;
SARQ $3, i;
JLE .L31_loopE;
ALIGN_4
.L31_bodyB:
#if !defined(TRMMKERNEL)||(defined(TRMMKERNEL)&&defined(LEFT)&&defined(TRANSA))||(defined(TRMMKERNEL)&&!defined(LEFT)&&!defined(TRANSA))
MOVQ bb, ptrbb;
#else
MOVQ bb, ptrbb;
MOVQ kk, %rax;
LEAQ (, %rax, SIZE), %rax;
LEAQ (ptrba, %rax, 8), ptrba;
LEAQ (ptrbb, %rax, 2), ptrbb;
#endif
#### Initial ####
XOR_SY	yvec15, yvec15, yvec15;
XOR_SY	yvec14, yvec14, yvec14;
XOR_SY	yvec13, yvec13, yvec13;
XOR_SY	yvec12, yvec12, yvec12;
#ifndef	TRMMKERNEL
MOVQ bk, k;
#elif (defined(LEFT) && !defined(TRANSA))||(!defined(LEFT)&&defined(TRANSA))
MOVQ	bk, %rax;
SUBQ	kk, %rax;
MOVQ	%rax, kkk;
#else
MOVQ	kk, %rax;
#ifdef	LEFT
ADDQ	$8, %rax;
#else
ADDQ	$2, %rax;
#endif
MOVQ	%rax, kkk;
#endif
SARQ $2, k;
JLE .L311_loopE;
ALIGN_4
.L311_bodyB:
LD_SX	0*SIZE(ptrbb), xvec2;
SHUF_SX $0x50, xvec2, xvec3;
LD_SX	0*SIZE(ptrba), xvec0;
LD_SX	4*SIZE(ptrba), xvec1;

MOV_SX	xvec3, xvec4;
MUL_SX xvec0, xvec3, xvec3;
ADD_SX xvec3, xvec15, xvec15;
SHUF_SX $0x4e, xvec4, xvec5;
MUL_SX xvec1, xvec4, xvec4;
ADD_SX xvec4, xvec14, xvec14;

MOV_SX	xvec5, xvec6;
MUL_SX xvec0, xvec5, xvec5;
ADD_SX xvec5, xvec13, xvec13;
MUL_SX xvec1, xvec6, xvec6;
ADD_SX xvec6, xvec12, xvec12;

SHUF_SX	$0xfa, xvec2, xvec3;
LD_SX	8*SIZE(ptrba), xvec0;
LD_SX	12*SIZE(ptrba), xvec1;

MOV_SX	xvec3, xvec4;
MUL_SX xvec0, xvec3, xvec3;
ADD_SX xvec3, xvec15, xvec15;
SHUF_SX	$0x4e, xvec4, xvec5;
MUL_SX xvec1, xvec4, xvec4;
ADD_SX xvec4, xvec14, xvec14;

MOV_SX	xvec5, xvec6;
MUL_SX xvec0, xvec5, xvec5;
ADD_SX xvec5, xvec13, xvec13;
MUL_SX xvec1, xvec6, xvec6;
ADD_SX xvec6, xvec12, xvec12;

LD_SX	4*SIZE(ptrbb), xvec2;
SHUF_SX	$0x50, xvec2, xvec3;
LD_SX	16*SIZE(ptrba), xvec0;
LD_SX	20*SIZE(ptrba), xvec1;

MOV_SX	xvec3, xvec4;
MUL_SX xvec0, xvec3, xvec3;
ADD_SX xvec3, xvec15, xvec15;
SHUF_SX	$0x4e, xvec4, xvec5;
MUL_SX xvec1, xvec4, xvec4;
ADD_SX xvec4, xvec14, xvec14;

MOV_SX	xvec5, xvec6;
MUL_SX xvec0, xvec5, xvec5;
ADD_SX xvec5, xvec13, xvec13;
MUL_SX xvec1, xvec6, xvec6;
ADD_SX xvec6, xvec12, xvec12;

SHUF_SX $0xfa, xvec2, xvec3;
LD_SX	24*SIZE(ptrba), xvec0;
LD_SX	28*SIZE(ptrba), xvec1;

MOV_SX	xvec3, xvec4;
MUL_SX xvec0, xvec3, xvec3;
ADD_SX xvec3, xvec15, xvec15;
SHUF_SX	$0x4e, xvec4, xvec5;
MUL_SX xvec1, xvec4, xvec4;
ADD_SX xvec4, xvec14, xvec14;

MOV_SX	xvec5, xvec6;
MUL_SX xvec0, xvec5, xvec5;
ADD_SX xvec5, xvec13, xvec13;
MUL_SX xvec1, xvec6, xvec6;
ADD_SX xvec6, xvec12, xvec12;

ADDQ	$32*SIZE, ptrba;
ADDQ	$8*SIZE, ptrbb;
DECQ	k;
JG .L311_bodyB;
ALIGN_4
.L311_loopE:
#ifndef	TRMMKERNEL
TEST $2, bk;
#else
TEST $2, kkk;
#endif
JLE .L312_loopE;
ALIGN_4
.L312_bodyB:
LD_SX	0*SIZE(ptrbb), xvec2;
SHUF_SX $0x50, xvec2, xvec3;
LD_SX	0*SIZE(ptrba), xvec0;
LD_SX	4*SIZE(ptrba), xvec1;

MOV_SX	xvec3, xvec4;
MUL_SX xvec0, xvec3, xvec3;
ADD_SX xvec3, xvec15, xvec15;
SHUF_SX $0x4e, xvec4, xvec5;
MUL_SX xvec1, xvec4, xvec4;
ADD_SX xvec4, xvec14, xvec14;

MOV_SX	xvec5, xvec6;
MUL_SX xvec0, xvec5, xvec5;
ADD_SX xvec5, xvec13, xvec13;
MUL_SX xvec1, xvec6, xvec6;
ADD_SX xvec6, xvec12, xvec12;

SHUF_SX	$0xfa, xvec2, xvec3;
LD_SX	8*SIZE(ptrba), xvec0;
LD_SX	12*SIZE(ptrba), xvec1;

MOV_SX	xvec3, xvec4;
MUL_SX xvec0, xvec3, xvec3;
ADD_SX xvec3, xvec15, xvec15;
SHUF_SX	$0x4e, xvec4, xvec5;
MUL_SX xvec1, xvec4, xvec4;
ADD_SX xvec4, xvec14, xvec14;

MOV_SX	xvec5, xvec6;
MUL_SX xvec0, xvec5, xvec5;
ADD_SX xvec5, xvec13, xvec13;
MUL_SX xvec1, xvec6, xvec6;
ADD_SX xvec6, xvec12, xvec12;
ADDQ	$16*SIZE, ptrba;
ADDQ	$4*SIZE, ptrbb;

.L312_loopE:
#ifndef	TRMMKERNEL
TEST $1, bk;
#else
TEST $1, kkk;
#endif
JLE .L313_loopE;
ALIGN_4
.L313_bodyB:
LD_SX	0*SIZE(ptrbb), xvec2;
SHUF_SX $0x50, xvec2, xvec3;
LD_SX	0*SIZE(ptrba), xvec0;
LD_SX	4*SIZE(ptrba), xvec1;

MOV_SX	xvec3, xvec4;
MUL_SX xvec0, xvec3, xvec3;
ADD_SX xvec3, xvec15, xvec15;
SHUF_SX $0x4e, xvec4, xvec5;
MUL_SX xvec1, xvec4, xvec4;
ADD_SX xvec4, xvec14, xvec14;

MOV_SX	xvec5, xvec6;
MUL_SX xvec0, xvec5, xvec5;
ADD_SX xvec5, xvec13, xvec13;
MUL_SX xvec1, xvec6, xvec6;
ADD_SX xvec6, xvec12, xvec12;
ADDQ	$8*SIZE, ptrba;
ADDQ	$2*SIZE, ptrbb;

.L313_loopE:
BROAD_SX MEMALPHA, xvec7;
MUL_SX xvec7, xvec15, xvec15;
MUL_SX xvec7, xvec14, xvec14;
MUL_SX xvec7, xvec13, xvec13;
MUL_SX xvec7, xvec12, xvec12;
#### Writing Back ####
#ifndef	TRMMKERNEL
LDL_SX 0*SIZE(C0), xvec0, xvec0;
LDH_SX 2*SIZE(C1), xvec0, xvec0;
LDL_SX 4*SIZE(C0), xvec1, xvec1;
LDH_SX 6*SIZE(C1), xvec1, xvec1;
LDL_SX 0*SIZE(C1), xvec2, xvec2;
LDH_SX 2*SIZE(C0), xvec2, xvec2;
LDL_SX 4*SIZE(C1), xvec3, xvec3;
LDH_SX 6*SIZE(C0), xvec3, xvec3;
ADD_SX xvec0, xvec15, xvec15;
ADD_SX xvec1, xvec14, xvec14;
ADD_SX xvec2, xvec13, xvec13;
ADD_SX xvec3, xvec12, xvec12;
#endif
STL_SX	xvec15, 0*SIZE(C0);
STH_SX	xvec15, 2*SIZE(C1);
STL_SX	xvec14, 4*SIZE(C0);
STH_SX	xvec14, 6*SIZE(C1);
STL_SX	xvec13, 0*SIZE(C1);
STH_SX	xvec13, 2*SIZE(C0);
STL_SX	xvec12, 4*SIZE(C1);
STH_SX	xvec12, 6*SIZE(C0);
#if (defined(TRMMKERNEL)&&defined(LEFT)&&defined(TRANSA))||(defined(TRMMKERNEL)&&!defined(LEFT)&&!defined(TRANSA))
MOVQ	bk, %rax;
SUBQ	kkk, %rax;
LEAQ	(,%rax, SIZE), %rax;
LEAQ 	(ptrba, %rax, 8), ptrba;
LEAQ	(ptrbb, %rax, 2), ptrbb;
#endif
#if defined(TRMMKERNEL) && defined(LEFT)
ADDQ	$8, kk
#endif
ADDQ	$8*SIZE, C0;
ADDQ	$8*SIZE, C1;
DECQ	i;
JG .L31_bodyB;
ALIGN_4
.L31_loopE:
TEST $4, bm;
JLE .L32_loopE;
ALIGN_4
.L32_bodyB:
#if !defined(TRMMKERNEL)||(defined(TRMMKERNEL)&&defined(LEFT)&&defined(TRANSA))||(defined(TRMMKERNEL)&&!defined(LEFT)&&!defined(TRANSA))
MOVQ bb, ptrbb;
#else
MOVQ bb, ptrbb;
MOVQ kk, %rax;
LEAQ	(,%rax, SIZE), %rax;
LEAQ	(ptrba, %rax, 4), ptrba;
LEAQ	(ptrbb, %rax, 2), ptrbb;
#endif
#### Initial ####
XOR_SY	yvec15, yvec15, yvec15;
XOR_SY	yvec14, yvec14, yvec14;
#ifndef	TRMMKERNEL
MOVQ bk, k;
#elif (defined(LEFT) && !defined(TRANSA))||(!defined(LEFT)&&defined(TRANSA))
MOVQ	bk, %rax;
SUBQ	kk, %rax;
MOVQ	%rax, kkk;
#else
MOVQ	kk, %rax;
#ifdef	LEFT
ADDQ	$4, %rax;
#else
ADDQ	$2, %rax;
#endif
MOVQ	%rax, kkk;
#endif
SARQ $2, k;
JLE .L321_loopE;
ALIGN_4
.L321_bodyB:
LD_SX	0*SIZE(ptrba), xvec0;
LD_SX	0*SIZE(ptrbb), xvec2;
SHUF_SX	$0x50, xvec2, xvec3;
SHUF_SX $0x05, xvec2, xvec4;
MUL_SX xvec0, xvec3, xvec3;
ADD_SX xvec3, xvec15, xvec15;
MUL_SX xvec0, xvec4, xvec4;
ADD_SX xvec4, xvec14, xvec14;

LD_SX	4*SIZE(ptrba), xvec0;
SHUF_SX	$0xfa, xvec2, xvec5;
SHUF_SX	$0xaf, xvec2, xvec6;
MUL_SX xvec0, xvec5, xvec5;
ADD_SX xvec5, xvec15, xvec15;
MUL_SX xvec0, xvec6, xvec6;
ADD_SX xvec6, xvec14, xvec14;

LD_SX	8*SIZE(ptrba), xvec0;
LD_SX	4*SIZE(ptrbb), xvec2;
SHUF_SX	$0x50, xvec2, xvec3;
SHUF_SX	$0x05, xvec2, xvec4;
MUL_SX xvec0, xvec3, xvec3;
ADD_SX xvec3, xvec15, xvec15;
MUL_SX xvec0, xvec4, xvec4;
ADD_SX xvec4, xvec14, xvec14;

LD_SX	12*SIZE(ptrba), xvec0;
SHUF_SX	$0xfa, xvec2, xvec5;
SHUF_SX	$0xaf, xvec2, xvec6;
MUL_SX xvec0, xvec5, xvec5;
ADD_SX xvec5, xvec15, xvec15;
MUL_SX xvec0, xvec6, xvec6;
ADD_SX xvec6, xvec14, xvec14;

ADDQ	$16*SIZE, ptrba;
ADDQ	$8*SIZE, ptrbb;
DECQ	k;
JG .L321_bodyB;
ALIGN_4
.L321_loopE:
#ifndef	TRMMKERNEL
TEST $2, bk;
#else
TEST $2, kkk;
#endif
JLE .L322_loopE;
ALIGN_4
.L322_bodyB:
LD_SX	0*SIZE(ptrba), xvec0;
LD_SX	0*SIZE(ptrbb), xvec2;
SHUF_SX	$0x50, xvec2, xvec3;
SHUF_SX $0x05, xvec2, xvec4;
MUL_SX xvec0, xvec3, xvec3;
ADD_SX xvec3, xvec15, xvec15;
MUL_SX xvec0, xvec4, xvec4;
ADD_SX xvec4, xvec14, xvec14;

LD_SX	4*SIZE(ptrba), xvec0;
SHUF_SX	$0xfa, xvec2, xvec5;
SHUF_SX	$0xaf, xvec2, xvec6;
MUL_SX xvec0, xvec5, xvec5;
ADD_SX xvec5, xvec15, xvec15;
MUL_SX xvec0, xvec6, xvec6;
ADD_SX xvec6, xvec14, xvec14;
ADDQ	$8*SIZE, ptrba;
ADDQ	$4*SIZE, ptrbb;

.L322_loopE:
#ifndef	TRMMKERNEL
TEST $1, bk;
#else
TEST $1, kkk;
#endif
JLE .L323_loopE;
ALIGN_4
.L323_bodyB:
LD_SX	0*SIZE(ptrba), xvec0;
LD_SX	0*SIZE(ptrbb), xvec2;
SHUF_SX	$0x50, xvec2, xvec3;
SHUF_SX $0x05, xvec2, xvec4;
MUL_SX xvec0, xvec3, xvec3;
ADD_SX xvec3, xvec15, xvec15;
MUL_SX xvec0, xvec4, xvec4;
ADD_SX xvec4, xvec14, xvec14;
ADDQ	$4*SIZE, ptrba;
ADDQ	$2*SIZE, ptrbb;

.L323_loopE:
BROAD_SX MEMALPHA, xvec7;
MUL_SX xvec7, xvec15, xvec15;
MUL_SX xvec7, xvec14, xvec14;
#### Writing back ####
#ifndef	TRMMKERNEL
LDL_SX 0*SIZE(C0), xvec0, xvec0;
LDH_SX 2*SIZE(C1), xvec0, xvec0;
LDL_SX 0*SIZE(C1), xvec1, xvec1;
LDH_SX 2*SIZE(C0), xvec1, xvec1;
ADD_SX xvec0, xvec15, xvec15;
ADD_SX xvec1, xvec14, xvec14;
#endif
STL_SX	xvec15, 0*SIZE(C0);
STH_SX	xvec15, 2*SIZE(C1);
STL_SX	xvec14, 0*SIZE(C1);
STH_SX	xvec14, 2*SIZE(C0);
#if (defined(TRMMKERNEL)&& defined(LEFT)&&defined(TRANSA))||(defined(TRMMKERNEL)&&!defined(LEFT)&& !defined(TRANSA))
MOVQ	bk, %rax;
SUBQ	kkk, %rax;
LEAQ	(,%rax, SIZE), %rax;
LEAQ	(ptrba, %rax, 4), ptrba;
LEAQ	(ptrbb, %rax, 2), ptrbb;
#endif
#if defined(TRMMKERNEL) && defined(LEFT)
ADDQ	$4, kk
#endif
ADDQ	$4*SIZE, C0;
ADDQ	$4*SIZE, C1;
.L32_loopE:
TEST $2, bm;
JLE .L33_loopE;
ALIGN_4
.L33_bodyB:
#if !defined(TRMMKERNEL)||(defined(TRMMKERNEL)&&defined(LEFT)&&defined(TRANSA))||(defined(TRMMKERNEL)&&!defined(LEFT)&&!defined(TRANSA))
MOVQ bb, ptrbb;
#else
MOVQ bb, ptrbb;
MOVQ kk, %rax;
LEAQ	(,%rax, SIZE), %rax;
LEAQ	(ptrba, %rax, 2), ptrba;
LEAQ	(ptrbb, %rax, 2), ptrbb;
#endif
#### Initial ####
XOR_SY	yvec15, yvec15, yvec15;
XOR_SY	yvec14, yvec14, yvec14;
XOR_SY	yvec13, yvec13, yvec13;
XOR_SY	yvec12, yvec12, yvec12;
#ifndef	TRMMKERNEL
MOVQ bk, k;
#elif (defined(LEFT)&&!defined(TRANSA))||(!defined(LEFT)&&defined(TRANSA))
MOVQ	bk, %rax;
SUBQ	kk, %rax;
MOVQ	%rax, kkk;
#else
MOVQ	kk, %rax;
#ifdef	LEFT
ADDQ	$2, %rax;
#else
ADDQ	$2, %rax;
#endif
MOVQ	%rax, kkk;
#endif
SARQ $2, k;
JLE .L331_loopE;
ALIGN_4
.L331_bodyB:
LD_SX	0*SIZE(ptrba), xvec0;		# a0, a1, a2, a3
EDUP_SX	0*SIZE(ptrbb), xvec2;		# b0, b0, b2, b2
ODUP_SX	0*SIZE(ptrbb), xvec3;		# b1, b1, b3, b3

MUL_SX xvec0, xvec2, xvec2;
ADD_SX xvec2, xvec15, xvec15;
MUL_SX xvec0, xvec3, xvec3;
ADD_SX xvec3, xvec14, xvec14;

LD_SX	4*SIZE(ptrba), xvec0;
EDUP_SX	4*SIZE(ptrbb), xvec2;
ODUP_SX	4*SIZE(ptrbb), xvec3;

MUL_SX xvec0, xvec2, xvec2;
ADD_SX xvec2, xvec15, xvec15;
MUL_SX xvec0, xvec3, xvec3;
ADD_SX xvec3, xvec14, xvec14;
ADDQ	$8*SIZE, ptrba;
ADDQ	$8*SIZE, ptrbb;
DECQ	k;
JG .L331_bodyB;
ALIGN_4
.L331_loopE:
#ifndef	TRMMKERNEL
TEST $2, bk;
#else
TEST $2, kkk;
#endif
JLE .L332_loopE;
ALIGN_4
.L332_bodyB:
LD_SX	0*SIZE(ptrba), xvec0;		# a0, a1, a2, a3
EDUP_SX	0*SIZE(ptrbb), xvec2;		# b0, b0, b2, b2
ODUP_SX	0*SIZE(ptrbb), xvec3;		# b1, b1, b3, b3

MUL_SX xvec0, xvec2, xvec2;
ADD_SX xvec2, xvec15, xvec15;
MUL_SX xvec0, xvec3, xvec3;
ADD_SX xvec3, xvec14, xvec14;

ADDQ	$4*SIZE, ptrba;
ADDQ	$4*SIZE, ptrbb;

.L332_loopE:
#ifndef	TRMMKERNEL
TEST $1, bk;
#else
TEST $1, kkk;
#endif
JLE .L333_loopE;
ALIGN_4
.L333_bodyB:
movss	0*SIZE(ptrba), xvec0;
movss	1*SIZE(ptrba), xvec1;
movss	0*SIZE(ptrbb), xvec2;
XOR_SY	yvec3, yvec3, yvec3;
movss	xvec2, xvec3;
mulss	xvec0, xvec2;
addss	xvec2, xvec15;
mulss	xvec1, xvec3;
SHUF_SX	$0xe1, xvec3, xvec4;
ADD_SX xvec4, xvec15, xvec15;

movss	1*SIZE(ptrbb), xvec5;
XOR_SY	yvec6, yvec6, yvec6;
movss	xvec5, xvec6;
mulss	xvec0, xvec5;
addss	xvec5, xvec14;
mulss	xvec1, xvec6;
SHUF_SX	$0xe1, xvec6, xvec7;
ADD_SX xvec7, xvec14, xvec14

ADDQ	$2*SIZE, ptrba;
ADDQ	$2*SIZE, ptrbb;
.L333_loopE:
BROAD_SX MEMALPHA, xvec7;
MUL_SX xvec7, xvec15, xvec15;
MUL_SX xvec7, xvec14, xvec14;
SHUF_SX	$0xee, xvec15, xvec13;
SHUF_SX	$0xee, xvec14, xvec12;
SHUF_SX	$0x44, xvec15, xvec11;
SHUF_SX	$0x44, xvec14, xvec10;
ADD_SX xvec13, xvec11, xvec11;
ADD_SX xvec12, xvec10, xvec10;

#ifndef	TRMMKERNEL
LDL_SX 0*SIZE(C0), xvec0, xvec0;
LDL_SX 0*SIZE(C1), xvec1, xvec1;
ADD_SX xvec0, xvec11, xvec11;
ADD_SX xvec1, xvec10, xvec10;
#endif
STL_SX	xvec11, 0*SIZE(C0);
STL_SX	xvec10, 0*SIZE(C1);
#if (defined(TRMMKERNEL)&&defined(LEFT)&&defined(TRANSA))||(defined(TRMMKERNEL)&&!defined(LEFT)&&!defined(TRANSA))
MOVQ	bk, %rax;
SUBQ	kkk, %rax;
LEAQ	(,%rax, SIZE), %rax;
LEAQ	(ptrba, %rax, 2), ptrba;
LEAQ	(ptrbb, %rax, 2), ptrbb;
#endif
#if defined(TRMMKERNEL) && defined(LEFT)
ADDQ	$2, kk;
#endif
ADDQ	$2*SIZE, C0;
ADDQ	$2*SIZE, C1;
#### Writing Back ####
.L33_loopE:
TEST $1, bm;
JLE .L34_loopE;
ALIGN_4
.L34_bodyB:
#if !defined(TRMMKERNEL)||(defined(TRMMKERNEL)&&defined(LEFT)&&defined(TRANSA))||(defined(TRMMKERNEL)&&!defined(LEFT)&&!defined(TRANSA))
MOVQ bb, ptrbb;
#else
MOVQ bb, ptrbb;
MOVQ kk, %rax;
LEAQ	(, %rax, SIZE), %rax;
ADDQ	%rax, ptrba;
LEAQ	(ptrbb, %rax, 2), ptrbb;
#endif
#### Initial ####
XOR_SY	yvec15, yvec15, yvec15;
XOR_SY	yvec14, yvec14, yvec14;
#ifndef	TRMMKERNEL
MOVQ bk, k;
#elif (defined(LEFT)&&!defined(TRANSA))||(!defined(LEFT)&&defined(TRANSA))
MOVQ bk, %rax;
SUBQ kk, %rax;
MOVQ %rax, kkk;
#else
MOVQ kk, %rax;
#ifdef	LEFT
ADDQ	$1, %rax;
#else
ADDQ	$2, %rax;
#endif
MOVQ	%rax, kkk;
#endif
SARQ $2, k;
JLE .L341_loopE;
ALIGN_4
.L341_bodyB:
movss	0*SIZE(ptrba), xvec0;
movss	0*SIZE(ptrbb), xvec1;
mulss	xvec0, xvec1;
addss	xvec1, xvec15;

movss	1*SIZE(ptrbb), xvec2;
mulss	xvec0, xvec2;
addss	xvec2, xvec14;

movss	1*SIZE(ptrba), xvec0;
movss	2*SIZE(ptrbb), xvec1;
mulss	xvec0, xvec1;
addss	xvec1, xvec15;

movss	3*SIZE(ptrbb), xvec2;
mulss	xvec0, xvec2;
addss	xvec2, xvec14;

movss	2*SIZE(ptrba), xvec0;
movss	4*SIZE(ptrbb), xvec1;
mulss	xvec0, xvec1;
addss	xvec1, xvec15;

movss	5*SIZE(ptrbb), xvec2;
mulss	xvec0, xvec2;
addss	xvec2, xvec14;

movss	3*SIZE(ptrba), xvec0;
movss	6*SIZE(ptrbb), xvec1;
mulss	xvec0, xvec1;
addss	xvec1, xvec15;

movss	7*SIZE(ptrbb), xvec2;
mulss	xvec0, xvec2;
addss	xvec2, xvec14;

addq	$4*SIZE, ptrba;
addq	$8*SIZE, ptrbb;
decq	k;
jg	.L341_bodyB;
ALIGN_4
.L341_loopE:
#ifndef	TRMMKERNEL
TEST $2, bk;
#else
TEST $2, kkk;
#endif
JLE .L342_loopE;
ALIGN_4
.L342_bodyB:
movss	0*SIZE(ptrba), xvec0;
movss	0*SIZE(ptrbb), xvec1;
mulss	xvec0, xvec1;
addss	xvec1, xvec15;

movss	1*SIZE(ptrbb), xvec2;
mulss	xvec0, xvec2;
addss	xvec2, xvec14;

movss	1*SIZE(ptrba), xvec0;
movss	2*SIZE(ptrbb), xvec1;
mulss	xvec0, xvec1;
addss	xvec1, xvec15;

movss	3*SIZE(ptrbb), xvec2;
mulss	xvec0, xvec2;
addss	xvec2, xvec14;
addq	$2*SIZE, ptrba;
addq	$4*SIZE, ptrbb;
.L342_loopE:
#ifndef	TRMMKERNEL
TEST $1, bk;
#else
TEST $1, kkk;
#endif
JLE .L343_loopE;
ALIGN_4
.L343_bodyB:
movss	0*SIZE(ptrba), xvec0;
movss	0*SIZE(ptrbb), xvec1;
mulss	xvec0, xvec1;
addss	xvec1, xvec15;

movss	1*SIZE(ptrbb), xvec2;
mulss	xvec0, xvec2;
addss	xvec2, xvec14;
addq	$1*SIZE, ptrba;
addq	$2*SIZE, ptrbb

.L343_loopE:
#### Writing back ####
movss	MEMALPHA, xvec7;
mulss	xvec7, xvec15;
mulss	xvec7, xvec14;
movss	0*SIZE(C0), xvec0;
movss	0*SIZE(C1), xvec1;
#ifndef	TRMMKERNEL
addss	xvec0, xvec15;
addss	xvec1, xvec14;
#endif
movss	xvec15, 0*SIZE(C0);
movss	xvec14, 0*SIZE(C1);
#if (defined(TRMMKERNEL)&&defined(LEFT)&&defined(TRANSA))||(defined(TRMMKERNEL)&&!defined(LEFT)&&!defined(TRANSA))
MOVQ	bk, %rax;
SUBQ	kkk, %rax;
LEAQ	(,%rax, SIZE), %rax;
ADDQ	%rax, ptrba;
LEAQ	(ptrbb, %rax, 2), ptrbb;
#endif
#if defined(TRMMKERNEL) && defined(LEFT)
ADDQ	$1, kk;
#endif
addq	$1*SIZE, C0;
addq	$1*SIZE, C1;
.L34_loopE:
#if defined(TRMMKERNEL) && !defined(LEFT)
ADDQ	$2, kk;
#endif
MOVQ bk, k;
SALQ $3, k;
ADDQ k, bb;
LEAQ (C, ldc, 2), C;
.L30_loopE:
TEST $1, bn;
JLE .L40_loopE;
ALIGN_4
.L40_bodyB:
#if defined(TRMMKERNEL)&&defined(LEFT)
MOVQ	OFFSET, %rax;
MOVQ	%rax, kk;
#endif
MOVQ C, C0;
MOVQ ba, ptrba;
MOVQ bm, i;
SARQ $3, i;
JLE .L41_loopE;
ALIGN_4
.L41_bodyB:
#if !defined(TRMMKERNEL)||(defined(TRMMKERNEL)&&defined(LEFT)&&defined(TRANSA))||(defined(TRMMKERNEL)&&!defined(LEFT)&&!defined(TRANSA))
MOVQ bb, ptrbb;
#else
MOVQ bb, ptrbb;
MOVQ kk, %rax
LEAQ (, %rax, SIZE), %rax;
LEAQ (ptrba, %rax, 8), ptrba;
ADDQ	%rax, ptrbb;
#endif
#### initial ####
XOR_SY	yvec15, yvec15, yvec15;
#ifndef	TRMMKERNEL
MOVQ bk, k;
#elif (defined(LEFT)&&!defined(TRANSA))||(!defined(LEFT)&&defined(TRANSA))
MOVQ bk, %rax;
SUBQ kk, %rax;
MOVQ %rax, kkk;
#else
MOVQ kk, %rax;
#ifdef	LEFT
ADDQ $8, %rax;
#else
ADDQ $1, %rax;
#endif
MOVQ %rax, kkk;
#endif
SARQ $2, k;
JLE .L411_loopE;
ALIGN_4
.L411_bodyB:
LD_SY		0*SIZE(ptrba), yvec0;
BROAD_SY	0*SIZE(ptrbb), yvec1;
MUL_SY	yvec0, yvec1, yvec2;
ADD_SY	yvec2, yvec15, yvec15;

LD_SY		8*SIZE(ptrba), yvec0;
BROAD_SY	1*SIZE(ptrbb), yvec1;
MUL_SY	yvec0, yvec1, yvec2;
ADD_SY	yvec2, yvec15, yvec15;

LD_SY		16*SIZE(ptrba), yvec0;
BROAD_SY	2*SIZE(ptrbb), yvec1;
MUL_SY	yvec0, yvec1, yvec2;
ADD_SY	yvec2, yvec15, yvec15;

LD_SY		24*SIZE(ptrba), yvec0;
BROAD_SY	3*SIZE(ptrbb), yvec1;
MUL_SY	yvec0, yvec1, yvec2;
ADD_SY	yvec2, yvec15, yvec15;

ADDQ	$32*SIZE, ptrba;
ADDQ	$4*SIZE, ptrbb;
DECQ	k;
JG .L411_bodyB;
ALIGN_4
.L411_loopE:
#ifndef	TRMMKERNEL
TEST $2, bk;
#else
TEST $2, kkk;
#endif
JLE .L412_loopE;
ALIGN_4
.L412_bodyB:
LD_SY		0*SIZE(ptrba), yvec0;
BROAD_SY	0*SIZE(ptrbb), yvec1;
MUL_SY	yvec0, yvec1, yvec2;
ADD_SY	yvec2, yvec15, yvec15;

LD_SY		8*SIZE(ptrba), yvec0;
BROAD_SY	1*SIZE(ptrbb), yvec1;
MUL_SY	yvec0, yvec1, yvec2;
ADD_SY	yvec2, yvec15, yvec15;

ADDQ	$16*SIZE, ptrba;
ADDQ	$2*SIZE, ptrbb;
.L412_loopE:
#ifndef	TRMMKERNEL
TEST $1, bk;
#else
TEST $1, kkk;
#endif
JLE .L413_loopE;
ALIGN_4
.L413_bodyB:
LD_SY		0*SIZE(ptrba), yvec0;
BROAD_SY	0*SIZE(ptrbb), yvec1;
MUL_SY	yvec0, yvec1, yvec2;
ADD_SY	yvec2, yvec15, yvec15;
ADDQ	$8*SIZE, ptrba;
ADDQ	$1*SIZE, ptrbb;
.L413_loopE:
#### Writing ####
BROAD_SY MEMALPHA, yvec7;
MUL_SY	yvec7, yvec15, yvec15;
EXTRA_SY $1, yvec15, xvec14;
SHUF_SX	$0x44, xvec15, xvec13;
SHUF_SX	$0xee, xvec15, xvec12;
SHUF_SX	$0x44, xvec14, xvec11;
SHUF_SX	$0xee, xvec14, xvec10;
#ifndef	TRMMKERNEL
LDL_SX 0*SIZE(C0), xvec0, xvec0;
LDL_SX 2*SIZE(C0), xvec1, xvec1;
LDL_SX 4*SIZE(C0), xvec2, xvec2;
LDL_SX 6*SIZE(C0), xvec3, xvec3;
ADD_SX xvec0, xvec13, xvec13;
ADD_SX xvec1, xvec12, xvec12;
ADD_SX xvec2, xvec11, xvec11;
ADD_SX xvec3, xvec10, xvec10;
#endif
STL_SX	xvec13, 0*SIZE(C0);
STL_SX	xvec12, 2*SIZE(C0);
STL_SX	xvec11, 4*SIZE(C0);
STL_SX	xvec10, 6*SIZE(C0);
#if (defined(TRMMKERNEL)&&defined(LEFT)&&defined(TRANSA))||(defined(TRMMKERNEL)&&!defined(LEFT)&&!defined(TRANSA))
MOVQ bk, %rax;
SUBQ kkk, %rax;
LEAQ (,%rax, SIZE), %rax;
LEAQ (ptrba, %rax, 8), ptrba;
ADDQ %rax, ptrbb;
#endif
#if defined(TRMMKERNEL)&&defined(LEFT)
ADDQ	$8, kk;
#endif
ADDQ	$8*SIZE, C0;
DECQ	i;
JG	.L41_bodyB;
ALIGN_4
.L41_loopE:
TEST $4, bm;
JLE .L42_loopE;
ALIGN_4
.L42_bodyB:
#if !defined(TRMMKERNEL)||(defined(TRMMKERNEL)&&defined(LEFT)&&defined(TRANSA))||(defined(TRMMKERNEL)&&!defined(LEFT)&&!defined(TRANSA))
MOVQ bb, ptrbb;
#else
MOVQ bb, ptrbb;
MOVQ kk, %rax;
LEAQ (,%rax, SIZE), %rax;
LEAQ (ptrba, %rax, 4), ptrba;
ADDQ	%rax, ptrbb;
#endif
XOR_SY	yvec15, yvec15, yvec15;
#ifndef	TRMMKERNEL
MOVQ bk, k;
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
MOVQ bk, %rax;
SUBQ kk, %rax;
MOVQ %rax, kkk;
#else
MOVQ kk, %rax;
#ifdef	LEFT
ADDQ $4, %rax;
#else
ADDQ $1, %rax;
#endif
MOVQ %rax, kkk
#endif
SARQ $2, k;
JLE .L421_loopE;
ALIGN_4
.L421_bodyB:
LD_SX	0*SIZE(ptrba), xvec0;
BROAD_SX 0*SIZE(ptrbb), xvec1;
MUL_SX xvec0, xvec1, xvec1;
ADD_SX xvec1, xvec15, xvec15;

LD_SX	4*SIZE(ptrba), xvec0;
BROAD_SX 1*SIZE(ptrbb), xvec1;
MUL_SX xvec0, xvec1, xvec1;
ADD_SX xvec1, xvec15, xvec15;

LD_SX	8*SIZE(ptrba), xvec0;
BROAD_SX 2*SIZE(ptrbb), xvec1;
MUL_SX xvec0, xvec1, xvec1;
ADD_SX xvec1, xvec15, xvec15;

LD_SX	12*SIZE(ptrba), xvec0;
BROAD_SX 3*SIZE(ptrbb), xvec1;
MUL_SX xvec0, xvec1, xvec1;
ADD_SX xvec1, xvec15, xvec15;
ADDQ	$16*SIZE, ptrba;
ADDQ	$4*SIZE, ptrbb;
DECQ 	k;
JG .L421_bodyB;
ALIGN_4
.L421_loopE:
#ifndef	TRMMKERNEL
TEST $2, bk;
#else
TEST $2, kkk;
#endif
JLE .L422_loopE;
ALIGN_4
.L422_bodyB:
LD_SX	0*SIZE(ptrba), xvec0;
BROAD_SX 0*SIZE(ptrbb), xvec1;
MUL_SX xvec0, xvec1, xvec1;
ADD_SX xvec1, xvec15, xvec15;

LD_SX	4*SIZE(ptrba), xvec0;
BROAD_SX 1*SIZE(ptrbb), xvec1;
MUL_SX xvec0, xvec1, xvec1;
ADD_SX xvec1, xvec15, xvec15;

ADDQ	$8*SIZE, ptrba;
ADDQ	$2*SIZE, ptrbb;
.L422_loopE:
#ifndef	TRMMKERNEL
TEST $1, bk;
#else
TEST $1, kkk;
#endif
JLE .L423_loopE;
ALIGN_4
.L423_bodyB:
LD_SX	0*SIZE(ptrba), xvec0;
BROAD_SX 0*SIZE(ptrbb), xvec1;
MUL_SX xvec0, xvec1, xvec1;
ADD_SX xvec1, xvec15, xvec15;
ADDQ	$4*SIZE, ptrba;
ADDQ	$1*SIZE, ptrbb;

.L423_loopE:
#### Writing back ####
BROAD_SX MEMALPHA, xvec7;
MUL_SX xvec7, xvec15, xvec15;
#ifndef	TRMMKERNEL
LDL_SX 0*SIZE(C0), xvec0, xvec0;
LDH_SX 2*SIZE(C0), xvec0, xvec0;
ADD_SX xvec0, xvec15, xvec15;
#endif
STL_SX	xvec15, 0*SIZE(C0);
STH_SX	xvec15, 2*SIZE(C0);
#if (defined(TRMMKERNEL) && defined(LEFT) && defined(TRANSA))||(defined(TRMMKERNEL)&&!defined(LEFT)&&!defined(TRANSA))
MOVQ bk, %rax;
SUBQ kkk, %rax;
LEAQ (, %rax, SIZE), %rax;
LEAQ (ptrba, %rax, 4), ptrba;
ADDQ %rax, ptrbb;
#endif
#if defined(TRMMKERNEL) && defined(LEFT)
ADDQ $4, kk
#endif
ADDQ	$4*SIZE, C0;

.L42_loopE:
TEST $2, bm;
JLE .L43_loopE;
ALIGN_4
.L43_bodyB:
#if !defined(TRMMKERNEL) || (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
MOVQ bb, ptrbb;
#else
MOVQ bb, ptrbb;
MOVQ kk, %rax
LEAQ (, %rax, SIZE), %rax
LEAQ (ptrba, %rax, 2), ptrba
ADDQ %rax, ptrbb;
#endif
XOR_SY	yvec15, yvec15, yvec15;
XOR_SY	yvec14, yvec14, yvec14;
#ifndef	TRMMKERNEL
MOVQ bk, k;
#elif (defined(LEFT)&&!defined(TRANSA))||(!defined(LEFT)&&defined(TRANSA))
MOVQ bk, %rax;
SUBQ kk, %rax;
MOVQ %rax, kkk;
#else
MOVQ kk, %rax;
#ifdef	LEFT
ADDQ $2, %rax;
#else
ADDQ $1, %rax;
#endif
MOVQ %rax, kkk;
#endif
SARQ $2, k;
JLE .L431_loopE;
ALIGN_4
.L431_bodyB:
vmovss	0*SIZE(ptrba), xvec0;
vmovss	1*SIZE(ptrba), xvec1;
vmovss	0*SIZE(ptrbb), xvec2;
vmulss	xvec2, xvec0, xvec0;
vaddss	xvec0, xvec15, xvec15;
vmulss	xvec2, xvec1, xvec1;
vaddss	xvec1, xvec14, xvec14;

vmovss	2*SIZE(ptrba), xvec3;
vmovss	3*SIZE(ptrba), xvec4;
vmovss	1*SIZE(ptrbb), xvec5;
vmulss	xvec5, xvec3, xvec3;
vaddss	xvec3, xvec15, xvec15;
vmulss	xvec5, xvec4, xvec4;
vaddss	xvec4, xvec14, xvec14;

vmovss	4*SIZE(ptrba), xvec0;
vmovss	5*SIZE(ptrba), xvec1;
vmovss	2*SIZE(ptrbb), xvec2;
vmulss	xvec2, xvec0, xvec0;
vaddss	xvec0, xvec15, xvec15;
vmulss	xvec2, xvec1, xvec1;
vaddss	xvec1, xvec14, xvec14;

vmovss	6*SIZE(ptrba), xvec3;
vmovss	7*SIZE(ptrba), xvec4;
vmovss	3*SIZE(ptrbb), xvec5;
vmulss	xvec5, xvec3, xvec3;
vaddss	xvec3, xvec15, xvec15;
vmulss	xvec5, xvec4, xvec4;
vaddss	xvec4, xvec14, xvec14;
addq	$8*SIZE, ptrba;
addq	$4*SIZE, ptrbb;
decq	k;
JG .L431_bodyB;
ALIGN_4
.L431_loopE:
#ifndef	TRMMKERNEL
TEST $2, bk;
#else
TEST $2, kkk;
#endif
JLE .L432_loopE;
ALIGN_4
.L432_bodyB:
vmovss	0*SIZE(ptrba), xvec0;
vmovss	1*SIZE(ptrba), xvec1;
vmovss	0*SIZE(ptrbb), xvec2;
vmulss	xvec2, xvec0, xvec0;
vaddss	xvec0, xvec15, xvec15;
vmulss	xvec2, xvec1, xvec1;
vaddss	xvec1, xvec14, xvec14;

vmovss	2*SIZE(ptrba), xvec3;
vmovss	3*SIZE(ptrba), xvec4;
vmovss	1*SIZE(ptrbb), xvec5;
vmulss	xvec5, xvec3, xvec3;
vaddss	xvec3, xvec15, xvec15;
vmulss	xvec5, xvec4, xvec4;
vaddss	xvec4, xvec14, xvec14;
addq	$4*SIZE, ptrba;
addq	$2*SIZE, ptrbb;

.L432_loopE:
#ifndef	TRMMKERNEL
TEST $1, bk;
#else
TEST $1, kkk;
#endif
JLE .L433_loopE;
ALIGN_4
.L433_bodyB:
vmovss	0*SIZE(ptrba), xvec0;
vmovss	1*SIZE(ptrba), xvec1;
vmovss	0*SIZE(ptrbb), xvec2;
vmulss	xvec2, xvec0, xvec0;
vaddss	xvec0, xvec15, xvec15;
vmulss	xvec2, xvec1, xvec1;
vaddss	xvec1, xvec14, xvec14;
addq	$2*SIZE, ptrba;
addq	$1*SIZE, ptrbb;

.L433_loopE:
#### Writing Back ####
vmovss	MEMALPHA, xvec7;
vmulss	xvec7, xvec15, xvec15;
vmulss	xvec7, xvec14, xvec14;

#ifndef	TRMMKERNEL
vaddss	0*SIZE(C0), xvec15, xvec15;
vaddss	1*SIZE(C0), xvec14, xvec14;
#endif
vmovss	xvec15, 0*SIZE(C0);
vmovss	xvec14, 1*SIZE(C0);
#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) ||(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
MOVQ bk, %rax;
SUBQ kkk, %rax;
LEAQ (,%rax, SIZE), %rax;
LEAQ (ptrba, %rax, 2), ptrba;
ADDQ %rax, ptrbb;
#endif
#if defined(TRMMKERNEL) && defined(LEFT)
addq $2, kk
#endif
addq	$2*SIZE, C0;

.L43_loopE:
TEST $1, bm;
JLE .L44_loopE;
ALIGN_4
.L44_bodyB:
#if !defined(TRMMKERNEL) || (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
MOVQ bb, ptrbb;
#else
MOVQ bb, ptrbb;
MOVQ kk, %rax;
LEAQ (, %rax, SIZE), %rax;
ADDQ %rax, ptrba;
ADDQ %rax, ptrbb;
#endif
XOR_SY	yvec15, yvec15, yvec15;
#ifndef	TRMMKERNEL
MOVQ bk, k;
#elif (defined(LEFT)&& !defined(TRANSA))||(!defined(LEFT)&&defined(TRANSA))
MOVQ bk, %rax;
SUBQ kk, %rax;
MOVQ %rax, kkk;
#else
MOVQ kk, %rax;
#ifdef	LEFT
ADDQ $1, %rax;
#else
ADDQ $1, %rax;
#endif
MOVQ %rax, kkk;
#endif
SARQ $2, k;
JLE .L441_loopE;
ALIGN_4
.L441_bodyB:
vmovss	0*SIZE(ptrba), xvec0;
vmovss	0*SIZE(ptrbb), xvec1;
vmulss	xvec0, xvec1, xvec1;
vaddss	xvec1, xvec15, xvec15;

vmovss	1*SIZE(ptrba), xvec0;
vmovss	1*SIZE(ptrbb), xvec1;
vmulss	xvec0, xvec1, xvec1;
vaddss	xvec1, xvec15, xvec15;

vmovss	2*SIZE(ptrba), xvec0;
vmovss	2*SIZE(ptrbb), xvec1;
vmulss	xvec0, xvec1, xvec1;
vaddss	xvec1, xvec15, xvec15;

vmovss	3*SIZE(ptrba), xvec0;
vmovss	3*SIZE(ptrbb), xvec1;
vmulss	xvec0, xvec1, xvec1;
vaddss	xvec1, xvec15, xvec15;
addq	$4*SIZE, ptrba;
addq	$4*SIZE, ptrbb;
decq	k;
JG .L441_bodyB;
ALIGN_4
.L441_loopE:
#ifndef	TRMMKERNEL
TEST $2, bk;
#else
TEST $2, kkk;
#endif
JLE .L442_loopE;
ALIGN_4
.L442_bodyB:
vmovss	0*SIZE(ptrba), xvec0;
vmovss	0*SIZE(ptrbb), xvec1;
vmulss	xvec0, xvec1, xvec1;
vaddss	xvec1, xvec15, xvec15;

vmovss	1*SIZE(ptrba), xvec0;
vmovss	1*SIZE(ptrbb), xvec1;
vmulss	xvec0, xvec1, xvec1;
vaddss	xvec1, xvec15, xvec15;
addq	$2*SIZE, ptrba;
addq	$2*SIZE, ptrbb;

.L442_loopE:
#ifndef	TRMMKERNEL
TEST $1, bk;
#else
TEST $1, kkk;
#endif
JLE .L443_loopE;
ALIGN_4
.L443_bodyB:
vmovss	0*SIZE(ptrba), xvec0;
vmovss	0*SIZE(ptrbb), xvec1;
vmulss	xvec0, xvec1, xvec1;
vaddss	xvec1, xvec15, xvec15;
addq	$1*SIZE, ptrba;
addq	$1*SIZE, ptrbb;

.L443_loopE:
#### Writing Back ####
vmovss	MEMALPHA, xvec7;
vmulss	xvec7, xvec15, xvec15;
#ifndef	TRMMKERNEL
vaddss	0*SIZE(C0), xvec15, xvec15;
#endif
vmovss	xvec15, 0*SIZE(C0);
#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) ||(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
MOVQ bk, %rax;
SUBQ kkk, %rax;
LEAQ (,%rax, SIZE), %rax;
ADDQ %rax, ptrba;
ADDQ %rax, ptrbb;
#endif
#if defined(TRMMKERNEL) && defined(LEFT)
addq $1, kk
#endif
addq	$1*SIZE, C0;

.L44_loopE:
MOV	bk, k;
SALQ $2, k;
ADDQ k, bb;
ADDQ ldc, C;

.L40_loopE:
movq      0(%rsp), %rbx;
movq      8(%rsp), %rbp;
movq     16(%rsp), %r12;
movq     24(%rsp), %r13;
movq     32(%rsp), %r14;
movq     40(%rsp), %r15;

vzeroupper

#ifdef WINDOWS_ABI
	movq	 48(%rsp), %rdi
	movq	 56(%rsp), %rsi
	movups	 64(%rsp), %xmm6
	movups	 80(%rsp), %xmm7
	movups	 96(%rsp), %xmm8
	movups	112(%rsp), %xmm9
	movups	128(%rsp), %xmm10
	movups	144(%rsp), %xmm11
	movups	160(%rsp), %xmm12
	movups	176(%rsp), %xmm13
	movups	192(%rsp), %xmm14
	movups	208(%rsp), %xmm15
#endif

addq    $STACKSIZE, %rsp;
ret

EPILOGUE
