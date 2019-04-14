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
#define NOP
#define XOR		xorpd
#undef	MOVQ
#define MOVQ	movq

#define XOR_DY	vxorpd
#define XOR_DX	vxorpd

#define LD_DY	vmovapd
#define LD_DX	vmovapd
#define LDL_DX	vmovlpd
#define LDL_DY	vmovlpd
#define LDH_DX	vmovhpd
#define LDH_DY	vmovhpd

#define ST_DY	vmovapd
#define ST_DX	vmovapd
#define STL_DX	vmovlpd
#define STL_DY	vmovlpd
#define STH_DX	vmovhpd
#define STH_DY	vmovhpd

#define EDUP_DY	vmovddup

#define ADD_DY	vaddpd
#define ADD_DX	vaddpd

#define ADD1_DY	vaddpd
#define ADD2_DY	vaddpd
#define ADDSUB_DY	vaddsubpd

#define MUL_DY	vmulpd
#define MUL_DX	vmulpd

#define SHUF_DY	vperm2f128
#define SHUF_DX	vpshufd

#define VPERMILP_DY	vpermilpd

#define BROAD_DY vbroadcastsd
#define BROAD_DX vmovddup

#define MOV_DY vmovapd
#define MOV_DX vmovapd

#define REVS_DY vshufpd
#define REVS_DX	vmovsd

#define EXTRA_DY vextractf128

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
SARQ $2,j;							# Rn = 4
JLE .L0_loopE;
ALIGN_5;
.L0_bodyB:;
#if defined(TRMMKERNEL) && defined(LEFT)
MOVQ OFFSET, %rax;
MOVQ %rax, kk;
#endif

MOVQ C,C0;
LEAQ (C,ldc,2),C1;
MOVQ bk, k;
SALQ $5, k;
LEAQ (bb, k, 1), prebb;
MOVQ ba,ptrba;
MOVQ bm,i;
SARQ $3,i;							# Rm = 8
JLE .L1_loopE;
ALIGN_5;
.L1_bodyB:;
#if !defined(TRMMKERNEL)||(defined(TRMMKERNEL) && defined(LEFT) && defined(TRANSA))||(defined(TRMMKERNEL)&&!defined(LEFT)&&!defined(TRANSA))
MOVQ bb, ptrbb;
#else
MOVQ bb, ptrbb;
MOVQ kk, %rax;
LEAQ (, %rax, SIZE), %rax;
LEAQ (ptrba, %rax, 8), ptrba;
LEAQ (ptrbb, %rax, 4), ptrbb;
#endif
//#### Initial Results Register ####
PREFETCH2	0*SIZE(prebb);
XOR_DY yvec15, yvec15, yvec15;
PREFETCH2	8*SIZE(prebb);
XOR_DY yvec14, yvec14, yvec14;
XOR_DY yvec13, yvec13, yvec13;
ADDQ	$16*SIZE, prebb
XOR_DY yvec12, yvec12, yvec12;
PREFETCH0	3*SIZE(C0)
LD_DY	0*SIZE(ptrbb), yvec2;
PREFETCH0	3*SIZE(C0, ldc, 1)
XOR_DY yvec11, yvec11, yvec11;
PREFETCH0	3*SIZE(C1)
XOR_DY yvec10, yvec10, yvec10;
PREFETCH0	3*SIZE(C1, ldc, 1)
LD_DY	0*SIZE(ptrba), yvec0;
XOR_DY yvec9, yvec9, yvec9;
XOR_DY yvec8, yvec8, yvec8;
VPERMILP_DY	$0x05, yvec2, yvec3;
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
JLE .L2_loopE;
ALIGN_5;
.L2_bodyB:;
# Computing kernel

//#### Unroll times 1 ####
LD_DY	4*SIZE(ptrba), yvec1;
MUL_DY	yvec0, yvec2, yvec6;
SHUF_DY	$0x03, yvec2, yvec2, yvec4;
MUL_DY	yvec0, yvec3, yvec7;
SHUF_DY	$0x03, yvec3, yvec3, yvec5;
ADD_DY	yvec15, yvec6, yvec15;
ADD_DY	yvec13, yvec7, yvec13;

PREFETCH0	64*SIZE(ptrba)
MUL_DY	yvec1, yvec2, yvec6;
LD_DY	4*SIZE(ptrbb), yvec2;
MUL_DY	yvec1, yvec3, yvec7;
VPERMILP_DY	$0x05, yvec2, yvec3;
ADD_DY	yvec14, yvec6, yvec14;
ADD_DY	yvec12, yvec7, yvec12;

MUL_DY	yvec0, yvec4, yvec6;
MUL_DY	yvec0, yvec5, yvec7;
LD_DY	8*SIZE(ptrba), yvec0;
ADD_DY	yvec11, yvec6, yvec11;
ADD_DY	yvec9, yvec7, yvec9;

MUL_DY	yvec1, yvec4, yvec6;
MUL_DY	yvec1, yvec5, yvec7;
ADD_DY	yvec10, yvec6, yvec10;
ADD_DY	yvec8, yvec7, yvec8;

//#### Unroll times 2 ####
LD_DY	12*SIZE(ptrba), yvec1;
MUL_DY	yvec0, yvec2, yvec6;
SHUF_DY	$0x03, yvec2, yvec2, yvec4;
MUL_DY	yvec0, yvec3, yvec7;
SHUF_DY	$0x03, yvec3, yvec3, yvec5;
ADD_DY	yvec15, yvec6, yvec15;
ADD_DY	yvec13, yvec7, yvec13;

PREFETCH0	72*SIZE(ptrba)
MUL_DY	yvec1, yvec2, yvec6;
LD_DY	8*SIZE(ptrbb), yvec2;
MUL_DY	yvec1, yvec3, yvec7;
VPERMILP_DY	$0x05, yvec2, yvec3;
ADD_DY	yvec14, yvec6, yvec14;
ADD_DY	yvec12, yvec7, yvec12;

MUL_DY	yvec0, yvec4, yvec6;
MUL_DY	yvec0, yvec5, yvec7;
LD_DY	16*SIZE(ptrba), yvec0;
ADD_DY	yvec11, yvec6, yvec11;
ADD_DY	yvec9, yvec7, yvec9;

MUL_DY	yvec1, yvec4, yvec6;
MUL_DY	yvec1, yvec5, yvec7;
ADD_DY	yvec10, yvec6, yvec10;
ADD_DY	yvec8, yvec7, yvec8;

//#### Unroll times 3 ####
LD_DY	20*SIZE(ptrba), yvec1;
MUL_DY	yvec0, yvec2, yvec6;
SHUF_DY	$0x03, yvec2, yvec2, yvec4;
MUL_DY	yvec0, yvec3, yvec7;
SHUF_DY	$0x03, yvec3, yvec3, yvec5;
ADD_DY	yvec15, yvec6, yvec15;
ADD_DY	yvec13, yvec7, yvec13;

PREFETCH0	80*SIZE(ptrba)
MUL_DY	yvec1, yvec2, yvec6;
LD_DY	12*SIZE(ptrbb), yvec2;
ADDQ	$16*SIZE, ptrbb;
MUL_DY	yvec1, yvec3, yvec7;
VPERMILP_DY	$0x05, yvec2, yvec3;
ADD_DY	yvec14, yvec6, yvec14;
ADD_DY	yvec12, yvec7, yvec12;

MUL_DY	yvec0, yvec4, yvec6;
MUL_DY	yvec0, yvec5, yvec7;
LD_DY	24*SIZE(ptrba), yvec0;
ADD_DY	yvec11, yvec6, yvec11;
ADD_DY	yvec9, yvec7, yvec9;

MUL_DY	yvec1, yvec4, yvec6;
MUL_DY	yvec1, yvec5, yvec7;
ADD_DY	yvec10, yvec6, yvec10;
ADD_DY	yvec8, yvec7, yvec8;

//#### Unroll times 4 ####
LD_DY	28*SIZE(ptrba), yvec1;
MUL_DY	yvec0, yvec2, yvec6;
SHUF_DY	$0x03, yvec2, yvec2, yvec4;
MUL_DY	yvec0, yvec3, yvec7;
SHUF_DY	$0x03, yvec3, yvec3, yvec5;
ADDQ	$32*SIZE, ptrba;
ADD_DY	yvec15, yvec6, yvec15;
ADD_DY	yvec13, yvec7, yvec13;

PREFETCH0	88*SIZE(ptrba)
MUL_DY	yvec1, yvec2, yvec6;
LD_DY	0*SIZE(ptrbb), yvec2;
MUL_DY	yvec1, yvec3, yvec7;
VPERMILP_DY	$0x05, yvec2, yvec3;
ADD_DY	yvec14, yvec6, yvec14;
ADD_DY	yvec12, yvec7, yvec12;

MUL_DY	yvec0, yvec4, yvec6;
MUL_DY	yvec0, yvec5, yvec7;
LD_DY	0*SIZE(ptrba), yvec0;
ADD_DY	yvec11, yvec6, yvec11;
ADD_DY	yvec9, yvec7, yvec9;

MUL_DY	yvec1, yvec4, yvec6;
MUL_DY	yvec1, yvec5, yvec7;
ADD_DY	yvec10, yvec6, yvec10;
ADD_DY	yvec8, yvec7, yvec8;
.L2_bodyE:;
DECQ k;
JG .L2_bodyB;
ALIGN_5
.L2_loopE:;
PREFETCH2	0*SIZE(prebb);
ADDQ	$8*SIZE, prebb;
#ifndef	TRMMKERNEL
TEST	$2, bk;
#else
MOVQ	kkk, %rax;
TEST 	$2, %rax;
#endif
JLE		.L3_loopE;
ALIGN_5
.L3_bodyB:
//#### Unroll times 1 ####
PREFETCH0	64*SIZE(ptrba)
LD_DY	4*SIZE(ptrba), yvec1;
MUL_DY	yvec0, yvec2, yvec6;
SHUF_DY	$0x03, yvec2, yvec2, yvec4;
MUL_DY	yvec0, yvec3, yvec7;
SHUF_DY	$0x03, yvec3, yvec3, yvec5;
ADD_DY	yvec15, yvec6, yvec15;
ADD_DY	yvec13, yvec7, yvec13;

MUL_DY	yvec1, yvec2, yvec6;
LD_DY	4*SIZE(ptrbb), yvec2;
ADDQ	$8*SIZE, ptrbb;
MUL_DY	yvec1, yvec3, yvec7;
VPERMILP_DY	$0x05, yvec2, yvec3;
ADD_DY	yvec14, yvec6, yvec14;
ADD_DY	yvec12, yvec7, yvec12;

MUL_DY	yvec0, yvec4, yvec6;
MUL_DY	yvec0, yvec5, yvec7;
LD_DY	8*SIZE(ptrba), yvec0;
ADD_DY	yvec11, yvec6, yvec11;
ADD_DY	yvec9, yvec7, yvec9;

MUL_DY	yvec1, yvec4, yvec6;
MUL_DY	yvec1, yvec5, yvec7;
ADD_DY	yvec10, yvec6, yvec10;
ADD_DY	yvec8, yvec7, yvec8;

//#### Unroll times 2 ####
PREFETCH0	72*SIZE(ptrba)
LD_DY	12*SIZE(ptrba), yvec1;
MUL_DY	yvec0, yvec2, yvec6;
SHUF_DY	$0x03, yvec2, yvec2, yvec4;
MUL_DY	yvec0, yvec3, yvec7;
SHUF_DY	$0x03, yvec3, yvec3, yvec5;
ADDQ	$16*SIZE, ptrba;
ADD_DY	yvec15, yvec6, yvec15;
ADD_DY	yvec13, yvec7, yvec13;

MUL_DY	yvec1, yvec2, yvec6;
LD_DY	0*SIZE(ptrbb), yvec2;
MUL_DY	yvec1, yvec3, yvec7;
VPERMILP_DY	$0x05, yvec2, yvec3;
ADD_DY	yvec14, yvec6, yvec14;
ADD_DY	yvec12, yvec7, yvec12;

MUL_DY	yvec0, yvec4, yvec6;
MUL_DY	yvec0, yvec5, yvec7;
LD_DY	0*SIZE(ptrba), yvec0;
ADD_DY	yvec11, yvec6, yvec11;
ADD_DY	yvec9, yvec7, yvec9;

MUL_DY	yvec1, yvec4, yvec6;
MUL_DY	yvec1, yvec5, yvec7;
ADD_DY	yvec10, yvec6, yvec10;
ADD_DY	yvec8, yvec7, yvec8;

.L3_loopE:
PREFETCH2	0*SIZE(prebb);
ADDQ	$8*SIZE, prebb
#ifndef	TRMMKERNEL
TEST	$1, bk;
#else
MOVQ	kkk, %rax;
TEST 	$1, %rax;
#endif
JLE		.L4_loopE;
ALIGN_5
.L4_bodyB:;
//#### Unroll times 1 ####
PREFETCH0	64*SIZE(ptrba)
LD_DY	4*SIZE(ptrba), yvec1;
MUL_DY	yvec0, yvec2, yvec6;
SHUF_DY	$0x03, yvec2, yvec2, yvec4;
MUL_DY	yvec0, yvec3, yvec7;
SHUF_DY	$0x03, yvec3, yvec3, yvec5;
ADDQ	$8*SIZE, ptrba;
ADD_DY	yvec15, yvec6, yvec15;
ADD_DY	yvec13, yvec7, yvec13;

MUL_DY	yvec1, yvec2, yvec6;
MUL_DY	yvec1, yvec3, yvec7;
ADDQ	$4*SIZE, ptrbb;
ADD_DY	yvec14, yvec6, yvec14;
ADD_DY	yvec12, yvec7, yvec12;

MUL_DY	yvec0, yvec4, yvec6;
MUL_DY	yvec0, yvec5, yvec7;
ADD_DY	yvec11, yvec6, yvec11;
ADD_DY	yvec9, yvec7, yvec9;

MUL_DY	yvec1, yvec4, yvec6;
MUL_DY	yvec1, yvec5, yvec7;
ADD_DY	yvec10, yvec6, yvec10;
ADD_DY	yvec8, yvec7, yvec8;

.L4_loopE:;
//#### Load Alpha ####
BROAD_DY MEMALPHA,yvec7;
//#### Multiply Alpha ####
MUL_DY yvec7,yvec15,yvec15;
MUL_DY yvec7,yvec14,yvec14;
MUL_DY yvec7,yvec13,yvec13;
MUL_DY yvec7,yvec12,yvec12;
MUL_DY yvec7,yvec11,yvec11;
MUL_DY yvec7,yvec10,yvec10;
MUL_DY yvec7,yvec9,yvec9;
MUL_DY yvec7,yvec8,yvec8;
//#### Reverse the Results ####
MOV_DY yvec15,yvec7;
REVS_DY $0x0a,yvec13,yvec15,yvec15;
REVS_DY $0x0a,yvec7,yvec13,yvec13;
MOV_DY yvec14,yvec7;
REVS_DY $0x0a,yvec12,yvec14,yvec14;
REVS_DY $0x0a,yvec7,yvec12,yvec12;
MOV_DY yvec11,yvec7;
REVS_DY $0x0a,yvec9,yvec11,yvec11;
REVS_DY $0x0a,yvec7,yvec9,yvec9;
MOV_DY yvec10,yvec7;
REVS_DY $0x0a,yvec8,yvec10,yvec10;
REVS_DY $0x0a,yvec7,yvec8,yvec8;
//#### Testing alignment ####
MOVQ	C0, %rax;
OR		ldc, %rax;
TEST	$15, %rax;
JNE		.L4_loopEx;					# Unalign part write back
ALIGN_5
//#### Writing Back ####
EXTRA_DY $1,yvec15,xvec7;
EXTRA_DY $1,yvec14,xvec6;
EXTRA_DY $1,yvec13,xvec5;
EXTRA_DY $1,yvec12,xvec4;
EXTRA_DY $1,yvec11,xvec3;
EXTRA_DY $1,yvec10,xvec2;
EXTRA_DY $1,yvec9,xvec1;
EXTRA_DY $1,yvec8,xvec0;
#ifndef	TRMMKERNEL
ADD_DY 0*SIZE(C0),xvec15,xvec15;
ADD_DY 2*SIZE(C1),xvec7,xvec7;
ADD_DY 4*SIZE(C0),xvec14,xvec14;
ADD_DY 6*SIZE(C1),xvec6,xvec6;
ADD_DY 0*SIZE(C0,ldc,1),xvec13,xvec13;
ADD_DY 2*SIZE(C1,ldc,1),xvec5,xvec5;
ADD_DY 4*SIZE(C0,ldc,1),xvec12,xvec12;
ADD_DY 6*SIZE(C1,ldc,1),xvec4,xvec4;
ADD_DY 0*SIZE(C1),xvec11,xvec11;
ADD_DY 2*SIZE(C0),xvec3,xvec3;
ADD_DY 4*SIZE(C1),xvec10,xvec10;
ADD_DY 6*SIZE(C0),xvec2,xvec2;
ADD_DY 0*SIZE(C1,ldc,1),xvec9,xvec9;
ADD_DY 2*SIZE(C0,ldc,1),xvec1,xvec1;
ADD_DY 4*SIZE(C1,ldc,1),xvec8,xvec8;
ADD_DY 6*SIZE(C0,ldc,1),xvec0,xvec0;
#endif
ST_DY xvec15, 0*SIZE(C0);
ST_DY xvec7, 2*SIZE(C1);
ST_DY xvec14, 4*SIZE(C0);
ST_DY xvec6, 6*SIZE(C1);
ST_DY xvec13, 0*SIZE(C0,ldc,1);
ST_DY xvec5, 2*SIZE(C1,ldc,1);
ST_DY xvec12, 4*SIZE(C0,ldc,1);
ST_DY xvec4, 6*SIZE(C1,ldc,1);
ST_DY xvec11, 0*SIZE(C1);
ST_DY xvec3, 2*SIZE(C0);
ST_DY xvec10, 4*SIZE(C1);
ST_DY xvec2, 6*SIZE(C0);
ST_DY xvec9, 0*SIZE(C1,ldc,1);
ST_DY xvec1, 2*SIZE(C0,ldc,1);
ST_DY xvec8, 4*SIZE(C1,ldc,1);
ST_DY xvec0, 6*SIZE(C0,ldc,1);
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
ADDQ $8*SIZE,C0;
ADDQ $8*SIZE,C1;
.L1_bodyE:;
DECQ i;
JG .L1_bodyB;
JMP	.L1_loopE;
ALIGN_5;
.L4_loopEx:;
EXTRA_DY $1, yvec15, xvec7;
#ifndef	TRMMKERNEL
LDL_DY	0*SIZE(C0), xvec6, xvec6;
LDH_DY	1*SIZE(C0), xvec6, xvec6;
ADD_DY	xvec6, xvec15, xvec15;
LDL_DY	2*SIZE(C1), xvec5, xvec5;
LDH_DY	3*SIZE(C1), xvec5, xvec5;
ADD_DY	xvec5, xvec7, xvec7;
#endif
STL_DY	xvec15, 0*SIZE(C0);
STH_DY	xvec15, 1*SIZE(C0);
STL_DY	xvec7, 2*SIZE(C1);
STH_DY	xvec7, 3*SIZE(C1);

EXTRA_DY $1, yvec14, xvec4;
#ifndef	TRMMKERNEL
LDL_DY	4*SIZE(C0), xvec3, xvec3;
LDH_DY	5*SIZE(C0), xvec3, xvec3;
ADD_DY	xvec3, xvec14, xvec14;
LDL_DY	6*SIZE(C1), xvec2, xvec2;
LDH_DY	7*SIZE(C1), xvec2, xvec2;
ADD_DY	xvec2, xvec4, xvec4;
#endif
STL_DY	xvec14, 4*SIZE(C0);
STH_DY	xvec14, 5*SIZE(C0);
STL_DY	xvec4, 6*SIZE(C1);
STH_DY	xvec4, 7*SIZE(C1);

EXTRA_DY $1, yvec13, xvec7;
#ifndef	TRMMKERNEL
LDL_DY	0*SIZE(C0, ldc, 1), xvec6, xvec6;
LDH_DY	1*SIZE(C0, ldc, 1), xvec6, xvec6;
ADD_DY	xvec6, xvec13, xvec13;
LDL_DY	2*SIZE(C1, ldc, 1), xvec5, xvec5;
LDH_DY	3*SIZE(C1, ldc, 1), xvec5, xvec5;
ADD_DY	xvec5, xvec7, xvec7;
#endif
STL_DY	xvec13, 0*SIZE(C0, ldc, 1);
STH_DY	xvec13, 1*SIZE(C0, ldc, 1);
STL_DY	xvec7, 2*SIZE(C1, ldc, 1);
STH_DY	xvec7, 3*SIZE(C1, ldc, 1);

EXTRA_DY $1, yvec12, xvec4;
#ifndef	TRMMKERNEL
LDL_DY	4*SIZE(C0, ldc, 1), xvec3, xvec3;
LDH_DY	5*SIZE(C0, ldc, 1), xvec3, xvec3;
ADD_DY	xvec3, xvec12, xvec12;
LDL_DY	6*SIZE(C1, ldc, 1), xvec2, xvec2;
LDH_DY	7*SIZE(C1, ldc, 1), xvec2, xvec2;
ADD_DY	xvec2, xvec4, xvec4;
#endif
STL_DY	xvec12, 4*SIZE(C0, ldc, 1);
STH_DY	xvec12, 5*SIZE(C0, ldc ,1);
STL_DY	xvec4, 6*SIZE(C1, ldc, 1);
STH_DY	xvec4, 7*SIZE(C1, ldc, 1);

EXTRA_DY $1, yvec11, xvec7;
#ifndef	TRMMKERNEL
LDL_DY	0*SIZE(C1), xvec6, xvec6;
LDH_DY	1*SIZE(C1), xvec6, xvec6;
ADD_DY	xvec6, xvec11, xvec11;
LDL_DY	2*SIZE(C0), xvec5, xvec5;
LDH_DY	3*SIZE(C0), xvec5, xvec5;
ADD_DY	xvec5, xvec7, xvec7;
#endif
STL_DY	xvec11, 0*SIZE(C1);
STH_DY	xvec11, 1*SIZE(C1);
STL_DY	xvec7, 2*SIZE(C0);
STH_DY	xvec7, 3*SIZE(C0);

EXTRA_DY $1, yvec10, xvec4;
#ifndef	TRMMKERNEL
LDL_DY	4*SIZE(C1), xvec3, xvec3;
LDH_DY	5*SIZE(C1), xvec3, xvec3;
ADD_DY	xvec3, xvec10, xvec10;
LDL_DY	6*SIZE(C0), xvec2, xvec2;
LDH_DY	7*SIZE(C0), xvec2, xvec2;
ADD_DY	xvec2, xvec4, xvec4;
#endif
STL_DY	xvec10, 4*SIZE(C1);
STH_DY	xvec10, 5*SIZE(C1);
STL_DY	xvec4, 6*SIZE(C0);
STH_DY	xvec4, 7*SIZE(C0);

EXTRA_DY $1, yvec9, xvec7;
#ifndef	TRMMKERNEL
LDL_DY	0*SIZE(C1, ldc, 1), xvec6, xvec6;
LDH_DY	1*SIZE(C1, ldc, 1), xvec6, xvec6;
ADD_DY	xvec6, xvec9, xvec9;
LDL_DY	2*SIZE(C0, ldc, 1), xvec5, xvec5;
LDH_DY	3*SIZE(C0, ldc ,1), xvec5, xvec5;
ADD_DY	xvec5, xvec7, xvec7;
#endif
STL_DY	xvec9, 0*SIZE(C1, ldc, 1);
STH_DY	xvec9, 1*SIZE(C1, ldc, 1);
STL_DY	xvec7, 2*SIZE(C0, ldc, 1);
STH_DY	xvec7, 3*SIZE(C0, ldc, 1);

EXTRA_DY $1, yvec8, xvec4;
#ifndef	TRMMKERNEL
LDL_DY	4*SIZE(C1, ldc, 1), xvec3, xvec3;
LDH_DY	5*SIZE(C1, ldc, 1), xvec3, xvec3;
ADD_DY	xvec3, xvec8, xvec8;
LDL_DY	6*SIZE(C0, ldc, 1), xvec2, xvec2;
LDH_DY	7*SIZE(C0, ldc, 1), xvec2, xvec2;
ADD_DY	xvec2, xvec4, xvec4;
#endif
STL_DY	xvec8, 4*SIZE(C1, ldc, 1);
STH_DY	xvec8, 5*SIZE(C1, ldc, 1);
STL_DY	xvec4, 6*SIZE(C0, ldc, 1);
STH_DY	xvec4, 7*SIZE(C0, ldc, 1);
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
DECQ i;
JG .L1_bodyB;
ALIGN_5
.L1_loopE:;
TEST $4, bm;							# Rm = 4
JLE	.L5_loopE;
ALIGN_5
.L5_bodyB:;
#if !defined(TRMMKERNEL)||(defined(TRMMKERNEL)&&defined(LEFT)&&defined(TRANSA))||(defined(TRMMKERNEL)&&!defined(LEFT)&&!defined(TRANSA))
MOVQ bb, ptrbb;
#else
MOVQ bb, ptrbb;
MOVQ kk, %rax;
LEAQ (, %rax, SIZE), %rax;
LEAQ (ptrba, %rax, 4), ptrba;
LEAQ (ptrbb, %rax, 4), ptrbb;
#endif
//#### Initial Results Register ####
XOR_DY	yvec15, yvec15, yvec15;
XOR_DY 	yvec13, yvec13, yvec13;
LD_DY	0*SIZE(ptrbb), yvec2;
XOR_DY	yvec11, yvec11, yvec11;
XOR_DY	yvec9, yvec9, yvec9;
LD_DY	0*SIZE(ptrba), yvec0;
VPERMILP_DY	$0x05, yvec2, yvec3;
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
JLE .L6_loopE;
ALIGN_5;
.L6_bodyB:;
# Computing kernel

//#### Untoll time 1 ####
LD_DY	4*SIZE(ptrba), yvec1;
MUL_DY	yvec0, yvec2, yvec6;
ADD_DY	yvec15, yvec6, yvec15;
SHUF_DY	$0x03, yvec2, yvec2, yvec4;
MUL_DY	yvec0, yvec3, yvec7;
ADD_DY	yvec13, yvec7, yvec13;
SHUF_DY	$0x03, yvec3, yvec3, yvec5;

LD_DY	4*SIZE(ptrbb), yvec2;
MUL_DY	yvec0, yvec4, yvec6;
ADD_DY	yvec11, yvec6, yvec11;
VPERMILP_DY	$0x05, yvec2, yvec3;
MUL_DY	yvec0, yvec5, yvec7;
ADD_DY	yvec9, yvec7, yvec9;

//#### Untoll time 2 ####
LD_DY	8*SIZE(ptrba), yvec0;
MUL_DY	yvec1, yvec2, yvec6;
ADD_DY	yvec15, yvec6, yvec15;
SHUF_DY	$0x03, yvec2, yvec2, yvec4;
MUL_DY	yvec1, yvec3, yvec7;
ADD_DY	yvec13, yvec7, yvec13;
SHUF_DY	$0x03, yvec3, yvec3, yvec5;

LD_DY	8*SIZE(ptrbb), yvec2;
MUL_DY	yvec1, yvec4, yvec6;
ADD_DY	yvec11, yvec6, yvec11;
VPERMILP_DY	$0x05, yvec2, yvec3;
MUL_DY	yvec1, yvec5, yvec7;
ADD_DY	yvec9, yvec7, yvec9;

//#### Untoll time 3 ####
LD_DY	12*SIZE(ptrba), yvec1;
MUL_DY	yvec0, yvec2, yvec6;
ADD_DY	yvec15, yvec6, yvec15;
SHUF_DY	$0x03, yvec2, yvec2, yvec4;
ADDQ	$16*SIZE, ptrba;
MUL_DY	yvec0, yvec3, yvec7;
ADD_DY	yvec13, yvec7, yvec13;
SHUF_DY	$0x03, yvec3, yvec3, yvec5;

LD_DY	12*SIZE(ptrbb), yvec2;
MUL_DY	yvec0, yvec4, yvec6;
ADD_DY	yvec11, yvec6, yvec11;
VPERMILP_DY	$0x05, yvec2, yvec3;
ADDQ	$16*SIZE, ptrbb;
MUL_DY	yvec0, yvec5, yvec7;
ADD_DY	yvec9, yvec7, yvec9;

//#### Untoll time 4 ####
LD_DY	0*SIZE(ptrba), yvec0;
MUL_DY	yvec1, yvec2, yvec6;
ADD_DY	yvec15, yvec6, yvec15;
SHUF_DY	$0x03, yvec2, yvec2, yvec4;
MUL_DY	yvec1, yvec3, yvec7;
ADD_DY	yvec13, yvec7, yvec13;
SHUF_DY	$0x03, yvec3, yvec3, yvec5;

LD_DY	0*SIZE(ptrbb), yvec2;
MUL_DY	yvec1, yvec4, yvec6;
ADD_DY	yvec11, yvec6, yvec11;
VPERMILP_DY	$0x05, yvec2, yvec3;
MUL_DY	yvec1, yvec5, yvec7;
ADD_DY	yvec9, yvec7, yvec9;
DECQ k;
JG .L6_bodyB;
ALIGN_5
.L6_loopE:;
#ifndef	TRMMKERNEL
TEST $2, bk;
#else
MOVQ kkk, %rax;
TEST $2, %rax;
#endif
JLE .L7_loopE;
ALIGN_5
.L7_bodyB:;
//#### Untoll time 1 ####
LD_DY	4*SIZE(ptrba), yvec1;
MUL_DY	yvec0, yvec2, yvec6;
ADD_DY	yvec15, yvec6, yvec15;
SHUF_DY	$0x03, yvec2, yvec2, yvec4;
ADDQ	$8*SIZE, ptrba;
MUL_DY	yvec0, yvec3, yvec7;
ADD_DY	yvec13, yvec7, yvec13;
SHUF_DY	$0x03, yvec3, yvec3, yvec5;

LD_DY	4*SIZE(ptrbb), yvec2;
MUL_DY	yvec0, yvec4, yvec6;
ADD_DY	yvec11, yvec6, yvec11;
VPERMILP_DY	$0x05, yvec2, yvec3;
ADDQ	$8*SIZE, ptrbb;
MUL_DY	yvec0, yvec5, yvec7;
ADD_DY	yvec9, yvec7, yvec9;

//#### Untoll time 2 ####
LD_DY	0*SIZE(ptrba), yvec0;
MUL_DY	yvec1, yvec2, yvec6;
ADD_DY	yvec15, yvec6, yvec15;
SHUF_DY	$0x03, yvec2, yvec2, yvec4;
MUL_DY	yvec1, yvec3, yvec7;
ADD_DY	yvec13, yvec7, yvec13;
SHUF_DY	$0x03, yvec3, yvec3, yvec5;

LD_DY	0*SIZE(ptrbb), yvec2;
MUL_DY	yvec1, yvec4, yvec6;
ADD_DY	yvec11, yvec6, yvec11;
VPERMILP_DY	$0x05, yvec2, yvec3;
MUL_DY	yvec1, yvec5, yvec7;
ADD_DY	yvec9, yvec7, yvec9;

.L7_loopE:;
#ifndef	TRMMKERNEL
TEST $1, bk
#else
MOVQ kkk, %rax;
TEST $1, %rax;
#endif
JLE .L8_loopE;
ALIGN_5
.L8_bodyB:;
//#### Untoll time 1 ####
MUL_DY	yvec0, yvec2, yvec6;
ADD_DY	yvec15, yvec6, yvec15;
SHUF_DY	$0x03, yvec2, yvec2, yvec4;
ADDQ	$4*SIZE, ptrba;
MUL_DY	yvec0, yvec3, yvec7;
ADD_DY	yvec13, yvec7, yvec13;
SHUF_DY	$0x03, yvec3, yvec3, yvec5;

MUL_DY	yvec0, yvec4, yvec6;
ADD_DY	yvec11, yvec6, yvec11;
ADDQ	$4*SIZE, ptrbb;
MUL_DY	yvec0, yvec5, yvec7;
ADD_DY	yvec9, yvec7, yvec9;

.L8_loopE:;
//#### Load Alpha ####
BROAD_DY	MEMALPHA, yvec7;
//#### Multiply Alpha ####
MUL_DY yvec7,yvec15,yvec15;
MUL_DY yvec7,yvec13,yvec13;
MUL_DY yvec7,yvec11,yvec11;
MUL_DY yvec7,yvec9,yvec9;
//#### Reverse the Results ####
MOV_DY	yvec15, yvec7;
REVS_DY $0x0a,yvec13,yvec15,yvec15;
REVS_DY $0x0a,yvec7,yvec13,yvec13;
MOV_DY yvec11,yvec7;
REVS_DY $0x0a,yvec9,yvec11,yvec11;
REVS_DY $0x0a,yvec7,yvec9,yvec9;
//#### Testing alignment ####
MOVQ	C0, %rax;
OR		ldc, %rax;
TEST	$15, %rax;
JNE		.L8_loopEx;					# Unalign part write back
ALIGN_5
//#### Writing Back ####
EXTRA_DY $1,yvec15,xvec7;
EXTRA_DY $1,yvec13,xvec5;
EXTRA_DY $1,yvec11,xvec3;
EXTRA_DY $1,yvec9,xvec1;
#ifndef	TRMMKERNEL
ADD_DX 0*SIZE(C0), xvec15, xvec15;
ADD_DX 2*SIZE(C1), xvec7, xvec7;
ADD_DX 0*SIZE(C0, ldc, 1), xvec13, xvec13;
ADD_DX 2*SIZE(C1, ldc, 1), xvec5, xvec5;
ADD_DX 0*SIZE(C1), xvec11, xvec11;
ADD_DX 2*SIZE(C0), xvec3, xvec3;
ADD_DX 0*SIZE(C1, ldc, 1), xvec9, xvec9;
ADD_DX 2*SIZE(C0, ldc, 1), xvec1, xvec1;
#endif
ST_DX xvec15, 0*SIZE(C0);
ST_DX xvec7, 2*SIZE(C1);
ST_DX xvec13, 0*SIZE(C0,ldc,1);
ST_DX xvec5, 2*SIZE(C1,ldc,1);
ST_DX xvec11, 0*SIZE(C1);
ST_DX xvec3, 2*SIZE(C0);
ST_DX xvec9, 0*SIZE(C1,ldc,1);
ST_DX xvec1, 2*SIZE(C0,ldc,1);
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
ADDQ $4*SIZE, C0;
ADDQ $4*SIZE, C1;
JMP	.L5_loopE;
ALIGN_5
.L8_loopEx:;
EXTRA_DY $1,yvec15,xvec7;
EXTRA_DY $1,yvec13,xvec5;
EXTRA_DY $1,yvec11,xvec3;
EXTRA_DY $1,yvec9,xvec1;
#ifndef	TRMMKERNEL
LDL_DX 0*SIZE(C0), xvec14, xvec14;
LDH_DX 1*SIZE(C0), xvec14, xvec14;
LDL_DX 0*SIZE(C0, ldc, 1), xvec12, xvec12;
LDH_DX 1*SIZE(C0, ldc, 1), xvec12, xvec12;
LDL_DX 0*SIZE(C1), xvec10, xvec10;
LDH_DX 1*SIZE(C1), xvec10, xvec10;
LDL_DX 0*SIZE(C1, ldc, 1), xvec8, xvec8;
LDH_DX 1*SIZE(C1, ldc, 1), xvec8, xvec8;
ADD_DX xvec14, xvec15, xvec15;
ADD_DX xvec12, xvec13, xvec13;
ADD_DX xvec10, xvec11, xvec11;
ADD_DX xvec8, xvec9, xvec9;
#endif
STL_DX	xvec15, 0*SIZE(C0);
STH_DX	xvec15, 1*SIZE(C0);
STL_DX	xvec13, 0*SIZE(C0, ldc, 1);
STH_DX	xvec13, 1*SIZE(C0, ldc, 1);
STL_DX	xvec11, 0*SIZE(C1);
STH_DX	xvec11, 1*SIZE(C1);
STL_DX	xvec9,	0*SIZE(C1, ldc, 1);
STH_DX	xvec9, 	1*SIZE(C1, ldc, 1);
#ifndef	TRMMKERNEL
LDL_DX 2*SIZE(C0), xvec0, xvec0;
LDH_DX 3*SIZE(C0), xvec0, xvec0;
LDL_DX 2*SIZE(C0, ldc, 1), xvec2, xvec2;
LDH_DX 3*SIZE(C0, ldc, 1), xvec2, xvec2;
LDL_DX 2*SIZE(C1), xvec4, xvec4;
LDH_DX 3*SIZE(C1), xvec4, xvec4;
LDL_DX 2*SIZE(C1, ldc, 1), xvec6, xvec6;
LDH_DX 3*SIZE(C1, ldc, 1), xvec6, xvec6;
ADD_DX xvec0, xvec3, xvec3;
ADD_DX xvec2, xvec1, xvec1;
ADD_DX xvec4, xvec7, xvec7;
ADD_DX xvec6, xvec5, xvec5;
#endif
STL_DX	xvec3, 2*SIZE(C0);
STH_DX	xvec3, 3*SIZE(C0);
STL_DX	xvec1, 2*SIZE(C0, ldc, 1);
STH_DX	xvec1, 3*SIZE(C0, ldc, 1);
STL_DX	xvec7, 2*SIZE(C1);
STH_DX	xvec7, 3*SIZE(C1);
STL_DX	xvec5, 2*SIZE(C1, ldc, 1);
STH_DX	xvec5, 3*SIZE(C1, ldc, 1);
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
.L5_loopE:;
TEST $2, bm;
JLE .L9_loopE;
ALIGN_5
.L9_bodyB:;
#if	!defined(TRMMKERNEL)||(defined(TRMMKERNEL)&&defined(LEFT)&&defined(TRANSA))||(defined(TRMMKERNEL)&&!defined(LEFT)&&!defined(TRANSA))
MOVQ bb, ptrbb;
#else
MOVQ bb, ptrbb;
MOVQ kk, %rax;
LEAQ (, %rax, SIZE), %rax;
LEAQ (ptrba, %rax, 2), ptrba;
LEAQ (ptrbb, %rax, 4), ptrbb
#endif
//#### Initial Results Register ####
LD_DX	0*SIZE(ptrbb), xvec2;
XOR_DY yvec15, yvec15, yvec15;
LD_DX	2*SIZE(ptrbb), xvec3;
XOR_DY yvec13, yvec13, yvec13;
LD_DX	0*SIZE(ptrba), xvec0;
XOR_DY yvec11, yvec11, yvec11;
SHUF_DX	$0x4e, xvec2, xvec4;
XOR_DY yvec9, yvec9, yvec9;
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
JLE .L10_loopE;
ALIGN_5;
.L10_bodyB:;
# Computing kernel

//#### Unroll time 1 ####
LD_DX	4*SIZE(ptrbb), xvec6;
SHUF_DX	$0x4e, xvec3, xvec5;
MUL_DX xvec0, xvec2, xvec2;
ADD_DX xvec2, xvec15, xvec15;

LD_DX	6*SIZE(ptrbb), xvec7;
MUL_DX xvec0, xvec3, xvec3;
ADD_DX xvec3, xvec11, xvec11;

LD_DX	2*SIZE(ptrba), xvec1;
MUL_DX xvec0, xvec4, xvec4;
ADD_DX xvec4, xvec13, xvec13;
SHUF_DX	$0x4e, xvec6, xvec4;
MUL_DX xvec0, xvec5, xvec5;
ADD_DX xvec5, xvec9, xvec9;

//#### Unroll time 2 ####
LD_DX	8*SIZE(ptrbb), xvec2;
SHUF_DX	$0x4e, xvec7, xvec5;
MUL_DX xvec1, xvec6, xvec6;
ADD_DX xvec6, xvec15, xvec15;

LD_DX	10*SIZE(ptrbb), xvec3;
MUL_DX xvec1, xvec7, xvec7;
ADD_DX xvec7, xvec11, xvec11;

LD_DX	4*SIZE(ptrba), xvec0;
MUL_DX xvec1, xvec4, xvec4;
ADD_DX xvec4, xvec13, xvec13;
SHUF_DX	$0x4e, xvec2, xvec4;
MUL_DX xvec1, xvec5, xvec5;
ADD_DX xvec5, xvec9, xvec9;

//#### Unroll time 3 ####
LD_DX	12*SIZE(ptrbb), xvec6;
SHUF_DX	$0x4e, xvec3, xvec5;
MUL_DX xvec0, xvec2, xvec2;
ADD_DX xvec2, xvec15, xvec15;

LD_DX	14*SIZE(ptrbb), xvec7;
MUL_DX xvec0, xvec3, xvec3;
ADD_DX xvec3, xvec11, xvec11;
ADDQ	$16*SIZE, ptrbb;

LD_DX	6*SIZE(ptrba), xvec1;
MUL_DX xvec0, xvec4, xvec4;
ADD_DX xvec4, xvec13, xvec13;
SHUF_DX	$0x4e, xvec6, xvec4;
ADDQ	$8*SIZE, ptrba;
MUL_DX xvec0, xvec5, xvec5;
ADD_DX xvec5, xvec9, xvec9;

//#### Unroll time 4 ####
LD_DX	0*SIZE(ptrbb), xvec2;
SHUF_DX	$0x4e, xvec7, xvec5;
MUL_DX xvec1, xvec6, xvec6;
ADD_DX xvec6, xvec15, xvec15;

LD_DX	2*SIZE(ptrbb), xvec3;
MUL_DX xvec1, xvec7, xvec7;
ADD_DX xvec7, xvec11, xvec11;

LD_DX	0*SIZE(ptrba), xvec0;
MUL_DX xvec1, xvec4, xvec4;
ADD_DX xvec4, xvec13, xvec13;
SHUF_DX	$0x4e, xvec2, xvec4;
MUL_DX xvec1, xvec5, xvec5;
ADD_DX xvec5, xvec9, xvec9;
DECQ k;
JG .L10_bodyB;
ALIGN_5
.L10_loopE:;
#ifndef	TRMMKERNEL
TEST $2, bk
#else
MOVQ kkk, %rax;
TEST $2, %rax;
#endif
JLE .L11_loopE;
ALIGN_5
.L11_bodyB:;
//#### Unroll time 1 ####
LD_DX	4*SIZE(ptrbb), xvec6;
SHUF_DX	$0x4e, xvec3, xvec5;
MUL_DX xvec0, xvec2, xvec2;
ADD_DX xvec2, xvec15, xvec15;

LD_DX	6*SIZE(ptrbb), xvec7;
MUL_DX xvec0, xvec3, xvec3;
ADD_DX xvec3, xvec11, xvec11;
ADDQ	$8*SIZE, ptrbb;

LD_DX	2*SIZE(ptrba), xvec1;
MUL_DX xvec0, xvec4, xvec4;
ADD_DX xvec4, xvec13, xvec13;
SHUF_DX	$0x4e, xvec6, xvec4;
ADDQ	$4*SIZE, ptrba;

MUL_DX xvec0, xvec5, xvec5;
ADD_DX xvec5, xvec9, xvec9;

//#### Unroll time 2 ####
LD_DX	0*SIZE(ptrbb), xvec2;
SHUF_DX	$0x4e, xvec7, xvec5;
MUL_DX xvec1, xvec6, xvec6;
ADD_DX xvec6, xvec15, xvec15;

LD_DX	2*SIZE(ptrbb), xvec3;
MUL_DX xvec1, xvec7, xvec7;
ADD_DX xvec7, xvec11, xvec11;

LD_DX	0*SIZE(ptrba), xvec0;
MUL_DX xvec1, xvec4, xvec4;
ADD_DX xvec4, xvec13, xvec13;
SHUF_DX	$0x4e, xvec2, xvec4;
MUL_DX xvec1, xvec5, xvec5;
ADD_DX xvec5, xvec9, xvec9;

.L11_loopE:;
#ifndef	TRMMKERNEL
TEST $1, bk
#else
MOVQ kkk, %rax;
TEST $1, %rax;
#endif
JLE .L12_loopE;
ALIGN_5
.L12_bodyB:;
SHUF_DX	$0x4e, xvec3, xvec5;
MUL_DX xvec0, xvec2, xvec2;
ADD_DX xvec2, xvec15, xvec15;
ADDQ	$4*SIZE, ptrbb;

MUL_DX xvec0, xvec3, xvec3;
ADD_DX xvec3, xvec11, xvec11;
ADDQ	$2*SIZE, ptrba;

MUL_DX xvec0, xvec4, xvec4;
ADD_DX xvec4, xvec13, xvec13;

MUL_DX xvec0, xvec5, xvec5;
ADD_DX xvec5, xvec9, xvec9;

.L12_loopE:;
//#### Load Alpha ####
BROAD_DX MEMALPHA, xvec7;
//#### Multiply Alpha ####
MUL_DX xvec7, xvec15, xvec15;
MUL_DX xvec7, xvec13, xvec13;
MUL_DX xvec7, xvec11, xvec11;
MUL_DX xvec7, xvec9, xvec9;
//#### Reverse the Results ####
MOV_DX	xvec15, xvec6;
REVS_DX xvec13, xvec15, xvec15;
REVS_DX xvec6, xvec13, xvec13;
MOV_DX	xvec11, xvec6;
REVS_DX xvec9, xvec11, xvec11;
REVS_DX xvec6, xvec9, xvec9;
//#### Testing Alignment ####
MOVQ	C0, %rax;
OR		ldc, %rax;
TEST	$15, %rax;
JNE		.L12_loopEx;
ALIGN_5
//#### Writing Back ####
#ifndef	TRMMKERNEL
ADD_DX 0*SIZE(C0), xvec13, xvec13;
ADD_DX 0*SIZE(C0, ldc, 1), xvec15, xvec15;
ADD_DX 0*SIZE(C1), xvec9, xvec9;
ADD_DX 0*SIZE(C1, ldc, 1), xvec11, xvec11;
#endif
ST_DX	xvec13, 0*SIZE(C0);
ST_DX	xvec15, 0*SIZE(C0, ldc, 1);
ST_DX	xvec9, 0*SIZE(C1);
ST_DX	xvec11, 0*SIZE(C1, ldc, 1);
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
ADDQ	$2*SIZE, C0
ADDQ	$2*SIZE, C1
JMP		.L9_loopE;
ALIGN_5
.L12_loopEx:
#ifndef	TRMMKERNEL
LDL_DX 0*SIZE(C0), xvec14, xvec14;
LDH_DX 1*SIZE(C0), xvec14, xvec14;
LDL_DX 0*SIZE(C0, ldc, 1), xvec12, xvec12;
LDH_DX 1*SIZE(C0, ldc, 1), xvec12, xvec12;
LDL_DX 0*SIZE(C1), xvec10, xvec10;
LDH_DX 1*SIZE(C1), xvec10, xvec10;
LDL_DX 0*SIZE(C1, ldc, 1), xvec8, xvec8;
LDH_DX 1*SIZE(C1, ldc, 1), xvec8, xvec8;
ADD_DX xvec14, xvec13, xvec13;
ADD_DX xvec12, xvec15, xvec15;
ADD_DX xvec10, xvec9, xvec9;
ADD_DX xvec8, xvec11, xvec11;
#endif
STL_DX	xvec13, 0*SIZE(C0);
STH_DX	xvec13, 1*SIZE(C0);
STL_DX	xvec15, 0*SIZE(C0, ldc, 1);
STH_DX	xvec15, 1*SIZE(C0, ldc, 1);
STL_DX	xvec9,	0*SIZE(C1);
STH_DX	xvec9,	1*SIZE(C1);
STL_DX	xvec11,	0*SIZE(C1, ldc, 1);
STH_DX	xvec11,	1*SIZE(C1, ldc, 1);
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
.L9_loopE:;
TEST $1, bm
JLE	.L13_loopE;
ALIGN_5
.L13_bodyB:;
#if !defined(TRMMKERNEL)||(defined(TRMMKERNEL)&&defined(LEFT)&&defined(TRANSA))||(defined(TRMMKERNEL)&&!defined(LEFT)&&!defined(TRANSA))
MOVQ bb, ptrbb;
#else
MOVQ bb, ptrbb;
MOVQ kk, %rax;
LEAQ (,%rax, SIZE), %rax;
ADDQ	%rax, ptrba;
LEAQ (ptrbb, %rax, 4), ptrbb;
#endif
//#### Initial Results Register ####
XOR_DY	yvec15, yvec15, yvec15;
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
JLE .L14_loopE;
ALIGN_5
.L14_bodyB:;
BROAD_DY 0*SIZE(ptrba), yvec0;
LD_DY	0*SIZE(ptrbb), yvec2;
MUL_DY	yvec0, yvec2, yvec6;
ADD_DY	yvec15, yvec6, yvec15;

BROAD_DY 1*SIZE(ptrba), yvec1;
LD_DY	4*SIZE(ptrbb), yvec3;
MUL_DY	yvec1, yvec3, yvec7;
ADD_DY	yvec15, yvec7, yvec15;

BROAD_DY 2*SIZE(ptrba), yvec0;
LD_DY	8*SIZE(ptrbb), yvec2;
MUL_DY	yvec0, yvec2, yvec6;
ADD_DY	yvec15, yvec6, yvec15;

BROAD_DY 3*SIZE(ptrba), yvec1;
LD_DY	12*SIZE(ptrbb), yvec3;
MUL_DY	yvec1, yvec3, yvec7;
ADD_DY	yvec15, yvec7, yvec15;
ADDQ	$4*SIZE, ptrba;
ADDQ	$16*SIZE, ptrbb;
DECQ	k;
JG	.L14_bodyB;
ALIGN_5
.L14_loopE:
#ifndef	TRMMKERNEL
TEST $2, bk;
#else
MOVQ kkk, %rax;
TEST $2, %rax;
#endif
JLE .L15_loopE;
ALIGN_5
.L15_bodyB:
BROAD_DY 0*SIZE(ptrba), yvec0;
LD_DY	0*SIZE(ptrbb), yvec2;
MUL_DY	yvec0, yvec2, yvec6;
ADD_DY	yvec15, yvec6, yvec15;

BROAD_DY 1*SIZE(ptrba), yvec1;
LD_DY	4*SIZE(ptrbb), yvec3;
MUL_DY	yvec1, yvec3, yvec7;
ADD_DY	yvec15, yvec7, yvec15;
ADDQ	$2*SIZE, ptrba;
ADDQ	$8*SIZE, ptrbb;
.L15_loopE:;
#ifndef	TRMMKERNEL
TEST $1, bk;
#else
MOVQ kkk, %rax;
TEST $1, %rax;
#endif
JLE .L16_loopE;
ALIGN_5
.L16_bodyB:;
BROAD_DY 0*SIZE(ptrba), yvec0;
LD_DY	0*SIZE(ptrbb), yvec2;
MUL_DY	yvec0, yvec2, yvec6;
ADD_DY	yvec15, yvec6, yvec15;
ADDQ	$1*SIZE, ptrba;
ADDQ	$4*SIZE, ptrbb;

.L16_loopE:
//#### Load Alpha ####
BROAD_DY MEMALPHA, yvec7;
//#### Multiply Alpha ####
MUL_DY	yvec15, yvec7, yvec15;
//#### Writing Back ####
EXTRA_DY $1, yvec15, xvec7;
#ifndef	TRMMKERNEL
LDL_DX 0*SIZE(C0), xvec0, xvec0;
LDH_DX 0*SIZE(C0, ldc, 1), xvec0, xvec0;
LDL_DX 0*SIZE(C1), xvec1, xvec1;
LDH_DX 0*SIZE(C1, ldc, 1), xvec1, xvec1;
ADD_DX xvec0, xvec15, xvec15;
ADD_DX xvec1, xvec7, xvec7;
#endif
STL_DX	xvec15, 0*SIZE(C0);
STH_DX	xvec15, 0*SIZE(C0, ldc, 1);
STL_DX	xvec7, 0*SIZE(C1);
STH_DX	xvec7, 0*SIZE(C1, ldc, 1);
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
ADDQ	$1*SIZE, C0
ADDQ	$1*SIZE, C1
.L13_loopE:;
#if	defined(TRMMKERNEL)&&!defined(LEFT)
ADDQ	$4, kk
#endif
MOVQ bk,k;
SALQ $5,k;
ADDQ k,bb;
LEAQ (C,ldc,4),C;
.L0_bodyE:;
DECQ j;
JG .L0_bodyB;
ALIGN_5;
.L0_loopE:;
TEST $2, bn;
JLE .L20_loopE;
ALIGN_5;
.L20_loopB:;
#if defined(TRMMKERNEL) && defined(LEFT)
MOVQ	OFFSET, %rax;
MOVQ	%rax, kk
#endif
MOVQ C, C0;
LEAQ (C, ldc, 1), C1;
MOVQ ba, ptrba;
MOVQ bm, i;
SARQ $3, i;					# Rm = 8
JLE	.L21_loopE;
ALIGN_5;
.L21_bodyB:;
#if !defined(TRMMKERNEL)||(defined(TRMMKERNEL)&&defined(LEFT)&&defined(TRANSA))||(defined(TRMMKERNEL)&&!defined(LEFT)&&!defined(TRANSA))
MOVQ bb, ptrbb;
#else
MOVQ bb, ptrbb;
MOVQ kk, %rax;
LEAQ (, %rax, SIZE), %rax;
LEAQ (ptrba, %rax, 8), ptrba;
LEAQ (ptrbb, %rax, 2), ptrbb;
#endif
//#### Initial Results Register ####
XOR_DY yvec15, yvec15, yvec15;
XOR_DY yvec14, yvec14, yvec14;
XOR_DY yvec13, yvec13, yvec13;
XOR_DY yvec12, yvec12, yvec12;
XOR_DY yvec11, yvec11, yvec11;
XOR_DY yvec10, yvec10, yvec10;
XOR_DY yvec9, yvec9, yvec9;
XOR_DY yvec8, yvec8, yvec8;
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
JLE .L211_loopE;
ALIGN_5;
.L211_bodyB:
# Computing kernel
//#### Unroll time 1 ####
LD_DX	0*SIZE(ptrba), xvec0;
LD_DX	0*SIZE(ptrbb), xvec4;
MOV_DX	xvec4, xvec5;
MUL_DX xvec0, xvec4, xvec4;
ADD_DX xvec4, xvec15, xvec15;

LD_DX	2*SIZE(ptrba), xvec1;
MOV_DX	xvec5, xvec6;
MUL_DX xvec1, xvec5, xvec5;
ADD_DX xvec5, xvec14, xvec14;

LD_DX	4*SIZE(ptrba), xvec2;
MOV_DX	xvec6, xvec7;
MUL_DX xvec2, xvec6, xvec6;
ADD_DX xvec6, xvec13, xvec13;

LD_DX	6*SIZE(ptrba), xvec3;
SHUF_DX	$0x4e, xvec7, xvec4;
MUL_DX xvec3, xvec7, xvec7;
ADD_DX xvec7, xvec12, xvec12;

MOV_DX	xvec4, xvec5;
MUL_DX xvec0, xvec4, xvec4;
ADD_DX xvec4, xvec11, xvec11;

MOV_DX	xvec5, xvec6;
MUL_DX xvec1, xvec5, xvec5;
ADD_DX xvec5, xvec10, xvec10;

MOV_DX	xvec6, xvec7;
MUL_DX xvec2, xvec6, xvec6;
ADD_DX xvec6, xvec9, xvec9;

MUL_DX xvec3, xvec7, xvec7;
ADD_DX xvec7, xvec8, xvec8;

//#### Unroll time 2 ####
LD_DX	8*SIZE(ptrba), xvec0;
LD_DX	2*SIZE(ptrbb), xvec4;
MOV_DX	xvec4, xvec5;
MUL_DX xvec0, xvec4, xvec4;
ADD_DX xvec4, xvec15, xvec15;

LD_DX	10*SIZE(ptrba), xvec1;
MOV_DX	xvec5, xvec6;
MUL_DX xvec1, xvec5, xvec5;
ADD_DX xvec5, xvec14, xvec14;

LD_DX	12*SIZE(ptrba), xvec2;
MOV_DX	xvec6, xvec7;
MUL_DX xvec2, xvec6, xvec6;
ADD_DX xvec6, xvec13, xvec13;

LD_DX	14*SIZE(ptrba), xvec3;
SHUF_DX	$0x4e, xvec7, xvec4;
MUL_DX xvec3, xvec7, xvec7;
ADD_DX xvec7, xvec12, xvec12;

MOV_DX	xvec4, xvec5;
MUL_DX xvec0, xvec4, xvec4;
ADD_DX xvec4, xvec11, xvec11;

MOV_DX	xvec5, xvec6;
MUL_DX xvec1, xvec5, xvec5;
ADD_DX xvec5, xvec10, xvec10;

MOV_DX	xvec6, xvec7;
MUL_DX xvec2, xvec6, xvec6;
ADD_DX xvec6, xvec9, xvec9;

MUL_DX xvec3, xvec7, xvec7;
ADD_DX xvec7, xvec8, xvec8;

//#### Unroll time 3 ####
LD_DX	16*SIZE(ptrba), xvec0;
LD_DX	4*SIZE(ptrbb), xvec4;
MOV_DX	xvec4, xvec5;
MUL_DX xvec0, xvec4, xvec4;
ADD_DX xvec4, xvec15, xvec15;

LD_DX	18*SIZE(ptrba), xvec1;
MOV_DX	xvec5, xvec6;
MUL_DX xvec1, xvec5, xvec5;
ADD_DX xvec5, xvec14, xvec14;

LD_DX	20*SIZE(ptrba), xvec2;
MOV_DX	xvec6, xvec7;
MUL_DX xvec2, xvec6, xvec6;
ADD_DX xvec6, xvec13, xvec13;

LD_DX	22*SIZE(ptrba), xvec3;
SHUF_DX	$0x4e, xvec7, xvec4;
MUL_DX xvec3, xvec7, xvec7;
ADD_DX xvec7, xvec12, xvec12;

MOV_DX	xvec4, xvec5;
MUL_DX xvec0, xvec4, xvec4;
ADD_DX xvec4, xvec11, xvec11;

MOV_DX	xvec5, xvec6;
MUL_DX xvec1, xvec5, xvec5;
ADD_DX xvec5, xvec10, xvec10;

MOV_DX	xvec6, xvec7;
MUL_DX xvec2, xvec6, xvec6;
ADD_DX xvec6, xvec9, xvec9;

MUL_DX xvec3, xvec7, xvec7;
ADD_DX xvec7, xvec8, xvec8;

//#### Unroll time 4 ####
LD_DX	24*SIZE(ptrba), xvec0;
LD_DX	6*SIZE(ptrbb), xvec4;
MOV_DX	xvec4, xvec5;
MUL_DX xvec0, xvec4, xvec4;
ADD_DX xvec4, xvec15, xvec15;
ADDQ	$8*SIZE, ptrbb;

LD_DX	26*SIZE(ptrba), xvec1;
MOV_DX	xvec5, xvec6;
MUL_DX xvec1, xvec5, xvec5;
ADD_DX xvec5, xvec14, xvec14;

LD_DX	28*SIZE(ptrba), xvec2;
MOV_DX	xvec6, xvec7;
MUL_DX xvec2, xvec6, xvec6;
ADD_DX xvec6, xvec13, xvec13;

LD_DX	30*SIZE(ptrba), xvec3;
SHUF_DX	$0x4e, xvec7, xvec4;
MUL_DX xvec3, xvec7, xvec7;
ADD_DX xvec7, xvec12, xvec12;
ADDQ	$32*SIZE, ptrba;

MOV_DX	xvec4, xvec5;
MUL_DX xvec0, xvec4, xvec4;
ADD_DX xvec4, xvec11, xvec11;

MOV_DX	xvec5, xvec6;
MUL_DX xvec1, xvec5, xvec5;
ADD_DX xvec5, xvec10, xvec10;

MOV_DX	xvec6, xvec7;
MUL_DX xvec2, xvec6, xvec6;
ADD_DX xvec6, xvec9, xvec9;

MUL_DX xvec3, xvec7, xvec7;
ADD_DX xvec7, xvec8, xvec8;
DECQ	k;
JG	.L211_bodyB;
ALIGN_5
.L211_loopE:
#ifndef	TRMMKERNEL
TEST $2, bk;
#else
MOVQ kkk, %rax;
TEST $2, %rax;
#endif
JLE .L212_loopE;
ALIGN_5;
.L212_bodyB:
# Computing kernel
//#### Unroll time 1 ####
LD_DX	0*SIZE(ptrba), xvec0;
LD_DX	0*SIZE(ptrbb), xvec4;
MOV_DX	xvec4, xvec5;
MUL_DX xvec0, xvec4, xvec4;
ADD_DX xvec4, xvec15, xvec15;

LD_DX	2*SIZE(ptrba), xvec1;
MOV_DX	xvec5, xvec6;
MUL_DX xvec1, xvec5, xvec5;
ADD_DX xvec5, xvec14, xvec14;

LD_DX	4*SIZE(ptrba), xvec2;
MOV_DX	xvec6, xvec7;
MUL_DX xvec2, xvec6, xvec6;
ADD_DX xvec6, xvec13, xvec13;

LD_DX	6*SIZE(ptrba), xvec3;
SHUF_DX	$0x4e, xvec7, xvec4;
MUL_DX xvec3, xvec7, xvec7;
ADD_DX xvec7, xvec12, xvec12;

MOV_DX	xvec4, xvec5;
MUL_DX xvec0, xvec4, xvec4;
ADD_DX xvec4, xvec11, xvec11;

MOV_DX	xvec5, xvec6;
MUL_DX xvec1, xvec5, xvec5;
ADD_DX xvec5, xvec10, xvec10;

MOV_DX	xvec6, xvec7;
MUL_DX xvec2, xvec6, xvec6;
ADD_DX xvec6, xvec9, xvec9;

MUL_DX xvec3, xvec7, xvec7;
ADD_DX xvec7, xvec8, xvec8;

//#### Unroll time 2 ####
LD_DX	8*SIZE(ptrba), xvec0;
LD_DX	2*SIZE(ptrbb), xvec4;
MOV_DX	xvec4, xvec5;
MUL_DX xvec0, xvec4, xvec4;
ADD_DX xvec4, xvec15, xvec15;
ADDQ	$4*SIZE, ptrbb;

LD_DX	10*SIZE(ptrba), xvec1;
MOV_DX	xvec5, xvec6;
MUL_DX xvec1, xvec5, xvec5;
ADD_DX xvec5, xvec14, xvec14;

LD_DX	12*SIZE(ptrba), xvec2;
MOV_DX	xvec6, xvec7;
MUL_DX xvec2, xvec6, xvec6;
ADD_DX xvec6, xvec13, xvec13;

LD_DX	14*SIZE(ptrba), xvec3;
SHUF_DX	$0x4e, xvec7, xvec4;
MUL_DX xvec3, xvec7, xvec7;
ADD_DX xvec7, xvec12, xvec12;
ADDQ	$16*SIZE, ptrba;

MOV_DX	xvec4, xvec5;
MUL_DX xvec0, xvec4, xvec4;
ADD_DX xvec4, xvec11, xvec11;

MOV_DX	xvec5, xvec6;
MUL_DX xvec1, xvec5, xvec5;
ADD_DX xvec5, xvec10, xvec10;

MOV_DX	xvec6, xvec7;
MUL_DX xvec2, xvec6, xvec6;
ADD_DX xvec6, xvec9, xvec9;

MUL_DX xvec3, xvec7, xvec7;
ADD_DX xvec7, xvec8, xvec8;

.L212_loopE:
#ifndef	TRMMKERNEL
TEST $1, bk;
#else
MOVQ kkk, %rax;
TEST $1, %rax;
#endif
JLE .L213_loopE;
ALIGN_5
.L213_bodyB:
//#### Unroll time 1 ####
LD_DX	0*SIZE(ptrba), xvec0;
LD_DX	0*SIZE(ptrbb), xvec4;
MOV_DX	xvec4, xvec5;
MUL_DX xvec0, xvec4, xvec4;
ADD_DX xvec4, xvec15, xvec15;
ADDQ	$2*SIZE, ptrbb;

LD_DX	2*SIZE(ptrba), xvec1;
MOV_DX	xvec5, xvec6;
MUL_DX xvec1, xvec5, xvec5;
ADD_DX xvec5, xvec14, xvec14;

LD_DX	4*SIZE(ptrba), xvec2;
MOV_DX	xvec6, xvec7;
MUL_DX xvec2, xvec6, xvec6;
ADD_DX xvec6, xvec13, xvec13;

LD_DX	6*SIZE(ptrba), xvec3;
SHUF_DX	$0x4e, xvec7, xvec4;
MUL_DX xvec3, xvec7, xvec7;
ADD_DX xvec7, xvec12, xvec12;
ADDQ	$8*SIZE, ptrba;

MOV_DX	xvec4, xvec5;
MUL_DX xvec0, xvec4, xvec4;
ADD_DX xvec4, xvec11, xvec11;

MOV_DX	xvec5, xvec6;
MUL_DX xvec1, xvec5, xvec5;
ADD_DX xvec5, xvec10, xvec10;

MOV_DX	xvec6, xvec7;
MUL_DX xvec2, xvec6, xvec6;
ADD_DX xvec6, xvec9, xvec9;

MUL_DX xvec3, xvec7, xvec7;
ADD_DX xvec7, xvec8, xvec8;

.L213_loopE:
//#### Multiply Alpha ####
BROAD_DX MEMALPHA, xvec7;
MUL_DX xvec7, xvec15, xvec15;
MUL_DX xvec7, xvec14, xvec14;
MUL_DX xvec7, xvec13, xvec13;
MUL_DX xvec7, xvec12, xvec12;
MUL_DX xvec7, xvec11, xvec11;
MUL_DX xvec7, xvec10, xvec10;
MUL_DX xvec7, xvec9, xvec9;
MUL_DX xvec7, xvec8, xvec8;
//#### Reverse ####
MOV_DX	xvec15, xvec6;
REVS_DX xvec11, xvec15, xvec15;
REVS_DX xvec6, xvec11, xvec11;
MOV_DX	xvec14, xvec6;
REVS_DX xvec10, xvec14, xvec14;
REVS_DX xvec6, xvec10, xvec10;
MOV_DX	xvec13, xvec6;
REVS_DX xvec9, xvec13, xvec13;
REVS_DX xvec6, xvec9, xvec9;
MOV_DX	xvec12, xvec6;
REVS_DX xvec8, xvec12, xvec12;
REVS_DX xvec6, xvec8, xvec8;
//#### Testing Alignment ####
MOVQ	C0, %rax;
OR		ldc, %rax;
TEST	$15, %rax;
JNE		.L213_loopEx;
ALIGN_5
//#### Writing Back ####
#ifndef	TRMMKERNEL
ADD_DX 0*SIZE(C0), xvec11, xvec11;
ADD_DX 2*SIZE(C0), xvec10, xvec10;
ADD_DX 4*SIZE(C0), xvec9, xvec9;
ADD_DX 6*SIZE(C0), xvec8, xvec8;
ADD_DX 0*SIZE(C1), xvec15, xvec15;
ADD_DX 2*SIZE(C1), xvec14, xvec14;
ADD_DX 4*SIZE(C1), xvec13, xvec13;
ADD_DX 6*SIZE(C1), xvec12, xvec12;
#endif
ST_DX	xvec11,	0*SIZE(C0);
ST_DX	xvec10,	2*SIZE(C0);
ST_DX	xvec9,	4*SIZE(C0);
ST_DX	xvec8,	6*SIZE(C0);
ST_DX	xvec15, 0*SIZE(C1);
ST_DX	xvec14, 2*SIZE(C1);
ST_DX	xvec13, 4*SIZE(C1);
ST_DX	xvec12, 6*SIZE(C1);
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
DECQ i;
JG	.L21_bodyB;
JMP	.L21_loopE;
ALIGN_5
.L213_loopEx:;
#ifndef	TRMMKERNEL
LDL_DX 0*SIZE(C0), xvec0, xvec0;
LDH_DX 1*SIZE(C0), xvec0, xvec0;
LDL_DX 2*SIZE(C0), xvec1, xvec1;
LDH_DX 3*SIZE(C0), xvec1, xvec1;
LDL_DX 4*SIZE(C0), xvec2, xvec2;
LDH_DX 5*SIZE(C0), xvec2, xvec2;
LDL_DX 6*SIZE(C0), xvec3, xvec3;
LDH_DX 7*SIZE(C0), xvec3, xvec3;
ADD_DX xvec0, xvec11, xvec11;
ADD_DX xvec1, xvec10, xvec10;
ADD_DX xvec2, xvec9, xvec9;
ADD_DX xvec3, xvec8, xvec8;
#endif
STL_DX	xvec11, 0*SIZE(C0);
STH_DX	xvec11, 1*SIZE(C0);
STL_DX	xvec10, 2*SIZE(C0);
STH_DX	xvec10, 3*SIZE(C0);
STL_DX	xvec9, 4*SIZE(C0);
STH_DX	xvec9, 5*SIZE(C0);
STL_DX	xvec8, 6*SIZE(C0);
STH_DX	xvec8, 7*SIZE(C0);
#ifndef	TRMMKERNEL
LDL_DX 0*SIZE(C1), xvec4, xvec4;
LDH_DX 1*SIZE(C1), xvec4, xvec4;
LDL_DX 2*SIZE(C1), xvec5, xvec5;
LDH_DX 3*SIZE(C1), xvec5, xvec5;
LDL_DX 4*SIZE(C1), xvec6, xvec6;
LDH_DX 5*SIZE(C1), xvec6, xvec6;
LDL_DX 6*SIZE(C1), xvec7, xvec7;
LDH_DX 7*SIZE(C1), xvec7, xvec7;
ADD_DX xvec4, xvec15, xvec15;
ADD_DX xvec5, xvec14, xvec14;
ADD_DX xvec6, xvec13, xvec13;
ADD_DX xvec7, xvec12, xvec12;
#endif
STL_DX	xvec15, 0*SIZE(C1);
STH_DX	xvec15, 1*SIZE(C1);
STL_DX	xvec14, 2*SIZE(C1);
STH_DX	xvec14, 3*SIZE(C1);
STL_DX	xvec13, 4*SIZE(C1);
STH_DX	xvec13, 5*SIZE(C1);
STL_DX	xvec12, 6*SIZE(C1);
STH_DX	xvec12, 7*SIZE(C1);
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
JG	.L21_bodyB;
.L21_loopE:;
TEST $4, bm;				# Rm = 4
JLE .L22_loopE;
ALIGN_5;
.L22_bodyB:;
#if !defined(TRMMKERNEL)||(defined(TRMMKERNEL)&&defined(LEFT)&&defined(TRANSA))||(defined(TRMMKERNEL)&&!defined(LEFT)&&!defined(TRANSA))
MOVQ bb, ptrbb;
#else
MOVQ bb, ptrbb;
MOVQ kk, %rax;
LEAQ	(,%rax, SIZE), %rax;
LEAQ	(ptrba, %rax, 4), ptrba;
LEAQ	(ptrbb, %rax, 2), ptrbb;
#endif
//#### Initial Results Register ####
XOR_DY yvec15, yvec15, yvec15;
XOR_DY yvec14, yvec14, yvec14;
XOR_DY yvec11, yvec11, yvec11;
XOR_DY yvec10, yvec10, yvec10;
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
JLE	.L221_loopE;
ALIGN_5
.L221_bodyB:;
# Computing kernel
//#### Unroll time 1 ####
LD_DX	0*SIZE(ptrba), xvec0;
LD_DX	0*SIZE(ptrbb), xvec4;
MOV_DX	xvec4, xvec5;
MUL_DX xvec0, xvec4, xvec4;
ADD_DX xvec4, xvec15, xvec15;

LD_DX	2*SIZE(ptrba), xvec1;
SHUF_DX	$0x4e, xvec5, xvec4;
MUL_DX xvec1, xvec5, xvec5;
ADD_DX xvec5, xvec14, xvec14;

MOV_DX	xvec4, xvec5;
MUL_DX xvec0, xvec4, xvec4;
ADD_DX xvec4, xvec11, xvec11;

MUL_DX xvec1, xvec5, xvec5;
ADD_DX xvec5, xvec10, xvec10;

//#### Unroll time 2 ####
LD_DX	4*SIZE(ptrba), xvec0;
LD_DX	2*SIZE(ptrbb), xvec4;
MOV_DX	xvec4, xvec5;
MUL_DX xvec0, xvec4, xvec4;
ADD_DX xvec4, xvec15, xvec15;

LD_DX	6*SIZE(ptrba), xvec1;
SHUF_DX	$0x4e, xvec5, xvec4;
MUL_DX xvec1, xvec5, xvec5;
ADD_DX xvec5, xvec14, xvec14;

MOV_DX	xvec4, xvec5;
MUL_DX xvec0, xvec4, xvec4;
ADD_DX xvec4, xvec11, xvec11;

MUL_DX xvec1, xvec5, xvec5;
ADD_DX xvec5, xvec10, xvec10;

//#### Unroll time 3 ####
LD_DX	8*SIZE(ptrba), xvec0;
LD_DX	4*SIZE(ptrbb), xvec4;
MOV_DX	xvec4, xvec5;
MUL_DX xvec0, xvec4, xvec4;
ADD_DX xvec4, xvec15, xvec15;

LD_DX	10*SIZE(ptrba), xvec1;
SHUF_DX	$0x4e, xvec5, xvec4;
MUL_DX xvec1, xvec5, xvec5;
ADD_DX xvec5, xvec14, xvec14;

MOV_DX	xvec4, xvec5;
MUL_DX xvec0, xvec4, xvec4;
ADD_DX xvec4, xvec11, xvec11;

MUL_DX xvec1, xvec5, xvec5;
ADD_DX xvec5, xvec10, xvec10;

//#### Unroll time 4 ####
LD_DX	12*SIZE(ptrba), xvec0;
LD_DX	6*SIZE(ptrbb), xvec4;
MOV_DX	xvec4, xvec5;
MUL_DX xvec0, xvec4, xvec4;
ADD_DX xvec4, xvec15, xvec15;
ADDQ	$8*SIZE, ptrbb;

LD_DX	14*SIZE(ptrba), xvec1;
SHUF_DX	$0x4e, xvec5, xvec4;
MUL_DX xvec1, xvec5, xvec5;
ADD_DX xvec5, xvec14, xvec14;
ADDQ	$16*SIZE, ptrba;

MOV_DX	xvec4, xvec5;
MUL_DX xvec0, xvec4, xvec4;
ADD_DX xvec4, xvec11, xvec11;

MUL_DX xvec1, xvec5, xvec5;
ADD_DX xvec5, xvec10, xvec10;
DECQ	k;
JG	.L221_bodyB;
ALIGN_5
.L221_loopE:;
#ifndef	TRMMKERNEL
TEST $2, bk;
#else
MOVQ kkk, %rax;
TEST $2, %rax;
#endif
JLE .L222_loopE;
ALIGN_5
.L222_bodyB:
//#### Unroll time 1 ####
LD_DX	0*SIZE(ptrba), xvec0;
LD_DX	0*SIZE(ptrbb), xvec4;
MOV_DX	xvec4, xvec5;
MUL_DX xvec0, xvec4, xvec4;
ADD_DX xvec4, xvec15, xvec15;

LD_DX	2*SIZE(ptrba), xvec1;
SHUF_DX	$0x4e, xvec5, xvec4;
MUL_DX xvec1, xvec5, xvec5;
ADD_DX xvec5, xvec14, xvec14;

MOV_DX	xvec4, xvec5;
MUL_DX xvec0, xvec4, xvec4;
ADD_DX xvec4, xvec11, xvec11;

MUL_DX xvec1, xvec5, xvec5;
ADD_DX xvec5, xvec10, xvec10;

//#### Unroll time 2 ####
LD_DX	4*SIZE(ptrba), xvec0;
LD_DX	2*SIZE(ptrbb), xvec4;
MOV_DX	xvec4, xvec5;
MUL_DX xvec0, xvec4, xvec4;
ADD_DX xvec4, xvec15, xvec15;
ADDQ	$4*SIZE, ptrbb;

LD_DX	6*SIZE(ptrba), xvec1;
SHUF_DX	$0x4e, xvec5, xvec4;
MUL_DX xvec1, xvec5, xvec5;
ADD_DX xvec5, xvec14, xvec14;
ADDQ	$8*SIZE, ptrba;
MOV_DX	xvec4, xvec5;
MUL_DX xvec0, xvec4, xvec4;
ADD_DX xvec4, xvec11, xvec11;

MUL_DX xvec1, xvec5, xvec5;
ADD_DX xvec5, xvec10, xvec10;

.L222_loopE:
#ifndef	TRMMKERNEL
TEST $1, bk
#else
MOVQ kkk, %rax;
TEST $1, %rax;
#endif
JLE .L223_loopE;
ALIGN_5
.L223_bodyB:
//#### Unroll time 1 ####
LD_DX	0*SIZE(ptrba), xvec0;
LD_DX	0*SIZE(ptrbb), xvec4;
MOV_DX	xvec4, xvec5;
MUL_DX xvec0, xvec4, xvec4;
ADD_DX xvec4, xvec15, xvec15;
ADDQ	$2*SIZE, ptrbb;

LD_DX	2*SIZE(ptrba), xvec1;
SHUF_DX	$0x4e, xvec5, xvec4;
MUL_DX xvec1, xvec5, xvec5;
ADD_DX xvec5, xvec14, xvec14;
ADDQ	$4*SIZE, ptrba;

MOV_DX	xvec4, xvec5;
MUL_DX xvec0, xvec4, xvec4;
ADD_DX xvec4, xvec11, xvec11;

MUL_DX xvec1, xvec5, xvec5;
ADD_DX xvec5, xvec10, xvec10;

.L223_loopE:
//#### Multiply Alpha ####
BROAD_DX MEMALPHA, xvec7;
MUL_DX xvec7, xvec15, xvec15;
MUL_DX xvec7, xvec14, xvec14;
MUL_DX xvec7, xvec11, xvec11;
MUL_DX xvec7, xvec10, xvec10;
//#### Reverse ####
MOV_DX	xvec15, xvec6;
REVS_DX xvec11, xvec15, xvec15;
REVS_DX xvec6, xvec11, xvec11;
MOV_DX	xvec14, xvec6;
REVS_DX xvec10, xvec14, xvec14;
REVS_DX xvec6, xvec10, xvec10;
//#### Testing Alignment ####
MOVQ	C0, %rax;
OR		ldc, %rax;
TEST	$15, %rax;
JNE		.L223_loopEx;
ALIGN_5
//#### Writing Back ####
#ifndef	TRMMKERNEL
ADD_DX 0*SIZE(C0), xvec11, xvec11;
ADD_DX 2*SIZE(C0), xvec10, xvec10;
ADD_DX 0*SIZE(C1), xvec15, xvec15;
ADD_DX 2*SIZE(C1), xvec14, xvec14;
#endif
ST_DX	xvec11,	0*SIZE(C0);
ST_DX	xvec10,	2*SIZE(C0);
ST_DX	xvec15, 0*SIZE(C1);
ST_DX	xvec14, 2*SIZE(C1);
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
JMP	.L22_loopE;
ALIGN_5
.L223_loopEx:;
#ifndef	TRMMKERNEL
LDL_DX 0*SIZE(C0), xvec0, xvec0;
LDH_DX 1*SIZE(C0), xvec0, xvec0;
LDL_DX 2*SIZE(C0), xvec1, xvec1;
LDH_DX 3*SIZE(C0), xvec1, xvec1;
ADD_DX xvec0, xvec11, xvec11;
ADD_DX xvec1, xvec10, xvec10;
#endif
STL_DX	xvec11, 0*SIZE(C0);
STH_DX	xvec11, 1*SIZE(C0);
STL_DX	xvec10, 2*SIZE(C0);
STH_DX	xvec10, 3*SIZE(C0);
#ifndef	TRMMKERNEL
LDL_DX 0*SIZE(C1), xvec4, xvec4;
LDH_DX 1*SIZE(C1), xvec4, xvec4;
LDL_DX 2*SIZE(C1), xvec5, xvec5;
LDH_DX 3*SIZE(C1), xvec5, xvec5;
ADD_DX xvec4, xvec15, xvec15;
ADD_DX xvec5, xvec14, xvec14;
#endif
STL_DX	xvec15, 0*SIZE(C1);
STH_DX	xvec15, 1*SIZE(C1);
STL_DX	xvec14, 2*SIZE(C1);
STH_DX	xvec14, 3*SIZE(C1);
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
.L22_loopE:;
TEST $2, bm;				// Rm = 2
JLE .L23_loopE;
ALIGN_5;
.L23_bodyB:
#if !defined(TRMMKERNEL)||(defined(TRMMKERNEL)&&defined(LEFT)&&defined(TRANSA))||(defined(TRMMKERNEL)&&!defined(LEFT)&&!defined(TRANSA))
MOVQ bb, ptrbb;
#else
MOVQ bb, ptrbb;
MOVQ kk, %rax;
LEAQ	(,%rax, SIZE), %rax;
LEAQ	(ptrba, %rax, 2), ptrba;
LEAQ	(ptrbb, %rax, 2), ptrbb;
#endif
XOR_DY yvec15, yvec15, yvec15;
XOR_DY yvec11, yvec11, yvec11;
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
JLE .L231_loopE;
ALIGN_5
.L231_bodyB:
# Computing kernel
//#### Unroll time 1 ####
LD_DX	0*SIZE(ptrba), xvec0;
LD_DX	0*SIZE(ptrbb), xvec4;
SHUF_DX $0x4e, xvec4, xvec5;
MUL_DX xvec0, xvec4, xvec4;
ADD_DX xvec4, xvec15, xvec15;

MUL_DX xvec0, xvec5, xvec5;
ADD_DX xvec5, xvec11, xvec11;
//#### Unroll time 2 ####
LD_DX	2*SIZE(ptrba), xvec0;
LD_DX	2*SIZE(ptrbb), xvec4;
SHUF_DX $0x4e, xvec4, xvec5;
MUL_DX xvec0, xvec4, xvec4;
ADD_DX xvec4, xvec15, xvec15;

MUL_DX xvec0, xvec5, xvec5;
ADD_DX xvec5, xvec11, xvec11;
//#### Unroll time 3 ####
LD_DX	4*SIZE(ptrba), xvec0;
LD_DX	4*SIZE(ptrbb), xvec4;
SHUF_DX $0x4e, xvec4, xvec5;
MUL_DX xvec0, xvec4, xvec4;
ADD_DX xvec4, xvec15, xvec15;

MUL_DX xvec0, xvec5, xvec5;
ADD_DX xvec5, xvec11, xvec11;
//#### Unroll time 4 ####
LD_DX	6*SIZE(ptrba), xvec0;
LD_DX	6*SIZE(ptrbb), xvec4;
SHUF_DX $0x4e, xvec4, xvec5;
MUL_DX xvec0, xvec4, xvec4;
ADD_DX xvec4, xvec15, xvec15;
ADDQ	$8*SIZE, ptrba;
MUL_DX xvec0, xvec5, xvec5;
ADD_DX xvec5, xvec11, xvec11;
ADDQ	$8*SIZE, ptrbb;
DECQ k;
JG .L231_bodyB;
ALIGN_5
.L231_loopE:
#ifndef	TRMMKERNEL
TEST $2, bk;
#else
MOVQ kkk, %rax;
TEST $2, %rax;
#endif
JLE .L232_loopE;
ALIGN_5
.L232_bodyB:
//#### Unroll time 1 ####
LD_DX	0*SIZE(ptrba), xvec0;
LD_DX	0*SIZE(ptrbb), xvec4;
SHUF_DX $0x4e, xvec4, xvec5;
MUL_DX xvec0, xvec4, xvec4;
ADD_DX xvec4, xvec15, xvec15;

MUL_DX xvec0, xvec5, xvec5;
ADD_DX xvec5, xvec11, xvec11;
//#### Unroll time 2 ####
LD_DX	2*SIZE(ptrba), xvec0;
LD_DX	2*SIZE(ptrbb), xvec4;
SHUF_DX $0x4e, xvec4, xvec5;
MUL_DX xvec0, xvec4, xvec4;
ADD_DX xvec4, xvec15, xvec15;
ADDQ	$4*SIZE, ptrba;
MUL_DX xvec0, xvec5, xvec5;
ADD_DX xvec5, xvec11, xvec11;
ADDQ	$4*SIZE, ptrbb;
.L232_loopE:
#ifndef	TRMMKERNEL
TEST $1, bk;
#else
MOVQ kkk, %rax;
TEST $1, %rax;
#endif
JLE .L233_loopE;
ALIGN_5
.L233_bodyB:
//#### Unroll time 1 ####
LD_DX	0*SIZE(ptrba), xvec0;
LD_DX	0*SIZE(ptrbb), xvec4;
SHUF_DX $0x4e, xvec4, xvec5;
MUL_DX xvec0, xvec4, xvec4;
ADD_DX xvec4, xvec15, xvec15;
ADDQ	$2*SIZE, ptrba;
MUL_DX xvec0, xvec5, xvec5;
ADD_DX xvec5, xvec11, xvec11;
ADDQ	$2*SIZE, ptrbb;
.L233_loopE:
//#### Multiply Alpha ####
BROAD_DX MEMALPHA, xvec7;
MUL_DX xvec7, xvec15, xvec15;
MUL_DX xvec7, xvec11, xvec11;
//#### Reverse ####
MOV_DX	xvec15, xvec6;
REVS_DX xvec11, xvec15, xvec15;
REVS_DX xvec6, xvec11, xvec11;
//#### Testing Alignment ####
MOVQ	C0, %rax;
OR		ldc, %rax;
TEST	$15, %rax;
JNE		.L233_loopEx;
ALIGN_5
//#### Writing Back ####
#ifndef	TRMMKERNEL
ADD_DX 0*SIZE(C0), xvec11, xvec11;
ADD_DX 0*SIZE(C1), xvec15, xvec15;
#endif
ST_DX	xvec11,	0*SIZE(C0);
ST_DX	xvec15, 0*SIZE(C1);
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
JMP	.L23_loopE;
ALIGN_5
.L233_loopEx:;
#ifndef	TRMMKERNEL
LDL_DX 0*SIZE(C0), xvec0, xvec0;
LDH_DX 1*SIZE(C0), xvec0, xvec0;
ADD_DX xvec0, xvec11, xvec11;
#endif
STL_DX	xvec11, 0*SIZE(C0);
STH_DX	xvec11, 1*SIZE(C0);
#ifndef	TRMMKERNEL
LDL_DX 0*SIZE(C1), xvec4, xvec4;
LDH_DX 1*SIZE(C1), xvec4, xvec4;
ADD_DX xvec4, xvec15, xvec15;
#endif
STL_DX	xvec15, 0*SIZE(C1);
STH_DX	xvec15, 1*SIZE(C1);
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
.L23_loopE:
TEST $1, bm;				// Rm = 1
JLE .L24_loopE;
ALIGN_5;
.L24_bodyB:
#if !defined(TRMMKERNEL)||(defined(TRMMKERNEL)&&defined(LEFT)&&defined(TRANSA))||(defined(TRMMKERNEL)&&!defined(LEFT)&&!defined(TRANSA))
MOVQ bb, ptrbb;
#else
MOVQ bb, ptrbb;
MOVQ kk, %rax;
LEAQ	(, %rax, SIZE), %rax;
ADDQ	%rax, ptrba;
LEAQ	(ptrbb, %rax, 2), ptrbb;
#endif
XOR_DY	yvec15, yvec15, yvec15;
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
JLE .L241_loopE;
ALIGN_5
.L241_bodyB:
BROAD_DX 0*SIZE(ptrba), xvec0;
LD_DX	0*SIZE(ptrbb), xvec2;
MUL_DX xvec0, xvec2, xvec2;
ADD_DX xvec2, xvec15, xvec15;

BROAD_DX 1*SIZE(ptrba), xvec1;
LD_DX	2*SIZE(ptrbb), xvec3;
MUL_DX xvec1, xvec3, xvec3;
ADD_DX xvec3, xvec15, xvec15;

BROAD_DX 2*SIZE(ptrba), xvec0;
LD_DX	4*SIZE(ptrbb), xvec2;
MUL_DX xvec0, xvec2, xvec2;
ADD_DX xvec2, xvec15, xvec15;

BROAD_DX 3*SIZE(ptrba), xvec1;
LD_DX	6*SIZE(ptrbb), xvec3;
MUL_DX xvec1, xvec3, xvec3;
ADD_DX xvec3, xvec15, xvec15;
ADDQ	$4*SIZE, ptrba;
ADDQ	$8*SIZE, ptrbb;
DECQ k;
JG .L241_bodyB;
ALIGN_5
.L241_loopE:
#ifndef	TRMMKERNEL
TEST $2, bk;
#else
MOVQ kkk, %rax;
TEST $2, %rax;
#endif
JLE .L242_loopE;
ALIGN_5
.L242_bodyB:
BROAD_DX 0*SIZE(ptrba), xvec0;
LD_DX	0*SIZE(ptrbb), xvec2;
MUL_DX xvec0, xvec2, xvec2;
ADD_DX xvec2, xvec15, xvec15;

BROAD_DX 1*SIZE(ptrba), xvec1;
LD_DX	2*SIZE(ptrbb), xvec3;
MUL_DX xvec1, xvec3, xvec3;
ADD_DX xvec3, xvec15, xvec15;
ADDQ	$2*SIZE, ptrba;
ADDQ	$4*SIZE, ptrbb;
.L242_loopE:
#ifndef	TRMMKERNEL
TEST $1, bk;
#else
MOVQ kkk, %rax;
TEST $1, %rax;
#endif
JLE .L243_loopE;
ALIGN_5
.L243_bodyB:
BROAD_DX 0*SIZE(ptrba), xvec0;
LD_DX	0*SIZE(ptrbb), xvec2;
MUL_DX xvec0, xvec2, xvec2;
ADD_DX xvec2, xvec15, xvec15;
ADDQ	$1*SIZE, ptrba;
ADDQ	$2*SIZE, ptrbb;

.L243_loopE:
BROAD_DX MEMALPHA, xvec7;
MUL_DX xvec7, xvec15, xvec15;
#ifndef	TRMMKERNEL
LDL_DX 0*SIZE(C0), xvec0, xvec0;
LDH_DX 0*SIZE(C1), xvec0, xvec0;
ADD_DX xvec0, xvec15, xvec15;
#endif
STL_DX	xvec15, 0*SIZE(C0);
STH_DX	xvec15, 0*SIZE(C1);
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
ADDQ	$1*SIZE, C0;
ADDQ	$1*SIZE, C1;
.L24_loopE:
#if defined(TRMMKERNEL) && !defined(LEFT)
ADDQ	$2, kk;
#endif
MOVQ bk, k;
SALQ $4, k;
ADDQ k, bb;
LEAQ (C, ldc, 2), C;
.L20_loopE:;
TEST $1, bn;					// Rn = 1
JLE .L30_loopE;
ALIGN_5
.L30_bodyB:
#if defined(TRMMKERNEL)&&defined(LEFT)
MOVQ	OFFSET, %rax;
MOVQ	%rax, kk;
#endif
MOVQ C, C0;
MOVQ ba, ptrba;
MOVQ bm, i;
SARQ $3, i;
JLE .L31_loopE;
ALIGN_5
.L31_bodyB:
#if !defined(TRMMKERNEL)||(defined(TRMMKERNEL)&&defined(LEFT)&&defined(TRANSA))||(defined(TRMMKERNEL)&&!defined(LEFT)&&!defined(TRANSA))
MOVQ bb, ptrbb;
#else
MOVQ bb, ptrbb;
MOVQ kk, %rax
LEAQ (, %rax, SIZE), %rax;
LEAQ (ptrba, %rax, 8), ptrba;
ADDQ	%rax, ptrbb;
#endif
//#### Initial Results Register ####
XOR_DY	yvec15, yvec15, yvec15;
XOR_DY	yvec14, yvec14, yvec14;
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
JLE .L311_loopE;
ALIGN_5
.L311_bodyB:
//#### Unroll time 1 ####
LD_DY	0*SIZE(ptrba), yvec0;
LD_DY	4*SIZE(ptrba), yvec1;
BROAD_DY 0*SIZE(ptrbb), yvec2;
MUL_DY	yvec2, yvec0, yvec0;
ADD_DY	yvec0, yvec15, yvec15;
MUL_DY	yvec2, yvec1, yvec1;
ADD_DY	yvec1, yvec14, yvec14;

//#### Unroll time 2 ####
LD_DY	8*SIZE(ptrba), yvec3;
LD_DY	12*SIZE(ptrba), yvec4;
BROAD_DY 1*SIZE(ptrbb), yvec5;
MUL_DY	yvec5, yvec3, yvec3;
ADD_DY	yvec3, yvec15, yvec15;
MUL_DY	yvec5, yvec4, yvec4
ADD_DY	yvec4, yvec14, yvec14;

//#### Unroll time 3 ####
LD_DY	16*SIZE(ptrba), yvec0;
LD_DY	20*SIZE(ptrba), yvec1;
BROAD_DY 2*SIZE(ptrbb), yvec2;
MUL_DY	yvec2, yvec0, yvec0;
ADD_DY	yvec0, yvec15, yvec15;
MUL_DY	yvec2, yvec1, yvec1;
ADD_DY	yvec1, yvec14, yvec14;

//#### Unroll time 2 ####
LD_DY	24*SIZE(ptrba), yvec3;
LD_DY	28*SIZE(ptrba), yvec4;
BROAD_DY 3*SIZE(ptrbb), yvec5;
MUL_DY	yvec5, yvec3, yvec3;
ADD_DY	yvec3, yvec15, yvec15;
ADDQ	$32*SIZE, ptrba;
MUL_DY	yvec5, yvec4, yvec4;
ADD_DY	yvec4, yvec14, yvec14;
ADDQ	$4*SIZE, ptrbb;
DECQ	k;
JG	.L311_bodyB;
ALIGN_5
.L311_loopE:
#ifndef	TRMMKERNEL
TEST $2, bk;
#else
MOVQ kkk, %rax;
TEST $2, %rax;
#endif
JLE .L312_loopE;
ALIGN_5
.L312_bodyB:
//#### Unroll time 1 ####
LD_DY	0*SIZE(ptrba), yvec0;
LD_DY	4*SIZE(ptrba), yvec1;
BROAD_DY 0*SIZE(ptrbb), yvec2;
MUL_DY	yvec2, yvec0, yvec0;
ADD_DY	yvec0, yvec15, yvec15;
MUL_DY	yvec2, yvec1, yvec1;
ADD_DY	yvec1, yvec14, yvec14;

//#### Unroll time 2 ####
LD_DY	8*SIZE(ptrba), yvec3;
LD_DY	12*SIZE(ptrba), yvec4;
BROAD_DY 1*SIZE(ptrbb), yvec5;
MUL_DY	yvec5, yvec3, yvec3;
ADD_DY	yvec3, yvec15, yvec15;
ADDQ	$16*SIZE, ptrba;
MUL_DY	yvec5, yvec4, yvec4
ADD_DY	yvec4, yvec14, yvec14;
ADDQ	$2*SIZE, ptrbb;

.L312_loopE:
#ifndef	TRMMKERNEL
TEST $1, bk;
#else
MOVQ kkk, %rax;
TEST $1, %rax;
#endif
JLE .L313_loopE;
ALIGN_5
.L313_bodyB:
//#### Unroll time 1 ####
LD_DY	0*SIZE(ptrba), yvec0;
LD_DY	4*SIZE(ptrba), yvec1;
BROAD_DY 0*SIZE(ptrbb), yvec2;
MUL_DY	yvec2, yvec0, yvec0;
ADD_DY	yvec0, yvec15, yvec15;
ADDQ	$8*SIZE, ptrba;
MUL_DY	yvec2, yvec1, yvec1;
ADD_DY	yvec1, yvec14, yvec14;
ADDQ	$1*SIZE, ptrbb;

.L313_loopE:
//#### Multiply Alpha ####
BROAD_DY MEMALPHA, yvec7;
MUL_DY	yvec7, yvec15, yvec15;
MUL_DY	yvec7, yvec14, yvec14;
//#### Testing Alignment ####
MOVQ	C0, %rax;
OR		ldc, %rax;
TEST 	$15, %rax;
JNE		.L313_loopEx;
ALIGN_5
//#### Writing Back ####
EXTRA_DY $1, yvec15, xvec13;
EXTRA_DY $1, yvec14, xvec12;
#ifndef	TRMMKERNEL
ADD_DX 0*SIZE(C0), xvec15, xvec15;
ADD_DX 2*SIZE(C0), xvec13, xvec13;
ADD_DX 4*SIZE(C0), xvec14, xvec14;
ADD_DX 6*SIZE(C0), xvec12, xvec12;
#endif
ST_DX	xvec15, 0*SIZE(C0);
ST_DX	xvec13, 2*SIZE(C0);
ST_DX	xvec14,	4*SIZE(C0);
ST_DX	xvec12, 6*SIZE(C0);
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
JG	.L31_bodyB;
JMP .L31_loopE;
ALIGN_5
.L313_loopEx:
EXTRA_DY $1, yvec15, xvec13;
EXTRA_DY $1, yvec14, xvec12;
#ifndef	TRMMKERNEL
LDL_DX 0*SIZE(C0), xvec11, xvec11;
LDH_DX 1*SIZE(C0), xvec11, xvec11;
LDL_DX 2*SIZE(C0), xvec10, xvec10;
LDH_DX 3*SIZE(C0), xvec10, xvec10;
LDL_DX 4*SIZE(C0), xvec9, xvec9;
LDH_DX 5*SIZE(C0), xvec9, xvec9;
LDL_DX 6*SIZE(C0), xvec8, xvec8;
LDH_DX 7*SIZE(C0), xvec8, xvec8;
ADD_DX xvec11, xvec15, xvec15;
ADD_DX xvec10, xvec13, xvec13;
ADD_DX xvec9, xvec14, xvec14;
ADD_DX xvec8, xvec12, xvec12;
#endif
STL_DX	xvec15, 0*SIZE(C0);
STH_DX	xvec15, 1*SIZE(C0);
STL_DX	xvec13, 2*SIZE(C0);
STH_DX	xvec13, 3*SIZE(C0);
STL_DX	xvec14, 4*SIZE(C0);
STH_DX	xvec14, 5*SIZE(C0);
STL_DX	xvec12, 6*SIZE(C0);
STH_DX	xvec12, 7*SIZE(C0);
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
JG	.L31_bodyB;
.L31_loopE:
TEST $4, bm
JLE .L32_loopE;
ALIGN_5
.L32_bodyB:
#if !defined(TRMMKERNEL)||(defined(TRMMKERNEL)&&defined(LEFT)&&defined(TRANSA))||(defined(TRMMKERNEL)&&!defined(LEFT)&&!defined(TRANSA))
MOVQ bb, ptrbb;
#else
MOVQ bb, ptrbb;
MOVQ kk, %rax;
LEAQ (,%rax, SIZE), %rax;
LEAQ (ptrba, %rax, 4), ptrba;
ADDQ	%rax, ptrbb;
#endif
//#### Initial Results Register ####
XOR_DY	yvec15, yvec15, yvec15;
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
JLE .L321_loopE;
ALIGN_5
.L321_bodyB:
LD_DY	0*SIZE(ptrba), yvec0;
BROAD_DY 0*SIZE(ptrbb), yvec1;
MUL_DY	yvec0, yvec1, yvec1;
ADD_DY	yvec1, yvec15, yvec15;

LD_DY	4*SIZE(ptrba), yvec2;
BROAD_DY 1*SIZE(ptrbb), yvec3;
MUL_DY	yvec2, yvec3, yvec3;
ADD_DY	yvec3, yvec15, yvec15;

LD_DY	8*SIZE(ptrba), yvec4;
BROAD_DY 2*SIZE(ptrbb), yvec5;
MUL_DY	yvec4, yvec5, yvec5;
ADD_DY	yvec5, yvec15, yvec15;

LD_DY	12*SIZE(ptrba), yvec6;
BROAD_DY 3*SIZE(ptrbb), yvec7;
MUL_DY	yvec6, yvec7, yvec7;
ADD_DY	yvec7, yvec15, yvec15;
ADDQ	$16*SIZE, ptrba;
ADDQ	$4*SIZE, ptrbb;
DECQ	k;
JG .L321_bodyB;
ALIGN_5
.L321_loopE:
#ifndef	TRMMKERNEL
TEST $2, bk;
#else
MOVQ kkk, %rax;
TEST $2, %rax;
#endif
JLE .L322_loopE;
ALIGN_5
.L322_bodyB:
LD_DY	0*SIZE(ptrba), yvec0;
BROAD_DY 0*SIZE(ptrbb), yvec1;
MUL_DY	yvec0, yvec1, yvec1;
ADD_DY	yvec1, yvec15, yvec15;

LD_DY	4*SIZE(ptrba), yvec2;
BROAD_DY 1*SIZE(ptrbb), yvec3;
MUL_DY	yvec2, yvec3, yvec3;
ADD_DY	yvec3, yvec15, yvec15;
ADDQ	$8*SIZE, ptrba;
ADDQ	$2*SIZE, ptrbb;

.L322_loopE:
#ifndef	TRMMKERNEL
TEST $1, bk;
#else
MOVQ kkk, %rax;
TEST $1, %rax;
#endif
JLE .L323_loopE;
ALIGN_5
.L323_bodyB:
LD_DY	0*SIZE(ptrba), yvec0;
BROAD_DY 0*SIZE(ptrbb), yvec1;
MUL_DY	yvec0, yvec1, yvec1;
ADD_DY	yvec1, yvec15, yvec15;
ADDQ	$4*SIZE, ptrba;
ADDQ	$1*SIZE, ptrbb;

.L323_loopE:
//#### Multiply Alpha ####
BROAD_DY MEMALPHA, yvec7;
MUL_DY	yvec7, yvec15, yvec15;
//#### Testing Alignment ####
MOVQ	C0, %rax;
OR		ldc, %rax;
TEST	$15, %rax;
JNE	.L323_loopEx;
ALIGN_5
//#### Writing Back ####
EXTRA_DY $1, yvec15, xvec14;
#ifndef	TRMMKERNEL
ADD_DX 0*SIZE(C0), xvec15, xvec15;
ADD_DX 2*SIZE(C0), xvec14, xvec14;
#endif
ST_DX	xvec15, 0*SIZE(C0);
ST_DX	xvec14, 2*SIZE(C0);
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
JMP .L32_loopE;
ALIGN_5
.L323_loopEx:
//#### Writing Back ####
EXTRA_DY $1, yvec15, xvec14;
#ifndef	TRMMKERNEL
LDL_DX 0*SIZE(C0), xvec13, xvec13;
LDH_DX 1*SIZE(C0), xvec13, xvec13;
LDL_DX 2*SIZE(C0), xvec12, xvec12;
LDH_DX 3*SIZE(C0), xvec12, xvec12;
ADD_DX xvec13, xvec15, xvec15;
ADD_DX xvec12, xvec14, xvec14;
#endif
STL_DX	xvec15, 0*SIZE(C0);
STH_DX	xvec15, 1*SIZE(C0);
STL_DX	xvec14, 2*SIZE(C0);
STH_DX	xvec14, 3*SIZE(C0);
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
.L32_loopE:
TEST $2, bm
JLE .L33_loopE;
ALIGN_5
.L33_bodyB:
#if !defined(TRMMKERNEL) || (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
MOVQ bb, ptrbb;
#else
MOVQ bb, ptrbb;
MOVQ kk, %rax
LEAQ (, %rax, SIZE), %rax
LEAQ (ptrba, %rax, 2), ptrba
ADDQ %rax, ptrbb;
#endif
//#### Initial Result ####
XOR_DY	yvec15, yvec15, yvec15;
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
JLE .L331_loopE;
ALIGN_5
.L331_bodyB:
LD_DX	0*SIZE(ptrba), xvec0;
BROAD_DX 0*SIZE(ptrbb), xvec2;
MUL_DX xvec0, xvec2, xvec2;
ADD_DX xvec2, xvec15, xvec15;

LD_DX	2*SIZE(ptrba), xvec1;
BROAD_DX 1*SIZE(ptrbb), xvec3;
MUL_DX xvec1, xvec3, xvec3;
ADD_DX xvec3, xvec15, xvec15;

LD_DX	4*SIZE(ptrba), xvec4;
BROAD_DX 2*SIZE(ptrbb), xvec5;
MUL_DX xvec4, xvec5, xvec5;
ADD_DX xvec5, xvec15, xvec15;

LD_DX	6*SIZE(ptrba), xvec6;
BROAD_DX 3*SIZE(ptrbb), xvec7;
MUL_DX xvec6, xvec7, xvec7;
ADD_DX xvec7, xvec15, xvec15;
ADDQ	$8*SIZE, ptrba;
ADDQ	$4*SIZE, ptrbb;
DECQ	k;
JG	.L331_bodyB;
ALIGN_5
.L331_loopE:
#ifndef	TRMMKERNEL
TEST $2,bk;
#else
MOVQ kkk, %rax;
TEST $2, %rax
#endif
JLE .L332_loopE;
ALIGN_5
.L332_bodyB:
LD_DX	0*SIZE(ptrba), xvec0;
BROAD_DX 0*SIZE(ptrbb), xvec2;
MUL_DX xvec0, xvec2, xvec2;
ADD_DX xvec2, xvec15, xvec15;

LD_DX	2*SIZE(ptrba), xvec1;
BROAD_DX 1*SIZE(ptrbb), xvec3;
MUL_DX xvec1, xvec3, xvec3;
ADD_DX xvec3, xvec15, xvec15;
ADDQ	$4*SIZE, ptrba;
ADDQ	$2*SIZE, ptrbb;
.L332_loopE:
#ifndef	TRMMKERNEL
TEST $1, bk;
#else
MOVQ kkk, %rax;
TEST $1, %rax;
#endif
JLE .L333_loopE;
ALIGN_5
.L333_bodyB:
LD_DX	0*SIZE(ptrba), xvec0;
BROAD_DX 0*SIZE(ptrbb), xvec2;
MUL_DX xvec0, xvec2, xvec2;
ADD_DX xvec2, xvec15, xvec15;
ADDQ	$2*SIZE, ptrba;
ADDQ	$1*SIZE, ptrbb;
.L333_loopE:
//#### Multiply Alpha ####
BROAD_DX MEMALPHA, xvec7;
MUL_DX xvec7, xvec15, xvec15;
#ifndef	TRMMKERNEL
LDL_DX 0*SIZE(C0), xvec14, xvec14;
LDH_DX 1*SIZE(C0), xvec14, xvec14;
ADD_DX xvec14, xvec15, xvec15;
#endif
STL_DX	xvec15, 0*SIZE(C0);
STH_DX	xvec15, 1*SIZE(C0);
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
ADDQ	$2*SIZE, C0;
.L33_loopE:
TEST $1, bm
JLE .L34_loopE;
ALIGN_5
.L34_bodyB:
#if !defined(TRMMKERNEL) || (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
MOVQ bb, ptrbb;
#else
MOVQ bb, ptrbb;
MOVQ kk, %rax;
LEAQ (, %rax, SIZE), %rax;
ADDQ %rax, ptrba;
ADDQ %rax, ptrbb;
#endif
XOR_DY	yvec15, yvec15, yvec15;
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
JLE .L341_loopE;
ALIGN_5
.L341_bodyB:
vmovsd	0*SIZE(ptrba), xvec0;
vmovsd 	0*SIZE(ptrbb), xvec1;
vmulsd	xvec0, xvec1, xvec1;
vaddsd	xvec1, xvec15, xvec15;

vmovsd	1*SIZE(ptrba), xvec0;
vmovsd 	1*SIZE(ptrbb), xvec1;
vmulsd	xvec0, xvec1, xvec1;
vaddsd	xvec1, xvec15, xvec15;

vmovsd	2*SIZE(ptrba), xvec0;
vmovsd 	2*SIZE(ptrbb), xvec1;
vmulsd	xvec0, xvec1, xvec1;
vaddsd	xvec1, xvec15, xvec15;

vmovsd	3*SIZE(ptrba), xvec0;
vmovsd 	3*SIZE(ptrbb), xvec1;
vmulsd	xvec0, xvec1, xvec1;
vaddsd	xvec1, xvec15, xvec15;
addq	$4*SIZE, ptrba;
addq	$4*SIZE, ptrbb;
decq	k;
JG	.L341_bodyB;
ALIGN_5
.L341_loopE:
#ifndef	TRMMKERNEL
TEST $2, bk;
#else
MOVQ kkk, %rax;
TEST $2, %rax;
#endif
JLE .L342_loopE;
ALIGN_5
.L342_bodyB:
vmovsd	0*SIZE(ptrba), xvec0;
vmovsd 	0*SIZE(ptrbb), xvec1;
vmulsd	xvec0, xvec1, xvec1;
vaddsd	xvec1, xvec15, xvec15;

vmovsd	1*SIZE(ptrba), xvec0;
vmovsd 	1*SIZE(ptrbb), xvec1;
vmulsd	xvec0, xvec1, xvec1;
vaddsd	xvec1, xvec15, xvec15;
addq	$2*SIZE, ptrba;
addq	$2*SIZE, ptrbb;

.L342_loopE:
#ifndef	TRMMKERNEL
TEST $1, bk
#else
MOVQ kkk, %rax;
TEST $1, %rax;
#endif
JLE .L343_loopE;
ALIGN_5
.L343_bodyB:
vmovsd	0*SIZE(ptrba), xvec0;
vmovsd 	0*SIZE(ptrbb), xvec1;
vmulsd	xvec0, xvec1, xvec1;
vaddsd	xvec1, xvec15, xvec15;
addq	$1*SIZE, ptrba;
addq	$1*SIZE, ptrbb;

.L343_loopE:
//#### Writing Back ####
vmovsd	MEMALPHA, xvec7;
vmulsd	xvec7, xvec15, xvec15;
#ifndef	TRMMKERNEL
vmovsd	0*SIZE(C0), xvec0;
vaddsd	xvec0, xvec15, xvec15;
#endif
movsd	xvec15, 0*SIZE(C0);
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
.L34_loopE:
MOVQ bk, k
SALQ $3, k;
ADDQ k, bb;
LEAQ (C, ldc, 1), C;

.L30_loopE:
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
