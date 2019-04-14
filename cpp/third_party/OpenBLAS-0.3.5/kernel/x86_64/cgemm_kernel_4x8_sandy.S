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

#define	old_bm		%rdi
#define old_bn		%rsi
#define	old_bk		%rdx

#define bm	%r13
#define bn	%r14
#define bk	%r15

#define ALPHA	%xmm0
#define ba		%rcx
#define bb		%r8
#define C		%r9
#define ldc		%r10

#define i	%r11
#define k	%rax

#define ptrba	%rdi
#define ptrbb	%rsi
#define C0		%rbx
#define C1		%rbp

#define prebb	%r12

#ifndef WINDOWS_ABI

#define STACKSIZE 128

#define old_ldc	8+STACKSIZE(%rsp)
#define old_offset	16+STACKSIZE(%rsp)

#define MEMALPHA_R	48(%rsp)
#define MEMALPHA_I	56(%rsp)
#define j	64(%rsp)
#define OFFSET	72(%rsp)
#define kk	80(%rsp)
#define kkk	88(%rsp)

#else

#define STACKSIZE 512

#define OLD_ALPHA_I	40 + STACKSIZE(%rsp)
#define OLD_A		48 + STACKSIZE(%rsp)
#define OLD_B		56 + STACKSIZE(%rsp)
#define OLD_C		64 + STACKSIZE(%rsp)
#define old_ldc		72 + STACKSIZE(%rsp)
#define old_offset	80 + STACKSIZE(%rsp)

#define MEMALPHA_R	  224(%rsp)
#define MEMALPHA_I	  232(%rsp)
#define j	  240(%rsp)
#define OFFSET	  248(%rsp)
#define kk	  256(%rsp)
#define kkk	  264(%rsp)

#endif

#define PREFETCH0	prefetcht0
#define PREFETCH1	prefetcht0
#define PREFETCH2	prefetcht0
#define PRESIZE	64

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
#define SUB_SY	vsubps
#define SUB_SX	vsubps

#define ADDSUB_SY	vaddsubps
#define ADDSUB_SX	vaddsubps

#define MUL_SY	vmulps
#define MUL_SX	vmulps

#define SHUF_SY	vperm2f128
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

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
#define ADD1_SY ADD_SY
#define ADD2_SY	ADDSUB_SY
#define ADD1_SX	ADD_SX
#define ADD2_SX	ADDSUB_SX
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
#define ADD1_SY SUB_SY
#define ADD2_SY	ADDSUB_SY
#define ADD1_SX	SUB_SX
#define ADD2_SX	ADDSUB_SX
#elif  defined(RN) || defined(RT) || defined(CN) || defined(CT)
#define ADD1_SY SUB_SY
#define ADD2_SY	ADDSUB_SY
#define ADD1_SX	SUB_SX
#define ADD2_SX	ADDSUB_SX
#else
#define ADD1_SY ADD_SY
#define ADD2_SY	ADDSUB_SY
#define ADD1_SX	ADD_SX
#define ADD2_SX	ADDSUB_SX
#endif

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
	movsd	OLD_ALPHA_I, %xmm1
#else
movq	old_ldc, ldc
#ifdef	TRMMKERNEL
movq	old_offset, %r11;
#endif
#endif

vzeroupper

vmovlps	%xmm0, MEMALPHA_R
vmovlps	%xmm1, MEMALPHA_I
movq	old_bm,	bm
movq	old_bn, bn
movq	old_bk, bk
salq	$ZBASE_SHIFT, ldc
#ifdef	TRMMKERNEL
movq	%r11, OFFSET
#ifndef	LEFT
negq	%r11;
#endif
movq	%r11, kk;
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
LEAQ (bb, k, 1), prebb;				# Rn=4, SIZE=4 COMPLEX=2
MOVQ ba,ptrba;
MOVQ bm,i;
SARQ $3,i;							# Rm = 8
JLE .L1_loopE;
ALIGN_5;
.L1_bodyB:;
#if !defined(TRMMKERNEL)||(defined(TRMMKERNEL)&&defined(LEFT)&&defined(TRANSA))||(defined(TRMMKERNEL)&&!defined(LEFT)&&!defined(TRANSA))
MOVQ bb,ptrbb;
#else
MOVQ bb, ptrbb;
MOVQ kk, %rax;
SALQ $ZBASE_SHIFT, %rax;
LEAQ (ptrba, %rax, 8), ptrba;
LEAQ (ptrbb, %rax, 4), ptrbb;
#endif
# Initial results register
PREFETCH0	0*SIZE(prebb);
XOR_SY	yvec15, yvec15, yvec15;
PREFETCH0	16*SIZE(prebb);
ADDQ	$32*SIZE, prebb;
XOR_SY	yvec14, yvec14, yvec14;
PREFETCH2	3*SIZE(C0);
XOR_SY	yvec13, yvec13, yvec13;
PREFETCH2	3*SIZE(C0, ldc, 1);
XOR_SY	yvec12, yvec12, yvec12;
PREFETCH2	3*SIZE(C1);
EDUP_SY 0*SIZE(ptrbb), yvec2;		# Br0, Br1, Br2, Br3
PREFETCH2	3*SIZE(C1, ldc, 1);
XOR_SY	yvec11, yvec11, yvec11;
XOR_SY	yvec10, yvec10, yvec10;
LD_SY 	0*SIZE(ptrba), yvec0;			# Ar0, Ai0, Ar1, Ai1..
XOR_SY	yvec9, yvec9, yvec9;
XOR_SY	yvec8, yvec8, yvec8;
VPERMILP_SY	$0x4e, yvec2, yvec3;	# Br2, Br3, Br0, Br1
#ifndef	TRMMKERNEL
MOVQ bk,k;
#elif (defined(LEFT)&&!defined(TRANSA))||(!defined(LEFT)&&defined(TRANSA))
MOVQ bk, %rax;
SUBQ kk, %rax;
MOVQ %rax, kkk;
#else
MOVQ kk, %rax;
#ifdef	LEFT
ADDQ $8, %rax;
#else
ADDQ $4, %rax;
#endif
MOVQ %rax, kkk;
#endif
SARQ $2,k;							# Unroll 4 times
JLE .L2_loopE;
ALIGN_5;
.L2_bodyB:;
# Computing kernel

######### Unroll 1 ##################
PREFETCH0	PRESIZE*SIZE(ptrba);
LD_SY 8*SIZE(ptrba), yvec1;			# Ar4, Ai4, Ar5, Ai5..
MUL_SY	yvec0, yvec2, yvec6;
SHUF_SY	$0x03, yvec2, yvec2, yvec4;	# Br1, Br0, Br3, Br2
MUL_SY	yvec0, yvec3, yvec7;
SHUF_SY $0x03, yvec3, yvec3, yvec5;	# Br3, Br2, Br1, Br0
ADD1_SY	yvec6, yvec15, yvec15;
ADD1_SY	yvec7, yvec13, yvec13;

MUL_SY	yvec1, yvec2, yvec6;
ODUP_SY	0*SIZE(ptrbb), yvec2;		# Bi0, Bi1, Bi2, Bi3
MUL_SY	yvec1, yvec3, yvec7;
VPERMILP_SY	$0x4e, yvec2, yvec3;	# Bi2, Bi3, Bi0, Bi1
ADD1_SY	yvec6, yvec14, yvec14;
ADD1_SY	yvec7, yvec12, yvec12;

MUL_SY	yvec0, yvec4, yvec6;
MUL_SY 	yvec0, yvec5, yvec7;
VPERMILP_SY $0xb1, yvec0, yvec0;	# Ai0, Ar0, Ai1, Ar1..
ADD1_SY	yvec6, yvec11, yvec11;
ADD1_SY	yvec7, yvec9, yvec9;

MUL_SY	yvec1, yvec4, yvec6;
SHUF_SY	$0x03, yvec2, yvec2, yvec4;	# Br1, Br0, Br3, Br2
MUL_SY	yvec1, yvec5, yvec7;
SHUF_SY $0x03, yvec3, yvec3, yvec5;	# Br3, Br2, Br1, Br0
ADD1_SY	yvec6, yvec10, yvec10;
ADD1_SY	yvec7, yvec8, yvec8;

VPERMILP_SY $0xb1, yvec1, yvec1;
MUL_SY	yvec0, yvec2, yvec6;
MUL_SY	yvec0, yvec3, yvec7;
ADD2_SY	yvec6, yvec15, yvec15;
ADD2_SY	yvec7, yvec13, yvec13;

MUL_SY	yvec1, yvec2, yvec6;
EDUP_SY	8*SIZE(ptrbb), yvec2;
MUL_SY	yvec1, yvec3, yvec7;
VPERMILP_SY $0x4e, yvec2, yvec3;
ADD2_SY	yvec6, yvec14, yvec14;
ADD2_SY	yvec7, yvec12, yvec12;

MUL_SY	yvec0, yvec4, yvec6;
MUL_SY	yvec0, yvec5, yvec7;
LD_SY	16*SIZE(ptrba), yvec0;
ADD2_SY	yvec6, yvec11, yvec11;
ADD2_SY	yvec7, yvec9, yvec9;

MUL_SY	yvec1, yvec4, yvec6;
MUL_SY	yvec1, yvec5, yvec7;
ADD2_SY	yvec6, yvec10, yvec10;
ADD2_SY	yvec7, yvec8, yvec8;

######### Unroll 2 ##################
PREFETCH0	(PRESIZE+16)*SIZE(ptrba);
LD_SY 	24*SIZE(ptrba), yvec1;			# Ar4, Ai4, Ar5, Ai5..
MUL_SY	yvec0, yvec2, yvec6;
SHUF_SY	$0x03, yvec2, yvec2, yvec4;	# Br1, Br0, Br3, Br2
MUL_SY	yvec0, yvec3, yvec7;
SHUF_SY $0x03, yvec3, yvec3, yvec5;	# Br3, Br2, Br1, Br0
ADD1_SY	yvec6, yvec15, yvec15;
ADD1_SY	yvec7, yvec13, yvec13;

MUL_SY	yvec1, yvec2, yvec6;
ODUP_SY	8*SIZE(ptrbb), yvec2;		# Bi0, Bi1, Bi2, Bi3
MUL_SY	yvec1, yvec3, yvec7;
VPERMILP_SY	$0x4e, yvec2, yvec3;	# Bi2, Bi3, Bi0, Bi1
ADD1_SY	yvec6, yvec14, yvec14;
ADD1_SY	yvec7, yvec12, yvec12;

MUL_SY	yvec0, yvec4, yvec6;
MUL_SY 	yvec0, yvec5, yvec7;
VPERMILP_SY $0xb1, yvec0, yvec0;	# Ai0, Ar0, Ai1, Ar1..
ADD1_SY	yvec6, yvec11, yvec11;
ADD1_SY	yvec7, yvec9, yvec9;

MUL_SY	yvec1, yvec4, yvec6;
SHUF_SY	$0x03, yvec2, yvec2, yvec4;	# Br1, Br0, Br3, Br2
MUL_SY	yvec1, yvec5, yvec7;
SHUF_SY $0x03, yvec3, yvec3, yvec5;	# Br3, Br2, Br1, Br0
ADD1_SY	yvec6, yvec10, yvec10;
ADD1_SY	yvec7, yvec8, yvec8;

VPERMILP_SY $0xb1, yvec1, yvec1;
MUL_SY	yvec0, yvec2, yvec6;
MUL_SY	yvec0, yvec3, yvec7;
ADD2_SY	yvec6, yvec15, yvec15;
ADD2_SY	yvec7, yvec13, yvec13;

MUL_SY	yvec1, yvec2, yvec6;
EDUP_SY	16*SIZE(ptrbb), yvec2;
MUL_SY	yvec1, yvec3, yvec7;
VPERMILP_SY $0x4e, yvec2, yvec3;
ADD2_SY	yvec6, yvec14, yvec14;
ADD2_SY	yvec7, yvec12, yvec12;

MUL_SY	yvec0, yvec4, yvec6;
MUL_SY	yvec0, yvec5, yvec7;
LD_SY	32*SIZE(ptrba), yvec0;
ADD2_SY	yvec6, yvec11, yvec11;
ADD2_SY	yvec7, yvec9, yvec9;

MUL_SY	yvec1, yvec4, yvec6;
MUL_SY	yvec1, yvec5, yvec7;
ADD2_SY	yvec6, yvec10, yvec10;
ADD2_SY	yvec7, yvec8, yvec8;

######### Unroll 3 ##################
PREFETCH0	(PRESIZE+32)*SIZE(ptrba);
LD_SY 	40*SIZE(ptrba), yvec1;			# Ar4, Ai4, Ar5, Ai5..
MUL_SY	yvec0, yvec2, yvec6;
SHUF_SY	$0x03, yvec2, yvec2, yvec4;	# Br1, Br0, Br3, Br2
MUL_SY	yvec0, yvec3, yvec7;
SHUF_SY $0x03, yvec3, yvec3, yvec5;	# Br3, Br2, Br1, Br0
ADD1_SY	yvec6, yvec15, yvec15;
ADD1_SY	yvec7, yvec13, yvec13;

MUL_SY	yvec1, yvec2, yvec6;
ODUP_SY	16*SIZE(ptrbb), yvec2;		# Bi0, Bi1, Bi2, Bi3
MUL_SY	yvec1, yvec3, yvec7;
VPERMILP_SY	$0x4e, yvec2, yvec3;	# Bi2, Bi3, Bi0, Bi1
ADD1_SY	yvec6, yvec14, yvec14;
ADD1_SY	yvec7, yvec12, yvec12;

MUL_SY	yvec0, yvec4, yvec6;
MUL_SY 	yvec0, yvec5, yvec7;
VPERMILP_SY $0xb1, yvec0, yvec0;	# Ai0, Ar0, Ai1, Ar1..
ADD1_SY	yvec6, yvec11, yvec11;
ADD1_SY	yvec7, yvec9, yvec9;

MUL_SY	yvec1, yvec4, yvec6;
SHUF_SY	$0x03, yvec2, yvec2, yvec4;	# Br1, Br0, Br3, Br2
MUL_SY	yvec1, yvec5, yvec7;
SHUF_SY $0x03, yvec3, yvec3, yvec5;	# Br3, Br2, Br1, Br0
ADD1_SY	yvec6, yvec10, yvec10;
ADD1_SY	yvec7, yvec8, yvec8;

VPERMILP_SY $0xb1, yvec1, yvec1;
MUL_SY	yvec0, yvec2, yvec6;
MUL_SY	yvec0, yvec3, yvec7;
ADD2_SY	yvec6, yvec15, yvec15;
ADD2_SY	yvec7, yvec13, yvec13;

MUL_SY	yvec1, yvec2, yvec6;
EDUP_SY	24*SIZE(ptrbb), yvec2;
MUL_SY	yvec1, yvec3, yvec7;
VPERMILP_SY $0x4e, yvec2, yvec3;
ADD2_SY	yvec6, yvec14, yvec14;
ADD2_SY	yvec7, yvec12, yvec12;

MUL_SY	yvec0, yvec4, yvec6;
MUL_SY	yvec0, yvec5, yvec7;
LD_SY	48*SIZE(ptrba), yvec0;
ADD2_SY	yvec6, yvec11, yvec11;
ADD2_SY	yvec7, yvec9, yvec9;

MUL_SY	yvec1, yvec4, yvec6;
MUL_SY	yvec1, yvec5, yvec7;
ADD2_SY	yvec6, yvec10, yvec10;
ADD2_SY	yvec7, yvec8, yvec8;

######### Unroll 4 ##################
PREFETCH0	(PRESIZE+48)*SIZE(ptrba);
LD_SY 	56*SIZE(ptrba), yvec1;			# Ar4, Ai4, Ar5, Ai5..
MUL_SY	yvec0, yvec2, yvec6;
SHUF_SY	$0x03, yvec2, yvec2, yvec4;	# Br1, Br0, Br3, Br2
MUL_SY	yvec0, yvec3, yvec7;
SHUF_SY $0x03, yvec3, yvec3, yvec5;	# Br3, Br2, Br1, Br0
ADDQ	$64*SIZE, ptrba;
ADD1_SY	yvec6, yvec15, yvec15;
ADD1_SY	yvec7, yvec13, yvec13;

MUL_SY	yvec1, yvec2, yvec6;
ODUP_SY	24*SIZE(ptrbb), yvec2;		# Bi0, Bi1, Bi2, Bi3
MUL_SY	yvec1, yvec3, yvec7;
VPERMILP_SY	$0x4e, yvec2, yvec3;	# Bi2, Bi3, Bi0, Bi1
ADDQ	$32*SIZE, ptrbb;
ADD1_SY	yvec6, yvec14, yvec14;
ADD1_SY	yvec7, yvec12, yvec12;

MUL_SY	yvec0, yvec4, yvec6;
MUL_SY 	yvec0, yvec5, yvec7;
VPERMILP_SY $0xb1, yvec0, yvec0;	# Ai0, Ar0, Ai1, Ar1..
ADD1_SY	yvec6, yvec11, yvec11;
ADD1_SY	yvec7, yvec9, yvec9;

MUL_SY	yvec1, yvec4, yvec6;
SHUF_SY	$0x03, yvec2, yvec2, yvec4;	# Br1, Br0, Br3, Br2
MUL_SY	yvec1, yvec5, yvec7;
SHUF_SY $0x03, yvec3, yvec3, yvec5;	# Br3, Br2, Br1, Br0
ADD1_SY	yvec6, yvec10, yvec10;
ADD1_SY	yvec7, yvec8, yvec8;

VPERMILP_SY $0xb1, yvec1, yvec1;
MUL_SY	yvec0, yvec2, yvec6;
MUL_SY	yvec0, yvec3, yvec7;
ADD2_SY	yvec6, yvec15, yvec15;
ADD2_SY	yvec7, yvec13, yvec13;

MUL_SY	yvec1, yvec2, yvec6;
EDUP_SY	0*SIZE(ptrbb), yvec2;
MUL_SY	yvec1, yvec3, yvec7;
VPERMILP_SY $0x4e, yvec2, yvec3;
ADD2_SY	yvec6, yvec14, yvec14;
ADD2_SY	yvec7, yvec12, yvec12;

MUL_SY	yvec0, yvec4, yvec6;
MUL_SY	yvec0, yvec5, yvec7;
LD_SY	0*SIZE(ptrba), yvec0;
ADD2_SY	yvec6, yvec11, yvec11;
ADD2_SY	yvec7, yvec9, yvec9;

MUL_SY	yvec1, yvec4, yvec6;
MUL_SY	yvec1, yvec5, yvec7;
ADD2_SY	yvec6, yvec10, yvec10;
ADD2_SY	yvec7, yvec8, yvec8;
.L2_bodyE:;
DECQ k;
JG .L2_bodyB;
ALIGN_5
.L2_loopE:;
#ifndef	TRMMKERNEL
TEST	$2, bk;
#else
TEST 	$2, kkk;
#endif
JLE		.L3_loopE;
ALIGN_5
.L3_loopB:
######### Unroll 1 ##################
PREFETCH0	PRESIZE*SIZE(ptrba)
LD_SY 8*SIZE(ptrba), yvec1;			# Ar4, Ai4, Ar5, Ai5..
MUL_SY	yvec0, yvec2, yvec6;
MUL_SY	yvec0, yvec3, yvec7;
SHUF_SY	$0x03, yvec2, yvec2, yvec4;	# Br1, Br0, Br3, Br2
ADD1_SY	yvec6, yvec15, yvec15;
ADD1_SY	yvec7, yvec13, yvec13;

MUL_SY	yvec1, yvec2, yvec6;
MUL_SY	yvec1, yvec3, yvec7;
SHUF_SY $0x03, yvec3, yvec3, yvec5;	# Br3, Br2, Br1, Br0
ADD1_SY	yvec6, yvec14, yvec14;
ADD1_SY	yvec7, yvec12, yvec12;

ODUP_SY	0*SIZE(ptrbb), yvec2;		# Bi0, Bi1, Bi2, Bi3
MUL_SY	yvec0, yvec4, yvec6;
MUL_SY 	yvec0, yvec5, yvec7;
VPERMILP_SY	$0x4e, yvec2, yvec3;	# Bi2, Bi3, Bi0, Bi1
ADD1_SY	yvec6, yvec11, yvec11;
ADD1_SY	yvec7, yvec9, yvec9;

MUL_SY	yvec1, yvec4, yvec6;
MUL_SY	yvec1, yvec5, yvec7;
VPERMILP_SY $0xb1, yvec0, yvec0;	# Ai0, Ar0, Ai1, Ar1..
ADD1_SY	yvec6, yvec10, yvec10;
ADD1_SY	yvec7, yvec8, yvec8;

VPERMILP_SY $0xb1, yvec1, yvec1;
MUL_SY	yvec0, yvec2, yvec6;
MUL_SY	yvec0, yvec3, yvec7;
SHUF_SY	$0x03, yvec2, yvec2, yvec4;	# Br1, Br0, Br3, Br2
ADD2_SY	yvec6, yvec15, yvec15;
ADD2_SY	yvec7, yvec13, yvec13;

MUL_SY	yvec1, yvec2, yvec6;
MUL_SY	yvec1, yvec3, yvec7;
SHUF_SY $0x03, yvec3, yvec3, yvec5;	# Br3, Br2, Br1, Br0
ADD2_SY	yvec6, yvec14, yvec14;
ADD2_SY	yvec7, yvec12, yvec12;

EDUP_SY	8*SIZE(ptrbb), yvec2;
MUL_SY	yvec0, yvec4, yvec6;
MUL_SY	yvec0, yvec5, yvec7;
VPERMILP_SY $0x4e, yvec2, yvec3;
ADD2_SY	yvec6, yvec11, yvec11;
ADD2_SY	yvec7, yvec9, yvec9;

LD_SY	16*SIZE(ptrba), yvec0;
MUL_SY	yvec1, yvec4, yvec6;
MUL_SY	yvec1, yvec5, yvec7;
ADD2_SY	yvec6, yvec10, yvec10;
ADD2_SY	yvec7, yvec8, yvec8;

######### Unroll 2 ##################
PREFETCH0	(PRESIZE+16)*SIZE(ptrba)
LD_SY 	24*SIZE(ptrba), yvec1;			# Ar4, Ai4, Ar5, Ai5..
MUL_SY	yvec0, yvec2, yvec6;
MUL_SY	yvec0, yvec3, yvec7;
ADDQ	$32*SIZE, ptrba
SHUF_SY	$0x03, yvec2, yvec2, yvec4;	# Br1, Br0, Br3, Br2
ADD1_SY	yvec6, yvec15, yvec15;
ADD1_SY	yvec7, yvec13, yvec13;

MUL_SY	yvec1, yvec2, yvec6;
MUL_SY	yvec1, yvec3, yvec7;
SHUF_SY $0x03, yvec3, yvec3, yvec5;	# Br3, Br2, Br1, Br0
ADD1_SY	yvec6, yvec14, yvec14;
ADD1_SY	yvec7, yvec12, yvec12;

ODUP_SY	8*SIZE(ptrbb), yvec2;		# Bi0, Bi1, Bi2, Bi3
MUL_SY	yvec0, yvec4, yvec6;
MUL_SY 	yvec0, yvec5, yvec7;
ADDQ	$16*SIZE, ptrbb;
VPERMILP_SY	$0x4e, yvec2, yvec3;	# Bi2, Bi3, Bi0, Bi1
ADD1_SY	yvec6, yvec11, yvec11;
ADD1_SY	yvec7, yvec9, yvec9;

MUL_SY	yvec1, yvec4, yvec6;
MUL_SY	yvec1, yvec5, yvec7;
VPERMILP_SY $0xb1, yvec0, yvec0;	# Ai0, Ar0, Ai1, Ar1..
ADD1_SY	yvec6, yvec10, yvec10;
ADD1_SY	yvec7, yvec8, yvec8;

VPERMILP_SY $0xb1, yvec1, yvec1;
MUL_SY	yvec0, yvec2, yvec6;
MUL_SY	yvec0, yvec3, yvec7;
SHUF_SY	$0x03, yvec2, yvec2, yvec4;	# Br1, Br0, Br3, Br2
ADD2_SY	yvec6, yvec15, yvec15;
ADD2_SY	yvec7, yvec13, yvec13;

MUL_SY	yvec1, yvec2, yvec6;
MUL_SY	yvec1, yvec3, yvec7;
SHUF_SY $0x03, yvec3, yvec3, yvec5;	# Br3, Br2, Br1, Br0
ADD2_SY	yvec6, yvec14, yvec14;
ADD2_SY	yvec7, yvec12, yvec12;

EDUP_SY	0*SIZE(ptrbb), yvec2;
MUL_SY	yvec0, yvec4, yvec6;
MUL_SY	yvec0, yvec5, yvec7;
VPERMILP_SY $0x4e, yvec2, yvec3;
ADD2_SY	yvec6, yvec11, yvec11;
ADD2_SY	yvec7, yvec9, yvec9;

LD_SY	0*SIZE(ptrba), yvec0;
MUL_SY	yvec1, yvec4, yvec6;
MUL_SY	yvec1, yvec5, yvec7;
ADD2_SY	yvec6, yvec10, yvec10;
ADD2_SY	yvec7, yvec8, yvec8;
.L3_loopE:
#ifndef	TRMMKERNEL
TEST	$1, bk;
#else
TEST 	$1, kkk;
#endif
JLE		.L4_loopE;
ALIGN_5
.L4_loopB:;
######### Unroll 1 ##################
PREFETCH0	PRESIZE*SIZE(ptrba)
LD_SY	8*SIZE(ptrba), yvec1;			# Ar4, Ai4, Ar5, Ai5..
MUL_SY	yvec0, yvec2, yvec6;
MUL_SY	yvec0, yvec3, yvec7;
ADDQ	$16*SIZE, ptrba;
SHUF_SY	$0x03, yvec2, yvec2, yvec4;	# Br1, Br0, Br3, Br2
ADD1_SY	yvec6, yvec15, yvec15;
ADD1_SY	yvec7, yvec13, yvec13;

MUL_SY	yvec1, yvec2, yvec6;
MUL_SY	yvec1, yvec3, yvec7;
SHUF_SY $0x03, yvec3, yvec3, yvec5;	# Br3, Br2, Br1, Br0
ADD1_SY	yvec6, yvec14, yvec14;
ADD1_SY	yvec7, yvec12, yvec12;

ODUP_SY	0*SIZE(ptrbb), yvec2;		# Bi0, Bi1, Bi2, Bi3
MUL_SY	yvec0, yvec4, yvec6;
MUL_SY 	yvec0, yvec5, yvec7;
ADDQ	$8*SIZE, ptrbb;
VPERMILP_SY	$0x4e, yvec2, yvec3;	# Bi2, Bi3, Bi0, Bi1
ADD1_SY	yvec6, yvec11, yvec11;
ADD1_SY	yvec7, yvec9, yvec9;

MUL_SY	yvec1, yvec4, yvec6;
MUL_SY	yvec1, yvec5, yvec7;
VPERMILP_SY $0xb1, yvec0, yvec0;	# Ai0, Ar0, Ai1, Ar1..
ADD1_SY	yvec6, yvec10, yvec10;
ADD1_SY	yvec7, yvec8, yvec8;

VPERMILP_SY $0xb1, yvec1, yvec1;
MUL_SY	yvec0, yvec2, yvec6;
MUL_SY	yvec0, yvec3, yvec7;
SHUF_SY	$0x03, yvec2, yvec2, yvec4;	# Br1, Br0, Br3, Br2
ADD2_SY	yvec6, yvec15, yvec15;
ADD2_SY	yvec7, yvec13, yvec13;

MUL_SY	yvec1, yvec2, yvec6;
ADD2_SY	yvec6, yvec14, yvec14;
SHUF_SY $0x03, yvec3, yvec3, yvec5;	# Br3, Br2, Br1, Br0
MUL_SY	yvec1, yvec3, yvec7;
ADD2_SY	yvec7, yvec12, yvec12;

MUL_SY	yvec0, yvec4, yvec6;
MUL_SY	yvec0, yvec5, yvec7;
VPERMILP_SY $0x4e, yvec2, yvec3;
ADD2_SY	yvec6, yvec11, yvec11;
ADD2_SY	yvec7, yvec9, yvec9;

MUL_SY	yvec1, yvec4, yvec6;
MUL_SY	yvec1, yvec5, yvec7;
ADD2_SY	yvec6, yvec10, yvec10;
ADD2_SY	yvec7, yvec8, yvec8;

.L4_loopE:;
#### Handle ####
XOR_SY	yvec7, yvec7, yvec7;
#if  defined(RN) || defined(RT) || defined(CN) || defined(CT)
ADDSUB_SY	yvec15, yvec7, yvec15;
ADDSUB_SY	yvec14, yvec7, yvec14;
ADDSUB_SY	yvec13, yvec7, yvec13;
ADDSUB_SY	yvec12, yvec7, yvec12;
ADDSUB_SY	yvec11, yvec7, yvec11;
ADDSUB_SY	yvec10, yvec7, yvec10;
ADDSUB_SY	yvec9, yvec7, yvec9;
ADDSUB_SY	yvec8, yvec7, yvec8;
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
SUB_SY	yvec15, yvec7, yvec15;
SUB_SY	yvec14, yvec7, yvec14;
SUB_SY	yvec13, yvec7, yvec13;
SUB_SY	yvec12, yvec7, yvec12;
SUB_SY	yvec11, yvec7, yvec11;
SUB_SY	yvec10, yvec7, yvec10;
SUB_SY	yvec9, yvec7, yvec9;
SUB_SY	yvec8, yvec7, yvec8;
#elif defined(RR) || defined(RC) || defined(CR) || defined(CC)
VPERMILP_SY $0xb1, yvec15, yvec15;
VPERMILP_SY $0xb1, yvec14, yvec14;
VPERMILP_SY $0xb1, yvec13, yvec13;
VPERMILP_SY $0xb1, yvec12, yvec12;
VPERMILP_SY $0xb1, yvec11, yvec11;
VPERMILP_SY $0xb1, yvec10, yvec10;
VPERMILP_SY $0xb1, yvec9, yvec9;
VPERMILP_SY $0xb1, yvec8, yvec8;
ADDSUB_SY	yvec15, yvec7, yvec15;
ADDSUB_SY	yvec14, yvec7, yvec14;
ADDSUB_SY	yvec13, yvec7, yvec13;
ADDSUB_SY	yvec12, yvec7, yvec12;
ADDSUB_SY	yvec11, yvec7, yvec11;
ADDSUB_SY	yvec10, yvec7, yvec10;
ADDSUB_SY	yvec9, yvec7, yvec9;
ADDSUB_SY	yvec8, yvec7, yvec8;
VPERMILP_SY $0xb1, yvec15, yvec15;
VPERMILP_SY $0xb1, yvec14, yvec14;
VPERMILP_SY $0xb1, yvec13, yvec13;
VPERMILP_SY $0xb1, yvec12, yvec12;
VPERMILP_SY $0xb1, yvec11, yvec11;
VPERMILP_SY $0xb1, yvec10, yvec10;
VPERMILP_SY $0xb1, yvec9, yvec9;
VPERMILP_SY $0xb1, yvec8, yvec8;
#endif
##### Load Alpha ####
BROAD_SY MEMALPHA_R,yvec7;
BROAD_SY MEMALPHA_I,yvec6;
##### Multiply Alpha ####
VPERMILP_SY $0xb1,yvec15, yvec5;
MUL_SY	yvec15, yvec7, yvec15;
MUL_SY	yvec5, yvec6, yvec5;
ADDSUB_SY	yvec5, yvec15, yvec15;
VPERMILP_SY $0xb1,yvec14, yvec4;
MUL_SY	yvec14, yvec7, yvec14;
MUL_SY	yvec4, yvec6, yvec4;
ADDSUB_SY	yvec4, yvec14, yvec14;
VPERMILP_SY $0xb1,yvec13, yvec3;
MUL_SY	yvec13, yvec7, yvec13;
MUL_SY	yvec3, yvec6, yvec3;
ADDSUB_SY	yvec3, yvec13, yvec13;
VPERMILP_SY $0xb1,yvec12, yvec2;
MUL_SY	yvec12, yvec7, yvec12;
MUL_SY	yvec2, yvec6, yvec2;
ADDSUB_SY	yvec2, yvec12, yvec12;
VPERMILP_SY $0xb1,yvec11, yvec1;
MUL_SY	yvec11, yvec7, yvec11;
MUL_SY	yvec1, yvec6, yvec1;
ADDSUB_SY	yvec1, yvec11, yvec11;
VPERMILP_SY $0xb1,yvec10, yvec0;
MUL_SY	yvec10, yvec7, yvec10;
MUL_SY	yvec0, yvec6, yvec0;
ADDSUB_SY	yvec0, yvec10, yvec10;
VPERMILP_SY $0xb1,yvec9, yvec5;
MUL_SY	yvec9, yvec7, yvec9;
MUL_SY	yvec5, yvec6, yvec5;
ADDSUB_SY	yvec5, yvec9, yvec9;
VPERMILP_SY $0xb1,yvec8, yvec4;
MUL_SY	yvec8, yvec7, yvec8;
MUL_SY	yvec4, yvec6, yvec4;
ADDSUB_SY	yvec4, yvec8, yvec8;
#### Shuffle Results ####
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
#### Store Back ####
#### Testing alignment ####
MOVQ	C0, %rax;
OR		ldc, %rax;
TEST 	$15, %rax;
JNE		.L4_loopEx;
ALIGN_5
EXTRA_SY $1,yvec15,xvec7;
EXTRA_SY $1,yvec14,xvec6;
EXTRA_SY $1,yvec13,xvec5;
EXTRA_SY $1,yvec12,xvec4;
EXTRA_SY $1,yvec11,xvec3;
EXTRA_SY $1,yvec10,xvec2;
EXTRA_SY $1,yvec9,xvec1;
EXTRA_SY $1,yvec8,xvec0;
#ifndef	TRMMKERNEL
ADD_SY 0*SIZE(C0),xvec15, xvec15;
ADD_SY 4*SIZE(C1),xvec7, xvec7;
ADD_SY 8*SIZE(C0),xvec14, xvec14;
ADD_SY 12*SIZE(C1),xvec6, xvec6;
ADD_SY 0*SIZE(C0,ldc,1),xvec13, xvec13;
ADD_SY 4*SIZE(C1,ldc,1),xvec5, xvec5;
ADD_SY 8*SIZE(C0,ldc,1),xvec12, xvec12;
ADD_SY 12*SIZE(C1,ldc,1),xvec4, xvec4;
ADD_SY 0*SIZE(C1),xvec11, xvec11;
ADD_SY 4*SIZE(C0),xvec3, xvec3;
ADD_SY 8*SIZE(C1),xvec10, xvec10;
ADD_SY 12*SIZE(C0),xvec2, xvec2;
ADD_SY 0*SIZE(C1,ldc,1),xvec9, xvec9;
ADD_SY 4*SIZE(C0,ldc,1),xvec1, xvec1;
ADD_SY 8*SIZE(C1,ldc,1),xvec8, xvec8;
ADD_SY 12*SIZE(C0,ldc,1),xvec0, xvec0;
#endif
ST_SY xvec15,0*SIZE(C0);
ST_SY xvec7,4*SIZE(C1);
ST_SY xvec14,8*SIZE(C0);
ST_SY xvec6,12*SIZE(C1);
ST_SY xvec13,0*SIZE(C0,ldc,1);
ST_SY xvec5,4*SIZE(C1,ldc,1);
ST_SY xvec12,8*SIZE(C0,ldc,1);
ST_SY xvec4,12*SIZE(C1,ldc,1);
ST_SY xvec11,0*SIZE(C1);
ST_SY xvec3,4*SIZE(C0);
ST_SY xvec10,8*SIZE(C1);
ST_SY xvec2,12*SIZE(C0);
ST_SY xvec9,0*SIZE(C1,ldc,1);
ST_SY xvec1,4*SIZE(C0,ldc,1);
ST_SY xvec8,8*SIZE(C1,ldc,1);
ST_SY xvec0,12*SIZE(C0,ldc,1);
#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA))||(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
MOVQ bk, %rax;
SUBQ kkk, %rax;
SALQ $ZBASE_SHIFT, %rax;
LEAQ (ptrba, %rax, 8), ptrba;
LEAQ (ptrbb, %rax, 4), ptrbb;
#endif
#if defined(TRMMKERNEL) && defined(LEFT)
ADDQ	$8, kk;
#endif
ADDQ $16*SIZE,C0;
ADDQ $16*SIZE,C1;
.L1_bodyE:;
DECQ i;
JG .L1_bodyB;
JMP .L1_loopE;
ALIGN_5
.L4_loopEx:
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
LDL_SY	8*SIZE(C0), xvec5, xvec5;
LDH_SY	10*SIZE(C0), xvec5, xvec5;
ADD_SY	xvec5, xvec14, xvec14;
#endif
STL_SY	xvec14, 8*SIZE(C0);
STH_SY	xvec14, 10*SIZE(C0);
#ifndef	TRMMKERNEL
LDL_SY	12*SIZE(C1), xvec4, xvec4;
LDH_SY	14*SIZE(C1), xvec4, xvec4;
ADD_SY	xvec4, xvec6, xvec6;
#endif
STL_SY	xvec6, 12*SIZE(C1);
STH_SY	xvec6, 14*SIZE(C1);

EXTRA_SY $1, yvec13, xvec5;
#ifndef	TRMMKERNEL
LDL_SY	0*SIZE(C0, ldc, 1), xvec4, xvec4;
LDH_SY	2*SIZE(C0, ldc, 1), xvec4, xvec4;
ADD_SY	xvec4, xvec13, xvec13;
#endif
STL_SY	xvec13, 0*SIZE(C0, ldc, 1);
STH_SY	xvec13, 2*SIZE(C0, ldc, 1);
#ifndef	TRMMKERNEL
LDL_SY	4*SIZE(C1, ldc, 1), xvec3, xvec3;
LDH_SY	6*SIZE(C1, ldc, 1), xvec3, xvec3;
ADD_SY	xvec3, xvec5, xvec5;
#endif
STL_SY	xvec5, 4*SIZE(C1, ldc, 1);
STH_SX	xvec5, 6*SIZE(C1, ldc, 1);

EXTRA_SY $1, yvec12, xvec4;
#ifndef	TRMMKERNEL
LDL_SY	8*SIZE(C0, ldc, 1), xvec3, xvec3;
LDH_SY	10*SIZE(C0, ldc, 1), xvec3, xvec3;
ADD_SY	xvec3, xvec12, xvec12;
#endif
STL_SY	xvec12, 8*SIZE(C0, ldc, 1);
STH_SY	xvec12, 10*SIZE(C0, ldc, 1);
#ifndef	TRMMKERNEL
LDL_SY	12*SIZE(C1, ldc, 1), xvec2, xvec2;
LDH_SY	14*SIZE(C1, ldc, 1), xvec2, xvec2;
ADD_SY	xvec2, xvec4, xvec4;
#endif
STL_SY	xvec4, 12*SIZE(C1, ldc, 1);
STH_SY	xvec4, 14*SIZE(C1, ldc, 1);

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
LDL_SY	8*SIZE(C1), xvec1, xvec1;
LDH_SY	10*SIZE(C1), xvec1, xvec1;
ADD_SY	xvec1, xvec10, xvec10;
#endif
STL_SY	xvec10, 8*SIZE(C1);
STH_SY	xvec10, 10*SIZE(C1);
#ifndef	TRMMKERNEL
LDL_SY	12*SIZE(C0), xvec0, xvec0;
LDH_SY	14*SIZE(C0), xvec0, xvec0;
ADD_SY	xvec0, xvec2, xvec2;
#endif
STL_SY	xvec2, 12*SIZE(C0);
STH_SY	xvec2, 14*SIZE(C0);

EXTRA_SY $1, yvec9, xvec1;
#ifndef	TRMMKERNEL
LDL_SY	0*SIZE(C1, ldc, 1), xvec7, xvec7;
LDH_SY	2*SIZE(C1, ldc, 1), xvec7, xvec7;
ADD_SY	xvec7, xvec9, xvec9;
#endif
STL_SY	xvec9, 0*SIZE(C1, ldc, 1);
STH_SY	xvec9, 2*SIZE(C1, ldc, 1);
#ifndef	TRMMKERNEL
LDL_SY	4*SIZE(C0, ldc, 1), xvec6, xvec6;
LDH_SY	6*SIZE(C0, ldc, 1), xvec6, xvec6;
ADD_SY	xvec6, xvec1, xvec1;
#endif
STL_SY	xvec1, 4*SIZE(C0, ldc, 1);
STH_SY	xvec1, 6*SIZE(C0, ldc, 1);

EXTRA_SY $1, yvec8, xvec0;
#ifndef	TRMMKERNEL
LDL_SY	8*SIZE(C1, ldc, 1), xvec6, xvec6;
LDH_SY	10*SIZE(C1, ldc, 1), xvec6, xvec6;
ADD_SY	xvec6, xvec8, xvec8;
#endif
STL_SY	xvec8, 8*SIZE(C1, ldc, 1);
STH_SY	xvec8, 10*SIZE(C1, ldc, 1);
#ifndef	TRMMKERNEL
LDL_SY	12*SIZE(C0, ldc, 1), xvec5, xvec5;
LDH_SY	14*SIZE(C0, ldc, 1), xvec5, xvec5;
ADD_SY	xvec5, xvec0, xvec0;
#endif
STL_SY	xvec0, 12*SIZE(C0, ldc, 1);
STH_SY	xvec0, 14*SIZE(C0, ldc, 1);
#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA))||(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
MOVQ bk, %rax;
SUBQ kkk, %rax;
SALQ $ZBASE_SHIFT, %rax;
LEAQ (ptrba, %rax, 8), ptrba;
LEAQ (ptrbb, %rax, 4), ptrbb;
#endif
#if defined(TRMMKERNEL) && defined(LEFT)
ADDQ	$8, kk;
#endif
ADDQ	$16*SIZE, C0;
ADDQ	$16*SIZE, C1;
DECQ	i;
JG	.L1_bodyB;
ALIGN_5;
.L1_loopE:;
TEST $4, bm;
JLE .L5_loopE;
ALIGN_5
.L5_bodyB:
#if !defined(TRMMKERNEL)||(defined(TRMMKERNEL)&&defined(LEFT)&&defined(TRANSA))||(defined(TRMMKERNEL)&&!defined(LEFT)&&!defined(TRANSA))
MOVQ bb,ptrbb;
#else
MOVQ bb, ptrbb;
MOVQ kk, %rax;
SALQ $ZBASE_SHIFT, %rax;
LEAQ (ptrba, %rax, 4), ptrba;
LEAQ (ptrbb, %rax, 4), ptrbb;
#endif
XOR_SY	yvec15, yvec15, yvec15;
XOR_SY	yvec13, yvec13, yvec13;
XOR_SY	yvec11, yvec11, yvec11;
XOR_SY	yvec9, yvec9, yvec9;
#ifndef	TRMMKERNEL
MOVQ bk,k;
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
JLE .L8_loopE;
ALIGN_5
.L8_bodyB:
#### Unroll times 1 ####
LD_SY	0*SIZE(ptrba), yvec0;
VPERMILP_SY	$0xb1, yvec0, yvec1;
EDUP_SY	0*SIZE(ptrbb), yvec2;
VPERMILP_SY $0x4e, yvec2, yvec3;
MUL_SY	yvec0, yvec2, yvec6;
ADD1_SY	yvec6, yvec15, yvec15;
SHUF_SY	$0x03, yvec2, yvec2, yvec4;
MUL_SY	yvec0, yvec3, yvec7;
ADD1_SY	yvec7, yvec13, yvec13;

ODUP_SY 0*SIZE(ptrbb), yvec2;
SHUF_SY	$0x03, yvec3, yvec3, yvec5;
MUL_SY	yvec0, yvec4, yvec6;
ADD1_SY	yvec6, yvec11, yvec11;
VPERMILP_SY $0x4e, yvec2, yvec3;
MUL_SY	yvec0, yvec5, yvec7;
ADD1_SY	yvec7, yvec9, yvec9;

MUL_SY	yvec1, yvec2, yvec6;
ADD2_SY	yvec6, yvec15, yvec15;
SHUF_SY	$0x03, yvec2, yvec2, yvec4;
MUL_SY	yvec1, yvec3, yvec7;
ADD2_SY	yvec7, yvec13, yvec13;

SHUF_SY $0x03, yvec3, yvec3, yvec5;
MUL_SY	yvec1, yvec4, yvec6;
ADD2_SY	yvec6, yvec11, yvec11;
MUL_SY	yvec1, yvec5, yvec7;
ADD2_SY	yvec7, yvec9, yvec9;

#### Unroll time 2 ####
LD_SY	8*SIZE(ptrba), yvec0;
VPERMILP_SY	$0xb1, yvec0, yvec1;
EDUP_SY	8*SIZE(ptrbb), yvec2;
VPERMILP_SY $0x4e, yvec2, yvec3;
MUL_SY	yvec0, yvec2, yvec6;
ADD1_SY	yvec6, yvec15, yvec15;
SHUF_SY	$0x03, yvec2, yvec2, yvec4;
MUL_SY	yvec0, yvec3, yvec7;
ADD1_SY	yvec7, yvec13, yvec13;

ODUP_SY 8*SIZE(ptrbb), yvec2;
SHUF_SY	$0x03, yvec3, yvec3, yvec5;
MUL_SY	yvec0, yvec4, yvec6;
ADD1_SY	yvec6, yvec11, yvec11;
VPERMILP_SY $0x4e, yvec2, yvec3;
MUL_SY	yvec0, yvec5, yvec7;
ADD1_SY	yvec7, yvec9, yvec9;

MUL_SY	yvec1, yvec2, yvec6;
ADD2_SY	yvec6, yvec15, yvec15;
SHUF_SY	$0x03, yvec2, yvec2, yvec4;
MUL_SY	yvec1, yvec3, yvec7;
ADD2_SY	yvec7, yvec13, yvec13;

SHUF_SY $0x03, yvec3, yvec3, yvec5;
MUL_SY	yvec1, yvec4, yvec6;
ADD2_SY	yvec6, yvec11, yvec11;
MUL_SY	yvec1, yvec5, yvec7;
ADD2_SY	yvec7, yvec9, yvec9;

#### Unroll time 3 ####
LD_SY	16*SIZE(ptrba), yvec0;
VPERMILP_SY	$0xb1, yvec0, yvec1;
EDUP_SY	16*SIZE(ptrbb), yvec2;
VPERMILP_SY $0x4e, yvec2, yvec3;
MUL_SY	yvec0, yvec2, yvec6;
ADD1_SY	yvec6, yvec15, yvec15;
SHUF_SY	$0x03, yvec2, yvec2, yvec4;
MUL_SY	yvec0, yvec3, yvec7;
ADD1_SY	yvec7, yvec13, yvec13;

ODUP_SY 16*SIZE(ptrbb), yvec2;
SHUF_SY	$0x03, yvec3, yvec3, yvec5;
MUL_SY	yvec0, yvec4, yvec6;
ADD1_SY	yvec6, yvec11, yvec11;
VPERMILP_SY $0x4e, yvec2, yvec3;
MUL_SY	yvec0, yvec5, yvec7;
ADD1_SY	yvec7, yvec9, yvec9;

MUL_SY	yvec1, yvec2, yvec6;
ADD2_SY	yvec6, yvec15, yvec15;
SHUF_SY	$0x03, yvec2, yvec2, yvec4;
MUL_SY	yvec1, yvec3, yvec7;
ADD2_SY	yvec7, yvec13, yvec13;

SHUF_SY $0x03, yvec3, yvec3, yvec5;
MUL_SY	yvec1, yvec4, yvec6;
ADD2_SY	yvec6, yvec11, yvec11;
MUL_SY	yvec1, yvec5, yvec7;
ADD2_SY	yvec7, yvec9, yvec9;

#### Unroll time 3 ####
LD_SY	24*SIZE(ptrba), yvec0;
VPERMILP_SY	$0xb1, yvec0, yvec1;
EDUP_SY	24*SIZE(ptrbb), yvec2;
VPERMILP_SY $0x4e, yvec2, yvec3;
MUL_SY	yvec0, yvec2, yvec6;
ADD1_SY	yvec6, yvec15, yvec15;
SHUF_SY	$0x03, yvec2, yvec2, yvec4;
MUL_SY	yvec0, yvec3, yvec7;
ADD1_SY	yvec7, yvec13, yvec13;

ODUP_SY 24*SIZE(ptrbb), yvec2;
SHUF_SY	$0x03, yvec3, yvec3, yvec5;
MUL_SY	yvec0, yvec4, yvec6;
ADD1_SY	yvec6, yvec11, yvec11;
VPERMILP_SY $0x4e, yvec2, yvec3;
MUL_SY	yvec0, yvec5, yvec7;
ADD1_SY	yvec7, yvec9, yvec9;

MUL_SY	yvec1, yvec2, yvec6;
ADD2_SY	yvec6, yvec15, yvec15;
SHUF_SY	$0x03, yvec2, yvec2, yvec4;
MUL_SY	yvec1, yvec3, yvec7;
ADD2_SY	yvec7, yvec13, yvec13;

SHUF_SY $0x03, yvec3, yvec3, yvec5;
MUL_SY	yvec1, yvec4, yvec6;
ADD2_SY	yvec6, yvec11, yvec11;
MUL_SY	yvec1, yvec5, yvec7;
ADD2_SY	yvec7, yvec9, yvec9;
ADDQ	$32*SIZE, ptrba;
ADDQ	$32*SIZE, ptrbb;
DECQ	k;
JG .L8_bodyB;
ALIGN_5
.L8_loopE:
#ifndef	TRMMKERNEL
TEST $2, bk;
#else
TEST $2, kkk;
#endif
JLE .L9_loopE;
ALIGN_5
.L9_bodyB:
#### Unroll times 1 ####
LD_SY	0*SIZE(ptrba), yvec0;
VPERMILP_SY	$0xb1, yvec0, yvec1;
EDUP_SY	0*SIZE(ptrbb), yvec2;
VPERMILP_SY $0x4e, yvec2, yvec3;
MUL_SY	yvec0, yvec2, yvec6;
ADD1_SY	yvec6, yvec15, yvec15;
SHUF_SY	$0x03, yvec2, yvec2, yvec4;
MUL_SY	yvec0, yvec3, yvec7;
ADD1_SY	yvec7, yvec13, yvec13;

ODUP_SY 0*SIZE(ptrbb), yvec2;
SHUF_SY	$0x03, yvec3, yvec3, yvec5;
MUL_SY	yvec0, yvec4, yvec6;
ADD1_SY	yvec6, yvec11, yvec11;
VPERMILP_SY $0x4e, yvec2, yvec3;
MUL_SY	yvec0, yvec5, yvec7;
ADD1_SY	yvec7, yvec9, yvec9;

MUL_SY	yvec1, yvec2, yvec6;
ADD2_SY	yvec6, yvec15, yvec15;
SHUF_SY	$0x03, yvec2, yvec2, yvec4;
MUL_SY	yvec1, yvec3, yvec7;
ADD2_SY	yvec7, yvec13, yvec13;

SHUF_SY $0x03, yvec3, yvec3, yvec5;
MUL_SY	yvec1, yvec4, yvec6;
ADD2_SY	yvec6, yvec11, yvec11;
MUL_SY	yvec1, yvec5, yvec7;
ADD2_SY	yvec7, yvec9, yvec9;

#### Unroll time 2 ####
LD_SY	8*SIZE(ptrba), yvec0;
VPERMILP_SY	$0xb1, yvec0, yvec1;
EDUP_SY	8*SIZE(ptrbb), yvec2;
VPERMILP_SY $0x4e, yvec2, yvec3;
MUL_SY	yvec0, yvec2, yvec6;
ADD1_SY	yvec6, yvec15, yvec15;
SHUF_SY	$0x03, yvec2, yvec2, yvec4;
MUL_SY	yvec0, yvec3, yvec7;
ADD1_SY	yvec7, yvec13, yvec13;

ODUP_SY 8*SIZE(ptrbb), yvec2;
SHUF_SY	$0x03, yvec3, yvec3, yvec5;
MUL_SY	yvec0, yvec4, yvec6;
ADD1_SY	yvec6, yvec11, yvec11;
VPERMILP_SY $0x4e, yvec2, yvec3;
MUL_SY	yvec0, yvec5, yvec7;
ADD1_SY	yvec7, yvec9, yvec9;

MUL_SY	yvec1, yvec2, yvec6;
ADD2_SY	yvec6, yvec15, yvec15;
SHUF_SY	$0x03, yvec2, yvec2, yvec4;
MUL_SY	yvec1, yvec3, yvec7;
ADD2_SY	yvec7, yvec13, yvec13;

SHUF_SY $0x03, yvec3, yvec3, yvec5;
MUL_SY	yvec1, yvec4, yvec6;
ADD2_SY	yvec6, yvec11, yvec11;
MUL_SY	yvec1, yvec5, yvec7;
ADD2_SY	yvec7, yvec9, yvec9;
ADDQ	$16*SIZE, ptrba;
ADDQ	$16*SIZE, ptrbb;

.L9_loopE:
#ifndef	TRMMKERNEL
TEST $1, bk;
#else
TEST $1, kkk;
#endif
JLE .L10_loopE;
ALIGN_5
.L10_bodyB:
#### Unroll times 1 ####
LD_SY	0*SIZE(ptrba), yvec0;
VPERMILP_SY	$0xb1, yvec0, yvec1;
EDUP_SY	0*SIZE(ptrbb), yvec2;
VPERMILP_SY $0x4e, yvec2, yvec3;
MUL_SY	yvec0, yvec2, yvec6;
ADD1_SY	yvec6, yvec15, yvec15;
SHUF_SY	$0x03, yvec2, yvec2, yvec4;
MUL_SY	yvec0, yvec3, yvec7;
ADD1_SY	yvec7, yvec13, yvec13;

ODUP_SY 0*SIZE(ptrbb), yvec2;
SHUF_SY	$0x03, yvec3, yvec3, yvec5;
MUL_SY	yvec0, yvec4, yvec6;
ADD1_SY	yvec6, yvec11, yvec11;
VPERMILP_SY $0x4e, yvec2, yvec3;
MUL_SY	yvec0, yvec5, yvec7;
ADD1_SY	yvec7, yvec9, yvec9;

MUL_SY	yvec1, yvec2, yvec6;
ADD2_SY	yvec6, yvec15, yvec15;
SHUF_SY	$0x03, yvec2, yvec2, yvec4;
MUL_SY	yvec1, yvec3, yvec7;
ADD2_SY	yvec7, yvec13, yvec13;

SHUF_SY $0x03, yvec3, yvec3, yvec5;
MUL_SY	yvec1, yvec4, yvec6;
ADD2_SY	yvec6, yvec11, yvec11;
MUL_SY	yvec1, yvec5, yvec7;
ADD2_SY	yvec7, yvec9, yvec9;
ADDQ	$8*SIZE, ptrba;
ADDQ	$8*SIZE, ptrbb;

.L10_loopE:
#### Handle ####
XOR_SY	yvec7, yvec7, yvec7;
#if  defined(RN) || defined(RT) || defined(CN) || defined(CT)
ADDSUB_SY	yvec15, yvec7, yvec15;
ADDSUB_SY	yvec13, yvec7, yvec13;
ADDSUB_SY	yvec11, yvec7, yvec11;
ADDSUB_SY	yvec9, yvec7, yvec9;
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
SUB_SY	yvec15, yvec7, yvec15;
SUB_SY	yvec13, yvec7, yvec13;
SUB_SY	yvec11, yvec7, yvec11;
SUB_SY	yvec9, yvec7, yvec9;
#elif defined(RR) || defined(RC) || defined(CR) || defined(CC)
VPERMILP_SY $0xb1, yvec15, yvec15;
VPERMILP_SY $0xb1, yvec13, yvec13;
VPERMILP_SY $0xb1, yvec11, yvec11;
VPERMILP_SY $0xb1, yvec9, yvec9;
ADDSUB_SY	yvec15, yvec7, yvec15;
ADDSUB_SY	yvec13, yvec7, yvec13;
ADDSUB_SY	yvec11, yvec7, yvec11;
ADDSUB_SY	yvec9, yvec7, yvec9;
VPERMILP_SY $0xb1, yvec15, yvec15;
VPERMILP_SY $0xb1, yvec13, yvec13;
VPERMILP_SY $0xb1, yvec11, yvec11;
VPERMILP_SY $0xb1, yvec9, yvec9;
#endif
##### Load Alpha ####
BROAD_SY MEMALPHA_R,yvec7;
BROAD_SY MEMALPHA_I,yvec6;
##### Multiply Alpha ####
VPERMILP_SY $0xb1,yvec15, yvec5;
MUL_SY	yvec15, yvec7, yvec15;
MUL_SY	yvec5, yvec6, yvec5;
ADDSUB_SY	yvec5, yvec15, yvec15;
VPERMILP_SY $0xb1,yvec13, yvec3;
MUL_SY	yvec13, yvec7, yvec13;
MUL_SY	yvec3, yvec6, yvec3;
ADDSUB_SY	yvec3, yvec13, yvec13;
VPERMILP_SY $0xb1,yvec11, yvec1;
MUL_SY	yvec11, yvec7, yvec11;
MUL_SY	yvec1, yvec6, yvec1;
ADDSUB_SY	yvec1, yvec11, yvec11;
VPERMILP_SY $0xb1,yvec9, yvec5;
MUL_SY	yvec9, yvec7, yvec9;
MUL_SY	yvec5, yvec6, yvec5;
ADDSUB_SY	yvec5, yvec9, yvec9;
#### Writing back ####
#### Shuffle Results ####
MOV_SY yvec15,yvec7;
REVS_SY $0xe4,yvec13,yvec15,yvec15;
REVS_SY $0xe4,yvec7,yvec13,yvec13;
MOV_SY yvec11,yvec7;
REVS_SY $0xe4,yvec9,yvec11,yvec11;
REVS_SY $0xe4,yvec7,yvec9,yvec9;
#### Writing back ####
EXTRA_SY $1, yvec15, xvec7;
#ifndef	TRMMKERNEL
LDL_SX 0*SIZE(C0), xvec6, xvec6;
LDH_SX 2*SIZE(C0), xvec6, xvec6;
ADD_SX xvec6, xvec15, xvec15;
#endif
STL_SX	xvec15, 0*SIZE(C0);
STH_SX	xvec15, 2*SIZE(C0);
#ifndef	TRMMKERNEL
LDL_SX 4*SIZE(C1), xvec4, xvec4;
LDH_SX 6*SIZE(C1), xvec4, xvec4;
ADD_SX xvec4, xvec7, xvec7;
#endif
STL_SX	xvec7, 4*SIZE(C1);
STH_SX	xvec7, 6*SIZE(C1);

EXTRA_SY $1, yvec13, xvec5;
#ifndef	TRMMKERNEL
LDL_SX 0*SIZE(C0, ldc, 1), xvec4, xvec4;
LDH_SX 2*SIZE(C0, ldc, 1), xvec4, xvec4;
ADD_SX xvec4, xvec13, xvec13;
#endif
STL_SX	xvec13, 0*SIZE(C0, ldc, 1);
STH_SX	xvec13, 2*SIZE(C0, ldc, 1);
#ifndef	TRMMKERNEL
LDL_SX 4*SIZE(C1, ldc, 1), xvec2, xvec2;
LDH_SX 6*SIZE(C1, ldc, 1), xvec2, xvec2;
ADD_SX xvec2, xvec5, xvec5;
#endif
STL_SX	xvec5, 4*SIZE(C1, ldc, 1);
STH_SX	xvec5, 6*SIZE(C1, ldc, 1);

EXTRA_SY $1, yvec11, xvec3;
#ifndef	TRMMKERNEL
LDL_SX 0*SIZE(C1), xvec2, xvec2;
LDH_SX 2*SIZE(C1), xvec2, xvec2;
ADD_SX xvec2, xvec11, xvec11;
#endif
STL_SX	xvec11, 0*SIZE(C1);
STH_SX	xvec11, 2*SIZE(C1);
#ifndef	TRMMKERNEL
LDL_SX 4*SIZE(C0), xvec0, xvec0;
LDH_SX 6*SIZE(C0), xvec0, xvec0;
ADD_SX xvec0, xvec3, xvec3;
#endif
STL_SX	xvec3, 4*SIZE(C0);
STH_SX	xvec3, 6*SIZE(C0);

EXTRA_SY $1, yvec9, xvec1;
#ifndef	TRMMKERNEL
LDL_SX 0*SIZE(C1, ldc, 1), xvec0, xvec0;
LDH_SX 2*SIZE(C1, ldc, 1), xvec0, xvec0;
ADD_SX xvec0, xvec9, xvec9;
#endif
STL_SX	xvec9, 0*SIZE(C1, ldc, 1);
STH_SX	xvec9, 2*SIZE(C1, ldc, 1);
#ifndef	TRMMKERNEL
LDL_SX 4*SIZE(C0, ldc, 1), xvec6, xvec6;
LDH_SX 6*SIZE(C0, ldc, 1), xvec6, xvec6;
ADD_SX xvec6, xvec1, xvec1;
#endif
STL_SX	xvec1, 4*SIZE(C0, ldc, 1);
STH_SX	xvec1, 6*SIZE(C0, ldc, 1);
#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA))||(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
MOVQ bk, %rax;
SUBQ kkk, %rax;
SALQ $ZBASE_SHIFT, %rax;
LEAQ (ptrba, %rax, 4), ptrba;
LEAQ (ptrbb, %rax, 4), ptrbb;
#endif
#if defined(TRMMKERNEL) && defined(LEFT)
ADDQ	$4, kk;
#endif

ADDQ	$8*SIZE, C0;
ADDQ	$8*SIZE, C1;

.L5_loopE:
TEST $2, bm;
JLE .L6_loopE;
ALIGN_5
.L6_bodyB:
#if !defined(TRMMKERNEL)||(defined(TRMMKERNEL)&&defined(LEFT)&&defined(TRANSA))||(defined(TRMMKERNEL)&&!defined(LEFT)&&!defined(TRANSA))
MOVQ bb,ptrbb;
#else
MOVQ bb, ptrbb;
MOVQ kk, %rax;
SALQ $ZBASE_SHIFT, %rax;
LEAQ (ptrba, %rax, 2), ptrba;
LEAQ (ptrbb, %rax, 4), ptrbb;
#endif
#### Initial Results Register ####
XOR_SY	yvec15, yvec15, yvec15;
XOR_SY	yvec14, yvec14, yvec14;
XOR_SY	yvec13, yvec13, yvec13;
XOR_SY	yvec12, yvec12, yvec12;
#ifndef	TRMMKERNEL
MOVQ bk,k;
#elif (defined(LEFT)&&!defined(TRANSA))||(!defined(LEFT)&&defined(TRANSA))
MOVQ bk, %rax;
SUBQ kk, %rax;
MOVQ %rax, kkk;
#else
MOVQ kk, %rax;
#ifdef	LEFT
ADDQ $2, %rax;
#else
ADDQ $4, %rax;
#endif
MOVQ %rax, kkk;
#endif
SARQ $2, k;
JLE .L11_loopE;
ALIGN_5
.L11_bodyB:
LD_SX	0*SIZE(ptrba), xvec0;	# ar1, ai1, ar2, ai2
EDUP_SX	0*SIZE(ptrbb), xvec2;	# br1, br1, br2, br2
SHUF_SX	$0x4e, xvec2, xvec3;	# br3, br3, br4, br4
MUL_SX xvec0, xvec2, xvec2;
ADD1_SX xvec2, xvec15, xvec15;
MUL_SX xvec0, xvec3, xvec3;
ADD1_SX xvec3, xvec14, xvec14;

EDUP_SX	4*SIZE(ptrbb), xvec4;
SHUF_SX	$0x4e, xvec4, xvec5;
MUL_SX xvec0, xvec4, xvec4;
ADD1_SX xvec4, xvec13, xvec13;
MUL_SX xvec0, xvec5, xvec5;
ADD1_SX xvec5, xvec12, xvec12;

SHUF_SX $0xb1, xvec0, xvec1;
ODUP_SX 0*SIZE(ptrbb), xvec2;
SHUF_SX	$0x4e, xvec2, xvec3;
MUL_SX xvec1, xvec2, xvec2;
ADD2_SX xvec2, xvec15, xvec15;
MUL_SX xvec1, xvec3, xvec3;
ADD2_SX xvec3, xvec14, xvec14;

ODUP_SX	4*SIZE(ptrbb), xvec4;
SHUF_SX	$0x4e, xvec4, xvec5;
MUL_SX xvec1, xvec4, xvec4;
ADD2_SX xvec4, xvec13, xvec13;
MUL_SX xvec1, xvec5, xvec5;
ADD2_SX xvec5, xvec12, xvec12;

LD_SX	4*SIZE(ptrba), xvec0;	# ar1, ai1, ar2, ai2
EDUP_SX	8*SIZE(ptrbb), xvec2;	# br1, br1, br2, br2
SHUF_SX	$0x4e, xvec2, xvec3;	# br3, br3, br4, br4
MUL_SX xvec0, xvec2, xvec2;
ADD1_SX xvec2, xvec15, xvec15;
MUL_SX xvec0, xvec3, xvec3;
ADD1_SX xvec3, xvec14, xvec14;

EDUP_SX	12*SIZE(ptrbb), xvec4;
SHUF_SX	$0x4e, xvec4, xvec5;
MUL_SX xvec0, xvec4, xvec4;
ADD1_SX xvec4, xvec13, xvec13;
MUL_SX xvec0, xvec5, xvec5;
ADD1_SX xvec5, xvec12, xvec12;

SHUF_SX $0xb1, xvec0, xvec1;
ODUP_SX 8*SIZE(ptrbb), xvec2;
SHUF_SX	$0x4e, xvec2, xvec3;
MUL_SX xvec1, xvec2, xvec2;
ADD2_SX xvec2, xvec15, xvec15;
MUL_SX xvec1, xvec3, xvec3;
ADD2_SX xvec3, xvec14, xvec14;

ODUP_SX	12*SIZE(ptrbb), xvec4;
SHUF_SX	$0x4e, xvec4, xvec5;
MUL_SX xvec1, xvec4, xvec4;
ADD2_SX xvec4, xvec13, xvec13;
MUL_SX xvec1, xvec5, xvec5;
ADD2_SX xvec5, xvec12, xvec12;

LD_SX	8*SIZE(ptrba), xvec0;	# ar1, ai1, ar2, ai2
EDUP_SX	16*SIZE(ptrbb), xvec2;	# br1, br1, br2, br2
SHUF_SX	$0x4e, xvec2, xvec3;	# br3, br3, br4, br4
MUL_SX xvec0, xvec2, xvec2;
ADD1_SX xvec2, xvec15, xvec15;
MUL_SX xvec0, xvec3, xvec3;
ADD1_SX xvec3, xvec14, xvec14;

EDUP_SX	20*SIZE(ptrbb), xvec4;
SHUF_SX	$0x4e, xvec4, xvec5;
MUL_SX xvec0, xvec4, xvec4;
ADD1_SX xvec4, xvec13, xvec13;
MUL_SX xvec0, xvec5, xvec5;
ADD1_SX xvec5, xvec12, xvec12;

SHUF_SX $0xb1, xvec0, xvec1;
ODUP_SX 16*SIZE(ptrbb), xvec2;
SHUF_SX	$0x4e, xvec2, xvec3;
MUL_SX xvec1, xvec2, xvec2;
ADD2_SX xvec2, xvec15, xvec15;
MUL_SX xvec1, xvec3, xvec3;
ADD2_SX xvec3, xvec14, xvec14;

ODUP_SX	20*SIZE(ptrbb), xvec4;
SHUF_SX	$0x4e, xvec4, xvec5;
MUL_SX xvec1, xvec4, xvec4;
ADD2_SX xvec4, xvec13, xvec13;
MUL_SX xvec1, xvec5, xvec5;
ADD2_SX xvec5, xvec12, xvec12;

LD_SX	12*SIZE(ptrba), xvec0;	# ar1, ai1, ar2, ai2
EDUP_SX	24*SIZE(ptrbb), xvec2;	# br1, br1, br2, br2
SHUF_SX	$0x4e, xvec2, xvec3;	# br3, br3, br4, br4
MUL_SX xvec0, xvec2, xvec2;
ADD1_SX xvec2, xvec15, xvec15;
MUL_SX xvec0, xvec3, xvec3;
ADD1_SX xvec3, xvec14, xvec14;

EDUP_SX	28*SIZE(ptrbb), xvec4;
SHUF_SX	$0x4e, xvec4, xvec5;
MUL_SX xvec0, xvec4, xvec4;
ADD1_SX xvec4, xvec13, xvec13;
MUL_SX xvec0, xvec5, xvec5;
ADD1_SX xvec5, xvec12, xvec12;

SHUF_SX $0xb1, xvec0, xvec1;
ODUP_SX 24*SIZE(ptrbb), xvec2;
SHUF_SX	$0x4e, xvec2, xvec3;
MUL_SX xvec1, xvec2, xvec2;
ADD2_SX xvec2, xvec15, xvec15;
MUL_SX xvec1, xvec3, xvec3;
ADD2_SX xvec3, xvec14, xvec14;

ODUP_SX	28*SIZE(ptrbb), xvec4;
SHUF_SX	$0x4e, xvec4, xvec5;
MUL_SX xvec1, xvec4, xvec4;
ADD2_SX xvec4, xvec13, xvec13;
MUL_SX xvec1, xvec5, xvec5;
ADD2_SX xvec5, xvec12, xvec12;
ADDQ	$16*SIZE, ptrba;
ADDQ	$32*SIZE, ptrbb;
DECQ	k;
JG .L11_bodyB;
ALIGN_5
.L11_loopE:
#ifndef	TRMMKERNEL
TEST $2, bk;
#else
TEST $2, kkk;
#endif
JLE .L12_loopE;
ALIGN_5
.L12_bodyB:
LD_SX	0*SIZE(ptrba), xvec0;	# ar1, ai1, ar2, ai2
EDUP_SX	0*SIZE(ptrbb), xvec2;	# br1, br1, br2, br2
SHUF_SX	$0x4e, xvec2, xvec3;	# br3, br3, br4, br4
MUL_SX xvec0, xvec2, xvec2;
ADD1_SX xvec2, xvec15, xvec15;
MUL_SX xvec0, xvec3, xvec3;
ADD1_SX xvec3, xvec14, xvec14;

EDUP_SX	4*SIZE(ptrbb), xvec4;
SHUF_SX	$0x4e, xvec4, xvec5;
MUL_SX xvec0, xvec4, xvec4;
ADD1_SX xvec4, xvec13, xvec13;
MUL_SX xvec0, xvec5, xvec5;
ADD1_SX xvec5, xvec12, xvec12;

SHUF_SX $0xb1, xvec0, xvec1;
ODUP_SX 0*SIZE(ptrbb), xvec2;
SHUF_SX	$0x4e, xvec2, xvec3;
MUL_SX xvec1, xvec2, xvec2;
ADD2_SX xvec2, xvec15, xvec15;
MUL_SX xvec1, xvec3, xvec3;
ADD2_SX xvec3, xvec14, xvec14;

ODUP_SX	4*SIZE(ptrbb), xvec4;
SHUF_SX	$0x4e, xvec4, xvec5;
MUL_SX xvec1, xvec4, xvec4;
ADD2_SX xvec4, xvec13, xvec13;
MUL_SX xvec1, xvec5, xvec5;
ADD2_SX xvec5, xvec12, xvec12;

LD_SX	4*SIZE(ptrba), xvec0;	# ar1, ai1, ar2, ai2
EDUP_SX	8*SIZE(ptrbb), xvec2;	# br1, br1, br2, br2
SHUF_SX	$0x4e, xvec2, xvec3;	# br3, br3, br4, br4
MUL_SX xvec0, xvec2, xvec2;
ADD1_SX xvec2, xvec15, xvec15;
MUL_SX xvec0, xvec3, xvec3;
ADD1_SX xvec3, xvec14, xvec14;

EDUP_SX	12*SIZE(ptrbb), xvec4;
SHUF_SX	$0x4e, xvec4, xvec5;
MUL_SX xvec0, xvec4, xvec4;
ADD1_SX xvec4, xvec13, xvec13;
MUL_SX xvec0, xvec5, xvec5;
ADD1_SX xvec5, xvec12, xvec12;

SHUF_SX $0xb1, xvec0, xvec1;
ODUP_SX 8*SIZE(ptrbb), xvec2;
SHUF_SX	$0x4e, xvec2, xvec3;
MUL_SX xvec1, xvec2, xvec2;
ADD2_SX xvec2, xvec15, xvec15;
MUL_SX xvec1, xvec3, xvec3;
ADD2_SX xvec3, xvec14, xvec14;

ODUP_SX	12*SIZE(ptrbb), xvec4;
SHUF_SX	$0x4e, xvec4, xvec5;
MUL_SX xvec1, xvec4, xvec4;
ADD2_SX xvec4, xvec13, xvec13;
MUL_SX xvec1, xvec5, xvec5;
ADD2_SX xvec5, xvec12, xvec12;
ADDQ	$8*SIZE, ptrba;
ADDQ	$16*SIZE, ptrbb;

.L12_loopE:
#ifndef	TRMMKERNEL
TEST $1, bk;
#else
TEST $1, kkk;
#endif
JLE .L13_loopE;
ALIGN_5
.L13_bodyB:
LD_SX	0*SIZE(ptrba), xvec0;	# ar1, ai1, ar2, ai2
EDUP_SX	0*SIZE(ptrbb), xvec2;	# br1, br1, br2, br2
SHUF_SX	$0x4e, xvec2, xvec3;	# br3, br3, br4, br4
MUL_SX xvec0, xvec2, xvec2;
ADD1_SX xvec2, xvec15, xvec15;
MUL_SX xvec0, xvec3, xvec3;
ADD1_SX xvec3, xvec14, xvec14;

EDUP_SX	4*SIZE(ptrbb), xvec4;
SHUF_SX	$0x4e, xvec4, xvec5;
MUL_SX xvec0, xvec4, xvec4;
ADD1_SX xvec4, xvec13, xvec13;
MUL_SX xvec0, xvec5, xvec5;
ADD1_SX xvec5, xvec12, xvec12;

SHUF_SX $0xb1, xvec0, xvec1;
ODUP_SX 0*SIZE(ptrbb), xvec2;
SHUF_SX	$0x4e, xvec2, xvec3;
MUL_SX xvec1, xvec2, xvec2;
ADD2_SX xvec2, xvec15, xvec15;
MUL_SX xvec1, xvec3, xvec3;
ADD2_SX xvec3, xvec14, xvec14;

ODUP_SX	4*SIZE(ptrbb), xvec4;
SHUF_SX	$0x4e, xvec4, xvec5;
MUL_SX xvec1, xvec4, xvec4;
ADD2_SX xvec4, xvec13, xvec13;
MUL_SX xvec1, xvec5, xvec5;
ADD2_SX xvec5, xvec12, xvec12;
ADDQ	$4*SIZE, ptrba;
ADDQ	$8*SIZE, ptrbb;

.L13_loopE:
#### Handle ####
#if  defined(RN) || defined(RT) || defined(CN) || defined(CT)
XOR_SY	yvec7, yvec7, yvec7;
ADDSUB_SX xvec15, xvec7, xvec7;
MOV_SX		xvec7, xvec15;
XOR_SY	yvec7, yvec7, yvec7;
ADDSUB_SX xvec14, xvec7, xvec7;
MOV_SX		xvec7, xvec14;
XOR_SY	yvec7, yvec7, yvec7;
ADDSUB_SX xvec13, xvec7, xvec7;
MOV_SX		xvec7, xvec13;
XOR_SY	yvec7, yvec7, yvec7;
ADDSUB_SX xvec12, xvec7, xvec7;
MOV_SX		xvec7, xvec12;
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
XOR_SY	yvec7, yvec7, yvec7;
SUB_SX xvec15, xvec7, xvec7;
MOV_SX	xvec7, xvec15;
XOR_SY	yvec7, yvec7, yvec7;
SUB_SX xvec14, xvec7, xvec7;
MOV_SX	xvec7, xvec14;
XOR_SY	yvec7, yvec7, yvec7;
SUB_SX xvec13, xvec7, xvec7;
MOV_SX	xvec7, xvec13;
XOR_SY	yvec7, yvec7, yvec7;
SUB_SX xvec12, xvec7, xvec7;
MOV_SX	xvec7, xvec12;
#elif defined(RR) || defined(RC) || defined(CR) || defined(CC)
SHUF_SX $0xb1, xvec15, xvec15;
SHUF_SX $0xb1, xvec14, xvec14;
SHUF_SX $0xb1, xvec13, xvec13;
SHUF_SX $0xb1, xvec12, xvec12;
XOR_SY	yvec7, yvec7, yvec7;
ADDSUB_SX xvec15, xvec7, xvec7;
MOV_SX		xvec7, xvec15;
XOR_SY	yvec7, yvec7, yvec7;
ADDSUB_SX xvec14, xvec7, xvec7;
MOV_SX		xvec7, xvec14;
XOR_SY	yvec7, yvec7, yvec7;
ADDSUB_SX xvec13, xvec7, xvec7;
MOV_SX		xvec7, xvec13;
XOR_SY	yvec7, yvec7, yvec7;
ADDSUB_SX xvec12, xvec7, xvec7;
MOV_SX		xvec7, xvec12;
SHUF_SX $0xb1, xvec15, xvec15;
SHUF_SX $0xb1, xvec14, xvec14;
SHUF_SX $0xb1, xvec13, xvec13;
SHUF_SX $0xb1, xvec12, xvec12;
#endif
##### Load Alpha ####
BROAD_SX MEMALPHA_R,xvec7;
BROAD_SX MEMALPHA_I,xvec6;
##### Multiply Alpha ####
VPERMILP_SX $0xb1,xvec15, xvec5;
MUL_SX xvec7, xvec15, xvec15;
MUL_SX xvec6, xvec5, xvec5;
ADDSUB_SX xvec5, xvec15, xvec15;
VPERMILP_SX $0xb1,xvec14, xvec4;
MUL_SX xvec7, xvec14, xvec14;
MUL_SX xvec6, xvec4, xvec4;
ADDSUB_SX xvec4, xvec14, xvec14;
VPERMILP_SX $0xb1,xvec13, xvec3;
MUL_SX xvec7, xvec13, xvec13;
MUL_SX xvec6, xvec3, xvec3;
ADDSUB_SX xvec3, xvec13, xvec13;
VPERMILP_SX $0xb1,xvec12, xvec2;
MUL_SX xvec7, xvec12, xvec12;
MUL_SX xvec6, xvec2, xvec2;
ADDSUB_SX xvec2, xvec12, xvec12;
#### Writing back ####
#ifndef	TRMMKERNEL
LDL_SX 0*SIZE(C0), xvec0, xvec0;
LDH_SX 2*SIZE(C0, ldc,1), xvec0, xvec0;
LDL_SX 0*SIZE(C0, ldc,1), xvec1, xvec1;
LDH_SX 2*SIZE(C0), xvec1, xvec1;
LDL_SX 0*SIZE(C1), xvec2, xvec2;
LDH_SX 2*SIZE(C1, ldc, 1), xvec2, xvec2;
LDL_SX 0*SIZE(C1, ldc, 1), xvec3, xvec3;
LDH_SX 2*SIZE(C1), xvec3, xvec3;
ADD_SX xvec0, xvec15, xvec15;
ADD_SX xvec1, xvec14, xvec14;
ADD_SX xvec2, xvec13, xvec13;
ADD_SX xvec3, xvec12, xvec12;
#endif
STL_SX	xvec15, 0*SIZE(C0);
STH_SX	xvec15, 2*SIZE(C0, ldc, 1);
STL_SX	xvec14, 0*SIZE(C0, ldc, 1);
STH_SX	xvec14, 2*SIZE(C0);
STL_SX	xvec13, 0*SIZE(C1);
STH_SX	xvec13, 2*SIZE(C1, ldc, 1);
STL_SX	xvec12, 0*SIZE(C1, ldc, 1);
STH_SX	xvec12, 2*SIZE(C1);
#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA))||(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
MOVQ bk, %rax;
SUBQ kkk, %rax;
SALQ $ZBASE_SHIFT, %rax;
LEAQ (ptrba, %rax, 2), ptrba;
LEAQ (ptrbb, %rax, 4), ptrbb;
#endif
#if defined(TRMMKERNEL) && defined(LEFT)
ADDQ	$2, kk;
#endif

ADDQ	$4*SIZE, C0;
ADDQ	$4*SIZE, C1;

.L6_loopE:
TEST $1, bm;
JLE .L7_loopE;
ALIGN_5
.L7_bodyB:
#if !defined(TRMMKERNEL)||(defined(TRMMKERNEL)&&defined(LEFT)&&defined(TRANSA))||(defined(TRMMKERNEL)&&!defined(LEFT)&&!defined(TRANSA))
MOVQ bb,ptrbb;
#else
MOVQ bb, ptrbb;
MOVQ kk, %rax;
SALQ $ZBASE_SHIFT, %rax;
ADDQ %rax, ptrba;
LEAQ (ptrbb, %rax, 4), ptrbb;
#endif
XOR_SY	yvec15, yvec15, yvec15;
XOR_SY	yvec14, yvec14, yvec14;
#ifndef	TRMMKERNEL
MOVQ bk,k;
#elif (defined(LEFT)&&!defined(TRANSA))||(!defined(LEFT)&&defined(TRANSA))
MOVQ bk, %rax;
SUBQ kk, %rax;
MOVQ %rax, kkk;
#else
MOVQ kk, %rax;
#ifdef	LEFT
ADDQ $1, %rax;
#else
ADDQ $4, %rax;
#endif
MOVQ %rax, kkk;
#endif
SARQ $2, k;
JLE .L14_loopE;
ALIGN_5
.L14_bodyB:
BROAD_SX 0*SIZE(ptrba), xvec0;
LD_SX	0*SIZE(ptrbb), xvec2;
SHUF_SX	$0xb1, xvec2, xvec3;
MUL_SX xvec0, xvec2, xvec2;
ADD1_SX xvec2, xvec15, xvec15;

LD_SX	4*SIZE(ptrbb), xvec4;
SHUF_SX	$0xb1, xvec4, xvec5;
MUL_SX xvec0, xvec4, xvec4;
ADD1_SX xvec4, xvec14, xvec14;

BROAD_SX 1*SIZE(ptrba), xvec1;
MUL_SX xvec1, xvec3, xvec3;
ADD2_SX xvec3, xvec15, xvec15;
MUL_SX xvec1, xvec5, xvec5;
ADD2_SX xvec5, xvec14, xvec14;

BROAD_SX 2*SIZE(ptrba), xvec0;
LD_SX	8*SIZE(ptrbb), xvec2;
SHUF_SX	$0xb1, xvec2, xvec3;
MUL_SX xvec0, xvec2, xvec2;
ADD1_SX xvec2, xvec15, xvec15;

LD_SX	12*SIZE(ptrbb), xvec4;
SHUF_SX	$0xb1, xvec4, xvec5;
MUL_SX xvec0, xvec4, xvec4;
ADD1_SX xvec4, xvec14, xvec14;

BROAD_SX 3*SIZE(ptrba), xvec1;
MUL_SX xvec1, xvec3, xvec3;
ADD2_SX xvec3, xvec15, xvec15;
MUL_SX xvec1, xvec5, xvec5;
ADD2_SX xvec5, xvec14, xvec14;

BROAD_SX 4*SIZE(ptrba), xvec0;
LD_SX	16*SIZE(ptrbb), xvec2;
SHUF_SX	$0xb1, xvec2, xvec3;
MUL_SX xvec0, xvec2, xvec2;
ADD1_SX xvec2, xvec15, xvec15;

LD_SX	20*SIZE(ptrbb), xvec4;
SHUF_SX	$0xb1, xvec4, xvec5;
MUL_SX xvec0, xvec4, xvec4;
ADD1_SX xvec4, xvec14, xvec14;

BROAD_SX 5*SIZE(ptrba), xvec1;
MUL_SX xvec1, xvec3, xvec3;
ADD2_SX xvec3, xvec15, xvec15;
MUL_SX xvec1, xvec5, xvec5;
ADD2_SX xvec5, xvec14, xvec14;

BROAD_SX 6*SIZE(ptrba), xvec0;
LD_SX	24*SIZE(ptrbb), xvec2;
SHUF_SX	$0xb1, xvec2, xvec3;
MUL_SX xvec0, xvec2, xvec2;
ADD1_SX xvec2, xvec15, xvec15;

LD_SX	28*SIZE(ptrbb), xvec4;
SHUF_SX	$0xb1, xvec4, xvec5;
MUL_SX xvec0, xvec4, xvec4;
ADD1_SX xvec4, xvec14, xvec14;

BROAD_SX 7*SIZE(ptrba), xvec1;
MUL_SX xvec1, xvec3, xvec3;
ADD2_SX xvec3, xvec15, xvec15;
MUL_SX xvec1, xvec5, xvec5;
ADD2_SX xvec5, xvec14, xvec14;
ADDQ	$8*SIZE, ptrba;
ADDQ	$32*SIZE, ptrbb;
DECQ	k;
JG	.L14_bodyB;
ALIGN_5
.L14_loopE:
#ifndef	TRMMKERNEL
TEST $2, bk;
#else
TEST $2, kkk;
#endif
JLE .L15_loopE;
ALIGN_5
.L15_bodyB:
BROAD_SX 0*SIZE(ptrba), xvec0;
LD_SX	0*SIZE(ptrbb), xvec2;
SHUF_SX	$0xb1, xvec2, xvec3;
MUL_SX xvec0, xvec2, xvec2;
ADD1_SX xvec2, xvec15, xvec15;

LD_SX	4*SIZE(ptrbb), xvec4;
SHUF_SX	$0xb1, xvec4, xvec5;
MUL_SX xvec0, xvec4, xvec4;
ADD1_SX xvec4, xvec14, xvec14;

BROAD_SX 1*SIZE(ptrba), xvec1;
MUL_SX xvec1, xvec3, xvec3;
ADD2_SX xvec3, xvec15, xvec15;
MUL_SX xvec1, xvec5, xvec5;
ADD2_SX xvec5, xvec14, xvec14;

BROAD_SX 2*SIZE(ptrba), xvec0;
LD_SX	8*SIZE(ptrbb), xvec2;
SHUF_SX	$0xb1, xvec2, xvec3;
MUL_SX xvec0, xvec2, xvec2;
ADD1_SX xvec2, xvec15, xvec15;

LD_SX	12*SIZE(ptrbb), xvec4;
SHUF_SX	$0xb1, xvec4, xvec5;
MUL_SX xvec0, xvec4, xvec4;
ADD1_SX xvec4, xvec14, xvec14;

BROAD_SX 3*SIZE(ptrba), xvec1;
MUL_SX xvec1, xvec3, xvec3;
ADD2_SX xvec3, xvec15, xvec15;
MUL_SX xvec1, xvec5, xvec5;
ADD2_SX xvec5, xvec14, xvec14;
ADDQ	$4*SIZE, ptrba;
ADDQ	$16*SIZE, ptrbb;

.L15_loopE:
#ifndef	TRMMKERNEL
TEST $1, bk;
#else
TEST $1, kkk;
#endif
JLE .L16_loopE;
ALIGN_5
.L16_bodyB:
BROAD_SX 0*SIZE(ptrba), xvec0;
LD_SX	0*SIZE(ptrbb), xvec2;
SHUF_SX	$0xb1, xvec2, xvec3;
MUL_SX xvec0, xvec2, xvec2;
ADD1_SX xvec2, xvec15, xvec15;

LD_SX	4*SIZE(ptrbb), xvec4;
SHUF_SX	$0xb1, xvec4, xvec5;
MUL_SX xvec0, xvec4, xvec4;
ADD1_SX xvec4, xvec14, xvec14;

BROAD_SX 1*SIZE(ptrba), xvec1;
MUL_SX xvec1, xvec3, xvec3;
ADD2_SX xvec3, xvec15, xvec15;
MUL_SX xvec1, xvec5, xvec5;
ADD2_SX xvec5, xvec14, xvec14;
ADDQ	$2*SIZE, ptrba;
ADDQ	$8*SIZE, ptrbb;

.L16_loopE:
#### Handle ####
#if defined(NR) || defined(NC) || defined(TR) || defined(TC)
XOR_SY	yvec7, yvec7, yvec7;
ADDSUB_SX xvec15, xvec7, xvec7;
MOV_SX		xvec7, xvec15;
XOR_SY	yvec7, yvec7, yvec7;
ADDSUB_SX xvec14, xvec7, xvec7;
MOV_SX		xvec7, xvec14;
#elif  defined(RN) || defined(RT) || defined(CN) || defined(CT)
XOR_SY	yvec7, yvec7, yvec7;
SUB_SX xvec15, xvec7, xvec7;
MOV_SX	xvec7, xvec15;
XOR_SY	yvec7, yvec7, yvec7;
SUB_SX xvec14, xvec7, xvec7;
MOV_SX	xvec7, xvec14;
#elif defined(RR) || defined(RC) || defined(CR) || defined(CC)
SHUF_SX $0xb1, xvec15, xvec15;
SHUF_SX $0xb1, xvec14, xvec14;
XOR_SY	yvec7, yvec7, yvec7;
ADDSUB_SX xvec15, xvec7, xvec7;
MOV_SX		xvec7, xvec15;
XOR_SY	yvec7, yvec7, yvec7;
ADDSUB_SX xvec14, xvec7, xvec7;
MOV_SX		xvec7, xvec14;
SHUF_SX $0xb1, xvec15, xvec15;
SHUF_SX $0xb1, xvec14, xvec14;
#endif
##### Load Alpha ####
BROAD_SX MEMALPHA_R,xvec7;
BROAD_SX MEMALPHA_I,xvec6;
##### Multiply Alpha ####
VPERMILP_SX $0xb1,xvec15, xvec5;
MUL_SX xvec7, xvec15, xvec15;
MUL_SX xvec6, xvec5, xvec5;
ADDSUB_SX xvec5, xvec15, xvec15;
VPERMILP_SX $0xb1,xvec14, xvec4;
MUL_SX xvec7, xvec14, xvec14;
MUL_SX xvec6, xvec4, xvec4;
ADDSUB_SX xvec4, xvec14, xvec14;
#### Writing back ####
#ifndef	TRMMKERNEL
LDL_SX 0*SIZE(C0), xvec0, xvec0;
LDH_SX 0*SIZE(C0, ldc, 1), xvec0, xvec0;
LDL_SX 0*SIZE(C1), xvec1, xvec1;
LDH_SX 0*SIZE(C1, ldc, 1), xvec1, xvec1;
ADD_SX xvec0, xvec15, xvec15;
ADD_SX xvec1, xvec14, xvec14;
#endif
STL_SX	xvec15, 0*SIZE(C0);
STH_SX	xvec15, 0*SIZE(C0, ldc, 1);
STL_SX	xvec14, 0*SIZE(C1);
STH_SX	xvec14, 0*SIZE(C1, ldc, 1);
#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA))||(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
MOVQ bk, %rax;
SUBQ kkk, %rax;
SALQ $ZBASE_SHIFT, %rax;
ADDQ %rax, ptrba;
LEAQ (ptrbb, %rax, 4), ptrbb;
#endif
#if defined(TRMMKERNEL) && defined(LEFT)
ADDQ	$1, kk;
#endif

ADDQ	$2*SIZE, C0;
ADDQ	$2*SIZE, C1;
.L7_loopE:
#if defined(TRMMKERNEL) && !defined(LEFT)
ADDQ	$4, kk;
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
ALIGN_5
.L20_bodyB:
#if defined(TRMMKERNEL) && defined(LEFT)
MOVQ OFFSET, %rax;
MOVQ %rax, kk;
#endif
MOVQ C, C0;
LEAQ (C, ldc, 1), C1;
MOVQ ba, ptrba;
MOVQ bm, i;
SARQ $3, i;
JLE .L21_loopE;
ALIGN_5
.L21_bodyB:
#if !defined(TRMMKERNEL)||(defined(TRMMKERNEL)&&defined(LEFT)&&defined(TRANSA))||(defined(TRMMKERNEL)&&!defined(LEFT)&&!defined(TRANSA))
MOVQ bb,ptrbb;
#else
MOVQ bb, ptrbb;
MOVQ kk, %rax;
SALQ $ZBASE_SHIFT, %rax;
LEAQ (ptrba, %rax, 8), ptrba;
LEAQ (ptrbb, %rax, 2), ptrbb;
#endif
XOR_SY	yvec15, yvec15, yvec15;
XOR_SY	yvec14, yvec14, yvec14;
XOR_SY	yvec13, yvec13, yvec13;
XOR_SY	yvec12, yvec12, yvec12;
XOR_SY	yvec11, yvec11, yvec11;
XOR_SY	yvec10, yvec10, yvec10;
XOR_SY	yvec9, yvec9, yvec9;
XOR_SY	yvec8, yvec8, yvec8;
#ifndef	TRMMKERNEL
MOVQ bk,k;
#elif (defined(LEFT)&&!defined(TRANSA))||(!defined(LEFT)&&defined(TRANSA))
MOVQ bk, %rax;
SUBQ kk, %rax;
MOVQ %rax, kkk;
#else
MOVQ kk, %rax;
#ifdef	LEFT
ADDQ $8, %rax;
#else
ADDQ $2, %rax;
#endif
MOVQ %rax, kkk;
#endif
SARQ $2, k;
JLE .L211_loopE;
ALIGN_5
.L211_bodyB:
EDUP_SX	0*SIZE(ptrbb), xvec4;
ODUP_SX	0*SIZE(ptrbb), xvec5;
SHUF_SX	$0x4e, xvec4, xvec6;
SHUF_SX	$0x4e, xvec5, xvec7;

LD_SX	0*SIZE(ptrba), xvec0;
MOV_SX	xvec0, xvec1;
MUL_SX xvec4, xvec0, xvec0;
ADD1_SX xvec0, xvec15, xvec15;
SHUF_SX	$0xb1, xvec1, xvec2;
MUL_SX xvec6, xvec1, xvec1;
ADD1_SX xvec1, xvec11, xvec11;

MOV_SX	xvec2, xvec3;
MUL_SX xvec5, xvec2, xvec2;
ADD2_SX xvec2, xvec15, xvec15;
MUL_SX xvec7, xvec3, xvec3;
ADD2_SX xvec3, xvec11, xvec11;

LD_SX	4*SIZE(ptrba), xvec0;
MOV_SX	xvec0, xvec1;
MUL_SX xvec4, xvec0, xvec0;
ADD1_SX xvec0, xvec14, xvec14;
SHUF_SX	$0xb1, xvec1, xvec2;
MUL_SX xvec6, xvec1, xvec1;
ADD1_SX xvec1, xvec10, xvec10;

MOV_SX	xvec2, xvec3;
MUL_SX xvec5, xvec2, xvec2;
ADD2_SX xvec2, xvec14, xvec14;
MUL_SX xvec7, xvec3, xvec3;
ADD2_SX xvec3, xvec10, xvec10;

LD_SX	8*SIZE(ptrba), xvec0;
MOV_SX	xvec0, xvec1;
MUL_SX xvec4, xvec0, xvec0;
ADD1_SX xvec0, xvec13, xvec13;
SHUF_SX	$0xb1, xvec1, xvec2;
MUL_SX xvec6, xvec1, xvec1;
ADD1_SX xvec1, xvec9, xvec9;

MOV_SX	xvec2, xvec3;
MUL_SX xvec5, xvec2, xvec2;
ADD2_SX xvec2, xvec13, xvec13;
MUL_SX xvec7, xvec3, xvec3;
ADD2_SX xvec3, xvec9, xvec9;

LD_SX	12*SIZE(ptrba), xvec0;
MOV_SX	xvec0, xvec1;
MUL_SX xvec4, xvec0, xvec0;
ADD1_SX xvec0, xvec12, xvec12;
SHUF_SX	$0xb1, xvec1, xvec2;
MUL_SX xvec6, xvec1, xvec1;
ADD1_SX xvec1, xvec8, xvec8;

MOV_SX	xvec2, xvec3;
MUL_SX xvec5, xvec2, xvec2;
ADD2_SX xvec2, xvec12, xvec12;
MUL_SX xvec7, xvec3, xvec3;
ADD2_SX xvec3, xvec8, xvec8;

EDUP_SX	4*SIZE(ptrbb), xvec4;
ODUP_SX	4*SIZE(ptrbb), xvec5;
SHUF_SX	$0x4e, xvec4, xvec6;
SHUF_SX	$0x4e, xvec5, xvec7;

LD_SX	16*SIZE(ptrba), xvec0;
MOV_SX	xvec0, xvec1;
MUL_SX xvec4, xvec0, xvec0;
ADD1_SX xvec0, xvec15, xvec15;
SHUF_SX	$0xb1, xvec1, xvec2;
MUL_SX xvec6, xvec1, xvec1;
ADD1_SX xvec1, xvec11, xvec11;

MOV_SX	xvec2, xvec3;
MUL_SX xvec5, xvec2, xvec2;
ADD2_SX xvec2, xvec15, xvec15;
MUL_SX xvec7, xvec3, xvec3;
ADD2_SX xvec3, xvec11, xvec11;

LD_SX	20*SIZE(ptrba), xvec0;
MOV_SX	xvec0, xvec1;
MUL_SX xvec4, xvec0, xvec0;
ADD1_SX xvec0, xvec14, xvec14;
SHUF_SX	$0xb1, xvec1, xvec2;
MUL_SX xvec6, xvec1, xvec1;
ADD1_SX xvec1, xvec10, xvec10;

MOV_SX	xvec2, xvec3;
MUL_SX xvec5, xvec2, xvec2;
ADD2_SX xvec2, xvec14, xvec14;
MUL_SX xvec7, xvec3, xvec3;
ADD2_SX xvec3, xvec10, xvec10;

LD_SX	24*SIZE(ptrba), xvec0;
MOV_SX	xvec0, xvec1;
MUL_SX xvec4, xvec0, xvec0;
ADD1_SX xvec0, xvec13, xvec13;
SHUF_SX	$0xb1, xvec1, xvec2;
MUL_SX xvec6, xvec1, xvec1;
ADD1_SX xvec1, xvec9, xvec9;

MOV_SX	xvec2, xvec3;
MUL_SX xvec5, xvec2, xvec2;
ADD2_SX xvec2, xvec13, xvec13;
MUL_SX xvec7, xvec3, xvec3;
ADD2_SX xvec3, xvec9, xvec9;

LD_SX	28*SIZE(ptrba), xvec0;
MOV_SX	xvec0, xvec1;
MUL_SX xvec4, xvec0, xvec0;
ADD1_SX xvec0, xvec12, xvec12;
SHUF_SX	$0xb1, xvec1, xvec2;
MUL_SX xvec6, xvec1, xvec1;
ADD1_SX xvec1, xvec8, xvec8;

MOV_SX	xvec2, xvec3;
MUL_SX xvec5, xvec2, xvec2;
ADD2_SX xvec2, xvec12, xvec12;
MUL_SX xvec7, xvec3, xvec3;
ADD2_SX xvec3, xvec8, xvec8;

EDUP_SX	8*SIZE(ptrbb), xvec4;
ODUP_SX	8*SIZE(ptrbb), xvec5;
SHUF_SX	$0x4e, xvec4, xvec6;
SHUF_SX	$0x4e, xvec5, xvec7;

LD_SX	32*SIZE(ptrba), xvec0;
MOV_SX	xvec0, xvec1;
MUL_SX xvec4, xvec0, xvec0;
ADD1_SX xvec0, xvec15, xvec15;
SHUF_SX	$0xb1, xvec1, xvec2;
MUL_SX xvec6, xvec1, xvec1;
ADD1_SX xvec1, xvec11, xvec11;

MOV_SX	xvec2, xvec3;
MUL_SX xvec5, xvec2, xvec2;
ADD2_SX xvec2, xvec15, xvec15;
MUL_SX xvec7, xvec3, xvec3;
ADD2_SX xvec3, xvec11, xvec11;

LD_SX	36*SIZE(ptrba), xvec0;
MOV_SX	xvec0, xvec1;
MUL_SX xvec4, xvec0, xvec0;
ADD1_SX xvec0, xvec14, xvec14;
SHUF_SX	$0xb1, xvec1, xvec2;
MUL_SX xvec6, xvec1, xvec1;
ADD1_SX xvec1, xvec10, xvec10;

MOV_SX	xvec2, xvec3;
MUL_SX xvec5, xvec2, xvec2;
ADD2_SX xvec2, xvec14, xvec14;
MUL_SX xvec7, xvec3, xvec3;
ADD2_SX xvec3, xvec10, xvec10;

LD_SX	40*SIZE(ptrba), xvec0;
MOV_SX	xvec0, xvec1;
MUL_SX xvec4, xvec0, xvec0;
ADD1_SX xvec0, xvec13, xvec13;
SHUF_SX	$0xb1, xvec1, xvec2;
MUL_SX xvec6, xvec1, xvec1;
ADD1_SX xvec1, xvec9, xvec9;

MOV_SX	xvec2, xvec3;
MUL_SX xvec5, xvec2, xvec2;
ADD2_SX xvec2, xvec13, xvec13;
MUL_SX xvec7, xvec3, xvec3;
ADD2_SX xvec3, xvec9, xvec9;

LD_SX	44*SIZE(ptrba), xvec0;
MOV_SX	xvec0, xvec1;
MUL_SX xvec4, xvec0, xvec0;
ADD1_SX xvec0, xvec12, xvec12;
SHUF_SX	$0xb1, xvec1, xvec2;
MUL_SX xvec6, xvec1, xvec1;
ADD1_SX xvec1, xvec8, xvec8;

MOV_SX	xvec2, xvec3;
MUL_SX xvec5, xvec2, xvec2;
ADD2_SX xvec2, xvec12, xvec12;
MUL_SX xvec7, xvec3, xvec3;
ADD2_SX xvec3, xvec8, xvec8;

EDUP_SX	12*SIZE(ptrbb), xvec4;
ODUP_SX	12*SIZE(ptrbb), xvec5;
SHUF_SX	$0x4e, xvec4, xvec6;
SHUF_SX	$0x4e, xvec5, xvec7;

LD_SX	48*SIZE(ptrba), xvec0;
MOV_SX	xvec0, xvec1;
MUL_SX xvec4, xvec0, xvec0;
ADD1_SX xvec0, xvec15, xvec15;
SHUF_SX	$0xb1, xvec1, xvec2;
MUL_SX xvec6, xvec1, xvec1;
ADD1_SX xvec1, xvec11, xvec11;

MOV_SX	xvec2, xvec3;
MUL_SX xvec5, xvec2, xvec2;
ADD2_SX xvec2, xvec15, xvec15;
MUL_SX xvec7, xvec3, xvec3;
ADD2_SX xvec3, xvec11, xvec11;

LD_SX	52*SIZE(ptrba), xvec0;
MOV_SX	xvec0, xvec1;
MUL_SX xvec4, xvec0, xvec0;
ADD1_SX xvec0, xvec14, xvec14;
SHUF_SX	$0xb1, xvec1, xvec2;
MUL_SX xvec6, xvec1, xvec1;
ADD1_SX xvec1, xvec10, xvec10;

MOV_SX	xvec2, xvec3;
MUL_SX xvec5, xvec2, xvec2;
ADD2_SX xvec2, xvec14, xvec14;
MUL_SX xvec7, xvec3, xvec3;
ADD2_SX xvec3, xvec10, xvec10;

LD_SX	56*SIZE(ptrba), xvec0;
MOV_SX	xvec0, xvec1;
MUL_SX xvec4, xvec0, xvec0;
ADD1_SX xvec0, xvec13, xvec13;
SHUF_SX	$0xb1, xvec1, xvec2;
MUL_SX xvec6, xvec1, xvec1;
ADD1_SX xvec1, xvec9, xvec9;

MOV_SX	xvec2, xvec3;
MUL_SX xvec5, xvec2, xvec2;
ADD2_SX xvec2, xvec13, xvec13;
MUL_SX xvec7, xvec3, xvec3;
ADD2_SX xvec3, xvec9, xvec9;

LD_SX	60*SIZE(ptrba), xvec0;
MOV_SX	xvec0, xvec1;
MUL_SX xvec4, xvec0, xvec0;
ADD1_SX xvec0, xvec12, xvec12;
SHUF_SX	$0xb1, xvec1, xvec2;
MUL_SX xvec6, xvec1, xvec1;
ADD1_SX xvec1, xvec8, xvec8;

MOV_SX	xvec2, xvec3;
MUL_SX xvec5, xvec2, xvec2;
ADD2_SX xvec2, xvec12, xvec12;
MUL_SX xvec7, xvec3, xvec3;
ADD2_SX xvec3, xvec8, xvec8;
ADDQ	$64*SIZE, ptrba;
ADDQ	$16*SIZE, ptrbb;
DECQ 	k;
JG	.L211_bodyB;
ALIGN_5
.L211_loopE:
#ifndef	TRMMKERNEL
TEST $2, bk;
#else
TEST $2, kkk;
#endif
JLE .L212_loopE;
ALIGN_5
.L212_bodyB:
EDUP_SX	0*SIZE(ptrbb), xvec4;
ODUP_SX	0*SIZE(ptrbb), xvec5;
SHUF_SX	$0x4e, xvec4, xvec6;
SHUF_SX	$0x4e, xvec5, xvec7;

LD_SX	0*SIZE(ptrba), xvec0;
MOV_SX	xvec0, xvec1;
MUL_SX xvec4, xvec0, xvec0;
ADD1_SX xvec0, xvec15, xvec15;
SHUF_SX	$0xb1, xvec1, xvec2;
MUL_SX xvec6, xvec1, xvec1;
ADD1_SX xvec1, xvec11, xvec11;

MOV_SX	xvec2, xvec3;
MUL_SX xvec5, xvec2, xvec2;
ADD2_SX xvec2, xvec15, xvec15;
MUL_SX xvec7, xvec3, xvec3;
ADD2_SX xvec3, xvec11, xvec11;

LD_SX	4*SIZE(ptrba), xvec0;
MOV_SX	xvec0, xvec1;
MUL_SX xvec4, xvec0, xvec0;
ADD1_SX xvec0, xvec14, xvec14;
SHUF_SX	$0xb1, xvec1, xvec2;
MUL_SX xvec6, xvec1, xvec1;
ADD1_SX xvec1, xvec10, xvec10;

MOV_SX	xvec2, xvec3;
MUL_SX xvec5, xvec2, xvec2;
ADD2_SX xvec2, xvec14, xvec14;
MUL_SX xvec7, xvec3, xvec3;
ADD2_SX xvec3, xvec10, xvec10;

LD_SX	8*SIZE(ptrba), xvec0;
MOV_SX	xvec0, xvec1;
MUL_SX xvec4, xvec0, xvec0;
ADD1_SX xvec0, xvec13, xvec13;
SHUF_SX	$0xb1, xvec1, xvec2;
MUL_SX xvec6, xvec1, xvec1;
ADD1_SX xvec1, xvec9, xvec9;

MOV_SX	xvec2, xvec3;
MUL_SX xvec5, xvec2, xvec2;
ADD2_SX xvec2, xvec13, xvec13;
MUL_SX xvec7, xvec3, xvec3;
ADD2_SX xvec3, xvec9, xvec9;

LD_SX	12*SIZE(ptrba), xvec0;
MOV_SX	xvec0, xvec1;
MUL_SX xvec4, xvec0, xvec0;
ADD1_SX xvec0, xvec12, xvec12;
SHUF_SX	$0xb1, xvec1, xvec2;
MUL_SX xvec6, xvec1, xvec1;
ADD1_SX xvec1, xvec8, xvec8;

MOV_SX	xvec2, xvec3;
MUL_SX xvec5, xvec2, xvec2;
ADD2_SX xvec2, xvec12, xvec12;
MUL_SX xvec7, xvec3, xvec3;
ADD2_SX xvec3, xvec8, xvec8;

EDUP_SX	4*SIZE(ptrbb), xvec4;
ODUP_SX	4*SIZE(ptrbb), xvec5;
SHUF_SX	$0x4e, xvec4, xvec6;
SHUF_SX	$0x4e, xvec5, xvec7;

LD_SX	16*SIZE(ptrba), xvec0;
MOV_SX	xvec0, xvec1;
MUL_SX xvec4, xvec0, xvec0;
ADD1_SX xvec0, xvec15, xvec15;
SHUF_SX	$0xb1, xvec1, xvec2;
MUL_SX xvec6, xvec1, xvec1;
ADD1_SX xvec1, xvec11, xvec11;

MOV_SX	xvec2, xvec3;
MUL_SX xvec5, xvec2, xvec2;
ADD2_SX xvec2, xvec15, xvec15;
MUL_SX xvec7, xvec3, xvec3;
ADD2_SX xvec3, xvec11, xvec11;

LD_SX	20*SIZE(ptrba), xvec0;
MOV_SX	xvec0, xvec1;
MUL_SX xvec4, xvec0, xvec0;
ADD1_SX xvec0, xvec14, xvec14;
SHUF_SX	$0xb1, xvec1, xvec2;
MUL_SX xvec6, xvec1, xvec1;
ADD1_SX xvec1, xvec10, xvec10;

MOV_SX	xvec2, xvec3;
MUL_SX xvec5, xvec2, xvec2;
ADD2_SX xvec2, xvec14, xvec14;
MUL_SX xvec7, xvec3, xvec3;
ADD2_SX xvec3, xvec10, xvec10;

LD_SX	24*SIZE(ptrba), xvec0;
MOV_SX	xvec0, xvec1;
MUL_SX xvec4, xvec0, xvec0;
ADD1_SX xvec0, xvec13, xvec13;
SHUF_SX	$0xb1, xvec1, xvec2;
MUL_SX xvec6, xvec1, xvec1;
ADD1_SX xvec1, xvec9, xvec9;

MOV_SX	xvec2, xvec3;
MUL_SX xvec5, xvec2, xvec2;
ADD2_SX xvec2, xvec13, xvec13;
MUL_SX xvec7, xvec3, xvec3;
ADD2_SX xvec3, xvec9, xvec9;

LD_SX	28*SIZE(ptrba), xvec0;
MOV_SX	xvec0, xvec1;
MUL_SX xvec4, xvec0, xvec0;
ADD1_SX xvec0, xvec12, xvec12;
SHUF_SX	$0xb1, xvec1, xvec2;
MUL_SX xvec6, xvec1, xvec1;
ADD1_SX xvec1, xvec8, xvec8;

MOV_SX	xvec2, xvec3;
MUL_SX xvec5, xvec2, xvec2;
ADD2_SX xvec2, xvec12, xvec12;
MUL_SX xvec7, xvec3, xvec3;
ADD2_SX xvec3, xvec8, xvec8;
ADDQ	$32*SIZE, ptrba;
ADDQ	$8*SIZE, ptrbb;

.L212_loopE:
#ifndef	TRMMKERNEL
TEST $1, bk;
#else
TEST $1, kkk;
#endif
JLE .L213_loopE;
ALIGN_5
.L213_bodyB:
EDUP_SX	0*SIZE(ptrbb), xvec4;
ODUP_SX	0*SIZE(ptrbb), xvec5;
SHUF_SX	$0x4e, xvec4, xvec6;
SHUF_SX	$0x4e, xvec5, xvec7;

LD_SX	0*SIZE(ptrba), xvec0;
MOV_SX	xvec0, xvec1;
MUL_SX xvec4, xvec0, xvec0;
ADD1_SX xvec0, xvec15, xvec15;
SHUF_SX	$0xb1, xvec1, xvec2;
MUL_SX xvec6, xvec1, xvec1;
ADD1_SX xvec1, xvec11, xvec11;

MOV_SX	xvec2, xvec3;
MUL_SX xvec5, xvec2, xvec2;
ADD2_SX xvec2, xvec15, xvec15;
MUL_SX xvec7, xvec3, xvec3;
ADD2_SX xvec3, xvec11, xvec11;

LD_SX	4*SIZE(ptrba), xvec0;
MOV_SX	xvec0, xvec1;
MUL_SX xvec4, xvec0, xvec0;
ADD1_SX xvec0, xvec14, xvec14;
SHUF_SX	$0xb1, xvec1, xvec2;
MUL_SX xvec6, xvec1, xvec1;
ADD1_SX xvec1, xvec10, xvec10;

MOV_SX	xvec2, xvec3;
MUL_SX xvec5, xvec2, xvec2;
ADD2_SX xvec2, xvec14, xvec14;
MUL_SX xvec7, xvec3, xvec3;
ADD2_SX xvec3, xvec10, xvec10;

LD_SX	8*SIZE(ptrba), xvec0;
MOV_SX	xvec0, xvec1;
MUL_SX xvec4, xvec0, xvec0;
ADD1_SX xvec0, xvec13, xvec13;
SHUF_SX	$0xb1, xvec1, xvec2;
MUL_SX xvec6, xvec1, xvec1;
ADD1_SX xvec1, xvec9, xvec9;

MOV_SX	xvec2, xvec3;
MUL_SX xvec5, xvec2, xvec2;
ADD2_SX xvec2, xvec13, xvec13;
MUL_SX xvec7, xvec3, xvec3;
ADD2_SX xvec3, xvec9, xvec9;

LD_SX	12*SIZE(ptrba), xvec0;
MOV_SX	xvec0, xvec1;
MUL_SX xvec4, xvec0, xvec0;
ADD1_SX xvec0, xvec12, xvec12;
SHUF_SX	$0xb1, xvec1, xvec2;
MUL_SX xvec6, xvec1, xvec1;
ADD1_SX xvec1, xvec8, xvec8;

MOV_SX	xvec2, xvec3;
MUL_SX xvec5, xvec2, xvec2;
ADD2_SX xvec2, xvec12, xvec12;
MUL_SX xvec7, xvec3, xvec3;
ADD2_SX xvec3, xvec8, xvec8;
ADDQ	$16*SIZE, ptrba;
ADDQ	$4*SIZE, ptrbb

.L213_loopE:
#### Handle ####
#if  defined(RN) || defined(RT) || defined(CN) || defined(CT)
XOR_SY	yvec7, yvec7, yvec7;
ADDSUB_SX xvec15, xvec7, xvec7;
MOV_SX		xvec7, xvec15;
XOR_SY	yvec7, yvec7, yvec7;
ADDSUB_SX xvec14, xvec7, xvec7;
MOV_SX		xvec7, xvec14;
XOR_SY	yvec7, yvec7, yvec7;
ADDSUB_SX xvec13, xvec7, xvec7;
MOV_SX		xvec7, xvec13;
XOR_SY	yvec7, yvec7, yvec7;
ADDSUB_SX xvec12, xvec7, xvec7;
MOV_SX		xvec7, xvec12;
XOR_SY	yvec7, yvec7, yvec7;
ADDSUB_SX xvec11, xvec7, xvec7;
MOV_SX		xvec7, xvec11;
XOR_SY	yvec7, yvec7, yvec7;
ADDSUB_SX xvec10, xvec7, xvec7;
MOV_SX		xvec7, xvec10;
XOR_SY	yvec7, yvec7, yvec7;
ADDSUB_SX xvec9, xvec7, xvec7;
MOV_SX		xvec7, xvec9;
XOR_SY	yvec7, yvec7, yvec7;
ADDSUB_SX xvec8, xvec7, xvec7;
MOV_SX		xvec7, xvec8;
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
XOR_SY	yvec7, yvec7, yvec7;
SUB_SX xvec15, xvec7, xvec7;
MOV_SX	xvec7, xvec15;
XOR_SY	yvec7, yvec7, yvec7;
SUB_SX xvec14, xvec7, xvec7;
MOV_SX	xvec7, xvec14;
XOR_SY	yvec7, yvec7, yvec7;
SUB_SX xvec13, xvec7, xvec7;
MOV_SX	xvec7, xvec13;
XOR_SY	yvec7, yvec7, yvec7;
SUB_SX xvec12, xvec7, xvec7;
MOV_SX	xvec7, xvec12;
XOR_SY	yvec7, yvec7, yvec7;
SUB_SX xvec11, xvec7, xvec7;
MOV_SX	xvec7, xvec11;
XOR_SY	yvec7, yvec7, yvec7;
SUB_SX xvec10, xvec7, xvec7;
MOV_SX	xvec7, xvec10;
XOR_SY	yvec7, yvec7, yvec7;
SUB_SX xvec9, xvec7, xvec7;
MOV_SX	xvec7, xvec9;
XOR_SY	yvec7, yvec7, yvec7;
SUB_SX xvec8, xvec7, xvec7;
MOV_SX	xvec7, xvec8;
#elif defined(RR) || defined(RC) || defined(CR) || defined(CC)
SHUF_SX $0xb1, xvec15, xvec15;
SHUF_SX $0xb1, xvec14, xvec14;
SHUF_SX $0xb1, xvec13, xvec13;
SHUF_SX $0xb1, xvec12, xvec12;
SHUF_SX $0xb1, xvec11, xvec11;
SHUF_SX $0xb1, xvec10, xvec10;
SHUF_SX $0xb1, xvec9, xvec9;
SHUF_SX $0xb1, xvec8, xvec8;
XOR_SY	yvec7, yvec7, yvec7;
ADDSUB_SX xvec15, xvec7, xvec7;
MOV_SX		xvec7, xvec15;
XOR_SY	yvec7, yvec7, yvec7;
ADDSUB_SX xvec14, xvec7, xvec7;
MOV_SX		xvec7, xvec14;
XOR_SY	yvec7, yvec7, yvec7;
ADDSUB_SX xvec13, xvec7, xvec7;
MOV_SX		xvec7, xvec13;
XOR_SY	yvec7, yvec7, yvec7;
ADDSUB_SX xvec12, xvec7, xvec7;
MOV_SX		xvec7, xvec12;
XOR_SY	yvec7, yvec7, yvec7;
ADDSUB_SX xvec11, xvec7, xvec7;
MOV_SX		xvec7, xvec11;
XOR_SY	yvec7, yvec7, yvec7;
ADDSUB_SX xvec10, xvec7, xvec7;
MOV_SX		xvec7, xvec10;
XOR_SY	yvec7, yvec7, yvec7;
ADDSUB_SX xvec9, xvec7, xvec7;
MOV_SX		xvec7, xvec9;
XOR_SY	yvec7, yvec7, yvec7;
ADDSUB_SX xvec8, xvec7, xvec7;
MOV_SX		xvec7, xvec8;
SHUF_SX $0xb1, xvec15, xvec15;
SHUF_SX $0xb1, xvec14, xvec14;
SHUF_SX $0xb1, xvec13, xvec13;
SHUF_SX $0xb1, xvec12, xvec12;
SHUF_SX $0xb1, xvec11, xvec11;
SHUF_SX $0xb1, xvec10, xvec10;
SHUF_SX $0xb1, xvec9, xvec9;
SHUF_SX	$0xb1, xvec8, xvec8;
#endif
#### Mulitply Alpha ####
BROAD_SX MEMALPHA_R, xvec7;
BROAD_SX MEMALPHA_I, xvec6;
#### Writng back ####
VPERMILP_SX $0xb1,xvec15, xvec5;
MUL_SX xvec7, xvec15, xvec15;
MUL_SX xvec6, xvec5, xvec5;
ADDSUB_SX xvec5, xvec15, xvec15;
VPERMILP_SX $0xb1,xvec14, xvec4;
MUL_SX xvec7, xvec14, xvec14;
MUL_SX xvec6, xvec4, xvec4;
ADDSUB_SX xvec4, xvec14, xvec14;
VPERMILP_SX $0xb1,xvec13, xvec3;
MUL_SX xvec7, xvec13, xvec13;
MUL_SX xvec6, xvec3, xvec3;
ADDSUB_SX xvec3, xvec13, xvec13;
VPERMILP_SX $0xb1,xvec12, xvec2;
MUL_SX xvec7, xvec12, xvec12;
MUL_SX xvec6, xvec2, xvec2;
ADDSUB_SX xvec2, xvec12, xvec12;
VPERMILP_SX $0xb1,xvec11, xvec1;
MUL_SX xvec7, xvec11, xvec11;
MUL_SX xvec6, xvec1, xvec1;
ADDSUB_SX xvec1, xvec11, xvec11;
VPERMILP_SX $0xb1,xvec10, xvec0;
MUL_SX xvec7, xvec10, xvec10;
MUL_SX xvec6, xvec0, xvec0;
ADDSUB_SX xvec0, xvec10, xvec10;
VPERMILP_SX $0xb1,xvec9, xvec5;
MUL_SX xvec7, xvec9, xvec9;
MUL_SX xvec6, xvec5, xvec5;
ADDSUB_SX xvec5, xvec9, xvec9;
VPERMILP_SX $0xb1,xvec8, xvec4;
MUL_SX xvec7, xvec8, xvec8;
MUL_SX xvec6, xvec4, xvec4;
ADDSUB_SX xvec4, xvec8, xvec8;
#ifndef	TRMMKERNEL
LDL_SX 0*SIZE(C0), xvec0, xvec0;
LDH_SX 2*SIZE(C1), xvec0, xvec0;
LDL_SX 4*SIZE(C0), xvec1, xvec1;
LDH_SX 6*SIZE(C1), xvec1, xvec1;
LDL_SX 8*SIZE(C0), xvec2, xvec2;
LDH_SX 10*SIZE(C1), xvec2, xvec2;
LDL_SX 12*SIZE(C0), xvec3, xvec3;
LDH_SX 14*SIZE(C1), xvec3, xvec3;
ADD_SX xvec0, xvec15, xvec15;
ADD_SX xvec1, xvec14, xvec14;
ADD_SX xvec2, xvec13, xvec13;
ADD_SX xvec3, xvec12, xvec12;
#endif
STL_SX	xvec15, 0*SIZE(C0);
STH_SX	xvec15, 2*SIZE(C1);
STL_SX	xvec14, 4*SIZE(C0);
STH_SX	xvec14, 6*SIZE(C1);
STL_SX	xvec13, 8*SIZE(C0);
STH_SX	xvec13, 10*SIZE(C1);
STL_SX	xvec12, 12*SIZE(C0);
STH_SX	xvec12, 14*SIZE(C1);
#ifndef	TRMMKERNEL
LDL_SX 0*SIZE(C1), xvec4, xvec4;
LDH_SX 2*SIZE(C0), xvec4, xvec4;
LDL_SX 4*SIZE(C1), xvec5, xvec5;
LDH_SX 6*SIZE(C0), xvec5, xvec5;
LDL_SX 8*SIZE(C1), xvec6, xvec6;
LDH_SX 10*SIZE(C0), xvec6, xvec6;
LDL_SX 12*SIZE(C1), xvec7, xvec7;
LDH_SX 14*SIZE(C0), xvec7, xvec7;
ADD_SX xvec4, xvec11, xvec11;
ADD_SX xvec5, xvec10, xvec10;
ADD_SX xvec6, xvec9, xvec9;
ADD_SX xvec7, xvec8, xvec8;
#endif
STL_SX	xvec11, 0*SIZE(C1);
STH_SX	xvec11, 2*SIZE(C0);
STL_SX	xvec10, 4*SIZE(C1);
STH_SX	xvec10, 6*SIZE(C0);
STL_SX	xvec9, 8*SIZE(C1);
STH_SX	xvec9, 10*SIZE(C0);
STL_SX	xvec8, 12*SIZE(C1);
STH_SX	xvec8, 14*SIZE(C0);
#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA))||(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
MOVQ bk, %rax;
SUBQ kkk, %rax;
SALQ $ZBASE_SHIFT, %rax;
LEAQ (ptrba, %rax, 8), ptrba;
LEAQ (ptrbb, %rax, 2), ptrbb;
#endif
#if defined(TRMMKERNEL) && defined(LEFT)
ADDQ	$8, kk;
#endif

ADDQ	$16*SIZE, C0;
ADDQ	$16*SIZE, C1;
DECQ	i;
JG .L21_bodyB;
ALIGN_5
.L21_loopE:
TEST $4, bm;
JLE .L22_loopE;
ALIGN_5
.L22_bodyB:
#if !defined(TRMMKERNEL)||(defined(TRMMKERNEL)&&defined(LEFT)&&defined(TRANSA))||(defined(TRMMKERNEL)&&!defined(LEFT)&&!defined(TRANSA))
MOVQ bb,ptrbb;
#else
MOVQ bb, ptrbb;
MOVQ kk, %rax;
SALQ $ZBASE_SHIFT, %rax;
LEAQ (ptrba, %rax, 4), ptrba;
LEAQ (ptrbb, %rax, 2), ptrbb;
#endif
XOR_SY	yvec15, yvec15, yvec15;
XOR_SY	yvec14, yvec14, yvec14;
XOR_SY	yvec11, yvec11, yvec11;
XOR_SY	yvec10, yvec10, yvec10;
#ifndef	TRMMKERNEL
MOVQ bk,k;
#elif (defined(LEFT)&&!defined(TRANSA))||(!defined(LEFT)&&defined(TRANSA))
MOVQ bk, %rax;
SUBQ kk, %rax;
MOVQ %rax, kkk;
#else
MOVQ kk, %rax;
#ifdef	LEFT
ADDQ $4, %rax;
#else
ADDQ $2, %rax;
#endif
MOVQ %rax, kkk;
#endif

SARQ $2, k;
JLE .L221_loopE;
ALIGN_5
.L221_bodyB:
EDUP_SX	0*SIZE(ptrbb), xvec4;
ODUP_SX	0*SIZE(ptrbb), xvec5;
SHUF_SX	$0x4e, xvec4, xvec6;
SHUF_SX	$0x4e, xvec5, xvec7;

LD_SX	0*SIZE(ptrba), xvec0;
MOV_SX	xvec0, xvec1;
MUL_SX xvec4, xvec0, xvec0;
ADD1_SX xvec0, xvec15, xvec15;
SHUF_SX	$0xb1, xvec1, xvec2;
MUL_SX xvec6, xvec1, xvec1;
ADD1_SX xvec1, xvec11, xvec11;

MOV_SX	xvec2, xvec3;
MUL_SX xvec5, xvec2, xvec2;
ADD2_SX xvec2, xvec15, xvec15;
MUL_SX xvec7, xvec3, xvec3;
ADD2_SX xvec3, xvec11, xvec11;

LD_SX	4*SIZE(ptrba), xvec0;
MOV_SX	xvec0, xvec1;
MUL_SX xvec4, xvec0, xvec0;
ADD1_SX xvec0, xvec14, xvec14;
SHUF_SX	$0xb1, xvec1, xvec2;
MUL_SX xvec6, xvec1, xvec1;
ADD1_SX xvec1, xvec10, xvec10;

MOV_SX	xvec2, xvec3;
MUL_SX xvec5, xvec2, xvec2;
ADD2_SX xvec2, xvec14, xvec14;
MUL_SX xvec7, xvec3, xvec3;
ADD2_SX xvec3, xvec10, xvec10;

#### Unroll 2 #####
EDUP_SX	4*SIZE(ptrbb), xvec4;
ODUP_SX	4*SIZE(ptrbb), xvec5;
SHUF_SX	$0x4e, xvec4, xvec6;
SHUF_SX	$0x4e, xvec5, xvec7;

LD_SX	8*SIZE(ptrba), xvec0;
MOV_SX	xvec0, xvec1;
MUL_SX xvec4, xvec0, xvec0;
ADD1_SX xvec0, xvec15, xvec15;
SHUF_SX	$0xb1, xvec1, xvec2;
MUL_SX xvec6, xvec1, xvec1;
ADD1_SX xvec1, xvec11, xvec11;

MOV_SX	xvec2, xvec3;
MUL_SX xvec5, xvec2, xvec2;
ADD2_SX xvec2, xvec15, xvec15;
MUL_SX xvec7, xvec3, xvec3;
ADD2_SX xvec3, xvec11, xvec11;

LD_SX	12*SIZE(ptrba), xvec0;
MOV_SX	xvec0, xvec1;
MUL_SX xvec4, xvec0, xvec0;
ADD1_SX xvec0, xvec14, xvec14;
SHUF_SX	$0xb1, xvec1, xvec2;
MUL_SX xvec6, xvec1, xvec1;
ADD1_SX xvec1, xvec10, xvec10;

MOV_SX	xvec2, xvec3;
MUL_SX xvec5, xvec2, xvec2;
ADD2_SX xvec2, xvec14, xvec14;
MUL_SX xvec7, xvec3, xvec3;
ADD2_SX xvec3, xvec10, xvec10;

#### Unroll 3 ####
EDUP_SX	8*SIZE(ptrbb), xvec4;
ODUP_SX	8*SIZE(ptrbb), xvec5;
SHUF_SX	$0x4e, xvec4, xvec6;
SHUF_SX	$0x4e, xvec5, xvec7;

LD_SX	16*SIZE(ptrba), xvec0;
MOV_SX	xvec0, xvec1;
MUL_SX xvec4, xvec0, xvec0;
ADD1_SX xvec0, xvec15, xvec15;
SHUF_SX	$0xb1, xvec1, xvec2;
MUL_SX xvec6, xvec1, xvec1;
ADD1_SX xvec1, xvec11, xvec11;

MOV_SX	xvec2, xvec3;
MUL_SX xvec5, xvec2, xvec2;
ADD2_SX xvec2, xvec15, xvec15;
MUL_SX xvec7, xvec3, xvec3;
ADD2_SX xvec3, xvec11, xvec11;

LD_SX	20*SIZE(ptrba), xvec0;
MOV_SX	xvec0, xvec1;
MUL_SX xvec4, xvec0, xvec0;
ADD1_SX xvec0, xvec14, xvec14;
SHUF_SX	$0xb1, xvec1, xvec2;
MUL_SX xvec6, xvec1, xvec1;
ADD1_SX xvec1, xvec10, xvec10;

MOV_SX	xvec2, xvec3;
MUL_SX xvec5, xvec2, xvec2;
ADD2_SX xvec2, xvec14, xvec14;
MUL_SX xvec7, xvec3, xvec3;
ADD2_SX xvec3, xvec10, xvec10;

#### Unroll 4 ####
EDUP_SX	12*SIZE(ptrbb), xvec4;
ODUP_SX	12*SIZE(ptrbb), xvec5;
SHUF_SX	$0x4e, xvec4, xvec6;
SHUF_SX	$0x4e, xvec5, xvec7;

LD_SX	24*SIZE(ptrba), xvec0;
MOV_SX	xvec0, xvec1;
MUL_SX xvec4, xvec0, xvec0;
ADD1_SX xvec0, xvec15, xvec15;
SHUF_SX	$0xb1, xvec1, xvec2;
MUL_SX xvec6, xvec1, xvec1;
ADD1_SX xvec1, xvec11, xvec11;

MOV_SX	xvec2, xvec3;
MUL_SX xvec5, xvec2, xvec2;
ADD2_SX xvec2, xvec15, xvec15;
MUL_SX xvec7, xvec3, xvec3;
ADD2_SX xvec3, xvec11, xvec11;

LD_SX	28*SIZE(ptrba), xvec0;
MOV_SX	xvec0, xvec1;
MUL_SX xvec4, xvec0, xvec0;
ADD1_SX xvec0, xvec14, xvec14;
SHUF_SX	$0xb1, xvec1, xvec2;
MUL_SX xvec6, xvec1, xvec1;
ADD1_SX xvec1, xvec10, xvec10;

MOV_SX	xvec2, xvec3;
MUL_SX xvec5, xvec2, xvec2;
ADD2_SX xvec2, xvec14, xvec14;
MUL_SX xvec7, xvec3, xvec3;
ADD2_SX xvec3, xvec10, xvec10;
ADDQ	$32*SIZE, ptrba;
ADDQ	$16*SIZE, ptrbb;
DECQ	k;
JG	.L221_bodyB;
ALIGN_5
.L221_loopE:
#ifndef	TRMMKERNEL
TEST $2, bk;
#else
TEST $2, kkk;
#endif
JLE .L222_loopE;
ALIGN_5
.L222_bodyB:
EDUP_SX	0*SIZE(ptrbb), xvec4;
ODUP_SX	0*SIZE(ptrbb), xvec5;
SHUF_SX	$0x4e, xvec4, xvec6;
SHUF_SX	$0x4e, xvec5, xvec7;

LD_SX	0*SIZE(ptrba), xvec0;
MOV_SX	xvec0, xvec1;
MUL_SX xvec4, xvec0, xvec0;
ADD1_SX xvec0, xvec15, xvec15;
SHUF_SX	$0xb1, xvec1, xvec2;
MUL_SX xvec6, xvec1, xvec1;
ADD1_SX xvec1, xvec11, xvec11;

MOV_SX	xvec2, xvec3;
MUL_SX xvec5, xvec2, xvec2;
ADD2_SX xvec2, xvec15, xvec15;
MUL_SX xvec7, xvec3, xvec3;
ADD2_SX xvec3, xvec11, xvec11;

LD_SX	4*SIZE(ptrba), xvec0;
MOV_SX	xvec0, xvec1;
MUL_SX xvec4, xvec0, xvec0;
ADD1_SX xvec0, xvec14, xvec14;
SHUF_SX	$0xb1, xvec1, xvec2;
MUL_SX xvec6, xvec1, xvec1;
ADD1_SX xvec1, xvec10, xvec10;

MOV_SX	xvec2, xvec3;
MUL_SX xvec5, xvec2, xvec2;
ADD2_SX xvec2, xvec14, xvec14;
MUL_SX xvec7, xvec3, xvec3;
ADD2_SX xvec3, xvec10, xvec10;

#### Unroll 2 #####
EDUP_SX	4*SIZE(ptrbb), xvec4;
ODUP_SX	4*SIZE(ptrbb), xvec5;
SHUF_SX	$0x4e, xvec4, xvec6;
SHUF_SX	$0x4e, xvec5, xvec7;

LD_SX	8*SIZE(ptrba), xvec0;
MOV_SX	xvec0, xvec1;
MUL_SX xvec4, xvec0, xvec0;
ADD1_SX xvec0, xvec15, xvec15;
SHUF_SX	$0xb1, xvec1, xvec2;
MUL_SX xvec6, xvec1, xvec1;
ADD1_SX xvec1, xvec11, xvec11;

MOV_SX	xvec2, xvec3;
MUL_SX xvec5, xvec2, xvec2;
ADD2_SX xvec2, xvec15, xvec15;
MUL_SX xvec7, xvec3, xvec3;
ADD2_SX xvec3, xvec11, xvec11;

LD_SX	12*SIZE(ptrba), xvec0;
MOV_SX	xvec0, xvec1;
MUL_SX xvec4, xvec0, xvec0;
ADD1_SX xvec0, xvec14, xvec14;
SHUF_SX	$0xb1, xvec1, xvec2;
MUL_SX xvec6, xvec1, xvec1;
ADD1_SX xvec1, xvec10, xvec10;

MOV_SX	xvec2, xvec3;
MUL_SX xvec5, xvec2, xvec2;
ADD2_SX xvec2, xvec14, xvec14;
MUL_SX xvec7, xvec3, xvec3;
ADD2_SX xvec3, xvec10, xvec10;
ADDQ	$16*SIZE, ptrba;
ADDQ	$8*SIZE, ptrbb;


.L222_loopE:
#ifndef	TRMMKERNEL
TEST $1, bk;
#else
TEST $1, kkk;
#endif
JLE .L223_loopE;
ALIGN_5
.L223_bodyB:
EDUP_SX	0*SIZE(ptrbb), xvec4;
ODUP_SX	0*SIZE(ptrbb), xvec5;
SHUF_SX	$0x4e, xvec4, xvec6;
SHUF_SX	$0x4e, xvec5, xvec7;

LD_SX	0*SIZE(ptrba), xvec0;
MOV_SX	xvec0, xvec1;
MUL_SX xvec4, xvec0, xvec0;
ADD1_SX xvec0, xvec15, xvec15;
SHUF_SX	$0xb1, xvec1, xvec2;
MUL_SX xvec6, xvec1, xvec1;
ADD1_SX xvec1, xvec11, xvec11;

MOV_SX	xvec2, xvec3;
MUL_SX xvec5, xvec2, xvec2;
ADD2_SX xvec2, xvec15, xvec15;
MUL_SX xvec7, xvec3, xvec3;
ADD2_SX xvec3, xvec11, xvec11;

LD_SX	4*SIZE(ptrba), xvec0;
MOV_SX	xvec0, xvec1;
MUL_SX xvec4, xvec0, xvec0;
ADD1_SX xvec0, xvec14, xvec14;
SHUF_SX	$0xb1, xvec1, xvec2;
MUL_SX xvec6, xvec1, xvec1;
ADD1_SX xvec1, xvec10, xvec10;

MOV_SX	xvec2, xvec3;
MUL_SX xvec5, xvec2, xvec2;
ADD2_SX xvec2, xvec14, xvec14;
MUL_SX xvec7, xvec3, xvec3;
ADD2_SX xvec3, xvec10, xvec10;
ADDQ	$8*SIZE, ptrba;
ADDQ	$4*SIZE, ptrbb;

.L223_loopE:
#### Handle ####
#if  defined(RN) || defined(RT) || defined(CN) || defined(CT)
XOR_SY	yvec7, yvec7, yvec7;
ADDSUB_SX xvec15, xvec7, xvec7;
MOV_SX		xvec7, xvec15;
XOR_SY	yvec7, yvec7, yvec7;
ADDSUB_SX xvec14, xvec7, xvec7;
MOV_SX		xvec7, xvec14;
XOR_SY	yvec7, yvec7, yvec7;
ADDSUB_SX xvec11, xvec7, xvec7;
MOV_SX		xvec7, xvec11;
XOR_SY	yvec7, yvec7, yvec7;
ADDSUB_SX xvec10, xvec7, xvec7;
MOV_SX		xvec7, xvec10;
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
XOR_SY	yvec7, yvec7, yvec7;
SUB_SX xvec15, xvec7, xvec7;
MOV_SX	xvec7, xvec15;
XOR_SY	yvec7, yvec7, yvec7;
SUB_SX xvec14, xvec7, xvec7;
MOV_SX	xvec7, xvec14;
XOR_SY	yvec7, yvec7, yvec7;
SUB_SX xvec11, xvec7, xvec7;
MOV_SX	xvec7, xvec11;
XOR_SY	yvec7, yvec7, yvec7;
SUB_SX xvec10, xvec7, xvec7;
MOV_SX	xvec7, xvec10;
#elif defined(RR) || defined(RC) || defined(CR) || defined(CC)
SHUF_SX $0xb1, xvec15, xvec15;
SHUF_SX $0xb1, xvec14, xvec14;
SHUF_SX $0xb1, xvec11, xvec11;
SHUF_SX $0xb1, xvec10, xvec10;
XOR_SY	yvec7, yvec7, yvec7;
ADDSUB_SX xvec15, xvec7, xvec7;
MOV_SX		xvec7, xvec15;
XOR_SY	yvec7, yvec7, yvec7;
ADDSUB_SX xvec14, xvec7, xvec7;
MOV_SX		xvec7, xvec14;
XOR_SY	yvec7, yvec7, yvec7;
ADDSUB_SX xvec11, xvec7, xvec7;
MOV_SX		xvec7, xvec11;
XOR_SY	yvec7, yvec7, yvec7;
ADDSUB_SX xvec10, xvec7, xvec7;
MOV_SX		xvec7, xvec10;
SHUF_SX $0xb1, xvec15, xvec15;
SHUF_SX $0xb1, xvec14, xvec14;
SHUF_SX $0xb1, xvec11, xvec11;
SHUF_SX $0xb1, xvec10, xvec10;
#endif
#### Mulitply Alpha ####
BROAD_SX MEMALPHA_R, xvec7;
BROAD_SX MEMALPHA_I, xvec6;
#### Writng back ####
VPERMILP_SX $0xb1,xvec15, xvec5;
MUL_SX xvec7, xvec15, xvec15;
MUL_SX xvec6, xvec5, xvec5;
ADDSUB_SX xvec5, xvec15, xvec15;
VPERMILP_SX $0xb1,xvec14, xvec4;
MUL_SX xvec7, xvec14, xvec14;
MUL_SX xvec6, xvec4, xvec4;
ADDSUB_SX xvec4, xvec14, xvec14;
VPERMILP_SX $0xb1,xvec11, xvec1;
MUL_SX xvec7, xvec11, xvec11;
MUL_SX xvec6, xvec1, xvec1;
ADDSUB_SX xvec1, xvec11, xvec11;
VPERMILP_SX $0xb1,xvec10, xvec0;
MUL_SX xvec7, xvec10, xvec10;
MUL_SX xvec6, xvec0, xvec0;
ADDSUB_SX xvec0, xvec10, xvec10;
#ifndef	TRMMKERNEL
LDL_SX 0*SIZE(C0), xvec0, xvec0;
LDH_SX 2*SIZE(C1), xvec0, xvec0;
LDL_SX 4*SIZE(C0), xvec1, xvec1;
LDH_SX 6*SIZE(C1), xvec1, xvec1;
ADD_SX xvec0, xvec15, xvec15;
ADD_SX xvec1, xvec14, xvec14;
#endif
STL_SX	xvec15, 0*SIZE(C0);
STH_SX	xvec15, 2*SIZE(C1);
STL_SX	xvec14, 4*SIZE(C0);
STH_SX	xvec14, 6*SIZE(C1);
#ifndef	TRMMKERNEL
LDL_SX 0*SIZE(C1), xvec4, xvec4;
LDH_SX 2*SIZE(C0), xvec4, xvec4;
LDL_SX 4*SIZE(C1), xvec5, xvec5;
LDH_SX 6*SIZE(C0), xvec5, xvec5;
ADD_SX xvec4, xvec11, xvec11;
ADD_SX xvec5, xvec10, xvec10;
#endif
STL_SX	xvec11, 0*SIZE(C1);
STH_SX	xvec11, 2*SIZE(C0);
STL_SX	xvec10, 4*SIZE(C1);
STH_SX	xvec10, 6*SIZE(C0);
#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA))||(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
MOVQ bk, %rax;
SUBQ kkk, %rax;
SALQ $ZBASE_SHIFT, %rax;
LEAQ (ptrba, %rax, 4), ptrba;
LEAQ (ptrbb, %rax, 2), ptrbb;
#endif
#if defined(TRMMKERNEL) && defined(LEFT)
ADDQ	$4, kk;
#endif

ADDQ	$8*SIZE, C0;
ADDQ	$8*SIZE, C1;

.L22_loopE:
TEST $2, bm;
JLE .L23_loopE;
ALIGN_5
.L23_bodyB:
#if !defined(TRMMKERNEL)||(defined(TRMMKERNEL)&&defined(LEFT)&&defined(TRANSA))||(defined(TRMMKERNEL)&&!defined(LEFT)&&!defined(TRANSA))
MOVQ bb,ptrbb;
#else
MOVQ bb, ptrbb;
MOVQ kk, %rax;
SALQ $ZBASE_SHIFT, %rax;
LEAQ (ptrba, %rax, 2), ptrba;
LEAQ (ptrbb, %rax, 2), ptrbb;
#endif
XOR_SY	yvec15, yvec15, yvec15;
XOR_SY	yvec11, yvec11, yvec11;
#ifndef	TRMMKERNEL
MOVQ bk,k;
#elif (defined(LEFT)&&!defined(TRANSA))||(!defined(LEFT)&&defined(TRANSA))
MOVQ bk, %rax;
SUBQ kk, %rax;
MOVQ %rax, kkk;
#else
MOVQ kk, %rax;
#ifdef	LEFT
ADDQ $2, %rax;
#else
ADDQ $2, %rax;
#endif
MOVQ %rax, kkk;
#endif
SARQ $2, k;
JLE .L231_loopE;
ALIGN_5
.L231_bodyB:
EDUP_SX	0*SIZE(ptrbb), xvec4;
ODUP_SX	0*SIZE(ptrbb), xvec5;
SHUF_SX	$0x4e, xvec4, xvec6;
SHUF_SX	$0x4e, xvec5, xvec7;

LD_SX	0*SIZE(ptrba), xvec0;
MOV_SX	xvec0, xvec1;
MUL_SX xvec4, xvec0, xvec0;
ADD1_SX xvec0, xvec15, xvec15;
SHUF_SX	$0xb1, xvec1, xvec2;
MUL_SX xvec6, xvec1, xvec1;
ADD1_SX xvec1, xvec11, xvec11;

MOV_SX	xvec2, xvec3;
MUL_SX xvec5, xvec2, xvec2;
ADD2_SX xvec2, xvec15, xvec15;
MUL_SX xvec7, xvec3, xvec3;
ADD2_SX xvec3, xvec11, xvec11;

#### Unroll 2 #####
EDUP_SX	4*SIZE(ptrbb), xvec4;
ODUP_SX	4*SIZE(ptrbb), xvec5;
SHUF_SX	$0x4e, xvec4, xvec6;
SHUF_SX	$0x4e, xvec5, xvec7;

LD_SX	4*SIZE(ptrba), xvec0;
MOV_SX	xvec0, xvec1;
MUL_SX xvec4, xvec0, xvec0;
ADD1_SX xvec0, xvec15, xvec15;
SHUF_SX	$0xb1, xvec1, xvec2;
MUL_SX xvec6, xvec1, xvec1;
ADD1_SX xvec1, xvec11, xvec11;

MOV_SX	xvec2, xvec3;
MUL_SX xvec5, xvec2, xvec2;
ADD2_SX xvec2, xvec15, xvec15;
MUL_SX xvec7, xvec3, xvec3;
ADD2_SX xvec3, xvec11, xvec11;

#### Unroll 3 ####
EDUP_SX	8*SIZE(ptrbb), xvec4;
ODUP_SX	8*SIZE(ptrbb), xvec5;
SHUF_SX	$0x4e, xvec4, xvec6;
SHUF_SX	$0x4e, xvec5, xvec7;

LD_SX	8*SIZE(ptrba), xvec0;
MOV_SX	xvec0, xvec1;
MUL_SX xvec4, xvec0, xvec0;
ADD1_SX xvec0, xvec15, xvec15;
SHUF_SX	$0xb1, xvec1, xvec2;
MUL_SX xvec6, xvec1, xvec1;
ADD1_SX xvec1, xvec11, xvec11;

MOV_SX	xvec2, xvec3;
MUL_SX xvec5, xvec2, xvec2;
ADD2_SX xvec2, xvec15, xvec15;
MUL_SX xvec7, xvec3, xvec3;
ADD2_SX xvec3, xvec11, xvec11;

#### Unroll 4 ####
EDUP_SX	12*SIZE(ptrbb), xvec4;
ODUP_SX	12*SIZE(ptrbb), xvec5;
SHUF_SX	$0x4e, xvec4, xvec6;
SHUF_SX	$0x4e, xvec5, xvec7;

LD_SX	12*SIZE(ptrba), xvec0;
MOV_SX	xvec0, xvec1;
MUL_SX xvec4, xvec0, xvec0;
ADD1_SX xvec0, xvec15, xvec15;
SHUF_SX	$0xb1, xvec1, xvec2;
MUL_SX xvec6, xvec1, xvec1;
ADD1_SX xvec1, xvec11, xvec11;

MOV_SX	xvec2, xvec3;
MUL_SX xvec5, xvec2, xvec2;
ADD2_SX xvec2, xvec15, xvec15;
MUL_SX xvec7, xvec3, xvec3;
ADD2_SX xvec3, xvec11, xvec11;
ADDQ	$16*SIZE, ptrba;
ADDQ	$16*SIZE, ptrbb;
DECQ	k;
JG	.L231_bodyB;
ALIGN_5
.L231_loopE:
#ifndef	TRMMKERNEL
TEST $2, bk;
#else
TEST $2, kkk;
#endif
JLE .L232_loopE;
ALIGN_5
.L232_bodyB:
EDUP_SX	0*SIZE(ptrbb), xvec4;
ODUP_SX	0*SIZE(ptrbb), xvec5;
SHUF_SX	$0x4e, xvec4, xvec6;
SHUF_SX	$0x4e, xvec5, xvec7;

LD_SX	0*SIZE(ptrba), xvec0;
MOV_SX	xvec0, xvec1;
MUL_SX xvec4, xvec0, xvec0;
ADD1_SX xvec0, xvec15, xvec15;
SHUF_SX	$0xb1, xvec1, xvec2;
MUL_SX xvec6, xvec1, xvec1;
ADD1_SX xvec1, xvec11, xvec11;

MOV_SX	xvec2, xvec3;
MUL_SX xvec5, xvec2, xvec2;
ADD2_SX xvec2, xvec15, xvec15;
MUL_SX xvec7, xvec3, xvec3;
ADD2_SX xvec3, xvec11, xvec11;

#### Unroll 2 #####
EDUP_SX	4*SIZE(ptrbb), xvec4;
ODUP_SX	4*SIZE(ptrbb), xvec5;
SHUF_SX	$0x4e, xvec4, xvec6;
SHUF_SX	$0x4e, xvec5, xvec7;

LD_SX	4*SIZE(ptrba), xvec0;
MOV_SX	xvec0, xvec1;
MUL_SX xvec4, xvec0, xvec0;
ADD1_SX xvec0, xvec15, xvec15;
SHUF_SX	$0xb1, xvec1, xvec2;
MUL_SX xvec6, xvec1, xvec1;
ADD1_SX xvec1, xvec11, xvec11;

MOV_SX	xvec2, xvec3;
MUL_SX xvec5, xvec2, xvec2;
ADD2_SX xvec2, xvec15, xvec15;
MUL_SX xvec7, xvec3, xvec3;
ADD2_SX xvec3, xvec11, xvec11;
ADDQ	$8*SIZE, ptrba;
ADDQ	$8*SIZE, ptrbb;

.L232_loopE:
#ifndef	TRMMKERNEL
TEST $1, bk;
#else
TEST $1, kkk;
#endif
JLE .L233_loopE;
ALIGN_5
.L233_bodyB:
EDUP_SX	0*SIZE(ptrbb), xvec4;
ODUP_SX	0*SIZE(ptrbb), xvec5;
SHUF_SX	$0x4e, xvec4, xvec6;
SHUF_SX	$0x4e, xvec5, xvec7;

LD_SX	0*SIZE(ptrba), xvec0;
MOV_SX	xvec0, xvec1;
MUL_SX xvec4, xvec0, xvec0;
ADD1_SX xvec0, xvec15, xvec15;
SHUF_SX	$0xb1, xvec1, xvec2;
MUL_SX xvec6, xvec1, xvec1;
ADD1_SX xvec1, xvec11, xvec11;

MOV_SX	xvec2, xvec3;
MUL_SX xvec5, xvec2, xvec2;
ADD2_SX xvec2, xvec15, xvec15;
MUL_SX xvec7, xvec3, xvec3;
ADD2_SX xvec3, xvec11, xvec11;
ADDQ	$4*SIZE, ptrba;
ADDQ	$4*SIZE, ptrbb;

.L233_loopE:
#### Handle ####
#if  defined(RN) || defined(RT) || defined(CN) || defined(CT)
XOR_SY	yvec7, yvec7, yvec7;
ADDSUB_SX xvec15, xvec7, xvec7;
MOV_SX		xvec7, xvec15;
XOR_SY	yvec7, yvec7, yvec7;
ADDSUB_SX xvec11, xvec7, xvec7;
MOV_SX		xvec7, xvec11;
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
XOR_SY	yvec7, yvec7, yvec7;
SUB_SX xvec15, xvec7, xvec7;
MOV_SX	xvec7, xvec15;
XOR_SY	yvec7, yvec7, yvec7;
SUB_SX xvec11, xvec7, xvec7;
MOV_SX	xvec7, xvec11;
#elif defined(RR) || defined(RC) || defined(CR) || defined(CC)
SHUF_SX $0xb1, xvec15, xvec15;
SHUF_SX $0xb1, xvec11, xvec11;
XOR_SY	yvec7, yvec7, yvec7;
ADDSUB_SX xvec15, xvec7, xvec7;
MOV_SX		xvec7, xvec15;
XOR_SY	yvec7, yvec7, yvec7;
ADDSUB_SX xvec11, xvec7, xvec7;
MOV_SX		xvec7, xvec11;
SHUF_SX $0xb1, xvec15, xvec15;
SHUF_SX $0xb1, xvec11, xvec11;
#endif
#### Mulitply Alpha ####
BROAD_SX MEMALPHA_R, xvec7;
BROAD_SX MEMALPHA_I, xvec6;
#### Writng back ####
VPERMILP_SX $0xb1,xvec15, xvec5;
MUL_SX xvec7, xvec15, xvec15;
MUL_SX xvec6, xvec5, xvec5;
ADDSUB_SX xvec5, xvec15, xvec15;
VPERMILP_SX $0xb1,xvec11, xvec1;
MUL_SX xvec7, xvec11, xvec11;
MUL_SX xvec6, xvec1, xvec1;
ADDSUB_SX xvec1, xvec11, xvec11;
#ifndef	TRMMKERNEL
LDL_SX 0*SIZE(C0), xvec0, xvec0;
LDH_SX 2*SIZE(C1), xvec0, xvec0;
ADD_SX xvec0, xvec15, xvec15;
#endif
STL_SX	xvec15, 0*SIZE(C0);
STH_SX	xvec15, 2*SIZE(C1);
#ifndef	TRMMKERNEL
LDL_SX 0*SIZE(C1), xvec4, xvec4;
LDH_SX 2*SIZE(C0), xvec4, xvec4;
ADD_SX xvec4, xvec11, xvec11;
#endif
STL_SX	xvec11, 0*SIZE(C1);
STH_SX	xvec11, 2*SIZE(C0);
#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA))||(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
MOVQ bk, %rax;
SUBQ kkk, %rax;
SALQ $ZBASE_SHIFT, %rax;
LEAQ (ptrba, %rax, 2), ptrba;
LEAQ (ptrbb, %rax, 2), ptrbb;
#endif
#if defined(TRMMKERNEL) && defined(LEFT)
ADDQ	$2, kk;
#endif

ADDQ	$4*SIZE, C0;
ADDQ	$4*SIZE, C1;

.L23_loopE:
TEST $1, bm;
JLE .L24_loopE;
ALIGN_5
.L24_bodyB:
#if !defined(TRMMKERNEL)||(defined(TRMMKERNEL)&&defined(LEFT)&&defined(TRANSA))||(defined(TRMMKERNEL)&&!defined(LEFT)&&!defined(TRANSA))
MOVQ bb,ptrbb;
#else
MOVQ bb, ptrbb;
MOVQ kk, %rax;
SALQ $ZBASE_SHIFT, %rax;
ADDQ %rax, ptrba;
LEAQ (ptrbb, %rax, 2), ptrbb;
#endif
XOR_SY	yvec15, yvec15, yvec15;
#ifndef	TRMMKERNEL
MOVQ bk,k;
#elif (defined(LEFT)&&!defined(TRANSA))||(!defined(LEFT)&&defined(TRANSA))
MOVQ bk, %rax;
SUBQ kk, %rax;
MOVQ %rax, kkk;
#else
MOVQ kk, %rax;
#ifdef	LEFT
ADDQ $1, %rax;
#else
ADDQ $2, %rax;
#endif
MOVQ %rax, kkk;
#endif
SARQ $2, k;
JLE .L241_loopE;
ALIGN_5
.L241_bodyB:
BROAD_SX 0*SIZE(ptrba), xvec0;
LD_SX	0*SIZE(ptrbb), xvec2;
SHUF_SX	$0xb1, xvec2, xvec3;
MUL_SX xvec0, xvec2, xvec2;
ADD1_SX xvec2, xvec15, xvec15;

BROAD_SX 1*SIZE(ptrba), xvec1;
MUL_SX xvec1, xvec3, xvec3;
ADD2_SX xvec3, xvec15, xvec15;

BROAD_SX 2*SIZE(ptrba), xvec0;
LD_SX	4*SIZE(ptrbb), xvec2;
SHUF_SX	$0xb1, xvec2, xvec3;
MUL_SX xvec0, xvec2, xvec2;
ADD1_SX xvec2, xvec15, xvec15;

BROAD_SX 3*SIZE(ptrba), xvec1;
MUL_SX xvec1, xvec3, xvec3;
ADD2_SX xvec3, xvec15, xvec15;

BROAD_SX 4*SIZE(ptrba), xvec0;
LD_SX	8*SIZE(ptrbb), xvec2;
SHUF_SX	$0xb1, xvec2, xvec3;
MUL_SX xvec0, xvec2, xvec2;
ADD1_SX xvec2, xvec15, xvec15;

BROAD_SX 5*SIZE(ptrba), xvec1;
MUL_SX xvec1, xvec3, xvec3;
ADD2_SX xvec3, xvec15, xvec15;

BROAD_SX 6*SIZE(ptrba), xvec0;
LD_SX	12*SIZE(ptrbb), xvec2;
SHUF_SX	$0xb1, xvec2, xvec3;
MUL_SX xvec0, xvec2, xvec2;
ADD1_SX xvec2, xvec15, xvec15;

BROAD_SX 7*SIZE(ptrba), xvec1;
MUL_SX xvec1, xvec3, xvec3;
ADD2_SX xvec3, xvec15, xvec15;
ADDQ	$8*SIZE, ptrba;
ADDQ	$16*SIZE, ptrbb;
DECQ	k;
JG	.L241_bodyB;
ALIGN_5
.L241_loopE:
#ifndef	TRMMKERNEL
TEST $2, bk;
#else
TEST $2, kkk;
#endif
JLE .L242_loopE;
ALIGN_5
.L242_bodyB:
BROAD_SX 0*SIZE(ptrba), xvec0;
LD_SX	0*SIZE(ptrbb), xvec2;
SHUF_SX	$0xb1, xvec2, xvec3;
MUL_SX xvec0, xvec2, xvec2;
ADD1_SX xvec2, xvec15, xvec15;

BROAD_SX 1*SIZE(ptrba), xvec1;
MUL_SX xvec1, xvec3, xvec3;
ADD2_SX xvec3, xvec15, xvec15;

BROAD_SX 2*SIZE(ptrba), xvec0;
LD_SX	4*SIZE(ptrbb), xvec2;
SHUF_SX	$0xb1, xvec2, xvec3;
MUL_SX xvec0, xvec2, xvec2;
ADD1_SX xvec2, xvec15, xvec15;

BROAD_SX 3*SIZE(ptrba), xvec1;
MUL_SX xvec1, xvec3, xvec3;
ADD2_SX xvec3, xvec15, xvec15;
ADDQ	$4*SIZE, ptrba;
ADDQ	$8*SIZE, ptrbb;

.L242_loopE:
#ifndef	TRMMKERNEL
TEST $1, bk;
#else
TEST $1, kkk;
#endif
JLE .L243_loopE;
ALIGN_5
.L243_bodyB:
BROAD_SX 0*SIZE(ptrba), xvec0;
LD_SX	0*SIZE(ptrbb), xvec2;
SHUF_SX	$0xb1, xvec2, xvec3;
MUL_SX xvec0, xvec2, xvec2;
ADD1_SX xvec2, xvec15, xvec15;

BROAD_SX 1*SIZE(ptrba), xvec1;
MUL_SX xvec1, xvec3, xvec3;
ADD2_SX xvec3, xvec15, xvec15;
ADDQ	$2*SIZE, ptrba;
ADDQ	$4*SIZE, ptrbb;

.L243_loopE:
#### Handle ####
XOR_SY	yvec7, yvec7, yvec7;
#if defined(NR) || defined(NC) || defined(TR) || defined(TC)
ADDSUB_SX xvec15, xvec7, xvec7;
MOV_SX		xvec7, xvec15;
#elif  defined(RN) || defined(RT) || defined(CN) || defined(CT)
SUB_SX xvec15, xvec7, xvec7;
MOV_SX	xvec7, xvec15;
#elif defined(RR) || defined(RC) || defined(CR) || defined(CC)
SHUF_SX $0xb1, xvec15, xvec15;
ADDSUB_SX xvec15, xvec7, xvec7;
MOV_SX		xvec7, xvec15;
SHUF_SX $0xb1, xvec15, xvec15;
#endif
##### Load Alpha ####
BROAD_SX MEMALPHA_R,xvec7;
BROAD_SX MEMALPHA_I,xvec6;
##### Multiply Alpha ####
VPERMILP_SX $0xb1,xvec15, xvec5;
MUL_SX xvec7, xvec15, xvec15;
MUL_SX xvec6, xvec5, xvec5;
ADDSUB_SX xvec5, xvec15, xvec15;
#### Writing back ####
#ifndef	TRMMKERNEL
LDL_SX 0*SIZE(C0), xvec0, xvec0;
LDH_SX 0*SIZE(C1), xvec0, xvec0;
ADD_SX xvec0, xvec15, xvec15;
#endif
STL_SX	xvec15, 0*SIZE(C0);
STH_SX	xvec15, 0*SIZE(C1);
#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA))||(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
MOVQ bk, %rax;
SUBQ kkk, %rax;
SALQ $ZBASE_SHIFT, %rax;
ADDQ %rax, ptrba;
LEAQ (ptrbb, %rax, 2), ptrbb;
#endif
#if defined(TRMMKERNEL) && defined(LEFT)
ADDQ	$1, kk;
#endif
ADDQ	$2*SIZE, C0;
ADDQ	$2*SIZE, C1;
.L24_loopE:
#if defined(TRMMKERNEL) && !defined(LEFT)
ADDQ	$2, kk;
#endif
MOVQ bk, k;
SALQ $4, k;
ADDQ k, bb;
LEAQ (C, ldc, 2), C;
.L20_loopE:
TEST $1, bn;
JLE .L30_loopE;
ALIGN_5
.L30_bodyB:
#if defined(TRMMKERNEL) && defined(LEFT)
MOVQ OFFSET, %rax;
MOVQ %rax, kk;
#endif
MOVQ C, C0;
MOVQ ba, ptrba;
MOVQ bm, i;
SARQ $3, i;
JLE .L31_loopE;
ALIGN_5
.L31_bodyB:
MOVQ bb, ptrbb;
#if !defined(TRMMKERNEL)||(defined(TRMMKERNEL)&&defined(LEFT)&&defined(TRANSA))||(defined(TRMMKERNEL)&&!defined(LEFT)&&!defined(TRANSA))
MOVQ bb,ptrbb;
#else
MOVQ bb, ptrbb;
MOVQ kk, %rax;
SALQ $ZBASE_SHIFT, %rax;
LEAQ (ptrba, %rax, 8), ptrba;
ADDQ %rax, ptrbb;
#endif
XOR_SY	yvec15, yvec15, yvec15;
XOR_SY	yvec14, yvec14, yvec14;
MOVQ bk, k;
#ifndef	TRMMKERNEL
MOVQ bk,k;
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
#### Unroll 1 ####
LD_SY	0*SIZE(ptrba), yvec0;
LD_SY	8*SIZE(ptrba), yvec1;
BROAD_SY 0*SIZE(ptrbb), yvec2;
MUL_SY	yvec0, yvec2, yvec6;
ADD1_SY	yvec6, yvec15, yvec15;
MUL_SY	yvec1, yvec2, yvec7;
ADD1_SY	yvec7, yvec14, yvec14;

BROAD_SY 1*SIZE(ptrbb), yvec3;
VPERMILP_SY $0xb1, yvec0, yvec4;
VPERMILP_SY	$0xb1, yvec1, yvec5;
MUL_SY	yvec4, yvec3, yvec6;
ADD2_SY yvec6, yvec15, yvec15;
MUL_SY	yvec5, yvec3, yvec7;
ADD2_SY yvec7, yvec14, yvec14;

#### Unroll 2 ####
LD_SY	16*SIZE(ptrba), yvec0;
LD_SY	24*SIZE(ptrba), yvec1;
BROAD_SY 2*SIZE(ptrbb), yvec2;
MUL_SY	yvec0, yvec2, yvec6;
ADD1_SY	yvec6, yvec15, yvec15;
MUL_SY	yvec1, yvec2, yvec7;
ADD1_SY	yvec7, yvec14, yvec14;

BROAD_SY 3*SIZE(ptrbb), yvec3;
VPERMILP_SY $0xb1, yvec0, yvec4;
VPERMILP_SY	$0xb1, yvec1, yvec5;
MUL_SY	yvec4, yvec3, yvec6;
ADD2_SY yvec6, yvec15, yvec15;
MUL_SY	yvec5, yvec3, yvec7;
ADD2_SY yvec7, yvec14, yvec14;

#### Unroll 3 ####
LD_SY	32*SIZE(ptrba), yvec0;
LD_SY	40*SIZE(ptrba), yvec1;
BROAD_SY 4*SIZE(ptrbb), yvec2;
MUL_SY	yvec0, yvec2, yvec6;
ADD1_SY	yvec6, yvec15, yvec15;
MUL_SY	yvec1, yvec2, yvec7;
ADD1_SY	yvec7, yvec14, yvec14;

BROAD_SY 5*SIZE(ptrbb), yvec3;
VPERMILP_SY $0xb1, yvec0, yvec4;
VPERMILP_SY	$0xb1, yvec1, yvec5;
MUL_SY	yvec4, yvec3, yvec6;
ADD2_SY yvec6, yvec15, yvec15;
MUL_SY	yvec5, yvec3, yvec7;
ADD2_SY yvec7, yvec14, yvec14;

#### Unroll 4 ####
LD_SY	48*SIZE(ptrba), yvec0;
LD_SY	56*SIZE(ptrba), yvec1;
BROAD_SY 6*SIZE(ptrbb), yvec2;
MUL_SY	yvec0, yvec2, yvec6;
ADD1_SY	yvec6, yvec15, yvec15;
MUL_SY	yvec1, yvec2, yvec7;
ADD1_SY	yvec7, yvec14, yvec14;

BROAD_SY 7*SIZE(ptrbb), yvec3;
VPERMILP_SY $0xb1, yvec0, yvec4;
VPERMILP_SY	$0xb1, yvec1, yvec5;
MUL_SY	yvec4, yvec3, yvec6;
ADD2_SY yvec6, yvec15, yvec15;
MUL_SY	yvec5, yvec3, yvec7;
ADD2_SY yvec7, yvec14, yvec14;
ADDQ	$64*SIZE, ptrba;
ADDQ	$8*SIZE, ptrbb;
DECQ	k;
JG	.L311_bodyB;
ALIGN_5
.L311_loopE:
#ifndef	TRMMKERNEL
TEST $2, bk;
#else
TEST $2, kkk;
#endif
JLE .L312_loopE;
ALIGN_5
.L312_bodyB:
#### Unroll 1 ####
LD_SY	0*SIZE(ptrba), yvec0;
LD_SY	8*SIZE(ptrba), yvec1;
BROAD_SY 0*SIZE(ptrbb), yvec2;
MUL_SY	yvec0, yvec2, yvec6;
ADD1_SY	yvec6, yvec15, yvec15;
MUL_SY	yvec1, yvec2, yvec7;
ADD1_SY	yvec7, yvec14, yvec14;

BROAD_SY 1*SIZE(ptrbb), yvec3;
VPERMILP_SY $0xb1, yvec0, yvec4;
VPERMILP_SY	$0xb1, yvec1, yvec5;
MUL_SY	yvec4, yvec3, yvec6;
ADD2_SY yvec6, yvec15, yvec15;
MUL_SY	yvec5, yvec3, yvec7;
ADD2_SY yvec7, yvec14, yvec14;

#### Unroll 2 ####
LD_SY	16*SIZE(ptrba), yvec0;
LD_SY	24*SIZE(ptrba), yvec1;
BROAD_SY 2*SIZE(ptrbb), yvec2;
MUL_SY	yvec0, yvec2, yvec6;
ADD1_SY	yvec6, yvec15, yvec15;
MUL_SY	yvec1, yvec2, yvec7;
ADD1_SY	yvec7, yvec14, yvec14;

BROAD_SY 3*SIZE(ptrbb), yvec3;
VPERMILP_SY $0xb1, yvec0, yvec4;
VPERMILP_SY	$0xb1, yvec1, yvec5;
MUL_SY	yvec4, yvec3, yvec6;
ADD2_SY yvec6, yvec15, yvec15;
MUL_SY	yvec5, yvec3, yvec7;
ADD2_SY yvec7, yvec14, yvec14;
ADDQ	$32*SIZE, ptrba;
ADDQ	$4*SIZE, ptrbb;

.L312_loopE:
#ifndef	TRMMKERNEL
TEST $1, bk;
#else
TEST $1, kkk;
#endif
JLE .L313_loopE;
ALIGN_5
.L313_bodyB:
#### Unroll 1 ####
LD_SY	0*SIZE(ptrba), yvec0;
LD_SY	8*SIZE(ptrba), yvec1;
BROAD_SY 0*SIZE(ptrbb), yvec2;
MUL_SY	yvec0, yvec2, yvec6;
ADD1_SY	yvec6, yvec15, yvec15;
MUL_SY	yvec1, yvec2, yvec7;
ADD1_SY	yvec7, yvec14, yvec14;

BROAD_SY 1*SIZE(ptrbb), yvec3;
VPERMILP_SY $0xb1, yvec0, yvec4;
VPERMILP_SY	$0xb1, yvec1, yvec5;
MUL_SY	yvec4, yvec3, yvec6;
ADD2_SY yvec6, yvec15, yvec15;
MUL_SY	yvec5, yvec3, yvec7;
ADD2_SY yvec7, yvec14, yvec14;
ADDQ	$16*SIZE, ptrba;
ADDQ	$2*SIZE, ptrbb;

.L313_loopE:
#### Handle ####
XOR_SY	yvec7, yvec7, yvec7;
#if  defined(RN) || defined(RT) || defined(CN) || defined(CT)
ADDSUB_SY	yvec15, yvec7, yvec15;
ADDSUB_SY	yvec14, yvec7, yvec14;
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
SUB_SY	yvec15, yvec7, yvec15;
SUB_SY	yvec14, yvec7, yvec14;
#elif defined(RR) || defined(RC) || defined(CR) || defined(CC)
VPERMILP_SY $0xb1, yvec15, yvec15;
VPERMILP_SY $0xb1, yvec14, yvec14;
ADDSUB_SY	yvec15, yvec7, yvec15;
ADDSUB_SY	yvec14, yvec7, yvec14;
VPERMILP_SY $0xb1, yvec15, yvec15;
VPERMILP_SY $0xb1, yvec14, yvec14;
#endif
##### Load Alpha ####
BROAD_SY MEMALPHA_R,yvec7;
BROAD_SY MEMALPHA_I,yvec6;
##### Multiply Alpha ####
VPERMILP_SY $0xb1,yvec15, yvec5;
MUL_SY	yvec15, yvec7, yvec15;
MUL_SY	yvec5, yvec6, yvec5;
ADDSUB_SY	yvec5, yvec15, yvec15;
VPERMILP_SY $0xb1,yvec14, yvec4;
MUL_SY	yvec14, yvec7, yvec14;
MUL_SY	yvec4, yvec6, yvec4;
ADDSUB_SY	yvec4, yvec14, yvec14;
#### Writing back ####
EXTRA_SY $1, yvec15, xvec7;
EXTRA_SY $1, yvec14, xvec6;
#ifndef	TRMMKERNEL
LDL_SX 0*SIZE(C0), xvec0, xvec0;
LDH_SX 2*SIZE(C0), xvec0, xvec0;
LDL_SX 4*SIZE(C0), xvec1, xvec1;
LDH_SX 6*SIZE(C0), xvec1, xvec1;
LDL_SX 8*SIZE(C0), xvec2, xvec2;
LDH_SX 10*SIZE(C0), xvec2, xvec2;
LDL_SX 12*SIZE(C0), xvec3, xvec3;
LDH_SX 14*SIZE(C0), xvec3, xvec3;
ADD_SX xvec0, xvec15, xvec15;
ADD_SX xvec1, xvec7, xvec7;
ADD_SX xvec2, xvec14, xvec14;
ADD_SX xvec3, xvec6, xvec6;
#endif
STL_SX	xvec15, 0*SIZE(C0);
STH_SX	xvec15, 2*SIZE(C0);
STL_SX	xvec7, 4*SIZE(C0);
STH_SX	xvec7, 6*SIZE(C0);
STL_SX	xvec14, 8*SIZE(C0);
STH_SX	xvec14,	10*SIZE(C0);
STL_SX	xvec6, 12*SIZE(C0);
STH_SX	xvec6, 14*SIZE(C0);
#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA))||(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
MOVQ bk, %rax;
SUBQ kkk, %rax;
SALQ $ZBASE_SHIFT, %rax;
LEAQ (ptrba, %rax, 8), ptrba;
ADDQ %rax, ptrbb;
#endif
#if defined(TRMMKERNEL) && defined(LEFT)
ADDQ	$8, kk;
#endif
ADDQ	$16*SIZE, C0;
DECQ	i;
JG .L31_bodyB;
ALIGN_5
.L31_loopE:
TEST $4, bm;
JLE .L32_loopE;
ALIGN_5
.L32_bodyB:
#if !defined(TRMMKERNEL)||(defined(TRMMKERNEL)&&defined(LEFT)&&defined(TRANSA))||(defined(TRMMKERNEL)&&!defined(LEFT)&&!defined(TRANSA))
MOVQ bb,ptrbb;
#else
MOVQ bb, ptrbb;
MOVQ kk, %rax;
SALQ $ZBASE_SHIFT, %rax;
LEAQ (ptrba, %rax, 4), ptrba;
ADDQ %rax, ptrbb;
#endif
XOR_SY 	yvec15, yvec15, yvec15;
#ifndef	TRMMKERNEL
MOVQ bk,k;
#elif (defined(LEFT)&&!defined(TRANSA))||(!defined(LEFT)&&defined(TRANSA))
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
MOVQ %rax, kkk;
#endif
SARQ $2, k;
JLE .L321_loopE;
ALIGN_5
.L321_bodyB:
#### Unroll 1 ####
LD_SY	0*SIZE(ptrba), yvec0;
BROAD_SY 0*SIZE(ptrbb), yvec2;
MUL_SY	yvec0, yvec2, yvec6;
ADD1_SY	yvec6, yvec15, yvec15;

BROAD_SY 1*SIZE(ptrbb), yvec3;
VPERMILP_SY $0xb1, yvec0, yvec4;
MUL_SY	yvec4, yvec3, yvec6;
ADD2_SY yvec6, yvec15, yvec15;

#### Unroll 2 ####
LD_SY	8*SIZE(ptrba), yvec0;
BROAD_SY 2*SIZE(ptrbb), yvec2;
MUL_SY	yvec0, yvec2, yvec6;
ADD1_SY	yvec6, yvec15, yvec15;

BROAD_SY 3*SIZE(ptrbb), yvec3;
VPERMILP_SY $0xb1, yvec0, yvec4;
MUL_SY	yvec4, yvec3, yvec6;
ADD2_SY yvec6, yvec15, yvec15;

#### Unroll 3 ####
LD_SY	16*SIZE(ptrba), yvec0;
BROAD_SY 4*SIZE(ptrbb), yvec2;
MUL_SY	yvec0, yvec2, yvec6;
ADD1_SY	yvec6, yvec15, yvec15;

BROAD_SY 5*SIZE(ptrbb), yvec3;
VPERMILP_SY $0xb1, yvec0, yvec4;
MUL_SY	yvec4, yvec3, yvec6;
ADD2_SY yvec6, yvec15, yvec15;

#### Unroll 4 ####
LD_SY	24*SIZE(ptrba), yvec0;
BROAD_SY 6*SIZE(ptrbb), yvec2;
MUL_SY	yvec0, yvec2, yvec6;
ADD1_SY	yvec6, yvec15, yvec15;

BROAD_SY 7*SIZE(ptrbb), yvec3;
VPERMILP_SY $0xb1, yvec0, yvec4;
MUL_SY	yvec4, yvec3, yvec6;
ADD2_SY yvec6, yvec15, yvec15;
ADDQ	$32*SIZE, ptrba;
ADDQ	$8*SIZE, ptrbb;
DECQ	k;
JG .L321_bodyB;
ALIGN_5
.L321_loopE:
#ifndef	TRMMKERNEL
TEST $2, bk;
#else
TEST $2, kkk;
#endif
JLE .L322_loopE;
ALIGN_5
.L322_bodyB:
#### Unroll 1 ####
LD_SY	0*SIZE(ptrba), yvec0;
BROAD_SY 0*SIZE(ptrbb), yvec2;
MUL_SY	yvec0, yvec2, yvec6;
ADD1_SY	yvec6, yvec15, yvec15;

BROAD_SY 1*SIZE(ptrbb), yvec3;
VPERMILP_SY $0xb1, yvec0, yvec4;
MUL_SY	yvec4, yvec3, yvec6;
ADD2_SY yvec6, yvec15, yvec15;

#### Unroll 2 ####
LD_SY	8*SIZE(ptrba), yvec0;
BROAD_SY 2*SIZE(ptrbb), yvec2;
MUL_SY	yvec0, yvec2, yvec6;
ADD1_SY	yvec6, yvec15, yvec15;

BROAD_SY 3*SIZE(ptrbb), yvec3;
VPERMILP_SY $0xb1, yvec0, yvec4;
MUL_SY	yvec4, yvec3, yvec6;
ADD2_SY yvec6, yvec15, yvec15;
ADDQ	$16*SIZE, ptrba;
ADDQ	$4*SIZE, ptrbb;

.L322_loopE:
#ifndef	TRMMKERNEL
TEST $1, bk;
#else
TEST $1, kkk;
#endif
JLE	.L323_loopE;
ALIGN_5
.L323_bodyB:
#### Unroll 1 ####
LD_SY	0*SIZE(ptrba), yvec0;
BROAD_SY 0*SIZE(ptrbb), yvec2;
MUL_SY	yvec0, yvec2, yvec6;
ADD1_SY	yvec6, yvec15, yvec15;

BROAD_SY 1*SIZE(ptrbb), yvec3;
VPERMILP_SY $0xb1, yvec0, yvec4;
MUL_SY	yvec4, yvec3, yvec6;
ADD2_SY yvec6, yvec15, yvec15;
ADDQ	$8*SIZE, ptrba;
ADDQ	$2*SIZE, ptrbb;

.L323_loopE:
#### Handle ####
XOR_SY	yvec7, yvec7, yvec7;
#if  defined(RN) || defined(RT) || defined(CN) || defined(CT)
ADDSUB_SY	yvec15, yvec7, yvec15;
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
SUB_SY	yvec15, yvec7, yvec15;
#elif defined(RR) || defined(RC) || defined(CR) || defined(CC)
VPERMILP_SY $0xb1, yvec15, yvec15;
ADDSUB_SY	yvec15, yvec7, yvec15;
VPERMILP_SY $0xb1, yvec15, yvec15;
#endif
##### Load Alpha ####
BROAD_SY MEMALPHA_R,yvec7;
BROAD_SY MEMALPHA_I,yvec6;
##### Multiply Alpha ####
VPERMILP_SY $0xb1,yvec15, yvec5;
MUL_SY	yvec15, yvec7, yvec15;
MUL_SY	yvec5, yvec6, yvec5;
ADDSUB_SY	yvec5, yvec15, yvec15;
#### Writing back ####
EXTRA_SY $1, yvec15, xvec7;
#ifndef	TRMMKERNEL
LDL_SX 0*SIZE(C0), xvec0, xvec0;
LDH_SX 2*SIZE(C0), xvec0, xvec0;
LDL_SX 4*SIZE(C0), xvec1, xvec1;
LDH_SX 6*SIZE(C0), xvec1, xvec1;
ADD_SX xvec0, xvec15, xvec15;
ADD_SX xvec1, xvec7, xvec7;
#endif
STL_SX	xvec15, 0*SIZE(C0);
STH_SX	xvec15, 2*SIZE(C0);
STL_SX	xvec7, 4*SIZE(C0);
STH_SX	xvec7, 6*SIZE(C0);
#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA))||(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
MOVQ bk, %rax;
SUBQ kkk, %rax;
SALQ $ZBASE_SHIFT, %rax;
LEAQ (ptrba, %rax, 4), ptrba;
ADDQ %rax, ptrbb;
#endif
#if defined(TRMMKERNEL) && defined(LEFT)
ADDQ	$4, kk;
#endif
ADDQ	$8*SIZE, C0;

.L32_loopE:
TEST $2, bm;
JLE .L33_loopE;
ALIGN_5
.L33_bodyB:
#if !defined(TRMMKERNEL)||(defined(TRMMKERNEL)&&defined(LEFT)&&defined(TRANSA))||(defined(TRMMKERNEL)&&!defined(LEFT)&&!defined(TRANSA))
MOVQ bb,ptrbb;
#else
MOVQ bb, ptrbb;
MOVQ kk, %rax;
SALQ $ZBASE_SHIFT, %rax;
LEAQ (ptrba, %rax, 2), ptrba;
ADDQ %rax, ptrbb;
#endif
XOR_SY	yvec15, yvec15, yvec15;
#ifndef	TRMMKERNEL
MOVQ bk,k;
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
#### Unroll 1 ####
LD_SX	0*SIZE(ptrba), xvec0;
BROAD_SX 0*SIZE(ptrbb), xvec2;
MUL_SX xvec0, xvec2, xvec2;
ADD1_SX xvec2, xvec15, xvec15;
SHUF_SX	$0xb1, xvec0, xvec1;
BROAD_SX 1*SIZE(ptrbb), xvec3;
MUL_SX xvec1, xvec3, xvec3;
ADD2_SX xvec3, xvec15, xvec15;

#### Unroll 2 ####
LD_SX	4*SIZE(ptrba), xvec0;
BROAD_SX 2*SIZE(ptrbb), xvec2;
MUL_SX xvec0, xvec2, xvec2;
ADD1_SX xvec2, xvec15, xvec15;
SHUF_SX	$0xb1, xvec0, xvec1;
BROAD_SX 3*SIZE(ptrbb), xvec3;
MUL_SX xvec1, xvec3, xvec3;
ADD2_SX xvec3, xvec15, xvec15;

#### Unroll 3 ####
LD_SX	8*SIZE(ptrba), xvec0;
BROAD_SX 4*SIZE(ptrbb), xvec2;
MUL_SX xvec0, xvec2, xvec2;
ADD1_SX xvec2, xvec15, xvec15;
SHUF_SX	$0xb1, xvec0, xvec1;
BROAD_SX 5*SIZE(ptrbb), xvec3;
MUL_SX xvec1, xvec3, xvec3;
ADD2_SX xvec3, xvec15, xvec15;

#### Unroll 4 ####
LD_SX	12*SIZE(ptrba), xvec0;
BROAD_SX 6*SIZE(ptrbb), xvec2;
MUL_SX xvec0, xvec2, xvec2;
ADD1_SX xvec2, xvec15, xvec15;
SHUF_SX	$0xb1, xvec0, xvec1;
BROAD_SX 7*SIZE(ptrbb), xvec3;
MUL_SX xvec1, xvec3, xvec3;
ADD2_SX xvec3, xvec15, xvec15;
ADDQ	$16*SIZE, ptrba;
ADDQ	$8*SIZE, ptrbb;
DECQ	k;
JG 	.L331_bodyB;
ALIGN_5
.L331_loopE:
#ifndef	TRMMKERNEL
TEST $2, bk;
#else
TEST $2, kkk;
#endif
JLE .L332_loopE;
ALIGN_5
.L332_bodyB:
#### Unroll 1 ####
LD_SX	0*SIZE(ptrba), xvec0;
BROAD_SX 0*SIZE(ptrbb), xvec2;
MUL_SX xvec0, xvec2, xvec2;
ADD1_SX xvec2, xvec15, xvec15;
SHUF_SX	$0xb1, xvec0, xvec1;
BROAD_SX 1*SIZE(ptrbb), xvec3;
MUL_SX xvec1, xvec3, xvec3;
ADD2_SX xvec3, xvec15, xvec15;

#### Unroll 2 ####
LD_SX	4*SIZE(ptrba), xvec0;
BROAD_SX 2*SIZE(ptrbb), xvec2;
MUL_SX xvec0, xvec2, xvec2;
ADD1_SX xvec2, xvec15, xvec15;
SHUF_SX	$0xb1, xvec0, xvec1;
BROAD_SX 3*SIZE(ptrbb), xvec3;
MUL_SX xvec1, xvec3, xvec3;
ADD2_SX xvec3, xvec15, xvec15;
ADDQ	$8*SIZE, ptrba;
ADDQ	$4*SIZE, ptrbb;

.L332_loopE:
#ifndef	TRMMKERNEL
TEST $1, bk;
#else
TEST $1, kkk;
#endif
JLE .L333_loopE;
ALIGN_5
.L333_bodyB:
#### Unroll 1 ####
LD_SX	0*SIZE(ptrba), xvec0;
BROAD_SX 0*SIZE(ptrbb), xvec2;
MUL_SX xvec0, xvec2, xvec2;
ADD1_SX xvec2, xvec15, xvec15;
SHUF_SX	$0xb1, xvec0, xvec1;
BROAD_SX 1*SIZE(ptrbb), xvec3;
MUL_SX xvec1, xvec3, xvec3;
ADD2_SX xvec3, xvec15, xvec15;
ADDQ	$4*SIZE, ptrba;
ADDQ	$2*SIZE, ptrbb;

.L333_loopE:
#### Handle ####
XOR_SY	yvec7, yvec7, yvec7;
#if  defined(RN) || defined(RT) || defined(CN) || defined(CT)
ADDSUB_SX xvec15, xvec7, xvec7;
MOV_SX		xvec7, xvec15;
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
SUB_SX xvec15, xvec7, xvec7;
MOV_SX	xvec7, xvec15;
#elif defined(RR) || defined(RC) || defined(CR) || defined(CC)
SHUF_SX $0xb1, xvec15, xvec15;
ADDSUB_SX xvec15, xvec7, xvec7;
MOV_SX		xvec7, xvec15;
SHUF_SX $0xb1, xvec15, xvec15;
#endif
#### Mulitply Alpha ####
BROAD_SX MEMALPHA_R, xvec7;
BROAD_SX MEMALPHA_I, xvec6;
#### Writng back ####
VPERMILP_SX $0xb1,xvec15, xvec5;
MUL_SX xvec7, xvec15, xvec15;
MUL_SX xvec6, xvec5, xvec5;
ADDSUB_SX xvec5, xvec15, xvec15;
#ifndef	TRMMKERNEL
LDL_SX 0*SIZE(C0), xvec0, xvec0;
LDH_SX 2*SIZE(C0), xvec0, xvec0;
ADD_SX xvec0, xvec15, xvec15;
#endif
STL_SX	xvec15, 0*SIZE(C0);
STH_SX	xvec15, 2*SIZE(C0);
#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA))||(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
MOVQ bk, %rax;
SUBQ kkk, %rax;
SALQ $ZBASE_SHIFT, %rax;
LEAQ (ptrba, %rax, 2), ptrba;
ADDQ %rax, ptrbb;
#endif
#if defined(TRMMKERNEL) && defined(LEFT)
ADDQ	$2, kk;
#endif
ADDQ	$4*SIZE, C0;

.L33_loopE:
TEST $1, bm;
JLE .L34_loopE;
ALIGN_5
.L34_bodyB:
#if !defined(TRMMKERNEL)||(defined(TRMMKERNEL)&&defined(LEFT)&&defined(TRANSA))||(defined(TRMMKERNEL)&&!defined(LEFT)&&!defined(TRANSA))
MOVQ bb,ptrbb;
#else
MOVQ bb, ptrbb;
MOVQ kk, %rax;
SALQ $ZBASE_SHIFT, %rax;
ADDQ %rax, ptrba;
ADDQ %rax, ptrbb;
#endif
XOR_SY	yvec15, yvec15, yvec15;
#ifndef	TRMMKERNEL
MOVQ bk,k;
#elif (defined(LEFT)&&!defined(TRANSA))||(!defined(LEFT)&&defined(TRANSA))
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
LD_SX	0*SIZE(ptrba), xvec0;
LD_SX	0*SIZE(ptrbb), xvec2;
SHUF_SX	$0xa0, xvec2, xvec3;
MUL_SX xvec0, xvec3, xvec3;
ADD1_SX xvec3, xvec15, xvec15;
SHUF_SX	$0xb1, xvec0, xvec1;
SHUF_SX	$0xf5, xvec2, xvec4;
MUL_SX xvec1, xvec4, xvec4;
ADD2_SX xvec4, xvec15, xvec15;

LD_SX	4*SIZE(ptrba), xvec0;
LD_SX	4*SIZE(ptrbb), xvec2;
SHUF_SX	$0xa0, xvec2, xvec3;
MUL_SX xvec0, xvec3, xvec3;
ADD1_SX xvec3, xvec15, xvec15;
SHUF_SX	$0xb1, xvec0, xvec1;
SHUF_SX	$0xf5, xvec2, xvec4;
MUL_SX xvec1, xvec4, xvec4;
ADD2_SX xvec4, xvec15, xvec15;
ADDQ	$8*SIZE, ptrba;
ADDQ	$8*SIZE, ptrbb;
DECQ	k;
JG .L341_bodyB;
ALIGN_5
.L341_loopE:
#ifndef	TRMMKERNEL
TEST $2, bk;
#else
TEST $2, kkk;
#endif
JLE .L342_loopE;
ALIGN_5
.L342_bodyB:
LD_SX	0*SIZE(ptrba), xvec0;
LD_SX	0*SIZE(ptrbb), xvec2;
SHUF_SX	$0xa0, xvec2, xvec3;
MUL_SX xvec0, xvec3, xvec3;
ADD1_SX xvec3, xvec15, xvec15;
SHUF_SX	$0xb1, xvec0, xvec1;
SHUF_SX	$0xf5, xvec2, xvec4;
MUL_SX xvec1, xvec4, xvec4;
ADD2_SX xvec4, xvec15, xvec15;
ADDQ	$4*SIZE, ptrba;
ADDQ	$4*SIZE, ptrbb;

.L342_loopE:
#ifndef	TRMMKERNEL
TEST $1, bk;
#else
TEST $1, kkk;
#endif
JLE .L343_loopE;
ALIGN_5
.L343_bodyB:
XOR_SY	yvec0, yvec0, yvec0;
XOR_SY	yvec2, yvec2, yvec2;
LDL_SX 0*SIZE(ptrba), xvec0, xvec0;
LDL_SX 0*SIZE(ptrbb), xvec2, xvec2;
SHUF_SX	$0xe0, xvec2, xvec3;
MUL_SX xvec0, xvec3, xvec3;
ADD1_SX xvec3, xvec15, xvec15;
SHUF_SX	$0xe1, xvec0, xvec1;
SHUF_SX	$0xe5, xvec2, xvec4;
MUL_SX xvec1, xvec4, xvec4;
ADD2_SX xvec4, xvec15, xvec15;
ADDQ	$2*SIZE, ptrba;
ADDQ	$2*SIZE, ptrbb;

.L343_loopE:
#### Handle ####
XOR_SY	yvec7, yvec7, yvec7;
#if  defined(RN) || defined(RT) || defined(CN) || defined(CT)
ADDSUB_SX xvec15, xvec7, xvec7;
MOV_SX		xvec7, xvec15;
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
SUB_SX xvec15, xvec7, xvec7;
MOV_SX	xvec7, xvec15;
#elif defined(RR) || defined(RC) || defined(CR) || defined(CC)
SHUF_SX $0xb1, xvec15, xvec15;
ADDSUB_SX xvec15, xvec7, xvec7;
MOV_SX		xvec7, xvec15;
SHUF_SX $0xb1, xvec15, xvec15;
#endif
BROAD_SX MEMALPHA_R, xvec7;
BROAD_SX MEMALPHA_I, xvec6;
VPERMILP_SX	$0xb1, xvec15, xvec5;
MUL_SX xvec7, xvec15, xvec15;
MUL_SX xvec6, xvec5, xvec5;
ADDSUB_SX xvec5, xvec15, xvec15;
SHUF_SX	$0x44, xvec15, xvec14;
SHUF_SX	$0xee, xvec15, xvec13;
ADD_SX xvec13, xvec14, xvec14;
#ifndef	TRMMKERNEL
LDL_SX 0*SIZE(C0), xvec0, xvec0;
ADD_SX xvec0, xvec14, xvec14;
#endif
STL_SX	xvec14, 0*SIZE(C0);
#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA))||(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
MOVQ bk, %rax;
SUBQ kkk, %rax;
SALQ $ZBASE_SHIFT, %rax;
ADDQ %rax, ptrba;
ADDQ %rax, ptrbb;
#endif
#if defined(TRMMKERNEL) && defined(LEFT)
ADDQ	$1, kk;
#endif
ADDQ	$2*SIZE, C0;

.L34_loopE:
#if defined(TRMMKERNEL) && !defined(LEFT)
ADDQ	$1, kk;
#endif

MOVQ bk, k;
SALQ $3, k;
ADDQ k, bb;
ADDQ ldc, C;
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
