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
#define old_offset 16+STACKSIZE(%rsp)

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

#define XOR_DY	vxorpd
#define XOR_DX	vxorpd

#define LD_DY	vmovapd
#define LD_DX	vmovapd
#define LDL_DY	vmovlpd
#define LDL_DX	vmovlpd
#define LDH_DY	vmovhpd
#define LDH_DX	vmovhpd

#define ST_DY	vmovapd
#define ST_DX	vmovapd
#define STL_DY	vmovlpd
#define STL_DX	vmovlpd
#define STH_DY	vmovhpd
#define STH_DX	vmovhpd

#define EDUP_DY	vmovddup

#define ADD_DY	vaddpd
#define ADD_DX	vaddpd
#define SUB_DY	vsubpd
#define SUB_DX	vsubpd

#define ADDSUB_DY	vaddsubpd
#define ADDSUB_DX	vaddsubpd

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


#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
#define ADD1_DX ADD_DX
#define ADD1_DY	ADD_DY
#define ADD2_DY	ADDSUB_DY
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
#define ADD1_DX	SUB_DX
#define ADD1_DY SUB_DY
#define ADD2_DY	ADDSUB_DY
#elif  defined(RN) || defined(RT) || defined(CN) || defined(CT)
#define ADD1_DX	SUB_DX
#define ADD1_DY SUB_DY
#define ADD2_DY ADDSUB_DY
#else
#define ADD1_DX	ADD_DX
#define ADD1_DY ADD_DY
#define ADD2_DY ADDSUB_DY
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
SARQ $2,j;						# Rn = 4
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
SALQ $6, k;
LEAQ (bb, k, 1), prebb;			# Rn=4 SIZE=8 COMPLEX=2
MOVQ ba,ptrba;
MOVQ bm,i;
SARQ $2,i;						# Rm = 4
JLE .L1_loopE;
ALIGN_5;
.L1_bodyB:;
#if !defined(TRMMKERNEL)||(defined(TRMMKERNEL)&&defined(LEFT)&&defined(TRANSA))||(defined(TRMMKERNEL)&&!defined(LEFT)&&!defined(TRANSA))
MOVQ bb,ptrbb;
#else
MOVQ bb, ptrbb;
MOVQ kk, %rax;
SALQ $ZBASE_SHIFT, %rax;
LEAQ (ptrba, %rax, 4), ptrba;
LEAQ (ptrbb, %rax, 4), ptrbb;
#endif

PREFETCH0	0*SIZE(prebb);
PREFETCH0	8*SIZE(prebb);
PREFETCH0	16*SIZE(prebb)
ADDQ	$24*SIZE, prebb;
# Initial Results Register
XOR_DY	yvec15, yvec15, yvec15;
XOR_DY	yvec14, yvec14, yvec14;
EDUP_DY	0*SIZE(ptrbb), yvec2;		# Br1, Br1, Br2, Br2
XOR_DY	yvec13, yvec13, yvec13;
XOR_DY	yvec12, yvec12, yvec12;
EDUP_DY	4*SIZE(ptrbb), yvec3;		# Br3, Br3, Br4, Br4
PREFETCH2	3*SIZE(C0);
PREFETCH2	3*SIZE(C1);
XOR_DY	yvec11, yvec11, yvec11;
XOR_DY	yvec10, yvec10, yvec10;
LD_DY	0*SIZE(ptrba), yvec0;		# Ar1, Ai1, Ar2, Ai2
PREFETCH2	7*SIZE(C0, ldc, 1);
PREFETCH2	7*SIZE(C1, ldc, 1);
XOR_DY	yvec9, yvec9, yvec9;
XOR_DY	yvec8, yvec8, yvec8;
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
SARQ $2,k;						# Unroll 4 times
JLE .L2_loopE;
ALIGN_5;
.L2_bodyB:;
#### Computing kernel ####

#### Unroll time 1 ####
LD_DY	4*SIZE(ptrba), yvec1;
MUL_DY	yvec0, yvec2, yvec6;
SHUF_DY	$0x03, yvec2, yvec2, yvec4;	# Br2, Br2, Br1, Br1
MUL_DY	yvec0, yvec3, yvec7;
SHUF_DY	$0x03, yvec3, yvec3, yvec5;	# Br4, Br4, Br3, Br3
ADD1_DY	yvec6, yvec15, yvec15;
ADD1_DY	yvec7, yvec11, yvec11;

PREFETCH0	PRESIZE*SIZE(ptrba);
MUL_DY	yvec1, yvec2, yvec6;
EDUP_DY	1*SIZE(ptrbb), yvec2;		# Bi1, Bi1, Bi2, Bi2
MUL_DY	yvec1, yvec3, yvec7;
EDUP_DY	5*SIZE(ptrbb), yvec3;		# Bi3, Bi3, Bi4, Bi4
ADD1_DY	yvec6, yvec14, yvec14;
ADD1_DY	yvec7, yvec10, yvec10;

MUL_DY	yvec0, yvec4, yvec6;
MUL_DY	yvec0, yvec5, yvec7;
VPERMILP_DY	$0x05, yvec0, yvec0;	# Ai1, Ar1, Ai2, Ar2
ADD1_DY	yvec6, yvec13, yvec13;
ADD1_DY	yvec7, yvec9, yvec9;

MUL_DY	yvec1, yvec4, yvec6;
SHUF_DY	$0x03, yvec2, yvec2, yvec4;	# Bi2, Bi2, Bi1, Bi1
MUL_DY	yvec1, yvec5, yvec7;
SHUF_DY	$0x03, yvec3, yvec3, yvec5;	# Bi4, Bi4, Bi3, Bi3
ADD1_DY	yvec6, yvec12, yvec12;
ADD1_DY	yvec7, yvec8, yvec8;

VPERMILP_DY	$0x05, yvec1, yvec1;	# Ai3, Ar3, Ai4, Ar4
MUL_DY	yvec0, yvec2, yvec6;
MUL_DY	yvec0, yvec3, yvec7;
ADD2_DY	yvec6, yvec15, yvec15;
ADD2_DY	yvec7, yvec11, yvec11;

MUL_DY	yvec1, yvec2, yvec6;
EDUP_DY	8*SIZE(ptrbb), yvec2;
MUL_DY	yvec1, yvec3, yvec7;
EDUP_DY	12*SIZE(ptrbb), yvec3;
ADD2_DY	yvec6, yvec14, yvec14;
ADD2_DY	yvec7, yvec10, yvec10;

MUL_DY	yvec0, yvec4, yvec6;
MUL_DY	yvec0, yvec5, yvec7;
LD_DY	8*SIZE(ptrba), yvec0;
ADD2_DY	yvec6, yvec13, yvec13;
ADD2_DY	yvec7, yvec9, yvec9;

MUL_DY	yvec1, yvec4, yvec6;
MUL_DY	yvec1, yvec5, yvec7;
ADD2_DY	yvec6, yvec12, yvec12;
ADD2_DY	yvec7, yvec8, yvec8;

#### Unroll time 2 ####
LD_DY	12*SIZE(ptrba), yvec1;
MUL_DY	yvec0, yvec2, yvec6;
SHUF_DY	$0x03, yvec2, yvec2, yvec4;
MUL_DY	yvec0, yvec3, yvec7;
SHUF_DY	$0x03, yvec3, yvec3, yvec5;	# Br4, Br4, Br3, Br3
ADD1_DY	yvec6, yvec15, yvec15;
ADD1_DY	yvec7, yvec11, yvec11;

PREFETCH0	(PRESIZE+8)*SIZE(ptrba);
MUL_DY	yvec1, yvec2, yvec6;
EDUP_DY	9*SIZE(ptrbb), yvec2;		# Bi1, Bi1, Bi2, Bi2
MUL_DY	yvec1, yvec3, yvec7;
EDUP_DY	13*SIZE(ptrbb), yvec3;		# Bi3, Bi3, Bi4, Bi4
ADD1_DY	yvec6, yvec14, yvec14;
ADD1_DY	yvec7, yvec10, yvec10;

MUL_DY	yvec0, yvec4, yvec6;
MUL_DY	yvec0, yvec5, yvec7;
VPERMILP_DY	$0x05, yvec0, yvec0;	# Ai1, Ar1, Ai2, Ar2
ADD1_DY	yvec6, yvec13, yvec13;
ADD1_DY	yvec7, yvec9, yvec9;

MUL_DY	yvec1, yvec4, yvec6;
SHUF_DY	$0x03, yvec2, yvec2, yvec4;	# Bi2, Bi2, Bi1, Bi1
MUL_DY	yvec1, yvec5, yvec7;
SHUF_DY	$0x03, yvec3, yvec3, yvec5;	# Bi4, Bi4, Bi3, Bi3
ADD1_DY	yvec6, yvec12, yvec12;
ADD1_DY	yvec7, yvec8, yvec8;

VPERMILP_DY	$0x05, yvec1, yvec1;	# Ai3, Ar3, Ai4, Ar4
MUL_DY	yvec0, yvec2, yvec6;
MUL_DY	yvec0, yvec3, yvec7;
ADD2_DY	yvec6, yvec15, yvec15;
ADD2_DY	yvec7, yvec11, yvec11;

MUL_DY	yvec1, yvec2, yvec6;
EDUP_DY	16*SIZE(ptrbb), yvec2;
MUL_DY	yvec1, yvec3, yvec7;
EDUP_DY	20*SIZE(ptrbb), yvec3;
ADD2_DY	yvec6, yvec14, yvec14;
ADD2_DY	yvec7, yvec10, yvec10;

MUL_DY	yvec0, yvec4, yvec6;
MUL_DY	yvec0, yvec5, yvec7;
LD_DY	16*SIZE(ptrba), yvec0;
ADD2_DY	yvec6, yvec13, yvec13;
ADD2_DY	yvec7, yvec9, yvec9;

MUL_DY	yvec1, yvec4, yvec6;
MUL_DY	yvec1, yvec5, yvec7;
ADD2_DY	yvec6, yvec12, yvec12;
ADD2_DY	yvec7, yvec8, yvec8;

#### Unroll time 3 ####
LD_DY	20*SIZE(ptrba), yvec1;
MUL_DY	yvec0, yvec2, yvec6;
SHUF_DY	$0x03, yvec2, yvec2, yvec4;
MUL_DY	yvec0, yvec3, yvec7;
SHUF_DY	$0x03, yvec3, yvec3, yvec5;	# Br4, Br4, Br3, Br3
ADD1_DY	yvec6, yvec15, yvec15;
ADD1_DY	yvec7, yvec11, yvec11;

PREFETCH0	(PRESIZE+16)*SIZE(ptrba);
MUL_DY	yvec1, yvec2, yvec6;
EDUP_DY	17*SIZE(ptrbb), yvec2;		# Bi1, Bi1, Bi2, Bi2
MUL_DY	yvec1, yvec3, yvec7;
EDUP_DY	21*SIZE(ptrbb), yvec3;		# Bi3, Bi3, Bi4, Bi4
ADD1_DY	yvec6, yvec14, yvec14;
ADD1_DY	yvec7, yvec10, yvec10;

MUL_DY	yvec0, yvec4, yvec6;
MUL_DY	yvec0, yvec5, yvec7;
VPERMILP_DY	$0x05, yvec0, yvec0;	# Ai1, Ar1, Ai2, Ar2
ADD1_DY	yvec6, yvec13, yvec13;
ADD1_DY	yvec7, yvec9, yvec9;

MUL_DY	yvec1, yvec4, yvec6;
SHUF_DY	$0x03, yvec2, yvec2, yvec4;	# Bi2, Bi2, Bi1, Bi1
MUL_DY	yvec1, yvec5, yvec7;
SHUF_DY	$0x03, yvec3, yvec3, yvec5;	# Bi4, Bi4, Bi3, Bi3
ADD1_DY	yvec6, yvec12, yvec12;
ADD1_DY	yvec7, yvec8, yvec8;

VPERMILP_DY	$0x05, yvec1, yvec1;	# Ai3, Ar3, Ai4, Ar4
MUL_DY	yvec0, yvec2, yvec6;
MUL_DY	yvec0, yvec3, yvec7;
ADD2_DY	yvec6, yvec15, yvec15;
ADD2_DY	yvec7, yvec11, yvec11;

MUL_DY	yvec1, yvec2, yvec6;
EDUP_DY	24*SIZE(ptrbb), yvec2;
MUL_DY	yvec1, yvec3, yvec7;
EDUP_DY	28*SIZE(ptrbb), yvec3;
ADD2_DY	yvec6, yvec14, yvec14;
ADD2_DY	yvec7, yvec10, yvec10;

MUL_DY	yvec0, yvec4, yvec6;
MUL_DY	yvec0, yvec5, yvec7;
LD_DY	24*SIZE(ptrba), yvec0;
ADD2_DY	yvec6, yvec13, yvec13;
ADD2_DY	yvec7, yvec9, yvec9;

MUL_DY	yvec1, yvec4, yvec6;
MUL_DY	yvec1, yvec5, yvec7;
ADD2_DY	yvec6, yvec12, yvec12;
ADD2_DY	yvec7, yvec8, yvec8;

#### Unroll time 4 ####
LD_DY	28*SIZE(ptrba), yvec1;
MUL_DY	yvec0, yvec2, yvec6;
SHUF_DY	$0x03, yvec2, yvec2, yvec4;
MUL_DY	yvec0, yvec3, yvec7;
SHUF_DY	$0x03, yvec3, yvec3, yvec5;	# Br4, Br4, Br3, Br3
ADDQ	$32*SIZE, ptrba;
ADD1_DY	yvec6, yvec15, yvec15;
ADD1_DY	yvec7, yvec11, yvec11;

PREFETCH0	(PRESIZE+24)*SIZE(ptrba);
MUL_DY	yvec1, yvec2, yvec6;
EDUP_DY	25*SIZE(ptrbb), yvec2;		# Bi1, Bi1, Bi2, Bi2
MUL_DY	yvec1, yvec3, yvec7;
EDUP_DY	29*SIZE(ptrbb), yvec3;		# Bi3, Bi3, Bi4, Bi4
ADD1_DY	yvec6, yvec14, yvec14;
ADD1_DY	yvec7, yvec10, yvec10;

MUL_DY	yvec0, yvec4, yvec6;
MUL_DY	yvec0, yvec5, yvec7;
VPERMILP_DY	$0x05, yvec0, yvec0;	# Ai1, Ar1, Ai2, Ar2
ADDQ	$32*SIZE, ptrbb;
ADD1_DY	yvec6, yvec13, yvec13;
ADD1_DY	yvec7, yvec9, yvec9;

MUL_DY	yvec1, yvec4, yvec6;
SHUF_DY	$0x03, yvec2, yvec2, yvec4;	# Bi2, Bi2, Bi1, Bi1
MUL_DY	yvec1, yvec5, yvec7;
SHUF_DY	$0x03, yvec3, yvec3, yvec5;	# Bi4, Bi4, Bi3, Bi3
ADD1_DY	yvec6, yvec12, yvec12;
ADD1_DY	yvec7, yvec8, yvec8;

VPERMILP_DY	$0x05, yvec1, yvec1;	# Ai3, Ar3, Ai4, Ar4
MUL_DY	yvec0, yvec2, yvec6;
MUL_DY	yvec0, yvec3, yvec7;
ADD2_DY	yvec6, yvec15, yvec15;
ADD2_DY	yvec7, yvec11, yvec11;

MUL_DY	yvec1, yvec2, yvec6;
EDUP_DY	0*SIZE(ptrbb), yvec2;
MUL_DY	yvec1, yvec3, yvec7;
EDUP_DY	4*SIZE(ptrbb), yvec3;
ADD2_DY	yvec6, yvec14, yvec14;
ADD2_DY	yvec7, yvec10, yvec10;

MUL_DY	yvec0, yvec4, yvec6;
MUL_DY	yvec0, yvec5, yvec7;
LD_DY	0*SIZE(ptrba), yvec0;
ADD2_DY	yvec6, yvec13, yvec13;
ADD2_DY	yvec7, yvec9, yvec9;

MUL_DY	yvec1, yvec4, yvec6;
MUL_DY	yvec1, yvec5, yvec7;
ADD2_DY	yvec6, yvec12, yvec12;
ADD2_DY	yvec7, yvec8, yvec8;
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
.L3_bodyB:
#### Unroll time 1 ####
LD_DY	4*SIZE(ptrba), yvec1;
MUL_DY	yvec0, yvec2, yvec6;
SHUF_DY	$0x03, yvec2, yvec2, yvec4;	# Br2, Br2, Br1, Br1
MUL_DY	yvec0, yvec3, yvec7;
SHUF_DY	$0x03, yvec3, yvec3, yvec5;	# Br4, Br4, Br3, Br3
ADD1_DY	yvec6, yvec15, yvec15;
ADD1_DY	yvec7, yvec11, yvec11;

PREFETCH0	PRESIZE*SIZE(ptrba);
MUL_DY	yvec1, yvec2, yvec6;
EDUP_DY	1*SIZE(ptrbb), yvec2;		# Bi1, Bi1, Bi2, Bi2
MUL_DY	yvec1, yvec3, yvec7;
EDUP_DY	5*SIZE(ptrbb), yvec3;		# Bi3, Bi3, Bi4, Bi4
ADD1_DY	yvec6, yvec14, yvec14;
ADD1_DY	yvec7, yvec10, yvec10;

MUL_DY	yvec0, yvec4, yvec6;
MUL_DY	yvec0, yvec5, yvec7;
VPERMILP_DY	$0x05, yvec0, yvec0;	# Ai1, Ar1, Ai2, Ar2
ADD1_DY	yvec6, yvec13, yvec13;
ADD1_DY	yvec7, yvec9, yvec9;

MUL_DY	yvec1, yvec4, yvec6;
SHUF_DY	$0x03, yvec2, yvec2, yvec4;	# Bi2, Bi2, Bi1, Bi1
MUL_DY	yvec1, yvec5, yvec7;
SHUF_DY	$0x03, yvec3, yvec3, yvec5;	# Bi4, Bi4, Bi3, Bi3
ADD1_DY	yvec6, yvec12, yvec12;
ADD1_DY	yvec7, yvec8, yvec8;

VPERMILP_DY	$0x05, yvec1, yvec1;	# Ai3, Ar3, Ai4, Ar4
MUL_DY	yvec0, yvec2, yvec6;
MUL_DY	yvec0, yvec3, yvec7;
ADD2_DY	yvec6, yvec15, yvec15;
ADD2_DY	yvec7, yvec11, yvec11;

MUL_DY	yvec1, yvec2, yvec6;
EDUP_DY	8*SIZE(ptrbb), yvec2;
MUL_DY	yvec1, yvec3, yvec7;
EDUP_DY	12*SIZE(ptrbb), yvec3;
ADD2_DY	yvec6, yvec14, yvec14;
ADD2_DY	yvec7, yvec10, yvec10;

MUL_DY	yvec0, yvec4, yvec6;
MUL_DY	yvec0, yvec5, yvec7;
LD_DY	8*SIZE(ptrba), yvec0;
ADD2_DY	yvec6, yvec13, yvec13;
ADD2_DY	yvec7, yvec9, yvec9;

MUL_DY	yvec1, yvec4, yvec6;
MUL_DY	yvec1, yvec5, yvec7;
ADD2_DY	yvec6, yvec12, yvec12;
ADD2_DY	yvec7, yvec8, yvec8;

#### Unroll time 2 ####
LD_DY	12*SIZE(ptrba), yvec1;
MUL_DY	yvec0, yvec2, yvec6;
SHUF_DY	$0x03, yvec2, yvec2, yvec4;
MUL_DY	yvec0, yvec3, yvec7;
SHUF_DY	$0x03, yvec3, yvec3, yvec5;	# Br4, Br4, Br3, Br3
ADDQ	$16*SIZE, ptrba
ADD1_DY	yvec6, yvec15, yvec15;
ADD1_DY	yvec7, yvec11, yvec11;

PREFETCH0	(PRESIZE+8)*SIZE(ptrba);
MUL_DY	yvec1, yvec2, yvec6;
EDUP_DY	9*SIZE(ptrbb), yvec2;		# Bi1, Bi1, Bi2, Bi2
MUL_DY	yvec1, yvec3, yvec7;
EDUP_DY	13*SIZE(ptrbb), yvec3;		# Bi3, Bi3, Bi4, Bi4
ADD1_DY	yvec6, yvec14, yvec14;
ADD1_DY	yvec7, yvec10, yvec10;

MUL_DY	yvec0, yvec4, yvec6;
MUL_DY	yvec0, yvec5, yvec7;
VPERMILP_DY	$0x05, yvec0, yvec0;	# Ai1, Ar1, Ai2, Ar2
ADDQ	$16*SIZE, ptrbb
ADD1_DY	yvec6, yvec13, yvec13;
ADD1_DY	yvec7, yvec9, yvec9;

MUL_DY	yvec1, yvec4, yvec6;
SHUF_DY	$0x03, yvec2, yvec2, yvec4;	# Bi2, Bi2, Bi1, Bi1
MUL_DY	yvec1, yvec5, yvec7;
SHUF_DY	$0x03, yvec3, yvec3, yvec5;	# Bi4, Bi4, Bi3, Bi3
ADD1_DY	yvec6, yvec12, yvec12;
ADD1_DY	yvec7, yvec8, yvec8;

VPERMILP_DY	$0x05, yvec1, yvec1;	# Ai3, Ar3, Ai4, Ar4
MUL_DY	yvec0, yvec2, yvec6;
MUL_DY	yvec0, yvec3, yvec7;
ADD2_DY	yvec6, yvec15, yvec15;
ADD2_DY	yvec7, yvec11, yvec11;

MUL_DY	yvec1, yvec2, yvec6;
EDUP_DY	0*SIZE(ptrbb), yvec2;
MUL_DY	yvec1, yvec3, yvec7;
EDUP_DY	4*SIZE(ptrbb), yvec3;
ADD2_DY	yvec6, yvec14, yvec14;
ADD2_DY	yvec7, yvec10, yvec10;

MUL_DY	yvec0, yvec4, yvec6;
MUL_DY	yvec0, yvec5, yvec7;
LD_DY	0*SIZE(ptrba), yvec0;
ADD2_DY	yvec6, yvec13, yvec13;
ADD2_DY	yvec7, yvec9, yvec9;

MUL_DY	yvec1, yvec4, yvec6;
MUL_DY	yvec1, yvec5, yvec7;
ADD2_DY	yvec6, yvec12, yvec12;
ADD2_DY	yvec7, yvec8, yvec8;
.L3_loopE:;
#ifndef	TRMMKERNEL
TEST	$1, bk;
#else
TEST 	$1, kkk;
#endif
JLE		.L4_loopE;
ALIGN_5
.L4_loopB:;
#### Unroll time 1 ####
PREFETCH0	PRESIZE*SIZE(ptrba);
LD_DY	4*SIZE(ptrba), yvec1;
MUL_DY	yvec0, yvec2, yvec6;
SHUF_DY	$0x03, yvec2, yvec2, yvec4;
MUL_DY	yvec0, yvec3, yvec7;
SHUF_DY	$0x03, yvec3, yvec3, yvec5;	# Br4, Br4, Br3, Br3
ADDQ	$8*SIZE, ptrba;
ADD1_DY	yvec6, yvec15, yvec15;
ADD1_DY	yvec7, yvec11, yvec11;

MUL_DY	yvec1, yvec2, yvec6;
EDUP_DY	1*SIZE(ptrbb), yvec2;		# Bi1, Bi1, Bi2, Bi2
MUL_DY	yvec1, yvec3, yvec7;
EDUP_DY	5*SIZE(ptrbb), yvec3;		# Bi3, Bi3, Bi4, Bi4
ADD1_DY	yvec6, yvec14, yvec14;
ADD1_DY	yvec7, yvec10, yvec10;

MUL_DY	yvec0, yvec4, yvec6;
MUL_DY	yvec0, yvec5, yvec7;
VPERMILP_DY	$0x05, yvec0, yvec0;	# Ai1, Ar1, Ai2, Ar2
ADDQ	$8*SIZE, ptrbb;
ADD1_DY	yvec6, yvec13, yvec13;
ADD1_DY	yvec7, yvec9, yvec9;

MUL_DY	yvec1, yvec4, yvec6;
SHUF_DY	$0x03, yvec2, yvec2, yvec4;	# Bi2, Bi2, Bi1, Bi1
MUL_DY	yvec1, yvec5, yvec7;
SHUF_DY	$0x03, yvec3, yvec3, yvec5;	# Bi4, Bi4, Bi3, Bi3
ADD1_DY	yvec6, yvec12, yvec12;
ADD1_DY	yvec7, yvec8, yvec8;

VPERMILP_DY	$0x05, yvec1, yvec1;	# Ai3, Ar3, Ai4, Ar4
MUL_DY	yvec0, yvec2, yvec6;
MUL_DY	yvec0, yvec3, yvec7;
ADD2_DY	yvec6, yvec15, yvec15;
ADD2_DY	yvec7, yvec11, yvec11;

MUL_DY	yvec1, yvec2, yvec6;
MUL_DY	yvec1, yvec3, yvec7;
ADD2_DY	yvec6, yvec14, yvec14;
ADD2_DY	yvec7, yvec10, yvec10;

MUL_DY	yvec0, yvec4, yvec6;
MUL_DY	yvec0, yvec5, yvec7;
ADD2_DY	yvec6, yvec13, yvec13;
ADD2_DY	yvec7, yvec9, yvec9;

MUL_DY	yvec1, yvec4, yvec6;
MUL_DY	yvec1, yvec5, yvec7;
ADD2_DY	yvec6, yvec12, yvec12;
ADD2_DY	yvec7, yvec8, yvec8;
.L4_loopE:;
#### Handle ####
XOR_DY	yvec7, yvec7, yvec7;
#if  defined(RN) || defined(RT) || defined(CN) || defined(CT)
ADDSUB_DY	yvec15, yvec7, yvec15;
ADDSUB_DY	yvec14, yvec7, yvec14;
ADDSUB_DY	yvec13, yvec7, yvec13;
ADDSUB_DY	yvec12, yvec7, yvec12;
ADDSUB_DY	yvec11, yvec7, yvec11;
ADDSUB_DY	yvec10, yvec7, yvec10;
ADDSUB_DY	yvec9, yvec7, yvec9;
ADDSUB_DY	yvec8, yvec7, yvec8;
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
SUB_DY	yvec15, yvec7, yvec15;
SUB_DY	yvec14, yvec7, yvec14;
SUB_DY	yvec13, yvec7, yvec13;
SUB_DY	yvec12, yvec7, yvec12;
SUB_DY	yvec11, yvec7, yvec11;
SUB_DY	yvec10, yvec7, yvec10;
SUB_DY	yvec9, yvec7, yvec9;
SUB_DY	yvec8, yvec7, yvec8;
#elif defined(RR) || defined(RC) || defined(CR) || defined(CC)
VPERMILP_DY $0x05, yvec15, yvec15;
VPERMILP_DY $0x05, yvec14, yvec14;
VPERMILP_DY $0x05, yvec13, yvec13;
VPERMILP_DY $0x05, yvec12, yvec12;
VPERMILP_DY $0x05, yvec11, yvec11;
VPERMILP_DY $0x05, yvec10, yvec10;
VPERMILP_DY $0x05, yvec9, yvec9;
VPERMILP_DY $0x05, yvec8, yvec8;
ADDSUB_DY	yvec15, yvec7, yvec15;
ADDSUB_DY	yvec14, yvec7, yvec14;
ADDSUB_DY	yvec13, yvec7, yvec13;
ADDSUB_DY	yvec12, yvec7, yvec12;
ADDSUB_DY	yvec11, yvec7, yvec11;
ADDSUB_DY	yvec10, yvec7, yvec10;
ADDSUB_DY	yvec9, yvec7, yvec9;
ADDSUB_DY	yvec8, yvec7, yvec8;
VPERMILP_DY $0x05, yvec15, yvec15;
VPERMILP_DY $0x05, yvec14, yvec14;
VPERMILP_DY $0x05, yvec13, yvec13;
VPERMILP_DY $0x05, yvec12, yvec12;
VPERMILP_DY $0x05, yvec11, yvec11;
VPERMILP_DY $0x05, yvec10, yvec10;
VPERMILP_DY $0x05, yvec9, yvec9;
VPERMILP_DY $0x05, yvec8, yvec8;
#endif
#### Load Alpha ####
BROAD_DY MEMALPHA_R,yvec7;
BROAD_DY MEMALPHA_I,yvec6;
#### Multiply Alpha	####
VPERMILP_DY	$0x05, yvec15, yvec5;
MUL_DY	yvec7, yvec15, yvec15;
MUL_DY	yvec6, yvec5, yvec5;
ADDSUB_DY	yvec5, yvec15, yvec15;
VPERMILP_DY	$0x05, yvec14, yvec4;
MUL_DY 	yvec7, yvec14, yvec14;
MUL_DY 	yvec6, yvec4, yvec4;
ADDSUB_DY 	yvec4, yvec14, yvec14;
VPERMILP_DY	$0x05, yvec13, yvec3;
MUL_DY	yvec7, yvec13, yvec13;
MUL_DY 	yvec6, yvec3, yvec3;
ADDSUB_DY	yvec3, yvec13, yvec13;
VPERMILP_DY	$0x05,yvec12, yvec2;
MUL_DY 	yvec7, yvec12, yvec12;
MUL_DY 	yvec6, yvec2, yvec2;
ADDSUB_DY 	yvec2, yvec12, yvec12;
VPERMILP_DY $0x05, yvec11, yvec1;
MUL_DY 	yvec7, yvec11, yvec11;
MUL_DY 	yvec6, yvec1, yvec1;
ADDSUB_DY 	yvec1, yvec11, yvec11;
VPERMILP_DY $0x05,yvec10, yvec0;
MUL_DY 	yvec7, yvec10, yvec10;
MUL_DY 	yvec6, yvec0, yvec0;
ADDSUB_DY 	yvec0, yvec10, yvec10;
VPERMILP_DY $0x05, yvec9, yvec5;
MUL_DY 	yvec7, yvec9, yvec9;
MUL_DY 	yvec6, yvec5, yvec5;
ADDSUB_DY 	yvec5, yvec9, yvec9;
VPERMILP_DY $0x05, yvec8, yvec4;
MUL_DY 	yvec7, yvec8, yvec8;
MUL_DY 	yvec6, yvec4, yvec4;
ADDSUB_DY	yvec4, yvec8, yvec8;
#### Testing Alignment ####
MOVQ	C0, %rax;
OR		ldc, %rax;
TEST 	$15, %rax;
JNE		.L4_loopEx;
ALIGN_5
#### Store Back ####
EXTRA_DY $1,yvec15,xvec7;
EXTRA_DY $1,yvec14,xvec6;
EXTRA_DY $1,yvec13,xvec5;
EXTRA_DY $1,yvec12,xvec4;
EXTRA_DY $1,yvec11,xvec3;
EXTRA_DY $1,yvec10,xvec2;
EXTRA_DY $1,yvec9,xvec1;
EXTRA_DY $1,yvec8,xvec0;
#ifndef	TRMMKERNEL
ADD_DY 0*SIZE(C0),xvec15, xvec15;
ADD_DY 2*SIZE(C0,ldc,1), xvec7, xvec7;
ADD_DY 4*SIZE(C0),xvec14, xvec14;
ADD_DY 6*SIZE(C0,ldc,1),xvec6, xvec6;
ADD_DY 0*SIZE(C0,ldc,1),xvec13, xvec13;
ADD_DY 2*SIZE(C0),xvec5, xvec5;
ADD_DY 4*SIZE(C0,ldc,1),xvec12, xvec12;
ADD_DY 6*SIZE(C0),xvec4, xvec4;
ADD_DY 0*SIZE(C1),xvec11, xvec11;
ADD_DY 2*SIZE(C1,ldc,1),xvec3, xvec3;
ADD_DY 4*SIZE(C1),xvec10, xvec10;
ADD_DY 6*SIZE(C1,ldc,1),xvec2, xvec2;
ADD_DY 0*SIZE(C1,ldc,1),xvec9, xvec9;
ADD_DY 2*SIZE(C1),xvec1, xvec1;
ADD_DY 4*SIZE(C1,ldc,1),xvec8, xvec8;
ADD_DY 6*SIZE(C1),xvec0, xvec0;
#endif
ST_DY xvec15,0*SIZE(C0);
ST_DY xvec7,2*SIZE(C0,ldc,1);
ST_DY xvec14,4*SIZE(C0);
ST_DY xvec6,6*SIZE(C0,ldc,1);
ST_DY xvec13,0*SIZE(C0,ldc,1);
ST_DY xvec5,2*SIZE(C0);
ST_DY xvec12,4*SIZE(C0,ldc,1);
ST_DY xvec4,6*SIZE(C0);
ST_DY xvec11,0*SIZE(C1);
ST_DY xvec3,2*SIZE(C1,ldc,1);
ST_DY xvec10,4*SIZE(C1);
ST_DY xvec2,6*SIZE(C1,ldc,1);
ST_DY xvec9,0*SIZE(C1,ldc,1);
ST_DY xvec1,2*SIZE(C1);
ST_DY xvec8,4*SIZE(C1,ldc,1);
ST_DY xvec0,6*SIZE(C1);
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
ADDQ $8*SIZE,C0;
ADDQ $8*SIZE,C1;
.L1_bodyE:;
DECQ i;
JG .L1_bodyB;
JMP	.L1_loopE;
ALIGN_5
.L4_loopEx:
EXTRA_DY $1, yvec15, xvec7;
EXTRA_DY $1, yvec14, xvec6;
#ifndef	TRMMKERNEL
LDL_DY	0*SIZE(C0), xvec0, xvec0;
LDH_DY	1*SIZE(C0), xvec0, xvec0;
LDL_DY	2*SIZE(C0, ldc, 1), xvec1, xvec1;
LDH_DY	3*SIZE(C0, ldc, 1), xvec1, xvec1;
LDL_DY	4*SIZE(C0), xvec2, xvec2;
LDH_DY	5*SIZE(C0), xvec2, xvec2;
LDL_DY	6*SIZE(C0, ldc, 1), xvec3, xvec3;
LDH_DY	7*SIZE(C0, ldc, 1), xvec3, xvec3;
ADD_DY	xvec0, xvec15, xvec15;
ADD_DY	xvec1, xvec7, xvec7;
ADD_DY	xvec2, xvec14, xvec14;
ADD_DY	xvec3, xvec6, xvec6;
#endif
STL_DY	xvec15, 0*SIZE(C0);
STH_DY	xvec15, 1*SIZE(C0);
STL_DY	xvec7, 2*SIZE(C0, ldc, 1);
STH_DY	xvec7, 3*SIZE(C0, ldc, 1);
STL_DY	xvec14, 4*SIZE(C0);
STH_DY	xvec14, 5*SIZE(C0);
STL_DY	xvec6, 6*SIZE(C0, ldc, 1);
STH_DY	xvec6, 7*SIZE(C0, ldc, 1);
EXTRA_DY $1, yvec13, xvec5;
EXTRA_DY $1, yvec12, xvec4;
#ifndef	TRMMKERNEL
LDL_DY	0*SIZE(C0, ldc, 1), xvec3, xvec3;
LDH_DY	1*SIZE(C0, ldc, 1), xvec3, xvec3;
LDL_DY	2*SIZE(C0), xvec2, xvec2;
LDH_DY	3*SIZE(C0), xvec2, xvec2;
LDL_DY	4*SIZE(C0, ldc, 1), xvec1, xvec1;
LDH_DY	5*SIZE(C0, ldc, 1), xvec1, xvec1;
LDL_DY	6*SIZE(C0), xvec0, xvec0;
LDH_DY	7*SIZE(C0), xvec0, xvec0;
ADD_DY	xvec3, xvec13, xvec13;
ADD_DY	xvec2, xvec5, xvec5;
ADD_DY	xvec1, xvec12, xvec12;
ADD_DY	xvec0, xvec4, xvec4;
#endif
STL_DY	xvec13, 0*SIZE(C0, ldc, 1);
STH_DY	xvec13, 1*SIZE(C0, ldc, 1);
STL_DY	xvec5, 2*SIZE(C0);
STH_DY	xvec5, 3*SIZE(C0);
STL_DY	xvec12, 4*SIZE(C0, ldc, 1);
STH_DY	xvec12, 5*SIZE(C0, ldc, 1);
STL_DY	xvec4, 6*SIZE(C0);
STH_DY	xvec4, 7*SIZE(C0);
EXTRA_DY $1, yvec11, xvec3;
EXTRA_DY $1, yvec10, xvec2;
#ifndef	TRMMKERNEL
LDL_DY	0*SIZE(C1), xvec7, xvec7;
LDH_DY	1*SIZE(C1), xvec7, xvec7;
LDL_DY	2*SIZE(C1, ldc, 1), xvec6, xvec6;
LDH_DY	3*SIZE(C1, ldc, 1), xvec6, xvec6;
LDL_DY	4*SIZE(C1), xvec5, xvec5;
LDH_DY	5*SIZE(C1), xvec5, xvec5;
LDL_DY	6*SIZE(C1, ldc, 1), xvec4, xvec4;
LDH_DY	7*SIZE(C1, ldc, 1), xvec4, xvec4;
ADD_DY	xvec7, xvec11, xvec11;
ADD_DY	xvec6, xvec3, xvec3;
ADD_DY	xvec5, xvec10, xvec10;
ADD_DY	xvec4, xvec2, xvec2;
#endif
STL_DY	xvec11, 0*SIZE(C1);
STH_DY	xvec11, 1*SIZE(C1);
STL_DY	xvec3, 2*SIZE(C1, ldc, 1);
STH_DY	xvec3, 3*SIZE(C1, ldc, 1);
STL_DY	xvec10, 4*SIZE(C1);
STH_DY	xvec10, 5*SIZE(C1);
STL_DY	xvec2, 6*SIZE(C1, ldc, 1);
STH_DY	xvec2, 7*SIZE(C1, ldc, 1);
EXTRA_DY $1, yvec9, xvec1;
EXTRA_DY $1, yvec8, xvec0;
#ifndef	TRMMKERNEL
LDL_DY	0*SIZE(C1, ldc, 1), xvec5, xvec5;
LDH_DY	1*SIZE(C1, ldc, 1), xvec5, xvec5;
LDL_DY	2*SIZE(C1), xvec4, xvec4;
LDH_DY	3*SIZE(C1), xvec4, xvec4;
LDL_DY	4*SIZE(C1, ldc, 1), xvec3, xvec3;
LDH_DY	5*SIZE(C1, ldc, 1), xvec3, xvec3;
LDL_DY	6*SIZE(C1), xvec2, xvec2;
LDH_DY	7*SIZE(C1), xvec2, xvec2;
ADD_DY	xvec5, xvec9, xvec9;
ADD_DY	xvec4, xvec1, xvec1;
ADD_DY	xvec3, xvec8, xvec8;
ADD_DY	xvec2, xvec0, xvec0;
#endif
STL_DY	xvec9, 0*SIZE(C1, ldc, 1);
STH_DY	xvec9, 1*SIZE(C1, ldc, 1);
STL_DY	xvec1, 2*SIZE(C1);
STH_DY	xvec1, 3*SIZE(C1);
STL_DY	xvec8, 4*SIZE(C1, ldc, 1);
STH_DY	xvec8, 5*SIZE(C1, ldc, 1);
STL_DY	xvec0, 6*SIZE(C1);
STH_DY	xvec0, 7*SIZE(C1);
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
DECQ	i;
JG .L1_bodyB;
ALIGN_5;
.L1_loopE:;
TEST $2, bm;
JLE .L5_loopE;
ALIGN_5
.L5_bodyB:
#if !defined(TRMMKERNEL)||(defined(TRMMKERNEL)&&defined(LEFT)&&defined(TRANSA))||(defined(TRMMKERNEL)&&!defined(LEFT)&&!defined(TRANSA))
MOVQ bb,ptrbb;
#else
MOVQ bb, ptrbb;
MOVQ kk, %rax;
SALQ $ZBASE_SHIFT, %rax;
LEAQ (ptrba, %rax, 2), ptrba;
LEAQ (ptrbb, %rax, 4), ptrbb;
#endif
XOR_DY	yvec15, yvec15, yvec15;
XOR_DY	yvec14, yvec14, yvec14;
XOR_DY	yvec13, yvec13, yvec13;
XOR_DY	yvec12, yvec12, yvec12;
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
JLE .L7_loopE;
ALIGN_5
.L7_bodyB:
#### Compute kernel ####
#### Unroll times 1 ####
LD_DY	0*SIZE(ptrba), yvec0;
EDUP_DY	0*SIZE(ptrbb), yvec2;
EDUP_DY	4*SIZE(ptrbb), yvec3;

MUL_DY	yvec0, yvec2, yvec6;
ADD1_DY	yvec6, yvec15, yvec15;
SHUF_DY	$0x03, yvec2, yvec2, yvec4;
MUL_DY	yvec0, yvec3, yvec7;
ADD1_DY	yvec7, yvec14, yvec14;
SHUF_DY	$0x03, yvec3, yvec3, yvec5;

MUL_DY	yvec0, yvec4, yvec6;
ADD1_DY	yvec6, yvec13, yvec13;
EDUP_DY	1*SIZE(ptrbb), yvec2;
MUL_DY	yvec0, yvec5, yvec7;
ADD1_DY	yvec7 ,yvec12, yvec12;
EDUP_DY	5*SIZE(ptrbb), yvec3

VPERMILP_DY	$0x05, yvec0, yvec0;
MUL_DY	yvec0, yvec2, yvec6;
ADD2_DY	yvec6, yvec15, yvec15;
SHUF_DY	$0x03, yvec2, yvec2, yvec4;
MUL_DY	yvec0, yvec3, yvec7;
ADD2_DY	yvec7, yvec14, yvec14;
SHUF_DY	$0x03, yvec3, yvec3, yvec5;

MUL_DY	yvec0, yvec4, yvec6;
ADD2_DY	yvec6, yvec13, yvec13;
MUL_DY	yvec0, yvec5, yvec7;
ADD2_DY	yvec7, yvec12, yvec12;

#### Unroll time 2 ####
LD_DY	4*SIZE(ptrba), yvec0;
EDUP_DY	8*SIZE(ptrbb), yvec2;
EDUP_DY	12*SIZE(ptrbb), yvec3;

MUL_DY	yvec0, yvec2, yvec6;
ADD1_DY	yvec6, yvec15, yvec15;
SHUF_DY	$0x03, yvec2, yvec2, yvec4;
MUL_DY	yvec0, yvec3, yvec7;
ADD1_DY	yvec7, yvec14, yvec14;
SHUF_DY	$0x03, yvec3, yvec3, yvec5;

MUL_DY	yvec0, yvec4, yvec6;
ADD1_DY	yvec6, yvec13, yvec13;
EDUP_DY	9*SIZE(ptrbb), yvec2;
MUL_DY	yvec0, yvec5, yvec7;
ADD1_DY	yvec7 ,yvec12, yvec12;
EDUP_DY	13*SIZE(ptrbb), yvec3

VPERMILP_DY	$0x05, yvec0, yvec0;
MUL_DY	yvec0, yvec2, yvec6;
ADD2_DY	yvec6, yvec15, yvec15;
SHUF_DY	$0x03, yvec2, yvec2, yvec4;
MUL_DY	yvec0, yvec3, yvec7;
ADD2_DY	yvec7, yvec14, yvec14;
SHUF_DY	$0x03, yvec3, yvec3, yvec5;

MUL_DY	yvec0, yvec4, yvec6;
ADD2_DY	yvec6, yvec13, yvec13;
MUL_DY	yvec0, yvec5, yvec7;
ADD2_DY	yvec7, yvec12, yvec12;

#### Unroll time 3 ####
LD_DY	8*SIZE(ptrba), yvec0;
EDUP_DY	16*SIZE(ptrbb), yvec2;
EDUP_DY	20*SIZE(ptrbb), yvec3;

MUL_DY	yvec0, yvec2, yvec6;
ADD1_DY	yvec6, yvec15, yvec15;
SHUF_DY	$0x03, yvec2, yvec2, yvec4;
MUL_DY	yvec0, yvec3, yvec7;
ADD1_DY	yvec7, yvec14, yvec14;
SHUF_DY	$0x03, yvec3, yvec3, yvec5;

MUL_DY	yvec0, yvec4, yvec6;
ADD1_DY	yvec6, yvec13, yvec13;
EDUP_DY	17*SIZE(ptrbb), yvec2;
MUL_DY	yvec0, yvec5, yvec7;
ADD1_DY	yvec7 ,yvec12, yvec12;
EDUP_DY	21*SIZE(ptrbb), yvec3

VPERMILP_DY	$0x05, yvec0, yvec0;
MUL_DY	yvec0, yvec2, yvec6;
ADD2_DY	yvec6, yvec15, yvec15;
SHUF_DY	$0x03, yvec2, yvec2, yvec4;
MUL_DY	yvec0, yvec3, yvec7;
ADD2_DY	yvec7, yvec14, yvec14;
SHUF_DY	$0x03, yvec3, yvec3, yvec5;

MUL_DY	yvec0, yvec4, yvec6;
ADD2_DY	yvec6, yvec13, yvec13;
MUL_DY	yvec0, yvec5, yvec7;
ADD2_DY	yvec7, yvec12, yvec12;

#### Unroll time 4 ####
LD_DY	12*SIZE(ptrba), yvec0;
EDUP_DY	24*SIZE(ptrbb), yvec2;
EDUP_DY	28*SIZE(ptrbb), yvec3;

MUL_DY	yvec0, yvec2, yvec6;
ADD1_DY	yvec6, yvec15, yvec15;
SHUF_DY	$0x03, yvec2, yvec2, yvec4;
MUL_DY	yvec0, yvec3, yvec7;
ADD1_DY	yvec7, yvec14, yvec14;
SHUF_DY	$0x03, yvec3, yvec3, yvec5;

MUL_DY	yvec0, yvec4, yvec6;
ADD1_DY	yvec6, yvec13, yvec13;
EDUP_DY	25*SIZE(ptrbb), yvec2;
MUL_DY	yvec0, yvec5, yvec7;
ADD1_DY	yvec7 ,yvec12, yvec12;
EDUP_DY	29*SIZE(ptrbb), yvec3

VPERMILP_DY	$0x05, yvec0, yvec0;
MUL_DY	yvec0, yvec2, yvec6;
ADD2_DY	yvec6, yvec15, yvec15;
SHUF_DY	$0x03, yvec2, yvec2, yvec4;
MUL_DY	yvec0, yvec3, yvec7;
ADD2_DY	yvec7, yvec14, yvec14;
SHUF_DY	$0x03, yvec3, yvec3, yvec5;

MUL_DY	yvec0, yvec4, yvec6;
ADD2_DY	yvec6, yvec13, yvec13;
ADDQ	$16*SIZE, ptrba;
MUL_DY	yvec0, yvec5, yvec7;
ADD2_DY	yvec7, yvec12, yvec12;
ADDQ	$32*SIZE, ptrbb;
DECQ	k;
JG .L7_bodyB;
ALIGN_5
.L7_loopE:
#ifndef	TRMMKERNEL
TEST $2, bk;
#else
TEST $2, kkk;
#endif
JLE .L8_loopE;
ALIGN_5
.L8_bodyB:
#### Unroll times 1 ####
LD_DY	0*SIZE(ptrba), yvec0;
EDUP_DY	0*SIZE(ptrbb), yvec2;
EDUP_DY	4*SIZE(ptrbb), yvec3;

MUL_DY	yvec0, yvec2, yvec6;
ADD1_DY	yvec6, yvec15, yvec15;
SHUF_DY	$0x03, yvec2, yvec2, yvec4;
MUL_DY	yvec0, yvec3, yvec7;
ADD1_DY	yvec7, yvec14, yvec14;
SHUF_DY	$0x03, yvec3, yvec3, yvec5;

MUL_DY	yvec0, yvec4, yvec6;
ADD1_DY	yvec6, yvec13, yvec13;
EDUP_DY	1*SIZE(ptrbb), yvec2;
MUL_DY	yvec0, yvec5, yvec7;
ADD1_DY	yvec7 ,yvec12, yvec12;
EDUP_DY	5*SIZE(ptrbb), yvec3

VPERMILP_DY	$0x05, yvec0, yvec0;
MUL_DY	yvec0, yvec2, yvec6;
ADD2_DY	yvec6, yvec15, yvec15;
SHUF_DY	$0x03, yvec2, yvec2, yvec4;
MUL_DY	yvec0, yvec3, yvec7;
ADD2_DY	yvec7, yvec14, yvec14;
SHUF_DY	$0x03, yvec3, yvec3, yvec5;

MUL_DY	yvec0, yvec4, yvec6;
ADD2_DY	yvec6, yvec13, yvec13;
MUL_DY	yvec0, yvec5, yvec7;
ADD2_DY	yvec7, yvec12, yvec12;

#### Unroll time 2 ####
LD_DY	4*SIZE(ptrba), yvec0;
EDUP_DY	8*SIZE(ptrbb), yvec2;
EDUP_DY	12*SIZE(ptrbb), yvec3;

MUL_DY	yvec0, yvec2, yvec6;
ADD1_DY	yvec6, yvec15, yvec15;
SHUF_DY	$0x03, yvec2, yvec2, yvec4;
MUL_DY	yvec0, yvec3, yvec7;
ADD1_DY	yvec7, yvec14, yvec14;
SHUF_DY	$0x03, yvec3, yvec3, yvec5;

MUL_DY	yvec0, yvec4, yvec6;
ADD1_DY	yvec6, yvec13, yvec13;
EDUP_DY	9*SIZE(ptrbb), yvec2;
MUL_DY	yvec0, yvec5, yvec7;
ADD1_DY	yvec7 ,yvec12, yvec12;
EDUP_DY	13*SIZE(ptrbb), yvec3

VPERMILP_DY	$0x05, yvec0, yvec0;
MUL_DY	yvec0, yvec2, yvec6;
ADD2_DY	yvec6, yvec15, yvec15;
SHUF_DY	$0x03, yvec2, yvec2, yvec4;
MUL_DY	yvec0, yvec3, yvec7;
ADD2_DY	yvec7, yvec14, yvec14;
SHUF_DY	$0x03, yvec3, yvec3, yvec5;

MUL_DY	yvec0, yvec4, yvec6;
ADD2_DY	yvec6, yvec13, yvec13;
ADDQ	$8*SIZE, ptrba;
MUL_DY	yvec0, yvec5, yvec7;
ADD2_DY	yvec7, yvec12, yvec12;
ADDQ	$16*SIZE, ptrbb;
.L8_loopE:
#ifndef	TRMMKERNEL
TEST $1, bk;
#else
TEST $1, kkk;
#endif
JLE .L9_loopE;
ALIGN_5
.L9_bodyB:
#### Unroll times 1 ####
LD_DY	0*SIZE(ptrba), yvec0;
EDUP_DY	0*SIZE(ptrbb), yvec2;
EDUP_DY	4*SIZE(ptrbb), yvec3;

MUL_DY	yvec0, yvec2, yvec6;
ADD1_DY	yvec6, yvec15, yvec15;
SHUF_DY	$0x03, yvec2, yvec2, yvec4;
MUL_DY	yvec0, yvec3, yvec7;
ADD1_DY	yvec7, yvec14, yvec14;
SHUF_DY	$0x03, yvec3, yvec3, yvec5;

MUL_DY	yvec0, yvec4, yvec6;
ADD1_DY	yvec6, yvec13, yvec13;
EDUP_DY	1*SIZE(ptrbb), yvec2;
MUL_DY	yvec0, yvec5, yvec7;
ADD1_DY	yvec7 ,yvec12, yvec12;
EDUP_DY	5*SIZE(ptrbb), yvec3

VPERMILP_DY	$0x05, yvec0, yvec0;
MUL_DY	yvec0, yvec2, yvec6;
ADD2_DY	yvec6, yvec15, yvec15;
SHUF_DY	$0x03, yvec2, yvec2, yvec4;
MUL_DY	yvec0, yvec3, yvec7;
ADD2_DY	yvec7, yvec14, yvec14;
SHUF_DY	$0x03, yvec3, yvec3, yvec5;

MUL_DY	yvec0, yvec4, yvec6;
ADD2_DY	yvec6, yvec13, yvec13;
MUL_DY	yvec0, yvec5, yvec7;
ADD2_DY	yvec7, yvec12, yvec12;
ADDQ	$4*SIZE, ptrba;
ADDQ	$8*SIZE, ptrbb;

.L9_loopE:
#### Handle ####
XOR_DY	yvec7, yvec7, yvec7;
#if  defined(RN) || defined(RT) || defined(CN) || defined(CT)
ADDSUB_DY	yvec15, yvec7, yvec15;
ADDSUB_DY	yvec14, yvec7, yvec14;
ADDSUB_DY	yvec13, yvec7, yvec13;
ADDSUB_DY	yvec12, yvec7, yvec12;
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
SUB_DY	yvec15, yvec7, yvec15;
SUB_DY	yvec14, yvec7, yvec14;
SUB_DY	yvec13, yvec7, yvec13;
SUB_DY	yvec12, yvec7, yvec12;
#elif defined(RR) || defined(RC) || defined(CR) || defined(CC)
VPERMILP_DY $0x05, yvec15, yvec15;
VPERMILP_DY $0x05, yvec14, yvec14;
VPERMILP_DY $0x05, yvec13, yvec13;
VPERMILP_DY $0x05, yvec12, yvec12;
ADDSUB_DY	yvec15, yvec7, yvec15;
ADDSUB_DY	yvec14, yvec7, yvec14;
ADDSUB_DY	yvec13, yvec7, yvec13;
ADDSUB_DY	yvec12, yvec7, yvec12;
VPERMILP_DY $0x05, yvec15, yvec15;
VPERMILP_DY $0x05, yvec14, yvec14;
VPERMILP_DY $0x05, yvec13, yvec13;
VPERMILP_DY $0x05, yvec12, yvec12;
#endif
#### Load Alpha ####
BROAD_DY	MEMALPHA_R, yvec7;
BROAD_DY	MEMALPHA_I, yvec6;
#### Multiply Alpha ####
VPERMILP_DY	$0x05, yvec15, yvec5;
MUL_DY	yvec7, yvec15, yvec15;
MUL_DY	yvec6, yvec5, yvec5;
ADD2_DY	yvec5, yvec15, yvec15;
VPERMILP_DY	$0x05, yvec14, yvec4;
MUL_DY 	yvec7, yvec14, yvec14;
MUL_DY 	yvec6, yvec4, yvec4;
ADD2_DY 	yvec4, yvec14, yvec14;
VPERMILP_DY	$0x05, yvec13, yvec3;
MUL_DY	yvec7, yvec13, yvec13;
MUL_DY 	yvec6, yvec3, yvec3;
ADD2_DY	yvec3, yvec13, yvec13;
VPERMILP_DY	$0x05,yvec12, yvec2;
MUL_DY 	yvec7, yvec12, yvec12;
MUL_DY 	yvec6, yvec2, yvec2;
ADD2_DY 	yvec2, yvec12, yvec12;
#### Testing Alignment ####
MOVQ	C0, %rax;
OR		ldc, %rax;
TEST	$15, %rax;
JNE		.L9_loopEx;
ALIGN_5
#### Writing back ####
EXTRA_DY $1, yvec15, xvec7;
EXTRA_DY $1, yvec14, xvec6;
EXTRA_DY $1, yvec13, xvec5;
EXTRA_DY $1, yvec12, xvec4;
#ifndef	TRMMKERNEL
ADD_DX 0*SIZE(C0), xvec15, xvec15;
ADD_DX 2*SIZE(C0, ldc, 1), xvec7, xvec7;
ADD_DX 0*SIZE(C0, ldc, 1), xvec13, xvec13;
ADD_DX 2*SIZE(C0), xvec5, xvec5;
ADD_DX 0*SIZE(C1), xvec14, xvec14;
ADD_DX 2*SIZE(C1, ldc, 1), xvec6, xvec6;
ADD_DX 0*SIZE(C1, ldc, 1), xvec12, xvec12;
ADD_DX 2*SIZE(C1), xvec4, xvec4;
#endif
ST_DX	xvec15, 0*SIZE(C0);
ST_DX	xvec7, 2*SIZE(C0, ldc, 1);
ST_DX	xvec13, 0*SIZE(C0, ldc, 1);
ST_DX	xvec5, 2*SIZE(C0);
ST_DX	xvec14, 0*SIZE(C1);
ST_DX	xvec6, 2*SIZE(C1, ldc, 1);
ST_DX	xvec12, 0*SIZE(C1, ldc, 1);
ST_DX	xvec4, 2*SIZE(C1);
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
JMP .L5_loopE;
ALIGN_5
.L9_loopEx:
EXTRA_DY $1, yvec15, xvec7;
EXTRA_DY $1, yvec14, xvec6;
EXTRA_DY $1, yvec13, xvec5;
EXTRA_DY $1, yvec12, xvec4;
#ifndef	TRMMKERNEL
LDL_DX 0*SIZE(C0), xvec0, xvec0;
LDH_DX 1*SIZE(C0), xvec0, xvec0;
LDL_DX 2*SIZE(C0, ldc, 1), xvec1, xvec1;
LDH_DX 3*SIZE(C0, ldc, 1), xvec1, xvec1;
LDL_DX 0*SIZE(C0, ldc, 1), xvec2, xvec2;
LDH_DX 1*SIZE(C0, ldc, 1), xvec2, xvec2;
LDL_DX 2*SIZE(C0), xvec3, xvec3;
LDH_DX 3*SIZE(C0), xvec3, xvec3;
ADD_DX xvec0, xvec15, xvec15;
ADD_DX xvec1, xvec7, xvec7;
ADD_DX xvec2, xvec13, xvec13;
ADD_DX xvec3, xvec5, xvec5;
#endif
STL_DX	xvec15, 0*SIZE(C0);
STH_DX	xvec15, 1*SIZE(C0);
STL_DX	xvec7, 2*SIZE(C0, ldc, 1);
STH_DX	xvec7, 3*SIZE(C0, ldc, 1);
STL_DX	xvec13, 0*SIZE(C0, ldc, 1);
STH_DX	xvec13, 1*SIZE(C0, ldc, 1);
STL_DX	xvec5, 2*SIZE(C0);
STH_DX	xvec5, 3*SIZE(C0);
#ifndef	TRMMKERNEL
LDL_DX 0*SIZE(C1), xvec0, xvec0;
LDH_DX 1*SIZE(C1), xvec0, xvec0;
LDL_DX 2*SIZE(C1, ldc, 1), xvec1, xvec1;
LDH_DX 3*SIZE(C1, ldc, 1), xvec1, xvec1;
LDL_DX 0*SIZE(C1, ldc, 1), xvec2, xvec2;
LDH_DX 1*SIZE(C1, ldc, 1), xvec2, xvec2;
LDL_DX 2*SIZE(C1), xvec3, xvec3;
LDH_DX 3*SIZE(C1), xvec3, xvec3;
ADD_DX xvec0, xvec14, xvec14;
ADD_DX xvec1, xvec6, xvec6;
ADD_DX xvec2, xvec12, xvec12;
ADD_DX xvec3, xvec4, xvec4;
#endif
STL_DX	xvec14, 0*SIZE(C1);
STH_DX	xvec14, 1*SIZE(C1);
STL_DX	xvec6, 2*SIZE(C1, ldc, 1);
STH_DX	xvec6, 3*SIZE(C1, ldc, 1);
STL_DX	xvec12, 0*SIZE(C1, ldc, 1);
STH_DX	xvec12, 1*SIZE(C1, ldc, 1);
STL_DX	xvec4, 2*SIZE(C1);
STH_DX	xvec4, 3*SIZE(C1);
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
.L5_loopE:
TEST $1, bm;
JLE .L6_loopE;
ALIGN_5
.L6_bodyB:
#if !defined(TRMMKERNEL)||(defined(TRMMKERNEL)&&defined(LEFT)&&defined(TRANSA))||(defined(TRMMKERNEL)&&!defined(LEFT)&&!defined(TRANSA))
MOVQ bb,ptrbb;
#else
MOVQ bb, ptrbb;
MOVQ kk, %rax;
SALQ $ZBASE_SHIFT, %rax;
ADDQ %rax, ptrba;
LEAQ (ptrbb, %rax, 4), ptrbb;
#endif
XOR_DY	yvec15, yvec15, yvec15;
XOR_DY	yvec14, yvec14, yvec14;
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
JLE .L10_loopE;
ALIGN_5
.L10_bodyB:
LD_DY	0*SIZE(ptrba), yvec0;	#### A1r A1i A2r A2i
EDUP_DY	0*SIZE(ptrbb), yvec2;
EDUP_DY	4*SIZE(ptrbb), yvec3;

SHUF_DY	$0x20, yvec0, yvec0, yvec1;
MUL_DY	yvec1, yvec2, yvec6;
ADD1_DY	yvec6, yvec15, yvec15;
MUL_DY	yvec1, yvec3, yvec7;
ADD1_DY	yvec7, yvec14, yvec14;

VPERMILP_DY	$0x05, yvec1, yvec4;
EDUP_DY	1*SIZE(ptrbb), yvec2;
EDUP_DY	5*SIZE(ptrbb), yvec3;
MUL_DY	yvec4, yvec2, yvec6;
ADD2_DY	yvec6, yvec15, yvec15;
MUL_DY	yvec4, yvec3, yvec7;
ADD2_DY	yvec7, yvec14, yvec14;

SHUF_DY	$0x31, yvec0, yvec0, yvec1;
EDUP_DY	8*SIZE(ptrbb), yvec2;
EDUP_DY	12*SIZE(ptrbb), yvec3;

MUL_DY	yvec1, yvec2, yvec6;
ADD1_DY	yvec6, yvec15, yvec15;
MUL_DY	yvec1, yvec3, yvec7;
ADD1_DY	yvec7, yvec14, yvec14;

VPERMILP_DY	$0x05, yvec1, yvec4;
EDUP_DY	9*SIZE(ptrbb), yvec2;
EDUP_DY	13*SIZE(ptrbb), yvec3;
MUL_DY	yvec4, yvec2, yvec6;
ADD2_DY	yvec6, yvec15, yvec15;
MUL_DY	yvec4, yvec3, yvec7;
ADD2_DY	yvec7, yvec14, yvec14;

LD_DY	4*SIZE(ptrba), yvec0;
EDUP_DY	16*SIZE(ptrbb), yvec2;
EDUP_DY	20*SIZE(ptrbb), yvec3;

SHUF_DY	$0x20, yvec0, yvec0, yvec1;
MUL_DY	yvec1, yvec2, yvec6;
ADD1_DY	yvec6, yvec15, yvec15;
MUL_DY	yvec1, yvec3, yvec7;
ADD1_DY	yvec7, yvec14, yvec14;

VPERMILP_DY	$0x05, yvec1, yvec4;
EDUP_DY	17*SIZE(ptrbb), yvec2;
EDUP_DY	21*SIZE(ptrbb), yvec3;
MUL_DY	yvec4, yvec2, yvec6;
ADD2_DY	yvec6, yvec15, yvec15;
MUL_DY	yvec4, yvec3, yvec7;
ADD2_DY	yvec7, yvec14, yvec14;

SHUF_DY	$0x31, yvec0, yvec0, yvec1;
EDUP_DY	24*SIZE(ptrbb), yvec2;
EDUP_DY	28*SIZE(ptrbb), yvec3;
MUL_DY	yvec1, yvec2, yvec6;
ADD1_DY	yvec6, yvec15, yvec15;
MUL_DY	yvec1, yvec3, yvec7;
ADD1_DY	yvec7, yvec14, yvec14;

VPERMILP_DY	$0x05, yvec1, yvec4;
EDUP_DY	25*SIZE(ptrbb), yvec2;
EDUP_DY	29*SIZE(ptrbb), yvec3;
MUL_DY	yvec4, yvec2, yvec6;
ADD2_DY	yvec6, yvec15, yvec15;
MUL_DY	yvec4, yvec3, yvec7;
ADD2_DY	yvec7, yvec14, yvec14
ADDQ	$8*SIZE, ptrba;
ADDQ	$32*SIZE, ptrbb;
DECQ	k;
JG .L10_bodyB;
ALIGN_5
.L10_loopE:
#ifndef	TRMMKERNEL
TEST $2, bk;
#else
TEST $2, kkk;
#endif
JLE .L11_loopE;
ALIGN_5
.L11_bodyB:
LD_DY	0*SIZE(ptrba), yvec0;	#### A1r A1i A2r A2i
EDUP_DY	0*SIZE(ptrbb), yvec2;
EDUP_DY	4*SIZE(ptrbb), yvec3;

SHUF_DY	$0x20, yvec0, yvec0, yvec1;
MUL_DY	yvec1, yvec2, yvec6;
ADD1_DY	yvec6, yvec15, yvec15;
MUL_DY	yvec1, yvec3, yvec7;
ADD1_DY	yvec7, yvec14, yvec14;

VPERMILP_DY	$0x05, yvec1, yvec4;
EDUP_DY	1*SIZE(ptrbb), yvec2;
EDUP_DY	5*SIZE(ptrbb), yvec3;
MUL_DY	yvec4, yvec2, yvec6;
ADD2_DY	yvec6, yvec15, yvec15;
MUL_DY	yvec4, yvec3, yvec7;
ADD2_DY	yvec7, yvec14, yvec14;

SHUF_DY	$0x31, yvec0, yvec0, yvec1;
EDUP_DY	8*SIZE(ptrbb), yvec2;
EDUP_DY	12*SIZE(ptrbb), yvec3;

MUL_DY	yvec1, yvec2, yvec6;
ADD1_DY	yvec6, yvec15, yvec15;
MUL_DY	yvec1, yvec3, yvec7;
ADD1_DY	yvec7, yvec14, yvec14;

VPERMILP_DY	$0x05, yvec1, yvec4;
EDUP_DY	9*SIZE(ptrbb), yvec2;
EDUP_DY	13*SIZE(ptrbb), yvec3;
MUL_DY	yvec4, yvec2, yvec6;
ADD2_DY	yvec6, yvec15, yvec15;
MUL_DY	yvec4, yvec3, yvec7;
ADD2_DY	yvec7, yvec14, yvec14;
ADDQ	$4*SIZE, ptrba;
ADDQ	$16*SIZE, ptrbb;

.L11_loopE:
#ifndef	TRMMKERNEL
TEST $1, bk;
#else
TEST $1, kkk;
#endif
JLE .L12_loopE;
ALIGN_5
.L12_bodyB:
LD_DY	0*SIZE(ptrba), yvec0;	#### A1r A1i A2r A2i
EDUP_DY	0*SIZE(ptrbb), yvec2;
EDUP_DY	4*SIZE(ptrbb), yvec3;

SHUF_DY	$0x20, yvec0, yvec0, yvec1;
MUL_DY	yvec1, yvec2, yvec6;
ADD1_DY	yvec6, yvec15, yvec15;
MUL_DY	yvec1, yvec3, yvec7;
ADD1_DY	yvec7, yvec14, yvec14;

VPERMILP_DY	$0x05, yvec1, yvec4;
EDUP_DY	1*SIZE(ptrbb), yvec2;
EDUP_DY	5*SIZE(ptrbb), yvec3;
MUL_DY	yvec4, yvec2, yvec6;
ADD2_DY	yvec6, yvec15, yvec15;
MUL_DY	yvec4, yvec3, yvec7;
ADD2_DY	yvec7, yvec14, yvec14;
ADDQ	$2*SIZE, ptrba;
ADDQ	$8*SIZE, ptrbb;

.L12_loopE:
#### Handle ####
XOR_DY	yvec7, yvec7, yvec7;
#if  defined(RN) || defined(RT) || defined(CN) || defined(CT)
ADDSUB_DY	yvec15, yvec7, yvec15;
ADDSUB_DY	yvec14, yvec7, yvec14;
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
SUB_DY	yvec15, yvec7, yvec15;
SUB_DY	yvec14, yvec7, yvec14;
#elif defined(RR) || defined(RC) || defined(CR) || defined(CC)
VPERMILP_DY $0x05, yvec15, yvec15;
VPERMILP_DY $0x05, yvec14, yvec14;
ADDSUB_DY	yvec15, yvec7, yvec15;
ADDSUB_DY	yvec14, yvec7, yvec14;
VPERMILP_DY $0x05, yvec15, yvec15;
VPERMILP_DY $0x05, yvec14, yvec14;
#endif
#### Multiply Alpha	####
BROAD_DY MEMALPHA_R, yvec7;
BROAD_DY MEMALPHA_I, yvec6;
VPERMILP_DY	$0x05, yvec15, yvec5;
MUL_DY	yvec7, yvec15, yvec15;
MUL_DY	yvec6, yvec5, yvec5;
ADD2_DY	yvec5, yvec15, yvec15;
VPERMILP_DY	$0x05, yvec14, yvec4;
MUL_DY 	yvec7, yvec14, yvec14;
MUL_DY 	yvec6, yvec4, yvec4;
ADD2_DY 	yvec4, yvec14, yvec14;
#### Writing Back ####
EXTRA_DY $1, yvec15, xvec7;
EXTRA_DY $1, yvec14, xvec6;
#ifndef	TRMMKERNEL
LDL_DX 0*SIZE(C0), xvec0, xvec0;
LDH_DX 1*SIZE(C0), xvec0, xvec0;
LDL_DX 0*SIZE(C0, ldc, 1), xvec1, xvec1;
LDH_DX 1*SIZE(C0, ldc, 1), xvec1, xvec1;
LDL_DX 0*SIZE(C1), xvec2, xvec2;
LDH_DX 1*SIZE(C1), xvec2, xvec2;
LDL_DX 0*SIZE(C1, ldc, 1), xvec3, xvec3;
LDH_DX 1*SIZE(C1, ldc, 1), xvec3, xvec3;
ADD_DX xvec0, xvec15, xvec15;
ADD_DX xvec1, xvec7, xvec7;
ADD_DX xvec2, xvec14, xvec14;
ADD_DX xvec3, xvec6, xvec6;
#endif
STL_DX	xvec15, 0*SIZE(C0);
STH_DX	xvec15, 1*SIZE(C0);
STL_DX	xvec7, 0*SIZE(C0, ldc, 1);
STH_DX	xvec7, 1*SIZE(C0, ldc, 1);
STL_DX	xvec14, 0*SIZE(C1);
STH_DX	xvec14, 1*SIZE(C1);
STL_DX	xvec6, 0*SIZE(C1, ldc, 1);
STH_DX	xvec6, 1*SIZE(C1, ldc, 1);
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
.L6_loopE:
#if defined(TRMMKERNEL) && !defined(LEFT)
ADDQ	$4, kk;
#endif
MOVQ bk,k;
SALQ $6,k;
ADDQ k,bb;
LEAQ (C,ldc,4),C;
.L0_bodyE:;
DECQ j;
JG .L0_bodyB;
ALIGN_5;
.L0_loopE:;
TEST $2, bn;
JLE	.L20_loopE;
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
SARQ $2, i;
JLE .L21_loopE;
ALIGN_5
.L21_bodyB:
#if !defined(TRMMKERNEL)||(defined(TRMMKERNEL)&&defined(LEFT)&&defined(TRANSA))||(defined(TRMMKERNEL)&&!defined(LEFT)&&!defined(TRANSA))
MOVQ bb,ptrbb;
#else
MOVQ bb, ptrbb;
MOVQ kk, %rax;
SALQ $ZBASE_SHIFT, %rax;
LEAQ (ptrba, %rax, 4), ptrba;
LEAQ (ptrbb, %rax, 2), ptrbb;
#endif
XOR_DY yvec15, yvec15, yvec15;
XOR_DY yvec14, yvec14, yvec14;
XOR_DY yvec13, yvec13, yvec13;
XOR_DY yvec12, yvec12, yvec12;
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
JLE .L211_loopE;
ALIGN_5
.L211_bodyB:
#### Unroll time 1 ####
EDUP_DY	0*SIZE(ptrbb), yvec2;
LD_DY	0*SIZE(ptrba), yvec0;
MUL_DY	yvec0, yvec2, yvec6;
ADD1_DY	yvec6, yvec15, yvec15;
SHUF_DY	$0x03, yvec2, yvec2, yvec4;
LD_DY	4*SIZE(ptrba), yvec1;
MUL_DY	yvec1, yvec2, yvec7;
ADD1_DY	yvec7, yvec14, yvec14;

EDUP_DY	1*SIZE(ptrbb), yvec3;
MUL_DY	yvec0, yvec4, yvec6;
ADD1_DY	yvec6, yvec13, yvec13;
VPERMILP_DY	$0x05, yvec0, yvec0;
MUL_DY	yvec1, yvec4, yvec7;
ADD1_DY 	yvec7, yvec12, yvec12;
VPERMILP_DY	$0x05, yvec1, yvec1;

MUL_DY	yvec0, yvec3, yvec6;
ADD2_DY	yvec6, yvec15, yvec15;
SHUF_DY	$0x03, yvec3, yvec3, yvec5;
MUL_DY	yvec1, yvec3, yvec7;
ADD2_DY	yvec7, yvec14, yvec14;

MUL_DY	yvec0, yvec5, yvec6;
ADD2_DY	yvec6, yvec13, yvec13;
MUL_DY	yvec1, yvec5, yvec7;
ADD2_DY	yvec7, yvec12, yvec12;

#### Unroll time 2 ####
EDUP_DY	4*SIZE(ptrbb), yvec2;
LD_DY	8*SIZE(ptrba), yvec0;
MUL_DY	yvec0, yvec2, yvec6;
ADD1_DY	yvec6, yvec15, yvec15;
SHUF_DY	$0x03, yvec2, yvec2, yvec4;
LD_DY	12*SIZE(ptrba), yvec1;
MUL_DY	yvec1, yvec2, yvec7;
ADD1_DY	yvec7, yvec14, yvec14;

EDUP_DY	5*SIZE(ptrbb), yvec3;
MUL_DY	yvec0, yvec4, yvec6;
ADD1_DY	yvec6, yvec13, yvec13;
VPERMILP_DY	$0x05, yvec0, yvec0;
MUL_DY	yvec1, yvec4, yvec7;
ADD1_DY 	yvec7, yvec12, yvec12;
VPERMILP_DY	$0x05, yvec1, yvec1;

MUL_DY	yvec0, yvec3, yvec6;
ADD2_DY	yvec6, yvec15, yvec15;
SHUF_DY	$0x03, yvec3, yvec3, yvec5;
MUL_DY	yvec1, yvec3, yvec7;
ADD2_DY	yvec7, yvec14, yvec14;

MUL_DY	yvec0, yvec5, yvec6;
ADD2_DY	yvec6, yvec13, yvec13;
MUL_DY	yvec1, yvec5, yvec7;
ADD2_DY	yvec7, yvec12, yvec12;

#### Unroll time 3 ####
EDUP_DY	8*SIZE(ptrbb), yvec2;
LD_DY	16*SIZE(ptrba), yvec0;
MUL_DY	yvec0, yvec2, yvec6;
ADD1_DY	yvec6, yvec15, yvec15;
SHUF_DY	$0x03, yvec2, yvec2, yvec4;
LD_DY	20*SIZE(ptrba), yvec1;
MUL_DY	yvec1, yvec2, yvec7;
ADD1_DY	yvec7, yvec14, yvec14;

EDUP_DY	9*SIZE(ptrbb), yvec3;
MUL_DY	yvec0, yvec4, yvec6;
ADD1_DY	yvec6, yvec13, yvec13;
VPERMILP_DY	$0x05, yvec0, yvec0;
MUL_DY	yvec1, yvec4, yvec7;
ADD1_DY 	yvec7, yvec12, yvec12;
VPERMILP_DY	$0x05, yvec1, yvec1;

MUL_DY	yvec0, yvec3, yvec6;
ADD2_DY	yvec6, yvec15, yvec15;
SHUF_DY	$0x03, yvec3, yvec3, yvec5;
MUL_DY	yvec1, yvec3, yvec7;
ADD2_DY	yvec7, yvec14, yvec14;

MUL_DY	yvec0, yvec5, yvec6;
ADD2_DY	yvec6, yvec13, yvec13;
MUL_DY	yvec1, yvec5, yvec7;
ADD2_DY	yvec7, yvec12, yvec12;

#### Unroll time 4 ####
EDUP_DY	12*SIZE(ptrbb), yvec2;
LD_DY	24*SIZE(ptrba), yvec0;
MUL_DY	yvec0, yvec2, yvec6;
ADD1_DY	yvec6, yvec15, yvec15;
SHUF_DY	$0x03, yvec2, yvec2, yvec4;
LD_DY	28*SIZE(ptrba), yvec1;
MUL_DY	yvec1, yvec2, yvec7;
ADD1_DY	yvec7, yvec14, yvec14;

EDUP_DY	13*SIZE(ptrbb), yvec3;
MUL_DY	yvec0, yvec4, yvec6;
ADD1_DY	yvec6, yvec13, yvec13;
VPERMILP_DY	$0x05, yvec0, yvec0;
MUL_DY	yvec1, yvec4, yvec7;
ADD1_DY 	yvec7, yvec12, yvec12;
VPERMILP_DY	$0x05, yvec1, yvec1;

MUL_DY	yvec0, yvec3, yvec6;
ADD2_DY	yvec6, yvec15, yvec15;
SHUF_DY	$0x03, yvec3, yvec3, yvec5;
MUL_DY	yvec1, yvec3, yvec7;
ADD2_DY	yvec7, yvec14, yvec14;
ADDQ	$16*SIZE, ptrbb;

MUL_DY	yvec0, yvec5, yvec6;
ADD2_DY	yvec6, yvec13, yvec13;
MUL_DY	yvec1, yvec5, yvec7;
ADD2_DY	yvec7, yvec12, yvec12;
ADDQ	$32*SIZE, ptrba;
DECQ	k;
JG .L211_bodyB;
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
#### Unroll time 1 ####
EDUP_DY	0*SIZE(ptrbb), yvec2;
LD_DY	0*SIZE(ptrba), yvec0;
MUL_DY	yvec0, yvec2, yvec6;
ADD1_DY	yvec6, yvec15, yvec15;
SHUF_DY	$0x03, yvec2, yvec2, yvec4;
LD_DY	4*SIZE(ptrba), yvec1;
MUL_DY	yvec1, yvec2, yvec7;
ADD1_DY	yvec7, yvec14, yvec14;

EDUP_DY	1*SIZE(ptrbb), yvec3;
MUL_DY	yvec0, yvec4, yvec6;
ADD1_DY	yvec6, yvec13, yvec13;
VPERMILP_DY	$0x05, yvec0, yvec0;
MUL_DY	yvec1, yvec4, yvec7;
ADD1_DY 	yvec7, yvec12, yvec12;
VPERMILP_DY	$0x05, yvec1, yvec1;

MUL_DY	yvec0, yvec3, yvec6;
ADD2_DY	yvec6, yvec15, yvec15;
SHUF_DY	$0x03, yvec3, yvec3, yvec5;
MUL_DY	yvec1, yvec3, yvec7;
ADD2_DY	yvec7, yvec14, yvec14;

MUL_DY	yvec0, yvec5, yvec6;
ADD2_DY	yvec6, yvec13, yvec13;
MUL_DY	yvec1, yvec5, yvec7;
ADD2_DY	yvec7, yvec12, yvec12;

#### Unroll time 2 ####
EDUP_DY	4*SIZE(ptrbb), yvec2;
LD_DY	8*SIZE(ptrba), yvec0;
MUL_DY	yvec0, yvec2, yvec6;
ADD1_DY	yvec6, yvec15, yvec15;
SHUF_DY	$0x03, yvec2, yvec2, yvec4;
LD_DY	12*SIZE(ptrba), yvec1;
MUL_DY	yvec1, yvec2, yvec7;
ADD1_DY	yvec7, yvec14, yvec14;

EDUP_DY	5*SIZE(ptrbb), yvec3;
MUL_DY	yvec0, yvec4, yvec6;
ADD1_DY	yvec6, yvec13, yvec13;
VPERMILP_DY	$0x05, yvec0, yvec0;
MUL_DY	yvec1, yvec4, yvec7;
ADD1_DY 	yvec7, yvec12, yvec12;
VPERMILP_DY	$0x05, yvec1, yvec1;

MUL_DY	yvec0, yvec3, yvec6;
ADD2_DY	yvec6, yvec15, yvec15;
SHUF_DY	$0x03, yvec3, yvec3, yvec5;
MUL_DY	yvec1, yvec3, yvec7;
ADD2_DY	yvec7, yvec14, yvec14;

MUL_DY	yvec0, yvec5, yvec6;
ADD2_DY	yvec6, yvec13, yvec13;
MUL_DY	yvec1, yvec5, yvec7;
ADD2_DY	yvec7, yvec12, yvec12;

ADDQ	$8*SIZE, ptrbb;
ADDQ	$16*SIZE, ptrba;

.L212_loopE:
#ifndef	TRMMKERNEL
TEST $1, bk;
#else
TEST $1, kkk;
#endif
JLE .L213_loopE;
ALIGN_5
.L213_bodyB:
#### Unroll time 1 ####
EDUP_DY	0*SIZE(ptrbb), yvec2;
LD_DY	0*SIZE(ptrba), yvec0;
MUL_DY	yvec0, yvec2, yvec6;
ADD1_DY	yvec6, yvec15, yvec15;
SHUF_DY	$0x03, yvec2, yvec2, yvec4;
LD_DY	4*SIZE(ptrba), yvec1;
MUL_DY	yvec1, yvec2, yvec7;
ADD1_DY	yvec7, yvec14, yvec14;

EDUP_DY	1*SIZE(ptrbb), yvec3;
MUL_DY	yvec0, yvec4, yvec6;
ADD1_DY	yvec6, yvec13, yvec13;
VPERMILP_DY	$0x05, yvec0, yvec0;
MUL_DY	yvec1, yvec4, yvec7;
ADD1_DY 	yvec7, yvec12, yvec12;
VPERMILP_DY	$0x05, yvec1, yvec1;

MUL_DY	yvec0, yvec3, yvec6;
ADD2_DY	yvec6, yvec15, yvec15;
SHUF_DY	$0x03, yvec3, yvec3, yvec5;
MUL_DY	yvec1, yvec3, yvec7;
ADD2_DY	yvec7, yvec14, yvec14;

MUL_DY	yvec0, yvec5, yvec6;
ADD2_DY	yvec6, yvec13, yvec13;
MUL_DY	yvec1, yvec5, yvec7;
ADD2_DY	yvec7, yvec12, yvec12;
ADDQ	$4*SIZE, ptrbb;
ADDQ	$8*SIZE, ptrba;

.L213_loopE:
#### Handle ####
XOR_DY	yvec7, yvec7, yvec7;
#if  defined(RN) || defined(RT) || defined(CN) || defined(CT)
ADDSUB_DY	yvec15, yvec7, yvec15;
ADDSUB_DY	yvec14, yvec7, yvec14;
ADDSUB_DY	yvec13, yvec7, yvec13;
ADDSUB_DY	yvec12, yvec7, yvec12;
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
SUB_DY	yvec15, yvec7, yvec15;
SUB_DY	yvec14, yvec7, yvec14;
SUB_DY	yvec13, yvec7, yvec13;
SUB_DY	yvec12, yvec7, yvec12;
#elif defined(RR) || defined(RC) || defined(CR) || defined(CC)
VPERMILP_DY $0x05, yvec15, yvec15;
VPERMILP_DY $0x05, yvec14, yvec14;
VPERMILP_DY $0x05, yvec13, yvec13;
VPERMILP_DY $0x05, yvec12, yvec12;
ADDSUB_DY	yvec15, yvec7, yvec15;
ADDSUB_DY	yvec14, yvec7, yvec14;
ADDSUB_DY	yvec13, yvec7, yvec13;
ADDSUB_DY	yvec12, yvec7, yvec12;
VPERMILP_DY $0x05, yvec15, yvec15;
VPERMILP_DY $0x05, yvec14, yvec14;
VPERMILP_DY $0x05, yvec13, yvec13;
VPERMILP_DY $0x05, yvec12, yvec12;
#endif
#### Load Alpha ####
BROAD_DY MEMALPHA_R,yvec7;
BROAD_DY MEMALPHA_I,yvec6;
#### Multiply Alpha	####
VPERMILP_DY	$0x05, yvec15, yvec5;
MUL_DY	yvec7, yvec15, yvec15;
MUL_DY	yvec6, yvec5, yvec5;
ADD2_DY	yvec5, yvec15, yvec15;
VPERMILP_DY	$0x05, yvec14, yvec4;
MUL_DY 	yvec7, yvec14, yvec14;
MUL_DY 	yvec6, yvec4, yvec4;
ADD2_DY 	yvec4, yvec14, yvec14;
VPERMILP_DY	$0x05, yvec13, yvec3;
MUL_DY	yvec7, yvec13, yvec13;
MUL_DY 	yvec6, yvec3, yvec3;
ADD2_DY	yvec3, yvec13, yvec13;
VPERMILP_DY	$0x05,yvec12, yvec2;
MUL_DY 	yvec7, yvec12, yvec12;
MUL_DY 	yvec6, yvec2, yvec2;
ADD2_DY 	yvec2, yvec12, yvec12;
EXTRA_DY $1, yvec15, xvec7;
EXTRA_DY $1, yvec14, xvec6;
EXTRA_DY $1, yvec13, xvec5;
EXTRA_DY $1, yvec12, xvec4;
#### Testing Alignment ####
MOVQ	C0, %rax;
OR		ldc, %rax;
TEST 	$15, %rax;
JNE		.L213_loopEx;
ALIGN_5
#### Writing back ####
#ifndef	TRMMKERNEL
ADD_DX 0*SIZE(C0), xvec15, xvec15;
ADD_DX 2*SIZE(C1), xvec7, xvec7;
ADD_DX 4*SIZE(C0), xvec14, xvec14;
ADD_DX 6*SIZE(C1), xvec6, xvec6;
ADD_DX 0*SIZE(C1), xvec13, xvec13;
ADD_DX 2*SIZE(C0), xvec5, xvec5;
ADD_DX 4*SIZE(C1), xvec12, xvec12;
ADD_DX 6*SIZE(C0), xvec4, xvec4;
#endif
ST_DX xvec15,0*SIZE(C0);
ST_DX xvec7,2*SIZE(C1);
ST_DX xvec14,4*SIZE(C0);
ST_DX xvec6,6*SIZE(C1);
ST_DX xvec13,0*SIZE(C1);
ST_DX xvec5,2*SIZE(C0);
ST_DX xvec12,4*SIZE(C1);
ST_DX xvec4,6*SIZE(C0);
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
ADDQ $8*SIZE, C0;
ADDQ $8*SIZE, C1;
DECQ	i;
JG .L21_bodyB;
JMP .L21_loopE;
ALIGN_5
.L213_loopEx:
#ifndef	TRMMKERNEL
LDL_DX 0*SIZE(C0), xvec0, xvec0;
LDH_DX 1*SIZE(C0), xvec0, xvec0;
LDL_DX 2*SIZE(C1), xvec1, xvec1;
LDH_DX 3*SIZE(C1), xvec1, xvec1;
LDL_DX 4*SIZE(C0), xvec2, xvec2;
LDH_DX 5*SIZE(C0), xvec2, xvec2;
LDL_DX 6*SIZE(C1), xvec3, xvec3;
LDH_DX 7*SIZE(C1), xvec3, xvec3;
ADD_DX xvec0, xvec15, xvec15;
ADD_DX xvec1, xvec7, xvec7;
ADD_DX xvec2, xvec14, xvec14;
ADD_DX xvec3, xvec6, xvec6;
#endif
STL_DX	xvec15, 0*SIZE(C0);
STH_DX	xvec15, 1*SIZE(C0);
STL_DX	xvec7, 2*SIZE(C1);
STH_DX	xvec7, 3*SIZE(C1);
STL_DX	xvec14, 4*SIZE(C0);
STH_DX	xvec14, 5*SIZE(C0);
STL_DX	xvec6, 6*SIZE(C1);
STH_DX	xvec6, 7*SIZE(C1);
#ifndef	TRMMKERNEL
LDL_DX 0*SIZE(C1), xvec3, xvec3;
LDH_DX 1*SIZE(C1), xvec3, xvec3;
LDL_DX 2*SIZE(C0), xvec2, xvec2;
LDH_DX 3*SIZE(C0), xvec2, xvec2;
LDL_DX 4*SIZE(C1), xvec1, xvec1;
LDH_DX 5*SIZE(C1), xvec1, xvec1;
LDL_DX 6*SIZE(C0), xvec0, xvec0;
LDH_DX 7*SIZE(C0), xvec0, xvec0;
ADD_DX xvec3, xvec13, xvec13;
ADD_DX xvec2, xvec5, xvec5;
ADD_DX xvec1, xvec12, xvec12;
ADD_DX xvec0, xvec4, xvec4;
#endif
STL_DX	xvec13, 0*SIZE(C1);
STH_DX	xvec13, 1*SIZE(C1);
STL_DX	xvec5, 2*SIZE(C0);
STH_DX	xvec5, 3*SIZE(C0);
STL_DX	xvec12, 4*SIZE(C1);
STH_DX	xvec12, 5*SIZE(C1);
STL_DX	xvec4, 6*SIZE(C0);
STH_DX	xvec4, 7*SIZE(C0);
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
DECQ	i;
JG .L21_bodyB;
ALIGN_5
.L21_loopE:
TEST $2, bm;
JLE .L22_loopE;
ALIGN_5
.L22_bodyB:
#if !defined(TRMMKERNEL)||(defined(TRMMKERNEL)&&defined(LEFT)&&defined(TRANSA))||(defined(TRMMKERNEL)&&!defined(LEFT)&&!defined(TRANSA))
MOVQ bb,ptrbb;
#else
MOVQ bb, ptrbb;
MOVQ kk, %rax;
SALQ $ZBASE_SHIFT, %rax;
LEAQ (ptrba, %rax, 2), ptrba;
LEAQ (ptrbb, %rax, 2), ptrbb;
#endif
XOR_DY	yvec15, yvec15, yvec15;
XOR_DY	yvec14, yvec14, yvec13;
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
JLE .L221_loopE;
ALIGN_5
.L221_bodyB:
#### Unroll time 1 ####
EDUP_DY	0*SIZE(ptrbb), yvec2;
LD_DY	0*SIZE(ptrba), yvec0;
SHUF_DY	$0x03, yvec2, yvec2, yvec4;
MUL_DY	yvec0, yvec2, yvec6;
ADD1_DY	yvec6, yvec15, yvec15;

EDUP_DY	1*SIZE(ptrbb), yvec3;
MUL_DY	yvec0, yvec4, yvec6;
ADD1_DY	yvec6, yvec13, yvec13;
VPERMILP_DY	$0x05, yvec0, yvec0;

MUL_DY	yvec0, yvec3, yvec6;
ADD2_DY	yvec6, yvec15, yvec15;
SHUF_DY	$0x03, yvec3, yvec3, yvec5;

MUL_DY	yvec0, yvec5, yvec6;
ADD2_DY	yvec6, yvec13, yvec13;

#### Unroll time 2 ####
EDUP_DY	4*SIZE(ptrbb), yvec2;
LD_DY	4*SIZE(ptrba), yvec0;
SHUF_DY	$0x03, yvec2, yvec2, yvec4;
MUL_DY	yvec0, yvec2, yvec6;
ADD1_DY	yvec6, yvec15, yvec15;

EDUP_DY	5*SIZE(ptrbb), yvec3;
MUL_DY	yvec0, yvec4, yvec6;
ADD1_DY	yvec6, yvec13, yvec13;
VPERMILP_DY	$0x05, yvec0, yvec0;

MUL_DY	yvec0, yvec3, yvec6;
ADD2_DY	yvec6, yvec15, yvec15;
SHUF_DY	$0x03, yvec3, yvec3, yvec5;

MUL_DY	yvec0, yvec5, yvec6;
ADD2_DY	yvec6, yvec13, yvec13;

#### Unroll time 3 ####
EDUP_DY	8*SIZE(ptrbb), yvec2;
LD_DY	8*SIZE(ptrba), yvec0;
MUL_DY	yvec0, yvec2, yvec6;
ADD1_DY	yvec6, yvec15, yvec15;
SHUF_DY	$0x03, yvec2, yvec2, yvec4;

EDUP_DY	9*SIZE(ptrbb), yvec3;
MUL_DY	yvec0, yvec4, yvec6;
ADD1_DY	yvec6, yvec13, yvec13;
VPERMILP_DY	$0x05, yvec0, yvec0;

MUL_DY	yvec0, yvec3, yvec6;
ADD2_DY	yvec6, yvec15, yvec15;
SHUF_DY	$0x03, yvec3, yvec3, yvec5;

MUL_DY	yvec0, yvec5, yvec6;
ADD2_DY	yvec6, yvec13, yvec13;

#### Unroll time 4 ####
EDUP_DY	12*SIZE(ptrbb), yvec2;
LD_DY	12*SIZE(ptrba), yvec0;
MUL_DY	yvec0, yvec2, yvec6;
ADD1_DY	yvec6, yvec15, yvec15;
SHUF_DY	$0x03, yvec2, yvec2, yvec4;

EDUP_DY	13*SIZE(ptrbb), yvec3;
MUL_DY	yvec0, yvec4, yvec6;
ADD1_DY	yvec6, yvec13, yvec13;
VPERMILP_DY	$0x05, yvec0, yvec0;

MUL_DY	yvec0, yvec3, yvec6;
ADD2_DY	yvec6, yvec15, yvec15;
SHUF_DY	$0x03, yvec3, yvec3, yvec5;
ADDQ	$16*SIZE, ptrbb;

MUL_DY	yvec0, yvec5, yvec6;
ADD2_DY	yvec6, yvec13, yvec13;
ADDQ	$16*SIZE, ptrba;
DECQ	k;
JG .L221_bodyB;
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
#### Unroll time 1 ####
EDUP_DY	0*SIZE(ptrbb), yvec2;
LD_DY	0*SIZE(ptrba), yvec0;
SHUF_DY	$0x03, yvec2, yvec2, yvec4;
MUL_DY	yvec0, yvec2, yvec6;
ADD1_DY	yvec6, yvec15, yvec15;

EDUP_DY	1*SIZE(ptrbb), yvec3;
MUL_DY	yvec0, yvec4, yvec6;
ADD1_DY	yvec6, yvec13, yvec13;
VPERMILP_DY	$0x05, yvec0, yvec0;

MUL_DY	yvec0, yvec3, yvec6;
ADD2_DY	yvec6, yvec15, yvec15;
SHUF_DY	$0x03, yvec3, yvec3, yvec5;

MUL_DY	yvec0, yvec5, yvec6;
ADD2_DY	yvec6, yvec13, yvec13;

#### Unroll time 2 ####
EDUP_DY	4*SIZE(ptrbb), yvec2;
LD_DY	4*SIZE(ptrba), yvec0;
SHUF_DY	$0x03, yvec2, yvec2, yvec4;
MUL_DY	yvec0, yvec2, yvec6;
ADD1_DY	yvec6, yvec15, yvec15;

EDUP_DY	5*SIZE(ptrbb), yvec3;
MUL_DY	yvec0, yvec4, yvec6;
ADD1_DY	yvec6, yvec13, yvec13;
VPERMILP_DY	$0x05, yvec0, yvec0;

MUL_DY	yvec0, yvec3, yvec6;
ADD2_DY	yvec6, yvec15, yvec15;
SHUF_DY	$0x03, yvec3, yvec3, yvec5;

MUL_DY	yvec0, yvec5, yvec6;
ADD2_DY	yvec6, yvec13, yvec13;
ADDQ	$8*SIZE, ptrba;
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
#### Unroll time 1 ####
EDUP_DY	0*SIZE(ptrbb), yvec2;
LD_DY	0*SIZE(ptrba), yvec0;
SHUF_DY	$0x03, yvec2, yvec2, yvec4;
MUL_DY	yvec0, yvec2, yvec6;
ADD1_DY	yvec6, yvec15, yvec15;

EDUP_DY	1*SIZE(ptrbb), yvec3;
MUL_DY	yvec0, yvec4, yvec6;
ADD1_DY	yvec6, yvec13, yvec13;
VPERMILP_DY	$0x05, yvec0, yvec0;

MUL_DY	yvec0, yvec3, yvec6;
ADD2_DY	yvec6, yvec15, yvec15;
SHUF_DY	$0x03, yvec3, yvec3, yvec5;

MUL_DY	yvec0, yvec5, yvec6;
ADD2_DY	yvec6, yvec13, yvec13;
ADDQ	$4*SIZE, ptrba;
ADDQ	$4*SIZE, ptrbb;

.L223_loopE:
#### Handle ####
XOR_DY	yvec7, yvec7, yvec7;
#if  defined(RN) || defined(RT) || defined(CN) || defined(CT)
ADDSUB_DY	yvec15, yvec7, yvec15;
ADDSUB_DY	yvec13, yvec7, yvec13;
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
SUB_DY	yvec15, yvec7, yvec15;
SUB_DY	yvec13, yvec7, yvec13;
#elif defined(RR) || defined(RC) || defined(CR) || defined(CC)
VPERMILP_DY $0x05, yvec15, yvec15;
VPERMILP_DY $0x05, yvec13, yvec13;
ADDSUB_DY	yvec15, yvec7, yvec15;
ADDSUB_DY	yvec13, yvec7, yvec13;
VPERMILP_DY $0x05, yvec15, yvec15;
VPERMILP_DY $0x05, yvec13, yvec13;
#endif

#### Load Alpha ####
BROAD_DY MEMALPHA_R,yvec7;
BROAD_DY MEMALPHA_I,yvec6;
#### Multiply Alpha	####
VPERMILP_DY	$0x05, yvec15, yvec5;
MUL_DY	yvec7, yvec15, yvec15;
MUL_DY	yvec6, yvec5, yvec5;
ADD2_DY	yvec5, yvec15, yvec15;
VPERMILP_DY	$0x05, yvec13, yvec3;
MUL_DY	yvec7, yvec13, yvec13;
MUL_DY 	yvec6, yvec3, yvec3;
ADD2_DY	yvec3, yvec13, yvec13;
EXTRA_DY $1, yvec15, xvec7;
EXTRA_DY $1, yvec13, xvec5;
#### Write back ####
#ifndef	TRMMKERNEL
LDL_DX 0*SIZE(C0), xvec0, xvec0;
LDH_DX 1*SIZE(C0), xvec0, xvec0;
LDL_DX 2*SIZE(C1), xvec1, xvec1;
LDH_DX 3*SIZE(C1), xvec1, xvec1;
LDL_DX 0*SIZE(C1), xvec2, xvec2;
LDH_DX 1*SIZE(C1), xvec2, xvec2;
LDL_DX 2*SIZE(C0), xvec3, xvec3;
LDH_DX 3*SIZE(C0), xvec3, xvec3;
ADD_DX xvec0, xvec15, xvec15;
ADD_DX xvec1, xvec7, xvec7;
ADD_DX xvec2, xvec13, xvec13;
ADD_DX xvec3, xvec5, xvec5;
#endif
STL_DX	xvec15, 0*SIZE(C0);
STH_DX	xvec15, 1*SIZE(C0);
STL_DX	xvec7, 2*SIZE(C1);
STH_DX	xvec7, 3*SIZE(C1);
STL_DX	xvec13, 0*SIZE(C1);
STH_DX	xvec13, 1*SIZE(C1);
STL_DX	xvec5, 2*SIZE(C0);
STH_DX	xvec5, 3*SIZE(C0);
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

.L22_loopE:
TEST $1, bm;
JLE .L23_loopE;
ALIGN_5
.L23_bodyB:
#if !defined(TRMMKERNEL)||(defined(TRMMKERNEL)&&defined(LEFT)&&defined(TRANSA))||(defined(TRMMKERNEL)&&!defined(LEFT)&&!defined(TRANSA))
MOVQ bb,ptrbb;
#else
MOVQ bb, ptrbb;
MOVQ kk, %rax;
SALQ $ZBASE_SHIFT, %rax;
ADDQ %rax, ptrba;
LEAQ (ptrbb, %rax, 2), ptrbb;
#endif
XOR_DY	yvec15, yvec15, yvec15;
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
JLE .L231_loopE;
ALIGN_5
.L231_bodyB:
LD_DY	0*SIZE(ptrba), yvec0;	#### A1r A1i A2r A2i
EDUP_DY	0*SIZE(ptrbb), yvec2;

SHUF_DY	$0x20, yvec0, yvec0, yvec1;
MUL_DY	yvec1, yvec2, yvec6;
ADD1_DY	yvec6, yvec15, yvec15;

VPERMILP_DY	$0x05, yvec1, yvec4;
EDUP_DY	1*SIZE(ptrbb), yvec2;
MUL_DY	yvec4, yvec2, yvec6;
ADD2_DY	yvec6, yvec15, yvec15;

SHUF_DY	$0x31, yvec0, yvec0, yvec1;
EDUP_DY	4*SIZE(ptrbb), yvec2;

MUL_DY	yvec1, yvec2, yvec6;
ADD1_DY	yvec6, yvec15, yvec15;

VPERMILP_DY	$0x05, yvec1, yvec4;
EDUP_DY	5*SIZE(ptrbb), yvec2;
MUL_DY	yvec4, yvec2, yvec6;
ADD2_DY	yvec6, yvec15, yvec15;

LD_DY	4*SIZE(ptrba), yvec0;
EDUP_DY	8*SIZE(ptrbb), yvec2;

SHUF_DY	$0x20, yvec0, yvec0, yvec1;
MUL_DY	yvec1, yvec2, yvec6;
ADD1_DY	yvec6, yvec15, yvec15;

VPERMILP_DY	$0x05, yvec1, yvec4;
EDUP_DY	9*SIZE(ptrbb), yvec2;
MUL_DY	yvec4, yvec2, yvec6;
ADD2_DY	yvec6, yvec15, yvec15;

SHUF_DY	$0x31, yvec0, yvec0, yvec1;
EDUP_DY	12*SIZE(ptrbb), yvec2;
MUL_DY	yvec1, yvec2, yvec6;
ADD1_DY	yvec6, yvec15, yvec15;

VPERMILP_DY	$0x05, yvec1, yvec4;
EDUP_DY	13*SIZE(ptrbb), yvec2;
MUL_DY	yvec4, yvec2, yvec6;
ADD2_DY	yvec6, yvec15, yvec15;
ADDQ	$8*SIZE, ptrba;
ADDQ	$16*SIZE, ptrbb;
DECQ	k;
JG .L231_bodyB;
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
LD_DY	0*SIZE(ptrba), yvec0;	#### A1r A1i A2r A2i
EDUP_DY	0*SIZE(ptrbb), yvec2;

SHUF_DY	$0x20, yvec0, yvec0, yvec1;
MUL_DY	yvec1, yvec2, yvec6;
ADD1_DY	yvec6, yvec15, yvec15;

VPERMILP_DY	$0x05, yvec1, yvec4;
EDUP_DY	1*SIZE(ptrbb), yvec2;
MUL_DY	yvec4, yvec2, yvec6;
ADD2_DY	yvec6, yvec15, yvec15;

SHUF_DY	$0x31, yvec0, yvec0, yvec1;
EDUP_DY	4*SIZE(ptrbb), yvec2;

MUL_DY	yvec1, yvec2, yvec6;
ADD1_DY	yvec6, yvec15, yvec15;

VPERMILP_DY	$0x05, yvec1, yvec4;
EDUP_DY	5*SIZE(ptrbb), yvec2;
MUL_DY	yvec4, yvec2, yvec6;
ADD2_DY	yvec6, yvec15, yvec15;
ADDQ	$4*SIZE, ptrba;
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
LD_DY	0*SIZE(ptrba), yvec0;	#### A1r A1i A2r A2i
EDUP_DY	0*SIZE(ptrbb), yvec2;

SHUF_DY	$0x20, yvec0, yvec0, yvec1;
MUL_DY	yvec1, yvec2, yvec6;
ADD1_DY	yvec6, yvec15, yvec15;

VPERMILP_DY	$0x05, yvec1, yvec4;
EDUP_DY	1*SIZE(ptrbb), yvec2;
MUL_DY	yvec4, yvec2, yvec6;
ADD2_DY	yvec6, yvec15, yvec15;
ADDQ	$2*SIZE, ptrba;
ADDQ	$4*SIZE, ptrbb;

.L233_loopE:
#### Handle ####
XOR_DY	yvec7, yvec7, yvec7;
#if  defined(RN) || defined(RT) || defined(CN) || defined(CT)
ADDSUB_DY	yvec15, yvec7, yvec15;
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
SUB_DY	yvec15, yvec7, yvec15;
#elif defined(RR) || defined(RC) || defined(CR) || defined(CC)
VPERMILP_DY $0x05, yvec15, yvec15;
ADDSUB_DY	yvec15, yvec7, yvec15;
VPERMILP_DY $0x05, yvec15, yvec15;
#endif

#### Multiply Alpha	####
BROAD_DY MEMALPHA_R, yvec7;
BROAD_DY MEMALPHA_I, yvec6;
#### Writing Back ####
VPERMILP_DY	$0x05, yvec15, yvec5;
MUL_DY	yvec7, yvec15, yvec15;
MUL_DY	yvec6, yvec5, yvec5;
ADD2_DY	yvec5, yvec15, yvec15;
EXTRA_DY $1, yvec15, xvec7;
#### Writing Back ####
#ifndef	TRMMKERNEL
LDL_DX 0*SIZE(C0), xvec0, xvec0;
LDH_DX 1*SIZE(C0), xvec0, xvec0;
LDL_DX 0*SIZE(C1), xvec1, xvec1;
LDH_DX 1*SIZE(C1), xvec1, xvec1;
ADD_DX xvec0, xvec15, xvec15;
ADD_DX xvec1, xvec7, xvec7;
#endif
STL_DX	xvec15, 0*SIZE(C0);
STH_DX	xvec15, 1*SIZE(C0);
STL_DX	xvec7, 0*SIZE(C1);
STH_DX	xvec7, 1*SIZE(C1);
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
ADDQ	$2*SIZE, C0;
.L23_loopE:
#if defined(TRMMKERNEL) && !defined(LEFT)
ADDQ	$2, kk;
#endif
MOVQ bk, k;
SALQ $5, k;
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
MOVQ ba, ptrba;
MOVQ C, C0;
MOVQ bm, i;
SARQ $2, i;
JLE .L31_loopE;
ALIGN_5
.L31_bodyB:
#if !defined(TRMMKERNEL)||(defined(TRMMKERNEL)&&defined(LEFT)&&defined(TRANSA))||(defined(TRMMKERNEL)&&!defined(LEFT)&&!defined(TRANSA))
MOVQ bb,ptrbb;
#else
MOVQ bb, ptrbb;
MOVQ kk, %rax;
SALQ $ZBASE_SHIFT, %rax;
LEAQ (ptrba, %rax, 4), ptrba;
ADDQ %rax, ptrbb;
#endif
XOR_DY	yvec15, yvec15, yvec15;
XOR_DY	yvec14, yvec14, yvec14;
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
JLE .L311_loopE;
ALIGN_5
.L311_bodyB:
LD_DY	0*SIZE(ptrba), yvec0;
BROAD_DY 0*SIZE(ptrbb), yvec2;
MUL_DY	yvec0, yvec2, yvec6;
ADD1_DY	yvec6, yvec15, yvec15;

LD_DY	4*SIZE(ptrba), yvec1;
MUL_DY	yvec1, yvec2, yvec7;
ADD1_DY	yvec7, yvec14, yvec14;

VPERMILP_DY	$0x05, yvec0, yvec4;
BROAD_DY 1*SIZE(ptrbb), yvec3;
MUL_DY	yvec4, yvec3, yvec6;
ADD2_DY	yvec6, yvec15, yvec15;

VPERMILP_DY	$0x05, yvec1, yvec5;
MUL_DY	yvec5, yvec3, yvec7;
ADD2_DY	yvec7, yvec14, yvec14;

LD_DY	8*SIZE(ptrba), yvec0;
BROAD_DY 2*SIZE(ptrbb), yvec2;
MUL_DY	yvec0, yvec2, yvec6;
ADD1_DY	yvec6, yvec15, yvec15;

LD_DY	12*SIZE(ptrba), yvec1;
MUL_DY	yvec1, yvec2, yvec7;
ADD1_DY	yvec7, yvec14, yvec14;

VPERMILP_DY	$0x05, yvec0, yvec4;
BROAD_DY 3*SIZE(ptrbb), yvec3;
MUL_DY	yvec4, yvec3, yvec6;
ADD2_DY	yvec6, yvec15, yvec15;

VPERMILP_DY	$0x05, yvec1, yvec5;
MUL_DY	yvec5, yvec3, yvec7;
ADD2_DY	yvec7, yvec14, yvec14;

LD_DY	16*SIZE(ptrba), yvec0;
BROAD_DY 4*SIZE(ptrbb), yvec2;
MUL_DY	yvec0, yvec2, yvec6;
ADD1_DY	yvec6, yvec15, yvec15;

LD_DY	20*SIZE(ptrba), yvec1;
MUL_DY	yvec1, yvec2, yvec7;
ADD1_DY	yvec7, yvec14, yvec14;

VPERMILP_DY	$0x05, yvec0, yvec4;
BROAD_DY	5*SIZE(ptrbb), yvec3;
MUL_DY	yvec4, yvec3, yvec6;
ADD2_DY	yvec6, yvec15, yvec15;

VPERMILP_DY	$0x05, yvec1, yvec5;
MUL_DY	yvec5, yvec3, yvec7;
ADD2_DY	yvec7, yvec14, yvec14;

LD_DY	24*SIZE(ptrba), yvec0;
BROAD_DY 6*SIZE(ptrbb), yvec2;
MUL_DY	yvec0, yvec2, yvec6;
ADD1_DY	yvec6, yvec15, yvec15;

LD_DY	28*SIZE(ptrba), yvec1;
MUL_DY	yvec1, yvec2, yvec7;
ADD1_DY	yvec7, yvec14, yvec14;

VPERMILP_DY	$0x05, yvec0, yvec4;
BROAD_DY 7*SIZE(ptrbb), yvec3;
MUL_DY	yvec4, yvec3, yvec6;
ADD2_DY	yvec6, yvec15, yvec15;

VPERMILP_DY	$0x05, yvec1, yvec5;
MUL_DY	yvec5, yvec3, yvec7;
ADD2_DY	yvec7, yvec14, yvec14;
ADDQ	$32*SIZE, ptrba;
ADDQ	$8*SIZE, ptrbb;
DECQ	k;
JG .L311_bodyB;
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
LD_DY	0*SIZE(ptrba), yvec0;
BROAD_DY 0*SIZE(ptrbb), yvec2;
MUL_DY	yvec0, yvec2, yvec6;
ADD1_DY	yvec6, yvec15, yvec15;

LD_DY	4*SIZE(ptrba), yvec1;
MUL_DY	yvec1, yvec2, yvec7;
ADD1_DY	yvec7, yvec14, yvec14;

VPERMILP_DY	$0x05, yvec0, yvec4;
BROAD_DY 1*SIZE(ptrbb), yvec3;
MUL_DY	yvec4, yvec3, yvec6;
ADD2_DY	yvec6, yvec15, yvec15;

VPERMILP_DY	$0x05, yvec1, yvec5;
MUL_DY	yvec5, yvec3, yvec7;
ADD2_DY	yvec7, yvec14, yvec14;

LD_DY	8*SIZE(ptrba), yvec0;
BROAD_DY 2*SIZE(ptrbb), yvec2;
MUL_DY	yvec0, yvec2, yvec6;
ADD1_DY	yvec6, yvec15, yvec15;

LD_DY	12*SIZE(ptrba), yvec1;
MUL_DY	yvec1, yvec2, yvec7;
ADD1_DY	yvec7, yvec14, yvec14;

VPERMILP_DY	$0x05, yvec0, yvec4;
BROAD_DY 3*SIZE(ptrbb), yvec3;
MUL_DY	yvec4, yvec3, yvec6;
ADD2_DY	yvec6, yvec15, yvec15;

VPERMILP_DY	$0x05, yvec1, yvec5;
MUL_DY	yvec5, yvec3, yvec7;
ADD2_DY	yvec7, yvec14, yvec14;
ADDQ	$16*SIZE, ptrba;
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
LD_DY	0*SIZE(ptrba), yvec0;
BROAD_DY 0*SIZE(ptrbb), yvec2;
MUL_DY	yvec0, yvec2, yvec6;
ADD1_DY	yvec6, yvec15, yvec15;

LD_DY	4*SIZE(ptrba), yvec1;
MUL_DY	yvec1, yvec2, yvec7;
ADD1_DY	yvec7, yvec14, yvec14;

VPERMILP_DY	$0x05, yvec0, yvec4;
BROAD_DY 1*SIZE(ptrbb), yvec3;
MUL_DY	yvec4, yvec3, yvec6;
ADD2_DY	yvec6, yvec15, yvec15;

VPERMILP_DY	$0x05, yvec1, yvec5;
MUL_DY	yvec5, yvec3, yvec7;
ADD2_DY	yvec7, yvec14, yvec14;
ADDQ	$8*SIZE, ptrba;
ADDQ	$2*SIZE, ptrbb;

.L313_loopE:
#### Handle ####
XOR_DY	yvec7, yvec7, yvec7;
#if  defined(RN) || defined(RT) || defined(CN) || defined(CT)
ADDSUB_DY	yvec15, yvec7, yvec15;
ADDSUB_DY	yvec14, yvec7, yvec14;
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
SUB_DY	yvec15, yvec7, yvec15;
SUB_DY	yvec14, yvec7, yvec14;
#elif defined(RR) || defined(RC) || defined(CR) || defined(CC)
VPERMILP_DY $0x05, yvec15, yvec15;
VPERMILP_DY $0x05, yvec14, yvec14;
ADDSUB_DY	yvec15, yvec7, yvec15;
ADDSUB_DY	yvec14, yvec7, yvec14;
VPERMILP_DY $0x05, yvec15, yvec15;
VPERMILP_DY $0x05, yvec14, yvec14;
#endif

#### Load Alpha ####
BROAD_DY MEMALPHA_R,yvec7;
BROAD_DY MEMALPHA_I,yvec6;
#### Multiply Alpha	####
VPERMILP_DY	$0x05, yvec15, yvec5;
MUL_DY	yvec7, yvec15, yvec15;
MUL_DY	yvec6, yvec5, yvec5;
ADD2_DY	yvec5, yvec15, yvec15;
VPERMILP_DY	$0x05, yvec14, yvec4;
MUL_DY 	yvec7, yvec14, yvec14;
MUL_DY 	yvec6, yvec4, yvec4;
ADD2_DY	yvec4, yvec14, yvec14;
EXTRA_DY $1, yvec15, xvec7;
EXTRA_DY $1, yvec14, xvec6;
#### Writing Back ####
#ifndef	TRMMKERNEL
LDL_DX 0*SIZE(C0), xvec0, xvec0;
LDH_DX 1*SIZE(C0), xvec0, xvec0;
LDL_DX 2*SIZE(C0), xvec1, xvec1;
LDH_DX 3*SIZE(C0), xvec1, xvec1;
LDL_DX 4*SIZE(C0), xvec2, xvec2;
LDH_DX 5*SIZE(C0), xvec2, xvec2;
LDL_DX 6*SIZE(C0), xvec3, xvec3;
LDH_DX 7*SIZE(C0), xvec3, xvec3;
ADD_DX xvec0, xvec15, xvec15;
ADD_DX xvec1, xvec7, xvec7;
ADD_DX xvec2, xvec14, xvec14;
ADD_DX xvec3, xvec6, xvec6;
#endif
STL_DX	xvec15, 0*SIZE(C0);
STH_DX	xvec15, 1*SIZE(C0);
STL_DX	xvec7, 2*SIZE(C0);
STH_DX	xvec7, 3*SIZE(C0);
STL_DX	xvec14, 4*SIZE(C0);
STH_DX	xvec14, 5*SIZE(C0);
STL_DX	xvec6, 6*SIZE(C0);
STH_DX	xvec6, 7*SIZE(C0);
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
DECQ	i;
JG .L31_bodyB;
ALIGN_5
.L31_loopE:
TEST $2, bm;
JLE .L32_loopE;
ALIGN_5
.L32_bodyB:
#if !defined(TRMMKERNEL)||(defined(TRMMKERNEL)&&defined(LEFT)&&defined(TRANSA))||(defined(TRMMKERNEL)&&!defined(LEFT)&&!defined(TRANSA))
MOVQ bb,ptrbb;
#else
MOVQ bb, ptrbb;
MOVQ kk, %rax;
SALQ $ZBASE_SHIFT, %rax;
LEAQ (ptrba, %rax, 2), ptrba;
ADDQ %rax, ptrbb;
#endif
XOR_DY yvec15, yvec15, yvec15;
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
JLE .L321_loopE;
ALIGN_5
.L321_bodyB:
LD_DY	0*SIZE(ptrba), yvec0;
BROAD_DY 0*SIZE(ptrbb), yvec2;
MUL_DY	yvec0, yvec2, yvec6;
ADD1_DY	yvec6, yvec15, yvec15;
VPERMILP_DY	$0x05, yvec0, yvec1;
BROAD_DY 1*SIZE(ptrbb), yvec3;
MUL_DY	yvec1, yvec3, yvec7;
ADD2_DY	yvec7, yvec15, yvec15;

LD_DY	4*SIZE(ptrba), yvec0;
BROAD_DY 2*SIZE(ptrbb), yvec2;
MUL_DY	yvec0, yvec2, yvec6;
ADD1_DY	yvec6, yvec15, yvec15;
VPERMILP_DY	$0x05, yvec0, yvec1;
BROAD_DY 3*SIZE(ptrbb), yvec3;
MUL_DY	yvec1, yvec3, yvec7;
ADD2_DY	yvec7, yvec15, yvec15;

LD_DY	8*SIZE(ptrba), yvec0;
BROAD_DY 4*SIZE(ptrbb), yvec2;
MUL_DY	yvec0, yvec2, yvec6;
ADD1_DY	yvec6, yvec15, yvec15;
VPERMILP_DY	$0x05, yvec0, yvec1;
BROAD_DY 5*SIZE(ptrbb), yvec3;
MUL_DY	yvec1, yvec3, yvec7;
ADD2_DY	yvec7, yvec15, yvec15;

LD_DY	12*SIZE(ptrba), yvec0;
BROAD_DY 6*SIZE(ptrbb), yvec2;
MUL_DY	yvec0, yvec2, yvec6;
ADD1_DY	yvec6, yvec15, yvec15;
VPERMILP_DY	$0x05, yvec0, yvec1;
BROAD_DY 7*SIZE(ptrbb), yvec3;
MUL_DY	yvec1, yvec3, yvec7;
ADD2_DY	yvec7, yvec15, yvec15;
ADDQ	$16*SIZE, ptrba;
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
LD_DY	0*SIZE(ptrba), yvec0;
BROAD_DY 0*SIZE(ptrbb), yvec2;
MUL_DY	yvec0, yvec2, yvec6;
ADD1_DY	yvec6, yvec15, yvec15;
VPERMILP_DY	$0x05, yvec0, yvec1;
BROAD_DY 1*SIZE(ptrbb), yvec3;
MUL_DY	yvec1, yvec3, yvec7;
ADD2_DY	yvec7, yvec15, yvec15;

LD_DY	4*SIZE(ptrba), yvec0;
BROAD_DY 2*SIZE(ptrbb), yvec2;
MUL_DY	yvec0, yvec2, yvec6;
ADD1_DY	yvec6, yvec15, yvec15;
VPERMILP_DY	$0x05, yvec0, yvec1;
BROAD_DY 3*SIZE(ptrbb), yvec3;
MUL_DY	yvec1, yvec3, yvec7;
ADD2_DY	yvec7, yvec15, yvec15;
ADDQ	$8*SIZE, ptrba;
ADDQ	$4*SIZE, ptrbb;

.L322_loopE:
#ifndef	TRMMKERNEL
TEST $1, bk;
#else
TEST $1, kkk;
#endif
JLE .L323_loopE;
ALIGN_5
.L323_bodyB:
LD_DY	0*SIZE(ptrba), yvec0;
BROAD_DY 0*SIZE(ptrbb), yvec2;
MUL_DY	yvec0, yvec2, yvec6;
ADD1_DY	yvec6, yvec15, yvec15;
VPERMILP_DY	$0x05, yvec0, yvec1;
BROAD_DY 1*SIZE(ptrbb), yvec3;
MUL_DY	yvec1, yvec3, yvec7;
ADD2_DY	yvec7, yvec15, yvec15;
ADDQ	$4*SIZE, ptrba;
ADDQ	$2*SIZE, ptrbb;
.L323_loopE:
#### Handle ####
XOR_DY	yvec7, yvec7, yvec7;
#if  defined(RN) || defined(RT) || defined(CN) || defined(CT)
ADDSUB_DY	yvec15, yvec7, yvec15;
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
SUB_DY	yvec15, yvec7, yvec15;
#elif defined(RR) || defined(RC) || defined(CR) || defined(CC)
VPERMILP_DY $0x05, yvec15, yvec15;
ADDSUB_DY	yvec15, yvec7, yvec15;
VPERMILP_DY $0x05, yvec15, yvec15;
#endif

#### Load Alpha ####
BROAD_DY MEMALPHA_R,yvec7;
BROAD_DY MEMALPHA_I,yvec6;
#### Multiply Alpha	####
VPERMILP_DY	$0x05, yvec15, yvec5;
MUL_DY	yvec7, yvec15, yvec15;
MUL_DY	yvec6, yvec5, yvec5;
ADD2_DY	yvec5, yvec15, yvec15;
EXTRA_DY $1, yvec15, xvec7;
#### Writing Back ####
#ifndef	TRMMKERNEL
LDL_DX 0*SIZE(C0), xvec0, xvec0;
LDH_DX 1*SIZE(C0), xvec0, xvec0;
LDL_DX 2*SIZE(C0), xvec1, xvec1;
LDH_DX 3*SIZE(C0), xvec1, xvec1;
ADD_DX xvec0, xvec15, xvec15;
ADD_DX xvec1, xvec7, xvec7;
#endif
STL_DX	xvec15, 0*SIZE(C0);
STH_DX	xvec15, 1*SIZE(C0);
STL_DX	xvec7, 2*SIZE(C0);
STH_DX	xvec7, 3*SIZE(C0);
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
.L32_loopE:
TEST $1, bm;
JLE .L33_loopE;
ALIGN_5
.L33_bodyB:
#if !defined(TRMMKERNEL)||(defined(TRMMKERNEL)&&defined(LEFT)&&defined(TRANSA))||(defined(TRMMKERNEL)&&!defined(LEFT)&&!defined(TRANSA))
MOVQ bb,ptrbb;
#else
MOVQ bb, ptrbb;
MOVQ kk, %rax;
SALQ $ZBASE_SHIFT, %rax;
ADDQ %rax, ptrba;
ADDQ %rax, ptrbb;
#endif
XOR_DY yvec15, yvec15, yvec15;
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
JLE .L331_loopE;
ALIGN_5
.L331_bodyB:
LD_DX	0*SIZE(ptrba), xvec0;
BROAD_DX 0*SIZE(ptrbb), xvec2;
MUL_DX xvec0, xvec2, xvec2;
ADD1_DX xvec2, xvec15, xvec15;

SHUF_DX	$0x4e, xvec0, xvec1;
BROAD_DX 1*SIZE(ptrbb), xvec3;
MUL_DX xvec1, xvec3, xvec3;
ADDSUB_DX xvec3, xvec15, xvec15;

LD_DX	2*SIZE(ptrba), xvec0;
BROAD_DX 2*SIZE(ptrbb), xvec2;
MUL_DX xvec0, xvec2, xvec2;
ADD1_DX xvec2, xvec15, xvec15;

SHUF_DX	$0x4e, xvec0, xvec1;
BROAD_DX 3*SIZE(ptrbb), xvec3;
MUL_DX xvec1, xvec3, xvec3;
ADDSUB_DX xvec3, xvec15, xvec15;

LD_DX	4*SIZE(ptrba), xvec0;
BROAD_DX 4*SIZE(ptrbb), xvec2;
MUL_DX xvec0, xvec2, xvec2;
ADD1_DX xvec2, xvec15, xvec15;

SHUF_DX	$0x4e, xvec0, xvec1;
BROAD_DX 5*SIZE(ptrbb), xvec3;
MUL_DX xvec1, xvec3, xvec3;
ADDSUB_DX xvec3, xvec15, xvec15;

LD_DX	6*SIZE(ptrba), xvec0;
BROAD_DX 6*SIZE(ptrbb), xvec2;
MUL_DX xvec0, xvec2, xvec2;
ADD1_DX xvec2, xvec15, xvec15;

SHUF_DX	$0x4e, xvec0, xvec1;
BROAD_DX 7*SIZE(ptrbb), xvec3;
MUL_DX xvec1, xvec3, xvec3;
ADDSUB_DX xvec3, xvec15, xvec15;
ADDQ	$8*SIZE, ptrba;
ADDQ	$8*SIZE, ptrbb;
DECQ	k;
JG .L331_bodyB;
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
LD_DX	0*SIZE(ptrba), xvec0;
BROAD_DX 0*SIZE(ptrbb), xvec2;
MUL_DX xvec0, xvec2, xvec2;
ADD1_DX xvec2, xvec15, xvec15;

SHUF_DX	$0x4e, xvec0, xvec1;
BROAD_DX 1*SIZE(ptrbb), xvec3;
MUL_DX xvec1, xvec3, xvec3;
ADDSUB_DX xvec3, xvec15, xvec15;

LD_DX	2*SIZE(ptrba), xvec0;
BROAD_DX 2*SIZE(ptrbb), xvec2;
MUL_DX xvec0, xvec2, xvec2;
ADD1_DX xvec2, xvec15, xvec15;

SHUF_DX	$0x4e, xvec0, xvec1;
BROAD_DX 3*SIZE(ptrbb), xvec3;
MUL_DX xvec1, xvec3, xvec3;
ADDSUB_DX xvec3, xvec15, xvec15;
ADDQ	$4*SIZE, ptrba;
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
LD_DX	0*SIZE(ptrba), xvec0;
BROAD_DX 0*SIZE(ptrbb), xvec2;
MUL_DX xvec0, xvec2, xvec2;
ADD1_DX xvec2, xvec15, xvec15;

SHUF_DX	$0x4e, xvec0, xvec1;
BROAD_DX 1*SIZE(ptrbb), xvec3;
MUL_DX xvec1, xvec3, xvec3;
ADDSUB_DX xvec3, xvec15, xvec15;
ADDQ	$2*SIZE, ptrba;
ADDQ	$2*SIZE, ptrbb;

.L333_loopE:
#### Handle ####
XOR_DY	yvec7, yvec7, yvec7;
#if  defined(RN) || defined(RT) || defined(CN) || defined(CT)
ADDSUB_DX xvec15, xvec7, xvec7;
MOV_DX	xvec7, xvec15;
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
SUB_DX xvec15, xvec7, xvec7;
MOV_DX	xvec7, xvec15;
#elif defined(RR) || defined(RC) || defined(CR) || defined(CC)
SHUF_DX $0x4e, xvec15, xvec15;
ADDSUB_DX xvec15, xvec7, xvec7;
MOV_DX	xvec7, xvec15;
SHUF_DX $0x4e, xvec15, xvec15;
#endif

#### Load Alpha ####
BROAD_DX MEMALPHA_R,xvec7;
BROAD_DX MEMALPHA_I,xvec6;
#### Multiply Alpha	####
SHUF_DX	$0x4e, xvec15, xvec5;
MUL_DX xvec7, xvec15, xvec15;
MUL_DX xvec6, xvec5, xvec5;
ADDSUB_DX xvec5, xvec15, xvec15;
#### Writing back ####
#ifndef	TRMMKERNEL
LDL_DX 0*SIZE(C0), xvec0, xvec0;
LDH_DX 1*SIZE(C0), xvec0, xvec0;
ADD_DX xvec0, xvec15, xvec15;
#endif
STL_DX	xvec15, 0*SIZE(C0);
STH_DX	xvec15, 1*SIZE(C0);
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
.L33_loopE:
#if defined(TRMMKERNEL) && !defined(LEFT)
ADDQ	$1, kk;
#endif
MOVQ bk, k;
SALQ $4*SIZE, k;
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
