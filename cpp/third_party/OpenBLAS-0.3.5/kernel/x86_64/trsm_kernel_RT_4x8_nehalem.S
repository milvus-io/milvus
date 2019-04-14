/*********************************************************************/
/* Copyright 2009, 2010 The University of Texas at Austin.           */
/* All rights reserved.                                              */
/*                                                                   */
/* Redistribution and use in source and binary forms, with or        */
/* without modification, are permitted provided that the following   */
/* conditions are met:                                               */
/*                                                                   */
/*   1. Redistributions of source code must retain the above         */
/*      copyright notice, this list of conditions and the following  */
/*      disclaimer.                                                  */
/*                                                                   */
/*   2. Redistributions in binary form must reproduce the above      */
/*      copyright notice, this list of conditions and the following  */
/*      disclaimer in the documentation and/or other materials       */
/*      provided with the distribution.                              */
/*                                                                   */
/*    THIS  SOFTWARE IS PROVIDED  BY THE  UNIVERSITY OF  TEXAS AT    */
/*    AUSTIN  ``AS IS''  AND ANY  EXPRESS OR  IMPLIED WARRANTIES,    */
/*    INCLUDING, BUT  NOT LIMITED  TO, THE IMPLIED  WARRANTIES OF    */
/*    MERCHANTABILITY  AND FITNESS FOR  A PARTICULAR  PURPOSE ARE    */
/*    DISCLAIMED.  IN  NO EVENT SHALL THE UNIVERSITY  OF TEXAS AT    */
/*    AUSTIN OR CONTRIBUTORS BE  LIABLE FOR ANY DIRECT, INDIRECT,    */
/*    INCIDENTAL,  SPECIAL, EXEMPLARY,  OR  CONSEQUENTIAL DAMAGES    */
/*    (INCLUDING, BUT  NOT LIMITED TO,  PROCUREMENT OF SUBSTITUTE    */
/*    GOODS  OR  SERVICES; LOSS  OF  USE,  DATA,  OR PROFITS;  OR    */
/*    BUSINESS INTERRUPTION) HOWEVER CAUSED  AND ON ANY THEORY OF    */
/*    LIABILITY, WHETHER  IN CONTRACT, STRICT  LIABILITY, OR TORT    */
/*    (INCLUDING NEGLIGENCE OR OTHERWISE)  ARISING IN ANY WAY OUT    */
/*    OF  THE  USE OF  THIS  SOFTWARE,  EVEN  IF ADVISED  OF  THE    */
/*    POSSIBILITY OF SUCH DAMAGE.                                    */
/*                                                                   */
/* The views and conclusions contained in the software and           */
/* documentation are those of the authors and should not be          */
/* interpreted as representing official policies, either expressed   */
/* or implied, of The University of Texas at Austin.                 */
/*********************************************************************/

#define ASSEMBLER
#include "common.h"

#define OLD_M	%rdi
#define OLD_N	%rsi
#define OLD_K	%rdx

#define M	%r13
#define N	%r14
#define K	%r15

#define A	%rcx
#define B	%r8
#define C	%r9
#define LDC	%r10

#define I	%r11
#define AO	%rdi
#define BO	%rsi
#define	CO1	%rbx
#define CO2	%rbp
#define KK	%rdx
#define BB	%r12

#ifndef WINDOWS_ABI

#define STACKSIZE 64

#define OLD_LDC		 8 + STACKSIZE(%rsp)
#define OLD_OFFSET	16 + STACKSIZE(%rsp)

#define OFFSET	   48(%rsp)
#define J	   56(%rsp)
#define KKK	   64(%rsp)
#define AORIG	   72(%rsp)

#else

#define STACKSIZE 256

#define OLD_A		40 + STACKSIZE(%rsp)
#define OLD_B		48 + STACKSIZE(%rsp)
#define OLD_C		56 + STACKSIZE(%rsp)
#define OLD_LDC		64 + STACKSIZE(%rsp)
#define OLD_OFFSET	72 + STACKSIZE(%rsp)

#define OFFSET	  224(%rsp)
#define J	  232(%rsp)
#define KKK	  240(%rsp)
#define AORIG	  248(%rsp)

#endif

#define PREFETCHSIZE  (16 * 1 + 4)
#define PREFETCH     prefetcht0

	PROLOGUE
	PROFCODE

	subq	$STACKSIZE, %rsp

	movq	%rbx,  0(%rsp)
	movq	%rbp,  8(%rsp)
	movq	%r12, 16(%rsp)
	movq	%r13, 24(%rsp)
	movq	%r14, 32(%rsp)
	movq	%r15, 40(%rsp)

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

	movq	ARG1,      OLD_M
	movq	ARG2,      OLD_N
	movq	ARG3,      OLD_K
	movq	OLD_A,     A
	movq	OLD_B,     B
	movq	OLD_C,     C
#endif

	subq	$-32 * SIZE, A
	subq	$-32 * SIZE, B

	movq	OLD_M, M
	movq	OLD_N, N
	movq	OLD_K, K

	movq	OLD_LDC,   LDC
	movq	OLD_OFFSET, KK

	leaq	(, LDC, SIZE), LDC

	movq	KK, OFFSET
	negq	KK

#ifdef LN
       leaq	(, M, SIZE), %rax
       addq	%rax, C
       imulq	K, %rax
       addq	%rax, A
#endif

#ifdef RT
       leaq	(, N, SIZE), %rax
       imulq	K, %rax
       addq	%rax, B
       movq	N, %rax
       imulq	LDC, %rax
       addq	%rax, C
#endif

#ifdef RT
       movq	N, %rax
       subq	OFFSET, %rax
       movq	%rax, KK
#endif

	testq	$1, N
	jle	.L40

#if defined(LT) || defined(RN)
	movq	A, AO
#else
	movq	A, AORIG
#endif

#ifdef RT
	movq	K, %rax
	salq	$BASE_SHIFT, %rax
	subq	%rax, B

	subq	LDC, C
#endif

	movq	C, CO1
#ifndef RT
	addq	LDC, C
#endif

#ifdef LN
	movq	OFFSET, %rax
	addq	M, %rax
	movq	%rax, KK
#endif

#ifdef LT
	movq	OFFSET, %rax
	movq	%rax, KK
#endif

	movq	M,  I
	sarq	$2, I
	NOBRANCH
	jle	.L110
	ALIGN_4

.L101:
#ifdef LN
       movq	K, %rax
       salq	$2 + BASE_SHIFT, %rax
       subq	%rax, AORIG
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	movq	AORIG, AO
	leaq	(AO, %rax, 4), AO
	leaq	(B,  %rax, 1), BO
#else
	movq	B, BO
#endif

	xorps	%xmm1, %xmm1
	movaps	-32 * SIZE(AO), %xmm0
	movsd	-32 * SIZE(BO), %xmm3
	xorps	%xmm8,  %xmm8
	prefetcht2     4 * SIZE(CO1)

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	sarq	$2, %rax
	NOBRANCH
	jle	.L105
	ALIGN_3

.L102:
	PREFETCH	(PREFETCHSIZE +  0) * SIZE(AO)

	addps	%xmm1, %xmm8
	pshufd	$0x00,  %xmm3, %xmm1
	movss	-31 * SIZE(BO), %xmm3
	mulps	%xmm0, %xmm1
	movaps	-28 * SIZE(AO), %xmm0

	addps	%xmm1, %xmm8
	pshufd	$0x00,  %xmm3, %xmm1
	movss	-30 * SIZE(BO), %xmm3
	mulps	%xmm0, %xmm1
	movaps	-24 * SIZE(AO), %xmm0

	addps	%xmm1, %xmm8
	pshufd	$0x00,  %xmm3, %xmm1
	movss	-29 * SIZE(BO), %xmm3
	mulps	%xmm0, %xmm1
	movaps	-20 * SIZE(AO), %xmm0

	addps	%xmm1, %xmm8
	pshufd	$0x00,  %xmm3, %xmm1
	movss	-28 * SIZE(BO), %xmm3
	mulps	%xmm0, %xmm1
	movaps	-16 * SIZE(AO), %xmm0

	subq	$-16 * SIZE, AO
	subq	$ -4 * SIZE, BO
	subq	$1, %rax
	BRANCH
	jg	.L102
	ALIGN_3

.L105:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	BRANCH
	je	.L108
	ALIGN_3

.L106:
	addps	%xmm1, %xmm8
	pshufd	$0x00,  %xmm3, %xmm1
	movss	-31 * SIZE(BO), %xmm3
	mulps	%xmm0, %xmm1
	movaps	-28 * SIZE(AO), %xmm0

	addq	$4 * SIZE, AO
	addq	$1 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L106
	ALIGN_3

.L108:
#if defined(LN) || defined(RT)
	movq	KK, %rax
#ifdef LN
	subq	$4, %rax
#else
	subq	$1, %rax
#endif

	leaq	(, %rax, SIZE), %rax

	movq	AORIG, AO
	leaq	(AO, %rax, 4), AO
	leaq	(B,  %rax, 1), BO
#endif

	addps	%xmm1, %xmm8

#if defined(LN) || defined(LT)
	movsd	-32 * SIZE(BO), %xmm0
	movhps	-30 * SIZE(BO), %xmm0

	subps	%xmm8,  %xmm0

	pshufd	$0xff, %xmm0, %xmm3
	pshufd	$0xaa, %xmm0, %xmm2
	pshufd	$0x55, %xmm0, %xmm1
#else
	movaps	-32 * SIZE(AO), %xmm0

	subps	%xmm8,  %xmm0
#endif

#ifdef LN
	movaps	-20 * SIZE(AO), %xmm8

	pshufd	 $0xff, %xmm8, %xmm15
	mulss	 %xmm15, %xmm3
	pshufd	 $0xaa, %xmm8, %xmm15
	mulss	 %xmm3,  %xmm15
	subss	 %xmm15, %xmm2
	pshufd	 $0x55, %xmm8, %xmm15
	mulss	 %xmm3,  %xmm15
	subss	 %xmm15, %xmm1
	pshufd	 $0x00, %xmm8, %xmm15
	mulss	 %xmm3,  %xmm15
	subss	 %xmm15, %xmm0

	movaps	-24 * SIZE(AO), %xmm8

	pshufd	 $0xaa, %xmm8, %xmm15
	mulss	 %xmm15, %xmm2
	pshufd	 $0x55, %xmm8, %xmm15
	mulss	 %xmm2,  %xmm15
	subss	 %xmm15, %xmm1
	pshufd	 $0x00, %xmm8, %xmm15
	mulss	 %xmm2,  %xmm15
	subss	 %xmm15, %xmm0

	movaps	-28 * SIZE(AO), %xmm8

	pshufd	 $0x55, %xmm8, %xmm15
	mulss	 %xmm15, %xmm1
	pshufd	 $0x00, %xmm8, %xmm15
	mulss	 %xmm1,  %xmm15
	subss	 %xmm15, %xmm0

	movaps	-32 * SIZE(AO), %xmm8

	pshufd	 $0x00, %xmm8, %xmm15
	mulss	 %xmm15, %xmm0
#endif

#ifdef LT
	movaps	-32 * SIZE(AO), %xmm8

	pshufd	 $0x00, %xmm8, %xmm15
	mulss	 %xmm15, %xmm0
	pshufd	 $0x55, %xmm8, %xmm15
	mulss	 %xmm0,  %xmm15
	subss	 %xmm15, %xmm1
	pshufd	 $0xaa, %xmm8, %xmm15
	mulss	 %xmm0,  %xmm15
	subss	 %xmm15, %xmm2
	pshufd	 $0xff, %xmm8, %xmm15
	mulss	 %xmm0,  %xmm15
	subss	 %xmm15, %xmm3

	movaps	-28 * SIZE(AO), %xmm8

	pshufd	 $0x55, %xmm8, %xmm15
	mulss	 %xmm15, %xmm1
	pshufd	 $0xaa, %xmm8, %xmm15
	mulss	 %xmm1,  %xmm15
	subss	 %xmm15, %xmm2
	pshufd	 $0xff, %xmm8, %xmm15
	mulss	 %xmm1,  %xmm15
	subss	 %xmm15, %xmm3

	movaps	-24 * SIZE(AO), %xmm8

	pshufd	 $0xaa, %xmm8, %xmm15
	mulss	 %xmm15, %xmm2
	pshufd	 $0xff, %xmm8, %xmm15
	mulss	 %xmm2,  %xmm15
	subss	 %xmm15, %xmm3

	movaps	-20 * SIZE(AO), %xmm8

	pshufd	 $0xff, %xmm8, %xmm15
	mulss	 %xmm15, %xmm3
#endif

#if defined(RN) || defined(RT)
	movaps	-32 * SIZE(BO), %xmm8

	pshufd	 $0x00, %xmm8, %xmm15
	mulps	 %xmm15, %xmm0
#endif

#ifdef LN
	subq	$4 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	unpcklps %xmm1, %xmm0
	unpcklps %xmm3, %xmm2

	movlps	%xmm0, -32 * SIZE(BO)
	movlps	%xmm2, -30 * SIZE(BO)

	movlps	%xmm0,  0 * SIZE(CO1)
	movlps	%xmm2,  2 * SIZE(CO1)
#else
	movaps	%xmm0, -32 * SIZE(AO)

	movlps	%xmm0,  0 * SIZE(CO1)
	movhps	%xmm0,  2 * SIZE(CO1)
#endif

#ifndef LN
	addq	$4 * SIZE, CO1
#endif

#if defined(LT) || defined(RN)
	movq	K,  %rax
	subq	KK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 1), BO
#endif

#ifdef LN
	subq	$4, KK
#endif

#ifdef LT
	addq	$4, KK
#endif

#ifdef RT
       movq	K, %rax
       salq	$2 + BASE_SHIFT, %rax
       addq	%rax, AORIG
#endif

	decq	I
	BRANCH
	jg	.L101
	ALIGN_4

.L110:
	testq	$2, M
	BRANCH
	jle	.L120

#ifdef LN
       movq	K, %rax
       salq	$1 + BASE_SHIFT, %rax
       subq	%rax, AORIG
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	movq	AORIG, AO
	leaq	(AO, %rax, 2), AO
	leaq	(B,  %rax, 1), BO
#else
	movq	B, BO
#endif

	xorps	%xmm1, %xmm1
	movddup	-32 * SIZE(AO), %xmm0
	xorps	%xmm8,  %xmm8

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	sarq	$2, %rax
	NOBRANCH
	jle	.L115
	ALIGN_3

.L112:
	addps	%xmm1, %xmm8
	movss	-32 * SIZE(BO), %xmm1
	unpcklps %xmm1, %xmm1
	mulps	%xmm0, %xmm1
	movsd	-30 * SIZE(AO), %xmm0

	addps	%xmm1, %xmm8
	movss	-31 * SIZE(BO), %xmm1
	unpcklps %xmm1, %xmm1
	mulps	%xmm0, %xmm1
	movsd	-28 * SIZE(AO), %xmm0

	addps	%xmm1, %xmm8
	movss	-30 * SIZE(BO), %xmm1
	unpcklps %xmm1, %xmm1
	mulps	%xmm0, %xmm1
	movsd	-26 * SIZE(AO), %xmm0

	addps	%xmm1, %xmm8
	movss	-29 * SIZE(BO), %xmm1
	unpcklps %xmm1, %xmm1
	mulps	%xmm0, %xmm1
	movsd	-24 * SIZE(AO), %xmm0

	subq	$-4 * SIZE, BO
	subq	$-8 * SIZE, AO

	subq	$1, %rax
	BRANCH
	jg	.L112
	ALIGN_3

.L115:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	BRANCH
	je	.L118
	ALIGN_3

.L116:
	addps	%xmm1, %xmm8
	movss	-32 * SIZE(BO), %xmm1
	unpcklps %xmm1, %xmm1
	mulps	%xmm0, %xmm1
	movsd	-30 * SIZE(AO), %xmm0

	addq	$2 * SIZE, AO
	addq	$1 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L116
	ALIGN_3

.L118:
#if defined(LN) || defined(RT)
	movq	KK, %rax
#ifdef LN
	subq	$2, %rax
#else
	subq	$1, %rax
#endif

	leaq	(, %rax, SIZE), %rax

	movq	AORIG, AO
	leaq	(AO, %rax, 2), AO
	leaq	(B,  %rax, 1), BO
#endif

	addps	%xmm1, %xmm8

#if defined(LN) || defined(LT)
	movsd	-32 * SIZE(BO), %xmm0

	subps	%xmm8,  %xmm0

	pshufd	$0x55, %xmm0, %xmm1
#else
	movsd	-32 * SIZE(AO), %xmm0

	subps	%xmm8,  %xmm0
#endif

#ifdef LN
	movaps	-32 * SIZE(AO), %xmm8

	pshufd	 $0xff, %xmm8, %xmm15
	mulss	 %xmm15, %xmm1
	pshufd	 $0xaa, %xmm8, %xmm15
	mulss	 %xmm1,  %xmm15
	subss	 %xmm15, %xmm0
	pshufd	 $0x00, %xmm8, %xmm15
	mulss	 %xmm15, %xmm0
#endif

#ifdef LT
	movaps	-32 * SIZE(AO), %xmm8

	pshufd	 $0x00, %xmm8, %xmm15
	mulss	 %xmm15, %xmm0
	pshufd	 $0x55, %xmm8, %xmm15
	mulss	 %xmm0,  %xmm15
	subss	 %xmm15, %xmm1
	pshufd	 $0xff, %xmm8, %xmm15
	mulss	 %xmm15, %xmm1
#endif

#if defined(RN) || defined(RT)
	movaps	-32 * SIZE(BO), %xmm8

	pshufd	 $0x00, %xmm8, %xmm15
	mulps	 %xmm15, %xmm0
#endif

#ifdef LN
	subq	$2 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	unpcklps %xmm1, %xmm0

	movlps	%xmm0, -32 * SIZE(BO)

	movlps	%xmm0,  0 * SIZE(CO1)
#else
	movlps	%xmm0, -32 * SIZE(AO)

	movlps	%xmm0,  0 * SIZE(CO1)
#endif

#ifndef LN
	addq	$2 * SIZE, CO1
#endif

#if defined(LT) || defined(RN)
	movq	K,  %rax
	subq	KK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 1), BO
#endif

#ifdef LN
	subq	$2, KK
#endif

#ifdef LT
	addq	$2, KK
#endif

#ifdef RT
       movq	K, %rax
       salq	$1 + BASE_SHIFT, %rax
       addq	%rax, AORIG
#endif
	ALIGN_4

.L120:
	testq	$1, M
	BRANCH
	jle	.L129

#ifdef LN
       movq	K, %rax
       salq	$BASE_SHIFT, %rax
       subq	%rax, AORIG
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	movq	AORIG, AO
	leaq	(AO, %rax, 1), AO
	leaq	(B,  %rax, 1), BO
#else
	movq	B, BO
#endif

	xorps	%xmm2, %xmm2
	movss	-32 * SIZE(AO), %xmm0
	xorps	%xmm8, %xmm8

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	sarq	$2, %rax
	NOBRANCH
	jle	.L125
	ALIGN_3

.L122:
	addss	%xmm2, %xmm8
	movss	-32 * SIZE(BO), %xmm2
	mulss	%xmm0, %xmm2
	movss	-31 * SIZE(AO), %xmm0

	addss	%xmm2, %xmm8
	movss	-31 * SIZE(BO), %xmm2
	mulss	%xmm0, %xmm2
	movss	-30 * SIZE(AO), %xmm0

	addss	%xmm2, %xmm8
	movss	-30 * SIZE(BO), %xmm2
	mulss	%xmm0, %xmm2
	movss	-29 * SIZE(AO), %xmm0

	addss	%xmm2, %xmm8
	movss	-29 * SIZE(BO), %xmm2
	mulss	%xmm0, %xmm2
	movss	-28 * SIZE(AO), %xmm0

	subq	$-4 * SIZE, AO
	subq	$-4 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L122
	ALIGN_3

.L125:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	BRANCH
	je	.L128
	ALIGN_3

.L126:
	addss	%xmm2, %xmm8
	movss	-32 * SIZE(BO), %xmm2
	mulss	%xmm0, %xmm2
	movss	-31 * SIZE(AO), %xmm0

	addq	$1 * SIZE, AO
	addq	$1 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L126
	ALIGN_3

.L128:
#if defined(LN) || defined(RT)
	movq	KK, %rax
	subq	$1, %rax

	leaq	(, %rax, SIZE), %rax

	movq	AORIG, AO
	leaq	(AO, %rax, 1), AO
	leaq	(B,  %rax, 1), BO
#endif

	addss	%xmm2, %xmm8

#if defined(LN) || defined(LT)
	movss	-32 * SIZE(BO), %xmm0

	subss	%xmm8,  %xmm0
#else
	movss	-32 * SIZE(AO), %xmm0

	subss	%xmm8,  %xmm0
#endif

#if defined(LN) || defined(LT)
	movss	-32 * SIZE(AO), %xmm8
#endif

#if defined(RN) || defined(RT)
	movaps	-32 * SIZE(BO), %xmm8
#endif

	mulss	 %xmm8, %xmm0

#ifdef LN
	subq	$1 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	movss	%xmm0, -32 * SIZE(BO)
#else
	movss	%xmm0, -32 * SIZE(AO)
#endif

	movss	%xmm0,  (CO1)

#ifndef LN
	addq	$1 * SIZE, CO1
#endif

#if defined(LT) || defined(RN)
	movq	K,  %rax
	subq	KK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 1), BO
#endif

#ifdef LN
	subq	$1, KK
#endif

#ifdef LT
	addq	$1, KK
#endif

#ifdef RT
       movq	K, %rax
       salq	$BASE_SHIFT, %rax
       addq	%rax, AORIG
#endif
	ALIGN_4

.L129:
#ifdef LN
       leaq	(, K, SIZE), %rax
       leaq	(B, %rax, 1), B
#endif
#if defined(LT) || defined(RN)
	movq	BO, B
#endif

#ifdef RN
	addq	$1, KK
#endif

#ifdef RT
	subq	$1, KK
#endif
	ALIGN_4

.L40:
	testq	$2, N
	jle	.L70

#if defined(LT) || defined(RN)
	movq	A, AO
#else
	movq	A, AORIG
#endif

#ifdef RT
       movq	K, %rax
       salq	$1 + BASE_SHIFT, %rax
       subq	%rax, B

       leaq	(, LDC, 2), %rax
       subq	%rax, C
#endif

	movq	C, CO1
	leaq	(C, LDC, 1), CO2
#ifndef RT
	leaq	(C, LDC, 2), C
#endif

#ifdef LN
	movq	OFFSET, %rax
	addq	M, %rax
	movq	%rax, KK
#endif

#ifdef LT
	movq	OFFSET, %rax
	movq	%rax, KK
#endif

	movq	M,  I
	sarq	$2, I
	NOBRANCH
	jle	.L80
	ALIGN_4

.L71:
#ifdef LN
       movq	K, %rax
       salq	$2 + BASE_SHIFT, %rax
       subq	%rax, AORIG
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	movq	AORIG, AO
	leaq	(AO, %rax, 4), AO
	leaq	(B,  %rax, 2), BO
#else
	movq	B, BO
#endif

	xorps	%xmm1, %xmm1
	movaps	-32 * SIZE(AO), %xmm0
	xorps	%xmm2, %xmm2
	movsd	-32 * SIZE(BO), %xmm3

	xorps	%xmm8,  %xmm8
	prefetcht2     4 * SIZE(CO1)
	xorps	%xmm9,  %xmm9
	prefetcht2     4 * SIZE(CO2)

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	sarq	$2, %rax
	NOBRANCH
	jle	.L75
	ALIGN_3

.L72:
	PREFETCH	(PREFETCHSIZE +  0) * SIZE(AO)

	addps	%xmm1, %xmm8
	pshufd	$0x00,  %xmm3, %xmm1
	mulps	%xmm0, %xmm1
	addps	%xmm2, %xmm9
	pshufd	$0x55,  %xmm3, %xmm2
	movsd	-30 * SIZE(BO), %xmm3
	mulps	%xmm0, %xmm2
	movaps	-28 * SIZE(AO), %xmm0

	addps	%xmm1, %xmm8
	pshufd	$0x00,  %xmm3, %xmm1
	mulps	%xmm0, %xmm1
	addps	%xmm2, %xmm9
	pshufd	$0x55,  %xmm3, %xmm2
	movsd	-28 * SIZE(BO), %xmm3
	mulps	%xmm0, %xmm2
	movaps	-24 * SIZE(AO), %xmm0

	addps	%xmm1, %xmm8
	pshufd	$0x00,  %xmm3, %xmm1
	mulps	%xmm0, %xmm1
	addps	%xmm2, %xmm9
	pshufd	$0x55,  %xmm3, %xmm2
	movsd	-26 * SIZE(BO), %xmm3
	mulps	%xmm0, %xmm2
	movaps	-20 * SIZE(AO), %xmm0

	addps	%xmm1, %xmm8
	pshufd	$0x00,  %xmm3, %xmm1
	mulps	%xmm0, %xmm1
	addps	%xmm2, %xmm9
	pshufd	$0x55,  %xmm3, %xmm2
	movsd	-24 * SIZE(BO), %xmm3
	mulps	%xmm0, %xmm2
	movaps	-16 * SIZE(AO), %xmm0

	subq	$-16 * SIZE, AO
	subq	$ -8 * SIZE, BO
	subq	$1, %rax
	BRANCH
	jg	.L72
	ALIGN_3

.L75:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	BRANCH
	je	.L78
	ALIGN_3

.L76:
	addps	%xmm1, %xmm8
	pshufd	$0x00,  %xmm3, %xmm1
	mulps	%xmm0, %xmm1
	addps	%xmm2, %xmm9
	pshufd	$0x55,  %xmm3, %xmm2
	movsd	-30 * SIZE(BO), %xmm3
	mulps	%xmm0, %xmm2
	movaps	-28 * SIZE(AO), %xmm0

	addq	$4 * SIZE, AO
	addq	$2 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L76
	ALIGN_3

.L78:
#if defined(LN) || defined(RT)
	movq	KK, %rax
#ifdef LN
	subq	$4, %rax
#else
	subq	$2, %rax
#endif

	leaq	(, %rax, SIZE), %rax

	movq	AORIG, AO
	leaq	(AO, %rax, 4), AO
	leaq	(B,  %rax, 2), BO
#endif

	addps	%xmm1, %xmm8
	addps	%xmm2, %xmm9

#if defined(LN) || defined(LT)
	movaps	%xmm8,  %xmm4
	unpcklps %xmm9, %xmm8
	unpckhps %xmm9, %xmm4

	movaps	-32 * SIZE(BO), %xmm0
	movaps	-28 * SIZE(BO), %xmm2

	subps	%xmm8,  %xmm0
	subps	%xmm4,  %xmm2

	movhlps	%xmm0, %xmm1
	movhlps	%xmm2, %xmm3
#else
	movaps	-32 * SIZE(AO), %xmm0
	movaps	-28 * SIZE(AO), %xmm2

	subps	%xmm8,  %xmm0
	subps	%xmm9,  %xmm2
#endif


#ifdef LN
	movaps	-20 * SIZE(AO), %xmm8

	pshufd	 $0xff, %xmm8, %xmm15
	mulps	 %xmm15, %xmm3
	pshufd	 $0xaa, %xmm8, %xmm15
	mulps	 %xmm3,  %xmm15
	subps	 %xmm15, %xmm2
	pshufd	 $0x55, %xmm8, %xmm15
	mulps	 %xmm3,  %xmm15
	subps	 %xmm15, %xmm1
	pshufd	 $0x00, %xmm8, %xmm15
	mulps	 %xmm3,  %xmm15
	subps	 %xmm15, %xmm0

	movaps	-24 * SIZE(AO), %xmm8

	pshufd	 $0xaa, %xmm8, %xmm15
	mulps	 %xmm15, %xmm2
	pshufd	 $0x55, %xmm8, %xmm15
	mulps	 %xmm2,  %xmm15
	subps	 %xmm15, %xmm1
	pshufd	 $0x00, %xmm8, %xmm15
	mulps	 %xmm2,  %xmm15
	subps	 %xmm15, %xmm0

	movaps	-28 * SIZE(AO), %xmm8

	pshufd	 $0x55, %xmm8, %xmm15
	mulps	 %xmm15, %xmm1
	pshufd	 $0x00, %xmm8, %xmm15
	mulps	 %xmm1,  %xmm15
	subps	 %xmm15, %xmm0

	movaps	-32 * SIZE(AO), %xmm8

	pshufd	 $0x00, %xmm8, %xmm15
	mulps	 %xmm15, %xmm0
#endif

#ifdef LT
	movaps	-32 * SIZE(AO), %xmm8

	pshufd	 $0x00, %xmm8, %xmm15
	mulps	 %xmm15, %xmm0
	pshufd	 $0x55, %xmm8, %xmm15
	mulps	 %xmm0,  %xmm15
	subps	 %xmm15, %xmm1
	pshufd	 $0xaa, %xmm8, %xmm15
	mulps	 %xmm0,  %xmm15
	subps	 %xmm15, %xmm2
	pshufd	 $0xff, %xmm8, %xmm15
	mulps	 %xmm0,  %xmm15
	subps	 %xmm15, %xmm3

	movaps	-28 * SIZE(AO), %xmm8

	pshufd	 $0x55, %xmm8, %xmm15
	mulps	 %xmm15, %xmm1
	pshufd	 $0xaa, %xmm8, %xmm15
	mulps	 %xmm1,  %xmm15
	subps	 %xmm15, %xmm2
	pshufd	 $0xff, %xmm8, %xmm15
	mulps	 %xmm1,  %xmm15
	subps	 %xmm15, %xmm3

	movaps	-24 * SIZE(AO), %xmm8

	pshufd	 $0xaa, %xmm8, %xmm15
	mulps	 %xmm15, %xmm2
	pshufd	 $0xff, %xmm8, %xmm15
	mulps	 %xmm2,  %xmm15
	subps	 %xmm15, %xmm3

	movaps	-20 * SIZE(AO), %xmm8

	pshufd	 $0xff, %xmm8, %xmm15
	mulps	 %xmm15, %xmm3
#endif

#ifdef RN
	movaps	-32 * SIZE(BO), %xmm8

	pshufd	 $0x00, %xmm8, %xmm15
	mulps	 %xmm15, %xmm0
	pshufd	 $0x55,  %xmm8, %xmm15
	mulps	 %xmm0,  %xmm15
	subps	 %xmm15, %xmm2

	pshufd	 $0xff, %xmm8, %xmm15
	mulps	 %xmm15, %xmm2
#endif

#ifdef RT
	movaps	-32 * SIZE(BO), %xmm8

	pshufd	 $0xff, %xmm8, %xmm15
	mulps	 %xmm15, %xmm2
	pshufd	 $0xaa,  %xmm8, %xmm15
	mulps	 %xmm2,  %xmm15
	subps	 %xmm15, %xmm0
	pshufd	 $0x00, %xmm8, %xmm15
	mulps	 %xmm15, %xmm0
#endif

#ifdef LN
	subq	$4 * SIZE, CO1
	subq	$4 * SIZE, CO2
#endif

#if defined(LN) || defined(LT)
	movlps	%xmm0, -32 * SIZE(BO)
	movlps	%xmm1, -30 * SIZE(BO)
	movlps	%xmm2, -28 * SIZE(BO)
	movlps	%xmm3, -26 * SIZE(BO)

	unpcklps %xmm1, %xmm0
	unpcklps %xmm3, %xmm2

	movlps	%xmm0,  0 * SIZE(CO1)
	movlps	%xmm2,  2 * SIZE(CO1)
	movhps	%xmm0,  0 * SIZE(CO2)
	movhps	%xmm2,  2 * SIZE(CO2)

#else
	movaps	%xmm0, -32 * SIZE(AO)
	movaps	%xmm2, -28 * SIZE(AO)

	movlps	%xmm0,  0 * SIZE(CO1)
	movhps	%xmm0,  2 * SIZE(CO1)
	movlps	%xmm2,  0 * SIZE(CO2)
	movhps	%xmm2,  2 * SIZE(CO2)
#endif

#ifndef LN
	addq	$4 * SIZE, CO1
	addq	$4 * SIZE, CO2
#endif

#if defined(LT) || defined(RN)
	movq	K,  %rax
	subq	KK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 2), BO
#endif

#ifdef LN
	subq	$4, KK
#endif

#ifdef LT
	addq	$4, KK
#endif

#ifdef RT
       movq	K, %rax
       salq	$2 + BASE_SHIFT, %rax
       addq	%rax, AORIG
#endif

	decq	I
	BRANCH
	jg	.L71
	ALIGN_4

.L80:
	testq	$2, M
	BRANCH
	jle	.L90

#ifdef LN
       movq	K, %rax
       salq	$1 + BASE_SHIFT, %rax
       subq	%rax, AORIG
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	movq	AORIG, AO
	leaq	(AO, %rax, 2), AO
	leaq	(B,  %rax, 2), BO
#else
	movq	B, BO
#endif

	xorps	%xmm1, %xmm1
	movddup	-32 * SIZE(AO), %xmm0
	xorps	%xmm2, %xmm2
	movsd	-32 * SIZE(BO), %xmm5
	xorps	%xmm8,  %xmm8
	xorps	%xmm9,  %xmm9

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	sarq	$2, %rax
	NOBRANCH
	jle	.L85
	ALIGN_3

.L82:
	addps	%xmm1, %xmm8
	movsd	-32 * SIZE(BO), %xmm1
	unpcklps    %xmm1, %xmm1
	mulps	%xmm0, %xmm1
	movddup	-30 * SIZE(AO), %xmm0

	addps	%xmm1, %xmm8
	movsd	-30 * SIZE(BO), %xmm1
	unpcklps    %xmm1, %xmm1
	mulps	%xmm0, %xmm1
	movddup	-28 * SIZE(AO), %xmm0

	addps	%xmm1, %xmm8
	movsd	-28 * SIZE(BO), %xmm1
	unpcklps    %xmm1, %xmm1
	mulps	%xmm0, %xmm1
	movddup	-26 * SIZE(AO), %xmm0

	addps	%xmm1, %xmm8
	movsd	-26 * SIZE(BO), %xmm1
	unpcklps    %xmm1, %xmm1
	mulps	%xmm0, %xmm1
	movddup	-24 * SIZE(AO), %xmm0

	subq	$-8 * SIZE, BO
	subq	$-8 * SIZE, AO

	subq	$1, %rax
	BRANCH
	jg	.L82
	ALIGN_3

.L85:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	BRANCH
	je	.L88
	ALIGN_3

.L86:
	addps	%xmm1, %xmm8
	movsd	-32 * SIZE(BO), %xmm1
	unpcklps    %xmm1, %xmm1
	mulps	%xmm0, %xmm1
	movddup	-30 * SIZE(AO), %xmm0

	addq	$2 * SIZE, AO
	addq	$2 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L86
	ALIGN_3

.L88:
#if defined(LN) || defined(RT)
	movq	KK, %rax
#ifdef LN
	subq	$2, %rax
#else
	subq	$2, %rax
#endif

	leaq	(, %rax, SIZE), %rax

	movq	AORIG, AO
	leaq	(AO, %rax, 2), AO
	leaq	(B,  %rax, 2), BO
#endif

	addps	%xmm1, %xmm8

#if defined(LN) || defined(LT)
	pshufd	$0xd8, %xmm8, %xmm8

	movaps	-32 * SIZE(BO), %xmm0
#else
	movaps	-32 * SIZE(AO), %xmm0
#endif

	subps	%xmm8, %xmm0

	movhlps	%xmm0, %xmm1

#ifdef LN
	movaps	-32 * SIZE(AO), %xmm8

	pshufd	 $0xff, %xmm8, %xmm15
	mulps	 %xmm15, %xmm1
	pshufd	 $0xaa, %xmm8, %xmm15
	mulps	 %xmm1,  %xmm15
	subps	 %xmm15, %xmm0
	pshufd	 $0x00, %xmm8, %xmm15
	mulps	 %xmm15, %xmm0
#endif

#ifdef LT
	movaps	-32 * SIZE(AO), %xmm8

	pshufd	 $0x00, %xmm8, %xmm15
	mulps	 %xmm15, %xmm0
	pshufd	 $0x55, %xmm8, %xmm15
	mulps	 %xmm0,  %xmm15
	subps	 %xmm15, %xmm1
	pshufd	 $0xff, %xmm8, %xmm15
	mulps	 %xmm15, %xmm1
#endif

#ifdef RN
	movaps	-32 * SIZE(BO), %xmm8

	pshufd	 $0x00, %xmm8, %xmm15
	mulps	 %xmm15, %xmm0
	pshufd	 $0x55,  %xmm8, %xmm15
	mulps	 %xmm0,  %xmm15
	subps	 %xmm15, %xmm1
	pshufd	 $0xff, %xmm8, %xmm15
	mulps	 %xmm15, %xmm1
#endif

#ifdef RT
	movaps	-32 * SIZE(BO), %xmm8

	pshufd	 $0xff, %xmm8, %xmm15
	mulps	 %xmm15, %xmm1
	pshufd	 $0xaa,  %xmm8, %xmm15
	mulps	 %xmm1,  %xmm15
	subps	 %xmm15, %xmm0
	pshufd	 $0x00, %xmm8, %xmm15
	mulps	 %xmm15, %xmm0
#endif

#ifdef LN
	subq	$2 * SIZE, CO1
	subq	$2 * SIZE, CO2
#endif

#if defined(LN) || defined(LT)
	movlps	%xmm0, -32 * SIZE(BO)
	movlps	%xmm1, -30 * SIZE(BO)

	unpcklps       %xmm1, %xmm0

	movlps	%xmm0,  (CO1)
	movhps	%xmm0,  (CO2)
#else
	movlps	%xmm0, -32 * SIZE(AO)
	movlps	%xmm1, -30 * SIZE(AO)

	movsd	%xmm0,  (CO1)
	movsd	%xmm1,  (CO2)
#endif

#ifndef LN
	addq	$2 * SIZE, CO1
	addq	$2 * SIZE, CO2
#endif

#if defined(LT) || defined(RN)
	movq	K,  %rax
	subq	KK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 2), BO
#endif

#ifdef LN
	subq	$2, KK
#endif

#ifdef LT
	addq	$2, KK
#endif

#ifdef RT
       movq	K, %rax
       salq	$1 + BASE_SHIFT, %rax
       addq	%rax, AORIG
#endif
	ALIGN_4

.L90:
	testq	$1, M
	BRANCH
	jle	.L99

#ifdef LN
       movq	K, %rax
       salq	$BASE_SHIFT, %rax
       subq	%rax, AORIG
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	movq	AORIG, AO
	leaq	(AO, %rax, 1), AO
	leaq	(B,  %rax, 2), BO
#else
	movq	B, BO
#endif

	xorps	%xmm2, %xmm2
	movsd	-32 * SIZE(AO), %xmm0
	xorps	%xmm8, %xmm8
	xorps	%xmm9, %xmm9

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	sarq	$2, %rax
	NOBRANCH
	jle	.L95
	ALIGN_3

.L92:
	pshufd	$0x00, %xmm0, %xmm1
	addps	%xmm2, %xmm8
	movsd	-32 * SIZE(BO), %xmm2
	mulps	%xmm1, %xmm2

	pshufd	$0x55, %xmm0, %xmm1
	movsd	-30 * SIZE(AO), %xmm0
	addps	%xmm2, %xmm9
	movsd	-30 * SIZE(BO), %xmm2
	mulps	%xmm1, %xmm2

	pshufd	$0x00, %xmm0, %xmm1
	addps	%xmm2, %xmm8
	movsd	-28 * SIZE(BO), %xmm2
	mulps	%xmm1, %xmm2

	pshufd	$0x55, %xmm0, %xmm1
	movsd	-28 * SIZE(AO), %xmm0
	addps	%xmm2, %xmm9
	movsd	-26 * SIZE(BO), %xmm2
	mulps	%xmm1, %xmm2

	subq	$-4 * SIZE, AO
	subq	$-8 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L92
	addps	%xmm9, %xmm8
	ALIGN_3

.L95:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	BRANCH
	je	.L98
	ALIGN_3

.L96:
	pshufd	$0x00, %xmm0, %xmm1
	movss	-31 * SIZE(AO), %xmm0
	addps	%xmm2, %xmm8
	movsd	-32 * SIZE(BO), %xmm2
	mulps	%xmm1, %xmm2

	addq	$1 * SIZE, AO
	addq	$2 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L96
	ALIGN_3

.L98:
#if defined(LN) || defined(RT)
	movq	KK, %rax
#ifdef LN
	subq	$1, %rax
#else
	subq	$2, %rax
#endif

	leaq	(, %rax, SIZE), %rax

	movq	AORIG, AO
	leaq	(AO, %rax, 1), AO
	leaq	(B,  %rax, 2), BO
#endif

	addps	%xmm2, %xmm8

#if defined(LN) || defined(LT)
	movsd	-32 * SIZE(BO), %xmm0

	subps	%xmm8,  %xmm0
#else
	movsd	-32 * SIZE(AO), %xmm0

	subps	%xmm8,  %xmm0
#endif

	pshufd	$0x55, %xmm0, %xmm1
	pshufd	$0x00, %xmm0, %xmm0

#if defined(LN) || defined(LT)
	movss	-32 * SIZE(AO), %xmm8

	mulss	 %xmm8, %xmm0
	mulss	 %xmm8, %xmm1
#endif

#ifdef RN
	movaps	-32 * SIZE(BO), %xmm8

	pshufd	 $0x00, %xmm8, %xmm15
	mulss	 %xmm15, %xmm0
	pshufd	 $0x55,  %xmm8, %xmm15
	mulss	 %xmm0,  %xmm15
	subss	 %xmm15, %xmm1
	pshufd	 $0xff, %xmm8, %xmm15
	mulss	 %xmm15, %xmm1
#endif

#ifdef RT
	movaps	-32 * SIZE(BO), %xmm8

	pshufd	 $0xff, %xmm8, %xmm15
	mulss	 %xmm15, %xmm1
	pshufd	 $0xaa,  %xmm8, %xmm15
	mulss	 %xmm1,  %xmm15
	subss	 %xmm15, %xmm0
	pshufd	 $0x00, %xmm8, %xmm15
	mulss	 %xmm15, %xmm0
#endif

#ifdef LN
	subq	$1 * SIZE, CO1
	subq	$1 * SIZE, CO2
#endif

#if defined(LN) || defined(LT)
	movss	%xmm0, -32 * SIZE(BO)
	movss	%xmm1, -31 * SIZE(BO)
#else
	movss	%xmm0, -32 * SIZE(AO)
	movss	%xmm1, -31 * SIZE(AO)
#endif

	movss	%xmm0,  (CO1)
	movss	%xmm1,  (CO2)

#ifndef LN
	addq	$1 * SIZE, CO1
	addq	$1 * SIZE, CO2
#endif

#if defined(LT) || defined(RN)
	movq	K,  %rax
	subq	KK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 2), BO
#endif

#ifdef LN
	subq	$1, KK
#endif

#ifdef LT
	addq	$1, KK
#endif

#ifdef RT
       movq	K, %rax
       salq	$BASE_SHIFT, %rax
       addq	%rax, AORIG
#endif
	ALIGN_4

.L99:
#ifdef LN
       leaq	(, K, SIZE), %rax
       leaq	(B, %rax, 2), B
#endif
#if defined(LT) || defined(RN)
	movq	BO, B
#endif

#ifdef RN
	addq	$2, KK
#endif

#ifdef RT
	subq	$2, KK
#endif
	ALIGN_4

.L70:
	testq	$4, N
	jle	.L100

#if defined(LT) || defined(RN)
	movq	A, AO
#else
	movq	A, AORIG
#endif

#ifdef RT
       movq	K, %rax
       salq	$2 + BASE_SHIFT, %rax
       subq	%rax, B

       leaq	(, LDC, 4), %rax
       subq	%rax, C
#endif

	movq	C, CO1
	leaq	(C, LDC, 2), CO2
#ifndef RT
	leaq	(C, LDC, 4), C
#endif

#ifdef LN
	movq	OFFSET, %rax
	addq	M, %rax
	movq	%rax, KK
#endif

#ifdef LT
	movq	OFFSET, %rax
	movq	%rax, KK
#endif

	movq	M,  I
	sarq	$2, I
	NOBRANCH
	jle	.L50
	ALIGN_4

.L41:
#ifdef LN
       movq	K, %rax
       salq	$2 + BASE_SHIFT, %rax
       subq	%rax, AORIG
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	movq	AORIG, AO
	leaq	(AO, %rax, 4), AO
	leaq	(B,  %rax, 4), BO
#else
	movq	B, BO
#endif

	xorps	%xmm1, %xmm1
	movaps	-32 * SIZE(AO), %xmm0
	xorps	%xmm2, %xmm2
	xorps	%xmm3, %xmm3
	xorps	%xmm4, %xmm4

	xorps	%xmm8,  %xmm8
	prefetcht2     4 * SIZE(CO1)
	xorps	%xmm9,  %xmm9
	prefetcht2     4 * SIZE(CO1, LDC,  1)
	xorps	%xmm10, %xmm10
	prefetcht2     4 * SIZE(CO2)
	xorps	%xmm11, %xmm11
	prefetcht2     4 * SIZE(CO2, LDC,  1)

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	sarq	$2, %rax
	NOBRANCH
	jle	.L45
	ALIGN_3

.L42:
	PREFETCH	(PREFETCHSIZE +  0) * SIZE(AO)

	addps	%xmm1, %xmm8
	movaps	-32 * SIZE(BO), %xmm1
	addps	%xmm2, %xmm9
	pshufd	$0x39, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	addps	%xmm3, %xmm10
	pshufd	$0x39, %xmm2, %xmm3
	mulps	%xmm0, %xmm2

	addps	%xmm4, %xmm11
	pshufd	$0x39, %xmm3, %xmm4
	mulps	%xmm0, %xmm3
	mulps	%xmm0, %xmm4
	movaps	-28 * SIZE(AO), %xmm0

	addps	%xmm1, %xmm8
	movaps	-28 * SIZE(BO), %xmm1
	addps	%xmm2, %xmm9
	pshufd	$0x39, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	addps	%xmm3, %xmm10
	pshufd	$0x39, %xmm2, %xmm3
	mulps	%xmm0, %xmm2

	addps	%xmm4, %xmm11
	pshufd	$0x39, %xmm3, %xmm4
	mulps	%xmm0, %xmm3
	mulps	%xmm0, %xmm4
	movaps	-24 * SIZE(AO), %xmm0

	addps	%xmm1, %xmm8
	movaps	-24 * SIZE(BO), %xmm1
	addps	%xmm2, %xmm9
	pshufd	$0x39, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	addps	%xmm3, %xmm10
	pshufd	$0x39, %xmm2, %xmm3
	mulps	%xmm0, %xmm2

	addps	%xmm4, %xmm11
	pshufd	$0x39, %xmm3, %xmm4
	mulps	%xmm0, %xmm3
	mulps	%xmm0, %xmm4
	movaps	-20 * SIZE(AO), %xmm0

	addps	%xmm1, %xmm8
	movaps	-20 * SIZE(BO), %xmm1
	addps	%xmm2, %xmm9
	pshufd	$0x39, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	addps	%xmm3, %xmm10
	pshufd	$0x39, %xmm2, %xmm3
	mulps	%xmm0, %xmm2

	addps	%xmm4, %xmm11
	pshufd	$0x39, %xmm3, %xmm4
	mulps	%xmm0, %xmm3
	mulps	%xmm0, %xmm4
	movaps	-16 * SIZE(AO), %xmm0

	subq	$-16 * SIZE, AO
	subq	$-16 * SIZE, BO
	subq	$1, %rax
	BRANCH
	jg	.L42
	ALIGN_3

.L45:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	BRANCH
	je	.L48
	ALIGN_3

.L46:
	addps	%xmm1, %xmm8
	movaps	-32 * SIZE(BO), %xmm1
	addps	%xmm2, %xmm9
	pshufd	$0x39, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	addps	%xmm3, %xmm10
	pshufd	$0x39, %xmm2, %xmm3
	mulps	%xmm0, %xmm2

	addps	%xmm4, %xmm11
	pshufd	$0x39, %xmm3, %xmm4
	mulps	%xmm0, %xmm3
	mulps	%xmm0, %xmm4
	movaps	-28 * SIZE(AO), %xmm0

	addq	$4 * SIZE, AO
	addq	$4 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L46
	ALIGN_3

.L48:
#if defined(LN) || defined(RT)
	movq	KK, %rax
#ifdef LN
	subq	$4, %rax
#else
	subq	$4, %rax
#endif

	leaq	(, %rax, SIZE), %rax

	movq	AORIG, AO
	leaq	(AO, %rax, 4), AO
	leaq	(B,  %rax, 4), BO
#endif

	addps	%xmm1, %xmm8
	addps	%xmm2, %xmm9
	addps	%xmm3, %xmm10
	addps	%xmm4, %xmm11

#if defined(LN) || defined(LT)
	movaps	%xmm8, %xmm4
	shufps	$0x88, %xmm9,  %xmm8
	movaps	%xmm10, %xmm5
	shufps	$0x88, %xmm11, %xmm10
	shufps	$0xdd, %xmm11, %xmm4
	shufps	$0xdd, %xmm9,  %xmm5

	movaps	%xmm8, %xmm6
	shufps	$0x88, %xmm10, %xmm8
	shufps	$0xdd, %xmm6,  %xmm10

	movaps	%xmm4, %xmm9
	movaps	%xmm5, %xmm11
	shufps	$0x22, %xmm5, %xmm9
	shufps	$0x77, %xmm4, %xmm11

	movaps	-32 * SIZE(BO), %xmm0
	movaps	-28 * SIZE(BO), %xmm1
	movaps	-24 * SIZE(BO), %xmm2
	movaps	-20 * SIZE(BO), %xmm3
#else
	movaps	%xmm9, %xmm4
	shufps	$0xd8, %xmm8, %xmm9
	shufps	$0xd8, %xmm11, %xmm8
	shufps	$0xd8, %xmm10, %xmm11
	shufps	$0xd8, %xmm4, %xmm10

	movaps	%xmm8, %xmm4
	shufps	$0xd8, %xmm10, %xmm8
	shufps	$0xd8, %xmm4, %xmm10
	movaps	%xmm9, %xmm5
	shufps	$0xd8, %xmm11, %xmm9
	shufps	$0xd8, %xmm5, %xmm11

	movaps	-32 * SIZE(AO), %xmm0
	movaps	-28 * SIZE(AO), %xmm1
	movaps	-24 * SIZE(AO), %xmm2
	movaps	-20 * SIZE(AO), %xmm3
#endif

	subps	%xmm8,  %xmm0
	subps	%xmm9,  %xmm1
	subps	%xmm10, %xmm2
	subps	%xmm11, %xmm3

#ifdef LN
	movaps	-20 * SIZE(AO), %xmm8

	pshufd	 $0xff, %xmm8, %xmm15
	mulps	 %xmm15, %xmm3
	pshufd	 $0xaa, %xmm8, %xmm15
	mulps	 %xmm3,  %xmm15
	subps	 %xmm15, %xmm2
	pshufd	 $0x55, %xmm8, %xmm15
	mulps	 %xmm3,  %xmm15
	subps	 %xmm15, %xmm1
	pshufd	 $0x00, %xmm8, %xmm15
	mulps	 %xmm3,  %xmm15
	subps	 %xmm15, %xmm0

	movaps	-24 * SIZE(AO), %xmm8

	pshufd	 $0xaa, %xmm8, %xmm15
	mulps	 %xmm15, %xmm2
	pshufd	 $0x55, %xmm8, %xmm15
	mulps	 %xmm2,  %xmm15
	subps	 %xmm15, %xmm1
	pshufd	 $0x00, %xmm8, %xmm15
	mulps	 %xmm2,  %xmm15
	subps	 %xmm15, %xmm0

	movaps	-28 * SIZE(AO), %xmm8

	pshufd	 $0x55, %xmm8, %xmm15
	mulps	 %xmm15, %xmm1
	pshufd	 $0x00, %xmm8, %xmm15
	mulps	 %xmm1,  %xmm15
	subps	 %xmm15, %xmm0

	movaps	-32 * SIZE(AO), %xmm8

	pshufd	 $0x00, %xmm8, %xmm15
	mulps	 %xmm15, %xmm0
#endif

#ifdef LT
	movaps	-32 * SIZE(AO), %xmm8

	pshufd	 $0x00, %xmm8, %xmm15
	mulps	 %xmm15, %xmm0
	pshufd	 $0x55, %xmm8, %xmm15
	mulps	 %xmm0,  %xmm15
	subps	 %xmm15, %xmm1
	pshufd	 $0xaa, %xmm8, %xmm15
	mulps	 %xmm0,  %xmm15
	subps	 %xmm15, %xmm2
	pshufd	 $0xff, %xmm8, %xmm15
	mulps	 %xmm0,  %xmm15
	subps	 %xmm15, %xmm3

	movaps	-28 * SIZE(AO), %xmm8

	pshufd	 $0x55, %xmm8, %xmm15
	mulps	 %xmm15, %xmm1
	pshufd	 $0xaa, %xmm8, %xmm15
	mulps	 %xmm1,  %xmm15
	subps	 %xmm15, %xmm2
	pshufd	 $0xff, %xmm8, %xmm15
	mulps	 %xmm1,  %xmm15
	subps	 %xmm15, %xmm3

	movaps	-24 * SIZE(AO), %xmm8

	pshufd	 $0xaa, %xmm8, %xmm15
	mulps	 %xmm15, %xmm2
	pshufd	 $0xff, %xmm8, %xmm15
	mulps	 %xmm2,  %xmm15
	subps	 %xmm15, %xmm3

	movaps	-20 * SIZE(AO), %xmm8

	pshufd	 $0xff, %xmm8, %xmm15
	mulps	 %xmm15, %xmm3
#endif

#ifdef RN
	movaps	-32 * SIZE(BO), %xmm8

	pshufd	 $0x00, %xmm8, %xmm15
	mulps	 %xmm15, %xmm0
	pshufd	 $0x55,  %xmm8, %xmm15
	mulps	 %xmm0,  %xmm15
	subps	 %xmm15, %xmm1
	pshufd	 $0xaa,  %xmm8, %xmm15
	mulps	 %xmm0,  %xmm15
	subps	 %xmm15, %xmm2
	pshufd	 $0xff,  %xmm8, %xmm15
	mulps	 %xmm0,  %xmm15
	subps	 %xmm15, %xmm3

	movaps	-28 * SIZE(BO), %xmm8

	pshufd	 $0x55, %xmm8, %xmm15
	mulps	 %xmm15, %xmm1
	pshufd	 $0xaa,  %xmm8, %xmm15
	mulps	 %xmm1,  %xmm15
	subps	 %xmm15, %xmm2
	pshufd	 $0xff,  %xmm8, %xmm15
	mulps	 %xmm1,  %xmm15
	subps	 %xmm15, %xmm3

	movaps	-24 * SIZE(BO), %xmm8

	pshufd	 $0xaa, %xmm8, %xmm15
	mulps	 %xmm15, %xmm2
	pshufd	 $0xff,  %xmm8, %xmm15
	mulps	 %xmm2,  %xmm15
	subps	 %xmm15, %xmm3

	movaps	-20 * SIZE(BO), %xmm8

	pshufd	 $0xff, %xmm8, %xmm15
	mulps	 %xmm15, %xmm3
#endif

#ifdef RT
	movaps	-20 * SIZE(BO), %xmm8

	pshufd	 $0xff, %xmm8, %xmm15
	mulps	 %xmm15, %xmm3
	pshufd	 $0xaa,  %xmm8, %xmm15
	mulps	 %xmm3,  %xmm15
	subps	 %xmm15, %xmm2
	pshufd	 $0x55,  %xmm8, %xmm15
	mulps	 %xmm3,  %xmm15
	subps	 %xmm15, %xmm1
	pshufd	 $0x00,  %xmm8, %xmm15
	mulps	 %xmm3,  %xmm15
	subps	 %xmm15, %xmm0

	movaps	-24 * SIZE(BO), %xmm8

	pshufd	 $0xaa, %xmm8, %xmm15
	mulps	 %xmm15, %xmm2
	pshufd	 $0x55,  %xmm8, %xmm15
	mulps	 %xmm2,  %xmm15
	subps	 %xmm15, %xmm1
	pshufd	 $0x00,  %xmm8, %xmm15
	mulps	 %xmm2,  %xmm15
	subps	 %xmm15, %xmm0

	movaps	-28 * SIZE(BO), %xmm8

	pshufd	 $0x55, %xmm8, %xmm15
	mulps	 %xmm15, %xmm1
	pshufd	 $0x00,  %xmm8, %xmm15
	mulps	 %xmm1,  %xmm15
	subps	 %xmm15, %xmm0

	movaps	-32 * SIZE(BO), %xmm8

	pshufd	 $0x00, %xmm8, %xmm15
	mulps	 %xmm15, %xmm0
#endif

#ifdef LN
	subq	$4 * SIZE, CO1
	subq	$4 * SIZE, CO2
#endif

#if defined(LN) || defined(LT)
	movaps	%xmm0, -32 * SIZE(BO)
	movaps	%xmm1, -28 * SIZE(BO)
	movaps	%xmm2, -24 * SIZE(BO)
	movaps	%xmm3, -20 * SIZE(BO)

	movaps	%xmm0, %xmm8
	shufps	$0x88, %xmm1, %xmm0
	shufps	$0xdd, %xmm8, %xmm1

	movaps	%xmm2, %xmm9
	shufps	$0x88, %xmm3, %xmm2
	shufps	$0xdd, %xmm9, %xmm3

	movaps	%xmm0, %xmm8
	shufps	$0x88, %xmm2, %xmm0
	movaps	%xmm1, %xmm9
	shufps	$0x22, %xmm3, %xmm1
	shufps	$0xdd, %xmm2, %xmm8
	movaps	%xmm8, %xmm2
	shufps	$0x77, %xmm3, %xmm9
	movaps	%xmm9, %xmm3
#else
	movaps	%xmm0, -32 * SIZE(AO)
	movaps	%xmm1, -28 * SIZE(AO)
	movaps	%xmm2, -24 * SIZE(AO)
	movaps	%xmm3, -20 * SIZE(AO)
#endif

	leaq	(LDC, LDC, 2), %rax

	movsd	%xmm0,  0 * SIZE(CO1)
	movhps	%xmm0,  2 * SIZE(CO1)
	movsd	%xmm1,  0 * SIZE(CO1, LDC,  1)
	movhps	%xmm1,  2 * SIZE(CO1, LDC,  1)

	movsd	%xmm2,  0 * SIZE(CO2)
	movhps	%xmm2,  2 * SIZE(CO2)
	movsd	%xmm3,  0 * SIZE(CO2, LDC,  1)
	movhps	%xmm3,  2 * SIZE(CO2, LDC,  1)

#ifndef LN
	addq	$4 * SIZE, CO1
	addq	$4 * SIZE, CO2
#endif

#if defined(LT) || defined(RN)
	movq	K,  %rax
	subq	KK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 4), BO
#endif

#ifdef LN
	subq	$4, KK
#endif

#ifdef LT
	addq	$4, KK
#endif

#ifdef RT
       movq	K, %rax
       salq	$2 + BASE_SHIFT, %rax
       addq	%rax, AORIG
#endif

	decq	I
	BRANCH
	jg	.L41
	ALIGN_4

.L50:
	testq	$2, M
	BRANCH
	jle	.L60

#ifdef LN
       movq	K, %rax
       salq	$1 + BASE_SHIFT, %rax
       subq	%rax, AORIG
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	movq	AORIG, AO
	leaq	(AO, %rax, 2), AO
	leaq	(B,  %rax, 4), BO
#else
	movq	B, BO
#endif

	xorps	%xmm1, %xmm1
	movddup	-32 * SIZE(AO), %xmm0
	xorps	%xmm2, %xmm2
	movaps	-32 * SIZE(BO), %xmm5
	xorps	%xmm8,  %xmm8
	xorps	%xmm9,  %xmm9

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	sarq	$2, %rax
	NOBRANCH
	jle	.L55
	ALIGN_3

.L52:
	addps	%xmm1, %xmm8
	pshufd	$0x50, %xmm5, %xmm1
	mulps	%xmm0, %xmm1
	addps	%xmm2, %xmm9
	pshufd	$0xfa, %xmm5, %xmm2
	movaps	-28 * SIZE(BO), %xmm5
	mulps	%xmm0, %xmm2
	movddup	-30 * SIZE(AO), %xmm0

	addps	%xmm1, %xmm8
	pshufd	$0x50, %xmm5, %xmm1
	mulps	%xmm0, %xmm1
	addps	%xmm2, %xmm9
	pshufd	$0xfa, %xmm5, %xmm2
	movaps	-24 * SIZE(BO), %xmm5
	mulps	%xmm0, %xmm2
	movddup	-28 * SIZE(AO), %xmm0

	addps	%xmm1, %xmm8
	pshufd	$0x50, %xmm5, %xmm1
	mulps	%xmm0, %xmm1
	addps	%xmm2, %xmm9
	pshufd	$0xfa, %xmm5, %xmm2
	movaps	-20 * SIZE(BO), %xmm5
	mulps	%xmm0, %xmm2
	movddup	-26 * SIZE(AO), %xmm0

	addps	%xmm1, %xmm8
	pshufd	$0x50, %xmm5, %xmm1
	mulps	%xmm0, %xmm1
	addps	%xmm2, %xmm9
	pshufd	$0xfa, %xmm5, %xmm2
	movaps	-16 * SIZE(BO), %xmm5
	mulps	%xmm0, %xmm2
	movddup	-24 * SIZE(AO), %xmm0

	subq	$-16 * SIZE, BO
	subq	$ -8 * SIZE, AO

	subq	$1, %rax
	BRANCH
	jg	.L52
	ALIGN_3

.L55:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	BRANCH
	je	.L58
	ALIGN_3

.L56:
	addps	%xmm1, %xmm8
	pshufd	$0x50, %xmm5, %xmm1
	mulps	%xmm0, %xmm1
	addps	%xmm2, %xmm9
	pshufd	$0xfa, %xmm5, %xmm2
	movaps	-28 * SIZE(BO), %xmm5
	mulps	%xmm0, %xmm2
	movddup	-30 * SIZE(AO), %xmm0

	addq	$2 * SIZE, AO
	addq	$4 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L56
	ALIGN_3

.L58:
#if defined(LN) || defined(RT)
	movq	KK, %rax
#ifdef LN
	subq	$2, %rax
#else
	subq	$4, %rax
#endif

	leaq	(, %rax, SIZE), %rax

	movq	AORIG, AO
	leaq	(AO, %rax, 2), AO
	leaq	(B,  %rax, 4), BO
#endif

	addps	%xmm1, %xmm8
	addps	%xmm2, %xmm9

#if defined(LN) || defined(LT)
	movaps	%xmm8, %xmm4
	shufps	$0x88, %xmm9, %xmm8
	shufps	$0xdd, %xmm9, %xmm4

	movaps	-32 * SIZE(BO), %xmm0
	movaps	-28 * SIZE(BO), %xmm1

	subps	%xmm8,  %xmm0
	subps	%xmm4,  %xmm1
#else
	movaps	-32 * SIZE(AO), %xmm0
	movaps	-28 * SIZE(AO), %xmm2

	subps	%xmm8,  %xmm0
	subps	%xmm9,  %xmm2

	movhlps	%xmm0, %xmm1
	movhlps	%xmm2, %xmm3
#endif

#ifdef LN
	movaps	-32 * SIZE(AO), %xmm8

	pshufd	 $0xff, %xmm8, %xmm15
	mulps	 %xmm15, %xmm1
	pshufd	 $0xaa, %xmm8, %xmm15
	mulps	 %xmm1,  %xmm15
	subps	 %xmm15, %xmm0
	pshufd	 $0x00, %xmm8, %xmm15
	mulps	 %xmm15, %xmm0
#endif

#ifdef LT
	movaps	-32 * SIZE(AO), %xmm8

	pshufd	 $0x00, %xmm8, %xmm15
	mulps	 %xmm15, %xmm0
	pshufd	 $0x55, %xmm8, %xmm15
	mulps	 %xmm0,  %xmm15
	subps	 %xmm15, %xmm1
	pshufd	 $0xff, %xmm8, %xmm15
	mulps	 %xmm15, %xmm1
#endif

#ifdef RN
	movaps	-32 * SIZE(BO), %xmm8

	pshufd	 $0x00, %xmm8, %xmm15
	mulps	 %xmm15, %xmm0
	pshufd	 $0x55,  %xmm8, %xmm15
	mulps	 %xmm0,  %xmm15
	subps	 %xmm15, %xmm1
	pshufd	 $0xaa,  %xmm8, %xmm15
	mulps	 %xmm0,  %xmm15
	subps	 %xmm15, %xmm2
	pshufd	 $0xff,  %xmm8, %xmm15
	mulps	 %xmm0,  %xmm15
	subps	 %xmm15, %xmm3

	movaps	-28 * SIZE(BO), %xmm8

	pshufd	 $0x55, %xmm8, %xmm15
	mulps	 %xmm15, %xmm1
	pshufd	 $0xaa,  %xmm8, %xmm15
	mulps	 %xmm1,  %xmm15
	subps	 %xmm15, %xmm2
	pshufd	 $0xff,  %xmm8, %xmm15
	mulps	 %xmm1,  %xmm15
	subps	 %xmm15, %xmm3

	movaps	-24 * SIZE(BO), %xmm8

	pshufd	 $0xaa, %xmm8, %xmm15
	mulps	 %xmm15, %xmm2
	pshufd	 $0xff,  %xmm8, %xmm15
	mulps	 %xmm2,  %xmm15
	subps	 %xmm15, %xmm3

	movaps	-20 * SIZE(BO), %xmm8

	pshufd	 $0xff, %xmm8, %xmm15
	mulps	 %xmm15, %xmm3
#endif

#ifdef RT
	movaps	-20 * SIZE(BO), %xmm8

	pshufd	 $0xff, %xmm8, %xmm15
	mulps	 %xmm15, %xmm3
	pshufd	 $0xaa,  %xmm8, %xmm15
	mulps	 %xmm3,  %xmm15
	subps	 %xmm15, %xmm2
	pshufd	 $0x55,  %xmm8, %xmm15
	mulps	 %xmm3,  %xmm15
	subps	 %xmm15, %xmm1
	pshufd	 $0x00,  %xmm8, %xmm15
	mulps	 %xmm3,  %xmm15
	subps	 %xmm15, %xmm0

	movaps	-24 * SIZE(BO), %xmm8

	pshufd	 $0xaa, %xmm8, %xmm15
	mulps	 %xmm15, %xmm2
	pshufd	 $0x55,  %xmm8, %xmm15
	mulps	 %xmm2,  %xmm15
	subps	 %xmm15, %xmm1
	pshufd	 $0x00,  %xmm8, %xmm15
	mulps	 %xmm2,  %xmm15
	subps	 %xmm15, %xmm0

	movaps	-28 * SIZE(BO), %xmm8

	pshufd	 $0x55, %xmm8, %xmm15
	mulps	 %xmm15, %xmm1
	pshufd	 $0x00,  %xmm8, %xmm15
	mulps	 %xmm1,  %xmm15
	subps	 %xmm15, %xmm0

	movaps	-32 * SIZE(BO), %xmm8

	pshufd	 $0x00, %xmm8, %xmm15
	mulps	 %xmm15, %xmm0
#endif

#ifdef LN
	subq	$2 * SIZE, CO1
	subq	$2 * SIZE, CO2
#endif

	leaq	(LDC, LDC, 2), %rax

#if defined(LN) || defined(LT)
	movaps	%xmm0, -32 * SIZE(BO)
	movaps	%xmm1, -28 * SIZE(BO)

	movaps	%xmm0,  %xmm4
	unpcklps %xmm1, %xmm0
	unpckhps %xmm1, %xmm4

	movsd	%xmm0,  (CO1)
	movhps	%xmm0,  (CO1, LDC,  1)
	movsd	%xmm4,  (CO2)
	movhps	%xmm4,  (CO2, LDC,  1)
#else
	movlhps	%xmm1, %xmm0
	movlhps	%xmm3, %xmm2

	movaps	%xmm0, -32 * SIZE(AO)
	movaps	%xmm2, -28 * SIZE(AO)

	movsd	%xmm0,  (CO1)
	movsd	%xmm1,  (CO1, LDC,  1)
	movsd	%xmm2,  (CO2)
	movsd	%xmm3,  (CO2, LDC,  1)
#endif

#ifndef LN
	addq	$2 * SIZE, CO1
	addq	$2 * SIZE, CO2
#endif

#if defined(LT) || defined(RN)
	movq	K,  %rax
	subq	KK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 4), BO
#endif

#ifdef LN
	subq	$2, KK
#endif

#ifdef LT
	addq	$2, KK
#endif

#ifdef RT
       movq	K, %rax
       salq	$1 + BASE_SHIFT, %rax
       addq	%rax, AORIG
#endif
	ALIGN_4

.L60:
	testq	$1, M
	BRANCH
	jle	.L69

#ifdef LN
       movq	K, %rax
       salq	$BASE_SHIFT, %rax
       subq	%rax, AORIG
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	movq	AORIG, AO
	leaq	(AO, %rax, 1), AO
	leaq	(B,  %rax, 4), BO
#else
	movq	B, BO
#endif

	xorps	%xmm2, %xmm2
	movsd	-32 * SIZE(AO), %xmm0
	xorps	%xmm8, %xmm8
	xorps	%xmm9, %xmm9

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	sarq	$2, %rax
	NOBRANCH
	jle	.L65
	ALIGN_3

.L62:
	pshufd	$0x00, %xmm0, %xmm1
	addps	%xmm2, %xmm8
	movaps	-32 * SIZE(BO), %xmm2
	mulps	%xmm1, %xmm2

	pshufd	$0x55, %xmm0, %xmm1
	movsd	-30 * SIZE(AO), %xmm0
	addps	%xmm2, %xmm9
	movaps	-28 * SIZE(BO), %xmm2
	mulps	%xmm1, %xmm2

	pshufd	$0x00, %xmm0, %xmm1
	addps	%xmm2, %xmm8
	movaps	-24 * SIZE(BO), %xmm2
	mulps	%xmm1, %xmm2

	pshufd	$0x55, %xmm0, %xmm1
	movsd	-28 * SIZE(AO), %xmm0
	addps	%xmm2, %xmm9
	movaps	-20 * SIZE(BO), %xmm2
	mulps	%xmm1, %xmm2

	subq	$-16 * SIZE, BO
	subq	$ -4 * SIZE, AO

	subq	$1, %rax
	BRANCH
	jg	.L62
	addps	%xmm9, %xmm8
	ALIGN_3

.L65:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	BRANCH
	je	.L68
	ALIGN_3

.L66:
	pshufd	$0x00, %xmm0, %xmm1
	movss	-31 * SIZE(AO), %xmm0
	addps	%xmm2, %xmm8
	movaps	-32 * SIZE(BO), %xmm2
	mulps	%xmm1, %xmm2

	addq	$1 * SIZE, AO
	addq	$4 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L66
	ALIGN_3

.L68:
#if defined(LN) || defined(RT)
	movq	KK, %rax
#ifdef LN
	subq	$1, %rax
#else
	subq	$4, %rax
#endif

	leaq	(, %rax, SIZE), %rax

	movq	AORIG, AO
	leaq	(AO, %rax, 1), AO
	leaq	(B,  %rax, 4), BO
#endif

	addps	%xmm2, %xmm8

#if defined(LN) || defined(LT)
	movaps	-32 * SIZE(BO), %xmm0

	subps	%xmm8,  %xmm0
#else
	movsd	-32 * SIZE(AO), %xmm0
	movhps	-30 * SIZE(AO), %xmm0

	subps	%xmm8,  %xmm0

	pshufd	$0xff, %xmm0, %xmm3
	pshufd	$0xaa, %xmm0, %xmm2
	pshufd	$0x55, %xmm0, %xmm1
	pshufd	$0x00, %xmm0, %xmm0
#endif

#if defined(LN) || defined(LT)
	movaps	-32 * SIZE(AO), %xmm8

	pshufd	 $0x00, %xmm8, %xmm15
	mulps	 %xmm15, %xmm0
#endif

#ifdef RN
	movaps	-32 * SIZE(BO), %xmm8

	pshufd	 $0x00, %xmm8, %xmm15
	mulss	 %xmm15, %xmm0
	pshufd	 $0x55,  %xmm8, %xmm15
	mulss	 %xmm0,  %xmm15
	subss	 %xmm15, %xmm1
	pshufd	 $0xaa,  %xmm8, %xmm15
	mulss	 %xmm0,  %xmm15
	subss	 %xmm15, %xmm2
	pshufd	 $0xff,  %xmm8, %xmm15
	mulss	 %xmm0,  %xmm15
	subss	 %xmm15, %xmm3

	movaps	-28 * SIZE(BO), %xmm8

	pshufd	 $0x55, %xmm8, %xmm15
	mulss	 %xmm15, %xmm1
	pshufd	 $0xaa,  %xmm8, %xmm15
	mulss	 %xmm1,  %xmm15
	subss	 %xmm15, %xmm2
	pshufd	 $0xff,  %xmm8, %xmm15
	mulss	 %xmm1,  %xmm15
	subss	 %xmm15, %xmm3

	movaps	-24 * SIZE(BO), %xmm8

	pshufd	 $0xaa, %xmm8, %xmm15
	mulss	 %xmm15, %xmm2
	pshufd	 $0xff,  %xmm8, %xmm15
	mulss	 %xmm2,  %xmm15
	subss	 %xmm15, %xmm3

	movaps	-20 * SIZE(BO), %xmm8

	pshufd	 $0xff, %xmm8, %xmm15
	mulss	 %xmm15, %xmm3
#endif

#ifdef RT
	movaps	-20 * SIZE(BO), %xmm8

	pshufd	 $0xff, %xmm8, %xmm15
	mulss	 %xmm15, %xmm3
	pshufd	 $0xaa,  %xmm8, %xmm15
	mulss	 %xmm3,  %xmm15
	subss	 %xmm15, %xmm2
	pshufd	 $0x55,  %xmm8, %xmm15
	mulss	 %xmm3,  %xmm15
	subss	 %xmm15, %xmm1
	pshufd	 $0x00,  %xmm8, %xmm15
	mulss	 %xmm3,  %xmm15
	subss	 %xmm15, %xmm0

	movaps	-24 * SIZE(BO), %xmm8

	pshufd	 $0xaa, %xmm8, %xmm15
	mulss	 %xmm15, %xmm2
	pshufd	 $0x55,  %xmm8, %xmm15
	mulss	 %xmm2,  %xmm15
	subss	 %xmm15, %xmm1
	pshufd	 $0x00,  %xmm8, %xmm15
	mulss	 %xmm2,  %xmm15
	subss	 %xmm15, %xmm0

	movaps	-28 * SIZE(BO), %xmm8

	pshufd	 $0x55, %xmm8, %xmm15
	mulss	 %xmm15, %xmm1
	pshufd	 $0x00,  %xmm8, %xmm15
	mulss	 %xmm1,  %xmm15
	subss	 %xmm15, %xmm0

	movaps	-32 * SIZE(BO), %xmm8

	pshufd	 $0x00, %xmm8, %xmm15
	mulss	 %xmm15, %xmm0
#endif

#ifdef LN
	subq	$1 * SIZE, CO1
	subq	$1 * SIZE, CO2
#endif

#if defined(LN) || defined(LT)
	movaps	%xmm0, -32 * SIZE(BO)

	pshufd	$0xff, %xmm0, %xmm3
	pshufd	$0xaa, %xmm0, %xmm2
	pshufd	$0x55, %xmm0, %xmm1
	pshufd	$0x00, %xmm0, %xmm0
#else
	unpcklps %xmm1, %xmm0
	unpcklps %xmm3, %xmm2

	movlps	%xmm0, -32 * SIZE(AO)
	movlps	%xmm2, -30 * SIZE(AO)
#endif

	movss	%xmm0,  (CO1)
	movss	%xmm1,  (CO1, LDC,  1)
	movss	%xmm2,  (CO2)
	movss	%xmm3,  (CO2, LDC,  1)

#ifndef LN
	addq	$1 * SIZE, CO1
	addq	$1 * SIZE, CO2
#endif

#if defined(LT) || defined(RN)
	movq	K,  %rax
	subq	KK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 4), BO
#endif

#ifdef LN
	subq	$1, KK
#endif

#ifdef LT
	addq	$1, KK
#endif

#ifdef RT
       movq	K, %rax
       salq	$BASE_SHIFT, %rax
       addq	%rax, AORIG
#endif
	ALIGN_4

.L69:
#ifdef LN
       leaq	(, K, SIZE), %rax
       leaq	(B, %rax, 4), B
#endif
#if defined(LT) || defined(RN)
	movq	BO, B
#endif

#ifdef RN
	addq	$4, KK
#endif

#ifdef RT
	subq	$4, KK
#endif
	ALIGN_4

.L100:
	movq	N,  J
	sarq	$3, J
	NOBRANCH
	jle	.L999
	ALIGN_4

.L10:
#if defined(LT) || defined(RN)
	movq	A, AO
#else
	movq	A, AORIG
#endif

#ifdef RT
       movq	K, %rax
       salq	$3 + BASE_SHIFT, %rax
       subq	%rax, B

       leaq	(, LDC, 8), %rax
       subq	%rax, C
#endif

	movq	C, CO1
	leaq	(C, LDC, 4), CO2
#ifndef RT
	leaq	(C, LDC, 8), C
#endif

#ifdef LN
	movq	OFFSET, %rax
	addq	M, %rax
	movq	%rax, KK
#endif

#ifdef LT
	movq	OFFSET, %rax
	movq	%rax, KK
#endif

	movq	K, %rax
	salq	$BASE_SHIFT + 3, %rax
	leaq	(B, %rax), BB

	movq	M,  I
	sarq	$2, I
	NOBRANCH
	jle	.L20
	ALIGN_4

.L11:
#ifdef LN
       movq	K, %rax
       salq	$2 + BASE_SHIFT, %rax
       subq	%rax, AORIG
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	movq	AORIG, AO
	leaq	(AO, %rax, 4), AO
	leaq	(B,  %rax, 8), BO
#else
	movq	B, BO
#endif

	prefetchnta	 -32 * SIZE(BB)
	subq		 $-16 * SIZE, BB

	xorps	%xmm1, %xmm1
	movaps	-32 * SIZE(AO), %xmm0
	xorps	%xmm2, %xmm2
	xorps	%xmm3, %xmm3
	xorps	%xmm4, %xmm4

	leaq	(LDC, LDC, 2), %rax

	xorps	%xmm8,  %xmm8
	prefetcht2     4 * SIZE(CO1)
	xorps	%xmm9,  %xmm9
	prefetcht2     4 * SIZE(CO1, LDC,  1)
	xorps	%xmm10, %xmm10
	prefetcht2     4 * SIZE(CO1, LDC,  2)
	xorps	%xmm11, %xmm11
	prefetcht2     4 * SIZE(CO1, %rax, 1)

	xorps	%xmm12, %xmm12
	prefetcht2     4 * SIZE(CO2)
	xorps	%xmm13, %xmm13
	prefetcht2     4 * SIZE(CO2, LDC,  1)
	xorps	%xmm14, %xmm14
	prefetcht2     4 * SIZE(CO2, LDC,  2)
	xorps	%xmm15, %xmm15
	prefetcht2     4 * SIZE(CO2, %rax, 1)

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	sarq	$2, %rax
	NOBRANCH
	jle	.L15
	ALIGN_3

.L12:
	PREFETCH	(PREFETCHSIZE +  0) * SIZE(AO)

	addps	%xmm1, %xmm12
	movaps	-32 * SIZE(BO), %xmm1
	addps	%xmm2, %xmm13
	pshufd	$0x39, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	pshufd	$0x39, %xmm2, %xmm5
	mulps	%xmm0, %xmm2

	addps	%xmm3, %xmm14
	addps	%xmm4, %xmm15
	pshufd	$0x39, %xmm5, %xmm6
	mulps	%xmm0, %xmm5
	mulps	%xmm0, %xmm6

	addps	%xmm1, %xmm8
	movaps	-28 * SIZE(BO), %xmm1
	addps	%xmm2, %xmm9
	pshufd	$0x39, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	pshufd	$0x39, %xmm2, %xmm3
	mulps	%xmm0, %xmm2

	addps	%xmm5, %xmm10
	addps	%xmm6, %xmm11
	pshufd	$0x39, %xmm3, %xmm4
	mulps	%xmm0, %xmm3
	mulps	%xmm0, %xmm4

	movaps	-28 * SIZE(AO), %xmm0

	addps	%xmm1, %xmm12
	movaps	-24 * SIZE(BO), %xmm1
	addps	%xmm2, %xmm13
	pshufd	$0x39, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	pshufd	$0x39, %xmm2, %xmm5
	mulps	%xmm0, %xmm2

	addps	%xmm3, %xmm14
	addps	%xmm4, %xmm15
	pshufd	$0x39, %xmm5, %xmm6
	mulps	%xmm0, %xmm5
	mulps	%xmm0, %xmm6

	addps	%xmm1, %xmm8
	movaps	-20 * SIZE(BO), %xmm1
	addps	%xmm2, %xmm9
	pshufd	$0x39, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	pshufd	$0x39, %xmm2, %xmm3
	mulps	%xmm0, %xmm2

	addps	%xmm5, %xmm10
	addps	%xmm6, %xmm11
	pshufd	$0x39, %xmm3, %xmm4
	mulps	%xmm0, %xmm3
	mulps	%xmm0, %xmm4

	movaps	-24 * SIZE(AO), %xmm0

	addps	%xmm1, %xmm12
	movaps	-16 * SIZE(BO), %xmm1
	addps	%xmm2, %xmm13
	pshufd	$0x39, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	pshufd	$0x39, %xmm2, %xmm5
	mulps	%xmm0, %xmm2

	addps	%xmm3, %xmm14
	addps	%xmm4, %xmm15
	pshufd	$0x39, %xmm5, %xmm6
	mulps	%xmm0, %xmm5
	mulps	%xmm0, %xmm6

	addps	%xmm1, %xmm8
	movaps	-12 * SIZE(BO), %xmm1
	addps	%xmm2, %xmm9
	pshufd	$0x39, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	pshufd	$0x39, %xmm2, %xmm3
	mulps	%xmm0, %xmm2

	addps	%xmm5, %xmm10
	addps	%xmm6, %xmm11
	pshufd	$0x39, %xmm3, %xmm4
	mulps	%xmm0, %xmm3
	mulps	%xmm0, %xmm4

	movaps	-20 * SIZE(AO), %xmm0

	addps	%xmm1, %xmm12
	movaps	 -8 * SIZE(BO), %xmm1
	addps	%xmm2, %xmm13
	pshufd	$0x39, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	pshufd	$0x39, %xmm2, %xmm5
	mulps	%xmm0, %xmm2

	addps	%xmm3, %xmm14
	addps	%xmm4, %xmm15
	pshufd	$0x39, %xmm5, %xmm6
	mulps	%xmm0, %xmm5
	mulps	%xmm0, %xmm6

	addps	%xmm1, %xmm8
	movaps	 -4 * SIZE(BO), %xmm1
	addps	%xmm2, %xmm9
	subq	$-32 * SIZE, BO
	pshufd	$0x39, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	pshufd	$0x39, %xmm2, %xmm3
	mulps	%xmm0, %xmm2

	addps	%xmm5, %xmm10
	addps	%xmm6, %xmm11
	pshufd	$0x39, %xmm3, %xmm4
	mulps	%xmm0, %xmm3
	mulps	%xmm0, %xmm4

	movaps	-16 * SIZE(AO), %xmm0

	subq	$-16 * SIZE, AO
	subq	$1, %rax
	BRANCH
	jg	.L12
	ALIGN_3

.L15:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	BRANCH
	je	.L18
	ALIGN_3

.L16:
	addps	%xmm1, %xmm12
	movaps	-32 * SIZE(BO), %xmm1
	addps	%xmm2, %xmm13
	pshufd	$0x39, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	pshufd	$0x39, %xmm2, %xmm5
	mulps	%xmm0, %xmm2

	addps	%xmm3, %xmm14
	addps	%xmm4, %xmm15
	pshufd	$0x39, %xmm5, %xmm6
	mulps	%xmm0, %xmm5
	mulps	%xmm0, %xmm6

	addps	%xmm1, %xmm8
	movaps	-28 * SIZE(BO), %xmm1
	addps	%xmm2, %xmm9
	pshufd	$0x39, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	pshufd	$0x39, %xmm2, %xmm3
	mulps	%xmm0, %xmm2

	addps	%xmm5, %xmm10
	addps	%xmm6, %xmm11
	pshufd	$0x39, %xmm3, %xmm4
	mulps	%xmm0, %xmm3
	mulps	%xmm0, %xmm4

	movaps	-28 * SIZE(AO), %xmm0

	addq	$4 * SIZE, AO
	addq	$8 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L16
	ALIGN_3

.L18:
#if defined(LN) || defined(RT)
	movq	KK, %rax
#ifdef LN
	subq	$4, %rax
#else
	subq	$8, %rax
#endif

	leaq	(, %rax, SIZE), %rax

	movq	AORIG, AO
	leaq	(AO, %rax, 4), AO
	leaq	(B,  %rax, 8), BO
#endif

	addps	%xmm1, %xmm12
	addps	%xmm2, %xmm13
	addps	%xmm3, %xmm14
	addps	%xmm4, %xmm15

#if defined(LN) || defined(LT)
	movaps	%xmm8, %xmm4
	shufps	$0x88, %xmm9,  %xmm8
	movaps	%xmm10, %xmm5
	shufps	$0x88, %xmm11, %xmm10
	shufps	$0xdd, %xmm11, %xmm4
	shufps	$0xdd, %xmm9,  %xmm5

	movaps	%xmm8, %xmm6
	shufps	$0x88, %xmm10, %xmm8
	shufps	$0xdd, %xmm6,  %xmm10

	movaps	%xmm4, %xmm9
	movaps	%xmm5, %xmm11
	shufps	$0x22, %xmm5, %xmm9
	shufps	$0x77, %xmm4, %xmm11

	movaps	%xmm12, %xmm4
	shufps	$0x88, %xmm13,  %xmm12
	movaps	%xmm14, %xmm5
	shufps	$0x88, %xmm15, %xmm14
	shufps	$0xdd, %xmm15, %xmm4
	shufps	$0xdd, %xmm13,  %xmm5

	movaps	%xmm12, %xmm6
	shufps	$0x88, %xmm14, %xmm12
	shufps	$0xdd, %xmm6,  %xmm14

	movaps	%xmm4, %xmm13
	movaps	%xmm5, %xmm15
	shufps	$0x22, %xmm5, %xmm13
	shufps	$0x77, %xmm4, %xmm15

	movaps	-32 * SIZE(BO), %xmm0
	movaps	-28 * SIZE(BO), %xmm4
	movaps	-24 * SIZE(BO), %xmm1
	movaps	-20 * SIZE(BO), %xmm5
	movaps	-16 * SIZE(BO), %xmm2
	movaps	-12 * SIZE(BO), %xmm6
	movaps	 -8 * SIZE(BO), %xmm3
	movaps	 -4 * SIZE(BO), %xmm7

#else
	movaps	%xmm9, %xmm4
	shufps	$0xd8, %xmm8, %xmm9
	shufps	$0xd8, %xmm11, %xmm8
	shufps	$0xd8, %xmm10, %xmm11
	shufps	$0xd8, %xmm4, %xmm10

	movaps	%xmm8, %xmm4
	shufps	$0xd8, %xmm10, %xmm8
	shufps	$0xd8, %xmm4, %xmm10
	movaps	%xmm9, %xmm5
	shufps	$0xd8, %xmm11, %xmm9
	shufps	$0xd8, %xmm5, %xmm11

	movaps	%xmm13, %xmm4
	shufps	$0xd8, %xmm12, %xmm13
	shufps	$0xd8, %xmm15, %xmm12
	shufps	$0xd8, %xmm14, %xmm15
	shufps	$0xd8, %xmm4, %xmm14

	movaps	%xmm12, %xmm4
	shufps	$0xd8, %xmm14, %xmm12
	shufps	$0xd8, %xmm4, %xmm14
	movaps	%xmm13, %xmm5
	shufps	$0xd8, %xmm15, %xmm13
	shufps	$0xd8, %xmm5, %xmm15

	movaps	-32 * SIZE(AO), %xmm0
	movaps	-28 * SIZE(AO), %xmm1
	movaps	-24 * SIZE(AO), %xmm2
	movaps	-20 * SIZE(AO), %xmm3
	movaps	-16 * SIZE(AO), %xmm4
	movaps	-12 * SIZE(AO), %xmm5
	movaps	 -8 * SIZE(AO), %xmm6
	movaps	 -4 * SIZE(AO), %xmm7
#endif

	subps	%xmm8,  %xmm0
	subps	%xmm9,  %xmm1
	subps	%xmm10, %xmm2
	subps	%xmm11, %xmm3
	subps	%xmm12, %xmm4
	subps	%xmm13, %xmm5
	subps	%xmm14, %xmm6
	subps	%xmm15, %xmm7

#ifdef LN
	movaps	-20 * SIZE(AO), %xmm8

	pshufd	 $0xff, %xmm8, %xmm15
	mulps	 %xmm15, %xmm3
	mulps	 %xmm15, %xmm7
	pshufd	 $0xaa, %xmm8, %xmm15
	mulps	 %xmm3,  %xmm15
	subps	 %xmm15, %xmm2
	pshufd	 $0xaa, %xmm8, %xmm15
	mulps	 %xmm7,  %xmm15
	subps	 %xmm15, %xmm6
	pshufd	 $0x55, %xmm8, %xmm15
	mulps	 %xmm3,  %xmm15
	subps	 %xmm15, %xmm1
	pshufd	 $0x55, %xmm8, %xmm15
	mulps	 %xmm7,  %xmm15
	subps	 %xmm15, %xmm5
	pshufd	 $0x00, %xmm8, %xmm15
	mulps	 %xmm3,  %xmm15
	subps	 %xmm15, %xmm0
	pshufd	 $0x00, %xmm8, %xmm15
	mulps	 %xmm7,  %xmm15
	subps	 %xmm15, %xmm4

	movaps	-24 * SIZE(AO), %xmm8

	pshufd	 $0xaa, %xmm8, %xmm15
	mulps	 %xmm15, %xmm2
	mulps	 %xmm15, %xmm6
	pshufd	 $0x55, %xmm8, %xmm15
	mulps	 %xmm2,  %xmm15
	subps	 %xmm15, %xmm1
	pshufd	 $0x55, %xmm8, %xmm15
	mulps	 %xmm6,  %xmm15
	subps	 %xmm15, %xmm5
	pshufd	 $0x00, %xmm8, %xmm15
	mulps	 %xmm2,  %xmm15
	subps	 %xmm15, %xmm0
	pshufd	 $0x00, %xmm8, %xmm15
	mulps	 %xmm6,  %xmm15
	subps	 %xmm15, %xmm4

	movaps	-28 * SIZE(AO), %xmm8

	pshufd	 $0x55, %xmm8, %xmm15
	mulps	 %xmm15, %xmm1
	mulps	 %xmm15, %xmm5
	pshufd	 $0x00, %xmm8, %xmm15
	mulps	 %xmm1,  %xmm15
	subps	 %xmm15, %xmm0
	pshufd	 $0x00, %xmm8, %xmm15
	mulps	 %xmm5,  %xmm15
	subps	 %xmm15, %xmm4

	movaps	-32 * SIZE(AO), %xmm8

	pshufd	 $0x00, %xmm8, %xmm15
	mulps	 %xmm15, %xmm0
	mulps	 %xmm15, %xmm4
#endif

#ifdef LT
	movaps	-32 * SIZE(AO), %xmm8

	pshufd	 $0x00, %xmm8, %xmm15
	mulps	 %xmm15, %xmm0
	mulps	 %xmm15, %xmm4
	pshufd	 $0x55, %xmm8, %xmm15
	mulps	 %xmm0,  %xmm15
	subps	 %xmm15, %xmm1
	pshufd	 $0x55, %xmm8, %xmm15
	mulps	 %xmm4,  %xmm15
	subps	 %xmm15, %xmm5
	pshufd	 $0xaa, %xmm8, %xmm15
	mulps	 %xmm0,  %xmm15
	subps	 %xmm15, %xmm2
	pshufd	 $0xaa, %xmm8, %xmm15
	mulps	 %xmm4,  %xmm15
	subps	 %xmm15, %xmm6
	pshufd	 $0xff, %xmm8, %xmm15
	mulps	 %xmm0,  %xmm15
	subps	 %xmm15, %xmm3
	pshufd	 $0xff, %xmm8, %xmm15
	mulps	 %xmm4,  %xmm15
	subps	 %xmm15, %xmm7

	movaps	-28 * SIZE(AO), %xmm8

	pshufd	 $0x55, %xmm8, %xmm15
	mulps	 %xmm15, %xmm1
	mulps	 %xmm15, %xmm5
	pshufd	 $0xaa, %xmm8, %xmm15
	mulps	 %xmm1,  %xmm15
	subps	 %xmm15, %xmm2
	pshufd	 $0xaa, %xmm8, %xmm15
	mulps	 %xmm5,  %xmm15
	subps	 %xmm15, %xmm6
	pshufd	 $0xff, %xmm8, %xmm15
	mulps	 %xmm1,  %xmm15
	subps	 %xmm15, %xmm3
	pshufd	 $0xff, %xmm8, %xmm15
	mulps	 %xmm5,  %xmm15
	subps	 %xmm15, %xmm7

	movaps	-24 * SIZE(AO), %xmm8

	pshufd	 $0xaa, %xmm8, %xmm15
	mulps	 %xmm15, %xmm2
	mulps	 %xmm15, %xmm6
	pshufd	 $0xff, %xmm8, %xmm15
	mulps	 %xmm2,  %xmm15
	subps	 %xmm15, %xmm3
	pshufd	 $0xff, %xmm8, %xmm15
	mulps	 %xmm6,  %xmm15
	subps	 %xmm15, %xmm7

	movaps	-20 * SIZE(AO), %xmm8

	pshufd	 $0xff, %xmm8, %xmm15
	mulps	 %xmm15, %xmm3
	mulps	 %xmm15, %xmm7
#endif

#ifdef RN
	movaps	-32 * SIZE(BO), %xmm8

	pshufd	 $0x00, %xmm8, %xmm15
	mulps	 %xmm15, %xmm0
	pshufd	 $0x55,  %xmm8, %xmm15
	mulps	 %xmm0,  %xmm15
	subps	 %xmm15, %xmm1
	pshufd	 $0xaa,  %xmm8, %xmm15
	mulps	 %xmm0,  %xmm15
	subps	 %xmm15, %xmm2
	pshufd	 $0xff,  %xmm8, %xmm15
	mulps	 %xmm0,  %xmm15
	subps	 %xmm15, %xmm3

	movaps	-28 * SIZE(BO), %xmm8

	pshufd	 $0x00,  %xmm8, %xmm15
	mulps	 %xmm0,  %xmm15
	subps	 %xmm15, %xmm4
	pshufd	 $0x55,  %xmm8, %xmm15
	mulps	 %xmm0,  %xmm15
	subps	 %xmm15, %xmm5
	pshufd	 $0xaa,  %xmm8, %xmm15
	mulps	 %xmm0,  %xmm15
	subps	 %xmm15, %xmm6
	pshufd	 $0xff,  %xmm8, %xmm15
	mulps	 %xmm0,  %xmm15
	subps	 %xmm15, %xmm7

	movaps	-24 * SIZE(BO), %xmm8

	pshufd	 $0x55, %xmm8, %xmm15
	mulps	 %xmm15, %xmm1
	pshufd	 $0xaa,  %xmm8, %xmm15
	mulps	 %xmm1,  %xmm15
	subps	 %xmm15, %xmm2
	pshufd	 $0xff,  %xmm8, %xmm15
	mulps	 %xmm1,  %xmm15
	subps	 %xmm15, %xmm3

	movaps	-20 * SIZE(BO), %xmm8

	pshufd	 $0x00,  %xmm8, %xmm15
	mulps	 %xmm1,  %xmm15
	subps	 %xmm15, %xmm4
	pshufd	 $0x55,  %xmm8, %xmm15
	mulps	 %xmm1,  %xmm15
	subps	 %xmm15, %xmm5
	pshufd	 $0xaa,  %xmm8, %xmm15
	mulps	 %xmm1,  %xmm15
	subps	 %xmm15, %xmm6
	pshufd	 $0xff,  %xmm8, %xmm15
	mulps	 %xmm1,  %xmm15
	subps	 %xmm15, %xmm7

	movaps	-16 * SIZE(BO), %xmm8

	pshufd	 $0xaa, %xmm8, %xmm15
	mulps	 %xmm15, %xmm2
	pshufd	 $0xff,  %xmm8, %xmm15
	mulps	 %xmm2,  %xmm15
	subps	 %xmm15, %xmm3

	movaps	-12 * SIZE(BO), %xmm8

	pshufd	 $0x00,  %xmm8, %xmm15
	mulps	 %xmm2,  %xmm15
	subps	 %xmm15, %xmm4
	pshufd	 $0x55,  %xmm8, %xmm15
	mulps	 %xmm2,  %xmm15
	subps	 %xmm15, %xmm5
	pshufd	 $0xaa,  %xmm8, %xmm15
	mulps	 %xmm2,  %xmm15
	subps	 %xmm15, %xmm6
	pshufd	 $0xff,  %xmm8, %xmm15
	mulps	 %xmm2,  %xmm15
	subps	 %xmm15, %xmm7

	movaps	 -8 * SIZE(BO), %xmm8

	pshufd	 $0xff, %xmm8, %xmm15
	mulps	 %xmm15, %xmm3

	movaps	 -4 * SIZE(BO), %xmm8

	pshufd	 $0x00,  %xmm8, %xmm15
	mulps	 %xmm3,  %xmm15
	subps	 %xmm15, %xmm4
	pshufd	 $0x55,  %xmm8, %xmm15
	mulps	 %xmm3,  %xmm15
	subps	 %xmm15, %xmm5
	pshufd	 $0xaa,  %xmm8, %xmm15
	mulps	 %xmm3,  %xmm15
	subps	 %xmm15, %xmm6
	pshufd	 $0xff,  %xmm8, %xmm15
	mulps	 %xmm3,  %xmm15
	subps	 %xmm15, %xmm7

	movaps	  4 * SIZE(BO), %xmm8

	pshufd	 $0x00, %xmm8, %xmm15
	mulps	 %xmm15, %xmm4
	pshufd	 $0x55,  %xmm8, %xmm15
	mulps	 %xmm4,  %xmm15
	subps	 %xmm15, %xmm5
	pshufd	 $0xaa,  %xmm8, %xmm15
	mulps	 %xmm4,  %xmm15
	subps	 %xmm15, %xmm6
	pshufd	 $0xff,  %xmm8, %xmm15
	mulps	 %xmm4,  %xmm15
	subps	 %xmm15, %xmm7

	movaps	 12 * SIZE(BO), %xmm8

	pshufd	 $0x55, %xmm8, %xmm15
	mulps	 %xmm15, %xmm5
	pshufd	 $0xaa,  %xmm8, %xmm15
	mulps	 %xmm5,  %xmm15
	subps	 %xmm15, %xmm6
	pshufd	 $0xff,  %xmm8, %xmm15
	mulps	 %xmm5,  %xmm15
	subps	 %xmm15, %xmm7

	movaps	 20 * SIZE(BO), %xmm8

	pshufd	 $0xaa, %xmm8, %xmm15
	mulps	 %xmm15, %xmm6
	pshufd	 $0xff,  %xmm8, %xmm15
	mulps	 %xmm6,  %xmm15
	subps	 %xmm15, %xmm7

	movaps	 28 * SIZE(BO), %xmm8

	pshufd	 $0xff, %xmm8, %xmm15
	mulps	 %xmm15, %xmm7
#endif

#ifdef RT
	movaps	 28 * SIZE(BO), %xmm8

	pshufd	 $0xff, %xmm8, %xmm15
	mulps	 %xmm15, %xmm7
	pshufd	 $0xaa,  %xmm8, %xmm15
	mulps	 %xmm7,  %xmm15
	subps	 %xmm15, %xmm6
	pshufd	 $0x55,  %xmm8, %xmm15
	mulps	 %xmm7,  %xmm15
	subps	 %xmm15, %xmm5
	pshufd	 $0x00,  %xmm8, %xmm15
	mulps	 %xmm7,  %xmm15
	subps	 %xmm15, %xmm4

	movaps	 24 * SIZE(BO), %xmm8

	pshufd	 $0xff,  %xmm8, %xmm15
	mulps	 %xmm7,  %xmm15
	subps	 %xmm15, %xmm3
	pshufd	 $0xaa,  %xmm8, %xmm15
	mulps	 %xmm7,  %xmm15
	subps	 %xmm15, %xmm2
	pshufd	 $0x55,  %xmm8, %xmm15
	mulps	 %xmm7,  %xmm15
	subps	 %xmm15, %xmm1
	pshufd	 $0x00,  %xmm8, %xmm15
	mulps	 %xmm7,  %xmm15
	subps	 %xmm15, %xmm0

	movaps	 20 * SIZE(BO), %xmm8

	pshufd	 $0xaa, %xmm8, %xmm15
	mulps	 %xmm15, %xmm6
	pshufd	 $0x55,  %xmm8, %xmm15
	mulps	 %xmm6,  %xmm15
	subps	 %xmm15, %xmm5
	pshufd	 $0x00,  %xmm8, %xmm15
	mulps	 %xmm6,  %xmm15
	subps	 %xmm15, %xmm4

	movaps	 16 * SIZE(BO), %xmm8

	pshufd	 $0xff,  %xmm8, %xmm15
	mulps	 %xmm6,  %xmm15
	subps	 %xmm15, %xmm3
	pshufd	 $0xaa,  %xmm8, %xmm15
	mulps	 %xmm6,  %xmm15
	subps	 %xmm15, %xmm2
	pshufd	 $0x55,  %xmm8, %xmm15
	mulps	 %xmm6,  %xmm15
	subps	 %xmm15, %xmm1
	pshufd	 $0x00,  %xmm8, %xmm15
	mulps	 %xmm6,  %xmm15
	subps	 %xmm15, %xmm0

	movaps	 12 * SIZE(BO), %xmm8

	pshufd	 $0x55, %xmm8, %xmm15
	mulps	 %xmm15, %xmm5
	pshufd	 $0x00,  %xmm8, %xmm15
	mulps	 %xmm5,  %xmm15
	subps	 %xmm15, %xmm4

	movaps	  8 * SIZE(BO), %xmm8

	pshufd	 $0xff,  %xmm8, %xmm15
	mulps	 %xmm5,  %xmm15
	subps	 %xmm15, %xmm3
	pshufd	 $0xaa,  %xmm8, %xmm15
	mulps	 %xmm5,  %xmm15
	subps	 %xmm15, %xmm2
	pshufd	 $0x55,  %xmm8, %xmm15
	mulps	 %xmm5,  %xmm15
	subps	 %xmm15, %xmm1
	pshufd	 $0x00,  %xmm8, %xmm15
	mulps	 %xmm5,  %xmm15
	subps	 %xmm15, %xmm0

	movaps	  4 * SIZE(BO), %xmm8

	pshufd	 $0x00, %xmm8, %xmm15
	mulps	 %xmm15, %xmm4

	movaps	  0 * SIZE(BO), %xmm8

	pshufd	 $0xff,  %xmm8, %xmm15
	mulps	 %xmm4,  %xmm15
	subps	 %xmm15, %xmm3
	pshufd	 $0xaa,  %xmm8, %xmm15
	mulps	 %xmm4,  %xmm15
	subps	 %xmm15, %xmm2
	pshufd	 $0x55,  %xmm8, %xmm15
	mulps	 %xmm4,  %xmm15
	subps	 %xmm15, %xmm1
	pshufd	 $0x00,  %xmm8, %xmm15
	mulps	 %xmm4,  %xmm15
	subps	 %xmm15, %xmm0

	movaps	 -8 * SIZE(BO), %xmm8

	pshufd	 $0xff, %xmm8, %xmm15
	mulps	 %xmm15, %xmm3
	pshufd	 $0xaa,  %xmm8, %xmm15
	mulps	 %xmm3,  %xmm15
	subps	 %xmm15, %xmm2
	pshufd	 $0x55,  %xmm8, %xmm15
	mulps	 %xmm3,  %xmm15
	subps	 %xmm15, %xmm1
	pshufd	 $0x00,  %xmm8, %xmm15
	mulps	 %xmm3,  %xmm15
	subps	 %xmm15, %xmm0

	movaps	-16 * SIZE(BO), %xmm8

	pshufd	 $0xaa, %xmm8, %xmm15
	mulps	 %xmm15, %xmm2
	pshufd	 $0x55,  %xmm8, %xmm15
	mulps	 %xmm2,  %xmm15
	subps	 %xmm15, %xmm1
	pshufd	 $0x00,  %xmm8, %xmm15
	mulps	 %xmm2,  %xmm15
	subps	 %xmm15, %xmm0

	movaps	-24 * SIZE(BO), %xmm8

	pshufd	 $0x55, %xmm8, %xmm15
	mulps	 %xmm15, %xmm1
	pshufd	 $0x00,  %xmm8, %xmm15
	mulps	 %xmm1,  %xmm15
	subps	 %xmm15, %xmm0

	movaps	-32 * SIZE(BO), %xmm8

	pshufd	 $0x00, %xmm8, %xmm15
	mulps	 %xmm15, %xmm0
#endif

#ifdef LN
	subq	$4 * SIZE, CO1
	subq	$4 * SIZE, CO2
#endif

#if defined(LN) || defined(LT)
	movaps	%xmm0, -32 * SIZE(BO)
	movaps	%xmm4, -28 * SIZE(BO)
	movaps	%xmm1, -24 * SIZE(BO)
	movaps	%xmm5, -20 * SIZE(BO)
	movaps	%xmm2, -16 * SIZE(BO)
	movaps	%xmm6, -12 * SIZE(BO)
	movaps	%xmm3,  -8 * SIZE(BO)
	movaps	%xmm7,  -4 * SIZE(BO)

	movaps	%xmm0, %xmm8
	shufps	$0x88, %xmm1, %xmm0
	shufps	$0xdd, %xmm8, %xmm1

	movaps	%xmm2, %xmm9
	shufps	$0x88, %xmm3, %xmm2
	shufps	$0xdd, %xmm9, %xmm3

	movaps	%xmm0, %xmm8
	shufps	$0x88, %xmm2, %xmm0
	movaps	%xmm1, %xmm9
	shufps	$0x22, %xmm3, %xmm1
	shufps	$0xdd, %xmm2, %xmm8
	movaps	%xmm8, %xmm2
	shufps	$0x77, %xmm3, %xmm9
	movaps	%xmm9, %xmm3

	movaps	%xmm4, %xmm8
	shufps	$0x88, %xmm5, %xmm4
	shufps	$0xdd, %xmm8, %xmm5

	movaps	%xmm6, %xmm9
	shufps	$0x88, %xmm7, %xmm6
	shufps	$0xdd, %xmm9, %xmm7

	movaps	%xmm4, %xmm8
	shufps	$0x88, %xmm6, %xmm4
	movaps	%xmm5, %xmm9
	shufps	$0x22, %xmm7, %xmm5
	shufps	$0xdd, %xmm6, %xmm8
	movaps	%xmm8, %xmm6
	shufps	$0x77, %xmm7, %xmm9
	movaps	%xmm9, %xmm7

#else
	movaps	%xmm0, -32 * SIZE(AO)
	movaps	%xmm1, -28 * SIZE(AO)
	movaps	%xmm2, -24 * SIZE(AO)
	movaps	%xmm3, -20 * SIZE(AO)
	movaps	%xmm4, -16 * SIZE(AO)
	movaps	%xmm5, -12 * SIZE(AO)
	movaps	%xmm6,  -8 * SIZE(AO)
	movaps	%xmm7,  -4 * SIZE(AO)
#endif

	leaq	(LDC, LDC, 2), %rax

	movsd	%xmm0,  0 * SIZE(CO1)
	movhps	%xmm0,  2 * SIZE(CO1)
	movsd	%xmm1,  0 * SIZE(CO1, LDC,  1)
	movhps	%xmm1,  2 * SIZE(CO1, LDC,  1)

	movsd	%xmm2,  0 * SIZE(CO1, LDC,  2)
	movhps	%xmm2,  2 * SIZE(CO1, LDC,  2)
	movsd	%xmm3,  0 * SIZE(CO1, %rax, 1)
	movhps	%xmm3,  2 * SIZE(CO1, %rax, 1)

	movsd	%xmm4,  0 * SIZE(CO2)
	movhps	%xmm4,  2 * SIZE(CO2)
	movsd	%xmm5,  0 * SIZE(CO2, LDC,  1)
	movhps	%xmm5,  2 * SIZE(CO2, LDC,  1)

	movsd	%xmm6,  0 * SIZE(CO2, LDC,  2)
	movhps	%xmm6,  2 * SIZE(CO2, LDC,  2)
	movsd	%xmm7,  0 * SIZE(CO2, %rax, 1)
	movhps	%xmm7,  2 * SIZE(CO2, %rax, 1)

#ifndef LN
	addq	$4 * SIZE, CO1
	addq	$4 * SIZE, CO2
#endif

#if defined(LT) || defined(RN)
	movq	K,  %rax
	subq	KK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 8), BO
#endif

#ifdef LN
	subq	$4, KK
#endif

#ifdef LT
	addq	$4, KK
#endif

#ifdef RT
       movq	K, %rax
       salq	$2 + BASE_SHIFT, %rax
       addq	%rax, AORIG
#endif

	decq	I
	BRANCH
	jg	.L11
	ALIGN_4

.L20:
	testq	$2, M
	BRANCH
	jle	.L30

#ifdef LN
       movq	K, %rax
       salq	$1 + BASE_SHIFT, %rax
       subq	%rax, AORIG
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	movq	AORIG, AO
	leaq	(AO, %rax, 2), AO
	leaq	(B,  %rax, 8), BO
#else
	movq	B, BO
#endif

	xorps	%xmm1, %xmm1
	movddup	-32 * SIZE(AO), %xmm0
	xorps	%xmm2, %xmm2
	movaps	-32 * SIZE(BO), %xmm5
	xorps	%xmm3, %xmm3
	xorps	%xmm4, %xmm4

	xorps	%xmm8,  %xmm8
	xorps	%xmm9,  %xmm9
	xorps	%xmm10, %xmm10
	xorps	%xmm11, %xmm11

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	sarq	$2, %rax
	NOBRANCH
	jle	.L25
	ALIGN_3

.L22:
	addps	%xmm1, %xmm8
	pshufd	$0x50, %xmm5, %xmm1
	mulps	%xmm0, %xmm1
	addps	%xmm2, %xmm9
	pshufd	$0xfa, %xmm5, %xmm2
	mulps	%xmm0, %xmm2
	movaps	-28 * SIZE(BO), %xmm5

	addps	%xmm3, %xmm10
	pshufd	$0x50, %xmm5, %xmm3
	mulps	%xmm0, %xmm3
	addps	%xmm4, %xmm11
	pshufd	$0xfa, %xmm5, %xmm4
	mulps	%xmm0, %xmm4
	movaps	-24 * SIZE(BO), %xmm5

	movddup	-30 * SIZE(AO), %xmm0

	addps	%xmm1, %xmm8
	pshufd	$0x50, %xmm5, %xmm1
	mulps	%xmm0, %xmm1
	addps	%xmm2, %xmm9
	pshufd	$0xfa, %xmm5, %xmm2
	mulps	%xmm0, %xmm2
	movaps	-20 * SIZE(BO), %xmm5

	addps	%xmm3, %xmm10
	pshufd	$0x50, %xmm5, %xmm3
	mulps	%xmm0, %xmm3
	addps	%xmm4, %xmm11
	pshufd	$0xfa, %xmm5, %xmm4
	mulps	%xmm0, %xmm4
	movaps	-16 * SIZE(BO), %xmm5

	movddup	-28 * SIZE(AO), %xmm0

	addps	%xmm1, %xmm8
	pshufd	$0x50, %xmm5, %xmm1
	mulps	%xmm0, %xmm1
	addps	%xmm2, %xmm9
	pshufd	$0xfa, %xmm5, %xmm2
	mulps	%xmm0, %xmm2
	movaps	-12 * SIZE(BO), %xmm5

	addps	%xmm3, %xmm10
	pshufd	$0x50, %xmm5, %xmm3
	mulps	%xmm0, %xmm3
	addps	%xmm4, %xmm11
	pshufd	$0xfa, %xmm5, %xmm4
	mulps	%xmm0, %xmm4
	movaps	 -8 * SIZE(BO), %xmm5

	movddup	-26 * SIZE(AO), %xmm0

	addps	%xmm1, %xmm8
	pshufd	$0x50, %xmm5, %xmm1
	mulps	%xmm0, %xmm1
	addps	%xmm2, %xmm9
	pshufd	$0xfa, %xmm5, %xmm2
	mulps	%xmm0, %xmm2
	movaps	 -4 * SIZE(BO), %xmm5

	addps	%xmm3, %xmm10
	pshufd	$0x50, %xmm5, %xmm3
	mulps	%xmm0, %xmm3
	addps	%xmm4, %xmm11
	pshufd	$0xfa, %xmm5, %xmm4
	mulps	%xmm0, %xmm4
	movaps	 0 * SIZE(BO), %xmm5

	movddup	-24 * SIZE(AO), %xmm0

	subq	$-32 * SIZE, BO
	subq	$ -8 * SIZE, AO

	subq	$1, %rax
	BRANCH
	jg	.L22
	ALIGN_3

.L25:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	BRANCH
	je	.L28
	ALIGN_3

.L26:
	addps	%xmm1, %xmm8
	pshufd	$0x50, %xmm5, %xmm1
	mulps	%xmm0, %xmm1
	addps	%xmm2, %xmm9
	pshufd	$0xfa, %xmm5, %xmm2
	mulps	%xmm0, %xmm2
	movaps	-28 * SIZE(BO), %xmm5

	addps	%xmm3, %xmm10
	pshufd	$0x50, %xmm5, %xmm3
	mulps	%xmm0, %xmm3
	addps	%xmm4, %xmm11
	pshufd	$0xfa, %xmm5, %xmm4
	mulps	%xmm0, %xmm4
	movaps	-24 * SIZE(BO), %xmm5

	movddup	-30 * SIZE(AO), %xmm0

	addq	$2 * SIZE, AO
	addq	$8 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L26
	ALIGN_3

.L28:
#if defined(LN) || defined(RT)
	movq	KK, %rax
#ifdef LN
	subq	$2, %rax
#else
	subq	$8, %rax
#endif

	leaq	(, %rax, SIZE), %rax

	movq	AORIG, AO
	leaq	(AO, %rax, 2), AO
	leaq	(B,  %rax, 8), BO
#endif

	addps	%xmm1, %xmm8
	addps	%xmm2, %xmm9
	addps	%xmm3, %xmm10
	addps	%xmm4, %xmm11

#if defined(LN) || defined(LT)
	movaps	%xmm8, %xmm4
	shufps	$0x88, %xmm9, %xmm8
	shufps	$0xdd, %xmm9, %xmm4

	movaps	%xmm10, %xmm5
	shufps	$0x88, %xmm11, %xmm10
	shufps	$0xdd, %xmm11, %xmm5

	movaps	-32 * SIZE(BO), %xmm0
	movaps	-28 * SIZE(BO), %xmm2
	movaps	-24 * SIZE(BO), %xmm1
	movaps	-20 * SIZE(BO), %xmm3

	subps	%xmm8,  %xmm0
	subps	%xmm4,  %xmm1
	subps	%xmm10, %xmm2
	subps	%xmm5,  %xmm3
#else
	movaps	-32 * SIZE(AO), %xmm0
	movaps	-28 * SIZE(AO), %xmm2
	movaps	-24 * SIZE(AO), %xmm4
	movaps	-20 * SIZE(AO), %xmm6

	subps	%xmm8,  %xmm0
	subps	%xmm9,  %xmm2
	subps	%xmm10, %xmm4
	subps	%xmm11, %xmm6

	movhlps	%xmm0, %xmm1
	movhlps	%xmm2, %xmm3
	movhlps	%xmm4, %xmm5
	movhlps	%xmm6, %xmm7
#endif

#ifdef LN
	movaps	-32 * SIZE(AO), %xmm8

	pshufd	 $0xff, %xmm8, %xmm15
	mulps	 %xmm15, %xmm1
	mulps	 %xmm15, %xmm3

	pshufd	 $0xaa, %xmm8, %xmm15
	mulps	 %xmm1,  %xmm15
	subps	 %xmm15, %xmm0
	pshufd	 $0xaa, %xmm8, %xmm15
	mulps	 %xmm3,  %xmm15
	subps	 %xmm15, %xmm2

	pshufd	 $0x00, %xmm8, %xmm15
	mulps	 %xmm15, %xmm0
	mulps	 %xmm15, %xmm2
#endif

#ifdef LT
	movaps	-32 * SIZE(AO), %xmm8

	pshufd	 $0x00, %xmm8, %xmm15
	mulps	 %xmm15, %xmm0
	mulps	 %xmm15, %xmm2

	pshufd	 $0x55, %xmm8, %xmm15
	mulps	 %xmm0,  %xmm15
	subps	 %xmm15, %xmm1
	pshufd	 $0x55, %xmm8, %xmm15
	mulps	 %xmm2,  %xmm15
	subps	 %xmm15, %xmm3

	pshufd	 $0xff, %xmm8, %xmm15
	mulps	 %xmm15, %xmm1
	mulps	 %xmm15, %xmm3
#endif

#ifdef RN
	movaps	-32 * SIZE(BO), %xmm8

	pshufd	 $0x00, %xmm8, %xmm15
	mulps	 %xmm15, %xmm0
	pshufd	 $0x55,  %xmm8, %xmm15
	mulps	 %xmm0,  %xmm15
	subps	 %xmm15, %xmm1
	pshufd	 $0xaa,  %xmm8, %xmm15
	mulps	 %xmm0,  %xmm15
	subps	 %xmm15, %xmm2
	pshufd	 $0xff,  %xmm8, %xmm15
	mulps	 %xmm0,  %xmm15
	subps	 %xmm15, %xmm3

	movaps	-28 * SIZE(BO), %xmm8

	pshufd	 $0x00,  %xmm8, %xmm15
	mulps	 %xmm0,  %xmm15
	subps	 %xmm15, %xmm4
	pshufd	 $0x55,  %xmm8, %xmm15
	mulps	 %xmm0,  %xmm15
	subps	 %xmm15, %xmm5
	pshufd	 $0xaa,  %xmm8, %xmm15
	mulps	 %xmm0,  %xmm15
	subps	 %xmm15, %xmm6
	pshufd	 $0xff,  %xmm8, %xmm15
	mulps	 %xmm0,  %xmm15
	subps	 %xmm15, %xmm7

	movaps	-24 * SIZE(BO), %xmm8

	pshufd	 $0x55, %xmm8, %xmm15
	mulps	 %xmm15, %xmm1
	pshufd	 $0xaa,  %xmm8, %xmm15
	mulps	 %xmm1,  %xmm15
	subps	 %xmm15, %xmm2
	pshufd	 $0xff,  %xmm8, %xmm15
	mulps	 %xmm1,  %xmm15
	subps	 %xmm15, %xmm3

	movaps	-20 * SIZE(BO), %xmm8

	pshufd	 $0x00,  %xmm8, %xmm15
	mulps	 %xmm1,  %xmm15
	subps	 %xmm15, %xmm4
	pshufd	 $0x55,  %xmm8, %xmm15
	mulps	 %xmm1,  %xmm15
	subps	 %xmm15, %xmm5
	pshufd	 $0xaa,  %xmm8, %xmm15
	mulps	 %xmm1,  %xmm15
	subps	 %xmm15, %xmm6
	pshufd	 $0xff,  %xmm8, %xmm15
	mulps	 %xmm1,  %xmm15
	subps	 %xmm15, %xmm7

	movaps	-16 * SIZE(BO), %xmm8

	pshufd	 $0xaa, %xmm8, %xmm15
	mulps	 %xmm15, %xmm2
	pshufd	 $0xff,  %xmm8, %xmm15
	mulps	 %xmm2,  %xmm15
	subps	 %xmm15, %xmm3

	movaps	-12 * SIZE(BO), %xmm8

	pshufd	 $0x00,  %xmm8, %xmm15
	mulps	 %xmm2,  %xmm15
	subps	 %xmm15, %xmm4
	pshufd	 $0x55,  %xmm8, %xmm15
	mulps	 %xmm2,  %xmm15
	subps	 %xmm15, %xmm5
	pshufd	 $0xaa,  %xmm8, %xmm15
	mulps	 %xmm2,  %xmm15
	subps	 %xmm15, %xmm6
	pshufd	 $0xff,  %xmm8, %xmm15
	mulps	 %xmm2,  %xmm15
	subps	 %xmm15, %xmm7

	movaps	 -8 * SIZE(BO), %xmm8

	pshufd	 $0xff, %xmm8, %xmm15
	mulps	 %xmm15, %xmm3

	movaps	 -4 * SIZE(BO), %xmm8

	pshufd	 $0x00,  %xmm8, %xmm15
	mulps	 %xmm3,  %xmm15
	subps	 %xmm15, %xmm4
	pshufd	 $0x55,  %xmm8, %xmm15
	mulps	 %xmm3,  %xmm15
	subps	 %xmm15, %xmm5
	pshufd	 $0xaa,  %xmm8, %xmm15
	mulps	 %xmm3,  %xmm15
	subps	 %xmm15, %xmm6
	pshufd	 $0xff,  %xmm8, %xmm15
	mulps	 %xmm3,  %xmm15
	subps	 %xmm15, %xmm7

	movaps	  4 * SIZE(BO), %xmm8

	pshufd	 $0x00, %xmm8, %xmm15
	mulps	 %xmm15, %xmm4
	pshufd	 $0x55,  %xmm8, %xmm15
	mulps	 %xmm4,  %xmm15
	subps	 %xmm15, %xmm5
	pshufd	 $0xaa,  %xmm8, %xmm15
	mulps	 %xmm4,  %xmm15
	subps	 %xmm15, %xmm6
	pshufd	 $0xff,  %xmm8, %xmm15
	mulps	 %xmm4,  %xmm15
	subps	 %xmm15, %xmm7

	movaps	 12 * SIZE(BO), %xmm8

	pshufd	 $0x55, %xmm8, %xmm15
	mulps	 %xmm15, %xmm5
	pshufd	 $0xaa,  %xmm8, %xmm15
	mulps	 %xmm5,  %xmm15
	subps	 %xmm15, %xmm6
	pshufd	 $0xff,  %xmm8, %xmm15
	mulps	 %xmm5,  %xmm15
	subps	 %xmm15, %xmm7

	movaps	 20 * SIZE(BO), %xmm8

	pshufd	 $0xaa, %xmm8, %xmm15
	mulps	 %xmm15, %xmm6
	pshufd	 $0xff,  %xmm8, %xmm15
	mulps	 %xmm6,  %xmm15
	subps	 %xmm15, %xmm7

	movaps	 28 * SIZE(BO), %xmm8

	pshufd	 $0xff, %xmm8, %xmm15
	mulps	 %xmm15, %xmm7
#endif

#ifdef RT
	movaps	 28 * SIZE(BO), %xmm8

	pshufd	 $0xff, %xmm8, %xmm15
	mulps	 %xmm15, %xmm7
	pshufd	 $0xaa,  %xmm8, %xmm15
	mulps	 %xmm7,  %xmm15
	subps	 %xmm15, %xmm6
	pshufd	 $0x55,  %xmm8, %xmm15
	mulps	 %xmm7,  %xmm15
	subps	 %xmm15, %xmm5
	pshufd	 $0x00,  %xmm8, %xmm15
	mulps	 %xmm7,  %xmm15
	subps	 %xmm15, %xmm4

	movaps	 24 * SIZE(BO), %xmm8

	pshufd	 $0xff,  %xmm8, %xmm15
	mulps	 %xmm7,  %xmm15
	subps	 %xmm15, %xmm3
	pshufd	 $0xaa,  %xmm8, %xmm15
	mulps	 %xmm7,  %xmm15
	subps	 %xmm15, %xmm2
	pshufd	 $0x55,  %xmm8, %xmm15
	mulps	 %xmm7,  %xmm15
	subps	 %xmm15, %xmm1
	pshufd	 $0x00,  %xmm8, %xmm15
	mulps	 %xmm7,  %xmm15
	subps	 %xmm15, %xmm0

	movaps	 20 * SIZE(BO), %xmm8

	pshufd	 $0xaa, %xmm8, %xmm15
	mulps	 %xmm15, %xmm6
	pshufd	 $0x55,  %xmm8, %xmm15
	mulps	 %xmm6,  %xmm15
	subps	 %xmm15, %xmm5
	pshufd	 $0x00,  %xmm8, %xmm15
	mulps	 %xmm6,  %xmm15
	subps	 %xmm15, %xmm4

	movaps	 16 * SIZE(BO), %xmm8

	pshufd	 $0xff,  %xmm8, %xmm15
	mulps	 %xmm6,  %xmm15
	subps	 %xmm15, %xmm3
	pshufd	 $0xaa,  %xmm8, %xmm15
	mulps	 %xmm6,  %xmm15
	subps	 %xmm15, %xmm2
	pshufd	 $0x55,  %xmm8, %xmm15
	mulps	 %xmm6,  %xmm15
	subps	 %xmm15, %xmm1
	pshufd	 $0x00,  %xmm8, %xmm15
	mulps	 %xmm6,  %xmm15
	subps	 %xmm15, %xmm0

	movaps	 12 * SIZE(BO), %xmm8

	pshufd	 $0x55, %xmm8, %xmm15
	mulps	 %xmm15, %xmm5
	pshufd	 $0x00,  %xmm8, %xmm15
	mulps	 %xmm5,  %xmm15
	subps	 %xmm15, %xmm4

	movaps	  8 * SIZE(BO), %xmm8

	pshufd	 $0xff,  %xmm8, %xmm15
	mulps	 %xmm5,  %xmm15
	subps	 %xmm15, %xmm3
	pshufd	 $0xaa,  %xmm8, %xmm15
	mulps	 %xmm5,  %xmm15
	subps	 %xmm15, %xmm2
	pshufd	 $0x55,  %xmm8, %xmm15
	mulps	 %xmm5,  %xmm15
	subps	 %xmm15, %xmm1
	pshufd	 $0x00,  %xmm8, %xmm15
	mulps	 %xmm5,  %xmm15
	subps	 %xmm15, %xmm0

	movaps	  4 * SIZE(BO), %xmm8

	pshufd	 $0x00, %xmm8, %xmm15
	mulps	 %xmm15, %xmm4

	movaps	  0 * SIZE(BO), %xmm8

	pshufd	 $0xff,  %xmm8, %xmm15
	mulps	 %xmm4,  %xmm15
	subps	 %xmm15, %xmm3
	pshufd	 $0xaa,  %xmm8, %xmm15
	mulps	 %xmm4,  %xmm15
	subps	 %xmm15, %xmm2
	pshufd	 $0x55,  %xmm8, %xmm15
	mulps	 %xmm4,  %xmm15
	subps	 %xmm15, %xmm1
	pshufd	 $0x00,  %xmm8, %xmm15
	mulps	 %xmm4,  %xmm15
	subps	 %xmm15, %xmm0

	movaps	 -8 * SIZE(BO), %xmm8

	pshufd	 $0xff, %xmm8, %xmm15
	mulps	 %xmm15, %xmm3
	pshufd	 $0xaa,  %xmm8, %xmm15
	mulps	 %xmm3,  %xmm15
	subps	 %xmm15, %xmm2
	pshufd	 $0x55,  %xmm8, %xmm15
	mulps	 %xmm3,  %xmm15
	subps	 %xmm15, %xmm1
	pshufd	 $0x00,  %xmm8, %xmm15
	mulps	 %xmm3,  %xmm15
	subps	 %xmm15, %xmm0

	movaps	-16 * SIZE(BO), %xmm8

	pshufd	 $0xaa, %xmm8, %xmm15
	mulps	 %xmm15, %xmm2
	pshufd	 $0x55,  %xmm8, %xmm15
	mulps	 %xmm2,  %xmm15
	subps	 %xmm15, %xmm1
	pshufd	 $0x00,  %xmm8, %xmm15
	mulps	 %xmm2,  %xmm15
	subps	 %xmm15, %xmm0

	movaps	-24 * SIZE(BO), %xmm8

	pshufd	 $0x55, %xmm8, %xmm15
	mulps	 %xmm15, %xmm1
	pshufd	 $0x00,  %xmm8, %xmm15
	mulps	 %xmm1,  %xmm15
	subps	 %xmm15, %xmm0

	movaps	-32 * SIZE(BO), %xmm8

	pshufd	 $0x00, %xmm8, %xmm15
	mulps	 %xmm15, %xmm0
#endif

#ifdef LN
	subq	$2 * SIZE, CO1
	subq	$2 * SIZE, CO2
#endif

	leaq	(LDC, LDC, 2), %rax

#if defined(LN) || defined(LT)
	movaps	%xmm0, -32 * SIZE(BO)
	movaps	%xmm2, -28 * SIZE(BO)
	movaps	%xmm1, -24 * SIZE(BO)
	movaps	%xmm3, -20 * SIZE(BO)

	movaps	%xmm0,  %xmm4
	unpcklps %xmm1, %xmm0
	unpckhps %xmm1, %xmm4

	movaps	%xmm2,  %xmm5
	unpcklps %xmm3, %xmm2
	unpckhps %xmm3, %xmm5

	movsd	%xmm0,  (CO1)
	movhps	%xmm0,  (CO1, LDC,  1)
	movsd	%xmm4,  (CO1, LDC,  2)
	movhps	%xmm4,  (CO1, %rax, 1)

	movsd	%xmm2,  (CO2)
	movhps	%xmm2,  (CO2, LDC,  1)
	movsd	%xmm5,  (CO2, LDC,  2)
	movhps	%xmm5,  (CO2, %rax, 1)
#else
	movlhps	%xmm1, %xmm0
	movlhps	%xmm3, %xmm2
	movlhps	%xmm5, %xmm4
	movlhps	%xmm7, %xmm6

	movaps	%xmm0, -32 * SIZE(AO)
	movaps	%xmm2, -28 * SIZE(AO)
	movaps	%xmm4, -24 * SIZE(AO)
	movaps	%xmm6, -20 * SIZE(AO)

	movsd	%xmm0,  (CO1)
	movsd	%xmm1,  (CO1, LDC,  1)
	movsd	%xmm2,  (CO1, LDC,  2)
	movsd	%xmm3,  (CO1, %rax, 1)

	movsd	%xmm4,  (CO2)
	movsd	%xmm5,  (CO2, LDC,  1)
	movsd	%xmm6,  (CO2, LDC,  2)
	movsd	%xmm7,  (CO2, %rax, 1)
#endif

#ifndef LN
	addq	$2 * SIZE, CO1
	addq	$2 * SIZE, CO2
#endif

#if defined(LT) || defined(RN)
	movq	K,  %rax
	subq	KK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 8), BO
#endif

#ifdef LN
	subq	$2, KK
#endif

#ifdef LT
	addq	$2, KK
#endif

#ifdef RT
       movq	K, %rax
       salq	$1 + BASE_SHIFT, %rax
       addq	%rax, AORIG
#endif
	ALIGN_4

.L30:
	testq	$1, M
	BRANCH
	jle	.L39

#ifdef LN
       movq	K, %rax
       salq	$BASE_SHIFT, %rax
       subq	%rax, AORIG
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	movq	AORIG, AO
	leaq	(AO, %rax, 1), AO
	leaq	(B,  %rax, 8), BO
#else
	movq	B, BO
#endif

	xorps	%xmm2, %xmm2
	movsd	-32 * SIZE(AO), %xmm0
	xorps	%xmm3, %xmm3
	xorps	%xmm8,  %xmm8
	xorps	%xmm12, %xmm12

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	sarq	$2, %rax
	NOBRANCH
	jle	.L35
	ALIGN_3

.L32:
	pshufd	$0x00, %xmm0, %xmm1
	addps	%xmm2, %xmm8
	movaps	-32 * SIZE(BO), %xmm2
	mulps	%xmm1, %xmm2
	addps	%xmm3, %xmm12
	movaps	-28 * SIZE(BO), %xmm3
	mulps	%xmm1, %xmm3

	pshufd	$0x55, %xmm0, %xmm1
	movsd	-30 * SIZE(AO), %xmm0
	addps	%xmm2, %xmm8
	movaps	-24 * SIZE(BO), %xmm2
	mulps	%xmm1, %xmm2
	addps	%xmm3, %xmm12
	movaps	-20 * SIZE(BO), %xmm3
	mulps	%xmm1, %xmm3

	pshufd	$0x00, %xmm0, %xmm1
	addps	%xmm2, %xmm8
	movaps	-16 * SIZE(BO), %xmm2
	mulps	%xmm1, %xmm2
	addps	%xmm3, %xmm12
	movaps	-12 * SIZE(BO), %xmm3
	mulps	%xmm1, %xmm3

	pshufd	$0x55, %xmm0, %xmm1
	movsd	-28 * SIZE(AO), %xmm0
	addps	%xmm2, %xmm8
	movaps	 -8 * SIZE(BO), %xmm2
	mulps	%xmm1, %xmm2
	addps	%xmm3, %xmm12
	movaps	 -4 * SIZE(BO), %xmm3
	mulps	%xmm1, %xmm3

	subq	$-32 * SIZE, BO
	subq	$ -4 * SIZE, AO

	subq	$1, %rax
	BRANCH
	jg	.L32
	ALIGN_3

.L35:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	BRANCH
	je	.L38
	ALIGN_3

.L36:
	pshufd	$0x00, %xmm0, %xmm1
	movss	-31 * SIZE(AO), %xmm0
	addps	%xmm2, %xmm8
	movaps	-32 * SIZE(BO), %xmm2
	mulps	%xmm1, %xmm2
	addps	%xmm3, %xmm12
	movaps	-28 * SIZE(BO), %xmm3
	mulps	%xmm1, %xmm3

	addq	$1 * SIZE, AO
	addq	$8 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L36
	ALIGN_3

.L38:
#if defined(LN) || defined(RT)
	movq	KK, %rax
#ifdef LN
	subq	$1, %rax
#else
	subq	$8, %rax
#endif

	leaq	(, %rax, SIZE), %rax

	movq	AORIG, AO
	leaq	(AO, %rax, 1), AO
	leaq	(B,  %rax, 8), BO
#endif

	addps	%xmm2, %xmm8
	addps	%xmm3, %xmm12

#if defined(LN) || defined(LT)
	movaps	-32 * SIZE(BO), %xmm0
	movaps	-28 * SIZE(BO), %xmm4

	subps	%xmm8,  %xmm0
	subps	%xmm12, %xmm4
#else
	movsd	-32 * SIZE(AO), %xmm0
	movhps	-30 * SIZE(AO), %xmm0
	movsd	-28 * SIZE(AO), %xmm4
	movhps	-26 * SIZE(AO), %xmm4

	subps	%xmm8,  %xmm0
	subps	%xmm12, %xmm4

	pshufd	$0xff, %xmm0, %xmm3
	pshufd	$0xaa, %xmm0, %xmm2
	pshufd	$0x55, %xmm0, %xmm1
	pshufd	$0x00, %xmm0, %xmm0

	pshufd	$0xff, %xmm4, %xmm7
	pshufd	$0xaa, %xmm4, %xmm6
	pshufd	$0x55, %xmm4, %xmm5
	pshufd	$0x00, %xmm4, %xmm4
#endif

#if defined(LN) || defined(LT)
	movaps	-32 * SIZE(AO), %xmm8

	pshufd	 $0x00, %xmm8, %xmm15
	mulps	 %xmm15, %xmm0
	mulps	 %xmm15, %xmm4
#endif

#ifdef RN
	movaps	-32 * SIZE(BO), %xmm8

	pshufd	 $0x00, %xmm8, %xmm15
	mulss	 %xmm15, %xmm0
	pshufd	 $0x55,  %xmm8, %xmm15
	mulss	 %xmm0,  %xmm15
	subss	 %xmm15, %xmm1
	pshufd	 $0xaa,  %xmm8, %xmm15
	mulss	 %xmm0,  %xmm15
	subss	 %xmm15, %xmm2
	pshufd	 $0xff,  %xmm8, %xmm15
	mulss	 %xmm0,  %xmm15
	subss	 %xmm15, %xmm3

	movaps	-28 * SIZE(BO), %xmm8

	pshufd	 $0x00,  %xmm8, %xmm15
	mulss	 %xmm0,  %xmm15
	subss	 %xmm15, %xmm4
	pshufd	 $0x55,  %xmm8, %xmm15
	mulss	 %xmm0,  %xmm15
	subss	 %xmm15, %xmm5
	pshufd	 $0xaa,  %xmm8, %xmm15
	mulss	 %xmm0,  %xmm15
	subss	 %xmm15, %xmm6
	pshufd	 $0xff,  %xmm8, %xmm15
	mulss	 %xmm0,  %xmm15
	subss	 %xmm15, %xmm7

	movaps	-24 * SIZE(BO), %xmm8

	pshufd	 $0x55, %xmm8, %xmm15
	mulss	 %xmm15, %xmm1
	pshufd	 $0xaa,  %xmm8, %xmm15
	mulss	 %xmm1,  %xmm15
	subss	 %xmm15, %xmm2
	pshufd	 $0xff,  %xmm8, %xmm15
	mulss	 %xmm1,  %xmm15
	subss	 %xmm15, %xmm3

	movaps	-20 * SIZE(BO), %xmm8

	pshufd	 $0x00,  %xmm8, %xmm15
	mulss	 %xmm1,  %xmm15
	subss	 %xmm15, %xmm4
	pshufd	 $0x55,  %xmm8, %xmm15
	mulss	 %xmm1,  %xmm15
	subss	 %xmm15, %xmm5
	pshufd	 $0xaa,  %xmm8, %xmm15
	mulss	 %xmm1,  %xmm15
	subss	 %xmm15, %xmm6
	pshufd	 $0xff,  %xmm8, %xmm15
	mulss	 %xmm1,  %xmm15
	subss	 %xmm15, %xmm7

	movaps	-16 * SIZE(BO), %xmm8

	pshufd	 $0xaa, %xmm8, %xmm15
	mulss	 %xmm15, %xmm2
	pshufd	 $0xff,  %xmm8, %xmm15
	mulss	 %xmm2,  %xmm15
	subss	 %xmm15, %xmm3

	movaps	-12 * SIZE(BO), %xmm8

	pshufd	 $0x00,  %xmm8, %xmm15
	mulss	 %xmm2,  %xmm15
	subss	 %xmm15, %xmm4
	pshufd	 $0x55,  %xmm8, %xmm15
	mulss	 %xmm2,  %xmm15
	subss	 %xmm15, %xmm5
	pshufd	 $0xaa,  %xmm8, %xmm15
	mulss	 %xmm2,  %xmm15
	subss	 %xmm15, %xmm6
	pshufd	 $0xff,  %xmm8, %xmm15
	mulss	 %xmm2,  %xmm15
	subss	 %xmm15, %xmm7

	movaps	 -8 * SIZE(BO), %xmm8

	pshufd	 $0xff, %xmm8, %xmm15
	mulss	 %xmm15, %xmm3

	movaps	 -4 * SIZE(BO), %xmm8

	pshufd	 $0x00,  %xmm8, %xmm15
	mulss	 %xmm3,  %xmm15
	subss	 %xmm15, %xmm4
	pshufd	 $0x55,  %xmm8, %xmm15
	mulss	 %xmm3,  %xmm15
	subss	 %xmm15, %xmm5
	pshufd	 $0xaa,  %xmm8, %xmm15
	mulss	 %xmm3,  %xmm15
	subss	 %xmm15, %xmm6
	pshufd	 $0xff,  %xmm8, %xmm15
	mulss	 %xmm3,  %xmm15
	subss	 %xmm15, %xmm7

	movaps	  4 * SIZE(BO), %xmm8

	pshufd	 $0x00, %xmm8, %xmm15
	mulss	 %xmm15, %xmm4
	pshufd	 $0x55,  %xmm8, %xmm15
	mulss	 %xmm4,  %xmm15
	subss	 %xmm15, %xmm5
	pshufd	 $0xaa,  %xmm8, %xmm15
	mulss	 %xmm4,  %xmm15
	subss	 %xmm15, %xmm6
	pshufd	 $0xff,  %xmm8, %xmm15
	mulss	 %xmm4,  %xmm15
	subss	 %xmm15, %xmm7

	movaps	 12 * SIZE(BO), %xmm8

	pshufd	 $0x55, %xmm8, %xmm15
	mulss	 %xmm15, %xmm5
	pshufd	 $0xaa,  %xmm8, %xmm15
	mulss	 %xmm5,  %xmm15
	subss	 %xmm15, %xmm6
	pshufd	 $0xff,  %xmm8, %xmm15
	mulss	 %xmm5,  %xmm15
	subss	 %xmm15, %xmm7

	movaps	 20 * SIZE(BO), %xmm8

	pshufd	 $0xaa, %xmm8, %xmm15
	mulss	 %xmm15, %xmm6
	pshufd	 $0xff,  %xmm8, %xmm15
	mulss	 %xmm6,  %xmm15
	subss	 %xmm15, %xmm7

	movaps	 28 * SIZE(BO), %xmm8

	pshufd	 $0xff, %xmm8, %xmm15
	mulss	 %xmm15, %xmm7
#endif

#ifdef RT
	movaps	 28 * SIZE(BO), %xmm8

	pshufd	 $0xff, %xmm8, %xmm15
	mulss	 %xmm15, %xmm7
	pshufd	 $0xaa,  %xmm8, %xmm15
	mulss	 %xmm7,  %xmm15
	subss	 %xmm15, %xmm6
	pshufd	 $0x55,  %xmm8, %xmm15
	mulss	 %xmm7,  %xmm15
	subss	 %xmm15, %xmm5
	pshufd	 $0x00,  %xmm8, %xmm15
	mulss	 %xmm7,  %xmm15
	subss	 %xmm15, %xmm4

	movaps	 24 * SIZE(BO), %xmm8

	pshufd	 $0xff,  %xmm8, %xmm15
	mulss	 %xmm7,  %xmm15
	subss	 %xmm15, %xmm3
	pshufd	 $0xaa,  %xmm8, %xmm15
	mulss	 %xmm7,  %xmm15
	subss	 %xmm15, %xmm2
	pshufd	 $0x55,  %xmm8, %xmm15
	mulss	 %xmm7,  %xmm15
	subss	 %xmm15, %xmm1
	pshufd	 $0x00,  %xmm8, %xmm15
	mulss	 %xmm7,  %xmm15
	subss	 %xmm15, %xmm0

	movaps	 20 * SIZE(BO), %xmm8

	pshufd	 $0xaa, %xmm8, %xmm15
	mulss	 %xmm15, %xmm6
	pshufd	 $0x55,  %xmm8, %xmm15
	mulss	 %xmm6,  %xmm15
	subss	 %xmm15, %xmm5
	pshufd	 $0x00,  %xmm8, %xmm15
	mulss	 %xmm6,  %xmm15
	subss	 %xmm15, %xmm4

	movaps	 16 * SIZE(BO), %xmm8

	pshufd	 $0xff,  %xmm8, %xmm15
	mulss	 %xmm6,  %xmm15
	subss	 %xmm15, %xmm3
	pshufd	 $0xaa,  %xmm8, %xmm15
	mulss	 %xmm6,  %xmm15
	subss	 %xmm15, %xmm2
	pshufd	 $0x55,  %xmm8, %xmm15
	mulss	 %xmm6,  %xmm15
	subss	 %xmm15, %xmm1
	pshufd	 $0x00,  %xmm8, %xmm15
	mulss	 %xmm6,  %xmm15
	subss	 %xmm15, %xmm0

	movaps	 12 * SIZE(BO), %xmm8

	pshufd	 $0x55, %xmm8, %xmm15
	mulss	 %xmm15, %xmm5
	pshufd	 $0x00,  %xmm8, %xmm15
	mulss	 %xmm5,  %xmm15
	subss	 %xmm15, %xmm4

	movaps	  8 * SIZE(BO), %xmm8

	pshufd	 $0xff,  %xmm8, %xmm15
	mulss	 %xmm5,  %xmm15
	subss	 %xmm15, %xmm3
	pshufd	 $0xaa,  %xmm8, %xmm15
	mulss	 %xmm5,  %xmm15
	subss	 %xmm15, %xmm2
	pshufd	 $0x55,  %xmm8, %xmm15
	mulss	 %xmm5,  %xmm15
	subss	 %xmm15, %xmm1
	pshufd	 $0x00,  %xmm8, %xmm15
	mulss	 %xmm5,  %xmm15
	subss	 %xmm15, %xmm0

	movaps	  4 * SIZE(BO), %xmm8

	pshufd	 $0x00, %xmm8, %xmm15
	mulss	 %xmm15, %xmm4

	movaps	  0 * SIZE(BO), %xmm8

	pshufd	 $0xff,  %xmm8, %xmm15
	mulss	 %xmm4,  %xmm15
	subss	 %xmm15, %xmm3
	pshufd	 $0xaa,  %xmm8, %xmm15
	mulss	 %xmm4,  %xmm15
	subss	 %xmm15, %xmm2
	pshufd	 $0x55,  %xmm8, %xmm15
	mulss	 %xmm4,  %xmm15
	subss	 %xmm15, %xmm1
	pshufd	 $0x00,  %xmm8, %xmm15
	mulss	 %xmm4,  %xmm15
	subss	 %xmm15, %xmm0

	movaps	 -8 * SIZE(BO), %xmm8

	pshufd	 $0xff, %xmm8, %xmm15
	mulss	 %xmm15, %xmm3
	pshufd	 $0xaa,  %xmm8, %xmm15
	mulss	 %xmm3,  %xmm15
	subss	 %xmm15, %xmm2
	pshufd	 $0x55,  %xmm8, %xmm15
	mulss	 %xmm3,  %xmm15
	subss	 %xmm15, %xmm1
	pshufd	 $0x00,  %xmm8, %xmm15
	mulss	 %xmm3,  %xmm15
	subss	 %xmm15, %xmm0

	movaps	-16 * SIZE(BO), %xmm8

	pshufd	 $0xaa, %xmm8, %xmm15
	mulss	 %xmm15, %xmm2
	pshufd	 $0x55,  %xmm8, %xmm15
	mulss	 %xmm2,  %xmm15
	subss	 %xmm15, %xmm1
	pshufd	 $0x00,  %xmm8, %xmm15
	mulss	 %xmm2,  %xmm15
	subss	 %xmm15, %xmm0

	movaps	-24 * SIZE(BO), %xmm8

	pshufd	 $0x55, %xmm8, %xmm15
	mulss	 %xmm15, %xmm1
	pshufd	 $0x00,  %xmm8, %xmm15
	mulss	 %xmm1,  %xmm15
	subss	 %xmm15, %xmm0

	movaps	-32 * SIZE(BO), %xmm8

	pshufd	 $0x00, %xmm8, %xmm15
	mulss	 %xmm15, %xmm0
#endif

#ifdef LN
	subq	$1 * SIZE, CO1
	subq	$1 * SIZE, CO2
#endif

	leaq	(LDC, LDC, 2), %rax

#if defined(LN) || defined(LT)
	movaps	%xmm0, -32 * SIZE(BO)
	movaps	%xmm4, -28 * SIZE(BO)

	pshufd	$0xff, %xmm0, %xmm3
	pshufd	$0xaa, %xmm0, %xmm2
	pshufd	$0x55, %xmm0, %xmm1
	pshufd	$0x00, %xmm0, %xmm0

	pshufd	$0xff, %xmm4, %xmm7
	pshufd	$0xaa, %xmm4, %xmm6
	pshufd	$0x55, %xmm4, %xmm5
	pshufd	$0x00, %xmm4, %xmm4
#else
	unpcklps %xmm1, %xmm0
	unpcklps %xmm3, %xmm2
	unpcklps %xmm5, %xmm4
	unpcklps %xmm7, %xmm6

	movlps	%xmm0, -32 * SIZE(AO)
	movlps	%xmm2, -30 * SIZE(AO)
	movlps	%xmm4, -28 * SIZE(AO)
	movlps	%xmm6, -26 * SIZE(AO)
#endif

	movss	%xmm0,  (CO1)
	movss	%xmm1,  (CO1, LDC,  1)
	movss	%xmm2,  (CO1, LDC,  2)
	movss	%xmm3,  (CO1, %rax, 1)

	movss	%xmm4,  (CO2)
	movss	%xmm5,  (CO2, LDC,  1)
	movss	%xmm6,  (CO2, LDC,  2)
	movss	%xmm7,  (CO2, %rax, 1)

#ifndef LN
	addq	$1 * SIZE, CO1
	addq	$1 * SIZE, CO2
#endif

#if defined(LT) || defined(RN)
	movq	K,  %rax
	subq	KK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 8), BO
#endif

#ifdef LN
	subq	$1, KK
#endif

#ifdef LT
	addq	$1, KK
#endif

#ifdef RT
       movq	K, %rax
       salq	$BASE_SHIFT, %rax
       addq	%rax, AORIG
#endif
	ALIGN_4

.L39:
#ifdef LN
       leaq	(, K, SIZE), %rax
       leaq	(B, %rax, 8), B
#endif
#if defined(LT) || defined(RN)
	movq	BO, B
#endif

#ifdef RN
	addq	$8, KK
#endif

#ifdef RT
	subq	$8, KK
#endif

	subq	$1, J
	BRANCH
	jg	.L10
	ALIGN_4

.L999:
	movq	  0(%rsp), %rbx
	movq	  8(%rsp), %rbp
	movq	 16(%rsp), %r12
	movq	 24(%rsp), %r13
	movq	 32(%rsp), %r14
	movq	 40(%rsp), %r15

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

	addq	$STACKSIZE, %rsp
	ret

	EPILOGUE
