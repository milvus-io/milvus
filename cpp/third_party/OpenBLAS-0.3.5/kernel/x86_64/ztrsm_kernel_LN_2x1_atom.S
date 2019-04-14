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

#define M	%rdi
#define N	%rsi
#define K	%rdx

#define A	%rcx
#define B	%r8
#define C	%r9
#define LDC	%r10

#define I	%r11
#define J	%r12
#define AO	%r13
#define BO	%r14
#define	CO1	%r15
#define BB	%rbx
#define KK	%rbp

#ifndef WINDOWS_ABI

#define STACKSIZE 128

#define OLD_LDC		 8 + STACKSIZE(%rsp)
#define OLD_OFFSET	16 + STACKSIZE(%rsp)

#define OFFSET	 48(%rsp)
#define KKK	 56(%rsp)
#define AORIG    64(%rsp)

#else

#define STACKSIZE 256

#define OLD_A		48 + STACKSIZE(%rsp)
#define OLD_B		56 + STACKSIZE(%rsp)
#define OLD_C		64 + STACKSIZE(%rsp)
#define OLD_LDC		72 + STACKSIZE(%rsp)
#define OLD_OFFSET	80 + STACKSIZE(%rsp)

#define OFFSET	 224(%rsp)
#define KKK	 232(%rsp)
#define AORIG    240(%rsp)
#endif


#define PREFETCH     prefetcht0
#define PREFETCHSIZE (8 * 8 + 3)

#ifndef CONJ
#define ADDSD1	addsd
#define ADDSD2	addsd
#define ADDSD3	addsd
#define ADDSD4	subsd

#elif defined(LN) || defined(LT)
#define ADDSD1	addsd
#define ADDSD2	addsd
#define ADDSD3	subsd
#define ADDSD4	addsd
#else
#define ADDSD1	addsd
#define ADDSD2	subsd
#define ADDSD3	addsd
#define ADDSD4	addsd
#endif


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

	movq	ARG1,      M
	movq	ARG2,      N
	movq	ARG3,      K
	movq	OLD_A,     A
	movq	OLD_B,     B
	movq	OLD_C,     C
	movq	OLD_LDC,   LDC
#endif

	movq	OLD_LDC,    LDC
	movq	OLD_OFFSET, KK

	movq	KK, OFFSET

	salq	$ZBASE_SHIFT, LDC

#ifdef LN
       movq	M, %rax
       salq	$ZBASE_SHIFT, %rax
       addq	%rax, C
       imulq	K, %rax
       addq	%rax, A
#endif

#ifdef RT
       movq	N, %rax
       salq	$ZBASE_SHIFT, %rax
       imulq	K, %rax
       addq	%rax, B

       movq	N, %rax
       imulq	LDC, %rax
       addq	%rax, C
#endif

#ifdef RN
	negq	KK
#endif

#ifdef RT
       movq	N, KK
       subq	OFFSET, KK
#endif

	movq	N,  J
	testq	N, N
	jle	.L999
	ALIGN_4

.L01:
#if defined(LT) || defined(RN)
	movq	A, AO
#else
	movq	A, AORIG
#endif

#ifdef RT
	movq	K, %rax
	salq	$ZBASE_SHIFT, %rax
	subq	%rax, B
	subq	LDC, C
#endif

	movq	C, CO1

#ifndef RT
	addq	LDC, C
#endif

#ifdef LN
	movq	OFFSET, KK
	addq	M, KK
#endif
#ifdef LT
	movq	OFFSET, KK
#endif

	movq	K, %rax
	salq	$ZBASE_SHIFT, %rax
	leaq	(B, %rax), BB

	testq	$1, M
	jle	.L20

#ifdef LN
       movq	K, %rax
       salq	$0 + ZBASE_SHIFT, %rax
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

	movsd	 0 * SIZE(AO), %xmm0
	xorps	%xmm2,  %xmm2
	movsd	 1 * SIZE(AO), %xmm4
	xorps	%xmm5,  %xmm5
	movsd	 2 * SIZE(AO), %xmm5
	xorps	%xmm6,  %xmm6
	movsd	 3 * SIZE(AO), %xmm7

	movsd	 0 * SIZE(BO), %xmm1
	xorps	%xmm8,  %xmm8
	xorps	%xmm9,  %xmm9
	movsd	 1 * SIZE(BO), %xmm3
	xorps	%xmm10, %xmm10
	xorps	%xmm11, %xmm11

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	sarq	$2, %rax
	je	.L25
	ALIGN_4

.L22:
	PREFETCH (PREFETCHSIZE + 0) * SIZE(AO)
	ADDSD2	 %xmm2, %xmm9
	movaps	 %xmm0, %xmm2
	mulsd	 %xmm1, %xmm0

	ADDSD4	 %xmm6, %xmm11
	movaps	 %xmm4, %xmm6
	mulsd	 %xmm1, %xmm4
	movsd	 2 * SIZE(BO), %xmm1

	ADDSD1	 %xmm0, %xmm8
	movsd	 4 * SIZE(AO), %xmm0
	mulsd	 %xmm3, %xmm2

	ADDSD3	 %xmm4, %xmm10
	movsd	 5 * SIZE(AO), %xmm4
	mulsd	 %xmm3, %xmm6
	movsd	 3 * SIZE(BO), %xmm3

	ADDSD2	 %xmm2, %xmm9
	movaps	 %xmm5, %xmm2
	mulsd	 %xmm1, %xmm5

	ADDSD4	 %xmm6, %xmm11
	movaps	 %xmm7, %xmm6
	mulsd	 %xmm1, %xmm7
	movsd	 4 * SIZE(BO), %xmm1

	ADDSD1	 %xmm5, %xmm8
	movsd	 6 * SIZE(AO), %xmm5
	mulsd	 %xmm3, %xmm2

	ADDSD3	 %xmm7, %xmm10
	movsd	 7 * SIZE(AO), %xmm7
	mulsd	 %xmm3, %xmm6
	movsd	 5 * SIZE(BO), %xmm3

	ADDSD2	 %xmm2, %xmm9
	movaps	 %xmm0, %xmm2
	mulsd	 %xmm1, %xmm0

	ADDSD4	 %xmm6, %xmm11
	movaps	 %xmm4, %xmm6
	mulsd	 %xmm1, %xmm4
	movsd	 6 * SIZE(BO), %xmm1

	ADDSD1	 %xmm0, %xmm8
	movsd	 8 * SIZE(AO), %xmm0
	mulsd	 %xmm3, %xmm2

	ADDSD3	 %xmm4, %xmm10
	movsd	 9 * SIZE(AO), %xmm4
	mulsd	 %xmm3, %xmm6
	movsd	 7 * SIZE(BO), %xmm3

	ADDSD2	 %xmm2, %xmm9
	movaps	 %xmm5, %xmm2
	mulsd	 %xmm1, %xmm5

	ADDSD4	 %xmm6, %xmm11
	movaps	 %xmm7, %xmm6
	mulsd	 %xmm1, %xmm7
	movsd	 8 * SIZE(BO), %xmm1

	ADDSD1	 %xmm5, %xmm8
	movsd	10 * SIZE(AO), %xmm5
	mulsd	 %xmm3, %xmm2

	ADDSD3	 %xmm7, %xmm10
	movsd	11 * SIZE(AO), %xmm7
	mulsd	 %xmm3, %xmm6
	movsd	 9 * SIZE(BO), %xmm3

	addq	$8 * SIZE, AO
	addq	$8 * SIZE, BO

	decq	%rax
	jne    .L22
	ALIGN_4

.L25:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$3, %rax
	BRANCH
	BRANCH
	je	.L29
	ALIGN_4

.L26:
	ADDSD2	 %xmm2, %xmm9
	movaps	 %xmm0, %xmm2
	mulsd	 %xmm1, %xmm0

	ADDSD4	 %xmm6, %xmm11
	movaps	 %xmm4, %xmm6
	mulsd	 %xmm1, %xmm4
	movsd	 2 * SIZE(BO), %xmm1

	mulsd	 %xmm3, %xmm2
	ADDSD1	 %xmm0, %xmm8
	movsd	 2 * SIZE(AO), %xmm0

	mulsd	 %xmm3, %xmm6
	movsd	 3 * SIZE(BO), %xmm3
	ADDSD3	 %xmm4, %xmm10
	movsd	 3 * SIZE(AO), %xmm4

	addq	$2 * SIZE, AO
	addq	$2 * SIZE, BO
	decq	%rax
	BRANCH
	jg	.L26
	ALIGN_4

.L29:
	ADDSD2	 %xmm2, %xmm9
	ADDSD4	 %xmm6, %xmm11

	addsd	%xmm11,  %xmm8
	addsd	%xmm9,  %xmm10

#if defined(LN) || defined(RT)
	movq	KK, %rax
#ifdef LN
	subq	$1, %rax
#else
	subq	$1, %rax
#endif

	leaq	(, %rax, SIZE), %rax

	movq	AORIG, AO
	leaq	(AO, %rax, 2), AO
	leaq	(B,  %rax, 2), BO
#endif

#if defined(LN) || defined(LT)
	movsd	 0 * SIZE(BO), %xmm0
	movsd	 1 * SIZE(BO), %xmm1
#else
	movsd	 0 * SIZE(AO), %xmm0
	movsd	 1 * SIZE(AO), %xmm1
#endif

	subsd	%xmm8,  %xmm0
	subsd	%xmm10, %xmm1

#if defined(LN) || defined(LT)
	movsd	 0 * SIZE(AO), %xmm6
	movaps	 %xmm0, %xmm5
	movsd	 1 * SIZE(AO), %xmm7
	movaps	 %xmm1, %xmm4

	mulsd	 %xmm6, %xmm0
	mulsd	 %xmm6, %xmm1
	mulsd	 %xmm7, %xmm5
	mulsd	 %xmm7, %xmm4

	ADDSD4	 %xmm4, %xmm0
	ADDSD3	 %xmm5, %xmm1
#endif

#if defined(RN) || defined(RT)
	movsd	 0 * SIZE(BO), %xmm8
	movaps	 %xmm0, %xmm5
	movsd	 1 * SIZE(BO), %xmm9
	movaps	 %xmm1, %xmm4

	mulsd	 %xmm8, %xmm0
	mulsd	 %xmm8, %xmm1
	mulsd	 %xmm9, %xmm5
	mulsd	 %xmm9, %xmm4

	ADDSD4	 %xmm4, %xmm0
	ADDSD2	 %xmm5, %xmm1
#endif

#ifdef LN
	subq	$2 * SIZE, CO1
#endif

	movsd	%xmm0,  0 * SIZE(CO1)
	movsd	%xmm1,  1 * SIZE(CO1)

#if defined(LN) || defined(LT)
	movsd	%xmm0,   0 * SIZE(BO)
	movsd	%xmm1,   1 * SIZE(BO)
#else
	movsd	%xmm0,   0 * SIZE(AO)
	movsd	%xmm1,   1 * SIZE(AO)
#endif

#ifndef LN
	addq	$2 * SIZE, CO1
#endif

#if defined(LT) || defined(RN)
	movq	K,  %rax
	subq	KK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
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
       salq	$0 + ZBASE_SHIFT, %rax
       addq	%rax, AORIG
#endif
	ALIGN_4

.L20:
	movq	M,  I
	sarq	$1, I
	jle	.L99
	ALIGN_4

.L10:
#ifdef LN
       movq	K, %rax
       salq	$1 + ZBASE_SHIFT, %rax
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

	prefetcht0	 0 * SIZE(BB)
	subq	   $-8 * SIZE, BB

	movsd	 0 * SIZE(AO), %xmm0
	xorps	%xmm2,  %xmm2
	movsd	 1 * SIZE(AO), %xmm4
	xorps	%xmm5,  %xmm5
	movsd	 2 * SIZE(AO), %xmm5
	xorps	%xmm6,  %xmm6
	xorps	%xmm7,  %xmm7

	movsd	 0 * SIZE(BO), %xmm1
	xorps	%xmm8,  %xmm8
	xorps	%xmm9,  %xmm9
	movsd	 1 * SIZE(BO), %xmm3
	xorps	%xmm10, %xmm10
	xorps	%xmm11, %xmm11

	prefetcht0     3 * SIZE(CO1)
	xorps	%xmm12, %xmm12
	xorps	%xmm13, %xmm13
	xorps	%xmm14, %xmm14
	xorps	%xmm15, %xmm15

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	sarq	$2, %rax
	je	.L15
	ALIGN_4

.L12:
	ADDSD2	 %xmm2, %xmm13
	PREFETCH (PREFETCHSIZE + 0) * SIZE(AO)
	movaps	 %xmm0, %xmm2
	mulsd	 %xmm1, %xmm0

	ADDSD3	 %xmm7, %xmm14
	movsd	 3 * SIZE(AO), %xmm7
	mulsd	 %xmm3, %xmm2

	ADDSD4	 %xmm6, %xmm15
	PREFETCH ((PREFETCHSIZE) >> 1 + 0) * SIZE(BO)
	movaps	 %xmm4, %xmm6
	mulsd	 %xmm1, %xmm4

	ADDSD1	 %xmm0, %xmm8
	movsd	 4 * SIZE(AO), %xmm0
	mulsd	 %xmm3, %xmm6

	ADDSD2	 %xmm2, %xmm9
	movaps	 %xmm5, %xmm2
	mulsd	 %xmm1, %xmm5

	ADDSD3	 %xmm4, %xmm10
	movsd	 5 * SIZE(AO), %xmm4
	mulsd	 %xmm3, %xmm2

	ADDSD4	 %xmm6, %xmm11
	movaps	 %xmm7, %xmm6
	mulsd	 %xmm1, %xmm7
	movsd	 2 * SIZE(BO), %xmm1

	ADDSD1	 %xmm5, %xmm12
	movsd	 6 * SIZE(AO), %xmm5
	mulsd	 %xmm3, %xmm6
	movsd	 3 * SIZE(BO), %xmm3

	ADDSD2	 %xmm2, %xmm13
	movaps	 %xmm0, %xmm2
	mulsd	 %xmm1, %xmm0

	ADDSD3	 %xmm7, %xmm14
	movsd	 7 * SIZE(AO), %xmm7
	mulsd	 %xmm3, %xmm2

	ADDSD4	 %xmm6, %xmm15
	movaps	 %xmm4, %xmm6
	mulsd	 %xmm1, %xmm4

	ADDSD1	 %xmm0, %xmm8
	movsd	 8 * SIZE(AO), %xmm0
	mulsd	 %xmm3, %xmm6

	ADDSD2	 %xmm2, %xmm9
	movaps	 %xmm5, %xmm2
	mulsd	 %xmm1, %xmm5

	ADDSD3	 %xmm4, %xmm10
	movsd	 9 * SIZE(AO), %xmm4
	mulsd	 %xmm3, %xmm2

	ADDSD4	 %xmm6, %xmm11
	movaps	 %xmm7, %xmm6
	mulsd	 %xmm1, %xmm7
	movsd	 4 * SIZE(BO), %xmm1

	ADDSD1	 %xmm5, %xmm12
	movsd	10 * SIZE(AO), %xmm5
	mulsd	 %xmm3, %xmm6
	movsd	 5 * SIZE(BO), %xmm3

	ADDSD2	 %xmm2, %xmm13
	PREFETCH (PREFETCHSIZE + 8) * SIZE(AO)
	movaps	 %xmm0, %xmm2
	mulsd	 %xmm1, %xmm0

	ADDSD3	 %xmm7, %xmm14
	movsd	11 * SIZE(AO), %xmm7
	mulsd	 %xmm3, %xmm2

	ADDSD4	 %xmm6, %xmm15
	movaps	 %xmm4, %xmm6
	mulsd	 %xmm1, %xmm4

	ADDSD1	 %xmm0, %xmm8
	movsd	12 * SIZE(AO), %xmm0
	mulsd	 %xmm3, %xmm6

	ADDSD2	 %xmm2, %xmm9
	movaps	 %xmm5, %xmm2
	mulsd	 %xmm1, %xmm5

	ADDSD3	 %xmm4, %xmm10
	movsd	13 * SIZE(AO), %xmm4
	mulsd	 %xmm3, %xmm2

	ADDSD4	 %xmm6, %xmm11
	movaps	 %xmm7, %xmm6
	mulsd	 %xmm1, %xmm7
	movsd	 6 * SIZE(BO), %xmm1

	ADDSD1	 %xmm5, %xmm12
	movsd	14 * SIZE(AO), %xmm5
	mulsd	 %xmm3, %xmm6
	movsd	 7 * SIZE(BO), %xmm3

	ADDSD2	 %xmm2, %xmm13
	movaps	 %xmm0, %xmm2
	mulsd	 %xmm1, %xmm0

	ADDSD3	 %xmm7, %xmm14
	movsd	15 * SIZE(AO), %xmm7
	mulsd	 %xmm3, %xmm2
	subq   $-16 * SIZE, AO

	ADDSD4	 %xmm6, %xmm15
	movaps	 %xmm4, %xmm6
	mulsd	 %xmm1, %xmm4

	ADDSD1	 %xmm0, %xmm8
	movsd	 0 * SIZE(AO), %xmm0
	mulsd	 %xmm3, %xmm6

	ADDSD2	 %xmm2, %xmm9
	movaps	 %xmm5, %xmm2
	mulsd	 %xmm1, %xmm5
	addq   $  8 * SIZE, BO

	ADDSD3	 %xmm4, %xmm10
	movsd	 1 * SIZE(AO), %xmm4
	mulsd	 %xmm3, %xmm2
	decq   %rax

	ADDSD4	 %xmm6, %xmm11
	movaps	 %xmm7, %xmm6
	mulsd	 %xmm1, %xmm7
	movsd	 0 * SIZE(BO), %xmm1

	ADDSD1	 %xmm5, %xmm12
	movsd	 2 * SIZE(AO), %xmm5
	mulsd	 %xmm3, %xmm6
	movsd	 1 * SIZE(BO), %xmm3

	jne    .L12
	ALIGN_4

.L15:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$3, %rax
	BRANCH
	BRANCH
	je	.L18
	ALIGN_4

.L16:
	ADDSD2	 %xmm2, %xmm13
	movaps	 %xmm0, %xmm2
	mulsd	 %xmm1, %xmm0

	ADDSD3	 %xmm7, %xmm14
	movsd	 3 * SIZE(AO), %xmm7
	mulsd	 %xmm3, %xmm2

	ADDSD4	 %xmm6, %xmm15
	movaps	 %xmm4, %xmm6
	mulsd	 %xmm1, %xmm4

	ADDSD1	 %xmm0, %xmm8
	movsd	 4 * SIZE(AO), %xmm0
	mulsd	 %xmm3, %xmm6

	ADDSD2	 %xmm2, %xmm9
	movaps	 %xmm5, %xmm2
	mulsd	 %xmm1, %xmm5

	ADDSD3	 %xmm4, %xmm10
	movsd	 5 * SIZE(AO), %xmm4
	mulsd	 %xmm3, %xmm2

	ADDSD4	 %xmm6, %xmm11
	movaps	 %xmm7, %xmm6
	mulsd	 %xmm1, %xmm7
	movsd	 2 * SIZE(BO), %xmm1

	ADDSD1	 %xmm5, %xmm12
	movsd	 6 * SIZE(AO), %xmm5
	mulsd	 %xmm3, %xmm6
	movsd	 3 * SIZE(BO), %xmm3

	addq	$4 * SIZE, AO
	addq	$2 * SIZE, BO
	decq	%rax
	BRANCH
	jg	.L16
	ALIGN_4

.L18:
	ADDSD2	 %xmm2, %xmm13
	ADDSD3	 %xmm7, %xmm14
	ADDSD4	 %xmm6, %xmm15

	addsd	%xmm11, %xmm8
	addsd	%xmm9,  %xmm10
	addsd	%xmm15, %xmm12
	addsd	%xmm13, %xmm14

#if defined(LN) || defined(RT)
	movq	KK, %rax
#ifdef LN
	subq	$2, %rax
#else
	subq	$1, %rax
#endif

	leaq	(, %rax, SIZE), %rax

	movq	AORIG, AO
	leaq	(AO, %rax, 4), AO
	leaq	(B,  %rax, 2), BO
#endif

#if defined(LN) || defined(LT)
	movsd	 0 * SIZE(BO), %xmm0
	movsd	 1 * SIZE(BO), %xmm1
	movsd	 2 * SIZE(BO), %xmm2
	movsd	 3 * SIZE(BO), %xmm3
#else
	movsd	 0 * SIZE(AO), %xmm0
	movsd	 1 * SIZE(AO), %xmm1
	movsd	 2 * SIZE(AO), %xmm2
	movsd	 3 * SIZE(AO), %xmm3
#endif

	subsd	%xmm8,  %xmm0
	subsd	%xmm10, %xmm1
	subsd	%xmm12, %xmm2
	subsd	%xmm14, %xmm3

#ifdef LN
	movsd	 6 * SIZE(AO), %xmm6
	movsd	 7 * SIZE(AO), %xmm7

	movaps	 %xmm2, %xmm5
	movaps	 %xmm3, %xmm4

	mulsd	 %xmm6, %xmm2
	mulsd	 %xmm6, %xmm3
	movsd	 4 * SIZE(AO), %xmm6
	mulsd	 %xmm7, %xmm5
	mulsd	 %xmm7, %xmm4
	movsd	 5 * SIZE(AO), %xmm7

	ADDSD4	 %xmm4, %xmm2
	ADDSD3	 %xmm5, %xmm3

	movaps	 %xmm2, %xmm4
	movaps	 %xmm3, %xmm5

	mulsd	 %xmm6, %xmm4
	mulsd	 %xmm7, %xmm5
	mulsd	 %xmm3, %xmm6
	mulsd	 %xmm2, %xmm7

	subsd	 %xmm4, %xmm0
	subsd	 %xmm6, %xmm1
	movsd	 0 * SIZE(AO), %xmm6

	ADDSD3	 %xmm5, %xmm0
	ADDSD4	 %xmm7, %xmm1
	movsd	 1 * SIZE(AO), %xmm7

	movaps	 %xmm0, %xmm5
	movaps	 %xmm1, %xmm4

	mulsd	 %xmm6, %xmm0
	mulsd	 %xmm6, %xmm1
	mulsd	 %xmm7, %xmm5
	mulsd	 %xmm7, %xmm4

	ADDSD4	 %xmm4, %xmm0
	ADDSD3	 %xmm5, %xmm1
#endif

#ifdef LT
	movsd	 0 * SIZE(AO), %xmm6
	movsd	 1 * SIZE(AO), %xmm7

	movaps	 %xmm0, %xmm5
	movaps	 %xmm1, %xmm4

	mulsd	 %xmm6, %xmm0
	mulsd	 %xmm6, %xmm1
	movsd	 2 * SIZE(AO), %xmm6
	mulsd	 %xmm7, %xmm5
	mulsd	 %xmm7, %xmm4
	movsd	 3 * SIZE(AO), %xmm7

	ADDSD4	 %xmm4, %xmm0
	ADDSD3	 %xmm5, %xmm1

	movaps	 %xmm0, %xmm4
	movaps	 %xmm1, %xmm5

	mulsd	 %xmm6, %xmm4
	mulsd	 %xmm7, %xmm5
	mulsd	 %xmm1, %xmm6
	mulsd	 %xmm0, %xmm7

	subsd	 %xmm4, %xmm2
	subsd	 %xmm6, %xmm3
	movsd	 6 * SIZE(AO), %xmm6

	ADDSD3	 %xmm5, %xmm2
	ADDSD4	 %xmm7, %xmm3
	movsd	 7 * SIZE(AO), %xmm7

	movaps	 %xmm2, %xmm5
	movaps	 %xmm3, %xmm4

	mulsd	 %xmm6, %xmm2
	mulsd	 %xmm6, %xmm3
	mulsd	 %xmm7, %xmm5
	mulsd	 %xmm7, %xmm4

	ADDSD4	 %xmm4, %xmm2
	ADDSD3	 %xmm5, %xmm3
#endif

#if defined(RN) || defined(RT)
	movsd	 0 * SIZE(BO), %xmm8
	movaps	 %xmm0, %xmm5
	movsd	 1 * SIZE(BO), %xmm9
	movaps	 %xmm1, %xmm4
	movaps	 %xmm2, %xmm7
	movaps	 %xmm3, %xmm6

	mulsd	 %xmm8, %xmm0
	mulsd	 %xmm8, %xmm1
	mulsd	 %xmm9, %xmm5
	mulsd	 %xmm9, %xmm4

	ADDSD4	 %xmm4, %xmm0
	mulsd	 %xmm8, %xmm2
	ADDSD2	 %xmm5, %xmm1
	mulsd	 %xmm8, %xmm3
	mulsd	 %xmm9, %xmm7
	mulsd	 %xmm9, %xmm6

	ADDSD4	 %xmm6, %xmm2
	ADDSD2	 %xmm7, %xmm3
#endif

#ifdef LN
	subq	$4 * SIZE, CO1
#endif

	movsd	%xmm0,  0 * SIZE(CO1)
	movsd	%xmm1,  1 * SIZE(CO1)
	movsd	%xmm2,  2 * SIZE(CO1)
	movsd	%xmm3,  3 * SIZE(CO1)

#if defined(LN) || defined(LT)
	movsd	%xmm0,   0 * SIZE(BO)
	movsd	%xmm1,   1 * SIZE(BO)
	movsd	%xmm2,   2 * SIZE(BO)
	movsd	%xmm3,   3 * SIZE(BO)
#else
	movsd	%xmm0,   0 * SIZE(AO)
	movsd	%xmm1,   1 * SIZE(AO)
	movsd	%xmm2,   2 * SIZE(AO)
	movsd	%xmm3,   3 * SIZE(AO)
#endif

#ifndef LN
	addq	$4 * SIZE, CO1
#endif

#if defined(LT) || defined(RN)
	movq	K,  %rax
	subq	KK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
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
       salq	$1 + ZBASE_SHIFT, %rax
       addq	%rax, AORIG
#endif
	decq	I			# i --
	jg	.L10
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
	addq	$1, KK
#endif

#ifdef RT
	subq	$1, KK
#endif
	decq	J			# j --
	jg	.L01
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
