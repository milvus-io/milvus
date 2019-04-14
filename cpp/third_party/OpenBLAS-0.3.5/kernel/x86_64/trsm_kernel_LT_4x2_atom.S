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
#define AO	%r13
#define BO	%r14
#define	CO1	%r15
#define CO2	%rbx
#define KK	%rbp
#define BB	%r12

#ifndef WINDOWS_ABI

#define STACKSIZE 128

#define OLD_LDC		 8 + STACKSIZE(%rsp)
#define OLD_OFFSET	16 + STACKSIZE(%rsp)

#define OFFSET	 48(%rsp)
#define J	 56(%rsp)
#define KKK	 64(%rsp)
#define AORIG	 72(%rsp)

#else

#define STACKSIZE 256

#define OLD_A		40 + STACKSIZE(%rsp)
#define OLD_B		48 + STACKSIZE(%rsp)
#define OLD_C		56 + STACKSIZE(%rsp)
#define OLD_LDC		64 + STACKSIZE(%rsp)
#define OLD_OFFSET	72 + STACKSIZE(%rsp)

#define OFFSET	 224(%rsp)
#define J	 232(%rsp)
#define KKK	 240(%rsp)
#define AORIG	 248(%rsp)

#endif

#define PREFETCH     prefetcht0
#define PREFETCHSIZE (8 * 8 + 3)

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
#endif

	movq	OLD_LDC,   LDC
	movq	OLD_OFFSET, KK

	movq	KK, OFFSET

	leaq	(, LDC, SIZE), LDC

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

#ifdef RN
	negq	KK
#endif

#ifdef RT
       movq	N, %rax
       subq	OFFSET, %rax
       movq	%rax, KK
#endif

	movq	N,  J
	sarq	$1, J
	jle	.L40
	ALIGN_4

.L10:
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

	movq	K, %rax
	salq	$BASE_SHIFT + 1, %rax
	leaq	(B, %rax), BB

#ifdef LT
	movq	OFFSET, %rax
	movq	%rax, KK
#endif

	movq	M,  I
	sarq	$2, I
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
	leaq	(B,  %rax, 2), BO
#else
	movq	B, BO
#endif

	prefetcht0	  0 * SIZE(BB)
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
	prefetcht0     3 * SIZE(CO2)
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
	addsd	 %xmm2, %xmm13
	PREFETCH (PREFETCHSIZE + 0) * SIZE(AO)
	movaps	 %xmm0, %xmm2
	mulsd	 %xmm1, %xmm0

	addsd	 %xmm7, %xmm14
	movsd	 3 * SIZE(AO), %xmm7
	mulsd	 %xmm3, %xmm2

	addsd	 %xmm6, %xmm15
	PREFETCH ((PREFETCHSIZE) >> 1 + 0) * SIZE(BO)
	movaps	 %xmm4, %xmm6
	mulsd	 %xmm1, %xmm4

	addsd	 %xmm0, %xmm8
	movsd	 4 * SIZE(AO), %xmm0
	mulsd	 %xmm3, %xmm6

	addsd	 %xmm2, %xmm9
	movaps	 %xmm5, %xmm2
	mulsd	 %xmm1, %xmm5

	addsd	 %xmm4, %xmm10
	movsd	 5 * SIZE(AO), %xmm4
	mulsd	 %xmm3, %xmm2

	addsd	 %xmm6, %xmm11
	movaps	 %xmm7, %xmm6
	mulsd	 %xmm1, %xmm7
	movsd	 2 * SIZE(BO), %xmm1

	addsd	 %xmm5, %xmm12
	movsd	 6 * SIZE(AO), %xmm5
	mulsd	 %xmm3, %xmm6
	movsd	 3 * SIZE(BO), %xmm3

	addsd	 %xmm2, %xmm13
	movaps	 %xmm0, %xmm2
	mulsd	 %xmm1, %xmm0

	addsd	 %xmm7, %xmm14
	movsd	 7 * SIZE(AO), %xmm7
	mulsd	 %xmm3, %xmm2

	addsd	 %xmm6, %xmm15
	movaps	 %xmm4, %xmm6
	mulsd	 %xmm1, %xmm4

	addsd	 %xmm0, %xmm8
	movsd	 8 * SIZE(AO), %xmm0
	mulsd	 %xmm3, %xmm6

	addsd	 %xmm2, %xmm9
	movaps	 %xmm5, %xmm2
	mulsd	 %xmm1, %xmm5

	addsd	 %xmm4, %xmm10
	movsd	 9 * SIZE(AO), %xmm4
	mulsd	 %xmm3, %xmm2

	addsd	 %xmm6, %xmm11
	movaps	 %xmm7, %xmm6
	mulsd	 %xmm1, %xmm7
	movsd	 4 * SIZE(BO), %xmm1

	addsd	 %xmm5, %xmm12
	movsd	10 * SIZE(AO), %xmm5
	mulsd	 %xmm3, %xmm6
	movsd	 5 * SIZE(BO), %xmm3

	addsd	 %xmm2, %xmm13
	PREFETCH (PREFETCHSIZE + 8) * SIZE(AO)
	movaps	 %xmm0, %xmm2
	mulsd	 %xmm1, %xmm0

	addsd	 %xmm7, %xmm14
	movsd	11 * SIZE(AO), %xmm7
	mulsd	 %xmm3, %xmm2

	addsd	 %xmm6, %xmm15
	movaps	 %xmm4, %xmm6
	mulsd	 %xmm1, %xmm4

	addsd	 %xmm0, %xmm8
	movsd	12 * SIZE(AO), %xmm0
	mulsd	 %xmm3, %xmm6

	addsd	 %xmm2, %xmm9
	movaps	 %xmm5, %xmm2
	mulsd	 %xmm1, %xmm5

	addsd	 %xmm4, %xmm10
	movsd	13 * SIZE(AO), %xmm4
	mulsd	 %xmm3, %xmm2

	addsd	 %xmm6, %xmm11
	movaps	 %xmm7, %xmm6
	mulsd	 %xmm1, %xmm7
	movsd	 6 * SIZE(BO), %xmm1

	addsd	 %xmm5, %xmm12
	movsd	14 * SIZE(AO), %xmm5
	mulsd	 %xmm3, %xmm6
	movsd	 7 * SIZE(BO), %xmm3

	addsd	 %xmm2, %xmm13
	movaps	 %xmm0, %xmm2
	mulsd	 %xmm1, %xmm0

	addsd	 %xmm7, %xmm14
	movsd	15 * SIZE(AO), %xmm7
	mulsd	 %xmm3, %xmm2
	subq   $-16 * SIZE, AO

	addsd	 %xmm6, %xmm15
	movaps	 %xmm4, %xmm6
	mulsd	 %xmm1, %xmm4

	addsd	 %xmm0, %xmm8
	movsd	 0 * SIZE(AO), %xmm0
	mulsd	 %xmm3, %xmm6

	addsd	 %xmm2, %xmm9
	movaps	 %xmm5, %xmm2
	mulsd	 %xmm1, %xmm5
	addq   $  8 * SIZE, BO

	addsd	 %xmm4, %xmm10
	movsd	 1 * SIZE(AO), %xmm4
	mulsd	 %xmm3, %xmm2
	decq   %rax

	addsd	 %xmm6, %xmm11
	movaps	 %xmm7, %xmm6
	mulsd	 %xmm1, %xmm7
	movsd	 0 * SIZE(BO), %xmm1

	addsd	 %xmm5, %xmm12
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
	je .L19
	ALIGN_4

.L16:
	addsd	 %xmm2, %xmm13
	movaps	 %xmm0, %xmm2
	mulsd	 %xmm1, %xmm0

	addsd	 %xmm7, %xmm14
	movsd	 3 * SIZE(AO), %xmm7
	mulsd	 %xmm3, %xmm2

	addsd	 %xmm6, %xmm15
	movaps	 %xmm4, %xmm6
	mulsd	 %xmm1, %xmm4

	addsd	 %xmm0, %xmm8
	movsd	 4 * SIZE(AO), %xmm0
	mulsd	 %xmm3, %xmm6

	addsd	 %xmm2, %xmm9
	movaps	 %xmm5, %xmm2
	mulsd	 %xmm1, %xmm5

	addsd	 %xmm4, %xmm10
	movsd	 5 * SIZE(AO), %xmm4
	mulsd	 %xmm3, %xmm2

	addsd	 %xmm6, %xmm11
	movaps	 %xmm7, %xmm6
	mulsd	 %xmm1, %xmm7
	movsd	 2 * SIZE(BO), %xmm1

	addsd	 %xmm5, %xmm12
	movsd	 6 * SIZE(AO), %xmm5
	mulsd	 %xmm3, %xmm6
	movsd	 3 * SIZE(BO), %xmm3

	addq	$4 * SIZE, AO
	addq	$2 * SIZE, BO
	decq	%rax
	BRANCH
	jg	.L16
	ALIGN_4

.L19:
	addsd	 %xmm2, %xmm13
	addsd	 %xmm7, %xmm14
	addsd	 %xmm6, %xmm15

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

#if defined(LN) || defined(LT)
	movsd	 0 * SIZE(BO), %xmm0
	movsd	 1 * SIZE(BO), %xmm1
	movsd	 2 * SIZE(BO), %xmm2
	movsd	 3 * SIZE(BO), %xmm3
	movsd	 4 * SIZE(BO), %xmm4
	movsd	 5 * SIZE(BO), %xmm5
	movsd	 6 * SIZE(BO), %xmm6
	movsd	 7 * SIZE(BO), %xmm7

	subsd	%xmm8,  %xmm0
	subsd	%xmm9,  %xmm1
	subsd	%xmm10, %xmm2
	subsd	%xmm11, %xmm3
	subsd	%xmm12, %xmm4
	subsd	%xmm13, %xmm5
	subsd	%xmm14, %xmm6
	subsd	%xmm15, %xmm7
#else
	movsd	 0 * SIZE(AO), %xmm0
	movsd	 1 * SIZE(AO), %xmm2
	movsd	 2 * SIZE(AO), %xmm4
	movsd	 3 * SIZE(AO), %xmm6

	movsd	 4 * SIZE(AO), %xmm1
	movsd	 5 * SIZE(AO), %xmm3
	movsd	 6 * SIZE(AO), %xmm5
	movsd	 7 * SIZE(AO), %xmm7

	subsd	%xmm8,  %xmm0
	subsd	%xmm10, %xmm2
	subsd	%xmm12, %xmm4
	subsd	%xmm14, %xmm6
	subsd	%xmm9,  %xmm1
	subsd	%xmm11, %xmm3
	subsd	%xmm13, %xmm5
	subsd	%xmm15, %xmm7
#endif

#ifdef LN
	movsd	15 * SIZE(AO), %xmm8
	mulsd	 %xmm8, %xmm6
	movsd	14 * SIZE(AO), %xmm9
	mulsd	 %xmm8, %xmm7
	movsd	13 * SIZE(AO), %xmm11

	movaps	%xmm9,  %xmm10
	movsd	12 * SIZE(AO), %xmm13
	mulsd	%xmm6,  %xmm9
	movsd	10 * SIZE(AO), %xmm8
	mulsd	%xmm7,  %xmm10
	subsd	%xmm9,  %xmm4
	movsd	 9 * SIZE(AO), %xmm9
	subsd	%xmm10, %xmm5

	movaps	%xmm11,  %xmm12
	mulsd	%xmm6,  %xmm11
	mulsd	%xmm7,  %xmm12
	subsd	%xmm11, %xmm2
	movsd	 8 * SIZE(AO), %xmm11
	subsd	%xmm12, %xmm3

	movaps	%xmm13,  %xmm14
	mulsd	%xmm6,  %xmm13
	mulsd	%xmm7,  %xmm14
	subsd	%xmm13, %xmm0
	subsd	%xmm14, %xmm1

	mulsd	 %xmm8, %xmm4
	mulsd	 %xmm8, %xmm5
	movsd	 5 * SIZE(AO), %xmm8

	movaps	%xmm9,  %xmm10
	mulsd	%xmm4,  %xmm9
	mulsd	%xmm5,  %xmm10
	subsd	%xmm9,  %xmm2
	movsd	 4 * SIZE(AO), %xmm9
	subsd	%xmm10, %xmm3

	movaps	%xmm11,  %xmm12
	mulsd	%xmm4,  %xmm11
	mulsd	%xmm5,  %xmm12
	subsd	%xmm11, %xmm0
	movsd	 0 * SIZE(AO), %xmm11
	subsd	%xmm12, %xmm1

	mulsd	 %xmm8, %xmm2
	mulsd	 %xmm8, %xmm3

	movaps	%xmm9,  %xmm10
	mulsd	%xmm2,  %xmm9
	mulsd	%xmm3,  %xmm10
	subsd	%xmm9,  %xmm0
	subsd	%xmm10, %xmm1

	mulsd	 %xmm11, %xmm0
	mulsd	 %xmm11, %xmm1
#endif

#ifdef LT
	movsd	 0 * SIZE(AO), %xmm8
	mulsd	 %xmm8, %xmm0
	movsd	 1 * SIZE(AO), %xmm9
	mulsd	 %xmm8, %xmm1

	movsd	 2 * SIZE(AO), %xmm11
	movaps	%xmm9,  %xmm10
	movsd	 3 * SIZE(AO), %xmm13
	mulsd	%xmm0,  %xmm9
	movsd	 5 * SIZE(AO), %xmm8
	mulsd	%xmm1,  %xmm10
	subsd	%xmm9,  %xmm2
	movsd	 6 * SIZE(AO), %xmm9
	subsd	%xmm10, %xmm3

	movaps	%xmm11, %xmm12
	mulsd	%xmm0,  %xmm11
	mulsd	%xmm1,  %xmm12
	subsd	%xmm11, %xmm4
	movsd	 7 * SIZE(AO), %xmm11
	subsd	%xmm12, %xmm5

	movaps	%xmm13, %xmm14
	mulsd	%xmm0,  %xmm13
	mulsd	%xmm1,  %xmm14
	subsd	%xmm13, %xmm6
	subsd	%xmm14, %xmm7

	mulsd	 %xmm8, %xmm2
	mulsd	 %xmm8, %xmm3
	movsd	10 * SIZE(AO), %xmm8

	movaps	%xmm9,  %xmm10
	mulsd	%xmm2,  %xmm9
	mulsd	%xmm3,  %xmm10
	subsd	%xmm9,  %xmm4
	movsd	11 * SIZE(AO), %xmm9
	subsd	%xmm10, %xmm5

	movaps	%xmm11, %xmm12
	mulsd	%xmm2,  %xmm11
	mulsd	%xmm3,  %xmm12
	subsd	%xmm11, %xmm6
	subsd	%xmm12, %xmm7

	mulsd	 %xmm8, %xmm4
	mulsd	 %xmm8, %xmm5
	movsd	15 * SIZE(AO), %xmm8

	movaps	%xmm9,  %xmm10
	mulsd	%xmm4,  %xmm9
	mulsd	%xmm5,  %xmm10
	subsd	%xmm9,  %xmm6
	subsd	%xmm10, %xmm7

	mulsd	 %xmm8, %xmm6
	mulsd	 %xmm8, %xmm7
#endif

#ifdef RN
	movsd	 0 * SIZE(BO), %xmm8
	mulsd	 %xmm8, %xmm0
	movsd	 1 * SIZE(BO), %xmm9
	mulsd	 %xmm8, %xmm2
	movsd	 3 * SIZE(BO), %xmm13
	mulsd	 %xmm8, %xmm4
	mulsd	 %xmm8, %xmm6

	movaps	 %xmm9, %xmm10
	movaps	 %xmm9, %xmm11
	movaps	 %xmm9, %xmm12

	mulsd	 %xmm0, %xmm9
	mulsd	 %xmm2, %xmm10
	mulsd	 %xmm4, %xmm11
	mulsd	 %xmm6, %xmm12

	subsd	 %xmm9,  %xmm1
	subsd	 %xmm10, %xmm3
	subsd	 %xmm11, %xmm5
	subsd	 %xmm12, %xmm7

	mulsd	 %xmm13, %xmm1
	mulsd	 %xmm13, %xmm3
	mulsd	 %xmm13, %xmm5
	mulsd	 %xmm13, %xmm7
#endif

#ifdef RT
	movsd	 3 * SIZE(BO), %xmm8
	mulsd	 %xmm8, %xmm1
	movsd	 2 * SIZE(BO), %xmm9
	mulsd	 %xmm8, %xmm3
	movsd	 0 * SIZE(BO), %xmm13
	mulsd	 %xmm8, %xmm5
	mulsd	 %xmm8, %xmm7

	movaps	 %xmm9, %xmm10
	movaps	 %xmm9, %xmm11
	movaps	 %xmm9, %xmm12

	mulsd	 %xmm1, %xmm9
	mulsd	 %xmm3, %xmm10
	mulsd	 %xmm5, %xmm11
	mulsd	 %xmm7, %xmm12

	subsd	 %xmm9,  %xmm0
	subsd	 %xmm10, %xmm2
	subsd	 %xmm11, %xmm4
	subsd	 %xmm12, %xmm6

	mulsd	 %xmm13, %xmm0
	mulsd	 %xmm13, %xmm2
	mulsd	 %xmm13, %xmm4
	mulsd	 %xmm13, %xmm6
#endif

#ifdef LN
	subq	$4 * SIZE, CO1
	subq	$4 * SIZE, CO2
#endif

	movsd	%xmm0,  0 * SIZE(CO1)
	movsd	%xmm2,  1 * SIZE(CO1)
	movsd	%xmm4,  2 * SIZE(CO1)
	movsd	%xmm6,  3 * SIZE(CO1)

	movsd	%xmm1,  0 * SIZE(CO2)
	movsd	%xmm3,  1 * SIZE(CO2)
	movsd	%xmm5,  2 * SIZE(CO2)
	movsd	%xmm7,  3 * SIZE(CO2)

#if defined(LN) || defined(LT)
	movsd	%xmm0,   0 * SIZE(BO)
	movsd	%xmm1,   1 * SIZE(BO)
	movsd	%xmm2,   2 * SIZE(BO)
	movsd	%xmm3,   3 * SIZE(BO)
	movsd	%xmm4,   4 * SIZE(BO)
	movsd	%xmm5,   5 * SIZE(BO)
	movsd	%xmm6,   6 * SIZE(BO)
	movsd	%xmm7,   7 * SIZE(BO)
#else
	movsd	%xmm0,   0 * SIZE(AO)
	movsd	%xmm2,   1 * SIZE(AO)
	movsd	%xmm4,   2 * SIZE(AO)
	movsd	%xmm6,   3 * SIZE(AO)
	movsd	%xmm1,   4 * SIZE(AO)
	movsd	%xmm3,   5 * SIZE(AO)
	movsd	%xmm5,   6 * SIZE(AO)
	movsd	%xmm7,   7 * SIZE(AO)
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

	decq	I			# i --
	jg	.L11
	ALIGN_4

.L20:
	testq	$2, M
	BRANCH
	je	.L30

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
	addsd	 %xmm2, %xmm9
	movaps	 %xmm0, %xmm2
	mulsd	 %xmm1, %xmm0

	addsd	 %xmm6, %xmm11
	movaps	 %xmm4, %xmm6
	mulsd	 %xmm1, %xmm4
	movsd	 2 * SIZE(BO), %xmm1

	addsd	 %xmm0, %xmm8
	movsd	 4 * SIZE(AO), %xmm0
	mulsd	 %xmm3, %xmm2

	addsd	 %xmm4, %xmm10
	movsd	 5 * SIZE(AO), %xmm4
	mulsd	 %xmm3, %xmm6
	movsd	 3 * SIZE(BO), %xmm3

	addsd	 %xmm2, %xmm9
	movaps	 %xmm5, %xmm2
	mulsd	 %xmm1, %xmm5

	addsd	 %xmm6, %xmm11
	movaps	 %xmm7, %xmm6
	mulsd	 %xmm1, %xmm7
	movsd	 4 * SIZE(BO), %xmm1

	addsd	 %xmm5, %xmm8
	movsd	 6 * SIZE(AO), %xmm5
	mulsd	 %xmm3, %xmm2

	addsd	 %xmm7, %xmm10
	movsd	 7 * SIZE(AO), %xmm7
	mulsd	 %xmm3, %xmm6
	movsd	 5 * SIZE(BO), %xmm3

	addsd	 %xmm2, %xmm9
	movaps	 %xmm0, %xmm2
	mulsd	 %xmm1, %xmm0

	addsd	 %xmm6, %xmm11
	movaps	 %xmm4, %xmm6
	mulsd	 %xmm1, %xmm4
	movsd	 6 * SIZE(BO), %xmm1

	addsd	 %xmm0, %xmm8
	movsd	 8 * SIZE(AO), %xmm0
	mulsd	 %xmm3, %xmm2

	addsd	 %xmm4, %xmm10
	movsd	 9 * SIZE(AO), %xmm4
	mulsd	 %xmm3, %xmm6
	movsd	 7 * SIZE(BO), %xmm3

	addsd	 %xmm2, %xmm9
	movaps	 %xmm5, %xmm2
	mulsd	 %xmm1, %xmm5

	addsd	 %xmm6, %xmm11
	movaps	 %xmm7, %xmm6
	mulsd	 %xmm1, %xmm7
	movsd	 8 * SIZE(BO), %xmm1

	addsd	 %xmm5, %xmm8
	movsd	10 * SIZE(AO), %xmm5
	mulsd	 %xmm3, %xmm2

	addsd	 %xmm7, %xmm10
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
	je .L29
	ALIGN_4

.L26:
	addsd	 %xmm2, %xmm9
	movaps	 %xmm0, %xmm2
	mulsd	 %xmm1, %xmm0

	addsd	 %xmm6, %xmm11
	movaps	 %xmm4, %xmm6
	mulsd	 %xmm1, %xmm4
	movsd	 2 * SIZE(BO), %xmm1

	mulsd	 %xmm3, %xmm2
	addsd	 %xmm0, %xmm8
	movsd	 2 * SIZE(AO), %xmm0

	mulsd	 %xmm3, %xmm6
	movsd	 3 * SIZE(BO), %xmm3
	addsd	 %xmm4, %xmm10
	movsd	 3 * SIZE(AO), %xmm4

	addq	$2 * SIZE, AO
	addq	$2 * SIZE, BO
	decq	%rax
	BRANCH
	jg	.L26
	ALIGN_4

.L29:
	addsd	 %xmm2, %xmm9
	addsd	 %xmm6, %xmm11

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

#if defined(LN) || defined(LT)
	movsd	 0 * SIZE(BO), %xmm0
	movsd	 1 * SIZE(BO), %xmm1
	movsd	 2 * SIZE(BO), %xmm2
	movsd	 3 * SIZE(BO), %xmm3

	subsd	%xmm8,  %xmm0
	subsd	%xmm9,  %xmm1
	subsd	%xmm10, %xmm2
	subsd	%xmm11, %xmm3
#else
	movsd	 0 * SIZE(AO), %xmm0
	movsd	 1 * SIZE(AO), %xmm2
	movsd	 2 * SIZE(AO), %xmm1
	movsd	 3 * SIZE(AO), %xmm3

	subsd	%xmm8,  %xmm0
	subsd	%xmm10, %xmm2
	subsd	%xmm9,  %xmm1
	subsd	%xmm11, %xmm3
#endif

#ifdef LN
	movsd	 3 * SIZE(AO), %xmm8
	mulsd	 %xmm8, %xmm2
	movsd	 2 * SIZE(AO), %xmm9
	mulsd	 %xmm8, %xmm3
	movsd	 0 * SIZE(AO), %xmm13

	movaps	 %xmm9, %xmm10
	mulsd	 %xmm2, %xmm9
	mulsd	 %xmm3, %xmm10

	subsd	 %xmm9,  %xmm0
	subsd	 %xmm10, %xmm1

	mulsd	 %xmm13, %xmm0
	mulsd	 %xmm13, %xmm1
#endif

#ifdef LT
	movsd	 0 * SIZE(AO), %xmm8
	mulsd	 %xmm8, %xmm0
	movsd	 1 * SIZE(AO), %xmm9
	mulsd	 %xmm8, %xmm1
	movsd	 3 * SIZE(AO), %xmm13

	movaps	 %xmm9, %xmm10
	mulsd	 %xmm0, %xmm9
	mulsd	 %xmm1, %xmm10

	subsd	 %xmm9,  %xmm2
	subsd	 %xmm10, %xmm3

	mulsd	 %xmm13, %xmm2
	mulsd	 %xmm13, %xmm3
#endif

#ifdef RN
	movsd	 0 * SIZE(BO), %xmm8
	mulsd	 %xmm8, %xmm0
	movsd	 1 * SIZE(BO), %xmm9
	mulsd	 %xmm8, %xmm2
	movsd	 3 * SIZE(BO), %xmm13

	movaps	 %xmm9, %xmm10
	mulsd	 %xmm0, %xmm9
	mulsd	 %xmm2, %xmm10

	subsd	 %xmm9,  %xmm1
	subsd	 %xmm10, %xmm3

	mulsd	 %xmm13, %xmm1
	mulsd	 %xmm13, %xmm3
#endif

#ifdef RT
	movsd	 3 * SIZE(BO), %xmm8
	mulsd	 %xmm8, %xmm1
	movsd	 2 * SIZE(BO), %xmm9
	mulsd	 %xmm8, %xmm3
	movsd	 0 * SIZE(BO), %xmm13

	movaps	 %xmm9, %xmm10
	mulsd	 %xmm1, %xmm9
	mulsd	 %xmm3, %xmm10

	subsd	 %xmm9,  %xmm0
	subsd	 %xmm10, %xmm2

	mulsd	 %xmm13, %xmm0
	mulsd	 %xmm13, %xmm2
#endif

#ifdef LN
	subq	$2 * SIZE, CO1
	subq	$2 * SIZE, CO2
#endif

	movsd	%xmm0,  0 * SIZE(CO1)
	movsd	%xmm2,  1 * SIZE(CO1)
	movsd	%xmm1,  0 * SIZE(CO2)
	movsd	%xmm3,  1 * SIZE(CO2)

#if defined(LN) || defined(LT)
	movsd	%xmm0,   0 * SIZE(BO)
	movsd	%xmm1,   1 * SIZE(BO)
	movsd	%xmm2,   2 * SIZE(BO)
	movsd	%xmm3,   3 * SIZE(BO)
#else
	movsd	%xmm0,   0 * SIZE(AO)
	movsd	%xmm2,   1 * SIZE(AO)
	movsd	%xmm1,   2 * SIZE(AO)
	movsd	%xmm3,   3 * SIZE(AO)
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

.L30:
	testq	$1, M
	je	.L39

#ifdef LN
       movq	K, %rax
       salq	$0 + BASE_SHIFT, %rax
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

	movsd	 0 * SIZE(AO), %xmm0
	xorps	%xmm7,  %xmm7
	movsd	 1 * SIZE(AO), %xmm2
	xorps	%xmm5,  %xmm5

	movsd	 0 * SIZE(BO), %xmm1
	xorps	%xmm8,  %xmm8
	xorps	%xmm9,  %xmm9
	movsd	 1 * SIZE(BO), %xmm3

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	sarq	$2, %rax
	je	.L35
	ALIGN_4

.L32:
	addsd	 %xmm5, %xmm8
	movsd	 2 * SIZE(BO), %xmm5
	mulsd	 %xmm0, %xmm1
	PREFETCH (PREFETCHSIZE + 0) * SIZE(AO)

	addsd	 %xmm7, %xmm9
	movsd	 3 * SIZE(BO), %xmm7
	mulsd	 %xmm0, %xmm3
	movsd	 2 * SIZE(AO), %xmm0

	addsd	 %xmm1, %xmm8
	movsd	 4 * SIZE(BO), %xmm1
	mulsd	 %xmm2, %xmm5

	addsd	 %xmm3, %xmm9
	movsd	 5 * SIZE(BO), %xmm3
	mulsd	 %xmm2, %xmm7
	movsd	 3 * SIZE(AO), %xmm2

	addsd	 %xmm5, %xmm8
	movsd	 6 * SIZE(BO), %xmm5
	mulsd	 %xmm0, %xmm1

	addsd	 %xmm7, %xmm9
	movsd	 7 * SIZE(BO), %xmm7
	mulsd	 %xmm0, %xmm3
	movsd	 4 * SIZE(AO), %xmm0

	addsd	 %xmm1, %xmm8
	movsd	 8 * SIZE(BO), %xmm1
	mulsd	 %xmm2, %xmm5

	addsd	 %xmm3, %xmm9
	movsd	 9 * SIZE(BO), %xmm3
	mulsd	 %xmm2, %xmm7
	movsd	 5 * SIZE(AO), %xmm2

	addq	$4 * SIZE, AO
	addq	$8 * SIZE, BO

	decq	%rax
	jne    .L32
	ALIGN_4

.L35:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	addsd	 %xmm5, %xmm8
	addsd	 %xmm7, %xmm9

	andq	$3, %rax
	BRANCH
	BRANCH
	je	.L38
	ALIGN_4

.L36:
	mulsd	 %xmm0, %xmm1
	addq	$2 * SIZE, BO
	mulsd	 %xmm0, %xmm3
	movsd	 1 * SIZE(AO), %xmm0

	addsd	 %xmm1, %xmm8
	movsd	 0 * SIZE(BO), %xmm1
	addsd	 %xmm3, %xmm9
	movsd	 1 * SIZE(BO), %xmm3

	addq	$1 * SIZE, AO
	decq	%rax
	BRANCH
	jg	.L36
	ALIGN_4

.L38:
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

#if defined(LN) || defined(LT)
	movsd	 0 * SIZE(BO), %xmm0
	movsd	 1 * SIZE(BO), %xmm1

	subsd	%xmm8,  %xmm0
	subsd	%xmm9,  %xmm1
#else
	movsd	 0 * SIZE(AO), %xmm0
	movsd	 1 * SIZE(AO), %xmm1

	subsd	%xmm8,  %xmm0
	subsd	%xmm9,  %xmm1
#endif

#if defined(LN) || defined(LT)
	movsd	 0 * SIZE(AO), %xmm8
	mulsd	 %xmm8, %xmm0
	mulsd	 %xmm8, %xmm1
#endif

#ifdef RN
	movsd	 0 * SIZE(BO), %xmm8
	mulsd	 %xmm8, %xmm0
	movsd	 1 * SIZE(BO), %xmm9
	mulsd	 %xmm0,  %xmm9
	movsd	 3 * SIZE(BO), %xmm13
	subsd	 %xmm9,  %xmm1
	mulsd	 %xmm13, %xmm1
#endif

#ifdef RT
	movsd	 3 * SIZE(BO), %xmm8
	mulsd	 %xmm8, %xmm1
	movsd	 2 * SIZE(BO), %xmm9
	mulsd	 %xmm1,  %xmm9
	movsd	 0 * SIZE(BO), %xmm13
	subsd	 %xmm9,  %xmm0
	mulsd	 %xmm13, %xmm0
#endif

#ifdef LN
	subq	$1 * SIZE, CO1
	subq	$1 * SIZE, CO2
#endif

	movsd	%xmm0,  0 * SIZE(CO1)
	movsd	%xmm1,  0 * SIZE(CO2)

#if defined(LN) || defined(LT)
	movsd	%xmm0,   0 * SIZE(BO)
	movsd	%xmm1,   1 * SIZE(BO)
#else
	movsd	%xmm0,   0 * SIZE(AO)
	movsd	%xmm1,   1 * SIZE(AO)
#endif

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
       salq	$0 + BASE_SHIFT, %rax
       addq	%rax, AORIG
#endif
	ALIGN_4

.L39:
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

	decq	J			# j --
	jg	.L10
	ALIGN_4

.L40:
	testq	$1, N
	je	.L999
	ALIGN_4

#if defined(LT) || defined(RN)
	movq	A, AO
#else
	movq	A, AORIG
#endif

#ifdef RT
       movq	K, %rax
       salq	$0 + BASE_SHIFT, %rax
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
	leaq	(B,  %rax, 1), BO
#else
	movq	B, BO
#endif

	movsd	 0 * SIZE(AO), %xmm0
	xorps	%xmm9,   %xmm9
	movsd	 1 * SIZE(AO), %xmm1
	xorps	%xmm11,  %xmm11
	movsd	 2 * SIZE(AO), %xmm2
	xorps	%xmm13,  %xmm13
	movsd	 3 * SIZE(AO), %xmm3
	xorps	%xmm15,  %xmm15

	movsd	 0 * SIZE(BO), %xmm4
	xorps	%xmm8,  %xmm8
	movsd	 1 * SIZE(BO), %xmm5
	xorps	%xmm10, %xmm10
	prefetcht0     3 * SIZE(CO1)
	xorps	%xmm12, %xmm12
	xorps	%xmm14, %xmm14

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	sarq	$2, %rax
	je	.L45
	ALIGN_4

.L42:
	addsd	 %xmm9,  %xmm8
	movsd	 4 * SIZE(AO), %xmm9
	mulsd	 %xmm4, %xmm0
	PREFETCH (PREFETCHSIZE + 0) * SIZE(AO)

	addsd	 %xmm11, %xmm10
	movsd	 5 * SIZE(AO), %xmm11
	mulsd	 %xmm4, %xmm1

	addsd	 %xmm13, %xmm12
	movsd	 6 * SIZE(AO), %xmm13
	mulsd	 %xmm4, %xmm2

	addsd	 %xmm15, %xmm14
	movsd	 7 * SIZE(AO), %xmm15
	mulsd	 %xmm4, %xmm3
	movsd	 2 * SIZE(BO), %xmm4

	addsd	 %xmm0, %xmm8
	movsd	 8 * SIZE(AO), %xmm0
	mulsd	 %xmm5, %xmm9

	addsd	 %xmm1, %xmm10
	movsd	 9 * SIZE(AO), %xmm1
	mulsd	 %xmm5, %xmm11

	addsd	 %xmm2, %xmm12
	movsd	10 * SIZE(AO), %xmm2
	mulsd	 %xmm5, %xmm13

	addsd	 %xmm3, %xmm14
	movsd	11 * SIZE(AO), %xmm3
	mulsd	 %xmm5, %xmm15
	movsd	 3 * SIZE(BO), %xmm5

	addsd	 %xmm9,  %xmm8
	movsd	12 * SIZE(AO), %xmm9
	mulsd	 %xmm4, %xmm0
	PREFETCH (PREFETCHSIZE + 8) * SIZE(AO)

	addsd	 %xmm11, %xmm10
	movsd	13 * SIZE(AO), %xmm11
	mulsd	 %xmm4, %xmm1

	addsd	 %xmm13, %xmm12
	movsd	14 * SIZE(AO), %xmm13
	mulsd	 %xmm4, %xmm2

	addsd	 %xmm15, %xmm14
	movsd	15 * SIZE(AO), %xmm15
	mulsd	 %xmm4, %xmm3
	movsd	 4 * SIZE(BO), %xmm4
	subq	$-16 * SIZE, AO

	addsd	 %xmm0, %xmm8
	movsd	 0 * SIZE(AO), %xmm0
	mulsd	 %xmm5, %xmm9

	addsd	 %xmm1, %xmm10
	movsd	 1 * SIZE(AO), %xmm1
	mulsd	 %xmm5, %xmm11
	addq	$  4 * SIZE, BO

	addsd	 %xmm2, %xmm12
	movsd	 2 * SIZE(AO), %xmm2
	mulsd	 %xmm5, %xmm13
	decq	%rax

	addsd	 %xmm3, %xmm14
	movsd	 3 * SIZE(AO), %xmm3
	mulsd	 %xmm5, %xmm15
	movsd	 1 * SIZE(BO), %xmm5

	jne    .L42
	ALIGN_4

.L45:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif

	addsd	 %xmm9,  %xmm8
	addsd	 %xmm11, %xmm10
	addsd	 %xmm13, %xmm12
	addsd	 %xmm15, %xmm14

	andq	$3, %rax
	BRANCH
	BRANCH
	je	.L49
	ALIGN_4

.L46:
	mulsd	 %xmm4, %xmm0
	mulsd	 %xmm4, %xmm1
	mulsd	 %xmm4, %xmm2
	mulsd	 %xmm4, %xmm3
	movsd	 1 * SIZE(BO), %xmm4

	addsd	 %xmm0, %xmm8
	movsd	 4 * SIZE(AO), %xmm0
	addsd	 %xmm1, %xmm10
	movsd	 5 * SIZE(AO), %xmm1
	addsd	 %xmm2, %xmm12
	movsd	 6 * SIZE(AO), %xmm2
	addsd	 %xmm3, %xmm14
	movsd	 7 * SIZE(AO), %xmm3

	addq	$4 * SIZE, AO
	addq	$1 * SIZE, BO
	decq	%rax
	BRANCH
	jg	.L46
	ALIGN_4

.L49:
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

#if defined(LN) || defined(LT)
	movsd	 0 * SIZE(BO), %xmm0
	movsd	 1 * SIZE(BO), %xmm2
	movsd	 2 * SIZE(BO), %xmm4
	movsd	 3 * SIZE(BO), %xmm6

	subsd	%xmm8,  %xmm0
	subsd	%xmm10, %xmm2
	subsd	%xmm12, %xmm4
	subsd	%xmm14, %xmm6
#else
	movsd	 0 * SIZE(AO), %xmm0
	movsd	 1 * SIZE(AO), %xmm2
	movsd	 2 * SIZE(AO), %xmm4
	movsd	 3 * SIZE(AO), %xmm6

	subsd	%xmm8,  %xmm0
	subsd	%xmm10, %xmm2
	subsd	%xmm12, %xmm4
	subsd	%xmm14, %xmm6
#endif

#ifdef LN
	movsd	15 * SIZE(AO), %xmm8
	mulsd	 %xmm8, %xmm6
	movsd	14 * SIZE(AO), %xmm9
	mulsd	%xmm6,  %xmm9
	movsd	13 * SIZE(AO), %xmm11
	subsd	%xmm9,  %xmm4
	movsd	12 * SIZE(AO), %xmm13
	mulsd	%xmm6,  %xmm11
	movsd	10 * SIZE(AO), %xmm8
	subsd	%xmm11, %xmm2
	movsd	 9 * SIZE(AO), %xmm9
	mulsd	%xmm6,  %xmm13
	movsd	 8 * SIZE(AO), %xmm11
	subsd	%xmm13, %xmm0

	mulsd	 %xmm8, %xmm4
	movsd	 5 * SIZE(AO), %xmm8
	mulsd	%xmm4,  %xmm9
	subsd	%xmm9,  %xmm2
	movsd	 4 * SIZE(AO), %xmm9
	mulsd	%xmm4,  %xmm11
	subsd	%xmm11, %xmm0
	movsd	 0 * SIZE(AO), %xmm11
	mulsd	 %xmm8, %xmm2
	mulsd	%xmm2,  %xmm9
	subsd	%xmm9,  %xmm0
	mulsd	 %xmm11, %xmm0
#endif

#ifdef LT
	movsd	 0 * SIZE(AO), %xmm8
	mulsd	 %xmm8, %xmm0
	movsd	 1 * SIZE(AO), %xmm9
	mulsd	%xmm0,  %xmm9
	movsd	 2 * SIZE(AO), %xmm11
	subsd	%xmm9,  %xmm2
	movsd	 3 * SIZE(AO), %xmm13
	mulsd	%xmm0,  %xmm11
	movsd	 5 * SIZE(AO), %xmm8
	subsd	%xmm11, %xmm4
	movsd	 6 * SIZE(AO), %xmm9
	mulsd	%xmm0,  %xmm13
	movsd	 7 * SIZE(AO), %xmm11
	subsd	%xmm13, %xmm6

	mulsd	 %xmm8, %xmm2
	movsd	10 * SIZE(AO), %xmm8
	mulsd	%xmm2,  %xmm9
	subsd	%xmm9,  %xmm4
	movsd	11 * SIZE(AO), %xmm9
	mulsd	%xmm2,  %xmm11
	subsd	%xmm11, %xmm6
	mulsd	 %xmm8, %xmm4
	movsd	15 * SIZE(AO), %xmm8
	mulsd	%xmm4,  %xmm9
	subsd	%xmm9,  %xmm6
	mulsd	%xmm8,  %xmm6
#endif

#if defined(RN) || defined(RT)
	movsd	 0 * SIZE(BO), %xmm8
	mulsd	 %xmm8, %xmm0
	mulsd	 %xmm8, %xmm2
	mulsd	 %xmm8, %xmm4
	mulsd	 %xmm8, %xmm6
#endif

#ifdef LN
	subq	$4 * SIZE, CO1
#endif

	movsd	%xmm0,  0 * SIZE(CO1)
	movsd	%xmm2,  1 * SIZE(CO1)
	movsd	%xmm4,  2 * SIZE(CO1)
	movsd	%xmm6,  3 * SIZE(CO1)

#if defined(LN) || defined(LT)
	movsd	%xmm0,   0 * SIZE(BO)
	movsd	%xmm2,   1 * SIZE(BO)
	movsd	%xmm4,   2 * SIZE(BO)
	movsd	%xmm6,   3 * SIZE(BO)
#else
	movsd	%xmm0,   0 * SIZE(AO)
	movsd	%xmm2,   1 * SIZE(AO)
	movsd	%xmm4,   2 * SIZE(AO)
	movsd	%xmm6,   3 * SIZE(AO)
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

	decq	I			# i --
	jg	.L41
	ALIGN_4

.L50:
	testq	$2, M
	je	.L60

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

	movsd	 0 * SIZE(AO), %xmm0
	xorps	%xmm2,   %xmm2
	movsd	 1 * SIZE(AO), %xmm1
	xorps	%xmm3,   %xmm3

	movsd	 0 * SIZE(BO), %xmm4
	xorps	%xmm8,  %xmm8
	movsd	 1 * SIZE(BO), %xmm5
	xorps	%xmm10, %xmm10

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	sarq	$2, %rax
	je	.L55
	ALIGN_4

.L52:
	addsd	 %xmm2, %xmm8
	movsd	 2 * SIZE(AO), %xmm2
	mulsd	 %xmm4, %xmm0
	PREFETCH (PREFETCHSIZE + 0) * SIZE(AO)

	addsd	 %xmm3, %xmm10
	movsd	 3 * SIZE(AO), %xmm3
	mulsd	 %xmm4, %xmm1
	movsd	 2 * SIZE(BO), %xmm4

	addsd	 %xmm0, %xmm8
	movsd	 4 * SIZE(AO), %xmm0
	mulsd	 %xmm5, %xmm2
	addq	$8 * SIZE, AO

	addsd	 %xmm1, %xmm10
	movsd	-3 * SIZE(AO), %xmm1
	mulsd	 %xmm5, %xmm3
	movsd	 3 * SIZE(BO), %xmm5

	addsd	 %xmm2, %xmm8
	movsd	-2 * SIZE(AO), %xmm2
	mulsd	 %xmm4, %xmm0
	addq	$4 * SIZE, BO

	addsd	 %xmm3, %xmm10
	movsd	-1 * SIZE(AO), %xmm3
	mulsd	 %xmm4, %xmm1
	movsd	 0 * SIZE(BO), %xmm4

	addsd	 %xmm0, %xmm8
	movsd	 0 * SIZE(AO), %xmm0
	mulsd	 %xmm5, %xmm2
	decq	%rax

	addsd	 %xmm1, %xmm10
	movsd	 1 * SIZE(AO), %xmm1
	mulsd	 %xmm5, %xmm3
	movsd	 1 * SIZE(BO), %xmm5

	jne    .L52
	ALIGN_4

.L55:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	addsd	 %xmm2, %xmm8
	addsd	 %xmm3, %xmm10

	andq	$3, %rax
	BRANCH
	je .L59
	ALIGN_4

.L56:
	mulsd	 %xmm4, %xmm0
	mulsd	 %xmm4, %xmm1
	movsd	 1 * SIZE(BO), %xmm4

	addsd	 %xmm0, %xmm8
	movsd	 2 * SIZE(AO), %xmm0
	addsd	 %xmm1, %xmm10
	movsd	 3 * SIZE(AO), %xmm1

	addq	$2 * SIZE, AO
	addq	$1 * SIZE, BO
	decq	%rax
	BRANCH
	jg	.L56
	ALIGN_4

.L59:
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

#if defined(LN) || defined(LT)
	movsd	 0 * SIZE(BO), %xmm0
	movsd	 1 * SIZE(BO), %xmm2

	subsd	%xmm8,  %xmm0
	subsd	%xmm10, %xmm2
#else
	movsd	 0 * SIZE(AO), %xmm0
	movsd	 1 * SIZE(AO), %xmm2

	subsd	%xmm8,  %xmm0
	subsd	%xmm10, %xmm2
#endif

#ifdef LN
	movsd	 3 * SIZE(AO), %xmm8
	movsd	 2 * SIZE(AO), %xmm9
	movsd	 0 * SIZE(AO), %xmm11
	mulsd	%xmm8, %xmm2
	mulsd	%xmm2, %xmm9
	subsd	%xmm9, %xmm0
	mulsd	%xmm11,%xmm0
#endif

#ifdef LT
	movsd	 0 * SIZE(AO), %xmm8
	movsd	 1 * SIZE(AO), %xmm9
	movsd	 3 * SIZE(AO), %xmm11
	mulsd	%xmm8, %xmm0
	mulsd	%xmm0, %xmm9
	subsd	%xmm9, %xmm2
	mulsd	%xmm11,%xmm2
#endif

#if defined(RN) || defined(RT)
	movsd	 0 * SIZE(BO), %xmm8
	mulsd	 %xmm8, %xmm0
	mulsd	 %xmm8, %xmm2
#endif

#ifdef LN
	subq	$2 * SIZE, CO1
#endif

	movsd	%xmm0,  0 * SIZE(CO1)
	movsd	%xmm2,  1 * SIZE(CO1)

#if defined(LN) || defined(LT)
	movsd	%xmm0,   0 * SIZE(BO)
	movsd	%xmm2,   1 * SIZE(BO)
#else
	movsd	%xmm0,   0 * SIZE(AO)
	movsd	%xmm2,   1 * SIZE(AO)
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

.L60:
	testq	$1, M
	je	.L69

#ifdef LN
       movq	K, %rax
       salq	$0 + BASE_SHIFT, %rax
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

	movsd	 0 * SIZE(AO), %xmm0
	xorps	%xmm5,  %xmm5
	movsd	 1 * SIZE(AO), %xmm2
	xorps	%xmm7,  %xmm7

	movsd	 0 * SIZE(BO), %xmm1
	xorps	%xmm8,  %xmm8
	movsd	 1 * SIZE(BO), %xmm3
	xorps	%xmm9,  %xmm9
	movsd	 2 * SIZE(AO), %xmm4
	movsd	 3 * SIZE(AO), %xmm6

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	sarq	$2, %rax
	je	.L65
	ALIGN_4

.L62:
	addsd	 %xmm5, %xmm8
	movsd	 2 * SIZE(BO), %xmm5
	mulsd	 %xmm0, %xmm1
	movsd	 4 * SIZE(AO), %xmm0

	addsd	 %xmm7, %xmm9
	movsd	 3 * SIZE(BO), %xmm7
	mulsd	 %xmm2, %xmm3
	movsd	 5 * SIZE(AO), %xmm2

	addsd	 %xmm1, %xmm8
	movsd	 4 * SIZE(BO), %xmm1
	mulsd	 %xmm4, %xmm5
	movsd	 6 * SIZE(AO), %xmm4

	addsd	 %xmm3, %xmm9
	movsd	 5 * SIZE(BO), %xmm3
	mulsd	 %xmm6, %xmm7
	movsd	 7 * SIZE(AO), %xmm6

	addq	$4 * SIZE, AO
	addq	$4 * SIZE, BO

	decq	%rax
	jne    .L62

	addsd	 %xmm5, %xmm8
	addsd	 %xmm7, %xmm9
	ALIGN_4

.L65:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$3, %rax
	BRANCH
	je .L68
	ALIGN_4

.L66:
	movsd	 0 * SIZE(AO), %xmm0
	movsd	 0 * SIZE(BO), %xmm1

	mulsd	 %xmm0, %xmm1
	addsd	 %xmm1, %xmm8

	addq	$1 * SIZE, AO
	addq	$1 * SIZE, BO

	decq	%rax
	BRANCH
	jg	.L66
	ALIGN_4

.L68:
	addsd	%xmm9, %xmm8

#if defined(LN) || defined(RT)
	movq	KK, %rax
#ifdef LN
	subq	$1, %rax
#else
	subq	$1, %rax
#endif
	leaq	(, %rax, SIZE), %rax

	movq	AORIG, AO
	leaq	(AO, %rax, 1), AO
	leaq	(B,  %rax, 1), BO
#endif

#if defined(LN) || defined(LT)
	movsd	 0 * SIZE(BO), %xmm0
	subsd	%xmm8,  %xmm0
#else
	movsd	 0 * SIZE(AO), %xmm0
	subsd	%xmm8,  %xmm0
#endif

#if defined(LN) || defined(LT)
	movsd	 0 * SIZE(AO), %xmm8
	mulsd	 %xmm8, %xmm0
#endif

#if defined(RN) || defined(RT)
	movsd	 0 * SIZE(BO), %xmm8
	mulsd	 %xmm8, %xmm0
#endif

#ifdef LN
	subq	$1 * SIZE, CO1
#endif

	movsd	%xmm0,  0 * SIZE(CO1)

#if defined(LN) || defined(LT)
	movsd	%xmm0,   0 * SIZE(BO)
#else
	movsd	%xmm0,   0 * SIZE(AO)
#endif

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
       salq	$0 + BASE_SHIFT, %rax
       addq	%rax, AORIG
#endif
	ALIGN_4

.L69:
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
	ALIGN_2

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
