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
#define M	%r13
#define N	%r14
#define K	%rdx

#define A	%rcx
#define B	%r8
#define C	%r9
#define LDC	%r10

#define I	%r11
#define AO	%rdi
#define BO	%rsi
#define	CO1	%rbx
#define CO2	%rbp
#define BB	%r12

#ifndef WINDOWS_ABI

#define STACKSIZE 64

#define OLD_LDC		 8 + STACKSIZE(%rsp)
#define OLD_OFFSET	16 + STACKSIZE(%rsp)

#else

#define STACKSIZE 256

#define OLD_A		40 + STACKSIZE(%rsp)
#define OLD_B		48 + STACKSIZE(%rsp)
#define OLD_C		56 + STACKSIZE(%rsp)
#define OLD_LDC		64 + STACKSIZE(%rsp)
#define OLD_OFFSET	72 + STACKSIZE(%rsp)

#endif

#define J	  0(%rsp)
#define OFFSET	  8(%rsp)
#define KK	 16(%rsp)
#define KKK	 24(%rsp)
#define AORIG	 32(%rsp)
#define BORIG	 40(%rsp)
#define BUFFER	128(%rsp)

#define PREFETCH_R    (8 * 4 + 0)
#define PREFETCH_W    (PREFETCH_R)

#define PREFETCHSIZE  (8 * 17 + 2)
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
	movq	ARG3,      K
	movq	OLD_A,     A
	movq	OLD_B,     B
	movq	OLD_C,     C
#endif

	movq	OLD_LDC,    LDC
	movq	OLD_OFFSET, %rax

	movq	%rsp, %r15	# save old stack
	subq	$128 + LOCAL_BUFFER_SIZE, %rsp
	andq	$-4096, %rsp	# align stack

	STACK_TOUCHING

	movq	%rax, KK
	movq	%rax, OFFSET

	movq	OLD_M, M
	movq	OLD_N, N

	subq	$-16 * SIZE, A
	subq	$-16 * SIZE, B

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
	sarq	$2, J		# j = (n >> 2)
	jle	.L40

.L01:
/* Copying to Sub Buffer */

#ifdef LN
	movq	OFFSET, %rax
	addq	M, %rax
	movq	%rax, KK
#endif

	leaq	16 * SIZE + BUFFER, BO

#ifdef RT
       movq	K, %rax
       salq	$2 + BASE_SHIFT, %rax
       subq	%rax, B
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	movq	B, BORIG
	leaq	(, %rax, SIZE), %rax
	leaq	(B,  %rax, 4), B
	leaq	(BO, %rax, 8), BO
#endif

#if defined(LT)
	movq	OFFSET, %rax
	movq	%rax, KK
#endif

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	sarq	$2, %rax
	jle	.L03
	ALIGN_4

.L02:
	prefetcht0	(PREFETCH_R + 0) * SIZE(B)
	movapd	 -16 * SIZE(B), %xmm0
	movapd	 -14 * SIZE(B), %xmm1
	movapd	 -12 * SIZE(B), %xmm2
	movapd	 -10 * SIZE(B), %xmm3
	movapd	  -8 * SIZE(B), %xmm4
	movapd	  -6 * SIZE(B), %xmm5
	movapd	  -4 * SIZE(B), %xmm6
	movapd	  -2 * SIZE(B), %xmm7

	prefetcht0	(PREFETCH_R + 8) * SIZE(B)
	movddup	  %xmm0, %xmm8
	unpckhpd  %xmm0, %xmm0
	movddup	  %xmm1, %xmm9
	unpckhpd  %xmm1, %xmm1
	movddup	  %xmm2, %xmm10
	unpckhpd  %xmm2, %xmm2
	movddup	  %xmm3, %xmm11
	unpckhpd  %xmm3, %xmm3
	movddup	  %xmm4, %xmm12
	unpckhpd  %xmm4, %xmm4
	movddup	  %xmm5, %xmm13
	unpckhpd  %xmm5, %xmm5
	movddup	  %xmm6, %xmm14
	unpckhpd  %xmm6, %xmm6
	movddup	  %xmm7, %xmm15
	unpckhpd  %xmm7, %xmm7

	prefetcht0	(PREFETCH_W +  0) * SIZE(BO)
	movapd	%xmm8,  -16 * SIZE(BO)
	movapd	%xmm0,  -14 * SIZE(BO)
	movapd	%xmm9,  -12 * SIZE(BO)
	movapd	%xmm1,  -10 * SIZE(BO)

	prefetcht0	(PREFETCH_W +  8) * SIZE(BO)
	movapd	%xmm10,  -8 * SIZE(BO)
	movapd	%xmm2,   -6 * SIZE(BO)
	movapd	%xmm11,  -4 * SIZE(BO)
	movapd	%xmm3,   -2 * SIZE(BO)

	prefetcht0	(PREFETCH_W + 16) * SIZE(BO)
	movapd	%xmm12,   0 * SIZE(BO)
	movapd	%xmm4,    2 * SIZE(BO)
	movapd	%xmm13,   4 * SIZE(BO)
	movapd	%xmm5,    6 * SIZE(BO)

	prefetcht0	(PREFETCH_W + 24) * SIZE(BO)
	movapd	%xmm14,   8 * SIZE(BO)
	movapd	%xmm6,   10 * SIZE(BO)
	movapd	%xmm15,  12 * SIZE(BO)
	movapd	%xmm7,   14 * SIZE(BO)

	subq	$-16  * SIZE, B
	subq	$-32 * SIZE, BO
	subq	$1, %rax
	jne	.L02
	ALIGN_4

.L03:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$3, %rax
	BRANCH
	jle	.L10
	ALIGN_4

.L04:
	movapd	 -16 * SIZE(B), %xmm0
	movapd	 -14 * SIZE(B), %xmm1

	movddup	  %xmm0, %xmm8
	unpckhpd  %xmm0, %xmm0
	movddup	  %xmm1, %xmm9
	unpckhpd  %xmm1, %xmm1

	movapd	%xmm8,  -16 * SIZE(BO)
	movapd	%xmm0,  -14 * SIZE(BO)
	movapd	%xmm9,  -12 * SIZE(BO)
	movapd	%xmm1,  -10 * SIZE(BO)

	addq	$4 * SIZE, B
	addq	$8 * SIZE, BO
	subq	$1, %rax
	jne	.L04
	ALIGN_4

.L10:
	leaq	(PREFETCH_R +  0) * SIZE(B), BB

#if defined(LT) || defined(RN)
	movq	A, AO
#else
	movq	A, AORIG
#endif

#ifdef RT
       leaq	(, LDC, 4), %rax
       subq	%rax, C
#endif

	movq	C, CO1			# coffset1 = c
	leaq	(C, LDC, 1), CO2	# coffset2 = c + ldc
#ifndef RT
	leaq	(C, LDC, 4), C
#endif

	testq	$1, M
	je	.L20
	ALIGN_4

.L31:
#ifdef LN
       movq	K, %rax
       salq	$0 + BASE_SHIFT, %rax
       subq	%rax, AORIG
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	movq	AORIG, AO
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 1), AO
#endif

	leaq	16 * SIZE + BUFFER, BO

#if defined(LN) || defined(RT)
	movq	KK, %rax
	salq	$2 + BASE_SHIFT, %rax
	leaq	(BO, %rax, 2), BO
#endif

	pxor	%xmm8, %xmm8
	pxor	%xmm9, %xmm9
	pxor	%xmm10, %xmm10
	pxor	%xmm11, %xmm11

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
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)

	movsd	-16 * SIZE(AO), %xmm0
	movsd	-16 * SIZE(BO), %xmm2
	movsd	-14 * SIZE(BO), %xmm3
	movsd	-12 * SIZE(BO), %xmm4
	movsd	-10 * SIZE(BO), %xmm5

	mulsd	%xmm0, %xmm2
	mulsd	%xmm0, %xmm3
	mulsd	%xmm0, %xmm4
	mulsd	%xmm0, %xmm5

	addsd	%xmm2, %xmm8
	addsd	%xmm3, %xmm9
	addsd	%xmm4, %xmm10
	addsd	%xmm5, %xmm11

	movsd	-15 * SIZE(AO), %xmm0
	movsd	 -8 * SIZE(BO), %xmm2
	movsd	 -6 * SIZE(BO), %xmm3
	movsd	 -4 * SIZE(BO), %xmm4
	movsd	 -2 * SIZE(BO), %xmm5

	mulsd	%xmm0, %xmm2
	mulsd	%xmm0, %xmm3
	mulsd	%xmm0, %xmm4
	mulsd	%xmm0, %xmm5

	addsd	%xmm2, %xmm8
	addsd	%xmm3, %xmm9
	addsd	%xmm4, %xmm10
	addsd	%xmm5, %xmm11

	movsd	-14 * SIZE(AO), %xmm0
	movsd	  0 * SIZE(BO), %xmm2
	movsd	  2 * SIZE(BO), %xmm3
	movsd	  4 * SIZE(BO), %xmm4
	movsd	  6 * SIZE(BO), %xmm5

	mulsd	%xmm0, %xmm2
	mulsd	%xmm0, %xmm3
	mulsd	%xmm0, %xmm4
	mulsd	%xmm0, %xmm5

	addsd	%xmm2, %xmm8
	addsd	%xmm3, %xmm9
	addsd	%xmm4, %xmm10
	addsd	%xmm5, %xmm11

	movsd	-13 * SIZE(AO), %xmm0
	movsd	  8 * SIZE(BO), %xmm2
	movsd	 10 * SIZE(BO), %xmm3
	movsd	 12 * SIZE(BO), %xmm4
	movsd	 14 * SIZE(BO), %xmm5

	mulsd	%xmm0, %xmm2
	mulsd	%xmm0, %xmm3
	mulsd	%xmm0, %xmm4
	mulsd	%xmm0, %xmm5

	addsd	%xmm2, %xmm8
	addsd	%xmm3, %xmm9
	addsd	%xmm4, %xmm10
	addsd	%xmm5, %xmm11

	subq	$ -4 * SIZE, AO
	subq	$-32 * SIZE, BO
	subq	$1, %rax
	jne    .L32
	ALIGN_4

.L35:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	BRANCH
	je .L38
	ALIGN_4

.L36:
	movsd	-16 * SIZE(AO), %xmm0
	movsd	-16 * SIZE(BO), %xmm2
	movsd	-14 * SIZE(BO), %xmm3
	movsd	-12 * SIZE(BO), %xmm4
	movsd	-10 * SIZE(BO), %xmm5

	mulsd	%xmm0, %xmm2
	mulsd	%xmm0, %xmm3
	mulsd	%xmm0, %xmm4
	mulsd	%xmm0, %xmm5

	addsd	%xmm2, %xmm8
	addsd	%xmm3, %xmm9
	addsd	%xmm4, %xmm10
	addsd	%xmm5, %xmm11

	addq	$1 * SIZE, AO
	addq	$8 * SIZE, BO
	subq	$1, %rax
	jg	.L36
	ALIGN_4

.L38:
#if defined(LN) || defined(RT)
	movq	KK, %rax
#ifdef LN
	subq	$1, %rax
#else
	subq	$4, %rax
#endif

	movq	AORIG, AO
	movq	BORIG, B
	leaq	16 * SIZE + BUFFER, BO

	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(B,  %rax, 4), B
	leaq	(BO, %rax, 8), BO
#endif

#if defined(LN) || defined(LT)
	movsd	 -16 * SIZE(B), %xmm12
	movsd	 -15 * SIZE(B), %xmm13
	movsd	 -14 * SIZE(B), %xmm14
	movsd	 -13 * SIZE(B), %xmm15
#else
	movsd	 -16 * SIZE(AO), %xmm12
	movsd	 -15 * SIZE(AO), %xmm13
	movsd	 -14 * SIZE(AO), %xmm14
	movsd	 -13 * SIZE(AO), %xmm15
#endif

	subsd	%xmm8,  %xmm12
	subsd	%xmm9,  %xmm13
	subsd	%xmm10,  %xmm14
	subsd	%xmm11,  %xmm15

#ifdef LN
	movsd	-16 * SIZE(AO), %xmm8

	mulsd	 %xmm8, %xmm12
	mulsd	 %xmm8, %xmm13
	mulsd	 %xmm8, %xmm14
	mulsd	 %xmm8, %xmm15
#endif

#ifdef LT
	movsd	-16 * SIZE(AO), %xmm8

	mulsd	 %xmm8, %xmm12
	mulsd	 %xmm8, %xmm13
	mulsd	 %xmm8, %xmm14
	mulsd	 %xmm8, %xmm15
#endif

#ifdef RN
	mulsd	-16 * SIZE(B), %xmm12
	movlpd	-15 * SIZE(B), %xmm9
	mulsd	 %xmm12, %xmm9
	subsd	 %xmm9, %xmm13
	movlpd	-14 * SIZE(B), %xmm10
	mulsd	 %xmm12, %xmm10
	subsd	 %xmm10, %xmm14
	movlpd  -13 * SIZE(B), %xmm11
	mulsd	 %xmm12, %xmm11
	subsd	 %xmm11, %xmm15

	mulsd	-11 * SIZE(B), %xmm13
	movlpd	-10 * SIZE(B), %xmm9
	mulsd	 %xmm13, %xmm9
	subsd	 %xmm9, %xmm14
	movlpd	 -9 * SIZE(B), %xmm10
	mulsd	 %xmm13, %xmm10
	subsd	 %xmm10, %xmm15

	mulsd	 -6 * SIZE(B), %xmm14
	movlpd	 -5 * SIZE(B), %xmm9
	mulsd	 %xmm14, %xmm9
	subsd	 %xmm9, %xmm15

	mulsd	 -1 * SIZE(B), %xmm15
#endif

#ifdef RT
	mulsd	 -1 * SIZE(B), %xmm15

	movlpd	 -2 * SIZE(B), %xmm9
	mulsd	 %xmm15, %xmm9
	subsd	 %xmm9, %xmm14
	movlpd	 -3 * SIZE(B), %xmm10
	mulsd	 %xmm15, %xmm10
	subsd	 %xmm10, %xmm13
	movlpd	 -4 * SIZE(B), %xmm11
	mulsd	 %xmm15, %xmm11
	subsd	 %xmm11, %xmm12

	mulsd	 -6 * SIZE(B), %xmm14

	movlpd	 -7 * SIZE(B), %xmm9
	mulsd	 %xmm14, %xmm9
	subsd	 %xmm9, %xmm13
	movlpd	 -8 * SIZE(B), %xmm10
	mulsd	 %xmm14, %xmm10
	subsd	 %xmm10, %xmm12

	mulsd	-11 * SIZE(B), %xmm13

	movlpd	-12 * SIZE(B), %xmm9
	mulsd	 %xmm13, %xmm9
	subsd	 %xmm9, %xmm12

	mulsd	-16 * SIZE(B), %xmm12
#endif

#ifdef LN
	subq	$1 * SIZE, CO1
	subq	$1 * SIZE, CO2
#endif

	movsd	%xmm12,  0 * SIZE(CO1)
	movsd	%xmm13,  0 * SIZE(CO2)
	movsd	%xmm14,  0 * SIZE(CO1, LDC, 2)
	movsd	%xmm15,  0 * SIZE(CO2, LDC, 2)

#if defined(LN) || defined(LT)
	movsd	%xmm12,  -16 * SIZE(B)
	movsd	%xmm13,  -15 * SIZE(B)
	movsd	%xmm14,  -14 * SIZE(B)
	movsd	%xmm15,  -13 * SIZE(B)

	movsd	%xmm12,  -16 * SIZE(BO)
	movsd	%xmm12,  -15 * SIZE(BO)
	movsd	%xmm13,  -14 * SIZE(BO)
	movsd	%xmm13,  -13 * SIZE(BO)
	movsd	%xmm14,  -12 * SIZE(BO)
	movsd	%xmm14,  -11 * SIZE(BO)
	movsd	%xmm15,  -10 * SIZE(BO)
	movsd	%xmm15,   -9 * SIZE(BO)
#else
	movsd	%xmm12,  -16 * SIZE(AO)
	movsd	%xmm13,  -15 * SIZE(AO)
	movsd	%xmm14,  -14 * SIZE(AO)
	movsd	%xmm15,  -13 * SIZE(AO)
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
#ifdef LT
	addq	$4 * SIZE, B
#endif
#endif

#ifdef LN
	subq	$1, KK
	movq	BORIG, B
#endif

#ifdef LT
	addq	$1, KK
#endif

#ifdef RT
       movq	K, %rax
       movq	BORIG, B
       salq	$0 + BASE_SHIFT, %rax
       addq	%rax, AORIG
#endif
	ALIGN_4

.L20:
	testq	$2, M
	je	.L30
	ALIGN_4

.L21:
#ifdef LN
       movq	K, %rax
       salq	$1 + BASE_SHIFT, %rax
       subq	%rax, AORIG
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	movq	AORIG, AO
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
#endif

	leaq	16 * SIZE + BUFFER, BO

#if defined(LN) || defined(RT)
	movq	KK, %rax
	salq	$2 + BASE_SHIFT, %rax
	leaq	(BO, %rax, 2), BO
#endif

	pxor	%xmm8, %xmm8
	pxor	%xmm9, %xmm9
	pxor	%xmm10, %xmm10
	pxor	%xmm11, %xmm11

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
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)

	movapd	-16 * SIZE(AO), %xmm0
	movapd	-16 * SIZE(BO), %xmm2
	movapd	-14 * SIZE(BO), %xmm3
	movapd	-12 * SIZE(BO), %xmm4
	movapd	-10 * SIZE(BO), %xmm5

	mulpd	%xmm0, %xmm2
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm4
	mulpd	%xmm0, %xmm5

	addpd	%xmm2, %xmm8
	addpd	%xmm3, %xmm9
	addpd	%xmm4, %xmm10
	addpd	%xmm5, %xmm11

	movapd	-14 * SIZE(AO), %xmm0
	movapd	 -8 * SIZE(BO), %xmm2
	movapd	 -6 * SIZE(BO), %xmm3
	movapd	 -4 * SIZE(BO), %xmm4
	movapd	 -2 * SIZE(BO), %xmm5

	mulpd	%xmm0, %xmm2
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm4
	mulpd	%xmm0, %xmm5

	addpd	%xmm2, %xmm8
	addpd	%xmm3, %xmm9
	addpd	%xmm4, %xmm10
	addpd	%xmm5, %xmm11

	movapd	-12 * SIZE(AO), %xmm0
	movapd	  0 * SIZE(BO), %xmm2
	movapd	  2 * SIZE(BO), %xmm3
	movapd	  4 * SIZE(BO), %xmm4
	movapd	  6 * SIZE(BO), %xmm5

	mulpd	%xmm0, %xmm2
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm4
	mulpd	%xmm0, %xmm5

	addpd	%xmm2, %xmm8
	addpd	%xmm3, %xmm9
	addpd	%xmm4, %xmm10
	addpd	%xmm5, %xmm11

	movapd	-10 * SIZE(AO), %xmm0
	movapd	  8 * SIZE(BO), %xmm2
	movapd	 10 * SIZE(BO), %xmm3
	movapd	 12 * SIZE(BO), %xmm4
	movapd	 14 * SIZE(BO), %xmm5

	mulpd	%xmm0, %xmm2
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm4
	mulpd	%xmm0, %xmm5

	addpd	%xmm2, %xmm8
	addpd	%xmm3, %xmm9
	addpd	%xmm4, %xmm10
	addpd	%xmm5, %xmm11

	subq	$ -8 * SIZE, AO
	subq	$-32 * SIZE, BO
	subq	$1, %rax
	jne    .L22
	ALIGN_4

.L25:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	BRANCH
	je .L29
	ALIGN_4

.L26:
	movapd	-16 * SIZE(AO), %xmm0
	movapd	-16 * SIZE(BO), %xmm2
	movapd	-14 * SIZE(BO), %xmm3
	movapd	-12 * SIZE(BO), %xmm4
	movapd	-10 * SIZE(BO), %xmm5

	mulpd	%xmm0, %xmm2
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm4
	mulpd	%xmm0, %xmm5

	addpd	%xmm2, %xmm8
	addpd	%xmm3, %xmm9
	addpd	%xmm4, %xmm10
	addpd	%xmm5, %xmm11

	addq	$2 * SIZE, AO
	addq	$8 * SIZE, BO
	subq	$1, %rax
	jne    .L26
	ALIGN_4

.L29:
#if defined(LN) || defined(RT)
	movq	KK, %rax
#ifdef LN
	subq	$2, %rax
#else
	subq	$4, %rax
#endif

	movq	AORIG, AO
	movq	BORIG, B
	leaq	16 * SIZE + BUFFER, BO

	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(B,  %rax, 4), B
	leaq	(BO, %rax, 8), BO
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm8, %xmm0
	unpcklpd %xmm9, %xmm8
	unpckhpd %xmm9, %xmm0

	movapd	%xmm10, %xmm2
	unpcklpd %xmm11, %xmm10
	unpckhpd %xmm11, %xmm2

	movapd	 -16 * SIZE(B), %xmm9
	movapd	 -14 * SIZE(B), %xmm11
	movapd	 -12 * SIZE(B), %xmm13
	movapd	 -10 * SIZE(B), %xmm15

	subpd	%xmm8,  %xmm9
	subpd	%xmm10,  %xmm11
	subpd	%xmm0,  %xmm13
	subpd	%xmm2, %xmm15
#else
	movapd	 -16 * SIZE(AO), %xmm0
	movapd	 -14 * SIZE(AO), %xmm2
	movapd	 -12 * SIZE(AO), %xmm4
	movapd	 -10 * SIZE(AO), %xmm6

	subpd	%xmm8, %xmm0
	subpd	%xmm9, %xmm2
	subpd	%xmm10, %xmm4
	subpd	%xmm11, %xmm6
#endif

#ifdef LN
	movddup	 -13 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm13
	mulpd	 %xmm8, %xmm15

	movddup	 -14 * SIZE(AO), %xmm10
	movapd	 %xmm10, %xmm12
	mulpd	 %xmm13, %xmm10
	subpd	 %xmm10, %xmm9
	mulpd	 %xmm15, %xmm12
	subpd	 %xmm12, %xmm11

	movddup	 -16 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm9
	mulpd	 %xmm8, %xmm11
#endif

#ifdef LT
	movddup	 -16 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm9
	mulpd	 %xmm8, %xmm11

	movddup	 -15 * SIZE(AO), %xmm10
	movapd	 %xmm10, %xmm12
	mulpd	 %xmm9, %xmm10
	subpd	 %xmm10, %xmm13
	mulpd	 %xmm11, %xmm12
	subpd	 %xmm12, %xmm15

	movddup	 -13 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm13
	mulpd	 %xmm8, %xmm15
#endif

#ifdef RN
	movddup	 -16 * SIZE(B), %xmm8
	mulpd	 %xmm8, %xmm0

	movddup	 -15 * SIZE(B), %xmm9
	mulpd	 %xmm0, %xmm9
	subpd	 %xmm9, %xmm2
	movddup	 -14 * SIZE(B), %xmm10
	mulpd	 %xmm0, %xmm10
	subpd	 %xmm10, %xmm4
	movddup	 -13 * SIZE(B), %xmm11
	mulpd	 %xmm0, %xmm11
	subpd	 %xmm11, %xmm6

	movddup	 -11 * SIZE(B), %xmm8
	mulpd	 %xmm8, %xmm2
	movddup	 -10 * SIZE(B), %xmm9
	mulpd	 %xmm2, %xmm9
	subpd	 %xmm9, %xmm4
	movddup	  -9 * SIZE(B), %xmm10
	mulpd	 %xmm2, %xmm10
	subpd	 %xmm10, %xmm6

	movddup	  -6 * SIZE(B), %xmm8
	mulpd	 %xmm8, %xmm4

	movddup	  -5 * SIZE(B), %xmm9
	mulpd	 %xmm4, %xmm9
	subpd	 %xmm9, %xmm6

	movddup	  -1 * SIZE(B), %xmm8
	mulpd	 %xmm8, %xmm6
#endif

#ifdef RT
	movddup	-1 * SIZE(B), %xmm8
	mulpd	 %xmm8, %xmm6

	movddup	-2 * SIZE(B), %xmm9
	mulpd	 %xmm6, %xmm9
	subpd	 %xmm9, %xmm4
	movddup	-3 * SIZE(B), %xmm10
	mulpd	 %xmm6, %xmm10
	subpd	 %xmm10, %xmm2
	movddup	-4 * SIZE(B), %xmm11
	mulpd	 %xmm6, %xmm11
	subpd	 %xmm11, %xmm0

	movddup	-6 * SIZE(B), %xmm8
	mulpd	 %xmm8, %xmm4
	movddup	-7 * SIZE(B), %xmm9
	mulpd	 %xmm4, %xmm9
	subpd	 %xmm9, %xmm2
	movddup	-8 * SIZE(B), %xmm10
	mulpd	 %xmm4, %xmm10
	subpd	 %xmm10, %xmm0

	movddup	-11 * SIZE(B), %xmm8
	mulpd	 %xmm8, %xmm2
	movddup	-12 * SIZE(B), %xmm9
	mulpd	 %xmm2, %xmm9
	subpd	 %xmm9, %xmm0

	movddup	-16 * SIZE(B), %xmm8
	mulpd	 %xmm8, %xmm0
#endif

#ifdef LN
	subq	$2 * SIZE, CO1
	subq	$2 * SIZE, CO2
#endif

#if defined(LN) || defined(LT)
	movsd	%xmm9,  0 * SIZE(CO1)
	movsd	%xmm13,  1 * SIZE(CO1)

	movhpd	%xmm9,  0 * SIZE(CO2)
	movhpd	%xmm13,  1 * SIZE(CO2)

	movsd	%xmm11,  0 * SIZE(CO1, LDC, 2)
	movsd	%xmm15,  1 * SIZE(CO1, LDC, 2)

	movhpd	%xmm11,  0 * SIZE(CO2, LDC, 2)
	movhpd	%xmm15,  1 * SIZE(CO2, LDC, 2)
#else
	movsd	%xmm0,  0 * SIZE(CO1)
	movhpd	%xmm0,  1 * SIZE(CO1)

	movsd	%xmm2,  0 * SIZE(CO2)
	movhpd	%xmm2,  1 * SIZE(CO2)

	movsd	%xmm4,  0 * SIZE(CO1, LDC, 2)
	movhpd	%xmm4,  1 * SIZE(CO1, LDC, 2)

	movsd	%xmm6,  0 * SIZE(CO2, LDC, 2)
	movhpd	%xmm6,  1 * SIZE(CO2, LDC, 2)
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm9,  -16 * SIZE(B)
	movapd	%xmm11,  -14 * SIZE(B)
	movapd	%xmm13,  -12 * SIZE(B)
	movapd	%xmm15,  -10 * SIZE(B)

	movddup	%xmm9,  %xmm8
	SHUFPD_3 %xmm9,  %xmm9
	movddup	%xmm11,  %xmm10
	SHUFPD_3 %xmm11,  %xmm11
	movddup	%xmm13,  %xmm12
	SHUFPD_3 %xmm13,  %xmm13
	movddup	%xmm15,  %xmm14
	SHUFPD_3 %xmm15,  %xmm15

	movapd	%xmm8, -16 * SIZE(BO)
	movapd	%xmm9, -14 * SIZE(BO)
	movapd	%xmm10, -12 * SIZE(BO)
	movapd	%xmm11, -10 * SIZE(BO)
	movapd	%xmm12,  -8 * SIZE(BO)
	movapd	%xmm13,  -6 * SIZE(BO)
	movapd	%xmm14,  -4 * SIZE(BO)
	movapd	%xmm15,  -2 * SIZE(BO)
#else
	movapd	%xmm0,  -16 * SIZE(AO)
	movapd	%xmm2, -14 * SIZE(AO)
	movapd	%xmm4, -12 * SIZE(AO)
	movapd	%xmm6, -10 * SIZE(AO)
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
#ifdef LT
	addq	$8 * SIZE, B
#endif
#endif

#ifdef LN
	subq	$2, KK
	movq	BORIG, B
#endif

#ifdef LT
	addq	$2, KK
#endif

#ifdef RT
       movq	K, %rax
       movq	BORIG, B
       salq	$1 + BASE_SHIFT, %rax
       addq	%rax, AORIG
#endif
	ALIGN_4

.L30:
	movq	M,  I
	sarq	$2, I	# i = (m >> 2)
	jle	.L39
	ALIGN_4

.L11:
#ifdef LN
       movq	K, %rax
       salq	$2 + BASE_SHIFT, %rax
       subq	%rax, AORIG
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	movq	AORIG, AO
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
#endif

	leaq	16 * SIZE + BUFFER, BO

#if defined(LN) || defined(RT)
	movq	KK, %rax
	salq	$2 + BASE_SHIFT, %rax
	leaq	(BO, %rax, 2), BO
#endif

	prefetcht2	0 * SIZE(BB)

	pxor	%xmm8, %xmm8
	pxor	%xmm9, %xmm9
	pxor	%xmm10, %xmm10
	pxor	%xmm11, %xmm11

#ifdef LN
	prefetcht2     -3 * SIZE(CO1)
	pxor	%xmm12, %xmm12
	prefetcht2     -3 * SIZE(CO2)
	pxor	%xmm13, %xmm13
	prefetcht2     -3 * SIZE(CO1, LDC, 2)
	pxor	%xmm14, %xmm14
	prefetcht2     -3 * SIZE(CO2, LDC, 2)
	pxor	%xmm15, %xmm15
#else
	prefetcht2     3 * SIZE(CO1)
	pxor	%xmm12, %xmm12
	prefetcht2     3 * SIZE(CO2)
	pxor	%xmm13, %xmm13
	prefetcht2     3 * SIZE(CO1, LDC, 2)
	pxor	%xmm14, %xmm14
	prefetcht2     3 * SIZE(CO2, LDC, 2)
	pxor	%xmm15, %xmm15
#endif

	pxor	%xmm2, %xmm2
	pxor	%xmm3, %xmm3
	pxor	%xmm4, %xmm4
	pxor	%xmm5, %xmm5

	subq		$-8 * SIZE, BB

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	sarq	$2, %rax
	jle	.L15
	ALIGN_4

.L12:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)

	addpd	%xmm2, %xmm10
	movapd	-16 * SIZE(AO), %xmm0
	addpd	%xmm3, %xmm14
	movapd	-16 * SIZE(BO), %xmm2
	movapd	 %xmm2, %xmm3
	movapd	-14 * SIZE(AO), %xmm1
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm3
	addpd	%xmm4, %xmm11
	movapd	-14 * SIZE(BO), %xmm4
	addpd	%xmm5, %xmm15
	movapd	 %xmm4, %xmm5
	mulpd	%xmm0, %xmm4
	mulpd	%xmm1, %xmm5

	addpd	%xmm2, %xmm8
	addpd	%xmm3, %xmm12
	movapd	-12 * SIZE(BO), %xmm2
	movapd	 %xmm2, %xmm3
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm3
	addpd	%xmm4, %xmm9
	movapd	-10 * SIZE(BO), %xmm4
	addpd	%xmm5, %xmm13
	movapd	 %xmm4, %xmm5
	mulpd	%xmm0, %xmm4
	movapd	-12 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm5

	addpd	%xmm2, %xmm10
	addpd	%xmm3, %xmm14
	movapd	 -8 * SIZE(BO), %xmm2
	movapd	 %xmm2, %xmm3
	movapd	-10 * SIZE(AO), %xmm1
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm3
	addpd	%xmm4, %xmm11
	addpd	%xmm5, %xmm15
	movapd	-6 * SIZE(BO), %xmm4
	movapd	 %xmm4, %xmm5
	mulpd	%xmm0, %xmm4
	mulpd	%xmm1, %xmm5

	addpd	%xmm2, %xmm8
	addpd	%xmm3, %xmm12
	movapd	-4 * SIZE(BO), %xmm2
	movapd	 %xmm2, %xmm3
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm3
	addpd	%xmm4, %xmm9
	addpd	%xmm5, %xmm13
	movapd	-2 * SIZE(BO), %xmm4
	movapd	 %xmm4, %xmm5
	mulpd	%xmm0, %xmm4
	PREFETCH (PREFETCHSIZE +  8) * SIZE(AO)
	movapd	-8 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm5

	addpd	%xmm2, %xmm10
	addpd	%xmm3, %xmm14
	movapd	 0 * SIZE(BO), %xmm2
	movapd	 %xmm2, %xmm3
	movapd	-6 * SIZE(AO), %xmm1
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm3
	addpd	%xmm4, %xmm11
	addpd	%xmm5, %xmm15
	movapd	 2 * SIZE(BO), %xmm4
	movapd	 %xmm4, %xmm5
	mulpd	%xmm0, %xmm4
	mulpd	%xmm1, %xmm5

	addpd	%xmm2, %xmm8
	addpd	%xmm3, %xmm12
	movapd	 4 * SIZE(BO), %xmm2
	movapd	 %xmm2, %xmm3
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm3
	addpd	%xmm4, %xmm9
	addpd	%xmm5, %xmm13
	movapd	 6 * SIZE(BO), %xmm4
	movapd	 %xmm4, %xmm5
	mulpd	%xmm0, %xmm4
	movapd	-4 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm5

	addpd	%xmm2, %xmm10
	addpd	%xmm3, %xmm14
	movapd	 8 * SIZE(BO), %xmm2
	movapd	 %xmm2, %xmm3
	movapd	-2 * SIZE(AO), %xmm1
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm3
	addpd	%xmm4, %xmm11
	addpd	%xmm5, %xmm15
	movapd	10 * SIZE(BO), %xmm4
	movapd	 %xmm4, %xmm5
	addq	$32 * SIZE, BO
	mulpd	%xmm0, %xmm4
	mulpd	%xmm1, %xmm5

	addpd	%xmm2, %xmm8
	addpd	%xmm3, %xmm12
	movapd	-20 * SIZE(BO), %xmm2
	movapd	 %xmm2, %xmm3
	mulpd	%xmm0, %xmm2
	subq	$-16 * SIZE, AO
	mulpd	%xmm1, %xmm3
	addpd	%xmm4, %xmm9
	addpd	%xmm5, %xmm13
	movapd	-18 * SIZE(BO), %xmm4
	movapd	 %xmm4, %xmm5
	mulpd	%xmm0, %xmm4
	subq	$1, %rax
	mulpd	%xmm1, %xmm5

	BRANCH
	jg	.L12
	ALIGN_4

.L15:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	BRANCH
	je .L19
	ALIGN_4

.L16:
	movapd	-16 * SIZE(AO), %xmm0
	addpd	%xmm2, %xmm10
	movapd	-16 * SIZE(BO), %xmm2
	addpd	%xmm3, %xmm14
	movapd	 %xmm2, %xmm3
	movapd	-14 * SIZE(AO), %xmm1
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm3
	addpd	%xmm4, %xmm11
	movapd	-14 * SIZE(BO), %xmm4
	addpd	%xmm5, %xmm15
	movapd	 %xmm4, %xmm5
	mulpd	%xmm0, %xmm4
	mulpd	%xmm1, %xmm5

	addpd	%xmm2, %xmm8
	movapd	-12 * SIZE(BO), %xmm2
	addpd	%xmm3, %xmm12
	movapd	 %xmm2, %xmm3
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm3
	addpd	%xmm4, %xmm9
	movapd	-10 * SIZE(BO), %xmm4
	addpd	%xmm5, %xmm13
	movapd	 %xmm4, %xmm5
	mulpd	%xmm0, %xmm4
	mulpd	%xmm1, %xmm5

	addq	$4 * SIZE, AO		# aoffset  += 4
	addq	$8 * SIZE, BO		# boffset1 += 8
	subq	$1, %rax
	BRANCH
	jg	.L16
	ALIGN_4

.L19:
	addpd	%xmm2, %xmm10
	addpd	%xmm3, %xmm14
	addpd	%xmm4, %xmm11
	addpd	%xmm5, %xmm15

#if defined(LN) || defined(RT)
	movq	KK, %rax
#ifdef LN
	subq	$4, %rax
#else
	subq	$4, %rax
#endif

	movq	AORIG, AO
	movq	BORIG, B
	leaq	16 * SIZE + BUFFER, BO

	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(B,  %rax, 4), B
	leaq	(BO, %rax, 8), BO
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm8, %xmm0
	unpcklpd %xmm9, %xmm8
	unpckhpd %xmm9, %xmm0

	movapd	%xmm10, %xmm2
	unpcklpd %xmm11, %xmm10
	unpckhpd %xmm11, %xmm2

	movapd	%xmm12, %xmm4
	unpcklpd %xmm13, %xmm12
	unpckhpd %xmm13, %xmm4

	movapd	%xmm14, %xmm6
	unpcklpd %xmm15, %xmm14
	unpckhpd %xmm15, %xmm6

	movapd	-16 * SIZE(B), %xmm9
	movapd	-14 * SIZE(B), %xmm11
	movapd	-12 * SIZE(B), %xmm13
	movapd	-10 * SIZE(B), %xmm15
	movapd	 -8 * SIZE(B), %xmm1
	movapd	 -6 * SIZE(B), %xmm3
	movapd	 -4 * SIZE(B), %xmm5
	movapd	 -2 * SIZE(B), %xmm7

	subpd	%xmm8,  %xmm9
	subpd	%xmm10,  %xmm11
	subpd	%xmm0,  %xmm13
	subpd	%xmm2, %xmm15
	subpd	%xmm12,  %xmm1
	subpd	%xmm14,  %xmm3
	subpd	%xmm4, %xmm5
	subpd	%xmm6, %xmm7
#else
	movapd	 -16 * SIZE(AO), %xmm0
	movapd	 -14 * SIZE(AO), %xmm1
	movapd	 -12 * SIZE(AO), %xmm2
	movapd	 -10 * SIZE(AO), %xmm3

	movapd	  -8 * SIZE(AO), %xmm4
	movapd	  -6 * SIZE(AO), %xmm5
	movapd	  -4 * SIZE(AO), %xmm6
	movapd	  -2 * SIZE(AO), %xmm7

	subpd	%xmm8, %xmm0
	subpd	%xmm12, %xmm1
	subpd	%xmm9, %xmm2
	subpd	%xmm13, %xmm3
	subpd	%xmm10, %xmm4
	subpd	%xmm14, %xmm5
	subpd	%xmm11, %xmm6
	subpd	%xmm15, %xmm7
#endif

#ifdef LN
	movddup	-1 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm5
	mulpd	 %xmm8, %xmm7

	movddup	-2 * SIZE(AO), %xmm10
	movapd	%xmm10, %xmm12
	mulpd	 %xmm5, %xmm10
	subpd	 %xmm10, %xmm1
	mulpd	 %xmm7, %xmm12
	subpd	 %xmm12, %xmm3

	movddup	-3 * SIZE(AO), %xmm10
	movapd	%xmm10, %xmm12
	mulpd	 %xmm5, %xmm10
	subpd	 %xmm10, %xmm13
	mulpd	 %xmm7, %xmm12
	subpd	 %xmm12, %xmm15

	movddup	-4 * SIZE(AO), %xmm10
	movapd	%xmm10, %xmm12
	mulpd	 %xmm5, %xmm10
	subpd	 %xmm10, %xmm9
	mulpd	 %xmm7, %xmm12
	subpd	 %xmm12, %xmm11

	movddup	-6 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm1
	mulpd	 %xmm8, %xmm3

	movddup	-7 * SIZE(AO), %xmm10
	movapd	%xmm10, %xmm12
	mulpd	 %xmm1, %xmm10
	subpd	 %xmm10, %xmm13
	mulpd	 %xmm3, %xmm12
	subpd	 %xmm12, %xmm15

	movddup	-8 * SIZE(AO), %xmm10
	movapd	%xmm10, %xmm12
	mulpd	 %xmm1, %xmm10
	subpd	 %xmm10, %xmm9
	mulpd	 %xmm3, %xmm12
	subpd	 %xmm12, %xmm11

	movddup	-11 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm13
	mulpd	 %xmm8, %xmm15

	movddup	-12 * SIZE(AO), %xmm10
	movapd	%xmm10, %xmm12
	mulpd	 %xmm13, %xmm10
	subpd	 %xmm10, %xmm9
	mulpd	 %xmm15, %xmm12
	subpd	 %xmm12, %xmm11

	movddup	-16 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm9
	mulpd	 %xmm8, %xmm11
#endif

#ifdef LT
	movddup	-16 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm9
	mulpd	 %xmm8, %xmm11

	movddup	-15 * SIZE(AO), %xmm10
	movapd	%xmm10, %xmm12
	mulpd	 %xmm9, %xmm10
	subpd	 %xmm10, %xmm13
	mulpd	 %xmm11, %xmm12
	subpd	 %xmm12, %xmm15

	movddup	-14 * SIZE(AO), %xmm10
	movapd	%xmm10, %xmm12
	mulpd	 %xmm9, %xmm10
	subpd	 %xmm10, %xmm1
	mulpd	 %xmm11, %xmm12
	subpd	 %xmm12, %xmm3

	movddup	-13 * SIZE(AO), %xmm10
	movapd	%xmm10, %xmm12
	mulpd	 %xmm9, %xmm10
	subpd	 %xmm10, %xmm5
	mulpd	 %xmm11, %xmm12
	subpd	 %xmm12, %xmm7

	movddup	-11 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm13
	mulpd	 %xmm8, %xmm15

	movddup	-10 * SIZE(AO), %xmm10
	movapd	%xmm10, %xmm12
	mulpd	 %xmm13, %xmm10
	subpd	 %xmm10, %xmm1
	mulpd	 %xmm15, %xmm12
	subpd	 %xmm12, %xmm3

	movddup	 -9 * SIZE(AO), %xmm10
	movapd	%xmm10, %xmm12
	mulpd	 %xmm13, %xmm10
	subpd	 %xmm10, %xmm5
	mulpd	 %xmm15, %xmm12
	subpd	 %xmm12, %xmm7

	movddup	 -6 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm1
	mulpd	 %xmm8, %xmm3

	movddup	 -5 * SIZE(AO), %xmm10
	movapd	%xmm10, %xmm12
	mulpd	 %xmm1, %xmm10
	subpd	 %xmm10, %xmm5
	mulpd	 %xmm3, %xmm12
	subpd	 %xmm12, %xmm7

	movddup	 -1 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm5
	mulpd	 %xmm8, %xmm7
#endif


#ifdef RN
	movddup	-16 * SIZE(B), %xmm8
	mulpd	 %xmm8, %xmm0
	mulpd	 %xmm8, %xmm1

	movddup	 -15 * SIZE(B), %xmm10
	movapd	%xmm10, %xmm12
	mulpd	 %xmm0, %xmm10
	subpd	 %xmm10, %xmm2
	mulpd	 %xmm1, %xmm12
	subpd	 %xmm12, %xmm3

	movddup	 -14 * SIZE(B), %xmm10
	movapd	%xmm10, %xmm12
	mulpd	 %xmm0, %xmm10
	subpd	 %xmm10, %xmm4
	mulpd	 %xmm1, %xmm12
	subpd	 %xmm12, %xmm5

	movddup	 -13 * SIZE(B), %xmm10
	movapd	%xmm10, %xmm12
	mulpd	 %xmm0, %xmm10
	subpd	 %xmm10, %xmm6
	mulpd	 %xmm1, %xmm12
	subpd	 %xmm12, %xmm7

	movddup	 -11 * SIZE(B), %xmm8
	mulpd	 %xmm8, %xmm2
	mulpd	 %xmm8, %xmm3

	movddup	 -10 * SIZE(B), %xmm10
	movapd	%xmm10, %xmm12
	mulpd	 %xmm2, %xmm10
	subpd	 %xmm10, %xmm4
	mulpd	 %xmm3, %xmm12
	subpd	 %xmm12, %xmm5

	movddup	  -9 * SIZE(B), %xmm10
	movapd	%xmm10, %xmm12
	mulpd	 %xmm2, %xmm10
	subpd	 %xmm10, %xmm6
	mulpd	 %xmm3, %xmm12
	subpd	 %xmm12, %xmm7

	movddup	  -6 * SIZE(B), %xmm8
	mulpd	 %xmm8, %xmm4
	mulpd	 %xmm8, %xmm5

	movddup	  -5 * SIZE(B), %xmm10
	movapd	%xmm10, %xmm12
	mulpd	 %xmm4, %xmm10
	subpd	 %xmm10, %xmm6
	mulpd	 %xmm5, %xmm12
	subpd	 %xmm12, %xmm7

	movddup	  -1 * SIZE(B), %xmm8
	mulpd	 %xmm8, %xmm6
	mulpd	 %xmm8, %xmm7
#endif

#ifdef RT
	movddup	 -1 * SIZE(B), %xmm8
	mulpd	 %xmm8, %xmm6
	mulpd	 %xmm8, %xmm7

	movddup	 -2 * SIZE(B), %xmm10
	movapd	%xmm10, %xmm12
	mulpd	 %xmm6, %xmm10
	subpd	 %xmm10, %xmm4
	mulpd	 %xmm7, %xmm12
	subpd	 %xmm12, %xmm5

	movddup	 -3 * SIZE(B), %xmm10
	movapd	%xmm10, %xmm12
	mulpd	 %xmm6, %xmm10
	subpd	 %xmm10, %xmm2
	mulpd	 %xmm7, %xmm12
	subpd	 %xmm12, %xmm3

	movddup	 -4 * SIZE(B), %xmm10
	movapd	%xmm10, %xmm12
	mulpd	 %xmm6, %xmm10
	subpd	 %xmm10, %xmm0
	mulpd	 %xmm7, %xmm12
	subpd	 %xmm12, %xmm1

	movddup	 -6 * SIZE(B), %xmm8
	mulpd	 %xmm8, %xmm4
	mulpd	 %xmm8, %xmm5

	movddup	 -7 * SIZE(B), %xmm10
	movapd	%xmm10, %xmm12
	mulpd	 %xmm4, %xmm10
	subpd	 %xmm10, %xmm2
	mulpd	 %xmm5, %xmm12
	subpd	 %xmm12, %xmm3

	movddup	 -8 * SIZE(B), %xmm10
	movapd	%xmm10, %xmm12
	mulpd	 %xmm4, %xmm10
	subpd	 %xmm10, %xmm0
	mulpd	 %xmm5, %xmm12
	subpd	 %xmm12, %xmm1

	movddup	-11 * SIZE(B), %xmm8
	mulpd	 %xmm8, %xmm2
	mulpd	 %xmm8, %xmm3

	movddup	-12 * SIZE(B), %xmm10
	movapd	%xmm10, %xmm12
	mulpd	 %xmm2, %xmm10
	subpd	 %xmm10, %xmm0
	mulpd	 %xmm3, %xmm12
	subpd	 %xmm12, %xmm1

	movddup	-16 * SIZE(B), %xmm8
	mulpd	 %xmm8, %xmm0
	mulpd	 %xmm8, %xmm1
#endif

#ifdef LN
	subq	$4 * SIZE, CO1
	subq	$4 * SIZE, CO2
#endif

#if defined(LN) || defined(LT)
	movsd	%xmm9,  0 * SIZE(CO1)
	movsd	%xmm13,  1 * SIZE(CO1)
	movsd	%xmm1,  2 * SIZE(CO1)
	movsd	%xmm5, 3 * SIZE(CO1)

	movhpd	%xmm9,  0 * SIZE(CO2)
	movhpd	%xmm13,  1 * SIZE(CO2)
	movhpd	%xmm1,  2 * SIZE(CO2)
	movhpd	%xmm5, 3 * SIZE(CO2)

	movsd	%xmm11,  0 * SIZE(CO1, LDC, 2)
	movsd	%xmm15,  1 * SIZE(CO1, LDC, 2)
	movsd	%xmm3, 2 * SIZE(CO1, LDC, 2)
	movsd	%xmm7, 3 * SIZE(CO1, LDC, 2)

	movhpd	%xmm11,  0 * SIZE(CO2, LDC, 2)
	movhpd	%xmm15,  1 * SIZE(CO2, LDC, 2)
	movhpd	%xmm3, 2 * SIZE(CO2, LDC, 2)
	movhpd	%xmm7, 3 * SIZE(CO2, LDC, 2)
#else
	movsd	%xmm0,  0 * SIZE(CO1)
	movhpd	%xmm0,  1 * SIZE(CO1)
	movsd	%xmm1,  2 * SIZE(CO1)
	movhpd	%xmm1,  3 * SIZE(CO1)

	movsd	%xmm2,  0 * SIZE(CO2)
	movhpd	%xmm2,  1 * SIZE(CO2)
	movsd	%xmm3,  2 * SIZE(CO2)
	movhpd	%xmm3,  3 * SIZE(CO2)

	movsd	%xmm4,  0 * SIZE(CO1, LDC, 2)
	movhpd	%xmm4,  1 * SIZE(CO1, LDC, 2)
	movsd	%xmm5,  2 * SIZE(CO1, LDC, 2)
	movhpd	%xmm5,  3 * SIZE(CO1, LDC, 2)

	movsd	%xmm6,  0 * SIZE(CO2, LDC, 2)
	movhpd	%xmm6,  1 * SIZE(CO2, LDC, 2)
	movsd	%xmm7,  2 * SIZE(CO2, LDC, 2)
	movhpd	%xmm7,  3 * SIZE(CO2, LDC, 2)
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm9, -16 * SIZE(B)
	movapd	%xmm11, -14 * SIZE(B)
	movapd	%xmm13, -12 * SIZE(B)
	movapd	%xmm15, -10 * SIZE(B)
	movapd	%xmm1,  -8 * SIZE(B)
	movapd	%xmm3, -6 * SIZE(B)
	movapd	%xmm5, -4 * SIZE(B)
	movapd	%xmm7, -2 * SIZE(B)

	movddup	%xmm9,  %xmm8
	SHUFPD_3 %xmm9,  %xmm9
	movddup	%xmm11,  %xmm10
	SHUFPD_3 %xmm11,  %xmm11
	movddup	%xmm13,  %xmm12
	SHUFPD_3 %xmm13,  %xmm13
	movddup	%xmm15,  %xmm14
	SHUFPD_3 %xmm15,  %xmm15
	movddup	%xmm1,  %xmm0
	SHUFPD_3 %xmm1,  %xmm1
	movddup	%xmm3, %xmm2
	SHUFPD_3 %xmm3, %xmm3
	movddup	%xmm5, %xmm4
	SHUFPD_3 %xmm5, %xmm5
	movddup	%xmm7, %xmm6
	SHUFPD_3 %xmm7, %xmm7

	movapd	%xmm8, -16 * SIZE(BO)
	movapd	%xmm9, -14 * SIZE(BO)
	movapd	%xmm10, -12 * SIZE(BO)
	movapd	%xmm11, -10 * SIZE(BO)
	movapd	%xmm12,  -8 * SIZE(BO)
	movapd	%xmm13,  -6 * SIZE(BO)
	movapd	%xmm14,  -4 * SIZE(BO)
	movapd	%xmm15,  -2 * SIZE(BO)
	movapd	%xmm0,   0 * SIZE(BO)
	movapd	%xmm1,   2 * SIZE(BO)
	movapd	%xmm2,  4 * SIZE(BO)
	movapd	%xmm3,  6 * SIZE(BO)
	movapd	%xmm4,  8 * SIZE(BO)
	movapd	%xmm5, 10 * SIZE(BO)
	movapd	%xmm6, 12 * SIZE(BO)
	movapd	%xmm7, 14 * SIZE(BO)
#else
	movapd	%xmm0,  -16 * SIZE(AO)
	movapd	%xmm1,  -14 * SIZE(AO)
	movapd	%xmm2, -12 * SIZE(AO)
	movapd	%xmm3, -10 * SIZE(AO)
	movapd	%xmm4,  -8 * SIZE(AO)
	movapd	%xmm5,  -6 * SIZE(AO)
	movapd	%xmm6,  -4 * SIZE(AO)
	movapd	%xmm7,  -2 * SIZE(AO)
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
#ifdef LT
	addq	$16 * SIZE, B
#endif
#endif

#ifdef LN
	subq	$4, KK
	movq	BORIG, B
#endif

#ifdef LT
	addq	$4, KK
#endif

#ifdef RT
	movq	K, %rax
	movq	BORIG, B
	salq	$2 + BASE_SHIFT, %rax
	addq	%rax, AORIG
#endif

	decq	I			# i --
	jg	.L11
	ALIGN_4


.L39:
#ifdef LN
       leaq	(, K, SIZE), %rax
       leaq	(B, %rax, 4), B
#endif

#if defined(LT) || defined(RN)
	movq	K,  %rax
	subq	KK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(B,  %rax, 4), B
#endif

#ifdef RN
	addq	$4, KK
#endif

#ifdef RT
	subq	$4, KK
#endif

	decq	J			# j --
	jg	.L01
	ALIGN_4

.L40:
	testq	$3, N
	je	.L999

	testq	$2, N
	je	.L80
	ALIGN_4

.L41:
/* Copying to Sub Buffer */

#ifdef LN
	movq	OFFSET, %rax
	addq	M, %rax
	movq	%rax, KK
#endif

	leaq	BUFFER, BO

#ifdef RT
       movq	K, %rax
       salq	$1 + BASE_SHIFT, %rax
       subq	%rax, B
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	movq	B, BORIG
	leaq	(, %rax, SIZE), %rax
	leaq	(B,  %rax, 2), B
	leaq	(BO, %rax, 4), BO
#endif

#ifdef LT
	movq	OFFSET, %rax
	movq	%rax, KK
#endif

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	sarq	$2, %rax
	jle	.L43
	ALIGN_4

.L42:
	movddup	 -16 * SIZE(B), %xmm0
	movddup	 -15 * SIZE(B), %xmm1
	movddup	 -14 * SIZE(B), %xmm2
	movddup	 -13 * SIZE(B), %xmm3
	movddup	 -12 * SIZE(B), %xmm4
	movddup	 -11 * SIZE(B), %xmm5
	movddup	 -10 * SIZE(B), %xmm6
	movddup	  -9 * SIZE(B), %xmm7

	movapd	%xmm0,  0 * SIZE(BO)
	movapd	%xmm1,  2 * SIZE(BO)
	movapd	%xmm2,  4 * SIZE(BO)
	movapd	%xmm3,  6 * SIZE(BO)
	movapd	%xmm4,  8 * SIZE(BO)
	movapd	%xmm5, 10 * SIZE(BO)
	movapd	%xmm6, 12 * SIZE(BO)
	movapd	%xmm7, 14 * SIZE(BO)

	addq	$8 * SIZE, B
	addq	$16 * SIZE, BO
	subq	$1, %rax
	jne	.L42
	ALIGN_4

.L43:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$3, %rax
	BRANCH
	jle	.L50
	ALIGN_4

.L44:
	movddup	 -16 * SIZE(B), %xmm0
	movddup	 -15 * SIZE(B), %xmm1

	movapd	%xmm0,  0 * SIZE(BO)
	movapd	%xmm1,  2 * SIZE(BO)

	addq	$2 * SIZE, B
	addq	$4 * SIZE, BO
	decq	%rax
	jne	.L44
	ALIGN_4

.L50:
#if defined(LT) || defined(RN)
	movq	A, AO
#else
	movq	A, AORIG
#endif

#ifdef RT
       leaq	(, LDC, 2), %rax
       subq	%rax, C
#endif

	movq	C, CO1			# coffset1 = c
	leaq	(C, LDC, 1), CO2	# coffset2 = c + ldc
#ifndef RT
	leaq	(C, LDC, 2), C
#endif

	testq	$1, M
	je	.L60
	ALIGN_4

.L71:
#ifdef LN
       movq	K, %rax
       salq	$0 + BASE_SHIFT, %rax
       subq	%rax, AORIG
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	movq	AORIG, AO
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 1), AO
#endif

	leaq	16 * SIZE + BUFFER, BO

#if defined(LN) || defined(RT)
	movq	KK, %rax
	salq	$1 + BASE_SHIFT, %rax
	leaq	(BO, %rax, 2), BO
#endif

	pxor	%xmm8, %xmm8
	pxor	%xmm9, %xmm9
	pxor	%xmm10, %xmm10
	pxor	%xmm11, %xmm11

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	sarq	$2, %rax
	je	.L75
	ALIGN_4

.L72:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)

	movsd	-16 * SIZE(AO), %xmm0
	movsd	-15 * SIZE(AO), %xmm1
	movsd	-16 * SIZE(BO), %xmm2
	movsd	-14 * SIZE(BO), %xmm3
	movsd	-12 * SIZE(BO), %xmm4
	movsd	-10 * SIZE(BO), %xmm5

	mulsd	%xmm0, %xmm2
	mulsd	%xmm0, %xmm3
	mulsd	%xmm1, %xmm4
	mulsd	%xmm1, %xmm5

	addsd	%xmm2, %xmm8
	addsd	%xmm3, %xmm9
	addsd	%xmm4, %xmm10
	addsd	%xmm5, %xmm11

	movsd	-14 * SIZE(AO), %xmm0
	movsd	-13 * SIZE(AO), %xmm1
	movsd	 -8 * SIZE(BO), %xmm2
	movsd	 -6 * SIZE(BO), %xmm3
	movsd	 -4 * SIZE(BO), %xmm4
	movsd	 -2 * SIZE(BO), %xmm5

	mulsd	%xmm0, %xmm2
	mulsd	%xmm0, %xmm3
	mulsd	%xmm1, %xmm4
	mulsd	%xmm1, %xmm5

	addsd	%xmm2, %xmm8
	addsd	%xmm3, %xmm9
	addsd	%xmm4, %xmm10
	addsd	%xmm5, %xmm11

	subq	$ -4 * SIZE, AO
	subq	$-16 * SIZE, BO
	subq	$1, %rax
	jne    .L72
	ALIGN_4

.L75:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	BRANCH
	je .L78
	ALIGN_4

.L76:
	movsd	-16 * SIZE(AO), %xmm0
	movsd	-16 * SIZE(BO), %xmm2
	movsd	-14 * SIZE(BO), %xmm3

	mulsd	%xmm0, %xmm2
	mulsd	%xmm0, %xmm3

	addsd	%xmm2, %xmm8
	addsd	%xmm3, %xmm9

	addq	$1 * SIZE, AO
	addq	$4 * SIZE, BO
	subq	$1, %rax
	jg	.L76
	ALIGN_4

.L78:
	addsd	%xmm10, %xmm8
	addsd	%xmm11, %xmm9

#if defined(LN) || defined(RT)
	movq	KK, %rax
#ifdef LN
	subq	$1, %rax
#else
	subq	$2, %rax
#endif

	movq	AORIG, AO
	movq	BORIG, B
	leaq	16 * SIZE + BUFFER, BO

	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(B,  %rax, 2), B
	leaq	(BO, %rax, 4), BO
#endif

#if defined(LN) || defined(LT)
	movsd	-16 * SIZE(B), %xmm12
	movsd	-15 * SIZE(B), %xmm13
#else
	movsd	-16 * SIZE(AO), %xmm12
	movsd	-15 * SIZE(AO), %xmm13
#endif

	subsd	%xmm8,  %xmm12
	subsd	%xmm9,  %xmm13

#ifdef LN
	movsd	-16 * SIZE(AO), %xmm8

	mulsd	 %xmm8, %xmm12
	mulsd	 %xmm8, %xmm13
#endif

#ifdef LT
	movsd	-16 * SIZE(AO), %xmm8

	mulsd	 %xmm8, %xmm12
	mulsd	 %xmm8, %xmm13
#endif

#ifdef RN
	mulsd	-16 * SIZE(B), %xmm12
	movsd	-15 * SIZE(B), %xmm9
	mulsd	 %xmm12, %xmm9
	subsd	 %xmm9, %xmm13

	mulsd	-13 * SIZE(B), %xmm13
#endif

#ifdef RT
	mulsd	-13 * SIZE(B), %xmm13

	movlpd	-14 * SIZE(B), %xmm9
	mulsd	 %xmm13, %xmm9
	subsd	 %xmm9, %xmm12

	mulsd	-16 * SIZE(B), %xmm12
#endif

#ifdef LN
	subq	$1 * SIZE, CO1
	subq	$1 * SIZE, CO2
#endif

	movsd	%xmm12,  0 * SIZE(CO1)
	movsd	%xmm13,  0 * SIZE(CO2)

#if defined(LN) || defined(LT)
	movsd	%xmm12,  -16 * SIZE(B)
	movsd	%xmm13,  -15 * SIZE(B)

	movsd	%xmm12,  -16 * SIZE(BO)
	movsd	%xmm12,  -15 * SIZE(BO)
	movsd	%xmm13,  -14 * SIZE(BO)
	movsd	%xmm13,  -13 * SIZE(BO)
#else
	movsd	%xmm12,  -16 * SIZE(AO)
	movsd	%xmm13,  -15 * SIZE(AO)
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
#ifdef LT
	addq	$2 * SIZE, B
#endif
#endif

#ifdef LN
	subq	$1, KK
	movq	BORIG, B
#endif

#ifdef LT
	addq	$1, KK
#endif

#ifdef RT
	movq	K, %rax
	movq	BORIG, B
	salq	$0 + BASE_SHIFT, %rax
	addq	%rax, AORIG
#endif
	ALIGN_4

.L60:
	testq	$2, M
	je	.L70
	ALIGN_4

.L61:
#ifdef LN
       movq	K, %rax
       salq	$1 + BASE_SHIFT, %rax
       subq	%rax, AORIG
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	movq	AORIG, AO
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
#endif

	leaq	16 * SIZE + BUFFER, BO

#if defined(LN) || defined(RT)
	movq	KK, %rax
	salq	$1 + BASE_SHIFT, %rax
	leaq	(BO, %rax, 2), BO
#endif

	pxor	%xmm8, %xmm8
	pxor	%xmm9, %xmm9
	pxor	%xmm10, %xmm10
	pxor	%xmm11, %xmm11

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
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)

	movapd	-16 * SIZE(AO), %xmm0
	movapd	-14 * SIZE(AO), %xmm1
	movapd	-16 * SIZE(BO), %xmm2
	movapd	-14 * SIZE(BO), %xmm3
	movapd	-12 * SIZE(BO), %xmm4
	movapd	-10 * SIZE(BO), %xmm5

	mulpd	%xmm0, %xmm2
	mulpd	%xmm0, %xmm3
	mulpd	%xmm1, %xmm4
	mulpd	%xmm1, %xmm5

	addpd	%xmm2, %xmm8
	addpd	%xmm3, %xmm9
	addpd	%xmm4, %xmm10
	addpd	%xmm5, %xmm11

	movapd	-12 * SIZE(AO), %xmm0
	movapd	-10 * SIZE(AO), %xmm1
	movapd	 -8 * SIZE(BO), %xmm2
	movapd	 -6 * SIZE(BO), %xmm3
	movapd	 -4 * SIZE(BO), %xmm4
	movapd	 -2 * SIZE(BO), %xmm5

	mulpd	%xmm0, %xmm2
	mulpd	%xmm0, %xmm3
	mulpd	%xmm1, %xmm4
	mulpd	%xmm1, %xmm5

	addpd	%xmm2, %xmm8
	addpd	%xmm3, %xmm9
	addpd	%xmm4, %xmm10
	addpd	%xmm5, %xmm11

	subq	$ -8 * SIZE, AO
	subq	$-16 * SIZE, BO
	subq	$1, %rax
	jne    .L62
	ALIGN_4

.L65:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	BRANCH
	je .L69
	ALIGN_4

.L66:
	movapd	-16 * SIZE(AO), %xmm0
	movapd	-16 * SIZE(BO), %xmm2
	movapd	-14 * SIZE(BO), %xmm3

	mulpd	%xmm0, %xmm2
	mulpd	%xmm0, %xmm3

	addpd	%xmm2, %xmm8
	addpd	%xmm3, %xmm9

	addq	$2 * SIZE, AO
	addq	$4 * SIZE, BO
	subq	$1, %rax
	jg	.L66
	ALIGN_4

.L69:
	addpd	%xmm10, %xmm8
	addpd	%xmm11, %xmm9

#if defined(LN) || defined(RT)
	movq	KK, %rax
#ifdef LN
	subq	$2, %rax
#else
	subq	$2, %rax
#endif

	movq	AORIG, AO
	movq	BORIG, B
	leaq	16 * SIZE + BUFFER, BO

	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(B,  %rax, 2), B
	leaq	(BO, %rax, 4), BO
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm8, %xmm0
	unpcklpd %xmm9, %xmm8
	unpckhpd %xmm9, %xmm0

	movapd	 -16 * SIZE(B), %xmm9
	movapd	 -14 * SIZE(B), %xmm13

	subpd	%xmm8,  %xmm9
	subpd	%xmm0,  %xmm13
#else
	movapd	-16 * SIZE(AO), %xmm0
	movapd	-14 * SIZE(AO), %xmm2

	subpd	%xmm8, %xmm0
	subpd	%xmm9, %xmm2
#endif


#ifdef LN
	movddup	-13 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm13

	movddup	-14 * SIZE(AO), %xmm10
	mulpd	 %xmm13, %xmm10
	subpd	 %xmm10, %xmm9

	movddup	-16 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm9
#endif

#ifdef LT
	movddup	-16 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm9

	movddup	-15 * SIZE(AO), %xmm10
	mulpd	 %xmm9, %xmm10
	subpd	 %xmm10, %xmm13

	movddup	-13 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm13
#endif

#ifdef RN
	movddup	-16 * SIZE(B), %xmm8
	mulpd	 %xmm8, %xmm0

	movddup	-15 * SIZE(B), %xmm9
	mulpd	 %xmm0, %xmm9
	subpd	 %xmm9, %xmm2

	movddup	-13 * SIZE(B), %xmm8
	mulpd	 %xmm8, %xmm2
#endif

#ifdef RT
	movddup	-13 * SIZE(B), %xmm8
	mulpd	 %xmm8, %xmm2

	movddup	-14 * SIZE(B), %xmm9
	mulpd	 %xmm2, %xmm9
	subpd	 %xmm9, %xmm0

	movddup	-16 * SIZE(B), %xmm8
	mulpd	 %xmm8, %xmm0
#endif

#ifdef LN
	subq	$2 * SIZE, CO1
	subq	$2 * SIZE, CO2
#endif

#if defined(LN) || defined(LT)
	movsd	%xmm9,  0 * SIZE(CO1)
	movsd	%xmm13,  1 * SIZE(CO1)

	movhpd	%xmm9,  0 * SIZE(CO2)
	movhpd	%xmm13,  1 * SIZE(CO2)
#else
	movsd	%xmm0,  0 * SIZE(CO1)
	movhpd	%xmm0,  1 * SIZE(CO1)

	movsd	%xmm2,  0 * SIZE(CO2)
	movhpd	%xmm2,  1 * SIZE(CO2)
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm9,  -16 * SIZE(B)
	movapd	%xmm13,  -14 * SIZE(B)

	movddup	%xmm9,  %xmm8
	SHUFPD_3 %xmm9,  %xmm9
	movddup	%xmm13,  %xmm12
	SHUFPD_3 %xmm13,  %xmm13

	movapd	%xmm8,  -16 * SIZE(BO)
	movapd	%xmm9,  -14 * SIZE(BO)
	movapd	%xmm12,  -12 * SIZE(BO)
	movapd	%xmm13,  -10 * SIZE(BO)
#else
	movapd	%xmm0,  -16 * SIZE(AO)
	movapd	%xmm2, -14 * SIZE(AO)
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
#ifdef LT
	addq	$4 * SIZE, B
#endif
#endif

#ifdef LN
	subq	$2, KK
	movq	BORIG, B
#endif

#ifdef LT
	addq	$2, KK
#endif

#ifdef RT
	movq	K, %rax
	movq	BORIG, B
	salq	$1 + BASE_SHIFT, %rax
	addq	%rax, AORIG
#endif
	ALIGN_4

.L70:
	movq	M,  I
	sarq	$2, I	# i = (m >> 2)
	jle	.L79
	ALIGN_4

.L51:
#ifdef LN
       movq	K, %rax
       salq	$2 + BASE_SHIFT, %rax
       subq	%rax, AORIG
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	movq	AORIG, AO
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
#endif

	leaq	16 * SIZE + BUFFER, BO

#if defined(LN) || defined(RT)
	movq	KK, %rax
	salq	$1 + BASE_SHIFT, %rax
	leaq	(BO, %rax, 2), BO
#endif

	pxor	%xmm8, %xmm8
	pxor	%xmm9, %xmm9

#ifdef LN
	prefetcht2    -3 * SIZE(CO1)
	pxor	%xmm12, %xmm12
	prefetcht2    -3 * SIZE(CO2)
	pxor	%xmm13, %xmm13
#else
	prefetcht2     3 * SIZE(CO1)
	pxor	%xmm12, %xmm12
	prefetcht2     3 * SIZE(CO2)
	pxor	%xmm13, %xmm13
#endif

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
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)

	movapd	-16 * SIZE(AO), %xmm0
	movapd	-14 * SIZE(AO), %xmm1

	movapd	-16 * SIZE(BO), %xmm2
	movapd	 %xmm2, %xmm3
	movapd	-14 * SIZE(BO), %xmm4
	movapd	 %xmm4, %xmm5

	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm3
	mulpd	%xmm0, %xmm4
	mulpd	%xmm1, %xmm5

	addpd	%xmm2, %xmm8
	addpd	%xmm3, %xmm12
	addpd	%xmm4, %xmm9
	addpd	%xmm5, %xmm13

	movapd	-12 * SIZE(AO), %xmm0
	movapd	-10 * SIZE(AO), %xmm1

	movapd	-12 * SIZE(BO), %xmm2
	movapd	 %xmm2, %xmm3
	movapd	-10 * SIZE(BO), %xmm4
	movapd	 %xmm4, %xmm5

	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm3
	mulpd	%xmm0, %xmm4
	mulpd	%xmm1, %xmm5

	addpd	%xmm2, %xmm8
	addpd	%xmm3, %xmm12
	addpd	%xmm4, %xmm9
	addpd	%xmm5, %xmm13

	movapd	 -8 * SIZE(AO), %xmm0
	movapd	 -6 * SIZE(AO), %xmm1

	movapd	 -8 * SIZE(BO), %xmm2
	movapd	 %xmm2, %xmm3
	movapd	 -6 * SIZE(BO), %xmm4
	movapd	 %xmm4, %xmm5

	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm3
	mulpd	%xmm0, %xmm4
	mulpd	%xmm1, %xmm5

	addpd	%xmm2, %xmm8
	addpd	%xmm3, %xmm12
	addpd	%xmm4, %xmm9
	addpd	%xmm5, %xmm13

	movapd	 -4 * SIZE(AO), %xmm0
	movapd	 -2 * SIZE(AO), %xmm1

	movapd	 -4 * SIZE(BO), %xmm2
	movapd	 %xmm2, %xmm3
	movapd	 -2 * SIZE(BO), %xmm4
	movapd	 %xmm4, %xmm5

	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm3
	mulpd	%xmm0, %xmm4
	mulpd	%xmm1, %xmm5

	addpd	%xmm2, %xmm8
	addpd	%xmm3, %xmm12
	addpd	%xmm4, %xmm9
	addpd	%xmm5, %xmm13

	subq	$-16 * SIZE, AO
	subq	$-16 * SIZE, BO
	subq	$1, %rax
	jne    .L52
	ALIGN_4

.L55:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	BRANCH
	je .L59
	ALIGN_4

.L56:
	movapd	-16 * SIZE(AO), %xmm0
	movapd	-14 * SIZE(AO), %xmm1

	movapd	-16 * SIZE(BO), %xmm2
	movapd	 %xmm2, %xmm3
	movapd	-14 * SIZE(BO), %xmm4
	movapd	 %xmm4, %xmm5

	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm3
	mulpd	%xmm0, %xmm4
	mulpd	%xmm1, %xmm5

	addpd	%xmm2, %xmm8
	addpd	%xmm3, %xmm12
	addpd	%xmm4, %xmm9
	addpd	%xmm5, %xmm13

	addq	$4 * SIZE, AO
	addq	$4 * SIZE, BO
	subq	$1, %rax
	jg	.L56
	ALIGN_4

.L59:
#if defined(LN) || defined(RT)
	movq	KK, %rax
#ifdef LN
	subq	$4, %rax
#else
	subq	$2, %rax
#endif

	movq	AORIG, AO
	movq	BORIG, B
	leaq	16 * SIZE + BUFFER, BO

	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(B,  %rax, 2), B
	leaq	(BO, %rax, 4), BO
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm8, %xmm0
	unpcklpd %xmm9, %xmm8
	unpckhpd %xmm9, %xmm0

	movapd	%xmm12, %xmm4
	unpcklpd %xmm13, %xmm12
	unpckhpd %xmm13, %xmm4

	movapd	 -16 * SIZE(B), %xmm9
	movapd	 -14 * SIZE(B), %xmm13
	movapd	 -12 * SIZE(B), %xmm1
	movapd	 -10 * SIZE(B), %xmm5

	subpd	%xmm8,  %xmm9
	subpd	%xmm0,  %xmm13
	subpd	%xmm12,  %xmm1
	subpd	%xmm4, %xmm5
#else
	movapd	 -16 * SIZE(AO), %xmm0
	movapd	 -14 * SIZE(AO), %xmm1
	movapd	 -12 * SIZE(AO), %xmm2
	movapd	 -10 * SIZE(AO), %xmm3

	subpd	%xmm8, %xmm0
	subpd	%xmm12, %xmm1
	subpd	%xmm9, %xmm2
	subpd	%xmm13, %xmm3
#endif

#ifdef LN
	movddup	 -1 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm5
	movddup	 -2 * SIZE(AO), %xmm10
	mulpd	 %xmm5, %xmm10
	subpd	 %xmm10, %xmm1
	movddup	 -3 * SIZE(AO), %xmm12
	mulpd	 %xmm5, %xmm12
	subpd	 %xmm12, %xmm13
	movddup	 -4 * SIZE(AO), %xmm14
	mulpd	 %xmm5, %xmm14
	subpd	 %xmm14, %xmm9

	movddup	 -6 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm1
	movddup	 -7 * SIZE(AO), %xmm10
	mulpd	 %xmm1, %xmm10
	subpd	 %xmm10, %xmm13
	movddup	 -8 * SIZE(AO), %xmm12
	mulpd	 %xmm1, %xmm12
	subpd	 %xmm12, %xmm9

	movddup	-11 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm13
	movddup	-12 * SIZE(AO), %xmm10
	mulpd	 %xmm13, %xmm10
	subpd	 %xmm10, %xmm9

	movddup	-16 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm9
#endif

#ifdef LT
	movddup	-16 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm9
	movddup	-15 * SIZE(AO), %xmm10
	mulpd	 %xmm9, %xmm10
	subpd	 %xmm10, %xmm13
	movddup	-14 * SIZE(AO), %xmm12
	mulpd	 %xmm9, %xmm12
	subpd	 %xmm12, %xmm1
	movddup	-13 * SIZE(AO), %xmm14
	mulpd	 %xmm9, %xmm14
	subpd	 %xmm14, %xmm5


	movddup	-11 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm13

	movddup	-10 * SIZE(AO), %xmm10
	mulpd	 %xmm13, %xmm10
	subpd	 %xmm10, %xmm1
	movddup	 -9 * SIZE(AO), %xmm12
	mulpd	 %xmm13, %xmm12
	subpd	 %xmm12, %xmm5

	movddup	 -6 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm1
	movddup	 -5 * SIZE(AO), %xmm10
	mulpd	 %xmm1, %xmm10
	subpd	 %xmm10, %xmm5

	movddup	 -1 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm5
#endif

#ifdef RN
	movddup	-16 * SIZE(B), %xmm8
	mulpd	 %xmm8, %xmm0
	mulpd	 %xmm8, %xmm1

	movddup	-15 * SIZE(B), %xmm9
	movapd	%xmm9, %xmm10
	mulpd	 %xmm0, %xmm9
	subpd	 %xmm9, %xmm2
	mulpd	 %xmm1, %xmm10
	subpd	 %xmm10, %xmm3

	movddup	-13 * SIZE(B), %xmm8
	mulpd	 %xmm8, %xmm2
	mulpd	 %xmm8, %xmm3
#endif

#ifdef RT
	movddup	-13 * SIZE(B), %xmm8
	mulpd	 %xmm8, %xmm2
	mulpd	 %xmm8, %xmm3

	movddup	-14 * SIZE(B), %xmm9
	movapd	%xmm9, %xmm10
	mulpd	 %xmm2, %xmm9
	subpd	 %xmm9, %xmm0
	mulpd	 %xmm3, %xmm10
	subpd	 %xmm10, %xmm1

	movddup	-16 * SIZE(B), %xmm8
	mulpd	 %xmm8, %xmm0
	mulpd	 %xmm8, %xmm1
#endif

#ifdef LN
	subq	$4 * SIZE, CO1
	subq	$4 * SIZE, CO2
#endif

#if defined(LN) || defined(LT)
	movsd	%xmm9,  0 * SIZE(CO1)
	movsd	%xmm13,  1 * SIZE(CO1)
	movsd	%xmm1,  2 * SIZE(CO1)
	movsd	%xmm5, 3 * SIZE(CO1)

	movhpd	%xmm9,  0 * SIZE(CO2)
	movhpd	%xmm13,  1 * SIZE(CO2)
	movhpd	%xmm1,  2 * SIZE(CO2)
	movhpd	%xmm5, 3 * SIZE(CO2)
#else
	movsd	%xmm0,  0 * SIZE(CO1)
	movhpd	%xmm0,  1 * SIZE(CO1)
	movsd	%xmm1,  2 * SIZE(CO1)
	movhpd	%xmm1,  3 * SIZE(CO1)

	movsd	%xmm2,  0 * SIZE(CO2)
	movhpd	%xmm2,  1 * SIZE(CO2)
	movsd	%xmm3,  2 * SIZE(CO2)
	movhpd	%xmm3,  3 * SIZE(CO2)
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm9,  -16 * SIZE(B)
	movapd	%xmm13,  -14 * SIZE(B)
	movapd	%xmm1,  -12 * SIZE(B)
	movapd	%xmm5, -10 * SIZE(B)

	movddup	%xmm9,  %xmm8
	SHUFPD_3 %xmm9,  %xmm9
	movddup	%xmm13,  %xmm12
	SHUFPD_3 %xmm13,  %xmm13
	movddup	%xmm1,  %xmm0
	SHUFPD_3 %xmm1,  %xmm1
	movddup	%xmm5, %xmm4
	SHUFPD_3 %xmm5, %xmm5

	movapd	%xmm8,  -16 * SIZE(BO)
	movapd	%xmm9,  -14 * SIZE(BO)
	movapd	%xmm12,  -12 * SIZE(BO)
	movapd	%xmm13,  -10 * SIZE(BO)
	movapd	%xmm0,   -8 * SIZE(BO)
	movapd	%xmm1,   -6 * SIZE(BO)
	movapd	%xmm4,  -4 * SIZE(BO)
	movapd	%xmm5,  -2 * SIZE(BO)
#else
	movapd	%xmm0,  -16 * SIZE(AO)
	movapd	%xmm1,  -14 * SIZE(AO)
	movapd	%xmm2, -12 * SIZE(AO)
	movapd	%xmm3, -10 * SIZE(AO)
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
#ifdef LT
	addq	$8 * SIZE, B
#endif
#endif

#ifdef LN
	subq	$4, KK
	movq	BORIG, B
#endif

#ifdef LT
	addq	$4, KK
#endif

#ifdef RT
       movq	K, %rax
       movq	BORIG, B
       salq	$2 + BASE_SHIFT, %rax
       addq	%rax, AORIG
#endif

	decq	I			# i --
	jg	.L51
	ALIGN_4

.L79:
#ifdef LN
       leaq	(, K, SIZE), %rax
       leaq	(B, %rax, 2), B
#endif

#if defined(LT) || defined(RN)
	movq	K,  %rax
	subq	KK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(B,  %rax, 2), B
#endif

#ifdef RN
	addq	$2, KK
#endif

#ifdef RT
	subq	$2, KK
#endif
	ALIGN_4

.L80:
	testq	$1, N
	je	.L999
	ALIGN_4

.L81:
/* Copying to Sub Buffer */

#ifdef LN
	movq	OFFSET, %rax
	addq	M, %rax
	movq	%rax, KK
#endif

	leaq	BUFFER, BO

#ifdef RT
       movq	K, %rax
       salq	$0 + BASE_SHIFT, %rax
       subq	%rax, B
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	movq	B, BORIG
	leaq	(, %rax, SIZE), %rax
	leaq	(B,  %rax, 1), B
	leaq	(BO, %rax, 2), BO
#endif

#ifdef LT
	movq	OFFSET, %rax
	movq	%rax, KK
#endif

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	sarq	$3, %rax
	jle	.L83
	ALIGN_4

.L82:
	movddup	 -16 * SIZE(B), %xmm0
	movddup	 -15 * SIZE(B), %xmm1
	movddup	 -14 * SIZE(B), %xmm2
	movddup	 -13 * SIZE(B), %xmm3
	movddup	 -12 * SIZE(B), %xmm4
	movddup	 -11 * SIZE(B), %xmm5
	movddup	 -10 * SIZE(B), %xmm6
	movddup	  -9 * SIZE(B), %xmm7

	movapd	%xmm0,  0 * SIZE(BO)
	movapd	%xmm1,  2 * SIZE(BO)
	movapd	%xmm2,  4 * SIZE(BO)
	movapd	%xmm3,  6 * SIZE(BO)
	movapd	%xmm4,  8 * SIZE(BO)
	movapd	%xmm5, 10 * SIZE(BO)
	movapd	%xmm6, 12 * SIZE(BO)
	movapd	%xmm7, 14 * SIZE(BO)

	addq	$  8 * SIZE, B
	subq	$-16 * SIZE, BO
	subq	$1, %rax
	jne	.L82
	ALIGN_4

.L83:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$7, %rax
	BRANCH
	jle	.L90
	ALIGN_4

.L84:
	movddup	 -16 * SIZE(B), %xmm0

	movapd	%xmm0,  0 * SIZE(BO)

	addq	$1 * SIZE, B
	addq	$2 * SIZE, BO
	subq	$1, %rax
	jne	.L84
	ALIGN_4

.L90:
#if defined(LT) || defined(RN)
	movq	A, AO
#else
	movq	A, AORIG
#endif

#ifdef RT
	subq	LDC, C
#endif

	movq	C, CO1			# coffset1 = c
#ifndef RT
	addq	LDC, C
#endif

	testq	$1, M
	je	.L100
	ALIGN_4

.L111:
#ifdef LN
       movq	K, %rax
       salq	$0 + BASE_SHIFT, %rax
       subq	%rax, AORIG
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	movq	AORIG, AO
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 1), AO
#endif

	leaq	16 * SIZE + BUFFER, BO

#if defined(LN) || defined(RT)
	movq	KK, %rax
	salq	$0 + BASE_SHIFT, %rax
	leaq	(BO, %rax, 2), BO
#endif

	pxor	%xmm8, %xmm8
	pxor	%xmm9, %xmm9
	pxor	%xmm10, %xmm10
	pxor	%xmm11, %xmm11

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	sarq	$2, %rax
	je	.L115
	ALIGN_4

.L112:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)

	movsd	-16 * SIZE(AO), %xmm0
	movsd	-15 * SIZE(AO), %xmm1
	movsd	-16 * SIZE(BO), %xmm2
	movsd	-14 * SIZE(BO), %xmm3

	mulsd	%xmm0, %xmm2
	mulsd	%xmm1, %xmm3
	addsd	%xmm2, %xmm8
	addsd	%xmm3, %xmm9

	movsd	-14 * SIZE(AO), %xmm0
	movsd	-13 * SIZE(AO), %xmm1
	movsd	-12 * SIZE(BO), %xmm2
	movsd	-10 * SIZE(BO), %xmm3

	mulsd	%xmm0, %xmm2
	mulsd	%xmm1, %xmm3
	addsd	%xmm2, %xmm10
	addsd	%xmm3, %xmm11

	subq	$-4 * SIZE, AO
	subq	$-8 * SIZE, BO
	subq	$1, %rax
	jne    .L112
	ALIGN_4

.L115:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	BRANCH
	je .L118
	ALIGN_4

.L116:
	movsd	-16 * SIZE(AO), %xmm0
	movsd	-16 * SIZE(BO), %xmm2

	mulsd	%xmm0, %xmm2
	addsd	%xmm2, %xmm8

	addq	$1 * SIZE, AO
	addq	$2 * SIZE, BO
	subq	$1, %rax
	jg	.L116
	ALIGN_4

.L118:
	addsd	%xmm10, %xmm8
	addsd	%xmm11, %xmm9
	addsd	%xmm9, %xmm8

#if defined(LN) || defined(RT)
	movq	KK, %rax
#ifdef LN
	subq	$1, %rax
#else
	subq	$1, %rax
#endif

	movq	AORIG, AO
	movq	BORIG, B
	leaq	16 * SIZE + BUFFER, BO

	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(B,  %rax, 1), B
	leaq	(BO, %rax, 2), BO
#endif

#if defined(LN) || defined(LT)
	movsd	-16 * SIZE(B), %xmm10
	subsd	%xmm8,  %xmm10
#else
	movsd	-16 * SIZE(AO), %xmm10
	subsd	%xmm8, %xmm10
#endif

#ifdef LN
	movsd	-16 * SIZE(AO), %xmm12
	mulsd	 %xmm12, %xmm10
#endif

#ifdef LT
	movsd	-16 * SIZE(AO), %xmm12
	mulsd	 %xmm12, %xmm10
#endif

#ifdef RN
	movsd	-16 * SIZE(B), %xmm8
	mulsd	 %xmm8, %xmm10
#endif

#ifdef RT
	movsd	-16 * SIZE(B), %xmm8
	mulsd	 %xmm8, %xmm10
#endif

#ifdef LN
	subq	$1 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	movsd	%xmm10,  0 * SIZE(CO1)
#else
	movsd	%xmm10,  0 * SIZE(CO1)
#endif

#if defined(LN) || defined(LT)
	movsd	%xmm10,   -16 * SIZE(B)

	movlpd	%xmm10,   -16 * SIZE(BO)
	movlpd	%xmm10,   -15 * SIZE(BO)
#else
	movsd	%xmm10,   -16 * SIZE(AO)
#endif

#ifndef LN
	addq	$1 * SIZE, CO1
#endif

#if defined(LT) || defined(RN)
	movq	K,  %rax
	subq	KK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 1), AO
#ifdef LT
	addq	$1 * SIZE, B
#endif
#endif

#ifdef LN
	subq	$1, KK
	movq	BORIG, B
#endif

#ifdef LT
	addq	$1, KK
#endif

#ifdef RT
       movq	K, %rax
       movq	BORIG, B
       salq	$0 + BASE_SHIFT, %rax
       addq	%rax, AORIG
#endif
	ALIGN_4

.L100:
	testq	$2, M
	je	.L110
	ALIGN_4

.L101:
#ifdef LN
       movq	K, %rax
       salq	$1 + BASE_SHIFT, %rax
       subq	%rax, AORIG
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	movq	AORIG, AO
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
#endif

	leaq	16 * SIZE + BUFFER, BO

#if defined(LN) || defined(RT)
	movq	KK, %rax
	salq	$0 + BASE_SHIFT, %rax
	leaq	(BO, %rax, 2), BO
#endif

	pxor	%xmm8, %xmm8
	pxor	%xmm9, %xmm9
	pxor	%xmm10, %xmm10
	pxor	%xmm11, %xmm11

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	sarq	$2, %rax
	je	.L105
	ALIGN_4

.L102:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)

	movapd	-16 * SIZE(AO), %xmm0
	movapd	-14 * SIZE(AO), %xmm1
	movapd	-16 * SIZE(BO), %xmm2
	movapd	-14 * SIZE(BO), %xmm3

	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm3

	addpd	%xmm2, %xmm8
	addpd	%xmm3, %xmm9

	movapd	-12 * SIZE(AO), %xmm0
	movapd	-10 * SIZE(AO), %xmm1
	movapd	-12 * SIZE(BO), %xmm2
	movapd	-10 * SIZE(BO), %xmm3

	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm3

	addpd	%xmm2, %xmm10
	addpd	%xmm3, %xmm11

	subq	$-8 * SIZE, AO
	subq	$-8 * SIZE, BO
	subq	$1, %rax
	jne    .L102
	ALIGN_4

.L105:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	BRANCH
	je .L109
	ALIGN_4

.L106:
	movapd	-16 * SIZE(AO), %xmm0
	movapd	-16 * SIZE(BO), %xmm2

	mulpd	%xmm0, %xmm2
	addpd	%xmm2, %xmm8

	addq	$2 * SIZE, AO
	addq	$2 * SIZE, BO
	subq	$1, %rax
	jg	.L106
	ALIGN_4

.L109:
	addpd	%xmm9, %xmm8
	addpd	%xmm11, %xmm10
	addpd	%xmm10, %xmm8

#if defined(LN) || defined(RT)
	movq	KK, %rax
#ifdef LN
	subq	$2, %rax
#else
	subq	$1, %rax
#endif

	movq	AORIG, AO
	movq	BORIG, B
	leaq	16 * SIZE + BUFFER, BO

	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(B,  %rax, 1), B
	leaq	(BO, %rax, 2), BO
#endif

#if defined(LN) || defined(LT)
	movapd	 -16 * SIZE(B), %xmm10
	subpd	%xmm8,  %xmm10
#else
	movapd	 -16 * SIZE(AO), %xmm10
	subpd	%xmm8, %xmm10
#endif

#ifdef LN
	movapd	%xmm10, %xmm8
        unpckhpd %xmm8, %xmm8

	movsd	-13 * SIZE(AO), %xmm12
	mulsd	 %xmm12, %xmm8

	movsd	-14 * SIZE(AO), %xmm13
	mulsd	 %xmm8, %xmm13
	subsd	 %xmm13, %xmm10

	movsd	-16 * SIZE(AO), %xmm12
	mulsd	 %xmm12, %xmm10

	unpcklpd %xmm8, %xmm10
#endif

#ifdef LT
	movapd	%xmm10, %xmm8
        unpckhpd %xmm8, %xmm8

	movsd	-16 * SIZE(AO), %xmm12
	mulsd	 %xmm12, %xmm10

	movsd	-15 * SIZE(AO), %xmm13
	mulsd	 %xmm10, %xmm13
	subsd	 %xmm13, %xmm8

	movsd	-13 * SIZE(AO), %xmm12
	mulsd	 %xmm12, %xmm8

	unpcklpd %xmm8, %xmm10
#endif

#ifdef RN
	movddup	-16 * SIZE(B), %xmm8
	mulpd	 %xmm8, %xmm10
#endif

#ifdef RT
	movddup -16 * SIZE(B), %xmm8
	mulpd	 %xmm8, %xmm10
#endif

#ifdef LN
	subq	$2 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	movsd	%xmm10,  0 * SIZE(CO1)
	movhpd	%xmm10,  1 * SIZE(CO1)
#else
	movsd	%xmm10,  0 * SIZE(CO1)
	movhpd	%xmm10,  1 * SIZE(CO1)
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm10,   -16 * SIZE(B)

	movddup	%xmm10,  %xmm8
	SHUFPD_3 %xmm10,  %xmm10

	movapd	%xmm8,  -16 * SIZE(BO)
	movapd	%xmm10,  -14 * SIZE(BO)
#else
	movapd	%xmm10,  -16 * SIZE(AO)
#endif

#ifndef LN
	addq	$2 * SIZE, CO1
#endif

#if defined(LT) || defined(RN)
	movq	K,  %rax
	subq	KK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
#ifdef LT
	addq	$2 * SIZE, B
#endif
#endif

#ifdef LN
	subq	$2, KK
	movq	BORIG, B
#endif

#ifdef LT
	addq	$2, KK
#endif

#ifdef RT
       movq	K, %rax
       movq	BORIG, B
       salq	$1 + BASE_SHIFT, %rax
       addq	%rax, AORIG
#endif
	ALIGN_4

.L110:
	movq	M,  I
	sarq	$2, I	# i = (m >> 2)
	jle	.L119
	ALIGN_4

.L91:
#ifdef LN
       movq	K, %rax
       salq	$2 + BASE_SHIFT, %rax
       subq	%rax, AORIG
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	movq	AORIG, AO
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
#endif

	leaq	16 * SIZE + BUFFER, BO

#if defined(LN) || defined(RT)
	movq	KK, %rax
	salq	$0 + BASE_SHIFT, %rax
	leaq	(BO, %rax, 2), BO
#endif

	pxor	%xmm8, %xmm8
#ifdef LN
	prefetcht2    -3 * SIZE(CO1)
#else
	prefetcht2     3 * SIZE(CO1)
#endif
	pxor	%xmm9, %xmm9
	pxor	%xmm10, %xmm10
	pxor	%xmm11, %xmm11

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	sarq	$2, %rax
	je	.L95
	ALIGN_4

.L92:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)

	movapd	-16 * SIZE(AO), %xmm0
	movapd	-14 * SIZE(AO), %xmm1
	movapd	-16 * SIZE(BO), %xmm2
	movapd	 %xmm2, %xmm3

	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm3
	addpd	%xmm2, %xmm8
	addpd	%xmm3, %xmm9

	movapd	-12 * SIZE(AO), %xmm0
	movapd	-10 * SIZE(AO), %xmm1
	movapd	-14 * SIZE(BO), %xmm2
	movapd	 %xmm2, %xmm3

	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm3
	addpd	%xmm2, %xmm10
	addpd	%xmm3, %xmm11

	movapd	 -8 * SIZE(AO), %xmm0
	movapd	 -6 * SIZE(AO), %xmm1
	movapd	-12 * SIZE(BO), %xmm2
	movapd	 %xmm2, %xmm3

	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm3
	addpd	%xmm2, %xmm8
	addpd	%xmm3, %xmm9

	movapd	 -4 * SIZE(AO), %xmm0
	movapd	 -2 * SIZE(AO), %xmm1
	movapd	-10 * SIZE(BO), %xmm2
	movapd	 %xmm2, %xmm3

	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm3
	addpd	%xmm2, %xmm10
	addpd	%xmm3, %xmm11

	subq	$-16 * SIZE, AO
	subq	$ -8 * SIZE, BO
	subq	$1, %rax
	jne    .L92
	ALIGN_4

.L95:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	BRANCH
	je .L99
	ALIGN_4

.L96:
	movapd	-16 * SIZE(AO), %xmm0
	movapd	-14 * SIZE(AO), %xmm1

	movapd	-16 * SIZE(BO), %xmm2
	movapd	 %xmm2, %xmm3

	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm3
	addpd	%xmm2, %xmm8
	addpd	%xmm3, %xmm9

	addq	$4 * SIZE, AO
	addq	$2 * SIZE, BO
	subq	$1, %rax
	jg	.L96
	ALIGN_4

.L99:
	addpd	%xmm10, %xmm8
	addpd	%xmm11, %xmm9

#if defined(LN) || defined(RT)
	movq	KK, %rax
#ifdef LN
	subq	$4, %rax
#else
	subq	$1, %rax
#endif

	movq	AORIG, AO
	movq	BORIG, B
	leaq	16 * SIZE + BUFFER, BO

	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(B,  %rax, 1), B
	leaq	(BO, %rax, 2), BO
#endif

#if defined(LN) || defined(LT)
	movapd	 -16 * SIZE(B), %xmm10
	movapd	 -14 * SIZE(B), %xmm11

	subpd	%xmm8,  %xmm10
	subpd	%xmm9,  %xmm11
#else
	movapd	 -16 * SIZE(AO), %xmm10
	movapd	 -14 * SIZE(AO), %xmm11

	subpd	%xmm8, %xmm10
	subpd	%xmm9, %xmm11
#endif

#ifdef LN
	movapd	%xmm10, %xmm8
        unpckhpd %xmm8, %xmm8

	movapd	%xmm11, %xmm9
        unpckhpd %xmm9, %xmm9

	movsd	 -1 * SIZE(AO), %xmm12
	mulsd	 %xmm12, %xmm9

	movsd	 -2 * SIZE(AO), %xmm13
	mulsd	 %xmm9, %xmm13
	subsd	 %xmm13, %xmm11
	movsd	 -3 * SIZE(AO), %xmm14
	mulsd	 %xmm9, %xmm14
	subsd	 %xmm14, %xmm8
	movsd	 -4 * SIZE(AO), %xmm15
	mulsd	 %xmm9, %xmm15
	subsd	 %xmm15, %xmm10

	movsd	 -6 * SIZE(AO), %xmm12
	mulsd	 %xmm12, %xmm11

	movsd	 -7 * SIZE(AO), %xmm13
	mulsd	 %xmm11, %xmm13
	subsd	 %xmm13, %xmm8
	movsd	 -8 * SIZE(AO), %xmm14
	mulsd	 %xmm11, %xmm14
	subsd	 %xmm14, %xmm10

	movsd	-11 * SIZE(AO), %xmm12
	mulsd	 %xmm12, %xmm8

	movsd	-12 * SIZE(AO), %xmm13
	mulsd	 %xmm8, %xmm13
	subsd	 %xmm13, %xmm10

	movsd	-16 * SIZE(AO), %xmm12
	mulsd	 %xmm12, %xmm10

	unpcklpd %xmm8, %xmm10
	unpcklpd %xmm9, %xmm11
#endif

#ifdef LT
	movapd	%xmm10, %xmm8
        unpckhpd %xmm8, %xmm8

	movapd	%xmm11, %xmm9
        unpckhpd %xmm9, %xmm9

	movsd	-16 * SIZE(AO), %xmm12
	mulsd	 %xmm12, %xmm10

	movsd	-15 * SIZE(AO), %xmm13
	mulsd	 %xmm10, %xmm13
	subsd	 %xmm13, %xmm8
	movsd	-14 * SIZE(AO), %xmm14
	mulsd	 %xmm10, %xmm14
	subsd	 %xmm14, %xmm11
	movsd	-13 * SIZE(AO), %xmm15
	mulsd	 %xmm10, %xmm15
	subsd	 %xmm15, %xmm9

	movsd	-11 * SIZE(AO), %xmm12
	mulsd	 %xmm12, %xmm8

	movsd	-10 * SIZE(AO), %xmm13
	mulsd	 %xmm8, %xmm13
	subsd	 %xmm13, %xmm11
	movsd	 -9 * SIZE(AO), %xmm14
	mulsd	 %xmm8, %xmm14
	subsd	 %xmm14, %xmm9

	movsd	 -6 * SIZE(AO), %xmm12
	mulsd	 %xmm12, %xmm11

	movsd	 -5 * SIZE(AO), %xmm13
	mulsd	 %xmm11, %xmm13
	subsd	 %xmm13, %xmm9

	movsd	 -1 * SIZE(AO), %xmm12
	mulsd	 %xmm12, %xmm9

	unpcklpd %xmm8, %xmm10
	unpcklpd %xmm9, %xmm11
#endif

#ifdef RN
	movddup	 -16 * SIZE(B), %xmm8
	mulpd	 %xmm8, %xmm10
	mulpd	 %xmm8, %xmm11
#endif

#ifdef RT
	movddup	 -16 * SIZE(B), %xmm8
	mulpd	 %xmm8, %xmm10
	mulpd	 %xmm8, %xmm11
#endif

#ifdef LN
	subq	$4 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	movsd	%xmm10,  0 * SIZE(CO1)
	movhpd	%xmm10,  1 * SIZE(CO1)
	movsd	%xmm11,  2 * SIZE(CO1)
	movhpd	%xmm11,  3 * SIZE(CO1)
#else
	movsd	%xmm10,  0 * SIZE(CO1)
	movhpd	%xmm10,  1 * SIZE(CO1)
	movsd	%xmm11,  2 * SIZE(CO1)
	movhpd	%xmm11,  3 * SIZE(CO1)
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm10,  -16 * SIZE(B)
	movapd	%xmm11,  -14 * SIZE(B)

	movddup	%xmm10,  %xmm8
	SHUFPD_3 %xmm10,  %xmm10
	movddup	%xmm11,  %xmm9
	SHUFPD_3 %xmm11,  %xmm11

	movapd	%xmm8,  -16 * SIZE(BO)
	movapd	%xmm10,  -14 * SIZE(BO)
	movapd	%xmm9,  -12 * SIZE(BO)
	movapd	%xmm11,  -10 * SIZE(BO)
#else
	movapd	%xmm10,  -16 * SIZE(AO)
	movapd	%xmm11,  -14 * SIZE(AO)
#endif

#ifndef LN
	addq	$4 * SIZE, CO1
#endif

#if defined(LT) || defined(RN)
	movq	K,  %rax
	subq	KK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
#ifdef LT
	addq	$4 * SIZE, B
#endif
#endif

#ifdef LN
	subq	$4, KK
	movq	BORIG, B
#endif

#ifdef LT
	addq	$4, KK
#endif

#ifdef RT
       movq	K, %rax
       movq	BORIG, B
       salq	$2 + BASE_SHIFT, %rax
       addq	%rax, AORIG
#endif

	decq	I			# i --
	jg	.L91
	ALIGN_4

.L119:
#ifdef LN
       leaq	(, K, SIZE), %rax
       leaq	(B, %rax, 1), B
#endif

#if defined(LT) || defined(RN)
	movq	K,  %rax
	subq	KK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(B,  %rax, 1), B
#endif

#ifdef RN
	addq	$1, KK
#endif

#ifdef RT
	subq	$1, KK
#endif
	ALIGN_4


.L999:
	movq	%r15, %rsp

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
