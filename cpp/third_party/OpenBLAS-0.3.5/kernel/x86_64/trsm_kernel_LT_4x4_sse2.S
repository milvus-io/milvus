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
#define CO2	%rbp

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

#define ALPHA	  0(%rsp)
#define OFFSET	 16(%rsp)
#define KK	 24(%rsp)
#define KKK	 32(%rsp)
#define AORIG	 40(%rsp)
#define BORIG	 48(%rsp)
#define BUFFER	128(%rsp)

#if defined(OPTERON) || defined(BARCELONA) || defined(SHANGHAI)
#define PREFETCH     prefetch
#define PREFETCHW    prefetchw
#define PREFETCHNTA  prefetchnta
#ifndef ALLOC_HUGETLB
#define PREFETCHSIZE (8 * 4 + 4)
#else
#define PREFETCHSIZE (8 * 2 + 4)
#endif
#endif

#ifdef GENERIC
#define PREFETCH     prefetcht0
#define PREFETCHW    prefetcht0
#define PREFETCHNTA  prefetchnta
#define PREFETCHSIZE (8 * 4 + 4)
#endif

#ifdef OPTERON
#define movsd	movlpd
#endif

#define KERNEL1(xx) \
	mulpd	%xmm8, %xmm9 ;\
	addpd	%xmm9, %xmm0 ;\
	movapd	 0 * SIZE + 2 * (xx) * SIZE(BO), %xmm9 ;\
	mulpd	%xmm8, %xmm11 ;\
	PREFETCH	(PREFETCHSIZE     +  0) * SIZE + 1 * (xx) * SIZE(AO) ;\
	addpd	%xmm11, %xmm1 ;\
	movapd	 2 * SIZE + 2 * (xx) * SIZE(BO), %xmm11 ;\
	mulpd	%xmm8, %xmm13 ;\
	mulpd	 6 * SIZE + 2 * (xx) * SIZE(BO), %xmm8 ;\
	addpd	%xmm13, %xmm2 ;\
	movapd	 4 * SIZE + 2 * (xx) * SIZE(BO), %xmm13 ;\
	addpd	%xmm8, %xmm3 ;\
	movapd	 8 * SIZE + 1 * (xx) * SIZE(AO), %xmm8

#define KERNEL2(xx) \
	mulpd	%xmm10, %xmm9 ;\
	addpd	%xmm9, %xmm4 ;\
	movapd	16 * SIZE + 2 * (xx) * SIZE(BO), %xmm9 ;\
	mulpd	%xmm10, %xmm11 ;\
	addpd	%xmm11, %xmm5 ;\
	movapd	10 * SIZE + 2 * (xx) * SIZE(BO), %xmm11 ;\
	mulpd	%xmm10, %xmm13 ;\
	mulpd	 6 * SIZE + 2 * (xx) * SIZE(BO), %xmm10 ;\
	addpd	%xmm13, %xmm6 ;\
	movapd	12 * SIZE + 2 * (xx) * SIZE(BO), %xmm13 ;\
	addpd	%xmm10, %xmm7 ;\
	movapd	10 * SIZE + 1 * (xx) * SIZE(AO), %xmm10

#define KERNEL3(xx) \
	mulpd	%xmm12, %xmm15 ;\
	addpd	%xmm15, %xmm0 ;\
	movapd	 8 * SIZE + 2 * (xx) * SIZE(BO), %xmm15 ;\
	mulpd	%xmm12, %xmm11 ;\
	addpd	%xmm11, %xmm1 ;\
	movapd	10 * SIZE + 2 * (xx) * SIZE(BO), %xmm11 ;\
	mulpd	%xmm12, %xmm13 ;\
	mulpd	14 * SIZE + 2 * (xx) * SIZE(BO), %xmm12 ;\
	addpd	%xmm13, %xmm2 ;\
	movapd	12 * SIZE + 2 * (xx) * SIZE(BO), %xmm13 ;\
	addpd	%xmm12, %xmm3 ;\
	movapd	12 * SIZE + 1 * (xx) * SIZE(AO), %xmm12

#define KERNEL4(xx) \
	mulpd	%xmm14, %xmm15 ;\
	addpd	%xmm15, %xmm4 ;\
	movapd	24 * SIZE + 2 * (xx) * SIZE(BO), %xmm15 ;\
	mulpd	%xmm14, %xmm11 ;\
	addpd	%xmm11, %xmm5 ;\
	movapd	18 * SIZE + 2 * (xx) * SIZE(BO), %xmm11 ;\
	mulpd	%xmm14, %xmm13 ;\
	mulpd	14 * SIZE + 2 * (xx) * SIZE(BO), %xmm14 ;\
	addpd	%xmm13, %xmm6 ;\
	movapd	20 * SIZE + 2 * (xx) * SIZE(BO), %xmm13 ;\
	addpd	%xmm14, %xmm7 ;\
	movapd	14 * SIZE + 1 * (xx) * SIZE(AO), %xmm14

#define KERNEL5(xx) \
	mulpd	%xmm8, %xmm9 ;\
	addpd	%xmm9, %xmm0 ;\
	movapd	16 * SIZE + 2 * (xx) * SIZE(BO), %xmm9 ;\
	mulpd	%xmm8, %xmm11 ;\
	PREFETCH	(PREFETCHSIZE     +  8) * SIZE + 1 * (xx) * SIZE(AO) ;\
	addpd	%xmm11, %xmm1 ;\
	movapd	18 * SIZE + 2 * (xx) * SIZE(BO), %xmm11 ;\
	mulpd	%xmm8, %xmm13 ;\
	mulpd	22 * SIZE + 2 * (xx) * SIZE(BO), %xmm8 ;\
	addpd	%xmm13, %xmm2 ;\
	movapd	20 * SIZE + 2 * (xx) * SIZE(BO), %xmm13 ;\
	addpd	%xmm8, %xmm3 ;\
	movapd	16 * SIZE + 1 * (xx) * SIZE(AO), %xmm8

#define KERNEL6(xx) \
	mulpd	%xmm10, %xmm9 ;\
	addpd	%xmm9, %xmm4 ;\
	movapd	32 * SIZE + 2 * (xx) * SIZE(BO), %xmm9 ;\
	mulpd	%xmm10, %xmm11 ;\
	addpd	%xmm11, %xmm5 ;\
	movapd	26 * SIZE + 2 * (xx) * SIZE(BO), %xmm11 ;\
	mulpd	%xmm10, %xmm13 ;\
	mulpd	22 * SIZE + 2 * (xx) * SIZE(BO), %xmm10 ;\
	addpd	%xmm13, %xmm6 ;\
	movapd	28 * SIZE + 2 * (xx) * SIZE(BO), %xmm13 ;\
	addpd	%xmm10, %xmm7 ;\
	movapd	18 * SIZE + 1 * (xx) * SIZE(AO), %xmm10

#define KERNEL7(xx) \
	mulpd	%xmm12, %xmm15 ;\
	addpd	%xmm15, %xmm0 ;\
	movapd	24 * SIZE + 2 * (xx) * SIZE(BO), %xmm15 ;\
	mulpd	%xmm12, %xmm11 ;\
	addpd	%xmm11, %xmm1 ;\
	movapd	26 * SIZE + 2 * (xx) * SIZE(BO), %xmm11 ;\
	mulpd	%xmm12, %xmm13 ;\
	mulpd	30 * SIZE + 2 * (xx) * SIZE(BO), %xmm12 ;\
	addpd	%xmm13, %xmm2 ;\
	movapd	28 * SIZE + 2 * (xx) * SIZE(BO), %xmm13 ;\
	addpd	%xmm12, %xmm3 ;\
	movapd	20 * SIZE + 1 * (xx) * SIZE(AO), %xmm12

#define KERNEL8(xx) \
	mulpd	%xmm14, %xmm15 ;\
	addpd	%xmm15, %xmm4 ;\
	movapd	40 * SIZE + 2 * (xx) * SIZE(BO), %xmm15 ;\
	mulpd	%xmm14, %xmm11 ;\
	addpd	%xmm11, %xmm5 ;\
	movapd	34 * SIZE + 2 * (xx) * SIZE(BO), %xmm11 ;\
	mulpd	%xmm14, %xmm13 ;\
	mulpd	30 * SIZE + 2 * (xx) * SIZE(BO), %xmm14 ;\
	addpd	%xmm13, %xmm6 ;\
	movapd	36 * SIZE + 2 * (xx) * SIZE(BO), %xmm13 ;\
	addpd	%xmm14, %xmm7 ;\
	movapd	22 * SIZE + 1 * (xx) * SIZE(AO), %xmm14

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
	movsd	OLD_OFFSET, %xmm4

	movaps	%xmm3, %xmm0

#else
	movq	OLD_LDC,   LDC
	movsd	OLD_OFFSET, %xmm4

#endif

	movq	%rsp, %rbx	# save old stack
	subq	$128 + LOCAL_BUFFER_SIZE, %rsp
	andq	$-4096, %rsp	# align stack

	STACK_TOUCHING

	movsd	%xmm4, OFFSET
	movsd	%xmm4, KK

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

	leaq	BUFFER, BO

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

	addq	%rax, %rax
	ALIGN_4

.L02:
	PREFETCHNTA	 40 * SIZE(B)

	movsd	 0 * SIZE(B), %xmm0
	movsd	 1 * SIZE(B), %xmm1
	movsd	 2 * SIZE(B), %xmm2
	movsd	 3 * SIZE(B), %xmm3
	movsd	 4 * SIZE(B), %xmm4
	movsd	 5 * SIZE(B), %xmm5
	movsd	 6 * SIZE(B), %xmm6
	movsd	 7 * SIZE(B), %xmm7

	addq	$16 * SIZE, BO
	addq	$ 8 * SIZE, B

	movsd	%xmm0, -16 * SIZE(BO)
	movsd	%xmm0, -15 * SIZE(BO)
	movsd	%xmm1, -14 * SIZE(BO)
	movsd	%xmm1, -13 * SIZE(BO)
	movsd	%xmm2, -12 * SIZE(BO)
	movsd	%xmm2, -11 * SIZE(BO)
	movsd	%xmm3, -10 * SIZE(BO)
	movsd	%xmm3,  -9 * SIZE(BO)
	movsd	%xmm4,  -8 * SIZE(BO)
	movsd	%xmm4,  -7 * SIZE(BO)
	movsd	%xmm5,  -6 * SIZE(BO)
	movsd	%xmm5,  -5 * SIZE(BO)
	movsd	%xmm6,  -4 * SIZE(BO)
	movsd	%xmm6,  -3 * SIZE(BO)
	movsd	%xmm7,  -2 * SIZE(BO)
	movsd	%xmm7,  -1 * SIZE(BO)

	decq	%rax
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
	movsd	 0 * SIZE(B), %xmm0
	movsd	 1 * SIZE(B), %xmm1
	movsd	 2 * SIZE(B), %xmm2
	movsd	 3 * SIZE(B), %xmm3

	movsd	%xmm0,  0 * SIZE(BO)
	movsd	%xmm0,  1 * SIZE(BO)
	movsd	%xmm1,  2 * SIZE(BO)
	movsd	%xmm1,  3 * SIZE(BO)
	movsd	%xmm2,  4 * SIZE(BO)
	movsd	%xmm2,  5 * SIZE(BO)
	movsd	%xmm3,  6 * SIZE(BO)
	movsd	%xmm3,  7 * SIZE(BO)

	addq	$4 * SIZE, B
	addq	$8 * SIZE, BO
	decq	%rax
	jne	.L04
	ALIGN_4

.L10:
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

	movq	M,  I
	sarq	$2, I	# i = (m >> 2)
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
	movq	AORIG, AO
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
#endif

	leaq	BUFFER, BO

#if defined(LN) || defined(RT)
	movq	KK, %rax
	salq	$2 + BASE_SHIFT, %rax
	leaq	(BO, %rax, 2), BO
#endif

	movapd	 0 * SIZE(BO), %xmm9
	movapd	 2 * SIZE(BO), %xmm11
	movapd	 4 * SIZE(BO), %xmm13
	movapd	 8 * SIZE(BO), %xmm15

	movapd	 0 * SIZE(AO), %xmm8
	pxor	%xmm0, %xmm0
	movapd	 2 * SIZE(AO), %xmm10
	pxor	%xmm1, %xmm1
	movapd	 4 * SIZE(AO), %xmm12
	pxor	%xmm2, %xmm2
	movapd	 6 * SIZE(AO), %xmm14
	pxor	%xmm3, %xmm3

	PREFETCHW      4 * SIZE(CO1)
	pxor	%xmm4, %xmm4
	PREFETCHW      4 * SIZE(CO2)
	pxor	%xmm5, %xmm5
	PREFETCHW      4 * SIZE(CO1, LDC, 2)
	pxor	%xmm6, %xmm6
	PREFETCHW      4 * SIZE(CO2, LDC, 2)
	pxor	%xmm7, %xmm7

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif

#if 1
	andq	$-8, %rax
	salq	$4, %rax
	je	.L15
.L1X:
	KERNEL1(16 *  0)
	KERNEL2(16 *  0)
	KERNEL3(16 *  0)
	KERNEL4(16 *  0)
	KERNEL5(16 *  0)
	KERNEL6(16 *  0)
	KERNEL7(16 *  0)
	KERNEL8(16 *  0)
	KERNEL1(16 *  1)
	KERNEL2(16 *  1)
	KERNEL3(16 *  1)
	KERNEL4(16 *  1)
	KERNEL5(16 *  1)
	KERNEL6(16 *  1)
	KERNEL7(16 *  1)
	KERNEL8(16 *  1)
	cmpq	$64 *  2, %rax
	jle	.L12
	KERNEL1(16 *  2)
	KERNEL2(16 *  2)
	KERNEL3(16 *  2)
	KERNEL4(16 *  2)
	KERNEL5(16 *  2)
	KERNEL6(16 *  2)
	KERNEL7(16 *  2)
	KERNEL8(16 *  2)
	KERNEL1(16 *  3)
	KERNEL2(16 *  3)
	KERNEL3(16 *  3)
	KERNEL4(16 *  3)
	KERNEL5(16 *  3)
	KERNEL6(16 *  3)
	KERNEL7(16 *  3)
	KERNEL8(16 *  3)
	cmpq	$64 *  4, %rax
	jle	.L12
	KERNEL1(16 *  4)
	KERNEL2(16 *  4)
	KERNEL3(16 *  4)
	KERNEL4(16 *  4)
	KERNEL5(16 *  4)
	KERNEL6(16 *  4)
	KERNEL7(16 *  4)
	KERNEL8(16 *  4)
	KERNEL1(16 *  5)
	KERNEL2(16 *  5)
	KERNEL3(16 *  5)
	KERNEL4(16 *  5)
	KERNEL5(16 *  5)
	KERNEL6(16 *  5)
	KERNEL7(16 *  5)
	KERNEL8(16 *  5)
	cmpq	$64 *  6, %rax
	jle	.L12
	KERNEL1(16 *  6)
	KERNEL2(16 *  6)
	KERNEL3(16 *  6)
	KERNEL4(16 *  6)
	KERNEL5(16 *  6)
	KERNEL6(16 *  6)
	KERNEL7(16 *  6)
	KERNEL8(16 *  6)
	KERNEL1(16 *  7)
	KERNEL2(16 *  7)
	KERNEL3(16 *  7)
	KERNEL4(16 *  7)
	KERNEL5(16 *  7)
	KERNEL6(16 *  7)
	KERNEL7(16 *  7)
	KERNEL8(16 *  7)

	addq	$16 * 8  * SIZE, AO
	addq	$32 * 8  * SIZE, BO
	subq	$64 * 8, %rax
	jg	.L1X

.L12:
	leaq	(AO, %rax, 2), AO	# * 16
	leaq	(BO, %rax, 4), BO	# * 64
	ALIGN_4

#else
	sarq	$3, %rax
	je	.L15

.L12:
	KERNEL1(16 *  0)
	KERNEL2(16 *  0)
	KERNEL3(16 *  0)
	KERNEL4(16 *  0)
	KERNEL5(16 *  0)
	KERNEL6(16 *  0)
	KERNEL7(16 *  0)
	KERNEL8(16 *  0)
	KERNEL1(16 *  1)
	KERNEL2(16 *  1)
	KERNEL3(16 *  1)
	KERNEL4(16 *  1)
	KERNEL5(16 *  1)
	KERNEL6(16 *  1)
	KERNEL7(16 *  1)
	KERNEL8(16 *  1)

	addq	$32 * SIZE, AO
	addq	$64 * SIZE, BO
	decq	%rax
	jg	.L12
	ALIGN_4
#endif


.L15:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L19
	ALIGN_4

.L16:
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm0
	movapd	 2 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm1
	movapd	 4 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	mulpd	 6 * SIZE(BO), %xmm8
	addpd	%xmm9, %xmm2
	movapd	 0 * SIZE(BO), %xmm9
	addpd	%xmm8, %xmm3
	movapd	 4 * SIZE(AO), %xmm8
	mulpd	%xmm10, %xmm9
	addpd	%xmm9, %xmm4
	movapd	 2 * SIZE(BO), %xmm9
	mulpd	%xmm10, %xmm9
	addpd	%xmm9, %xmm5
	movapd	 4 * SIZE(BO), %xmm9
	mulpd	%xmm10, %xmm9
	mulpd	 6 * SIZE(BO), %xmm10
	addpd	%xmm9, %xmm6
	movapd	 8 * SIZE(BO), %xmm9
	addpd	%xmm10, %xmm7
	movapd	 6 * SIZE(AO), %xmm10

	addq	$4 * SIZE, AO		# aoffset  += 4
	addq	$8 * SIZE, BO		# boffset1 += 8
	decq	%rax
	jg	.L16
	ALIGN_4

.L19:
#if defined(LN) || defined(RT)
	movq	KK, %rax
#ifdef LN
	subq	$4, %rax
#else
	subq	$4, %rax
#endif

	movq	AORIG, AO
	movq	BORIG, B
	leaq	BUFFER, BO

	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(B,  %rax, 4), B
	leaq	(BO, %rax, 8), BO
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm0, %xmm8
	unpcklpd %xmm1, %xmm0
	unpckhpd %xmm1, %xmm8

	movapd	%xmm2, %xmm10
	unpcklpd %xmm3, %xmm2
	unpckhpd %xmm3, %xmm10

	movapd	%xmm4, %xmm12
	unpcklpd %xmm5, %xmm4
	unpckhpd %xmm5, %xmm12

	movapd	%xmm6, %xmm14
	unpcklpd %xmm7, %xmm6
	unpckhpd %xmm7, %xmm14

	movapd	 0 * SIZE(B), %xmm1
	movapd	 2 * SIZE(B), %xmm3
	movapd	 4 * SIZE(B), %xmm5
	movapd	 6 * SIZE(B), %xmm7
	movapd	 8 * SIZE(B), %xmm9
	movapd	10 * SIZE(B), %xmm11
	movapd	12 * SIZE(B), %xmm13
	movapd	14 * SIZE(B), %xmm15

	subpd	%xmm0,  %xmm1
	subpd	%xmm2,  %xmm3
	subpd	%xmm8,  %xmm5
	subpd	%xmm10, %xmm7
	subpd	%xmm4,  %xmm9
	subpd	%xmm6,  %xmm11
	subpd	%xmm12, %xmm13
	subpd	%xmm14, %xmm15
#else
	movapd	 0 * SIZE(AO), %xmm8
	movapd	 2 * SIZE(AO), %xmm9
	movapd	 4 * SIZE(AO), %xmm10
	movapd	 6 * SIZE(AO), %xmm11

	movapd	 8 * SIZE(AO), %xmm12
	movapd	10 * SIZE(AO), %xmm13
	movapd	12 * SIZE(AO), %xmm14
	movapd	14 * SIZE(AO), %xmm15

	subpd	%xmm0, %xmm8
	subpd	%xmm4, %xmm9
	subpd	%xmm1, %xmm10
	subpd	%xmm5, %xmm11
	subpd	%xmm2, %xmm12
	subpd	%xmm6, %xmm13
	subpd	%xmm3, %xmm14
	subpd	%xmm7, %xmm15
#endif

#ifdef LN
	movlpd	15 * SIZE(AO), %xmm0
	movhpd	15 * SIZE(AO), %xmm0
	mulpd	 %xmm0, %xmm13
	mulpd	 %xmm0, %xmm15

	movlpd	14 * SIZE(AO), %xmm2
	movhpd	14 * SIZE(AO), %xmm2
	mulpd	 %xmm13, %xmm2
	subpd	 %xmm2, %xmm9
	movlpd	14 * SIZE(AO), %xmm2
	movhpd	14 * SIZE(AO), %xmm2
	mulpd	 %xmm15, %xmm2
	subpd	 %xmm2, %xmm11

	movlpd	13 * SIZE(AO), %xmm4
	movhpd	13 * SIZE(AO), %xmm4
	mulpd	 %xmm13, %xmm4
	subpd	 %xmm4, %xmm5
	movlpd	13 * SIZE(AO), %xmm4
	movhpd	13 * SIZE(AO), %xmm4
	mulpd	 %xmm15, %xmm4
	subpd	 %xmm4, %xmm7

	movlpd	12 * SIZE(AO), %xmm6
	movhpd	12 * SIZE(AO), %xmm6
	mulpd	 %xmm13, %xmm6
	subpd	 %xmm6, %xmm1
	movlpd	12 * SIZE(AO), %xmm6
	movhpd	12 * SIZE(AO), %xmm6
	mulpd	 %xmm15, %xmm6
	subpd	 %xmm6, %xmm3

	movlpd	10 * SIZE(AO), %xmm0
	movhpd	10 * SIZE(AO), %xmm0
	mulpd	 %xmm0, %xmm9
	mulpd	 %xmm0, %xmm11

	movlpd	 9 * SIZE(AO), %xmm2
	movhpd	 9 * SIZE(AO), %xmm2
	mulpd	 %xmm9, %xmm2
	subpd	 %xmm2, %xmm5
	movlpd	 9 * SIZE(AO), %xmm2
	movhpd	 9 * SIZE(AO), %xmm2
	mulpd	 %xmm11, %xmm2
	subpd	 %xmm2, %xmm7

	movlpd	 8 * SIZE(AO), %xmm4
	movhpd	 8 * SIZE(AO), %xmm4
	mulpd	 %xmm9, %xmm4
	subpd	 %xmm4, %xmm1
	movlpd	 8 * SIZE(AO), %xmm4
	movhpd	 8 * SIZE(AO), %xmm4
	mulpd	 %xmm11, %xmm4
	subpd	 %xmm4, %xmm3

	movlpd	 5 * SIZE(AO), %xmm0
	movhpd	 5 * SIZE(AO), %xmm0
	mulpd	 %xmm0, %xmm5
	mulpd	 %xmm0, %xmm7

	movlpd	 4 * SIZE(AO), %xmm2
	movhpd	 4 * SIZE(AO), %xmm2
	mulpd	 %xmm5, %xmm2
	subpd	 %xmm2, %xmm1
	movlpd	 4 * SIZE(AO), %xmm2
	movhpd	 4 * SIZE(AO), %xmm2
	mulpd	 %xmm7, %xmm2
	subpd	 %xmm2, %xmm3

	movlpd	 0 * SIZE(AO), %xmm0
	movhpd	 0 * SIZE(AO), %xmm0
	mulpd	 %xmm0, %xmm1
	mulpd	 %xmm0, %xmm3
#endif

#ifdef LT
	movlpd	 0 * SIZE(AO), %xmm0
	movhpd	 0 * SIZE(AO), %xmm0
	mulpd	 %xmm0, %xmm1
	mulpd	 %xmm0, %xmm3

	movlpd	 1 * SIZE(AO), %xmm2
	movhpd	 1 * SIZE(AO), %xmm2
	mulpd	 %xmm1, %xmm2
	subpd	 %xmm2, %xmm5

	movlpd	 1 * SIZE(AO), %xmm2
	movhpd	 1 * SIZE(AO), %xmm2
	mulpd	 %xmm3, %xmm2
	subpd	 %xmm2, %xmm7

	movlpd	 2 * SIZE(AO), %xmm4
	movhpd	 2 * SIZE(AO), %xmm4
	mulpd	 %xmm1, %xmm4
	subpd	 %xmm4, %xmm9
	movlpd	 2 * SIZE(AO), %xmm4
	movhpd	 2 * SIZE(AO), %xmm4
	mulpd	 %xmm3, %xmm4
	subpd	 %xmm4, %xmm11

	movlpd	 3 * SIZE(AO), %xmm6
	movhpd	 3 * SIZE(AO), %xmm6
	mulpd	 %xmm1, %xmm6
	subpd	 %xmm6, %xmm13
	movlpd	 3 * SIZE(AO), %xmm6
	movhpd	 3 * SIZE(AO), %xmm6
	mulpd	 %xmm3, %xmm6
	subpd	 %xmm6, %xmm15

	movlpd	 5 * SIZE(AO), %xmm0
	movhpd	 5 * SIZE(AO), %xmm0
	mulpd	 %xmm0, %xmm5
	mulpd	 %xmm0, %xmm7

	movlpd	 6 * SIZE(AO), %xmm2
	movhpd	 6 * SIZE(AO), %xmm2
	mulpd	 %xmm5, %xmm2
	subpd	 %xmm2, %xmm9
	movlpd	 6 * SIZE(AO), %xmm2
	movhpd	 6 * SIZE(AO), %xmm2
	mulpd	 %xmm7, %xmm2
	subpd	 %xmm2, %xmm11

	movlpd	 7 * SIZE(AO), %xmm4
	movhpd	 7 * SIZE(AO), %xmm4
	mulpd	 %xmm5, %xmm4
	subpd	 %xmm4, %xmm13
	movlpd	 7 * SIZE(AO), %xmm4
	movhpd	 7 * SIZE(AO), %xmm4
	mulpd	 %xmm7, %xmm4
	subpd	 %xmm4, %xmm15

	movlpd	10 * SIZE(AO), %xmm0
	movhpd	10 * SIZE(AO), %xmm0
	mulpd	 %xmm0, %xmm9
	mulpd	 %xmm0, %xmm11

	movlpd	11 * SIZE(AO), %xmm2
	movhpd	11 * SIZE(AO), %xmm2
	mulpd	 %xmm9, %xmm2
	subpd	 %xmm2, %xmm13
	movlpd	11 * SIZE(AO), %xmm2
	movhpd	11 * SIZE(AO), %xmm2
	mulpd	 %xmm11, %xmm2
	subpd	 %xmm2, %xmm15

	movlpd	15 * SIZE(AO), %xmm0
	movhpd	15 * SIZE(AO), %xmm0
	mulpd	 %xmm0, %xmm13
	mulpd	 %xmm0, %xmm15
#endif


#ifdef RN
	movlpd	 0 * SIZE(B), %xmm0
	movhpd	 0 * SIZE(B), %xmm0
	mulpd	 %xmm0, %xmm8
	mulpd	 %xmm0, %xmm9

	movlpd	 1 * SIZE(B), %xmm1
	movhpd	 1 * SIZE(B), %xmm1
	mulpd	 %xmm8, %xmm1
	subpd	 %xmm1, %xmm10
	movlpd	 1 * SIZE(B), %xmm1
	movhpd	 1 * SIZE(B), %xmm1
	mulpd	 %xmm9, %xmm1
	subpd	 %xmm1, %xmm11

	movlpd	 2 * SIZE(B), %xmm2
	movhpd	 2 * SIZE(B), %xmm2
	mulpd	 %xmm8, %xmm2
	subpd	 %xmm2, %xmm12
	movlpd	 2 * SIZE(B), %xmm2
	movhpd	 2 * SIZE(B), %xmm2
	mulpd	 %xmm9, %xmm2
	subpd	 %xmm2, %xmm13

	movlpd	 3 * SIZE(B), %xmm3
	movhpd	 3 * SIZE(B), %xmm3
	mulpd	 %xmm8, %xmm3
	subpd	 %xmm3, %xmm14
	movlpd	 3 * SIZE(B), %xmm3
	movhpd	 3 * SIZE(B), %xmm3
	mulpd	 %xmm9, %xmm3
	subpd	 %xmm3, %xmm15

	movlpd	 5 * SIZE(B), %xmm0
	movhpd	 5 * SIZE(B), %xmm0
	mulpd	 %xmm0, %xmm10
	mulpd	 %xmm0, %xmm11

	movlpd	 6 * SIZE(B), %xmm1
	movhpd	 6 * SIZE(B), %xmm1
	mulpd	 %xmm10, %xmm1
	subpd	 %xmm1, %xmm12
	movlpd	 6 * SIZE(B), %xmm1
	movhpd	 6 * SIZE(B), %xmm1
	mulpd	 %xmm11, %xmm1
	subpd	 %xmm1, %xmm13

	movlpd	 7 * SIZE(B), %xmm2
	movhpd	 7 * SIZE(B), %xmm2
	mulpd	 %xmm10, %xmm2
	subpd	 %xmm2, %xmm14
	movlpd	 7 * SIZE(B), %xmm2
	movhpd	 7 * SIZE(B), %xmm2
	mulpd	 %xmm11, %xmm2
	subpd	 %xmm2, %xmm15

	movlpd	10 * SIZE(B), %xmm0
	movhpd	10 * SIZE(B), %xmm0
	mulpd	 %xmm0, %xmm12
	mulpd	 %xmm0, %xmm13

	movlpd	11 * SIZE(B), %xmm1
	movhpd	11 * SIZE(B), %xmm1
	mulpd	 %xmm12, %xmm1
	subpd	 %xmm1, %xmm14
	movlpd	11 * SIZE(B), %xmm1
	movhpd	11 * SIZE(B), %xmm1
	mulpd	 %xmm13, %xmm1
	subpd	 %xmm1, %xmm15

	movlpd	15 * SIZE(B), %xmm0
	movhpd	15 * SIZE(B), %xmm0
	mulpd	 %xmm0, %xmm14
	mulpd	 %xmm0, %xmm15
#endif

#ifdef RT
	movlpd	15 * SIZE(B), %xmm0
	movhpd	15 * SIZE(B), %xmm0
	mulpd	 %xmm0, %xmm14
	mulpd	 %xmm0, %xmm15

	movlpd	14 * SIZE(B), %xmm1
	movhpd	14 * SIZE(B), %xmm1
	mulpd	 %xmm14, %xmm1
	subpd	 %xmm1, %xmm12
	movlpd	14 * SIZE(B), %xmm1
	movhpd	14 * SIZE(B), %xmm1
	mulpd	 %xmm15, %xmm1
	subpd	 %xmm1, %xmm13

	movlpd	13 * SIZE(B), %xmm2
	movhpd	13 * SIZE(B), %xmm2
	mulpd	 %xmm14, %xmm2
	subpd	 %xmm2, %xmm10
	movlpd	13 * SIZE(B), %xmm2
	movhpd	13 * SIZE(B), %xmm2
	mulpd	 %xmm15, %xmm2
	subpd	 %xmm2, %xmm11

	movlpd	12 * SIZE(B), %xmm3
	movhpd	12 * SIZE(B), %xmm3
	mulpd	 %xmm14, %xmm3
	subpd	 %xmm3, %xmm8
	movlpd	12 * SIZE(B), %xmm3
	movhpd	12 * SIZE(B), %xmm3
	mulpd	 %xmm15, %xmm3
	subpd	 %xmm3, %xmm9

	movlpd	10 * SIZE(B), %xmm0
	movhpd	10 * SIZE(B), %xmm0
	mulpd	 %xmm0, %xmm12
	mulpd	 %xmm0, %xmm13

	movlpd	 9 * SIZE(B), %xmm1
	movhpd	 9 * SIZE(B), %xmm1
	mulpd	 %xmm12, %xmm1
	subpd	 %xmm1, %xmm10
	movlpd	 9 * SIZE(B), %xmm1
	movhpd	 9 * SIZE(B), %xmm1
	mulpd	 %xmm13, %xmm1
	subpd	 %xmm1, %xmm11

	movlpd	 8 * SIZE(B), %xmm2
	movhpd	 8 * SIZE(B), %xmm2
	mulpd	 %xmm12, %xmm2
	subpd	 %xmm2, %xmm8
	movlpd	 8 * SIZE(B), %xmm2
	movhpd	 8 * SIZE(B), %xmm2
	mulpd	 %xmm13, %xmm2
	subpd	 %xmm2, %xmm9

	movlpd	 5 * SIZE(B), %xmm0
	movhpd	 5 * SIZE(B), %xmm0
	mulpd	 %xmm0, %xmm10
	mulpd	 %xmm0, %xmm11

	movlpd	 4 * SIZE(B), %xmm1
	movhpd	 4 * SIZE(B), %xmm1
	mulpd	 %xmm10, %xmm1
	subpd	 %xmm1, %xmm8
	movlpd	 4 * SIZE(B), %xmm1
	movhpd	 4 * SIZE(B), %xmm1
	mulpd	 %xmm11, %xmm1
	subpd	 %xmm1, %xmm9

	movlpd	 0 * SIZE(B), %xmm0
	movhpd	 0 * SIZE(B), %xmm0
	mulpd	 %xmm0, %xmm8
	mulpd	 %xmm0, %xmm9
#endif

#ifdef LN
	subq	$4 * SIZE, CO1
	subq	$4 * SIZE, CO2
#endif

#if defined(LN) || defined(LT)
	movsd	%xmm1,  0 * SIZE(CO1)
	movsd	%xmm5,  1 * SIZE(CO1)
	movsd	%xmm9,  2 * SIZE(CO1)
	movsd	%xmm13, 3 * SIZE(CO1)

	movhpd	%xmm1,  0 * SIZE(CO2)
	movhpd	%xmm5,  1 * SIZE(CO2)
	movhpd	%xmm9,  2 * SIZE(CO2)
	movhpd	%xmm13, 3 * SIZE(CO2)

	movsd	%xmm3,  0 * SIZE(CO1, LDC, 2)
	movsd	%xmm7,  1 * SIZE(CO1, LDC, 2)
	movsd	%xmm11, 2 * SIZE(CO1, LDC, 2)
	movsd	%xmm15, 3 * SIZE(CO1, LDC, 2)

	movhpd	%xmm3,  0 * SIZE(CO2, LDC, 2)
	movhpd	%xmm7,  1 * SIZE(CO2, LDC, 2)
	movhpd	%xmm11, 2 * SIZE(CO2, LDC, 2)
	movhpd	%xmm15, 3 * SIZE(CO2, LDC, 2)
#else
	movsd	%xmm8,  0 * SIZE(CO1)
	movhpd	%xmm8,  1 * SIZE(CO1)
	movsd	%xmm9,  2 * SIZE(CO1)
	movhpd	%xmm9,  3 * SIZE(CO1)

	movsd	%xmm10,  0 * SIZE(CO2)
	movhpd	%xmm10,  1 * SIZE(CO2)
	movsd	%xmm11,  2 * SIZE(CO2)
	movhpd	%xmm11,  3 * SIZE(CO2)

	movsd	%xmm12,  0 * SIZE(CO1, LDC, 2)
	movhpd	%xmm12,  1 * SIZE(CO1, LDC, 2)
	movsd	%xmm13,  2 * SIZE(CO1, LDC, 2)
	movhpd	%xmm13,  3 * SIZE(CO1, LDC, 2)

	movsd	%xmm14,  0 * SIZE(CO2, LDC, 2)
	movhpd	%xmm14,  1 * SIZE(CO2, LDC, 2)
	movsd	%xmm15,  2 * SIZE(CO2, LDC, 2)
	movhpd	%xmm15,  3 * SIZE(CO2, LDC, 2)
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm1,   0 * SIZE(B)
	movapd	%xmm3,   2 * SIZE(B)
	movapd	%xmm5,   4 * SIZE(B)
	movapd	%xmm7,   6 * SIZE(B)
	movapd	%xmm9,   8 * SIZE(B)
	movapd	%xmm11, 10 * SIZE(B)
	movapd	%xmm13, 12 * SIZE(B)
	movapd	%xmm15, 14 * SIZE(B)

	movlpd	%xmm1,   0 * SIZE(BO)
	movlpd	%xmm1,   1 * SIZE(BO)
	movhpd	%xmm1,   2 * SIZE(BO)
	movhpd	%xmm1,   3 * SIZE(BO)
	movlpd	%xmm3,   4 * SIZE(BO)
	movlpd	%xmm3,   5 * SIZE(BO)
	movhpd	%xmm3,   6 * SIZE(BO)
	movhpd	%xmm3,   7 * SIZE(BO)
	movlpd	%xmm5,   8 * SIZE(BO)
	movlpd	%xmm5,   9 * SIZE(BO)
	movhpd	%xmm5,  10 * SIZE(BO)
	movhpd	%xmm5,  11 * SIZE(BO)
	movlpd	%xmm7,  12 * SIZE(BO)
	movlpd	%xmm7,  13 * SIZE(BO)
	movhpd	%xmm7,  14 * SIZE(BO)
	movhpd	%xmm7,  15 * SIZE(BO)
	movlpd	%xmm9,  16 * SIZE(BO)
	movlpd	%xmm9,  17 * SIZE(BO)
	movhpd	%xmm9,  18 * SIZE(BO)
	movhpd	%xmm9,  19 * SIZE(BO)
	movlpd	%xmm11, 20 * SIZE(BO)
	movlpd	%xmm11, 21 * SIZE(BO)
	movhpd	%xmm11, 22 * SIZE(BO)
	movhpd	%xmm11, 23 * SIZE(BO)
	movlpd	%xmm13, 24 * SIZE(BO)
	movlpd	%xmm13, 25 * SIZE(BO)
	movhpd	%xmm13, 26 * SIZE(BO)
	movhpd	%xmm13, 27 * SIZE(BO)
	movlpd	%xmm15, 28 * SIZE(BO)
	movlpd	%xmm15, 29 * SIZE(BO)
	movhpd	%xmm15, 30 * SIZE(BO)
	movhpd	%xmm15, 31 * SIZE(BO)
#else
	movapd	%xmm8,   0 * SIZE(AO)
	movapd	%xmm9,   2 * SIZE(AO)
	movapd	%xmm10,  4 * SIZE(AO)
	movapd	%xmm11,  6 * SIZE(AO)
	movapd	%xmm12,  8 * SIZE(AO)
	movapd	%xmm13, 10 * SIZE(AO)
	movapd	%xmm14, 12 * SIZE(AO)
	movapd	%xmm15, 14 * SIZE(AO)
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

.L20:
	testq	$3, M
	je	.L39

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

	leaq	BUFFER, BO

#if defined(LN) || defined(RT)
	movq	KK, %rax
	salq	$2 + BASE_SHIFT, %rax
	leaq	(BO, %rax, 2), BO
#endif

	movapd	 0 * SIZE(AO), %xmm8
	pxor	%xmm0, %xmm0
	movapd	 0 * SIZE(BO), %xmm9
	pxor	%xmm1, %xmm1
	movapd	 8 * SIZE(AO), %xmm10
	pxor	%xmm2, %xmm2
	movapd	 8 * SIZE(BO), %xmm11
	pxor	%xmm3, %xmm3

	movapd	16 * SIZE(BO), %xmm13
	movapd	24 * SIZE(BO), %xmm15

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	sarq	$3, %rax
	je	.L25
	ALIGN_4

.L22:
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm0
	PREFETCH	(PREFETCHSIZE     +  0) * SIZE(AO)
	movapd	 2 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm1
	movapd	 4 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	mulpd	 6 * SIZE(BO), %xmm8
	addpd	%xmm9, %xmm2
	movapd	32 * SIZE(BO), %xmm9
	addpd	%xmm8, %xmm3
	movapd	 2 * SIZE(AO), %xmm8

	mulpd	%xmm8, %xmm11
	addpd	%xmm11, %xmm0
	movapd	10 * SIZE(BO), %xmm11
	mulpd	%xmm8, %xmm11
	addpd	%xmm11, %xmm1
	movapd	12 * SIZE(BO), %xmm11
	mulpd	%xmm8, %xmm11
	mulpd	14 * SIZE(BO), %xmm8
	addpd	%xmm11, %xmm2
	movapd	40 * SIZE(BO), %xmm11
	addpd	%xmm8, %xmm3
	movapd	 4 * SIZE(AO), %xmm8

	mulpd	%xmm8, %xmm13
	addpd	%xmm13, %xmm0
	movapd	18 * SIZE(BO), %xmm13
	mulpd	%xmm8, %xmm13
	addpd	%xmm13, %xmm1
	movapd	20 * SIZE(BO), %xmm13
	mulpd	%xmm8, %xmm13
	mulpd	22 * SIZE(BO), %xmm8
	addpd	%xmm13, %xmm2
	movapd	48 * SIZE(BO), %xmm13
	addpd	%xmm8, %xmm3
	movapd	 6 * SIZE(AO), %xmm8

	mulpd	%xmm8, %xmm15
	addpd	%xmm15, %xmm0
	movapd	26 * SIZE(BO), %xmm15
	mulpd	%xmm8, %xmm15
	addpd	%xmm15, %xmm1
	movapd	28 * SIZE(BO), %xmm15
	mulpd	%xmm8, %xmm15
	mulpd	30 * SIZE(BO), %xmm8
	addpd	%xmm15, %xmm2
	movapd	56 * SIZE(BO), %xmm15
	addpd	%xmm8, %xmm3
	movapd	16 * SIZE(AO), %xmm8

	PREFETCH	(PREFETCHSIZE     +  8) * SIZE(AO)
	mulpd	%xmm10, %xmm9
	addpd	%xmm9, %xmm0
	movapd	34 * SIZE(BO), %xmm9
	mulpd	%xmm10, %xmm9
	addpd	%xmm9, %xmm1
	movapd	36 * SIZE(BO), %xmm9
	mulpd	%xmm10, %xmm9
	mulpd	38 * SIZE(BO), %xmm10
	addpd	%xmm9, %xmm2
	movapd	64 * SIZE(BO), %xmm9
	addpd	%xmm10, %xmm3
	movapd	10 * SIZE(AO), %xmm10

	mulpd	%xmm10, %xmm11
	addpd	%xmm11, %xmm0
	movapd	42 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm11
	addpd	%xmm11, %xmm1
	movapd	44 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm11
	mulpd	46 * SIZE(BO), %xmm10
	addpd	%xmm11, %xmm2
	movapd	72 * SIZE(BO), %xmm11
	addpd	%xmm10, %xmm3
	movapd	12 * SIZE(AO), %xmm10

	mulpd	%xmm10, %xmm13
	addpd	%xmm13, %xmm0
	movapd	50 * SIZE(BO), %xmm13
	mulpd	%xmm10, %xmm13
	addpd	%xmm13, %xmm1
	movapd	52 * SIZE(BO), %xmm13
	mulpd	%xmm10, %xmm13
	mulpd	54 * SIZE(BO), %xmm10
	addpd	%xmm13, %xmm2
	movapd	80 * SIZE(BO), %xmm13
	addpd	%xmm10, %xmm3
	movapd	14 * SIZE(AO), %xmm10

	mulpd	%xmm10, %xmm15
	addpd	%xmm15, %xmm0
	movapd	58 * SIZE(BO), %xmm15
	mulpd	%xmm10, %xmm15
	addpd	%xmm15, %xmm1
	movapd	60 * SIZE(BO), %xmm15
	mulpd	%xmm10, %xmm15
	mulpd	62 * SIZE(BO), %xmm10
	addpd	%xmm15, %xmm2
	movapd	88 * SIZE(BO), %xmm15
	addpd	%xmm10, %xmm3
	movapd	24 * SIZE(AO), %xmm10

	addq   $16 * SIZE, AO
	addq   $64 * SIZE, BO
	decq   %rax
	jne    .L22
	ALIGN_4

.L25:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L29
	ALIGN_4

.L26:
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm0
	movapd	 2 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm1
	movapd	 4 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	mulpd	 6 * SIZE(BO), %xmm8
	addpd	%xmm9, %xmm2
	movapd	 8 * SIZE(BO), %xmm9
	addpd	%xmm8, %xmm3
	movapd	 2 * SIZE(AO), %xmm8

	addq	$2 * SIZE, AO		# aoffset  += 4
	addq	$8 * SIZE, BO		# boffset1 += 8
	decq	%rax
	jg	.L26
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
	leaq	BUFFER, BO

	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(B,  %rax, 4), B
	leaq	(BO, %rax, 8), BO
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm0, %xmm8
	unpcklpd %xmm1, %xmm0
	unpckhpd %xmm1, %xmm8

	movapd	%xmm2, %xmm10
	unpcklpd %xmm3, %xmm2
	unpckhpd %xmm3, %xmm10

	movapd	 0 * SIZE(B), %xmm1
	movapd	 2 * SIZE(B), %xmm3
	movapd	 4 * SIZE(B), %xmm5
	movapd	 6 * SIZE(B), %xmm7

	subpd	%xmm0,  %xmm1
	subpd	%xmm2,  %xmm3
	subpd	%xmm8,  %xmm5
	subpd	%xmm10, %xmm7
#else
	movapd	 0 * SIZE(AO), %xmm8
	movapd	 2 * SIZE(AO), %xmm10
	movapd	 4 * SIZE(AO), %xmm12
	movapd	 6 * SIZE(AO), %xmm14

	subpd	%xmm0, %xmm8
	subpd	%xmm1, %xmm10
	subpd	%xmm2, %xmm12
	subpd	%xmm3, %xmm14
#endif

#ifdef LN
	movlpd	 3 * SIZE(AO), %xmm0
	movhpd	 3 * SIZE(AO), %xmm0
	mulpd	 %xmm0, %xmm5
	mulpd	 %xmm0, %xmm7

	movlpd	 2 * SIZE(AO), %xmm2
	movhpd	 2 * SIZE(AO), %xmm2
	mulpd	 %xmm5, %xmm2
	subpd	 %xmm2, %xmm1
	movlpd	 2 * SIZE(AO), %xmm2
	movhpd	 2 * SIZE(AO), %xmm2
	mulpd	 %xmm7, %xmm2
	subpd	 %xmm2, %xmm3

	movlpd	 0 * SIZE(AO), %xmm0
	movhpd	 0 * SIZE(AO), %xmm0
	mulpd	 %xmm0, %xmm1
	mulpd	 %xmm0, %xmm3
#endif

#ifdef LT
	movlpd	 0 * SIZE(AO), %xmm0
	movhpd	 0 * SIZE(AO), %xmm0
	mulpd	 %xmm0, %xmm1
	mulpd	 %xmm0, %xmm3

	movlpd	 1 * SIZE(AO), %xmm2
	movhpd	 1 * SIZE(AO), %xmm2
	mulpd	 %xmm1, %xmm2
	subpd	 %xmm2, %xmm5
	movlpd	 1 * SIZE(AO), %xmm2
	movhpd	 1 * SIZE(AO), %xmm2
	mulpd	 %xmm3, %xmm2
	subpd	 %xmm2, %xmm7

	movlpd	 3 * SIZE(AO), %xmm0
	movhpd	 3 * SIZE(AO), %xmm0
	mulpd	 %xmm0, %xmm5
	mulpd	 %xmm0, %xmm7
#endif

#ifdef RN
	movlpd	 0 * SIZE(B), %xmm0
	movhpd	 0 * SIZE(B), %xmm0
	mulpd	 %xmm0, %xmm8

	movlpd	 1 * SIZE(B), %xmm1
	movhpd	 1 * SIZE(B), %xmm1
	mulpd	 %xmm8, %xmm1
	subpd	 %xmm1, %xmm10
	movlpd	 2 * SIZE(B), %xmm2
	movhpd	 2 * SIZE(B), %xmm2
	mulpd	 %xmm8, %xmm2
	subpd	 %xmm2, %xmm12
	movlpd	 3 * SIZE(B), %xmm3
	movhpd	 3 * SIZE(B), %xmm3
	mulpd	 %xmm8, %xmm3
	subpd	 %xmm3, %xmm14

	movlpd	 5 * SIZE(B), %xmm0
	movhpd	 5 * SIZE(B), %xmm0
	mulpd	 %xmm0, %xmm10
	movlpd	 6 * SIZE(B), %xmm1
	movhpd	 6 * SIZE(B), %xmm1
	mulpd	 %xmm10, %xmm1
	subpd	 %xmm1, %xmm12
	movlpd	 7 * SIZE(B), %xmm2
	movhpd	 7 * SIZE(B), %xmm2
	mulpd	 %xmm10, %xmm2
	subpd	 %xmm2, %xmm14

	movlpd	10 * SIZE(B), %xmm0
	movhpd	10 * SIZE(B), %xmm0
	mulpd	 %xmm0, %xmm12

	movlpd	11 * SIZE(B), %xmm1
	movhpd	11 * SIZE(B), %xmm1
	mulpd	 %xmm12, %xmm1
	subpd	 %xmm1, %xmm14

	movlpd	15 * SIZE(B), %xmm0
	movhpd	15 * SIZE(B), %xmm0
	mulpd	 %xmm0, %xmm14
#endif

#ifdef RT
	movlpd	15 * SIZE(B), %xmm0
	movhpd	15 * SIZE(B), %xmm0
	mulpd	 %xmm0, %xmm14

	movlpd	14 * SIZE(B), %xmm1
	movhpd	14 * SIZE(B), %xmm1
	mulpd	 %xmm14, %xmm1
	subpd	 %xmm1, %xmm12
	movlpd	13 * SIZE(B), %xmm2
	movhpd	13 * SIZE(B), %xmm2
	mulpd	 %xmm14, %xmm2
	subpd	 %xmm2, %xmm10
	movlpd	12 * SIZE(B), %xmm3
	movhpd	12 * SIZE(B), %xmm3
	mulpd	 %xmm14, %xmm3
	subpd	 %xmm3, %xmm8

	movlpd	10 * SIZE(B), %xmm0
	movhpd	10 * SIZE(B), %xmm0
	mulpd	 %xmm0, %xmm12
	movlpd	 9 * SIZE(B), %xmm1
	movhpd	 9 * SIZE(B), %xmm1
	mulpd	 %xmm12, %xmm1
	subpd	 %xmm1, %xmm10
	movlpd	 8 * SIZE(B), %xmm2
	movhpd	 8 * SIZE(B), %xmm2
	mulpd	 %xmm12, %xmm2
	subpd	 %xmm2, %xmm8

	movlpd	 5 * SIZE(B), %xmm0
	movhpd	 5 * SIZE(B), %xmm0
	mulpd	 %xmm0, %xmm10
	movlpd	 4 * SIZE(B), %xmm1
	movhpd	 4 * SIZE(B), %xmm1
	mulpd	 %xmm10, %xmm1
	subpd	 %xmm1, %xmm8

	movlpd	 0 * SIZE(B), %xmm0
	movhpd	 0 * SIZE(B), %xmm0
	mulpd	 %xmm0, %xmm8
#endif

#ifdef LN
	subq	$2 * SIZE, CO1
	subq	$2 * SIZE, CO2
#endif

#if defined(LN) || defined(LT)
	movsd	%xmm1,  0 * SIZE(CO1)
	movsd	%xmm5,  1 * SIZE(CO1)

	movhpd	%xmm1,  0 * SIZE(CO2)
	movhpd	%xmm5,  1 * SIZE(CO2)

	movsd	%xmm3,  0 * SIZE(CO1, LDC, 2)
	movsd	%xmm7,  1 * SIZE(CO1, LDC, 2)

	movhpd	%xmm3,  0 * SIZE(CO2, LDC, 2)
	movhpd	%xmm7,  1 * SIZE(CO2, LDC, 2)
#else
	movsd	%xmm8,  0 * SIZE(CO1)
	movhpd	%xmm8,  1 * SIZE(CO1)

	movsd	%xmm10,  0 * SIZE(CO2)
	movhpd	%xmm10,  1 * SIZE(CO2)

	movsd	%xmm12,  0 * SIZE(CO1, LDC, 2)
	movhpd	%xmm12,  1 * SIZE(CO1, LDC, 2)

	movsd	%xmm14,  0 * SIZE(CO2, LDC, 2)
	movhpd	%xmm14,  1 * SIZE(CO2, LDC, 2)
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm1,   0 * SIZE(B)
	movapd	%xmm3,   2 * SIZE(B)
	movapd	%xmm5,   4 * SIZE(B)
	movapd	%xmm7,   6 * SIZE(B)

	movlpd	%xmm1,   0 * SIZE(BO)
	movlpd	%xmm1,   1 * SIZE(BO)
	movhpd	%xmm1,   2 * SIZE(BO)
	movhpd	%xmm1,   3 * SIZE(BO)
	movlpd	%xmm3,   4 * SIZE(BO)
	movlpd	%xmm3,   5 * SIZE(BO)
	movhpd	%xmm3,   6 * SIZE(BO)
	movhpd	%xmm3,   7 * SIZE(BO)
	movlpd	%xmm5,   8 * SIZE(BO)
	movlpd	%xmm5,   9 * SIZE(BO)
	movhpd	%xmm5,  10 * SIZE(BO)
	movhpd	%xmm5,  11 * SIZE(BO)
	movlpd	%xmm7,  12 * SIZE(BO)
	movlpd	%xmm7,  13 * SIZE(BO)
	movhpd	%xmm7,  14 * SIZE(BO)
	movhpd	%xmm7,  15 * SIZE(BO)
#else
	movapd	%xmm8,   0 * SIZE(AO)
	movapd	%xmm10,  2 * SIZE(AO)
	movapd	%xmm12,  4 * SIZE(AO)
	movapd	%xmm14,  6 * SIZE(AO)
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
	testq	$1, M
	je	.L39
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

	leaq	BUFFER, BO

#if defined(LN) || defined(RT)
	movq	KK, %rax
	salq	$2 + BASE_SHIFT, %rax
	leaq	(BO, %rax, 2), BO
#endif

	movsd	 0 * SIZE(AO), %xmm8
	pxor	%xmm0, %xmm0
	movsd	 0 * SIZE(BO), %xmm9
	pxor	%xmm1, %xmm1
	movsd	 8 * SIZE(AO), %xmm10
	pxor	%xmm2, %xmm2
	movsd	 8 * SIZE(BO), %xmm11
	pxor	%xmm3, %xmm3

	movsd	16 * SIZE(BO), %xmm13
	movsd	24 * SIZE(BO), %xmm15

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	sarq	$3, %rax
	je	.L35
	ALIGN_4

.L32:
	mulsd	%xmm8, %xmm9
	addsd	%xmm9, %xmm0
	PREFETCH	(PREFETCHSIZE     +  0) * SIZE(AO)
	movsd	 2 * SIZE(BO), %xmm9
	mulsd	%xmm8, %xmm9
	addsd	%xmm9, %xmm1
	movsd	 4 * SIZE(BO), %xmm9
	mulsd	%xmm8, %xmm9
	mulsd	 6 * SIZE(BO), %xmm8
	addsd	%xmm9, %xmm2
	movsd	32 * SIZE(BO), %xmm9
	addsd	%xmm8, %xmm3
	movsd	 1 * SIZE(AO), %xmm8

	mulsd	%xmm8, %xmm11
	addsd	%xmm11, %xmm0
	movsd	10 * SIZE(BO), %xmm11
	mulsd	%xmm8, %xmm11
	addsd	%xmm11, %xmm1
	movsd	12 * SIZE(BO), %xmm11
	mulsd	%xmm8, %xmm11
	mulsd	14 * SIZE(BO), %xmm8
	addsd	%xmm11, %xmm2
	movsd	40 * SIZE(BO), %xmm11
	addsd	%xmm8, %xmm3
	movsd	 2 * SIZE(AO), %xmm8

	mulsd	%xmm8, %xmm13
	addsd	%xmm13, %xmm0
	movsd	18 * SIZE(BO), %xmm13
	mulsd	%xmm8, %xmm13
	addsd	%xmm13, %xmm1
	movsd	20 * SIZE(BO), %xmm13
	mulsd	%xmm8, %xmm13
	mulsd	22 * SIZE(BO), %xmm8
	addsd	%xmm13, %xmm2
	movsd	48 * SIZE(BO), %xmm13
	addsd	%xmm8, %xmm3
	movsd	 3 * SIZE(AO), %xmm8

	mulsd	%xmm8, %xmm15
	addsd	%xmm15, %xmm0
	movsd	26 * SIZE(BO), %xmm15
	mulsd	%xmm8, %xmm15
	addsd	%xmm15, %xmm1
	movsd	28 * SIZE(BO), %xmm15
	mulsd	%xmm8, %xmm15
	mulsd	30 * SIZE(BO), %xmm8
	addsd	%xmm15, %xmm2
	movsd	56 * SIZE(BO), %xmm15
	addsd	%xmm8, %xmm3
	movsd	 4 * SIZE(AO), %xmm8

	mulsd	%xmm8, %xmm9
	addsd	%xmm9, %xmm0
	movsd	34 * SIZE(BO), %xmm9
	mulsd	%xmm8, %xmm9
	addsd	%xmm9, %xmm1
	movsd	36 * SIZE(BO), %xmm9
	mulsd	%xmm8, %xmm9
	mulsd	38 * SIZE(BO), %xmm8
	addsd	%xmm9, %xmm2
	movsd	64 * SIZE(BO), %xmm9
	addsd	%xmm8, %xmm3
	movsd	 5 * SIZE(AO), %xmm8

	mulsd	%xmm8, %xmm11
	addsd	%xmm11, %xmm0
	movsd	42 * SIZE(BO), %xmm11
	mulsd	%xmm8, %xmm11
	addsd	%xmm11, %xmm1
	movsd	44 * SIZE(BO), %xmm11
	mulsd	%xmm8, %xmm11
	mulsd	46 * SIZE(BO), %xmm8
	addsd	%xmm11, %xmm2
	movsd	72 * SIZE(BO), %xmm11
	addsd	%xmm8, %xmm3
	movsd	 6 * SIZE(AO), %xmm8

	mulsd	%xmm8, %xmm13
	addsd	%xmm13, %xmm0
	movsd	50 * SIZE(BO), %xmm13
	mulsd	%xmm8, %xmm13
	addsd	%xmm13, %xmm1
	movsd	52 * SIZE(BO), %xmm13
	mulsd	%xmm8, %xmm13
	mulsd	54 * SIZE(BO), %xmm8
	addsd	%xmm13, %xmm2
	movsd	80 * SIZE(BO), %xmm13
	addsd	%xmm8, %xmm3
	movsd	 7 * SIZE(AO), %xmm8

	mulsd	%xmm8, %xmm15
	addsd	%xmm15, %xmm0
	movsd	58 * SIZE(BO), %xmm15
	mulsd	%xmm8, %xmm15
	addsd	%xmm15, %xmm1
	movsd	60 * SIZE(BO), %xmm15
	mulsd	%xmm8, %xmm15
	mulsd	62 * SIZE(BO), %xmm8
	addsd	%xmm15, %xmm2
	movsd	88 * SIZE(BO), %xmm15
	addsd	%xmm8, %xmm3
	movsd	 8 * SIZE(AO), %xmm8

	addq   $ 8 * SIZE, AO
	addq   $64 * SIZE, BO
	decq   %rax
	jne    .L32
	ALIGN_4

.L35:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L38
	ALIGN_4

.L36:
	mulsd	%xmm8, %xmm9
	addsd	%xmm9, %xmm0
	movsd	 2 * SIZE(BO), %xmm9
	mulsd	%xmm8, %xmm9
	addsd	%xmm9, %xmm1
	movsd	 4 * SIZE(BO), %xmm9
	mulsd	%xmm8, %xmm9
	mulsd	 6 * SIZE(BO), %xmm8
	addsd	%xmm9, %xmm2
	movsd	 8 * SIZE(BO), %xmm9
	addsd	%xmm8, %xmm3
	movsd	 1 * SIZE(AO), %xmm8

	addq	$1 * SIZE, AO		# aoffset  += 4
	addq	$8 * SIZE, BO		# boffset1 += 8
	decq	%rax
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
	leaq	BUFFER, BO

	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(B,  %rax, 4), B
	leaq	(BO, %rax, 8), BO
#endif

#if defined(LN) || defined(LT)
	movsd	 0 * SIZE(B), %xmm4
	movsd	 1 * SIZE(B), %xmm5
	movsd	 2 * SIZE(B), %xmm6
	movsd	 3 * SIZE(B), %xmm7
#else
	movsd	 0 * SIZE(AO), %xmm4
	movsd	 1 * SIZE(AO), %xmm5
	movsd	 2 * SIZE(AO), %xmm6
	movsd	 3 * SIZE(AO), %xmm7
#endif

	subsd	%xmm0,  %xmm4
	subsd	%xmm1,  %xmm5
	subsd	%xmm2,  %xmm6
	subsd	%xmm3,  %xmm7

#ifdef LN
	movsd	 0 * SIZE(AO), %xmm0

	mulsd	 %xmm0, %xmm4
	mulsd	 %xmm0, %xmm5
	mulsd	 %xmm0, %xmm6
	mulsd	 %xmm0, %xmm7
#endif

#ifdef LT
	movsd	 0 * SIZE(AO), %xmm0

	mulsd	 %xmm0, %xmm4
	mulsd	 %xmm0, %xmm5
	mulsd	 %xmm0, %xmm6
	mulsd	 %xmm0, %xmm7
#endif

#ifdef RN
	mulsd	 0 * SIZE(B), %xmm4
	movlpd	 1 * SIZE(B), %xmm1
	mulsd	 %xmm4, %xmm1
	subsd	 %xmm1, %xmm5
	movlpd	 2 * SIZE(B), %xmm2
	mulsd	 %xmm4, %xmm2
	subsd	 %xmm2, %xmm6
	movlpd	 3 * SIZE(B), %xmm3
	mulsd	 %xmm4, %xmm3
	subsd	 %xmm3, %xmm7

	mulsd	 5 * SIZE(B), %xmm5
	movlpd	 6 * SIZE(B), %xmm1
	mulsd	 %xmm5, %xmm1
	subsd	 %xmm1, %xmm6
	movlpd	 7 * SIZE(B), %xmm2
	mulsd	 %xmm5, %xmm2
	subsd	 %xmm2, %xmm7

	mulsd	10 * SIZE(B), %xmm6
	movlpd	11 * SIZE(B), %xmm1
	mulsd	 %xmm6, %xmm1
	subsd	 %xmm1, %xmm7

	mulsd	15 * SIZE(B), %xmm7
#endif

#ifdef RT
	mulsd	 15 * SIZE(B), %xmm7

	movlpd	14 * SIZE(B), %xmm1
	mulsd	 %xmm7, %xmm1
	subsd	 %xmm1, %xmm6
	movlpd	13 * SIZE(B), %xmm2
	mulsd	 %xmm7, %xmm2
	subsd	 %xmm2, %xmm5
	movlpd	12 * SIZE(B), %xmm3
	mulsd	 %xmm7, %xmm3
	subsd	 %xmm3, %xmm4

	mulsd	 10 * SIZE(B), %xmm6

	movlpd	 9 * SIZE(B), %xmm1
	mulsd	 %xmm6, %xmm1
	subsd	 %xmm1, %xmm5
	movlpd	 8 * SIZE(B), %xmm2
	mulsd	 %xmm6, %xmm2
	subsd	 %xmm2, %xmm4

	mulsd	 5 * SIZE(B), %xmm5

	movlpd	 4 * SIZE(B), %xmm1
	mulsd	 %xmm5, %xmm1
	subsd	 %xmm1, %xmm4

	mulsd	 0 * SIZE(B), %xmm4
#endif

#ifdef LN
	subq	$1 * SIZE, CO1
	subq	$1 * SIZE, CO2
#endif

	movsd	%xmm4,  0 * SIZE(CO1)
	movsd	%xmm5,  0 * SIZE(CO2)
	movsd	%xmm6,  0 * SIZE(CO1, LDC, 2)
	movsd	%xmm7,  0 * SIZE(CO2, LDC, 2)

#if defined(LN) || defined(LT)
	movsd	%xmm4,   0 * SIZE(B)
	movsd	%xmm5,   1 * SIZE(B)
	movsd	%xmm6,   2 * SIZE(B)
	movsd	%xmm7,   3 * SIZE(B)

	movsd	%xmm4,   0 * SIZE(BO)
	movsd	%xmm4,   1 * SIZE(BO)
	movsd	%xmm5,   2 * SIZE(BO)
	movsd	%xmm5,   3 * SIZE(BO)
	movsd	%xmm6,   4 * SIZE(BO)
	movsd	%xmm6,   5 * SIZE(BO)
	movsd	%xmm7,   6 * SIZE(BO)
	movsd	%xmm7,   7 * SIZE(BO)
#else
	movsd	%xmm4,   0 * SIZE(AO)
	movsd	%xmm5,   1 * SIZE(AO)
	movsd	%xmm6,   2 * SIZE(AO)
	movsd	%xmm7,   3 * SIZE(AO)
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
	PREFETCH	 56 * SIZE(B)

	movsd	 0 * SIZE(B), %xmm0
	movsd	 1 * SIZE(B), %xmm1
	movsd	 2 * SIZE(B), %xmm2
	movsd	 3 * SIZE(B), %xmm3
	movsd	 4 * SIZE(B), %xmm4
	movsd	 5 * SIZE(B), %xmm5
	movsd	 6 * SIZE(B), %xmm6
	movsd	 7 * SIZE(B), %xmm7

	addq	$ 8 * SIZE, B
	addq	$16 * SIZE, BO

	movsd	%xmm0, -16 * SIZE(BO)
	movsd	%xmm0, -15 * SIZE(BO)
	movsd	%xmm1, -14 * SIZE(BO)
	movsd	%xmm1, -13 * SIZE(BO)
	movsd	%xmm2, -12 * SIZE(BO)
	movsd	%xmm2, -11 * SIZE(BO)
	movsd	%xmm3, -10 * SIZE(BO)
	movsd	%xmm3,  -9 * SIZE(BO)
	movsd	%xmm4,  -8 * SIZE(BO)
	movsd	%xmm4,  -7 * SIZE(BO)
	movsd	%xmm5,  -6 * SIZE(BO)
	movsd	%xmm5,  -5 * SIZE(BO)
	movsd	%xmm6,  -4 * SIZE(BO)
	movsd	%xmm6,  -3 * SIZE(BO)
	movsd	%xmm7,  -2 * SIZE(BO)
	movsd	%xmm7,  -1 * SIZE(BO)

	decq	%rax
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
	movsd	 0 * SIZE(B), %xmm0
	movsd	 1 * SIZE(B), %xmm1

	movsd	%xmm0,  0 * SIZE(BO)
	movsd	%xmm0,  1 * SIZE(BO)
	movsd	%xmm1,  2 * SIZE(BO)
	movsd	%xmm1,  3 * SIZE(BO)

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

	movq	M,  I
	sarq	$2, I	# i = (m >> 2)
	jle	.L60
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

	leaq	BUFFER, BO

#if defined(LN) || defined(RT)
	movq	KK, %rax
	salq	$1 + BASE_SHIFT, %rax
	leaq	(BO, %rax, 2), BO
#endif

	movapd	 0 * SIZE(AO), %xmm8
	pxor	%xmm0, %xmm0
	movapd	 0 * SIZE(BO), %xmm9
	pxor	%xmm1, %xmm1
	movapd	 8 * SIZE(AO), %xmm10
	pxor	%xmm4, %xmm4
	movapd	 8 * SIZE(BO), %xmm11
	pxor	%xmm5, %xmm5

	movapd	16 * SIZE(AO), %xmm12
	movapd	16 * SIZE(BO), %xmm13
	movapd	24 * SIZE(AO), %xmm14
	movapd	24 * SIZE(BO), %xmm15

	PREFETCHW      4 * SIZE(CO1)
	PREFETCHW      4 * SIZE(CO2)

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	sarq	$3, %rax
	je	.L55
	ALIGN_4

.L52:
	mulpd	%xmm8, %xmm9
	PREFETCH	(PREFETCHSIZE     +  0) * SIZE(AO)
	mulpd	 2 * SIZE(BO), %xmm8
	addpd	%xmm9, %xmm0
	movapd	 0 * SIZE(BO), %xmm9
	addpd	%xmm8, %xmm1
	movapd	 2 * SIZE(AO), %xmm8
	mulpd	%xmm8, %xmm9
	mulpd	 2 * SIZE(BO), %xmm8
	addpd	%xmm9, %xmm4
	movapd	 4 * SIZE(BO), %xmm9
	addpd	%xmm8, %xmm5
	movapd	 4 * SIZE(AO), %xmm8

	mulpd	%xmm8, %xmm9
	mulpd	 6 * SIZE(BO), %xmm8
	addpd	%xmm9, %xmm0
	movapd	 4 * SIZE(BO), %xmm9
	addpd	%xmm8, %xmm1
	movapd	 6 * SIZE(AO), %xmm8
	mulpd	%xmm8, %xmm9
	mulpd	 6 * SIZE(BO), %xmm8
	addpd	%xmm9, %xmm4
	movapd	32 * SIZE(BO), %xmm9
	addpd	%xmm8, %xmm5
	movapd	32 * SIZE(AO), %xmm8

	PREFETCH	(PREFETCHSIZE     +  8) * SIZE(AO)
	mulpd	%xmm10, %xmm11
	mulpd	10 * SIZE(BO), %xmm10
	addpd	%xmm11, %xmm0
	movapd	 8 * SIZE(BO), %xmm11
	addpd	%xmm10, %xmm1
	movapd	10 * SIZE(AO), %xmm10
	mulpd	%xmm10, %xmm11
	mulpd	10 * SIZE(BO), %xmm10
	addpd	%xmm11, %xmm4
	movapd	12 * SIZE(BO), %xmm11
	addpd	%xmm10, %xmm5
	movapd	12 * SIZE(AO), %xmm10

	mulpd	%xmm10, %xmm11
	mulpd	14 * SIZE(BO), %xmm10
	addpd	%xmm11, %xmm0
	movapd	12 * SIZE(BO), %xmm11
	addpd	%xmm10, %xmm1
	movapd	14 * SIZE(AO), %xmm10
	mulpd	%xmm10, %xmm11
	mulpd	14 * SIZE(BO), %xmm10
	addpd	%xmm11, %xmm4
	movapd	40 * SIZE(BO), %xmm11
	addpd	%xmm10, %xmm5
	movapd	40 * SIZE(AO), %xmm10

	PREFETCH	(PREFETCHSIZE     + 16) * SIZE(AO)
	mulpd	%xmm12, %xmm13
	mulpd	18 * SIZE(BO), %xmm12
	addpd	%xmm13, %xmm0
	movapd	16 * SIZE(BO), %xmm13
	addpd	%xmm12, %xmm1
	movapd	18 * SIZE(AO), %xmm12
	mulpd	%xmm12, %xmm13
	mulpd	18 * SIZE(BO), %xmm12
	addpd	%xmm13, %xmm4
	movapd	20 * SIZE(BO), %xmm13
	addpd	%xmm12, %xmm5
	movapd	20 * SIZE(AO), %xmm12

	mulpd	%xmm12, %xmm13
	mulpd	22 * SIZE(BO), %xmm12
	addpd	%xmm13, %xmm0
	movapd	20 * SIZE(BO), %xmm13
	addpd	%xmm12, %xmm1
	movapd	22 * SIZE(AO), %xmm12
	mulpd	%xmm12, %xmm13
	mulpd	22 * SIZE(BO), %xmm12
	addpd	%xmm13, %xmm4
	movapd	48 * SIZE(BO), %xmm13
	addpd	%xmm12, %xmm5
	movapd	48 * SIZE(AO), %xmm12

	PREFETCH	(PREFETCHSIZE     + 24) * SIZE(AO)
	mulpd	%xmm14, %xmm15
	mulpd	26 * SIZE(BO), %xmm14
	addpd	%xmm15, %xmm0
	movapd	24 * SIZE(BO), %xmm15
	addpd	%xmm14, %xmm1
	movapd	26 * SIZE(AO), %xmm14
	mulpd	%xmm14, %xmm15
	mulpd	26 * SIZE(BO), %xmm14
	addpd	%xmm15, %xmm4
	movapd	28 * SIZE(BO), %xmm15
	addpd	%xmm14, %xmm5
	movapd	28 * SIZE(AO), %xmm14

	mulpd	%xmm14, %xmm15
	mulpd	30 * SIZE(BO), %xmm14
	addpd	%xmm15, %xmm0
	movapd	28 * SIZE(BO), %xmm15
	addpd	%xmm14, %xmm1
	movapd	30 * SIZE(AO), %xmm14
	mulpd	%xmm14, %xmm15
	mulpd	30 * SIZE(BO), %xmm14
	addpd	%xmm15, %xmm4
	movapd	56 * SIZE(BO), %xmm15
	addpd	%xmm14, %xmm5
	movapd	56 * SIZE(AO), %xmm14

	addq   $32 * SIZE, AO
	addq   $32 * SIZE, BO
	decq   %rax
	jne    .L52
	ALIGN_4

.L55:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L59
	ALIGN_4

.L56:
	movapd	 0 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm0
	mulpd	 2 * SIZE(BO), %xmm8
	addpd	%xmm8, %xmm1
	movapd	 2 * SIZE(AO), %xmm8
	movapd	 0 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm4
	mulpd	 2 * SIZE(BO), %xmm8
	addpd	%xmm8, %xmm5
	movapd	 4 * SIZE(AO), %xmm8

	addq	$4 * SIZE, AO		# aoffset  += 4
	addq	$4 * SIZE, BO		# boffset1 += 8
	decq	%rax
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
	leaq	BUFFER, BO

	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(B,  %rax, 2), B
	leaq	(BO, %rax, 4), BO
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm0, %xmm8
	unpcklpd %xmm1, %xmm0
	unpckhpd %xmm1, %xmm8

	movapd	%xmm4, %xmm12
	unpcklpd %xmm5, %xmm4
	unpckhpd %xmm5, %xmm12

	movapd	 0 * SIZE(B), %xmm1
	movapd	 2 * SIZE(B), %xmm5
	movapd	 4 * SIZE(B), %xmm9
	movapd	 6 * SIZE(B), %xmm13

	subpd	%xmm0,  %xmm1
	subpd	%xmm8,  %xmm5
	subpd	%xmm4,  %xmm9
	subpd	%xmm12, %xmm13
#else
	movapd	 0 * SIZE(AO), %xmm8
	movapd	 2 * SIZE(AO), %xmm9
	movapd	 4 * SIZE(AO), %xmm10
	movapd	 6 * SIZE(AO), %xmm11

	subpd	%xmm0, %xmm8
	subpd	%xmm4, %xmm9
	subpd	%xmm1, %xmm10
	subpd	%xmm5, %xmm11
#endif

#ifdef LN
	movlpd	15 * SIZE(AO), %xmm0
	movhpd	15 * SIZE(AO), %xmm0
	mulpd	 %xmm0, %xmm13
	movlpd	14 * SIZE(AO), %xmm2
	movhpd	14 * SIZE(AO), %xmm2
	mulpd	 %xmm13, %xmm2
	subpd	 %xmm2, %xmm9
	movlpd	13 * SIZE(AO), %xmm4
	movhpd	13 * SIZE(AO), %xmm4
	mulpd	 %xmm13, %xmm4
	subpd	 %xmm4, %xmm5
	movlpd	12 * SIZE(AO), %xmm6
	movhpd	12 * SIZE(AO), %xmm6
	mulpd	 %xmm13, %xmm6
	subpd	 %xmm6, %xmm1

	movlpd	10 * SIZE(AO), %xmm0
	movhpd	10 * SIZE(AO), %xmm0
	mulpd	 %xmm0, %xmm9
	movlpd	 9 * SIZE(AO), %xmm2
	movhpd	 9 * SIZE(AO), %xmm2
	mulpd	 %xmm9, %xmm2
	subpd	 %xmm2, %xmm5
	movlpd	 8 * SIZE(AO), %xmm4
	movhpd	 8 * SIZE(AO), %xmm4
	mulpd	 %xmm9, %xmm4
	subpd	 %xmm4, %xmm1

	movlpd	 5 * SIZE(AO), %xmm0
	movhpd	 5 * SIZE(AO), %xmm0
	mulpd	 %xmm0, %xmm5
	movlpd	 4 * SIZE(AO), %xmm2
	movhpd	 4 * SIZE(AO), %xmm2
	mulpd	 %xmm5, %xmm2
	subpd	 %xmm2, %xmm1

	movlpd	 0 * SIZE(AO), %xmm0
	movhpd	 0 * SIZE(AO), %xmm0
	mulpd	 %xmm0, %xmm1
#endif

#ifdef LT
	movlpd	 0 * SIZE(AO), %xmm0
	movhpd	 0 * SIZE(AO), %xmm0
	mulpd	 %xmm0, %xmm1
	movlpd	 1 * SIZE(AO), %xmm2
	movhpd	 1 * SIZE(AO), %xmm2
	mulpd	 %xmm1, %xmm2
	subpd	 %xmm2, %xmm5
	movlpd	 2 * SIZE(AO), %xmm4
	movhpd	 2 * SIZE(AO), %xmm4
	mulpd	 %xmm1, %xmm4
	subpd	 %xmm4, %xmm9
	movlpd	 3 * SIZE(AO), %xmm6
	movhpd	 3 * SIZE(AO), %xmm6
	mulpd	 %xmm1, %xmm6
	subpd	 %xmm6, %xmm13


	movlpd	 5 * SIZE(AO), %xmm0
	movhpd	 5 * SIZE(AO), %xmm0
	mulpd	 %xmm0, %xmm5

	movlpd	 6 * SIZE(AO), %xmm2
	movhpd	 6 * SIZE(AO), %xmm2
	mulpd	 %xmm5, %xmm2
	subpd	 %xmm2, %xmm9
	movlpd	 7 * SIZE(AO), %xmm4
	movhpd	 7 * SIZE(AO), %xmm4
	mulpd	 %xmm5, %xmm4
	subpd	 %xmm4, %xmm13

	movlpd	10 * SIZE(AO), %xmm0
	movhpd	10 * SIZE(AO), %xmm0
	mulpd	 %xmm0, %xmm9
	movlpd	11 * SIZE(AO), %xmm2
	movhpd	11 * SIZE(AO), %xmm2
	mulpd	 %xmm9, %xmm2
	subpd	 %xmm2, %xmm13

	movlpd	15 * SIZE(AO), %xmm0
	movhpd	15 * SIZE(AO), %xmm0
	mulpd	 %xmm0, %xmm13
#endif

#ifdef RN
	movlpd	 0 * SIZE(B), %xmm0
	movhpd	 0 * SIZE(B), %xmm0
	mulpd	 %xmm0, %xmm8
	mulpd	 %xmm0, %xmm9

	movlpd	 1 * SIZE(B), %xmm1
	movhpd	 1 * SIZE(B), %xmm1
	mulpd	 %xmm8, %xmm1
	subpd	 %xmm1, %xmm10
	movlpd	 1 * SIZE(B), %xmm1
	movhpd	 1 * SIZE(B), %xmm1
	mulpd	 %xmm9, %xmm1
	subpd	 %xmm1, %xmm11

	movlpd	 3 * SIZE(B), %xmm0
	movhpd	 3 * SIZE(B), %xmm0
	mulpd	 %xmm0, %xmm10
	mulpd	 %xmm0, %xmm11
#endif

#ifdef RT
	movlpd	 3 * SIZE(B), %xmm0
	movhpd	 3 * SIZE(B), %xmm0
	mulpd	 %xmm0, %xmm10
	mulpd	 %xmm0, %xmm11

	movlpd	 2 * SIZE(B), %xmm1
	movhpd	 2 * SIZE(B), %xmm1
	mulpd	 %xmm10, %xmm1
	subpd	 %xmm1, %xmm8
	movlpd	 2 * SIZE(B), %xmm1
	movhpd	 2 * SIZE(B), %xmm1
	mulpd	 %xmm11, %xmm1
	subpd	 %xmm1, %xmm9

	movlpd	 0 * SIZE(B), %xmm0
	movhpd	 0 * SIZE(B), %xmm0
	mulpd	 %xmm0, %xmm8
	mulpd	 %xmm0, %xmm9
#endif

#ifdef LN
	subq	$4 * SIZE, CO1
	subq	$4 * SIZE, CO2
#endif

#if defined(LN) || defined(LT)
	movsd	%xmm1,  0 * SIZE(CO1)
	movsd	%xmm5,  1 * SIZE(CO1)
	movsd	%xmm9,  2 * SIZE(CO1)
	movsd	%xmm13, 3 * SIZE(CO1)

	movhpd	%xmm1,  0 * SIZE(CO2)
	movhpd	%xmm5,  1 * SIZE(CO2)
	movhpd	%xmm9,  2 * SIZE(CO2)
	movhpd	%xmm13, 3 * SIZE(CO2)
#else
	movsd	%xmm8,  0 * SIZE(CO1)
	movhpd	%xmm8,  1 * SIZE(CO1)
	movsd	%xmm9,  2 * SIZE(CO1)
	movhpd	%xmm9,  3 * SIZE(CO1)

	movsd	%xmm10,  0 * SIZE(CO2)
	movhpd	%xmm10,  1 * SIZE(CO2)
	movsd	%xmm11,  2 * SIZE(CO2)
	movhpd	%xmm11,  3 * SIZE(CO2)
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm1,   0 * SIZE(B)
	movapd	%xmm5,   2 * SIZE(B)
	movapd	%xmm9,   4 * SIZE(B)
	movapd	%xmm13,  6 * SIZE(B)

	movlpd	%xmm1,   0 * SIZE(BO)
	movlpd	%xmm1,   1 * SIZE(BO)
	movhpd	%xmm1,   2 * SIZE(BO)
	movhpd	%xmm1,   3 * SIZE(BO)
	movlpd	%xmm5,   4 * SIZE(BO)
	movlpd	%xmm5,   5 * SIZE(BO)
	movhpd	%xmm5,   6 * SIZE(BO)
	movhpd	%xmm5,   7 * SIZE(BO)
	movlpd	%xmm9,   8 * SIZE(BO)
	movlpd	%xmm9,   9 * SIZE(BO)
	movhpd	%xmm9,  10 * SIZE(BO)
	movhpd	%xmm9,  11 * SIZE(BO)
	movlpd	%xmm13, 12 * SIZE(BO)
	movlpd	%xmm13, 13 * SIZE(BO)
	movhpd	%xmm13, 14 * SIZE(BO)
	movhpd	%xmm13, 15 * SIZE(BO)
#else
	movapd	%xmm8,   0 * SIZE(AO)
	movapd	%xmm9,   2 * SIZE(AO)
	movapd	%xmm10,  4 * SIZE(AO)
	movapd	%xmm11,  6 * SIZE(AO)
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

	leaq	BUFFER, BO

#if defined(LN) || defined(RT)
	movq	KK, %rax
	salq	$1 + BASE_SHIFT, %rax
	leaq	(BO, %rax, 2), BO
#endif

	movapd	 0 * SIZE(AO), %xmm8
	pxor	%xmm0, %xmm0
	movapd	 0 * SIZE(BO), %xmm9
	pxor	%xmm1, %xmm1
	movapd	 8 * SIZE(AO), %xmm10
	pxor	%xmm2, %xmm2
	movapd	 8 * SIZE(BO), %xmm11
	pxor	%xmm3, %xmm3

	movapd	16 * SIZE(BO), %xmm13
	movapd	24 * SIZE(BO), %xmm15

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	sarq	$3, %rax
	je	.L65
	ALIGN_4

.L62:
	mulpd	%xmm8, %xmm9
	PREFETCH	(PREFETCHSIZE     +  0) * SIZE(AO)
	mulpd	 2 * SIZE(BO), %xmm8
	addpd	%xmm9, %xmm0
	movapd	 4 * SIZE(BO), %xmm9
	addpd	%xmm8, %xmm1
	movapd	 2 * SIZE(AO), %xmm8

	mulpd	%xmm8, %xmm9
	mulpd	 6 * SIZE(BO), %xmm8
	addpd	%xmm9, %xmm2
	movapd	32 * SIZE(BO), %xmm9
	addpd	%xmm8, %xmm3
	movapd	 4 * SIZE(AO), %xmm8

	mulpd	%xmm8, %xmm11
	mulpd	10 * SIZE(BO), %xmm8
	addpd	%xmm11, %xmm0
	movapd	12 * SIZE(BO), %xmm11
	addpd	%xmm8, %xmm1
	movapd	 6 * SIZE(AO), %xmm8

	mulpd	%xmm8, %xmm11
	mulpd	14 * SIZE(BO), %xmm8
	addpd	%xmm11, %xmm2
	movapd	40 * SIZE(BO), %xmm11
	addpd	%xmm8, %xmm3
	movapd	16 * SIZE(AO), %xmm8

	PREFETCH	(PREFETCHSIZE     +  8) * SIZE(AO)
	mulpd	%xmm10, %xmm13
	mulpd	18 * SIZE(BO), %xmm10
	addpd	%xmm13, %xmm0
	movapd	20 * SIZE(BO), %xmm13
	addpd	%xmm10, %xmm1
	movapd	10 * SIZE(AO), %xmm10

	mulpd	%xmm10, %xmm13
	mulpd	22 * SIZE(BO), %xmm10
	addpd	%xmm13, %xmm2
	movapd	48 * SIZE(BO), %xmm13
	addpd	%xmm10, %xmm3
	movapd	12 * SIZE(AO), %xmm10

	mulpd	%xmm10, %xmm15
	mulpd	26 * SIZE(BO), %xmm10
	addpd	%xmm15, %xmm0
	movapd	28 * SIZE(BO), %xmm15
	addpd	%xmm10, %xmm1
	movapd	14 * SIZE(AO), %xmm10

	mulpd	%xmm10, %xmm15
	mulpd	30 * SIZE(BO), %xmm10
	addpd	%xmm15, %xmm2
	movapd	56 * SIZE(BO), %xmm15
	addpd	%xmm10, %xmm3
	movapd	24 * SIZE(AO), %xmm10

	addq   $16 * SIZE, AO
	addq   $32 * SIZE, BO
	decq   %rax
	jne    .L62
	ALIGN_4

.L65:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L69
	ALIGN_4

.L66:
	mulpd	%xmm8, %xmm9
	mulpd	 2 * SIZE(BO), %xmm8
	addpd	%xmm9, %xmm0
	movapd	 4 * SIZE(BO), %xmm9
	addpd	%xmm8, %xmm1
	movapd	 2 * SIZE(AO), %xmm8

	addq	$2 * SIZE, AO		# aoffset  += 4
	addq	$4 * SIZE, BO		# boffset1 += 8
	decq	%rax
	jg	.L66
	ALIGN_4

.L69:
	addpd	%xmm2, %xmm0
	addpd	%xmm3, %xmm1

#if defined(LN) || defined(RT)
	movq	KK, %rax
#ifdef LN
	subq	$2, %rax
#else
	subq	$2, %rax
#endif

	movq	AORIG, AO
	movq	BORIG, B
	leaq	BUFFER, BO

	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(B,  %rax, 2), B
	leaq	(BO, %rax, 4), BO
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm0, %xmm8
	unpcklpd %xmm1, %xmm0
	unpckhpd %xmm1, %xmm8

	movapd	 0 * SIZE(B), %xmm1
	movapd	 2 * SIZE(B), %xmm5

	subpd	%xmm0,  %xmm1
	subpd	%xmm8,  %xmm5
#else
	movapd	 0 * SIZE(AO), %xmm8
	movapd	 2 * SIZE(AO), %xmm10

	subpd	%xmm0, %xmm8
	subpd	%xmm1, %xmm10
#endif


#ifdef LN
	movlpd	 3 * SIZE(AO), %xmm0
	movhpd	 3 * SIZE(AO), %xmm0
	mulpd	 %xmm0, %xmm5

	movlpd	 2 * SIZE(AO), %xmm2
	movhpd	 2 * SIZE(AO), %xmm2
	mulpd	 %xmm5, %xmm2
	subpd	 %xmm2, %xmm1

	movlpd	 0 * SIZE(AO), %xmm0
	movhpd	 0 * SIZE(AO), %xmm0
	mulpd	 %xmm0, %xmm1
#endif

#ifdef LT
	movlpd	 0 * SIZE(AO), %xmm0
	movhpd	 0 * SIZE(AO), %xmm0
	mulpd	 %xmm0, %xmm1

	movlpd	 1 * SIZE(AO), %xmm2
	movhpd	 1 * SIZE(AO), %xmm2
	mulpd	 %xmm1, %xmm2
	subpd	 %xmm2, %xmm5

	movlpd	 3 * SIZE(AO), %xmm0
	movhpd	 3 * SIZE(AO), %xmm0
	mulpd	 %xmm0, %xmm5
#endif

#ifdef RN
	movlpd	 0 * SIZE(B), %xmm0
	movhpd	 0 * SIZE(B), %xmm0
	mulpd	 %xmm0, %xmm8

	movlpd	 1 * SIZE(B), %xmm1
	movhpd	 1 * SIZE(B), %xmm1
	mulpd	 %xmm8, %xmm1
	subpd	 %xmm1, %xmm10

	movlpd	 3 * SIZE(B), %xmm0
	movhpd	 3 * SIZE(B), %xmm0
	mulpd	 %xmm0, %xmm10
#endif

#ifdef RT
	movlpd	 3 * SIZE(B), %xmm0
	movhpd	 3 * SIZE(B), %xmm0
	mulpd	 %xmm0, %xmm10

	movlpd	 2 * SIZE(B), %xmm1
	movhpd	 2 * SIZE(B), %xmm1
	mulpd	 %xmm10, %xmm1
	subpd	 %xmm1, %xmm8

	movlpd	 0 * SIZE(B), %xmm0
	movhpd	 0 * SIZE(B), %xmm0
	mulpd	 %xmm0, %xmm8
#endif

#ifdef LN
	subq	$2 * SIZE, CO1
	subq	$2 * SIZE, CO2
#endif

#if defined(LN) || defined(LT)
	movsd	%xmm1,  0 * SIZE(CO1)
	movsd	%xmm5,  1 * SIZE(CO1)

	movhpd	%xmm1,  0 * SIZE(CO2)
	movhpd	%xmm5,  1 * SIZE(CO2)
#else
	movsd	%xmm8,  0 * SIZE(CO1)
	movhpd	%xmm8,  1 * SIZE(CO1)

	movsd	%xmm10,  0 * SIZE(CO2)
	movhpd	%xmm10,  1 * SIZE(CO2)
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm1,   0 * SIZE(B)
	movapd	%xmm5,   2 * SIZE(B)

	movlpd	%xmm1,   0 * SIZE(BO)
	movlpd	%xmm1,   1 * SIZE(BO)
	movhpd	%xmm1,   2 * SIZE(BO)
	movhpd	%xmm1,   3 * SIZE(BO)
	movlpd	%xmm5,   4 * SIZE(BO)
	movlpd	%xmm5,   5 * SIZE(BO)
	movhpd	%xmm5,   6 * SIZE(BO)
	movhpd	%xmm5,   7 * SIZE(BO)
#else
	movapd	%xmm8,   0 * SIZE(AO)
	movapd	%xmm10,  2 * SIZE(AO)
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
	testq	$1, M
	je	.L79
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

	leaq	BUFFER, BO

#if defined(LN) || defined(RT)
	movq	KK, %rax
	salq	$1 + BASE_SHIFT, %rax
	leaq	(BO, %rax, 2), BO
#endif

	movsd	 0 * SIZE(AO), %xmm8
	pxor	%xmm0, %xmm0
	movsd	 0 * SIZE(BO), %xmm9
	pxor	%xmm1, %xmm1
	movsd	 4 * SIZE(AO), %xmm10
	pxor	%xmm2, %xmm2
	movsd	 8 * SIZE(BO), %xmm11
	pxor	%xmm3, %xmm3

	movsd	16 * SIZE(BO), %xmm13
	movsd	24 * SIZE(BO), %xmm15

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	sarq	$3, %rax
	je	.L75
	ALIGN_4

.L72:
	mulsd	%xmm8, %xmm9
	PREFETCH	(PREFETCHSIZE     +  0) * SIZE(AO)
	mulsd	 2 * SIZE(BO), %xmm8
	addsd	%xmm9, %xmm0
	movsd	 4 * SIZE(BO), %xmm9
	addsd	%xmm8, %xmm1
	movsd	 1 * SIZE(AO), %xmm8

	mulsd	%xmm8, %xmm9
	mulsd	 6 * SIZE(BO), %xmm8
	addsd	%xmm9, %xmm2
	movsd	32 * SIZE(BO), %xmm9
	addsd	%xmm8, %xmm3
	movsd	 2 * SIZE(AO), %xmm8

	mulsd	%xmm8, %xmm11
	mulsd	10 * SIZE(BO), %xmm8
	addsd	%xmm11, %xmm0
	movsd	12 * SIZE(BO), %xmm11
	addsd	%xmm8, %xmm1
	movsd	 3 * SIZE(AO), %xmm8

	mulsd	%xmm8, %xmm11
	mulsd	14 * SIZE(BO), %xmm8
	addsd	%xmm11, %xmm2
	movsd	40 * SIZE(BO), %xmm11
	addsd	%xmm8, %xmm3
	movsd	 8 * SIZE(AO), %xmm8

	mulsd	%xmm10, %xmm13
	mulsd	18 * SIZE(BO), %xmm10
	addsd	%xmm13, %xmm0
	movsd	20 * SIZE(BO), %xmm13
	addsd	%xmm10, %xmm1
	movsd	 5 * SIZE(AO), %xmm10

	mulsd	%xmm10, %xmm13
	mulsd	22 * SIZE(BO), %xmm10
	addsd	%xmm13, %xmm2
	movsd	48 * SIZE(BO), %xmm13
	addsd	%xmm10, %xmm3
	movsd	 6 * SIZE(AO), %xmm10

	mulsd	%xmm10, %xmm15
	mulsd	26 * SIZE(BO), %xmm10
	addsd	%xmm15, %xmm0
	movsd	28 * SIZE(BO), %xmm15
	addsd	%xmm10, %xmm1
	movsd	 7 * SIZE(AO), %xmm10

	mulsd	%xmm10, %xmm15
	mulsd	30 * SIZE(BO), %xmm10
	addsd	%xmm15, %xmm2
	movsd	56 * SIZE(BO), %xmm15
	addsd	%xmm10, %xmm3
	movsd	12 * SIZE(AO), %xmm10

	addq   $ 8 * SIZE, AO
	addq   $32 * SIZE, BO
	decq   %rax
	jne    .L72
	ALIGN_4

.L75:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L78
	ALIGN_4

.L76:
	mulsd	%xmm8, %xmm9
	mulsd	 2 * SIZE(BO), %xmm8
	addsd	%xmm9, %xmm0
	addsd	%xmm8, %xmm1
	movsd	 1 * SIZE(AO), %xmm8
	movsd	 4 * SIZE(BO), %xmm9

	addq	$1 * SIZE, AO		# aoffset  += 4
	addq	$4 * SIZE, BO		# boffset1 += 8
	decq	%rax
	jg	.L76
	ALIGN_4

.L78:
	addsd	%xmm2, %xmm0
	addsd	%xmm3, %xmm1

#if defined(LN) || defined(RT)
	movq	KK, %rax
#ifdef LN
	subq	$1, %rax
#else
	subq	$2, %rax
#endif

	movq	AORIG, AO
	movq	BORIG, B
	leaq	BUFFER, BO

	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(B,  %rax, 2), B
	leaq	(BO, %rax, 4), BO
#endif

#if defined(LN) || defined(LT)
	movsd	 0 * SIZE(B), %xmm4
	movsd	 1 * SIZE(B), %xmm5
#else
	movsd	 0 * SIZE(AO), %xmm4
	movsd	 1 * SIZE(AO), %xmm5
#endif

	subsd	%xmm0,  %xmm4
	subsd	%xmm1,  %xmm5

#ifdef LN
	movsd	 0 * SIZE(AO), %xmm0

	mulsd	 %xmm0, %xmm4
	mulsd	 %xmm0, %xmm5
#endif

#ifdef LT
	movsd	 0 * SIZE(AO), %xmm0

	mulsd	 %xmm0, %xmm4
	mulsd	 %xmm0, %xmm5
#endif

#ifdef RN
	mulsd	 0 * SIZE(B), %xmm4
	movsd	 1 * SIZE(B), %xmm1
	mulsd	 %xmm4, %xmm1
	subsd	 %xmm1, %xmm5

	mulsd	 3 * SIZE(B), %xmm5
#endif

#ifdef RT
	mulsd	 3 * SIZE(B), %xmm5

	movlpd	 2 * SIZE(B), %xmm1
	mulsd	 %xmm5, %xmm1
	subsd	 %xmm1, %xmm4

	mulsd	 0 * SIZE(B), %xmm4
#endif

#ifdef LN
	subq	$1 * SIZE, CO1
	subq	$1 * SIZE, CO2
#endif

	movsd	%xmm4,  0 * SIZE(CO1)
	movsd	%xmm5,  0 * SIZE(CO2)

#if defined(LN) || defined(LT)
	movsd	%xmm4,   0 * SIZE(B)
	movsd	%xmm5,   1 * SIZE(B)

	movsd	%xmm4,   0 * SIZE(BO)
	movsd	%xmm4,   1 * SIZE(BO)
	movsd	%xmm5,   2 * SIZE(BO)
	movsd	%xmm5,   3 * SIZE(BO)
#else
	movsd	%xmm4,   0 * SIZE(AO)
	movsd	%xmm5,   1 * SIZE(AO)
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
	PREFETCH	 56 * SIZE(B)

	movsd	 0 * SIZE(B), %xmm0
	movsd	 1 * SIZE(B), %xmm1
	movsd	 2 * SIZE(B), %xmm2
	movsd	 3 * SIZE(B), %xmm3
	movsd	 4 * SIZE(B), %xmm4
	movsd	 5 * SIZE(B), %xmm5
	movsd	 6 * SIZE(B), %xmm6
	movsd	 7 * SIZE(B), %xmm7

	addq	$ 8 * SIZE, B
	addq	$16 * SIZE, BO

	movsd	%xmm0, -16 * SIZE(BO)
	movsd	%xmm0, -15 * SIZE(BO)
	movsd	%xmm1, -14 * SIZE(BO)
	movsd	%xmm1, -13 * SIZE(BO)
	movsd	%xmm2, -12 * SIZE(BO)
	movsd	%xmm2, -11 * SIZE(BO)
	movsd	%xmm3, -10 * SIZE(BO)
	movsd	%xmm3,  -9 * SIZE(BO)
	movsd	%xmm4,  -8 * SIZE(BO)
	movsd	%xmm4,  -7 * SIZE(BO)
	movsd	%xmm5,  -6 * SIZE(BO)
	movsd	%xmm5,  -5 * SIZE(BO)
	movsd	%xmm6,  -4 * SIZE(BO)
	movsd	%xmm6,  -3 * SIZE(BO)
	movsd	%xmm7,  -2 * SIZE(BO)
	movsd	%xmm7,  -1 * SIZE(BO)

	decq	%rax
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
	movsd	 0 * SIZE(B), %xmm0

	movsd	%xmm0,  0 * SIZE(BO)
	movsd	%xmm0,  1 * SIZE(BO)

	addq	$1 * SIZE, B
	addq	$2 * SIZE, BO
	decq	%rax
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

	movq	M,  I
	sarq	$2, I	# i = (m >> 2)
	jle	.L100
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

	leaq	BUFFER, BO

#if defined(LN) || defined(RT)
	movq	KK, %rax
	salq	$0 + BASE_SHIFT, %rax
	leaq	(BO, %rax, 2), BO
#endif

	movapd	 0 * SIZE(AO), %xmm8
	pxor	%xmm0, %xmm0
	movapd	 0 * SIZE(BO), %xmm9
	pxor	%xmm1, %xmm1
	movapd	 8 * SIZE(AO), %xmm10
	pxor	%xmm2, %xmm2
	movapd	 8 * SIZE(BO), %xmm11
	pxor	%xmm3, %xmm3

	movapd	16 * SIZE(AO), %xmm12
	movapd	24 * SIZE(AO), %xmm14

	PREFETCHW      4 * SIZE(CO1)

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	sarq	$3, %rax
	je	.L95
	ALIGN_4

.L92:
	mulpd	%xmm9, %xmm8
	PREFETCH	(PREFETCHSIZE     +  0) * SIZE(AO)
	mulpd	 2 * SIZE(AO), %xmm9
	addpd	%xmm8, %xmm0
	movapd	 4 * SIZE(AO), %xmm8
	addpd	%xmm9, %xmm1
	movapd	 2 * SIZE(BO), %xmm9
	mulpd	%xmm9, %xmm8
	mulpd	 6 * SIZE(AO), %xmm9
	addpd	%xmm8, %xmm2
	movapd	32 * SIZE(AO), %xmm8
	PREFETCH	(PREFETCHSIZE     +  8) * SIZE(AO)
	addpd	%xmm9, %xmm3
	movapd	 4 * SIZE(BO), %xmm9
	mulpd	%xmm9, %xmm10
	mulpd	10 * SIZE(AO), %xmm9
	addpd	%xmm10, %xmm0
	movapd	12 * SIZE(AO), %xmm10
	addpd	%xmm9, %xmm1
	movapd	 6 * SIZE(BO), %xmm9
	mulpd	%xmm9, %xmm10
	mulpd	14 * SIZE(AO), %xmm9
	addpd	%xmm10, %xmm2
	movapd	40 * SIZE(AO), %xmm10
	PREFETCH	(PREFETCHSIZE     + 16) * SIZE(AO)
	addpd	%xmm9, %xmm3
	movapd	16 * SIZE(BO), %xmm9
	mulpd	%xmm11, %xmm12
	mulpd	18 * SIZE(AO), %xmm11
	addpd	%xmm12, %xmm0
	movapd	20 * SIZE(AO), %xmm12
	addpd	%xmm11, %xmm1
	movapd	10 * SIZE(BO), %xmm11
	mulpd	%xmm11, %xmm12
	mulpd	22 * SIZE(AO), %xmm11
	addpd	%xmm12, %xmm2
	movapd	48 * SIZE(AO), %xmm12
	PREFETCH	(PREFETCHSIZE     + 24) * SIZE(AO)
	addpd	%xmm11, %xmm3
	movapd	12 * SIZE(BO), %xmm11
	mulpd	%xmm11, %xmm14
	mulpd	26 * SIZE(AO), %xmm11
	addpd	%xmm14, %xmm0
	movapd	28 * SIZE(AO), %xmm14
	addpd	%xmm11, %xmm1
	movapd	14 * SIZE(BO), %xmm11
	mulpd	%xmm11, %xmm14
	mulpd	30 * SIZE(AO), %xmm11
	addpd	%xmm14, %xmm2
	movapd	56 * SIZE(AO), %xmm14
	addpd	%xmm11, %xmm3
	movapd	24 * SIZE(BO), %xmm11

	addq   $32 * SIZE, AO
	addq   $16 * SIZE, BO
	decq   %rax
	jne    .L92
	ALIGN_4

.L95:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L99
	ALIGN_4

.L96:
	mulpd	%xmm9, %xmm8
	mulpd	 2 * SIZE(AO), %xmm9
	addpd	%xmm8, %xmm0
	movapd	 4 * SIZE(AO), %xmm8
	addpd	%xmm9, %xmm1
	movapd	 2 * SIZE(BO), %xmm9

	addq	$4 * SIZE, AO		# aoffset  += 4
	addq	$2 * SIZE, BO		# boffset1 += 8
	decq	%rax
	jg	.L96
	ALIGN_4

.L99:
	addpd	%xmm2, %xmm0
	addpd	%xmm3, %xmm1

#if defined(LN) || defined(RT)
	movq	KK, %rax
#ifdef LN
	subq	$4, %rax
#else
	subq	$1, %rax
#endif

	movq	AORIG, AO
	movq	BORIG, B
	leaq	BUFFER, BO

	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(B,  %rax, 1), B
	leaq	(BO, %rax, 2), BO
#endif

#if defined(LN) || defined(LT)
	movapd	 0 * SIZE(B), %xmm2
	movapd	 2 * SIZE(B), %xmm3

	subpd	%xmm0,  %xmm2
	subpd	%xmm1,  %xmm3
#else
	movapd	 0 * SIZE(AO), %xmm2
	movapd	 2 * SIZE(AO), %xmm3

	subpd	%xmm0, %xmm2
	subpd	%xmm1, %xmm3
#endif

#ifdef LN
	movapd	%xmm2, %xmm0
        unpckhpd %xmm0, %xmm0

	movapd	%xmm3, %xmm1
        unpckhpd %xmm1, %xmm1

	movsd	15 * SIZE(AO), %xmm4
	mulsd	 %xmm4, %xmm1

	movsd	14 * SIZE(AO), %xmm5
	mulsd	 %xmm1, %xmm5
	subsd	 %xmm5, %xmm3
	movsd	13 * SIZE(AO), %xmm6
	mulsd	 %xmm1, %xmm6
	subsd	 %xmm6, %xmm0
	movsd	12 * SIZE(AO), %xmm7
	mulsd	 %xmm1, %xmm7
	subsd	 %xmm7, %xmm2

	movsd	10 * SIZE(AO), %xmm4
	mulsd	 %xmm4, %xmm3

	movsd	 9 * SIZE(AO), %xmm5
	mulsd	 %xmm3, %xmm5
	subsd	 %xmm5, %xmm0
	movsd	 8 * SIZE(AO), %xmm6
	mulsd	 %xmm3, %xmm6
	subsd	 %xmm6, %xmm2

	movsd	 5 * SIZE(AO), %xmm4
	mulsd	 %xmm4, %xmm0

	movsd	 4 * SIZE(AO), %xmm5
	mulsd	 %xmm0, %xmm5
	subsd	 %xmm5, %xmm2

	movsd	 0 * SIZE(AO), %xmm4
	mulsd	 %xmm4, %xmm2

	unpcklpd %xmm0, %xmm2
	unpcklpd %xmm1, %xmm3
#endif

#ifdef LT
	movapd	%xmm2, %xmm0
        unpckhpd %xmm0, %xmm0

	movapd	%xmm3, %xmm1
        unpckhpd %xmm1, %xmm1

	movsd	 0 * SIZE(AO), %xmm4
	mulsd	 %xmm4, %xmm2

	movsd	 1 * SIZE(AO), %xmm5
	mulsd	 %xmm2, %xmm5
	subsd	 %xmm5, %xmm0
	movsd	 2 * SIZE(AO), %xmm6
	mulsd	 %xmm2, %xmm6
	subsd	 %xmm6, %xmm3
	movsd	 3 * SIZE(AO), %xmm7
	mulsd	 %xmm2, %xmm7
	subsd	 %xmm7, %xmm1

	movsd	 5 * SIZE(AO), %xmm4
	mulsd	 %xmm4, %xmm0

	movsd	 6 * SIZE(AO), %xmm5
	mulsd	 %xmm0, %xmm5
	subsd	 %xmm5, %xmm3
	movsd	 7 * SIZE(AO), %xmm6
	mulsd	 %xmm0, %xmm6
	subsd	 %xmm6, %xmm1

	movsd	10 * SIZE(AO), %xmm4
	mulsd	 %xmm4, %xmm3

	movsd	11 * SIZE(AO), %xmm5
	mulsd	 %xmm3, %xmm5
	subsd	 %xmm5, %xmm1

	movsd	15 * SIZE(AO), %xmm4
	mulsd	 %xmm4, %xmm1

	unpcklpd %xmm0, %xmm2
	unpcklpd %xmm1, %xmm3
#endif

#ifdef RN
	movlpd	 0 * SIZE(B), %xmm0
	movhpd	 0 * SIZE(B), %xmm0
	mulpd	 %xmm0, %xmm2
	mulpd	 %xmm0, %xmm3
#endif

#ifdef RT
	movlpd	 0 * SIZE(B), %xmm0
	movhpd	 0 * SIZE(B), %xmm0
	mulpd	 %xmm0, %xmm2
	mulpd	 %xmm0, %xmm3
#endif

#ifdef LN
	subq	$4 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	movsd	%xmm2,  0 * SIZE(CO1)
	movhpd	%xmm2,  1 * SIZE(CO1)
	movsd	%xmm3,  2 * SIZE(CO1)
	movhpd	%xmm3,  3 * SIZE(CO1)
#else
	movsd	%xmm2,  0 * SIZE(CO1)
	movhpd	%xmm2,  1 * SIZE(CO1)
	movsd	%xmm3,  2 * SIZE(CO1)
	movhpd	%xmm3,  3 * SIZE(CO1)
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm2,   0 * SIZE(B)
	movapd	%xmm3,   2 * SIZE(B)

	movlpd	%xmm2,   0 * SIZE(BO)
	movlpd	%xmm2,   1 * SIZE(BO)
	movhpd	%xmm2,   2 * SIZE(BO)
	movhpd	%xmm2,   3 * SIZE(BO)
	movlpd	%xmm3,   4 * SIZE(BO)
	movlpd	%xmm3,   5 * SIZE(BO)
	movhpd	%xmm3,   6 * SIZE(BO)
	movhpd	%xmm3,   7 * SIZE(BO)
#else
	movapd	%xmm2,   0 * SIZE(AO)
	movapd	%xmm3,   2 * SIZE(AO)
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

	leaq	BUFFER, BO

#if defined(LN) || defined(RT)
	movq	KK, %rax
	salq	$0 + BASE_SHIFT, %rax
	leaq	(BO, %rax, 2), BO
#endif

	movapd	 0 * SIZE(AO), %xmm8
	pxor	%xmm0, %xmm0
	movapd	 0 * SIZE(BO), %xmm9
	pxor	%xmm1, %xmm1
	movapd	 8 * SIZE(AO), %xmm10
	pxor	%xmm2, %xmm2
	movapd	 8 * SIZE(BO), %xmm11
	pxor	%xmm3, %xmm3

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	sarq	$3, %rax
	je	.L105
	ALIGN_4

.L102:
	mulpd	%xmm8, %xmm9
	PREFETCH	(PREFETCHSIZE     +  0) * SIZE(AO)
	movapd	 2 * SIZE(AO), %xmm8
	mulpd	 2 * SIZE(BO), %xmm8
	addpd	%xmm9, %xmm0
	movapd	16 * SIZE(BO), %xmm9
	addpd	%xmm8, %xmm1
	movapd	 4 * SIZE(AO), %xmm8
	mulpd	 4 * SIZE(BO), %xmm8
	addpd	%xmm8, %xmm2
	movapd	 6 * SIZE(AO), %xmm8
	mulpd	 6 * SIZE(BO), %xmm8
	addpd	%xmm8, %xmm3
	movapd	16 * SIZE(AO), %xmm8
	PREFETCH	(PREFETCHSIZE     +  0) * SIZE(AO)
	mulpd	%xmm10, %xmm11
	movapd	10 * SIZE(AO), %xmm10
	mulpd	10 * SIZE(BO), %xmm10
	addpd	%xmm11, %xmm0
	movapd	24 * SIZE(BO), %xmm11
	addpd	%xmm10, %xmm1
	movapd	12 * SIZE(AO), %xmm10
	mulpd	12 * SIZE(BO), %xmm10
	addpd	%xmm10, %xmm2
	movapd	14 * SIZE(AO), %xmm10
	mulpd	14 * SIZE(BO), %xmm10
	addpd	%xmm10, %xmm3
	movapd	24 * SIZE(AO), %xmm10

	addq   $16 * SIZE, AO
	addq   $16 * SIZE, BO
	decq   %rax
	jne    .L102
	ALIGN_4

.L105:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L109
	ALIGN_4

.L106:
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm0
	movapd	 2 * SIZE(AO), %xmm8
	movapd	 2 * SIZE(BO), %xmm9

	addq	$2 * SIZE, AO		# aoffset  += 4
	addq	$2 * SIZE, BO		# boffset1 += 8
	decq	%rax
	jg	.L106
	ALIGN_4

.L109:
	addpd	%xmm1, %xmm0
	addpd	%xmm3, %xmm2
	addpd	%xmm2, %xmm0

#if defined(LN) || defined(RT)
	movq	KK, %rax
#ifdef LN
	subq	$2, %rax
#else
	subq	$1, %rax
#endif

	movq	AORIG, AO
	movq	BORIG, B
	leaq	BUFFER, BO

	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(B,  %rax, 1), B
	leaq	(BO, %rax, 2), BO
#endif

#if defined(LN) || defined(LT)
	movapd	 0 * SIZE(B), %xmm2
	subpd	%xmm0,  %xmm2
#else
	movapd	 0 * SIZE(AO), %xmm2
	subpd	%xmm0, %xmm2
#endif

#ifdef LN
	movapd	%xmm2, %xmm0
        unpckhpd %xmm0, %xmm0

	movsd	 3 * SIZE(AO), %xmm4
	mulsd	 %xmm4, %xmm0

	movsd	 2 * SIZE(AO), %xmm5
	mulsd	 %xmm0, %xmm5
	subsd	 %xmm5, %xmm2

	movsd	 0 * SIZE(AO), %xmm4
	mulsd	 %xmm4, %xmm2

	unpcklpd %xmm0, %xmm2
#endif

#ifdef LT
	movapd	%xmm2, %xmm0
        unpckhpd %xmm0, %xmm0

	movsd	 0 * SIZE(AO), %xmm4
	mulsd	 %xmm4, %xmm2

	movsd	 1 * SIZE(AO), %xmm5
	mulsd	 %xmm2, %xmm5
	subsd	 %xmm5, %xmm0

	movsd	 3 * SIZE(AO), %xmm4
	mulsd	 %xmm4, %xmm0

	unpcklpd %xmm0, %xmm2
#endif

#ifdef RN
	movlpd	 0 * SIZE(B), %xmm0
	movhpd	 0 * SIZE(B), %xmm0
	mulpd	 %xmm0, %xmm2
#endif

#ifdef RT
	movlpd	 0 * SIZE(B), %xmm0
	movhpd	 0 * SIZE(B), %xmm0
	mulpd	 %xmm0, %xmm2
#endif

#ifdef LN
	subq	$2 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	movsd	%xmm2,  0 * SIZE(CO1)
	movhpd	%xmm2,  1 * SIZE(CO1)
#else
	movsd	%xmm2,  0 * SIZE(CO1)
	movhpd	%xmm2,  1 * SIZE(CO1)
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm2,   0 * SIZE(B)

	movlpd	%xmm2,   0 * SIZE(BO)
	movlpd	%xmm2,   1 * SIZE(BO)
	movhpd	%xmm2,   2 * SIZE(BO)
	movhpd	%xmm2,   3 * SIZE(BO)
#else
	movapd	%xmm2,   0 * SIZE(AO)
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
	testq	$1, M
	je	.L119
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

	leaq	BUFFER, BO

#if defined(LN) || defined(RT)
	movq	KK, %rax
	salq	$0 + BASE_SHIFT, %rax
	leaq	(BO, %rax, 2), BO
#endif

	movsd	 0 * SIZE(AO), %xmm8
	pxor	%xmm0, %xmm0
	movsd	 0 * SIZE(BO), %xmm9
	pxor	%xmm1, %xmm1
	movsd	 4 * SIZE(AO), %xmm10
	pxor	%xmm2, %xmm2
	movsd	 8 * SIZE(BO), %xmm11
	pxor	%xmm3, %xmm3

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	sarq	$3, %rax
	je	.L115
	ALIGN_4

.L112:
	mulsd	%xmm8, %xmm9
	PREFETCH	(PREFETCHSIZE     +  0) * SIZE(AO)
	movsd	 1 * SIZE(AO), %xmm8
	addsd	%xmm9, %xmm0
	movsd	16 * SIZE(BO), %xmm9
	mulsd	 2 * SIZE(BO), %xmm8
	addsd	%xmm8, %xmm1
	movsd	 2 * SIZE(AO), %xmm8
	mulsd	 4 * SIZE(BO), %xmm8
	addsd	%xmm8, %xmm2
	movsd	 3 * SIZE(AO), %xmm8
	mulsd	 6 * SIZE(BO), %xmm8
	addsd	%xmm8, %xmm3
	movsd	 8 * SIZE(AO), %xmm8
	mulsd	%xmm10, %xmm11
	movsd	 5 * SIZE(AO), %xmm10
	addsd	%xmm11, %xmm0
	movsd	24 * SIZE(BO), %xmm11
	mulsd	10 * SIZE(BO), %xmm10
	addsd	%xmm10, %xmm1
	movsd	 6 * SIZE(AO), %xmm10
	mulsd	12 * SIZE(BO), %xmm10
	addsd	%xmm10, %xmm2
	movsd	 7 * SIZE(AO), %xmm10
	mulsd	14 * SIZE(BO), %xmm10
	addsd	%xmm10, %xmm3
	movsd	12 * SIZE(AO), %xmm10

	addq   $ 8 * SIZE, AO
	addq   $16 * SIZE, BO
	decq   %rax
	jne    .L112
	ALIGN_4

.L115:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L118
	ALIGN_4

.L116:
	mulsd	%xmm8, %xmm9
	movsd	 1 * SIZE(AO), %xmm8
	addsd	%xmm9, %xmm0
	movsd	 2 * SIZE(BO), %xmm9

	addq	$1 * SIZE, AO		# aoffset  += 4
	addq	$2 * SIZE, BO		# boffset1 += 8
	decq	%rax
	jg	.L116
	ALIGN_4

.L118:
	addsd	%xmm2, %xmm0
	addsd	%xmm3, %xmm1
	addsd	%xmm1, %xmm0

#if defined(LN) || defined(RT)
	movq	KK, %rax
#ifdef LN
	subq	$1, %rax
#else
	subq	$1, %rax
#endif

	movq	AORIG, AO
	movq	BORIG, B
	leaq	BUFFER, BO

	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(B,  %rax, 1), B
	leaq	(BO, %rax, 2), BO
#endif

#if defined(LN) || defined(LT)
	movsd	 0 * SIZE(B), %xmm2
	subsd	%xmm0,  %xmm2
#else
	movsd	 0 * SIZE(AO), %xmm2
	subsd	%xmm0, %xmm2
#endif

#ifdef LN
	movsd	 0 * SIZE(AO), %xmm4
	mulsd	 %xmm4, %xmm2
#endif

#ifdef LT
	movsd	 0 * SIZE(AO), %xmm4
	mulsd	 %xmm4, %xmm2
#endif

#ifdef RN
	movsd	 0 * SIZE(B), %xmm0
	mulsd	 %xmm0, %xmm2
#endif

#ifdef RT
	movsd	 0 * SIZE(B), %xmm0
	mulsd	 %xmm0, %xmm2
#endif

#ifdef LN
	subq	$1 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	movsd	%xmm2,  0 * SIZE(CO1)
#else
	movsd	%xmm2,  0 * SIZE(CO1)
#endif

#if defined(LN) || defined(LT)
	movsd	%xmm2,   0 * SIZE(B)

	movlpd	%xmm2,   0 * SIZE(BO)
	movlpd	%xmm2,   1 * SIZE(BO)
#else
	movsd	%xmm2,   0 * SIZE(AO)
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
	movq	%rbx, %rsp

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
