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

#ifdef PENTIUM4
#define PREFETCH     prefetcht0
#define PREFETCHW    prefetcht0
#endif

#if defined(OPTERON) || defined(BARCELONA) || defined(BULLDOZER)
#define PREFETCH     prefetch
#define PREFETCHW    prefetchw
#define movsd movlps
#endif

#ifdef GENERIC
#define PREFETCH     prefetcht0
#define PREFETCHW    prefetcht0
#endif

#ifndef PREFETCH
#define PREFETCH     prefetcht0
#endif

#ifndef PREFETCHW
#define PREFETCHW    prefetcht0
#endif

	PROLOGUE
	PROFCODE

	subq	$STACKSIZE, %rsp

	EMMS

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
	jle	.L50

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
	salq	$2 + BASE_SHIFT, %rax
	leaq	(B,  %rax, 1), B
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
	jle	.L03
	ALIGN_4

.L02:
	movaps	 0 * SIZE(B), %xmm3
	movaps	 4 * SIZE(B), %xmm7
	movaps	 8 * SIZE(B), %xmm11
	movaps	12 * SIZE(B), %xmm15

	pshufd	 $0x00, %xmm3, %xmm0
	pshufd	 $0x55, %xmm3, %xmm1
	pshufd	 $0xaa, %xmm3, %xmm2
	pshufd	 $0xff, %xmm3, %xmm3

	pshufd	 $0x00, %xmm7, %xmm4
	pshufd	 $0x55, %xmm7, %xmm5
	pshufd	 $0xaa, %xmm7, %xmm6
	pshufd	 $0xff, %xmm7, %xmm7

	movaps	%xmm0,  0 * SIZE(BO)
	movaps	%xmm1,  4 * SIZE(BO)
	movaps	%xmm2,  8 * SIZE(BO)
	movaps	%xmm3, 12 * SIZE(BO)
	movaps	%xmm4, 16 * SIZE(BO)
	movaps	%xmm5, 20 * SIZE(BO)
	movaps	%xmm6, 24 * SIZE(BO)
	movaps	%xmm7, 28 * SIZE(BO)

	pshufd	 $0x00, %xmm11, %xmm8
	pshufd	 $0x55, %xmm11, %xmm9
	pshufd	 $0xaa, %xmm11, %xmm10
	pshufd	 $0xff, %xmm11, %xmm11

	pshufd	 $0x00, %xmm15, %xmm12
	pshufd	 $0x55, %xmm15, %xmm13
	pshufd	 $0xaa, %xmm15, %xmm14
	pshufd	 $0xff, %xmm15, %xmm15

	movaps	%xmm8,  32 * SIZE(BO)
	movaps	%xmm9,  36 * SIZE(BO)
	movaps	%xmm10, 40 * SIZE(BO)
	movaps	%xmm11, 44 * SIZE(BO)
	movaps	%xmm12, 48 * SIZE(BO)
	movaps	%xmm13, 52 * SIZE(BO)
	movaps	%xmm14, 56 * SIZE(BO)
	movaps	%xmm15, 60 * SIZE(BO)

	addq	$16 * SIZE, B
	addq	$64 * SIZE, BO

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
	movaps	 0 * SIZE(B), %xmm3

	pshufd	 $0x00, %xmm3, %xmm0
	pshufd	 $0x55, %xmm3, %xmm1
	pshufd	 $0xaa, %xmm3, %xmm2
	pshufd	 $0xff, %xmm3, %xmm3

	movaps	%xmm0,  0 * SIZE(BO)
	movaps	%xmm1,  4 * SIZE(BO)
	movaps	%xmm2,  8 * SIZE(BO)
	movaps	%xmm3, 12 * SIZE(BO)

	addq	$ 4 * SIZE, B
	addq	$16 * SIZE, BO
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

	testq	$1, M
	je	.L20

#ifdef LN
       movq	K, %rax
       salq	$BASE_SHIFT, %rax
       subq	%rax, AORIG
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	movq	AORIG, AO
	leaq	(AO, %rax, SIZE), AO
#endif

	leaq	BUFFER, BO

#if defined(LN) || defined(RT)
	movq	KK, %rax
	salq	$2 + BASE_SHIFT, %rax
	leaq	(BO, %rax, 4), BO
#endif

	movss	 0 * SIZE(AO), %xmm8
	movss	 4 * SIZE(AO), %xmm10

	movss	 0 * SIZE(BO), %xmm9
	movss	16 * SIZE(BO), %xmm11
	movss	32 * SIZE(BO), %xmm13
	movss	48 * SIZE(BO), %xmm15

	pxor	%xmm0, %xmm0
	pxor	%xmm1, %xmm1
	pxor	%xmm2, %xmm2
	pxor	%xmm3, %xmm3

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	sarq	$3, %rax
	je	.L45
	ALIGN_4

.L42:
	mulss	%xmm8, %xmm9
	addss	%xmm9, %xmm0
#if defined(OPTERON) && defined(HAVE_PREFETCH)
	PREFETCH	(PREFETCHSIZE +  0) * SIZE(AO)
#endif
	movss	 4 * SIZE(BO), %xmm9
	mulss	%xmm8, %xmm9
	addss	%xmm9, %xmm1
	movss	 8 * SIZE(BO), %xmm9
	mulss	%xmm8, %xmm9
	addss	%xmm9, %xmm2
	movss	12 * SIZE(BO), %xmm9
	mulss	%xmm8, %xmm9
	movss	 1 * SIZE(AO), %xmm8
	addss	%xmm9, %xmm3
	movss	64 * SIZE(BO), %xmm9

	mulss	%xmm8, %xmm11
	addss	%xmm11, %xmm0
	movss	20 * SIZE(BO), %xmm11
	mulss	%xmm8, %xmm11
	addss	%xmm11, %xmm1
	movss	24 * SIZE(BO), %xmm11
	mulss	%xmm8, %xmm11
	addss	%xmm11, %xmm2
	movss	28 * SIZE(BO), %xmm11
	mulss	%xmm8, %xmm11
	movss	 2 * SIZE(AO), %xmm8
	addss	%xmm11, %xmm3
	movss	80 * SIZE(BO), %xmm11

	mulss	%xmm8, %xmm13
	addss	%xmm13, %xmm0
	movss	36 * SIZE(BO), %xmm13
	mulss	%xmm8, %xmm13
	addss	%xmm13, %xmm1
	movss	40 * SIZE(BO), %xmm13
	mulss	%xmm8, %xmm13
	addss	%xmm13, %xmm2
	movss	44 * SIZE(BO), %xmm13
	mulss	%xmm8, %xmm13
	movss	 3 * SIZE(AO), %xmm8
	addss	%xmm13, %xmm3
	movss	96 * SIZE(BO), %xmm13

	mulss	%xmm8, %xmm15
	addss	%xmm15, %xmm0
	movss	52 * SIZE(BO), %xmm15
	mulss	%xmm8, %xmm15
	addss	%xmm15, %xmm1
	movss	56 * SIZE(BO), %xmm15
	mulss	%xmm8, %xmm15
	addss	%xmm15, %xmm2
	movss	60 * SIZE(BO), %xmm15
	mulss	%xmm8, %xmm15
	movss	 8 * SIZE(AO), %xmm8
	addss	%xmm15, %xmm3
	movss	112 * SIZE(BO), %xmm15

	mulss	%xmm10, %xmm9
	addss	%xmm9, %xmm0
	movss	68 * SIZE(BO), %xmm9
	mulss	%xmm10, %xmm9
	addss	%xmm9, %xmm1
	movss	72 * SIZE(BO), %xmm9
	mulss	%xmm10, %xmm9
	addss	%xmm9, %xmm2
	movss	76 * SIZE(BO), %xmm9
	mulss	%xmm10, %xmm9
	movss	 5 * SIZE(AO), %xmm10
	addss	%xmm9, %xmm3
	movss	128 * SIZE(BO), %xmm9

	mulss	%xmm10, %xmm11
	addss	%xmm11, %xmm0
	movss	84 * SIZE(BO), %xmm11
	mulss	%xmm10, %xmm11
	addss	%xmm11, %xmm1
	movss	88 * SIZE(BO), %xmm11
	mulss	%xmm10, %xmm11
	addss	%xmm11, %xmm2
	movss	92 * SIZE(BO), %xmm11
	mulss	%xmm10, %xmm11
	movss	 6 * SIZE(AO), %xmm10
	addss	%xmm11, %xmm3
	movss	144 * SIZE(BO), %xmm11

	mulss	%xmm10, %xmm13
	addss	%xmm13, %xmm0
	movss	100 * SIZE(BO), %xmm13
	mulss	%xmm10, %xmm13
	addss	%xmm13, %xmm1
	movss	104 * SIZE(BO), %xmm13
	mulss	%xmm10, %xmm13
	addss	%xmm13, %xmm2
	movss	108 * SIZE(BO), %xmm13
	mulss	%xmm10, %xmm13
	movss	 7 * SIZE(AO), %xmm10
	addss	%xmm13, %xmm3
	movss	160 * SIZE(BO), %xmm13

	mulss	%xmm10, %xmm15
	addss	%xmm15, %xmm0
	movss	116 * SIZE(BO), %xmm15
	mulss	%xmm10, %xmm15
	addss	%xmm15, %xmm1
	movss	120 * SIZE(BO), %xmm15
	mulss	%xmm10, %xmm15
	addss	%xmm15, %xmm2
	movss	124 * SIZE(BO), %xmm15
	mulss	%xmm10, %xmm15
	movss	 12 * SIZE(AO), %xmm10
	addss	%xmm15, %xmm3
	movss	176 * SIZE(BO), %xmm15

	addq   $  8 * SIZE, AO
	addq   $128 * SIZE, BO
	decq   %rax
	jne    .L42
	ALIGN_4

.L45:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L48
	ALIGN_4

.L46:
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm0
	movss	 4 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm1
	movss	 8 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm2
	movss	12 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	movss	 1 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm3
	movss	16 * SIZE(BO), %xmm9

	addq	$ 1 * SIZE, AO		# aoffset  += 4
	addq	$16 * SIZE, BO		# boffset1 += 8
	decq	%rax
	jg	.L46
	ALIGN_4

.L48:
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

	salq	$BASE_SHIFT, %rax
	leaq	(AO, %rax, 1), AO
	leaq	(B,  %rax, 4), B
	leaq	(BO, %rax, 8), BO
	leaq	(BO, %rax, 8), BO
#endif

#if defined(LN) || defined(LT)
	unpcklps %xmm2,  %xmm0
	unpcklps %xmm3,  %xmm1

	unpcklps %xmm1,  %xmm0

	movapd	 0 * SIZE(B), %xmm1
	subps	%xmm0,  %xmm1
#else
	movss	 0 * SIZE(AO), %xmm8
	movss	 1 * SIZE(AO), %xmm10
	movss	 2 * SIZE(AO), %xmm12
	movss	 3 * SIZE(AO), %xmm14

	subss	%xmm0, %xmm8
	subss	%xmm1, %xmm10
	subss	%xmm2, %xmm12
	subss	%xmm3, %xmm14
#endif

#if defined(LN) || defined(LT)
	movss	  0 * SIZE(AO), %xmm6
	pshufd	 $0x00, %xmm6, %xmm8
	mulps	 %xmm8, %xmm1
#endif

#ifdef RN
	movaps	 0 * SIZE(B), %xmm0
	pshufd	 $0x00, %xmm0, %xmm2
	mulss	 %xmm2, %xmm8
	pshufd	 $0x55, %xmm0, %xmm2
	mulss	 %xmm8, %xmm2
	subss	 %xmm2, %xmm10
	pshufd	 $0xaa, %xmm0, %xmm2
	mulss	 %xmm8, %xmm2
	subss	 %xmm2, %xmm12
	pshufd	 $0xff, %xmm0, %xmm2
	mulss	 %xmm8, %xmm2
	subss	 %xmm2, %xmm14

	movaps	 4 * SIZE(B), %xmm0
	pshufd	 $0x55, %xmm0, %xmm2
	mulss	 %xmm2, %xmm10
	pshufd	 $0xaa, %xmm0, %xmm2
	mulss	 %xmm10, %xmm2
	subss	 %xmm2, %xmm12
	pshufd	 $0xff, %xmm0, %xmm2
	mulss	 %xmm10, %xmm2
	subss	 %xmm2, %xmm14

	movaps	 8 * SIZE(B), %xmm0
	pshufd	 $0xaa, %xmm0, %xmm2
	mulss	 %xmm2, %xmm12
	pshufd	 $0xff, %xmm0, %xmm2
	mulss	 %xmm12, %xmm2
	subss	 %xmm2, %xmm14

	movaps	 12 * SIZE(B), %xmm0
	pshufd	 $0xff, %xmm0, %xmm2
	mulss	 %xmm2, %xmm14
#endif

#ifdef RT
	movaps	 12 * SIZE(B), %xmm0
	pshufd	 $0xff, %xmm0, %xmm2
	mulss	 %xmm2, %xmm14
	pshufd	 $0xaa, %xmm0, %xmm2
	mulss	 %xmm14, %xmm2
	subss	 %xmm2, %xmm12
	pshufd	 $0x55, %xmm0, %xmm2
	mulss	 %xmm14, %xmm2
	subss	 %xmm2, %xmm10
	pshufd	 $0x00, %xmm0, %xmm2
	mulss	 %xmm14, %xmm2
	subss	 %xmm2, %xmm8

	movaps	  8 * SIZE(B), %xmm0
	pshufd	 $0xaa, %xmm0, %xmm2
	mulss	 %xmm2, %xmm12
	pshufd	 $0x55, %xmm0, %xmm2
	mulss	 %xmm12, %xmm2
	subss	 %xmm2, %xmm10
	pshufd	 $0x00, %xmm0, %xmm2
	mulss	 %xmm12, %xmm2
	subss	 %xmm2, %xmm8

	movaps	  4 * SIZE(B), %xmm0
	pshufd	 $0x55, %xmm0, %xmm2
	mulss	 %xmm2, %xmm10
	pshufd	 $0x00, %xmm0, %xmm2
	mulss	 %xmm10, %xmm2
	subss	 %xmm2, %xmm8

	movaps	  0 * SIZE(B), %xmm0
	pshufd	 $0x00, %xmm0, %xmm2
	mulss	 %xmm2, %xmm8
#endif

#ifdef LN
	subq	$1 * SIZE, CO1
	subq	$1 * SIZE, CO2
#endif

#if defined(LN) || defined(LT)
	movaps	%xmm1,   0 * SIZE(B)

	pshufd	$0x00, %xmm1, %xmm2
	pshufd	$0x55, %xmm1, %xmm3
	pshufd	$0xaa, %xmm1, %xmm4
	pshufd	$0xff, %xmm1, %xmm6
	movaps	%xmm2,   0 * SIZE(BO)
	movaps	%xmm3,   4 * SIZE(BO)
	movaps	%xmm4,   8 * SIZE(BO)
	movaps	%xmm6,  12 * SIZE(BO)
#else
	movss	%xmm8,   0 * SIZE(AO)
	movss	%xmm10,  1 * SIZE(AO)
	movss	%xmm12,  2 * SIZE(AO)
	movss	%xmm14,  3 * SIZE(AO)
#endif

#if defined(LN) || defined(LT)
	movaps	 %xmm1,  %xmm0
	unpcklps %xmm10, %xmm1
	unpckhps %xmm10, %xmm0

	movaps	 %xmm5,  %xmm7
	unpcklps %xmm11, %xmm5
	unpckhps %xmm11, %xmm7

	movaps	 %xmm1,  %xmm10
	unpcklps %xmm5,  %xmm1
	unpckhps %xmm5,  %xmm10

	movaps	 %xmm0,  %xmm11
	unpcklps %xmm7,  %xmm0
	unpckhps %xmm7,  %xmm11

	movss	%xmm1,  0 * SIZE(CO1)
	movss	%xmm10, 0 * SIZE(CO2)
	movss	%xmm0,  0 * SIZE(CO1, LDC, 2)
	movss	%xmm11, 0 * SIZE(CO2, LDC, 2)
#else
	movss	%xmm8,   0 * SIZE(CO1)
	movss	%xmm10,  0 * SIZE(CO2)
	movss	%xmm12,  0 * SIZE(CO1, LDC, 2)
	movss	%xmm14,  0 * SIZE(CO2, LDC, 2)
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
	salq	$BASE_SHIFT, %rax
	addq	%rax, AORIG
#endif
	ALIGN_4

.L20:
	testq	$2, M
	je	.L30

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
	leaq	(BO, %rax, 4), BO
#endif

	movaps	 0 * SIZE(AO), %xmm8
	movaps	 8 * SIZE(AO), %xmm10

	movaps	 0 * SIZE(BO), %xmm9
	movaps	16 * SIZE(BO), %xmm11
	movaps	32 * SIZE(BO), %xmm13
	movaps	48 * SIZE(BO), %xmm15

	pxor	%xmm0, %xmm0
	pxor	%xmm1, %xmm1
	pxor	%xmm2, %xmm2
	pxor	%xmm3, %xmm3

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
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm0
#if defined(OPTERON) && defined(HAVE_PREFETCH)
	PREFETCH	(PREFETCHSIZE +  0) * SIZE(AO)
#endif
	movaps	 4 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm1
	movaps	 8 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm2
	movaps	12 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	movsd	 2 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm3
	movaps	64 * SIZE(BO), %xmm9

	mulps	%xmm8, %xmm11
	addps	%xmm11, %xmm0
	movaps	20 * SIZE(BO), %xmm11
	mulps	%xmm8, %xmm11
	addps	%xmm11, %xmm1
	movaps	24 * SIZE(BO), %xmm11
	mulps	%xmm8, %xmm11
	addps	%xmm11, %xmm2
	movaps	28 * SIZE(BO), %xmm11
	mulps	%xmm8, %xmm11
	movsd	 4 * SIZE(AO), %xmm8
	addps	%xmm11, %xmm3
	movaps	80 * SIZE(BO), %xmm11

	mulps	%xmm8, %xmm13
	addps	%xmm13, %xmm0
	movaps	36 * SIZE(BO), %xmm13
	mulps	%xmm8, %xmm13
	addps	%xmm13, %xmm1
	movaps	40 * SIZE(BO), %xmm13
	mulps	%xmm8, %xmm13
	addps	%xmm13, %xmm2
	movaps	44 * SIZE(BO), %xmm13
	mulps	%xmm8, %xmm13
	movsd	 6 * SIZE(AO), %xmm8
	addps	%xmm13, %xmm3
	movaps	96 * SIZE(BO), %xmm13

	mulps	%xmm8, %xmm15
	addps	%xmm15, %xmm0
	movaps	52 * SIZE(BO), %xmm15
	mulps	%xmm8, %xmm15
	addps	%xmm15, %xmm1
	movaps	56 * SIZE(BO), %xmm15
	mulps	%xmm8, %xmm15
	addps	%xmm15, %xmm2
	movaps	60 * SIZE(BO), %xmm15
	mulps	%xmm8, %xmm15
	movsd	16 * SIZE(AO), %xmm8
	addps	%xmm15, %xmm3
	movaps	112 * SIZE(BO), %xmm15

	mulps	%xmm10, %xmm9
	addps	%xmm9, %xmm0
	movaps	68 * SIZE(BO), %xmm9
	mulps	%xmm10, %xmm9
	addps	%xmm9, %xmm1
	movaps	72 * SIZE(BO), %xmm9
	mulps	%xmm10, %xmm9
	addps	%xmm9, %xmm2
	movaps	76 * SIZE(BO), %xmm9
	mulps	%xmm10, %xmm9
	movsd	10 * SIZE(AO), %xmm10
	addps	%xmm9, %xmm3
	movaps	128 * SIZE(BO), %xmm9

	mulps	%xmm10, %xmm11
	addps	%xmm11, %xmm0
	movaps	84 * SIZE(BO), %xmm11
	mulps	%xmm10, %xmm11
	addps	%xmm11, %xmm1
	movaps	88 * SIZE(BO), %xmm11
	mulps	%xmm10, %xmm11
	addps	%xmm11, %xmm2
	movaps	92 * SIZE(BO), %xmm11
	mulps	%xmm10, %xmm11
	movsd	12 * SIZE(AO), %xmm10
	addps	%xmm11, %xmm3
	movaps	144 * SIZE(BO), %xmm11

	mulps	%xmm10, %xmm13
	addps	%xmm13, %xmm0
	movaps	100 * SIZE(BO), %xmm13
	mulps	%xmm10, %xmm13
	addps	%xmm13, %xmm1
	movaps	104 * SIZE(BO), %xmm13
	mulps	%xmm10, %xmm13
	addps	%xmm13, %xmm2
	movaps	108 * SIZE(BO), %xmm13
	mulps	%xmm10, %xmm13
	movsd	14 * SIZE(AO), %xmm10
	addps	%xmm13, %xmm3
	movaps	160 * SIZE(BO), %xmm13

	mulps	%xmm10, %xmm15
	addps	%xmm15, %xmm0
	movaps	116 * SIZE(BO), %xmm15
	mulps	%xmm10, %xmm15
	addps	%xmm15, %xmm1
	movaps	120 * SIZE(BO), %xmm15
	mulps	%xmm10, %xmm15
	addps	%xmm15, %xmm2
	movaps	124 * SIZE(BO), %xmm15
	mulps	%xmm10, %xmm15
	movsd	24 * SIZE(AO), %xmm10
	addps	%xmm15, %xmm3
	movaps	176 * SIZE(BO), %xmm15

	addq   $ 16 * SIZE, AO
	addq   $128 * SIZE, BO
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
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm0
	movaps	 4 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm1
	movaps	 8 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm2
	movaps	12 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	movsd	 2 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm3
	movaps	16 * SIZE(BO), %xmm9

	addq	$ 2 * SIZE, AO		# aoffset  += 4
	addq	$16 * SIZE, BO		# boffset1 += 8
	decq	%rax
	jg	.L36
	ALIGN_4

.L38:
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

	salq	$1 + BASE_SHIFT, %rax
	leaq	(AO, %rax, 1), AO
	leaq	(B,  %rax, 2), B
	leaq	(BO, %rax, 8), BO
#endif

#if defined(LN) || defined(LT)
	unpcklps %xmm2,  %xmm0
	unpcklps %xmm3,  %xmm1

	movaps	 %xmm0,  %xmm2
	unpcklps %xmm1,  %xmm0
	unpckhps %xmm1,  %xmm2

	movapd	 0 * SIZE(B), %xmm1
	movapd	 4 * SIZE(B), %xmm5

	subps	%xmm0,  %xmm1
	subps	%xmm2,  %xmm5
#else
#ifdef	movsd
	xorps	%xmm8, %xmm8
#endif
	movsd	 0 * SIZE(AO), %xmm8
#ifdef	movsd
	xorps	%xmm10, %xmm10
#endif
	movsd	 2 * SIZE(AO), %xmm10
#ifdef	movsd
	xorps	%xmm12, %xmm12
#endif
	movsd	 4 * SIZE(AO), %xmm12
#ifdef	movsd
	xorps	%xmm14, %xmm14
#endif
	movsd	 6 * SIZE(AO), %xmm14

	subps	%xmm0, %xmm8
	subps	%xmm1, %xmm10
	subps	%xmm2, %xmm12
	subps	%xmm3, %xmm14
#endif

#ifdef LN
	movaps	  0 * SIZE(AO), %xmm6

	pshufd	 $0xff, %xmm6, %xmm8
	mulps	 %xmm8, %xmm5
	pshufd	 $0xaa, %xmm6, %xmm8
	mulps	 %xmm5, %xmm8
	subps	 %xmm8, %xmm1

	pshufd	 $0x00, %xmm6, %xmm8
	mulps	 %xmm8, %xmm1
#endif

#ifdef LT
	movaps	 0 * SIZE(AO), %xmm6
	pshufd	 $0x00, %xmm6, %xmm8
	mulps	 %xmm8, %xmm1
	pshufd	 $0x55, %xmm6, %xmm8
	mulps	 %xmm1, %xmm8
	subps	 %xmm8, %xmm5
	pshufd	 $0xff, %xmm6, %xmm8
	mulps	 %xmm8, %xmm5
#endif

#ifdef RN
	movaps	 0 * SIZE(B), %xmm0
	pshufd	 $0x00, %xmm0, %xmm2
	mulps	 %xmm2, %xmm8
	pshufd	 $0x55, %xmm0, %xmm2
	mulps	 %xmm8, %xmm2
	subps	 %xmm2, %xmm10
	pshufd	 $0xaa, %xmm0, %xmm2
	mulps	 %xmm8, %xmm2
	subps	 %xmm2, %xmm12
	pshufd	 $0xff, %xmm0, %xmm2
	mulps	 %xmm8, %xmm2
	subps	 %xmm2, %xmm14

	movaps	 4 * SIZE(B), %xmm0
	pshufd	 $0x55, %xmm0, %xmm2
	mulps	 %xmm2, %xmm10
	pshufd	 $0xaa, %xmm0, %xmm2
	mulps	 %xmm10, %xmm2
	subps	 %xmm2, %xmm12
	pshufd	 $0xff, %xmm0, %xmm2
	mulps	 %xmm10, %xmm2
	subps	 %xmm2, %xmm14

	movaps	 8 * SIZE(B), %xmm0
	pshufd	 $0xaa, %xmm0, %xmm2
	mulps	 %xmm2, %xmm12
	pshufd	 $0xff, %xmm0, %xmm2
	mulps	 %xmm12, %xmm2
	subps	 %xmm2, %xmm14

	movaps	 12 * SIZE(B), %xmm0
	pshufd	 $0xff, %xmm0, %xmm2
	mulps	 %xmm2, %xmm14
#endif

#ifdef RT
	movaps	 12 * SIZE(B), %xmm0
	pshufd	 $0xff, %xmm0, %xmm2
	mulps	 %xmm2, %xmm14
	pshufd	 $0xaa, %xmm0, %xmm2
	mulps	 %xmm14, %xmm2
	subps	 %xmm2, %xmm12
	pshufd	 $0x55, %xmm0, %xmm2
	mulps	 %xmm14, %xmm2
	subps	 %xmm2, %xmm10
	pshufd	 $0x00, %xmm0, %xmm2
	mulps	 %xmm14, %xmm2
	subps	 %xmm2, %xmm8

	movaps	  8 * SIZE(B), %xmm0
	pshufd	 $0xaa, %xmm0, %xmm2
	mulps	 %xmm2, %xmm12
	pshufd	 $0x55, %xmm0, %xmm2
	mulps	 %xmm12, %xmm2
	subps	 %xmm2, %xmm10
	pshufd	 $0x00, %xmm0, %xmm2
	mulps	 %xmm12, %xmm2
	subps	 %xmm2, %xmm8

	movaps	  4 * SIZE(B), %xmm0
	pshufd	 $0x55, %xmm0, %xmm2
	mulps	 %xmm2, %xmm10
	pshufd	 $0x00, %xmm0, %xmm2
	mulps	 %xmm10, %xmm2
	subps	 %xmm2, %xmm8

	movaps	  0 * SIZE(B), %xmm0
	pshufd	 $0x00, %xmm0, %xmm2
	mulps	 %xmm2, %xmm8
#endif

#ifdef LN
	subq	$2 * SIZE, CO1
	subq	$2 * SIZE, CO2
#endif

#if defined(LN) || defined(LT)
	movaps	%xmm1,   0 * SIZE(B)
	movaps	%xmm5,   4 * SIZE(B)

	pshufd	$0x00, %xmm1, %xmm2
	pshufd	$0x55, %xmm1, %xmm3
	pshufd	$0xaa, %xmm1, %xmm4
	pshufd	$0xff, %xmm1, %xmm6
	movaps	%xmm2,   0 * SIZE(BO)
	movaps	%xmm3,   4 * SIZE(BO)
	movaps	%xmm4,   8 * SIZE(BO)
	movaps	%xmm6,  12 * SIZE(BO)

	pshufd	$0x00, %xmm5, %xmm2
	pshufd	$0x55, %xmm5, %xmm3
	pshufd	$0xaa, %xmm5, %xmm4
	pshufd	$0xff, %xmm5, %xmm6
	movaps	%xmm2,  16 * SIZE(BO)
	movaps	%xmm3,  20 * SIZE(BO)
	movaps	%xmm4,  24 * SIZE(BO)
	movaps	%xmm6,  28 * SIZE(BO)
#else
	movlps	%xmm8,   0 * SIZE(AO)
	movlps	%xmm10,  2 * SIZE(AO)
	movlps	%xmm12,  4 * SIZE(AO)
	movlps	%xmm14,  6 * SIZE(AO)
#endif

#if defined(LN) || defined(LT)
	movaps	 %xmm1,  %xmm0
	unpcklps %xmm10, %xmm1
	unpckhps %xmm10, %xmm0

	movaps	 %xmm5,  %xmm7
	unpcklps %xmm11, %xmm5
	unpckhps %xmm11, %xmm7

	movaps	 %xmm1,  %xmm10
	unpcklps %xmm5,  %xmm1
	unpckhps %xmm5,  %xmm10

	movaps	 %xmm0,  %xmm11
	unpcklps %xmm7,  %xmm0
	unpckhps %xmm7,  %xmm11

	movlps	%xmm1,  0 * SIZE(CO1)
	movlps	%xmm10, 0 * SIZE(CO2)
	movlps	%xmm0,  0 * SIZE(CO1, LDC, 2)
	movlps	%xmm11, 0 * SIZE(CO2, LDC, 2)
#else
	movlps	%xmm8,   0 * SIZE(CO1)
	movlps	%xmm10,  0 * SIZE(CO2)
	movlps	%xmm12,  0 * SIZE(CO1, LDC, 2)
	movlps	%xmm14,  0 * SIZE(CO2, LDC, 2)
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
	testq	$4, M
	je	.L40

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
	leaq	(BO, %rax, 4), BO
#endif

	movaps	 0 * SIZE(AO), %xmm8
	movaps	16 * SIZE(AO), %xmm10

	movaps	 0 * SIZE(BO), %xmm9
	movaps	16 * SIZE(BO), %xmm11
	movaps	32 * SIZE(BO), %xmm13
	movaps	48 * SIZE(BO), %xmm15

	pxor	%xmm0, %xmm0
	pxor	%xmm1, %xmm1
	pxor	%xmm2, %xmm2
	pxor	%xmm3, %xmm3

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
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm0
#if defined(OPTERON) && defined(HAVE_PREFETCH)
	PREFETCH	(PREFETCHSIZE +  0) * SIZE(AO)
#endif
	movaps	 4 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm1
	movaps	 8 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	mulps	12 * SIZE(BO), %xmm8
	addps	%xmm9, %xmm2
	movaps	64 * SIZE(BO), %xmm9
	addps	%xmm8, %xmm3
	movaps	 4 * SIZE(AO), %xmm8

	mulps	%xmm8, %xmm11
	addps	%xmm11, %xmm0
	movaps	20 * SIZE(BO), %xmm11
	mulps	%xmm8, %xmm11
	addps	%xmm11, %xmm1
	movaps	24 * SIZE(BO), %xmm11
	mulps	%xmm8, %xmm11
	mulps	28 * SIZE(BO), %xmm8
	addps	%xmm11, %xmm2
	movaps	80 * SIZE(BO), %xmm11
	addps	%xmm8, %xmm3
	movaps	 8 * SIZE(AO), %xmm8

	mulps	%xmm8, %xmm13
	addps	%xmm13, %xmm0
	movaps	36 * SIZE(BO), %xmm13
	mulps	%xmm8, %xmm13
	addps	%xmm13, %xmm1
	movaps	40 * SIZE(BO), %xmm13
	mulps	%xmm8, %xmm13
	mulps	44 * SIZE(BO), %xmm8
	addps	%xmm13, %xmm2
	movaps	96 * SIZE(BO), %xmm13
	addps	%xmm8, %xmm3
	movaps	12 * SIZE(AO), %xmm8

	mulps	%xmm8, %xmm15
	addps	%xmm15, %xmm0
	movaps	52 * SIZE(BO), %xmm15
	mulps	%xmm8, %xmm15
	addps	%xmm15, %xmm1
	movaps	56 * SIZE(BO), %xmm15
	mulps	%xmm8, %xmm15
	mulps	60 * SIZE(BO), %xmm8
	addps	%xmm15, %xmm2
	movaps	112 * SIZE(BO), %xmm15
	addps	%xmm8, %xmm3
	movaps	32 * SIZE(AO), %xmm8

#if defined(OPTERON) && defined(HAVE_PREFETCH)
	PREFETCH	(PREFETCHSIZE + 16) * SIZE(AO)
#endif
	mulps	%xmm10, %xmm9
	addps	%xmm9, %xmm0
	movaps	68 * SIZE(BO), %xmm9
	mulps	%xmm10, %xmm9
	addps	%xmm9, %xmm1
	movaps	72 * SIZE(BO), %xmm9
	mulps	%xmm10, %xmm9
	mulps	76 * SIZE(BO), %xmm10
	addps	%xmm9, %xmm2
	movaps	128 * SIZE(BO), %xmm9
	addps	%xmm10, %xmm3
	movaps	20 * SIZE(AO), %xmm10

	mulps	%xmm10, %xmm11
	addps	%xmm11, %xmm0
	movaps	84 * SIZE(BO), %xmm11
	mulps	%xmm10, %xmm11
	addps	%xmm11, %xmm1
	movaps	88 * SIZE(BO), %xmm11
	mulps	%xmm10, %xmm11
	mulps	92 * SIZE(BO), %xmm10
	addps	%xmm11, %xmm2
	movaps	144 * SIZE(BO), %xmm11
	addps	%xmm10, %xmm3
	movaps	24 * SIZE(AO), %xmm10

	mulps	%xmm10, %xmm13
	addps	%xmm13, %xmm0
	movaps	100 * SIZE(BO), %xmm13
	mulps	%xmm10, %xmm13
	addps	%xmm13, %xmm1
	movaps	104 * SIZE(BO), %xmm13
	mulps	%xmm10, %xmm13
	mulps	108 * SIZE(BO), %xmm10
	addps	%xmm13, %xmm2
	movaps	160 * SIZE(BO), %xmm13
	addps	%xmm10, %xmm3
	movaps	28 * SIZE(AO), %xmm10

	mulps	%xmm10, %xmm15
	addps	%xmm15, %xmm0
	movaps	116 * SIZE(BO), %xmm15
	mulps	%xmm10, %xmm15
	addps	%xmm15, %xmm1
	movaps	120 * SIZE(BO), %xmm15
	mulps	%xmm10, %xmm15
	mulps	124 * SIZE(BO), %xmm10
	addps	%xmm15, %xmm2
	movaps	176 * SIZE(BO), %xmm15
	addps	%xmm10, %xmm3
	movaps	48 * SIZE(AO), %xmm10

	addq   $ 32 * SIZE, AO
	addq   $128 * SIZE, BO
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
	je .L28
	ALIGN_4

.L26:
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm0
	movaps	 4 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm1
	movaps	 8 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	mulps	12 * SIZE(BO), %xmm8
	addps	%xmm9, %xmm2
	movaps	16 * SIZE(BO), %xmm9
	addps	%xmm8, %xmm3
	movaps	 4 * SIZE(AO), %xmm8

	addq	$ 4 * SIZE, AO		# aoffset  += 4
	addq	$16 * SIZE, BO		# boffset1 += 8
	decq	%rax
	jg	.L26
	ALIGN_4

.L28:
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

	salq	$2 + BASE_SHIFT, %rax
	leaq	(AO, %rax, 1), AO
	leaq	(B,  %rax, 1), B
	leaq	(BO, %rax, 4), BO
#endif

#if defined(LN) || defined(LT)
	movaps	 %xmm0,  %xmm8
	unpcklps %xmm2,  %xmm0
	unpckhps %xmm2,  %xmm8

	movaps	 %xmm1,  %xmm14
	unpcklps %xmm3,  %xmm1
	unpckhps %xmm3,  %xmm14

	movaps	 %xmm0,  %xmm2
	unpcklps %xmm1,  %xmm0
	unpckhps %xmm1,  %xmm2

	movaps	 %xmm8,  %xmm3
	unpcklps %xmm14, %xmm8
	unpckhps %xmm14, %xmm3

	movaps	 0 * SIZE(B), %xmm1
	movaps	 4 * SIZE(B), %xmm5
	movaps	 8 * SIZE(B), %xmm10
	movaps	12 * SIZE(B), %xmm11

	subps	%xmm0,  %xmm1
	subps	%xmm2,  %xmm5
	subps	%xmm8,  %xmm10
	subps	%xmm3,  %xmm11
#else
	movaps	 0 * SIZE(AO), %xmm8
	movaps	 4 * SIZE(AO), %xmm10
	movaps	 8 * SIZE(AO), %xmm12
	movaps	12 * SIZE(AO), %xmm14

	subps	%xmm0, %xmm8
	subps	%xmm1, %xmm10
	subps	%xmm2, %xmm12
	subps	%xmm3, %xmm14
#endif

#ifdef LN
	movaps	 12 * SIZE(AO), %xmm6
	pshufd	 $0xff, %xmm6, %xmm8
	mulps	 %xmm8, %xmm11
	pshufd	 $0xaa, %xmm6, %xmm8
	mulps	 %xmm11, %xmm8
	subps	 %xmm8, %xmm10
	pshufd	 $0x55, %xmm6, %xmm8
	mulps	 %xmm11, %xmm8
	subps	 %xmm8, %xmm5
	pshufd	 $0x00, %xmm6, %xmm8
	mulps	 %xmm11, %xmm8
	subps	 %xmm8, %xmm1

	movaps	  8 * SIZE(AO), %xmm6
	pshufd	 $0xaa, %xmm6, %xmm8
	mulps	 %xmm8, %xmm10
	pshufd	 $0x55, %xmm6, %xmm8
	mulps	 %xmm10, %xmm8
	subps	 %xmm8, %xmm5
	pshufd	 $0x00, %xmm6, %xmm8
	mulps	 %xmm10, %xmm8
	subps	 %xmm8, %xmm1

	movaps	  4 * SIZE(AO), %xmm6
	pshufd	 $0x55, %xmm6, %xmm8
	mulps	 %xmm8, %xmm5
	pshufd	 $0x00, %xmm6, %xmm8
	mulps	 %xmm5, %xmm8
	subps	 %xmm8, %xmm1

	movaps	  0 * SIZE(AO), %xmm6
	pshufd	 $0x00, %xmm6, %xmm8
	mulps	 %xmm8, %xmm1
#endif

#ifdef LT
	movaps	 0 * SIZE(AO), %xmm6
	pshufd	 $0x00, %xmm6, %xmm8
	mulps	 %xmm8, %xmm1
	pshufd	 $0x55, %xmm6, %xmm8
	mulps	 %xmm1, %xmm8
	subps	 %xmm8, %xmm5
	pshufd	 $0xaa, %xmm6, %xmm8
	mulps	 %xmm1, %xmm8
	subps	 %xmm8, %xmm10
	pshufd	 $0xff, %xmm6, %xmm8
	mulps	 %xmm1, %xmm8
	subps	 %xmm8, %xmm11

	movaps	 4 * SIZE(AO), %xmm6
	pshufd	 $0x55, %xmm6, %xmm8
	mulps	 %xmm8, %xmm5
	pshufd	 $0xaa, %xmm6, %xmm8
	mulps	 %xmm5, %xmm8
	subps	 %xmm8, %xmm10
	pshufd	 $0xff, %xmm6, %xmm8
	mulps	 %xmm5, %xmm8
	subps	 %xmm8, %xmm11

	movaps	 8 * SIZE(AO), %xmm6
	pshufd	 $0xaa, %xmm6, %xmm8
	mulps	 %xmm8, %xmm10
	pshufd	 $0xff, %xmm6, %xmm8
	mulps	 %xmm10, %xmm8
	subps	 %xmm8, %xmm11

	movaps	12 * SIZE(AO), %xmm6
	pshufd	 $0xff, %xmm6, %xmm8
	mulps	 %xmm8, %xmm11
#endif

#ifdef RN
	movaps	 0 * SIZE(B), %xmm0
	pshufd	 $0x00, %xmm0, %xmm2
	mulps	 %xmm2, %xmm8
	pshufd	 $0x55, %xmm0, %xmm2
	mulps	 %xmm8, %xmm2
	subps	 %xmm2, %xmm10
	pshufd	 $0xaa, %xmm0, %xmm2
	mulps	 %xmm8, %xmm2
	subps	 %xmm2, %xmm12
	pshufd	 $0xff, %xmm0, %xmm2
	mulps	 %xmm8, %xmm2
	subps	 %xmm2, %xmm14

	movaps	 4 * SIZE(B), %xmm0
	pshufd	 $0x55, %xmm0, %xmm2
	mulps	 %xmm2, %xmm10
	pshufd	 $0xaa, %xmm0, %xmm2
	mulps	 %xmm10, %xmm2
	subps	 %xmm2, %xmm12
	pshufd	 $0xff, %xmm0, %xmm2
	mulps	 %xmm10, %xmm2
	subps	 %xmm2, %xmm14

	movaps	 8 * SIZE(B), %xmm0
	pshufd	 $0xaa, %xmm0, %xmm2
	mulps	 %xmm2, %xmm12
	pshufd	 $0xff, %xmm0, %xmm2
	mulps	 %xmm12, %xmm2
	subps	 %xmm2, %xmm14

	movaps	 12 * SIZE(B), %xmm0
	pshufd	 $0xff, %xmm0, %xmm2
	mulps	 %xmm2, %xmm14
#endif

#ifdef RT
	movaps	 12 * SIZE(B), %xmm0
	pshufd	 $0xff, %xmm0, %xmm2
	mulps	 %xmm2, %xmm14
	pshufd	 $0xaa, %xmm0, %xmm2
	mulps	 %xmm14, %xmm2
	subps	 %xmm2, %xmm12
	pshufd	 $0x55, %xmm0, %xmm2
	mulps	 %xmm14, %xmm2
	subps	 %xmm2, %xmm10
	pshufd	 $0x00, %xmm0, %xmm2
	mulps	 %xmm14, %xmm2
	subps	 %xmm2, %xmm8

	movaps	  8 * SIZE(B), %xmm0
	pshufd	 $0xaa, %xmm0, %xmm2
	mulps	 %xmm2, %xmm12
	pshufd	 $0x55, %xmm0, %xmm2
	mulps	 %xmm12, %xmm2
	subps	 %xmm2, %xmm10
	pshufd	 $0x00, %xmm0, %xmm2
	mulps	 %xmm12, %xmm2
	subps	 %xmm2, %xmm8

	movaps	  4 * SIZE(B), %xmm0
	pshufd	 $0x55, %xmm0, %xmm2
	mulps	 %xmm2, %xmm10
	pshufd	 $0x00, %xmm0, %xmm2
	mulps	 %xmm10, %xmm2
	subps	 %xmm2, %xmm8

	movaps	  0 * SIZE(B), %xmm0
	pshufd	 $0x00, %xmm0, %xmm2
	mulps	 %xmm2, %xmm8
#endif

#ifdef LN
	subq	$4 * SIZE, CO1
	subq	$4 * SIZE, CO2
#endif

#if defined(LN) || defined(LT)
	movaps	%xmm1,   0 * SIZE(B)
	movaps	%xmm5,   4 * SIZE(B)
	movaps	%xmm10,  8 * SIZE(B)
	movaps	%xmm11, 12 * SIZE(B)

	pshufd	$0x00, %xmm1, %xmm2
	pshufd	$0x55, %xmm1, %xmm3
	pshufd	$0xaa, %xmm1, %xmm4
	pshufd	$0xff, %xmm1, %xmm6
	movaps	%xmm2,   0 * SIZE(BO)
	movaps	%xmm3,   4 * SIZE(BO)
	movaps	%xmm4,   8 * SIZE(BO)
	movaps	%xmm6,  12 * SIZE(BO)

	pshufd	$0x00, %xmm5, %xmm2
	pshufd	$0x55, %xmm5, %xmm3
	pshufd	$0xaa, %xmm5, %xmm4
	pshufd	$0xff, %xmm5, %xmm6
	movaps	%xmm2,  16 * SIZE(BO)
	movaps	%xmm3,  20 * SIZE(BO)
	movaps	%xmm4,  24 * SIZE(BO)
	movaps	%xmm6,  28 * SIZE(BO)

	pshufd	$0x00, %xmm10, %xmm2
	pshufd	$0x55, %xmm10, %xmm3
	pshufd	$0xaa, %xmm10, %xmm4
	pshufd	$0xff, %xmm10, %xmm6
	movaps	%xmm2,  32 * SIZE(BO)
	movaps	%xmm3,  36 * SIZE(BO)
	movaps	%xmm4,  40 * SIZE(BO)
	movaps	%xmm6,  44 * SIZE(BO)

	pshufd	$0x00, %xmm11, %xmm2
	pshufd	$0x55, %xmm11, %xmm3
	pshufd	$0xaa, %xmm11, %xmm4
	pshufd	$0xff, %xmm11, %xmm6
	movaps	%xmm2,  48 * SIZE(BO)
	movaps	%xmm3,  52 * SIZE(BO)
	movaps	%xmm4,  56 * SIZE(BO)
	movaps	%xmm6,  60 * SIZE(BO)
#else
	movaps	%xmm8,   0 * SIZE(AO)
	movaps	%xmm10,  4 * SIZE(AO)
	movaps	%xmm12,  8 * SIZE(AO)
	movaps	%xmm14, 12 * SIZE(AO)
#endif

#if defined(LN) || defined(LT)
	movaps	 %xmm1,  %xmm0
	unpcklps %xmm10, %xmm1
	unpckhps %xmm10, %xmm0

	movaps	 %xmm5,  %xmm7
	unpcklps %xmm11, %xmm5
	unpckhps %xmm11, %xmm7

	movaps	 %xmm1,  %xmm10
	unpcklps %xmm5,  %xmm1
	unpckhps %xmm5,  %xmm10

	movaps	 %xmm0,  %xmm11
	unpcklps %xmm7,  %xmm0
	unpckhps %xmm7,  %xmm11

	movlps	%xmm1,  0 * SIZE(CO1)
	movhps	%xmm1,  2 * SIZE(CO1)
	movlps	%xmm10, 0 * SIZE(CO2)
	movhps	%xmm10, 2 * SIZE(CO2)

	movlps	%xmm0,  0 * SIZE(CO1, LDC, 2)
	movhps	%xmm0,  2 * SIZE(CO1, LDC, 2)
	movlps	%xmm11, 0 * SIZE(CO2, LDC, 2)
	movhps	%xmm11, 2 * SIZE(CO2, LDC, 2)
#else
	movlps	%xmm8,   0 * SIZE(CO1)
	movhps	%xmm8,   2 * SIZE(CO1)
	movlps	%xmm10,  0 * SIZE(CO2)
	movhps	%xmm10,  2 * SIZE(CO2)

	movlps	%xmm12,  0 * SIZE(CO1, LDC, 2)
	movhps	%xmm12,  2 * SIZE(CO1, LDC, 2)
	movlps	%xmm14,  0 * SIZE(CO2, LDC, 2)
	movhps	%xmm14,  2 * SIZE(CO2, LDC, 2)
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
	ALIGN_4

.L40:
	movq	M,  I
	sarq	$3, I	# i = (m >> 3)
	jle	.L49
	ALIGN_4

.L11:
#ifdef LN
       movq	K, %rax
       salq	$3 + BASE_SHIFT, %rax
       subq	%rax, AORIG
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	movq	AORIG, AO
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 8), AO
#endif

	leaq	BUFFER, BO

#if defined(LN) || defined(RT)
	movq	KK, %rax
	salq	$2 + BASE_SHIFT, %rax
	leaq	(BO, %rax, 4), BO
#endif

	movaps	 0 * SIZE(BO), %xmm9
	movaps	 4 * SIZE(BO), %xmm11
	movaps	 8 * SIZE(BO), %xmm13
	movaps	16 * SIZE(BO), %xmm15

	movaps	 0 * SIZE(AO), %xmm8
	pxor	%xmm0, %xmm0
	movaps	 4 * SIZE(AO), %xmm10
	pxor	%xmm1, %xmm1
	movaps	 8 * SIZE(AO), %xmm12
	pxor	%xmm2, %xmm2
	movaps	12 * SIZE(AO), %xmm14
	pxor	%xmm3, %xmm3

	PREFETCHW     -8 * SIZE(CO1)
	pxor	%xmm4, %xmm4
	PREFETCHW     -8 * SIZE(CO2)
	pxor	%xmm5, %xmm5
	PREFETCHW     -8 * SIZE(CO1, LDC, 2)
	pxor	%xmm6, %xmm6
	PREFETCHW     -8 * SIZE(CO2, LDC, 2)
	pxor	%xmm7, %xmm7

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
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm0
	movaps	 0 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm11
	addps	%xmm11, %xmm1
	movaps	 4 * SIZE(BO), %xmm11
	mulps	%xmm8, %xmm13
	mulps	12 * SIZE(BO), %xmm8
	addps	%xmm13, %xmm2
	movaps	 8 * SIZE(BO), %xmm13
	addps	%xmm8, %xmm3
	movaps	16 * SIZE(AO), %xmm8
	mulps	%xmm10, %xmm9
	addps	%xmm9, %xmm4
	movaps	32 * SIZE(BO), %xmm9
	mulps	%xmm10, %xmm11
	addps	%xmm11, %xmm5
	movaps	20 * SIZE(BO), %xmm11
	mulps	%xmm10, %xmm13
	mulps	12 * SIZE(BO), %xmm10
	addps	%xmm13, %xmm6
	movaps	24 * SIZE(BO), %xmm13
	addps	%xmm10, %xmm7
	movaps	20 * SIZE(AO), %xmm10
	mulps	%xmm12, %xmm15
	addps	%xmm15, %xmm0
	movaps	16 * SIZE(BO), %xmm15
	mulps	%xmm12, %xmm11
	addps	%xmm11, %xmm1
	movaps	20 * SIZE(BO), %xmm11
	mulps	%xmm12, %xmm13
	mulps	28 * SIZE(BO), %xmm12
	addps	%xmm13, %xmm2
	movaps	24 * SIZE(BO), %xmm13
	addps	%xmm12, %xmm3
	movaps	24 * SIZE(AO), %xmm12
	mulps	%xmm14, %xmm15
	addps	%xmm15, %xmm4
	movaps	48 * SIZE(BO), %xmm15
	mulps	%xmm14, %xmm11
	addps	%xmm11, %xmm5
	movaps	36 * SIZE(BO), %xmm11
	mulps	%xmm14, %xmm13
	mulps	28 * SIZE(BO), %xmm14
	addps	%xmm13, %xmm6
	movaps	40 * SIZE(BO), %xmm13
	addps	%xmm14, %xmm7
	movaps	28 * SIZE(AO), %xmm14
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm0
	movaps	32 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm11
	addps	%xmm11, %xmm1
	movaps	36 * SIZE(BO), %xmm11
	mulps	%xmm8, %xmm13
	mulps	44 * SIZE(BO), %xmm8
	addps	%xmm13, %xmm2
	movaps	40 * SIZE(BO), %xmm13
	addps	%xmm8, %xmm3
	movaps	32 * SIZE(AO), %xmm8
	mulps	%xmm10, %xmm9
	addps	%xmm9, %xmm4
	movaps	64 * SIZE(BO), %xmm9
	mulps	%xmm10, %xmm11
	addps	%xmm11, %xmm5
	movaps	52 * SIZE(BO), %xmm11
	mulps	%xmm10, %xmm13
	mulps	44 * SIZE(BO), %xmm10
	addps	%xmm13, %xmm6
	movaps	56 * SIZE(BO), %xmm13
	addps	%xmm10, %xmm7
	movaps	36 * SIZE(AO), %xmm10
	mulps	%xmm12, %xmm15
	addps	%xmm15, %xmm0
	movaps	48 * SIZE(BO), %xmm15
	mulps	%xmm12, %xmm11
	addps	%xmm11, %xmm1
	movaps	52 * SIZE(BO), %xmm11
	mulps	%xmm12, %xmm13
	mulps	60 * SIZE(BO), %xmm12
	addps	%xmm13, %xmm2
	movaps	56 * SIZE(BO), %xmm13
	addps	%xmm12, %xmm3
	movaps	40 * SIZE(AO), %xmm12
	mulps	%xmm14, %xmm15
	addps	%xmm15, %xmm4
	movaps	80 * SIZE(BO), %xmm15
	mulps	%xmm14, %xmm11
	addps	%xmm11, %xmm5
	movaps	68 * SIZE(BO), %xmm11
	mulps	%xmm14, %xmm13
	mulps	60 * SIZE(BO), %xmm14
	addps	%xmm13, %xmm6
	movaps	72 * SIZE(BO), %xmm13
	addps	%xmm14, %xmm7
	movaps	44 * SIZE(AO), %xmm14

	addq	$32 * SIZE, AO
	addq	$64 * SIZE, BO
	decq	%rax
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
	je .L18
	ALIGN_4
.L16:
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm0
	movaps	 4 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm1
	movaps	 8 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	mulps	12 * SIZE(BO), %xmm8
	addps	%xmm9, %xmm2
	movaps	 0 * SIZE(BO), %xmm9
	addps	%xmm8, %xmm3
	movaps	 8 * SIZE(AO), %xmm8
	mulps	%xmm10, %xmm9
	addps	%xmm9, %xmm4
	movaps	 4 * SIZE(BO), %xmm9
	mulps	%xmm10, %xmm9
	addps	%xmm9, %xmm5
	movaps	 8 * SIZE(BO), %xmm9
	mulps	%xmm10, %xmm9
	mulps	12 * SIZE(BO), %xmm10
	addps	%xmm9, %xmm6
	movaps	16 * SIZE(BO), %xmm9
	addps	%xmm10, %xmm7
	movaps	12 * SIZE(AO), %xmm10

	addq	$8  * SIZE, AO
	addq	$16 * SIZE, BO
	decq	%rax
	jg	.L16
	ALIGN_4

.L18:
#if defined(LN) || defined(RT)
	movq	KK, %rax
#ifdef LN
	subq	$8, %rax
#else
	subq	$4, %rax
#endif

	movq	AORIG, AO
	movq	BORIG, B
	leaq	BUFFER, BO

	salq	$2 + BASE_SHIFT, %rax
	leaq	(AO, %rax, 2), AO
	leaq	(B,  %rax, 1), B
	leaq	(BO, %rax, 4), BO
#endif

#if defined(LN) || defined(LT)
	movaps	 %xmm0,  %xmm8
	unpcklps %xmm2,  %xmm0
	unpckhps %xmm2,  %xmm8

	movaps	 %xmm1,  %xmm14
	unpcklps %xmm3,  %xmm1
	unpckhps %xmm3,  %xmm14

	movaps	 %xmm0,  %xmm2
	unpcklps %xmm1,  %xmm0
	unpckhps %xmm1,  %xmm2

	movaps	 %xmm8,  %xmm3
	unpcklps %xmm14, %xmm8
	unpckhps %xmm14, %xmm3

	movaps	 %xmm4,  %xmm9
	unpcklps %xmm6,  %xmm4
	unpckhps %xmm6,  %xmm9

	movaps	 %xmm5,  %xmm14
	unpcklps %xmm7,  %xmm5
	unpckhps %xmm7,  %xmm14

	movaps	 %xmm4,  %xmm6
	unpcklps %xmm5,  %xmm4
	unpckhps %xmm5,  %xmm6

	movaps	 %xmm9,  %xmm7
	unpcklps %xmm14, %xmm9
	unpckhps %xmm14, %xmm7

	movaps	 0 * SIZE(B), %xmm1
	movaps	 4 * SIZE(B), %xmm5
	movaps	 8 * SIZE(B), %xmm10
	movaps	12 * SIZE(B), %xmm11
	movaps	16 * SIZE(B), %xmm12
	movaps	20 * SIZE(B), %xmm13
	movaps	24 * SIZE(B), %xmm14
	movaps	28 * SIZE(B), %xmm15

	subps	%xmm0,  %xmm1
	subps	%xmm2,  %xmm5
	subps	%xmm8,  %xmm10
	subps	%xmm3,  %xmm11
	subps	%xmm4,  %xmm12
	subps	%xmm6,  %xmm13
	subps	%xmm9,  %xmm14
	subps	%xmm7,  %xmm15
#else
	movaps	 0 * SIZE(AO), %xmm8
	movaps	 4 * SIZE(AO), %xmm9
	movaps	 8 * SIZE(AO), %xmm10
	movaps	12 * SIZE(AO), %xmm11

	movaps	16 * SIZE(AO), %xmm12
	movaps	20 * SIZE(AO), %xmm13
	movaps	24 * SIZE(AO), %xmm14
	movaps	28 * SIZE(AO), %xmm15

	subps	%xmm0, %xmm8
	subps	%xmm4, %xmm9
	subps	%xmm1, %xmm10
	subps	%xmm5, %xmm11
	subps	%xmm2, %xmm12
	subps	%xmm6, %xmm13
	subps	%xmm3, %xmm14
	subps	%xmm7, %xmm15
#endif

#ifdef LN
	movaps	 60 * SIZE(AO), %xmm6
	pshufd	 $0xff, %xmm6, %xmm8
	mulps	 %xmm8, %xmm15
	pshufd	 $0xaa, %xmm6, %xmm8
	mulps	 %xmm15, %xmm8
	subps	 %xmm8, %xmm14
	pshufd	 $0x55, %xmm6, %xmm8
	mulps	 %xmm15, %xmm8
	subps	 %xmm8, %xmm13
	pshufd	 $0x00, %xmm6, %xmm8
	mulps	 %xmm15, %xmm8
	subps	 %xmm8, %xmm12

	movaps	 56 * SIZE(AO), %xmm6
	pshufd	 $0xff, %xmm6, %xmm8
	mulps	 %xmm15, %xmm8
	subps	 %xmm8, %xmm11
	pshufd	 $0xaa, %xmm6, %xmm8
	mulps	 %xmm15, %xmm8
	subps	 %xmm8, %xmm10
	pshufd	 $0x55, %xmm6, %xmm8
	mulps	 %xmm15, %xmm8
	subps	 %xmm8, %xmm5
	pshufd	 $0x00, %xmm6, %xmm8
	mulps	 %xmm15, %xmm8
	subps	 %xmm8, %xmm1

	movaps	 52 * SIZE(AO), %xmm6
	pshufd	 $0xaa, %xmm6, %xmm8
	mulps	 %xmm8, %xmm14
	pshufd	 $0x55, %xmm6, %xmm8
	mulps	 %xmm14, %xmm8
	subps	 %xmm8, %xmm13
	pshufd	 $0x00, %xmm6, %xmm8
	mulps	 %xmm14, %xmm8
	subps	 %xmm8, %xmm12

	movaps	 48 * SIZE(AO), %xmm6
	pshufd	 $0xff, %xmm6, %xmm8
	mulps	 %xmm14, %xmm8
	subps	 %xmm8, %xmm11
	pshufd	 $0xaa, %xmm6, %xmm8
	mulps	 %xmm14, %xmm8
	subps	 %xmm8, %xmm10
	pshufd	 $0x55, %xmm6, %xmm8
	mulps	 %xmm14, %xmm8
	subps	 %xmm8, %xmm5
	pshufd	 $0x00, %xmm6, %xmm8
	mulps	 %xmm14, %xmm8
	subps	 %xmm8, %xmm1

	movaps	 44 * SIZE(AO), %xmm6
	pshufd	 $0x55, %xmm6, %xmm8
	mulps	 %xmm8, %xmm13
	pshufd	 $0x00, %xmm6, %xmm8
	mulps	 %xmm13, %xmm8
	subps	 %xmm8, %xmm12

	movaps	 40 * SIZE(AO), %xmm6
	pshufd	 $0xff, %xmm6, %xmm8
	mulps	 %xmm13, %xmm8
	subps	 %xmm8, %xmm11
	pshufd	 $0xaa, %xmm6, %xmm8
	mulps	 %xmm13, %xmm8
	subps	 %xmm8, %xmm10
	pshufd	 $0x55, %xmm6, %xmm8
	mulps	 %xmm13, %xmm8
	subps	 %xmm8, %xmm5
	pshufd	 $0x00, %xmm6, %xmm8
	mulps	 %xmm13, %xmm8
	subps	 %xmm8, %xmm1

	movaps	 36 * SIZE(AO), %xmm6
	pshufd	 $0x00, %xmm6, %xmm8
	mulps	 %xmm8, %xmm12

	movaps	 32 * SIZE(AO), %xmm6
	pshufd	 $0xff, %xmm6, %xmm8
	mulps	 %xmm12, %xmm8
	subps	 %xmm8, %xmm11
	pshufd	 $0xaa, %xmm6, %xmm8
	mulps	 %xmm12, %xmm8
	subps	 %xmm8, %xmm10
	pshufd	 $0x55, %xmm6, %xmm8
	mulps	 %xmm12, %xmm8
	subps	 %xmm8, %xmm5
	pshufd	 $0x00, %xmm6, %xmm8
	mulps	 %xmm12, %xmm8
	subps	 %xmm8, %xmm1

	movaps	 24 * SIZE(AO), %xmm6
	pshufd	 $0xff, %xmm6, %xmm8
	mulps	 %xmm8, %xmm11
	pshufd	 $0xaa, %xmm6, %xmm8
	mulps	 %xmm11, %xmm8
	subps	 %xmm8, %xmm10
	pshufd	 $0x55, %xmm6, %xmm8
	mulps	 %xmm11, %xmm8
	subps	 %xmm8, %xmm5
	pshufd	 $0x00, %xmm6, %xmm8
	mulps	 %xmm11, %xmm8
	subps	 %xmm8, %xmm1

	movaps	 16 * SIZE(AO), %xmm6
	pshufd	 $0xaa, %xmm6, %xmm8
	mulps	 %xmm8, %xmm10
	pshufd	 $0x55, %xmm6, %xmm8
	mulps	 %xmm10, %xmm8
	subps	 %xmm8, %xmm5
	pshufd	 $0x00, %xmm6, %xmm8
	mulps	 %xmm10, %xmm8
	subps	 %xmm8, %xmm1

	movaps	  8 * SIZE(AO), %xmm6
	pshufd	 $0x55, %xmm6, %xmm8
	mulps	 %xmm8, %xmm5
	pshufd	 $0x00, %xmm6, %xmm8
	mulps	 %xmm5, %xmm8
	subps	 %xmm8, %xmm1

	movaps	  0 * SIZE(AO), %xmm6
	pshufd	 $0x00, %xmm6, %xmm8
	mulps	 %xmm8, %xmm1
#endif

#ifdef LT
	movaps	 0 * SIZE(AO), %xmm6

	pshufd	 $0x00, %xmm6, %xmm8
	mulps	 %xmm8, %xmm1
	pshufd	 $0x55, %xmm6, %xmm8
	mulps	 %xmm1, %xmm8
	subps	 %xmm8, %xmm5
	pshufd	 $0xaa, %xmm6, %xmm8
	mulps	 %xmm1, %xmm8
	subps	 %xmm8, %xmm10
	pshufd	 $0xff, %xmm6, %xmm8
	mulps	 %xmm1, %xmm8
	subps	 %xmm8, %xmm11

	movaps	 4 * SIZE(AO), %xmm7
	pshufd	 $0x00, %xmm7, %xmm8
	mulps	 %xmm1, %xmm8
	subps	 %xmm8, %xmm12
	pshufd	 $0x55, %xmm7, %xmm8
	mulps	 %xmm1, %xmm8
	subps	 %xmm8, %xmm13
	pshufd	 $0xaa, %xmm7, %xmm8
	mulps	 %xmm1, %xmm8
	subps	 %xmm8, %xmm14
	pshufd	 $0xff, %xmm7, %xmm8
	mulps	 %xmm1, %xmm8
	subps	 %xmm8, %xmm15

	movaps	 8 * SIZE(AO), %xmm6
	pshufd	 $0x55, %xmm6, %xmm8
	mulps	 %xmm8, %xmm5
	pshufd	 $0xaa, %xmm6, %xmm8
	mulps	 %xmm5, %xmm8
	subps	 %xmm8, %xmm10
	pshufd	 $0xff, %xmm6, %xmm8
	mulps	 %xmm5, %xmm8
	subps	 %xmm8, %xmm11

	movaps	12 * SIZE(AO), %xmm7
	pshufd	 $0x00, %xmm7, %xmm8
	mulps	 %xmm5, %xmm8
	subps	 %xmm8, %xmm12
	pshufd	 $0x55, %xmm7, %xmm8
	mulps	 %xmm5, %xmm8
	subps	 %xmm8, %xmm13
	pshufd	 $0xaa, %xmm7, %xmm8
	mulps	 %xmm5, %xmm8
	subps	 %xmm8, %xmm14
	pshufd	 $0xff, %xmm7, %xmm8
	mulps	 %xmm5, %xmm8
	subps	 %xmm8, %xmm15

	movaps	16 * SIZE(AO), %xmm6
	pshufd	 $0xaa, %xmm6, %xmm8
	mulps	 %xmm8, %xmm10
	pshufd	 $0xff, %xmm6, %xmm8
	mulps	 %xmm10, %xmm8
	subps	 %xmm8, %xmm11

	movaps	20 * SIZE(AO), %xmm7
	pshufd	 $0x00, %xmm7, %xmm8
	mulps	 %xmm10, %xmm8
	subps	 %xmm8, %xmm12
	pshufd	 $0x55, %xmm7, %xmm8
	mulps	 %xmm10, %xmm8
	subps	 %xmm8, %xmm13
	pshufd	 $0xaa, %xmm7, %xmm8
	mulps	 %xmm10, %xmm8
	subps	 %xmm8, %xmm14
	pshufd	 $0xff, %xmm7, %xmm8
	mulps	 %xmm10, %xmm8
	subps	 %xmm8, %xmm15

	movaps	24 * SIZE(AO), %xmm6
	pshufd	 $0xff, %xmm6, %xmm8
	mulps	 %xmm8, %xmm11

	movaps	28 * SIZE(AO), %xmm7
	pshufd	 $0x00, %xmm7, %xmm8
	mulps	 %xmm11, %xmm8
	subps	 %xmm8, %xmm12
	pshufd	 $0x55, %xmm7, %xmm8
	mulps	 %xmm11, %xmm8
	subps	 %xmm8, %xmm13
	pshufd	 $0xaa, %xmm7, %xmm8
	mulps	 %xmm11, %xmm8
	subps	 %xmm8, %xmm14
	pshufd	 $0xff, %xmm7, %xmm8
	mulps	 %xmm11, %xmm8
	subps	 %xmm8, %xmm15

	movaps	36 * SIZE(AO), %xmm6
	pshufd	 $0x00, %xmm6, %xmm8
	mulps	 %xmm8, %xmm12
	pshufd	 $0x55, %xmm6, %xmm8
	mulps	 %xmm12, %xmm8
	subps	 %xmm8, %xmm13
	pshufd	 $0xaa, %xmm6, %xmm8
	mulps	 %xmm12, %xmm8
	subps	 %xmm8, %xmm14
	pshufd	 $0xff, %xmm6, %xmm8
	mulps	 %xmm12, %xmm8
	subps	 %xmm8, %xmm15

	movaps	44 * SIZE(AO), %xmm7
	pshufd	 $0x55, %xmm7, %xmm8
	mulps	 %xmm8, %xmm13
	pshufd	 $0xaa, %xmm7, %xmm8
	mulps	 %xmm13, %xmm8
	subps	 %xmm8, %xmm14
	pshufd	 $0xff, %xmm7, %xmm8
	mulps	 %xmm13, %xmm8
	subps	 %xmm8, %xmm15

	movaps	52 * SIZE(AO), %xmm6
	pshufd	 $0xaa, %xmm6, %xmm8
	mulps	 %xmm8, %xmm14
	pshufd	 $0xff, %xmm6, %xmm8
	mulps	 %xmm14, %xmm8
	subps	 %xmm8, %xmm15

	movaps	60 * SIZE(AO), %xmm7
	pshufd	 $0xff, %xmm7, %xmm8
	mulps	 %xmm8, %xmm15
#endif

#ifdef RN
	movaps	 0 * SIZE(B), %xmm0
	pshufd	 $0x00, %xmm0, %xmm2
	mulps	 %xmm2, %xmm8
	mulps	 %xmm2, %xmm9
	pshufd	 $0x55, %xmm0, %xmm2
	mulps	 %xmm8, %xmm2
	subps	 %xmm2, %xmm10
	pshufd	 $0x55, %xmm0, %xmm2
	mulps	 %xmm9, %xmm2
	subps	 %xmm2, %xmm11
	pshufd	 $0xaa, %xmm0, %xmm2
	mulps	 %xmm8, %xmm2
	subps	 %xmm2, %xmm12
	pshufd	 $0xaa, %xmm0, %xmm2
	mulps	 %xmm9, %xmm2
	subps	 %xmm2, %xmm13
	pshufd	 $0xff, %xmm0, %xmm2
	mulps	 %xmm8, %xmm2
	subps	 %xmm2, %xmm14
	pshufd	 $0xff, %xmm0, %xmm2
	mulps	 %xmm9, %xmm2
	subps	 %xmm2, %xmm15

	movaps	 4 * SIZE(B), %xmm0
	pshufd	 $0x55, %xmm0, %xmm2
	mulps	 %xmm2, %xmm10
	mulps	 %xmm2, %xmm11
	pshufd	 $0xaa, %xmm0, %xmm2
	mulps	 %xmm10, %xmm2
	subps	 %xmm2, %xmm12
	pshufd	 $0xaa, %xmm0, %xmm2
	mulps	 %xmm11, %xmm2
	subps	 %xmm2, %xmm13
	pshufd	 $0xff, %xmm0, %xmm2
	mulps	 %xmm10, %xmm2
	subps	 %xmm2, %xmm14
	pshufd	 $0xff, %xmm0, %xmm2
	mulps	 %xmm11, %xmm2
	subps	 %xmm2, %xmm15

	movaps	 8 * SIZE(B), %xmm0
	pshufd	 $0xaa, %xmm0, %xmm2
	mulps	 %xmm2, %xmm12
	mulps	 %xmm2, %xmm13
	pshufd	 $0xff, %xmm0, %xmm2
	mulps	 %xmm12, %xmm2
	subps	 %xmm2, %xmm14
	pshufd	 $0xff, %xmm0, %xmm2
	mulps	 %xmm13, %xmm2
	subps	 %xmm2, %xmm15

	movaps	 12 * SIZE(B), %xmm0
	pshufd	 $0xff, %xmm0, %xmm2
	mulps	 %xmm2, %xmm14
	mulps	 %xmm2, %xmm15
#endif

#ifdef RT
	movaps	 12 * SIZE(B), %xmm0
	pshufd	 $0xff, %xmm0, %xmm2
	mulps	 %xmm2, %xmm14
	mulps	 %xmm2, %xmm15
	pshufd	 $0xaa, %xmm0, %xmm2
	mulps	 %xmm14, %xmm2
	subps	 %xmm2, %xmm12
	pshufd	 $0xaa, %xmm0, %xmm2
	mulps	 %xmm15, %xmm2
	subps	 %xmm2, %xmm13
	pshufd	 $0x55, %xmm0, %xmm2
	mulps	 %xmm14, %xmm2
	subps	 %xmm2, %xmm10
	pshufd	 $0x55, %xmm0, %xmm2
	mulps	 %xmm15, %xmm2
	subps	 %xmm2, %xmm11
	pshufd	 $0x00, %xmm0, %xmm2
	mulps	 %xmm14, %xmm2
	subps	 %xmm2, %xmm8
	pshufd	 $0x00, %xmm0, %xmm2
	mulps	 %xmm15, %xmm2
	subps	 %xmm2, %xmm9

	movaps	  8 * SIZE(B), %xmm0
	pshufd	 $0xaa, %xmm0, %xmm2
	mulps	 %xmm2, %xmm12
	mulps	 %xmm2, %xmm13
	pshufd	 $0x55, %xmm0, %xmm2
	mulps	 %xmm12, %xmm2
	subps	 %xmm2, %xmm10
	pshufd	 $0x55, %xmm0, %xmm2
	mulps	 %xmm13, %xmm2
	subps	 %xmm2, %xmm11
	pshufd	 $0x00, %xmm0, %xmm2
	mulps	 %xmm12, %xmm2
	subps	 %xmm2, %xmm8
	pshufd	 $0x00, %xmm0, %xmm2
	mulps	 %xmm13, %xmm2
	subps	 %xmm2, %xmm9

	movaps	  4 * SIZE(B), %xmm0
	pshufd	 $0x55, %xmm0, %xmm2
	mulps	 %xmm2, %xmm10
	mulps	 %xmm2, %xmm11
	pshufd	 $0x00, %xmm0, %xmm2
	mulps	 %xmm10, %xmm2
	subps	 %xmm2, %xmm8
	pshufd	 $0x00, %xmm0, %xmm2
	mulps	 %xmm11, %xmm2
	subps	 %xmm2, %xmm9

	movaps	  0 * SIZE(B), %xmm0
	pshufd	 $0x00, %xmm0, %xmm2
	mulps	 %xmm2, %xmm8
	mulps	 %xmm2, %xmm9
#endif

#ifdef LN
	subq	$8 * SIZE, CO1
	subq	$8 * SIZE, CO2
#endif

#if defined(LN) || defined(LT)
	movaps	%xmm1,   0 * SIZE(B)
	movaps	%xmm5,   4 * SIZE(B)
	movaps	%xmm10,  8 * SIZE(B)
	movaps	%xmm11, 12 * SIZE(B)
	movaps	%xmm12, 16 * SIZE(B)
	movaps	%xmm13, 20 * SIZE(B)
	movaps	%xmm14, 24 * SIZE(B)
	movaps	%xmm15, 28 * SIZE(B)

	pshufd	$0x00, %xmm1, %xmm2
	pshufd	$0x55, %xmm1, %xmm3
	pshufd	$0xaa, %xmm1, %xmm4
	pshufd	$0xff, %xmm1, %xmm6
	movaps	%xmm2,   0 * SIZE(BO)
	movaps	%xmm3,   4 * SIZE(BO)
	movaps	%xmm4,   8 * SIZE(BO)
	movaps	%xmm6,  12 * SIZE(BO)

	pshufd	$0x00, %xmm5, %xmm2
	pshufd	$0x55, %xmm5, %xmm3
	pshufd	$0xaa, %xmm5, %xmm4
	pshufd	$0xff, %xmm5, %xmm6
	movaps	%xmm2,  16 * SIZE(BO)
	movaps	%xmm3,  20 * SIZE(BO)
	movaps	%xmm4,  24 * SIZE(BO)
	movaps	%xmm6,  28 * SIZE(BO)

	pshufd	$0x00, %xmm10, %xmm2
	pshufd	$0x55, %xmm10, %xmm3
	pshufd	$0xaa, %xmm10, %xmm4
	pshufd	$0xff, %xmm10, %xmm6
	movaps	%xmm2,  32 * SIZE(BO)
	movaps	%xmm3,  36 * SIZE(BO)
	movaps	%xmm4,  40 * SIZE(BO)
	movaps	%xmm6,  44 * SIZE(BO)

	pshufd	$0x00, %xmm11, %xmm2
	pshufd	$0x55, %xmm11, %xmm3
	pshufd	$0xaa, %xmm11, %xmm4
	pshufd	$0xff, %xmm11, %xmm6
	movaps	%xmm2,  48 * SIZE(BO)
	movaps	%xmm3,  52 * SIZE(BO)
	movaps	%xmm4,  56 * SIZE(BO)
	movaps	%xmm6,  60 * SIZE(BO)

	pshufd	$0x00, %xmm12, %xmm2
	pshufd	$0x55, %xmm12, %xmm3
	pshufd	$0xaa, %xmm12, %xmm4
	pshufd	$0xff, %xmm12, %xmm6
	movaps	%xmm2,  64 * SIZE(BO)
	movaps	%xmm3,  68 * SIZE(BO)
	movaps	%xmm4,  72 * SIZE(BO)
	movaps	%xmm6,  76 * SIZE(BO)

	pshufd	$0x00, %xmm13, %xmm2
	pshufd	$0x55, %xmm13, %xmm3
	pshufd	$0xaa, %xmm13, %xmm4
	pshufd	$0xff, %xmm13, %xmm6
	movaps	%xmm2,  80 * SIZE(BO)
	movaps	%xmm3,  84 * SIZE(BO)
	movaps	%xmm4,  88 * SIZE(BO)
	movaps	%xmm6,  92 * SIZE(BO)

	pshufd	$0x00, %xmm14, %xmm2
	pshufd	$0x55, %xmm14, %xmm3
	pshufd	$0xaa, %xmm14, %xmm4
	pshufd	$0xff, %xmm14, %xmm6
	movaps	%xmm2,  96 * SIZE(BO)
	movaps	%xmm3, 100 * SIZE(BO)
	movaps	%xmm4, 104 * SIZE(BO)
	movaps	%xmm6, 108 * SIZE(BO)

	pshufd	$0x00, %xmm15, %xmm2
	pshufd	$0x55, %xmm15, %xmm3
	pshufd	$0xaa, %xmm15, %xmm4
	pshufd	$0xff, %xmm15, %xmm6
	movaps	%xmm2, 112 * SIZE(BO)
	movaps	%xmm3, 116 * SIZE(BO)
	movaps	%xmm4, 120 * SIZE(BO)
	movaps	%xmm6, 124 * SIZE(BO)

#else
	movaps	%xmm8,   0 * SIZE(AO)
	movaps	%xmm9,   4 * SIZE(AO)
	movaps	%xmm10,  8 * SIZE(AO)
	movaps	%xmm11, 12 * SIZE(AO)
	movaps	%xmm12, 16 * SIZE(AO)
	movaps	%xmm13, 20 * SIZE(AO)
	movaps	%xmm14, 24 * SIZE(AO)
	movaps	%xmm15, 28 * SIZE(AO)
#endif

#if defined(LN) || defined(LT)
	movaps	 %xmm1,  %xmm0
	unpcklps %xmm10, %xmm1
	unpckhps %xmm10, %xmm0

	movaps	 %xmm5,  %xmm7
	unpcklps %xmm11, %xmm5
	unpckhps %xmm11, %xmm7

	movaps	 %xmm1,  %xmm10
	unpcklps %xmm5,  %xmm1
	unpckhps %xmm5,  %xmm10

	movaps	 %xmm0,  %xmm11
	unpcklps %xmm7,  %xmm0
	unpckhps %xmm7,  %xmm11

	movaps	 %xmm12, %xmm2
	unpcklps %xmm14, %xmm12
	unpckhps %xmm14, %xmm2

	movaps	 %xmm13, %xmm7
	unpcklps %xmm15, %xmm13
	unpckhps %xmm15, %xmm7

	movaps	 %xmm12, %xmm14
	unpcklps %xmm13, %xmm12
	unpckhps %xmm13, %xmm14

	movaps	 %xmm2,  %xmm15
	unpcklps %xmm7,  %xmm2
	unpckhps %xmm7,  %xmm15

	movlps	%xmm1,  0 * SIZE(CO1)
	movhps	%xmm1,  2 * SIZE(CO1)
	movlps	%xmm12, 4 * SIZE(CO1)
	movhps	%xmm12, 6 * SIZE(CO1)

	movlps	%xmm10, 0 * SIZE(CO2)
	movhps	%xmm10, 2 * SIZE(CO2)
	movlps	%xmm14, 4 * SIZE(CO2)
	movhps	%xmm14, 6 * SIZE(CO2)

	movlps	%xmm0,  0 * SIZE(CO1, LDC, 2)
	movhps	%xmm0,  2 * SIZE(CO1, LDC, 2)
	movlps	%xmm2,  4 * SIZE(CO1, LDC, 2)
	movhps	%xmm2,  6 * SIZE(CO1, LDC, 2)

	movlps	%xmm11, 0 * SIZE(CO2, LDC, 2)
	movhps	%xmm11, 2 * SIZE(CO2, LDC, 2)
	movlps	%xmm15, 4 * SIZE(CO2, LDC, 2)
	movhps	%xmm15, 6 * SIZE(CO2, LDC, 2)
#else
	movlps	%xmm8,   0 * SIZE(CO1)
	movhps	%xmm8,   2 * SIZE(CO1)
	movlps	%xmm9,   4 * SIZE(CO1)
	movhps	%xmm9,   6 * SIZE(CO1)

	movlps	%xmm10,  0 * SIZE(CO2)
	movhps	%xmm10,  2 * SIZE(CO2)
	movlps	%xmm11,  4 * SIZE(CO2)
	movhps	%xmm11,  6 * SIZE(CO2)

	movlps	%xmm12,  0 * SIZE(CO1, LDC, 2)
	movhps	%xmm12,  2 * SIZE(CO1, LDC, 2)
	movlps	%xmm13,  4 * SIZE(CO1, LDC, 2)
	movhps	%xmm13,  6 * SIZE(CO1, LDC, 2)

	movlps	%xmm14,  0 * SIZE(CO2, LDC, 2)
	movhps	%xmm14,  2 * SIZE(CO2, LDC, 2)
	movlps	%xmm15,  4 * SIZE(CO2, LDC, 2)
	movhps	%xmm15,  6 * SIZE(CO2, LDC, 2)
#endif

#ifndef LN
	addq	$8 * SIZE, CO1
	addq	$8 * SIZE, CO2
#endif

#if defined(LT) || defined(RN)
	movq	K,  %rax
	subq	KK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 8), AO
#ifdef LT
	addq	$32 * SIZE, B
#endif
#endif

#ifdef LN
	subq	$8, KK
	movq	BORIG, B
#endif

#ifdef LT
	addq	$8, KK
#endif

#ifdef RT
	movq	K, %rax
	movq	BORIG, B
	salq	$3 + BASE_SHIFT, %rax
	addq	%rax, AORIG
#endif

	decq	I			# i --
	jg	.L11
	ALIGN_4

.L49:
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

.L50:
	testq	$2, N
	je	.L100

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
	salq	$1 + BASE_SHIFT, %rax
	leaq	(B,  %rax, 1), B
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
	jle	.L53
	ALIGN_4

.L52:
	movaps	 0 * SIZE(B), %xmm3
	movaps	 4 * SIZE(B), %xmm7

	pshufd	 $0x00, %xmm3, %xmm0
	pshufd	 $0x55, %xmm3, %xmm1
	pshufd	 $0xaa, %xmm3, %xmm2
	pshufd	 $0xff, %xmm3, %xmm3

	pshufd	 $0x00, %xmm7, %xmm4
	pshufd	 $0x55, %xmm7, %xmm5
	pshufd	 $0xaa, %xmm7, %xmm6
	pshufd	 $0xff, %xmm7, %xmm7

	movaps	%xmm0,  0 * SIZE(BO)
	movaps	%xmm1,  4 * SIZE(BO)
	movaps	%xmm2,  8 * SIZE(BO)
	movaps	%xmm3, 12 * SIZE(BO)
	movaps	%xmm4, 16 * SIZE(BO)
	movaps	%xmm5, 20 * SIZE(BO)
	movaps	%xmm6, 24 * SIZE(BO)
	movaps	%xmm7, 28 * SIZE(BO)

	addq	$ 8 * SIZE, B
	addq	$32 * SIZE, BO

	decq	%rax
	jne	.L52
	ALIGN_4

.L53:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$3, %rax
	BRANCH
	jle	.L60
	ALIGN_4

.L54:
	movsd	 0 * SIZE(B), %xmm3

	pshufd	 $0x00, %xmm3, %xmm0
	pshufd	 $0x55, %xmm3, %xmm1

	movaps	%xmm0,  0 * SIZE(BO)
	movaps	%xmm1,  4 * SIZE(BO)

	addq	$2 * SIZE, B
	addq	$8 * SIZE, BO
	decq	%rax
	jne	.L54
	ALIGN_4

.L60:
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
#ifndef RT
	leaq	(C, LDC, 2), C
#endif

	testq	$1, M
	je	.L70

#ifdef LN
       movq	K, %rax
       salq	$BASE_SHIFT, %rax
       subq	%rax, AORIG
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	movq	AORIG, AO
	leaq	(AO, %rax, SIZE), AO
#endif

	leaq	BUFFER, BO

#if defined(LN) || defined(RT)
	movq	KK, %rax
	salq	$1 + BASE_SHIFT, %rax
	leaq	(BO, %rax, 4), BO
#endif

	movss	 0 * SIZE(AO), %xmm8
	movss	 4 * SIZE(AO), %xmm10

	movss	 0 * SIZE(BO), %xmm9
	movss	16 * SIZE(BO), %xmm11
	movss	32 * SIZE(BO), %xmm13
	movss	48 * SIZE(BO), %xmm15

	pxor	%xmm0, %xmm0
	pxor	%xmm1, %xmm1
	pxor	%xmm2, %xmm2
	pxor	%xmm3, %xmm3

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
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm0
#if defined(OPTERON) && defined(HAVE_PREFETCH)
	PREFETCH	(PREFETCHSIZE +  0) * SIZE(AO)
#endif
	movss	 4 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	movss	 1 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm1
	movss	 8 * SIZE(BO), %xmm9

	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm2
	movss	12 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	movss	 2 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm3
	movss	64 * SIZE(BO), %xmm9

	mulps	%xmm8, %xmm11
	addps	%xmm11, %xmm0
	movss	20 * SIZE(BO), %xmm11
	mulps	%xmm8, %xmm11
	movss	 3 * SIZE(AO), %xmm8
	addps	%xmm11, %xmm1
	movss	24 * SIZE(BO), %xmm11

	mulps	%xmm8, %xmm11
	addps	%xmm11, %xmm2
	movss	28 * SIZE(BO), %xmm11
	mulps	%xmm8, %xmm11
	movss	 8 * SIZE(AO), %xmm8
	addps	%xmm11, %xmm3
	movss	 80 * SIZE(BO), %xmm11

	mulps	%xmm10, %xmm13
	addps	%xmm13, %xmm0
	movss	36 * SIZE(BO), %xmm13
	mulps	%xmm10, %xmm13
	movss	 5 * SIZE(AO), %xmm10
	addps	%xmm13, %xmm1
	movss	40 * SIZE(BO), %xmm13

	mulps	%xmm10, %xmm13
	addps	%xmm13, %xmm2
	movss	44 * SIZE(BO), %xmm13
	mulps	%xmm10, %xmm13
	movss	 6 * SIZE(AO), %xmm10
	addps	%xmm13, %xmm3
	movss	 96 * SIZE(BO), %xmm13

	mulps	%xmm10, %xmm15
	addps	%xmm15, %xmm0
	movss	52 * SIZE(BO), %xmm15
	mulps	%xmm10, %xmm15
	movss	 7 * SIZE(AO), %xmm10
	addps	%xmm15, %xmm1
	movss	56 * SIZE(BO), %xmm15

	mulps	%xmm10, %xmm15
	addps	%xmm15, %xmm2
	movss	60 * SIZE(BO), %xmm15
	mulps	%xmm10, %xmm15
	movss	12 * SIZE(AO), %xmm10
	addps	%xmm15, %xmm3
	movss	112 * SIZE(BO), %xmm15

	addq   $ 8 * SIZE, AO
	addq   $64 * SIZE, BO
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
	je .L98
	ALIGN_4

.L96:
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm0
	movss	 4 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	movss	 1 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm1
	movss	 8 * SIZE(BO), %xmm9

	addq	$1 * SIZE, AO		# aoffset  += 4
	addq	$8 * SIZE, BO		# boffset1 += 8
	decq	%rax
	jg	.L96
	ALIGN_4

.L98:
	addss	%xmm2, %xmm0
	addss	%xmm3, %xmm1

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

	salq	$ BASE_SHIFT, %rax
	leaq	(AO, %rax, 1), AO
	leaq	(B,  %rax, 2), B
	leaq	(BO, %rax, 8), BO
#endif

#if defined(LN) || defined(LT)
	unpcklps %xmm1,  %xmm0

#ifdef	movsd
	xorps	%xmm1, %xmm1
#endif
	movsd	 0 * SIZE(B), %xmm1
	subps	%xmm0,  %xmm1
#else
	movss	 0 * SIZE(AO), %xmm8
	movss	 1 * SIZE(AO), %xmm10
	subss	%xmm0, %xmm8
	subss	%xmm1, %xmm10
#endif

#if defined(LN) || defined(LT)
	movaps	  0 * SIZE(AO), %xmm6
	pshufd	 $0x00, %xmm6, %xmm8
	mulps	 %xmm8, %xmm1
#endif

#ifdef RN
	movaps	 0 * SIZE(B), %xmm0
	pshufd	 $0x00, %xmm0, %xmm2
	mulss	 %xmm2, %xmm8
	pshufd	 $0x55, %xmm0, %xmm2
	mulss	 %xmm8, %xmm2
	subss	 %xmm2, %xmm10

	pshufd	 $0xff, %xmm0, %xmm2
	mulss	 %xmm2, %xmm10
#endif

#ifdef RT
	movaps	  0 * SIZE(B), %xmm0
	pshufd	 $0xff, %xmm0, %xmm2
	mulss	 %xmm2, %xmm10
	pshufd	 $0xaa, %xmm0, %xmm2
	mulss	 %xmm10, %xmm2
	subss	 %xmm2, %xmm8
	pshufd	 $0x00, %xmm0, %xmm2
	mulss	 %xmm2, %xmm8
#endif

#ifdef LN
	subq	$1 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	movlps	%xmm1,   0 * SIZE(B)

	pshufd	$0x00, %xmm1, %xmm2
	pshufd	$0x55, %xmm1, %xmm3
	movaps	%xmm2,   0 * SIZE(BO)
	movaps	%xmm3,   4 * SIZE(BO)
#else
	movss	%xmm8,   0 * SIZE(AO)
	movss	%xmm10,  1 * SIZE(AO)
#endif

#if defined(LN) || defined(LT)
	unpcklps %xmm10, %xmm1
	unpcklps %xmm11, %xmm5

	movaps	 %xmm1,  %xmm10
	unpcklps %xmm5,  %xmm1
	unpckhps %xmm5,  %xmm10

	movss	%xmm1,   0 * SIZE(CO1)
	movss	%xmm10,  0 * SIZE(CO1, LDC, 1)
#else
	movss	%xmm8,   0 * SIZE(CO1)
	movss	%xmm10,  0 * SIZE(CO1, LDC, 1)
#endif

#ifndef LN
	addq	$1 * SIZE, CO1
#endif

#if defined(LT) || defined(RN)
	movq	K,  %rax
	subq	KK, %rax
	leaq	(AO, %rax, SIZE), AO
#ifdef LT
	addq	$ 2 * SIZE, B
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
	salq	$BASE_SHIFT, %rax
	addq	%rax, AORIG
#endif
	ALIGN_4

.L70:
	testq	$2, M
	je	.L80

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
	leaq	(BO, %rax, 4), BO
#endif

	movaps	 0 * SIZE(AO), %xmm8
	movaps	 8 * SIZE(AO), %xmm10

	movaps	 0 * SIZE(BO), %xmm9
	movaps	16 * SIZE(BO), %xmm11
	movaps	32 * SIZE(BO), %xmm13
	movaps	48 * SIZE(BO), %xmm15

	pxor	%xmm0, %xmm0
	pxor	%xmm1, %xmm1
	pxor	%xmm2, %xmm2
	pxor	%xmm3, %xmm3

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	sarq	$3, %rax
	je	.L85
	ALIGN_4

.L82:
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm0
#if defined(OPTERON) && defined(HAVE_PREFETCH)
	PREFETCH	(PREFETCHSIZE +  0) * SIZE(AO)
#endif
	movaps	 4 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	movsd	 2 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm1
	movaps	 8 * SIZE(BO), %xmm9

	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm2
	movaps	12 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	movsd	 4 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm3
	movaps	64 * SIZE(BO), %xmm9

	mulps	%xmm8, %xmm11
	addps	%xmm11, %xmm0
	movaps	20 * SIZE(BO), %xmm11
	mulps	%xmm8, %xmm11
	movsd	 6 * SIZE(AO), %xmm8
	addps	%xmm11, %xmm1
	movaps	24 * SIZE(BO), %xmm11

	mulps	%xmm8, %xmm11
	addps	%xmm11, %xmm2
	movaps	28 * SIZE(BO), %xmm11
	mulps	%xmm8, %xmm11
	movsd	16 * SIZE(AO), %xmm8
	addps	%xmm11, %xmm3
	movaps	 80 * SIZE(BO), %xmm11

	mulps	%xmm10, %xmm13
	addps	%xmm13, %xmm0
	movaps	36 * SIZE(BO), %xmm13
	mulps	%xmm10, %xmm13
	movsd	10 * SIZE(AO), %xmm10
	addps	%xmm13, %xmm1
	movaps	40 * SIZE(BO), %xmm13

	mulps	%xmm10, %xmm13
	addps	%xmm13, %xmm2
	movaps	44 * SIZE(BO), %xmm13
	mulps	%xmm10, %xmm13
	movsd	12 * SIZE(AO), %xmm10
	addps	%xmm13, %xmm3
	movaps	 96 * SIZE(BO), %xmm13

	mulps	%xmm10, %xmm15
	addps	%xmm15, %xmm0
	movaps	52 * SIZE(BO), %xmm15
	mulps	%xmm10, %xmm15
	movsd	14 * SIZE(AO), %xmm10
	addps	%xmm15, %xmm1
	movaps	56 * SIZE(BO), %xmm15

	mulps	%xmm10, %xmm15
	addps	%xmm15, %xmm2
	movaps	60 * SIZE(BO), %xmm15
	mulps	%xmm10, %xmm15
	movsd	24 * SIZE(AO), %xmm10
	addps	%xmm15, %xmm3
	movaps	112 * SIZE(BO), %xmm15

	addq   $16 * SIZE, AO
	addq   $64 * SIZE, BO
	decq   %rax
	jne    .L82
	ALIGN_4

.L85:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L88
	ALIGN_4

.L86:
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm0
	movaps	 4 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	movsd	 2 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm1
	movaps	 8 * SIZE(BO), %xmm9

	addq	$2 * SIZE, AO		# aoffset  += 4
	addq	$8 * SIZE, BO		# boffset1 += 8
	decq	%rax
	jg	.L86
	ALIGN_4

.L88:
	addps	%xmm2, %xmm0
	addps	%xmm3, %xmm1

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

	salq	$1 + BASE_SHIFT, %rax
	leaq	(AO, %rax, 1), AO
	leaq	(B,  %rax, 1), B
	leaq	(BO, %rax, 4), BO
#endif

#if defined(LN) || defined(LT)
	unpcklps %xmm2,  %xmm0
	unpcklps %xmm3,  %xmm1

	movaps	 %xmm0,  %xmm2
	unpcklps %xmm1,  %xmm0
	unpckhps %xmm1,  %xmm2

#ifdef	movsd
	xorps	%xmm1, %xmm1
#endif
	movsd	 0 * SIZE(B), %xmm1
#ifdef	movsd
	xorps	%xmm5, %xmm5
#endif
	movsd	 2 * SIZE(B), %xmm5

	subps	%xmm0,  %xmm1
	subps	%xmm2,  %xmm5
#else
#ifdef	movsd
	xorps	%xmm8, %xmm8
#endif
	movsd	 0 * SIZE(AO), %xmm8
#ifdef	movsd
	xorps	%xmm10, %xmm10
#endif
	movsd	 2 * SIZE(AO), %xmm10

	subps	%xmm0, %xmm8
	subps	%xmm1, %xmm10
#endif

#ifdef LN
	movaps	  0 * SIZE(AO), %xmm6
	pshufd	 $0xff, %xmm6, %xmm8
	mulps	 %xmm8, %xmm5
	pshufd	 $0xaa, %xmm6, %xmm8
	mulps	 %xmm5, %xmm8
	subps	 %xmm8, %xmm1
	pshufd	 $0x00, %xmm6, %xmm8
	mulps	 %xmm8, %xmm1
#endif

#ifdef LT
	movaps	 0 * SIZE(AO), %xmm6

	pshufd	 $0x00, %xmm6, %xmm8
	mulps	 %xmm8, %xmm1
	pshufd	 $0x55, %xmm6, %xmm8
	mulps	 %xmm1, %xmm8
	subps	 %xmm8, %xmm5

	pshufd	 $0xff, %xmm6, %xmm8
	mulps	 %xmm8, %xmm5
#endif

#ifdef RN
	movaps	 0 * SIZE(B), %xmm0
	pshufd	 $0x00, %xmm0, %xmm2
	mulps	 %xmm2, %xmm8
	pshufd	 $0x55, %xmm0, %xmm2
	mulps	 %xmm8, %xmm2
	subps	 %xmm2, %xmm10

	pshufd	 $0xff, %xmm0, %xmm2
	mulps	 %xmm2, %xmm10
#endif

#ifdef RT
	movaps	  0 * SIZE(B), %xmm0
	pshufd	 $0xff, %xmm0, %xmm2
	mulps	 %xmm2, %xmm10
	pshufd	 $0xaa, %xmm0, %xmm2
	mulps	 %xmm10, %xmm2
	subps	 %xmm2, %xmm8
	pshufd	 $0x00, %xmm0, %xmm2
	mulps	 %xmm2, %xmm8
#endif

#ifdef LN
	subq	$2 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	movlps	%xmm1,   0 * SIZE(B)
	movlps	%xmm5,   2 * SIZE(B)

	pshufd	$0x00, %xmm1, %xmm2
	pshufd	$0x55, %xmm1, %xmm3
	movaps	%xmm2,   0 * SIZE(BO)
	movaps	%xmm3,   4 * SIZE(BO)

	pshufd	$0x00, %xmm5, %xmm2
	pshufd	$0x55, %xmm5, %xmm3
	movaps	%xmm2,   8 * SIZE(BO)
	movaps	%xmm3,  12 * SIZE(BO)
#else
	movlps	%xmm8,   0 * SIZE(AO)
	movlps	%xmm10,  2 * SIZE(AO)
#endif

#if defined(LN) || defined(LT)
	unpcklps %xmm10, %xmm1
	unpcklps %xmm11, %xmm5

	movaps	 %xmm1,  %xmm10
	unpcklps %xmm5,  %xmm1
	unpckhps %xmm5,  %xmm10

	movlps	%xmm1,   0 * SIZE(CO1)
	movlps	%xmm10,  0 * SIZE(CO1, LDC, 1)
#else
	movlps	%xmm8,   0 * SIZE(CO1)
	movlps	%xmm10,  0 * SIZE(CO1, LDC, 1)
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
	addq	$ 4 * SIZE, B
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

.L80:
	testq	$4, M
	je	.L90

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
	leaq	(BO, %rax, 4), BO
#endif

	movaps	 0 * SIZE(AO), %xmm8
	movaps	16 * SIZE(AO), %xmm10

	movaps	 0 * SIZE(BO), %xmm9
	movaps	16 * SIZE(BO), %xmm11
	movaps	32 * SIZE(BO), %xmm13
	movaps	48 * SIZE(BO), %xmm15

	pxor	%xmm0, %xmm0
	pxor	%xmm1, %xmm1
	pxor	%xmm2, %xmm2
	pxor	%xmm3, %xmm3

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
	mulps	%xmm8, %xmm9
#if defined(OPTERON) && defined(HAVE_PREFETCH)
	PREFETCH	(PREFETCHSIZE +  0) * SIZE(AO)
#endif

	mulps	 4 * SIZE(BO), %xmm8
	addps	%xmm9, %xmm0
	movaps	 8 * SIZE(BO), %xmm9
	addps	%xmm8, %xmm1
	movaps	 4 * SIZE(AO), %xmm8

	mulps	%xmm8, %xmm9
	mulps	12 * SIZE(BO), %xmm8
	addps	%xmm9, %xmm2
	movaps	64 * SIZE(BO), %xmm9
	addps	%xmm8, %xmm3
	movaps	 8 * SIZE(AO), %xmm8

	mulps	%xmm8, %xmm11
	mulps	20 * SIZE(BO), %xmm8
	addps	%xmm11, %xmm0
	movaps	24 * SIZE(BO), %xmm11
	addps	%xmm8, %xmm1
	movaps	12 * SIZE(AO), %xmm8

	mulps	%xmm8, %xmm11
	mulps	28 * SIZE(BO), %xmm8
	addps	%xmm11, %xmm2
	movaps	80 * SIZE(BO), %xmm11
	addps	%xmm8, %xmm3
	movaps	32 * SIZE(AO), %xmm8

	mulps	%xmm10, %xmm13
	mulps	36 * SIZE(BO), %xmm10
	addps	%xmm13, %xmm0
	movaps	40 * SIZE(BO), %xmm13
	addps	%xmm10, %xmm1
	movaps	20 * SIZE(AO), %xmm10

	mulps	%xmm10, %xmm13
	mulps	44 * SIZE(BO), %xmm10
	addps	%xmm13, %xmm2
	movaps	96 * SIZE(BO), %xmm13
	addps	%xmm10, %xmm3
	movaps	24 * SIZE(AO), %xmm10

	mulps	%xmm10, %xmm15
	mulps	52 * SIZE(BO), %xmm10
	addps	%xmm15, %xmm0
	movaps	56 * SIZE(BO), %xmm15
	addps	%xmm10, %xmm1
	movaps	28 * SIZE(AO), %xmm10

	mulps	%xmm10, %xmm15
	mulps	60 * SIZE(BO), %xmm10
	addps	%xmm15, %xmm2
	movaps	112 * SIZE(BO), %xmm15
	addps	%xmm10, %xmm3
	movaps	48 * SIZE(AO), %xmm10

	addq   $32 * SIZE, AO
	addq   $64 * SIZE, BO
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
	mulps	%xmm8, %xmm9
	mulps	 4 * SIZE(BO), %xmm8
	addps	%xmm9, %xmm0
	movaps	 8 * SIZE(BO), %xmm9
	addps	%xmm8, %xmm1
	movaps	 4 * SIZE(AO), %xmm8

	addq	$4 * SIZE, AO		# aoffset  += 4
	addq	$8 * SIZE, BO		# boffset1 += 8
	decq	%rax
	jg	.L76
	ALIGN_4

.L78:
	addps	%xmm2, %xmm0
	addps	%xmm3, %xmm1

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

	salq	$1 + BASE_SHIFT, %rax
	leaq	(AO, %rax, 2), AO
	leaq	(B,  %rax, 1), B
	leaq	(BO, %rax, 4), BO
#endif

#if defined(LN) || defined(LT)
	movaps	 %xmm0,  %xmm8
	unpcklps %xmm2,  %xmm0
	unpckhps %xmm2,  %xmm8

	movaps	 %xmm1,  %xmm14
	unpcklps %xmm3,  %xmm1
	unpckhps %xmm3,  %xmm14

	movaps	 %xmm0,  %xmm2
	unpcklps %xmm1,  %xmm0
	unpckhps %xmm1,  %xmm2

	movaps	 %xmm8,  %xmm3
	unpcklps %xmm14, %xmm8
	unpckhps %xmm14, %xmm3

#ifdef	movsd
	xorps	%xmm1, %xmm1
#endif
	movsd	 0 * SIZE(B), %xmm1
#ifdef	movsd
	xorps	%xmm5, %xmm5
#endif
	movsd	 2 * SIZE(B), %xmm5
#ifdef	movsd
	xorps	%xmm10, %xmm10
#endif
	movsd	 4 * SIZE(B), %xmm10
#ifdef	movsd
	xorps	%xmm11, %xmm11
#endif
	movsd	 6 * SIZE(B), %xmm11

	subps	%xmm0,  %xmm1
	subps	%xmm2,  %xmm5
	subps	%xmm8,  %xmm10
	subps	%xmm3,  %xmm11
#else
	movaps	 0 * SIZE(AO), %xmm8
	movaps	 4 * SIZE(AO), %xmm10

	subps	%xmm0, %xmm8
	subps	%xmm1, %xmm10
#endif

#ifdef LN
	movaps	 12 * SIZE(AO), %xmm6
	pshufd	 $0xff, %xmm6, %xmm8
	mulps	 %xmm8, %xmm11
	pshufd	 $0xaa, %xmm6, %xmm8
	mulps	 %xmm11, %xmm8
	subps	 %xmm8, %xmm10
	pshufd	 $0x55, %xmm6, %xmm8
	mulps	 %xmm11, %xmm8
	subps	 %xmm8, %xmm5
	pshufd	 $0x00, %xmm6, %xmm8
	mulps	 %xmm11, %xmm8
	subps	 %xmm8, %xmm1

	movaps	  8 * SIZE(AO), %xmm6
	pshufd	 $0xaa, %xmm6, %xmm8
	mulps	 %xmm8, %xmm10
	pshufd	 $0x55, %xmm6, %xmm8
	mulps	 %xmm10, %xmm8
	subps	 %xmm8, %xmm5
	pshufd	 $0x00, %xmm6, %xmm8
	mulps	 %xmm10, %xmm8
	subps	 %xmm8, %xmm1

	movaps	  4 * SIZE(AO), %xmm6
	pshufd	 $0x55, %xmm6, %xmm8
	mulps	 %xmm8, %xmm5
	pshufd	 $0x00, %xmm6, %xmm8
	mulps	 %xmm5, %xmm8
	subps	 %xmm8, %xmm1

	movaps	  0 * SIZE(AO), %xmm6
	pshufd	 $0x00, %xmm6, %xmm8
	mulps	 %xmm8, %xmm1
#endif

#ifdef LT
	movaps	 0 * SIZE(AO), %xmm6

	pshufd	 $0x00, %xmm6, %xmm8
	mulps	 %xmm8, %xmm1
	pshufd	 $0x55, %xmm6, %xmm8
	mulps	 %xmm1, %xmm8
	subps	 %xmm8, %xmm5
	pshufd	 $0xaa, %xmm6, %xmm8
	mulps	 %xmm1, %xmm8
	subps	 %xmm8, %xmm10
	pshufd	 $0xff, %xmm6, %xmm8
	mulps	 %xmm1, %xmm8
	subps	 %xmm8, %xmm11

	movaps	 4 * SIZE(AO), %xmm6
	pshufd	 $0x55, %xmm6, %xmm8
	mulps	 %xmm8, %xmm5
	pshufd	 $0xaa, %xmm6, %xmm8
	mulps	 %xmm5, %xmm8
	subps	 %xmm8, %xmm10
	pshufd	 $0xff, %xmm6, %xmm8
	mulps	 %xmm5, %xmm8
	subps	 %xmm8, %xmm11

	movaps	 8 * SIZE(AO), %xmm6
	pshufd	 $0xaa, %xmm6, %xmm8
	mulps	 %xmm8, %xmm10
	pshufd	 $0xff, %xmm6, %xmm8
	mulps	 %xmm10, %xmm8
	subps	 %xmm8, %xmm11

	movaps	12 * SIZE(AO), %xmm6
	pshufd	 $0xff, %xmm6, %xmm8
	mulps	 %xmm8, %xmm11
#endif

#ifdef RN
	movaps	 0 * SIZE(B), %xmm0
	pshufd	 $0x00, %xmm0, %xmm2
	mulps	 %xmm2, %xmm8
	pshufd	 $0x55, %xmm0, %xmm2
	mulps	 %xmm8, %xmm2
	subps	 %xmm2, %xmm10

	pshufd	 $0xff, %xmm0, %xmm2
	mulps	 %xmm2, %xmm10
#endif

#ifdef RT
	movaps	  0 * SIZE(B), %xmm0
	pshufd	 $0xff, %xmm0, %xmm2
	mulps	 %xmm2, %xmm10
	pshufd	 $0xaa, %xmm0, %xmm2
	mulps	 %xmm10, %xmm2
	subps	 %xmm2, %xmm8
	pshufd	 $0x00, %xmm0, %xmm2
	mulps	 %xmm2, %xmm8
#endif

#ifdef LN
	subq	$4 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	movlps	%xmm1,   0 * SIZE(B)
	movlps	%xmm5,   2 * SIZE(B)
	movlps	%xmm10,  4 * SIZE(B)
	movlps	%xmm11,  6 * SIZE(B)

	pshufd	$0x00, %xmm1, %xmm2
	pshufd	$0x55, %xmm1, %xmm3
	movaps	%xmm2,   0 * SIZE(BO)
	movaps	%xmm3,   4 * SIZE(BO)

	pshufd	$0x00, %xmm5, %xmm2
	pshufd	$0x55, %xmm5, %xmm3
	movaps	%xmm2,   8 * SIZE(BO)
	movaps	%xmm3,  12 * SIZE(BO)

	pshufd	$0x00, %xmm10, %xmm2
	pshufd	$0x55, %xmm10, %xmm3
	movaps	%xmm2,  16 * SIZE(BO)
	movaps	%xmm3,  20 * SIZE(BO)

	pshufd	$0x00, %xmm11, %xmm2
	pshufd	$0x55, %xmm11, %xmm3
	movaps	%xmm2,  24 * SIZE(BO)
	movaps	%xmm3,  28 * SIZE(BO)
#else
	movaps	%xmm8,   0 * SIZE(AO)
	movaps	%xmm10,  4 * SIZE(AO)
#endif

#if defined(LN) || defined(LT)
	unpcklps %xmm10, %xmm1
	unpcklps %xmm11, %xmm5

	movaps	 %xmm1,  %xmm10
	unpcklps %xmm5,  %xmm1
	unpckhps %xmm5,  %xmm10

	movlps	%xmm1,   0 * SIZE(CO1)
	movhps	%xmm1,   2 * SIZE(CO1)
	movlps	%xmm10,  0 * SIZE(CO1, LDC, 1)
	movhps	%xmm10,  2 * SIZE(CO1, LDC, 1)
#else
	movlps	%xmm8,   0 * SIZE(CO1)
	movhps	%xmm8,   2 * SIZE(CO1)
	movlps	%xmm10,  0 * SIZE(CO1, LDC, 1)
	movhps	%xmm10,  2 * SIZE(CO1, LDC, 1)
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
	addq	$ 8 * SIZE, B
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
	ALIGN_4

.L90:
	movq	M,  I
	sarq	$3, I	# i = (m >> 3)
	jle	.L99
	ALIGN_4

.L61:
#ifdef LN
       movq	K, %rax
       salq	$3 + BASE_SHIFT, %rax
       subq	%rax, AORIG
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	movq	AORIG, AO
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 8), AO
#endif

	leaq	BUFFER, BO

#if defined(LN) || defined(RT)
	movq	KK, %rax
	salq	$1 + BASE_SHIFT, %rax
	leaq	(BO, %rax, 4), BO
#endif

	movaps	 0 * SIZE(AO), %xmm8
	movaps	16 * SIZE(AO), %xmm10
	movaps	32 * SIZE(AO), %xmm12
	movaps	48 * SIZE(AO), %xmm14

	movaps	 0 * SIZE(BO), %xmm9
	movaps	16 * SIZE(BO), %xmm11
	movaps	32 * SIZE(BO), %xmm13
	movaps	48 * SIZE(BO), %xmm15

	pxor	%xmm0, %xmm0
	pxor	%xmm1, %xmm1

	PREFETCHW     -8 * SIZE(CO1)
	pxor	%xmm4, %xmm4
	PREFETCHW     -8 * SIZE(CO2)
	pxor	%xmm5, %xmm5

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
	mulps	%xmm8, %xmm9
#if defined(OPTERON) && defined(HAVE_PREFETCH)
	PREFETCH	(PREFETCHSIZE +  0) * SIZE(AO)
#endif
	mulps	 4 * SIZE(BO), %xmm8
	addps	%xmm9, %xmm0
	movaps	 0 * SIZE(BO), %xmm9
	addps	%xmm8, %xmm1
	movaps	 4 * SIZE(AO), %xmm8
	mulps	%xmm8, %xmm9
	mulps	 4 * SIZE(BO), %xmm8
	addps	%xmm9, %xmm4
	movaps	 8 * SIZE(BO), %xmm9
	addps	%xmm8, %xmm5
	movaps	 8 * SIZE(AO), %xmm8

	mulps	%xmm8, %xmm9
	mulps	12 * SIZE(BO), %xmm8
	addps	%xmm9, %xmm0
	movaps	 8 * SIZE(BO), %xmm9
	addps	%xmm8, %xmm1
	movaps	12 * SIZE(AO), %xmm8
	mulps	%xmm8, %xmm9
	mulps	12 * SIZE(BO), %xmm8
	addps	%xmm9, %xmm4
	movaps	64 * SIZE(BO), %xmm9
	addps	%xmm8, %xmm5
	movaps	64 * SIZE(AO), %xmm8

#if defined(OPTERON) && defined(HAVE_PREFETCH)
	PREFETCH	(PREFETCHSIZE + 16) * SIZE(AO)
#endif
	mulps	%xmm10, %xmm11
	mulps	20 * SIZE(BO), %xmm10
	addps	%xmm11, %xmm0
	movaps	16 * SIZE(BO), %xmm11
	addps	%xmm10, %xmm1
	movaps	20 * SIZE(AO), %xmm10
	mulps	%xmm10, %xmm11
	mulps	20 * SIZE(BO), %xmm10
	addps	%xmm11, %xmm4
	movaps	24 * SIZE(BO), %xmm11
	addps	%xmm10, %xmm5
	movaps	24 * SIZE(AO), %xmm10

	mulps	%xmm10, %xmm11
	mulps	28 * SIZE(BO), %xmm10
	addps	%xmm11, %xmm0
	movaps	24 * SIZE(BO), %xmm11
	addps	%xmm10, %xmm1
	movaps	28 * SIZE(AO), %xmm10
	mulps	%xmm10, %xmm11
	mulps	28 * SIZE(BO), %xmm10
	addps	%xmm11, %xmm4
	movaps	80 * SIZE(BO), %xmm11
	addps	%xmm10, %xmm5
	movaps	80 * SIZE(AO), %xmm10

#if defined(OPTERON) && defined(HAVE_PREFETCH)
	PREFETCH	(PREFETCHSIZE + 32) * SIZE(AO)
#endif
	mulps	%xmm12, %xmm13
	mulps	36 * SIZE(BO), %xmm12
	addps	%xmm13, %xmm0
	movaps	32 * SIZE(BO), %xmm13
	addps	%xmm12, %xmm1
	movaps	36 * SIZE(AO), %xmm12
	mulps	%xmm12, %xmm13
	mulps	36 * SIZE(BO), %xmm12
	addps	%xmm13, %xmm4
	movaps	40 * SIZE(BO), %xmm13
	addps	%xmm12, %xmm5
	movaps	40 * SIZE(AO), %xmm12

	mulps	%xmm12, %xmm13
	mulps	44 * SIZE(BO), %xmm12
	addps	%xmm13, %xmm0
	movaps	40 * SIZE(BO), %xmm13
	addps	%xmm12, %xmm1
	movaps	44 * SIZE(AO), %xmm12
	mulps	%xmm12, %xmm13
	mulps	44 * SIZE(BO), %xmm12
	addps	%xmm13, %xmm4
	addps	%xmm12, %xmm5
	movaps	96 * SIZE(BO), %xmm13
	movaps	96 * SIZE(AO), %xmm12

#if defined(OPTERON) && defined(HAVE_PREFETCH)
	PREFETCH	(PREFETCHSIZE + 48) * SIZE(AO)
#endif
	mulps	%xmm14, %xmm15
	mulps	52 * SIZE(BO), %xmm14
	addps	%xmm15, %xmm0
	movaps	48 * SIZE(BO), %xmm15
	addps	%xmm14, %xmm1
	movaps	52 * SIZE(AO), %xmm14
	mulps	%xmm14, %xmm15
	mulps	52 * SIZE(BO), %xmm14
	addps	%xmm15, %xmm4
	movaps	56 * SIZE(BO), %xmm15
	addps	%xmm14, %xmm5
	movaps	56 * SIZE(AO), %xmm14

	mulps	%xmm14, %xmm15
	mulps	60 * SIZE(BO), %xmm14
	addps	%xmm15, %xmm0
	movaps	56 * SIZE(BO), %xmm15
	addps	%xmm14, %xmm1
	movaps	60 * SIZE(AO), %xmm14
	mulps	%xmm14, %xmm15
	mulps	60 * SIZE(BO), %xmm14
	addps	%xmm15, %xmm4
	movaps	112 * SIZE(BO), %xmm15
	addps	%xmm14, %xmm5
	movaps	112 * SIZE(AO), %xmm14

	addq   $64 * SIZE, AO
	addq   $64 * SIZE, BO
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
	je .L68
	ALIGN_4

.L66:
	mulps	%xmm8, %xmm9
	mulps	 4 * SIZE(BO), %xmm8
	addps	%xmm9, %xmm0
	movaps	 0 * SIZE(BO), %xmm9
	addps	%xmm8, %xmm1
	movaps	 4 * SIZE(AO), %xmm8
	mulps	%xmm8, %xmm9
	mulps	 4 * SIZE(BO), %xmm8
	addps	%xmm9, %xmm4
	movaps	 8 * SIZE(BO), %xmm9
	addps	%xmm8, %xmm5
	movaps	 8 * SIZE(AO), %xmm8

	addq	$8 * SIZE, AO		# aoffset  += 4
	addq	$8 * SIZE, BO		# boffset1 += 8
	decq	%rax
	jg	.L66
	ALIGN_4

.L68:
#if defined(LN) || defined(RT)
	movq	KK, %rax
#ifdef LN
	subq	$8, %rax
#else
	subq	$2, %rax
#endif

	movq	AORIG, AO
	movq	BORIG, B
	leaq	BUFFER, BO

	salq	$1 + BASE_SHIFT, %rax
	leaq	(AO, %rax, 4), AO
	leaq	(B,  %rax, 1), B
	leaq	(BO, %rax, 4), BO
#endif

#if defined(LN) || defined(LT)
	movaps	 %xmm0,  %xmm8
	unpcklps %xmm2,  %xmm0
	unpckhps %xmm2,  %xmm8

	movaps	 %xmm1,  %xmm14
	unpcklps %xmm3,  %xmm1
	unpckhps %xmm3,  %xmm14

	movaps	 %xmm0,  %xmm2
	unpcklps %xmm1,  %xmm0
	unpckhps %xmm1,  %xmm2

	movaps	 %xmm8,  %xmm3
	unpcklps %xmm14, %xmm8
	unpckhps %xmm14, %xmm3

	movaps	 %xmm4,  %xmm9
	unpcklps %xmm6,  %xmm4
	unpckhps %xmm6,  %xmm9

	movaps	 %xmm5,  %xmm14
	unpcklps %xmm7,  %xmm5
	unpckhps %xmm7,  %xmm14

	movaps	 %xmm4,  %xmm6
	unpcklps %xmm5,  %xmm4
	unpckhps %xmm5,  %xmm6

	movaps	 %xmm9,  %xmm7
	unpcklps %xmm14, %xmm9
	unpckhps %xmm14, %xmm7

#ifdef	movsd
	xorps	%xmm1, %xmm1
#endif
	movsd	 0 * SIZE(B), %xmm1
#ifdef	movsd
	xorps	%xmm5, %xmm5
#endif
	movsd	 2 * SIZE(B), %xmm5
#ifdef	movsd
	xorps	%xmm10, %xmm10
#endif
	movsd	 4 * SIZE(B), %xmm10
#ifdef	movsd
	xorps	%xmm11, %xmm11
#endif
	movsd	 6 * SIZE(B), %xmm11
#ifdef	movsd
	xorps	%xmm12, %xmm12
#endif
	movsd	 8 * SIZE(B), %xmm12
#ifdef	movsd
	xorps	%xmm13, %xmm13
#endif
	movsd	10 * SIZE(B), %xmm13
#ifdef	movsd
	xorps	%xmm14, %xmm14
#endif
	movsd	12 * SIZE(B), %xmm14
#ifdef	movsd
	xorps	%xmm15, %xmm15
#endif
	movsd	14 * SIZE(B), %xmm15

	subps	%xmm0,  %xmm1
	subps	%xmm2,  %xmm5
	subps	%xmm8,  %xmm10
	subps	%xmm3,  %xmm11
	subps	%xmm4,  %xmm12
	subps	%xmm6,  %xmm13
	subps	%xmm9,  %xmm14
	subps	%xmm7,  %xmm15
#else
	movaps	 0 * SIZE(AO), %xmm8
	movaps	 4 * SIZE(AO), %xmm9
	movaps	 8 * SIZE(AO), %xmm10
	movaps	12 * SIZE(AO), %xmm11

	subps	%xmm0, %xmm8
	subps	%xmm4, %xmm9
	subps	%xmm1, %xmm10
	subps	%xmm5, %xmm11
#endif

#ifdef LN
	movaps	 60 * SIZE(AO), %xmm6
	pshufd	 $0xff, %xmm6, %xmm8
	mulps	 %xmm8, %xmm15
	pshufd	 $0xaa, %xmm6, %xmm8
	mulps	 %xmm15, %xmm8
	subps	 %xmm8, %xmm14
	pshufd	 $0x55, %xmm6, %xmm8
	mulps	 %xmm15, %xmm8
	subps	 %xmm8, %xmm13
	pshufd	 $0x00, %xmm6, %xmm8
	mulps	 %xmm15, %xmm8
	subps	 %xmm8, %xmm12

	movaps	 56 * SIZE(AO), %xmm6
	pshufd	 $0xff, %xmm6, %xmm8
	mulps	 %xmm15, %xmm8
	subps	 %xmm8, %xmm11
	pshufd	 $0xaa, %xmm6, %xmm8
	mulps	 %xmm15, %xmm8
	subps	 %xmm8, %xmm10
	pshufd	 $0x55, %xmm6, %xmm8
	mulps	 %xmm15, %xmm8
	subps	 %xmm8, %xmm5
	pshufd	 $0x00, %xmm6, %xmm8
	mulps	 %xmm15, %xmm8
	subps	 %xmm8, %xmm1

	movaps	 52 * SIZE(AO), %xmm6
	pshufd	 $0xaa, %xmm6, %xmm8
	mulps	 %xmm8, %xmm14
	pshufd	 $0x55, %xmm6, %xmm8
	mulps	 %xmm14, %xmm8
	subps	 %xmm8, %xmm13
	pshufd	 $0x00, %xmm6, %xmm8
	mulps	 %xmm14, %xmm8
	subps	 %xmm8, %xmm12

	movaps	 48 * SIZE(AO), %xmm6
	pshufd	 $0xff, %xmm6, %xmm8
	mulps	 %xmm14, %xmm8
	subps	 %xmm8, %xmm11
	pshufd	 $0xaa, %xmm6, %xmm8
	mulps	 %xmm14, %xmm8
	subps	 %xmm8, %xmm10
	pshufd	 $0x55, %xmm6, %xmm8
	mulps	 %xmm14, %xmm8
	subps	 %xmm8, %xmm5
	pshufd	 $0x00, %xmm6, %xmm8
	mulps	 %xmm14, %xmm8
	subps	 %xmm8, %xmm1

	movaps	 44 * SIZE(AO), %xmm6
	pshufd	 $0x55, %xmm6, %xmm8
	mulps	 %xmm8, %xmm13
	pshufd	 $0x00, %xmm6, %xmm8
	mulps	 %xmm13, %xmm8
	subps	 %xmm8, %xmm12

	movaps	 40 * SIZE(AO), %xmm6
	pshufd	 $0xff, %xmm6, %xmm8
	mulps	 %xmm13, %xmm8
	subps	 %xmm8, %xmm11
	pshufd	 $0xaa, %xmm6, %xmm8
	mulps	 %xmm13, %xmm8
	subps	 %xmm8, %xmm10
	pshufd	 $0x55, %xmm6, %xmm8
	mulps	 %xmm13, %xmm8
	subps	 %xmm8, %xmm5
	pshufd	 $0x00, %xmm6, %xmm8
	mulps	 %xmm13, %xmm8
	subps	 %xmm8, %xmm1

	movaps	 36 * SIZE(AO), %xmm6
	pshufd	 $0x00, %xmm6, %xmm8
	mulps	 %xmm8, %xmm12

	movaps	 32 * SIZE(AO), %xmm6
	pshufd	 $0xff, %xmm6, %xmm8
	mulps	 %xmm12, %xmm8
	subps	 %xmm8, %xmm11
	pshufd	 $0xaa, %xmm6, %xmm8
	mulps	 %xmm12, %xmm8
	subps	 %xmm8, %xmm10
	pshufd	 $0x55, %xmm6, %xmm8
	mulps	 %xmm12, %xmm8
	subps	 %xmm8, %xmm5
	pshufd	 $0x00, %xmm6, %xmm8
	mulps	 %xmm12, %xmm8
	subps	 %xmm8, %xmm1

	movaps	 24 * SIZE(AO), %xmm6
	pshufd	 $0xff, %xmm6, %xmm8
	mulps	 %xmm8, %xmm11
	pshufd	 $0xaa, %xmm6, %xmm8
	mulps	 %xmm11, %xmm8
	subps	 %xmm8, %xmm10
	pshufd	 $0x55, %xmm6, %xmm8
	mulps	 %xmm11, %xmm8
	subps	 %xmm8, %xmm5
	pshufd	 $0x00, %xmm6, %xmm8
	mulps	 %xmm11, %xmm8
	subps	 %xmm8, %xmm1

	movaps	 16 * SIZE(AO), %xmm6
	pshufd	 $0xaa, %xmm6, %xmm8
	mulps	 %xmm8, %xmm10
	pshufd	 $0x55, %xmm6, %xmm8
	mulps	 %xmm10, %xmm8
	subps	 %xmm8, %xmm5
	pshufd	 $0x00, %xmm6, %xmm8
	mulps	 %xmm10, %xmm8
	subps	 %xmm8, %xmm1

	movaps	  8 * SIZE(AO), %xmm6
	pshufd	 $0x55, %xmm6, %xmm8
	mulps	 %xmm8, %xmm5
	pshufd	 $0x00, %xmm6, %xmm8
	mulps	 %xmm5, %xmm8
	subps	 %xmm8, %xmm1

	movaps	  0 * SIZE(AO), %xmm6
	pshufd	 $0x00, %xmm6, %xmm8
	mulps	 %xmm8, %xmm1
#endif

#ifdef LT
	movaps	 0 * SIZE(AO), %xmm6

	pshufd	 $0x00, %xmm6, %xmm8
	mulps	 %xmm8, %xmm1
	pshufd	 $0x55, %xmm6, %xmm8
	mulps	 %xmm1, %xmm8
	subps	 %xmm8, %xmm5
	pshufd	 $0xaa, %xmm6, %xmm8
	mulps	 %xmm1, %xmm8
	subps	 %xmm8, %xmm10
	pshufd	 $0xff, %xmm6, %xmm8
	mulps	 %xmm1, %xmm8
	subps	 %xmm8, %xmm11

	movaps	 4 * SIZE(AO), %xmm7
	pshufd	 $0x00, %xmm7, %xmm8
	mulps	 %xmm1, %xmm8
	subps	 %xmm8, %xmm12
	pshufd	 $0x55, %xmm7, %xmm8
	mulps	 %xmm1, %xmm8
	subps	 %xmm8, %xmm13
	pshufd	 $0xaa, %xmm7, %xmm8
	mulps	 %xmm1, %xmm8
	subps	 %xmm8, %xmm14
	pshufd	 $0xff, %xmm7, %xmm8
	mulps	 %xmm1, %xmm8
	subps	 %xmm8, %xmm15

	movaps	 8 * SIZE(AO), %xmm6
	pshufd	 $0x55, %xmm6, %xmm8
	mulps	 %xmm8, %xmm5
	pshufd	 $0xaa, %xmm6, %xmm8
	mulps	 %xmm5, %xmm8
	subps	 %xmm8, %xmm10
	pshufd	 $0xff, %xmm6, %xmm8
	mulps	 %xmm5, %xmm8
	subps	 %xmm8, %xmm11

	movaps	12 * SIZE(AO), %xmm7
	pshufd	 $0x00, %xmm7, %xmm8
	mulps	 %xmm5, %xmm8
	subps	 %xmm8, %xmm12
	pshufd	 $0x55, %xmm7, %xmm8
	mulps	 %xmm5, %xmm8
	subps	 %xmm8, %xmm13
	pshufd	 $0xaa, %xmm7, %xmm8
	mulps	 %xmm5, %xmm8
	subps	 %xmm8, %xmm14
	pshufd	 $0xff, %xmm7, %xmm8
	mulps	 %xmm5, %xmm8
	subps	 %xmm8, %xmm15

	movaps	16 * SIZE(AO), %xmm6
	pshufd	 $0xaa, %xmm6, %xmm8
	mulps	 %xmm8, %xmm10
	pshufd	 $0xff, %xmm6, %xmm8
	mulps	 %xmm10, %xmm8
	subps	 %xmm8, %xmm11

	movaps	20 * SIZE(AO), %xmm7
	pshufd	 $0x00, %xmm7, %xmm8
	mulps	 %xmm10, %xmm8
	subps	 %xmm8, %xmm12
	pshufd	 $0x55, %xmm7, %xmm8
	mulps	 %xmm10, %xmm8
	subps	 %xmm8, %xmm13
	pshufd	 $0xaa, %xmm7, %xmm8
	mulps	 %xmm10, %xmm8
	subps	 %xmm8, %xmm14
	pshufd	 $0xff, %xmm7, %xmm8
	mulps	 %xmm10, %xmm8
	subps	 %xmm8, %xmm15

	movaps	24 * SIZE(AO), %xmm6
	pshufd	 $0xff, %xmm6, %xmm8
	mulps	 %xmm8, %xmm11

	movaps	28 * SIZE(AO), %xmm7
	pshufd	 $0x00, %xmm7, %xmm8
	mulps	 %xmm11, %xmm8
	subps	 %xmm8, %xmm12
	pshufd	 $0x55, %xmm7, %xmm8
	mulps	 %xmm11, %xmm8
	subps	 %xmm8, %xmm13
	pshufd	 $0xaa, %xmm7, %xmm8
	mulps	 %xmm11, %xmm8
	subps	 %xmm8, %xmm14
	pshufd	 $0xff, %xmm7, %xmm8
	mulps	 %xmm11, %xmm8
	subps	 %xmm8, %xmm15

	movaps	36 * SIZE(AO), %xmm6
	pshufd	 $0x00, %xmm6, %xmm8
	mulps	 %xmm8, %xmm12
	pshufd	 $0x55, %xmm6, %xmm8
	mulps	 %xmm12, %xmm8
	subps	 %xmm8, %xmm13
	pshufd	 $0xaa, %xmm6, %xmm8
	mulps	 %xmm12, %xmm8
	subps	 %xmm8, %xmm14
	pshufd	 $0xff, %xmm6, %xmm8
	mulps	 %xmm12, %xmm8
	subps	 %xmm8, %xmm15

	movaps	44 * SIZE(AO), %xmm7
	pshufd	 $0x55, %xmm7, %xmm8
	mulps	 %xmm8, %xmm13
	pshufd	 $0xaa, %xmm7, %xmm8
	mulps	 %xmm13, %xmm8
	subps	 %xmm8, %xmm14
	pshufd	 $0xff, %xmm7, %xmm8
	mulps	 %xmm13, %xmm8
	subps	 %xmm8, %xmm15

	movaps	52 * SIZE(AO), %xmm6
	pshufd	 $0xaa, %xmm6, %xmm8
	mulps	 %xmm8, %xmm14
	pshufd	 $0xff, %xmm6, %xmm8
	mulps	 %xmm14, %xmm8
	subps	 %xmm8, %xmm15

	movaps	60 * SIZE(AO), %xmm7
	pshufd	 $0xff, %xmm7, %xmm8
	mulps	 %xmm8, %xmm15
#endif

#ifdef RN
	movaps	 0 * SIZE(B), %xmm0
	pshufd	 $0x00, %xmm0, %xmm2
	mulps	 %xmm2, %xmm8
	mulps	 %xmm2, %xmm9
	pshufd	 $0x55, %xmm0, %xmm2
	mulps	 %xmm8, %xmm2
	subps	 %xmm2, %xmm10
	pshufd	 $0x55, %xmm0, %xmm2
	mulps	 %xmm9, %xmm2
	subps	 %xmm2, %xmm11

	pshufd	 $0xff, %xmm0, %xmm2
	mulps	 %xmm2, %xmm10
	mulps	 %xmm2, %xmm11
#endif

#ifdef RT
	movaps	  0 * SIZE(B), %xmm0
	pshufd	 $0xff, %xmm0, %xmm2
	mulps	 %xmm2, %xmm10
	mulps	 %xmm2, %xmm11
	pshufd	 $0xaa, %xmm0, %xmm2
	mulps	 %xmm10, %xmm2
	subps	 %xmm2, %xmm8
	pshufd	 $0xaa, %xmm0, %xmm2
	mulps	 %xmm11, %xmm2
	subps	 %xmm2, %xmm9
	pshufd	 $0x00, %xmm0, %xmm2
	mulps	 %xmm2, %xmm8
	mulps	 %xmm2, %xmm9
#endif

#ifdef LN
	subq	$8 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	movlps	%xmm1,   0 * SIZE(B)
	movlps	%xmm5,   2 * SIZE(B)
	movlps	%xmm10,  4 * SIZE(B)
	movlps	%xmm11,  6 * SIZE(B)
	movlps	%xmm12,  8 * SIZE(B)
	movlps	%xmm13, 10 * SIZE(B)
	movlps	%xmm14, 12 * SIZE(B)
	movlps	%xmm15, 14 * SIZE(B)

	pshufd	$0x00, %xmm1, %xmm2
	pshufd	$0x55, %xmm1, %xmm3
	movaps	%xmm2,   0 * SIZE(BO)
	movaps	%xmm3,   4 * SIZE(BO)

	pshufd	$0x00, %xmm5, %xmm2
	pshufd	$0x55, %xmm5, %xmm3
	movaps	%xmm2,   8 * SIZE(BO)
	movaps	%xmm3,  12 * SIZE(BO)

	pshufd	$0x00, %xmm10, %xmm2
	pshufd	$0x55, %xmm10, %xmm3
	movaps	%xmm2,  16 * SIZE(BO)
	movaps	%xmm3,  20 * SIZE(BO)

	pshufd	$0x00, %xmm11, %xmm2
	pshufd	$0x55, %xmm11, %xmm3
	movaps	%xmm2,  24 * SIZE(BO)
	movaps	%xmm3,  28 * SIZE(BO)

	pshufd	$0x00, %xmm12, %xmm2
	pshufd	$0x55, %xmm12, %xmm3
	movaps	%xmm2,  32 * SIZE(BO)
	movaps	%xmm3,  36 * SIZE(BO)

	pshufd	$0x00, %xmm13, %xmm2
	pshufd	$0x55, %xmm13, %xmm3
	movaps	%xmm2,  40 * SIZE(BO)
	movaps	%xmm3,  44 * SIZE(BO)

	pshufd	$0x00, %xmm14, %xmm2
	pshufd	$0x55, %xmm14, %xmm3
	movaps	%xmm2,  48 * SIZE(BO)
	movaps	%xmm3,  52 * SIZE(BO)

	pshufd	$0x00, %xmm15, %xmm2
	pshufd	$0x55, %xmm15, %xmm3
	movaps	%xmm2,  56 * SIZE(BO)
	movaps	%xmm3,  60 * SIZE(BO)
#else
	movaps	%xmm8,   0 * SIZE(AO)
	movaps	%xmm9,   4 * SIZE(AO)
	movaps	%xmm10,  8 * SIZE(AO)
	movaps	%xmm11, 12 * SIZE(AO)
#endif

#if defined(LN) || defined(LT)
	unpcklps %xmm10, %xmm1
	unpcklps %xmm11, %xmm5

	movaps	 %xmm1,  %xmm10
	unpcklps %xmm5,  %xmm1
	unpckhps %xmm5,  %xmm10

	unpcklps %xmm14, %xmm12
	unpcklps %xmm15, %xmm13

	movaps	 %xmm12, %xmm14
	unpcklps %xmm13, %xmm12
	unpckhps %xmm13, %xmm14

	movlps	%xmm1,  0 * SIZE(CO1)
	movhps	%xmm1,  2 * SIZE(CO1)
	movlps	%xmm12, 4 * SIZE(CO1)
	movhps	%xmm12, 6 * SIZE(CO1)

	movlps	%xmm10, 0 * SIZE(CO1, LDC, 1)
	movhps	%xmm10, 2 * SIZE(CO1, LDC, 1)
	movlps	%xmm14, 4 * SIZE(CO1, LDC, 1)
	movhps	%xmm14, 6 * SIZE(CO1, LDC, 1)
#else
	movlps	%xmm8,   0 * SIZE(CO1)
	movhps	%xmm8,   2 * SIZE(CO1)
	movlps	%xmm9,   4 * SIZE(CO1)
	movhps	%xmm9,   6 * SIZE(CO1)

	movlps	%xmm10,  0 * SIZE(CO1, LDC, 1)
	movhps	%xmm10,  2 * SIZE(CO1, LDC, 1)
	movlps	%xmm11,  4 * SIZE(CO1, LDC, 1)
	movhps	%xmm11,  6 * SIZE(CO1, LDC, 1)
#endif

#ifndef LN
	addq	$8 * SIZE, CO1
#endif

#if defined(LT) || defined(RN)
	movq	K,  %rax
	subq	KK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 8), AO
#ifdef LT
	addq	$16 * SIZE, B
#endif
#endif

#ifdef LN
	subq	$8, KK
	movq	BORIG, B
#endif

#ifdef LT
	addq	$8, KK
#endif

#ifdef RT
	movq	K, %rax
	movq	BORIG, B
	salq	$3 + BASE_SHIFT, %rax
	addq	%rax, AORIG
#endif

	decq	I			# i --
	jg	.L61
	ALIGN_4

.L99:
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

.L100:
	testq	$1, N
	je	.L999

#ifdef LN
	movq	OFFSET, %rax
	addq	M, %rax
	movq	%rax, KK
#endif

	leaq	BUFFER, BO

#ifdef RT
       movq	K, %rax
       salq	$BASE_SHIFT, %rax
       subq	%rax, B
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	movq	B, BORIG
	salq	$BASE_SHIFT, %rax
	leaq	(B,  %rax, 1), B
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
	sarq	$3, %rax
	jle	.L103
	ALIGN_4

.L102:
	movsd	 0 * SIZE(B), %xmm3
	movhps	 2 * SIZE(B), %xmm3
	movsd	 4 * SIZE(B), %xmm7
	movhps	 6 * SIZE(B), %xmm7

	pshufd	 $0x00, %xmm3, %xmm0
	pshufd	 $0x55, %xmm3, %xmm1
	pshufd	 $0xaa, %xmm3, %xmm2
	pshufd	 $0xff, %xmm3, %xmm3

	pshufd	 $0x00, %xmm7, %xmm4
	pshufd	 $0x55, %xmm7, %xmm5
	pshufd	 $0xaa, %xmm7, %xmm6
	pshufd	 $0xff, %xmm7, %xmm7

	movaps	%xmm0,  0 * SIZE(BO)
	movaps	%xmm1,  4 * SIZE(BO)
	movaps	%xmm2,  8 * SIZE(BO)
	movaps	%xmm3, 12 * SIZE(BO)
	movaps	%xmm4, 16 * SIZE(BO)
	movaps	%xmm5, 20 * SIZE(BO)
	movaps	%xmm6, 24 * SIZE(BO)
	movaps	%xmm7, 28 * SIZE(BO)

	addq	$ 8 * SIZE, B
	addq	$32 * SIZE, BO

	decq	%rax
	jne	.L102
	ALIGN_4

.L103:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$7, %rax
	BRANCH
	jle	.L110
	ALIGN_4

.L104:
	movss	 0 * SIZE(B), %xmm3

	pshufd	 $0x00, %xmm3, %xmm0

	movaps	%xmm0,  0 * SIZE(BO)

	addq	$ 1 * SIZE, B
	addq	$ 4 * SIZE, BO
	decq	%rax
	jne	.L104
	ALIGN_4

.L110:
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
	je	.L120

#ifdef LN
       movq	K, %rax
       salq	$BASE_SHIFT, %rax
       subq	%rax, AORIG
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	movq	AORIG, AO
	leaq	(AO, %rax, SIZE), AO
#endif

	leaq	BUFFER, BO

#if defined(LN) || defined(RT)
	movq	KK, %rax
	salq	$BASE_SHIFT, %rax
	leaq	(BO, %rax, 4), BO
#endif

	movss	 0 * SIZE(AO), %xmm8
	movss	 4 * SIZE(AO), %xmm10

	movss	 0 * SIZE(BO), %xmm9
	movss	16 * SIZE(BO), %xmm11

	pxor	%xmm0, %xmm0
	pxor	%xmm1, %xmm1
	pxor	%xmm2, %xmm2
	pxor	%xmm3, %xmm3

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	sarq	$3, %rax
	je	.L145
	ALIGN_4

.L142:
	mulss	%xmm8, %xmm9
#if defined(OPTERON) && defined(HAVE_PREFETCH)
	PREFETCH	(PREFETCHSIZE +  0) * SIZE(AO)
#endif
	movss	 1 * SIZE(AO), %xmm8
	mulss	 4 * SIZE(BO), %xmm8
	addss	%xmm9, %xmm0
	movss	32 * SIZE(BO), %xmm9
	addss	%xmm8, %xmm1
	movss	 2 * SIZE(AO), %xmm8
	mulss	 8 * SIZE(BO), %xmm8
	addss	%xmm8, %xmm2
	movss	 3 * SIZE(AO), %xmm8
	mulss	12 * SIZE(BO), %xmm8
	addss	%xmm8, %xmm3
	movss	 8 * SIZE(AO), %xmm8
	mulss	%xmm10, %xmm11
	movss	 5 * SIZE(AO), %xmm10
	mulss	20 * SIZE(BO), %xmm10
	addss	%xmm11, %xmm0
	movss	48 * SIZE(BO), %xmm11
	addss	%xmm10, %xmm1
	movss	 6 * SIZE(AO), %xmm10
	mulss	24 * SIZE(BO), %xmm10
	addss	%xmm10, %xmm2
	movss	 7 * SIZE(AO), %xmm10
	mulss	28 * SIZE(BO), %xmm10
	addss	%xmm10, %xmm3
	movss	12 * SIZE(AO), %xmm10

	addq   $ 8 * SIZE, AO
	addq   $32 * SIZE, BO
	decq   %rax
	jne    .L142
	ALIGN_4

.L145:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L148
	ALIGN_4

.L146:
	mulss	%xmm8, %xmm9
	movss	 1 * SIZE(AO), %xmm8
	addss	%xmm9, %xmm0
	movss	 4 * SIZE(BO), %xmm9

	addq	$1 * SIZE, AO
	addq	$4 * SIZE, BO
	decq	%rax
	jg	.L146
	ALIGN_4

.L148:
	addss	%xmm1, %xmm0
	addss	%xmm3, %xmm2
	addss	%xmm2, %xmm0

#if defined(LN) || defined(RT)
	movq	KK, %rax
	subq	$1, %rax

	movq	AORIG, AO
	movq	BORIG, B
	leaq	BUFFER, BO

	salq	$BASE_SHIFT, %rax
	leaq	(AO, %rax, 1), AO
	leaq	(B,  %rax, 1), B
	leaq	(BO, %rax, 4), BO
#endif

#if defined(LN) || defined(LT)
	movss	 0 * SIZE(B), %xmm1
	subss	%xmm0,  %xmm1
#else
	movss	 0 * SIZE(AO), %xmm8
	subps	%xmm0, %xmm8
#endif

#if defined(LN) || defined(LT)
	mulss	  0 * SIZE(AO), %xmm1
#endif

#if defined(RN) || defined(RT)
	mulss	  0 * SIZE(B),  %xmm8
#endif

#ifdef LN
	subq	$1 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	movss	%xmm1,   0 * SIZE(B)

	pshufd	$0x00, %xmm1, %xmm2
	movaps	%xmm2,   0 * SIZE(BO)
#else
	movss	%xmm8,   0 * SIZE(AO)
#endif

#if defined(LN) || defined(LT)
	movss	%xmm1,  0 * SIZE(CO1)
#else
	movss	%xmm8,  0 * SIZE(CO1)
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
	salq	$BASE_SHIFT, %rax
	addq	%rax, AORIG
#endif
	ALIGN_4

.L120:
	testq	$2, M
	je	.L130

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
	salq	$BASE_SHIFT, %rax
	leaq	(BO, %rax, 4), BO
#endif

	movaps	 0 * SIZE(AO), %xmm8
	movaps	 8 * SIZE(AO), %xmm10

	movaps	 0 * SIZE(BO), %xmm9
	movaps	16 * SIZE(BO), %xmm11

	pxor	%xmm0, %xmm0
	pxor	%xmm1, %xmm1
	pxor	%xmm2, %xmm2
	pxor	%xmm3, %xmm3

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	sarq	$3, %rax
	je	.L135
	ALIGN_4

.L132:
	mulps	%xmm8, %xmm9
#if defined(OPTERON) && defined(HAVE_PREFETCH)
	PREFETCH	(PREFETCHSIZE +  0) * SIZE(AO)
#endif
	movsd	 2 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm0
	movaps	 4 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	movsd	 4 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm1
	movaps	 8 * SIZE(BO), %xmm9

	mulps	%xmm8, %xmm9
	movsd	 6 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm0
	movaps	12 * SIZE(BO), %xmm9

	mulps	%xmm8, %xmm9
	movsd	16 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm1
	movaps	32 * SIZE(BO), %xmm9

	mulps	%xmm10, %xmm11
	movsd	10 * SIZE(AO), %xmm10
	addps	%xmm11, %xmm0
	movaps	20 * SIZE(BO), %xmm11

	mulps	%xmm10, %xmm11
	movsd	12 * SIZE(AO), %xmm10
	addps	%xmm11, %xmm1
	movaps	24 * SIZE(BO), %xmm11

	mulps	%xmm10, %xmm11
	movsd	14 * SIZE(AO), %xmm10
	addps	%xmm11, %xmm0
	movaps	28 * SIZE(BO), %xmm11

	mulps	%xmm10, %xmm11
	movsd	24 * SIZE(AO), %xmm10
	addps	%xmm11, %xmm1
	movaps	48 * SIZE(BO), %xmm11

	addq   $16 * SIZE, AO
	addq   $32 * SIZE, BO
	decq   %rax
	jne    .L132
	ALIGN_4

.L135:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L138
	ALIGN_4

.L136:
	mulps	%xmm8, %xmm9
	movsd	 2 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm0
	movaps	 4 * SIZE(BO), %xmm9

	addq	$2 * SIZE, AO		# aoffset  += 4
	addq	$4 * SIZE, BO		# boffset1 += 8
	decq	%rax
	jg	.L136
	ALIGN_4

.L138:
	addps	%xmm1,  %xmm0

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

	salq	$BASE_SHIFT, %rax
	leaq	(AO, %rax, 2), AO
	leaq	(B,  %rax, 1), B
	leaq	(BO, %rax, 4), BO
#endif

#if defined(LN) || defined(LT)
	unpcklps %xmm2,  %xmm0
	unpcklps %xmm3,  %xmm1

	movaps	 %xmm0,  %xmm2
	unpcklps %xmm1,  %xmm0
	unpckhps %xmm1,  %xmm2

	movss	 0 * SIZE(B), %xmm1
	movss	 1 * SIZE(B), %xmm5

	subss	%xmm0,  %xmm1
	subss	%xmm2,  %xmm5
#else
#ifdef	movsd
	xorps	%xmm8, %xmm8
#endif
	movsd	 0 * SIZE(AO), %xmm8

	subps	%xmm0, %xmm8
#endif

#ifdef LN
	movaps	  0 * SIZE(AO), %xmm6
	pshufd	 $0xff, %xmm6, %xmm8
	mulss	 %xmm8, %xmm5
	pshufd	 $0xaa, %xmm6, %xmm8
	mulss	 %xmm5, %xmm8
	subss	 %xmm8, %xmm1
	pshufd	 $0x00, %xmm6, %xmm8
	mulss	 %xmm8, %xmm1
#endif

#ifdef LT
	movaps	 0 * SIZE(AO), %xmm6
	pshufd	 $0x00, %xmm6, %xmm8
	mulss	 %xmm8, %xmm1
	pshufd	 $0x55, %xmm6, %xmm8
	mulss	 %xmm1, %xmm8
	subss	 %xmm8, %xmm5

	pshufd	 $0xff, %xmm6, %xmm8
	mulss	 %xmm8, %xmm5
#endif

#if defined(RN) || defined(RT)
	movss	 0 * SIZE(B), %xmm0
	pshufd	 $0x00, %xmm0, %xmm2
	mulps	 %xmm2, %xmm8
#endif

#ifdef LN
	subq	$2 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	movss	%xmm1,   0 * SIZE(B)
	movss	%xmm5,   1 * SIZE(B)

	pshufd	$0x00, %xmm1, %xmm2
	movaps	%xmm2,   0 * SIZE(BO)
	pshufd	$0x00, %xmm5, %xmm2
	movaps	%xmm2,   4 * SIZE(BO)
#else
	movlps	%xmm8,   0 * SIZE(AO)
#endif

#if defined(LN) || defined(LT)
	unpcklps %xmm10, %xmm1
	unpcklps %xmm11, %xmm5
	unpcklps %xmm5,  %xmm1

	movlps	%xmm1,  0 * SIZE(CO1)
#else
	movlps	%xmm8,  0 * SIZE(CO1)
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

.L130:
	testq	$4, M
	je	.L140

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
	salq	$BASE_SHIFT, %rax
	leaq	(BO, %rax, 4), BO
#endif

	movaps	 0 * SIZE(AO), %xmm8
	movaps	16 * SIZE(AO), %xmm10

	movaps	 0 * SIZE(BO), %xmm9
	movaps	16 * SIZE(BO), %xmm11

	pxor	%xmm0, %xmm0
	pxor	%xmm1, %xmm1
	pxor	%xmm2, %xmm2
	pxor	%xmm3, %xmm3

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	sarq	$3, %rax
	je	.L125
	ALIGN_4

.L122:
	mulps	%xmm8, %xmm9
#if defined(OPTERON) && defined(HAVE_PREFETCH)
	PREFETCH	(PREFETCHSIZE +  0) * SIZE(AO)
#endif
	movaps	 4 * SIZE(AO), %xmm8
	mulps	 4 * SIZE(BO), %xmm8
	addps	%xmm9, %xmm0
	movaps	32 * SIZE(BO), %xmm9
	addps	%xmm8, %xmm1
	movaps	 8 * SIZE(AO), %xmm8
	mulps	 8 * SIZE(BO), %xmm8
	addps	%xmm8, %xmm2
	movaps	12 * SIZE(AO), %xmm8
	mulps	12 * SIZE(BO), %xmm8
	addps	%xmm8, %xmm3
	movaps	32 * SIZE(AO), %xmm8

#if defined(OPTERON) && defined(HAVE_PREFETCH)
	PREFETCH	(PREFETCHSIZE + 16) * SIZE(AO)
#endif
	mulps	%xmm10, %xmm11
	movaps	20 * SIZE(AO), %xmm10
	mulps	20 * SIZE(BO), %xmm10
	addps	%xmm11, %xmm0
	movaps	48 * SIZE(BO), %xmm11
	addps	%xmm10, %xmm1
	movaps	24 * SIZE(AO), %xmm10
	mulps	24 * SIZE(BO), %xmm10
	addps	%xmm10, %xmm2
	movaps	28 * SIZE(AO), %xmm10
	mulps	28 * SIZE(BO), %xmm10
	addps	%xmm10, %xmm3
	movaps	48 * SIZE(AO), %xmm10

	addq   $32 * SIZE, AO
	addq   $32 * SIZE, BO
	decq   %rax
	jne    .L122
	ALIGN_4

.L125:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L128
	ALIGN_4

.L126:
	mulps	%xmm8, %xmm9
	movaps	 4 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm0
	movaps	 4 * SIZE(BO), %xmm9

	addq	$4 * SIZE, AO		# aoffset  += 4
	addq	$4 * SIZE, BO		# boffset1 += 8
	decq	%rax
	jg	.L126
	ALIGN_4

.L128:
	addps	%xmm1, %xmm0
	addps	%xmm3, %xmm2
	addps	%xmm2, %xmm0

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

	salq	$BASE_SHIFT, %rax
	leaq	(AO, %rax, 4), AO
	leaq	(B,  %rax, 1), B
	leaq	(BO, %rax, 4), BO
#endif

#if defined(LN) || defined(LT)
	movaps	 %xmm0,  %xmm8
	unpcklps %xmm2,  %xmm0
	unpckhps %xmm2,  %xmm8

	movaps	 %xmm1,  %xmm14
	unpcklps %xmm3,  %xmm1
	unpckhps %xmm3,  %xmm14

	movaps	 %xmm0,  %xmm2
	unpcklps %xmm1,  %xmm0
	unpckhps %xmm1,  %xmm2

	movaps	 %xmm8,  %xmm3
	unpcklps %xmm14, %xmm8
	unpckhps %xmm14, %xmm3

	movss	 0 * SIZE(B), %xmm1
	movss	 1 * SIZE(B), %xmm5
	movss	 2 * SIZE(B), %xmm10
	movss	 3 * SIZE(B), %xmm11

	subss	%xmm0,  %xmm1
	subss	%xmm2,  %xmm5
	subss	%xmm8,  %xmm10
	subss	%xmm3,  %xmm11
#else
	movaps	 0 * SIZE(AO), %xmm8

	subps	%xmm0, %xmm8
#endif

#ifdef LN
	movaps	 12 * SIZE(AO), %xmm6
	pshufd	 $0xff, %xmm6, %xmm8
	mulss	 %xmm8, %xmm11
	pshufd	 $0xaa, %xmm6, %xmm8
	mulss	 %xmm11, %xmm8
	subss	 %xmm8, %xmm10
	pshufd	 $0x55, %xmm6, %xmm8
	mulss	 %xmm11, %xmm8
	subss	 %xmm8, %xmm5
	pshufd	 $0x00, %xmm6, %xmm8
	mulss	 %xmm11, %xmm8
	subss	 %xmm8, %xmm1

	movaps	  8 * SIZE(AO), %xmm6
	pshufd	 $0xaa, %xmm6, %xmm8
	mulss	 %xmm8, %xmm10
	pshufd	 $0x55, %xmm6, %xmm8
	mulss	 %xmm10, %xmm8
	subss	 %xmm8, %xmm5
	pshufd	 $0x00, %xmm6, %xmm8
	mulss	 %xmm10, %xmm8
	subss	 %xmm8, %xmm1

	movaps	  4 * SIZE(AO), %xmm6
	pshufd	 $0x55, %xmm6, %xmm8
	mulss	 %xmm8, %xmm5
	pshufd	 $0x00, %xmm6, %xmm8
	mulss	 %xmm5, %xmm8
	subss	 %xmm8, %xmm1

	movaps	  0 * SIZE(AO), %xmm6
	pshufd	 $0x00, %xmm6, %xmm8
	mulss	 %xmm8, %xmm1
#endif

#ifdef LT
	movaps	 0 * SIZE(AO), %xmm6

	pshufd	 $0x00, %xmm6, %xmm8
	mulss	 %xmm8, %xmm1
	pshufd	 $0x55, %xmm6, %xmm8
	mulss	 %xmm1, %xmm8
	subss	 %xmm8, %xmm5
	pshufd	 $0xaa, %xmm6, %xmm8
	mulss	 %xmm1, %xmm8
	subss	 %xmm8, %xmm10
	pshufd	 $0xff, %xmm6, %xmm8
	mulss	 %xmm1, %xmm8
	subss	 %xmm8, %xmm11

	movaps	 4 * SIZE(AO), %xmm6
	pshufd	 $0x55, %xmm6, %xmm8
	mulss	 %xmm8, %xmm5
	pshufd	 $0xaa, %xmm6, %xmm8
	mulss	 %xmm5, %xmm8
	subss	 %xmm8, %xmm10
	pshufd	 $0xff, %xmm6, %xmm8
	mulss	 %xmm5, %xmm8
	subss	 %xmm8, %xmm11

	movaps	 8 * SIZE(AO), %xmm6
	pshufd	 $0xaa, %xmm6, %xmm8
	mulss	 %xmm8, %xmm10
	pshufd	 $0xff, %xmm6, %xmm8
	mulss	 %xmm10, %xmm8
	subss	 %xmm8, %xmm11

	movaps	12 * SIZE(AO), %xmm6
	pshufd	 $0xff, %xmm6, %xmm8
	mulss	 %xmm8, %xmm11
#endif

#if defined(RN) || defined(RT)
	movss	 0 * SIZE(B), %xmm0
	pshufd	 $0x00, %xmm0, %xmm2
	mulps	 %xmm2, %xmm8
#endif

#ifdef LN
	subq	$4 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	movss	%xmm1,   0 * SIZE(B)
	movss	%xmm5,   1 * SIZE(B)
	movss	%xmm10,  2 * SIZE(B)
	movss	%xmm11,  3 * SIZE(B)

	pshufd	$0x00, %xmm1, %xmm2
	movaps	%xmm2,   0 * SIZE(BO)
	pshufd	$0x00, %xmm5, %xmm2
	movaps	%xmm2,   4 * SIZE(BO)
	pshufd	$0x00, %xmm10, %xmm2
	movaps	%xmm2,   8 * SIZE(BO)
	pshufd	$0x00, %xmm11, %xmm2
	movaps	%xmm2,  12 * SIZE(BO)
#else
	movaps	%xmm8,   0 * SIZE(AO)
#endif

#if defined(LN) || defined(LT)
	unpcklps %xmm10, %xmm1
	unpcklps %xmm11, %xmm5
	unpcklps %xmm5,  %xmm1

	movlps	%xmm1,  0 * SIZE(CO1)
	movhps	%xmm1,  2 * SIZE(CO1)
#else
	movlps	%xmm8,   0 * SIZE(CO1)
	movhps	%xmm8,   2 * SIZE(CO1)
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
	ALIGN_4

.L140:
	movq	M,  I
	sarq	$3, I	# i = (m >> 3)
	jle	.L149
	ALIGN_4

.L111:
#ifdef LN
       movq	K, %rax
       salq	$3 + BASE_SHIFT, %rax
       subq	%rax, AORIG
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	movq	AORIG, AO
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 8), AO
#endif

	leaq	BUFFER, BO

#if defined(LN) || defined(RT)
	movq	KK, %rax
	salq	$BASE_SHIFT, %rax
	leaq	(BO, %rax, 4), BO
#endif

	movaps	 0 * SIZE(AO), %xmm8
	movaps	16 * SIZE(AO), %xmm10
	movaps	32 * SIZE(AO), %xmm12
	movaps	48 * SIZE(AO), %xmm14

	movaps	 0 * SIZE(BO), %xmm9
	movaps	16 * SIZE(BO), %xmm11
	movaps	32 * SIZE(BO), %xmm13
	movaps	48 * SIZE(BO), %xmm15

	pxor	%xmm0, %xmm0
	pxor	%xmm1, %xmm1

	PREFETCHW     -8 * SIZE(CO1)
	pxor	%xmm4, %xmm4
	pxor	%xmm5, %xmm5

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
	mulps	%xmm9, %xmm8
#if defined(OPTERON) && defined(HAVE_PREFETCH)
	PREFETCH	(PREFETCHSIZE +  0) * SIZE(AO)
#endif

	mulps	 4 * SIZE(AO), %xmm9
	addps	%xmm8, %xmm0
	movaps	 8 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm4
	movaps	 4 * SIZE(BO), %xmm9

	mulps	%xmm9, %xmm8
	mulps	12 * SIZE(AO), %xmm9
	addps	%xmm8, %xmm0
	movaps	64 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm4
	movaps	 8 * SIZE(BO), %xmm9

#if defined(OPTERON) && defined(HAVE_PREFETCH)
	PREFETCH	(PREFETCHSIZE + 16) * SIZE(AO)
#endif
	mulps	%xmm9, %xmm10
	mulps	20 * SIZE(AO), %xmm9
	addps	%xmm10, %xmm0
	movaps	24 * SIZE(AO), %xmm10
	addps	%xmm9, %xmm4
	movaps	12 * SIZE(BO), %xmm9

	mulps	%xmm9, %xmm10
	mulps	28 * SIZE(AO), %xmm9
	addps	%xmm10, %xmm0
	movaps	80 * SIZE(AO), %xmm10
	addps	%xmm9, %xmm4
	movaps	32 * SIZE(BO), %xmm9

#if defined(OPTERON) && defined(HAVE_PREFETCH)
	PREFETCH	(PREFETCHSIZE + 32) * SIZE(AO)
#endif
	mulps	%xmm11, %xmm12
	mulps	36 * SIZE(AO), %xmm11
	addps	%xmm12, %xmm0
	movaps	40 * SIZE(AO), %xmm12
	addps	%xmm11, %xmm4
	movaps	20 * SIZE(BO), %xmm11

	mulps	%xmm11, %xmm12
	mulps	44 * SIZE(AO), %xmm11
	addps	%xmm12, %xmm0
	movaps	96 * SIZE(AO), %xmm12
	addps	%xmm11, %xmm4
	movaps	24 * SIZE(BO), %xmm11

#if defined(OPTERON) && defined(HAVE_PREFETCH)
	PREFETCH	(PREFETCHSIZE + 48) * SIZE(AO)
#endif
	mulps	%xmm11, %xmm14
	mulps	52 * SIZE(AO), %xmm11
	addps	%xmm14, %xmm0
	movaps	56 * SIZE(AO), %xmm14
	addps	%xmm11, %xmm4
	movaps	28 * SIZE(BO), %xmm11

	mulps	%xmm11, %xmm14
	mulps	60 * SIZE(AO), %xmm11
	addps	%xmm14, %xmm0
	movaps	112 * SIZE(AO), %xmm14
	addps	%xmm11, %xmm4
	movaps	48 * SIZE(BO), %xmm11

	addq   $64 * SIZE, AO
	addq   $32 * SIZE, BO
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
	mulps	%xmm9, %xmm8
	mulps	 4 * SIZE(AO), %xmm9
	addps	%xmm8, %xmm0
	movaps	 8 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm4
	movaps	 4 * SIZE(BO), %xmm9

	addq	$8 * SIZE, AO		# aoffset  += 4
	addq	$4 * SIZE, BO		# boffset1 += 8
	decq	%rax
	jg	.L116
	ALIGN_4

.L118:
#if defined(LN) || defined(RT)
	movq	KK, %rax
#ifdef LN
	subq	$8, %rax
#else
	subq	$1, %rax
#endif

	movq	AORIG, AO
	movq	BORIG, B
	leaq	BUFFER, BO

	salq	$BASE_SHIFT, %rax
	leaq	(AO, %rax, 8), AO
	leaq	(B,  %rax, 1), B
	leaq	(BO, %rax, 4), BO
#endif

#if defined(LN) || defined(LT)
	movaps	 %xmm0,  %xmm8
	unpcklps %xmm2,  %xmm0
	unpckhps %xmm2,  %xmm8

	movaps	 %xmm1,  %xmm14
	unpcklps %xmm3,  %xmm1
	unpckhps %xmm3,  %xmm14

	movaps	 %xmm0,  %xmm2
	unpcklps %xmm1,  %xmm0
	unpckhps %xmm1,  %xmm2

	movaps	 %xmm8,  %xmm3
	unpcklps %xmm14, %xmm8
	unpckhps %xmm14, %xmm3

	movaps	 %xmm4,  %xmm9
	unpcklps %xmm6,  %xmm4
	unpckhps %xmm6,  %xmm9

	movaps	 %xmm5,  %xmm14
	unpcklps %xmm7,  %xmm5
	unpckhps %xmm7,  %xmm14

	movaps	 %xmm4,  %xmm6
	unpcklps %xmm5,  %xmm4
	unpckhps %xmm5,  %xmm6

	movaps	 %xmm9,  %xmm7
	unpcklps %xmm14, %xmm9
	unpckhps %xmm14, %xmm7

	movss	 0 * SIZE(B), %xmm1
	movss	 1 * SIZE(B), %xmm5
	movss	 2 * SIZE(B), %xmm10
	movss	 3 * SIZE(B), %xmm11
	movss	 4 * SIZE(B), %xmm12
	movss	 5 * SIZE(B), %xmm13
	movss	 6 * SIZE(B), %xmm14
	movss	 7 * SIZE(B), %xmm15

	subss	%xmm0,  %xmm1
	subss	%xmm2,  %xmm5
	subss	%xmm8,  %xmm10
	subss	%xmm3,  %xmm11
	subss	%xmm4,  %xmm12
	subss	%xmm6,  %xmm13
	subss	%xmm9,  %xmm14
	subss	%xmm7,  %xmm15
#else
	movaps	 0 * SIZE(AO), %xmm8
	movaps	 4 * SIZE(AO), %xmm9

	subps	%xmm0, %xmm8
	subps	%xmm4, %xmm9
#endif

#ifdef LN
	movaps	 60 * SIZE(AO), %xmm6
	pshufd	 $0xff, %xmm6, %xmm8
	mulss	 %xmm8, %xmm15
	pshufd	 $0xaa, %xmm6, %xmm8
	mulss	 %xmm15, %xmm8
	subss	 %xmm8, %xmm14
	pshufd	 $0x55, %xmm6, %xmm8
	mulss	 %xmm15, %xmm8
	subss	 %xmm8, %xmm13
	pshufd	 $0x00, %xmm6, %xmm8
	mulss	 %xmm15, %xmm8
	subss	 %xmm8, %xmm12

	movaps	 56 * SIZE(AO), %xmm6
	pshufd	 $0xff, %xmm6, %xmm8
	mulss	 %xmm15, %xmm8
	subss	 %xmm8, %xmm11
	pshufd	 $0xaa, %xmm6, %xmm8
	mulss	 %xmm15, %xmm8
	subss	 %xmm8, %xmm10
	pshufd	 $0x55, %xmm6, %xmm8
	mulss	 %xmm15, %xmm8
	subss	 %xmm8, %xmm5
	pshufd	 $0x00, %xmm6, %xmm8
	mulss	 %xmm15, %xmm8
	subss	 %xmm8, %xmm1

	movaps	 52 * SIZE(AO), %xmm6
	pshufd	 $0xaa, %xmm6, %xmm8
	mulss	 %xmm8, %xmm14
	pshufd	 $0x55, %xmm6, %xmm8
	mulss	 %xmm14, %xmm8
	subss	 %xmm8, %xmm13
	pshufd	 $0x00, %xmm6, %xmm8
	mulss	 %xmm14, %xmm8
	subss	 %xmm8, %xmm12

	movaps	 48 * SIZE(AO), %xmm6
	pshufd	 $0xff, %xmm6, %xmm8
	mulss	 %xmm14, %xmm8
	subss	 %xmm8, %xmm11
	pshufd	 $0xaa, %xmm6, %xmm8
	mulss	 %xmm14, %xmm8
	subss	 %xmm8, %xmm10
	pshufd	 $0x55, %xmm6, %xmm8
	mulss	 %xmm14, %xmm8
	subss	 %xmm8, %xmm5
	pshufd	 $0x00, %xmm6, %xmm8
	mulss	 %xmm14, %xmm8
	subss	 %xmm8, %xmm1

	movaps	 44 * SIZE(AO), %xmm6
	pshufd	 $0x55, %xmm6, %xmm8
	mulss	 %xmm8, %xmm13
	pshufd	 $0x00, %xmm6, %xmm8
	mulss	 %xmm13, %xmm8
	subss	 %xmm8, %xmm12

	movaps	 40 * SIZE(AO), %xmm6
	pshufd	 $0xff, %xmm6, %xmm8
	mulss	 %xmm13, %xmm8
	subss	 %xmm8, %xmm11
	pshufd	 $0xaa, %xmm6, %xmm8
	mulss	 %xmm13, %xmm8
	subss	 %xmm8, %xmm10
	pshufd	 $0x55, %xmm6, %xmm8
	mulss	 %xmm13, %xmm8
	subss	 %xmm8, %xmm5
	pshufd	 $0x00, %xmm6, %xmm8
	mulss	 %xmm13, %xmm8
	subss	 %xmm8, %xmm1

	movaps	 36 * SIZE(AO), %xmm6
	pshufd	 $0x00, %xmm6, %xmm8
	mulss	 %xmm8, %xmm12

	movaps	 32 * SIZE(AO), %xmm6
	pshufd	 $0xff, %xmm6, %xmm8
	mulss	 %xmm12, %xmm8
	subss	 %xmm8, %xmm11
	pshufd	 $0xaa, %xmm6, %xmm8
	mulss	 %xmm12, %xmm8
	subss	 %xmm8, %xmm10
	pshufd	 $0x55, %xmm6, %xmm8
	mulss	 %xmm12, %xmm8
	subss	 %xmm8, %xmm5
	pshufd	 $0x00, %xmm6, %xmm8
	mulss	 %xmm12, %xmm8
	subss	 %xmm8, %xmm1

	movaps	 24 * SIZE(AO), %xmm6
	pshufd	 $0xff, %xmm6, %xmm8
	mulss	 %xmm8, %xmm11
	pshufd	 $0xaa, %xmm6, %xmm8
	mulss	 %xmm11, %xmm8
	subss	 %xmm8, %xmm10
	pshufd	 $0x55, %xmm6, %xmm8
	mulss	 %xmm11, %xmm8
	subss	 %xmm8, %xmm5
	pshufd	 $0x00, %xmm6, %xmm8
	mulss	 %xmm11, %xmm8
	subss	 %xmm8, %xmm1

	movaps	 16 * SIZE(AO), %xmm6
	pshufd	 $0xaa, %xmm6, %xmm8
	mulss	 %xmm8, %xmm10
	pshufd	 $0x55, %xmm6, %xmm8
	mulss	 %xmm10, %xmm8
	subss	 %xmm8, %xmm5
	pshufd	 $0x00, %xmm6, %xmm8
	mulss	 %xmm10, %xmm8
	subss	 %xmm8, %xmm1

	movaps	  8 * SIZE(AO), %xmm6
	pshufd	 $0x55, %xmm6, %xmm8
	mulss	 %xmm8, %xmm5
	pshufd	 $0x00, %xmm6, %xmm8
	mulss	 %xmm5, %xmm8
	subss	 %xmm8, %xmm1

	movaps	  0 * SIZE(AO), %xmm6
	pshufd	 $0x00, %xmm6, %xmm8
	mulss	 %xmm8, %xmm1
#endif

#ifdef LT
	movaps	 0 * SIZE(AO), %xmm6

	pshufd	 $0x00, %xmm6, %xmm8
	mulss	 %xmm8, %xmm1
	pshufd	 $0x55, %xmm6, %xmm8
	mulss	 %xmm1, %xmm8
	subss	 %xmm8, %xmm5
	pshufd	 $0xaa, %xmm6, %xmm8
	mulss	 %xmm1, %xmm8
	subss	 %xmm8, %xmm10
	pshufd	 $0xff, %xmm6, %xmm8
	mulss	 %xmm1, %xmm8
	subss	 %xmm8, %xmm11

	movaps	 4 * SIZE(AO), %xmm7
	pshufd	 $0x00, %xmm7, %xmm8
	mulss	 %xmm1, %xmm8
	subss	 %xmm8, %xmm12
	pshufd	 $0x55, %xmm7, %xmm8
	mulss	 %xmm1, %xmm8
	subss	 %xmm8, %xmm13
	pshufd	 $0xaa, %xmm7, %xmm8
	mulss	 %xmm1, %xmm8
	subss	 %xmm8, %xmm14
	pshufd	 $0xff, %xmm7, %xmm8
	mulss	 %xmm1, %xmm8
	subss	 %xmm8, %xmm15

	movaps	 8 * SIZE(AO), %xmm6
	pshufd	 $0x55, %xmm6, %xmm8
	mulss	 %xmm8, %xmm5
	pshufd	 $0xaa, %xmm6, %xmm8
	mulss	 %xmm5, %xmm8
	subss	 %xmm8, %xmm10
	pshufd	 $0xff, %xmm6, %xmm8
	mulss	 %xmm5, %xmm8
	subss	 %xmm8, %xmm11

	movaps	12 * SIZE(AO), %xmm7
	pshufd	 $0x00, %xmm7, %xmm8
	mulss	 %xmm5, %xmm8
	subss	 %xmm8, %xmm12
	pshufd	 $0x55, %xmm7, %xmm8
	mulss	 %xmm5, %xmm8
	subss	 %xmm8, %xmm13
	pshufd	 $0xaa, %xmm7, %xmm8
	mulss	 %xmm5, %xmm8
	subss	 %xmm8, %xmm14
	pshufd	 $0xff, %xmm7, %xmm8
	mulss	 %xmm5, %xmm8
	subss	 %xmm8, %xmm15

	movaps	16 * SIZE(AO), %xmm6
	pshufd	 $0xaa, %xmm6, %xmm8
	mulss	 %xmm8, %xmm10
	pshufd	 $0xff, %xmm6, %xmm8
	mulss	 %xmm10, %xmm8
	subss	 %xmm8, %xmm11

	movaps	20 * SIZE(AO), %xmm7
	pshufd	 $0x00, %xmm7, %xmm8
	mulss	 %xmm10, %xmm8
	subss	 %xmm8, %xmm12
	pshufd	 $0x55, %xmm7, %xmm8
	mulss	 %xmm10, %xmm8
	subss	 %xmm8, %xmm13
	pshufd	 $0xaa, %xmm7, %xmm8
	mulss	 %xmm10, %xmm8
	subss	 %xmm8, %xmm14
	pshufd	 $0xff, %xmm7, %xmm8
	mulss	 %xmm10, %xmm8
	subss	 %xmm8, %xmm15

	movaps	24 * SIZE(AO), %xmm6
	pshufd	 $0xff, %xmm6, %xmm8
	mulss	 %xmm8, %xmm11

	movaps	28 * SIZE(AO), %xmm7
	pshufd	 $0x00, %xmm7, %xmm8
	mulss	 %xmm11, %xmm8
	subss	 %xmm8, %xmm12
	pshufd	 $0x55, %xmm7, %xmm8
	mulss	 %xmm11, %xmm8
	subss	 %xmm8, %xmm13
	pshufd	 $0xaa, %xmm7, %xmm8
	mulss	 %xmm11, %xmm8
	subss	 %xmm8, %xmm14
	pshufd	 $0xff, %xmm7, %xmm8
	mulss	 %xmm11, %xmm8
	subss	 %xmm8, %xmm15

	movaps	36 * SIZE(AO), %xmm6
	pshufd	 $0x00, %xmm6, %xmm8
	mulss	 %xmm8, %xmm12
	pshufd	 $0x55, %xmm6, %xmm8
	mulss	 %xmm12, %xmm8
	subss	 %xmm8, %xmm13
	pshufd	 $0xaa, %xmm6, %xmm8
	mulss	 %xmm12, %xmm8
	subss	 %xmm8, %xmm14
	pshufd	 $0xff, %xmm6, %xmm8
	mulss	 %xmm12, %xmm8
	subss	 %xmm8, %xmm15

	movaps	44 * SIZE(AO), %xmm7
	pshufd	 $0x55, %xmm7, %xmm8
	mulss	 %xmm8, %xmm13
	pshufd	 $0xaa, %xmm7, %xmm8
	mulss	 %xmm13, %xmm8
	subss	 %xmm8, %xmm14
	pshufd	 $0xff, %xmm7, %xmm8
	mulss	 %xmm13, %xmm8
	subss	 %xmm8, %xmm15

	movaps	52 * SIZE(AO), %xmm6
	pshufd	 $0xaa, %xmm6, %xmm8
	mulss	 %xmm8, %xmm14
	pshufd	 $0xff, %xmm6, %xmm8
	mulss	 %xmm14, %xmm8
	subss	 %xmm8, %xmm15

	movaps	60 * SIZE(AO), %xmm7
	pshufd	 $0xff, %xmm7, %xmm8
	mulss	 %xmm8, %xmm15
#endif

#if defined(RN) || defined(RT)
	movss	 0 * SIZE(B), %xmm0
	pshufd	 $0x00, %xmm0, %xmm2
	mulps	 %xmm2, %xmm8
	mulps	 %xmm2, %xmm9
#endif

#ifdef LN
	subq	$8 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	movss	%xmm1,   0 * SIZE(B)
	movss	%xmm5,   1 * SIZE(B)
	movss	%xmm10,  2 * SIZE(B)
	movss	%xmm11,  3 * SIZE(B)
	movss	%xmm12,  4 * SIZE(B)
	movss	%xmm13,  5 * SIZE(B)
	movss	%xmm14,  6 * SIZE(B)
	movss	%xmm15,  7 * SIZE(B)

	pshufd	$0x00, %xmm1, %xmm2
	movaps	%xmm2,   0 * SIZE(BO)
	pshufd	$0x00, %xmm5, %xmm2
	movaps	%xmm2,   4 * SIZE(BO)
	pshufd	$0x00, %xmm10, %xmm2
	movaps	%xmm2,   8 * SIZE(BO)
	pshufd	$0x00, %xmm11, %xmm2
	movaps	%xmm2,  12 * SIZE(BO)

	pshufd	$0x00, %xmm12, %xmm2
	movaps	%xmm2,  16 * SIZE(BO)
	pshufd	$0x00, %xmm13, %xmm2
	movaps	%xmm2,  20 * SIZE(BO)
	pshufd	$0x00, %xmm14, %xmm2
	movaps	%xmm2,  24 * SIZE(BO)
	pshufd	$0x00, %xmm15, %xmm2
	movaps	%xmm2,  28 * SIZE(BO)
#else
	movaps	%xmm8,   0 * SIZE(AO)
	movaps	%xmm9,   4 * SIZE(AO)
#endif

#if defined(LN) || defined(LT)
	unpcklps %xmm10, %xmm1
	unpcklps %xmm11, %xmm5
	unpcklps %xmm5,  %xmm1

	unpcklps %xmm14, %xmm12
	unpcklps %xmm15, %xmm13
	unpcklps %xmm13, %xmm12

	movlps	%xmm1,  0 * SIZE(CO1)
	movhps	%xmm1,  2 * SIZE(CO1)
	movlps	%xmm12, 4 * SIZE(CO1)
	movhps	%xmm12, 6 * SIZE(CO1)
#else
	movlps	%xmm8,   0 * SIZE(CO1)
	movhps	%xmm8,   2 * SIZE(CO1)
	movlps	%xmm9,   4 * SIZE(CO1)
	movhps	%xmm9,   6 * SIZE(CO1)
#endif

#ifndef LN
	addq	$8 * SIZE, CO1
#endif

#if defined(LT) || defined(RN)
	movq	K,  %rax
	subq	KK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 8), AO
#ifdef LT
	addq	$8 * SIZE, B
#endif
#endif

#ifdef LN
	subq	$8, KK
	movq	BORIG, B
#endif

#ifdef LT
	addq	$8, KK
#endif

#ifdef RT
	movq	K, %rax
	movq	BORIG, B
	salq	$3 + BASE_SHIFT, %rax
	addq	%rax, AORIG
#endif

	decq	I			# i --
	jg	.L111
	ALIGN_4

.L149:
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
	EMMS
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
