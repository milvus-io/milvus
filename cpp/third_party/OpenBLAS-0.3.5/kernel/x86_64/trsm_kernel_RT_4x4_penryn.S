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

#define STACKSIZE 128

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

#define PREFETCH_R    (8 *  4 + 0)
#define PREFETCHSIZE  (8 * 21 + 6)
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

	movq	OLD_M, M
	movq	OLD_N, N
	movq	OLD_K, K
	movq	OLD_LDC,    LDC
	movq	OLD_OFFSET, KK

	subq	$-16 * SIZE, A
	subq	$-16 * SIZE, B

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
	BRANCH
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
	sarq	$2, I	# i = (m >> 2)
	NOBRANCH
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
	leaq	(, %rax, SIZE), %rax
	movq	AORIG, AO
	leaq	(AO, %rax, 4), AO
	leaq	(B,  %rax, 1), BO
#else
	movq	B, BO
#endif

	movaps	-16 * SIZE(AO), %xmm0
	movaps	-14 * SIZE(AO), %xmm1
	movsd	-16 * SIZE(BO), %xmm2

#ifdef LN
	prefetcht0    -4 * SIZE(CO1)
#else
	prefetcht0     3 * SIZE(CO1)
#endif
	pxor	%xmm8,  %xmm8
	pxor	%xmm9,  %xmm9
	pxor	%xmm12, %xmm12
	pxor	%xmm13, %xmm13

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	sarq	$2, %rax
	NOBRANCH
	jle	.L95
	ALIGN_4

.L92:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)

	pshufd	$0x44, %xmm2, %xmm3
	pshufd	$0x44, %xmm2, %xmm4
	movsd	-15 * SIZE(BO), %xmm2

	mulpd	%xmm0, %xmm3
	movaps	-12 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm4
	movaps	-10 * SIZE(AO), %xmm1

	addpd	%xmm3, %xmm8
	addpd	%xmm4, %xmm12

	pshufd	$0x44, %xmm2, %xmm3
	pshufd	$0x44, %xmm2, %xmm4
	movsd	-14 * SIZE(BO), %xmm2

	mulpd	%xmm0, %xmm3
	movaps	 -8 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm4
	movaps	 -6 * SIZE(AO), %xmm1

	addpd	%xmm3, %xmm8
	addpd	%xmm4, %xmm12

	PREFETCH (PREFETCHSIZE +  8) * SIZE(AO)

	pshufd	$0x44, %xmm2, %xmm3
	pshufd	$0x44, %xmm2, %xmm4
	movsd	-13 * SIZE(BO), %xmm2

	mulpd	%xmm0, %xmm3
	movaps	 -4 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm4
	movaps	 -2 * SIZE(AO), %xmm1

	addpd	%xmm3, %xmm8
	addpd	%xmm4, %xmm12

	pshufd	$0x44, %xmm2, %xmm3
	pshufd	$0x44, %xmm2, %xmm4
	movsd	-12 * SIZE(BO), %xmm2

	mulpd	%xmm0, %xmm3
	movaps	  0 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm4
	movaps	  2 * SIZE(AO), %xmm1

	addpd	%xmm3, %xmm8
	addpd	%xmm4, %xmm12

	subq	$-16 * SIZE, AO
	subq	$ -4 * SIZE, BO
	subq	$1, %rax
	BRANCH
	jg	.L92
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
	je	.L98
	ALIGN_4

.L96:
	pshufd	$0x44, %xmm2, %xmm3
	pshufd	$0x44, %xmm2, %xmm4
	movsd	-15 * SIZE(BO), %xmm2

	mulpd	%xmm0, %xmm3
	movaps	-12 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm4
	movaps	-10 * SIZE(AO), %xmm1

	addpd	%xmm3, %xmm8
	addpd	%xmm4, %xmm12

	addq	$4 * SIZE, AO
	addq	$1 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L96
	ALIGN_4

.L98:
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
	movapd	 -16 * SIZE(BO), %xmm10
	movapd	 -14 * SIZE(BO), %xmm11

	subpd	%xmm8,   %xmm10
	subpd	%xmm12,  %xmm11
#else
	movapd	 -16 * SIZE(AO), %xmm10
	movapd	 -14 * SIZE(AO), %xmm11

	subpd	%xmm8,  %xmm10
	subpd	%xmm12, %xmm11
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
	movddup	 -16 * SIZE(BO), %xmm8
	mulpd	 %xmm8, %xmm10
	mulpd	 %xmm8, %xmm11
#endif

#ifdef RT
	movddup	 -16 * SIZE(BO), %xmm8
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
	movapd	%xmm10,  -16 * SIZE(BO)
	movapd	%xmm11,  -14 * SIZE(BO)
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
	jg	.L91
	ALIGN_4

.L100:
	testq	$2, M
	BRANCH
	jle	.L110
	ALIGN_4

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

	movaps	-16 * SIZE(AO), %xmm0
	pxor	%xmm8,  %xmm8
	movsd	-16 * SIZE(BO), %xmm2
	pxor	%xmm9,  %xmm9
	movhps	-15 * SIZE(BO), %xmm2

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	sarq	$2, %rax
	NOBRANCH
	jle	.L105
	ALIGN_4

.L102:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)

	pshufd	$0x44, %xmm2, %xmm3
	movsd	-15 * SIZE(BO), %xmm2

	mulpd	%xmm0, %xmm3
	movaps	-14 * SIZE(AO), %xmm0
	addpd	%xmm3, %xmm8

	pshufd	$0x44, %xmm2, %xmm3
	movsd	-14 * SIZE(BO), %xmm2

	mulpd	%xmm0, %xmm3
	movaps	-12 * SIZE(AO), %xmm0
	addpd	%xmm3, %xmm9

	pshufd	$0x44, %xmm2, %xmm3
	movsd	-13 * SIZE(BO), %xmm2

	mulpd	%xmm0, %xmm3
	movaps	-10 * SIZE(AO), %xmm0
	addpd	%xmm3, %xmm8

	pshufd	$0x44, %xmm2, %xmm3
	movsd	-12 * SIZE(BO), %xmm2

	mulpd	%xmm0, %xmm3
	movaps	 -8 * SIZE(AO), %xmm0
	addpd	%xmm3, %xmm9

	subq	$-8 * SIZE, AO
	subq	$-4 * SIZE, BO
	subq	$1, %rax
	BRANCH
	jg	.L102
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
	je	.L108
	ALIGN_4

.L106:
	pshufd	$0x44, %xmm2, %xmm3
	movsd	-15 * SIZE(BO), %xmm2

	mulpd	%xmm0, %xmm3
	movaps	-14 * SIZE(AO), %xmm0
	addpd	%xmm3, %xmm8

	addq	$2 * SIZE, AO
	addq	$1 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L106
	ALIGN_4

.L108:
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

	addpd	%xmm9, %xmm8

#if defined(LN) || defined(LT)
	movapd	 -16 * SIZE(BO), %xmm10
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
	movddup	-16 * SIZE(BO), %xmm8
	mulpd	 %xmm8, %xmm10
#endif

#ifdef RT
	movddup -16 * SIZE(BO), %xmm8
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
	movapd	%xmm10,  -16 * SIZE(BO)
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

.L110:
	testq	$1, M
	BRANCH
	jle	.L119

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

	movsd	-16 * SIZE(AO), %xmm0
	movsd	-16 * SIZE(BO), %xmm2

	pxor	%xmm8,  %xmm8
	pxor	%xmm9,  %xmm9

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	sarq	$2, %rax
	NOBRANCH
	jle	.L115
	ALIGN_4

.L112:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)

	mulsd	%xmm0, %xmm2
	addsd	%xmm2, %xmm8
	movsd	-15 * SIZE(AO), %xmm0
	movsd	-15 * SIZE(BO), %xmm2

	mulsd	%xmm0, %xmm2
	addsd	%xmm2, %xmm8
	movsd	-14 * SIZE(AO), %xmm0
	movsd	-14 * SIZE(BO), %xmm2

	mulsd	%xmm0, %xmm2
	addsd	%xmm2, %xmm8
	movsd	-13 * SIZE(AO), %xmm0
	movsd	-13 * SIZE(BO), %xmm2

	mulsd	%xmm0, %xmm2
	addsd	%xmm2, %xmm8
	movsd	-12 * SIZE(AO), %xmm0
	movsd	-12 * SIZE(BO), %xmm2

	subq	$-4 * SIZE, AO
	subq	$-4 * SIZE, BO
	subq	$1, %rax
	BRANCH
	jg	.L112
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
	je	.L118
	ALIGN_4

.L116:
	mulsd	%xmm0, %xmm2
	addsd	%xmm2, %xmm8
	movsd	-15 * SIZE(AO), %xmm0
	movsd	-15 * SIZE(BO), %xmm2

	addq	$1 * SIZE, AO
	addq	$1 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L116
	ALIGN_4

.L118:
#if defined(LN) || defined(RT)
	movq	KK, %rax
	subq	$1, %rax

	leaq	(, %rax, SIZE), %rax

	movq	AORIG, AO
	leaq	(AO, %rax, 1), AO
	leaq	(B,  %rax, 1), BO
#endif

	addpd	%xmm9, %xmm8


#if defined(LN) || defined(LT)
	movsd	-16 * SIZE(BO), %xmm10
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
	movsd	-16 * SIZE(BO), %xmm8
	mulsd	 %xmm8, %xmm10
#endif

#ifdef RT
	movsd	-16 * SIZE(BO), %xmm8
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
	movsd	%xmm10,   -16 * SIZE(BO)
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

.L119:
#ifdef LN
       leaq	(B, K, SIZE), B
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
	BRANCH
	jle	.L80

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
	movq	B, BB
	subq	%rax, BB

#ifdef LT
	movq	OFFSET, %rax
	movq	%rax, KK
#endif

	movq	M,  I
	sarq	$2, I	# i = (m >> 2)
	NOBRANCH
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
	leaq	(, %rax, SIZE), %rax
	movq	AORIG, AO
	leaq	(AO, %rax, 4), AO
	leaq	(B,  %rax, 2), BO
#else
	movq	B, BO
#endif

	prefetcht2	 -16 * SIZE(BB)
	subq		 $-4 * SIZE, BB

	movaps	-16 * SIZE(AO), %xmm0
	movaps	-14 * SIZE(AO), %xmm1
	movaps	-16 * SIZE(BO), %xmm2

#ifdef LN
	prefetcht0    -4 * SIZE(CO1)
	pxor	%xmm8,  %xmm8
	pxor	%xmm9,  %xmm9
	prefetcht0    -4 * SIZE(CO2)
	pxor	%xmm12, %xmm12
	pxor	%xmm13, %xmm13
#else
	prefetcht0     3 * SIZE(CO1)
	pxor	%xmm8,  %xmm8
	pxor	%xmm9,  %xmm9
	prefetcht0     3 * SIZE(CO2)
	pxor	%xmm12, %xmm12
	pxor	%xmm13, %xmm13
#endif

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	sarq	$2, %rax
	NOBRANCH
	jle	.L55
	ALIGN_4

.L52:
	movaps	%xmm2, %xmm4
	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)
	mulpd	%xmm1, %xmm4

	movaps	%xmm7, %xmm6
	mulpd	%xmm0, %xmm7
	movaps	-12 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm6
	movaps	-10 * SIZE(AO), %xmm1

	addpd	%xmm2, %xmm9
	movaps	-14 * SIZE(BO), %xmm2
	addpd	%xmm4, %xmm13
	addpd	%xmm7, %xmm8
	addpd	%xmm6, %xmm12

	movaps	%xmm2, %xmm4
	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	PREFETCH (PREFETCHSIZE +  8) * SIZE(AO)
	mulpd	%xmm1, %xmm4

	movaps	%xmm7, %xmm6
	mulpd	%xmm0, %xmm7
	movaps	 -8 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm6
	movaps	 -6 * SIZE(AO), %xmm1

	addpd	%xmm2, %xmm9
	movaps	-12 * SIZE(BO), %xmm2
	addpd	%xmm4, %xmm13
	addpd	%xmm7, %xmm8
	addpd	%xmm6, %xmm12

	movaps	%xmm2, %xmm4
	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm4

	movaps	%xmm7, %xmm6
	mulpd	%xmm0, %xmm7
	movaps	 -4 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm6
	movaps	 -2 * SIZE(AO), %xmm1

	addpd	%xmm2, %xmm9
	movaps	-10 * SIZE(BO), %xmm2
	addpd	%xmm4, %xmm13
	addpd	%xmm7, %xmm8
	addpd	%xmm6, %xmm12

	movaps	%xmm2, %xmm4
	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm4

	movaps	%xmm7, %xmm6
	mulpd	%xmm0, %xmm7
	movaps	  0 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm6
	movaps	  2 * SIZE(AO), %xmm1

	addpd	%xmm2, %xmm9
	movaps	 -8 * SIZE(BO), %xmm2
	addpd	%xmm4, %xmm13
	addpd	%xmm7, %xmm8
	addpd	%xmm6, %xmm12

	subq	$-16 * SIZE, AO
	subq	$ -8 * SIZE, BO
	subq	$1, %rax
	BRANCH
	jg	.L52
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
	je	.L58
	ALIGN_4

.L56:
	movaps	%xmm2, %xmm4
	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm4

	movaps	%xmm7, %xmm6
	mulpd	%xmm0, %xmm7
	movaps	-12 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm6
	movaps	-10 * SIZE(AO), %xmm1

	addpd	%xmm2, %xmm9
	movaps	-14 * SIZE(BO), %xmm2
	addpd	%xmm4, %xmm13
	addpd	%xmm7, %xmm8
	addpd	%xmm6, %xmm12

	addq	$4 * SIZE, AO
	addq	$2 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L56
	ALIGN_4

.L58:
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

	movapd	 %xmm8, %xmm0
	movsd    %xmm9, %xmm8
	movsd    %xmm0, %xmm9

	movapd	 %xmm12, %xmm0
	movsd    %xmm13, %xmm12
	movsd    %xmm0,  %xmm13

#if defined(LN) || defined(LT)
	movapd	%xmm8,  %xmm0
	unpcklpd %xmm9, %xmm8
	unpckhpd %xmm9, %xmm0

	movapd	%xmm12, %xmm4
	unpcklpd %xmm13, %xmm12
	unpckhpd %xmm13, %xmm4

	movapd	 -16 * SIZE(BO), %xmm9
	movapd	 -14 * SIZE(BO), %xmm13
	movapd	 -12 * SIZE(BO), %xmm1
	movapd	 -10 * SIZE(BO), %xmm5

	subpd	%xmm8,  %xmm9
	subpd	%xmm0,  %xmm13
	subpd	%xmm12, %xmm1
	subpd	%xmm4,  %xmm5
#else
	movapd	 -16 * SIZE(AO), %xmm0
	movapd	 -14 * SIZE(AO), %xmm1
	movapd	 -12 * SIZE(AO), %xmm2
	movapd	 -10 * SIZE(AO), %xmm3

	subpd	%xmm8,  %xmm0
	subpd	%xmm12, %xmm1
	subpd	%xmm9,  %xmm2
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
	movddup	-16 * SIZE(BO), %xmm8
	mulpd	 %xmm8, %xmm0
	mulpd	 %xmm8, %xmm1

	movddup	-15 * SIZE(BO), %xmm9
	movapd	%xmm9, %xmm10
	mulpd	 %xmm0, %xmm9
	subpd	 %xmm9, %xmm2
	mulpd	 %xmm1, %xmm10
	subpd	 %xmm10, %xmm3

	movddup	-13 * SIZE(BO), %xmm8
	mulpd	 %xmm8, %xmm2
	mulpd	 %xmm8, %xmm3
#endif

#ifdef RT
	movddup	-13 * SIZE(BO), %xmm8
	mulpd	 %xmm8, %xmm2
	mulpd	 %xmm8, %xmm3

	movddup	-14 * SIZE(BO), %xmm9
	movapd	%xmm9, %xmm10
	mulpd	 %xmm2, %xmm9
	subpd	 %xmm9, %xmm0
	mulpd	 %xmm3, %xmm10
	subpd	 %xmm10, %xmm1

	movddup	-16 * SIZE(BO), %xmm8
	mulpd	 %xmm8, %xmm0
	mulpd	 %xmm8, %xmm1
#endif

#ifdef LN
	subq	$4 * SIZE, CO1
	subq	$4 * SIZE, CO2
#endif

#if defined(LN) || defined(LT)
	movsd	%xmm9,  0 * SIZE(CO1)
	movsd	%xmm13, 1 * SIZE(CO1)
	movsd	%xmm1,  2 * SIZE(CO1)
	movsd	%xmm5,  3 * SIZE(CO1)

	movhpd	%xmm9,  0 * SIZE(CO2)
	movhpd	%xmm13, 1 * SIZE(CO2)
	movhpd	%xmm1,  2 * SIZE(CO2)
	movhpd	%xmm5,  3 * SIZE(CO2)
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
	movapd	%xmm9,  -16 * SIZE(BO)
	movapd	%xmm13, -14 * SIZE(BO)
	movapd	%xmm1,  -12 * SIZE(BO)
	movapd	%xmm5,  -10 * SIZE(BO)
#else
	movapd	%xmm0,  -16 * SIZE(AO)
	movapd	%xmm1,  -14 * SIZE(AO)
	movapd	%xmm2,  -12 * SIZE(AO)
	movapd	%xmm3,  -10 * SIZE(AO)
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
	jg	.L51
	ALIGN_4

.L60:
	testq	$2, M
	BRANCH
	jle	.L70
	ALIGN_4

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

	movaps	-16 * SIZE(AO), %xmm0
	pxor	%xmm8,  %xmm8
	pxor	%xmm9,  %xmm9
	movaps	-16 * SIZE(BO), %xmm2
	pxor	%xmm10, %xmm10
	pxor	%xmm11, %xmm11

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	sarq	$2, %rax
	NOBRANCH
	jle	.L65
	ALIGN_4

.L62:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)

	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	mulpd	%xmm0, %xmm7
	movaps	-14 * SIZE(AO), %xmm0

	addpd	%xmm2, %xmm9
	addpd	%xmm7, %xmm8
	movaps	-14 * SIZE(BO), %xmm2

	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	mulpd	%xmm0, %xmm7
	movaps	-12 * SIZE(AO), %xmm0

	addpd	%xmm2, %xmm11
	addpd	%xmm7, %xmm10
	movaps	-12 * SIZE(BO), %xmm2

	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	mulpd	%xmm0, %xmm7
	movaps	-10 * SIZE(AO), %xmm0

	addpd	%xmm2, %xmm9
	addpd	%xmm7, %xmm8
	movaps	-10 * SIZE(BO), %xmm2

	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	mulpd	%xmm0, %xmm7
	movaps	 -8 * SIZE(AO), %xmm0

	addpd	%xmm2, %xmm11
	addpd	%xmm7, %xmm10
	movaps	 -8 * SIZE(BO), %xmm2

	subq	$-8 * SIZE, AO
	subq	$-8 * SIZE, BO
	subq	$1, %rax
	BRANCH
	jg	.L62
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
	je	.L68
	ALIGN_4

.L66:
	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	mulpd	%xmm0, %xmm7
	movaps	-14 * SIZE(AO), %xmm0

	addpd	%xmm2, %xmm9
	addpd	%xmm7, %xmm8
	movaps	-14 * SIZE(BO), %xmm2

	addq	$2 * SIZE, AO
	addq	$2 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L66
	ALIGN_4

.L68:
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

	addpd	%xmm10, %xmm8
	addpd	%xmm11, %xmm9

	movapd	 %xmm8, %xmm0
	movsd    %xmm9, %xmm8
	movsd    %xmm0, %xmm9


#if defined(LN) || defined(LT)
	movapd	%xmm8, %xmm0
	unpcklpd %xmm9, %xmm8
	unpckhpd %xmm9, %xmm0

	movapd	 -16 * SIZE(BO), %xmm9
	movapd	 -14 * SIZE(BO), %xmm13

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
	movddup	-16 * SIZE(BO), %xmm8
	mulpd	 %xmm8, %xmm0

	movddup	-15 * SIZE(BO), %xmm9
	mulpd	 %xmm0, %xmm9
	subpd	 %xmm9, %xmm2

	movddup	-13 * SIZE(BO), %xmm8
	mulpd	 %xmm8, %xmm2
#endif

#ifdef RT
	movddup	-13 * SIZE(BO), %xmm8
	mulpd	 %xmm8, %xmm2

	movddup	-14 * SIZE(BO), %xmm9
	mulpd	 %xmm2, %xmm9
	subpd	 %xmm9, %xmm0

	movddup	-16 * SIZE(BO), %xmm8
	mulpd	 %xmm8, %xmm0
#endif

#ifdef LN
	subq	$2 * SIZE, CO1
	subq	$2 * SIZE, CO2
#endif

#if defined(LN) || defined(LT)
	movsd	%xmm9,  0 * SIZE(CO1)
	movsd	%xmm13, 1 * SIZE(CO1)

	movhpd	%xmm9,  0 * SIZE(CO2)
	movhpd	%xmm13, 1 * SIZE(CO2)
#else
	movsd	%xmm0,  0 * SIZE(CO1)
	movhpd	%xmm0,  1 * SIZE(CO1)

	movsd	%xmm2,  0 * SIZE(CO2)
	movhpd	%xmm2,  1 * SIZE(CO2)
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm9,  -16 * SIZE(BO)
	movapd	%xmm13, -14 * SIZE(BO)
#else
	movapd	%xmm0,  -16 * SIZE(AO)
	movapd	%xmm2,  -14 * SIZE(AO)
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

.L70:
	testq	$1, M
	BRANCH
	jle	.L79
	ALIGN_4

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

	movsd	-16 * SIZE(AO), %xmm0
	movaps	-16 * SIZE(BO), %xmm2

	pxor	%xmm8,  %xmm8
	pxor	%xmm9,  %xmm9

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	sarq	$2, %rax
	NOBRANCH
	jle	.L75
	ALIGN_4

.L72:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)

	shufps	$0x44, %xmm0, %xmm0
	mulpd	%xmm0, %xmm2
	movsd	-15 * SIZE(AO), %xmm0
	addpd	%xmm2, %xmm8
	movaps	-14 * SIZE(BO), %xmm2

	shufps	$0x44, %xmm0, %xmm0
	mulpd	%xmm0, %xmm2
	movsd	-14 * SIZE(AO), %xmm0
	addpd	%xmm2, %xmm9
	movaps	-12 * SIZE(BO), %xmm2

	shufps	$0x44, %xmm0, %xmm0
	mulpd	%xmm0, %xmm2
	movsd	-13 * SIZE(AO), %xmm0
	addpd	%xmm2, %xmm8
	movaps	-10 * SIZE(BO), %xmm2

	shufps	$0x44, %xmm0, %xmm0
	mulpd	%xmm0, %xmm2
	movsd	-12 * SIZE(AO), %xmm0
	addpd	%xmm2, %xmm9
	movaps	 -8 * SIZE(BO), %xmm2

	subq	$-4 * SIZE, AO
	subq	$-8 * SIZE, BO
	subq	$1, %rax
	BRANCH
	jg	.L72
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
	je	.L78
	ALIGN_4

.L76:
	shufps	$0x44, %xmm0, %xmm0
	mulpd	%xmm0, %xmm2
	movsd	-15 * SIZE(AO), %xmm0
	addpd	%xmm2, %xmm8
	movaps	-14 * SIZE(BO), %xmm2

	addq	$1 * SIZE, AO
	addq	$2 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L76
	ALIGN_4

.L78:
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

	addpd	%xmm9, %xmm8
	movhlps %xmm8, %xmm9

#if defined(LN) || defined(LT)
	movsd	-16 * SIZE(BO), %xmm12
	movsd	-15 * SIZE(BO), %xmm13
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
	mulsd	-16 * SIZE(BO), %xmm12
	movsd	-15 * SIZE(BO), %xmm9
	mulsd	 %xmm12, %xmm9
	subsd	 %xmm9, %xmm13

	mulsd	-13 * SIZE(BO), %xmm13
#endif

#ifdef RT
	mulsd	-13 * SIZE(BO), %xmm13

	movlpd	-14 * SIZE(BO), %xmm9
	mulsd	 %xmm13, %xmm9
	subsd	 %xmm9, %xmm12

	mulsd	-16 * SIZE(BO), %xmm12
#endif

#ifdef LN
	subq	$1 * SIZE, CO1
	subq	$1 * SIZE, CO2
#endif

	movsd	%xmm12,  0 * SIZE(CO1)
	movsd	%xmm13,  0 * SIZE(CO2)

#if defined(LN) || defined(LT)
	movsd	%xmm12,  -16 * SIZE(BO)
	movsd	%xmm13,  -15 * SIZE(BO)
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

.L79:
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

.L80:
	movq	N,  J
	sarq	$2, J
	NOBRANCH
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
       salq	$2 + BASE_SHIFT, %rax
       subq	%rax, B

       leaq	(, LDC, 4), %rax
       subq	%rax, C
#endif

	movq	C, CO1
	leaq	(C, LDC, 1), CO2
#ifndef RT
	leaq	(C, LDC, 4), C
#endif

#ifdef LN
	movq	OFFSET, %rax
	addq	M, %rax
	movq	%rax, KK
#endif

	movq	K, %rax
	salq	$BASE_SHIFT + 2, %rax
	movq	B, BB
	subq	%rax, BB

#ifdef LT
	movq	OFFSET, %rax
	movq	%rax, KK
#endif

	movq	M,  I
	sarq	$2, I	# i = (m >> 2)
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
	leaq	(B,  %rax, 4), BO
#else
	movq	B, BO
#endif

	prefetcht2	 -16 * SIZE(BB)
	subq		 $-8 * SIZE, BB

	movaps	-16 * SIZE(AO), %xmm0
	pxor	%xmm3, %xmm3
	movaps	-14 * SIZE(AO), %xmm1
	pxor	%xmm4, %xmm4
	movaps	-16 * SIZE(BO), %xmm2

	pxor	%xmm5, %xmm5
	pxor	%xmm6, %xmm6

#ifdef LN
	prefetcht0    -4 * SIZE(CO1)
	movapd	%xmm4, %xmm8
	movapd	%xmm4, %xmm9
	prefetcht0    -4 * SIZE(CO2)
	movapd	%xmm4, %xmm10
	movapd	%xmm4, %xmm11

	prefetcht0    -4 * SIZE(CO1, LDC, 2)
	movapd	%xmm4, %xmm12
	movapd	%xmm4, %xmm13
	prefetcht0    -4 * SIZE(CO2, LDC, 2)
	movapd	%xmm4, %xmm14
	movapd	%xmm4, %xmm15
#else
	prefetcht0     3 * SIZE(CO1)
	movapd	%xmm4, %xmm8
	movapd	%xmm4, %xmm9
	prefetcht0     3 * SIZE(CO2)
	movapd	%xmm4, %xmm10
	movapd	%xmm4, %xmm11

	prefetcht0     3 * SIZE(CO1, LDC, 2)
	movapd	%xmm4, %xmm12
	movapd	%xmm4, %xmm13
	prefetcht0     3 * SIZE(CO2, LDC, 2)
	movapd	%xmm4, %xmm14
	movapd	%xmm4, %xmm15
#endif

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	sarq	$3, %rax
	NOBRANCH
	jle	.L15
	ALIGN_3

.L12:
	addpd	%xmm3, %xmm11
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)
	movaps	-14 * SIZE(BO), %xmm3
	addpd	%xmm4, %xmm15
	movaps	%xmm2, %xmm4
	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm4

	addpd	%xmm5, %xmm10
	addpd	%xmm6, %xmm14
	movaps	%xmm7, %xmm6
	mulpd	%xmm0, %xmm7
	mulpd	%xmm1, %xmm6

	addpd	%xmm2, %xmm9
	movaps	-12 * SIZE(BO), %xmm2
	addpd	%xmm4, %xmm13
	movaps	%xmm3, %xmm4
	pshufd	$0x4e, %xmm3, %xmm5
	mulpd	%xmm0, %xmm3
	mulpd	%xmm1, %xmm4

	addpd	%xmm7, %xmm8
	addpd	%xmm6, %xmm12
	movaps	%xmm5, %xmm6
	mulpd	%xmm0, %xmm5
	movaps	-12 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm6
	movaps	-10 * SIZE(AO), %xmm1

	addpd	%xmm3, %xmm11
	movaps	-10 * SIZE(BO), %xmm3
	addpd	%xmm4, %xmm15
	movaps	%xmm2, %xmm4
	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm4

	addpd	%xmm5, %xmm10
	addpd	%xmm6, %xmm14
	movaps	%xmm7, %xmm6
	mulpd	%xmm0, %xmm7
	mulpd	%xmm1, %xmm6

	addpd	%xmm2, %xmm9
	movaps	 -8 * SIZE(BO), %xmm2
	addpd	%xmm4, %xmm13
	movaps	%xmm3, %xmm4
	pshufd	$0x4e, %xmm3, %xmm5
	mulpd	%xmm0, %xmm3
	mulpd	%xmm1, %xmm4

	addpd	%xmm7, %xmm8
	addpd	%xmm6, %xmm12
	movaps	%xmm5, %xmm6
	mulpd	%xmm0, %xmm5
	movaps	 -8 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm6
	movaps	 -6 * SIZE(AO), %xmm1

	addpd	%xmm3, %xmm11
	movaps	 -6 * SIZE(BO), %xmm3
	addpd	%xmm4, %xmm15
	movaps	%xmm2, %xmm4
	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm4

	addpd	%xmm5, %xmm10
	addpd	%xmm6, %xmm14
	movaps	%xmm7, %xmm6
	PREFETCH (PREFETCHSIZE +  8) * SIZE(AO)
	mulpd	%xmm0, %xmm7
	mulpd	%xmm1, %xmm6

	addpd	%xmm2, %xmm9
	movaps	 -4 * SIZE(BO), %xmm2
	addpd	%xmm4, %xmm13
	movaps	%xmm3, %xmm4
	pshufd	$0x4e, %xmm3, %xmm5
	mulpd	%xmm0, %xmm3
	mulpd	%xmm1, %xmm4

	addpd	%xmm7, %xmm8
	addpd	%xmm6, %xmm12
	movaps	%xmm5, %xmm6
	mulpd	%xmm0, %xmm5
	movaps	 -4 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm6
	movaps	 -2 * SIZE(AO), %xmm1

	addpd	%xmm3, %xmm11
	movaps	 -2 * SIZE(BO), %xmm3
	addpd	%xmm4, %xmm15
	movaps	%xmm2, %xmm4
	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm4

	addpd	%xmm5, %xmm10
	addpd	%xmm6, %xmm14
	movaps	%xmm7, %xmm6
	mulpd	%xmm0, %xmm7
	mulpd	%xmm1, %xmm6

	addpd	%xmm2, %xmm9
	movaps	  0 * SIZE(BO), %xmm2
	addpd	%xmm4, %xmm13
	movaps	%xmm3, %xmm4
	pshufd	$0x4e, %xmm3, %xmm5
	mulpd	%xmm0, %xmm3
	mulpd	%xmm1, %xmm4

	addpd	%xmm7, %xmm8
	addpd	%xmm6, %xmm12
	movaps	%xmm5, %xmm6
	mulpd	%xmm0, %xmm5
	movaps	  0 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm6
	movaps	  2 * SIZE(AO), %xmm1

	addpd	%xmm3, %xmm11
	movaps	  2 * SIZE(BO), %xmm3
	addpd	%xmm4, %xmm15
	movaps	%xmm2, %xmm4
	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm4

	addpd	%xmm5, %xmm10
	addpd	%xmm6, %xmm14
	movaps	%xmm7, %xmm6
	PREFETCH (PREFETCHSIZE + 16) * SIZE(AO)
	mulpd	%xmm0, %xmm7
	mulpd	%xmm1, %xmm6

	addpd	%xmm2, %xmm9
	movaps	  4 * SIZE(BO), %xmm2
	addpd	%xmm4, %xmm13
	movaps	%xmm3, %xmm4
	pshufd	$0x4e, %xmm3, %xmm5
	mulpd	%xmm0, %xmm3
	mulpd	%xmm1, %xmm4

	addpd	%xmm7, %xmm8
	addpd	%xmm6, %xmm12
	movaps	%xmm5, %xmm6
	mulpd	%xmm0, %xmm5
	movaps	  4 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm6
	movaps	  6 * SIZE(AO), %xmm1

	addpd	%xmm3, %xmm11
	movaps	  6 * SIZE(BO), %xmm3
	addpd	%xmm4, %xmm15
	movaps	%xmm2, %xmm4
	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm4

	addpd	%xmm5, %xmm10
	addpd	%xmm6, %xmm14
	movaps	%xmm7, %xmm6
	mulpd	%xmm0, %xmm7
	mulpd	%xmm1, %xmm6

	addpd	%xmm2, %xmm9
	movaps	  8 * SIZE(BO), %xmm2
	addpd	%xmm4, %xmm13
	movaps	%xmm3, %xmm4
	pshufd	$0x4e, %xmm3, %xmm5
	mulpd	%xmm0, %xmm3
	mulpd	%xmm1, %xmm4

	addpd	%xmm7, %xmm8
	addpd	%xmm6, %xmm12
	movaps	%xmm5, %xmm6
	mulpd	%xmm0, %xmm5
	movaps	  8 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm6
	movaps	 10 * SIZE(AO), %xmm1

	addpd	%xmm3, %xmm11
	movaps	 10 * SIZE(BO), %xmm3
	addpd	%xmm4, %xmm15
	movaps	%xmm2, %xmm4
	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm4

	addpd	%xmm5, %xmm10
	addpd	%xmm6, %xmm14
	movaps	%xmm7, %xmm6
	PREFETCH (PREFETCHSIZE + 24) * SIZE(AO)
	mulpd	%xmm0, %xmm7
	mulpd	%xmm1, %xmm6

	addpd	%xmm2, %xmm9
	movaps	 12 * SIZE(BO), %xmm2
	addpd	%xmm4, %xmm13
	movaps	%xmm3, %xmm4
	pshufd	$0x4e, %xmm3, %xmm5
	mulpd	%xmm0, %xmm3
	mulpd	%xmm1, %xmm4

	addpd	%xmm7, %xmm8
	addpd	%xmm6, %xmm12
	movaps	%xmm5, %xmm6
	mulpd	%xmm0, %xmm5
	movaps	 12 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm6
	movaps	 14 * SIZE(AO), %xmm1

	addpd	%xmm3, %xmm11
	movaps	 14 * SIZE(BO), %xmm3
	addpd	%xmm4, %xmm15
	movaps	%xmm2, %xmm4
	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm4

	addpd	%xmm5, %xmm10
	addpd	%xmm6, %xmm14
	movaps	%xmm7, %xmm6
	mulpd	%xmm0, %xmm7
	mulpd	%xmm1, %xmm6

	addpd	%xmm2, %xmm9
	movaps	 16 * SIZE(BO), %xmm2
	addpd	%xmm4, %xmm13
	movaps	%xmm3, %xmm4
	pshufd	$0x4e, %xmm3, %xmm5
	mulpd	%xmm0, %xmm3
	mulpd	%xmm1, %xmm4

	addpd	%xmm7, %xmm8
	subq	$-32 * SIZE, AO
	addpd	%xmm6, %xmm12
	movaps	%xmm5, %xmm6
	mulpd	%xmm0, %xmm5
	movaps	-16 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm6
	movaps	-14 * SIZE(AO), %xmm1

	subq	$-32 * SIZE, BO

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
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je	.L18
	ALIGN_3

.L16:
	addpd	%xmm3, %xmm11
	movaps	-14 * SIZE(BO), %xmm3
	addpd	%xmm4, %xmm15
	movaps	%xmm2, %xmm4
	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm4

	addpd	%xmm5, %xmm10
	addpd	%xmm6, %xmm14
	movaps	%xmm7, %xmm6
	mulpd	%xmm0, %xmm7
	mulpd	%xmm1, %xmm6

	addpd	%xmm2, %xmm9
	movaps	-12 * SIZE(BO), %xmm2
	addpd	%xmm4, %xmm13
	movaps	%xmm3, %xmm4
	pshufd	$0x4e, %xmm3, %xmm5
	mulpd	%xmm0, %xmm3
	mulpd	%xmm1, %xmm4

	addpd	%xmm7, %xmm8
	addpd	%xmm6, %xmm12
	movaps	%xmm5, %xmm6
	mulpd	%xmm0, %xmm5
	movaps	-12 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm6
	movaps	-10 * SIZE(AO), %xmm1

	addq	$4 * SIZE, AO
	addq	$4 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L16
	ALIGN_3

.L18:
#if defined(LN) || defined(RT)
	movq	KK, %rax
	subq	$4, %rax

	leaq	(, %rax, SIZE), %rax

	movq	AORIG, AO
	leaq	(AO, %rax, 4), AO
	leaq	(B,  %rax, 4), BO
#endif

	addpd	%xmm3, %xmm11
	addpd	%xmm4, %xmm15
	addpd	%xmm5, %xmm10
	addpd	%xmm6, %xmm14

	movapd	 %xmm8, %xmm0
	movsd    %xmm9, %xmm8
	movsd    %xmm0, %xmm9

	movapd	 %xmm10, %xmm0
	movsd    %xmm11, %xmm10
	movsd    %xmm0,  %xmm11

	movapd	 %xmm12, %xmm0
	movsd    %xmm13, %xmm12
	movsd    %xmm0,  %xmm13

	movapd	 %xmm14, %xmm0
	movsd    %xmm15, %xmm14
	movsd    %xmm0,  %xmm15

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

	movapd	-16 * SIZE(BO), %xmm9
	movapd	-14 * SIZE(BO), %xmm11
	movapd	-12 * SIZE(BO), %xmm13
	movapd	-10 * SIZE(BO), %xmm15
	movapd	 -8 * SIZE(BO), %xmm1
	movapd	 -6 * SIZE(BO), %xmm3
	movapd	 -4 * SIZE(BO), %xmm5
	movapd	 -2 * SIZE(BO), %xmm7

	subpd	%xmm8,  %xmm9
	subpd	%xmm10, %xmm11
	subpd	%xmm0,  %xmm13
	subpd	%xmm2,  %xmm15
	subpd	%xmm12, %xmm1
	subpd	%xmm14, %xmm3
	subpd	%xmm4,  %xmm5
	subpd	%xmm6,  %xmm7
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
	movddup	-16 * SIZE(BO), %xmm8
	mulpd	 %xmm8, %xmm0
	mulpd	 %xmm8, %xmm1

	movddup	 -15 * SIZE(BO), %xmm10
	movapd	%xmm10, %xmm12
	mulpd	 %xmm0, %xmm10
	subpd	 %xmm10, %xmm2
	mulpd	 %xmm1, %xmm12
	subpd	 %xmm12, %xmm3

	movddup	 -14 * SIZE(BO), %xmm10
	movapd	%xmm10, %xmm12
	mulpd	 %xmm0, %xmm10
	subpd	 %xmm10, %xmm4
	mulpd	 %xmm1, %xmm12
	subpd	 %xmm12, %xmm5

	movddup	 -13 * SIZE(BO), %xmm10
	movapd	%xmm10, %xmm12
	mulpd	 %xmm0, %xmm10
	subpd	 %xmm10, %xmm6
	mulpd	 %xmm1, %xmm12
	subpd	 %xmm12, %xmm7

	movddup	 -11 * SIZE(BO), %xmm8
	mulpd	 %xmm8, %xmm2
	mulpd	 %xmm8, %xmm3

	movddup	 -10 * SIZE(BO), %xmm10
	movapd	%xmm10, %xmm12
	mulpd	 %xmm2, %xmm10
	subpd	 %xmm10, %xmm4
	mulpd	 %xmm3, %xmm12
	subpd	 %xmm12, %xmm5

	movddup	  -9 * SIZE(BO), %xmm10
	movapd	%xmm10, %xmm12
	mulpd	 %xmm2, %xmm10
	subpd	 %xmm10, %xmm6
	mulpd	 %xmm3, %xmm12
	subpd	 %xmm12, %xmm7

	movddup	  -6 * SIZE(BO), %xmm8
	mulpd	 %xmm8, %xmm4
	mulpd	 %xmm8, %xmm5

	movddup	  -5 * SIZE(BO), %xmm10
	movapd	%xmm10, %xmm12
	mulpd	 %xmm4, %xmm10
	subpd	 %xmm10, %xmm6
	mulpd	 %xmm5, %xmm12
	subpd	 %xmm12, %xmm7

	movddup	  -1 * SIZE(BO), %xmm8
	mulpd	 %xmm8, %xmm6
	mulpd	 %xmm8, %xmm7
#endif

#ifdef RT
	movddup	 -1 * SIZE(BO), %xmm8
	mulpd	 %xmm8, %xmm6
	mulpd	 %xmm8, %xmm7

	movddup	 -2 * SIZE(BO), %xmm10
	movapd	%xmm10, %xmm12
	mulpd	 %xmm6, %xmm10
	subpd	 %xmm10, %xmm4
	mulpd	 %xmm7, %xmm12
	subpd	 %xmm12, %xmm5

	movddup	 -3 * SIZE(BO), %xmm10
	movapd	%xmm10, %xmm12
	mulpd	 %xmm6, %xmm10
	subpd	 %xmm10, %xmm2
	mulpd	 %xmm7, %xmm12
	subpd	 %xmm12, %xmm3

	movddup	 -4 * SIZE(BO), %xmm10
	movapd	%xmm10, %xmm12
	mulpd	 %xmm6, %xmm10
	subpd	 %xmm10, %xmm0
	mulpd	 %xmm7, %xmm12
	subpd	 %xmm12, %xmm1

	movddup	 -6 * SIZE(BO), %xmm8
	mulpd	 %xmm8, %xmm4
	mulpd	 %xmm8, %xmm5

	movddup	 -7 * SIZE(BO), %xmm10
	movapd	%xmm10, %xmm12
	mulpd	 %xmm4, %xmm10
	subpd	 %xmm10, %xmm2
	mulpd	 %xmm5, %xmm12
	subpd	 %xmm12, %xmm3

	movddup	 -8 * SIZE(BO), %xmm10
	movapd	%xmm10, %xmm12
	mulpd	 %xmm4, %xmm10
	subpd	 %xmm10, %xmm0
	mulpd	 %xmm5, %xmm12
	subpd	 %xmm12, %xmm1

	movddup	-11 * SIZE(BO), %xmm8
	mulpd	 %xmm8, %xmm2
	mulpd	 %xmm8, %xmm3

	movddup	-12 * SIZE(BO), %xmm10
	movapd	%xmm10, %xmm12
	mulpd	 %xmm2, %xmm10
	subpd	 %xmm10, %xmm0
	mulpd	 %xmm3, %xmm12
	subpd	 %xmm12, %xmm1

	movddup	-16 * SIZE(BO), %xmm8
	mulpd	 %xmm8, %xmm0
	mulpd	 %xmm8, %xmm1
#endif

#ifdef LN
	subq	$4 * SIZE, CO1
	subq	$4 * SIZE, CO2
#endif

#if defined(LN) || defined(LT)
	movsd	%xmm9,  0 * SIZE(CO1)
	movsd	%xmm13, 1 * SIZE(CO1)
	movsd	%xmm1,  2 * SIZE(CO1)
	movsd	%xmm5,  3 * SIZE(CO1)

	movhpd	%xmm9,  0 * SIZE(CO2)
	movhpd	%xmm13, 1 * SIZE(CO2)
	movhpd	%xmm1,  2 * SIZE(CO2)
	movhpd	%xmm5,  3 * SIZE(CO2)

	movsd	%xmm11, 0 * SIZE(CO1, LDC, 2)
	movsd	%xmm15, 1 * SIZE(CO1, LDC, 2)
	movsd	%xmm3,  2 * SIZE(CO1, LDC, 2)
	movsd	%xmm7,  3 * SIZE(CO1, LDC, 2)

	movhpd	%xmm11, 0 * SIZE(CO2, LDC, 2)
	movhpd	%xmm15, 1 * SIZE(CO2, LDC, 2)
	movhpd	%xmm3,  2 * SIZE(CO2, LDC, 2)
	movhpd	%xmm7,  3 * SIZE(CO2, LDC, 2)
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
	movapd	%xmm9,  -16 * SIZE(BO)
	movapd	%xmm11, -14 * SIZE(BO)
	movapd	%xmm13, -12 * SIZE(BO)
	movapd	%xmm15, -10 * SIZE(BO)
	movapd	%xmm1,   -8 * SIZE(BO)
	movapd	%xmm3,   -6 * SIZE(BO)
	movapd	%xmm5,   -4 * SIZE(BO)
	movapd	%xmm7,   -2 * SIZE(BO)
#else
	movapd	%xmm0, -16 * SIZE(AO)
	movapd	%xmm1, -14 * SIZE(AO)
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

	decq	I			# i --
	BRANCH
	jg	.L11
	ALIGN_4

.L20:
	testq	$2, M
	BRANCH
	jle	.L30
	ALIGN_4

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

	movaps	-16 * SIZE(AO), %xmm0
	pxor	%xmm3, %xmm3
	movaps	-16 * SIZE(BO), %xmm2
	pxor	%xmm5, %xmm5

	pxor	%xmm8,  %xmm8
	pxor	%xmm9,  %xmm9
	pxor	%xmm10, %xmm10
	pxor	%xmm11, %xmm11

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	sarq	$2, %rax
	NOBRANCH
	jle	.L25
	ALIGN_4

.L22:
	addpd	%xmm3, %xmm11
	movaps	-14 * SIZE(BO), %xmm3
	pshufd	$0x4e, %xmm2, %xmm7
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)
	mulpd	%xmm0, %xmm2
	addpd	%xmm5, %xmm10
	mulpd	%xmm0, %xmm7

	addpd	%xmm2, %xmm9
	movaps	-12 * SIZE(BO), %xmm2
	pshufd	$0x4e, %xmm3, %xmm5
	mulpd	%xmm0, %xmm3
	addpd	%xmm7, %xmm8
	mulpd	%xmm0, %xmm5
	movaps	-14 * SIZE(AO), %xmm0

	addpd	%xmm3, %xmm11
	movaps	-10 * SIZE(BO), %xmm3
	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	addpd	%xmm5, %xmm10
	mulpd	%xmm0, %xmm7

	addpd	%xmm2, %xmm9
	movaps	 -8 * SIZE(BO), %xmm2
	pshufd	$0x4e, %xmm3, %xmm5
	mulpd	%xmm0, %xmm3
	addpd	%xmm7, %xmm8
	mulpd	%xmm0, %xmm5
	movaps	-12 * SIZE(AO), %xmm0

	addpd	%xmm3, %xmm11
	movaps	 -6 * SIZE(BO), %xmm3
	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	addpd	%xmm5, %xmm10
	mulpd	%xmm0, %xmm7

	addpd	%xmm2, %xmm9
	movaps	 -4 * SIZE(BO), %xmm2
	pshufd	$0x4e, %xmm3, %xmm5
	mulpd	%xmm0, %xmm3
	addpd	%xmm7, %xmm8
	mulpd	%xmm0, %xmm5
	movaps	-10 * SIZE(AO), %xmm0

	addpd	%xmm3, %xmm11
	movaps	 -2 * SIZE(BO), %xmm3
	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	addpd	%xmm5, %xmm10
	mulpd	%xmm0, %xmm7
	subq	$ -8 * SIZE, AO

	addpd	%xmm2, %xmm9
	movaps	  0 * SIZE(BO), %xmm2
	pshufd	$0x4e, %xmm3, %xmm5
	mulpd	%xmm0, %xmm3
	addpd	%xmm7, %xmm8
	mulpd	%xmm0, %xmm5
	movaps	-16 * SIZE(AO), %xmm0

	subq	$-16 * SIZE, BO
	subq	$1, %rax
	BRANCH
	jg	.L22
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
	je	.L28
	ALIGN_4

.L26:
	addpd	%xmm3, %xmm11
	movaps	-14 * SIZE(BO), %xmm3
	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	addpd	%xmm5, %xmm10
	mulpd	%xmm0, %xmm7

	addpd	%xmm2, %xmm9
	movaps	-12 * SIZE(BO), %xmm2
	pshufd	$0x4e, %xmm3, %xmm5
	mulpd	%xmm0, %xmm3
	addpd	%xmm7, %xmm8
	mulpd	%xmm0, %xmm5
	movaps	-14 * SIZE(AO), %xmm0

	addq	$2 * SIZE, AO
	addq	$4 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L26
	ALIGN_4

.L28:
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

	addpd	%xmm3, %xmm11
	addpd	%xmm5, %xmm10

	movapd	 %xmm8, %xmm0
	movsd    %xmm9, %xmm8
	movsd    %xmm0, %xmm9

	movapd	 %xmm10, %xmm0
	movsd    %xmm11, %xmm10
	movsd    %xmm0,  %xmm11

#if defined(LN) || defined(LT)
	movapd	%xmm8,  %xmm0
	unpcklpd %xmm9, %xmm8
	unpckhpd %xmm9, %xmm0

	movapd	%xmm10,  %xmm2
	unpcklpd %xmm11, %xmm10
	unpckhpd %xmm11, %xmm2

	movapd	 -16 * SIZE(BO), %xmm9
	movapd	 -14 * SIZE(BO), %xmm11
	movapd	 -12 * SIZE(BO), %xmm13
	movapd	 -10 * SIZE(BO), %xmm15

	subpd	%xmm8,  %xmm9
	subpd	%xmm10, %xmm11
	subpd	%xmm0,  %xmm13
	subpd	%xmm2,  %xmm15
#else
	movapd	 -16 * SIZE(AO), %xmm0
	movapd	 -14 * SIZE(AO), %xmm2
	movapd	 -12 * SIZE(AO), %xmm4
	movapd	 -10 * SIZE(AO), %xmm6

	subpd	%xmm8,  %xmm0
	subpd	%xmm9,  %xmm2
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
	movddup	 -16 * SIZE(BO), %xmm8
	mulpd	 %xmm8, %xmm0

	movddup	 -15 * SIZE(BO), %xmm9
	mulpd	 %xmm0, %xmm9
	subpd	 %xmm9, %xmm2
	movddup	 -14 * SIZE(BO), %xmm10
	mulpd	 %xmm0, %xmm10
	subpd	 %xmm10, %xmm4
	movddup	 -13 * SIZE(BO), %xmm11
	mulpd	 %xmm0, %xmm11
	subpd	 %xmm11, %xmm6

	movddup	 -11 * SIZE(BO), %xmm8
	mulpd	 %xmm8, %xmm2
	movddup	 -10 * SIZE(BO), %xmm9
	mulpd	 %xmm2, %xmm9
	subpd	 %xmm9, %xmm4
	movddup	  -9 * SIZE(BO), %xmm10
	mulpd	 %xmm2, %xmm10
	subpd	 %xmm10, %xmm6

	movddup	  -6 * SIZE(BO), %xmm8
	mulpd	 %xmm8, %xmm4

	movddup	  -5 * SIZE(BO), %xmm9
	mulpd	 %xmm4, %xmm9
	subpd	 %xmm9, %xmm6

	movddup	  -1 * SIZE(BO), %xmm8
	mulpd	 %xmm8, %xmm6
#endif

#ifdef RT
	movddup	-1 * SIZE(BO), %xmm8
	mulpd	 %xmm8, %xmm6

	movddup	-2 * SIZE(BO), %xmm9
	mulpd	 %xmm6, %xmm9
	subpd	 %xmm9, %xmm4
	movddup	-3 * SIZE(BO), %xmm10
	mulpd	 %xmm6, %xmm10
	subpd	 %xmm10, %xmm2
	movddup	-4 * SIZE(BO), %xmm11
	mulpd	 %xmm6, %xmm11
	subpd	 %xmm11, %xmm0

	movddup	-6 * SIZE(BO), %xmm8
	mulpd	 %xmm8, %xmm4
	movddup	-7 * SIZE(BO), %xmm9
	mulpd	 %xmm4, %xmm9
	subpd	 %xmm9, %xmm2
	movddup	-8 * SIZE(BO), %xmm10
	mulpd	 %xmm4, %xmm10
	subpd	 %xmm10, %xmm0

	movddup	-11 * SIZE(BO), %xmm8
	mulpd	 %xmm8, %xmm2
	movddup	-12 * SIZE(BO), %xmm9
	mulpd	 %xmm2, %xmm9
	subpd	 %xmm9, %xmm0

	movddup	-16 * SIZE(BO), %xmm8
	mulpd	 %xmm8, %xmm0
#endif

#ifdef LN
	subq	$2 * SIZE, CO1
	subq	$2 * SIZE, CO2
#endif

#if defined(LN) || defined(LT)
	movsd	%xmm9,  0 * SIZE(CO1)
	movsd	%xmm13, 1 * SIZE(CO1)

	movhpd	%xmm9,  0 * SIZE(CO2)
	movhpd	%xmm13, 1 * SIZE(CO2)

	movsd	%xmm11, 0 * SIZE(CO1, LDC, 2)
	movsd	%xmm15, 1 * SIZE(CO1, LDC, 2)

	movhpd	%xmm11, 0 * SIZE(CO2, LDC, 2)
	movhpd	%xmm15, 1 * SIZE(CO2, LDC, 2)
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
	movapd	%xmm9,  -16 * SIZE(BO)
	movapd	%xmm11, -14 * SIZE(BO)
	movapd	%xmm13, -12 * SIZE(BO)
	movapd	%xmm15, -10 * SIZE(BO)
#else
	movapd	%xmm0,  -16 * SIZE(AO)
	movapd	%xmm2,  -14 * SIZE(AO)
	movapd	%xmm4,  -12 * SIZE(AO)
	movapd	%xmm6,  -10 * SIZE(AO)
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

.L30:
	testq	$1, M
	BRANCH
	jle	.L39
	ALIGN_4

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

	movsd	-16 * SIZE(AO), %xmm0
	movaps	-16 * SIZE(BO), %xmm2
	movaps	-14 * SIZE(BO), %xmm3

	pxor	%xmm8,  %xmm8
	pxor	%xmm9,  %xmm9
	pxor	%xmm10, %xmm10
	pxor	%xmm11, %xmm11

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	sarq	$2, %rax
	NOBRANCH
	jle	.L35
	ALIGN_4

.L32:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)

	shufps	$0x44, %xmm0, %xmm0
	mulpd	%xmm0, %xmm2
	mulpd	%xmm0, %xmm3
	movsd	-15 * SIZE(AO), %xmm0

	addpd	%xmm2, %xmm8
	movaps	-12 * SIZE(BO), %xmm2
	addpd	%xmm3, %xmm9
	movaps	-10 * SIZE(BO), %xmm3

	shufps	$0x44, %xmm0, %xmm0
	mulpd	%xmm0, %xmm2
	mulpd	%xmm0, %xmm3
	movsd	-14 * SIZE(AO), %xmm0

	addpd	%xmm2, %xmm10
	movaps	 -8 * SIZE(BO), %xmm2
	addpd	%xmm3, %xmm11
	movaps	 -6 * SIZE(BO), %xmm3

	shufps	$0x44, %xmm0, %xmm0
	mulpd	%xmm0, %xmm2
	mulpd	%xmm0, %xmm3
	movsd	-13 * SIZE(AO), %xmm0

	addpd	%xmm2, %xmm8
	movaps	 -4 * SIZE(BO), %xmm2
	addpd	%xmm3, %xmm9
	movaps	 -2 * SIZE(BO), %xmm3

	shufps	$0x44, %xmm0, %xmm0
	mulpd	%xmm0, %xmm2
	mulpd	%xmm0, %xmm3
	movsd	-12 * SIZE(AO), %xmm0

	addpd	%xmm2, %xmm10
	movaps	  0 * SIZE(BO), %xmm2
	addpd	%xmm3, %xmm11
	movaps	  2 * SIZE(BO), %xmm3

	subq	$ -4 * SIZE, AO
	subq	$-16 * SIZE, BO
	subq	$1, %rax
	BRANCH
	jg	.L32
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
	je	.L38
	ALIGN_4

.L36:
	shufps	$0x44, %xmm0, %xmm0
	mulpd	%xmm0, %xmm2
	mulpd	%xmm0, %xmm3
	movsd	-15 * SIZE(AO), %xmm0

	addpd	%xmm2, %xmm8
	movaps	-12 * SIZE(BO), %xmm2
	addpd	%xmm3, %xmm9
	movaps	-10 * SIZE(BO), %xmm3

	addq	$1 * SIZE, AO
	addq	$4 * SIZE, BO

	subq	$1, %rax
	BRANCH
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

	leaq	(, %rax, SIZE), %rax
	movq	AORIG, AO
	leaq	(AO, %rax, 1), AO
	leaq	(B,  %rax, 4), BO
#endif

	addpd	%xmm10, %xmm8
	addpd	%xmm11, %xmm9

#if defined(LN) || defined(LT)
	movaps	 -16 * SIZE(BO), %xmm12
	movaps	 -14 * SIZE(BO), %xmm13
#else
	movaps	 -16 * SIZE(AO), %xmm12
	movaps	 -14 * SIZE(AO), %xmm13
#endif

	subpd	%xmm8,  %xmm12
	subpd	%xmm9,  %xmm13

#if defined(RN) || defined(RT)
	movhlps %xmm13, %xmm15
   	movsd	%xmm13, %xmm14
	movhlps %xmm12, %xmm13
   	movsd	%xmm12, %xmm12
#endif

#if defined(LN) || defined(LT)
	movddup	-16 * SIZE(AO), %xmm8

	mulpd	 %xmm8, %xmm12
	mulpd	 %xmm8, %xmm13
#endif

#ifdef RN
	mulsd	-16 * SIZE(BO), %xmm12
	movlpd	-15 * SIZE(BO), %xmm9
	mulsd	 %xmm12, %xmm9
	subsd	 %xmm9, %xmm13
	movlpd	-14 * SIZE(BO), %xmm10
	mulsd	 %xmm12, %xmm10
	subsd	 %xmm10, %xmm14
	movlpd  -13 * SIZE(BO), %xmm11
	mulsd	 %xmm12, %xmm11
	subsd	 %xmm11, %xmm15

	mulsd	-11 * SIZE(BO), %xmm13
	movlpd	-10 * SIZE(BO), %xmm9
	mulsd	 %xmm13, %xmm9
	subsd	 %xmm9, %xmm14
	movlpd	 -9 * SIZE(BO), %xmm10
	mulsd	 %xmm13, %xmm10
	subsd	 %xmm10, %xmm15

	mulsd	 -6 * SIZE(BO), %xmm14
	movlpd	 -5 * SIZE(BO), %xmm9
	mulsd	 %xmm14, %xmm9
	subsd	 %xmm9, %xmm15

	mulsd	 -1 * SIZE(BO), %xmm15
#endif

#ifdef RT
	mulsd	 -1 * SIZE(BO), %xmm15

	movlpd	 -2 * SIZE(BO), %xmm9
	mulsd	 %xmm15, %xmm9
	subsd	 %xmm9, %xmm14
	movlpd	 -3 * SIZE(BO), %xmm10
	mulsd	 %xmm15, %xmm10
	subsd	 %xmm10, %xmm13
	movlpd	 -4 * SIZE(BO), %xmm11
	mulsd	 %xmm15, %xmm11
	subsd	 %xmm11, %xmm12

	mulsd	 -6 * SIZE(BO), %xmm14

	movlpd	 -7 * SIZE(BO), %xmm9
	mulsd	 %xmm14, %xmm9
	subsd	 %xmm9, %xmm13
	movlpd	 -8 * SIZE(BO), %xmm10
	mulsd	 %xmm14, %xmm10
	subsd	 %xmm10, %xmm12

	mulsd	-11 * SIZE(BO), %xmm13

	movlpd	-12 * SIZE(BO), %xmm9
	mulsd	 %xmm13, %xmm9
	subsd	 %xmm9, %xmm12

	mulsd	-16 * SIZE(BO), %xmm12
#endif

#ifdef LN
	subq	$1 * SIZE, CO1
	subq	$1 * SIZE, CO2
#endif

#if defined(LN) || defined(LT)
	movsd	%xmm12,  0 * SIZE(CO1)
	movhps	%xmm12,  0 * SIZE(CO2)
	movsd	%xmm13,  0 * SIZE(CO1, LDC, 2)
	movhps	%xmm13,  0 * SIZE(CO2, LDC, 2)

	movaps	%xmm12,  -16 * SIZE(BO)
	movaps	%xmm13,  -14 * SIZE(BO)
#else
	movsd	%xmm12,  0 * SIZE(CO1)
	movsd	%xmm13,  0 * SIZE(CO2)
	movsd	%xmm14,  0 * SIZE(CO1, LDC, 2)
	movsd	%xmm15,  0 * SIZE(CO2, LDC, 2)

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

.L39:
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

	subq	$1, J
	BRANCH
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
