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

#define STACKSIZE 512

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

#define PREFETCHSIZE  (8 * 1 - 4)
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

	subq	$-16 * SIZE, A
	subq	$-16 * SIZE, B

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

	movq	N,  J
	sarq	$3, J
	NOBRANCH
	jle	.L30
	ALIGN_4

.L01:
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
	sarq	$1, I
	NOBRANCH
	jle	.L20
	ALIGN_4

.L11:
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

	prefetcht0	 -16 * SIZE(BB)
	subq		 $-8 * SIZE, BB

	xorps	%xmm1, %xmm1
	movapd	-16 * SIZE(AO), %xmm0
	xorps	%xmm2, %xmm2
	xorps	%xmm3, %xmm3
	xorps	%xmm4, %xmm4

	leaq	(LDC, LDC, 2), %rax

	xorps	%xmm8,  %xmm8
	prefetcht0     1 * SIZE(CO1)
	xorps	%xmm9,  %xmm9
	prefetcht0     2 * SIZE(CO1, LDC,  1)
	xorps	%xmm10, %xmm10
	prefetcht0     1 * SIZE(CO1, LDC,  2)
	xorps	%xmm11, %xmm11
	prefetcht0     2 * SIZE(CO1, %rax, 1)

	xorps	%xmm12, %xmm12
	prefetcht0     1 * SIZE(CO2)
	xorps	%xmm13, %xmm13
	prefetcht0     2 * SIZE(CO2, LDC,  1)
	xorps	%xmm14, %xmm14
	prefetcht0     1 * SIZE(CO2, LDC,  2)
	xorps	%xmm15, %xmm15
	prefetcht0     2 * SIZE(CO2, %rax, 1)

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

	addpd	%xmm1, %xmm12
	movaps	-16 * SIZE(BO), %xmm6
	addpd	%xmm2, %xmm13
	pshufd	$0x4e, %xmm6, %xmm2
	mulpd	%xmm0, %xmm6
	mulpd	%xmm0, %xmm2

	addpd	%xmm3, %xmm14
	movaps	-14 * SIZE(BO), %xmm3
	addpd	%xmm4, %xmm15
	pshufd	$0x4e, %xmm3, %xmm4
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm4

	addpd	%xmm6, %xmm8
	movaps	-12 * SIZE(BO), %xmm6
	addpd	%xmm2, %xmm9
	pshufd	$0x4e, %xmm6, %xmm2
	mulpd	%xmm0, %xmm6
	mulpd	%xmm0, %xmm2

	addpd	%xmm3, %xmm10
	movaps	-10 * SIZE(BO), %xmm3
	addpd	%xmm4, %xmm11
	pshufd	$0x4e, %xmm3, %xmm4
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm4

	addpd	%xmm6, %xmm12
	movaps	 -8 * SIZE(BO), %xmm1
	addpd	%xmm2, %xmm13
	movaps	-14 * SIZE(AO), %xmm5
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm5, %xmm1
	mulpd	%xmm5, %xmm2

	addpd	%xmm3, %xmm14
	movaps	 -6 * SIZE(BO), %xmm3
	addpd	%xmm4, %xmm15
	pshufd	$0x4e, %xmm3, %xmm4
	mulpd	%xmm5, %xmm3
	mulpd	%xmm5, %xmm4

	addpd	%xmm1, %xmm8
	movaps	 -4 * SIZE(BO), %xmm1
	addpd	%xmm2, %xmm9
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm5, %xmm1
	mulpd	%xmm5, %xmm2

	addpd	%xmm3, %xmm10
	movaps	 -2 * SIZE(BO), %xmm3
	addpd	%xmm4, %xmm11
	pshufd	$0x4e, %xmm3, %xmm4
	movaps	-12 * SIZE(AO), %xmm0
	mulpd	%xmm5, %xmm3
	mulpd	%xmm5, %xmm4

	addpd	%xmm1, %xmm12
	movaps	  0 * SIZE(BO), %xmm6
	addpd	%xmm2, %xmm13
	pshufd	$0x4e, %xmm6, %xmm2
	mulpd	%xmm0, %xmm6
	mulpd	%xmm0, %xmm2

	addpd	%xmm3, %xmm14
	movaps	  2 * SIZE(BO), %xmm3
	addpd	%xmm4, %xmm15
	pshufd	$0x4e, %xmm3, %xmm4
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm4

	addpd	%xmm6, %xmm8
	movaps	  4 * SIZE(BO), %xmm6
	addpd	%xmm2, %xmm9
	pshufd	$0x4e, %xmm6, %xmm2
	mulpd	%xmm0, %xmm6
	mulpd	%xmm0, %xmm2

	addpd	%xmm3, %xmm10
	movaps	  6 * SIZE(BO), %xmm3
	addpd	%xmm4, %xmm11
	pshufd	$0x4e, %xmm3, %xmm4
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm4

	addpd	%xmm6, %xmm12
	movaps	  8 * SIZE(BO), %xmm1
	addpd	%xmm2, %xmm13
	movaps	-10 * SIZE(AO), %xmm5
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm5, %xmm1
	mulpd	%xmm5, %xmm2

	addpd	%xmm3, %xmm14
	movaps	 10 * SIZE(BO), %xmm3
	addpd	%xmm4, %xmm15
	pshufd	$0x4e, %xmm3, %xmm4
	mulpd	%xmm5, %xmm3
	mulpd	%xmm5, %xmm4

	addpd	%xmm1, %xmm8
	movaps	 12 * SIZE(BO), %xmm1
	addpd	%xmm2, %xmm9
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm5, %xmm1
	mulpd	%xmm5, %xmm2

	addpd	%xmm3, %xmm10
	movaps	 14 * SIZE(BO), %xmm3
	addpd	%xmm4, %xmm11
	pshufd	$0x4e, %xmm3, %xmm4
	movaps	 -8 * SIZE(AO), %xmm0
	mulpd	%xmm5, %xmm3
	mulpd	%xmm5, %xmm4

	addq	$32 * SIZE, BO
	subq	$-8 * SIZE, AO
	decq	%rax
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
	addpd	%xmm1, %xmm12
	movaps	-16 * SIZE(BO), %xmm1
	addpd	%xmm2, %xmm13
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2

	addpd	%xmm3, %xmm14
	movaps	-14 * SIZE(BO), %xmm3
	addpd	%xmm4, %xmm15
	pshufd	$0x4e, %xmm3, %xmm4
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm4

	addpd	%xmm1, %xmm8
	movaps	-12 * SIZE(BO), %xmm1
	addpd	%xmm2, %xmm9
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2

	addpd	%xmm3, %xmm10
	movaps	-10 * SIZE(BO), %xmm3
	addpd	%xmm4, %xmm11
	pshufd	$0x4e, %xmm3, %xmm4
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm4

	movaps	-14 * SIZE(AO), %xmm0

	addq	$2 * SIZE, AO
	addq	$8 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L16
	ALIGN_4

.L18:
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

	addpd	%xmm1, %xmm12
	addpd	%xmm2, %xmm13
	addpd	%xmm3, %xmm14
	addpd	%xmm4, %xmm15

#if defined(LN) || defined(LT)
	movaps	%xmm8, %xmm0
	shufpd	$0, %xmm9, %xmm8
	shufpd	$3, %xmm0, %xmm9

	movaps	%xmm10, %xmm0
	shufpd	$0, %xmm11, %xmm10
	shufpd	$3, %xmm0, %xmm11

	movaps	%xmm12, %xmm0
	shufpd	$0, %xmm13, %xmm12
	shufpd	$3, %xmm0, %xmm13

	movaps	%xmm14, %xmm0
	shufpd	$0, %xmm15, %xmm14
	shufpd	$3, %xmm0, %xmm15

	movapd	-16 * SIZE(BO), %xmm0
	movapd	-14 * SIZE(BO), %xmm2
	movapd	-12 * SIZE(BO), %xmm4
	movapd	-10 * SIZE(BO), %xmm6
	movapd	 -8 * SIZE(BO), %xmm1
	movapd	 -6 * SIZE(BO), %xmm3
	movapd	 -4 * SIZE(BO), %xmm5
	movapd	 -2 * SIZE(BO), %xmm7
#else
	movaps	%xmm8, %xmm0
	shufpd	$2, %xmm9, %xmm8
	shufpd	$2, %xmm0, %xmm9

	movaps	%xmm10, %xmm0
	shufpd	$2, %xmm11, %xmm10
	shufpd	$2, %xmm0, %xmm11

	movaps	%xmm12, %xmm0
	shufpd	$2, %xmm13, %xmm12
	shufpd	$2, %xmm0, %xmm13

	movaps	%xmm14, %xmm0
	shufpd	$2, %xmm15, %xmm14
	shufpd	$2, %xmm0, %xmm15

	movapd	 -16 * SIZE(AO), %xmm0
	movapd	 -14 * SIZE(AO), %xmm1
	movapd	 -12 * SIZE(AO), %xmm2
	movapd	 -10 * SIZE(AO), %xmm3

	movapd	  -8 * SIZE(AO), %xmm4
	movapd	  -6 * SIZE(AO), %xmm5
	movapd	  -4 * SIZE(AO), %xmm6
	movapd	  -2 * SIZE(AO), %xmm7
#endif

	subpd	%xmm8,  %xmm0
	subpd	%xmm9,  %xmm1
	subpd	%xmm10, %xmm2
	subpd	%xmm11, %xmm3
	subpd	%xmm12, %xmm4
	subpd	%xmm13, %xmm5
	subpd	%xmm14, %xmm6
	subpd	%xmm15, %xmm7

#ifdef LN
	movddup	-13 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm1
	mulpd	 %xmm8, %xmm3
	mulpd	 %xmm8, %xmm5
	mulpd	 %xmm8, %xmm7

	movddup	-14 * SIZE(AO), %xmm12
	movapd	%xmm12, %xmm13
	movapd	%xmm12, %xmm14
	movapd	%xmm12, %xmm15

	mulpd	%xmm1, %xmm12
	mulpd	%xmm3, %xmm13
	mulpd	%xmm5, %xmm14
	mulpd	%xmm7, %xmm15

	subpd	%xmm12, %xmm0
	subpd	%xmm13, %xmm2
	subpd	%xmm14, %xmm4
	subpd	%xmm15, %xmm6

	movddup	-16 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm0
	mulpd	 %xmm8, %xmm2
	mulpd	 %xmm8, %xmm4
	mulpd	 %xmm8, %xmm6
#endif

#ifdef LT
	movddup	-16 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm0
	mulpd	 %xmm8, %xmm2
	mulpd	 %xmm8, %xmm4
	mulpd	 %xmm8, %xmm6

	movddup	-15 * SIZE(AO), %xmm12
	movapd	%xmm12, %xmm13
	movapd	%xmm12, %xmm14
	movapd	%xmm12, %xmm15

	mulpd	%xmm0, %xmm12
	mulpd	%xmm2, %xmm13
	mulpd	%xmm4, %xmm14
	mulpd	%xmm6, %xmm15

	subpd	%xmm12, %xmm1
	subpd	%xmm13, %xmm3
	subpd	%xmm14, %xmm5
	subpd	%xmm15, %xmm7

	movddup	-13 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm1
	mulpd	 %xmm8, %xmm3
	mulpd	 %xmm8, %xmm5
	mulpd	 %xmm8, %xmm7
#endif

#ifdef RN
	movddup	-16 * SIZE(BO), %xmm8
	mulpd	%xmm8, %xmm0
	movddup	-15 * SIZE(BO), %xmm9
	mulpd	%xmm0,  %xmm9
	subpd	%xmm9,  %xmm1
	movddup	-14 * SIZE(BO), %xmm10
	mulpd	%xmm0,  %xmm10
	subpd	%xmm10, %xmm2
	movddup	-13 * SIZE(BO), %xmm11
	mulpd	%xmm0,  %xmm11
	subpd	%xmm11, %xmm3
	movddup	-12 * SIZE(BO), %xmm12
	mulpd	%xmm0,  %xmm12
	subpd	%xmm12, %xmm4
	movddup	-11 * SIZE(BO), %xmm13
	mulpd	%xmm0,  %xmm13
	subpd	%xmm13, %xmm5
	movddup	-10 * SIZE(BO), %xmm14
	mulpd	%xmm0,  %xmm14
	subpd	%xmm14, %xmm6
	movddup	 -9 * SIZE(BO), %xmm15
	mulpd	%xmm0,  %xmm15
	subpd	%xmm15, %xmm7

	movddup	 -7 * SIZE(BO), %xmm9
	mulpd	%xmm9, %xmm1
	movddup	 -6 * SIZE(BO), %xmm10
	mulpd	%xmm1,  %xmm10
	subpd	%xmm10, %xmm2
	movddup	 -5 * SIZE(BO), %xmm11
	mulpd	%xmm1,  %xmm11
	subpd	%xmm11, %xmm3
	movddup	 -4 * SIZE(BO), %xmm12
	mulpd	%xmm1,  %xmm12
	subpd	%xmm12, %xmm4
	movddup	 -3 * SIZE(BO), %xmm13
	mulpd	%xmm1,  %xmm13
	subpd	%xmm13, %xmm5
	movddup	 -2 * SIZE(BO), %xmm14
	mulpd	%xmm1,  %xmm14
	subpd	%xmm14, %xmm6
	movddup	 -1 * SIZE(BO), %xmm15
	mulpd	%xmm1,  %xmm15
	subpd	%xmm15, %xmm7

	movddup	  2 * SIZE(BO), %xmm10
	mulpd	%xmm10, %xmm2
	movddup	  3 * SIZE(BO), %xmm11
	mulpd	%xmm2,  %xmm11
	subpd	%xmm11, %xmm3
	movddup	  4 * SIZE(BO), %xmm12
	mulpd	%xmm2,  %xmm12
	subpd	%xmm12, %xmm4
	movddup	  5 * SIZE(BO), %xmm13
	mulpd	%xmm2,  %xmm13
	subpd	%xmm13, %xmm5
	movddup	  6 * SIZE(BO), %xmm14
	mulpd	%xmm2,  %xmm14
	subpd	%xmm14, %xmm6
	movddup	  7 * SIZE(BO), %xmm15
	mulpd	%xmm2,  %xmm15
	subpd	%xmm15, %xmm7

	movddup	 11 * SIZE(BO), %xmm11
	mulpd	%xmm11, %xmm3
	movddup	 12 * SIZE(BO), %xmm12
	mulpd	%xmm3,  %xmm12
	subpd	%xmm12, %xmm4
	movddup	 13 * SIZE(BO), %xmm13
	mulpd	%xmm3,  %xmm13
	subpd	%xmm13, %xmm5
	movddup	 14 * SIZE(BO), %xmm14
	mulpd	%xmm3,  %xmm14
	subpd	%xmm14, %xmm6
	movddup	 15 * SIZE(BO), %xmm15
	mulpd	%xmm3,  %xmm15
	subpd	%xmm15, %xmm7

	movddup	 20 * SIZE(BO), %xmm12
	mulpd	%xmm12, %xmm4
	movddup	 21 * SIZE(BO), %xmm13
	mulpd	%xmm4,  %xmm13
	subpd	%xmm13, %xmm5
	movddup	 22 * SIZE(BO), %xmm14
	mulpd	%xmm4,  %xmm14
	subpd	%xmm14, %xmm6
	movddup	 23 * SIZE(BO), %xmm15
	mulpd	%xmm4,  %xmm15
	subpd	%xmm15, %xmm7

	movddup	 29 * SIZE(BO), %xmm13
	mulpd	%xmm13, %xmm5
	movddup	 30 * SIZE(BO), %xmm14
	mulpd	%xmm5,  %xmm14
	subpd	%xmm14, %xmm6
	movddup	 31 * SIZE(BO), %xmm15
	mulpd	%xmm5,  %xmm15
	subpd	%xmm15, %xmm7

	movddup	 38 * SIZE(BO), %xmm14
	mulpd	%xmm14, %xmm6
	movddup	 39 * SIZE(BO), %xmm15
	mulpd	%xmm6,  %xmm15
	subpd	%xmm15, %xmm7

	movddup	 47 * SIZE(BO), %xmm15
	mulpd	%xmm15, %xmm7
#endif

#ifdef RT
	movddup	 47 * SIZE(BO), %xmm8
	mulpd	%xmm8,  %xmm7
	movddup	 46 * SIZE(BO), %xmm9
	mulpd	%xmm7,  %xmm9
	subpd	%xmm9,  %xmm6
	movddup	 45 * SIZE(BO), %xmm10
	mulpd	%xmm7,  %xmm10
	subpd	%xmm10, %xmm5
	movddup	 44 * SIZE(BO), %xmm11
	mulpd	%xmm7,  %xmm11
	subpd	%xmm11, %xmm4
	movddup	 43 * SIZE(BO), %xmm12
	mulpd	%xmm7,  %xmm12
	subpd	%xmm12, %xmm3
	movddup	 42 * SIZE(BO), %xmm13
	mulpd	%xmm7,  %xmm13
	subpd	%xmm13, %xmm2
	movddup	 41 * SIZE(BO), %xmm14
	mulpd	%xmm7,  %xmm14
	subpd	%xmm14, %xmm1
	movddup	 40 * SIZE(BO), %xmm15
	mulpd	%xmm7,  %xmm15
	subpd	%xmm15, %xmm0

	movddup	 38 * SIZE(BO), %xmm9
	mulpd	%xmm9,  %xmm6
	movddup	 37 * SIZE(BO), %xmm10
	mulpd	%xmm6,  %xmm10
	subpd	%xmm10, %xmm5
	movddup	 36 * SIZE(BO), %xmm11
	mulpd	%xmm6,  %xmm11
	subpd	%xmm11, %xmm4
	movddup	 35 * SIZE(BO), %xmm12
	mulpd	%xmm6,  %xmm12
	subpd	%xmm12, %xmm3
	movddup	 34 * SIZE(BO), %xmm13
	mulpd	%xmm6,  %xmm13
	subpd	%xmm13, %xmm2
	movddup	 33 * SIZE(BO), %xmm14
	mulpd	%xmm6,  %xmm14
	subpd	%xmm14, %xmm1
	movddup	 32 * SIZE(BO), %xmm15
	mulpd	%xmm6,  %xmm15
	subpd	%xmm15, %xmm0

	movddup	 29 * SIZE(BO), %xmm10
	mulpd	%xmm10, %xmm5
	movddup	 28 * SIZE(BO), %xmm11
	mulpd	%xmm5,  %xmm11
	subpd	%xmm11, %xmm4
	movddup	 27 * SIZE(BO), %xmm12
	mulpd	%xmm5,  %xmm12
	subpd	%xmm12, %xmm3
	movddup	 26 * SIZE(BO), %xmm13
	mulpd	%xmm5,  %xmm13
	subpd	%xmm13, %xmm2
	movddup	 25 * SIZE(BO), %xmm14
	mulpd	%xmm5,  %xmm14
	subpd	%xmm14, %xmm1
	movddup	 24 * SIZE(BO), %xmm15
	mulpd	%xmm5,  %xmm15
	subpd	%xmm15, %xmm0

	movddup	 20 * SIZE(BO), %xmm11
	mulpd	%xmm11, %xmm4
	movddup	 19 * SIZE(BO), %xmm12
	mulpd	%xmm4,  %xmm12
	subpd	%xmm12, %xmm3
	movddup	 18 * SIZE(BO), %xmm13
	mulpd	%xmm4,  %xmm13
	subpd	%xmm13, %xmm2
	movddup	 17 * SIZE(BO), %xmm14
	mulpd	%xmm4,  %xmm14
	subpd	%xmm14, %xmm1
	movddup	 16 * SIZE(BO), %xmm15
	mulpd	%xmm4,  %xmm15
	subpd	%xmm15, %xmm0

	movddup	 11 * SIZE(BO), %xmm12
	mulpd	%xmm12, %xmm3
	movddup	 10 * SIZE(BO), %xmm13
	mulpd	%xmm3,  %xmm13
	subpd	%xmm13, %xmm2
	movddup	  9 * SIZE(BO), %xmm14
	mulpd	%xmm3,  %xmm14
	subpd	%xmm14, %xmm1
	movddup	  8 * SIZE(BO), %xmm15
	mulpd	%xmm3,  %xmm15
	subpd	%xmm15, %xmm0

	movddup	  2 * SIZE(BO), %xmm13
	mulpd	%xmm13, %xmm2
	movddup	  1 * SIZE(BO), %xmm14
	mulpd	%xmm2,  %xmm14
	subpd	%xmm14, %xmm1
	movddup	  0 * SIZE(BO), %xmm15
	mulpd	%xmm2,  %xmm15
	subpd	%xmm15, %xmm0

	movddup	 -7 * SIZE(BO), %xmm14
	mulpd	%xmm14, %xmm1
	movddup	 -8 * SIZE(BO), %xmm15
	mulpd	%xmm1,  %xmm15
	subpd	%xmm15, %xmm0

	movddup	-16 * SIZE(BO), %xmm15
	mulpd	%xmm15, %xmm0
#endif


#ifdef LN
	subq	$2 * SIZE, CO1
	subq	$2 * SIZE, CO2
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm0,  -16 * SIZE(BO)
	movapd	%xmm2,  -14 * SIZE(BO)
	movapd	%xmm4,  -12 * SIZE(BO)
	movapd	%xmm6,  -10 * SIZE(BO)
	movapd	%xmm1,   -8 * SIZE(BO)
	movapd	%xmm3,   -6 * SIZE(BO)
	movapd	%xmm5,   -4 * SIZE(BO)
	movapd	%xmm7,   -2 * SIZE(BO)
#else
	movapd	%xmm0,  -16 * SIZE(AO)
	movapd	%xmm1,  -14 * SIZE(AO)
	movapd	%xmm2,  -12 * SIZE(AO)
	movapd	%xmm3,  -10 * SIZE(AO)
	movapd	%xmm4,   -8 * SIZE(AO)
	movapd	%xmm5 ,  -6 * SIZE(AO)
	movapd	%xmm6,   -4 * SIZE(AO)
	movapd	%xmm7,   -2 * SIZE(AO)
#endif

	leaq	(LDC, LDC, 2), %rax

#if defined(LN) || defined(LT)
	movsd	%xmm0,  0 * SIZE(CO1)
	movsd	%xmm1,  1 * SIZE(CO1)
	movhps	%xmm0,  0 * SIZE(CO1, LDC,  1)
	movhps	%xmm1,  1 * SIZE(CO1, LDC,  1)

	movsd	%xmm2,  0 * SIZE(CO1, LDC,  2)
	movsd	%xmm3,  1 * SIZE(CO1, LDC,  2)
	movhps	%xmm2,  0 * SIZE(CO1, %rax, 1)
	movhps	%xmm3,  1 * SIZE(CO1, %rax, 1)

	movsd	%xmm4,  0 * SIZE(CO2)
	movsd	%xmm5,  1 * SIZE(CO2)
	movhps	%xmm4,  0 * SIZE(CO2, LDC,  1)
	movhps	%xmm5,  1 * SIZE(CO2, LDC,  1)

	movsd	%xmm6,  0 * SIZE(CO2, LDC,  2)
	movsd	%xmm7,  1 * SIZE(CO2, LDC,  2)
	movhps	%xmm6,  0 * SIZE(CO2, %rax, 1)
	movhps	%xmm7,  1 * SIZE(CO2, %rax, 1)
#else
	movups	%xmm0,  0 * SIZE(CO1)
	movups	%xmm1,  0 * SIZE(CO1, LDC,  1)
	movups	%xmm2,  0 * SIZE(CO1, LDC,  2)
	movups	%xmm3,  0 * SIZE(CO1, %rax, 1)
	movups	%xmm4,  0 * SIZE(CO2)
	movups	%xmm5,  0 * SIZE(CO2, LDC,  1)
	movups	%xmm6,  0 * SIZE(CO2, LDC,  2)
	movups	%xmm7,  0 * SIZE(CO2, %rax, 1)
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

	decq	I
	BRANCH
	jg	.L11
	ALIGN_4

.L20:
	testq	$1, M
	BRANCH
	jle	.L29
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
	leaq	(B,  %rax, 8), BO
#else
	movq	B, BO
#endif

	movddup	-16 * SIZE(AO), %xmm0
	xorps	%xmm8,  %xmm8
	movaps	-16 * SIZE(BO), %xmm1
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
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm8
	movaps	-14 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm9
	movaps	-12 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm10
	movaps	-10 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	movddup	-15 * SIZE(AO), %xmm0
	addpd	%xmm1, %xmm11
	movaps	 -8 * SIZE(BO), %xmm1

	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm8
	movaps	 -6 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm9
	movaps	 -4 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm10
	movaps	 -2 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	movddup	-14 * SIZE(AO), %xmm0
	addpd	%xmm1, %xmm11
	movaps	  0 * SIZE(BO), %xmm1

	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm8
	movaps	  2 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm9
	movaps	  4 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm10
	movaps	  6 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	movddup	-13 * SIZE(AO), %xmm0
	addpd	%xmm1, %xmm11
	movaps	  8 * SIZE(BO), %xmm1

	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm8
	movaps	 10 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm9
	movaps	 12 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm10
	movaps	 14 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	movddup	-12 * SIZE(AO), %xmm0
	addpd	%xmm1, %xmm11
	movaps	 16 * SIZE(BO), %xmm1

	subq	$ -4 * SIZE, AO
	subq	$-32 * SIZE, BO

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
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm8
	movaps	-14 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm9
	movaps	-12 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm10
	movaps	-10 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	movddup	-15 * SIZE(AO), %xmm0
	addpd	%xmm1, %xmm11
	movaps	 -8 * SIZE(BO), %xmm1

	addq	$1 * SIZE, AO
	addq	$8 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L26
	ALIGN_4

.L28:
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

#if defined(LN) || defined(LT)
	movapd	-16 * SIZE(BO), %xmm0
	movapd	-14 * SIZE(BO), %xmm1
	movapd	-12 * SIZE(BO), %xmm2
	movapd	-10 * SIZE(BO), %xmm3
#else
	movapd	-16 * SIZE(AO), %xmm0
	movapd	-14 * SIZE(AO), %xmm1
	movapd	-12 * SIZE(AO), %xmm2
	movapd	-10 * SIZE(AO), %xmm3
#endif

	subpd	%xmm8,  %xmm0
	subpd	%xmm9,  %xmm1
	subpd	%xmm10, %xmm2
	subpd	%xmm11, %xmm3

#if defined(LN) || defined(LT)
	movddup	-16 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm0
	mulpd	 %xmm8, %xmm1
	mulpd	 %xmm8, %xmm2
	mulpd	 %xmm8, %xmm3
#endif

#if defined(RN) || defined(RT)
	pshufd	$0xe, %xmm3, %xmm7
	movaps	%xmm3, %xmm6
	pshufd	$0xe, %xmm2, %xmm5
	movaps	%xmm2, %xmm4
	pshufd	$0xe, %xmm1, %xmm3
	movaps	%xmm1, %xmm2
	pshufd	$0xe, %xmm0, %xmm1
#endif

#ifdef RN
	movsd	-16 * SIZE(BO), %xmm8
	mulsd	%xmm8, %xmm0
	movsd	-15 * SIZE(BO), %xmm9
	mulsd	%xmm0,  %xmm9
	subsd	%xmm9,  %xmm1
	movsd	-14 * SIZE(BO), %xmm10
	mulsd	%xmm0,  %xmm10
	subsd	%xmm10, %xmm2
	movsd	-13 * SIZE(BO), %xmm11
	mulsd	%xmm0,  %xmm11
	subsd	%xmm11, %xmm3
	movsd	-12 * SIZE(BO), %xmm12
	mulsd	%xmm0,  %xmm12
	subsd	%xmm12, %xmm4
	movsd	-11 * SIZE(BO), %xmm13
	mulsd	%xmm0,  %xmm13
	subsd	%xmm13, %xmm5
	movsd	-10 * SIZE(BO), %xmm14
	mulsd	%xmm0,  %xmm14
	subsd	%xmm14, %xmm6
	movsd	 -9 * SIZE(BO), %xmm15
	mulsd	%xmm0,  %xmm15
	subsd	%xmm15, %xmm7

	movsd	 -7 * SIZE(BO), %xmm9
	mulsd	%xmm9, %xmm1
	movsd	 -6 * SIZE(BO), %xmm10
	mulsd	%xmm1,  %xmm10
	subsd	%xmm10, %xmm2
	movsd	 -5 * SIZE(BO), %xmm11
	mulsd	%xmm1,  %xmm11
	subsd	%xmm11, %xmm3
	movsd	 -4 * SIZE(BO), %xmm12
	mulsd	%xmm1,  %xmm12
	subsd	%xmm12, %xmm4
	movsd	 -3 * SIZE(BO), %xmm13
	mulsd	%xmm1,  %xmm13
	subsd	%xmm13, %xmm5
	movsd	 -2 * SIZE(BO), %xmm14
	mulsd	%xmm1,  %xmm14
	subsd	%xmm14, %xmm6
	movsd	 -1 * SIZE(BO), %xmm15
	mulsd	%xmm1,  %xmm15
	subsd	%xmm15, %xmm7

	movsd	  2 * SIZE(BO), %xmm10
	mulsd	%xmm10, %xmm2
	movsd	  3 * SIZE(BO), %xmm11
	mulsd	%xmm2,  %xmm11
	subsd	%xmm11, %xmm3
	movsd	  4 * SIZE(BO), %xmm12
	mulsd	%xmm2,  %xmm12
	subsd	%xmm12, %xmm4
	movsd	  5 * SIZE(BO), %xmm13
	mulsd	%xmm2,  %xmm13
	subsd	%xmm13, %xmm5
	movsd	  6 * SIZE(BO), %xmm14
	mulsd	%xmm2,  %xmm14
	subsd	%xmm14, %xmm6
	movsd	  7 * SIZE(BO), %xmm15
	mulsd	%xmm2,  %xmm15
	subsd	%xmm15, %xmm7

	movsd	 11 * SIZE(BO), %xmm11
	mulsd	%xmm11, %xmm3
	movsd	 12 * SIZE(BO), %xmm12
	mulsd	%xmm3,  %xmm12
	subsd	%xmm12, %xmm4
	movsd	 13 * SIZE(BO), %xmm13
	mulsd	%xmm3,  %xmm13
	subsd	%xmm13, %xmm5
	movsd	 14 * SIZE(BO), %xmm14
	mulsd	%xmm3,  %xmm14
	subsd	%xmm14, %xmm6
	movsd	 15 * SIZE(BO), %xmm15
	mulsd	%xmm3,  %xmm15
	subsd	%xmm15, %xmm7

	movsd	 20 * SIZE(BO), %xmm12
	mulsd	%xmm12, %xmm4
	movsd	 21 * SIZE(BO), %xmm13
	mulsd	%xmm4,  %xmm13
	subsd	%xmm13, %xmm5
	movsd	 22 * SIZE(BO), %xmm14
	mulsd	%xmm4,  %xmm14
	subsd	%xmm14, %xmm6
	movsd	 23 * SIZE(BO), %xmm15
	mulsd	%xmm4,  %xmm15
	subsd	%xmm15, %xmm7

	movsd	 29 * SIZE(BO), %xmm13
	mulsd	%xmm13, %xmm5
	movsd	 30 * SIZE(BO), %xmm14
	mulsd	%xmm5,  %xmm14
	subsd	%xmm14, %xmm6
	movsd	 31 * SIZE(BO), %xmm15
	mulsd	%xmm5,  %xmm15
	subsd	%xmm15, %xmm7

	movsd	 38 * SIZE(BO), %xmm14
	mulsd	%xmm14, %xmm6
	movsd	 39 * SIZE(BO), %xmm15
	mulsd	%xmm6,  %xmm15
	subsd	%xmm15, %xmm7

	movsd	 47 * SIZE(BO), %xmm15
	mulsd	%xmm15, %xmm7
#endif

#ifdef RT
	movsd	 47 * SIZE(BO), %xmm8
	mulsd	%xmm8,  %xmm7
	movsd	 46 * SIZE(BO), %xmm9
	mulsd	%xmm7,  %xmm9
	subsd	%xmm9,  %xmm6
	movsd	 45 * SIZE(BO), %xmm10
	mulsd	%xmm7,  %xmm10
	subsd	%xmm10, %xmm5
	movsd	 44 * SIZE(BO), %xmm11
	mulsd	%xmm7,  %xmm11
	subsd	%xmm11, %xmm4
	movsd	 43 * SIZE(BO), %xmm12
	mulsd	%xmm7,  %xmm12
	subsd	%xmm12, %xmm3
	movsd	 42 * SIZE(BO), %xmm13
	mulsd	%xmm7,  %xmm13
	subsd	%xmm13, %xmm2
	movsd	 41 * SIZE(BO), %xmm14
	mulsd	%xmm7,  %xmm14
	subsd	%xmm14, %xmm1
	movsd	 40 * SIZE(BO), %xmm15
	mulsd	%xmm7,  %xmm15
	subsd	%xmm15, %xmm0

	movsd	 38 * SIZE(BO), %xmm9
	mulsd	%xmm9,  %xmm6
	movsd	 37 * SIZE(BO), %xmm10
	mulsd	%xmm6,  %xmm10
	subsd	%xmm10, %xmm5
	movsd	 36 * SIZE(BO), %xmm11
	mulsd	%xmm6,  %xmm11
	subsd	%xmm11, %xmm4
	movsd	 35 * SIZE(BO), %xmm12
	mulsd	%xmm6,  %xmm12
	subsd	%xmm12, %xmm3
	movsd	 34 * SIZE(BO), %xmm13
	mulsd	%xmm6,  %xmm13
	subsd	%xmm13, %xmm2
	movsd	 33 * SIZE(BO), %xmm14
	mulsd	%xmm6,  %xmm14
	subsd	%xmm14, %xmm1
	movsd	 32 * SIZE(BO), %xmm15
	mulsd	%xmm6,  %xmm15
	subsd	%xmm15, %xmm0

	movsd	 29 * SIZE(BO), %xmm10
	mulsd	%xmm10, %xmm5
	movsd	 28 * SIZE(BO), %xmm11
	mulsd	%xmm5,  %xmm11
	subsd	%xmm11, %xmm4
	movsd	 27 * SIZE(BO), %xmm12
	mulsd	%xmm5,  %xmm12
	subsd	%xmm12, %xmm3
	movsd	 26 * SIZE(BO), %xmm13
	mulsd	%xmm5,  %xmm13
	subsd	%xmm13, %xmm2
	movsd	 25 * SIZE(BO), %xmm14
	mulsd	%xmm5,  %xmm14
	subsd	%xmm14, %xmm1
	movsd	 24 * SIZE(BO), %xmm15
	mulsd	%xmm5,  %xmm15
	subsd	%xmm15, %xmm0

	movsd	 20 * SIZE(BO), %xmm11
	mulsd	%xmm11, %xmm4
	movsd	 19 * SIZE(BO), %xmm12
	mulsd	%xmm4,  %xmm12
	subsd	%xmm12, %xmm3
	movsd	 18 * SIZE(BO), %xmm13
	mulsd	%xmm4,  %xmm13
	subsd	%xmm13, %xmm2
	movsd	 17 * SIZE(BO), %xmm14
	mulsd	%xmm4,  %xmm14
	subsd	%xmm14, %xmm1
	movsd	 16 * SIZE(BO), %xmm15
	mulsd	%xmm4,  %xmm15
	subsd	%xmm15, %xmm0

	movsd	 11 * SIZE(BO), %xmm12
	mulsd	%xmm12, %xmm3
	movsd	 10 * SIZE(BO), %xmm13
	mulsd	%xmm3,  %xmm13
	subsd	%xmm13, %xmm2
	movsd	  9 * SIZE(BO), %xmm14
	mulsd	%xmm3,  %xmm14
	subsd	%xmm14, %xmm1
	movsd	  8 * SIZE(BO), %xmm15
	mulsd	%xmm3,  %xmm15
	subsd	%xmm15, %xmm0

	movsd	  2 * SIZE(BO), %xmm13
	mulsd	%xmm13, %xmm2
	movsd	  1 * SIZE(BO), %xmm14
	mulsd	%xmm2,  %xmm14
	subsd	%xmm14, %xmm1
	movsd	  0 * SIZE(BO), %xmm15
	mulsd	%xmm2,  %xmm15
	subsd	%xmm15, %xmm0

	movsd	 -7 * SIZE(BO), %xmm14
	mulsd	%xmm14, %xmm1
	movsd	 -8 * SIZE(BO), %xmm15
	mulsd	%xmm1,  %xmm15
	subsd	%xmm15, %xmm0

	movsd	-16 * SIZE(BO), %xmm15
	mulsd	%xmm15, %xmm0
#endif

#if defined(RN) || defined(RT)
	unpcklpd   %xmm1, %xmm0
	movaps	   %xmm2, %xmm1
	unpcklpd   %xmm3, %xmm1
	movaps	   %xmm4, %xmm2
	unpcklpd   %xmm5, %xmm2
	movaps	   %xmm6, %xmm3
	unpcklpd   %xmm7, %xmm3
#endif

#ifdef LN
	subq	$1 * SIZE, CO1
	subq	$1 * SIZE, CO2
#endif

	leaq	(LDC, LDC, 2), %rax

	movsd	%xmm0,  0 * SIZE(CO1)
	movhps	%xmm0,  0 * SIZE(CO1, LDC,  1)
	movsd	%xmm1,  0 * SIZE(CO1, LDC,  2)
	movhps	%xmm1,  0 * SIZE(CO1, %rax, 1)
	movsd	%xmm2,  0 * SIZE(CO2)
	movhps	%xmm2,  0 * SIZE(CO2, LDC,  1)
	movsd	%xmm3,  0 * SIZE(CO2, LDC,  2)
	movhps	%xmm3,  0 * SIZE(CO2, %rax, 1)

#if defined(LN) || defined(LT)
	movapd	%xmm0,  -16 * SIZE(BO)
	movapd	%xmm1,  -14 * SIZE(BO)
	movapd	%xmm2,  -12 * SIZE(BO)
	movapd	%xmm3,  -10 * SIZE(BO)
#else
	movapd	%xmm0,  -16 * SIZE(AO)
	movapd	%xmm1,  -14 * SIZE(AO)
	movapd	%xmm2,  -12 * SIZE(AO)
	movapd	%xmm3,  -10 * SIZE(AO)
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

.L29:
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
	jg	.L01
	ALIGN_4

.L30:
	testq	$4, N
	jle	.L50
	ALIGN_4

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
	sarq	$1, I
	NOBRANCH
	jle	.L40
	ALIGN_4

.L31:
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
	movaps	-16 * SIZE(AO), %xmm0
	xorps	%xmm2, %xmm2
	xorps	%xmm3, %xmm3
	xorps	%xmm4, %xmm4

	xorps	%xmm8,  %xmm8
	prefetcht0     2 * SIZE(CO1)
	xorps	%xmm9,  %xmm9
	prefetcht0     2 * SIZE(CO1, LDC,  1)
	xorps	%xmm10, %xmm10
	prefetcht0     2 * SIZE(CO2)
	xorps	%xmm11, %xmm11
	prefetcht0     2 * SIZE(CO2, LDC,  1)

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
	PREFETCH	(PREFETCHSIZE +  0) * SIZE(AO)

	addpd	%xmm1, %xmm8
	movaps	-16 * SIZE(BO), %xmm1
	addpd	%xmm2, %xmm9
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2

	addpd	%xmm3, %xmm10
	movaps	-14 * SIZE(BO), %xmm3
	addpd	%xmm4, %xmm11
	pshufd	$0x4e, %xmm3, %xmm4
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm4

	movaps	-14 * SIZE(AO), %xmm0

	addpd	%xmm1, %xmm8
	movaps	-12 * SIZE(BO), %xmm1
	addpd	%xmm2, %xmm9
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2

	addpd	%xmm3, %xmm10
	movaps	-10 * SIZE(BO), %xmm3
	addpd	%xmm4, %xmm11
	pshufd	$0x4e, %xmm3, %xmm4
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm4

	movaps	-12 * SIZE(AO), %xmm0

	addpd	%xmm1, %xmm8
	movaps	 -8 * SIZE(BO), %xmm1
	addpd	%xmm2, %xmm9
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2

	addpd	%xmm3, %xmm10
	movaps	 -6 * SIZE(BO), %xmm3
	addpd	%xmm4, %xmm11
	pshufd	$0x4e, %xmm3, %xmm4
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm4

	movaps	-10 * SIZE(AO), %xmm0

	addpd	%xmm1, %xmm8
	movaps	 -4 * SIZE(BO), %xmm1
	addpd	%xmm2, %xmm9
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2

	addpd	%xmm3, %xmm10
	movaps	 -2 * SIZE(BO), %xmm3
	addpd	%xmm4, %xmm11
	pshufd	$0x4e, %xmm3, %xmm4
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm4

	movaps	 -8 * SIZE(AO), %xmm0

	subq	$-8  * SIZE, AO
	subq	$-16 * SIZE, BO

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
	addpd	%xmm1, %xmm8
	movaps	-16 * SIZE(BO), %xmm1
	addpd	%xmm2, %xmm9
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2

	addpd	%xmm3, %xmm10
	movaps	-14 * SIZE(BO), %xmm3
	addpd	%xmm4, %xmm11
	pshufd	$0x4e, %xmm3, %xmm4
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm4

	movaps	-14 * SIZE(AO), %xmm0

	addq	$2 * SIZE, AO
	addq	$4 * SIZE, BO

	subq	$1, %rax
	BRANCH
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

	leaq	(, %rax, SIZE), %rax

	movq	AORIG, AO
	leaq	(AO, %rax, 2), AO
	leaq	(B,  %rax, 4), BO
#endif

	addpd	%xmm1, %xmm8
	addpd	%xmm2, %xmm9
	addpd	%xmm3, %xmm10
	addpd	%xmm4, %xmm11

#if defined(LN) || defined(LT)
	movaps	%xmm8, %xmm0
	shufpd	$0, %xmm9, %xmm8
	shufpd	$3, %xmm0, %xmm9

	movaps	%xmm10, %xmm0
	shufpd	$0, %xmm11, %xmm10
	shufpd	$3, %xmm0, %xmm11

	movapd	-16 * SIZE(BO), %xmm0
	movapd	-14 * SIZE(BO), %xmm2
	movapd	-12 * SIZE(BO), %xmm1
	movapd	-10 * SIZE(BO), %xmm3
#else
	movaps	%xmm8, %xmm0
	shufpd	$2, %xmm9, %xmm8
	shufpd	$2, %xmm0, %xmm9

	movaps	%xmm10, %xmm0
	shufpd	$2, %xmm11, %xmm10
	shufpd	$2, %xmm0, %xmm11

	movapd	 -16 * SIZE(AO), %xmm0
	movapd	 -14 * SIZE(AO), %xmm1
	movapd	 -12 * SIZE(AO), %xmm2
	movapd	 -10 * SIZE(AO), %xmm3
#endif

	subpd	%xmm8,  %xmm0
	subpd	%xmm9,  %xmm1
	subpd	%xmm10, %xmm2
	subpd	%xmm11, %xmm3

#ifdef LN
	movddup	-13 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm1
	mulpd	 %xmm8, %xmm3

	movddup	-14 * SIZE(AO), %xmm12
	movapd	%xmm12, %xmm13

	mulpd	%xmm1, %xmm12
	mulpd	%xmm3, %xmm13

	subpd	%xmm12, %xmm0
	subpd	%xmm13, %xmm2

	movddup	-16 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm0
	mulpd	 %xmm8, %xmm2
#endif

#ifdef LT
	movddup	-16 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm0
	mulpd	 %xmm8, %xmm2

	movddup	-15 * SIZE(AO), %xmm12
	movapd	%xmm12, %xmm13

	mulpd	%xmm0, %xmm12
	mulpd	%xmm2, %xmm13

	subpd	%xmm12, %xmm1
	subpd	%xmm13, %xmm3

	movddup	-13 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm1
	mulpd	 %xmm8, %xmm3
#endif

#ifdef RN
	movddup	-16 * SIZE(BO), %xmm8
	mulpd	%xmm8, %xmm0
	movddup	-15 * SIZE(BO), %xmm9
	mulpd	%xmm0,  %xmm9
	subpd	%xmm9,  %xmm1
	movddup	-14 * SIZE(BO), %xmm10
	mulpd	%xmm0,  %xmm10
	subpd	%xmm10, %xmm2
	movddup	-13 * SIZE(BO), %xmm11
	mulpd	%xmm0,  %xmm11
	subpd	%xmm11, %xmm3

	movddup	-11 * SIZE(BO), %xmm9
	mulpd	%xmm9, %xmm1
	movddup	-10 * SIZE(BO), %xmm10
	mulpd	%xmm1,  %xmm10
	subpd	%xmm10, %xmm2
	movddup	 -9 * SIZE(BO), %xmm11
	mulpd	%xmm1,  %xmm11
	subpd	%xmm11, %xmm3

	movddup	 -6 * SIZE(BO), %xmm10
	mulpd	%xmm10, %xmm2
	movddup	 -5 * SIZE(BO), %xmm11
	mulpd	%xmm2,  %xmm11
	subpd	%xmm11, %xmm3

	movddup	 -1 * SIZE(BO), %xmm11
	mulpd	%xmm11, %xmm3
#endif

#ifdef RT
	movddup	 -1 * SIZE(BO), %xmm12
	mulpd	%xmm12, %xmm3
	movddup	 -2 * SIZE(BO), %xmm13
	mulpd	%xmm3,  %xmm13
	subpd	%xmm13, %xmm2
	movddup	 -3 * SIZE(BO), %xmm14
	mulpd	%xmm3,  %xmm14
	subpd	%xmm14, %xmm1
	movddup	 -4 * SIZE(BO), %xmm15
	mulpd	%xmm3,  %xmm15
	subpd	%xmm15, %xmm0

	movddup	 -6 * SIZE(BO), %xmm13
	mulpd	%xmm13, %xmm2
	movddup	 -7 * SIZE(BO), %xmm14
	mulpd	%xmm2,  %xmm14
	subpd	%xmm14, %xmm1
	movddup	 -8 * SIZE(BO), %xmm15
	mulpd	%xmm2,  %xmm15
	subpd	%xmm15, %xmm0

	movddup	-11 * SIZE(BO), %xmm14
	mulpd	%xmm14, %xmm1
	movddup	-12 * SIZE(BO), %xmm15
	mulpd	%xmm1,  %xmm15
	subpd	%xmm15, %xmm0

	movddup	-16 * SIZE(BO), %xmm15
	mulpd	%xmm15, %xmm0
#endif


#ifdef LN
	subq	$2 * SIZE, CO1
	subq	$2 * SIZE, CO2
#endif

	leaq	(LDC, LDC, 2), %rax

#if defined(LN) || defined(LT)
	movsd	%xmm0,  0 * SIZE(CO1)
	movsd	%xmm1,  1 * SIZE(CO1)
	movhps	%xmm0,  0 * SIZE(CO1, LDC,  1)
	movhps	%xmm1,  1 * SIZE(CO1, LDC,  1)

	movsd	%xmm2,  0 * SIZE(CO2)
	movsd	%xmm3,  1 * SIZE(CO2)
	movhps	%xmm2,  0 * SIZE(CO2, LDC,  1)
	movhps	%xmm3,  1 * SIZE(CO2, LDC,  1)
#else
	movsd	%xmm0,  0 * SIZE(CO1)
	movhps	%xmm0,  1 * SIZE(CO1)
	movsd	%xmm1,  0 * SIZE(CO1, LDC,  1)
	movhps	%xmm1,  1 * SIZE(CO1, LDC,  1)

	movsd	%xmm2,  0 * SIZE(CO2)
	movhps	%xmm2,  1 * SIZE(CO2)
	movsd	%xmm3,  0 * SIZE(CO2, LDC,  1)
	movhps	%xmm3,  1 * SIZE(CO2, LDC,  1)
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm0,  -16 * SIZE(BO)
	movapd	%xmm2,  -14 * SIZE(BO)
	movapd	%xmm1,  -12 * SIZE(BO)
	movapd	%xmm3,  -10 * SIZE(BO)
#else
	movapd	%xmm0,  -16 * SIZE(AO)
	movapd	%xmm1,  -14 * SIZE(AO)
	movapd	%xmm2,  -12 * SIZE(AO)
	movapd	%xmm3,  -10 * SIZE(AO)
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

	decq	I
	BRANCH
	jg	.L31
	ALIGN_4

.L40:
	testq	$1, M
	BRANCH
	jle	.L49
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

	movddup	-16 * SIZE(AO), %xmm0
	xorps	%xmm8,  %xmm8
	movaps	-16 * SIZE(BO), %xmm1
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
	jle	.L45
	ALIGN_3

.L42:
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm8
	movaps	-14 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	movddup	-15 * SIZE(AO), %xmm0
	addpd	%xmm1, %xmm9
	movaps	-12 * SIZE(BO), %xmm1

	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm10
	movaps	-10 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	movddup	-14 * SIZE(AO), %xmm0
	addpd	%xmm1, %xmm11
	movaps	 -8 * SIZE(BO), %xmm1

	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm8
	movaps	 -6 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	movddup	-13 * SIZE(AO), %xmm0
	addpd	%xmm1, %xmm9
	movaps	 -4 * SIZE(BO), %xmm1

	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm10
	movaps	 -2 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	movddup	-12 * SIZE(AO), %xmm0
	addpd	%xmm1, %xmm11
	movaps	  0 * SIZE(BO), %xmm1

	subq	$ -4 * SIZE, AO
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
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm8
	movaps	-14 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	movddup	-15 * SIZE(AO), %xmm0
	addpd	%xmm1, %xmm9
	movaps	-12 * SIZE(BO), %xmm1

	addq	$1 * SIZE, AO
	addq	$4 * SIZE, BO

	subq	$1, %rax
	BRANCH
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

	leaq	(, %rax, SIZE), %rax

	movq	AORIG, AO
	leaq	(AO, %rax, 1), AO
	leaq	(B,  %rax, 4), BO
#endif

	addpd	%xmm10, %xmm8
	addpd	%xmm11, %xmm9

#if defined(LN) || defined(LT)
	movapd	-16 * SIZE(BO), %xmm0
	movapd	-14 * SIZE(BO), %xmm1
#else
	movapd	-16 * SIZE(AO), %xmm0
	movapd	-14 * SIZE(AO), %xmm1
#endif

	subpd	%xmm8,  %xmm0
	subpd	%xmm9,  %xmm1

#if defined(LN) || defined(LT)
	movddup	-16 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm0
	mulpd	 %xmm8, %xmm1
#endif

#if defined(RN) || defined(RT)
	pshufd	$0xe, %xmm1, %xmm3
	movaps	%xmm1, %xmm2
	pshufd	$0xe, %xmm0, %xmm1
#endif

#ifdef RN
	movsd	-16 * SIZE(BO), %xmm8
	mulsd	%xmm8, %xmm0
	movsd	-15 * SIZE(BO), %xmm9
	mulsd	%xmm0,  %xmm9
	subsd	%xmm9,  %xmm1
	movsd	-14 * SIZE(BO), %xmm10
	mulsd	%xmm0,  %xmm10
	subsd	%xmm10, %xmm2
	movsd	-13 * SIZE(BO), %xmm11
	mulsd	%xmm0,  %xmm11
	subsd	%xmm11, %xmm3

	movsd	-11 * SIZE(BO), %xmm9
	mulsd	%xmm9, %xmm1
	movsd	-10 * SIZE(BO), %xmm10
	mulsd	%xmm1,  %xmm10
	subsd	%xmm10, %xmm2
	movsd	 -9 * SIZE(BO), %xmm11
	mulsd	%xmm1,  %xmm11
	subsd	%xmm11, %xmm3

	movsd	 -6 * SIZE(BO), %xmm10
	mulsd	%xmm10, %xmm2
	movsd	 -5 * SIZE(BO), %xmm11
	mulsd	%xmm2,  %xmm11
	subsd	%xmm11, %xmm3

	movsd	 -1 * SIZE(BO), %xmm11
	mulsd	%xmm11, %xmm3
#endif

#ifdef RT
	movsd	 -1 * SIZE(BO), %xmm12
	mulsd	%xmm12, %xmm3
	movsd	 -2 * SIZE(BO), %xmm13
	mulsd	%xmm3,  %xmm13
	subsd	%xmm13, %xmm2
	movsd	 -3 * SIZE(BO), %xmm14
	mulsd	%xmm3,  %xmm14
	subsd	%xmm14, %xmm1
	movsd	 -4 * SIZE(BO), %xmm15
	mulsd	%xmm3,  %xmm15
	subsd	%xmm15, %xmm0

	movsd	 -6 * SIZE(BO), %xmm13
	mulsd	%xmm13, %xmm2
	movsd	 -7 * SIZE(BO), %xmm14
	mulsd	%xmm2,  %xmm14
	subsd	%xmm14, %xmm1
	movsd	 -8 * SIZE(BO), %xmm15
	mulsd	%xmm2,  %xmm15
	subsd	%xmm15, %xmm0

	movsd	-11 * SIZE(BO), %xmm14
	mulsd	%xmm14, %xmm1
	movsd	-12 * SIZE(BO), %xmm15
	mulsd	%xmm1,  %xmm15
	subsd	%xmm15, %xmm0

	movsd	-16 * SIZE(BO), %xmm15
	mulsd	%xmm15, %xmm0
#endif

#if defined(RN) || defined(RT)
	unpcklpd   %xmm1, %xmm0
	movaps	   %xmm2, %xmm1
	unpcklpd   %xmm3, %xmm1
#endif

#ifdef LN
	subq	$1 * SIZE, CO1
	subq	$1 * SIZE, CO2
#endif

	movsd	%xmm0,  0 * SIZE(CO1)
	movhps	%xmm0,  0 * SIZE(CO1, LDC,  1)
	movsd	%xmm1,  0 * SIZE(CO2)
	movhps	%xmm1,  0 * SIZE(CO2, LDC,  1)

#if defined(LN) || defined(LT)
	movapd	%xmm0,  -16 * SIZE(BO)
	movapd	%xmm1,  -14 * SIZE(BO)
#else
	movapd	%xmm0,  -16 * SIZE(AO)
	movapd	%xmm1,  -14 * SIZE(AO)
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

.L49:
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

.L50:
	testq	$2, N
	jle	.L70
	ALIGN_4

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
	sarq	$1, I
	NOBRANCH
	jle	.L60
	ALIGN_4

.L51:
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
	movaps	-16 * SIZE(AO), %xmm0
	xorps	%xmm2, %xmm2

	xorps	%xmm8,  %xmm8
	prefetcht0     2 * SIZE(CO1)
	xorps	%xmm9,  %xmm9
	prefetcht0     2 * SIZE(CO2)
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
	jle	.L55
	ALIGN_3

.L52:
	PREFETCH	(PREFETCHSIZE +  0) * SIZE(AO)

	addpd	%xmm1, %xmm8
	movaps	-16 * SIZE(BO), %xmm1
	addpd	%xmm2, %xmm9
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2
	movaps	-14 * SIZE(AO), %xmm0

	addpd	%xmm1, %xmm10
	movaps	-14 * SIZE(BO), %xmm1
	addpd	%xmm2, %xmm11
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2
	movaps	-12 * SIZE(AO), %xmm0

	addpd	%xmm1, %xmm8
	movaps	-12 * SIZE(BO), %xmm1
	addpd	%xmm2, %xmm9
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2
	movaps	-10 * SIZE(AO), %xmm0

	addpd	%xmm1, %xmm10
	movaps	-10 * SIZE(BO), %xmm1
	addpd	%xmm2, %xmm11
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2
	movaps	 -8 * SIZE(AO), %xmm0

	subq	$-8 * SIZE, AO
	subq	$-8 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L52

	addpd	%xmm10, %xmm8
	addpd	%xmm11, %xmm9
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
	addpd	%xmm1, %xmm8
	movaps	-16 * SIZE(BO), %xmm1
	addpd	%xmm2, %xmm9
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2
	movaps	-14 * SIZE(AO), %xmm0

	addq	$2 * SIZE, AO
	addq	$2 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L56
	ALIGN_4

.L58:
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

	addpd	%xmm1, %xmm8
	addpd	%xmm2, %xmm9

#if defined(LN) || defined(LT)
	movaps	%xmm8, %xmm0
	shufpd	$0, %xmm9, %xmm8
	shufpd	$3, %xmm0, %xmm9

	movapd	-16 * SIZE(BO), %xmm0
	movapd	-14 * SIZE(BO), %xmm1
#else
	movaps	%xmm8, %xmm0
	shufpd	$2, %xmm9, %xmm8
	shufpd	$2, %xmm0, %xmm9

	movapd	 -16 * SIZE(AO), %xmm0
	movapd	 -14 * SIZE(AO), %xmm1
#endif

	subpd	%xmm8,  %xmm0
	subpd	%xmm9,  %xmm1

#ifdef LN
	movddup	-13 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm1
	movddup	-14 * SIZE(AO), %xmm12
	mulpd	%xmm1, %xmm12
	subpd	%xmm12, %xmm0
	movddup	-16 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm0
#endif

#ifdef LT
	movddup	-16 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm0
	movddup	-15 * SIZE(AO), %xmm12
	mulpd	%xmm0, %xmm12
	subpd	%xmm12, %xmm1
	movddup	-13 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm1
#endif

#ifdef RN
	movddup	-16 * SIZE(BO), %xmm10
	mulpd	%xmm10, %xmm0
	movddup	-15 * SIZE(BO), %xmm11
	mulpd	%xmm0,  %xmm11
	subpd	%xmm11, %xmm1

	movddup	-13 * SIZE(BO), %xmm11
	mulpd	%xmm11, %xmm1
#endif

#ifdef RT
	movddup	-13 * SIZE(BO), %xmm14
	mulpd	%xmm14, %xmm1
	movddup	-14 * SIZE(BO), %xmm15
	mulpd	%xmm1,  %xmm15
	subpd	%xmm15, %xmm0

	movddup	-16 * SIZE(BO), %xmm15
	mulpd	%xmm15, %xmm0
#endif

#ifdef LN
	subq	$2 * SIZE, CO1
	subq	$2 * SIZE, CO2
#endif

#if defined(LN) || defined(LT)
	movsd	%xmm0,  0 * SIZE(CO1)
	movsd	%xmm1,  1 * SIZE(CO1)
	movhps	%xmm0,  0 * SIZE(CO2)
	movhps	%xmm1,  1 * SIZE(CO2)
#else
	movsd	%xmm0,  0 * SIZE(CO1)
	movhps	%xmm0,  1 * SIZE(CO1)
	movsd	%xmm1,  0 * SIZE(CO2)
	movhps	%xmm1,  1 * SIZE(CO2)
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm0,  -16 * SIZE(BO)
	movapd	%xmm1,  -14 * SIZE(BO)
#else
	movapd	%xmm0,  -16 * SIZE(AO)
	movapd	%xmm1,  -14 * SIZE(AO)
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

	decq	I
	BRANCH
	jg	.L51
	ALIGN_4

.L60:
	testq	$1, M
	BRANCH
	jle	.L69
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

	movddup	-16 * SIZE(AO), %xmm0
	xorps	%xmm8,  %xmm8
	movaps	-16 * SIZE(BO), %xmm1
	xorps	%xmm9,  %xmm9

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
	mulpd	%xmm0, %xmm1
	movddup	-15 * SIZE(AO), %xmm0
	addpd	%xmm1, %xmm8
	movaps	-14 * SIZE(BO), %xmm1

	mulpd	%xmm0, %xmm1
	movddup	-14 * SIZE(AO), %xmm0
	addpd	%xmm1, %xmm9
	movaps	-12 * SIZE(BO), %xmm1

	mulpd	%xmm0, %xmm1
	movddup	-13 * SIZE(AO), %xmm0
	addpd	%xmm1, %xmm8
	movaps	-10 * SIZE(BO), %xmm1

	mulpd	%xmm0, %xmm1
	movddup	-12 * SIZE(AO), %xmm0
	addpd	%xmm1, %xmm9
	movaps	 -8 * SIZE(BO), %xmm1

	subq	$-4 * SIZE, AO
	subq	$-8 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L62
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
	mulpd	%xmm0, %xmm1
	movddup	-15 * SIZE(AO), %xmm0
	addpd	%xmm1, %xmm8
	movaps	-14 * SIZE(BO), %xmm1

	addq	$1 * SIZE, AO
	addq	$2 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L66
	ALIGN_4

.L68:
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

#if defined(LN) || defined(LT)
	movapd	-16 * SIZE(BO), %xmm0
#else
	movapd	-16 * SIZE(AO), %xmm0
#endif

	subpd	%xmm8,  %xmm0

#if defined(LN) || defined(LT)
	movddup	-16 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm0
#endif

#if defined(RN) || defined(RT)
	pshufd	$0xe, %xmm0, %xmm1
#endif

#ifdef RN
	movsd	-16 * SIZE(BO), %xmm10
	mulsd	%xmm10, %xmm0
	movsd	-15 * SIZE(BO), %xmm11
	mulsd	%xmm0,  %xmm11
	subsd	%xmm11, %xmm1

	movsd	-13 * SIZE(BO), %xmm11
	mulsd	%xmm11, %xmm1
#endif

#ifdef RT
	movsd	-13 * SIZE(BO), %xmm14
	mulsd	%xmm14, %xmm1
	movsd	-14 * SIZE(BO), %xmm15
	mulsd	%xmm1,  %xmm15
	subsd	%xmm15, %xmm0

	movsd	-16 * SIZE(BO), %xmm15
	mulsd	%xmm15, %xmm0
#endif

#if defined(RN) || defined(RT)
	unpcklpd   %xmm1, %xmm0
#endif

#ifdef LN
	subq	$1 * SIZE, CO1
	subq	$1 * SIZE, CO2
#endif

	movsd	%xmm0,  0 * SIZE(CO1)
	movhps	%xmm0,  0 * SIZE(CO2)

#if defined(LN) || defined(LT)
	movapd	%xmm0,  -16 * SIZE(BO)
#else
	movapd	%xmm0,  -16 * SIZE(AO)
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

.L69:
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
	testq	$1, N
	jle	.L999
	ALIGN_4

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
	sarq	$1, I
	NOBRANCH
	jle	.L80
	ALIGN_4

.L71:
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
	movaps	-16 * SIZE(AO), %xmm0
	xorps	%xmm2, %xmm2

	xorps	%xmm8,  %xmm8
	prefetcht0     2 * SIZE(CO1)
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
	jle	.L75
	ALIGN_3

.L72:
	PREFETCH	(PREFETCHSIZE +  0) * SIZE(AO)

	addpd	%xmm1, %xmm8
	movddup	-16 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	movaps	-14 * SIZE(AO), %xmm0

	addpd	%xmm1, %xmm9
	movddup	-15 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	movaps	-12 * SIZE(AO), %xmm0

	addpd	%xmm1, %xmm8
	movddup	-14 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	movaps	-10 * SIZE(AO), %xmm0

	addpd	%xmm1, %xmm9
	movddup	-13 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	movaps	 -8 * SIZE(AO), %xmm0

	subq	$-8 * SIZE, AO
	subq	$-4 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L72

	addpd	%xmm9, %xmm8
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
	addpd	%xmm1, %xmm8
	movddup	-16 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	movaps	-14 * SIZE(AO), %xmm0

	addq	$2 * SIZE, AO
	addq	$1 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L76
	ALIGN_4

.L78:
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

	addpd	%xmm1, %xmm8

#if defined(LN) || defined(LT)
	movapd	-16 * SIZE(BO), %xmm0
#else
	movapd	-16 * SIZE(AO), %xmm0
#endif

	subpd	%xmm8,  %xmm0

#if defined(LN) || defined(LT)
	pshufd	$0xe, %xmm0, %xmm1
#endif

#ifdef LN
	movsd	-13 * SIZE(AO), %xmm8
	mulsd	 %xmm8, %xmm1
	movsd	-14 * SIZE(AO), %xmm12
	mulsd	%xmm1, %xmm12
	subsd	%xmm12, %xmm0
	movsd	-16 * SIZE(AO), %xmm8
	mulsd	 %xmm8, %xmm0
#endif

#ifdef LT
	movsd	-16 * SIZE(AO), %xmm8
	mulsd	 %xmm8, %xmm0
	movsd	-15 * SIZE(AO), %xmm12
	mulsd	%xmm0, %xmm12
	subsd	%xmm12, %xmm1
	movsd	-13 * SIZE(AO), %xmm8
	mulsd	 %xmm8, %xmm1
#endif

#if defined(LN) || defined(LT)
	unpcklpd   %xmm1, %xmm0
#endif

#if defined(RN) || defined(RT)
	movddup	-16 * SIZE(BO), %xmm10
	mulpd	%xmm10, %xmm0
#endif

#ifdef LN
	subq	$2 * SIZE, CO1
#endif

	movsd	%xmm0,  0 * SIZE(CO1)
	movhps	%xmm0,  1 * SIZE(CO1)

#if defined(LN) || defined(LT)
	movapd	%xmm0,  -16 * SIZE(BO)
#else
	movapd	%xmm0,  -16 * SIZE(AO)
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

	decq	I
	BRANCH
	jg	.L71
	ALIGN_4

.L80:
	testq	$1, M
	BRANCH
	jle	.L89
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
	leaq	(B,  %rax, 1), BO
#else
	movq	B, BO
#endif

	movsd	-16 * SIZE(AO), %xmm0
	movhps	-15 * SIZE(AO), %xmm0
	xorps	%xmm8,  %xmm8
	movsd	-16 * SIZE(BO), %xmm1
	movhps	-15 * SIZE(BO), %xmm1
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
	mulpd	%xmm0, %xmm1
	movsd	-14 * SIZE(AO), %xmm0
	movhps	-13 * SIZE(AO), %xmm0
	addpd	%xmm1, %xmm8
	movsd	-14 * SIZE(BO), %xmm1
	movhps	-13 * SIZE(BO), %xmm1

	mulpd	%xmm0, %xmm1
	movsd	-12 * SIZE(AO), %xmm0
	movhps	-11 * SIZE(AO), %xmm0
	addpd	%xmm1, %xmm9
	movsd	-12 * SIZE(BO), %xmm1
	movhps	-11 * SIZE(BO), %xmm1

	subq	$-4 * SIZE, AO
	subq	$-4 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L82

	addpd	%xmm9, %xmm8
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
	mulsd	%xmm0, %xmm1
	movsd	-15 * SIZE(AO), %xmm0
	addsd	%xmm1, %xmm8
	movsd	-15 * SIZE(BO), %xmm1

	addq	$1 * SIZE, AO
	addq	$1 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L86
	ALIGN_4

.L88:
#if defined(LN) || defined(RT)
	movq	KK, %rax
	subq	$1, %rax

	leaq	(, %rax, SIZE), %rax

	movq	AORIG, AO
	leaq	(AO, %rax, 1), AO
	leaq	(B,  %rax, 1), BO
#endif

	haddpd	%xmm8, %xmm8

#if defined(LN) || defined(LT)
	movsd	-16 * SIZE(BO), %xmm0
#else
	movsd	-16 * SIZE(AO), %xmm0
#endif

	subsd	%xmm8,  %xmm0

#if defined(LN) || defined(LT)
	movsd	-16 * SIZE(AO), %xmm8
	mulsd	 %xmm8, %xmm0
#endif

#if defined(RN) || defined(RT)
	movsd	-16 * SIZE(BO), %xmm10
	mulsd	%xmm10, %xmm0
#endif

#ifdef LN
	subq	$1 * SIZE, CO1
#endif

	movsd	%xmm0,  0 * SIZE(CO1)

#if defined(LN) || defined(LT)
	movsd	%xmm0,  -16 * SIZE(BO)
#else
	movsd	%xmm0,  -16 * SIZE(AO)
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

.L89:
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
