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

#define OLD_A		48 + STACKSIZE(%rsp)
#define OLD_B		56 + STACKSIZE(%rsp)
#define OLD_C		64 + STACKSIZE(%rsp)
#define OLD_LDC		72 + STACKSIZE(%rsp)
#define OLD_OFFSET	80 + STACKSIZE(%rsp)

#define OFFSET	  224(%rsp)
#define J	  232(%rsp)
#define KKK	  240(%rsp)
#define AORIG	  248(%rsp)

#endif

#define PREFETCHSIZE  (8 * 1 + 2)
#define PREFETCH     prefetcht0

#define ADD1	  addpd
#define ADD2	  addpd

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

	salq	$ZBASE_SHIFT, LDC

	movq	KK, OFFSET
	negq	KK

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

#ifdef RT
       movq	N, KK
       subq	OFFSET, KK
#endif

	testq	M, M
	jle	.L999

	testq	$1, N
	BRANCH
	jle	.L20

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

	movq	M,  I
	ALIGN_4

.L31:
#ifdef LN
       movq	K, %rax
       salq	$ZBASE_SHIFT, %rax
       subq	%rax, AORIG
#endif

#if defined(LN) || defined(RT)
	movq	AORIG, AO

	movq	KK, %rax
	salq	$ZBASE_SHIFT, %rax

	leaq	(AO, %rax, 1), AO
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
	jle	.L35
	ALIGN_3

.L32:
	PREFETCH	(PREFETCHSIZE +  0) * SIZE(AO)

	ADD1	%xmm1, %xmm8
	movaps	-16 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2
	movaps	-14 * SIZE(AO), %xmm0

	ADD1	%xmm1, %xmm10
	movaps	-14 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm11
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2
	movaps	-12 * SIZE(AO), %xmm0

	ADD1	%xmm1, %xmm8
	movaps	-12 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2
	movaps	-10 * SIZE(AO), %xmm0

	ADD1	%xmm1, %xmm10
	movaps	-10 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm11
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2
	movaps	 -8 * SIZE(AO), %xmm0

	subq	$-8 * SIZE, AO
	subq	$-8 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L32

	addpd	%xmm10, %xmm8
	addpd	%xmm11, %xmm9
	ALIGN_3

.L35:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$3, %rax
	BRANCH
	je	.L38
	ALIGN_3

.L36:
	ADD1	%xmm1, %xmm8
	movaps	-16 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2
	movaps	-14 * SIZE(AO), %xmm0

	addq	$2 * SIZE, AO
	addq	$2 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L36
	ALIGN_3

.L38:
#if defined(LN) || defined(RT)
	movq	KK, %rax
	subq	$1, %rax

	salq	$ZBASE_SHIFT, %rax

	movq	AORIG, AO
	leaq	(AO, %rax, 1), AO
	leaq	(B,  %rax, 1), BO
#endif

	ADD1	%xmm1, %xmm8
	ADD2	%xmm2, %xmm9

	pcmpeqb	%xmm0, %xmm0
	psllq	$63,   %xmm0

#if defined(LN) || defined(LT)

#ifndef CONJ
	shufps	$0x40, %xmm0, %xmm0
	xorps	%xmm0, %xmm8
#else
	shufps	$0x40, %xmm0, %xmm0
	xorps	%xmm0, %xmm9
#endif

#else

#ifndef CONJ
	shufps	$0x40, %xmm0, %xmm0
	xorps	%xmm0, %xmm8
#else
	shufps	$0x04, %xmm0, %xmm0
	xorps	%xmm0, %xmm9
#endif

#endif

	haddpd	%xmm9,  %xmm8

#if defined(LN) || defined(LT)
	movapd	-16 * SIZE(BO), %xmm9
	movapd	-14 * SIZE(BO), %xmm11

	subpd	%xmm8,   %xmm9
#else
	movapd	-16 * SIZE(AO), %xmm9
	movapd	-14 * SIZE(AO), %xmm11

	subpd	%xmm8,   %xmm9
#endif

	pcmpeqb	%xmm7, %xmm7
	psllq	$63,   %xmm7

#ifndef CONJ
	shufps	$0x04, %xmm7,  %xmm7
#else
	shufps	$0x40, %xmm7,  %xmm7
#endif

#if defined(LN) || defined(LT)
	movddup	-16 * SIZE(AO), %xmm0
	movddup	-15 * SIZE(AO), %xmm1

	pshufd	$0x4e, %xmm9,  %xmm8

	xorpd	 %xmm7, %xmm8
	mulpd	 %xmm0, %xmm9
	mulpd	 %xmm1, %xmm8
	addpd	 %xmm8,  %xmm9
#endif

#if defined(RN) || defined(RT)
	movddup	-16 * SIZE(BO), %xmm0
	movddup	-15 * SIZE(BO), %xmm1

	pshufd	$0x4e, %xmm9,  %xmm8
	xorpd	 %xmm7, %xmm8
	mulpd	 %xmm0, %xmm9
	mulpd	 %xmm1, %xmm8
	addpd	 %xmm8, %xmm9
#endif

#ifdef LN
	subq	$2 * SIZE, CO1
#endif

	movsd	%xmm9,  0 * SIZE(CO1)
	movhpd	%xmm9,  1 * SIZE(CO1)

#if defined(LN) || defined(LT)
	movapd	%xmm9,  -16 * SIZE(BO)
#else
	movapd	%xmm9,  -16 * SIZE(AO)
#endif

#ifndef LN
	addq	$2 * SIZE, CO1
#endif

#if defined(LT) || defined(RN)
	movq	K,  %rax
	subq	KK, %rax
	salq	$ZBASE_SHIFT, %rax
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
       salq	$ZBASE_SHIFT, %rax
       addq	%rax, AORIG
#endif
	decq	I
	BRANCH
	jg	.L31

#ifdef LN
       movq	K, %rax
       salq	$ZBASE_SHIFT, %rax
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

.L20:
	testq	$2, N
	BRANCH
	jle	.L30

#if defined(LT) || defined(RN)
	movq	A, AO
#else
	movq	A, AORIG
#endif

#ifdef RT
       movq	K, %rax
       salq	$1 + ZBASE_SHIFT, %rax
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
	movq	OFFSET, KK
	addq	M, KK
#endif

#ifdef LT
	movq	OFFSET, KK
#endif

	movq	M,  I
	ALIGN_4

.L21:
#ifdef LN
       movq	K, %rax
       salq	$ZBASE_SHIFT, %rax
       subq	%rax, AORIG
#endif

#if defined(LN) || defined(RT)
	movq	AORIG, AO

	movq	KK, %rax
	salq	$ZBASE_SHIFT, %rax

	leaq	(AO, %rax, 1), AO
	leaq	(B,  %rax, 2), BO
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
	jle	.L25
	ALIGN_3

.L22:
	PREFETCH	(PREFETCHSIZE +  0) * SIZE(AO)

	ADD1	%xmm1, %xmm8
	movaps	-16 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2

	ADD1	%xmm3, %xmm10
	movaps	-14 * SIZE(BO), %xmm3
	ADD2	%xmm4, %xmm11
	pshufd	$0x4e, %xmm3, %xmm4
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm4

	movaps	-14 * SIZE(AO), %xmm0

	ADD1	%xmm1, %xmm8
	movaps	-12 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2

	ADD1	%xmm3, %xmm10
	movaps	-10 * SIZE(BO), %xmm3
	ADD2	%xmm4, %xmm11
	pshufd	$0x4e, %xmm3, %xmm4
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm4

	movaps	-12 * SIZE(AO), %xmm0

	ADD1	%xmm1, %xmm8
	movaps	 -8 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2

	ADD1	%xmm3, %xmm10
	movaps	 -6 * SIZE(BO), %xmm3
	ADD2	%xmm4, %xmm11
	pshufd	$0x4e, %xmm3, %xmm4
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm4

	movaps	-10 * SIZE(AO), %xmm0

	ADD1	%xmm1, %xmm8
	movaps	 -4 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2

	ADD1	%xmm3, %xmm10
	movaps	 -2 * SIZE(BO), %xmm3
	ADD2	%xmm4, %xmm11
	pshufd	$0x4e, %xmm3, %xmm4
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm4

	movaps	 -8 * SIZE(AO), %xmm0

	subq	$-8  * SIZE, AO
	subq	$-16 * SIZE, BO

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
	ADD1	%xmm1, %xmm8
	movaps	-16 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2

	ADD1	%xmm3, %xmm10
	movaps	-14 * SIZE(BO), %xmm3
	ADD2	%xmm4, %xmm11
	pshufd	$0x4e, %xmm3, %xmm4
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm4

	movaps	-14 * SIZE(AO), %xmm0

	addq	$2 * SIZE, AO
	addq	$4 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L26
	ALIGN_3

.L28:
#if defined(LN) || defined(RT)
	movq	KK, %rax
#ifdef LN
	subq	$1, %rax
#else
	subq	$2, %rax
#endif

	salq	$ZBASE_SHIFT, %rax

	movq	AORIG, AO
	leaq	(AO, %rax, 1), AO
	leaq	(B,  %rax, 2), BO
#endif

	ADD1	%xmm1, %xmm8
	ADD2	%xmm2, %xmm9
	ADD1	%xmm3, %xmm10
	ADD2	%xmm4, %xmm11

	pcmpeqb	%xmm0, %xmm0
	psllq	$63,   %xmm0

#if defined(LN) || defined(LT)

#ifndef CONJ
	shufps	$0x40, %xmm0, %xmm0
	xorps	%xmm0, %xmm8
	xorps	%xmm0, %xmm10
#else
	shufps	$0x40, %xmm0, %xmm0
	xorps	%xmm0, %xmm9
	xorps	%xmm0, %xmm11
#endif

#else

#ifndef CONJ
	shufps	$0x40, %xmm0, %xmm0
	xorps	%xmm0, %xmm8
	xorps	%xmm0, %xmm10
#else
	shufps	$0x04, %xmm0, %xmm0
	xorps	%xmm0, %xmm9
	xorps	%xmm0, %xmm11
#endif

#endif

	haddpd	%xmm9,  %xmm8
	haddpd	%xmm11, %xmm10

#if defined(LN) || defined(LT)
	movapd	-16 * SIZE(BO), %xmm9
	movapd	-14 * SIZE(BO), %xmm11

	subpd	%xmm8,   %xmm9
	subpd	%xmm10,  %xmm11
#else
	movapd	-16 * SIZE(AO), %xmm9
	movapd	-14 * SIZE(AO), %xmm11

	subpd	%xmm8,   %xmm9
	subpd	%xmm10,  %xmm11
#endif

	pcmpeqb	%xmm7, %xmm7
	psllq	$63,   %xmm7

#ifndef CONJ
	shufps	$0x04, %xmm7,  %xmm7
#else
	shufps	$0x40, %xmm7,  %xmm7
#endif

#if defined(LN) || defined(LT)
	movddup	-16 * SIZE(AO), %xmm0
	movddup	-15 * SIZE(AO), %xmm1

	pshufd	$0x4e, %xmm9,  %xmm8
	pshufd	$0x4e, %xmm11, %xmm10

	xorpd	 %xmm7, %xmm8
	xorpd	 %xmm7, %xmm10

	mulpd	 %xmm0, %xmm9
	mulpd	 %xmm1, %xmm8
	mulpd	 %xmm0, %xmm11
	mulpd	 %xmm1, %xmm10

	addpd	 %xmm8,  %xmm9
	addpd	 %xmm10, %xmm11
#endif

#ifdef RN
	movddup	-16 * SIZE(BO), %xmm0
	movddup	-15 * SIZE(BO), %xmm1

	pshufd	$0x4e, %xmm9,  %xmm8
	xorpd	 %xmm7, %xmm8
	mulpd	 %xmm0, %xmm9
	mulpd	 %xmm1, %xmm8
	addpd	 %xmm8, %xmm9

	movddup	-14 * SIZE(BO), %xmm2
	movddup	-13 * SIZE(BO), %xmm3

	pshufd	$0x4e, %xmm9,  %xmm8
	xorpd	 %xmm7, %xmm8
	mulpd	 %xmm9, %xmm2
	mulpd	 %xmm8, %xmm3
	subpd	 %xmm2, %xmm11
	subpd	 %xmm3, %xmm11

	movddup	-10 * SIZE(BO), %xmm0
	movddup	 -9 * SIZE(BO), %xmm1

	pshufd	$0x4e, %xmm11, %xmm10
	xorpd	 %xmm7,  %xmm10
	mulpd	 %xmm0,  %xmm11
	mulpd	 %xmm1,  %xmm10
	addpd	 %xmm10, %xmm11
#endif

#ifdef RT
	movddup	-10 * SIZE(BO), %xmm0
	movddup	 -9 * SIZE(BO), %xmm1

	pshufd	$0x4e, %xmm11, %xmm10
	xorpd	 %xmm7,  %xmm10
	mulpd	 %xmm0,  %xmm11
	mulpd	 %xmm1,  %xmm10
	addpd	 %xmm10, %xmm11

	movddup	-12 * SIZE(BO), %xmm2
	movddup	-11 * SIZE(BO), %xmm3

	pshufd	$0x4e, %xmm11,  %xmm10
	xorpd	 %xmm7,  %xmm10
	mulpd	 %xmm11, %xmm2
	mulpd	 %xmm10, %xmm3
	subpd	 %xmm2,  %xmm9
	subpd	 %xmm3,  %xmm9

	movddup	-16 * SIZE(BO), %xmm0
	movddup	-15 * SIZE(BO), %xmm1

	pshufd	$0x4e, %xmm9, %xmm8
	xorpd	 %xmm7,  %xmm8
	mulpd	 %xmm0,  %xmm9
	mulpd	 %xmm1,  %xmm8
	addpd	 %xmm8,  %xmm9
#endif

#ifdef LN
	subq	$2 * SIZE, CO1
	subq	$2 * SIZE, CO2
#endif

	movsd	%xmm9,  0 * SIZE(CO1)
	movhpd	%xmm9,  1 * SIZE(CO1)
	movsd	%xmm11, 0 * SIZE(CO2)
	movhpd	%xmm11, 1 * SIZE(CO2)

#if defined(LN) || defined(LT)
	movapd	%xmm9,  -16 * SIZE(BO)
	movapd	%xmm11, -14 * SIZE(BO)
#else
	movapd	%xmm9,  -16 * SIZE(AO)
	movapd	%xmm11, -14 * SIZE(AO)
#endif

#ifndef LN
	addq	$2 * SIZE, CO1
	addq	$2 * SIZE, CO2
#endif

#if defined(LT) || defined(RN)
	movq	K,  %rax
	subq	KK, %rax
	salq	$ZBASE_SHIFT, %rax
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
       salq	$ZBASE_SHIFT, %rax
       addq	%rax, AORIG
#endif

	decq	I
	BRANCH
	jg	.L21

#ifdef LN
       movq	K, %rax
       salq	$ZBASE_SHIFT, %rax
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

.L30:
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
       salq	$2 + ZBASE_SHIFT, %rax
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
	movq	OFFSET, KK
	addq	M, KK
#endif

	movq	K, %rax
	salq	$ZBASE_SHIFT + 2, %rax
	leaq	(B, %rax), BB

#ifdef LT
	movq	OFFSET, KK
#endif

	movq	M,  I
	ALIGN_4

.L11:
#ifdef LN
       movq	K, %rax
       salq	$ZBASE_SHIFT, %rax
       subq	%rax, AORIG
#endif

#if defined(LN) || defined(RT)
	movq	AORIG, AO

	movq	KK, %rax
	salq	$ZBASE_SHIFT, %rax

	leaq	(AO, %rax, 1), AO
	leaq	(B,  %rax, 4), BO
#else
	movq	B, BO
#endif

	prefetchnta	 -16 * SIZE(BB)
	subq		 $-8 * SIZE, BB

	xorps	%xmm1, %xmm1
	movaps	-16 * SIZE(AO), %xmm0
	xorps	%xmm2, %xmm2
	xorps	%xmm3, %xmm3
	xorps	%xmm4, %xmm4

	xorps	%xmm8,  %xmm8
	prefetcht0     2 * SIZE(CO1)
	xorps	%xmm9,  %xmm9
	prefetcht0     2 * SIZE(CO1, LDC)
	xorps	%xmm10, %xmm10
	xorps	%xmm11, %xmm11

	xorps	%xmm12, %xmm12
	prefetcht0     2 * SIZE(CO2)
	xorps	%xmm13, %xmm13
	prefetcht0     2 * SIZE(CO2, LDC)
	xorps	%xmm14, %xmm14
	xorps	%xmm15, %xmm15

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

	ADD1	%xmm1, %xmm12
	movaps	-16 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm13
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2

	ADD1	%xmm3, %xmm14
	movaps	-14 * SIZE(BO), %xmm3
	ADD2	%xmm4, %xmm15
	pshufd	$0x4e, %xmm3, %xmm4
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm4

	ADD1	%xmm1, %xmm8
	movaps	-12 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2

	ADD1	%xmm3, %xmm10
	movaps	-10 * SIZE(BO), %xmm3
	ADD2	%xmm4, %xmm11
	pshufd	$0x4e, %xmm3, %xmm4
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm4

	ADD1	%xmm1, %xmm12
	movaps	 -8 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm13
	movaps	-14 * SIZE(AO), %xmm0
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2

	ADD1	%xmm3, %xmm14
	movaps	 -6 * SIZE(BO), %xmm3
	ADD2	%xmm4, %xmm15
	pshufd	$0x4e, %xmm3, %xmm4
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm4

	ADD1	%xmm1, %xmm8
	movaps	 -4 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2

	ADD1	%xmm3, %xmm10
	movaps	 -2 * SIZE(BO), %xmm3
	ADD2	%xmm4, %xmm11
	pshufd	$0x4e, %xmm3, %xmm4
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm4

	ADD1	%xmm1, %xmm12
	movaps	  0 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm13
	movaps	-12 * SIZE(AO), %xmm0
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2

	ADD1	%xmm3, %xmm14
	movaps	  2 * SIZE(BO), %xmm3
	ADD2	%xmm4, %xmm15
	pshufd	$0x4e, %xmm3, %xmm4
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm4

	ADD1	%xmm1, %xmm8
	movaps	  4 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2

	ADD1	%xmm3, %xmm10
	movaps	  6 * SIZE(BO), %xmm3
	ADD2	%xmm4, %xmm11
	pshufd	$0x4e, %xmm3, %xmm4
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm4

	movaps	-10 * SIZE(AO), %xmm0
	ADD1	%xmm1, %xmm12
	movaps	  8 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm13
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2

	ADD1	%xmm3, %xmm14
	movaps	 10 * SIZE(BO), %xmm3
	ADD2	%xmm4, %xmm15
	pshufd	$0x4e, %xmm3, %xmm4
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm4

	ADD1	%xmm1, %xmm8
	movaps	 12 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2

	ADD1	%xmm3, %xmm10
	movaps	 14 * SIZE(BO), %xmm3
	ADD2	%xmm4, %xmm11
	pshufd	$0x4e, %xmm3, %xmm4
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm4
	movaps	 -8 * SIZE(AO), %xmm0

	subq	$-8  * SIZE, AO
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
	andq	$3, %rax		# if (k & 1)
	BRANCH
	je	.L18
	ALIGN_3

.L16:
	ADD1	%xmm1, %xmm12
	movaps	-16 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm13
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2

	ADD1	%xmm3, %xmm14
	movaps	-14 * SIZE(BO), %xmm3
	ADD2	%xmm4, %xmm15
	pshufd	$0x4e, %xmm3, %xmm4
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm4

	ADD1	%xmm1, %xmm8
	movaps	-12 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2

	ADD1	%xmm3, %xmm10
	movaps	-10 * SIZE(BO), %xmm3
	ADD2	%xmm4, %xmm11
	pshufd	$0x4e, %xmm3, %xmm4
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm4

	movaps	-14 * SIZE(AO), %xmm0

	addq	$2 * SIZE, AO
	addq	$8 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L16
	ALIGN_3

.L18:
#if defined(LN) || defined(RT)
	movq	KK, %rax
#ifdef LN
	subq	$1, %rax
#else
	subq	$4, %rax
#endif

	salq	$ZBASE_SHIFT, %rax

	movq	AORIG, AO
	leaq	(AO, %rax, 1), AO
	leaq	(B,  %rax, 4), BO
#endif

	ADD1	%xmm1, %xmm12
	ADD2	%xmm2, %xmm13
	ADD1	%xmm3, %xmm14
	ADD2	%xmm4, %xmm15

	pcmpeqb	%xmm0, %xmm0
	psllq	$63,   %xmm0

#if defined(LN) || defined(LT)

#ifndef CONJ
	shufps	$0x40, %xmm0, %xmm0
	xorps	%xmm0, %xmm8
	xorps	%xmm0, %xmm10
	xorps	%xmm0, %xmm12
	xorps	%xmm0, %xmm14
#else
	shufps	$0x40, %xmm0, %xmm0
	xorps	%xmm0, %xmm9
	xorps	%xmm0, %xmm11
	xorps	%xmm0, %xmm13
	xorps	%xmm0, %xmm15
#endif

#else

#ifndef CONJ
	shufps	$0x40, %xmm0, %xmm0
	xorps	%xmm0, %xmm8
	xorps	%xmm0, %xmm10
	xorps	%xmm0, %xmm12
	xorps	%xmm0, %xmm14
#else
	shufps	$0x04, %xmm0, %xmm0
	xorps	%xmm0, %xmm9
	xorps	%xmm0, %xmm11
	xorps	%xmm0, %xmm13
	xorps	%xmm0, %xmm15
#endif

#endif

	haddpd	%xmm9,  %xmm8
	haddpd	%xmm11, %xmm10
	haddpd	%xmm13, %xmm12
	haddpd	%xmm15, %xmm14

#if defined(LN) || defined(LT)
	movapd	-16 * SIZE(BO), %xmm9
	movapd	-14 * SIZE(BO), %xmm11
	movapd	-12 * SIZE(BO), %xmm13
	movapd	-10 * SIZE(BO), %xmm15

	subpd	%xmm8,   %xmm9
	subpd	%xmm10,  %xmm11
	subpd	%xmm12,  %xmm13
	subpd	%xmm14,  %xmm15
#else
	movapd	-16 * SIZE(AO), %xmm9
	movapd	-14 * SIZE(AO), %xmm11
	movapd	-12 * SIZE(AO), %xmm13
	movapd	-10 * SIZE(AO), %xmm15

	subpd	%xmm8,   %xmm9
	subpd	%xmm10,  %xmm11
	subpd	%xmm12,  %xmm13
	subpd	%xmm14,  %xmm15
#endif

	pcmpeqb	%xmm7, %xmm7
	psllq	$63,   %xmm7

#ifndef CONJ
	shufps	$0x04, %xmm7,  %xmm7
#else
	shufps	$0x40, %xmm7,  %xmm7
#endif

#if defined(LN) || defined(LT)
	movddup	-16 * SIZE(AO), %xmm0
	movddup	-15 * SIZE(AO), %xmm1

	pshufd	$0x4e, %xmm9,  %xmm8
	pshufd	$0x4e, %xmm11, %xmm10
	pshufd	$0x4e, %xmm13, %xmm12
	pshufd	$0x4e, %xmm15, %xmm14

	xorpd	 %xmm7, %xmm8
	xorpd	 %xmm7, %xmm10
	xorpd	 %xmm7, %xmm12
	xorpd	 %xmm7, %xmm14

	mulpd	 %xmm0, %xmm9
	mulpd	 %xmm1, %xmm8
	mulpd	 %xmm0, %xmm11
	mulpd	 %xmm1, %xmm10
	mulpd	 %xmm0, %xmm13
	mulpd	 %xmm1, %xmm12
	mulpd	 %xmm0, %xmm15
	mulpd	 %xmm1, %xmm14

	addpd	 %xmm8,  %xmm9
	addpd	 %xmm10, %xmm11
	addpd	 %xmm12, %xmm13
	addpd	 %xmm14, %xmm15
#endif

#ifdef RN
	movddup	-16 * SIZE(BO), %xmm0
	movddup	-15 * SIZE(BO), %xmm1

	pshufd	$0x4e, %xmm9,  %xmm8
	xorpd	 %xmm7, %xmm8
	mulpd	 %xmm0, %xmm9
	mulpd	 %xmm1, %xmm8
	addpd	 %xmm8, %xmm9

	movddup	-14 * SIZE(BO), %xmm2
	movddup	-13 * SIZE(BO), %xmm3

	pshufd	$0x4e, %xmm9,  %xmm8
	xorpd	 %xmm7, %xmm8
	mulpd	 %xmm9, %xmm2
	mulpd	 %xmm8, %xmm3
	subpd	 %xmm2, %xmm11
	subpd	 %xmm3, %xmm11

	movddup	-12 * SIZE(BO), %xmm0
	movddup	-11 * SIZE(BO), %xmm1

	pshufd	$0x4e, %xmm9,  %xmm8
	xorpd	 %xmm7, %xmm8
	mulpd	 %xmm9, %xmm0
	mulpd	 %xmm8, %xmm1
	subpd	 %xmm0, %xmm13
	subpd	 %xmm1, %xmm13

	movddup	-10 * SIZE(BO), %xmm2
	movddup	 -9 * SIZE(BO), %xmm3

	pshufd	$0x4e, %xmm9,  %xmm8
	xorpd	 %xmm7, %xmm8
	mulpd	 %xmm9, %xmm2
	mulpd	 %xmm8, %xmm3
	subpd	 %xmm2, %xmm15
	subpd	 %xmm3, %xmm15

	movddup	 -6 * SIZE(BO), %xmm0
	movddup	 -5 * SIZE(BO), %xmm1

	pshufd	$0x4e, %xmm11, %xmm10
	xorpd	 %xmm7,  %xmm10
	mulpd	 %xmm0,  %xmm11
	mulpd	 %xmm1,  %xmm10
	addpd	 %xmm10, %xmm11

	movddup	 -4 * SIZE(BO), %xmm0
	movddup	 -3 * SIZE(BO), %xmm1

	pshufd	$0x4e, %xmm11,  %xmm10
	xorpd	 %xmm7,  %xmm10
	mulpd	 %xmm11, %xmm0
	mulpd	 %xmm10, %xmm1
	subpd	 %xmm0,  %xmm13
	subpd	 %xmm1,  %xmm13

	movddup	 -2 * SIZE(BO), %xmm2
	movddup	 -1 * SIZE(BO), %xmm3

	pshufd	$0x4e, %xmm11,  %xmm10
	xorpd	 %xmm7,  %xmm10
	mulpd	 %xmm11, %xmm2
	mulpd	 %xmm10, %xmm3
	subpd	 %xmm2,  %xmm15
	subpd	 %xmm3,  %xmm15

	movddup	  4 * SIZE(BO), %xmm0
	movddup	  5 * SIZE(BO), %xmm1

	pshufd	$0x4e, %xmm13, %xmm12
	xorpd	 %xmm7,  %xmm12
	mulpd	 %xmm0,  %xmm13
	mulpd	 %xmm1,  %xmm12
	addpd	 %xmm12, %xmm13

	movddup	  6 * SIZE(BO), %xmm2
	movddup	  7 * SIZE(BO), %xmm3

	pshufd	$0x4e, %xmm13,  %xmm12
	xorpd	 %xmm7,  %xmm12
	mulpd	 %xmm13, %xmm2
	mulpd	 %xmm12, %xmm3
	subpd	 %xmm2,  %xmm15
	subpd	 %xmm3,  %xmm15

	movddup	 14 * SIZE(BO), %xmm0
	movddup	 15 * SIZE(BO), %xmm1

	pshufd	$0x4e, %xmm15, %xmm14
	xorpd	 %xmm7,  %xmm14
	mulpd	 %xmm0,  %xmm15
	mulpd	 %xmm1,  %xmm14
	addpd	 %xmm14, %xmm15
#endif

#ifdef RT
	movddup	 14 * SIZE(BO), %xmm0
	movddup	 15 * SIZE(BO), %xmm1

	pshufd	$0x4e, %xmm15,  %xmm14
	xorpd	 %xmm7,  %xmm14
	mulpd	 %xmm0,  %xmm15
	mulpd	 %xmm1,  %xmm14
	addpd	 %xmm14, %xmm15

	movddup	 12 * SIZE(BO), %xmm2
	movddup	 13 * SIZE(BO), %xmm3

	pshufd	$0x4e, %xmm15,  %xmm14
	xorpd	 %xmm7,  %xmm14
	mulpd	 %xmm15, %xmm2
	mulpd	 %xmm14, %xmm3
	subpd	 %xmm2,  %xmm13
	subpd	 %xmm3,  %xmm13

	movddup	 10 * SIZE(BO), %xmm0
	movddup	 11 * SIZE(BO), %xmm1

	pshufd	$0x4e, %xmm15, %xmm14
	xorpd	 %xmm7,  %xmm14
	mulpd	 %xmm15, %xmm0
	mulpd	 %xmm14, %xmm1
	subpd	 %xmm0,  %xmm11
	subpd	 %xmm1,  %xmm11

	movddup	  8 * SIZE(BO), %xmm2
	movddup	  9 * SIZE(BO), %xmm3

	pshufd	$0x4e, %xmm15,  %xmm14
	xorpd	 %xmm7,  %xmm14
	mulpd	 %xmm15, %xmm2
	mulpd	 %xmm14, %xmm3
	subpd	 %xmm2,  %xmm9
	subpd	 %xmm3,  %xmm9

	movddup	  4 * SIZE(BO), %xmm0
	movddup	  5 * SIZE(BO), %xmm1

	pshufd	$0x4e, %xmm13, %xmm12
	xorpd	 %xmm7,  %xmm12
	mulpd	 %xmm0,  %xmm13
	mulpd	 %xmm1,  %xmm12
	addpd	 %xmm12, %xmm13

	movddup	  2 * SIZE(BO), %xmm0
	movddup	  3 * SIZE(BO), %xmm1

	pshufd	$0x4e, %xmm13,  %xmm12
	xorpd	 %xmm7,  %xmm12
	mulpd	 %xmm13, %xmm0
	mulpd	 %xmm12, %xmm1
	subpd	 %xmm0,  %xmm11
	subpd	 %xmm1,  %xmm11

	movddup	  0 * SIZE(BO), %xmm2
	movddup	  1 * SIZE(BO), %xmm3

	pshufd	$0x4e, %xmm13,  %xmm12
	xorpd	 %xmm7,  %xmm12
	mulpd	 %xmm13, %xmm2
	mulpd	 %xmm12, %xmm3
	subpd	 %xmm2,  %xmm9
	subpd	 %xmm3,  %xmm9

	movddup	 -6 * SIZE(BO), %xmm0
	movddup	 -5 * SIZE(BO), %xmm1

	pshufd	$0x4e, %xmm11, %xmm10
	xorpd	 %xmm7,  %xmm10
	mulpd	 %xmm0,  %xmm11
	mulpd	 %xmm1,  %xmm10
	addpd	 %xmm10, %xmm11

	movddup	 -8 * SIZE(BO), %xmm2
	movddup	 -7 * SIZE(BO), %xmm3

	pshufd	$0x4e, %xmm11,  %xmm10
	xorpd	 %xmm7,  %xmm10
	mulpd	 %xmm11, %xmm2
	mulpd	 %xmm10, %xmm3
	subpd	 %xmm2,  %xmm9
	subpd	 %xmm3,  %xmm9

	movddup	-16 * SIZE(BO), %xmm0
	movddup	-15 * SIZE(BO), %xmm1

	pshufd	$0x4e, %xmm9, %xmm8
	xorpd	 %xmm7,  %xmm8
	mulpd	 %xmm0,  %xmm9
	mulpd	 %xmm1,  %xmm8
	addpd	 %xmm8,  %xmm9
#endif

#ifdef LN
	subq	$2 * SIZE, CO1
	subq	$2 * SIZE, CO2
#endif

	movsd	%xmm9,  0 * SIZE(CO1)
	movhpd	%xmm9,  1 * SIZE(CO1)
	movsd	%xmm11, 0 * SIZE(CO1, LDC)
	movhpd	%xmm11, 1 * SIZE(CO1, LDC)
	movsd	%xmm13, 0 * SIZE(CO2)
	movhpd	%xmm13, 1 * SIZE(CO2)
	movsd	%xmm15, 0 * SIZE(CO2, LDC)
	movhpd	%xmm15, 1 * SIZE(CO2, LDC)

#if defined(LN) || defined(LT)
	movapd	%xmm9,  -16 * SIZE(BO)
	movapd	%xmm11, -14 * SIZE(BO)
	movapd	%xmm13, -12 * SIZE(BO)
	movapd	%xmm15, -10 * SIZE(BO)
#else
	movapd	%xmm9,  -16 * SIZE(AO)
	movapd	%xmm11, -14 * SIZE(AO)
	movapd	%xmm13, -12 * SIZE(AO)
	movapd	%xmm15, -10 * SIZE(AO)
#endif

#ifndef LN
	addq	$2 * SIZE, CO1
	addq	$2 * SIZE, CO2
#endif

#if defined(LT) || defined(RN)
	movq	K,  %rax
	subq	KK, %rax
	salq	$ZBASE_SHIFT, %rax
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
       salq	$ZBASE_SHIFT, %rax
       addq	%rax, AORIG
#endif

	decq	I
	BRANCH
	jg	.L11

#ifdef LN
       movq	K, %rax
       salq	$ZBASE_SHIFT, %rax
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
