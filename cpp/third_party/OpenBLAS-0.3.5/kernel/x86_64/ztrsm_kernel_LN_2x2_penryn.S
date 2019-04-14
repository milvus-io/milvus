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

#define PREFETCH_R    (8 *  4 + 0)
#define PREFETCHSIZE  (8 * 21 + 6)
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

	movq	OLD_M, M
	movq	OLD_N, N
	movq	OLD_K, K
	movq	OLD_LDC,    LDC
	movq	OLD_OFFSET, KK

	subq	$-16 * SIZE, A
	subq	$-16 * SIZE, B

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

	movq	N,  J
	sarq	$1, J
	NOBRANCH
	jle	.L40
	ALIGN_4

.L01:
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

	movq	K, %rax
	salq	$ZBASE_SHIFT + 1, %rax
	leaq	(B, %rax), BB

#ifdef LT
	movq	OFFSET, KK
#endif

	testq	$1, M
	BRANCH
	jle	.L20
	ALIGN_4

#ifdef LN
       movq	K, %rax
       salq	$ZBASE_SHIFT, %rax
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
	movaps	-16 * SIZE(BO), %xmm2
	movaps	-14 * SIZE(BO), %xmm3

	pxor	%xmm3, %xmm3
	pxor	%xmm5, %xmm5

	movapd	%xmm3, %xmm8
	movapd	%xmm3, %xmm9
	movapd	%xmm3, %xmm12
	movapd	%xmm3, %xmm13

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
	ADD1	%xmm3, %xmm12
	movaps	-14 * SIZE(BO), %xmm3
	pshufd	$0x4e, %xmm2, %xmm7
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)
	mulpd	%xmm0, %xmm2
	ADD2	%xmm5, %xmm13
	mulpd	%xmm0, %xmm7

	ADD1	%xmm2, %xmm8
	movaps	-12 * SIZE(BO), %xmm2
	pshufd	$0x4e, %xmm3, %xmm5
	mulpd	%xmm0, %xmm3
	ADD2	%xmm7, %xmm9
	mulpd	%xmm0, %xmm5
	movaps	-14 * SIZE(AO), %xmm0

	ADD1	%xmm3, %xmm12
	movaps	-10 * SIZE(BO), %xmm3
	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	ADD2	%xmm5, %xmm13
	mulpd	%xmm0, %xmm7

	ADD1	%xmm2, %xmm8
	movaps	 -8 * SIZE(BO), %xmm2
	pshufd	$0x4e, %xmm3, %xmm5
	mulpd	%xmm0, %xmm3
	ADD2	%xmm7, %xmm9
	mulpd	%xmm0, %xmm5
	movaps	-12 * SIZE(AO), %xmm0

	ADD1	%xmm3, %xmm12
	movaps	 -6 * SIZE(BO), %xmm3
	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	ADD2	%xmm5, %xmm13
	mulpd	%xmm0, %xmm7

	ADD1	%xmm2, %xmm8
	movaps	 -4 * SIZE(BO), %xmm2
	pshufd	$0x4e, %xmm3, %xmm5
	mulpd	%xmm0, %xmm3
	ADD2	%xmm7, %xmm9
	mulpd	%xmm0, %xmm5
	movaps	-10 * SIZE(AO), %xmm0

	ADD1	%xmm3, %xmm12
	movaps	 -2 * SIZE(BO), %xmm3
	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	ADD2	%xmm5, %xmm13
	mulpd	%xmm0, %xmm7
	subq	$ -8 * SIZE, AO

	ADD1	%xmm2, %xmm8
	movaps	  0 * SIZE(BO), %xmm2
	pshufd	$0x4e, %xmm3, %xmm5
	mulpd	%xmm0, %xmm3
	ADD2	%xmm7, %xmm9
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
	ADD1	%xmm3, %xmm12
	movaps	-14 * SIZE(BO), %xmm3
	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	ADD2	%xmm5, %xmm13
	mulpd	%xmm0, %xmm7

	ADD1	%xmm2, %xmm8
	movaps	-12 * SIZE(BO), %xmm2
	pshufd	$0x4e, %xmm3, %xmm5
	mulpd	%xmm0, %xmm3
	ADD2	%xmm7, %xmm9
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
	subq	$1, %rax
#else
	subq	$2, %rax
#endif

	leaq	(, %rax, SIZE), %rax

	movq	AORIG, AO
	leaq	(AO, %rax, 2), AO
	leaq	(B,  %rax, 4), BO
#endif

	ADD1	%xmm3, %xmm12
	pcmpeqb	%xmm7, %xmm7
	ADD2	%xmm5, %xmm13
	psllq	$63,   %xmm7

#ifndef CONJ
	pshufd	$0x40, %xmm7, %xmm0
	shufps	$0x04, %xmm7, %xmm7

	pxor	%xmm0, %xmm8
	pxor	%xmm0, %xmm12
#else
#if defined(LN) || defined(LT)
	pshufd	$0x40, %xmm7, %xmm0
#else
	pshufd	$0x04, %xmm7, %xmm0
#endif
	shufps	$0x40, %xmm7, %xmm7

	pxor	%xmm0, %xmm9
	pxor	%xmm0, %xmm13
#endif

	haddpd	%xmm9,  %xmm8
	haddpd	%xmm13, %xmm12

#if defined(LN) || defined(LT)
	movapd	-16 * SIZE(BO), %xmm9
	movapd	-14 * SIZE(BO), %xmm11

	subpd	%xmm8,  %xmm9
	subpd	%xmm12,  %xmm11
#else
	movapd	-16 * SIZE(AO), %xmm9
	movapd	-14 * SIZE(AO), %xmm11

	subpd	%xmm8,  %xmm9
	subpd	%xmm12,  %xmm11
#endif

#if defined(LN) || defined(LT)
	movddup	-16 * SIZE(AO), %xmm0
	movddup	-15 * SIZE(AO), %xmm1

	pshufd	$0x4e, %xmm9, %xmm8
	pshufd	$0x4e, %xmm11, %xmm10

	xorpd	 %xmm7, %xmm8
	xorpd	 %xmm7, %xmm10

	mulpd	 %xmm0, %xmm9
	mulpd	 %xmm1, %xmm8
	mulpd	 %xmm0, %xmm11
	mulpd	 %xmm1, %xmm10

	addpd	 %xmm8, %xmm9
	addpd	 %xmm10, %xmm11
#endif

#ifdef RN
	movddup	-16 * SIZE(BO), %xmm0
	movddup	-15 * SIZE(BO), %xmm1
	movddup	-14 * SIZE(BO), %xmm2
	movddup	-13 * SIZE(BO), %xmm3
	movddup	-10 * SIZE(BO), %xmm4
	movddup	 -9 * SIZE(BO), %xmm5

	pshufd	$0x4e, %xmm9, %xmm8

	xorpd	 %xmm7, %xmm8

	mulpd	 %xmm0, %xmm9
	mulpd	 %xmm1, %xmm8

	addpd	 %xmm8, %xmm9

	movapd	 %xmm9, %xmm8
	pshufd	 $0x4e, %xmm9, %xmm12

	xorpd	 %xmm7, %xmm12

	mulpd	 %xmm2, %xmm8
	mulpd	 %xmm3, %xmm12

	subpd	 %xmm8, %xmm11
	subpd	 %xmm12, %xmm11

	pshufd	$0x4e, %xmm11, %xmm10

	xorpd	 %xmm7, %xmm10

	mulpd	 %xmm4, %xmm11
	mulpd	 %xmm5, %xmm10

	addpd	 %xmm10, %xmm11
#endif

#ifdef RT
	movddup	-10 * SIZE(BO), %xmm0
	movddup	 -9 * SIZE(BO), %xmm1
	movddup	-12 * SIZE(BO), %xmm2
	movddup	-11 * SIZE(BO), %xmm3
	movddup	-16 * SIZE(BO), %xmm4
	movddup	-15 * SIZE(BO), %xmm5

	pshufd	$0x4e, %xmm11, %xmm10

	xorpd	 %xmm7, %xmm10

	mulpd	 %xmm0, %xmm11
	mulpd	 %xmm1, %xmm10

	addpd	 %xmm10, %xmm11

	movapd	 %xmm11, %xmm8
	pshufd	 $0x4e, %xmm11, %xmm12

	xorpd	 %xmm7, %xmm12

	mulpd	 %xmm2, %xmm8
	mulpd	 %xmm3, %xmm12

	subpd	 %xmm8, %xmm9
	subpd	 %xmm12, %xmm9

	pshufd	$0x4e, %xmm9, %xmm8

	xorpd	 %xmm7, %xmm8

	mulpd	 %xmm4, %xmm9
	mulpd	 %xmm5, %xmm8

	addpd	 %xmm8, %xmm9
#endif

#ifdef LN
	subq	$2 * SIZE, CO1
	subq	$2 * SIZE, CO2
#endif

	movsd	%xmm9,   0 * SIZE(CO1)
	movhpd	%xmm9,   1 * SIZE(CO1)

	movsd	%xmm11,  0 * SIZE(CO2)
	movhpd	%xmm11,  1 * SIZE(CO2)

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
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
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
	ALIGN_4

.L20:
	movq	M,  I
	sarq	$1, I
	NOBRANCH
	jle	.L39
	ALIGN_4

.L11:
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
#else
	prefetcht0     3 * SIZE(CO1)
	movapd	%xmm4, %xmm8
	movapd	%xmm4, %xmm9
	prefetcht0     3 * SIZE(CO2)
#endif
	movapd	%xmm4, %xmm10
	movapd	%xmm4, %xmm11

	movapd	%xmm4, %xmm12
	movapd	%xmm4, %xmm13
	movapd	%xmm4, %xmm14
	movapd	%xmm4, %xmm15

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
	ADD1	%xmm3, %xmm12
	movaps	-14 * SIZE(BO), %xmm3
	ADD1	%xmm4, %xmm14
	movaps	%xmm2, %xmm4
	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm4

	ADD2	%xmm5, %xmm13
	ADD2	%xmm6, %xmm15
	movaps	%xmm7, %xmm6
	mulpd	%xmm0, %xmm7
	mulpd	%xmm1, %xmm6

	ADD1	%xmm2, %xmm8
	movaps	-12 * SIZE(BO), %xmm2
	ADD1	%xmm4, %xmm10
	movaps	%xmm3, %xmm4
	pshufd	$0x4e, %xmm3, %xmm5
	mulpd	%xmm0, %xmm3
	mulpd	%xmm1, %xmm4

	ADD2	%xmm7, %xmm9
	ADD2	%xmm6, %xmm11
	movaps	%xmm5, %xmm6
	mulpd	%xmm0, %xmm5
	movaps	-12 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm6
	movaps	-10 * SIZE(AO), %xmm1

	ADD1	%xmm3, %xmm12
	movaps	-10 * SIZE(BO), %xmm3
	ADD1	%xmm4, %xmm14
	movaps	%xmm2, %xmm4
	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm4

	ADD2	%xmm5, %xmm13
	ADD2	%xmm6, %xmm15
	movaps	%xmm7, %xmm6
	mulpd	%xmm0, %xmm7
	mulpd	%xmm1, %xmm6

	ADD1	%xmm2, %xmm8
	movaps	 -8 * SIZE(BO), %xmm2
	ADD1	%xmm4, %xmm10
	movaps	%xmm3, %xmm4
	pshufd	$0x4e, %xmm3, %xmm5
	mulpd	%xmm0, %xmm3
	mulpd	%xmm1, %xmm4

	ADD2	%xmm7, %xmm9
	ADD2	%xmm6, %xmm11
	movaps	%xmm5, %xmm6
	mulpd	%xmm0, %xmm5
	movaps	 -8 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm6
	movaps	 -6 * SIZE(AO), %xmm1

	ADD1	%xmm3, %xmm12
	movaps	 -6 * SIZE(BO), %xmm3
	ADD1	%xmm4, %xmm14
	movaps	%xmm2, %xmm4
	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm4

	ADD2	%xmm5, %xmm13
	ADD2	%xmm6, %xmm15
	movaps	%xmm7, %xmm6
	mulpd	%xmm0, %xmm7
	mulpd	%xmm1, %xmm6

	ADD1	%xmm2, %xmm8
	movaps	 -4 * SIZE(BO), %xmm2
	ADD1	%xmm4, %xmm10
	movaps	%xmm3, %xmm4
	pshufd	$0x4e, %xmm3, %xmm5
	mulpd	%xmm0, %xmm3
	mulpd	%xmm1, %xmm4

	ADD2	%xmm7, %xmm9
	ADD2	%xmm6, %xmm11
	movaps	%xmm5, %xmm6
	mulpd	%xmm0, %xmm5
	movaps	 -4 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm6
	movaps	 -2 * SIZE(AO), %xmm1

	ADD1	%xmm3, %xmm12
	movaps	 -2 * SIZE(BO), %xmm3
	ADD1	%xmm4, %xmm14
	movaps	%xmm2, %xmm4
	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm4

	ADD2	%xmm5, %xmm13
	ADD2	%xmm6, %xmm15
	movaps	%xmm7, %xmm6
	mulpd	%xmm0, %xmm7
	mulpd	%xmm1, %xmm6

	ADD1	%xmm2, %xmm8
	movaps	  0 * SIZE(BO), %xmm2
	ADD1	%xmm4, %xmm10
	movaps	%xmm3, %xmm4
	pshufd	$0x4e, %xmm3, %xmm5
	mulpd	%xmm0, %xmm3
	mulpd	%xmm1, %xmm4

	ADD2	%xmm7, %xmm9
	ADD2	%xmm6, %xmm11
	movaps	%xmm5, %xmm6
	mulpd	%xmm0, %xmm5
	movaps	  0 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm6
	movaps	  2 * SIZE(AO), %xmm1

	ADD1	%xmm3, %xmm12
	movaps	  2 * SIZE(BO), %xmm3
	ADD1	%xmm4, %xmm14
	movaps	%xmm2, %xmm4
	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm4

	ADD2	%xmm5, %xmm13
	ADD2	%xmm6, %xmm15
	movaps	%xmm7, %xmm6
	mulpd	%xmm0, %xmm7
	mulpd	%xmm1, %xmm6

	ADD1	%xmm2, %xmm8
	movaps	  4 * SIZE(BO), %xmm2
	ADD1	%xmm4, %xmm10
	movaps	%xmm3, %xmm4
	pshufd	$0x4e, %xmm3, %xmm5
	mulpd	%xmm0, %xmm3
	mulpd	%xmm1, %xmm4

	ADD2	%xmm7, %xmm9
	ADD2	%xmm6, %xmm11
	movaps	%xmm5, %xmm6
	mulpd	%xmm0, %xmm5
	movaps	  4 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm6
	movaps	  6 * SIZE(AO), %xmm1

	ADD1	%xmm3, %xmm12
	movaps	  6 * SIZE(BO), %xmm3
	ADD1	%xmm4, %xmm14
	movaps	%xmm2, %xmm4
	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm4

	ADD2	%xmm5, %xmm13
	ADD2	%xmm6, %xmm15
	movaps	%xmm7, %xmm6
	mulpd	%xmm0, %xmm7
	mulpd	%xmm1, %xmm6

	ADD1	%xmm2, %xmm8
	movaps	  8 * SIZE(BO), %xmm2
	ADD1	%xmm4, %xmm10
	movaps	%xmm3, %xmm4
	pshufd	$0x4e, %xmm3, %xmm5
	mulpd	%xmm0, %xmm3
	mulpd	%xmm1, %xmm4

	ADD2	%xmm7, %xmm9
	ADD2	%xmm6, %xmm11
	movaps	%xmm5, %xmm6
	mulpd	%xmm0, %xmm5
	movaps	  8 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm6
	movaps	 10 * SIZE(AO), %xmm1

	ADD1	%xmm3, %xmm12
	movaps	 10 * SIZE(BO), %xmm3
	ADD1	%xmm4, %xmm14
	movaps	%xmm2, %xmm4
	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm4

	ADD2	%xmm5, %xmm13
	ADD2	%xmm6, %xmm15
	movaps	%xmm7, %xmm6
	mulpd	%xmm0, %xmm7
	mulpd	%xmm1, %xmm6

	ADD1	%xmm2, %xmm8
	movaps	 12 * SIZE(BO), %xmm2
	ADD1	%xmm4, %xmm10
	movaps	%xmm3, %xmm4
	pshufd	$0x4e, %xmm3, %xmm5
	mulpd	%xmm0, %xmm3
	mulpd	%xmm1, %xmm4

	ADD2	%xmm7, %xmm9
	ADD2	%xmm6, %xmm11
	movaps	%xmm5, %xmm6
	mulpd	%xmm0, %xmm5
	movaps	 12 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm6
	movaps	 14 * SIZE(AO), %xmm1

	ADD1	%xmm3, %xmm12
	movaps	 14 * SIZE(BO), %xmm3
	ADD1	%xmm4, %xmm14
	movaps	%xmm2, %xmm4
	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm4

	ADD2	%xmm5, %xmm13
	ADD2	%xmm6, %xmm15
	movaps	%xmm7, %xmm6
	mulpd	%xmm0, %xmm7
	mulpd	%xmm1, %xmm6

	ADD1	%xmm2, %xmm8
	movaps	 16 * SIZE(BO), %xmm2
	ADD1	%xmm4, %xmm10
	movaps	%xmm3, %xmm4
	pshufd	$0x4e, %xmm3, %xmm5
	mulpd	%xmm0, %xmm3
	mulpd	%xmm1, %xmm4

	ADD2	%xmm7, %xmm9
	subq	$-32 * SIZE, AO
	ADD2	%xmm6, %xmm11
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
	ADD1	%xmm3, %xmm12
	movaps	-14 * SIZE(BO), %xmm3
	ADD1	%xmm4, %xmm14
	movaps	%xmm2, %xmm4
	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm4

	ADD2	%xmm5, %xmm13
	ADD2	%xmm6, %xmm15
	movaps	%xmm7, %xmm6
	mulpd	%xmm0, %xmm7
	mulpd	%xmm1, %xmm6

	ADD1	%xmm2, %xmm8
	movaps	-12 * SIZE(BO), %xmm2
	ADD1	%xmm4, %xmm10
	movaps	%xmm3, %xmm4
	pshufd	$0x4e, %xmm3, %xmm5
	mulpd	%xmm0, %xmm3
	mulpd	%xmm1, %xmm4

	ADD2	%xmm7, %xmm9
	ADD2	%xmm6, %xmm11
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
	subq	$2, %rax

	leaq	(, %rax, SIZE), %rax

	movq	AORIG, AO
	leaq	(AO, %rax, 4), AO
	leaq	(B,  %rax, 4), BO
#endif

	ADD1	%xmm3, %xmm12
	pcmpeqb	%xmm7, %xmm7
	ADD1	%xmm4, %xmm14
	psllq	$63,   %xmm7
	ADD2	%xmm5, %xmm13
	ADD2	%xmm6, %xmm15

#ifndef CONJ
	pshufd	$0x40, %xmm7, %xmm0
	shufps	$0x04, %xmm7, %xmm7

	pxor	%xmm0, %xmm8
	pxor	%xmm0, %xmm10
	pxor	%xmm0, %xmm12
	pxor	%xmm0, %xmm14
#else
#if defined(LN) || defined(LT)
	pshufd	$0x40, %xmm7, %xmm0
#else
	pshufd	$0x04, %xmm7, %xmm0
#endif
	shufps	$0x40, %xmm7, %xmm7

	pxor	%xmm0, %xmm9
	pxor	%xmm0, %xmm11
	pxor	%xmm0, %xmm13
	pxor	%xmm0, %xmm15
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
	subpd	%xmm12,  %xmm11
	subpd	%xmm10,  %xmm13
	subpd	%xmm14,  %xmm15
#else
	movapd	-16 * SIZE(AO), %xmm9
	movapd	-14 * SIZE(AO), %xmm13
	movapd	-12 * SIZE(AO), %xmm11
	movapd	-10 * SIZE(AO), %xmm15

	subpd	%xmm8,   %xmm9
	subpd	%xmm12,  %xmm11
	subpd	%xmm10,  %xmm13
	subpd	%xmm14,  %xmm15
#endif

#ifdef LN
	movddup	-10 * SIZE(AO), %xmm0
	movddup	 -9 * SIZE(AO), %xmm1
	movddup	-12 * SIZE(AO), %xmm2
	movddup	-11 * SIZE(AO), %xmm3
	movddup	-16 * SIZE(AO), %xmm4
	movddup	-15 * SIZE(AO), %xmm5

	pshufd	$0x4e, %xmm13, %xmm12
	pshufd	$0x4e, %xmm15, %xmm14

	xorpd	 %xmm7, %xmm12
	xorpd	 %xmm7, %xmm14

	mulpd	 %xmm0, %xmm13
	mulpd	 %xmm1, %xmm12
	mulpd	 %xmm0, %xmm15
	mulpd	 %xmm1, %xmm14

	addpd	 %xmm12, %xmm13
	addpd	 %xmm14, %xmm15

	movapd	 %xmm13, %xmm8
	movapd	 %xmm15, %xmm10
	pshufd	 $0x4e, %xmm13, %xmm12
	pshufd	 $0x4e, %xmm15, %xmm14

	xorpd	 %xmm7, %xmm12
	xorpd	 %xmm7, %xmm14

	mulpd	 %xmm2, %xmm8
	mulpd	 %xmm2, %xmm10
	mulpd	 %xmm3, %xmm12
	mulpd	 %xmm3, %xmm14

	subpd	 %xmm8, %xmm9
	subpd	 %xmm10, %xmm11
	subpd	 %xmm12, %xmm9
	subpd	 %xmm14, %xmm11

	pshufd	$0x4e, %xmm9, %xmm8
	pshufd	$0x4e, %xmm11, %xmm10

	xorpd	 %xmm7, %xmm8
	xorpd	 %xmm7, %xmm10

	mulpd	 %xmm4, %xmm9
	mulpd	 %xmm5, %xmm8
	mulpd	 %xmm4, %xmm11
	mulpd	 %xmm5, %xmm10

	addpd	 %xmm8, %xmm9
	addpd	 %xmm10, %xmm11
#endif

#ifdef LT
	movddup	-16 * SIZE(AO), %xmm0
	movddup	-15 * SIZE(AO), %xmm1
	movddup	-14 * SIZE(AO), %xmm2
	movddup	-13 * SIZE(AO), %xmm3
	movddup	-10 * SIZE(AO), %xmm4
	movddup	 -9 * SIZE(AO), %xmm5

	pshufd	$0x4e, %xmm9, %xmm8
	pshufd	$0x4e, %xmm11, %xmm10

	xorpd	 %xmm7, %xmm8
	xorpd	 %xmm7, %xmm10

	mulpd	 %xmm0, %xmm9
	mulpd	 %xmm1, %xmm8
	mulpd	 %xmm0, %xmm11
	mulpd	 %xmm1, %xmm10

	addpd	 %xmm8, %xmm9
	addpd	 %xmm10, %xmm11

	movapd	 %xmm9, %xmm8
	movapd	 %xmm11, %xmm10
	pshufd	 $0x4e, %xmm9, %xmm12
	pshufd	 $0x4e, %xmm11, %xmm14

	xorpd	 %xmm7, %xmm12
	xorpd	 %xmm7, %xmm14

	mulpd	 %xmm2, %xmm8
	mulpd	 %xmm2, %xmm10
	mulpd	 %xmm3, %xmm12
	mulpd	 %xmm3, %xmm14

	subpd	 %xmm8, %xmm13
	subpd	 %xmm10, %xmm15
	subpd	 %xmm12, %xmm13
	subpd	 %xmm14, %xmm15

	pshufd	$0x4e, %xmm13, %xmm12
	pshufd	$0x4e, %xmm15, %xmm14

	xorpd	 %xmm7, %xmm12
	xorpd	 %xmm7, %xmm14

	mulpd	 %xmm4, %xmm13
	mulpd	 %xmm5, %xmm12
	mulpd	 %xmm4, %xmm15
	mulpd	 %xmm5, %xmm14

	addpd	 %xmm12, %xmm13
	addpd	 %xmm14, %xmm15
#endif

#ifdef RN
	movddup	-16 * SIZE(BO), %xmm0
	movddup	-15 * SIZE(BO), %xmm1
	movddup	-14 * SIZE(BO), %xmm2
	movddup	-13 * SIZE(BO), %xmm3
	movddup	-10 * SIZE(BO), %xmm4
	movddup	 -9 * SIZE(BO), %xmm5

	pshufd	$0x4e, %xmm9, %xmm8
	pshufd	$0x4e, %xmm13, %xmm12

	xorpd	 %xmm7, %xmm8
	xorpd	 %xmm7, %xmm12

	mulpd	 %xmm0, %xmm9
	mulpd	 %xmm1, %xmm8
	mulpd	 %xmm0, %xmm13
	mulpd	 %xmm1, %xmm12

	addpd	 %xmm8, %xmm9
	addpd	 %xmm12, %xmm13

	movapd	 %xmm9, %xmm8
	movapd	 %xmm13, %xmm10
	pshufd	 $0x4e, %xmm9, %xmm12
	pshufd	 $0x4e, %xmm13, %xmm14

	xorpd	 %xmm7, %xmm12
	xorpd	 %xmm7, %xmm14

	mulpd	 %xmm2, %xmm8
	mulpd	 %xmm2, %xmm10
	mulpd	 %xmm3, %xmm12
	mulpd	 %xmm3, %xmm14

	subpd	 %xmm8, %xmm11
	subpd	 %xmm10, %xmm15
	subpd	 %xmm12, %xmm11
	subpd	 %xmm14, %xmm15

	pshufd	$0x4e, %xmm11, %xmm10
	pshufd	$0x4e, %xmm15, %xmm14

	xorpd	 %xmm7, %xmm10
	xorpd	 %xmm7, %xmm14

	mulpd	 %xmm4, %xmm11
	mulpd	 %xmm5, %xmm10
	mulpd	 %xmm4, %xmm15
	mulpd	 %xmm5, %xmm14

	addpd	 %xmm10, %xmm11
	addpd	 %xmm14, %xmm15
#endif

#ifdef RT
	movddup	-10 * SIZE(BO), %xmm0
	movddup	 -9 * SIZE(BO), %xmm1
	movddup	-12 * SIZE(BO), %xmm2
	movddup	-11 * SIZE(BO), %xmm3
	movddup	-16 * SIZE(BO), %xmm4
	movddup	-15 * SIZE(BO), %xmm5

	pshufd	$0x4e, %xmm11, %xmm10
	pshufd	$0x4e, %xmm15, %xmm14

	xorpd	 %xmm7, %xmm10
	xorpd	 %xmm7, %xmm14

	mulpd	 %xmm0, %xmm11
	mulpd	 %xmm1, %xmm10
	mulpd	 %xmm0, %xmm15
	mulpd	 %xmm1, %xmm14

	addpd	 %xmm10, %xmm11
	addpd	 %xmm14, %xmm15

	movapd	 %xmm11, %xmm8
	movapd	 %xmm15, %xmm10
	pshufd	 $0x4e, %xmm11, %xmm12
	pshufd	 $0x4e, %xmm15, %xmm14

	xorpd	 %xmm7, %xmm12
	xorpd	 %xmm7, %xmm14

	mulpd	 %xmm2, %xmm8
	mulpd	 %xmm2, %xmm10
	mulpd	 %xmm3, %xmm12
	mulpd	 %xmm3, %xmm14

	subpd	 %xmm8, %xmm9
	subpd	 %xmm10, %xmm13
	subpd	 %xmm12, %xmm9
	subpd	 %xmm14, %xmm13

	pshufd	$0x4e, %xmm9, %xmm8
	pshufd	$0x4e, %xmm13, %xmm12

	xorpd	 %xmm7, %xmm8
	xorpd	 %xmm7, %xmm12

	mulpd	 %xmm4, %xmm9
	mulpd	 %xmm5, %xmm8
	mulpd	 %xmm4, %xmm13
	mulpd	 %xmm5, %xmm12

	addpd	 %xmm8, %xmm9
	addpd	 %xmm12, %xmm13
#endif

#ifdef LN
	subq	$4 * SIZE, CO1
	subq	$4 * SIZE, CO2
#endif

	movsd	%xmm9,    0 * SIZE(CO1)
	movhpd	%xmm9,    1 * SIZE(CO1)
	movsd	%xmm13,   2 * SIZE(CO1)
	movhpd	%xmm13,   3 * SIZE(CO1)

	movsd	%xmm11,   0 * SIZE(CO2)
	movhpd	%xmm11,   1 * SIZE(CO2)
	movsd	%xmm15,   2 * SIZE(CO2)
	movhpd	%xmm15,   3 * SIZE(CO2)

#if defined(LN) || defined(LT)
	movapd	%xmm9,  -16 * SIZE(BO)
	movapd	%xmm11, -14 * SIZE(BO)
	movapd	%xmm13, -12 * SIZE(BO)
	movapd	%xmm15, -10 * SIZE(BO)
#else
	movapd	%xmm9,  -16 * SIZE(AO)
	movapd	%xmm13, -14 * SIZE(AO)
	movapd	%xmm11, -12 * SIZE(AO)
	movapd	%xmm15, -10 * SIZE(AO)
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
	BRANCH
	jg	.L11
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
	addq	$2, KK
#endif

#ifdef RT
	subq	$2, KK
#endif

	subq	$1, J
	BRANCH
	jg	.L01
	ALIGN_4

.L40:
	testq	$1, N
	BRANCH
	jle	.L999

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
	leaq	(C, LDC, 1), CO2
#ifndef RT
	addq	LDC, C
#endif

#ifdef LN
	movq	OFFSET, KK
	addq	M, KK
#endif

	movq	K, %rax
	salq	$ZBASE_SHIFT, %rax
	leaq	(B, %rax), BB

#ifdef LT
	movq	OFFSET, KK
#endif

	testq	$1, M
	BRANCH
	jle	.L60
	ALIGN_4

#ifdef LN
       movq	K, %rax
       salq	$ZBASE_SHIFT, %rax
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

	ADD1	%xmm2, %xmm8
	ADD2	%xmm7, %xmm9
	movaps	-14 * SIZE(BO), %xmm2

	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	mulpd	%xmm0, %xmm7
	movaps	-12 * SIZE(AO), %xmm0

	ADD1	%xmm2, %xmm10
	ADD2	%xmm7, %xmm11
	movaps	-12 * SIZE(BO), %xmm2

	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	mulpd	%xmm0, %xmm7
	movaps	-10 * SIZE(AO), %xmm0

	ADD1	%xmm2, %xmm8
	ADD2	%xmm7, %xmm9
	movaps	-10 * SIZE(BO), %xmm2

	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	mulpd	%xmm0, %xmm7
	movaps	 -8 * SIZE(AO), %xmm0

	ADD1	%xmm2, %xmm10
	ADD2	%xmm7, %xmm11
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

	ADD1	%xmm2, %xmm8
	ADD2	%xmm7, %xmm9
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
	subq	$1, %rax

	leaq	(, %rax, SIZE), %rax

	movq	AORIG, AO
	leaq	(AO, %rax, 2), AO
	leaq	(B,  %rax, 2), BO
#endif

	addpd	%xmm10, %xmm8
	addpd	%xmm11, %xmm9

	pcmpeqb	%xmm7, %xmm7
	psllq	$63,   %xmm7

#ifndef CONJ
	pshufd	$0x40, %xmm7, %xmm0
	shufps	$0x04, %xmm7, %xmm7

	pxor	%xmm0, %xmm8
#else
#if defined(LN) || defined(LT)
	pshufd	$0x40, %xmm7, %xmm0
#else
	pshufd	$0x04, %xmm7, %xmm0
#endif
	shufps	$0x40, %xmm7, %xmm7

	pxor	%xmm0, %xmm9
#endif

	haddpd	%xmm9,  %xmm8

#if defined(LN) || defined(LT)
	movapd	-16 * SIZE(BO), %xmm9

	subpd	%xmm8,  %xmm9
#else
	movapd	-16 * SIZE(AO), %xmm9

	subpd	%xmm8,  %xmm9
#endif

#ifdef LN
	movddup	-16 * SIZE(AO), %xmm0
	movddup	-15 * SIZE(AO), %xmm1

	pshufd	$0x4e, %xmm9, %xmm8
	xorpd	 %xmm7, %xmm8

	mulpd	 %xmm0, %xmm9
	mulpd	 %xmm1, %xmm8

	addpd	 %xmm8, %xmm9
#endif

#ifdef LT
	movddup	-16 * SIZE(AO), %xmm0
	movddup	-15 * SIZE(AO), %xmm1

	pshufd	$0x4e, %xmm9, %xmm8

	xorpd	 %xmm7, %xmm8

	mulpd	 %xmm0, %xmm9
	mulpd	 %xmm1, %xmm8

	addpd	 %xmm8, %xmm9
#endif

#ifdef RN
	movddup	-16 * SIZE(BO), %xmm0
	movddup	-15 * SIZE(BO), %xmm1

	pshufd	$0x4e, %xmm9, %xmm8

	xorpd	 %xmm7, %xmm8

	mulpd	 %xmm0, %xmm9
	mulpd	 %xmm1, %xmm8

	addpd	 %xmm8, %xmm9
#endif

#ifdef RT
	movddup	-16 * SIZE(BO), %xmm0
	movddup	-15 * SIZE(BO), %xmm1

	pshufd	$0x4e, %xmm9, %xmm8

	xorpd	 %xmm7, %xmm8

	mulpd	 %xmm0, %xmm9
	mulpd	 %xmm1, %xmm8

	addpd	 %xmm8, %xmm9
#endif

#ifdef LN
	subq	$2 * SIZE, CO1
#endif

	movsd	%xmm9,   0 * SIZE(CO1)
	movhpd	%xmm9,   1 * SIZE(CO1)

#if defined(LN) || defined(LT)
	movapd	%xmm9, -16 * SIZE(BO)
#else
	movapd	%xmm9, -16 * SIZE(AO)
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
	ALIGN_4

.L60:
	movq	M,  I
	sarq	$1, I	# i = (m >> 2)
	NOBRANCH
	jle	.L79
	ALIGN_4

.L51:
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

	prefetcht2	 -16 * SIZE(BB)
	subq		 $-4 * SIZE, BB

	movaps	-16 * SIZE(AO), %xmm0
	movaps	-14 * SIZE(AO), %xmm1
	movaps	-16 * SIZE(BO), %xmm2

	prefetcht0     3 * SIZE(CO1)
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

	ADD1	%xmm2, %xmm8
	movaps	-14 * SIZE(BO), %xmm2
	ADD1	%xmm4, %xmm12
	ADD2	%xmm7, %xmm9
	ADD2	%xmm6, %xmm13

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

	ADD1	%xmm2, %xmm8
	movaps	-12 * SIZE(BO), %xmm2
	ADD1	%xmm4, %xmm12
	ADD2	%xmm7, %xmm9
	ADD2	%xmm6, %xmm13

	movaps	%xmm2, %xmm4
	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm4

	movaps	%xmm7, %xmm6
	mulpd	%xmm0, %xmm7
	movaps	 -4 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm6
	movaps	 -2 * SIZE(AO), %xmm1

	ADD1	%xmm2, %xmm8
	movaps	-10 * SIZE(BO), %xmm2
	ADD1	%xmm4, %xmm12
	ADD2	%xmm7, %xmm9
	ADD2	%xmm6, %xmm13

	movaps	%xmm2, %xmm4
	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm4

	movaps	%xmm7, %xmm6
	mulpd	%xmm0, %xmm7
	movaps	  0 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm6
	movaps	  2 * SIZE(AO), %xmm1

	ADD1	%xmm2, %xmm8
	movaps	 -8 * SIZE(BO), %xmm2
	ADD1	%xmm4, %xmm12
	ADD2	%xmm7, %xmm9
	ADD2	%xmm6, %xmm13

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

	ADD1	%xmm2, %xmm8
	movaps	-14 * SIZE(BO), %xmm2
	ADD1	%xmm4, %xmm12
	ADD2	%xmm7, %xmm9
	ADD2	%xmm6, %xmm13

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
	subq	$2, %rax
#else
	subq	$1, %rax
#endif

	leaq	(, %rax, SIZE), %rax

	movq	AORIG, AO
	leaq	(AO, %rax, 4), AO
	leaq	(B,  %rax, 2), BO
#endif

	pcmpeqb	%xmm7, %xmm7
	psllq	$63,   %xmm7

#ifndef CONJ
	pshufd	$0x40, %xmm7, %xmm0
	shufps	$0x04, %xmm7, %xmm7

	pxor	%xmm0, %xmm8
	pxor	%xmm0, %xmm12
#else
#if defined(LN) || defined(LT)
	pshufd	$0x40, %xmm7, %xmm0
#else
	pshufd	$0x04, %xmm7, %xmm0
#endif
	shufps	$0x40, %xmm7, %xmm7

	pxor	%xmm0, %xmm9
	pxor	%xmm0, %xmm13
#endif

	haddpd	%xmm9,  %xmm8
	haddpd	%xmm13, %xmm12

#if defined(LN) || defined(LT)
	movapd	-16 * SIZE(BO), %xmm9
	movapd	-14 * SIZE(BO), %xmm13

	subpd	%xmm8,  %xmm9
	subpd	%xmm12,  %xmm13
#else
	movapd	-16 * SIZE(AO), %xmm9
	movapd	-14 * SIZE(AO), %xmm13

	subpd	%xmm8,  %xmm9
	subpd	%xmm12,  %xmm13
#endif

#ifdef LN
	movddup	-10 * SIZE(AO), %xmm0
	movddup	 -9 * SIZE(AO), %xmm1
	movddup	-12 * SIZE(AO), %xmm2
	movddup	-11 * SIZE(AO), %xmm3
	movddup	-16 * SIZE(AO), %xmm4
	movddup	-15 * SIZE(AO), %xmm5

	pshufd	$0x4e, %xmm13, %xmm12

	xorpd	 %xmm7, %xmm12

	mulpd	 %xmm0, %xmm13
	mulpd	 %xmm1, %xmm12

	addpd	 %xmm12, %xmm13

	movapd	 %xmm13, %xmm8
	pshufd	 $0x4e, %xmm13, %xmm12

	xorpd	 %xmm7, %xmm12

	mulpd	 %xmm2, %xmm8
	mulpd	 %xmm3, %xmm12

	subpd	 %xmm8, %xmm9
	subpd	 %xmm12, %xmm9

	pshufd	$0x4e, %xmm9, %xmm8

	xorpd	 %xmm7, %xmm8

	mulpd	 %xmm4, %xmm9
	mulpd	 %xmm5, %xmm8

	addpd	 %xmm8, %xmm9
#endif

#ifdef LT
	movddup	-16 * SIZE(AO), %xmm0
	movddup	-15 * SIZE(AO), %xmm1
	movddup	-14 * SIZE(AO), %xmm2
	movddup	-13 * SIZE(AO), %xmm3
	movddup	-10 * SIZE(AO), %xmm4
	movddup	 -9 * SIZE(AO), %xmm5

	pshufd	$0x4e, %xmm9, %xmm8

	xorpd	 %xmm7, %xmm8

	mulpd	 %xmm0, %xmm9
	mulpd	 %xmm1, %xmm8

	addpd	 %xmm8, %xmm9

	movapd	 %xmm9, %xmm8
	pshufd	 $0x4e, %xmm9, %xmm12

	xorpd	 %xmm7, %xmm12

	mulpd	 %xmm2, %xmm8
	mulpd	 %xmm3, %xmm12

	subpd	 %xmm8, %xmm13
	subpd	 %xmm12, %xmm13

	pshufd	$0x4e, %xmm13, %xmm12

	xorpd	 %xmm7, %xmm12

	mulpd	 %xmm4, %xmm13
	mulpd	 %xmm5, %xmm12

	addpd	 %xmm12, %xmm13
#endif

#ifdef RN
	movddup	-16 * SIZE(BO), %xmm0
	movddup	-15 * SIZE(BO), %xmm1

	pshufd	$0x4e, %xmm9, %xmm8
	pshufd	$0x4e, %xmm13, %xmm12

	xorpd	 %xmm7, %xmm8
	xorpd	 %xmm7, %xmm12

	mulpd	 %xmm0, %xmm9
	mulpd	 %xmm1, %xmm8
	mulpd	 %xmm0, %xmm13
	mulpd	 %xmm1, %xmm12

	addpd	 %xmm8, %xmm9
	addpd	 %xmm12, %xmm13
#endif

#ifdef RT
	movddup	-16 * SIZE(BO), %xmm0
	movddup	-15 * SIZE(BO), %xmm1

	pshufd	$0x4e, %xmm9, %xmm8
	pshufd	$0x4e, %xmm13, %xmm12

	xorpd	 %xmm7, %xmm8
	xorpd	 %xmm7, %xmm12

	mulpd	 %xmm0, %xmm9
	mulpd	 %xmm1, %xmm8
	mulpd	 %xmm0, %xmm13
	mulpd	 %xmm1, %xmm12

	addpd	 %xmm8, %xmm9
	addpd	 %xmm12, %xmm13
#endif

#ifdef LN
	subq	$4 * SIZE, CO1
#endif

	movsd	%xmm9,   0 * SIZE(CO1)
	movhpd	%xmm9,   1 * SIZE(CO1)
	movsd	%xmm13,  2 * SIZE(CO1)
	movhpd	%xmm13,  3 * SIZE(CO1)

#if defined(LN) || defined(LT)
	movapd	%xmm9,  -16 * SIZE(BO)
	movapd	%xmm13, -14 * SIZE(BO)
#else
	movapd	%xmm9,  -16 * SIZE(AO)
	movapd	%xmm13, -14 * SIZE(AO)
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
	decq	I
	BRANCH
	jg	.L51
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
