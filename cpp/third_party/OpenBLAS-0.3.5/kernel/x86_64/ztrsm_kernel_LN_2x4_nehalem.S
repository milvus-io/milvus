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

#define OLD_ALPHA_I	40 + STACKSIZE(%rsp)
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

#define PREFETCHSIZE  (16 * 1 + 4)
#define PREFETCH     prefetcht0

#define ADD1	  addps
#define ADD2	  addps

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
	sarq	$2, J
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

	testq	$1, M
	BRANCH
	jle	.L20

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

	xorps	%xmm1, %xmm1
	movddup  -32 * SIZE(AO), %xmm0
	xorps	%xmm2, %xmm2
	movaps   -32 * SIZE(BO), %xmm5
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
	ADD1	%xmm1, %xmm8
	pshufd	$0xa0, %xmm5, %xmm1
	mulps	%xmm0, %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0xf5, %xmm5, %xmm2
	movaps   -28 * SIZE(BO), %xmm5
	mulps	%xmm0, %xmm2
	ADD1	%xmm3, %xmm10
	pshufd	$0xa0, %xmm5, %xmm3
	mulps	%xmm0, %xmm3
	ADD2	%xmm4, %xmm11
	pshufd	$0xf5, %xmm5, %xmm4
	movaps   -24 * SIZE(BO), %xmm5
	mulps	%xmm0, %xmm4
	movddup  -30 * SIZE(AO), %xmm0

	ADD1	%xmm1, %xmm8
	pshufd	$0xa0, %xmm5, %xmm1
	mulps	%xmm0, %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0xf5, %xmm5, %xmm2
	movaps   -20 * SIZE(BO), %xmm5
	mulps	%xmm0, %xmm2
	ADD1	%xmm3, %xmm10
	pshufd	$0xa0, %xmm5, %xmm3
	mulps	%xmm0, %xmm3
	ADD2	%xmm4, %xmm11
	pshufd	$0xf5, %xmm5, %xmm4
	movaps   -16 * SIZE(BO), %xmm5
	mulps	%xmm0, %xmm4
	movddup  -28 * SIZE(AO), %xmm0

	ADD1	%xmm1, %xmm8
	pshufd	$0xa0, %xmm5, %xmm1
	mulps	%xmm0, %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0xf5, %xmm5, %xmm2
	movaps   -12 * SIZE(BO), %xmm5
	mulps	%xmm0, %xmm2
	ADD1	%xmm3, %xmm10
	pshufd	$0xa0, %xmm5, %xmm3
	mulps	%xmm0, %xmm3
	ADD2	%xmm4, %xmm11
	pshufd	$0xf5, %xmm5, %xmm4
	movaps    -8 * SIZE(BO), %xmm5
	mulps	%xmm0, %xmm4
	movddup  -26 * SIZE(AO), %xmm0

	ADD1	%xmm1, %xmm8
	pshufd	$0xa0, %xmm5, %xmm1
	mulps	%xmm0, %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0xf5, %xmm5, %xmm2
	movaps    -4 * SIZE(BO), %xmm5
	mulps	%xmm0, %xmm2
	ADD1	%xmm3, %xmm10
	pshufd	$0xa0, %xmm5, %xmm3
	mulps	%xmm0, %xmm3
	ADD2	%xmm4, %xmm11
	pshufd	$0xf5, %xmm5, %xmm4
	movaps     0 * SIZE(BO), %xmm5
	mulps	%xmm0, %xmm4
	movddup  -24 * SIZE(AO), %xmm0

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
	ADD1	%xmm1, %xmm8
	pshufd	$0xa0, %xmm5, %xmm1
	mulps	%xmm0, %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0xf5, %xmm5, %xmm2
	movaps   -28 * SIZE(BO), %xmm5
	mulps	%xmm0, %xmm2
	ADD1	%xmm3, %xmm10
	pshufd	$0xa0, %xmm5, %xmm3
	mulps	%xmm0, %xmm3
	ADD2	%xmm4, %xmm11
	pshufd	$0xf5, %xmm5, %xmm4
	movaps   -24 * SIZE(BO), %xmm5
	mulps	%xmm0, %xmm4
	movddup  -30 * SIZE(AO), %xmm0

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
	subq	$1, %rax
#else
	subq	$4, %rax
#endif

	salq	$ZBASE_SHIFT, %rax

	movq	AORIG, AO
	leaq	(AO, %rax, 1), AO
	leaq	(B,  %rax, 4), BO
#endif

	ADD1	%xmm1, %xmm8
	ADD2	%xmm2, %xmm9
	ADD1	%xmm3, %xmm10
	ADD2	%xmm4, %xmm11

	pcmpeqb	%xmm0, %xmm0
	psllq	$63,   %xmm0

#if defined(LN) || defined(LT)

#ifndef CONJ
	xorps	%xmm0, %xmm9
	xorps	%xmm0, %xmm11
	shufps	$0xb1, %xmm9,  %xmm9
	shufps	$0xb1, %xmm11, %xmm11
#else
	xorps	%xmm0, %xmm8
	xorps	%xmm0, %xmm10
	shufps	$0xb1, %xmm9,  %xmm9
	shufps	$0xb1, %xmm11, %xmm11
#endif

#else

#ifndef CONJ
	xorps	%xmm0, %xmm9
	xorps	%xmm0, %xmm11
	shufps	$0xb1, %xmm9,  %xmm9
	shufps	$0xb1, %xmm11, %xmm11
#else
	shufps	$0xb1, %xmm9,  %xmm9
	shufps	$0xb1, %xmm11, %xmm11
	xorps	%xmm0, %xmm9
	xorps	%xmm0, %xmm11
#endif

#endif

	addps	%xmm9,  %xmm8
	addps	%xmm11, %xmm10

#if defined(LN) || defined(LT)
	movaps	-32 * SIZE(BO), %xmm9
	movaps	-28 * SIZE(BO), %xmm11

	subps	%xmm8,   %xmm9
	subps	%xmm10,  %xmm11
#else
	movaps	-32 * SIZE(AO), %xmm9
	movaps	-28 * SIZE(AO), %xmm13

	subps	%xmm8,   %xmm9
	subps	%xmm10,  %xmm13

	movhlps	%xmm9,  %xmm11
	movhlps	%xmm13, %xmm15
#endif

	pcmpeqb	%xmm7, %xmm7
	psllq	$63,   %xmm7

#ifndef CONJ
	shufps	$0xb1, %xmm7,  %xmm7
#endif

#if defined(LN) || defined(LT)
	movsd	-32 * SIZE(AO), %xmm5

	pshufd	$0x00, %xmm5, %xmm0
	pshufd	$0x55, %xmm5, %xmm1

	pshufd	$0xb1, %xmm9,  %xmm10
	pshufd	$0xb1, %xmm11, %xmm12

	xorps	 %xmm7, %xmm10
	xorps	 %xmm7, %xmm12

	mulps	 %xmm0, %xmm9
	mulps	 %xmm1, %xmm10
	mulps	 %xmm0, %xmm11
	mulps	 %xmm1, %xmm12

	addps	 %xmm10, %xmm9
	addps	 %xmm12, %xmm11
#endif

#ifdef RN
	movaps	-32 * SIZE(BO), %xmm5

	pshufd	$0x00, %xmm5, %xmm0
	pshufd	$0x55, %xmm5, %xmm1

	pshufd	$0xb1, %xmm9, %xmm8

	xorps	 %xmm7, %xmm8

	mulps	 %xmm0, %xmm9
	mulps	 %xmm1, %xmm8

	addps	 %xmm8, %xmm9

	movaps	%xmm9,  %xmm3
	pshufd	$0xb1, %xmm9, %xmm2

	xorps	 %xmm7, %xmm2

	pshufd	$0xaa, %xmm5, %xmm0
	pshufd	$0xff, %xmm5, %xmm1

	mulps	 %xmm3, %xmm0
	mulps	 %xmm2, %xmm1

	subps	 %xmm0, %xmm11
	subps	 %xmm1, %xmm11

	movaps	-28 * SIZE(BO), %xmm5

	pshufd	$0x00, %xmm5, %xmm0
	pshufd	$0x55, %xmm5, %xmm1

	mulps	 %xmm3, %xmm0
	mulps	 %xmm2, %xmm1

	subps	 %xmm0, %xmm13
	subps	 %xmm1, %xmm13

	pshufd	$0xaa, %xmm5, %xmm0
	pshufd	$0xff, %xmm5, %xmm1

	mulps	 %xmm3, %xmm0
	mulps	 %xmm2, %xmm1

	subps	 %xmm0, %xmm15
	subps	 %xmm1, %xmm15

	movaps	-24 * SIZE(BO), %xmm5

	pshufd	$0xaa, %xmm5, %xmm0
	pshufd	$0xff, %xmm5, %xmm1

	pshufd	$0xb1, %xmm11, %xmm10

	xorps	 %xmm7, %xmm10

	mulps	 %xmm0, %xmm11
	mulps	 %xmm1, %xmm10

	addps	 %xmm10, %xmm11

	movaps	%xmm11,  %xmm3
	pshufd	$0xb1, %xmm11, %xmm2

	xorps	 %xmm7, %xmm2

	movaps	-20 * SIZE(BO), %xmm5

	pshufd	$0x00, %xmm5, %xmm0
	pshufd	$0x55, %xmm5, %xmm1

	mulps	 %xmm3, %xmm0
	mulps	 %xmm2, %xmm1

	subps	 %xmm0, %xmm13
	subps	 %xmm1, %xmm13

	pshufd	$0xaa, %xmm5, %xmm0
	pshufd	$0xff, %xmm5, %xmm1

	mulps	 %xmm3, %xmm0
	mulps	 %xmm2, %xmm1

	subps	 %xmm0, %xmm15
	subps	 %xmm1, %xmm15

	movaps	-12 * SIZE(BO), %xmm5

	pshufd	$0x00, %xmm5, %xmm0
	pshufd	$0x55, %xmm5, %xmm1

	pshufd	$0xb1, %xmm13, %xmm12

	xorps	 %xmm7, %xmm12

	mulps	 %xmm0, %xmm13
	mulps	 %xmm1, %xmm12

	addps	 %xmm12, %xmm13

	movaps	%xmm13,  %xmm3
	pshufd	$0xb1, %xmm13, %xmm2

	xorps	 %xmm7, %xmm2

	pshufd	$0xaa, %xmm5, %xmm0
	pshufd	$0xff, %xmm5, %xmm1

	mulps	 %xmm3, %xmm0
	mulps	 %xmm2, %xmm1

	subps	 %xmm0, %xmm15
	subps	 %xmm1, %xmm15

	movaps	 -4 * SIZE(BO), %xmm5

	pshufd	$0xaa, %xmm5, %xmm0
	pshufd	$0xff, %xmm5, %xmm1

	pshufd	$0xb1, %xmm15, %xmm14

	xorps	 %xmm7, %xmm14

	mulps	 %xmm0, %xmm15
	mulps	 %xmm1, %xmm14

	addps	 %xmm14, %xmm15
#endif

#ifdef RT
	movaps	 -4 * SIZE(BO), %xmm5

	pshufd	$0xaa, %xmm5, %xmm0
	pshufd	$0xff, %xmm5, %xmm1

	pshufd	$0xb1, %xmm15, %xmm14

	xorps	 %xmm7, %xmm14

	mulps	 %xmm0, %xmm15
	mulps	 %xmm1, %xmm14

	addps	 %xmm14, %xmm15

	movaps	%xmm15,  %xmm3
	pshufd	$0xb1, %xmm15, %xmm2

	xorps	 %xmm7, %xmm2

	pshufd	$0x00, %xmm5, %xmm0
	pshufd	$0x55, %xmm5, %xmm1

	mulps	 %xmm3, %xmm0
	mulps	 %xmm2, %xmm1

	subps	 %xmm0, %xmm13
	subps	 %xmm1, %xmm13

	movaps	 -8 * SIZE(BO), %xmm5

	pshufd	$0xaa, %xmm5, %xmm0
	pshufd	$0xff, %xmm5, %xmm1

	mulps	 %xmm3, %xmm0
	mulps	 %xmm2, %xmm1

	subps	 %xmm0, %xmm11
	subps	 %xmm1, %xmm11

	pshufd	$0x00, %xmm5, %xmm0
	pshufd	$0x55, %xmm5, %xmm1

	mulps	 %xmm3, %xmm0
	mulps	 %xmm2, %xmm1

	subps	 %xmm0, %xmm9
	subps	 %xmm1, %xmm9

	movaps	-12 * SIZE(BO), %xmm5

	pshufd	$0x00, %xmm5, %xmm0
	pshufd	$0x55, %xmm5, %xmm1

	pshufd	$0xb1, %xmm13, %xmm12

	xorps	 %xmm7, %xmm12

	mulps	 %xmm0, %xmm13
	mulps	 %xmm1, %xmm12

	addps	 %xmm12, %xmm13

	movaps	%xmm13,  %xmm3
	pshufd	$0xb1, %xmm13, %xmm2

	xorps	 %xmm7, %xmm2

	movaps	-16 * SIZE(BO), %xmm5

	pshufd	$0xaa, %xmm5, %xmm0
	pshufd	$0xff, %xmm5, %xmm1

	mulps	 %xmm3, %xmm0
	mulps	 %xmm2, %xmm1

	subps	 %xmm0, %xmm11
	subps	 %xmm1, %xmm11

	pshufd	$0x00, %xmm5, %xmm0
	pshufd	$0x55, %xmm5, %xmm1

	mulps	 %xmm3, %xmm0
	mulps	 %xmm2, %xmm1

	subps	 %xmm0, %xmm9
	subps	 %xmm1, %xmm9

	movaps	-24 * SIZE(BO), %xmm5

	pshufd	$0xaa, %xmm5, %xmm0
	pshufd	$0xff, %xmm5, %xmm1

	pshufd	$0xb1, %xmm11, %xmm10

	xorps	 %xmm7, %xmm10

	mulps	 %xmm0, %xmm11
	mulps	 %xmm1, %xmm10

	addps	 %xmm10, %xmm11

	movaps	%xmm11,  %xmm3
	pshufd	$0xb1, %xmm11, %xmm2

	xorps	 %xmm7, %xmm2

	pshufd	$0x00, %xmm5, %xmm0
	pshufd	$0x55, %xmm5, %xmm1

	mulps	 %xmm3, %xmm0
	mulps	 %xmm2, %xmm1

	subps	 %xmm0, %xmm9
	subps	 %xmm1, %xmm9

	movaps	-32 * SIZE(BO), %xmm5

	pshufd	$0x00, %xmm5, %xmm0
	pshufd	$0x55, %xmm5, %xmm1

	pshufd	$0xb1, %xmm9, %xmm8

	xorps	 %xmm7, %xmm8

	mulps	 %xmm0, %xmm9
	mulps	 %xmm1, %xmm8

	addps	 %xmm8, %xmm9
#endif

#ifdef LN
	subq	$2 * SIZE, CO1
	subq	$2 * SIZE, CO2
#endif

#if defined(LN) || defined(LT)
	movaps	%xmm9,  -32 * SIZE(BO)
	movaps	%xmm11, -28 * SIZE(BO)

	movsd	%xmm9,  (CO1)
	movhps	%xmm9,  (CO1, LDC)
	movsd	%xmm11, (CO2)
	movhps	%xmm11, (CO2, LDC)
#else
	movlhps %xmm11, %xmm9
	movlhps %xmm15, %xmm13

	movaps	%xmm9,  -32 * SIZE(AO)
	movaps	%xmm13, -28 * SIZE(AO)

	movlps	%xmm9,  (CO1)
	movlps	%xmm11, (CO1, LDC)
	movlps	%xmm13, (CO2)
	movlps	%xmm15, (CO2, LDC)
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
	ALIGN_4

.L20:
	movq	M,  I
	sarq	$1, I
	NOBRANCH
	jle	.L29
	ALIGN_4

.L11:
#ifdef LN
       movq	K, %rax
       salq	$1 + ZBASE_SHIFT, %rax
       subq	%rax, AORIG
#endif

#if defined(LN) || defined(RT)
	movq	AORIG, AO

	movq	KK, %rax
	salq	$ZBASE_SHIFT, %rax

	leaq	(AO, %rax, 2), AO
	leaq	(B,  %rax, 4), BO
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

	xorps	%xmm8,  %xmm8
	prefetcht2     4 * SIZE(CO1)
	xorps	%xmm9,  %xmm9
	prefetcht2     4 * SIZE(CO1, LDC,  1)
	xorps	%xmm10, %xmm10
	xorps	%xmm11, %xmm11

	xorps	%xmm12, %xmm12
	prefetcht2     4 * SIZE(CO2)
	xorps	%xmm13, %xmm13
	prefetcht2     4 * SIZE(CO2, LDC,  1)
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
	movaps	-32 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm13
	pshufd	$0xb1, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	pshufd	$0x1b, %xmm2, %xmm5
	mulps	%xmm0, %xmm2

	ADD1	%xmm3, %xmm14
	ADD2	%xmm4, %xmm15
	pshufd	$0xb1, %xmm5, %xmm6
	mulps	%xmm0, %xmm5
	mulps	%xmm0, %xmm6

	ADD1	%xmm1, %xmm8
	movaps	-28 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0xb1, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	pshufd	$0x1b, %xmm2, %xmm3
	mulps	%xmm0, %xmm2

	ADD1	%xmm5, %xmm10
	ADD2	%xmm6, %xmm11
	pshufd	$0xb1, %xmm3, %xmm4
	mulps	%xmm0, %xmm3
	mulps	%xmm0, %xmm4

	movaps	-28 * SIZE(AO), %xmm0

	ADD1	%xmm1, %xmm12
	movaps	-24 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm13
	pshufd	$0xb1, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	pshufd	$0x1b, %xmm2, %xmm5
	mulps	%xmm0, %xmm2

	ADD1	%xmm3, %xmm14
	ADD2	%xmm4, %xmm15
	pshufd	$0xb1, %xmm5, %xmm6
	mulps	%xmm0, %xmm5
	mulps	%xmm0, %xmm6

	ADD1	%xmm1, %xmm8
	movaps	-20 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0xb1, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	pshufd	$0x1b, %xmm2, %xmm3
	mulps	%xmm0, %xmm2

	ADD1	%xmm5, %xmm10
	ADD2	%xmm6, %xmm11
	pshufd	$0xb1, %xmm3, %xmm4
	mulps	%xmm0, %xmm3
	mulps	%xmm0, %xmm4

	movaps	-24 * SIZE(AO), %xmm0

	ADD1	%xmm1, %xmm12
	movaps	-16 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm13
	pshufd	$0xb1, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	pshufd	$0x1b, %xmm2, %xmm5
	mulps	%xmm0, %xmm2

	ADD1	%xmm3, %xmm14
	ADD2	%xmm4, %xmm15
	pshufd	$0xb1, %xmm5, %xmm6
	mulps	%xmm0, %xmm5
	mulps	%xmm0, %xmm6

	ADD1	%xmm1, %xmm8
	movaps	-12 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0xb1, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	pshufd	$0x1b, %xmm2, %xmm3
	mulps	%xmm0, %xmm2

	ADD1	%xmm5, %xmm10
	ADD2	%xmm6, %xmm11
	pshufd	$0xb1, %xmm3, %xmm4
	mulps	%xmm0, %xmm3
	mulps	%xmm0, %xmm4

	movaps	-20 * SIZE(AO), %xmm0

	ADD1	%xmm1, %xmm12
	movaps	 -8 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm13
	pshufd	$0xb1, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	pshufd	$0x1b, %xmm2, %xmm5
	mulps	%xmm0, %xmm2

	ADD1	%xmm3, %xmm14
	ADD2	%xmm4, %xmm15
	pshufd	$0xb1, %xmm5, %xmm6
	mulps	%xmm0, %xmm5
	mulps	%xmm0, %xmm6

	ADD1	%xmm1, %xmm8
	movaps	 -4 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm9
	subq	$-32 * SIZE, BO
	pshufd	$0xb1, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	pshufd	$0x1b, %xmm2, %xmm3
	mulps	%xmm0, %xmm2

	ADD1	%xmm5, %xmm10
	ADD2	%xmm6, %xmm11
	pshufd	$0xb1, %xmm3, %xmm4
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
	ADD1	%xmm1, %xmm12
	movaps	-32 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm13
	pshufd	$0xb1, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	pshufd	$0x1b, %xmm2, %xmm5
	mulps	%xmm0, %xmm2

	ADD1	%xmm3, %xmm14
	ADD2	%xmm4, %xmm15
	pshufd	$0xb1, %xmm5, %xmm6
	mulps	%xmm0, %xmm5
	mulps	%xmm0, %xmm6

	ADD1	%xmm1, %xmm8
	movaps	-28 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0xb1, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	pshufd	$0x1b, %xmm2, %xmm3
	mulps	%xmm0, %xmm2

	ADD1	%xmm5, %xmm10
	ADD2	%xmm6, %xmm11
	pshufd	$0xb1, %xmm3, %xmm4
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
	subq	$2, %rax
#else
	subq	$4, %rax
#endif

	salq	$ZBASE_SHIFT, %rax

	movq	AORIG, AO
	leaq	(AO, %rax, 2), AO
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
	xorps	%xmm0, %xmm8
	xorps	%xmm0, %xmm10
	xorps	%xmm0, %xmm12
	xorps	%xmm0, %xmm14
#else
	xorps	%xmm0, %xmm9
	xorps	%xmm0, %xmm11
	xorps	%xmm0, %xmm13
	xorps	%xmm0, %xmm15
#endif

#else

#ifndef CONJ
	xorps	%xmm0, %xmm8
	xorps	%xmm0, %xmm10
	xorps	%xmm0, %xmm12
	xorps	%xmm0, %xmm14
#else
	shufps	$0xb1, %xmm0,  %xmm0

	xorps	%xmm0, %xmm9
	xorps	%xmm0, %xmm11
	xorps	%xmm0, %xmm13
	xorps	%xmm0, %xmm15
#endif

#endif

	haddps	%xmm9,  %xmm8
	haddps	%xmm11, %xmm10
	haddps	%xmm13, %xmm12
	haddps	%xmm15, %xmm14

	shufps	$0xd8, %xmm8,  %xmm8
	shufps	$0xd8, %xmm10, %xmm10
	shufps	$0xd8, %xmm12, %xmm12
	shufps	$0xd8, %xmm14, %xmm14

	movaps	%xmm8, %xmm9
	shufps	$0xe4, %xmm10, %xmm8
	shufps	$0xe4, %xmm9, %xmm10

	movaps	%xmm12, %xmm13
	shufps	$0xe4, %xmm14, %xmm12
	shufps	$0xe4, %xmm13, %xmm14

#if defined(LN) || defined(LT)
	movaps	%xmm8,  %xmm9
	movlhps	%xmm10, %xmm8
	movhlps %xmm9,  %xmm10

	movaps	%xmm12, %xmm11
	movlhps	%xmm14, %xmm12
	movhlps %xmm11, %xmm14

	movaps	-32 * SIZE(BO), %xmm9
	movaps	-28 * SIZE(BO), %xmm13
	movaps	-24 * SIZE(BO), %xmm11
	movaps	-20 * SIZE(BO), %xmm15

	subps	%xmm8,   %xmm9
	subps	%xmm10,  %xmm11
	subps	%xmm12,  %xmm13
	subps	%xmm14,  %xmm15
#else
	movaps	-32 * SIZE(AO), %xmm9
	movaps	-28 * SIZE(AO), %xmm11
	movaps	-24 * SIZE(AO), %xmm13
	movaps	-20 * SIZE(AO), %xmm15

	subps	%xmm8,   %xmm9
	subps	%xmm10,  %xmm11
	subps	%xmm12,  %xmm13
	subps	%xmm14,  %xmm15
#endif

	pcmpeqb	%xmm7, %xmm7
	psllq	$63,   %xmm7

#ifndef CONJ
	shufps	$0xb1, %xmm7,  %xmm7
#endif

#ifdef LN
	movaps	-28 * SIZE(AO), %xmm5

	pshufd	$0xaa, %xmm5, %xmm0
	pshufd	$0xff, %xmm5, %xmm1

	pshufd	$0xb1, %xmm11, %xmm10
	pshufd	$0xb1, %xmm15, %xmm14

	xorps	 %xmm7, %xmm10
	xorps	 %xmm7, %xmm14

	mulps	 %xmm0, %xmm11
	mulps	 %xmm1, %xmm10
	mulps	 %xmm0, %xmm15
	mulps	 %xmm1, %xmm14

	addps	 %xmm10, %xmm11
	addps	 %xmm14, %xmm15

	pshufd	$0x00, %xmm5, %xmm0
	pshufd	$0x55, %xmm5, %xmm1

	movaps	%xmm11, %xmm3
	pshufd	$0xb1, %xmm11, %xmm2
	movaps	%xmm15, %xmm5
	pshufd	$0xb1, %xmm15, %xmm4

	xorps	 %xmm7, %xmm2
	xorps	 %xmm7, %xmm4

	mulps	 %xmm0, %xmm3
	mulps	 %xmm1, %xmm2
	mulps	 %xmm0, %xmm5
	mulps	 %xmm1, %xmm4

	subps	 %xmm3, %xmm9
	subps	 %xmm2, %xmm9
	subps	 %xmm5, %xmm13
	subps	 %xmm4, %xmm13

	movaps	-32 * SIZE(AO), %xmm5

	pshufd	$0x00, %xmm5, %xmm0
	pshufd	$0x55, %xmm5, %xmm1

	pshufd	$0xb1, %xmm9, %xmm10
	pshufd	$0xb1, %xmm13, %xmm14

	xorps	 %xmm7, %xmm10
	xorps	 %xmm7, %xmm14

	mulps	 %xmm0, %xmm9
	mulps	 %xmm1, %xmm10
	mulps	 %xmm0, %xmm13
	mulps	 %xmm1, %xmm14

	addps	 %xmm10, %xmm9
	addps	 %xmm14, %xmm13
#endif

#ifdef LT
	movaps	-32 * SIZE(AO), %xmm5

	pshufd	$0x00, %xmm5, %xmm0
	pshufd	$0x55, %xmm5, %xmm1

	pshufd	$0xb1, %xmm9, %xmm10
	pshufd	$0xb1, %xmm13, %xmm14

	xorps	 %xmm7, %xmm10
	xorps	 %xmm7, %xmm14

	mulps	 %xmm0, %xmm9
	mulps	 %xmm1, %xmm10
	mulps	 %xmm0, %xmm13
	mulps	 %xmm1, %xmm14

	addps	 %xmm10, %xmm9
	addps	 %xmm14, %xmm13

	pshufd	$0xaa, %xmm5, %xmm0
	pshufd	$0xff, %xmm5, %xmm1

	movaps	%xmm9, %xmm3
	pshufd	$0xb1, %xmm9, %xmm2
	movaps	%xmm13, %xmm5
	pshufd	$0xb1, %xmm13, %xmm4

	xorps	 %xmm7, %xmm2
	xorps	 %xmm7, %xmm4

	mulps	 %xmm0, %xmm3
	mulps	 %xmm1, %xmm2
	mulps	 %xmm0, %xmm5
	mulps	 %xmm1, %xmm4

	subps	 %xmm3, %xmm11
	subps	 %xmm2, %xmm11
	subps	 %xmm5, %xmm15
	subps	 %xmm4, %xmm15

	movaps	-28 * SIZE(AO), %xmm5

	pshufd	$0xaa, %xmm5, %xmm0
	pshufd	$0xff, %xmm5, %xmm1

	pshufd	$0xb1, %xmm11, %xmm10
	pshufd	$0xb1, %xmm15, %xmm14

	xorps	 %xmm7, %xmm10
	xorps	 %xmm7, %xmm14

	mulps	 %xmm0, %xmm11
	mulps	 %xmm1, %xmm10
	mulps	 %xmm0, %xmm15
	mulps	 %xmm1, %xmm14

	addps	 %xmm10, %xmm11
	addps	 %xmm14, %xmm15
#endif

#ifdef RN
	movaps	-32 * SIZE(BO), %xmm5

	pshufd	$0x00, %xmm5, %xmm0
	pshufd	$0x55, %xmm5, %xmm1

	pshufd	$0xb1, %xmm9, %xmm8

	xorps	 %xmm7, %xmm8

	mulps	 %xmm0, %xmm9
	mulps	 %xmm1, %xmm8

	addps	 %xmm8, %xmm9

	movaps	%xmm9,  %xmm3
	pshufd	$0xb1, %xmm9, %xmm2

	xorps	 %xmm7, %xmm2

	pshufd	$0xaa, %xmm5, %xmm0
	pshufd	$0xff, %xmm5, %xmm1

	mulps	 %xmm3, %xmm0
	mulps	 %xmm2, %xmm1

	subps	 %xmm0, %xmm11
	subps	 %xmm1, %xmm11

	movaps	-28 * SIZE(BO), %xmm5

	pshufd	$0x00, %xmm5, %xmm0
	pshufd	$0x55, %xmm5, %xmm1

	mulps	 %xmm3, %xmm0
	mulps	 %xmm2, %xmm1

	subps	 %xmm0, %xmm13
	subps	 %xmm1, %xmm13

	pshufd	$0xaa, %xmm5, %xmm0
	pshufd	$0xff, %xmm5, %xmm1

	mulps	 %xmm3, %xmm0
	mulps	 %xmm2, %xmm1

	subps	 %xmm0, %xmm15
	subps	 %xmm1, %xmm15

	movaps	-24 * SIZE(BO), %xmm5

	pshufd	$0xaa, %xmm5, %xmm0
	pshufd	$0xff, %xmm5, %xmm1

	pshufd	$0xb1, %xmm11, %xmm10

	xorps	 %xmm7, %xmm10

	mulps	 %xmm0, %xmm11
	mulps	 %xmm1, %xmm10

	addps	 %xmm10, %xmm11

	movaps	%xmm11,  %xmm3
	pshufd	$0xb1, %xmm11, %xmm2

	xorps	 %xmm7, %xmm2

	movaps	-20 * SIZE(BO), %xmm5

	pshufd	$0x00, %xmm5, %xmm0
	pshufd	$0x55, %xmm5, %xmm1

	mulps	 %xmm3, %xmm0
	mulps	 %xmm2, %xmm1

	subps	 %xmm0, %xmm13
	subps	 %xmm1, %xmm13

	pshufd	$0xaa, %xmm5, %xmm0
	pshufd	$0xff, %xmm5, %xmm1

	mulps	 %xmm3, %xmm0
	mulps	 %xmm2, %xmm1

	subps	 %xmm0, %xmm15
	subps	 %xmm1, %xmm15

	movaps	-12 * SIZE(BO), %xmm5

	pshufd	$0x00, %xmm5, %xmm0
	pshufd	$0x55, %xmm5, %xmm1

	pshufd	$0xb1, %xmm13, %xmm12

	xorps	 %xmm7, %xmm12

	mulps	 %xmm0, %xmm13
	mulps	 %xmm1, %xmm12

	addps	 %xmm12, %xmm13

	movaps	%xmm13,  %xmm3
	pshufd	$0xb1, %xmm13, %xmm2

	xorps	 %xmm7, %xmm2

	pshufd	$0xaa, %xmm5, %xmm0
	pshufd	$0xff, %xmm5, %xmm1

	mulps	 %xmm3, %xmm0
	mulps	 %xmm2, %xmm1

	subps	 %xmm0, %xmm15
	subps	 %xmm1, %xmm15

	movaps	 -4 * SIZE(BO), %xmm5

	pshufd	$0xaa, %xmm5, %xmm0
	pshufd	$0xff, %xmm5, %xmm1

	pshufd	$0xb1, %xmm15, %xmm14

	xorps	 %xmm7, %xmm14

	mulps	 %xmm0, %xmm15
	mulps	 %xmm1, %xmm14

	addps	 %xmm14, %xmm15
#endif

#ifdef RT
	movaps	 -4 * SIZE(BO), %xmm5

	pshufd	$0xaa, %xmm5, %xmm0
	pshufd	$0xff, %xmm5, %xmm1

	pshufd	$0xb1, %xmm15, %xmm14

	xorps	 %xmm7, %xmm14

	mulps	 %xmm0, %xmm15
	mulps	 %xmm1, %xmm14

	addps	 %xmm14, %xmm15

	movaps	%xmm15,  %xmm3
	pshufd	$0xb1, %xmm15, %xmm2

	xorps	 %xmm7, %xmm2

	pshufd	$0x00, %xmm5, %xmm0
	pshufd	$0x55, %xmm5, %xmm1

	mulps	 %xmm3, %xmm0
	mulps	 %xmm2, %xmm1

	subps	 %xmm0, %xmm13
	subps	 %xmm1, %xmm13

	movaps	 -8 * SIZE(BO), %xmm5

	pshufd	$0xaa, %xmm5, %xmm0
	pshufd	$0xff, %xmm5, %xmm1

	mulps	 %xmm3, %xmm0
	mulps	 %xmm2, %xmm1

	subps	 %xmm0, %xmm11
	subps	 %xmm1, %xmm11

	pshufd	$0x00, %xmm5, %xmm0
	pshufd	$0x55, %xmm5, %xmm1

	mulps	 %xmm3, %xmm0
	mulps	 %xmm2, %xmm1

	subps	 %xmm0, %xmm9
	subps	 %xmm1, %xmm9

	movaps	-12 * SIZE(BO), %xmm5

	pshufd	$0x00, %xmm5, %xmm0
	pshufd	$0x55, %xmm5, %xmm1

	pshufd	$0xb1, %xmm13, %xmm12

	xorps	 %xmm7, %xmm12

	mulps	 %xmm0, %xmm13
	mulps	 %xmm1, %xmm12

	addps	 %xmm12, %xmm13

	movaps	%xmm13,  %xmm3
	pshufd	$0xb1, %xmm13, %xmm2

	xorps	 %xmm7, %xmm2

	movaps	-16 * SIZE(BO), %xmm5

	pshufd	$0xaa, %xmm5, %xmm0
	pshufd	$0xff, %xmm5, %xmm1

	mulps	 %xmm3, %xmm0
	mulps	 %xmm2, %xmm1

	subps	 %xmm0, %xmm11
	subps	 %xmm1, %xmm11

	pshufd	$0x00, %xmm5, %xmm0
	pshufd	$0x55, %xmm5, %xmm1

	mulps	 %xmm3, %xmm0
	mulps	 %xmm2, %xmm1

	subps	 %xmm0, %xmm9
	subps	 %xmm1, %xmm9


	movaps	-24 * SIZE(BO), %xmm5

	pshufd	$0xaa, %xmm5, %xmm0
	pshufd	$0xff, %xmm5, %xmm1

	pshufd	$0xb1, %xmm11, %xmm10

	xorps	 %xmm7, %xmm10

	mulps	 %xmm0, %xmm11
	mulps	 %xmm1, %xmm10

	addps	 %xmm10, %xmm11

	movaps	%xmm11,  %xmm3
	pshufd	$0xb1, %xmm11, %xmm2

	xorps	 %xmm7, %xmm2

	pshufd	$0x00, %xmm5, %xmm0
	pshufd	$0x55, %xmm5, %xmm1

	mulps	 %xmm3, %xmm0
	mulps	 %xmm2, %xmm1

	subps	 %xmm0, %xmm9
	subps	 %xmm1, %xmm9

	movaps	-32 * SIZE(BO), %xmm5

	pshufd	$0x00, %xmm5, %xmm0
	pshufd	$0x55, %xmm5, %xmm1

	pshufd	$0xb1, %xmm9, %xmm8

	xorps	 %xmm7, %xmm8

	mulps	 %xmm0, %xmm9
	mulps	 %xmm1, %xmm8

	addps	 %xmm8, %xmm9
#endif

#ifdef LN
	subq	$4 * SIZE, CO1
	subq	$4 * SIZE, CO2
#endif

#if defined(LN) || defined(LT)
	movaps	%xmm9,  -32 * SIZE(BO)
	movaps	%xmm13, -28 * SIZE(BO)
	movaps	%xmm11, -24 * SIZE(BO)
	movaps	%xmm15, -20 * SIZE(BO)

	movsd	%xmm9,  0 * SIZE(CO1)
	movsd	%xmm11, 2 * SIZE(CO1)
	movhps	%xmm9,  0 * SIZE(CO1, LDC)
	movhps	%xmm11, 2 * SIZE(CO1, LDC)

	movsd	%xmm13, 0 * SIZE(CO2)
	movsd	%xmm15, 2 * SIZE(CO2)
	movhps	%xmm13, 0 * SIZE(CO2, LDC)
	movhps	%xmm15, 2 * SIZE(CO2, LDC)
#else
	movaps	%xmm9,  -32 * SIZE(AO)
	movaps	%xmm11, -28 * SIZE(AO)
	movaps	%xmm13, -24 * SIZE(AO)
	movaps	%xmm15, -20 * SIZE(AO)

	movsd	%xmm9,  0 * SIZE(CO1)
	movhps	%xmm9,  2 * SIZE(CO1)
	movsd	%xmm11, 0 * SIZE(CO1, LDC)
	movhps	%xmm11, 2 * SIZE(CO1, LDC)
	movsd	%xmm13, 0 * SIZE(CO2)
	movhps	%xmm13, 2 * SIZE(CO2)
	movsd	%xmm15, 0 * SIZE(CO2, LDC)
	movhps	%xmm15, 2 * SIZE(CO2, LDC)
#endif

#ifndef LN
	addq	$4 * SIZE, CO1
	addq	$4 * SIZE, CO2
#endif

#if defined(LT) || defined(RN)
	movq	K,  %rax
	subq	KK, %rax
	salq	$ZBASE_SHIFT, %rax
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
       salq	$1 + ZBASE_SHIFT, %rax
       addq	%rax, AORIG
#endif

	decq	I			# i --
	BRANCH
	jg	.L11
	ALIGN_4

.L29:
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

.L30:
	testq	$2, N
	BRANCH
	jle	.L50

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

	testq	$1, M
	BRANCH
	jle	.L40

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
	movddup  -32 * SIZE(AO), %xmm0
	xorps	%xmm2, %xmm2
	movaps   -32 * SIZE(BO), %xmm5
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
	jle	.L45
	ALIGN_3

.L42:
	ADD1	%xmm1, %xmm8
	pshufd	$0xa0, %xmm5, %xmm1
	mulps	%xmm0, %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0xf5, %xmm5, %xmm2
	movaps   -28 * SIZE(BO), %xmm5
	mulps	%xmm0, %xmm2
	movddup  -30 * SIZE(AO), %xmm0

	ADD1	%xmm1, %xmm8
	pshufd	$0xa0, %xmm5, %xmm1
	mulps	%xmm0, %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0xf5, %xmm5, %xmm2
	movaps   -24 * SIZE(BO), %xmm5
	mulps	%xmm0, %xmm2
	movddup  -28 * SIZE(AO), %xmm0

	ADD1	%xmm1, %xmm8
	pshufd	$0xa0, %xmm5, %xmm1
	mulps	%xmm0, %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0xf5, %xmm5, %xmm2
	movaps   -20 * SIZE(BO), %xmm5
	mulps	%xmm0, %xmm2
	movddup  -26 * SIZE(AO), %xmm0

	ADD1	%xmm1, %xmm8
	pshufd	$0xa0, %xmm5, %xmm1
	mulps	%xmm0, %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0xf5, %xmm5, %xmm2
	movaps   -16 * SIZE(BO), %xmm5
	mulps	%xmm0, %xmm2
	movddup  -24 * SIZE(AO), %xmm0

	subq	$-16 * SIZE, BO
	subq	$ -8 * SIZE, AO

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
	ADD1	%xmm1, %xmm8
	pshufd	$0xa0, %xmm5, %xmm1
	mulps	%xmm0, %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0xf5, %xmm5, %xmm2
	movaps   -28 * SIZE(BO), %xmm5
	mulps	%xmm0, %xmm2
	movddup  -30 * SIZE(AO), %xmm0

	addq	$2 * SIZE, AO
	addq	$4 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L46
	ALIGN_3

.L48:
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

	pcmpeqb	%xmm0, %xmm0
	psllq	$63,   %xmm0

#if defined(LN) || defined(LT)

#ifndef CONJ
	xorps	%xmm0, %xmm9
	shufps	$0xb1, %xmm9,  %xmm9
#else
	xorps	%xmm0, %xmm8
	shufps	$0xb1, %xmm9,  %xmm9
#endif

#else

#ifndef CONJ
	xorps	%xmm0, %xmm9
	shufps	$0xb1, %xmm9,  %xmm9
#else
	shufps	$0xb1, %xmm9,  %xmm9
	xorps	%xmm0, %xmm9
#endif

#endif

	addps	%xmm9,  %xmm8

#if defined(LN) || defined(LT)
	movaps	-32 * SIZE(BO), %xmm9

	subps	%xmm8,   %xmm9
#else
	movaps	-32 * SIZE(AO), %xmm9

	subps	%xmm8, %xmm9
	movhlps %xmm9, %xmm11
#endif

	pcmpeqb	%xmm7, %xmm7
	psllq	$63,   %xmm7

#ifndef CONJ
	shufps	$0xb1, %xmm7,  %xmm7
#endif

#if defined(LN) || defined(LT)
	movsd	-32 * SIZE(AO), %xmm5

	pshufd	$0x00, %xmm5, %xmm0
	pshufd	$0x55, %xmm5, %xmm1

	pshufd	$0xb1, %xmm9, %xmm10

	xorps	 %xmm7, %xmm10

	mulps	 %xmm0, %xmm9
	mulps	 %xmm1, %xmm10

	addps	 %xmm10, %xmm9
#endif

#ifdef RN
	movaps	-32 * SIZE(BO), %xmm5

	pshufd	$0x00, %xmm5, %xmm0
	pshufd	$0x55, %xmm5, %xmm1

	pshufd	$0xb1, %xmm9, %xmm8

	xorps	 %xmm7, %xmm8

	mulps	 %xmm0, %xmm9
	mulps	 %xmm1, %xmm8

	addps	 %xmm8, %xmm9

	movaps	%xmm9,  %xmm3
	pshufd	$0xb1, %xmm9, %xmm2

	xorps	 %xmm7, %xmm2

	pshufd	$0xaa, %xmm5, %xmm0
	pshufd	$0xff, %xmm5, %xmm1

	mulps	 %xmm3, %xmm0
	mulps	 %xmm2, %xmm1

	subps	 %xmm0, %xmm11
	subps	 %xmm1, %xmm11

	movaps	-28 * SIZE(BO), %xmm5

	pshufd	$0xaa, %xmm5, %xmm0
	pshufd	$0xff, %xmm5, %xmm1

	pshufd	$0xb1, %xmm11, %xmm10

	xorps	 %xmm7, %xmm10

	mulps	 %xmm0, %xmm11
	mulps	 %xmm1, %xmm10

	addps	 %xmm10, %xmm11
#endif

#ifdef RT
	movaps	-28 * SIZE(BO), %xmm5

	pshufd	$0xaa, %xmm5, %xmm0
	pshufd	$0xff, %xmm5, %xmm1

	pshufd	$0xb1, %xmm11, %xmm10

	xorps	 %xmm7, %xmm10

	mulps	 %xmm0, %xmm11
	mulps	 %xmm1, %xmm10

	addps	 %xmm10, %xmm11

	movaps	%xmm11,  %xmm3
	pshufd	$0xb1, %xmm11, %xmm2

	xorps	 %xmm7, %xmm2

	pshufd	$0x00, %xmm5, %xmm0
	pshufd	$0x55, %xmm5, %xmm1

	mulps	 %xmm3, %xmm0
	mulps	 %xmm2, %xmm1

	subps	 %xmm0, %xmm9
	subps	 %xmm1, %xmm9

	movaps	-32 * SIZE(BO), %xmm5

	pshufd	$0x00, %xmm5, %xmm0
	pshufd	$0x55, %xmm5, %xmm1

	pshufd	$0xb1, %xmm9, %xmm8

	xorps	 %xmm7, %xmm8

	mulps	 %xmm0, %xmm9
	mulps	 %xmm1, %xmm8

	addps	 %xmm8, %xmm9
#endif

#ifdef LN
	subq	$2 * SIZE, CO1
	subq	$2 * SIZE, CO2
#endif

#if defined(LN) || defined(LT)
	movaps	%xmm9,  -32 * SIZE(BO)

	movlps	%xmm9,  (CO1)
	movhps	%xmm9,  (CO2)
#else
	movlps	%xmm9,  -32 * SIZE(AO)
	movlps	%xmm11, -30 * SIZE(AO)

	movlps	%xmm9,  (CO1)
	movlps	%xmm11, (CO2)
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
	ALIGN_4

.L40:
	movq	M,  I
	sarq	$1, I
	NOBRANCH
	jle	.L49
	ALIGN_4

.L31:
#ifdef LN
       movq	K, %rax
       salq	$1 + ZBASE_SHIFT, %rax
       subq	%rax, AORIG
#endif

#if defined(LN) || defined(RT)
	movq	AORIG, AO

	movq	KK, %rax
	salq	$ZBASE_SHIFT, %rax

	leaq	(AO, %rax, 2), AO
	leaq	(B,  %rax, 2), BO
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
	prefetcht2     4 * SIZE(CO2)
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
	movaps	-32 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0xb1, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	ADD1	%xmm3, %xmm10
	pshufd	$0x1b, %xmm2, %xmm3
	mulps	%xmm0, %xmm2
	ADD2	%xmm4, %xmm11
	pshufd	$0xb1, %xmm3, %xmm4
	mulps	%xmm0, %xmm3
	mulps	%xmm0, %xmm4
	movaps	-28 * SIZE(AO), %xmm0

	ADD1	%xmm1, %xmm8
	movaps	-28 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0xb1, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	ADD1	%xmm3, %xmm10
	pshufd	$0x1b, %xmm2, %xmm3
	mulps	%xmm0, %xmm2
	ADD2	%xmm4, %xmm11
	pshufd	$0xb1, %xmm3, %xmm4
	mulps	%xmm0, %xmm3
	mulps	%xmm0, %xmm4
	movaps	-24 * SIZE(AO), %xmm0

	ADD1	%xmm1, %xmm8
	movaps	-24 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0xb1, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	ADD1	%xmm3, %xmm10
	pshufd	$0x1b, %xmm2, %xmm3
	mulps	%xmm0, %xmm2
	ADD2	%xmm4, %xmm11
	pshufd	$0xb1, %xmm3, %xmm4
	mulps	%xmm0, %xmm3
	mulps	%xmm0, %xmm4
	movaps	-20 * SIZE(AO), %xmm0

	ADD1	%xmm1, %xmm8
	movaps	-20 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0xb1, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	ADD1	%xmm3, %xmm10
	pshufd	$0x1b, %xmm2, %xmm3
	mulps	%xmm0, %xmm2
	ADD2	%xmm4, %xmm11
	pshufd	$0xb1, %xmm3, %xmm4
	mulps	%xmm0, %xmm3
	mulps	%xmm0, %xmm4
	movaps	-16 * SIZE(AO), %xmm0

	subq	$-16 * SIZE, BO
	subq	$-16 * SIZE, AO

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
	ADD1	%xmm1, %xmm8
	movaps	-32 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0xb1, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	ADD1	%xmm3, %xmm10
	pshufd	$0x1b, %xmm2, %xmm3
	mulps	%xmm0, %xmm2
	ADD2	%xmm4, %xmm11
	pshufd	$0xb1, %xmm3, %xmm4
	mulps	%xmm0, %xmm3
	mulps	%xmm0, %xmm4
	movaps	-28 * SIZE(AO), %xmm0

	addq	$4 * SIZE, AO
	addq	$4 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L36
	ALIGN_3

.L38:
#if defined(LN) || defined(RT)
	movq	KK, %rax
#ifdef LN
	subq	$2, %rax
#else
	subq	$2, %rax
#endif

	salq	$ZBASE_SHIFT, %rax

	movq	AORIG, AO
	leaq	(AO, %rax, 2), AO
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
	xorps	%xmm0, %xmm8
	xorps	%xmm0, %xmm10
#else
	xorps	%xmm0, %xmm9
	xorps	%xmm0, %xmm11
#endif

#else

#ifndef CONJ
	xorps	%xmm0, %xmm8
	xorps	%xmm0, %xmm10
#else
	shufps	$0xb1, %xmm0,  %xmm0

	xorps	%xmm0, %xmm9
	xorps	%xmm0, %xmm11
#endif

#endif

	haddps	%xmm9,  %xmm8
	haddps	%xmm11, %xmm10

	shufps	$0xd8, %xmm8,  %xmm8
	shufps	$0xd8, %xmm10, %xmm10

	movaps	%xmm8, %xmm9
	shufps	$0xe4, %xmm10, %xmm8
	shufps	$0xe4, %xmm9, %xmm10

#if defined(LN) || defined(LT)
	movaps	%xmm8,  %xmm9
	movlhps	%xmm10, %xmm8
	movhlps %xmm9,  %xmm10

	movaps	-32 * SIZE(BO), %xmm9
	movaps	-28 * SIZE(BO), %xmm11

	subps	%xmm8,   %xmm9
	subps	%xmm10,  %xmm11
#else
	movaps	-32 * SIZE(AO), %xmm9
	movaps	-28 * SIZE(AO), %xmm11

	subps	%xmm8,   %xmm9
	subps	%xmm10,  %xmm11
#endif

	pcmpeqb	%xmm7, %xmm7
	psllq	$63,   %xmm7

#ifndef CONJ
	shufps	$0xb1, %xmm7,  %xmm7
#endif

#ifdef LN
	movaps	-28 * SIZE(AO), %xmm5

	pshufd	$0xaa, %xmm5, %xmm0
	pshufd	$0xff, %xmm5, %xmm1

	pshufd	$0xb1, %xmm11, %xmm10

	xorps	 %xmm7, %xmm10

	mulps	 %xmm0, %xmm11
	mulps	 %xmm1, %xmm10

	addps	 %xmm10, %xmm11

	pshufd	$0x00, %xmm5, %xmm0
	pshufd	$0x55, %xmm5, %xmm1

	movaps	%xmm11, %xmm3
	pshufd	$0xb1, %xmm11, %xmm2

	xorps	 %xmm7, %xmm2

	mulps	 %xmm0, %xmm3
	mulps	 %xmm1, %xmm2

	subps	 %xmm3, %xmm9
	subps	 %xmm2, %xmm9

	movaps	-32 * SIZE(AO), %xmm5

	pshufd	$0x00, %xmm5, %xmm0
	pshufd	$0x55, %xmm5, %xmm1

	pshufd	$0xb1, %xmm9, %xmm10

	xorps	 %xmm7, %xmm10

	mulps	 %xmm0, %xmm9
	mulps	 %xmm1, %xmm10

	addps	 %xmm10, %xmm9
#endif

#ifdef LT
	movaps	-32 * SIZE(AO), %xmm5

	pshufd	$0x00, %xmm5, %xmm0
	pshufd	$0x55, %xmm5, %xmm1

	pshufd	$0xb1, %xmm9, %xmm10

	xorps	 %xmm7, %xmm10

	mulps	 %xmm0, %xmm9
	mulps	 %xmm1, %xmm10

	addps	 %xmm10, %xmm9

	pshufd	$0xaa, %xmm5, %xmm0
	pshufd	$0xff, %xmm5, %xmm1

	movaps	%xmm9, %xmm3
	pshufd	$0xb1, %xmm9, %xmm2

	xorps	 %xmm7, %xmm2

	mulps	 %xmm0, %xmm3
	mulps	 %xmm1, %xmm2

	subps	 %xmm3, %xmm11
	subps	 %xmm2, %xmm11

	movaps	-28 * SIZE(AO), %xmm5

	pshufd	$0xaa, %xmm5, %xmm0
	pshufd	$0xff, %xmm5, %xmm1

	pshufd	$0xb1, %xmm11, %xmm10

	xorps	 %xmm7, %xmm10

	mulps	 %xmm0, %xmm11
	mulps	 %xmm1, %xmm10

	addps	 %xmm10, %xmm11
#endif

#ifdef RN
	movaps	-32 * SIZE(BO), %xmm5

	pshufd	$0x00, %xmm5, %xmm0
	pshufd	$0x55, %xmm5, %xmm1

	pshufd	$0xb1, %xmm9, %xmm8

	xorps	 %xmm7, %xmm8

	mulps	 %xmm0, %xmm9
	mulps	 %xmm1, %xmm8

	addps	 %xmm8, %xmm9

	movaps	%xmm9,  %xmm3
	pshufd	$0xb1, %xmm9, %xmm2

	xorps	 %xmm7, %xmm2

	pshufd	$0xaa, %xmm5, %xmm0
	pshufd	$0xff, %xmm5, %xmm1

	mulps	 %xmm3, %xmm0
	mulps	 %xmm2, %xmm1

	subps	 %xmm0, %xmm11
	subps	 %xmm1, %xmm11

	movaps	-28 * SIZE(BO), %xmm5

	pshufd	$0xaa, %xmm5, %xmm0
	pshufd	$0xff, %xmm5, %xmm1

	pshufd	$0xb1, %xmm11, %xmm10

	xorps	 %xmm7, %xmm10

	mulps	 %xmm0, %xmm11
	mulps	 %xmm1, %xmm10

	addps	 %xmm10, %xmm11
#endif

#ifdef RT
	movaps	-28 * SIZE(BO), %xmm5

	pshufd	$0xaa, %xmm5, %xmm0
	pshufd	$0xff, %xmm5, %xmm1

	pshufd	$0xb1, %xmm11, %xmm10

	xorps	 %xmm7, %xmm10

	mulps	 %xmm0, %xmm11
	mulps	 %xmm1, %xmm10

	addps	 %xmm10, %xmm11

	movaps	%xmm11,  %xmm3
	pshufd	$0xb1, %xmm11, %xmm2

	xorps	 %xmm7, %xmm2

	pshufd	$0x00, %xmm5, %xmm0
	pshufd	$0x55, %xmm5, %xmm1

	mulps	 %xmm3, %xmm0
	mulps	 %xmm2, %xmm1

	subps	 %xmm0, %xmm9
	subps	 %xmm1, %xmm9

	movaps	-32 * SIZE(BO), %xmm5

	pshufd	$0x00, %xmm5, %xmm0
	pshufd	$0x55, %xmm5, %xmm1

	pshufd	$0xb1, %xmm9, %xmm8

	xorps	 %xmm7, %xmm8

	mulps	 %xmm0, %xmm9
	mulps	 %xmm1, %xmm8

	addps	 %xmm8, %xmm9
#endif

#ifdef LN
	subq	$4 * SIZE, CO1
	subq	$4 * SIZE, CO2
#endif

#if defined(LN) || defined(LT)
	movaps	%xmm9,  -32 * SIZE(BO)
	movaps	%xmm11, -28 * SIZE(BO)

	movsd	%xmm9,  0 * SIZE(CO1)
	movsd	%xmm11, 2 * SIZE(CO1)
	movhps	%xmm9,  0 * SIZE(CO2)
	movhps	%xmm11, 2 * SIZE(CO2)
#else
	movaps	%xmm9,  -32 * SIZE(AO)
	movaps	%xmm11, -28 * SIZE(AO)

	movsd	%xmm9,  0 * SIZE(CO1)
	movhps	%xmm9,  2 * SIZE(CO1)
	movsd	%xmm11, 0 * SIZE(CO2)
	movhps	%xmm11, 2 * SIZE(CO2)
#endif

#ifndef LN
	addq	$4 * SIZE, CO1
	addq	$4 * SIZE, CO2
#endif

#if defined(LT) || defined(RN)
	movq	K,  %rax
	subq	KK, %rax
	salq	$ZBASE_SHIFT, %rax
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
       salq	$1 + ZBASE_SHIFT, %rax
       addq	%rax, AORIG
#endif

	decq	I			# i --
	BRANCH
	jg	.L31
	ALIGN_4

.L49:
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

.L50:
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

	testq	$1, M
	BRANCH
	jle	.L60

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
	movddup  -32 * SIZE(AO), %xmm0
	xorps	%xmm2, %xmm2
	movsd   -32 * SIZE(BO), %xmm5
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
	jle	.L65
	ALIGN_3

.L62:
	ADD1	%xmm1, %xmm8
	pshufd	$0xa0, %xmm5, %xmm1
	mulps	%xmm0, %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0xf5, %xmm5, %xmm2
	movsd   -30 * SIZE(BO), %xmm5
	mulps	%xmm0, %xmm2
	movddup -30 * SIZE(AO), %xmm0

	ADD1	%xmm1, %xmm8
	pshufd	$0xa0, %xmm5, %xmm1
	mulps	%xmm0, %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0xf5, %xmm5, %xmm2
	movsd   -28 * SIZE(BO), %xmm5
	mulps	%xmm0, %xmm2
	movddup -28 * SIZE(AO), %xmm0

	ADD1	%xmm1, %xmm8
	pshufd	$0xa0, %xmm5, %xmm1
	mulps	%xmm0, %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0xf5, %xmm5, %xmm2
	movsd   -26 * SIZE(BO), %xmm5
	mulps	%xmm0, %xmm2
	movddup -26 * SIZE(AO), %xmm0

	ADD1	%xmm1, %xmm8
	pshufd	$0xa0, %xmm5, %xmm1
	mulps	%xmm0, %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0xf5, %xmm5, %xmm2
	movsd   -24 * SIZE(BO), %xmm5
	mulps	%xmm0, %xmm2
	movddup -24 * SIZE(AO), %xmm0

	subq	$-8 * SIZE, BO
	subq	$-8 * SIZE, AO

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
	ADD1	%xmm1, %xmm8
	pshufd	$0xa0, %xmm5, %xmm1
	mulps	%xmm0, %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0xf5, %xmm5, %xmm2
	movsd   -30 * SIZE(BO), %xmm5
	mulps	%xmm0, %xmm2
	movddup -30 * SIZE(AO), %xmm0

	addq	$2 * SIZE, AO
	addq	$2 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L66
	ALIGN_3

.L68:
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
	xorps	%xmm0, %xmm9
	shufps	$0xb1, %xmm9,  %xmm9
#else
	xorps	%xmm0, %xmm8
	shufps	$0xb1, %xmm9,  %xmm9
#endif

#else

#ifndef CONJ
	xorps	%xmm0, %xmm9
	shufps	$0xb1, %xmm9,  %xmm9
#else
	shufps	$0xb1, %xmm9,  %xmm9
	xorps	%xmm0, %xmm9
#endif

#endif

	addps	%xmm9,  %xmm8

#if defined(LN) || defined(LT)
	movsd	-32 * SIZE(BO), %xmm9
#else
	movsd	-32 * SIZE(AO), %xmm9
#endif

	subps	%xmm8, %xmm9

	pcmpeqb	%xmm7, %xmm7
	psllq	$63,   %xmm7

#ifndef CONJ
	shufps	$0xb1, %xmm7,  %xmm7
#endif

#if defined(LN) || defined(LT)
	movsd	-32 * SIZE(AO), %xmm5
#endif

#if defined(RN) || defined(RT)
	movsd	-32 * SIZE(BO), %xmm5
#endif

	pshufd	$0x00, %xmm5, %xmm0
	pshufd	$0x55, %xmm5, %xmm1

	pshufd	$0xb1, %xmm9, %xmm10

	xorps	 %xmm7, %xmm10

	mulps	 %xmm0, %xmm9
	mulps	 %xmm1, %xmm10

	addps	 %xmm10, %xmm9

#ifdef LN
	subq	$2 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	movlps	%xmm9,  -32 * SIZE(BO)
#else
	movlps	%xmm9,  -32 * SIZE(AO)
#endif

	movlps	%xmm9,  (CO1)

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
	ALIGN_4

.L60:
	movq	M,  I
	sarq	$1, I
	NOBRANCH
	jle	.L69
	ALIGN_4

.L51:
#ifdef LN
       movq	K, %rax
       salq	$1 + ZBASE_SHIFT, %rax
       subq	%rax, AORIG
#endif

#if defined(LN) || defined(RT)
	movq	AORIG, AO

	movq	KK, %rax
	salq	$ZBASE_SHIFT, %rax

	leaq	(AO, %rax, 2), AO
	leaq	(B,  %rax, 1), BO
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

	ADD1	%xmm1, %xmm8
	movddup	-32 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0xb1, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	mulps	%xmm0, %xmm2
	movaps	-28 * SIZE(AO), %xmm0

	ADD1	%xmm1, %xmm8
	movddup	-30 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0xb1, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	mulps	%xmm0, %xmm2
	movaps	-24 * SIZE(AO), %xmm0

	ADD1	%xmm1, %xmm8
	movddup	-28 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0xb1, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	mulps	%xmm0, %xmm2
	movaps	-20 * SIZE(AO), %xmm0

	ADD1	%xmm1, %xmm8
	movddup	-26 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0xb1, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	mulps	%xmm0, %xmm2
	movaps	-16 * SIZE(AO), %xmm0

	subq	$ -8 * SIZE, BO
	subq	$-16 * SIZE, AO

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
	ADD1	%xmm1, %xmm8
	movddup	-32 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0xb1, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	mulps	%xmm0, %xmm2
	movaps	-28 * SIZE(AO), %xmm0

	addq	$4 * SIZE, AO
	addq	$2 * SIZE, BO

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
	subq	$1, %rax
#endif

	salq	$ZBASE_SHIFT, %rax

	movq	AORIG, AO
	leaq	(AO, %rax, 2), AO
	leaq	(B,  %rax, 1), BO
#endif

	ADD1	%xmm1, %xmm8
	ADD2	%xmm2, %xmm9

	pcmpeqb	%xmm0, %xmm0
	psllq	$63,   %xmm0

#if defined(LN) || defined(LT)

#ifndef CONJ
	xorps	%xmm0, %xmm8
#else
	xorps	%xmm0, %xmm9
#endif

#else

#ifndef CONJ
	xorps	%xmm0, %xmm8
#else
	shufps	$0xb1, %xmm0,  %xmm0

	xorps	%xmm0, %xmm9
#endif

#endif

	haddps	%xmm9,  %xmm8

	shufps	$0xd8, %xmm8,  %xmm8

#if defined(LN) || defined(LT)
	movaps	-32 * SIZE(BO), %xmm9

	subps	%xmm8, %xmm9
	movhlps %xmm9, %xmm11
#else
	movaps	-32 * SIZE(AO), %xmm9

	subps	%xmm8, %xmm9
#endif

	pcmpeqb	%xmm7, %xmm7
	psllq	$63,   %xmm7

#ifndef CONJ
	shufps	$0xb1, %xmm7,  %xmm7
#endif

#ifdef LN
	movaps	-28 * SIZE(AO), %xmm5

	pshufd	$0xaa, %xmm5, %xmm0
	pshufd	$0xff, %xmm5, %xmm1

	pshufd	$0xb1, %xmm11, %xmm10

	xorps	 %xmm7, %xmm10

	mulps	 %xmm0, %xmm11
	mulps	 %xmm1, %xmm10

	addps	 %xmm10, %xmm11

	pshufd	$0x00, %xmm5, %xmm0
	pshufd	$0x55, %xmm5, %xmm1

	movaps	%xmm11, %xmm3
	pshufd	$0xb1, %xmm11, %xmm2

	xorps	 %xmm7, %xmm2

	mulps	 %xmm0, %xmm3
	mulps	 %xmm1, %xmm2

	subps	 %xmm3, %xmm9
	subps	 %xmm2, %xmm9

	movaps	-32 * SIZE(AO), %xmm5

	pshufd	$0x00, %xmm5, %xmm0
	pshufd	$0x55, %xmm5, %xmm1

	pshufd	$0xb1, %xmm9, %xmm10

	xorps	 %xmm7, %xmm10

	mulps	 %xmm0, %xmm9
	mulps	 %xmm1, %xmm10

	addps	 %xmm10, %xmm9
#endif

#ifdef LT
	movaps	-32 * SIZE(AO), %xmm5

	pshufd	$0x00, %xmm5, %xmm0
	pshufd	$0x55, %xmm5, %xmm1

	pshufd	$0xb1, %xmm9, %xmm10

	xorps	 %xmm7, %xmm10

	mulps	 %xmm0, %xmm9
	mulps	 %xmm1, %xmm10

	addps	 %xmm10, %xmm9

	pshufd	$0xaa, %xmm5, %xmm0
	pshufd	$0xff, %xmm5, %xmm1

	movaps	%xmm9, %xmm3
	pshufd	$0xb1, %xmm9, %xmm2

	xorps	 %xmm7, %xmm2

	mulps	 %xmm0, %xmm3
	mulps	 %xmm1, %xmm2

	subps	 %xmm3, %xmm11
	subps	 %xmm2, %xmm11

	movaps	-28 * SIZE(AO), %xmm5

	pshufd	$0xaa, %xmm5, %xmm0
	pshufd	$0xff, %xmm5, %xmm1

	pshufd	$0xb1, %xmm11, %xmm10

	xorps	 %xmm7, %xmm10

	mulps	 %xmm0, %xmm11
	mulps	 %xmm1, %xmm10

	addps	 %xmm10, %xmm11
#endif

#if defined(RN) || defined(RT)
	movaps	-32 * SIZE(BO), %xmm5

	pshufd	$0x00, %xmm5, %xmm0
	pshufd	$0x55, %xmm5, %xmm1

	pshufd	$0xb1, %xmm9, %xmm8

	xorps	 %xmm7, %xmm8

	mulps	 %xmm0, %xmm9
	mulps	 %xmm1, %xmm8

	addps	 %xmm8, %xmm9
#endif

#ifdef LN
	subq	$4 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	movlps	%xmm9,  -32 * SIZE(BO)
	movlps	%xmm11, -30 * SIZE(BO)

	movlps	%xmm9,  0 * SIZE(CO1)
	movlps	%xmm11, 2 * SIZE(CO1)
#else
	movaps	%xmm9,  -32 * SIZE(AO)

	movlps	%xmm9,  0 * SIZE(CO1)
	movhps	%xmm9,  2 * SIZE(CO1)
#endif

#ifndef LN
	addq	$4 * SIZE, CO1
#endif

#if defined(LT) || defined(RN)
	movq	K,  %rax
	subq	KK, %rax
	salq	$ZBASE_SHIFT, %rax
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
       salq	$1 + ZBASE_SHIFT, %rax
       addq	%rax, AORIG
#endif

	decq	I			# i --
	BRANCH
	jg	.L51
	ALIGN_4

.L69:
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
