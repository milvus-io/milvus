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

#define OLD_A		48 + STACKSIZE(%rsp)
#define OLD_B		56 + STACKSIZE(%rsp)
#define OLD_C		64 + STACKSIZE(%rsp)
#define OLD_LDC		72 + STACKSIZE(%rsp)
#define OLD_OFFSET	80 + STACKSIZE(%rsp)

#endif

#define POSINV	  0(%rsp)
#define J	 16(%rsp)
#define OFFSET	 24(%rsp)
#define KK	 32(%rsp)
#define KKK	 40(%rsp)
#define AORIG    48(%rsp)
#define BORIG	 56(%rsp)
#define BUFFER	128(%rsp)

#define PREFETCH_R    (8 * 4 + 0)
#define PREFETCH_W    (PREFETCH_R)

#define PREFETCHSIZE  (8 * 17 + 2)
#define PREFETCH     prefetcht0

#ifndef CONJ
#define NN
#else
#if defined(LN) || defined(LT)
#define CN
#else
#define NC
#endif
#endif

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

	pcmpeqb	%xmm15, %xmm15
	psllq	$63, %xmm15	# Generate mask
	pxor	%xmm2, %xmm2

	movlpd	  %xmm2,  0 + POSINV
	movlpd	  %xmm15, 8 + POSINV

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
       movq	N, %rax
       subq	OFFSET, %rax
       movq	%rax, KK
#endif

	movq	N,  J
	sarq	$1, J		# j = (n >> 2)
	jle	.L100
	ALIGN_4

.L01:
#ifdef LN
	movq	OFFSET, %rax
	addq	M, %rax
	movq	%rax, KK
#endif

	leaq	16 * SIZE +  BUFFER, BO

#ifdef RT
       movq	K, %rax
       salq	$1 + ZBASE_SHIFT, %rax
       subq	%rax, B
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	movq	B, BORIG
	salq	$ZBASE_SHIFT, %rax
	leaq	(B,  %rax, 2), B
	leaq	(BO, %rax, 4), BO
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
	prefetcht0	(PREFETCH_R + 0) * SIZE(B)

	movddup	 -16 * SIZE(B), %xmm8
	movddup	 -15 * SIZE(B), %xmm9
	movddup	 -14 * SIZE(B), %xmm10
	movddup	 -13 * SIZE(B), %xmm11
	movddup	 -12 * SIZE(B), %xmm12
	movddup	 -11 * SIZE(B), %xmm13
	movddup	 -10 * SIZE(B), %xmm14
	movddup	  -9 * SIZE(B), %xmm15

	prefetcht0	(PREFETCH_W + 0) * SIZE(BO)

	movapd	%xmm8,  -16 * SIZE(BO)
	movapd	%xmm9,  -14 * SIZE(BO)
	movapd	%xmm10,  -12 * SIZE(BO)
	movapd	%xmm11,  -10 * SIZE(BO)

	prefetcht0	(PREFETCH_W + 8) * SIZE(BO)

	movapd	%xmm12,   -8 * SIZE(BO)
	movapd	%xmm13,   -6 * SIZE(BO)
	movapd	%xmm14,   -4 * SIZE(BO)
	movapd	%xmm15,   -2 * SIZE(BO)

	addq	$  8 * SIZE, B
	subq	$-16 * SIZE, BO
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
	jle	.L05
	ALIGN_4

.L04:
	movddup	 -16 * SIZE(B), %xmm8
	movddup	 -15 * SIZE(B), %xmm9
	movddup	 -14 * SIZE(B), %xmm10
	movddup	 -13 * SIZE(B), %xmm11

	movapd	%xmm8,  -16 * SIZE(BO)
	movapd	%xmm9,  -14 * SIZE(BO)
	movapd	%xmm10,  -12 * SIZE(BO)
	movapd	%xmm11,  -10 * SIZE(BO)

	addq	$ 4 * SIZE, B
	addq	$ 8 * SIZE, BO

	decq	%rax
	jne	.L04
	ALIGN_4

.L05:
#if defined(LT) || defined(RN)
	movq	A, AO
#else
	movq	A, AORIG
#endif

#ifdef RT
       leaq	(, LDC, 2), %rax
       subq	%rax, C
#endif

	movq	C, CO1
	leaq	(C, LDC, 1), CO2

#ifndef RT
	leaq	(C, LDC, 2), C
#endif

	testq	$1, M
	jle	.L30

#ifdef LN
       movq	K, %rax
       salq	$0 + ZBASE_SHIFT, %rax
       subq	%rax, AORIG
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	movq	AORIG, AO
	salq	$ZBASE_SHIFT, %rax
	addq	%rax, AO
#endif

	leaq	16 * SIZE + BUFFER, BO

#if defined(LN) || defined(RT)
	movq	KK, %rax
	salq	$1 + ZBASE_SHIFT, %rax
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
	je	.L42

.L41:
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

	ADD1	%xmm2, %xmm8
	ADD2	%xmm3, %xmm9
	ADD1	%xmm4, %xmm10
	ADD2	%xmm5, %xmm11

	movapd	-14 * SIZE(AO), %xmm0
	movapd	 -8 * SIZE(BO), %xmm2
	movapd	 -6 * SIZE(BO), %xmm3
	movapd	 -4 * SIZE(BO), %xmm4
	movapd	 -2 * SIZE(BO), %xmm5

	mulpd	%xmm0, %xmm2
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm4
	mulpd	%xmm0, %xmm5

	ADD1	%xmm2, %xmm8
	ADD2	%xmm3, %xmm9
	ADD1	%xmm4, %xmm10
	ADD2	%xmm5, %xmm11

	movapd	-12 * SIZE(AO), %xmm0
	movapd	  0 * SIZE(BO), %xmm2
	movapd	  2 * SIZE(BO), %xmm3
	movapd	  4 * SIZE(BO), %xmm4
	movapd	  6 * SIZE(BO), %xmm5

	mulpd	%xmm0, %xmm2
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm4
	mulpd	%xmm0, %xmm5

	ADD1	%xmm2, %xmm8
	ADD2	%xmm3, %xmm9
	ADD1	%xmm4, %xmm10
	ADD2	%xmm5, %xmm11

	movapd	-10 * SIZE(AO), %xmm0
	movapd	  8 * SIZE(BO), %xmm2
	movapd	 10 * SIZE(BO), %xmm3
	movapd	 12 * SIZE(BO), %xmm4
	movapd	 14 * SIZE(BO), %xmm5

	mulpd	%xmm0, %xmm2
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm4
	mulpd	%xmm0, %xmm5

	ADD1	%xmm2, %xmm8
	ADD2	%xmm3, %xmm9
	ADD1	%xmm4, %xmm10
	ADD2	%xmm5, %xmm11

	subq	$ -8 * SIZE, AO
	subq	$-32 * SIZE, BO
	subq	$1, %rax
	jne    .L41

.L42:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	movapd	POSINV,  %xmm7

	andq	$3, %rax		# if (k & 1)
	BRANCH
	jle .L44

.L43:
	movapd	-16 * SIZE(AO), %xmm0
	movapd	-16 * SIZE(BO), %xmm2
	movapd	-14 * SIZE(BO), %xmm3
	movapd	-12 * SIZE(BO), %xmm4
	movapd	-10 * SIZE(BO), %xmm5

	mulpd	%xmm0, %xmm2
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm4
	mulpd	%xmm0, %xmm5

	ADD1	%xmm2, %xmm8
	ADD2	%xmm3, %xmm9
	ADD1	%xmm4, %xmm10
	ADD2	%xmm5, %xmm11

	addq	$2 * SIZE, AO
	addq	$8 * SIZE, BO
	subq	$1, %rax
	jg	.L43
	ALIGN_4

.L44:
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

	salq	$ZBASE_SHIFT, %rax
	leaq	(AO, %rax, 1), AO
	leaq	(B,  %rax, 2), B
	leaq	(BO, %rax, 4), BO
#endif

	SHUFPD_1 %xmm9, %xmm9
	SHUFPD_1 %xmm11, %xmm11

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)
	xorpd	%xmm7, %xmm9
	xorpd	%xmm7, %xmm11
#else
	xorpd	%xmm7, %xmm8
	xorpd	%xmm7, %xmm10
#endif

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(RR) || defined(RC) || defined(CR) || defined(CC)
	subpd	%xmm9, %xmm8
	subpd	%xmm11, %xmm10
#else
	addpd	%xmm9, %xmm8
	addpd	%xmm11, %xmm10
#endif

#if defined(LN) || defined(LT)
	movapd	-16 * SIZE(B), %xmm9
	movapd	-14 * SIZE(B), %xmm11

	subpd	%xmm8,  %xmm9
	subpd	%xmm10,  %xmm11
#else
	movapd	-16 * SIZE(AO), %xmm9
	movapd	-14 * SIZE(AO), %xmm11

	subpd	%xmm8,  %xmm9
	subpd	%xmm10,  %xmm11
#endif

#ifndef CONJ
	SHUFPD_1 %xmm7, %xmm7
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
	movddup	-16 * SIZE(B), %xmm0
	movddup	-15 * SIZE(B), %xmm1
	movddup	-14 * SIZE(B), %xmm2
	movddup	-13 * SIZE(B), %xmm3
	movddup	-10 * SIZE(B), %xmm4
	movddup	 -9 * SIZE(B), %xmm5

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
	movddup	-10 * SIZE(B), %xmm0
	movddup	 -9 * SIZE(B), %xmm1
	movddup	-12 * SIZE(B), %xmm2
	movddup	-11 * SIZE(B), %xmm3
	movddup	-16 * SIZE(B), %xmm4
	movddup	-15 * SIZE(B), %xmm5

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
	movapd	%xmm9,  -16 * SIZE(B)
	movapd	%xmm11, -14 * SIZE(B)

	movddup	%xmm9,  %xmm8
	unpckhpd %xmm9,  %xmm9
	movddup	%xmm11, %xmm10
	unpckhpd %xmm11, %xmm11

	movapd	%xmm8,  -16 * SIZE(BO)
	movapd	%xmm9,  -14 * SIZE(BO)
	movapd	%xmm10, -12 * SIZE(BO)
	movapd	%xmm11, -10 * SIZE(BO)
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
	salq	$0 + ZBASE_SHIFT, %rax
	addq	%rax, AORIG
#endif
	ALIGN_4

.L30:
	movq	M,  I
	sarq	$1, I		# i = (m >> 2)
	jle	.L99
	ALIGN_4

.L10:
	leaq	(PREFETCH_R +  0) * SIZE(B), BB

#ifdef LN
       movq	K, %rax
       salq	$1 + ZBASE_SHIFT, %rax
       subq	%rax, AORIG
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	movq	AORIG, AO
	salq	$ZBASE_SHIFT, %rax
	leaq	(AO, %rax, 2), AO
#endif

	leaq	16 * SIZE + BUFFER, BO

#if defined(LN) || defined(RT)
	movq	KK, %rax
	salq	$1 + ZBASE_SHIFT, %rax
	leaq	(BO, %rax, 2), BO
#endif

	prefetcht2	0 * SIZE(BB)

#ifdef LN
	pxor	%xmm8, %xmm8
	prefetcht1    -3 * SIZE(CO1)
	pxor	%xmm9, %xmm9
	pxor	%xmm10, %xmm10
	prefetcht1    -3 * SIZE(CO2)
	pxor	%xmm11, %xmm11
#else
	pxor	%xmm8, %xmm8
	prefetcht1     3 * SIZE(CO1)
	pxor	%xmm9, %xmm9
	pxor	%xmm10, %xmm10
	prefetcht1     3 * SIZE(CO2)
	pxor	%xmm11, %xmm11
#endif

	pxor	%xmm12, %xmm12
	pxor	%xmm13, %xmm13
	pxor	%xmm14, %xmm14
	pxor	%xmm15, %xmm15

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
	NOBRANCH
	jle	.L15
	ALIGN_4

.L12:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)

	movapd	-16 * SIZE(AO), %xmm0
	ADD1	%xmm2, %xmm10
	movapd	-16 * SIZE(BO), %xmm2
	ADD1	%xmm3, %xmm14
	movapd	 %xmm2, %xmm3
	movapd	-14 * SIZE(AO), %xmm1
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm3
	ADD2	%xmm4, %xmm11
	movapd	-14 * SIZE(BO), %xmm4
	ADD2	%xmm5, %xmm15
	movapd	 %xmm4, %xmm5
	mulpd	%xmm0, %xmm4
	mulpd	%xmm1, %xmm5

	ADD1	%xmm2, %xmm8
	movapd	-12 * SIZE(BO), %xmm2
	ADD1	%xmm3, %xmm12
	movapd	 %xmm2, %xmm3
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm3
	ADD2	%xmm4, %xmm9
	movapd	-10 * SIZE(BO), %xmm4
	ADD2	%xmm5, %xmm13
	movapd	 %xmm4, %xmm5
	mulpd	%xmm0, %xmm4
	mulpd	%xmm1, %xmm5

	movapd	-12 * SIZE(AO), %xmm0
	ADD1	%xmm2, %xmm10
	movapd	 -8 * SIZE(BO), %xmm2
	ADD1	%xmm3, %xmm14
	movapd	 %xmm2, %xmm3
	movapd	-10 * SIZE(AO), %xmm1
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm3
	ADD2	%xmm4, %xmm11
	ADD2	%xmm5, %xmm15
	movapd	-6 * SIZE(BO), %xmm4
	movapd	 %xmm4, %xmm5
	mulpd	%xmm0, %xmm4
	mulpd	%xmm1, %xmm5

	ADD1	%xmm2, %xmm8
	ADD1	%xmm3, %xmm12
	movapd	-4 * SIZE(BO), %xmm2
	movapd	 %xmm2, %xmm3
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm3
	ADD2	%xmm4, %xmm9
	ADD2	%xmm5, %xmm13
	movapd	-2 * SIZE(BO), %xmm4
	movapd	 %xmm4, %xmm5
	PREFETCH (PREFETCHSIZE +  8) * SIZE(AO)
	mulpd	%xmm0, %xmm4
	mulpd	%xmm1, %xmm5

	movapd	-8 * SIZE(AO), %xmm0
	ADD1	%xmm2, %xmm10
	movapd	 0 * SIZE(BO), %xmm2
	ADD1	%xmm3, %xmm14
	movapd	 %xmm2, %xmm3
	movapd	-6 * SIZE(AO), %xmm1
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm3
	ADD2	%xmm4, %xmm11
	movapd	 2 * SIZE(BO), %xmm4
	ADD2	%xmm5, %xmm15
	movapd	 %xmm4, %xmm5
	mulpd	%xmm0, %xmm4
	mulpd	%xmm1, %xmm5

	ADD1	%xmm2, %xmm8
	movapd	 4 * SIZE(BO), %xmm2
	ADD1	%xmm3, %xmm12
	movapd	 %xmm2, %xmm3
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm3
	ADD2	%xmm4, %xmm9
	movapd	 6 * SIZE(BO), %xmm4
	ADD2	%xmm5, %xmm13
	movapd	 %xmm4, %xmm5
	mulpd	%xmm0, %xmm4
	mulpd	%xmm1, %xmm5

	movapd	-4 * SIZE(AO), %xmm0
	ADD1	%xmm2, %xmm10
	ADD1	%xmm3, %xmm14
	movapd	 8 * SIZE(BO), %xmm2
	movapd	 %xmm2, %xmm3
	mulpd	%xmm0, %xmm2
	movapd	-2 * SIZE(AO), %xmm1
	mulpd	%xmm1, %xmm3
	ADD2	%xmm4, %xmm11
	movapd	10 * SIZE(BO), %xmm4
	ADD2	%xmm5, %xmm15
	subq	$-32 * SIZE, BO
	movapd	 %xmm4, %xmm5
	mulpd	%xmm0, %xmm4
	mulpd	%xmm1, %xmm5

	ADD1	%xmm2, %xmm8
	ADD1	%xmm3, %xmm12
	movapd	-20 * SIZE(BO), %xmm2
	movapd	 %xmm2, %xmm3
	mulpd	%xmm0, %xmm2
	subq	$-16 * SIZE, AO
	mulpd	%xmm1, %xmm3
	ADD2	%xmm4, %xmm9
	ADD2	%xmm5, %xmm13
	movapd	-18 * SIZE(BO), %xmm4
	movapd	 %xmm4, %xmm5
	mulpd	%xmm0, %xmm4
	mulpd	%xmm1, %xmm5

	subq	$1, %rax
	BRANCH
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
	movapd	POSINV,  %xmm7

	andq	$3, %rax
	BRANCH
	BRANCH
	je	.L19
	ALIGN_4

.L16:
	ADD1	%xmm2, %xmm10
	ADD1	%xmm3, %xmm14
	ADD2	%xmm4, %xmm11
	ADD2	%xmm5, %xmm15

	movapd	-16 * SIZE(BO), %xmm2
	movapd	 %xmm2, %xmm3
	movapd	-14 * SIZE(BO), %xmm4
	movapd	 %xmm4, %xmm5

	movapd	-16 * SIZE(AO), %xmm0
	mulpd	%xmm0, %xmm2
	movapd	-14 * SIZE(AO), %xmm1
	mulpd	%xmm1, %xmm3
	mulpd	%xmm0, %xmm4
	mulpd	%xmm1, %xmm5

	ADD1	%xmm2, %xmm8
	ADD1	%xmm3, %xmm12
	ADD2	%xmm4, %xmm9
	ADD2	%xmm5, %xmm13

	movapd	-12 * SIZE(BO), %xmm2
	movapd	 %xmm2, %xmm3
	movapd	-10 * SIZE(BO), %xmm4
	movapd	 %xmm4, %xmm5

	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm3
	mulpd	%xmm0, %xmm4
	mulpd	%xmm1, %xmm5

	addq	$4 * SIZE, AO
	addq	$8 * SIZE, BO
	subq	$1, %rax
	BRANCH
	jg	.L16
	ALIGN_4

.L19:
	ADD1	%xmm2, %xmm10
	ADD1	%xmm3, %xmm14
	ADD2	%xmm4, %xmm11
	ADD2	%xmm5, %xmm15

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

	salq	$ZBASE_SHIFT, %rax
	leaq	(AO, %rax, 2), AO
	leaq	(B,  %rax, 2), B
	leaq	(BO, %rax, 4), BO
#endif

	SHUFPD_1 %xmm9,  %xmm9
	SHUFPD_1 %xmm11, %xmm11
	SHUFPD_1 %xmm13, %xmm13
	SHUFPD_1 %xmm15, %xmm15

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)
	xorpd	%xmm7, %xmm9
	xorpd	%xmm7, %xmm11
	xorpd	%xmm7, %xmm13
	xorpd	%xmm7, %xmm15
#else
	xorpd	%xmm7, %xmm8
	xorpd	%xmm7, %xmm10
	xorpd	%xmm7, %xmm12
	xorpd	%xmm7, %xmm14
#endif

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(RR) || defined(RC) || defined(CR) || defined(CC)
	subpd	%xmm9,  %xmm8
	subpd	%xmm11, %xmm10
	subpd	%xmm13, %xmm12
	subpd	%xmm15, %xmm14
#else
	addpd	%xmm9,  %xmm8
	addpd	%xmm11, %xmm10
	addpd	%xmm13, %xmm12
	addpd	%xmm15, %xmm14
#endif

#if defined(LN) || defined(LT)
	movapd	-16 * SIZE(B), %xmm9
	movapd	-14 * SIZE(B), %xmm11
	movapd	-12 * SIZE(B), %xmm13
	movapd	-10 * SIZE(B), %xmm15

	subpd	%xmm8,   %xmm9
	subpd	%xmm10,  %xmm11
	subpd	%xmm12,  %xmm13
	subpd	%xmm14,  %xmm15
#else
	movapd	-16 * SIZE(AO), %xmm9
	movapd	-14 * SIZE(AO), %xmm13
	movapd	-12 * SIZE(AO), %xmm11
	movapd	-10 * SIZE(AO), %xmm15

	subpd	%xmm8,   %xmm9
	subpd	%xmm10,  %xmm11
	subpd	%xmm12,  %xmm13
	subpd	%xmm14,  %xmm15
#endif

#ifndef CONJ
	SHUFPD_1 %xmm7, %xmm7
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
	movddup	-16 * SIZE(B), %xmm0
	movddup	-15 * SIZE(B), %xmm1
	movddup	-14 * SIZE(B), %xmm2
	movddup	-13 * SIZE(B), %xmm3
	movddup	-10 * SIZE(B), %xmm4
	movddup	 -9 * SIZE(B), %xmm5

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
	movddup	-10 * SIZE(B), %xmm0
	movddup	 -9 * SIZE(B), %xmm1
	movddup	-12 * SIZE(B), %xmm2
	movddup	-11 * SIZE(B), %xmm3
	movddup	-16 * SIZE(B), %xmm4
	movddup	-15 * SIZE(B), %xmm5

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
	movapd	%xmm9,  -16 * SIZE(B)
	movapd	%xmm11, -14 * SIZE(B)
	movapd	%xmm13, -12 * SIZE(B)
	movapd	%xmm15, -10 * SIZE(B)

	movddup	%xmm9,  %xmm8
	unpckhpd %xmm9,  %xmm9
	movddup	%xmm11, %xmm10
	unpckhpd %xmm11, %xmm11
	movddup	%xmm13, %xmm12
	unpckhpd %xmm13, %xmm13
	movddup	%xmm15, %xmm14
	unpckhpd %xmm15, %xmm15

	movapd	%xmm8,  -16 * SIZE(BO)
	movapd	%xmm9,  -14 * SIZE(BO)
	movapd	%xmm10, -12 * SIZE(BO)
	movapd	%xmm11, -10 * SIZE(BO)
	movapd	%xmm12,  -8 * SIZE(BO)
	movapd	%xmm13,  -6 * SIZE(BO)
	movapd	%xmm14,  -4 * SIZE(BO)
	movapd	%xmm15,  -2 * SIZE(BO)
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
	salq	$ZBASE_SHIFT, %rax
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
	salq	$1 + ZBASE_SHIFT, %rax
	addq	%rax, AORIG
#endif

	decq	I			# i --
	jg	.L10
	ALIGN_4

.L99:
#ifdef LN
       leaq	(, K, SIZE), %rax
       leaq	(B, %rax, 4), B
#endif

#if defined(LT) || defined(RN)
	movq	K,  %rax
	subq	KK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(B,  %rax, 2 * COMPSIZE), B
#endif

#ifdef RN
	addq	$2, KK
#endif

#ifdef RT
	subq	$2, KK
#endif

	decq	J			# j --
	jg	.L01

.L100:
	testq	$1, N
	jle	.L999

.L101:
#ifdef LN
	movq	OFFSET, %rax
	addq	M, %rax
	movq	%rax, KK
#endif

	leaq	BUFFER, BO

#ifdef RT
       movq	K, %rax
       salq	$0 + ZBASE_SHIFT, %rax
       subq	%rax, B
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	movq	B, BORIG
	salq	$ZBASE_SHIFT, %rax
	leaq	(B,  %rax, 1), B
	leaq	(BO, %rax, 2), BO
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
	jle	.L103
	ALIGN_4

.L102:
	movddup	 -16 * SIZE(B), %xmm8
	movddup	 -15 * SIZE(B), %xmm9
	movddup	 -14 * SIZE(B), %xmm10
	movddup	 -13 * SIZE(B), %xmm11
	movddup	 -12 * SIZE(B), %xmm12
	movddup	 -11 * SIZE(B), %xmm13
	movddup	 -10 * SIZE(B), %xmm14
	movddup	  -9 * SIZE(B), %xmm15

	movapd	%xmm8,  0 * SIZE(BO)
	movapd	%xmm9,  2 * SIZE(BO)
	movapd	%xmm10,  4 * SIZE(BO)
	movapd	%xmm11,  6 * SIZE(BO)
	movapd	%xmm12,  8 * SIZE(BO)
	movapd	%xmm13, 10 * SIZE(BO)
	movapd	%xmm14, 12 * SIZE(BO)
	movapd	%xmm15, 14 * SIZE(BO)

	addq	$  8 * SIZE, B
	subq	$-16 * SIZE, BO
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
	andq	$3, %rax
	BRANCH
	jle	.L105
	ALIGN_4

.L104:
	movddup	 -16 * SIZE(B), %xmm8
	movddup	 -15 * SIZE(B), %xmm9

	movapd	%xmm8,  0 * SIZE(BO)
	movapd	%xmm9,  2 * SIZE(BO)

	addq	$4 * SIZE, BO
	addq	$2 * SIZE, B
	decq	%rax
	jne	.L104
	ALIGN_4

.L105:
#if defined(LT) || defined(RN)
	movq	A, AO
#else
	movq	A, AORIG
#endif

#ifdef RT
       subq	LDC, C
#endif

	movq	C, CO1
#ifndef RT
	addq	LDC, C
#endif

	testq	$1, M
	jle	.L130
	ALIGN_4

.L140:
#ifdef LN
       movq	K, %rax
       salq	$0 + ZBASE_SHIFT, %rax
       subq	%rax, AORIG
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	movq	AORIG, AO
	salq	$ZBASE_SHIFT, %rax
	leaq	(AO, %rax, 1), AO
#endif

	leaq	16 * SIZE + BUFFER, BO

#if defined(LN) || defined(RT)
	movq	KK, %rax
	salq	$0 + ZBASE_SHIFT, %rax
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
	je	.L142

.L141:
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

	ADD1	%xmm2, %xmm8
	ADD2	%xmm3, %xmm9
	ADD1	%xmm4, %xmm10
	ADD2	%xmm5, %xmm11

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

	ADD1	%xmm2, %xmm8
	ADD2	%xmm3, %xmm9
	ADD1	%xmm4, %xmm10
	ADD2	%xmm5, %xmm11

	subq	$ -8 * SIZE, AO
	subq	$-16 * SIZE, BO
	subq	$1, %rax
	jne    .L141

.L142:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	movapd	POSINV, %xmm7

	andq	$3, %rax		# if (k & 1)
	BRANCH
	jle .L144

.L143:
	movapd	-16 * SIZE(AO), %xmm0
	movapd	-16 * SIZE(BO), %xmm2
	movapd	-14 * SIZE(BO), %xmm3

	mulpd	%xmm0, %xmm2
	mulpd	%xmm0, %xmm3

	ADD1	%xmm2, %xmm8
	ADD2	%xmm3, %xmm9

	addq	$2 * SIZE, AO
	addq	$4 * SIZE, BO
	subq	$1, %rax
	jg	.L143
	ALIGN_4

.L144:
	addpd	%xmm10, %xmm8
	addpd	%xmm11, %xmm9

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

	salq	$ZBASE_SHIFT, %rax
	leaq	(AO, %rax, 1), AO
	leaq	(B,  %rax, 1), B
	leaq	(BO, %rax, 2), BO
#endif

	SHUFPD_1 %xmm9, %xmm9

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)
	xorpd	%xmm7, %xmm9
#else
	xorpd	%xmm7, %xmm8
#endif

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(RR) || defined(RC) || defined(CR) || defined(CC)
	subpd	%xmm9, %xmm8
#else
	addpd	%xmm9, %xmm8
#endif


#if defined(LN) || defined(LT)
	movapd	-16 * SIZE(B), %xmm9

	subpd	%xmm8,  %xmm9
#else
	movapd	-16 * SIZE(AO), %xmm9

	subpd	%xmm8,  %xmm9
#endif

#ifndef CONJ
	SHUFPD_1 %xmm7, %xmm7
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
	movddup	-16 * SIZE(B), %xmm0
	movddup	-15 * SIZE(B), %xmm1

	pshufd	$0x4e, %xmm9, %xmm8

	xorpd	 %xmm7, %xmm8

	mulpd	 %xmm0, %xmm9
	mulpd	 %xmm1, %xmm8

	addpd	 %xmm8, %xmm9
#endif

#ifdef RT
	movddup	-16 * SIZE(B), %xmm0
	movddup	-15 * SIZE(B), %xmm1

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
	movapd	%xmm9, -16 * SIZE(B)

	movddup	%xmm9,  %xmm8
	unpckhpd %xmm9,  %xmm9

	movapd	%xmm8,  -16 * SIZE(BO)
	movapd	%xmm9,  -14 * SIZE(BO)
#else
	movapd	%xmm9, -16 * SIZE(AO)
#endif

#ifndef LN
	addq	$2 * SIZE, CO1
#endif

#if defined(LT) || defined(RN)
	movq	K,  %rax
	subq	KK, %rax
	salq	$ZBASE_SHIFT, %rax
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
	salq	$0 + ZBASE_SHIFT, %rax
	addq	%rax, AORIG
#endif
	ALIGN_4

.L130:
	movq	M,  I
	sarq	$1, I		# i = (m >> 2)
	jle	.L199
	ALIGN_4

.L110:
#ifdef LN
       movq	K, %rax
       salq	$1 + ZBASE_SHIFT, %rax
       subq	%rax, AORIG
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	movq	AORIG, AO
	salq	$ZBASE_SHIFT, %rax
	leaq	(AO, %rax, 2), AO
#endif

	leaq	16 * SIZE + BUFFER, BO

#if defined(LN) || defined(RT)
	movq	KK, %rax
	salq	$0 + ZBASE_SHIFT, %rax
	leaq	(BO, %rax, 2), BO
#endif

	pxor	%xmm8, %xmm8
	pxor	%xmm9, %xmm9
	pxor	%xmm12, %xmm12
	pxor	%xmm13, %xmm13
	prefetcht0    -3 * SIZE(CO1)

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	sarq	$2, %rax
	je	.L112

.L111:
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

	ADD1	%xmm2, %xmm8
	ADD1	%xmm3, %xmm12
	ADD2	%xmm4, %xmm9
	ADD2	%xmm5, %xmm13

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

	ADD1	%xmm2, %xmm8
	ADD1	%xmm3, %xmm12
	ADD2	%xmm4, %xmm9
	ADD2	%xmm5, %xmm13

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

	ADD1	%xmm2, %xmm8
	ADD1	%xmm3, %xmm12
	ADD2	%xmm4, %xmm9
	ADD2	%xmm5, %xmm13

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

	ADD1	%xmm2, %xmm8
	ADD1	%xmm3, %xmm12
	ADD2	%xmm4, %xmm9
	ADD2	%xmm5, %xmm13

	subq	$-16 * SIZE, AO
	subq	$-16 * SIZE, BO
	subq	$1, %rax
	jne    .L111
	ALIGN_4

.L112:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	movapd	POSINV,  %xmm7
	andq	$3, %rax		# if (k & 1)
	BRANCH
	jle .L114

.L113:
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

	ADD1	%xmm2, %xmm8
	ADD1	%xmm3, %xmm12
	ADD2	%xmm4, %xmm9
	ADD2	%xmm5, %xmm13

	addq	$4 * SIZE, AO
	addq	$4 * SIZE, BO
	subq	$1, %rax
	jg	.L113
	ALIGN_4

.L114:
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

	salq	$ZBASE_SHIFT, %rax
	leaq	(AO, %rax, 2), AO
	leaq	(B,  %rax, 1), B
	leaq	(BO, %rax, 2), BO
#endif

	SHUFPD_1 %xmm9, %xmm9
	SHUFPD_1 %xmm13, %xmm13

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)
	xorpd	%xmm7, %xmm9
	xorpd	%xmm7, %xmm13
#else
	xorpd	%xmm7, %xmm8
	xorpd	%xmm7, %xmm12
#endif

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(RR) || defined(RC) || defined(CR) || defined(CC)
	subpd	%xmm9, %xmm8
	subpd	%xmm13, %xmm12
#else
	addpd	%xmm9, %xmm8
	addpd	%xmm13, %xmm12
#endif

#if defined(LN) || defined(LT)
	movapd	-16 * SIZE(B), %xmm9
	movapd	-14 * SIZE(B), %xmm13

	subpd	%xmm8,  %xmm9
	subpd	%xmm12,  %xmm13
#else
	movapd	-16 * SIZE(AO), %xmm9
	movapd	-14 * SIZE(AO), %xmm13

	subpd	%xmm8,  %xmm9
	subpd	%xmm12,  %xmm13
#endif

#ifndef CONJ
	SHUFPD_1 %xmm7, %xmm7
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
	movddup	-16 * SIZE(B), %xmm0
	movddup	-15 * SIZE(B), %xmm1

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
	movddup	-16 * SIZE(B), %xmm0
	movddup	-15 * SIZE(B), %xmm1

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
	movapd	%xmm9,  -16 * SIZE(B)
	movapd	%xmm13, -14 * SIZE(B)

	movddup	%xmm9,  %xmm8
	unpckhpd %xmm9,  %xmm9
	movddup	%xmm13, %xmm12
	unpckhpd %xmm13, %xmm13

	movapd	%xmm8,  -16 * SIZE(BO)
	movapd	%xmm9,  -14 * SIZE(BO)
	movapd	%xmm12, -12 * SIZE(BO)
	movapd	%xmm13, -10 * SIZE(BO)
#else
	movapd	%xmm9,  -16 * SIZE(AO)
	movapd	%xmm13, -14 * SIZE(AO)
#endif

#ifndef LN
	addq	$4 * SIZE, CO1
#endif

#if defined(LT) || defined(RN)
	movq	K,  %rax
	subq	KK, %rax
	salq	$ZBASE_SHIFT, %rax
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
	salq	$1 + ZBASE_SHIFT, %rax
	addq	%rax, AORIG
#endif

	decq	I			# i --
	jg	.L110
	ALIGN_4

.L199:
#ifdef LN
       leaq	(, K, SIZE), %rax
       leaq	(B, %rax, 2), B
#endif

#if defined(LT) || defined(RN)
	movq	K,  %rax
	subq	KK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(B,  %rax, 1 * COMPSIZE), B
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
