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
#define J	%r12
#define AO	%rdi
#define BO	%rsi
#define	CO1	%r15
#define CO2	%rbp

#ifndef WINDOWS_ABI

#define STACKSIZE 64

#define OLD_LDC		 8 + STACKSIZE(%rsp)
#define OLD_OFFSET	16 + STACKSIZE(%rsp)

#else

#define STACKSIZE 256

#define OLD_ALPHA_I	40 + STACKSIZE(%rsp)
#define OLD_A		48 + STACKSIZE(%rsp)
#define OLD_B		56 + STACKSIZE(%rsp)
#define OLD_C		64 + STACKSIZE(%rsp)
#define OLD_LDC		72 + STACKSIZE(%rsp)
#define OLD_OFFSET	80 + STACKSIZE(%rsp)

#endif

#define POSINV	  0(%rsp)
#define ALPHA_R	 16(%rsp)
#define ALPHA_I	 32(%rsp)
#define OFFSET	 40(%rsp)
#define KK	 48(%rsp)
#define KKK	 56(%rsp)
#define AORIG    64(%rsp)
#define BORIG	 72(%rsp)
#define BUFFER	128(%rsp)

#if defined(OPTERON) || defined(BARCELONA) || defined(SHANGHAI) || defined(BOBCAT) || defined(BULLDOZER)
#define PREFETCH     prefetch
#define PREFETCHW    prefetchw
#define PREFETCHNTA  prefetchnta
#define PREFETCHSIZE (8 * 6 + 4)
#endif

#ifdef GENERIC
#define PREFETCH     prefetcht0
#define PREFETCHW    prefetcht0
#define PREFETCHNTA  prefetchnta
#define PREFETCHSIZE (8 * 6 + 4)
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

#ifndef CONJ
#define NN
#else
#if defined(LN) || defined(LT)
#define CN
#else
#define NC
#endif
#endif

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

	movq	OLD_M, M
	movq	OLD_N, N

	pcmpeqb	%xmm15, %xmm15
	psllq	$63, %xmm15	# Generate mask
	pxor	%xmm2, %xmm2

	movlpd	  %xmm2,  0 + POSINV
	movlpd	  %xmm15, 8 + POSINV

	movlpd	%xmm4, OFFSET
	movlpd	%xmm4, KK

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

	testq	$1, N
	jle	.L100

.L101:
#ifdef LN
	movq	OFFSET, %rax
	addq	M, %rax
	movq	%rax, KK
#endif

/* Copying to Sub Buffer */
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
	movlpd	 0 * SIZE(B), %xmm0
	movlpd	 1 * SIZE(B), %xmm1
	movlpd	 2 * SIZE(B), %xmm2
	movlpd	 3 * SIZE(B), %xmm3
	movlpd	 4 * SIZE(B), %xmm4
	movlpd	 5 * SIZE(B), %xmm5
	movlpd	 6 * SIZE(B), %xmm6
	movlpd	 7 * SIZE(B), %xmm7

	movlpd	%xmm0,  0 * SIZE(BO)
	movlpd	%xmm0,  1 * SIZE(BO)
	movlpd	%xmm1,  2 * SIZE(BO)
	movlpd	%xmm1,  3 * SIZE(BO)
	movlpd	%xmm2,  4 * SIZE(BO)
	movlpd	%xmm2,  5 * SIZE(BO)
	movlpd	%xmm3,  6 * SIZE(BO)
	movlpd	%xmm3,  7 * SIZE(BO)
	movlpd	%xmm4,  8 * SIZE(BO)
	movlpd	%xmm4,  9 * SIZE(BO)
	movlpd	%xmm5, 10 * SIZE(BO)
	movlpd	%xmm5, 11 * SIZE(BO)
	movlpd	%xmm6, 12 * SIZE(BO)
	movlpd	%xmm6, 13 * SIZE(BO)
	movlpd	%xmm7, 14 * SIZE(BO)
	movlpd	%xmm7, 15 * SIZE(BO)

	subq	$-16 * SIZE, BO
	addq	$ 8 * SIZE, B
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
	movlpd	 0 * SIZE(B), %xmm0
	movlpd	 1 * SIZE(B), %xmm1

	movlpd	%xmm0,  0 * SIZE(BO)
	movlpd	%xmm0,  1 * SIZE(BO)
	movlpd	%xmm1,  2 * SIZE(BO)
	movlpd	%xmm1,  3 * SIZE(BO)

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

	movq	M,  I
	sarq	$1, I		# i = (m >> 2)
	jle	.L130
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

	leaq	BUFFER, BO

#if defined(LN) || defined(RT)
	movq	KK, %rax
	salq	$0 + ZBASE_SHIFT, %rax
	leaq	(BO, %rax, 2), BO
#endif

	pxor	%xmm0, %xmm0
	pxor	%xmm1, %xmm1
	pxor	%xmm4, %xmm4
	pxor	%xmm5, %xmm5
	PREFETCHW      4 * SIZE(CO1)

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	sarq	$2, %rax
	je	.L112

.L111:
	movapd	 0 * SIZE(AO), %xmm8
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
	movapd	 4 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm0
	mulpd	 6 * SIZE(BO), %xmm8
	addpd	%xmm8, %xmm1

	movapd	 6 * SIZE(AO), %xmm8
	movapd	 4 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm4
	mulpd	 6 * SIZE(BO), %xmm8
	addpd	%xmm8, %xmm5

	movapd	 8 * SIZE(AO), %xmm8
	movapd	 8 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm0
	mulpd	10 * SIZE(BO), %xmm8
	addpd	%xmm8, %xmm1

	movapd	10 * SIZE(AO), %xmm8
	movapd	 8 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm4
	mulpd	10 * SIZE(BO), %xmm8
	addpd	%xmm8, %xmm5

	movapd	12 * SIZE(AO), %xmm8
	movapd	12 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm0
	mulpd	14 * SIZE(BO), %xmm8
	addpd	%xmm8, %xmm1

	movapd	14 * SIZE(AO), %xmm8
	movapd	12 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm4
	mulpd	14 * SIZE(BO), %xmm8
	addpd	%xmm8, %xmm5

	addq   $16 * SIZE, AO
	addq   $16 * SIZE, BO
	decq   %rax
	jne    .L111
	ALIGN_4

.L112:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	movapd	POSINV,  %xmm15
	andq	$3, %rax		# if (k & 1)
	BRANCH
	jle .L114

.L113:
	movapd	 0 * SIZE(AO), %xmm8
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

	addq	$4 * SIZE, AO		# aoffset  += 4
	addq	$4 * SIZE, BO		# boffset1 += 8
	decq	%rax
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
	leaq	BUFFER, BO

	salq	$ZBASE_SHIFT, %rax
	leaq	(AO, %rax, 2), AO
	leaq	(B,  %rax, 1), B
	leaq	(BO, %rax, 2), BO
#endif

	SHUFPD_1 %xmm1, %xmm1
	SHUFPD_1 %xmm5, %xmm5

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)
	xorpd	%xmm15, %xmm1
	xorpd	%xmm15, %xmm5
#else
	xorpd	%xmm15, %xmm0
	xorpd	%xmm15, %xmm4
#endif

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(RR) || defined(RC) || defined(CR) || defined(CC)
	subpd	%xmm1, %xmm0
	subpd	%xmm5, %xmm4
#else
	addpd	%xmm1, %xmm0
	addpd	%xmm5, %xmm4
#endif

#if defined(LN) || defined(LT)
	movapd	 0 * SIZE(B), %xmm1
	movapd	 2 * SIZE(B), %xmm5

	subpd	%xmm0,  %xmm1
	subpd	%xmm4,  %xmm5
#else
	movapd	 0 * SIZE(AO), %xmm1
	movapd	 2 * SIZE(AO), %xmm5

	subpd	%xmm0,  %xmm1
	subpd	%xmm4,  %xmm5
#endif

#ifndef CONJ
	SHUFPD_1 %xmm15, %xmm15
#endif

#ifdef LN
	movlpd	 6 * SIZE(AO), %xmm8
	movhpd	 6 * SIZE(AO), %xmm8
	movlpd	 7 * SIZE(AO), %xmm9
	movhpd	 7 * SIZE(AO), %xmm9
	movlpd	 4 * SIZE(AO), %xmm10
	movhpd	 4 * SIZE(AO), %xmm10
	movlpd	 5 * SIZE(AO), %xmm11
	movhpd	 5 * SIZE(AO), %xmm11
	movlpd	 0 * SIZE(AO), %xmm12
	movhpd	 0 * SIZE(AO), %xmm12
	movlpd	 1 * SIZE(AO), %xmm13
	movhpd	 1 * SIZE(AO), %xmm13

	pshufd	$0x4e, %xmm5, %xmm4

	xorpd	 %xmm15, %xmm4

	mulpd	 %xmm8, %xmm5
	mulpd	 %xmm9, %xmm4

	addpd	 %xmm4, %xmm5

	movapd	 %xmm5, %xmm0
	pshufd	 $0x4e, %xmm5, %xmm4

	xorpd	 %xmm15, %xmm4

	mulpd	 %xmm10, %xmm0
	mulpd	 %xmm11, %xmm4

	subpd	 %xmm0, %xmm1
	subpd	 %xmm4, %xmm1

	pshufd	$0x4e, %xmm1, %xmm0

	xorpd	 %xmm15, %xmm0

	mulpd	 %xmm12, %xmm1
	mulpd	 %xmm13, %xmm0

	addpd	 %xmm0, %xmm1
#endif

#ifdef LT
	movlpd	 0 * SIZE(AO), %xmm8
	movhpd	 0 * SIZE(AO), %xmm8
	movlpd	 1 * SIZE(AO), %xmm9
	movhpd	 1 * SIZE(AO), %xmm9
	movlpd	 2 * SIZE(AO), %xmm10
	movhpd	 2 * SIZE(AO), %xmm10
	movlpd	 3 * SIZE(AO), %xmm11
	movhpd	 3 * SIZE(AO), %xmm11
	movlpd	 6 * SIZE(AO), %xmm12
	movhpd	 6 * SIZE(AO), %xmm12
	movlpd	 7 * SIZE(AO), %xmm13
	movhpd	 7 * SIZE(AO), %xmm13

	pshufd	$0x4e, %xmm1, %xmm0

	xorpd	 %xmm15, %xmm0

	mulpd	 %xmm8, %xmm1
	mulpd	 %xmm9, %xmm0

	addpd	 %xmm0, %xmm1

	movapd	 %xmm1, %xmm0
	pshufd	 $0x4e, %xmm1, %xmm4

	xorpd	 %xmm15, %xmm4

	mulpd	 %xmm10, %xmm0
	mulpd	 %xmm11, %xmm4

	subpd	 %xmm0, %xmm5
	subpd	 %xmm4, %xmm5

	pshufd	$0x4e, %xmm5, %xmm4

	xorpd	 %xmm15, %xmm4

	mulpd	 %xmm12, %xmm5
	mulpd	 %xmm13, %xmm4

	addpd	 %xmm4, %xmm5
#endif

#ifdef RN
	movlpd	 0 * SIZE(B), %xmm8
	movhpd	 0 * SIZE(B), %xmm8
	movlpd	 1 * SIZE(B), %xmm9
	movhpd	 1 * SIZE(B), %xmm9

	pshufd	$0x4e, %xmm1, %xmm0
	pshufd	$0x4e, %xmm5, %xmm4

	xorpd	 %xmm15, %xmm0
	xorpd	 %xmm15, %xmm4

	mulpd	 %xmm8, %xmm1
	mulpd	 %xmm9, %xmm0
	mulpd	 %xmm8, %xmm5
	mulpd	 %xmm9, %xmm4

	addpd	 %xmm0, %xmm1
	addpd	 %xmm4, %xmm5
#endif

#ifdef RT
	movlpd	 0 * SIZE(B), %xmm8
	movhpd	 0 * SIZE(B), %xmm8
	movlpd	 1 * SIZE(B), %xmm9
	movhpd	 1 * SIZE(B), %xmm9

	pshufd	$0x4e, %xmm1, %xmm0
	pshufd	$0x4e, %xmm5, %xmm4

	xorpd	 %xmm15, %xmm0
	xorpd	 %xmm15, %xmm4

	mulpd	 %xmm8, %xmm1
	mulpd	 %xmm9, %xmm0
	mulpd	 %xmm8, %xmm5
	mulpd	 %xmm9, %xmm4

	addpd	 %xmm0, %xmm1
	addpd	 %xmm4, %xmm5
#endif

#ifdef LN
	subq	$4 * SIZE, CO1
#endif

	movsd	%xmm1,   0 * SIZE(CO1)
	movhpd	%xmm1,   1 * SIZE(CO1)
	movsd	%xmm5,   2 * SIZE(CO1)
	movhpd	%xmm5,   3 * SIZE(CO1)

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
	movapd	%xmm1,   0 * SIZE(AO)
	movapd	%xmm5,   2 * SIZE(AO)
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

.L130:
	testq	$1, M
	jle	.L199
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

	leaq	BUFFER, BO

#if defined(LN) || defined(RT)
	movq	KK, %rax
	salq	$0 + ZBASE_SHIFT, %rax
	leaq	(BO, %rax, 2), BO
#endif

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
	sarq	$2, %rax
	je	.L142

.L141:
	movapd	 0 * SIZE(AO), %xmm8
	movapd	 0 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm0
	mulpd	 2 * SIZE(BO), %xmm8
	addpd	%xmm8, %xmm1

	movapd	 2 * SIZE(AO), %xmm8
	movapd	 4 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm2
	mulpd	 6 * SIZE(BO), %xmm8
	addpd	%xmm8, %xmm3

	movapd	 4 * SIZE(AO), %xmm8
	movapd	 8 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm0
	mulpd	10 * SIZE(BO), %xmm8
	addpd	%xmm8, %xmm1

	movapd	 6 * SIZE(AO), %xmm8
	movapd	12 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm2
	mulpd	14 * SIZE(BO), %xmm8
	addpd	%xmm8, %xmm3

	addq   $8  * SIZE, AO
	addq   $16 * SIZE, BO
	decq   %rax
	jne    .L141

.L142:
	addpd	%xmm2, %xmm0
	addpd	%xmm3, %xmm1

	movapd	POSINV, %xmm15

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	BRANCH
	jle .L144

.L143:
	movapd	 0 * SIZE(AO), %xmm8
	movapd	 0 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm0
	mulpd	 2 * SIZE(BO), %xmm8
	addpd	%xmm8, %xmm1

	addq	$2 * SIZE, AO		# aoffset  += 4
	addq	$4 * SIZE, BO		# boffset1 += 8
	decq	%rax
	jg	.L143
	ALIGN_4

.L144:
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

	salq	$ZBASE_SHIFT, %rax
	leaq	(AO, %rax, 1), AO
	leaq	(B,  %rax, 1), B
	leaq	(BO, %rax, 2), BO
#endif

	SHUFPD_1 %xmm1, %xmm1

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)
	xorpd	%xmm15, %xmm1
#else
	xorpd	%xmm15, %xmm0
#endif

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(RR) || defined(RC) || defined(CR) || defined(CC)
	subpd	%xmm1, %xmm0
#else
	addpd	%xmm1, %xmm0
#endif


#if defined(LN) || defined(LT)
	movapd	 0 * SIZE(B), %xmm1

	subpd	%xmm0,  %xmm1
#else
	movapd	 0 * SIZE(AO), %xmm1

	subpd	%xmm0,  %xmm1
#endif

#ifndef CONJ
	SHUFPD_1 %xmm15, %xmm15
#endif

#ifdef LN
	movlpd	 0 * SIZE(AO), %xmm8
	movhpd	 0 * SIZE(AO), %xmm8
	movlpd	 1 * SIZE(AO), %xmm9
	movhpd	 1 * SIZE(AO), %xmm9

	pshufd	$0x4e, %xmm1, %xmm0
	xorpd	 %xmm15, %xmm0

	mulpd	 %xmm8, %xmm1
	mulpd	 %xmm9, %xmm0

	addpd	 %xmm0, %xmm1
#endif

#ifdef LT
	movlpd	 0 * SIZE(AO), %xmm8
	movhpd	 0 * SIZE(AO), %xmm8
	movlpd	 1 * SIZE(AO), %xmm9
	movhpd	 1 * SIZE(AO), %xmm9

	pshufd	$0x4e, %xmm1, %xmm0

	xorpd	 %xmm15, %xmm0

	mulpd	 %xmm8, %xmm1
	mulpd	 %xmm9, %xmm0

	addpd	 %xmm0, %xmm1
#endif

#ifdef RN
	movlpd	 0 * SIZE(B), %xmm8
	movhpd	 0 * SIZE(B), %xmm8
	movlpd	 1 * SIZE(B), %xmm9
	movhpd	 1 * SIZE(B), %xmm9

	pshufd	$0x4e, %xmm1, %xmm0

	xorpd	 %xmm15, %xmm0

	mulpd	 %xmm8, %xmm1
	mulpd	 %xmm9, %xmm0

	addpd	 %xmm0, %xmm1
#endif

#ifdef RT
	movlpd	 0 * SIZE(B), %xmm8
	movhpd	 0 * SIZE(B), %xmm8
	movlpd	 1 * SIZE(B), %xmm9
	movhpd	 1 * SIZE(B), %xmm9

	pshufd	$0x4e, %xmm1, %xmm0

	xorpd	 %xmm15, %xmm0

	mulpd	 %xmm8, %xmm1
	mulpd	 %xmm9, %xmm0

	addpd	 %xmm0, %xmm1
#endif

#ifdef LN
	subq	$2 * SIZE, CO1
#endif

	movsd	%xmm1,   0 * SIZE(CO1)
	movhpd	%xmm1,   1 * SIZE(CO1)

#if defined(LN) || defined(LT)
	movapd	%xmm1,   0 * SIZE(B)

	movlpd	%xmm1,   0 * SIZE(BO)
	movlpd	%xmm1,   1 * SIZE(BO)
	movhpd	%xmm1,   2 * SIZE(BO)
	movhpd	%xmm1,   3 * SIZE(BO)
#else
	movapd	%xmm1,   0 * SIZE(AO)
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

.L100:
	movq	N,  J
	sarq	$1, J		# j = (n >> 2)
	jle	.L999
	ALIGN_4

.L01:
#ifdef LN
	movq	OFFSET, %rax
	addq	M, %rax
	movq	%rax, KK
#endif

/* Copying to Sub Buffer */
	leaq	BUFFER, BO

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
	PREFETCHNTA	 56 * SIZE(B)

	movlpd	 0 * SIZE(B), %xmm0
	movlpd	 1 * SIZE(B), %xmm1
	movlpd	 2 * SIZE(B), %xmm2
	movlpd	 3 * SIZE(B), %xmm3
	movlpd	 4 * SIZE(B), %xmm4
	movlpd	 5 * SIZE(B), %xmm5
	movlpd	 6 * SIZE(B), %xmm6
	movlpd	 7 * SIZE(B), %xmm7

	movlpd	%xmm0,  0 * SIZE(BO)
	movlpd	%xmm0,  1 * SIZE(BO)
	movlpd	%xmm1,  2 * SIZE(BO)
	movlpd	%xmm1,  3 * SIZE(BO)
	movlpd	%xmm2,  4 * SIZE(BO)
	movlpd	%xmm2,  5 * SIZE(BO)
	movlpd	%xmm3,  6 * SIZE(BO)
	movlpd	%xmm3,  7 * SIZE(BO)
	movlpd	%xmm4,  8 * SIZE(BO)
	movlpd	%xmm4,  9 * SIZE(BO)
	movlpd	%xmm5, 10 * SIZE(BO)
	movlpd	%xmm5, 11 * SIZE(BO)
	movlpd	%xmm6, 12 * SIZE(BO)
	movlpd	%xmm6, 13 * SIZE(BO)
	movlpd	%xmm7, 14 * SIZE(BO)
	movlpd	%xmm7, 15 * SIZE(BO)

	subq	$-16 * SIZE, BO
	addq	$  8 * SIZE, B
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
	movlpd	 0 * SIZE(B), %xmm0
	movlpd	 1 * SIZE(B), %xmm1
	movlpd	 2 * SIZE(B), %xmm2
	movlpd	 3 * SIZE(B), %xmm3

	movlpd	%xmm0,  0 * SIZE(BO)
	movlpd	%xmm0,  1 * SIZE(BO)
	movlpd	%xmm1,  2 * SIZE(BO)
	movlpd	%xmm1,  3 * SIZE(BO)
	movlpd	%xmm2,  4 * SIZE(BO)
	movlpd	%xmm2,  5 * SIZE(BO)
	movlpd	%xmm3,  6 * SIZE(BO)
	movlpd	%xmm3,  7 * SIZE(BO)

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

	movq	M,  I
	sarq	$1, I		# i = (m >> 2)
	jle	.L30
	ALIGN_4

.L10:
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

	leaq	BUFFER, BO

#if defined(LN) || defined(RT)
	movq	KK, %rax
	salq	$1 + ZBASE_SHIFT, %rax
	leaq	(BO, %rax, 2), BO
#endif

	movapd	 0 * SIZE(AO), %xmm8
	pxor	%xmm0, %xmm0
	movapd	 2 * SIZE(AO), %xmm10
	pxor	%xmm1, %xmm1
	movapd	 4 * SIZE(AO), %xmm12
	pxor	%xmm2, %xmm2
	movapd	 6 * SIZE(AO), %xmm14
	pxor	%xmm3, %xmm3

	movapd	 0 * SIZE(BO), %xmm9
	pxor	%xmm4, %xmm4
	movapd	 2 * SIZE(BO), %xmm11
	pxor	%xmm5, %xmm5
	movapd	 4 * SIZE(BO), %xmm13
	movapd	 8 * SIZE(BO), %xmm15

	PREFETCHW      4 * SIZE(CO1)
	pxor	%xmm6, %xmm6
	PREFETCHW      4 * SIZE(CO2)
	pxor	%xmm7, %xmm7

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
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

.L15:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	movapd	POSINV,  %xmm15
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
	subq	$2, %rax
#else
	subq	$2, %rax
#endif

	movq	AORIG, AO
	movq	BORIG, B
	leaq	BUFFER, BO

	salq	$ZBASE_SHIFT, %rax
	leaq	(AO, %rax, 2), AO
	leaq	(B,  %rax, 2), B
	leaq	(BO, %rax, 4), BO
#endif

	SHUFPD_1 %xmm1, %xmm1
	SHUFPD_1 %xmm3, %xmm3
	SHUFPD_1 %xmm5, %xmm5
	SHUFPD_1 %xmm7, %xmm7

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)
	xorpd	%xmm15, %xmm1
	xorpd	%xmm15, %xmm3
	xorpd	%xmm15, %xmm5
	xorpd	%xmm15, %xmm7
#else
	xorpd	%xmm15, %xmm0
	xorpd	%xmm15, %xmm2
	xorpd	%xmm15, %xmm4
	xorpd	%xmm15, %xmm6
#endif

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(RR) || defined(RC) || defined(CR) || defined(CC)
	subpd	%xmm1, %xmm0
	subpd	%xmm3, %xmm2
	subpd	%xmm5, %xmm4
	subpd	%xmm7, %xmm6
#else
	addpd	%xmm1, %xmm0
	addpd	%xmm3, %xmm2
	addpd	%xmm5, %xmm4
	addpd	%xmm7, %xmm6
#endif

#if defined(LN) || defined(LT)
	movapd	 0 * SIZE(B), %xmm1
	movapd	 2 * SIZE(B), %xmm3
	movapd	 4 * SIZE(B), %xmm5
	movapd	 6 * SIZE(B), %xmm7

	subpd	%xmm0,  %xmm1
	subpd	%xmm2,  %xmm3
	subpd	%xmm4,  %xmm5
	subpd	%xmm6,  %xmm7
#else
	movapd	 0 * SIZE(AO), %xmm1
	movapd	 2 * SIZE(AO), %xmm5
	movapd	 4 * SIZE(AO), %xmm3
	movapd	 6 * SIZE(AO), %xmm7

	subpd	%xmm0,  %xmm1
	subpd	%xmm2,  %xmm3
	subpd	%xmm4,  %xmm5
	subpd	%xmm6,  %xmm7
#endif

#ifndef CONJ
	SHUFPD_1 %xmm15, %xmm15
#endif

#ifdef LN
	movlpd	 6 * SIZE(AO), %xmm8
	movhpd	 6 * SIZE(AO), %xmm8
	movlpd	 7 * SIZE(AO), %xmm9
	movhpd	 7 * SIZE(AO), %xmm9
	movlpd	 4 * SIZE(AO), %xmm10
	movhpd	 4 * SIZE(AO), %xmm10
	movlpd	 5 * SIZE(AO), %xmm11
	movhpd	 5 * SIZE(AO), %xmm11
	movlpd	 0 * SIZE(AO), %xmm12
	movhpd	 0 * SIZE(AO), %xmm12
	movlpd	 1 * SIZE(AO), %xmm13
	movhpd	 1 * SIZE(AO), %xmm13

	pshufd	$0x4e, %xmm5, %xmm4
	pshufd	$0x4e, %xmm7, %xmm6

	xorpd	 %xmm15, %xmm4
	xorpd	 %xmm15, %xmm6

	mulpd	 %xmm8, %xmm5
	mulpd	 %xmm9, %xmm4
	mulpd	 %xmm8, %xmm7
	mulpd	 %xmm9, %xmm6

	addpd	 %xmm4, %xmm5
	addpd	 %xmm6, %xmm7

	movapd	 %xmm5, %xmm0
	movapd	 %xmm7, %xmm2
	pshufd	 $0x4e, %xmm5, %xmm4
	pshufd	 $0x4e, %xmm7, %xmm6

	xorpd	 %xmm15, %xmm4
	xorpd	 %xmm15, %xmm6

	mulpd	 %xmm10, %xmm0
	mulpd	 %xmm10, %xmm2
	mulpd	 %xmm11, %xmm4
	mulpd	 %xmm11, %xmm6

	subpd	 %xmm0, %xmm1
	subpd	 %xmm2, %xmm3
	subpd	 %xmm4, %xmm1
	subpd	 %xmm6, %xmm3

	pshufd	$0x4e, %xmm1, %xmm0
	pshufd	$0x4e, %xmm3, %xmm2

	xorpd	 %xmm15, %xmm0
	xorpd	 %xmm15, %xmm2

	mulpd	 %xmm12, %xmm1
	mulpd	 %xmm13, %xmm0
	mulpd	 %xmm12, %xmm3
	mulpd	 %xmm13, %xmm2

	addpd	 %xmm0, %xmm1
	addpd	 %xmm2, %xmm3
#endif

#ifdef LT
	movlpd	 0 * SIZE(AO), %xmm8
	movhpd	 0 * SIZE(AO), %xmm8
	movlpd	 1 * SIZE(AO), %xmm9
	movhpd	 1 * SIZE(AO), %xmm9
	movlpd	 2 * SIZE(AO), %xmm10
	movhpd	 2 * SIZE(AO), %xmm10
	movlpd	 3 * SIZE(AO), %xmm11
	movhpd	 3 * SIZE(AO), %xmm11
	movlpd	 6 * SIZE(AO), %xmm12
	movhpd	 6 * SIZE(AO), %xmm12
	movlpd	 7 * SIZE(AO), %xmm13
	movhpd	 7 * SIZE(AO), %xmm13

	pshufd	$0x4e, %xmm1, %xmm0
	pshufd	$0x4e, %xmm3, %xmm2

	xorpd	 %xmm15, %xmm0
	xorpd	 %xmm15, %xmm2

	mulpd	 %xmm8, %xmm1
	mulpd	 %xmm9, %xmm0
	mulpd	 %xmm8, %xmm3
	mulpd	 %xmm9, %xmm2

	addpd	 %xmm0, %xmm1
	addpd	 %xmm2, %xmm3

	movapd	 %xmm1, %xmm0
	movapd	 %xmm3, %xmm2
	pshufd	 $0x4e, %xmm1, %xmm4
	pshufd	 $0x4e, %xmm3, %xmm6

	xorpd	 %xmm15, %xmm4
	xorpd	 %xmm15, %xmm6

	mulpd	 %xmm10, %xmm0
	mulpd	 %xmm10, %xmm2
	mulpd	 %xmm11, %xmm4
	mulpd	 %xmm11, %xmm6

	subpd	 %xmm0, %xmm5
	subpd	 %xmm2, %xmm7
	subpd	 %xmm4, %xmm5
	subpd	 %xmm6, %xmm7

	pshufd	$0x4e, %xmm5, %xmm4
	pshufd	$0x4e, %xmm7, %xmm6

	xorpd	 %xmm15, %xmm4
	xorpd	 %xmm15, %xmm6

	mulpd	 %xmm12, %xmm5
	mulpd	 %xmm13, %xmm4
	mulpd	 %xmm12, %xmm7
	mulpd	 %xmm13, %xmm6

	addpd	 %xmm4, %xmm5
	addpd	 %xmm6, %xmm7
#endif

#ifdef RN
	movlpd	 0 * SIZE(B), %xmm8
	movhpd	 0 * SIZE(B), %xmm8
	movlpd	 1 * SIZE(B), %xmm9
	movhpd	 1 * SIZE(B), %xmm9
	movlpd	 2 * SIZE(B), %xmm10
	movhpd	 2 * SIZE(B), %xmm10
	movlpd	 3 * SIZE(B), %xmm11
	movhpd	 3 * SIZE(B), %xmm11
	movlpd	 6 * SIZE(B), %xmm12
	movhpd	 6 * SIZE(B), %xmm12
	movlpd	 7 * SIZE(B), %xmm13
	movhpd	 7 * SIZE(B), %xmm13

	pshufd	$0x4e, %xmm1, %xmm0
	pshufd	$0x4e, %xmm5, %xmm4

	xorpd	 %xmm15, %xmm0
	xorpd	 %xmm15, %xmm4

	mulpd	 %xmm8, %xmm1
	mulpd	 %xmm9, %xmm0
	mulpd	 %xmm8, %xmm5
	mulpd	 %xmm9, %xmm4

	addpd	 %xmm0, %xmm1
	addpd	 %xmm4, %xmm5

	movapd	 %xmm1, %xmm0
	movapd	 %xmm5, %xmm2
	pshufd	 $0x4e, %xmm1, %xmm4
	pshufd	 $0x4e, %xmm5, %xmm6

	xorpd	 %xmm15, %xmm4
	xorpd	 %xmm15, %xmm6

	mulpd	 %xmm10, %xmm0
	mulpd	 %xmm10, %xmm2
	mulpd	 %xmm11, %xmm4
	mulpd	 %xmm11, %xmm6

	subpd	 %xmm0, %xmm3
	subpd	 %xmm2, %xmm7
	subpd	 %xmm4, %xmm3
	subpd	 %xmm6, %xmm7

	pshufd	$0x4e, %xmm3, %xmm2
	pshufd	$0x4e, %xmm7, %xmm6

	xorpd	 %xmm15, %xmm2
	xorpd	 %xmm15, %xmm6

	mulpd	 %xmm12, %xmm3
	mulpd	 %xmm13, %xmm2
	mulpd	 %xmm12, %xmm7
	mulpd	 %xmm13, %xmm6

	addpd	 %xmm2, %xmm3
	addpd	 %xmm6, %xmm7
#endif

#ifdef RT
	movlpd	 6 * SIZE(B), %xmm8
	movhpd	 6 * SIZE(B), %xmm8
	movlpd	 7 * SIZE(B), %xmm9
	movhpd	 7 * SIZE(B), %xmm9
	movlpd	 4 * SIZE(B), %xmm10
	movhpd	 4 * SIZE(B), %xmm10
	movlpd	 5 * SIZE(B), %xmm11
	movhpd	 5 * SIZE(B), %xmm11
	movlpd	 0 * SIZE(B), %xmm12
	movhpd	 0 * SIZE(B), %xmm12
	movlpd	 1 * SIZE(B), %xmm13
	movhpd	 1 * SIZE(B), %xmm13

	pshufd	$0x4e, %xmm3, %xmm2
	pshufd	$0x4e, %xmm7, %xmm6

	xorpd	 %xmm15, %xmm2
	xorpd	 %xmm15, %xmm6

	mulpd	 %xmm8, %xmm3
	mulpd	 %xmm9, %xmm2
	mulpd	 %xmm8, %xmm7
	mulpd	 %xmm9, %xmm6

	addpd	 %xmm2, %xmm3
	addpd	 %xmm6, %xmm7

	movapd	 %xmm3, %xmm0
	movapd	 %xmm7, %xmm2
	pshufd	 $0x4e, %xmm3, %xmm4
	pshufd	 $0x4e, %xmm7, %xmm6

	xorpd	 %xmm15, %xmm4
	xorpd	 %xmm15, %xmm6

	mulpd	 %xmm10, %xmm0
	mulpd	 %xmm10, %xmm2
	mulpd	 %xmm11, %xmm4
	mulpd	 %xmm11, %xmm6

	subpd	 %xmm0, %xmm1
	subpd	 %xmm2, %xmm5
	subpd	 %xmm4, %xmm1
	subpd	 %xmm6, %xmm5

	pshufd	$0x4e, %xmm1, %xmm0
	pshufd	$0x4e, %xmm5, %xmm4

	xorpd	 %xmm15, %xmm0
	xorpd	 %xmm15, %xmm4

	mulpd	 %xmm12, %xmm1
	mulpd	 %xmm13, %xmm0
	mulpd	 %xmm12, %xmm5
	mulpd	 %xmm13, %xmm4

	addpd	 %xmm0, %xmm1
	addpd	 %xmm4, %xmm5
#endif

#ifdef LN
	subq	$4 * SIZE, CO1
	subq	$4 * SIZE, CO2
#endif

	movsd	%xmm1,   0 * SIZE(CO1)
	movhpd	%xmm1,   1 * SIZE(CO1)
	movsd	%xmm5,   2 * SIZE(CO1)
	movhpd	%xmm5,   3 * SIZE(CO1)

	movsd	%xmm3,   0 * SIZE(CO2)
	movhpd	%xmm3,   1 * SIZE(CO2)
	movsd	%xmm7,   2 * SIZE(CO2)
	movhpd	%xmm7,   3 * SIZE(CO2)

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
	movapd	%xmm1,   0 * SIZE(AO)
	movapd	%xmm5,   2 * SIZE(AO)
	movapd	%xmm3,   4 * SIZE(AO)
	movapd	%xmm7,   6 * SIZE(AO)
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

.L30:
	testq	$1, M
	jle	.L99

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

	leaq	BUFFER, BO

#if defined(LN) || defined(RT)
	movq	KK, %rax
	salq	$1 + ZBASE_SHIFT, %rax
	leaq	(BO, %rax, 2), BO
#endif

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
	sarq	$2, %rax
	je	.L42

.L41:
	movapd	 0 * SIZE(AO), %xmm8

	movapd	 0 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm0

	movapd	 2 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm1

	movapd	 4 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm2

	movapd	 6 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm3

	movapd	 2 * SIZE(AO), %xmm8

	movapd	 8 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm0

	movapd	10 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm1

	movapd	12 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm2

	movapd	14 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm3

	movapd	 4 * SIZE(AO), %xmm8

	movapd	16 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm0

	movapd	18 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm1

	movapd	20 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm2

	movapd	22 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm3

	movapd	 6 * SIZE(AO), %xmm8

	movapd	24 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm0

	movapd	26 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm1

	movapd	28 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm2

	movapd	30 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm3

	addq   $ 8 * SIZE, AO
	addq   $32 * SIZE, BO
	decq   %rax
	jne    .L41

.L42:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	movapd	POSINV,  %xmm15
	andq	$3, %rax		# if (k & 1)
	BRANCH
	jle .L44

.L43:
	movapd	 0 * SIZE(AO), %xmm8

	movapd	 0 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm0

	movapd	 2 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm1

	movapd	 4 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm2

	movapd	 6 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm3

	addq	$2 * SIZE, AO		# aoffset  += 4
	addq	$8 * SIZE, BO		# boffset1 += 8

	decq	%rax
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
	leaq	BUFFER, BO

	salq	$ZBASE_SHIFT, %rax
	leaq	(AO, %rax, 1), AO
	leaq	(B,  %rax, 2), B
	leaq	(BO, %rax, 4), BO
#endif

	SHUFPD_1 %xmm1, %xmm1
	SHUFPD_1 %xmm3, %xmm3

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)
	xorpd	%xmm15, %xmm1
	xorpd	%xmm15, %xmm3
#else
	xorpd	%xmm15, %xmm0
	xorpd	%xmm15, %xmm2
#endif

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(RR) || defined(RC) || defined(CR) || defined(CC)
	subpd	%xmm1, %xmm0
	subpd	%xmm3, %xmm2
#else
	addpd	%xmm1, %xmm0
	addpd	%xmm3, %xmm2
#endif

#if defined(LN) || defined(LT)
	movapd	 0 * SIZE(B), %xmm1
	movapd	 2 * SIZE(B), %xmm3

	subpd	%xmm0,  %xmm1
	subpd	%xmm2,  %xmm3
#else
	movapd	 0 * SIZE(AO), %xmm1
	movapd	 2 * SIZE(AO), %xmm3

	subpd	%xmm0,  %xmm1
	subpd	%xmm2,  %xmm3
#endif

#ifndef CONJ
	SHUFPD_1 %xmm15, %xmm15
#endif

#if defined(LN) || defined(LT)
	movlpd	 0 * SIZE(AO), %xmm8
	movhpd	 0 * SIZE(AO), %xmm8
	movlpd	 1 * SIZE(AO), %xmm9
	movhpd	 1 * SIZE(AO), %xmm9

	pshufd	$0x4e, %xmm1, %xmm0
	pshufd	$0x4e, %xmm3, %xmm2

	xorpd	 %xmm15, %xmm0
	xorpd	 %xmm15, %xmm2

	mulpd	 %xmm8, %xmm1
	mulpd	 %xmm9, %xmm0
	mulpd	 %xmm8, %xmm3
	mulpd	 %xmm9, %xmm2

	addpd	 %xmm0, %xmm1
	addpd	 %xmm2, %xmm3
#endif

#ifdef RN
	movlpd	 0 * SIZE(B), %xmm8
	movhpd	 0 * SIZE(B), %xmm8
	movlpd	 1 * SIZE(B), %xmm9
	movhpd	 1 * SIZE(B), %xmm9
	movlpd	 2 * SIZE(B), %xmm10
	movhpd	 2 * SIZE(B), %xmm10
	movlpd	 3 * SIZE(B), %xmm11
	movhpd	 3 * SIZE(B), %xmm11
	movlpd	 6 * SIZE(B), %xmm12
	movhpd	 6 * SIZE(B), %xmm12
	movlpd	 7 * SIZE(B), %xmm13
	movhpd	 7 * SIZE(B), %xmm13

	pshufd	$0x4e, %xmm1, %xmm0

	xorpd	 %xmm15, %xmm0

	mulpd	 %xmm8, %xmm1
	mulpd	 %xmm9, %xmm0

	addpd	 %xmm0, %xmm1

	movapd	 %xmm1, %xmm0
	pshufd	 $0x4e, %xmm1, %xmm4

	xorpd	 %xmm15, %xmm4

	mulpd	 %xmm10, %xmm0
	mulpd	 %xmm11, %xmm4

	subpd	 %xmm0, %xmm3
	subpd	 %xmm4, %xmm3

	pshufd	$0x4e, %xmm3, %xmm2

	xorpd	 %xmm15, %xmm2

	mulpd	 %xmm12, %xmm3
	mulpd	 %xmm13, %xmm2

	addpd	 %xmm2, %xmm3
#endif

#ifdef RT
	movlpd	 6 * SIZE(B), %xmm8
	movhpd	 6 * SIZE(B), %xmm8
	movlpd	 7 * SIZE(B), %xmm9
	movhpd	 7 * SIZE(B), %xmm9
	movlpd	 4 * SIZE(B), %xmm10
	movhpd	 4 * SIZE(B), %xmm10
	movlpd	 5 * SIZE(B), %xmm11
	movhpd	 5 * SIZE(B), %xmm11
	movlpd	 0 * SIZE(B), %xmm12
	movhpd	 0 * SIZE(B), %xmm12
	movlpd	 1 * SIZE(B), %xmm13
	movhpd	 1 * SIZE(B), %xmm13

	pshufd	$0x4e, %xmm3, %xmm2

	xorpd	 %xmm15, %xmm2

	mulpd	 %xmm8, %xmm3
	mulpd	 %xmm9, %xmm2

	addpd	 %xmm2, %xmm3

	movapd	 %xmm3, %xmm0
	pshufd	 $0x4e, %xmm3, %xmm4

	xorpd	 %xmm15, %xmm4

	mulpd	 %xmm10, %xmm0
	mulpd	 %xmm11, %xmm4

	subpd	 %xmm0, %xmm1
	subpd	 %xmm4, %xmm1

	pshufd	$0x4e, %xmm1, %xmm0

	xorpd	 %xmm15, %xmm0

	mulpd	 %xmm12, %xmm1
	mulpd	 %xmm13, %xmm0

	addpd	 %xmm0, %xmm1
#endif

#ifdef LN
	subq	$2 * SIZE, CO1
	subq	$2 * SIZE, CO2
#endif

	movsd	%xmm1,   0 * SIZE(CO1)
	movhpd	%xmm1,   1 * SIZE(CO1)

	movsd	%xmm3,   0 * SIZE(CO2)
	movhpd	%xmm3,   1 * SIZE(CO2)

#if defined(LN) || defined(LT)
	movapd	%xmm1,   0 * SIZE(B)
	movapd	%xmm3,   2 * SIZE(B)

	movlpd	%xmm1,   0 * SIZE(BO)
	movlpd	%xmm1,   1 * SIZE(BO)
	movhpd	%xmm1,   2 * SIZE(BO)
	movhpd	%xmm1,   3 * SIZE(BO)
	movlpd	%xmm3,   4 * SIZE(BO)
	movlpd	%xmm3,   5 * SIZE(BO)
	movhpd	%xmm3,   6 * SIZE(BO)
	movhpd	%xmm3,   7 * SIZE(BO)
#else
	movapd	%xmm1,   0 * SIZE(AO)
	movapd	%xmm3,   2 * SIZE(AO)

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
	ALIGN_3

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
