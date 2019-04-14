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
#define OFFSET	 16(%rsp)
#define KK	 24(%rsp)
#define KKK	 32(%rsp)
#define AORIG    40(%rsp)
#define BORIG	 48(%rsp)
#define BUFFER  128(%rsp)

#ifdef OPTERON
#define movsd movlps
#endif

#if defined(PENTIUM4) || defined(CORE2) || defined(PENRYN) || defined(DUNNINGTON) || defined(ATOM) || defined(NANO)
#define PREFETCH     prefetcht0
#define PREFETCHW    prefetcht0
#define PREFETCHNTA  prefetchnta
#define PREFETCHSIZE (8 * 6 + 4)
#endif

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
	mulps	%xmm8, %xmm9 ;\
	addps	%xmm9, %xmm0 ;\
	movaps	 0 * SIZE + 2 * (xx) * SIZE(BO), %xmm9 ;\
	mulps	%xmm8, %xmm11 ;\
	PREFETCH	(PREFETCHSIZE     +  0) * SIZE + 1 * (xx) * SIZE(AO) ;\
	addps	%xmm11, %xmm1 ;\
	movaps	 4 * SIZE + 2 * (xx) * SIZE(BO), %xmm11 ;\
	mulps	%xmm8, %xmm13 ;\
	mulps	12 * SIZE + 2 * (xx) * SIZE(BO), %xmm8 ;\
	addps	%xmm13, %xmm2 ;\
	movaps	 8 * SIZE + 2 * (xx) * SIZE(BO), %xmm13 ;\
	addps	%xmm8, %xmm3 ;\
	movaps	16 * SIZE + 1 * (xx) * SIZE(AO), %xmm8

#define KERNEL2(xx) \
	mulps	%xmm10, %xmm9 ;\
	addps	%xmm9, %xmm4 ;\
	movaps	32 * SIZE + 2 * (xx) * SIZE(BO), %xmm9 ;\
	mulps	%xmm10, %xmm11 ;\
	addps	%xmm11, %xmm5 ;\
	movaps	20 * SIZE + 2 * (xx) * SIZE(BO), %xmm11 ;\
	mulps	%xmm10, %xmm13 ;\
	mulps	12 * SIZE + 2 * (xx) * SIZE(BO), %xmm10 ;\
	addps	%xmm13, %xmm6 ;\
	movaps	24 * SIZE + 2 * (xx) * SIZE(BO), %xmm13 ;\
	addps	%xmm10, %xmm7 ;\
	movaps	20 * SIZE + 1 * (xx) * SIZE(AO), %xmm10

#define KERNEL3(xx) \
	mulps	%xmm12, %xmm15 ;\
	addps	%xmm15, %xmm0 ;\
	movaps	16 * SIZE + 2 * (xx) * SIZE(BO), %xmm15 ;\
	mulps	%xmm12, %xmm11 ;\
	addps	%xmm11, %xmm1 ;\
	movaps	20 * SIZE + 2 * (xx) * SIZE(BO), %xmm11 ;\
	mulps	%xmm12, %xmm13 ;\
	mulps	28 * SIZE + 2 * (xx) * SIZE(BO), %xmm12 ;\
	addps	%xmm13, %xmm2 ;\
	movaps	24 * SIZE + 2 * (xx) * SIZE(BO), %xmm13 ;\
	addps	%xmm12, %xmm3 ;\
	movaps	24 * SIZE + 1 * (xx) * SIZE(AO), %xmm12

#define KERNEL4(xx) \
	mulps	%xmm14, %xmm15 ;\
	addps	%xmm15, %xmm4 ;\
	movaps	48 * SIZE + 2 * (xx) * SIZE(BO), %xmm15 ;\
	mulps	%xmm14, %xmm11 ;\
	addps	%xmm11, %xmm5 ;\
	movaps	36 * SIZE + 2 * (xx) * SIZE(BO), %xmm11 ;\
	mulps	%xmm14, %xmm13 ;\
	mulps	28 * SIZE + 2 * (xx) * SIZE(BO), %xmm14 ;\
	addps	%xmm13, %xmm6 ;\
	movaps	40 * SIZE + 2 * (xx) * SIZE(BO), %xmm13 ;\
	addps	%xmm14, %xmm7 ;\
	movaps	28 * SIZE + 1 * (xx) * SIZE(AO), %xmm14

#define KERNEL5(xx) \
	mulps	%xmm8, %xmm9 ;\
	addps	%xmm9, %xmm0 ;\
	movaps	32 * SIZE + 2 * (xx) * SIZE(BO), %xmm9 ;\
	mulps	%xmm8, %xmm11 ;\
	PREFETCH	(PREFETCHSIZE     +  8) * SIZE + 1 * (xx) * SIZE(AO) ;\
	addps	%xmm11, %xmm1 ;\
	movaps	36 * SIZE + 2 * (xx) * SIZE(BO), %xmm11 ;\
	mulps	%xmm8, %xmm13 ;\
	mulps	44 * SIZE + 2 * (xx) * SIZE(BO), %xmm8 ;\
	addps	%xmm13, %xmm2 ;\
	movaps	40 * SIZE + 2 * (xx) * SIZE(BO), %xmm13 ;\
	addps	%xmm8, %xmm3 ;\
	movaps	32 * SIZE + 1 * (xx) * SIZE(AO), %xmm8

#define KERNEL6(xx) \
	mulps	%xmm10, %xmm9 ;\
	addps	%xmm9, %xmm4 ;\
	movaps	64 * SIZE + 2 * (xx) * SIZE(BO), %xmm9 ;\
	mulps	%xmm10, %xmm11 ;\
	addps	%xmm11, %xmm5 ;\
	movaps	52 * SIZE + 2 * (xx) * SIZE(BO), %xmm11 ;\
	mulps	%xmm10, %xmm13 ;\
	mulps	44 * SIZE + 2 * (xx) * SIZE(BO), %xmm10 ;\
	addps	%xmm13, %xmm6 ;\
	movaps	56 * SIZE + 2 * (xx) * SIZE(BO), %xmm13 ;\
	addps	%xmm10, %xmm7 ;\
	movaps	36 * SIZE + 1 * (xx) * SIZE(AO), %xmm10

#define KERNEL7(xx) \
	mulps	%xmm12, %xmm15 ;\
	addps	%xmm15, %xmm0 ;\
	movaps	48 * SIZE + 2 * (xx) * SIZE(BO), %xmm15 ;\
	mulps	%xmm12, %xmm11 ;\
	addps	%xmm11, %xmm1 ;\
	movaps	52 * SIZE + 2 * (xx) * SIZE(BO), %xmm11 ;\
	mulps	%xmm12, %xmm13 ;\
	mulps	60 * SIZE + 2 * (xx) * SIZE(BO), %xmm12 ;\
	addps	%xmm13, %xmm2 ;\
	movaps	56 * SIZE + 2 * (xx) * SIZE(BO), %xmm13 ;\
	addps	%xmm12, %xmm3 ;\
	movaps	40 * SIZE + 1 * (xx) * SIZE(AO), %xmm12

#define KERNEL8(xx) \
	mulps	%xmm14, %xmm15 ;\
	addps	%xmm15, %xmm4 ;\
	movaps	80 * SIZE + 2 * (xx) * SIZE(BO), %xmm15 ;\
	mulps	%xmm14, %xmm11 ;\
	addps	%xmm11, %xmm5 ;\
	movaps	68 * SIZE + 2 * (xx) * SIZE(BO), %xmm11 ;\
	mulps	%xmm14, %xmm13 ;\
	mulps	60 * SIZE + 2 * (xx) * SIZE(BO), %xmm14 ;\
	addps	%xmm13, %xmm6 ;\
	movaps	72 * SIZE + 2 * (xx) * SIZE(BO), %xmm13 ;\
	addps	%xmm14, %xmm7 ;\
	movaps	44 * SIZE + 1 * (xx) * SIZE(AO), %xmm14

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

	pxor	%xmm15, %xmm15
	cmpeqps	%xmm15, %xmm15
	pslld	$31, %xmm15	# Generate mask
	pxor	%xmm2, %xmm2

#ifndef CONJ
	movss	  %xmm15,  0 + POSINV
	movss	  %xmm2,   4 + POSINV
	movss	  %xmm15,  8 + POSINV
	movss	  %xmm2,  12 + POSINV
#else
	movss	  %xmm2,   0 + POSINV
	movss	  %xmm15,  4 + POSINV
	movss	  %xmm2,   8 + POSINV
	movss	  %xmm15, 12 + POSINV
#endif

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

	movq	N,  J
	sarq	$1, J		# j = (n >> 2)
	jle	.L40
	ALIGN_4

.L01:
#ifdef LN
	movq	OFFSET, %rax
	addq	M, %rax
	movq	%rax, KK
#endif

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
	movaps	 0 * SIZE(B), %xmm3
	movaps	 4 * SIZE(B), %xmm7

	pshufd	 $0x00, %xmm3, %xmm0
	pshufd	 $0x55, %xmm3, %xmm1
	pshufd	 $0xaa, %xmm3, %xmm2
	pshufd	 $0xff, %xmm3, %xmm3

	movaps	%xmm0,  0 * SIZE(BO)
	movaps	%xmm1,  4 * SIZE(BO)
	movaps	%xmm2,  8 * SIZE(BO)
	movaps	%xmm3, 12 * SIZE(BO)

	pshufd	 $0x00, %xmm7, %xmm4
	pshufd	 $0x55, %xmm7, %xmm5
	pshufd	 $0xaa, %xmm7, %xmm6
	pshufd	 $0xff, %xmm7, %xmm7

	movaps	%xmm4, 16 * SIZE(BO)
	movaps	%xmm5, 20 * SIZE(BO)
	movaps	%xmm6, 24 * SIZE(BO)
	movaps	%xmm7, 28 * SIZE(BO)

	movaps	 8 * SIZE(B), %xmm3
	movaps	12 * SIZE(B), %xmm7

	pshufd	 $0x00, %xmm3, %xmm0
	pshufd	 $0x55, %xmm3, %xmm1
	pshufd	 $0xaa, %xmm3, %xmm2
	pshufd	 $0xff, %xmm3, %xmm3

	movaps	%xmm0, 32 * SIZE(BO)
	movaps	%xmm1, 36 * SIZE(BO)
	movaps	%xmm2, 40 * SIZE(BO)
	movaps	%xmm3, 44 * SIZE(BO)

	pshufd	 $0x00, %xmm7, %xmm4
	pshufd	 $0x55, %xmm7, %xmm5
	pshufd	 $0xaa, %xmm7, %xmm6
	pshufd	 $0xff, %xmm7, %xmm7

	movaps	%xmm4, 48 * SIZE(BO)
	movaps	%xmm5, 52 * SIZE(BO)
	movaps	%xmm6, 56 * SIZE(BO)
	movaps	%xmm7, 60 * SIZE(BO)

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
       leaq	(, LDC, 2), %rax
       subq	%rax, C
#endif

	movq	C, CO1			# coffset1 = c
	leaq	(C, LDC, 1), CO2

#ifndef RT
	leaq	(C, LDC, 2), C
#endif

	testq	$1, M
	je	.L20

#ifdef LN
       movq	K, %rax
       salq	$ZBASE_SHIFT, %rax
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
	salq	$1 + ZBASE_SHIFT, %rax
	leaq	(BO, %rax, 4), BO
#endif

	movlps	 0 * SIZE(AO), %xmm8
	movhps	 2 * SIZE(AO), %xmm8
	movlps	 8 * SIZE(AO), %xmm10
	movhps	10 * SIZE(AO), %xmm10

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
	movaps	  4 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm1
	movaps	  8 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm2
	movaps	 12 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	movsd	  2 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm3
	movaps	 64 * SIZE(BO), %xmm9

	mulps	%xmm8, %xmm11
	addps	%xmm11, %xmm0
	movaps	 20 * SIZE(BO), %xmm11
	mulps	%xmm8, %xmm11
	addps	%xmm11, %xmm1
	movaps	 24 * SIZE(BO), %xmm11
	mulps	%xmm8, %xmm11
	addps	%xmm11, %xmm2
	movaps	 28 * SIZE(BO), %xmm11
	mulps	%xmm8, %xmm11
	movsd	  4 * SIZE(AO), %xmm8
	addps	%xmm11, %xmm3
	movaps	 80 * SIZE(BO), %xmm11

	mulps	%xmm8, %xmm13
	addps	%xmm13, %xmm0
	movaps	 36 * SIZE(BO), %xmm13
	mulps	%xmm8, %xmm13
	addps	%xmm13, %xmm1
	movaps	 40 * SIZE(BO), %xmm13
	mulps	%xmm8, %xmm13
	addps	%xmm13, %xmm2
	movaps	 44 * SIZE(BO), %xmm13
	mulps	%xmm8, %xmm13
	movsd	  6 * SIZE(AO), %xmm8
	addps	%xmm13, %xmm3
	movaps	 96 * SIZE(BO), %xmm13

	mulps	%xmm8, %xmm15
	addps	%xmm15, %xmm0
	movaps	 52 * SIZE(BO), %xmm15
	mulps	%xmm8, %xmm15
	addps	%xmm15, %xmm1
	movaps	 56 * SIZE(BO), %xmm15
	mulps	%xmm8, %xmm15
	addps	%xmm15, %xmm2
	movaps	 60 * SIZE(BO), %xmm15
	mulps	%xmm8, %xmm15
	movsd	 16 * SIZE(AO), %xmm8
	addps	%xmm15, %xmm3
	movaps	112 * SIZE(BO), %xmm15

	mulps	%xmm10, %xmm9
	addps	%xmm9, %xmm0
	movaps	 68 * SIZE(BO), %xmm9
	mulps	%xmm10, %xmm9
	addps	%xmm9, %xmm1
	movaps	 72 * SIZE(BO), %xmm9
	mulps	%xmm10, %xmm9
	addps	%xmm9, %xmm2
	movaps	 76 * SIZE(BO), %xmm9
	mulps	%xmm10, %xmm9
	movsd	 10 * SIZE(AO), %xmm10
	addps	%xmm9, %xmm3
	movaps	128 * SIZE(BO), %xmm9

	mulps	%xmm10, %xmm11
	addps	%xmm11, %xmm0
	movaps	 84 * SIZE(BO), %xmm11
	mulps	%xmm10, %xmm11
	addps	%xmm11, %xmm1
	movaps	 88 * SIZE(BO), %xmm11
	mulps	%xmm10, %xmm11
	addps	%xmm11, %xmm2
	movaps	 92 * SIZE(BO), %xmm11
	mulps	%xmm10, %xmm11
	movsd	 12 * SIZE(AO), %xmm10
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
	movsd	 14 * SIZE(AO), %xmm10
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
	movsd	 24 * SIZE(AO), %xmm10
	addps	%xmm15, %xmm3
	movaps	176 * SIZE(BO), %xmm15

	addq   $16  * SIZE, AO
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
	movaps	POSINV,  %xmm15
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
	shufps	$0xb1, %xmm1, %xmm1
	shufps	$0xb1, %xmm3, %xmm3

#if defined(LN) || defined(LT)
#ifndef CONJ
	xorps	 %xmm15, %xmm1
	xorps	 %xmm15, %xmm3
#else
	xorps	 %xmm15, %xmm0
	xorps	 %xmm15, %xmm2
#endif
#else
	xorps	 %xmm15, %xmm1
	xorps	 %xmm15, %xmm3
#endif

	addps	%xmm1, %xmm0
	addps	%xmm3, %xmm2

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
	leaq	(BO, %rax, 8), BO
#endif

#if defined(LN) || defined(LT)
	unpcklpd %xmm2, %xmm0

	movaps	 0 * SIZE(B), %xmm2

	subps	%xmm0,  %xmm2
#else
#ifdef	movsd
	xorps	%xmm1, %xmm1
#endif
	movsd	 0 * SIZE(AO), %xmm1
#ifdef	movsd
	xorps	%xmm5, %xmm5
#endif
	movsd	 2 * SIZE(AO), %xmm5

	subps	%xmm0,  %xmm1
	subps	%xmm2,  %xmm5
#endif


#ifdef LN
	movaps	 0 * SIZE(AO), %xmm8

	pshufd	 $0x44, %xmm8, %xmm9
	pshufd	 $0x11, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm2, %xmm0
	pshufd	 $0xf5, %xmm2, %xmm2

#ifndef CONJ
	xorps	 %xmm15, %xmm2
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm2
	addps	%xmm0,  %xmm2
#endif

#ifdef LT
	movaps	 0 * SIZE(AO), %xmm8

	pshufd	 $0x44, %xmm8, %xmm9
	pshufd	 $0x11, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm2, %xmm0
	pshufd	 $0xf5, %xmm2, %xmm2

#ifndef CONJ
	xorps	 %xmm15, %xmm2
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm2
	addps	%xmm0,  %xmm2
#endif

#ifdef RN
	movaps	 0 * SIZE(B), %xmm8

	pshufd	 $0x44, %xmm8, %xmm9
	pshufd	 $0x11, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm1, %xmm0
	pshufd	 $0xf5, %xmm1, %xmm1

#ifndef CONJ
	xorps	 %xmm15, %xmm1
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm1

	addps	%xmm0,  %xmm1

	pshufd	 $0xee, %xmm8, %xmm9
	pshufd	 $0xbb, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm1, %xmm0
	pshufd	 $0xf5, %xmm1, %xmm2

#ifndef CONJ
	xorps	 %xmm15, %xmm2
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm2

	subps	%xmm0,  %xmm5
	subps	%xmm2,  %xmm5

	movaps	 4 * SIZE(B), %xmm8

	pshufd	 $0xee, %xmm8, %xmm9
	pshufd	 $0xbb, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm5, %xmm4
	pshufd	 $0xf5, %xmm5, %xmm5

#ifndef CONJ
	xorps	 %xmm15, %xmm5
#else
	xorps	 %xmm15, %xmm4
#endif

	mulps	%xmm9,  %xmm4
	mulps	%xmm10, %xmm5

	addps	%xmm4,  %xmm5
#endif

#ifdef RT
	movaps	 4 * SIZE(B), %xmm8

	pshufd	 $0xee, %xmm8, %xmm9
	pshufd	 $0xbb, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm5, %xmm0
	pshufd	 $0xf5, %xmm5, %xmm5

#ifndef CONJ
	xorps	 %xmm15, %xmm5
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm5

	addps	%xmm0,  %xmm5

	pshufd	 $0x44, %xmm8, %xmm9
	pshufd	 $0x11, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm5, %xmm0
	pshufd	 $0xf5, %xmm5, %xmm2

#ifndef CONJ
	xorps	 %xmm15, %xmm2
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm2

	subps	%xmm0,  %xmm1
	subps	%xmm2,  %xmm1

	movaps	 0 * SIZE(B), %xmm8

	pshufd	 $0x44, %xmm8, %xmm9
	pshufd	 $0x11, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm1, %xmm4
	pshufd	 $0xf5, %xmm1, %xmm1

#ifndef CONJ
	xorps	 %xmm15, %xmm1
#else
	xorps	 %xmm15, %xmm4
#endif

	mulps	%xmm9,  %xmm4
	mulps	%xmm10, %xmm1

	addps	%xmm4,  %xmm1
#endif

#ifdef LN
	subq	$2 * SIZE, CO1
	subq	$2 * SIZE, CO2
#endif

#if defined(LN) || defined(LT)
	movaps	%xmm2,   0 * SIZE(B)

	pshufd	$0x00, %xmm2, %xmm0
	pshufd	$0x55, %xmm2, %xmm1
	pshufd	$0xaa, %xmm2, %xmm4
	pshufd	$0xff, %xmm2, %xmm5

	movaps	%xmm0,   0 * SIZE(BO)
	movaps	%xmm1,   4 * SIZE(BO)
	movaps	%xmm4,   8 * SIZE(BO)
	movaps	%xmm5,  12 * SIZE(BO)

	movlps	%xmm2,   0 * SIZE(CO1)
	movhps	%xmm2,   0 * SIZE(CO2)
#else
	movlps	%xmm1,   0 * SIZE(AO)
	movlps	%xmm5,   2 * SIZE(AO)

	movlps	%xmm1,   0 * SIZE(CO1)
	movlps	%xmm5,   0 * SIZE(CO2)
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
	salq	$ZBASE_SHIFT, %rax
	addq	%rax, AORIG
#endif
	ALIGN_4

.L20:
	testq	$2, M
	je	.L30

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
	movaps	  4 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm1
	movaps	  8 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	mulps	 12 * SIZE(BO), %xmm8
	addps	%xmm9, %xmm2
	movaps	 64 * SIZE(BO), %xmm9
	addps	%xmm8, %xmm3
	movaps	  4 * SIZE(AO), %xmm8

	mulps	%xmm8, %xmm11
	addps	%xmm11, %xmm0
	movaps	 20 * SIZE(BO), %xmm11
	mulps	%xmm8, %xmm11
	addps	%xmm11, %xmm1
	movaps	 24 * SIZE(BO), %xmm11
	mulps	%xmm8, %xmm11
	mulps	 28 * SIZE(BO), %xmm8
	addps	%xmm11, %xmm2
	movaps	 80 * SIZE(BO), %xmm11
	addps	%xmm8, %xmm3
	movaps	  8 * SIZE(AO), %xmm8

	mulps	%xmm8, %xmm13
	addps	%xmm13, %xmm0
	movaps	 36 * SIZE(BO), %xmm13
	mulps	%xmm8, %xmm13
	addps	%xmm13, %xmm1
	movaps	 40 * SIZE(BO), %xmm13
	mulps	%xmm8, %xmm13
	mulps	 44 * SIZE(BO), %xmm8
	addps	%xmm13, %xmm2
	movaps	 96 * SIZE(BO), %xmm13
	addps	%xmm8, %xmm3
	movaps	 12 * SIZE(AO), %xmm8

	mulps	%xmm8, %xmm15
	addps	%xmm15, %xmm0
	movaps	 52 * SIZE(BO), %xmm15
	mulps	%xmm8, %xmm15
	addps	%xmm15, %xmm1
	movaps	 56 * SIZE(BO), %xmm15
	mulps	%xmm8, %xmm15
	mulps	 60 * SIZE(BO), %xmm8
	addps	%xmm15, %xmm2
	movaps	112 * SIZE(BO), %xmm15
	addps	%xmm8, %xmm3
	movaps	 32 * SIZE(AO), %xmm8

#if defined(OPTERON) && defined(HAVE_PREFETCH)
	PREFETCH	(PREFETCHSIZE + 16) * SIZE(AO)
#endif
	mulps	%xmm10, %xmm9
	addps	%xmm9, %xmm0
	movaps	 68 * SIZE(BO), %xmm9
	mulps	%xmm10, %xmm9
	addps	%xmm9, %xmm1
	movaps	 72 * SIZE(BO), %xmm9
	mulps	%xmm10, %xmm9
	mulps	 76 * SIZE(BO), %xmm10
	addps	%xmm9, %xmm2
	movaps	128 * SIZE(BO), %xmm9
	addps	%xmm10, %xmm3
	movaps	 20 * SIZE(AO), %xmm10

	mulps	%xmm10, %xmm11
	addps	%xmm11, %xmm0
	movaps	 84 * SIZE(BO), %xmm11
	mulps	%xmm10, %xmm11
	addps	%xmm11, %xmm1
	movaps	 88 * SIZE(BO), %xmm11
	mulps	%xmm10, %xmm11
	mulps	 92 * SIZE(BO), %xmm10
	addps	%xmm11, %xmm2
	movaps	144 * SIZE(BO), %xmm11
	addps	%xmm10, %xmm3
	movaps	 24 * SIZE(AO), %xmm10

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
	movaps	 28 * SIZE(AO), %xmm10

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
	movaps	 48 * SIZE(AO), %xmm10

	addq   $32  * SIZE, AO
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
	movaps	POSINV,  %xmm15
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
	movaps	 16 * SIZE(BO), %xmm9
	addps	%xmm8, %xmm3
	movaps	  4 * SIZE(AO), %xmm8

	addq	$ 4 * SIZE, AO		# aoffset  += 4
	addq	$16 * SIZE, BO		# boffset1 += 8
	decq	%rax
	jg	.L26
	ALIGN_4

.L28:
	shufps	$0xb1, %xmm1, %xmm1
	shufps	$0xb1, %xmm3, %xmm3

#if defined(LN) || defined(LT)
#ifndef CONJ
	xorps	 %xmm15, %xmm1
	xorps	 %xmm15, %xmm3
#else
	xorps	 %xmm15, %xmm0
	xorps	 %xmm15, %xmm2
#endif
#else
	xorps	 %xmm15, %xmm1
	xorps	 %xmm15, %xmm3
#endif

	addps	%xmm1, %xmm0
	addps	%xmm3, %xmm2

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
	leaq	(BO, %rax, 8), BO
#endif

#if defined(LN) || defined(LT)
	movaps	%xmm0, %xmm1
	unpcklpd %xmm2, %xmm0
	unpckhpd %xmm2, %xmm1

	movaps	 0 * SIZE(B), %xmm2
	movaps	 4 * SIZE(B), %xmm3

	subps	%xmm0,  %xmm2
	subps	%xmm1,  %xmm3
#else
	movaps	 0 * SIZE(AO), %xmm1
	movaps	 4 * SIZE(AO), %xmm5

	subps	%xmm0,  %xmm1
	subps	%xmm2,  %xmm5
#endif


#ifdef LN
	movaps	 4 * SIZE(AO), %xmm8

	pshufd	 $0xee, %xmm8, %xmm9
	pshufd	 $0xbb, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm3, %xmm0
	pshufd	 $0xf5, %xmm3, %xmm3

#ifndef CONJ
	xorps	 %xmm15, %xmm3
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm3
	addps	%xmm0,  %xmm3

	pshufd	 $0x44, %xmm8, %xmm9
	pshufd	 $0x11, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm3, %xmm0
	pshufd	 $0xf5, %xmm3, %xmm1

#ifndef CONJ
	xorps	 %xmm15, %xmm1
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm1
	subps	%xmm0,  %xmm2
	subps	%xmm1,  %xmm2

	movaps	 0 * SIZE(AO), %xmm8

	pshufd	 $0x44, %xmm8, %xmm9
	pshufd	 $0x11, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm2, %xmm0
	pshufd	 $0xf5, %xmm2, %xmm2

#ifndef CONJ
	xorps	 %xmm15, %xmm2
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm2
	addps	%xmm0,  %xmm2
#endif

#ifdef LT
	movaps	 0 * SIZE(AO), %xmm8

	pshufd	 $0x44, %xmm8, %xmm9
	pshufd	 $0x11, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm2, %xmm0
	pshufd	 $0xf5, %xmm2, %xmm2

#ifndef CONJ
	xorps	 %xmm15, %xmm2
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm2
	addps	%xmm0,  %xmm2

	pshufd	 $0xee, %xmm8, %xmm9
	pshufd	 $0xbb, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm2, %xmm0
	pshufd	 $0xf5, %xmm2, %xmm1

#ifndef CONJ
	xorps	 %xmm15, %xmm1
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm1
	subps	%xmm0,  %xmm3
	subps	%xmm1,  %xmm3

	movaps	 4 * SIZE(AO), %xmm8

	pshufd	 $0xee, %xmm8, %xmm9
	pshufd	 $0xbb, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm3, %xmm0
	pshufd	 $0xf5, %xmm3, %xmm3

#ifndef CONJ
	xorps	 %xmm15, %xmm3
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm3
	addps	%xmm0,  %xmm3
#endif

#ifdef RN
	movaps	 0 * SIZE(B), %xmm8

	pshufd	 $0x44, %xmm8, %xmm9
	pshufd	 $0x11, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm1, %xmm0
	pshufd	 $0xf5, %xmm1, %xmm1

#ifndef CONJ
	xorps	 %xmm15, %xmm1
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm1

	addps	%xmm0,  %xmm1

	pshufd	 $0xee, %xmm8, %xmm9
	pshufd	 $0xbb, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm1, %xmm0
	pshufd	 $0xf5, %xmm1, %xmm2

#ifndef CONJ
	xorps	 %xmm15, %xmm2
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm2

	subps	%xmm0,  %xmm5
	subps	%xmm2,  %xmm5

	movaps	 4 * SIZE(B), %xmm8

	pshufd	 $0xee, %xmm8, %xmm9
	pshufd	 $0xbb, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm5, %xmm4
	pshufd	 $0xf5, %xmm5, %xmm5

#ifndef CONJ
	xorps	 %xmm15, %xmm5
#else
	xorps	 %xmm15, %xmm4
#endif

	mulps	%xmm9,  %xmm4
	mulps	%xmm10, %xmm5

	addps	%xmm4,  %xmm5
#endif

#ifdef RT
	movaps	 4 * SIZE(B), %xmm8

	pshufd	 $0xee, %xmm8, %xmm9
	pshufd	 $0xbb, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm5, %xmm0
	pshufd	 $0xf5, %xmm5, %xmm5

#ifndef CONJ
	xorps	 %xmm15, %xmm5
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm5

	addps	%xmm0,  %xmm5

	pshufd	 $0x44, %xmm8, %xmm9
	pshufd	 $0x11, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm5, %xmm0
	pshufd	 $0xf5, %xmm5, %xmm2

#ifndef CONJ
	xorps	 %xmm15, %xmm2
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm2

	subps	%xmm0,  %xmm1
	subps	%xmm2,  %xmm1

	movaps	 0 * SIZE(B), %xmm8

	pshufd	 $0x44, %xmm8, %xmm9
	pshufd	 $0x11, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm1, %xmm4
	pshufd	 $0xf5, %xmm1, %xmm1

#ifndef CONJ
	xorps	 %xmm15, %xmm1
#else
	xorps	 %xmm15, %xmm4
#endif

	mulps	%xmm9,  %xmm4
	mulps	%xmm10, %xmm1

	addps	%xmm4,  %xmm1
#endif

#ifdef LN
	subq	$4 * SIZE, CO1
	subq	$4 * SIZE, CO2
#endif

#if defined(LN) || defined(LT)
	movaps	%xmm2,   0 * SIZE(B)
	movaps	%xmm3,   4 * SIZE(B)

	pshufd	$0x00, %xmm2, %xmm0
	pshufd	$0x55, %xmm2, %xmm1
	pshufd	$0xaa, %xmm2, %xmm4
	pshufd	$0xff, %xmm2, %xmm5

	movaps	%xmm0,   0 * SIZE(BO)
	movaps	%xmm1,   4 * SIZE(BO)
	movaps	%xmm4,   8 * SIZE(BO)
	movaps	%xmm5,  12 * SIZE(BO)

	pshufd	$0x00, %xmm3, %xmm0
	pshufd	$0x55, %xmm3, %xmm1
	pshufd	$0xaa, %xmm3, %xmm4
	pshufd	$0xff, %xmm3, %xmm5

	movaps	%xmm0,  16 * SIZE(BO)
	movaps	%xmm1,  20 * SIZE(BO)
	movaps	%xmm4,  24 * SIZE(BO)
	movaps	%xmm5,  28 * SIZE(BO)

	movlps	%xmm2,   0 * SIZE(CO1)
	movlps	%xmm3,   2 * SIZE(CO1)
	movhps	%xmm2,   0 * SIZE(CO2)
	movhps	%xmm3,   2 * SIZE(CO2)
#else
	movaps	%xmm1,   0 * SIZE(AO)
	movaps	%xmm5,   4 * SIZE(AO)

	movlps	%xmm1,   0 * SIZE(CO1)
	movhps	%xmm1,   2 * SIZE(CO1)

	movlps	%xmm5,   0 * SIZE(CO2)
	movhps	%xmm5,   2 * SIZE(CO2)
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
	ALIGN_4

.L30:
	movq	M,  I
	sarq	$2, I		# i = (m >> 2)
	jle	.L39
	ALIGN_4

.L11:
#ifdef LN
       movq	K, %rax
       salq	$2 + ZBASE_SHIFT, %rax
       subq	%rax, AORIG
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	movq	AORIG, AO
	salq	$ZBASE_SHIFT, %rax
	leaq	(AO, %rax, 4), AO
#endif

	leaq	BUFFER, BO

#if defined(LN) || defined(RT)
	movq	KK, %rax
	salq	$1 + ZBASE_SHIFT, %rax
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
	pxor	%xmm6, %xmm6
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
	KERNEL1(32 *  0)
	KERNEL2(32 *  0)
	KERNEL3(32 *  0)
	KERNEL4(32 *  0)
	KERNEL5(32 *  0)
	KERNEL6(32 *  0)
	KERNEL7(32 *  0)
	KERNEL8(32 *  0)
	KERNEL1(32 *  1)
	KERNEL2(32 *  1)
	KERNEL3(32 *  1)
	KERNEL4(32 *  1)
	KERNEL5(32 *  1)
	KERNEL6(32 *  1)
	KERNEL7(32 *  1)
	KERNEL8(32 *  1)

	addq	$32 * 2  * SIZE, AO
	addq	$64 * 2  * SIZE, BO
	subq	$64 * 2, %rax
	jg	.L1X

.L12:
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 4), BO
	ALIGN_4

.L15:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	movaps	POSINV,  %xmm15
	andq	$7, %rax		# if (k & 1)
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

	addq	$ 8 * SIZE, AO		# aoffset  += 4
	addq	$16 * SIZE, BO		# boffset1 += 8
	decq	%rax
	jg	.L16
	ALIGN_4

.L18:
	shufps	$0xb1, %xmm1, %xmm1
	shufps	$0xb1, %xmm3, %xmm3
	shufps	$0xb1, %xmm5, %xmm5
	shufps	$0xb1, %xmm7, %xmm7

#if defined(LN) || defined(LT)
#ifndef CONJ
	xorps	 %xmm15, %xmm1
	xorps	 %xmm15, %xmm3
	xorps	 %xmm15, %xmm5
	xorps	 %xmm15, %xmm7
#else
	xorps	 %xmm15, %xmm0
	xorps	 %xmm15, %xmm2
	xorps	 %xmm15, %xmm4
	xorps	 %xmm15, %xmm6
#endif
#else
	xorps	 %xmm15, %xmm1
	xorps	 %xmm15, %xmm3
	xorps	 %xmm15, %xmm5
	xorps	 %xmm15, %xmm7
#endif

	addps	%xmm1, %xmm0
	addps	%xmm3, %xmm2
	addps	%xmm5, %xmm4
	addps	%xmm7, %xmm6

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

	salq	$ZBASE_SHIFT, %rax
	leaq	(AO, %rax, 4), AO
	leaq	(B,  %rax, 2), B
	leaq	(BO, %rax, 8), BO
#endif

#if defined(LN) || defined(LT)
	movaps	%xmm0, %xmm1
	unpcklpd %xmm2, %xmm0
	unpckhpd %xmm2, %xmm1

	movaps	%xmm4, %xmm5
	unpcklpd %xmm6, %xmm4
	unpckhpd %xmm6, %xmm5

	movaps	 0 * SIZE(B), %xmm2
	movaps	 4 * SIZE(B), %xmm3
	movaps	 8 * SIZE(B), %xmm6
	movaps	12 * SIZE(B), %xmm7

	subps	%xmm0,  %xmm2
	subps	%xmm1,  %xmm3
	subps	%xmm4,  %xmm6
	subps	%xmm5,  %xmm7
#else
	movaps	 0 * SIZE(AO), %xmm1
	movaps	 4 * SIZE(AO), %xmm3
	movaps	 8 * SIZE(AO), %xmm5
	movaps	12 * SIZE(AO), %xmm7

	subps	%xmm0,  %xmm1
	subps	%xmm4,  %xmm3
	subps	%xmm2,  %xmm5
	subps	%xmm6,  %xmm7
#endif

#ifdef LN
	movaps	28 * SIZE(AO), %xmm8

	pshufd	 $0xee, %xmm8, %xmm9
	pshufd	 $0xbb, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm7, %xmm0
	pshufd	 $0xf5, %xmm7, %xmm7

#ifndef CONJ
	xorps	 %xmm15, %xmm7
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm7
	addps	%xmm0,  %xmm7

	pshufd	 $0x44, %xmm8, %xmm9
	pshufd	 $0x11, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm7, %xmm0
	pshufd	 $0xf5, %xmm7, %xmm1

#ifndef CONJ
	xorps	 %xmm15, %xmm1
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm1
	subps	%xmm0,  %xmm6
	subps	%xmm1,  %xmm6

	movaps	24 * SIZE(AO), %xmm8

	pshufd	 $0xee, %xmm8, %xmm9
	pshufd	 $0xbb, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm7, %xmm0
	pshufd	 $0xf5, %xmm7, %xmm1

#ifndef CONJ
	xorps	 %xmm15, %xmm1
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm1
	subps	%xmm0,  %xmm3
	subps	%xmm1,  %xmm3

	pshufd	 $0x44, %xmm8, %xmm9
	pshufd	 $0x11, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm7, %xmm0
	pshufd	 $0xf5, %xmm7, %xmm1

#ifndef CONJ
	xorps	 %xmm15, %xmm1
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm1
	subps	%xmm0,  %xmm2
	subps	%xmm1,  %xmm2

	movaps	20 * SIZE(AO), %xmm8

	pshufd	 $0x44, %xmm8, %xmm9
	pshufd	 $0x11, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm6, %xmm0
	pshufd	 $0xf5, %xmm6, %xmm6

#ifndef CONJ
	xorps	 %xmm15, %xmm6
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm6
	addps	%xmm0,  %xmm6

	movaps	16 * SIZE(AO), %xmm8

	pshufd	 $0xee, %xmm8, %xmm9
	pshufd	 $0xbb, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm6, %xmm0
	pshufd	 $0xf5, %xmm6, %xmm1

#ifndef CONJ
	xorps	 %xmm15, %xmm1
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm1
	subps	%xmm0,  %xmm3
	subps	%xmm1,  %xmm3

	pshufd	 $0x44, %xmm8, %xmm9
	pshufd	 $0x11, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm6, %xmm0
	pshufd	 $0xf5, %xmm6, %xmm1

#ifndef CONJ
	xorps	 %xmm15, %xmm1
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm1
	subps	%xmm0,  %xmm2
	subps	%xmm1,  %xmm2

	movaps	 8 * SIZE(AO), %xmm8

	pshufd	 $0xee, %xmm8, %xmm9
	pshufd	 $0xbb, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm3, %xmm0
	pshufd	 $0xf5, %xmm3, %xmm3

#ifndef CONJ
	xorps	 %xmm15, %xmm3
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm3
	addps	%xmm0,  %xmm3

	pshufd	 $0x44, %xmm8, %xmm9
	pshufd	 $0x11, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm3, %xmm0
	pshufd	 $0xf5, %xmm3, %xmm1

#ifndef CONJ
	xorps	 %xmm15, %xmm1
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm1
	subps	%xmm0,  %xmm2
	subps	%xmm1,  %xmm2

	movaps	 0 * SIZE(AO), %xmm8

	pshufd	 $0x44, %xmm8, %xmm9
	pshufd	 $0x11, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm2, %xmm0
	pshufd	 $0xf5, %xmm2, %xmm2

#ifndef CONJ
	xorps	 %xmm15, %xmm2
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm2
	addps	%xmm0,  %xmm2
#endif

#ifdef LT
	movaps	 0 * SIZE(AO), %xmm8

	pshufd	 $0x44, %xmm8, %xmm9
	pshufd	 $0x11, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm2, %xmm0
	pshufd	 $0xf5, %xmm2, %xmm2

#ifndef CONJ
	xorps	 %xmm15, %xmm2
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm2
	addps	%xmm0,  %xmm2

	pshufd	 $0xee, %xmm8, %xmm9
	pshufd	 $0xbb, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm2, %xmm0
	pshufd	 $0xf5, %xmm2, %xmm1

#ifndef CONJ
	xorps	 %xmm15, %xmm1
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm1
	subps	%xmm0,  %xmm3
	subps	%xmm1,  %xmm3

	movaps	 4 * SIZE(AO), %xmm8

	pshufd	 $0x44, %xmm8, %xmm9
	pshufd	 $0x11, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm2, %xmm0
	pshufd	 $0xf5, %xmm2, %xmm1

#ifndef CONJ
	xorps	 %xmm15, %xmm1
#else
	xorps	 %xmm15, %xmm0
#endif
	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm1
	subps	%xmm0,  %xmm6
	subps	%xmm1,  %xmm6

	pshufd	 $0xee, %xmm8, %xmm9
	pshufd	 $0xbb, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm2, %xmm0
	pshufd	 $0xf5, %xmm2, %xmm1

#ifndef CONJ
	xorps	 %xmm15, %xmm1
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm1
	subps	%xmm0,  %xmm7
	subps	%xmm1,  %xmm7

	movaps	 8 * SIZE(AO), %xmm8

	pshufd	 $0xee, %xmm8, %xmm9
	pshufd	 $0xbb, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm3, %xmm0
	pshufd	 $0xf5, %xmm3, %xmm3

#ifndef CONJ
	xorps	 %xmm15, %xmm3
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm3
	addps	%xmm0,  %xmm3

	movaps	 12 * SIZE(AO), %xmm8

	pshufd	 $0x44, %xmm8, %xmm9
	pshufd	 $0x11, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm3, %xmm0
	pshufd	 $0xf5, %xmm3, %xmm1

#ifndef CONJ
	xorps	 %xmm15, %xmm1
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm1
	subps	%xmm0,  %xmm6
	subps	%xmm1,  %xmm6

	pshufd	 $0xee, %xmm8, %xmm9
	pshufd	 $0xbb, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm3, %xmm0
	pshufd	 $0xf5, %xmm3, %xmm1

#ifndef CONJ
	xorps	 %xmm15, %xmm1
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm1
	subps	%xmm0,  %xmm7
	subps	%xmm1,  %xmm7

	movaps	20 * SIZE(AO), %xmm8

	pshufd	 $0x44, %xmm8, %xmm9
	pshufd	 $0x11, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm6, %xmm0
	pshufd	 $0xf5, %xmm6, %xmm6

#ifndef CONJ
	xorps	 %xmm15, %xmm6
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm6
	addps	%xmm0,  %xmm6

	pshufd	 $0xee, %xmm8, %xmm9
	pshufd	 $0xbb, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm6, %xmm0
	pshufd	 $0xf5, %xmm6, %xmm1

#ifndef CONJ
	xorps	 %xmm15, %xmm1
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm1
	subps	%xmm0,  %xmm7
	subps	%xmm1,  %xmm7

	movaps	28 * SIZE(AO), %xmm8

	pshufd	 $0xee, %xmm8, %xmm9
	pshufd	 $0xbb, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm7, %xmm0
	pshufd	 $0xf5, %xmm7, %xmm7

#ifndef CONJ
	xorps	 %xmm15, %xmm7
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm7
	addps	%xmm0,  %xmm7
#endif

#ifdef RN
	movaps	 0 * SIZE(B), %xmm8

	pshufd	 $0x44, %xmm8, %xmm9
	pshufd	 $0x11, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm1, %xmm0
	pshufd	 $0xf5, %xmm1, %xmm1

	pshufd	 $0xa0, %xmm3, %xmm2
	pshufd	 $0xf5, %xmm3, %xmm3

#ifndef CONJ
	xorps	 %xmm15, %xmm1
	xorps	 %xmm15, %xmm3
#else
	xorps	 %xmm15, %xmm0
	xorps	 %xmm15, %xmm2
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm9,  %xmm2
	mulps	%xmm10, %xmm1
	mulps	%xmm10, %xmm3

	addps	%xmm0,  %xmm1
	addps	%xmm2,  %xmm3

	pshufd	 $0xee, %xmm8, %xmm9
	pshufd	 $0xbb, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm1, %xmm0
	pshufd	 $0xf5, %xmm1, %xmm2

	pshufd	 $0xa0, %xmm3, %xmm4
	pshufd	 $0xf5, %xmm3, %xmm6

#ifndef CONJ
	xorps	 %xmm15, %xmm2
	xorps	 %xmm15, %xmm6
#else
	xorps	 %xmm15, %xmm0
	xorps	 %xmm15, %xmm4
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm9,  %xmm4

	mulps	%xmm10, %xmm2
	mulps	%xmm10, %xmm6

	subps	%xmm0,  %xmm5
	subps	%xmm4,  %xmm7

	subps	%xmm2,  %xmm5
	subps	%xmm6,  %xmm7

	movaps	 4 * SIZE(B), %xmm8

	pshufd	 $0xee, %xmm8, %xmm9
	pshufd	 $0xbb, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm5, %xmm4
	pshufd	 $0xf5, %xmm5, %xmm5

	pshufd	 $0xa0, %xmm7, %xmm6
	pshufd	 $0xf5, %xmm7, %xmm7

#ifndef CONJ
	xorps	 %xmm15, %xmm5
	xorps	 %xmm15, %xmm7
#else
	xorps	 %xmm15, %xmm4
	xorps	 %xmm15, %xmm6
#endif

	mulps	%xmm9,  %xmm4
	mulps	%xmm9,  %xmm6
	mulps	%xmm10, %xmm5
	mulps	%xmm10, %xmm7

	addps	%xmm4,  %xmm5
	addps	%xmm6,  %xmm7
#endif

#ifdef RT
	movaps	 4 * SIZE(B), %xmm8

	pshufd	 $0xee, %xmm8, %xmm9
	pshufd	 $0xbb, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm5, %xmm0
	pshufd	 $0xf5, %xmm5, %xmm5

	pshufd	 $0xa0, %xmm7, %xmm2
	pshufd	 $0xf5, %xmm7, %xmm7

#ifndef CONJ
	xorps	 %xmm15, %xmm5
	xorps	 %xmm15, %xmm7
#else
	xorps	 %xmm15, %xmm0
	xorps	 %xmm15, %xmm2
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm9,  %xmm2
	mulps	%xmm10, %xmm5
	mulps	%xmm10, %xmm7

	addps	%xmm0,  %xmm5
	addps	%xmm2,  %xmm7

	pshufd	 $0x44, %xmm8, %xmm9
	pshufd	 $0x11, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm5, %xmm0
	pshufd	 $0xf5, %xmm5, %xmm2

	pshufd	 $0xa0, %xmm7, %xmm4
	pshufd	 $0xf5, %xmm7, %xmm6

#ifndef CONJ
	xorps	 %xmm15, %xmm2
	xorps	 %xmm15, %xmm6
#else
	xorps	 %xmm15, %xmm0
	xorps	 %xmm15, %xmm4
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm9,  %xmm4

	mulps	%xmm10, %xmm2
	mulps	%xmm10, %xmm6

	subps	%xmm0,  %xmm1
	subps	%xmm4,  %xmm3

	subps	%xmm2,  %xmm1
	subps	%xmm6,  %xmm3

	movaps	 0 * SIZE(B), %xmm8

	pshufd	 $0x44, %xmm8, %xmm9
	pshufd	 $0x11, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm1, %xmm4
	pshufd	 $0xf5, %xmm1, %xmm1

	pshufd	 $0xa0, %xmm3, %xmm6
	pshufd	 $0xf5, %xmm3, %xmm3

#ifndef CONJ
	xorps	 %xmm15, %xmm1
	xorps	 %xmm15, %xmm3
#else
	xorps	 %xmm15, %xmm4
	xorps	 %xmm15, %xmm6
#endif

	mulps	%xmm9,  %xmm4
	mulps	%xmm9,  %xmm6
	mulps	%xmm10, %xmm1
	mulps	%xmm10, %xmm3

	addps	%xmm4,  %xmm1
	addps	%xmm6,  %xmm3

#endif

#ifdef LN
	subq	$8 * SIZE, CO1
	subq	$8 * SIZE, CO2
#endif

#if defined(LN) || defined(LT)
	movaps	%xmm2,   0 * SIZE(B)
	movaps	%xmm3,   4 * SIZE(B)
	movaps	%xmm6,   8 * SIZE(B)
	movaps	%xmm7,  12 * SIZE(B)

	pshufd	$0x00, %xmm2, %xmm0
	pshufd	$0x55, %xmm2, %xmm1
	pshufd	$0xaa, %xmm2, %xmm4
	pshufd	$0xff, %xmm2, %xmm5

	movaps	%xmm0,   0 * SIZE(BO)
	movaps	%xmm1,   4 * SIZE(BO)
	movaps	%xmm4,   8 * SIZE(BO)
	movaps	%xmm5,  12 * SIZE(BO)

	pshufd	$0x00, %xmm3, %xmm0
	pshufd	$0x55, %xmm3, %xmm1
	pshufd	$0xaa, %xmm3, %xmm4
	pshufd	$0xff, %xmm3, %xmm5

	movaps	%xmm0,  16 * SIZE(BO)
	movaps	%xmm1,  20 * SIZE(BO)
	movaps	%xmm4,  24 * SIZE(BO)
	movaps	%xmm5,  28 * SIZE(BO)

	pshufd	$0x00, %xmm6, %xmm0
	pshufd	$0x55, %xmm6, %xmm1
	pshufd	$0xaa, %xmm6, %xmm4
	pshufd	$0xff, %xmm6, %xmm5

	movaps	%xmm0,  32 * SIZE(BO)
	movaps	%xmm1,  36 * SIZE(BO)
	movaps	%xmm4,  40 * SIZE(BO)
	movaps	%xmm5,  44 * SIZE(BO)

	pshufd	$0x00, %xmm7, %xmm0
	pshufd	$0x55, %xmm7, %xmm1
	pshufd	$0xaa, %xmm7, %xmm4
	pshufd	$0xff, %xmm7, %xmm5

	movaps	%xmm0,  48 * SIZE(BO)
	movaps	%xmm1,  52 * SIZE(BO)
	movaps	%xmm4,  56 * SIZE(BO)
	movaps	%xmm5,  60 * SIZE(BO)

	movlps	%xmm2,   0 * SIZE(CO1)
	movlps	%xmm3,   2 * SIZE(CO1)
	movlps	%xmm6,   4 * SIZE(CO1)
	movlps	%xmm7,   6 * SIZE(CO1)

	movhps	%xmm2,   0 * SIZE(CO2)
	movhps	%xmm3,   2 * SIZE(CO2)
	movhps	%xmm6,   4 * SIZE(CO2)
	movhps	%xmm7,   6 * SIZE(CO2)
#else
	movaps	%xmm1,   0 * SIZE(AO)
	movaps	%xmm3,   4 * SIZE(AO)
	movaps	%xmm5,   8 * SIZE(AO)
	movaps	%xmm7,  12 * SIZE(AO)

	movlps	%xmm1,   0 * SIZE(CO1)
	movhps	%xmm1,   2 * SIZE(CO1)
	movlps	%xmm3,   4 * SIZE(CO1)
	movhps	%xmm3,   6 * SIZE(CO1)

	movlps	%xmm5,   0 * SIZE(CO2)
	movhps	%xmm5,   2 * SIZE(CO2)
	movlps	%xmm7,   4 * SIZE(CO2)
	movhps	%xmm7,   6 * SIZE(CO2)
#endif


#ifndef LN
	addq	$8 * SIZE, CO1
	addq	$8 * SIZE, CO2
#endif

#if defined(LT) || defined(RN)
	movq	K,  %rax
	subq	KK, %rax
	salq	$ZBASE_SHIFT, %rax
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
	salq	$2 + ZBASE_SHIFT, %rax
	addq	%rax, AORIG
#endif

	decq	I			# i --
	jg	.L11
	ALIGN_4

.L39:
#ifdef LN
       leaq	(, K, SIZE), %rax
       leaq	(B, %rax, 2 * COMPSIZE), B
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
	ALIGN_4

.L40:
	testq	$1, N
	je	.L999
	ALIGN_4

#ifdef LN
	movq	OFFSET, %rax
	addq	M, %rax
	movq	%rax, KK
#endif

	leaq	BUFFER, BO

#ifdef RT
       movq	K, %rax
       salq	$ZBASE_SHIFT, %rax
       subq	%rax, B
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	movq	B, BORIG
	salq	$ZBASE_SHIFT, %rax
	leaq	(B,  %rax, 1), B
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
	jle	.L43
	ALIGN_4

.L42:
	movaps	 0 * SIZE(B), %xmm3
	movaps	 4 * SIZE(B), %xmm7

	pshufd	 $0x00, %xmm3, %xmm0
	pshufd	 $0x55, %xmm3, %xmm1
	pshufd	 $0xaa, %xmm3, %xmm2
	pshufd	 $0xff, %xmm3, %xmm3

	movaps	%xmm0,  0 * SIZE(BO)
	movaps	%xmm1,  4 * SIZE(BO)
	movaps	%xmm2,  8 * SIZE(BO)
	movaps	%xmm3, 12 * SIZE(BO)

	pshufd	 $0x00, %xmm7, %xmm4
	pshufd	 $0x55, %xmm7, %xmm5
	pshufd	 $0xaa, %xmm7, %xmm6
	pshufd	 $0xff, %xmm7, %xmm7

	movaps	%xmm4, 16 * SIZE(BO)
	movaps	%xmm5, 20 * SIZE(BO)
	movaps	%xmm6, 24 * SIZE(BO)
	movaps	%xmm7, 28 * SIZE(BO)

	addq	$ 8 * SIZE, B
	addq	$32 * SIZE, BO
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
	movsd	 0 * SIZE(B), %xmm3

	pshufd	 $0x00, %xmm3, %xmm0
	pshufd	 $0x55, %xmm3, %xmm1

	movaps	%xmm0,  0 * SIZE(BO)
	movaps	%xmm1,  4 * SIZE(BO)

	addq	$2 * SIZE, B
	addq	$8 * SIZE, BO
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
       subq	LDC, C
#endif

	movq	C, CO1			# coffset1 = c

#ifndef RT
	addq	LDC, C
#endif

	testq	$1, M
	je	.L60

#ifdef LN
       movq	K, %rax
       salq	$ZBASE_SHIFT, %rax
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
	salq	$ZBASE_SHIFT, %rax
	leaq	(BO, %rax, 4), BO
#endif

	movsd	 0 * SIZE(AO), %xmm8
	movhps	 2 * SIZE(AO), %xmm8
	movsd	 8 * SIZE(AO), %xmm10
	movhps	10 * SIZE(AO), %xmm10

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
	jne    .L72
	ALIGN_4

.L75:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	movaps	POSINV,  %xmm15
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L78
	ALIGN_4

.L76:
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
	jg	.L76
	ALIGN_4

.L78:
	addps	%xmm2, %xmm0
	addps	%xmm3, %xmm1

	shufps	$0xb1, %xmm1, %xmm1

#if defined(LN) || defined(LT)
#ifndef CONJ
	xorps	 %xmm15, %xmm1
#else
	xorps	 %xmm15, %xmm0
#endif
#else
	xorps	 %xmm15, %xmm1
#endif

	addps	%xmm1, %xmm0

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
	leaq	(BO, %rax, 4), BO
#endif

#if defined(LN) || defined(LT)
#ifdef	movsd
	xorps	%xmm2, %xmm2
#endif
	movsd	 0 * SIZE(B), %xmm2

	subps	%xmm0,  %xmm2
#else
#ifdef	movsd
	xorps	%xmm1, %xmm1
#endif
	movsd	 0 * SIZE(AO), %xmm1

	subps	%xmm0,  %xmm1
#endif

#if defined(LN) || defined(LT)
	movaps	 0 * SIZE(AO), %xmm8

	pshufd	 $0x44, %xmm8, %xmm9
	pshufd	 $0x11, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm2, %xmm0
	pshufd	 $0xf5, %xmm2, %xmm2

#ifndef CONJ
	xorps	 %xmm15, %xmm2
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm2
	addps	%xmm0,  %xmm2
#endif

#if defined(RN) || defined(RT)
	movaps	 0 * SIZE(B), %xmm8

	pshufd	 $0x44, %xmm8, %xmm9
	pshufd	 $0x11, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm1, %xmm4
	pshufd	 $0xf5, %xmm1, %xmm1

#ifndef CONJ
	xorps	 %xmm15, %xmm1
#else
	xorps	 %xmm15, %xmm4
#endif

	mulps	%xmm9,  %xmm4
	mulps	%xmm10, %xmm1

	addps	%xmm4,  %xmm1
#endif

#ifdef LN
	subq	$2 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	movlps	%xmm2,   0 * SIZE(B)

	pshufd	$0x00, %xmm2, %xmm0
	pshufd	$0x55, %xmm2, %xmm1

	movaps	%xmm0,   0 * SIZE(BO)
	movaps	%xmm1,   4 * SIZE(BO)

	movlps	%xmm2,   0 * SIZE(CO1)
#else
	movlps	%xmm1,   0 * SIZE(AO)

	movlps	%xmm1,   0 * SIZE(CO1)
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
	salq	$ZBASE_SHIFT, %rax
	addq	%rax, AORIG
#endif
	ALIGN_4

.L60:
	testq	$2, M
	je	.L70

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
	salq	$ZBASE_SHIFT, %rax
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
	je	.L65
	ALIGN_4

.L62:
	mulps	%xmm8, %xmm9
#if defined(OPTERON) && defined(HAVE_PREFETCH)
	PREFETCH	(PREFETCHSIZE +  0) * SIZE(AO)
#endif

	mulps	 4 * SIZE(BO), %xmm8
	addps	%xmm9, %xmm0
	movaps	 8 * SIZE(BO), %xmm9
	addps	%xmm8, %xmm1
	movaps   4 * SIZE(AO), %xmm8
	mulps	%xmm8, %xmm9
	mulps	12 * SIZE(BO), %xmm8
	addps	%xmm9, %xmm2
	movaps	64 * SIZE(BO), %xmm9
	addps	%xmm8, %xmm3
	movaps   8 * SIZE(AO), %xmm8

	mulps	%xmm8, %xmm11
	mulps	20 * SIZE(BO), %xmm8
	addps	%xmm11, %xmm0
	movaps	24 * SIZE(BO), %xmm11
	addps	%xmm8, %xmm1
	movaps  12 * SIZE(AO), %xmm8
	mulps	%xmm8, %xmm11
	mulps	28 * SIZE(BO), %xmm8
	addps	%xmm11, %xmm2
	movaps	 80 * SIZE(BO), %xmm11
	addps	%xmm8, %xmm3
	movaps  32 * SIZE(AO), %xmm8

#if defined(OPTERON) && defined(HAVE_PREFETCH)
	PREFETCH	(PREFETCHSIZE + 16) * SIZE(AO)
#endif
	mulps	%xmm10, %xmm13
	mulps	36 * SIZE(BO), %xmm10
	addps	%xmm13, %xmm0
	movaps	40 * SIZE(BO), %xmm13
	addps	%xmm10, %xmm1
	movaps  20 * SIZE(AO), %xmm10
	mulps	%xmm10, %xmm13
	mulps	44 * SIZE(BO), %xmm10
	addps	%xmm13, %xmm2
	movaps	 96 * SIZE(BO), %xmm13
	addps	%xmm10, %xmm3
	movaps  24 * SIZE(AO), %xmm10

	mulps	%xmm10, %xmm15
	mulps	52 * SIZE(BO), %xmm10
	addps	%xmm15, %xmm0
	movaps	56 * SIZE(BO), %xmm15
	addps	%xmm10, %xmm1
	movaps  28 * SIZE(AO), %xmm10
	mulps	%xmm10, %xmm15
	mulps	60 * SIZE(BO), %xmm10
	addps	%xmm15, %xmm2
	movaps	112 * SIZE(BO), %xmm15
	addps	%xmm10, %xmm3
	movaps  48 * SIZE(AO), %xmm10

	addq   $32 * SIZE, AO
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
	movaps	POSINV,  %xmm15
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L68
	ALIGN_4

.L66:
	mulps	%xmm8, %xmm9
	mulps	 4 * SIZE(BO), %xmm8
	addps	%xmm9, %xmm0
	movaps	 8 * SIZE(BO), %xmm9
	addps	%xmm8, %xmm1
	movaps   4 * SIZE(AO), %xmm8

	addq	$4 * SIZE, AO		# aoffset  += 4
	addq	$8 * SIZE, BO		# boffset1 += 8
	decq	%rax
	jg	.L66
	ALIGN_4

.L68:
	addps	%xmm2, %xmm0
	addps	%xmm3, %xmm1

	shufps	$0xb1, %xmm1, %xmm1

#if defined(LN) || defined(LT)
#ifndef CONJ
	xorps	 %xmm15, %xmm1
#else
	xorps	 %xmm15, %xmm0
#endif
#else
	xorps	 %xmm15, %xmm1
#endif

	addps	%xmm1, %xmm0

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
	leaq	(BO, %rax, 4), BO
#endif

#if defined(LN) || defined(LT)
	movaps	%xmm0, %xmm1
	unpcklpd %xmm2, %xmm0
	unpckhpd %xmm2, %xmm1

#ifdef	movsd
	xorps	%xmm2, %xmm2
#endif
	movsd	 0 * SIZE(B), %xmm2
#ifdef	movsd
	xorps	%xmm3, %xmm3
#endif
	movsd	 2 * SIZE(B), %xmm3

	subps	%xmm0,  %xmm2
	subps	%xmm1,  %xmm3
#else
	movaps	 0 * SIZE(AO), %xmm1
	subps	%xmm0,  %xmm1
#endif

#ifdef LN
	movaps	 4 * SIZE(AO), %xmm8

	pshufd	 $0xee, %xmm8, %xmm9
	pshufd	 $0xbb, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm3, %xmm0
	pshufd	 $0xf5, %xmm3, %xmm3

#ifndef CONJ
	xorps	 %xmm15, %xmm3
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm3
	addps	%xmm0,  %xmm3

	pshufd	 $0x44, %xmm8, %xmm9
	pshufd	 $0x11, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm3, %xmm0
	pshufd	 $0xf5, %xmm3, %xmm1

#ifndef CONJ
	xorps	 %xmm15, %xmm1
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm1
	subps	%xmm0,  %xmm2
	subps	%xmm1,  %xmm2

	movaps	 0 * SIZE(AO), %xmm8

	pshufd	 $0x44, %xmm8, %xmm9
	pshufd	 $0x11, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm2, %xmm0
	pshufd	 $0xf5, %xmm2, %xmm2

#ifndef CONJ
	xorps	 %xmm15, %xmm2
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm2
	addps	%xmm0,  %xmm2
#endif

#ifdef LT
	movaps	 0 * SIZE(AO), %xmm8

	pshufd	 $0x44, %xmm8, %xmm9
	pshufd	 $0x11, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm2, %xmm0
	pshufd	 $0xf5, %xmm2, %xmm2

#ifndef CONJ
	xorps	 %xmm15, %xmm2
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm2
	addps	%xmm0,  %xmm2

	pshufd	 $0xee, %xmm8, %xmm9
	pshufd	 $0xbb, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm2, %xmm0
	pshufd	 $0xf5, %xmm2, %xmm1

#ifndef CONJ
	xorps	 %xmm15, %xmm1
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm1
	subps	%xmm0,  %xmm3
	subps	%xmm1,  %xmm3

	movaps	 4 * SIZE(AO), %xmm8

	pshufd	 $0xee, %xmm8, %xmm9
	pshufd	 $0xbb, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm3, %xmm0
	pshufd	 $0xf5, %xmm3, %xmm3

#ifndef CONJ
	xorps	 %xmm15, %xmm3
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm3
	addps	%xmm0,  %xmm3
#endif

#if defined(RN) || defined(RT)
	movaps	 0 * SIZE(B), %xmm8

	pshufd	 $0x44, %xmm8, %xmm9
	pshufd	 $0x11, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm1, %xmm4
	pshufd	 $0xf5, %xmm1, %xmm1

#ifndef CONJ
	xorps	 %xmm15, %xmm1
#else
	xorps	 %xmm15, %xmm4
#endif

	mulps	%xmm9,  %xmm4
	mulps	%xmm10, %xmm1

	addps	%xmm4,  %xmm1
#endif

#ifdef LN
	subq	$4 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	movlps	%xmm2,   0 * SIZE(B)
	movlps	%xmm3,   2 * SIZE(B)

	pshufd	$0x00, %xmm2, %xmm0
	pshufd	$0x55, %xmm2, %xmm1

	movaps	%xmm0,   0 * SIZE(BO)
	movaps	%xmm1,   4 * SIZE(BO)

	pshufd	$0x00, %xmm3, %xmm0
	pshufd	$0x55, %xmm3, %xmm1

	movaps	%xmm0,   8 * SIZE(BO)
	movaps	%xmm1,  12 * SIZE(BO)

	movlps	%xmm2,   0 * SIZE(CO1)
	movlps	%xmm3,   2 * SIZE(CO1)
#else
	movaps	%xmm1,   0 * SIZE(AO)

	movlps	%xmm1,   0 * SIZE(CO1)
	movhps	%xmm1,   2 * SIZE(CO1)
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
	ALIGN_4

.L70:
	movq	M,  I
	sarq	$2, I		# i = (m >> 2)
	jle	.L79
	ALIGN_4

.L51:
#ifdef LN
       movq	K, %rax
       salq	$2 + ZBASE_SHIFT, %rax
       subq	%rax, AORIG
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	movq	AORIG, AO
	salq	$ZBASE_SHIFT, %rax
	leaq	(AO, %rax, 4), AO
#endif

	leaq	BUFFER, BO

#if defined(LN) || defined(RT)
	movq	KK, %rax
	salq	$ZBASE_SHIFT, %rax
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
	pxor	%xmm2, %xmm2
	pxor	%xmm3, %xmm3

	pxor	%xmm4, %xmm4
	pxor	%xmm5, %xmm5
	pxor	%xmm6, %xmm6
	pxor	%xmm7, %xmm7

	PREFETCHW     -8 * SIZE(CO1)

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
	movaps	 80 * SIZE(BO), %xmm11
	addps	%xmm10, %xmm5
	movaps	 80 * SIZE(AO), %xmm10

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
	movaps	 96 * SIZE(BO), %xmm13
	addps	%xmm12, %xmm5
	movaps	 96 * SIZE(AO), %xmm12

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
	jne    .L52
	ALIGN_4

.L55:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	movaps	POSINV,  %xmm15
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L58
	ALIGN_4

.L56:
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

	addq	$ 8 * SIZE, AO		# aoffset  += 4
	addq	$ 8 * SIZE, BO		# boffset1 += 8
	decq	%rax
	jg	.L56
	ALIGN_4

.L58:
	shufps	$0xb1, %xmm1, %xmm1
	shufps	$0xb1, %xmm5, %xmm5

#if defined(LN) || defined(LT)
#ifndef CONJ
	xorps	 %xmm15, %xmm1
	xorps	 %xmm15, %xmm5
#else
	xorps	 %xmm15, %xmm0
	xorps	 %xmm15, %xmm4
#endif
#else
	xorps	 %xmm15, %xmm1
	xorps	 %xmm15, %xmm5
#endif

	addps	%xmm1, %xmm0
	addps	%xmm5, %xmm4

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

	salq	$ZBASE_SHIFT, %rax
	leaq	(AO, %rax, 4), AO
	leaq	(B,  %rax, 1), B
	leaq	(BO, %rax, 4), BO
#endif

#if defined(LN) || defined(LT)
	movaps	%xmm0, %xmm1
	unpcklpd %xmm2, %xmm0
	unpckhpd %xmm2, %xmm1

	movaps	%xmm4, %xmm5
	unpcklpd %xmm6, %xmm4
	unpckhpd %xmm6, %xmm5

#ifdef	movsd
	xorps	%xmm2, %xmm2
#endif
	movsd	 0 * SIZE(B), %xmm2
#ifdef	movsd
	xorps	%xmm3, %xmm3
#endif
	movsd	 2 * SIZE(B), %xmm3
#ifdef	movsd
	xorps	%xmm6, %xmm6
#endif
	movsd	 4 * SIZE(B), %xmm6
#ifdef	movsd
	xorps	%xmm7, %xmm7
#endif
	movsd	 6 * SIZE(B), %xmm7

	subps	%xmm0,  %xmm2
	subps	%xmm1,  %xmm3
	subps	%xmm4,  %xmm6
	subps	%xmm5,  %xmm7
#else
	movaps	 0 * SIZE(AO), %xmm1
	movaps	 4 * SIZE(AO), %xmm3

	subps	%xmm0,  %xmm1
	subps	%xmm4,  %xmm3
#endif

#ifdef LN
	movaps	28 * SIZE(AO), %xmm8

	pshufd	 $0xee, %xmm8, %xmm9
	pshufd	 $0xbb, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm7, %xmm0
	pshufd	 $0xf5, %xmm7, %xmm7

#ifndef CONJ
	xorps	 %xmm15, %xmm7
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm7
	addps	%xmm0,  %xmm7

	pshufd	 $0x44, %xmm8, %xmm9
	pshufd	 $0x11, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm7, %xmm0
	pshufd	 $0xf5, %xmm7, %xmm1

#ifndef CONJ
	xorps	 %xmm15, %xmm1
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm1
	subps	%xmm0,  %xmm6
	subps	%xmm1,  %xmm6

	movaps	24 * SIZE(AO), %xmm8

	pshufd	 $0xee, %xmm8, %xmm9
	pshufd	 $0xbb, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm7, %xmm0
	pshufd	 $0xf5, %xmm7, %xmm1

#ifndef CONJ
	xorps	 %xmm15, %xmm1
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm1
	subps	%xmm0,  %xmm3
	subps	%xmm1,  %xmm3

	pshufd	 $0x44, %xmm8, %xmm9
	pshufd	 $0x11, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm7, %xmm0
	pshufd	 $0xf5, %xmm7, %xmm1

#ifndef CONJ
	xorps	 %xmm15, %xmm1
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm1
	subps	%xmm0,  %xmm2
	subps	%xmm1,  %xmm2

	movaps	20 * SIZE(AO), %xmm8

	pshufd	 $0x44, %xmm8, %xmm9
	pshufd	 $0x11, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm6, %xmm0
	pshufd	 $0xf5, %xmm6, %xmm6

#ifndef CONJ
	xorps	 %xmm15, %xmm6
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm6
	addps	%xmm0,  %xmm6

	movaps	16 * SIZE(AO), %xmm8

	pshufd	 $0xee, %xmm8, %xmm9
	pshufd	 $0xbb, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm6, %xmm0
	pshufd	 $0xf5, %xmm6, %xmm1

#ifndef CONJ
	xorps	 %xmm15, %xmm1
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm1
	subps	%xmm0,  %xmm3
	subps	%xmm1,  %xmm3

	pshufd	 $0x44, %xmm8, %xmm9
	pshufd	 $0x11, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm6, %xmm0
	pshufd	 $0xf5, %xmm6, %xmm1

#ifndef CONJ
	xorps	 %xmm15, %xmm1
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm1
	subps	%xmm0,  %xmm2
	subps	%xmm1,  %xmm2

	movaps	 8 * SIZE(AO), %xmm8

	pshufd	 $0xee, %xmm8, %xmm9
	pshufd	 $0xbb, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm3, %xmm0
	pshufd	 $0xf5, %xmm3, %xmm3

#ifndef CONJ
	xorps	 %xmm15, %xmm3
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm3
	addps	%xmm0,  %xmm3

	pshufd	 $0x44, %xmm8, %xmm9
	pshufd	 $0x11, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm3, %xmm0
	pshufd	 $0xf5, %xmm3, %xmm1

#ifndef CONJ
	xorps	 %xmm15, %xmm1
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm1
	subps	%xmm0,  %xmm2
	subps	%xmm1,  %xmm2

	movaps	 0 * SIZE(AO), %xmm8

	pshufd	 $0x44, %xmm8, %xmm9
	pshufd	 $0x11, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm2, %xmm0
	pshufd	 $0xf5, %xmm2, %xmm2

#ifndef CONJ
	xorps	 %xmm15, %xmm2
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm2
	addps	%xmm0,  %xmm2
#endif

#ifdef LT
	movaps	 0 * SIZE(AO), %xmm8

	pshufd	 $0x44, %xmm8, %xmm9
	pshufd	 $0x11, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm2, %xmm0
	pshufd	 $0xf5, %xmm2, %xmm2

#ifndef CONJ
	xorps	 %xmm15, %xmm2
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm2
	addps	%xmm0,  %xmm2

	pshufd	 $0xee, %xmm8, %xmm9
	pshufd	 $0xbb, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm2, %xmm0
	pshufd	 $0xf5, %xmm2, %xmm1

#ifndef CONJ
	xorps	 %xmm15, %xmm1
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm1
	subps	%xmm0,  %xmm3
	subps	%xmm1,  %xmm3

	movaps	 4 * SIZE(AO), %xmm8

	pshufd	 $0x44, %xmm8, %xmm9
	pshufd	 $0x11, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm2, %xmm0
	pshufd	 $0xf5, %xmm2, %xmm1

#ifndef CONJ
	xorps	 %xmm15, %xmm1
#else
	xorps	 %xmm15, %xmm0
#endif
	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm1
	subps	%xmm0,  %xmm6
	subps	%xmm1,  %xmm6

	pshufd	 $0xee, %xmm8, %xmm9
	pshufd	 $0xbb, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm2, %xmm0
	pshufd	 $0xf5, %xmm2, %xmm1

#ifndef CONJ
	xorps	 %xmm15, %xmm1
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm1
	subps	%xmm0,  %xmm7
	subps	%xmm1,  %xmm7

	movaps	 8 * SIZE(AO), %xmm8

	pshufd	 $0xee, %xmm8, %xmm9
	pshufd	 $0xbb, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm3, %xmm0
	pshufd	 $0xf5, %xmm3, %xmm3

#ifndef CONJ
	xorps	 %xmm15, %xmm3
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm3
	addps	%xmm0,  %xmm3

	movaps	 12 * SIZE(AO), %xmm8

	pshufd	 $0x44, %xmm8, %xmm9
	pshufd	 $0x11, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm3, %xmm0
	pshufd	 $0xf5, %xmm3, %xmm1

#ifndef CONJ
	xorps	 %xmm15, %xmm1
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm1
	subps	%xmm0,  %xmm6
	subps	%xmm1,  %xmm6

	pshufd	 $0xee, %xmm8, %xmm9
	pshufd	 $0xbb, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm3, %xmm0
	pshufd	 $0xf5, %xmm3, %xmm1

#ifndef CONJ
	xorps	 %xmm15, %xmm1
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm1
	subps	%xmm0,  %xmm7
	subps	%xmm1,  %xmm7

	movaps	20 * SIZE(AO), %xmm8

	pshufd	 $0x44, %xmm8, %xmm9
	pshufd	 $0x11, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm6, %xmm0
	pshufd	 $0xf5, %xmm6, %xmm6

#ifndef CONJ
	xorps	 %xmm15, %xmm6
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm6
	addps	%xmm0,  %xmm6

	pshufd	 $0xee, %xmm8, %xmm9
	pshufd	 $0xbb, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm6, %xmm0
	pshufd	 $0xf5, %xmm6, %xmm1

#ifndef CONJ
	xorps	 %xmm15, %xmm1
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm1
	subps	%xmm0,  %xmm7
	subps	%xmm1,  %xmm7

	movaps	28 * SIZE(AO), %xmm8

	pshufd	 $0xee, %xmm8, %xmm9
	pshufd	 $0xbb, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm7, %xmm0
	pshufd	 $0xf5, %xmm7, %xmm7

#ifndef CONJ
	xorps	 %xmm15, %xmm7
#else
	xorps	 %xmm15, %xmm0
#endif

	mulps	%xmm9,  %xmm0
	mulps	%xmm10, %xmm7
	addps	%xmm0,  %xmm7
#endif

#if defined(RN) || defined(RT)
	movaps	 0 * SIZE(B), %xmm8

	pshufd	 $0x44, %xmm8, %xmm9
	pshufd	 $0x11, %xmm8, %xmm10

	pshufd	 $0xa0, %xmm1, %xmm4
	pshufd	 $0xf5, %xmm1, %xmm1

	pshufd	 $0xa0, %xmm3, %xmm6
	pshufd	 $0xf5, %xmm3, %xmm3

#ifndef CONJ
	xorps	 %xmm15, %xmm1
	xorps	 %xmm15, %xmm3
#else
	xorps	 %xmm15, %xmm4
	xorps	 %xmm15, %xmm6
#endif

	mulps	%xmm9,  %xmm4
	mulps	%xmm9,  %xmm6
	mulps	%xmm10, %xmm1
	mulps	%xmm10, %xmm3

	addps	%xmm4,  %xmm1
	addps	%xmm6,  %xmm3
#endif

#ifdef LN
	subq	$8 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	movlps	%xmm2,   0 * SIZE(B)
	movlps	%xmm3,   2 * SIZE(B)
	movlps	%xmm6,   4 * SIZE(B)
	movlps	%xmm7,   6 * SIZE(B)

	pshufd	$0x00, %xmm2, %xmm0
	pshufd	$0x55, %xmm2, %xmm1

	movaps	%xmm0,   0 * SIZE(BO)
	movaps	%xmm1,   4 * SIZE(BO)

	pshufd	$0x00, %xmm3, %xmm0
	pshufd	$0x55, %xmm3, %xmm1

	movaps	%xmm0,   8 * SIZE(BO)
	movaps	%xmm1,  12 * SIZE(BO)

	pshufd	$0x00, %xmm6, %xmm0
	pshufd	$0x55, %xmm6, %xmm1

	movaps	%xmm0,  16 * SIZE(BO)
	movaps	%xmm1,  20 * SIZE(BO)

	pshufd	$0x00, %xmm7, %xmm0
	pshufd	$0x55, %xmm7, %xmm1

	movaps	%xmm0,  24 * SIZE(BO)
	movaps	%xmm1,  28 * SIZE(BO)

	movlps	%xmm2,   0 * SIZE(CO1)
	movlps	%xmm3,   2 * SIZE(CO1)
	movlps	%xmm6,   4 * SIZE(CO1)
	movlps	%xmm7,   6 * SIZE(CO1)
#else
	movaps	%xmm1,   0 * SIZE(AO)
	movaps	%xmm3,   4 * SIZE(AO)

	movlps	%xmm1,   0 * SIZE(CO1)
	movhps	%xmm1,   2 * SIZE(CO1)
	movlps	%xmm3,   4 * SIZE(CO1)
	movhps	%xmm3,   6 * SIZE(CO1)
#endif

#ifndef LN
	addq	$8 * SIZE, CO1
#endif

#if defined(LT) || defined(RN)
	movq	K,  %rax
	subq	KK, %rax
	salq	$ZBASE_SHIFT, %rax
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
	salq	$2 + ZBASE_SHIFT, %rax
	addq	%rax, AORIG
#endif

	decq	I			# i --
	jg	.L51
	ALIGN_4

.L79:
#ifdef LN
       leaq	(, K, SIZE), %rax
       leaq	(B, %rax, COMPSIZE), B
#endif

#if defined(LT) || defined(RN)
	movq	K,  %rax
	subq	KK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(B,  %rax, COMPSIZE), B
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
