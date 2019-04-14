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
#define	CO1	%r15
#define CO2	%rbp
#define BB	%r12

#ifndef WINDOWS_ABI

#define STACKSIZE 64

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
#define J	 48(%rsp)
#define OFFSET	 56(%rsp)
#define KK	 64(%rsp)
#define KKK	 72(%rsp)
#define BUFFER  256(%rsp)

#ifdef OPTERON
#define movsd movlps
#endif

#if defined(OPTERON) || defined(BARCELONA) || defined(SHANGHAI)
#define PREFETCH     prefetch
#define PREFETCHW    prefetchw
#define PREFETCHSIZE (16 * 5 + 8)
#endif

#if defined(PENTIUM4) || defined(GENERIC)
#define PREFETCH     prefetcht0
#define PREFETCHW    prefetcht0
#define PREFETCHSIZE  160
#endif

#define RPREFETCHSIZE (8 *  7 + 4)
#define WPREFETCHSIZE (8 *  8 + 4)

#ifndef GENERIC
#define KERNEL1(xx) \
	mulps	%xmm0, %xmm1 ;\
	addps	%xmm1, %xmm8 ;\
	movaps	-32 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm1 ;\
	mulps	%xmm0, %xmm3 ;\
	addps	%xmm3, %xmm9 ;\
	movaps	-28 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm3 ;\
	mulps	%xmm0, %xmm5 ;\
	PREFETCH	(PREFETCHSIZE     +  0) * SIZE + 1 * (xx) * SIZE(AO, %rax, 4) ;\
	mulps	-20 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm0 ;\
	addps	%xmm5, %xmm10 ;\
	movaps	-24 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm5 ;\
	addps	%xmm0, %xmm11 ;\
	movaps	-16 * SIZE + 1 * (xx) * SIZE(AO, %rax, 4), %xmm0

#define KERNEL2(xx) \
	mulps	%xmm2, %xmm1 ;\
	addps	%xmm1, %xmm12 ;\
	movaps	  0 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm1 ;\
	mulps	%xmm2, %xmm3 ;\
	addps	%xmm3, %xmm13 ;\
	movaps	-12 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm3 ;\
	mulps	%xmm2, %xmm5 ;\
	mulps	-20 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm2 ;\
	addps	%xmm5, %xmm14 ;\
	movaps	 -8 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm5 ;\
	addps	%xmm2, %xmm15 ;\
	movaps	-12 * SIZE + 1 * (xx) * SIZE(AO, %rax, 4), %xmm2

#define KERNEL3(xx) \
	mulps	%xmm4, %xmm7 ;\
	addps	%xmm7, %xmm8 ;\
	movaps	-16 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm7 ;\
	mulps	%xmm4, %xmm3 ;\
	addps	%xmm3, %xmm9 ;\
	movaps	-12 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm3 ;\
	mulps	%xmm4, %xmm5 ;\
	mulps	 -4 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm4 ;\
	addps	%xmm5, %xmm10 ;\
	movaps	 -8 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm5 ;\
	addps	%xmm4, %xmm11 ;\
	movaps	 -8 * SIZE + 1 * (xx) * SIZE(AO, %rax, 4), %xmm4

#define KERNEL4(xx) \
	mulps	%xmm6, %xmm7 ;\
	addps	%xmm7, %xmm12 ;\
	movaps	 16 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm7 ;\
	mulps	%xmm6, %xmm3 ;\
	addps	%xmm3, %xmm13 ;\
	movaps	  4 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm3 ;\
	mulps	%xmm6, %xmm5 ;\
	mulps	 -4 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm6 ;\
	addps	%xmm5, %xmm14 ;\
	movaps	  8 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm5 ;\
 	PREFETCH	(PREFETCHSIZE     + 16) * SIZE + 1 * (xx) * SIZE(AO, %rax, 4) ;\
	addps	%xmm6, %xmm15 ;\
	movaps	 -4 * SIZE + 1 * (xx) * SIZE(AO, %rax, 4), %xmm6

#define KERNEL5(xx) \
	mulps	%xmm0, %xmm1 ;\
	addps	%xmm1, %xmm8 ;\
	movaps	  0 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm1 ;\
	mulps	%xmm0, %xmm3 ;\
	addps	%xmm3, %xmm9 ;\
	movaps	  4 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm3 ;\
	mulps	%xmm0, %xmm5 ;\
	mulps	 12 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm0 ;\
	addps	%xmm5, %xmm10 ;\
	movaps	  8 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm5 ;\
	addps	%xmm0, %xmm11 ;\
	movaps	  0 * SIZE + 1 * (xx) * SIZE(AO, %rax, 4), %xmm0

#define KERNEL6(xx) \
	mulps	%xmm2, %xmm1 ;\
	addps	%xmm1, %xmm12 ;\
	movaps	 32 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm1 ;\
	mulps	%xmm2, %xmm3 ;\
	addps	%xmm3, %xmm13 ;\
	movaps	 20 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm3 ;\
	mulps	%xmm2, %xmm5 ;\
	mulps	 12 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm2 ;\
	addps	%xmm5, %xmm14 ;\
	movaps	 24 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm5 ;\
	addps	%xmm2, %xmm15 ;\
	movaps	  4 * SIZE + 1 * (xx) * SIZE(AO, %rax, 4), %xmm2

#define KERNEL7(xx) \
	mulps	%xmm4, %xmm7 ;\
	addps	%xmm7, %xmm8 ;\
	movaps	 16 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm7 ;\
	mulps	%xmm4, %xmm3 ;\
	addps	%xmm3, %xmm9 ;\
	movaps	 20 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm3 ;\
	mulps	%xmm4, %xmm5 ;\
	mulps	 28 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm4 ;\
	addps	%xmm5, %xmm10 ;\
	movaps	 24 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm5 ;\
	addps	%xmm4, %xmm11 ;\
	movaps	  8 * SIZE + 1 * (xx) * SIZE(AO, %rax, 4), %xmm4

#define KERNEL8(xx) \
	mulps	%xmm6, %xmm7 ;\
	addps	%xmm7, %xmm12 ;\
	movaps	 48 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm7 ;\
	mulps	%xmm6, %xmm3 ;\
	addps	%xmm3, %xmm13 ;\
	movaps	 36 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm3 ;\
	mulps	%xmm6, %xmm5 ;\
	mulps	 28 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm6 ;\
	addps	%xmm5, %xmm14 ;\
	movaps	 40 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm5 ;\
	addps	%xmm6, %xmm15 ;\
	movaps	 12 * SIZE + 1 * (xx) * SIZE(AO, %rax, 4), %xmm6

#else

#define KERNEL1(xx) \
	mulps	%xmm0, %xmm1 ;\
	addps	%xmm1, %xmm8 ;\
	movaps	-32 * SIZE + 2 * (xx) * SIZE(BO), %xmm1 ;\
	mulps	%xmm0, %xmm3 ;\
	addps	%xmm3, %xmm9 ;\
	movaps	-28 * SIZE + 2 * (xx) * SIZE(BO), %xmm3 ;\
	mulps	%xmm0, %xmm5 ;\
	PREFETCH	(PREFETCHSIZE     +  0) * SIZE + 1 * (xx) * SIZE(AO) ;\
	mulps	-20 * SIZE + 2 * (xx) * SIZE(BO), %xmm0 ;\
	addps	%xmm5, %xmm10 ;\
	movaps	-24 * SIZE + 2 * (xx) * SIZE(BO), %xmm5 ;\
	addps	%xmm0, %xmm11 ;\
	movaps	-16 * SIZE + 1 * (xx) * SIZE(AO), %xmm0

#define KERNEL2(xx) \
	mulps	%xmm2, %xmm1 ;\
	addps	%xmm1, %xmm12 ;\
	movaps	  0 * SIZE + 2 * (xx) * SIZE(BO), %xmm1 ;\
	mulps	%xmm2, %xmm3 ;\
	addps	%xmm3, %xmm13 ;\
	movaps	-12 * SIZE + 2 * (xx) * SIZE(BO), %xmm3 ;\
	mulps	%xmm2, %xmm5 ;\
	mulps	-20 * SIZE + 2 * (xx) * SIZE(BO), %xmm2 ;\
	addps	%xmm5, %xmm14 ;\
	movaps	 -8 * SIZE + 2 * (xx) * SIZE(BO), %xmm5 ;\
	addps	%xmm2, %xmm15 ;\
	movaps	-12 * SIZE + 1 * (xx) * SIZE(AO), %xmm2 ;\

#define KERNEL3(xx) \
	mulps	%xmm4, %xmm7 ;\
	addps	%xmm7, %xmm8 ;\
	movaps	-16 * SIZE + 2 * (xx) * SIZE(BO), %xmm7 ;\
	mulps	%xmm4, %xmm3 ;\
	addps	%xmm3, %xmm9 ;\
	movaps	-12 * SIZE + 2 * (xx) * SIZE(BO), %xmm3 ;\
	mulps	%xmm4, %xmm5 ;\
	mulps	 -4 * SIZE + 2 * (xx) * SIZE(BO), %xmm4 ;\
	addps	%xmm5, %xmm10 ;\
	movaps	 -8 * SIZE + 2 * (xx) * SIZE(BO), %xmm5 ;\
	addps	%xmm4, %xmm11 ;\
	movaps	 -8 * SIZE + 1 * (xx) * SIZE(AO), %xmm4

#define KERNEL4(xx) \
	mulps	%xmm6, %xmm7 ;\
	addps	%xmm7, %xmm12 ;\
	movaps	 16 * SIZE + 2 * (xx) * SIZE(BO), %xmm7 ;\
	mulps	%xmm6, %xmm3 ;\
	addps	%xmm3, %xmm13 ;\
	movaps	  4 * SIZE + 2 * (xx) * SIZE(BO), %xmm3 ;\
	mulps	%xmm6, %xmm5 ;\
	mulps	 -4 * SIZE + 2 * (xx) * SIZE(BO), %xmm6 ;\
	addps	%xmm5, %xmm14 ;\
	movaps	  8 * SIZE + 2 * (xx) * SIZE(BO), %xmm5 ;\
	addps	%xmm6, %xmm15 ;\
	movaps	 -4 * SIZE + 1 * (xx) * SIZE(AO), %xmm6

#define KERNEL5(xx) \
	mulps	%xmm0, %xmm1 ;\
 	PREFETCH	(PREFETCHSIZE     + 16) * SIZE + 1 * (xx) * SIZE(AO) ;\
	addps	%xmm1, %xmm8 ;\
	movaps	  0 * SIZE + 2 * (xx) * SIZE(BO), %xmm1 ;\
	mulps	%xmm0, %xmm3 ;\
	addps	%xmm3, %xmm9 ;\
	movaps	  4 * SIZE + 2 * (xx) * SIZE(BO), %xmm3 ;\
	mulps	%xmm0, %xmm5 ;\
	mulps	 12 * SIZE + 2 * (xx) * SIZE(BO), %xmm0 ;\
	addps	%xmm5, %xmm10 ;\
	movaps	  8 * SIZE + 2 * (xx) * SIZE(BO), %xmm5 ;\
	addps	%xmm0, %xmm11 ;\
	movaps	  0 * SIZE + 1 * (xx) * SIZE(AO), %xmm0

#define KERNEL6(xx) \
	mulps	%xmm2, %xmm1 ;\
	addps	%xmm1, %xmm12 ;\
	movaps	 32 * SIZE + 2 * (xx) * SIZE(BO), %xmm1 ;\
	mulps	%xmm2, %xmm3 ;\
	addps	%xmm3, %xmm13 ;\
	movaps	 20 * SIZE + 2 * (xx) * SIZE(BO), %xmm3 ;\
	mulps	%xmm2, %xmm5 ;\
	mulps	 12 * SIZE + 2 * (xx) * SIZE(BO), %xmm2 ;\
	addps	%xmm5, %xmm14 ;\
	movaps	 24 * SIZE + 2 * (xx) * SIZE(BO), %xmm5 ;\
	addps	%xmm2, %xmm15 ;\
	movaps	  4 * SIZE + 1 * (xx) * SIZE(AO), %xmm2

#define KERNEL7(xx) \
	mulps	%xmm4, %xmm7 ;\
	addps	%xmm7, %xmm8 ;\
	movaps	 16 * SIZE + 2 * (xx) * SIZE(BO), %xmm7 ;\
	mulps	%xmm4, %xmm3 ;\
	addps	%xmm3, %xmm9 ;\
	movaps	 20 * SIZE + 2 * (xx) * SIZE(BO), %xmm3 ;\
	mulps	%xmm4, %xmm5 ;\
	mulps	 28 * SIZE + 2 * (xx) * SIZE(BO), %xmm4 ;\
	addps	%xmm5, %xmm10 ;\
	movaps	 24 * SIZE + 2 * (xx) * SIZE(BO), %xmm5 ;\
	addps	%xmm4, %xmm11 ;\
	movaps	  8 * SIZE + 1 * (xx) * SIZE(AO), %xmm4

#define KERNEL8(xx) \
	mulps	%xmm6, %xmm7 ;\
	addps	%xmm7, %xmm12 ;\
	movaps	 48 * SIZE + 2 * (xx) * SIZE(BO), %xmm7 ;\
	mulps	%xmm6, %xmm3 ;\
	addps	%xmm3, %xmm13 ;\
	movaps	 36 * SIZE + 2 * (xx) * SIZE(BO), %xmm3 ;\
	mulps	%xmm6, %xmm5 ;\
	mulps	 28 * SIZE + 2 * (xx) * SIZE(BO), %xmm6 ;\
	addps	%xmm5, %xmm14 ;\
	movaps	 40 * SIZE + 2 * (xx) * SIZE(BO), %xmm5 ;\
	addps	%xmm6, %xmm15 ;\
	movaps	 12 * SIZE + 1 * (xx) * SIZE(AO), %xmm6
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
#ifdef TRMMKERNEL
	movsd	OLD_OFFSET, %xmm12
#endif
	movaps	%xmm3,       %xmm0
	movsd	OLD_ALPHA_I, %xmm1
#else
	movq	72(%rsp), LDC
#ifdef TRMMKERNEL
	movsd	80(%rsp), %xmm12
#endif

#endif

	movq	%rsp, %rbx	# save old stack
	subq	$256 + LOCAL_BUFFER_SIZE, %rsp
	andq	$-4096, %rsp	# align stack

	STACK_TOUCHING

	movq	OLD_M, M
	movq	OLD_N, N

	pxor	%xmm7, %xmm7
	cmpeqps	%xmm7, %xmm7
	pslld	$31, %xmm7	# Generate mask
	pxor	%xmm10, %xmm10

	shufps	$0, %xmm0, %xmm0
	movaps	 %xmm0,  0 + ALPHA_R

	movss	 %xmm1,  4 + ALPHA_I
	movss	 %xmm1, 12 + ALPHA_I
	xorps	 %xmm7, %xmm1
	movss	 %xmm1,  0 + ALPHA_I
	movss	 %xmm1,  8 + ALPHA_I

#if   defined(NN) || defined(NT) || defined(NR) || defined(NC) || \
      defined(TN) || defined(TT) || defined(TR) || defined(TC)
	movss	  %xmm7,  0 + POSINV
	movss	  %xmm10, 4 + POSINV
	movss	  %xmm7,  8 + POSINV
	movss	  %xmm10,12 + POSINV
#else
	movss	  %xmm10, 0 + POSINV
	movss	  %xmm7,  4 + POSINV
	movss	  %xmm10, 8 + POSINV
	movss	  %xmm7, 12 + POSINV
#endif

	addq	$32 * SIZE, A

#ifdef TRMMKERNEL
	movsd	%xmm12, OFFSET
	movsd	%xmm12, KK
#ifndef LEFT
	negq	KK
#endif
#endif

	salq	$ZBASE_SHIFT, LDC
	movq	N,  J
	sarq	$1, J		# j = (n >> 2)
	jle	.L40
	ALIGN_4

.L01:
#if defined(TRMMKERNEL) && defined(LEFT)
	movq	OFFSET, %rax
	movq	%rax, KK
#endif

/* Copying to Sub Buffer */
	leaq	BUFFER, BO
	movaps	POSINV, %xmm7

	movq	K, %rax
	sarq	$2, %rax
	jle	.L03

	addq	%rax, %rax
	ALIGN_4

.L02:
	PREFETCH	 (RPREFETCHSIZE +  0)  * SIZE(B)

	movss	 0 * SIZE(B), %xmm8
	movss	 1 * SIZE(B), %xmm9
	movss	 2 * SIZE(B), %xmm10
	movss	 3 * SIZE(B), %xmm11
	movss	 4 * SIZE(B), %xmm12
	movss	 5 * SIZE(B), %xmm13
	movss	 6 * SIZE(B), %xmm14
	movss	 7 * SIZE(B), %xmm15

	PREFETCHW	 (WPREFETCHSIZE +  0) * SIZE(BO)

	shufps	$0, %xmm8, %xmm8
	shufps	$0, %xmm9, %xmm9
	shufps	$0, %xmm10, %xmm10
	shufps	$0, %xmm11, %xmm11
	shufps	$0, %xmm12, %xmm12
	shufps	$0, %xmm13, %xmm13
	shufps	$0, %xmm14, %xmm14
	shufps	$0, %xmm15, %xmm15

	PREFETCHW	 (WPREFETCHSIZE + 16) * SIZE(BO)

#if defined(NN) || defined(NT) || defined(NR) || defined(NC)  || \
    defined(TN) || defined(TT) || defined(TR) || defined(TC)
	xorps	 %xmm7, %xmm9
	xorps	 %xmm7, %xmm11
	xorps	 %xmm7, %xmm13
	xorps	 %xmm7, %xmm15
#else
	xorps	 %xmm7, %xmm8
	xorps	 %xmm7, %xmm10
	xorps	 %xmm7, %xmm12
	xorps	 %xmm7, %xmm14
#endif

	movaps	%xmm8,  0 * SIZE(BO)
	movaps	%xmm9,  4 * SIZE(BO)
	movaps	%xmm10,  8 * SIZE(BO)
	movaps	%xmm11, 12 * SIZE(BO)
	movaps	%xmm12, 16 * SIZE(BO)
	movaps	%xmm13, 20 * SIZE(BO)
	movaps	%xmm14, 24 * SIZE(BO)
	movaps	%xmm15, 28 * SIZE(BO)

	addq	$32 * SIZE, BO
	addq	$ 8 * SIZE, B
	decq	%rax
	jne	.L02
	ALIGN_4

.L03:
	movq	K, %rax
	andq	$3, %rax
	BRANCH
	jle	.L10
	ALIGN_4

.L04:
	movss	 0 * SIZE(B), %xmm8
	movss	 1 * SIZE(B), %xmm9
	movss	 2 * SIZE(B), %xmm10
	movss	 3 * SIZE(B), %xmm11

	shufps	$0, %xmm8, %xmm8
	shufps	$0, %xmm9, %xmm9
	shufps	$0, %xmm10, %xmm10
	shufps	$0, %xmm11, %xmm11

#if defined(NN) || defined(NT) || defined(NR) || defined(NC)  || \
    defined(TN) || defined(TT) || defined(TR) || defined(TC)
	xorps	 %xmm7, %xmm9
	xorps	 %xmm7, %xmm11
#else
	xorps	 %xmm7, %xmm8
	xorps	 %xmm7, %xmm10
#endif

	movaps	%xmm8,  0 * SIZE(BO)
	movaps	%xmm9,  4 * SIZE(BO)
	movaps	%xmm10,  8 * SIZE(BO)
	movaps	%xmm11, 12 * SIZE(BO)

	addq	$ 4 * SIZE, B
	addq	$16 * SIZE, BO
	decq	%rax
	jne	.L04
	ALIGN_4

.L10:
	movq	C, CO1			# coffset1 = c
	leaq	(C, LDC, 1), CO2	# coffset2 = c + ldc
	movq	A, AO		# aoffset = a

	leaq	 (RPREFETCHSIZE +  0)  * SIZE(B), BB

	movq	M,  I
	sarq	$2, I		# i = (m >> 2)
	jle	.L20
	ALIGN_4

.L11:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	32 * SIZE + BUFFER, BO
#else
	leaq	32 * SIZE + BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax,   8), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 8), BO
#endif

	movaps	-32 * SIZE(AO), %xmm0
	movaps	-32 * SIZE(BO), %xmm1
	pxor	%xmm8, %xmm8
	movaps	-28 * SIZE(AO), %xmm2
	movaps	-28 * SIZE(BO), %xmm3
	pxor	%xmm9, %xmm9
	movaps	-24 * SIZE(AO), %xmm4
	movaps	-24 * SIZE(BO), %xmm5
	pxor	%xmm10, %xmm10
	movaps	-20 * SIZE(AO), %xmm6
	movaps	-16 * SIZE(BO), %xmm7
	pxor	%xmm11, %xmm11

	PREFETCHW      7 * SIZE(CO1)
	pxor	%xmm12, %xmm12
	PREFETCHW      7 * SIZE(CO2)
	pxor	%xmm13, %xmm13
	PREFETCH	-32  * SIZE(BB)
	pxor	%xmm14, %xmm14
	PREFETCH	-16  * SIZE(BB)
	pxor	%xmm15, %xmm15
	subq	 $-16 * SIZE, BB

#ifndef TRMMKERNEL
	movq	K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movq	K, %rax
	subq	KK, %rax
	movq	%rax, KKK
#else
	movq	KK, %rax
#ifdef LEFT
	addq	$4, %rax
#else
	addq	$2, %rax
#endif
	movq	%rax, KKK
#endif
#ifndef GENERIC
	andq	$-8, %rax

	leaq	(, %rax, 8), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 8), BO
	negq	%rax
	NOBRANCH
	je	.L15
	ALIGN_3

.L12:
	KERNEL1(16 *  0)
	KERNEL2(16 *  0)
	KERNEL3(16 *  0)
	KERNEL4(16 *  0)
	KERNEL5(16 *  0)
	KERNEL6(16 *  0)
	KERNEL7(16 *  0)
	KERNEL8(16 *  0)

	KERNEL1(16 *  2)
	KERNEL2(16 *  2)
	KERNEL3(16 *  2)
	KERNEL4(16 *  2)
	KERNEL5(16 *  2)
	KERNEL6(16 *  2)
	KERNEL7(16 *  2)
	KERNEL8(16 *  2)

	addq	$16 * SIZE, %rax
	NOBRANCH
	je	.L15
	KERNEL1(16 *  0)
	KERNEL2(16 *  0)
	KERNEL3(16 *  0)
	KERNEL4(16 *  0)
	KERNEL5(16 *  0)
	KERNEL6(16 *  0)
	KERNEL7(16 *  0)
	KERNEL8(16 *  0)

	KERNEL1(16 *  2)
	KERNEL2(16 *  2)
	KERNEL3(16 *  2)
	KERNEL4(16 *  2)
	KERNEL5(16 *  2)
	KERNEL6(16 *  2)
	KERNEL7(16 *  2)
	KERNEL8(16 *  2)

	addq	$16 * SIZE, %rax
	NOBRANCH
	je	.L15
	KERNEL1(16 *  0)
	KERNEL2(16 *  0)
	KERNEL3(16 *  0)
	KERNEL4(16 *  0)
	KERNEL5(16 *  0)
	KERNEL6(16 *  0)
	KERNEL7(16 *  0)
	KERNEL8(16 *  0)

	KERNEL1(16 *  2)
	KERNEL2(16 *  2)
	KERNEL3(16 *  2)
	KERNEL4(16 *  2)
	KERNEL5(16 *  2)
	KERNEL6(16 *  2)
	KERNEL7(16 *  2)
	KERNEL8(16 *  2)

	addq	$16 * SIZE, %rax
	NOBRANCH
	je	.L15
	KERNEL1(16 *  0)
	KERNEL2(16 *  0)
	KERNEL3(16 *  0)
	KERNEL4(16 *  0)
	KERNEL5(16 *  0)
	KERNEL6(16 *  0)
	KERNEL7(16 *  0)
	KERNEL8(16 *  0)

	KERNEL1(16 *  2)
	KERNEL2(16 *  2)
	KERNEL3(16 *  2)
	KERNEL4(16 *  2)
	KERNEL5(16 *  2)
	KERNEL6(16 *  2)
	KERNEL7(16 *  2)
	KERNEL8(16 *  2)

	addq	$16 * SIZE, %rax
	NOBRANCH
	je	.L15
	KERNEL1(16 *  0)
	KERNEL2(16 *  0)
	KERNEL3(16 *  0)
	KERNEL4(16 *  0)
	KERNEL5(16 *  0)
	KERNEL6(16 *  0)
	KERNEL7(16 *  0)
	KERNEL8(16 *  0)

	KERNEL1(16 *  2)
	KERNEL2(16 *  2)
	KERNEL3(16 *  2)
	KERNEL4(16 *  2)
	KERNEL5(16 *  2)
	KERNEL6(16 *  2)
	KERNEL7(16 *  2)
	KERNEL8(16 *  2)

	addq	$16 * SIZE, %rax
	NOBRANCH
	je	.L15
	KERNEL1(16 *  0)
	KERNEL2(16 *  0)
	KERNEL3(16 *  0)
	KERNEL4(16 *  0)
	KERNEL5(16 *  0)
	KERNEL6(16 *  0)
	KERNEL7(16 *  0)
	KERNEL8(16 *  0)

	KERNEL1(16 *  2)
	KERNEL2(16 *  2)
	KERNEL3(16 *  2)
	KERNEL4(16 *  2)
	KERNEL5(16 *  2)
	KERNEL6(16 *  2)
	KERNEL7(16 *  2)
	KERNEL8(16 *  2)

	addq	$16 * SIZE, %rax
	NOBRANCH
	je	.L15
	KERNEL1(16 *  0)
	KERNEL2(16 *  0)
	KERNEL3(16 *  0)
	KERNEL4(16 *  0)
	KERNEL5(16 *  0)
	KERNEL6(16 *  0)
	KERNEL7(16 *  0)
	KERNEL8(16 *  0)

	KERNEL1(16 *  2)
	KERNEL2(16 *  2)
	KERNEL3(16 *  2)
	KERNEL4(16 *  2)
	KERNEL5(16 *  2)
	KERNEL6(16 *  2)
	KERNEL7(16 *  2)
	KERNEL8(16 *  2)

	addq	$16 * SIZE, %rax
	NOBRANCH
	je	.L15
	KERNEL1(16 *  0)
	KERNEL2(16 *  0)
	KERNEL3(16 *  0)
	KERNEL4(16 *  0)
	KERNEL5(16 *  0)
	KERNEL6(16 *  0)
	KERNEL7(16 *  0)
	KERNEL8(16 *  0)

	KERNEL1(16 *  2)
	KERNEL2(16 *  2)
	KERNEL3(16 *  2)
	KERNEL4(16 *  2)
	KERNEL5(16 *  2)
	KERNEL6(16 *  2)
	KERNEL7(16 *  2)
	KERNEL8(16 *  2)

	addq	$16 * SIZE, %rax
	BRANCH
	jl	.L12
	ALIGN_3

.L15:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	testq	$4, %rax
	je .L16
	xorq	%rax, %rax
	ALIGN_3

	KERNEL1(16 *  0)
	KERNEL2(16 *  0)
	KERNEL3(16 *  0)
	KERNEL4(16 *  0)
	KERNEL5(16 *  0)
	KERNEL6(16 *  0)
	KERNEL7(16 *  0)
	KERNEL8(16 *  0)

	addq	$64 * SIZE, BO
	addq	$32 * SIZE, AO
	ALIGN_3
#else
	sarq	$2, %rax
	NOBRANCH
	jle	.L16
	ALIGN_3

.L12:
	KERNEL1(16 *  0)
	KERNEL2(16 *  0)
	KERNEL3(16 *  0)
	KERNEL4(16 *  0)
	KERNEL5(16 *  0)
	KERNEL6(16 *  0)
	KERNEL7(16 *  0)
	KERNEL8(16 *  0)

	addq	$ 64 * SIZE, BO
	subq	$-32 * SIZE, AO
	decq	%rax
	BRANCH
	jg	.L12
#endif


.L16:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	movaps	ALPHA_R, %xmm6
	movaps	ALPHA_I, %xmm7
	andq	$3, %rax		# if (k & 1)
	BRANCH
	je .L18

	leaq	(, %rax, 8), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 8), BO
	negq	%rax
	ALIGN_4

.L17:
	mulps	%xmm0, %xmm1
	addps	%xmm1, %xmm8
	movaps	-28 * SIZE(BO, %rax, 8), %xmm1
	mulps	%xmm0, %xmm1
	addps	%xmm1, %xmm9
	movaps	-24 * SIZE(BO, %rax, 8), %xmm1
	mulps	%xmm0, %xmm1
	mulps	-20 * SIZE(BO, %rax, 8), %xmm0
	addps	%xmm1, %xmm10
	movaps	-32 * SIZE(BO, %rax, 8), %xmm1
	addps	%xmm0, %xmm11
	movaps	-24 * SIZE(AO, %rax, 4), %xmm0
	mulps	%xmm2, %xmm1
	addps	%xmm1, %xmm12
	movaps	-28 * SIZE(BO, %rax, 8), %xmm1
	mulps	%xmm2, %xmm1
	addps	%xmm1, %xmm13
	movaps	-24 * SIZE(BO, %rax, 8), %xmm1
	mulps	%xmm2, %xmm1
	mulps	-20 * SIZE(BO, %rax, 8), %xmm2
	addps	%xmm1, %xmm14
	movaps	-16 * SIZE(BO, %rax, 8), %xmm1
	addps	%xmm2, %xmm15
	movaps	-20 * SIZE(AO, %rax, 4), %xmm2

	addq	$SIZE * 2, %rax
	jl	.L17
	ALIGN_4

.L18:
#ifndef TRMMKERNEL
	movsd	0 * SIZE(CO1), %xmm0
	movhps	2 * SIZE(CO1), %xmm0
	movsd	4 * SIZE(CO1), %xmm2
	movhps	6 * SIZE(CO1), %xmm2

	movsd	0 * SIZE(CO2), %xmm1
	movhps	2 * SIZE(CO2), %xmm1
	movsd	4 * SIZE(CO2), %xmm3
	movhps	6 * SIZE(CO2), %xmm3
#endif

	shufps	$0xb1, %xmm9, %xmm9
	shufps	$0xb1, %xmm11, %xmm11
	shufps	$0xb1, %xmm13, %xmm13
	shufps	$0xb1, %xmm15, %xmm15

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(RR) || defined(RC) || defined(CR) || defined(CC)
	subps	%xmm9, %xmm8
	subps	%xmm11, %xmm10
	subps	%xmm13, %xmm12
	subps	%xmm15, %xmm14
#else
	addps	%xmm9, %xmm8
	addps	%xmm11, %xmm10
	addps	%xmm13, %xmm12
	addps	%xmm15, %xmm14
#endif

	movaps	%xmm8, %xmm9
	movaps	%xmm10, %xmm11
	movaps	%xmm12, %xmm13
 	movaps	%xmm14, %xmm15

	shufps	$0xb1, %xmm8, %xmm8
	shufps	$0xb1, %xmm10, %xmm10
	shufps	$0xb1, %xmm12, %xmm12
	shufps	$0xb1, %xmm14, %xmm14

	mulps	%xmm6, %xmm9
	mulps	%xmm7, %xmm8
	mulps	%xmm6, %xmm11
	mulps	%xmm7, %xmm10

	mulps	%xmm6, %xmm13
	mulps	%xmm7, %xmm12
	mulps	%xmm6, %xmm15
	mulps	%xmm7, %xmm14

	addps	%xmm9, %xmm8
	addps	%xmm11, %xmm10
	addps	%xmm13, %xmm12
	addps	%xmm15, %xmm14

#ifndef TRMMKERNEL
	addps	%xmm0,  %xmm8
	addps	%xmm1,  %xmm10
	addps	%xmm2, %xmm12
	addps	%xmm3, %xmm14
#endif

	movsd	%xmm8, 0 * SIZE(CO1)
	movhps	%xmm8, 2 * SIZE(CO1)
	movsd	%xmm12, 4 * SIZE(CO1)
	movhps	%xmm12, 6 * SIZE(CO1)

	movsd	%xmm10, 0 * SIZE(CO2)
	movhps	%xmm10, 2 * SIZE(CO2)
	movsd	%xmm14, 4 * SIZE(CO2)
	movhps	%xmm14, 6 * SIZE(CO2)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax, 8), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 8), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$4, KK
#endif

	addq	$8 * SIZE, CO1		# coffset += 4
	addq	$8 * SIZE, CO2		# coffset += 4
	decq	I			# i --
	jg	.L11
	ALIGN_4

.L20:
	testq	$2, M
	je	.L30

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	32 * SIZE + BUFFER, BO
#else
	leaq	32 * SIZE + BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax,   8), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 8), BO
#endif

	movaps	-32 * SIZE(AO), %xmm0
	movaps	-16 * SIZE(AO), %xmm2
	movaps	  0 * SIZE(AO), %xmm4
	movaps	 16 * SIZE(AO), %xmm6

	movaps	-32 * SIZE(BO), %xmm1
	movaps	-16 * SIZE(BO), %xmm3
	movaps	  0 * SIZE(BO), %xmm5
	movaps	 16 * SIZE(BO), %xmm7

	pxor	%xmm8, %xmm8
	pxor	%xmm9, %xmm9
	pxor	%xmm10, %xmm10
	pxor	%xmm11, %xmm11

#ifndef TRMMKERNEL
	movq	K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movq	K, %rax
	subq	KK, %rax
	movq	%rax, KKK
#else
	movq	KK, %rax
#ifdef LEFT
	addq	$2, %rax
#else
	addq	$2, %rax
#endif
	movq	%rax, KKK
#endif
	sarq	$3, %rax
	je	.L25
	ALIGN_4

.L22:
	mulps	%xmm0, %xmm1
	PREFETCH	(PREFETCHSIZE +  0) * SIZE(AO)
	addps	%xmm1, %xmm8
	movaps	-28 * SIZE(BO), %xmm1
	mulps	%xmm0, %xmm1
	addps	%xmm1, %xmm9
	movaps	-24 * SIZE(BO), %xmm1
	mulps	%xmm0, %xmm1
	mulps	-20 * SIZE(BO), %xmm0
	addps	%xmm1, %xmm10
	movaps	 32 * SIZE(BO), %xmm1
	addps	%xmm0, %xmm11
	movaps	-28 * SIZE(AO), %xmm0

	mulps	%xmm0, %xmm3
	addps	%xmm3, %xmm8
	movaps	-12 * SIZE(BO), %xmm3
	mulps	%xmm0, %xmm3
	addps	%xmm3, %xmm9
	movaps	 -8 * SIZE(BO), %xmm3
	mulps	%xmm0, %xmm3
	mulps	 -4 * SIZE(BO), %xmm0
	addps	%xmm3, %xmm10
	movaps	 48 * SIZE(BO), %xmm3
	addps	%xmm0, %xmm11
	movaps	-24 * SIZE(AO), %xmm0

	mulps	%xmm0, %xmm5
	addps	%xmm5, %xmm8
	movaps	  4 * SIZE(BO), %xmm5
	mulps	%xmm0, %xmm5
	addps	%xmm5, %xmm9
	movaps	  8 * SIZE(BO), %xmm5
	mulps	%xmm0, %xmm5
	mulps	 12 * SIZE(BO), %xmm0
	addps	%xmm5, %xmm10
	movaps	 64 * SIZE(BO), %xmm5
	addps	%xmm0, %xmm11
	movaps	-20 * SIZE(AO), %xmm0

	mulps	%xmm0, %xmm7
	addps	%xmm7, %xmm8
	movaps	 20 * SIZE(BO), %xmm7
	mulps	%xmm0, %xmm7
	addps	%xmm7, %xmm9
	movaps	 24 * SIZE(BO), %xmm7
	mulps	%xmm0, %xmm7
	mulps	 28 * SIZE(BO), %xmm0
	addps	%xmm7, %xmm10
	movaps	 80 * SIZE(BO), %xmm7
	addps	%xmm0, %xmm11
	movaps	  0 * SIZE(AO), %xmm0

	mulps	%xmm2, %xmm1
	PREFETCH	(PREFETCHSIZE + 16) * SIZE(AO)
	addps	%xmm1, %xmm8
	movaps	 36 * SIZE(BO), %xmm1
	mulps	%xmm2, %xmm1
	addps	%xmm1, %xmm9
	movaps	 40 * SIZE(BO), %xmm1
	mulps	%xmm2, %xmm1
	mulps	 44 * SIZE(BO), %xmm2
	addps	%xmm1, %xmm10
	movaps	 96 * SIZE(BO), %xmm1
	addps	%xmm2, %xmm11
	movaps	-12 * SIZE(AO), %xmm2

	mulps	%xmm2, %xmm3
	addps	%xmm3, %xmm8
	movaps	 52 * SIZE(BO), %xmm3
	mulps	%xmm2, %xmm3
	addps	%xmm3, %xmm9
	movaps	 56 * SIZE(BO), %xmm3
	mulps	%xmm2, %xmm3
	mulps	 60 * SIZE(BO), %xmm2
	addps	%xmm3, %xmm10
	movaps	112 * SIZE(BO), %xmm3
	addps	%xmm2, %xmm11
	movaps	 -8 * SIZE(AO), %xmm2

	mulps	%xmm2, %xmm5
	addps	%xmm5, %xmm8
	movaps	 68 * SIZE(BO), %xmm5
	mulps	%xmm2, %xmm5
	addps	%xmm5, %xmm9
	movaps	 72 * SIZE(BO), %xmm5
	mulps	%xmm2, %xmm5
	mulps	 76 * SIZE(BO), %xmm2
	addps	%xmm5, %xmm10
	movaps	128 * SIZE(BO), %xmm5
	addps	%xmm2, %xmm11
	movaps	 -4 * SIZE(AO), %xmm2

	mulps	%xmm2, %xmm7
	addps	%xmm7, %xmm8
	movaps	 84 * SIZE(BO), %xmm7
	mulps	%xmm2, %xmm7
	addps	%xmm7, %xmm9
	movaps	 88 * SIZE(BO), %xmm7
	mulps	%xmm2, %xmm7
	mulps	 92 * SIZE(BO), %xmm2
	addps	%xmm7, %xmm10
	movaps	144 * SIZE(BO), %xmm7
	addps	%xmm2, %xmm11
	movaps	 16 * SIZE(AO), %xmm2

	subq   $ -32 * SIZE, AO
	subq   $-128 * SIZE, BO

	decq   %rax
	jne    .L22
	ALIGN_4

.L25:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	movaps	ALPHA_R, %xmm6
	movaps	ALPHA_I, %xmm7
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L28
	ALIGN_4

.L26:
	mulps	%xmm0, %xmm1
	addps	%xmm1, %xmm8
	movaps	-28 * SIZE(BO), %xmm1
	mulps	%xmm0, %xmm1
	addps	%xmm1, %xmm9
	movaps	-24 * SIZE(BO), %xmm1
	mulps	%xmm0, %xmm1
	mulps	-20 * SIZE(BO), %xmm0
	addps	%xmm1, %xmm10
	movaps	-16 * SIZE(BO), %xmm1
	addps	%xmm0, %xmm11
	movaps	-28 * SIZE(AO), %xmm0

	subq	$- 4 * SIZE, AO
	subq	$-16 * SIZE, BO
	decq	%rax
	jg	.L26
	ALIGN_4

.L28:
#ifndef TRMMKERNEL
	movsd	0 * SIZE(CO1), %xmm0
	movhps	2 * SIZE(CO1), %xmm0

	movsd	0 * SIZE(CO2), %xmm1
	movhps	2 * SIZE(CO2), %xmm1
#endif

	shufps	$0xb1, %xmm9, %xmm9
	shufps	$0xb1, %xmm11, %xmm11

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(RR) || defined(RC) || defined(CR) || defined(CC)
	subps	%xmm9, %xmm8
	subps	%xmm11, %xmm10
#else
	addps	%xmm9, %xmm8
	addps	%xmm11, %xmm10
#endif

	movaps	%xmm8, %xmm9
	movaps	%xmm10, %xmm11

	shufps	$0xb1, %xmm8, %xmm8
	shufps	$0xb1, %xmm10, %xmm10

	mulps	%xmm6, %xmm9
	mulps	%xmm7, %xmm8
	mulps	%xmm6, %xmm11
	mulps	%xmm7, %xmm10

	addps	%xmm9, %xmm8
	addps	%xmm11, %xmm10

#ifndef TRMMKERNEL
	addps	%xmm0,  %xmm8
	addps	%xmm1,  %xmm10
#endif

	movsd	%xmm8, 0 * SIZE(CO1)
	movhps	%xmm8, 2 * SIZE(CO1)

	movsd	%xmm10, 0 * SIZE(CO2)
	movhps	%xmm10, 2 * SIZE(CO2)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax,    8), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 8), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$2, KK
#endif

	addq	$4 * SIZE, CO1		# coffset += 4
	addq	$4 * SIZE, CO2		# coffset += 4
	ALIGN_4

.L30:
	testq	$1, M
	je	.L39

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	32 * SIZE + BUFFER, BO
#else
	leaq	32 * SIZE + BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax,   8), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 8), BO
#endif

	movaps	-32 * SIZE(AO), %xmm0
	movaps	-24 * SIZE(AO), %xmm2

	movaps	-32 * SIZE(BO), %xmm1
	movaps	-16 * SIZE(BO), %xmm3
	movaps	  0 * SIZE(BO), %xmm5
	movaps	 16 * SIZE(BO), %xmm7

	pxor	%xmm8, %xmm8
	pxor	%xmm9, %xmm9
	pxor	%xmm10, %xmm10
	pxor	%xmm11, %xmm11

#ifndef TRMMKERNEL
	movq	K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movq	K, %rax
	subq	KK, %rax
	movq	%rax, KKK
#else
	movq	KK, %rax
#ifdef LEFT
	addq	$1, %rax
#else
	addq	$2, %rax
#endif
	movq	%rax, KKK
#endif
	sarq	$3, %rax
	je	.L35
	ALIGN_4

.L32:
	mulps	%xmm0, %xmm1
	PREFETCH	(PREFETCHSIZE +  0) * SIZE(AO)
	addps	%xmm1, %xmm8
	movaps	-28 * SIZE(BO), %xmm1
	mulps	%xmm0, %xmm1
	addps	%xmm1, %xmm9
	movaps	-24 * SIZE(BO), %xmm1
	mulps	%xmm0, %xmm1
	addps	%xmm1, %xmm10
	movaps	-20 * SIZE(BO), %xmm1
	mulps	%xmm0, %xmm1
	movsd	-30 * SIZE(AO), %xmm0
	addps	%xmm1, %xmm11
	movaps	 32 * SIZE(BO), %xmm1

	mulps	%xmm0, %xmm3
	addps	%xmm3, %xmm8
	movaps	-12 * SIZE(BO), %xmm3
	mulps	%xmm0, %xmm3
	addps	%xmm3, %xmm9
	movaps	 -8 * SIZE(BO), %xmm3
	mulps	%xmm0, %xmm3
	addps	%xmm3, %xmm10
	movaps	 -4 * SIZE(BO), %xmm3
	mulps	%xmm0, %xmm3
	movsd	-28 * SIZE(AO), %xmm0
	addps	%xmm3, %xmm11
	movaps	 48 * SIZE(BO), %xmm3

	mulps	%xmm0, %xmm5
	addps	%xmm5, %xmm8
	movaps	  4 * SIZE(BO), %xmm5
	mulps	%xmm0, %xmm5
	addps	%xmm5, %xmm9
	movaps	  8 * SIZE(BO), %xmm5
	mulps	%xmm0, %xmm5
	addps	%xmm5, %xmm10
	movaps	 12 * SIZE(BO), %xmm5
	mulps	%xmm0, %xmm5
	movsd	-26 * SIZE(AO), %xmm0
	addps	%xmm5, %xmm11
	movaps	 64 * SIZE(BO), %xmm5

	mulps	%xmm0, %xmm7
	addps	%xmm7, %xmm8
	movaps	 20 * SIZE(BO), %xmm7
	mulps	%xmm0, %xmm7
	addps	%xmm7, %xmm9
	movaps	 24 * SIZE(BO), %xmm7
	mulps	%xmm0, %xmm7
	addps	%xmm7, %xmm10
	movaps	 28 * SIZE(BO), %xmm7
	mulps	%xmm0, %xmm7
	movsd	-16 * SIZE(AO), %xmm0
	addps	%xmm7, %xmm11
	movaps	 80 * SIZE(BO), %xmm7

	mulps	%xmm2, %xmm1
	addps	%xmm1, %xmm8
	movaps	 36 * SIZE(BO), %xmm1
	mulps	%xmm2, %xmm1
	addps	%xmm1, %xmm9
	movaps	 40 * SIZE(BO), %xmm1
	mulps	%xmm2, %xmm1
	addps	%xmm1, %xmm10
	movaps	 44 * SIZE(BO), %xmm1
	mulps	%xmm2, %xmm1
	movsd	-22 * SIZE(AO), %xmm2
	addps	%xmm1, %xmm11
	movaps	 96 * SIZE(BO), %xmm1

	mulps	%xmm2, %xmm3
	addps	%xmm3, %xmm8
	movaps	 52 * SIZE(BO), %xmm3
	mulps	%xmm2, %xmm3
	addps	%xmm3, %xmm9
	movaps	 56 * SIZE(BO), %xmm3
	mulps	%xmm2, %xmm3
	addps	%xmm3, %xmm10
	movaps	 60 * SIZE(BO), %xmm3
	mulps	%xmm2, %xmm3
	movsd	-20 * SIZE(AO), %xmm2
	addps	%xmm3, %xmm11
	movaps	112 * SIZE(BO), %xmm3

	mulps	%xmm2, %xmm5
	addps	%xmm5, %xmm8
	movaps	 68 * SIZE(BO), %xmm5
	mulps	%xmm2, %xmm5
	addps	%xmm5, %xmm9
	movaps	 72 * SIZE(BO), %xmm5
	mulps	%xmm2, %xmm5
	addps	%xmm5, %xmm10
	movaps	 76 * SIZE(BO), %xmm5
	mulps	%xmm2, %xmm5
	movsd	-18 * SIZE(AO), %xmm2
	addps	%xmm5, %xmm11
	movaps	128 * SIZE(BO), %xmm5

	mulps	%xmm2, %xmm7
	addps	%xmm7, %xmm8
	movaps	 84 * SIZE(BO), %xmm7
	mulps	%xmm2, %xmm7
	addps	%xmm7, %xmm9
	movaps	 88 * SIZE(BO), %xmm7
	mulps	%xmm2, %xmm7
	addps	%xmm7, %xmm10
	movaps	 92 * SIZE(BO), %xmm7
	mulps	%xmm2, %xmm7
	movsd	 -8 * SIZE(AO), %xmm2
	addps	%xmm7, %xmm11
	movaps	144 * SIZE(BO), %xmm7

	subq   $ -16 * SIZE, AO
	subq   $-128 * SIZE, BO

	decq   %rax
	jne    .L32
	ALIGN_4

.L35:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	movaps	ALPHA_R, %xmm6
	movaps	ALPHA_I, %xmm7
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L38
	ALIGN_4

.L36:
	mulps	%xmm0, %xmm1
	addps	%xmm1, %xmm8
	movaps	-28 * SIZE(BO), %xmm1
	mulps	%xmm0, %xmm1
	addps	%xmm1, %xmm9
	movaps	-24 * SIZE(BO), %xmm1
	mulps	%xmm0, %xmm1
	addps	%xmm1, %xmm10
	movaps	-20 * SIZE(BO), %xmm1
	mulps	%xmm0, %xmm1
	movsd	-30 * SIZE(AO), %xmm0
	addps	%xmm1, %xmm11
	movaps	-16 * SIZE(BO), %xmm1

	subq	$ -2 * SIZE, AO
	subq	$-16 * SIZE, BO
	decq	%rax
	jg	.L36
	ALIGN_4

.L38:
#ifndef TRMMKERNEL
#ifdef movsd
	xorps	%xmm0, %xmm0
#endif
	movsd	0 * SIZE(CO1), %xmm0
#ifdef movsd
	xorps	%xmm1, %xmm1
#endif
	movsd	0 * SIZE(CO2), %xmm1
#endif

	shufps	$0xb1, %xmm9, %xmm9
	shufps	$0xb1, %xmm11, %xmm11

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(RR) || defined(RC) || defined(CR) || defined(CC)
	subps	%xmm9, %xmm8
	subps	%xmm11, %xmm10
#else
	addps	%xmm9, %xmm8
	addps	%xmm11, %xmm10
#endif

	movaps	%xmm8, %xmm9
	movaps	%xmm10, %xmm11

	shufps	$0xb1, %xmm8, %xmm8
	shufps	$0xb1, %xmm10, %xmm10

	mulps	%xmm6, %xmm9
	mulps	%xmm7, %xmm8
	mulps	%xmm6, %xmm11
	mulps	%xmm7, %xmm10

	addps	%xmm9, %xmm8
	addps	%xmm11, %xmm10

#ifndef TRMMKERNEL
	addps	%xmm0,  %xmm8
	addps	%xmm1,  %xmm10
#endif

	movlps	%xmm8,  0 * SIZE(CO1)
	movlps	%xmm10, 0 * SIZE(CO2)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax,    8), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 8), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$1, KK
#endif
	ALIGN_4

.L39:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addl	$2, KK
#endif

	leaq	(C, LDC, 2), C		# c += 2 * ldc
	decq	J			# j --
	jg	.L01
	ALIGN_4

.L40:
	testq	$1, N
	je	.L999
	ALIGN_4

.L41:
#if defined(TRMMKERNEL) && defined(LEFT)
	movq	OFFSET, %rax
	movq	%rax, KK
#endif

/* Copying to Sub Buffer */
	leaq	BUFFER, BO
	movaps	POSINV, %xmm7

	movq	K, %rax
	sarq	$2, %rax
	jle	.L43
	ALIGN_4

.L42:
	movss	 0 * SIZE(B), %xmm8
	movss	 1 * SIZE(B), %xmm9
	movss	 2 * SIZE(B), %xmm10
	movss	 3 * SIZE(B), %xmm11
	movss	 4 * SIZE(B), %xmm12
	movss	 5 * SIZE(B), %xmm13
	movss	 6 * SIZE(B), %xmm14
	movss	 7 * SIZE(B), %xmm15

	shufps	$0, %xmm8, %xmm8
	shufps	$0, %xmm9, %xmm9
	shufps	$0, %xmm10, %xmm10
	shufps	$0, %xmm11, %xmm11
	shufps	$0, %xmm12, %xmm12
	shufps	$0, %xmm13, %xmm13
	shufps	$0, %xmm14, %xmm14
	shufps	$0, %xmm15, %xmm15

#if defined(NN) || defined(NT) || defined(NR) || defined(NC)  || \
    defined(TN) || defined(TT) || defined(TR) || defined(TC)
	xorps	 %xmm7, %xmm9
	xorps	 %xmm7, %xmm11
	xorps	 %xmm7, %xmm13
	xorps	 %xmm7, %xmm15
#else
	xorps	 %xmm7, %xmm8
	xorps	 %xmm7, %xmm10
	xorps	 %xmm7, %xmm12
	xorps	 %xmm7, %xmm14
#endif

#if defined(OPTERON) || defined(BARCELONA) || defined(SHANGHAI)
	prefetchnta	 56 * SIZE(B)
#endif

	movaps	%xmm8,  0 * SIZE(BO)
	movaps	%xmm9,  4 * SIZE(BO)
	movaps	%xmm10,  8 * SIZE(BO)
	movaps	%xmm11, 12 * SIZE(BO)
	movaps	%xmm12, 16 * SIZE(BO)
	movaps	%xmm13, 20 * SIZE(BO)
	movaps	%xmm14, 24 * SIZE(BO)
	movaps	%xmm15, 28 * SIZE(BO)

#if defined(PENTIUM4) || defined(GENERIC)
        PREFETCHW	128 * SIZE(BO)
	PREFETCH	112 * SIZE(B)
#endif

	addq	$32 * SIZE, BO
	addq	$ 8 * SIZE, B
	decq	%rax
	jne	.L42
	ALIGN_4

.L43:
	movq	K, %rax
	andq	$3, %rax
	BRANCH
	jle	.L50
	ALIGN_4

.L44:
	movss	 0 * SIZE(B), %xmm8
	movss	 1 * SIZE(B), %xmm9

	shufps	$0, %xmm8, %xmm8
	shufps	$0, %xmm9, %xmm9

#if defined(NN) || defined(NT) || defined(NR) || defined(NC)  || \
    defined(TN) || defined(TT) || defined(TR) || defined(TC)
	xorps	 %xmm7, %xmm9
#else
	xorps	 %xmm7, %xmm8
#endif

	movaps	%xmm8,  0 * SIZE(BO)
	movaps	%xmm9,  4 * SIZE(BO)

	addq	$2 * SIZE, B
	addq	$8 * SIZE, BO
	decq	%rax
	jne	.L44
	ALIGN_4

.L50:
	movq	C, CO1			# coffset1 = c
	movq	A, AO		# aoffset = a

	movq	M,  I
	sarq	$2, I		# i = (m >> 2)
	jle	.L60
	ALIGN_4

.L51:

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	32 * SIZE + BUFFER, BO
#else
	leaq	32 * SIZE + BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax, 8), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 4), BO
#endif

	movaps	-32 * SIZE(AO), %xmm0
	pxor	%xmm8, %xmm8
	movaps	-16 * SIZE(AO), %xmm2
	pxor	%xmm9, %xmm9
	movaps	  0 * SIZE(AO), %xmm4
	pxor	%xmm10, %xmm10
	movaps	 16 * SIZE(AO), %xmm6
	pxor	%xmm11, %xmm11

	movaps	-32 * SIZE(BO), %xmm1
	pxor	%xmm12, %xmm12
	movaps	-16 * SIZE(BO), %xmm3
	pxor	%xmm13, %xmm13
	movaps	  0 * SIZE(BO), %xmm5
	pxor	%xmm14, %xmm14
	movaps	 16 * SIZE(BO), %xmm7
	pxor	%xmm15, %xmm15

	PREFETCHW      7 * SIZE(CO1)

#ifndef TRMMKERNEL
	movq	K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movq	K, %rax
	subq	KK, %rax
	movq	%rax, KKK
#else
	movq	KK, %rax
#ifdef LEFT
	addq	$4, %rax
#else
	addq	$1, %rax
#endif
	movq	%rax, KKK
#endif
	sarq	$3, %rax
	je	.L55
	ALIGN_4

.L52:
	mulps	%xmm0, %xmm1
	PREFETCH	(PREFETCHSIZE +  0) * SIZE(AO)
	mulps	-28 * SIZE(BO), %xmm0
	addps	%xmm1, %xmm8
	movaps	-32 * SIZE(BO), %xmm1
	addps	%xmm0, %xmm9
	movaps	-28 * SIZE(AO), %xmm0
	mulps	%xmm0, %xmm1
	mulps	-28 * SIZE(BO), %xmm0
	addps	%xmm1, %xmm12
	movaps	-24 * SIZE(BO), %xmm1
	addps	%xmm0, %xmm13
	movaps	-24 * SIZE(AO), %xmm0

	mulps	%xmm0, %xmm1
	mulps	-20 * SIZE(BO), %xmm0
	addps	%xmm1, %xmm8
	movaps	-24 * SIZE(BO), %xmm1
	addps	%xmm0, %xmm9
	movaps	-20 * SIZE(AO), %xmm0
	mulps	%xmm0, %xmm1
	mulps	-20 * SIZE(BO), %xmm0
	addps	%xmm1, %xmm12
	movaps	 32 * SIZE(BO), %xmm1
	addps	%xmm0, %xmm13
	movaps	 32 * SIZE(AO), %xmm0

	PREFETCH	(PREFETCHSIZE + 16) * SIZE(AO)

	mulps	%xmm2, %xmm3
	mulps	-12 * SIZE(BO), %xmm2
	addps	%xmm3, %xmm8
	movaps	-16 * SIZE(BO), %xmm3
	addps	%xmm2, %xmm9
	movaps	-12 * SIZE(AO), %xmm2
	mulps	%xmm2, %xmm3
	mulps	-12 * SIZE(BO), %xmm2
	addps	%xmm3, %xmm12
	movaps	 -8 * SIZE(BO), %xmm3
	addps	%xmm2, %xmm13
	movaps	 -8 * SIZE(AO), %xmm2

	mulps	%xmm2, %xmm3
	mulps	 -4 * SIZE(BO), %xmm2
	addps	%xmm3, %xmm8
	movaps	 -8 * SIZE(BO), %xmm3
	addps	%xmm2, %xmm9
	movaps	 -4 * SIZE(AO), %xmm2
	mulps	%xmm2, %xmm3
	mulps	 -4 * SIZE(BO), %xmm2
	addps	%xmm3, %xmm12
	movaps	 48 * SIZE(BO), %xmm3
	addps	%xmm2, %xmm13
	movaps	 48 * SIZE(AO), %xmm2

	PREFETCH	(PREFETCHSIZE + 32) * SIZE(AO)

	mulps	%xmm4, %xmm5
	mulps	  4 * SIZE(BO), %xmm4
	addps	%xmm5, %xmm8
	movaps	  0 * SIZE(BO), %xmm5
	addps	%xmm4, %xmm9
	movaps	  4 * SIZE(AO), %xmm4
	mulps	%xmm4, %xmm5
	mulps	  4 * SIZE(BO), %xmm4
	addps	%xmm5, %xmm12
	movaps	  8 * SIZE(BO), %xmm5
	addps	%xmm4, %xmm13
	movaps	  8 * SIZE(AO), %xmm4

	mulps	%xmm4, %xmm5
	mulps	 12 * SIZE(BO), %xmm4
	addps	%xmm5, %xmm8
	movaps	  8 * SIZE(BO), %xmm5
	addps	%xmm4, %xmm9
	movaps	 12 * SIZE(AO), %xmm4
	mulps	%xmm4, %xmm5
	mulps	 12 * SIZE(BO), %xmm4
	addps	%xmm5, %xmm12
	movaps	 64 * SIZE(BO), %xmm5
	addps	%xmm4, %xmm13
	movaps	 64 * SIZE(AO), %xmm4

	PREFETCH	(PREFETCHSIZE + 48) * SIZE(AO)

	mulps	%xmm6, %xmm7
	mulps	 20 * SIZE(BO), %xmm6
	addps	%xmm7, %xmm8
	movaps	 16 * SIZE(BO), %xmm7
	addps	%xmm6, %xmm9
	movaps	 20 * SIZE(AO), %xmm6
	mulps	%xmm6, %xmm7
	mulps	 20 * SIZE(BO), %xmm6
	addps	%xmm7, %xmm12
	movaps	 24 * SIZE(BO), %xmm7
	addps	%xmm6, %xmm13
	movaps	 24 * SIZE(AO), %xmm6

	mulps	%xmm6, %xmm7
	mulps	 28 * SIZE(BO), %xmm6
	addps	%xmm7, %xmm8
	movaps	 24 * SIZE(BO), %xmm7
	addps	%xmm6, %xmm9
	movaps	 28 * SIZE(AO), %xmm6
	mulps	%xmm6, %xmm7
	mulps	 28 * SIZE(BO), %xmm6
	addps	%xmm7, %xmm12
	movaps	 80 * SIZE(BO), %xmm7
	addps	%xmm6, %xmm13
	movaps	 80 * SIZE(AO), %xmm6

	subq   $-64 * SIZE, AO
	subq   $-64 * SIZE, BO

	decq   %rax
	jne    .L52
	ALIGN_4

.L55:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	movaps	ALPHA_R, %xmm6
	movaps	ALPHA_I, %xmm7
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L58
	ALIGN_4

.L56:
	mulps	%xmm0, %xmm1
	mulps	-28 * SIZE(BO), %xmm0
	addps	%xmm1, %xmm8
	movaps	-32 * SIZE(BO), %xmm1
	addps	%xmm0, %xmm9
	movaps	-28 * SIZE(AO), %xmm0
	mulps	%xmm0, %xmm1
	mulps	-28 * SIZE(BO), %xmm0
	addps	%xmm1, %xmm12
	movaps	-24 * SIZE(BO), %xmm1
	addps	%xmm0, %xmm13
	movaps	-24 * SIZE(AO), %xmm0

	addq	$ 8 * SIZE, AO
	addq	$ 8 * SIZE, BO
	decq	%rax
	jg	.L56
	ALIGN_4

.L58:
#ifndef TRMMKERNEL
	movsd	0 * SIZE(CO1), %xmm0
	movhps	2 * SIZE(CO1), %xmm0
	movsd	4 * SIZE(CO1), %xmm2
	movhps	6 * SIZE(CO1), %xmm2
#endif

	shufps	$0xb1, %xmm9, %xmm9
	shufps	$0xb1, %xmm13, %xmm13

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(RR) || defined(RC) || defined(CR) || defined(CC)
	subps	%xmm9, %xmm8
	subps	%xmm13, %xmm12
#else
	addps	%xmm9, %xmm8
	addps	%xmm13, %xmm12
#endif

	movaps	%xmm8, %xmm9
	movaps	%xmm12, %xmm13

	shufps	$0xb1, %xmm8, %xmm8
	shufps	$0xb1, %xmm12, %xmm12

	mulps	%xmm6, %xmm9
	mulps	%xmm7, %xmm8
	mulps	%xmm6, %xmm13
	mulps	%xmm7, %xmm12

	addps	%xmm9, %xmm8
	addps	%xmm13, %xmm12

#ifndef TRMMKERNEL
	addps	%xmm0,  %xmm8
	addps	%xmm2, %xmm12
#endif

	movlps	%xmm8,  0 * SIZE(CO1)
	movhps	%xmm8,  2 * SIZE(CO1)
	movlps	%xmm12, 4 * SIZE(CO1)
	movhps	%xmm12, 6 * SIZE(CO1)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax, 8), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 4), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$4, KK
#endif

	addq	$8 * SIZE, CO1		# coffset += 4
	decq	I			# i --
	jg	.L51
	ALIGN_4

.L60:
	testq	$2, M
	je	.L70

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	32 * SIZE + BUFFER, BO
#else
	leaq	32 * SIZE + BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax, 8), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 4), BO
#endif

	movaps	-32 * SIZE(AO), %xmm0
	pxor	%xmm8, %xmm8
	movaps	-16 * SIZE(AO), %xmm2
	pxor	%xmm9, %xmm9

	movaps	-32 * SIZE(BO), %xmm1
	pxor	%xmm10, %xmm10
	movaps	-16 * SIZE(BO), %xmm3
	pxor	%xmm11, %xmm11
	movaps	  0 * SIZE(BO), %xmm5
	movaps	 16 * SIZE(BO), %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movq	K, %rax
	subq	KK, %rax
	movq	%rax, KKK
#else
	movq	KK, %rax
#ifdef LEFT
	addq	$2, %rax
#else
	addq	$1, %rax
#endif
	movq	%rax, KKK
#endif

	sarq	$3, %rax
	je	.L65
	ALIGN_4

.L62:
	mulps	%xmm0, %xmm1
	PREFETCH	(PREFETCHSIZE +  0) * SIZE(AO)
	mulps	-28 * SIZE(BO), %xmm0
	addps	%xmm1, %xmm8
	movaps	-24 * SIZE(BO), %xmm1
	addps	%xmm0, %xmm9
	movaps  -28 * SIZE(AO), %xmm0
	mulps	%xmm0, %xmm1
	mulps	-20 * SIZE(BO), %xmm0
	addps	%xmm1, %xmm10
	movaps	 32 * SIZE(BO), %xmm1
	addps	%xmm0, %xmm11
	movaps  -24 * SIZE(AO), %xmm0

	mulps	%xmm0, %xmm3
	mulps	-12 * SIZE(BO), %xmm0
	addps	%xmm3, %xmm8
	movaps	 -8 * SIZE(BO), %xmm3
	addps	%xmm0, %xmm9
	movaps  -20 * SIZE(AO), %xmm0
	mulps	%xmm0, %xmm3
	mulps	 -4 * SIZE(BO), %xmm0
	addps	%xmm3, %xmm10
	movaps	 48 * SIZE(BO), %xmm3
	addps	%xmm0, %xmm11
	movaps    0 * SIZE(AO), %xmm0

	PREFETCH	(PREFETCHSIZE + 16) * SIZE(AO)

	mulps	%xmm2, %xmm5
	mulps	  4 * SIZE(BO), %xmm2
	addps	%xmm5, %xmm8
	movaps	  8 * SIZE(BO), %xmm5
	addps	%xmm2, %xmm9
	movaps  -12 * SIZE(AO), %xmm2
	mulps	%xmm2, %xmm5
	mulps	 12 * SIZE(BO), %xmm2
	addps	%xmm5, %xmm10
	movaps	 64 * SIZE(BO), %xmm5
	addps	%xmm2, %xmm11
	movaps   -8 * SIZE(AO), %xmm2

	mulps	%xmm2, %xmm7
	mulps	 20 * SIZE(BO), %xmm2
	addps	%xmm7, %xmm8
	movaps	 24 * SIZE(BO), %xmm7
	addps	%xmm2, %xmm9
	movaps   -4 * SIZE(AO), %xmm2
	mulps	%xmm2, %xmm7
	mulps	 28 * SIZE(BO), %xmm2
	addps	%xmm7, %xmm10
	movaps	 80 * SIZE(BO), %xmm7
	addps	%xmm2, %xmm11
	movaps   16 * SIZE(AO), %xmm2

	subq   $-32 * SIZE, AO
	subq   $-64 * SIZE, BO

	decq   %rax
	jne    .L62
	ALIGN_4

.L65:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	movaps	ALPHA_R, %xmm6
	movaps	ALPHA_I, %xmm7
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L68
	ALIGN_4

.L66:
	mulps	%xmm0, %xmm1
	mulps	-28 * SIZE(BO), %xmm0
	addps	%xmm1, %xmm8
	movaps	-24 * SIZE(BO), %xmm1
	addps	%xmm0, %xmm9
	movaps  -28 * SIZE(AO), %xmm0

	addq	$4 * SIZE, AO		# aoffset  += 4
	addq	$8 * SIZE, BO		# boffset1 += 8
	decq	%rax
	jg	.L66
	ALIGN_4

.L68:
#ifndef TRMMKERNEL
	movsd	0 * SIZE(CO1), %xmm0
	movhps	2 * SIZE(CO1), %xmm0
#endif

	addps	%xmm10, %xmm8
	addps	%xmm11, %xmm9

	shufps	$0xb1, %xmm9, %xmm9

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(RR) || defined(RC) || defined(CR) || defined(CC)
	subps	%xmm9, %xmm8
#else
	addps	%xmm9, %xmm8
#endif

	movaps	%xmm8, %xmm9

	shufps	$0xb1, %xmm8, %xmm8

	mulps	%xmm6, %xmm9
	mulps	%xmm7, %xmm8

	addps	%xmm9, %xmm8

#ifndef TRMMKERNEL
	addps	%xmm0,  %xmm8
#endif

	movsd	%xmm8, 0 * SIZE(CO1)
	movhps	%xmm8, 2 * SIZE(CO1)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax,  8), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 4), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$2, KK
#endif

	addq	$4 * SIZE, CO1		# coffset += 4
	ALIGN_4

.L70:
	testq	$1, M
	je	.L999

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	32 * SIZE + BUFFER, BO
#else
	leaq	32 * SIZE + BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax, 8), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 4), BO
#endif

	movaps	-32 * SIZE(AO), %xmm0
	pxor	%xmm8, %xmm8
	movaps	-24 * SIZE(AO), %xmm2
	pxor	%xmm9, %xmm9

	movaps	-32 * SIZE(BO), %xmm1
	pxor	%xmm10, %xmm10
	movaps	-16 * SIZE(BO), %xmm3
	pxor	%xmm11, %xmm11
	movaps	  0 * SIZE(BO), %xmm5
	movaps	 16 * SIZE(BO), %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movq	K, %rax
	subq	KK, %rax
	movq	%rax, KKK
#else
	movq	KK, %rax
#ifdef LEFT
	addq	$1, %rax
#else
	addq	$1, %rax
#endif
	movq	%rax, KKK
#endif
	sarq	$3, %rax
	je	.L75
	ALIGN_4

.L72:
	mulps	%xmm0, %xmm1
	PREFETCH	(PREFETCHSIZE +  0) * SIZE(AO)
	addps	%xmm1, %xmm8
	movaps	-28 * SIZE(BO), %xmm1
	mulps	%xmm0, %xmm1
	movsd	-30 * SIZE(AO), %xmm0
	addps	%xmm1, %xmm9
	movaps	-24 * SIZE(BO), %xmm1

	mulps	%xmm0, %xmm1
	addps	%xmm1, %xmm10
	movaps	-20 * SIZE(BO), %xmm1
	mulps	%xmm0, %xmm1
	movsd	-28 * SIZE(AO), %xmm0
	addps	%xmm1, %xmm11
	movaps	 32 * SIZE(BO), %xmm1

	mulps	%xmm0, %xmm3
	addps	%xmm3, %xmm8
	movaps	-12 * SIZE(BO), %xmm3
	mulps	%xmm0, %xmm3
	movsd	-26 * SIZE(AO), %xmm0
	addps	%xmm3, %xmm9
	movaps	 -8 * SIZE(BO), %xmm3

	mulps	%xmm0, %xmm3
	addps	%xmm3, %xmm10
	movaps	 -4 * SIZE(BO), %xmm3
	mulps	%xmm0, %xmm3
	movsd	-16 * SIZE(AO), %xmm0
	addps	%xmm3, %xmm11
	movaps	 48 * SIZE(BO), %xmm3

	mulps	%xmm2, %xmm5
	addps	%xmm5, %xmm8
	movaps	  4 * SIZE(BO), %xmm5
	mulps	%xmm2, %xmm5
	movsd	-22 * SIZE(AO), %xmm2
	addps	%xmm5, %xmm9
	movaps	  8 * SIZE(BO), %xmm5

	mulps	%xmm2, %xmm5
	addps	%xmm5, %xmm10
	movaps	 12 * SIZE(BO), %xmm5
	mulps	%xmm2, %xmm5
	movsd	-20 * SIZE(AO), %xmm2
	addps	%xmm5, %xmm11
	movaps	 64 * SIZE(BO), %xmm5

	mulps	%xmm2, %xmm7
	addps	%xmm7, %xmm8
	movaps	 20 * SIZE(BO), %xmm7
	mulps	%xmm2, %xmm7
	movsd	-18 * SIZE(AO), %xmm2
	addps	%xmm7, %xmm9
	movaps	 24 * SIZE(BO), %xmm7

	mulps	%xmm2, %xmm7
	addps	%xmm7, %xmm10
	movaps	 28 * SIZE(BO), %xmm7
	mulps	%xmm2, %xmm7
	movsd	 -8 * SIZE(AO), %xmm2
	addps	%xmm7, %xmm11
	movaps	 80 * SIZE(BO), %xmm7

	subq   $-16 * SIZE, AO
	subq   $-64 * SIZE, BO

	decq   %rax
	jne    .L72
	ALIGN_4

.L75:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	movaps	ALPHA_R, %xmm6
	movaps	ALPHA_I, %xmm7
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L78
	ALIGN_4

.L76:
	mulps	%xmm0, %xmm1
	addps	%xmm1, %xmm8
	movaps	-28 * SIZE(BO), %xmm1
	mulps	%xmm0, %xmm1
	movsd	-30 * SIZE(AO), %xmm0
	addps	%xmm1, %xmm9
	movaps	-24 * SIZE(BO), %xmm1

	addq	$2 * SIZE, AO
	addq	$8 * SIZE, BO
	decq	%rax
	jg	.L76
	ALIGN_4

.L78:
#ifndef TRMMKERNEL
#ifdef	movsd
	xorps	%xmm0, %xmm0
#endif
	movsd	0 * SIZE(CO1), %xmm0
#endif

	addps	%xmm10, %xmm8
	addps	%xmm11, %xmm9

	shufps	$0xb1, %xmm9, %xmm9

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(RR) || defined(RC) || defined(CR) || defined(CC)
	subps	%xmm9, %xmm8
#else
	addps	%xmm9, %xmm8
#endif

	movaps	%xmm8, %xmm9

	shufps	$0xb1, %xmm8, %xmm8

	mulps	%xmm6, %xmm9
	mulps	%xmm7, %xmm8

	addps	%xmm9, %xmm8
#ifndef TRMMKERNEL
	addps	%xmm0,  %xmm8
#endif
	movlps	%xmm8, 0 * SIZE(CO1)
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
