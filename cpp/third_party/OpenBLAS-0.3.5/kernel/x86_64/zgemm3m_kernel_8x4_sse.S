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

#define ALPHA	  0(%rsp)
#define J	 16(%rsp)
#define OFFSET	 24(%rsp)
#define KK	 32(%rsp)
#define KKK	 40(%rsp)
#define BUFFER	128(%rsp)

#ifdef OPTERON
#define movsd movlps
#endif

#if defined(OPTERON) || defined(BARCELONA) || defined(SHANGHAI)
#define PREFETCH     prefetch
#define PREFETCHW    prefetchw
#define PREFETCHNTA  prefetchnta
#define PREFETCHSIZE (16 * 5 + 8)
#endif

#ifdef GENERIC
#define PREFETCH     prefetcht0
#define PREFETCHW    prefetcht0
#define PREFETCHNTA  prefetchnta
#define PREFETCHSIZE (16 * 5 + 8)
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

#if defined(OS_LINUX) && defined(CORE_BARCELONA)
	.align 32768
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
	movsd	OLD_OFFSET, %xmm4
#endif
	movaps	%xmm3, %xmm0
	movss	OLD_ALPHA_I, %xmm1
#else
	movq	OLD_LDC,   LDC
#ifdef TRMMKERNEL
	movsd	OLD_OFFSET, %xmm4
#endif

#endif

	EMMS

	movq	%rsp, %rbx	# save old stack
	subq	$128 + LOCAL_BUFFER_SIZE, %rsp
	andq	$-1024, %rsp	# align stack

	STACK_TOUCHING

	movq	OLD_M, M
	movq	OLD_N, N

	movss	%xmm0,  0 + ALPHA
	movss	%xmm1,  4 + ALPHA
	movss	%xmm0,  8 + ALPHA
	movss	%xmm1, 12 + ALPHA

#ifdef TRMMKERNEL
	movsd	%xmm4, OFFSET
	movsd	%xmm4, KK
#ifndef LEFT
	negq	KK
#endif
#endif

	subq	$-32 * SIZE, A

	salq	$ZBASE_SHIFT, LDC

	movq	N,  J
	sarq	$2, J		# j = (n >> 2)
	jle	.L50

.L01:
#if defined(TRMMKERNEL) && defined(LEFT)
	movq	OFFSET, %rax
	movq	%rax, KK
#endif

/* Copying to Sub Buffer */
	leaq	BUFFER, BO

	movq	K, %rax
	sarq	$2, %rax
	jle	.L03

	addq	%rax, %rax
	ALIGN_4

.L02:
	PREFETCH	 (RPREFETCHSIZE +  0)  * SIZE(B)

	movd	 0 * SIZE(B), %mm0
	movd	 1 * SIZE(B), %mm1
	movd	 2 * SIZE(B), %mm2
	movd	 3 * SIZE(B), %mm3
	movd	 4 * SIZE(B), %mm4
	movd	 5 * SIZE(B), %mm5
	movd	 6 * SIZE(B), %mm6
	movd	 7 * SIZE(B), %mm7

	PREFETCHW	 (WPREFETCHSIZE +  0) * SIZE(BO)

	punpckldq %mm0, %mm0
	punpckldq %mm1, %mm1
	punpckldq %mm2, %mm2
	punpckldq %mm3, %mm3
	punpckldq %mm4, %mm4
	punpckldq %mm5, %mm5
	punpckldq %mm6, %mm6
	punpckldq %mm7, %mm7

	movq	%mm0,  0 * SIZE(BO)
	movq	%mm0,  2 * SIZE(BO)
	movq	%mm1,  4 * SIZE(BO)
	movq	%mm1,  6 * SIZE(BO)
	movq	%mm2,  8 * SIZE(BO)
	movq	%mm2, 10 * SIZE(BO)
	movq	%mm3, 12 * SIZE(BO)
	movq	%mm3, 14 * SIZE(BO)

	PREFETCHW	 (WPREFETCHSIZE + 16) * SIZE(BO)

	movq	%mm4, 16 * SIZE(BO)
	movq	%mm4, 18 * SIZE(BO)
	movq	%mm5, 20 * SIZE(BO)
	movq	%mm5, 22 * SIZE(BO)
	movq	%mm6, 24 * SIZE(BO)
	movq	%mm6, 26 * SIZE(BO)
	movq	%mm7, 28 * SIZE(BO)
	movq	%mm7, 30 * SIZE(BO)

	addq	$ 8 * SIZE, B
	addq	$32 * SIZE, BO

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
	movd	 0 * SIZE(B), %mm0
	movd	 1 * SIZE(B), %mm1
	movd	 2 * SIZE(B), %mm2
	movd	 3 * SIZE(B), %mm3

	punpckldq %mm0, %mm0
	punpckldq %mm1, %mm1
	punpckldq %mm2, %mm2
	punpckldq %mm3, %mm3

	movq	%mm0,  0 * SIZE(BO)
	movq	%mm0,  2 * SIZE(BO)
	movq	%mm1,  4 * SIZE(BO)
	movq	%mm1,  6 * SIZE(BO)
	movq	%mm2,  8 * SIZE(BO)
	movq	%mm2, 10 * SIZE(BO)
	movq	%mm3, 12 * SIZE(BO)
	movq	%mm3, 14 * SIZE(BO)

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
	sarq	$3, I	# i = (m >> 3)
	jle	.L20
	ALIGN_4

.L11:
	PREFETCH	  0  * SIZE(BB)
	subq		 $-16 * SIZE, BB

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

	PREFETCHW     15 * SIZE(CO1)
	pxor	%xmm12, %xmm12
	PREFETCHW     15 * SIZE(CO2)
	pxor	%xmm13, %xmm13
	PREFETCHW     15 * SIZE(CO1, LDC, 2)
	pxor	%xmm14, %xmm14
	PREFETCHW     15 * SIZE(CO2, LDC, 2)
	pxor	%xmm15, %xmm15

#ifndef TRMMKERNEL
	movq	K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movq	K, %rax
	subq	KK, %rax
	movq	%rax, KKK
#else
	movq	KK, %rax
#ifdef LEFT
	addq	$8, %rax
#else
	addq	$4, %rax
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
	movaps	ALPHA, %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
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
	movsd	 0 * SIZE(CO1), %xmm0
	movhps	 2 * SIZE(CO1), %xmm0
	movsd	 4 * SIZE(CO1), %xmm1
	movhps	 6 * SIZE(CO1), %xmm1
	movsd	 8 * SIZE(CO1), %xmm2
	movhps	10 * SIZE(CO1), %xmm2
	movsd	12 * SIZE(CO1), %xmm3
	movhps	14 * SIZE(CO1), %xmm3

	pshufd	$0x50, %xmm8,  %xmm4
	pshufd	$0xfa, %xmm8,  %xmm8
	pshufd	$0x50, %xmm12, %xmm5
	pshufd	$0xfa, %xmm12, %xmm12

	mulps	%xmm7, %xmm4
	mulps	%xmm7, %xmm8
	mulps	%xmm7, %xmm5
	mulps	%xmm7, %xmm12

	addps	%xmm0, %xmm4
	addps	%xmm1, %xmm8
	addps	%xmm2, %xmm5
	addps	%xmm3, %xmm12

	movlps	%xmm4,   0 * SIZE(CO1)
	movhps	%xmm4,   2 * SIZE(CO1)
	movlps	%xmm8,   4 * SIZE(CO1)
	movhps	%xmm8,   6 * SIZE(CO1)
	movlps	%xmm5,   8 * SIZE(CO1)
	movhps	%xmm5,  10 * SIZE(CO1)
	movlps	%xmm12, 12 * SIZE(CO1)
	movhps	%xmm12, 14 * SIZE(CO1)

	movsd	 0 * SIZE(CO2), %xmm0
	movhps	 2 * SIZE(CO2), %xmm0
	movsd	 4 * SIZE(CO2), %xmm1
	movhps	 6 * SIZE(CO2), %xmm1
	movsd	 8 * SIZE(CO2), %xmm2
	movhps	10 * SIZE(CO2), %xmm2
	movsd	12 * SIZE(CO2), %xmm3
	movhps	14 * SIZE(CO2), %xmm3

	pshufd	$0x50, %xmm9,  %xmm4
	pshufd	$0xfa, %xmm9,  %xmm9
	pshufd	$0x50, %xmm13, %xmm5
	pshufd	$0xfa, %xmm13, %xmm13

	mulps	%xmm7, %xmm4
	mulps	%xmm7, %xmm9
	mulps	%xmm7, %xmm5
	mulps	%xmm7, %xmm13

	addps	%xmm0, %xmm4
	addps	%xmm1, %xmm9
	addps	%xmm2, %xmm5
	addps	%xmm3, %xmm13

	movlps	%xmm4,   0 * SIZE(CO2)
	movhps	%xmm4,   2 * SIZE(CO2)
	movlps	%xmm9,   4 * SIZE(CO2)
	movhps	%xmm9,   6 * SIZE(CO2)
	movlps	%xmm5,   8 * SIZE(CO2)
	movhps	%xmm5,  10 * SIZE(CO2)
	movlps	%xmm13, 12 * SIZE(CO2)
	movhps	%xmm13, 14 * SIZE(CO2)

	movsd	 0 * SIZE(CO1, LDC, 2), %xmm0
	movhps	 2 * SIZE(CO1, LDC, 2), %xmm0
	movsd	 4 * SIZE(CO1, LDC, 2), %xmm1
	movhps	 6 * SIZE(CO1, LDC, 2), %xmm1
	movsd	 8 * SIZE(CO1, LDC, 2), %xmm2
	movhps	10 * SIZE(CO1, LDC, 2), %xmm2
	movsd	12 * SIZE(CO1, LDC, 2), %xmm3
	movhps	14 * SIZE(CO1, LDC, 2), %xmm3

	pshufd	$0x50, %xmm10,  %xmm4
	pshufd	$0xfa, %xmm10,  %xmm10
	pshufd	$0x50, %xmm14, %xmm5
	pshufd	$0xfa, %xmm14, %xmm14

	mulps	%xmm7, %xmm4
	mulps	%xmm7, %xmm10
	mulps	%xmm7, %xmm5
	mulps	%xmm7, %xmm14

	addps	%xmm0, %xmm4
	addps	%xmm1, %xmm10
	addps	%xmm2, %xmm5
	addps	%xmm3, %xmm14

	movlps	%xmm4,   0 * SIZE(CO1, LDC, 2)
	movhps	%xmm4,   2 * SIZE(CO1, LDC, 2)
	movlps	%xmm10,  4 * SIZE(CO1, LDC, 2)
	movhps	%xmm10,  6 * SIZE(CO1, LDC, 2)
	movlps	%xmm5,   8 * SIZE(CO1, LDC, 2)
	movhps	%xmm5,  10 * SIZE(CO1, LDC, 2)
	movlps	%xmm14, 12 * SIZE(CO1, LDC, 2)
	movhps	%xmm14, 14 * SIZE(CO1, LDC, 2)

	movsd	 0 * SIZE(CO2, LDC, 2), %xmm0
	movhps	 2 * SIZE(CO2, LDC, 2), %xmm0
	movsd	 4 * SIZE(CO2, LDC, 2), %xmm1
	movhps	 6 * SIZE(CO2, LDC, 2), %xmm1
	movsd	 8 * SIZE(CO2, LDC, 2), %xmm2
	movhps	10 * SIZE(CO2, LDC, 2), %xmm2
	movsd	12 * SIZE(CO2, LDC, 2), %xmm3
	movhps	14 * SIZE(CO2, LDC, 2), %xmm3

	pshufd	$0x50, %xmm11, %xmm4
	pshufd	$0xfa, %xmm11, %xmm11
	pshufd	$0x50, %xmm15, %xmm5
	pshufd	$0xfa, %xmm15, %xmm15

	mulps	%xmm7, %xmm4
	mulps	%xmm7, %xmm11
	mulps	%xmm7, %xmm5
	mulps	%xmm7, %xmm15

	addps	%xmm0, %xmm4
	addps	%xmm1, %xmm11
	addps	%xmm2, %xmm5
	addps	%xmm3, %xmm15

	movlps	%xmm4,   0 * SIZE(CO2, LDC, 2)
	movhps	%xmm4,   2 * SIZE(CO2, LDC, 2)
	movlps	%xmm11,  4 * SIZE(CO2, LDC, 2)
	movhps	%xmm11,  6 * SIZE(CO2, LDC, 2)
	movlps	%xmm5,   8 * SIZE(CO2, LDC, 2)
	movhps	%xmm5,  10 * SIZE(CO2, LDC, 2)
	movlps	%xmm15, 12 * SIZE(CO2, LDC, 2)
	movhps	%xmm15, 14 * SIZE(CO2, LDC, 2)

	addq	$16 * SIZE, CO1		# coffset += 4
	addq	$16 * SIZE, CO2		# coffset += 4
	decq	I			# i --
	jg	.L11
	ALIGN_4

.L20:
	testq	$4, M
	je	.L30

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	BUFFER, BO
#else
	leaq	BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax,   8), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 8), BO
#endif

	movaps	-32 * SIZE(AO), %xmm8
	movaps	-16 * SIZE(AO), %xmm10

	movaps	  0 * SIZE(BO), %xmm9
	movaps	 16 * SIZE(BO), %xmm11
	movaps	 32 * SIZE(BO), %xmm13
	movaps	 48 * SIZE(BO), %xmm15

	pxor	%xmm0, %xmm0
	pxor	%xmm1, %xmm1
	pxor	%xmm2, %xmm2
	pxor	%xmm3, %xmm3

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
	addq	$4, %rax
#endif
	movq	%rax, KKK
#endif
	sarq	$3, %rax
	je	.L25
	ALIGN_4

.L22:
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm0
#if defined(OPTERON) || defined(BARCELONA) || defined(SHANGHAI)
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
	movaps	-28 * SIZE(AO), %xmm8

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
	movaps	-24 * SIZE(AO), %xmm8

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
	movaps	-20 * SIZE(AO), %xmm8

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
	movaps	 0 * SIZE(AO), %xmm8

#if defined(OPTERON) || defined(BARCELONA) || defined(SHANGHAI)
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
	movaps	-12 * SIZE(AO), %xmm10

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
	movaps	-8 * SIZE(AO), %xmm10

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
	movaps	-4 * SIZE(AO), %xmm10

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
	movaps	16 * SIZE(AO), %xmm10

	addq   $ 32 * SIZE, AO
	addq   $128 * SIZE, BO
	decq   %rax
	jne    .L22
	ALIGN_4

.L25:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	movaps	ALPHA, %xmm7
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
	movaps	-28 * SIZE(AO), %xmm8

	addq	$ 4 * SIZE, AO		# aoffset  += 4
	addq	$16 * SIZE, BO		# boffset1 += 8
	decq	%rax
	jg	.L26
	ALIGN_4

.L28:
	movsd	 0 * SIZE(CO1), %xmm8
	movhps	 2 * SIZE(CO1), %xmm8
	movsd	 4 * SIZE(CO1), %xmm9
	movhps	 6 * SIZE(CO1), %xmm9

	pshufd	$0x50, %xmm0,  %xmm4
	pshufd	$0xfa, %xmm0,  %xmm0

	mulps	%xmm7, %xmm4
	mulps	%xmm7, %xmm0

	addps	%xmm8, %xmm4
	addps	%xmm9, %xmm0

	movlps	%xmm4,   0 * SIZE(CO1)
	movhps	%xmm4,   2 * SIZE(CO1)
	movlps	%xmm0,   4 * SIZE(CO1)
	movhps	%xmm0,   6 * SIZE(CO1)

	movsd	 0 * SIZE(CO2), %xmm8
	movhps	 2 * SIZE(CO2), %xmm8
	movsd	 4 * SIZE(CO2), %xmm9
	movhps	 6 * SIZE(CO2), %xmm9

	pshufd	$0x50, %xmm1,  %xmm4
	pshufd	$0xfa, %xmm1,  %xmm1

	mulps	%xmm7, %xmm4
	mulps	%xmm7, %xmm1

	addps	%xmm8, %xmm4
	addps	%xmm9, %xmm1

	movlps	%xmm4,   0 * SIZE(CO2)
	movhps	%xmm4,   2 * SIZE(CO2)
	movlps	%xmm1,   4 * SIZE(CO2)
	movhps	%xmm1,   6 * SIZE(CO2)

	movsd	 0 * SIZE(CO1, LDC, 2), %xmm8
	movhps	 2 * SIZE(CO1, LDC, 2), %xmm8
	movsd	 4 * SIZE(CO1, LDC, 2), %xmm9
	movhps	 6 * SIZE(CO1, LDC, 2), %xmm9

	pshufd	$0x50, %xmm2,  %xmm4
	pshufd	$0xfa, %xmm2,  %xmm2

	mulps	%xmm7, %xmm4
	mulps	%xmm7, %xmm2

	addps	%xmm8, %xmm4
	addps	%xmm9, %xmm2

	movlps	%xmm4,   0 * SIZE(CO1, LDC, 2)
	movhps	%xmm4,   2 * SIZE(CO1, LDC, 2)
	movlps	%xmm2,   4 * SIZE(CO1, LDC, 2)
	movhps	%xmm2,   6 * SIZE(CO1, LDC, 2)

	movsd	 0 * SIZE(CO2, LDC, 2), %xmm8
	movhps	 2 * SIZE(CO2, LDC, 2), %xmm8
	movsd	 4 * SIZE(CO2, LDC, 2), %xmm9
	movhps	 6 * SIZE(CO2, LDC, 2), %xmm9

	pshufd	$0x50, %xmm3,  %xmm4
	pshufd	$0xfa, %xmm3,  %xmm3

	mulps	%xmm7, %xmm4
	mulps	%xmm7, %xmm3

	addps	%xmm8, %xmm4
	addps	%xmm9, %xmm3

	movlps	%xmm4,   0 * SIZE(CO2, LDC, 2)
	movhps	%xmm4,   2 * SIZE(CO2, LDC, 2)
	movlps	%xmm3,   4 * SIZE(CO2, LDC, 2)
	movhps	%xmm3,   6 * SIZE(CO2, LDC, 2)

	addq	$8 * SIZE, CO1		# coffset += 4
	addq	$8 * SIZE, CO2		# coffset += 4
	ALIGN_4

.L30:
	testq	$2, M
	je	.L40

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	BUFFER, BO
#else
	leaq	BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax,   8), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 8), BO
#endif

	movaps	-32 * SIZE(AO), %xmm8
	movaps	-24 * SIZE(AO), %xmm10

	movaps	 0 * SIZE(BO), %xmm9
	movaps	16 * SIZE(BO), %xmm11
	movaps	32 * SIZE(BO), %xmm13
	movaps	48 * SIZE(BO), %xmm15

	pxor	%xmm0, %xmm0
	pxor	%xmm1, %xmm1
	pxor	%xmm2, %xmm2
	pxor	%xmm3, %xmm3

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
	addq	$4, %rax
#endif
	movq	%rax, KKK
#endif
	sarq	$3, %rax
	je	.L35
	ALIGN_4

.L32:
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm0
#if defined(OPTERON) || defined(BARCELONA) || defined(SHANGHAI)
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
	movsd	-30 * SIZE(AO), %xmm8
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
	movsd	-28 * SIZE(AO), %xmm8
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
	movsd	-26 * SIZE(AO), %xmm8
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
	movsd	-16 * SIZE(AO), %xmm8
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
	movsd	-22 * SIZE(AO), %xmm10
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
	movsd	-20 * SIZE(AO), %xmm10
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
	movsd	-18 * SIZE(AO), %xmm10
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
	movsd	 -8 * SIZE(AO), %xmm10
	addps	%xmm15, %xmm3
	movaps	176 * SIZE(BO), %xmm15

	addq   $ 16 * SIZE, AO
	addq   $128 * SIZE, BO
	decq   %rax
	jne    .L32
	ALIGN_4

.L35:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	movaps	ALPHA, %xmm7
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L38
	ALIGN_4

.L36:
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm0
	movaps	  4 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm1
	movaps	  8 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm2
	movaps	 12 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	movsd	-30 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm3
	movaps	 16 * SIZE(BO), %xmm9

	addq	$ 2 * SIZE, AO		# aoffset  += 4
	addq	$16 * SIZE, BO		# boffset1 += 8
	decq	%rax
	jg	.L36
	ALIGN_4

.L38:
	movsd	 0 * SIZE(CO1), %xmm8
	movhps	 2 * SIZE(CO1), %xmm8

	pshufd	$0x50, %xmm0,  %xmm4
	mulps	%xmm7, %xmm4
	addps	%xmm8, %xmm4

	movlps	%xmm4,   0 * SIZE(CO1)
	movhps	%xmm4,   2 * SIZE(CO1)

	movsd	 0 * SIZE(CO2), %xmm8
	movhps	 2 * SIZE(CO2), %xmm8

	pshufd	$0x50, %xmm1,  %xmm4
	mulps	%xmm7, %xmm4
	addps	%xmm8, %xmm4

	movlps	%xmm4,   0 * SIZE(CO2)
	movhps	%xmm4,   2 * SIZE(CO2)

	movsd	 0 * SIZE(CO1, LDC, 2), %xmm8
	movhps	 2 * SIZE(CO1, LDC, 2), %xmm8

	pshufd	$0x50, %xmm2,  %xmm4
	mulps	%xmm7, %xmm4
	addps	%xmm8, %xmm4

	movlps	%xmm4,   0 * SIZE(CO1, LDC, 2)
	movhps	%xmm4,   2 * SIZE(CO1, LDC, 2)

	movsd	 0 * SIZE(CO2, LDC, 2), %xmm8
	movhps	 2 * SIZE(CO2, LDC, 2), %xmm8

	pshufd	$0x50, %xmm3,  %xmm4
	mulps	%xmm7, %xmm4
	addps	%xmm8, %xmm4

	movlps	%xmm4,   0 * SIZE(CO2, LDC, 2)
	movhps	%xmm4,   2 * SIZE(CO2, LDC, 2)

	addq	$4 * SIZE, CO1		# coffset += 4
	addq	$4 * SIZE, CO2		# coffset += 4
	ALIGN_4

.L40:
	testq	$1, M
	je	.L49

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	BUFFER, BO
#else
	leaq	BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax,   4), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 8), BO
	leaq	(BO, %rax, 8), BO
#endif

	movss	-32 * SIZE(AO), %xmm8
	movss	-28 * SIZE(AO), %xmm10

	movss	 0 * SIZE(BO), %xmm9
	movss	16 * SIZE(BO), %xmm11
	movss	32 * SIZE(BO), %xmm13
	movss	48 * SIZE(BO), %xmm15

	pxor	%xmm0, %xmm0
	pxor	%xmm1, %xmm1
	pxor	%xmm2, %xmm2
	pxor	%xmm3, %xmm3

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
	addq	$4, %rax
#endif
	movq	%rax, KKK
#endif
	sarq	$3, %rax
	je	.L45
	ALIGN_4

.L42:
	mulss	%xmm8, %xmm9
	addss	%xmm9, %xmm0
#if defined(OPTERON) || defined(BARCELONA) || defined(SHANGHAI)
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
	movss	-31 * SIZE(AO), %xmm8
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
	movss	-30 * SIZE(AO), %xmm8
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
	movss	-29 * SIZE(AO), %xmm8
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
	movss	-24 * SIZE(AO), %xmm8
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
	movss	-27 * SIZE(AO), %xmm10
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
	movss	-26 * SIZE(AO), %xmm10
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
	movss	-25 * SIZE(AO), %xmm10
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
	movss	-20 * SIZE(AO), %xmm10
	addss	%xmm15, %xmm3
	movss	176 * SIZE(BO), %xmm15

	addq   $  8 * SIZE, AO
	addq   $128 * SIZE, BO
	decq   %rax
	jne    .L42
	ALIGN_4

.L45:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	movaps	ALPHA, %xmm7
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
	movss	-31 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm3
	movss	16 * SIZE(BO), %xmm9

	addq	$ 1 * SIZE, AO		# aoffset  += 4
	addq	$16 * SIZE, BO		# boffset1 += 8
	decq	%rax
	jg	.L46
	ALIGN_4

.L48:
	movsd	 0 * SIZE(CO1), %xmm8

	pshufd	$0x50, %xmm0,  %xmm4
	mulps	%xmm7, %xmm4
	addps	%xmm8, %xmm4

	movlps	%xmm4,   0 * SIZE(CO1)

	movsd	 0 * SIZE(CO2), %xmm8

	pshufd	$0x50, %xmm1,  %xmm4
	mulps	%xmm7, %xmm4
	addps	%xmm8, %xmm4

	movlps	%xmm4,   0 * SIZE(CO2)

	movsd	 0 * SIZE(CO1, LDC, 2), %xmm8

	pshufd	$0x50, %xmm2,  %xmm4
	mulps	%xmm7, %xmm4
	addps	%xmm8, %xmm4

	movlps	%xmm4,   0 * SIZE(CO1, LDC, 2)

	movsd	 0 * SIZE(CO2, LDC, 2), %xmm8

	pshufd	$0x50, %xmm3,  %xmm4
	mulps	%xmm7, %xmm4
	addps	%xmm8, %xmm4

	movlps	%xmm4,   0 * SIZE(CO2, LDC, 2)
	ALIGN_4

.L49:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addl	$4, KK
#endif
	leaq	(C, LDC, 4), C		# c += 4 * ldc
	decq	J			# j --
	jg	.L01

.L50:
	testq	$2, N
	je	.L100

.L51:
#if defined(TRMMKERNEL) && defined(LEFT)
	movq	OFFSET, %rax
	movq	%rax, KK
#endif

/* Copying to Sub Buffer */
	leaq	BUFFER, BO

	movq	K, %rax
	sarq	$2, %rax
	jle	.L53
	ALIGN_4

.L52:
#if defined(PENTIUM4) || defined(GENERIC)
	movss	 0 * SIZE(B), %xmm0
	movss	 1 * SIZE(B), %xmm1
	movss	 2 * SIZE(B), %xmm2
	movss	 3 * SIZE(B), %xmm3
	movss	 4 * SIZE(B), %xmm4
	movss	 5 * SIZE(B), %xmm5
	movss	 6 * SIZE(B), %xmm6
	movss	 7 * SIZE(B), %xmm7

	PREFETCHNTA	 32 * SIZE(B)

	shufps	 $0, %xmm0, %xmm0
	shufps	 $0, %xmm1, %xmm1
	shufps	 $0, %xmm2, %xmm2
	shufps	 $0, %xmm3, %xmm3
	shufps	 $0, %xmm4, %xmm4
	shufps	 $0, %xmm5, %xmm5
	shufps	 $0, %xmm6, %xmm6
	shufps	 $0, %xmm7, %xmm7

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
#endif

#if defined(OPTERON) || defined(BARCELONA) || defined(SHANGHAI)
	PREFETCHNTA	 32 * SIZE(B)

	movd	 0 * SIZE(B), %mm0
	movd	 1 * SIZE(B), %mm1
	movd	 2 * SIZE(B), %mm2
	movd	 3 * SIZE(B), %mm3
	movd	 4 * SIZE(B), %mm4
	movd	 5 * SIZE(B), %mm5
	movd	 6 * SIZE(B), %mm6
	movd	 7 * SIZE(B), %mm7

	punpckldq %mm0, %mm0
	punpckldq %mm1, %mm1
	punpckldq %mm2, %mm2
	punpckldq %mm3, %mm3
	punpckldq %mm4, %mm4
	punpckldq %mm5, %mm5
	punpckldq %mm6, %mm6
	punpckldq %mm7, %mm7

	movq	%mm0,  0 * SIZE(BO)
	movq	%mm0,  2 * SIZE(BO)
	movq	%mm1,  4 * SIZE(BO)
	movq	%mm1,  6 * SIZE(BO)
	movq	%mm2,  8 * SIZE(BO)
	movq	%mm2, 10 * SIZE(BO)
	movq	%mm3, 12 * SIZE(BO)
	movq	%mm3, 14 * SIZE(BO)
	movq	%mm4, 16 * SIZE(BO)
	movq	%mm4, 18 * SIZE(BO)
	movq	%mm5, 20 * SIZE(BO)
	movq	%mm5, 22 * SIZE(BO)
	movq	%mm6, 24 * SIZE(BO)
	movq	%mm6, 26 * SIZE(BO)
	movq	%mm7, 28 * SIZE(BO)
	movq	%mm7, 30 * SIZE(BO)

	addq	$ 8 * SIZE, B
	addq	$32 * SIZE, BO
#endif

	decq	%rax
	jne	.L52
	ALIGN_4

.L53:
	movq	K, %rax
	andq	$3, %rax
	BRANCH
	jle	.L60
	ALIGN_4

.L54:
#if defined(PENTIUM4) || defined(GENERIC)
	movss	 0 * SIZE(B), %xmm0
	movss	 1 * SIZE(B), %xmm1

	shufps	 $0, %xmm0, %xmm0
	shufps	 $0, %xmm1, %xmm1

	movaps	%xmm0,  0 * SIZE(BO)
	movaps	%xmm1,  4 * SIZE(BO)
#endif

#if defined(OPTERON) || defined(BARCELONA) || defined(SHANGHAI)
	movd	 0 * SIZE(B), %mm0
	movd	 1 * SIZE(B), %mm1

	punpckldq %mm0, %mm0
	punpckldq %mm1, %mm1

	movq	%mm0,  0 * SIZE(BO)
	movq	%mm0,  2 * SIZE(BO)
	movq	%mm1,  4 * SIZE(BO)
	movq	%mm1,  6 * SIZE(BO)
#endif

	addq	$ 2 * SIZE, B
	addq	$ 8 * SIZE, BO
	decq	%rax
	jne	.L54
	ALIGN_4

.L60:
	movq	C, CO1			# coffset1 = c
	leaq	(C, LDC, 1), CO2	# coffset2 = c + ldc
	movq	A, AO		# aoffset = a

	movq	M,  I
	sarq	$3, I	# i = (m >> 3)
	jle	.L70
	ALIGN_4

.L61:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	BUFFER, BO
#else
	leaq	BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax,   8), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 4), BO
#endif

	movaps	-32 * SIZE(AO), %xmm8
	movaps	-16 * SIZE(AO), %xmm10
	movaps	  0 * SIZE(AO), %xmm12
	movaps	 16 * SIZE(AO), %xmm14

	movaps	 0 * SIZE(BO), %xmm9
	movaps	16 * SIZE(BO), %xmm11
	movaps	32 * SIZE(BO), %xmm13
	movaps	48 * SIZE(BO), %xmm15

	pxor	%xmm0, %xmm0
	pxor	%xmm1, %xmm1

	PREFETCHW     15 * SIZE(CO1)
	pxor	%xmm4, %xmm4
	PREFETCHW     15 * SIZE(CO2)
	pxor	%xmm5, %xmm5

#ifndef TRMMKERNEL
	movq	K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movq	K, %rax
	subq	KK, %rax
	movq	%rax, KKK
#else
	movq	KK, %rax
#ifdef LEFT
	addq	$8, %rax
#else
	addq	$2, %rax
#endif
	movq	%rax, KKK
#endif
	sarq	$3, %rax
	je	.L65
	ALIGN_4

.L62:
	mulps	%xmm8, %xmm9
#if defined(OPTERON) || defined(BARCELONA) || defined(SHANGHAI)
	PREFETCH	(PREFETCHSIZE +  0) * SIZE(AO)
#endif
	mulps	 4 * SIZE(BO), %xmm8
	addps	%xmm9, %xmm0
	movaps	 0 * SIZE(BO), %xmm9
	addps	%xmm8, %xmm1
	movaps	-28 * SIZE(AO), %xmm8
	mulps	%xmm8, %xmm9
	mulps	 4 * SIZE(BO), %xmm8
	addps	%xmm9, %xmm4
	movaps	 8 * SIZE(BO), %xmm9
	addps	%xmm8, %xmm5
	movaps	-24 * SIZE(AO), %xmm8

	mulps	%xmm8, %xmm9
	mulps	12 * SIZE(BO), %xmm8
	addps	%xmm9, %xmm0
	movaps	 8 * SIZE(BO), %xmm9
	addps	%xmm8, %xmm1
	movaps	-20 * SIZE(AO), %xmm8
	mulps	%xmm8, %xmm9
	mulps	12 * SIZE(BO), %xmm8
	addps	%xmm9, %xmm4
	movaps	64 * SIZE(BO), %xmm9
	addps	%xmm8, %xmm5
	movaps	32 * SIZE(AO), %xmm8

#if defined(OPTERON) || defined(BARCELONA) || defined(SHANGHAI)
	PREFETCH	(PREFETCHSIZE + 16) * SIZE(AO)
#endif
	mulps	%xmm10, %xmm11
	mulps	20 * SIZE(BO), %xmm10
	addps	%xmm11, %xmm0
	movaps	16 * SIZE(BO), %xmm11
	addps	%xmm10, %xmm1
	movaps	-12 * SIZE(AO), %xmm10
	mulps	%xmm10, %xmm11
	mulps	20 * SIZE(BO), %xmm10
	addps	%xmm11, %xmm4
	movaps	24 * SIZE(BO), %xmm11
	addps	%xmm10, %xmm5
	movaps	 -8 * SIZE(AO), %xmm10

	mulps	%xmm10, %xmm11
	mulps	28 * SIZE(BO), %xmm10
	addps	%xmm11, %xmm0
	movaps	24 * SIZE(BO), %xmm11
	addps	%xmm10, %xmm1
	movaps	-4 * SIZE(AO), %xmm10
	mulps	%xmm10, %xmm11
	mulps	28 * SIZE(BO), %xmm10
	addps	%xmm11, %xmm4
	movaps	80 * SIZE(BO), %xmm11
	addps	%xmm10, %xmm5
	movaps	48 * SIZE(AO), %xmm10

#if defined(OPTERON) || defined(BARCELONA) || defined(SHANGHAI)
	PREFETCH	(PREFETCHSIZE + 32) * SIZE(AO)
#endif
	mulps	%xmm12, %xmm13
	mulps	36 * SIZE(BO), %xmm12
	addps	%xmm13, %xmm0
	movaps	32 * SIZE(BO), %xmm13
	addps	%xmm12, %xmm1
	movaps	 4 * SIZE(AO), %xmm12
	mulps	%xmm12, %xmm13
	mulps	36 * SIZE(BO), %xmm12
	addps	%xmm13, %xmm4
	movaps	40 * SIZE(BO), %xmm13
	addps	%xmm12, %xmm5
	movaps	 8 * SIZE(AO), %xmm12

	mulps	%xmm12, %xmm13
	mulps	44 * SIZE(BO), %xmm12
	addps	%xmm13, %xmm0
	movaps	40 * SIZE(BO), %xmm13
	addps	%xmm12, %xmm1
	movaps	12 * SIZE(AO), %xmm12
	mulps	%xmm12, %xmm13
	mulps	44 * SIZE(BO), %xmm12
	addps	%xmm13, %xmm4
	movaps	96 * SIZE(BO), %xmm13
	addps	%xmm12, %xmm5
	movaps	64 * SIZE(AO), %xmm12

#if defined(OPTERON) || defined(BARCELONA) || defined(SHANGHAI)
	PREFETCH	(PREFETCHSIZE + 48) * SIZE(AO)
#endif
	mulps	%xmm14, %xmm15
	mulps	52 * SIZE(BO), %xmm14
	addps	%xmm15, %xmm0
	movaps	48 * SIZE(BO), %xmm15
	addps	%xmm14, %xmm1
	movaps	20 * SIZE(AO), %xmm14
	mulps	%xmm14, %xmm15
	mulps	52 * SIZE(BO), %xmm14
	addps	%xmm15, %xmm4
	movaps	56 * SIZE(BO), %xmm15
	addps	%xmm14, %xmm5
	movaps	24 * SIZE(AO), %xmm14

	mulps	%xmm14, %xmm15
	mulps	60 * SIZE(BO), %xmm14
	addps	%xmm15, %xmm0
	movaps	56 * SIZE(BO), %xmm15
	addps	%xmm14, %xmm1
	movaps	28 * SIZE(AO), %xmm14
	mulps	%xmm14, %xmm15
	mulps	60 * SIZE(BO), %xmm14
	addps	%xmm15, %xmm4
	movaps	112 * SIZE(BO), %xmm15
	addps	%xmm14, %xmm5
	movaps	80 * SIZE(AO), %xmm14

	addq   $64 * SIZE, AO
	addq   $64 * SIZE, BO
	decq   %rax
	jne    .L62
	ALIGN_4

.L65:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	movaps	ALPHA, %xmm7
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
	movaps	-28 * SIZE(AO), %xmm8
	mulps	%xmm8, %xmm9
	mulps	 4 * SIZE(BO), %xmm8
	addps	%xmm9, %xmm4
	movaps	 8 * SIZE(BO), %xmm9
	addps	%xmm8, %xmm5
	movaps	-24 * SIZE(AO), %xmm8

	addq	$8 * SIZE, AO		# aoffset  += 4
	addq	$8 * SIZE, BO		# boffset1 += 8
	decq	%rax
	jg	.L66
	ALIGN_4

.L68:
	movsd	 0 * SIZE(CO1), %xmm8
	movhps	 2 * SIZE(CO1), %xmm8
	movsd	 4 * SIZE(CO1), %xmm9
	movhps	 6 * SIZE(CO1), %xmm9
	movsd	 8 * SIZE(CO1), %xmm10
	movhps	10 * SIZE(CO1), %xmm10
	movsd	12 * SIZE(CO1), %xmm11
	movhps	14 * SIZE(CO1), %xmm11

	pshufd	$0x50, %xmm0,  %xmm2
	pshufd	$0xfa, %xmm0,  %xmm0
	pshufd	$0x50, %xmm4,  %xmm3
	pshufd	$0xfa, %xmm4,  %xmm4

	mulps	%xmm7, %xmm2
	mulps	%xmm7, %xmm0
	mulps	%xmm7, %xmm3
	mulps	%xmm7, %xmm4

	addps	%xmm8,  %xmm2
	addps	%xmm9,  %xmm0
	addps	%xmm10, %xmm3
	addps	%xmm11, %xmm4

	movlps	%xmm2,   0 * SIZE(CO1)
	movhps	%xmm2,   2 * SIZE(CO1)
	movlps	%xmm0,   4 * SIZE(CO1)
	movhps	%xmm0,   6 * SIZE(CO1)
	movlps	%xmm3,   8 * SIZE(CO1)
	movhps	%xmm3,  10 * SIZE(CO1)
	movlps	%xmm4,  12 * SIZE(CO1)
	movhps	%xmm4,  14 * SIZE(CO1)

	movsd	 0 * SIZE(CO2), %xmm8
	movhps	 2 * SIZE(CO2), %xmm8
	movsd	 4 * SIZE(CO2), %xmm9
	movhps	 6 * SIZE(CO2), %xmm9
	movsd	 8 * SIZE(CO2), %xmm10
	movhps	10 * SIZE(CO2), %xmm10
	movsd	12 * SIZE(CO2), %xmm11
	movhps	14 * SIZE(CO2), %xmm11

	pshufd	$0x50, %xmm1,  %xmm2
	pshufd	$0xfa, %xmm1,  %xmm1
	pshufd	$0x50, %xmm5,  %xmm3
	pshufd	$0xfa, %xmm5,  %xmm5

	mulps	%xmm7, %xmm2
	mulps	%xmm7, %xmm1
	mulps	%xmm7, %xmm3
	mulps	%xmm7, %xmm5

	addps	%xmm8,  %xmm2
	addps	%xmm9,  %xmm1
	addps	%xmm10, %xmm3
	addps	%xmm11, %xmm5

	movlps	%xmm2,   0 * SIZE(CO2)
	movhps	%xmm2,   2 * SIZE(CO2)
	movlps	%xmm1,   4 * SIZE(CO2)
	movhps	%xmm1,   6 * SIZE(CO2)
	movlps	%xmm3,   8 * SIZE(CO2)
	movhps	%xmm3,  10 * SIZE(CO2)
	movlps	%xmm5,  12 * SIZE(CO2)
	movhps	%xmm5,  14 * SIZE(CO2)

	addq	$16 * SIZE, CO1		# coffset += 4
	addq	$16 * SIZE, CO2		# coffset += 4
	decq	I			# i --
	jg	.L61
	ALIGN_4

.L70:
	testq	$4, M
	je	.L80


#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	BUFFER, BO
#else
	leaq	BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax,   8), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 4), BO
#endif

	movaps	-32 * SIZE(AO), %xmm8
	movaps	-16 * SIZE(AO), %xmm10

	movaps	 0 * SIZE(BO), %xmm9
	movaps	16 * SIZE(BO), %xmm11
	movaps	32 * SIZE(BO), %xmm13
	movaps	48 * SIZE(BO), %xmm15

	pxor	%xmm0, %xmm0
	pxor	%xmm1, %xmm1
	pxor	%xmm2, %xmm2
	pxor	%xmm3, %xmm3

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
	sarq	$3, %rax
	je	.L75
	ALIGN_4

.L72:
	mulps	%xmm8, %xmm9
#if defined(OPTERON) || defined(BARCELONA) || defined(SHANGHAI)
	PREFETCH	(PREFETCHSIZE +  0) * SIZE(AO)
#endif

	mulps	 4 * SIZE(BO), %xmm8
	addps	%xmm9, %xmm0
	movaps	 8 * SIZE(BO), %xmm9
	addps	%xmm8, %xmm1
	movaps	-28 * SIZE(AO), %xmm8

	mulps	%xmm8, %xmm9
	mulps	12 * SIZE(BO), %xmm8
	addps	%xmm9, %xmm2
	movaps	64 * SIZE(BO), %xmm9
	addps	%xmm8, %xmm3
	movaps	-24 * SIZE(AO), %xmm8

	mulps	%xmm8, %xmm11
	mulps	20 * SIZE(BO), %xmm8
	addps	%xmm11, %xmm0
	movaps	24 * SIZE(BO), %xmm11
	addps	%xmm8, %xmm1
	movaps	-20 * SIZE(AO), %xmm8

	mulps	%xmm8, %xmm11
	mulps	28 * SIZE(BO), %xmm8
	addps	%xmm11, %xmm2
	movaps	80 * SIZE(BO), %xmm11
	addps	%xmm8, %xmm3
	movaps	 0 * SIZE(AO), %xmm8

	mulps	%xmm10, %xmm13
	mulps	36 * SIZE(BO), %xmm10
	addps	%xmm13, %xmm0
	movaps	40 * SIZE(BO), %xmm13
	addps	%xmm10, %xmm1
	movaps	-12 * SIZE(AO), %xmm10

	mulps	%xmm10, %xmm13
	mulps	44 * SIZE(BO), %xmm10
	addps	%xmm13, %xmm2
	movaps	96 * SIZE(BO), %xmm13
	addps	%xmm10, %xmm3
	movaps	 -8 * SIZE(AO), %xmm10

	mulps	%xmm10, %xmm15
	mulps	52 * SIZE(BO), %xmm10
	addps	%xmm15, %xmm0
	movaps	56 * SIZE(BO), %xmm15
	addps	%xmm10, %xmm1
	movaps	 -4 * SIZE(AO), %xmm10

	mulps	%xmm10, %xmm15
	mulps	60 * SIZE(BO), %xmm10
	addps	%xmm15, %xmm2
	movaps	112 * SIZE(BO), %xmm15
	addps	%xmm10, %xmm3
	movaps	16 * SIZE(AO), %xmm10

	addq   $32 * SIZE, AO
	addq   $64 * SIZE, BO
	decq   %rax
	jne    .L72
	ALIGN_4

.L75:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	movaps	ALPHA, %xmm7
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
	movaps	-28 * SIZE(AO), %xmm8

	addq	$4 * SIZE, AO		# aoffset  += 4
	addq	$8 * SIZE, BO		# boffset1 += 8
	decq	%rax
	jg	.L76
	ALIGN_4

.L78:
	addps	%xmm2, %xmm0
	addps	%xmm3, %xmm1

	movsd	 0 * SIZE(CO1), %xmm8
	movhps	 2 * SIZE(CO1), %xmm8
	movsd	 4 * SIZE(CO1), %xmm9
	movhps	 6 * SIZE(CO1), %xmm9

	pshufd	$0x50, %xmm0,  %xmm2
	pshufd	$0xfa, %xmm0,  %xmm0

	mulps	%xmm7, %xmm2
	mulps	%xmm7, %xmm0
	addps	%xmm8,  %xmm2
	addps	%xmm9,  %xmm0

	movlps	%xmm2,   0 * SIZE(CO1)
	movhps	%xmm2,   2 * SIZE(CO1)
	movlps	%xmm0,   4 * SIZE(CO1)
	movhps	%xmm0,   6 * SIZE(CO1)

	movsd	 0 * SIZE(CO2), %xmm8
	movhps	 2 * SIZE(CO2), %xmm8
	movsd	 4 * SIZE(CO2), %xmm9
	movhps	 6 * SIZE(CO2), %xmm9

	pshufd	$0x50, %xmm1,  %xmm2
	pshufd	$0xfa, %xmm1,  %xmm1

	mulps	%xmm7, %xmm2
	mulps	%xmm7, %xmm1
	addps	%xmm8,  %xmm2
	addps	%xmm9,  %xmm1

	movlps	%xmm2,   0 * SIZE(CO2)
	movhps	%xmm2,   2 * SIZE(CO2)
	movlps	%xmm1,   4 * SIZE(CO2)
	movhps	%xmm1,   6 * SIZE(CO2)

	addq	$8 * SIZE, CO1		# coffset += 4
	addq	$8 * SIZE, CO2		# coffset += 4
	ALIGN_4

.L80:
	testq	$2, M
	je	.L90

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	BUFFER, BO
#else
	leaq	BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax,   8), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 4), BO
#endif

	movaps	-32 * SIZE(AO), %xmm8
	movaps	-24 * SIZE(AO), %xmm10

	movaps	 0 * SIZE(BO), %xmm9
	movaps	16 * SIZE(BO), %xmm11
	movaps	32 * SIZE(BO), %xmm13
	movaps	48 * SIZE(BO), %xmm15

	pxor	%xmm0, %xmm0
	pxor	%xmm1, %xmm1
	pxor	%xmm2, %xmm2
	pxor	%xmm3, %xmm3

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
	je	.L85
	ALIGN_4

.L82:
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm0
#if defined(OPTERON) || defined(BARCELONA) || defined(SHANGHAI)
	PREFETCH	(PREFETCHSIZE +  0) * SIZE(AO)
#endif
	movaps	 4 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	movsd	-30 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm1
	movaps	 8 * SIZE(BO), %xmm9

	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm2
	movaps	12 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	movsd	-28 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm3
	movaps	64 * SIZE(BO), %xmm9

	mulps	%xmm8, %xmm11
	addps	%xmm11, %xmm0
	movaps	20 * SIZE(BO), %xmm11
	mulps	%xmm8, %xmm11
	movsd	-26 * SIZE(AO), %xmm8
	addps	%xmm11, %xmm1
	movaps	24 * SIZE(BO), %xmm11

	mulps	%xmm8, %xmm11
	addps	%xmm11, %xmm2
	movaps	28 * SIZE(BO), %xmm11
	mulps	%xmm8, %xmm11
	movsd	-16 * SIZE(AO), %xmm8
	addps	%xmm11, %xmm3
	movaps	 80 * SIZE(BO), %xmm11

	mulps	%xmm10, %xmm13
	addps	%xmm13, %xmm0
	movaps	36 * SIZE(BO), %xmm13
	mulps	%xmm10, %xmm13
	movsd	-22 * SIZE(AO), %xmm10
	addps	%xmm13, %xmm1
	movaps	40 * SIZE(BO), %xmm13

	mulps	%xmm10, %xmm13
	addps	%xmm13, %xmm2
	movaps	44 * SIZE(BO), %xmm13
	mulps	%xmm10, %xmm13
	movsd	-20 * SIZE(AO), %xmm10
	addps	%xmm13, %xmm3
	movaps	 96 * SIZE(BO), %xmm13

	mulps	%xmm10, %xmm15
	addps	%xmm15, %xmm0
	movaps	52 * SIZE(BO), %xmm15
	mulps	%xmm10, %xmm15
	movsd	-18 * SIZE(AO), %xmm10
	addps	%xmm15, %xmm1
	movaps	56 * SIZE(BO), %xmm15

	mulps	%xmm10, %xmm15
	addps	%xmm15, %xmm2
	movaps	60 * SIZE(BO), %xmm15
	mulps	%xmm10, %xmm15
	movsd	-8 * SIZE(AO), %xmm10
	addps	%xmm15, %xmm3
	movaps	112 * SIZE(BO), %xmm15

	addq   $16 * SIZE, AO
	addq   $64 * SIZE, BO
	decq   %rax
	jne    .L82
	ALIGN_4

.L85:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	movaps	ALPHA, %xmm7
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L88
	ALIGN_4

.L86:
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm0
	movaps	  4 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	movsd	-30 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm1
	movaps	  8 * SIZE(BO), %xmm9

	addq	$2 * SIZE, AO		# aoffset  += 4
	addq	$8 * SIZE, BO		# boffset1 += 8
	decq	%rax
	jg	.L86
	ALIGN_4

.L88:
	addps	%xmm2, %xmm0
	addps	%xmm3, %xmm1

	movsd	 0 * SIZE(CO1), %xmm8
	movhps	 2 * SIZE(CO1), %xmm8

	pshufd	$0x50, %xmm0,  %xmm2
	mulps	%xmm7, %xmm2
	addps	%xmm8,  %xmm2

	movlps	%xmm2,   0 * SIZE(CO1)
	movhps	%xmm2,   2 * SIZE(CO1)

	movsd	 0 * SIZE(CO2), %xmm8
	movhps	 2 * SIZE(CO2), %xmm8

	pshufd	$0x50, %xmm1,  %xmm2
	mulps	%xmm7, %xmm2
	addps	%xmm8,  %xmm2

	movlps	%xmm2,   0 * SIZE(CO2)
	movhps	%xmm2,   2 * SIZE(CO2)

	addq	$4 * SIZE, CO1		# coffset += 4
	addq	$4 * SIZE, CO2		# coffset += 4
	ALIGN_4

.L90:
	testq	$1, M
	je	.L99

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	BUFFER, BO
#else
	leaq	BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax,   4), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 8), BO
#endif

	movss	-32 * SIZE(AO), %xmm8
	movss	-28 * SIZE(AO), %xmm10

	movss	 0 * SIZE(BO), %xmm9
	movss	16 * SIZE(BO), %xmm11
	movss	32 * SIZE(BO), %xmm13
	movss	48 * SIZE(BO), %xmm15

	pxor	%xmm0, %xmm0
	pxor	%xmm1, %xmm1
	pxor	%xmm2, %xmm2
	pxor	%xmm3, %xmm3

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
	je	.L95
	ALIGN_4

.L92:
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm0
#if defined(OPTERON) || defined(BARCELONA) || defined(SHANGHAI)
	PREFETCH	(PREFETCHSIZE +  0) * SIZE(AO)
#endif
	movss	 4 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	movss	-31 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm1
	movss	 8 * SIZE(BO), %xmm9

	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm2
	movss	12 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	movss	-30 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm3
	movss	64 * SIZE(BO), %xmm9

	mulps	%xmm8, %xmm11
	addps	%xmm11, %xmm0
	movss	20 * SIZE(BO), %xmm11
	mulps	%xmm8, %xmm11
	movss	-29 * SIZE(AO), %xmm8
	addps	%xmm11, %xmm1
	movss	24 * SIZE(BO), %xmm11

	mulps	%xmm8, %xmm11
	addps	%xmm11, %xmm2
	movss	28 * SIZE(BO), %xmm11
	mulps	%xmm8, %xmm11
	movss	-24 * SIZE(AO), %xmm8
	addps	%xmm11, %xmm3
	movss	 80 * SIZE(BO), %xmm11

	mulps	%xmm10, %xmm13
	addps	%xmm13, %xmm0
	movss	36 * SIZE(BO), %xmm13
	mulps	%xmm10, %xmm13
	movss	-27 * SIZE(AO), %xmm10
	addps	%xmm13, %xmm1
	movss	40 * SIZE(BO), %xmm13

	mulps	%xmm10, %xmm13
	addps	%xmm13, %xmm2
	movss	44 * SIZE(BO), %xmm13
	mulps	%xmm10, %xmm13
	movss	-26 * SIZE(AO), %xmm10
	addps	%xmm13, %xmm3
	movss	 96 * SIZE(BO), %xmm13

	mulps	%xmm10, %xmm15
	addps	%xmm15, %xmm0
	movss	52 * SIZE(BO), %xmm15
	mulps	%xmm10, %xmm15
	movss	-25 * SIZE(AO), %xmm10
	addps	%xmm15, %xmm1
	movss	56 * SIZE(BO), %xmm15

	mulps	%xmm10, %xmm15
	addps	%xmm15, %xmm2
	movss	60 * SIZE(BO), %xmm15
	mulps	%xmm10, %xmm15
	movss	-20 * SIZE(AO), %xmm10
	addps	%xmm15, %xmm3
	movss	112 * SIZE(BO), %xmm15

	addq   $ 8 * SIZE, AO
	addq   $64 * SIZE, BO
	decq   %rax
	jne    .L92
	ALIGN_4

.L95:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	movaps	ALPHA, %xmm7
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L98
	ALIGN_4

.L96:
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm0
	movss	 4 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	movss	-31 * SIZE(AO), %xmm8
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

	movsd	 0 * SIZE(CO1), %xmm8

	pshufd	$0x50, %xmm0,  %xmm2
	mulps	%xmm7, %xmm2
	addps	%xmm8,  %xmm2

	movlps	%xmm2,   0 * SIZE(CO1)

	movsd	 0 * SIZE(CO2), %xmm8

	pshufd	$0x50, %xmm1,  %xmm2
	mulps	%xmm7, %xmm2
	addps	%xmm8,  %xmm2

	movlps	%xmm2,   0 * SIZE(CO2)
	ALIGN_4

.L99:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addl	$2, KK
#endif
	leaq	(C, LDC, 2), C		# c += 4 * ldc
	ALIGN_4


.L100:
	testq	$1, N
	je	.L999

.L101:
#if defined(TRMMKERNEL) && defined(LEFT)
	movq	OFFSET, %rax
	movq	%rax, KK
#endif

/* Copying to Sub Buffer */
	leaq	BUFFER, BO

	movq	K, %rax
	sarq	$3, %rax
	jle	.L103
	ALIGN_4


.L102:
#if defined(PENTIUM4) || defined(GENERIC)
	movss	 0 * SIZE(B), %xmm0
	movss	 1 * SIZE(B), %xmm1
	movss	 2 * SIZE(B), %xmm2
	movss	 3 * SIZE(B), %xmm3
	movss	 4 * SIZE(B), %xmm4
	movss	 5 * SIZE(B), %xmm5
	movss	 6 * SIZE(B), %xmm6
	movss	 7 * SIZE(B), %xmm7

	PREFETCHNTA	 32 * SIZE(B)

	shufps	 $0, %xmm0, %xmm0
	shufps	 $0, %xmm1, %xmm1
	shufps	 $0, %xmm2, %xmm2
	shufps	 $0, %xmm3, %xmm3
	shufps	 $0, %xmm4, %xmm4
	shufps	 $0, %xmm5, %xmm5
	shufps	 $0, %xmm6, %xmm6
	shufps	 $0, %xmm7, %xmm7

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
#endif

#if defined(OPTERON) || defined(BARCELONA) || defined(SHANGHAI)
	PREFETCHNTA	 32 * SIZE(B)

	movd	 0 * SIZE(B), %mm0
	movd	 1 * SIZE(B), %mm1
	movd	 2 * SIZE(B), %mm2
	movd	 3 * SIZE(B), %mm3
	movd	 4 * SIZE(B), %mm4
	movd	 5 * SIZE(B), %mm5
	movd	 6 * SIZE(B), %mm6
	movd	 7 * SIZE(B), %mm7

	punpckldq %mm0, %mm0
	punpckldq %mm1, %mm1
	punpckldq %mm2, %mm2
	punpckldq %mm3, %mm3
	punpckldq %mm4, %mm4
	punpckldq %mm5, %mm5
	punpckldq %mm6, %mm6
	punpckldq %mm7, %mm7

	movq	%mm0,  0 * SIZE(BO)
	movq	%mm0,  2 * SIZE(BO)
	movq	%mm1,  4 * SIZE(BO)
	movq	%mm1,  6 * SIZE(BO)
	movq	%mm2,  8 * SIZE(BO)
	movq	%mm2, 10 * SIZE(BO)
	movq	%mm3, 12 * SIZE(BO)
	movq	%mm3, 14 * SIZE(BO)
	movq	%mm4, 16 * SIZE(BO)
	movq	%mm4, 18 * SIZE(BO)
	movq	%mm5, 20 * SIZE(BO)
	movq	%mm5, 22 * SIZE(BO)
	movq	%mm6, 24 * SIZE(BO)
	movq	%mm6, 26 * SIZE(BO)
	movq	%mm7, 28 * SIZE(BO)
	movq	%mm7, 30 * SIZE(BO)

	addq	$ 8 * SIZE, B
	addq	$32 * SIZE, BO
#endif

	decq	%rax
	jne	.L102
	ALIGN_4

.L103:
	movq	K, %rax
	andq	$7, %rax
	BRANCH
	jle	.L110
	ALIGN_4

.L104:
#if defined(PENTIUM4) || defined(GENERIC)
	movss	 0 * SIZE(B), %xmm0
	shufps	 $0, %xmm0, %xmm0
	movaps	%xmm0,  0 * SIZE(BO)
#endif

#if defined(OPTERON) || defined(BARCELONA) || defined(SHANGHAI)
	movd	 0 * SIZE(B), %mm0
	punpckldq %mm0, %mm0
	movq	%mm0,  0 * SIZE(BO)
	movq	%mm0,  2 * SIZE(BO)
#endif

	addq	$ 1 * SIZE, B
	addq	$ 4 * SIZE, BO
	decq	%rax
	jne	.L104
	ALIGN_4

.L110:
	movq	C, CO1			# coffset1 = c
	movq	A, AO		# aoffset = a

	movq	M,  I
	sarq	$3, I	# i = (m >> 3)
	jle	.L120
	ALIGN_4

.L111:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	BUFFER, BO
#else
	leaq	BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax,   8), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 2), BO
#endif

	movaps	-32 * SIZE(AO), %xmm8
	movaps	-16 * SIZE(AO), %xmm10
	movaps	  0 * SIZE(AO), %xmm12
	movaps	 16 * SIZE(AO), %xmm14

	movaps	 0 * SIZE(BO), %xmm9
	movaps	16 * SIZE(BO), %xmm11
	movaps	32 * SIZE(BO), %xmm13
	movaps	48 * SIZE(BO), %xmm15

	pxor	%xmm0, %xmm0
	pxor	%xmm1, %xmm1

	PREFETCHW     15 * SIZE(CO1)
	pxor	%xmm4, %xmm4
	pxor	%xmm5, %xmm5

#ifndef TRMMKERNEL
	movq	K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movq	K, %rax
	subq	KK, %rax
	movq	%rax, KKK
#else
	movq	KK, %rax
#ifdef LEFT
	addq	$8, %rax
#else
	addq	$1, %rax
#endif
	movq	%rax, KKK
#endif
	sarq	$3, %rax
	je	.L115
	ALIGN_4

.L112:
	mulps	%xmm9, %xmm8
#if defined(OPTERON) || defined(BARCELONA) || defined(SHANGHAI)
	PREFETCH	(PREFETCHSIZE +  0) * SIZE(AO)
#endif

	mulps	-28 * SIZE(AO), %xmm9
	addps	%xmm8, %xmm0
	movaps	-24 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm4
	movaps	 4 * SIZE(BO), %xmm9

	mulps	%xmm9, %xmm8
	mulps	-20 * SIZE(AO), %xmm9
	addps	%xmm8, %xmm0
	movaps	 32 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm4
	movaps	 8 * SIZE(BO), %xmm9

#if defined(OPTERON) || defined(BARCELONA) || defined(SHANGHAI)
	PREFETCH	(PREFETCHSIZE + 16) * SIZE(AO)
#endif
	mulps	%xmm9, %xmm10
	mulps	-12 * SIZE(AO), %xmm9
	addps	%xmm10, %xmm0
	movaps	 -8 * SIZE(AO), %xmm10
	addps	%xmm9, %xmm4
	movaps	12 * SIZE(BO), %xmm9

	mulps	%xmm9, %xmm10
	mulps	 -4 * SIZE(AO), %xmm9
	addps	%xmm10, %xmm0
	movaps	 48 * SIZE(AO), %xmm10
	addps	%xmm9, %xmm4
	movaps	32 * SIZE(BO), %xmm9

#if defined(OPTERON) || defined(BARCELONA) || defined(SHANGHAI)
	PREFETCH	(PREFETCHSIZE + 32) * SIZE(AO)
#endif
	mulps	%xmm11, %xmm12
	mulps	 4 * SIZE(AO), %xmm11
	addps	%xmm12, %xmm0
	movaps	 8 * SIZE(AO), %xmm12
	addps	%xmm11, %xmm4
	movaps	20 * SIZE(BO), %xmm11

	mulps	%xmm11, %xmm12
	mulps	12 * SIZE(AO), %xmm11
	addps	%xmm12, %xmm0
	movaps	64 * SIZE(AO), %xmm12
	addps	%xmm11, %xmm4
	movaps	24 * SIZE(BO), %xmm11

#if defined(OPTERON) || defined(BARCELONA) || defined(SHANGHAI)
	PREFETCH	(PREFETCHSIZE + 48) * SIZE(AO)
#endif
	mulps	%xmm11, %xmm14
	mulps	20 * SIZE(AO), %xmm11
	addps	%xmm14, %xmm0
	movaps	24 * SIZE(AO), %xmm14
	addps	%xmm11, %xmm4
	movaps	28 * SIZE(BO), %xmm11

	mulps	%xmm11, %xmm14
	mulps	28 * SIZE(AO), %xmm11
	addps	%xmm14, %xmm0
	movaps	80 * SIZE(AO), %xmm14
	addps	%xmm11, %xmm4
	movaps	48 * SIZE(BO), %xmm11

	addq   $64 * SIZE, AO
	addq   $32 * SIZE, BO
	decq   %rax
	jne    .L112
	ALIGN_4

.L115:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	movaps	ALPHA, %xmm7
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L118
	ALIGN_4

.L116:
	mulps	%xmm9, %xmm8
	mulps	-28 * SIZE(AO), %xmm9
	addps	%xmm8, %xmm0
	movaps	-24 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm4
	movaps	 4 * SIZE(BO), %xmm9

	addq	$8 * SIZE, AO		# aoffset  += 4
	addq	$4 * SIZE, BO		# boffset1 += 8
	decq	%rax
	jg	.L116
	ALIGN_4

.L118:
	movsd	 0 * SIZE(CO1), %xmm8
	movhps	 2 * SIZE(CO1), %xmm8
	movsd	 4 * SIZE(CO1), %xmm9
	movhps	 6 * SIZE(CO1), %xmm9
	movsd	 8 * SIZE(CO1), %xmm10
	movhps	10 * SIZE(CO1), %xmm10
	movsd	12 * SIZE(CO1), %xmm11
	movhps	14 * SIZE(CO1), %xmm11

	pshufd	$0x50, %xmm0,  %xmm2
	pshufd	$0xfa, %xmm0,  %xmm0
	pshufd	$0x50, %xmm4,  %xmm3
	pshufd	$0xfa, %xmm4,  %xmm4

	mulps	%xmm7, %xmm2
	mulps	%xmm7, %xmm0
	mulps	%xmm7, %xmm3
	mulps	%xmm7, %xmm4

	addps	%xmm8,  %xmm2
	addps	%xmm9,  %xmm0
	addps	%xmm10, %xmm3
	addps	%xmm11, %xmm4

	movlps	%xmm2,   0 * SIZE(CO1)
	movhps	%xmm2,   2 * SIZE(CO1)
	movlps	%xmm0,   4 * SIZE(CO1)
	movhps	%xmm0,   6 * SIZE(CO1)
	movlps	%xmm3,   8 * SIZE(CO1)
	movhps	%xmm3,  10 * SIZE(CO1)
	movlps	%xmm4,  12 * SIZE(CO1)
	movhps	%xmm4,  14 * SIZE(CO1)

	addq	$16 * SIZE, CO1		# coffset += 4
	decq	I			# i --
	jg	.L111
	ALIGN_4

.L120:
	testq	$4, M
	je	.L130

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	BUFFER, BO
#else
	leaq	BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax,   8), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 2), BO
#endif

	movaps	-32 * SIZE(AO), %xmm8
	movaps	-16 * SIZE(AO), %xmm10

	movaps	 0 * SIZE(BO), %xmm9
	movaps	16 * SIZE(BO), %xmm11

	pxor	%xmm0, %xmm0
	pxor	%xmm1, %xmm1
	pxor	%xmm2, %xmm2
	pxor	%xmm3, %xmm3

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
	je	.L125
	ALIGN_4

.L122:
	mulps	%xmm8, %xmm9
#if defined(OPTERON) || defined(BARCELONA) || defined(SHANGHAI)
	PREFETCH	(PREFETCHSIZE +  0) * SIZE(AO)
#endif
	movaps	-28 * SIZE(AO), %xmm8
	mulps	 4 * SIZE(BO), %xmm8
	addps	%xmm9, %xmm0
	movaps	32 * SIZE(BO), %xmm9
	addps	%xmm8, %xmm1
	movaps	-24 * SIZE(AO), %xmm8
	mulps	 8 * SIZE(BO), %xmm8
	addps	%xmm8, %xmm2
	movaps	-20 * SIZE(AO), %xmm8
	mulps	12 * SIZE(BO), %xmm8
	addps	%xmm8, %xmm3
	movaps	  0 * SIZE(AO), %xmm8

#if defined(OPTERON) || defined(BARCELONA) || defined(SHANGHAI)
	PREFETCH	(PREFETCHSIZE + 16) * SIZE(AO)
#endif
	mulps	%xmm10, %xmm11
	movaps	-12 * SIZE(AO), %xmm10
	mulps	20 * SIZE(BO), %xmm10
	addps	%xmm11, %xmm0
	movaps	48 * SIZE(BO), %xmm11
	addps	%xmm10, %xmm1
	movaps	 -8 * SIZE(AO), %xmm10
	mulps	24 * SIZE(BO), %xmm10
	addps	%xmm10, %xmm2
	movaps	-4 * SIZE(AO), %xmm10
	mulps	28 * SIZE(BO), %xmm10
	addps	%xmm10, %xmm3
	movaps	16 * SIZE(AO), %xmm10

	addq   $32 * SIZE, AO
	addq   $32 * SIZE, BO
	decq   %rax
	jne    .L122
	ALIGN_4

.L125:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	movaps	ALPHA, %xmm7
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L128
	ALIGN_4

.L126:
	mulps	%xmm8, %xmm9
	movaps	-28 * SIZE(AO), %xmm8
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

	movsd	 0 * SIZE(CO1), %xmm8
	movhps	 2 * SIZE(CO1), %xmm8
	movsd	 4 * SIZE(CO1), %xmm9
	movhps	 6 * SIZE(CO1), %xmm9

	pshufd	$0x50, %xmm0,  %xmm2
	pshufd	$0xfa, %xmm0,  %xmm0

	mulps	%xmm7, %xmm2
	mulps	%xmm7, %xmm0

	addps	%xmm8,  %xmm2
	addps	%xmm9,  %xmm0

	movlps	%xmm2,   0 * SIZE(CO1)
	movhps	%xmm2,   2 * SIZE(CO1)
	movlps	%xmm0,   4 * SIZE(CO1)
	movhps	%xmm0,   6 * SIZE(CO1)

	addq	$8 * SIZE, CO1		# coffset += 4
	ALIGN_4

.L130:
	testq	$2, M
	je	.L140

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	BUFFER, BO
#else
	leaq	BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax,   8), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 2), BO
#endif

	movaps	-32 * SIZE(AO), %xmm8
	movaps	-24 * SIZE(AO), %xmm10

	movaps	 0 * SIZE(BO), %xmm9
	movaps	16 * SIZE(BO), %xmm11

	pxor	%xmm0, %xmm0
	pxor	%xmm1, %xmm1
	pxor	%xmm2, %xmm2
	pxor	%xmm3, %xmm3

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
	je	.L135
	ALIGN_4

.L132:
	mulps	%xmm8, %xmm9
#if defined(OPTERON) || defined(BARCELONA) || defined(SHANGHAI)
	PREFETCH	(PREFETCHSIZE +  0) * SIZE(AO)
#endif
	movsd	-30 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm0
	movaps	 4 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	movsd	-28 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm1
	movaps	 8 * SIZE(BO), %xmm9

	mulps	%xmm8, %xmm9
	movsd	-26 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm0
	movaps	12 * SIZE(BO), %xmm9

	mulps	%xmm8, %xmm9
	movsd	-16 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm1
	movaps	32 * SIZE(BO), %xmm9

	mulps	%xmm10, %xmm11
	movsd	-22 * SIZE(AO), %xmm10
	addps	%xmm11, %xmm0
	movaps	20 * SIZE(BO), %xmm11

	mulps	%xmm10, %xmm11
	movsd	-20 * SIZE(AO), %xmm10
	addps	%xmm11, %xmm1
	movaps	24 * SIZE(BO), %xmm11

	mulps	%xmm10, %xmm11
	movsd	-18 * SIZE(AO), %xmm10
	addps	%xmm11, %xmm0
	movaps	28 * SIZE(BO), %xmm11

	mulps	%xmm10, %xmm11
	movsd	 -8 * SIZE(AO), %xmm10
	addps	%xmm11, %xmm1
	movaps	48 * SIZE(BO), %xmm11

	addq   $16 * SIZE, AO
	addq   $32 * SIZE, BO
	decq   %rax
	jne    .L132
	ALIGN_4

.L135:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	movaps	ALPHA, %xmm7
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L138
	ALIGN_4

.L136:
	mulps	%xmm8, %xmm9
	movsd	-30 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm0
	movaps	 4 * SIZE(BO), %xmm9

	addq	$2 * SIZE, AO		# aoffset  += 4
	addq	$4 * SIZE, BO		# boffset1 += 8
	decq	%rax
	jg	.L136
	ALIGN_4

.L138:
	addps	%xmm1,  %xmm0

	movsd	 0 * SIZE(CO1), %xmm8
	movhps	 2 * SIZE(CO1), %xmm8

	pshufd	$0x50, %xmm0,  %xmm2
	mulps	%xmm7, %xmm2
	addps	%xmm8,  %xmm2

	movlps	%xmm2,   0 * SIZE(CO1)
	movhps	%xmm2,   2 * SIZE(CO1)

	addq	$4 * SIZE, CO1		# coffset += 4
	ALIGN_4

.L140:
	testq	$1, M
	je	.L999

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	BUFFER, BO
#else
	leaq	BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax,   4), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 4), BO
#endif

	movss	-32 * SIZE(AO), %xmm8
	movss	-28 * SIZE(AO), %xmm10

	movss	 0 * SIZE(BO), %xmm9
	movss	16 * SIZE(BO), %xmm11

	pxor	%xmm0, %xmm0
	pxor	%xmm1, %xmm1
	pxor	%xmm2, %xmm2
	pxor	%xmm3, %xmm3

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
	je	.L145
	ALIGN_4

.L142:
	mulss	%xmm8, %xmm9
#if defined(OPTERON) || defined(BARCELONA) || defined(SHANGHAI)
	PREFETCH	(PREFETCHSIZE +  0) * SIZE(AO)
#endif
	movss	-31 * SIZE(AO), %xmm8
	mulss	 4 * SIZE(BO), %xmm8
	addss	%xmm9, %xmm0
	movss	32 * SIZE(BO), %xmm9
	addss	%xmm8, %xmm1
	movss	-30 * SIZE(AO), %xmm8
	mulss	 8 * SIZE(BO), %xmm8
	addss	%xmm8, %xmm2
	movss	-29 * SIZE(AO), %xmm8
	mulss	12 * SIZE(BO), %xmm8
	addss	%xmm8, %xmm3
	movss	-24 * SIZE(AO), %xmm8
	mulss	%xmm10, %xmm11
	movss	-27 * SIZE(AO), %xmm10
	mulss	20 * SIZE(BO), %xmm10
	addss	%xmm11, %xmm0
	movss	48 * SIZE(BO), %xmm11
	addss	%xmm10, %xmm1
	movss	-26 * SIZE(AO), %xmm10
	mulss	24 * SIZE(BO), %xmm10
	addss	%xmm10, %xmm2
	movss	-25 * SIZE(AO), %xmm10
	mulss	28 * SIZE(BO), %xmm10
	addss	%xmm10, %xmm3
	movss	-20 * SIZE(AO), %xmm10

	addq   $ 8 * SIZE, AO
	addq   $32 * SIZE, BO
	decq   %rax
	jne    .L142
	ALIGN_4

.L145:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	movaps	ALPHA, %xmm7
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L148
	ALIGN_4

.L146:
	mulss	%xmm8, %xmm9
	movss	-31 * SIZE(AO), %xmm8
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

	movsd	 0 * SIZE(CO1), %xmm8

	pshufd	$0x50, %xmm0,  %xmm2
	mulps	%xmm7, %xmm2
	addps	%xmm8,  %xmm2

	movlps	%xmm2,   0 * SIZE(CO1)
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
