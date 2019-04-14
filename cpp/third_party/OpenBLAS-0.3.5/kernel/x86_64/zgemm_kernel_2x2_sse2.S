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
#define BUFFER	256(%rsp)

#ifdef OPTERON
#define PREFETCH     prefetch
#define PREFETCHW    prefetchw
#define PREFETCHSIZE (8 * 9 + 4)

#define RPREFETCHSIZE (8 *  7 + 4)
#define WPREFETCHSIZE (8 *  8 + 4)
#endif

#ifdef GENERIC
#define PREFETCH     prefetcht0
#define PREFETCHW    prefetcht0
#define PREFETCHSIZE (8 * 5 + 4)

#define RPREFETCHSIZE (8 *  7 + 4)
#define WPREFETCHSIZE (8 *  8 + 4)
#endif

#ifndef GENERIC
#define KERNEL1(xx) \
	mulpd	%xmm0, %xmm1 ;\
	addpd	%xmm1, %xmm8 ;\
	movapd	-16 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm1 ;\
	mulpd	%xmm0, %xmm3 ;\
	addpd	%xmm3, %xmm9 ;\
	movapd	-14 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm3 ;\
	mulpd	%xmm0, %xmm5 ;\
	PREFETCH	(PREFETCHSIZE     +  0) * SIZE + 1 * (xx) * SIZE(AO, %rax, 4) ;\
	mulpd	-10 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm0 ;\
	addpd	%xmm5, %xmm10 ;\
	movapd	-12 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm5 ;\
	addpd	%xmm0, %xmm11 ;\
	movapd	-8 * SIZE + 1 * (xx) * SIZE(AO, %rax, 4), %xmm0

#define KERNEL2(xx) \
	mulpd	%xmm2, %xmm1 ;\
	addpd	%xmm1, %xmm12 ;\
	movapd	 0 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm1 ;\
	mulpd	%xmm2, %xmm3 ;\
	addpd	%xmm3, %xmm13 ;\
	movapd	-6 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm3 ;\
	mulpd	%xmm2, %xmm5 ;\
	mulpd	-10 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm2 ;\
	addpd	%xmm5, %xmm14 ;\
	movapd	 -4 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm5 ;\
	addpd	%xmm2, %xmm15 ;\
	movapd	-6 * SIZE + 1 * (xx) * SIZE(AO, %rax, 4), %xmm2

#define KERNEL3(xx) \
	mulpd	%xmm4, %xmm7 ;\
	addpd	%xmm7, %xmm8 ;\
	movapd	-8 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm7 ;\
	mulpd	%xmm4, %xmm3 ;\
	addpd	%xmm3, %xmm9 ;\
	movapd	-6 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm3 ;\
	mulpd	%xmm4, %xmm5 ;\
	mulpd	-2 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm4 ;\
	addpd	%xmm5, %xmm10 ;\
	movapd	-4 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm5 ;\
	addpd	%xmm4, %xmm11 ;\
	movapd	-4 * SIZE + 1 * (xx) * SIZE(AO, %rax, 4), %xmm4

#define KERNEL4(xx) \
	mulpd	%xmm6, %xmm7 ;\
	addpd	%xmm7, %xmm12 ;\
	movapd	 8 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm7 ;\
	mulpd	%xmm6, %xmm3 ;\
	addpd	%xmm3, %xmm13 ;\
	movapd	 2 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm3 ;\
	mulpd	%xmm6, %xmm5 ;\
	mulpd	-2 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm6 ;\
	addpd	%xmm5, %xmm14 ;\
	movapd	 4 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm5 ;\
 	PREFETCH	(PREFETCHSIZE     +  8) * SIZE + 1 * (xx) * SIZE(AO, %rax, 4) ;\
	addpd	%xmm6, %xmm15 ;\
	movapd	-2 * SIZE + 1 * (xx) * SIZE(AO, %rax, 4), %xmm6

#define KERNEL5(xx) \
	mulpd	%xmm0, %xmm1 ;\
	addpd	%xmm1, %xmm8 ;\
	movapd	 0 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm1 ;\
	mulpd	%xmm0, %xmm3 ;\
	addpd	%xmm3, %xmm9 ;\
	movapd	 2 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm3 ;\
	mulpd	%xmm0, %xmm5 ;\
	mulpd	 6 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm0 ;\
	addpd	%xmm5, %xmm10 ;\
	movapd	 4 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm5 ;\
	addpd	%xmm0, %xmm11 ;\
	movapd	 0 * SIZE + 1 * (xx) * SIZE(AO, %rax, 4), %xmm0

#define KERNEL6(xx) \
	mulpd	%xmm2, %xmm1 ;\
	addpd	%xmm1, %xmm12 ;\
	movapd	16 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm1 ;\
	mulpd	%xmm2, %xmm3 ;\
	addpd	%xmm3, %xmm13 ;\
	movapd	10 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm3 ;\
	mulpd	%xmm2, %xmm5 ;\
	mulpd	 6 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm2 ;\
	addpd	%xmm5, %xmm14 ;\
	movapd	12 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm5 ;\
	addpd	%xmm2, %xmm15 ;\
	movapd	 2 * SIZE + 1 * (xx) * SIZE(AO, %rax, 4), %xmm2

#define KERNEL7(xx) \
	mulpd	%xmm4, %xmm7 ;\
	addpd	%xmm7, %xmm8 ;\
	movapd	 8 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm7 ;\
	mulpd	%xmm4, %xmm3 ;\
	addpd	%xmm3, %xmm9 ;\
	movapd	10 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm3 ;\
	mulpd	%xmm4, %xmm5 ;\
	mulpd	14 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm4 ;\
	addpd	%xmm5, %xmm10 ;\
	movapd	12 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm5 ;\
	addpd	%xmm4, %xmm11 ;\
	movapd	 4 * SIZE + 1 * (xx) * SIZE(AO, %rax, 4), %xmm4

#define KERNEL8(xx) \
	mulpd	%xmm6, %xmm7 ;\
	addpd	%xmm7, %xmm12 ;\
	movapd	24 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm7 ;\
	mulpd	%xmm6, %xmm3 ;\
	addpd	%xmm3, %xmm13 ;\
	movapd	18 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm3 ;\
	mulpd	%xmm6, %xmm5 ;\
	mulpd	14 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm6 ;\
	addpd	%xmm5, %xmm14 ;\
	movapd	20 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm5 ;\
	addpd	%xmm6, %xmm15 ;\
	movapd	 6 * SIZE + 1 * (xx) * SIZE(AO, %rax, 4), %xmm6

#else
#define KERNEL1(xx) \
	mulpd	%xmm0, %xmm1 ;\
	addpd	%xmm1, %xmm8 ;\
	movapd	-16 * SIZE + 2 * (xx) * SIZE(BO), %xmm1 ;\
	mulpd	%xmm0, %xmm3 ;\
	addpd	%xmm3, %xmm9 ;\
	movapd	-14 * SIZE + 2 * (xx) * SIZE(BO), %xmm3 ;\
	mulpd	%xmm0, %xmm5 ;\
	PREFETCH	(PREFETCHSIZE     +  0) * SIZE + 1 * (xx) * SIZE(AO) ;\
	mulpd	-10 * SIZE + 2 * (xx) * SIZE(BO), %xmm0 ;\
	addpd	%xmm5, %xmm10 ;\
	movapd	-12 * SIZE + 2 * (xx) * SIZE(BO), %xmm5 ;\
	addpd	%xmm0, %xmm11 ;\
	movapd	-8 * SIZE + 1 * (xx) * SIZE(AO), %xmm0

#define KERNEL2(xx) \
	mulpd	%xmm2, %xmm1 ;\
	addpd	%xmm1, %xmm12 ;\
	movapd	 0 * SIZE + 2 * (xx) * SIZE(BO), %xmm1 ;\
	mulpd	%xmm2, %xmm3 ;\
	addpd	%xmm3, %xmm13 ;\
	movapd	-6 * SIZE + 2 * (xx) * SIZE(BO), %xmm3 ;\
	mulpd	%xmm2, %xmm5 ;\
	mulpd	-10 * SIZE + 2 * (xx) * SIZE(BO), %xmm2 ;\
	addpd	%xmm5, %xmm14 ;\
	movapd	 -4 * SIZE + 2 * (xx) * SIZE(BO), %xmm5 ;\
	addpd	%xmm2, %xmm15 ;\
	movapd	-6 * SIZE + 1 * (xx) * SIZE(AO), %xmm2

#define KERNEL3(xx) \
	mulpd	%xmm4, %xmm7 ;\
	addpd	%xmm7, %xmm8 ;\
	movapd	-8 * SIZE + 2 * (xx) * SIZE(BO), %xmm7 ;\
	mulpd	%xmm4, %xmm3 ;\
	addpd	%xmm3, %xmm9 ;\
	movapd	-6 * SIZE + 2 * (xx) * SIZE(BO), %xmm3 ;\
	mulpd	%xmm4, %xmm5 ;\
	mulpd	-2 * SIZE + 2 * (xx) * SIZE(BO), %xmm4 ;\
	addpd	%xmm5, %xmm10 ;\
	movapd	-4 * SIZE + 2 * (xx) * SIZE(BO), %xmm5 ;\
	addpd	%xmm4, %xmm11 ;\
	movapd	-4 * SIZE + 1 * (xx) * SIZE(AO), %xmm4

#define KERNEL4(xx) \
	mulpd	%xmm6, %xmm7 ;\
	addpd	%xmm7, %xmm12 ;\
	movapd	 8 * SIZE + 2 * (xx) * SIZE(BO), %xmm7 ;\
	mulpd	%xmm6, %xmm3 ;\
	addpd	%xmm3, %xmm13 ;\
	movapd	 2 * SIZE + 2 * (xx) * SIZE(BO), %xmm3 ;\
	mulpd	%xmm6, %xmm5 ;\
	mulpd	-2 * SIZE + 2 * (xx) * SIZE(BO), %xmm6 ;\
	addpd	%xmm5, %xmm14 ;\
	movapd	 4 * SIZE + 2 * (xx) * SIZE(BO), %xmm5 ;\
 	PREFETCH	(PREFETCHSIZE     +  8) * SIZE + 1 * (xx) * SIZE(AO) ;\
	addpd	%xmm6, %xmm15 ;\
	movapd	-2 * SIZE + 1 * (xx) * SIZE(AO), %xmm6

#define KERNEL5(xx) \
	mulpd	%xmm0, %xmm1 ;\
	addpd	%xmm1, %xmm8 ;\
	movapd	 0 * SIZE + 2 * (xx) * SIZE(BO), %xmm1 ;\
	mulpd	%xmm0, %xmm3 ;\
	addpd	%xmm3, %xmm9 ;\
	movapd	 2 * SIZE + 2 * (xx) * SIZE(BO), %xmm3 ;\
	mulpd	%xmm0, %xmm5 ;\
	mulpd	 6 * SIZE + 2 * (xx) * SIZE(BO), %xmm0 ;\
	addpd	%xmm5, %xmm10 ;\
	movapd	 4 * SIZE + 2 * (xx) * SIZE(BO), %xmm5 ;\
	addpd	%xmm0, %xmm11 ;\
	movapd	 0 * SIZE + 1 * (xx) * SIZE(AO), %xmm0

#define KERNEL6(xx) \
	mulpd	%xmm2, %xmm1 ;\
	addpd	%xmm1, %xmm12 ;\
	movapd	16 * SIZE + 2 * (xx) * SIZE(BO), %xmm1 ;\
	mulpd	%xmm2, %xmm3 ;\
	addpd	%xmm3, %xmm13 ;\
	movapd	10 * SIZE + 2 * (xx) * SIZE(BO), %xmm3 ;\
	mulpd	%xmm2, %xmm5 ;\
	mulpd	 6 * SIZE + 2 * (xx) * SIZE(BO), %xmm2 ;\
	addpd	%xmm5, %xmm14 ;\
	movapd	12 * SIZE + 2 * (xx) * SIZE(BO), %xmm5 ;\
	addpd	%xmm2, %xmm15 ;\
	movapd	 2 * SIZE + 1 * (xx) * SIZE(AO), %xmm2

#define KERNEL7(xx) \
	mulpd	%xmm4, %xmm7 ;\
	addpd	%xmm7, %xmm8 ;\
	movapd	 8 * SIZE + 2 * (xx) * SIZE(BO), %xmm7 ;\
	mulpd	%xmm4, %xmm3 ;\
	addpd	%xmm3, %xmm9 ;\
	movapd	10 * SIZE + 2 * (xx) * SIZE(BO), %xmm3 ;\
	mulpd	%xmm4, %xmm5 ;\
	mulpd	14 * SIZE + 2 * (xx) * SIZE(BO), %xmm4 ;\
	addpd	%xmm5, %xmm10 ;\
	movapd	12 * SIZE + 2 * (xx) * SIZE(BO), %xmm5 ;\
	addpd	%xmm4, %xmm11 ;\
	movapd	 4 * SIZE + 1 * (xx) * SIZE(AO), %xmm4

#define KERNEL8(xx) \
	mulpd	%xmm6, %xmm7 ;\
	addpd	%xmm7, %xmm12 ;\
	movapd	24 * SIZE + 2 * (xx) * SIZE(BO), %xmm7 ;\
	mulpd	%xmm6, %xmm3 ;\
	addpd	%xmm3, %xmm13 ;\
	movapd	18 * SIZE + 2 * (xx) * SIZE(BO), %xmm3 ;\
	mulpd	%xmm6, %xmm5 ;\
	mulpd	14 * SIZE + 2 * (xx) * SIZE(BO), %xmm6 ;\
	addpd	%xmm5, %xmm14 ;\
	movapd	20 * SIZE + 2 * (xx) * SIZE(BO), %xmm5 ;\
	addpd	%xmm6, %xmm15 ;\
	movapd	 6 * SIZE + 1 * (xx) * SIZE(AO), %xmm6

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

	EMMS

	movq	%rsp, %rbx	# save old stack
	subq	$256 + LOCAL_BUFFER_SIZE, %rsp
	andq	$-4096, %rsp	# align stack

	STACK_TOUCHING

	movq	OLD_M, M
	movq	OLD_N, N

	pcmpeqb	%xmm7, %xmm7
	psllq	$63, %xmm7	# Generate mask
	pxor	%xmm10, %xmm10

	movlpd	 %xmm0, 0 + ALPHA_R
	movlpd	 %xmm0, 8 + ALPHA_R

	movlpd	 %xmm1, 8 + ALPHA_I
	xorpd	 %xmm7, %xmm1
	movlpd	 %xmm1, 0 + ALPHA_I

	movlpd	  %xmm10,  0 + POSINV
	movlpd	  %xmm7, 8 + POSINV

#ifdef TRMMKERNEL
	movlpd	%xmm12, OFFSET
	movlpd	%xmm12, KK
#ifndef LEFT
	negq	KK
#endif
#endif

	subq	$-16 * SIZE, A

	salq	$ZBASE_SHIFT, LDC

	movq	N,  J
	sarq	$1, J		# j = (n >> 2)
	jle	.L100
	ALIGN_4

.L01:
	movq	C, CO1			# coffset1 = c
	leaq	(C, LDC, 1), CO2	# coffset2 = c + ldc

#if defined(TRMMKERNEL) && defined(LEFT)
	movq	OFFSET, %rax
	movq	%rax, KK
#endif

	leaq	16 * SIZE + BUFFER, BO

	movq	K, %rax
	sarq	$2, %rax
	jle	.L03
	ALIGN_4

.L02:
	PREFETCH	 (RPREFETCHSIZE +  0)  * SIZE(B)

	movq	 0 * SIZE(B), %mm0
	movq	%mm0,  -16 * SIZE(BO)
	movq	%mm0,  -15 * SIZE(BO)
	movq	 1 * SIZE(B), %mm1
	movq	%mm1,  -14 * SIZE(BO)
	movq	%mm1,  -13 * SIZE(BO)

	movq	 2 * SIZE(B), %mm2
	movq	%mm2,  -12 * SIZE(BO)
	movq	%mm2,  -11 * SIZE(BO)
	movq	 3 * SIZE(B), %mm3
	movq	%mm3,  -10 * SIZE(BO)
	movq	%mm3,   -9 * SIZE(BO)

	PREFETCHW	 (WPREFETCHSIZE +  0)  * SIZE(BO)

	movq	 4 * SIZE(B), %mm4
	movq	%mm4,   -8 * SIZE(BO)
	movq	%mm4,   -7 * SIZE(BO)
	movq	 5 * SIZE(B), %mm5
	movq	%mm5,   -6 * SIZE(BO)
	movq	%mm5,   -5 * SIZE(BO)

	PREFETCHW	 (WPREFETCHSIZE +  8)  * SIZE(BO)

	movq	 6 * SIZE(B), %mm6
	movq	%mm6,   -4 * SIZE(BO)
	movq	%mm6,   -3 * SIZE(BO)
	movq	 7 * SIZE(B), %mm7
	movq	%mm7,   -2 * SIZE(BO)
	movq	%mm7,   -1 * SIZE(BO)

	PREFETCH	 (RPREFETCHSIZE +  8)  * SIZE(B)

	movq	 8 * SIZE(B), %mm0
	movq	%mm0,   0 * SIZE(BO)
	movq	%mm0,   1 * SIZE(BO)
	movq	 9 * SIZE(B), %mm1
	movq	%mm1,   2 * SIZE(BO)
	movq	%mm1,   3 * SIZE(BO)

	movq	10 * SIZE(B), %mm2
	movq	%mm2,   4 * SIZE(BO)
	movq	%mm2,   5 * SIZE(BO)
	movq	11 * SIZE(B), %mm3
	movq	%mm3,   6 * SIZE(BO)
	movq	%mm3,   7 * SIZE(BO)

	PREFETCHW	 (WPREFETCHSIZE + 16)  * SIZE(BO)

	movq	12 * SIZE(B), %mm4
	movq	%mm4,   8 * SIZE(BO)
	movq	%mm4,   9 * SIZE(BO)
	movq	13 * SIZE(B), %mm5
	movq	%mm5,  10 * SIZE(BO)
	movq	%mm5,  11 * SIZE(BO)

	PREFETCHW	 (WPREFETCHSIZE + 24)  * SIZE(BO)

	movq	14 * SIZE(B), %mm6
	movq	%mm6,  12 * SIZE(BO)
	movq	%mm6,  13 * SIZE(BO)
	movq	15 * SIZE(B), %mm7
	movq	%mm7,  14 * SIZE(BO)
	movq	%mm7,  15 * SIZE(BO)

	addq	$ 32 * SIZE, BO
	subq	$-16 * SIZE, B
	decq	%rax
	jne	.L02
	ALIGN_4

.L03:
	movq	K, %rax
	andq	$3, %rax
	BRANCH
	jle	.L05
	ALIGN_4

.L04:
	movq	 0 * SIZE(B), %mm0
	movq	%mm0, -16 * SIZE(BO)
	movq	%mm0, -15 * SIZE(BO)
	movq	 1 * SIZE(B), %mm1
	movq	%mm1, -14 * SIZE(BO)
	movq	%mm1, -13 * SIZE(BO)

	movq	 2 * SIZE(B), %mm2
	movq	%mm2, -12 * SIZE(BO)
	movq	%mm2, -11 * SIZE(BO)
	movq	 3 * SIZE(B), %mm3
	movq	%mm3, -10 * SIZE(BO)
	movq	%mm3,  -9 * SIZE(BO)

	addq	$ 4 * SIZE, B
	addq	$ 8 * SIZE, BO

	decq	%rax
	jne	.L04
	ALIGN_4

.L05:
	movq	A, AO		# aoffset = a

	leaq	 (RPREFETCHSIZE +  0)  * SIZE(B), BB

	movq	M,  I
	sarq	$1, I		# i = (m >> 2)
	jle	.L30
	ALIGN_4

.L10:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	16 * SIZE + BUFFER, BO
#else
	leaq	16 * SIZE + BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 8), BO
#endif

	movapd	-16 * SIZE(AO), %xmm0
	movapd	-16 * SIZE(BO), %xmm1
	pxor	%xmm8, %xmm8
	PREFETCH	  0  * SIZE(BB)
	movapd	-14 * SIZE(AO), %xmm2
	movapd	-14 * SIZE(BO), %xmm3
	pxor	%xmm9, %xmm9
	movapd	-12 * SIZE(AO), %xmm4
	movapd	-12 * SIZE(BO), %xmm5
	pxor	%xmm10, %xmm10
	movapd	-10 * SIZE(AO), %xmm6
	movapd	 -8 * SIZE(BO), %xmm7
	pxor	%xmm11, %xmm11

	pxor	%xmm12, %xmm12
	PREFETCHW      3 * SIZE(CO1)
	pxor	%xmm13, %xmm13
	PREFETCHW      3 * SIZE(CO2)
	pxor	%xmm14, %xmm14
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
	addq	$2, %rax
#else
	addq	$2, %rax
#endif
	movq	%rax, KKK
#endif
#ifndef GENERIC
	andq	$-8, %rax

	leaq	(, %rax, SIZE), %rax
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

	KERNEL1(16 *  1)
	KERNEL2(16 *  1)
	KERNEL3(16 *  1)
	KERNEL4(16 *  1)
	KERNEL5(16 *  1)
	KERNEL6(16 *  1)
	KERNEL7(16 *  1)
	KERNEL8(16 *  1)

	addq	$8 * SIZE, %rax
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

	KERNEL1(16 *  1)
	KERNEL2(16 *  1)
	KERNEL3(16 *  1)
	KERNEL4(16 *  1)
	KERNEL5(16 *  1)
	KERNEL6(16 *  1)
	KERNEL7(16 *  1)
	KERNEL8(16 *  1)

	addq	$8 * SIZE, %rax
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

	KERNEL1(16 *  1)
	KERNEL2(16 *  1)
	KERNEL3(16 *  1)
	KERNEL4(16 *  1)
	KERNEL5(16 *  1)
	KERNEL6(16 *  1)
	KERNEL7(16 *  1)
	KERNEL8(16 *  1)

	addq	$8 * SIZE, %rax
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

	KERNEL1(16 *  1)
	KERNEL2(16 *  1)
	KERNEL3(16 *  1)
	KERNEL4(16 *  1)
	KERNEL5(16 *  1)
	KERNEL6(16 *  1)
	KERNEL7(16 *  1)
	KERNEL8(16 *  1)

	addq	$8 * SIZE, %rax
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

	KERNEL1(16 *  1)
	KERNEL2(16 *  1)
	KERNEL3(16 *  1)
	KERNEL4(16 *  1)
	KERNEL5(16 *  1)
	KERNEL6(16 *  1)
	KERNEL7(16 *  1)
	KERNEL8(16 *  1)

	addq	$8 * SIZE, %rax
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

	KERNEL1(16 *  1)
	KERNEL2(16 *  1)
	KERNEL3(16 *  1)
	KERNEL4(16 *  1)
	KERNEL5(16 *  1)
	KERNEL6(16 *  1)
	KERNEL7(16 *  1)
	KERNEL8(16 *  1)

	addq	$8 * SIZE, %rax
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

	KERNEL1(16 *  1)
	KERNEL2(16 *  1)
	KERNEL3(16 *  1)
	KERNEL4(16 *  1)
	KERNEL5(16 *  1)
	KERNEL6(16 *  1)
	KERNEL7(16 *  1)
	KERNEL8(16 *  1)

	addq	$8 * SIZE, %rax
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

	KERNEL1(16 *  1)
	KERNEL2(16 *  1)
	KERNEL3(16 *  1)
	KERNEL4(16 *  1)
	KERNEL5(16 *  1)
	KERNEL6(16 *  1)
	KERNEL7(16 *  1)
	KERNEL8(16 *  1)

	addq	$8 * SIZE, %rax
	BRANCH
	jl	.L12
	ALIGN_3

.L15:
	PREFETCH	  8  * SIZE(BB)
	subq	 $-16 * SIZE, BB

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

	addq	$32 * SIZE, BO
	addq	$16 * SIZE, AO
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

	addq	$ 32 * SIZE, BO
	subq	$-16 * SIZE, AO
	decq	%rax
	BRANCH
	jg	.L12
#endif

.L16:
	movapd	POSINV,  %xmm5
	movapd	ALPHA_R, %xmm6
	movapd	ALPHA_I, %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	je .L19

	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 8), BO
	negq	%rax
	ALIGN_3

.L17:
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm8
	movapd	-14 * SIZE(BO, %rax, 8), %xmm1
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm9
	movapd	-12 * SIZE(BO, %rax, 8), %xmm1
	mulpd	%xmm0, %xmm1
	mulpd	-10 * SIZE(BO, %rax, 8), %xmm0
	addpd	%xmm1, %xmm10
	movapd	-16 * SIZE(BO, %rax, 8), %xmm1
	addpd	%xmm0, %xmm11
	movapd	-12 * SIZE(AO, %rax, 4), %xmm0
	mulpd	%xmm2, %xmm1
	addpd	%xmm1, %xmm12
	movapd	-14 * SIZE(BO, %rax, 8), %xmm1
	mulpd	%xmm2, %xmm1
	addpd	%xmm1, %xmm13
	movapd	-12 * SIZE(BO, %rax, 8), %xmm1
	mulpd	%xmm2, %xmm1
	mulpd	-10 * SIZE(BO, %rax, 8), %xmm2
	addpd	%xmm1, %xmm14
	movapd	 -8 * SIZE(BO, %rax, 8), %xmm1
	addpd	%xmm2, %xmm15
	movapd	-10 * SIZE(AO, %rax, 4), %xmm2

	addq	$SIZE, %rax
	jl	.L17
	ALIGN_3

.L19:
#ifndef TRMMKERNEL
	movlpd	0 * SIZE(CO1), %xmm0
	movhpd	1 * SIZE(CO1), %xmm0
	movlpd	2 * SIZE(CO1), %xmm2
	movhpd	3 * SIZE(CO1), %xmm2

	movlpd	0 * SIZE(CO2), %xmm1
	movhpd	1 * SIZE(CO2), %xmm1
	movlpd	2 * SIZE(CO2), %xmm3
	movhpd	3 * SIZE(CO2), %xmm3
#endif

	SHUFPD_1 %xmm9, %xmm9
	SHUFPD_1 %xmm11, %xmm11
	SHUFPD_1 %xmm13, %xmm13
	SHUFPD_1 %xmm15, %xmm15

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)
	xorpd	%xmm5, %xmm9
	xorpd	%xmm5, %xmm11
	xorpd	%xmm5, %xmm13
	xorpd	%xmm5, %xmm15
#else
	xorpd	%xmm5, %xmm8
	xorpd	%xmm5, %xmm10
	xorpd	%xmm5, %xmm12
	xorpd	%xmm5, %xmm14
#endif

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(RR) || defined(RC) || defined(CR) || defined(CC)
	subpd	%xmm9, %xmm8
	subpd	%xmm11, %xmm10
	subpd	%xmm13, %xmm12
	subpd	%xmm15, %xmm14
#else
	addpd	%xmm9, %xmm8
	addpd	%xmm11, %xmm10
	addpd	%xmm13, %xmm12
	addpd	%xmm15, %xmm14
#endif

	pshufd	$0x4e, %xmm8, %xmm9
	pshufd	$0x4e, %xmm10, %xmm11
	pshufd	$0x4e, %xmm12, %xmm13
	pshufd	$0x4e, %xmm14, %xmm15

	mulpd	%xmm6, %xmm8
	mulpd	%xmm7, %xmm9
	mulpd	%xmm6, %xmm10
	mulpd	%xmm7, %xmm11

	mulpd	%xmm6, %xmm12
	mulpd	%xmm7, %xmm13
	mulpd	%xmm6, %xmm14
	mulpd	%xmm7, %xmm15

	addpd	%xmm9, %xmm8
	addpd	%xmm11, %xmm10
	addpd	%xmm13, %xmm12
	addpd	%xmm15, %xmm14

#ifndef TRMMKERNEL
	addpd	%xmm0,  %xmm8
	addpd	%xmm2, %xmm12
	addpd	%xmm1,  %xmm10
	addpd	%xmm3, %xmm14
#endif

	movlpd	%xmm8, 0 * SIZE(CO1)
	movhpd	%xmm8, 1 * SIZE(CO1)
	movlpd	%xmm12, 2 * SIZE(CO1)
	movhpd	%xmm12, 3 * SIZE(CO1)

	movlpd	%xmm10, 0 * SIZE(CO2)
	movhpd	%xmm10, 1 * SIZE(CO2)
	movlpd	%xmm14, 2 * SIZE(CO2)
	movhpd	%xmm14, 3 * SIZE(CO2)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 8), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$2, KK
#endif

	addq	$4 * SIZE, CO1		# coffset += 4
	addq	$4 * SIZE, CO2		# coffset += 4
	decq	I			# i --
	jg	.L10
	ALIGN_4

.L30:
	testq	$1, M
	jle	.L99

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	16 * SIZE + BUFFER, BO
#else
	leaq	16 * SIZE + BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 8), BO
#endif

	movapd	-16 * SIZE(AO), %xmm0
	pxor	%xmm8, %xmm8
	movapd	 -8 * SIZE(AO), %xmm2
	pxor	%xmm9, %xmm9
	movapd	-16 * SIZE(BO), %xmm1
	pxor	%xmm10, %xmm10
	movapd	 -8 * SIZE(BO), %xmm3
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
	je	.L44
	ALIGN_4

.L41:
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm8
	movapd	-14 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	PREFETCH	(PREFETCHSIZE     +  0) * SIZE(AO)
	addpd	%xmm1, %xmm9
	movapd	-12 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	mulpd	-10 * SIZE(BO), %xmm0
	addpd	%xmm1, %xmm10
	movapd	  0 * SIZE(BO), %xmm1
	addpd	%xmm0, %xmm11
	movapd	-14 * SIZE(AO), %xmm0
	mulpd	%xmm0, %xmm3
	addpd	%xmm3, %xmm8
	movapd	 -6 * SIZE(BO), %xmm3
	mulpd	%xmm0, %xmm3
	addpd	%xmm3, %xmm9
	movapd	 -4 * SIZE(BO), %xmm3
	mulpd	%xmm0, %xmm3
	mulpd	 -2 * SIZE(BO), %xmm0
	addpd	%xmm3, %xmm10
	movapd	  8 * SIZE(BO), %xmm3
	addpd	%xmm0, %xmm11
	movapd	-12 * SIZE(AO), %xmm0
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm8
	movapd	  2 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	PREFETCH	(PREFETCHSIZE     +  8) * SIZE(AO)
	addpd	%xmm1, %xmm9
	movapd	  4 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	mulpd	  6 * SIZE(BO), %xmm0
	addpd	%xmm1, %xmm10
	movapd	 16 * SIZE(BO), %xmm1
	addpd	%xmm0, %xmm11
	movapd	-10 * SIZE(AO), %xmm0
	mulpd	%xmm0, %xmm3
	addpd	%xmm3, %xmm8
	movapd	 10 * SIZE(BO), %xmm3
	mulpd	%xmm0, %xmm3
	addpd	%xmm3, %xmm9
	movapd	 12 * SIZE(BO), %xmm3
	mulpd	%xmm0, %xmm3
	mulpd	 14 * SIZE(BO), %xmm0
	addpd	%xmm3, %xmm10
	movapd	 24 * SIZE(BO), %xmm3
	addpd	%xmm0, %xmm11
	movapd	  0 * SIZE(AO), %xmm0
	mulpd	%xmm2, %xmm1
	addpd	%xmm1, %xmm8
	movapd	 18 * SIZE(BO), %xmm1
	mulpd	%xmm2, %xmm1
	addpd	%xmm1, %xmm9
	movapd	 20 * SIZE(BO), %xmm1
	mulpd	%xmm2, %xmm1
	mulpd	 22 * SIZE(BO), %xmm2
	addpd	%xmm1, %xmm10
	movapd	 32 * SIZE(BO), %xmm1
	addpd	%xmm2, %xmm11
	movapd	 -6 * SIZE(AO), %xmm2
	mulpd	%xmm2, %xmm3
	addpd	%xmm3, %xmm8
	movapd	 26 * SIZE(BO), %xmm3
	mulpd	%xmm2, %xmm3
	addpd	%xmm3, %xmm9
	movapd	 28 * SIZE(BO), %xmm3
	mulpd	%xmm2, %xmm3
	mulpd	 30 * SIZE(BO), %xmm2
	addpd	%xmm3, %xmm10
	movapd	 40 * SIZE(BO), %xmm3
	addpd	%xmm2, %xmm11
	movapd	 -4 * SIZE(AO), %xmm2
	mulpd	%xmm2, %xmm1
	addpd	%xmm1, %xmm8
	movapd	 34 * SIZE(BO), %xmm1
	mulpd	%xmm2, %xmm1
	addpd	%xmm1, %xmm9
	movapd	 36 * SIZE(BO), %xmm1
	mulpd	%xmm2, %xmm1
	mulpd	 38 * SIZE(BO), %xmm2
	addpd	%xmm1, %xmm10
	movapd	 48 * SIZE(BO), %xmm1
	addpd	%xmm2, %xmm11
	movapd	 -2 * SIZE(AO), %xmm2
	mulpd	%xmm2, %xmm3
	addpd	%xmm3, %xmm8
	movapd	 42 * SIZE(BO), %xmm3
	mulpd	%xmm2, %xmm3
	addpd	%xmm3, %xmm9
	movapd	 44 * SIZE(BO), %xmm3
	mulpd	%xmm2, %xmm3
	mulpd	 46 * SIZE(BO), %xmm2
	addpd	%xmm3, %xmm10
	movapd	 56 * SIZE(BO), %xmm3
	addpd	%xmm2, %xmm11
	movapd	  8 * SIZE(AO), %xmm2

	subq   $-16 * SIZE, AO
	addq   $64 * SIZE, BO
	decq   %rax
	jne    .L41
	ALIGN_4

.L44:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$4, %rax
	BRANCH
	jle .L45

	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm8
	movapd	-14 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm9
	movapd	-12 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	mulpd	-10 * SIZE(BO), %xmm0
	addpd	%xmm1, %xmm10
	movapd	  0 * SIZE(BO), %xmm1
	addpd	%xmm0, %xmm11
	movapd	-14 * SIZE(AO), %xmm0
	mulpd	%xmm0, %xmm3
	addpd	%xmm3, %xmm8
	movapd	 -6 * SIZE(BO), %xmm3
	mulpd	%xmm0, %xmm3
	addpd	%xmm3, %xmm9
	movapd	 -4 * SIZE(BO), %xmm3
	mulpd	%xmm0, %xmm3
	mulpd	 -2 * SIZE(BO), %xmm0
	addpd	%xmm3, %xmm10
	movapd	  8 * SIZE(BO), %xmm3
	addpd	%xmm0, %xmm11
	movapd	-12 * SIZE(AO), %xmm0
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm8
	movapd	  2 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm9
	movapd	  4 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	mulpd	  6 * SIZE(BO), %xmm0
	addpd	%xmm1, %xmm10
	movapd	 16 * SIZE(BO), %xmm1
	addpd	%xmm0, %xmm11
	movapd	-10 * SIZE(AO), %xmm0
	mulpd	%xmm0, %xmm3
	addpd	%xmm3, %xmm8
	movapd	 10 * SIZE(BO), %xmm3
	mulpd	%xmm0, %xmm3
	addpd	%xmm3, %xmm9
	movapd	 12 * SIZE(BO), %xmm3
	mulpd	%xmm0, %xmm3
	mulpd	 14 * SIZE(BO), %xmm0
	addpd	%xmm3, %xmm10
	movapd	 24 * SIZE(BO), %xmm3
	addpd	%xmm0, %xmm11
	movapd	 -8 * SIZE(AO), %xmm0

	addq   $ 8 * SIZE, AO
	addq   $32 * SIZE, BO
	ALIGN_4

.L45:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	movapd	POSINV,  %xmm5
	movapd	ALPHA_R, %xmm6
	movapd	ALPHA_I, %xmm7
	andq	$3, %rax		# if (k & 1)
	BRANCH
	jle .L47
	ALIGN_4

.L46:
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm8
	movapd	-14 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm9
	movapd	-12 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	mulpd	-10 * SIZE(BO), %xmm0
	addpd	%xmm1, %xmm10
	movapd	 -8 * SIZE(BO), %xmm1
	addpd	%xmm0, %xmm11
	movapd	-14 * SIZE(AO), %xmm0

	addq	$2 * SIZE, AO
	addq	$8 * SIZE, BO

	decq	%rax
	jg	.L46
	ALIGN_4

.L47:
#ifndef TRMMKERNEL
	movlpd	0 * SIZE(CO1), %xmm0
	movhpd	1 * SIZE(CO1), %xmm0
	movlpd	0 * SIZE(CO2), %xmm1
	movhpd	1 * SIZE(CO2), %xmm1
#endif

	SHUFPD_1 %xmm9, %xmm9
	SHUFPD_1 %xmm11, %xmm11

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)
	xorpd	%xmm5, %xmm9
	xorpd	%xmm5, %xmm11
#else
	xorpd	%xmm5, %xmm8
	xorpd	%xmm5, %xmm10
#endif

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(RR) || defined(RC) || defined(CR) || defined(CC)
	subpd	%xmm9, %xmm8
	subpd	%xmm11, %xmm10
#else
	addpd	%xmm9, %xmm8
	addpd	%xmm11, %xmm10
#endif

	pshufd	$0x4e, %xmm8, %xmm9
	pshufd	$0x4e, %xmm10, %xmm11

	mulpd	%xmm6, %xmm8
	mulpd	%xmm7, %xmm9
	mulpd	%xmm6, %xmm10
	mulpd	%xmm7, %xmm11

	addpd	%xmm9, %xmm8
	addpd	%xmm11, %xmm10

#ifndef TRMMKERNEL
	addpd	%xmm0,  %xmm8
	addpd	%xmm1,  %xmm10
#endif

	movlpd	%xmm8, 0 * SIZE(CO1)
	movhpd	%xmm8, 1 * SIZE(CO1)
	movlpd	%xmm10, 0 * SIZE(CO2)
	movhpd	%xmm10, 1 * SIZE(CO2)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 8), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$1, KK
#endif
	ALIGN_4

.L99:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addl	$2, KK
#endif

	leaq	(C, LDC, 2), C		# c += 2 * ldc
	decq	J			# j --
	jg	.L01

.L100:
	testq	$1, N
	jle	.L999

.L101:
#if defined(TRMMKERNEL) && defined(LEFT)
	movq	OFFSET, %rax
	movq	%rax, KK
#endif

/* Copying to Sub Buffer */
	leaq	BUFFER, BO

	movq	K, %rax
	sarq	$2, %rax
	jle	.L103
	ALIGN_4

.L102:
	movlpd	 0 * SIZE(B), %xmm8
	movlpd	 1 * SIZE(B), %xmm9
	movlpd	 2 * SIZE(B), %xmm10
	movlpd	 3 * SIZE(B), %xmm11
	movlpd	 4 * SIZE(B), %xmm12
	movlpd	 5 * SIZE(B), %xmm13
	movlpd	 6 * SIZE(B), %xmm14
	movlpd	 7 * SIZE(B), %xmm15

	movlpd	%xmm8,  0 * SIZE(BO)
	movlpd	%xmm8,  1 * SIZE(BO)
	movlpd	%xmm9,  2 * SIZE(BO)
	movlpd	%xmm9,  3 * SIZE(BO)
	movlpd	%xmm10,  4 * SIZE(BO)
	movlpd	%xmm10,  5 * SIZE(BO)
	movlpd	%xmm11,  6 * SIZE(BO)
	movlpd	%xmm11,  7 * SIZE(BO)
	movlpd	%xmm12,  8 * SIZE(BO)
	movlpd	%xmm12,  9 * SIZE(BO)
	movlpd	%xmm13, 10 * SIZE(BO)
	movlpd	%xmm13, 11 * SIZE(BO)
	movlpd	%xmm14, 12 * SIZE(BO)
	movlpd	%xmm14, 13 * SIZE(BO)
	movlpd	%xmm15, 14 * SIZE(BO)
	movlpd	%xmm15, 15 * SIZE(BO)

	subq	$-16 * SIZE, BO
	addq	$ 8 * SIZE, B
	decq	%rax
	jne	.L102
	ALIGN_4

.L103:
	movq	K, %rax
	andq	$3, %rax
	BRANCH
	jle	.L105
	ALIGN_4

.L104:
	movlpd	 0 * SIZE(B), %xmm8
	movlpd	 1 * SIZE(B), %xmm9

	movlpd	%xmm8,  0 * SIZE(BO)
	movlpd	%xmm8,  1 * SIZE(BO)
	movlpd	%xmm9,  2 * SIZE(BO)
	movlpd	%xmm9,  3 * SIZE(BO)

	addq	$4 * SIZE, BO
	addq	$2 * SIZE, B
	decq	%rax
	jne	.L104
	ALIGN_4

.L105:
	movq	C, CO1		# coffset1 = c
	movq	A, AO		# aoffset = a

	movq	M,  I
	sarq	$1, I		# i = (m >> 2)
	jle	.L130
	ALIGN_4

.L110:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	16 * SIZE + BUFFER, BO
#else
	leaq	16 * SIZE + BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 4), BO
#endif

	movapd	-16 * SIZE(AO), %xmm0
	pxor	%xmm8, %xmm8
	movapd	-16 * SIZE(BO), %xmm1
	pxor	%xmm9, %xmm9
	movapd	 -8 * SIZE(AO), %xmm2
	pxor	%xmm12, %xmm12
	movapd	 -8 * SIZE(BO), %xmm3
	pxor	%xmm13, %xmm13
	PREFETCHW      3 * SIZE(CO1)

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
	sarq	$2, %rax
	je	.L112

.L111:
	mulpd	%xmm0, %xmm1
	PREFETCH	(PREFETCHSIZE     +  0) * SIZE(AO)
	mulpd	-14 * SIZE(BO), %xmm0
	addpd	%xmm1, %xmm8
	movapd	-16 * SIZE(BO), %xmm1
	addpd	%xmm0, %xmm9
	movapd	-14 * SIZE(AO), %xmm0
	mulpd	%xmm0, %xmm1
	mulpd	-14 * SIZE(BO), %xmm0
	addpd	%xmm1, %xmm12
	movapd	-12 * SIZE(BO), %xmm1
	addpd	%xmm0, %xmm13
	movapd	-12 * SIZE(AO), %xmm0
	mulpd	%xmm0, %xmm1
	mulpd	-10 * SIZE(BO), %xmm0
	addpd	%xmm1, %xmm8
	movapd	-12 * SIZE(BO), %xmm1
	addpd	%xmm0, %xmm9
	movapd	-10 * SIZE(AO), %xmm0
	mulpd	%xmm0, %xmm1
	mulpd	-10 * SIZE(BO), %xmm0
	addpd	%xmm1, %xmm12
	movapd	  0 * SIZE(BO), %xmm1
	addpd	%xmm0, %xmm13
	movapd	  0 * SIZE(AO), %xmm0
	mulpd	%xmm2, %xmm3
	PREFETCH	(PREFETCHSIZE     +  8) * SIZE(AO)
	mulpd	 -6 * SIZE(BO), %xmm2
	addpd	%xmm3, %xmm8
	movapd	 -8 * SIZE(BO), %xmm3
	addpd	%xmm2, %xmm9
	movapd	 -6 * SIZE(AO), %xmm2
	mulpd	%xmm2, %xmm3
	mulpd	 -6 * SIZE(BO), %xmm2
	addpd	%xmm3, %xmm12
	movapd	 -4 * SIZE(BO), %xmm3
	addpd	%xmm2, %xmm13
	movapd	 -4 * SIZE(AO), %xmm2
	mulpd	%xmm2, %xmm3
	mulpd	 -2 * SIZE(BO), %xmm2
	addpd	%xmm3, %xmm8
	movapd	 -4 * SIZE(BO), %xmm3
	addpd	%xmm2, %xmm9
	movapd	 -2 * SIZE(AO), %xmm2
	mulpd	%xmm2, %xmm3
	mulpd	 -2 * SIZE(BO), %xmm2
	addpd	%xmm3, %xmm12
	movapd	  8 * SIZE(BO), %xmm3
	addpd	%xmm2, %xmm13
	movapd	  8 * SIZE(AO), %xmm2

	subq   $-16 * SIZE, AO
	subq   $-16 * SIZE, BO
	decq   %rax
	jne    .L111
	ALIGN_4

.L112:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	movapd	POSINV,  %xmm5
	movapd	ALPHA_R, %xmm6
	movapd	ALPHA_I, %xmm7
	andq	$3, %rax		# if (k & 1)
	BRANCH
	jle .L114

.L113:
	mulpd	%xmm0, %xmm1
	mulpd	 -14 * SIZE(BO), %xmm0
	addpd	%xmm1, %xmm8
	movapd	 -16 * SIZE(BO), %xmm1
	addpd	%xmm0, %xmm9
	movapd	 -14 * SIZE(AO), %xmm0
	mulpd	%xmm0, %xmm1
	mulpd	 -14 * SIZE(BO), %xmm0
	addpd	%xmm1, %xmm12
	movapd	 -12 * SIZE(BO), %xmm1
	addpd	%xmm0, %xmm13
	movapd	 -12 * SIZE(AO), %xmm0

	addq	$4 * SIZE, AO		# aoffset  += 4
	addq	$4 * SIZE, BO		# boffset1 += 8
	decq	%rax
	jg	.L113
	ALIGN_4

.L114:
#ifndef TRMMKERNEL
	movlpd	0 * SIZE(CO1), %xmm0
	movhpd	1 * SIZE(CO1), %xmm0
	movlpd	2 * SIZE(CO1), %xmm2
	movhpd	3 * SIZE(CO1), %xmm2
#endif

	SHUFPD_1 %xmm9, %xmm9
	SHUFPD_1 %xmm13, %xmm13

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)
	xorpd	%xmm5, %xmm9
	xorpd	%xmm5, %xmm13
#else
	xorpd	%xmm5, %xmm8
	xorpd	%xmm5, %xmm12
#endif

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(RR) || defined(RC) || defined(CR) || defined(CC)
	subpd	%xmm9, %xmm8
	subpd	%xmm13, %xmm12
#else
	addpd	%xmm9, %xmm8
	addpd	%xmm13, %xmm12
#endif

	pshufd	$0x4e, %xmm8, %xmm9
	pshufd	$0x4e, %xmm12, %xmm13

	mulpd	%xmm6, %xmm8
	mulpd	%xmm7, %xmm9
	mulpd	%xmm6, %xmm12
	mulpd	%xmm7, %xmm13

	addpd	%xmm9, %xmm8
	addpd	%xmm13, %xmm12

#ifndef TRMMKERNEL
	addpd	%xmm0,  %xmm8
	addpd	%xmm2, %xmm12
#endif

	movlpd	%xmm8, 0 * SIZE(CO1)
	movhpd	%xmm8, 1 * SIZE(CO1)
	movlpd	%xmm12, 2 * SIZE(CO1)
	movhpd	%xmm12, 3 * SIZE(CO1)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 4), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$2, KK
#endif

	addq	$4 * SIZE, CO1		# coffset += 4
	decq	I			# i --
	jg	.L110
	ALIGN_4

.L130:
	testq	$1, M
	jle	.L999

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	16 * SIZE + BUFFER, BO
#else
	leaq	16 * SIZE + BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 4), BO
#endif

	movapd	-16 * SIZE(AO), %xmm0
	movapd	-16 * SIZE(BO), %xmm1
	movapd	 -8 * SIZE(AO), %xmm2
	movapd	 -8 * SIZE(BO), %xmm3

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
	addq	$1, %rax
#endif
	movq	%rax, KKK
#endif
	sarq	$3, %rax
	je	.L144
	ALIGN_4

.L141:
	mulpd	%xmm0, %xmm1
	PREFETCH	(PREFETCHSIZE     +  0) * SIZE(AO)
	mulpd	-14 * SIZE(BO), %xmm0
	addpd	%xmm1, %xmm8
	movapd	-12 * SIZE(BO), %xmm1
	addpd	%xmm0, %xmm9
	movapd	-14 * SIZE(AO), %xmm0
	mulpd	%xmm0, %xmm1
	mulpd	-10 * SIZE(BO), %xmm0
	addpd	%xmm1, %xmm10
	movapd	  0 * SIZE(BO), %xmm1
	addpd	%xmm0, %xmm11
	movapd	-12 * SIZE(AO), %xmm0
	mulpd	%xmm0, %xmm3
	mulpd	 -6 * SIZE(BO), %xmm0
	addpd	%xmm3, %xmm8
	movapd	 -4 * SIZE(BO), %xmm3
	addpd	%xmm0, %xmm9
	movapd	-10 * SIZE(AO), %xmm0
	mulpd	%xmm0, %xmm3
	mulpd	 -2 * SIZE(BO), %xmm0
	addpd	%xmm3, %xmm10
	movapd	  8 * SIZE(BO), %xmm3
	addpd	%xmm0, %xmm11
	movapd	  0 * SIZE(AO), %xmm0
	mulpd	%xmm2, %xmm1
	PREFETCH	(PREFETCHSIZE     +  8) * SIZE(AO)
	mulpd	  2 * SIZE(BO), %xmm2
	addpd	%xmm1, %xmm8
	movapd	  4 * SIZE(BO), %xmm1
	addpd	%xmm2, %xmm9
	movapd	 -6 * SIZE(AO), %xmm2
	mulpd	%xmm2, %xmm1
	mulpd	  6 * SIZE(BO), %xmm2
	addpd	%xmm1, %xmm10
	movapd	 16 * SIZE(BO), %xmm1
	addpd	%xmm2, %xmm11
	movapd	 -4 * SIZE(AO), %xmm2
	mulpd	%xmm2, %xmm3
	mulpd	 10 * SIZE(BO), %xmm2
	addpd	%xmm3, %xmm8
	movapd	 12 * SIZE(BO), %xmm3
	addpd	%xmm2, %xmm9
	movapd	 -2 * SIZE(AO), %xmm2
	mulpd	%xmm2, %xmm3
	mulpd	 14 * SIZE(BO), %xmm2
	addpd	%xmm3, %xmm10
	movapd	 24 * SIZE(BO), %xmm3
	addpd	%xmm2, %xmm11
	movapd	  8 * SIZE(AO), %xmm2

	subq   $-16 * SIZE, AO
	subq   $-32 * SIZE, BO
	decq   %rax
	jne    .L141
	ALIGN_4


.L144:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$4, %rax		# if (k & 1)
	BRANCH
	jle .L145

	mulpd	%xmm0, %xmm1
	mulpd	-14 * SIZE(BO), %xmm0
	addpd	%xmm1, %xmm8
	movapd	-12 * SIZE(BO), %xmm1
	addpd	%xmm0, %xmm9
	movapd	-14 * SIZE(AO), %xmm0
	mulpd	%xmm0, %xmm1
	mulpd	-10 * SIZE(BO), %xmm0
	addpd	%xmm1, %xmm10
	movapd	  0 * SIZE(BO), %xmm1
	addpd	%xmm0, %xmm11
	movapd	-12 * SIZE(AO), %xmm0
	mulpd	%xmm0, %xmm3
	mulpd	 -6 * SIZE(BO), %xmm0
	addpd	%xmm3, %xmm8
	movapd	 -4 * SIZE(BO), %xmm3
	addpd	%xmm0, %xmm9
	movapd	-10 * SIZE(AO), %xmm0
	mulpd	%xmm0, %xmm3
	mulpd	 -2 * SIZE(BO), %xmm0
	addpd	%xmm3, %xmm10
	addpd	%xmm0, %xmm11
	movapd	 -8 * SIZE(AO), %xmm0

	addq   $8   * SIZE, AO
	subq   $-16 * SIZE, BO
	ALIGN_4

.L145:
	movapd	POSINV, %xmm5
	movapd	ALPHA_R, %xmm6
	movapd	ALPHA_I, %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	BRANCH
	jle .L148
	ALIGN_4

.L146:
	mulpd	%xmm0, %xmm1
	mulpd	-14 * SIZE(BO), %xmm0
	addpd	%xmm1, %xmm8
	movapd	-12 * SIZE(BO), %xmm1
	addpd	%xmm0, %xmm9
	movapd	-14 * SIZE(AO), %xmm0

	addq	$2 * SIZE, AO		# aoffset  += 4
	addq	$4 * SIZE, BO		# boffset1 += 8
	decq	%rax
	jg	.L146
	ALIGN_4

.L148:
	addpd	%xmm10, %xmm8
	addpd	%xmm11, %xmm9

#ifndef TRMMKERNEL
	movlpd	0 * SIZE(CO1), %xmm0
	movhpd	1 * SIZE(CO1), %xmm0
#endif

	SHUFPD_1 %xmm9, %xmm9

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)
	xorpd	%xmm5, %xmm9
#else
	xorpd	%xmm5, %xmm8
#endif

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(RR) || defined(RC) || defined(CR) || defined(CC)
	subpd	%xmm9, %xmm8
#else
	addpd	%xmm9, %xmm8
#endif

	pshufd	$0x4e, %xmm8, %xmm9

	mulpd	%xmm6, %xmm8
	mulpd	%xmm7, %xmm9

	addpd	%xmm9, %xmm8

#ifndef TRMMKERNEL
	addpd	%xmm0,  %xmm8
#endif

	movlpd	%xmm8, 0 * SIZE(CO1)
	movhpd	%xmm8, 1 * SIZE(CO1)
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
