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

#define OLD_A		40 + STACKSIZE(%rsp)
#define OLD_B		48 + STACKSIZE(%rsp)
#define OLD_C		56 + STACKSIZE(%rsp)
#define OLD_LDC		64 + STACKSIZE(%rsp)
#define OLD_OFFSET	72 + STACKSIZE(%rsp)

#endif

#define ALPHA	  0(%rsp)
#define J	 16(%rsp)
#define OFFSET	 24(%rsp)
#define KK	 32(%rsp)
#define KKK	 40(%rsp)
#define BUFFER	256(%rsp)

#ifdef OPTERON
#define PREFETCH     prefetch
#define PREFETCHW    prefetchw
#define PREFETCHSIZE (8 * 9 + 4)
#define movsd	movlps
#define movapd	movaps
#endif

#ifdef GENERIC
#define PREFETCH     prefetcht0
#define PREFETCHW    prefetcht0
#define PREFETCHSIZE (8 * 13 + 4)
#define movapd	movaps
#endif

#ifndef GENERIC
#define KERNEL1(xx) \
	mulpd	%xmm0, %xmm1 ;\
	addpd	%xmm1, %xmm8 ;\
	movaps	-16 * SIZE + 2 * (xx) * SIZE(BO, %rax, 8), %xmm1 ;\
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
	movaps	%xmm3, %xmm0

#else
	movq	OLD_LDC,   LDC
#ifdef TRMMKERNEL
	movsd	OLD_OFFSET, %xmm12
#endif

#endif

	EMMS

	movq	%rsp, %rbx	# save old stack
	subq	$256 + LOCAL_BUFFER_SIZE, %rsp
	andq	$-4096, %rsp	# align stack

	STACK_TOUCHING

	movq	OLD_M, M
	movq	OLD_N, N

	subq	$-16 * SIZE, A

	unpcklpd %xmm0, %xmm0
	movapd	 %xmm0, ALPHA

	leaq	(, LDC, SIZE), LDC

#ifdef TRMMKERNEL
	movsd	%xmm12, OFFSET
	movsd	%xmm12, KK
#ifndef LEFT
	negq	KK
#endif
#endif
	movq	N,  J
	sarq	$2, J		# j = (n >> 2)
	jle	.L40
	ALIGN_3

.L01:
/* Copying to Sub Buffer */
	leaq	16 * SIZE + BUFFER, BO
	movq	C, CO1			# coffset1 = c
	leaq	(C, LDC, 1), CO2	# coffset2 = c + ldc

#if defined(TRMMKERNEL) && defined(LEFT)
	movq	OFFSET, %rax
	movq	%rax, KK
#endif

	movq	K, %rax
	sarq	$2, %rax
	jle	.L03
	ALIGN_3


#define RPREFETCHSIZE (8 *  7 + 4)
#define WPREFETCHSIZE (8 *  8 + 4)

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

	subq	$1, %rax
	jne	.L02
	ALIGN_3

.L03:
	movq	K, %rax
	andq	$3, %rax
	BRANCH
	jle	.L10
	ALIGN_3

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

	addq	$4 * SIZE, B
	addq	$8 * SIZE, BO
	subq	$1, %rax
	jne	.L04
	ALIGN_3

.L10:
	movq	A, AO		# aoffset = a

	leaq	 (RPREFETCHSIZE +  0)  * SIZE(B), BB

	movq	M,  I
	sarq	$2, I	# i = (m >> 2)
	jle	.L20
	ALIGN_3

.L11:
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
	movapd	-14 * SIZE(AO), %xmm2
	movapd	-14 * SIZE(BO), %xmm3
	pxor	%xmm9, %xmm9
	movapd	-12 * SIZE(AO), %xmm4
	movapd	-12 * SIZE(BO), %xmm5
	pxor	%xmm10, %xmm10
	movapd	-10 * SIZE(AO), %xmm6
	movapd	 -8 * SIZE(BO), %xmm7
	pxor	%xmm11, %xmm11

	PREFETCHW      3 * SIZE(CO1)
	pxor	%xmm12, %xmm12
	PREFETCHW      7 * SIZE(CO2)
	pxor	%xmm13, %xmm13
	PREFETCHW      3 * SIZE(CO1, LDC, 2)
	pxor	%xmm14, %xmm14
	PREFETCHW      7 * SIZE(CO2, LDC, 2)
	pxor	%xmm15, %xmm15

	PREFETCH	 0  * SIZE(BB)

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
	movapd	ALPHA, %xmm7

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
	PREFETCH	 8  * SIZE(BB)
	subq		 $-12 * SIZE, BB

#ifndef TRMMKERNEL
	movsd	0 * SIZE(CO1), %xmm0
	movhpd	1 * SIZE(CO1), %xmm0
	movsd	2 * SIZE(CO1), %xmm1
	movhpd	3 * SIZE(CO1), %xmm1

	movsd	0 * SIZE(CO2), %xmm2
	movhpd	1 * SIZE(CO2), %xmm2
	movsd	2 * SIZE(CO2), %xmm3
	movhpd	3 * SIZE(CO2), %xmm3
#endif

	mulpd	%xmm7, %xmm8
	mulpd	%xmm7, %xmm9
	mulpd	%xmm7, %xmm10
	mulpd	%xmm7, %xmm11

	mulpd	%xmm7, %xmm12
	mulpd	%xmm7, %xmm13
	mulpd	%xmm7, %xmm14
	mulpd	%xmm7, %xmm15

#ifndef TRMMKERNEL
	movlpd	0 * SIZE(CO1, LDC, 2), %xmm4
	movhpd	1 * SIZE(CO1, LDC, 2), %xmm4
	movlpd	2 * SIZE(CO1, LDC, 2), %xmm5
	movhpd	3 * SIZE(CO1, LDC, 2), %xmm5

	movlpd	0 * SIZE(CO2, LDC, 2), %xmm6
	movhpd	1 * SIZE(CO2, LDC, 2), %xmm6
	movlpd	2 * SIZE(CO2, LDC, 2), %xmm7
	movhpd	3 * SIZE(CO2, LDC, 2), %xmm7

	addpd	%xmm0, %xmm8
	addpd	%xmm1, %xmm12
	addpd	%xmm2, %xmm9
	addpd	%xmm3, %xmm13
#endif

	movlpd	%xmm8, 0 * SIZE(CO1)
	movhpd	%xmm8, 1 * SIZE(CO1)
	movlpd	%xmm12, 2 * SIZE(CO1)
	movhpd	%xmm12, 3 * SIZE(CO1)

	movlpd	%xmm9, 0 * SIZE(CO2)
	movhpd	%xmm9, 1 * SIZE(CO2)
	movlpd	%xmm13, 2 * SIZE(CO2)
	movhpd	%xmm13, 3 * SIZE(CO2)

#ifndef TRMMKERNEL
	addpd	%xmm4, %xmm10
	addpd	%xmm5, %xmm14
	addpd	%xmm6, %xmm11
	addpd	%xmm7, %xmm15
#endif

	movlpd	%xmm10, 0 * SIZE(CO1, LDC, 2)
	movhpd	%xmm10, 1 * SIZE(CO1, LDC, 2)
	movlpd	%xmm14, 2 * SIZE(CO1, LDC, 2)
	movhpd	%xmm14, 3 * SIZE(CO1, LDC, 2)

	movlpd	%xmm11, 0 * SIZE(CO2, LDC, 2)
	movhpd	%xmm11, 1 * SIZE(CO2, LDC, 2)
	movlpd	%xmm15, 2 * SIZE(CO2, LDC, 2)
	movhpd	%xmm15, 3 * SIZE(CO2, LDC, 2)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 8), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$4, KK
#endif

	addq	$4 * SIZE, CO1		# coffset += 4
	addq	$4 * SIZE, CO2		# coffset += 4
	decq	I			# i --
	BRANCH
	jg	.L11
	ALIGN_3

.L20:
	testq	$3, M
	je	.L39

	testq	$2, M
	je	.L30
	ALIGN_3

.L21:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	BUFFER, BO
#else
	leaq	BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 8), BO
#endif

	movapd	-16 * SIZE(AO), %xmm0
	pxor	%xmm8, %xmm8
	movapd	 0 * SIZE(BO), %xmm1
	pxor	%xmm9, %xmm9
	movapd	 -8 * SIZE(AO), %xmm2
	pxor	%xmm10, %xmm10
	movapd	 8 * SIZE(BO), %xmm3
	pxor	%xmm11, %xmm11

	movapd	16 * SIZE(BO), %xmm5
	movapd	24 * SIZE(BO), %xmm7

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
	je	.L25
	ALIGN_3

.L22:
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm8
	PREFETCH	(PREFETCHSIZE     +  0) * SIZE(AO)
	movapd	 2 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm9
	movapd	 4 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	mulpd	 6 * SIZE(BO), %xmm0
	addpd	%xmm1, %xmm10
	movapd	32 * SIZE(BO), %xmm1
	addpd	%xmm0, %xmm11
	movapd	-14 * SIZE(AO), %xmm0

	mulpd	%xmm0, %xmm3
	addpd	%xmm3, %xmm8
	movapd	10 * SIZE(BO), %xmm3
	mulpd	%xmm0, %xmm3
	addpd	%xmm3, %xmm9
	movapd	12 * SIZE(BO), %xmm3
	mulpd	%xmm0, %xmm3
	mulpd	14 * SIZE(BO), %xmm0
	addpd	%xmm3, %xmm10
	movapd	40 * SIZE(BO), %xmm3
	addpd	%xmm0, %xmm11
	movapd	-12 * SIZE(AO), %xmm0

	mulpd	%xmm0, %xmm5
	addpd	%xmm5, %xmm8
	movapd	18 * SIZE(BO), %xmm5
	mulpd	%xmm0, %xmm5
	addpd	%xmm5, %xmm9
	movapd	20 * SIZE(BO), %xmm5
	mulpd	%xmm0, %xmm5
	mulpd	22 * SIZE(BO), %xmm0
	addpd	%xmm5, %xmm10
	movapd	48 * SIZE(BO), %xmm5
	addpd	%xmm0, %xmm11
	movapd	-10 * SIZE(AO), %xmm0

	mulpd	%xmm0, %xmm7
	addpd	%xmm7, %xmm8
	movapd	26 * SIZE(BO), %xmm7
	mulpd	%xmm0, %xmm7
	addpd	%xmm7, %xmm9
	movapd	28 * SIZE(BO), %xmm7
	mulpd	%xmm0, %xmm7
	mulpd	30 * SIZE(BO), %xmm0
	addpd	%xmm7, %xmm10
	movapd	56 * SIZE(BO), %xmm7
	addpd	%xmm0, %xmm11
	movapd	  0 * SIZE(AO), %xmm0

	PREFETCH	(PREFETCHSIZE     +  8) * SIZE(AO)
	mulpd	%xmm2, %xmm1
	addpd	%xmm1, %xmm8
	movapd	34 * SIZE(BO), %xmm1
	mulpd	%xmm2, %xmm1
	addpd	%xmm1, %xmm9
	movapd	36 * SIZE(BO), %xmm1
	mulpd	%xmm2, %xmm1
	mulpd	38 * SIZE(BO), %xmm2
	addpd	%xmm1, %xmm10
	movapd	64 * SIZE(BO), %xmm1
	addpd	%xmm2, %xmm11
	movapd	-6 * SIZE(AO), %xmm2

	mulpd	%xmm2, %xmm3
	addpd	%xmm3, %xmm8
	movapd	42 * SIZE(BO), %xmm3
	mulpd	%xmm2, %xmm3
	addpd	%xmm3, %xmm9
	movapd	44 * SIZE(BO), %xmm3
	mulpd	%xmm2, %xmm3
	mulpd	46 * SIZE(BO), %xmm2
	addpd	%xmm3, %xmm10
	movapd	72 * SIZE(BO), %xmm3
	addpd	%xmm2, %xmm11
	movapd	-4 * SIZE(AO), %xmm2

	mulpd	%xmm2, %xmm5
	addpd	%xmm5, %xmm8
	movapd	50 * SIZE(BO), %xmm5
	mulpd	%xmm2, %xmm5
	addpd	%xmm5, %xmm9
	movapd	52 * SIZE(BO), %xmm5
	mulpd	%xmm2, %xmm5
	mulpd	54 * SIZE(BO), %xmm2
	addpd	%xmm5, %xmm10
	movapd	80 * SIZE(BO), %xmm5
	addpd	%xmm2, %xmm11
	movapd	-2 * SIZE(AO), %xmm2

	mulpd	%xmm2, %xmm7
	addpd	%xmm7, %xmm8
	movapd	58 * SIZE(BO), %xmm7
	mulpd	%xmm2, %xmm7
	addpd	%xmm7, %xmm9
	movapd	60 * SIZE(BO), %xmm7
	mulpd	%xmm2, %xmm7
	mulpd	62 * SIZE(BO), %xmm2
	addpd	%xmm7, %xmm10
	movapd	88 * SIZE(BO), %xmm7
	addpd	%xmm2, %xmm11
	movapd	 8 * SIZE(AO), %xmm2

	addq   $16 * SIZE, AO
	addq   $64 * SIZE, BO
	decq   %rax
	jne    .L22
	ALIGN_3

.L25:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	movapd	ALPHA, %xmm7
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L29
	ALIGN_3

.L26:
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm8
	movapd	  2 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm9
	movapd	  4 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	mulpd	  6 * SIZE(BO), %xmm0
	addpd	%xmm1, %xmm10
	movapd	  8 * SIZE(BO), %xmm1
	addpd	%xmm0, %xmm11
	movapd	-14 * SIZE(AO), %xmm0

	addq	$2 * SIZE, AO		# aoffset  += 4
	addq	$8 * SIZE, BO		# boffset1 += 8
	decq	%rax
	jg	.L26
	ALIGN_3

.L29:
#ifndef TRMMKERNEL
	movlpd	0 * SIZE(CO1), %xmm0
	movhpd	1 * SIZE(CO1), %xmm0
	movlpd	0 * SIZE(CO2), %xmm2
	movhpd	1 * SIZE(CO2), %xmm2

	movlpd	0 * SIZE(CO1, LDC, 2), %xmm4
	movhpd	1 * SIZE(CO1, LDC, 2), %xmm4
	movlpd	0 * SIZE(CO2, LDC, 2), %xmm6
	movhpd	1 * SIZE(CO2, LDC, 2), %xmm6
#endif
	mulpd	%xmm7, %xmm8
	mulpd	%xmm7, %xmm9
	mulpd	%xmm7, %xmm10
	mulpd	%xmm7, %xmm11

#ifndef TRMMKERNEL
	addpd	%xmm0,  %xmm8
	addpd	%xmm2, %xmm9
	addpd	%xmm4, %xmm10
	addpd	%xmm6, %xmm11
#endif

	movlpd	%xmm8, 0 * SIZE(CO1)
	movhpd	%xmm8, 1 * SIZE(CO1)
	movlpd	%xmm9, 0 * SIZE(CO2)
	movhpd	%xmm9, 1 * SIZE(CO2)
	movlpd	%xmm10, 0 * SIZE(CO1, LDC, 2)
	movhpd	%xmm10, 1 * SIZE(CO1, LDC, 2)
	movlpd	%xmm11, 0 * SIZE(CO2, LDC, 2)
	movhpd	%xmm11, 1 * SIZE(CO2, LDC, 2)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 8), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$2, KK
#endif

	addq	$2 * SIZE, CO1		# coffset += 4
	addq	$2 * SIZE, CO2		# coffset += 4
	ALIGN_3

.L30:
	testq	$1, M
	je	.L39
	ALIGN_3

.L31:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	BUFFER, BO
#else
	leaq	BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 8), BO
#endif

	movsd	-16 * SIZE(AO), %xmm0
	pxor	%xmm8, %xmm8
	movsd	 0 * SIZE(BO), %xmm1
	pxor	%xmm9, %xmm9
	movsd	 -8 * SIZE(AO), %xmm2
	pxor	%xmm10, %xmm10
	movsd	 8 * SIZE(BO), %xmm3
	pxor	%xmm11, %xmm11

	movsd	16 * SIZE(BO), %xmm5
	movsd	24 * SIZE(BO), %xmm7

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
	je	.L35
	ALIGN_3

.L32:
	mulsd	%xmm0, %xmm1
	addsd	%xmm1, %xmm8
	PREFETCH	(PREFETCHSIZE     +  0) * SIZE(AO)
	movsd	 2 * SIZE(BO), %xmm1
	mulsd	%xmm0, %xmm1
	addsd	%xmm1, %xmm9
	movsd	 4 * SIZE(BO), %xmm1
	mulsd	%xmm0, %xmm1
	mulsd	 6 * SIZE(BO), %xmm0
	addsd	%xmm1, %xmm10
	movsd	32 * SIZE(BO), %xmm1
	addsd	%xmm0, %xmm11
	movsd	-15 * SIZE(AO), %xmm0

	mulsd	%xmm0, %xmm3
	addsd	%xmm3, %xmm8
	movsd	10 * SIZE(BO), %xmm3
	mulsd	%xmm0, %xmm3
	addsd	%xmm3, %xmm9
	movsd	12 * SIZE(BO), %xmm3
	mulsd	%xmm0, %xmm3
	mulsd	14 * SIZE(BO), %xmm0
	addsd	%xmm3, %xmm10
	movsd	40 * SIZE(BO), %xmm3
	addsd	%xmm0, %xmm11
	movsd	-14 * SIZE(AO), %xmm0

	mulsd	%xmm0, %xmm5
	addsd	%xmm5, %xmm8
	movsd	18 * SIZE(BO), %xmm5
	mulsd	%xmm0, %xmm5
	addsd	%xmm5, %xmm9
	movsd	20 * SIZE(BO), %xmm5
	mulsd	%xmm0, %xmm5
	mulsd	22 * SIZE(BO), %xmm0
	addsd	%xmm5, %xmm10
	movsd	48 * SIZE(BO), %xmm5
	addsd	%xmm0, %xmm11
	movsd	-13 * SIZE(AO), %xmm0

	mulsd	%xmm0, %xmm7
	addsd	%xmm7, %xmm8
	movsd	26 * SIZE(BO), %xmm7
	mulsd	%xmm0, %xmm7
	addsd	%xmm7, %xmm9
	movsd	28 * SIZE(BO), %xmm7
	mulsd	%xmm0, %xmm7
	mulsd	30 * SIZE(BO), %xmm0
	addsd	%xmm7, %xmm10
	movsd	56 * SIZE(BO), %xmm7
	addsd	%xmm0, %xmm11
	movsd	-12 * SIZE(AO), %xmm0

	mulsd	%xmm0, %xmm1
	addsd	%xmm1, %xmm8
	movsd	34 * SIZE(BO), %xmm1
	mulsd	%xmm0, %xmm1
	addsd	%xmm1, %xmm9
	movsd	36 * SIZE(BO), %xmm1
	mulsd	%xmm0, %xmm1
	mulsd	38 * SIZE(BO), %xmm0
	addsd	%xmm1, %xmm10
	movsd	64 * SIZE(BO), %xmm1
	addsd	%xmm0, %xmm11
	movsd	-11 * SIZE(AO), %xmm0

	mulsd	%xmm0, %xmm3
	addsd	%xmm3, %xmm8
	movsd	42 * SIZE(BO), %xmm3
	mulsd	%xmm0, %xmm3
	addsd	%xmm3, %xmm9
	movsd	44 * SIZE(BO), %xmm3
	mulsd	%xmm0, %xmm3
	mulsd	46 * SIZE(BO), %xmm0
	addsd	%xmm3, %xmm10
	movsd	72 * SIZE(BO), %xmm3
	addsd	%xmm0, %xmm11
	movsd	-10 * SIZE(AO), %xmm0

	mulsd	%xmm0, %xmm5
	addsd	%xmm5, %xmm8
	movsd	50 * SIZE(BO), %xmm5
	mulsd	%xmm0, %xmm5
	addsd	%xmm5, %xmm9
	movsd	52 * SIZE(BO), %xmm5
	mulsd	%xmm0, %xmm5
	mulsd	54 * SIZE(BO), %xmm0
	addsd	%xmm5, %xmm10
	movsd	80 * SIZE(BO), %xmm5
	addsd	%xmm0, %xmm11
	movsd	-9 * SIZE(AO), %xmm0

	mulsd	%xmm0, %xmm7
	addsd	%xmm7, %xmm8
	movsd	58 * SIZE(BO), %xmm7
	mulsd	%xmm0, %xmm7
	addsd	%xmm7, %xmm9
	movsd	60 * SIZE(BO), %xmm7
	mulsd	%xmm0, %xmm7
	mulsd	62 * SIZE(BO), %xmm0
	addsd	%xmm7, %xmm10
	movsd	88 * SIZE(BO), %xmm7
	addsd	%xmm0, %xmm11
	movsd	-8 * SIZE(AO), %xmm0

	addq   $ 8 * SIZE, AO
	addq   $64 * SIZE, BO
	decq   %rax
	jne    .L32
	ALIGN_3

.L35:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	movsd	ALPHA, %xmm7
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L38
	ALIGN_3

.L36:
	mulsd	%xmm0, %xmm1
	addsd	%xmm1, %xmm8
	movsd	 2 * SIZE(BO), %xmm1
	mulsd	%xmm0, %xmm1
	addsd	%xmm1, %xmm9
	movsd	 4 * SIZE(BO), %xmm1
	mulsd	%xmm0, %xmm1
	mulsd	 6 * SIZE(BO), %xmm0
	addsd	%xmm1, %xmm10
	movsd	 8 * SIZE(BO), %xmm1
	addsd	%xmm0, %xmm11
	movsd	-15 * SIZE(AO), %xmm0

	addq	$1 * SIZE, AO		# aoffset  += 4
	addq	$8 * SIZE, BO		# boffset1 += 8
	decq	%rax
	jg	.L36
	ALIGN_3

.L38:
#ifndef TRMMKERNEL
	movsd	0 * SIZE(CO1), %xmm0
	movsd	0 * SIZE(CO2), %xmm2
	movsd	0 * SIZE(CO1, LDC, 2), %xmm4
	movsd	0 * SIZE(CO2, LDC, 2), %xmm6
#endif

	mulsd	%xmm7, %xmm8
	mulsd	%xmm7, %xmm9
	mulsd	%xmm7, %xmm10
	mulsd	%xmm7, %xmm11

#ifndef TRMMKERNEL
	addsd	%xmm0,  %xmm8
	addsd	%xmm2, %xmm9
	addsd	%xmm4, %xmm10
	addsd	%xmm6, %xmm11
#endif

	movsd	%xmm8, 0 * SIZE(CO1)
	movsd	%xmm9, 0 * SIZE(CO2)
	movsd	%xmm10, 0 * SIZE(CO1, LDC, 2)
	movsd	%xmm11, 0 * SIZE(CO2, LDC, 2)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 8), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$1, KK
#endif
	ALIGN_3

.L39:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addl	$4, KK
#endif

	leaq	(C, LDC, 4), C		# c += 4 * ldc
	decq	J			# j --
	jg	.L01
	ALIGN_3

.L40:
	testq	$3, N
	je	.L999

	testq	$2, N
	je	.L80
	ALIGN_4

.L41:
/* Copying to Sub Buffer */
	leaq	BUFFER, BO

#if defined(TRMMKERNEL) && defined(LEFT)
	movq	OFFSET, %rax
	movq	%rax, KK
#endif

	movq	K, %rax
	sarq	$2, %rax
	jle	.L43
	ALIGN_3

.L42:
	PREFETCH	 56 * SIZE(B)

	movq	 0 * SIZE(B), %mm0
	movq	 1 * SIZE(B), %mm1
	movq	 2 * SIZE(B), %mm2
	movq	 3 * SIZE(B), %mm3
	movq	 4 * SIZE(B), %mm4
	movq	 5 * SIZE(B), %mm5
	movq	 6 * SIZE(B), %mm6
	movq	 7 * SIZE(B), %mm7

	addq	$ 8 * SIZE, B
	addq	$16 * SIZE, BO

	movq	%mm0, -16 * SIZE(BO)
	movq	%mm0, -15 * SIZE(BO)
	movq	%mm1, -14 * SIZE(BO)
	movq	%mm1, -13 * SIZE(BO)
	movq	%mm2, -12 * SIZE(BO)
	movq	%mm2, -11 * SIZE(BO)
	movq	%mm3, -10 * SIZE(BO)
	movq	%mm3,  -9 * SIZE(BO)
	movq	%mm4,  -8 * SIZE(BO)
	movq	%mm4,  -7 * SIZE(BO)
	movq	%mm5,  -6 * SIZE(BO)
	movq	%mm5,  -5 * SIZE(BO)
	movq	%mm6,  -4 * SIZE(BO)
	movq	%mm6,  -3 * SIZE(BO)
	movq	%mm7,  -2 * SIZE(BO)
	movq	%mm7,  -1 * SIZE(BO)

	decq	%rax
	jne	.L42
	ALIGN_3

.L43:
	movq	K, %rax
	andq	$3, %rax
	BRANCH
	jle	.L50
	ALIGN_3

.L44:
	movq	 0 * SIZE(B), %mm0
	movq	 1 * SIZE(B), %mm1

	movq	%mm0,  0 * SIZE(BO)
	movq	%mm0,  1 * SIZE(BO)
	movq	%mm1,  2 * SIZE(BO)
	movq	%mm1,  3 * SIZE(BO)

	addq	$2 * SIZE, B
	addq	$4 * SIZE, BO
	decq	%rax
	jne	.L44
	ALIGN_3

.L50:
	movq	C, CO1			# coffset1 = c
	leaq	(C, LDC, 1), CO2	# coffset2 = c + ldc
	movq	A, AO		# aoffset = a

	movq	M,  I
	sarq	$2, I	# i = (m >> 2)
	jle	.L60
	ALIGN_3

.L51:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	BUFFER, BO
#else
	leaq	BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 4), BO
#endif

	movapd	-16 * SIZE(AO), %xmm0
	pxor	%xmm8, %xmm8
	movapd	 0 * SIZE(BO), %xmm1
	pxor	%xmm9, %xmm9
	movapd	 -8 * SIZE(AO), %xmm2
	pxor	%xmm12, %xmm12
	movapd	 8 * SIZE(BO), %xmm3
	pxor	%xmm13, %xmm13

	movapd	 0 * SIZE(AO), %xmm4
	movapd	16 * SIZE(BO), %xmm5
	movapd	 8 * SIZE(AO), %xmm6
	movapd	24 * SIZE(BO), %xmm7

	PREFETCHW      4 * SIZE(CO1)
	PREFETCHW      4 * SIZE(CO2)

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
	je	.L55
	ALIGN_3

.L52:
	mulpd	%xmm0, %xmm1
	PREFETCH	(PREFETCHSIZE     +  0) * SIZE(AO)
	mulpd	 2 * SIZE(BO), %xmm0
	addpd	%xmm1, %xmm8
	movapd	 0 * SIZE(BO), %xmm1
	addpd	%xmm0, %xmm9
	movapd	-14 * SIZE(AO), %xmm0
	mulpd	%xmm0, %xmm1
	mulpd	 2 * SIZE(BO), %xmm0
	addpd	%xmm1, %xmm12
	movapd	 4 * SIZE(BO), %xmm1
	addpd	%xmm0, %xmm13
	movapd	-12 * SIZE(AO), %xmm0

	mulpd	%xmm0, %xmm1
	mulpd	 6 * SIZE(BO), %xmm0
	addpd	%xmm1, %xmm8
	movapd	 4 * SIZE(BO), %xmm1
	addpd	%xmm0, %xmm9
	movapd	-10 * SIZE(AO), %xmm0
	mulpd	%xmm0, %xmm1
	mulpd	 6 * SIZE(BO), %xmm0
	addpd	%xmm1, %xmm12
	movapd	32 * SIZE(BO), %xmm1
	addpd	%xmm0, %xmm13
	movapd	16 * SIZE(AO), %xmm0

	PREFETCH	(PREFETCHSIZE     +  8) * SIZE(AO)
	mulpd	%xmm2, %xmm3
	mulpd	10 * SIZE(BO), %xmm2
	addpd	%xmm3, %xmm8
	movapd	 8 * SIZE(BO), %xmm3
	addpd	%xmm2, %xmm9
	movapd	-6 * SIZE(AO), %xmm2
	mulpd	%xmm2, %xmm3
	mulpd	10 * SIZE(BO), %xmm2
	addpd	%xmm3, %xmm12
	movapd	12 * SIZE(BO), %xmm3
	addpd	%xmm2, %xmm13
	movapd	-4 * SIZE(AO), %xmm2

	mulpd	%xmm2, %xmm3
	mulpd	14 * SIZE(BO), %xmm2
	addpd	%xmm3, %xmm8
	movapd	12 * SIZE(BO), %xmm3
	addpd	%xmm2, %xmm9
	movapd	-2 * SIZE(AO), %xmm2
	mulpd	%xmm2, %xmm3
	mulpd	14 * SIZE(BO), %xmm2
	addpd	%xmm3, %xmm12
	movapd	40 * SIZE(BO), %xmm3
	addpd	%xmm2, %xmm13
	movapd	24 * SIZE(AO), %xmm2

	PREFETCH	(PREFETCHSIZE     + 16) * SIZE(AO)
	mulpd	%xmm4, %xmm5
	mulpd	18 * SIZE(BO), %xmm4
	addpd	%xmm5, %xmm8
	movapd	16 * SIZE(BO), %xmm5
	addpd	%xmm4, %xmm9
	movapd	 2 * SIZE(AO), %xmm4
	mulpd	%xmm4, %xmm5
	mulpd	18 * SIZE(BO), %xmm4
	addpd	%xmm5, %xmm12
	movapd	20 * SIZE(BO), %xmm5
	addpd	%xmm4, %xmm13
	movapd	 4 * SIZE(AO), %xmm4

	mulpd	%xmm4, %xmm5
	mulpd	22 * SIZE(BO), %xmm4
	addpd	%xmm5, %xmm8
	movapd	20 * SIZE(BO), %xmm5
	addpd	%xmm4, %xmm9
	movapd	 6 * SIZE(AO), %xmm4
	mulpd	%xmm4, %xmm5
	mulpd	22 * SIZE(BO), %xmm4
	addpd	%xmm5, %xmm12
	movapd	48 * SIZE(BO), %xmm5
	addpd	%xmm4, %xmm13
	movapd	32 * SIZE(AO), %xmm4

	PREFETCH	(PREFETCHSIZE     + 24) * SIZE(AO)
	mulpd	%xmm6, %xmm7
	mulpd	26 * SIZE(BO), %xmm6
	addpd	%xmm7, %xmm8
	movapd	24 * SIZE(BO), %xmm7
	addpd	%xmm6, %xmm9
	movapd	10 * SIZE(AO), %xmm6
	mulpd	%xmm6, %xmm7
	mulpd	26 * SIZE(BO), %xmm6
	addpd	%xmm7, %xmm12
	movapd	28 * SIZE(BO), %xmm7
	addpd	%xmm6, %xmm13
	movapd	12 * SIZE(AO), %xmm6

	mulpd	%xmm6, %xmm7
	mulpd	30 * SIZE(BO), %xmm6
	addpd	%xmm7, %xmm8
	movapd	28 * SIZE(BO), %xmm7
	addpd	%xmm6, %xmm9
	movapd	14 * SIZE(AO), %xmm6
	mulpd	%xmm6, %xmm7
	mulpd	30 * SIZE(BO), %xmm6
	addpd	%xmm7, %xmm12
	movapd	56 * SIZE(BO), %xmm7
	addpd	%xmm6, %xmm13
	movapd	40 * SIZE(AO), %xmm6

	addq   $32 * SIZE, AO
	addq   $32 * SIZE, BO
	decq   %rax
	jne    .L52
	ALIGN_3

.L55:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	movapd	ALPHA, %xmm7
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L59
	ALIGN_3

.L56:
	movapd	 0 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm8
	mulpd	 2 * SIZE(BO), %xmm0
	addpd	%xmm0, %xmm9
	movapd	-14 * SIZE(AO), %xmm0
	movapd	 0 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm12
	mulpd	 2 * SIZE(BO), %xmm0
	addpd	%xmm0, %xmm13
	movapd	-12 * SIZE(AO), %xmm0

	addq	$4 * SIZE, AO		# aoffset  += 4
	addq	$4 * SIZE, BO		# boffset1 += 8
	decq	%rax
	jg	.L56
	ALIGN_3

.L59:
#ifndef TRMMKERNEL
	movsd	0 * SIZE(CO1), %xmm0
	movhpd	1 * SIZE(CO1), %xmm0
	movsd	2 * SIZE(CO1), %xmm1
	movhpd	3 * SIZE(CO1), %xmm1
	movsd	0 * SIZE(CO2), %xmm2
	movhpd	1 * SIZE(CO2), %xmm2
	movsd	2 * SIZE(CO2), %xmm3
	movhpd	3 * SIZE(CO2), %xmm3
#endif

	mulpd	%xmm7, %xmm8
	mulpd	%xmm7, %xmm9
	mulpd	%xmm7, %xmm12
	mulpd	%xmm7, %xmm13

#ifndef TRMMKERNEL
	addpd	%xmm0,  %xmm8
	addpd	%xmm1,  %xmm12
	addpd	%xmm2, %xmm9
	addpd	%xmm3, %xmm13
#endif

	movsd	%xmm8, 0 * SIZE(CO1)
	movhpd	%xmm8, 1 * SIZE(CO1)
	movsd	%xmm12, 2 * SIZE(CO1)
	movhpd	%xmm12, 3 * SIZE(CO1)
	movsd	%xmm9, 0 * SIZE(CO2)
	movhpd	%xmm9, 1 * SIZE(CO2)
	movsd	%xmm13, 2 * SIZE(CO2)
	movhpd	%xmm13, 3 * SIZE(CO2)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 4), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$4, KK
#endif

	addq	$4 * SIZE, CO1		# coffset += 4
	addq	$4 * SIZE, CO2		# coffset += 4
	decq	I			# i --
	jg	.L51
	ALIGN_3

.L60:
	testq	$2, M
	je	.L70
	ALIGN_3

.L61:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	BUFFER, BO
#else
	leaq	BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 4), BO
#endif

	movapd	-16 * SIZE(AO), %xmm0
	pxor	%xmm8, %xmm8
	movapd	 0 * SIZE(BO), %xmm1
	pxor	%xmm9, %xmm9
	movapd	 -8 * SIZE(AO), %xmm2
	pxor	%xmm10, %xmm10
	movapd	 8 * SIZE(BO), %xmm3
	pxor	%xmm11, %xmm11

	movapd	16 * SIZE(BO), %xmm5
	movapd	24 * SIZE(BO), %xmm7

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
	je	.L65
	ALIGN_3

.L62:
	mulpd	%xmm0, %xmm1
	PREFETCH	(PREFETCHSIZE     +  0) * SIZE(AO)
	mulpd	 2 * SIZE(BO), %xmm0
	addpd	%xmm1, %xmm8
	movapd	 4 * SIZE(BO), %xmm1
	addpd	%xmm0, %xmm9
	movapd	-14 * SIZE(AO), %xmm0

	mulpd	%xmm0, %xmm1
	mulpd	 6 * SIZE(BO), %xmm0
	addpd	%xmm1, %xmm10
	movapd	32 * SIZE(BO), %xmm1
	addpd	%xmm0, %xmm11
	movapd	-12 * SIZE(AO), %xmm0

	mulpd	%xmm0, %xmm3
	mulpd	10 * SIZE(BO), %xmm0
	addpd	%xmm3, %xmm8
	movapd	12 * SIZE(BO), %xmm3
	addpd	%xmm0, %xmm9
	movapd	-10 * SIZE(AO), %xmm0

	mulpd	%xmm0, %xmm3
	mulpd	14 * SIZE(BO), %xmm0
	addpd	%xmm3, %xmm10
	movapd	40 * SIZE(BO), %xmm3
	addpd	%xmm0, %xmm11
	movapd	 0 * SIZE(AO), %xmm0

	PREFETCH	(PREFETCHSIZE     +  8) * SIZE(AO)
	mulpd	%xmm2, %xmm5
	mulpd	18 * SIZE(BO), %xmm2
	addpd	%xmm5, %xmm8
	movapd	20 * SIZE(BO), %xmm5
	addpd	%xmm2, %xmm9
	movapd	-6 * SIZE(AO), %xmm2

	mulpd	%xmm2, %xmm5
	mulpd	22 * SIZE(BO), %xmm2
	addpd	%xmm5, %xmm10
	movapd	48 * SIZE(BO), %xmm5
	addpd	%xmm2, %xmm11
	movapd	-4 * SIZE(AO), %xmm2

	mulpd	%xmm2, %xmm7
	mulpd	26 * SIZE(BO), %xmm2
	addpd	%xmm7, %xmm8
	movapd	28 * SIZE(BO), %xmm7
	addpd	%xmm2, %xmm9
	movapd	-2 * SIZE(AO), %xmm2

	mulpd	%xmm2, %xmm7
	mulpd	30 * SIZE(BO), %xmm2
	addpd	%xmm7, %xmm10
	movapd	56 * SIZE(BO), %xmm7
	addpd	%xmm2, %xmm11
	movapd	 8 * SIZE(AO), %xmm2

	addq   $16 * SIZE, AO
	addq   $32 * SIZE, BO
	decq   %rax
	jne    .L62
	ALIGN_3

.L65:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	movapd	ALPHA, %xmm7
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L69
	ALIGN_3

.L66:
	mulpd	%xmm0, %xmm1
	mulpd	 2 * SIZE(BO), %xmm0
	addpd	%xmm1, %xmm8
	movapd	 4 * SIZE(BO), %xmm1
	addpd	%xmm0, %xmm9
	movapd	-14 * SIZE(AO), %xmm0

	addq	$2 * SIZE, AO		# aoffset  += 4
	addq	$4 * SIZE, BO		# boffset1 += 8
	decq	%rax
	jg	.L66
	ALIGN_3

.L69:
#ifndef TRMMKERNEL
	movsd	0 * SIZE(CO1), %xmm0
	movhpd	1 * SIZE(CO1), %xmm0
	movsd	0 * SIZE(CO2), %xmm2
	movhpd	1 * SIZE(CO2), %xmm2
#endif

	addpd	%xmm10, %xmm8
	addpd	%xmm11, %xmm9

	mulpd	%xmm7, %xmm8
	mulpd	%xmm7, %xmm9

#ifndef TRMMKERNEL
	addpd	%xmm0,  %xmm8
	addpd	%xmm2, %xmm9
#endif

	movsd	%xmm8, 0 * SIZE(CO1)
	movhpd	%xmm8, 1 * SIZE(CO1)
	movsd	%xmm9, 0 * SIZE(CO2)
	movhpd	%xmm9, 1 * SIZE(CO2)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 4), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$2, KK
#endif

	addq	$2 * SIZE, CO1		# coffset += 4
	addq	$2 * SIZE, CO2		# coffset += 4
	ALIGN_3

.L70:
	testq	$1, M
	je	.L79
	ALIGN_3

.L71:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	BUFFER, BO
#else
	leaq	BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 4), BO
#endif

	movsd	-16 * SIZE(AO), %xmm0
	pxor	%xmm8, %xmm8
	movsd	 0 * SIZE(BO), %xmm1
	pxor	%xmm9, %xmm9
	movsd	-12 * SIZE(AO), %xmm2
	pxor	%xmm10, %xmm10
	movsd	 8 * SIZE(BO), %xmm3
	pxor	%xmm11, %xmm11

	movsd	16 * SIZE(BO), %xmm5
	movsd	24 * SIZE(BO), %xmm7

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
	je	.L75
	ALIGN_3

.L72:
	mulsd	%xmm0, %xmm1
	PREFETCH	(PREFETCHSIZE     +  0) * SIZE(AO)
	mulsd	 2 * SIZE(BO), %xmm0
	addsd	%xmm1, %xmm8
	movsd	 4 * SIZE(BO), %xmm1
	addsd	%xmm0, %xmm9
	movsd	-15 * SIZE(AO), %xmm0

	mulsd	%xmm0, %xmm1
	mulsd	 6 * SIZE(BO), %xmm0
	addsd	%xmm1, %xmm10
	movsd	32 * SIZE(BO), %xmm1
	addsd	%xmm0, %xmm11
	movsd	-14 * SIZE(AO), %xmm0

	mulsd	%xmm0, %xmm3
	mulsd	10 * SIZE(BO), %xmm0
	addsd	%xmm3, %xmm8
	movsd	12 * SIZE(BO), %xmm3
	addsd	%xmm0, %xmm9
	movsd	-13 * SIZE(AO), %xmm0

	mulsd	%xmm0, %xmm3
	mulsd	14 * SIZE(BO), %xmm0
	addsd	%xmm3, %xmm10
	movsd	40 * SIZE(BO), %xmm3
	addsd	%xmm0, %xmm11
	movsd	-8 * SIZE(AO), %xmm0

	mulsd	%xmm2, %xmm5
	mulsd	18 * SIZE(BO), %xmm2
	addsd	%xmm5, %xmm8
	movsd	20 * SIZE(BO), %xmm5
	addsd	%xmm2, %xmm9
	movsd	-11 * SIZE(AO), %xmm2

	mulsd	%xmm2, %xmm5
	mulsd	22 * SIZE(BO), %xmm2
	addsd	%xmm5, %xmm10
	movsd	48 * SIZE(BO), %xmm5
	addsd	%xmm2, %xmm11
	movsd	-10 * SIZE(AO), %xmm2

	mulsd	%xmm2, %xmm7
	mulsd	26 * SIZE(BO), %xmm2
	addsd	%xmm7, %xmm8
	movsd	28 * SIZE(BO), %xmm7
	addsd	%xmm2, %xmm9
	movsd	-9 * SIZE(AO), %xmm2

	mulsd	%xmm2, %xmm7
	mulsd	30 * SIZE(BO), %xmm2
	addsd	%xmm7, %xmm10
	movsd	56 * SIZE(BO), %xmm7
	addsd	%xmm2, %xmm11
	movsd	-4 * SIZE(AO), %xmm2

	addq   $ 8 * SIZE, AO
	addq   $32 * SIZE, BO
	decq   %rax
	jne    .L72
	ALIGN_3

.L75:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	movsd	ALPHA, %xmm7
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L78
	ALIGN_3

.L76:
	mulsd	%xmm0, %xmm1
	mulsd	 2 * SIZE(BO), %xmm0
	addsd	%xmm1, %xmm8
	addsd	%xmm0, %xmm9
	movsd	-15 * SIZE(AO), %xmm0
	movsd	 4 * SIZE(BO), %xmm1

	addq	$1 * SIZE, AO		# aoffset  += 4
	addq	$4 * SIZE, BO		# boffset1 += 8
	decq	%rax
	jg	.L76
	ALIGN_3

.L78:
#ifndef TRMMKERNEL
	movsd	0 * SIZE(CO1), %xmm0
	movsd	0 * SIZE(CO2), %xmm2
#endif

	addsd	%xmm10, %xmm8
	addsd	%xmm11, %xmm9

	mulsd	%xmm7, %xmm8
	mulsd	%xmm7, %xmm9

#ifndef TRMMKERNEL
	addsd	%xmm0,  %xmm8
	addsd	%xmm2, %xmm9
#endif

	movsd	%xmm8, 0 * SIZE(CO1)
	movsd	%xmm9, 0 * SIZE(CO2)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 4), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$1, KK
#endif
	ALIGN_3

.L79:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addl	$2, KK
#endif
	leaq	(C, LDC, 2), C
	ALIGN_3

.L80:
	testq	$1, N
	je	.L999
	ALIGN_4

.L81:
/* Copying to Sub Buffer */
	leaq	BUFFER, BO

#if defined(TRMMKERNEL) && defined(LEFT)
	movq	OFFSET, %rax
	movq	%rax, KK
#endif

	movq	K, %rax
	sarq	$3, %rax
	jle	.L83
	ALIGN_3

.L82:
	PREFETCH	 56 * SIZE(B)

	movq	 0 * SIZE(B), %mm0
	movq	 1 * SIZE(B), %mm1
	movq	 2 * SIZE(B), %mm2
	movq	 3 * SIZE(B), %mm3
	movq	 4 * SIZE(B), %mm4
	movq	 5 * SIZE(B), %mm5
	movq	 6 * SIZE(B), %mm6
	movq	 7 * SIZE(B), %mm7

	addq	$ 8 * SIZE, B
	addq	$16 * SIZE, BO

	movq	%mm0, -16 * SIZE(BO)
	movq	%mm0, -15 * SIZE(BO)
	movq	%mm1, -14 * SIZE(BO)
	movq	%mm1, -13 * SIZE(BO)
	movq	%mm2, -12 * SIZE(BO)
	movq	%mm2, -11 * SIZE(BO)
	movq	%mm3, -10 * SIZE(BO)
	movq	%mm3,  -9 * SIZE(BO)
	movq	%mm4,  -8 * SIZE(BO)
	movq	%mm4,  -7 * SIZE(BO)
	movq	%mm5,  -6 * SIZE(BO)
	movq	%mm5,  -5 * SIZE(BO)
	movq	%mm6,  -4 * SIZE(BO)
	movq	%mm6,  -3 * SIZE(BO)
	movq	%mm7,  -2 * SIZE(BO)
	movq	%mm7,  -1 * SIZE(BO)

	decq	%rax
	jne	.L82
	ALIGN_3

.L83:
	movq	K, %rax
	andq	$7, %rax
	BRANCH
	jle	.L90
	ALIGN_3

.L84:
	movq	 0 * SIZE(B), %mm0

	movq	%mm0,  0 * SIZE(BO)
	movq	%mm0,  1 * SIZE(BO)

	addq	$1 * SIZE, B
	addq	$2 * SIZE, BO
	decq	%rax
	jne	.L84
	ALIGN_3

.L90:
	movq	C, CO1			# coffset1 = c
	movq	A, AO		# aoffset = a

	movq	M,  I
	sarq	$2, I	# i = (m >> 2)
	jle	.L100
	ALIGN_3

.L91:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	BUFFER, BO
#else
	leaq	BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 2), BO
#endif

	movapd	-16 * SIZE(AO), %xmm0
	pxor	%xmm8, %xmm8
	movapd	 0 * SIZE(BO), %xmm1
	pxor	%xmm9, %xmm9
	movapd	 -8 * SIZE(AO), %xmm2
	pxor	%xmm10, %xmm10
	movapd	 8 * SIZE(BO), %xmm3
	pxor	%xmm11, %xmm11

	movapd	 0 * SIZE(AO), %xmm4
	movapd	 8 * SIZE(AO), %xmm6

	PREFETCHW      4 * SIZE(CO1)

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
	je	.L95
	ALIGN_3

.L92:
	mulpd	%xmm1, %xmm0
	PREFETCH	(PREFETCHSIZE     +  0) * SIZE(AO)
	mulpd	-14 * SIZE(AO), %xmm1
	addpd	%xmm0, %xmm8
	movapd	-12 * SIZE(AO), %xmm0
	addpd	%xmm1, %xmm9
	movapd	 2 * SIZE(BO), %xmm1
	mulpd	%xmm1, %xmm0
	mulpd	-10 * SIZE(AO), %xmm1
	addpd	%xmm0, %xmm10
	movapd	 16 * SIZE(AO), %xmm0
	PREFETCH	(PREFETCHSIZE     +  8) * SIZE(AO)
	addpd	%xmm1, %xmm11
	movapd	 4 * SIZE(BO), %xmm1
	mulpd	%xmm1, %xmm2
	mulpd	-6 * SIZE(AO), %xmm1
	addpd	%xmm2, %xmm8
	movapd	-4 * SIZE(AO), %xmm2
	addpd	%xmm1, %xmm9
	movapd	 6 * SIZE(BO), %xmm1
	mulpd	%xmm1, %xmm2
	mulpd	-2 * SIZE(AO), %xmm1
	addpd	%xmm2, %xmm10
	movapd	24 * SIZE(AO), %xmm2
	PREFETCH	(PREFETCHSIZE     + 16) * SIZE(AO)
	addpd	%xmm1, %xmm11
	movapd	16 * SIZE(BO), %xmm1
	mulpd	%xmm3, %xmm4
	mulpd	 2 * SIZE(AO), %xmm3
	addpd	%xmm4, %xmm8
	movapd	 4 * SIZE(AO), %xmm4
	addpd	%xmm3, %xmm9
	movapd	10 * SIZE(BO), %xmm3
	mulpd	%xmm3, %xmm4
	mulpd	 6 * SIZE(AO), %xmm3
	addpd	%xmm4, %xmm10
	movapd	32 * SIZE(AO), %xmm4
	PREFETCH	(PREFETCHSIZE     + 24) * SIZE(AO)
	addpd	%xmm3, %xmm11
	movapd	12 * SIZE(BO), %xmm3
	mulpd	%xmm3, %xmm6
	mulpd	10 * SIZE(AO), %xmm3
	addpd	%xmm6, %xmm8
	movapd	12 * SIZE(AO), %xmm6
	addpd	%xmm3, %xmm9
	movapd	14 * SIZE(BO), %xmm3
	mulpd	%xmm3, %xmm6
	mulpd	14 * SIZE(AO), %xmm3
	addpd	%xmm6, %xmm10
	movapd	40 * SIZE(AO), %xmm6
	addpd	%xmm3, %xmm11
	movapd	24 * SIZE(BO), %xmm3

	addq   $32 * SIZE, AO
	addq   $16 * SIZE, BO
	decq   %rax
	jne    .L92
	ALIGN_3

.L95:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	movapd	ALPHA, %xmm7
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L99
	ALIGN_3

.L96:
	mulpd	%xmm1, %xmm0
	mulpd	-14 * SIZE(AO), %xmm1
	addpd	%xmm0, %xmm8
	movapd	-12 * SIZE(AO), %xmm0
	addpd	%xmm1, %xmm9
	movapd	 2 * SIZE(BO), %xmm1

	addq	$4 * SIZE, AO		# aoffset  += 4
	addq	$2 * SIZE, BO		# boffset1 += 8
	decq	%rax
	jg	.L96
	ALIGN_3

.L99:
#ifndef TRMMKERNEL
	movsd	0 * SIZE(CO1), %xmm0
	movhpd	1 * SIZE(CO1), %xmm0
	movsd	2 * SIZE(CO1), %xmm1
	movhpd	3 * SIZE(CO1), %xmm1
#endif

	addpd	%xmm10, %xmm8
	addpd	%xmm11, %xmm9

	mulpd	%xmm7, %xmm8
	mulpd	%xmm7, %xmm9

#ifndef TRMMKERNEL
	addpd	%xmm0,  %xmm8
	addpd	%xmm1,  %xmm9
#endif

	movsd	%xmm8, 0 * SIZE(CO1)
	movhpd	%xmm8, 1 * SIZE(CO1)
	movsd	%xmm9, 2 * SIZE(CO1)
	movhpd	%xmm9, 3 * SIZE(CO1)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 2), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$4, KK
#endif

	addq	$4 * SIZE, CO1		# coffset += 4
	decq	I			# i --
	jg	.L91
	ALIGN_3

.L100:
	testq	$2, M
	je	.L110
	ALIGN_3

.L101:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	BUFFER, BO
#else
	leaq	BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 2), BO
#endif

	movapd	-16 * SIZE(AO), %xmm0
	pxor	%xmm8, %xmm8
	movapd	 0 * SIZE(BO), %xmm1
	pxor	%xmm9, %xmm9
	movapd	-8 * SIZE(AO), %xmm2
	pxor	%xmm10, %xmm10
	movapd	 8 * SIZE(BO), %xmm3
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
	addq	$1, %rax
#endif
	movq	%rax, KKK
#endif
	sarq	$3, %rax
	je	.L105
	ALIGN_3

.L102:
	mulpd	%xmm0, %xmm1
	PREFETCH	(PREFETCHSIZE     +  0) * SIZE(AO)
	movapd	-14 * SIZE(AO), %xmm0
	mulpd	 2 * SIZE(BO), %xmm0
	addpd	%xmm1, %xmm8
	movapd	16 * SIZE(BO), %xmm1
	addpd	%xmm0, %xmm9
	movapd	-12 * SIZE(AO), %xmm0
	mulpd	 4 * SIZE(BO), %xmm0
	addpd	%xmm0, %xmm10
	movapd	-10 * SIZE(AO), %xmm0
	mulpd	 6 * SIZE(BO), %xmm0
	addpd	%xmm0, %xmm11
	movapd	 0 * SIZE(AO), %xmm0
	PREFETCH	(PREFETCHSIZE     +  0) * SIZE(AO)
	mulpd	%xmm2, %xmm3
	movapd	-6 * SIZE(AO), %xmm2
	mulpd	10 * SIZE(BO), %xmm2
	addpd	%xmm3, %xmm8
	movapd	24 * SIZE(BO), %xmm3
	addpd	%xmm2, %xmm9
	movapd	-4 * SIZE(AO), %xmm2
	mulpd	12 * SIZE(BO), %xmm2
	addpd	%xmm2, %xmm10
	movapd	-2 * SIZE(AO), %xmm2
	mulpd	14 * SIZE(BO), %xmm2
	addpd	%xmm2, %xmm11
	movapd	 8 * SIZE(AO), %xmm2

	addq   $16 * SIZE, AO
	addq   $16 * SIZE, BO
	decq   %rax
	jne    .L102
	ALIGN_3

.L105:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	movapd	ALPHA, %xmm7
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L109
	ALIGN_3

.L106:
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm8
	movapd	-14 * SIZE(AO), %xmm0
	movapd	  2 * SIZE(BO), %xmm1

	addq	$2 * SIZE, AO		# aoffset  += 4
	addq	$2 * SIZE, BO		# boffset1 += 8
	decq	%rax
	jg	.L106
	ALIGN_3

.L109:
	addpd	%xmm9, %xmm8
	addpd	%xmm11, %xmm10
	addpd	%xmm10, %xmm8

	mulpd	%xmm7, %xmm8

#ifndef TRMMKERNEL
	movsd	0 * SIZE(CO1), %xmm0
	movhpd	1 * SIZE(CO1), %xmm0

	addpd	%xmm0,  %xmm8
#endif

	movsd	%xmm8, 0 * SIZE(CO1)
	movhpd	%xmm8, 1 * SIZE(CO1)
	addq	$2 * SIZE, CO1		# coffset += 4

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 2), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$2, KK
#endif
	ALIGN_3

.L110:
	testq	$1, M
	je	.L999
	ALIGN_3

.L111:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	BUFFER, BO
#else
	leaq	BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 2), BO
#endif

	movsd	-16 * SIZE(AO), %xmm0
	pxor	%xmm8, %xmm8
	movsd	 0 * SIZE(BO), %xmm1
	pxor	%xmm9, %xmm9
	movsd	-12 * SIZE(AO), %xmm2
	pxor	%xmm10, %xmm10
	movsd	 8 * SIZE(BO), %xmm3
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
	je	.L115
	ALIGN_3

.L112:
	mulsd	%xmm0, %xmm1
	PREFETCH	(PREFETCHSIZE     +  0) * SIZE(AO)
	movsd	-15 * SIZE(AO), %xmm0
	addsd	%xmm1, %xmm8
	movsd	16 * SIZE(BO), %xmm1
	mulsd	 2 * SIZE(BO), %xmm0
	addsd	%xmm0, %xmm9
	movsd	-14 * SIZE(AO), %xmm0
	mulsd	 4 * SIZE(BO), %xmm0
	addsd	%xmm0, %xmm10
	movsd	-13 * SIZE(AO), %xmm0
	mulsd	 6 * SIZE(BO), %xmm0
	addsd	%xmm0, %xmm11
	movsd	 -8 * SIZE(AO), %xmm0
	mulsd	%xmm2, %xmm3
	movsd	-11 * SIZE(AO), %xmm2
	addsd	%xmm3, %xmm8
	movsd	24 * SIZE(BO), %xmm3
	mulsd	10 * SIZE(BO), %xmm2
	addsd	%xmm2, %xmm9
	movsd	-10 * SIZE(AO), %xmm2
	mulsd	12 * SIZE(BO), %xmm2
	addsd	%xmm2, %xmm10
	movsd	-9 * SIZE(AO), %xmm2
	mulsd	14 * SIZE(BO), %xmm2
	addsd	%xmm2, %xmm11
	movsd	-4 * SIZE(AO), %xmm2

	addq   $ 8 * SIZE, AO
	addq   $16 * SIZE, BO
	decq   %rax
	jne    .L112
	ALIGN_3

.L115:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	movsd	ALPHA, %xmm7
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L118
	ALIGN_3

.L116:
	mulsd	%xmm0, %xmm1
	movsd	-15 * SIZE(AO), %xmm0
	addsd	%xmm1, %xmm8
	movsd	 2 * SIZE(BO), %xmm1

	addq	$1 * SIZE, AO		# aoffset  += 4
	addq	$2 * SIZE, BO		# boffset1 += 8
	decq	%rax
	jg	.L116
	ALIGN_3

.L118:
	addsd	%xmm10, %xmm8
	addsd	%xmm11, %xmm9
	addsd	%xmm9, %xmm8

	mulsd	%xmm7, %xmm8
#ifndef TRMMKERNEL
	addsd	0 * SIZE(CO1), %xmm8
#endif
	movsd	%xmm8, 0 * SIZE(CO1)
	ALIGN_3

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
