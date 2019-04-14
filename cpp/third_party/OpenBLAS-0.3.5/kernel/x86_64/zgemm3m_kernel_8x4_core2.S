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

#define OLD_ALPHA_I	40 + STACKSIZE(%rsp)
#define OLD_A		48 + STACKSIZE(%rsp)
#define OLD_B		56 + STACKSIZE(%rsp)
#define OLD_C		64 + STACKSIZE(%rsp)
#define OLD_LDC		72 + STACKSIZE(%rsp)
#define OLD_OFFSET	80 + STACKSIZE(%rsp)

#endif

#define ALPHA	    0(%rsp)
#define J	   16(%rsp)
#define OFFSET	   24(%rsp)
#define KK	   32(%rsp)
#define KKK	   40(%rsp)
#define BUFFER	  128(%rsp)

#define PREFETCH_R    (16 * 16 + 0)
#define PREFETCH_W    (PREFETCH_R * 2)

#define PREFETCHSIZE  (16 * 21 + 8)
#define PREFETCH     prefetcht0

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
	movsd	OLD_OFFSET, %xmm12
#endif
	movaps	%xmm3, %xmm0
	movss	OLD_ALPHA_I, %xmm1
#else
	movq	OLD_LDC,   LDC
#ifdef TRMMKERNEL
	movsd	OLD_OFFSET, %xmm12
#endif
#endif

	movq	%rsp, %r15	# save old stack
	subq	$256 + LOCAL_BUFFER_SIZE, %rsp
	andq	$-4096, %rsp	# align stack

	STACK_TOUCHING

	movss	%xmm0,  0 + ALPHA
	movss	%xmm1,  4 + ALPHA
	movss	%xmm0,  8 + ALPHA
	movss	%xmm1, 12 + ALPHA

	subq	$-32 * SIZE, A
	subq	$-32 * SIZE, B

#ifdef TRMMKERNEL
	movsd	%xmm12, OFFSET
	movsd	%xmm12, KK
#ifndef LEFT
	negq	KK
#endif
#endif

	movq	OLD_M, M
	movq	OLD_N, N

	salq	$ZBASE_SHIFT, LDC

	movq	N,  J
	sarq	$2, J
	jle	.L50
	ALIGN_4

.L01:
#if defined(TRMMKERNEL) && defined(LEFT)
	movq	OFFSET, %rax
	movq	%rax, KK
#endif

/* Copying to Sub Buffer */
	leaq	32 * SIZE + BUFFER, BO

	movq	K, %rax
	sarq	$2, %rax
	jle	.L05
	ALIGN_4

.L02:
	prefetcht0	(PREFETCH_R + 0) * SIZE(B)
	movaps	 -32 * SIZE(B), %xmm3
	movaps	 -28 * SIZE(B), %xmm7
	movaps	 -24 * SIZE(B), %xmm11
	movaps	 -20 * SIZE(B), %xmm15

	prefetcht0	(PREFETCH_W +  0) * SIZE(BO)
	pshufd	$0x00, %xmm3,  %xmm0
	pshufd	$0x55, %xmm3,  %xmm1
	pshufd	$0xaa, %xmm3,  %xmm2
	pshufd	$0xff, %xmm3,  %xmm3
	pshufd	$0x00, %xmm7,  %xmm4
	pshufd	$0x55, %xmm7,  %xmm5
	pshufd	$0xaa, %xmm7,  %xmm6
	pshufd	$0xff, %xmm7,  %xmm7

	prefetcht0	(PREFETCH_W + 16) * SIZE(BO)
	pshufd	$0x00, %xmm11, %xmm8
	pshufd	$0x55, %xmm11, %xmm9
	pshufd	$0xaa, %xmm11, %xmm10
	pshufd	$0xff, %xmm11, %xmm11
	pshufd	$0x00, %xmm15, %xmm12
	pshufd	$0x55, %xmm15, %xmm13
	pshufd	$0xaa, %xmm15, %xmm14
	pshufd	$0xff, %xmm15, %xmm15

	prefetcht0	(PREFETCH_W + 32) * SIZE(BO)
	movaps	%xmm0,  -32 * SIZE(BO)
	movaps	%xmm1,  -28 * SIZE(BO)
	movaps	%xmm2,  -24 * SIZE(BO)
	movaps	%xmm3,  -20 * SIZE(BO)
	movaps	%xmm4,  -16 * SIZE(BO)
	movaps	%xmm5,  -12 * SIZE(BO)
	movaps	%xmm6,   -8 * SIZE(BO)
	movaps	%xmm7,   -4 * SIZE(BO)

	prefetcht0	(PREFETCH_W + 48) * SIZE(BO)
	movaps	%xmm8,    0 * SIZE(BO)
	movaps	%xmm9,    4 * SIZE(BO)
	movaps	%xmm10,   8 * SIZE(BO)
	movaps	%xmm11,  12 * SIZE(BO)
	movaps	%xmm12,  16 * SIZE(BO)
	movaps	%xmm13,  20 * SIZE(BO)
	movaps	%xmm14,  24 * SIZE(BO)
	movaps	%xmm15,  28 * SIZE(BO)

	subq	$-16 * SIZE, B
	subq	$-64 * SIZE, BO
	subq	$1, %rax
	jne	.L02
	ALIGN_4

.L05:
	movq	K, %rax
	andq	$3, %rax
	BRANCH
	jle	.L10
	ALIGN_4

.L06:
	movaps	 -32 * SIZE(B), %xmm3

	pshufd	$0x00, %xmm3,  %xmm0
	pshufd	$0x55, %xmm3,  %xmm1
	pshufd	$0xaa, %xmm3,  %xmm2
	pshufd	$0xff, %xmm3,  %xmm3

	movaps	%xmm0,  -32 * SIZE(BO)
	movaps	%xmm1,  -28 * SIZE(BO)
	movaps	%xmm2,  -24 * SIZE(BO)
	movaps	%xmm3,  -20 * SIZE(BO)

	addq	$ 4 * SIZE, B
	addq	$16 * SIZE, BO
	subq	$1, %rax
	jne	.L06
	ALIGN_4

.L10:
	movq	B, BB

	movq	C, CO1			# coffset1 = c
	leaq	(C, LDC, 1), CO2	# coffset2 = c + ldc
	movq	A, AO		# aoffset = a

	movq	M,  I
	sarq	$3, I
	jle	.L20
	ALIGN_4

.L11:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	40 * SIZE + BUFFER, BO
#else
	leaq	40 * SIZE + BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax,   8), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 8), BO
#endif

	pxor	%xmm8, %xmm8
	movaps	-32 * SIZE(AO), %xmm0
	pxor	%xmm9, %xmm9
	movaps	-28 * SIZE(AO), %xmm1
	pxor	%xmm10, %xmm10
	movaps	-40 * SIZE(BO), %xmm6
	pxor	%xmm11, %xmm11
	movaps	-36 * SIZE(BO), %xmm7

	prefetcht0	(PREFETCH_R +  0) * SIZE(BB)

	prefetcht0    15 * SIZE(CO1)
	pxor	%xmm12, %xmm12
	pxor	%xmm13, %xmm13
	prefetcht0    15 * SIZE(CO2)
	pxor	%xmm14, %xmm14
	pxor	%xmm15, %xmm15

	prefetcht0    15 * SIZE(CO1, LDC, 2)
	pxor	%xmm2, %xmm2
	pxor	%xmm3, %xmm3
	prefetcht0    15 * SIZE(CO2, LDC, 2)
	pxor	%xmm4, %xmm4
	pxor	%xmm5, %xmm5

	subq		$-8 * SIZE, BB

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
	sarq	$2, %rax
	jle	.L15
	ALIGN_4

.L12:
	addps	%xmm2, %xmm10
	movaps	-32 * SIZE(BO), %xmm2
	addps	%xmm3, %xmm14
	PADDING;
	movaps	%xmm6, %xmm3
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)
	mulps	%xmm0, %xmm6
	mulps	%xmm1, %xmm3

	addps	%xmm4, %xmm11
	movaps	-28 * SIZE(BO), %xmm4
	addps	%xmm5, %xmm15
	movaps	%xmm7, %xmm5
	mulps	%xmm0, %xmm7
	mulps	%xmm1, %xmm5

	addps	%xmm6, %xmm8
	movaps	-24 * SIZE(BO), %xmm6
	addps	%xmm3, %xmm12
	movaps	%xmm2, %xmm3
	mulps	%xmm0, %xmm2
	mulps	%xmm1, %xmm3

	addps	%xmm7, %xmm9
	movaps	-20 * SIZE(BO), %xmm7
	addps	%xmm5, %xmm13
	movaps	 %xmm4, %xmm5
	mulps	%xmm0, %xmm4
	movaps	-24 * SIZE(AO), %xmm0
	mulps	%xmm1, %xmm5
	movaps	-20 * SIZE(AO), %xmm1

	addps	%xmm2, %xmm10
	movaps	-16 * SIZE(BO), %xmm2
	addps	%xmm3, %xmm14
	movaps	%xmm6, %xmm3
	mulps	%xmm0, %xmm6
	mulps	%xmm1, %xmm3

	addps	%xmm4, %xmm11
	movaps  -12 * SIZE(BO), %xmm4
	addps	%xmm5, %xmm15
	movaps	%xmm7, %xmm5
	mulps	%xmm0, %xmm7
	mulps	%xmm1, %xmm5

	addps	%xmm6, %xmm8
	movaps	 -8 * SIZE(BO), %xmm6
	addps	%xmm3, %xmm12
	movaps	%xmm2, %xmm3
	mulps	%xmm0, %xmm2
	mulps	%xmm1, %xmm3

	addps	%xmm7, %xmm9
	movaps	 -4 * SIZE(BO), %xmm7
	addps	%xmm5, %xmm13
	movaps	 %xmm4, %xmm5
	mulps	%xmm0, %xmm4
	movaps	-16 * SIZE(AO), %xmm0
	mulps	%xmm1, %xmm5
	movaps	-12 * SIZE(AO), %xmm1

	addps	%xmm2, %xmm10
	movaps	  0 * SIZE(BO), %xmm2
	addps	%xmm3, %xmm14
	PADDING;
	movaps	%xmm6, %xmm3
	PREFETCH (PREFETCHSIZE + 16) * SIZE(AO)
	mulps	%xmm0, %xmm6
	mulps	%xmm1, %xmm3

	addps	%xmm4, %xmm11
	movaps	  4 * SIZE(BO), %xmm4
	addps	%xmm5, %xmm15
	movaps	%xmm7, %xmm5
	mulps	%xmm0, %xmm7
	mulps	%xmm1, %xmm5

	addps	%xmm6, %xmm8
	movaps	  8 * SIZE(BO), %xmm6
	addps	%xmm3, %xmm12
	movaps	%xmm2, %xmm3
	mulps	%xmm0, %xmm2
	mulps	%xmm1, %xmm3

	addps	%xmm7, %xmm9
	movaps	 12 * SIZE(BO), %xmm7
	addps	%xmm5, %xmm13
	movaps	 %xmm4, %xmm5
	mulps	%xmm0, %xmm4
	movaps	 -8 * SIZE(AO), %xmm0
	mulps	%xmm1, %xmm5
	movaps	 -4 * SIZE(AO), %xmm1

	addps	%xmm2, %xmm10
	movaps	 16 * SIZE(BO), %xmm2
	addps	%xmm3, %xmm14
	movaps	%xmm6, %xmm3
	mulps	%xmm0, %xmm6
	mulps	%xmm1, %xmm3

	addps	%xmm4, %xmm11
	movaps	 20 * SIZE(BO), %xmm4
	addps	%xmm5, %xmm15
	movaps	%xmm7, %xmm5
	mulps	%xmm0, %xmm7
	mulps	%xmm1, %xmm5

	addps	%xmm6, %xmm8
	movaps	 24 * SIZE(BO), %xmm6
	addps	%xmm3, %xmm12
	movaps	%xmm2, %xmm3
	mulps	%xmm0, %xmm2
	mulps	%xmm1, %xmm3
	subq	$-32 * SIZE, AO

	addps	%xmm7, %xmm9
	movaps	 28 * SIZE(BO), %xmm7
	addps	%xmm5, %xmm13
	movaps	 %xmm4, %xmm5
	mulps	%xmm0, %xmm4
	movaps	-32 * SIZE(AO), %xmm0
	mulps	%xmm1, %xmm5
	movaps	-28 * SIZE(AO), %xmm1

	subq	$-64 * SIZE, BO
	subq	$1, %rax
	BRANCH
	jg	.L12
	ALIGN_4

.L15:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	BRANCH
	je .L18
	ALIGN_4

.L16:
	addps	%xmm2, %xmm10
	movaps	-32 * SIZE(BO), %xmm2
	addps	%xmm3, %xmm14
	movaps	%xmm6, %xmm3
	mulps	%xmm0, %xmm6
	mulps	%xmm1, %xmm3

	addps	%xmm4, %xmm11
	movaps	-28 * SIZE(BO), %xmm4
	addps	%xmm5, %xmm15
	movaps	%xmm7, %xmm5
	mulps	%xmm0, %xmm7
	mulps	%xmm1, %xmm5

	addps	%xmm6, %xmm8
	movaps	-24 * SIZE(BO), %xmm6
	addps	%xmm3, %xmm12
	addq	$8  * SIZE, AO
	movaps	%xmm2, %xmm3
	mulps	%xmm0, %xmm2
	mulps	%xmm1, %xmm3

	addps	%xmm7, %xmm9
	movaps	-20 * SIZE(BO), %xmm7
	addps	%xmm5, %xmm13
	addq	$16 * SIZE, BO
	movaps	 %xmm4, %xmm5
	mulps	%xmm0, %xmm4
	movaps	-32 * SIZE(AO), %xmm0
	mulps	%xmm1, %xmm5
	movaps	-28 * SIZE(AO), %xmm1

	subq	$1, %rax
	BRANCH
	jg	.L16
	ALIGN_4

.L18:
	movaps	ALPHA, %xmm7

	addps	%xmm2, %xmm10
	addps	%xmm3, %xmm14
	addps	%xmm4, %xmm11
	addps	%xmm5, %xmm15

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

	addq	$16 * SIZE, CO1
	addq	$16 * SIZE, CO2
	subq	$1, I
	jg	.L11
	ALIGN_4

.L20:
	testq	$4, M
	jle	.L30

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
	addq	$4, %rax
#else
	addq	$4, %rax
#endif
	movq	%rax, KKK
#endif
	sarq	$2, %rax
	jle	.L25
	ALIGN_4

.L21:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)

	movaps	-32 * SIZE(AO), %xmm0
	movaps	-32 * SIZE(BO), %xmm2
	movaps	-28 * SIZE(BO), %xmm3
	movaps	-24 * SIZE(BO), %xmm4
	movaps	-20 * SIZE(BO), %xmm5

	mulps	%xmm0, %xmm2
	mulps	%xmm0, %xmm3
	mulps	%xmm0, %xmm4
	mulps	%xmm0, %xmm5

	addps	%xmm2, %xmm8
	addps	%xmm3, %xmm9
	addps	%xmm4, %xmm10
	addps	%xmm5, %xmm11

	movaps	-28 * SIZE(AO), %xmm0
	movaps	-16 * SIZE(BO), %xmm2
	movaps	-12 * SIZE(BO), %xmm3
	movaps	 -8 * SIZE(BO), %xmm4
	movaps	 -4 * SIZE(BO), %xmm5

	mulps	%xmm0, %xmm2
	mulps	%xmm0, %xmm3
	mulps	%xmm0, %xmm4
	mulps	%xmm0, %xmm5

	addps	%xmm2, %xmm8
	addps	%xmm3, %xmm9
	addps	%xmm4, %xmm10
	addps	%xmm5, %xmm11

	movaps	-24 * SIZE(AO), %xmm0
	movaps	  0 * SIZE(BO), %xmm2
	movaps	  4 * SIZE(BO), %xmm3
	movaps	  8 * SIZE(BO), %xmm4
	movaps	 12 * SIZE(BO), %xmm5

	mulps	%xmm0, %xmm2
	mulps	%xmm0, %xmm3
	mulps	%xmm0, %xmm4
	mulps	%xmm0, %xmm5

	addps	%xmm2, %xmm8
	addps	%xmm3, %xmm9
	addps	%xmm4, %xmm10
	addps	%xmm5, %xmm11

	movaps	-20 * SIZE(AO), %xmm0
	movaps	 16 * SIZE(BO), %xmm2
	movaps	 20 * SIZE(BO), %xmm3
	movaps	 24 * SIZE(BO), %xmm4
	movaps	 28 * SIZE(BO), %xmm5

	mulps	%xmm0, %xmm2
	mulps	%xmm0, %xmm3
	mulps	%xmm0, %xmm4
	mulps	%xmm0, %xmm5

	addps	%xmm2, %xmm8
	addps	%xmm3, %xmm9
	addps	%xmm4, %xmm10
	addps	%xmm5, %xmm11

	subq	$-16 * SIZE, AO
	subq	$-64 * SIZE, BO
	subq	$1, %rax
	jg	.L21
	ALIGN_4

.L25:
	movaps	ALPHA, %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	je .L28
	ALIGN_4

.L26:
	movaps	-32 * SIZE(AO), %xmm0
	movaps	-32 * SIZE(BO), %xmm2
	movaps	-28 * SIZE(BO), %xmm3
	movaps	-24 * SIZE(BO), %xmm4
	movaps	-20 * SIZE(BO), %xmm5

	mulps	%xmm0, %xmm2
	mulps	%xmm0, %xmm3
	mulps	%xmm0, %xmm4
	mulps	%xmm0, %xmm5

	addps	%xmm2, %xmm8
	addps	%xmm3, %xmm9
	addps	%xmm4, %xmm10
	addps	%xmm5, %xmm11

	addq	$ 4 * SIZE, AO
	addq	$16 * SIZE, BO
	subq	$1, %rax
	jg	.L26
	ALIGN_4

.L28:
	movsd	 0 * SIZE(CO1), %xmm0
	movhps	 2 * SIZE(CO1), %xmm0
	movsd	 4 * SIZE(CO1), %xmm1
	movhps	 6 * SIZE(CO1), %xmm1

	pshufd	$0x50, %xmm8,  %xmm4
	pshufd	$0xfa, %xmm8,  %xmm8

	mulps	%xmm7, %xmm4
	mulps	%xmm7, %xmm8
	addps	%xmm0, %xmm4
	addps	%xmm1, %xmm8

	movlps	%xmm4,   0 * SIZE(CO1)
	movhps	%xmm4,   2 * SIZE(CO1)
	movlps	%xmm8,   4 * SIZE(CO1)
	movhps	%xmm8,   6 * SIZE(CO1)

	movsd	 0 * SIZE(CO2), %xmm0
	movhps	 2 * SIZE(CO2), %xmm0
	movsd	 4 * SIZE(CO2), %xmm1
	movhps	 6 * SIZE(CO2), %xmm1

	pshufd	$0x50, %xmm9,  %xmm4
	pshufd	$0xfa, %xmm9,  %xmm9

	mulps	%xmm7, %xmm4
	mulps	%xmm7, %xmm9
	addps	%xmm0, %xmm4
	addps	%xmm1, %xmm9

	movlps	%xmm4,   0 * SIZE(CO2)
	movhps	%xmm4,   2 * SIZE(CO2)
	movlps	%xmm9,   4 * SIZE(CO2)
	movhps	%xmm9,   6 * SIZE(CO2)

	movsd	 0 * SIZE(CO1, LDC, 2), %xmm0
	movhps	 2 * SIZE(CO1, LDC, 2), %xmm0
	movsd	 4 * SIZE(CO1, LDC, 2), %xmm1
	movhps	 6 * SIZE(CO1, LDC, 2), %xmm1

	pshufd	$0x50, %xmm10,  %xmm4
	pshufd	$0xfa, %xmm10,  %xmm10

	mulps	%xmm7, %xmm4
	mulps	%xmm7, %xmm10
	addps	%xmm0, %xmm4
	addps	%xmm1, %xmm10

	movlps	%xmm4,   0 * SIZE(CO1, LDC, 2)
	movhps	%xmm4,   2 * SIZE(CO1, LDC, 2)
	movlps	%xmm10,  4 * SIZE(CO1, LDC, 2)
	movhps	%xmm10,  6 * SIZE(CO1, LDC, 2)

	movsd	 0 * SIZE(CO2, LDC, 2), %xmm0
	movhps	 2 * SIZE(CO2, LDC, 2), %xmm0
	movsd	 4 * SIZE(CO2, LDC, 2), %xmm1
	movhps	 6 * SIZE(CO2, LDC, 2), %xmm1

	pshufd	$0x50, %xmm11, %xmm4
	pshufd	$0xfa, %xmm11, %xmm11

	mulps	%xmm7, %xmm4
	mulps	%xmm7, %xmm11
	addps	%xmm0, %xmm4
	addps	%xmm1, %xmm11

	movlps	%xmm4,   0 * SIZE(CO2, LDC, 2)
	movhps	%xmm4,   2 * SIZE(CO2, LDC, 2)
	movlps	%xmm11,  4 * SIZE(CO2, LDC, 2)
	movhps	%xmm11,  6 * SIZE(CO2, LDC, 2)

	addq	$8 * SIZE, CO1
	addq	$8 * SIZE, CO2
	ALIGN_4

.L30:
	testq	$2, M
	jle	.L40

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

	pxor	%xmm8, %xmm8
	pxor	%xmm9, %xmm9
	pxor	%xmm10, %xmm10
	pxor	%xmm11, %xmm11

	pxor	%xmm12, %xmm12
	pxor	%xmm13, %xmm13
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
	addq	$4, %rax
#endif
	movq	%rax, KKK
#endif
	sarq	$2, %rax
	jle	.L35
	ALIGN_4

.L31:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)

	movsd	-32 * SIZE(AO), %xmm0
	movsd	-32 * SIZE(BO), %xmm2
	movsd	-28 * SIZE(BO), %xmm3
	movsd	-24 * SIZE(BO), %xmm4
	movsd	-20 * SIZE(BO), %xmm5

	mulps	%xmm0, %xmm2
	mulps	%xmm0, %xmm3
	mulps	%xmm0, %xmm4
	mulps	%xmm0, %xmm5

	addps	%xmm2, %xmm8
	addps	%xmm3, %xmm9
	addps	%xmm4, %xmm10
	addps	%xmm5, %xmm11

	movsd	-30 * SIZE(AO), %xmm0
	movsd	-16 * SIZE(BO), %xmm2
	movsd	-12 * SIZE(BO), %xmm3
	movsd	 -8 * SIZE(BO), %xmm4
	movsd	 -4 * SIZE(BO), %xmm5

	mulps	%xmm0, %xmm2
	mulps	%xmm0, %xmm3
	mulps	%xmm0, %xmm4
	mulps	%xmm0, %xmm5

	addps	%xmm2, %xmm8
	addps	%xmm3, %xmm9
	addps	%xmm4, %xmm10
	addps	%xmm5, %xmm11

	movsd	-28 * SIZE(AO), %xmm0
	movsd	  0 * SIZE(BO), %xmm2
	movsd	  4 * SIZE(BO), %xmm3
	movsd	  8 * SIZE(BO), %xmm4
	movsd	 12 * SIZE(BO), %xmm5

	mulps	%xmm0, %xmm2
	mulps	%xmm0, %xmm3
	mulps	%xmm0, %xmm4
	mulps	%xmm0, %xmm5

	addps	%xmm2, %xmm8
	addps	%xmm3, %xmm9
	addps	%xmm4, %xmm10
	addps	%xmm5, %xmm11

	movsd	-26 * SIZE(AO), %xmm0
	movsd	 16 * SIZE(BO), %xmm2
	movsd	 20 * SIZE(BO), %xmm3
	movsd	 24 * SIZE(BO), %xmm4
	movsd	 28 * SIZE(BO), %xmm5

	mulps	%xmm0, %xmm2
	mulps	%xmm0, %xmm3
	mulps	%xmm0, %xmm4
	mulps	%xmm0, %xmm5

	addps	%xmm2, %xmm8
	addps	%xmm3, %xmm9
	addps	%xmm4, %xmm10
	addps	%xmm5, %xmm11

	subq	$ -8 * SIZE, AO
	subq	$-64 * SIZE, BO
	subq	$1, %rax
	jg	.L31
	ALIGN_4

.L35:
	movaps	ALPHA, %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	je .L38
	ALIGN_4

.L36:
	movsd	-32 * SIZE(AO), %xmm0
	movsd	-32 * SIZE(BO), %xmm2
	movsd	-28 * SIZE(BO), %xmm3
	movsd	-24 * SIZE(BO), %xmm4
	movsd	-20 * SIZE(BO), %xmm5

	mulps	%xmm0, %xmm2
	mulps	%xmm0, %xmm3
	mulps	%xmm0, %xmm4
	mulps	%xmm0, %xmm5

	addps	%xmm2, %xmm8
	addps	%xmm3, %xmm9
	addps	%xmm4, %xmm10
	addps	%xmm5, %xmm11

	addq	$ 2 * SIZE, AO
	addq	$16 * SIZE, BO
	subq	$1, %rax
	jg	.L36
	ALIGN_4

.L38:
	movsd	 0 * SIZE(CO1), %xmm0
	movhps	 2 * SIZE(CO1), %xmm0

	pshufd	$0x50, %xmm8,  %xmm4
	mulps	%xmm7, %xmm4
	addps	%xmm0, %xmm4

	movlps	%xmm4,   0 * SIZE(CO1)
	movhps	%xmm4,   2 * SIZE(CO1)

	movsd	 0 * SIZE(CO2), %xmm0
	movhps	 2 * SIZE(CO2), %xmm0

	pshufd	$0x50, %xmm9,  %xmm4
	mulps	%xmm7, %xmm4
	addps	%xmm0, %xmm4

	movlps	%xmm4,   0 * SIZE(CO2)
	movhps	%xmm4,   2 * SIZE(CO2)

	movsd	 0 * SIZE(CO1, LDC, 2), %xmm0
	movhps	 2 * SIZE(CO1, LDC, 2), %xmm0

	pshufd	$0x50, %xmm10,  %xmm4
	mulps	%xmm7, %xmm4
	addps	%xmm0, %xmm4

	movlps	%xmm4,   0 * SIZE(CO1, LDC, 2)
	movhps	%xmm4,   2 * SIZE(CO1, LDC, 2)

	movsd	 0 * SIZE(CO2, LDC, 2), %xmm0
	movhps	 2 * SIZE(CO2, LDC, 2), %xmm0

	pshufd	$0x50, %xmm11, %xmm4
	mulps	%xmm7, %xmm4
	addps	%xmm0, %xmm4

	movlps	%xmm4,   0 * SIZE(CO2, LDC, 2)
	movhps	%xmm4,   2 * SIZE(CO2, LDC, 2)

	addq	$4 * SIZE, CO1
	addq	$4 * SIZE, CO2
	ALIGN_4

.L40:
	testq	$1, M
	jle	.L49

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	32 * SIZE + BUFFER, BO
#else
	leaq	32 * SIZE + BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax,   4), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 8), BO
	leaq	(BO, %rax, 8), BO
#endif

	pxor	%xmm8, %xmm8
	pxor	%xmm9, %xmm9
	pxor	%xmm10, %xmm10
	pxor	%xmm11, %xmm11

	pxor	%xmm12, %xmm12
	pxor	%xmm13, %xmm13
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
	addq	$1, %rax
#else
	addq	$4, %rax
#endif
	movq	%rax, KKK
#endif
	sarq	$2, %rax
	jle	.L45
	ALIGN_4

.L41:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)

	movss	-32 * SIZE(AO), %xmm0
	movss	-32 * SIZE(BO), %xmm2
	movss	-28 * SIZE(BO), %xmm3
	movss	-24 * SIZE(BO), %xmm4
	movss	-20 * SIZE(BO), %xmm5

	mulss	%xmm0, %xmm2
	mulss	%xmm0, %xmm3
	mulss	%xmm0, %xmm4
	mulss	%xmm0, %xmm5

	addss	%xmm2, %xmm8
	addss	%xmm3, %xmm9
	addss	%xmm4, %xmm10
	addss	%xmm5, %xmm11

	movss	-31 * SIZE(AO), %xmm0
	movss	-16 * SIZE(BO), %xmm2
	movss	-12 * SIZE(BO), %xmm3
	movss	 -8 * SIZE(BO), %xmm4
	movss	 -4 * SIZE(BO), %xmm5

	mulss	%xmm0, %xmm2
	mulss	%xmm0, %xmm3
	mulss	%xmm0, %xmm4
	mulss	%xmm0, %xmm5

	addss	%xmm2, %xmm8
	addss	%xmm3, %xmm9
	addss	%xmm4, %xmm10
	addss	%xmm5, %xmm11

	movss	-30 * SIZE(AO), %xmm0
	movss	  0 * SIZE(BO), %xmm2
	movss	  4 * SIZE(BO), %xmm3
	movss	  8 * SIZE(BO), %xmm4
	movss	 12 * SIZE(BO), %xmm5

	mulss	%xmm0, %xmm2
	mulss	%xmm0, %xmm3
	mulss	%xmm0, %xmm4
	mulss	%xmm0, %xmm5

	addss	%xmm2, %xmm8
	addss	%xmm3, %xmm9
	addss	%xmm4, %xmm10
	addss	%xmm5, %xmm11

	movss	-29 * SIZE(AO), %xmm0
	movss	 16 * SIZE(BO), %xmm2
	movss	 20 * SIZE(BO), %xmm3
	movss	 24 * SIZE(BO), %xmm4
	movss	 28 * SIZE(BO), %xmm5

	mulss	%xmm0, %xmm2
	mulss	%xmm0, %xmm3
	mulss	%xmm0, %xmm4
	mulss	%xmm0, %xmm5

	addss	%xmm2, %xmm8
	addss	%xmm3, %xmm9
	addss	%xmm4, %xmm10
	addss	%xmm5, %xmm11

	subq	$ -4 * SIZE, AO
	subq	$-64 * SIZE, BO
	subq	$1, %rax
	jg	.L41
	ALIGN_4

.L45:
	movaps	ALPHA, %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	je .L48
	ALIGN_4

.L46:
	movss	-32 * SIZE(AO), %xmm0
	movss	-32 * SIZE(BO), %xmm2
	movss	-28 * SIZE(BO), %xmm3
	movss	-24 * SIZE(BO), %xmm4
	movss	-20 * SIZE(BO), %xmm5

	mulss	%xmm0, %xmm2
	mulss	%xmm0, %xmm3
	mulss	%xmm0, %xmm4
	mulss	%xmm0, %xmm5

	addss	%xmm2, %xmm8
	addss	%xmm3, %xmm9
	addss	%xmm4, %xmm10
	addss	%xmm5, %xmm11

	addq	$ 1 * SIZE, AO
	addq	$16 * SIZE, BO
	subq	$1, %rax
	jg	.L46
	ALIGN_4

.L48:
	movsd	 0 * SIZE(CO1), %xmm0

	pshufd	$0x50, %xmm8,  %xmm4
	mulps	%xmm7, %xmm4
	addps	%xmm0, %xmm4

	movlps	%xmm4,   0 * SIZE(CO1)

	movsd	 0 * SIZE(CO2), %xmm0

	pshufd	$0x50, %xmm9,  %xmm4
	mulps	%xmm7, %xmm4
	addps	%xmm0, %xmm4

	movlps	%xmm4,   0 * SIZE(CO2)

	movsd	 0 * SIZE(CO1, LDC, 2), %xmm0

	pshufd	$0x50, %xmm10,  %xmm4
	mulps	%xmm7, %xmm4
	addps	%xmm0, %xmm4

	movlps	%xmm4,   0 * SIZE(CO1, LDC, 2)

	movsd	 0 * SIZE(CO2, LDC, 2), %xmm0

	pshufd	$0x50, %xmm11, %xmm4
	mulps	%xmm7, %xmm4
	addps	%xmm0, %xmm4

	movlps	%xmm4,   0 * SIZE(CO2, LDC, 2)
	ALIGN_4

.L49:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addl	$4, KK
#endif

	leaq	(C, LDC, 4), C
	subq	$1, J
	jg	.L01
	ALIGN_4

.L50:
	testq	$2, N
	jle	.L100
	ALIGN_4

.L51:
/* Copying to Sub Buffer */
	leaq	BUFFER, BO

#if defined(TRMMKERNEL) && defined(LEFT)
	movq	OFFSET, %rax
	movq	%rax, KK
#endif

	movq	K, %rax
	sarq	$3, %rax
	jle	.L53

	addq	%rax, %rax
	ALIGN_4

.L52:
	movaps	 -32 * SIZE(B), %xmm3
	movaps	 -28 * SIZE(B), %xmm7

	pshufd	$0x00, %xmm3,  %xmm0
	pshufd	$0x55, %xmm3,  %xmm1
	pshufd	$0xaa, %xmm3,  %xmm2
	pshufd	$0xff, %xmm3,  %xmm3
	prefetcht0	(PREFETCH_W +  0) * SIZE(BO)

	pshufd	$0x00, %xmm7,  %xmm4
	pshufd	$0x55, %xmm7,  %xmm5
	pshufd	$0xaa, %xmm7,  %xmm6
	pshufd	$0xff, %xmm7,  %xmm7

	prefetcht0	(PREFETCH_W + 16) * SIZE(BO)

	movaps	%xmm0,    0 * SIZE(BO)
	movaps	%xmm1,    4 * SIZE(BO)
	movaps	%xmm2,    8 * SIZE(BO)
	movaps	%xmm3,   12 * SIZE(BO)
	movaps	%xmm4,   16 * SIZE(BO)
	movaps	%xmm5,   20 * SIZE(BO)
	movaps	%xmm6,   24 * SIZE(BO)
	movaps	%xmm7,   28 * SIZE(BO)

	addq	$ 8 * SIZE, B
	addq	$32 * SIZE, BO

	subq	$1, %rax
	jne	.L52
	ALIGN_4

.L53:
	movq	K, %rax
	andq	$7, %rax
	BRANCH
	jle	.L55
	ALIGN_4

.L54:
	movss	 -32 * SIZE(B), %xmm8
	movss	 -31 * SIZE(B), %xmm9

	shufps	$0, %xmm8,  %xmm8
	shufps	$0, %xmm9,  %xmm9

	movaps	%xmm8,  0 * SIZE(BO)
	movaps	%xmm9,  4 * SIZE(BO)

	addq	$2 * SIZE, B
	addq	$8 * SIZE, BO
	subq	$1, %rax
	jne	.L54
	ALIGN_4

.L55:
	movq	C, CO1
	leaq	(C, LDC, 1), CO2
	movq	A, AO		# aoffset = a

	movq	M,  I
	sarq	$3, I
	jle	.L70
	ALIGN_4

.L60:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	32 * SIZE + BUFFER, BO
#else
	leaq	32 * SIZE + BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax,   8), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 4), BO
#endif

	pxor	%xmm8, %xmm8
	pxor	%xmm9, %xmm9

	prefetcht0    15 * SIZE(CO1)
	pxor	%xmm12, %xmm12
	prefetcht0    15 * SIZE(CO2)
	pxor	%xmm13, %xmm13

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
	sarq	$2, %rax
	jle	.L65
	ALIGN_4

.L61:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)

	movaps	-32 * SIZE(AO), %xmm0
	movaps	-28 * SIZE(AO), %xmm1

	movaps	-32 * SIZE(BO), %xmm2
	movaps	 %xmm2, %xmm3
	movaps	-28 * SIZE(BO), %xmm4
	movaps	 %xmm4, %xmm5

	mulps	%xmm0, %xmm2
	mulps	%xmm1, %xmm3
	mulps	%xmm0, %xmm4
	mulps	%xmm1, %xmm5

	addps	%xmm2, %xmm8
	addps	%xmm3, %xmm12
	addps	%xmm4, %xmm9
	addps	%xmm5, %xmm13

	movaps	-24 * SIZE(AO), %xmm0
	movaps	-20 * SIZE(AO), %xmm1

	movaps	-24 * SIZE(BO), %xmm2
	movaps	 %xmm2, %xmm3
	movaps	-20 * SIZE(BO), %xmm4
	movaps	 %xmm4, %xmm5

	mulps	%xmm0, %xmm2
	mulps	%xmm1, %xmm3
	mulps	%xmm0, %xmm4
	mulps	%xmm1, %xmm5

	addps	%xmm2, %xmm8
	addps	%xmm3, %xmm12
	addps	%xmm4, %xmm9
	addps	%xmm5, %xmm13

	movaps	-16 * SIZE(AO), %xmm0
	movaps	-12 * SIZE(AO), %xmm1

	movaps	-16 * SIZE(BO), %xmm2
	movaps	 %xmm2, %xmm3
	movaps	-12 * SIZE(BO), %xmm4
	movaps	 %xmm4, %xmm5

	mulps	%xmm0, %xmm2
	mulps	%xmm1, %xmm3
	mulps	%xmm0, %xmm4
	mulps	%xmm1, %xmm5

	addps	%xmm2, %xmm8
	addps	%xmm3, %xmm12
	addps	%xmm4, %xmm9
	addps	%xmm5, %xmm13

	movaps	 -8 * SIZE(AO), %xmm0
	movaps	 -4 * SIZE(AO), %xmm1

	movaps	 -8 * SIZE(BO), %xmm2
	movaps	 %xmm2, %xmm3
	movaps	 -4 * SIZE(BO), %xmm4
	movaps	 %xmm4, %xmm5

	mulps	%xmm0, %xmm2
	mulps	%xmm1, %xmm3
	mulps	%xmm0, %xmm4
	mulps	%xmm1, %xmm5

	addps	%xmm2, %xmm8
	addps	%xmm3, %xmm12
	addps	%xmm4, %xmm9
	addps	%xmm5, %xmm13

	subq	$-32 * SIZE, AO
	subq	$-32 * SIZE, BO
	subq	$1, %rax
	jg	.L61
	ALIGN_4

.L65:
	movaps	ALPHA, %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	je .L68
	ALIGN_4

.L66:
	movaps	-32 * SIZE(AO), %xmm0
	movaps	-28 * SIZE(AO), %xmm1

	movaps	-32 * SIZE(BO), %xmm2
	movaps	 %xmm2, %xmm3
	movaps	-28 * SIZE(BO), %xmm4
	movaps	 %xmm4, %xmm5

	mulps	%xmm0, %xmm2
	mulps	%xmm1, %xmm3
	mulps	%xmm0, %xmm4
	mulps	%xmm1, %xmm5

	addps	%xmm2, %xmm8
	addps	%xmm3, %xmm12
	addps	%xmm4, %xmm9
	addps	%xmm5, %xmm13

	addq	$8 * SIZE, AO
	addq	$8 * SIZE, BO
	subq	$1, %rax
	jg	.L66
	ALIGN_4

.L68:
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

	addq	$16 * SIZE, CO1
	addq	$16 * SIZE, CO2
	subq	$1, I
	jg	.L60
	ALIGN_4

.L70:
	testq	$4, M
	jle	.L80

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	32 * SIZE + BUFFER, BO
#else
	leaq	32 * SIZE + BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax,   8), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 4), BO
#endif

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
	addq	$4, %rax
#else
	addq	$2, %rax
#endif
	movq	%rax, KKK
#endif
	sarq	$2, %rax
	jle	.L75
	ALIGN_4

.L71:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)

	movaps	-32 * SIZE(AO), %xmm0
	movaps	-28 * SIZE(AO), %xmm1
	movaps	-32 * SIZE(BO), %xmm2
	movaps	-28 * SIZE(BO), %xmm3
	movaps	-24 * SIZE(BO), %xmm4
	movaps	-20 * SIZE(BO), %xmm5

	mulps	%xmm0, %xmm2
	mulps	%xmm0, %xmm3
	mulps	%xmm1, %xmm4
	mulps	%xmm1, %xmm5

	addps	%xmm2, %xmm8
	addps	%xmm3, %xmm9
	addps	%xmm4, %xmm10
	addps	%xmm5, %xmm11

	movaps	-24 * SIZE(AO), %xmm0
	movaps	-20 * SIZE(AO), %xmm1
	movaps	-16 * SIZE(BO), %xmm2
	movaps	-12 * SIZE(BO), %xmm3
	movaps	 -8 * SIZE(BO), %xmm4
	movaps	 -4 * SIZE(BO), %xmm5

	mulps	%xmm0, %xmm2
	mulps	%xmm0, %xmm3
	mulps	%xmm1, %xmm4
	mulps	%xmm1, %xmm5

	addps	%xmm2, %xmm8
	addps	%xmm3, %xmm9
	addps	%xmm4, %xmm10
	addps	%xmm5, %xmm11

	subq	$-16 * SIZE, AO
	subq	$-32 * SIZE, BO
	subq	$1, %rax
	jg	.L71
	ALIGN_4

.L75:
	movaps	ALPHA, %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	je .L78
	ALIGN_4

.L76:
	movaps	-32 * SIZE(AO), %xmm0
	movaps	-32 * SIZE(BO), %xmm2
	movaps	-28 * SIZE(BO), %xmm3

	mulps	%xmm0, %xmm2
	mulps	%xmm0, %xmm3

	addps	%xmm2, %xmm8
	addps	%xmm3, %xmm9

	addq	$4 * SIZE, AO
	addq	$8 * SIZE, BO
	subq	$1, %rax
	jg	.L76
	ALIGN_4

.L78:
	addps	%xmm10, %xmm8
	addps	%xmm11, %xmm9

	movsd	 0 * SIZE(CO1), %xmm0
	movhps	 2 * SIZE(CO1), %xmm0
	movsd	 4 * SIZE(CO1), %xmm1
	movhps	 6 * SIZE(CO1), %xmm1

	pshufd	$0x50, %xmm8,  %xmm4
	pshufd	$0xfa, %xmm8,  %xmm8

	mulps	%xmm7, %xmm4
	mulps	%xmm7, %xmm8
	addps	%xmm0, %xmm4
	addps	%xmm1, %xmm8

	movlps	%xmm4,   0 * SIZE(CO1)
	movhps	%xmm4,   2 * SIZE(CO1)
	movlps	%xmm8,   4 * SIZE(CO1)
	movhps	%xmm8,   6 * SIZE(CO1)

	movsd	 0 * SIZE(CO2), %xmm0
	movhps	 2 * SIZE(CO2), %xmm0
	movsd	 4 * SIZE(CO2), %xmm1
	movhps	 6 * SIZE(CO2), %xmm1

	pshufd	$0x50, %xmm9,  %xmm4
	pshufd	$0xfa, %xmm9,  %xmm9

	mulps	%xmm7, %xmm4
	mulps	%xmm7, %xmm9
	addps	%xmm0, %xmm4
	addps	%xmm1, %xmm9

	movlps	%xmm4,   0 * SIZE(CO2)
	movhps	%xmm4,   2 * SIZE(CO2)
	movlps	%xmm9,   4 * SIZE(CO2)
	movhps	%xmm9,   6 * SIZE(CO2)

	addq	$8 * SIZE, CO1
	addq	$8 * SIZE, CO2
	ALIGN_4

.L80:
	testq	$2, M
	jle	.L90

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	32 * SIZE + BUFFER, BO
#else
	leaq	32 * SIZE + BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax,   8), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 4), BO
#endif

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
	sarq	$2, %rax
	jle	.L85
	ALIGN_4

.L81:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)

	movsd	-32 * SIZE(AO), %xmm0
	movsd	-30 * SIZE(AO), %xmm1
	movsd	-32 * SIZE(BO), %xmm2
	movsd	-28 * SIZE(BO), %xmm3
	movsd	-24 * SIZE(BO), %xmm4
	movsd	-20 * SIZE(BO), %xmm5

	mulps	%xmm0, %xmm2
	mulps	%xmm0, %xmm3
	mulps	%xmm1, %xmm4
	mulps	%xmm1, %xmm5

	addps	%xmm2, %xmm8
	addps	%xmm3, %xmm9
	addps	%xmm4, %xmm10
	addps	%xmm5, %xmm11

	movsd	-28 * SIZE(AO), %xmm0
	movsd	-26 * SIZE(AO), %xmm1
	movsd	-16 * SIZE(BO), %xmm2
	movsd	-12 * SIZE(BO), %xmm3
	movsd	 -8 * SIZE(BO), %xmm4
	movsd	 -4 * SIZE(BO), %xmm5

	mulps	%xmm0, %xmm2
	mulps	%xmm0, %xmm3
	mulps	%xmm1, %xmm4
	mulps	%xmm1, %xmm5

	addps	%xmm2, %xmm8
	addps	%xmm3, %xmm9
	addps	%xmm4, %xmm10
	addps	%xmm5, %xmm11

	subq	$ -8 * SIZE, AO
	subq	$-32 * SIZE, BO
	subq	$1, %rax
	jg	.L81
	ALIGN_4

.L85:
	movaps	ALPHA, %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	je .L88
	ALIGN_4

.L86:
	movsd	-32 * SIZE(AO), %xmm0
	movsd	-32 * SIZE(BO), %xmm2
	movsd	-28 * SIZE(BO), %xmm3

	mulps	%xmm0, %xmm2
	mulps	%xmm0, %xmm3

	addps	%xmm2, %xmm8
	addps	%xmm3, %xmm9

	addq	$2 * SIZE, AO
	addq	$8 * SIZE, BO
	subq	$1, %rax
	jg	.L86
	ALIGN_4

.L88:
	addps	%xmm10, %xmm8
	addps	%xmm11, %xmm9

	movsd	 0 * SIZE(CO1), %xmm0
	movhps	 2 * SIZE(CO1), %xmm0

	pshufd	$0x50, %xmm8,  %xmm4

	mulps	%xmm7, %xmm4
	addps	%xmm0, %xmm4

	movlps	%xmm4,   0 * SIZE(CO1)
	movhps	%xmm4,   2 * SIZE(CO1)

	movsd	 0 * SIZE(CO2), %xmm0
	movhps	 2 * SIZE(CO2), %xmm0

	pshufd	$0x50, %xmm9,  %xmm4

	mulps	%xmm7, %xmm4
	addps	%xmm0, %xmm4

	movlps	%xmm4,   0 * SIZE(CO2)
	movhps	%xmm4,   2 * SIZE(CO2)

	addq	$4 * SIZE, CO1
	addq	$4 * SIZE, CO2
	ALIGN_4

.L90:
	testq	$1, M
	jle	.L99

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	32 * SIZE + BUFFER, BO
#else
	leaq	32 * SIZE + BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax,   4), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 8), BO
#endif

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
	sarq	$2, %rax
	jle	.L95
	ALIGN_4

.L91:

	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)

	movss	-32 * SIZE(AO), %xmm0
	movss	-31 * SIZE(AO), %xmm1
	movss	-32 * SIZE(BO), %xmm2
	movss	-28 * SIZE(BO), %xmm3
	movss	-24 * SIZE(BO), %xmm4
	movss	-20 * SIZE(BO), %xmm5

	mulss	%xmm0, %xmm2
	mulss	%xmm0, %xmm3
	mulss	%xmm1, %xmm4
	mulss	%xmm1, %xmm5

	addss	%xmm2, %xmm8
	addss	%xmm3, %xmm9
	addss	%xmm4, %xmm10
	addss	%xmm5, %xmm11

	movss	-30 * SIZE(AO), %xmm0
	movss	-29 * SIZE(AO), %xmm1
	movss	-16 * SIZE(BO), %xmm2
	movss	-12 * SIZE(BO), %xmm3
	movss	 -8 * SIZE(BO), %xmm4
	movss	 -4 * SIZE(BO), %xmm5

	mulss	%xmm0, %xmm2
	mulss	%xmm0, %xmm3
	mulss	%xmm1, %xmm4
	mulss	%xmm1, %xmm5

	addss	%xmm2, %xmm8
	addss	%xmm3, %xmm9
	addss	%xmm4, %xmm10
	addss	%xmm5, %xmm11

	subq	$ -4 * SIZE, AO
	subq	$-32 * SIZE, BO
	subq	$1, %rax
	jg	.L91
	ALIGN_4

.L95:
	movaps	ALPHA, %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	je .L98
	ALIGN_4

.L96:
	movss	-32 * SIZE(AO), %xmm0
	movss	-32 * SIZE(BO), %xmm2
	movss	-28 * SIZE(BO), %xmm3

	mulss	%xmm0, %xmm2
	mulss	%xmm0, %xmm3

	addss	%xmm2, %xmm8
	addss	%xmm3, %xmm9

	addq	$1 * SIZE, AO
	addq	$8 * SIZE, BO
	subq	$1, %rax
	jg	.L96
	ALIGN_4

.L98:
	addss	%xmm10, %xmm8
	addss	%xmm11, %xmm9

	movsd	 0 * SIZE(CO1), %xmm0

	pshufd	$0x50, %xmm8,  %xmm4

	mulps	%xmm7, %xmm4
	addps	%xmm0, %xmm4

	movlps	%xmm4,   0 * SIZE(CO1)

	movsd	 0 * SIZE(CO2), %xmm0

	pshufd	$0x50, %xmm9,  %xmm4

	mulps	%xmm7, %xmm4
	addps	%xmm0, %xmm4

	movlps	%xmm4,   0 * SIZE(CO2)
	ALIGN_4

.L99:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addl	$2, KK
#endif

	leaq	(C, LDC, 2), C
	ALIGN_4



.L100:
	testq	$1, N
	jle	.L999
	ALIGN_4

.L101:
/* Copying to Sub Buffer */
	leaq	BUFFER, BO

#if defined(TRMMKERNEL) && defined(LEFT)
	movq	OFFSET, %rax
	movq	%rax, KK
#endif

	movq	K, %rax
	sarq	$4, %rax
	jle	.L103

	addq	%rax, %rax
	ALIGN_4

.L102:
	movss	 -32 * SIZE(B), %xmm0
	movss	 -31 * SIZE(B), %xmm1
	movss	 -30 * SIZE(B), %xmm2
	movss	 -29 * SIZE(B), %xmm3
	movss	 -28 * SIZE(B), %xmm4
	movss	 -27 * SIZE(B), %xmm5
	movss	 -26 * SIZE(B), %xmm6
	movss	 -25 * SIZE(B), %xmm7

	shufps	$0, %xmm0,  %xmm0
	shufps	$0, %xmm1,  %xmm1
	shufps	$0, %xmm2,  %xmm2
	shufps	$0, %xmm3,  %xmm3
	shufps	$0, %xmm4,  %xmm4
	shufps	$0, %xmm5,  %xmm5
	shufps	$0, %xmm6,  %xmm6
	shufps	$0, %xmm7,  %xmm7

	movaps	%xmm0,   0 * SIZE(BO)
	movaps	%xmm1,   4 * SIZE(BO)
	movaps	%xmm2,   8 * SIZE(BO)
	movaps	%xmm3,  12 * SIZE(BO)
	movaps	%xmm4,  16 * SIZE(BO)
	movaps	%xmm5,  20 * SIZE(BO)
	movaps	%xmm6,  24 * SIZE(BO)
	movaps	%xmm7,  28 * SIZE(BO)

	addq	$  8 * SIZE, B
	subq	$-32 * SIZE, BO
	subq	$1, %rax
	jne	.L102
	ALIGN_4

.L103:
	movq	K, %rax
	andq	$15, %rax
	BRANCH
	jle	.L105
	ALIGN_4

.L104:
	movss	 -32 * SIZE(B), %xmm8

	shufps	$0, %xmm8,  %xmm8

	movaps	%xmm8,  0 * SIZE(BO)

	addq	$1 * SIZE, B
	addq	$4 * SIZE, BO
	subq	$1, %rax
	jne	.L104
	ALIGN_4

.L105:
	movq	C, CO1
	movq	A, AO

	movq	M,  I
	sarq	$3, I
	jle	.L120
	ALIGN_4

.L110:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	32 * SIZE + BUFFER, BO
#else
	leaq	32 * SIZE + BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax,   8), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 2), BO
#endif

	pxor	%xmm8, %xmm8
	pxor	%xmm9, %xmm9

	prefetcht0    15 * SIZE(CO1)
	pxor	%xmm12, %xmm12
	pxor	%xmm13, %xmm13

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
	sarq	$2, %rax
	jle	.L115
	ALIGN_4

.L111:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)

	movaps	-32 * SIZE(AO), %xmm0
	movaps	-28 * SIZE(AO), %xmm1
	movaps	-32 * SIZE(BO), %xmm2
	movaps	 %xmm2, %xmm3

	mulps	%xmm0, %xmm2
	mulps	%xmm1, %xmm3
	addps	%xmm2, %xmm8
	addps	%xmm3, %xmm12

	movaps	-24 * SIZE(AO), %xmm0
	movaps	-20 * SIZE(AO), %xmm1
	movaps	-28 * SIZE(BO), %xmm2
	movaps	 %xmm2, %xmm3

	mulps	%xmm0, %xmm2
	mulps	%xmm1, %xmm3
	addps	%xmm2, %xmm9
	addps	%xmm3, %xmm13

	movaps	-16 * SIZE(AO), %xmm0
	movaps	-12 * SIZE(AO), %xmm1
	movaps	-24 * SIZE(BO), %xmm2
	movaps	 %xmm2, %xmm3

	mulps	%xmm0, %xmm2
	mulps	%xmm1, %xmm3
	addps	%xmm2, %xmm8
	addps	%xmm3, %xmm12

	movaps	 -8 * SIZE(AO), %xmm0
	movaps	 -4 * SIZE(AO), %xmm1
	movaps	-20 * SIZE(BO), %xmm2
	movaps	 %xmm2, %xmm3

	mulps	%xmm0, %xmm2
	mulps	%xmm1, %xmm3
	addps	%xmm2, %xmm9
	addps	%xmm3, %xmm13

	subq	$-32 * SIZE, AO
	subq	$-16 * SIZE, BO
	subq	$1, %rax
	jg	.L111
	ALIGN_4

.L115:
	movaps	ALPHA, %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	je .L118
	ALIGN_4

.L116:
	movaps	-32 * SIZE(AO), %xmm0
	movaps	-28 * SIZE(AO), %xmm1

	movaps	-32 * SIZE(BO), %xmm2
	movaps	 %xmm2, %xmm3

	mulps	%xmm0, %xmm2
	mulps	%xmm1, %xmm3
	addps	%xmm2, %xmm8
	addps	%xmm3, %xmm12

	addq	$8 * SIZE, AO
	addq	$4 * SIZE, BO
	subq	$1, %rax
	jg	.L116
	ALIGN_4

.L118:
	addps	%xmm9,  %xmm8
	addps	%xmm13, %xmm12

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

	addq	$16 * SIZE, CO1
	subq	$1, I
	jg	.L110
	ALIGN_4

.L120:
	testq	$4, M
	jle	.L130

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	32 * SIZE + BUFFER, BO
#else
	leaq	32 * SIZE + BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax,   8), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 2), BO
#endif

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
	addq	$4, %rax
#else
	addq	$1, %rax
#endif
	movq	%rax, KKK
#endif
	sarq	$2, %rax
	jle	.L125
	ALIGN_4

.L121:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)

	movaps	-32 * SIZE(AO), %xmm0
	movaps	-28 * SIZE(AO), %xmm1
	movaps	-32 * SIZE(BO), %xmm2
	movaps	-28 * SIZE(BO), %xmm3

	mulps	%xmm0, %xmm2
	mulps	%xmm1, %xmm3

	addps	%xmm2, %xmm8
	addps	%xmm3, %xmm9

	movaps	-24 * SIZE(AO), %xmm0
	movaps	-20 * SIZE(AO), %xmm1
	movaps	-24 * SIZE(BO), %xmm2
	movaps	-20 * SIZE(BO), %xmm3

	mulps	%xmm0, %xmm2
	mulps	%xmm1, %xmm3

	addps	%xmm2, %xmm10
	addps	%xmm3, %xmm11

	subq	$-16 * SIZE, AO
	subq	$-16 * SIZE, BO
	subq	$1, %rax
	jg	.L121
	ALIGN_4

.L125:
	movaps	ALPHA, %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	je .L128
	ALIGN_4

.L126:
	movaps	-32 * SIZE(AO), %xmm0
	movaps	-32 * SIZE(BO), %xmm2

	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm8

	addq	$4 * SIZE, AO
	addq	$4 * SIZE, BO
	subq	$1, %rax
	jg	.L126
	ALIGN_4

.L128:
	addps	%xmm10, %xmm8
	addps	%xmm11, %xmm9

	addps	%xmm9, %xmm8

	movsd	 0 * SIZE(CO1), %xmm0
	movhps	 2 * SIZE(CO1), %xmm0
	movsd	 4 * SIZE(CO1), %xmm1
	movhps	 6 * SIZE(CO1), %xmm1

	pshufd	$0x50, %xmm8,  %xmm4
	pshufd	$0xfa, %xmm8,  %xmm8

	mulps	%xmm7, %xmm4
	mulps	%xmm7, %xmm8
	addps	%xmm0, %xmm4
	addps	%xmm1, %xmm8

	movlps	%xmm4,   0 * SIZE(CO1)
	movhps	%xmm4,   2 * SIZE(CO1)
	movlps	%xmm8,   4 * SIZE(CO1)
	movhps	%xmm8,   6 * SIZE(CO1)

	addq	$8 * SIZE, CO1
	ALIGN_4

.L130:
	testq	$2, M
	jle	.L140

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	32 * SIZE + BUFFER, BO
#else
	leaq	32 * SIZE + BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax,   8), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 2), BO
#endif

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
	addq	$1, %rax
#endif
	movq	%rax, KKK
#endif
	sarq	$2, %rax
	jle	.L135
	ALIGN_4

.L131:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)

	movsd	-32 * SIZE(AO), %xmm0
	movsd	-30 * SIZE(AO), %xmm1
	movsd	-32 * SIZE(BO), %xmm2
	movsd	-28 * SIZE(BO), %xmm3

	mulps	%xmm0, %xmm2
	mulps	%xmm1, %xmm3
	addps	%xmm2, %xmm8
	addps	%xmm3, %xmm9

	movsd	-28 * SIZE(AO), %xmm0
	movsd	-26 * SIZE(AO), %xmm1
	movsd	-24 * SIZE(BO), %xmm2
	movsd	-20 * SIZE(BO), %xmm3

	mulps	%xmm0, %xmm2
	mulps	%xmm1, %xmm3
	addps	%xmm2, %xmm10
	addps	%xmm3, %xmm11

	subq	$ -8 * SIZE, AO
	subq	$-16 * SIZE, BO
	subq	$1, %rax
	jg	.L131
	ALIGN_4

.L135:
	movaps	ALPHA, %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	je .L138
	ALIGN_4

.L136:
	movsd	-32 * SIZE(AO), %xmm0
	movsd	-32 * SIZE(BO), %xmm2

	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm8

	addq	$2 * SIZE, AO
	addq	$4 * SIZE, BO
	subq	$1, %rax
	jg	.L136
	ALIGN_4

.L138:
	addps	%xmm10, %xmm8
	addps	%xmm11, %xmm9
	addps	%xmm9,  %xmm8

	movsd	 0 * SIZE(CO1), %xmm0
	movhps	 2 * SIZE(CO1), %xmm0

	pshufd	$0x50, %xmm8,  %xmm4

	mulps	%xmm7, %xmm4
	addps	%xmm0, %xmm4

	movlps	%xmm4,   0 * SIZE(CO1)
	movhps	%xmm4,   2 * SIZE(CO1)

	addq	$4 * SIZE, CO1
	ALIGN_4

.L140:
	testq	$1, M
	jle	.L999

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	32 * SIZE + BUFFER, BO
#else
	leaq	32 * SIZE + BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax,   4), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 4), BO
#endif

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
	sarq	$2, %rax
	jle	.L145
	ALIGN_4

.L141:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)

	movss	-32 * SIZE(AO), %xmm0
	movss	-31 * SIZE(AO), %xmm1
	movss	-32 * SIZE(BO), %xmm2
	movss	-28 * SIZE(BO), %xmm3

	mulss	%xmm0, %xmm2
	mulss	%xmm1, %xmm3
	addss	%xmm2, %xmm8
	addss	%xmm3, %xmm9

	movss	-30 * SIZE(AO), %xmm0
	movss	-29 * SIZE(AO), %xmm1
	movss	-24 * SIZE(BO), %xmm2
	movss	-20 * SIZE(BO), %xmm3

	mulss	%xmm0, %xmm2
	mulss	%xmm1, %xmm3
	addss	%xmm2, %xmm10
	addss	%xmm3, %xmm11

	subq	$ -4 * SIZE, AO
	subq	$-16 * SIZE, BO
	subq	$1, %rax
	jg	.L141
	ALIGN_4

.L145:
	movaps	ALPHA, %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	je .L148
	ALIGN_4

.L146:
	movss	-32 * SIZE(AO), %xmm0
	movss	-32 * SIZE(BO), %xmm2

	mulss	%xmm0, %xmm2
	addss	%xmm2, %xmm8

	addq	$1 * SIZE, AO
	addq	$4 * SIZE, BO
	subq	$1, %rax
	jg	.L146
	ALIGN_4

.L148:
	addss	%xmm10, %xmm8
	addss	%xmm11, %xmm9
	addss	%xmm9, %xmm8

	movsd	 0 * SIZE(CO1), %xmm0

	pshufd	$0x50, %xmm8,  %xmm4

	mulps	%xmm7, %xmm4
	addps	%xmm0, %xmm4

	movlps	%xmm4,   0 * SIZE(CO1)
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
