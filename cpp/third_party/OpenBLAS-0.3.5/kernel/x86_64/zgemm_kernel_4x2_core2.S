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

#define ALPHA_R	  0(%rsp)
#define ALPHA_I	 16(%rsp)
#define J	 32(%rsp)
#define OFFSET	 40(%rsp)
#define KK	 48(%rsp)
#define KKK	 56(%rsp)
#define BUFFER	128(%rsp)

#define PREFETCH_R    (16 *  4 + 0)
#define PREFETCH_W    (PREFETCH_R * 4)

#define PREFETCHSIZE  (16 * 13 + 10)
#define PREFETCH     prefetcht0

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(RN) || defined(RT) || defined(CN) || defined(CT)
#define ADDSUB	addps
#else
#define ADDSUB	subps
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
	movsd	OLD_ALPHA_I, %xmm1
#else
	movq	OLD_LDC,   LDC
#ifdef TRMMKERNEL
	movsd	OLD_OFFSET, %xmm12
#endif

#endif

	movq	%rsp, %r15	# save old stack
	subq	$128 + LOCAL_BUFFER_SIZE, %rsp
	andq	$-4096, %rsp	# align stack

	STACK_TOUCHING

	pxor	%xmm7, %xmm7
	cmpeqps	%xmm7, %xmm7
	pslld	$31, %xmm7	# Generate mask

	shufps	$0, %xmm0, %xmm0
	movaps	 %xmm0,  0 + ALPHA_R

	movss	 %xmm1,  4 + ALPHA_I
	movss	 %xmm1, 12 + ALPHA_I
	xorps	 %xmm7, %xmm1
	movss	 %xmm1,  0 + ALPHA_I
	movss	 %xmm1,  8 + ALPHA_I

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
	sarq	$1, J		# j = (n >> 2)
	jle	.L40
	ALIGN_4

.L01:
#if defined(TRMMKERNEL) && defined(LEFT)
	movq	OFFSET, %rax
	movq	%rax, KK
#endif

/* Copying to Sub Buffer */
	leaq	32 * SIZE + BUFFER, BO

	movaps	 -32 * SIZE(B), %xmm3

	movq	K, %rax
	sarq	$2, %rax
	jle	.L03
	ALIGN_4

.L02:
	prefetcht0	(PREFETCH_R + 0) * SIZE(B)
	movaps	 -28 * SIZE(B), %xmm7
	movaps	 -24 * SIZE(B), %xmm11
	movaps	 -20 * SIZE(B), %xmm15

	prefetcht0	(PREFETCH_W +  0) * SIZE(BO)
	pshufd	 $0x00, %xmm3, %xmm0
	movaps	%xmm0,  -32 * SIZE(BO)
	pshufd	 $0x55, %xmm3, %xmm1
	movaps	%xmm1,  -28 * SIZE(BO)
	pshufd	 $0xaa, %xmm3, %xmm2
	movaps	%xmm2,  -24 * SIZE(BO)
	pshufd	 $0xff, %xmm3, %xmm3
	movaps	%xmm3,  -20 * SIZE(BO)

	movaps	 -16 * SIZE(B), %xmm3

	prefetcht0	(PREFETCH_W + 16) * SIZE(BO)
	pshufd	 $0x00, %xmm7, %xmm4
	movaps	%xmm4,  -16 * SIZE(BO)
	pshufd	 $0x55, %xmm7, %xmm5
	movaps	%xmm5,  -12 * SIZE(BO)
	pshufd	 $0xaa, %xmm7, %xmm6
	movaps	%xmm6,   -8 * SIZE(BO)
	pshufd	 $0xff, %xmm7, %xmm7
	movaps	%xmm7,   -4 * SIZE(BO)

	prefetcht0	(PREFETCH_W + 32) * SIZE(BO)

	pshufd	 $0x00, %xmm11, %xmm8
	movaps	%xmm8,    0 * SIZE(BO)
	pshufd	 $0x55, %xmm11, %xmm9
	movaps	%xmm9,    4 * SIZE(BO)
	pshufd	 $0xaa, %xmm11, %xmm10
	movaps	%xmm10,   8 * SIZE(BO)
	pshufd	 $0xff, %xmm11, %xmm11
	movaps	%xmm11,  12 * SIZE(BO)

	prefetcht0	(PREFETCH_W + 48) * SIZE(BO)

	pshufd	 $0x00, %xmm15, %xmm12
	movaps	%xmm12,  16 * SIZE(BO)
	pshufd	 $0x55, %xmm15, %xmm13
	movaps	%xmm13,  20 * SIZE(BO)
	pshufd	 $0xaa, %xmm15, %xmm14
	movaps	%xmm14,  24 * SIZE(BO)
	pshufd	 $0xff, %xmm15, %xmm15
	movaps	%xmm15,  28 * SIZE(BO)

	subq	$-16 * SIZE, B
	subq	$-64 * SIZE, BO
	subq	$1, %rax
	jne	.L02
	ALIGN_4

.L03:
	movq	K, %rax
	andq	$3, %rax
	BRANCH
	jle	.L10
	ALIGN_4

.L04:
	pshufd	 $0x00, %xmm3, %xmm0
	movaps	%xmm0,  -32 * SIZE(BO)
	pshufd	 $0x55, %xmm3, %xmm1
	movaps	%xmm1,  -28 * SIZE(BO)
	pshufd	 $0xaa, %xmm3, %xmm2
	movaps	%xmm2,  -24 * SIZE(BO)
	pshufd	 $0xff, %xmm3, %xmm3
	movaps	%xmm3,  -20 * SIZE(BO)

	movaps	 -28 * SIZE(B), %xmm3

	addq	$ 4 * SIZE, B
	addq	$16 * SIZE, BO
	subq	$1, %rax
	jne	.L04
	ALIGN_4

.L10:
	leaq	(PREFETCH_R +  0) * SIZE(B), BB

	movq	C, CO1
	leaq	(C, LDC, 1), CO2
	movq	A, AO

	movq	M,  I
	sarq	$2, I		# i = (m >> 2)
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

	movaps	-32 * SIZE(AO), %xmm0
	movaps	-28 * SIZE(AO), %xmm1
	movaps	-40 * SIZE(BO), %xmm6
	movaps	-36 * SIZE(BO), %xmm7

	prefetcht2	-32 * SIZE(BB)
	pxor	%xmm8, %xmm8
	pxor	%xmm9, %xmm9

	pxor	%xmm10, %xmm10
	pxor	%xmm11, %xmm11

	pxor	%xmm12, %xmm12
	prefetcht0     7 * SIZE(CO1)
	pxor	%xmm13, %xmm13
	pxor	%xmm14, %xmm14
	pxor	%xmm15, %xmm15

	pxor	%xmm2, %xmm2
	pxor	%xmm3, %xmm3
	prefetcht0     7 * SIZE(CO2)
	pxor	%xmm4, %xmm4
	pxor	%xmm5, %xmm5

	subq		$-32 * SIZE, BB

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

	ADDSUB	%xmm4, %xmm11
	movaps	-28 * SIZE(BO), %xmm4
	ADDSUB	%xmm5, %xmm15
	movaps	%xmm7, %xmm5
	mulps	%xmm0, %xmm7
	mulps	%xmm1, %xmm5

	addps	%xmm6, %xmm8
	movaps	-24 * SIZE(BO), %xmm6
	addps	%xmm3, %xmm12
	movaps	%xmm2, %xmm3
	mulps	%xmm0, %xmm2
	mulps	%xmm1, %xmm3

	ADDSUB	%xmm7, %xmm9
	movaps	-20 * SIZE(BO), %xmm7
	ADDSUB	%xmm5, %xmm13
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

	ADDSUB	%xmm4, %xmm11
	movaps  -12 * SIZE(BO), %xmm4
	ADDSUB	%xmm5, %xmm15
	movaps	%xmm7, %xmm5
	mulps	%xmm0, %xmm7
	mulps	%xmm1, %xmm5

	addps	%xmm6, %xmm8
	movaps	 -8 * SIZE(BO), %xmm6
	addps	%xmm3, %xmm12
	movaps	%xmm2, %xmm3
	mulps	%xmm0, %xmm2
	mulps	%xmm1, %xmm3

	ADDSUB	%xmm7, %xmm9
	movaps	 -4 * SIZE(BO), %xmm7
	ADDSUB	%xmm5, %xmm13
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
	PREFETCH (PREFETCHSIZE +  8) * SIZE(AO)
	mulps	%xmm0, %xmm6
	mulps	%xmm1, %xmm3

	ADDSUB	%xmm4, %xmm11
	movaps	  4 * SIZE(BO), %xmm4
	ADDSUB	%xmm5, %xmm15
	movaps	%xmm7, %xmm5
	mulps	%xmm0, %xmm7
	mulps	%xmm1, %xmm5

	addps	%xmm6, %xmm8
	movaps	  8 * SIZE(BO), %xmm6
	addps	%xmm3, %xmm12
	movaps	%xmm2, %xmm3
	mulps	%xmm0, %xmm2
	mulps	%xmm1, %xmm3

	ADDSUB	%xmm7, %xmm9
	movaps	 12 * SIZE(BO), %xmm7
	ADDSUB	%xmm5, %xmm13
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

	ADDSUB	%xmm4, %xmm11
	movaps	 20 * SIZE(BO), %xmm4
	ADDSUB	%xmm5, %xmm15
	movaps	%xmm7, %xmm5
	subq	$-32 * SIZE, AO
	mulps	%xmm0, %xmm7
	mulps	%xmm1, %xmm5

	addps	%xmm6, %xmm8
	movaps	 24 * SIZE(BO), %xmm6
	addps	%xmm3, %xmm12
	movaps	%xmm2, %xmm3
	mulps	%xmm0, %xmm2
	mulps	%xmm1, %xmm3

	ADDSUB	%xmm7, %xmm9
	movaps	 28 * SIZE(BO), %xmm7
	ADDSUB	%xmm5, %xmm13
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
	prefetcht2	-16 * SIZE(BB)

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

	ADDSUB	%xmm4, %xmm11
	movaps	-28 * SIZE(BO), %xmm4
	ADDSUB	%xmm5, %xmm15
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

	ADDSUB	%xmm7, %xmm9
	movaps	-20 * SIZE(BO), %xmm7
	ADDSUB	%xmm5, %xmm13
	addq	$16 * SIZE, BO
	movaps	 %xmm4, %xmm5
	mulps	%xmm0, %xmm4
	movaps	-32 * SIZE(AO), %xmm0
	mulps	%xmm1, %xmm5
	movaps	-28 * SIZE(AO), %xmm1

	subq	$1, %rax
	jg	.L16
	ALIGN_4

.L18:
	movaps	ALPHA_R, %xmm6
	movaps	ALPHA_I, %xmm7

	addps	%xmm2, %xmm10
	addps	%xmm3, %xmm14
	ADDSUB	%xmm4, %xmm11
	ADDSUB	%xmm5, %xmm15

#if !defined(TRMMKERNEL) && !defined(BETAZERO)
	movsd	0 * SIZE(CO1), %xmm0
	movhps	2 * SIZE(CO1), %xmm0
	movsd	4 * SIZE(CO1), %xmm2
	movhps	6 * SIZE(CO1), %xmm2

	movsd	0 * SIZE(CO2), %xmm1
	movhps	2 * SIZE(CO2), %xmm1
	movsd	4 * SIZE(CO2), %xmm3
	movhps	6 * SIZE(CO2), %xmm3
#endif

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)

	shufps	$0xb1, %xmm9, %xmm9
	shufps	$0xb1, %xmm11, %xmm11
	shufps	$0xb1, %xmm13, %xmm13
	shufps	$0xb1, %xmm15, %xmm15

	addsubps	%xmm9, %xmm8
	addsubps	%xmm11, %xmm10
	addsubps	%xmm13, %xmm12
	addsubps	%xmm15, %xmm14

	movaps	%xmm8, %xmm9
	movaps	%xmm10, %xmm11
	movaps	%xmm12, %xmm13
	movaps	%xmm14, %xmm15

	shufps	$0xb1, %xmm8, %xmm8
	shufps	$0xb1, %xmm10, %xmm10
	shufps	$0xb1, %xmm12, %xmm12
	shufps	$0xb1, %xmm14, %xmm14
#else
	shufps	$0xb1, %xmm8, %xmm8
	shufps	$0xb1, %xmm10, %xmm10
	shufps	$0xb1, %xmm12, %xmm12
	shufps	$0xb1, %xmm14, %xmm14

	addsubps	%xmm8, %xmm9
	addsubps	%xmm10, %xmm11
	addsubps	%xmm12, %xmm13
	addsubps	%xmm14, %xmm15

	movaps	%xmm9, %xmm8
	movaps	%xmm11, %xmm10
	movaps	%xmm13, %xmm12
	movaps	%xmm15, %xmm14

	shufps	$0xb1, %xmm9, %xmm9
	shufps	$0xb1, %xmm11, %xmm11
	shufps	$0xb1, %xmm13, %xmm13
	shufps	$0xb1, %xmm15, %xmm15
#endif

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

#if !defined(TRMMKERNEL) && !defined(BETAZERO)
	addps	%xmm0, %xmm8
	addps	%xmm1, %xmm10
	addps	%xmm2, %xmm12
	addps	%xmm3, %xmm14
#endif

	movsd	%xmm8,  0 * SIZE(CO1)
	movhps	%xmm8,  2 * SIZE(CO1)
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
	je	.L25
	ALIGN_4

.L22:
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
	ADDSUB	%xmm3, %xmm9
	addps	%xmm4, %xmm10
	ADDSUB	%xmm5, %xmm11

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
	ADDSUB	%xmm3, %xmm9
	addps	%xmm4, %xmm10
	ADDSUB	%xmm5, %xmm11

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
	ADDSUB	%xmm3, %xmm9
	addps	%xmm4, %xmm10
	ADDSUB	%xmm5, %xmm11

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
	ADDSUB	%xmm3, %xmm9
	addps	%xmm4, %xmm10
	ADDSUB	%xmm5, %xmm11

	subq	$-16 * SIZE, AO
	subq	$-64 * SIZE, BO
	subq	$1, %rax
	jne    .L22
	ALIGN_4

.L25:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	BRANCH
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
	ADDSUB	%xmm3, %xmm9
	addps	%xmm4, %xmm10
	ADDSUB	%xmm5, %xmm11

	addq	$ 4 * SIZE, AO
	addq	$16 * SIZE, BO
	subq	$1, %rax
	jg	.L26
	ALIGN_4

.L28:
	movaps	ALPHA_R, %xmm6
	movaps	ALPHA_I, %xmm7

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)

	shufps	$0xb1, %xmm9, %xmm9
	shufps	$0xb1, %xmm11, %xmm11

	addsubps	%xmm9, %xmm8
	addsubps	%xmm11, %xmm10

	movaps	%xmm8, %xmm9
	movaps	%xmm10, %xmm11

	shufps	$0xb1, %xmm8, %xmm8
	shufps	$0xb1, %xmm10, %xmm10
#else
	shufps	$0xb1, %xmm8, %xmm8
	shufps	$0xb1, %xmm10, %xmm10

	addsubps	%xmm8, %xmm9
	addsubps	%xmm10, %xmm11

	movaps	%xmm9, %xmm8
	movaps	%xmm11, %xmm10

	shufps	$0xb1, %xmm9, %xmm9
	shufps	$0xb1, %xmm11, %xmm11
#endif

	mulps	%xmm6, %xmm9
	mulps	%xmm7, %xmm8
	mulps	%xmm6, %xmm11
	mulps	%xmm7, %xmm10

	addps	%xmm9, %xmm8
	addps	%xmm11, %xmm10

#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	movsd	0 * SIZE(CO1), %xmm0
	movhps	2 * SIZE(CO1), %xmm0
	movsd	0 * SIZE(CO2), %xmm2
	movhps	2 * SIZE(CO2), %xmm2

	addps	%xmm0,  %xmm8
	addps	%xmm2, %xmm10
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
	je	.L35
	ALIGN_4

.L32:
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
	ADDSUB	%xmm3, %xmm9
	addps	%xmm4, %xmm10
	ADDSUB	%xmm5, %xmm11

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
	ADDSUB	%xmm3, %xmm9
	addps	%xmm4, %xmm10
	ADDSUB	%xmm5, %xmm11

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
	ADDSUB	%xmm3, %xmm9
	addps	%xmm4, %xmm10
	ADDSUB	%xmm5, %xmm11

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
	ADDSUB	%xmm3, %xmm9
	addps	%xmm4, %xmm10
	ADDSUB	%xmm5, %xmm11

	subq	$ -8 * SIZE, AO
	subq	$-64 * SIZE, BO
	subq	$1, %rax
	jne    .L32
	ALIGN_4

.L35:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	BRANCH
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
	ADDSUB	%xmm3, %xmm9
	addps	%xmm4, %xmm10
	ADDSUB	%xmm5, %xmm11

	addq	$ 2 * SIZE, AO
	addq	$16 * SIZE, BO
	subq	$1, %rax
	jg	.L36
	ALIGN_4

.L38:
	movaps	ALPHA_R, %xmm6
	movaps	ALPHA_I, %xmm7

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)

	shufps	$0xb1, %xmm9, %xmm9
	shufps	$0xb1, %xmm11, %xmm11

	addsubps	%xmm9, %xmm8
	addsubps	%xmm11, %xmm10

	movaps	%xmm8, %xmm9
	movaps	%xmm10, %xmm11

	shufps	$0xb1, %xmm8, %xmm8
	shufps	$0xb1, %xmm10, %xmm10
#else
	shufps	$0xb1, %xmm8, %xmm8
	shufps	$0xb1, %xmm10, %xmm10

	addsubps	%xmm8, %xmm9
	addsubps	%xmm10, %xmm11

	movaps	%xmm9, %xmm8
	movaps	%xmm11, %xmm10

	shufps	$0xb1, %xmm9, %xmm9
	shufps	$0xb1, %xmm11, %xmm11
#endif

	mulps	%xmm6, %xmm9
	mulps	%xmm7, %xmm8
	mulps	%xmm6, %xmm11
	mulps	%xmm7, %xmm10

	addps	%xmm9, %xmm8
	addps	%xmm11, %xmm10

#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	movsd	0 * SIZE(CO1), %xmm0
	movsd	0 * SIZE(CO2), %xmm2

	addps	%xmm0,  %xmm8
	addps	%xmm2, %xmm10
#endif

	movsd	%xmm8, 0 * SIZE(CO1)
	movsd	%xmm10, 0 * SIZE(CO2)

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

	movq	K, %rax
	sarq	$2, %rax
	jle	.L43
	ALIGN_4

.L42:
	movss	 -32 * SIZE(B), %xmm8
	movss	 -31 * SIZE(B), %xmm9
	movss	 -30 * SIZE(B), %xmm10
	movss	 -29 * SIZE(B), %xmm11
	movss	 -28 * SIZE(B), %xmm12
	movss	 -27 * SIZE(B), %xmm13
	movss	 -26 * SIZE(B), %xmm14
	movss	 -25 * SIZE(B), %xmm15

	shufps	$0, %xmm8,  %xmm8
	shufps	$0, %xmm9,  %xmm9
	shufps	$0, %xmm10,  %xmm10
	shufps	$0, %xmm11,  %xmm11
	shufps	$0, %xmm12,  %xmm12
	shufps	$0, %xmm13,  %xmm13
	shufps	$0, %xmm14,  %xmm14
	shufps	$0, %xmm15,  %xmm15

	movaps	%xmm8,  0 * SIZE(BO)
	movaps	%xmm9,  4 * SIZE(BO)
	movaps	%xmm10,  8 * SIZE(BO)
	movaps	%xmm11, 12 * SIZE(BO)
	movaps	%xmm12, 16 * SIZE(BO)
	movaps	%xmm13, 20 * SIZE(BO)
	movaps	%xmm14, 24 * SIZE(BO)
	movaps	%xmm15, 28 * SIZE(BO)

	addq	$ 8 * SIZE, B
	addq	$32 * SIZE, BO

	subq	$1, %rax
	jne	.L42
	ALIGN_4

.L43:
	movq	K, %rax
	andq	$3, %rax
	BRANCH
	jle	.L50
	ALIGN_4

.L44:
	movss	 -32 * SIZE(B), %xmm8
	movss	 -31 * SIZE(B), %xmm9

	shufps	$0, %xmm8,  %xmm8
	shufps	$0, %xmm9,  %xmm9

	movaps	%xmm8,  0 * SIZE(BO)
	movaps	%xmm9,  4 * SIZE(BO)

	addq	$2 * SIZE, B
	addq	$8 * SIZE, BO
	subq	$1, %rax
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

	pxor	%xmm8, %xmm8
	prefetcht0     3 * SIZE(CO1)
	pxor	%xmm9, %xmm9
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
	addq	$4, %rax
#else
	addq	$1, %rax
#endif
	movq	%rax, KKK
#endif
	sarq	$2, %rax
	je	.L55
	ALIGN_4

.L52:
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
	ADDSUB	%xmm4, %xmm9
	ADDSUB	%xmm5, %xmm13

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
	ADDSUB	%xmm4, %xmm9
	ADDSUB	%xmm5, %xmm13

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
	ADDSUB	%xmm4, %xmm9
	ADDSUB	%xmm5, %xmm13

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
	ADDSUB	%xmm4, %xmm9
	ADDSUB	%xmm5, %xmm13

	subq	$-32 * SIZE, AO
	subq	$-32 * SIZE, BO
	subq	$1, %rax
	jne    .L52
	ALIGN_4

.L55:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	BRANCH
	je .L58
	ALIGN_4

.L56:
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
	ADDSUB	%xmm4, %xmm9
	ADDSUB	%xmm5, %xmm13

	addq	$8 * SIZE, AO
	addq	$8 * SIZE, BO
	subq	$1, %rax
	jg	.L56
	ALIGN_4

.L58:
	movaps	ALPHA_R, %xmm6
	movaps	ALPHA_I, %xmm7

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)

	shufps	$0xb1, %xmm9, %xmm9
	shufps	$0xb1, %xmm13, %xmm13

	addsubps	%xmm9, %xmm8
	addsubps	%xmm13, %xmm12

	movaps	%xmm8, %xmm9
	movaps	%xmm12, %xmm13

	shufps	$0xb1, %xmm8, %xmm8
	shufps	$0xb1, %xmm12, %xmm12
#else
	shufps	$0xb1, %xmm8, %xmm8
	shufps	$0xb1, %xmm12, %xmm12

	addsubps	%xmm8, %xmm9
	addsubps	%xmm12, %xmm13

	movaps	%xmm9, %xmm8
	movaps	%xmm13, %xmm12

	shufps	$0xb1, %xmm9, %xmm9
	shufps	$0xb1, %xmm13, %xmm13
#endif

	mulps	%xmm6, %xmm9
	mulps	%xmm7, %xmm8
	mulps	%xmm6, %xmm13
	mulps	%xmm7, %xmm12

	addps	%xmm9, %xmm8
	addps	%xmm13, %xmm12

#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	movsd	0 * SIZE(CO1), %xmm0
	movhps	2 * SIZE(CO1), %xmm0
	movsd	4 * SIZE(CO1), %xmm1
	movhps	6 * SIZE(CO1), %xmm1

	addps	%xmm0, %xmm8
	addps	%xmm1, %xmm12
#endif

	movsd	%xmm8, 0 * SIZE(CO1)
	movhps	%xmm8, 2 * SIZE(CO1)
	movsd	%xmm12, 4 * SIZE(CO1)
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
	je	.L65
	ALIGN_4

.L62:
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
	ADDSUB	%xmm3, %xmm9
	addps	%xmm4, %xmm10
	ADDSUB	%xmm5, %xmm11

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
	ADDSUB	%xmm3, %xmm9
	addps	%xmm4, %xmm10
	ADDSUB	%xmm5, %xmm11

	subq	$-16 * SIZE, AO
	subq	$-32 * SIZE, BO
	subq	$1, %rax
	jne    .L62
	ALIGN_4

.L65:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	BRANCH
	je .L68
	ALIGN_4

.L66:
	movaps	-32 * SIZE(AO), %xmm0
	movaps	-32 * SIZE(BO), %xmm2
	movaps	-28 * SIZE(BO), %xmm3

	mulps	%xmm0, %xmm2
	mulps	%xmm0, %xmm3

	addps	%xmm2, %xmm8
	ADDSUB	%xmm3, %xmm9

	addq	$4 * SIZE, AO
	addq	$8 * SIZE, BO
	subq	$1, %rax
	jg	.L66
	ALIGN_4

.L68:
	movaps	ALPHA_R, %xmm6
	movaps	ALPHA_I, %xmm7

	addps	%xmm10, %xmm8
	addps	%xmm11, %xmm9

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)
	shufps	$0xb1, %xmm9, %xmm9
	addsubps	%xmm9, %xmm8
	movaps	%xmm8, %xmm9
	shufps	$0xb1, %xmm8, %xmm8
#else
	shufps	$0xb1, %xmm8, %xmm8
	addsubps	%xmm8, %xmm9
	movaps	%xmm9, %xmm8
	shufps	$0xb1, %xmm9, %xmm9
#endif

	mulps	%xmm6, %xmm9
	mulps	%xmm7, %xmm8
	addps	%xmm9, %xmm8

#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	movsd	0 * SIZE(CO1), %xmm0
	movhps	2 * SIZE(CO1), %xmm0

	addps	%xmm0, %xmm8
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
	je	.L75
	ALIGN_4

.L72:
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
	ADDSUB	%xmm3, %xmm9
	addps	%xmm4, %xmm10
	ADDSUB	%xmm5, %xmm11

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
	ADDSUB	%xmm3, %xmm9
	addps	%xmm4, %xmm10
	ADDSUB	%xmm5, %xmm11

	subq	$ -8 * SIZE, AO
	subq	$-32 * SIZE, BO
	subq	$1, %rax
	jne    .L72
	ALIGN_4

.L75:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax
	BRANCH
	je .L78
	ALIGN_4

.L76:
	movsd	-32 * SIZE(AO), %xmm0
	movsd	-32 * SIZE(BO), %xmm2
	movsd	-28 * SIZE(BO), %xmm3

	mulps	%xmm0, %xmm2
	mulps	%xmm0, %xmm3

	addps	%xmm2, %xmm8
	ADDSUB	%xmm3, %xmm9

	addq	$2 * SIZE, AO
	addq	$8 * SIZE, BO
	subq	$1, %rax
	jg	.L76
	ALIGN_4

.L78:
	movaps	ALPHA_R, %xmm6
	movaps	ALPHA_I, %xmm7

	addps	%xmm10, %xmm8
	addps	%xmm11, %xmm9

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)
	shufps	$0xb1, %xmm9, %xmm9
	addsubps	%xmm9, %xmm8
	movaps	%xmm8, %xmm9
	shufps	$0xb1, %xmm8, %xmm8
#else
	shufps	$0xb1, %xmm8, %xmm8
	addsubps	%xmm8, %xmm9
	movaps	%xmm9, %xmm8
	shufps	$0xb1, %xmm9, %xmm9
#endif

	mulps	%xmm6, %xmm9
	mulps	%xmm7, %xmm8
	addps	%xmm9, %xmm8

#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	movsd	0 * SIZE(CO1), %xmm0

	addps	%xmm0, %xmm8
#endif

	movsd	%xmm8, 0 * SIZE(CO1)
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
