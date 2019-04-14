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

#define PREFETCH     prefetcht0
#define PREFETCHW    prefetcht0
#define RPREFETCHSIZE (16 * 4)
#define PREFETCHSIZE  (16 * 8 + 8)

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

#else
	movq	OLD_LDC,   LDC
#ifdef TRMMKERNEL
	movsd	OLD_OFFSET, %xmm4
#endif

#endif

	movq	%rsp, %rbx	# save old stack
	subq	$128 + LOCAL_BUFFER_SIZE, %rsp
	andq	$-4096, %rsp	# align stack

	STACK_TOUCHING

	movq	OLD_M, M
	movq	OLD_N, N

	shufps	$0, %xmm0, %xmm0
	movaps	%xmm0, ALPHA

#ifdef TRMMKERNEL
	movsd	%xmm4, OFFSET
	movsd	%xmm4, KK
#ifndef LEFT
	negq	KK
#endif
#endif

	subq	$-32 * SIZE, A

	salq	$BASE_SHIFT, LDC

	movq	N,  J
	sarq	$3, J
	jle	.L40

.L01:
#if defined(TRMMKERNEL) && defined(LEFT)
	movq	OFFSET, %rax
	movq	%rax, KK
#endif

	leaq	32 * SIZE + BUFFER, BO

	movaps	 0 * SIZE(B), %xmm1
	movaps	 4 * SIZE(B), %xmm3
	movaps	 8 * SIZE(B), %xmm5
	movaps	12 * SIZE(B), %xmm7

	movq	K, %rax
	sarq	$1, %rax
	jle	.L03
	ALIGN_4

.L02:
	PREFETCH	 (RPREFETCHSIZE +  0)  * SIZE(B)

	pshufd	 $0x50, %xmm1, %xmm0
	movaps	%xmm0, -32 * SIZE(BO)
	pshufd	 $0xfa, %xmm1, %xmm1
	movaps	%xmm1, -28 * SIZE(BO)

	movaps	16 * SIZE(B), %xmm1

	pshufd	 $0x50, %xmm3, %xmm2
	movaps	%xmm2, -24 * SIZE(BO)
	pshufd	 $0xfa, %xmm3, %xmm3
	movaps	%xmm3, -20 * SIZE(BO)

	movaps	20 * SIZE(B), %xmm3

	pshufd	 $0x50, %xmm5, %xmm4
	movaps	%xmm4, -16 * SIZE(BO)
	pshufd	 $0xfa, %xmm5, %xmm5
	movaps	%xmm5, -12 * SIZE(BO)

	movaps	24 * SIZE(B), %xmm5

	pshufd	 $0x50, %xmm7, %xmm6
	movaps	%xmm6,  -8 * SIZE(BO)
	pshufd	 $0xfa, %xmm7, %xmm7
	movaps	%xmm7,  -4 * SIZE(BO)

	movaps	28 * SIZE(B), %xmm7

	addq	$16 * SIZE, B
	addq	$32 * SIZE, BO

	decq	%rax
	jne	.L02
	ALIGN_4

.L03:
	movq	K, %rax
	andq	$1, %rax
	BRANCH
	jle	.L10

	pshufd	 $0x50, %xmm1, %xmm0
	movaps	%xmm0, -32 * SIZE(BO)
	pshufd	 $0xfa, %xmm1, %xmm1
	movaps	%xmm1, -28 * SIZE(BO)

	pshufd	 $0x50, %xmm3, %xmm2
	movaps	%xmm2, -24 * SIZE(BO)
	pshufd	 $0xfa, %xmm3, %xmm3
	movaps	%xmm3, -20 * SIZE(BO)

	addq	$  8 * SIZE, B
	subq	$-16 * SIZE, BO
	ALIGN_4

.L10:
	movq	C, CO1
	leaq	(C, LDC, 4), CO2
	movq	A, AO

	leaq	 (RPREFETCHSIZE +  0)  * SIZE(B), BB

	movq	M,  I
	sarq	$2, I
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
	salq	$BASE_SHIFT + 1, %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 8), BO
#endif

	leaq	(LDC, LDC, 2), %rax

	movaps	-32 * SIZE(AO), %xmm0
	movaps	-32 * SIZE(BO), %xmm1

	pxor	%xmm8, %xmm8
	PREFETCHW      3 * SIZE(CO1)
	pxor	%xmm9, %xmm9
	PREFETCHW      5 * SIZE(CO1, LDC, 1)
	pxor	%xmm10, %xmm10
	PREFETCHW      3 * SIZE(CO1, LDC, 2)
	pxor	%xmm11, %xmm11
	PREFETCHW      5 * SIZE(CO1, %rax)

	pxor	%xmm12, %xmm12
	PREFETCHW      3 * SIZE(CO2)
	pxor	%xmm13, %xmm13
	PREFETCHW      5 * SIZE(CO2, LDC, 1)
	pxor	%xmm14, %xmm14
	PREFETCHW      3 * SIZE(CO2, LDC, 2)
	pxor	%xmm15, %xmm15
	PREFETCHW      5 * SIZE(CO2, %rax)

	PREFETCH	 -32  * SIZE(BB)
	addq		 $16 * SIZE, BB

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
	addq	$8, %rax
#endif
	movq	%rax, KKK
#endif
	sarq	$2, %rax
	NOBRANCH
	jle	.L16
	ALIGN_3

.L12:
	PREFETCH	(PREFETCHSIZE +  0)(AO)

	pshufd	$0x4e, %xmm1, %xmm3
	mulps	%xmm0, %xmm1
	addps	%xmm1, %xmm8
	movaps	-28 * SIZE(BO), %xmm1
	mulps	%xmm0, %xmm3
	movaps	%xmm0, %xmm2
	movaps	-16 * SIZE(AO), %xmm0
	addps	%xmm3, %xmm9

	pshufd	$0x4e, %xmm1, %xmm3
	mulps	%xmm2, %xmm1
	addps	%xmm1, %xmm10
	movaps	-24 * SIZE(BO), %xmm1
	mulps	%xmm2, %xmm3
	addps	%xmm3, %xmm11

	pshufd	$0x4e, %xmm1, %xmm3
	mulps	%xmm2, %xmm1
	addps	%xmm1, %xmm12
	movaps	-20 * SIZE(BO), %xmm1
	mulps	%xmm2, %xmm3
	addps	%xmm3, %xmm13

	pshufd	$0x4e, %xmm1, %xmm3
	mulps	%xmm2, %xmm1
	addps	%xmm1, %xmm14
	movaps	-16 * SIZE(BO), %xmm1
	mulps	%xmm2, %xmm3
	movaps	-28 * SIZE(AO), %xmm2
	addps	%xmm3, %xmm15

	pshufd	$0x4e, %xmm1, %xmm3
	mulps	%xmm2, %xmm1
	addps	%xmm1, %xmm8
	movaps	-12 * SIZE(BO), %xmm1
	mulps	%xmm2, %xmm3
	addps	%xmm3, %xmm9

	pshufd	$0x4e, %xmm1, %xmm3
	mulps	%xmm2, %xmm1
	addps	%xmm1, %xmm10
	movaps	 -8 * SIZE(BO), %xmm1
	mulps	%xmm2, %xmm3
	addps	%xmm3, %xmm11

	pshufd	$0x4e, %xmm1, %xmm3
	mulps	%xmm2, %xmm1
	addps	%xmm1, %xmm12
	movaps	 -4 * SIZE(BO), %xmm1
	mulps	%xmm2, %xmm3
	addps	%xmm3, %xmm13

	pshufd	$0x4e, %xmm1, %xmm3
	mulps	%xmm2, %xmm1
	addps	%xmm1, %xmm14
	movaps	  0 * SIZE(BO), %xmm1
	mulps	%xmm2, %xmm3
	movaps	-24 * SIZE(AO), %xmm2
	addps	%xmm3, %xmm15

	pshufd	$0x4e, %xmm1, %xmm3
	mulps	%xmm2, %xmm1
	addps	%xmm1, %xmm8
	movaps	  4 * SIZE(BO), %xmm1
	mulps	%xmm2, %xmm3
	addps	%xmm3, %xmm9

	pshufd	$0x4e, %xmm1, %xmm3
	mulps	%xmm2, %xmm1
	addps	%xmm1, %xmm10
	movaps	  8 * SIZE(BO), %xmm1
	mulps	%xmm2, %xmm3
	addps	%xmm3, %xmm11

	pshufd	$0x4e, %xmm1, %xmm3
	mulps	%xmm2, %xmm1
	addps	%xmm1, %xmm12
	movaps	 12 * SIZE(BO), %xmm1
	mulps	%xmm2, %xmm3
	addps	%xmm3, %xmm13

	pshufd	$0x4e, %xmm1, %xmm3
	mulps	%xmm2, %xmm1
	addps	%xmm1, %xmm14
	movaps	 16 * SIZE(BO), %xmm1
	mulps	%xmm2, %xmm3
	movaps	-20 * SIZE(AO), %xmm2
	addps	%xmm3, %xmm15

	pshufd	$0x4e, %xmm1, %xmm3
	mulps	%xmm2, %xmm1
	addps	%xmm1, %xmm8
	movaps	 20 * SIZE(BO), %xmm1
	mulps	%xmm2, %xmm3
	addps	%xmm3, %xmm9

	pshufd	$0x4e, %xmm1, %xmm3
	mulps	%xmm2, %xmm1
	addps	%xmm1, %xmm10
	movaps	 24 * SIZE(BO), %xmm1
	mulps	%xmm2, %xmm3
	addps	%xmm3, %xmm11

	pshufd	$0x4e, %xmm1, %xmm3
	mulps	%xmm2, %xmm1
	addps	%xmm1, %xmm12
	movaps	 28 * SIZE(BO), %xmm1
	mulps	%xmm2, %xmm3
	addps	%xmm3, %xmm13

	pshufd	$0x4e, %xmm1, %xmm3
	mulps	%xmm2, %xmm1
	addps	%xmm1, %xmm14
	movaps	 32 * SIZE(BO), %xmm1
	mulps	%xmm2, %xmm3
	addps	%xmm3, %xmm15

	subq	$-16 * SIZE, AO
	addq	$ 64 * SIZE, BO
	decq	%rax
	BRANCH
	jg	.L12

.L16:
	movaps	ALPHA, %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax
	je .L18
	ALIGN_4

.L17:
	pshufd	$0x4e, %xmm1, %xmm3
	mulps	%xmm0, %xmm1
	addps	%xmm1, %xmm8
	movaps	-28 * SIZE(BO), %xmm1
	mulps	%xmm0, %xmm3
	addps	%xmm3, %xmm9

	pshufd	$0x4e, %xmm1, %xmm3
	mulps	%xmm0, %xmm1
	addps	%xmm1, %xmm10
	movaps	-24 * SIZE(BO), %xmm1
	mulps	%xmm0, %xmm3
	addps	%xmm3, %xmm11

	pshufd	$0x4e, %xmm1, %xmm3
	mulps	%xmm0, %xmm1
	addps	%xmm1, %xmm12
	movaps	-20 * SIZE(BO), %xmm1
	mulps	%xmm0, %xmm3
	addps	%xmm3, %xmm13

	pshufd	$0x4e, %xmm1, %xmm3
	mulps	%xmm0, %xmm1
	addps	%xmm1, %xmm14
	movaps	-16 * SIZE(BO), %xmm1
	mulps	%xmm0, %xmm3
	movaps	-28 * SIZE(AO), %xmm0
	addps	%xmm3, %xmm15

	addq	$  4 * SIZE, AO
	subq	$-16 * SIZE, BO
	decq	%rax
	jg	.L17
	ALIGN_4

.L18:
	leaq	(LDC, LDC, 2), %rax

	mulps	%xmm7, %xmm8
	mulps	%xmm7, %xmm9
	mulps	%xmm7, %xmm10
	mulps	%xmm7, %xmm11

#ifndef TRMMKERNEL
	movsd	0 * SIZE(CO1), %xmm0
	movhps	2 * SIZE(CO1, LDC, 1), %xmm0
	movsd	0 * SIZE(CO1, LDC, 1), %xmm1
	movhps	2 * SIZE(CO1), %xmm1

	movsd	0 * SIZE(CO1, LDC, 2), %xmm2
	movhps	2 * SIZE(CO1, %rax), %xmm2
	movsd	0 * SIZE(CO1, %rax), %xmm3
	movhps	2 * SIZE(CO1, LDC, 2), %xmm3

	addps	%xmm0, %xmm8
	addps	%xmm1, %xmm9
	addps	%xmm2, %xmm10
	addps	%xmm3, %xmm11
#endif

	mulps	%xmm7, %xmm12
	mulps	%xmm7, %xmm13
	mulps	%xmm7, %xmm14
	mulps	%xmm7, %xmm15

#ifndef TRMMKERNEL
	movsd	0 * SIZE(CO2), %xmm4
	movhps	2 * SIZE(CO2, LDC, 1), %xmm4
	movsd	0 * SIZE(CO2, LDC, 1), %xmm5
	movhps	2 * SIZE(CO2), %xmm5

	movsd	0 * SIZE(CO2, LDC, 2), %xmm6
	movhps	2 * SIZE(CO2, %rax), %xmm6
	movsd	0 * SIZE(CO2, %rax), %xmm7
	movhps	2 * SIZE(CO2, LDC, 2), %xmm7

	addps	%xmm4, %xmm12
	addps	%xmm5, %xmm13
	addps	%xmm6, %xmm14
	addps	%xmm7, %xmm15
#endif

	movlps	%xmm8,  0 * SIZE(CO1)
	movhps	%xmm8,  2 * SIZE(CO1, LDC, 1)
	movlps	%xmm9,  0 * SIZE(CO1, LDC, 1)
	movhps	%xmm9,  2 * SIZE(CO1)

	movlps	%xmm10, 0 * SIZE(CO1, LDC, 2)
	movhps	%xmm10, 2 * SIZE(CO1, %rax)
	movlps	%xmm11, 0 * SIZE(CO1, %rax)
	movhps	%xmm11, 2 * SIZE(CO1, LDC, 2)

	movlps	%xmm12, 0 * SIZE(CO2)
	movhps	%xmm12, 2 * SIZE(CO2, LDC, 1)
	movlps	%xmm13, 0 * SIZE(CO2, LDC, 1)
	movhps	%xmm13, 2 * SIZE(CO2)

	movlps	%xmm14, 0 * SIZE(CO2, LDC, 2)
	movhps	%xmm14, 2 * SIZE(CO2, %rax)
	movlps	%xmm15, 0 * SIZE(CO2, %rax)
	movhps	%xmm15, 2 * SIZE(CO2, LDC, 2)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	salq	$BASE_SHIFT + 1, %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 8), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$4, KK
#endif

	addq	$4 * SIZE, CO1
	addq	$4 * SIZE, CO2
	decq	I
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
	salq	$BASE_SHIFT + 1, %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 8), BO
#endif

	movddup	-32 * SIZE(AO), %xmm0
	pxor	%xmm8, %xmm8
	movaps	-32 * SIZE(BO), %xmm1
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
	addq	$8, %rax
#endif
	movq	%rax, KKK
#endif
	sarq	$2, %rax
	NOBRANCH
	jle	.L26
	ALIGN_3

.L22:
	PREFETCH	(PREFETCHSIZE +  0)(AO)

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
	movddup	-30 * SIZE(AO), %xmm0

	mulps	%xmm0, %xmm1
	addps	%xmm1, %xmm8
	movaps	-12 * SIZE(BO), %xmm1
	mulps	%xmm0, %xmm1
	addps	%xmm1, %xmm9
	movaps	 -8 * SIZE(BO), %xmm1
	mulps	%xmm0, %xmm1
	mulps	 -4 * SIZE(BO), %xmm0
	addps	%xmm1, %xmm10
	movaps	  0 * SIZE(BO), %xmm1
	addps	%xmm0, %xmm11
	movddup	-28 * SIZE(AO), %xmm0

	mulps	%xmm0, %xmm1
	addps	%xmm1, %xmm8
	movaps	  4 * SIZE(BO), %xmm1
	mulps	%xmm0, %xmm1
	addps	%xmm1, %xmm9
	movaps	  8 * SIZE(BO), %xmm1
	mulps	%xmm0, %xmm1
	mulps	 12 * SIZE(BO), %xmm0
	addps	%xmm1, %xmm10
	movaps	 16 * SIZE(BO), %xmm1
	addps	%xmm0, %xmm11
	movddup	-26 * SIZE(AO), %xmm0

	mulps	%xmm0, %xmm1
	addps	%xmm1, %xmm8
	movaps	 20 * SIZE(BO), %xmm1
	mulps	%xmm0, %xmm1
	addps	%xmm1, %xmm9
	movaps	 24 * SIZE(BO), %xmm1
	mulps	%xmm0, %xmm1
	mulps	 28 * SIZE(BO), %xmm0
	addps	%xmm1, %xmm10
	movaps	 32 * SIZE(BO), %xmm1
	addps	%xmm0, %xmm11
	movddup	-24 * SIZE(AO), %xmm0

	subq	$-8 * SIZE, AO
	addq	$64 * SIZE, BO
	decq	%rax
	BRANCH
	jg	.L22

.L26:
	movaps	ALPHA, %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax
	je .L28
	ALIGN_4

.L27:
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
	movddup	-30 * SIZE(AO), %xmm0

	addq	$  2 * SIZE, AO
	subq	$-16 * SIZE, BO
	decq	%rax
	jg	.L27
	ALIGN_4

.L28:
	leaq	(LDC, LDC, 2), %rax

	mulps	%xmm7, %xmm8
	mulps	%xmm7, %xmm9
	mulps	%xmm7, %xmm10
	mulps	%xmm7, %xmm11

#ifndef TRMMKERNEL
	movsd	(CO1), %xmm0
	movhps	(CO1, LDC, 1), %xmm0
	movsd	(CO1, LDC, 2), %xmm1
	movhps	(CO1, %rax), %xmm1

	movsd	(CO2), %xmm2
	movhps	(CO2, LDC, 1), %xmm2
	movsd	(CO2, LDC, 2), %xmm3
	movhps	(CO2, %rax), %xmm3

	addps	%xmm0, %xmm8
	addps	%xmm1, %xmm9
	addps	%xmm2, %xmm10
	addps	%xmm3, %xmm11
#endif

	movlps	%xmm8,  (CO1)
	movhps	%xmm8,  (CO1, LDC, 1)

	movlps	%xmm9,  (CO1, LDC, 2)
	movhps	%xmm9,  (CO1, %rax)

	movlps	%xmm10, (CO2)
	movhps	%xmm10, (CO2, LDC, 1)

	movlps	%xmm11, (CO2, LDC, 2)
	movhps	%xmm11, (CO2, %rax)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	salq	$BASE_SHIFT + 1, %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 8), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$2, KK
#endif

	addq	$2 * SIZE, CO1
	addq	$2 * SIZE, CO2
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
	salq	$BASE_SHIFT, %rax
	leaq	(AO, %rax, 1), AO
	addq	%rax, %rax
	leaq	(BO, %rax, 8), BO
#endif

	movss	-32 * SIZE(AO), %xmm0
	pxor	%xmm8, %xmm8
	movaps	-32 * SIZE(BO), %xmm1
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
	addq	$8, %rax
#endif
	movq	%rax, KKK
#endif
	sarq	$2, %rax
	NOBRANCH
	jle	.L36
	ALIGN_3

.L32:
	PREFETCH	(PREFETCHSIZE +  0)(AO)

	shufps	$0, %xmm0, %xmm0
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
	movss	-31 * SIZE(AO), %xmm0

	shufps	$0, %xmm0, %xmm0
	mulps	%xmm0, %xmm1
	addps	%xmm1, %xmm8
	movaps	-12 * SIZE(BO), %xmm1
	mulps	%xmm0, %xmm1
	addps	%xmm1, %xmm9
	movaps	 -8 * SIZE(BO), %xmm1
	mulps	%xmm0, %xmm1
	mulps	 -4 * SIZE(BO), %xmm0
	addps	%xmm1, %xmm10
	movaps	  0 * SIZE(BO), %xmm1
	addps	%xmm0, %xmm11
	movss	-30 * SIZE(AO), %xmm0

	shufps	$0, %xmm0, %xmm0
	mulps	%xmm0, %xmm1
	addps	%xmm1, %xmm8
	movaps	  4 * SIZE(BO), %xmm1
	mulps	%xmm0, %xmm1
	addps	%xmm1, %xmm9
	movaps	  8 * SIZE(BO), %xmm1
	mulps	%xmm0, %xmm1
	mulps	 12 * SIZE(BO), %xmm0
	addps	%xmm1, %xmm10
	movaps	 16 * SIZE(BO), %xmm1
	addps	%xmm0, %xmm11
	movss	-29 * SIZE(AO), %xmm0

	shufps	$0, %xmm0, %xmm0
	mulps	%xmm0, %xmm1
	addps	%xmm1, %xmm8
	movaps	 20 * SIZE(BO), %xmm1
	mulps	%xmm0, %xmm1
	addps	%xmm1, %xmm9
	movaps	 24 * SIZE(BO), %xmm1
	mulps	%xmm0, %xmm1
	mulps	 28 * SIZE(BO), %xmm0
	addps	%xmm1, %xmm10
	movaps	 32 * SIZE(BO), %xmm1
	addps	%xmm0, %xmm11
	movss	-28 * SIZE(AO), %xmm0

	subq	$-4 * SIZE, AO
	addq	$64 * SIZE, BO
	decq	%rax
	BRANCH
	jg	.L32

.L36:
	movaps	ALPHA, %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax
	je .L38
	ALIGN_4

.L37:
	shufps	$0, %xmm0, %xmm0
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
	movss	-31 * SIZE(AO), %xmm0

	addq	$  1 * SIZE, AO
	subq	$-16 * SIZE, BO
	decq	%rax
	jg	.L37
	ALIGN_4

.L38:
	leaq	(LDC, LDC, 2), %rax

	mulps	%xmm7, %xmm8
	mulps	%xmm7, %xmm9
	mulps	%xmm7, %xmm10
	mulps	%xmm7, %xmm11

	movhlps	%xmm8,  %xmm12
	movhlps	%xmm9,  %xmm13
	movhlps	%xmm10, %xmm14
	movhlps	%xmm11, %xmm15

#ifndef TRMMKERNEL
	addss	(CO1), %xmm8
	addss	(CO1, LDC, 1), %xmm12
	addss	(CO1, LDC, 2), %xmm9
	addss	(CO1, %rax), %xmm13

	addss	(CO2), %xmm10
	addss	(CO2, LDC, 1), %xmm14
	addss	(CO2, LDC, 2), %xmm11
	addss	(CO2, %rax), %xmm15
#endif

	movss	%xmm8,  (CO1)
	movss	%xmm12, (CO1, LDC, 1)

	movss	%xmm9,  (CO1, LDC, 2)
	movss	%xmm13, (CO1, %rax)

	movss	%xmm10, (CO2)
	movss	%xmm14, (CO2, LDC, 1)

	movss	%xmm11, (CO2, LDC, 2)
	movss	%xmm15, (CO2, %rax)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	salq	$BASE_SHIFT, %rax
	leaq	(AO, %rax, 1), AO
	addq	%rax, %rax
	leaq	(BO, %rax, 8), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$1, KK
#endif
	ALIGN_4

.L39:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addl	$8, KK
#endif
	leaq	(C, LDC, 8), C
	decq	J
	jg	.L01
	ALIGN_4

.L40:
	testq	$4, N
	jle	.L80

#if defined(TRMMKERNEL) && defined(LEFT)
	movq	OFFSET, %rax
	movq	%rax, KK
#endif

	leaq	32 * SIZE + BUFFER, BO

	movaps	 0 * SIZE(B), %xmm1
	movaps	 4 * SIZE(B), %xmm3
	movaps	 8 * SIZE(B), %xmm5
	movaps	12 * SIZE(B), %xmm7

	movq	K, %rax
	sarq	$2, %rax
	jle	.L43
	ALIGN_4

.L42:
	PREFETCH	 (RPREFETCHSIZE +  0)  * SIZE(B)

	pshufd	 $0x50, %xmm1, %xmm0
	movaps	%xmm0, -32 * SIZE(BO)
	pshufd	 $0xfa, %xmm1, %xmm1
	movaps	%xmm1, -28 * SIZE(BO)

	movaps	16 * SIZE(B), %xmm1

	pshufd	 $0x50, %xmm3, %xmm2
	movaps	%xmm2, -24 * SIZE(BO)
	pshufd	 $0xfa, %xmm3, %xmm3
	movaps	%xmm3, -20 * SIZE(BO)

	movaps	20 * SIZE(B), %xmm3

	pshufd	 $0x50, %xmm5, %xmm4
	movaps	%xmm4, -16 * SIZE(BO)
	pshufd	 $0xfa, %xmm5, %xmm5
	movaps	%xmm5, -12 * SIZE(BO)

	movaps	24 * SIZE(B), %xmm5

	pshufd	 $0x50, %xmm7, %xmm6
	movaps	%xmm6,  -8 * SIZE(BO)
	pshufd	 $0xfa, %xmm7, %xmm7
	movaps	%xmm7,  -4 * SIZE(BO)

	movaps	28 * SIZE(B), %xmm7

	addq	$16 * SIZE, B
	addq	$32 * SIZE, BO

	decq	%rax
	jne	.L42
	ALIGN_4

.L43:
	movq	K, %rax
	andq	$3, %rax
	BRANCH
	jle	.L50
	ALIGN_4

.L45:
	pshufd	 $0x50, %xmm1, %xmm0
	movaps	%xmm0, -32 * SIZE(BO)
	pshufd	 $0xfa, %xmm1, %xmm1
	movaps	%xmm1, -28 * SIZE(BO)
	movaps	4 * SIZE(B), %xmm1

	addq	$ 4 * SIZE, B
	subq	$-8 * SIZE, BO
	decq	%rax
	jne	.L45
	ALIGN_4

.L50:
	movq	C, CO1
	leaq	(C, LDC, 2), CO2
	movq	A, AO

	movq	M,  I
	sarq	$2, I
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
	salq	$BASE_SHIFT + 1, %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 4), BO
#endif

	movaps	-32 * SIZE(AO), %xmm0
	movaps	-32 * SIZE(BO), %xmm1

	pxor	%xmm8, %xmm8
	PREFETCHW      3 * SIZE(CO1)
	pxor	%xmm9, %xmm9
	PREFETCHW      5 * SIZE(CO1, LDC)
	pxor	%xmm10, %xmm10
	PREFETCHW      3 * SIZE(CO2)
	pxor	%xmm11, %xmm11
	PREFETCHW      5 * SIZE(CO2, LDC)

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
	NOBRANCH
	jle	.L56
	ALIGN_3

.L52:
	PREFETCH	(PREFETCHSIZE +  0)(AO)

	pshufd	$0x4e, %xmm1, %xmm3
	mulps	%xmm0, %xmm1
	addps	%xmm1, %xmm8
	movaps	-28 * SIZE(BO), %xmm1
	mulps	%xmm0, %xmm3
	addps	%xmm3, %xmm9

	pshufd	$0x4e, %xmm1, %xmm3
	mulps	%xmm0, %xmm1
	addps	%xmm1, %xmm10
	movaps	-24 * SIZE(BO), %xmm1
	mulps	%xmm0, %xmm3
	movaps	-28 * SIZE(AO), %xmm0
	addps	%xmm3, %xmm11

	pshufd	$0x4e, %xmm1, %xmm3
	mulps	%xmm0, %xmm1
	addps	%xmm1, %xmm8
	movaps	-20 * SIZE(BO), %xmm1
	mulps	%xmm0, %xmm3
	addps	%xmm3, %xmm9

	pshufd	$0x4e, %xmm1, %xmm3
	mulps	%xmm0, %xmm1
	addps	%xmm1, %xmm10
	movaps	-16 * SIZE(BO), %xmm1
	mulps	%xmm0, %xmm3
	movaps	-24 * SIZE(AO), %xmm0
	addps	%xmm3, %xmm11

	pshufd	$0x4e, %xmm1, %xmm3
	mulps	%xmm0, %xmm1
	addps	%xmm1, %xmm8
	movaps	-12 * SIZE(BO), %xmm1
	mulps	%xmm0, %xmm3
	addps	%xmm3, %xmm9

	pshufd	$0x4e, %xmm1, %xmm3
	mulps	%xmm0, %xmm1
	addps	%xmm1, %xmm10
	movaps	 -8 * SIZE(BO), %xmm1
	mulps	%xmm0, %xmm3
	movaps	-20 * SIZE(AO), %xmm0
	addps	%xmm3, %xmm11

	pshufd	$0x4e, %xmm1, %xmm3
	mulps	%xmm0, %xmm1
	addps	%xmm1, %xmm8
	movaps	 -4 * SIZE(BO), %xmm1
	mulps	%xmm0, %xmm3
	addps	%xmm3, %xmm9

	pshufd	$0x4e, %xmm1, %xmm3
	mulps	%xmm0, %xmm1
	addps	%xmm1, %xmm10
	movaps	  0 * SIZE(BO), %xmm1
	mulps	%xmm0, %xmm3
	movaps	-16 * SIZE(AO), %xmm0
	addps	%xmm3, %xmm11

	subq	$-16 * SIZE, AO
	subq	$-32 * SIZE, BO
	decq	%rax
	BRANCH
	jg	.L52

.L56:
	movaps	ALPHA, %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax
	je .L58
	ALIGN_4

.L57:
	pshufd	$0x4e, %xmm1, %xmm3
	mulps	%xmm0, %xmm1
	addps	%xmm1, %xmm8
	movaps	-28 * SIZE(BO), %xmm1
	mulps	%xmm0, %xmm3
	addps	%xmm3, %xmm9

	pshufd	$0x4e, %xmm1, %xmm3
	mulps	%xmm0, %xmm1
	addps	%xmm1, %xmm10
	movaps	-24 * SIZE(BO), %xmm1
	mulps	%xmm0, %xmm3
	movaps	-28 * SIZE(AO), %xmm0
	addps	%xmm3, %xmm11

	addq	$ 4 * SIZE, AO
	subq	$-8 * SIZE, BO
	decq	%rax
	jg	.L57
	ALIGN_4

.L58:
	mulps	%xmm7, %xmm8
	mulps	%xmm7, %xmm9
	mulps	%xmm7, %xmm10
	mulps	%xmm7, %xmm11

#ifndef TRMMKERNEL
	movsd	0 * SIZE(CO1), %xmm0
	movhps	2 * SIZE(CO1, LDC), %xmm0
	movsd	0 * SIZE(CO1, LDC), %xmm1
	movhps	2 * SIZE(CO1), %xmm1

	movsd	0 * SIZE(CO2), %xmm2
	movhps	2 * SIZE(CO2, LDC), %xmm2
	movsd	0 * SIZE(CO2, LDC), %xmm3
	movhps	2 * SIZE(CO2), %xmm3

	addps	%xmm0, %xmm8
	addps	%xmm1, %xmm9
	addps	%xmm2, %xmm10
	addps	%xmm3, %xmm11
#endif

	movlps	%xmm8,  0 * SIZE(CO1)
	movhps	%xmm8,  2 * SIZE(CO1, LDC)
	movlps	%xmm9,  0 * SIZE(CO1, LDC)
	movhps	%xmm9,  2 * SIZE(CO1)

	movlps	%xmm10, 0 * SIZE(CO2)
	movhps	%xmm10, 2 * SIZE(CO2, LDC)
	movlps	%xmm11, 0 * SIZE(CO2, LDC)
	movhps	%xmm11, 2 * SIZE(CO2)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	salq	$BASE_SHIFT + 1, %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 4), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$4, KK
#endif

	addq	$4 * SIZE, CO1
	addq	$4 * SIZE, CO2
	decq	I
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
	salq	$BASE_SHIFT + 1, %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 4), BO
#endif

	movddup	-32 * SIZE(AO), %xmm0
	pxor	%xmm8, %xmm8
	movaps	-32 * SIZE(BO), %xmm1
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
	addq	$4, %rax
#endif
	movq	%rax, KKK
#endif
	sarq	$2, %rax
	NOBRANCH
	jle	.L66
	ALIGN_3

.L62:
	PREFETCH	(PREFETCHSIZE +  0)(AO)

	mulps	%xmm0, %xmm1
	mulps	-28 * SIZE(BO), %xmm0
	addps	%xmm1, %xmm8
	movaps	-24 * SIZE(BO), %xmm1
	addps	%xmm0, %xmm9
	movddup	-30 * SIZE(AO), %xmm0

	mulps	%xmm0, %xmm1
	mulps	-20 * SIZE(BO), %xmm0
	addps	%xmm1, %xmm8
	movaps	-16 * SIZE(BO), %xmm1
	addps	%xmm0, %xmm9
	movddup	-28 * SIZE(AO), %xmm0

	mulps	%xmm0, %xmm1
	mulps	-12 * SIZE(BO), %xmm0
	addps	%xmm1, %xmm8
	movaps	 -8 * SIZE(BO), %xmm1
	addps	%xmm0, %xmm9
	movddup	-26 * SIZE(AO), %xmm0

	mulps	%xmm0, %xmm1
	mulps	 -4 * SIZE(BO), %xmm0
	addps	%xmm1, %xmm8
	movaps	  0 * SIZE(BO), %xmm1
	addps	%xmm0, %xmm9
	movddup	-24 * SIZE(AO), %xmm0

	subq	$ -8 * SIZE, AO
	subq	$-32 * SIZE, BO
	decq	%rax
	BRANCH
	jg	.L62

.L66:
	movaps	ALPHA, %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax
	je .L68
	ALIGN_4

.L67:
	mulps	%xmm0, %xmm1
	mulps	-28 * SIZE(BO), %xmm0
	addps	%xmm1, %xmm8
	movaps	-24 * SIZE(BO), %xmm1
	addps	%xmm0, %xmm9
	movddup	-30 * SIZE(AO), %xmm0

	addq	$ 2 * SIZE, AO
	subq	$-8 * SIZE, BO
	decq	%rax
	jg	.L67
	ALIGN_4

.L68:
	mulps	%xmm7, %xmm8
	mulps	%xmm7, %xmm9

#ifndef TRMMKERNEL
	movsd	(CO1), %xmm0
	movhps	(CO1, LDC), %xmm0

	movsd	(CO2), %xmm1
	movhps	(CO2, LDC), %xmm1

	addps	%xmm0, %xmm8
	addps	%xmm1, %xmm9
#endif

	movlps	%xmm8,  (CO1)
	movhps	%xmm8,  (CO1, LDC)

	movlps	%xmm9,  (CO2)
	movhps	%xmm9,  (CO2, LDC)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	salq	$BASE_SHIFT + 1, %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 4), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$2, KK
#endif

	addq	$2 * SIZE, CO1
	addq	$2 * SIZE, CO2
	ALIGN_4

.L70:
	testq	$1, M
	je	.L79

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	32 * SIZE + BUFFER, BO
#else
	leaq	32 * SIZE + BUFFER, BO
	movq	KK, %rax
	salq	$BASE_SHIFT, %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 8), BO
#endif

	movss	-32 * SIZE(AO), %xmm0
	pxor	%xmm8, %xmm8
	movaps	-32 * SIZE(BO), %xmm1
	pxor	%xmm9, %xmm9

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
	NOBRANCH
	jle	.L76
	ALIGN_3

.L72:
	PREFETCH	(PREFETCHSIZE +  0)(AO)

	shufps	$0, %xmm0, %xmm0
	mulps	%xmm0, %xmm1
	mulps	-28 * SIZE(BO), %xmm0
	addps	%xmm1, %xmm8
	movaps	-24 * SIZE(BO), %xmm1
	addps	%xmm0, %xmm9
	movss	-31 * SIZE(AO), %xmm0

	shufps	$0, %xmm0, %xmm0
	mulps	%xmm0, %xmm1
	mulps	-20 * SIZE(BO), %xmm0
	addps	%xmm1, %xmm8
	movaps	-16 * SIZE(BO), %xmm1
	addps	%xmm0, %xmm9
	movss	-30 * SIZE(AO), %xmm0

	shufps	$0, %xmm0, %xmm0
	mulps	%xmm0, %xmm1
	mulps	-12 * SIZE(BO), %xmm0
	addps	%xmm1, %xmm8
	movaps	 -8 * SIZE(BO), %xmm1
	addps	%xmm0, %xmm9
	movss	-29 * SIZE(AO), %xmm0

	shufps	$0, %xmm0, %xmm0
	mulps	%xmm0, %xmm1
	mulps	 -4 * SIZE(BO), %xmm0
	addps	%xmm1, %xmm8
	movaps	  0 * SIZE(BO), %xmm1
	addps	%xmm0, %xmm9
	movss	-28 * SIZE(AO), %xmm0

	subq	$ -4 * SIZE, AO
	subq	$-32 * SIZE, BO
	decq	%rax
	BRANCH
	jg	.L72

.L76:
	movaps	ALPHA, %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax
	je .L78
	ALIGN_4

.L77:
	shufps	$0, %xmm0, %xmm0
	mulps	%xmm0, %xmm1
	mulps	-28 * SIZE(BO), %xmm0
	addps	%xmm1, %xmm8
	movaps	-24 * SIZE(BO), %xmm1
	addps	%xmm0, %xmm9
	movss	-31 * SIZE(AO), %xmm0

	addq	$ 1 * SIZE, AO
	subq	$-8 * SIZE, BO
	decq	%rax
	jg	.L77
	ALIGN_4

.L78:
	mulps	%xmm7, %xmm8
	mulps	%xmm7, %xmm9

	movhlps	%xmm8,  %xmm10
	movhlps	%xmm9,  %xmm11

#ifndef TRMMKERNEL
	addss	(CO1), %xmm8
	addss	(CO1, LDC), %xmm10
	addss	(CO2), %xmm9
	addss	(CO2, LDC), %xmm11
#endif

	movss	%xmm8,  (CO1)
	movss	%xmm10, (CO1, LDC)

	movss	%xmm9,  (CO2)
	movss	%xmm11, (CO2, LDC)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	salq	$BASE_SHIFT, %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 8), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$1, KK
#endif
	ALIGN_4

.L79:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addl	$4, KK
#endif
	leaq	(C, LDC, 4), C
	ALIGN_4

.L80:
	testq	$2, N
	jle	.L120

#if defined(TRMMKERNEL) && defined(LEFT)
	movq	OFFSET, %rax
	movq	%rax, KK
#endif

	leaq	32 * SIZE + BUFFER, BO

	movaps	 0 * SIZE(B), %xmm1
	movaps	 4 * SIZE(B), %xmm3

	movq	K, %rax
	sarq	$2, %rax
	jle	.L83
	ALIGN_4

.L82:
	pshufd	 $0x50, %xmm1, %xmm0
	movaps	%xmm0, -32 * SIZE(BO)
	pshufd	 $0xfa, %xmm1, %xmm1
	movaps	%xmm1, -28 * SIZE(BO)

	movaps	 8 * SIZE(B), %xmm1

	pshufd	 $0x50, %xmm3, %xmm2
	movaps	%xmm2, -24 * SIZE(BO)
	pshufd	 $0xfa, %xmm3, %xmm3
	movaps	%xmm3, -20 * SIZE(BO)

	movaps	12 * SIZE(B), %xmm3

	addq	$  8 * SIZE, B
	subq	$-16 * SIZE, BO

	decq	%rax
	jne	.L82
	ALIGN_4

.L83:
	movq	K, %rax
	andq	$3, %rax
	BRANCH
	jle	.L90
	ALIGN_4

.L85:
	pshufd	 $0x50, %xmm1, %xmm0
	movaps	%xmm0, -32 * SIZE(BO)
	movsd	2 * SIZE(B), %xmm1

	addq	$ 2 * SIZE, B
	subq	$-4 * SIZE, BO
	decq	%rax
	jne	.L85
	ALIGN_4

.L90:
	movq	C, CO1
	leaq	(C, LDC), CO2
	movq	A, AO

	movq	M,  I
	sarq	$2, I
	jle	.L100
	ALIGN_4

.L91:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	32 * SIZE + BUFFER, BO
#else
	leaq	32 * SIZE + BUFFER, BO
	movq	KK, %rax
	salq	$BASE_SHIFT, %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 4), BO
#endif

	movaps	-32 * SIZE(AO), %xmm0
	movaps	-32 * SIZE(BO), %xmm1

	pxor	%xmm8, %xmm8
	PREFETCHW      3 * SIZE(CO1)
	pxor	%xmm9, %xmm9
	PREFETCHW      3 * SIZE(CO2)

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
	NOBRANCH
	jle	.L96
	ALIGN_3

.L92:
	PREFETCH	(PREFETCHSIZE +  0)(AO)

	pshufd	$0x4e, %xmm1, %xmm3
	mulps	%xmm0, %xmm1
	addps	%xmm1, %xmm8
	movaps	-28 * SIZE(BO), %xmm1
	mulps	%xmm0, %xmm3
	addps	%xmm3, %xmm9
	movaps	-28 * SIZE(AO), %xmm0

	pshufd	$0x4e, %xmm1, %xmm3
	mulps	%xmm0, %xmm1
	addps	%xmm1, %xmm8
	movaps	-24 * SIZE(BO), %xmm1
	mulps	%xmm0, %xmm3
	addps	%xmm3, %xmm9
	movaps	-24 * SIZE(AO), %xmm0

	pshufd	$0x4e, %xmm1, %xmm3
	mulps	%xmm0, %xmm1
	addps	%xmm1, %xmm8
	movaps	-20 * SIZE(BO), %xmm1
	mulps	%xmm0, %xmm3
	addps	%xmm3, %xmm9
	movaps	-20 * SIZE(AO), %xmm0

	pshufd	$0x4e, %xmm1, %xmm3
	mulps	%xmm0, %xmm1
	addps	%xmm1, %xmm8
	movaps	-16 * SIZE(BO), %xmm1
	mulps	%xmm0, %xmm3
	addps	%xmm3, %xmm9
	movaps	-16 * SIZE(AO), %xmm0

	subq	$-16 * SIZE, AO
	subq	$-16 * SIZE, BO
	decq	%rax
	BRANCH
	jg	.L92

.L96:
	movaps	ALPHA, %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax
	je .L98
	ALIGN_4

.L97:
	pshufd	$0x4e, %xmm1, %xmm3
	mulps	%xmm0, %xmm1
	addps	%xmm1, %xmm8
	movaps	-28 * SIZE(BO), %xmm1
	mulps	%xmm0, %xmm3
	addps	%xmm3, %xmm9
	movaps	-28 * SIZE(AO), %xmm0

	addq	$ 4 * SIZE, AO
	subq	$-4 * SIZE, BO
	decq	%rax
	jg	.L97
	ALIGN_4

.L98:
	mulps	%xmm7, %xmm8
	mulps	%xmm7, %xmm9

#ifndef TRMMKERNEL
	movsd	0 * SIZE(CO1), %xmm0
	movhps	2 * SIZE(CO2), %xmm0
	movsd	0 * SIZE(CO2), %xmm1
	movhps	2 * SIZE(CO1), %xmm1

	addps	%xmm0, %xmm8
	addps	%xmm1, %xmm9
#endif

	movlps	%xmm8,  0 * SIZE(CO1)
	movhps	%xmm8,  2 * SIZE(CO2)
	movlps	%xmm9,  0 * SIZE(CO2)
	movhps	%xmm9,  2 * SIZE(CO1)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	salq	$BASE_SHIFT, %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 4), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$4, KK
#endif

	addq	$4 * SIZE, CO1
	addq	$4 * SIZE, CO2
	decq	I
	jg	.L91
	ALIGN_4

.L100:
	testq	$2, M
	je	.L110

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	32 * SIZE + BUFFER, BO
#else
	leaq	32 * SIZE + BUFFER, BO
	movq	KK, %rax
	salq	$BASE_SHIFT, %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 4), BO
#endif

	movddup	-32 * SIZE(AO), %xmm0
	pxor	%xmm8, %xmm8
	movaps	-32 * SIZE(BO), %xmm1

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
	NOBRANCH
	jle	.L106
	ALIGN_3

.L102:
	PREFETCH	(PREFETCHSIZE +  0)(AO)

	mulps	%xmm0, %xmm1
	movddup	-30 * SIZE(AO), %xmm0
	addps	%xmm1, %xmm8
	movaps	-28 * SIZE(BO), %xmm1

	mulps	%xmm0, %xmm1
	movddup	-28 * SIZE(AO), %xmm0
	addps	%xmm1, %xmm8
	movaps	-24 * SIZE(BO), %xmm1

	mulps	%xmm0, %xmm1
	movddup	-26 * SIZE(AO), %xmm0
	addps	%xmm1, %xmm8
	movaps	-20 * SIZE(BO), %xmm1

	mulps	%xmm0, %xmm1
	movddup	-24 * SIZE(AO), %xmm0
	addps	%xmm1, %xmm8
	movaps	-16 * SIZE(BO), %xmm1

	subq	$ -8 * SIZE, AO
	subq	$-16 * SIZE, BO
	decq	%rax
	BRANCH
	jg	.L102

.L106:
	movaps	ALPHA, %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax
	je .L108
	ALIGN_4

.L107:
	mulps	%xmm0, %xmm1
	movddup	-30 * SIZE(AO), %xmm0
	addps	%xmm1, %xmm8
	movaps	-28 * SIZE(BO), %xmm1

	addq	$ 2 * SIZE, AO
	subq	$-4 * SIZE, BO
	decq	%rax
	jg	.L107
	ALIGN_4

.L108:
	mulps	%xmm7, %xmm8

#ifndef TRMMKERNEL
	movsd	(CO1), %xmm0
	movhps	(CO2), %xmm0

	addps	%xmm0, %xmm8
#endif

	movlps	%xmm8,  (CO1)
	movhps	%xmm8,  (CO2)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	salq	$BASE_SHIFT, %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 4), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$2, KK
#endif

	addq	$2 * SIZE, CO1
	addq	$2 * SIZE, CO2
	ALIGN_4

.L110:
	testq	$1, M
	je	.L119

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	32 * SIZE + BUFFER, BO
#else
	leaq	32 * SIZE + BUFFER, BO
	movq	KK, %rax
	salq	$BASE_SHIFT, %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 4), BO
#endif

	movss	-32 * SIZE(AO), %xmm0
	pxor	%xmm8, %xmm8
	movaps	-32 * SIZE(BO), %xmm1

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
	NOBRANCH
	jle	.L116
	ALIGN_3

.L112:
	PREFETCH	(PREFETCHSIZE +  0)(AO)

	shufps	$0, %xmm0, %xmm0
	mulps	%xmm0, %xmm1
	movss	-31 * SIZE(AO), %xmm0
	addps	%xmm1, %xmm8
	movaps	-28 * SIZE(BO), %xmm1

	shufps	$0, %xmm0, %xmm0
	mulps	%xmm0, %xmm1
	movss	-30 * SIZE(AO), %xmm0
	addps	%xmm1, %xmm8
	movaps	-24 * SIZE(BO), %xmm1

	shufps	$0, %xmm0, %xmm0
	mulps	%xmm0, %xmm1
	movss	-29 * SIZE(AO), %xmm0
	addps	%xmm1, %xmm8
	movaps	-20 * SIZE(BO), %xmm1

	shufps	$0, %xmm0, %xmm0
	mulps	%xmm0, %xmm1
	movss	-28 * SIZE(AO), %xmm0
	addps	%xmm1, %xmm8
	movaps	-16 * SIZE(BO), %xmm1

	subq	$ -4 * SIZE, AO
	subq	$-16 * SIZE, BO
	decq	%rax
	BRANCH
	jg	.L112

.L116:
	movaps	ALPHA, %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax
	je .L118
	ALIGN_4

.L117:
	shufps	$0, %xmm0, %xmm0
	mulps	%xmm0, %xmm1
	movss	-31 * SIZE(AO), %xmm0
	addps	%xmm1, %xmm8
	movaps	-28 * SIZE(BO), %xmm1

	addq	$ 1 * SIZE, AO
	subq	$-4 * SIZE, BO
	decq	%rax
	jg	.L117
	ALIGN_4

.L118:
	mulps	%xmm7, %xmm8
	movhlps	%xmm8, %xmm9

#ifndef TRMMKERNEL
	addss	(CO1), %xmm8
	addss	(CO2), %xmm9
#endif

	movss	%xmm8,  (CO1)
	movss	%xmm9,  (CO2)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	salq	$BASE_SHIFT, %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 4), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$1, KK
#endif
	ALIGN_4

.L119:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addl	$2, KK
#endif
	leaq	(C, LDC, 2), C
	ALIGN_4

.L120:
	testq	$1, N
	jle	.L999

#if defined(TRMMKERNEL) && defined(LEFT)
	movq	OFFSET, %rax
	movq	%rax, KK
#endif

	leaq	32 * SIZE + BUFFER, BO

	movsd	 0 * SIZE(B), %xmm1
	movhps	 2 * SIZE(B), %xmm1

	movq	K, %rax
	sarq	$2, %rax
	jle	.L123
	ALIGN_4

.L122:
	pshufd	 $0x50, %xmm1, %xmm0
	movaps	%xmm0, -32 * SIZE(BO)
	pshufd	 $0xfa, %xmm1, %xmm1
	movaps	%xmm1, -28 * SIZE(BO)

	movsd	 4 * SIZE(B), %xmm1
	movhps	 6 * SIZE(B), %xmm1

	addq	$ 4 * SIZE, B
	subq	$-8 * SIZE, BO

	decq	%rax
	jne	.L122
	ALIGN_4

.L123:
	movq	K, %rax
	andq	$3, %rax
	BRANCH
	jle	.L130
	ALIGN_4

.L125:
	pshufd	 $0x50, %xmm1, %xmm0
	movlps	%xmm0, -32 * SIZE(BO)
	movss	1 * SIZE(B), %xmm1

	addq	$ 1 * SIZE, B
	subq	$-2 * SIZE, BO
	decq	%rax
	jne	.L125
	ALIGN_4

.L130:
	movq	C, CO1
	movq	A, AO

	movq	M,  I
	sarq	$2, I
	jle	.L140
	ALIGN_4

.L131:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	32 * SIZE + BUFFER, BO
#else
	leaq	32 * SIZE + BUFFER, BO
	movq	KK, %rax
	salq	$BASE_SHIFT, %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 2), BO
#endif

	movaps	-32 * SIZE(AO), %xmm0
	movddup	-32 * SIZE(BO), %xmm1

	pxor	%xmm8, %xmm8
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
	addq	$4, %rax
#else
	addq	$1, %rax
#endif
	movq	%rax, KKK
#endif
	sarq	$2, %rax
	NOBRANCH
	jle	.L136
	ALIGN_3

.L132:
	PREFETCH	(PREFETCHSIZE +  0)(AO)

	mulps	%xmm0, %xmm1
	movaps	-28 * SIZE(AO), %xmm0
	addps	%xmm1, %xmm8
	movddup	-30 * SIZE(BO), %xmm1

	mulps	%xmm0, %xmm1
	movaps	-24 * SIZE(AO), %xmm0
	addps	%xmm1, %xmm8
	movddup	-28 * SIZE(BO), %xmm1

	mulps	%xmm0, %xmm1
	movaps	-20 * SIZE(AO), %xmm0
	addps	%xmm1, %xmm8
	movddup	-26 * SIZE(BO), %xmm1

	mulps	%xmm0, %xmm1
	movaps	-16 * SIZE(AO), %xmm0
	addps	%xmm1, %xmm8
	movddup	-24 * SIZE(BO), %xmm1

	subq	$-16 * SIZE, AO
	subq	$ -8 * SIZE, BO
	decq	%rax
	BRANCH
	jg	.L132

.L136:
	movaps	ALPHA, %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax
	je .L138
	ALIGN_4

.L137:
	mulps	%xmm0, %xmm1
	movaps	-28 * SIZE(AO), %xmm0
	addps	%xmm1, %xmm8
	movddup	-30 * SIZE(BO), %xmm1

	addq	$ 4 * SIZE, AO
	subq	$-2 * SIZE, BO
	decq	%rax
	jg	.L137
	ALIGN_4

.L138:
	mulps	%xmm7, %xmm8

#ifndef TRMMKERNEL
	movsd	0 * SIZE(CO1), %xmm0
	movhps	2 * SIZE(CO1), %xmm0

	addps	%xmm0, %xmm8
#endif

	movlps	%xmm8,  0 * SIZE(CO1)
	movhps	%xmm8,  2 * SIZE(CO1)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	salq	$BASE_SHIFT, %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 2), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$4, KK
#endif

	addq	$4 * SIZE, CO1
	decq	I
	jg	.L131
	ALIGN_4

.L140:
	testq	$2, M
	je	.L150

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	32 * SIZE + BUFFER, BO
#else
	leaq	32 * SIZE + BUFFER, BO
	movq	KK, %rax
	salq	$BASE_SHIFT, %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 2), BO
#endif

	movddup	-32 * SIZE(AO), %xmm0
	pxor	%xmm8, %xmm8
	movaps	-32 * SIZE(BO), %xmm1

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
	NOBRANCH
	jle	.L146
	ALIGN_3

.L142:
	PREFETCH	(PREFETCHSIZE +  0)(AO)

	mulps	%xmm0, %xmm1
	movddup	-30 * SIZE(AO), %xmm0
	addps	%xmm1, %xmm8
	movsd	-30 * SIZE(BO), %xmm1

	mulps	%xmm0, %xmm1
	movddup	-28 * SIZE(AO), %xmm0
	addps	%xmm1, %xmm8
	movsd	-28 * SIZE(BO), %xmm1

	mulps	%xmm0, %xmm1
	movddup	-26 * SIZE(AO), %xmm0
	addps	%xmm1, %xmm8
	movsd	-26 * SIZE(BO), %xmm1

	mulps	%xmm0, %xmm1
	movddup	-24 * SIZE(AO), %xmm0
	addps	%xmm1, %xmm8
	movsd	-24 * SIZE(BO), %xmm1

	subq	$-8 * SIZE, AO
	subq	$-8 * SIZE, BO
	decq	%rax
	BRANCH
	jg	.L142

.L146:
	movaps	ALPHA, %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax
	je .L148
	ALIGN_4

.L147:
	mulps	%xmm0, %xmm1
	movddup	-30 * SIZE(AO), %xmm0
	addps	%xmm1, %xmm8
	movsd	-30 * SIZE(BO), %xmm1

	addq	$ 2 * SIZE, AO
	subq	$-2 * SIZE, BO
	decq	%rax
	jg	.L147
	ALIGN_4

.L148:
	mulps	%xmm7, %xmm8

#ifndef TRMMKERNEL
	movsd	(CO1), %xmm0

	addps	%xmm0, %xmm8
#endif

	movlps	%xmm8,  (CO1)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	salq	$BASE_SHIFT, %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 2), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$2, KK
#endif

	addq	$2 * SIZE, CO1
	ALIGN_4

.L150:
	testq	$1, M
	je	.L999

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	32 * SIZE + BUFFER, BO
#else
	leaq	32 * SIZE + BUFFER, BO
	movq	KK, %rax
	salq	$BASE_SHIFT, %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 2), BO
#endif

	movss	-32 * SIZE(AO), %xmm0
	pxor	%xmm8, %xmm8
	movss	-32 * SIZE(BO), %xmm1

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
	NOBRANCH
	jle	.L156
	ALIGN_3

.L152:
	PREFETCH	(PREFETCHSIZE +  0)(AO)

	mulss	%xmm0, %xmm1
	movss	-31 * SIZE(AO), %xmm0
	addss	%xmm1, %xmm8
	movss	-30 * SIZE(BO), %xmm1

	mulss	%xmm0, %xmm1
	movss	-30 * SIZE(AO), %xmm0
	addss	%xmm1, %xmm8
	movss	-28 * SIZE(BO), %xmm1

	mulss	%xmm0, %xmm1
	movss	-29 * SIZE(AO), %xmm0
	addss	%xmm1, %xmm8
	movss	-26 * SIZE(BO), %xmm1

	mulss	%xmm0, %xmm1
	movss	-28 * SIZE(AO), %xmm0
	addss	%xmm1, %xmm8
	movss	-24 * SIZE(BO), %xmm1

	subq	$-4 * SIZE, AO
	subq	$-8 * SIZE, BO
	decq	%rax
	BRANCH
	jg	.L152

.L156:
	movaps	ALPHA, %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax
	je .L158
	ALIGN_4

.L157:
	mulss	%xmm0, %xmm1
	movss	-31 * SIZE(AO), %xmm0
	addss	%xmm1, %xmm8
	movss	-30 * SIZE(BO), %xmm1

	addq	$1 * SIZE, AO
	addq	$2 * SIZE, BO
	decq	%rax
	jg	.L157
	ALIGN_4

.L158:
	mulss	%xmm7, %xmm8

#ifndef TRMMKERNEL
	addss	(CO1), %xmm8
#endif

	movss	%xmm8,  (CO1)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	salq	$BASE_SHIFT, %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 2), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$1, KK
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
