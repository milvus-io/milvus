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
#define OLD_K	%rdx

#define M	%r13
#define N	%r14
#define K	%r15

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

#define PREA	%rdx

#ifndef WINDOWS_ABI

#define STACKSIZE 128

#define OLD_LDC		 8 + STACKSIZE(%rsp)
#define OLD_OFFSET	16 + STACKSIZE(%rsp)

#define ALPHA_R	   48(%rsp)
#define ALPHA_I	   56(%rsp)
#define J	   64(%rsp)
#define OFFSET	   72(%rsp)
#define KK	   80(%rsp)
#define KKK	   88(%rsp)

#else

#define STACKSIZE 512

#define OLD_ALPHA_I	40 + STACKSIZE(%rsp)
#define OLD_A		48 + STACKSIZE(%rsp)
#define OLD_B		56 + STACKSIZE(%rsp)
#define OLD_C		64 + STACKSIZE(%rsp)
#define OLD_LDC		72 + STACKSIZE(%rsp)
#define OLD_OFFSET	80 + STACKSIZE(%rsp)

#define ALPHA_R	  224(%rsp)
#define ALPHA_I	  232(%rsp)
#define J	  240(%rsp)
#define OFFSET	  248(%rsp)
#define KK	  256(%rsp)
#define KKK	  264(%rsp)

#endif

#define PREFETCHSIZE  (8 * 17 + 4)
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
	movq	ARG3,      OLD_K
	movq	OLD_A,     A
	movq	OLD_B,     B
	movq	OLD_C,     C
	movq	OLD_LDC,   LDC
#ifdef TRMMKERNEL
	movq	OLD_OFFSET, %r11
#endif
	movaps	%xmm3, %xmm0
	movss	OLD_ALPHA_I, %xmm1
#else
	movq	OLD_LDC,   LDC
#ifdef TRMMKERNEL
	movq	OLD_OFFSET, %r11
#endif
#endif

	unpcklps %xmm1, %xmm0

	movlps	 %xmm0, ALPHA_R
	movlps	 %xmm0, ALPHA_I

	subq	$-32 * SIZE, A
	subq	$-32 * SIZE, B

	movq	OLD_M, M
	movq	OLD_N, N
	movq	OLD_K, K

	salq	$ZBASE_SHIFT, LDC

	movq	N,  J
	sarq	$2, J
	NOBRANCH
	jle	.L50
	ALIGN_4

.L10:
#if defined(TRMMKERNEL) && defined(LEFT)
        movq    OFFSET, %rax
	movq    %rax, KK
#endif

	movq	C, CO1
	leaq	(C, LDC, 1), CO2
	movq	A, AO

	movq	K, %rax
	salq	$BASE_SHIFT + 2, %rax
	leaq	(B, %rax), BB

	movq	M,  I
	sarq	$3, I
	NOBRANCH
	jle	.L20
	ALIGN_4

.L11:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	movq	B, BO
#else
	movq	B, BO

	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 8), AO
	leaq	(BO, %rax, 4), BO
#endif

	movaps	-32 * SIZE(AO), %xmm0
	xorpd	%xmm3, %xmm3
	movaps	-28 * SIZE(AO), %xmm1
	xorpd	%xmm4, %xmm4
	movaps	-32 * SIZE(BO), %xmm2

	xorpd	%xmm5, %xmm5
	prefetcht0	 -32 * SIZE(BB)
	xorpd	%xmm6, %xmm6

	prefetcht2     7 * SIZE(CO1)
	movapd	%xmm4, %xmm8
	movapd	%xmm4, %xmm9
	prefetcht2     7 * SIZE(CO2)
	movapd	%xmm4, %xmm10
	movapd	%xmm4, %xmm11

	prefetcht2     7 * SIZE(CO1, LDC, 2)
	movapd	%xmm4, %xmm12
	movaps	%xmm4, %xmm13
	prefetcht2     7 * SIZE(CO2, LDC, 2)
	movaps	%xmm4, %xmm14
	movaps	%xmm4, %xmm15

	subq		$-24 * SIZE, BB

	leaq		(PREFETCHSIZE +  0) * SIZE(AO), PREA

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
	sarq	$3, %rax
	NOBRANCH
	jle	.L15
	ALIGN_3

.L12:
	PREFETCH -32 * SIZE(PREA)
	addps	%xmm6, %xmm10
	addps	%xmm3, %xmm14
	movaps	%xmm2, %xmm3
	pshufd	$0x39, %xmm2, %xmm7
	mulps	%xmm0, %xmm2
	mulps	%xmm1, %xmm3

	addps	%xmm4, %xmm11
	addps	%xmm5, %xmm15
	movaps	%xmm7, %xmm5
	pshufd	$0x39, %xmm7, %xmm6
	mulps	%xmm0, %xmm7
	mulps	%xmm1, %xmm5

	addps	%xmm2, %xmm8
	movaps	-28 * SIZE(BO), %xmm2
	addps	%xmm3, %xmm12
	movaps	%xmm6, %xmm3
	pshufd	$0x39, %xmm6, %xmm4
	mulps	%xmm0, %xmm6
	mulps	%xmm1, %xmm3

	addps	%xmm7, %xmm9
	addps	%xmm5, %xmm13
	movaps	%xmm4, %xmm5
	mulps	%xmm0, %xmm4
	movaps	-24 * SIZE(AO), %xmm0
	mulps	%xmm1, %xmm5
	movaps	-20 * SIZE(AO), %xmm1

	addps	%xmm6, %xmm10
	addps	%xmm3, %xmm14
	movaps	%xmm2, %xmm3
	pshufd	$0x39, %xmm2, %xmm7
	mulps	%xmm0, %xmm2
	mulps	%xmm1, %xmm3

	addps	%xmm4, %xmm11
	addps	%xmm5, %xmm15
	movaps	%xmm7, %xmm5
	pshufd	$0x39, %xmm7, %xmm6
	mulps	%xmm0, %xmm7
	mulps	%xmm1, %xmm5

	addps	%xmm2, %xmm8
	movaps	-24 * SIZE(BO), %xmm2
	addps	%xmm3, %xmm12
	movaps	%xmm6, %xmm3
	pshufd	$0x39, %xmm6, %xmm4
	mulps	%xmm0, %xmm6
	mulps	%xmm1, %xmm3

	addps	%xmm7, %xmm9
	addps	%xmm5, %xmm13
	movaps	%xmm4, %xmm5
	mulps	%xmm0, %xmm4
	movaps	-16 * SIZE(AO), %xmm0
	mulps	%xmm1, %xmm5
	movaps	-12 * SIZE(AO), %xmm1

	addps	%xmm6, %xmm10
	addps	%xmm3, %xmm14
	movaps	%xmm2, %xmm3
	pshufd	$0x39, %xmm2, %xmm7
	mulps	%xmm0, %xmm2
	mulps	%xmm1, %xmm3

	addps	%xmm4, %xmm11
	addps	%xmm5, %xmm15
	PREFETCH -16 * SIZE(PREA)
	movaps	%xmm7, %xmm5
	pshufd	$0x39, %xmm7, %xmm6
	mulps	%xmm0, %xmm7
	mulps	%xmm1, %xmm5

	addps	%xmm2, %xmm8
	movaps	-20 * SIZE(BO), %xmm2
	addps	%xmm3, %xmm12
	movaps	%xmm6, %xmm3
	pshufd	$0x39, %xmm6, %xmm4
	mulps	%xmm0, %xmm6
	mulps	%xmm1, %xmm3

	addps	%xmm7, %xmm9
	addps	%xmm5, %xmm13
	movaps	%xmm4, %xmm5
	mulps	%xmm0, %xmm4
	movaps	 -8 * SIZE(AO), %xmm0
	mulps	%xmm1, %xmm5
	movaps	 -4 * SIZE(AO), %xmm1

	addps	%xmm6, %xmm10
	addps	%xmm3, %xmm14
	movaps	%xmm2, %xmm3
	pshufd	$0x39, %xmm2, %xmm7
	mulps	%xmm0, %xmm2
	mulps	%xmm1, %xmm3

	addps	%xmm4, %xmm11
	addps	%xmm5, %xmm15
	movaps	%xmm7, %xmm5
	pshufd	$0x39, %xmm7, %xmm6
	mulps	%xmm0, %xmm7
	mulps	%xmm1, %xmm5

	addps	%xmm2, %xmm8
	movaps	-16 * SIZE(BO), %xmm2
	addps	%xmm3, %xmm12
	movaps	%xmm6, %xmm3
	pshufd	$0x39, %xmm6, %xmm4
	mulps	%xmm0, %xmm6
	mulps	%xmm1, %xmm3

	addps	%xmm7, %xmm9
	addps	%xmm5, %xmm13
	movaps	%xmm4, %xmm5
	mulps	%xmm0, %xmm4
	movaps	  0 * SIZE(AO), %xmm0
	mulps	%xmm1, %xmm5
	movaps	  4 * SIZE(AO), %xmm1

	addps	%xmm6, %xmm10
	addps	%xmm3, %xmm14
	PREFETCH  0 * SIZE(PREA)
	movaps	%xmm2, %xmm3
	pshufd	$0x39, %xmm2, %xmm7
	mulps	%xmm0, %xmm2
	mulps	%xmm1, %xmm3

	addps	%xmm4, %xmm11
	addps	%xmm5, %xmm15
	movaps	%xmm7, %xmm5
	pshufd	$0x39, %xmm7, %xmm6
	mulps	%xmm0, %xmm7
	mulps	%xmm1, %xmm5

	addps	%xmm2, %xmm8
	movaps	-12 * SIZE(BO), %xmm2
	addps	%xmm3, %xmm12
	movaps	%xmm6, %xmm3
	pshufd	$0x39, %xmm6, %xmm4
	mulps	%xmm0, %xmm6
	mulps	%xmm1, %xmm3

	addps	%xmm7, %xmm9
	addps	%xmm5, %xmm13
	movaps	%xmm4, %xmm5
	mulps	%xmm0, %xmm4
	movaps	  8 * SIZE(AO), %xmm0
	mulps	%xmm1, %xmm5
	movaps	 12 * SIZE(AO), %xmm1

	addps	%xmm6, %xmm10
	addps	%xmm3, %xmm14
	movaps	%xmm2, %xmm3
	pshufd	$0x39, %xmm2, %xmm7
	mulps	%xmm0, %xmm2
	mulps	%xmm1, %xmm3

	addps	%xmm4, %xmm11
	addps	%xmm5, %xmm15
	movaps	%xmm7, %xmm5
	pshufd	$0x39, %xmm7, %xmm6
	mulps	%xmm0, %xmm7
	mulps	%xmm1, %xmm5

	addps	%xmm2, %xmm8
	movaps	 -8 * SIZE(BO), %xmm2
	addps	%xmm3, %xmm12
	movaps	%xmm6, %xmm3
	pshufd	$0x39, %xmm6, %xmm4
	mulps	%xmm0, %xmm6
	mulps	%xmm1, %xmm3

	addps	%xmm7, %xmm9
	addps	%xmm5, %xmm13
	movaps	%xmm4, %xmm5
	mulps	%xmm0, %xmm4
	movaps	 16 * SIZE(AO), %xmm0
	mulps	%xmm1, %xmm5
	movaps	 20 * SIZE(AO), %xmm1

	addps	%xmm6, %xmm10
	addps	%xmm3, %xmm14
	PREFETCH  16 * SIZE(PREA)
	movaps	%xmm2, %xmm3
	pshufd	$0x39, %xmm2, %xmm7
	mulps	%xmm0, %xmm2
	mulps	%xmm1, %xmm3

	addps	%xmm4, %xmm11
	addps	%xmm5, %xmm15
	movaps	%xmm7, %xmm5
	pshufd	$0x39, %xmm7, %xmm6
	mulps	%xmm0, %xmm7
	mulps	%xmm1, %xmm5

	addps	%xmm2, %xmm8
	movaps	 -4 * SIZE(BO), %xmm2
	addps	%xmm3, %xmm12
	movaps	%xmm6, %xmm3
	pshufd	$0x39, %xmm6, %xmm4
	mulps	%xmm0, %xmm6
	mulps	%xmm1, %xmm3

	addps	%xmm7, %xmm9
	addps	%xmm5, %xmm13
	movaps	%xmm4, %xmm5
	mulps	%xmm0, %xmm4
	movaps	 24 * SIZE(AO), %xmm0
	mulps	%xmm1, %xmm5
	movaps	 28 * SIZE(AO), %xmm1

	addps	%xmm6, %xmm10
	addps	%xmm3, %xmm14
	movaps	%xmm2, %xmm3
	pshufd	$0x39, %xmm2, %xmm7
	mulps	%xmm0, %xmm2
	mulps	%xmm1, %xmm3

	addps	%xmm4, %xmm11
	addps	%xmm5, %xmm15
	movaps	%xmm7, %xmm5
	subq	$-64 * SIZE, AO
	pshufd	$0x39, %xmm7, %xmm6
	mulps	%xmm0, %xmm7
	mulps	%xmm1, %xmm5

	addps	%xmm2, %xmm8
	movaps	 0 * SIZE(BO), %xmm2
	addps	%xmm3, %xmm12
	movaps	%xmm6, %xmm3
	subq	$-32 * SIZE, BO
	pshufd	$0x39, %xmm6, %xmm4
	mulps	%xmm0, %xmm6
	mulps	%xmm1, %xmm3

	addps	%xmm7, %xmm9
	addps	%xmm5, %xmm13
	movaps	%xmm4, %xmm5
	mulps	%xmm0, %xmm4
	movaps  -32 * SIZE(AO), %xmm0
	mulps	%xmm1, %xmm5
	movaps	-28 * SIZE(AO), %xmm1

	subq	$-64 * SIZE, PREA

	subq	$1, %rax
	BRANCH
	jg	.L12
	ALIGN_3

.L15:
	prefetcht0	 -16 * SIZE(BB)

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je	.L18
	ALIGN_3

.L16:
	addps	%xmm6, %xmm10
	addps	%xmm3, %xmm14
	movaps	%xmm2, %xmm3
	pshufd	$0x39, %xmm2, %xmm7
	mulps	%xmm0, %xmm2
	mulps	%xmm1, %xmm3

	addps	%xmm4, %xmm11
	addps	%xmm5, %xmm15
	movaps	%xmm7, %xmm5
	pshufd	$0x39, %xmm7, %xmm6
	mulps	%xmm0, %xmm7
	mulps	%xmm1, %xmm5

	addps	%xmm2, %xmm8
	movaps	-28 * SIZE(BO), %xmm2
	addps	%xmm3, %xmm12
	movaps	%xmm6, %xmm3
	pshufd	$0x39, %xmm6, %xmm4
	mulps	%xmm0, %xmm6
	mulps	%xmm1, %xmm3

	addps	%xmm7, %xmm9
	addps	%xmm5, %xmm13
	movaps	%xmm4, %xmm5
	mulps	%xmm0, %xmm4
	movaps	-24 * SIZE(AO), %xmm0
	mulps	%xmm1, %xmm5
	movaps	-20 * SIZE(AO), %xmm1

	addq	$8 * SIZE, AO
	addq	$4 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L16
	ALIGN_3

.L18:
	movups	ALPHA_R, %xmm7

	addps	%xmm6, %xmm10
	addps	%xmm3, %xmm14
	addps	%xmm4, %xmm11
	addps	%xmm5, %xmm15

	movaps	%xmm9, %xmm4
	shufps	$0xd8, %xmm8, %xmm9
	shufps	$0xd8, %xmm11, %xmm8
	shufps	$0xd8, %xmm10, %xmm11
	shufps	$0xd8, %xmm4, %xmm10

	movaps	%xmm8, %xmm4
	shufps	$0xd8, %xmm10, %xmm8
	shufps	$0xd8, %xmm4, %xmm10
	movaps	%xmm9, %xmm5
	shufps	$0xd8, %xmm11, %xmm9
	shufps	$0xd8, %xmm5, %xmm11

	movaps	%xmm13, %xmm4
	shufps	$0xd8, %xmm12, %xmm13
	shufps	$0xd8, %xmm15, %xmm12
	shufps	$0xd8, %xmm14, %xmm15
	shufps	$0xd8, %xmm4, %xmm14

	movaps	%xmm12, %xmm4
	shufps	$0xd8, %xmm14, %xmm12
	shufps	$0xd8, %xmm4, %xmm14
	movaps	%xmm13, %xmm5
	shufps	$0xd8, %xmm15, %xmm13
	shufps	$0xd8, %xmm5, %xmm15

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
	decq	I
	BRANCH
	jg	.L11
	ALIGN_4

.L20:
	testq	$4, M
	BRANCH
	jle	.L30

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	movq	B, BO
#else
	movq	B, BO

	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 4), BO
#endif

	movaps	-32 * SIZE(AO), %xmm0
	xorps	%xmm3, %xmm3
	xorps	%xmm4, %xmm4

	movaps	-32 * SIZE(BO), %xmm2
	xorps	%xmm5, %xmm5
	xorps	%xmm6, %xmm6

	movaps	%xmm4, %xmm8
	movaps	%xmm4, %xmm9
	movaps	%xmm4, %xmm10
	movaps	%xmm4, %xmm11

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
	jle	.L25
	ALIGN_3

.L22:
	addps	%xmm6, %xmm10
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)
	pshufd	$0x39, %xmm2, %xmm7
	mulps	%xmm0, %xmm2
	addps	%xmm4, %xmm11
	pshufd	$0x39, %xmm7, %xmm6
	mulps	%xmm0, %xmm7

	addps	%xmm2, %xmm8
	movaps	-28 * SIZE(BO), %xmm2
	pshufd	$0x39, %xmm6, %xmm4
	mulps	%xmm0, %xmm6
	addps	%xmm7, %xmm9
	mulps	%xmm0, %xmm4
	movaps	-28 * SIZE(AO), %xmm0

	addps	%xmm6, %xmm10
	pshufd	$0x39, %xmm2, %xmm7
	mulps	%xmm0, %xmm2
	addps	%xmm4, %xmm11
	pshufd	$0x39, %xmm7, %xmm6
	mulps	%xmm0, %xmm7

	addps	%xmm2, %xmm8
	movaps	-24 * SIZE(BO), %xmm2
	pshufd	$0x39, %xmm6, %xmm4
	mulps	%xmm0, %xmm6
	addps	%xmm7, %xmm9
	mulps	%xmm0, %xmm4
	movaps	-24 * SIZE(AO), %xmm0

	addps	%xmm6, %xmm10
	pshufd	$0x39, %xmm2, %xmm7
	mulps	%xmm0, %xmm2
	addps	%xmm4, %xmm11
	pshufd	$0x39, %xmm7, %xmm6
	mulps	%xmm0, %xmm7

	addps	%xmm2, %xmm8
	movaps	-20 * SIZE(BO), %xmm2
	pshufd	$0x39, %xmm6, %xmm4
	mulps	%xmm0, %xmm6
	addps	%xmm7, %xmm9
	mulps	%xmm0, %xmm4
	movaps	-20 * SIZE(AO), %xmm0

	addps	%xmm6, %xmm10
	pshufd	$0x39, %xmm2, %xmm7
	mulps	%xmm0, %xmm2
	addps	%xmm4, %xmm11
	pshufd	$0x39, %xmm7, %xmm6
	mulps	%xmm0, %xmm7

	addps	%xmm2, %xmm8
	movaps	-16 * SIZE(BO), %xmm2
	pshufd	$0x39, %xmm6, %xmm4
	mulps	%xmm0, %xmm6
	subq	$-16 * SIZE, AO
	addps	%xmm7, %xmm9
	mulps	%xmm0, %xmm4
	movaps	-32 * SIZE(AO), %xmm0

	subq	$-16 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L22
	ALIGN_3

.L25:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax
	BRANCH
	je	.L28
	ALIGN_3

.L26:
	addps	%xmm6, %xmm10
	pshufd	$0x39, %xmm2, %xmm7
	mulps	%xmm0, %xmm2
	addps	%xmm4, %xmm11
	pshufd	$0x39, %xmm7, %xmm6
	mulps	%xmm0, %xmm7

	addps	%xmm2, %xmm8
	movaps	-28 * SIZE(BO), %xmm2
	pshufd	$0x39, %xmm6, %xmm4
	mulps	%xmm0, %xmm6
	addps	%xmm7, %xmm9
	mulps	%xmm0, %xmm4
	movaps	-28 * SIZE(AO), %xmm0

	addq	$4 * SIZE, AO
	addq	$4 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L26
	ALIGN_3

.L28:
	movups	ALPHA_R, %xmm7

	addps	%xmm6, %xmm10
	addps	%xmm4, %xmm11

	movaps	%xmm9, %xmm4
	shufps	$0xd8, %xmm8, %xmm9
	shufps	$0xd8, %xmm11, %xmm8
	shufps	$0xd8, %xmm10, %xmm11
	shufps	$0xd8, %xmm4, %xmm10

	movaps	%xmm8, %xmm4
	shufps	$0xd8, %xmm10, %xmm8
	shufps	$0xd8, %xmm4, %xmm10
	movaps	%xmm9, %xmm5
	shufps	$0xd8, %xmm11, %xmm9
	shufps	$0xd8, %xmm5, %xmm11

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
	BRANCH
	jle	.L40

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	movq	B, BO
#else
	movq	B, BO

	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 4), BO
#endif

	movaps	-32 * SIZE(AO), %xmm0
	xorps	%xmm3, %xmm3
	xorps	%xmm4, %xmm4
	movaps	-32 * SIZE(BO), %xmm2
	xorps	%xmm5, %xmm5
	xorps	%xmm6, %xmm6

	movaps	%xmm4, %xmm8
	movaps	%xmm4, %xmm9
	movaps	%xmm4, %xmm10
	movaps	%xmm4, %xmm11

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
	jle	.L35
	ALIGN_3

.L32:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)

	pshufd	$0x44, %xmm0, %xmm1
	addps	%xmm3, %xmm8
	pshufd	$0x50, %xmm2, %xmm3
	mulps	%xmm1, %xmm3
	addps	%xmm4, %xmm9
	pshufd	$0xfa, %xmm2, %xmm4
	movaps	-28 * SIZE(BO), %xmm2
	mulps	%xmm1, %xmm4

	pshufd	$0xee, %xmm0, %xmm1
	movaps	-28 * SIZE(AO), %xmm0
	addps	%xmm3, %xmm10
	pshufd	$0x50, %xmm2, %xmm3
	mulps	%xmm1, %xmm3
	addps	%xmm4, %xmm11
	pshufd	$0xfa, %xmm2, %xmm4
	movaps	-24 * SIZE(BO), %xmm2
	mulps	%xmm1, %xmm4

	pshufd	$0x44, %xmm0, %xmm1
	addps	%xmm3, %xmm8
	pshufd	$0x50, %xmm2, %xmm3
	mulps	%xmm1, %xmm3
	addps	%xmm4, %xmm9
	pshufd	$0xfa, %xmm2, %xmm4
	movaps	-20 * SIZE(BO), %xmm2
	mulps	%xmm1, %xmm4

	pshufd	$0xee, %xmm0, %xmm1
	movaps	-24 * SIZE(AO), %xmm0
	addps	%xmm3, %xmm10
	pshufd	$0x50, %xmm2, %xmm3
	mulps	%xmm1, %xmm3
	addps	%xmm4, %xmm11
	pshufd	$0xfa, %xmm2, %xmm4
	movaps	-16 * SIZE(BO), %xmm2
	mulps	%xmm1, %xmm4

	subq	$-8 * SIZE, AO
	subq	$-16 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L32
	ALIGN_3

.L35:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax
	BRANCH
	je	.L38
	ALIGN_3

.L36:
	pshufd	$0x44, %xmm0, %xmm1
	movsd	-30 * SIZE(AO), %xmm0
	addps	%xmm3, %xmm8
	pshufd	$0x50, %xmm2, %xmm3
	mulps	%xmm1, %xmm3
	addps	%xmm4, %xmm9
	pshufd	$0xfa, %xmm2, %xmm4
	movaps	-28 * SIZE(BO), %xmm2
	mulps	%xmm1, %xmm4

	addq	$2 * SIZE, AO
	addq	$4 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L36
	ALIGN_3

.L38:
	movups	ALPHA_R, %xmm7

	addps	%xmm10, %xmm8
	addps	%xmm11, %xmm9

	addps	%xmm3, %xmm8
	addps	%xmm4, %xmm9

	movsd	 0 * SIZE(CO1), %xmm0
	movhps	 2 * SIZE(CO1), %xmm0
	movsd	 0 * SIZE(CO2), %xmm1
	movhps	 2 * SIZE(CO2), %xmm1

	pshufd	$0x50, %xmm8,  %xmm4
	pshufd	$0xfa, %xmm8,  %xmm8

	mulps	%xmm7, %xmm4
	mulps	%xmm7, %xmm8
	addps	%xmm0, %xmm4
	addps	%xmm1, %xmm8

	movlps	%xmm4,   0 * SIZE(CO1)
	movhps	%xmm4,   2 * SIZE(CO1)
	movlps	%xmm8,   0 * SIZE(CO2)
	movhps	%xmm8,   2 * SIZE(CO2)

	movsd	 0 * SIZE(CO1, LDC, 2), %xmm0
	movhps	 2 * SIZE(CO1, LDC, 2), %xmm0
	movsd	 0 * SIZE(CO2, LDC, 2), %xmm1
	movhps	 2 * SIZE(CO2, LDC, 2), %xmm1

	pshufd	$0x50, %xmm9,  %xmm4
	pshufd	$0xfa, %xmm9,  %xmm9

	mulps	%xmm7, %xmm4
	mulps	%xmm7, %xmm9
	addps	%xmm0, %xmm4
	addps	%xmm1, %xmm9

	movlps	%xmm4,   0 * SIZE(CO1, LDC, 2)
	movhps	%xmm4,   2 * SIZE(CO1, LDC, 2)
	movlps	%xmm9,   0 * SIZE(CO2, LDC, 2)
	movhps	%xmm9,   2 * SIZE(CO2, LDC, 2)

	addq	$4 * SIZE, CO1
	addq	$4 * SIZE, CO2
	ALIGN_4

.L40:
	testq	$1, M
	BRANCH
	jle	.L49

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	movq	B, BO
#else
	movq	B, BO

	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 4), BO
#endif

	movsd	-32 * SIZE(AO), %xmm0
	xorps	%xmm8, %xmm8
	movaps	-32 * SIZE(BO), %xmm2
	xorps	%xmm9, %xmm9

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
	jle	.L45
	ALIGN_3

.L42:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)

	pshufd	$0x00, %xmm0, %xmm1
	movss	-31 * SIZE(AO), %xmm0
	mulps	%xmm1, %xmm2
	addps	%xmm2, %xmm8
	movaps	-28 * SIZE(BO), %xmm2

	pshufd	$0x00, %xmm0, %xmm1
	movss	-30 * SIZE(AO), %xmm0
	mulps	%xmm1, %xmm2
	addps	%xmm2, %xmm9
	movaps	-24 * SIZE(BO), %xmm2

	pshufd	$0x00, %xmm0, %xmm1
	movss	-29 * SIZE(AO), %xmm0
	mulps	%xmm1, %xmm2
	addps	%xmm2, %xmm8
	movaps	-20 * SIZE(BO), %xmm2

	pshufd	$0x00, %xmm0, %xmm1
	movss	-28 * SIZE(AO), %xmm0
	mulps	%xmm1, %xmm2
	addps	%xmm2, %xmm9
	movaps	-16 * SIZE(BO), %xmm2

	subq	$ -4 * SIZE, AO
	subq	$-16 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L42
	ALIGN_3

.L45:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax
	BRANCH
	je	.L48
	ALIGN_3

.L46:
	pshufd	$0x00, %xmm0, %xmm1
	movss	-31 * SIZE(AO), %xmm0
	mulps	%xmm1, %xmm2
	addps	%xmm2, %xmm8
	movaps	-28 * SIZE(BO), %xmm2

	addq	$1 * SIZE, AO
	addq	$4 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L46
	ALIGN_3

.L48:
	movups	ALPHA_R, %xmm7

	addps	%xmm9, %xmm8

	movsd	 0 * SIZE(CO1), %xmm0
	movhps	 0 * SIZE(CO2), %xmm0
	movsd	 0 * SIZE(CO1, LDC, 2), %xmm1
	movhps	 0 * SIZE(CO2, LDC, 2), %xmm1

	pshufd	$0x50, %xmm8,  %xmm4
	pshufd	$0xfa, %xmm8,  %xmm8

	mulps	%xmm7, %xmm4
	mulps	%xmm7, %xmm8
	addps	%xmm0, %xmm4
	addps	%xmm1, %xmm8

	movlps	%xmm4,   0 * SIZE(CO1)
	movhps	%xmm4,   0 * SIZE(CO2)
	movlps	%xmm8,   0 * SIZE(CO1, LDC, 2)
	movhps	%xmm8,   0 * SIZE(CO2, LDC, 2)
	ALIGN_4

.L49:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addq	$4, KK
#endif

	movq	BO, B

	leaq	(C, LDC, 4), C

	subq	$1, J
	BRANCH
	jg	.L10
	ALIGN_4

.L50:
	testq	$2, N
	jle	.L90

#if defined(TRMMKERNEL) && defined(LEFT)
        movq    OFFSET, %rax
	movq    %rax, KK
#endif

	movq	C, CO1
	leaq	(C, LDC, 1), CO2
	movq	A, AO

	movq	K, %rax
	salq	$BASE_SHIFT + 1, %rax
	leaq	(B, %rax), BB

	movq	M,  I
	sarq	$3, I
	NOBRANCH
	jle	.L60
	ALIGN_4

.L51:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	movq	B, BO
#else
	movq	B, BO

	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 8), AO
	leaq	(BO, %rax, 2), BO
#endif

	prefetcht2	 -32 * SIZE(BB)
	subq		$-8 * SIZE, BB

	movaps	-32 * SIZE(AO), %xmm0
	xorps	%xmm3, %xmm3
	movaps	-28 * SIZE(AO), %xmm1
	xorps	%xmm4, %xmm4
	movaps	-32 * SIZE(BO), %xmm2

	xorps	%xmm5, %xmm5
	xorps	%xmm6, %xmm6

	prefetcht0     7 * SIZE(CO1)
	movaps	%xmm4, %xmm8
	movaps	%xmm4, %xmm9
	prefetcht0     7 * SIZE(CO2)
	movaps	%xmm4, %xmm10
	movaps	%xmm4, %xmm11


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
	NOBRANCH
	jle	.L55
	ALIGN_3

.L52:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)

	addps	%xmm3, %xmm8
	pshufd	$0x00, %xmm2, %xmm3
	mulps	%xmm0, %xmm3
	addps	%xmm4, %xmm9
	pshufd	$0x55, %xmm2, %xmm4
	mulps	%xmm0, %xmm4
	movaps	-24 * SIZE(AO), %xmm0

	addps	%xmm5, %xmm10
	pshufd	$0x00, %xmm2, %xmm5
	mulps	%xmm1, %xmm5
	addps	%xmm6, %xmm11
	pshufd	$0x55, %xmm2, %xmm6
	mulps	%xmm1, %xmm6
	movaps	-20 * SIZE(AO), %xmm1

	addps	%xmm3, %xmm8
	pshufd	$0xaa, %xmm2, %xmm3
	mulps	%xmm0, %xmm3
	addps	%xmm4, %xmm9
	pshufd	$0xff, %xmm2, %xmm4
	mulps	%xmm0, %xmm4
	movaps	-16 * SIZE(AO), %xmm0

	PREFETCH (PREFETCHSIZE + 16) * SIZE(AO)

	addps	%xmm5, %xmm10
	pshufd	$0xaa, %xmm2, %xmm5
	mulps	%xmm1, %xmm5
	addps	%xmm6, %xmm11
	pshufd	$0xff, %xmm2, %xmm6
	movaps	-28 * SIZE(BO), %xmm2
	mulps	%xmm1, %xmm6
	movaps	-12 * SIZE(AO), %xmm1

	addps	%xmm3, %xmm8
	pshufd	$0x00, %xmm2, %xmm3
	mulps	%xmm0, %xmm3
	addps	%xmm4, %xmm9
	pshufd	$0x55, %xmm2, %xmm4
	mulps	%xmm0, %xmm4
	movaps	 -8 * SIZE(AO), %xmm0

	addps	%xmm5, %xmm10
	pshufd	$0x00, %xmm2, %xmm5
	mulps	%xmm1, %xmm5
	addps	%xmm6, %xmm11
	pshufd	$0x55, %xmm2, %xmm6
	mulps	%xmm1, %xmm6
	movaps	 -4 * SIZE(AO), %xmm1

	addps	%xmm3, %xmm8
	pshufd	$0xaa, %xmm2, %xmm3
	mulps	%xmm0, %xmm3
	addps	%xmm4, %xmm9
	pshufd	$0xff, %xmm2, %xmm4
	mulps	%xmm0, %xmm4
	movaps	  0 * SIZE(AO), %xmm0

	addps	%xmm5, %xmm10
	pshufd	$0xaa, %xmm2, %xmm5
	mulps	%xmm1, %xmm5
	addps	%xmm6, %xmm11
	pshufd	$0xff, %xmm2, %xmm6
	movaps	-24 * SIZE(BO), %xmm2
	mulps	%xmm1, %xmm6
	movaps	  4 * SIZE(AO), %xmm1

	subq	$-32 * SIZE, AO
	subq	$ -8 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L52
	ALIGN_3

.L55:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	BRANCH
	je	.L58
	ALIGN_3

.L56:
	addps	%xmm3, %xmm8
	pshufd	$0x00, %xmm2, %xmm3
	mulps	%xmm0, %xmm3
	addps	%xmm4, %xmm9
	pshufd	$0x55, %xmm2, %xmm4
	mulps	%xmm0, %xmm4
	movaps	-24 * SIZE(AO), %xmm0

	addps	%xmm5, %xmm10
	pshufd	$0x00, %xmm2, %xmm5
	mulps	%xmm1, %xmm5
	addps	%xmm6, %xmm11
	pshufd	$0x55, %xmm2, %xmm6
	movsd	-30 * SIZE(BO), %xmm2
	mulps	%xmm1, %xmm6
	movaps	-20 * SIZE(AO), %xmm1

	addq	$8 * SIZE, AO
	addq	$2 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L56
	ALIGN_3

.L58:
	movups	ALPHA_R, %xmm7

	addps	%xmm3, %xmm8
	addps	%xmm4, %xmm9
	addps	%xmm5, %xmm10
	addps	%xmm6, %xmm11

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
	pshufd	$0x50, %xmm10, %xmm5
	pshufd	$0xfa, %xmm10, %xmm10

	mulps	%xmm7, %xmm4
	mulps	%xmm7, %xmm8
	mulps	%xmm7, %xmm5
	mulps	%xmm7, %xmm10

	addps	%xmm0, %xmm4
	addps	%xmm1, %xmm8
	addps	%xmm2, %xmm5
	addps	%xmm3, %xmm10

	movlps	%xmm4,   0 * SIZE(CO1)
	movhps	%xmm4,   2 * SIZE(CO1)
	movlps	%xmm8,   4 * SIZE(CO1)
	movhps	%xmm8,   6 * SIZE(CO1)
	movlps	%xmm5,   8 * SIZE(CO1)
	movhps	%xmm5,  10 * SIZE(CO1)
	movlps	%xmm10, 12 * SIZE(CO1)
	movhps	%xmm10, 14 * SIZE(CO1)

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
	pshufd	$0x50, %xmm11, %xmm5
	pshufd	$0xfa, %xmm11, %xmm11

	mulps	%xmm7, %xmm4
	mulps	%xmm7, %xmm9
	mulps	%xmm7, %xmm5
	mulps	%xmm7, %xmm11

	addps	%xmm0, %xmm4
	addps	%xmm1, %xmm9
	addps	%xmm2, %xmm5
	addps	%xmm3, %xmm11

	movlps	%xmm4,   0 * SIZE(CO2)
	movhps	%xmm4,   2 * SIZE(CO2)
	movlps	%xmm9,   4 * SIZE(CO2)
	movhps	%xmm9,   6 * SIZE(CO2)
	movlps	%xmm5,   8 * SIZE(CO2)
	movhps	%xmm5,  10 * SIZE(CO2)
	movlps	%xmm11, 12 * SIZE(CO2)
	movhps	%xmm11, 14 * SIZE(CO2)

	addq	$16 * SIZE, CO1
	addq	$16 * SIZE, CO2
	decq	I
	BRANCH
	jg	.L51
	ALIGN_4

.L60:
	testq	$4, M
	BRANCH
	jle	.L70

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	movq	B, BO
#else
	movq	B, BO

	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 2), BO
#endif

	movaps	-32 * SIZE(AO), %xmm0
	xorps	%xmm3, %xmm3
	xorps	%xmm4, %xmm4
	movaps	-32 * SIZE(BO), %xmm2
	xorps	%xmm8, %xmm8
	xorps	%xmm9, %xmm9

	xorps	%xmm10, %xmm10
	xorps	%xmm11, %xmm11

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
	jle	.L65
	ALIGN_3

.L62:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)

	addps	%xmm3, %xmm8
	pshufd	$0x00, %xmm2, %xmm3
	mulps	%xmm0, %xmm3
	addps	%xmm4, %xmm9
	pshufd	$0x55, %xmm2, %xmm4
	mulps	%xmm0, %xmm4
	movaps	-28 * SIZE(AO), %xmm0

	addps	%xmm3, %xmm10
	pshufd	$0xaa, %xmm2, %xmm3
	mulps	%xmm0, %xmm3
	addps	%xmm4, %xmm11
	pshufd	$0xff, %xmm2, %xmm4
	movaps	-28 * SIZE(BO), %xmm2
	mulps	%xmm0, %xmm4
	movaps	-24 * SIZE(AO), %xmm0

	addps	%xmm3, %xmm8
	pshufd	$0x00, %xmm2, %xmm3
	mulps	%xmm0, %xmm3
	addps	%xmm4, %xmm9
	pshufd	$0x55, %xmm2, %xmm4
	mulps	%xmm0, %xmm4
	movaps	-20 * SIZE(AO), %xmm0

	addps	%xmm3, %xmm10
	pshufd	$0xaa, %xmm2, %xmm3
	mulps	%xmm0, %xmm3
	addps	%xmm4, %xmm11
	pshufd	$0xff, %xmm2, %xmm4
	movaps	-24 * SIZE(BO), %xmm2
	mulps	%xmm0, %xmm4
	movaps	-16 * SIZE(AO), %xmm0

	subq	$-16 * SIZE, AO
	subq	$ -8 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L62
	ALIGN_3

.L65:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax
	BRANCH
	je	.L68
	ALIGN_3

.L66:
	addps	%xmm3, %xmm8
	pshufd	$0x00, %xmm2, %xmm3
	mulps	%xmm0, %xmm3
	addps	%xmm4, %xmm9
	pshufd	$0x55, %xmm2, %xmm4
	movsd	-30 * SIZE(BO), %xmm2
	mulps	%xmm0, %xmm4
	movaps	-28 * SIZE(AO), %xmm0

	addq	$4 * SIZE, AO
	addq	$2 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L66
	ALIGN_3

.L68:
	movups	ALPHA_R, %xmm7

	addps	%xmm10, %xmm8
	addps	%xmm11, %xmm9

	addps	%xmm3, %xmm8
	addps	%xmm4, %xmm9

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

.L70:
	testq	$2, M
	BRANCH
	jle	.L80

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	movq	B, BO
#else
	movq	B, BO

	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 2), BO
#endif

	movaps	-32 * SIZE(AO), %xmm0
	xorps	%xmm3, %xmm3
	movaps	-32 * SIZE(BO), %xmm2
	xorps	%xmm8, %xmm8
	xorps	%xmm9, %xmm9

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
	jle	.L75
	ALIGN_3

.L72:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)

	addps	%xmm3, %xmm8
	pshufd	$0x44, %xmm0, %xmm1
	pshufd	$0x50, %xmm2, %xmm3
	mulps	%xmm1, %xmm3

	addps	%xmm3, %xmm9
	pshufd	$0xee, %xmm0, %xmm1
	movaps	-28 * SIZE(AO), %xmm0
	pshufd	$0xfa, %xmm2, %xmm3
	movaps	-28 * SIZE(BO), %xmm2
	mulps	%xmm1, %xmm3

	addps	%xmm3, %xmm8
	pshufd	$0x44, %xmm0, %xmm1
	pshufd	$0x50, %xmm2, %xmm3
	mulps	%xmm1, %xmm3

	addps	%xmm3, %xmm9
	pshufd	$0xee, %xmm0, %xmm1
	movaps	-24 * SIZE(AO), %xmm0
	pshufd	$0xfa, %xmm2, %xmm3
	movaps	-24 * SIZE(BO), %xmm2
	mulps	%xmm1, %xmm3

	subq	$-8 * SIZE, AO
	subq	$-8 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L72
	ALIGN_3

.L75:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax
	BRANCH
	je	.L78
	ALIGN_3

.L76:
	addps	%xmm3, %xmm8
	pshufd	$0x44, %xmm0, %xmm1
	movsd	-30 * SIZE(AO), %xmm0
	pshufd	$0x50, %xmm2, %xmm3
	movsd	-30 * SIZE(BO), %xmm2
	mulps	%xmm1, %xmm3

	addq	$2 * SIZE, AO
	addq	$2 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L76
	ALIGN_3

.L78:
	movups	ALPHA_R, %xmm7

	addps	%xmm9, %xmm8
	addps	%xmm3, %xmm8

	movsd	 0 * SIZE(CO1), %xmm0
	movhps	 2 * SIZE(CO1), %xmm0
	movsd	 0 * SIZE(CO2), %xmm1
	movhps	 2 * SIZE(CO2), %xmm1

	pshufd	$0x50, %xmm8,  %xmm4
	pshufd	$0xfa, %xmm8,  %xmm8

	mulps	%xmm7, %xmm4
	mulps	%xmm7, %xmm8
	addps	%xmm0, %xmm4
	addps	%xmm1, %xmm8

	movlps	%xmm4,   0 * SIZE(CO1)
	movhps	%xmm4,   2 * SIZE(CO1)
	movlps	%xmm8,   0 * SIZE(CO2)
	movhps	%xmm8,   2 * SIZE(CO2)

	addq	$4 * SIZE, CO1
	addq	$4 * SIZE, CO2
	ALIGN_4

.L80:
	testq	$1, M
	BRANCH
	jle	.L89

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	movq	B, BO
#else
	movq	B, BO

	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 2), BO
#endif

	movsd	-32 * SIZE(AO), %xmm0
	xorps	%xmm8, %xmm8
	movsd	-32 * SIZE(BO), %xmm2
	xorps	%xmm9, %xmm9

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
	jle	.L85
	ALIGN_3

.L82:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)

	pshufd	$0x00, %xmm0, %xmm1
	movss	-31 * SIZE(AO), %xmm0
	mulps	%xmm1, %xmm2
	addps	%xmm2, %xmm8
	movsd	-30 * SIZE(BO), %xmm2

	pshufd	$0x00, %xmm0, %xmm1
	movss	-30 * SIZE(AO), %xmm0
	mulps	%xmm1, %xmm2
	addps	%xmm2, %xmm9
	movsd	-28 * SIZE(BO), %xmm2

	pshufd	$0x00, %xmm0, %xmm1
	movss	-29 * SIZE(AO), %xmm0
	mulps	%xmm1, %xmm2
	addps	%xmm2, %xmm8
	movsd	-26 * SIZE(BO), %xmm2

	pshufd	$0x00, %xmm0, %xmm1
	movss	-28 * SIZE(AO), %xmm0
	mulps	%xmm1, %xmm2
	addps	%xmm2, %xmm9
	movsd	-24 * SIZE(BO), %xmm2

	subq	$-4 * SIZE, AO
	subq	$-8 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L82
	ALIGN_3

.L85:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax
	BRANCH
	je	.L88
	ALIGN_3

.L86:
	pshufd	$0x00, %xmm0, %xmm1
	movss	-31 * SIZE(AO), %xmm0
	mulps	%xmm1, %xmm2
	addps	%xmm2, %xmm8
	movsd	-30 * SIZE(BO), %xmm2

	addq	$1 * SIZE, AO
	addq	$2 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L86
	ALIGN_3

.L88:
	movups	ALPHA_R, %xmm7

	addps	%xmm9, %xmm8

	movsd	 0 * SIZE(CO1), %xmm0
	movhps	 0 * SIZE(CO2), %xmm0

	pshufd	$0x50, %xmm8,  %xmm4

	mulps	%xmm7, %xmm4
	addps	%xmm0, %xmm4

	movlps	%xmm4,   0 * SIZE(CO1)
	movhps	%xmm4,   0 * SIZE(CO2)
	ALIGN_4

.L89:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addq	$2, KK
#endif

	movq	BO, B

	leaq	(C, LDC, 2), C
	ALIGN_4

.L90:
	testq	$1, N
	jle	.L999

#if defined(TRMMKERNEL) && defined(LEFT)
        movq    OFFSET, %rax
	movq    %rax, KK
#endif

	movq	C, CO1
	movq	A, AO

	movq	M,  I
	sarq	$3, I
	NOBRANCH
	jle	.L100
	ALIGN_4

.L91:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	movq	B, BO
#else
	movq	B, BO

	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 8), AO
	leaq	(BO, %rax, 1), BO
#endif

	movaps	-32 * SIZE(AO), %xmm0
	xorps	%xmm8,  %xmm8
	movaps	-28 * SIZE(AO), %xmm1
	xorps	%xmm9,  %xmm9
	movsd	-32 * SIZE(BO), %xmm2
	xorps	%xmm10, %xmm10
	prefetcht0     7 * SIZE(CO1)
	xorps	%xmm11, %xmm11

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
	NOBRANCH
	jle	.L95
	ALIGN_3

.L92:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)

	pshufd	$0x00, %xmm2, %xmm3
	mulps	%xmm3, %xmm0
	addps	%xmm0, %xmm8
	movaps	-24 * SIZE(AO), %xmm0
	mulps	%xmm3, %xmm1
	addps	%xmm1, %xmm9
	movaps	-20 * SIZE(AO), %xmm1

	pshufd	$0x55, %xmm2, %xmm3
	movsd	-30 * SIZE(BO), %xmm2
	mulps	%xmm3, %xmm0
	addps	%xmm0, %xmm10
	movaps	-16 * SIZE(AO), %xmm0
	mulps	%xmm3, %xmm1
	addps	%xmm1, %xmm11
	movaps	-12 * SIZE(AO), %xmm1

	PREFETCH (PREFETCHSIZE + 16) * SIZE(AO)

	pshufd	$0x00, %xmm2, %xmm3
	mulps	%xmm3, %xmm0
	addps	%xmm0, %xmm8
	movaps	 -8 * SIZE(AO), %xmm0
	mulps	%xmm3, %xmm1
	addps	%xmm1, %xmm9
	movaps	 -4 * SIZE(AO), %xmm1

	pshufd	$0x55, %xmm2, %xmm3
	movsd	-28 * SIZE(BO), %xmm2
	mulps	%xmm3, %xmm0
	addps	%xmm0, %xmm10
	movaps	  0 * SIZE(AO), %xmm0
	mulps	%xmm3, %xmm1
	addps	%xmm1, %xmm11
	movaps	  4 * SIZE(AO), %xmm1

	subq	$-32 * SIZE, AO
	subq	$ -4 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L92
	ALIGN_3

.L95:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	BRANCH
	je	.L98
	ALIGN_3

.L96:
	pshufd	$0x00, %xmm2, %xmm3
	movss	-31 * SIZE(BO), %xmm2
	mulps	%xmm3, %xmm0
	addps	%xmm0, %xmm8
	movaps	-24 * SIZE(AO), %xmm0
	mulps	%xmm3, %xmm1
	addps	%xmm1, %xmm9
	movaps	-20 * SIZE(AO), %xmm1

	addq	$8 * SIZE, AO
	addq	$1 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L96
	ALIGN_3

.L98:
	movups	ALPHA_R, %xmm7

	addps	%xmm10, %xmm8
	addps	%xmm11, %xmm9

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
	pshufd	$0x50, %xmm9,  %xmm5
	pshufd	$0xfa, %xmm9,  %xmm9

	mulps	%xmm7, %xmm4
	mulps	%xmm7, %xmm8
	mulps	%xmm7, %xmm5
	mulps	%xmm7, %xmm9

	addps	%xmm0, %xmm4
	addps	%xmm1, %xmm8
	addps	%xmm2, %xmm5
	addps	%xmm3, %xmm9

	movlps	%xmm4,   0 * SIZE(CO1)
	movhps	%xmm4,   2 * SIZE(CO1)
	movlps	%xmm8,   4 * SIZE(CO1)
	movhps	%xmm8,   6 * SIZE(CO1)
	movlps	%xmm5,   8 * SIZE(CO1)
	movhps	%xmm5,  10 * SIZE(CO1)
	movlps	%xmm9,  12 * SIZE(CO1)
	movhps	%xmm9,  14 * SIZE(CO1)

	addq	$16 * SIZE, CO1
	decq	I
	BRANCH
	jg	.L91
	ALIGN_4

.L100:
	testq	$4, M
	BRANCH
	jle	.L110

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	movq	B, BO
#else
	movq	B, BO

	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 1), BO
#endif

	movaps	-32 * SIZE(AO), %xmm0
	xorps	%xmm8, %xmm8
	movsd	-32 * SIZE(BO), %xmm2
	xorps	%xmm9, %xmm9

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
	jle	.L105
	ALIGN_3

.L102:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)

	pshufd	$0x00, %xmm2, %xmm3
	mulps	%xmm0, %xmm3
	movaps	-28 * SIZE(AO), %xmm0
	addps	%xmm3, %xmm8

	pshufd	$0x55, %xmm2, %xmm3
	movsd	-30 * SIZE(BO), %xmm2
	mulps	%xmm0, %xmm3
	movaps	-24 * SIZE(AO), %xmm0
	addps	%xmm3, %xmm9

	pshufd	$0x00, %xmm2, %xmm3
	mulps	%xmm0, %xmm3
	movaps	-20 * SIZE(AO), %xmm0
	addps	%xmm3, %xmm8

	pshufd	$0x55, %xmm2, %xmm3
	movsd	-28 * SIZE(BO), %xmm2
	mulps	%xmm0, %xmm3
	movaps	-16 * SIZE(AO), %xmm0
	addps	%xmm3, %xmm9

	subq	$-16 * SIZE, AO
	subq	$ -4 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L102
	ALIGN_3

.L105:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax
	BRANCH
	je	.L108
	ALIGN_3

.L106:
	pshufd	$0x00, %xmm2, %xmm3
	movss	-31 * SIZE(BO), %xmm2
	mulps	%xmm0, %xmm3
	movaps	-28 * SIZE(AO), %xmm0
	addps	%xmm3, %xmm8

	addq	$4 * SIZE, AO
	addq	$1 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L106
	ALIGN_3

.L108:
	movups	ALPHA_R, %xmm7

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

.L110:
	testq	$2, M
	BRANCH
	jle	.L120

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	movq	B, BO
#else
	movq	B, BO

	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 1), BO
#endif

	movaps	-32 * SIZE(AO), %xmm0
	xorps	%xmm3, %xmm3
	movsd	-32 * SIZE(BO), %xmm2
	xorps	%xmm8, %xmm8
	xorps	%xmm9, %xmm9

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
	jle	.L115
	ALIGN_3

.L112:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)

	pshufd	$0x00, %xmm2, %xmm3
	mulps	%xmm0, %xmm3
	movsd	-30 * SIZE(AO), %xmm0
	addps	%xmm3, %xmm8

	pshufd	$0x55, %xmm2, %xmm3
	movsd	-30 * SIZE(BO), %xmm2
	mulps	%xmm0, %xmm3
	movsd	-28 * SIZE(AO), %xmm0
	addps	%xmm3, %xmm8

	pshufd	$0x00, %xmm2, %xmm3
	mulps	%xmm0, %xmm3
	movsd	-26 * SIZE(AO), %xmm0
	addps	%xmm3, %xmm8

	pshufd	$0x55, %xmm2, %xmm3
	movsd	-28 * SIZE(BO), %xmm2
	mulps	%xmm0, %xmm3
	movsd	-24 * SIZE(AO), %xmm0
	addps	%xmm3, %xmm8

	subq	$-8 * SIZE, AO
	subq	$-4 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L112
	ALIGN_3

.L115:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax
	BRANCH
	je	.L118
	ALIGN_3

.L116:
	pshufd	$0x00, %xmm2, %xmm3
	movss	-31 * SIZE(BO), %xmm2
	mulps	%xmm0, %xmm3
	movsd	-30 * SIZE(AO), %xmm0
	addps	%xmm3, %xmm8

	addq	$2 * SIZE, AO
	addq	$1 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L116
	ALIGN_3

.L118:
	movups	ALPHA_R, %xmm7

	movsd	 0 * SIZE(CO1), %xmm0
	movhps	 2 * SIZE(CO1), %xmm0

	pshufd	$0x50, %xmm8,  %xmm4

	mulps	%xmm7, %xmm4
	addps	%xmm0, %xmm4

	movlps	%xmm4,   0 * SIZE(CO1)
	movhps	%xmm4,   2 * SIZE(CO1)

	addq	$4 * SIZE, CO1
	ALIGN_4

.L120:
	testq	$1, M
	BRANCH
	jle	.L999

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	movq	B, BO
#else
	movq	B, BO

	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 1), BO
#endif

	movss	-32 * SIZE(AO), %xmm0
	xorps	%xmm8, %xmm8
	movss	-32 * SIZE(BO), %xmm2
	xorps	%xmm9, %xmm9

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
	jle	.L125
	ALIGN_3

.L122:
	mulss	%xmm0, %xmm2
	movss	-31 * SIZE(AO), %xmm0
	addss	%xmm2, %xmm8
	movss	-31 * SIZE(BO), %xmm2

	mulss	%xmm0, %xmm2
	movss	-30 * SIZE(AO), %xmm0
	addss	%xmm2, %xmm9
	movss	-30 * SIZE(BO), %xmm2

	mulss	%xmm0, %xmm2
	movss	-29 * SIZE(AO), %xmm0
	addss	%xmm2, %xmm8
	movss	-29 * SIZE(BO), %xmm2

	mulss	%xmm0, %xmm2
	movss	-28 * SIZE(AO), %xmm0
	addss	%xmm2, %xmm9
	movss	-28 * SIZE(BO), %xmm2

	subq	$-4 * SIZE, AO
	subq	$-4 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L122
	ALIGN_3

.L125:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax
	BRANCH
	je	.L128
	ALIGN_3

.L126:
	mulss	%xmm0, %xmm2
	movss	-31 * SIZE(AO), %xmm0
	addss	%xmm2, %xmm8
	movss	-31 * SIZE(BO), %xmm2

	addq	$1 * SIZE, AO
	addq	$1 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L126
	ALIGN_3

.L128:
	movups	ALPHA_R, %xmm7

	addss	%xmm9, %xmm8

	movsd	 0 * SIZE(CO1), %xmm0

	pshufd	$0x50, %xmm8,  %xmm4

	mulps	%xmm7, %xmm4
	addps	%xmm0, %xmm4

	movlps	%xmm4,   0 * SIZE(CO1)
	ALIGN_4

.L999:
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
