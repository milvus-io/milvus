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
#define LDC	%rbp

#define I	%r11
#define AO	%rdi
#define BO	%rsi
#define	CO1	%rbx
#define CO2	%rdx
#define BB	%r12

#define PREA	%r10

#ifndef WINDOWS_ABI

#define STACKSIZE 128

#define OLD_LDC		 8 + STACKSIZE(%rsp)
#define OLD_OFFSET	16 + STACKSIZE(%rsp)

#define ALPHA	   48(%rsp)
#define J	   56(%rsp)
#define OFFSET	   64(%rsp)
#define KK	   72(%rsp)
#define KKK	   80(%rsp)

#else

#define STACKSIZE 512

#define OLD_A		40 + STACKSIZE(%rsp)
#define OLD_B		48 + STACKSIZE(%rsp)
#define OLD_C		56 + STACKSIZE(%rsp)
#define OLD_LDC		64 + STACKSIZE(%rsp)
#define OLD_OFFSET	72 + STACKSIZE(%rsp)

#define ALPHA	  224(%rsp)
#define J	  232(%rsp)
#define OFFSET	  240(%rsp)
#define KK	  248(%rsp)
#define KKK	  256(%rsp)

#endif

#define PREFETCHSIZE  8
#define PREFETCH     prefetcht0

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

#else
	movq	OLD_LDC,   LDC
#ifdef TRMMKERNEL
	movq	OLD_OFFSET, %r11
#endif

#endif

	unpcklps %xmm0, %xmm0
	movlps	 %xmm0, ALPHA

	subq	$-32 * SIZE, A
	subq	$-32 * SIZE, B

	movq	OLD_M, M
	movq	OLD_N, N
	movq	OLD_K, K

	salq	$BASE_SHIFT, LDC

#ifdef TRMMKERNEL
	movq	%r11, OFFSET
#ifndef LEFT
	negq	%r11
#endif
	movq	%r11, KK
#endif

	movq	N,  J
	sarq	$3, J
	NOBRANCH
	jle	.L40
	ALIGN_4

.L10:
#if defined(TRMMKERNEL) && defined(LEFT)
        movq    OFFSET, %rax
	movq    %rax, KK
#endif

	movq	C, CO1
	leaq	(C, LDC, 4), CO2
	movq	A, AO

	movq	K, %rax
	salq	$BASE_SHIFT + 3, %rax
	leaq	(B, %rax), BB

	movq	M,  I
	sarq	$2, I
	NOBRANCH
	jle	.L20
	ALIGN_4

.L11:
	prefetcht2	 -32 * SIZE(BB)
	subq		 $-16 * SIZE, BB

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	movq	B, BO
#else
	movq	B, BO

	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 8), BO
#endif

	leaq	(LDC, LDC, 2), %rax

	xorps	%xmm1, %xmm1
	xorps	%xmm2, %xmm2
	xorps	%xmm3, %xmm3
	PADDING
	xorps	%xmm4, %xmm4

	PADDING
	xorps	%xmm8,  %xmm8
	prefetcht0     3 * SIZE(CO1)
	xorps	%xmm9,  %xmm9
	prefetcht0     7 * SIZE(CO1, LDC,  1)
	PADDING
	xorps	%xmm10, %xmm10
	prefetcht0     3 * SIZE(CO1, LDC,  2)
	PADDING
	xorps	%xmm11, %xmm11
	prefetcht0     7 * SIZE(CO1, %rax, 1)

	movaps	-32 * SIZE(AO), %xmm0

	PADDING
	xorps	%xmm12, %xmm12
	prefetcht0     3 * SIZE(CO2)
	xorps	%xmm13, %xmm13
	prefetcht0     7 * SIZE(CO2, LDC,  1)
	xorps	%xmm14, %xmm14
	prefetcht0     3 * SIZE(CO2, LDC,  2)
	xorps	%xmm15, %xmm15
	prefetcht0     7 * SIZE(CO2, %rax, 1)

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
	jle	.L15
	ALIGN_3

.L12:
	PREFETCH	(PREFETCHSIZE +  0) * SIZE(AO)

	addps	%xmm1, %xmm12
	movaps	-32 * SIZE(BO), %xmm1
	addps	%xmm2, %xmm13
	pshufd	$0x39, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	pshufd	$0x39, %xmm2, %xmm5
	mulps	%xmm0, %xmm2

	addps	%xmm3, %xmm14
	addps	%xmm4, %xmm15
	pshufd	$0x39, %xmm5, %xmm6
	mulps	%xmm0, %xmm5
	mulps	%xmm0, %xmm6

	addps	%xmm1, %xmm8
	movaps	-28 * SIZE(BO), %xmm1
	addps	%xmm2, %xmm9
	pshufd	$0x39, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	pshufd	$0x39, %xmm2, %xmm3
	mulps	%xmm0, %xmm2

	addps	%xmm5, %xmm10
	addps	%xmm6, %xmm11
	pshufd	$0x39, %xmm3, %xmm4
 	movaps	-28 * SIZE(AO), %xmm7
	mulps	%xmm0, %xmm3
	mulps	%xmm0, %xmm4

	addps	%xmm1, %xmm12
	movaps	-24 * SIZE(BO), %xmm1
	addps	%xmm2, %xmm13
	pshufd	$0x39, %xmm1, %xmm2
	mulps	%xmm7, %xmm1
	pshufd	$0x39, %xmm2, %xmm5
	mulps	%xmm7, %xmm2

	addps	%xmm3, %xmm14
	addps	%xmm4, %xmm15
	pshufd	$0x39, %xmm5, %xmm6
	mulps	%xmm7, %xmm5
	mulps	%xmm7, %xmm6

	addps	%xmm1, %xmm8
	movaps	-20 * SIZE(BO), %xmm1
	addps	%xmm2, %xmm9
	pshufd	$0x39, %xmm1, %xmm2
	mulps	%xmm7, %xmm1
	pshufd	$0x39, %xmm2, %xmm3
	mulps	%xmm7, %xmm2

	addps	%xmm5, %xmm10
	addps	%xmm6, %xmm11
	pshufd	$0x39, %xmm3, %xmm4
	movaps	-24 * SIZE(AO), %xmm0
	mulps	%xmm7, %xmm3
	mulps	%xmm7, %xmm4

	addps	%xmm1, %xmm12
	movaps	-16 * SIZE(BO), %xmm1
	addps	%xmm2, %xmm13
	pshufd	$0x39, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	pshufd	$0x39, %xmm2, %xmm5
	mulps	%xmm0, %xmm2

	addps	%xmm3, %xmm14
	addps	%xmm4, %xmm15
	pshufd	$0x39, %xmm5, %xmm6
	mulps	%xmm0, %xmm5
	mulps	%xmm0, %xmm6

	addps	%xmm1, %xmm8
	movaps	-12 * SIZE(BO), %xmm1
	addps	%xmm2, %xmm9
	pshufd	$0x39, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	pshufd	$0x39, %xmm2, %xmm3
	mulps	%xmm0, %xmm2

	addps	%xmm5, %xmm10
	addps	%xmm6, %xmm11
	pshufd	$0x39, %xmm3, %xmm4
	movaps	-20 * SIZE(AO), %xmm7
	mulps	%xmm0, %xmm3
	mulps	%xmm0, %xmm4

	addps	%xmm1, %xmm12
	movaps	 -8 * SIZE(BO), %xmm1
	addps	%xmm2, %xmm13
	pshufd	$0x39, %xmm1, %xmm2
	mulps	%xmm7, %xmm1
	pshufd	$0x39, %xmm2, %xmm5
	mulps	%xmm7, %xmm2

	addps	%xmm3, %xmm14
	addps	%xmm4, %xmm15
	pshufd	$0x39, %xmm5, %xmm6
	mulps	%xmm7, %xmm5
	mulps	%xmm7, %xmm6

	addps	%xmm1, %xmm8
	movaps	 -4 * SIZE(BO), %xmm1
	addps	%xmm2, %xmm9
	subq	$-32 * SIZE, BO
	pshufd	$0x39, %xmm1, %xmm2
	mulps	%xmm7, %xmm1
	pshufd	$0x39, %xmm2, %xmm3
	mulps	%xmm7, %xmm2

	addps	%xmm5, %xmm10
	addps	%xmm6, %xmm11
	pshufd	$0x39, %xmm3, %xmm4
	mulps	%xmm7, %xmm3
	movaps	-16 * SIZE(AO), %xmm0
	mulps	%xmm7, %xmm4

	subq	$-16 * SIZE, AO
	decq	%rax
	BRANCH
	jg	.L12
	ALIGN_3

.L15:
	movddup	ALPHA, %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	BRANCH
	je	.L18
	ALIGN_3

.L16:
	addps	%xmm1, %xmm12
	movaps	-32 * SIZE(BO), %xmm1
	addps	%xmm2, %xmm13
	pshufd	$0x39, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	pshufd	$0x39, %xmm2, %xmm5
	mulps	%xmm0, %xmm2

	addps	%xmm3, %xmm14
	addps	%xmm4, %xmm15
	pshufd	$0x39, %xmm5, %xmm6
	mulps	%xmm0, %xmm5
	mulps	%xmm0, %xmm6

	addps	%xmm1, %xmm8
	movaps	-28 * SIZE(BO), %xmm1
	addps	%xmm2, %xmm9
	pshufd	$0x39, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	pshufd	$0x39, %xmm2, %xmm3
	mulps	%xmm0, %xmm2

	addps	%xmm5, %xmm10
	addps	%xmm6, %xmm11
	pshufd	$0x39, %xmm3, %xmm4
	mulps	%xmm0, %xmm3
	mulps	%xmm0, %xmm4

	movaps	-28 * SIZE(AO), %xmm0

	addq	$4 * SIZE, AO
	addq	$8 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L16
	ALIGN_3

.L18:
	addps	%xmm1, %xmm12
	addps	%xmm2, %xmm13
	addps	%xmm3, %xmm14
	addps	%xmm4, %xmm15

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

	mulps	%xmm7, %xmm8
	mulps	%xmm7, %xmm9
	mulps	%xmm7, %xmm10
	mulps	%xmm7, %xmm11

	mulps	%xmm7, %xmm12
	mulps	%xmm7, %xmm13
	mulps	%xmm7, %xmm14
	mulps	%xmm7, %xmm15

	leaq	(LDC, LDC, 2), %rax

#ifndef TRMMKERNEL
	movups	(CO1), %xmm0
	movups	(CO1, LDC,  1), %xmm1
	movups	(CO1, LDC,  2), %xmm2
	movups	(CO1, %rax, 1), %xmm3

	movups	(CO2), %xmm4
	movups	(CO2, LDC,  1), %xmm5
	movups	(CO2, LDC,  2), %xmm6
	movups	(CO2, %rax, 1), %xmm7

	addps	%xmm0, %xmm8
	addps	%xmm1, %xmm9
	addps	%xmm2, %xmm10
	addps	%xmm3, %xmm11
	addps	%xmm4, %xmm12
	addps	%xmm5, %xmm13
	addps	%xmm6, %xmm14
	addps	%xmm7, %xmm15
#endif

	movups	%xmm8,  (CO1)
	movups	%xmm9,  (CO1, LDC,  1)
	movups	%xmm10, (CO1, LDC,  2)
	movups	%xmm11, (CO1, %rax, 1)

	movups	%xmm12, (CO2)
	movups	%xmm13, (CO2, LDC,  1)
	movups	%xmm14, (CO2, LDC,  2)
	movups	%xmm15, (CO2, %rax, 1)

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

	addq	$4 * SIZE, CO1
	addq	$4 * SIZE, CO2
	decq	I
	BRANCH
	jg	.L11
	ALIGN_4

.L20:
	testq	$2, M
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
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 8), BO
#endif

	xorps	%xmm1, %xmm1
	movddup	-32 * SIZE(AO), %xmm0
	xorps	%xmm2, %xmm2
	movaps	-32 * SIZE(BO), %xmm5
	xorps	%xmm3, %xmm3
	xorps	%xmm4, %xmm4

	xorps	%xmm8,  %xmm8
	xorps	%xmm9,  %xmm9
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
	addq	$2, %rax
#else
	addq	$8, %rax
#endif
	movq	%rax, KKK
#endif
	sarq	$2, %rax
	NOBRANCH
	jle	.L25
	ALIGN_3

.L22:
	addps	%xmm1, %xmm8
	pshufd	$0x50, %xmm5, %xmm1
	mulps	%xmm0, %xmm1
	addps	%xmm2, %xmm9
	pshufd	$0xfa, %xmm5, %xmm2
	mulps	%xmm0, %xmm2
	movaps	-28 * SIZE(BO), %xmm5

	addps	%xmm3, %xmm10
	pshufd	$0x50, %xmm5, %xmm3
	mulps	%xmm0, %xmm3
	addps	%xmm4, %xmm11
	pshufd	$0xfa, %xmm5, %xmm4
	mulps	%xmm0, %xmm4
	movaps	-24 * SIZE(BO), %xmm5

	movddup	-30 * SIZE(AO), %xmm0

	addps	%xmm1, %xmm8
	pshufd	$0x50, %xmm5, %xmm1
	mulps	%xmm0, %xmm1
	addps	%xmm2, %xmm9
	pshufd	$0xfa, %xmm5, %xmm2
	mulps	%xmm0, %xmm2
	movaps	-20 * SIZE(BO), %xmm5

	addps	%xmm3, %xmm10
	pshufd	$0x50, %xmm5, %xmm3
	mulps	%xmm0, %xmm3
	addps	%xmm4, %xmm11
	pshufd	$0xfa, %xmm5, %xmm4
	mulps	%xmm0, %xmm4
	movaps	-16 * SIZE(BO), %xmm5

	movddup	-28 * SIZE(AO), %xmm0

	addps	%xmm1, %xmm8
	pshufd	$0x50, %xmm5, %xmm1
	mulps	%xmm0, %xmm1
	addps	%xmm2, %xmm9
	pshufd	$0xfa, %xmm5, %xmm2
	mulps	%xmm0, %xmm2
	movaps	-12 * SIZE(BO), %xmm5

	addps	%xmm3, %xmm10
	pshufd	$0x50, %xmm5, %xmm3
	mulps	%xmm0, %xmm3
	addps	%xmm4, %xmm11
	pshufd	$0xfa, %xmm5, %xmm4
	mulps	%xmm0, %xmm4
	movaps	 -8 * SIZE(BO), %xmm5

	movddup	-26 * SIZE(AO), %xmm0

	addps	%xmm1, %xmm8
	pshufd	$0x50, %xmm5, %xmm1
	mulps	%xmm0, %xmm1
	addps	%xmm2, %xmm9
	pshufd	$0xfa, %xmm5, %xmm2
	mulps	%xmm0, %xmm2
	movaps	 -4 * SIZE(BO), %xmm5

	addps	%xmm3, %xmm10
	pshufd	$0x50, %xmm5, %xmm3
	mulps	%xmm0, %xmm3
	addps	%xmm4, %xmm11
	pshufd	$0xfa, %xmm5, %xmm4
	mulps	%xmm0, %xmm4
	movaps	 0 * SIZE(BO), %xmm5

	movddup	-24 * SIZE(AO), %xmm0

	subq	$-32 * SIZE, BO
	subq	$ -8 * SIZE, AO

	subq	$1, %rax
	BRANCH
	jg	.L22
	ALIGN_3

.L25:
	movddup	ALPHA, %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	BRANCH
	je	.L28
	ALIGN_3

.L26:
	addps	%xmm1, %xmm8
	pshufd	$0x50, %xmm5, %xmm1
	mulps	%xmm0, %xmm1
	addps	%xmm2, %xmm9
	pshufd	$0xfa, %xmm5, %xmm2
	mulps	%xmm0, %xmm2
	movaps	-28 * SIZE(BO), %xmm5

	addps	%xmm3, %xmm10
	pshufd	$0x50, %xmm5, %xmm3
	mulps	%xmm0, %xmm3
	addps	%xmm4, %xmm11
	pshufd	$0xfa, %xmm5, %xmm4
	mulps	%xmm0, %xmm4
	movaps	-24 * SIZE(BO), %xmm5

	movddup	-30 * SIZE(AO), %xmm0

	addq	$2 * SIZE, AO
	addq	$8 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L26
	ALIGN_3

.L28:
	addps	%xmm1, %xmm8
	addps	%xmm2, %xmm9
	addps	%xmm3, %xmm10
	addps	%xmm4, %xmm11

	mulps	%xmm7, %xmm8
	mulps	%xmm7, %xmm9
	mulps	%xmm7, %xmm10
	mulps	%xmm7, %xmm11

	leaq	(LDC, LDC, 2), %rax

#ifndef TRMMKERNEL
	movsd	(CO1), %xmm0
	movhps	(CO1, LDC,  1), %xmm0
	movsd	(CO1, LDC,  2), %xmm1
	movhps	(CO1, %rax, 1), %xmm1

	movsd	(CO2), %xmm2
	movhps	(CO2, LDC,  1), %xmm2
	movsd	(CO2, LDC,  2), %xmm3
	movhps	(CO2, %rax, 1), %xmm3

	addps	%xmm0, %xmm8
	addps	%xmm1, %xmm9
	addps	%xmm2, %xmm10
	addps	%xmm3, %xmm11
#endif

	movsd	%xmm8,  (CO1)
	movhps	%xmm8,  (CO1, LDC,  1)
	movsd	%xmm9, (CO1, LDC,  2)
	movhps	%xmm9, (CO1, %rax, 1)

	movsd	%xmm10, (CO2)
	movhps	%xmm10, (CO2, LDC,  1)
	movsd	%xmm11, (CO2, LDC,  2)
	movhps	%xmm11, (CO2, %rax, 1)

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

	addq	$2 * SIZE, CO1
	addq	$2 * SIZE, CO2
	ALIGN_4

.L30:
	testq	$1, M
	BRANCH
	jle	.L39

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	movq	B, BO
#else
	movq	B, BO

	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 8), BO
#endif

	xorps	%xmm2, %xmm2
	movsd	-32 * SIZE(AO), %xmm0
	xorps	%xmm3, %xmm3
	xorps	%xmm8,  %xmm8
	xorps	%xmm12, %xmm12

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
	jle	.L35
	ALIGN_3

.L32:
	pshufd	$0x00, %xmm0, %xmm1
	addps	%xmm2, %xmm8
	movaps	-32 * SIZE(BO), %xmm2
	mulps	%xmm1, %xmm2
	addps	%xmm3, %xmm12
	movaps	-28 * SIZE(BO), %xmm3
	mulps	%xmm1, %xmm3

	pshufd	$0x55, %xmm0, %xmm1
	movsd	-30 * SIZE(AO), %xmm0
	addps	%xmm2, %xmm8
	movaps	-24 * SIZE(BO), %xmm2
	mulps	%xmm1, %xmm2
	addps	%xmm3, %xmm12
	movaps	-20 * SIZE(BO), %xmm3
	mulps	%xmm1, %xmm3

	pshufd	$0x00, %xmm0, %xmm1
	addps	%xmm2, %xmm8
	movaps	-16 * SIZE(BO), %xmm2
	mulps	%xmm1, %xmm2
	addps	%xmm3, %xmm12
	movaps	-12 * SIZE(BO), %xmm3
	mulps	%xmm1, %xmm3

	pshufd	$0x55, %xmm0, %xmm1
	movsd	-28 * SIZE(AO), %xmm0
	addps	%xmm2, %xmm8
	movaps	 -8 * SIZE(BO), %xmm2
	mulps	%xmm1, %xmm2
	addps	%xmm3, %xmm12
	movaps	 -4 * SIZE(BO), %xmm3
	mulps	%xmm1, %xmm3

	subq	$-32 * SIZE, BO
	subq	$ -4 * SIZE, AO

	subq	$1, %rax
	BRANCH
	jg	.L32
	ALIGN_3

.L35:
	movddup	ALPHA, %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	BRANCH
	je	.L38
	ALIGN_3

.L36:
	pshufd	$0x00, %xmm0, %xmm1
	movss	-31 * SIZE(AO), %xmm0
	addps	%xmm2, %xmm8
	movaps	-32 * SIZE(BO), %xmm2
	mulps	%xmm1, %xmm2
	addps	%xmm3, %xmm12
	movaps	-28 * SIZE(BO), %xmm3
	mulps	%xmm1, %xmm3

	addq	$1 * SIZE, AO
	addq	$8 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L36
	ALIGN_3

.L38:
	addps	%xmm2, %xmm8
	addps	%xmm3, %xmm12

	mulps	%xmm7, %xmm8
	mulps	%xmm7, %xmm12

	pshufd	$0xff, %xmm8,  %xmm11
	pshufd	$0xaa, %xmm8,  %xmm10
	pshufd	$0x55, %xmm8,  %xmm9
	pshufd	$0x00, %xmm8,  %xmm8

	pshufd	$0xff, %xmm12, %xmm15
	pshufd	$0xaa, %xmm12, %xmm14
	pshufd	$0x55, %xmm12, %xmm13
	pshufd	$0x00, %xmm12, %xmm12

	leaq	(LDC, LDC, 2), %rax

#ifndef TRMMKERNEL
	addss	(CO1), %xmm8
	addss	(CO1, LDC,  1), %xmm9
	addss	(CO1, LDC,  2), %xmm10
	addss	(CO1, %rax, 1), %xmm11

	addss	(CO2), %xmm12
	addss	(CO2, LDC,  1), %xmm13
	addss	(CO2, LDC,  2), %xmm14
	addss	(CO2, %rax, 1), %xmm15
#endif

	movss	%xmm8,  (CO1)
	movss	%xmm9,  (CO1, LDC,  1)
	movss	%xmm10, (CO1, LDC,  2)
	movss	%xmm11, (CO1, %rax, 1)

	movss	%xmm12, (CO2)
	movss	%xmm13, (CO2, LDC,  1)
	movss	%xmm14, (CO2, LDC,  2)
	movss	%xmm15, (CO2, %rax, 1)

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

	ALIGN_4

.L39:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addq	$8, KK
#endif

	movq	BO, B

	leaq	(C, LDC, 8), C

	subq	$1, J
	BRANCH
	jg	.L10
	ALIGN_4

.L40:
	testq	$4, N
	jle	.L70

#if defined(TRMMKERNEL) && defined(LEFT)
        movq    OFFSET, %rax
	movq    %rax, KK
#endif

	movq	C, CO1
	leaq	(C, LDC, 2), CO2
	movq	A, AO

	movq	M,  I
	sarq	$2, I
	NOBRANCH
	jle	.L50
	ALIGN_4

.L41:
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

	xorps	%xmm1, %xmm1
	movaps	-32 * SIZE(AO), %xmm0
	xorps	%xmm2, %xmm2
	xorps	%xmm3, %xmm3
	xorps	%xmm4, %xmm4

	xorps	%xmm8,  %xmm8
	prefetcht2     4 * SIZE(CO1)
	xorps	%xmm9,  %xmm9
	prefetcht2     4 * SIZE(CO1, LDC,  1)
	xorps	%xmm10, %xmm10
	prefetcht2     4 * SIZE(CO2)
	xorps	%xmm11, %xmm11
	prefetcht2     4 * SIZE(CO2, LDC,  1)

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
	jle	.L45
	ALIGN_3

.L42:
	PREFETCH	(PREFETCHSIZE +  0) * SIZE(AO)

	addps	%xmm1, %xmm8
	movaps	-32 * SIZE(BO), %xmm1
	addps	%xmm2, %xmm9
	pshufd	$0x39, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	addps	%xmm3, %xmm10
	pshufd	$0x39, %xmm2, %xmm3
	mulps	%xmm0, %xmm2

	addps	%xmm4, %xmm11
	pshufd	$0x39, %xmm3, %xmm4
	mulps	%xmm0, %xmm3
	mulps	%xmm0, %xmm4
	movaps	-28 * SIZE(AO), %xmm0

	addps	%xmm1, %xmm8
	movaps	-28 * SIZE(BO), %xmm1
	addps	%xmm2, %xmm9
	pshufd	$0x39, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	addps	%xmm3, %xmm10
	pshufd	$0x39, %xmm2, %xmm3
	mulps	%xmm0, %xmm2

	addps	%xmm4, %xmm11
	pshufd	$0x39, %xmm3, %xmm4
	mulps	%xmm0, %xmm3
	mulps	%xmm0, %xmm4
	movaps	-24 * SIZE(AO), %xmm0

	addps	%xmm1, %xmm8
	movaps	-24 * SIZE(BO), %xmm1
	addps	%xmm2, %xmm9
	pshufd	$0x39, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	addps	%xmm3, %xmm10
	pshufd	$0x39, %xmm2, %xmm3
	mulps	%xmm0, %xmm2

	addps	%xmm4, %xmm11
	pshufd	$0x39, %xmm3, %xmm4
	mulps	%xmm0, %xmm3
	mulps	%xmm0, %xmm4
	movaps	-20 * SIZE(AO), %xmm0

	addps	%xmm1, %xmm8
	movaps	-20 * SIZE(BO), %xmm1
	addps	%xmm2, %xmm9
	pshufd	$0x39, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	addps	%xmm3, %xmm10
	pshufd	$0x39, %xmm2, %xmm3
	mulps	%xmm0, %xmm2

	addps	%xmm4, %xmm11
	pshufd	$0x39, %xmm3, %xmm4
	mulps	%xmm0, %xmm3
	mulps	%xmm0, %xmm4
	movaps	-16 * SIZE(AO), %xmm0

	subq	$-16 * SIZE, AO
	subq	$-16 * SIZE, BO
	subq	$1, %rax
	BRANCH
	jg	.L42
	ALIGN_3

.L45:
	movddup	ALPHA, %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	BRANCH
	je	.L48
	ALIGN_3

.L46:
	addps	%xmm1, %xmm8
	movaps	-32 * SIZE(BO), %xmm1
	addps	%xmm2, %xmm9
	pshufd	$0x39, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	addps	%xmm3, %xmm10
	pshufd	$0x39, %xmm2, %xmm3
	mulps	%xmm0, %xmm2

	addps	%xmm4, %xmm11
	pshufd	$0x39, %xmm3, %xmm4
	mulps	%xmm0, %xmm3
	mulps	%xmm0, %xmm4
	movaps	-28 * SIZE(AO), %xmm0

	addq	$4 * SIZE, AO
	addq	$4 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L46
	ALIGN_3

.L48:
	addps	%xmm1, %xmm8
	addps	%xmm2, %xmm9
	addps	%xmm3, %xmm10
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

	mulps	%xmm7, %xmm8
	mulps	%xmm7, %xmm9
	mulps	%xmm7, %xmm10
	mulps	%xmm7, %xmm11

#ifndef TRMMKERNEL
	movsd	0 * SIZE(CO1), %xmm0
	movhps	2 * SIZE(CO1), %xmm0
	movsd	0 * SIZE(CO1, LDC,  1), %xmm1
 	movhps	2 * SIZE(CO1, LDC,  1), %xmm1

	movsd	0 * SIZE(CO2), %xmm2
	movhps	2 * SIZE(CO2), %xmm2
	movsd	0 * SIZE(CO2, LDC,  1), %xmm3
	movhps	2 * SIZE(CO2, LDC,  1), %xmm3

	addps	%xmm0, %xmm8
	addps	%xmm1, %xmm9
	addps	%xmm2, %xmm10
	addps	%xmm3, %xmm11
#endif

	movsd	%xmm8,  0 * SIZE(CO1)
	movhps	%xmm8,  2 * SIZE(CO1)
	movsd	%xmm9,  0 * SIZE(CO1, LDC,  1)
	movhps	%xmm9,  2 * SIZE(CO1, LDC,  1)

	movsd	%xmm10, 0 * SIZE(CO2)
	movhps	%xmm10, 2 * SIZE(CO2)
	movsd	%xmm11, 0 * SIZE(CO2, LDC,  1)
	movhps	%xmm11, 2 * SIZE(CO2, LDC,  1)

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

	addq	$4 * SIZE, CO1
	addq	$4 * SIZE, CO2
	decq	I
	BRANCH
	jg	.L41
	ALIGN_4

.L50:
	testq	$2, M
	BRANCH
	jle	.L60

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

	xorps	%xmm1, %xmm1
	movddup	-32 * SIZE(AO), %xmm0
	xorps	%xmm2, %xmm2
	movaps	-32 * SIZE(BO), %xmm5
	xorps	%xmm8,  %xmm8
	xorps	%xmm9,  %xmm9

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
	jle	.L55
	ALIGN_3

.L52:
	addps	%xmm1, %xmm8
	pshufd	$0x50, %xmm5, %xmm1
	mulps	%xmm0, %xmm1
	addps	%xmm2, %xmm9
	pshufd	$0xfa, %xmm5, %xmm2
	movaps	-28 * SIZE(BO), %xmm5
	mulps	%xmm0, %xmm2
	movddup	-30 * SIZE(AO), %xmm0

	addps	%xmm1, %xmm8
	pshufd	$0x50, %xmm5, %xmm1
	mulps	%xmm0, %xmm1
	addps	%xmm2, %xmm9
	pshufd	$0xfa, %xmm5, %xmm2
	movaps	-24 * SIZE(BO), %xmm5
	mulps	%xmm0, %xmm2
	movddup	-28 * SIZE(AO), %xmm0

	addps	%xmm1, %xmm8
	pshufd	$0x50, %xmm5, %xmm1
	mulps	%xmm0, %xmm1
	addps	%xmm2, %xmm9
	pshufd	$0xfa, %xmm5, %xmm2
	movaps	-20 * SIZE(BO), %xmm5
	mulps	%xmm0, %xmm2
	movddup	-26 * SIZE(AO), %xmm0

	addps	%xmm1, %xmm8
	pshufd	$0x50, %xmm5, %xmm1
	mulps	%xmm0, %xmm1
	addps	%xmm2, %xmm9
	pshufd	$0xfa, %xmm5, %xmm2
	movaps	-16 * SIZE(BO), %xmm5
	mulps	%xmm0, %xmm2
	movddup	-24 * SIZE(AO), %xmm0

	subq	$-16 * SIZE, BO
	subq	$ -8 * SIZE, AO

	subq	$1, %rax
	BRANCH
	jg	.L52
	ALIGN_3

.L55:
	movddup	ALPHA, %xmm7

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
	addps	%xmm1, %xmm8
	pshufd	$0x50, %xmm5, %xmm1
	mulps	%xmm0, %xmm1
	addps	%xmm2, %xmm9
	pshufd	$0xfa, %xmm5, %xmm2
	movaps	-28 * SIZE(BO), %xmm5
	mulps	%xmm0, %xmm2
	movddup	-30 * SIZE(AO), %xmm0

	addq	$2 * SIZE, AO
	addq	$4 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L56
	ALIGN_3

.L58:
	addps	%xmm1, %xmm8
	addps	%xmm2, %xmm9

	mulps	%xmm7, %xmm8
	mulps	%xmm7, %xmm9

#ifndef TRMMKERNEL
	movsd	(CO1), %xmm0
	movhps	(CO1, LDC,  1), %xmm0
	movsd	(CO2), %xmm1
	movhps	(CO2, LDC,  1), %xmm1

	addps	%xmm0, %xmm8
	addps	%xmm1, %xmm9
#endif

	movsd	%xmm8,  (CO1)
	movhps	%xmm8,  (CO1, LDC,  1)
	movsd	%xmm9,  (CO2)
	movhps	%xmm9,  (CO2, LDC,  1)

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

	addq	$2 * SIZE, CO1
	addq	$2 * SIZE, CO2
	ALIGN_4

.L60:
	testq	$1, M
	BRANCH
	jle	.L69

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

	xorps	%xmm2, %xmm2
	movsd	-32 * SIZE(AO), %xmm0
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
	addq	$1, %rax
#else
	addq	$4, %rax
#endif
	movq	%rax, KKK
#endif
	sarq	$2, %rax
	NOBRANCH
	jle	.L65
	ALIGN_3

.L62:
	pshufd	$0x00, %xmm0, %xmm1
	addps	%xmm2, %xmm8
	movaps	-32 * SIZE(BO), %xmm2
	mulps	%xmm1, %xmm2

	pshufd	$0x55, %xmm0, %xmm1
	movsd	-30 * SIZE(AO), %xmm0
	addps	%xmm2, %xmm9
	movaps	-28 * SIZE(BO), %xmm2
	mulps	%xmm1, %xmm2

	pshufd	$0x00, %xmm0, %xmm1
	addps	%xmm2, %xmm8
	movaps	-24 * SIZE(BO), %xmm2
	mulps	%xmm1, %xmm2

	pshufd	$0x55, %xmm0, %xmm1
	movsd	-28 * SIZE(AO), %xmm0
	addps	%xmm2, %xmm9
	movaps	-20 * SIZE(BO), %xmm2
	mulps	%xmm1, %xmm2

	subq	$-16 * SIZE, BO
	subq	$ -4 * SIZE, AO

	subq	$1, %rax
	BRANCH
	jg	.L62
	addps	%xmm9, %xmm8
	ALIGN_3

.L65:
	movddup	ALPHA, %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	BRANCH
	je	.L68
	ALIGN_3

.L66:
	pshufd	$0x00, %xmm0, %xmm1
	movss	-31 * SIZE(AO), %xmm0
	addps	%xmm2, %xmm8
	movaps	-32 * SIZE(BO), %xmm2
	mulps	%xmm1, %xmm2

	addq	$1 * SIZE, AO
	addq	$4 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L66
	ALIGN_3

.L68:
	addps	%xmm2, %xmm8
	mulps	%xmm7, %xmm8

	pshufd	$0xff, %xmm8,  %xmm11
	pshufd	$0xaa, %xmm8,  %xmm10
	pshufd	$0x55, %xmm8,  %xmm9
	pshufd	$0x00, %xmm8,  %xmm8

#ifndef TRMMKERNEL
	addss	(CO1), %xmm8
	addss	(CO1, LDC,  1), %xmm9
	addss	(CO2), %xmm10
	addss	(CO2, LDC,  1), %xmm11
#endif

	movss	%xmm8,  (CO1)
	movss	%xmm9,  (CO1, LDC,  1)
	movss	%xmm10, (CO2)
	movss	%xmm11, (CO2, LDC,  1)

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

	ALIGN_4

.L69:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addq	$4, KK
#endif

	movq	BO, B

	leaq	(C, LDC, 4), C
	ALIGN_4

.L70:
	testq	$2, N
	jle	.L100

#if defined(TRMMKERNEL) && defined(LEFT)
        movq    OFFSET, %rax
	movq    %rax, KK
#endif

	movq	C, CO1
	leaq	(C, LDC), CO2
	movq	A, AO

	movq	M,  I
	sarq	$2, I
	NOBRANCH
	jle	.L80
	ALIGN_4

.L71:
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

	xorps	%xmm1, %xmm1
	movaps	-32 * SIZE(AO), %xmm0
	xorps	%xmm2, %xmm2
	movsd	-32 * SIZE(BO), %xmm3

	xorps	%xmm8,  %xmm8
	prefetcht2     4 * SIZE(CO1)
	xorps	%xmm9,  %xmm9
	prefetcht2     4 * SIZE(CO2)

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
	jle	.L75
	ALIGN_3

.L72:
	PREFETCH	(PREFETCHSIZE +  0) * SIZE(AO)

	addps	%xmm1, %xmm8
	pshufd	$0x00,  %xmm3, %xmm1
	mulps	%xmm0, %xmm1
	addps	%xmm2, %xmm9
	pshufd	$0x55,  %xmm3, %xmm2
	movsd	-30 * SIZE(BO), %xmm3
	mulps	%xmm0, %xmm2
	movaps	-28 * SIZE(AO), %xmm0

	addps	%xmm1, %xmm8
	pshufd	$0x00,  %xmm3, %xmm1
	mulps	%xmm0, %xmm1
	addps	%xmm2, %xmm9
	pshufd	$0x55,  %xmm3, %xmm2
	movsd	-28 * SIZE(BO), %xmm3
	mulps	%xmm0, %xmm2
	movaps	-24 * SIZE(AO), %xmm0

	addps	%xmm1, %xmm8
	pshufd	$0x00,  %xmm3, %xmm1
	mulps	%xmm0, %xmm1
	addps	%xmm2, %xmm9
	pshufd	$0x55,  %xmm3, %xmm2
	movsd	-26 * SIZE(BO), %xmm3
	mulps	%xmm0, %xmm2
	movaps	-20 * SIZE(AO), %xmm0

	addps	%xmm1, %xmm8
	pshufd	$0x00,  %xmm3, %xmm1
	mulps	%xmm0, %xmm1
	addps	%xmm2, %xmm9
	pshufd	$0x55,  %xmm3, %xmm2
	movsd	-24 * SIZE(BO), %xmm3
	mulps	%xmm0, %xmm2
	movaps	-16 * SIZE(AO), %xmm0

	subq	$-16 * SIZE, AO
	subq	$ -8 * SIZE, BO
	subq	$1, %rax
	BRANCH
	jg	.L72
	ALIGN_3

.L75:
	movddup	ALPHA, %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	BRANCH
	je	.L78
	ALIGN_3

.L76:
	addps	%xmm1, %xmm8
	pshufd	$0x00,  %xmm3, %xmm1
	mulps	%xmm0, %xmm1
	addps	%xmm2, %xmm9
	pshufd	$0x55,  %xmm3, %xmm2
	movsd	-30 * SIZE(BO), %xmm3
	mulps	%xmm0, %xmm2
	movaps	-28 * SIZE(AO), %xmm0

	addq	$4 * SIZE, AO
	addq	$2 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L76
	ALIGN_3

.L78:
	addps	%xmm1, %xmm8
	addps	%xmm2, %xmm9

	mulps	%xmm7, %xmm8
	mulps	%xmm7, %xmm9

#ifndef TRMMKERNEL
	movsd	0 * SIZE(CO1), %xmm0
	movhps	2 * SIZE(CO1), %xmm0
	movsd	0 * SIZE(CO2), %xmm1
	movhps	2 * SIZE(CO2), %xmm1

	addps	%xmm0, %xmm8
	addps	%xmm1, %xmm9
#endif

	movsd	%xmm8,  0 * SIZE(CO1)
	movhps	%xmm8,  2 * SIZE(CO1)
	movsd	%xmm9,  0 * SIZE(CO2)
	movhps	%xmm9,  2 * SIZE(CO2)

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

	addq	$4 * SIZE, CO1
	addq	$4 * SIZE, CO2
	decq	I
	BRANCH
	jg	.L71
	ALIGN_4

.L80:
	testq	$2, M
	BRANCH
	jle	.L90

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

	xorps	%xmm1, %xmm1
	movddup	-32 * SIZE(AO), %xmm0
	xorps	%xmm2, %xmm2
	movsd	-32 * SIZE(BO), %xmm5
	xorps	%xmm8,  %xmm8
	xorps	%xmm9,  %xmm9

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
	jle	.L85
	ALIGN_3

.L82:
	addps	%xmm1, %xmm8
	movsd	-32 * SIZE(BO), %xmm1
	unpcklps    %xmm1, %xmm1
	mulps	%xmm0, %xmm1
	movddup	-30 * SIZE(AO), %xmm0

	addps	%xmm1, %xmm8
	movsd	-30 * SIZE(BO), %xmm1
	unpcklps    %xmm1, %xmm1
	mulps	%xmm0, %xmm1
	movddup	-28 * SIZE(AO), %xmm0

	addps	%xmm1, %xmm8
	movsd	-28 * SIZE(BO), %xmm1
	unpcklps    %xmm1, %xmm1
	mulps	%xmm0, %xmm1
	movddup	-26 * SIZE(AO), %xmm0

	addps	%xmm1, %xmm8
	movsd	-26 * SIZE(BO), %xmm1
	unpcklps    %xmm1, %xmm1
	mulps	%xmm0, %xmm1
	movddup	-24 * SIZE(AO), %xmm0

	subq	$-8 * SIZE, BO
	subq	$-8 * SIZE, AO

	subq	$1, %rax
	BRANCH
	jg	.L82
	ALIGN_3

.L85:
	movddup	ALPHA, %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	BRANCH
	je	.L88
	ALIGN_3

.L86:
	addps	%xmm1, %xmm8
	movsd	-32 * SIZE(BO), %xmm1
	unpcklps    %xmm1, %xmm1
	mulps	%xmm0, %xmm1
	movddup	-30 * SIZE(AO), %xmm0

	addq	$2 * SIZE, AO
	addq	$2 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L86
	ALIGN_3

.L88:
	addps	%xmm1, %xmm8
	mulps	%xmm7, %xmm8

#ifndef TRMMKERNEL
	movsd	(CO1), %xmm0
	movhps	(CO2), %xmm0

	addps	%xmm0, %xmm8
#endif

	movsd	%xmm8,  (CO1)
	movhps	%xmm8,  (CO2)

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

	addq	$2 * SIZE, CO1
	addq	$2 * SIZE, CO2
	ALIGN_4

.L90:
	testq	$1, M
	BRANCH
	jle	.L99

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

	xorps	%xmm2, %xmm2
	movsd	-32 * SIZE(AO), %xmm0
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
	addq	$1, %rax
#else
	addq	$2, %rax
#endif
	movq	%rax, KKK
#endif
	sarq	$2, %rax
	NOBRANCH
	jle	.L95
	ALIGN_3

.L92:
	pshufd	$0x00, %xmm0, %xmm1
	addps	%xmm2, %xmm8
	movsd	-32 * SIZE(BO), %xmm2
	mulps	%xmm1, %xmm2

	pshufd	$0x55, %xmm0, %xmm1
	movsd	-30 * SIZE(AO), %xmm0
	addps	%xmm2, %xmm9
	movsd	-30 * SIZE(BO), %xmm2
	mulps	%xmm1, %xmm2

	pshufd	$0x00, %xmm0, %xmm1
	addps	%xmm2, %xmm8
	movsd	-28 * SIZE(BO), %xmm2
	mulps	%xmm1, %xmm2

	pshufd	$0x55, %xmm0, %xmm1
	movsd	-28 * SIZE(AO), %xmm0
	addps	%xmm2, %xmm9
	movsd	-26 * SIZE(BO), %xmm2
	mulps	%xmm1, %xmm2

	subq	$-4 * SIZE, AO
	subq	$-8 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L92
	addps	%xmm9, %xmm8
	ALIGN_3

.L95:
	movddup	ALPHA, %xmm7

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
	pshufd	$0x00, %xmm0, %xmm1
	movss	-31 * SIZE(AO), %xmm0
	addps	%xmm2, %xmm8
	movsd	-32 * SIZE(BO), %xmm2
	mulps	%xmm1, %xmm2

	addq	$1 * SIZE, AO
	addq	$2 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L96
	ALIGN_3

.L98:
	addps	%xmm2, %xmm8
	mulps	%xmm7, %xmm8

	pshufd	$0x55, %xmm8,  %xmm9
	pshufd	$0x00, %xmm8,  %xmm8

#ifndef TRMMKERNEL
	addss	(CO1), %xmm8
	addss	(CO2), %xmm9
#endif

	movss	%xmm8, (CO1)
	movss	%xmm9, (CO2)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 2), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$1, KK
#endif

	ALIGN_4

.L99:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addq	$2, KK
#endif

	movq	BO, B

	leaq	(C, LDC, 2), C
	ALIGN_4

.L100:
	testq	$1, N
	jle	.L999

#if defined(TRMMKERNEL) && defined(LEFT)
        movq    OFFSET, %rax
	movq    %rax, KK
#endif

	movq	C, CO1
	movq	A, AO

	movq	M,  I
	sarq	$2, I
	NOBRANCH
	jle	.L110
	ALIGN_4

.L101:
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

	xorps	%xmm1, %xmm1
	movaps	-32 * SIZE(AO), %xmm0
	movsd	-32 * SIZE(BO), %xmm3
	xorps	%xmm8,  %xmm8
	prefetcht2     4 * SIZE(CO1)

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
	PREFETCH	(PREFETCHSIZE +  0) * SIZE(AO)

	addps	%xmm1, %xmm8
	pshufd	$0x00,  %xmm3, %xmm1
	movss	-31 * SIZE(BO), %xmm3
	mulps	%xmm0, %xmm1
	movaps	-28 * SIZE(AO), %xmm0

	addps	%xmm1, %xmm8
	pshufd	$0x00,  %xmm3, %xmm1
	movss	-30 * SIZE(BO), %xmm3
	mulps	%xmm0, %xmm1
	movaps	-24 * SIZE(AO), %xmm0

	addps	%xmm1, %xmm8
	pshufd	$0x00,  %xmm3, %xmm1
	movss	-29 * SIZE(BO), %xmm3
	mulps	%xmm0, %xmm1
	movaps	-20 * SIZE(AO), %xmm0

	addps	%xmm1, %xmm8
	pshufd	$0x00,  %xmm3, %xmm1
	movss	-28 * SIZE(BO), %xmm3
	mulps	%xmm0, %xmm1
	movaps	-16 * SIZE(AO), %xmm0

	subq	$-16 * SIZE, AO
	subq	$ -4 * SIZE, BO
	subq	$1, %rax
	BRANCH
	jg	.L102
	ALIGN_3

.L105:
	movddup	ALPHA, %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	BRANCH
	je	.L108
	ALIGN_3

.L106:
	addps	%xmm1, %xmm8
	pshufd	$0x00,  %xmm3, %xmm1
	movss	-31 * SIZE(BO), %xmm3
	mulps	%xmm0, %xmm1
	movaps	-28 * SIZE(AO), %xmm0

	addq	$4 * SIZE, AO
	addq	$1 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L106
	ALIGN_3

.L108:
	addps	%xmm1, %xmm8

	mulps	%xmm7, %xmm8

#ifndef TRMMKERNEL
	movsd	0 * SIZE(CO1), %xmm0
	movhps	2 * SIZE(CO1), %xmm0

	addps	%xmm0, %xmm8
#endif

	movsd	%xmm8,  0 * SIZE(CO1)
	movhps	%xmm8,  2 * SIZE(CO1)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 1), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$4, KK
#endif

	addq	$4 * SIZE, CO1
	decq	I
	BRANCH
	jg	.L101
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

	xorps	%xmm1, %xmm1
	movddup	-32 * SIZE(AO), %xmm0
	xorps	%xmm8,  %xmm8

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
	addps	%xmm1, %xmm8
	movss	-32 * SIZE(BO), %xmm1
	unpcklps %xmm1, %xmm1
	mulps	%xmm0, %xmm1
	movsd	-30 * SIZE(AO), %xmm0

	addps	%xmm1, %xmm8
	movss	-31 * SIZE(BO), %xmm1
	unpcklps %xmm1, %xmm1
	mulps	%xmm0, %xmm1
	movsd	-28 * SIZE(AO), %xmm0

	addps	%xmm1, %xmm8
	movss	-30 * SIZE(BO), %xmm1
	unpcklps %xmm1, %xmm1
	mulps	%xmm0, %xmm1
	movsd	-26 * SIZE(AO), %xmm0

	addps	%xmm1, %xmm8
	movss	-29 * SIZE(BO), %xmm1
	unpcklps %xmm1, %xmm1
	mulps	%xmm0, %xmm1
	movsd	-24 * SIZE(AO), %xmm0

	subq	$-4 * SIZE, BO
	subq	$-8 * SIZE, AO

	subq	$1, %rax
	BRANCH
	jg	.L112
	ALIGN_3

.L115:
	movddup	ALPHA, %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	BRANCH
	je	.L118
	ALIGN_3

.L116:
	addps	%xmm1, %xmm8
	movss	-32 * SIZE(BO), %xmm1
	unpcklps %xmm1, %xmm1
	mulps	%xmm0, %xmm1
	movsd	-30 * SIZE(AO), %xmm0

	addq	$2 * SIZE, AO
	addq	$1 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L116
	ALIGN_3

.L118:
	addps	%xmm1, %xmm8
	mulps	%xmm7, %xmm8

#ifndef TRMMKERNEL
	movsd	(CO1), %xmm0
	addps	%xmm0, %xmm8
#endif

	movsd	%xmm8,  (CO1)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 1), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$2, KK
#endif

	addq	$2 * SIZE, CO1
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

	xorps	%xmm2, %xmm2
	movss	-32 * SIZE(AO), %xmm0
	xorps	%xmm8, %xmm8

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
	addss	%xmm2, %xmm8
	movss	-32 * SIZE(BO), %xmm2
	mulss	%xmm0, %xmm2
	movss	-31 * SIZE(AO), %xmm0

	addss	%xmm2, %xmm8
	movss	-31 * SIZE(BO), %xmm2
	mulss	%xmm0, %xmm2
	movss	-30 * SIZE(AO), %xmm0

	addss	%xmm2, %xmm8
	movss	-30 * SIZE(BO), %xmm2
	mulss	%xmm0, %xmm2
	movss	-29 * SIZE(AO), %xmm0

	addss	%xmm2, %xmm8
	movss	-29 * SIZE(BO), %xmm2
	mulss	%xmm0, %xmm2
	movss	-28 * SIZE(AO), %xmm0

	subq	$-4 * SIZE, AO
	subq	$-4 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L122
	ALIGN_3

.L125:
	movddup	ALPHA, %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	BRANCH
	je	.L128
	ALIGN_3

.L126:
	addss	%xmm2, %xmm8
	movss	-32 * SIZE(BO), %xmm2
	mulss	%xmm0, %xmm2
	movss	-31 * SIZE(AO), %xmm0

	addq	$1 * SIZE, AO
	addq	$1 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L126
	ALIGN_3

.L128:
	addps	%xmm2, %xmm8

	mulps	%xmm7, %xmm8

#ifndef TRMMKERNEL
	addss	(CO1), %xmm8
#endif

	movss	%xmm8, (CO1)
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
