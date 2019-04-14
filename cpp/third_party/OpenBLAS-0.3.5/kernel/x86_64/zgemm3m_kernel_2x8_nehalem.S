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

#define BX	%rdx

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

#define PREFETCHSIZE  (8 * 1 - 4)
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
	movsd	OLD_ALPHA_I, %xmm1
#else
	movq	OLD_LDC,   LDC
#ifdef TRMMKERNEL
	movq	OLD_OFFSET, %r11
#endif
#endif

	movlps	 %xmm0, ALPHA_R
	movlps	 %xmm1, ALPHA_I

	subq	$-16 * SIZE, A
	subq	$-16 * SIZE, B

	movq	OLD_M, M
	movq	OLD_N, N
	movq	OLD_K, K

	salq	$ZBASE_SHIFT, LDC

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
	jle	.L30
	ALIGN_4

.L01:
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
	sarq	$1, I
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
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 8), BO
#endif

	prefetcht0	 -16 * SIZE(BB)
	subq		 $-8 * SIZE, BB

	xorps	%xmm1, %xmm1
	movaps	-16 * SIZE(AO), %xmm0
	xorps	%xmm2, %xmm2
	xorps	%xmm3, %xmm3
	xorps	%xmm4, %xmm4

	leaq	(LDC, LDC, 2), %rax

	xorps	%xmm8,  %xmm8
	prefetcht0     3 * SIZE(CO1)
	xorps	%xmm9,  %xmm9
	prefetcht0     7 * SIZE(CO1, LDC,  1)
	xorps	%xmm10, %xmm10
	prefetcht0     3 * SIZE(CO1, LDC,  2)
	xorps	%xmm11, %xmm11
	prefetcht0     7 * SIZE(CO1, %rax, 1)

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
	addq	$2, %rax
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

	addpd	%xmm1, %xmm12
	movaps	-16 * SIZE(BO), %xmm6
	addpd	%xmm2, %xmm13
	pshufd	$0x4e, %xmm6, %xmm2
	mulpd	%xmm0, %xmm6
	mulpd	%xmm0, %xmm2

	addpd	%xmm3, %xmm14
	movaps	-14 * SIZE(BO), %xmm3
	addpd	%xmm4, %xmm15
	pshufd	$0x4e, %xmm3, %xmm4
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm4

	addpd	%xmm6, %xmm8
	movaps	-12 * SIZE(BO), %xmm6
	addpd	%xmm2, %xmm9
	pshufd	$0x4e, %xmm6, %xmm2
	mulpd	%xmm0, %xmm6
	mulpd	%xmm0, %xmm2

	addpd	%xmm3, %xmm10
	movaps	-10 * SIZE(BO), %xmm3
	addpd	%xmm4, %xmm11
	pshufd	$0x4e, %xmm3, %xmm4
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm4

	addpd	%xmm6, %xmm12
	movaps	 -8 * SIZE(BO), %xmm1
	addpd	%xmm2, %xmm13
	movaps	-14 * SIZE(AO), %xmm5
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm5, %xmm1
	mulpd	%xmm5, %xmm2

	addpd	%xmm3, %xmm14
	movaps	 -6 * SIZE(BO), %xmm3
	addpd	%xmm4, %xmm15
	pshufd	$0x4e, %xmm3, %xmm4
	mulpd	%xmm5, %xmm3
	mulpd	%xmm5, %xmm4

	addpd	%xmm1, %xmm8
	movaps	 -4 * SIZE(BO), %xmm1
	addpd	%xmm2, %xmm9
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm5, %xmm1
	mulpd	%xmm5, %xmm2

	addpd	%xmm3, %xmm10
	movaps	 -2 * SIZE(BO), %xmm3
	addpd	%xmm4, %xmm11
	pshufd	$0x4e, %xmm3, %xmm4
	movaps	-12 * SIZE(AO), %xmm0
	mulpd	%xmm5, %xmm3
	mulpd	%xmm5, %xmm4

	addpd	%xmm1, %xmm12
	movaps	  0 * SIZE(BO), %xmm6
	addpd	%xmm2, %xmm13
	pshufd	$0x4e, %xmm6, %xmm2
	mulpd	%xmm0, %xmm6
	mulpd	%xmm0, %xmm2

	addpd	%xmm3, %xmm14
	movaps	  2 * SIZE(BO), %xmm3
	addpd	%xmm4, %xmm15
	pshufd	$0x4e, %xmm3, %xmm4
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm4

	addpd	%xmm6, %xmm8
	movaps	  4 * SIZE(BO), %xmm6
	addpd	%xmm2, %xmm9
	pshufd	$0x4e, %xmm6, %xmm2
	mulpd	%xmm0, %xmm6
	mulpd	%xmm0, %xmm2

	addpd	%xmm3, %xmm10
	movaps	  6 * SIZE(BO), %xmm3
	addpd	%xmm4, %xmm11
	pshufd	$0x4e, %xmm3, %xmm4
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm4

	addpd	%xmm6, %xmm12
	movaps	  8 * SIZE(BO), %xmm1
	addpd	%xmm2, %xmm13
	movaps	-10 * SIZE(AO), %xmm5
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm5, %xmm1
	mulpd	%xmm5, %xmm2

	addpd	%xmm3, %xmm14
	movaps	 10 * SIZE(BO), %xmm3
	addpd	%xmm4, %xmm15
	pshufd	$0x4e, %xmm3, %xmm4
	mulpd	%xmm5, %xmm3
	mulpd	%xmm5, %xmm4

	addpd	%xmm1, %xmm8
	movaps	 12 * SIZE(BO), %xmm1
	addpd	%xmm2, %xmm9
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm5, %xmm1
	mulpd	%xmm5, %xmm2

	addpd	%xmm3, %xmm10
	movaps	 14 * SIZE(BO), %xmm3
	addpd	%xmm4, %xmm11
	pshufd	$0x4e, %xmm3, %xmm4
	movaps	 -8 * SIZE(AO), %xmm0
	mulpd	%xmm5, %xmm3
	mulpd	%xmm5, %xmm4

	addq	$32 * SIZE, BO
	subq	$-8 * SIZE, AO
	decq	%rax
	BRANCH
	jg	.L12
	ALIGN_3

.L15:
	movups	ALPHA_R, %xmm7

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
	addpd	%xmm1, %xmm12
	movaps	-16 * SIZE(BO), %xmm1
	addpd	%xmm2, %xmm13
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2

	addpd	%xmm3, %xmm14
	movaps	-14 * SIZE(BO), %xmm3
	addpd	%xmm4, %xmm15
	pshufd	$0x4e, %xmm3, %xmm4
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm4

	addpd	%xmm1, %xmm8
	movaps	-12 * SIZE(BO), %xmm1
	addpd	%xmm2, %xmm9
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2

	addpd	%xmm3, %xmm10
	movaps	-10 * SIZE(BO), %xmm3
	addpd	%xmm4, %xmm11
	pshufd	$0x4e, %xmm3, %xmm4
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm4

	movaps	-14 * SIZE(AO), %xmm0

	addq	$2 * SIZE, AO
	addq	$8 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L16
	ALIGN_4

.L18:
	addpd	%xmm1, %xmm12
	addpd	%xmm2, %xmm13
	addpd	%xmm3, %xmm14
	addpd	%xmm4, %xmm15

	movaps	%xmm8, %xmm0
	shufpd	$2, %xmm9, %xmm8
	shufpd	$2, %xmm0, %xmm9
	movaps	%xmm10, %xmm0
	shufpd	$2, %xmm11, %xmm10
	shufpd	$2, %xmm0, %xmm11

	movaps	%xmm12, %xmm0
	shufpd	$2, %xmm13, %xmm12
	shufpd	$2, %xmm0, %xmm13
	movaps	%xmm14, %xmm0
	shufpd	$2, %xmm15, %xmm14
	shufpd	$2, %xmm0, %xmm15

	leaq	(LDC, LDC, 2), %rax

	movsd	0 * SIZE(CO1), %xmm0
	movhps	1 * SIZE(CO1), %xmm0
	movsd	2 * SIZE(CO1), %xmm1
	movhps	3 * SIZE(CO1), %xmm1

	movsd	0 * SIZE(CO1, LDC), %xmm2
	movhps	1 * SIZE(CO1, LDC), %xmm2
	movsd	2 * SIZE(CO1, LDC), %xmm3
	movhps	3 * SIZE(CO1, LDC), %xmm3

	movddup	 %xmm8,  %xmm4
	mulpd	 %xmm7,  %xmm4
	addpd	 %xmm4,  %xmm0

	unpckhpd %xmm8,  %xmm8
	mulpd	 %xmm7,  %xmm8
	addpd	 %xmm8,  %xmm1

	movddup	 %xmm9,  %xmm5
	mulpd	 %xmm7,  %xmm5
	addpd	 %xmm5,  %xmm2

	unpckhpd %xmm9,  %xmm9
	mulpd	 %xmm7,  %xmm9
	addpd	 %xmm9,  %xmm3

	movlps	%xmm0,  0 * SIZE(CO1)
	movhps	%xmm0,  1 * SIZE(CO1)
	movlps	%xmm1,  2 * SIZE(CO1)
	movhps	%xmm1,  3 * SIZE(CO1)

	movlps	%xmm2,  0 * SIZE(CO1, LDC)
	movhps	%xmm2,  1 * SIZE(CO1, LDC)
	movlps	%xmm3,  2 * SIZE(CO1, LDC)
	movhps	%xmm3,  3 * SIZE(CO1, LDC)

	movsd	0 * SIZE(CO1, LDC, 2), %xmm0
	movhps	1 * SIZE(CO1, LDC, 2), %xmm0
	movsd	2 * SIZE(CO1, LDC, 2), %xmm1
	movhps	3 * SIZE(CO1, LDC, 2), %xmm1

	movsd	0 * SIZE(CO1, %rax), %xmm2
	movhps	1 * SIZE(CO1, %rax), %xmm2
	movsd	2 * SIZE(CO1, %rax), %xmm3
	movhps	3 * SIZE(CO1, %rax), %xmm3

	movddup	 %xmm10,  %xmm4
	mulpd	 %xmm7,  %xmm4
	addpd	 %xmm4,  %xmm0

	unpckhpd %xmm10, %xmm10
	mulpd	 %xmm7,  %xmm10
	addpd	 %xmm10, %xmm1

	movddup	 %xmm11, %xmm5
	mulpd	 %xmm7,  %xmm5
	addpd	 %xmm5,  %xmm2

	unpckhpd %xmm11, %xmm11
	mulpd	 %xmm7,  %xmm11
	addpd	 %xmm11, %xmm3

	movlps	%xmm0,  0 * SIZE(CO1, LDC, 2)
	movhps	%xmm0,  1 * SIZE(CO1, LDC, 2)
	movlps	%xmm1,  2 * SIZE(CO1, LDC, 2)
	movhps	%xmm1,  3 * SIZE(CO1, LDC, 2)

	movlps	%xmm2,  0 * SIZE(CO1, %rax)
	movhps	%xmm2,  1 * SIZE(CO1, %rax)
	movlps	%xmm3,  2 * SIZE(CO1, %rax)
	movhps	%xmm3,  3 * SIZE(CO1, %rax)

	movsd	0 * SIZE(CO2), %xmm0
	movhps	1 * SIZE(CO2), %xmm0
	movsd	2 * SIZE(CO2), %xmm1
	movhps	3 * SIZE(CO2), %xmm1

	movsd	0 * SIZE(CO2, LDC), %xmm2
	movhps	1 * SIZE(CO2, LDC), %xmm2
	movsd	2 * SIZE(CO2, LDC), %xmm3
	movhps	3 * SIZE(CO2, LDC), %xmm3

	movddup	 %xmm12, %xmm4
	mulpd	 %xmm7,  %xmm4
	addpd	 %xmm4,  %xmm0

	unpckhpd %xmm12, %xmm12
	mulpd	 %xmm7,  %xmm12
	addpd	 %xmm12, %xmm1

	movddup	 %xmm13, %xmm5
	mulpd	 %xmm7,  %xmm5
	addpd	 %xmm5,  %xmm2

	unpckhpd %xmm13, %xmm13
	mulpd	 %xmm7,  %xmm13
	addpd	 %xmm13, %xmm3

	movlps	%xmm0,  0 * SIZE(CO2)
	movhps	%xmm0,  1 * SIZE(CO2)
	movlps	%xmm1,  2 * SIZE(CO2)
	movhps	%xmm1,  3 * SIZE(CO2)

	movlps	%xmm2,  0 * SIZE(CO2, LDC)
	movhps	%xmm2,  1 * SIZE(CO2, LDC)
	movlps	%xmm3,  2 * SIZE(CO2, LDC)
	movhps	%xmm3,  3 * SIZE(CO2, LDC)

	movsd	0 * SIZE(CO2, LDC, 2), %xmm0
	movhps	1 * SIZE(CO2, LDC, 2), %xmm0
	movsd	2 * SIZE(CO2, LDC, 2), %xmm1
	movhps	3 * SIZE(CO2, LDC, 2), %xmm1

	movsd	0 * SIZE(CO2, %rax), %xmm2
	movhps	1 * SIZE(CO2, %rax), %xmm2
	movsd	2 * SIZE(CO2, %rax), %xmm3
	movhps	3 * SIZE(CO2, %rax), %xmm3

	movddup	 %xmm14, %xmm4
	mulpd	 %xmm7,  %xmm4
	addpd	 %xmm4,  %xmm0

	unpckhpd %xmm14, %xmm14
	mulpd	 %xmm7,  %xmm14
	addpd	 %xmm14, %xmm1

	movddup	 %xmm15, %xmm5
	mulpd	 %xmm7,  %xmm5
	addpd	 %xmm5,  %xmm2

	unpckhpd %xmm15, %xmm15
	mulpd	 %xmm7,  %xmm15
	addpd	 %xmm15, %xmm3

	movlps	%xmm0,  0 * SIZE(CO2, LDC, 2)
	movhps	%xmm0,  1 * SIZE(CO2, LDC, 2)
	movlps	%xmm1,  2 * SIZE(CO2, LDC, 2)
	movhps	%xmm1,  3 * SIZE(CO2, LDC, 2)

	movlps	%xmm2,  0 * SIZE(CO2, %rax)
	movhps	%xmm2,  1 * SIZE(CO2, %rax)
	movlps	%xmm3,  2 * SIZE(CO2, %rax)
	movhps	%xmm3,  3 * SIZE(CO2, %rax)

	addq	$4 * SIZE, CO1
	addq	$4 * SIZE, CO2

	decq	I
	BRANCH
	jg	.L11
	ALIGN_4

.L20:
	testq	$1, M
	BRANCH
	jle	.L29
	ALIGN_4

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

	movddup	-16 * SIZE(AO), %xmm0
	xorps	%xmm8,  %xmm8
	movaps	-16 * SIZE(BO), %xmm1
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
	addq	$1, %rax
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
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm8
	movaps	-14 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm9
	movaps	-12 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm10
	movaps	-10 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	movddup	-15 * SIZE(AO), %xmm0
	addpd	%xmm1, %xmm11
	movaps	 -8 * SIZE(BO), %xmm1

	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm8
	movaps	 -6 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm9
	movaps	 -4 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm10
	movaps	 -2 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	movddup	-14 * SIZE(AO), %xmm0
	addpd	%xmm1, %xmm11
	movaps	  0 * SIZE(BO), %xmm1

	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm8
	movaps	  2 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm9
	movaps	  4 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm10
	movaps	  6 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	movddup	-13 * SIZE(AO), %xmm0
	addpd	%xmm1, %xmm11
	movaps	  8 * SIZE(BO), %xmm1

	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm8
	movaps	 10 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm9
	movaps	 12 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm10
	movaps	 14 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	movddup	-12 * SIZE(AO), %xmm0
	addpd	%xmm1, %xmm11
	movaps	 16 * SIZE(BO), %xmm1

	subq	$ -4 * SIZE, AO
	subq	$-32 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L22
	ALIGN_3

.L25:
	movups	ALPHA_R, %xmm7

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
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm8
	movaps	-14 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm9
	movaps	-12 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm10
	movaps	-10 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	movddup	-15 * SIZE(AO), %xmm0
	addpd	%xmm1, %xmm11
	movaps	 -8 * SIZE(BO), %xmm1

	addq	$1 * SIZE, AO
	addq	$8 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L26
	ALIGN_4

.L28:
	leaq	(LDC, LDC, 2), %rax

	movsd	0 * SIZE(CO1), %xmm0
	movhps	1 * SIZE(CO1), %xmm0
	movsd	0 * SIZE(CO1, LDC), %xmm1
	movhps	1 * SIZE(CO1, LDC), %xmm1

	movsd	0 * SIZE(CO1, LDC, 2), %xmm2
	movhps	1 * SIZE(CO1, LDC, 2), %xmm2
	movsd	0 * SIZE(CO1, %rax), %xmm3
	movhps	1 * SIZE(CO1, %rax), %xmm3

	movddup	 %xmm8,  %xmm4
	mulpd	 %xmm7,  %xmm4
	addpd	 %xmm4,  %xmm0

	unpckhpd %xmm8,  %xmm8
	mulpd	 %xmm7,  %xmm8
	addpd	 %xmm8,  %xmm1

	movddup	 %xmm9,  %xmm5
	mulpd	 %xmm7,  %xmm5
	addpd	 %xmm5,  %xmm2

	unpckhpd %xmm9,  %xmm9
	mulpd	 %xmm7,  %xmm9
	addpd	 %xmm9,  %xmm3

	movlps	%xmm0,  0 * SIZE(CO1)
	movhps	%xmm0,  1 * SIZE(CO1)
	movlps	%xmm1,  0 * SIZE(CO1, LDC)
	movhps	%xmm1,  1 * SIZE(CO1, LDC)

	movlps	%xmm2,  0 * SIZE(CO1, LDC, 2)
	movhps	%xmm2,  1 * SIZE(CO1, LDC, 2)
	movlps	%xmm3,  0 * SIZE(CO1, %rax)
	movhps	%xmm3,  1 * SIZE(CO1, %rax)

	movsd	0 * SIZE(CO2), %xmm0
	movhps	1 * SIZE(CO2), %xmm0
	movsd	0 * SIZE(CO2, LDC), %xmm1
	movhps	1 * SIZE(CO2, LDC), %xmm1

	movsd	0 * SIZE(CO2, LDC, 2), %xmm2
	movhps	1 * SIZE(CO2, LDC, 2), %xmm2
	movsd	0 * SIZE(CO2, %rax), %xmm3
	movhps	1 * SIZE(CO2, %rax), %xmm3

	movddup	 %xmm10, %xmm4
	mulpd	 %xmm7,  %xmm4
	addpd	 %xmm4,  %xmm0

	unpckhpd %xmm10, %xmm10
	mulpd	 %xmm7,  %xmm10
	addpd	 %xmm10, %xmm1

	movddup	 %xmm11, %xmm5
	mulpd	 %xmm7,  %xmm5
	addpd	 %xmm5,  %xmm2

	unpckhpd %xmm11, %xmm11
	mulpd	 %xmm7,  %xmm11
	addpd	 %xmm11, %xmm3

	movlps	%xmm0,  0 * SIZE(CO2)
	movhps	%xmm0,  1 * SIZE(CO2)
	movlps	%xmm1,  0 * SIZE(CO2, LDC)
	movhps	%xmm1,  1 * SIZE(CO2, LDC)

	movlps	%xmm2,  0 * SIZE(CO2, LDC, 2)
	movhps	%xmm2,  1 * SIZE(CO2, LDC, 2)
	movlps	%xmm3,  0 * SIZE(CO2, %rax)
	movhps	%xmm3,  1 * SIZE(CO2, %rax)
	ALIGN_4

.L29:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addq	$8, KK
#endif

	movq	BO, B

	leaq	(C, LDC, 8), C

	subq	$1, J
	BRANCH
	jg	.L01
	ALIGN_4

.L30:
	testq	$4, N
	jle	.L50
	ALIGN_4

#if defined(TRMMKERNEL) && defined(LEFT)
        movq    OFFSET, %rax
	movq    %rax, KK
#endif

	movq	C, CO1
	leaq	(C, LDC, 2), CO2
	movq	A, AO

	movq	M,  I
	sarq	$1, I
	NOBRANCH
	jle	.L40
	ALIGN_4

.L31:
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
	movaps	-16 * SIZE(AO), %xmm0
	xorps	%xmm2, %xmm2
	xorps	%xmm3, %xmm3
	xorps	%xmm4, %xmm4

	xorps	%xmm8,  %xmm8
	prefetcht0     3 * SIZE(CO1)
	xorps	%xmm9,  %xmm9
	prefetcht0     7 * SIZE(CO1, LDC,  1)
	xorps	%xmm10, %xmm10
	prefetcht0     3 * SIZE(CO2)
	xorps	%xmm11, %xmm11
	prefetcht0     7 * SIZE(CO2, LDC,  1)

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
	PREFETCH	(PREFETCHSIZE +  0) * SIZE(AO)

	addpd	%xmm1, %xmm8
	movaps	-16 * SIZE(BO), %xmm1
	addpd	%xmm2, %xmm9
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2

	addpd	%xmm3, %xmm10
	movaps	-14 * SIZE(BO), %xmm3
	addpd	%xmm4, %xmm11
	pshufd	$0x4e, %xmm3, %xmm4
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm4

	movaps	-14 * SIZE(AO), %xmm0

	addpd	%xmm1, %xmm8
	movaps	-12 * SIZE(BO), %xmm1
	addpd	%xmm2, %xmm9
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2

	addpd	%xmm3, %xmm10
	movaps	-10 * SIZE(BO), %xmm3
	addpd	%xmm4, %xmm11
	pshufd	$0x4e, %xmm3, %xmm4
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm4

	movaps	-12 * SIZE(AO), %xmm0

	addpd	%xmm1, %xmm8
	movaps	 -8 * SIZE(BO), %xmm1
	addpd	%xmm2, %xmm9
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2

	addpd	%xmm3, %xmm10
	movaps	 -6 * SIZE(BO), %xmm3
	addpd	%xmm4, %xmm11
	pshufd	$0x4e, %xmm3, %xmm4
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm4

	movaps	-10 * SIZE(AO), %xmm0

	addpd	%xmm1, %xmm8
	movaps	 -4 * SIZE(BO), %xmm1
	addpd	%xmm2, %xmm9
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2

	addpd	%xmm3, %xmm10
	movaps	 -2 * SIZE(BO), %xmm3
	addpd	%xmm4, %xmm11
	pshufd	$0x4e, %xmm3, %xmm4
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm4

	movaps	 -8 * SIZE(AO), %xmm0

	subq	$-8  * SIZE, AO
	subq	$-16 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L32
	ALIGN_3

.L35:
	movups	ALPHA_R, %xmm7

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
	addpd	%xmm1, %xmm8
	movaps	-16 * SIZE(BO), %xmm1
	addpd	%xmm2, %xmm9
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2

	addpd	%xmm3, %xmm10
	movaps	-14 * SIZE(BO), %xmm3
	addpd	%xmm4, %xmm11
	pshufd	$0x4e, %xmm3, %xmm4
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm4

	movaps	-14 * SIZE(AO), %xmm0

	addq	$2 * SIZE, AO
	addq	$4 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L36
	ALIGN_4

.L38:
	addpd	%xmm1, %xmm8
	addpd	%xmm2, %xmm9
	addpd	%xmm3, %xmm10
	addpd	%xmm4, %xmm11

	movaps	%xmm8, %xmm0
	shufpd	$2, %xmm9, %xmm8
	shufpd	$2, %xmm0, %xmm9

	movaps	%xmm10, %xmm0
	shufpd	$2, %xmm11, %xmm10
	shufpd	$2, %xmm0,  %xmm11

	movsd	0 * SIZE(CO1), %xmm0
	movhps	1 * SIZE(CO1), %xmm0
	movsd	2 * SIZE(CO1), %xmm1
	movhps	3 * SIZE(CO1), %xmm1

	movsd	0 * SIZE(CO1, LDC), %xmm2
	movhps	1 * SIZE(CO1, LDC), %xmm2
	movsd	2 * SIZE(CO1, LDC), %xmm3
	movhps	3 * SIZE(CO1, LDC), %xmm3

	movddup	 %xmm8,  %xmm4
	mulpd	 %xmm7,  %xmm4
	addpd	 %xmm4,  %xmm0

	unpckhpd %xmm8,  %xmm8
	mulpd	 %xmm7,  %xmm8
	addpd	 %xmm8,  %xmm1

	movddup	 %xmm9,  %xmm5
	mulpd	 %xmm7,  %xmm5
	addpd	 %xmm5,  %xmm2

	unpckhpd %xmm9,  %xmm9
	mulpd	 %xmm7,  %xmm9
	addpd	 %xmm9,  %xmm3

	movlps	%xmm0,  0 * SIZE(CO1)
	movhps	%xmm0,  1 * SIZE(CO1)
	movlps	%xmm1,  2 * SIZE(CO1)
	movhps	%xmm1,  3 * SIZE(CO1)

	movlps	%xmm2,  0 * SIZE(CO1, LDC)
	movhps	%xmm2,  1 * SIZE(CO1, LDC)
	movlps	%xmm3,  2 * SIZE(CO1, LDC)
	movhps	%xmm3,  3 * SIZE(CO1, LDC)

	movsd	0 * SIZE(CO2), %xmm0
	movhps	1 * SIZE(CO2), %xmm0
	movsd	2 * SIZE(CO2), %xmm1
	movhps	3 * SIZE(CO2), %xmm1

	movsd	0 * SIZE(CO2, LDC), %xmm2
	movhps	1 * SIZE(CO2, LDC), %xmm2
	movsd	2 * SIZE(CO2, LDC), %xmm3
	movhps	3 * SIZE(CO2, LDC), %xmm3

	movddup	 %xmm10, %xmm4
	mulpd	 %xmm7,  %xmm4
	addpd	 %xmm4,  %xmm0

	unpckhpd %xmm10, %xmm10
	mulpd	 %xmm7,  %xmm10
	addpd	 %xmm10, %xmm1

	movddup	 %xmm11, %xmm5
	mulpd	 %xmm7,  %xmm5
	addpd	 %xmm5,  %xmm2

	unpckhpd %xmm11, %xmm11
	mulpd	 %xmm7,  %xmm11
	addpd	 %xmm11, %xmm3

	movlps	%xmm0,  0 * SIZE(CO2)
	movhps	%xmm0,  1 * SIZE(CO2)
	movlps	%xmm1,  2 * SIZE(CO2)
	movhps	%xmm1,  3 * SIZE(CO2)

	movlps	%xmm2,  0 * SIZE(CO2, LDC)
	movhps	%xmm2,  1 * SIZE(CO2, LDC)
	movlps	%xmm3,  2 * SIZE(CO2, LDC)
	movhps	%xmm3,  3 * SIZE(CO2, LDC)

	addq	$4 * SIZE, CO1
	addq	$4 * SIZE, CO2
	decq	I
	BRANCH
	jg	.L31
	ALIGN_4

.L40:
	testq	$1, M
	BRANCH
	jle	.L49
	ALIGN_4

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

	movddup	-16 * SIZE(AO), %xmm0
	xorps	%xmm8,  %xmm8
	movaps	-16 * SIZE(BO), %xmm1
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
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm8
	movaps	-14 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	movddup	-15 * SIZE(AO), %xmm0
	addpd	%xmm1, %xmm9
	movaps	-12 * SIZE(BO), %xmm1

	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm10
	movaps	-10 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	movddup	-14 * SIZE(AO), %xmm0
	addpd	%xmm1, %xmm11
	movaps	 -8 * SIZE(BO), %xmm1

	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm8
	movaps	 -6 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	movddup	-13 * SIZE(AO), %xmm0
	addpd	%xmm1, %xmm9
	movaps	 -4 * SIZE(BO), %xmm1

	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm10
	movaps	 -2 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	movddup	-12 * SIZE(AO), %xmm0
	addpd	%xmm1, %xmm11
	movaps	  0 * SIZE(BO), %xmm1

	subq	$ -4 * SIZE, AO
	subq	$-16 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L42
	ALIGN_3

.L45:
	movups	ALPHA_R, %xmm7

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
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm8
	movaps	-14 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	movddup	-15 * SIZE(AO), %xmm0
	addpd	%xmm1, %xmm9
	movaps	-12 * SIZE(BO), %xmm1

	addq	$1 * SIZE, AO
	addq	$4 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L46
	ALIGN_4

.L48:
	addpd	%xmm10, %xmm8
	addpd	%xmm11, %xmm9

	movsd	0 * SIZE(CO1), %xmm0
	movhps	1 * SIZE(CO1), %xmm0
	movsd	0 * SIZE(CO1, LDC), %xmm1
	movhps	1 * SIZE(CO1, LDC), %xmm1

	movsd	0 * SIZE(CO2), %xmm2
	movhps	1 * SIZE(CO2), %xmm2
	movsd	0 * SIZE(CO2, LDC), %xmm3
	movhps	1 * SIZE(CO2, LDC), %xmm3

	movddup	 %xmm8,  %xmm4
	mulpd	 %xmm7,  %xmm4
	addpd	 %xmm4,  %xmm0

	unpckhpd %xmm8,  %xmm8
	mulpd	 %xmm7,  %xmm8
	addpd	 %xmm8,  %xmm1

	movddup	 %xmm9,  %xmm5
	mulpd	 %xmm7,  %xmm5
	addpd	 %xmm5,  %xmm2

	unpckhpd %xmm9,  %xmm9
	mulpd	 %xmm7,  %xmm9
	addpd	 %xmm9,  %xmm3

	movlps	%xmm0,  0 * SIZE(CO1)
	movhps	%xmm0,  1 * SIZE(CO1)
	movlps	%xmm1,  0 * SIZE(CO1, LDC)
	movhps	%xmm1,  1 * SIZE(CO1, LDC)

	movlps	%xmm2,  0 * SIZE(CO2)
	movhps	%xmm2,  1 * SIZE(CO2)
	movlps	%xmm3,  0 * SIZE(CO2, LDC)
	movhps	%xmm3,  1 * SIZE(CO2, LDC)
	ALIGN_4

.L49:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addq	$4, KK
#endif

	movq	BO, B

	leaq	(C, LDC, 4), C
	ALIGN_4

.L50:
	testq	$2, N
	jle	.L70
	ALIGN_4

#if defined(TRMMKERNEL) && defined(LEFT)
        movq    OFFSET, %rax
	movq    %rax, KK
#endif

	movq	C, CO1
	leaq	(C, LDC), CO2
	movq	A, AO

	movq	M,  I
	sarq	$1, I
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
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 2), BO
#endif

	xorps	%xmm1, %xmm1
	movaps	-16 * SIZE(AO), %xmm0
	xorps	%xmm2, %xmm2

	xorps	%xmm8,  %xmm8
	prefetcht0     3 * SIZE(CO1)
	xorps	%xmm9,  %xmm9
	prefetcht0     7 * SIZE(CO2)
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
	addq	$2, %rax
#endif
	movq	%rax, KKK
#endif
	sarq	$2, %rax
	NOBRANCH
	jle	.L55
	ALIGN_3

.L52:
	PREFETCH	(PREFETCHSIZE +  0) * SIZE(AO)

	addpd	%xmm1, %xmm8
	movaps	-16 * SIZE(BO), %xmm1
	addpd	%xmm2, %xmm9
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2
	movaps	-14 * SIZE(AO), %xmm0

	addpd	%xmm1, %xmm10
	movaps	-14 * SIZE(BO), %xmm1
	addpd	%xmm2, %xmm11
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2
	movaps	-12 * SIZE(AO), %xmm0

	addpd	%xmm1, %xmm8
	movaps	-12 * SIZE(BO), %xmm1
	addpd	%xmm2, %xmm9
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2
	movaps	-10 * SIZE(AO), %xmm0

	addpd	%xmm1, %xmm10
	movaps	-10 * SIZE(BO), %xmm1
	addpd	%xmm2, %xmm11
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2
	movaps	 -8 * SIZE(AO), %xmm0

	subq	$-8 * SIZE, AO
	subq	$-8 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L52

	addpd	%xmm10, %xmm8
	addpd	%xmm11, %xmm9
	ALIGN_3

.L55:
	movups	ALPHA_R, %xmm7

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
	addpd	%xmm1, %xmm8
	movaps	-16 * SIZE(BO), %xmm1
	addpd	%xmm2, %xmm9
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2
	movaps	-14 * SIZE(AO), %xmm0

	addq	$2 * SIZE, AO
	addq	$2 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L56
	ALIGN_4

.L58:
	addpd	%xmm1, %xmm8
	addpd	%xmm2, %xmm9

	movaps	%xmm8, %xmm0
	shufpd	$2, %xmm9, %xmm8
	shufpd	$2, %xmm0, %xmm9

	movsd	0 * SIZE(CO1), %xmm0
	movhps	1 * SIZE(CO1), %xmm0
	movsd	2 * SIZE(CO1), %xmm1
	movhps	3 * SIZE(CO1), %xmm1

	movsd	0 * SIZE(CO2), %xmm2
	movhps	1 * SIZE(CO2), %xmm2
	movsd	2 * SIZE(CO2), %xmm3
	movhps	3 * SIZE(CO2), %xmm3

	movddup	 %xmm8,  %xmm4
	mulpd	 %xmm7,  %xmm4
	addpd	 %xmm4,  %xmm0

	unpckhpd %xmm8,  %xmm8
	mulpd	 %xmm7,  %xmm8
	addpd	 %xmm8,  %xmm1

	movddup	 %xmm9,  %xmm5
	mulpd	 %xmm7,  %xmm5
	addpd	 %xmm5,  %xmm2

	unpckhpd %xmm9,  %xmm9
	mulpd	 %xmm7,  %xmm9
	addpd	 %xmm9,  %xmm3

	movlps	%xmm0,  0 * SIZE(CO1)
	movhps	%xmm0,  1 * SIZE(CO1)
	movlps	%xmm1,  2 * SIZE(CO1)
	movhps	%xmm1,  3 * SIZE(CO1)

	movlps	%xmm2,  0 * SIZE(CO2)
	movhps	%xmm2,  1 * SIZE(CO2)
	movlps	%xmm3,  2 * SIZE(CO2)
	movhps	%xmm3,  3 * SIZE(CO2)

	addq	$4 * SIZE, CO1
	addq	$4 * SIZE, CO2
	decq	I
	BRANCH
	jg	.L51
	ALIGN_4

.L60:
	testq	$1, M
	BRANCH
	jle	.L69
	ALIGN_4

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

	movddup	-16 * SIZE(AO), %xmm0
	xorps	%xmm8,  %xmm8
	movaps	-16 * SIZE(BO), %xmm1
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
	addq	$1, %rax
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
	mulpd	%xmm0, %xmm1
	movddup	-15 * SIZE(AO), %xmm0
	addpd	%xmm1, %xmm8
	movaps	-14 * SIZE(BO), %xmm1

	mulpd	%xmm0, %xmm1
	movddup	-14 * SIZE(AO), %xmm0
	addpd	%xmm1, %xmm9
	movaps	-12 * SIZE(BO), %xmm1

	mulpd	%xmm0, %xmm1
	movddup	-13 * SIZE(AO), %xmm0
	addpd	%xmm1, %xmm8
	movaps	-10 * SIZE(BO), %xmm1

	mulpd	%xmm0, %xmm1
	movddup	-12 * SIZE(AO), %xmm0
	addpd	%xmm1, %xmm9
	movaps	 -8 * SIZE(BO), %xmm1

	subq	$-4 * SIZE, AO
	subq	$-8 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L62
	ALIGN_3

.L65:
	movups	ALPHA_R, %xmm7

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
	mulpd	%xmm0, %xmm1
	movddup	-15 * SIZE(AO), %xmm0
	addpd	%xmm1, %xmm8
	movaps	-14 * SIZE(BO), %xmm1

	addq	$1 * SIZE, AO
	addq	$2 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L66
	ALIGN_4

.L68:
	addpd	%xmm9, %xmm8

	movsd	0 * SIZE(CO1), %xmm0
	movhps	1 * SIZE(CO1), %xmm0
	movsd	0 * SIZE(CO2), %xmm1
	movhps	1 * SIZE(CO2), %xmm1

	movddup	 %xmm8,  %xmm4
	mulpd	 %xmm7,  %xmm4
	addpd	 %xmm4,  %xmm0

	unpckhpd %xmm8,  %xmm8
	mulpd	 %xmm7,  %xmm8
	addpd	 %xmm8,  %xmm1

	movlps	%xmm0,  0 * SIZE(CO1)
	movhps	%xmm0,  1 * SIZE(CO1)
	movlps	%xmm1,  0 * SIZE(CO2)
	movhps	%xmm1,  1 * SIZE(CO2)
	ALIGN_4

.L69:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addq	$2, KK
#endif

	movq	BO, B

	leaq	(C, LDC, 2), C
	ALIGN_4

.L70:
	testq	$1, N
	jle	.L999
	ALIGN_4

#if defined(TRMMKERNEL) && defined(LEFT)
        movq    OFFSET, %rax
	movq    %rax, KK
#endif

	movq	C, CO1
	movq	A, AO

	movq	M,  I
	sarq	$1, I
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
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 1), BO
#endif

	xorps	%xmm1, %xmm1
	movaps	-16 * SIZE(AO), %xmm0
	xorps	%xmm2, %xmm2

	xorps	%xmm8,  %xmm8
	prefetcht0     3 * SIZE(CO1)
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
	addq	$1, %rax
#endif
	movq	%rax, KKK
#endif
	sarq	$2, %rax
	NOBRANCH
	jle	.L75
	ALIGN_3

.L72:
	PREFETCH	(PREFETCHSIZE +  0) * SIZE(AO)

	addpd	%xmm1, %xmm8
	movddup	-16 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	movaps	-14 * SIZE(AO), %xmm0

	addpd	%xmm1, %xmm9
	movddup	-15 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	movaps	-12 * SIZE(AO), %xmm0

	addpd	%xmm1, %xmm8
	movddup	-14 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	movaps	-10 * SIZE(AO), %xmm0

	addpd	%xmm1, %xmm9
	movddup	-13 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	movaps	 -8 * SIZE(AO), %xmm0

	subq	$-8 * SIZE, AO
	subq	$-4 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L72

	addpd	%xmm9, %xmm8
	ALIGN_3

.L75:
	movups	ALPHA_R, %xmm7

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
	addpd	%xmm1, %xmm8
	movddup	-16 * SIZE(BO), %xmm1
	mulpd	%xmm0, %xmm1
	movaps	-14 * SIZE(AO), %xmm0

	addq	$2 * SIZE, AO
	addq	$1 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L76
	ALIGN_4

.L78:
	addpd	%xmm1, %xmm8

	movsd	0 * SIZE(CO1), %xmm0
	movhps	1 * SIZE(CO1), %xmm0
	movsd	2 * SIZE(CO1), %xmm1
	movhps	3 * SIZE(CO1), %xmm1

	movddup	 %xmm8,  %xmm4
	mulpd	 %xmm7,  %xmm4
	addpd	 %xmm4,  %xmm0

	unpckhpd %xmm8,  %xmm8
	mulpd	 %xmm7,  %xmm8
	addpd	 %xmm8,  %xmm1

	movlps	%xmm0,  0 * SIZE(CO1)
	movhps	%xmm0,  1 * SIZE(CO1)
	movlps	%xmm1,  2 * SIZE(CO1)
	movhps	%xmm1,  3 * SIZE(CO1)

	addq	$4 * SIZE, CO1
	decq	I
	BRANCH
	jg	.L71
	ALIGN_4

.L80:
	testq	$1, M
	BRANCH
	jle	.L999
	ALIGN_4

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

#ifndef TRMMKERNEL
	movaps	-16 * SIZE(AO), %xmm0
	xorps	%xmm8,  %xmm8
	movaps	-16 * SIZE(BO), %xmm1
	xorps	%xmm9,  %xmm9
#else
	movsd	-16 * SIZE(AO), %xmm0
	movhpd	-15 * SIZE(AO), %xmm0
	xorps	%xmm8,  %xmm8
	movsd	-16 * SIZE(BO), %xmm1
	movhpd	-15 * SIZE(BO), %xmm1
	xorps	%xmm9,  %xmm9
#endif

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
	jle	.L85
	ALIGN_3

.L82:
	mulpd	%xmm0, %xmm1
#ifndef TRMMKERNEL
	movapd	-14 * SIZE(AO), %xmm0
#else
	movsd	-14 * SIZE(AO), %xmm0
	movhpd	-13 * SIZE(AO), %xmm0
#endif
	addpd	%xmm1, %xmm8
#ifndef TRMMKERNEL
	movapd	-14 * SIZE(BO), %xmm1
#else
	movsd	-14 * SIZE(BO), %xmm1
	movhpd	-13 * SIZE(BO), %xmm1
#endif

	mulpd	%xmm0, %xmm1
#ifndef TRMMKERNEL
	movapd	-12 * SIZE(AO), %xmm0
#else
	movsd	-12 * SIZE(AO), %xmm0
	movhpd	-11 * SIZE(AO), %xmm0
#endif
	addpd	%xmm1, %xmm9
#ifndef TRMMKERNEL
	movapd	-12 * SIZE(BO), %xmm1
#else
	movsd	-12 * SIZE(BO), %xmm1
	movhpd	-11 * SIZE(BO), %xmm1
#endif

	subq	$-4 * SIZE, AO
	subq	$-4 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L82

	addpd	%xmm9, %xmm8
	ALIGN_3

.L85:
	movups	ALPHA_R, %xmm7

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
	mulsd	%xmm0, %xmm1
	movsd	-15 * SIZE(AO), %xmm0
	addsd	%xmm1, %xmm8
	movsd	-15 * SIZE(BO), %xmm1

	addq	$1 * SIZE, AO
	addq	$1 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L86
	ALIGN_4

.L88:
	haddpd	%xmm8, %xmm8

	movsd	0 * SIZE(CO1), %xmm0
	movhps	1 * SIZE(CO1), %xmm0

	movddup	 %xmm8,  %xmm4
	mulpd	 %xmm7,  %xmm4
	addpd	 %xmm4,  %xmm0

	movlps	%xmm0,  0 * SIZE(CO1)
	movhps	%xmm0,  1 * SIZE(CO1)
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
