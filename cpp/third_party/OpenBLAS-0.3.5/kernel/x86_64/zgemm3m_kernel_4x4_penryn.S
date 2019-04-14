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

#ifdef NANO
#define PREFETCHSIZE  (8 * 2 + 4)
#define PREFETCHW     prefetcht0
#define PREFETCHB     prefetcht0
#endif

#ifndef PREFETCH
#define PREFETCH      prefetcht0
#endif

#ifndef PREFETCHW
#define PREFETCHW     prefetcht2
#endif

#ifndef PREFETCHB
#define PREFETCHB     prefetcht0
#endif

#ifndef PREFETCHSIZE
#define PREFETCHSIZE  (8 * 17 + 4)
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
	subq	$-17 * SIZE, B

	movq	OLD_M, M
	movq	OLD_N, N
	movq	OLD_K, K

	salq	$ZBASE_SHIFT, LDC

	movq	N,  J
	sarq	$2, J
	NOBRANCH
	jle	.L40
	ALIGN_4

.L01:
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
	sarq	$2, I	# i = (m >> 2)
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
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 4), BO
#endif

	movaps	-16 * SIZE(AO), %xmm0
	xorpd	%xmm3, %xmm3
	movaps	-14 * SIZE(AO), %xmm1
	xorpd	%xmm4, %xmm4
	movaps	-17 * SIZE(BO), %xmm2

	PREFETCHB	 -16 * SIZE(BB)

	xorpd	%xmm5, %xmm5
	xorpd	%xmm6, %xmm6

	PREFETCHW     3 * SIZE(CO1)
	movaps	%xmm4, %xmm8
	movaps	%xmm4, %xmm9
	PREFETCHW     7 * SIZE(CO2)
	movaps	%xmm4, %xmm10
	movaps	%xmm4, %xmm11

	PREFETCHW     3 * SIZE(CO1, LDC, 2)
	movaps	%xmm4, %xmm12
	movaps	%xmm4, %xmm13
	PREFETCHW     7 * SIZE(CO2, LDC, 2)
	movapd	%xmm4, %xmm14
	movapd	%xmm4, %xmm15

	subq		 $-12 * SIZE, BB

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
	jle	.L15
	ALIGN_3

.L12:
	PREFETCH	(PREFETCHSIZE +  0) * SIZE(AO)
	addpd	%xmm3, %xmm11
	movaps	-15 * SIZE(BO), %xmm3
	addpd	%xmm4, %xmm15
	movaps	%xmm2, %xmm4
	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm4

	addpd	%xmm5, %xmm10
	addpd	%xmm6, %xmm14
	movaps	%xmm7, %xmm6
	mulpd	%xmm0, %xmm7
	mulpd	%xmm1, %xmm6

	addpd	%xmm2, %xmm9
	movaps	-13 * SIZE(BO), %xmm2
	addpd	%xmm4, %xmm13
	movaps	%xmm3, %xmm4
	pshufd	$0x4e, %xmm3, %xmm5
	mulpd	%xmm0, %xmm3
	mulpd	%xmm1, %xmm4

	addpd	%xmm7, %xmm8
	addpd	%xmm6, %xmm12
	movaps	%xmm5, %xmm6
	mulpd	%xmm0, %xmm5
	movaps	-12 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm6
	movaps	-10 * SIZE(AO), %xmm1

	addpd	%xmm3, %xmm11
	movaps	-11 * SIZE(BO), %xmm3
	addpd	%xmm4, %xmm15
	movaps	%xmm2, %xmm4
	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm4

	addpd	%xmm5, %xmm10
	addpd	%xmm6, %xmm14
	movaps	%xmm7, %xmm6
	mulpd	%xmm0, %xmm7
	mulpd	%xmm1, %xmm6

	addpd	%xmm2, %xmm9
	movaps	 -9 * SIZE(BO), %xmm2
	addpd	%xmm4, %xmm13
	movaps	%xmm3, %xmm4
	pshufd	$0x4e, %xmm3, %xmm5
	mulpd	%xmm0, %xmm3
	mulpd	%xmm1, %xmm4

	addpd	%xmm7, %xmm8
	addpd	%xmm6, %xmm12
	movaps	%xmm5, %xmm6
	mulpd	%xmm0, %xmm5
	movaps	 -8 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm6
	movaps	 -6 * SIZE(AO), %xmm1

	addpd	%xmm3, %xmm11
	movaps	 -7 * SIZE(BO), %xmm3
	addpd	%xmm4, %xmm15
	movapd	%xmm2, %xmm4
	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm4

	addpd	%xmm5, %xmm10
	addpd	%xmm6, %xmm14
	movapd	%xmm7, %xmm6
	mulpd	%xmm0, %xmm7
	mulpd	%xmm1, %xmm6
	PADDING
	PREFETCH	(PREFETCHSIZE +  8) * SIZE(AO)

	addpd	%xmm2, %xmm9
	movaps	 -5 * SIZE(BO), %xmm2
	addpd	%xmm4, %xmm13
	movaps	%xmm3, %xmm4
	pshufd	$0x4e, %xmm3, %xmm5
	mulpd	%xmm0, %xmm3
	mulpd	%xmm1, %xmm4

	addpd	%xmm7, %xmm8
	addpd	%xmm6, %xmm12
	movaps	%xmm5, %xmm6
	mulpd	%xmm0, %xmm5
	movaps	 -4 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm6
	movaps	 -2 * SIZE(AO), %xmm1

	addpd	%xmm3, %xmm11
	subq	$-16 * SIZE, AO
	movaps	 -3 * SIZE(BO), %xmm3
	addpd	%xmm4, %xmm15
	movaps	%xmm2, %xmm4
	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm4

	addpd	%xmm5, %xmm10
	addpd	%xmm6, %xmm14
	movaps	%xmm7, %xmm6
	mulpd	%xmm0, %xmm7
	mulpd	%xmm1, %xmm6

	addpd	%xmm2, %xmm9
	movaps	 -1 * SIZE(BO), %xmm2
	addpd	%xmm4, %xmm13
	movaps	%xmm3, %xmm4
	pshufd	$0x4e, %xmm3, %xmm5
	subq	$-16 * SIZE, BO
	mulpd	%xmm0, %xmm3
	mulpd	%xmm1, %xmm4

	addpd	%xmm7, %xmm8
	addpd	%xmm6, %xmm12
	movaps	%xmm5, %xmm6
	mulpd	%xmm0, %xmm5
	movaps	-16 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm6
	movaps	-14 * SIZE(AO), %xmm1

	subq	$1, %rax
	BRANCH
	jg	.L12
	ALIGN_3

.L15:
	PREFETCHB	 -8 * SIZE(BB)

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
	addpd	%xmm3, %xmm11
	movaps	-15 * SIZE(BO), %xmm3
	addpd	%xmm4, %xmm15
	movaps	%xmm2, %xmm4
	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm4

	addpd	%xmm5, %xmm10
	addpd	%xmm6, %xmm14
	movaps	%xmm7, %xmm6
	mulpd	%xmm0, %xmm7
	mulpd	%xmm1, %xmm6

	addpd	%xmm2, %xmm9
	movaps	-13 * SIZE(BO), %xmm2
	addpd	%xmm4, %xmm13
	movaps	%xmm3, %xmm4
	pshufd	$0x4e, %xmm3, %xmm5
	mulpd	%xmm0, %xmm3
	mulpd	%xmm1, %xmm4

	addpd	%xmm7, %xmm8
	addpd	%xmm6, %xmm12
	movaps	%xmm5, %xmm6
	mulpd	%xmm0, %xmm5
	movaps	-12 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm6
	movaps	-10 * SIZE(AO), %xmm1

	addq	$4 * SIZE, AO
	addq	$4 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L16
	ALIGN_4

.L18:
	movups	ALPHA_R, %xmm7

	addpd	%xmm3, %xmm11
	addpd	%xmm4, %xmm15
	addpd	%xmm5, %xmm10
	addpd	%xmm6, %xmm14

	movaps	 %xmm8, %xmm0
	movsd    %xmm9, %xmm8
	movsd    %xmm0, %xmm9

	movaps	 %xmm10, %xmm0
	movsd    %xmm11, %xmm10
	movsd    %xmm0,  %xmm11

	movaps	 %xmm12, %xmm0
	movsd    %xmm13, %xmm12
	movsd    %xmm0,  %xmm13

	movaps	 %xmm14, %xmm0
	movsd    %xmm15, %xmm14
	movsd    %xmm0,  %xmm15

	movsd	0 * SIZE(CO1), %xmm0
	movhps	1 * SIZE(CO1), %xmm0
	movsd	2 * SIZE(CO1), %xmm1
	movhps	3 * SIZE(CO1), %xmm1

	movsd	4 * SIZE(CO1), %xmm2
	movhps	5 * SIZE(CO1), %xmm2
	movsd	6 * SIZE(CO1), %xmm3
	movhps	7 * SIZE(CO1), %xmm3

	movddup	 %xmm8,  %xmm4
	mulpd	 %xmm7,  %xmm4
	addpd	 %xmm4,  %xmm0

	unpckhpd %xmm8,  %xmm8
	mulpd	 %xmm7,  %xmm8
	addpd	 %xmm8,  %xmm1

	movddup	 %xmm12, %xmm5
	mulpd	 %xmm7,  %xmm5
	addpd	 %xmm5,  %xmm2

	unpckhpd %xmm12, %xmm12
	mulpd	 %xmm7,  %xmm12
	addpd	 %xmm12, %xmm3

	movlps	%xmm0,  0 * SIZE(CO1)
	movhps	%xmm0,  1 * SIZE(CO1)
	movlps	%xmm1,  2 * SIZE(CO1)
	movhps	%xmm1,  3 * SIZE(CO1)

	movlps	%xmm2,  4 * SIZE(CO1)
	movhps	%xmm2,  5 * SIZE(CO1)
	movlps	%xmm3,  6 * SIZE(CO1)
	movhps	%xmm3,  7 * SIZE(CO1)

	movsd	0 * SIZE(CO2), %xmm0
	movhps	1 * SIZE(CO2), %xmm0
	movsd	2 * SIZE(CO2), %xmm1
	movhps	3 * SIZE(CO2), %xmm1

	movsd	4 * SIZE(CO2), %xmm2
	movhps	5 * SIZE(CO2), %xmm2
	movsd	6 * SIZE(CO2), %xmm3
	movhps	7 * SIZE(CO2), %xmm3

	movddup	 %xmm9,  %xmm4
	mulpd	 %xmm7,  %xmm4
	addpd	 %xmm4,  %xmm0

	unpckhpd %xmm9,  %xmm9
	mulpd	 %xmm7,  %xmm9
	addpd	 %xmm9,  %xmm1

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

	movlps	%xmm2,  4 * SIZE(CO2)
	movhps	%xmm2,  5 * SIZE(CO2)
	movlps	%xmm3,  6 * SIZE(CO2)
	movhps	%xmm3,  7 * SIZE(CO2)

	movsd	0 * SIZE(CO1, LDC, 2), %xmm0
	movhps	1 * SIZE(CO1, LDC, 2), %xmm0
	movsd	2 * SIZE(CO1, LDC, 2), %xmm1
	movhps	3 * SIZE(CO1, LDC, 2), %xmm1

	movsd	4 * SIZE(CO1, LDC, 2), %xmm2
	movhps	5 * SIZE(CO1, LDC, 2), %xmm2
	movsd	6 * SIZE(CO1, LDC, 2), %xmm3
	movhps	7 * SIZE(CO1, LDC, 2), %xmm3

	movddup	 %xmm10, %xmm4
	mulpd	 %xmm7,  %xmm4
	addpd	 %xmm4,  %xmm0

	unpckhpd %xmm10, %xmm10
	mulpd	 %xmm7,  %xmm10
	addpd	 %xmm10, %xmm1

	movddup	 %xmm14, %xmm5
	mulpd	 %xmm7,  %xmm5
	addpd	 %xmm5,  %xmm2

	unpckhpd %xmm14, %xmm14
	mulpd	 %xmm7,  %xmm14
	addpd	 %xmm14, %xmm3

	movlps	%xmm0,  0 * SIZE(CO1, LDC, 2)
	movhps	%xmm0,  1 * SIZE(CO1, LDC, 2)
	movlps	%xmm1,  2 * SIZE(CO1, LDC, 2)
	movhps	%xmm1,  3 * SIZE(CO1, LDC, 2)

	movlps	%xmm2,  4 * SIZE(CO1, LDC, 2)
	movhps	%xmm2,  5 * SIZE(CO1, LDC, 2)
	movlps	%xmm3,  6 * SIZE(CO1, LDC, 2)
	movhps	%xmm3,  7 * SIZE(CO1, LDC, 2)

	movsd	0 * SIZE(CO2, LDC, 2), %xmm0
	movhps	1 * SIZE(CO2, LDC, 2), %xmm0
	movsd	2 * SIZE(CO2, LDC, 2), %xmm1
	movhps	3 * SIZE(CO2, LDC, 2), %xmm1

	movsd	4 * SIZE(CO2, LDC, 2), %xmm2
	movhps	5 * SIZE(CO2, LDC, 2), %xmm2
	movsd	6 * SIZE(CO2, LDC, 2), %xmm3
	movhps	7 * SIZE(CO2, LDC, 2), %xmm3

	movddup	 %xmm11, %xmm4
	mulpd	 %xmm7,  %xmm4
	addpd	 %xmm4,  %xmm0

	unpckhpd %xmm11, %xmm11
	mulpd	 %xmm7,  %xmm11
	addpd	 %xmm11, %xmm1

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

	movlps	%xmm2,  4 * SIZE(CO2, LDC, 2)
	movhps	%xmm2,  5 * SIZE(CO2, LDC, 2)
	movlps	%xmm3,  6 * SIZE(CO2, LDC, 2)
	movhps	%xmm3,  7 * SIZE(CO2, LDC, 2)

	addq	$8 * SIZE, CO1		# coffset += 4
	addq	$8 * SIZE, CO2		# coffset += 4

	decq	I			# i --
	BRANCH
	jg	.L11
	ALIGN_4

.L20:
	testq	$2, M
	BRANCH
	jle	.L30
	ALIGN_4

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

	movaps	-16 * SIZE(AO), %xmm0
	movaps	-17 * SIZE(BO), %xmm2
	movaps	-15 * SIZE(BO), %xmm3

	xorps	%xmm3, %xmm3
	xorps	%xmm4, %xmm4
	xorps	%xmm5, %xmm5
	xorps	%xmm6, %xmm6

	movaps	%xmm3, %xmm8
	movaps	%xmm3, %xmm9
	movaps	%xmm3, %xmm10
	movaps	%xmm3, %xmm11

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
	jle	.L25
	ALIGN_4

.L22:
	addpd	%xmm3, %xmm11
	movaps	-15 * SIZE(BO), %xmm3
	pshufd	$0x4e, %xmm2, %xmm7
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)
	mulpd	%xmm0, %xmm2
	addpd	%xmm5, %xmm10
	mulpd	%xmm0, %xmm7

	addpd	%xmm2, %xmm9
	movaps	-13 * SIZE(BO), %xmm2
	pshufd	$0x4e, %xmm3, %xmm5
	mulpd	%xmm0, %xmm3
	addpd	%xmm7, %xmm8
	mulpd	%xmm0, %xmm5
	movaps	-14 * SIZE(AO), %xmm0

	addpd	%xmm3, %xmm11
	movaps	-11 * SIZE(BO), %xmm3
	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	addpd	%xmm5, %xmm10
	mulpd	%xmm0, %xmm7

	addpd	%xmm2, %xmm9
	movaps	 -9 * SIZE(BO), %xmm2
	pshufd	$0x4e, %xmm3, %xmm5
	mulpd	%xmm0, %xmm3
	addpd	%xmm7, %xmm8
	mulpd	%xmm0, %xmm5
	movaps	-12 * SIZE(AO), %xmm0

	addpd	%xmm3, %xmm11
	movaps	 -7 * SIZE(BO), %xmm3
	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	addpd	%xmm5, %xmm10
	mulpd	%xmm0, %xmm7

	addpd	%xmm2, %xmm9
	movaps	 -5 * SIZE(BO), %xmm2
	pshufd	$0x4e, %xmm3, %xmm5
	mulpd	%xmm0, %xmm3
	addpd	%xmm7, %xmm8
	mulpd	%xmm0, %xmm5
	movaps	-10 * SIZE(AO), %xmm0

	addpd	%xmm3, %xmm11
	movaps	 -3 * SIZE(BO), %xmm3
	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	addpd	%xmm5, %xmm10
	mulpd	%xmm0, %xmm7
	subq	$ -8 * SIZE, AO

	addpd	%xmm2, %xmm9
	movaps	 -1 * SIZE(BO), %xmm2
	pshufd	$0x4e, %xmm3, %xmm5
	mulpd	%xmm0, %xmm3
	addpd	%xmm7, %xmm8
	mulpd	%xmm0, %xmm5
	movaps	-16 * SIZE(AO), %xmm0

	subq	$-16 * SIZE, BO
	subq	$1, %rax
	BRANCH
	jg	.L22
	ALIGN_4

.L25:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	BRANCH
	je	.L28
	ALIGN_4

.L26:
	addpd	%xmm3, %xmm11
	movaps	-15 * SIZE(BO), %xmm3
	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	addpd	%xmm5, %xmm10
	mulpd	%xmm0, %xmm7

	addpd	%xmm2, %xmm9
	movaps	-13 * SIZE(BO), %xmm2
	pshufd	$0x4e, %xmm3, %xmm5
	mulpd	%xmm0, %xmm3
	addpd	%xmm7, %xmm8
	mulpd	%xmm0, %xmm5
	movaps	-14 * SIZE(AO), %xmm0

	addq	$2 * SIZE, AO
	addq	$4 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L26
	ALIGN_4

.L28:
	movups	ALPHA_R, %xmm7

	addpd	%xmm3, %xmm11
	addpd	%xmm5, %xmm10

	movaps	 %xmm8, %xmm0
	movsd    %xmm9, %xmm8
	movsd    %xmm0, %xmm9

	movaps	 %xmm10, %xmm0
	movsd    %xmm11, %xmm10
	movsd    %xmm0,  %xmm11

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

	movsd	0 * SIZE(CO2), %xmm0
	movhps	1 * SIZE(CO2), %xmm0
	movsd	2 * SIZE(CO2), %xmm1
	movhps	3 * SIZE(CO2), %xmm1

	movddup	 %xmm9,  %xmm4
	mulpd	 %xmm7,  %xmm4
	addpd	 %xmm4,  %xmm0

	unpckhpd %xmm9,  %xmm9
	mulpd	 %xmm7,  %xmm9
	addpd	 %xmm9,  %xmm1

	movlps	%xmm0,  0 * SIZE(CO2)
	movhps	%xmm0,  1 * SIZE(CO2)
	movlps	%xmm1,  2 * SIZE(CO2)
	movhps	%xmm1,  3 * SIZE(CO2)

	movsd	0 * SIZE(CO1, LDC, 2), %xmm0
	movhps	1 * SIZE(CO1, LDC, 2), %xmm0
	movsd	2 * SIZE(CO1, LDC, 2), %xmm1
	movhps	3 * SIZE(CO1, LDC, 2), %xmm1

	movddup	 %xmm10, %xmm4
	mulpd	 %xmm7,  %xmm4
	addpd	 %xmm4,  %xmm0

	unpckhpd %xmm10, %xmm10
	mulpd	 %xmm7,  %xmm10
	addpd	 %xmm10, %xmm1

	movlps	%xmm0,  0 * SIZE(CO1, LDC, 2)
	movhps	%xmm0,  1 * SIZE(CO1, LDC, 2)
	movlps	%xmm1,  2 * SIZE(CO1, LDC, 2)
	movhps	%xmm1,  3 * SIZE(CO1, LDC, 2)

	movsd	0 * SIZE(CO2, LDC, 2), %xmm0
	movhps	1 * SIZE(CO2, LDC, 2), %xmm0
	movsd	2 * SIZE(CO2, LDC, 2), %xmm1
	movhps	3 * SIZE(CO2, LDC, 2), %xmm1

	movddup	 %xmm11, %xmm4
	mulpd	 %xmm7,  %xmm4
	addpd	 %xmm4,  %xmm0

	unpckhpd %xmm11, %xmm11
	mulpd	 %xmm7,  %xmm11
	addpd	 %xmm11, %xmm1

	movlps	%xmm0,  0 * SIZE(CO2, LDC, 2)
	movhps	%xmm0,  1 * SIZE(CO2, LDC, 2)
	movlps	%xmm1,  2 * SIZE(CO2, LDC, 2)
	movhps	%xmm1,  3 * SIZE(CO2, LDC, 2)

	addq	$4 * SIZE, CO1		# coffset += 4
	addq	$4 * SIZE, CO2		# coffset += 4
	ALIGN_4

.L30:
	testq	$1, M
	BRANCH
	jle	.L39
	ALIGN_4

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	movq	B, BO
#else
	movq	B, BO

	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	addq	%rax, AO
	leaq	(BO, %rax, 4), BO
#endif

	movsd	-16 * SIZE(AO), %xmm0
	movaps	-17 * SIZE(BO), %xmm2
	movaps	-15 * SIZE(BO), %xmm3

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
	addq	$1, %rax
#else
	addq	$4, %rax
#endif
	movq	%rax, KKK
#endif
	sarq	$2, %rax
	NOBRANCH
	jle	.L35
	ALIGN_4

.L32:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)

	shufps	$0x44, %xmm0, %xmm0
	mulpd	%xmm0, %xmm2
	mulpd	%xmm0, %xmm3
	movsd	-15 * SIZE(AO), %xmm0

	addpd	%xmm2, %xmm8
	movaps	-13 * SIZE(BO), %xmm2
	addpd	%xmm3, %xmm9
	movaps	-11 * SIZE(BO), %xmm3

	shufps	$0x44, %xmm0, %xmm0
	mulpd	%xmm0, %xmm2
	mulpd	%xmm0, %xmm3
	movsd	-14 * SIZE(AO), %xmm0

	addpd	%xmm2, %xmm10
	movaps	 -9 * SIZE(BO), %xmm2
	addpd	%xmm3, %xmm11
	movaps	 -7 * SIZE(BO), %xmm3

	shufps	$0x44, %xmm0, %xmm0
	mulpd	%xmm0, %xmm2
	mulpd	%xmm0, %xmm3
	movsd	-13 * SIZE(AO), %xmm0

	addpd	%xmm2, %xmm8
	movaps	 -5 * SIZE(BO), %xmm2
	addpd	%xmm3, %xmm9
	movaps	 -3 * SIZE(BO), %xmm3

	shufps	$0x44, %xmm0, %xmm0
	mulpd	%xmm0, %xmm2
	mulpd	%xmm0, %xmm3
	movsd	-12 * SIZE(AO), %xmm0

	addpd	%xmm2, %xmm10
	movaps	 -1 * SIZE(BO), %xmm2
	addpd	%xmm3, %xmm11
	movaps	  1 * SIZE(BO), %xmm3

	subq	$ -4 * SIZE, AO
	subq	$-16 * SIZE, BO
	subq	$1, %rax
	BRANCH
	jg	.L32
	ALIGN_4

.L35:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	BRANCH
	je	.L38
	ALIGN_4

.L36:
	shufps	$0x44, %xmm0, %xmm0
	mulpd	%xmm0, %xmm2
	mulpd	%xmm0, %xmm3
	movsd	-15 * SIZE(AO), %xmm0

	addpd	%xmm2, %xmm8
	movaps	-13 * SIZE(BO), %xmm2
	addpd	%xmm3, %xmm9
	movaps	-11 * SIZE(BO), %xmm3

	addq	$1 * SIZE, AO
	addq	$4 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L36
	ALIGN_4

.L38:
	movups	ALPHA_R, %xmm7

	addpd	%xmm10, %xmm8
	addpd	%xmm11, %xmm9

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

	movsd	0 * SIZE(CO1, LDC, 2), %xmm0
	movhps	1 * SIZE(CO1, LDC, 2), %xmm0
	movsd	0 * SIZE(CO2, LDC, 2), %xmm1
	movhps	1 * SIZE(CO2, LDC, 2), %xmm1

	movddup	 %xmm9, %xmm4
	mulpd	 %xmm7, %xmm4
	addpd	 %xmm4, %xmm0

	unpckhpd %xmm9, %xmm9
	mulpd	 %xmm7, %xmm9
	addpd	 %xmm9, %xmm1

	movlps	%xmm0,  0 * SIZE(CO1, LDC, 2)
	movhps	%xmm0,  1 * SIZE(CO1, LDC, 2)
	movlps	%xmm1,  0 * SIZE(CO2, LDC, 2)
	movhps	%xmm1,  1 * SIZE(CO2, LDC, 2)
	ALIGN_4

.L39:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addq	$4, KK
#endif

	movq	BO, B

	leaq	(C, LDC, 4), C

	subq	$1, J
	BRANCH
	jg	.L01
	ALIGN_4

.L40:
	testq	$2, N
	BRANCH
	jle	.L80

	movq	C, CO1
	leaq	(C, LDC, 1), CO2
	movq	A, AO

#if defined(TRMMKERNEL) && defined(LEFT)
        movq    OFFSET, %rax
	movq    %rax, KK
#endif

	movq	K, %rax
	salq	$BASE_SHIFT + 1, %rax
	leaq	(B, %rax), BB

	movq	M,  I
	sarq	$2, I	# i = (m >> 2)
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
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 2), BO
#endif

	PREFETCHB	 -16 * SIZE(BB)
	subq		 $-4 * SIZE, BB

	movaps	-16 * SIZE(AO), %xmm0
	movaps	-14 * SIZE(AO), %xmm1
	movaps	-17 * SIZE(BO), %xmm2

	PREFETCHW     3 * SIZE(CO1)
	xorps	%xmm8,  %xmm8
	xorps	%xmm9,  %xmm9
	PREFETCHW     3 * SIZE(CO2)
	xorps	%xmm12, %xmm12
	xorps	%xmm13, %xmm13

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
	jle	.L55
	ALIGN_4

.L52:

	movaps	%xmm2, %xmm4
	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)
	mulpd	%xmm1, %xmm4

	movaps	%xmm7, %xmm6
	mulpd	%xmm0, %xmm7
	movaps	-12 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm6
	movaps	-10 * SIZE(AO), %xmm1

	addpd	%xmm2, %xmm9
	movaps	-15 * SIZE(BO), %xmm2
	addpd	%xmm4, %xmm13
	addpd	%xmm7, %xmm8
	addpd	%xmm6, %xmm12

	movaps	%xmm2, %xmm4
	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	PREFETCH (PREFETCHSIZE +  8) * SIZE(AO)
	mulpd	%xmm1, %xmm4

	movaps	%xmm7, %xmm6
	mulpd	%xmm0, %xmm7
	movaps	 -8 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm6
	movaps	 -6 * SIZE(AO), %xmm1

	addpd	%xmm2, %xmm9
	movaps	-13 * SIZE(BO), %xmm2
	addpd	%xmm4, %xmm13
	addpd	%xmm7, %xmm8
	addpd	%xmm6, %xmm12

	movaps	%xmm2, %xmm4
	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm4

	movaps	%xmm7, %xmm6
	mulpd	%xmm0, %xmm7
	movaps	 -4 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm6
	movaps	 -2 * SIZE(AO), %xmm1

	addpd	%xmm2, %xmm9
	movaps	-11 * SIZE(BO), %xmm2
	addpd	%xmm4, %xmm13
	addpd	%xmm7, %xmm8
	addpd	%xmm6, %xmm12

	movaps	%xmm2, %xmm4
	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm4

	movaps	%xmm7, %xmm6
	mulpd	%xmm0, %xmm7
	movaps	  0 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm6
	movaps	  2 * SIZE(AO), %xmm1

	addpd	%xmm2, %xmm9
	movaps	 -9 * SIZE(BO), %xmm2
	addpd	%xmm4, %xmm13
	addpd	%xmm7, %xmm8
	addpd	%xmm6, %xmm12

	subq	$-16 * SIZE, AO
	subq	$ -8 * SIZE, BO
	subq	$1, %rax
	BRANCH
	jg	.L52
	ALIGN_4

.L55:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	BRANCH
	je	.L58
	ALIGN_4

.L56:
	movaps	%xmm2, %xmm4
	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm4

	movaps	%xmm7, %xmm6
	mulpd	%xmm0, %xmm7
	movaps	-12 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm6
	movaps	-10 * SIZE(AO), %xmm1

	addpd	%xmm2, %xmm9
	movaps	-15 * SIZE(BO), %xmm2
	addpd	%xmm4, %xmm13
	addpd	%xmm7, %xmm8
	addpd	%xmm6, %xmm12

	addq	$4 * SIZE, AO
	addq	$2 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L56
	ALIGN_4

.L58:
	movups	ALPHA_R, %xmm7

	movaps	 %xmm8, %xmm0
	movsd    %xmm9, %xmm8
	movsd    %xmm0, %xmm9

	movaps	 %xmm12, %xmm0
	movsd    %xmm13, %xmm12
	movsd    %xmm0,  %xmm13

	movsd	0 * SIZE(CO1), %xmm0
	movhps	1 * SIZE(CO1), %xmm0
	movsd	2 * SIZE(CO1), %xmm1
	movhps	3 * SIZE(CO1), %xmm1

	movsd	4 * SIZE(CO1), %xmm2
	movhps	5 * SIZE(CO1), %xmm2
	movsd	6 * SIZE(CO1), %xmm3
	movhps	7 * SIZE(CO1), %xmm3

	movddup	 %xmm8,  %xmm4
	mulpd	 %xmm7,  %xmm4
	addpd	 %xmm4,  %xmm0

	unpckhpd %xmm8,  %xmm8
	mulpd	 %xmm7,  %xmm8
	addpd	 %xmm8,  %xmm1

	movddup	 %xmm12, %xmm5
	mulpd	 %xmm7,  %xmm5
	addpd	 %xmm5,  %xmm2

	unpckhpd %xmm12, %xmm12
	mulpd	 %xmm7,  %xmm12
	addpd	 %xmm12, %xmm3

	movlps	%xmm0,  0 * SIZE(CO1)
	movhps	%xmm0,  1 * SIZE(CO1)
	movlps	%xmm1,  2 * SIZE(CO1)
	movhps	%xmm1,  3 * SIZE(CO1)

	movlps	%xmm2,  4 * SIZE(CO1)
	movhps	%xmm2,  5 * SIZE(CO1)
	movlps	%xmm3,  6 * SIZE(CO1)
	movhps	%xmm3,  7 * SIZE(CO1)

	movsd	0 * SIZE(CO2), %xmm0
	movhps	1 * SIZE(CO2), %xmm0
	movsd	2 * SIZE(CO2), %xmm1
	movhps	3 * SIZE(CO2), %xmm1

	movsd	4 * SIZE(CO2), %xmm2
	movhps	5 * SIZE(CO2), %xmm2
	movsd	6 * SIZE(CO2), %xmm3
	movhps	7 * SIZE(CO2), %xmm3

	movddup	 %xmm9,  %xmm4
	mulpd	 %xmm7,  %xmm4
	addpd	 %xmm4,  %xmm0

	unpckhpd %xmm9,  %xmm9
	mulpd	 %xmm7,  %xmm9
	addpd	 %xmm9,  %xmm1

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

	movlps	%xmm2,  4 * SIZE(CO2)
	movhps	%xmm2,  5 * SIZE(CO2)
	movlps	%xmm3,  6 * SIZE(CO2)
	movhps	%xmm3,  7 * SIZE(CO2)

	addq	$8 * SIZE, CO1
	addq	$8 * SIZE, CO2
	decq	I
	BRANCH
	jg	.L51
	ALIGN_4

.L60:
	testq	$2, M
	BRANCH
	jle	.L70
	ALIGN_4

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

	movaps	-16 * SIZE(AO), %xmm0
	xorps	%xmm8,  %xmm8
	xorps	%xmm9,  %xmm9
	movaps	-17 * SIZE(BO), %xmm2
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
	jle	.L65
	ALIGN_4

.L62:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)

	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	mulpd	%xmm0, %xmm7
	movaps	-14 * SIZE(AO), %xmm0

	addpd	%xmm2, %xmm9
	addpd	%xmm7, %xmm8
	movaps	-15 * SIZE(BO), %xmm2

	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	mulpd	%xmm0, %xmm7
	movaps	-12 * SIZE(AO), %xmm0

	addpd	%xmm2, %xmm11
	addpd	%xmm7, %xmm10
	movaps	-13 * SIZE(BO), %xmm2

	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	mulpd	%xmm0, %xmm7
	movaps	-10 * SIZE(AO), %xmm0

	addpd	%xmm2, %xmm9
	addpd	%xmm7, %xmm8
	movaps	-11 * SIZE(BO), %xmm2

	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	mulpd	%xmm0, %xmm7
	movaps	 -8 * SIZE(AO), %xmm0

	addpd	%xmm2, %xmm11
	addpd	%xmm7, %xmm10
	movaps	 -9 * SIZE(BO), %xmm2

	subq	$-8 * SIZE, AO
	subq	$-8 * SIZE, BO
	subq	$1, %rax
	BRANCH
	jg	.L62
	ALIGN_4

.L65:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	BRANCH
	je	.L68
	ALIGN_4

.L66:
	pshufd	$0x4e, %xmm2, %xmm7
	mulpd	%xmm0, %xmm2
	mulpd	%xmm0, %xmm7
	movaps	-14 * SIZE(AO), %xmm0

	addpd	%xmm2, %xmm9
	addpd	%xmm7, %xmm8
	movaps	-15 * SIZE(BO), %xmm2

	addq	$2 * SIZE, AO
	addq	$2 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L66
	ALIGN_4

.L68:
	movups	ALPHA_R, %xmm7

	addpd	%xmm10, %xmm8
	addpd	%xmm11, %xmm9

	movaps	 %xmm8, %xmm0
	movsd    %xmm9, %xmm8
	movsd    %xmm0, %xmm9

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

	movsd	0 * SIZE(CO2), %xmm0
	movhps	1 * SIZE(CO2), %xmm0
	movsd	2 * SIZE(CO2), %xmm1
	movhps	3 * SIZE(CO2), %xmm1

	movddup	 %xmm9,  %xmm4
	mulpd	 %xmm7,  %xmm4
	addpd	 %xmm4,  %xmm0

	unpckhpd %xmm9,  %xmm9
	mulpd	 %xmm7,  %xmm9
	addpd	 %xmm9,  %xmm1

	movlps	%xmm0,  0 * SIZE(CO2)
	movhps	%xmm0,  1 * SIZE(CO2)
	movlps	%xmm1,  2 * SIZE(CO2)
	movhps	%xmm1,  3 * SIZE(CO2)

	addq	$4 * SIZE, CO1
	addq	$4 * SIZE, CO2
	ALIGN_4

.L70:
	testq	$1, M
	BRANCH
	jle	.L79
	ALIGN_4

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	movq	B, BO
#else
	movq	B, BO

	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	addq	%rax, AO
	leaq	(BO, %rax, 2), BO
#endif

	movsd	-16 * SIZE(AO), %xmm0
	movaps	-17 * SIZE(BO), %xmm2

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
	addq	$1, %rax
#else
	addq	$2, %rax
#endif
	movq	%rax, KKK
#endif
	sarq	$2, %rax
	NOBRANCH
	jle	.L75
	ALIGN_4

.L72:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)

	shufps	$0x44, %xmm0, %xmm0
	mulpd	%xmm0, %xmm2
	movsd	-15 * SIZE(AO), %xmm0
	addpd	%xmm2, %xmm8
	movaps	-15 * SIZE(BO), %xmm2

	shufps	$0x44, %xmm0, %xmm0
	mulpd	%xmm0, %xmm2
	movsd	-14 * SIZE(AO), %xmm0
	addpd	%xmm2, %xmm9
	movaps	-13 * SIZE(BO), %xmm2

	shufps	$0x44, %xmm0, %xmm0
	mulpd	%xmm0, %xmm2
	movsd	-13 * SIZE(AO), %xmm0
	addpd	%xmm2, %xmm8
	movaps	-11 * SIZE(BO), %xmm2

	shufps	$0x44, %xmm0, %xmm0
	mulpd	%xmm0, %xmm2
	movsd	-12 * SIZE(AO), %xmm0
	addpd	%xmm2, %xmm9
	movaps	 -9 * SIZE(BO), %xmm2

	subq	$-4 * SIZE, AO
	subq	$-8 * SIZE, BO
	subq	$1, %rax
	BRANCH
	jg	.L72
	ALIGN_4

.L75:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	BRANCH
	je	.L78
	ALIGN_4

.L76:
	shufps	$0x44, %xmm0, %xmm0
	mulpd	%xmm0, %xmm2
	movsd	-15 * SIZE(AO), %xmm0
	addpd	%xmm2, %xmm8
	movaps	-15 * SIZE(BO), %xmm2

	addq	$1 * SIZE, AO
	addq	$2 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L76
	ALIGN_4

.L78:
	movups	ALPHA_R, %xmm7

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

.L79:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addq	$2, KK
#endif

	leaq	(C, LDC, 2), C
	movq	BO, B
	ALIGN_4

.L80:
	testq	$1, N
	BRANCH
	jle	.L999

#if defined(TRMMKERNEL) && defined(LEFT)
        movq    OFFSET, %rax
	movq    %rax, KK
#endif

	movq	C, CO1
	movq	A, AO

	movq	M,  I
	sarq	$2, I	# i = (m >> 2)
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
	leaq	(AO, %rax, 4), AO
	addq	%rax, BO
#endif

	movaps	-16 * SIZE(AO), %xmm0
	movaps	-14 * SIZE(AO), %xmm1
	movsd	-17 * SIZE(BO), %xmm2

	PREFETCHW     3 * SIZE(CO1)
	xorps	%xmm8,  %xmm8
	xorps	%xmm9,  %xmm9
	xorps	%xmm12, %xmm12
	xorps	%xmm13, %xmm13

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
	jle	.L95
	ALIGN_4

.L92:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)

	pshufd	$0x44, %xmm2, %xmm3
	pshufd	$0x44, %xmm2, %xmm4
	movsd	-16 * SIZE(BO), %xmm2

	mulpd	%xmm0, %xmm3
	movaps	-12 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm4
	movaps	-10 * SIZE(AO), %xmm1

	addpd	%xmm3, %xmm8
	addpd	%xmm4, %xmm12

	pshufd	$0x44, %xmm2, %xmm3
	pshufd	$0x44, %xmm2, %xmm4
	movsd	-15 * SIZE(BO), %xmm2

	mulpd	%xmm0, %xmm3
	movaps	 -8 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm4
	movaps	 -6 * SIZE(AO), %xmm1

	addpd	%xmm3, %xmm8
	addpd	%xmm4, %xmm12

	PREFETCH (PREFETCHSIZE +  8) * SIZE(AO)

	pshufd	$0x44, %xmm2, %xmm3
	pshufd	$0x44, %xmm2, %xmm4
	movsd	-14 * SIZE(BO), %xmm2

	mulpd	%xmm0, %xmm3
	movaps	 -4 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm4
	movaps	 -2 * SIZE(AO), %xmm1

	addpd	%xmm3, %xmm8
	addpd	%xmm4, %xmm12

	pshufd	$0x44, %xmm2, %xmm3
	pshufd	$0x44, %xmm2, %xmm4
	movsd	-13 * SIZE(BO), %xmm2

	mulpd	%xmm0, %xmm3
	movaps	  0 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm4
	movaps	  2 * SIZE(AO), %xmm1

	addpd	%xmm3, %xmm8
	addpd	%xmm4, %xmm12

	subq	$-16 * SIZE, AO
	subq	$ -4 * SIZE, BO
	subq	$1, %rax
	BRANCH
	jg	.L92
	ALIGN_4

.L95:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	BRANCH
	je	.L98
	ALIGN_4

.L96:
	pshufd	$0x44, %xmm2, %xmm3
	pshufd	$0x44, %xmm2, %xmm4
	movsd	-16 * SIZE(BO), %xmm2

	mulpd	%xmm0, %xmm3
	movaps	-12 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm4
	movaps	-10 * SIZE(AO), %xmm1

	addpd	%xmm3, %xmm8
	addpd	%xmm4, %xmm12

	addq	$4 * SIZE, AO
	addq	$1 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L96
	ALIGN_4

.L98:
	movups	ALPHA_R, %xmm7

	movsd	0 * SIZE(CO1), %xmm0
	movhps	1 * SIZE(CO1), %xmm0
	movsd	2 * SIZE(CO1), %xmm1
	movhps	3 * SIZE(CO1), %xmm1

	movsd	4 * SIZE(CO1), %xmm2
	movhps	5 * SIZE(CO1), %xmm2
	movsd	6 * SIZE(CO1), %xmm3
	movhps	7 * SIZE(CO1), %xmm3

	movddup	 %xmm8,  %xmm4
	mulpd	 %xmm7,  %xmm4
	addpd	 %xmm4,  %xmm0

	unpckhpd %xmm8,  %xmm8
	mulpd	 %xmm7,  %xmm8
	addpd	 %xmm8,  %xmm1

	movddup	 %xmm12, %xmm5
	mulpd	 %xmm7,  %xmm5
	addpd	 %xmm5,  %xmm2

	unpckhpd %xmm12, %xmm12
	mulpd	 %xmm7,  %xmm12
	addpd	 %xmm12, %xmm3

	movlps	%xmm0,  0 * SIZE(CO1)
	movhps	%xmm0,  1 * SIZE(CO1)
	movlps	%xmm1,  2 * SIZE(CO1)
	movhps	%xmm1,  3 * SIZE(CO1)

	movlps	%xmm2,  4 * SIZE(CO1)
	movhps	%xmm2,  5 * SIZE(CO1)
	movlps	%xmm3,  6 * SIZE(CO1)
	movhps	%xmm3,  7 * SIZE(CO1)

	addq	$8 * SIZE, CO1
	decq	I
	BRANCH
	jg	.L91
	ALIGN_4

.L100:
	testq	$2, M
	BRANCH
	jle	.L110
	ALIGN_4

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	movq	B, BO
#else
	movq	B, BO

	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
	addq	%rax, BO
#endif

	movaps	-16 * SIZE(AO), %xmm0
	xorps	%xmm8,  %xmm8
	movaps	-17 * SIZE(BO), %xmm2
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
	addq	$1, %rax
#endif
	movq	%rax, KKK
#endif
	sarq	$2, %rax
	NOBRANCH
	jle	.L105
	ALIGN_4

.L102:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)

	pshufd	$0x44, %xmm2, %xmm3
	movsd	-16 * SIZE(BO), %xmm2

	mulpd	%xmm0, %xmm3
	movaps	-14 * SIZE(AO), %xmm0
	addpd	%xmm3, %xmm8

	pshufd	$0x44, %xmm2, %xmm3
	movsd	-15 * SIZE(BO), %xmm2

	mulpd	%xmm0, %xmm3
	movaps	-12 * SIZE(AO), %xmm0
	addpd	%xmm3, %xmm9

	pshufd	$0x44, %xmm2, %xmm3
	movsd	-14 * SIZE(BO), %xmm2

	mulpd	%xmm0, %xmm3
	movaps	-10 * SIZE(AO), %xmm0
	addpd	%xmm3, %xmm8

	pshufd	$0x44, %xmm2, %xmm3
	movsd	-13 * SIZE(BO), %xmm2

	mulpd	%xmm0, %xmm3
	movaps	 -8 * SIZE(AO), %xmm0
	addpd	%xmm3, %xmm9

	subq	$-8 * SIZE, AO
	subq	$-4 * SIZE, BO
	subq	$1, %rax
	BRANCH
	jg	.L102
	ALIGN_4

.L105:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	BRANCH
	je	.L108
	ALIGN_4

.L106:
	pshufd	$0x44, %xmm2, %xmm3
	movsd	-16 * SIZE(BO), %xmm2

	mulpd	%xmm0, %xmm3
	movaps	-14 * SIZE(AO), %xmm0
	addpd	%xmm3, %xmm8

	addq	$2 * SIZE, AO
	addq	$1 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L106
	ALIGN_4

.L108:
	movups	ALPHA_R, %xmm7

	addpd	%xmm9, %xmm8

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
	ALIGN_4

.L110:
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
	addq	%rax, AO
	addq	%rax, BO
#endif

	movsd	-16 * SIZE(AO), %xmm0
	movsd	-17 * SIZE(BO), %xmm2

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
	addq	$1, %rax
#else
	addq	$1, %rax
#endif
	movq	%rax, KKK
#endif
	sarq	$2, %rax
	NOBRANCH
	jle	.L115
	ALIGN_4

.L112:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)

	mulsd	%xmm0, %xmm2
	addsd	%xmm2, %xmm8
	movsd	-15 * SIZE(AO), %xmm0
	movsd	-16 * SIZE(BO), %xmm2

	mulsd	%xmm0, %xmm2
	addsd	%xmm2, %xmm8
	movsd	-14 * SIZE(AO), %xmm0
	movsd	-15 * SIZE(BO), %xmm2

	mulsd	%xmm0, %xmm2
	addsd	%xmm2, %xmm8
	movsd	-13 * SIZE(AO), %xmm0
	movsd	-14 * SIZE(BO), %xmm2

	mulsd	%xmm0, %xmm2
	addsd	%xmm2, %xmm8
	movsd	-12 * SIZE(AO), %xmm0
	movsd	-13 * SIZE(BO), %xmm2

	subq	$-4 * SIZE, AO
	subq	$-4 * SIZE, BO
	subq	$1, %rax
	BRANCH
	jg	.L112
	ALIGN_4

.L115:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	BRANCH
	je	.L118
	ALIGN_4

.L116:
	mulsd	%xmm0, %xmm2
	addsd	%xmm2, %xmm8
	movsd	-15 * SIZE(AO), %xmm0
	movsd	-16 * SIZE(BO), %xmm2

	addq	$1 * SIZE, AO
	addq	$1 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L116
	ALIGN_4

.L118:
	movups	ALPHA_R, %xmm7

	addpd	%xmm9, %xmm8

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
