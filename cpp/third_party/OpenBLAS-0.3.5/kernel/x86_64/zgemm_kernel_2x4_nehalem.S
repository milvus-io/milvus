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

#define PREFETCHSIZE  8
#define PREFETCH     prefetcht0

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
#define ADD1	  addps
#define ADD2	  addps
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
#define ADD1	  addps
#define ADD2	  addps
#elif  defined(RN) || defined(RT) || defined(CN) || defined(CT)
#define ADD1	  addps
#define ADD2	  addps
#else
#define ADD1	  addps
#define ADD2	  subps
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

	unpcklps %xmm0, %xmm0
	unpcklps %xmm1, %xmm1

	movlps	 %xmm0, ALPHA_R
	movlps	 %xmm1, ALPHA_I

	subq	$-32 * SIZE, A
	subq	$-32 * SIZE, B

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
	sarq	$2, J
	NOBRANCH
	jle	.L30
	ALIGN_4

.L01:
#if defined(TRMMKERNEL) && defined(LEFT)
        movq    OFFSET, %rax
	movq    %rax, KK
#endif

	movq	C, CO1
	leaq	(C, LDC, 2), CO2
	movq	A, AO

	movq	K, %rax
	salq	$ZBASE_SHIFT + 2, %rax
	leaq	(B, %rax), BB

	movq	M,  I
	sarq	$1, I
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
	salq	$ZBASE_SHIFT, %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 4), BO
#endif

	xorps	%xmm1, %xmm1
	xorps	%xmm2, %xmm2
	xorps	%xmm3, %xmm3
	xorps	%xmm4, %xmm4

	xorps	%xmm8,  %xmm8
	prefetcht0     1 * SIZE(CO1)
	xorps	%xmm9,  %xmm9
	prefetcht0     3 * SIZE(CO1, LDC,  1)
	xorps	%xmm10, %xmm10
	xorps	%xmm11, %xmm11

	movaps	-32 * SIZE(AO), %xmm0

	xorps	%xmm12, %xmm12
	prefetcht0     1 * SIZE(CO2)
	xorps	%xmm13, %xmm13
	prefetcht0     3 * SIZE(CO2, LDC,  1)
	xorps	%xmm14, %xmm14
	xorps	%xmm15, %xmm15

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
	jle	.L15
	ALIGN_3

.L12:
	PREFETCH	(PREFETCHSIZE +  0) * SIZE(AO)

	ADD1	%xmm1, %xmm12
	movaps	-32 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm13
	pshufd	$0xb1, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	pshufd	$0x1b, %xmm2, %xmm5
	mulps	%xmm0, %xmm2

	ADD1	%xmm3, %xmm14
	ADD2	%xmm4, %xmm15
	pshufd	$0xb1, %xmm5, %xmm6
	mulps	%xmm0, %xmm5
	mulps	%xmm0, %xmm6

	ADD1	%xmm1, %xmm8
	movaps	-28 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0xb1, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	pshufd	$0x1b, %xmm2, %xmm3
	mulps	%xmm0, %xmm2

	ADD1	%xmm5, %xmm10
	ADD2	%xmm6, %xmm11
	pshufd	$0xb1, %xmm3, %xmm4
	movaps	-28 * SIZE(AO), %xmm7
	mulps	%xmm0, %xmm3
	mulps	%xmm0, %xmm4

	ADD1	%xmm1, %xmm12
	movaps	-24 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm13
	pshufd	$0xb1, %xmm1, %xmm2
	mulps	%xmm7, %xmm1
	pshufd	$0x1b, %xmm2, %xmm5
	mulps	%xmm7, %xmm2

	ADD1	%xmm3, %xmm14
	ADD2	%xmm4, %xmm15
	pshufd	$0xb1, %xmm5, %xmm6
	mulps	%xmm7, %xmm5
	mulps	%xmm7, %xmm6

	ADD1	%xmm1, %xmm8
	movaps	-20 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0xb1, %xmm1, %xmm2
	mulps	%xmm7, %xmm1
	pshufd	$0x1b, %xmm2, %xmm3
	mulps	%xmm7, %xmm2

	ADD1	%xmm5, %xmm10
	ADD2	%xmm6, %xmm11
	pshufd	$0xb1, %xmm3, %xmm4
	movaps	-24 * SIZE(AO), %xmm0
	mulps	%xmm7, %xmm3
	mulps	%xmm7, %xmm4

	ADD1	%xmm1, %xmm12
	movaps	-16 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm13
	pshufd	$0xb1, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	pshufd	$0x1b, %xmm2, %xmm5
	mulps	%xmm0, %xmm2

	ADD1	%xmm3, %xmm14
	ADD2	%xmm4, %xmm15
	pshufd	$0xb1, %xmm5, %xmm6
	mulps	%xmm0, %xmm5
	mulps	%xmm0, %xmm6

	ADD1	%xmm1, %xmm8
	movaps	-12 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0xb1, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	pshufd	$0x1b, %xmm2, %xmm3
	mulps	%xmm0, %xmm2

	ADD1	%xmm5, %xmm10
	ADD2	%xmm6, %xmm11
	pshufd	$0xb1, %xmm3, %xmm4
	movaps	-20 * SIZE(AO), %xmm7
	mulps	%xmm0, %xmm3
	mulps	%xmm0, %xmm4

	ADD1	%xmm1, %xmm12
	movaps	 -8 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm13
	pshufd	$0xb1, %xmm1, %xmm2
	mulps	%xmm7, %xmm1
	pshufd	$0x1b, %xmm2, %xmm5
	mulps	%xmm7, %xmm2

	ADD1	%xmm3, %xmm14
	ADD2	%xmm4, %xmm15
	pshufd	$0xb1, %xmm5, %xmm6
	mulps	%xmm7, %xmm5
	mulps	%xmm7, %xmm6

	ADD1	%xmm1, %xmm8
	movaps	 -4 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm9
	subq	$-32 * SIZE, BO
	pshufd	$0xb1, %xmm1, %xmm2
	mulps	%xmm7, %xmm1
	pshufd	$0x1b, %xmm2, %xmm3
	mulps	%xmm7, %xmm2

	ADD1	%xmm5, %xmm10
	ADD2	%xmm6, %xmm11
	pshufd	$0xb1, %xmm3, %xmm4
	mulps	%xmm7, %xmm3
	movaps	-16 * SIZE(AO), %xmm0
	mulps	%xmm7, %xmm4

	subq	$-16 * SIZE, AO
	subq	$1, %rax
	BRANCH
	jg	.L12
	ALIGN_3

.L15:
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
	ADD1	%xmm1, %xmm12
	movaps	-32 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm13
	pshufd	$0xb1, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	pshufd	$0x1b, %xmm2, %xmm5
	mulps	%xmm0, %xmm2

	ADD1	%xmm3, %xmm14
	ADD2	%xmm4, %xmm15
	pshufd	$0xb1, %xmm5, %xmm6
	mulps	%xmm0, %xmm5
	mulps	%xmm0, %xmm6

	ADD1	%xmm1, %xmm8
	movaps	-28 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0xb1, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	pshufd	$0x1b, %xmm2, %xmm3
	mulps	%xmm0, %xmm2

	ADD1	%xmm5, %xmm10
	ADD2	%xmm6, %xmm11
	pshufd	$0xb1, %xmm3, %xmm4
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
	ADD1	%xmm1, %xmm12
	ADD2	%xmm2, %xmm13
	ADD1	%xmm3, %xmm14
	ADD2	%xmm4, %xmm15

	movddup	ALPHA_R, %xmm2
	movddup	ALPHA_I, %xmm3

	pcmpeqb	%xmm0, %xmm0
	psllq	$63,   %xmm0

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
      defined(RR) || defined(RC) || defined(CR) || defined(CC)
	pxor	%xmm0, %xmm8
	pxor	%xmm0, %xmm10
	pxor	%xmm0, %xmm12
	pxor	%xmm0, %xmm14
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
	pshufd	$0xb1, %xmm0, %xmm0

	pxor	%xmm0, %xmm9
	pxor	%xmm0, %xmm11
	pxor	%xmm0, %xmm13
	pxor	%xmm0, %xmm15
#elif defined(RN) || defined(RT) || defined(CN) || defined(CT)
	pxor	%xmm0, %xmm9
	pxor	%xmm0, %xmm11
	pxor	%xmm0, %xmm13
	pxor	%xmm0, %xmm15
#endif

	haddps	%xmm9,  %xmm8
	haddps	%xmm11, %xmm10
	haddps	%xmm13, %xmm12
	haddps	%xmm15, %xmm14

	shufps	$0xd8, %xmm8,  %xmm8
	shufps	$0xd8, %xmm10, %xmm10
	shufps	$0xd8, %xmm12, %xmm12
	shufps	$0xd8, %xmm14, %xmm14

	movaps	%xmm8, %xmm9
	shufps	$0xe4, %xmm10, %xmm8
	shufps	$0xe4, %xmm9, %xmm10

	movaps	%xmm12, %xmm13
	shufps	$0xe4, %xmm14, %xmm12
	shufps	$0xe4, %xmm13, %xmm14

	pshufd	$0xb1, %xmm8,  %xmm9
	pshufd	$0xb1, %xmm10, %xmm11
	pshufd	$0xb1, %xmm12, %xmm13
	pshufd	$0xb1, %xmm14, %xmm15

	mulps	%xmm2, %xmm8
	mulps	%xmm3, %xmm9
	mulps	%xmm2, %xmm12
	mulps	%xmm3, %xmm13

	mulps	%xmm2, %xmm10
	mulps	%xmm3, %xmm11
	mulps	%xmm2, %xmm14
	mulps	%xmm3, %xmm15

	addsubps	%xmm9,  %xmm8
	addsubps	%xmm11, %xmm10
	addsubps	%xmm13, %xmm12
	addsubps	%xmm15, %xmm14

#ifndef TRMMKERNEL
	movups	0 * SIZE(CO1), %xmm0
	movups	0 * SIZE(CO1, LDC), %xmm1
	movups	0 * SIZE(CO2), %xmm2
	movups	0 * SIZE(CO2, LDC), %xmm3

	addps	%xmm0, %xmm8
	addps	%xmm1, %xmm10
	addps	%xmm2, %xmm12
	addps	%xmm3, %xmm14
#endif

	movups	%xmm8,  0 * SIZE(CO1)
	movups	%xmm10, 0 * SIZE(CO1, LDC)
	movups	%xmm12, 0 * SIZE(CO2)
	movups	%xmm14, 0 * SIZE(CO2, LDC)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	salq	$ZBASE_SHIFT, %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 4), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$2, KK
#endif

	addq	$4 * SIZE, CO1
	addq	$4 * SIZE, CO2
	decq	I			# i --
	BRANCH
	jg	.L11
	ALIGN_4

.L20:
	testq	$1, M
	BRANCH
	jle	.L29

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	movq	B, BO
#else
	movq	B, BO

	movq	KK, %rax
	salq	$ZBASE_SHIFT, %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 4), BO
#endif

	xorps	%xmm1, %xmm1
	movddup  -32 * SIZE(AO), %xmm0
	xorps	%xmm2, %xmm2
	movaps   -32 * SIZE(BO), %xmm5
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
	addq	$1, %rax
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
	ADD1	%xmm1, %xmm8
	pshufd	$0xa0, %xmm5, %xmm1
	mulps	%xmm0, %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0xf5, %xmm5, %xmm2
	movaps   -28 * SIZE(BO), %xmm5
	mulps	%xmm0, %xmm2
	ADD1	%xmm3, %xmm10
	pshufd	$0xa0, %xmm5, %xmm3
	mulps	%xmm0, %xmm3
	ADD2	%xmm4, %xmm11
	pshufd	$0xf5, %xmm5, %xmm4
	movaps   -24 * SIZE(BO), %xmm5
	mulps	%xmm0, %xmm4
	movddup  -30 * SIZE(AO), %xmm0

	ADD1	%xmm1, %xmm8
	pshufd	$0xa0, %xmm5, %xmm1
	mulps	%xmm0, %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0xf5, %xmm5, %xmm2
	movaps   -20 * SIZE(BO), %xmm5
	mulps	%xmm0, %xmm2
	ADD1	%xmm3, %xmm10
	pshufd	$0xa0, %xmm5, %xmm3
	mulps	%xmm0, %xmm3
	ADD2	%xmm4, %xmm11
	pshufd	$0xf5, %xmm5, %xmm4
	movaps   -16 * SIZE(BO), %xmm5
	mulps	%xmm0, %xmm4
	movddup  -28 * SIZE(AO), %xmm0

	ADD1	%xmm1, %xmm8
	pshufd	$0xa0, %xmm5, %xmm1
	mulps	%xmm0, %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0xf5, %xmm5, %xmm2
	movaps   -12 * SIZE(BO), %xmm5
	mulps	%xmm0, %xmm2
	ADD1	%xmm3, %xmm10
	pshufd	$0xa0, %xmm5, %xmm3
	mulps	%xmm0, %xmm3
	ADD2	%xmm4, %xmm11
	pshufd	$0xf5, %xmm5, %xmm4
	movaps    -8 * SIZE(BO), %xmm5
	mulps	%xmm0, %xmm4
	movddup  -26 * SIZE(AO), %xmm0

	ADD1	%xmm1, %xmm8
	pshufd	$0xa0, %xmm5, %xmm1
	mulps	%xmm0, %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0xf5, %xmm5, %xmm2
	movaps    -4 * SIZE(BO), %xmm5
	mulps	%xmm0, %xmm2
	ADD1	%xmm3, %xmm10
	pshufd	$0xa0, %xmm5, %xmm3
	mulps	%xmm0, %xmm3
	ADD2	%xmm4, %xmm11
	pshufd	$0xf5, %xmm5, %xmm4
	movaps     0 * SIZE(BO), %xmm5
	mulps	%xmm0, %xmm4
	movddup  -24 * SIZE(AO), %xmm0

	subq	$-32 * SIZE, BO
	subq	$ -8 * SIZE, AO

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
	andq	$3, %rax		# if (k & 1)
	BRANCH
	je	.L28
	ALIGN_3

.L26:
	ADD1	%xmm1, %xmm8
	pshufd	$0xa0, %xmm5, %xmm1
	mulps	%xmm0, %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0xf5, %xmm5, %xmm2
	movaps   -28 * SIZE(BO), %xmm5
	mulps	%xmm0, %xmm2
	ADD1	%xmm3, %xmm10
	pshufd	$0xa0, %xmm5, %xmm3
	mulps	%xmm0, %xmm3
	ADD2	%xmm4, %xmm11
	pshufd	$0xf5, %xmm5, %xmm4
	movaps   -24 * SIZE(BO), %xmm5
	mulps	%xmm0, %xmm4
	movddup  -30 * SIZE(AO), %xmm0

	addq	$2 * SIZE, AO
	addq	$8 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L26
	ALIGN_3

.L28:
	ADD1	%xmm1, %xmm8
	ADD2	%xmm2, %xmm9
	ADD1	%xmm3, %xmm10
	ADD2	%xmm4, %xmm11

	pcmpeqb	%xmm0, %xmm0
	psllq	$63,   %xmm0

	movddup	ALPHA_R, %xmm2
	movddup	ALPHA_I, %xmm3

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
	pxor	%xmm0, %xmm9
	pxor	%xmm0, %xmm11

	shufps	$0xb1, %xmm9,  %xmm9
	shufps	$0xb1, %xmm11, %xmm11

#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
	shufps	$0xb1, %xmm9,  %xmm9
	shufps	$0xb1, %xmm11, %xmm11

	pxor	%xmm0, %xmm9
	pxor	%xmm0, %xmm11
#else
	pxor	%xmm0, %xmm8
	pxor	%xmm0, %xmm10

	shufps	$0xb1, %xmm9,  %xmm9
	shufps	$0xb1, %xmm11, %xmm11
#endif

	addps	%xmm9,  %xmm8
	addps	%xmm11, %xmm10

	pshufd	$0xb1, %xmm8,  %xmm9
	pshufd	$0xb1, %xmm10, %xmm11

	mulps	%xmm2, %xmm8
	mulps	%xmm3, %xmm9
	mulps	%xmm2, %xmm10
	mulps	%xmm3, %xmm11

	addsubps	%xmm9,  %xmm8
	addsubps	%xmm11, %xmm10

#ifndef TRMMKERNEL
	movsd	(CO1), %xmm0
	movhps	(CO1, LDC), %xmm0
	movsd	(CO2), %xmm1
	movhps	(CO2, LDC), %xmm1

	addps	%xmm0, %xmm8
	addps	%xmm1, %xmm10
#endif

	movsd	%xmm8,  (CO1)
	movhps	%xmm8,  (CO1, LDC)
	movsd	%xmm10, (CO2)
	movhps	%xmm10, (CO2, LDC)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	salq	$ZBASE_SHIFT, %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 4), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$1, KK
#endif
	ALIGN_4

.L29:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addq	$4, KK
#endif

	leaq	(C, LDC, 4), C
	movq	BO, B

	subq	$1, J
	BRANCH
	jg	.L01
	ALIGN_4

.L30:
	testq	$2, N
	BRANCH
	jle	.L50

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
	salq	$ZBASE_SHIFT, %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 2), BO
#endif

	xorps	%xmm1, %xmm1
	movaps	-32 * SIZE(AO), %xmm0
	xorps	%xmm2, %xmm2
	xorps	%xmm3, %xmm3
	xorps	%xmm4, %xmm4

	xorps	%xmm8,  %xmm8
	prefetcht2     4 * SIZE(CO1)
	xorps	%xmm9,  %xmm9
	prefetcht2     4 * SIZE(CO2)
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
	jle	.L35
	ALIGN_3

.L32:
	PREFETCH	(PREFETCHSIZE +  0) * SIZE(AO)

	ADD1	%xmm1, %xmm8
	movaps	-32 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0xb1, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	ADD1	%xmm3, %xmm10
	pshufd	$0x1b, %xmm2, %xmm3
	mulps	%xmm0, %xmm2
	ADD2	%xmm4, %xmm11
	pshufd	$0xb1, %xmm3, %xmm4
	mulps	%xmm0, %xmm3
	mulps	%xmm0, %xmm4
	movaps	-28 * SIZE(AO), %xmm0

	ADD1	%xmm1, %xmm8
	movaps	-28 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0xb1, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	ADD1	%xmm3, %xmm10
	pshufd	$0x1b, %xmm2, %xmm3
	mulps	%xmm0, %xmm2
	ADD2	%xmm4, %xmm11
	pshufd	$0xb1, %xmm3, %xmm4
	mulps	%xmm0, %xmm3
	mulps	%xmm0, %xmm4
	movaps	-24 * SIZE(AO), %xmm0

	ADD1	%xmm1, %xmm8
	movaps	-24 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0xb1, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	ADD1	%xmm3, %xmm10
	pshufd	$0x1b, %xmm2, %xmm3
	mulps	%xmm0, %xmm2
	ADD2	%xmm4, %xmm11
	pshufd	$0xb1, %xmm3, %xmm4
	mulps	%xmm0, %xmm3
	mulps	%xmm0, %xmm4
	movaps	-20 * SIZE(AO), %xmm0

	ADD1	%xmm1, %xmm8
	movaps	-20 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0xb1, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	ADD1	%xmm3, %xmm10
	pshufd	$0x1b, %xmm2, %xmm3
	mulps	%xmm0, %xmm2
	ADD2	%xmm4, %xmm11
	pshufd	$0xb1, %xmm3, %xmm4
	mulps	%xmm0, %xmm3
	mulps	%xmm0, %xmm4
	movaps	-16 * SIZE(AO), %xmm0

	subq	$-16 * SIZE, BO
	subq	$-16 * SIZE, AO

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
	andq	$3, %rax		# if (k & 1)
	BRANCH
	je	.L38
	ALIGN_3

.L36:
	ADD1	%xmm1, %xmm8
	movaps	-32 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0xb1, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	ADD1	%xmm3, %xmm10
	pshufd	$0x1b, %xmm2, %xmm3
	mulps	%xmm0, %xmm2
	ADD2	%xmm4, %xmm11
	pshufd	$0xb1, %xmm3, %xmm4
	mulps	%xmm0, %xmm3
	mulps	%xmm0, %xmm4
	movaps	-28 * SIZE(AO), %xmm0

	addq	$4 * SIZE, AO
	addq	$4 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L36
	ALIGN_3

.L38:
	ADD1	%xmm1, %xmm8
	ADD2	%xmm2, %xmm9
	ADD1	%xmm3, %xmm10
	ADD2	%xmm4, %xmm11

	pcmpeqb	%xmm0, %xmm0
	psllq	$63,   %xmm0
	movddup	ALPHA_R, %xmm2
	movddup	ALPHA_I, %xmm3

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
      defined(RR) || defined(RC) || defined(CR) || defined(CC)
	pxor	%xmm0, %xmm8
	pxor	%xmm0, %xmm10
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
	pshufd	$0xb1, %xmm0, %xmm0

	pxor	%xmm0, %xmm9
	pxor	%xmm0, %xmm11
#elif defined(RN) || defined(RT) || defined(CN) || defined(CT)
	pxor	%xmm0, %xmm9
	pxor	%xmm0, %xmm11
#endif

	haddps	%xmm9,  %xmm8
	haddps	%xmm11, %xmm10

	shufps	$0xd8, %xmm8,  %xmm8
	shufps	$0xd8, %xmm10, %xmm10

	movaps	%xmm8, %xmm9
	shufps	$0xe4, %xmm10, %xmm8
	shufps	$0xe4, %xmm9, %xmm10

	pshufd	$0xb1, %xmm8,  %xmm9
	pshufd	$0xb1, %xmm10, %xmm11

	mulps	%xmm2, %xmm8
	mulps	%xmm3, %xmm9

	mulps	%xmm2, %xmm10
	mulps	%xmm3, %xmm11

	addsubps	%xmm9,  %xmm8
	addsubps	%xmm11, %xmm10

#ifndef TRMMKERNEL
	movsd	0 * SIZE(CO1), %xmm0
	movhps	2 * SIZE(CO1), %xmm0
	movsd	0 * SIZE(CO2), %xmm1
	movhps	2 * SIZE(CO2), %xmm1

	addps	%xmm0, %xmm8
	addps	%xmm1, %xmm10
#endif

	movsd	%xmm8,  0 * SIZE(CO1)
	movhps	%xmm8,  2 * SIZE(CO1)
	movsd	%xmm10, 0 * SIZE(CO2)
	movhps	%xmm10, 2 * SIZE(CO2)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	salq	$ZBASE_SHIFT, %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 2), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$2, KK
#endif

	addq	$4 * SIZE, CO1
	addq	$4 * SIZE, CO2
	decq	I			# i --
	BRANCH
	jg	.L31
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
	salq	$ZBASE_SHIFT, %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 2), BO
#endif

	xorps	%xmm1, %xmm1
	movddup  -32 * SIZE(AO), %xmm0
	xorps	%xmm2, %xmm2
	movaps   -32 * SIZE(BO), %xmm5
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
	addq	$1, %rax
#else
	addq	$2, %rax
#endif
	movq	%rax, KKK
#endif
	sarq	$2, %rax
	NOBRANCH
	jle	.L45
	ALIGN_3

.L42:
	ADD1	%xmm1, %xmm8
	pshufd	$0xa0, %xmm5, %xmm1
	mulps	%xmm0, %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0xf5, %xmm5, %xmm2
	movaps   -28 * SIZE(BO), %xmm5
	mulps	%xmm0, %xmm2
	movddup  -30 * SIZE(AO), %xmm0

	ADD1	%xmm1, %xmm8
	pshufd	$0xa0, %xmm5, %xmm1
	mulps	%xmm0, %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0xf5, %xmm5, %xmm2
	movaps   -24 * SIZE(BO), %xmm5
	mulps	%xmm0, %xmm2
	movddup  -28 * SIZE(AO), %xmm0

	ADD1	%xmm1, %xmm8
	pshufd	$0xa0, %xmm5, %xmm1
	mulps	%xmm0, %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0xf5, %xmm5, %xmm2
	movaps   -20 * SIZE(BO), %xmm5
	mulps	%xmm0, %xmm2
	movddup  -26 * SIZE(AO), %xmm0

	ADD1	%xmm1, %xmm8
	pshufd	$0xa0, %xmm5, %xmm1
	mulps	%xmm0, %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0xf5, %xmm5, %xmm2
	movaps   -16 * SIZE(BO), %xmm5
	mulps	%xmm0, %xmm2
	movddup  -24 * SIZE(AO), %xmm0

	subq	$-16 * SIZE, BO
	subq	$ -8 * SIZE, AO

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
	andq	$3, %rax		# if (k & 1)
	BRANCH
	je	.L48
	ALIGN_3

.L46:
	ADD1	%xmm1, %xmm8
	pshufd	$0xa0, %xmm5, %xmm1
	mulps	%xmm0, %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0xf5, %xmm5, %xmm2
	movaps   -28 * SIZE(BO), %xmm5
	mulps	%xmm0, %xmm2
	movddup  -30 * SIZE(AO), %xmm0

	addq	$2 * SIZE, AO
	addq	$4 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L46
	ALIGN_3

.L48:
	ADD1	%xmm1, %xmm8
	ADD2	%xmm2, %xmm9

	pcmpeqb	%xmm0, %xmm0
	psllq	$63,   %xmm0

	movddup	ALPHA_R, %xmm2
	movddup	ALPHA_I, %xmm3

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
	pxor	%xmm0, %xmm9

	shufps	$0xb1, %xmm9,  %xmm9
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
	shufps	$0xb1, %xmm9,  %xmm9

	pxor	%xmm0, %xmm9
#else
	pxor	%xmm0, %xmm8

	shufps	$0xb1, %xmm9,  %xmm9
#endif

	addps	%xmm9,  %xmm8

	pshufd	$0xb1, %xmm8,  %xmm9

	mulps	%xmm2, %xmm8
	mulps	%xmm3, %xmm9

	addsubps	%xmm9,  %xmm8

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
	salq	$ZBASE_SHIFT, %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 2), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$1, KK
#endif
	ALIGN_4

.L49:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addq	$2, KK
#endif

	leaq	(C, LDC, 2), C
	movq	BO, B
	ALIGN_4

.L50:
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
	salq	$ZBASE_SHIFT, %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 1), BO
#endif

	xorps	%xmm1, %xmm1
	movaps	-32 * SIZE(AO), %xmm0
	xorps	%xmm2, %xmm2
	xorps	%xmm3, %xmm3
	xorps	%xmm4, %xmm4

	xorps	%xmm8,  %xmm8
	prefetcht2     4 * SIZE(CO1)
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
	jle	.L55
	ALIGN_3

.L52:
	PREFETCH	(PREFETCHSIZE +  0) * SIZE(AO)

	ADD1	%xmm1, %xmm8
	movddup	-32 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0xb1, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	mulps	%xmm0, %xmm2
	movaps	-28 * SIZE(AO), %xmm0

	ADD1	%xmm1, %xmm8
	movddup	-30 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0xb1, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	mulps	%xmm0, %xmm2
	movaps	-24 * SIZE(AO), %xmm0

	ADD1	%xmm1, %xmm8
	movddup	-28 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0xb1, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	mulps	%xmm0, %xmm2
	movaps	-20 * SIZE(AO), %xmm0

	ADD1	%xmm1, %xmm8
	movddup	-26 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0xb1, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	mulps	%xmm0, %xmm2
	movaps	-16 * SIZE(AO), %xmm0

	subq	$ -8 * SIZE, BO
	subq	$-16 * SIZE, AO

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
	ADD1	%xmm1, %xmm8
	movddup	-32 * SIZE(BO), %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0xb1, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	mulps	%xmm0, %xmm2
	movaps	-28 * SIZE(AO), %xmm0

	addq	$4 * SIZE, AO
	addq	$2 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L56
	ALIGN_3

.L58:
	ADD1	%xmm1, %xmm8
	ADD2	%xmm2, %xmm9

	pcmpeqb	%xmm0, %xmm0
	psllq	$63,   %xmm0
	movddup	ALPHA_R, %xmm2
	movddup	ALPHA_I, %xmm3

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
      defined(RR) || defined(RC) || defined(CR) || defined(CC)
	pxor	%xmm0, %xmm8
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
	pshufd	$0xb1, %xmm0, %xmm0

	pxor	%xmm0, %xmm9
#elif defined(RN) || defined(RT) || defined(CN) || defined(CT)
	pxor	%xmm0, %xmm9
#endif

	haddps	%xmm9,  %xmm8

	shufps	$0xd8, %xmm8,  %xmm8
	pshufd	$0xb1, %xmm8,  %xmm9

	mulps	%xmm2, %xmm8
	mulps	%xmm3, %xmm9

	addsubps	%xmm9,  %xmm8

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
	salq	$ZBASE_SHIFT, %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 1), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$2, KK
#endif

	addq	$4 * SIZE, CO1
	decq	I			# i --
	BRANCH
	jg	.L51
	ALIGN_4

.L60:
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
	salq	$ZBASE_SHIFT, %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 1), BO
#endif

	xorps	%xmm1, %xmm1
	movddup  -32 * SIZE(AO), %xmm0
	xorps	%xmm2, %xmm2
	movsd   -32 * SIZE(BO), %xmm5
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
	jle	.L65
	ALIGN_3

.L62:
	ADD1	%xmm1, %xmm8
	pshufd	$0xa0, %xmm5, %xmm1
	mulps	%xmm0, %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0xf5, %xmm5, %xmm2
	movsd   -30 * SIZE(BO), %xmm5
	mulps	%xmm0, %xmm2
	movddup -30 * SIZE(AO), %xmm0

	ADD1	%xmm1, %xmm8
	pshufd	$0xa0, %xmm5, %xmm1
	mulps	%xmm0, %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0xf5, %xmm5, %xmm2
	movsd   -28 * SIZE(BO), %xmm5
	mulps	%xmm0, %xmm2
	movddup -28 * SIZE(AO), %xmm0

	ADD1	%xmm1, %xmm8
	pshufd	$0xa0, %xmm5, %xmm1
	mulps	%xmm0, %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0xf5, %xmm5, %xmm2
	movsd   -26 * SIZE(BO), %xmm5
	mulps	%xmm0, %xmm2
	movddup -26 * SIZE(AO), %xmm0

	ADD1	%xmm1, %xmm8
	pshufd	$0xa0, %xmm5, %xmm1
	mulps	%xmm0, %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0xf5, %xmm5, %xmm2
	movsd   -24 * SIZE(BO), %xmm5
	mulps	%xmm0, %xmm2
	movddup -24 * SIZE(AO), %xmm0

	subq	$-8 * SIZE, BO
	subq	$-8 * SIZE, AO

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
	andq	$3, %rax		# if (k & 1)
	BRANCH
	je	.L68
	ALIGN_3

.L66:
	ADD1	%xmm1, %xmm8
	pshufd	$0xa0, %xmm5, %xmm1
	mulps	%xmm0, %xmm1
	ADD2	%xmm2, %xmm9
	pshufd	$0xf5, %xmm5, %xmm2
	movsd   -30 * SIZE(BO), %xmm5
	mulps	%xmm0, %xmm2
	movddup -30 * SIZE(AO), %xmm0

	addq	$2 * SIZE, AO
	addq	$2 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L66
	ALIGN_3

.L68:
	ADD1	%xmm1, %xmm8
	ADD2	%xmm2, %xmm9

	pcmpeqb	%xmm0, %xmm0
	psllq	$63,   %xmm0

	movddup	ALPHA_R, %xmm2
	movddup	ALPHA_I, %xmm3

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
	pxor	%xmm0, %xmm9

	shufps	$0xb1, %xmm9,  %xmm9
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
	shufps	$0xb1, %xmm9,  %xmm9

	pxor	%xmm0, %xmm9
#else
	pxor	%xmm0, %xmm8

	shufps	$0xb1, %xmm9,  %xmm9
#endif

	addps	%xmm9,  %xmm8

	pshufd	$0xb1, %xmm8,  %xmm9

	mulps	%xmm2, %xmm8
	mulps	%xmm3, %xmm9

	addsubps	%xmm9,  %xmm8

#ifndef TRMMKERNEL
	movsd	(CO1), %xmm0
	addps	%xmm0, %xmm8
#endif

	movsd	%xmm8,  (CO1)
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
