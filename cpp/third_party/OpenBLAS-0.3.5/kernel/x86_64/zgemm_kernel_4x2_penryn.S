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
	sarq	$1, J
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
	salq	$ZBASE_SHIFT + 1, %rax
	leaq	(B, %rax), BB

	movq	M,  I
	sarq	$2, I
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
	salq	$ZBASE_SHIFT, %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 2), BO
#endif

	movaps	-32 * SIZE(AO), %xmm0
	pxor	%xmm3, %xmm3
	movaps	-28 * SIZE(AO), %xmm1
	pxor	%xmm4, %xmm4
	movaps	-32 * SIZE(BO), %xmm2

	pxor	%xmm5, %xmm5
	prefetcht0	 -32 * SIZE(BB)
	pxor	%xmm6, %xmm6

	prefetcht2     7 * SIZE(CO1)
	movapd	%xmm4, %xmm8
	movapd	%xmm4, %xmm9
	movapd	%xmm4, %xmm10
	movapd	%xmm4, %xmm11

	prefetcht2     7 * SIZE(CO2)
	movapd	%xmm4, %xmm12
	movapd	%xmm4, %xmm13
	movapd	%xmm4, %xmm14
	movapd	%xmm4, %xmm15

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
	addq	$4, %rax
#else
	addq	$2, %rax
#endif
	movq	%rax, KKK
#endif
	sarq	$3, %rax
	NOBRANCH
	jle	.L15
	ALIGN_3

.L12:
	PREFETCH -32 * SIZE(PREA)
	ADD1	%xmm6, %xmm10
	ADD1	%xmm3, %xmm14
	movaps	%xmm2, %xmm3
	pshufd	$0xb1, %xmm2, %xmm7
	mulps	%xmm0, %xmm2
	mulps	%xmm1, %xmm3

	ADD2	%xmm4, %xmm11
	ADD2	%xmm5, %xmm15
	movaps	%xmm7, %xmm5
	pshufd	$0x1b, %xmm7, %xmm6
	mulps	%xmm0, %xmm7
	mulps	%xmm1, %xmm5

	ADD1	%xmm2, %xmm8
	movaps	-28 * SIZE(BO), %xmm2
	ADD1	%xmm3, %xmm12
	movaps	%xmm6, %xmm3
	pshufd	$0xb1, %xmm6, %xmm4
	mulps	%xmm0, %xmm6
	mulps	%xmm1, %xmm3

	ADD2	%xmm7, %xmm9
	ADD2	%xmm5, %xmm13
	movaps	%xmm4, %xmm5
	mulps	%xmm0, %xmm4
	movaps	-24 * SIZE(AO), %xmm0
	mulps	%xmm1, %xmm5
	movaps	-20 * SIZE(AO), %xmm1

	ADD1	%xmm6, %xmm10
	ADD1	%xmm3, %xmm14
	movaps	%xmm2, %xmm3
	pshufd	$0xb1, %xmm2, %xmm7
	mulps	%xmm0, %xmm2
	mulps	%xmm1, %xmm3

	ADD2	%xmm4, %xmm11
	ADD2	%xmm5, %xmm15
	movaps	%xmm7, %xmm5
	pshufd	$0x1b, %xmm7, %xmm6
	mulps	%xmm0, %xmm7
	mulps	%xmm1, %xmm5

	ADD1	%xmm2, %xmm8
	movaps	-24 * SIZE(BO), %xmm2
	ADD1	%xmm3, %xmm12
	movaps	%xmm6, %xmm3
	pshufd	$0xb1, %xmm6, %xmm4
	mulps	%xmm0, %xmm6
	mulps	%xmm1, %xmm3

	ADD2	%xmm7, %xmm9
	ADD2	%xmm5, %xmm13
	movaps	%xmm4, %xmm5
	mulps	%xmm0, %xmm4
	movaps	-16 * SIZE(AO), %xmm0
	mulps	%xmm1, %xmm5
	movaps	-12 * SIZE(AO), %xmm1

	ADD1	%xmm6, %xmm10
	ADD1	%xmm3, %xmm14
	PREFETCH -16 * SIZE(PREA)
	movaps	%xmm2, %xmm3
	pshufd	$0xb1, %xmm2, %xmm7
	mulps	%xmm0, %xmm2
	mulps	%xmm1, %xmm3

	ADD2	%xmm4, %xmm11
	ADD2	%xmm5, %xmm15
	movaps	%xmm7, %xmm5
	pshufd	$0x1b, %xmm7, %xmm6
	mulps	%xmm0, %xmm7
	mulps	%xmm1, %xmm5

	ADD1	%xmm2, %xmm8
	movaps	-20 * SIZE(BO), %xmm2
	ADD1	%xmm3, %xmm12
	movaps	%xmm6, %xmm3
	pshufd	$0xb1, %xmm6, %xmm4
	mulps	%xmm0, %xmm6
	mulps	%xmm1, %xmm3

	ADD2	%xmm7, %xmm9
	ADD2	%xmm5, %xmm13
	movaps	%xmm4, %xmm5
	mulps	%xmm0, %xmm4
	movaps	 -8 * SIZE(AO), %xmm0
	mulps	%xmm1, %xmm5
	movaps	 -4 * SIZE(AO), %xmm1

	ADD1	%xmm6, %xmm10
	ADD1	%xmm3, %xmm14
	movaps	%xmm2, %xmm3
	pshufd	$0xb1, %xmm2, %xmm7
	mulps	%xmm0, %xmm2
	mulps	%xmm1, %xmm3

	ADD2	%xmm4, %xmm11
	ADD2	%xmm5, %xmm15
	movaps	%xmm7, %xmm5
	pshufd	$0x1b, %xmm7, %xmm6
	mulps	%xmm0, %xmm7
	mulps	%xmm1, %xmm5

	ADD1	%xmm2, %xmm8
	movaps	-16 * SIZE(BO), %xmm2
	ADD1	%xmm3, %xmm12
	movaps	%xmm6, %xmm3
	pshufd	$0xb1, %xmm6, %xmm4
	mulps	%xmm0, %xmm6
	mulps	%xmm1, %xmm3

	ADD2	%xmm7, %xmm9
	ADD2	%xmm5, %xmm13
	movaps	%xmm4, %xmm5
	mulps	%xmm0, %xmm4
	movaps	  0 * SIZE(AO), %xmm0
	mulps	%xmm1, %xmm5
	movaps	  4 * SIZE(AO), %xmm1

	ADD1	%xmm6, %xmm10
	ADD1	%xmm3, %xmm14
	PREFETCH  0 * SIZE(PREA)
	movaps	%xmm2, %xmm3
	pshufd	$0xb1, %xmm2, %xmm7
	mulps	%xmm0, %xmm2
	mulps	%xmm1, %xmm3

	ADD2	%xmm4, %xmm11
	ADD2	%xmm5, %xmm15
	movaps	%xmm7, %xmm5
	pshufd	$0x1b, %xmm7, %xmm6
	mulps	%xmm0, %xmm7
	mulps	%xmm1, %xmm5

	ADD1	%xmm2, %xmm8
	movaps	-12 * SIZE(BO), %xmm2
	ADD1	%xmm3, %xmm12
	movaps	%xmm6, %xmm3
	pshufd	$0xb1, %xmm6, %xmm4
	mulps	%xmm0, %xmm6
	mulps	%xmm1, %xmm3

	ADD2	%xmm7, %xmm9
	ADD2	%xmm5, %xmm13
	movaps	%xmm4, %xmm5
	mulps	%xmm0, %xmm4
	movaps	  8 * SIZE(AO), %xmm0
	mulps	%xmm1, %xmm5
	movaps	 12 * SIZE(AO), %xmm1

	ADD1	%xmm6, %xmm10
	ADD1	%xmm3, %xmm14
	movaps	%xmm2, %xmm3
	pshufd	$0xb1, %xmm2, %xmm7
	mulps	%xmm0, %xmm2
	mulps	%xmm1, %xmm3

	ADD2	%xmm4, %xmm11
	ADD2	%xmm5, %xmm15
	movaps	%xmm7, %xmm5
	pshufd	$0x1b, %xmm7, %xmm6
	mulps	%xmm0, %xmm7
	mulps	%xmm1, %xmm5

	ADD1	%xmm2, %xmm8
	movaps	 -8 * SIZE(BO), %xmm2
	ADD1	%xmm3, %xmm12
	movaps	%xmm6, %xmm3
	pshufd	$0xb1, %xmm6, %xmm4
	mulps	%xmm0, %xmm6
	mulps	%xmm1, %xmm3

	ADD2	%xmm7, %xmm9
	ADD2	%xmm5, %xmm13
	movaps	%xmm4, %xmm5
	mulps	%xmm0, %xmm4
	movaps	 16 * SIZE(AO), %xmm0
	mulps	%xmm1, %xmm5
	movaps	 20 * SIZE(AO), %xmm1

	ADD1	%xmm6, %xmm10
	ADD1	%xmm3, %xmm14
	PREFETCH  16 * SIZE(PREA)
	movaps	%xmm2, %xmm3
	pshufd	$0xb1, %xmm2, %xmm7
	mulps	%xmm0, %xmm2
	mulps	%xmm1, %xmm3

	ADD2	%xmm4, %xmm11
	ADD2	%xmm5, %xmm15
	movaps	%xmm7, %xmm5
	pshufd	$0x1b, %xmm7, %xmm6
	mulps	%xmm0, %xmm7
	mulps	%xmm1, %xmm5

	ADD1	%xmm2, %xmm8
	movaps	 -4 * SIZE(BO), %xmm2
	ADD1	%xmm3, %xmm12
	movaps	%xmm6, %xmm3
	pshufd	$0xb1, %xmm6, %xmm4
	mulps	%xmm0, %xmm6
	mulps	%xmm1, %xmm3

	ADD2	%xmm7, %xmm9
	ADD2	%xmm5, %xmm13
	movaps	%xmm4, %xmm5
	mulps	%xmm0, %xmm4
	movaps	 24 * SIZE(AO), %xmm0
	mulps	%xmm1, %xmm5
	movaps	 28 * SIZE(AO), %xmm1

	ADD1	%xmm6, %xmm10
	ADD1	%xmm3, %xmm14
	movaps	%xmm2, %xmm3
	pshufd	$0xb1, %xmm2, %xmm7
	mulps	%xmm0, %xmm2
	mulps	%xmm1, %xmm3

	ADD2	%xmm4, %xmm11
	ADD2	%xmm5, %xmm15
	movaps	%xmm7, %xmm5
	pshufd	$0x1b, %xmm7, %xmm6
	mulps	%xmm0, %xmm7
	mulps	%xmm1, %xmm5

	ADD1	%xmm2, %xmm8
	subq	$-64 * SIZE, AO
	movaps	 0 * SIZE(BO), %xmm2
	ADD1	%xmm3, %xmm12
	movaps	%xmm6, %xmm3
	subq	$-32 * SIZE, BO
	pshufd	$0xb1, %xmm6, %xmm4
	mulps	%xmm0, %xmm6
	mulps	%xmm1, %xmm3

	ADD2	%xmm7, %xmm9
	ADD2	%xmm5, %xmm13
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
	ADD1	%xmm6, %xmm10
	ADD1	%xmm3, %xmm14
	movaps	%xmm2, %xmm3
	pshufd	$0xb1, %xmm2, %xmm7
	mulps	%xmm0, %xmm2
	mulps	%xmm1, %xmm3

	ADD2	%xmm4, %xmm11
	ADD2	%xmm5, %xmm15
	movaps	%xmm7, %xmm5
	pshufd	$0x1b, %xmm7, %xmm6
	mulps	%xmm0, %xmm7
	mulps	%xmm1, %xmm5

	ADD1	%xmm2, %xmm8
	movaps	-28 * SIZE(BO), %xmm2
	ADD1	%xmm3, %xmm12
	movaps	%xmm6, %xmm3
	pshufd	$0xb1, %xmm6, %xmm4
	mulps	%xmm0, %xmm6
	mulps	%xmm1, %xmm3

	ADD2	%xmm7, %xmm9
	ADD2	%xmm5, %xmm13
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
	ADD1	%xmm6, %xmm10
	ADD1	%xmm3, %xmm14
	ADD2	%xmm4, %xmm11
	ADD2	%xmm5, %xmm15

	pcmpeqb	%xmm0, %xmm0
	psllq	$63,   %xmm0
	movddup	ALPHA_R, %xmm2
	movddup	ALPHA_I, %xmm3

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
	movsd	0 * SIZE(CO1), %xmm0
	movhps	2 * SIZE(CO1), %xmm0
	movsd	4 * SIZE(CO1), %xmm1
	movhps	6 * SIZE(CO1), %xmm1
	movsd	0 * SIZE(CO2), %xmm2
	movhps	2 * SIZE(CO2), %xmm2
	movsd	4 * SIZE(CO2), %xmm3
	movhps	6 * SIZE(CO2), %xmm3

	addps	%xmm0, %xmm8
	addps	%xmm1, %xmm12
	addps	%xmm2, %xmm10
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
	salq	$ZBASE_SHIFT, %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 2), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$4, KK
#endif

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

	movaps	-32 * SIZE(AO), %xmm0
	pxor	%xmm4, %xmm4
	pxor	%xmm6, %xmm6
	movaps	-32 * SIZE(BO), %xmm2
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
	NOBRANCH
	jle	.L25
	ALIGN_3

.L22:
	ADD1	%xmm6, %xmm10
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)
	pshufd	$0xb1, %xmm2, %xmm7
	mulps	%xmm0, %xmm2
	ADD2	%xmm4, %xmm11
	pshufd	$0x1b, %xmm7, %xmm6
	mulps	%xmm0, %xmm7

	ADD1	%xmm2, %xmm8
	movaps	-28 * SIZE(BO), %xmm2
	pshufd	$0xb1, %xmm6, %xmm4
	mulps	%xmm0, %xmm6
	ADD2	%xmm7, %xmm9
	mulps	%xmm0, %xmm4
	movaps	-28 * SIZE(AO), %xmm0

	ADD1	%xmm6, %xmm10
	pshufd	$0xb1, %xmm2, %xmm7
	mulps	%xmm0, %xmm2
	ADD2	%xmm4, %xmm11
	pshufd	$0x1b, %xmm7, %xmm6
	mulps	%xmm0, %xmm7

	ADD1	%xmm2, %xmm8
	movaps	-24 * SIZE(BO), %xmm2
	pshufd	$0xb1, %xmm6, %xmm4
	mulps	%xmm0, %xmm6
	ADD2	%xmm7, %xmm9
	mulps	%xmm0, %xmm4
	movaps	-24 * SIZE(AO), %xmm0

	ADD1	%xmm6, %xmm10
	pshufd	$0xb1, %xmm2, %xmm7
	mulps	%xmm0, %xmm2
	ADD2	%xmm4, %xmm11
	pshufd	$0x1b, %xmm7, %xmm6
	mulps	%xmm0, %xmm7

	ADD1	%xmm2, %xmm8
	movaps	-20 * SIZE(BO), %xmm2
	pshufd	$0xb1, %xmm6, %xmm4
	mulps	%xmm0, %xmm6
	ADD2	%xmm7, %xmm9
	mulps	%xmm0, %xmm4
	movaps	-20 * SIZE(AO), %xmm0

	ADD1	%xmm6, %xmm10
	pshufd	$0xb1, %xmm2, %xmm7
	mulps	%xmm0, %xmm2
	ADD2	%xmm4, %xmm11
	pshufd	$0x1b, %xmm7, %xmm6
	mulps	%xmm0, %xmm7

	ADD1	%xmm2, %xmm8
	movaps	-16 * SIZE(BO), %xmm2
	pshufd	$0xb1, %xmm6, %xmm4
	mulps	%xmm0, %xmm6
	subq	$-16 * SIZE, AO
	ADD2	%xmm7, %xmm9
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
	andq	$3, %rax		# if (k & 1)
	BRANCH
	je	.L28
	ALIGN_3

.L26:
	ADD1	%xmm6, %xmm10
	pshufd	$0xb1, %xmm2, %xmm7
	mulps	%xmm0, %xmm2
	ADD2	%xmm4, %xmm11
	pshufd	$0x1b, %xmm7, %xmm6
	mulps	%xmm0, %xmm7

	ADD1	%xmm2, %xmm8
	movaps	-28 * SIZE(BO), %xmm2
	pshufd	$0xb1, %xmm6, %xmm4
	mulps	%xmm0, %xmm6
	ADD2	%xmm7, %xmm9
	mulps	%xmm0, %xmm4
	movaps	-28 * SIZE(AO), %xmm0

	addq	$4 * SIZE, AO
	addq	$4 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L26
	ALIGN_3

.L28:
	ADD1	%xmm6, %xmm10
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
	movsd	0 * SIZE(CO2), %xmm2
	movhps	2 * SIZE(CO2), %xmm2

	addps	%xmm0, %xmm8
	addps	%xmm2, %xmm10
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
	salq	$ZBASE_SHIFT, %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 2), BO
#endif

	movsd	-32 * SIZE(AO), %xmm0
	pxor	%xmm4, %xmm4
	pxor	%xmm6, %xmm6
	movaps	-32 * SIZE(BO), %xmm2
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
	NOBRANCH
	jle	.L35
	ALIGN_3

.L32:
	ADD1	%xmm6, %xmm10
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)
	pshufd	$0xb1, %xmm2, %xmm7
	mulps	%xmm0, %xmm2
	ADD2	%xmm4, %xmm11
	pshufd	$0x1b, %xmm7, %xmm6
	mulps	%xmm0, %xmm7

	ADD1	%xmm2, %xmm8
	movaps	-28 * SIZE(BO), %xmm2
	pshufd	$0xb1, %xmm6, %xmm4
	mulps	%xmm0, %xmm6
	ADD2	%xmm7, %xmm9
	mulps	%xmm0, %xmm4
	movsd	-30 * SIZE(AO), %xmm0

	ADD1	%xmm6, %xmm10
	pshufd	$0xb1, %xmm2, %xmm7
	mulps	%xmm0, %xmm2
	ADD2	%xmm4, %xmm11
	pshufd	$0x1b, %xmm7, %xmm6
	mulps	%xmm0, %xmm7

	ADD1	%xmm2, %xmm8
	movaps	-24 * SIZE(BO), %xmm2
	pshufd	$0xb1, %xmm6, %xmm4
	mulps	%xmm0, %xmm6
	ADD2	%xmm7, %xmm9
	mulps	%xmm0, %xmm4
	movsd	-28 * SIZE(AO), %xmm0

	ADD1	%xmm6, %xmm10
	pshufd	$0xb1, %xmm2, %xmm7
	mulps	%xmm0, %xmm2
	ADD2	%xmm4, %xmm11
	pshufd	$0x1b, %xmm7, %xmm6
	mulps	%xmm0, %xmm7

	ADD1	%xmm2, %xmm8
	movaps	-20 * SIZE(BO), %xmm2
	pshufd	$0xb1, %xmm6, %xmm4
	mulps	%xmm0, %xmm6
	ADD2	%xmm7, %xmm9
	mulps	%xmm0, %xmm4
	movsd	-26 * SIZE(AO), %xmm0

	ADD1	%xmm6, %xmm10
	pshufd	$0xb1, %xmm2, %xmm7
	mulps	%xmm0, %xmm2
	ADD2	%xmm4, %xmm11
	pshufd	$0x1b, %xmm7, %xmm6
	mulps	%xmm0, %xmm7

	ADD1	%xmm2, %xmm8
	movaps	-16 * SIZE(BO), %xmm2
	pshufd	$0xb1, %xmm6, %xmm4
	mulps	%xmm0, %xmm6
	subq	$-8 * SIZE, AO
	ADD2	%xmm7, %xmm9
	mulps	%xmm0, %xmm4
	movsd	-32 * SIZE(AO), %xmm0

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
	andq	$3, %rax		# if (k & 1)
	BRANCH
	je	.L38
	ALIGN_3

.L36:
	ADD1	%xmm6, %xmm10
	pshufd	$0xb1, %xmm2, %xmm7
	mulps	%xmm0, %xmm2
	ADD2	%xmm4, %xmm11
	pshufd	$0x1b, %xmm7, %xmm6
	mulps	%xmm0, %xmm7

	ADD1	%xmm2, %xmm8
	movaps	-28 * SIZE(BO), %xmm2
	pshufd	$0xb1, %xmm6, %xmm4
	mulps	%xmm0, %xmm6
	ADD2	%xmm7, %xmm9
	mulps	%xmm0, %xmm4
	movsd	-30 * SIZE(AO), %xmm0

	addq	$2 * SIZE, AO
	addq	$4 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L36
	ALIGN_3

.L38:
	ADD1	%xmm6, %xmm10
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
	movsd	0 * SIZE(CO2), %xmm2

	addps	%xmm0, %xmm8
	addps	%xmm2, %xmm10
#endif

	movsd	%xmm8,  0 * SIZE(CO1)
	movsd	%xmm10, 0 * SIZE(CO2)

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

.L39:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addq	$2, KK
#endif

	leaq	(C, LDC, 2), C
	movq	BO, B

	subq	$1, J
	BRANCH
	jg	.L01
	ALIGN_4

.L40:
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
	salq	$ZBASE_SHIFT, %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 1), BO
#endif

	movaps	-32 * SIZE(AO), %xmm0
	pxor	%xmm3, %xmm3
	movaps	-28 * SIZE(AO), %xmm1
	pxor	%xmm4, %xmm4
	movaps	-32 * SIZE(BO), %xmm2
	pxor	%xmm5, %xmm5
	pxor	%xmm6, %xmm6

	prefetcht0     7 * SIZE(CO1)
	pxor	%xmm8,  %xmm8
	pxor	%xmm9,  %xmm9
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
	NOBRANCH
	jle	.L45
	ALIGN_3

.L42:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)

	addps	%xmm3, %xmm8
	pshufd	$0x00, %xmm2, %xmm3
	mulps	%xmm0, %xmm3
	addps	%xmm4, %xmm12
	pshufd	$0x00, %xmm2, %xmm4
	mulps	%xmm1, %xmm4

	addps	%xmm5, %xmm9
	pshufd	$0x55, %xmm2, %xmm5
	mulps	%xmm0, %xmm5
	movaps	-24 * SIZE(AO), %xmm0
	addps	%xmm6, %xmm13
	pshufd	$0x55, %xmm2, %xmm6
	mulps	%xmm1, %xmm6
	movaps	-20 * SIZE(AO), %xmm1

	addps	%xmm3, %xmm8
	pshufd	$0xaa, %xmm2, %xmm3
	mulps	%xmm0, %xmm3
	addps	%xmm4, %xmm12
	pshufd	$0xaa, %xmm2, %xmm4
	mulps	%xmm1, %xmm4

	addps	%xmm5, %xmm9
	pshufd	$0xff, %xmm2, %xmm5
	mulps	%xmm0, %xmm5
	movaps	-16 * SIZE(AO), %xmm0
	addps	%xmm6, %xmm13
	pshufd	$0xff, %xmm2, %xmm6
	movaps	-28 * SIZE(BO), %xmm2
	mulps	%xmm1, %xmm6
	movaps	-12 * SIZE(AO), %xmm1

	PREFETCH (PREFETCHSIZE + 16) * SIZE(AO)

	addps	%xmm3, %xmm8
	pshufd	$0x00, %xmm2, %xmm3
	mulps	%xmm0, %xmm3
	addps	%xmm4, %xmm12
	pshufd	$0x00, %xmm2, %xmm4
	mulps	%xmm1, %xmm4

	addps	%xmm5, %xmm9
	pshufd	$0x55, %xmm2, %xmm5
	mulps	%xmm0, %xmm5
	movaps	 -8 * SIZE(AO), %xmm0
	addps	%xmm6, %xmm13
	pshufd	$0x55, %xmm2, %xmm6
	mulps	%xmm1, %xmm6
	movaps	 -4 * SIZE(AO), %xmm1

	addps	%xmm3, %xmm8
	pshufd	$0xaa, %xmm2, %xmm3
	mulps	%xmm0, %xmm3
	addps	%xmm4, %xmm12
	pshufd	$0xaa, %xmm2, %xmm4
	mulps	%xmm1, %xmm4

	addps	%xmm5, %xmm9
	pshufd	$0xff, %xmm2, %xmm5
	mulps	%xmm0, %xmm5
	movaps	  0 * SIZE(AO), %xmm0
	addps	%xmm6, %xmm13
	pshufd	$0xff, %xmm2, %xmm6
	movaps	-24 * SIZE(BO), %xmm2
	mulps	%xmm1, %xmm6
	movaps	  4 * SIZE(AO), %xmm1

	subq	$-32 * SIZE, AO
	subq	$ -8 * SIZE, BO

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
	addps	%xmm3, %xmm8
	pshufd	$0x00, %xmm2, %xmm3
	mulps	%xmm0, %xmm3
	addps	%xmm4, %xmm12
	pshufd	$0x00, %xmm2, %xmm4
	mulps	%xmm1, %xmm4

	addps	%xmm5, %xmm9
	pshufd	$0x55, %xmm2, %xmm5
	mulps	%xmm0, %xmm5
	movaps	-24 * SIZE(AO), %xmm0
	addps	%xmm6, %xmm13
	pshufd	$0x55, %xmm2, %xmm6
	movsd	-30 * SIZE(BO), %xmm2
	mulps	%xmm1, %xmm6
	movaps	-20 * SIZE(AO), %xmm1

	addq	$8 * SIZE, AO
	addq	$2 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L46
	ALIGN_3

.L48:
	addps	%xmm3, %xmm8
	addps	%xmm4, %xmm12
	addps	%xmm5, %xmm9
	addps	%xmm6, %xmm13

	pshufd	 $0xb1, %xmm9,  %xmm9
	movddup	ALPHA_R, %xmm2
	pshufd	 $0xb1, %xmm13, %xmm13
	movddup	ALPHA_I, %xmm3

	pcmpeqb	%xmm0, %xmm0
	psllq	$63,   %xmm0

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
	pxor	%xmm0, %xmm9
	pxor	%xmm0, %xmm13

	subps	%xmm9,  %xmm8
	subps	%xmm13, %xmm12
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
	pxor	%xmm0, %xmm9
	pxor	%xmm0, %xmm13

	addps	%xmm9,  %xmm8
	addps	%xmm13, %xmm12
#elif defined(RN) || defined(RT) || defined(CN) || defined(CT)
	pxor	%xmm0, %xmm8
	pxor	%xmm0, %xmm12

	addps	%xmm9,  %xmm8
	addps	%xmm13, %xmm12
#else
	pxor	%xmm0, %xmm8
	pxor	%xmm0, %xmm12

	subps	%xmm9,  %xmm8
	subps	%xmm13, %xmm12
#endif

	pshufd	$0xb1, %xmm8,  %xmm9
	pshufd	$0xb1, %xmm12, %xmm13

	mulps	%xmm2, %xmm8
	mulps	%xmm3, %xmm9
	mulps	%xmm2, %xmm12
	mulps	%xmm3, %xmm13

	addsubps       %xmm9,  %xmm8
	addsubps       %xmm13, %xmm12

#ifndef TRMMKERNEL
	movsd	0 * SIZE(CO1), %xmm0
	movhps	2 * SIZE(CO1), %xmm0
	movsd	4 * SIZE(CO1), %xmm1
	movhps	6 * SIZE(CO1), %xmm1

	addps	%xmm0, %xmm8
	addps	%xmm1, %xmm12
#endif

	movsd	%xmm8,  0 * SIZE(CO1)
	movhps	%xmm8,  2 * SIZE(CO1)
	movsd	%xmm12, 4 * SIZE(CO1)
	movhps	%xmm12, 6 * SIZE(CO1)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	salq	$ZBASE_SHIFT, %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 1), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$4, KK
#endif

	addq	$8 * SIZE, CO1		# coffset += 4
	decq	I			# i --
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
	salq	$ZBASE_SHIFT, %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 1), BO
#endif

	movaps	-32 * SIZE(AO), %xmm0
	pxor	%xmm3, %xmm3
	pxor	%xmm4, %xmm4
	movaps	-32 * SIZE(BO), %xmm2
	pxor	%xmm8, %xmm8
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
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)

	addps	%xmm3, %xmm8
	pshufd	$0x00, %xmm2, %xmm3
	mulps	%xmm0, %xmm3
	addps	%xmm4, %xmm9
	pshufd	$0x55, %xmm2, %xmm4
	mulps	%xmm0, %xmm4
	movaps	-28 * SIZE(AO), %xmm0

	addps	%xmm3, %xmm8
	pshufd	$0xaa, %xmm2, %xmm3
	mulps	%xmm0, %xmm3
	addps	%xmm4, %xmm9
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

	addps	%xmm3, %xmm8
	pshufd	$0xaa, %xmm2, %xmm3
	mulps	%xmm0, %xmm3
	addps	%xmm4, %xmm9
	pshufd	$0xff, %xmm2, %xmm4
	movaps	-24 * SIZE(BO), %xmm2
	mulps	%xmm0, %xmm4
	movaps	-16 * SIZE(AO), %xmm0

	subq	$-16 * SIZE, AO
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
	movsd	-30 * SIZE(BO), %xmm2
	mulps	%xmm0, %xmm4
	movaps	-28 * SIZE(AO), %xmm0

	addq	$4 * SIZE, AO
	addq	$2 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L56
	ALIGN_3

.L58:
	addps	%xmm3, %xmm8
	movddup	ALPHA_R, %xmm2
	addps	%xmm4, %xmm9
	movddup	ALPHA_I, %xmm3

	pshufd	 $0xb1, %xmm9,  %xmm9
	pcmpeqb	%xmm0, %xmm0
	psllq	$63,   %xmm0

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
	pxor	%xmm0, %xmm9

	subps	%xmm9,  %xmm8
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
	pxor	%xmm0, %xmm9

	addps	%xmm9,  %xmm8
#elif defined(RN) || defined(RT) || defined(CN) || defined(CT)
	pxor	%xmm0, %xmm8

	addps	%xmm9,  %xmm8
#else
	pxor	%xmm0, %xmm8

	subps	%xmm9,  %xmm8
#endif

	pshufd	$0xb1, %xmm8,  %xmm9

	mulps	%xmm2, %xmm8
	mulps	%xmm3, %xmm9

	addsubps       %xmm9,  %xmm8

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

	movsd	-32 * SIZE(AO), %xmm0
	pxor	%xmm3, %xmm3
	pxor	%xmm4, %xmm4
	movaps	-32 * SIZE(BO), %xmm2
	pxor	%xmm8, %xmm8
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
	addq	$1, %rax
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
	movsd	-30 * SIZE(AO), %xmm0

	addps	%xmm3, %xmm8
	pshufd	$0xaa, %xmm2, %xmm3
	mulps	%xmm0, %xmm3
	addps	%xmm4, %xmm9
	pshufd	$0xff, %xmm2, %xmm4
	movaps	-28 * SIZE(BO), %xmm2
	mulps	%xmm0, %xmm4
	movsd	-28 * SIZE(AO), %xmm0

	addps	%xmm3, %xmm8
	pshufd	$0x00, %xmm2, %xmm3
	mulps	%xmm0, %xmm3
	addps	%xmm4, %xmm9
	pshufd	$0x55, %xmm2, %xmm4
	mulps	%xmm0, %xmm4
	movsd	-26 * SIZE(AO), %xmm0

	addps	%xmm3, %xmm8
	pshufd	$0xaa, %xmm2, %xmm3
	mulps	%xmm0, %xmm3
	addps	%xmm4, %xmm9
	pshufd	$0xff, %xmm2, %xmm4
	movaps	-24 * SIZE(BO), %xmm2
	mulps	%xmm0, %xmm4
	movsd	-24 * SIZE(AO), %xmm0

	subq	$-8 * SIZE, AO
	subq	$-8 * SIZE, BO

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
	addps	%xmm3, %xmm8
	pshufd	$0x00, %xmm2, %xmm3
	mulps	%xmm0, %xmm3
	addps	%xmm4, %xmm9
	pshufd	$0x55, %xmm2, %xmm4
	movsd	-30 * SIZE(BO), %xmm2
	mulps	%xmm0, %xmm4
	movsd	-30 * SIZE(AO), %xmm0

	addq	$2 * SIZE, AO
	addq	$2 * SIZE, BO

	subq	$1, %rax
	BRANCH
	jg	.L66
	ALIGN_3

.L68:
	addps	%xmm3, %xmm8
	movddup	ALPHA_R, %xmm2
	addps	%xmm4, %xmm9
	movddup	ALPHA_I, %xmm3

	pshufd	 $0xb1, %xmm9,  %xmm9
	pcmpeqb	%xmm0, %xmm0
	psllq	$63,   %xmm0

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
	pxor	%xmm0, %xmm9

	subps	%xmm9,  %xmm8
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
	pxor	%xmm0, %xmm9

	addps	%xmm9,  %xmm8
#elif defined(RN) || defined(RT) || defined(CN) || defined(CT)
	pxor	%xmm0, %xmm8

	addps	%xmm9,  %xmm8
#else
	pxor	%xmm0, %xmm8

	subps	%xmm9,  %xmm8
#endif

	pshufd	$0xb1, %xmm8,  %xmm9
	mulps	%xmm2, %xmm8
	mulps	%xmm3, %xmm9
	addsubps       %xmm9,  %xmm8

#ifndef TRMMKERNEL
	movsd	0 * SIZE(CO1), %xmm0
	addps	%xmm0, %xmm8
#endif
	movsd	%xmm8,  0 * SIZE(CO1)
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
