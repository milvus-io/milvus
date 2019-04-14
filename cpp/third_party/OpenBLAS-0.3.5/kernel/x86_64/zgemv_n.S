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
#include "l2param.h"

#ifndef WINDOWS_ABI

#define STACKSIZE	128

#define OLD_INCX	 8 + STACKSIZE(%rsp)
#define OLD_Y		16 + STACKSIZE(%rsp)
#define OLD_INCY	24 + STACKSIZE(%rsp)
#define OLD_BUFFER	32 + STACKSIZE(%rsp)
#define ALPHA_R		48	      (%rsp)
#define ALPHA_I		56	      (%rsp)

#define MMM	64(%rsp)
#define NN	72(%rsp)
#define AA	80(%rsp)
#define XX	88(%rsp)
#define LDAX	96(%rsp)
#define ALPHAR	104(%rsp)
#define	ALPHAI	112(%rsp)

#define M	  %rdi
#define N	  %rsi
#define A	  %rcx
#define LDA	  %r8
#define X	  %r9
#define INCX	  %rdx
#define Y	  %rbp
#define INCY	  %r10

#else

#define STACKSIZE	304

#define OLD_ALPHA_I	 40 + STACKSIZE(%rsp)
#define OLD_A		 48 + STACKSIZE(%rsp)
#define OLD_LDA		 56 + STACKSIZE(%rsp)
#define OLD_X		 64 + STACKSIZE(%rsp)
#define OLD_INCX	 72 + STACKSIZE(%rsp)
#define OLD_Y		 80 + STACKSIZE(%rsp)
#define OLD_INCY	 88 + STACKSIZE(%rsp)
#define OLD_BUFFER	 96 + STACKSIZE(%rsp)
#define ALPHA_R		224	       (%rsp)
#define ALPHA_I		232	       (%rsp)

#define MMM	240(%rsp)
#define NN	248(%rsp)
#define AA	256(%rsp)
#define	XX	264(%rsp)
#define LDAX	272(%rsp)
#define ALPHAR	280(%rsp)
#define ALPHAI	288(%rsp)

#define M	  %rcx
#define N	  %rdx
#define A	  %r8
#define LDA	  %r9
#define X	  %rdi
#define INCX	  %rsi
#define Y	  %rbp
#define INCY	  %r10

#endif

#define I	%rax
#define A1	%r12
#define A2	%r13

#define Y1	%r14
#define BUFFER	%r15

#define J	%r11

#undef SUBPD

#if (!defined(CONJ) && !defined(XCONJ)) || (defined(CONJ) && defined(XCONJ))
#define SUBPD	   subpd
#else
#define SUBPD	   addpd
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

	movq	OLD_A,     A
	movq	OLD_LDA,   LDA
	movq	OLD_X,     X

	movapd	%xmm3,       %xmm0
	movsd	OLD_ALPHA_I, %xmm1
#endif

	movq	A, AA
	movq	N, NN
	movq	M, MMM
	movq	LDA, LDAX
	movq	X, XX
	movq	OLD_Y,     Y
	movsd	%xmm0,ALPHAR
	movsd	%xmm1,ALPHAI

.L0t:
	xorq	I,I
	addq	$1,I
	salq	$18,I
	subq	I,MMM
	movq	I,M
	movsd	ALPHAR,%xmm0
	movsd	ALPHAI,%xmm1
	jge	.L00t

	movq	MMM,M
	addq	I,M
	jle	.L999x

.L00t:
	movq	AA, A
	movq	NN, N
	movq	LDAX, LDA
	movq	XX, X

	movq	OLD_INCX,  INCX
#	movq	OLD_Y,     Y
	movq	OLD_INCY,  INCY
	movq	OLD_BUFFER, BUFFER

	salq	$ZBASE_SHIFT,   LDA
	salq	$ZBASE_SHIFT,   INCX
	salq	$ZBASE_SHIFT,   INCY

	movlpd	%xmm0, ALPHA_R
	movlpd	%xmm1, ALPHA_I

	subq	$-16 * SIZE, A

	testq	M, M
	jle	.L999
	testq	N, N
	jle	.L999
	ALIGN_3

	movq	BUFFER, Y1

	pxor	%xmm4, %xmm4

	movq	M,   %rax
	addq	$8,  %rax
	sarq	$3,  %rax
	ALIGN_3

.L01:
	movapd	%xmm4,  0 * SIZE(Y1)
	movapd	%xmm4,  2 * SIZE(Y1)
	movapd	%xmm4,  4 * SIZE(Y1)
	movapd	%xmm4,  6 * SIZE(Y1)
	movapd	%xmm4,  8 * SIZE(Y1)
	movapd	%xmm4, 10 * SIZE(Y1)
	movapd	%xmm4, 12 * SIZE(Y1)
	movapd	%xmm4, 14 * SIZE(Y1)

	subq	$-16 * SIZE, Y1
	decq	%rax
	jg	.L01
	ALIGN_3

.L10:
#ifdef ALIGNED_ACCESS
	testq	$SIZE, A
	jne	.L100
#endif

#if GEMV_UNROLL >= 4

	cmpq	$4, N
	jl	.L20
	ALIGN_3

.L11:
	subq	$4, N

	leaq	16 * SIZE(BUFFER), Y1
	movq	A,  A1
	leaq	(A,  LDA, 2), A2
	leaq	(A,  LDA, 4), A

	movsd	0 * SIZE(X), %xmm8
	movhpd	1 * SIZE(X), %xmm8
	addq	INCX, X
	movsd	0 * SIZE(X), %xmm10
	movhpd	1 * SIZE(X), %xmm10
	addq	INCX, X
	movsd	0 * SIZE(X), %xmm12
	movhpd	1 * SIZE(X), %xmm12
	addq	INCX, X
	movsd	0 * SIZE(X), %xmm14
	movhpd	1 * SIZE(X), %xmm14
	addq	INCX, X

	pcmpeqb	%xmm5, %xmm5
	psllq	$63,   %xmm5
	shufps	$0xc0, %xmm5, %xmm5

	pshufd	$0x4e, %xmm8,  %xmm9
	pshufd	$0x4e, %xmm10, %xmm11
	pshufd	$0x4e, %xmm12, %xmm13
	pshufd	$0x4e, %xmm14, %xmm15

#ifdef HAVE_SSE3
	movddup	ALPHA_R, %xmm6
	movddup	ALPHA_I, %xmm7
#else
	movsd	ALPHA_R, %xmm6
	unpcklpd %xmm6, %xmm6
	movsd	ALPHA_I, %xmm7
	unpcklpd %xmm7, %xmm7
#endif

	xorpd	 %xmm5, %xmm9
	xorpd	 %xmm5, %xmm11
	xorpd	 %xmm5, %xmm13
	xorpd	 %xmm5, %xmm15

	mulpd	 %xmm6, %xmm8
	mulpd	 %xmm7, %xmm9
	mulpd	 %xmm6, %xmm10
	mulpd	 %xmm7, %xmm11

	mulpd	 %xmm6, %xmm12
	mulpd	 %xmm7, %xmm13
	mulpd	 %xmm6, %xmm14
	mulpd	 %xmm7, %xmm15

#ifndef XCONJ
	subpd	 %xmm9,  %xmm8
	subpd	 %xmm11, %xmm10
	subpd	 %xmm13, %xmm12
	subpd	 %xmm15, %xmm14
#else
	addpd	 %xmm9,  %xmm8
	addpd	 %xmm11, %xmm10
	addpd	 %xmm13, %xmm12
	addpd	 %xmm15, %xmm14
#endif

	pshufd	 $0xee, %xmm8,  %xmm9
	pshufd	 $0x44, %xmm8,  %xmm8

	pshufd	 $0xee, %xmm10, %xmm11
	pshufd	 $0x44, %xmm10, %xmm10

	pshufd	 $0xee, %xmm12, %xmm13
	pshufd	 $0x44, %xmm12, %xmm12

	pshufd	 $0xee, %xmm14, %xmm15
	pshufd	 $0x44, %xmm14, %xmm14

#ifndef CONJ
	xorpd	 %xmm5, %xmm9
	xorpd	 %xmm5, %xmm11
	xorpd	 %xmm5, %xmm13
	xorpd	 %xmm5, %xmm15
#else
	xorpd	 %xmm5, %xmm8
	xorpd	 %xmm5, %xmm10
	xorpd	 %xmm5, %xmm12
	xorpd	 %xmm5, %xmm14
#endif

	MOVUPS_YL1(-16 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-14 * SIZE, Y1, %xmm1)
	MOVUPS_YL1(-12 * SIZE, Y1, %xmm2)
	MOVUPS_YL1(-10 * SIZE, Y1, %xmm3)
	ALIGN_3

	movq	M,   I
	sarq	$2,  I
	jle	.L15

	MOVUPS_A1(-16 * SIZE, A1, %xmm4)
	MOVUPS_A1(-14 * SIZE, A1, %xmm6)

	decq	 I
	jle	 .L14
	ALIGN_3

.L13:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A1)
#endif

	pshufd	 $0x4e, %xmm4, %xmm5
	mulpd	 %xmm8,  %xmm4
	addpd	 %xmm4,  %xmm0
	MOVUPS_A1(-12 * SIZE, A1, %xmm4)
	pshufd	 $0x4e, %xmm6, %xmm7
	mulpd	 %xmm8,  %xmm6
	addpd	 %xmm6,  %xmm1
	MOVUPS_A1(-10 * SIZE, A1, %xmm6)

	mulpd	 %xmm9,  %xmm5
	SUBPD	 %xmm5,  %xmm0
	mulpd	 %xmm9,  %xmm7
	SUBPD	 %xmm7,  %xmm1

	pshufd	 $0x4e, %xmm4, %xmm5
	mulpd	 %xmm8,  %xmm4
	addpd	 %xmm4,  %xmm2
	MOVUPS_A2(-16 * SIZE, A1, LDA, 1, %xmm4)
	pshufd	 $0x4e, %xmm6, %xmm7
	mulpd	 %xmm8,  %xmm6
	addpd	 %xmm6,  %xmm3
	MOVUPS_A2(-14 * SIZE, A1, LDA, 1, %xmm6)

	mulpd	 %xmm9,  %xmm5
	SUBPD	 %xmm5,  %xmm2
	mulpd	 %xmm9,  %xmm7
	SUBPD	 %xmm7,  %xmm3

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A1, LDA)
#endif

	pshufd	 $0x4e, %xmm4, %xmm5
	mulpd	 %xmm10, %xmm4
	addpd	 %xmm4,  %xmm0
	MOVUPS_A2(-12 * SIZE, A1, LDA, 1, %xmm4)
	pshufd	 $0x4e, %xmm6, %xmm7
	mulpd	 %xmm10, %xmm6
	addpd	 %xmm6,  %xmm1
	MOVUPS_A2(-10 * SIZE, A1, LDA, 1, %xmm6)

	mulpd	 %xmm11, %xmm5
	SUBPD	 %xmm5,  %xmm0
	mulpd	 %xmm11, %xmm7
	SUBPD	 %xmm7,  %xmm1

	pshufd	 $0x4e, %xmm4, %xmm5
	mulpd	 %xmm10, %xmm4
	addpd	 %xmm4,  %xmm2
	MOVUPS_A1(-16 * SIZE, A2, %xmm4)
	pshufd	 $0x4e, %xmm6, %xmm7
	mulpd	 %xmm10, %xmm6
	addpd	 %xmm6,  %xmm3
	MOVUPS_A1(-14 * SIZE, A2, %xmm6)

	mulpd	 %xmm11, %xmm5
	SUBPD	 %xmm5,  %xmm2
	mulpd	 %xmm11, %xmm7
	SUBPD	 %xmm7,  %xmm3

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A2)
#endif

	pshufd	 $0x4e, %xmm4, %xmm5
	mulpd	 %xmm12, %xmm4
	addpd	 %xmm4,  %xmm0
	MOVUPS_A1(-12 * SIZE, A2, %xmm4)
	pshufd	 $0x4e, %xmm6, %xmm7
	mulpd	 %xmm12, %xmm6
	addpd	 %xmm6,  %xmm1
	MOVUPS_A1(-10 * SIZE, A2, %xmm6)

	mulpd	 %xmm13, %xmm5
	SUBPD	 %xmm5,  %xmm0
	mulpd	 %xmm13, %xmm7
	SUBPD	 %xmm7,  %xmm1

	pshufd	 $0x4e, %xmm4, %xmm5
	mulpd	 %xmm12, %xmm4
	addpd	 %xmm4,  %xmm2
	MOVUPS_A2(-16 * SIZE, A2, LDA, 1, %xmm4)
	pshufd	 $0x4e, %xmm6, %xmm7
	mulpd	 %xmm12, %xmm6
	addpd	 %xmm6,  %xmm3
	MOVUPS_A2(-14 * SIZE, A2, LDA, 1, %xmm6)

	mulpd	 %xmm13, %xmm5
	SUBPD	 %xmm5,  %xmm2
	mulpd	 %xmm13, %xmm7
	SUBPD	 %xmm7,  %xmm3

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A2, LDA)
#endif

	pshufd	 $0x4e, %xmm4, %xmm5
	mulpd	 %xmm14, %xmm4
	addpd	 %xmm4,  %xmm0
	MOVUPS_A2(-12 * SIZE, A2, LDA, 1, %xmm4)
	pshufd	 $0x4e, %xmm6, %xmm7
	mulpd	 %xmm14, %xmm6
	addpd	 %xmm6,  %xmm1
	MOVUPS_A2(-10 * SIZE, A2, LDA, 1, %xmm6)

	mulpd	 %xmm15, %xmm5
	SUBPD	 %xmm5,  %xmm0
	mulpd	 %xmm15, %xmm7
	SUBPD	 %xmm7,  %xmm1

	pshufd	 $0x4e, %xmm4, %xmm5
	mulpd	 %xmm14, %xmm4
	addpd	 %xmm4,  %xmm2
	MOVUPS_A1( -8 * SIZE, A1, %xmm4)
	pshufd	 $0x4e, %xmm6, %xmm7
	mulpd	 %xmm14, %xmm6
	addpd	 %xmm6,  %xmm3
	MOVUPS_A1( -6 * SIZE, A1, %xmm6)

	mulpd	 %xmm15, %xmm5
	SUBPD	 %xmm5,  %xmm2
	mulpd	 %xmm15, %xmm7
	SUBPD	 %xmm7,  %xmm3

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE) - 128 + PREOFFSET(Y1)
#endif

	MOVUPS_YS1(-16 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-14 * SIZE, Y1, %xmm1)
	MOVUPS_YS1(-12 * SIZE, Y1, %xmm2)
	MOVUPS_YS1(-10 * SIZE, Y1, %xmm3)

	MOVUPS_YL1( -8 * SIZE, Y1, %xmm0)
	MOVUPS_YL1( -6 * SIZE, Y1, %xmm1)
	MOVUPS_YL1( -4 * SIZE, Y1, %xmm2)
	MOVUPS_YL1( -2 * SIZE, Y1, %xmm3)

	subq	 $-8 * SIZE, A1
	subq	 $-8 * SIZE, A2
	subq	 $-8 * SIZE, Y1

	subq	 $1, I
	BRANCH
	jg	.L13
	ALIGN_3

.L14:
	pshufd	 $0x4e, %xmm4, %xmm5
	mulpd	 %xmm8,  %xmm4
	addpd	 %xmm4,  %xmm0
	MOVUPS_A1(-12 * SIZE, A1, %xmm4)
	pshufd	 $0x4e, %xmm6, %xmm7
	mulpd	 %xmm8,  %xmm6
	addpd	 %xmm6,  %xmm1
	MOVUPS_A1(-10 * SIZE, A1, %xmm6)

	mulpd	 %xmm9,  %xmm5
	SUBPD	 %xmm5,  %xmm0
	mulpd	 %xmm9,  %xmm7
	SUBPD	 %xmm7,  %xmm1

	pshufd	 $0x4e, %xmm4, %xmm5
	mulpd	 %xmm8,  %xmm4
	addpd	 %xmm4,  %xmm2
	MOVUPS_A2(-16 * SIZE, A1, LDA, 1, %xmm4)
	pshufd	 $0x4e, %xmm6, %xmm7
	mulpd	 %xmm8,  %xmm6
	addpd	 %xmm6,  %xmm3
	MOVUPS_A2(-14 * SIZE, A1, LDA, 1, %xmm6)

	mulpd	 %xmm9,  %xmm5
	SUBPD	 %xmm5,  %xmm2
	mulpd	 %xmm9,  %xmm7
	SUBPD	 %xmm7,  %xmm3

	pshufd	 $0x4e, %xmm4, %xmm5
	mulpd	 %xmm10, %xmm4
	addpd	 %xmm4,  %xmm0
	MOVUPS_A2(-12 * SIZE, A1, LDA, 1, %xmm4)
	pshufd	 $0x4e, %xmm6, %xmm7
	mulpd	 %xmm10, %xmm6
	addpd	 %xmm6,  %xmm1
	MOVUPS_A2(-10 * SIZE, A1, LDA, 1, %xmm6)

	mulpd	 %xmm11, %xmm5
	SUBPD	 %xmm5,  %xmm0
	mulpd	 %xmm11, %xmm7
	SUBPD	 %xmm7,  %xmm1

	pshufd	 $0x4e, %xmm4, %xmm5
	mulpd	 %xmm10, %xmm4
	addpd	 %xmm4,  %xmm2
	MOVUPS_A1(-16 * SIZE, A2, %xmm4)
	pshufd	 $0x4e, %xmm6, %xmm7
	mulpd	 %xmm10, %xmm6
	addpd	 %xmm6,  %xmm3
	MOVUPS_A1(-14 * SIZE, A2, %xmm6)

	mulpd	 %xmm11, %xmm5
	SUBPD	 %xmm5,  %xmm2
	mulpd	 %xmm11, %xmm7
	SUBPD	 %xmm7,  %xmm3

	pshufd	 $0x4e, %xmm4, %xmm5
	mulpd	 %xmm12, %xmm4
	addpd	 %xmm4,  %xmm0
	MOVUPS_A1(-12 * SIZE, A2, %xmm4)
	pshufd	 $0x4e, %xmm6, %xmm7
	mulpd	 %xmm12, %xmm6
	addpd	 %xmm6,  %xmm1
	MOVUPS_A1(-10 * SIZE, A2, %xmm6)

	mulpd	 %xmm13, %xmm5
	SUBPD	 %xmm5,  %xmm0
	mulpd	 %xmm13, %xmm7
	SUBPD	 %xmm7,  %xmm1

	pshufd	 $0x4e, %xmm4, %xmm5
	mulpd	 %xmm12, %xmm4
	addpd	 %xmm4,  %xmm2
	MOVUPS_A2(-16 * SIZE, A2, LDA, 1, %xmm4)
	pshufd	 $0x4e, %xmm6, %xmm7
	mulpd	 %xmm12, %xmm6
	addpd	 %xmm6,  %xmm3
	MOVUPS_A2(-14 * SIZE, A2, LDA, 1, %xmm6)

	mulpd	 %xmm13, %xmm5
	SUBPD	 %xmm5,  %xmm2
	mulpd	 %xmm13, %xmm7
	SUBPD	 %xmm7,  %xmm3

	pshufd	 $0x4e, %xmm4, %xmm5
	mulpd	 %xmm14, %xmm4
	addpd	 %xmm4,  %xmm0
	MOVUPS_A2(-12 * SIZE, A2, LDA, 1, %xmm4)
	pshufd	 $0x4e, %xmm6, %xmm7
	mulpd	 %xmm14, %xmm6
	addpd	 %xmm6,  %xmm1
	MOVUPS_A2(-10 * SIZE, A2, LDA, 1, %xmm6)

	mulpd	 %xmm15, %xmm5
	SUBPD	 %xmm5,  %xmm0
	mulpd	 %xmm15, %xmm7
	SUBPD	 %xmm7,  %xmm1

	pshufd	 $0x4e, %xmm4, %xmm5
	mulpd	 %xmm14, %xmm4
	addpd	 %xmm4,  %xmm2
	pshufd	 $0x4e, %xmm6, %xmm7
	mulpd	 %xmm14, %xmm6
	addpd	 %xmm6,  %xmm3

	mulpd	 %xmm15, %xmm5
	SUBPD	 %xmm5,  %xmm2
	mulpd	 %xmm15, %xmm7
	SUBPD	 %xmm7,  %xmm3

	MOVUPS_YS1(-16 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-14 * SIZE, Y1, %xmm1)
	MOVUPS_YS1(-12 * SIZE, Y1, %xmm2)
	MOVUPS_YS1(-10 * SIZE, Y1, %xmm3)

	MOVUPS_YL1( -8 * SIZE, Y1, %xmm0)
	MOVUPS_YL1( -6 * SIZE, Y1, %xmm1)
	MOVUPS_YL1( -4 * SIZE, Y1, %xmm2)
	MOVUPS_YL1( -2 * SIZE, Y1, %xmm3)

	subq	 $-8 * SIZE, A1
	subq	 $-8 * SIZE, A2
	subq	 $-8 * SIZE, Y1
	ALIGN_3

.L15:
	testq	$2, M
	je	.L17

	MOVUPS_A1(-16 * SIZE, A1, %xmm4)
	MOVUPS_A1(-14 * SIZE, A1, %xmm6)

	pshufd	 $0x4e, %xmm4, %xmm5
	mulpd	 %xmm8,  %xmm4
	addpd	 %xmm4,  %xmm0
	MOVUPS_A2(-16 * SIZE, A1, LDA, 1, %xmm4)
	pshufd	 $0x4e, %xmm6, %xmm7
	mulpd	 %xmm8,  %xmm6
	addpd	 %xmm6,  %xmm1
	MOVUPS_A2(-14 * SIZE, A1, LDA, 1, %xmm6)

	mulpd	 %xmm9,  %xmm5
	SUBPD	 %xmm5,  %xmm0
	mulpd	 %xmm9,  %xmm7
	SUBPD	 %xmm7,  %xmm1

	pshufd	 $0x4e, %xmm4, %xmm5
	mulpd	 %xmm10, %xmm4
	addpd	 %xmm4,  %xmm0
	MOVUPS_A1(-16 * SIZE, A2, %xmm4)
	pshufd	 $0x4e, %xmm6, %xmm7
	mulpd	 %xmm10, %xmm6
	addpd	 %xmm6,  %xmm1
	MOVUPS_A1(-14 * SIZE, A2, %xmm6)

	mulpd	 %xmm11, %xmm5
	SUBPD	 %xmm5,  %xmm0
	mulpd	 %xmm11, %xmm7
	SUBPD	 %xmm7,  %xmm1

	pshufd	 $0x4e, %xmm4, %xmm5
	mulpd	 %xmm12, %xmm4
	addpd	 %xmm4,  %xmm0
	MOVUPS_A2(-16 * SIZE, A2, LDA, 1, %xmm4)
	pshufd	 $0x4e, %xmm6, %xmm7
	mulpd	 %xmm12, %xmm6
	addpd	 %xmm6,  %xmm1
	MOVUPS_A2(-14 * SIZE, A2, LDA, 1, %xmm6)

	mulpd	 %xmm13, %xmm5
	SUBPD	 %xmm5,  %xmm0
	mulpd	 %xmm13, %xmm7
	SUBPD	 %xmm7,  %xmm1

	pshufd	 $0x4e, %xmm4, %xmm5
	mulpd	 %xmm14, %xmm4
	addpd	 %xmm4,  %xmm0
	mulpd	 %xmm15, %xmm5
	SUBPD	 %xmm5,  %xmm0

	pshufd	 $0x4e, %xmm6, %xmm7
	mulpd	 %xmm14, %xmm6
	addpd	 %xmm6,  %xmm1
	mulpd	 %xmm15, %xmm7
	SUBPD	 %xmm7,  %xmm1

	MOVUPS_YS1(-16 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-14 * SIZE, Y1, %xmm1)
	movapd	 %xmm2, %xmm0

	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, A2
	addq	 $4 * SIZE, Y1
	ALIGN_3

.L17:
	testq	$1, M
	je	.L19

	MOVUPS_A1(-16 * SIZE, A1, %xmm4)
	MOVUPS_A2(-16 * SIZE, A1, LDA, 1, %xmm6)

	pshufd	 $0x4e, %xmm4, %xmm5
	mulpd	 %xmm8,  %xmm4
	addpd	 %xmm4,  %xmm0
	MOVUPS_A1(-16 * SIZE, A2, %xmm4)
	mulpd	 %xmm9,  %xmm5
	SUBPD	 %xmm5,  %xmm0

	pshufd	 $0x4e, %xmm6, %xmm7
	mulpd	 %xmm10, %xmm6
	addpd	 %xmm6,  %xmm0
	MOVUPS_A2(-16 * SIZE, A2, LDA, 1, %xmm6)
	mulpd	 %xmm11, %xmm7
	SUBPD	 %xmm7,  %xmm0

	pshufd	 $0x4e, %xmm4, %xmm5
	mulpd	 %xmm12, %xmm4
	addpd	 %xmm4,  %xmm0
	mulpd	 %xmm13, %xmm5
	SUBPD	 %xmm5,  %xmm0

	pshufd	 $0x4e, %xmm6, %xmm7
	mulpd	 %xmm14, %xmm6
	addpd	 %xmm6,  %xmm0
	mulpd	 %xmm15, %xmm7
	SUBPD	 %xmm7,  %xmm0

	MOVUPS_YS1(-16 * SIZE, Y1, %xmm0)
	ALIGN_3

.L19:
	cmpq	$4, N
	jge	.L11
	ALIGN_3

.L20:
#endif

#if GEMV_UNROLL >= 2

	cmpq	$2, N
	jl	.L30

#if GEMV_UNROLL == 2
	ALIGN_3

.L21:
#endif

	subq	$2, N

	leaq	16 * SIZE(BUFFER), Y1
	movq	A,  A1
	leaq	(A,  LDA, 1), A2
	leaq	(A,  LDA, 2), A

	movsd	0 * SIZE(X), %xmm12
	movhpd	1 * SIZE(X), %xmm12
	addq	INCX, X
	movsd	0 * SIZE(X), %xmm14
	movhpd	1 * SIZE(X), %xmm14
	addq	INCX, X

	pcmpeqb	%xmm11, %xmm11
	psllq	$63,    %xmm11
	shufps	$0xc0, %xmm11, %xmm11

	pshufd	$0x4e, %xmm12, %xmm13
	pshufd	$0x4e, %xmm14, %xmm15

#ifdef HAVE_SSE3
	movddup	ALPHA_R, %xmm8
	movddup	ALPHA_I, %xmm9
#else
	movsd	ALPHA_R, %xmm8
	unpcklpd %xmm8, %xmm8
	movsd	ALPHA_I, %xmm9
	unpcklpd %xmm9, %xmm9
#endif

	xorpd	 %xmm11, %xmm13
	xorpd	 %xmm11, %xmm15

	mulpd	 %xmm8, %xmm12
	mulpd	 %xmm9, %xmm13
	mulpd	 %xmm8, %xmm14
	mulpd	 %xmm9, %xmm15

#ifndef XCONJ
	subpd	 %xmm13, %xmm12
	subpd	 %xmm15, %xmm14
#else
	addpd	 %xmm13, %xmm12
	addpd	 %xmm15, %xmm14
#endif

	pshufd	 $0xee, %xmm12, %xmm13
	pshufd	 $0x44, %xmm12, %xmm12

	pshufd	 $0xee, %xmm14, %xmm15
	pshufd	 $0x44, %xmm14, %xmm14

#ifndef CONJ
	xorpd	 %xmm11, %xmm13
	xorpd	 %xmm11, %xmm15
#else
	xorpd	 %xmm11, %xmm12
	xorpd	 %xmm11, %xmm14
#endif

	MOVUPS_YL1(-16 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-14 * SIZE, Y1, %xmm1)
	MOVUPS_YL1(-12 * SIZE, Y1, %xmm2)
	MOVUPS_YL1(-10 * SIZE, Y1, %xmm3)
	ALIGN_3

	movq	M,   I
	sarq	$2,  I
	jle	.L25

	MOVUPS_A1(-16 * SIZE, A1, %xmm4)
	MOVUPS_A1(-14 * SIZE, A1, %xmm6)
	MOVUPS_A1(-12 * SIZE, A1, %xmm8)
	MOVUPS_A1(-10 * SIZE, A1, %xmm10)

	decq	 I
	jle	 .L24
	ALIGN_3

.L23:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(A2)
#endif

	pshufd	 $0x4e, %xmm4,  %xmm5
	mulpd	 %xmm12, %xmm4
	addpd	 %xmm4,  %xmm0
	MOVUPS_A1(-16 * SIZE, A2, %xmm4)
	pshufd	 $0x4e, %xmm6,  %xmm7
	mulpd	 %xmm12, %xmm6
	addpd	 %xmm6,  %xmm1
	MOVUPS_A1(-14 * SIZE, A2, %xmm6)

	pshufd	 $0x4e, %xmm8,  %xmm9
	mulpd	 %xmm12, %xmm8
	addpd	 %xmm8,  %xmm2
	MOVUPS_A1(-12 * SIZE, A2, %xmm8)
	pshufd	 $0x4e, %xmm10, %xmm11
	mulpd	 %xmm12, %xmm10
	addpd	 %xmm10, %xmm3
	MOVUPS_A1(-10 * SIZE, A2, %xmm10)

	mulpd	 %xmm13, %xmm5
	SUBPD	 %xmm5,  %xmm0
	mulpd	 %xmm13, %xmm7
	SUBPD	 %xmm7,  %xmm1

	mulpd	 %xmm13, %xmm9
	SUBPD	 %xmm9,  %xmm2
	mulpd	 %xmm13, %xmm11
	SUBPD	 %xmm11, %xmm3

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(A1)
#endif

	pshufd	 $0x4e, %xmm4,  %xmm5
	mulpd	 %xmm14, %xmm4
	addpd	 %xmm4,  %xmm0
	MOVUPS_A1( -8 * SIZE, A1, %xmm4)
	pshufd	 $0x4e, %xmm6,  %xmm7
	mulpd	 %xmm14, %xmm6
	addpd	 %xmm6,  %xmm1
	MOVUPS_A1( -6 * SIZE, A1, %xmm6)

	pshufd	 $0x4e, %xmm8,  %xmm9
	mulpd	 %xmm14, %xmm8
	addpd	 %xmm8,  %xmm2
	MOVUPS_A1( -4 * SIZE, A1, %xmm8)
	pshufd	 $0x4e, %xmm10, %xmm11
	mulpd	 %xmm14, %xmm10
	addpd	 %xmm10, %xmm3
	MOVUPS_A1( -2 * SIZE, A1, %xmm10)

	mulpd	 %xmm15, %xmm5
	SUBPD	 %xmm5,  %xmm0
	mulpd	 %xmm15, %xmm7
	SUBPD	 %xmm7,  %xmm1

	mulpd	 %xmm15, %xmm9
	SUBPD	 %xmm9,  %xmm2
	mulpd	 %xmm15, %xmm11
	SUBPD	 %xmm11, %xmm3

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(Y1)
#endif

	MOVUPS_YS1(-16 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-14 * SIZE, Y1, %xmm1)
	MOVUPS_YS1(-12 * SIZE, Y1, %xmm2)
	MOVUPS_YS1(-10 * SIZE, Y1, %xmm3)

	MOVUPS_YL1( -8 * SIZE, Y1, %xmm0)
	MOVUPS_YL1( -6 * SIZE, Y1, %xmm1)
	MOVUPS_YL1( -4 * SIZE, Y1, %xmm2)
	MOVUPS_YL1( -2 * SIZE, Y1, %xmm3)

	subq	 $-8 * SIZE, A1
	subq	 $-8 * SIZE, A2
	subq	 $-8 * SIZE, Y1

	subq	 $1, I
	BRANCH
	jg	.L23
	ALIGN_3

.L24:
	pshufd	 $0x4e, %xmm4,  %xmm5
	mulpd	 %xmm12, %xmm4
	addpd	 %xmm4,  %xmm0
	MOVUPS_A1(-16 * SIZE, A2, %xmm4)
	pshufd	 $0x4e, %xmm6,  %xmm7
	mulpd	 %xmm12, %xmm6
	addpd	 %xmm6,  %xmm1
	MOVUPS_A1(-14 * SIZE, A2, %xmm6)

	pshufd	 $0x4e, %xmm8,  %xmm9
	mulpd	 %xmm12, %xmm8
	addpd	 %xmm8,  %xmm2
	MOVUPS_A1(-12 * SIZE, A2, %xmm8)
	pshufd	 $0x4e, %xmm10, %xmm11
	mulpd	 %xmm12, %xmm10
	addpd	 %xmm10, %xmm3
	MOVUPS_A1(-10 * SIZE, A2, %xmm10)

	mulpd	 %xmm13, %xmm5
	SUBPD	 %xmm5,  %xmm0
	mulpd	 %xmm13, %xmm7
	SUBPD	 %xmm7,  %xmm1

	mulpd	 %xmm13, %xmm9
	SUBPD	 %xmm9,  %xmm2
	mulpd	 %xmm13, %xmm11
	SUBPD	 %xmm11, %xmm3

	pshufd	 $0x4e, %xmm4,  %xmm5
	mulpd	 %xmm14, %xmm4
	addpd	 %xmm4,  %xmm0
	pshufd	 $0x4e, %xmm6,  %xmm7
	mulpd	 %xmm14, %xmm6
	addpd	 %xmm6,  %xmm1

	pshufd	 $0x4e, %xmm8,  %xmm9
	mulpd	 %xmm14, %xmm8
	addpd	 %xmm8,  %xmm2
	pshufd	 $0x4e, %xmm10, %xmm11
	mulpd	 %xmm14, %xmm10
	addpd	 %xmm10, %xmm3

	mulpd	 %xmm15, %xmm5
	SUBPD	 %xmm5,  %xmm0
	mulpd	 %xmm15, %xmm7
	SUBPD	 %xmm7,  %xmm1

	mulpd	 %xmm15, %xmm9
	SUBPD	 %xmm9,  %xmm2
	mulpd	 %xmm15, %xmm11
	SUBPD	 %xmm11, %xmm3

	MOVUPS_YS1(-16 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-14 * SIZE, Y1, %xmm1)
	MOVUPS_YS1(-12 * SIZE, Y1, %xmm2)
	MOVUPS_YS1(-10 * SIZE, Y1, %xmm3)

	MOVUPS_YL1( -8 * SIZE, Y1, %xmm0)
	MOVUPS_YL1( -6 * SIZE, Y1, %xmm1)
	MOVUPS_YL1( -4 * SIZE, Y1, %xmm2)
	MOVUPS_YL1( -2 * SIZE, Y1, %xmm3)

	subq	 $-8 * SIZE, A1
	subq	 $-8 * SIZE, A2
	subq	 $-8 * SIZE, Y1
	ALIGN_3

.L25:
	testq	$2, M
	je	.L27

	MOVUPS_A1(-16 * SIZE, A1, %xmm4)
	MOVUPS_A1(-14 * SIZE, A1, %xmm6)
	MOVUPS_A1(-16 * SIZE, A2, %xmm8)
	MOVUPS_A1(-14 * SIZE, A2, %xmm10)

	pshufd	 $0x4e, %xmm4,  %xmm5
	mulpd	 %xmm12, %xmm4
	addpd	 %xmm4,  %xmm0
	pshufd	 $0x4e, %xmm6,  %xmm7
	mulpd	 %xmm12, %xmm6
	addpd	 %xmm6,  %xmm1

	mulpd	 %xmm13, %xmm5
	SUBPD	 %xmm5,  %xmm0
	mulpd	 %xmm13, %xmm7
	SUBPD	 %xmm7,  %xmm1

	pshufd	 $0x4e, %xmm8,  %xmm9
	mulpd	 %xmm14, %xmm8
	addpd	 %xmm8,  %xmm0
	pshufd	 $0x4e, %xmm10, %xmm11
	mulpd	 %xmm14, %xmm10
	addpd	 %xmm10, %xmm1

	mulpd	 %xmm15, %xmm9
	SUBPD	 %xmm9,  %xmm0
	mulpd	 %xmm15, %xmm11
	SUBPD	 %xmm11, %xmm1

	MOVUPS_YS1(-16 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-14 * SIZE, Y1, %xmm1)
	movapd	 %xmm2, %xmm0

	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, A2
	addq	 $4 * SIZE, Y1
	ALIGN_3

.L27:
	testq	$1, M
#if GEMV_UNROLL == 2
	je	.L29
#else
	je	.L30
#endif

	MOVUPS_A1(-16 * SIZE, A1, %xmm4)
	MOVUPS_A1(-16 * SIZE, A2, %xmm6)

	pshufd	 $0x4e, %xmm4, %xmm5
	mulpd	 %xmm12, %xmm4
	addpd	 %xmm4,  %xmm0
	mulpd	 %xmm13, %xmm5
	SUBPD	 %xmm5,  %xmm0

	pshufd	 $0x4e, %xmm6, %xmm7
	mulpd	 %xmm14, %xmm6
	addpd	 %xmm6,  %xmm0
	mulpd	 %xmm15, %xmm7
	SUBPD	 %xmm7,  %xmm0

	MOVUPS_YS1(-16 * SIZE, Y1, %xmm0)

#if GEMV_UNROLL == 2
	ALIGN_3

.L29:
	cmpq	$2, N
	jge	.L21
#endif
	ALIGN_3

.L30:
#endif

	cmpq	$1, N
	jl	.L980

#if GEMV_UNROLL == 1
.L31:
	decq	N
#endif

	leaq	16 * SIZE(BUFFER), Y1
	movq	A,  A1
#if GEMV_UNROLL == 1
	addq	LDA, A
#endif

	movsd	0 * SIZE(X), %xmm12
	movhpd	1 * SIZE(X), %xmm12
	addq	INCX, X

	pcmpeqb	%xmm11, %xmm11
	psllq	$63,    %xmm11
	shufps	$0xc0, %xmm11, %xmm11

	pshufd	$0x4e, %xmm12, %xmm13

#ifdef HAVE_SSE3
	movddup	ALPHA_R, %xmm8
	movddup	ALPHA_I, %xmm9
#else
	movsd	ALPHA_R, %xmm8
	unpcklpd %xmm8, %xmm8
	movsd	ALPHA_I, %xmm9
	unpcklpd %xmm9, %xmm9
#endif

	xorpd	 %xmm11, %xmm13

	mulpd	 %xmm8, %xmm12
	mulpd	 %xmm9, %xmm13

#ifndef XCONJ
	subpd	 %xmm13, %xmm12
#else
	addpd	 %xmm13, %xmm12
#endif

	pshufd	 $0xee, %xmm12, %xmm13
	pshufd	 $0x44, %xmm12, %xmm12

#ifndef CONJ
	xorpd	 %xmm11, %xmm13
#else
	xorpd	 %xmm11, %xmm12
#endif

	MOVUPS_YL1(-16 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-14 * SIZE, Y1, %xmm1)
	MOVUPS_YL1(-12 * SIZE, Y1, %xmm2)
	MOVUPS_YL1(-10 * SIZE, Y1, %xmm3)

	movq	M,   I
	sarq	$2,  I
	jle	.L35

	MOVUPS_A1(-16 * SIZE, A1, %xmm4)
	MOVUPS_A1(-14 * SIZE, A1, %xmm6)
	MOVUPS_A1(-12 * SIZE, A1, %xmm8)
	MOVUPS_A1(-10 * SIZE, A1, %xmm10)

	decq	 I
	jle	 .L34
	ALIGN_3

.L33:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 4 - 128 + PREOFFSET(A1)
#endif

	pshufd	 $0x4e, %xmm4,  %xmm5
	mulpd	 %xmm12, %xmm4
	addpd	 %xmm4,  %xmm0
	MOVUPS_A1( -8 * SIZE, A1, %xmm4)
	pshufd	 $0x4e, %xmm6,  %xmm7
	mulpd	 %xmm12, %xmm6
	addpd	 %xmm6,  %xmm1
	MOVUPS_A1( -6 * SIZE, A1, %xmm6)

	pshufd	 $0x4e, %xmm8,  %xmm9
	mulpd	 %xmm12, %xmm8
	addpd	 %xmm8,  %xmm2
	MOVUPS_A1( -4 * SIZE, A1, %xmm8)
	pshufd	 $0x4e, %xmm10, %xmm11
	mulpd	 %xmm12, %xmm10
	addpd	 %xmm10, %xmm3
	MOVUPS_A1( -2 * SIZE, A1, %xmm10)

	mulpd	 %xmm13, %xmm5
	SUBPD	 %xmm5,  %xmm0
	mulpd	 %xmm13, %xmm7
	SUBPD	 %xmm7,  %xmm1

	mulpd	 %xmm13, %xmm9
	SUBPD	 %xmm9,  %xmm2
	mulpd	 %xmm13, %xmm11
	SUBPD	 %xmm11, %xmm3

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE) * 4 - 128 + PREOFFSET(Y1)
#endif

	MOVUPS_YS1(-16 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-14 * SIZE, Y1, %xmm1)
	MOVUPS_YS1(-12 * SIZE, Y1, %xmm2)
	MOVUPS_YS1(-10 * SIZE, Y1, %xmm3)

	MOVUPS_YL1( -8 * SIZE, Y1, %xmm0)
	MOVUPS_YL1( -6 * SIZE, Y1, %xmm1)
	MOVUPS_YL1( -4 * SIZE, Y1, %xmm2)
	MOVUPS_YL1( -2 * SIZE, Y1, %xmm3)

	subq	 $-8 * SIZE, A1
	subq	 $-8 * SIZE, Y1

	subq	 $1, I
	BRANCH
	jg	.L33
	ALIGN_3

.L34:
	pshufd	 $0x4e, %xmm4,  %xmm5
	mulpd	 %xmm12, %xmm4
	addpd	 %xmm4,  %xmm0
	pshufd	 $0x4e, %xmm6,  %xmm7
	mulpd	 %xmm12, %xmm6
	addpd	 %xmm6,  %xmm1

	pshufd	 $0x4e, %xmm8,  %xmm9
	mulpd	 %xmm12, %xmm8
	addpd	 %xmm8,  %xmm2
	pshufd	 $0x4e, %xmm10, %xmm11
	mulpd	 %xmm12, %xmm10
	addpd	 %xmm10, %xmm3

	mulpd	 %xmm13, %xmm5
	SUBPD	 %xmm5,  %xmm0
	mulpd	 %xmm13, %xmm7
	SUBPD	 %xmm7,  %xmm1

	mulpd	 %xmm13, %xmm9
	SUBPD	 %xmm9,  %xmm2
	mulpd	 %xmm13, %xmm11
	SUBPD	 %xmm11, %xmm3

	MOVUPS_YS1(-16 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-14 * SIZE, Y1, %xmm1)
	MOVUPS_YS1(-12 * SIZE, Y1, %xmm2)
	MOVUPS_YS1(-10 * SIZE, Y1, %xmm3)

	MOVUPS_YL1( -8 * SIZE, Y1, %xmm0)
	MOVUPS_YL1( -6 * SIZE, Y1, %xmm1)
	MOVUPS_YL1( -4 * SIZE, Y1, %xmm2)
	MOVUPS_YL1( -2 * SIZE, Y1, %xmm3)

	subq	 $-8 * SIZE, A1
	subq	 $-8 * SIZE, Y1
	ALIGN_3

.L35:
	testq	$2, M
	je	.L37

	MOVUPS_A1(-16 * SIZE, A1, %xmm4)
	MOVUPS_A1(-14 * SIZE, A1, %xmm6)

	pshufd	 $0x4e, %xmm4,  %xmm5
	mulpd	 %xmm12, %xmm4
	addpd	 %xmm4,  %xmm0
	pshufd	 $0x4e, %xmm6,  %xmm7
	mulpd	 %xmm12, %xmm6
	addpd	 %xmm6,  %xmm1

	mulpd	 %xmm13, %xmm5
	SUBPD	 %xmm5,  %xmm0
	mulpd	 %xmm13, %xmm7
	SUBPD	 %xmm7,  %xmm1

	MOVUPS_YS1(-16 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-14 * SIZE, Y1, %xmm1)
	movapd	 %xmm2, %xmm0

	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, Y1
	ALIGN_3

.L37:
	testq	$1, M
#if GEMV_UNROLL == 1
	je	.L39
#else
	je	.L980
#endif

	MOVUPS_A1(-16 * SIZE, A1, %xmm4)

	pshufd	 $0x4e, %xmm4, %xmm5
	mulpd	 %xmm12, %xmm4
	addpd	 %xmm4,  %xmm0
	mulpd	 %xmm13, %xmm5
	SUBPD	 %xmm5,  %xmm0

	MOVUPS_YS1(-16 * SIZE, Y1, %xmm0)

#if GEMV_UNROLL == 1
	ALIGN_3
.L39:
	cmpq	$1, N
	jge	.L31
#endif

#ifdef ALIGNED_ACCESS

	jmp	.L980
	ALIGN_3

.L100:
#if GEMV_UNROLL >= 4

	cmpq	$4, N
	jl	.L110
	ALIGN_3

.L101:
	subq	$4, N

	leaq	16 * SIZE(BUFFER), Y1
	movq	A,  A1
	leaq	(A,  LDA, 2), A2
	leaq	(A,  LDA, 4), A

	movsd	0 * SIZE(X), %xmm8
	movhpd	1 * SIZE(X), %xmm8
	addq	INCX, X
	movsd	0 * SIZE(X), %xmm10
	movhpd	1 * SIZE(X), %xmm10
	addq	INCX, X
	movsd	0 * SIZE(X), %xmm12
	movhpd	1 * SIZE(X), %xmm12
	addq	INCX, X
	movsd	0 * SIZE(X), %xmm14
	movhpd	1 * SIZE(X), %xmm14
	addq	INCX, X

	pcmpeqb	%xmm5, %xmm5
	psllq	$63,   %xmm5
	shufps	$0xc0, %xmm5, %xmm5

	pshufd	$0x4e, %xmm8,  %xmm9
	pshufd	$0x4e, %xmm10, %xmm11
	pshufd	$0x4e, %xmm12, %xmm13
	pshufd	$0x4e, %xmm14, %xmm15

#ifdef HAVE_SSE3
	movddup	ALPHA_R, %xmm6
	movddup	ALPHA_I, %xmm7
#else
	movsd	ALPHA_R, %xmm6
	unpcklpd %xmm6, %xmm6
	movsd	ALPHA_I, %xmm7
	unpcklpd %xmm7, %xmm7
#endif

	xorpd	 %xmm5, %xmm9
	xorpd	 %xmm5, %xmm11
	xorpd	 %xmm5, %xmm13
	xorpd	 %xmm5, %xmm15

	mulpd	 %xmm6, %xmm8
	mulpd	 %xmm7, %xmm9
	mulpd	 %xmm6, %xmm10
	mulpd	 %xmm7, %xmm11

	mulpd	 %xmm6, %xmm12
	mulpd	 %xmm7, %xmm13
	mulpd	 %xmm6, %xmm14
	mulpd	 %xmm7, %xmm15

#ifndef XCONJ
	subpd	 %xmm9,  %xmm8
	subpd	 %xmm11, %xmm10
	subpd	 %xmm13, %xmm12
	subpd	 %xmm15, %xmm14
#else
	addpd	 %xmm9,  %xmm8
	addpd	 %xmm11, %xmm10
	addpd	 %xmm13, %xmm12
	addpd	 %xmm15, %xmm14
#endif

	pshufd	 $0xee, %xmm8,  %xmm9
	pshufd	 $0x44, %xmm8,  %xmm8

	pshufd	 $0xee, %xmm10, %xmm11
	pshufd	 $0x44, %xmm10, %xmm10

	pshufd	 $0xee, %xmm12, %xmm13
	pshufd	 $0x44, %xmm12, %xmm12

	pshufd	 $0xee, %xmm14, %xmm15
	pshufd	 $0x44, %xmm14, %xmm14

#ifndef CONJ
	xorpd	 %xmm5, %xmm9
	xorpd	 %xmm5, %xmm11
	xorpd	 %xmm5, %xmm13
	xorpd	 %xmm5, %xmm15
#else
	xorpd	 %xmm5, %xmm8
	xorpd	 %xmm5, %xmm10
	xorpd	 %xmm5, %xmm12
	xorpd	 %xmm5, %xmm14
#endif

	MOVUPS_YL1(-16 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-14 * SIZE, Y1, %xmm1)
	MOVUPS_YL1(-12 * SIZE, Y1, %xmm2)
	MOVUPS_YL1(-10 * SIZE, Y1, %xmm3)
	ALIGN_3

	movq	M,   I
	sarq	$2,  I
	jle	.L105

	movsd	-16 * SIZE(A1), %xmm4
	movhpd	-15 * SIZE(A1), %xmm4
	movsd	-14 * SIZE(A1), %xmm6
	movhpd	-13 * SIZE(A1), %xmm6

	decq	 I
	jle	 .L104
	ALIGN_3

.L103:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A1)
#endif

	pshufd	 $0x4e, %xmm4, %xmm5
	mulpd	 %xmm8,  %xmm4
	addpd	 %xmm4,  %xmm0
	movsd	-12 * SIZE(A1), %xmm4
	movhpd	-11 * SIZE(A1), %xmm4
	pshufd	 $0x4e, %xmm6, %xmm7
	mulpd	 %xmm8,  %xmm6
	addpd	 %xmm6,  %xmm1
	movsd	-10 * SIZE(A1), %xmm6
	movhpd	 -9 * SIZE(A1), %xmm6

	mulpd	 %xmm9,  %xmm5
	SUBPD	 %xmm5,  %xmm0
	mulpd	 %xmm9,  %xmm7
	SUBPD	 %xmm7,  %xmm1

	pshufd	 $0x4e, %xmm4, %xmm5
	mulpd	 %xmm8,  %xmm4
	addpd	 %xmm4,  %xmm2
	movsd	-16 * SIZE(A1, LDA), %xmm4
	movhpd	-15 * SIZE(A1, LDA), %xmm4
	pshufd	 $0x4e, %xmm6, %xmm7
	mulpd	 %xmm8,  %xmm6
	addpd	 %xmm6,  %xmm3
	movsd	-14 * SIZE(A1, LDA), %xmm6
	movhpd	-13 * SIZE(A1, LDA), %xmm6

	mulpd	 %xmm9,  %xmm5
	SUBPD	 %xmm5,  %xmm2
	mulpd	 %xmm9,  %xmm7
	SUBPD	 %xmm7,  %xmm3

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A1, LDA)
#endif

	pshufd	 $0x4e, %xmm4, %xmm5
	mulpd	 %xmm10, %xmm4
	addpd	 %xmm4,  %xmm0
	movsd	-12 * SIZE(A1, LDA), %xmm4
	movhpd	-11 * SIZE(A1, LDA), %xmm4
	pshufd	 $0x4e, %xmm6, %xmm7
	mulpd	 %xmm10, %xmm6
	addpd	 %xmm6,  %xmm1
	movsd	-10 * SIZE(A1, LDA), %xmm6
	movhpd	 -9 * SIZE(A1, LDA), %xmm6

	mulpd	 %xmm11, %xmm5
	SUBPD	 %xmm5,  %xmm0
	mulpd	 %xmm11, %xmm7
	SUBPD	 %xmm7,  %xmm1

	pshufd	 $0x4e, %xmm4, %xmm5
	mulpd	 %xmm10, %xmm4
	addpd	 %xmm4,  %xmm2
	movsd	-16 * SIZE(A2), %xmm4
	movhpd	-15 * SIZE(A2), %xmm4
	pshufd	 $0x4e, %xmm6, %xmm7
	mulpd	 %xmm10, %xmm6
	addpd	 %xmm6,  %xmm3
	movsd	-14 * SIZE(A2), %xmm6
	movhpd	-13 * SIZE(A2), %xmm6

	mulpd	 %xmm11, %xmm5
	SUBPD	 %xmm5,  %xmm2
	mulpd	 %xmm11, %xmm7
	SUBPD	 %xmm7,  %xmm3

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A2)
#endif

	pshufd	 $0x4e, %xmm4, %xmm5
	mulpd	 %xmm12, %xmm4
	addpd	 %xmm4,  %xmm0
	movsd	-12 * SIZE(A2), %xmm4
	movhpd	-11 * SIZE(A2), %xmm4
	pshufd	 $0x4e, %xmm6, %xmm7
	mulpd	 %xmm12, %xmm6
	addpd	 %xmm6,  %xmm1
	movsd	-10 * SIZE(A2), %xmm6
	movhpd	 -9 * SIZE(A2), %xmm6

	mulpd	 %xmm13, %xmm5
	SUBPD	 %xmm5,  %xmm0
	mulpd	 %xmm13, %xmm7
	SUBPD	 %xmm7,  %xmm1

	pshufd	 $0x4e, %xmm4, %xmm5
	mulpd	 %xmm12, %xmm4
	addpd	 %xmm4,  %xmm2
	movsd	-16 * SIZE(A2, LDA), %xmm4
	movhpd	-15 * SIZE(A2, LDA), %xmm4
	pshufd	 $0x4e, %xmm6, %xmm7
	mulpd	 %xmm12, %xmm6
	addpd	 %xmm6,  %xmm3
	movsd	-14 * SIZE(A2, LDA), %xmm6
	movhpd	-13 * SIZE(A2, LDA), %xmm6

	mulpd	 %xmm13, %xmm5
	SUBPD	 %xmm5,  %xmm2
	mulpd	 %xmm13, %xmm7
	SUBPD	 %xmm7,  %xmm3

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A2, LDA)
#endif

	pshufd	 $0x4e, %xmm4, %xmm5
	mulpd	 %xmm14, %xmm4
	addpd	 %xmm4,  %xmm0
	movsd	-12 * SIZE(A2, LDA), %xmm4
	movhpd	-11 * SIZE(A2, LDA), %xmm4
	pshufd	 $0x4e, %xmm6, %xmm7
	mulpd	 %xmm14, %xmm6
	addpd	 %xmm6,  %xmm1
	movsd	-10 * SIZE(A2, LDA), %xmm6
	movhpd	 -9 * SIZE(A2, LDA), %xmm6

	mulpd	 %xmm15, %xmm5
	SUBPD	 %xmm5,  %xmm0
	mulpd	 %xmm15, %xmm7
	SUBPD	 %xmm7,  %xmm1

	pshufd	 $0x4e, %xmm4, %xmm5
	mulpd	 %xmm14, %xmm4
	addpd	 %xmm4,  %xmm2
	movsd	 -8 * SIZE(A1), %xmm4
	movhpd	 -7 * SIZE(A1), %xmm4
	pshufd	 $0x4e, %xmm6, %xmm7
	mulpd	 %xmm14, %xmm6
	addpd	 %xmm6,  %xmm3
	movsd	 -6 * SIZE(A1), %xmm6
	movhpd	 -5 * SIZE(A1), %xmm6

	mulpd	 %xmm15, %xmm5
	SUBPD	 %xmm5,  %xmm2
	mulpd	 %xmm15, %xmm7
	SUBPD	 %xmm7,  %xmm3

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE) - 128 + PREOFFSET(Y1)
#endif

	MOVUPS_YS1(-16 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-14 * SIZE, Y1, %xmm1)
	MOVUPS_YS1(-12 * SIZE, Y1, %xmm2)
	MOVUPS_YS1(-10 * SIZE, Y1, %xmm3)

	MOVUPS_YL1( -8 * SIZE, Y1, %xmm0)
	MOVUPS_YL1( -6 * SIZE, Y1, %xmm1)
	MOVUPS_YL1( -4 * SIZE, Y1, %xmm2)
	MOVUPS_YL1( -2 * SIZE, Y1, %xmm3)

	subq	 $-8 * SIZE, A1
	subq	 $-8 * SIZE, A2
	subq	 $-8 * SIZE, Y1

	subq	 $1, I
	BRANCH
	jg	.L103
	ALIGN_3

.L104:
	pshufd	 $0x4e, %xmm4, %xmm5
	mulpd	 %xmm8,  %xmm4
	addpd	 %xmm4,  %xmm0
	movsd	-12 * SIZE(A1), %xmm4
	movhpd	-11 * SIZE(A1), %xmm4
	pshufd	 $0x4e, %xmm6, %xmm7
	mulpd	 %xmm8,  %xmm6
	addpd	 %xmm6,  %xmm1
	movsd	-10 * SIZE(A1), %xmm6
	movhpd	 -9 * SIZE(A1), %xmm6

	mulpd	 %xmm9,  %xmm5
	SUBPD	 %xmm5,  %xmm0
	mulpd	 %xmm9,  %xmm7
	SUBPD	 %xmm7,  %xmm1

	pshufd	 $0x4e, %xmm4, %xmm5
	mulpd	 %xmm8,  %xmm4
	addpd	 %xmm4,  %xmm2
	movsd	-16 * SIZE(A1, LDA), %xmm4
	movhpd	-15 * SIZE(A1, LDA), %xmm4
	pshufd	 $0x4e, %xmm6, %xmm7
	mulpd	 %xmm8,  %xmm6
	addpd	 %xmm6,  %xmm3
	movsd	-14 * SIZE(A1, LDA), %xmm6
	movhpd	-13 * SIZE(A1, LDA), %xmm6

	mulpd	 %xmm9,  %xmm5
	SUBPD	 %xmm5,  %xmm2
	mulpd	 %xmm9,  %xmm7
	SUBPD	 %xmm7,  %xmm3

	pshufd	 $0x4e, %xmm4, %xmm5
	mulpd	 %xmm10, %xmm4
	addpd	 %xmm4,  %xmm0
	movsd	-12 * SIZE(A1, LDA), %xmm4
	movhpd	-11 * SIZE(A1, LDA), %xmm4
	pshufd	 $0x4e, %xmm6, %xmm7
	mulpd	 %xmm10, %xmm6
	addpd	 %xmm6,  %xmm1
	movsd	-10 * SIZE(A1, LDA), %xmm6
	movhpd	 -9 * SIZE(A1, LDA), %xmm6

	mulpd	 %xmm11, %xmm5
	SUBPD	 %xmm5,  %xmm0
	mulpd	 %xmm11, %xmm7
	SUBPD	 %xmm7,  %xmm1

	pshufd	 $0x4e, %xmm4, %xmm5
	mulpd	 %xmm10, %xmm4
	addpd	 %xmm4,  %xmm2
	movsd	-16 * SIZE(A2), %xmm4
	movhpd	-15 * SIZE(A2), %xmm4
	pshufd	 $0x4e, %xmm6, %xmm7
	mulpd	 %xmm10, %xmm6
	addpd	 %xmm6,  %xmm3
	movsd	-14 * SIZE(A2), %xmm6
	movhpd	-13 * SIZE(A2), %xmm6

	mulpd	 %xmm11, %xmm5
	SUBPD	 %xmm5,  %xmm2
	mulpd	 %xmm11, %xmm7
	SUBPD	 %xmm7,  %xmm3

	pshufd	 $0x4e, %xmm4, %xmm5
	mulpd	 %xmm12, %xmm4
	addpd	 %xmm4,  %xmm0
	movsd	-12 * SIZE(A2), %xmm4
	movhpd	-11 * SIZE(A2), %xmm4
	pshufd	 $0x4e, %xmm6, %xmm7
	mulpd	 %xmm12, %xmm6
	addpd	 %xmm6,  %xmm1
	movsd	-10 * SIZE(A2), %xmm6
	movhpd	 -9 * SIZE(A2), %xmm6

	mulpd	 %xmm13, %xmm5
	SUBPD	 %xmm5,  %xmm0
	mulpd	 %xmm13, %xmm7
	SUBPD	 %xmm7,  %xmm1

	pshufd	 $0x4e, %xmm4, %xmm5
	mulpd	 %xmm12, %xmm4
	addpd	 %xmm4,  %xmm2
	movsd	-16 * SIZE(A2, LDA), %xmm4
	movhpd	-15 * SIZE(A2, LDA), %xmm4
	pshufd	 $0x4e, %xmm6, %xmm7
	mulpd	 %xmm12, %xmm6
	addpd	 %xmm6,  %xmm3
	movsd	-14 * SIZE(A2, LDA), %xmm6
	movhpd	-13 * SIZE(A2, LDA), %xmm6

	mulpd	 %xmm13, %xmm5
	SUBPD	 %xmm5,  %xmm2
	mulpd	 %xmm13, %xmm7
	SUBPD	 %xmm7,  %xmm3

	pshufd	 $0x4e, %xmm4, %xmm5
	mulpd	 %xmm14, %xmm4
	addpd	 %xmm4,  %xmm0
	movsd	-12 * SIZE(A2, LDA), %xmm4
	movhpd	-11 * SIZE(A2, LDA), %xmm4
	pshufd	 $0x4e, %xmm6, %xmm7
	mulpd	 %xmm14, %xmm6
	addpd	 %xmm6,  %xmm1
	movsd	-10 * SIZE(A2, LDA), %xmm6
	movhpd	 -9 * SIZE(A2, LDA), %xmm6

	mulpd	 %xmm15, %xmm5
	SUBPD	 %xmm5,  %xmm0
	mulpd	 %xmm15, %xmm7
	SUBPD	 %xmm7,  %xmm1

	pshufd	 $0x4e, %xmm4, %xmm5
	mulpd	 %xmm14, %xmm4
	addpd	 %xmm4,  %xmm2
	pshufd	 $0x4e, %xmm6, %xmm7
	mulpd	 %xmm14, %xmm6
	addpd	 %xmm6,  %xmm3

	mulpd	 %xmm15, %xmm5
	SUBPD	 %xmm5,  %xmm2
	mulpd	 %xmm15, %xmm7
	SUBPD	 %xmm7,  %xmm3

	MOVUPS_YS1(-16 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-14 * SIZE, Y1, %xmm1)
	MOVUPS_YS1(-12 * SIZE, Y1, %xmm2)
	MOVUPS_YS1(-10 * SIZE, Y1, %xmm3)

	MOVUPS_YL1( -8 * SIZE, Y1, %xmm0)
	MOVUPS_YL1( -6 * SIZE, Y1, %xmm1)
	MOVUPS_YL1( -4 * SIZE, Y1, %xmm2)
	MOVUPS_YL1( -2 * SIZE, Y1, %xmm3)

	subq	 $-8 * SIZE, A1
	subq	 $-8 * SIZE, A2
	subq	 $-8 * SIZE, Y1
	ALIGN_3

.L105:
	testq	$2, M
	je	.L107

	movsd	-16 * SIZE(A1), %xmm4
	movhpd	-15 * SIZE(A1), %xmm4
	movsd	-14 * SIZE(A1), %xmm6
	movhpd	-13 * SIZE(A1), %xmm6

	pshufd	 $0x4e, %xmm4, %xmm5
	mulpd	 %xmm8,  %xmm4
	addpd	 %xmm4,  %xmm0
	movsd	-16 * SIZE(A1, LDA), %xmm4
	movhpd	-15 * SIZE(A1, LDA), %xmm4
	pshufd	 $0x4e, %xmm6, %xmm7
	mulpd	 %xmm8,  %xmm6
	addpd	 %xmm6,  %xmm1
	movsd	-14 * SIZE(A1, LDA), %xmm6
	movhpd	-13 * SIZE(A1, LDA), %xmm6

	mulpd	 %xmm9,  %xmm5
	SUBPD	 %xmm5,  %xmm0
	mulpd	 %xmm9,  %xmm7
	SUBPD	 %xmm7,  %xmm1

	pshufd	 $0x4e, %xmm4, %xmm5
	mulpd	 %xmm10, %xmm4
	addpd	 %xmm4,  %xmm0
	movsd	-16 * SIZE(A2), %xmm4
	movhpd	-15 * SIZE(A2), %xmm4
	pshufd	 $0x4e, %xmm6, %xmm7
	mulpd	 %xmm10, %xmm6
	addpd	 %xmm6,  %xmm1
	movsd	-14 * SIZE(A2), %xmm6
	movhpd	-13 * SIZE(A2), %xmm6

	mulpd	 %xmm11, %xmm5
	SUBPD	 %xmm5,  %xmm0
	mulpd	 %xmm11, %xmm7
	SUBPD	 %xmm7,  %xmm1

	pshufd	 $0x4e, %xmm4, %xmm5
	mulpd	 %xmm12, %xmm4
	addpd	 %xmm4,  %xmm0
	movsd	-16 * SIZE(A2, LDA), %xmm4
	movhpd	-15 * SIZE(A2, LDA), %xmm4
	pshufd	 $0x4e, %xmm6, %xmm7
	mulpd	 %xmm12, %xmm6
	addpd	 %xmm6,  %xmm1
	movsd	-14 * SIZE(A2, LDA), %xmm6
	movhpd	-13 * SIZE(A2, LDA), %xmm6

	mulpd	 %xmm13, %xmm5
	SUBPD	 %xmm5,  %xmm0
	mulpd	 %xmm13, %xmm7
	SUBPD	 %xmm7,  %xmm1

	pshufd	 $0x4e, %xmm4, %xmm5
	mulpd	 %xmm14, %xmm4
	addpd	 %xmm4,  %xmm0
	mulpd	 %xmm15, %xmm5
	SUBPD	 %xmm5,  %xmm0

	pshufd	 $0x4e, %xmm6, %xmm7
	mulpd	 %xmm14, %xmm6
	addpd	 %xmm6,  %xmm1
	mulpd	 %xmm15, %xmm7
	SUBPD	 %xmm7,  %xmm1

	MOVUPS_YS1(-16 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-14 * SIZE, Y1, %xmm1)
	movapd	 %xmm2, %xmm0

	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, A2
	addq	 $4 * SIZE, Y1
	ALIGN_3

.L107:
	testq	$1, M
	je	.L109

	movsd	-16 * SIZE(A1), %xmm4
	movhpd	-15 * SIZE(A1), %xmm4
	movsd	-16 * SIZE(A1, LDA), %xmm6
	movhpd	-15 * SIZE(A1, LDA), %xmm6

	pshufd	 $0x4e, %xmm4, %xmm5
	mulpd	 %xmm8,  %xmm4
	addpd	 %xmm4,  %xmm0
	movsd	-16 * SIZE(A2), %xmm4
	movhpd	-15 * SIZE(A2), %xmm4
	mulpd	 %xmm9,  %xmm5
	SUBPD	 %xmm5,  %xmm0

	pshufd	 $0x4e, %xmm6, %xmm7
	mulpd	 %xmm10, %xmm6
	addpd	 %xmm6,  %xmm0
	movsd	-16 * SIZE(A2, LDA), %xmm6
	movhpd	-15 * SIZE(A2, LDA), %xmm6
	mulpd	 %xmm11, %xmm7
	SUBPD	 %xmm7,  %xmm0

	pshufd	 $0x4e, %xmm4, %xmm5
	mulpd	 %xmm12, %xmm4
	addpd	 %xmm4,  %xmm0
	mulpd	 %xmm13, %xmm5
	SUBPD	 %xmm5,  %xmm0

	pshufd	 $0x4e, %xmm6, %xmm7
	mulpd	 %xmm14, %xmm6
	addpd	 %xmm6,  %xmm0
	mulpd	 %xmm15, %xmm7
	SUBPD	 %xmm7,  %xmm0

	MOVUPS_YS1(-16 * SIZE, Y1, %xmm0)
	ALIGN_3

.L109:
	cmpq	$4, N
	jge	.L101
	ALIGN_3

.L110:
#endif

#if GEMV_UNROLL >= 2

	cmpq	$2, N
	jl	.L120

#if GEMV_UNROLL == 2
	ALIGN_3

.L111:
#endif

	subq	$2, N

	leaq	16 * SIZE(BUFFER), Y1
	movq	A,  A1
	leaq	(A,  LDA, 1), A2
	leaq	(A,  LDA, 2), A

	movsd	0 * SIZE(X), %xmm12
	movhpd	1 * SIZE(X), %xmm12
	addq	INCX, X
	movsd	0 * SIZE(X), %xmm14
	movhpd	1 * SIZE(X), %xmm14
	addq	INCX, X

	pcmpeqb	%xmm11, %xmm11
	psllq	$63,    %xmm11
	shufps	$0xc0, %xmm11, %xmm11

	pshufd	$0x4e, %xmm12, %xmm13
	pshufd	$0x4e, %xmm14, %xmm15

#ifdef HAVE_SSE3
	movddup	ALPHA_R, %xmm8
	movddup	ALPHA_I, %xmm9
#else
	movsd	ALPHA_R, %xmm8
	unpcklpd %xmm8, %xmm8
	movsd	ALPHA_I, %xmm9
	unpcklpd %xmm9, %xmm9
#endif

	xorpd	 %xmm11, %xmm13
	xorpd	 %xmm11, %xmm15

	mulpd	 %xmm8, %xmm12
	mulpd	 %xmm9, %xmm13
	mulpd	 %xmm8, %xmm14
	mulpd	 %xmm9, %xmm15

#ifndef XCONJ
	subpd	 %xmm13, %xmm12
	subpd	 %xmm15, %xmm14
#else
	addpd	 %xmm13, %xmm12
	addpd	 %xmm15, %xmm14
#endif

	pshufd	 $0xee, %xmm12, %xmm13
	pshufd	 $0x44, %xmm12, %xmm12

	pshufd	 $0xee, %xmm14, %xmm15
	pshufd	 $0x44, %xmm14, %xmm14

#ifndef CONJ
	xorpd	 %xmm11, %xmm13
	xorpd	 %xmm11, %xmm15
#else
	xorpd	 %xmm11, %xmm12
	xorpd	 %xmm11, %xmm14
#endif

	MOVUPS_YL1(-16 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-14 * SIZE, Y1, %xmm1)
	MOVUPS_YL1(-12 * SIZE, Y1, %xmm2)
	MOVUPS_YL1(-10 * SIZE, Y1, %xmm3)
	ALIGN_3

	movq	M,   I
	sarq	$2,  I
	jle	.L115

	movsd	-16 * SIZE(A1), %xmm4
	movhpd	-15 * SIZE(A1), %xmm4
	movsd	-14 * SIZE(A1), %xmm6
	movhpd	-13 * SIZE(A1), %xmm6
	movsd	-12 * SIZE(A1), %xmm8
	movhpd	-11 * SIZE(A1), %xmm8
	movsd	-10 * SIZE(A1), %xmm10
	movhpd	 -9 * SIZE(A1), %xmm10

	decq	 I
	jle	 .L114
	ALIGN_3

.L113:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(A2)
#endif

	pshufd	 $0x4e, %xmm4,  %xmm5
	mulpd	 %xmm12, %xmm4
	addpd	 %xmm4,  %xmm0
	movsd	-16 * SIZE(A2), %xmm4
	movhpd	-15 * SIZE(A2), %xmm4
	pshufd	 $0x4e, %xmm6,  %xmm7
	mulpd	 %xmm12, %xmm6
	addpd	 %xmm6,  %xmm1
	movsd	-14 * SIZE(A2), %xmm6
	movhpd	-13 * SIZE(A2), %xmm6

	pshufd	 $0x4e, %xmm8,  %xmm9
	mulpd	 %xmm12, %xmm8
	addpd	 %xmm8,  %xmm2
	movsd	-12 * SIZE(A2), %xmm8
	movhpd	-11 * SIZE(A2), %xmm8
	pshufd	 $0x4e, %xmm10, %xmm11
	mulpd	 %xmm12, %xmm10
	addpd	 %xmm10, %xmm3
	movsd	-10 * SIZE(A2), %xmm10
	movhpd	 -9 * SIZE(A2), %xmm10

	mulpd	 %xmm13, %xmm5
	SUBPD	 %xmm5,  %xmm0
	mulpd	 %xmm13, %xmm7
	SUBPD	 %xmm7,  %xmm1

	mulpd	 %xmm13, %xmm9
	SUBPD	 %xmm9,  %xmm2
	mulpd	 %xmm13, %xmm11
	SUBPD	 %xmm11, %xmm3

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(A1)
#endif

	pshufd	 $0x4e, %xmm4,  %xmm5
	mulpd	 %xmm14, %xmm4
	addpd	 %xmm4,  %xmm0
	movsd	 -8 * SIZE(A1), %xmm4
	movhpd	 -7 * SIZE(A1), %xmm4
	pshufd	 $0x4e, %xmm6,  %xmm7
	mulpd	 %xmm14, %xmm6
	addpd	 %xmm6,  %xmm1
	movsd	 -6 * SIZE(A1), %xmm6
	movhpd	 -5 * SIZE(A1), %xmm6

	pshufd	 $0x4e, %xmm8,  %xmm9
	mulpd	 %xmm14, %xmm8
	addpd	 %xmm8,  %xmm2
	movsd	 -4 * SIZE(A1), %xmm8
	movhpd	 -3 * SIZE(A1), %xmm8
	pshufd	 $0x4e, %xmm10, %xmm11
	mulpd	 %xmm14, %xmm10
	addpd	 %xmm10, %xmm3
	movsd	 -2 * SIZE(A1), %xmm10
	movhpd	 -1 * SIZE(A1), %xmm10

	mulpd	 %xmm15, %xmm5
	SUBPD	 %xmm5,  %xmm0
	mulpd	 %xmm15, %xmm7
	SUBPD	 %xmm7,  %xmm1

	mulpd	 %xmm15, %xmm9
	SUBPD	 %xmm9,  %xmm2
	mulpd	 %xmm15, %xmm11
	SUBPD	 %xmm11, %xmm3

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(Y1)
#endif

	MOVUPS_YS1(-16 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-14 * SIZE, Y1, %xmm1)
	MOVUPS_YS1(-12 * SIZE, Y1, %xmm2)
	MOVUPS_YS1(-10 * SIZE, Y1, %xmm3)

	MOVUPS_YL1( -8 * SIZE, Y1, %xmm0)
	MOVUPS_YL1( -6 * SIZE, Y1, %xmm1)
	MOVUPS_YL1( -4 * SIZE, Y1, %xmm2)
	MOVUPS_YL1( -2 * SIZE, Y1, %xmm3)

	subq	 $-8 * SIZE, A1
	subq	 $-8 * SIZE, A2
	subq	 $-8 * SIZE, Y1

	subq	 $1, I
	BRANCH
	jg	.L113
	ALIGN_3

.L114:
	pshufd	 $0x4e, %xmm4,  %xmm5
	mulpd	 %xmm12, %xmm4
	addpd	 %xmm4,  %xmm0
	movsd	-16 * SIZE(A2), %xmm4
	movhpd	-15 * SIZE(A2), %xmm4
	pshufd	 $0x4e, %xmm6,  %xmm7
	mulpd	 %xmm12, %xmm6
	addpd	 %xmm6,  %xmm1
	movsd	-14 * SIZE(A2), %xmm6
	movhpd	-13 * SIZE(A2), %xmm6

	pshufd	 $0x4e, %xmm8,  %xmm9
	mulpd	 %xmm12, %xmm8
	addpd	 %xmm8,  %xmm2
	movsd	-12 * SIZE(A2), %xmm8
	movhpd	-11 * SIZE(A2), %xmm8
	pshufd	 $0x4e, %xmm10, %xmm11
	mulpd	 %xmm12, %xmm10
	addpd	 %xmm10, %xmm3
	movsd	-10 * SIZE(A2), %xmm10
	movhpd	 -9 * SIZE(A2), %xmm10

	mulpd	 %xmm13, %xmm5
	SUBPD	 %xmm5,  %xmm0
	mulpd	 %xmm13, %xmm7
	SUBPD	 %xmm7,  %xmm1

	mulpd	 %xmm13, %xmm9
	SUBPD	 %xmm9,  %xmm2
	mulpd	 %xmm13, %xmm11
	SUBPD	 %xmm11, %xmm3

	pshufd	 $0x4e, %xmm4,  %xmm5
	mulpd	 %xmm14, %xmm4
	addpd	 %xmm4,  %xmm0
	pshufd	 $0x4e, %xmm6,  %xmm7
	mulpd	 %xmm14, %xmm6
	addpd	 %xmm6,  %xmm1

	pshufd	 $0x4e, %xmm8,  %xmm9
	mulpd	 %xmm14, %xmm8
	addpd	 %xmm8,  %xmm2
	pshufd	 $0x4e, %xmm10, %xmm11
	mulpd	 %xmm14, %xmm10
	addpd	 %xmm10, %xmm3

	mulpd	 %xmm15, %xmm5
	SUBPD	 %xmm5,  %xmm0
	mulpd	 %xmm15, %xmm7
	SUBPD	 %xmm7,  %xmm1

	mulpd	 %xmm15, %xmm9
	SUBPD	 %xmm9,  %xmm2
	mulpd	 %xmm15, %xmm11
	SUBPD	 %xmm11, %xmm3

	MOVUPS_YS1(-16 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-14 * SIZE, Y1, %xmm1)
	MOVUPS_YS1(-12 * SIZE, Y1, %xmm2)
	MOVUPS_YS1(-10 * SIZE, Y1, %xmm3)

	MOVUPS_YL1( -8 * SIZE, Y1, %xmm0)
	MOVUPS_YL1( -6 * SIZE, Y1, %xmm1)
	MOVUPS_YL1( -4 * SIZE, Y1, %xmm2)
	MOVUPS_YL1( -2 * SIZE, Y1, %xmm3)

	subq	 $-8 * SIZE, A1
	subq	 $-8 * SIZE, A2
	subq	 $-8 * SIZE, Y1
	ALIGN_3

.L115:
	testq	$2, M
	je	.L117

	movsd	-16 * SIZE(A1), %xmm4
	movhpd	-15 * SIZE(A1), %xmm4
	movsd	-14 * SIZE(A1), %xmm6
	movhpd	-13 * SIZE(A1), %xmm6

	movsd	-16 * SIZE(A2), %xmm8
	movhpd	-15 * SIZE(A2), %xmm8
	movsd	-14 * SIZE(A2), %xmm10
	movhpd	-13 * SIZE(A2), %xmm10

	pshufd	 $0x4e, %xmm4,  %xmm5
	mulpd	 %xmm12, %xmm4
	addpd	 %xmm4,  %xmm0
	pshufd	 $0x4e, %xmm6,  %xmm7
	mulpd	 %xmm12, %xmm6
	addpd	 %xmm6,  %xmm1

	mulpd	 %xmm13, %xmm5
	SUBPD	 %xmm5,  %xmm0
	mulpd	 %xmm13, %xmm7
	SUBPD	 %xmm7,  %xmm1

	pshufd	 $0x4e, %xmm8,  %xmm9
	mulpd	 %xmm14, %xmm8
	addpd	 %xmm8,  %xmm0
	pshufd	 $0x4e, %xmm10, %xmm11
	mulpd	 %xmm14, %xmm10
	addpd	 %xmm10, %xmm1

	mulpd	 %xmm15, %xmm9
	SUBPD	 %xmm9,  %xmm0
	mulpd	 %xmm15, %xmm11
	SUBPD	 %xmm11, %xmm1

	MOVUPS_YS1(-16 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-14 * SIZE, Y1, %xmm1)
	movapd	 %xmm2, %xmm0

	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, A2
	addq	 $4 * SIZE, Y1
	ALIGN_3

.L117:
	testq	$1, M
#if GEMV_UNROLL == 2
	je	.L119
#else
	je	.L120
#endif

	movsd	-16 * SIZE(A1), %xmm4
	movhpd	-15 * SIZE(A1), %xmm4
	movsd	-16 * SIZE(A2), %xmm6
	movhpd	-15 * SIZE(A2), %xmm6

	pshufd	 $0x4e, %xmm4, %xmm5
	mulpd	 %xmm12, %xmm4
	addpd	 %xmm4,  %xmm0
	mulpd	 %xmm13, %xmm5
	SUBPD	 %xmm5,  %xmm0

	pshufd	 $0x4e, %xmm6, %xmm7
	mulpd	 %xmm14, %xmm6
	addpd	 %xmm6,  %xmm0
	mulpd	 %xmm15, %xmm7
	SUBPD	 %xmm7,  %xmm0

	MOVUPS_YS1(-16 * SIZE, Y1, %xmm0)

#if GEMV_UNROLL == 2
	ALIGN_3

.L119:
	cmpq	$2, N
	jge	.L111
#endif
	ALIGN_3

.L120:
#endif

	cmpq	$1, N
	jl	.L980

#if GEMV_UNROLL == 1
.L121:
	decq	N
#endif

	leaq	16 * SIZE(BUFFER), Y1
	movq	A,  A1
#if GEMV_UNROLL == 1
	addq	LDA, A
#endif

	movsd	0 * SIZE(X), %xmm12
	movhpd	1 * SIZE(X), %xmm12
	addq	INCX, X

	pcmpeqb	%xmm11, %xmm11
	psllq	$63,    %xmm11
	shufps	$0xc0, %xmm11, %xmm11

	pshufd	$0x4e, %xmm12, %xmm13

#ifdef HAVE_SSE3
	movddup	ALPHA_R, %xmm8
	movddup	ALPHA_I, %xmm9
#else
	movsd	ALPHA_R, %xmm8
	unpcklpd %xmm8, %xmm8
	movsd	ALPHA_I, %xmm9
	unpcklpd %xmm9, %xmm9
#endif

	xorpd	 %xmm11, %xmm13

	mulpd	 %xmm8, %xmm12
	mulpd	 %xmm9, %xmm13

#ifndef XCONJ
	subpd	 %xmm13, %xmm12
#else
	addpd	 %xmm13, %xmm12
#endif

	pshufd	 $0xee, %xmm12, %xmm13
	pshufd	 $0x44, %xmm12, %xmm12

#ifndef CONJ
	xorpd	 %xmm11, %xmm13
#else
	xorpd	 %xmm11, %xmm12
#endif

	MOVUPS_YL1(-16 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-14 * SIZE, Y1, %xmm1)
	MOVUPS_YL1(-12 * SIZE, Y1, %xmm2)
	MOVUPS_YL1(-10 * SIZE, Y1, %xmm3)

	movq	M,   I
	sarq	$2,  I
	jle	.L125

	movsd	-16 * SIZE(A1), %xmm4
	movhpd	-15 * SIZE(A1), %xmm4
	movsd	-14 * SIZE(A1), %xmm6
	movhpd	-13 * SIZE(A1), %xmm6
	movsd	-12 * SIZE(A1), %xmm8
	movhpd	-11 * SIZE(A1), %xmm8
	movsd	-10 * SIZE(A1), %xmm10
	movhpd	 -9 * SIZE(A1), %xmm10

	decq	 I
	jle	 .L124
	ALIGN_3

.L123:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 4 - 128 + PREOFFSET(A1)
#endif

	pshufd	 $0x4e, %xmm4,  %xmm5
	mulpd	 %xmm12, %xmm4
	addpd	 %xmm4,  %xmm0
	movsd	 -8 * SIZE(A1), %xmm4
	movhpd	 -7 * SIZE(A1), %xmm4
	pshufd	 $0x4e, %xmm6,  %xmm7
	mulpd	 %xmm12, %xmm6
	addpd	 %xmm6,  %xmm1
	movsd	 -6 * SIZE(A1), %xmm6
	movhpd	 -5 * SIZE(A1), %xmm6

	pshufd	 $0x4e, %xmm8,  %xmm9
	mulpd	 %xmm12, %xmm8
	addpd	 %xmm8,  %xmm2
	movsd	 -4 * SIZE(A1), %xmm8
	movhpd	 -3 * SIZE(A1), %xmm8
	pshufd	 $0x4e, %xmm10, %xmm11
	mulpd	 %xmm12, %xmm10
	addpd	 %xmm10, %xmm3
	movsd	 -2 * SIZE(A1), %xmm10
	movhpd	 -1 * SIZE(A1), %xmm10

	mulpd	 %xmm13, %xmm5
	SUBPD	 %xmm5,  %xmm0
	mulpd	 %xmm13, %xmm7
	SUBPD	 %xmm7,  %xmm1

	mulpd	 %xmm13, %xmm9
	SUBPD	 %xmm9,  %xmm2
	mulpd	 %xmm13, %xmm11
	SUBPD	 %xmm11, %xmm3

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE) * 4 - 128 + PREOFFSET(Y1)
#endif

	MOVUPS_YS1(-16 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-14 * SIZE, Y1, %xmm1)
	MOVUPS_YS1(-12 * SIZE, Y1, %xmm2)
	MOVUPS_YS1(-10 * SIZE, Y1, %xmm3)

	MOVUPS_YL1( -8 * SIZE, Y1, %xmm0)
	MOVUPS_YL1( -6 * SIZE, Y1, %xmm1)
	MOVUPS_YL1( -4 * SIZE, Y1, %xmm2)
	MOVUPS_YL1( -2 * SIZE, Y1, %xmm3)

	subq	 $-8 * SIZE, A1
	subq	 $-8 * SIZE, Y1

	subq	 $1, I
	BRANCH
	jg	.L123
	ALIGN_3

.L124:
	pshufd	 $0x4e, %xmm4,  %xmm5
	mulpd	 %xmm12, %xmm4
	addpd	 %xmm4,  %xmm0
	pshufd	 $0x4e, %xmm6,  %xmm7
	mulpd	 %xmm12, %xmm6
	addpd	 %xmm6,  %xmm1

	pshufd	 $0x4e, %xmm8,  %xmm9
	mulpd	 %xmm12, %xmm8
	addpd	 %xmm8,  %xmm2
	pshufd	 $0x4e, %xmm10, %xmm11
	mulpd	 %xmm12, %xmm10
	addpd	 %xmm10, %xmm3

	mulpd	 %xmm13, %xmm5
	SUBPD	 %xmm5,  %xmm0
	mulpd	 %xmm13, %xmm7
	SUBPD	 %xmm7,  %xmm1

	mulpd	 %xmm13, %xmm9
	SUBPD	 %xmm9,  %xmm2
	mulpd	 %xmm13, %xmm11
	SUBPD	 %xmm11, %xmm3

	MOVUPS_YS1(-16 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-14 * SIZE, Y1, %xmm1)
	MOVUPS_YS1(-12 * SIZE, Y1, %xmm2)
	MOVUPS_YS1(-10 * SIZE, Y1, %xmm3)

	MOVUPS_YL1( -8 * SIZE, Y1, %xmm0)
	MOVUPS_YL1( -6 * SIZE, Y1, %xmm1)
	MOVUPS_YL1( -4 * SIZE, Y1, %xmm2)
	MOVUPS_YL1( -2 * SIZE, Y1, %xmm3)

	subq	 $-8 * SIZE, A1
	subq	 $-8 * SIZE, Y1
	ALIGN_3

.L125:
	testq	$2, M
	je	.L127


	movsd	-16 * SIZE(A1), %xmm4
	movhpd	-15 * SIZE(A1), %xmm4
	movsd	-14 * SIZE(A1), %xmm6
	movhpd	-13 * SIZE(A1), %xmm6

	pshufd	 $0x4e, %xmm4,  %xmm5
	mulpd	 %xmm12, %xmm4
	addpd	 %xmm4,  %xmm0
	pshufd	 $0x4e, %xmm6,  %xmm7
	mulpd	 %xmm12, %xmm6
	addpd	 %xmm6,  %xmm1

	mulpd	 %xmm13, %xmm5
	SUBPD	 %xmm5,  %xmm0
	mulpd	 %xmm13, %xmm7
	SUBPD	 %xmm7,  %xmm1

	MOVUPS_YS1(-16 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-14 * SIZE, Y1, %xmm1)
	movapd	 %xmm2, %xmm0

	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, Y1
	ALIGN_3

.L127:
	testq	$1, M
#if GEMV_UNROLL == 1
	je	.L129
#else
	je	.L980
#endif

	movsd	-16 * SIZE(A1), %xmm4
	movhpd	-15 * SIZE(A1), %xmm4

	pshufd	 $0x4e, %xmm4, %xmm5
	mulpd	 %xmm12, %xmm4
	addpd	 %xmm4,  %xmm0
	mulpd	 %xmm13, %xmm5
	SUBPD	 %xmm5,  %xmm0

	MOVUPS_YS1(-16 * SIZE, Y1, %xmm0)

#if GEMV_UNROLL == 1
	ALIGN_3
.L129:
	cmpq	$1, N
	jge	.L121
#endif


#endif
	ALIGN_3

.L980:
	testq	$SIZE, Y
	jne	.L990

	movq	Y,  Y1

	movq	M,  %rax
	sarq	$3, %rax
	jle	.L184
	ALIGN_3

.L182:
 	movapd	 (Y), %xmm0
	addq	INCY, Y
	movapd	 (Y), %xmm1
	addq	INCY, Y
	movapd	 (Y), %xmm2
	addq	INCY, Y
	movapd	 (Y), %xmm3
	addq	INCY, Y
 	movapd	 (Y), %xmm4
	addq	INCY, Y
	movapd	 (Y), %xmm5
	addq	INCY, Y
	movapd	 (Y), %xmm6
	addq	INCY, Y
	movapd	 (Y), %xmm7
	addq	INCY, Y

	addpd	 0 * SIZE(BUFFER), %xmm0
	addpd	 2 * SIZE(BUFFER), %xmm1
	addpd	 4 * SIZE(BUFFER), %xmm2
	addpd	 6 * SIZE(BUFFER), %xmm3
	addpd	 8 * SIZE(BUFFER), %xmm4
	addpd	10 * SIZE(BUFFER), %xmm5
	addpd	12 * SIZE(BUFFER), %xmm6
	addpd	14 * SIZE(BUFFER), %xmm7

	movapd	%xmm0,  (Y1)
	addq	INCY, Y1
	movapd	%xmm1,  (Y1)
	addq	INCY, Y1
	movapd	%xmm2,  (Y1)
	addq	INCY, Y1
	movapd	%xmm3,  (Y1)
	addq	INCY, Y1
	movapd	%xmm4,  (Y1)
	addq	INCY, Y1
	movapd	%xmm5,  (Y1)
	addq	INCY, Y1
	movapd	%xmm6,  (Y1)
	addq	INCY, Y1
	movapd	%xmm7,  (Y1)
	addq	INCY, Y1

	subq	$-16 * SIZE, BUFFER
	decq	%rax
	jg	.L182
	ALIGN_3

.L184:
	testq	$7, M
	jle	.L999

	testq	$4, M
	jle	.L185

 	movapd	 (Y), %xmm0
	addq	INCY, Y
	movapd	 (Y), %xmm1
	addq	INCY, Y
	movapd	 (Y), %xmm2
	addq	INCY, Y
	movapd	 (Y), %xmm3
	addq	INCY, Y

	addpd	 0 * SIZE(BUFFER), %xmm0
	addpd	 2 * SIZE(BUFFER), %xmm1
	addpd	 4 * SIZE(BUFFER), %xmm2
	addpd	 6 * SIZE(BUFFER), %xmm3

	movapd	%xmm0,  (Y1)
	addq	INCY, Y1
	movapd	%xmm1,  (Y1)
	addq	INCY, Y1
	movapd	%xmm2,  (Y1)
	addq	INCY, Y1
	movapd	%xmm3,  (Y1)
	addq	INCY, Y1

	addq	$8 * SIZE, BUFFER
	ALIGN_3

.L185:
	testq	$2, M
	jle	.L186

 	movapd	 (Y), %xmm0
	addq	INCY, Y
	movapd	 (Y), %xmm1
	addq	INCY, Y
	addpd	 0 * SIZE(BUFFER), %xmm0
	addpd	 2 * SIZE(BUFFER), %xmm1

	movapd	%xmm0,  (Y1)
	addq	INCY, Y1
	movapd	%xmm1,  (Y1)
	addq	INCY, Y1

	addq	$4 * SIZE, BUFFER
	ALIGN_3

.L186:
	testq	$1, M
	jle	.L999

 	movapd	 (Y), %xmm0

	addpd	 (BUFFER), %xmm0

	movapd	%xmm0,  (Y1)
	jmp	.L999
	ALIGN_3

.L990:
	movq	Y,  Y1

	movq	M,  %rax
	sarq	$3, %rax
	jle	.L994
	ALIGN_3

.L992:
 	movsd	 0 * SIZE(Y), %xmm0
	movhpd	 1 * SIZE(Y), %xmm0
	addq	INCY, Y

	movsd	 0 * SIZE(Y), %xmm1
	movhpd	 1 * SIZE(Y), %xmm1
	addq	INCY, Y

	movsd	 0 * SIZE(Y), %xmm2
	movhpd	 1 * SIZE(Y), %xmm2
	addq	INCY, Y

	movsd	 0 * SIZE(Y), %xmm3
	movhpd	 1 * SIZE(Y), %xmm3
	addq	INCY, Y

 	movsd	 0 * SIZE(Y), %xmm4
	movhpd	 1 * SIZE(Y), %xmm4
	addq	INCY, Y

	movsd	 0 * SIZE(Y), %xmm5
	movhpd	 1 * SIZE(Y), %xmm5
	addq	INCY, Y

	movsd	 0 * SIZE(Y), %xmm6
	movhpd	 1 * SIZE(Y), %xmm6
	addq	INCY, Y

	movsd	 0 * SIZE(Y), %xmm7
	movhpd	 1 * SIZE(Y), %xmm7
	addq	INCY, Y

	addpd	 0 * SIZE(BUFFER), %xmm0
	addpd	 2 * SIZE(BUFFER), %xmm1
	addpd	 4 * SIZE(BUFFER), %xmm2
	addpd	 6 * SIZE(BUFFER), %xmm3
	addpd	 8 * SIZE(BUFFER), %xmm4
	addpd	10 * SIZE(BUFFER), %xmm5
	addpd	12 * SIZE(BUFFER), %xmm6
	addpd	14 * SIZE(BUFFER), %xmm7

	movlpd	%xmm0,  0 * SIZE(Y1)
	movhpd	%xmm0,  1 * SIZE(Y1)
	addq	INCY, Y1

	movlpd	%xmm1,  0 * SIZE(Y1)
	movhpd	%xmm1,  1 * SIZE(Y1)
	addq	INCY, Y1

	movlpd	%xmm2,  0 * SIZE(Y1)
	movhpd	%xmm2,  1 * SIZE(Y1)
	addq	INCY, Y1

	movlpd	%xmm3,  0 * SIZE(Y1)
	movhpd	%xmm3,  1 * SIZE(Y1)
	addq	INCY, Y1

	movlpd	%xmm4,  0 * SIZE(Y1)
	movhpd	%xmm4,  1 * SIZE(Y1)
	addq	INCY, Y1

	movlpd	%xmm5,  0 * SIZE(Y1)
	movhpd	%xmm5,  1 * SIZE(Y1)
	addq	INCY, Y1

	movlpd	%xmm6,  0 * SIZE(Y1)
	movhpd	%xmm6,  1 * SIZE(Y1)
	addq	INCY, Y1

	movlpd	%xmm7,  0 * SIZE(Y1)
	movhpd	%xmm7,  1 * SIZE(Y1)
	addq	INCY, Y1

	subq	$-16 * SIZE, BUFFER
	decq	%rax
	jg	.L992
	ALIGN_3

.L994:
	testq	$7, M
	jle	.L999

	testq	$4, M
	jle	.L995

 	movsd	 0 * SIZE(Y), %xmm0
	movhpd	 1 * SIZE(Y), %xmm0
	addq	INCY, Y

	movsd	 0 * SIZE(Y), %xmm1
	movhpd	 1 * SIZE(Y), %xmm1
	addq	INCY, Y

	movsd	 0 * SIZE(Y), %xmm2
	movhpd	 1 * SIZE(Y), %xmm2
	addq	INCY, Y

	movsd	 0 * SIZE(Y), %xmm3
	movhpd	 1 * SIZE(Y), %xmm3
	addq	INCY, Y

	addpd	 0 * SIZE(BUFFER), %xmm0
	addpd	 2 * SIZE(BUFFER), %xmm1
	addpd	 4 * SIZE(BUFFER), %xmm2
	addpd	 6 * SIZE(BUFFER), %xmm3

	movlpd	%xmm0,  0 * SIZE(Y1)
	movhpd	%xmm0,  1 * SIZE(Y1)
	addq	INCY, Y1

	movlpd	%xmm1,  0 * SIZE(Y1)
	movhpd	%xmm1,  1 * SIZE(Y1)
	addq	INCY, Y1

	movlpd	%xmm2,  0 * SIZE(Y1)
	movhpd	%xmm2,  1 * SIZE(Y1)
	addq	INCY, Y1

	movlpd	%xmm3,  0 * SIZE(Y1)
	movhpd	%xmm3,  1 * SIZE(Y1)
	addq	INCY, Y1

	addq	$8 * SIZE, BUFFER
	ALIGN_3

.L995:
	testq	$2, M
	jle	.L996

 	movsd	 0 * SIZE(Y), %xmm0
	movhpd	 1 * SIZE(Y), %xmm0
	addq	INCY, Y

	movsd	 0 * SIZE(Y), %xmm1
	movhpd	 1 * SIZE(Y), %xmm1
	addq	INCY, Y

	addpd	 0 * SIZE(BUFFER), %xmm0
	addpd	 2 * SIZE(BUFFER), %xmm1

	movlpd	%xmm0,  0 * SIZE(Y1)
	movhpd	%xmm0,  1 * SIZE(Y1)
	addq	INCY, Y1

	movlpd	%xmm1,  0 * SIZE(Y1)
	movhpd	%xmm1,  1 * SIZE(Y1)
	addq	INCY, Y1

	addq	$4 * SIZE, BUFFER
	ALIGN_3

.L996:
	testq	$1, M
	jle	.L999

 	movsd	 0 * SIZE(Y), %xmm0
	movhpd	 1 * SIZE(Y), %xmm0

	addpd	 0 * SIZE(BUFFER), %xmm0

	movlpd	%xmm0,  0 * SIZE(Y1)
	movhpd	%xmm0,  1 * SIZE(Y1)
	ALIGN_3

.L999:
	movq	M, I
	salq	$ZBASE_SHIFT,I
	addq	I,AA
	jmp	.L0t
.L999x:

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
