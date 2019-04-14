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

#define MMM	64(%rsp)
#define NN	72(%rsp)
#define AA	80(%rsp)
#define LDAX	88(%rsp)
#define ALPHAR	96(%rsp)
#define ALPHAI	104(%rsp)

#define M	  %rdi
#define N	  %rsi
#define A	  %rcx
#define LDA	  %r8
#define X	  %r9
#define INCX	  %rdx
#define Y	  %rbp
#define INCY	  %r10

#else

#define STACKSIZE	288

#define OLD_ALPHA_I	 40 + STACKSIZE(%rsp)
#define OLD_A		 48 + STACKSIZE(%rsp)
#define OLD_LDA		 56 + STACKSIZE(%rsp)
#define OLD_X		 64 + STACKSIZE(%rsp)
#define OLD_INCX	 72 + STACKSIZE(%rsp)
#define OLD_Y		 80 + STACKSIZE(%rsp)
#define OLD_INCY	 88 + STACKSIZE(%rsp)
#define OLD_BUFFER	 96 + STACKSIZE(%rsp)

#define MMM	232(%rsp)
#define NN	240(%rsp)
#define AA	248(%rsp)
#define LDAX 256(%rsp)
#define ALPHAR	264(%rsp)
#define ALPHAI	272(%rsp)

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
#define J	%rbx
#define A1	%r11
#define A2	%r12

#define X1	%r13
#define Y1	%r14
#define BUFFER	%r15

#define ALPHA_R	 %xmm14
#define ALPHA_I	 %xmm15

#undef SUBPD

#ifndef CONJ
#define SUBPD	   addpd
#else
#define SUBPD	   subpd
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
	movsd	%xmm0,ALPHAR
	movsd	%xmm1,ALPHAI

.L0t:
	xorq	I,I
	addq	$1,I
	salq	$19,I
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

	movq	OLD_INCX,  INCX
	movq	OLD_Y,     Y
	movq	OLD_INCY,  INCY
	movq	OLD_BUFFER, BUFFER

	salq	$ZBASE_SHIFT,   LDA
	salq	$ZBASE_SHIFT,   INCX
	salq	$ZBASE_SHIFT,   INCY

#ifdef HAVE_SSE3
	movddup	%xmm0, ALPHA_R
	movddup	%xmm1, ALPHA_I
#else
	pshufd	$0x44, %xmm0, ALPHA_R
	pshufd	$0x44, %xmm1, ALPHA_I
#endif

	subq	$-16 * SIZE, A

	testq	M, M
	jle	.L999
	testq	N, N
	jle	.L999
	ALIGN_3

	movq	BUFFER, X1

	movq	Y, Y1

	movq	M,  I
	sarq	$2, I
	jle	.L05
	ALIGN_4

.L02:
	movsd	 0 * SIZE(X), %xmm0
	movhpd	 1 * SIZE(X), %xmm0
	addq	INCX, X

	movsd	 0 * SIZE(X), %xmm1
	movhpd	 1 * SIZE(X), %xmm1
	addq	INCX, X

	movsd	 0 * SIZE(X), %xmm2
	movhpd	 1 * SIZE(X), %xmm2
	addq	INCX, X

	movsd	 0 * SIZE(X), %xmm3
	movhpd	 1 * SIZE(X), %xmm3
	addq	INCX, X

	movapd	%xmm0,  0 * SIZE(X1)
	movapd	%xmm1,  2 * SIZE(X1)
	movapd	%xmm2,  4 * SIZE(X1)
	movapd	%xmm3,  6 * SIZE(X1)

	addq	$8 * SIZE, X1
	decq	I
	jg	.L02
	ALIGN_4

.L05:
	movq	M,  I
	andq	$3, I
	jle	.L10
	ALIGN_2

.L06:
	movsd	 0 * SIZE(X), %xmm0
	movhpd	 1 * SIZE(X), %xmm0
	addq	INCX, X
	movapd	%xmm0, 0 * SIZE(X1)
	addq	$2 * SIZE, X1
	decq	I
	jg	.L06
	ALIGN_4

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

	leaq	16 * SIZE(BUFFER), X1

	movq	A, A1
	leaq	(A1, LDA, 2), A2
	leaq	(A1, LDA, 4), A

	MOVUPS_XL1(-16 * SIZE, X1, %xmm12)
	xorpd	%xmm0, %xmm0
	xorpd	%xmm1, %xmm1
	xorpd	%xmm2, %xmm2
	xorpd	%xmm3, %xmm3
	MOVUPS_XL1(-14 * SIZE, X1, %xmm13)
	xorpd	%xmm4, %xmm4
	xorpd	%xmm5, %xmm5
	xorpd	%xmm6, %xmm6
	xorpd	%xmm7, %xmm7

#ifdef PREFETCHW
	PREFETCHW	3 * SIZE(Y1)
#endif

	movq	M,   I
	sarq	$2,  I
	jle	.L15

	MOVUPS_A1(-16 * SIZE, A1, %xmm8)
	MOVUPS_A2(-16 * SIZE, A1, LDA, 1, %xmm10)

	decq	 I
	jle	 .L14
	ALIGN_3

.L13:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A1)
#endif

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm12,  %xmm8
	addpd	%xmm8,  %xmm0
	MOVUPS_A1(-16 * SIZE, A2, %xmm8)
	mulpd	%xmm12, %xmm9
	SUBPD	%xmm9,  %xmm1

	pshufd	$0x4e, %xmm10, %xmm11
	mulpd	%xmm12, %xmm10
	addpd	%xmm10, %xmm2
	MOVUPS_A2(-16 * SIZE, A2, LDA, 1, %xmm10)
	mulpd	%xmm12, %xmm11
	SUBPD	%xmm11, %xmm3

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm12, %xmm8
	addpd	%xmm8,  %xmm4
	MOVUPS_A1(-14 * SIZE, A1, %xmm8)
	mulpd	%xmm12, %xmm9
	SUBPD	%xmm9,  %xmm5

	pshufd	$0x4e, %xmm10, %xmm11
	mulpd	%xmm12, %xmm10
	addpd	%xmm10, %xmm6
	MOVUPS_A2(-14 * SIZE, A1, LDA, 1, %xmm10)
	mulpd	%xmm12, %xmm11
	MOVUPS_XL1(-12 * SIZE, X1, %xmm12)
	SUBPD	%xmm11, %xmm7

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A1, LDA)
#endif

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm13,  %xmm8
	addpd	%xmm8,  %xmm0
	MOVUPS_A1(-14 * SIZE, A2, %xmm8)
	mulpd	%xmm13, %xmm9
	SUBPD	%xmm9,  %xmm1

	pshufd	$0x4e, %xmm10, %xmm11
	mulpd	%xmm13, %xmm10
	addpd	%xmm10, %xmm2
	MOVUPS_A2(-14 * SIZE, A2, LDA, 1, %xmm10)
	mulpd	%xmm13, %xmm11
	SUBPD	%xmm11, %xmm3

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm13, %xmm8
	addpd	%xmm8,  %xmm4
	MOVUPS_A1(-12 * SIZE, A1, %xmm8)
	mulpd	%xmm13, %xmm9
	SUBPD	%xmm9,  %xmm5

	pshufd	$0x4e, %xmm10, %xmm11
	mulpd	%xmm13, %xmm10
	addpd	%xmm10, %xmm6
	MOVUPS_A2(-12 * SIZE, A1, LDA, 1, %xmm10)
	mulpd	%xmm13, %xmm11
	MOVUPS_XL1(-10 * SIZE, X1, %xmm13)
	SUBPD	%xmm11, %xmm7

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A2)
#endif

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm12,  %xmm8
	addpd	%xmm8,  %xmm0
	MOVUPS_A1(-12 * SIZE, A2, %xmm8)
	mulpd	%xmm12, %xmm9
	SUBPD	%xmm9,  %xmm1

	pshufd	$0x4e, %xmm10, %xmm11
	mulpd	%xmm12, %xmm10
	addpd	%xmm10, %xmm2
	MOVUPS_A2(-12 * SIZE, A2, LDA, 1, %xmm10)
	mulpd	%xmm12, %xmm11
	SUBPD	%xmm11, %xmm3

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm12, %xmm8
	addpd	%xmm8,  %xmm4
	MOVUPS_A1(-10 * SIZE, A1, %xmm8)
	mulpd	%xmm12, %xmm9
	SUBPD	%xmm9,  %xmm5

	pshufd	$0x4e, %xmm10, %xmm11
	mulpd	%xmm12, %xmm10
	addpd	%xmm10, %xmm6
	MOVUPS_A2(-10 * SIZE, A1, LDA, 1, %xmm10)
	mulpd	%xmm12, %xmm11
	MOVUPS_XL1( -8 * SIZE, X1, %xmm12)
	SUBPD	%xmm11, %xmm7

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A2, LDA)
#endif

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm13,  %xmm8
	addpd	%xmm8,  %xmm0
	MOVUPS_A1(-10 * SIZE, A2, %xmm8)
	mulpd	%xmm13, %xmm9
	SUBPD	%xmm9,  %xmm1

	pshufd	$0x4e, %xmm10, %xmm11
	mulpd	%xmm13, %xmm10
	addpd	%xmm10, %xmm2
	MOVUPS_A2(-10 * SIZE, A2, LDA, 1, %xmm10)
	mulpd	%xmm13, %xmm11
	SUBPD	%xmm11, %xmm3

#ifdef PREFETCHW
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(X1)
#endif

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm13, %xmm8
	addpd	%xmm8,  %xmm4
	MOVUPS_A1( -8 * SIZE, A1, %xmm8)
	mulpd	%xmm13, %xmm9
	SUBPD	%xmm9,  %xmm5

	pshufd	$0x4e, %xmm10, %xmm11
	mulpd	%xmm13, %xmm10
	addpd	%xmm10, %xmm6
	MOVUPS_A2( -8 * SIZE, A1, LDA, 1, %xmm10)
	mulpd	%xmm13, %xmm11
	MOVUPS_XL1( -6 * SIZE, X1, %xmm13)
	SUBPD	%xmm11, %xmm7

	subq	 $-8 * SIZE, A1
	subq	 $-8 * SIZE, A2
	subq	 $-8 * SIZE, X1

	subq	 $1, I
	BRANCH
	jg	.L13
	ALIGN_3

.L14:
	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm12,  %xmm8
	addpd	%xmm8,  %xmm0
	MOVUPS_A1(-16 * SIZE, A2, %xmm8)
	mulpd	%xmm12, %xmm9
	SUBPD	%xmm9,  %xmm1

	pshufd	$0x4e, %xmm10, %xmm11
	mulpd	%xmm12, %xmm10
	addpd	%xmm10, %xmm2
	MOVUPS_A2(-16 * SIZE, A2, LDA, 1, %xmm10)
	mulpd	%xmm12, %xmm11
	SUBPD	%xmm11, %xmm3

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm12, %xmm8
	addpd	%xmm8,  %xmm4
	MOVUPS_A1(-14 * SIZE, A1, %xmm8)
	mulpd	%xmm12, %xmm9
	SUBPD	%xmm9,  %xmm5

	pshufd	$0x4e, %xmm10, %xmm11
	mulpd	%xmm12, %xmm10
	addpd	%xmm10, %xmm6
	MOVUPS_A2(-14 * SIZE, A1, LDA, 1, %xmm10)
	mulpd	%xmm12, %xmm11
	MOVUPS_XL1(-12 * SIZE, X1, %xmm12)
	SUBPD	%xmm11, %xmm7

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm13,  %xmm8
	addpd	%xmm8,  %xmm0
	MOVUPS_A1(-14 * SIZE, A2, %xmm8)
	mulpd	%xmm13, %xmm9
	SUBPD	%xmm9,  %xmm1

	pshufd	$0x4e, %xmm10, %xmm11
	mulpd	%xmm13, %xmm10
	addpd	%xmm10, %xmm2
	MOVUPS_A2(-14 * SIZE, A2, LDA, 1, %xmm10)
	mulpd	%xmm13, %xmm11
	SUBPD	%xmm11, %xmm3

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm13, %xmm8
	addpd	%xmm8,  %xmm4
	MOVUPS_A1(-12 * SIZE, A1, %xmm8)
	mulpd	%xmm13, %xmm9
	SUBPD	%xmm9,  %xmm5

	pshufd	$0x4e, %xmm10, %xmm11
	mulpd	%xmm13, %xmm10
	addpd	%xmm10, %xmm6
	MOVUPS_A2(-12 * SIZE, A1, LDA, 1, %xmm10)
	mulpd	%xmm13, %xmm11
	MOVUPS_XL1(-10 * SIZE, X1, %xmm13)
	SUBPD	%xmm11, %xmm7

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm12,  %xmm8
	addpd	%xmm8,  %xmm0
	MOVUPS_A1(-12 * SIZE, A2, %xmm8)
	mulpd	%xmm12, %xmm9
	SUBPD	%xmm9,  %xmm1

	pshufd	$0x4e, %xmm10, %xmm11
	mulpd	%xmm12, %xmm10
	addpd	%xmm10, %xmm2
	MOVUPS_A2(-12 * SIZE, A2, LDA, 1, %xmm10)
	mulpd	%xmm12, %xmm11
	SUBPD	%xmm11, %xmm3

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm12, %xmm8
	addpd	%xmm8,  %xmm4
	MOVUPS_A1(-10 * SIZE, A1, %xmm8)
	mulpd	%xmm12, %xmm9
	SUBPD	%xmm9,  %xmm5

	pshufd	$0x4e, %xmm10, %xmm11
	mulpd	%xmm12, %xmm10
	addpd	%xmm10, %xmm6
	MOVUPS_A2(-10 * SIZE, A1, LDA, 1, %xmm10)
	mulpd	%xmm12, %xmm11
	MOVUPS_XL1( -8 * SIZE, X1, %xmm12)
	SUBPD	%xmm11, %xmm7

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm13,  %xmm8
	addpd	%xmm8,  %xmm0
	MOVUPS_A1(-10 * SIZE, A2, %xmm8)
	mulpd	%xmm13, %xmm9
	SUBPD	%xmm9,  %xmm1

	pshufd	$0x4e, %xmm10, %xmm11
	mulpd	%xmm13, %xmm10
	addpd	%xmm10, %xmm2
	MOVUPS_A2(-10 * SIZE, A2, LDA, 1, %xmm10)
	mulpd	%xmm13, %xmm11
	SUBPD	%xmm11, %xmm3

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm13, %xmm8
	addpd	%xmm8,  %xmm4
	mulpd	%xmm13, %xmm9
	SUBPD	%xmm9,  %xmm5

	pshufd	$0x4e, %xmm10, %xmm11
	mulpd	%xmm13, %xmm10
	addpd	%xmm10, %xmm6
	mulpd	%xmm13, %xmm11
	MOVUPS_XL1( -6 * SIZE, X1, %xmm13)
	SUBPD	%xmm11, %xmm7

	subq	 $-8 * SIZE, A1
	subq	 $-8 * SIZE, A2
	subq	 $-8 * SIZE, X1
	ALIGN_3

.L15:
	testq	$2, M
	je	.L17

	MOVUPS_A1(-16 * SIZE, A1, %xmm8)
	MOVUPS_A2(-16 * SIZE, A1, LDA, 1, %xmm10)

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm12,  %xmm8
	addpd	%xmm8,  %xmm0
	MOVUPS_A1(-16 * SIZE, A2, %xmm8)
	mulpd	%xmm12, %xmm9
	SUBPD	%xmm9,  %xmm1

	pshufd	$0x4e, %xmm10, %xmm11
	mulpd	%xmm12, %xmm10
	addpd	%xmm10, %xmm2
	MOVUPS_A2(-16 * SIZE, A2, LDA, 1, %xmm10)
	mulpd	%xmm12, %xmm11
	SUBPD	%xmm11, %xmm3

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm12, %xmm8
	addpd	%xmm8,  %xmm4
	MOVUPS_A1(-14 * SIZE, A1, %xmm8)
	mulpd	%xmm12, %xmm9
	SUBPD	%xmm9,  %xmm5

	pshufd	$0x4e, %xmm10, %xmm11
	mulpd	%xmm12, %xmm10
	addpd	%xmm10, %xmm6
	MOVUPS_A2(-14 * SIZE, A1, LDA, 1, %xmm10)
	mulpd	%xmm12, %xmm11
	MOVUPS_XL1(-12 * SIZE, X1, %xmm12)
	SUBPD	%xmm11, %xmm7

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm13,  %xmm8
	addpd	%xmm8,  %xmm0
	MOVUPS_A1(-14 * SIZE, A2, %xmm8)
	mulpd	%xmm13, %xmm9
	SUBPD	%xmm9,  %xmm1

	pshufd	$0x4e, %xmm10, %xmm11
	mulpd	%xmm13, %xmm10
	addpd	%xmm10, %xmm2
	MOVUPS_A2(-14 * SIZE, A2, LDA, 1, %xmm10)
	mulpd	%xmm13, %xmm11
	SUBPD	%xmm11, %xmm3

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm13, %xmm8
	addpd	%xmm8,  %xmm4
	mulpd	%xmm13, %xmm9
	SUBPD	%xmm9,  %xmm5

	pshufd	$0x4e, %xmm10, %xmm11
	mulpd	%xmm13, %xmm10
	addpd	%xmm10, %xmm6
	mulpd	%xmm13, %xmm11
	SUBPD	%xmm11, %xmm7

	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, A2
	ALIGN_3

.L17:
	testq	$1, M
	je	.L19

	MOVUPS_A1(-16 * SIZE, A1, %xmm8)
	MOVUPS_A2(-16 * SIZE, A1, LDA, 1, %xmm10)

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm12,  %xmm8
	addpd	%xmm8,  %xmm0
	MOVUPS_A1(-16 * SIZE, A2, %xmm8)
	mulpd	%xmm12, %xmm9
	SUBPD	%xmm9,  %xmm1

	pshufd	$0x4e, %xmm10, %xmm11
	mulpd	%xmm12, %xmm10
	addpd	%xmm10, %xmm2
	MOVUPS_A2(-16 * SIZE, A2, LDA, 1, %xmm10)
	mulpd	%xmm12, %xmm11
	SUBPD	%xmm11, %xmm3

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm12, %xmm8
	addpd	%xmm8,  %xmm4
	mulpd	%xmm12, %xmm9
	SUBPD	%xmm9,  %xmm5

	pshufd	$0x4e, %xmm10, %xmm11
	mulpd	%xmm12, %xmm10
	addpd	%xmm10, %xmm6
	mulpd	%xmm12, %xmm11
	SUBPD	%xmm11, %xmm7
	ALIGN_3

.L19:
	pcmpeqb	%xmm13, %xmm13
	psllq	$63,    %xmm13
	shufps	$0xc0, %xmm13, %xmm13

#if (!defined(CONJ) && !defined(XCONJ)) || (defined(CONJ) && defined(XCONJ))
	xorpd	%xmm13, %xmm0
	xorpd	%xmm13, %xmm2
	xorpd	%xmm13, %xmm4
	xorpd	%xmm13, %xmm6
#else
	xorpd	%xmm13, %xmm1
	xorpd	%xmm13, %xmm3
	xorpd	%xmm13, %xmm5
	xorpd	%xmm13, %xmm7
#endif

#ifdef HAVE_SSE3
	haddpd	%xmm1, %xmm0
	haddpd	%xmm3, %xmm2

	haddpd	%xmm5, %xmm4
	haddpd	%xmm7, %xmm6
#else
	movapd	%xmm0, %xmm8
	unpcklpd %xmm1, %xmm0
	unpckhpd %xmm1, %xmm8

	movapd	%xmm2, %xmm9
	unpcklpd %xmm3, %xmm2
	unpckhpd %xmm3, %xmm9

	movapd	%xmm4, %xmm10
	unpcklpd %xmm5, %xmm4
	unpckhpd %xmm5, %xmm10

	movapd	%xmm6, %xmm11
	unpcklpd %xmm7, %xmm6
	unpckhpd %xmm7, %xmm11

	addpd	%xmm8,  %xmm0
	addpd	%xmm9,  %xmm2
	addpd	%xmm10, %xmm4
	addpd	%xmm11, %xmm6
#endif

	pshufd	$0x4e, %xmm0, %xmm1
	pshufd	$0x4e, %xmm2, %xmm3
	pshufd	$0x4e, %xmm4, %xmm5
	pshufd	$0x4e, %xmm6, %xmm7

	mulpd	ALPHA_R, %xmm0
	mulpd	ALPHA_I, %xmm1
	mulpd	ALPHA_R, %xmm2
	mulpd	ALPHA_I, %xmm3

	mulpd	ALPHA_R, %xmm4
	mulpd	ALPHA_I, %xmm5
	mulpd	ALPHA_R, %xmm6
	mulpd	ALPHA_I, %xmm7

	xorpd	%xmm13, %xmm1
	xorpd	%xmm13, %xmm3
	xorpd	%xmm13, %xmm5
	xorpd	%xmm13, %xmm7

	subpd	%xmm1, %xmm0
	subpd	%xmm3, %xmm2
	subpd	%xmm5, %xmm4
	subpd	%xmm7, %xmm6

	movsd	 0 * SIZE(Y), %xmm1
	movhpd	 1 * SIZE(Y), %xmm1
	addq	INCY, Y
	movsd	 0 * SIZE(Y), %xmm3
	movhpd	 1 * SIZE(Y), %xmm3
	addq	INCY, Y
	movsd	 0 * SIZE(Y), %xmm5
	movhpd	 1 * SIZE(Y), %xmm5
	addq	INCY, Y
	movsd	 0 * SIZE(Y), %xmm7
	movhpd	 1 * SIZE(Y), %xmm7
	addq	INCY, Y

	addpd	%xmm1, %xmm0
	addpd	%xmm3, %xmm2
	addpd	%xmm5, %xmm4
	addpd	%xmm7, %xmm6

	movlpd	%xmm0,  0 * SIZE(Y1)
	movhpd	%xmm0,  1 * SIZE(Y1)
	addq	INCY, Y1
	movlpd	%xmm2,  0 * SIZE(Y1)
	movhpd	%xmm2,  1 * SIZE(Y1)
	addq	INCY, Y1
	movlpd	%xmm4,  0 * SIZE(Y1)
	movhpd	%xmm4,  1 * SIZE(Y1)
	addq	INCY, Y1
	movlpd	%xmm6,  0 * SIZE(Y1)
	movhpd	%xmm6,  1 * SIZE(Y1)
	addq	INCY, Y1

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

	leaq	16 * SIZE(BUFFER), X1

	movq	A, A1
	leaq	(A1, LDA), A2
	leaq	(A1, LDA, 2), A

	xorpd	%xmm0, %xmm0
	xorpd	%xmm1, %xmm1
	xorpd	%xmm2, %xmm2
	xorpd	%xmm3, %xmm3

	MOVUPS_XL1(-16 * SIZE, X1, %xmm4)
	MOVUPS_XL1(-14 * SIZE, X1, %xmm5)

#ifdef PREFETCHW
	PREFETCHW	3 * SIZE(Y1)
#endif

	movq	M,   I
	sarq	$2,  I
	jle	.L25

	MOVUPS_A1(-16 * SIZE, A1, %xmm8)
	MOVUPS_A1(-16 * SIZE, A2, %xmm10)
	MOVUPS_A1(-14 * SIZE, A1, %xmm12)
	MOVUPS_A1(-14 * SIZE, A2, %xmm6)

	decq	 I
	jle	 .L24
	ALIGN_3

.L23:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(A1)
#endif

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	MOVUPS_A1(-12 * SIZE, A1, %xmm8)
	mulpd	%xmm4,  %xmm9
	SUBPD	%xmm9,  %xmm1

	pshufd	$0x4e, %xmm10, %xmm11
	mulpd	%xmm4,  %xmm10
	addpd	%xmm10, %xmm2
	MOVUPS_A1(-12 * SIZE, A2, %xmm10)
	mulpd	%xmm4,  %xmm11
	SUBPD	%xmm11, %xmm3

	MOVUPS_XL1(-12 * SIZE, X1, %xmm4)

	pshufd	$0x4e, %xmm12,  %xmm13
	mulpd	%xmm5,  %xmm12
	addpd	%xmm12,  %xmm0
	MOVUPS_A1(-10 * SIZE, A1, %xmm12)
	mulpd	%xmm5,  %xmm13
	SUBPD	%xmm13,  %xmm1

	pshufd	$0x4e, %xmm6, %xmm7
	mulpd	%xmm5,  %xmm6
	addpd	%xmm6, %xmm2
	MOVUPS_A1(-10 * SIZE, A2, %xmm6)
	mulpd	%xmm5,  %xmm7
	SUBPD	%xmm7, %xmm3

	MOVUPS_XL1(-10 * SIZE, X1, %xmm5)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(A2)
#endif

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	MOVUPS_A1( -8 * SIZE, A1, %xmm8)
	mulpd	%xmm4,  %xmm9
	SUBPD	%xmm9,  %xmm1

	pshufd	$0x4e, %xmm10, %xmm11
	mulpd	%xmm4,  %xmm10
	addpd	%xmm10, %xmm2
	MOVUPS_A1( -8 * SIZE, A2, %xmm10)
	mulpd	%xmm4,  %xmm11
	SUBPD	%xmm11, %xmm3

	MOVUPS_XL1( -8 * SIZE, X1, %xmm4)

#ifdef PREFETCHW
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(X1)
#endif

	pshufd	$0x4e, %xmm12,  %xmm13
	mulpd	%xmm5,  %xmm12
	addpd	%xmm12,  %xmm0
	MOVUPS_A1( -6 * SIZE, A1, %xmm12)
	mulpd	%xmm5,  %xmm13
	SUBPD	%xmm13,  %xmm1

	pshufd	$0x4e, %xmm6, %xmm7
	mulpd	%xmm5,  %xmm6
	addpd	%xmm6, %xmm2
	MOVUPS_A1( -6 * SIZE, A2, %xmm6)
	mulpd	%xmm5,  %xmm7
	SUBPD	%xmm7, %xmm3

	MOVUPS_XL1( -6 * SIZE, X1, %xmm5)

	subq	 $-8 * SIZE, A1
	subq	 $-8 * SIZE, A2
	subq	 $-8 * SIZE, X1

	subq	 $1, I
	BRANCH
	jg	.L23
	ALIGN_3

.L24:
	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	MOVUPS_A1(-12 * SIZE, A1, %xmm8)
	mulpd	%xmm4,  %xmm9
	SUBPD	%xmm9,  %xmm1

	pshufd	$0x4e, %xmm10, %xmm11
	mulpd	%xmm4,  %xmm10
	addpd	%xmm10, %xmm2
	MOVUPS_A1(-12 * SIZE, A2, %xmm10)
	mulpd	%xmm4,  %xmm11
	SUBPD	%xmm11, %xmm3

	MOVUPS_XL1(-12 * SIZE, X1, %xmm4)

	pshufd	$0x4e, %xmm12,  %xmm13
	mulpd	%xmm5,  %xmm12
	addpd	%xmm12,  %xmm0
	MOVUPS_A1(-10 * SIZE, A1, %xmm12)
	mulpd	%xmm5,  %xmm13
	SUBPD	%xmm13,  %xmm1

	pshufd	$0x4e, %xmm6, %xmm7
	mulpd	%xmm5,  %xmm6
	addpd	%xmm6, %xmm2
	MOVUPS_A1(-10 * SIZE, A2, %xmm6)
	mulpd	%xmm5,  %xmm7
	SUBPD	%xmm7, %xmm3

	MOVUPS_XL1(-10 * SIZE, X1, %xmm5)

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	mulpd	%xmm4,  %xmm9
	SUBPD	%xmm9,  %xmm1

	pshufd	$0x4e, %xmm10, %xmm11
	mulpd	%xmm4,  %xmm10
	addpd	%xmm10, %xmm2
	mulpd	%xmm4,  %xmm11
	SUBPD	%xmm11, %xmm3

	MOVUPS_XL1( -8 * SIZE, X1, %xmm4)

	pshufd	$0x4e, %xmm12,  %xmm13
	mulpd	%xmm5,  %xmm12
	addpd	%xmm12,  %xmm0
	mulpd	%xmm5,  %xmm13
	SUBPD	%xmm13,  %xmm1

	pshufd	$0x4e, %xmm6, %xmm7
	mulpd	%xmm5,  %xmm6
	addpd	%xmm6, %xmm2
	mulpd	%xmm5,  %xmm7
	SUBPD	%xmm7, %xmm3

	MOVUPS_XL1( -6 * SIZE, X1, %xmm5)

	subq	 $-8 * SIZE, A1
	subq	 $-8 * SIZE, A2
	subq	 $-8 * SIZE, X1
	ALIGN_3

.L25:
	testq	$2, M
	je	.L27

	MOVUPS_A1(-16 * SIZE, A1, %xmm8)
	MOVUPS_A1(-16 * SIZE, A2, %xmm10)

	MOVUPS_A1(-14 * SIZE, A1, %xmm12)
	MOVUPS_A1(-14 * SIZE, A2, %xmm6)

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	mulpd	%xmm4,  %xmm9
	SUBPD	%xmm9,  %xmm1

	pshufd	$0x4e, %xmm10, %xmm11
	mulpd	%xmm4,  %xmm10
	addpd	%xmm10, %xmm2
	mulpd	%xmm4,  %xmm11
	SUBPD	%xmm11, %xmm3

	MOVUPS_XL1(-12 * SIZE, X1, %xmm4)

	pshufd	$0x4e, %xmm12,  %xmm13
	mulpd	%xmm5,  %xmm12
	addpd	%xmm12,  %xmm0
	mulpd	%xmm5,  %xmm13
	SUBPD	%xmm13,  %xmm1

	pshufd	$0x4e, %xmm6, %xmm7
	mulpd	%xmm5,  %xmm6
	addpd	%xmm6, %xmm2
	mulpd	%xmm5,  %xmm7
	SUBPD	%xmm7, %xmm3

	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, A2
	ALIGN_3

.L27:
	testq	$1, M
	je	.L29

	MOVUPS_A1(-16 * SIZE, A1, %xmm8)
	MOVUPS_A1(-16 * SIZE, A2, %xmm10)

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	mulpd	%xmm4,  %xmm9
	SUBPD	%xmm9,  %xmm1

	pshufd	$0x4e, %xmm10, %xmm11
	mulpd	%xmm4,  %xmm10
	addpd	%xmm10, %xmm2
	mulpd	%xmm4,  %xmm11
	SUBPD	%xmm11, %xmm3
	ALIGN_3

.L29:
	pcmpeqb	%xmm11, %xmm11
	psllq	$63,    %xmm11
	shufps	$0xc0, %xmm11, %xmm11

#if (!defined(CONJ) && !defined(XCONJ)) || (defined(CONJ) && defined(XCONJ))
	xorpd	%xmm11, %xmm0
	xorpd	%xmm11, %xmm2
#else
	xorpd	%xmm11, %xmm1
	xorpd	%xmm11, %xmm3
#endif

#ifdef HAVE_SSE3
	haddpd	%xmm1, %xmm0
	haddpd	%xmm3, %xmm2
#else
	movapd	%xmm0, %xmm8
	unpcklpd %xmm1, %xmm0
	unpckhpd %xmm1, %xmm8

	movapd	%xmm2, %xmm9
	unpcklpd %xmm3, %xmm2
	unpckhpd %xmm3, %xmm9

	addpd	%xmm8, %xmm0
	addpd	%xmm9, %xmm2
#endif

	pshufd	$0x4e, %xmm0, %xmm1
	pshufd	$0x4e, %xmm2, %xmm3

	mulpd	ALPHA_R, %xmm0
	mulpd	ALPHA_I, %xmm1
	mulpd	ALPHA_R, %xmm2
	mulpd	ALPHA_I, %xmm3

	xorpd	%xmm11, %xmm1
	xorpd	%xmm11, %xmm3

	subpd	%xmm1, %xmm0
	subpd	%xmm3, %xmm2

	movsd	 0 * SIZE(Y), %xmm4
	movhpd	 1 * SIZE(Y), %xmm4
	addq	INCY, Y
	movsd	 0 * SIZE(Y), %xmm5
	movhpd	 1 * SIZE(Y), %xmm5
	addq	INCY, Y

	addpd	%xmm4, %xmm0
	addpd	%xmm5, %xmm2

	movlpd	%xmm0,  0 * SIZE(Y1)
	movhpd	%xmm0,  1 * SIZE(Y1)
	addq	INCY, Y1
	movlpd	%xmm2,  0 * SIZE(Y1)
	movhpd	%xmm2,  1 * SIZE(Y1)
	addq	INCY, Y1

#if GEMV_UNROLL == 2
	cmpq	$2, N
	jge	.L21
#endif
	ALIGN_3

.L30:
#endif

	cmpq	$1, N
	jl	.L999

#if GEMV_UNROLL == 1
.L31:
	decq	N
#endif

	leaq	16 * SIZE(BUFFER), X1

	movq	A, A1
#if GEMV_UNROLL == 1
	addq	LDA, A
#endif

	xorpd	%xmm0, %xmm0
	xorpd	%xmm1, %xmm1

	MOVUPS_XL1(-16 * SIZE, X1, %xmm4)
	MOVUPS_XL1(-14 * SIZE, X1, %xmm5)

	movq	M,   I
	sarq	$2,  I
	jle	.L35

	MOVUPS_A1(-16 * SIZE, A1, %xmm8)
	MOVUPS_A1(-14 * SIZE, A1, %xmm12)

	decq	 I
	jle	 .L34
	ALIGN_3

.L33:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 4 - 128 + PREOFFSET(A1)
#endif

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	MOVUPS_A1(-12 * SIZE, A1, %xmm8)
	mulpd	%xmm4,  %xmm9
	SUBPD	%xmm9,  %xmm1

	MOVUPS_XL1(-12 * SIZE, X1, %xmm4)

	pshufd	$0x4e, %xmm12,  %xmm13
	mulpd	%xmm5,  %xmm12
	addpd	%xmm12,  %xmm0
	MOVUPS_A1(-10 * SIZE, A1, %xmm12)
	mulpd	%xmm5,  %xmm13
	SUBPD	%xmm13,  %xmm1

	MOVUPS_XL1(-10 * SIZE, X1, %xmm5)

#ifdef PREFETCHW
	PREFETCH	(PREFETCHSIZE) * 4 - 128 + PREOFFSET(X1)
#endif

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	MOVUPS_A1( -8 * SIZE, A1, %xmm8)
	mulpd	%xmm4,  %xmm9
	SUBPD	%xmm9,  %xmm1

	MOVUPS_XL1( -8 * SIZE, X1, %xmm4)

	pshufd	$0x4e, %xmm12,  %xmm13
	mulpd	%xmm5,  %xmm12
	addpd	%xmm12,  %xmm0
	MOVUPS_A1( -6 * SIZE, A1, %xmm12)
	mulpd	%xmm5,  %xmm13
	SUBPD	%xmm13,  %xmm1

	MOVUPS_XL1(-6 * SIZE, X1, %xmm5)

	subq	 $-8 * SIZE, A1
	subq	 $-8 * SIZE, X1

	subq	 $1, I
	BRANCH
	jg	.L33
	ALIGN_3

.L34:
	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	MOVUPS_A1(-12 * SIZE, A1, %xmm8)
	mulpd	%xmm4,  %xmm9
	SUBPD	%xmm9,  %xmm1

	MOVUPS_XL1(-12 * SIZE, X1, %xmm4)

	pshufd	$0x4e, %xmm12,  %xmm13
	mulpd	%xmm5,  %xmm12
	addpd	%xmm12,  %xmm0
	MOVUPS_A1(-10 * SIZE, A1, %xmm12)
	mulpd	%xmm5,  %xmm13
	SUBPD	%xmm13,  %xmm1

	MOVUPS_XL1(-10 * SIZE, X1, %xmm5)

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	mulpd	%xmm4,  %xmm9
	SUBPD	%xmm9,  %xmm1

	MOVUPS_XL1( -8 * SIZE, X1, %xmm4)

	pshufd	$0x4e, %xmm12,  %xmm13
	mulpd	%xmm5,  %xmm12
	addpd	%xmm12,  %xmm0
	mulpd	%xmm5,  %xmm13
	SUBPD	%xmm13,  %xmm1

	MOVUPS_XL1(-6 * SIZE, X1, %xmm5)

	subq	 $-8 * SIZE, A1
	subq	 $-8 * SIZE, X1
	ALIGN_3

.L35:
	testq	$2, M
	je	.L37

	MOVUPS_A1(-16 * SIZE, A1, %xmm8)
	MOVUPS_A1(-14 * SIZE, A1, %xmm12)

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	mulpd	%xmm4,  %xmm9
	SUBPD	%xmm9,  %xmm1

	MOVUPS_XL1(-12 * SIZE, X1, %xmm4)

	pshufd	$0x4e, %xmm12,  %xmm13
	mulpd	%xmm5,  %xmm12
	addpd	%xmm12,  %xmm0
	mulpd	%xmm5,  %xmm13
	SUBPD	%xmm13,  %xmm1

	addq	 $4 * SIZE, A1
	ALIGN_3

.L37:
	testq	$1, M
	je	.L39

	MOVUPS_A1(-16 * SIZE, A1, %xmm8)

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	mulpd	%xmm4,  %xmm9
	SUBPD	%xmm9,  %xmm1
	ALIGN_3

.L39:
	pcmpeqb	%xmm11, %xmm11
	psllq	$63,    %xmm11
	shufps	$0xc0, %xmm11, %xmm11

#if (!defined(CONJ) && !defined(XCONJ)) || (defined(CONJ) && defined(XCONJ))
	xorpd	%xmm11, %xmm0
#else
	xorpd	%xmm11, %xmm1
#endif

#ifdef HAVE_SSE3
	haddpd	%xmm1, %xmm0
#else
	movapd	%xmm0, %xmm8
	unpcklpd %xmm1, %xmm0
	unpckhpd %xmm1, %xmm8

	addpd	%xmm8, %xmm0
#endif

	pshufd	$0x4e, %xmm0, %xmm1

	mulpd	ALPHA_R, %xmm0
	mulpd	ALPHA_I, %xmm1

	xorpd	%xmm11, %xmm1

	subpd	%xmm1, %xmm0

	movsd	 0 * SIZE(Y), %xmm4
	movhpd	 1 * SIZE(Y), %xmm4

	addpd	%xmm4, %xmm0

	movlpd	%xmm0,  0 * SIZE(Y1)
	movhpd	%xmm0,  1 * SIZE(Y1)

#if GEMV_UNROLL == 1
	addq	INCY, Y
	addq	INCY, Y1

	cmpq	$1, N
	jge	.L31
#endif

#ifdef ALIGNED_ACCESS
	jmp	.L999
	ALIGN_3

.L100:
#if GEMV_UNROLL >= 4

	cmpq	$4, N
	jl	.L110
	ALIGN_3

.L101:
	subq	$4, N

	leaq	16 * SIZE(BUFFER), X1

	movq	A, A1
	leaq	(A1, LDA, 2), A2
	leaq	(A1, LDA, 4), A

	MOVUPS_XL1(-16 * SIZE, X1, %xmm12)
	xorpd	%xmm0, %xmm0
	xorpd	%xmm1, %xmm1
	xorpd	%xmm2, %xmm2
	xorpd	%xmm3, %xmm3
	MOVUPS_XL1(-14 * SIZE, X1, %xmm13)
	xorpd	%xmm4, %xmm4
	xorpd	%xmm5, %xmm5
	xorpd	%xmm6, %xmm6
	xorpd	%xmm7, %xmm7

#ifdef PREFETCHW
	PREFETCHW	3 * SIZE(Y1)
#endif

	movq	M,   I
	sarq	$2,  I
	jle	.L105

	movsd	-16 * SIZE(A1), %xmm8
	movhpd	-15 * SIZE(A1), %xmm8

	movsd	-16 * SIZE(A1, LDA), %xmm10
	movhpd	-15 * SIZE(A1, LDA), %xmm10

	decq	 I
	jle	 .L104
	ALIGN_3

.L103:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A1)
#endif

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm12,  %xmm8
	addpd	%xmm8,  %xmm0
	movsd	-16 * SIZE(A2), %xmm8
	movhpd	-15 * SIZE(A2), %xmm8
	mulpd	%xmm12, %xmm9
	SUBPD	%xmm9,  %xmm1

	pshufd	$0x4e, %xmm10, %xmm11
	mulpd	%xmm12, %xmm10
	addpd	%xmm10, %xmm2
	movsd	-16 * SIZE(A2, LDA), %xmm10
	movhpd	-15 * SIZE(A2, LDA), %xmm10
	mulpd	%xmm12, %xmm11
	SUBPD	%xmm11, %xmm3

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm12, %xmm8
	addpd	%xmm8,  %xmm4
	movsd	-14 * SIZE(A1), %xmm8
	movhpd	-13 * SIZE(A1), %xmm8
	mulpd	%xmm12, %xmm9
	SUBPD	%xmm9,  %xmm5

	pshufd	$0x4e, %xmm10, %xmm11
	mulpd	%xmm12, %xmm10
	addpd	%xmm10, %xmm6
	movsd	-14 * SIZE(A1, LDA), %xmm10
	movhpd	-13 * SIZE(A1, LDA), %xmm10
	mulpd	%xmm12, %xmm11
	MOVUPS_XL1(-12 * SIZE, X1, %xmm12)
	SUBPD	%xmm11, %xmm7

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A1, LDA)
#endif

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm13,  %xmm8
	addpd	%xmm8,  %xmm0
	movsd	-14 * SIZE(A2), %xmm8
	movhpd	-13 * SIZE(A2), %xmm8
	mulpd	%xmm13, %xmm9
	SUBPD	%xmm9,  %xmm1

	pshufd	$0x4e, %xmm10, %xmm11
	mulpd	%xmm13, %xmm10
	addpd	%xmm10, %xmm2
	movsd	-14 * SIZE(A2, LDA), %xmm10
	movhpd	-13 * SIZE(A2, LDA), %xmm10
	mulpd	%xmm13, %xmm11
	SUBPD	%xmm11, %xmm3

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm13, %xmm8
	addpd	%xmm8,  %xmm4
	movsd	-12 * SIZE(A1), %xmm8
	movhpd	-11 * SIZE(A1), %xmm8
	mulpd	%xmm13, %xmm9
	SUBPD	%xmm9,  %xmm5

	pshufd	$0x4e, %xmm10, %xmm11
	mulpd	%xmm13, %xmm10
	addpd	%xmm10, %xmm6
	movsd	-12 * SIZE(A1, LDA), %xmm10
	movhpd	-11 * SIZE(A1, LDA), %xmm10
	mulpd	%xmm13, %xmm11
	MOVUPS_XL1(-10 * SIZE, X1, %xmm13)
	SUBPD	%xmm11, %xmm7

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A2)
#endif

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm12,  %xmm8
	addpd	%xmm8,  %xmm0
	movsd	-12 * SIZE(A2), %xmm8
	movhpd	-11 * SIZE(A2), %xmm8
	mulpd	%xmm12, %xmm9
	SUBPD	%xmm9,  %xmm1

	pshufd	$0x4e, %xmm10, %xmm11
	mulpd	%xmm12, %xmm10
	addpd	%xmm10, %xmm2
	movsd	-12 * SIZE(A2, LDA), %xmm10
	movhpd	-11 * SIZE(A2, LDA), %xmm10
	mulpd	%xmm12, %xmm11
	SUBPD	%xmm11, %xmm3

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm12, %xmm8
	addpd	%xmm8,  %xmm4
	movsd	-10 * SIZE(A1), %xmm8
	movhpd	 -9 * SIZE(A1), %xmm8
	mulpd	%xmm12, %xmm9
	SUBPD	%xmm9,  %xmm5

	pshufd	$0x4e, %xmm10, %xmm11
	mulpd	%xmm12, %xmm10
	addpd	%xmm10, %xmm6
	movsd	-10 * SIZE(A1, LDA), %xmm10
	movhpd	 -9 * SIZE(A1, LDA), %xmm10
	mulpd	%xmm12, %xmm11
	MOVUPS_XL1( -8 * SIZE, X1, %xmm12)
	SUBPD	%xmm11, %xmm7

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A2, LDA)
#endif

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm13,  %xmm8
	addpd	%xmm8,  %xmm0
	movsd	-10 * SIZE(A2), %xmm8
	movhpd	 -9 * SIZE(A2), %xmm8
	mulpd	%xmm13, %xmm9
	SUBPD	%xmm9,  %xmm1

	pshufd	$0x4e, %xmm10, %xmm11
	mulpd	%xmm13, %xmm10
	addpd	%xmm10, %xmm2
	movsd	-10 * SIZE(A2, LDA), %xmm10
	movhpd	 -9 * SIZE(A2, LDA), %xmm10
	mulpd	%xmm13, %xmm11
	SUBPD	%xmm11, %xmm3

#ifdef PREFETCHW
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(X1)
#endif

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm13, %xmm8
	addpd	%xmm8,  %xmm4
	movsd	 -8 * SIZE(A1), %xmm8
	movhpd	 -7 * SIZE(A1), %xmm8
	mulpd	%xmm13, %xmm9
	SUBPD	%xmm9,  %xmm5

	pshufd	$0x4e, %xmm10, %xmm11
	mulpd	%xmm13, %xmm10
	addpd	%xmm10, %xmm6
	movsd	 -8 * SIZE(A1, LDA), %xmm10
	movhpd	 -7 * SIZE(A1, LDA), %xmm10
	mulpd	%xmm13, %xmm11
	MOVUPS_XL1( -6 * SIZE, X1, %xmm13)
	SUBPD	%xmm11, %xmm7

	subq	 $-8 * SIZE, A1
	subq	 $-8 * SIZE, A2
	subq	 $-8 * SIZE, X1

	subq	 $1, I
	BRANCH
	jg	.L103
	ALIGN_3

.L104:
	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm12,  %xmm8
	addpd	%xmm8,  %xmm0
	movsd	-16 * SIZE(A2), %xmm8
	movhpd	-15 * SIZE(A2), %xmm8
	mulpd	%xmm12, %xmm9
	SUBPD	%xmm9,  %xmm1

	pshufd	$0x4e, %xmm10, %xmm11
	mulpd	%xmm12, %xmm10
	addpd	%xmm10, %xmm2
	movsd	-16 * SIZE(A2, LDA), %xmm10
	movhpd	-15 * SIZE(A2, LDA), %xmm10
	mulpd	%xmm12, %xmm11
	SUBPD	%xmm11, %xmm3

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm12, %xmm8
	addpd	%xmm8,  %xmm4
	movsd	-14 * SIZE(A1), %xmm8
	movhpd	-13 * SIZE(A1), %xmm8
	mulpd	%xmm12, %xmm9
	SUBPD	%xmm9,  %xmm5

	pshufd	$0x4e, %xmm10, %xmm11
	mulpd	%xmm12, %xmm10
	addpd	%xmm10, %xmm6
	movsd	-14 * SIZE(A1, LDA), %xmm10
	movhpd	-13 * SIZE(A1, LDA), %xmm10
	mulpd	%xmm12, %xmm11
	MOVUPS_XL1(-12 * SIZE, X1, %xmm12)
	SUBPD	%xmm11, %xmm7

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm13,  %xmm8
	addpd	%xmm8,  %xmm0
	movsd	-14 * SIZE(A2), %xmm8
	movhpd	-13 * SIZE(A2), %xmm8
	mulpd	%xmm13, %xmm9
	SUBPD	%xmm9,  %xmm1

	pshufd	$0x4e, %xmm10, %xmm11
	mulpd	%xmm13, %xmm10
	addpd	%xmm10, %xmm2
	movsd	-14 * SIZE(A2, LDA), %xmm10
	movhpd	-13 * SIZE(A2, LDA), %xmm10
	mulpd	%xmm13, %xmm11
	SUBPD	%xmm11, %xmm3

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm13, %xmm8
	addpd	%xmm8,  %xmm4
	movsd	-12 * SIZE(A1), %xmm8
	movhpd	-11 * SIZE(A1), %xmm8
	mulpd	%xmm13, %xmm9
	SUBPD	%xmm9,  %xmm5

	pshufd	$0x4e, %xmm10, %xmm11
	mulpd	%xmm13, %xmm10
	addpd	%xmm10, %xmm6
	movsd	-12 * SIZE(A1, LDA), %xmm10
	movhpd	-11 * SIZE(A1, LDA), %xmm10
	mulpd	%xmm13, %xmm11
	MOVUPS_XL1(-10 * SIZE, X1, %xmm13)
	SUBPD	%xmm11, %xmm7

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm12,  %xmm8
	addpd	%xmm8,  %xmm0
	movsd	-12 * SIZE(A2), %xmm8
	movhpd	-11 * SIZE(A2), %xmm8
	mulpd	%xmm12, %xmm9
	SUBPD	%xmm9,  %xmm1

	pshufd	$0x4e, %xmm10, %xmm11
	mulpd	%xmm12, %xmm10
	addpd	%xmm10, %xmm2
	movsd	-12 * SIZE(A2, LDA), %xmm10
	movhpd	-11 * SIZE(A2, LDA), %xmm10
	mulpd	%xmm12, %xmm11
	SUBPD	%xmm11, %xmm3

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm12, %xmm8
	addpd	%xmm8,  %xmm4
	movsd	-10 * SIZE(A1), %xmm8
	movhpd	 -9 * SIZE(A1), %xmm8
	mulpd	%xmm12, %xmm9
	SUBPD	%xmm9,  %xmm5

	pshufd	$0x4e, %xmm10, %xmm11
	mulpd	%xmm12, %xmm10
	addpd	%xmm10, %xmm6
	movsd	-10 * SIZE(A1, LDA), %xmm10
	movhpd	 -9 * SIZE(A1, LDA), %xmm10
	mulpd	%xmm12, %xmm11
	MOVUPS_XL1( -8 * SIZE, X1, %xmm12)
	SUBPD	%xmm11, %xmm7

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm13,  %xmm8
	addpd	%xmm8,  %xmm0
	movsd	-10 * SIZE(A2), %xmm8
	movhpd	 -9 * SIZE(A2), %xmm8
	mulpd	%xmm13, %xmm9
	SUBPD	%xmm9,  %xmm1

	pshufd	$0x4e, %xmm10, %xmm11
	mulpd	%xmm13, %xmm10
	addpd	%xmm10, %xmm2
	movsd	-10 * SIZE(A2, LDA), %xmm10
	movhpd	 -9 * SIZE(A2, LDA), %xmm10
	mulpd	%xmm13, %xmm11
	SUBPD	%xmm11, %xmm3

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm13, %xmm8
	addpd	%xmm8,  %xmm4
	mulpd	%xmm13, %xmm9
	SUBPD	%xmm9,  %xmm5

	pshufd	$0x4e, %xmm10, %xmm11
	mulpd	%xmm13, %xmm10
	addpd	%xmm10, %xmm6
	mulpd	%xmm13, %xmm11
	MOVUPS_XL1( -6 * SIZE, X1, %xmm13)
	SUBPD	%xmm11, %xmm7

	subq	 $-8 * SIZE, A1
	subq	 $-8 * SIZE, A2
	subq	 $-8 * SIZE, X1
	ALIGN_3

.L105:
	testq	$2, M
	je	.L107

	movsd	-16 * SIZE(A1), %xmm8
	movhpd	-15 * SIZE(A1), %xmm8

	movsd	-16 * SIZE(A1, LDA), %xmm10
	movhpd	-15 * SIZE(A1, LDA), %xmm10

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm12,  %xmm8
	addpd	%xmm8,  %xmm0
	movsd	-16 * SIZE(A2), %xmm8
	movhpd	-15 * SIZE(A2), %xmm8
	mulpd	%xmm12, %xmm9
	SUBPD	%xmm9,  %xmm1

	pshufd	$0x4e, %xmm10, %xmm11
	mulpd	%xmm12, %xmm10
	addpd	%xmm10, %xmm2
	movsd	-16 * SIZE(A2, LDA), %xmm10
	movhpd	-15 * SIZE(A2, LDA), %xmm10
	mulpd	%xmm12, %xmm11
	SUBPD	%xmm11, %xmm3

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm12, %xmm8
	addpd	%xmm8,  %xmm4
	movsd	-14 * SIZE(A1), %xmm8
	movhpd	-13 * SIZE(A1), %xmm8
	mulpd	%xmm12, %xmm9
	SUBPD	%xmm9,  %xmm5

	pshufd	$0x4e, %xmm10, %xmm11
	mulpd	%xmm12, %xmm10
	addpd	%xmm10, %xmm6
	movsd	-14 * SIZE(A1, LDA), %xmm10
	movhpd	-13 * SIZE(A1, LDA), %xmm10
	mulpd	%xmm12, %xmm11
	MOVUPS_XL1(-12 * SIZE, X1, %xmm12)
	SUBPD	%xmm11, %xmm7

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm13,  %xmm8
	addpd	%xmm8,  %xmm0
	movsd	-14 * SIZE(A2), %xmm8
	movhpd	-13 * SIZE(A2), %xmm8
	mulpd	%xmm13, %xmm9
	SUBPD	%xmm9,  %xmm1

	pshufd	$0x4e, %xmm10, %xmm11
	mulpd	%xmm13, %xmm10
	addpd	%xmm10, %xmm2
	movsd	-14 * SIZE(A2, LDA), %xmm10
	movhpd	-13 * SIZE(A2, LDA), %xmm10
	mulpd	%xmm13, %xmm11
	SUBPD	%xmm11, %xmm3

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm13, %xmm8
	addpd	%xmm8,  %xmm4
	mulpd	%xmm13, %xmm9
	SUBPD	%xmm9,  %xmm5

	pshufd	$0x4e, %xmm10, %xmm11
	mulpd	%xmm13, %xmm10
	addpd	%xmm10, %xmm6
	mulpd	%xmm13, %xmm11
	SUBPD	%xmm11, %xmm7

	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, A2
	ALIGN_3

.L107:
	testq	$1, M
	je	.L109

	movsd	-16 * SIZE(A1), %xmm8
	movhpd	-15 * SIZE(A1), %xmm8

	movsd	-16 * SIZE(A1, LDA), %xmm10
	movhpd	-15 * SIZE(A1, LDA), %xmm10

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm12,  %xmm8
	addpd	%xmm8,  %xmm0
	movsd	-16 * SIZE(A2), %xmm8
	movhpd	-15 * SIZE(A2), %xmm8
	mulpd	%xmm12, %xmm9
	SUBPD	%xmm9,  %xmm1

	pshufd	$0x4e, %xmm10, %xmm11
	mulpd	%xmm12, %xmm10
	addpd	%xmm10, %xmm2
	movsd	-16 * SIZE(A2, LDA), %xmm10
	movhpd	-15 * SIZE(A2, LDA), %xmm10
	mulpd	%xmm12, %xmm11
	SUBPD	%xmm11, %xmm3

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm12, %xmm8
	addpd	%xmm8,  %xmm4
	mulpd	%xmm12, %xmm9
	SUBPD	%xmm9,  %xmm5

	pshufd	$0x4e, %xmm10, %xmm11
	mulpd	%xmm12, %xmm10
	addpd	%xmm10, %xmm6
	mulpd	%xmm12, %xmm11
	SUBPD	%xmm11, %xmm7
	ALIGN_3

.L109:
	pcmpeqb	%xmm13, %xmm13
	psllq	$63,    %xmm13
	shufps	$0xc0, %xmm13, %xmm13

#if (!defined(CONJ) && !defined(XCONJ)) || (defined(CONJ) && defined(XCONJ))
	xorpd	%xmm13, %xmm0
	xorpd	%xmm13, %xmm2
	xorpd	%xmm13, %xmm4
	xorpd	%xmm13, %xmm6
#else
	xorpd	%xmm13, %xmm1
	xorpd	%xmm13, %xmm3
	xorpd	%xmm13, %xmm5
	xorpd	%xmm13, %xmm7
#endif

#ifdef HAVE_SSE3
	haddpd	%xmm1, %xmm0
	haddpd	%xmm3, %xmm2

	haddpd	%xmm5, %xmm4
	haddpd	%xmm7, %xmm6
#else
	movapd	%xmm0, %xmm8
	unpcklpd %xmm1, %xmm0
	unpckhpd %xmm1, %xmm8

	movapd	%xmm2, %xmm9
	unpcklpd %xmm3, %xmm2
	unpckhpd %xmm3, %xmm9

	movapd	%xmm4, %xmm10
	unpcklpd %xmm5, %xmm4
	unpckhpd %xmm5, %xmm10

	movapd	%xmm6, %xmm11
	unpcklpd %xmm7, %xmm6
	unpckhpd %xmm7, %xmm11

	addpd	%xmm8,  %xmm0
	addpd	%xmm9,  %xmm2
	addpd	%xmm10, %xmm4
	addpd	%xmm11, %xmm6
#endif

	pshufd	$0x4e, %xmm0, %xmm1
	pshufd	$0x4e, %xmm2, %xmm3
	pshufd	$0x4e, %xmm4, %xmm5
	pshufd	$0x4e, %xmm6, %xmm7

	mulpd	ALPHA_R, %xmm0
	mulpd	ALPHA_I, %xmm1
	mulpd	ALPHA_R, %xmm2
	mulpd	ALPHA_I, %xmm3

	mulpd	ALPHA_R, %xmm4
	mulpd	ALPHA_I, %xmm5
	mulpd	ALPHA_R, %xmm6
	mulpd	ALPHA_I, %xmm7

	xorpd	%xmm13, %xmm1
	xorpd	%xmm13, %xmm3
	xorpd	%xmm13, %xmm5
	xorpd	%xmm13, %xmm7

	subpd	%xmm1, %xmm0
	subpd	%xmm3, %xmm2
	subpd	%xmm5, %xmm4
	subpd	%xmm7, %xmm6

	movsd	 0 * SIZE(Y), %xmm1
	movhpd	 1 * SIZE(Y), %xmm1
	addq	INCY, Y
	movsd	 0 * SIZE(Y), %xmm3
	movhpd	 1 * SIZE(Y), %xmm3
	addq	INCY, Y
	movsd	 0 * SIZE(Y), %xmm5
	movhpd	 1 * SIZE(Y), %xmm5
	addq	INCY, Y
	movsd	 0 * SIZE(Y), %xmm7
	movhpd	 1 * SIZE(Y), %xmm7
	addq	INCY, Y

	addpd	%xmm1, %xmm0
	addpd	%xmm3, %xmm2
	addpd	%xmm5, %xmm4
	addpd	%xmm7, %xmm6

	movlpd	%xmm0,  0 * SIZE(Y1)
	movhpd	%xmm0,  1 * SIZE(Y1)
	addq	INCY, Y1
	movlpd	%xmm2,  0 * SIZE(Y1)
	movhpd	%xmm2,  1 * SIZE(Y1)
	addq	INCY, Y1
	movlpd	%xmm4,  0 * SIZE(Y1)
	movhpd	%xmm4,  1 * SIZE(Y1)
	addq	INCY, Y1
	movlpd	%xmm6,  0 * SIZE(Y1)
	movhpd	%xmm6,  1 * SIZE(Y1)
	addq	INCY, Y1

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

	leaq	16 * SIZE(BUFFER), X1

	movq	A, A1
	leaq	(A1, LDA), A2
	leaq	(A1, LDA, 2), A

	xorpd	%xmm0, %xmm0
	xorpd	%xmm1, %xmm1
	xorpd	%xmm2, %xmm2
	xorpd	%xmm3, %xmm3

	MOVUPS_XL1(-16 * SIZE, X1, %xmm4)
	MOVUPS_XL1(-14 * SIZE, X1, %xmm5)

#ifdef PREFETCHW
	PREFETCHW	3 * SIZE(Y1)
#endif

	movq	M,   I
	sarq	$2,  I
	jle	.L115

	movsd	-16 * SIZE(A1), %xmm8
	movhpd	-15 * SIZE(A1), %xmm8
	movsd	-16 * SIZE(A2), %xmm10
	movhpd	-15 * SIZE(A2), %xmm10

	movsd	-14 * SIZE(A1), %xmm12
	movhpd	-13 * SIZE(A1), %xmm12
	movsd	-14 * SIZE(A2), %xmm6
	movhpd	-13 * SIZE(A2), %xmm6

	decq	 I
	jle	 .L114
	ALIGN_3

.L113:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(A1)
#endif

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	movsd	-12 * SIZE(A1), %xmm8
	movhpd	-11 * SIZE(A1), %xmm8
	mulpd	%xmm4,  %xmm9
	SUBPD	%xmm9,  %xmm1

	pshufd	$0x4e, %xmm10, %xmm11
	mulpd	%xmm4,  %xmm10
	addpd	%xmm10, %xmm2
	movsd	-12 * SIZE(A2), %xmm10
	movhpd	-11 * SIZE(A2), %xmm10
	mulpd	%xmm4,  %xmm11
	SUBPD	%xmm11, %xmm3

	MOVUPS_XL1(-12 * SIZE, X1, %xmm4)

	pshufd	$0x4e, %xmm12,  %xmm13
	mulpd	%xmm5,  %xmm12
	addpd	%xmm12,  %xmm0
	movsd	-10 * SIZE(A1), %xmm12
	movhpd	 -9 * SIZE(A1), %xmm12
	mulpd	%xmm5,  %xmm13
	SUBPD	%xmm13,  %xmm1

	pshufd	$0x4e, %xmm6, %xmm7
	mulpd	%xmm5,  %xmm6
	addpd	%xmm6, %xmm2
	movsd	-10 * SIZE(A2), %xmm6
	movhpd	 -9 * SIZE(A2), %xmm6
	mulpd	%xmm5,  %xmm7
	SUBPD	%xmm7, %xmm3

	MOVUPS_XL1(-10 * SIZE, X1, %xmm5)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(A2)
#endif

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	movsd	 -8 * SIZE(A1), %xmm8
	movhpd	 -7 * SIZE(A1), %xmm8
	mulpd	%xmm4,  %xmm9
	SUBPD	%xmm9,  %xmm1

	pshufd	$0x4e, %xmm10, %xmm11
	mulpd	%xmm4,  %xmm10
	addpd	%xmm10, %xmm2
	movsd	 -8 * SIZE(A2), %xmm10
	movhpd	 -7 * SIZE(A2), %xmm10
	mulpd	%xmm4,  %xmm11
	SUBPD	%xmm11, %xmm3

	MOVUPS_XL1( -8 * SIZE, X1, %xmm4)

#ifdef PREFETCHW
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(X1)
#endif

	pshufd	$0x4e, %xmm12,  %xmm13
	mulpd	%xmm5,  %xmm12
	addpd	%xmm12,  %xmm0
	movsd	 -6 * SIZE(A1), %xmm12
	movhpd	 -5 * SIZE(A1), %xmm12
	mulpd	%xmm5,  %xmm13
	SUBPD	%xmm13,  %xmm1

	pshufd	$0x4e, %xmm6, %xmm7
	mulpd	%xmm5,  %xmm6
	addpd	%xmm6, %xmm2
	movsd	 -6 * SIZE(A2), %xmm6
	movhpd	 -5 * SIZE(A2), %xmm6
	mulpd	%xmm5,  %xmm7
	SUBPD	%xmm7, %xmm3

	MOVUPS_XL1( -6 * SIZE, X1, %xmm5)

	subq	 $-8 * SIZE, A1
	subq	 $-8 * SIZE, A2
	subq	 $-8 * SIZE, X1

	subq	 $1, I
	BRANCH
	jg	.L113
	ALIGN_3

.L114:
	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	movsd	-12 * SIZE(A1), %xmm8
	movhpd	-11 * SIZE(A1), %xmm8
	mulpd	%xmm4,  %xmm9
	SUBPD	%xmm9,  %xmm1

	pshufd	$0x4e, %xmm10, %xmm11
	mulpd	%xmm4,  %xmm10
	addpd	%xmm10, %xmm2
	movsd	-12 * SIZE(A2), %xmm10
	movhpd	-11 * SIZE(A2), %xmm10
	mulpd	%xmm4,  %xmm11
	SUBPD	%xmm11, %xmm3

	MOVUPS_XL1(-12 * SIZE, X1, %xmm4)

	pshufd	$0x4e, %xmm12,  %xmm13
	mulpd	%xmm5,  %xmm12
	addpd	%xmm12,  %xmm0
	movsd	-10 * SIZE(A1), %xmm12
	movhpd	 -9 * SIZE(A1), %xmm12
	mulpd	%xmm5,  %xmm13
	SUBPD	%xmm13,  %xmm1

	pshufd	$0x4e, %xmm6, %xmm7
	mulpd	%xmm5,  %xmm6
	addpd	%xmm6, %xmm2
	movsd	-10 * SIZE(A2), %xmm6
	movhpd	 -9 * SIZE(A2), %xmm6
	mulpd	%xmm5,  %xmm7
	SUBPD	%xmm7, %xmm3

	MOVUPS_XL1(-10 * SIZE, X1, %xmm5)

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	mulpd	%xmm4,  %xmm9
	SUBPD	%xmm9,  %xmm1

	pshufd	$0x4e, %xmm10, %xmm11
	mulpd	%xmm4,  %xmm10
	addpd	%xmm10, %xmm2
	mulpd	%xmm4,  %xmm11
	SUBPD	%xmm11, %xmm3

	MOVUPS_XL1( -8 * SIZE, X1, %xmm4)

	pshufd	$0x4e, %xmm12,  %xmm13
	mulpd	%xmm5,  %xmm12
	addpd	%xmm12,  %xmm0
	mulpd	%xmm5,  %xmm13
	SUBPD	%xmm13,  %xmm1

	pshufd	$0x4e, %xmm6, %xmm7
	mulpd	%xmm5,  %xmm6
	addpd	%xmm6, %xmm2
	mulpd	%xmm5,  %xmm7
	SUBPD	%xmm7, %xmm3

	MOVUPS_XL1( -6 * SIZE, X1, %xmm5)

	subq	 $-8 * SIZE, A1
	subq	 $-8 * SIZE, A2
	subq	 $-8 * SIZE, X1
	ALIGN_3

.L115:
	testq	$2, M
	je	.L117

	movsd	-16 * SIZE(A1), %xmm8
	movhpd	-15 * SIZE(A1), %xmm8
	movsd	-16 * SIZE(A2), %xmm10
	movhpd	-15 * SIZE(A2), %xmm10

	movsd	-14 * SIZE(A1), %xmm12
	movhpd	-13 * SIZE(A1), %xmm12
	movsd	-14 * SIZE(A2), %xmm6
	movhpd	-13 * SIZE(A2), %xmm6

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	mulpd	%xmm4,  %xmm9
	SUBPD	%xmm9,  %xmm1

	pshufd	$0x4e, %xmm10, %xmm11
	mulpd	%xmm4,  %xmm10
	addpd	%xmm10, %xmm2
	mulpd	%xmm4,  %xmm11
	SUBPD	%xmm11, %xmm3

	MOVUPS_XL1(-12 * SIZE, X1, %xmm4)

	pshufd	$0x4e, %xmm12,  %xmm13
	mulpd	%xmm5,  %xmm12
	addpd	%xmm12,  %xmm0
	mulpd	%xmm5,  %xmm13
	SUBPD	%xmm13,  %xmm1

	pshufd	$0x4e, %xmm6, %xmm7
	mulpd	%xmm5,  %xmm6
	addpd	%xmm6, %xmm2
	mulpd	%xmm5,  %xmm7
	SUBPD	%xmm7, %xmm3

	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, A2
	ALIGN_3

.L117:
	testq	$1, M
	je	.L119

	movsd	-16 * SIZE(A1), %xmm8
	movhpd	-15 * SIZE(A1), %xmm8
	movsd	-16 * SIZE(A2), %xmm10
	movhpd	-15 * SIZE(A2), %xmm10

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	mulpd	%xmm4,  %xmm9
	SUBPD	%xmm9,  %xmm1

	pshufd	$0x4e, %xmm10, %xmm11
	mulpd	%xmm4,  %xmm10
	addpd	%xmm10, %xmm2
	mulpd	%xmm4,  %xmm11
	SUBPD	%xmm11, %xmm3
	ALIGN_3

.L119:
	pcmpeqb	%xmm11, %xmm11
	psllq	$63,    %xmm11
	shufps	$0xc0, %xmm11, %xmm11

#if (!defined(CONJ) && !defined(XCONJ)) || (defined(CONJ) && defined(XCONJ))
	xorpd	%xmm11, %xmm0
	xorpd	%xmm11, %xmm2
#else
	xorpd	%xmm11, %xmm1
	xorpd	%xmm11, %xmm3
#endif

#ifdef HAVE_SSE3
	haddpd	%xmm1, %xmm0
	haddpd	%xmm3, %xmm2
#else
	movapd	%xmm0, %xmm8
	unpcklpd %xmm1, %xmm0
	unpckhpd %xmm1, %xmm8

	movapd	%xmm2, %xmm9
	unpcklpd %xmm3, %xmm2
	unpckhpd %xmm3, %xmm9

	addpd	%xmm8, %xmm0
	addpd	%xmm9, %xmm2
#endif

	pshufd	$0x4e, %xmm0, %xmm1
	pshufd	$0x4e, %xmm2, %xmm3

	mulpd	ALPHA_R, %xmm0
	mulpd	ALPHA_I, %xmm1
	mulpd	ALPHA_R, %xmm2
	mulpd	ALPHA_I, %xmm3

	xorpd	%xmm11, %xmm1
	xorpd	%xmm11, %xmm3

	subpd	%xmm1, %xmm0
	subpd	%xmm3, %xmm2

	movsd	 0 * SIZE(Y), %xmm4
	movhpd	 1 * SIZE(Y), %xmm4
	addq	INCY, Y
	movsd	 0 * SIZE(Y), %xmm5
	movhpd	 1 * SIZE(Y), %xmm5
	addq	INCY, Y

	addpd	%xmm4, %xmm0
	addpd	%xmm5, %xmm2

	movlpd	%xmm0,  0 * SIZE(Y1)
	movhpd	%xmm0,  1 * SIZE(Y1)
	addq	INCY, Y1
	movlpd	%xmm2,  0 * SIZE(Y1)
	movhpd	%xmm2,  1 * SIZE(Y1)
	addq	INCY, Y1

#if GEMV_UNROLL == 2
	cmpq	$2, N
	jge	.L111
#endif
	ALIGN_3

.L120:
#endif

	cmpq	$1, N
	jl	.L999

#if GEMV_UNROLL == 1
.L121:
	decq	N
#endif

	leaq	16 * SIZE(BUFFER), X1

	movq	A, A1
#if GEMV_UNROLL == 1
	addq	LDA, A
#endif

	xorpd	%xmm0, %xmm0
	xorpd	%xmm1, %xmm1

	MOVUPS_XL1(-16 * SIZE, X1, %xmm4)
	MOVUPS_XL1(-14 * SIZE, X1, %xmm5)

	movq	M,   I
	sarq	$2,  I
	jle	.L125

	movsd	-16 * SIZE(A1), %xmm8
	movhpd	-15 * SIZE(A1), %xmm8
	movsd	-14 * SIZE(A1), %xmm12
	movhpd	-13 * SIZE(A1), %xmm12

	decq	 I
	jle	 .L124
	ALIGN_3

.L123:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 4 - 128 + PREOFFSET(A1)
#endif

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	movsd	-12 * SIZE(A1), %xmm8
	movhpd	-11 * SIZE(A1), %xmm8
	mulpd	%xmm4,  %xmm9
	SUBPD	%xmm9,  %xmm1

	MOVUPS_XL1(-12 * SIZE, X1, %xmm4)

	pshufd	$0x4e, %xmm12,  %xmm13
	mulpd	%xmm5,  %xmm12
	addpd	%xmm12,  %xmm0
	movsd	-10 * SIZE(A1), %xmm12
	movhpd	 -9 * SIZE(A1), %xmm12
	mulpd	%xmm5,  %xmm13
	SUBPD	%xmm13,  %xmm1

	MOVUPS_XL1(-10 * SIZE, X1, %xmm5)

#ifdef PREFETCHW
	PREFETCH	(PREFETCHSIZE) * 4 - 128 + PREOFFSET(X1)
#endif

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	movsd	 -8 * SIZE(A1), %xmm8
	movhpd	 -7 * SIZE(A1), %xmm8
	mulpd	%xmm4,  %xmm9
	SUBPD	%xmm9,  %xmm1

	MOVUPS_XL1( -8 * SIZE, X1, %xmm4)

	pshufd	$0x4e, %xmm12,  %xmm13
	mulpd	%xmm5,  %xmm12
	addpd	%xmm12,  %xmm0
	movsd	 -6 * SIZE(A1), %xmm12
	movhpd	 -5 * SIZE(A1), %xmm12
	mulpd	%xmm5,  %xmm13
	SUBPD	%xmm13,  %xmm1

	MOVUPS_XL1(-6 * SIZE, X1, %xmm5)

	subq	 $-8 * SIZE, A1
	subq	 $-8 * SIZE, X1

	subq	 $1, I
	BRANCH
	jg	.L123
	ALIGN_3

.L124:
	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	movsd	-12 * SIZE(A1), %xmm8
	movhpd	-11 * SIZE(A1), %xmm8
	mulpd	%xmm4,  %xmm9
	SUBPD	%xmm9,  %xmm1

	MOVUPS_XL1(-12 * SIZE, X1, %xmm4)

	pshufd	$0x4e, %xmm12,  %xmm13
	mulpd	%xmm5,  %xmm12
	addpd	%xmm12,  %xmm0
	movsd	-10 * SIZE(A1), %xmm12
	movhpd	 -9 * SIZE(A1), %xmm12
	mulpd	%xmm5,  %xmm13
	SUBPD	%xmm13,  %xmm1

	MOVUPS_XL1(-10 * SIZE, X1, %xmm5)

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	mulpd	%xmm4,  %xmm9
	SUBPD	%xmm9,  %xmm1

	MOVUPS_XL1( -8 * SIZE, X1, %xmm4)

	pshufd	$0x4e, %xmm12,  %xmm13
	mulpd	%xmm5,  %xmm12
	addpd	%xmm12,  %xmm0
	mulpd	%xmm5,  %xmm13
	SUBPD	%xmm13,  %xmm1

	MOVUPS_XL1(-6 * SIZE, X1, %xmm5)

	subq	 $-8 * SIZE, A1
	subq	 $-8 * SIZE, X1
	ALIGN_3

.L125:
	testq	$2, M
	je	.L127

	movsd	-16 * SIZE(A1), %xmm8
	movhpd	-15 * SIZE(A1), %xmm8
	movsd	-14 * SIZE(A1), %xmm12
	movhpd	-13 * SIZE(A1), %xmm12

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	mulpd	%xmm4,  %xmm9
	SUBPD	%xmm9,  %xmm1

	MOVUPS_XL1(-12 * SIZE, X1, %xmm4)

	pshufd	$0x4e, %xmm12,  %xmm13
	mulpd	%xmm5,  %xmm12
	addpd	%xmm12,  %xmm0
	mulpd	%xmm5,  %xmm13
	SUBPD	%xmm13,  %xmm1

	addq	 $4 * SIZE, A1
	ALIGN_3

.L127:
	testq	$1, M
	je	.L129

	movsd	-16 * SIZE(A1), %xmm8
	movhpd	-15 * SIZE(A1), %xmm8

	pshufd	$0x4e, %xmm8,  %xmm9
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	mulpd	%xmm4,  %xmm9
	SUBPD	%xmm9,  %xmm1
	ALIGN_3

.L129:
	pcmpeqb	%xmm11, %xmm11
	psllq	$63,    %xmm11
	shufps	$0xc0, %xmm11, %xmm11

#if (!defined(CONJ) && !defined(XCONJ)) || (defined(CONJ) && defined(XCONJ))
	xorpd	%xmm11, %xmm0
#else
	xorpd	%xmm11, %xmm1
#endif

#ifdef HAVE_SSE3
	haddpd	%xmm1, %xmm0
#else
	movapd	%xmm0, %xmm8
	unpcklpd %xmm1, %xmm0
	unpckhpd %xmm1, %xmm8

	addpd	%xmm8, %xmm0
#endif

	pshufd	$0x4e, %xmm0, %xmm1

	mulpd	ALPHA_R, %xmm0
	mulpd	ALPHA_I, %xmm1

	xorpd	%xmm11, %xmm1

	subpd	%xmm1, %xmm0

	movsd	 0 * SIZE(Y), %xmm4
	movhpd	 1 * SIZE(Y), %xmm4

	addpd	%xmm4, %xmm0

	movlpd	%xmm0,  0 * SIZE(Y1)
	movhpd	%xmm0,  1 * SIZE(Y1)

#if GEMV_UNROLL == 1
	addq	INCY, Y
	addq	INCY, Y1

	cmpq	$1, N
	jge	.L121
#endif


#endif
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
