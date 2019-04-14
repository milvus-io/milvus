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

#if GEMV_UNROLL < 2
#undef  GEMV_UNROLL
#define GEMV_UNROLL 2
#endif

#ifndef WINDOWS_ABI

#define STACKSIZE	128

#define OLD_INCX	 8 + STACKSIZE(%rsp)
#define OLD_Y		16 + STACKSIZE(%rsp)
#define OLD_INCY	24 + STACKSIZE(%rsp)
#define OLD_BUFFER	32 + STACKSIZE(%rsp)
#define ALPHA		48	      (%rsp)

#define MMM	64(%rsp)
#define NN	72(%rsp)
#define AA	80(%rsp)
#define XX	88(%rsp)
#define LDAX	96(%rsp)
#define ALPHAR	104(%rsp)
#define ALPHAI	112(%rsp)

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
#define ALPHA		224	       (%rsp)

#define MMM	232(%rsp)
#define NN	240(%rsp)
#define AA	248(%rsp)
#define	XX	256(%rsp)
#define LDAX	264(%rsp)
#define ALPHAR	272(%rsp)
#define ALPHAI	280(%rsp)

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
#define A1	%r11
#define A2	%r12

#define Y1	%r13
#define BUFFER	%r14

#ifdef ALIGNED_ACCESS
#define MM	%r15
#else
#define MM	M
#endif

#undef SUBPS

#if (!defined(CONJ) && !defined(XCONJ)) || (defined(CONJ) && defined(XCONJ))
#define SUBPS	   subps
#else
#define SUBPS	   addps
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

	movaps	%xmm3,       %xmm0
	movss	OLD_ALPHA_I, %xmm1
#endif
	movq	A, AA
	movq	N, NN
	movq	M, MMM
	movq	LDA, LDAX
	movq	X, XX
	movq	OLD_Y,     Y
	movss	%xmm0,ALPHAR
	movss	%xmm1,ALPHAI

.L0t:
	xorq	I,I
	addq	$1,I
	salq	$20,I
	subq	I,MMM
	movq	I,M
	movss	ALPHAR,%xmm0
	movss	ALPHAI,%xmm1
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

	unpcklps %xmm1, %xmm0

	movlps	%xmm0, ALPHA

	testq	M, M
	jle	.L999
	testq	N, N
	jle	.L999
	ALIGN_3

	subq	$-32 * SIZE, A

	movq	BUFFER, Y1

	pxor	%xmm4, %xmm4

	movq	M,   %rax
	addq	$8,  %rax
	sarq	$3,  %rax
	ALIGN_3

.L01:
	movaps	%xmm4,  0 * SIZE(Y1)
	movaps	%xmm4,  4 * SIZE(Y1)
	movaps	%xmm4,  8 * SIZE(Y1)
	movaps	%xmm4, 12 * SIZE(Y1)

	subq	$-16 * SIZE, Y1
	decq	%rax
	jg	.L01
	ALIGN_3

.L10:
#ifdef ALIGNED_ACCESS
	movq	M, MM

	movq	A, %rax
	andq	$4 * SIZE - 1, %rax
	leaq	2 * SIZE(BUFFER), A1
	leaq	-1(M), A2

	cmpq	$2 * SIZE, %rax
	cmovge	A1, BUFFER
	cmovge	A2, MM

	testq	$SIZE, A
	jne	.L200

	testq	$2 * SIZE, LDA
	jne	.L100
#endif

#if GEMV_UNROLL >= 4
	cmpq	$4, N
	jl	.L20
	ALIGN_3

.L11:
	subq	$4, N

	leaq	32 * SIZE(BUFFER), Y1
	movq	A,  A1
	leaq	(A,  LDA, 2), A2
	leaq	(A,  LDA, 4), A

	movsd	(X), %xmm9
	addq	INCX, X
	movsd	(X), %xmm11
	addq	INCX, X
	movsd	(X), %xmm13
	addq	INCX, X
	movsd	(X), %xmm15
	addq	INCX, X

#ifdef HAVE_SSE3
	movddup	ALPHA, %xmm6
#else
	movsd	ALPHA, %xmm6
	unpcklpd %xmm6, %xmm6
#endif

	pshufd	$0xb1, %xmm6, %xmm5

	pcmpeqb	%xmm7, %xmm7
	psllq	$63,   %xmm7

	pshufd	$0x00, %xmm9,  %xmm8
	pshufd	$0x55, %xmm9,  %xmm9
	pshufd	$0x00, %xmm11, %xmm10
	pshufd	$0x55, %xmm11, %xmm11
	pshufd	$0x00, %xmm13, %xmm12
	pshufd	$0x55, %xmm13, %xmm13
	pshufd	$0x00, %xmm15, %xmm14
	pshufd	$0x55, %xmm15, %xmm15

#ifndef XCONJ
	xorps	 %xmm7, %xmm9
	xorps	 %xmm7, %xmm11
	xorps	 %xmm7, %xmm13
	xorps	 %xmm7, %xmm15
#else
	xorps	 %xmm7, %xmm8
	xorps	 %xmm7, %xmm10
	xorps	 %xmm7, %xmm12
	xorps	 %xmm7, %xmm14
#endif

	mulps	 %xmm6, %xmm8
	mulps	 %xmm5, %xmm9
	mulps	 %xmm6, %xmm10
	mulps	 %xmm5, %xmm11
	mulps	 %xmm6, %xmm12
	mulps	 %xmm5, %xmm13
	mulps	 %xmm6, %xmm14
	mulps	 %xmm5, %xmm15

#ifndef XCONJ
	subps	 %xmm9,  %xmm8
	subps	 %xmm11, %xmm10
	subps	 %xmm13, %xmm12
	subps	 %xmm15, %xmm14
#else
	addps	 %xmm9,  %xmm8
	addps	 %xmm11, %xmm10
	addps	 %xmm13, %xmm12
	addps	 %xmm15, %xmm14
#endif

	pshufd	$0x55, %xmm8,  %xmm9
	pshufd	$0x00, %xmm8,  %xmm8
	pshufd	$0x55, %xmm10, %xmm11
	pshufd	$0x00, %xmm10, %xmm10
	pshufd	$0x55, %xmm12, %xmm13
	pshufd	$0x00, %xmm12, %xmm12
	pshufd	$0x55, %xmm14, %xmm15
	pshufd	$0x00, %xmm14, %xmm14

#ifndef CONJ
	xorps	 %xmm7, %xmm9
	xorps	 %xmm7, %xmm11
	xorps	 %xmm7, %xmm13
	xorps	 %xmm7, %xmm15
#else
	xorps	 %xmm7, %xmm8
	xorps	 %xmm7, %xmm10
	xorps	 %xmm7, %xmm12
	xorps	 %xmm7, %xmm14
#endif

#ifdef ALIGNED_ACCESS
	cmpq	M, MM
	je	.L1X

	movsd	 -32 * SIZE(A1), %xmm4
	movsd	 -32 * SIZE(A1, LDA), %xmm6

	movsd	 -32 * SIZE(Y1), %xmm0

	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm8,  %xmm4
	addps	 %xmm4,  %xmm0
	movsd	 -32 * SIZE(A2), %xmm4
	pshufd	 $0xb1, %xmm6, %xmm7
	mulps	 %xmm9,  %xmm5
	SUBPS	 %xmm5,  %xmm0

	mulps	 %xmm10, %xmm6
	addps	 %xmm6,  %xmm0
	movsd	 -32 * SIZE(A2, LDA), %xmm6
	mulps	 %xmm11, %xmm7
	SUBPS	 %xmm7,  %xmm0

	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	pshufd	 $0xb1, %xmm6, %xmm7
	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0

	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm0
	mulps	 %xmm15, %xmm7
	SUBPS	 %xmm7,  %xmm0

	movlps	 %xmm0, -32 * SIZE(Y1)

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, Y1
	ALIGN_3
.L1X:
#endif

	movaps	 -32 * SIZE(Y1), %xmm0
	movaps	 -28 * SIZE(Y1), %xmm1
	movaps	 -24 * SIZE(Y1), %xmm2
	movaps	 -20 * SIZE(Y1), %xmm3

	movq	MM,  I
	sarq	$3,  I
	jle	.L15

	MOVUPS_A1(-32 * SIZE, A1, %xmm4)
	MOVUPS_A1(-28 * SIZE, A1, %xmm6)

	decq	 I
	jle	 .L14
	ALIGN_3

.L13:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A1)
#endif

	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm8,  %xmm4
	addps	 %xmm4,  %xmm0
	MOVUPS_A1(-24 * SIZE, A1, %xmm4)
	pshufd	 $0xb1, %xmm6, %xmm7
	mulps	 %xmm8,  %xmm6
	addps	 %xmm6,  %xmm1
	MOVUPS_A1(-20 * SIZE, A1, %xmm6)

	mulps	 %xmm9,  %xmm5
	SUBPS	 %xmm5,  %xmm0
	mulps	 %xmm9,  %xmm7
	SUBPS	 %xmm7,  %xmm1

	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm8,  %xmm4
	addps	 %xmm4,  %xmm2
	MOVUPS_A2(-32 * SIZE, A1, LDA, 1, %xmm4)
	pshufd	 $0xb1, %xmm6, %xmm7
	mulps	 %xmm8,  %xmm6
	addps	 %xmm6,  %xmm3
	MOVUPS_A2(-28 * SIZE, A1, LDA, 1, %xmm6)

	mulps	 %xmm9,  %xmm5
	SUBPS	 %xmm5,  %xmm2
	mulps	 %xmm9,  %xmm7
	SUBPS	 %xmm7,  %xmm3

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A1, LDA)
#endif

	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm10, %xmm4
	addps	 %xmm4,  %xmm0
	MOVUPS_A2(-24 * SIZE, A1, LDA, 1, %xmm4)
	pshufd	 $0xb1, %xmm6, %xmm7
	mulps	 %xmm10, %xmm6
	addps	 %xmm6,  %xmm1
	MOVUPS_A2(-20 * SIZE, A1, LDA, 1, %xmm6)

	mulps	 %xmm11, %xmm5
	SUBPS	 %xmm5,  %xmm0
	mulps	 %xmm11, %xmm7
	SUBPS	 %xmm7,  %xmm1

	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm10, %xmm4
	addps	 %xmm4,  %xmm2
	MOVUPS_A1(-32 * SIZE, A2, %xmm4)
	pshufd	 $0xb1, %xmm6, %xmm7
	mulps	 %xmm10, %xmm6
	addps	 %xmm6,  %xmm3
	MOVUPS_A1(-28 * SIZE, A2, %xmm6)

	mulps	 %xmm11, %xmm5
	SUBPS	 %xmm5,  %xmm2
	mulps	 %xmm11, %xmm7
	SUBPS	 %xmm7,  %xmm3

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A2)
#endif

	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	MOVUPS_A1(-24 * SIZE, A2, %xmm4)
	pshufd	 $0xb1, %xmm6, %xmm7
	mulps	 %xmm12, %xmm6
	addps	 %xmm6,  %xmm1
	MOVUPS_A1(-20 * SIZE, A2, %xmm6)

	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0
	mulps	 %xmm13, %xmm7
	SUBPS	 %xmm7,  %xmm1

	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm2
	MOVUPS_A2(-32 * SIZE, A2, LDA, 1, %xmm4)
	pshufd	 $0xb1, %xmm6, %xmm7
	mulps	 %xmm12, %xmm6
	addps	 %xmm6,  %xmm3
	MOVUPS_A2(-28 * SIZE, A2, LDA, 1, %xmm6)

	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm2
	mulps	 %xmm13, %xmm7
	SUBPS	 %xmm7,  %xmm3

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A2, LDA)
#endif

	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm14, %xmm4
	addps	 %xmm4,  %xmm0
	MOVUPS_A2(-24 * SIZE, A2, LDA, 1, %xmm4)
	pshufd	 $0xb1, %xmm6, %xmm7
	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm1
	MOVUPS_A2(-20 * SIZE, A2, LDA, 1, %xmm6)

	mulps	 %xmm15, %xmm5
	SUBPS	 %xmm5,  %xmm0
	mulps	 %xmm15, %xmm7
	SUBPS	 %xmm7,  %xmm1

	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm14, %xmm4
	addps	 %xmm4,  %xmm2
	MOVUPS_A1(-16 * SIZE, A1, %xmm4)
	pshufd	 $0xb1, %xmm6, %xmm7
	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm3
	MOVUPS_A1(-12 * SIZE, A1, %xmm6)

	mulps	 %xmm15, %xmm5
	SUBPS	 %xmm5,  %xmm2
	mulps	 %xmm15, %xmm7
	SUBPS	 %xmm7,  %xmm3

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE) - 128 + PREOFFSET(Y1)
#endif

	movaps	 %xmm0, -32 * SIZE(Y1)
	movaps	 %xmm1, -28 * SIZE(Y1)
	movaps	 %xmm2, -24 * SIZE(Y1)
	movaps	 %xmm3, -20 * SIZE(Y1)

	movaps	 -16 * SIZE(Y1), %xmm0
	movaps	 -12 * SIZE(Y1), %xmm1
	movaps	  -8 * SIZE(Y1), %xmm2
	movaps	  -4 * SIZE(Y1), %xmm3

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, A2
	subq	 $-16 * SIZE, Y1

	subq	 $1, I
	BRANCH
	jg	.L13
	ALIGN_3

.L14:
	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm8,  %xmm4
	addps	 %xmm4,  %xmm0
	MOVUPS_A1(-24 * SIZE, A1, %xmm4)
	pshufd	 $0xb1, %xmm6, %xmm7
	mulps	 %xmm8,  %xmm6
	addps	 %xmm6,  %xmm1
	MOVUPS_A1(-20 * SIZE, A1, %xmm6)

	mulps	 %xmm9,  %xmm5
	SUBPS	 %xmm5,  %xmm0
	mulps	 %xmm9,  %xmm7
	SUBPS	 %xmm7,  %xmm1

	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm8,  %xmm4
	addps	 %xmm4,  %xmm2
	MOVUPS_A2(-32 * SIZE, A1, LDA, 1, %xmm4)
	pshufd	 $0xb1, %xmm6, %xmm7
	mulps	 %xmm8,  %xmm6
	addps	 %xmm6,  %xmm3
	MOVUPS_A2(-28 * SIZE, A1, LDA, 1, %xmm6)

	mulps	 %xmm9,  %xmm5
	SUBPS	 %xmm5,  %xmm2
	mulps	 %xmm9,  %xmm7
	SUBPS	 %xmm7,  %xmm3

	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm10, %xmm4
	addps	 %xmm4,  %xmm0
	MOVUPS_A2(-24 * SIZE, A1, LDA, 1, %xmm4)
	pshufd	 $0xb1, %xmm6, %xmm7
	mulps	 %xmm10, %xmm6
	addps	 %xmm6,  %xmm1
	MOVUPS_A2(-20 * SIZE, A1, LDA, 1, %xmm6)

	mulps	 %xmm11, %xmm5
	SUBPS	 %xmm5,  %xmm0
	mulps	 %xmm11, %xmm7
	SUBPS	 %xmm7,  %xmm1

	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm10, %xmm4
	addps	 %xmm4,  %xmm2
	MOVUPS_A1(-32 * SIZE, A2, %xmm4)
	pshufd	 $0xb1, %xmm6, %xmm7
	mulps	 %xmm10, %xmm6
	addps	 %xmm6,  %xmm3
	MOVUPS_A1(-28 * SIZE, A2, %xmm6)

	mulps	 %xmm11, %xmm5
	SUBPS	 %xmm5,  %xmm2
	mulps	 %xmm11, %xmm7
	SUBPS	 %xmm7,  %xmm3

	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	MOVUPS_A1(-24 * SIZE, A2, %xmm4)
	pshufd	 $0xb1, %xmm6, %xmm7
	mulps	 %xmm12, %xmm6
	addps	 %xmm6,  %xmm1
	MOVUPS_A1(-20 * SIZE, A2, %xmm6)

	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0
	mulps	 %xmm13, %xmm7
	SUBPS	 %xmm7,  %xmm1

	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm2
	MOVUPS_A2(-32 * SIZE, A2, LDA, 1, %xmm4)
	pshufd	 $0xb1, %xmm6, %xmm7
	mulps	 %xmm12, %xmm6
	addps	 %xmm6,  %xmm3
	MOVUPS_A2(-28 * SIZE, A2, LDA, 1, %xmm6)

	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm2
	mulps	 %xmm13, %xmm7
	SUBPS	 %xmm7,  %xmm3

	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm14, %xmm4
	addps	 %xmm4,  %xmm0
	MOVUPS_A2(-24 * SIZE, A2, LDA, 1, %xmm4)
	pshufd	 $0xb1, %xmm6, %xmm7
	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm1
	MOVUPS_A2(-20 * SIZE, A2, LDA, 1, %xmm6)

	mulps	 %xmm15, %xmm5
	SUBPS	 %xmm5,  %xmm0
	mulps	 %xmm15, %xmm7
	SUBPS	 %xmm7,  %xmm1

	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm14, %xmm4
	addps	 %xmm4,  %xmm2
	pshufd	 $0xb1, %xmm6, %xmm7
	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm3

	mulps	 %xmm15, %xmm5
	SUBPS	 %xmm5,  %xmm2
	mulps	 %xmm15, %xmm7
	SUBPS	 %xmm7,  %xmm3

	movaps	 %xmm0, -32 * SIZE(Y1)
	movaps	 %xmm1, -28 * SIZE(Y1)
	movaps	 %xmm2, -24 * SIZE(Y1)
	movaps	 %xmm3, -20 * SIZE(Y1)

	movaps	 -16 * SIZE(Y1), %xmm0
	movaps	 -12 * SIZE(Y1), %xmm1
	movaps	  -8 * SIZE(Y1), %xmm2
	movaps	  -4 * SIZE(Y1), %xmm3

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, A2
	subq	 $-16 * SIZE, Y1
	ALIGN_3

.L15:
	testq	$4, MM
	je	.L17

	MOVUPS_A1(-32 * SIZE, A1, %xmm4)
	MOVUPS_A1(-28 * SIZE, A1, %xmm6)

	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm8,  %xmm4
	addps	 %xmm4,  %xmm0
	pshufd	 $0xb1, %xmm6, %xmm7
	mulps	 %xmm8,  %xmm6
	addps	 %xmm6,  %xmm1

	mulps	 %xmm9,  %xmm5
	SUBPS	 %xmm5,  %xmm0
	mulps	 %xmm9,  %xmm7
	SUBPS	 %xmm7,  %xmm1

	MOVUPS_A2(-32 * SIZE, A1, LDA, 1, %xmm4)
	MOVUPS_A2(-28 * SIZE, A1, LDA, 1, %xmm6)

	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm10, %xmm4
	addps	 %xmm4,  %xmm0
	pshufd	 $0xb1, %xmm6, %xmm7
	mulps	 %xmm10, %xmm6
	addps	 %xmm6,  %xmm1

	mulps	 %xmm11, %xmm5
	SUBPS	 %xmm5,  %xmm0
	mulps	 %xmm11, %xmm7
	SUBPS	 %xmm7,  %xmm1

	MOVUPS_A1(-32 * SIZE, A2, %xmm4)
	MOVUPS_A1(-28 * SIZE, A2, %xmm6)

	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	pshufd	 $0xb1, %xmm6, %xmm7
	mulps	 %xmm12, %xmm6
	addps	 %xmm6,  %xmm1

	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0
	mulps	 %xmm13, %xmm7
	SUBPS	 %xmm7,  %xmm1

	MOVUPS_A2(-32 * SIZE, A2, LDA, 1, %xmm4)
	MOVUPS_A2(-28 * SIZE, A2, LDA, 1, %xmm6)

	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm14, %xmm4
	addps	 %xmm4,  %xmm0
	pshufd	 $0xb1, %xmm6, %xmm7
	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm1

	mulps	 %xmm15, %xmm5
	SUBPS	 %xmm5,  %xmm0
	mulps	 %xmm15, %xmm7
	SUBPS	 %xmm7,  %xmm1

	movaps	 %xmm0, -32 * SIZE(Y1)
	movaps	 %xmm1, -28 * SIZE(Y1)

	movaps	 %xmm2, %xmm0
	movaps	 %xmm3, %xmm1

	addq	 $8 * SIZE, A1
	addq	 $8 * SIZE, A2
	addq	 $8 * SIZE, Y1
	ALIGN_3

.L17:
	testq	$2, MM
	je	.L18

	MOVUPS_A1(-32 * SIZE, A1, %xmm4)
	MOVUPS_A2(-32 * SIZE, A1, LDA, 1, %xmm6)

	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm8,  %xmm4
	addps	 %xmm4,  %xmm0
	MOVUPS_A1(-32 * SIZE, A2, %xmm4)
	pshufd	 $0xb1, %xmm6, %xmm7
	mulps	 %xmm9,  %xmm5
	SUBPS	 %xmm5,  %xmm0

	mulps	 %xmm10, %xmm6
	addps	 %xmm6,  %xmm0
	MOVUPS_A2(-32 * SIZE, A2, LDA, 1, %xmm6)
	mulps	 %xmm11, %xmm7
	SUBPS	 %xmm7,  %xmm0

	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	pshufd	 $0xb1, %xmm6, %xmm7
	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0

	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm0
	mulps	 %xmm15, %xmm7
	SUBPS	 %xmm7,  %xmm0

	movaps	 %xmm0, -32 * SIZE(Y1)
	movaps	 %xmm1, %xmm0

	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, A2
	addq	 $4 * SIZE, Y1
	ALIGN_3

.L18:
	testq	$1, MM
	je	.L19

	movsd	 -32 * SIZE(A1), %xmm4
	movsd	 -32 * SIZE(A1, LDA), %xmm6

	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm8,  %xmm4
	addps	 %xmm4,  %xmm0
	movsd	 -32 * SIZE(A2), %xmm4
	pshufd	 $0xb1, %xmm6, %xmm7
	mulps	 %xmm9,  %xmm5
	SUBPS	 %xmm5,  %xmm0

	mulps	 %xmm10, %xmm6
	addps	 %xmm6,  %xmm0
	movsd	 -32 * SIZE(A2, LDA), %xmm6
	mulps	 %xmm11, %xmm7
	SUBPS	 %xmm7,  %xmm0

	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	pshufd	 $0xb1, %xmm6, %xmm7
	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0

	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm0
	mulps	 %xmm15, %xmm7
	SUBPS	 %xmm7,  %xmm0

	movlps	 %xmm0, -32 * SIZE(Y1)
	ALIGN_3

.L19:
	cmpq	$4, N
	jge	.L11
	ALIGN_3

.L20:
#endif

	cmpq	$2, N
	jl	.L30

#if GEMV_UNROLL == 2
	ALIGN_3

.L21:
#endif

	subq	$2, N

	leaq	32 * SIZE(BUFFER), Y1
	movq	A,  A1
	leaq	(A,  LDA, 1), A2
	leaq	(A,  LDA, 2), A

	movsd	(X), %xmm13
	addq	INCX, X
	movsd	(X), %xmm15
	addq	INCX, X

#ifdef HAVE_SSE3
	movddup	ALPHA, %xmm8
#else
	movsd	ALPHA, %xmm8
	unpcklpd %xmm8, %xmm8
#endif

	pshufd	$0xb1, %xmm8, %xmm9

	pcmpeqb	%xmm11, %xmm11
	psllq	$63,    %xmm11

	pshufd	$0x00, %xmm13, %xmm12
	pshufd	$0x55, %xmm13, %xmm13
	pshufd	$0x00, %xmm15, %xmm14
	pshufd	$0x55, %xmm15, %xmm15

#ifndef XCONJ
	xorps	 %xmm11, %xmm13
	xorps	 %xmm11, %xmm15
#else
	xorps	 %xmm11, %xmm12
	xorps	 %xmm11, %xmm14
#endif

	mulps	 %xmm8, %xmm12
	mulps	 %xmm9, %xmm13
	mulps	 %xmm8, %xmm14
	mulps	 %xmm9, %xmm15

#ifndef XCONJ
	subps	 %xmm13, %xmm12
	subps	 %xmm15, %xmm14
#else
	addps	 %xmm13, %xmm12
	addps	 %xmm15, %xmm14
#endif

	pshufd	$0x55, %xmm12, %xmm13
	pshufd	$0x00, %xmm12, %xmm12
	pshufd	$0x55, %xmm14, %xmm15
	pshufd	$0x00, %xmm14, %xmm14

#ifndef CONJ
	xorps	 %xmm11, %xmm13
	xorps	 %xmm11, %xmm15
#else
	xorps	 %xmm11, %xmm12
	xorps	 %xmm11, %xmm14
#endif

#ifdef ALIGNED_ACCESS
	cmpq	M, MM
	je	.L2X

	movsd	 -32 * SIZE(A1), %xmm4
	movsd	 -32 * SIZE(A2), %xmm6

	movsd	 -32 * SIZE(Y1), %xmm0

	pshufd	 $0xb1, %xmm4, %xmm5
	pshufd	 $0xb1, %xmm6, %xmm7

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0

	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm0
	mulps	 %xmm15, %xmm7
	SUBPS	 %xmm7,  %xmm0

	movlps	 %xmm0, -32 * SIZE(Y1)

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, Y1
	ALIGN_3
.L2X:
#endif

	movaps	 -32 * SIZE(Y1), %xmm0
	movaps	 -28 * SIZE(Y1), %xmm1
	movaps	 -24 * SIZE(Y1), %xmm2
	movaps	 -20 * SIZE(Y1), %xmm3
	ALIGN_3

	movq	MM,  I
	sarq	$3,  I
	jle	.L25

	MOVUPS_A1(-32 * SIZE, A1, %xmm4)
	MOVUPS_A1(-28 * SIZE, A1, %xmm6)
	MOVUPS_A1(-24 * SIZE, A1, %xmm8)
	MOVUPS_A1(-20 * SIZE, A1, %xmm10)

	decq	 I
	jle	 .L24
	ALIGN_3

.L23:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(A2)
#endif

	pshufd	 $0xb1, %xmm4,  %xmm5
	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	MOVUPS_A1(-32 * SIZE, A2, %xmm4)
	pshufd	 $0xb1, %xmm6,  %xmm7
	mulps	 %xmm12, %xmm6
	addps	 %xmm6,  %xmm1
	MOVUPS_A1(-28 * SIZE, A2, %xmm6)

	pshufd	 $0xb1, %xmm8,  %xmm9
	mulps	 %xmm12, %xmm8
	addps	 %xmm8,  %xmm2
	MOVUPS_A1(-24 * SIZE, A2, %xmm8)
	pshufd	 $0xb1, %xmm10, %xmm11
	mulps	 %xmm12, %xmm10
	addps	 %xmm10, %xmm3
	MOVUPS_A1(-20 * SIZE, A2, %xmm10)

	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0
	mulps	 %xmm13, %xmm7
	SUBPS	 %xmm7,  %xmm1

	mulps	 %xmm13, %xmm9
	SUBPS	 %xmm9,  %xmm2
	mulps	 %xmm13, %xmm11
	SUBPS	 %xmm11, %xmm3

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(A1)
#endif

	pshufd	 $0xb1, %xmm4,  %xmm5
	mulps	 %xmm14, %xmm4
	addps	 %xmm4,  %xmm0
	MOVUPS_A1(-16 * SIZE, A1, %xmm4)
	pshufd	 $0xb1, %xmm6,  %xmm7
	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm1
	MOVUPS_A1(-12 * SIZE, A1, %xmm6)

	pshufd	 $0xb1, %xmm8,  %xmm9
	mulps	 %xmm14, %xmm8
	addps	 %xmm8,  %xmm2
	MOVUPS_A1( -8 * SIZE, A1, %xmm8)
	pshufd	 $0xb1, %xmm10, %xmm11
	mulps	 %xmm14, %xmm10
	addps	 %xmm10, %xmm3
	MOVUPS_A1( -4 * SIZE, A1, %xmm10)

	mulps	 %xmm15, %xmm5
	SUBPS	 %xmm5,  %xmm0
	mulps	 %xmm15, %xmm7
	SUBPS	 %xmm7,  %xmm1

	mulps	 %xmm15, %xmm9
	SUBPS	 %xmm9,  %xmm2
	mulps	 %xmm15, %xmm11
	SUBPS	 %xmm11, %xmm3

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(Y1)
#endif

	movaps	 %xmm0, -32 * SIZE(Y1)
	movaps	 %xmm1, -28 * SIZE(Y1)
	movaps	 %xmm2, -24 * SIZE(Y1)
	movaps	 %xmm3, -20 * SIZE(Y1)

	movaps	 -16 * SIZE(Y1), %xmm0
	movaps	 -12 * SIZE(Y1), %xmm1
	movaps	  -8 * SIZE(Y1), %xmm2
	movaps	  -4 * SIZE(Y1), %xmm3

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, A2
	subq	 $-16 * SIZE, Y1

	subq	 $1, I
	BRANCH
	jg	.L23
	ALIGN_3

.L24:
	pshufd	 $0xb1, %xmm4,  %xmm5
	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	MOVUPS_A1(-32 * SIZE, A2, %xmm4)
	pshufd	 $0xb1, %xmm6,  %xmm7
	mulps	 %xmm12, %xmm6
	addps	 %xmm6,  %xmm1
	MOVUPS_A1(-28 * SIZE, A2, %xmm6)

	pshufd	 $0xb1, %xmm8,  %xmm9
	mulps	 %xmm12, %xmm8
	addps	 %xmm8,  %xmm2
	MOVUPS_A1(-24 * SIZE, A2, %xmm8)
	pshufd	 $0xb1, %xmm10, %xmm11
	mulps	 %xmm12, %xmm10
	addps	 %xmm10, %xmm3
	MOVUPS_A1(-20 * SIZE, A2, %xmm10)

	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0
	mulps	 %xmm13, %xmm7
	SUBPS	 %xmm7,  %xmm1

	mulps	 %xmm13, %xmm9
	SUBPS	 %xmm9,  %xmm2
	mulps	 %xmm13, %xmm11
	SUBPS	 %xmm11, %xmm3

	pshufd	 $0xb1, %xmm4,  %xmm5
	mulps	 %xmm14, %xmm4
	addps	 %xmm4,  %xmm0
	pshufd	 $0xb1, %xmm6,  %xmm7
	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm1

	pshufd	 $0xb1, %xmm8,  %xmm9
	mulps	 %xmm14, %xmm8
	addps	 %xmm8,  %xmm2
	pshufd	 $0xb1, %xmm10, %xmm11
	mulps	 %xmm14, %xmm10
	addps	 %xmm10, %xmm3

	mulps	 %xmm15, %xmm5
	SUBPS	 %xmm5,  %xmm0
	mulps	 %xmm15, %xmm7
	SUBPS	 %xmm7,  %xmm1

	mulps	 %xmm15, %xmm9
	SUBPS	 %xmm9,  %xmm2
	mulps	 %xmm15, %xmm11
	SUBPS	 %xmm11, %xmm3

	movaps	 %xmm0, -32 * SIZE(Y1)
	movaps	 %xmm1, -28 * SIZE(Y1)
	movaps	 %xmm2, -24 * SIZE(Y1)
	movaps	 %xmm3, -20 * SIZE(Y1)

	movaps	 -16 * SIZE(Y1), %xmm0
	movaps	 -12 * SIZE(Y1), %xmm1
	movaps	  -8 * SIZE(Y1), %xmm2
	movaps	  -4 * SIZE(Y1), %xmm3

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, A2
	subq	 $-16 * SIZE, Y1
	ALIGN_3

.L25:
	testq	$4, MM
	je	.L27

	MOVUPS_A1(-32 * SIZE, A1, %xmm4)
	MOVUPS_A1(-28 * SIZE, A1, %xmm6)
	MOVUPS_A1(-32 * SIZE, A2, %xmm8)
	MOVUPS_A1(-28 * SIZE, A2, %xmm10)

	pshufd	 $0xb1, %xmm4,  %xmm5
	pshufd	 $0xb1, %xmm6,  %xmm7
	pshufd	 $0xb1, %xmm8,  %xmm9
	pshufd	 $0xb1, %xmm10, %xmm11

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	mulps	 %xmm12, %xmm6
	addps	 %xmm6,  %xmm1

	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0
	mulps	 %xmm13, %xmm7
	SUBPS	 %xmm7,  %xmm1

	mulps	 %xmm14, %xmm8
	addps	 %xmm8,  %xmm0
	mulps	 %xmm14, %xmm10
	addps	 %xmm10, %xmm1

	mulps	 %xmm15, %xmm9
	SUBPS	 %xmm9,  %xmm0
	mulps	 %xmm15, %xmm11
	SUBPS	 %xmm11, %xmm1

	movaps	 %xmm0, -32 * SIZE(Y1)
	movaps	 %xmm1, -28 * SIZE(Y1)

	movaps	 %xmm2, %xmm0
	movaps	 %xmm3, %xmm1

	addq	 $8 * SIZE, A1
	addq	 $8 * SIZE, A2
	addq	 $8 * SIZE, Y1
	ALIGN_3

.L27:
	testq	$2, MM
	je	.L28

	MOVUPS_A1(-32 * SIZE, A1, %xmm4)
	MOVUPS_A1(-32 * SIZE, A2, %xmm6)

	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	pshufd	 $0xb1, %xmm6, %xmm7
	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0

	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm0
	mulps	 %xmm15, %xmm7
	SUBPS	 %xmm7,  %xmm0

	movaps	 %xmm0, -32 * SIZE(Y1)
	movaps	 %xmm1, %xmm0

	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, A2
	addq	 $4 * SIZE, Y1
	ALIGN_3

.L28:
	testq	$1, MM
#if GEMV_UNROLL == 2
	je	.L29
#else
	je	.L30
#endif

	movsd	 -32 * SIZE(A1), %xmm4
	movsd	 -32 * SIZE(A2), %xmm6

	pshufd	 $0xb1, %xmm4, %xmm5
	pshufd	 $0xb1, %xmm6, %xmm7

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0

	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm0
	mulps	 %xmm15, %xmm7
	SUBPS	 %xmm7,  %xmm0

	movlps	 %xmm0, -32 * SIZE(Y1)

#if GEMV_UNROLL == 2
	ALIGN_3

.L29:
	cmpq	$2, N
	jge	.L21
#endif
	ALIGN_3

.L30:
	cmpq	$1, N
	jl	.L990

	leaq	32 * SIZE(BUFFER), Y1
	movq	A,  A1

	movsd	(X), %xmm13
	addq	INCX, X

#ifdef HAVE_SSE3
	movddup	ALPHA, %xmm8
#else
	movsd	ALPHA, %xmm8
	unpcklpd %xmm8, %xmm8
#endif

	pshufd	$0xb1, %xmm8, %xmm9

	pcmpeqb	%xmm11, %xmm11
	psllq	$63,    %xmm11

	pshufd	$0x00, %xmm13, %xmm12
	pshufd	$0x55, %xmm13, %xmm13

#ifndef XCONJ
	xorps	 %xmm11, %xmm13
#else
	xorps	 %xmm11, %xmm12
#endif

	mulps	 %xmm8, %xmm12
	mulps	 %xmm9, %xmm13

#ifndef XCONJ
	subps	 %xmm13, %xmm12
#else
	addps	 %xmm13, %xmm12
#endif

	pshufd	$0x55, %xmm12, %xmm13
	pshufd	$0x00, %xmm12, %xmm12

#ifndef CONJ
	xorps	 %xmm11, %xmm13
#else
	xorps	 %xmm11, %xmm12
#endif

#ifdef ALIGNED_ACCESS
	cmpq	M, MM
	je	.L3X

	movsd	 -32 * SIZE(A1), %xmm4
	movsd	 -32 * SIZE(Y1), %xmm0

	pshufd	 $0xb1, %xmm4, %xmm5

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0

	movlps	 %xmm0, -32 * SIZE(Y1)

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, Y1
	ALIGN_3
.L3X:
#endif

	movaps	 -32 * SIZE(Y1), %xmm0
	movaps	 -28 * SIZE(Y1), %xmm1
	movaps	 -24 * SIZE(Y1), %xmm2
	movaps	 -20 * SIZE(Y1), %xmm3
	ALIGN_3

	movq	MM,  I
	sarq	$3,  I
	jle	.L35

	MOVUPS_A1(-32 * SIZE, A1, %xmm4)
	MOVUPS_A1(-28 * SIZE, A1, %xmm6)
	MOVUPS_A1(-24 * SIZE, A1, %xmm8)
	MOVUPS_A1(-20 * SIZE, A1, %xmm10)

	decq	 I
	jle	 .L34
	ALIGN_3

.L33:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 4 - 128 + PREOFFSET(A1)
#endif

	pshufd	 $0xb1, %xmm4,  %xmm5
	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	MOVUPS_A1(-16 * SIZE, A1, %xmm4)
	pshufd	 $0xb1, %xmm6,  %xmm7
	mulps	 %xmm12, %xmm6
	addps	 %xmm6,  %xmm1
	MOVUPS_A1(-12 * SIZE, A1, %xmm6)

	pshufd	 $0xb1, %xmm8,  %xmm9
	mulps	 %xmm12, %xmm8
	addps	 %xmm8,  %xmm2
	MOVUPS_A1( -8 * SIZE, A1, %xmm8)
	pshufd	 $0xb1, %xmm10, %xmm11
	mulps	 %xmm12, %xmm10
	addps	 %xmm10, %xmm3
	MOVUPS_A1( -4 * SIZE, A1, %xmm10)

	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0
	mulps	 %xmm13, %xmm7
	SUBPS	 %xmm7,  %xmm1

	mulps	 %xmm13, %xmm9
	SUBPS	 %xmm9,  %xmm2
	mulps	 %xmm13, %xmm11
	SUBPS	 %xmm11, %xmm3

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE) * 4 - 128 + PREOFFSET(Y1)
#endif

	movaps	 %xmm0, -32 * SIZE(Y1)
	movaps	 %xmm1, -28 * SIZE(Y1)
	movaps	 %xmm2, -24 * SIZE(Y1)
	movaps	 %xmm3, -20 * SIZE(Y1)

	movaps	 -16 * SIZE(Y1), %xmm0
	movaps	 -12 * SIZE(Y1), %xmm1
	movaps	  -8 * SIZE(Y1), %xmm2
	movaps	  -4 * SIZE(Y1), %xmm3

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, A2
	subq	 $-16 * SIZE, Y1

	subq	 $1, I
	BRANCH
	jg	.L33
	ALIGN_3

.L34:
	pshufd	 $0xb1, %xmm4,  %xmm5
	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	pshufd	 $0xb1, %xmm6,  %xmm7
	mulps	 %xmm12, %xmm6
	addps	 %xmm6,  %xmm1

	pshufd	 $0xb1, %xmm8,  %xmm9
	mulps	 %xmm12, %xmm8
	addps	 %xmm8,  %xmm2
	pshufd	 $0xb1, %xmm10, %xmm11
	mulps	 %xmm12, %xmm10
	addps	 %xmm10, %xmm3

	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0
	mulps	 %xmm13, %xmm7
	SUBPS	 %xmm7,  %xmm1

	mulps	 %xmm13, %xmm9
	SUBPS	 %xmm9,  %xmm2
	mulps	 %xmm13, %xmm11
	SUBPS	 %xmm11, %xmm3

	movaps	 %xmm0, -32 * SIZE(Y1)
	movaps	 %xmm1, -28 * SIZE(Y1)
	movaps	 %xmm2, -24 * SIZE(Y1)
	movaps	 %xmm3, -20 * SIZE(Y1)

	movaps	 -16 * SIZE(Y1), %xmm0
	movaps	 -12 * SIZE(Y1), %xmm1
	movaps	  -8 * SIZE(Y1), %xmm2
	movaps	  -4 * SIZE(Y1), %xmm3

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, Y1
	ALIGN_3

.L35:
	testq	$4, MM
	je	.L37

	MOVUPS_A1(-32 * SIZE, A1, %xmm4)
	MOVUPS_A1(-28 * SIZE, A1, %xmm6)

	pshufd	 $0xb1, %xmm4,  %xmm5
	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	pshufd	 $0xb1, %xmm6,  %xmm7
	mulps	 %xmm12, %xmm6
	addps	 %xmm6,  %xmm1

	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0
	mulps	 %xmm13, %xmm7
	SUBPS	 %xmm7,  %xmm1

	movaps	 %xmm0, -32 * SIZE(Y1)
	movaps	 %xmm1, -28 * SIZE(Y1)

	movaps	 %xmm2, %xmm0
	movaps	 %xmm3, %xmm1

	addq	 $8 * SIZE, A1
	addq	 $8 * SIZE, Y1
	ALIGN_3

.L37:
	testq	$2, MM
	je	.L38

	MOVUPS_A1(-32 * SIZE, A1, %xmm4)

	pshufd	 $0xb1, %xmm4, %xmm5

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0

	movaps	 %xmm0, -32 * SIZE(Y1)
	movaps	 %xmm1, %xmm0

	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, Y1
	ALIGN_3

.L38:
	testq	$1, MM
	je	.L990

	movsd	 -32 * SIZE(A1), %xmm4

	pshufd	 $0xb1, %xmm4, %xmm5

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0

	movlps	 %xmm0, -32 * SIZE(Y1)

#ifdef ALIGNED_ACCESS
	jmp	 .L990
	ALIGN_3

.L100:
#if GEMV_UNROLL >= 4
	cmpq	$4, N
	jl	.L110
	ALIGN_3

.L101:
	subq	$4, N

	leaq	32 * SIZE(BUFFER), Y1
	movq	A,  A1
	leaq	(A,  LDA, 2), A2
	leaq	(A,  LDA, 4), A

	movsd	(X), %xmm9
	addq	INCX, X
	movsd	(X), %xmm11
	addq	INCX, X
	movsd	(X), %xmm13
	addq	INCX, X
	movsd	(X), %xmm15
	addq	INCX, X

#ifdef HAVE_SSE3
	movddup	ALPHA, %xmm6
#else
	movsd	ALPHA, %xmm6
	unpcklpd %xmm6, %xmm6
#endif

	pshufd	$0xb1, %xmm6, %xmm5

	pcmpeqb	%xmm7, %xmm7
	psllq	$63,   %xmm7

	pshufd	$0x00, %xmm9,  %xmm8
	pshufd	$0x55, %xmm9,  %xmm9
	pshufd	$0x00, %xmm11, %xmm10
	pshufd	$0x55, %xmm11, %xmm11
	pshufd	$0x00, %xmm13, %xmm12
	pshufd	$0x55, %xmm13, %xmm13
	pshufd	$0x00, %xmm15, %xmm14
	pshufd	$0x55, %xmm15, %xmm15

#ifndef XCONJ
	xorps	 %xmm7, %xmm9
	xorps	 %xmm7, %xmm11
	xorps	 %xmm7, %xmm13
	xorps	 %xmm7, %xmm15
#else
	xorps	 %xmm7, %xmm8
	xorps	 %xmm7, %xmm10
	xorps	 %xmm7, %xmm12
	xorps	 %xmm7, %xmm14
#endif

	mulps	 %xmm6, %xmm8
	mulps	 %xmm5, %xmm9
	mulps	 %xmm6, %xmm10
	mulps	 %xmm5, %xmm11
	mulps	 %xmm6, %xmm12
	mulps	 %xmm5, %xmm13
	mulps	 %xmm6, %xmm14
	mulps	 %xmm5, %xmm15

#ifndef XCONJ
	subps	 %xmm9,  %xmm8
	subps	 %xmm11, %xmm10
	subps	 %xmm13, %xmm12
	subps	 %xmm15, %xmm14
#else
	addps	 %xmm9,  %xmm8
	addps	 %xmm11, %xmm10
	addps	 %xmm13, %xmm12
	addps	 %xmm15, %xmm14
#endif

	pshufd	$0x55, %xmm8,  %xmm9
	pshufd	$0x00, %xmm8,  %xmm8
	pshufd	$0x55, %xmm10, %xmm11
	pshufd	$0x00, %xmm10, %xmm10
	pshufd	$0x55, %xmm12, %xmm13
	pshufd	$0x00, %xmm12, %xmm12
	pshufd	$0x55, %xmm14, %xmm15
	pshufd	$0x00, %xmm14, %xmm14

#ifndef CONJ
	xorps	 %xmm7, %xmm9
	xorps	 %xmm7, %xmm11
	xorps	 %xmm7, %xmm13
	xorps	 %xmm7, %xmm15
#else
	xorps	 %xmm7, %xmm8
	xorps	 %xmm7, %xmm10
	xorps	 %xmm7, %xmm12
	xorps	 %xmm7, %xmm14
#endif

#ifdef ALIGNED_ACCESS
	cmpq	M, MM
	je	.L10X

	movsd	 -32 * SIZE(A1), %xmm4
	movsd	 -32 * SIZE(A1, LDA), %xmm6

	movsd	 -32 * SIZE(Y1), %xmm0

	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm8,  %xmm4
	addps	 %xmm4,  %xmm0
	movsd	 -32 * SIZE(A2), %xmm4
	pshufd	 $0xb1, %xmm6, %xmm7
	mulps	 %xmm9,  %xmm5
	SUBPS	 %xmm5,  %xmm0

	mulps	 %xmm10, %xmm6
	addps	 %xmm6,  %xmm0
	movsd	 -32 * SIZE(A2, LDA), %xmm6
	mulps	 %xmm11, %xmm7
	SUBPS	 %xmm7,  %xmm0

	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	pshufd	 $0xb1, %xmm6, %xmm7
	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0

	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm0
	mulps	 %xmm15, %xmm7
	SUBPS	 %xmm7,  %xmm0

	movlps	 %xmm0, -32 * SIZE(Y1)

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, Y1
	ALIGN_3
.L10X:
#endif

	movaps	 -32 * SIZE(Y1), %xmm0
	movaps	 -28 * SIZE(Y1), %xmm1
	movaps	 -24 * SIZE(Y1), %xmm2
	movaps	 -20 * SIZE(Y1), %xmm3

	movq	MM,  I
	sarq	$3,  I
	jle	.L105

	MOVUPS_A1(-32 * SIZE, A1, %xmm4)
	MOVUPS_A1(-28 * SIZE, A1, %xmm6)

	decq	 I
	jle	 .L104
	ALIGN_3

.L103:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A1)
#endif

	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm8,  %xmm4
	addps	 %xmm4,  %xmm0
	MOVUPS_A1(-24 * SIZE, A1, %xmm4)
	pshufd	 $0xb1, %xmm6, %xmm7
	mulps	 %xmm8,  %xmm6
	addps	 %xmm6,  %xmm1
	MOVUPS_A1(-20 * SIZE, A1, %xmm6)

	mulps	 %xmm9,  %xmm5
	SUBPS	 %xmm5,  %xmm0
	mulps	 %xmm9,  %xmm7
	SUBPS	 %xmm7,  %xmm1

	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm8,  %xmm4
	addps	 %xmm4,  %xmm2
	movsd	-32 * SIZE(A1, LDA), %xmm4
	movhps	-30 * SIZE(A1, LDA), %xmm4
	pshufd	 $0xb1, %xmm6, %xmm7
	mulps	 %xmm8,  %xmm6
	addps	 %xmm6,  %xmm3
	movsd	-28 * SIZE(A1, LDA), %xmm6
	movhps	-26 * SIZE(A1, LDA), %xmm6

	mulps	 %xmm9,  %xmm5
	SUBPS	 %xmm5,  %xmm2
	mulps	 %xmm9,  %xmm7
	SUBPS	 %xmm7,  %xmm3

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A1, LDA)
#endif

	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm10, %xmm4
	addps	 %xmm4,  %xmm0
	movsd	-24 * SIZE(A1, LDA), %xmm4
	movhps	-22 * SIZE(A1, LDA), %xmm4
	pshufd	 $0xb1, %xmm6, %xmm7
	mulps	 %xmm10, %xmm6
	addps	 %xmm6,  %xmm1
	movsd	-20 * SIZE(A1, LDA), %xmm6
	movhps	-18 * SIZE(A1, LDA), %xmm6

	mulps	 %xmm11, %xmm5
	SUBPS	 %xmm5,  %xmm0
	mulps	 %xmm11, %xmm7
	SUBPS	 %xmm7,  %xmm1

	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm10, %xmm4
	addps	 %xmm4,  %xmm2
	MOVUPS_A1(-32 * SIZE, A2, %xmm4)
	pshufd	 $0xb1, %xmm6, %xmm7
	mulps	 %xmm10, %xmm6
	addps	 %xmm6,  %xmm3
	MOVUPS_A1(-28 * SIZE, A2, %xmm6)

	mulps	 %xmm11, %xmm5
	SUBPS	 %xmm5,  %xmm2
	mulps	 %xmm11, %xmm7
	SUBPS	 %xmm7,  %xmm3

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A2)
#endif

	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	MOVUPS_A1(-24 * SIZE, A2, %xmm4)
	pshufd	 $0xb1, %xmm6, %xmm7
	mulps	 %xmm12, %xmm6
	addps	 %xmm6,  %xmm1
	MOVUPS_A1(-20 * SIZE, A2, %xmm6)

	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0
	mulps	 %xmm13, %xmm7
	SUBPS	 %xmm7,  %xmm1

	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm2
	movsd	-32 * SIZE(A2, LDA), %xmm4
	movhps	-30 * SIZE(A2, LDA), %xmm4
	pshufd	 $0xb1, %xmm6, %xmm7
	mulps	 %xmm12, %xmm6
	addps	 %xmm6,  %xmm3
	movsd	-28 * SIZE(A2, LDA), %xmm6
	movhps	-26 * SIZE(A2, LDA), %xmm6

	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm2
	mulps	 %xmm13, %xmm7
	SUBPS	 %xmm7,  %xmm3

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A2, LDA)
#endif

	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm14, %xmm4
	addps	 %xmm4,  %xmm0
	movsd	-24 * SIZE(A2, LDA), %xmm4
	movhps	-22 * SIZE(A2, LDA), %xmm4
	pshufd	 $0xb1, %xmm6, %xmm7
	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm1
	movsd	-20 * SIZE(A2, LDA), %xmm6
	movhps	-18 * SIZE(A2, LDA), %xmm6

	mulps	 %xmm15, %xmm5
	SUBPS	 %xmm5,  %xmm0
	mulps	 %xmm15, %xmm7
	SUBPS	 %xmm7,  %xmm1

	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm14, %xmm4
	addps	 %xmm4,  %xmm2
	MOVUPS_A1(-16 * SIZE, A1, %xmm4)
	pshufd	 $0xb1, %xmm6, %xmm7
	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm3
	MOVUPS_A1(-12 * SIZE, A1, %xmm6)

	mulps	 %xmm15, %xmm5
	SUBPS	 %xmm5,  %xmm2
	mulps	 %xmm15, %xmm7
	SUBPS	 %xmm7,  %xmm3

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE) - 128 + PREOFFSET(Y1)
#endif

	movaps	 %xmm0, -32 * SIZE(Y1)
	movaps	 %xmm1, -28 * SIZE(Y1)
	movaps	 %xmm2, -24 * SIZE(Y1)
	movaps	 %xmm3, -20 * SIZE(Y1)

	movaps	 -16 * SIZE(Y1), %xmm0
	movaps	 -12 * SIZE(Y1), %xmm1
	movaps	  -8 * SIZE(Y1), %xmm2
	movaps	  -4 * SIZE(Y1), %xmm3

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, A2
	subq	 $-16 * SIZE, Y1

	subq	 $1, I
	BRANCH
	jg	.L103
	ALIGN_3

.L104:
	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm8,  %xmm4
	addps	 %xmm4,  %xmm0
	MOVUPS_A1(-24 * SIZE, A1, %xmm4)
	pshufd	 $0xb1, %xmm6, %xmm7
	mulps	 %xmm8,  %xmm6
	addps	 %xmm6,  %xmm1
	MOVUPS_A1(-20 * SIZE, A1, %xmm6)

	mulps	 %xmm9,  %xmm5
	SUBPS	 %xmm5,  %xmm0
	mulps	 %xmm9,  %xmm7
	SUBPS	 %xmm7,  %xmm1

	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm8,  %xmm4
	addps	 %xmm4,  %xmm2
	movsd	-32 * SIZE(A1, LDA), %xmm4
	movhps	-30 * SIZE(A1, LDA), %xmm4
	pshufd	 $0xb1, %xmm6, %xmm7
	mulps	 %xmm8,  %xmm6
	addps	 %xmm6,  %xmm3
	movsd	-28 * SIZE(A1, LDA), %xmm6
	movhps	-26 * SIZE(A1, LDA), %xmm6

	mulps	 %xmm9,  %xmm5
	SUBPS	 %xmm5,  %xmm2
	mulps	 %xmm9,  %xmm7
	SUBPS	 %xmm7,  %xmm3

	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm10, %xmm4
	addps	 %xmm4,  %xmm0
	movsd	-24 * SIZE(A1, LDA), %xmm4
	movhps	-22 * SIZE(A1, LDA), %xmm4
	pshufd	 $0xb1, %xmm6, %xmm7
	mulps	 %xmm10, %xmm6
	addps	 %xmm6,  %xmm1
	movsd	-20 * SIZE(A1, LDA), %xmm6
	movhps	-18 * SIZE(A1, LDA), %xmm6

	mulps	 %xmm11, %xmm5
	SUBPS	 %xmm5,  %xmm0
	mulps	 %xmm11, %xmm7
	SUBPS	 %xmm7,  %xmm1

	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm10, %xmm4
	addps	 %xmm4,  %xmm2
	MOVUPS_A1(-32 * SIZE, A2, %xmm4)
	pshufd	 $0xb1, %xmm6, %xmm7
	mulps	 %xmm10, %xmm6
	addps	 %xmm6,  %xmm3
	MOVUPS_A1(-28 * SIZE, A2, %xmm6)

	mulps	 %xmm11, %xmm5
	SUBPS	 %xmm5,  %xmm2
	mulps	 %xmm11, %xmm7
	SUBPS	 %xmm7,  %xmm3

	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	MOVUPS_A1(-24 * SIZE, A2, %xmm4)
	pshufd	 $0xb1, %xmm6, %xmm7
	mulps	 %xmm12, %xmm6
	addps	 %xmm6,  %xmm1
	MOVUPS_A1(-20 * SIZE, A2, %xmm6)

	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0
	mulps	 %xmm13, %xmm7
	SUBPS	 %xmm7,  %xmm1

	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm2
	movsd	-32 * SIZE(A2, LDA), %xmm4
	movhps	-30 * SIZE(A2, LDA), %xmm4
	pshufd	 $0xb1, %xmm6, %xmm7
	mulps	 %xmm12, %xmm6
	addps	 %xmm6,  %xmm3
	movsd	-28 * SIZE(A2, LDA), %xmm6
	movhps	-26 * SIZE(A2, LDA), %xmm6

	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm2
	mulps	 %xmm13, %xmm7
	SUBPS	 %xmm7,  %xmm3

	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm14, %xmm4
	addps	 %xmm4,  %xmm0
	movsd	-24 * SIZE(A2, LDA), %xmm4
	movhps	-22 * SIZE(A2, LDA), %xmm4
	pshufd	 $0xb1, %xmm6, %xmm7
	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm1
	movsd	-20 * SIZE(A2, LDA), %xmm6
	movhps	-18 * SIZE(A2, LDA), %xmm6

	mulps	 %xmm15, %xmm5
	SUBPS	 %xmm5,  %xmm0
	mulps	 %xmm15, %xmm7
	SUBPS	 %xmm7,  %xmm1

	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm14, %xmm4
	addps	 %xmm4,  %xmm2
	pshufd	 $0xb1, %xmm6, %xmm7
	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm3

	mulps	 %xmm15, %xmm5
	SUBPS	 %xmm5,  %xmm2
	mulps	 %xmm15, %xmm7
	SUBPS	 %xmm7,  %xmm3

	movaps	 %xmm0, -32 * SIZE(Y1)
	movaps	 %xmm1, -28 * SIZE(Y1)
	movaps	 %xmm2, -24 * SIZE(Y1)
	movaps	 %xmm3, -20 * SIZE(Y1)

	movaps	 -16 * SIZE(Y1), %xmm0
	movaps	 -12 * SIZE(Y1), %xmm1
	movaps	  -8 * SIZE(Y1), %xmm2
	movaps	  -4 * SIZE(Y1), %xmm3

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, A2
	subq	 $-16 * SIZE, Y1
	ALIGN_3

.L105:
	testq	$4, MM
	je	.L107

	MOVUPS_A1(-32 * SIZE, A1, %xmm4)
	MOVUPS_A1(-28 * SIZE, A1, %xmm6)

	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm8,  %xmm4
	addps	 %xmm4,  %xmm0
	movsd	-32 * SIZE(A1, LDA), %xmm4
	movhps	-30 * SIZE(A1, LDA), %xmm4
	pshufd	 $0xb1, %xmm6, %xmm7
	mulps	 %xmm8,  %xmm6
	addps	 %xmm6,  %xmm1
	movsd	-28 * SIZE(A1, LDA), %xmm6
	movhps	-26 * SIZE(A1, LDA), %xmm6

	mulps	 %xmm9,  %xmm5
	SUBPS	 %xmm5,  %xmm0
	mulps	 %xmm9,  %xmm7
	SUBPS	 %xmm7,  %xmm1

	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm10, %xmm4
	addps	 %xmm4,  %xmm0
	MOVUPS_A1(-32 * SIZE, A2, %xmm4)
	pshufd	 $0xb1, %xmm6, %xmm7
	mulps	 %xmm10, %xmm6
	addps	 %xmm6,  %xmm1
	MOVUPS_A1(-28 * SIZE, A2, %xmm6)

	mulps	 %xmm11, %xmm5
	SUBPS	 %xmm5,  %xmm0
	mulps	 %xmm11, %xmm7
	SUBPS	 %xmm7,  %xmm1

	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	movsd	-32 * SIZE(A2, LDA), %xmm4
	movhps	-30 * SIZE(A2, LDA), %xmm4
	pshufd	 $0xb1, %xmm6, %xmm7
	mulps	 %xmm12, %xmm6
	addps	 %xmm6,  %xmm1
	movsd	-28 * SIZE(A2, LDA), %xmm6
	movhps	-26 * SIZE(A2, LDA), %xmm6

	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0
	mulps	 %xmm13, %xmm7
	SUBPS	 %xmm7,  %xmm1

	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm14, %xmm4
	addps	 %xmm4,  %xmm0
	pshufd	 $0xb1, %xmm6, %xmm7
	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm1

	mulps	 %xmm15, %xmm5
	SUBPS	 %xmm5,  %xmm0
	mulps	 %xmm15, %xmm7
	SUBPS	 %xmm7,  %xmm1

	movaps	 %xmm0, -32 * SIZE(Y1)
	movaps	 %xmm1, -28 * SIZE(Y1)

	movaps	 %xmm2, %xmm0
	movaps	 %xmm3, %xmm1

	addq	 $8 * SIZE, A1
	addq	 $8 * SIZE, A2
	addq	 $8 * SIZE, Y1
	ALIGN_3

.L107:
	testq	$2, MM
	je	.L108

	MOVUPS_A1(-32 * SIZE, A1, %xmm4)
	movsd	-32 * SIZE(A1, LDA), %xmm6
	movhps	-30 * SIZE(A1, LDA), %xmm6

	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm8,  %xmm4
	addps	 %xmm4,  %xmm0
	MOVUPS_A1(-32 * SIZE, A2, %xmm4)
	pshufd	 $0xb1, %xmm6, %xmm7
	mulps	 %xmm9,  %xmm5
	SUBPS	 %xmm5,  %xmm0

	mulps	 %xmm10, %xmm6
	addps	 %xmm6,  %xmm0
	movsd	-32 * SIZE(A2, LDA), %xmm6
	movhps	-30 * SIZE(A2, LDA), %xmm6
	mulps	 %xmm11, %xmm7
	SUBPS	 %xmm7,  %xmm0

	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	pshufd	 $0xb1, %xmm6, %xmm7
	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0

	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm0
	mulps	 %xmm15, %xmm7
	SUBPS	 %xmm7,  %xmm0

	movaps	 %xmm0, -32 * SIZE(Y1)
	movaps	 %xmm1, %xmm0

	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, A2
	addq	 $4 * SIZE, Y1
	ALIGN_3

.L108:
	testq	$1, MM
	je	.L109

	movsd	 -32 * SIZE(A1), %xmm4
	movsd	 -32 * SIZE(A1, LDA), %xmm6

	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm8,  %xmm4
	addps	 %xmm4,  %xmm0
	movsd	 -32 * SIZE(A2), %xmm4
	pshufd	 $0xb1, %xmm6, %xmm7
	mulps	 %xmm9,  %xmm5
	SUBPS	 %xmm5,  %xmm0

	mulps	 %xmm10, %xmm6
	addps	 %xmm6,  %xmm0
	movsd	 -32 * SIZE(A2, LDA), %xmm6
	mulps	 %xmm11, %xmm7
	SUBPS	 %xmm7,  %xmm0

	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	pshufd	 $0xb1, %xmm6, %xmm7
	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0

	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm0
	mulps	 %xmm15, %xmm7
	SUBPS	 %xmm7,  %xmm0

	movlps	 %xmm0, -32 * SIZE(Y1)
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

	leaq	32 * SIZE(BUFFER), Y1
	movq	A,  A1
	leaq	(A,  LDA, 1), A2
	leaq	(A,  LDA, 2), A

	movsd	(X), %xmm13
	addq	INCX, X
	movsd	(X), %xmm15
	addq	INCX, X

#ifdef HAVE_SSE3
	movddup	ALPHA, %xmm8
#else
	movsd	ALPHA, %xmm8
	unpcklpd %xmm8, %xmm8
#endif

	pshufd	$0xb1, %xmm8, %xmm9

	pcmpeqb	%xmm11, %xmm11
	psllq	$63,    %xmm11

	pshufd	$0x00, %xmm13, %xmm12
	pshufd	$0x55, %xmm13, %xmm13
	pshufd	$0x00, %xmm15, %xmm14
	pshufd	$0x55, %xmm15, %xmm15

#ifndef XCONJ
	xorps	 %xmm11, %xmm13
	xorps	 %xmm11, %xmm15
#else
	xorps	 %xmm11, %xmm12
	xorps	 %xmm11, %xmm14
#endif

	mulps	 %xmm8, %xmm12
	mulps	 %xmm9, %xmm13
	mulps	 %xmm8, %xmm14
	mulps	 %xmm9, %xmm15

#ifndef XCONJ
	subps	 %xmm13, %xmm12
	subps	 %xmm15, %xmm14
#else
	addps	 %xmm13, %xmm12
	addps	 %xmm15, %xmm14
#endif

	pshufd	$0x55, %xmm12, %xmm13
	pshufd	$0x00, %xmm12, %xmm12
	pshufd	$0x55, %xmm14, %xmm15
	pshufd	$0x00, %xmm14, %xmm14

#ifndef CONJ
	xorps	 %xmm11, %xmm13
	xorps	 %xmm11, %xmm15
#else
	xorps	 %xmm11, %xmm12
	xorps	 %xmm11, %xmm14
#endif

#ifdef ALIGNED_ACCESS
	cmpq	M, MM
	je	.L11X

	movsd	 -32 * SIZE(A1), %xmm4
	movsd	 -32 * SIZE(A2), %xmm6

	movsd	 -32 * SIZE(Y1), %xmm0

	pshufd	 $0xb1, %xmm4, %xmm5
	pshufd	 $0xb1, %xmm6, %xmm7

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0

	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm0
	mulps	 %xmm15, %xmm7
	SUBPS	 %xmm7,  %xmm0

	movlps	 %xmm0, -32 * SIZE(Y1)

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, Y1
	ALIGN_3
.L11X:
#endif

	movaps	 -32 * SIZE(Y1), %xmm0
	movaps	 -28 * SIZE(Y1), %xmm1
	movaps	 -24 * SIZE(Y1), %xmm2
	movaps	 -20 * SIZE(Y1), %xmm3
	ALIGN_3

	movq	MM,  I
	sarq	$3,  I
	jle	.L115

	MOVUPS_A1(-32 * SIZE, A1, %xmm4)
	MOVUPS_A1(-28 * SIZE, A1, %xmm6)
	MOVUPS_A1(-24 * SIZE, A1, %xmm8)
	MOVUPS_A1(-20 * SIZE, A1, %xmm10)

	decq	 I
	jle	 .L114
	ALIGN_3

.L113:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(A2)
#endif

	pshufd	 $0xb1, %xmm4,  %xmm5
	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	movsd	-32 * SIZE(A2), %xmm4
	movhps	-30 * SIZE(A2), %xmm4
	pshufd	 $0xb1, %xmm6,  %xmm7
	mulps	 %xmm12, %xmm6
	addps	 %xmm6,  %xmm1
	movsd	-28 * SIZE(A2), %xmm6
	movhps	-26 * SIZE(A2), %xmm6

	pshufd	 $0xb1, %xmm8,  %xmm9
	mulps	 %xmm12, %xmm8
	addps	 %xmm8,  %xmm2
	movsd	-24 * SIZE(A2), %xmm8
	movhps	-22 * SIZE(A2), %xmm8
	pshufd	 $0xb1, %xmm10, %xmm11
	mulps	 %xmm12, %xmm10
	addps	 %xmm10, %xmm3
	movsd	-20 * SIZE(A2), %xmm10
	movhps	-18 * SIZE(A2), %xmm10

	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0
	mulps	 %xmm13, %xmm7
	SUBPS	 %xmm7,  %xmm1

	mulps	 %xmm13, %xmm9
	SUBPS	 %xmm9,  %xmm2
	mulps	 %xmm13, %xmm11
	SUBPS	 %xmm11, %xmm3

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(A1)
#endif

	pshufd	 $0xb1, %xmm4,  %xmm5
	mulps	 %xmm14, %xmm4
	addps	 %xmm4,  %xmm0
	MOVUPS_A1(-16 * SIZE, A1, %xmm4)
	pshufd	 $0xb1, %xmm6,  %xmm7
	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm1
	MOVUPS_A1(-12 * SIZE, A1, %xmm6)

	pshufd	 $0xb1, %xmm8,  %xmm9
	mulps	 %xmm14, %xmm8
	addps	 %xmm8,  %xmm2
	MOVUPS_A1( -8 * SIZE, A1, %xmm8)
	pshufd	 $0xb1, %xmm10, %xmm11
	mulps	 %xmm14, %xmm10
	addps	 %xmm10, %xmm3
	MOVUPS_A1( -4 * SIZE, A1, %xmm10)

	mulps	 %xmm15, %xmm5
	SUBPS	 %xmm5,  %xmm0
	mulps	 %xmm15, %xmm7
	SUBPS	 %xmm7,  %xmm1

	mulps	 %xmm15, %xmm9
	SUBPS	 %xmm9,  %xmm2
	mulps	 %xmm15, %xmm11
	SUBPS	 %xmm11, %xmm3

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(Y1)
#endif

	movaps	 %xmm0, -32 * SIZE(Y1)
	movaps	 %xmm1, -28 * SIZE(Y1)
	movaps	 %xmm2, -24 * SIZE(Y1)
	movaps	 %xmm3, -20 * SIZE(Y1)

	movaps	 -16 * SIZE(Y1), %xmm0
	movaps	 -12 * SIZE(Y1), %xmm1
	movaps	  -8 * SIZE(Y1), %xmm2
	movaps	  -4 * SIZE(Y1), %xmm3

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, A2
	subq	 $-16 * SIZE, Y1

	subq	 $1, I
	BRANCH
	jg	.L113
	ALIGN_3

.L114:
	pshufd	 $0xb1, %xmm4,  %xmm5
	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	movsd	-32 * SIZE(A2), %xmm4
	movhps	-30 * SIZE(A2), %xmm4
	pshufd	 $0xb1, %xmm6,  %xmm7
	mulps	 %xmm12, %xmm6
	addps	 %xmm6,  %xmm1
	movsd	-28 * SIZE(A2), %xmm6
	movhps	-26 * SIZE(A2), %xmm6

	pshufd	 $0xb1, %xmm8,  %xmm9
	mulps	 %xmm12, %xmm8
	addps	 %xmm8,  %xmm2
	movsd	-24 * SIZE(A2), %xmm8
	movhps	-22 * SIZE(A2), %xmm8
	pshufd	 $0xb1, %xmm10, %xmm11
	mulps	 %xmm12, %xmm10
	addps	 %xmm10, %xmm3
	movsd	-20 * SIZE(A2), %xmm10
	movhps	-18 * SIZE(A2), %xmm10

	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0
	mulps	 %xmm13, %xmm7
	SUBPS	 %xmm7,  %xmm1

	mulps	 %xmm13, %xmm9
	SUBPS	 %xmm9,  %xmm2
	mulps	 %xmm13, %xmm11
	SUBPS	 %xmm11, %xmm3

	pshufd	 $0xb1, %xmm4,  %xmm5
	mulps	 %xmm14, %xmm4
	addps	 %xmm4,  %xmm0
	pshufd	 $0xb1, %xmm6,  %xmm7
	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm1

	pshufd	 $0xb1, %xmm8,  %xmm9
	mulps	 %xmm14, %xmm8
	addps	 %xmm8,  %xmm2
	pshufd	 $0xb1, %xmm10, %xmm11
	mulps	 %xmm14, %xmm10
	addps	 %xmm10, %xmm3

	mulps	 %xmm15, %xmm5
	SUBPS	 %xmm5,  %xmm0
	mulps	 %xmm15, %xmm7
	SUBPS	 %xmm7,  %xmm1

	mulps	 %xmm15, %xmm9
	SUBPS	 %xmm9,  %xmm2
	mulps	 %xmm15, %xmm11
	SUBPS	 %xmm11, %xmm3

	movaps	 %xmm0, -32 * SIZE(Y1)
	movaps	 %xmm1, -28 * SIZE(Y1)
	movaps	 %xmm2, -24 * SIZE(Y1)
	movaps	 %xmm3, -20 * SIZE(Y1)

	movaps	 -16 * SIZE(Y1), %xmm0
	movaps	 -12 * SIZE(Y1), %xmm1
	movaps	  -8 * SIZE(Y1), %xmm2
	movaps	  -4 * SIZE(Y1), %xmm3

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, A2
	subq	 $-16 * SIZE, Y1
	ALIGN_3

.L115:
	testq	$4, MM
	je	.L117

	MOVUPS_A1(-32 * SIZE, A1, %xmm4)
	MOVUPS_A1(-28 * SIZE, A1, %xmm6)
	movsd	-32 * SIZE(A2), %xmm8
	movhps	-30 * SIZE(A2), %xmm8
	movsd	-28 * SIZE(A2), %xmm10
	movhps	-26 * SIZE(A2), %xmm10

	pshufd	 $0xb1, %xmm4,  %xmm5
	pshufd	 $0xb1, %xmm6,  %xmm7
	pshufd	 $0xb1, %xmm8,  %xmm9
	pshufd	 $0xb1, %xmm10, %xmm11

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	mulps	 %xmm12, %xmm6
	addps	 %xmm6,  %xmm1

	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0
	mulps	 %xmm13, %xmm7
	SUBPS	 %xmm7,  %xmm1

	mulps	 %xmm14, %xmm8
	addps	 %xmm8,  %xmm0
	mulps	 %xmm14, %xmm10
	addps	 %xmm10, %xmm1

	mulps	 %xmm15, %xmm9
	SUBPS	 %xmm9,  %xmm0
	mulps	 %xmm15, %xmm11
	SUBPS	 %xmm11, %xmm1

	movaps	 %xmm0, -32 * SIZE(Y1)
	movaps	 %xmm1, -28 * SIZE(Y1)

	movaps	 %xmm2, %xmm0
	movaps	 %xmm3, %xmm1

	addq	 $8 * SIZE, A1
	addq	 $8 * SIZE, A2
	addq	 $8 * SIZE, Y1
	ALIGN_3

.L117:
	testq	$2, MM
	je	.L118

	MOVUPS_A1(-32 * SIZE, A1, %xmm4)
	movsd	-32 * SIZE(A2), %xmm6
	movhps	-30 * SIZE(A2), %xmm6

	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	pshufd	 $0xb1, %xmm6, %xmm7
	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0

	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm0
	mulps	 %xmm15, %xmm7
	SUBPS	 %xmm7,  %xmm0

	movaps	 %xmm0, -32 * SIZE(Y1)
	movaps	 %xmm1, %xmm0

	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, A2
	addq	 $4 * SIZE, Y1
	ALIGN_3

.L118:
	testq	$1, MM
#if GEMV_UNROLL == 2
	je	.L119
#else
	je	.L120
#endif

	movsd	 -32 * SIZE(A1), %xmm4
	movsd	 -32 * SIZE(A2), %xmm6

	pshufd	 $0xb1, %xmm4, %xmm5
	pshufd	 $0xb1, %xmm6, %xmm7

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0

	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm0
	mulps	 %xmm15, %xmm7
	SUBPS	 %xmm7,  %xmm0

	movlps	 %xmm0, -32 * SIZE(Y1)

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
	jl	.L990

	leaq	32 * SIZE(BUFFER), Y1
	movq	A,  A1

	movsd	(X), %xmm13
	addq	INCX, X

#ifdef HAVE_SSE3
	movddup	ALPHA, %xmm8
#else
	movsd	ALPHA, %xmm8
	unpcklpd %xmm8, %xmm8
#endif

	pshufd	$0xb1, %xmm8, %xmm9

	pcmpeqb	%xmm11, %xmm11
	psllq	$63,    %xmm11

	pshufd	$0x00, %xmm13, %xmm12
	pshufd	$0x55, %xmm13, %xmm13

#ifndef XCONJ
	xorps	 %xmm11, %xmm13
#else
	xorps	 %xmm11, %xmm12
#endif

	mulps	 %xmm8, %xmm12
	mulps	 %xmm9, %xmm13

#ifndef XCONJ
	subps	 %xmm13, %xmm12
#else
	addps	 %xmm13, %xmm12
#endif

	pshufd	$0x55, %xmm12, %xmm13
	pshufd	$0x00, %xmm12, %xmm12

#ifndef CONJ
	xorps	 %xmm11, %xmm13
#else
	xorps	 %xmm11, %xmm12
#endif

#ifdef ALIGNED_ACCESS
	cmpq	M, MM
	je	.L12X

	movsd	 -32 * SIZE(A1), %xmm4
	movsd	 -32 * SIZE(Y1), %xmm0

	pshufd	 $0xb1, %xmm4, %xmm5

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0

	movlps	 %xmm0, -32 * SIZE(Y1)

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, Y1
	ALIGN_3
.L12X:
#endif

	movaps	 -32 * SIZE(Y1), %xmm0
	movaps	 -28 * SIZE(Y1), %xmm1
	movaps	 -24 * SIZE(Y1), %xmm2
	movaps	 -20 * SIZE(Y1), %xmm3
	ALIGN_3

	movq	MM,  I
	sarq	$3,  I
	jle	.L125

	MOVUPS_A1(-32 * SIZE, A1, %xmm4)
	MOVUPS_A1(-28 * SIZE, A1, %xmm6)
	MOVUPS_A1(-24 * SIZE, A1, %xmm8)
	MOVUPS_A1(-20 * SIZE, A1, %xmm10)

	decq	 I
	jle	 .L124
	ALIGN_3

.L123:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 4 - 128 + PREOFFSET(A1)
#endif

	pshufd	 $0xb1, %xmm4,  %xmm5
	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	MOVUPS_A1(-16 * SIZE, A1, %xmm4)
	pshufd	 $0xb1, %xmm6,  %xmm7
	mulps	 %xmm12, %xmm6
	addps	 %xmm6,  %xmm1
	MOVUPS_A1(-12 * SIZE, A1, %xmm6)

	pshufd	 $0xb1, %xmm8,  %xmm9
	mulps	 %xmm12, %xmm8
	addps	 %xmm8,  %xmm2
	MOVUPS_A1( -8 * SIZE, A1, %xmm8)
	pshufd	 $0xb1, %xmm10, %xmm11
	mulps	 %xmm12, %xmm10
	addps	 %xmm10, %xmm3
	MOVUPS_A1( -4 * SIZE, A1, %xmm10)

	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0
	mulps	 %xmm13, %xmm7
	SUBPS	 %xmm7,  %xmm1

	mulps	 %xmm13, %xmm9
	SUBPS	 %xmm9,  %xmm2
	mulps	 %xmm13, %xmm11
	SUBPS	 %xmm11, %xmm3

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE) * 4 - 128 + PREOFFSET(Y1)
#endif

	movaps	 %xmm0, -32 * SIZE(Y1)
	movaps	 %xmm1, -28 * SIZE(Y1)
	movaps	 %xmm2, -24 * SIZE(Y1)
	movaps	 %xmm3, -20 * SIZE(Y1)

	movaps	 -16 * SIZE(Y1), %xmm0
	movaps	 -12 * SIZE(Y1), %xmm1
	movaps	  -8 * SIZE(Y1), %xmm2
	movaps	  -4 * SIZE(Y1), %xmm3

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, A2
	subq	 $-16 * SIZE, Y1

	subq	 $1, I
	BRANCH
	jg	.L123
	ALIGN_3

.L124:
	pshufd	 $0xb1, %xmm4,  %xmm5
	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	pshufd	 $0xb1, %xmm6,  %xmm7
	mulps	 %xmm12, %xmm6
	addps	 %xmm6,  %xmm1

	pshufd	 $0xb1, %xmm8,  %xmm9
	mulps	 %xmm12, %xmm8
	addps	 %xmm8,  %xmm2
	pshufd	 $0xb1, %xmm10, %xmm11
	mulps	 %xmm12, %xmm10
	addps	 %xmm10, %xmm3

	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0
	mulps	 %xmm13, %xmm7
	SUBPS	 %xmm7,  %xmm1

	mulps	 %xmm13, %xmm9
	SUBPS	 %xmm9,  %xmm2
	mulps	 %xmm13, %xmm11
	SUBPS	 %xmm11, %xmm3

	movaps	 %xmm0, -32 * SIZE(Y1)
	movaps	 %xmm1, -28 * SIZE(Y1)
	movaps	 %xmm2, -24 * SIZE(Y1)
	movaps	 %xmm3, -20 * SIZE(Y1)

	movaps	 -16 * SIZE(Y1), %xmm0
	movaps	 -12 * SIZE(Y1), %xmm1
	movaps	  -8 * SIZE(Y1), %xmm2
	movaps	  -4 * SIZE(Y1), %xmm3

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, Y1
	ALIGN_3

.L125:
	testq	$4, MM
	je	.L127

	MOVUPS_A1(-32 * SIZE, A1, %xmm4)
	MOVUPS_A1(-28 * SIZE, A1, %xmm6)

	pshufd	 $0xb1, %xmm4,  %xmm5
	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	pshufd	 $0xb1, %xmm6,  %xmm7
	mulps	 %xmm12, %xmm6
	addps	 %xmm6,  %xmm1

	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0
	mulps	 %xmm13, %xmm7
	SUBPS	 %xmm7,  %xmm1

	movaps	 %xmm0, -32 * SIZE(Y1)
	movaps	 %xmm1, -28 * SIZE(Y1)

	movaps	 %xmm2, %xmm0
	movaps	 %xmm3, %xmm1

	addq	 $8 * SIZE, A1
	addq	 $8 * SIZE, Y1
	ALIGN_3

.L127:
	testq	$2, MM
	je	.L128

	MOVUPS_A1(-32 * SIZE, A1, %xmm4)

	pshufd	 $0xb1, %xmm4, %xmm5

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0

	movaps	 %xmm0, -32 * SIZE(Y1)
	movaps	 %xmm1, %xmm0

	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, Y1
	ALIGN_3

.L128:
	testq	$1, MM
	je	.L990

	movsd	 -32 * SIZE(A1), %xmm4

	pshufd	 $0xb1, %xmm4, %xmm5

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0

	movlps	 %xmm0, -32 * SIZE(Y1)

	jmp	.L990
	ALIGN_3

.L200:
	testq	$2 * SIZE, LDA
	jne	.L300

	cmpq	$2, N
	jl	.L210
	ALIGN_3

.L201:
	subq	$2, N

	leaq	32 * SIZE(BUFFER), Y1
	movq	A,  A1
	leaq	(A,  LDA, 1), A2
	leaq	(A,  LDA, 2), A

	movsd	(X), %xmm13
	addq	INCX, X
	movsd	(X), %xmm15
	addq	INCX, X

#ifdef HAVE_SSE3
	movddup	ALPHA, %xmm8
#else
	movsd	ALPHA, %xmm8
	unpcklpd %xmm8, %xmm8
#endif

	pshufd	$0xb1, %xmm8, %xmm9

	pcmpeqb	%xmm11, %xmm11
	psllq	$63,    %xmm11

	pshufd	$0x00, %xmm13, %xmm12
	pshufd	$0x55, %xmm13, %xmm13
	pshufd	$0x00, %xmm15, %xmm14
	pshufd	$0x55, %xmm15, %xmm15

#ifndef XCONJ
	xorps	 %xmm11, %xmm13
	xorps	 %xmm11, %xmm15
#else
	xorps	 %xmm11, %xmm12
	xorps	 %xmm11, %xmm14
#endif

	mulps	 %xmm8, %xmm12
	mulps	 %xmm9, %xmm13
	mulps	 %xmm8, %xmm14
	mulps	 %xmm9, %xmm15

#ifndef XCONJ
	subps	 %xmm13, %xmm12
	subps	 %xmm15, %xmm14
#else
	addps	 %xmm13, %xmm12
	addps	 %xmm15, %xmm14
#endif

	pshufd	$0x55, %xmm12, %xmm13
	pshufd	$0x00, %xmm12, %xmm12
	pshufd	$0x55, %xmm14, %xmm15
	pshufd	$0x00, %xmm14, %xmm14

#ifndef CONJ
	xorps	 %xmm11, %xmm13
	xorps	 %xmm11, %xmm15
#else
	xorps	 %xmm11, %xmm12
	xorps	 %xmm11, %xmm14
#endif

#ifdef ALIGNED_ACCESS
	cmpq	M, MM
	je	.L20X

	movsd	 -32 * SIZE(A1), %xmm4
	movsd	 -32 * SIZE(A2), %xmm6

	movsd	 -32 * SIZE(Y1), %xmm0

	pshufd	 $0xb1, %xmm4, %xmm5
	pshufd	 $0xb1, %xmm6, %xmm7

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0

	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm0
	mulps	 %xmm15, %xmm7
	SUBPS	 %xmm7,  %xmm0

	movlps	 %xmm0, -32 * SIZE(Y1)

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, Y1
	ALIGN_3
.L20X:
#endif

	movaps	 -33 * SIZE(A1), %xmm4
	movaps	 -33 * SIZE(A2), %xmm6

	movaps	 -32 * SIZE(Y1), %xmm0
	movaps	 -28 * SIZE(Y1), %xmm1
	movaps	 -24 * SIZE(Y1), %xmm2
	movaps	 -20 * SIZE(Y1), %xmm3

	movq	MM,  I
	sarq	$3,  I
	jle	.L205

	movaps	 -29 * SIZE(A1), %xmm8
	movaps	 -25 * SIZE(A1), %xmm9
	movaps	 -21 * SIZE(A1), %xmm10

	decq	 I
	jle	 .L204
	ALIGN_3

.L203:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(A2)
#endif

	movss	 %xmm8,  %xmm4
	shufps	 $0x39,  %xmm4, %xmm4
	pshufd	 $0xb1,  %xmm4, %xmm5
	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	movaps	 -17 * SIZE(A1), %xmm4

	movss	 %xmm9,  %xmm8
	shufps	 $0x39,  %xmm8, %xmm8
	pshufd	 $0xb1,  %xmm8, %xmm7
	mulps	 %xmm12, %xmm8
	addps	 %xmm8,  %xmm1
	movaps	 -29 * SIZE(A2), %xmm8

	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0
	mulps	 %xmm13, %xmm7
	SUBPS	 %xmm7,  %xmm1

	movss	 %xmm10, %xmm9
	shufps	 $0x39,  %xmm9, %xmm9
	pshufd	 $0xb1,  %xmm9, %xmm5
	mulps	 %xmm12, %xmm9
	addps	 %xmm9,  %xmm2
	movaps	 -25 * SIZE(A2), %xmm9

	movss	 %xmm4,  %xmm10
	shufps	 $0x39,  %xmm10, %xmm10
	pshufd	 $0xb1,  %xmm10, %xmm7
	mulps	 %xmm12, %xmm10
	addps	 %xmm10, %xmm3
	movaps	 -21 * SIZE(A2), %xmm10

	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm2
	mulps	 %xmm13, %xmm7
	SUBPS	 %xmm7,  %xmm3

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(A1)
#endif

	movss	 %xmm8,  %xmm6
	shufps	 $0x39,  %xmm6, %xmm6
	pshufd	 $0xb1,  %xmm6, %xmm5
	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm0
	movaps	 -17 * SIZE(A2), %xmm6

	movss	 %xmm9,  %xmm8
	shufps	 $0x39,  %xmm8, %xmm8
	pshufd	 $0xb1,  %xmm8, %xmm7
	mulps	 %xmm14, %xmm8
	addps	 %xmm8,  %xmm1
	movaps	 -13 * SIZE(A1), %xmm8

	mulps	 %xmm15, %xmm5
	SUBPS	 %xmm5,  %xmm0
	mulps	 %xmm15, %xmm7
	SUBPS	 %xmm7,  %xmm1

	movss	 %xmm10, %xmm9
	shufps	 $0x39,  %xmm9, %xmm9
	pshufd	 $0xb1,  %xmm9, %xmm5
	mulps	 %xmm14, %xmm9
	addps	 %xmm9,  %xmm2
	movaps	  -9 * SIZE(A1), %xmm9

	movss	 %xmm6,  %xmm10
	shufps	 $0x39,  %xmm10, %xmm10
	pshufd	 $0xb1,  %xmm10, %xmm7
	mulps	 %xmm14, %xmm10
	addps	 %xmm10, %xmm3
	movaps	  -5 * SIZE(A1), %xmm10

	mulps	 %xmm15, %xmm5
	SUBPS	 %xmm5,  %xmm2
	mulps	 %xmm15, %xmm7
	SUBPS	 %xmm7,  %xmm3

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(Y1)
#endif

	movaps	 %xmm0, -32 * SIZE(Y1)
	movaps	 %xmm1, -28 * SIZE(Y1)
	movaps	 %xmm2, -24 * SIZE(Y1)
	movaps	 %xmm3, -20 * SIZE(Y1)

	movaps	 -16 * SIZE(Y1), %xmm0
	movaps	 -12 * SIZE(Y1), %xmm1
	movaps	  -8 * SIZE(Y1), %xmm2
	movaps	  -4 * SIZE(Y1), %xmm3

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, A2
	subq	 $-16 * SIZE, Y1

	subq	 $1, I
	BRANCH
	jg	.L203
	ALIGN_3

.L204:
	movss	 %xmm8,  %xmm4
	shufps	 $0x39,  %xmm4, %xmm4
	pshufd	 $0xb1,  %xmm4, %xmm5
	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	movaps	 -17 * SIZE(A1), %xmm4

	movss	 %xmm9,  %xmm8
	shufps	 $0x39,  %xmm8, %xmm8
	pshufd	 $0xb1,  %xmm8, %xmm7
	mulps	 %xmm12, %xmm8
	addps	 %xmm8,  %xmm1
	movaps	 -29 * SIZE(A2), %xmm8

	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0
	mulps	 %xmm13, %xmm7
	SUBPS	 %xmm7,  %xmm1

	movss	 %xmm10, %xmm9
	shufps	 $0x39,  %xmm9, %xmm9
	pshufd	 $0xb1,  %xmm9, %xmm5
	mulps	 %xmm12, %xmm9
	addps	 %xmm9,  %xmm2
	movaps	 -25 * SIZE(A2), %xmm9

	movss	 %xmm4,  %xmm10
	shufps	 $0x39,  %xmm10, %xmm10
	pshufd	 $0xb1,  %xmm10, %xmm7
	mulps	 %xmm12, %xmm10
	addps	 %xmm10, %xmm3
	movaps	 -21 * SIZE(A2), %xmm10

	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm2
	mulps	 %xmm13, %xmm7
	SUBPS	 %xmm7,  %xmm3

	movss	 %xmm8,  %xmm6
	shufps	 $0x39,  %xmm6, %xmm6
	pshufd	 $0xb1,  %xmm6, %xmm5
	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm0
	movaps	 -17 * SIZE(A2), %xmm6

	movss	 %xmm9,  %xmm8
	shufps	 $0x39,  %xmm8, %xmm8
	pshufd	 $0xb1,  %xmm8, %xmm7
	mulps	 %xmm14, %xmm8
	addps	 %xmm8,  %xmm1

	mulps	 %xmm15, %xmm5
	SUBPS	 %xmm5,  %xmm0
	mulps	 %xmm15, %xmm7
	SUBPS	 %xmm7,  %xmm1

	movss	 %xmm10, %xmm9
	shufps	 $0x39,  %xmm9, %xmm9
	pshufd	 $0xb1,  %xmm9, %xmm5
	mulps	 %xmm14, %xmm9
	addps	 %xmm9,  %xmm2

	movss	 %xmm6,  %xmm10
	shufps	 $0x39,  %xmm10, %xmm10
	pshufd	 $0xb1,  %xmm10, %xmm7
	mulps	 %xmm14, %xmm10
	addps	 %xmm10, %xmm3

	mulps	 %xmm15, %xmm5
	SUBPS	 %xmm5,  %xmm2
	mulps	 %xmm15, %xmm7
	SUBPS	 %xmm7,  %xmm3

	movaps	 %xmm0, -32 * SIZE(Y1)
	movaps	 %xmm1, -28 * SIZE(Y1)
	movaps	 %xmm2, -24 * SIZE(Y1)
	movaps	 %xmm3, -20 * SIZE(Y1)

	movaps	 -16 * SIZE(Y1), %xmm0
	movaps	 -12 * SIZE(Y1), %xmm1
	movaps	  -8 * SIZE(Y1), %xmm2
	movaps	  -4 * SIZE(Y1), %xmm3

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, A2
	subq	 $-16 * SIZE, Y1
	ALIGN_3

.L205:
	testq	$4, MM
	je	.L207

	movaps	 -29 * SIZE(A1), %xmm8
	movaps	 -25 * SIZE(A1), %xmm9
	movaps	 -29 * SIZE(A2), %xmm10
	movaps	 -25 * SIZE(A2), %xmm11

	movss	 %xmm8,  %xmm4
	shufps	 $0x39,  %xmm4, %xmm4
	pshufd	 $0xb1,  %xmm4, %xmm5
	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0

	movss	 %xmm9,  %xmm8
	shufps	 $0x39,  %xmm8, %xmm8
	pshufd	 $0xb1,  %xmm8, %xmm7
	mulps	 %xmm12, %xmm8
	addps	 %xmm8,  %xmm1

	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0
	mulps	 %xmm13, %xmm7
	SUBPS	 %xmm7,  %xmm1

	movss	 %xmm10, %xmm6
	shufps	 $0x39,  %xmm6, %xmm6
	pshufd	 $0xb1,  %xmm6, %xmm5
	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm0

	movss	 %xmm11, %xmm10
	shufps	 $0x39,  %xmm10, %xmm10
	pshufd	 $0xb1,  %xmm10, %xmm7
	mulps	 %xmm14, %xmm10
	addps	 %xmm10, %xmm1

	mulps	 %xmm15, %xmm5
	SUBPS	 %xmm5,  %xmm0
	mulps	 %xmm15, %xmm7
	SUBPS	 %xmm7,  %xmm1

	movaps	 %xmm0, -32 * SIZE(Y1)
	movaps	 %xmm1, -28 * SIZE(Y1)

	movaps	 %xmm9,  %xmm4
	movaps	 %xmm11, %xmm6
	movaps	 %xmm2, %xmm0
	movaps	 %xmm3, %xmm1

	addq	 $8 * SIZE, A1
	addq	 $8 * SIZE, A2
	addq	 $8 * SIZE, Y1
	ALIGN_3

.L207:
	testq	$2, MM
	je	.L208

	movaps	 -29 * SIZE(A1), %xmm8
	movaps	 -29 * SIZE(A2), %xmm9

	movss	 %xmm8,  %xmm4
	shufps	 $0x39,  %xmm4, %xmm4
	movss	 %xmm9, %xmm6
	shufps	 $0x39,  %xmm6, %xmm6

	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	pshufd	 $0xb1, %xmm6, %xmm7
	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0

	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm0
	mulps	 %xmm15, %xmm7
	SUBPS	 %xmm7,  %xmm0

	movaps	 %xmm0, -32 * SIZE(Y1)
	movaps	 %xmm1, %xmm0

	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, A2
	addq	 $4 * SIZE, Y1
	ALIGN_3

.L208:
	testq	$1, MM
	je	.L209

	movsd	 -32 * SIZE(A1), %xmm4
	movsd	 -32 * SIZE(A2), %xmm6

	pshufd	 $0xb1, %xmm4, %xmm5
	pshufd	 $0xb1, %xmm6, %xmm7

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0

	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm0
	mulps	 %xmm15, %xmm7
	SUBPS	 %xmm7,  %xmm0

	movlps	 %xmm0, -32 * SIZE(Y1)
	ALIGN_3

.L209:
	cmpq	$2, N
	jge	.L201
	ALIGN_3

.L210:
	cmpq	$1, N
	jl	.L990

	leaq	32 * SIZE(BUFFER), Y1
	movq	A,  A1

	movsd	(X), %xmm13
	addq	INCX, X

#ifdef HAVE_SSE3
	movddup	ALPHA, %xmm8
#else
	movsd	ALPHA, %xmm8
	unpcklpd %xmm8, %xmm8
#endif

	pshufd	$0xb1, %xmm8, %xmm9

	pcmpeqb	%xmm11, %xmm11
	psllq	$63,    %xmm11

	pshufd	$0x00, %xmm13, %xmm12
	pshufd	$0x55, %xmm13, %xmm13

#ifndef XCONJ
	xorps	 %xmm11, %xmm13
#else
	xorps	 %xmm11, %xmm12
#endif

	mulps	 %xmm8, %xmm12
	mulps	 %xmm9, %xmm13

#ifndef XCONJ
	subps	 %xmm13, %xmm12
#else
	addps	 %xmm13, %xmm12
#endif

	pshufd	$0x55, %xmm12, %xmm13
	pshufd	$0x00, %xmm12, %xmm12

#ifndef CONJ
	xorps	 %xmm11, %xmm13
#else
	xorps	 %xmm11, %xmm12
#endif

#ifdef ALIGNED_ACCESS
	cmpq	M, MM
	je	.L21X

	movsd	 -32 * SIZE(A1), %xmm4
	movsd	 -32 * SIZE(Y1), %xmm0

	pshufd	 $0xb1, %xmm4, %xmm5

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0

	movlps	 %xmm0, -32 * SIZE(Y1)

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, Y1
	ALIGN_3
.L21X:
#endif

	movaps	 -33 * SIZE(A1), %xmm4

	movaps	 -32 * SIZE(Y1), %xmm0
	movaps	 -28 * SIZE(Y1), %xmm1
	movaps	 -24 * SIZE(Y1), %xmm2
	movaps	 -20 * SIZE(Y1), %xmm3

	movq	MM,  I
	sarq	$3,  I
	jle	.L215

	movaps	 -29 * SIZE(A1), %xmm6
	movaps	 -25 * SIZE(A1), %xmm8
	movaps	 -21 * SIZE(A1), %xmm10

	decq	 I
	jle	 .L214
	ALIGN_3

.L213:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 4 - 128 + PREOFFSET(A1)
#endif

	movss	 %xmm6,  %xmm4
	shufps	 $0x39,  %xmm4, %xmm4
	pshufd	 $0xb1, %xmm4,  %xmm5
	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	movaps	 -17 * SIZE(A1), %xmm4

	movss	 %xmm8,  %xmm6
	shufps	 $0x39,  %xmm6, %xmm6
	pshufd	 $0xb1, %xmm6,  %xmm7
	mulps	 %xmm12, %xmm6
	addps	 %xmm6,  %xmm1
	movaps	 -13 * SIZE(A1), %xmm6

	movss	 %xmm10, %xmm8
	shufps	 $0x39, %xmm8, %xmm8
	pshufd	 $0xb1, %xmm8,  %xmm9
	mulps	 %xmm12, %xmm8
	addps	 %xmm8,  %xmm2
	movaps	  -9 * SIZE(A1), %xmm8

	movss	 %xmm4, %xmm10
	shufps	 $0x39, %xmm10, %xmm10
	pshufd	 $0xb1, %xmm10, %xmm11
	mulps	 %xmm12, %xmm10
	addps	 %xmm10, %xmm3
	movaps	  -5 * SIZE(A1), %xmm10

	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0
	mulps	 %xmm13, %xmm7
	SUBPS	 %xmm7,  %xmm1

	mulps	 %xmm13, %xmm9
	SUBPS	 %xmm9,  %xmm2
	mulps	 %xmm13, %xmm11
	SUBPS	 %xmm11, %xmm3

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE) * 4 - 128 + PREOFFSET(Y1)
#endif

	movaps	 %xmm0, -32 * SIZE(Y1)
	movaps	 %xmm1, -28 * SIZE(Y1)
	movaps	 %xmm2, -24 * SIZE(Y1)
	movaps	 %xmm3, -20 * SIZE(Y1)

	movaps	 -16 * SIZE(Y1), %xmm0
	movaps	 -12 * SIZE(Y1), %xmm1
	movaps	  -8 * SIZE(Y1), %xmm2
	movaps	  -4 * SIZE(Y1), %xmm3

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, A2
	subq	 $-16 * SIZE, Y1

	subq	 $1, I
	BRANCH
	jg	.L213
	ALIGN_3

.L214:
	movss	 %xmm6,  %xmm4
	shufps	 $0x39,  %xmm4, %xmm4
	pshufd	 $0xb1, %xmm4,  %xmm5
	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	movaps	 -17 * SIZE(A1), %xmm4

	movss	 %xmm8,  %xmm6
	shufps	 $0x39,  %xmm6, %xmm6
	pshufd	 $0xb1, %xmm6,  %xmm7
	mulps	 %xmm12, %xmm6
	addps	 %xmm6,  %xmm1

	movss	 %xmm10, %xmm8
	shufps	 $0x39, %xmm8, %xmm8
	pshufd	 $0xb1, %xmm8,  %xmm9
	mulps	 %xmm12, %xmm8
	addps	 %xmm8,  %xmm2

	movss	 %xmm4, %xmm10
	shufps	 $0x39, %xmm10, %xmm10
	pshufd	 $0xb1, %xmm10, %xmm11
	mulps	 %xmm12, %xmm10
	addps	 %xmm10, %xmm3

	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0
	mulps	 %xmm13, %xmm7
	SUBPS	 %xmm7,  %xmm1

	mulps	 %xmm13, %xmm9
	SUBPS	 %xmm9,  %xmm2
	mulps	 %xmm13, %xmm11
	SUBPS	 %xmm11, %xmm3

	movaps	 %xmm0, -32 * SIZE(Y1)
	movaps	 %xmm1, -28 * SIZE(Y1)
	movaps	 %xmm2, -24 * SIZE(Y1)
	movaps	 %xmm3, -20 * SIZE(Y1)

	movaps	 -16 * SIZE(Y1), %xmm0
	movaps	 -12 * SIZE(Y1), %xmm1
	movaps	  -8 * SIZE(Y1), %xmm2
	movaps	  -4 * SIZE(Y1), %xmm3

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, Y1
	ALIGN_3

.L215:
	testq	$4, MM
	je	.L217

	movaps	 -29 * SIZE(A1), %xmm6
	movaps	 -25 * SIZE(A1), %xmm8

	movss	 %xmm6, %xmm4
	shufps	 $0x39, %xmm4, %xmm4
	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0

	movss	 %xmm8, %xmm6
	shufps	 $0x39, %xmm6, %xmm6
	pshufd	 $0xb1, %xmm6, %xmm7
	mulps	 %xmm12, %xmm6
	addps	 %xmm6,  %xmm1

	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0
	mulps	 %xmm13, %xmm7
	SUBPS	 %xmm7,  %xmm1

	movaps	 %xmm0, -32 * SIZE(Y1)
	movaps	 %xmm1, -28 * SIZE(Y1)

	movaps	 %xmm2, %xmm0
	movaps	 %xmm3, %xmm1
	movaps	 %xmm8, %xmm4

	addq	 $8 * SIZE, A1
	addq	 $8 * SIZE, Y1
	ALIGN_3

.L217:
	testq	$2, MM
	je	.L218

	movaps	 -29 * SIZE(A1), %xmm6

	movss	 %xmm6, %xmm4
	shufps	 $0x39, %xmm4, %xmm4

	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0

	movaps	 %xmm0, -32 * SIZE(Y1)
	movaps	 %xmm1, %xmm0

	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, Y1
	ALIGN_3

.L218:
	testq	$1, MM
	je	.L990

	movsd	 -32 * SIZE(A1), %xmm4

	pshufd	 $0xb1, %xmm4, %xmm5

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0

	movlps	 %xmm0, -32 * SIZE(Y1)
	jmp	 .L990
	ALIGN_3

.L300:
	cmpq	$2, N
	jl	.L310
	ALIGN_3

.L301:
	subq	$2, N

	leaq	32 * SIZE(BUFFER), Y1
	movq	A,  A1
	leaq	(A,  LDA, 1), A2
	leaq	(A,  LDA, 2), A

	movsd	(X), %xmm13
	addq	INCX, X
	movsd	(X), %xmm15
	addq	INCX, X

#ifdef HAVE_SSE3
	movddup	ALPHA, %xmm8
#else
	movsd	ALPHA, %xmm8
	unpcklpd %xmm8, %xmm8
#endif

	pshufd	$0xb1, %xmm8, %xmm9

	pcmpeqb	%xmm11, %xmm11
	psllq	$63,    %xmm11

	pshufd	$0x00, %xmm13, %xmm12
	pshufd	$0x55, %xmm13, %xmm13
	pshufd	$0x00, %xmm15, %xmm14
	pshufd	$0x55, %xmm15, %xmm15

#ifndef XCONJ
	xorps	 %xmm11, %xmm13
	xorps	 %xmm11, %xmm15
#else
	xorps	 %xmm11, %xmm12
	xorps	 %xmm11, %xmm14
#endif

	mulps	 %xmm8, %xmm12
	mulps	 %xmm9, %xmm13
	mulps	 %xmm8, %xmm14
	mulps	 %xmm9, %xmm15

#ifndef XCONJ
	subps	 %xmm13, %xmm12
	subps	 %xmm15, %xmm14
#else
	addps	 %xmm13, %xmm12
	addps	 %xmm15, %xmm14
#endif

	pshufd	$0x55, %xmm12, %xmm13
	pshufd	$0x00, %xmm12, %xmm12
	pshufd	$0x55, %xmm14, %xmm15
	pshufd	$0x00, %xmm14, %xmm14

#ifndef CONJ
	xorps	 %xmm11, %xmm13
	xorps	 %xmm11, %xmm15
#else
	xorps	 %xmm11, %xmm12
	xorps	 %xmm11, %xmm14
#endif

#ifdef ALIGNED_ACCESS
	cmpq	M, MM
	je	.L30X

	movsd	 -32 * SIZE(A1), %xmm4
	movsd	 -32 * SIZE(A2), %xmm6

	movsd	 -32 * SIZE(Y1), %xmm0

	pshufd	 $0xb1, %xmm4, %xmm5
	pshufd	 $0xb1, %xmm6, %xmm7

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0

	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm0
	mulps	 %xmm15, %xmm7
	SUBPS	 %xmm7,  %xmm0

	movlps	 %xmm0, -32 * SIZE(Y1)

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, Y1
	ALIGN_3
.L30X:
#endif

	movaps	 -33 * SIZE(A1), %xmm4
	movaps	 -35 * SIZE(A2), %xmm6

	movaps	 -32 * SIZE(Y1), %xmm0
	movaps	 -28 * SIZE(Y1), %xmm1
	movaps	 -24 * SIZE(Y1), %xmm2
	movaps	 -20 * SIZE(Y1), %xmm3

	movq	MM,  I
	sarq	$3,  I
	jle	.L305

	movaps	 -29 * SIZE(A1), %xmm8
	movaps	 -25 * SIZE(A1), %xmm9
	movaps	 -21 * SIZE(A1), %xmm10

	decq	 I
	jle	 .L304
	ALIGN_3

.L303:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(A2)
#endif

	movss	 %xmm8,  %xmm4
	shufps	 $0x39,  %xmm4, %xmm4
	pshufd	 $0xb1,  %xmm4, %xmm5
	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	movaps	 -17 * SIZE(A1), %xmm4

	movss	 %xmm9,  %xmm8
	shufps	 $0x39,  %xmm8, %xmm8
	pshufd	 $0xb1,  %xmm8, %xmm7
	mulps	 %xmm12, %xmm8
	addps	 %xmm8,  %xmm1
	movaps	 -31 * SIZE(A2), %xmm8

	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0
	mulps	 %xmm13, %xmm7
	SUBPS	 %xmm7,  %xmm1

	movss	 %xmm10, %xmm9
	shufps	 $0x39,  %xmm9, %xmm9
	pshufd	 $0xb1,  %xmm9, %xmm5
	mulps	 %xmm12, %xmm9
	addps	 %xmm9,  %xmm2
	movaps	 -27 * SIZE(A2), %xmm9

	movss	 %xmm4,  %xmm10
	shufps	 $0x39,  %xmm10, %xmm10
	pshufd	 $0xb1,  %xmm10, %xmm7
	mulps	 %xmm12, %xmm10
	addps	 %xmm10, %xmm3
	movaps	 -23 * SIZE(A2), %xmm10

	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm2
	mulps	 %xmm13, %xmm7
	SUBPS	 %xmm7,  %xmm3

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(A1)
#endif

	movss	 %xmm8,  %xmm6
	shufps	 $0x93,  %xmm8, %xmm6
	pshufd	 $0xb1,  %xmm6, %xmm5
	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm0
	movaps	 -19 * SIZE(A2), %xmm6

	movss	 %xmm9,  %xmm8
	shufps	 $0x93,  %xmm9, %xmm8
	pshufd	 $0xb1,  %xmm8, %xmm7
	mulps	 %xmm14, %xmm8
	addps	 %xmm8,  %xmm1
	movaps	 -13 * SIZE(A1), %xmm8

	mulps	 %xmm15, %xmm5
	SUBPS	 %xmm5,  %xmm0
	mulps	 %xmm15, %xmm7
	SUBPS	 %xmm7,  %xmm1

	movss	 %xmm10, %xmm9
	shufps	 $0x93,  %xmm10, %xmm9
	pshufd	 $0xb1,  %xmm9, %xmm5
	mulps	 %xmm14, %xmm9
	addps	 %xmm9,  %xmm2
	movaps	  -9 * SIZE(A1), %xmm9

	movss	 %xmm6,  %xmm10
	shufps	 $0x93,  %xmm6, %xmm10
	pshufd	 $0xb1,  %xmm10, %xmm7
	mulps	 %xmm14, %xmm10
	addps	 %xmm10, %xmm3
	movaps	  -5 * SIZE(A1), %xmm10

	mulps	 %xmm15, %xmm5
	SUBPS	 %xmm5,  %xmm2
	mulps	 %xmm15, %xmm7
	SUBPS	 %xmm7,  %xmm3

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(Y1)
#endif

	movaps	 %xmm0, -32 * SIZE(Y1)
	movaps	 %xmm1, -28 * SIZE(Y1)
	movaps	 %xmm2, -24 * SIZE(Y1)
	movaps	 %xmm3, -20 * SIZE(Y1)

	movaps	 -16 * SIZE(Y1), %xmm0
	movaps	 -12 * SIZE(Y1), %xmm1
	movaps	  -8 * SIZE(Y1), %xmm2
	movaps	  -4 * SIZE(Y1), %xmm3

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, A2
	subq	 $-16 * SIZE, Y1

	subq	 $1, I
	BRANCH
	jg	.L303
	ALIGN_3

.L304:
	movss	 %xmm8,  %xmm4
	shufps	 $0x39,  %xmm4, %xmm4
	pshufd	 $0xb1,  %xmm4, %xmm5
	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	movaps	 -17 * SIZE(A1), %xmm4

	movss	 %xmm9,  %xmm8
	shufps	 $0x39,  %xmm8, %xmm8
	pshufd	 $0xb1,  %xmm8, %xmm7
	mulps	 %xmm12, %xmm8
	addps	 %xmm8,  %xmm1
	movaps	 -31 * SIZE(A2), %xmm8

	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0
	mulps	 %xmm13, %xmm7
	SUBPS	 %xmm7,  %xmm1

	movss	 %xmm10, %xmm9
	shufps	 $0x39,  %xmm9, %xmm9
	pshufd	 $0xb1,  %xmm9, %xmm5
	mulps	 %xmm12, %xmm9
	addps	 %xmm9,  %xmm2
	movaps	 -27 * SIZE(A2), %xmm9

	movss	 %xmm4,  %xmm10
	shufps	 $0x39,  %xmm10, %xmm10
	pshufd	 $0xb1,  %xmm10, %xmm7
	mulps	 %xmm12, %xmm10
	addps	 %xmm10, %xmm3
	movaps	 -23 * SIZE(A2), %xmm10

	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm2
	mulps	 %xmm13, %xmm7
	SUBPS	 %xmm7,  %xmm3

	movss	 %xmm8,  %xmm6
	shufps	 $0x93,  %xmm8, %xmm6
	pshufd	 $0xb1,  %xmm6, %xmm5
	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm0
	movaps	 -19 * SIZE(A2), %xmm6

	movss	 %xmm9,  %xmm8
	shufps	 $0x93,  %xmm9, %xmm8
	pshufd	 $0xb1,  %xmm8, %xmm7
	mulps	 %xmm14, %xmm8
	addps	 %xmm8,  %xmm1

	mulps	 %xmm15, %xmm5
	SUBPS	 %xmm5,  %xmm0
	mulps	 %xmm15, %xmm7
	SUBPS	 %xmm7,  %xmm1

	movss	 %xmm10, %xmm9
	shufps	 $0x93,  %xmm10, %xmm9
	pshufd	 $0xb1,  %xmm9, %xmm5
	mulps	 %xmm14, %xmm9
	addps	 %xmm9,  %xmm2

	movss	 %xmm6,  %xmm10
	shufps	 $0x93,  %xmm6,  %xmm10
	pshufd	 $0xb1,  %xmm10, %xmm7
	mulps	 %xmm14, %xmm10
	addps	 %xmm10, %xmm3

	mulps	 %xmm15, %xmm5
	SUBPS	 %xmm5,  %xmm2
	mulps	 %xmm15, %xmm7
	SUBPS	 %xmm7,  %xmm3

	movaps	 %xmm0, -32 * SIZE(Y1)
	movaps	 %xmm1, -28 * SIZE(Y1)
	movaps	 %xmm2, -24 * SIZE(Y1)
	movaps	 %xmm3, -20 * SIZE(Y1)

	movaps	 -16 * SIZE(Y1), %xmm0
	movaps	 -12 * SIZE(Y1), %xmm1
	movaps	  -8 * SIZE(Y1), %xmm2
	movaps	  -4 * SIZE(Y1), %xmm3

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, A2
	subq	 $-16 * SIZE, Y1
	ALIGN_3

.L305:
	testq	$4, MM
	je	.L307

	movaps	 -29 * SIZE(A1), %xmm8
	movaps	 -25 * SIZE(A1), %xmm9
	movaps	 -31 * SIZE(A2), %xmm10
	movaps	 -27 * SIZE(A2), %xmm11

	movss	 %xmm8,  %xmm4
	shufps	 $0x39,  %xmm4, %xmm4
	pshufd	 $0xb1,  %xmm4, %xmm5
	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0

	movss	 %xmm9,  %xmm8
	shufps	 $0x39,  %xmm8, %xmm8
	pshufd	 $0xb1,  %xmm8, %xmm7
	mulps	 %xmm12, %xmm8
	addps	 %xmm8,  %xmm1

	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0
	mulps	 %xmm13, %xmm7
	SUBPS	 %xmm7,  %xmm1

	movss	 %xmm10, %xmm6
	shufps	 $0x93,  %xmm10, %xmm6
	pshufd	 $0xb1,  %xmm6,  %xmm5
	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm0

	movss	 %xmm11, %xmm10
	shufps	 $0x93,  %xmm11, %xmm10
	pshufd	 $0xb1,  %xmm10, %xmm7
	mulps	 %xmm14, %xmm10
	addps	 %xmm10, %xmm1

	mulps	 %xmm15, %xmm5
	SUBPS	 %xmm5,  %xmm0
	mulps	 %xmm15, %xmm7
	SUBPS	 %xmm7,  %xmm1

	movaps	 %xmm0, -32 * SIZE(Y1)
	movaps	 %xmm1, -28 * SIZE(Y1)

	movaps	 %xmm9,  %xmm4
	movaps	 %xmm11, %xmm6
	movaps	 %xmm2, %xmm0
	movaps	 %xmm3, %xmm1

	addq	 $8 * SIZE, A1
	addq	 $8 * SIZE, A2
	addq	 $8 * SIZE, Y1
	ALIGN_3

.L307:
	testq	$2, MM
	je	.L308

	movaps	 -29 * SIZE(A1), %xmm8
	movaps	 -31 * SIZE(A2), %xmm9

	movss	 %xmm8,  %xmm4
	shufps	 $0x39,  %xmm4, %xmm4
	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0

	movss	 %xmm9, %xmm6
	shufps	 $0x93,  %xmm9, %xmm6
	pshufd	 $0xb1, %xmm6, %xmm7
	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0

	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm0
	mulps	 %xmm15, %xmm7
	SUBPS	 %xmm7,  %xmm0

	movaps	 %xmm0, -32 * SIZE(Y1)
	movaps	 %xmm1, %xmm0

	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, A2
	addq	 $4 * SIZE, Y1
	ALIGN_3

.L308:
	testq	$1, MM
	je	.L309

	movsd	 -32 * SIZE(A1), %xmm4
	movsd	 -32 * SIZE(A2), %xmm6

	pshufd	 $0xb1, %xmm4, %xmm5
	pshufd	 $0xb1, %xmm6, %xmm7

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0

	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm0
	mulps	 %xmm15, %xmm7
	SUBPS	 %xmm7,  %xmm0

	movlps	 %xmm0, -32 * SIZE(Y1)
	ALIGN_3

.L309:
	cmpq	$2, N
	jge	.L301
	ALIGN_3

.L310:
	cmpq	$1, N
	jl	.L990

	leaq	32 * SIZE(BUFFER), Y1
	movq	A,  A1

	movsd	(X), %xmm13
	addq	INCX, X

#ifdef HAVE_SSE3
	movddup	ALPHA, %xmm8
#else
	movsd	ALPHA, %xmm8
	unpcklpd %xmm8, %xmm8
#endif

	pshufd	$0xb1, %xmm8, %xmm9

	pcmpeqb	%xmm11, %xmm11
	psllq	$63,    %xmm11

	pshufd	$0x00, %xmm13, %xmm12
	pshufd	$0x55, %xmm13, %xmm13

#ifndef XCONJ
	xorps	 %xmm11, %xmm13
#else
	xorps	 %xmm11, %xmm12
#endif

	mulps	 %xmm8, %xmm12
	mulps	 %xmm9, %xmm13

#ifndef XCONJ
	subps	 %xmm13, %xmm12
#else
	addps	 %xmm13, %xmm12
#endif

	pshufd	$0x55, %xmm12, %xmm13
	pshufd	$0x00, %xmm12, %xmm12

#ifndef CONJ
	xorps	 %xmm11, %xmm13
#else
	xorps	 %xmm11, %xmm12
#endif

#ifdef ALIGNED_ACCESS
	cmpq	M, MM
	je	.L31X

	movsd	 -32 * SIZE(A1), %xmm4
	movsd	 -32 * SIZE(Y1), %xmm0

	pshufd	 $0xb1, %xmm4, %xmm5

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0

	movlps	 %xmm0, -32 * SIZE(Y1)

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, Y1
	ALIGN_3
.L31X:
#endif

	movaps	 -33 * SIZE(A1), %xmm4

	movaps	 -32 * SIZE(Y1), %xmm0
	movaps	 -28 * SIZE(Y1), %xmm1
	movaps	 -24 * SIZE(Y1), %xmm2
	movaps	 -20 * SIZE(Y1), %xmm3

	movq	MM,  I
	sarq	$3,  I
	jle	.L315

	movaps	 -29 * SIZE(A1), %xmm6
	movaps	 -25 * SIZE(A1), %xmm8
	movaps	 -21 * SIZE(A1), %xmm10

	decq	 I
	jle	 .L314
	ALIGN_3

.L313:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 4 - 128 + PREOFFSET(A1)
#endif

	movss	 %xmm6,  %xmm4
	shufps	 $0x39,  %xmm4, %xmm4
	pshufd	 $0xb1, %xmm4,  %xmm5
	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	movaps	 -17 * SIZE(A1), %xmm4

	movss	 %xmm8,  %xmm6
	shufps	 $0x39,  %xmm6, %xmm6
	pshufd	 $0xb1, %xmm6,  %xmm7
	mulps	 %xmm12, %xmm6
	addps	 %xmm6,  %xmm1
	movaps	 -13 * SIZE(A1), %xmm6

	movss	 %xmm10, %xmm8
	shufps	 $0x39, %xmm8, %xmm8
	pshufd	 $0xb1, %xmm8,  %xmm9
	mulps	 %xmm12, %xmm8
	addps	 %xmm8,  %xmm2
	movaps	  -9 * SIZE(A1), %xmm8

	movss	 %xmm4, %xmm10
	shufps	 $0x39, %xmm10, %xmm10
	pshufd	 $0xb1, %xmm10, %xmm11
	mulps	 %xmm12, %xmm10
	addps	 %xmm10, %xmm3
	movaps	  -5 * SIZE(A1), %xmm10

	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0
	mulps	 %xmm13, %xmm7
	SUBPS	 %xmm7,  %xmm1

	mulps	 %xmm13, %xmm9
	SUBPS	 %xmm9,  %xmm2
	mulps	 %xmm13, %xmm11
	SUBPS	 %xmm11, %xmm3

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE) * 4 - 128 + PREOFFSET(Y1)
#endif

	movaps	 %xmm0, -32 * SIZE(Y1)
	movaps	 %xmm1, -28 * SIZE(Y1)
	movaps	 %xmm2, -24 * SIZE(Y1)
	movaps	 %xmm3, -20 * SIZE(Y1)

	movaps	 -16 * SIZE(Y1), %xmm0
	movaps	 -12 * SIZE(Y1), %xmm1
	movaps	  -8 * SIZE(Y1), %xmm2
	movaps	  -4 * SIZE(Y1), %xmm3

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, A2
	subq	 $-16 * SIZE, Y1

	subq	 $1, I
	BRANCH
	jg	.L313
	ALIGN_3

.L314:
	movss	 %xmm6,  %xmm4
	shufps	 $0x39,  %xmm4, %xmm4
	pshufd	 $0xb1, %xmm4,  %xmm5
	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	movaps	 -17 * SIZE(A1), %xmm4

	movss	 %xmm8,  %xmm6
	shufps	 $0x39,  %xmm6, %xmm6
	pshufd	 $0xb1, %xmm6,  %xmm7
	mulps	 %xmm12, %xmm6
	addps	 %xmm6,  %xmm1

	movss	 %xmm10, %xmm8
	shufps	 $0x39, %xmm8, %xmm8
	pshufd	 $0xb1, %xmm8,  %xmm9
	mulps	 %xmm12, %xmm8
	addps	 %xmm8,  %xmm2

	movss	 %xmm4, %xmm10
	shufps	 $0x39, %xmm10, %xmm10
	pshufd	 $0xb1, %xmm10, %xmm11
	mulps	 %xmm12, %xmm10
	addps	 %xmm10, %xmm3

	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0
	mulps	 %xmm13, %xmm7
	SUBPS	 %xmm7,  %xmm1

	mulps	 %xmm13, %xmm9
	SUBPS	 %xmm9,  %xmm2
	mulps	 %xmm13, %xmm11
	SUBPS	 %xmm11, %xmm3

	movaps	 %xmm0, -32 * SIZE(Y1)
	movaps	 %xmm1, -28 * SIZE(Y1)
	movaps	 %xmm2, -24 * SIZE(Y1)
	movaps	 %xmm3, -20 * SIZE(Y1)

	movaps	 -16 * SIZE(Y1), %xmm0
	movaps	 -12 * SIZE(Y1), %xmm1
	movaps	  -8 * SIZE(Y1), %xmm2
	movaps	  -4 * SIZE(Y1), %xmm3

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, Y1
	ALIGN_3

.L315:
	testq	$4, MM
	je	.L317

	movaps	 -29 * SIZE(A1), %xmm6
	movaps	 -25 * SIZE(A1), %xmm8

	movss	 %xmm6, %xmm4
	shufps	 $0x39, %xmm4, %xmm4
	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0

	movss	 %xmm8, %xmm6
	shufps	 $0x39, %xmm6, %xmm6
	pshufd	 $0xb1, %xmm6, %xmm7
	mulps	 %xmm12, %xmm6
	addps	 %xmm6,  %xmm1

	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0
	mulps	 %xmm13, %xmm7
	SUBPS	 %xmm7,  %xmm1

	movaps	 %xmm0, -32 * SIZE(Y1)
	movaps	 %xmm1, -28 * SIZE(Y1)

	movaps	 %xmm2, %xmm0
	movaps	 %xmm3, %xmm1
	movaps	 %xmm8, %xmm4

	addq	 $8 * SIZE, A1
	addq	 $8 * SIZE, Y1
	ALIGN_3

.L317:
	testq	$2, MM
	je	.L318

	movaps	 -29 * SIZE(A1), %xmm6

	movss	 %xmm6, %xmm4
	shufps	 $0x39, %xmm4, %xmm4

	pshufd	 $0xb1, %xmm4, %xmm5
	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0

	movaps	 %xmm0, -32 * SIZE(Y1)
	movaps	 %xmm1, %xmm0

	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, Y1
	ALIGN_3

.L318:
	testq	$1, MM
	je	.L990

	movsd	 -32 * SIZE(A1), %xmm4

	pshufd	 $0xb1, %xmm4, %xmm5

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	mulps	 %xmm13, %xmm5
	SUBPS	 %xmm5,  %xmm0

	movlps	 %xmm0, -32 * SIZE(Y1)
#endif
	ALIGN_3

.L990:
	movq	Y,  Y1

#ifdef ALIGNED_ACCESS
	cmpq	M, MM
	je	.L991

	movsd	(Y), %xmm0
	addq	INCY, Y

	movsd	(BUFFER), %xmm1
	addq	 $2 * SIZE, BUFFER

	addps	 %xmm1, %xmm0

	movlps	%xmm0, (Y1)
	addq	INCY, Y1
	ALIGN_3

.L991:
#endif
	movq	MM, %rax
	sarq	$3, %rax
	jle	.L994
	ALIGN_3

.L992:
	movsd	(Y), %xmm0
	addq	INCY, Y
	movhps	(Y), %xmm0
	addq	INCY, Y

	movsd	(Y), %xmm1
	addq	INCY, Y
	movhps	(Y), %xmm1
	addq	INCY, Y

	movsd	(Y), %xmm2
	addq	INCY, Y
	movhps	(Y), %xmm2
	addq	INCY, Y

	movsd	(Y), %xmm3
	addq	INCY, Y
	movhps	(Y), %xmm3
	addq	INCY, Y

	addps	 0 * SIZE(BUFFER), %xmm0
	addps	 4 * SIZE(BUFFER), %xmm1
	addps	 8 * SIZE(BUFFER), %xmm2
	addps	12 * SIZE(BUFFER), %xmm3

	movlps	%xmm0, (Y1)
	addq	INCY, Y1
	movhps	%xmm0, (Y1)
	addq	INCY, Y1

	movlps	%xmm1, (Y1)
	addq	INCY, Y1
	movhps	%xmm1, (Y1)
	addq	INCY, Y1

	movlps	%xmm2, (Y1)
	addq	INCY, Y1
	movhps	%xmm2, (Y1)
	addq	INCY, Y1

	movlps	%xmm3, (Y1)
	addq	INCY, Y1
	movhps	%xmm3, (Y1)
	addq	INCY, Y1

	addq	$16 * SIZE, BUFFER
	decq	%rax
	jg	.L992
	ALIGN_3

.L994:
	testq	$7, MM
	jle	.L999

	testq	$4, MM
	jle	.L995

	movsd	(Y), %xmm0
	addq	INCY, Y
	movhps	(Y), %xmm0
	addq	INCY, Y

	movsd	(Y), %xmm1
	addq	INCY, Y
	movhps	(Y), %xmm1
	addq	INCY, Y

	addps	 0 * SIZE(BUFFER), %xmm0
	addps	 4 * SIZE(BUFFER), %xmm1

	movlps	%xmm0, (Y1)
	addq	INCY, Y1
	movhps	%xmm0, (Y1)
	addq	INCY, Y1

	movlps	%xmm1, (Y1)
	addq	INCY, Y1
	movhps	%xmm1, (Y1)
	addq	INCY, Y1

	addq	$8 * SIZE, BUFFER
	ALIGN_3

.L995:
	testq	$2, MM
	jle	.L996

	movsd	(Y), %xmm0
	addq	INCY, Y
	movhps	(Y), %xmm0
	addq	INCY, Y

	addps	 0 * SIZE(BUFFER), %xmm0

	movlps	%xmm0, (Y1)
	addq	INCY, Y1
	movhps	%xmm0, (Y1)
	addq	INCY, Y1

	addq	$4 * SIZE, BUFFER
	ALIGN_3

.L996:
	testq	$1, MM
	jle	.L999

	movsd	(Y), %xmm0

	addps	 0 * SIZE(BUFFER), %xmm0

	movlps	%xmm0, (Y1)
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
