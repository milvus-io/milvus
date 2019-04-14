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

#define STACKSIZE	64

#define OLD_INCX	 8 + STACKSIZE(%rsp)
#define OLD_Y		16 + STACKSIZE(%rsp)
#define OLD_INCY	24 + STACKSIZE(%rsp)
#define OLD_BUFFER	32 + STACKSIZE(%rsp)

#define M	  %rdi
#define N	  %rsi
#define A	  %rcx
#define LDA	  %r8
#define X	  %r9
#define INCX	  %rdx
#define Y	  %rbp
#define INCY	  %r10

#else

#define STACKSIZE	256

#define OLD_ALPHA_I	 40 + STACKSIZE(%rsp)
#define OLD_A		 48 + STACKSIZE(%rsp)
#define OLD_LDA		 56 + STACKSIZE(%rsp)
#define OLD_X		 64 + STACKSIZE(%rsp)
#define OLD_INCX	 72 + STACKSIZE(%rsp)
#define OLD_Y		 80 + STACKSIZE(%rsp)
#define OLD_INCY	 88 + STACKSIZE(%rsp)
#define OLD_BUFFER	 96 + STACKSIZE(%rsp)

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

	movaps	%xmm3,       %xmm0
	movss	OLD_ALPHA_I, %xmm1
#endif

	movq	OLD_INCX,  INCX
	movq	OLD_Y,     Y
	movq	OLD_INCY,  INCY
	movq	OLD_BUFFER, BUFFER

	salq	$ZBASE_SHIFT,   LDA
	salq	$ZBASE_SHIFT,   INCX
	salq	$ZBASE_SHIFT,   INCY

	pcmpeqb	%xmm5, %xmm5
	psllq	$63,   %xmm5
	shufps	$0x04, %xmm5, %xmm5

	unpcklpd %xmm1, %xmm0

	movaps	%xmm0, ALPHA_R
	pshufd	$0x4e, %xmm0,  ALPHA_I

	xorps	 %xmm5, ALPHA_I

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
	xorps	%xmm0, %xmm0
	xorps	%xmm1, %xmm1
	xorps	%xmm2, %xmm2
	xorps	%xmm3, %xmm3
	MOVUPS_XL1(-14 * SIZE, X1, %xmm13)
	xorps	%xmm4, %xmm4
	xorps	%xmm5, %xmm5
	xorps	%xmm6, %xmm6
	xorps	%xmm7, %xmm7

#ifdef PREFETCHW
	PREFETCHW	3 * SIZE(Y1)
#endif

	movq	M,   I
	sarq	$2,  I
	jle	.L15

	movddup	-16 * SIZE(A1), %xmm8
	movddup	-15 * SIZE(A1), %xmm9
	movddup	-16 * SIZE(A1, LDA), %xmm10
	movddup	-15 * SIZE(A1, LDA), %xmm11

	decq	 I
	jle	 .L14
	ALIGN_3

.L13:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A1)
#endif

	mulpd	%xmm12, %xmm8
	addpd	%xmm8,  %xmm0
	movddup	-16 * SIZE(A2), %xmm8
	mulpd	%xmm12, %xmm9
	addpd	%xmm9,  %xmm1
	movddup	-15 * SIZE(A2), %xmm9
	mulpd	%xmm12, %xmm10
	addpd	%xmm10, %xmm2
	movddup	-16 * SIZE(A2, LDA), %xmm10
	mulpd	%xmm12, %xmm11
	addpd	%xmm11, %xmm3
	movddup	-15 * SIZE(A2, LDA), %xmm11

	mulpd	%xmm12, %xmm8
	addpd	%xmm8,  %xmm4
	movddup	-14 * SIZE(A1), %xmm8
	mulpd	%xmm12, %xmm9
	addpd	%xmm9,  %xmm5
	movddup	-13 * SIZE(A1), %xmm9
	mulpd	%xmm12, %xmm10
	addpd	%xmm10, %xmm6
	movddup	-14 * SIZE(A1, LDA), %xmm10
	mulpd	%xmm12, %xmm11
	MOVUPS_XL1(-12 * SIZE, X1, %xmm12)
	addpd	%xmm11, %xmm7
	movddup	-13 * SIZE(A1, LDA), %xmm11

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A1, LDA)
#endif

	mulpd	%xmm13, %xmm8
	addpd	%xmm8,  %xmm0
	movddup	-14 * SIZE(A2), %xmm8
	mulpd	%xmm13, %xmm9
	addpd	%xmm9,  %xmm1
	movddup	-13 * SIZE(A2), %xmm9
	mulpd	%xmm13, %xmm10
	addpd	%xmm10, %xmm2
	movddup	-14 * SIZE(A2, LDA), %xmm10
	mulpd	%xmm13, %xmm11
	addpd	%xmm11, %xmm3
	movddup	-13 * SIZE(A2, LDA), %xmm11

	mulpd	%xmm13, %xmm8
	addpd	%xmm8,  %xmm4
	movddup	-12 * SIZE(A1), %xmm8
	mulpd	%xmm13, %xmm9
	addpd	%xmm9,  %xmm5
	movddup	-11 * SIZE(A1), %xmm9
	mulpd	%xmm13, %xmm10
	addpd	%xmm10, %xmm6
	movddup	-12 * SIZE(A1, LDA), %xmm10
	mulpd	%xmm13, %xmm11
	MOVUPS_XL1(-10 * SIZE, X1, %xmm13)
	addpd	%xmm11, %xmm7
	movddup	-11 * SIZE(A1, LDA), %xmm11

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A2)
#endif

	mulpd	%xmm12, %xmm8
	addpd	%xmm8,  %xmm0
	movddup	-12 * SIZE(A2), %xmm8
	mulpd	%xmm12, %xmm9
	addpd	%xmm9,  %xmm1
	movddup	-11 * SIZE(A2), %xmm9
	mulpd	%xmm12, %xmm10
	addpd	%xmm10, %xmm2
	movddup	-12 * SIZE(A2, LDA), %xmm10
	mulpd	%xmm12, %xmm11
	addpd	%xmm11, %xmm3
	movddup	-11 * SIZE(A2, LDA), %xmm11

	mulpd	%xmm12, %xmm8
	addpd	%xmm8,  %xmm4
	movddup	-10 * SIZE(A1), %xmm8
	mulpd	%xmm12, %xmm9
	addpd	%xmm9,  %xmm5
	movddup	 -9 * SIZE(A1), %xmm9
	mulpd	%xmm12, %xmm10
	addpd	%xmm10, %xmm6
	movddup	-10 * SIZE(A1, LDA), %xmm10
	mulpd	%xmm12, %xmm11
	MOVUPS_XL1( -8 * SIZE, X1, %xmm12)
	addpd	%xmm11, %xmm7
	movddup	 -9 * SIZE(A1, LDA), %xmm11

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A2, LDA)
#endif

	mulpd	%xmm13, %xmm8
	addpd	%xmm8,  %xmm0
	movddup	-10 * SIZE(A2), %xmm8
	mulpd	%xmm13, %xmm9
	addpd	%xmm9,  %xmm1
	movddup	 -9 * SIZE(A2), %xmm9
	mulpd	%xmm13, %xmm10
	addpd	%xmm10, %xmm2
	movddup	-10 * SIZE(A2, LDA), %xmm10
	mulpd	%xmm13, %xmm11
	addpd	%xmm11, %xmm3
	movddup	 -9 * SIZE(A2, LDA), %xmm11

#ifdef PREFETCHW
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(X1)
#endif

	mulpd	%xmm13, %xmm8
	addpd	%xmm8,  %xmm4
	movddup	 -8 * SIZE(A1), %xmm8
	mulpd	%xmm13, %xmm9
	addpd	%xmm9,  %xmm5
	movddup	 -7 * SIZE(A1), %xmm9
	mulpd	%xmm13, %xmm10
	addpd	%xmm10, %xmm6
	movddup	 -8 * SIZE(A1, LDA), %xmm10
	mulpd	%xmm13, %xmm11
	MOVUPS_XL1( -6 * SIZE, X1, %xmm13)
	addpd	%xmm11, %xmm7
	movddup	 -7 * SIZE(A1, LDA), %xmm11

	subq	 $-8 * SIZE, A1
	subq	 $-8 * SIZE, A2
	subq	 $-8 * SIZE, X1

	subq	 $1, I
	BRANCH
	jg	.L13
	ALIGN_3

.L14:
	mulpd	%xmm12, %xmm8
	addpd	%xmm8,  %xmm0
	movddup	-16 * SIZE(A2), %xmm8
	mulpd	%xmm12, %xmm9
	addpd	%xmm9,  %xmm1
	movddup	-15 * SIZE(A2), %xmm9
	mulpd	%xmm12, %xmm10
	addpd	%xmm10, %xmm2
	movddup	-16 * SIZE(A2, LDA), %xmm10
	mulpd	%xmm12, %xmm11
	addpd	%xmm11, %xmm3
	movddup	-15 * SIZE(A2, LDA), %xmm11

	mulpd	%xmm12, %xmm8
	addpd	%xmm8,  %xmm4
	movddup	-14 * SIZE(A1), %xmm8
	mulpd	%xmm12, %xmm9
	addpd	%xmm9,  %xmm5
	movddup	-13 * SIZE(A1), %xmm9
	mulpd	%xmm12, %xmm10
	addpd	%xmm10, %xmm6
	movddup	-14 * SIZE(A1, LDA), %xmm10
	mulpd	%xmm12, %xmm11
	MOVUPS_XL1(-12 * SIZE, X1, %xmm12)
	addpd	%xmm11, %xmm7
	movddup	-13 * SIZE(A1, LDA), %xmm11

	mulpd	%xmm13, %xmm8
	addpd	%xmm8,  %xmm0
	movddup	-14 * SIZE(A2), %xmm8
	mulpd	%xmm13, %xmm9
	addpd	%xmm9,  %xmm1
	movddup	-13 * SIZE(A2), %xmm9
	mulpd	%xmm13, %xmm10
	addpd	%xmm10, %xmm2
	movddup	-14 * SIZE(A2, LDA), %xmm10
	mulpd	%xmm13, %xmm11
	addpd	%xmm11, %xmm3
	movddup	-13 * SIZE(A2, LDA), %xmm11

	mulpd	%xmm13, %xmm8
	addpd	%xmm8,  %xmm4
	movddup	-12 * SIZE(A1), %xmm8
	mulpd	%xmm13, %xmm9
	addpd	%xmm9,  %xmm5
	movddup	-11 * SIZE(A1), %xmm9
	mulpd	%xmm13, %xmm10
	addpd	%xmm10, %xmm6
	movddup	-12 * SIZE(A1, LDA), %xmm10
	mulpd	%xmm13, %xmm11
	MOVUPS_XL1(-10 * SIZE, X1, %xmm13)
	addpd	%xmm11, %xmm7
	movddup	-11 * SIZE(A1, LDA), %xmm11

	mulpd	%xmm12, %xmm8
	addpd	%xmm8,  %xmm0
	movddup	-12 * SIZE(A2), %xmm8
	mulpd	%xmm12, %xmm9
	addpd	%xmm9,  %xmm1
	movddup	-11 * SIZE(A2), %xmm9
	mulpd	%xmm12, %xmm10
	addpd	%xmm10, %xmm2
	movddup	-12 * SIZE(A2, LDA), %xmm10
	mulpd	%xmm12, %xmm11
	addpd	%xmm11, %xmm3
	movddup	-11 * SIZE(A2, LDA), %xmm11

	mulpd	%xmm12, %xmm8
	addpd	%xmm8,  %xmm4
	movddup	-10 * SIZE(A1), %xmm8
	mulpd	%xmm12, %xmm9
	addpd	%xmm9,  %xmm5
	movddup	 -9 * SIZE(A1), %xmm9
	mulpd	%xmm12, %xmm10
	addpd	%xmm10, %xmm6
	movddup	-10 * SIZE(A1, LDA), %xmm10
	mulpd	%xmm12, %xmm11
	MOVUPS_XL1( -8 * SIZE, X1, %xmm12)
	addpd	%xmm11, %xmm7
	movddup	 -9 * SIZE(A1, LDA), %xmm11

	mulpd	%xmm13, %xmm8
	addpd	%xmm8,  %xmm0
	movddup	-10 * SIZE(A2), %xmm8
	mulpd	%xmm13, %xmm9
	addpd	%xmm9,  %xmm1
	movddup	 -9 * SIZE(A2), %xmm9
	mulpd	%xmm13, %xmm10
	addpd	%xmm10, %xmm2
	movddup	-10 * SIZE(A2, LDA), %xmm10
	mulpd	%xmm13, %xmm11
	addpd	%xmm11, %xmm3
	movddup	 -9 * SIZE(A2, LDA), %xmm11

	mulpd	%xmm13, %xmm8
	addpd	%xmm8,  %xmm4
	mulpd	%xmm13, %xmm9
	addpd	%xmm9,  %xmm5
	mulpd	%xmm13, %xmm10
	addpd	%xmm10, %xmm6
	mulpd	%xmm13, %xmm11
	MOVUPS_XL1( -6 * SIZE, X1, %xmm13)
	addpd	%xmm11, %xmm7

	subq	 $-8 * SIZE, A1
	subq	 $-8 * SIZE, A2
	subq	 $-8 * SIZE, X1
	ALIGN_3

.L15:
	testq	$2, M
	je	.L17

	movddup	-16 * SIZE(A1), %xmm8
	movddup	-15 * SIZE(A1), %xmm9
	movddup	-16 * SIZE(A1, LDA), %xmm10
	movddup	-15 * SIZE(A1, LDA), %xmm11

	mulpd	%xmm12, %xmm8
	addpd	%xmm8,  %xmm0
	movddup	-16 * SIZE(A2), %xmm8
	mulpd	%xmm12, %xmm9
	addpd	%xmm9,  %xmm1
	movddup	-15 * SIZE(A2), %xmm9
	mulpd	%xmm12, %xmm10
	addpd	%xmm10, %xmm2
	movddup	-16 * SIZE(A2, LDA), %xmm10
	mulpd	%xmm12, %xmm11
	addpd	%xmm11, %xmm3
	movddup	-15 * SIZE(A2, LDA), %xmm11

	mulpd	%xmm12, %xmm8
	addpd	%xmm8,  %xmm4
	movddup	-14 * SIZE(A1), %xmm8
	mulpd	%xmm12, %xmm9
	addpd	%xmm9,  %xmm5
	movddup	-13 * SIZE(A1), %xmm9
	mulpd	%xmm12, %xmm10
	addpd	%xmm10, %xmm6
	movddup	-14 * SIZE(A1, LDA), %xmm10
	mulpd	%xmm12, %xmm11
	addpd	%xmm11, %xmm7
	movddup	-13 * SIZE(A1, LDA), %xmm11

	MOVUPS_XL1(-12 * SIZE, X1, %xmm12)

	mulpd	%xmm13, %xmm8
	addpd	%xmm8,  %xmm0
	movddup	-14 * SIZE(A2), %xmm8
	mulpd	%xmm13, %xmm9
	addpd	%xmm9,  %xmm1
	movddup	-13 * SIZE(A2), %xmm9
	mulpd	%xmm13, %xmm10
	addpd	%xmm10, %xmm2
	movddup	-14 * SIZE(A2, LDA), %xmm10
	mulpd	%xmm13, %xmm11
	addpd	%xmm11, %xmm3
	movddup	-13 * SIZE(A2, LDA), %xmm11

	mulpd	%xmm13, %xmm8
	addpd	%xmm8,  %xmm4
	mulpd	%xmm13, %xmm9
	addpd	%xmm9,  %xmm5
	mulpd	%xmm13, %xmm10
	addpd	%xmm10, %xmm6
	mulpd	%xmm13, %xmm11
	addpd	%xmm11, %xmm7

	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, A2
	ALIGN_3

.L17:
	testq	$1, M
	je	.L19

	movddup	-16 * SIZE(A1), %xmm8
	movddup	-15 * SIZE(A1), %xmm9
	movddup	-16 * SIZE(A1, LDA), %xmm10
	movddup	-15 * SIZE(A1, LDA), %xmm11

	mulpd	%xmm12, %xmm8
	addpd	%xmm8,  %xmm0
	movddup	-16 * SIZE(A2), %xmm8
	mulpd	%xmm12, %xmm9
	addpd	%xmm9,  %xmm1
	movddup	-15 * SIZE(A2), %xmm9
	mulpd	%xmm12, %xmm10
	addpd	%xmm10, %xmm2
	movddup	-16 * SIZE(A2, LDA), %xmm10
	mulpd	%xmm12, %xmm11
	addpd	%xmm11, %xmm3
	movddup	-15 * SIZE(A2, LDA), %xmm11

	mulpd	%xmm12, %xmm8
	addpd	%xmm8,  %xmm4
	mulpd	%xmm12, %xmm9
	addpd	%xmm9,  %xmm5
	mulpd	%xmm12, %xmm10
	addpd	%xmm10, %xmm6
	mulpd	%xmm12, %xmm11
	addpd	%xmm11, %xmm7
	ALIGN_3

.L19:
	pcmpeqb	%xmm13, %xmm13
	psllq	$63,    %xmm13
	shufps	$0x40, %xmm13, %xmm13

#ifndef XCONJ
	xorps	%xmm13, %xmm1
	xorps	%xmm13, %xmm3
	xorps	%xmm13, %xmm5
	xorps	%xmm13, %xmm7
#else
	xorps	%xmm13, %xmm0
	xorps	%xmm13, %xmm2
	xorps	%xmm13, %xmm4
	xorps	%xmm13, %xmm6
#endif

	pshufd	$0x4e, %xmm1, %xmm1
	pshufd	$0x4e, %xmm3, %xmm3
	pshufd	$0x4e, %xmm5, %xmm5
	pshufd	$0x4e, %xmm7, %xmm7

#ifndef CONJ
	addpd	%xmm1, %xmm0
	addpd	%xmm3, %xmm2
	addpd	%xmm5, %xmm4
	addpd	%xmm7, %xmm6
#else
	subpd	%xmm1, %xmm0
	subpd	%xmm3, %xmm2
	subpd	%xmm5, %xmm4
	subpd	%xmm7, %xmm6
#endif

	pshufd	$0xee, %xmm0, %xmm1
	movddup	%xmm0, %xmm0
	pshufd	$0xee, %xmm2, %xmm3
	movddup	%xmm2, %xmm2
	pshufd	$0xee, %xmm4, %xmm5
	movddup	%xmm4, %xmm4
	pshufd	$0xee, %xmm6, %xmm7
	movddup	%xmm6, %xmm6

	mulpd	ALPHA_R, %xmm0
	mulpd	ALPHA_I, %xmm1
	mulpd	ALPHA_R, %xmm2
	mulpd	ALPHA_I, %xmm3
	mulpd	ALPHA_R, %xmm4
	mulpd	ALPHA_I, %xmm5
	mulpd	ALPHA_R, %xmm6
	mulpd	ALPHA_I, %xmm7

	addpd	%xmm1, %xmm0
	addpd	%xmm3, %xmm2
	addpd	%xmm5, %xmm4
	addpd	%xmm7, %xmm6

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

	MOVUPS_XL1(-16 * SIZE, X1, %xmm12)
	xorps	%xmm0, %xmm0
	xorps	%xmm1, %xmm1
	MOVUPS_XL1(-14 * SIZE, X1, %xmm13)
	xorps	%xmm2, %xmm2
	xorps	%xmm3, %xmm3

#ifdef PREFETCHW
	PREFETCHW	3 * SIZE(Y1)
#endif

	movq	M,   I
	sarq	$2,  I
	jle	.L25

	movddup	-16 * SIZE(A1), %xmm8
	movddup	-15 * SIZE(A1), %xmm9
	movddup	-16 * SIZE(A1, LDA), %xmm10
	movddup	-15 * SIZE(A1, LDA), %xmm11

	decq	 I
	jle	 .L24
	ALIGN_3

.L23:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(A1)
#endif

	mulpd	%xmm12, %xmm8
	addpd	%xmm8,  %xmm0
	movddup	-14 * SIZE(A1), %xmm8
	mulpd	%xmm12, %xmm9
	addpd	%xmm9,  %xmm1
 	movddup	-13 * SIZE(A1), %xmm9
	mulpd	%xmm12, %xmm10
	addpd	%xmm10, %xmm2
	movddup	-14 * SIZE(A1, LDA), %xmm10
	mulpd	%xmm12, %xmm11
	MOVUPS_XL1(-12 * SIZE, X1, %xmm12)
	addpd	%xmm11, %xmm3
	movddup	-13 * SIZE(A1, LDA), %xmm11

	mulpd	%xmm13, %xmm8
	addpd	%xmm8,  %xmm0
	movddup	-12 * SIZE(A1), %xmm8
	mulpd	%xmm13, %xmm9
	addpd	%xmm9,  %xmm1
	movddup	-11 * SIZE(A1), %xmm9
	mulpd	%xmm13, %xmm10
	addpd	%xmm10, %xmm2
	movddup	-12 * SIZE(A1, LDA), %xmm10
	mulpd	%xmm13, %xmm11
	MOVUPS_XL1(-10 * SIZE, X1, %xmm13)
	addpd	%xmm11, %xmm3
	movddup	-11 * SIZE(A1, LDA), %xmm11

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(A2)
#endif

	mulpd	%xmm12, %xmm8
	addpd	%xmm8,  %xmm0
	movddup	-10 * SIZE(A1), %xmm8
	mulpd	%xmm12, %xmm9
	addpd	%xmm9,  %xmm1
 	movddup	 -9 * SIZE(A1), %xmm9
	mulpd	%xmm12, %xmm10
	addpd	%xmm10, %xmm2
	movddup	-10 * SIZE(A1, LDA), %xmm10
	mulpd	%xmm12, %xmm11
	MOVUPS_XL1( -8 * SIZE, X1, %xmm12)
	addpd	%xmm11, %xmm3
	movddup	 -9 * SIZE(A1, LDA), %xmm11

#ifdef PREFETCHW
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(X1)
#endif

	mulpd	%xmm13, %xmm8
	addpd	%xmm8,  %xmm0
	movddup	 -8 * SIZE(A1), %xmm8
	mulpd	%xmm13, %xmm9
	addpd	%xmm9,  %xmm1
	movddup	 -7 * SIZE(A1), %xmm9
	mulpd	%xmm13, %xmm10
	addpd	%xmm10, %xmm2
	movddup	 -8 * SIZE(A1, LDA), %xmm10
	mulpd	%xmm13, %xmm11
	MOVUPS_XL1( -6 * SIZE, X1, %xmm13)
	addpd	%xmm11, %xmm3
	movddup	 -7 * SIZE(A1, LDA), %xmm11

	subq	 $-8 * SIZE, A1
	subq	 $-8 * SIZE, A2
	subq	 $-8 * SIZE, X1

	subq	 $1, I
	BRANCH
	jg	.L23
	ALIGN_3

.L24:
	mulpd	%xmm12, %xmm8
	addpd	%xmm8,  %xmm0
	movddup	-14 * SIZE(A1), %xmm8
	mulpd	%xmm12, %xmm9
	addpd	%xmm9,  %xmm1
 	movddup	-13 * SIZE(A1), %xmm9
	mulpd	%xmm12, %xmm10
	addpd	%xmm10, %xmm2
	movddup	-14 * SIZE(A1, LDA), %xmm10
	mulpd	%xmm12, %xmm11
	MOVUPS_XL1(-12 * SIZE, X1, %xmm12)
	addpd	%xmm11, %xmm3
	movddup	-13 * SIZE(A1, LDA), %xmm11

	mulpd	%xmm13, %xmm8
	addpd	%xmm8,  %xmm0
	movddup	-12 * SIZE(A1), %xmm8
	mulpd	%xmm13, %xmm9
	addpd	%xmm9,  %xmm1
	movddup	-11 * SIZE(A1), %xmm9
	mulpd	%xmm13, %xmm10
	addpd	%xmm10, %xmm2
	movddup	-12 * SIZE(A1, LDA), %xmm10
	mulpd	%xmm13, %xmm11
	MOVUPS_XL1(-10 * SIZE, X1, %xmm13)
	addpd	%xmm11, %xmm3
	movddup	-11 * SIZE(A1, LDA), %xmm11

	mulpd	%xmm12, %xmm8
	addpd	%xmm8,  %xmm0
	movddup	-10 * SIZE(A1), %xmm8
	mulpd	%xmm12, %xmm9
	addpd	%xmm9,  %xmm1
 	movddup	 -9 * SIZE(A1), %xmm9
	mulpd	%xmm12, %xmm10
	addpd	%xmm10, %xmm2
	movddup	-10 * SIZE(A1, LDA), %xmm10
	mulpd	%xmm12, %xmm11
	MOVUPS_XL1( -8 * SIZE, X1, %xmm12)
	addpd	%xmm11, %xmm3
	movddup	 -9 * SIZE(A1, LDA), %xmm11

	mulpd	%xmm13, %xmm8
	addpd	%xmm8,  %xmm0
	mulpd	%xmm13, %xmm9
	addpd	%xmm9,  %xmm1
	mulpd	%xmm13, %xmm10
	addpd	%xmm10, %xmm2
	mulpd	%xmm13, %xmm11
	MOVUPS_XL1( -6 * SIZE, X1, %xmm13)
	addpd	%xmm11, %xmm3

	subq	 $-8 * SIZE, A1
	subq	 $-8 * SIZE, A2
	subq	 $-8 * SIZE, X1
	ALIGN_3

.L25:
	testq	$2, M
	je	.L27

	movddup	-16 * SIZE(A1), %xmm8
	movddup	-15 * SIZE(A1), %xmm9
	movddup	-16 * SIZE(A1, LDA), %xmm10
	movddup	-15 * SIZE(A1, LDA), %xmm11

	mulpd	%xmm12, %xmm8
	addpd	%xmm8,  %xmm0
	movddup	-14 * SIZE(A1), %xmm8
	mulpd	%xmm12, %xmm9
	addpd	%xmm9,  %xmm1
 	movddup	-13 * SIZE(A1), %xmm9
	mulpd	%xmm12, %xmm10
	addpd	%xmm10, %xmm2
	movddup	-14 * SIZE(A1, LDA), %xmm10
	mulpd	%xmm12, %xmm11
	MOVUPS_XL1(-12 * SIZE, X1, %xmm12)
	addpd	%xmm11, %xmm3
	movddup	-13 * SIZE(A1, LDA), %xmm11

	mulpd	%xmm13, %xmm8
	addpd	%xmm8,  %xmm0
	mulpd	%xmm13, %xmm9
	addpd	%xmm9,  %xmm1
	mulpd	%xmm13, %xmm10
	addpd	%xmm10, %xmm2
	mulpd	%xmm13, %xmm11
	addpd	%xmm11, %xmm3

	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, A2
	ALIGN_3

.L27:
	testq	$1, M
	je	.L29

	movddup	-16 * SIZE(A1), %xmm8
	movddup	-15 * SIZE(A1), %xmm9
	movddup	-16 * SIZE(A1, LDA), %xmm10
	movddup	-15 * SIZE(A1, LDA), %xmm11

	mulpd	%xmm12, %xmm8
	addpd	%xmm8,  %xmm0
	mulpd	%xmm12, %xmm9
	addpd	%xmm9,  %xmm1
	mulpd	%xmm12, %xmm10
	addpd	%xmm10, %xmm2
	mulpd	%xmm12, %xmm11
	addpd	%xmm11, %xmm3
	ALIGN_3

.L29:
	pcmpeqb	%xmm13, %xmm13
	psllq	$63,    %xmm13
	shufps	$0x40, %xmm13, %xmm13

#ifndef XCONJ
	xorps	%xmm13, %xmm1
	xorps	%xmm13, %xmm3
#else
	xorps	%xmm13, %xmm0
	xorps	%xmm13, %xmm2
#endif

	pshufd	$0x4e, %xmm1, %xmm1
	pshufd	$0x4e, %xmm3, %xmm3

#ifndef CONJ
	addpd	%xmm1, %xmm0
	addpd	%xmm3, %xmm2
#else
	subpd	%xmm1, %xmm0
	subpd	%xmm3, %xmm2
#endif

	pshufd	$0xee, %xmm0, %xmm1
	movddup	%xmm0, %xmm0
	pshufd	$0xee, %xmm2, %xmm3
	movddup	%xmm2, %xmm2

	mulpd	ALPHA_R, %xmm0
	mulpd	ALPHA_I, %xmm1
	mulpd	ALPHA_R, %xmm2
	mulpd	ALPHA_I, %xmm3

	addpd	%xmm1, %xmm0
	addpd	%xmm3, %xmm2

	movsd	 0 * SIZE(Y), %xmm1
	movhpd	 1 * SIZE(Y), %xmm1
	addq	INCY, Y
	movsd	 0 * SIZE(Y), %xmm3
	movhpd	 1 * SIZE(Y), %xmm3
	addq	INCY, Y

	addpd	%xmm1, %xmm0
	addpd	%xmm3, %xmm2

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

	MOVUPS_XL1(-16 * SIZE, X1, %xmm12)
	xorps	%xmm0, %xmm0
	xorps	%xmm1, %xmm1
	MOVUPS_XL1(-14 * SIZE, X1, %xmm13)
	xorps	%xmm2, %xmm2
	xorps	%xmm3, %xmm3

	movq	M,   I
	sarq	$2,  I
	jle	.L35

	movddup	-16 * SIZE(A1), %xmm8
	movddup	-15 * SIZE(A1), %xmm9
	movddup	-14 * SIZE(A1), %xmm10
 	movddup	-13 * SIZE(A1), %xmm11

	decq	 I
	jle	 .L34
	ALIGN_3

.L33:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 4 - 128 + PREOFFSET(A1)
#endif

	mulpd	%xmm12, %xmm8
	addpd	%xmm8,  %xmm0
	movddup	-12 * SIZE(A1), %xmm8
	mulpd	%xmm12, %xmm9
	MOVUPS_XL1(-12 * SIZE, X1, %xmm12)
	addpd	%xmm9,  %xmm1
	movddup	-11 * SIZE(A1), %xmm9

	mulpd	%xmm13, %xmm10
	addpd	%xmm10, %xmm2
	movddup	-10 * SIZE(A1), %xmm10
	mulpd	%xmm13, %xmm11
	MOVUPS_XL1(-10 * SIZE, X1, %xmm13)
	addpd	%xmm11, %xmm3
 	movddup	 -9 * SIZE(A1), %xmm11

#ifdef PREFETCHW
	PREFETCH	(PREFETCHSIZE) * 4 - 128 + PREOFFSET(X1)
#endif

	mulpd	%xmm12, %xmm8
	addpd	%xmm8,  %xmm0
	movddup	 -8 * SIZE(A1), %xmm8
	mulpd	%xmm12, %xmm9
	MOVUPS_XL1( -8 * SIZE, X1, %xmm12)
	addpd	%xmm9,  %xmm1
	movddup	 -7 * SIZE(A1), %xmm9

	mulpd	%xmm13, %xmm10
	addpd	%xmm10, %xmm2
	movddup	 -6 * SIZE(A1), %xmm10
	mulpd	%xmm13, %xmm11
	MOVUPS_XL1( -6 * SIZE, X1, %xmm13)
	addpd	%xmm11, %xmm3
 	movddup	 -5 * SIZE(A1), %xmm11

	subq	 $-8 * SIZE, A1
	subq	 $-8 * SIZE, X1

	subq	 $1, I
	BRANCH
	jg	.L33
	ALIGN_3

.L34:
	mulpd	%xmm12, %xmm8
	addpd	%xmm8,  %xmm0
	movddup	-12 * SIZE(A1), %xmm8
	mulpd	%xmm12, %xmm9
	MOVUPS_XL1(-12 * SIZE, X1, %xmm12)
	addpd	%xmm9,  %xmm1
	movddup	-11 * SIZE(A1), %xmm9

	mulpd	%xmm13, %xmm10
	addpd	%xmm10, %xmm2
	movddup	-10 * SIZE(A1), %xmm10
	mulpd	%xmm13, %xmm11
	MOVUPS_XL1(-10 * SIZE, X1, %xmm13)
	addpd	%xmm11, %xmm3
 	movddup	 -9 * SIZE(A1), %xmm11

	mulpd	%xmm12, %xmm8
	addpd	%xmm8,  %xmm0
	mulpd	%xmm12, %xmm9
	MOVUPS_XL1( -8 * SIZE, X1, %xmm12)
	addpd	%xmm9,  %xmm1

	mulpd	%xmm13, %xmm10
	addpd	%xmm10, %xmm2
	mulpd	%xmm13, %xmm11
	MOVUPS_XL1( -6 * SIZE, X1, %xmm13)
	addpd	%xmm11, %xmm3

	subq	 $-8 * SIZE, A1
	subq	 $-8 * SIZE, X1
	ALIGN_3

.L35:
	testq	$2, M
	je	.L37

	movddup	-16 * SIZE(A1), %xmm8
	movddup	-15 * SIZE(A1), %xmm9
	movddup	-14 * SIZE(A1), %xmm10
 	movddup	-13 * SIZE(A1), %xmm11

	mulpd	%xmm12, %xmm8
	addpd	%xmm8,  %xmm0
	mulpd	%xmm12, %xmm9
	addpd	%xmm9,  %xmm1
	MOVUPS_XL1(-12 * SIZE, X1, %xmm12)

	mulpd	%xmm13, %xmm10
	addpd	%xmm10, %xmm2
	mulpd	%xmm13, %xmm11
	addpd	%xmm11, %xmm3

	addq	 $4 * SIZE, A1
	ALIGN_3

.L37:
	testq	$1, M
	je	.L39

	movddup	-16 * SIZE(A1), %xmm8
	movddup	-15 * SIZE(A1), %xmm9

	mulpd	%xmm12, %xmm8
	addpd	%xmm8,  %xmm0
	mulpd	%xmm12, %xmm9
	addpd	%xmm9,  %xmm1
	ALIGN_3

.L39:
	addpd	%xmm2, %xmm0
	addpd	%xmm3, %xmm1

	pcmpeqb	%xmm13, %xmm13
	psllq	$63,    %xmm13
	shufps	$0x40, %xmm13, %xmm13

#ifndef XCONJ
	xorps	%xmm13, %xmm1
#else
	xorps	%xmm13, %xmm0
#endif

	pshufd	$0x4e, %xmm1, %xmm1

#ifndef CONJ
	addpd	%xmm1, %xmm0
#else
	subpd	%xmm1, %xmm0
#endif

	pshufd	$0xee, %xmm0, %xmm1
	movddup	%xmm0, %xmm0

	mulpd	ALPHA_R, %xmm0
	mulpd	ALPHA_I, %xmm1

	addpd	%xmm1, %xmm0

	movsd	 0 * SIZE(Y), %xmm1
	movhpd	 1 * SIZE(Y), %xmm1

	addpd	%xmm1, %xmm0

	movlpd	%xmm0,  0 * SIZE(Y1)
	movhpd	%xmm0,  1 * SIZE(Y1)

#if GEMV_UNROLL == 1
	addq	INCY, Y
	addq	INCY, Y1

	cmpq	$1, N
	jge	.L31
#endif
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
