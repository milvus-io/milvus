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

#ifdef ATOM
#define PREFETCH	prefetchnta
#define PREFETCHW	prefetcht0
#define PREFETCHSIZE	(8 * 6)
#endif

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

#define ALPHA_R		%xmm14
#define ALPHA_I		%xmm15

#if !defined(CONJ) && !defined(XCONJ)
#define ADD1	   addsd
#define ADD2	   addsd
#define ADD3	   subsd
#define ADD4	   addsd
#endif

#if  defined(CONJ) && !defined(XCONJ)
#define ADD1	   addsd
#define ADD2	   addsd
#define ADD3	   addsd
#define ADD4	   subsd
#endif

#if !defined(CONJ) &&  defined(XCONJ)
#define ADD1	   addsd
#define ADD2	   subsd
#define ADD3	   addsd
#define ADD4	   addsd
#endif

#if  defined(CONJ) &&  defined(XCONJ)
#define ADD1	   addsd
#define ADD2	   subsd
#define ADD3	   subsd
#define ADD4	   subsd
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

	movaps	%xmm0, ALPHA_R
	movaps	%xmm1, ALPHA_I

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
	movq	N,  J
	sarq	$1, J
	jle	.L20
	ALIGN_3

.L11:
	leaq	16 * SIZE(BUFFER), X1

	movq	A, A1
	leaq	(A1, LDA), A2
	leaq	(A1, LDA, 2), A

	xorpd	%xmm0, %xmm0
	xorpd	%xmm1, %xmm1
	xorpd	%xmm2, %xmm2
	xorpd	%xmm3, %xmm3

	movsd	-16 * SIZE(X1), %xmm4
	movsd	-15 * SIZE(X1), %xmm5
	movsd	-14 * SIZE(X1), %xmm6
	movsd	-13 * SIZE(X1), %xmm7

#ifdef PREFETCHW
	PREFETCHW	3 * SIZE(Y1)
#endif

	movq	M,   I
	sarq	$2,  I
	jle	.L15

	movsd	-16 * SIZE(A1), %xmm8
	movsd	-15 * SIZE(A1), %xmm9
	movsd	-16 * SIZE(A2), %xmm10
	movsd	-15 * SIZE(A2), %xmm11

	movapd	%xmm8,  %xmm12
	mulsd	%xmm4,  %xmm8
	mulsd	%xmm5,  %xmm12

	decq	 I
	jle	 .L14
	ALIGN_3

.L13:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE + 0) * SIZE(A1)
#endif

	movapd	%xmm9,  %xmm13
	mulsd	%xmm5,  %xmm9
	ADD1	%xmm8,  %xmm0
	movsd	-14 * SIZE(A1), %xmm8
	mulsd	%xmm4,  %xmm13
	ADD2	%xmm12, %xmm1

	movapd	%xmm10, %xmm12
	mulsd	%xmm4,  %xmm10
	ADD3	%xmm9,  %xmm0
	movsd	-13 * SIZE(A1), %xmm9
	mulsd	%xmm5,  %xmm12
	ADD4	%xmm13, %xmm1

	movapd	%xmm11, %xmm13
	mulsd	%xmm5,  %xmm11
	movsd	-11 * SIZE(X1), %xmm5
	ADD1	%xmm10, %xmm2
	movsd	-14 * SIZE(A2), %xmm10
	mulsd	%xmm4,  %xmm13
	movsd	-12 * SIZE(X1), %xmm4
	ADD2	%xmm12, %xmm3

	movapd	%xmm8,  %xmm12
	mulsd	%xmm6,  %xmm8
	ADD3	%xmm11, %xmm2
	movsd	-13 * SIZE(A2), %xmm11

	mulsd	%xmm7,  %xmm12
	ADD4	%xmm13, %xmm3

	movapd	%xmm9,  %xmm13
	mulsd	%xmm7,  %xmm9
	ADD1	%xmm8,  %xmm0
	movsd	-12 * SIZE(A1), %xmm8
	mulsd	%xmm6,  %xmm13
	ADD2	%xmm12, %xmm1

	movapd	%xmm10, %xmm12
	mulsd	%xmm6,  %xmm10
	ADD3	%xmm9,  %xmm0
	movsd	-11 * SIZE(A1), %xmm9
	mulsd	%xmm7,  %xmm12
	ADD4	%xmm13, %xmm1

	movapd	%xmm11, %xmm13
	mulsd	%xmm7,  %xmm11
	movsd	 -9 * SIZE(X1), %xmm7
	ADD1	%xmm10, %xmm2
	movsd	-12 * SIZE(A2), %xmm10
	mulsd	%xmm6,  %xmm13
	movsd	-10 * SIZE(X1), %xmm6
	ADD2	%xmm12, %xmm3

	movapd	%xmm8,  %xmm12
	mulsd	%xmm4,  %xmm8
	ADD3	%xmm11, %xmm2
	movsd	-11 * SIZE(A2), %xmm11
	mulsd	%xmm5,  %xmm12
	ADD4	%xmm13, %xmm3

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE + 0) * SIZE(A2)
#endif

	movapd	%xmm9,  %xmm13
	mulsd	%xmm5,  %xmm9
	ADD1	%xmm8,  %xmm0
	movsd	-10 * SIZE(A1), %xmm8
	mulsd	%xmm4,  %xmm13
	ADD2	%xmm12, %xmm1

	movapd	%xmm10, %xmm12
	mulsd	%xmm4,  %xmm10
	ADD3	%xmm9,  %xmm0
	movsd	 -9 * SIZE(A1), %xmm9
	mulsd	%xmm5,  %xmm12
	ADD4	%xmm13, %xmm1

	movapd	%xmm11, %xmm13
	mulsd	%xmm5,  %xmm11
	movsd	 -7 * SIZE(X1), %xmm5
	ADD1	%xmm10, %xmm2
	movsd	-10 * SIZE(A2), %xmm10
	mulsd	%xmm4,  %xmm13
	movsd	 -8 * SIZE(X1), %xmm4
	ADD2	%xmm12, %xmm3

	movapd	%xmm8,  %xmm12
	mulsd	%xmm6,  %xmm8
	ADD3	%xmm11, %xmm2
	movsd	 -9 * SIZE(A2), %xmm11

	mulsd	%xmm7,  %xmm12
	ADD4	%xmm13, %xmm3

	movapd	%xmm9,  %xmm13
	mulsd	%xmm7,  %xmm9
	ADD1	%xmm8,  %xmm0
	movsd	 -8 * SIZE(A1), %xmm8
	mulsd	%xmm6,  %xmm13
	ADD2	%xmm12, %xmm1

	movapd	%xmm10, %xmm12
	mulsd	%xmm6,  %xmm10
	ADD3	%xmm9,  %xmm0
	movsd	 -7 * SIZE(A1), %xmm9
	mulsd	%xmm7,  %xmm12
	ADD4	%xmm13, %xmm1

	movapd	%xmm11, %xmm13
	mulsd	%xmm7,  %xmm11
	movsd	 -5 * SIZE(X1), %xmm7
	ADD1	%xmm10, %xmm2
	movsd	 -8 * SIZE(A2), %xmm10
	mulsd	%xmm6,  %xmm13
	movsd	 -6 * SIZE(X1), %xmm6
	ADD2	%xmm12, %xmm3

	movapd	%xmm8,  %xmm12
	subq	 $-8 * SIZE, A1
	mulsd	%xmm4,  %xmm8
	subq	 $-8 * SIZE, X1
	ADD3	%xmm11, %xmm2
	movsd	 -7 * SIZE(A2), %xmm11
	mulsd	%xmm5,  %xmm12
	subq	 $-8 * SIZE, A2
	ADD4	%xmm13, %xmm3

	subq	 $1, I
	BRANCH
	jg	.L13
	ALIGN_3

.L14:
	movapd	%xmm9,  %xmm13
	mulsd	%xmm5,  %xmm9
	ADD1	%xmm8,  %xmm0
	movsd	-14 * SIZE(A1), %xmm8
	mulsd	%xmm4,  %xmm13
	ADD2	%xmm12, %xmm1

	movapd	%xmm10, %xmm12
	mulsd	%xmm4,  %xmm10
	ADD3	%xmm9,  %xmm0
	movsd	-13 * SIZE(A1), %xmm9
	mulsd	%xmm5,  %xmm12
	ADD4	%xmm13, %xmm1

	movapd	%xmm11, %xmm13
	mulsd	%xmm5,  %xmm11
	movsd	-11 * SIZE(X1), %xmm5
	ADD1	%xmm10, %xmm2
	movsd	-14 * SIZE(A2), %xmm10
	mulsd	%xmm4,  %xmm13
	movsd	-12 * SIZE(X1), %xmm4
	ADD2	%xmm12, %xmm3

	movapd	%xmm8,  %xmm12
	mulsd	%xmm6,  %xmm8
	ADD3	%xmm11, %xmm2
	movsd	-13 * SIZE(A2), %xmm11

	mulsd	%xmm7,  %xmm12
	ADD4	%xmm13, %xmm3

	movapd	%xmm9,  %xmm13
	mulsd	%xmm7,  %xmm9
	ADD1	%xmm8,  %xmm0
	movsd	-12 * SIZE(A1), %xmm8
	mulsd	%xmm6,  %xmm13
	ADD2	%xmm12, %xmm1

	movapd	%xmm10, %xmm12
	mulsd	%xmm6,  %xmm10
	ADD3	%xmm9,  %xmm0
	movsd	-11 * SIZE(A1), %xmm9
	mulsd	%xmm7,  %xmm12
	ADD4	%xmm13, %xmm1

	movapd	%xmm11, %xmm13
	mulsd	%xmm7,  %xmm11
	movsd	 -9 * SIZE(X1), %xmm7
	ADD1	%xmm10, %xmm2
	movsd	-12 * SIZE(A2), %xmm10
	mulsd	%xmm6,  %xmm13
	movsd	-10 * SIZE(X1), %xmm6
	ADD2	%xmm12, %xmm3

	movapd	%xmm8,  %xmm12
	mulsd	%xmm4,  %xmm8
	ADD3	%xmm11, %xmm2
	movsd	-11 * SIZE(A2), %xmm11
	mulsd	%xmm5,  %xmm12
	ADD4	%xmm13, %xmm3

	movapd	%xmm9,  %xmm13
	mulsd	%xmm5,  %xmm9
	ADD1	%xmm8,  %xmm0
	movsd	-10 * SIZE(A1), %xmm8
	mulsd	%xmm4,  %xmm13
	ADD2	%xmm12, %xmm1

	movapd	%xmm10, %xmm12
	mulsd	%xmm4,  %xmm10
	ADD3	%xmm9,  %xmm0
	movsd	 -9 * SIZE(A1), %xmm9
	mulsd	%xmm5,  %xmm12
	ADD4	%xmm13, %xmm1

	movapd	%xmm11, %xmm13
	mulsd	%xmm5,  %xmm11
	movsd	 -7 * SIZE(X1), %xmm5
	ADD1	%xmm10, %xmm2
	movsd	-10 * SIZE(A2), %xmm10
	mulsd	%xmm4,  %xmm13
	movsd	 -8 * SIZE(X1), %xmm4
	ADD2	%xmm12, %xmm3

	movapd	%xmm8,  %xmm12
	mulsd	%xmm6,  %xmm8
	ADD3	%xmm11, %xmm2
	movsd	 -9 * SIZE(A2), %xmm11

	mulsd	%xmm7,  %xmm12
	ADD4	%xmm13, %xmm3

	movapd	%xmm9,  %xmm13
	mulsd	%xmm7,  %xmm9
	ADD1	%xmm8,  %xmm0
	mulsd	%xmm6,  %xmm13
	ADD2	%xmm12, %xmm1

	movapd	%xmm10, %xmm12
	mulsd	%xmm6,  %xmm10
	ADD3	%xmm9,  %xmm0
	mulsd	%xmm7,  %xmm12
	ADD4	%xmm13, %xmm1

	movapd	%xmm11, %xmm13
	mulsd	%xmm7,  %xmm11
	movsd	 -5 * SIZE(X1), %xmm7
	ADD1	%xmm10, %xmm2
	mulsd	%xmm6,  %xmm13
	movsd	 -6 * SIZE(X1), %xmm6
	ADD2	%xmm12, %xmm3

	ADD3	%xmm11, %xmm2
	ADD4	%xmm13, %xmm3

	subq	 $-8 * SIZE, A1
	subq	 $-8 * SIZE, A2
	subq	 $-8 * SIZE, X1
	ALIGN_3

.L15:
	testq	$2, M
	je	.L17

	movsd	-16 * SIZE(A1), %xmm8
	movsd	-15 * SIZE(A1), %xmm9
	movsd	-16 * SIZE(A2), %xmm10
	movsd	-15 * SIZE(A2), %xmm11

	movapd	%xmm8,  %xmm12
	mulsd	%xmm4,  %xmm8
	mulsd	%xmm5,  %xmm12

	movapd	%xmm9,  %xmm13
	mulsd	%xmm5,  %xmm9
	ADD1	%xmm8,  %xmm0
	movsd	-14 * SIZE(A1), %xmm8
	mulsd	%xmm4,  %xmm13
	ADD2	%xmm12, %xmm1

	movapd	%xmm10, %xmm12
	mulsd	%xmm4,  %xmm10
	ADD3	%xmm9,  %xmm0
	movsd	-13 * SIZE(A1), %xmm9
	mulsd	%xmm5,  %xmm12
	ADD4	%xmm13, %xmm1

	movapd	%xmm11, %xmm13
	mulsd	%xmm5,  %xmm11
	movsd	-11 * SIZE(X1), %xmm5
	ADD1	%xmm10, %xmm2
	movsd	-14 * SIZE(A2), %xmm10
	mulsd	%xmm4,  %xmm13
	movsd	-12 * SIZE(X1), %xmm4
	ADD2	%xmm12, %xmm3

	movapd	%xmm8,  %xmm12
	mulsd	%xmm6,  %xmm8
	ADD3	%xmm11, %xmm2
	movsd	-13 * SIZE(A2), %xmm11

	mulsd	%xmm7,  %xmm12
	ADD4	%xmm13, %xmm3

	movapd	%xmm9,  %xmm13
	mulsd	%xmm7,  %xmm9
	ADD1	%xmm8,  %xmm0
	mulsd	%xmm6,  %xmm13
	ADD2	%xmm12, %xmm1

	movapd	%xmm10, %xmm12
	mulsd	%xmm6,  %xmm10
	ADD3	%xmm9,  %xmm0
	mulsd	%xmm7,  %xmm12
	ADD4	%xmm13, %xmm1

	movapd	%xmm11, %xmm13
	mulsd	%xmm7,  %xmm11
	ADD1	%xmm10, %xmm2
	mulsd	%xmm6,  %xmm13
	ADD2	%xmm12, %xmm3

	ADD3	%xmm11, %xmm2
	ADD4	%xmm13, %xmm3

	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, A2
	ALIGN_3

.L17:
	testq	$1, M
	je	.L19

	movsd	-16 * SIZE(A1), %xmm8
	movsd	-15 * SIZE(A1), %xmm9
	movsd	-16 * SIZE(A2), %xmm10
	movsd	-15 * SIZE(A2), %xmm11

	movapd	%xmm8,  %xmm12
	mulsd	%xmm4,  %xmm8
	mulsd	%xmm5,  %xmm12

	movapd	%xmm9,  %xmm13
	mulsd	%xmm5,  %xmm9
	ADD1	%xmm8,  %xmm0
	mulsd	%xmm4,  %xmm13
	ADD2	%xmm12,  %xmm1

	movapd	%xmm10, %xmm12
	mulsd	%xmm4,  %xmm10
	ADD3	%xmm9,  %xmm0
	mulsd	%xmm5,  %xmm12
	ADD4	%xmm13,  %xmm1

	movapd	%xmm11, %xmm13
	mulsd	%xmm5,  %xmm11
	ADD1	%xmm10, %xmm2
	mulsd	%xmm4,  %xmm13
	ADD2	%xmm12,  %xmm3

	ADD3	%xmm11, %xmm2
	ADD4	%xmm13,  %xmm3
	ALIGN_3

.L19:
	movsd	 0 * SIZE(Y), %xmm4
	movapd	%xmm0, %xmm10
	mulsd	ALPHA_R, %xmm0
	movsd	 1 * SIZE(Y), %xmm5
	movapd	%xmm1, %xmm11
	mulsd	ALPHA_R, %xmm1
	addq	INCY, Y

	movsd	 0 * SIZE(Y), %xmm6
	movapd	%xmm2, %xmm12
	mulsd	ALPHA_R, %xmm2
	movsd	 1 * SIZE(Y), %xmm7
	movapd	%xmm3, %xmm13
	mulsd	ALPHA_R, %xmm3
	addq	INCY, Y

	mulsd	ALPHA_I, %xmm10
	mulsd	ALPHA_I, %xmm11
	mulsd	ALPHA_I, %xmm12
	mulsd	ALPHA_I, %xmm13

	addsd	%xmm10, %xmm1
	subsd	%xmm11, %xmm0
	addsd	%xmm12, %xmm3
	subsd	%xmm13, %xmm2

	addsd	%xmm4, %xmm0
	addsd	%xmm5, %xmm1
	addsd	%xmm6, %xmm2
	addsd	%xmm7, %xmm3

	movlpd	%xmm0,  0 * SIZE(Y1)
	movlpd	%xmm1,  1 * SIZE(Y1)
	addq	INCY, Y1
	movlpd	%xmm2,  0 * SIZE(Y1)
	movlpd	%xmm3,  1 * SIZE(Y1)
	addq	INCY, Y1

	decq	J
	jg	.L11
	ALIGN_3

.L20:
	testq	$1, N
	jle	.L999

	leaq	16 * SIZE(BUFFER), X1

	movq	A, A1

	xorpd	%xmm0, %xmm0
	xorpd	%xmm1, %xmm1

	movsd	-16 * SIZE(X1), %xmm4
	movsd	-15 * SIZE(X1), %xmm5
	movsd	-14 * SIZE(X1), %xmm6
	movsd	-13 * SIZE(X1), %xmm7

	movq	M,   I
	sarq	$2,  I
	jle	.L25

	movsd	-16 * SIZE(A1), %xmm8
	movsd	-15 * SIZE(A1), %xmm9

	movapd	%xmm8,  %xmm12
	mulsd	%xmm4,  %xmm8
	mulsd	%xmm5,  %xmm12

	decq	 I
	jle	 .L24
	ALIGN_3

.L23:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE + 0) * SIZE(A1)
#endif

	movapd	%xmm9,  %xmm13
	mulsd	%xmm5,  %xmm9
	movsd	-11 * SIZE(X1), %xmm5
	ADD1	%xmm8,  %xmm0
	movsd	-14 * SIZE(A1), %xmm8
	mulsd	%xmm4,  %xmm13
	movsd	-12 * SIZE(X1), %xmm4
	ADD2	%xmm12, %xmm1

	movapd	%xmm8,  %xmm12
	mulsd	%xmm6,  %xmm8
	ADD3	%xmm9,  %xmm0
 	movsd	-13 * SIZE(A1), %xmm9
	mulsd	%xmm7,  %xmm12
	ADD4	%xmm13, %xmm1

	movapd	%xmm9,  %xmm13
	mulsd	%xmm7,  %xmm9
	movsd	 -9 * SIZE(X1), %xmm7
	ADD1	%xmm8,  %xmm0
	movsd	-12 * SIZE(A1), %xmm8
	mulsd	%xmm6,  %xmm13
	movsd	-10 * SIZE(X1), %xmm6
	ADD2	%xmm12, %xmm1

	movapd	%xmm8,  %xmm12
	mulsd	%xmm4,  %xmm8
	ADD3	%xmm9,  %xmm0
	movsd	-11 * SIZE(A1), %xmm9
	mulsd	%xmm5,  %xmm12
	ADD4	%xmm13, %xmm1

	movapd	%xmm9,  %xmm13
	mulsd	%xmm5,  %xmm9
	movsd	 -7 * SIZE(X1), %xmm5
	ADD1	%xmm8,  %xmm0
	movsd	-10 * SIZE(A1), %xmm8
	mulsd	%xmm4,  %xmm13
	movsd	 -8 * SIZE(X1), %xmm4
	ADD2	%xmm12, %xmm1

	movapd	%xmm8,  %xmm12
	mulsd	%xmm6,  %xmm8
	ADD3	%xmm9,  %xmm0
 	movsd	 -9 * SIZE(A1), %xmm9
	mulsd	%xmm7,  %xmm12
	ADD4	%xmm13, %xmm1

	movapd	%xmm9,  %xmm13
	mulsd	%xmm7,  %xmm9
	movsd	 -5 * SIZE(X1), %xmm7
	ADD1	%xmm8,  %xmm0
	movsd	 -8 * SIZE(A1), %xmm8
	mulsd	%xmm6,  %xmm13
	movsd	 -6 * SIZE(X1), %xmm6
	ADD2	%xmm12, %xmm1

	movapd	%xmm8,  %xmm12
	mulsd	%xmm4,  %xmm8
	ADD3	%xmm9,  %xmm0
	mulsd	%xmm5,  %xmm12
	movsd	 -7 * SIZE(A1), %xmm9
	ADD4	%xmm13, %xmm1

	subq	 $-8 * SIZE, A1
	subq	 $-8 * SIZE, X1
	subq	 $-8 * SIZE, A2

	subq	 $1, I
	BRANCH
	jg	.L23
	ALIGN_3

.L24:
	movapd	%xmm9,  %xmm13
	mulsd	%xmm5,  %xmm9
	movsd	-11 * SIZE(X1), %xmm5
	ADD1	%xmm8,  %xmm0
	movsd	-14 * SIZE(A1), %xmm8
	mulsd	%xmm4,  %xmm13
	movsd	-12 * SIZE(X1), %xmm4
	ADD2	%xmm12, %xmm1

	movapd	%xmm8,  %xmm12
	mulsd	%xmm6,  %xmm8
	ADD3	%xmm9,  %xmm0
 	movsd	-13 * SIZE(A1), %xmm9
	mulsd	%xmm7,  %xmm12
	ADD4	%xmm13, %xmm1

	movapd	%xmm9,  %xmm13
	mulsd	%xmm7,  %xmm9
	movsd	 -9 * SIZE(X1), %xmm7
	ADD1	%xmm8,  %xmm0
	movsd	-12 * SIZE(A1), %xmm8
	mulsd	%xmm6,  %xmm13
	movsd	-10 * SIZE(X1), %xmm6
	ADD2	%xmm12, %xmm1

	movapd	%xmm8,  %xmm12
	mulsd	%xmm4,  %xmm8
	ADD3	%xmm9,  %xmm0
	movsd	-11 * SIZE(A1), %xmm9
	mulsd	%xmm5,  %xmm12
	ADD4	%xmm13, %xmm1

	movapd	%xmm9,  %xmm13
	mulsd	%xmm5,  %xmm9
	movsd	 -7 * SIZE(X1), %xmm5
	ADD1	%xmm8,  %xmm0
	movsd	-10 * SIZE(A1), %xmm8
	mulsd	%xmm4,  %xmm13
	movsd	 -8 * SIZE(X1), %xmm4
	ADD2	%xmm12, %xmm1

	movapd	%xmm8,  %xmm12
	mulsd	%xmm6,  %xmm8
	ADD3	%xmm9,  %xmm0
 	movsd	 -9 * SIZE(A1), %xmm9
	mulsd	%xmm7,  %xmm12
	ADD4	%xmm13, %xmm1

	movapd	%xmm9,  %xmm13
	mulsd	%xmm7,  %xmm9
	movsd	 -5 * SIZE(X1), %xmm7
	ADD1	%xmm8,  %xmm0
	mulsd	%xmm6,  %xmm13
	movsd	 -6 * SIZE(X1), %xmm6
	ADD2	%xmm12, %xmm1

	ADD3	%xmm9,  %xmm0
	ADD4	%xmm13, %xmm1

	subq	 $-8 * SIZE, A1
	subq	 $-8 * SIZE, A2
	subq	 $-8 * SIZE, X1
	ALIGN_3

.L25:
	testq	$2, M
	je	.L27

	movsd	-16 * SIZE(A1), %xmm8
	movsd	-15 * SIZE(A1), %xmm9

	movapd	%xmm8,  %xmm12
	mulsd	%xmm4,  %xmm8
	mulsd	%xmm5,  %xmm12

	movapd	%xmm9,  %xmm13
	mulsd	%xmm5,  %xmm9
	movsd	-11 * SIZE(X1), %xmm5
	ADD1	%xmm8,  %xmm0
	movsd	-14 * SIZE(A1), %xmm8
	mulsd	%xmm4,  %xmm13
	movsd	-12 * SIZE(X1), %xmm4
	ADD2	%xmm12, %xmm1

	movapd	%xmm8,  %xmm12
	mulsd	%xmm6,  %xmm8
	ADD3	%xmm9,  %xmm0
 	movsd	-13 * SIZE(A1), %xmm9
	mulsd	%xmm7,  %xmm12
	ADD4	%xmm13, %xmm1

	movapd	%xmm9,  %xmm13
	mulsd	%xmm7,  %xmm9
	ADD1	%xmm8,  %xmm0
	mulsd	%xmm6,  %xmm13
	ADD2	%xmm12, %xmm1

	ADD3	%xmm9,  %xmm0
	ADD4	%xmm13, %xmm1

	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, A2
	ALIGN_3

.L27:
	testq	$1, M
	je	.L29

	movsd	-16 * SIZE(A1), %xmm8
	movsd	-15 * SIZE(A1), %xmm9

	movapd	%xmm8,  %xmm12
	mulsd	%xmm4,  %xmm8
	mulsd	%xmm5,  %xmm12

	movapd	%xmm9,  %xmm13
	mulsd	%xmm5,  %xmm9
	ADD1	%xmm8,  %xmm0
	mulsd	%xmm4,  %xmm13
	ADD2	%xmm12,  %xmm1

	ADD3	%xmm9,  %xmm0
	ADD4	%xmm13, %xmm1
	ALIGN_3

.L29:
	movsd	 0 * SIZE(Y), %xmm4
	movapd	%xmm0, %xmm10
	mulsd	ALPHA_R, %xmm0
	movsd	 1 * SIZE(Y), %xmm5
	movapd	%xmm1, %xmm11
	mulsd	ALPHA_R, %xmm1

	mulsd	ALPHA_I, %xmm10
	mulsd	ALPHA_I, %xmm11

	addsd	%xmm10, %xmm1
	subsd	%xmm11, %xmm0
	addsd	%xmm4,  %xmm0
	addsd	%xmm5,  %xmm1

	movlpd	%xmm0,  0 * SIZE(Y1)
	movlpd	%xmm1,  1 * SIZE(Y1)
	ALIGN_3

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
