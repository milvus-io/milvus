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
#define J	%r11
#define A1	%r12
#define A2	%r13

#define Y1	%r14
#define BUFFER	%r15

#define ALPHA_R	%xmm14
#define ALPHA_I	%xmm15

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

	movapd	%xmm3,       %xmm0
	movsd	OLD_ALPHA_I, %xmm1
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
	movq	N,  J
	sarq	$1, J
	jle	.L20
	ALIGN_3

.L11:
	leaq	16 * SIZE(BUFFER), Y1
	movq	A,  A1
	leaq	(A,  LDA, 1), A2
	leaq	(A,  LDA, 2), A

	movsd	0 * SIZE(X), %xmm4
	movsd	1 * SIZE(X), %xmm5
	addq	INCX, X
	movsd	0 * SIZE(X), %xmm6
	movsd	1 * SIZE(X), %xmm7
	addq	INCX, X

	movapd	%xmm4,  %xmm8
	mulsd	ALPHA_R,  %xmm4
	mulsd	ALPHA_I,  %xmm8
	movapd	%xmm6, %xmm10
	mulsd	ALPHA_R,  %xmm6
	mulsd	ALPHA_I,  %xmm10

	movapd	%xmm5,  %xmm9
	mulsd	ALPHA_I,  %xmm9
	mulsd	ALPHA_R,  %xmm5
	movapd	%xmm7, %xmm11
	mulsd	ALPHA_I,  %xmm11
	mulsd	ALPHA_R,  %xmm7

#ifndef XCONJ
	subsd	%xmm9,  %xmm4
	addsd	%xmm8,  %xmm5
	subsd	%xmm11, %xmm6
	addsd	%xmm10, %xmm7
#else
	addsd	%xmm9,  %xmm4
	subsd	%xmm8,  %xmm5
	addsd	%xmm11, %xmm6
	subsd	%xmm10, %xmm7
#endif

	movsd	 -16 * SIZE(Y1), %xmm0
	movsd	 -15 * SIZE(Y1), %xmm1
	movsd	 -14 * SIZE(Y1), %xmm2
	movsd	 -13 * SIZE(Y1), %xmm3
	ALIGN_3

	movq	M,   I
	sarq	$2,  I
	jle	.L15

	movsd	 -16 * SIZE(A1), %xmm8
	movsd	 -15 * SIZE(A1), %xmm9
	movsd	 -14 * SIZE(A1), %xmm10
	movsd	 -13 * SIZE(A1), %xmm11

	movapd	%xmm8,  %xmm12
	mulsd	%xmm4,  %xmm8
	mulsd	%xmm5,  %xmm12

	movapd	%xmm10, %xmm13
	mulsd	%xmm4,  %xmm10
	ADD1	%xmm8,  %xmm0
	movsd	 -16 * SIZE(A2), %xmm8
	mulsd	%xmm5,  %xmm13
	ADD2	%xmm12, %xmm1

	decq	 I
	jle	 .L14
	ALIGN_3

.L13:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE + 0) * SIZE(A2)
#endif

	movapd	%xmm9,  %xmm12
	mulsd	%xmm5,  %xmm9
	ADD1	%xmm10, %xmm2
	movsd	 -14 * SIZE(A2), %xmm10
	mulsd	%xmm4,  %xmm12
	ADD2	%xmm13, %xmm3

	movapd	%xmm11, %xmm13
	mulsd	%xmm5,  %xmm11
	ADD3	%xmm9,  %xmm0
	movsd	 -15 * SIZE(A2), %xmm9
	mulsd	%xmm4,  %xmm13
	ADD4	%xmm12, %xmm1

	movapd	%xmm8, %xmm12
	mulsd	%xmm6,  %xmm8
	ADD3	%xmm11, %xmm2
	movsd	 -13 * SIZE(A2), %xmm11
	mulsd	%xmm7,  %xmm12
	ADD4	%xmm13, %xmm3

	movapd	%xmm10, %xmm13
	mulsd	%xmm6,  %xmm10
	ADD1	%xmm8, %xmm0
	movsd	 -12 * SIZE(A1), %xmm8
	mulsd	%xmm7,  %xmm13
	ADD2	%xmm12, %xmm1

	movapd	%xmm9, %xmm12
	mulsd	%xmm7,  %xmm9
	ADD1	%xmm10, %xmm2
	movsd	 -10 * SIZE(A1), %xmm10
	mulsd	%xmm6,  %xmm12
	ADD2	%xmm13, %xmm3

	movapd	%xmm11, %xmm13
	mulsd	%xmm7,  %xmm11
	ADD3	%xmm9,  %xmm0
	movsd	 -11 * SIZE(A1), %xmm9
	mulsd	%xmm6,  %xmm13
	ADD4	%xmm12, %xmm1

	movapd	%xmm8,  %xmm12
	movlpd	 %xmm0, -16 * SIZE(Y1)
	mulsd	%xmm4,  %xmm8
	movsd	 -12 * SIZE(Y1), %xmm0
	ADD3	%xmm11, %xmm2
	movsd	  -9 * SIZE(A1), %xmm11
	mulsd	%xmm5,  %xmm12
	movlpd	 %xmm1, -15 * SIZE(Y1)
	ADD4	%xmm13, %xmm3
	movsd	 -11 * SIZE(Y1), %xmm1

	movapd	%xmm10, %xmm13
	movlpd	 %xmm2, -14 * SIZE(Y1)
	mulsd	%xmm4,  %xmm10
	movlpd	 %xmm3, -13 * SIZE(Y1)
	ADD1	%xmm8,  %xmm0
	movsd	 -12 * SIZE(A2), %xmm8
	mulsd	%xmm5,  %xmm13
	movsd	 -10 * SIZE(Y1), %xmm2
	ADD2	%xmm12, %xmm1
	movsd	  -9 * SIZE(Y1), %xmm3

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE + 0) * SIZE(A1)
#endif

	movapd	%xmm9,  %xmm12
	mulsd	%xmm5,  %xmm9
	ADD1	%xmm10, %xmm2
	movsd	 -10 * SIZE(A2), %xmm10
	mulsd	%xmm4,  %xmm12
	ADD2	%xmm13, %xmm3

	movapd	%xmm11, %xmm13
	mulsd	%xmm5,  %xmm11
	ADD3	%xmm9,  %xmm0
	movsd	 -11 * SIZE(A2), %xmm9
	mulsd	%xmm4,  %xmm13
	ADD4	%xmm12, %xmm1

	movapd	%xmm8, %xmm12
	mulsd	%xmm6,  %xmm8
	ADD3	%xmm11, %xmm2
	movsd	  -9 * SIZE(A2), %xmm11
	mulsd	%xmm7,  %xmm12
	ADD4	%xmm13, %xmm3

	movapd	%xmm10, %xmm13
	mulsd	%xmm6,  %xmm10
	ADD1	%xmm8, %xmm0
	movsd	  -8 * SIZE(A1), %xmm8
	mulsd	%xmm7,  %xmm13
	ADD2	%xmm12, %xmm1

	movapd	%xmm9, %xmm12
	mulsd	%xmm7,  %xmm9
	ADD1	%xmm10, %xmm2
	movsd	  -6 * SIZE(A1), %xmm10
	mulsd	%xmm6,  %xmm12
	ADD2	%xmm13, %xmm3

	movapd	%xmm11, %xmm13
	mulsd	%xmm7,  %xmm11
	ADD3	%xmm9,  %xmm0
	movsd	  -7 * SIZE(A1), %xmm9
	mulsd	%xmm6,  %xmm13
	ADD4	%xmm12, %xmm1

	movapd	%xmm8,  %xmm12
	movlpd	 %xmm0, -12 * SIZE(Y1)
	mulsd	%xmm4,  %xmm8
	movsd	  -8 * SIZE(Y1), %xmm0
	ADD3	%xmm11, %xmm2
	movsd	  -5 * SIZE(A1), %xmm11
	mulsd	%xmm5,  %xmm12
	movlpd	 %xmm1, -11 * SIZE(Y1)
	ADD4	%xmm13, %xmm3
	movsd	  -7 * SIZE(Y1), %xmm1

	movapd	%xmm10, %xmm13
	movlpd	 %xmm2, -10 * SIZE(Y1)
	mulsd	%xmm4,  %xmm10
	movsd	  -6 * SIZE(Y1), %xmm2
	ADD1	%xmm8,  %xmm0
	movsd	  -8 * SIZE(A2), %xmm8
	mulsd	%xmm5,  %xmm13
	movlpd	 %xmm3,  -9 * SIZE(Y1)
	ADD2	%xmm12, %xmm1
	movsd	  -5 * SIZE(Y1), %xmm3

	subq	 $-8 * SIZE, A1
	subq	 $-8 * SIZE, A2
	subq	 $-8 * SIZE, Y1

	subq	 $1, I
	BRANCH
	jg	.L13
	ALIGN_3

.L14:
	movapd	%xmm9,  %xmm12
	mulsd	%xmm5,  %xmm9
	ADD1	%xmm10, %xmm2
	movsd	 -14 * SIZE(A2), %xmm10
	mulsd	%xmm4,  %xmm12
	ADD2	%xmm13, %xmm3

	movapd	%xmm11, %xmm13
	mulsd	%xmm5,  %xmm11
	ADD3	%xmm9,  %xmm0
	movsd	 -15 * SIZE(A2), %xmm9
	mulsd	%xmm4,  %xmm13
	ADD4	%xmm12, %xmm1

	movapd	%xmm8, %xmm12
	mulsd	%xmm6,  %xmm8
	ADD3	%xmm11, %xmm2
	movsd	 -13 * SIZE(A2), %xmm11
	mulsd	%xmm7,  %xmm12
	ADD4	%xmm13, %xmm3

	movapd	%xmm10, %xmm13
	mulsd	%xmm6,  %xmm10
	ADD1	%xmm8, %xmm0
	movsd	 -12 * SIZE(A1), %xmm8
	mulsd	%xmm7,  %xmm13
	ADD2	%xmm12, %xmm1

	movapd	%xmm9, %xmm12
	mulsd	%xmm7,  %xmm9
	ADD1	%xmm10, %xmm2
	movsd	 -10 * SIZE(A1), %xmm10
	mulsd	%xmm6,  %xmm12
	ADD2	%xmm13, %xmm3

	movapd	%xmm11, %xmm13
	mulsd	%xmm7,  %xmm11
	ADD3	%xmm9,  %xmm0
	movsd	 -11 * SIZE(A1), %xmm9
	mulsd	%xmm6,  %xmm13
	ADD4	%xmm12, %xmm1

	movapd	%xmm8,  %xmm12
	movlpd	 %xmm0, -16 * SIZE(Y1)
	mulsd	%xmm4,  %xmm8
	movsd	 -12 * SIZE(Y1), %xmm0
	ADD3	%xmm11, %xmm2
	movsd	  -9 * SIZE(A1), %xmm11
	mulsd	%xmm5,  %xmm12
	movlpd	 %xmm1, -15 * SIZE(Y1)
	ADD4	%xmm13, %xmm3
	movsd	 -11 * SIZE(Y1), %xmm1

	movapd	%xmm10, %xmm13
	movlpd	 %xmm2, -14 * SIZE(Y1)
	mulsd	%xmm4,  %xmm10
	movlpd	 %xmm3, -13 * SIZE(Y1)
	ADD1	%xmm8,  %xmm0
	movsd	 -12 * SIZE(A2), %xmm8
	mulsd	%xmm5,  %xmm13
	movsd	 -10 * SIZE(Y1), %xmm2
	ADD2	%xmm12, %xmm1
	movsd	  -9 * SIZE(Y1), %xmm3

	movapd	%xmm9,  %xmm12
	mulsd	%xmm5,  %xmm9
	ADD1	%xmm10, %xmm2
	movsd	 -10 * SIZE(A2), %xmm10
	mulsd	%xmm4,  %xmm12
	ADD2	%xmm13, %xmm3

	movapd	%xmm11, %xmm13
	mulsd	%xmm5,  %xmm11
	ADD3	%xmm9,  %xmm0
	movsd	 -11 * SIZE(A2), %xmm9
	mulsd	%xmm4,  %xmm13
	ADD4	%xmm12, %xmm1

	movapd	%xmm8, %xmm12
	mulsd	%xmm6,  %xmm8
	ADD3	%xmm11, %xmm2
	movsd	  -9 * SIZE(A2), %xmm11
	mulsd	%xmm7,  %xmm12
	ADD4	%xmm13, %xmm3

	movapd	%xmm10, %xmm13
	mulsd	%xmm6,  %xmm10
	ADD1	%xmm8, %xmm0
	mulsd	%xmm7,  %xmm13
	ADD2	%xmm12, %xmm1

	movapd	%xmm9, %xmm12
	mulsd	%xmm7,  %xmm9
	ADD1	%xmm10, %xmm2
	mulsd	%xmm6,  %xmm12
	ADD2	%xmm13, %xmm3

	movapd	%xmm11, %xmm13
	mulsd	%xmm7,  %xmm11
	ADD3	%xmm9,  %xmm0
	mulsd	%xmm6,  %xmm13
	ADD4	%xmm12, %xmm1

	ADD3	%xmm11, %xmm2
	movlpd	 %xmm0, -12 * SIZE(Y1)
	movsd	  -8 * SIZE(Y1), %xmm0
	ADD4	%xmm13, %xmm3
	movlpd	 %xmm1, -11 * SIZE(Y1)
	movsd	  -7 * SIZE(Y1), %xmm1

	movlpd	 %xmm2, -10 * SIZE(Y1)
	movsd	  -6 * SIZE(Y1), %xmm2
	movlpd	 %xmm3,  -9 * SIZE(Y1)
	movsd	  -5 * SIZE(Y1), %xmm3

	subq	 $-8 * SIZE, A1
	subq	 $-8 * SIZE, A2
	subq	 $-8 * SIZE, Y1
	ALIGN_3

.L15:
	testq	$2, M
	je	.L17

	movsd	 -16 * SIZE(A1), %xmm8
	movsd	 -15 * SIZE(A1), %xmm9
	movsd	 -14 * SIZE(A1), %xmm10
	movsd	 -13 * SIZE(A1), %xmm11

	movapd	%xmm8,  %xmm12
	mulsd	%xmm4,  %xmm8
	mulsd	%xmm5,  %xmm12

	movapd	%xmm10, %xmm13
	mulsd	%xmm4,  %xmm10
	ADD1	%xmm8,  %xmm0
	movsd	 -16 * SIZE(A2), %xmm8
	mulsd	%xmm5,  %xmm13
	ADD2	%xmm12, %xmm1

	movapd	%xmm9,  %xmm12
	mulsd	%xmm5,  %xmm9
	ADD1	%xmm10, %xmm2
	movsd	 -14 * SIZE(A2), %xmm10
	mulsd	%xmm4,  %xmm12
	ADD2	%xmm13, %xmm3

	movapd	%xmm11, %xmm13
	mulsd	%xmm5,  %xmm11
	ADD3	%xmm9,  %xmm0
	movsd	 -15 * SIZE(A2), %xmm9
	mulsd	%xmm4,  %xmm13
	ADD4	%xmm12, %xmm1

	movapd	%xmm8, %xmm12
	mulsd	%xmm6,  %xmm8
	ADD3	%xmm11, %xmm2
	movsd	 -13 * SIZE(A2), %xmm11
	mulsd	%xmm7,  %xmm12
	ADD4	%xmm13, %xmm3

	movapd	%xmm10, %xmm13
	mulsd	%xmm6,  %xmm10
	ADD1	%xmm8, %xmm0
	mulsd	%xmm7,  %xmm13
	ADD2	%xmm12, %xmm1

	movapd	%xmm9, %xmm12
	mulsd	%xmm7,  %xmm9
	ADD1	%xmm10, %xmm2
	mulsd	%xmm6,  %xmm12
	ADD2	%xmm13, %xmm3

	movapd	%xmm11, %xmm13
	mulsd	%xmm7,  %xmm11
	ADD3	%xmm9,  %xmm0
	mulsd	%xmm6,  %xmm13
	ADD4	%xmm12, %xmm1

	ADD3	%xmm11, %xmm2
	ADD4	%xmm13, %xmm3

	movlpd	 %xmm0, -16 * SIZE(Y1)
	movlpd	 %xmm1, -15 * SIZE(Y1)
	movsd	 -12 * SIZE(Y1), %xmm0
	movsd	 -11 * SIZE(Y1), %xmm1

	movlpd	 %xmm2, -14 * SIZE(Y1)
	movlpd	 %xmm3, -13 * SIZE(Y1)


	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, A2
	addq	 $4 * SIZE, Y1
	ALIGN_3

.L17:
	testq	$1, M
	je	.L19

	movsd	 -16 * SIZE(A1), %xmm8
	movsd	 -15 * SIZE(A1), %xmm9
	movsd	 -16 * SIZE(A2), %xmm10
	movsd	 -15 * SIZE(A2), %xmm11

	movapd	%xmm8,  %xmm12
	mulsd	%xmm4,  %xmm8
	mulsd	%xmm5,  %xmm12

	movapd	%xmm9,  %xmm13
	mulsd	%xmm5,  %xmm9
	ADD1	%xmm8,  %xmm0
	mulsd	%xmm4,  %xmm13
	ADD2	%xmm12, %xmm1

	movapd	%xmm10, %xmm12
	mulsd	%xmm6,  %xmm10
	ADD3	%xmm9,  %xmm0
	mulsd	%xmm7,  %xmm12
	ADD4	%xmm13, %xmm1

	movapd	%xmm11, %xmm13
	mulsd	%xmm7,  %xmm11
	ADD1	%xmm10, %xmm0
	mulsd	%xmm6,  %xmm13
	ADD2	%xmm12, %xmm1

	ADD3	%xmm11, %xmm0
	ADD4	%xmm13, %xmm1

	movlpd	 %xmm0, -16 * SIZE(Y1)
	movlpd	 %xmm1, -15 * SIZE(Y1)
	ALIGN_3

.L19:
	decq	J
	jg	.L11
	ALIGN_3

.L20:
	testq	$1, N
	jle	.L90

	leaq	16 * SIZE(BUFFER), Y1
	movq	A,  A1

	movsd	0 * SIZE(X), %xmm4
	movsd	1 * SIZE(X), %xmm5

	movapd	%xmm4,  %xmm8
	mulsd	ALPHA_R,  %xmm4
	mulsd	ALPHA_I,  %xmm8
	movapd	%xmm5,  %xmm9
	mulsd	ALPHA_I,  %xmm9
	mulsd	ALPHA_R,  %xmm5

#ifndef XCONJ
	subsd	%xmm9,  %xmm4
	addsd	%xmm8,  %xmm5
#else
	addsd	%xmm9,  %xmm4
	subsd	%xmm8,  %xmm5
#endif

	movsd	 -16 * SIZE(Y1), %xmm0
	movsd	 -15 * SIZE(Y1), %xmm1
	movsd	 -14 * SIZE(Y1), %xmm2
	movsd	 -13 * SIZE(Y1), %xmm3
	ALIGN_3

	movq	M,   I
	sarq	$2,  I
	jle	.L25

	movsd	 -16 * SIZE(A1), %xmm8
	movsd	 -15 * SIZE(A1), %xmm9
	movsd	 -14 * SIZE(A1), %xmm10
	movsd	 -13 * SIZE(A1), %xmm11

	movapd	%xmm8,  %xmm12
	mulsd	%xmm4,  %xmm8
	mulsd	%xmm5,  %xmm12

	movapd	%xmm10, %xmm13
	mulsd	%xmm4,  %xmm10
	ADD1	%xmm8,  %xmm0
	movsd	 -12 * SIZE(A1), %xmm8
	mulsd	%xmm5,  %xmm13
	ADD2	%xmm12, %xmm1

	decq	 I
	jle	 .L24
	ALIGN_3

.L23:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE + 0) * SIZE(A2)
#endif

	movapd	%xmm9,  %xmm12
	mulsd	%xmm5,  %xmm9
	ADD1	%xmm10, %xmm2
	movsd	 -10 * SIZE(A1), %xmm10
	mulsd	%xmm4,  %xmm12
	ADD2	%xmm13, %xmm3

	movapd	%xmm11,  %xmm13
	mulsd	%xmm5,  %xmm11
	ADD3	%xmm9,  %xmm0
	movsd	 -11 * SIZE(A1), %xmm9
	mulsd	%xmm4,  %xmm13
	ADD4	%xmm12, %xmm1

	movapd	%xmm8,  %xmm12
	movlpd	 %xmm0, -16 * SIZE(Y1)
	mulsd	%xmm4,  %xmm8
	movsd	 -12 * SIZE(Y1), %xmm0
	ADD3	%xmm11, %xmm2
	movsd	  -9 * SIZE(A1), %xmm11

	mulsd	%xmm5,  %xmm12
	movlpd	 %xmm1, -15 * SIZE(Y1)
	ADD4	%xmm13, %xmm3
	movsd	 -11 * SIZE(Y1), %xmm1

	movapd	%xmm10, %xmm13
	movlpd	 %xmm2, -14 * SIZE(Y1)
	mulsd	%xmm4,  %xmm10
	movsd	 -10 * SIZE(Y1), %xmm2
	ADD1	%xmm8,  %xmm0
	movsd	  -8 * SIZE(A1), %xmm8
	mulsd	%xmm5,  %xmm13
	movlpd	 %xmm3, -13 * SIZE(Y1)
	ADD2	%xmm12, %xmm1
	movsd	  -9 * SIZE(Y1), %xmm3

	movapd	%xmm9,  %xmm12
	mulsd	%xmm5,  %xmm9
	ADD1	%xmm10, %xmm2
	movsd	  -6 * SIZE(A1), %xmm10
	mulsd	%xmm4,  %xmm12
	ADD2	%xmm13, %xmm3

	movapd	%xmm11,  %xmm13
	mulsd	%xmm5,  %xmm11
	ADD3	%xmm9,  %xmm0
	movsd	  -7 * SIZE(A1), %xmm9
	mulsd	%xmm4,  %xmm13
	subq	 $-8 * SIZE, A1
	ADD4	%xmm12, %xmm1

	movapd	%xmm8,  %xmm12
	movlpd	 %xmm0, -12 * SIZE(Y1)
	mulsd	%xmm4,  %xmm8
	movsd	  -8 * SIZE(Y1), %xmm0
	ADD3	%xmm11, %xmm2
	movsd	 -13 * SIZE(A1), %xmm11
	mulsd	%xmm5,  %xmm12
	movlpd	 %xmm1, -11 * SIZE(Y1)
	ADD4	%xmm13, %xmm3
	movsd	  -7 * SIZE(Y1), %xmm1

	movapd	%xmm10, %xmm13
	movlpd	 %xmm2, -10 * SIZE(Y1)
	mulsd	%xmm4,  %xmm10
	movsd	  -6 * SIZE(Y1), %xmm2
	ADD1	%xmm8,  %xmm0
	movsd	 -12 * SIZE(A1), %xmm8
	mulsd	%xmm5,  %xmm13
	movlpd	 %xmm3,  -9 * SIZE(Y1)
	ADD2	%xmm12, %xmm1
	movsd	  -5 * SIZE(Y1), %xmm3

	subq	 $-8 * SIZE, Y1

	subq	 $1, I
	BRANCH
	jg	.L23
	ALIGN_3

.L24:
	movapd	%xmm9,  %xmm12
	mulsd	%xmm5,  %xmm9
	ADD1	%xmm10, %xmm2
	movsd	 -10 * SIZE(A1), %xmm10
	mulsd	%xmm4,  %xmm12
	ADD2	%xmm13, %xmm3

	movapd	%xmm11,  %xmm13
	mulsd	%xmm5,  %xmm11
	ADD3	%xmm9,  %xmm0
	movsd	 -11 * SIZE(A1), %xmm9
	mulsd	%xmm4,  %xmm13
	ADD4	%xmm12, %xmm1

	movapd	%xmm8,  %xmm12
	movlpd	 %xmm0, -16 * SIZE(Y1)
	mulsd	%xmm4,  %xmm8
	movsd	 -12 * SIZE(Y1), %xmm0
	ADD3	%xmm11, %xmm2
	movsd	  -9 * SIZE(A1), %xmm11

	mulsd	%xmm5,  %xmm12
	movlpd	 %xmm1, -15 * SIZE(Y1)
	ADD4	%xmm13, %xmm3
	movsd	 -11 * SIZE(Y1), %xmm1

	movapd	%xmm10, %xmm13
	mulsd	%xmm4,  %xmm10
	movlpd	 %xmm2, -14 * SIZE(Y1)
	ADD1	%xmm8,  %xmm0
	movsd	 -10 * SIZE(Y1), %xmm2
	mulsd	%xmm5,  %xmm13
	movlpd	 %xmm3, -13 * SIZE(Y1)
	ADD2	%xmm12, %xmm1
	movsd	  -9 * SIZE(Y1), %xmm3

	movapd	%xmm9,  %xmm12
	mulsd	%xmm5,  %xmm9
	ADD1	%xmm10, %xmm2
	mulsd	%xmm4,  %xmm12
	ADD2	%xmm13, %xmm3

	movapd	%xmm11,  %xmm13
	mulsd	%xmm5,  %xmm11
	ADD3	%xmm9,  %xmm0
	mulsd	%xmm4,  %xmm13
	ADD4	%xmm12, %xmm1

	ADD3	%xmm11, %xmm2
	movlpd	 %xmm0, -12 * SIZE(Y1)
	movsd	  -8 * SIZE(Y1), %xmm0
	ADD4	%xmm13, %xmm3
	movlpd	 %xmm1, -11 * SIZE(Y1)
	movsd	  -7 * SIZE(Y1), %xmm1

	movlpd	 %xmm2, -10 * SIZE(Y1)
	movlpd	 %xmm3,  -9 * SIZE(Y1)
	movsd	  -6 * SIZE(Y1), %xmm2
	movsd	  -5 * SIZE(Y1), %xmm3

	subq	 $-8 * SIZE, A1
	subq	 $-8 * SIZE, Y1
	ALIGN_3

.L25:
	testq	$2, M
	je	.L27

	movsd	 -16 * SIZE(A1), %xmm8
	movsd	 -15 * SIZE(A1), %xmm9
	movsd	 -14 * SIZE(A1), %xmm10
	movsd	 -13 * SIZE(A1), %xmm11

	movapd	%xmm8,  %xmm12
	mulsd	%xmm4,  %xmm8
	mulsd	%xmm5,  %xmm12

	movapd	%xmm10, %xmm13
	mulsd	%xmm4,  %xmm10
	ADD1	%xmm8,  %xmm0
	mulsd	%xmm5,  %xmm13
	ADD2	%xmm12, %xmm1

	movapd	%xmm9,  %xmm12
	mulsd	%xmm5,  %xmm9
	ADD1	%xmm10, %xmm2
	mulsd	%xmm4,  %xmm12
	ADD2	%xmm13, %xmm3

	movapd	%xmm11,  %xmm13
	mulsd	%xmm5,  %xmm11
	ADD3	%xmm9,  %xmm0
	mulsd	%xmm4,  %xmm13
	ADD4	%xmm12, %xmm1

	ADD3	%xmm11, %xmm2
	movlpd	 %xmm0, -16 * SIZE(Y1)
	movsd	 -12 * SIZE(Y1), %xmm0
	ADD4	%xmm13, %xmm3
	movlpd	 %xmm1, -15 * SIZE(Y1)
	movsd	 -11 * SIZE(Y1), %xmm1

	movlpd	 %xmm2, -14 * SIZE(Y1)
	movlpd	 %xmm3, -13 * SIZE(Y1)

	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, Y1
	ALIGN_3

.L27:
	testq	$1, M
	je	.L90

	movsd	 -16 * SIZE(A1), %xmm8
	movsd	 -15 * SIZE(A1), %xmm9

	movapd	%xmm8,  %xmm12
	mulsd	%xmm4,  %xmm8
	mulsd	%xmm5,  %xmm12

	movapd	%xmm9,  %xmm13
	mulsd	%xmm5,  %xmm9
	ADD1	%xmm8,  %xmm0
	mulsd	%xmm4,  %xmm13
	ADD2	%xmm12, %xmm1

	ADD3	%xmm9,  %xmm0
	ADD4	%xmm13, %xmm1

	movlpd	 %xmm0, -16 * SIZE(Y1)
	movlpd	 %xmm1, -15 * SIZE(Y1)
	ALIGN_3

.L90:
	movq	Y,  Y1

	movq	M,  %rax
	sarq	$3, %rax
	jle	.L94
	ALIGN_3

.L92:
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
	jg	.L92
	ALIGN_3

.L94:
	testq	$7, M
	jle	.L999

	testq	$4, M
	jle	.L95

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

.L95:
	testq	$2, M
	jle	.L96

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

.L96:
	testq	$1, M
	jle	.L999

 	movsd	 0 * SIZE(Y), %xmm0
	movhpd	 1 * SIZE(Y), %xmm0

	addpd	 0 * SIZE(BUFFER), %xmm0

	movlpd	%xmm0,  0 * SIZE(Y1)
	movhpd	%xmm0,  1 * SIZE(Y1)
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
