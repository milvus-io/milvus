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

#define OLD_M	  %rdi
#define OLD_N	  %rsi
#define OLD_A	  %rcx
#define OLD_LDA	  %r8
#define STACK_INCX	 8 + STACKSIZE(%rsp)
#define STACK_Y		16 + STACKSIZE(%rsp)
#define STACK_INCY	24 + STACKSIZE(%rsp)
#define STACK_BUFFER	32 + STACKSIZE(%rsp)
#define ALPHA	48	      (%rsp)

#define MMM	56(%rsp)
#define NN	64(%rsp)
#define AA	72(%rsp)
#define LDAX	80(%rsp)
#define XX	88(%rsp)
#else

#define STACKSIZE	288

#define OLD_M	  %rcx
#define OLD_N	  %rdx
#define OLD_A		 40 + STACKSIZE(%rsp)
#define OLD_LDA		 48 + STACKSIZE(%rsp)
#define OLD_X		 56 + STACKSIZE(%rsp)
#define STACK_INCX	 64 + STACKSIZE(%rsp)
#define STACK_Y		 72 + STACKSIZE(%rsp)
#define STACK_INCY	 80 + STACKSIZE(%rsp)
#define STACK_BUFFER	 88 + STACKSIZE(%rsp)
#define ALPHA	224	       (%rsp)

#define MMM	232(%rsp)
#define NN	240(%rsp)
#define AA	248(%rsp)
#define LDAX 256(%rsp)
#define XX	264(%rsp)

#endif

#define LDA	%r8
#define X	%r9

#define INCX	%rsi
#define INCY	%rdi

#define M	%r10
#define N	%r11
#define A	%r12
#define Y	%r14
#define BUFFER	%r13

#define I	%rax
#define A1	%rbx
#define A2	%rcx
#define LDA3	%rdx
#define Y1	%rbp

#ifdef ALIGNED_ACCESS
#define MM	%r15
#else
#define MM	M
#endif

#define TMP_M   %r15
#define Y2      %rbx

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

	movq	OLD_M,	      M
	movq	OLD_N,        N
	movq	OLD_A,        A
	movq	OLD_LDA,      LDA
	movq	OLD_X,        X
#else
	movq	OLD_M,	      M
	movq	OLD_N,        N
	movq	OLD_A,        A
	movq	OLD_LDA,      LDA
#endif

#ifndef WINDOWS_ABI
	movsd	 %xmm0, ALPHA
#else
	movsd	 %xmm3, ALPHA
#endif

	movq	STACK_Y,      Y
	movq	A,AA
	movq	N,NN
	movq	M,MMM
	movq	LDA,LDAX
	movq	X,XX

.L0t:
	xorq	I,I
	addq	$1,I
	salq	$21,I
	subq	I,MMM
	movq	I,M
	jge	.L00t

	movq	MMM,M
	addq	M, I
	jle	.L999x
	movq	I, M

.L00t:
	movq	XX,X
	movq	AA,A
	movq	NN,N
	movq	LDAX,LDA

	movq	STACK_INCX,   INCX
	movq	STACK_INCY,   INCY
	movq	STACK_BUFFER, BUFFER


	leaq	-1(INCY), %rax

	leaq	(,INCX, SIZE), INCX
	leaq	(,INCY, SIZE), INCY
	leaq	(,LDA,  SIZE), LDA

	leaq	(LDA, LDA, 2), LDA3

	subq	$-16 * SIZE, A

#ifdef ALIGNED_ACCESS
	leaq	-1 (M), MM
	testq	$SIZE, A
	cmoveq	M, MM
#endif

	testq	N, N		# if n <= 0 goto END
	jle	.L999
	testq	M, M		# if n <= 0 goto END
	jle	.L999

#if !defined(COPY_FORCE) && !defined(ALIGNED_ACCESS)
#ifndef  NOCOPY_UNALIGNED
	movq	Y,    Y1
	andq	$0xf, Y1
	orq	Y1, %rax
#endif
	testq	%rax, %rax
 	cmoveq	Y, BUFFER
	je	.L10
#endif

	movq	BUFFER, Y1

	pxor	%xmm4, %xmm4

	movq	M,   %rax
	addq	$16, %rax
	sarq	$4,  %rax
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
	leaq	SIZE(BUFFER), %rax
	testq	$SIZE, A
	cmovne	%rax, BUFFER

	testq	$SIZE, LDA
	jne	.L50
#endif

#if GEMV_UNROLL >= 8

	cmpq	$8, N
	jl	.L20
	ALIGN_3

.L11:
	subq	$8, N

	leaq	16 * SIZE(BUFFER), Y1
	movq	A,  A1
	leaq	(A,  LDA, 4), A2
	leaq	(A,  LDA, 8), A

#ifdef HAVE_SSE3
	movddup	(X), %xmm8
	addq	INCX, X
	movddup	(X), %xmm9
	addq	INCX, X
	movddup	(X), %xmm10
	addq	INCX, X
	movddup	(X), %xmm11
	addq	INCX, X
	movddup	(X), %xmm12
	addq	INCX, X
	movddup	(X), %xmm13
	addq	INCX, X
	movddup	(X), %xmm14
	addq	INCX, X
	movddup	(X), %xmm15
	addq	INCX, X

	movddup	ALPHA, %xmm0
#else
	movsd	(X), %xmm8
	unpcklpd %xmm8, %xmm8
	addq	INCX, X
	movsd	(X), %xmm9
	unpcklpd %xmm9, %xmm9
	addq	INCX, X
	movsd	(X), %xmm10
	unpcklpd %xmm10, %xmm10
	addq	INCX, X
	movsd	(X), %xmm11
	unpcklpd %xmm11, %xmm11
	addq	INCX, X
	movsd	(X), %xmm12
	unpcklpd %xmm12, %xmm12
	addq	INCX, X
	movsd	(X), %xmm13
	unpcklpd %xmm13, %xmm13
	addq	INCX, X
	movsd	(X), %xmm14
	unpcklpd %xmm14, %xmm14
	addq	INCX, X
	movsd	(X), %xmm15
	unpcklpd %xmm15, %xmm15
	addq	INCX, X

	movsd	ALPHA, %xmm0
	unpcklpd %xmm0, %xmm0
#endif

	mulpd	%xmm0, %xmm8
	mulpd	%xmm0, %xmm9
	mulpd	%xmm0, %xmm10
	mulpd	%xmm0, %xmm11
	mulpd	%xmm0, %xmm12
	mulpd	%xmm0, %xmm13
	mulpd	%xmm0, %xmm14
	mulpd	%xmm0, %xmm15

#ifdef ALIGNED_ACCESS
	testq	$SIZE, A
	je	.L1X

	movsd	 -16 * SIZE(A1),         %xmm4
	movsd	 -16 * SIZE(A1, LDA),    %xmm5
	movsd	 -16 * SIZE(A1, LDA, 2), %xmm6
	movsd	 -16 * SIZE(A1, LDA3),   %xmm7

	movsd	 -16 * SIZE(Y1),         %xmm0

	mulsd	 %xmm8,  %xmm4
	addsd	 %xmm4,  %xmm0
	movsd	 -16 * SIZE(A2),         %xmm4
	mulsd	 %xmm9,  %xmm5
	addsd	 %xmm5,  %xmm0
	movsd	 -16 * SIZE(A2, LDA),    %xmm5
	mulsd	 %xmm10, %xmm6
	addsd	 %xmm6,  %xmm0
	movsd	 -16 * SIZE(A2, LDA, 2), %xmm6
	mulsd	 %xmm11, %xmm7
	addsd	 %xmm7,  %xmm0
	movsd	 -16 * SIZE(A2, LDA3),   %xmm7

	mulsd	 %xmm12, %xmm4
	addsd	 %xmm4,  %xmm0
	mulsd	 %xmm13, %xmm5
	addsd	 %xmm5,  %xmm0
	mulsd	 %xmm14, %xmm6
	addsd	 %xmm6,  %xmm0
	mulsd	 %xmm15, %xmm7
	addsd	 %xmm7,  %xmm0

	movsd	 %xmm0, -16 * SIZE(Y1)

	addq	 $SIZE, A1
	addq	 $SIZE, A2
	addq	 $SIZE, Y1
	ALIGN_3

.L1X:
#endif

	movq	MM,  I
	sarq	$3,  I
	jle	.L15

	MOVUPS_A1(-16 * SIZE, A1, %xmm4)
	MOVUPS_A1(-14 * SIZE, A1, %xmm5)
	MOVUPS_A1(-12 * SIZE, A1, %xmm6)
	MOVUPS_A1(-10 * SIZE, A1, %xmm7)

	MOVUPS_YL1(-16 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-14 * SIZE, Y1, %xmm1)
	MOVUPS_YL1(-12 * SIZE, Y1, %xmm2)
	MOVUPS_YL1(-10 * SIZE, Y1, %xmm3)

	decq	 I
	jle	 .L14
	ALIGN_3

.L13:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A1)
#endif

	mulpd	 %xmm8,  %xmm4
	addpd	 %xmm4,  %xmm0
	MOVUPS_A2(-16 * SIZE, A1, LDA, 1, %xmm4)
	mulpd	 %xmm8,  %xmm5
	addpd	 %xmm5,  %xmm1
	MOVUPS_A2(-14 * SIZE, A1, LDA, 1, %xmm5)

	mulpd	 %xmm8,  %xmm6
	addpd	 %xmm6,  %xmm2
	MOVUPS_A2(-12 * SIZE, A1, LDA, 1, %xmm6)
	mulpd	 %xmm8,  %xmm7
	addpd	 %xmm7,  %xmm3
	MOVUPS_A2(-10 * SIZE, A1, LDA, 1, %xmm7)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A1, LDA, 1)
#endif

	mulpd	 %xmm9,  %xmm4
	addpd	 %xmm4,  %xmm0
	MOVUPS_A2(-16 * SIZE, A1, LDA, 2, %xmm4)
	mulpd	 %xmm9,  %xmm5
	addpd	 %xmm5,  %xmm1
	MOVUPS_A2(-14 * SIZE, A1, LDA, 2, %xmm5)

	mulpd	 %xmm9,  %xmm6
	addpd	 %xmm6,  %xmm2
	MOVUPS_A2(-12 * SIZE, A1, LDA, 2, %xmm6)
	mulpd	 %xmm9,  %xmm7
	addpd	 %xmm7,  %xmm3
	MOVUPS_A2(-10 * SIZE, A1, LDA, 2, %xmm7)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A1, LDA, 2)
#endif

	mulpd	 %xmm10, %xmm4
	addpd	 %xmm4,  %xmm0
	MOVUPS_A2(-16 * SIZE, A1, LDA3, 1, %xmm4)
	mulpd	 %xmm10, %xmm5
	addpd	 %xmm5,  %xmm1
	MOVUPS_A2(-14 * SIZE, A1, LDA3, 1, %xmm5)

	mulpd	 %xmm10, %xmm6
	addpd	 %xmm6,  %xmm2
	MOVUPS_A2(-12 * SIZE, A1, LDA3, 1, %xmm6)
	mulpd	 %xmm10, %xmm7
	addpd	 %xmm7,  %xmm3
	MOVUPS_A2(-10 * SIZE, A1, LDA3, 1, %xmm7)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A1, LDA3)
#endif

	mulpd	 %xmm11, %xmm4
	addpd	 %xmm4,  %xmm0
	MOVUPS_A1(-16 * SIZE, A2, %xmm4)
	mulpd	 %xmm11, %xmm5
	addpd	 %xmm5,  %xmm1
	MOVUPS_A1(-14 * SIZE, A2, %xmm5)

	mulpd	 %xmm11, %xmm6
	addpd	 %xmm6,  %xmm2
	MOVUPS_A1(-12 * SIZE, A2, %xmm6)
	mulpd	 %xmm11, %xmm7
	addpd	 %xmm7,  %xmm3
	MOVUPS_A1(-10 * SIZE, A2, %xmm7)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A2)
#endif

	mulpd	 %xmm12, %xmm4
	addpd	 %xmm4,  %xmm0
	MOVUPS_A2(-16 * SIZE, A2, LDA, 1, %xmm4)
	mulpd	 %xmm12, %xmm5
	addpd	 %xmm5,  %xmm1
	MOVUPS_A2(-14 * SIZE, A2, LDA, 1, %xmm5)

	mulpd	 %xmm12, %xmm6
	addpd	 %xmm6,  %xmm2
	MOVUPS_A2(-12 * SIZE, A2, LDA, 1, %xmm6)
	mulpd	 %xmm12, %xmm7
	addpd	 %xmm7,  %xmm3
	MOVUPS_A2(-10 * SIZE, A2, LDA, 1, %xmm7)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A2, LDA, 1)
#endif

	mulpd	 %xmm13, %xmm4
	addpd	 %xmm4,  %xmm0
	MOVUPS_A2(-16 * SIZE, A2, LDA, 2, %xmm4)
	mulpd	 %xmm13, %xmm5
	addpd	 %xmm5,  %xmm1
	MOVUPS_A2(-14 * SIZE, A2, LDA, 2, %xmm5)

	mulpd	 %xmm13, %xmm6
	addpd	 %xmm6,  %xmm2
	MOVUPS_A2(-12 * SIZE, A2, LDA, 2, %xmm6)
	mulpd	 %xmm13, %xmm7
	addpd	 %xmm7,  %xmm3
	MOVUPS_A2(-10 * SIZE, A2, LDA, 2, %xmm7)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A2, LDA, 2)
#endif

	mulpd	 %xmm14, %xmm4
	addpd	 %xmm4,  %xmm0
	MOVUPS_A2(-16 * SIZE, A2, LDA3, 1, %xmm4)
	mulpd	 %xmm14, %xmm5
	addpd	 %xmm5,  %xmm1
	MOVUPS_A2(-14 * SIZE, A2, LDA3, 1, %xmm5)

	mulpd	 %xmm14, %xmm6
	addpd	 %xmm6,  %xmm2
	MOVUPS_A2(-12 * SIZE, A2, LDA3, 1, %xmm6)
	mulpd	 %xmm14, %xmm7
	addpd	 %xmm7,  %xmm3
	MOVUPS_A2(-10 * SIZE, A2, LDA3, 1, %xmm7)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A2, LDA3)
#endif

	mulpd	 %xmm15, %xmm4
	addpd	 %xmm4,  %xmm0
	MOVUPS_A1( -8 * SIZE, A1, %xmm4)
	mulpd	 %xmm15, %xmm5
	addpd	 %xmm5,  %xmm1
	MOVUPS_A1( -6 * SIZE, A1, %xmm5)

	mulpd	 %xmm15, %xmm6
	addpd	 %xmm6,  %xmm2
	MOVUPS_A1( -4 * SIZE, A1, %xmm6)
	mulpd	 %xmm15, %xmm7
	addpd	 %xmm7,  %xmm3
	MOVUPS_A1( -2 * SIZE, A1, %xmm7)

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
	mulpd	 %xmm8,  %xmm4
	addpd	 %xmm4,  %xmm0
	MOVUPS_A2(-16 * SIZE, A1, LDA, 1, %xmm4)
	mulpd	 %xmm8,  %xmm5
	addpd	 %xmm5,  %xmm1
	MOVUPS_A2(-14 * SIZE, A1, LDA, 1, %xmm5)

	mulpd	 %xmm8,  %xmm6
	addpd	 %xmm6,  %xmm2
	MOVUPS_A2(-12 * SIZE, A1, LDA, 1, %xmm6)
	mulpd	 %xmm8,  %xmm7
	addpd	 %xmm7,  %xmm3
	MOVUPS_A2(-10 * SIZE, A1, LDA, 1, %xmm7)

	mulpd	 %xmm9,  %xmm4
	addpd	 %xmm4,  %xmm0
	MOVUPS_A2(-16 * SIZE, A1, LDA, 2, %xmm4)
	mulpd	 %xmm9,  %xmm5
	addpd	 %xmm5,  %xmm1
	MOVUPS_A2(-14 * SIZE, A1, LDA, 2, %xmm5)

	mulpd	 %xmm9,  %xmm6
	addpd	 %xmm6,  %xmm2
	MOVUPS_A2(-12 * SIZE, A1, LDA, 2, %xmm6)
	mulpd	 %xmm9,  %xmm7
	addpd	 %xmm7,  %xmm3
	MOVUPS_A2(-10 * SIZE, A1, LDA, 2, %xmm7)

	mulpd	 %xmm10, %xmm4
	addpd	 %xmm4,  %xmm0
	MOVUPS_A2(-16 * SIZE, A1, LDA3, 1, %xmm4)
	mulpd	 %xmm10, %xmm5
	addpd	 %xmm5,  %xmm1
	MOVUPS_A2(-14 * SIZE, A1, LDA3, 1, %xmm5)

	mulpd	 %xmm10, %xmm6
	addpd	 %xmm6,  %xmm2
	MOVUPS_A2(-12 * SIZE, A1, LDA3, 1, %xmm6)
	mulpd	 %xmm10, %xmm7
	addpd	 %xmm7,  %xmm3
	MOVUPS_A2(-10 * SIZE, A1, LDA3, 1, %xmm7)

	mulpd	 %xmm11, %xmm4
	addpd	 %xmm4,  %xmm0
	MOVUPS_A1(-16 * SIZE, A2, %xmm4)
	mulpd	 %xmm11, %xmm5
	addpd	 %xmm5,  %xmm1
	MOVUPS_A1(-14 * SIZE, A2, %xmm5)

	mulpd	 %xmm11, %xmm6
	addpd	 %xmm6,  %xmm2
	MOVUPS_A1(-12 * SIZE, A2, %xmm6)
	mulpd	 %xmm11, %xmm7
	addpd	 %xmm7,  %xmm3
	MOVUPS_A1(-10 * SIZE, A2, %xmm7)

	mulpd	 %xmm12, %xmm4
	addpd	 %xmm4,  %xmm0
	MOVUPS_A2(-16 * SIZE, A2, LDA, 1, %xmm4)
	mulpd	 %xmm12, %xmm5
	addpd	 %xmm5,  %xmm1
	MOVUPS_A2(-14 * SIZE, A2, LDA, 1, %xmm5)

	mulpd	 %xmm12, %xmm6
	addpd	 %xmm6,  %xmm2
	MOVUPS_A2(-12 * SIZE, A2, LDA, 1, %xmm6)
	mulpd	 %xmm12, %xmm7
	addpd	 %xmm7,  %xmm3
	MOVUPS_A2(-10 * SIZE, A2, LDA, 1, %xmm7)

	mulpd	 %xmm13, %xmm4
	addpd	 %xmm4,  %xmm0
	MOVUPS_A2(-16 * SIZE, A2, LDA, 2, %xmm4)
	mulpd	 %xmm13, %xmm5
	addpd	 %xmm5,  %xmm1
	MOVUPS_A2(-14 * SIZE, A2, LDA, 2, %xmm5)

	mulpd	 %xmm13, %xmm6
	addpd	 %xmm6,  %xmm2
	MOVUPS_A2(-12 * SIZE, A2, LDA, 2, %xmm6)
	mulpd	 %xmm13, %xmm7
	addpd	 %xmm7,  %xmm3
	MOVUPS_A2(-10 * SIZE, A2, LDA, 2, %xmm7)

	mulpd	 %xmm14, %xmm4
	addpd	 %xmm4,  %xmm0
	MOVUPS_A2(-16 * SIZE, A2, LDA3, 1, %xmm4)
	mulpd	 %xmm14, %xmm5
	addpd	 %xmm5,  %xmm1
	MOVUPS_A2(-14 * SIZE, A2, LDA3, 1, %xmm5)

	mulpd	 %xmm14, %xmm6
	addpd	 %xmm6,  %xmm2
	MOVUPS_A2(-12 * SIZE, A2, LDA3, 1, %xmm6)
	mulpd	 %xmm14, %xmm7
	addpd	 %xmm7,  %xmm3
	MOVUPS_A2(-10 * SIZE, A2, LDA3, 1, %xmm7)

	mulpd	 %xmm15, %xmm4
	addpd	 %xmm4,  %xmm0
	MOVUPS_YS1(-16 * SIZE, Y1, %xmm0)
	mulpd	 %xmm15, %xmm5
	addpd	 %xmm5,  %xmm1
	MOVUPS_YS1(-14 * SIZE, Y1, %xmm1)

	mulpd	 %xmm15, %xmm6
	addpd	 %xmm6,  %xmm2
	MOVUPS_YS1(-12 * SIZE, Y1, %xmm2)
	mulpd	 %xmm15, %xmm7
	addpd	 %xmm7,  %xmm3
	MOVUPS_YS1(-10 * SIZE, Y1, %xmm3)

	subq	 $-8 * SIZE, A1
	subq	 $-8 * SIZE, A2
	subq	 $-8 * SIZE, Y1
	ALIGN_3

.L15:
	testq	$4, MM
	je	.L16

	MOVUPS_A1(-16 * SIZE, A1, %xmm4)
	MOVUPS_A1(-14 * SIZE, A1, %xmm5)
	MOVUPS_A2(-16 * SIZE, A1, LDA, 1, %xmm6)
	MOVUPS_A2(-14 * SIZE, A1, LDA, 1, %xmm7)

	MOVUPS_YL1(-16 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-14 * SIZE, Y1, %xmm1)

	mulpd	 %xmm8,  %xmm4
	addpd	 %xmm4,  %xmm0
	MOVUPS_A2(-16 * SIZE, A1, LDA, 2, %xmm4)
	mulpd	 %xmm8,  %xmm5
	addpd	 %xmm5,  %xmm1
	MOVUPS_A2(-14 * SIZE, A1, LDA, 2, %xmm5)

	mulpd	 %xmm9,  %xmm6
	addpd	 %xmm6,  %xmm0
	MOVUPS_A2(-16 * SIZE, A1, LDA3, 1, %xmm6)
	mulpd	 %xmm9,  %xmm7
	addpd	 %xmm7,  %xmm1
	MOVUPS_A2(-14 * SIZE, A1, LDA3, 1, %xmm7)

	mulpd	 %xmm10, %xmm4
	addpd	 %xmm4,  %xmm0
	MOVUPS_A1(-16 * SIZE, A2, %xmm4)
	mulpd	 %xmm10, %xmm5
	addpd	 %xmm5,  %xmm1
	MOVUPS_A1(-14 * SIZE, A2, %xmm5)

	mulpd	 %xmm11, %xmm6
	addpd	 %xmm6,  %xmm0
	MOVUPS_A2(-16 * SIZE, A2, LDA, 1, %xmm6)
	mulpd	 %xmm11, %xmm7
	addpd	 %xmm7,  %xmm1
	MOVUPS_A2(-14 * SIZE, A2, LDA, 1, %xmm7)

	mulpd	 %xmm12, %xmm4
	addpd	 %xmm4,  %xmm0
	MOVUPS_A2(-16 * SIZE, A2, LDA, 2, %xmm4)
	mulpd	 %xmm12, %xmm5
	addpd	 %xmm5,  %xmm1
	MOVUPS_A2(-14 * SIZE, A2, LDA, 2, %xmm5)

	mulpd	 %xmm13, %xmm6
	addpd	 %xmm6,  %xmm0
	MOVUPS_A2(-16 * SIZE, A2, LDA3, 1, %xmm6)
	mulpd	 %xmm13, %xmm7
	addpd	 %xmm7,  %xmm1
	MOVUPS_A2(-14 * SIZE, A2, LDA3, 1, %xmm7)

	mulpd	 %xmm14, %xmm4
	addpd	 %xmm4,  %xmm0
	mulpd	 %xmm14, %xmm5
	addpd	 %xmm5,  %xmm1

	mulpd	 %xmm15, %xmm6
	addpd	 %xmm6,  %xmm0
	MOVUPS_YS1(-16 * SIZE, Y1, %xmm0)
	mulpd	 %xmm15, %xmm7
	addpd	 %xmm7,  %xmm1
	MOVUPS_YS1(-14 * SIZE, Y1, %xmm1)

	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, A2
	addq	 $4 * SIZE, Y1
	ALIGN_3

.L16:
	testq	$2, MM
	je	.L17

	MOVUPS_A1(-16 * SIZE, A1, %xmm4)
	MOVUPS_A2(-16 * SIZE, A1, LDA,  1, %xmm5)
	MOVUPS_A2(-16 * SIZE, A1, LDA,  2, %xmm6)
	MOVUPS_A2(-16 * SIZE, A1, LDA3, 1, %xmm7)

	MOVUPS_YL1(-16 * SIZE, Y1, %xmm0)

	mulpd	 %xmm8,  %xmm4
	addpd	 %xmm4,  %xmm0
	MOVUPS_A1(-16 * SIZE, A2, %xmm4)
	mulpd	 %xmm9,  %xmm5
	addpd	 %xmm5,  %xmm0
	MOVUPS_A2(-16 * SIZE, A2, LDA,  1, %xmm5)

	mulpd	 %xmm10, %xmm6
	addpd	 %xmm6,  %xmm0
	MOVUPS_A2(-16 * SIZE, A2, LDA,  2, %xmm6)
	mulpd	 %xmm11, %xmm7
	addpd	 %xmm7,  %xmm0
	MOVUPS_A2(-16 * SIZE, A2, LDA3, 1, %xmm7)

	mulpd	 %xmm12, %xmm4
	addpd	 %xmm4,  %xmm0
	mulpd	 %xmm13, %xmm5
	addpd	 %xmm5,  %xmm0
	mulpd	 %xmm14, %xmm6
	addpd	 %xmm6,  %xmm0
	mulpd	 %xmm15, %xmm7
	addpd	 %xmm7,  %xmm0

	MOVUPS_YS1(-16 * SIZE, Y1, %xmm0)

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, Y1
	ALIGN_3

.L17:
	testq	$1, MM
	je	.L18

	movsd	 -16 * SIZE(A1),         %xmm4
	movsd	 -16 * SIZE(A1, LDA),    %xmm5
	movsd	 -16 * SIZE(A1, LDA, 2), %xmm6
	movsd	 -16 * SIZE(A1, LDA3),   %xmm7

	movsd	 -16 * SIZE(Y1),         %xmm0

	mulsd	 %xmm8,  %xmm4
	addsd	 %xmm4,  %xmm0
	movsd	 -16 * SIZE(A2),         %xmm4
	mulsd	 %xmm9,  %xmm5
	addsd	 %xmm5,  %xmm0
	movsd	 -16 * SIZE(A2, LDA),    %xmm5
	mulsd	 %xmm10, %xmm6
	addsd	 %xmm6,  %xmm0
	movsd	 -16 * SIZE(A2, LDA, 2), %xmm6
	mulsd	 %xmm11, %xmm7
	addsd	 %xmm7,  %xmm0
	movsd	 -16 * SIZE(A2, LDA3),   %xmm7

	mulsd	 %xmm12, %xmm4
	addsd	 %xmm4,  %xmm0
	mulsd	 %xmm13, %xmm5
	addsd	 %xmm5,  %xmm0
	mulsd	 %xmm14, %xmm6
	addsd	 %xmm6,  %xmm0
	mulsd	 %xmm15, %xmm7
	addsd	 %xmm7,  %xmm0

	movsd	 %xmm0, -16 * SIZE(Y1)
	ALIGN_3

.L18:
	cmpq	$8, N
	jge	.L11
	ALIGN_3

.L20:
#endif

#if GEMV_UNROLL >= 4

	cmpq	$4, N
	jl	.L30

#if GEMV_UNROLL == 4
	ALIGN_3

.L21:
#endif

	subq	$4, N

	leaq	16 * SIZE(BUFFER), Y1
	movq	A,  A1
	leaq	(A,  LDA, 2), A2
	leaq	(A,  LDA, 4), A

#ifdef HAVE_SSE3
	movddup	(X), %xmm12
	addq	INCX, X
	movddup	(X), %xmm13
	addq	INCX, X
	movddup	(X), %xmm14
	addq	INCX, X
	movddup	(X), %xmm15
	addq	INCX, X

	movddup	ALPHA, %xmm0
#else
	movsd	(X), %xmm12
	unpcklpd %xmm12, %xmm12
	addq	INCX, X
	movsd	(X), %xmm13
	unpcklpd %xmm13, %xmm13
	addq	INCX, X
	movsd	(X), %xmm14
	unpcklpd %xmm14, %xmm14
	addq	INCX, X
	movsd	(X), %xmm15
	unpcklpd %xmm15, %xmm15
	addq	INCX, X

	movsd	ALPHA, %xmm0
	unpcklpd %xmm0, %xmm0
#endif

	mulpd	%xmm0, %xmm12
	mulpd	%xmm0, %xmm13
	mulpd	%xmm0, %xmm14
	mulpd	%xmm0, %xmm15

#ifdef ALIGNED_ACCESS
	testq	$SIZE, A
	je	.L2X

	movsd	 -16 * SIZE(A1),         %xmm4
	movsd	 -16 * SIZE(A1, LDA),    %xmm5
	movsd	 -16 * SIZE(A2),         %xmm6
	movsd	 -16 * SIZE(A2, LDA),    %xmm7

	movsd	 -16 * SIZE(Y1),         %xmm0

	mulsd	 %xmm12, %xmm4
	addsd	 %xmm4,  %xmm0
	mulsd	 %xmm13, %xmm5
	addsd	 %xmm5,  %xmm0
	mulsd	 %xmm14, %xmm6
	addsd	 %xmm6,  %xmm0
	mulsd	 %xmm15, %xmm7
	addsd	 %xmm7,  %xmm0

	movsd	 %xmm0, -16 * SIZE(Y1)

	addq	 $SIZE, A1
	addq	 $SIZE, A2
	addq	 $SIZE, Y1
	ALIGN_3

.L2X:
#endif

	movq	MM,  I
	sarq	$3,  I
	jle	.L25

	MOVUPS_A1(-16 * SIZE, A1, %xmm0)
	MOVUPS_A1(-14 * SIZE, A1, %xmm1)
	MOVUPS_A1(-12 * SIZE, A1, %xmm2)
	MOVUPS_A1(-10 * SIZE, A1, %xmm3)

	MOVUPS_YL1(-16 * SIZE, Y1, %xmm8)
	MOVUPS_YL1(-14 * SIZE, Y1, %xmm9)
	MOVUPS_YL1(-12 * SIZE, Y1, %xmm10)
	MOVUPS_YL1(-10 * SIZE, Y1, %xmm11)

	MOVUPS_A2(-16 * SIZE, A1, LDA, 1, %xmm4)
	MOVUPS_A2(-14 * SIZE, A1, LDA, 1, %xmm5)
	MOVUPS_A2(-12 * SIZE, A1, LDA, 1, %xmm6)
	MOVUPS_A2(-10 * SIZE, A1, LDA, 1, %xmm7)

	decq	 I
	jle	 .L24
	ALIGN_3

.L23:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2  - 128 + PREOFFSET(A1)
#endif

	mulpd	 %xmm12, %xmm0
	addpd	 %xmm0,  %xmm8
	MOVUPS_A1(-16 * SIZE, A2, %xmm0)
	mulpd	 %xmm12, %xmm1
	addpd	 %xmm1,  %xmm9
	MOVUPS_A1(-14 * SIZE, A2, %xmm1)

	mulpd	 %xmm12, %xmm2
	addpd	 %xmm2,  %xmm10
	MOVUPS_A1(-12 * SIZE, A2, %xmm2)
	mulpd	 %xmm12, %xmm3
	addpd	 %xmm3,  %xmm11
	MOVUPS_A1(-10 * SIZE, A2, %xmm3)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2  - 128 + PREOFFSET(A1, LDA)
#endif

	mulpd	 %xmm13, %xmm4
	addpd	 %xmm4,  %xmm8
	MOVUPS_A2(-16 * SIZE, A2, LDA, 1, %xmm4)
	mulpd	 %xmm13, %xmm5
	addpd	 %xmm5,  %xmm9
	MOVUPS_A2(-14 * SIZE, A2, LDA, 1, %xmm5)

	mulpd	 %xmm13, %xmm6
	addpd	 %xmm6,  %xmm10
	MOVUPS_A2(-12 * SIZE, A2, LDA, 1, %xmm6)
	mulpd	 %xmm13, %xmm7
	addpd	 %xmm7,  %xmm11
	MOVUPS_A2(-10 * SIZE, A2, LDA, 1, %xmm7)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2  - 128 + PREOFFSET(A2)
#endif

	mulpd	 %xmm14, %xmm0
	addpd	 %xmm0,  %xmm8
	MOVUPS_A1( -8 * SIZE, A1, %xmm0)
	mulpd	 %xmm14, %xmm1
	addpd	 %xmm1,  %xmm9
	MOVUPS_A1( -6 * SIZE, A1, %xmm1)

	mulpd	 %xmm14, %xmm2
	addpd	 %xmm2,  %xmm10
	MOVUPS_A1( -4 * SIZE, A1, %xmm2)
	mulpd	 %xmm14, %xmm3
	addpd	 %xmm3,  %xmm11
	MOVUPS_A1( -2 * SIZE, A1, %xmm3)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2  - 128 + PREOFFSET(A2, LDA)
#endif

	mulpd	 %xmm15, %xmm4
	addpd	 %xmm4,  %xmm8
	MOVUPS_A2( -8 * SIZE, A1, LDA, 1, %xmm4)
	mulpd	 %xmm15, %xmm5
	addpd	 %xmm5,  %xmm9
	MOVUPS_A2( -6 * SIZE, A1, LDA, 1, %xmm5)

	mulpd	 %xmm15, %xmm6
	addpd	 %xmm6,  %xmm10
	MOVUPS_A2( -4 * SIZE, A1, LDA, 1, %xmm6)
	mulpd	 %xmm15, %xmm7
	addpd	 %xmm7,  %xmm11
	MOVUPS_A2( -2 * SIZE, A1, LDA, 1, %xmm7)

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE) * 2  - 128 + PREOFFSET(Y1)
#endif

	MOVUPS_YS1(-16 * SIZE, Y1, %xmm8)
	MOVUPS_YS1(-14 * SIZE, Y1, %xmm9)
	MOVUPS_YS1(-12 * SIZE, Y1, %xmm10)
	MOVUPS_YS1(-10 * SIZE, Y1, %xmm11)

	MOVUPS_YL1( -8 * SIZE, Y1, %xmm8)
	MOVUPS_YL1( -6 * SIZE, Y1, %xmm9)
	MOVUPS_YL1( -4 * SIZE, Y1, %xmm10)
	MOVUPS_YL1( -2 * SIZE, Y1, %xmm11)

	subq	 $-8 * SIZE, A1
	subq	 $-8 * SIZE, A2
	subq	 $-8 * SIZE, Y1

	subq	 $1, I
	BRANCH
	jg	.L23
	ALIGN_3

.L24:
	mulpd	 %xmm12, %xmm0
	addpd	 %xmm0,  %xmm8
	MOVUPS_A1(-16 * SIZE, A2, %xmm0)
	mulpd	 %xmm12, %xmm1
	addpd	 %xmm1,  %xmm9
	MOVUPS_A1(-14 * SIZE, A2, %xmm1)

	mulpd	 %xmm12, %xmm2
	addpd	 %xmm2,  %xmm10
	MOVUPS_A1(-12 * SIZE, A2, %xmm2)
	mulpd	 %xmm12, %xmm3
	addpd	 %xmm3,  %xmm11
	MOVUPS_A1(-10 * SIZE, A2, %xmm3)

	mulpd	 %xmm13, %xmm4
	addpd	 %xmm4,  %xmm8
	MOVUPS_A2(-16 * SIZE, A2, LDA, 1, %xmm4)
	mulpd	 %xmm13, %xmm5
	addpd	 %xmm5,  %xmm9
	MOVUPS_A2(-14 * SIZE, A2, LDA, 1, %xmm5)

	mulpd	 %xmm13, %xmm6
	addpd	 %xmm6,  %xmm10
	MOVUPS_A2(-12 * SIZE, A2, LDA, 1, %xmm6)
	mulpd	 %xmm13, %xmm7
	addpd	 %xmm7,  %xmm11
	MOVUPS_A2(-10 * SIZE, A2, LDA, 1, %xmm7)

	mulpd	 %xmm14, %xmm0
	addpd	 %xmm0,  %xmm8
	mulpd	 %xmm14, %xmm1
	addpd	 %xmm1,  %xmm9

	mulpd	 %xmm14, %xmm2
	addpd	 %xmm2,  %xmm10
	mulpd	 %xmm14, %xmm3
	addpd	 %xmm3,  %xmm11

	mulpd	 %xmm15, %xmm4
	addpd	 %xmm4,  %xmm8
	MOVUPS_YS1(-16 * SIZE, Y1, %xmm8)
	mulpd	 %xmm15, %xmm5
	addpd	 %xmm5,  %xmm9
	MOVUPS_YS1(-14 * SIZE, Y1, %xmm9)

	mulpd	 %xmm15, %xmm6
	addpd	 %xmm6,  %xmm10
	MOVUPS_YS1(-12 * SIZE, Y1, %xmm10)
	mulpd	 %xmm15, %xmm7
	addpd	 %xmm7,  %xmm11
	MOVUPS_YS1(-10 * SIZE, Y1, %xmm11)

	subq	 $-8 * SIZE, A1
	subq	 $-8 * SIZE, A2
	subq	 $-8 * SIZE, Y1
	ALIGN_3

.L25:
	testq	$4, MM
	je	.L26

	MOVUPS_A1(-16 * SIZE, A1, %xmm0)
	MOVUPS_A1(-14 * SIZE, A1, %xmm1)

	MOVUPS_YL1(-16 * SIZE, Y1, %xmm8)
	MOVUPS_YL1(-14 * SIZE, Y1, %xmm9)

	mulpd	 %xmm12, %xmm0
	addpd	 %xmm0,  %xmm8
	mulpd	 %xmm12, %xmm1
	addpd	 %xmm1,  %xmm9

	MOVUPS_A2(-16 * SIZE, A1, LDA, 1, %xmm4)
	MOVUPS_A2(-14 * SIZE, A1, LDA, 1, %xmm5)

	mulpd	 %xmm13, %xmm4
	addpd	 %xmm4,  %xmm8
	mulpd	 %xmm13, %xmm5
	addpd	 %xmm5,  %xmm9

	MOVUPS_A1(-16 * SIZE, A2, %xmm0)
	MOVUPS_A1(-14 * SIZE, A2, %xmm1)

	mulpd	 %xmm14, %xmm0
	addpd	 %xmm0,  %xmm8
	mulpd	 %xmm14, %xmm1
	addpd	 %xmm1,  %xmm9

	MOVUPS_A2(-16 * SIZE, A2, LDA, 1, %xmm4)
	MOVUPS_A2(-14 * SIZE, A2, LDA, 1, %xmm5)

	mulpd	 %xmm15, %xmm4
	addpd	 %xmm4,  %xmm8
	mulpd	 %xmm15, %xmm5
	addpd	 %xmm5,  %xmm9

	MOVUPS_YS1(-16 * SIZE, Y1, %xmm8)
	MOVUPS_YS1(-14 * SIZE, Y1, %xmm9)

	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, A2
	addq	 $4 * SIZE, Y1
	ALIGN_3

.L26:
	testq	$2, MM
	je	.L27

	MOVUPS_A1(-16 * SIZE, A1, %xmm8)
	MOVUPS_A2(-16 * SIZE, A1, LDA, 1, %xmm9)
	MOVUPS_A1(-16 * SIZE, A2, %xmm10)
	MOVUPS_A2(-16 * SIZE, A2, LDA, 1, %xmm11)

	MOVUPS_YL1(-16 * SIZE, Y1, %xmm0)

	mulpd	 %xmm12, %xmm8
	addpd	 %xmm8,  %xmm0
	mulpd	 %xmm13, %xmm9
	addpd	 %xmm9,  %xmm0
	mulpd	 %xmm14, %xmm10
	addpd	 %xmm10, %xmm0
	mulpd	 %xmm15, %xmm11
	addpd	 %xmm11, %xmm0

	MOVUPS_YS1(-16 * SIZE, Y1, %xmm0)

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, Y1
	ALIGN_3

.L27:
	testq	$1, MM
#if GEMV_UNROLL == 4
	je	.L28
#else
	je	.L30
#endif

	movsd	 -16 * SIZE(Y1), %xmm0

	movsd	 -16 * SIZE(A1), %xmm8
	movsd	 -16 * SIZE(A1, LDA), %xmm9
	movsd	 -16 * SIZE(A2), %xmm10
	movsd	 -16 * SIZE(A2, LDA), %xmm11

	mulsd	 %xmm12, %xmm8
	addsd	 %xmm8,  %xmm0
	mulsd	 %xmm13, %xmm9
	addsd	 %xmm9,  %xmm0
	mulsd	 %xmm14, %xmm10
	addsd	 %xmm10, %xmm0
	mulsd	 %xmm15, %xmm11
	addsd	 %xmm11, %xmm0

	movsd	 %xmm0, -16 * SIZE(Y1)
	ALIGN_3

#if GEMV_UNROLL == 4
.L28:
	cmpq	$4, N
	jge	.L21
	ALIGN_3

#endif

.L30:
#endif

#if GEMV_UNROLL >= 2

	cmpq	$2, N
	jl	.L40

#if GEMV_UNROLL == 2
	ALIGN_3

.L31:
#endif

	subq	$2, N

	leaq	16 * SIZE(BUFFER), Y1
	movq	A,  A1
	leaq	(A,  LDA), A2
	leaq	(A,  LDA, 2), A

#ifdef HAVE_SSE3
	movddup	(X), %xmm12
	addq	INCX, X
	movddup	(X), %xmm13
	addq	INCX, X

	movddup	ALPHA, %xmm0
#else
	movsd	(X), %xmm12
	unpcklpd %xmm12, %xmm12
	addq	INCX, X
	movsd	(X), %xmm13
	unpcklpd %xmm13, %xmm13
	addq	INCX, X

	movsd	ALPHA, %xmm0
	unpcklpd %xmm0, %xmm0
#endif

	mulpd	%xmm0, %xmm12
	mulpd	%xmm0, %xmm13

#ifdef ALIGNED_ACCESS
	testq	$SIZE, A
	je	.L3X

	movsd	 -16 * SIZE(A1), %xmm4
	movsd	 -16 * SIZE(A2), %xmm5

	movsd	 -16 * SIZE(Y1), %xmm0

	mulsd	 %xmm12, %xmm4
	addsd	 %xmm4,  %xmm0
	mulsd	 %xmm13, %xmm5
	addsd	 %xmm5,  %xmm0

	movsd	 %xmm0, -16 * SIZE(Y1)

	addq	 $SIZE, A1
	addq	 $SIZE, A2
	addq	 $SIZE, Y1
	ALIGN_3

.L3X:
#endif

	movq	MM,  I
	sarq	$3,  I
	jle	.L35

	MOVUPS_A1(-16 * SIZE, A1, %xmm0)
	MOVUPS_A1(-14 * SIZE, A1, %xmm1)
	MOVUPS_A1(-12 * SIZE, A1, %xmm2)
	MOVUPS_A1(-10 * SIZE, A1, %xmm3)

	MOVUPS_YL1(-16 * SIZE, Y1, %xmm8)
	MOVUPS_YL1(-14 * SIZE, Y1, %xmm9)
	MOVUPS_YL1(-12 * SIZE, Y1, %xmm10)
	MOVUPS_YL1(-10 * SIZE, Y1, %xmm11)

	MOVUPS_A1(-16 * SIZE, A2, %xmm4)
	MOVUPS_A1(-14 * SIZE, A2, %xmm5)
	MOVUPS_A1(-12 * SIZE, A2, %xmm6)
	MOVUPS_A1(-10 * SIZE, A2, %xmm7)

	decq	 I
	jle	 .L34
	ALIGN_3

.L33:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 4  - 128 + PREOFFSET(A1)
#endif

	mulpd	 %xmm12, %xmm0
	addpd	 %xmm0,  %xmm8
	MOVUPS_A1( -8 * SIZE, A1, %xmm0)
	mulpd	 %xmm12, %xmm1
	addpd	 %xmm1,  %xmm9
	MOVUPS_A1( -6 * SIZE, A1, %xmm1)

	mulpd	 %xmm12, %xmm2
	addpd	 %xmm2,  %xmm10
	MOVUPS_A1( -4 * SIZE, A1, %xmm2)
	mulpd	 %xmm12, %xmm3
	addpd	 %xmm3,  %xmm11
	MOVUPS_A1( -2 * SIZE, A1, %xmm3)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 4  - 128 + PREOFFSET(A2)
#endif

	mulpd	 %xmm13, %xmm4
	addpd	 %xmm4,  %xmm8
	MOVUPS_A1( -8 * SIZE, A2, %xmm4)
	mulpd	 %xmm13, %xmm5
	addpd	 %xmm5,  %xmm9
	MOVUPS_A1( -6 * SIZE, A2, %xmm5)

	mulpd	 %xmm13, %xmm6
	addpd	 %xmm6,  %xmm10
	MOVUPS_A1( -4 * SIZE, A2, %xmm6)
	mulpd	 %xmm13, %xmm7
	addpd	 %xmm7,  %xmm11
	MOVUPS_A1( -2 * SIZE, A2, %xmm7)

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE) * 4  - 128 + PREOFFSET(Y1)
#endif

	MOVUPS_YS1(-16 * SIZE, Y1, %xmm8)
	MOVUPS_YS1(-14 * SIZE, Y1, %xmm9)
	MOVUPS_YS1(-12 * SIZE, Y1, %xmm10)
	MOVUPS_YS1(-10 * SIZE, Y1, %xmm11)

	MOVUPS_YL1( -8 * SIZE, Y1, %xmm8)
	MOVUPS_YL1( -6 * SIZE, Y1, %xmm9)
	MOVUPS_YL1( -4 * SIZE, Y1, %xmm10)
	MOVUPS_YL1( -2 * SIZE, Y1, %xmm11)

	subq	 $-8 * SIZE, A1
	subq	 $-8 * SIZE, A2
	subq	 $-8 * SIZE, Y1

	subq	 $1, I
	BRANCH
	jg	.L33
	ALIGN_3

.L34:
	mulpd	 %xmm12, %xmm0
	addpd	 %xmm0,  %xmm8
	mulpd	 %xmm12, %xmm1
	addpd	 %xmm1,  %xmm9
	mulpd	 %xmm12, %xmm2
	addpd	 %xmm2,  %xmm10
	mulpd	 %xmm12, %xmm3
	addpd	 %xmm3,  %xmm11

	mulpd	 %xmm13, %xmm4
	addpd	 %xmm4,  %xmm8
	MOVUPS_YS1(-16 * SIZE, Y1, %xmm8)
	mulpd	 %xmm13, %xmm5
	addpd	 %xmm5,  %xmm9
	MOVUPS_YS1(-14 * SIZE, Y1, %xmm9)
	mulpd	 %xmm13, %xmm6
	addpd	 %xmm6,  %xmm10
	MOVUPS_YS1(-12 * SIZE, Y1, %xmm10)
	mulpd	 %xmm13, %xmm7
	addpd	 %xmm7,  %xmm11
	MOVUPS_YS1(-10 * SIZE, Y1, %xmm11)

	subq	 $-8 * SIZE, A1
	subq	 $-8 * SIZE, A2
	subq	 $-8 * SIZE, Y1
	ALIGN_3

.L35:
	testq	$4, MM
	je	.L36


	MOVUPS_A1(-16 * SIZE, A1, %xmm0)
	MOVUPS_A1(-14 * SIZE, A1, %xmm1)

	MOVUPS_YL1(-16 * SIZE, Y1, %xmm8)
	MOVUPS_YL1(-14 * SIZE, Y1, %xmm9)

	mulpd	 %xmm12, %xmm0
	addpd	 %xmm0,  %xmm8
	mulpd	 %xmm12, %xmm1
	addpd	 %xmm1,  %xmm9

	MOVUPS_A1(-16 * SIZE, A2, %xmm4)
	MOVUPS_A1(-14 * SIZE, A2, %xmm5)

	mulpd	 %xmm13, %xmm4
	addpd	 %xmm4,  %xmm8
	MOVUPS_YS1(-16 * SIZE, Y1, %xmm8)
	mulpd	 %xmm13, %xmm5
	addpd	 %xmm5,  %xmm9
	MOVUPS_YS1(-14 * SIZE, Y1, %xmm9)

	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, A2
	addq	 $4 * SIZE, Y1
	ALIGN_3

.L36:
	testq	$2, MM
	je	.L37

	MOVUPS_A1(-16 * SIZE, A1, %xmm8)
	MOVUPS_A1(-16 * SIZE, A2, %xmm9)

	MOVUPS_YL1(-16 * SIZE, Y1, %xmm0)

	mulpd	 %xmm12, %xmm8
	addpd	 %xmm8,  %xmm0
	mulpd	 %xmm13, %xmm9
	addpd	 %xmm9,  %xmm0

	MOVUPS_YS1(-16 * SIZE, Y1, %xmm0)

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, Y1
	ALIGN_3

.L37:
	testq	$1, MM
#if GEMV_UNROLL == 2
	je	.L38
#else
	je	.L40
#endif

	movsd	 -16 * SIZE(Y1), %xmm0

	movsd	 -16 * SIZE(A1), %xmm8
	movsd	 -16 * SIZE(A2), %xmm9

	mulsd	 %xmm12, %xmm8
	addsd	 %xmm8,  %xmm0
	mulsd	 %xmm13, %xmm9
	addsd	 %xmm9,  %xmm0

	movsd	 %xmm0, -16 * SIZE(Y1)
	ALIGN_3

#if GEMV_UNROLL == 2
.L38:
	cmpq	$2, N
	jge	.L31
	ALIGN_3

#endif

.L40:
	cmpq	$1, N
	jl	.L900
#endif

	leaq	16 * SIZE(BUFFER), Y1
	movq	A,  A1

#ifdef HAVE_SSE3
	movddup	(X), %xmm12
	addq	INCX, X

	movddup	ALPHA, %xmm0
#else
	movsd	(X), %xmm12
	unpcklpd %xmm12, %xmm12
	addq	INCX, X

	movsd	ALPHA, %xmm0
	unpcklpd %xmm0, %xmm0
#endif

	mulpd	%xmm0, %xmm12

#ifdef ALIGNED_ACCESS
	testq	$SIZE, A
	je	.L4X

	movsd	 -16 * SIZE(A1), %xmm4
	movsd	 -16 * SIZE(Y1), %xmm0

	mulsd	 %xmm12, %xmm4
	addsd	 %xmm4,  %xmm0

	movsd	 %xmm0, -16 * SIZE(Y1)

	addq	 $SIZE, A1
	addq	 $SIZE, Y1
	ALIGN_3

.L4X:
#endif

	movq	MM,  I
	sarq	$3,  I
	jle	.L45

	MOVUPS_A1(-16 * SIZE, A1, %xmm0)
	MOVUPS_A1(-14 * SIZE, A1, %xmm1)
	MOVUPS_A1(-12 * SIZE, A1, %xmm2)
	MOVUPS_A1(-10 * SIZE, A1, %xmm3)

	MOVUPS_YL1(-16 * SIZE, Y1, %xmm8)
	MOVUPS_YL1(-14 * SIZE, Y1, %xmm9)
	MOVUPS_YL1(-12 * SIZE, Y1, %xmm10)
	MOVUPS_YL1(-10 * SIZE, Y1, %xmm11)

	decq	 I
	jle	 .L44
	ALIGN_3

.L43:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 8  - 128 + PREOFFSET(A1)
#endif

	mulpd	 %xmm12, %xmm0
	addpd	 %xmm0,  %xmm8
	MOVUPS_A1( -8 * SIZE, A1, %xmm0)
	mulpd	 %xmm12, %xmm1
	addpd	 %xmm1,  %xmm9
	MOVUPS_A1( -6 * SIZE, A1, %xmm1)

	mulpd	 %xmm12, %xmm2
	addpd	 %xmm2,  %xmm10
	MOVUPS_A1( -4 * SIZE, A1, %xmm2)
	mulpd	 %xmm12, %xmm3
	addpd	 %xmm3,  %xmm11
	MOVUPS_A1( -2 * SIZE, A1, %xmm3)

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE) * 8  - 128 + PREOFFSET(Y1)
#endif

	MOVUPS_YS1(-16 * SIZE, Y1, %xmm8)
	MOVUPS_YS1(-14 * SIZE, Y1, %xmm9)
	MOVUPS_YS1(-12 * SIZE, Y1, %xmm10)
	MOVUPS_YS1(-10 * SIZE, Y1, %xmm11)

	MOVUPS_YL1( -8 * SIZE, Y1, %xmm8)
	MOVUPS_YL1( -6 * SIZE, Y1, %xmm9)
	MOVUPS_YL1( -4 * SIZE, Y1, %xmm10)
	MOVUPS_YL1( -2 * SIZE, Y1, %xmm11)

	subq	 $-8 * SIZE, A1
	subq	 $-8 * SIZE, Y1

	subq	 $1, I
	BRANCH
	jg	.L43
	ALIGN_3

.L44:
	mulpd	 %xmm12, %xmm0
	addpd	 %xmm0,  %xmm8
	MOVUPS_YS1(-16 * SIZE, Y1, %xmm8)
	mulpd	 %xmm12, %xmm1
	addpd	 %xmm1,  %xmm9
	MOVUPS_YS1(-14 * SIZE, Y1, %xmm9)
	mulpd	 %xmm12, %xmm2
	addpd	 %xmm2,  %xmm10
	MOVUPS_YS1(-12 * SIZE, Y1, %xmm10)
	mulpd	 %xmm12, %xmm3
	addpd	 %xmm3,  %xmm11
	MOVUPS_YS1(-10 * SIZE, Y1, %xmm11)

	subq	 $-8 * SIZE, A1
	subq	 $-8 * SIZE, Y1
	ALIGN_3

.L45:
	testq	$4, MM
	je	.L46

	MOVUPS_A1(-16 * SIZE, A1, %xmm0)
	MOVUPS_A1(-14 * SIZE, A1, %xmm1)

	MOVUPS_YL1(-16 * SIZE, Y1, %xmm8)
	MOVUPS_YL1(-14 * SIZE, Y1, %xmm9)

	mulpd	 %xmm12, %xmm0
	addpd	 %xmm0,  %xmm8
	MOVUPS_YS1(-16 * SIZE, Y1, %xmm8)
	mulpd	 %xmm12, %xmm1
	addpd	 %xmm1,  %xmm9
	MOVUPS_YS1(-14 * SIZE, Y1, %xmm9)

	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, Y1
	ALIGN_3

.L46:
	testq	$2, MM
	je	.L47

	MOVUPS_A1(-16 * SIZE, A1, %xmm8)

	MOVUPS_YL1(-16 * SIZE, Y1, %xmm0)

	mulpd	 %xmm12, %xmm8
	addpd	 %xmm8,  %xmm0

	MOVUPS_YS1(-16 * SIZE, Y1, %xmm0)

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, Y1
	ALIGN_3

.L47:
	testq	$1, MM
	je	.L900

	movsd	 -16 * SIZE(Y1), %xmm0
	movsd	 -16 * SIZE(A1), %xmm8

	mulsd	 %xmm12, %xmm8
	addsd	 %xmm8,  %xmm0

	movsd	 %xmm0, -16 * SIZE(Y1)
	ALIGN_3

#ifdef ALIGNED_ACCESS
	jmp	.L900
	ALIGN_3

.L50:
#if GEMV_UNROLL >= 4

	cmpq	$4, N
	jl	.L60
	ALIGN_3

.L51:

	subq	$4, N

	leaq	16 * SIZE(BUFFER), Y1
	movq	A,  A1
	leaq	(A,  LDA, 2), A2
	leaq	(A,  LDA, 4), A

#ifdef HAVE_SSE3
	movddup	(X), %xmm12
	addq	INCX, X
	movddup	(X), %xmm13
	addq	INCX, X
	movddup	(X), %xmm14
	addq	INCX, X
	movddup	(X), %xmm15
	addq	INCX, X

	movddup	ALPHA, %xmm0
#else
	movsd	(X), %xmm12
	unpcklpd %xmm12, %xmm12
	addq	INCX, X
	movsd	(X), %xmm13
	unpcklpd %xmm13, %xmm13
	addq	INCX, X
	movsd	(X), %xmm14
	unpcklpd %xmm14, %xmm14
	addq	INCX, X
	movsd	(X), %xmm15
	unpcklpd %xmm15, %xmm15
	addq	INCX, X

	movsd	ALPHA, %xmm0
	unpcklpd %xmm0, %xmm0
#endif

	mulpd	%xmm0, %xmm12
	mulpd	%xmm0, %xmm13
	mulpd	%xmm0, %xmm14
	mulpd	%xmm0, %xmm15

	testq	$SIZE, A
	je	.L5X

	movsd	 -16 * SIZE(A1),         %xmm4
	movsd	 -16 * SIZE(A1, LDA),    %xmm5
	movsd	 -16 * SIZE(A2),         %xmm6
	movsd	 -16 * SIZE(A2, LDA),    %xmm7

	movsd	 -16 * SIZE(Y1),         %xmm0

	mulsd	 %xmm12, %xmm4
	addsd	 %xmm4,  %xmm0
	mulsd	 %xmm13, %xmm5
	addsd	 %xmm5,  %xmm0
	mulsd	 %xmm14, %xmm6
	addsd	 %xmm6,  %xmm0
	mulsd	 %xmm15, %xmm7
	addsd	 %xmm7,  %xmm0

	movsd	 %xmm0, -16 * SIZE(Y1)

	addq	 $SIZE, A1
	addq	 $SIZE, A2
	addq	 $SIZE, Y1
	ALIGN_3

.L5X:
	movhpd	-16 * SIZE(A1, LDA), %xmm8
	movhpd	-16 * SIZE(A2, LDA), %xmm9

	movq	MM,  I
	sarq	$3,  I
	jle	.L55

	MOVUPS_A1(-16 * SIZE, A1, %xmm4)
	MOVUPS_A1(-14 * SIZE, A1, %xmm5)
	MOVUPS_A1(-12 * SIZE, A1, %xmm6)

	MOVUPS_YL1(-16 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-14 * SIZE, Y1, %xmm1)
	MOVUPS_YL1(-12 * SIZE, Y1, %xmm2)
	MOVUPS_YL1(-10 * SIZE, Y1, %xmm3)

	decq	 I
	jle	 .L54
	ALIGN_3

.L53:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2  - 128 + PREOFFSET(A1)
#endif

	mulpd	 %xmm12, %xmm4
	addpd	 %xmm4,  %xmm0
	MOVUPS_A1(-10 * SIZE, A1, %xmm7)
	mulpd	 %xmm12, %xmm5
	addpd	 %xmm5,  %xmm1
	MOVUPS_A2(-15 * SIZE, A1, LDA, 1, %xmm4)

	mulpd	 %xmm12, %xmm6
	addpd	 %xmm6,  %xmm2
	MOVUPS_A2(-13 * SIZE, A1, LDA, 1, %xmm5)
	mulpd	 %xmm12, %xmm7
	addpd	 %xmm7,  %xmm3
	MOVUPS_A2(-11 * SIZE, A1, LDA, 1, %xmm6)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2  - 128 + PREOFFSET + 8(A1, LDA)
#endif

	shufpd	$1, %xmm4, %xmm8
	mulpd	 %xmm13, %xmm8
	addpd	 %xmm8,  %xmm0
	MOVUPS_A2( -9 * SIZE, A1, LDA, 1, %xmm8)
	shufpd	$1, %xmm5, %xmm4
	mulpd	 %xmm13, %xmm4
	addpd	 %xmm4,  %xmm1
	MOVUPS_A1(-16 * SIZE, A2, %xmm4)

	shufpd	$1, %xmm6, %xmm5
	mulpd	 %xmm13, %xmm5
	addpd	 %xmm5,  %xmm2
	MOVUPS_A1(-14 * SIZE, A2, %xmm5)
	shufpd	$1, %xmm8, %xmm6
	mulpd	 %xmm13, %xmm6
	addpd	 %xmm6,  %xmm3
	MOVUPS_A1(-12 * SIZE, A2, %xmm6)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2  - 128 + PREOFFSET(A2)
#endif

	mulpd	 %xmm14, %xmm4
	addpd	 %xmm4, %xmm0
	MOVUPS_A1(-10 * SIZE, A2, %xmm7)
	mulpd	 %xmm14, %xmm5
	addpd	 %xmm5, %xmm1
	MOVUPS_A2(-15 * SIZE, A2, LDA, 1, %xmm4)

	mulpd	 %xmm14, %xmm6
	addpd	 %xmm6,  %xmm2
	MOVUPS_A2(-13 * SIZE, A2, LDA, 1, %xmm5)
	mulpd	 %xmm14, %xmm7
	addpd	 %xmm7,  %xmm3
	MOVUPS_A2(-11 * SIZE, A2, LDA, 1, %xmm6)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2  - 128 + PREOFFSET + 8(A2, LDA)
#endif

	shufpd	$1, %xmm4, %xmm9
	mulpd	 %xmm15, %xmm9
	addpd	 %xmm9,  %xmm0
	MOVUPS_A2( -9 * SIZE, A2, LDA, 1, %xmm9)
	shufpd	$1, %xmm5, %xmm4
	mulpd	 %xmm15, %xmm4
	addpd	 %xmm4,  %xmm1
	MOVUPS_A1( -8 * SIZE, A1, %xmm4)

	shufpd	$1, %xmm6, %xmm5
	mulpd	 %xmm15, %xmm5
	addpd	 %xmm5,  %xmm2
	MOVUPS_A1( -6 * SIZE, A1, %xmm5)
	shufpd	$1, %xmm9, %xmm6
	mulpd	 %xmm15, %xmm6
	addpd	 %xmm6,  %xmm3
	MOVUPS_A1( -4 * SIZE, A1, %xmm6)

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE) * 2  - 128 + PREOFFSET(Y1)
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
	jg	.L53
	ALIGN_3

.L54:
	mulpd	 %xmm12, %xmm4
	addpd	 %xmm4,  %xmm0
	MOVUPS_A1(-10 * SIZE, A1, %xmm7)
	mulpd	 %xmm12, %xmm5
	addpd	 %xmm5,  %xmm1
	MOVUPS_A2(-15 * SIZE, A1, LDA, 1, %xmm4)

	mulpd	 %xmm12, %xmm6
	addpd	 %xmm6,  %xmm2
	MOVUPS_A2(-13 * SIZE, A1, LDA, 1, %xmm5)
	mulpd	 %xmm12, %xmm7
	addpd	 %xmm7,  %xmm3
	MOVUPS_A2(-11 * SIZE, A1, LDA, 1, %xmm6)

	shufpd	$1, %xmm4, %xmm8
	mulpd	 %xmm13, %xmm8
	addpd	 %xmm8,  %xmm0
	MOVUPS_A2( -9 * SIZE, A1, LDA, 1, %xmm8)
	shufpd	$1, %xmm5, %xmm4
	mulpd	 %xmm13, %xmm4
	addpd	 %xmm4,  %xmm1
	MOVUPS_A1(-16 * SIZE, A2, %xmm4)

	shufpd	$1, %xmm6, %xmm5
	mulpd	 %xmm13, %xmm5
	addpd	 %xmm5,  %xmm2
	MOVUPS_A1(-14 * SIZE, A2, %xmm5)
	shufpd	$1, %xmm8, %xmm6
	mulpd	 %xmm13, %xmm6
	addpd	 %xmm6,  %xmm3
	MOVUPS_A1(-12 * SIZE, A2, %xmm6)

	mulpd	 %xmm14, %xmm4
	addpd	 %xmm4, %xmm0
	MOVUPS_A1(-10 * SIZE, A2, %xmm7)
	mulpd	 %xmm14, %xmm5
	addpd	 %xmm5, %xmm1
	MOVUPS_A2(-15 * SIZE, A2, LDA, 1, %xmm4)

	mulpd	 %xmm14, %xmm6
	addpd	 %xmm6,  %xmm2
	MOVUPS_A2(-13 * SIZE, A2, LDA, 1, %xmm5)
	mulpd	 %xmm14, %xmm7
	addpd	 %xmm7,  %xmm3
	MOVUPS_A2(-11 * SIZE, A2, LDA, 1, %xmm6)

	shufpd	$1, %xmm4, %xmm9
	mulpd	 %xmm15, %xmm9
	addpd	 %xmm9,  %xmm0
	MOVUPS_A2( -9 * SIZE, A2, LDA, 1, %xmm9)

	shufpd	$1, %xmm5, %xmm4
	mulpd	 %xmm15, %xmm4
	addpd	 %xmm4,  %xmm1
	shufpd	$1, %xmm6, %xmm5
	mulpd	 %xmm15, %xmm5
	addpd	 %xmm5,  %xmm2
	shufpd	$1, %xmm9, %xmm6
	mulpd	 %xmm15, %xmm6
	addpd	 %xmm6,  %xmm3

	MOVUPS_YS1(-16 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-14 * SIZE, Y1, %xmm1)
	MOVUPS_YS1(-12 * SIZE, Y1, %xmm2)
	MOVUPS_YS1(-10 * SIZE, Y1, %xmm3)

	subq	 $-8 * SIZE, A1
	subq	 $-8 * SIZE, A2
	subq	 $-8 * SIZE, Y1
	ALIGN_3

.L55:
	testq	$4, MM
	je	.L56

	MOVUPS_A1(-16 * SIZE, A1, %xmm4)
	MOVUPS_A1(-14 * SIZE, A1, %xmm5)

	MOVUPS_YL1(-16 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-14 * SIZE, Y1, %xmm1)

	mulpd	 %xmm12, %xmm4
	addpd	 %xmm4,  %xmm0
	mulpd	 %xmm12, %xmm5
	addpd	 %xmm5,  %xmm1

	MOVUPS_A2(-15 * SIZE, A1, LDA, 1, %xmm6)
	MOVUPS_A2(-13 * SIZE, A1, LDA, 1, %xmm7)

	shufpd	$1, %xmm6, %xmm8
	mulpd	 %xmm13, %xmm8
	addpd	 %xmm8,  %xmm0
	movaps	 %xmm7,  %xmm8
	shufpd	$1, %xmm7, %xmm6
	mulpd	 %xmm13, %xmm6
	addpd	 %xmm6,  %xmm1

	MOVUPS_A1(-16 * SIZE, A2, %xmm4)
	MOVUPS_A1(-14 * SIZE, A2, %xmm5)

	mulpd	 %xmm14, %xmm4
	addpd	 %xmm4,  %xmm0
	mulpd	 %xmm14, %xmm5
	addpd	 %xmm5,  %xmm1

	MOVUPS_A2(-15 * SIZE, A2, LDA, 1, %xmm6)
	MOVUPS_A2(-13 * SIZE, A2, LDA, 1, %xmm7)

	shufpd	$1, %xmm6, %xmm9
	mulpd	 %xmm15, %xmm9
	addpd	 %xmm9,  %xmm0
	movaps	 %xmm7,  %xmm9
	shufpd	$1, %xmm7, %xmm6
	mulpd	 %xmm15, %xmm6
	addpd	 %xmm6,  %xmm1

	MOVUPS_YS1(-16 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-14 * SIZE, Y1, %xmm1)

	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, A2
	addq	 $4 * SIZE, Y1
	ALIGN_3

.L56:
	testq	$2, MM
	je	.L57

	MOVUPS_A1(-16 * SIZE, A1, %xmm4)
	MOVUPS_A2(-15 * SIZE, A1, LDA, 1, %xmm5)
	MOVUPS_A1(-16 * SIZE, A2, %xmm6)
	MOVUPS_A2(-15 * SIZE, A2, LDA, 1, %xmm7)

	MOVUPS_YL1(-16 * SIZE, Y1, %xmm0)

	mulpd	 %xmm12, %xmm4
	addpd	 %xmm4,  %xmm0
	shufpd	$1, %xmm5, %xmm8
	mulpd	 %xmm13, %xmm8
	addpd	 %xmm8,  %xmm0
	movaps	 %xmm5,  %xmm8
	mulpd	 %xmm14, %xmm6
	addpd	 %xmm6,  %xmm0
	shufpd	$1, %xmm7, %xmm9
	mulpd	 %xmm15, %xmm9
	addpd	 %xmm9,  %xmm0
	movaps	 %xmm7,  %xmm9

	MOVUPS_YS1(-16 * SIZE, Y1, %xmm0)

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, Y1
	ALIGN_3

.L57:
	testq	$1, MM
	je	.L58

	movsd	 -16 * SIZE(Y1), %xmm0

	movsd	 -16 * SIZE(A1), %xmm4
	shufpd	 $1, %xmm8, %xmm8
	movsd	 -16 * SIZE(A2), %xmm6
	shufpd	 $1, %xmm9, %xmm9

	mulsd	 %xmm12, %xmm4
	addsd	 %xmm4,  %xmm0
	mulsd	 %xmm13, %xmm8
	addsd	 %xmm8,  %xmm0
	mulsd	 %xmm14, %xmm6
	addsd	 %xmm6,  %xmm0
	mulsd	 %xmm15, %xmm9
	addsd	 %xmm9,  %xmm0

	movsd	 %xmm0, -16 * SIZE(Y1)
	ALIGN_3

.L58:
	cmpq	$4, N
	jge	.L51
	ALIGN_3

.L60:
#endif

#if GEMV_UNROLL >= 2

	cmpq	$2, N
	jl	.L70

#if GEMV_UNROLL == 2
	ALIGN_3

.L61:
#endif

	subq	$2, N

	leaq	16 * SIZE(BUFFER), Y1
	movq	A,  A1
	leaq	(A,  LDA), A2
	leaq	(A,  LDA, 2), A

#ifdef HAVE_SSE3
	movddup	(X), %xmm12
	addq	INCX, X
	movddup	(X), %xmm13
	addq	INCX, X

	movddup	ALPHA, %xmm0
#else
	movsd	(X), %xmm12
	unpcklpd %xmm12, %xmm12
	addq	INCX, X
	movsd	(X), %xmm13
	unpcklpd %xmm13, %xmm13
	addq	INCX, X

	movsd	ALPHA, %xmm0
	unpcklpd %xmm0, %xmm0
#endif

	mulpd	%xmm0, %xmm12
	mulpd	%xmm0, %xmm13

	testq	$SIZE, A
	je	.L6X

	movsd	 -16 * SIZE(A1), %xmm4
	movsd	 -16 * SIZE(A2), %xmm5

	movsd	 -16 * SIZE(Y1), %xmm0

	mulsd	 %xmm12, %xmm4
	addsd	 %xmm4,  %xmm0
	mulsd	 %xmm13, %xmm5
	addsd	 %xmm5,  %xmm0

	movsd	 %xmm0, -16 * SIZE(Y1)

	addq	 $SIZE, A1
	addq	 $SIZE, A2
	addq	 $SIZE, Y1
	ALIGN_3

.L6X:
	movhpd	-16 * SIZE(A2), %xmm8

	movq	MM,  I
	sarq	$3,  I
	jle	.L65

	MOVUPS_A1(-16 * SIZE, A1, %xmm4)
	MOVUPS_A1(-14 * SIZE, A1, %xmm5)
	MOVUPS_A1(-12 * SIZE, A1, %xmm6)

	MOVUPS_YL1(-16 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-14 * SIZE, Y1, %xmm1)
	MOVUPS_YL1(-12 * SIZE, Y1, %xmm2)
	MOVUPS_YL1(-10 * SIZE, Y1, %xmm3)

	decq	 I
	jle	 .L64
	ALIGN_3

.L63:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 4  - 128 + PREOFFSET(A1)
#endif

	mulpd	 %xmm12, %xmm4
	addpd	 %xmm4,  %xmm0
	MOVUPS_A1(-10 * SIZE, A1, %xmm7)
	mulpd	 %xmm12, %xmm5
	addpd	 %xmm5,  %xmm1
	MOVUPS_A1(-15 * SIZE, A2, %xmm4)

	mulpd	 %xmm12, %xmm6
	addpd	 %xmm6,  %xmm2
	MOVUPS_A1(-13 * SIZE, A2, %xmm5)
	mulpd	 %xmm12, %xmm7
	addpd	 %xmm7,  %xmm3
	MOVUPS_A1(-11 * SIZE, A2, %xmm6)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 4  - 128 + PREOFFSET + 8(A2)
#endif

	shufpd	$1, %xmm4, %xmm8
	mulpd	 %xmm13, %xmm8
	addpd	 %xmm8,  %xmm0
	MOVUPS_A1( -9 * SIZE, A2, %xmm8)
	shufpd	$1, %xmm5, %xmm4
	mulpd	 %xmm13, %xmm4
	addpd	 %xmm4,  %xmm1
	MOVUPS_A1( -8 * SIZE, A1, %xmm4)

	shufpd	$1, %xmm6, %xmm5
	mulpd	 %xmm13, %xmm5
	addpd	 %xmm5,  %xmm2
	MOVUPS_A1( -6 * SIZE, A1, %xmm5)
	shufpd	$1, %xmm8, %xmm6
	mulpd	 %xmm13, %xmm6
	addpd	 %xmm6,  %xmm3
	MOVUPS_A1( -4 * SIZE, A1, %xmm6)

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE) * 4  - 128 + PREOFFSET(Y1)
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
	jg	.L63
	ALIGN_3

.L64:
	mulpd	 %xmm12, %xmm4
	addpd	 %xmm4,  %xmm0
	MOVUPS_A1(-10 * SIZE, A1, %xmm7)
	mulpd	 %xmm12, %xmm5
	addpd	 %xmm5,  %xmm1
	MOVUPS_A1(-15 * SIZE, A2, %xmm4)

	mulpd	 %xmm12, %xmm6
	addpd	 %xmm6,  %xmm2
	MOVUPS_A1(-13 * SIZE, A2, %xmm5)
	mulpd	 %xmm12, %xmm7
	addpd	 %xmm7,  %xmm3
	MOVUPS_A1(-11 * SIZE, A2, %xmm6)

	shufpd	$1, %xmm4, %xmm8
	mulpd	 %xmm13, %xmm8
	addpd	 %xmm8,  %xmm0
	MOVUPS_A1( -9 * SIZE, A2, %xmm8)
	shufpd	$1, %xmm5, %xmm4
	mulpd	 %xmm13, %xmm4
	addpd	 %xmm4,  %xmm1

	shufpd	$1, %xmm6, %xmm5
	mulpd	 %xmm13, %xmm5
	addpd	 %xmm5,  %xmm2
	shufpd	$1, %xmm8, %xmm6
	mulpd	 %xmm13, %xmm6
	addpd	 %xmm6,  %xmm3

	MOVUPS_YS1(-16 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-14 * SIZE, Y1, %xmm1)
	MOVUPS_YS1(-12 * SIZE, Y1, %xmm2)
	MOVUPS_YS1(-10 * SIZE, Y1, %xmm3)

	subq	 $-8 * SIZE, A1
	subq	 $-8 * SIZE, A2
	subq	 $-8 * SIZE, Y1
	ALIGN_3

.L65:
	testq	$4, MM
	je	.L66


	MOVUPS_A1(-16 * SIZE, A1, %xmm4)
	MOVUPS_A1(-14 * SIZE, A1, %xmm5)

	MOVUPS_YL1(-16 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-14 * SIZE, Y1, %xmm1)

	mulpd	 %xmm12, %xmm4
	addpd	 %xmm4,  %xmm0
	mulpd	 %xmm12, %xmm5
	addpd	 %xmm5,  %xmm1

	MOVUPS_A1(-15 * SIZE, A2, %xmm6)
	MOVUPS_A1(-13 * SIZE, A2, %xmm7)

	shufpd	$1, %xmm6, %xmm8
	mulpd	 %xmm13, %xmm8
	addpd	 %xmm8,  %xmm0
	movaps	 %xmm7,  %xmm8
	shufpd	$1, %xmm7, %xmm6
	mulpd	 %xmm13, %xmm6
	addpd	 %xmm6,  %xmm1

	MOVUPS_YS1(-16 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-14 * SIZE, Y1, %xmm1)

	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, A2
	addq	 $4 * SIZE, Y1
	ALIGN_3

.L66:
	testq	$2, MM
	je	.L67

	MOVUPS_A1(-16 * SIZE, A1, %xmm4)
	MOVUPS_A1(-15 * SIZE, A2, %xmm5)

	MOVUPS_YL1(-16 * SIZE, Y1, %xmm0)

	mulpd	 %xmm12, %xmm4
	addpd	 %xmm4,  %xmm0
	shufpd	$1, %xmm5, %xmm8
	mulpd	 %xmm13, %xmm8
	addpd	 %xmm8,  %xmm0
	movaps	 %xmm5,  %xmm8

	MOVUPS_YS1(-16 * SIZE, Y1, %xmm0)

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, Y1
	ALIGN_3

.L67:
	testq	$1, MM
#if GEMV_UNROLL == 2
	je	.L68
#else
	je	.L70
#endif

	movsd	 -16 * SIZE(Y1), %xmm0

	movsd	 -16 * SIZE(A1), %xmm4
	shufpd	 $1, %xmm8, %xmm8

	mulsd	 %xmm12, %xmm4
	addsd	 %xmm4,  %xmm0
	mulsd	 %xmm13, %xmm8
	addsd	 %xmm8,  %xmm0

	movsd	 %xmm0, -16 * SIZE(Y1)
	ALIGN_3

#if GEMV_UNROLL == 2
.L68:
	cmpq	$2, N
	jge	.L61
	ALIGN_3

#endif

.L70:
	cmpq	$1, N
	jl	.L900

#endif

	leaq	16 * SIZE(BUFFER), Y1
	movq	A,  A1

#ifdef HAVE_SSE3
	movddup	(X), %xmm12
	addq	INCX, X

	movddup	ALPHA, %xmm0
#else
	movsd	(X), %xmm12
	unpcklpd %xmm12, %xmm12
	addq	INCX, X

	movsd	ALPHA, %xmm0
	unpcklpd %xmm0, %xmm0
#endif

	mulpd	%xmm0, %xmm12

	testq	$SIZE, A
	je	.L7X

	movsd	 -16 * SIZE(A1), %xmm4
	movsd	 -16 * SIZE(Y1), %xmm0

	mulsd	 %xmm12, %xmm4
	addsd	 %xmm4,  %xmm0

	movsd	 %xmm0, -16 * SIZE(Y1)

	addq	 $SIZE, A1
	addq	 $SIZE, Y1
	ALIGN_3

.L7X:

	movq	MM,  I
	sarq	$3,  I
	jle	.L75

	MOVUPS_A1(-16 * SIZE, A1, %xmm0)
	MOVUPS_A1(-14 * SIZE, A1, %xmm1)
	MOVUPS_A1(-12 * SIZE, A1, %xmm2)
	MOVUPS_A1(-10 * SIZE, A1, %xmm3)

	MOVUPS_YL1(-16 * SIZE, Y1, %xmm8)
	MOVUPS_YL1(-14 * SIZE, Y1, %xmm9)
	MOVUPS_YL1(-12 * SIZE, Y1, %xmm10)
	MOVUPS_YL1(-10 * SIZE, Y1, %xmm11)

	decq	 I
	jle	 .L74
	ALIGN_3

.L73:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 8  - 128 + PREOFFSET(A1)
#endif

	mulpd	 %xmm12, %xmm0
	addpd	 %xmm0,  %xmm8
	MOVUPS_A1( -8 * SIZE, A1, %xmm0)
	mulpd	 %xmm12, %xmm1
	addpd	 %xmm1,  %xmm9
	MOVUPS_A1( -6 * SIZE, A1, %xmm1)

	mulpd	 %xmm12, %xmm2
	addpd	 %xmm2,  %xmm10
	MOVUPS_A1( -4 * SIZE, A1, %xmm2)
	mulpd	 %xmm12, %xmm3
	addpd	 %xmm3,  %xmm11
	MOVUPS_A1( -2 * SIZE, A1, %xmm3)

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE) * 8  - 128 + PREOFFSET(Y1)
#endif

	MOVUPS_YS1(-16 * SIZE, Y1, %xmm8)
	MOVUPS_YS1(-14 * SIZE, Y1, %xmm9)
	MOVUPS_YS1(-12 * SIZE, Y1, %xmm10)
	MOVUPS_YS1(-10 * SIZE, Y1, %xmm11)

	MOVUPS_YL1( -8 * SIZE, Y1, %xmm8)
	MOVUPS_YL1( -6 * SIZE, Y1, %xmm9)
	MOVUPS_YL1( -4 * SIZE, Y1, %xmm10)
	MOVUPS_YL1( -2 * SIZE, Y1, %xmm11)

	subq	 $-8 * SIZE, A1
	subq	 $-8 * SIZE, Y1

	subq	 $1, I
	BRANCH
	jg	.L73
	ALIGN_3

.L74:
	mulpd	 %xmm12, %xmm0
	addpd	 %xmm0,  %xmm8
	MOVUPS_YS1(-16 * SIZE, Y1, %xmm8)
	mulpd	 %xmm12, %xmm1
	addpd	 %xmm1,  %xmm9
	MOVUPS_YS1(-14 * SIZE, Y1, %xmm9)
	mulpd	 %xmm12, %xmm2
	addpd	 %xmm2,  %xmm10
	MOVUPS_YS1(-12 * SIZE, Y1, %xmm10)
	mulpd	 %xmm12, %xmm3
	addpd	 %xmm3,  %xmm11
	MOVUPS_YS1(-10 * SIZE, Y1, %xmm11)

	subq	 $-8 * SIZE, A1
	subq	 $-8 * SIZE, Y1
	ALIGN_3

.L75:
	testq	$4, MM
	je	.L76

	MOVUPS_A1(-16 * SIZE, A1, %xmm0)
	MOVUPS_A1(-14 * SIZE, A1, %xmm1)

	MOVUPS_YL1(-16 * SIZE, Y1, %xmm8)
	MOVUPS_YL1(-14 * SIZE, Y1, %xmm9)

	mulpd	 %xmm12, %xmm0
	addpd	 %xmm0,  %xmm8
	MOVUPS_YS1(-16 * SIZE, Y1, %xmm8)
	mulpd	 %xmm12, %xmm1
	addpd	 %xmm1,  %xmm9
	MOVUPS_YS1(-14 * SIZE, Y1, %xmm9)

	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, Y1
	ALIGN_3

.L76:
	testq	$2, MM
	je	.L77

	MOVUPS_A1(-16 * SIZE, A1, %xmm8)

	MOVUPS_YL1(-16 * SIZE, Y1, %xmm0)

	mulpd	 %xmm12, %xmm8
	addpd	 %xmm8,  %xmm0

	MOVUPS_YS1(-16 * SIZE, Y1, %xmm0)

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, Y1
	ALIGN_3

.L77:
	testq	$1, MM
	je	.L900

	movsd	 -16 * SIZE(Y1), %xmm0
	movsd	 -16 * SIZE(A1), %xmm8

	mulsd	 %xmm12, %xmm8
	addsd	 %xmm8,  %xmm0

	movsd	 %xmm0, -16 * SIZE(Y1)
#endif
	ALIGN_3


.L900:
#ifndef COPY_FORCE
	cmpq	Y, BUFFER
	je	.L999
#endif
	movq	M,   TMP_M
	movq    Y,    Y1

	cmpq	$SIZE, INCY
	jne	.L950

	testq	$SIZE, Y1
	je	.L910

	movsd	(Y1), %xmm0
	addsd	(BUFFER), %xmm0
	movsd	%xmm0, (Y1)

	addq	$SIZE, Y1
	addq	$SIZE, BUFFER

	decq	TMP_M
	jle	.L999
	ALIGN_4

.L910:
	testq	$SIZE, BUFFER
	jne	.L920

	movq	TMP_M,   %rax
	sarq	$3,  %rax
	jle	.L914
	ALIGN_3

.L912:
#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE) * 4 + PREOFFSET(Y1)
#endif

	movapd	0 * SIZE(Y1), %xmm0
	movapd	2 * SIZE(Y1), %xmm1
	movapd	4 * SIZE(Y1), %xmm2
	movapd	6 * SIZE(Y1), %xmm3

	movapd	0 * SIZE(BUFFER), %xmm4
	movapd	2 * SIZE(BUFFER), %xmm5
	movapd	4 * SIZE(BUFFER), %xmm6
	movapd	6 * SIZE(BUFFER), %xmm7

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 4 + PREOFFSET(BUFFER)
#endif

	addpd	%xmm4, %xmm0
	addpd	%xmm5, %xmm1
	addpd	%xmm6, %xmm2
	addpd	%xmm7, %xmm3

	movapd	%xmm0,  0 * SIZE(Y1)
	movapd	%xmm1,  2 * SIZE(Y1)
	movapd	%xmm2,  4 * SIZE(Y1)
	movapd	%xmm3,  6 * SIZE(Y1)

	addq	$8 * SIZE, Y1
	addq	$8 * SIZE, BUFFER

	decq	%rax
	jg	.L912
	ALIGN_3

.L914:
	testq	$7, TMP_M
	jle	.L999

	testq	$4, TMP_M
	jle	.L915

	movapd	0 * SIZE(Y1), %xmm0
	movapd	2 * SIZE(Y1), %xmm1

	movapd	0 * SIZE(BUFFER), %xmm4
	movapd	2 * SIZE(BUFFER), %xmm5

	addpd	%xmm4, %xmm0
	addpd	%xmm5, %xmm1

	movapd	%xmm0,  0 * SIZE(Y1)
	movapd	%xmm1,  2 * SIZE(Y1)

	addq	$4 * SIZE, Y1
	addq	$4 * SIZE, BUFFER
	ALIGN_3

.L915:
	testq	$2, TMP_M
	jle	.L916

	movapd	(Y1), %xmm0

	movapd	(BUFFER), %xmm4

	addpd	%xmm4, %xmm0

	movapd	%xmm0,  (Y1)

	addq	$2 * SIZE, Y1
	addq	$2 * SIZE, BUFFER
	ALIGN_3

.L916:
	testq	$1, TMP_M
	jle	.L999

	movsd	(Y1), %xmm0

	movsd	0 * SIZE(BUFFER), %xmm4

	addsd	%xmm4, %xmm0

	movlpd	%xmm0, (Y1)
	ALIGN_3

	jmp	.L999
	ALIGN_4

.L920:
	movapd	-1 * SIZE(BUFFER), %xmm4

	movq	TMP_M,   %rax
	sarq	$3,  %rax
	jle	.L924
	ALIGN_3

.L922:
#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE) * 4 + PREOFFSET(Y1)
#endif

	movapd	 0 * SIZE(Y1), %xmm0
	movapd	 2 * SIZE(Y1), %xmm1
	movapd	 4 * SIZE(Y1), %xmm2
	movapd	 6 * SIZE(Y1), %xmm3

	movapd	1 * SIZE(BUFFER), %xmm5
	movapd	3 * SIZE(BUFFER), %xmm6
	movapd	5 * SIZE(BUFFER), %xmm7
	movapd	7 * SIZE(BUFFER), %xmm8

	shufpd	 $1, %xmm5, %xmm4
	shufpd	 $1, %xmm6, %xmm5
	shufpd	 $1, %xmm7, %xmm6
	shufpd	 $1, %xmm8, %xmm7

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 4 + PREOFFSET(BUFFER)
#endif

	addpd	%xmm4, %xmm0
	addpd	%xmm5, %xmm1
	addpd	%xmm6, %xmm2
	addpd	%xmm7, %xmm3

	movapd	%xmm0,  0 * SIZE(Y1)
	movapd	%xmm1,  2 * SIZE(Y1)
	movapd	%xmm2,  4 * SIZE(Y1)
	movapd	%xmm3,  6 * SIZE(Y1)

	movapd	%xmm8, %xmm4

	addq	$8 * SIZE, Y1
	addq	$8 * SIZE, BUFFER

	decq	%rax
	jg	.L922
	ALIGN_3

.L924:
	testq	$7, TMP_M
	jle	.L999

	testq	$4, TMP_M
	jle	.L925

	movapd	 0 * SIZE(Y1), %xmm0
	movapd	 2 * SIZE(Y1), %xmm1

	movapd	1 * SIZE(BUFFER), %xmm5
	movapd	3 * SIZE(BUFFER), %xmm6

	shufpd	 $1, %xmm5, %xmm4
	shufpd	 $1, %xmm6, %xmm5

	addpd	%xmm4, %xmm0
	addpd	%xmm5, %xmm1

	movapd	%xmm0,  0 * SIZE(Y1)
	movapd	%xmm1,  2 * SIZE(Y1)

	movapd	%xmm6, %xmm4

	addq	$4 * SIZE, Y1
	addq	$4 * SIZE, BUFFER
	ALIGN_3

.L925:
	testq	$2, TMP_M
	jle	.L926

	movapd	 (Y1), %xmm0

	movapd	 1 * SIZE(BUFFER), %xmm5

	shufpd	 $1, %xmm5, %xmm4

	addpd	%xmm4, %xmm0

	movapd	%xmm0,  (Y1)

	movaps	%xmm5, %xmm4

	addq	$2 * SIZE, Y1
	addq	$2 * SIZE, BUFFER
	ALIGN_3

.L926:
	testq	$1, TMP_M
	jle	.L999

	movsd	(Y1), %xmm0

	shufpd	$1, %xmm4, %xmm4

	addsd	%xmm4, %xmm0

	movlpd	%xmm0, (Y1)
	ALIGN_3

	jmp	.L999
	ALIGN_4

.L950:
	testq	$SIZE, BUFFER
	je	.L960

	movsd	(Y1), %xmm0
	addsd	(BUFFER), %xmm0
	movsd	%xmm0, (Y1)

	addq	INCY, Y1
	addq	$SIZE, BUFFER

	decq	TMP_M
	jle	.L999
	ALIGN_4

.L960:
	movq	Y1,  Y2

	movq	TMP_M,   %rax
	sarq	$3,  %rax
	jle	.L964
	ALIGN_3

.L962:
	movsd	(Y2), %xmm0
	addq	INCY, Y2
	movhpd	(Y2), %xmm0
	addq	INCY, Y2

	movapd	0 * SIZE(BUFFER), %xmm4

	movsd	(Y2), %xmm1
	addq	INCY, Y2
	movhpd	(Y2), %xmm1
	addq	INCY, Y2

	movapd	2 * SIZE(BUFFER), %xmm5

	movsd	(Y2), %xmm2
	addq	INCY, Y2
	movhpd	(Y2), %xmm2
	addq	INCY, Y2

	movapd	4 * SIZE(BUFFER), %xmm6

	addpd	%xmm4, %xmm0

	movsd	(Y2), %xmm3
	addq	INCY, Y2
	movhpd	(Y2), %xmm3
	addq	INCY, Y2

	movapd	6 * SIZE(BUFFER), %xmm7

	addpd	%xmm5, %xmm1

	movlpd	%xmm0, (Y1)
	addq	INCY, Y1
	movhpd	%xmm0, (Y1)
	addq	INCY, Y1

	addpd	%xmm6, %xmm2

	movlpd	%xmm1, (Y1)
	addq	INCY, Y1
	movhpd	%xmm1, (Y1)
	addq	INCY, Y1

	addpd	%xmm7, %xmm3

	movlpd	%xmm2, (Y1)
	addq	INCY, Y1
	movhpd	%xmm2, (Y1)
	addq	INCY, Y1
	movlpd	%xmm3, (Y1)
	addq	INCY, Y1
	movhpd	%xmm3, (Y1)
	addq	INCY, Y1

	addq	$8 * SIZE, BUFFER
	decq	%rax
	jg	.L962
	ALIGN_3

.L964:
	testq	$7, TMP_M
	jle	.L999

	testq	$4, TMP_M
	jle	.L965

	movsd	(Y2), %xmm0
	addq	INCY, Y2
	movhpd	(Y2), %xmm0
	addq	INCY, Y2

	movapd	0 * SIZE(BUFFER), %xmm4

	movsd	(Y2), %xmm1
	addq	INCY, Y2
	movhpd	(Y2), %xmm1
	addq	INCY, Y2

	movapd	2 * SIZE(BUFFER), %xmm5

	addpd	%xmm4, %xmm0
	addpd	%xmm5, %xmm1

	movlpd	%xmm0, (Y1)
	addq	INCY, Y1
	movhpd	%xmm0, (Y1)
	addq	INCY, Y1
	movlpd	%xmm1, (Y1)
	addq	INCY, Y1
	movhpd	%xmm1, (Y1)
	addq	INCY, Y1

	addq	$4 * SIZE, BUFFER
	ALIGN_3

.L965:
	testq	$2, TMP_M
	jle	.L966

	movsd	(Y2), %xmm0
	addq	INCY, Y2
	movhpd	(Y2), %xmm0
	addq	INCY, Y2

	movapd	0 * SIZE(BUFFER), %xmm4

	addpd	%xmm4, %xmm0

	movlpd	%xmm0, (Y1)
	addq	INCY, Y1
	movhpd	%xmm0, (Y1)
	addq	INCY, Y1

	addq	$2 * SIZE, BUFFER
	ALIGN_3

.L966:
	testq	$1, TMP_M
	jle	.L999

	movsd	(Y2), %xmm0

	movsd	0 * SIZE(BUFFER), %xmm4

	addsd	%xmm4, %xmm0

	movlpd	%xmm0, (Y1)
	ALIGN_3

.L999:
	leaq	(, M, SIZE), %rax
	addq %rax,AA
	movq	STACK_INCY, INCY
	imulq	INCY, %rax
	addq %rax, Y
	jmp .L0t
	ALIGN_4

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
