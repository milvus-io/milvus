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

#if GEMV_UNROLL < 4
#undef  GEMV_UNROLL
#define GEMV_UNROLL 4
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
#define XX	96(%rsp)
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
	movss	 %xmm0, ALPHA
#else
	movss	 %xmm3, ALPHA
#endif


	movq	M,MMM
	movq	A,AA
	movq	N,NN
	movq	LDA,LDAX
	movq	X,XX
	movq	STACK_Y,      Y
.L0t:
	xorq	I,I
	addq	$1,I
	salq	$22,I
	subq	I,MMM
	movq	I,M
	jge	.L00t

	movq	MMM,M
	addq	I,M
	jle	.L999x

.L00t:
	movq	AA,A
	movq	NN,N
	movq	LDAX,LDA
	movq	XX,X

	movq	STACK_INCX,   INCX
	movq	STACK_INCY,   INCY
	movq	STACK_BUFFER, BUFFER

	leaq	(,INCX, SIZE), INCX
	leaq	(,INCY, SIZE), INCY
	leaq	(,LDA,  SIZE), LDA

	leaq	(LDA, LDA, 2), LDA3

#ifdef ALIGNED_ACCESS
	movq	M, MM
	testq	$4 * SIZE - 1, A
	je	.L0X
	cmpq	$3, M
	jle	.L0X

	movq	A, MM
	sarq	$BASE_SHIFT, MM
	andq	$3, MM
	subq	$4, MM
	addq	M,  MM

.L0X:
#endif

	testq	N, N		# if n <= 0 goto END
	jle	.L999
	testq	M, M		# if n <= 0 goto END
	jle	.L999

	subq	$-32 * SIZE, A

	movq	BUFFER, Y1

	pxor	%xmm0, %xmm0

	movq	M,   %rax
#ifdef ALIGNED_ACCESS
	addq	$19, %rax
#else
	addq	$16, %rax
#endif
	sarq	$4,  %rax
	ALIGN_3

.L01:
	movaps	%xmm0,  0 * SIZE(Y1)
	movaps	%xmm0,  4 * SIZE(Y1)
	movaps	%xmm0,  8 * SIZE(Y1)
	movaps	%xmm0, 12 * SIZE(Y1)
	addq	$16 * SIZE, Y1
	decq	%rax
	jg	.L01
	ALIGN_3

.L10:
#ifdef ALIGNED_ACCESS
	movq	A,  %rax
	andq	$4 * SIZE - 1, %rax
	addq	%rax, BUFFER

	testq	$4 * SIZE - 1, LDA
	jne	.L100
#endif

#if GEMV_UNROLL >= 8

	cmpq	$8, N
	jl	.L20
	ALIGN_3

.L11:
	subq	$8, N

	leaq	32 * SIZE(BUFFER), Y1
	movq	A,  A1
	leaq	(A,  LDA, 4), A2
	leaq	(A,  LDA, 8), A

	movss	(X), %xmm8
	addq	INCX, X
	movss	(X), %xmm9
	addq	INCX, X
	movss	(X), %xmm10
	addq	INCX, X
	movss	(X), %xmm11
	addq	INCX, X
	movss	(X), %xmm12
	addq	INCX, X
	movss	(X), %xmm13
	addq	INCX, X
	movss	(X), %xmm14
	addq	INCX, X
	movss	(X), %xmm15
	addq	INCX, X

	movss	ALPHA, %xmm0

	mulss	%xmm0, %xmm8
	shufps	$0, %xmm8,  %xmm8
	mulss	%xmm0, %xmm9
	shufps	$0, %xmm9,  %xmm9
	mulss	%xmm0, %xmm10
	shufps	$0, %xmm10, %xmm10
	mulss	%xmm0, %xmm11
	shufps	$0, %xmm11, %xmm11

	mulss	%xmm0, %xmm12
	shufps	$0, %xmm12, %xmm12
	mulss	%xmm0, %xmm13
	shufps	$0, %xmm13, %xmm13
	mulss	%xmm0, %xmm14
	shufps	$0, %xmm14, %xmm14
	mulss	%xmm0, %xmm15
	shufps	$0, %xmm15, %xmm15

#ifdef ALIGNED_ACCESS
	cmpq	$3, M
	jle	.L17

	testq	$SIZE, A1
	je	.L1X

	movss	 -32 * SIZE(A1), %xmm4
	movss	 -32 * SIZE(A1, LDA,  1), %xmm5
	movss	 -32 * SIZE(A1, LDA,  2), %xmm6
	movss	 -32 * SIZE(A1, LDA3, 1), %xmm7

	movss	 -32 * SIZE(Y1), %xmm0

	mulss	 %xmm8,  %xmm4
	addss	 %xmm4,  %xmm0
	movss	 -32 * SIZE(A2), %xmm4
	mulss	 %xmm9,  %xmm5
	addss	 %xmm5,  %xmm0
	movss	 -32 * SIZE(A2, LDA,  1), %xmm5
	mulss	 %xmm10, %xmm6
	addss	 %xmm6,  %xmm0
	movss	 -32 * SIZE(A2, LDA,  2), %xmm6
	mulss	 %xmm11, %xmm7
	addss	 %xmm7,  %xmm0
	movss	 -32 * SIZE(A2, LDA3, 1), %xmm7

	mulss	 %xmm12, %xmm4
	addss	 %xmm4,  %xmm0
	mulss	 %xmm13, %xmm5
	addss	 %xmm5,  %xmm0
	mulss	 %xmm14, %xmm6
	addss	 %xmm6,  %xmm0
	mulss	 %xmm15, %xmm7
	addss	 %xmm7,  %xmm0

	movss	 %xmm0, -32 * SIZE(Y1)

	addq	 $1 * SIZE, A1
	addq	 $1 * SIZE, A2
	addq	 $1 * SIZE, Y1
	ALIGN_3

.L1X:
	testq	$2 * SIZE, A1
	je	.L1XX

	movsd	 -32 * SIZE(A1), %xmm4
	movsd	 -32 * SIZE(A1, LDA,  1), %xmm5
	movsd	 -32 * SIZE(A1, LDA,  2), %xmm6
	movsd	 -32 * SIZE(A1, LDA3, 1), %xmm7

	movsd	 -32 * SIZE(Y1), %xmm0

	mulps	 %xmm8,  %xmm4
	addps	 %xmm4,  %xmm0
	movsd	 -32 * SIZE(A2), %xmm4
	mulps	 %xmm9,  %xmm5
	addps	 %xmm5,  %xmm0
	movsd	 -32 * SIZE(A2, LDA,  1), %xmm5
	mulps	 %xmm10, %xmm6
	addps	 %xmm6,  %xmm0
	movsd	 -32 * SIZE(A2, LDA,  2), %xmm6
	mulps	 %xmm11, %xmm7
	addps	 %xmm7,  %xmm0
	movsd	 -32 * SIZE(A2, LDA3, 1), %xmm7

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	mulps	 %xmm13, %xmm5
	addps	 %xmm5,  %xmm0
	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm0
	mulps	 %xmm15, %xmm7
	addps	 %xmm7,  %xmm0

	movlps	 %xmm0, -32 * SIZE(Y1)

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, Y1
	ALIGN_3

.L1XX:
#endif
	movq	MM,  I
	sarq	$4,  I
	jle	.L15

	MOVUPS_A1 (-32 * SIZE, A1, %xmm4)
	MOVUPS_A1 (-28 * SIZE, A1, %xmm5)
	MOVUPS_A1 (-24 * SIZE, A1, %xmm6)
	MOVUPS_A1 (-20 * SIZE, A1, %xmm7)

	MOVUPS_YL1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-28 * SIZE, Y1, %xmm1)
	MOVUPS_YL1(-24 * SIZE, Y1, %xmm2)
	MOVUPS_YL1(-20 * SIZE, Y1, %xmm3)

	decq	 I
	jle	 .L14
	ALIGN_3

.L13:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A1)
#endif

	mulps	 %xmm8,  %xmm4
	addps	 %xmm4,  %xmm0
	MOVUPS_A2 (-32 * SIZE, A1, LDA,  1, %xmm4)
	mulps	 %xmm8,  %xmm5
	addps	 %xmm5,  %xmm1
	MOVUPS_A2 (-28 * SIZE, A1, LDA,  1, %xmm5)
	mulps	 %xmm8,  %xmm6
	addps	 %xmm6,  %xmm2
	MOVUPS_A2 (-24 * SIZE, A1, LDA,  1, %xmm6)
	mulps	 %xmm8,  %xmm7
	addps	 %xmm7,  %xmm3
	MOVUPS_A2 (-20 * SIZE, A1, LDA,  1, %xmm7)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A1, LDA)
#endif

	mulps	 %xmm9,  %xmm4
	addps	 %xmm4,  %xmm0
	MOVUPS_A2 (-32 * SIZE, A1, LDA,  2, %xmm4)
	mulps	 %xmm9,  %xmm5
	addps	 %xmm5,  %xmm1
	MOVUPS_A2 (-28 * SIZE, A1, LDA,  2, %xmm5)
	mulps	 %xmm9,  %xmm6
	addps	 %xmm6,  %xmm2
	MOVUPS_A2 (-24 * SIZE, A1, LDA,  2, %xmm6)
	mulps	 %xmm9,  %xmm7
	addps	 %xmm7,  %xmm3
	MOVUPS_A2 (-20 * SIZE, A1, LDA,  2, %xmm7)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A1, LDA, 2)
#endif

	mulps	 %xmm10, %xmm4
	addps	 %xmm4,  %xmm0
	MOVUPS_A2 (-32 * SIZE, A1, LDA3, 1, %xmm4)
	mulps	 %xmm10, %xmm5
	addps	 %xmm5,  %xmm1
	MOVUPS_A2 (-28 * SIZE, A1, LDA3, 1, %xmm5)
	mulps	 %xmm10, %xmm6
	addps	 %xmm6,  %xmm2
	MOVUPS_A2 (-24 * SIZE, A1, LDA3, 1, %xmm6)
	mulps	 %xmm10, %xmm7
	addps	 %xmm7,  %xmm3
	MOVUPS_A2 (-20 * SIZE, A1, LDA3, 1, %xmm7)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A1, LDA3)
#endif

	mulps	 %xmm11, %xmm4
	addps	 %xmm4,  %xmm0
	MOVUPS_A1 (-32 * SIZE, A2, %xmm4)
	mulps	 %xmm11, %xmm5
	addps	 %xmm5,  %xmm1
	MOVUPS_A1 (-28 * SIZE, A2, %xmm5)
	mulps	 %xmm11, %xmm6
	addps	 %xmm6,  %xmm2
	MOVUPS_A1 (-24 * SIZE, A2, %xmm6)
	mulps	 %xmm11, %xmm7
	addps	 %xmm7,  %xmm3
	MOVUPS_A1 (-20 * SIZE, A2, %xmm7)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A2)
#endif

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	MOVUPS_A2 (-32 * SIZE, A2, LDA,  1, %xmm4)
	mulps	 %xmm12, %xmm5
	addps	 %xmm5,  %xmm1
	MOVUPS_A2 (-28 * SIZE, A2, LDA,  1, %xmm5)
	mulps	 %xmm12, %xmm6
	addps	 %xmm6,  %xmm2
	MOVUPS_A2 (-24 * SIZE, A2, LDA,  1, %xmm6)
	mulps	 %xmm12, %xmm7
	addps	 %xmm7,  %xmm3
	MOVUPS_A2 (-20 * SIZE, A2, LDA,  1, %xmm7)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A2, LDA)
#endif

	mulps	 %xmm13, %xmm4
	addps	 %xmm4,  %xmm0
	MOVUPS_A2 (-32 * SIZE, A2, LDA,  2, %xmm4)
	mulps	 %xmm13, %xmm5
	addps	 %xmm5,  %xmm1
	MOVUPS_A2 (-28 * SIZE, A2, LDA,  2, %xmm5)
	mulps	 %xmm13, %xmm6
	addps	 %xmm6,  %xmm2
	MOVUPS_A2 (-24 * SIZE, A2, LDA,  2, %xmm6)
	mulps	 %xmm13, %xmm7
	addps	 %xmm7,  %xmm3
	MOVUPS_A2 (-20 * SIZE, A2, LDA,  2, %xmm7)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A2, LDA, 2)
#endif

	mulps	 %xmm14, %xmm4
	addps	 %xmm4,  %xmm0
	MOVUPS_A2 (-32 * SIZE, A2, LDA3, 1, %xmm4)
	mulps	 %xmm14, %xmm5
	addps	 %xmm5,  %xmm1
	MOVUPS_A2 (-28 * SIZE, A2, LDA3, 1, %xmm5)
	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm2
	MOVUPS_A2 (-24 * SIZE, A2, LDA3, 1, %xmm6)
	mulps	 %xmm14, %xmm7
	addps	 %xmm7,  %xmm3
	MOVUPS_A2 (-20 * SIZE, A2, LDA3, 1, %xmm7)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A2, LDA3)
#endif

	mulps	 %xmm15, %xmm4
	addps	 %xmm4,  %xmm0
	MOVUPS_A1 (-16 * SIZE, A1, %xmm4)
	mulps	 %xmm15, %xmm5
	addps	 %xmm5,  %xmm1
	MOVUPS_A1 (-12 * SIZE, A1, %xmm5)
	mulps	 %xmm15, %xmm6
	addps	 %xmm6,  %xmm2
	MOVUPS_A1 ( -8 * SIZE, A1, %xmm6)
	mulps	 %xmm15, %xmm7
	addps	 %xmm7,  %xmm3
	MOVUPS_A1 ( -4 * SIZE, A1, %xmm7)

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE) - 128 + PREOFFSET(Y1)
#endif

	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-28 * SIZE, Y1, %xmm1)
	MOVUPS_YS1(-24 * SIZE, Y1, %xmm2)
	MOVUPS_YS1(-20 * SIZE, Y1, %xmm3)

	MOVUPS_YL1(-16 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-12 * SIZE, Y1, %xmm1)
	MOVUPS_YL1( -8 * SIZE, Y1, %xmm2)
	MOVUPS_YL1( -4 * SIZE, Y1, %xmm3)

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, A2
	subq	 $-16 * SIZE, Y1

	subq	 $1, I
	BRANCH
	jg	.L13
	ALIGN_3

.L14:
	mulps	 %xmm8,  %xmm4
	addps	 %xmm4,  %xmm0
	MOVUPS_A2 (-32 * SIZE, A1, LDA,  1, %xmm4)
	mulps	 %xmm8,  %xmm5
	addps	 %xmm5,  %xmm1
	MOVUPS_A2 (-28 * SIZE, A1, LDA,  1, %xmm5)
	mulps	 %xmm8,  %xmm6
	addps	 %xmm6,  %xmm2
	MOVUPS_A2 (-24 * SIZE, A1, LDA,  1, %xmm6)
	mulps	 %xmm8,  %xmm7
	addps	 %xmm7,  %xmm3
	MOVUPS_A2 (-20 * SIZE, A1, LDA,  1, %xmm7)

	mulps	 %xmm9,  %xmm4
	addps	 %xmm4,  %xmm0
	MOVUPS_A2 (-32 * SIZE, A1, LDA,  2, %xmm4)
	mulps	 %xmm9,  %xmm5
	addps	 %xmm5,  %xmm1
	MOVUPS_A2 (-28 * SIZE, A1, LDA,  2, %xmm5)
	mulps	 %xmm9,  %xmm6
	addps	 %xmm6,  %xmm2
	MOVUPS_A2 (-24 * SIZE, A1, LDA,  2, %xmm6)
	mulps	 %xmm9,  %xmm7
	addps	 %xmm7,  %xmm3
	MOVUPS_A2 (-20 * SIZE, A1, LDA,  2, %xmm7)

	mulps	 %xmm10, %xmm4
	addps	 %xmm4,  %xmm0
	MOVUPS_A2 (-32 * SIZE, A1, LDA3, 1, %xmm4)
	mulps	 %xmm10, %xmm5
	addps	 %xmm5,  %xmm1
	MOVUPS_A2 (-28 * SIZE, A1, LDA3, 1, %xmm5)
	mulps	 %xmm10, %xmm6
	addps	 %xmm6,  %xmm2
	MOVUPS_A2 (-24 * SIZE, A1, LDA3, 1, %xmm6)
	mulps	 %xmm10, %xmm7
	addps	 %xmm7,  %xmm3
	MOVUPS_A2 (-20 * SIZE, A1, LDA3, 1, %xmm7)

	mulps	 %xmm11, %xmm4
	addps	 %xmm4,  %xmm0
	MOVUPS_A1 (-32 * SIZE, A2, %xmm4)
	mulps	 %xmm11, %xmm5
	addps	 %xmm5,  %xmm1
	MOVUPS_A1 (-28 * SIZE, A2, %xmm5)
	mulps	 %xmm11, %xmm6
	addps	 %xmm6,  %xmm2
	MOVUPS_A1 (-24 * SIZE, A2, %xmm6)
	mulps	 %xmm11, %xmm7
	addps	 %xmm7,  %xmm3
	MOVUPS_A1 (-20 * SIZE, A2, %xmm7)

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	MOVUPS_A2 (-32 * SIZE, A2, LDA,  1, %xmm4)
	mulps	 %xmm12, %xmm5
	addps	 %xmm5,  %xmm1
	MOVUPS_A2 (-28 * SIZE, A2, LDA,  1, %xmm5)
	mulps	 %xmm12, %xmm6
	addps	 %xmm6,  %xmm2
	MOVUPS_A2 (-24 * SIZE, A2, LDA,  1, %xmm6)
	mulps	 %xmm12, %xmm7
	addps	 %xmm7,  %xmm3
	MOVUPS_A2 (-20 * SIZE, A2, LDA,  1, %xmm7)

	mulps	 %xmm13, %xmm4
	addps	 %xmm4,  %xmm0
	MOVUPS_A2 (-32 * SIZE, A2, LDA,  2, %xmm4)
	mulps	 %xmm13, %xmm5
	addps	 %xmm5,  %xmm1
	MOVUPS_A2 (-28 * SIZE, A2, LDA,  2, %xmm5)
	mulps	 %xmm13, %xmm6
	addps	 %xmm6,  %xmm2
	MOVUPS_A2 (-24 * SIZE, A2, LDA,  2, %xmm6)
	mulps	 %xmm13, %xmm7
	addps	 %xmm7,  %xmm3
	MOVUPS_A2 (-20 * SIZE, A2, LDA,  2, %xmm7)

	mulps	 %xmm14, %xmm4
	addps	 %xmm4,  %xmm0
	MOVUPS_A2 (-32 * SIZE, A2, LDA3, 1, %xmm4)
	mulps	 %xmm14, %xmm5
	addps	 %xmm5,  %xmm1
	MOVUPS_A2 (-28 * SIZE, A2, LDA3, 1, %xmm5)
	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm2
	MOVUPS_A2 (-24 * SIZE, A2, LDA3, 1, %xmm6)
	mulps	 %xmm14, %xmm7
	addps	 %xmm7,  %xmm3
	MOVUPS_A2 (-20 * SIZE, A2, LDA3, 1, %xmm7)

	mulps	 %xmm15, %xmm4
	addps	 %xmm4,  %xmm0
	mulps	 %xmm15, %xmm5
	addps	 %xmm5,  %xmm1
	mulps	 %xmm15, %xmm6
	addps	 %xmm6,  %xmm2
	mulps	 %xmm15, %xmm7
	addps	 %xmm7,  %xmm3

	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-28 * SIZE, Y1, %xmm1)
	MOVUPS_YS1(-24 * SIZE, Y1, %xmm2)
	MOVUPS_YS1(-20 * SIZE, Y1, %xmm3)

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, A2
	subq	 $-16 * SIZE, Y1
	ALIGN_3

.L15:
	testq	$8, MM
	je	.L16

	MOVUPS_A1 (-32 * SIZE, A1, %xmm4)
	MOVUPS_A1 (-28 * SIZE, A1, %xmm5)

	MOVUPS_A2 (-32 * SIZE, A1, LDA,  1, %xmm6)
	MOVUPS_A2 (-28 * SIZE, A1, LDA,  1, %xmm7)

	MOVUPS_YL1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-28 * SIZE, Y1, %xmm1)

	mulps	 %xmm8,  %xmm4
	addps	 %xmm4,  %xmm0
	MOVUPS_A2 (-32 * SIZE, A1, LDA,  2, %xmm4)
	mulps	 %xmm8,  %xmm5
	addps	 %xmm5,  %xmm1
	MOVUPS_A2 (-28 * SIZE, A1, LDA,  2, %xmm5)

	mulps	 %xmm9,  %xmm6
	addps	 %xmm6,  %xmm0
	MOVUPS_A2 (-32 * SIZE, A1, LDA3, 1, %xmm6)
	mulps	 %xmm9,  %xmm7
	addps	 %xmm7,  %xmm1
	MOVUPS_A2 (-28 * SIZE, A1, LDA3, 1, %xmm7)

	mulps	 %xmm10, %xmm4
	addps	 %xmm4,  %xmm0
	MOVUPS_A1 (-32 * SIZE, A2, %xmm4)
	mulps	 %xmm10, %xmm5
	addps	 %xmm5,  %xmm1
	MOVUPS_A1 (-28 * SIZE, A2, %xmm5)

	mulps	 %xmm11, %xmm6
	addps	 %xmm6,  %xmm0
	MOVUPS_A2 (-32 * SIZE, A2, LDA,  1, %xmm6)
	mulps	 %xmm11, %xmm7
	addps	 %xmm7,  %xmm1
	MOVUPS_A2 (-28 * SIZE, A2, LDA,  1, %xmm7)

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	MOVUPS_A2 (-32 * SIZE, A2, LDA,  2, %xmm4)
	mulps	 %xmm12, %xmm5
	addps	 %xmm5,  %xmm1
	MOVUPS_A2 (-28 * SIZE, A2, LDA,  2, %xmm5)

	mulps	 %xmm13, %xmm6
	addps	 %xmm6,  %xmm0
	MOVUPS_A2 (-32 * SIZE, A2, LDA3, 1, %xmm6)
	mulps	 %xmm13, %xmm7
	addps	 %xmm7,  %xmm1
	MOVUPS_A2 (-28 * SIZE, A2, LDA3, 1, %xmm7)

	mulps	 %xmm14, %xmm4
	addps	 %xmm4,  %xmm0
	mulps	 %xmm14, %xmm5
	addps	 %xmm5,  %xmm1

	mulps	 %xmm15, %xmm6
	addps	 %xmm6,  %xmm0
	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)
	mulps	 %xmm15, %xmm7
	addps	 %xmm7,  %xmm1
	MOVUPS_YS1(-28 * SIZE, Y1, %xmm1)

	addq	 $8 * SIZE, A1
	addq	 $8 * SIZE, A2
	addq	 $8 * SIZE, Y1
	ALIGN_3

.L16:
	testq	$4, MM
	je	.L17

	MOVUPS_A1 (-32 * SIZE, A1, %xmm4)
	MOVUPS_A2 (-32 * SIZE, A1, LDA,  1, %xmm5)
	MOVUPS_A2 (-32 * SIZE, A1, LDA,  2, %xmm6)
	MOVUPS_A2 (-32 * SIZE, A1, LDA3, 1, %xmm7)

	MOVUPS_YL1(-32 * SIZE, Y1, %xmm0)

	mulps	 %xmm8,  %xmm4
	addps	 %xmm4,  %xmm0
	MOVUPS_A1 (-32 * SIZE, A2, %xmm4)
	mulps	 %xmm9,  %xmm5
	addps	 %xmm5,  %xmm0
	MOVUPS_A2 (-32 * SIZE, A2, LDA,  1, %xmm5)
	mulps	 %xmm10, %xmm6
	addps	 %xmm6,  %xmm0
	MOVUPS_A2 (-32 * SIZE, A2, LDA,  2, %xmm6)
	mulps	 %xmm11, %xmm7
	addps	 %xmm7,  %xmm0
	MOVUPS_A2 (-32 * SIZE, A2, LDA3, 1, %xmm7)

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	mulps	 %xmm13, %xmm5
	addps	 %xmm5,  %xmm0
	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm0
	mulps	 %xmm15, %xmm7
	addps	 %xmm7,  %xmm0

	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)

	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, A2
	addq	 $4 * SIZE, Y1
	ALIGN_3

.L17:
	testq	$2, MM
	je	.L18

	movsd	 -32 * SIZE(A1), %xmm4
	movsd	 -32 * SIZE(A1, LDA,  1), %xmm5
	movsd	 -32 * SIZE(A1, LDA,  2), %xmm6
	movsd	 -32 * SIZE(A1, LDA3, 1), %xmm7

	movsd	 -32 * SIZE(Y1), %xmm0

	mulps	 %xmm8,  %xmm4
	addps	 %xmm4,  %xmm0
	movsd	 -32 * SIZE(A2), %xmm4
	mulps	 %xmm9,  %xmm5
	addps	 %xmm5,  %xmm0
	movsd	 -32 * SIZE(A2, LDA,  1), %xmm5
	mulps	 %xmm10, %xmm6
	addps	 %xmm6,  %xmm0
	movsd	 -32 * SIZE(A2, LDA,  2), %xmm6
	mulps	 %xmm11, %xmm7
	addps	 %xmm7,  %xmm0
	movsd	 -32 * SIZE(A2, LDA3, 1), %xmm7

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	mulps	 %xmm13, %xmm5
	addps	 %xmm5,  %xmm0
	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm0
	mulps	 %xmm15, %xmm7
	addps	 %xmm7,  %xmm0

	movlps	 %xmm0, -32 * SIZE(Y1)

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, Y1
	ALIGN_3

.L18:
	testq	$1, MM
	je	.L19

	movss	 -32 * SIZE(A1), %xmm4
	movss	 -32 * SIZE(A1, LDA,  1), %xmm5
	movss	 -32 * SIZE(A1, LDA,  2), %xmm6
	movss	 -32 * SIZE(A1, LDA3, 1), %xmm7

	movss	 -32 * SIZE(Y1), %xmm0

	mulss	 %xmm8,  %xmm4
	addss	 %xmm4,  %xmm0
	movss	 -32 * SIZE(A2), %xmm4
	mulss	 %xmm9,  %xmm5
	addss	 %xmm5,  %xmm0
	movss	 -32 * SIZE(A2, LDA,  1), %xmm5
	mulss	 %xmm10, %xmm6
	addss	 %xmm6,  %xmm0
	movss	 -32 * SIZE(A2, LDA,  2), %xmm6
	mulss	 %xmm11, %xmm7
	addss	 %xmm7,  %xmm0
	movss	 -32 * SIZE(A2, LDA3, 1), %xmm7

	mulss	 %xmm12, %xmm4
	addss	 %xmm4,  %xmm0
	mulss	 %xmm13, %xmm5
	addss	 %xmm5,  %xmm0
	mulss	 %xmm14, %xmm6
	addss	 %xmm6,  %xmm0
	mulss	 %xmm15, %xmm7
	addss	 %xmm7,  %xmm0

	movss	 %xmm0, -32 * SIZE(Y1)
	ALIGN_3

.L19:
	cmpq	$8, N
	jge	.L11
	ALIGN_3

.L20:
#endif

	cmpq	$4, N
	jl	.L30

#if GEMV_UNROLL == 4
	ALIGN_3

.L21:
#endif
	subq	$4, N

	leaq	32 * SIZE(BUFFER), Y1
	movq	A,  A1
	leaq	(A,  LDA, 2), A2
	leaq	(A,  LDA, 4), A

	movss	(X), %xmm12
	addq	INCX, X
	movss	(X), %xmm13
	addq	INCX, X
	movss	(X), %xmm14
	addq	INCX, X
	movss	(X), %xmm15
	addq	INCX, X

	movss	ALPHA, %xmm0

	mulss	%xmm0, %xmm12
	mulss	%xmm0, %xmm13
	mulss	%xmm0, %xmm14
	mulss	%xmm0, %xmm15

	shufps	$0, %xmm12, %xmm12
	shufps	$0, %xmm13, %xmm13
	shufps	$0, %xmm14, %xmm14
	shufps	$0, %xmm15, %xmm15

#ifdef ALIGNED_ACCESS
	cmpq	$3, M
	jle	.L27

	testq	$SIZE, A1
	je	.L2X

	movss	 -32 * SIZE(A1), %xmm0
	movss	 -32 * SIZE(A1, LDA), %xmm1
	movss	 -32 * SIZE(A2), %xmm2
	movss	 -32 * SIZE(A2, LDA), %xmm3

	movss	 -32 * SIZE(Y1), %xmm8

	mulss	 %xmm12, %xmm0
	addss	 %xmm0,  %xmm8
	mulss	 %xmm13, %xmm1
	addss	 %xmm1,  %xmm8
	mulss	 %xmm14, %xmm2
	addss	 %xmm2, %xmm8
	mulss	 %xmm15, %xmm3
	addss	 %xmm3, %xmm8

	movss	 %xmm8, -32 * SIZE(Y1)

	addq	 $1 * SIZE, A1
	addq	 $1 * SIZE, A2
	addq	 $1 * SIZE, Y1
	ALIGN_3

.L2X:
	testq	$2 * SIZE, A1
	je	.L2XX

	movsd	 -32 * SIZE(A1), %xmm0
	movsd	 -32 * SIZE(A1, LDA), %xmm1
	movsd	 -32 * SIZE(A2), %xmm2
	movsd	 -32 * SIZE(A2, LDA), %xmm3

	movsd	 -32 * SIZE(Y1), %xmm8

	mulps	 %xmm12, %xmm0
	addps	 %xmm0,  %xmm8
	mulps	 %xmm13, %xmm1
	addps	 %xmm1,  %xmm8
	mulps	 %xmm14, %xmm2
	addps	 %xmm2, %xmm8
	mulps	 %xmm15, %xmm3
	addps	 %xmm3, %xmm8

	movlps	 %xmm8, -32 * SIZE(Y1)

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, Y1
	ALIGN_3

.L2XX:
#endif

	movq	MM,  I
	sarq	$4,  I
	jle	.L25

	MOVUPS_A1 (-32 * SIZE, A1, %xmm8)
	MOVUPS_A1 (-28 * SIZE, A1, %xmm9)
	MOVUPS_A1 (-24 * SIZE, A1, %xmm10)
	MOVUPS_A1 (-20 * SIZE, A1, %xmm11)

	MOVUPS_YL1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-28 * SIZE, Y1, %xmm1)
	MOVUPS_YL1(-24 * SIZE, Y1, %xmm2)
	MOVUPS_YL1(-20 * SIZE, Y1, %xmm3)

	MOVUPS_A2(-32 * SIZE, A1, LDA, 1, %xmm4)
	MOVUPS_A2(-28 * SIZE, A1, LDA, 1, %xmm5)
	MOVUPS_A2(-24 * SIZE, A1, LDA, 1, %xmm6)
	MOVUPS_A2(-20 * SIZE, A1, LDA, 1, %xmm7)

	decq	 I
	jle	 .L24
	ALIGN_3

.L23:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(A1)
#endif

	mulps	 %xmm12, %xmm8
	addps	 %xmm8,  %xmm0
	MOVUPS_A1(-32 * SIZE, A2, %xmm8)
	mulps	 %xmm12, %xmm9
	addps	 %xmm9,  %xmm1
	MOVUPS_A1(-28 * SIZE, A2, %xmm9)
	mulps	 %xmm12, %xmm10
	addps	 %xmm10,  %xmm2
	MOVUPS_A1(-24 * SIZE, A2, %xmm10)
	mulps	 %xmm12, %xmm11
	addps	 %xmm11,  %xmm3
	MOVUPS_A1(-20 * SIZE, A2, %xmm11)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(A1, LDA)
#endif

	mulps	 %xmm13, %xmm4
	addps	 %xmm4,  %xmm0
	MOVUPS_A2(-32 * SIZE, A2, LDA, 1, %xmm4)
	mulps	 %xmm13, %xmm5
	addps	 %xmm5,  %xmm1
	MOVUPS_A2(-28 * SIZE, A2, LDA, 1, %xmm5)
	mulps	 %xmm13, %xmm6
	addps	 %xmm6,  %xmm2
	MOVUPS_A2(-24 * SIZE, A2, LDA, 1, %xmm6)
	mulps	 %xmm13, %xmm7
	addps	 %xmm7,  %xmm3
	MOVUPS_A2(-20 * SIZE, A2, LDA, 1, %xmm7)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(A2)
#endif

	mulps	 %xmm14, %xmm8
	addps	 %xmm8,  %xmm0
	MOVUPS_A1 (-16 * SIZE, A1, %xmm8)
	mulps	 %xmm14, %xmm9
	addps	 %xmm9,  %xmm1
	MOVUPS_A1 (-12 * SIZE, A1, %xmm9)
	mulps	 %xmm14, %xmm10
	addps	 %xmm10,  %xmm2
	MOVUPS_A1 ( -8 * SIZE, A1, %xmm10)
	mulps	 %xmm14, %xmm11
	addps	 %xmm11,  %xmm3
	MOVUPS_A1 ( -4 * SIZE, A1, %xmm11)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(A2, LDA)
#endif

	mulps	 %xmm15, %xmm4
	addps	 %xmm4,  %xmm0
	MOVUPS_A2(-16 * SIZE, A1, LDA, 1, %xmm4)
	mulps	 %xmm15, %xmm5
	addps	 %xmm5,  %xmm1
	MOVUPS_A2(-12 * SIZE, A1, LDA, 1, %xmm5)
	mulps	 %xmm15, %xmm6
	addps	 %xmm6,  %xmm2
	MOVUPS_A2( -8 * SIZE, A1, LDA, 1, %xmm6)
	mulps	 %xmm15, %xmm7
	addps	 %xmm7,  %xmm3
	MOVUPS_A2( -4 * SIZE, A1, LDA, 1, %xmm7)

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(Y1)
#endif

	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-28 * SIZE, Y1, %xmm1)
	MOVUPS_YS1(-24 * SIZE, Y1, %xmm2)
	MOVUPS_YS1(-20 * SIZE, Y1, %xmm3)

	MOVUPS_YL1(-16 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-12 * SIZE, Y1, %xmm1)
	MOVUPS_YL1( -8 * SIZE, Y1, %xmm2)
	MOVUPS_YL1( -4 * SIZE, Y1, %xmm3)

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, A2
	subq	 $-16 * SIZE, Y1

	subq	 $1, I
	BRANCH
	jg	.L23
	ALIGN_3

.L24:
	mulps	 %xmm12, %xmm8
	addps	 %xmm8,  %xmm0
	MOVUPS_A1(-32 * SIZE, A2, %xmm8)
	mulps	 %xmm12, %xmm9
	addps	 %xmm9,  %xmm1
	MOVUPS_A1(-28 * SIZE, A2, %xmm9)
	mulps	 %xmm12, %xmm10
	addps	 %xmm10,  %xmm2
	MOVUPS_A1(-24 * SIZE, A2, %xmm10)
	mulps	 %xmm12, %xmm11
	addps	 %xmm11,  %xmm3
	MOVUPS_A1(-20 * SIZE, A2, %xmm11)

	mulps	 %xmm13, %xmm4
	addps	 %xmm4,  %xmm0
	MOVUPS_A2(-32 * SIZE, A2, LDA, 1, %xmm4)
	mulps	 %xmm13, %xmm5
	addps	 %xmm5,  %xmm1
	MOVUPS_A2(-28 * SIZE, A2, LDA, 1, %xmm5)
	mulps	 %xmm13, %xmm6
	addps	 %xmm6,  %xmm2
	MOVUPS_A2(-24 * SIZE, A2, LDA, 1, %xmm6)
	mulps	 %xmm13, %xmm7
	addps	 %xmm7,  %xmm3
	MOVUPS_A2(-20 * SIZE, A2, LDA, 1, %xmm7)

	mulps	 %xmm14, %xmm8
	addps	 %xmm8,  %xmm0
	mulps	 %xmm14, %xmm9
	addps	 %xmm9,  %xmm1
	mulps	 %xmm14, %xmm10
	addps	 %xmm10,  %xmm2
	mulps	 %xmm14, %xmm11
	addps	 %xmm11,  %xmm3

	mulps	 %xmm15, %xmm4
	addps	 %xmm4,  %xmm0
	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)
	mulps	 %xmm15, %xmm5
	addps	 %xmm5,  %xmm1
	MOVUPS_YS1(-28 * SIZE, Y1, %xmm1)
	mulps	 %xmm15, %xmm6
	addps	 %xmm6,  %xmm2
	MOVUPS_YS1(-24 * SIZE, Y1, %xmm2)
	mulps	 %xmm15, %xmm7
	addps	 %xmm7,  %xmm3
	MOVUPS_YS1(-20 * SIZE, Y1, %xmm3)

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, A2
	subq	 $-16 * SIZE, Y1
	ALIGN_3

.L25:
	testq	$8, MM
	je	.L26

	MOVUPS_A1 (-32 * SIZE, A1, %xmm8)
	MOVUPS_A1 (-28 * SIZE, A1, %xmm9)

	MOVUPS_YL1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-28 * SIZE, Y1, %xmm1)

	MOVUPS_A2(-32 * SIZE, A1, LDA, 1, %xmm4)
	MOVUPS_A2(-28 * SIZE, A1, LDA, 1, %xmm5)

	mulps	 %xmm12, %xmm8
	addps	 %xmm8,  %xmm0
	MOVUPS_A1(-32 * SIZE, A2, %xmm8)
	mulps	 %xmm12, %xmm9
	addps	 %xmm9,  %xmm1
	MOVUPS_A1(-28 * SIZE, A2, %xmm9)

	mulps	 %xmm13, %xmm4
	addps	 %xmm4,  %xmm0
	MOVUPS_A2(-32 * SIZE, A2, LDA, 1, %xmm4)
	mulps	 %xmm13, %xmm5
	addps	 %xmm5,  %xmm1
	MOVUPS_A2(-28 * SIZE, A2, LDA, 1, %xmm5)
	mulps	 %xmm13, %xmm6
	addps	 %xmm6,  %xmm2

	mulps	 %xmm14, %xmm8
	addps	 %xmm8,  %xmm0
	mulps	 %xmm14, %xmm9
	addps	 %xmm9,  %xmm1

	mulps	 %xmm15, %xmm4
	addps	 %xmm4,  %xmm0
	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)
	mulps	 %xmm15, %xmm5
	addps	 %xmm5,  %xmm1
	MOVUPS_YS1(-28 * SIZE, Y1, %xmm1)

	addq	 $8 * SIZE, A1
	addq	 $8 * SIZE, A2
	addq	 $8 * SIZE, Y1
	ALIGN_3

.L26:
	testq	$4, MM
	je	.L27

	MOVUPS_A1 (-32 * SIZE, A1, %xmm8)

	MOVUPS_YL1(-32 * SIZE, Y1, %xmm0)

	MOVUPS_A2(-32 * SIZE, A1, LDA, 1, %xmm4)

	mulps	 %xmm12, %xmm8
	addps	 %xmm8,  %xmm0
	MOVUPS_A1(-32 * SIZE, A2, %xmm8)

	mulps	 %xmm13, %xmm4
	addps	 %xmm4,  %xmm0
	MOVUPS_A2(-32 * SIZE, A2, LDA, 1, %xmm4)

	mulps	 %xmm14, %xmm8
	addps	 %xmm8,  %xmm0

	mulps	 %xmm15, %xmm4
	addps	 %xmm4,  %xmm0
	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)

	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, A2
	addq	 $4 * SIZE, Y1
	ALIGN_3

.L27:
	testq	$2, MM
	je	.L28

	movsd	 -32 * SIZE(A1), %xmm0
	movsd	 -32 * SIZE(A1, LDA), %xmm1
	movsd	 -32 * SIZE(A2), %xmm2
	movsd	 -32 * SIZE(A2, LDA), %xmm3

	movsd	 -32 * SIZE(Y1), %xmm8

	mulps	 %xmm12, %xmm0
	addps	 %xmm0,  %xmm8
	mulps	 %xmm13, %xmm1
	addps	 %xmm1,  %xmm8
	mulps	 %xmm14, %xmm2
	addps	 %xmm2, %xmm8
	mulps	 %xmm15, %xmm3
	addps	 %xmm3, %xmm8

	movlps	 %xmm8, -32 * SIZE(Y1)

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, Y1
	ALIGN_3

.L28:
	testq	$1, MM
#if GEMV_UNROLL == 4
	je	.L29
#else
	je	.L30
#endif

	movss	 -32 * SIZE(Y1), %xmm8

	movss	 -32 * SIZE(A1), %xmm0
	movss	 -32 * SIZE(A1, LDA), %xmm1
	movss	 -32 * SIZE(A2), %xmm2
	movss	 -32 * SIZE(A2, LDA), %xmm3

	mulss	 %xmm12, %xmm0
	addss	 %xmm0,  %xmm8
	mulss	 %xmm13, %xmm1
	addss	 %xmm1,  %xmm8
	mulss	 %xmm14, %xmm2
	addss	 %xmm2, %xmm8
	mulss	 %xmm15, %xmm3
	addss	 %xmm3, %xmm8

	movss	 %xmm8, -32 * SIZE(Y1)
	ALIGN_3

#if GEMV_UNROLL == 4
.L29:
	cmpq	$4, N
	jge	.L21
#endif
	ALIGN_3

.L30:
	testq	N, N
	jle	.L990

	cmpq	$3, N
	jne	.L40

	leaq	32 * SIZE(BUFFER), Y1
	movq	A,  A1
	leaq	(A,  LDA, 2), A2

	movss	(X), %xmm12
	addq	INCX, X
	movss	(X), %xmm13
	addq	INCX, X
	movss	(X), %xmm14
	addq	INCX, X

	movss	ALPHA, %xmm0

	mulss	%xmm0, %xmm12
	mulss	%xmm0, %xmm13
	mulss	%xmm0, %xmm14

	shufps	$0, %xmm12, %xmm12
	shufps	$0, %xmm13, %xmm13
	shufps	$0, %xmm14, %xmm14

#ifdef ALIGNED_ACCESS
	cmpq	$3, M
	jle	.L37

	testq	$SIZE, A1
	je	.L3X

	movss	 -32 * SIZE(A1), %xmm0
	movss	 -32 * SIZE(A1, LDA), %xmm1
	movss	 -32 * SIZE(A2), %xmm2

	movss	 -32 * SIZE(Y1), %xmm8

	mulss	 %xmm12, %xmm0
	addss	 %xmm0,  %xmm8
	mulss	 %xmm13, %xmm1
	addss	 %xmm1,  %xmm8
	mulss	 %xmm14, %xmm2
	addss	 %xmm2, %xmm8

	movss	 %xmm8, -32 * SIZE(Y1)

	addq	 $1 * SIZE, A1
	addq	 $1 * SIZE, A2
	addq	 $1 * SIZE, Y1
	ALIGN_3

.L3X:
	testq	$2 * SIZE, A1
	je	.L3XX

	movsd	 -32 * SIZE(A1), %xmm0
	movsd	 -32 * SIZE(A1, LDA), %xmm1
	movsd	 -32 * SIZE(A2), %xmm2

	movsd	 -32 * SIZE(Y1), %xmm8

	mulps	 %xmm12, %xmm0
	addps	 %xmm0,  %xmm8
	mulps	 %xmm13, %xmm1
	addps	 %xmm1,  %xmm8
	mulps	 %xmm14, %xmm2
	addps	 %xmm2, %xmm8

	movlps	 %xmm8, -32 * SIZE(Y1)

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, Y1
	ALIGN_3

.L3XX:
#endif

	movq	MM,  I
	sarq	$4,  I
	jle	.L35

	MOVUPS_A1 (-32 * SIZE, A1, %xmm8)
	MOVUPS_A1 (-28 * SIZE, A1, %xmm9)
	MOVUPS_A1 (-24 * SIZE, A1, %xmm10)
	MOVUPS_A1 (-20 * SIZE, A1, %xmm11)

	MOVUPS_YL1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-28 * SIZE, Y1, %xmm1)
	MOVUPS_YL1(-24 * SIZE, Y1, %xmm2)
	MOVUPS_YL1(-20 * SIZE, Y1, %xmm3)

	MOVUPS_A2(-32 * SIZE, A1, LDA, 1, %xmm4)
	MOVUPS_A2(-28 * SIZE, A1, LDA, 1, %xmm5)
	MOVUPS_A2(-24 * SIZE, A1, LDA, 1, %xmm6)
	MOVUPS_A2(-20 * SIZE, A1, LDA, 1, %xmm7)

	decq	 I
	jle	 .L34
	ALIGN_3

.L33:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 3 - 128 + PREOFFSET(A1)
#endif

	mulps	 %xmm12, %xmm8
	addps	 %xmm8,  %xmm0
	MOVUPS_A1(-32 * SIZE, A2, %xmm8)
	mulps	 %xmm12, %xmm9
	addps	 %xmm9,  %xmm1
	MOVUPS_A1(-28 * SIZE, A2, %xmm9)
	mulps	 %xmm12, %xmm10
	addps	 %xmm10,  %xmm2
	MOVUPS_A1(-24 * SIZE, A2, %xmm10)
	mulps	 %xmm12, %xmm11
	addps	 %xmm11,  %xmm3
	MOVUPS_A1(-20 * SIZE, A2, %xmm11)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 3 - 128 + PREOFFSET(A1, LDA)
#endif

	mulps	 %xmm13, %xmm4
	addps	 %xmm4,  %xmm0
	MOVUPS_A2(-16 * SIZE, A1, LDA, 1, %xmm4)
	mulps	 %xmm13, %xmm5
	addps	 %xmm5,  %xmm1
	MOVUPS_A2(-12 * SIZE, A1, LDA, 1, %xmm5)
	mulps	 %xmm13, %xmm6
	addps	 %xmm6,  %xmm2
	MOVUPS_A2( -8 * SIZE, A1, LDA, 1, %xmm6)
	mulps	 %xmm13, %xmm7
	addps	 %xmm7,  %xmm3
	MOVUPS_A2( -4 * SIZE, A1, LDA, 1, %xmm7)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 3 - 128 + PREOFFSET(A2)
#endif

	mulps	 %xmm14, %xmm8
	addps	 %xmm8,  %xmm0
	MOVUPS_A1 (-16 * SIZE, A1, %xmm8)
	mulps	 %xmm14, %xmm9
	addps	 %xmm9,  %xmm1
	MOVUPS_A1 (-12 * SIZE, A1, %xmm9)
	mulps	 %xmm14, %xmm10
	addps	 %xmm10,  %xmm2
	MOVUPS_A1 ( -8 * SIZE, A1, %xmm10)
	mulps	 %xmm14, %xmm11
	addps	 %xmm11,  %xmm3
	MOVUPS_A1 ( -4 * SIZE, A1, %xmm11)

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE) * 3 - 128 + PREOFFSET(Y1)
#endif

	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-28 * SIZE, Y1, %xmm1)
	MOVUPS_YS1(-24 * SIZE, Y1, %xmm2)
	MOVUPS_YS1(-20 * SIZE, Y1, %xmm3)

	MOVUPS_YL1(-16 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-12 * SIZE, Y1, %xmm1)
	MOVUPS_YL1( -8 * SIZE, Y1, %xmm2)
	MOVUPS_YL1( -4 * SIZE, Y1, %xmm3)

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, A2
	subq	 $-16 * SIZE, Y1

	subq	 $1, I
	BRANCH
	jg	.L33
	ALIGN_3

.L34:
	mulps	 %xmm12, %xmm8
	addps	 %xmm8,  %xmm0
	MOVUPS_A1(-32 * SIZE, A2, %xmm8)
	mulps	 %xmm12, %xmm9
	addps	 %xmm9,  %xmm1
	MOVUPS_A1(-28 * SIZE, A2, %xmm9)
	mulps	 %xmm12, %xmm10
	addps	 %xmm10,  %xmm2
	MOVUPS_A1(-24 * SIZE, A2, %xmm10)
	mulps	 %xmm12, %xmm11
	addps	 %xmm11,  %xmm3
	MOVUPS_A1(-20 * SIZE, A2, %xmm11)

	mulps	 %xmm13, %xmm4
	addps	 %xmm4,  %xmm0
	mulps	 %xmm13, %xmm5
	addps	 %xmm5,  %xmm1
	mulps	 %xmm13, %xmm6
	addps	 %xmm6,  %xmm2
	mulps	 %xmm13, %xmm7
	addps	 %xmm7,  %xmm3

	mulps	 %xmm14, %xmm8
	addps	 %xmm8,  %xmm0
	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)
	mulps	 %xmm14, %xmm9
	addps	 %xmm9,  %xmm1
	MOVUPS_YS1(-28 * SIZE, Y1, %xmm1)
	mulps	 %xmm14, %xmm10
	addps	 %xmm10,  %xmm2
	MOVUPS_YS1(-24 * SIZE, Y1, %xmm2)
	mulps	 %xmm14, %xmm11
	addps	 %xmm11,  %xmm3
	MOVUPS_YS1(-20 * SIZE, Y1, %xmm3)

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, A2
	subq	 $-16 * SIZE, Y1
	ALIGN_3

.L35:
	testq	$8, MM
	je	.L36

	MOVUPS_A1 (-32 * SIZE, A1, %xmm8)
	MOVUPS_A1 (-28 * SIZE, A1, %xmm9)

	MOVUPS_YL1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-28 * SIZE, Y1, %xmm1)

	MOVUPS_A2(-32 * SIZE, A1, LDA, 1, %xmm4)
	MOVUPS_A2(-28 * SIZE, A1, LDA, 1, %xmm5)

	mulps	 %xmm12, %xmm8
	addps	 %xmm8,  %xmm0
	MOVUPS_A1(-32 * SIZE, A2, %xmm8)
	mulps	 %xmm12, %xmm9
	addps	 %xmm9,  %xmm1
	MOVUPS_A1(-28 * SIZE, A2, %xmm9)

	mulps	 %xmm13, %xmm4
	addps	 %xmm4,  %xmm0
	mulps	 %xmm13, %xmm5
	addps	 %xmm5,  %xmm1
	mulps	 %xmm13, %xmm6
	addps	 %xmm6,  %xmm2

	mulps	 %xmm14, %xmm8
	addps	 %xmm8,  %xmm0
	mulps	 %xmm14, %xmm9
	addps	 %xmm9,  %xmm1

	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-28 * SIZE, Y1, %xmm1)

	addq	 $8 * SIZE, A1
	addq	 $8 * SIZE, A2
	addq	 $8 * SIZE, Y1
	ALIGN_3

.L36:
	testq	$4, MM
	je	.L37

	MOVUPS_A1 (-32 * SIZE, A1, %xmm8)

	MOVUPS_YL1(-32 * SIZE, Y1, %xmm0)

	MOVUPS_A2(-32 * SIZE, A1, LDA, 1, %xmm4)

	mulps	 %xmm12, %xmm8
	addps	 %xmm8,  %xmm0
	MOVUPS_A1(-32 * SIZE, A2, %xmm8)

	mulps	 %xmm13, %xmm4
	addps	 %xmm4,  %xmm0

	mulps	 %xmm14, %xmm8
	addps	 %xmm8,  %xmm0

	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)

	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, A2
	addq	 $4 * SIZE, Y1
	ALIGN_3

.L37:
	testq	$2, MM
	je	.L38

	movsd	 -32 * SIZE(A1), %xmm0
	movsd	 -32 * SIZE(A1, LDA), %xmm1
	movsd	 -32 * SIZE(A2), %xmm2

	movsd	 -32 * SIZE(Y1), %xmm8

	mulps	 %xmm12, %xmm0
	addps	 %xmm0,  %xmm8
	mulps	 %xmm13, %xmm1
	addps	 %xmm1,  %xmm8
	mulps	 %xmm14, %xmm2
	addps	 %xmm2, %xmm8

	movlps	 %xmm8, -32 * SIZE(Y1)

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, Y1
	ALIGN_3

.L38:
	testq	$1, MM
	je	.L990

	movss	 -32 * SIZE(Y1), %xmm8

	movss	 -32 * SIZE(A1), %xmm0
	movss	 -32 * SIZE(A1, LDA), %xmm1
	movss	 -32 * SIZE(A2), %xmm2

	mulss	 %xmm12, %xmm0
	addss	 %xmm0,  %xmm8
	mulss	 %xmm13, %xmm1
	addss	 %xmm1,  %xmm8
	mulss	 %xmm14, %xmm2
	addss	 %xmm2, %xmm8

	movss	 %xmm8, -32 * SIZE(Y1)
	jmp	 .L990
	ALIGN_3

.L40:
	cmpq	$2, N
	jne	.L50

	leaq	32 * SIZE(BUFFER), Y1
	movq	A,  A1
	leaq	(A,  LDA, 1), A2
	leaq	(A,  LDA, 2), A

	movss	(X), %xmm12
	addq	INCX, X
	movss	(X), %xmm13
	addq	INCX, X

	movss	ALPHA, %xmm0

	mulss	%xmm0, %xmm12
	mulss	%xmm0, %xmm13

	shufps	$0, %xmm12, %xmm12
	shufps	$0, %xmm13, %xmm13

#ifdef ALIGNED_ACCESS
	cmpq	$3, M
	jle	.L47

	testq	$SIZE, A1
	je	.L4X

	movss	 -32 * SIZE(Y1), %xmm8

	movss	 -32 * SIZE(A1), %xmm0
	movss	 -32 * SIZE(A2), %xmm1

	mulss	 %xmm12, %xmm0
	addss	 %xmm0,  %xmm8
	mulss	 %xmm13, %xmm1
	addss	 %xmm1,  %xmm8

	movss	 %xmm8, -32 * SIZE(Y1)

	addq	 $1 * SIZE, A1
	addq	 $1 * SIZE, A2
	addq	 $1 * SIZE, Y1
	ALIGN_3

.L4X:
	testq	$2 * SIZE, A1
	je	.L4XX

	movsd	 -32 * SIZE(A1), %xmm0
	movsd	 -32 * SIZE(A2), %xmm1

	movsd	 -32 * SIZE(Y1), %xmm8

	mulps	 %xmm12, %xmm0
	addps	 %xmm0,  %xmm8
	mulps	 %xmm13, %xmm1
	addps	 %xmm1,  %xmm8

	movlps	 %xmm8, -32 * SIZE(Y1)

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, Y1
	ALIGN_3

.L4XX:
#endif

	movq	MM,  I
	sarq	$4,  I
	jle	.L45

	MOVUPS_A1 (-32 * SIZE, A1, %xmm8)
	MOVUPS_A1 (-28 * SIZE, A1, %xmm9)
	MOVUPS_A1 (-24 * SIZE, A1, %xmm10)
	MOVUPS_A1 (-20 * SIZE, A1, %xmm11)

	MOVUPS_YL1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-28 * SIZE, Y1, %xmm1)
	MOVUPS_YL1(-24 * SIZE, Y1, %xmm2)
	MOVUPS_YL1(-20 * SIZE, Y1, %xmm3)

	MOVUPS_A1(-32 * SIZE, A2, %xmm4)
	MOVUPS_A1(-28 * SIZE, A2, %xmm5)
	MOVUPS_A1(-24 * SIZE, A2, %xmm6)
	MOVUPS_A1(-20 * SIZE, A2, %xmm7)

	decq	 I
	jle	 .L44
	ALIGN_3

.L43:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 4 - 128 + PREOFFSET(A1)
#endif

	mulps	 %xmm12, %xmm8
	addps	 %xmm8,  %xmm0
	MOVUPS_A1 (-16 * SIZE, A1, %xmm8)
	mulps	 %xmm12, %xmm9
	addps	 %xmm9,  %xmm1
	MOVUPS_A1 (-12 * SIZE, A1, %xmm9)
	mulps	 %xmm12, %xmm10
	addps	 %xmm10,  %xmm2
	MOVUPS_A1 ( -8 * SIZE, A1, %xmm10)
	mulps	 %xmm12, %xmm11
	addps	 %xmm11,  %xmm3
	MOVUPS_A1 ( -4 * SIZE, A1, %xmm11)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 4 - 128 + PREOFFSET(A2)
#endif

	mulps	 %xmm13, %xmm4
	addps	 %xmm4,  %xmm0
	MOVUPS_A1(-16 * SIZE, A2, %xmm4)
	mulps	 %xmm13, %xmm5
	addps	 %xmm5,  %xmm1
	MOVUPS_A1(-12 * SIZE, A2, %xmm5)
	mulps	 %xmm13, %xmm6
	addps	 %xmm6,  %xmm2
	MOVUPS_A1( -8 * SIZE, A2, %xmm6)
	mulps	 %xmm13, %xmm7
	addps	 %xmm7,  %xmm3
	MOVUPS_A1( -4 * SIZE, A2, %xmm7)

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE) * 4 - 128 + PREOFFSET(Y1)
#endif

	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-28 * SIZE, Y1, %xmm1)
	MOVUPS_YS1(-24 * SIZE, Y1, %xmm2)
	MOVUPS_YS1(-20 * SIZE, Y1, %xmm3)

	MOVUPS_YL1(-16 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-12 * SIZE, Y1, %xmm1)
	MOVUPS_YL1( -8 * SIZE, Y1, %xmm2)
	MOVUPS_YL1( -4 * SIZE, Y1, %xmm3)

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, A2
	subq	 $-16 * SIZE, Y1

	subq	 $1, I
	BRANCH
	jg	.L43
	ALIGN_3

.L44:
	mulps	 %xmm12, %xmm8
	addps	 %xmm8,  %xmm0
	mulps	 %xmm12, %xmm9
	addps	 %xmm9,  %xmm1
	mulps	 %xmm12, %xmm10
	addps	 %xmm10,  %xmm2
	mulps	 %xmm12, %xmm11
	addps	 %xmm11,  %xmm3

	mulps	 %xmm13, %xmm4
	addps	 %xmm4,  %xmm0
	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)
	mulps	 %xmm13, %xmm5
	addps	 %xmm5,  %xmm1
	MOVUPS_YS1(-28 * SIZE, Y1, %xmm1)
	mulps	 %xmm13, %xmm6
	addps	 %xmm6,  %xmm2
	MOVUPS_YS1(-24 * SIZE, Y1, %xmm2)
	mulps	 %xmm13, %xmm7
	addps	 %xmm7,  %xmm3
	MOVUPS_YS1(-20 * SIZE, Y1, %xmm3)

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, A2
	subq	 $-16 * SIZE, Y1
	ALIGN_3

.L45:
	testq	$8, MM
	je	.L46

	MOVUPS_A1 (-32 * SIZE, A1, %xmm8)
	MOVUPS_A1 (-28 * SIZE, A1, %xmm9)

	MOVUPS_YL1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-28 * SIZE, Y1, %xmm1)

	mulps	 %xmm12, %xmm8
	addps	 %xmm8,  %xmm0
	MOVUPS_A1(-32 * SIZE, A2, %xmm4)
	mulps	 %xmm12, %xmm9
	addps	 %xmm9,  %xmm1
	MOVUPS_A1(-28 * SIZE, A2, %xmm5)

	mulps	 %xmm13, %xmm4
	addps	 %xmm4,  %xmm0
	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)
	mulps	 %xmm13, %xmm5
	addps	 %xmm5,  %xmm1
	MOVUPS_YS1(-28 * SIZE, Y1, %xmm1)

	addq	 $8 * SIZE, A1
	addq	 $8 * SIZE, A2
	addq	 $8 * SIZE, Y1
	ALIGN_3

.L46:
	testq	$4, MM
	je	.L47

	MOVUPS_A1 (-32 * SIZE, A1, %xmm8)

	MOVUPS_YL1(-32 * SIZE, Y1, %xmm0)

	mulps	 %xmm12, %xmm8
	addps	 %xmm8,  %xmm0
	MOVUPS_A1(-32 * SIZE, A2, %xmm4)
	mulps	 %xmm13, %xmm4
	addps	 %xmm4,  %xmm0
	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)

	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, A2
	addq	 $4 * SIZE, Y1
	ALIGN_3

.L47:
	testq	$2, MM
	je	.L48

	movsd	 -32 * SIZE(A1), %xmm0
	movsd	 -32 * SIZE(A2), %xmm1

	movsd	 -32 * SIZE(Y1), %xmm8

	mulps	 %xmm12, %xmm0
	addps	 %xmm0,  %xmm8
	mulps	 %xmm13, %xmm1
	addps	 %xmm1,  %xmm8

	movlps	 %xmm8, -32 * SIZE(Y1)

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, Y1
	ALIGN_3

.L48:
	testq	$1, MM
	je	.L990

	movss	 -32 * SIZE(Y1), %xmm8

	movss	 -32 * SIZE(A1), %xmm0
	movss	 -32 * SIZE(A2), %xmm1

	mulss	 %xmm12, %xmm0
	addss	 %xmm0,  %xmm8
	mulss	 %xmm13, %xmm1
	addss	 %xmm1,  %xmm8

	movss	 %xmm8, -32 * SIZE(Y1)
	jmp	 .L990
	ALIGN_3

.L50:
	cmpq	$1, N
	jne	.L990

	leaq	32 * SIZE(BUFFER), Y1
	movq	A,  A1

	movss	(X), %xmm12

	mulss	ALPHA, %xmm12
	shufps	$0, %xmm12, %xmm12

#ifdef ALIGNED_ACCESS
	cmpq	$3, M
	jle	.L57

	testq	$SIZE, A1
	je	.L5X

	movss	 -32 * SIZE(Y1), %xmm8
	movss	 -32 * SIZE(A1), %xmm0

	mulss	 %xmm12, %xmm0
	addss	 %xmm0,  %xmm8

	movss	 %xmm8, -32 * SIZE(Y1)

	addq	 $1 * SIZE, A1
	addq	 $1 * SIZE, A2
	addq	 $1 * SIZE, Y1
	ALIGN_3

.L5X:
	testq	$2 * SIZE, A1
	je	.L5XX

	movsd	 -32 * SIZE(Y1), %xmm8
	movsd	 -32 * SIZE(A1), %xmm0

	mulps	 %xmm12, %xmm0
	addps	 %xmm0,  %xmm8

	movlps	 %xmm8, -32 * SIZE(Y1)

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, Y1
	ALIGN_3

.L5XX:
#endif

	movq	MM,  I
	sarq	$4,  I
	jle	.L55

	MOVUPS_A1 (-32 * SIZE, A1, %xmm8)
	MOVUPS_A1 (-28 * SIZE, A1, %xmm9)
	MOVUPS_A1 (-24 * SIZE, A1, %xmm10)
	MOVUPS_A1 (-20 * SIZE, A1, %xmm11)

	MOVUPS_YL1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-28 * SIZE, Y1, %xmm1)
	MOVUPS_YL1(-24 * SIZE, Y1, %xmm2)
	MOVUPS_YL1(-20 * SIZE, Y1, %xmm3)

	decq	 I
	jle	 .L54
	ALIGN_3

.L53:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 8 - 128 + PREOFFSET(A1)
#endif

	mulps	 %xmm12, %xmm8
	addps	 %xmm8,  %xmm0
	MOVUPS_A1 (-16 * SIZE, A1, %xmm8)
	mulps	 %xmm12, %xmm9
	addps	 %xmm9,  %xmm1
	MOVUPS_A1 (-12 * SIZE, A1, %xmm9)
	mulps	 %xmm12, %xmm10
	addps	 %xmm10,  %xmm2
	MOVUPS_A1 ( -8 * SIZE, A1, %xmm10)
	mulps	 %xmm12, %xmm11
	addps	 %xmm11,  %xmm3
	MOVUPS_A1 ( -4 * SIZE, A1, %xmm11)

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE) * 8 - 128 + PREOFFSET(Y1)
#endif

	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-28 * SIZE, Y1, %xmm1)
	MOVUPS_YS1(-24 * SIZE, Y1, %xmm2)
	MOVUPS_YS1(-20 * SIZE, Y1, %xmm3)

	MOVUPS_YL1(-16 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-12 * SIZE, Y1, %xmm1)
	MOVUPS_YL1( -8 * SIZE, Y1, %xmm2)
	MOVUPS_YL1( -4 * SIZE, Y1, %xmm3)

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, Y1

	subq	 $1, I
	BRANCH
	jg	.L53
	ALIGN_3

.L54:
	mulps	 %xmm12, %xmm8
	addps	 %xmm8,  %xmm0
	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)
	mulps	 %xmm12, %xmm9
	addps	 %xmm9,  %xmm1
	MOVUPS_YS1(-28 * SIZE, Y1, %xmm1)
	mulps	 %xmm12, %xmm10
	addps	 %xmm10,  %xmm2
	MOVUPS_YS1(-24 * SIZE, Y1, %xmm2)
	mulps	 %xmm12, %xmm11
	addps	 %xmm11,  %xmm3
	MOVUPS_YS1(-20 * SIZE, Y1, %xmm3)

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, Y1
	ALIGN_3

.L55:
	testq	$8, MM
	je	.L56

	MOVUPS_A1 (-32 * SIZE, A1, %xmm8)
	MOVUPS_A1 (-28 * SIZE, A1, %xmm9)

	MOVUPS_YL1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-28 * SIZE, Y1, %xmm1)

	mulps	 %xmm12, %xmm8
	addps	 %xmm8,  %xmm0
	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)
	mulps	 %xmm12, %xmm9
	addps	 %xmm9,  %xmm1
	MOVUPS_YS1(-28 * SIZE, Y1, %xmm1)

	addq	 $8 * SIZE, A1
	addq	 $8 * SIZE, Y1
	ALIGN_3

.L56:
	testq	$4, MM
	je	.L57

	MOVUPS_A1 (-32 * SIZE, A1, %xmm8)
	MOVUPS_YL1(-32 * SIZE, Y1, %xmm0)

	mulps	 %xmm12, %xmm8
	addps	 %xmm8,  %xmm0
	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)

	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, Y1
	ALIGN_3

.L57:
	testq	$2, MM
	je	.L58

	movsd	 -32 * SIZE(Y1), %xmm8
	movsd	 -32 * SIZE(A1), %xmm0

	mulps	 %xmm12, %xmm0
	addps	 %xmm0,  %xmm8

	movlps	 %xmm8, -32 * SIZE(Y1)

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, Y1
	ALIGN_3

.L58:
	testq	$1, MM
	je	.L990

	movss	 -32 * SIZE(Y1), %xmm8
	movss	 -32 * SIZE(A1), %xmm0

	mulss	 %xmm12, %xmm0
	addss	 %xmm0,  %xmm8

	movss	 %xmm8, -32 * SIZE(Y1)

#ifdef ALIGNED_ACCESS
       jmp	.L990
       ALIGN_3

.L100:
	testq	$2 * SIZE - 1, LDA
	jne	.L200

	cmpq	$4, N
	jl	.L110
	ALIGN_3

.L101:
	subq	$4, N

	leaq	32 * SIZE(BUFFER), Y1
	movq	A,  A1
	leaq	(A,  LDA, 2), A2
	leaq	(A,  LDA, 4), A

	movss	(X), %xmm12
	addq	INCX, X
	movss	(X), %xmm13
	addq	INCX, X
	movss	(X), %xmm14
	addq	INCX, X
	movss	(X), %xmm15
	addq	INCX, X

	movss	ALPHA, %xmm0

	mulss	%xmm0, %xmm12
	mulss	%xmm0, %xmm13
	mulss	%xmm0, %xmm14
	mulss	%xmm0, %xmm15

	shufps	$0, %xmm12, %xmm12
	shufps	$0, %xmm13, %xmm13
	shufps	$0, %xmm14, %xmm14
	shufps	$0, %xmm15, %xmm15

	cmpq	$3, M
	jle	.L107

	testq	$SIZE, A1
	je	.L10X

	movss	 -32 * SIZE(A1), %xmm0
	movss	 -32 * SIZE(A1, LDA), %xmm1
	movss	 -32 * SIZE(A2), %xmm2
	movss	 -32 * SIZE(A2, LDA), %xmm3

	movss	 -32 * SIZE(Y1), %xmm8

	mulss	 %xmm12, %xmm0
	addss	 %xmm0,  %xmm8
	mulss	 %xmm13, %xmm1
	addss	 %xmm1,  %xmm8
	mulss	 %xmm14, %xmm2
	addss	 %xmm2, %xmm8
	mulss	 %xmm15, %xmm3
	addss	 %xmm3, %xmm8

	movss	 %xmm8, -32 * SIZE(Y1)

	addq	 $1 * SIZE, A1
	addq	 $1 * SIZE, A2
	addq	 $1 * SIZE, Y1
	ALIGN_3

.L10X:
	testq	$2 * SIZE, A1
	je	.L10XX

	movsd	 -32 * SIZE(A1), %xmm0
	movsd	 -32 * SIZE(A1, LDA), %xmm1
	movsd	 -32 * SIZE(A2), %xmm2
	movsd	 -32 * SIZE(A2, LDA), %xmm3

	movsd	 -32 * SIZE(Y1), %xmm8

	mulps	 %xmm12, %xmm0
	addps	 %xmm0,  %xmm8
	mulps	 %xmm13, %xmm1
	addps	 %xmm1,  %xmm8
	mulps	 %xmm14, %xmm2
	addps	 %xmm2, %xmm8
	mulps	 %xmm15, %xmm3
	addps	 %xmm3, %xmm8

	movlps	 %xmm8, -32 * SIZE(Y1)

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, Y1
	ALIGN_3

.L10XX:
	movhps	-32 * SIZE(A1, LDA), %xmm8
	movhps	-32 * SIZE(A2, LDA), %xmm9

	movq	MM,  I
	sarq	$4,  I
	jle	.L105

	movaps	 -32 * SIZE(A1), %xmm4
	movaps	 -28 * SIZE(A1), %xmm5
	movaps	 -24 * SIZE(A1), %xmm6

	MOVUPS_YL1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-28 * SIZE, Y1, %xmm1)
	MOVUPS_YL1(-24 * SIZE, Y1, %xmm2)
	MOVUPS_YL1(-20 * SIZE, Y1, %xmm3)

	decq	 I
	jle	 .L104
	ALIGN_3

.L103:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(A1)
#endif

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	movaps	 -20 * SIZE(A1), %xmm7
	mulps	 %xmm12, %xmm5
	addps	 %xmm5,  %xmm1
	movaps	 -30 * SIZE(A1, LDA), %xmm4
	mulps	 %xmm12, %xmm6
	addps	 %xmm6,  %xmm2
	movaps	 -26 * SIZE(A1, LDA), %xmm5
	mulps	 %xmm12, %xmm7
	addps	 %xmm7,  %xmm3
	movaps	 -22 * SIZE(A1, LDA), %xmm6

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(A1, LDA)
#endif

	shufps	 $0x4e, %xmm4, %xmm8
	mulps	 %xmm13, %xmm8
	addps	 %xmm8,  %xmm0
	movaps	 -18 * SIZE(A1, LDA), %xmm8
	shufps	 $0x4e, %xmm5, %xmm4
	mulps	 %xmm13, %xmm4
	addps	 %xmm4,  %xmm1
	movaps	 -32 * SIZE(A2), %xmm4
	shufps	 $0x4e, %xmm6, %xmm5
	mulps	 %xmm13, %xmm5
	addps	 %xmm5,  %xmm2
	movaps	 -28 * SIZE(A2), %xmm5
	shufps	 $0x4e, %xmm8, %xmm6
	mulps	 %xmm13, %xmm6
	addps	 %xmm6,  %xmm3
	movaps	 -24 * SIZE(A2), %xmm6

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(A2)
#endif

	mulps	 %xmm14, %xmm4
	addps	 %xmm4,  %xmm0
	movaps	 -20 * SIZE(A2), %xmm7
	mulps	 %xmm14, %xmm5
	addps	 %xmm5,  %xmm1
	movaps	 -30 * SIZE(A2, LDA), %xmm4
	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm2
	movaps	 -26 * SIZE(A2, LDA), %xmm5
	mulps	 %xmm14, %xmm7
	addps	 %xmm7,  %xmm3
	movaps	 -22 * SIZE(A2, LDA), %xmm6

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(A2, LDA)
#endif

	shufps	 $0x4e, %xmm4, %xmm9
	mulps	 %xmm15, %xmm9
	addps	 %xmm9,  %xmm0
	movaps	 -18 * SIZE(A2, LDA), %xmm9
	shufps	 $0x4e, %xmm5, %xmm4
	mulps	 %xmm15, %xmm4
	addps	 %xmm4,  %xmm1
	movaps	 -16 * SIZE(A1), %xmm4
	shufps	 $0x4e, %xmm6, %xmm5
	mulps	 %xmm15, %xmm5
	addps	 %xmm5,  %xmm2
	movaps	 -12 * SIZE(A1), %xmm5
	shufps	 $0x4e, %xmm9, %xmm6
	mulps	 %xmm15, %xmm6
	addps	 %xmm6,  %xmm3
	movaps	  -8 * SIZE(A1), %xmm6

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(Y1)
#endif

	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-28 * SIZE, Y1, %xmm1)
	MOVUPS_YS1(-24 * SIZE, Y1, %xmm2)
	MOVUPS_YS1(-20 * SIZE, Y1, %xmm3)

	MOVUPS_YL1(-16 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-12 * SIZE, Y1, %xmm1)
	MOVUPS_YL1( -8 * SIZE, Y1, %xmm2)
	MOVUPS_YL1( -4 * SIZE, Y1, %xmm3)

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, A2
	subq	 $-16 * SIZE, Y1

	subq	 $1, I
	BRANCH
	jg	.L103
	ALIGN_3

.L104:
	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	movaps	 -20 * SIZE(A1), %xmm7
	mulps	 %xmm12, %xmm5
	addps	 %xmm5,  %xmm1
	movaps	 -30 * SIZE(A1, LDA), %xmm4
	mulps	 %xmm12, %xmm6
	addps	 %xmm6,  %xmm2
	movaps	 -26 * SIZE(A1, LDA), %xmm5
	mulps	 %xmm12, %xmm7
	addps	 %xmm7,  %xmm3
	movaps	 -22 * SIZE(A1, LDA), %xmm6

	shufps	 $0x4e, %xmm4, %xmm8
	mulps	 %xmm13, %xmm8
	addps	 %xmm8,  %xmm0
	movaps	 -18 * SIZE(A1, LDA), %xmm8
	shufps	 $0x4e, %xmm5, %xmm4
	mulps	 %xmm13, %xmm4
	addps	 %xmm4,  %xmm1
	movaps	 -32 * SIZE(A2), %xmm4
	shufps	 $0x4e, %xmm6, %xmm5
	mulps	 %xmm13, %xmm5
	addps	 %xmm5,  %xmm2
	movaps	 -28 * SIZE(A2), %xmm5
	shufps	 $0x4e, %xmm8, %xmm6
	mulps	 %xmm13, %xmm6
	addps	 %xmm6,  %xmm3
	movaps	 -24 * SIZE(A2), %xmm6

	mulps	 %xmm14, %xmm4
	addps	 %xmm4,  %xmm0
	movaps	 -20 * SIZE(A2), %xmm7
	mulps	 %xmm14, %xmm5
	addps	 %xmm5,  %xmm1
	movaps	 -30 * SIZE(A2, LDA), %xmm4
	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm2
	movaps	 -26 * SIZE(A2, LDA), %xmm5
	mulps	 %xmm14, %xmm7
	addps	 %xmm7,  %xmm3
	movaps	 -22 * SIZE(A2, LDA), %xmm6

	shufps	 $0x4e, %xmm4, %xmm9
	mulps	 %xmm15, %xmm9
	addps	 %xmm9,  %xmm0
	movaps	 -18 * SIZE(A2, LDA), %xmm9
	shufps	 $0x4e, %xmm5, %xmm4
	mulps	 %xmm15, %xmm4
	addps	 %xmm4,  %xmm1
	shufps	 $0x4e, %xmm6, %xmm5
	mulps	 %xmm15, %xmm5
	addps	 %xmm5,  %xmm2
	shufps	 $0x4e, %xmm9, %xmm6
	mulps	 %xmm15, %xmm6
	addps	 %xmm6,  %xmm3

	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-28 * SIZE, Y1, %xmm1)
	MOVUPS_YS1(-24 * SIZE, Y1, %xmm2)
	MOVUPS_YS1(-20 * SIZE, Y1, %xmm3)

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, A2
	subq	 $-16 * SIZE, Y1
	ALIGN_3

.L105:
	testq	$8, MM
	je	.L106

	movaps	 -32 * SIZE(A1), %xmm4
	movaps	 -28 * SIZE(A1), %xmm5
	movaps	 -30 * SIZE(A1, LDA), %xmm6
	movaps	 -26 * SIZE(A1, LDA), %xmm7

	MOVUPS_YL1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-28 * SIZE, Y1, %xmm1)

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	movaps	 -32 * SIZE(A2), %xmm4
	mulps	 %xmm12, %xmm5
	addps	 %xmm5,  %xmm1
	movaps	 -28 * SIZE(A2), %xmm10

	shufps	 $0x4e, %xmm6, %xmm8
	mulps	 %xmm13, %xmm8
	addps	 %xmm8,  %xmm0
	movaps	 -30 * SIZE(A2, LDA), %xmm11
	shufps	 $0x4e, %xmm7, %xmm6
	mulps	 %xmm13, %xmm6
	addps	 %xmm6,  %xmm1
	movaps	 %xmm7, %xmm8
	movaps	 -26 * SIZE(A2, LDA), %xmm7

	mulps	 %xmm14, %xmm4
	addps	 %xmm4,  %xmm0
	mulps	 %xmm14, %xmm10
	addps	 %xmm10,  %xmm1

	shufps	 $0x4e, %xmm11, %xmm9
	mulps	 %xmm15, %xmm9
	addps	 %xmm9,  %xmm0
	shufps	 $0x4e, %xmm7, %xmm11
	mulps	 %xmm15, %xmm11
	addps	 %xmm11,  %xmm1
	movaps	 %xmm7, %xmm9

	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-28 * SIZE, Y1, %xmm1)

	addq	 $8 * SIZE, A1
	addq	 $8 * SIZE, A2
	addq	 $8 * SIZE, Y1
	ALIGN_3

.L106:
	testq	$4, MM
	je	.L107

	movaps	 -32 * SIZE(A1), %xmm4
	movaps	 -30 * SIZE(A1, LDA), %xmm5
	movaps	 -32 * SIZE(A2), %xmm6
	movaps	 -30 * SIZE(A2, LDA), %xmm7

	MOVUPS_YL1(-32 * SIZE, Y1, %xmm0)

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	shufps	 $0x4e, %xmm5, %xmm8
	mulps	 %xmm13, %xmm8
	addps	 %xmm8,  %xmm0
	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm0
	shufps	 $0x4e, %xmm7, %xmm9
	mulps	 %xmm15, %xmm9
	addps	 %xmm9,  %xmm0

	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)

	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, A2
	addq	 $4 * SIZE, Y1
	ALIGN_3

.L107:
	testq	$2, MM
	je	.L108

	movsd	 -32 * SIZE(A1), %xmm4
	movsd	 -32 * SIZE(A1, LDA), %xmm5
	movsd	 -32 * SIZE(A2), %xmm6
	movsd	 -32 * SIZE(A2, LDA), %xmm7

	movsd	 -32 * SIZE(Y1), %xmm0

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	mulps	 %xmm13, %xmm5
	addps	 %xmm5,  %xmm0
	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm0
	mulps	 %xmm15, %xmm7
	addps	 %xmm7,  %xmm0

	movlps	 %xmm0, -32 * SIZE(Y1)

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, Y1
	ALIGN_3

.L108:
	testq	$1, MM
	je	.L109

	movss	 -32 * SIZE(Y1), %xmm0

	movss	 -32 * SIZE(A1), %xmm4
	movss	 -32 * SIZE(A1, LDA), %xmm5
	movss	 -32 * SIZE(A2), %xmm6
	movss	 -32 * SIZE(A2, LDA), %xmm7

	mulss	 %xmm12, %xmm4
	addss	 %xmm4,  %xmm0
	mulss	 %xmm13, %xmm5
	addss	 %xmm5,  %xmm0
	mulss	 %xmm14, %xmm6
	addss	 %xmm6,  %xmm0
	mulss	 %xmm15, %xmm7
	addss	 %xmm7,  %xmm0

	movss	 %xmm0, -32 * SIZE(Y1)
	ALIGN_3

.L109:
	cmpq	$4, N
	jge	.L101
	ALIGN_3

.L110:
	testq	N, N
	jle	.L990

	cmpq	$3, N
	jne	.L120

	leaq	32 * SIZE(BUFFER), Y1
	movq	A,  A1
	leaq	(A,  LDA, 2), A2
	leaq	(A,  LDA, 4), A

	movss	(X), %xmm12
	addq	INCX, X
	movss	(X), %xmm13
	addq	INCX, X
	movss	(X), %xmm14
	addq	INCX, X

	movss	ALPHA, %xmm0

	mulss	%xmm0, %xmm12
	mulss	%xmm0, %xmm13
	mulss	%xmm0, %xmm14

	shufps	$0, %xmm12, %xmm12
	shufps	$0, %xmm13, %xmm13
	shufps	$0, %xmm14, %xmm14

	cmpq	$3, M
	jle	.L117

	testq	$SIZE, A1
	je	.L11X

	movss	 -32 * SIZE(A1), %xmm0
	movss	 -32 * SIZE(A1, LDA), %xmm1
	movss	 -32 * SIZE(A2), %xmm2

	movss	 -32 * SIZE(Y1), %xmm8

	mulss	 %xmm12, %xmm0
	addss	 %xmm0,  %xmm8
	mulss	 %xmm13, %xmm1
	addss	 %xmm1,  %xmm8
	mulss	 %xmm14, %xmm2
	addss	 %xmm2, %xmm8

	movss	 %xmm8, -32 * SIZE(Y1)

	addq	 $1 * SIZE, A1
	addq	 $1 * SIZE, A2
	addq	 $1 * SIZE, Y1
	ALIGN_3

.L11X:
	testq	$2 * SIZE, A1
	je	.L11XX

	movsd	 -32 * SIZE(A1), %xmm0
	movsd	 -32 * SIZE(A1, LDA), %xmm1
	movsd	 -32 * SIZE(A2), %xmm2

	movsd	 -32 * SIZE(Y1), %xmm8

	mulps	 %xmm12, %xmm0
	addps	 %xmm0,  %xmm8
	mulps	 %xmm13, %xmm1
	addps	 %xmm1,  %xmm8
	mulps	 %xmm14, %xmm2
	addps	 %xmm2, %xmm8

	movlps	 %xmm8, -32 * SIZE(Y1)

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, Y1
	ALIGN_3

.L11XX:
	movhps	-32 * SIZE(A1, LDA), %xmm8
	movhps	-32 * SIZE(A2, LDA), %xmm9

	movq	MM,  I
	sarq	$4,  I
	jle	.L115

	movaps	 -32 * SIZE(A1), %xmm4
	movaps	 -28 * SIZE(A1), %xmm5
	movaps	 -24 * SIZE(A1), %xmm6

	MOVUPS_YL1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-28 * SIZE, Y1, %xmm1)
	MOVUPS_YL1(-24 * SIZE, Y1, %xmm2)
	MOVUPS_YL1(-20 * SIZE, Y1, %xmm3)

	decq	 I
	jle	 .L114
	ALIGN_3

.L113:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 3 - 128 + PREOFFSET(A1)
#endif

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	movaps	 -20 * SIZE(A1), %xmm7
	mulps	 %xmm12, %xmm5
	addps	 %xmm5,  %xmm1
	movaps	 -30 * SIZE(A1, LDA), %xmm4
	mulps	 %xmm12, %xmm6
	addps	 %xmm6,  %xmm2
	movaps	 -26 * SIZE(A1, LDA), %xmm5
	mulps	 %xmm12, %xmm7
	addps	 %xmm7,  %xmm3
	movaps	 -22 * SIZE(A1, LDA), %xmm6

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 3 - 128 + PREOFFSET(A1, LDA)
#endif

	shufps	 $0x4e, %xmm4, %xmm8
	mulps	 %xmm13, %xmm8
	addps	 %xmm8,  %xmm0
	movaps	 -18 * SIZE(A1, LDA), %xmm8
	shufps	 $0x4e, %xmm5, %xmm4
	mulps	 %xmm13, %xmm4
	addps	 %xmm4,  %xmm1
	movaps	 -32 * SIZE(A2), %xmm4
	shufps	 $0x4e, %xmm6, %xmm5
	mulps	 %xmm13, %xmm5
	addps	 %xmm5,  %xmm2
	movaps	 -28 * SIZE(A2), %xmm5
	shufps	 $0x4e, %xmm8, %xmm6
	mulps	 %xmm13, %xmm6
	addps	 %xmm6,  %xmm3
	movaps	 -24 * SIZE(A2), %xmm6

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 3 - 128 + PREOFFSET(A2)
#endif

	mulps	 %xmm14, %xmm4
	addps	 %xmm4,  %xmm0
	movaps	 -20 * SIZE(A2), %xmm7
	mulps	 %xmm14, %xmm5
	addps	 %xmm5,  %xmm1
	movaps	 -16 * SIZE(A1), %xmm4
	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm2
	movaps	 -12 * SIZE(A1), %xmm5
	mulps	 %xmm14, %xmm7
	addps	 %xmm7,  %xmm3
	movaps	  -8 * SIZE(A1), %xmm6

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE) * 3 - 128 + PREOFFSET(Y1)
#endif

	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-28 * SIZE, Y1, %xmm1)
	MOVUPS_YS1(-24 * SIZE, Y1, %xmm2)
	MOVUPS_YS1(-20 * SIZE, Y1, %xmm3)

	MOVUPS_YL1(-16 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-12 * SIZE, Y1, %xmm1)
	MOVUPS_YL1( -8 * SIZE, Y1, %xmm2)
	MOVUPS_YL1( -4 * SIZE, Y1, %xmm3)

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, A2
	subq	 $-16 * SIZE, Y1

	subq	 $1, I
	BRANCH
	jg	.L113
	ALIGN_3

.L114:
	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	movaps	 -20 * SIZE(A1), %xmm7
	mulps	 %xmm12, %xmm5
	addps	 %xmm5,  %xmm1
	movaps	 -30 * SIZE(A1, LDA), %xmm4
	mulps	 %xmm12, %xmm6
	addps	 %xmm6,  %xmm2
	movaps	 -26 * SIZE(A1, LDA), %xmm5
	mulps	 %xmm12, %xmm7
	addps	 %xmm7,  %xmm3
	movaps	 -22 * SIZE(A1, LDA), %xmm6

	shufps	 $0x4e, %xmm4, %xmm8
	mulps	 %xmm13, %xmm8
	addps	 %xmm8,  %xmm0
	movaps	 -18 * SIZE(A1, LDA), %xmm8
	shufps	 $0x4e, %xmm5, %xmm4
	mulps	 %xmm13, %xmm4
	addps	 %xmm4,  %xmm1
	movaps	 -32 * SIZE(A2), %xmm4
	shufps	 $0x4e, %xmm6, %xmm5
	mulps	 %xmm13, %xmm5
	addps	 %xmm5,  %xmm2
	movaps	 -28 * SIZE(A2), %xmm5
	shufps	 $0x4e, %xmm8, %xmm6
	mulps	 %xmm13, %xmm6
	addps	 %xmm6,  %xmm3
	movaps	 -24 * SIZE(A2), %xmm6

	mulps	 %xmm14, %xmm4
	addps	 %xmm4,  %xmm0
	movaps	 -20 * SIZE(A2), %xmm7
	mulps	 %xmm14, %xmm5
	addps	 %xmm5,  %xmm1
	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm2
	mulps	 %xmm14, %xmm7
	addps	 %xmm7,  %xmm3

	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-28 * SIZE, Y1, %xmm1)
	MOVUPS_YS1(-24 * SIZE, Y1, %xmm2)
	MOVUPS_YS1(-20 * SIZE, Y1, %xmm3)

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, A2
	subq	 $-16 * SIZE, Y1
	ALIGN_3

.L115:
	testq	$8, MM
	je	.L116

	movaps	 -32 * SIZE(A1), %xmm4
	movaps	 -28 * SIZE(A1), %xmm5
	movaps	 -30 * SIZE(A1, LDA), %xmm6
	movaps	 -26 * SIZE(A1, LDA), %xmm7

	MOVUPS_YL1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-28 * SIZE, Y1, %xmm1)

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	movaps	 -32 * SIZE(A2), %xmm4
	mulps	 %xmm12, %xmm5
	addps	 %xmm5,  %xmm1
	movaps	 -28 * SIZE(A2), %xmm10

	shufps	 $0x4e, %xmm6, %xmm8
	mulps	 %xmm13, %xmm8
	addps	 %xmm8,  %xmm0
	shufps	 $0x4e, %xmm7, %xmm6
	mulps	 %xmm13, %xmm6
	addps	 %xmm6,  %xmm1
	movaps	 %xmm7, %xmm8

	mulps	 %xmm14, %xmm4
	addps	 %xmm4,  %xmm0
	mulps	 %xmm14, %xmm10
	addps	 %xmm10,  %xmm1

	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-28 * SIZE, Y1, %xmm1)

	addq	 $8 * SIZE, A1
	addq	 $8 * SIZE, A2
	addq	 $8 * SIZE, Y1
	ALIGN_3

.L116:
	testq	$4, MM
	je	.L117

	movaps	 -32 * SIZE(A1), %xmm4
	movaps	 -30 * SIZE(A1, LDA), %xmm5
	movaps	 -32 * SIZE(A2), %xmm6

	MOVUPS_YL1(-32 * SIZE, Y1, %xmm0)

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	shufps	 $0x4e, %xmm5, %xmm8
	mulps	 %xmm13, %xmm8
	addps	 %xmm8,  %xmm0
	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm0

	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)

	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, A2
	addq	 $4 * SIZE, Y1
	ALIGN_3

.L117:
	testq	$2, MM
	je	.L118

	movsd	 -32 * SIZE(A1), %xmm4
	movsd	 -32 * SIZE(A1, LDA), %xmm5
	movsd	 -32 * SIZE(A2), %xmm6

	movsd	 -32 * SIZE(Y1), %xmm0

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	mulps	 %xmm13, %xmm5
	addps	 %xmm5,  %xmm0
	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm0

	movlps	 %xmm0, -32 * SIZE(Y1)

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, Y1
	ALIGN_3

.L118:
	testq	$1, MM
	je	.L990

	movss	 -32 * SIZE(Y1), %xmm0

	movss	 -32 * SIZE(A1), %xmm4
	movss	 -32 * SIZE(A1, LDA), %xmm5
	movss	 -32 * SIZE(A2), %xmm6

	mulss	 %xmm12, %xmm4
	addss	 %xmm4,  %xmm0
	mulss	 %xmm13, %xmm5
	addss	 %xmm5,  %xmm0
	mulss	 %xmm14, %xmm6
	addss	 %xmm6,  %xmm0

	movss	 %xmm0, -32 * SIZE(Y1)
	jmp	 .L990
	ALIGN_3

.L120:
	cmpq	$2, N
	jl	.L130

	leaq	32 * SIZE(BUFFER), Y1
	movq	A,  A1
	leaq	(A,  LDA, 1), A2
	leaq	(A,  LDA, 2), A

	movss	(X), %xmm12
	addq	INCX, X
	movss	(X), %xmm13
	addq	INCX, X

	movss	ALPHA, %xmm0

	mulss	%xmm0, %xmm12
	mulss	%xmm0, %xmm13

	shufps	$0, %xmm12, %xmm12
	shufps	$0, %xmm13, %xmm13

	cmpq	$3, M
	jle	.L127

	testq	$SIZE, A1
	je	.L12X

	movss	 -32 * SIZE(Y1), %xmm8

	movss	 -32 * SIZE(A1), %xmm0
	movss	 -32 * SIZE(A2), %xmm1

	mulss	 %xmm12, %xmm0
	addss	 %xmm0,  %xmm8
	mulss	 %xmm13, %xmm1
	addss	 %xmm1,  %xmm8

	movss	 %xmm8, -32 * SIZE(Y1)

	addq	 $1 * SIZE, A1
	addq	 $1 * SIZE, A2
	addq	 $1 * SIZE, Y1
	ALIGN_3

.L12X:
	testq	$2 * SIZE, A1
	je	.L12XX

	movsd	 -32 * SIZE(A1), %xmm0
	movsd	 -32 * SIZE(A2), %xmm1

	movsd	 -32 * SIZE(Y1), %xmm8

	mulps	 %xmm12, %xmm0
	addps	 %xmm0,  %xmm8
	mulps	 %xmm13, %xmm1
	addps	 %xmm1,  %xmm8

	movlps	 %xmm8, -32 * SIZE(Y1)

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, Y1
	ALIGN_3

.L12XX:
	movhps	-32 * SIZE(A2), %xmm8

	movq	MM,  I
	sarq	$4,  I
	jle	.L125

	movaps	 -32 * SIZE(A1), %xmm4
	movaps	 -28 * SIZE(A1), %xmm5
	movaps	 -24 * SIZE(A1), %xmm6

	MOVUPS_YL1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-28 * SIZE, Y1, %xmm1)
	MOVUPS_YL1(-24 * SIZE, Y1, %xmm2)
	MOVUPS_YL1(-20 * SIZE, Y1, %xmm3)

	decq	 I
	jle	 .L124
	ALIGN_3

.L123:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 4 - 128 + PREOFFSET(A1)
#endif

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	movaps	 -20 * SIZE(A1), %xmm7
	mulps	 %xmm12, %xmm5
	addps	 %xmm5,  %xmm1
	movaps	 -30 * SIZE(A1, LDA), %xmm4
	mulps	 %xmm12, %xmm6
	addps	 %xmm6,  %xmm2
	movaps	 -26 * SIZE(A1, LDA), %xmm5
	mulps	 %xmm12, %xmm7
	addps	 %xmm7,  %xmm3
	movaps	 -22 * SIZE(A1, LDA), %xmm6

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 4 - 128 + PREOFFSET(A2)
#endif

	shufps	 $0x4e, %xmm4, %xmm8
	mulps	 %xmm13, %xmm8
	addps	 %xmm8,  %xmm0
	movaps	 -18 * SIZE(A1, LDA), %xmm8
	shufps	 $0x4e, %xmm5, %xmm4
	mulps	 %xmm13, %xmm4
	addps	 %xmm4,  %xmm1
	movaps	 -16 * SIZE(A1), %xmm4
	shufps	 $0x4e, %xmm6, %xmm5
	mulps	 %xmm13, %xmm5
	addps	 %xmm5,  %xmm2
	movaps	 -12 * SIZE(A1), %xmm5
	shufps	 $0x4e, %xmm8, %xmm6
	mulps	 %xmm13, %xmm6
	addps	 %xmm6,  %xmm3
	movaps	  -8 * SIZE(A1), %xmm6

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE) * 4 - 128 + PREOFFSET(Y1)
#endif

	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-28 * SIZE, Y1, %xmm1)
	MOVUPS_YS1(-24 * SIZE, Y1, %xmm2)
	MOVUPS_YS1(-20 * SIZE, Y1, %xmm3)

	MOVUPS_YL1(-16 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-12 * SIZE, Y1, %xmm1)
	MOVUPS_YL1( -8 * SIZE, Y1, %xmm2)
	MOVUPS_YL1( -4 * SIZE, Y1, %xmm3)

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, A2
	subq	 $-16 * SIZE, Y1

	subq	 $1, I
	BRANCH
	jg	.L123
	ALIGN_3

.L124:
	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	movaps	 -20 * SIZE(A1), %xmm7
	mulps	 %xmm12, %xmm5
	addps	 %xmm5,  %xmm1
	movaps	 -30 * SIZE(A1, LDA), %xmm4
	mulps	 %xmm12, %xmm6
	addps	 %xmm6,  %xmm2
	movaps	 -26 * SIZE(A1, LDA), %xmm5
	mulps	 %xmm12, %xmm7
	addps	 %xmm7,  %xmm3
	movaps	 -22 * SIZE(A1, LDA), %xmm6

	shufps	 $0x4e, %xmm4, %xmm8
	mulps	 %xmm13, %xmm8
	addps	 %xmm8,  %xmm0
	movaps	 -18 * SIZE(A1, LDA), %xmm8
	shufps	 $0x4e, %xmm5, %xmm4
	mulps	 %xmm13, %xmm4
	addps	 %xmm4,  %xmm1
	shufps	 $0x4e, %xmm6, %xmm5
	mulps	 %xmm13, %xmm5
	addps	 %xmm5,  %xmm2
	shufps	 $0x4e, %xmm8, %xmm6
	mulps	 %xmm13, %xmm6
	addps	 %xmm6,  %xmm3

	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-28 * SIZE, Y1, %xmm1)
	MOVUPS_YS1(-24 * SIZE, Y1, %xmm2)
	MOVUPS_YS1(-20 * SIZE, Y1, %xmm3)

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, A2
	subq	 $-16 * SIZE, Y1
	ALIGN_3

.L125:
	testq	$8, MM
	je	.L126

	movaps	 -32 * SIZE(A1), %xmm4
	movaps	 -28 * SIZE(A1), %xmm5
	movaps	 -30 * SIZE(A2), %xmm6
	movaps	 -26 * SIZE(A2), %xmm7

	MOVUPS_YL1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-28 * SIZE, Y1, %xmm1)

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	mulps	 %xmm12, %xmm5
	addps	 %xmm5,  %xmm1

	shufps	 $0x4e, %xmm6, %xmm8
	mulps	 %xmm13, %xmm8
	addps	 %xmm8,  %xmm0
	shufps	 $0x4e, %xmm7, %xmm6
	mulps	 %xmm13, %xmm6
	addps	 %xmm6,  %xmm1
	movaps	 %xmm7, %xmm8

	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-28 * SIZE, Y1, %xmm1)

	addq	 $8 * SIZE, A1
	addq	 $8 * SIZE, A2
	addq	 $8 * SIZE, Y1
	ALIGN_3

.L126:
	testq	$4, MM
	je	.L127

	movaps	 -32 * SIZE(A1), %xmm4
	movaps	 -30 * SIZE(A2), %xmm5

	MOVUPS_YL1(-32 * SIZE, Y1, %xmm0)

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	shufps	 $0x4e, %xmm5, %xmm8
	mulps	 %xmm13, %xmm8
	addps	 %xmm8,  %xmm0

	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)

	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, A2
	addq	 $4 * SIZE, Y1
	ALIGN_3

.L127:
	testq	$2, MM
	je	.L128

	movsd	 -32 * SIZE(A1), %xmm0
	movsd	 -32 * SIZE(A2), %xmm1

	movsd	 -32 * SIZE(Y1), %xmm8

	mulps	 %xmm12, %xmm0
	addps	 %xmm0,  %xmm8
	mulps	 %xmm13, %xmm1
	addps	 %xmm1,  %xmm8

	movlps	 %xmm8, -32 * SIZE(Y1)

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, Y1
	ALIGN_3

.L128:
	testq	$1, MM
	je	.L990

	movss	 -32 * SIZE(Y1), %xmm8

	movss	 -32 * SIZE(A1), %xmm0
	movss	 -32 * SIZE(A2), %xmm1

	mulss	 %xmm12, %xmm0
	addss	 %xmm0,  %xmm8
	mulss	 %xmm13, %xmm1
	addss	 %xmm1,  %xmm8

	movss	 %xmm8, -32 * SIZE(Y1)
	jmp	 .L990
	ALIGN_3

.L130:
	cmpq	$1, N
	jne	.L990

	leaq	32 * SIZE(BUFFER), Y1
	movq	A,  A1

	movss	(X), %xmm12

	mulss	ALPHA, %xmm12
	shufps	$0, %xmm12, %xmm12

	cmpq	$3, M
	jle	.L137

	testq	$SIZE, A1
	je	.L13X

	movss	 -32 * SIZE(Y1), %xmm8
	movss	 -32 * SIZE(A1), %xmm0

	mulss	 %xmm12, %xmm0
	addss	 %xmm0,  %xmm8

	movss	 %xmm8, -32 * SIZE(Y1)

	addq	 $1 * SIZE, A1
	addq	 $1 * SIZE, A2
	addq	 $1 * SIZE, Y1
	ALIGN_3

.L13X:
	testq	$2 * SIZE, A1
	je	.L13XX

	movsd	 -32 * SIZE(Y1), %xmm8
	movsd	 -32 * SIZE(A1), %xmm0

	mulps	 %xmm12, %xmm0
	addps	 %xmm0,  %xmm8

	movlps	 %xmm8, -32 * SIZE(Y1)

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, Y1
	ALIGN_3

.L13XX:
	movq	MM,  I
	sarq	$4,  I
	jle	.L135

	movaps	-32 * SIZE(A1), %xmm8
	movaps	-28 * SIZE(A1), %xmm9
	movaps	-24 * SIZE(A1), %xmm10
	movaps	-20 * SIZE(A1), %xmm11

	MOVUPS_YL1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-28 * SIZE, Y1, %xmm1)
	MOVUPS_YL1(-24 * SIZE, Y1, %xmm2)
	MOVUPS_YL1(-20 * SIZE, Y1, %xmm3)

	decq	 I
	jle	 .L134
	ALIGN_3

.L133:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 8 - 128 + PREOFFSET(A1)
#endif

	mulps	 %xmm12, %xmm8
	addps	 %xmm8,  %xmm0
	movaps	-16 * SIZE(A1), %xmm8
	mulps	 %xmm12, %xmm9
	addps	 %xmm9,  %xmm1
	movaps	-12 * SIZE(A1), %xmm9
	mulps	 %xmm12, %xmm10
	addps	 %xmm10,  %xmm2
	movaps	 -8 * SIZE(A1), %xmm10
	mulps	 %xmm12, %xmm11
	addps	 %xmm11,  %xmm3
	movaps	 -4 * SIZE(A1), %xmm11

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE) * 8 - 128 + PREOFFSET(Y1)
#endif

	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-28 * SIZE, Y1, %xmm1)
	MOVUPS_YS1(-24 * SIZE, Y1, %xmm2)
	MOVUPS_YS1(-20 * SIZE, Y1, %xmm3)

	MOVUPS_YL1(-16 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-12 * SIZE, Y1, %xmm1)
	MOVUPS_YL1( -8 * SIZE, Y1, %xmm2)
	MOVUPS_YL1( -4 * SIZE, Y1, %xmm3)

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, Y1

	subq	 $1, I
	BRANCH
	jg	.L133
	ALIGN_3

.L134:
	mulps	 %xmm12, %xmm8
	addps	 %xmm8,  %xmm0
	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)
	mulps	 %xmm12, %xmm9
	addps	 %xmm9,  %xmm1
	MOVUPS_YS1(-28 * SIZE, Y1, %xmm1)
	mulps	 %xmm12, %xmm10
	addps	 %xmm10,  %xmm2
	MOVUPS_YS1(-24 * SIZE, Y1, %xmm2)
	mulps	 %xmm12, %xmm11
	addps	 %xmm11,  %xmm3
	MOVUPS_YS1(-20 * SIZE, Y1, %xmm3)

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, Y1
	ALIGN_3

.L135:
	testq	$8, MM
	je	.L136

	movaps	-32 * SIZE(A1), %xmm8
	movaps	-28 * SIZE(A1), %xmm9

	MOVUPS_YL1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-28 * SIZE, Y1, %xmm1)

	mulps	 %xmm12, %xmm8
	addps	 %xmm8,  %xmm0
	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)
	mulps	 %xmm12, %xmm9
	addps	 %xmm9,  %xmm1
	MOVUPS_YS1(-28 * SIZE, Y1, %xmm1)

	addq	 $8 * SIZE, A1
	addq	 $8 * SIZE, Y1
	ALIGN_3

.L136:
	testq	$4, MM
	je	.L137

	movaps	-32 * SIZE(A1), %xmm8
	MOVUPS_YL1(-32 * SIZE, Y1, %xmm0)

	mulps	 %xmm12, %xmm8
	addps	 %xmm8,  %xmm0
	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)

	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, Y1
	ALIGN_3

.L137:
	testq	$2, MM
	je	.L138

	movsd	 -32 * SIZE(Y1), %xmm8
	movsd	 -32 * SIZE(A1), %xmm0

	mulps	 %xmm12, %xmm0
	addps	 %xmm0,  %xmm8

	movlps	 %xmm8, -32 * SIZE(Y1)

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, Y1
	ALIGN_3

.L138:
	testq	$1, MM
	je	.L990

	movss	 -32 * SIZE(Y1), %xmm8
	movss	 -32 * SIZE(A1), %xmm0

	mulss	 %xmm12, %xmm0
	addss	 %xmm0,  %xmm8

	movss	 %xmm8, -32 * SIZE(Y1)
	jmp	.L990
	ALIGN_3

.L200:
	testq	$2 * SIZE, LDA
	jne	.L300

	cmpq	$4, N
	jl	.L210
	ALIGN_3

.L201:
	subq	$4, N

	leaq	32 * SIZE(BUFFER), Y1
	movq	A,  A1
	leaq	(A,  LDA, 2), A2
	leaq	(A,  LDA, 4), A

	movss	(X), %xmm12
	addq	INCX, X
	movss	(X), %xmm13
	addq	INCX, X
	movss	(X), %xmm14
	addq	INCX, X
	movss	(X), %xmm15
	addq	INCX, X

	movss	ALPHA, %xmm0

	mulss	%xmm0, %xmm12
	mulss	%xmm0, %xmm13
	mulss	%xmm0, %xmm14
	mulss	%xmm0, %xmm15

	shufps	$0, %xmm12, %xmm12
	shufps	$0, %xmm13, %xmm13
	shufps	$0, %xmm14, %xmm14
	shufps	$0, %xmm15, %xmm15

	cmpq	$3, M
	jle	.L207

	testq	$SIZE, A1
	je	.L20X

	movss	 -32 * SIZE(A1), %xmm0
	movss	 -32 * SIZE(A1, LDA), %xmm1
	movss	 -32 * SIZE(A2), %xmm2
	movss	 -32 * SIZE(A2, LDA), %xmm3

	movss	 -32 * SIZE(Y1), %xmm8

	mulss	 %xmm12, %xmm0
	addss	 %xmm0,  %xmm8
	mulss	 %xmm13, %xmm1
	addss	 %xmm1,  %xmm8
	mulss	 %xmm14, %xmm2
	addss	 %xmm2, %xmm8
	mulss	 %xmm15, %xmm3
	addss	 %xmm3, %xmm8

	movss	 %xmm8, -32 * SIZE(Y1)

	addq	 $1 * SIZE, A1
	addq	 $1 * SIZE, A2
	addq	 $1 * SIZE, Y1
	ALIGN_3

.L20X:
	testq	$2 * SIZE, A1
	je	.L20XX

	movsd	 -32 * SIZE(A1), %xmm0
	movsd	 -32 * SIZE(A1, LDA), %xmm1
	movsd	 -32 * SIZE(A2), %xmm2
	movsd	 -32 * SIZE(A2, LDA), %xmm3

	movsd	 -32 * SIZE(Y1), %xmm8

	mulps	 %xmm12, %xmm0
	addps	 %xmm0,  %xmm8
	mulps	 %xmm13, %xmm1
	addps	 %xmm1,  %xmm8
	mulps	 %xmm14, %xmm2
	addps	 %xmm2, %xmm8
	mulps	 %xmm15, %xmm3
	addps	 %xmm3, %xmm8

	movlps	 %xmm8, -32 * SIZE(Y1)

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, Y1
	ALIGN_3

.L20XX:
	movaps	-33 * SIZE(A1, LDA), %xmm8
	movaps	-34 * SIZE(A2), %xmm9
	movaps	-35 * SIZE(A2, LDA), %xmm10

	movq	MM,  I
	sarq	$4,  I
	jle	.L205

	movaps	 -32 * SIZE(A1), %xmm4
	movaps	 -28 * SIZE(A1), %xmm5
	movaps	 -24 * SIZE(A1), %xmm6

	MOVUPS_YL1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-28 * SIZE, Y1, %xmm1)
	MOVUPS_YL1(-24 * SIZE, Y1, %xmm2)
	MOVUPS_YL1(-20 * SIZE, Y1, %xmm3)

	decq	 I
	jle	 .L204
	ALIGN_3

.L203:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(A1)
#endif

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	movaps	 -20 * SIZE(A1), %xmm7
	mulps	 %xmm12, %xmm5
	addps	 %xmm5,  %xmm1
	movaps	 -29 * SIZE(A1, LDA), %xmm4
	mulps	 %xmm12, %xmm6
	addps	 %xmm6,  %xmm2
	movaps	 -25 * SIZE(A1, LDA), %xmm5
	mulps	 %xmm12, %xmm7
	addps	 %xmm7,  %xmm3
	movaps	 -21 * SIZE(A1, LDA), %xmm6

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(A1, LDA)
#endif

	movss	 %xmm4, %xmm8
	shufps	 $0x39, %xmm8, %xmm8
	mulps	 %xmm13, %xmm8
	addps	 %xmm8,  %xmm0
	movaps	 -17 * SIZE(A1, LDA), %xmm8
	movss	 %xmm5, %xmm4
	shufps	 $0x39, %xmm4, %xmm4
	mulps	 %xmm13, %xmm4
	addps	 %xmm4,  %xmm1
	movaps	 -30 * SIZE(A2), %xmm4
	movss	 %xmm6, %xmm5
	shufps	 $0x39, %xmm5, %xmm5
	mulps	 %xmm13, %xmm5
	addps	 %xmm5,  %xmm2
	movaps	 -26 * SIZE(A2), %xmm5
	movss	 %xmm8, %xmm6
	shufps	 $0x39, %xmm6, %xmm6
	mulps	 %xmm13, %xmm6
	addps	 %xmm6,  %xmm3
	movaps	 -22 * SIZE(A2), %xmm6

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(A2)
#endif

	shufps	 $0x4e, %xmm4, %xmm9
	mulps	 %xmm14, %xmm9
	addps	 %xmm9,  %xmm0
	movaps	 -18 * SIZE(A2), %xmm9
	shufps	 $0x4e, %xmm5, %xmm4
	mulps	 %xmm14, %xmm4
	addps	 %xmm4,  %xmm1
	movaps	 -31 * SIZE(A2, LDA), %xmm4
	shufps	 $0x4e, %xmm6, %xmm5
	mulps	 %xmm14, %xmm5
	addps	 %xmm5,  %xmm2
	movaps	 -27 * SIZE(A2, LDA), %xmm5
	shufps	 $0x4e, %xmm9, %xmm6
	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm3
	movaps	 -23 * SIZE(A2, LDA), %xmm6

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(A2, LDA)
#endif

	movss	 %xmm4,  %xmm10
	shufps	 $0x93, %xmm4, %xmm10
	mulps	 %xmm15, %xmm10
	addps	 %xmm10, %xmm0
	movaps	 -19 * SIZE(A2, LDA), %xmm10
	movss	 %xmm5,  %xmm4
	shufps	 $0x93, %xmm5, %xmm4
	mulps	 %xmm15, %xmm4
	addps	 %xmm4,  %xmm1
	movaps	 -16 * SIZE(A1), %xmm4
	movss	 %xmm6,  %xmm5
	shufps	 $0x93, %xmm6, %xmm5
	mulps	 %xmm15, %xmm5
	addps	 %xmm5,  %xmm2
	movaps	 -12 * SIZE(A1), %xmm5
	movss	 %xmm10,  %xmm6
	shufps	 $0x93, %xmm10, %xmm6
	mulps	 %xmm15, %xmm6
	addps	 %xmm6,  %xmm3
	movaps	  -8 * SIZE(A1), %xmm6

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(Y1)
#endif

	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-28 * SIZE, Y1, %xmm1)
	MOVUPS_YS1(-24 * SIZE, Y1, %xmm2)
	MOVUPS_YS1(-20 * SIZE, Y1, %xmm3)

	MOVUPS_YL1(-16 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-12 * SIZE, Y1, %xmm1)
	MOVUPS_YL1( -8 * SIZE, Y1, %xmm2)
	MOVUPS_YL1( -4 * SIZE, Y1, %xmm3)

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, A2
	subq	 $-16 * SIZE, Y1

	subq	 $1, I
	BRANCH
	jg	.L203
	ALIGN_3

.L204:
	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	movaps	 -20 * SIZE(A1), %xmm7
	mulps	 %xmm12, %xmm5
	addps	 %xmm5,  %xmm1
	movaps	 -29 * SIZE(A1, LDA), %xmm4
	mulps	 %xmm12, %xmm6
	addps	 %xmm6,  %xmm2
	movaps	 -25 * SIZE(A1, LDA), %xmm5
	mulps	 %xmm12, %xmm7
	addps	 %xmm7,  %xmm3
	movaps	 -21 * SIZE(A1, LDA), %xmm6

	movss	 %xmm4, %xmm8
	shufps	 $0x39, %xmm8, %xmm8
	mulps	 %xmm13, %xmm8
	addps	 %xmm8,  %xmm0
	movaps	 -17 * SIZE(A1, LDA), %xmm8
	movss	 %xmm5, %xmm4
	shufps	 $0x39, %xmm4, %xmm4
	mulps	 %xmm13, %xmm4
	addps	 %xmm4,  %xmm1
	movaps	 -30 * SIZE(A2), %xmm4

	movss	 %xmm6, %xmm5
	shufps	 $0x39, %xmm5, %xmm5
	mulps	 %xmm13, %xmm5
	addps	 %xmm5,  %xmm2
	movaps	 -26 * SIZE(A2), %xmm5
	movss	 %xmm8, %xmm6
	shufps	 $0x39, %xmm6, %xmm6
	mulps	 %xmm13, %xmm6
	addps	 %xmm6,  %xmm3
	movaps	 -22 * SIZE(A2), %xmm6

	shufps	 $0x4e, %xmm4, %xmm9
	mulps	 %xmm14, %xmm9
	addps	 %xmm9,  %xmm0
	movaps	 -18 * SIZE(A2), %xmm9
	shufps	 $0x4e, %xmm5, %xmm4
	mulps	 %xmm14, %xmm4
	addps	 %xmm4,  %xmm1
	movaps	 -31 * SIZE(A2, LDA), %xmm4
	shufps	 $0x4e, %xmm6, %xmm5
	mulps	 %xmm14, %xmm5
	addps	 %xmm5,  %xmm2
	movaps	 -27 * SIZE(A2, LDA), %xmm5
	shufps	 $0x4e, %xmm9, %xmm6
	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm3
	movaps	 -23 * SIZE(A2, LDA), %xmm6

	movss	 %xmm4,  %xmm10
	shufps	 $0x93, %xmm4, %xmm10
	mulps	 %xmm15, %xmm10
	addps	 %xmm10, %xmm0
	movaps	 -19 * SIZE(A2, LDA), %xmm10
	movss	 %xmm5,  %xmm4
	shufps	 $0x93, %xmm5, %xmm4
	mulps	 %xmm15, %xmm4
	addps	 %xmm4,  %xmm1

	movss	 %xmm6,  %xmm5
	shufps	 $0x93, %xmm6, %xmm5
	mulps	 %xmm15, %xmm5
	addps	 %xmm5,  %xmm2
	movss	 %xmm10,  %xmm6
	shufps	 $0x93, %xmm10, %xmm6
	mulps	 %xmm15, %xmm6
	addps	 %xmm6,  %xmm3

	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-28 * SIZE, Y1, %xmm1)
	MOVUPS_YS1(-24 * SIZE, Y1, %xmm2)
	MOVUPS_YS1(-20 * SIZE, Y1, %xmm3)

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, A2
	subq	 $-16 * SIZE, Y1
	ALIGN_3

.L205:
	testq	$8, MM
	je	.L206

	movaps	 -32 * SIZE(A1), %xmm4
	movaps	 -28 * SIZE(A1), %xmm5
	movaps	 -29 * SIZE(A1, LDA), %xmm6

	MOVUPS_YL1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-28 * SIZE, Y1, %xmm1)

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	movaps	 -30 * SIZE(A2), %xmm4
	mulps	 %xmm12, %xmm5
	addps	 %xmm5,  %xmm1
	movaps	 -26 * SIZE(A2), %xmm5

	movss	 %xmm6, %xmm8
	shufps	 $0x39, %xmm8, %xmm8
	mulps	 %xmm13, %xmm8
	addps	 %xmm8,  %xmm0
	movaps	 -25 * SIZE(A1, LDA), %xmm8
	movss	 %xmm8, %xmm6
	shufps	 $0x39, %xmm6, %xmm6
	mulps	 %xmm13, %xmm6
	addps	 %xmm6,  %xmm1
	movaps	 -31 * SIZE(A2, LDA), %xmm6

	shufps	 $0x4e, %xmm4, %xmm9
	mulps	 %xmm14, %xmm9
	addps	 %xmm9,  %xmm0
	movaps	 -27 * SIZE(A2, LDA), %xmm7

	shufps	 $0x4e, %xmm5, %xmm4
	mulps	 %xmm14, %xmm4
	addps	 %xmm4,  %xmm1
	movaps	 %xmm5, %xmm9

	movss	 %xmm6,  %xmm10
	shufps	 $0x93, %xmm6, %xmm10
	mulps	 %xmm15, %xmm10
	addps	 %xmm10, %xmm0

	movss	 %xmm7,  %xmm6
	shufps	 $0x93, %xmm7, %xmm6
	mulps	 %xmm15, %xmm6
	addps	 %xmm6,  %xmm1
	movaps	 %xmm7, %xmm10

	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-28 * SIZE, Y1, %xmm1)

	addq	 $8 * SIZE, A1
	addq	 $8 * SIZE, A2
	addq	 $8 * SIZE, Y1
	ALIGN_3

.L206:
	testq	$4, MM
	je	.L207

	movaps	 -32 * SIZE(A1), %xmm4
	movaps	 -29 * SIZE(A1, LDA), %xmm5
	movaps	 -30 * SIZE(A2), %xmm6
	movaps	 -31 * SIZE(A2, LDA), %xmm7

	MOVUPS_YL1(-32 * SIZE, Y1, %xmm0)

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	movss	 %xmm5, %xmm8
	shufps	 $0x39, %xmm8, %xmm8
	mulps	 %xmm13, %xmm8
	addps	 %xmm8,  %xmm0

	shufps	 $0x4e, %xmm6, %xmm9
	mulps	 %xmm14, %xmm9
	addps	 %xmm9,  %xmm0
	movss	 %xmm7,  %xmm10
	shufps	 $0x93, %xmm7, %xmm10
	mulps	 %xmm15, %xmm10
	addps	 %xmm10, %xmm0

	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)

	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, A2
	addq	 $4 * SIZE, Y1
	ALIGN_3

.L207:
	testq	$2, MM
	je	.L208

	movsd	 -32 * SIZE(A1), %xmm4
	movsd	 -32 * SIZE(A1, LDA), %xmm5
	movsd	 -32 * SIZE(A2), %xmm6
	movsd	 -32 * SIZE(A2, LDA), %xmm7

	movsd	 -32 * SIZE(Y1), %xmm0

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	mulps	 %xmm13, %xmm5
	addps	 %xmm5,  %xmm0
	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm0
	mulps	 %xmm15, %xmm7
	addps	 %xmm7,  %xmm0

	movlps	 %xmm0, -32 * SIZE(Y1)

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, Y1
	ALIGN_3

.L208:
	testq	$1, MM
	je	.L209

	movss	 -32 * SIZE(Y1), %xmm0

	movss	 -32 * SIZE(A1), %xmm4
	movss	 -32 * SIZE(A1, LDA), %xmm5
	movss	 -32 * SIZE(A2), %xmm6
	movss	 -32 * SIZE(A2, LDA), %xmm7

	mulss	 %xmm12, %xmm4
	addss	 %xmm4,  %xmm0
	mulss	 %xmm13, %xmm5
	addss	 %xmm5,  %xmm0
	mulss	 %xmm14, %xmm6
	addss	 %xmm6,  %xmm0
	mulss	 %xmm15, %xmm7
	addss	 %xmm7,  %xmm0

	movss	 %xmm0, -32 * SIZE(Y1)
	ALIGN_3

.L209:
	cmpq	$4, N
	jge	.L201
	ALIGN_3

.L210:
	cmpq	$3, N
	jne	.L220

	leaq	32 * SIZE(BUFFER), Y1
	movq	A,  A1
	leaq	(A,  LDA, 2), A2
	leaq	(A,  LDA, 4), A

	movss	(X), %xmm12
	addq	INCX, X
	movss	(X), %xmm13
	addq	INCX, X
	movss	(X), %xmm14
	addq	INCX, X

	movss	ALPHA, %xmm0

	mulss	%xmm0, %xmm12
	mulss	%xmm0, %xmm13
	mulss	%xmm0, %xmm14

	shufps	$0, %xmm12, %xmm12
	shufps	$0, %xmm13, %xmm13
	shufps	$0, %xmm14, %xmm14

	cmpq	$3, M
	jle	.L217

	testq	$SIZE, A1
	je	.L21X

	movss	 -32 * SIZE(A1), %xmm0
	movss	 -32 * SIZE(A1, LDA), %xmm1
	movss	 -32 * SIZE(A2), %xmm2

	movss	 -32 * SIZE(Y1), %xmm8

	mulss	 %xmm12, %xmm0
	addss	 %xmm0,  %xmm8
	mulss	 %xmm13, %xmm1
	addss	 %xmm1,  %xmm8
	mulss	 %xmm14, %xmm2
	addss	 %xmm2, %xmm8

	movss	 %xmm8, -32 * SIZE(Y1)

	addq	 $1 * SIZE, A1
	addq	 $1 * SIZE, A2
	addq	 $1 * SIZE, Y1
	ALIGN_3

.L21X:
	testq	$2 * SIZE, A1
	je	.L21XX

	movsd	 -32 * SIZE(A1), %xmm0
	movsd	 -32 * SIZE(A1, LDA), %xmm1
	movsd	 -32 * SIZE(A2), %xmm2

	movsd	 -32 * SIZE(Y1), %xmm8

	mulps	 %xmm12, %xmm0
	addps	 %xmm0,  %xmm8
	mulps	 %xmm13, %xmm1
	addps	 %xmm1,  %xmm8
	mulps	 %xmm14, %xmm2
	addps	 %xmm2, %xmm8

	movlps	 %xmm8, -32 * SIZE(Y1)

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, Y1
	ALIGN_3

.L21XX:
	movaps	-33 * SIZE(A1, LDA), %xmm8
	movaps	-34 * SIZE(A2), %xmm9
	movaps	-35 * SIZE(A2, LDA), %xmm10

	movq	MM,  I
	sarq	$4,  I
	jle	.L215

	movaps	 -32 * SIZE(A1), %xmm4
	movaps	 -28 * SIZE(A1), %xmm5
	movaps	 -24 * SIZE(A1), %xmm6

	MOVUPS_YL1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-28 * SIZE, Y1, %xmm1)
	MOVUPS_YL1(-24 * SIZE, Y1, %xmm2)
	MOVUPS_YL1(-20 * SIZE, Y1, %xmm3)

	decq	 I
	jle	 .L214
	ALIGN_3

.L213:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 3 - 128 + PREOFFSET(A1)
#endif

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	movaps	 -20 * SIZE(A1), %xmm7
	mulps	 %xmm12, %xmm5
	addps	 %xmm5,  %xmm1
	movaps	 -29 * SIZE(A1, LDA), %xmm4
	mulps	 %xmm12, %xmm6
	addps	 %xmm6,  %xmm2
	movaps	 -25 * SIZE(A1, LDA), %xmm5
	mulps	 %xmm12, %xmm7
	addps	 %xmm7,  %xmm3
	movaps	 -21 * SIZE(A1, LDA), %xmm6

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 3 - 128 + PREOFFSET(A1, LDA)
#endif

	movss	 %xmm4, %xmm8
	shufps	 $0x39, %xmm8, %xmm8
	mulps	 %xmm13, %xmm8
	addps	 %xmm8,  %xmm0
	movaps	 -17 * SIZE(A1, LDA), %xmm8
	movss	 %xmm5, %xmm4
	shufps	 $0x39, %xmm4, %xmm4
	mulps	 %xmm13, %xmm4
	addps	 %xmm4,  %xmm1
	movaps	 -30 * SIZE(A2), %xmm4
	movss	 %xmm6, %xmm5
	shufps	 $0x39, %xmm5, %xmm5
	mulps	 %xmm13, %xmm5
	addps	 %xmm5,  %xmm2
	movaps	 -26 * SIZE(A2), %xmm5
	movss	 %xmm8, %xmm6
	shufps	 $0x39, %xmm6, %xmm6
	mulps	 %xmm13, %xmm6
	addps	 %xmm6,  %xmm3
	movaps	 -22 * SIZE(A2), %xmm6

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 3 - 128 + PREOFFSET(A2)
#endif

	shufps	 $0x4e, %xmm4, %xmm9
	mulps	 %xmm14, %xmm9
	addps	 %xmm9,  %xmm0
	movaps	 -18 * SIZE(A2), %xmm9
	shufps	 $0x4e, %xmm5, %xmm4
	mulps	 %xmm14, %xmm4
	addps	 %xmm4,  %xmm1
	movaps	 -16 * SIZE(A1), %xmm4
	shufps	 $0x4e, %xmm6, %xmm5
	mulps	 %xmm14, %xmm5
	addps	 %xmm5,  %xmm2
	movaps	 -12 * SIZE(A1), %xmm5
	shufps	 $0x4e, %xmm9, %xmm6
	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm3
	movaps	  -8 * SIZE(A1), %xmm6

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE) * 3 - 128 + PREOFFSET(Y1)
#endif

	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-28 * SIZE, Y1, %xmm1)
	MOVUPS_YS1(-24 * SIZE, Y1, %xmm2)
	MOVUPS_YS1(-20 * SIZE, Y1, %xmm3)

	MOVUPS_YL1(-16 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-12 * SIZE, Y1, %xmm1)
	MOVUPS_YL1( -8 * SIZE, Y1, %xmm2)
	MOVUPS_YL1( -4 * SIZE, Y1, %xmm3)

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, A2
	subq	 $-16 * SIZE, Y1

	subq	 $1, I
	BRANCH
	jg	.L213
	ALIGN_3

.L214:
	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	movaps	 -20 * SIZE(A1), %xmm7
	mulps	 %xmm12, %xmm5
	addps	 %xmm5,  %xmm1
	movaps	 -29 * SIZE(A1, LDA), %xmm4
	mulps	 %xmm12, %xmm6
	addps	 %xmm6,  %xmm2
	movaps	 -25 * SIZE(A1, LDA), %xmm5
	mulps	 %xmm12, %xmm7
	addps	 %xmm7,  %xmm3
	movaps	 -21 * SIZE(A1, LDA), %xmm6

	movss	 %xmm4, %xmm8
	shufps	 $0x39, %xmm8, %xmm8
	mulps	 %xmm13, %xmm8
	addps	 %xmm8,  %xmm0
	movaps	 -17 * SIZE(A1, LDA), %xmm8
	movss	 %xmm5, %xmm4
	shufps	 $0x39, %xmm4, %xmm4
	mulps	 %xmm13, %xmm4
	addps	 %xmm4,  %xmm1
	movaps	 -30 * SIZE(A2), %xmm4

	movss	 %xmm6, %xmm5
	shufps	 $0x39, %xmm5, %xmm5
	mulps	 %xmm13, %xmm5
	addps	 %xmm5,  %xmm2
	movaps	 -26 * SIZE(A2), %xmm5
	movss	 %xmm8, %xmm6
	shufps	 $0x39, %xmm6, %xmm6
	mulps	 %xmm13, %xmm6
	addps	 %xmm6,  %xmm3
	movaps	 -22 * SIZE(A2), %xmm6

	shufps	 $0x4e, %xmm4, %xmm9
	mulps	 %xmm14, %xmm9
	addps	 %xmm9,  %xmm0
	movaps	 -18 * SIZE(A2), %xmm9
	shufps	 $0x4e, %xmm5, %xmm4
	mulps	 %xmm14, %xmm4
	addps	 %xmm4,  %xmm1
	shufps	 $0x4e, %xmm6, %xmm5
	mulps	 %xmm14, %xmm5
	addps	 %xmm5,  %xmm2
	shufps	 $0x4e, %xmm9, %xmm6
	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm3

	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-28 * SIZE, Y1, %xmm1)
	MOVUPS_YS1(-24 * SIZE, Y1, %xmm2)
	MOVUPS_YS1(-20 * SIZE, Y1, %xmm3)

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, A2
	subq	 $-16 * SIZE, Y1
	ALIGN_3

.L215:
	testq	$8, MM
	je	.L216

	movaps	 -32 * SIZE(A1), %xmm4
	movaps	 -28 * SIZE(A1), %xmm5
	movaps	 -29 * SIZE(A1, LDA), %xmm6

	MOVUPS_YL1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-28 * SIZE, Y1, %xmm1)

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	movaps	 -30 * SIZE(A2), %xmm4
	mulps	 %xmm12, %xmm5
	addps	 %xmm5,  %xmm1
	movaps	 -26 * SIZE(A2), %xmm5

	movss	 %xmm6, %xmm8
	shufps	 $0x39, %xmm8, %xmm8
	mulps	 %xmm13, %xmm8
	addps	 %xmm8,  %xmm0
	movaps	 -25 * SIZE(A1, LDA), %xmm8
	movss	 %xmm8, %xmm6
	shufps	 $0x39, %xmm6, %xmm6
	mulps	 %xmm13, %xmm6
	addps	 %xmm6,  %xmm1

	shufps	 $0x4e, %xmm4, %xmm9
	mulps	 %xmm14, %xmm9
	addps	 %xmm9,  %xmm0

	shufps	 $0x4e, %xmm5, %xmm4
	mulps	 %xmm14, %xmm4
	addps	 %xmm4,  %xmm1
	movaps	 %xmm5, %xmm9

	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-28 * SIZE, Y1, %xmm1)

	addq	 $8 * SIZE, A1
	addq	 $8 * SIZE, A2
	addq	 $8 * SIZE, Y1
	ALIGN_3

.L216:
	testq	$4, MM
	je	.L217

	movaps	 -32 * SIZE(A1), %xmm4
	movaps	 -29 * SIZE(A1, LDA), %xmm5
	movaps	 -30 * SIZE(A2), %xmm6

	MOVUPS_YL1(-32 * SIZE, Y1, %xmm0)

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	movss	 %xmm5, %xmm8
	shufps	 $0x39, %xmm8, %xmm8
	mulps	 %xmm13, %xmm8
	addps	 %xmm8,  %xmm0

	shufps	 $0x4e, %xmm6, %xmm9
	mulps	 %xmm14, %xmm9
	addps	 %xmm9,  %xmm0
	movss	 %xmm7,  %xmm10

	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)

	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, A2
	addq	 $4 * SIZE, Y1
	ALIGN_3

.L217:
	testq	$2, MM
	je	.L218

	movsd	 -32 * SIZE(A1), %xmm4
	movsd	 -32 * SIZE(A1, LDA), %xmm5
	movsd	 -32 * SIZE(A2), %xmm6

	movsd	 -32 * SIZE(Y1), %xmm0

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	mulps	 %xmm13, %xmm5
	addps	 %xmm5,  %xmm0
	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm0

	movlps	 %xmm0, -32 * SIZE(Y1)

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, Y1
	ALIGN_3

.L218:
	testq	$1, MM
	je	.L990

	movss	 -32 * SIZE(Y1), %xmm0

	movss	 -32 * SIZE(A1), %xmm4
	movss	 -32 * SIZE(A1, LDA), %xmm5
	movss	 -32 * SIZE(A2), %xmm6

	mulss	 %xmm12, %xmm4
	addss	 %xmm4,  %xmm0
	mulss	 %xmm13, %xmm5
	addss	 %xmm5,  %xmm0
	mulss	 %xmm14, %xmm6
	addss	 %xmm6,  %xmm0

	movss	 %xmm0, -32 * SIZE(Y1)
	jmp	 .L990
	ALIGN_4

.L220:
	testq	N, N
	jle	.L990

	cmpq	$2, N
	jne	.L230

	leaq	32 * SIZE(BUFFER), Y1
	movq	A,  A1
	leaq	(A,  LDA, 1), A2
	leaq	(A,  LDA, 2), A

	movss	(X), %xmm12
	addq	INCX, X
	movss	(X), %xmm13
	addq	INCX, X

	movss	ALPHA, %xmm0

	mulss	%xmm0, %xmm12
	mulss	%xmm0, %xmm13

	shufps	$0, %xmm12, %xmm12
	shufps	$0, %xmm13, %xmm13

	cmpq	$3, M
	jle	.L227

	testq	$SIZE, A1
	je	.L22X

	movss	 -32 * SIZE(Y1), %xmm9

	movss	 -32 * SIZE(A1), %xmm0
	movss	 -32 * SIZE(A2), %xmm1

	mulss	 %xmm12, %xmm0
	addss	 %xmm0,  %xmm9
	mulss	 %xmm13, %xmm1
	addss	 %xmm1,  %xmm9

	movss	 %xmm9, -32 * SIZE(Y1)

	addq	 $1 * SIZE, A1
	addq	 $1 * SIZE, A2
	addq	 $1 * SIZE, Y1
	ALIGN_3

.L22X:
	testq	$2 * SIZE, A1
	je	.L22XX

	movsd	 -32 * SIZE(A1), %xmm0
	movsd	 -32 * SIZE(A2), %xmm1

	movsd	 -32 * SIZE(Y1), %xmm9

	mulps	 %xmm12, %xmm0
	addps	 %xmm0,  %xmm9
	mulps	 %xmm13, %xmm1
	addps	 %xmm1,  %xmm9

	movlps	 %xmm9, -32 * SIZE(Y1)

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, Y1
	ALIGN_3

.L22XX:
	movaps	-33 * SIZE(A1, LDA), %xmm8

	movq	MM,  I
	sarq	$4,  I
	jle	.L225

	movaps	 -32 * SIZE(A1), %xmm4
	movaps	 -28 * SIZE(A1), %xmm5
	movaps	 -24 * SIZE(A1), %xmm6

	MOVUPS_YL1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-28 * SIZE, Y1, %xmm1)
	MOVUPS_YL1(-24 * SIZE, Y1, %xmm2)
	MOVUPS_YL1(-20 * SIZE, Y1, %xmm3)

	decq	 I
	jle	 .L224
	ALIGN_3

.L223:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 4 - 128 + PREOFFSET(A1)
#endif

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	movaps	 -20 * SIZE(A1), %xmm7
	mulps	 %xmm12, %xmm5
	addps	 %xmm5,  %xmm1
	movaps	 -29 * SIZE(A2), %xmm4
	mulps	 %xmm12, %xmm6
	addps	 %xmm6,  %xmm2
	movaps	 -25 * SIZE(A2), %xmm5
	mulps	 %xmm12, %xmm7
	addps	 %xmm7,  %xmm3
	movaps	 -21 * SIZE(A2), %xmm6

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 4 - 128 + PREOFFSET(A2)
#endif

	movss	 %xmm4, %xmm8
	shufps	 $0x39, %xmm8, %xmm8
	mulps	 %xmm13, %xmm8
	addps	 %xmm8,  %xmm0
	movaps	 -17 * SIZE(A2), %xmm8
	movss	 %xmm5, %xmm4
	shufps	 $0x39, %xmm4, %xmm4
	mulps	 %xmm13, %xmm4
	addps	 %xmm4,  %xmm1
	movaps	 -16 * SIZE(A1), %xmm4

	movss	 %xmm6, %xmm5
	shufps	 $0x39, %xmm5, %xmm5
	mulps	 %xmm13, %xmm5
	addps	 %xmm5,  %xmm2
	movaps	 -12 * SIZE(A1), %xmm5
	movss	 %xmm8, %xmm6
	shufps	 $0x39, %xmm6, %xmm6
	mulps	 %xmm13, %xmm6
	addps	 %xmm6,  %xmm3
	movaps	  -8 * SIZE(A1), %xmm6

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE) * 4 - 128 + PREOFFSET(Y1)
#endif

	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-28 * SIZE, Y1, %xmm1)
	MOVUPS_YS1(-24 * SIZE, Y1, %xmm2)
	MOVUPS_YS1(-20 * SIZE, Y1, %xmm3)

	MOVUPS_YL1(-16 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-12 * SIZE, Y1, %xmm1)
	MOVUPS_YL1( -8 * SIZE, Y1, %xmm2)
	MOVUPS_YL1( -4 * SIZE, Y1, %xmm3)

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, A2
	subq	 $-16 * SIZE, Y1

	subq	 $1, I
	BRANCH
	jg	.L223
	ALIGN_3

.L224:
	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	movaps	 -20 * SIZE(A1), %xmm7
	mulps	 %xmm12, %xmm5
	addps	 %xmm5,  %xmm1
	movaps	 -29 * SIZE(A2), %xmm4
	mulps	 %xmm12, %xmm6
	addps	 %xmm6,  %xmm2
	movaps	 -25 * SIZE(A2), %xmm5
	mulps	 %xmm12, %xmm7
	addps	 %xmm7,  %xmm3
	movaps	 -21 * SIZE(A2), %xmm6

	movss	 %xmm4, %xmm8
	shufps	 $0x39, %xmm8, %xmm8
	mulps	 %xmm13, %xmm8
	addps	 %xmm8,  %xmm0
	movaps	 -17 * SIZE(A2), %xmm8
	movss	 %xmm5, %xmm4
	shufps	 $0x39, %xmm4, %xmm4
	mulps	 %xmm13, %xmm4
	addps	 %xmm4,  %xmm1

	movss	 %xmm6, %xmm5
	shufps	 $0x39, %xmm5, %xmm5
	mulps	 %xmm13, %xmm5
	addps	 %xmm5,  %xmm2
	movss	 %xmm8, %xmm6
	shufps	 $0x39, %xmm6, %xmm6
	mulps	 %xmm13, %xmm6
	addps	 %xmm6,  %xmm3

	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-28 * SIZE, Y1, %xmm1)
	MOVUPS_YS1(-24 * SIZE, Y1, %xmm2)
	MOVUPS_YS1(-20 * SIZE, Y1, %xmm3)

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, A2
	subq	 $-16 * SIZE, Y1
	ALIGN_3

.L225:
	testq	$8, MM
	je	.L226

	movaps	 -32 * SIZE(A1), %xmm4
	movaps	 -28 * SIZE(A1), %xmm5

	MOVUPS_YL1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-28 * SIZE, Y1, %xmm1)

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	movaps	 -29 * SIZE(A2), %xmm6
	mulps	 %xmm12, %xmm5
	addps	 %xmm5,  %xmm1
	movaps	 -25 * SIZE(A2), %xmm7

	movss	 %xmm6, %xmm8
	shufps	 $0x39, %xmm8, %xmm8
	mulps	 %xmm13, %xmm8
	addps	 %xmm8,  %xmm0

	movss	 %xmm7, %xmm6
	shufps	 $0x39, %xmm6, %xmm6
	mulps	 %xmm13, %xmm6
	addps	 %xmm6,  %xmm1
	movaps	 %xmm7, %xmm8

	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-28 * SIZE, Y1, %xmm1)

	addq	 $8 * SIZE, A1
	addq	 $8 * SIZE, A2
	addq	 $8 * SIZE, Y1
	ALIGN_3

.L226:
	testq	$4, MM
	je	.L227

	movaps	 -32 * SIZE(A1), %xmm4
	movaps	 -29 * SIZE(A2), %xmm5

	MOVUPS_YL1(-32 * SIZE, Y1, %xmm0)

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	movss	 %xmm5, %xmm8
	shufps	 $0x39, %xmm8, %xmm8
	mulps	 %xmm13, %xmm8
	addps	 %xmm8,  %xmm0

	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)

	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, A2
	addq	 $4 * SIZE, Y1
	ALIGN_3

.L227:
	testq	$2, MM
	je	.L228

	movsd	 -32 * SIZE(A1), %xmm0
	movsd	 -32 * SIZE(A2), %xmm1

	movsd	 -32 * SIZE(Y1), %xmm9

	mulps	 %xmm12, %xmm0
	addps	 %xmm0,  %xmm9
	mulps	 %xmm13, %xmm1
	addps	 %xmm1,  %xmm9

	movlps	 %xmm9, -32 * SIZE(Y1)

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, Y1
	ALIGN_3

.L228:
	testq	$1, MM
	je	.L990

	movss	 -32 * SIZE(Y1), %xmm9

	movss	 -32 * SIZE(A1), %xmm0
	movss	 -32 * SIZE(A2), %xmm1

	mulss	 %xmm12, %xmm0
	addss	 %xmm0,  %xmm9
	mulss	 %xmm13, %xmm1
	addss	 %xmm1,  %xmm9

	movss	 %xmm9, -32 * SIZE(Y1)
	jmp	 .L990
	ALIGN_3

.L230:
	cmpq	$1, N
	jne	.L990

	leaq	32 * SIZE(BUFFER), Y1
	movq	A,  A1

	movss	(X), %xmm12

	mulss	ALPHA, %xmm12
	shufps	$0, %xmm12, %xmm12

	cmpq	$3, M
	jle	.L237

	testq	$SIZE, A1
	je	.L23X

	movss	 -32 * SIZE(Y1), %xmm8
	movss	 -32 * SIZE(A1), %xmm0

	mulss	 %xmm12, %xmm0
	addss	 %xmm0,  %xmm8

	movss	 %xmm8, -32 * SIZE(Y1)

	addq	 $1 * SIZE, A1
	addq	 $1 * SIZE, Y1
	ALIGN_3

.L23X:
	testq	$2 * SIZE, A1
	je	.L23XX

	movsd	 -32 * SIZE(Y1), %xmm8
	movsd	 -32 * SIZE(A1), %xmm0

	mulps	 %xmm12, %xmm0
	addps	 %xmm0,  %xmm8

	movlps	 %xmm8, -32 * SIZE(Y1)

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, Y1
	ALIGN_3

.L23XX:
	testq	$2 * SIZE, A1
	jne	.L230

	movq	MM,  I
	sarq	$4,  I
	jle	.L235

	movaps	-32 * SIZE(A1), %xmm8
	movaps	-28 * SIZE(A1), %xmm9
	movaps	-24 * SIZE(A1), %xmm10
	movaps	-20 * SIZE(A1), %xmm11

	MOVUPS_YL1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-28 * SIZE, Y1, %xmm1)
	MOVUPS_YL1(-24 * SIZE, Y1, %xmm2)
	MOVUPS_YL1(-20 * SIZE, Y1, %xmm3)

	decq	 I
	jle	 .L234
	ALIGN_3

.L233:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 8 - 128 + PREOFFSET(A1)
#endif

	mulps	 %xmm12, %xmm8
	addps	 %xmm8,  %xmm0
	movaps	-16 * SIZE(A1), %xmm8
	mulps	 %xmm12, %xmm9
	addps	 %xmm9,  %xmm1
	movaps	-12 * SIZE(A1), %xmm9
	mulps	 %xmm12, %xmm10
	addps	 %xmm10,  %xmm2
	movaps	 -8 * SIZE(A1), %xmm10
	mulps	 %xmm12, %xmm11
	addps	 %xmm11,  %xmm3
	movaps	 -4 * SIZE(A1), %xmm11

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE) * 8 - 128 + PREOFFSET(Y1)
#endif

	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-28 * SIZE, Y1, %xmm1)
	MOVUPS_YS1(-24 * SIZE, Y1, %xmm2)
	MOVUPS_YS1(-20 * SIZE, Y1, %xmm3)

	MOVUPS_YL1(-16 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-12 * SIZE, Y1, %xmm1)
	MOVUPS_YL1( -8 * SIZE, Y1, %xmm2)
	MOVUPS_YL1( -4 * SIZE, Y1, %xmm3)

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, Y1

	subq	 $1, I
	BRANCH
	jg	.L233
	ALIGN_3

.L234:
	mulps	 %xmm12, %xmm8
	addps	 %xmm8,  %xmm0
	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)
	mulps	 %xmm12, %xmm9
	addps	 %xmm9,  %xmm1
	MOVUPS_YS1(-28 * SIZE, Y1, %xmm1)
	mulps	 %xmm12, %xmm10
	addps	 %xmm10,  %xmm2
	MOVUPS_YS1(-24 * SIZE, Y1, %xmm2)
	mulps	 %xmm12, %xmm11
	addps	 %xmm11,  %xmm3
	MOVUPS_YS1(-20 * SIZE, Y1, %xmm3)

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, Y1
	ALIGN_3

.L235:
	testq	$8, MM
	je	.L236

	movaps	-32 * SIZE(A1), %xmm8
	movaps	-28 * SIZE(A1), %xmm9

	MOVUPS_YL1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-28 * SIZE, Y1, %xmm1)

	mulps	 %xmm12, %xmm8
	addps	 %xmm8,  %xmm0
	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)
	mulps	 %xmm12, %xmm9
	addps	 %xmm9,  %xmm1
	MOVUPS_YS1(-28 * SIZE, Y1, %xmm1)

	addq	 $8 * SIZE, A1
	addq	 $8 * SIZE, Y1
	ALIGN_3

.L236:
	testq	$4, MM
	je	.L237

	movaps	-32 * SIZE(A1), %xmm8
	MOVUPS_YL1(-32 * SIZE, Y1, %xmm0)

	mulps	 %xmm12, %xmm8
	addps	 %xmm8,  %xmm0
	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)

	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, Y1
	ALIGN_3

.L237:
	testq	$2, MM
	je	.L238

	movsd	 -32 * SIZE(Y1), %xmm8
	movsd	 -32 * SIZE(A1), %xmm0

	mulps	 %xmm12, %xmm0
	addps	 %xmm0,  %xmm8

	movlps	 %xmm8, -32 * SIZE(Y1)

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, Y1
	ALIGN_3

.L238:
	testq	$1, MM
	je	.L990

	movss	 -32 * SIZE(Y1), %xmm8
	movss	 -32 * SIZE(A1), %xmm0

	mulss	 %xmm12, %xmm0
	addss	 %xmm0,  %xmm8

	movss	 %xmm8, -32 * SIZE(Y1)
	jmp	.L990
	ALIGN_4

.L300:
	cmpq	$4, N
	jl	.L310
	ALIGN_3

.L301:
	subq	$4, N

	leaq	32 * SIZE(BUFFER), Y1
	movq	A,  A1
	leaq	(A,  LDA, 2), A2
	leaq	(A,  LDA, 4), A

	movss	(X), %xmm12
	addq	INCX, X
	movss	(X), %xmm13
	addq	INCX, X
	movss	(X), %xmm14
	addq	INCX, X
	movss	(X), %xmm15
	addq	INCX, X

	movss	ALPHA, %xmm0

	mulss	%xmm0, %xmm12
	mulss	%xmm0, %xmm13
	mulss	%xmm0, %xmm14
	mulss	%xmm0, %xmm15

	shufps	$0, %xmm12, %xmm12
	shufps	$0, %xmm13, %xmm13
	shufps	$0, %xmm14, %xmm14
	shufps	$0, %xmm15, %xmm15

	cmpq	$3, M
	jle	.L307

	testq	$SIZE, A1
	je	.L30X

	movss	 -32 * SIZE(A1), %xmm0
	movss	 -32 * SIZE(A1, LDA), %xmm1
	movss	 -32 * SIZE(A2), %xmm2
	movss	 -32 * SIZE(A2, LDA), %xmm3

	movss	 -32 * SIZE(Y1), %xmm8

	mulss	 %xmm12, %xmm0
	addss	 %xmm0,  %xmm8
	mulss	 %xmm13, %xmm1
	addss	 %xmm1,  %xmm8
	mulss	 %xmm14, %xmm2
	addss	 %xmm2, %xmm8
	mulss	 %xmm15, %xmm3
	addss	 %xmm3, %xmm8

	movss	 %xmm8, -32 * SIZE(Y1)

	addq	 $1 * SIZE, A1
	addq	 $1 * SIZE, A2
	addq	 $1 * SIZE, Y1
	ALIGN_3

.L30X:
	testq	$2 * SIZE, A1
	je	.L30XX

	movsd	 -32 * SIZE(A1), %xmm0
	movsd	 -32 * SIZE(A1, LDA), %xmm1
	movsd	 -32 * SIZE(A2), %xmm2
	movsd	 -32 * SIZE(A2, LDA), %xmm3

	movsd	 -32 * SIZE(Y1), %xmm8

	mulps	 %xmm12, %xmm0
	addps	 %xmm0,  %xmm8
	mulps	 %xmm13, %xmm1
	addps	 %xmm1,  %xmm8
	mulps	 %xmm14, %xmm2
	addps	 %xmm2, %xmm8
	mulps	 %xmm15, %xmm3
	addps	 %xmm3, %xmm8

	movlps	 %xmm8, -32 * SIZE(Y1)

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, Y1
	ALIGN_3

.L30XX:
	movaps	-35 * SIZE(A1, LDA), %xmm8
	movaps	-34 * SIZE(A2), %xmm9
	movaps	-33 * SIZE(A2, LDA), %xmm10

	movq	MM,  I
	sarq	$4,  I
	jle	.L305

	movaps	 -32 * SIZE(A1), %xmm4
	movaps	 -28 * SIZE(A1), %xmm5
	movaps	 -24 * SIZE(A1), %xmm6

	MOVUPS_YL1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-28 * SIZE, Y1, %xmm1)
	MOVUPS_YL1(-24 * SIZE, Y1, %xmm2)
	MOVUPS_YL1(-20 * SIZE, Y1, %xmm3)

	decq	 I
	jle	 .L304
	ALIGN_3

.L303:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(A1)
#endif

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	movaps	 -20 * SIZE(A1), %xmm7
	mulps	 %xmm12, %xmm5
	addps	 %xmm5,  %xmm1
	movaps	 -31 * SIZE(A1, LDA), %xmm4
	mulps	 %xmm12, %xmm6
	addps	 %xmm6,  %xmm2
	movaps	 -27 * SIZE(A1, LDA), %xmm5
	mulps	 %xmm12, %xmm7
	addps	 %xmm7,  %xmm3
	movaps	 -23 * SIZE(A1, LDA), %xmm6

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(A1, LDA)
#endif

	movss	 %xmm4,  %xmm8
	shufps	 $0x93, %xmm4, %xmm8
	mulps	 %xmm13, %xmm8
	addps	 %xmm8, %xmm0
	movaps	 -19 * SIZE(A1, LDA), %xmm8
	movss	 %xmm5,  %xmm4
	shufps	 $0x93, %xmm5, %xmm4
	mulps	 %xmm13, %xmm4
	addps	 %xmm4,  %xmm1
	movaps	 -30 * SIZE(A2), %xmm4
	movss	 %xmm6,  %xmm5
	shufps	 $0x93, %xmm6, %xmm5
	mulps	 %xmm13, %xmm5
	addps	 %xmm5,  %xmm2
	movaps	 -26 * SIZE(A2), %xmm5
	movss	 %xmm8,  %xmm6
	shufps	 $0x93, %xmm8, %xmm6
	mulps	 %xmm13, %xmm6
	addps	 %xmm6,  %xmm3
	movaps	 -22 * SIZE(A2), %xmm6

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(A2)
#endif

	shufps	 $0x4e, %xmm4, %xmm9
	mulps	 %xmm14, %xmm9
	addps	 %xmm9,  %xmm0
	movaps	 -18 * SIZE(A2), %xmm9
	shufps	 $0x4e, %xmm5, %xmm4
	mulps	 %xmm14, %xmm4
	addps	 %xmm4,  %xmm1
	movaps	 -29 * SIZE(A2, LDA), %xmm4
	shufps	 $0x4e, %xmm6, %xmm5
	mulps	 %xmm14, %xmm5
	addps	 %xmm5,  %xmm2
	movaps	 -25 * SIZE(A2, LDA), %xmm5
	shufps	 $0x4e, %xmm9, %xmm6
	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm3
	movaps	 -21 * SIZE(A2, LDA), %xmm6

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(A2, LDA)
#endif

	movss	 %xmm4, %xmm10
	shufps	 $0x39, %xmm10, %xmm10
	mulps	 %xmm15, %xmm10
	addps	 %xmm10,  %xmm0
	movaps	 -17 * SIZE(A2, LDA), %xmm10
	movss	 %xmm5, %xmm4
	shufps	 $0x39, %xmm4, %xmm4
	mulps	 %xmm15, %xmm4
	addps	 %xmm4,  %xmm1
	movaps	 -16 * SIZE(A1), %xmm4
	movss	 %xmm6, %xmm5
	shufps	 $0x39, %xmm5, %xmm5
	mulps	 %xmm15, %xmm5
	addps	 %xmm5,  %xmm2
	movaps	 -12 * SIZE(A1), %xmm5
	movss	 %xmm10, %xmm6
	shufps	 $0x39, %xmm6, %xmm6
	mulps	 %xmm15, %xmm6
	addps	 %xmm6,  %xmm3
	movaps	  -8 * SIZE(A1), %xmm6

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(Y1)
#endif

	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-28 * SIZE, Y1, %xmm1)
	MOVUPS_YS1(-24 * SIZE, Y1, %xmm2)
	MOVUPS_YS1(-20 * SIZE, Y1, %xmm3)

	MOVUPS_YL1(-16 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-12 * SIZE, Y1, %xmm1)
	MOVUPS_YL1( -8 * SIZE, Y1, %xmm2)
	MOVUPS_YL1( -4 * SIZE, Y1, %xmm3)

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, A2
	subq	 $-16 * SIZE, Y1

	subq	 $1, I
	BRANCH
	jg	.L303
	ALIGN_3

.L304:
	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	movaps	 -20 * SIZE(A1), %xmm7
	mulps	 %xmm12, %xmm5
	addps	 %xmm5,  %xmm1
	movaps	 -31 * SIZE(A1, LDA), %xmm4

	mulps	 %xmm12, %xmm6
	addps	 %xmm6,  %xmm2
	movaps	 -27 * SIZE(A1, LDA), %xmm5
	mulps	 %xmm12, %xmm7
	addps	 %xmm7,  %xmm3
	movaps	 -23 * SIZE(A1, LDA), %xmm6

	movss	 %xmm4,  %xmm8
	shufps	 $0x93, %xmm4, %xmm8
	mulps	 %xmm13, %xmm8
	addps	 %xmm8, %xmm0
	movaps	 -19 * SIZE(A1, LDA), %xmm8
	movss	 %xmm5,  %xmm4
	shufps	 $0x93, %xmm5, %xmm4
	mulps	 %xmm13, %xmm4
	addps	 %xmm4,  %xmm1
	movaps	 -30 * SIZE(A2), %xmm4

	movss	 %xmm6,  %xmm5
	shufps	 $0x93, %xmm6, %xmm5
	mulps	 %xmm13, %xmm5
	addps	 %xmm5,  %xmm2
	movaps	 -26 * SIZE(A2), %xmm5
	movss	 %xmm8,  %xmm6
	shufps	 $0x93, %xmm8, %xmm6
	mulps	 %xmm13, %xmm6
	addps	 %xmm6,  %xmm3
	movaps	 -22 * SIZE(A2), %xmm6

	shufps	 $0x4e, %xmm4, %xmm9
	mulps	 %xmm14, %xmm9
	addps	 %xmm9,  %xmm0
	movaps	 -18 * SIZE(A2), %xmm9
	shufps	 $0x4e, %xmm5, %xmm4
	mulps	 %xmm14, %xmm4
	addps	 %xmm4,  %xmm1
	movaps	 -29 * SIZE(A2, LDA), %xmm4
	shufps	 $0x4e, %xmm6, %xmm5
	mulps	 %xmm14, %xmm5
	addps	 %xmm5,  %xmm2
	movaps	 -25 * SIZE(A2, LDA), %xmm5
	shufps	 $0x4e, %xmm9, %xmm6
	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm3
	movaps	 -21 * SIZE(A2, LDA), %xmm6

	movss	 %xmm4, %xmm10
	shufps	 $0x39, %xmm10, %xmm10
	mulps	 %xmm15, %xmm10
	addps	 %xmm10,  %xmm0
	movaps	 -17 * SIZE(A2, LDA), %xmm10
	movss	 %xmm5, %xmm4
	shufps	 $0x39, %xmm4, %xmm4
	mulps	 %xmm15, %xmm4
	addps	 %xmm4,  %xmm1
	movss	 %xmm6, %xmm5
	shufps	 $0x39, %xmm5, %xmm5
	mulps	 %xmm15, %xmm5
	addps	 %xmm5,  %xmm2
	movss	 %xmm10, %xmm6
	shufps	 $0x39, %xmm6, %xmm6
	mulps	 %xmm15, %xmm6
	addps	 %xmm6,  %xmm3

	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-28 * SIZE, Y1, %xmm1)
	MOVUPS_YS1(-24 * SIZE, Y1, %xmm2)
	MOVUPS_YS1(-20 * SIZE, Y1, %xmm3)

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, A2
	subq	 $-16 * SIZE, Y1
	ALIGN_3

.L305:
	testq	$8, MM
	je	.L306

	movaps	 -32 * SIZE(A1), %xmm4
	movaps	 -28 * SIZE(A1), %xmm5

	MOVUPS_YL1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-28 * SIZE, Y1, %xmm1)

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	movaps	 -31 * SIZE(A1, LDA), %xmm6
	mulps	 %xmm12, %xmm5
	addps	 %xmm5,  %xmm1
	movaps	 -27 * SIZE(A1, LDA), %xmm7

	movss	 %xmm6,  %xmm8
	shufps	 $0x93, %xmm6, %xmm8
	mulps	 %xmm13, %xmm8
	addps	 %xmm8, %xmm0
	movaps	 -30 * SIZE(A2), %xmm4
	movss	 %xmm7,  %xmm6
	shufps	 $0x93, %xmm7, %xmm6
	mulps	 %xmm13, %xmm6
	addps	 %xmm6,  %xmm1
	movaps	 %xmm7, %xmm8
	movaps	 -26 * SIZE(A2), %xmm5

	shufps	 $0x4e, %xmm4, %xmm9
	mulps	 %xmm14, %xmm9
	addps	 %xmm9,  %xmm0
	movaps	 -29 * SIZE(A2, LDA), %xmm6
	shufps	 $0x4e, %xmm5, %xmm4
	mulps	 %xmm14, %xmm4
	addps	 %xmm4,  %xmm1
	movaps	 %xmm5, %xmm9
	movaps	 -25 * SIZE(A2, LDA), %xmm7

	movss	 %xmm6, %xmm10
	shufps	 $0x39, %xmm10, %xmm10
	mulps	 %xmm15, %xmm10
	addps	 %xmm10,  %xmm0
	movss	 %xmm7, %xmm6
	shufps	 $0x39, %xmm6, %xmm6
	mulps	 %xmm15, %xmm6
	addps	 %xmm6,  %xmm1
	movaps	 %xmm7, %xmm10

	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-28 * SIZE, Y1, %xmm1)

	addq	 $8 * SIZE, A1
	addq	 $8 * SIZE, A2
	addq	 $8 * SIZE, Y1
	ALIGN_3

.L306:
	testq	$4, MM
	je	.L307

	movaps	 -32 * SIZE(A1), %xmm4
	movaps	 -31 * SIZE(A1, LDA), %xmm5

	MOVUPS_YL1(-32 * SIZE, Y1, %xmm0)

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	movaps	 -30 * SIZE(A2), %xmm6
	movss	 %xmm5,  %xmm8
	shufps	 $0x93, %xmm5, %xmm8
	mulps	 %xmm13, %xmm8
	addps	 %xmm8, %xmm0
	movaps	 -29 * SIZE(A2, LDA), %xmm7

	shufps	 $0x4e, %xmm6, %xmm9
	mulps	 %xmm14, %xmm9
	addps	 %xmm9,  %xmm0
	movss	 %xmm7, %xmm10
	shufps	 $0x39, %xmm10, %xmm10
	mulps	 %xmm15, %xmm10
	addps	 %xmm10,  %xmm0

	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)

	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, A2
	addq	 $4 * SIZE, Y1
	ALIGN_3

.L307:
	testq	$2, MM
	je	.L308

	movsd	 -32 * SIZE(A1), %xmm4
	movsd	 -32 * SIZE(A1, LDA), %xmm5
	movsd	 -32 * SIZE(A2), %xmm6
	movsd	 -32 * SIZE(A2, LDA), %xmm7

	movsd	 -32 * SIZE(Y1), %xmm0

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	mulps	 %xmm13, %xmm5
	addps	 %xmm5,  %xmm0
	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm0
	mulps	 %xmm15, %xmm7
	addps	 %xmm7,  %xmm0

	movlps	 %xmm0, -32 * SIZE(Y1)

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, Y1
	ALIGN_3

.L308:
	testq	$1, MM
	je	.L309

	movss	 -32 * SIZE(Y1), %xmm0

	movss	 -32 * SIZE(A1), %xmm4
	movss	 -32 * SIZE(A1, LDA), %xmm5
	movss	 -32 * SIZE(A2), %xmm6
	movss	 -32 * SIZE(A2, LDA), %xmm7

	mulss	 %xmm12, %xmm4
	addss	 %xmm4,  %xmm0
	mulss	 %xmm13, %xmm5
	addss	 %xmm5,  %xmm0
	mulss	 %xmm14, %xmm6
	addss	 %xmm6,  %xmm0
	mulss	 %xmm15, %xmm7
	addss	 %xmm7,  %xmm0

	movss	 %xmm0, -32 * SIZE(Y1)
	ALIGN_3

.L309:
	cmpq	$4, N
	jge	.L301
	ALIGN_3

.L310:
	cmpq	$3, N
	jne	.L320

	leaq	32 * SIZE(BUFFER), Y1
	movq	A,  A1
	leaq	(A,  LDA, 2), A2
	leaq	(A,  LDA, 4), A

	movss	(X), %xmm12
	addq	INCX, X
	movss	(X), %xmm13
	addq	INCX, X
	movss	(X), %xmm14
	addq	INCX, X

	movss	ALPHA, %xmm0

	mulss	%xmm0, %xmm12
	mulss	%xmm0, %xmm13
	mulss	%xmm0, %xmm14

	shufps	$0, %xmm12, %xmm12
	shufps	$0, %xmm13, %xmm13
	shufps	$0, %xmm14, %xmm14

	cmpq	$3, M
	jle	.L317

	testq	$SIZE, A1
	je	.L31X

	movss	 -32 * SIZE(A1), %xmm0
	movss	 -32 * SIZE(A1, LDA), %xmm1
	movss	 -32 * SIZE(A2), %xmm2

	movss	 -32 * SIZE(Y1), %xmm8

	mulss	 %xmm12, %xmm0
	addss	 %xmm0,  %xmm8
	mulss	 %xmm13, %xmm1
	addss	 %xmm1,  %xmm8
	mulss	 %xmm14, %xmm2
	addss	 %xmm2, %xmm8

	movss	 %xmm8, -32 * SIZE(Y1)

	addq	 $1 * SIZE, A1
	addq	 $1 * SIZE, A2
	addq	 $1 * SIZE, Y1
	ALIGN_3

.L31X:
	testq	$2 * SIZE, A1
	je	.L31XX

	movsd	 -32 * SIZE(A1), %xmm0
	movsd	 -32 * SIZE(A1, LDA), %xmm1
	movsd	 -32 * SIZE(A2), %xmm2

	movsd	 -32 * SIZE(Y1), %xmm8

	mulps	 %xmm12, %xmm0
	addps	 %xmm0,  %xmm8
	mulps	 %xmm13, %xmm1
	addps	 %xmm1,  %xmm8
	mulps	 %xmm14, %xmm2
	addps	 %xmm2, %xmm8

	movlps	 %xmm8, -32 * SIZE(Y1)

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, Y1
	ALIGN_3

.L31XX:
	movaps	-35 * SIZE(A1, LDA), %xmm8
	movaps	-34 * SIZE(A2), %xmm9
	movaps	-33 * SIZE(A2, LDA), %xmm10

	movq	MM,  I
	sarq	$4,  I
	jle	.L315

	movaps	 -32 * SIZE(A1), %xmm4
	movaps	 -28 * SIZE(A1), %xmm5
	movaps	 -24 * SIZE(A1), %xmm6

	MOVUPS_YL1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-28 * SIZE, Y1, %xmm1)
	MOVUPS_YL1(-24 * SIZE, Y1, %xmm2)
	MOVUPS_YL1(-20 * SIZE, Y1, %xmm3)

	decq	 I
	jle	 .L314
	ALIGN_3

.L313:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 3 - 128 + PREOFFSET(A1)
#endif

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	movaps	 -20 * SIZE(A1), %xmm7
	mulps	 %xmm12, %xmm5
	addps	 %xmm5,  %xmm1
	movaps	 -31 * SIZE(A1, LDA), %xmm4
	mulps	 %xmm12, %xmm6
	addps	 %xmm6,  %xmm2
	movaps	 -27 * SIZE(A1, LDA), %xmm5
	mulps	 %xmm12, %xmm7
	addps	 %xmm7,  %xmm3
	movaps	 -23 * SIZE(A1, LDA), %xmm6

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 3 - 128 + PREOFFSET(A1, LDA)
#endif

	movss	 %xmm4,  %xmm8
	shufps	 $0x93, %xmm4, %xmm8
	mulps	 %xmm13, %xmm8
	addps	 %xmm8, %xmm0
	movaps	 -19 * SIZE(A1, LDA), %xmm8
	movss	 %xmm5,  %xmm4
	shufps	 $0x93, %xmm5, %xmm4
	mulps	 %xmm13, %xmm4
	addps	 %xmm4,  %xmm1
	movaps	 -30 * SIZE(A2), %xmm4
	movss	 %xmm6,  %xmm5
	shufps	 $0x93, %xmm6, %xmm5
	mulps	 %xmm13, %xmm5
	addps	 %xmm5,  %xmm2
	movaps	 -26 * SIZE(A2), %xmm5
	movss	 %xmm8,  %xmm6
	shufps	 $0x93, %xmm8, %xmm6
	mulps	 %xmm13, %xmm6
	addps	 %xmm6,  %xmm3
	movaps	 -22 * SIZE(A2), %xmm6

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 3 - 128 + PREOFFSET(A2)
#endif

	shufps	 $0x4e, %xmm4, %xmm9
	mulps	 %xmm14, %xmm9
	addps	 %xmm9,  %xmm0
	movaps	 -18 * SIZE(A2), %xmm9
	shufps	 $0x4e, %xmm5, %xmm4
	mulps	 %xmm14, %xmm4
	addps	 %xmm4,  %xmm1
	movaps	 -16 * SIZE(A1), %xmm4
	shufps	 $0x4e, %xmm6, %xmm5
	mulps	 %xmm14, %xmm5
	addps	 %xmm5,  %xmm2
	movaps	 -12 * SIZE(A1), %xmm5
	shufps	 $0x4e, %xmm9, %xmm6
	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm3
	movaps	  -8 * SIZE(A1), %xmm6

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE) * 3 - 128 + PREOFFSET(Y1)
#endif

	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-28 * SIZE, Y1, %xmm1)
	MOVUPS_YS1(-24 * SIZE, Y1, %xmm2)
	MOVUPS_YS1(-20 * SIZE, Y1, %xmm3)

	MOVUPS_YL1(-16 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-12 * SIZE, Y1, %xmm1)
	MOVUPS_YL1( -8 * SIZE, Y1, %xmm2)
	MOVUPS_YL1( -4 * SIZE, Y1, %xmm3)

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, A2
	subq	 $-16 * SIZE, Y1

	subq	 $1, I
	BRANCH
	jg	.L313
	ALIGN_3

.L314:
	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	movaps	 -20 * SIZE(A1), %xmm7
	mulps	 %xmm12, %xmm5
	addps	 %xmm5,  %xmm1
	movaps	 -31 * SIZE(A1, LDA), %xmm4

	mulps	 %xmm12, %xmm6
	addps	 %xmm6,  %xmm2
	movaps	 -27 * SIZE(A1, LDA), %xmm5
	mulps	 %xmm12, %xmm7
	addps	 %xmm7,  %xmm3
	movaps	 -23 * SIZE(A1, LDA), %xmm6

	movss	 %xmm4,  %xmm8
	shufps	 $0x93, %xmm4, %xmm8
	mulps	 %xmm13, %xmm8
	addps	 %xmm8, %xmm0
	movaps	 -19 * SIZE(A1, LDA), %xmm8
	movss	 %xmm5,  %xmm4
	shufps	 $0x93, %xmm5, %xmm4
	mulps	 %xmm13, %xmm4
	addps	 %xmm4,  %xmm1
	movaps	 -30 * SIZE(A2), %xmm4

	movss	 %xmm6,  %xmm5
	shufps	 $0x93, %xmm6, %xmm5
	mulps	 %xmm13, %xmm5
	addps	 %xmm5,  %xmm2
	movaps	 -26 * SIZE(A2), %xmm5
	movss	 %xmm8,  %xmm6
	shufps	 $0x93, %xmm8, %xmm6
	mulps	 %xmm13, %xmm6
	addps	 %xmm6,  %xmm3
	movaps	 -22 * SIZE(A2), %xmm6

	shufps	 $0x4e, %xmm4, %xmm9
	mulps	 %xmm14, %xmm9
	addps	 %xmm9,  %xmm0
	movaps	 -18 * SIZE(A2), %xmm9
	shufps	 $0x4e, %xmm5, %xmm4
	mulps	 %xmm14, %xmm4
	addps	 %xmm4,  %xmm1
	shufps	 $0x4e, %xmm6, %xmm5
	mulps	 %xmm14, %xmm5
	addps	 %xmm5,  %xmm2
	shufps	 $0x4e, %xmm9, %xmm6
	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm3

	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-28 * SIZE, Y1, %xmm1)
	MOVUPS_YS1(-24 * SIZE, Y1, %xmm2)
	MOVUPS_YS1(-20 * SIZE, Y1, %xmm3)

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, A2
	subq	 $-16 * SIZE, Y1
	ALIGN_3

.L315:
	testq	$8, MM
	je	.L316

	movaps	 -32 * SIZE(A1), %xmm4
	movaps	 -28 * SIZE(A1), %xmm5

	MOVUPS_YL1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-28 * SIZE, Y1, %xmm1)

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	movaps	 -31 * SIZE(A1, LDA), %xmm6
	mulps	 %xmm12, %xmm5
	addps	 %xmm5,  %xmm1
	movaps	 -27 * SIZE(A1, LDA), %xmm7

	movss	 %xmm6,  %xmm8
	shufps	 $0x93, %xmm6, %xmm8
	mulps	 %xmm13, %xmm8
	addps	 %xmm8, %xmm0
	movaps	 -30 * SIZE(A2), %xmm4
	movss	 %xmm7,  %xmm6
	shufps	 $0x93, %xmm7, %xmm6
	mulps	 %xmm13, %xmm6
	addps	 %xmm6,  %xmm1
	movaps	 %xmm7, %xmm8
	movaps	 -26 * SIZE(A2), %xmm5

	shufps	 $0x4e, %xmm4, %xmm9
	mulps	 %xmm14, %xmm9
	addps	 %xmm9,  %xmm0
	shufps	 $0x4e, %xmm5, %xmm4
	mulps	 %xmm14, %xmm4
	addps	 %xmm4,  %xmm1
	movaps	 %xmm5, %xmm9

	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-28 * SIZE, Y1, %xmm1)

	addq	 $8 * SIZE, A1
	addq	 $8 * SIZE, A2
	addq	 $8 * SIZE, Y1
	ALIGN_3

.L316:
	testq	$4, MM
	je	.L317

	movaps	 -32 * SIZE(A1), %xmm4
	movaps	 -31 * SIZE(A1, LDA), %xmm5

	MOVUPS_YL1(-32 * SIZE, Y1, %xmm0)

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	movaps	 -30 * SIZE(A2), %xmm6
	movss	 %xmm5,  %xmm8
	shufps	 $0x93, %xmm5, %xmm8
	mulps	 %xmm13, %xmm8
	addps	 %xmm8, %xmm0

	shufps	 $0x4e, %xmm6, %xmm9
	mulps	 %xmm14, %xmm9
	addps	 %xmm9,  %xmm0

	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)

	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, A2
	addq	 $4 * SIZE, Y1
	ALIGN_3

.L317:
	testq	$2, MM
	je	.L318

	movsd	 -32 * SIZE(A1), %xmm4
	movsd	 -32 * SIZE(A1, LDA), %xmm5
	movsd	 -32 * SIZE(A2), %xmm6

	movsd	 -32 * SIZE(Y1), %xmm0

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	mulps	 %xmm13, %xmm5
	addps	 %xmm5,  %xmm0
	mulps	 %xmm14, %xmm6
	addps	 %xmm6,  %xmm0

	movlps	 %xmm0, -32 * SIZE(Y1)

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, Y1
	ALIGN_3

.L318:
	testq	$1, MM
	je	.L990

	movss	 -32 * SIZE(Y1), %xmm0

	movss	 -32 * SIZE(A1), %xmm4
	movss	 -32 * SIZE(A1, LDA), %xmm5
	movss	 -32 * SIZE(A2), %xmm6

	mulss	 %xmm12, %xmm4
	addss	 %xmm4,  %xmm0
	mulss	 %xmm13, %xmm5
	addss	 %xmm5,  %xmm0
	mulss	 %xmm14, %xmm6
	addss	 %xmm6,  %xmm0

	movss	 %xmm0, -32 * SIZE(Y1)
	jmp	 .L990
	ALIGN_3

.L320:
	cmpq	$2, N
	jne	.L330

	leaq	32 * SIZE(BUFFER), Y1
	movq	A,  A1
	leaq	(A,  LDA, 1), A2

	movss	(X), %xmm12
	addq	INCX, X
	movss	(X), %xmm13
	addq	INCX, X

	movss	ALPHA, %xmm0

	mulss	%xmm0, %xmm12
	mulss	%xmm0, %xmm13

	shufps	$0, %xmm12, %xmm12
	shufps	$0, %xmm13, %xmm13

	cmpq	$3, M
	jle	.L327

	testq	$SIZE, A1
	je	.L32X

	movss	 -32 * SIZE(Y1), %xmm9

	movss	 -32 * SIZE(A1), %xmm0
	movss	 -32 * SIZE(A2), %xmm1

	mulss	 %xmm12, %xmm0
	addss	 %xmm0,  %xmm9
	mulss	 %xmm13, %xmm1
	addss	 %xmm1,  %xmm9

	movss	 %xmm9, -32 * SIZE(Y1)

	addq	 $1 * SIZE, A1
	addq	 $1 * SIZE, A2
	addq	 $1 * SIZE, Y1
	ALIGN_3

.L32X:
	testq	$2 * SIZE, A1
	je	.L32XX

	movsd	 -32 * SIZE(A1), %xmm0
	movsd	 -32 * SIZE(A2), %xmm1

	movsd	 -32 * SIZE(Y1), %xmm8

	mulps	 %xmm12, %xmm0
	addps	 %xmm0,  %xmm8
	mulps	 %xmm13, %xmm1
	addps	 %xmm1,  %xmm8

	movlps	 %xmm8, -32 * SIZE(Y1)

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, Y1
	ALIGN_3

.L32XX:
	movaps	-35 * SIZE(A1, LDA), %xmm8

	movq	MM,  I
	sarq	$4,  I
	jle	.L325

	movaps	 -32 * SIZE(A1), %xmm4
	movaps	 -28 * SIZE(A1), %xmm5
	movaps	 -24 * SIZE(A1), %xmm6

	MOVUPS_YL1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-28 * SIZE, Y1, %xmm1)
	MOVUPS_YL1(-24 * SIZE, Y1, %xmm2)
	MOVUPS_YL1(-20 * SIZE, Y1, %xmm3)

	decq	 I
	jle	 .L324
	ALIGN_3

.L323:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 4 - 128 + PREOFFSET(A1)
#endif

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	movaps	 -20 * SIZE(A1), %xmm7
	mulps	 %xmm12, %xmm5
	addps	 %xmm5,  %xmm1
	movaps	 -31 * SIZE(A2), %xmm4
	mulps	 %xmm12, %xmm6
	addps	 %xmm6,  %xmm2
	movaps	 -27 * SIZE(A2), %xmm5
	mulps	 %xmm12, %xmm7
	addps	 %xmm7,  %xmm3
	movaps	 -23 * SIZE(A2), %xmm6

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 4 - 128 + PREOFFSET(A2)
#endif

	movss	 %xmm4,  %xmm8
	shufps	 $0x93, %xmm4, %xmm8
	mulps	 %xmm13, %xmm8
	addps	 %xmm8, %xmm0
	movaps	 -19 * SIZE(A2), %xmm8
	movss	 %xmm5,  %xmm4
	shufps	 $0x93, %xmm5, %xmm4
	mulps	 %xmm13, %xmm4
	addps	 %xmm4,  %xmm1
	movaps	 -16 * SIZE(A1), %xmm4
	movss	 %xmm6,  %xmm5
	shufps	 $0x93, %xmm6, %xmm5
	mulps	 %xmm13, %xmm5
	addps	 %xmm5,  %xmm2
	movaps	 -12 * SIZE(A1), %xmm5
	movss	 %xmm8,  %xmm6
	shufps	 $0x93, %xmm8, %xmm6
	mulps	 %xmm13, %xmm6
	addps	 %xmm6,  %xmm3
	movaps	  -8 * SIZE(A1), %xmm6

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE) * 4 - 128 + PREOFFSET(Y1)
#endif

	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-28 * SIZE, Y1, %xmm1)
	MOVUPS_YS1(-24 * SIZE, Y1, %xmm2)
	MOVUPS_YS1(-20 * SIZE, Y1, %xmm3)

	MOVUPS_YL1(-16 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-12 * SIZE, Y1, %xmm1)
	MOVUPS_YL1( -8 * SIZE, Y1, %xmm2)
	MOVUPS_YL1( -4 * SIZE, Y1, %xmm3)

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, A2
	subq	 $-16 * SIZE, Y1

	subq	 $1, I
	BRANCH
	jg	.L323
	ALIGN_3

.L324:
	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	movaps	 -20 * SIZE(A1), %xmm7
	mulps	 %xmm12, %xmm5
	addps	 %xmm5,  %xmm1
	movaps	 -31 * SIZE(A2), %xmm4

	mulps	 %xmm12, %xmm6
	addps	 %xmm6,  %xmm2
	movaps	 -27 * SIZE(A2), %xmm5
	mulps	 %xmm12, %xmm7
	addps	 %xmm7,  %xmm3
	movaps	 -23 * SIZE(A2), %xmm6

	movss	 %xmm4,  %xmm8
	shufps	 $0x93, %xmm4, %xmm8
	mulps	 %xmm13, %xmm8
	addps	 %xmm8, %xmm0
	movaps	 -19 * SIZE(A2), %xmm8
	movss	 %xmm5,  %xmm4
	shufps	 $0x93, %xmm5, %xmm4
	mulps	 %xmm13, %xmm4
	addps	 %xmm4,  %xmm1

	movss	 %xmm6,  %xmm5
	shufps	 $0x93, %xmm6, %xmm5
	mulps	 %xmm13, %xmm5
	addps	 %xmm5,  %xmm2
	movss	 %xmm8,  %xmm6
	shufps	 $0x93, %xmm8, %xmm6
	mulps	 %xmm13, %xmm6
	addps	 %xmm6,  %xmm3

	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-28 * SIZE, Y1, %xmm1)
	MOVUPS_YS1(-24 * SIZE, Y1, %xmm2)
	MOVUPS_YS1(-20 * SIZE, Y1, %xmm3)

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, A2
	subq	 $-16 * SIZE, Y1
	ALIGN_3

.L325:
	testq	$8, MM
	je	.L326

	movaps	 -32 * SIZE(A1), %xmm4
	movaps	 -28 * SIZE(A1), %xmm5

	MOVUPS_YL1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-28 * SIZE, Y1, %xmm1)

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	movaps	 -31 * SIZE(A2), %xmm6
	mulps	 %xmm12, %xmm5
	addps	 %xmm5,  %xmm1
	movaps	 -27 * SIZE(A2), %xmm7

	movss	 %xmm6,  %xmm8
	shufps	 $0x93, %xmm6, %xmm8
	mulps	 %xmm13, %xmm8
	addps	 %xmm8, %xmm0
	movss	 %xmm7,  %xmm6
	shufps	 $0x93, %xmm7, %xmm6
	mulps	 %xmm13, %xmm6
	addps	 %xmm6,  %xmm1
	movaps	 %xmm7, %xmm8

	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-28 * SIZE, Y1, %xmm1)

	addq	 $8 * SIZE, A1
	addq	 $8 * SIZE, A2
	addq	 $8 * SIZE, Y1
	ALIGN_3

.L326:
	testq	$4, MM
	je	.L327

	movaps	 -32 * SIZE(A1), %xmm4
	movaps	 -31 * SIZE(A2), %xmm5

	MOVUPS_YL1(-32 * SIZE, Y1, %xmm0)

	mulps	 %xmm12, %xmm4
	addps	 %xmm4,  %xmm0
	movss	 %xmm5,  %xmm8
	shufps	 $0x93, %xmm5, %xmm8
	mulps	 %xmm13, %xmm8
	addps	 %xmm8, %xmm0

	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)

	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, A2
	addq	 $4 * SIZE, Y1
	ALIGN_3

.L327:
	testq	$2, MM
	je	.L328

	movsd	 -32 * SIZE(A1), %xmm0
	movsd	 -32 * SIZE(A2), %xmm1

	movsd	 -32 * SIZE(Y1), %xmm8

	mulps	 %xmm12, %xmm0
	addps	 %xmm0,  %xmm8
	mulps	 %xmm13, %xmm1
	addps	 %xmm1,  %xmm8

	movlps	 %xmm8, -32 * SIZE(Y1)

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, Y1
	ALIGN_3

.L328:
	testq	$1, MM
	je	.L990

	movss	 -32 * SIZE(Y1), %xmm8

	movss	 -32 * SIZE(A1), %xmm0
	movss	 -32 * SIZE(A2), %xmm1

	mulss	 %xmm12, %xmm0
	addss	 %xmm0,  %xmm8
	mulss	 %xmm13, %xmm1
	addss	 %xmm1,  %xmm8

	movss	 %xmm8, -32 * SIZE(Y1)
	jmp	 .L990
	ALIGN_3

.L330:
	cmpq	$1, N
	jne	.L990

	leaq	32 * SIZE(BUFFER), Y1
	movq	A,  A1

	movss	(X), %xmm12

	mulss	ALPHA, %xmm12
	shufps	$0, %xmm12, %xmm12

	cmpq	$3, M
	jle	.L337

	testq	$SIZE, A1
	je	.L33X

	movss	 -32 * SIZE(Y1), %xmm8
	movss	 -32 * SIZE(A1), %xmm0

	mulss	 %xmm12, %xmm0
	addss	 %xmm0,  %xmm8

	movss	 %xmm8, -32 * SIZE(Y1)

	addq	 $1 * SIZE, A1
	addq	 $1 * SIZE, Y1
	ALIGN_3

.L33X:
	testq	$2 * SIZE, A1
	je	.L33XX

	movsd	 -32 * SIZE(Y1), %xmm8
	movsd	 -32 * SIZE(A1), %xmm0

	mulps	 %xmm12, %xmm0
	addps	 %xmm0,  %xmm8

	movlps	 %xmm8, -32 * SIZE(Y1)

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, Y1
	ALIGN_3

.L33XX:
	movq	MM,  I
	sarq	$4,  I
	jle	.L335

	movaps	-32 * SIZE(A1), %xmm8
	movaps	-28 * SIZE(A1), %xmm9
	movaps	-24 * SIZE(A1), %xmm10
	movaps	-20 * SIZE(A1), %xmm11

	MOVUPS_YL1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-28 * SIZE, Y1, %xmm1)
	MOVUPS_YL1(-24 * SIZE, Y1, %xmm2)
	MOVUPS_YL1(-20 * SIZE, Y1, %xmm3)

	decq	 I
	jle	 .L334
	ALIGN_3

.L333:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 8 - 128 + PREOFFSET(A1)
#endif

	mulps	 %xmm12, %xmm8
	addps	 %xmm8,  %xmm0
	movaps	-16 * SIZE(A1), %xmm8
	mulps	 %xmm12, %xmm9
	addps	 %xmm9,  %xmm1
	movaps	-12 * SIZE(A1), %xmm9
	mulps	 %xmm12, %xmm10
	addps	 %xmm10,  %xmm2
	movaps	 -8 * SIZE(A1), %xmm10
	mulps	 %xmm12, %xmm11
	addps	 %xmm11,  %xmm3
	movaps	 -4 * SIZE(A1), %xmm11

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE) * 8 - 128 + PREOFFSET(Y1)
#endif

	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YS1(-28 * SIZE, Y1, %xmm1)
	MOVUPS_YS1(-24 * SIZE, Y1, %xmm2)
	MOVUPS_YS1(-20 * SIZE, Y1, %xmm3)

	MOVUPS_YL1(-16 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-12 * SIZE, Y1, %xmm1)
	MOVUPS_YL1( -8 * SIZE, Y1, %xmm2)
	MOVUPS_YL1( -4 * SIZE, Y1, %xmm3)

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, Y1

	subq	 $1, I
	BRANCH
	jg	.L333
	ALIGN_3

.L334:
	mulps	 %xmm12, %xmm8
	addps	 %xmm8,  %xmm0
	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)
	mulps	 %xmm12, %xmm9
	addps	 %xmm9,  %xmm1
	MOVUPS_YS1(-28 * SIZE, Y1, %xmm1)
	mulps	 %xmm12, %xmm10
	addps	 %xmm10,  %xmm2
	MOVUPS_YS1(-24 * SIZE, Y1, %xmm2)
	mulps	 %xmm12, %xmm11
	addps	 %xmm11,  %xmm3
	MOVUPS_YS1(-20 * SIZE, Y1, %xmm3)

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, Y1
	ALIGN_3

.L335:
	testq	$8, MM
	je	.L336

	movaps	-32 * SIZE(A1), %xmm8
	movaps	-28 * SIZE(A1), %xmm9

	MOVUPS_YL1(-32 * SIZE, Y1, %xmm0)
	MOVUPS_YL1(-28 * SIZE, Y1, %xmm1)

	mulps	 %xmm12, %xmm8
	addps	 %xmm8,  %xmm0
	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)
	mulps	 %xmm12, %xmm9
	addps	 %xmm9,  %xmm1
	MOVUPS_YS1(-28 * SIZE, Y1, %xmm1)

	addq	 $8 * SIZE, A1
	addq	 $8 * SIZE, Y1
	ALIGN_3

.L336:
	testq	$4, MM
	je	.L337

	movaps	-32 * SIZE(A1), %xmm8
	MOVUPS_YL1(-32 * SIZE, Y1, %xmm0)

	mulps	 %xmm12, %xmm8
	addps	 %xmm8,  %xmm0
	MOVUPS_YS1(-32 * SIZE, Y1, %xmm0)

	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, Y1
	ALIGN_3

.L337:
	testq	$2, MM
	je	.L338

	movsd	 -32 * SIZE(Y1), %xmm8
	movsd	 -32 * SIZE(A1), %xmm0

	mulps	 %xmm12, %xmm0
	addps	 %xmm0,  %xmm8

	movlps	 %xmm8, -32 * SIZE(Y1)

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, Y1
	ALIGN_3

.L338:
	testq	$1, MM
	je	.L990

	movss	 -32 * SIZE(Y1), %xmm8
	movss	 -32 * SIZE(A1), %xmm0

	mulss	 %xmm12, %xmm0
	addss	 %xmm0,  %xmm8

	movss	 %xmm8, -32 * SIZE(Y1)
	jmp	.L990
#endif
	ALIGN_4


.L990:
	movq	Y,  Y1

	movq	M,  %rax
	sarq	$3,  %rax
	jle	.L994
	ALIGN_3
.L992:
	movsd	0 * SIZE(BUFFER), %xmm0
	movhps	2 * SIZE(BUFFER), %xmm0
	movsd	4 * SIZE(BUFFER), %xmm4
	movhps	6 * SIZE(BUFFER), %xmm4

	pshufd	$0x01, %xmm0, %xmm1
	pshufd	$0x02, %xmm0, %xmm2
	pshufd	$0x03, %xmm0, %xmm3

	pshufd	$0x01, %xmm4, %xmm5
	pshufd	$0x02, %xmm4, %xmm6
	pshufd	$0x03, %xmm4, %xmm7

	addss	(Y), %xmm0
	addq	INCY, Y
	addss	(Y), %xmm1
	addq	INCY, Y
	addss	(Y), %xmm2
	addq	INCY, Y
	addss	(Y), %xmm3
	addq	INCY, Y
	addss	(Y), %xmm4
	addq	INCY, Y
	addss	(Y), %xmm5
	addq	INCY, Y
	addss	(Y), %xmm6
	addq	INCY, Y
	addss	(Y), %xmm7
	addq	INCY, Y

	movss	%xmm0, (Y1)
	addq	INCY, Y1
	movss	%xmm1, (Y1)
	addq	INCY, Y1
	movss	%xmm2, (Y1)
	addq	INCY, Y1
	movss	%xmm3, (Y1)
	addq	INCY, Y1
	movss	%xmm4, (Y1)
	addq	INCY, Y1
	movss	%xmm5, (Y1)
	addq	INCY, Y1
	movss	%xmm6, (Y1)
	addq	INCY, Y1
	movss	%xmm7, (Y1)
	addq	INCY, Y1

	addq	$8 * SIZE, BUFFER
	decq	%rax
	jg	.L992
	ALIGN_3

.L994:
	testq	$7, M
	jle	.L999

	testq	$4, M
	jle	.L995

	movsd	0 * SIZE(BUFFER), %xmm0
	movhps	2 * SIZE(BUFFER), %xmm0

	pshufd	$0x01, %xmm0, %xmm1
	pshufd	$0x02, %xmm0, %xmm2
	pshufd	$0x03, %xmm0, %xmm3

	addss	(Y), %xmm0
	addq	INCY, Y
	addss	(Y), %xmm1
	addq	INCY, Y
	addss	(Y), %xmm2
	addq	INCY, Y
	addss	(Y), %xmm3
	addq	INCY, Y

	movss	%xmm0, (Y1)
	addq	INCY, Y1
	movss	%xmm1, (Y1)
	addq	INCY, Y1
	movss	%xmm2, (Y1)
	addq	INCY, Y1
	movss	%xmm3, (Y1)
	addq	INCY, Y1

	addq	$4 * SIZE, BUFFER
	ALIGN_3

.L995:
	testq	$2, M
	jle	.L996

	movsd	(BUFFER), %xmm0

	pshufd	$0x01, %xmm0, %xmm1

	addss	(Y), %xmm0
	addq	INCY, Y
	addss	(Y), %xmm1
	addq	INCY, Y

	movss	%xmm0, (Y1)
	addq	INCY, Y1
	movss	%xmm1, (Y1)
	addq	INCY, Y1

	addq	$2 * SIZE, BUFFER
	ALIGN_3

.L996:
	testq	$1, M
	jle	.L999

	movss	(BUFFER), %xmm0

	addss	(Y), %xmm0

	movss	%xmm0, (Y1)
	ALIGN_3

.L999:
	leaq	(,M,SIZE),%rax
	addq	%rax,AA
	jmp		.L0t
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
