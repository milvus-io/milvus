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
#define MMM		56(%rsp)
#define NN		64(%rsp)
#define AA		72(%rsp)
#define LDAX	80(%rsp)

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
#define MMM	232(%rsp)
#define NN	240(%rsp)
#define AA	248(%rsp)
#define LDAX 256(%rsp)

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
#define X1	%rbp

#define Y1	INCX

#ifdef ALIGNED_ACCESS
#define MM	%r15
#else
#define MM	M
#endif

#define ALPHA	%xmm7

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

	movq	OLD_M,	      MMM
	movq	OLD_N,        NN
	movq	OLD_A,        X
	movq	X,	      AA
	movq	OLD_LDA,      X
	movq	X,	      LDAX
	movq	OLD_X,        X
#else
	movq	OLD_M,	      MMM
	movq	OLD_N,        NN
	movq	OLD_A,        AA
	movq	OLD_LDA,      LDAX
#endif
#ifndef WINDOWS_ABI
	pshufd	$0, %xmm0, ALPHA
#else
	pshufd	$0, %xmm3, ALPHA
#endif


.L0t:
	xorq	M,M
	addq	$1,M
	salq	$22,M
	subq	M,MMM
	jge		.L00t
	ALIGN_4

	movq	MMM,%rax
	addq	M,%rax
	jle		.L999x
	movq	%rax,M

.L00t:
	movq	LDAX,LDA
	movq	NN,N
	movq	AA,A
	movq	STACK_INCX,   INCX
	movq	STACK_Y,      Y
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

	testq	M, M
	jle	.L999
	testq	N, N
	jle	.L999
	ALIGN_4

	subq	$-32 * SIZE, A

#ifdef ALIGNED_ACCESS
	movq	A,  %rax
	andq	$4 * SIZE - 1, %rax
	addq	%rax, BUFFER
#endif

	movq	BUFFER, X1

	movq	M,  I
	sarq	$3, I
	jle	.L05
	ALIGN_4

.L02:
	movss	(X), %xmm0
	addq	INCX, X
	movss	(X), %xmm1
	addq	INCX, X

	movss	(X), %xmm2
	addq	INCX, X
	movss	(X), %xmm3
	addq	INCX, X

	movss	(X), %xmm4
	addq	INCX, X
	movss	(X), %xmm5
	addq	INCX, X

	movss	(X), %xmm6
	addq	INCX, X
	movss	(X), %xmm8
	addq	INCX, X

	movss	%xmm0, 0 * SIZE(X1)
	movss	%xmm1, 1 * SIZE(X1)
	movss	%xmm2, 2 * SIZE(X1)
	movss	%xmm3, 3 * SIZE(X1)
	movss	%xmm4, 4 * SIZE(X1)
	movss	%xmm5, 5 * SIZE(X1)
	movss	%xmm6, 6 * SIZE(X1)
	movss	%xmm8, 7 * SIZE(X1)

	addq	$8 * SIZE, X1
	decq	I
	jg	.L02
	ALIGN_4

.L05:
	movq	M,  I
	andq	$7, I
	jle	.L10
	ALIGN_2

.L06:
	movss	(X), %xmm0
	addq	INCX, X
	movss	%xmm0, 0 * SIZE(X1)
	addq	$SIZE, X1
	decq	I
	jg	.L06
	ALIGN_4

.L10:
	movq	Y, Y1

#ifdef ALIGNED_ACCESS
	testq	$4 * SIZE - 1, LDA
	jne	.L100
#endif

#if GEMV_UNROLL >= 8

	cmpq	$8, N
	jl	.L20
	ALIGN_3

.L11:
	subq	$8, N

	leaq	32 * SIZE(BUFFER), X1

	movq	A, A1
	leaq	(A1, LDA, 4), A2
	leaq	(A1, LDA, 8), A

	xorps	%xmm8, %xmm8
	xorps	%xmm9, %xmm9
	xorps	%xmm10, %xmm10
	xorps	%xmm11, %xmm11
	xorps	%xmm12, %xmm12
	xorps	%xmm13, %xmm13
	xorps	%xmm14, %xmm14
	xorps	%xmm15, %xmm15

#ifdef ALIGNED_ACCESS
	cmpq	$3, M
	jle	.L17

	testq	$SIZE, A1
	je	.L1X

	movss	-32 * SIZE(A1), %xmm0
	movss	-32 * SIZE(X1), %xmm4
	mulss	%xmm4, %xmm0
	addss	%xmm0, %xmm8
	movss	-32 * SIZE(A1, LDA, 1), %xmm1
	mulss	%xmm4, %xmm1
	addss	%xmm1, %xmm9
	movss	-32 * SIZE(A1, LDA, 2), %xmm2
	mulss	%xmm4, %xmm2
	addss	%xmm2, %xmm10
	movss	-32 * SIZE(A1, LDA3, 1), %xmm3
	mulss	%xmm4, %xmm3
	addss	%xmm3, %xmm11
	movss	-32 * SIZE(A2), %xmm0
	mulss	%xmm4, %xmm0
	addss	%xmm0, %xmm12
	movss	-32 * SIZE(A2, LDA, 1), %xmm1
	mulss	%xmm4, %xmm1
	addss	%xmm1, %xmm13
	movss	-32 * SIZE(A2, LDA, 2), %xmm2
	mulss	%xmm4, %xmm2
	addss	%xmm2, %xmm14
	movss	-32 * SIZE(A2, LDA3, 1), %xmm3
	mulss	%xmm4, %xmm3
	addss	%xmm3, %xmm15

	addq	 $1 * SIZE, A1
	addq	 $1 * SIZE, A2
	addq	 $1 * SIZE, X1
	ALIGN_3

.L1X:
	testq	$2 * SIZE, A1
	je	.L1XX

#ifdef movsd
	xorps	%xmm0, %xmm0
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(A1), %xmm0
	movsd	-32 * SIZE(X1), %xmm4
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
#ifdef movsd
	xorps	%xmm1, %xmm1
#endif
	movsd	-32 * SIZE(A1, LDA, 1), %xmm1
	mulps	%xmm4, %xmm1
	addps	%xmm1, %xmm9
#ifdef movsd
	xorps	%xmm2, %xmm2
#endif
	movsd	-32 * SIZE(A1, LDA, 2), %xmm2
	mulps	%xmm4, %xmm2
	addps	%xmm2, %xmm10
#ifdef movsd
	xorps	%xmm3, %xmm3
#endif
	movsd	-32 * SIZE(A1, LDA3, 1), %xmm3
	mulps	%xmm4, %xmm3
	addps	%xmm3, %xmm11
	movsd	-32 * SIZE(A2), %xmm0
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm12
	movsd	-32 * SIZE(A2, LDA, 1), %xmm1
	mulps	%xmm4, %xmm1
	addps	%xmm1, %xmm13
	movsd	-32 * SIZE(A2, LDA, 2), %xmm2
	mulps	%xmm4, %xmm2
	addps	%xmm2, %xmm14
	movsd	-32 * SIZE(A2, LDA3, 1), %xmm3
	mulps	%xmm4, %xmm3
	addps	%xmm3, %xmm15

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, X1
	ALIGN_3

.L1XX:
#endif

	MOVUPS_XL1 (-32 * SIZE, X1, %xmm4)
	MOVUPS_XL1 (-28 * SIZE, X1, %xmm5)

#ifdef PREFETCHW
	PREFETCHW	8 * SIZE(Y1)
#endif

	movq	MM,  I
	sarq	$4,  I
	jle	.L15

	MOVUPS_A1 (-32 * SIZE, A1, %xmm0)
	MOVUPS_A2 (-32 * SIZE, A1, LDA, 1, %xmm1)
	MOVUPS_A2 (-32 * SIZE, A1, LDA, 2, %xmm2)
	MOVUPS_A2 (-32 * SIZE, A1, LDA3, 1, %xmm3)

	decq	I
	jle	.L13
	ALIGN_4

.L12:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A1)
#endif

	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-32 * SIZE, A2, %xmm0)
	mulps	%xmm4, %xmm1
	addps	%xmm1, %xmm9
	MOVUPS_A2 (-32 * SIZE, A2, LDA, 1, %xmm1)
	mulps	%xmm4, %xmm2
	addps	%xmm2, %xmm10
	MOVUPS_A2 (-32 * SIZE, A2, LDA, 2, %xmm2)
	mulps	%xmm4, %xmm3
	addps	%xmm3, %xmm11
	MOVUPS_A2 (-32 * SIZE, A2, LDA3, 1, %xmm3)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A1, LDA, 1)
#endif

	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm12
	MOVUPS_A1 (-28 * SIZE, A1, %xmm0)
	mulps	%xmm4, %xmm1
	addps	%xmm1, %xmm13
	MOVUPS_A2 (-28 * SIZE, A1, LDA, 1, %xmm1)
	mulps	%xmm4, %xmm2
	addps	%xmm2, %xmm14
	MOVUPS_A2 (-28 * SIZE, A1, LDA, 2, %xmm2)
	mulps	%xmm4, %xmm3
	MOVUPS_XL1 (-24 * SIZE, X1, %xmm4)
	addps	%xmm3, %xmm15
	MOVUPS_A2 (-28 * SIZE, A1, LDA3, 1, %xmm3)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A1, LDA, 2)
#endif

	mulps	%xmm5, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-28 * SIZE, A2, %xmm0)
	mulps	%xmm5, %xmm1
	addps	%xmm1, %xmm9
	MOVUPS_A2 (-28 * SIZE, A2, LDA, 1, %xmm1)
	mulps	%xmm5, %xmm2
	addps	%xmm2, %xmm10
	MOVUPS_A2 (-28 * SIZE, A2, LDA, 2, %xmm2)
	mulps	%xmm5, %xmm3
	addps	%xmm3, %xmm11
	MOVUPS_A2 (-28 * SIZE, A2, LDA3, 1, %xmm3)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A1, LDA3)
#endif

	mulps	%xmm5, %xmm0
	addps	%xmm0, %xmm12
	MOVUPS_A1 (-24 * SIZE, A1, %xmm0)
	mulps	%xmm5, %xmm1
	addps	%xmm1, %xmm13
	MOVUPS_A2 (-24 * SIZE, A1, LDA, 1, %xmm1)
	mulps	%xmm5, %xmm2
	addps	%xmm2, %xmm14
	MOVUPS_A2 (-24 * SIZE, A1, LDA, 2, %xmm2)
	mulps	%xmm5, %xmm3
	MOVUPS_XL1 (-20 * SIZE, X1, %xmm5)
	addps	%xmm3, %xmm15
	MOVUPS_A2 (-24 * SIZE, A1, LDA3, 1, %xmm3)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A2)
#endif

	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-24 * SIZE, A2, %xmm0)
	mulps	%xmm4, %xmm1
	addps	%xmm1, %xmm9
	MOVUPS_A2 (-24 * SIZE, A2, LDA, 1, %xmm1)
	mulps	%xmm4, %xmm2
	addps	%xmm2, %xmm10
	MOVUPS_A2 (-24 * SIZE, A2, LDA, 2, %xmm2)
	mulps	%xmm4, %xmm3
	addps	%xmm3, %xmm11
	MOVUPS_A2 (-24 * SIZE, A2, LDA3, 1, %xmm3)
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm12

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A2, LDA, 1)
#endif

	MOVUPS_A1 (-20 * SIZE, A1, %xmm0)
	mulps	%xmm4, %xmm1
	addps	%xmm1, %xmm13
	MOVUPS_A2 (-20 * SIZE, A1, LDA, 1, %xmm1)
	mulps	%xmm4, %xmm2
	addps	%xmm2, %xmm14
	MOVUPS_A2 (-20 * SIZE, A1, LDA, 2, %xmm2)
	mulps	%xmm4, %xmm3
	MOVUPS_XL1 (-16 * SIZE, X1, %xmm4)
	addps	%xmm3, %xmm15
	MOVUPS_A2 (-20 * SIZE, A1, LDA3, 1, %xmm3)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A2, LDA, 2)
#endif

	mulps	%xmm5, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-20 * SIZE, A2, %xmm0)
	mulps	%xmm5, %xmm1
	addps	%xmm1, %xmm9
	MOVUPS_A2 (-20 * SIZE, A2, LDA, 1, %xmm1)
	mulps	%xmm5, %xmm2
	addps	%xmm2, %xmm10
	MOVUPS_A2 (-20 * SIZE, A2, LDA, 2, %xmm2)
	mulps	%xmm5, %xmm3
	addps	%xmm3, %xmm11
	MOVUPS_A2 (-20 * SIZE, A2, LDA3, 1, %xmm3)
	mulps	%xmm5, %xmm0
	addps	%xmm0, %xmm12

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A2, LDA3)
#endif

	MOVUPS_A1 (-16 * SIZE, A1, %xmm0)
	mulps	%xmm5, %xmm1
	addps	%xmm1, %xmm13
	MOVUPS_A2 (-16 * SIZE, A1, LDA, 1, %xmm1)
	mulps	%xmm5, %xmm2
	addps	%xmm2, %xmm14

#ifdef PREFETCHW
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(X1)
#endif

	MOVUPS_A2 (-16 * SIZE, A1, LDA, 2, %xmm2)
	mulps	%xmm5, %xmm3
	MOVUPS_XL1 (-12 * SIZE, X1, %xmm5)
	addps	%xmm3, %xmm15
	MOVUPS_A2 (-16 * SIZE, A1, LDA3, 1, %xmm3)

	addq	$16 * SIZE, A1
	addq	$16 * SIZE, A2
	addq	$16 * SIZE, X1

	decq	I
	jg	.L12
	ALIGN_4

.L13:
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-32 * SIZE, A2, %xmm0)
	mulps	%xmm4, %xmm1
	addps	%xmm1, %xmm9
	MOVUPS_A2 (-32 * SIZE, A2, LDA, 1, %xmm1)
	mulps	%xmm4, %xmm2
	addps	%xmm2, %xmm10
	MOVUPS_A2 (-32 * SIZE, A2, LDA, 2, %xmm2)
	mulps	%xmm4, %xmm3
	addps	%xmm3, %xmm11
	MOVUPS_A2 (-32 * SIZE, A2, LDA3, 1, %xmm3)
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm12
	MOVUPS_A1 (-28 * SIZE, A1, %xmm0)
	mulps	%xmm4, %xmm1
	addps	%xmm1, %xmm13
	MOVUPS_A2 (-28 * SIZE, A1, LDA, 1, %xmm1)
	mulps	%xmm4, %xmm2
	addps	%xmm2, %xmm14
	MOVUPS_A2 (-28 * SIZE, A1, LDA, 2, %xmm2)
	mulps	%xmm4, %xmm3
	MOVUPS_XL1 (-24 * SIZE, X1, %xmm4)
	addps	%xmm3, %xmm15
	MOVUPS_A2 (-28 * SIZE, A1, LDA3, 1, %xmm3)

	mulps	%xmm5, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-28 * SIZE, A2, %xmm0)
	mulps	%xmm5, %xmm1
	addps	%xmm1, %xmm9
	MOVUPS_A2 (-28 * SIZE, A2, LDA, 1, %xmm1)
	mulps	%xmm5, %xmm2
	addps	%xmm2, %xmm10
	MOVUPS_A2 (-28 * SIZE, A2, LDA, 2, %xmm2)
	mulps	%xmm5, %xmm3
	addps	%xmm3, %xmm11
	MOVUPS_A2 (-28 * SIZE, A2, LDA3, 1, %xmm3)
	mulps	%xmm5, %xmm0
	addps	%xmm0, %xmm12
	MOVUPS_A1 (-24 * SIZE, A1, %xmm0)
	mulps	%xmm5, %xmm1
	addps	%xmm1, %xmm13
	MOVUPS_A2 (-24 * SIZE, A1, LDA, 1, %xmm1)
	mulps	%xmm5, %xmm2
	addps	%xmm2, %xmm14
	MOVUPS_A2 (-24 * SIZE, A1, LDA, 2, %xmm2)
	mulps	%xmm5, %xmm3
	MOVUPS_XL1 (-20 * SIZE, X1, %xmm5)
	addps	%xmm3, %xmm15
	MOVUPS_A2 (-24 * SIZE, A1, LDA3, 1, %xmm3)

	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-24 * SIZE, A2, %xmm0)
	mulps	%xmm4, %xmm1
	addps	%xmm1, %xmm9
	MOVUPS_A2 (-24 * SIZE, A2, LDA, 1, %xmm1)
	mulps	%xmm4, %xmm2
	addps	%xmm2, %xmm10
	MOVUPS_A2 (-24 * SIZE, A2, LDA, 2, %xmm2)
	mulps	%xmm4, %xmm3
	addps	%xmm3, %xmm11
	MOVUPS_A2 (-24 * SIZE, A2, LDA3, 1, %xmm3)
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm12
	MOVUPS_A1 (-20 * SIZE, A1, %xmm0)
	mulps	%xmm4, %xmm1
	addps	%xmm1, %xmm13
	MOVUPS_A2 (-20 * SIZE, A1, LDA, 1, %xmm1)
	mulps	%xmm4, %xmm2
	addps	%xmm2, %xmm14
	MOVUPS_A2 (-20 * SIZE, A1, LDA, 2, %xmm2)
	mulps	%xmm4, %xmm3
	MOVUPS_XL1 (-16 * SIZE, X1, %xmm4)
	addps	%xmm3, %xmm15
	MOVUPS_A2 (-20 * SIZE, A1, LDA3, 1, %xmm3)

	mulps	%xmm5, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-20 * SIZE, A2, %xmm0)
	mulps	%xmm5, %xmm1
	addps	%xmm1, %xmm9
	MOVUPS_A2 (-20 * SIZE, A2, LDA, 1, %xmm1)
	mulps	%xmm5, %xmm2
	addps	%xmm2, %xmm10
	MOVUPS_A2 (-20 * SIZE, A2, LDA, 2, %xmm2)
	mulps	%xmm5, %xmm3
	addps	%xmm3, %xmm11
	MOVUPS_A2 (-20 * SIZE, A2, LDA3, 1, %xmm3)
	mulps	%xmm5, %xmm0
	addps	%xmm0, %xmm12
	mulps	%xmm5, %xmm1
	addps	%xmm1, %xmm13
	mulps	%xmm5, %xmm2
	addps	%xmm2, %xmm14
	mulps	%xmm5, %xmm3
	MOVUPS_XL1 (-12 * SIZE, X1, %xmm5)
	addps	%xmm3, %xmm15

	addq	$16 * SIZE, A1
	addq	$16 * SIZE, A2
	addq	$16 * SIZE, X1
	ALIGN_4

.L15:
	testq	$8, MM
	jle	.L16

	MOVUPS_A1 (-32 * SIZE, A1, %xmm0)
	MOVUPS_A2 (-32 * SIZE, A1, LDA, 1, %xmm1)
	MOVUPS_A2 (-32 * SIZE, A1, LDA, 2, %xmm2)
	MOVUPS_A2 (-32 * SIZE, A1, LDA3, 1, %xmm3)

	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-32 * SIZE, A2, %xmm0)
	mulps	%xmm4, %xmm1
	addps	%xmm1, %xmm9
	MOVUPS_A2 (-32 * SIZE, A2, LDA, 1, %xmm1)
	mulps	%xmm4, %xmm2
	addps	%xmm2, %xmm10
	MOVUPS_A2 (-32 * SIZE, A2, LDA, 2, %xmm2)
	mulps	%xmm4, %xmm3
	addps	%xmm3, %xmm11
	MOVUPS_A2 (-32 * SIZE, A2, LDA3, 1, %xmm3)
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm12
	MOVUPS_A1 (-28 * SIZE, A1, %xmm0)
	mulps	%xmm4, %xmm1
	addps	%xmm1, %xmm13
	MOVUPS_A2 (-28 * SIZE, A1, LDA, 1, %xmm1)
	mulps	%xmm4, %xmm2
	addps	%xmm2, %xmm14
	MOVUPS_A2 (-28 * SIZE, A1, LDA, 2, %xmm2)
	mulps	%xmm4, %xmm3
	MOVUPS_XL1 (-24 * SIZE, X1, %xmm4)
	addps	%xmm3, %xmm15
	MOVUPS_A2 (-28 * SIZE, A1, LDA3, 1, %xmm3)

	mulps	%xmm5, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-28 * SIZE, A2, %xmm0)
	mulps	%xmm5, %xmm1
	addps	%xmm1, %xmm9
	MOVUPS_A2 (-28 * SIZE, A2, LDA, 1, %xmm1)
	mulps	%xmm5, %xmm2
	addps	%xmm2, %xmm10
	MOVUPS_A2 (-28 * SIZE, A2, LDA, 2, %xmm2)
	mulps	%xmm5, %xmm3
	addps	%xmm3, %xmm11
	MOVUPS_A2 (-28 * SIZE, A2, LDA3, 1, %xmm3)
	mulps	%xmm5, %xmm0
	addps	%xmm0, %xmm12
	mulps	%xmm5, %xmm1
	addps	%xmm1, %xmm13
	mulps	%xmm5, %xmm2
	addps	%xmm2, %xmm14
	mulps	%xmm5, %xmm3
	addps	%xmm3, %xmm15

	addq	$8 * SIZE, A1
	addq	$8 * SIZE, A2
	addq	$8 * SIZE, X1
	ALIGN_4

.L16:
	testq	$4, MM
	jle	.L17

	MOVUPS_A1 (-32 * SIZE, A1, %xmm0)
	MOVUPS_A2 (-32 * SIZE, A1, LDA, 1, %xmm1)
	MOVUPS_A2 (-32 * SIZE, A1, LDA, 2, %xmm2)
	MOVUPS_A2 (-32 * SIZE, A1, LDA3, 1, %xmm3)

	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-32 * SIZE, A2, %xmm0)
	mulps	%xmm4, %xmm1
	addps	%xmm1, %xmm9
	MOVUPS_A2 (-32 * SIZE, A2, LDA, 1, %xmm1)
	mulps	%xmm4, %xmm2
	addps	%xmm2, %xmm10
	MOVUPS_A2 (-32 * SIZE, A2, LDA, 2, %xmm2)
	mulps	%xmm4, %xmm3
	addps	%xmm3, %xmm11
	MOVUPS_A2 (-32 * SIZE, A2, LDA3, 1, %xmm3)
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm12
	mulps	%xmm4, %xmm1
	addps	%xmm1, %xmm13
	mulps	%xmm4, %xmm2
	addps	%xmm2, %xmm14
	mulps	%xmm4, %xmm3
	addps	%xmm3, %xmm15

	addq	$4 * SIZE, A1
	addq	$4 * SIZE, A2
	addq	$4 * SIZE, X1
	ALIGN_4

.L17:
	testq	$2, MM
	jle	.L18

#ifdef movsd
	xorps	%xmm0, %xmm0
#endif
	movsd	-32 * SIZE(A1), %xmm0
#ifdef movsd
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(X1), %xmm4
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
#ifdef movsd
	xorps	%xmm1, %xmm1
#endif
	movsd	-32 * SIZE(A1, LDA, 1), %xmm1
	mulps	%xmm4, %xmm1
	addps	%xmm1, %xmm9
#ifdef movsd
	xorps	%xmm2, %xmm2
#endif
	movsd	-32 * SIZE(A1, LDA, 2), %xmm2
	mulps	%xmm4, %xmm2
	addps	%xmm2, %xmm10
#ifdef movsd
	xorps	%xmm3, %xmm3
#endif
	movsd	-32 * SIZE(A1, LDA3, 1), %xmm3
	mulps	%xmm4, %xmm3
	addps	%xmm3, %xmm11
	movsd	-32 * SIZE(A2), %xmm0
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm12
	movsd	-32 * SIZE(A2, LDA, 1), %xmm1
	mulps	%xmm4, %xmm1
	addps	%xmm1, %xmm13
	movsd	-32 * SIZE(A2, LDA, 2), %xmm2
	mulps	%xmm4, %xmm2
	addps	%xmm2, %xmm14
	movsd	-32 * SIZE(A2, LDA3, 1), %xmm3
	mulps	%xmm4, %xmm3
	addps	%xmm3, %xmm15

	addq	$2 * SIZE, A1
	addq	$2 * SIZE, A2
	addq	$2 * SIZE, X1
	ALIGN_4

.L18:
	testq	$1, MM
	jle	.L19

	movss	-32 * SIZE(A1), %xmm0
	movss	-32 * SIZE(X1), %xmm4
	mulss	%xmm4, %xmm0
	addss	%xmm0, %xmm8
	movss	-32 * SIZE(A1, LDA, 1), %xmm1
	mulss	%xmm4, %xmm1
	addss	%xmm1, %xmm9
	movss	-32 * SIZE(A1, LDA, 2), %xmm2
	mulss	%xmm4, %xmm2
	addss	%xmm2, %xmm10
	movss	-32 * SIZE(A1, LDA3, 1), %xmm3
	mulss	%xmm4, %xmm3
	addss	%xmm3, %xmm11
	movss	-32 * SIZE(A2), %xmm0
	mulss	%xmm4, %xmm0
	addss	%xmm0, %xmm12
	movss	-32 * SIZE(A2, LDA, 1), %xmm1
	mulss	%xmm4, %xmm1
	addss	%xmm1, %xmm13
	movss	-32 * SIZE(A2, LDA, 2), %xmm2
	mulss	%xmm4, %xmm2
	addss	%xmm2, %xmm14
	movss	-32 * SIZE(A2, LDA3, 1), %xmm3
	mulss	%xmm4, %xmm3
	addss	%xmm3, %xmm15
	ALIGN_4

.L19:
#ifdef HAVE_SSE3
	haddps	%xmm9, %xmm8
	haddps	%xmm11, %xmm10
	haddps	%xmm10, %xmm8

	pshufd	$0x1, %xmm8, %xmm9
	pshufd	$0x2, %xmm8, %xmm10
	pshufd	$0x3, %xmm8, %xmm11

	haddps	%xmm13, %xmm12
	haddps	%xmm15, %xmm14
	haddps	%xmm14, %xmm12

	pshufd	$0x1, %xmm12, %xmm13
	pshufd	$0x2, %xmm12, %xmm14
	pshufd	$0x3, %xmm12, %xmm15
#else
	movaps	%xmm8, %xmm0
	unpcklps %xmm9, %xmm8
	unpckhps %xmm9, %xmm0

	movaps	%xmm10, %xmm1
	unpcklps %xmm11, %xmm10
	unpckhps %xmm11, %xmm1

	movaps	%xmm8, %xmm9
	unpcklps %xmm10, %xmm8
	unpckhps %xmm10, %xmm9

	movaps	%xmm0, %xmm10
	unpcklps %xmm1, %xmm0
	unpckhps %xmm1, %xmm10

	addps	%xmm9, %xmm8
	addps	%xmm0, %xmm10
	addps	%xmm10, %xmm8

	pshufd	$0x2, %xmm8, %xmm9
	pshufd	$0x1, %xmm8, %xmm10
	pshufd	$0x3, %xmm8, %xmm11

	movaps	%xmm12, %xmm0
	unpcklps %xmm13, %xmm12
	unpckhps %xmm13, %xmm0

	movaps	%xmm14, %xmm1
	unpcklps %xmm15, %xmm14
	unpckhps %xmm15, %xmm1

	movaps	%xmm12, %xmm13
	unpcklps %xmm14, %xmm12
	unpckhps %xmm14, %xmm13

	movaps	%xmm0, %xmm14
	unpcklps %xmm1, %xmm0
	unpckhps %xmm1, %xmm14

	addps	%xmm13, %xmm12
	addps	%xmm0, %xmm14
	addps	%xmm14, %xmm12

	pshufd	$0x2, %xmm12, %xmm13
	pshufd	$0x1, %xmm12, %xmm14
	pshufd	$0x3, %xmm12, %xmm15
#endif

	mulss	ALPHA, %xmm8
	mulss	ALPHA, %xmm9
	mulss	ALPHA, %xmm10
	mulss	ALPHA, %xmm11
	mulss	ALPHA, %xmm12
	mulss	ALPHA, %xmm13
	mulss	ALPHA, %xmm14
	mulss	ALPHA, %xmm15

	addss	(Y), %xmm8
	addq	INCY, Y
	addss	(Y), %xmm9
	addq	INCY, Y
	addss	(Y), %xmm10
	addq	INCY, Y
	addss	(Y), %xmm11
	addq	INCY, Y
	addss	(Y), %xmm12
	addq	INCY, Y
	addss	(Y), %xmm13
	addq	INCY, Y
	addss	(Y), %xmm14
	addq	INCY, Y
	addss	(Y), %xmm15
	addq	INCY, Y

	movss	%xmm8,  (Y1)
	addq	INCY, Y1
	movss	%xmm9,  (Y1)
	addq	INCY, Y1
	movss	%xmm10, (Y1)
	addq	INCY, Y1
	movss	%xmm11, (Y1)
	addq	INCY, Y1
	movss	%xmm12, (Y1)
	addq	INCY, Y1
	movss	%xmm13, (Y1)
	addq	INCY, Y1
	movss	%xmm14, (Y1)
	addq	INCY, Y1
	movss	%xmm15, (Y1)
	addq	INCY, Y1

	cmpq	$8, N
	jge	.L11
	ALIGN_4

.L20:
#endif

	cmpq	$4, N
	jl	.L30

#if GEMV_UNROLL == 4
	ALIGN_3

.L21:
#endif
	subq	$4, N

	leaq	32 * SIZE(BUFFER), X1

	movq	A, A1
	leaq	(A1, LDA, 2), A2
	leaq	(A1, LDA, 4), A

	xorps	%xmm8, %xmm8
	xorps	%xmm9, %xmm9
	xorps	%xmm10, %xmm10
	xorps	%xmm11, %xmm11

#ifdef ALIGNED_ACCESS
	cmpq	$3, M
	jle	.L27

	testq	$SIZE, A1
	je	.L2X

	movss	-32 * SIZE(A1), %xmm0
	movss	-32 * SIZE(X1), %xmm4
	mulss	%xmm4, %xmm0
	addss	%xmm0, %xmm8
	movss	-32 * SIZE(A1, LDA), %xmm1
	mulss	%xmm4, %xmm1
	addss	%xmm1, %xmm9
	movss	-32 * SIZE(A2), %xmm2
	mulss	%xmm4, %xmm2
	addss	%xmm2, %xmm10
	movss	-32 * SIZE(A2, LDA), %xmm3
	mulss	%xmm4, %xmm3
	addss	%xmm3, %xmm11

	addq	 $1 * SIZE, A1
	addq	 $1 * SIZE, A2
	addq	 $1 * SIZE, X1
	ALIGN_3

.L2X:
	testq	$2 * SIZE, A1
	je	.L2XX

#ifdef movsd
	xorps	%xmm0, %xmm0
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(A1), %xmm0
	movsd	-32 * SIZE(X1), %xmm4
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
#ifdef movsd
	xorps	%xmm1, %xmm1
#endif
	movsd	-32 * SIZE(A1, LDA), %xmm1
	mulps	%xmm4, %xmm1
	addps	%xmm1, %xmm9
#ifdef movsd
	xorps	%xmm2, %xmm2
#endif
	movsd	-32 * SIZE(A2), %xmm2
	mulps	%xmm4, %xmm2
	addps	%xmm2, %xmm10
#ifdef movsd
	xorps	%xmm3, %xmm3
#endif
	movsd	-32 * SIZE(A2, LDA), %xmm3
	mulps	%xmm4, %xmm3
	addps	%xmm3, %xmm11

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, X1
	ALIGN_3

.L2XX:
#endif

	MOVUPS_XL1 (-32 * SIZE, X1, %xmm4)
	MOVUPS_XL1 (-28 * SIZE, X1, %xmm5)

#if (GEMV_UNROLL == 4) && defined(PREFETCHW)
	PREFETCHW	4 * SIZE(Y1)
#endif

	movq	MM,  I
	sarq	$4,  I
	jle	.L25

	MOVUPS_A1 (-32 * SIZE, A1, %xmm0)
	MOVUPS_A2 (-32 * SIZE, A1, LDA, 1, %xmm1)
	MOVUPS_A1 (-32 * SIZE, A2, %xmm2)
	MOVUPS_A2 (-32 * SIZE, A2, LDA, 1, %xmm3)

	MOVUPS_A1 (-28 * SIZE, A1, %xmm12)
	MOVUPS_A2 (-28 * SIZE, A1, LDA, 1, %xmm13)
	MOVUPS_A1 (-28 * SIZE, A2, %xmm14)
	MOVUPS_A2 (-28 * SIZE, A2, LDA, 1, %xmm15)

	decq	I
	jle	.L23
	ALIGN_4

.L22:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(A1)
#endif

	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-24 * SIZE, A1, %xmm0)
	mulps	%xmm4, %xmm1
	addps	%xmm1, %xmm9
	MOVUPS_A2 (-24 * SIZE, A1, LDA, 1, %xmm1)
	mulps	%xmm4, %xmm2
	addps	%xmm2, %xmm10
	MOVUPS_A1 (-24 * SIZE, A2, %xmm2)
	mulps	%xmm4, %xmm3
	MOVUPS_XL1 (-24 * SIZE, X1, %xmm4)
	addps	%xmm3, %xmm11
	MOVUPS_A2 (-24 * SIZE, A2, LDA, 1, %xmm3)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(A1, LDA)
#endif

	mulps	%xmm5, %xmm12
	addps	%xmm12, %xmm8
	MOVUPS_A1 (-20 * SIZE, A1, %xmm12)
	mulps	%xmm5, %xmm13
	addps	%xmm13, %xmm9
	MOVUPS_A2 (-20 * SIZE, A1, LDA, 1, %xmm13)
	mulps	%xmm5, %xmm14
	addps	%xmm14, %xmm10
	MOVUPS_A1 (-20 * SIZE, A2, %xmm14)
	mulps	%xmm5, %xmm15
	MOVUPS_XL1 (-20 * SIZE, X1, %xmm5)
	addps	%xmm15, %xmm11
	MOVUPS_A2 (-20 * SIZE, A2, LDA, 1, %xmm15)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(A2)
#endif

	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-16 * SIZE, A1, %xmm0)
	mulps	%xmm4, %xmm1
	addps	%xmm1, %xmm9
	MOVUPS_A2 (-16 * SIZE, A1, LDA, 1, %xmm1)
	mulps	%xmm4, %xmm2
	addps	%xmm2, %xmm10
	MOVUPS_A1 (-16 * SIZE, A2, %xmm2)
	mulps	%xmm4, %xmm3
	MOVUPS_XL1 (-16 * SIZE, X1, %xmm4)
	addps	%xmm3, %xmm11
	MOVUPS_A2 (-16 * SIZE, A2, LDA, 1, %xmm3)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(A2, LDA)
#endif

	mulps	%xmm5, %xmm12
	addps	%xmm12, %xmm8
	MOVUPS_A1 (-12 * SIZE, A1, %xmm12)
	mulps	%xmm5, %xmm13
	addps	%xmm13, %xmm9
	MOVUPS_A2 (-12 * SIZE, A1, LDA, 1, %xmm13)

#ifdef PREFETCHW
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(X1)
#endif

	mulps	%xmm5, %xmm14
	addps	%xmm14, %xmm10
	MOVUPS_A1 (-12 * SIZE, A2, %xmm14)
	mulps	%xmm5, %xmm15
	MOVUPS_XL1 (-12 * SIZE, X1, %xmm5)
	addps	%xmm15, %xmm11
	MOVUPS_A2 (-12 * SIZE, A2, LDA, 1, %xmm15)

	addq	$16 * SIZE, A1
	addq	$16 * SIZE, A2
	addq	$16 * SIZE, X1

	decq	I
	jg	.L22
	ALIGN_4

.L23:
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-24 * SIZE, A1, %xmm0)
	mulps	%xmm4, %xmm1
	addps	%xmm1, %xmm9
	MOVUPS_A2 (-24 * SIZE, A1, LDA, 1, %xmm1)
	mulps	%xmm4, %xmm2
	addps	%xmm2, %xmm10
	MOVUPS_A1 (-24 * SIZE, A2, %xmm2)
	mulps	%xmm4, %xmm3
	MOVUPS_XL1 (-24 * SIZE, X1, %xmm4)
	addps	%xmm3, %xmm11
	MOVUPS_A2 (-24 * SIZE, A2, LDA, 1, %xmm3)

	mulps	%xmm5, %xmm12
	addps	%xmm12, %xmm8
	MOVUPS_A1 (-20 * SIZE, A1, %xmm12)
	mulps	%xmm5, %xmm13
	addps	%xmm13, %xmm9
	MOVUPS_A2 (-20 * SIZE, A1, LDA, 1, %xmm13)
	mulps	%xmm5, %xmm14
	addps	%xmm14, %xmm10
	MOVUPS_A1 (-20 * SIZE, A2, %xmm14)
	mulps	%xmm5, %xmm15
	MOVUPS_XL1 (-20 * SIZE, X1, %xmm5)
	addps	%xmm15, %xmm11
	MOVUPS_A2 (-20 * SIZE, A2, LDA, 1, %xmm15)

	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	mulps	%xmm4, %xmm1
	addps	%xmm1, %xmm9
	mulps	%xmm4, %xmm2
	addps	%xmm2, %xmm10
	mulps	%xmm4, %xmm3
	MOVUPS_XL1 (-16 * SIZE, X1, %xmm4)
	addps	%xmm3, %xmm11

	mulps	%xmm5, %xmm12
	addps	%xmm12, %xmm8
	mulps	%xmm5, %xmm13
	addps	%xmm13, %xmm9
	mulps	%xmm5, %xmm14
	addps	%xmm14, %xmm10
	mulps	%xmm5, %xmm15
	MOVUPS_XL1 (-12 * SIZE, X1, %xmm5)
	addps	%xmm15, %xmm11

	addq	$16 * SIZE, A1
	addq	$16 * SIZE, A2
	addq	$16 * SIZE, X1
	ALIGN_4

.L25:
	testq	$8, MM
	jle	.L26

	MOVUPS_A1 (-32 * SIZE, A1, %xmm0)
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A2 (-32 * SIZE, A1, LDA, 1, %xmm1)
	mulps	%xmm4, %xmm1
	addps	%xmm1, %xmm9
	MOVUPS_A1 (-32 * SIZE, A2, %xmm2)
	mulps	%xmm4, %xmm2
	addps	%xmm2, %xmm10
	MOVUPS_A2 (-32 * SIZE, A2, LDA, 1, %xmm3)
	mulps	%xmm4, %xmm3
	MOVUPS_XL1 (-24 * SIZE, X1, %xmm4)
	addps	%xmm3, %xmm11

	MOVUPS_A1 (-28 * SIZE, A1, %xmm12)
	mulps	%xmm5, %xmm12
	addps	%xmm12, %xmm8
	MOVUPS_A2 (-28 * SIZE, A1, LDA, 1, %xmm13)
	mulps	%xmm5, %xmm13
	addps	%xmm13, %xmm9
	MOVUPS_A1 (-28 * SIZE, A2, %xmm14)
	mulps	%xmm5, %xmm14
	addps	%xmm14, %xmm10
	MOVUPS_A2 (-28 * SIZE, A2, LDA, 1, %xmm15)
	mulps	%xmm5, %xmm15
	addps	%xmm15, %xmm11

	addq	$8 * SIZE, A1
	addq	$8 * SIZE, A2
	addq	$8 * SIZE, X1
	ALIGN_4

.L26:
	testq	$4, MM
	jle	.L27

	MOVUPS_A1 (-32 * SIZE, A1, %xmm0)
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A2 (-32 * SIZE, A1, LDA, 1, %xmm1)
	mulps	%xmm4, %xmm1
	addps	%xmm1, %xmm9
	MOVUPS_A1 (-32 * SIZE, A2, %xmm2)
	mulps	%xmm4, %xmm2
	addps	%xmm2, %xmm10
	MOVUPS_A2 (-32 * SIZE, A2, LDA, 1, %xmm3)
	mulps	%xmm4, %xmm3
	addps	%xmm3, %xmm11

	addq	$4 * SIZE, A1
	addq	$4 * SIZE, A2
	addq	$4 * SIZE, X1
	ALIGN_4

.L27:
	testq	$2, MM
	jle	.L28

#ifdef movsd
	xorps	%xmm0, %xmm0
#endif
	movsd	-32 * SIZE(A1), %xmm0
#ifdef movsd
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(X1), %xmm4
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
#ifdef movsd
	xorps	%xmm1, %xmm1
#endif
	movsd	-32 * SIZE(A1, LDA), %xmm1
	mulps	%xmm4, %xmm1
	addps	%xmm1, %xmm9
#ifdef movsd
	xorps	%xmm2, %xmm2
#endif
	movsd	-32 * SIZE(A2), %xmm2
	mulps	%xmm4, %xmm2
	addps	%xmm2, %xmm10
#ifdef movsd
	xorps	%xmm3, %xmm3
#endif
	movsd	-32 * SIZE(A2, LDA), %xmm3
	mulps	%xmm4, %xmm3
	addps	%xmm3, %xmm11
	shufps	$0xe, %xmm4, %xmm4

	addq	$2 * SIZE, A1
	addq	$2 * SIZE, A2
	addq	$2 * SIZE, X1
	ALIGN_4

.L28:
	testq	$1, MM
	jle	.L29

	movss	-32 * SIZE(A1), %xmm0
	movss	-32 * SIZE(X1), %xmm4
	mulss	%xmm4, %xmm0
	addss	%xmm0, %xmm8
	movss	-32 * SIZE(A1, LDA), %xmm1
	mulss	%xmm4, %xmm1
	addss	%xmm1, %xmm9
	movss	-32 * SIZE(A2), %xmm2
	mulss	%xmm4, %xmm2
	addss	%xmm2, %xmm10
	movss	-32 * SIZE(A2, LDA), %xmm3
	mulss	%xmm4, %xmm3
	addss	%xmm3, %xmm11
	ALIGN_4

.L29:
#ifdef HAVE_SSE3
	haddps	%xmm9, %xmm8
	haddps	%xmm11, %xmm10
	haddps	%xmm10, %xmm8

	pshufd	$0x1, %xmm8, %xmm9
	pshufd	$0x2, %xmm8, %xmm10
	pshufd	$0x3, %xmm8, %xmm11
#else
	movaps	%xmm8, %xmm0
	unpcklps %xmm9, %xmm8
	unpckhps %xmm9, %xmm0

	movaps	%xmm10, %xmm1
	unpcklps %xmm11, %xmm10
	unpckhps %xmm11, %xmm1

	movaps	%xmm8, %xmm9
	unpcklps %xmm10, %xmm8
	unpckhps %xmm10, %xmm9

	movaps	%xmm0, %xmm10
	unpcklps %xmm1, %xmm0
	unpckhps %xmm1, %xmm10

	addps	%xmm9, %xmm8
	addps	%xmm0, %xmm10
	addps	%xmm10, %xmm8

	pshufd	$0x2, %xmm8, %xmm9
	pshufd	$0x1, %xmm8, %xmm10
	pshufd	$0x3, %xmm8, %xmm11
#endif

	mulss	ALPHA, %xmm8
	mulss	ALPHA, %xmm9
	mulss	ALPHA, %xmm10
	mulss	ALPHA, %xmm11

	addss	(Y), %xmm8
	addq	INCY, Y
	addss	(Y), %xmm9
	addq	INCY, Y
	addss	(Y), %xmm10
	addq	INCY, Y
	addss	(Y), %xmm11
	addq	INCY, Y

	movss	%xmm8, (Y1)
	addq	INCY, Y1
	movss	%xmm9, (Y1)
	addq	INCY, Y1
	movss	%xmm10, (Y1)
	addq	INCY, Y1
	movss	%xmm11, (Y1)
	addq	INCY, Y1

#if GEMV_UNROLL == 4
	cmpq	$4, N
	jge	.L21
#endif
	ALIGN_4

.L30:
	cmpq	$3, N
	jne	.L40

	leaq	32 * SIZE(BUFFER), X1

	movq	A, A1
	leaq	(A1, LDA, 2), A2
	leaq	(A1, LDA, 4), A

	xorps	%xmm8, %xmm8
	xorps	%xmm9, %xmm9
	xorps	%xmm10, %xmm10

#ifdef ALIGNED_ACCESS
	cmpq	$3, M
	jle	.L37

	testq	$SIZE, A1
	je	.L3X

	movss	-32 * SIZE(A1), %xmm0
	movss	-32 * SIZE(X1), %xmm4
	mulss	%xmm4, %xmm0
	addss	%xmm0, %xmm8
	movss	-32 * SIZE(A1, LDA), %xmm1
	mulss	%xmm4, %xmm1
	addss	%xmm1, %xmm9
	movss	-32 * SIZE(A2), %xmm2
	mulss	%xmm4, %xmm2
	addss	%xmm2, %xmm10
	movss	-32 * SIZE(A2, LDA), %xmm3
	mulss	%xmm4, %xmm3
	addss	%xmm3, %xmm11

	addq	 $1 * SIZE, A1
	addq	 $1 * SIZE, A2
	addq	 $1 * SIZE, X1
	ALIGN_3

.L3X:
	testq	$2 * SIZE, A1
	je	.L3XX

#ifdef movsd
	xorps	%xmm0, %xmm0
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(A1), %xmm0
	movsd	-32 * SIZE(X1), %xmm4
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
#ifdef movsd
	xorps	%xmm1, %xmm1
#endif
	movsd	-32 * SIZE(A1, LDA), %xmm1
	mulps	%xmm4, %xmm1
	addps	%xmm1, %xmm9
#ifdef movsd
	xorps	%xmm2, %xmm2
#endif
	movsd	-32 * SIZE(A2), %xmm2
	mulps	%xmm4, %xmm2
	addps	%xmm2, %xmm10
#ifdef movsd
	xorps	%xmm3, %xmm3
#endif
	movsd	-32 * SIZE(A2, LDA), %xmm3
	mulps	%xmm4, %xmm3
	addps	%xmm3, %xmm11

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, X1
	ALIGN_3

.L3XX:
#endif

	MOVUPS_XL1 (-32 * SIZE, X1, %xmm4)
	MOVUPS_XL1 (-28 * SIZE, X1, %xmm5)

#if (GEMV_UNROLL == 4) && defined(PREFETCHW)
	PREFETCHW	4 * SIZE(Y1)
#endif

	movq	MM,  I
	sarq	$4,  I
	jle	.L35

	MOVUPS_A1 (-32 * SIZE, A1, %xmm0)
	MOVUPS_A2 (-32 * SIZE, A1, LDA, 1, %xmm1)
	MOVUPS_A1 (-32 * SIZE, A2, %xmm2)

	MOVUPS_A1 (-28 * SIZE, A1, %xmm12)
	MOVUPS_A2 (-28 * SIZE, A1, LDA, 1, %xmm13)
	MOVUPS_A1 (-28 * SIZE, A2, %xmm14)

	decq	I
	jle	.L33
	ALIGN_4

.L32:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 3 - 128 + PREOFFSET(A1)
#endif

	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-24 * SIZE, A1, %xmm0)
	mulps	%xmm4, %xmm1
	addps	%xmm1, %xmm9
	MOVUPS_A2 (-24 * SIZE, A1, LDA, 1, %xmm1)
	mulps	%xmm4, %xmm2
	MOVUPS_XL1 (-24 * SIZE, X1, %xmm4)
	addps	%xmm2, %xmm10
	MOVUPS_A1 (-24 * SIZE, A2, %xmm2)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 3 - 128 + PREOFFSET(A1, LDA)
#endif

	mulps	%xmm5, %xmm12
	addps	%xmm12, %xmm8
	MOVUPS_A1 (-20 * SIZE, A1, %xmm12)
	mulps	%xmm5, %xmm13
	addps	%xmm13, %xmm9
	MOVUPS_A2 (-20 * SIZE, A1, LDA, 1, %xmm13)
	mulps	%xmm5, %xmm14
	MOVUPS_XL1 (-20 * SIZE, X1, %xmm5)
	addps	%xmm14, %xmm10
	MOVUPS_A1 (-20 * SIZE, A2, %xmm14)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 3 - 128 + PREOFFSET(A2)
#endif

	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-16 * SIZE, A1, %xmm0)
	mulps	%xmm4, %xmm1
	addps	%xmm1, %xmm9
	MOVUPS_A2 (-16 * SIZE, A1, LDA, 1, %xmm1)
	mulps	%xmm4, %xmm2
	MOVUPS_XL1 (-16 * SIZE, X1, %xmm4)
	addps	%xmm2, %xmm10
	MOVUPS_A1 (-16 * SIZE, A2, %xmm2)

#ifdef PREFETCHW
	PREFETCH	(PREFETCHSIZE) * 3 - 128 + PREOFFSET(X1)
#endif

	mulps	%xmm5, %xmm12
	addps	%xmm12, %xmm8
	MOVUPS_A1 (-12 * SIZE, A1, %xmm12)
	mulps	%xmm5, %xmm13
	addps	%xmm13, %xmm9
	MOVUPS_A2 (-12 * SIZE, A1, LDA, 1, %xmm13)
	mulps	%xmm5, %xmm14
	MOVUPS_XL1 (-12 * SIZE, X1, %xmm5)
	addps	%xmm14, %xmm10
	MOVUPS_A1 (-12 * SIZE, A2, %xmm14)

	addq	$16 * SIZE, A1
	addq	$16 * SIZE, A2
	addq	$16 * SIZE, X1

	decq	I
	jg	.L32
	ALIGN_4

.L33:
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-24 * SIZE, A1, %xmm0)
	mulps	%xmm4, %xmm1
	addps	%xmm1, %xmm9
	MOVUPS_A2 (-24 * SIZE, A1, LDA, 1, %xmm1)
	mulps	%xmm4, %xmm2
	MOVUPS_XL1 (-24 * SIZE, X1, %xmm4)
	addps	%xmm2, %xmm10
	MOVUPS_A1 (-24 * SIZE, A2, %xmm2)

	mulps	%xmm5, %xmm12
	addps	%xmm12, %xmm8
	MOVUPS_A1 (-20 * SIZE, A1, %xmm12)
	mulps	%xmm5, %xmm13
	addps	%xmm13, %xmm9
	MOVUPS_A2 (-20 * SIZE, A1, LDA, 1, %xmm13)
	mulps	%xmm5, %xmm14
	MOVUPS_XL1 (-20 * SIZE, X1, %xmm5)
	addps	%xmm14, %xmm10
	MOVUPS_A1 (-20 * SIZE, A2, %xmm14)

	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	mulps	%xmm4, %xmm1
	addps	%xmm1, %xmm9
	mulps	%xmm4, %xmm2
	addps	%xmm2, %xmm10
	MOVUPS_XL1 (-16 * SIZE, X1, %xmm4)

	mulps	%xmm5, %xmm12
	addps	%xmm12, %xmm8
	mulps	%xmm5, %xmm13
	addps	%xmm13, %xmm9
	mulps	%xmm5, %xmm14
	MOVUPS_XL1 (-12 * SIZE, X1, %xmm5)
	addps	%xmm14, %xmm10

	addq	$16 * SIZE, A1
	addq	$16 * SIZE, A2
	addq	$16 * SIZE, X1
	ALIGN_4

.L35:
	testq	$8, MM
	jle	.L36

	MOVUPS_A1 (-32 * SIZE, A1, %xmm0)
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A2 (-32 * SIZE, A1, LDA, 1, %xmm1)
	mulps	%xmm4, %xmm1
	addps	%xmm1, %xmm9
	MOVUPS_A1 (-32 * SIZE, A2, %xmm2)
	mulps	%xmm4, %xmm2
	MOVUPS_XL1 (-24 * SIZE, X1, %xmm4)
	addps	%xmm2, %xmm10

	MOVUPS_A1 (-28 * SIZE, A1, %xmm12)
	mulps	%xmm5, %xmm12
	addps	%xmm12, %xmm8
	MOVUPS_A2 (-28 * SIZE, A1, LDA, 1, %xmm13)
	mulps	%xmm5, %xmm13
	addps	%xmm13, %xmm9
	MOVUPS_A1 (-28 * SIZE, A2, %xmm14)
	mulps	%xmm5, %xmm14
	addps	%xmm14, %xmm10

	addq	$8 * SIZE, A1
	addq	$8 * SIZE, A2
	addq	$8 * SIZE, X1
	ALIGN_4

.L36:
	testq	$4, MM
	jle	.L37

	MOVUPS_A1 (-32 * SIZE, A1, %xmm0)
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A2 (-32 * SIZE, A1, LDA, 1, %xmm1)
	mulps	%xmm4, %xmm1
	addps	%xmm1, %xmm9
	MOVUPS_A1 (-32 * SIZE, A2, %xmm2)
	mulps	%xmm4, %xmm2
	addps	%xmm2, %xmm10

	addq	$4 * SIZE, A1
	addq	$4 * SIZE, A2
	addq	$4 * SIZE, X1
	ALIGN_4

.L37:
	testq	$2, MM
	jle	.L38

#ifdef movsd
	xorps	%xmm0, %xmm0
#endif
	movsd	-32 * SIZE(A1), %xmm0
#ifdef movsd
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(X1), %xmm4
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
#ifdef movsd
	xorps	%xmm1, %xmm1
#endif
	movsd	-32 * SIZE(A1, LDA), %xmm1
	mulps	%xmm4, %xmm1
	addps	%xmm1, %xmm9
#ifdef movsd
	xorps	%xmm2, %xmm2
#endif
	movsd	-32 * SIZE(A2), %xmm2
	mulps	%xmm4, %xmm2
	addps	%xmm2, %xmm10
#ifdef movsd
	xorps	%xmm3, %xmm3
#endif

	addq	$2 * SIZE, A1
	addq	$2 * SIZE, A2
	addq	$2 * SIZE, X1
	ALIGN_4

.L38:
	testq	$1, MM
	jle	.L39

	movss	-32 * SIZE(A1), %xmm0
	movss	-32 * SIZE(X1), %xmm4
	mulss	%xmm4, %xmm0
	addss	%xmm0, %xmm8
	movss	-32 * SIZE(A1, LDA), %xmm1
	mulss	%xmm4, %xmm1
	addss	%xmm1, %xmm9
	movss	-32 * SIZE(A2), %xmm2
	mulss	%xmm4, %xmm2
	addss	%xmm2, %xmm10
	ALIGN_4

.L39:
#ifdef HAVE_SSE3
	haddps	%xmm9, %xmm8
	haddps	%xmm11, %xmm10
	haddps	%xmm10, %xmm8

	pshufd	$0x1, %xmm8, %xmm9
	pshufd	$0x2, %xmm8, %xmm10
#else
	movaps	%xmm8, %xmm0
	unpcklps %xmm9, %xmm8
	unpckhps %xmm9, %xmm0

	movaps	%xmm10, %xmm1
	unpcklps %xmm11, %xmm10
	unpckhps %xmm11, %xmm1

	movaps	%xmm8, %xmm9
	unpcklps %xmm10, %xmm8
	unpckhps %xmm10, %xmm9

	movaps	%xmm0, %xmm10
	unpcklps %xmm1, %xmm0
	unpckhps %xmm1, %xmm10

	addps	%xmm9, %xmm8
	addps	%xmm0, %xmm10
	addps	%xmm10, %xmm8

	pshufd	$0x2, %xmm8, %xmm9
	pshufd	$0x1, %xmm8, %xmm10
#endif

	mulss	ALPHA, %xmm8
	mulss	ALPHA, %xmm9
	mulss	ALPHA, %xmm10

	addss	(Y), %xmm8
	addq	INCY, Y
	addss	(Y), %xmm9
	addq	INCY, Y
	addss	(Y), %xmm10
	addq	INCY, Y

	movss	%xmm8, (Y1)
	addq	INCY, Y1
	movss	%xmm9, (Y1)
	addq	INCY, Y1
	movss	%xmm10, (Y1)
	addq	INCY, Y1
	jmp	.L999
	ALIGN_4

.L40:
	cmpq	$2, N
	jne	.L50

	leaq	32 * SIZE(BUFFER), X1

	movq	A, A1
	leaq	(A1, LDA), A2
	leaq	(A1, LDA, 2), A

	xorps	%xmm8, %xmm8
	xorps	%xmm9, %xmm9

#ifdef ALIGNED_ACCESS
	cmpq	$3, M
	jle	.L47

	testq	$SIZE, A1
	je	.L4X

	movss	-32 * SIZE(A1), %xmm0
	movss	-32 * SIZE(X1), %xmm4
	mulss	%xmm4, %xmm0
	addss	%xmm0, %xmm8
	movss	-32 * SIZE(A2), %xmm1
	mulss	%xmm4, %xmm1
	addss	%xmm1, %xmm9

	addq	 $1 * SIZE, A1
	addq	 $1 * SIZE, A2
	addq	 $1 * SIZE, X1
	ALIGN_3

.L4X:
	testq	$2 * SIZE, A1
	je	.L4XX

#ifdef movsd
	xorps	%xmm0, %xmm0
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(A1), %xmm0
	movsd	-32 * SIZE(X1), %xmm4
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
#ifdef movsd
	xorps	%xmm1, %xmm1
#endif
	movsd	-32 * SIZE(A2), %xmm1
	mulps	%xmm4, %xmm1
	addps	%xmm1, %xmm9

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, X1
	ALIGN_3

.L4XX:
#endif

	MOVUPS_XL1 (-32 * SIZE, X1, %xmm4)
	MOVUPS_XL1 (-28 * SIZE, X1, %xmm5)

	movq	MM,  I
	sarq	$4,  I
	jle	.L45

	MOVUPS_A1 (-32 * SIZE, A1, %xmm0)
	MOVUPS_A1 (-32 * SIZE, A2, %xmm1)
	MOVUPS_A1 (-28 * SIZE, A1, %xmm12)
	MOVUPS_A1 (-28 * SIZE, A2, %xmm13)

	decq	I
	jle	.L43
	ALIGN_4

.L42:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 4 - 128 + PREOFFSET(A1)
#endif

	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-24 * SIZE, A1, %xmm0)
	mulps	%xmm4, %xmm1
	MOVUPS_XL1 (-24 * SIZE, X1, %xmm4)
	addps	%xmm1, %xmm9
	MOVUPS_A1 (-24 * SIZE, A2, %xmm1)

	mulps	%xmm5, %xmm12
	addps	%xmm12, %xmm8
	MOVUPS_A1 (-20 * SIZE, A1, %xmm12)
	mulps	%xmm5, %xmm13
	MOVUPS_XL1 (-20 * SIZE, X1, %xmm5)
	addps	%xmm13, %xmm9
	MOVUPS_A1 (-20 * SIZE, A2, %xmm13)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 4 - 128 + PREOFFSET(A2)
#endif

	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-16 * SIZE, A1, %xmm0)
	mulps	%xmm4, %xmm1
	MOVUPS_XL1 (-16 * SIZE, X1, %xmm4)
	addps	%xmm1, %xmm9
	MOVUPS_A1 (-16 * SIZE, A2, %xmm1)

#ifdef PREFETCHW
	PREFETCH	(PREFETCHSIZE) * 4 - 128 + PREOFFSET(X1)
#endif

	mulps	%xmm5, %xmm12
	addps	%xmm12, %xmm8
	MOVUPS_A1 (-12 * SIZE, A1, %xmm12)
	mulps	%xmm5, %xmm13
	MOVUPS_XL1 (-12 * SIZE, X1, %xmm5)
	addps	%xmm13, %xmm9
	MOVUPS_A1 (-12 * SIZE, A2, %xmm13)

	addq	$16 * SIZE, A1
	addq	$16 * SIZE, A2
	addq	$16 * SIZE, X1

	decq	I
	jg	.L42
	ALIGN_4

.L43:
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-24 * SIZE, A1, %xmm0)
	mulps	%xmm4, %xmm1
	MOVUPS_XL1 (-24 * SIZE, X1, %xmm4)
	addps	%xmm1, %xmm9
	MOVUPS_A1 (-24 * SIZE, A2, %xmm1)

	mulps	%xmm5, %xmm12
	addps	%xmm12, %xmm8
	MOVUPS_A1 (-20 * SIZE, A1, %xmm12)
	mulps	%xmm5, %xmm13
	MOVUPS_XL1 (-20 * SIZE, X1, %xmm5)
	addps	%xmm13, %xmm9
	MOVUPS_A1 (-20 * SIZE, A2, %xmm13)

	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	mulps	%xmm4, %xmm1
	MOVUPS_XL1 (-16 * SIZE, X1, %xmm4)
	addps	%xmm1, %xmm9

	mulps	%xmm5, %xmm12
	addps	%xmm12, %xmm8
	mulps	%xmm5, %xmm13
	MOVUPS_XL1 (-12 * SIZE, X1, %xmm5)
	addps	%xmm13, %xmm9

	addq	$16 * SIZE, A1
	addq	$16 * SIZE, A2
	addq	$16 * SIZE, X1
	ALIGN_4

.L45:
	testq	$8, MM
	jle	.L46

	MOVUPS_A1 (-32 * SIZE, A1, %xmm0)
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-32 * SIZE, A2, %xmm1)
	mulps	%xmm4, %xmm1
	MOVUPS_XL1 (-24 * SIZE, X1, %xmm4)
	addps	%xmm1, %xmm9

	MOVUPS_A1 (-28 * SIZE, A1, %xmm12)
	mulps	%xmm5, %xmm12
	addps	%xmm12, %xmm8
	MOVUPS_A1 (-28 * SIZE, A2, %xmm13)
	mulps	%xmm5, %xmm13
	addps	%xmm13, %xmm9

	addq	$8 * SIZE, A1
	addq	$8 * SIZE, A2
	addq	$8 * SIZE, X1
	ALIGN_4

.L46:
	testq	$4, MM
	jle	.L47

	MOVUPS_A1 (-32 * SIZE, A1, %xmm0)
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-32 * SIZE, A2, %xmm1)
	mulps	%xmm4, %xmm1
	addps	%xmm1, %xmm9

	addq	$4 * SIZE, A1
	addq	$4 * SIZE, A2
	addq	$4 * SIZE, X1
	ALIGN_4

.L47:
	testq	$2, MM
	jle	.L48

#ifdef movsd
	xorps	%xmm0, %xmm0
#endif
	movsd	-32 * SIZE(A1), %xmm0
#ifdef movsd
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(X1), %xmm4
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
#ifdef movsd
	xorps	%xmm1, %xmm1
#endif
	movsd	-32 * SIZE(A2), %xmm1
	mulps	%xmm4, %xmm1
	addps	%xmm1, %xmm9
	shufps	$0xe, %xmm4, %xmm4

	addq	$2 * SIZE, A1
	addq	$2 * SIZE, A2
	addq	$2 * SIZE, X1
	ALIGN_4

.L48:
	testq	$1, MM
	jle	.L49

	movss	-32 * SIZE(A1), %xmm0
	movss	-32 * SIZE(X1), %xmm4
	mulss	%xmm4, %xmm0
	addss	%xmm0, %xmm8
	movss	-32 * SIZE(A2), %xmm1
	mulss	%xmm4, %xmm1
	addss	%xmm1, %xmm9
	ALIGN_4

.L49:
#ifdef HAVE_SSE3
	haddps	%xmm9, %xmm8
	haddps	%xmm8, %xmm8
#else
	movaps	%xmm8, %xmm10
	unpcklps %xmm9, %xmm8
	unpckhps %xmm9, %xmm10

	addps	%xmm10, %xmm8
	movhlps %xmm8, %xmm9
	addps	%xmm9, %xmm8
#endif

	pshufd	$0x1, %xmm8, %xmm9

	mulss	ALPHA, %xmm8
	mulss	ALPHA, %xmm9

	addss	(Y), %xmm8
	addq	INCY, Y
	addss	(Y), %xmm9
	addq	INCY, Y

	movss	%xmm8, (Y1)
	addq	INCY, Y1
	movss	%xmm9, (Y1)
	addq	INCY, Y1
	jmp	.L999
	ALIGN_4

.L50:
	cmpq	$1, N
	jne	.L999

	leaq	32 * SIZE(BUFFER), X1

	movq	A, A1

	xorps	%xmm8, %xmm8
	xorps	%xmm9, %xmm9

#ifdef ALIGNED_ACCESS
	cmpq	$3, M
	jle	.L57

	testq	$SIZE, A1
	je	.L5X

	movss	-32 * SIZE(A1), %xmm0
	movss	-32 * SIZE(X1), %xmm4
	mulss	%xmm4, %xmm0
	addss	%xmm0, %xmm8

	addq	 $1 * SIZE, A1
	addq	 $1 * SIZE, X1
	ALIGN_3

.L5X:
	testq	$2 * SIZE, A1
	je	.L5XX


#ifdef movsd
	xorps	%xmm0, %xmm0
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(A1), %xmm0
	movsd	-32 * SIZE(X1), %xmm4
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	shufps	$0xe, %xmm4, %xmm4

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, X1
	ALIGN_3

.L5XX:
#endif

	MOVUPS_XL1 (-32 * SIZE, X1, %xmm4)
	MOVUPS_XL1 (-28 * SIZE, X1, %xmm5)

	movq	MM,  I
	sarq	$4,  I
	jle	.L55

	MOVUPS_A1 (-32 * SIZE, A1, %xmm0)
	MOVUPS_A1 (-28 * SIZE, A1, %xmm12)

	decq	I
	jle	.L53
	ALIGN_4

.L52:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 8 - 128 + PREOFFSET(A1)
#endif

	mulps	%xmm4, %xmm0
	MOVUPS_XL1 (-24 * SIZE, X1, %xmm4)
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-24 * SIZE, A1, %xmm0)

	mulps	%xmm5, %xmm12
	MOVUPS_XL1 (-20 * SIZE, X1, %xmm5)
	addps	%xmm12, %xmm9
	MOVUPS_A1 (-20 * SIZE, A1, %xmm12)

#ifdef PREFETCHW
	PREFETCH	(PREFETCHSIZE) * 8 - 128 + PREOFFSET(X1)
#endif

	mulps	%xmm4, %xmm0
	MOVUPS_XL1 (-16 * SIZE, X1, %xmm4)
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-16 * SIZE, A1, %xmm0)

	mulps	%xmm5, %xmm12
	MOVUPS_XL1 (-12 * SIZE, X1, %xmm5)
	addps	%xmm12, %xmm9
	MOVUPS_A1 (-12 * SIZE, A1, %xmm12)

	addq	$16 * SIZE, A1
	addq	$16 * SIZE, X1

	decq	I
	jg	.L52
	ALIGN_4

.L53:
	mulps	%xmm4, %xmm0
	MOVUPS_XL1 (-24 * SIZE, X1, %xmm4)
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-24 * SIZE, A1, %xmm0)

	mulps	%xmm5, %xmm12
	MOVUPS_XL1 (-20 * SIZE, X1, %xmm5)
	addps	%xmm12, %xmm9
	MOVUPS_A1 (-20 * SIZE, A1, %xmm12)

	mulps	%xmm4, %xmm0
	MOVUPS_XL1 (-16 * SIZE, X1, %xmm4)
	addps	%xmm0, %xmm8

	mulps	%xmm5, %xmm12
	MOVUPS_XL1 (-12 * SIZE, X1, %xmm5)
	addps	%xmm12, %xmm9

	addq	$16 * SIZE, A1
	addq	$16 * SIZE, X1
	ALIGN_4

.L55:
	testq	$8, MM
	jle	.L56

	MOVUPS_A1 (-32 * SIZE, A1, %xmm0)
	mulps	%xmm4, %xmm0
	MOVUPS_XL1 (-24 * SIZE, X1, %xmm4)
	addps	%xmm0, %xmm8

	MOVUPS_A1 (-28 * SIZE, A1, %xmm12)
	mulps	%xmm5, %xmm12
	addps	%xmm12, %xmm9

	addq	$8 * SIZE, A1
	addq	$8 * SIZE, X1
	ALIGN_4

.L56:
	testq	$4, MM
	jle	.L57

	MOVUPS_A1 (-32 * SIZE, A1, %xmm0)
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8

	addq	$4 * SIZE, A1
	addq	$4 * SIZE, X1
	ALIGN_4

.L57:
	testq	$2, MM
	jle	.L58

#ifdef movsd
	xorps	%xmm0, %xmm0
#endif
	movsd	-32 * SIZE(A1), %xmm0
#ifdef movsd
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(X1), %xmm4
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	shufps	$0xe, %xmm4, %xmm4

	addq	$2 * SIZE, A1
	addq	$2 * SIZE, X1
	ALIGN_4

.L58:
	testq	$1, MM
	jle	.L59

	movss	-32 * SIZE(A1), %xmm0
	movss	-32 * SIZE(X1), %xmm4
	mulss	%xmm4, %xmm0
	addss	%xmm0, %xmm8
	ALIGN_4

.L59:
	addps	%xmm9, %xmm8

#ifdef HAVE_SSE3
	haddps	%xmm8, %xmm8
	haddps	%xmm8, %xmm8
#else
	pshufd	$1, %xmm8, %xmm9
	pshufd	$2, %xmm8, %xmm10
	pshufd	$3, %xmm8, %xmm11

	addss	%xmm9, %xmm8
	addss	%xmm11, %xmm10
	addss	%xmm10, %xmm8
#endif

	mulss	ALPHA, %xmm8

	addss	(Y), %xmm8
	movss	%xmm8, (Y1)

#ifdef ALIGNED_ACCESS
	jmp	.L999
	ALIGN_4

.L100:
	testq	$2 * SIZE - 1, LDA
	jne	.L200

	cmpq	$4, N
	jl	.L110
	ALIGN_3

.L101:
	subq	$4, N

	leaq	32 * SIZE(BUFFER), X1

	movq	A, A1
	leaq	(A1, LDA, 2), A2
	leaq	(A1, LDA, 4), A

	xorps	%xmm8, %xmm8
	xorps	%xmm9, %xmm9
	xorps	%xmm10, %xmm10
	xorps	%xmm11, %xmm11

	cmpq	$3, M
	jle	.L107

	testq	$SIZE, A1
	je	.L10X

	movss	-32 * SIZE(A1), %xmm0
	movss	-32 * SIZE(X1), %xmm4
	mulss	%xmm4, %xmm0
	addss	%xmm0, %xmm8
	movss	-32 * SIZE(A1, LDA), %xmm1
	mulss	%xmm4, %xmm1
	addss	%xmm1, %xmm9
	movss	-32 * SIZE(A2), %xmm2
	mulss	%xmm4, %xmm2
	addss	%xmm2, %xmm10
	movss	-32 * SIZE(A2, LDA), %xmm3
	mulss	%xmm4, %xmm3
	addss	%xmm3, %xmm11

	addq	 $1 * SIZE, A1
	addq	 $1 * SIZE, A2
	addq	 $1 * SIZE, X1
	ALIGN_3

.L10X:
	testq	$2 * SIZE, A1
	je	.L10XX

#ifdef movsd
	xorps	%xmm0, %xmm0
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(A1), %xmm0
	movsd	-32 * SIZE(X1), %xmm4
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
#ifdef movsd
	xorps	%xmm1, %xmm1
#endif
	movsd	-32 * SIZE(A1, LDA), %xmm1
	mulps	%xmm4, %xmm1
	addps	%xmm1, %xmm9
#ifdef movsd
	xorps	%xmm2, %xmm2
#endif
	movsd	-32 * SIZE(A2), %xmm2
	mulps	%xmm4, %xmm2
	addps	%xmm2, %xmm10
#ifdef movsd
	xorps	%xmm3, %xmm3
#endif
	movsd	-32 * SIZE(A2, LDA), %xmm3
	mulps	%xmm4, %xmm3
	addps	%xmm3, %xmm11

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, X1
	ALIGN_3

.L10XX:
	MOVUPS_A2  (-34 * SIZE, A1, LDA, 1, %xmm12)
	MOVUPS_A2  (-34 * SIZE, A2, LDA, 1, %xmm13)

	MOVUPS_XL1 (-32 * SIZE, X1, %xmm4)
	MOVUPS_XL1 (-28 * SIZE, X1, %xmm5)

#ifdef PREFETCHW
	PREFETCHW	4 * SIZE(Y1)
#endif

	movq	MM,  I
	sarq	$4,  I
	jle	.L105

	MOVUPS_A1 (-32 * SIZE, A1, %xmm0)
	MOVUPS_A2 (-30 * SIZE, A1, LDA, 1, %xmm1)
	MOVUPS_A1 (-32 * SIZE, A2, %xmm2)
	MOVUPS_A2 (-30 * SIZE, A2, LDA, 1, %xmm3)

	decq	I
	jle	.L103
	ALIGN_4

.L102:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(A1)
#endif

	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-28 * SIZE, A1, %xmm0)
	shufps	$0x4e, %xmm1, %xmm12
	mulps	%xmm4, %xmm12
	addps	%xmm12, %xmm9
	MOVUPS_A2 (-26 * SIZE, A1, LDA, 1, %xmm12)
	mulps	%xmm4, %xmm2
	addps	%xmm2, %xmm10
	MOVUPS_A1 (-28 * SIZE, A2, %xmm2)
	shufps	$0x4e, %xmm3, %xmm13
	mulps	%xmm4, %xmm13
	MOVUPS_XL1 (-24 * SIZE, X1, %xmm4)
	addps	%xmm13, %xmm11
	MOVUPS_A2 (-26 * SIZE, A2, LDA, 1, %xmm13)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(A1, LDA)
#endif

	mulps	%xmm5, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-24 * SIZE, A1, %xmm0)
	shufps	$0x4e, %xmm12, %xmm1
	mulps	%xmm5, %xmm1
	addps	%xmm1, %xmm9
	MOVUPS_A2 (-22 * SIZE, A1, LDA, 1, %xmm1)
	mulps	%xmm5, %xmm2
	addps	%xmm2, %xmm10
	MOVUPS_A1 (-24 * SIZE, A2, %xmm2)
	shufps	$0x4e, %xmm13, %xmm3
	mulps	%xmm5, %xmm3
	MOVUPS_XL1 (-20 * SIZE, X1, %xmm5)
	addps	%xmm3, %xmm11
	MOVUPS_A2 (-22 * SIZE, A2, LDA, 1, %xmm3)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(A2)
#endif

	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-20 * SIZE, A1, %xmm0)
	shufps	$0x4e, %xmm1, %xmm12
	mulps	%xmm4, %xmm12
	addps	%xmm12, %xmm9
	MOVUPS_A2 (-18 * SIZE, A1, LDA, 1, %xmm12)
	mulps	%xmm4, %xmm2
	addps	%xmm2, %xmm10
	MOVUPS_A1 (-20 * SIZE, A2, %xmm2)
	shufps	$0x4e, %xmm3, %xmm13
	mulps	%xmm4, %xmm13
	MOVUPS_XL1 (-16 * SIZE, X1, %xmm4)
	addps	%xmm13, %xmm11
	MOVUPS_A2 (-18 * SIZE, A2, LDA, 1, %xmm13)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(A2, LDA)
#endif

	mulps	%xmm5, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-16 * SIZE, A1, %xmm0)
	shufps	$0x4e, %xmm12, %xmm1
	mulps	%xmm5, %xmm1
	addps	%xmm1, %xmm9
	MOVUPS_A2 (-14 * SIZE, A1, LDA, 1, %xmm1)

#ifdef PREFETCHW
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(X1)
#endif

	mulps	%xmm5, %xmm2
	addps	%xmm2, %xmm10
	MOVUPS_A1 (-16 * SIZE, A2, %xmm2)
	shufps	$0x4e, %xmm13, %xmm3
	mulps	%xmm5, %xmm3
	MOVUPS_XL1 (-12 * SIZE, X1, %xmm5)
	addps	%xmm3, %xmm11
	MOVUPS_A2 (-14 * SIZE, A2, LDA, 1, %xmm3)

	addq	$16 * SIZE, A1
	addq	$16 * SIZE, A2
	addq	$16 * SIZE, X1

	decq	I
	jg	.L102
	ALIGN_4

.L103:
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-28 * SIZE, A1, %xmm0)
	shufps	$0x4e, %xmm1, %xmm12
	mulps	%xmm4, %xmm12
	addps	%xmm12, %xmm9
	MOVUPS_A2 (-26 * SIZE, A1, LDA, 1, %xmm12)
	mulps	%xmm4, %xmm2
	addps	%xmm2, %xmm10
	MOVUPS_A1 (-28 * SIZE, A2, %xmm2)
	shufps	$0x4e, %xmm3, %xmm13
	mulps	%xmm4, %xmm13
	MOVUPS_XL1 (-24 * SIZE, X1, %xmm4)
	addps	%xmm13, %xmm11
	MOVUPS_A2 (-26 * SIZE, A2, LDA, 1, %xmm13)

	mulps	%xmm5, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-24 * SIZE, A1, %xmm0)
	shufps	$0x4e, %xmm12, %xmm1
	mulps	%xmm5, %xmm1
	addps	%xmm1, %xmm9
	MOVUPS_A2 (-22 * SIZE, A1, LDA, 1, %xmm1)
	mulps	%xmm5, %xmm2
	addps	%xmm2, %xmm10
	MOVUPS_A1 (-24 * SIZE, A2, %xmm2)
	shufps	$0x4e, %xmm13, %xmm3
	mulps	%xmm5, %xmm3
	MOVUPS_XL1 (-20 * SIZE, X1, %xmm5)
	addps	%xmm3, %xmm11
	MOVUPS_A2 (-22 * SIZE, A2, LDA, 1, %xmm3)

	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-20 * SIZE, A1, %xmm0)
	shufps	$0x4e, %xmm1, %xmm12
	mulps	%xmm4, %xmm12
	addps	%xmm12, %xmm9
	MOVUPS_A2 (-18 * SIZE, A1, LDA, 1, %xmm12)
	mulps	%xmm4, %xmm2
	addps	%xmm2, %xmm10
	MOVUPS_A1 (-20 * SIZE, A2, %xmm2)
	shufps	$0x4e, %xmm3, %xmm13
	mulps	%xmm4, %xmm13
	MOVUPS_XL1 (-16 * SIZE, X1, %xmm4)
	addps	%xmm13, %xmm11
	MOVUPS_A2 (-18 * SIZE, A2, LDA, 1, %xmm13)

	mulps	%xmm5, %xmm0
	addps	%xmm0, %xmm8
	shufps	$0x4e, %xmm12, %xmm1
	mulps	%xmm5, %xmm1
	addps	%xmm1, %xmm9
	mulps	%xmm5, %xmm2
	addps	%xmm2, %xmm10
	shufps	$0x4e, %xmm13, %xmm3
	mulps	%xmm5, %xmm3
	MOVUPS_XL1 (-12 * SIZE, X1, %xmm5)
	addps	%xmm3, %xmm11

	addq	$16 * SIZE, A1
	addq	$16 * SIZE, A2
	addq	$16 * SIZE, X1
	ALIGN_4

.L105:
	testq	$8, MM
	jle	.L106

	MOVUPS_A1 (-32 * SIZE, A1, %xmm0)
	MOVUPS_A2 (-30 * SIZE, A1, LDA, 1, %xmm1)
	MOVUPS_A1 (-32 * SIZE, A2, %xmm2)
	MOVUPS_A2 (-30 * SIZE, A2, LDA, 1, %xmm3)

	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-28 * SIZE, A1, %xmm0)
	shufps	$0x4e, %xmm1, %xmm12
	mulps	%xmm4, %xmm12
	addps	%xmm12, %xmm9
	MOVUPS_A2 (-26 * SIZE, A1, LDA, 1, %xmm12)
	mulps	%xmm4, %xmm2
	addps	%xmm2, %xmm10
	MOVUPS_A1 (-28 * SIZE, A2, %xmm2)
	shufps	$0x4e, %xmm3, %xmm13
	mulps	%xmm4, %xmm13
	MOVUPS_XL1 (-24 * SIZE, X1, %xmm4)
	addps	%xmm13, %xmm11
	MOVUPS_A2 (-26 * SIZE, A2, LDA, 1, %xmm13)

	mulps	%xmm5, %xmm0
	addps	%xmm0, %xmm8
	shufps	$0x4e, %xmm12, %xmm1
	mulps	%xmm5, %xmm1
	addps	%xmm1, %xmm9
	mulps	%xmm5, %xmm2
	addps	%xmm2, %xmm10
	shufps	$0x4e, %xmm13, %xmm3
	mulps	%xmm5, %xmm3
	addps	%xmm3, %xmm11

	addq	$8 * SIZE, A1
	addq	$8 * SIZE, A2
	addq	$8 * SIZE, X1
	ALIGN_4

.L106:
	testq	$4, MM
	jle	.L107

	MOVUPS_A1 (-32 * SIZE, A1, %xmm0)
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A2 (-30 * SIZE, A1, LDA, 1, %xmm1)
	shufps	$0x4e, %xmm1, %xmm12
	mulps	%xmm4, %xmm12
	addps	%xmm12, %xmm9

	MOVUPS_A1 (-32 * SIZE, A2, %xmm2)
	mulps	%xmm4, %xmm2
	addps	%xmm2, %xmm10
	MOVUPS_A2 (-30 * SIZE, A2, LDA, 1, %xmm3)
	shufps	$0x4e, %xmm3, %xmm13
	mulps	%xmm4, %xmm13
	addps	%xmm13, %xmm11

	addq	$4 * SIZE, A1
	addq	$4 * SIZE, A2
	addq	$4 * SIZE, X1
	ALIGN_4

.L107:
	testq	$2, MM
	jle	.L108

#ifdef movsd
	xorps	%xmm0, %xmm0
#endif
	movsd	-32 * SIZE(A1), %xmm0
#ifdef movsd
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(X1), %xmm4
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
#ifdef movsd
	xorps	%xmm1, %xmm1
#endif
	movsd	-32 * SIZE(A1, LDA), %xmm1
	mulps	%xmm4, %xmm1
	addps	%xmm1, %xmm9
#ifdef movsd
	xorps	%xmm2, %xmm2
#endif
	movsd	-32 * SIZE(A2), %xmm2
	mulps	%xmm4, %xmm2
	addps	%xmm2, %xmm10
#ifdef movsd
	xorps	%xmm3, %xmm3
#endif
	movsd	-32 * SIZE(A2, LDA), %xmm3
	mulps	%xmm4, %xmm3
	addps	%xmm3, %xmm11
	shufps	$0xe, %xmm4, %xmm4

	addq	$2 * SIZE, A1
	addq	$2 * SIZE, A2
	addq	$2 * SIZE, X1
	ALIGN_4

.L108:
	testq	$1, MM
	jle	.L109

	movss	-32 * SIZE(A1), %xmm0
	movss	-32 * SIZE(X1), %xmm4
	mulss	%xmm4, %xmm0
	addss	%xmm0, %xmm8
	movss	-32 * SIZE(A1, LDA), %xmm1
	mulss	%xmm4, %xmm1
	addss	%xmm1, %xmm9
	movss	-32 * SIZE(A2), %xmm2
	mulss	%xmm4, %xmm2
	addss	%xmm2, %xmm10
	movss	-32 * SIZE(A2, LDA), %xmm3
	mulss	%xmm4, %xmm3
	addss	%xmm3, %xmm11
	ALIGN_4

.L109:
#ifdef HAVE_SSE3
	haddps	%xmm9, %xmm8
	haddps	%xmm11, %xmm10
	haddps	%xmm10, %xmm8

	pshufd	$0x1, %xmm8, %xmm9
	pshufd	$0x2, %xmm8, %xmm10
	pshufd	$0x3, %xmm8, %xmm11
#else
	movaps	%xmm8, %xmm0
	unpcklps %xmm9, %xmm8
	unpckhps %xmm9, %xmm0

	movaps	%xmm10, %xmm1
	unpcklps %xmm11, %xmm10
	unpckhps %xmm11, %xmm1

	movaps	%xmm8, %xmm9
	unpcklps %xmm10, %xmm8
	unpckhps %xmm10, %xmm9

	movaps	%xmm0, %xmm10
	unpcklps %xmm1, %xmm0
	unpckhps %xmm1, %xmm10

	addps	%xmm9, %xmm8
	addps	%xmm0, %xmm10
	addps	%xmm10, %xmm8

	pshufd	$0x2, %xmm8, %xmm9
	pshufd	$0x1, %xmm8, %xmm10
	pshufd	$0x3, %xmm8, %xmm11
#endif

	mulss	ALPHA, %xmm8
	mulss	ALPHA, %xmm9
	mulss	ALPHA, %xmm10
	mulss	ALPHA, %xmm11

	addss	(Y), %xmm8
	addq	INCY, Y
	addss	(Y), %xmm9
	addq	INCY, Y
	addss	(Y), %xmm10
	addq	INCY, Y
	addss	(Y), %xmm11
	addq	INCY, Y

	movss	%xmm8, (Y1)
	addq	INCY, Y1
	movss	%xmm9, (Y1)
	addq	INCY, Y1
	movss	%xmm10, (Y1)
	addq	INCY, Y1
	movss	%xmm11, (Y1)
	addq	INCY, Y1

	cmpq	$4, N
	jge	.L101
	ALIGN_4

.L110:
	cmpq	$3, N
	jne	.L120

	leaq	32 * SIZE(BUFFER), X1

	movq	A, A1
	leaq	(A1, LDA, 2), A2
	leaq	(A1, LDA, 4), A

	xorps	%xmm8, %xmm8
	xorps	%xmm9, %xmm9
	xorps	%xmm10, %xmm10

	cmpq	$3, M
	jle	.L117

	testq	$SIZE, A1
	je	.L11X

	movss	-32 * SIZE(A1), %xmm0
	movss	-32 * SIZE(X1), %xmm4
	mulss	%xmm4, %xmm0
	addss	%xmm0, %xmm8
	movss	-32 * SIZE(A1, LDA), %xmm1
	mulss	%xmm4, %xmm1
	addss	%xmm1, %xmm9
	movss	-32 * SIZE(A2), %xmm2
	mulss	%xmm4, %xmm2
	addss	%xmm2, %xmm10

	addq	 $1 * SIZE, A1
	addq	 $1 * SIZE, A2
	addq	 $1 * SIZE, X1
	ALIGN_3

.L11X:
	testq	$2 * SIZE, A1
	je	.L11XX

#ifdef movsd
	xorps	%xmm0, %xmm0
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(A1), %xmm0
	movsd	-32 * SIZE(X1), %xmm4
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
#ifdef movsd
	xorps	%xmm1, %xmm1
#endif
	movsd	-32 * SIZE(A1, LDA), %xmm1
	mulps	%xmm4, %xmm1
	addps	%xmm1, %xmm9
#ifdef movsd
	xorps	%xmm2, %xmm2
#endif
	movsd	-32 * SIZE(A2), %xmm2
	mulps	%xmm4, %xmm2
	addps	%xmm2, %xmm10

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, X1
	ALIGN_3

.L11XX:
	MOVUPS_A2  (-34 * SIZE, A1, LDA, 1, %xmm12)

	MOVUPS_XL1 (-32 * SIZE, X1, %xmm4)
	MOVUPS_XL1 (-28 * SIZE, X1, %xmm5)

	movq	MM,  I
	sarq	$4,  I
	jle	.L115

	MOVUPS_A1 (-32 * SIZE, A1, %xmm0)
	MOVUPS_A2 (-30 * SIZE, A1, LDA, 1, %xmm1)
	MOVUPS_A1 (-32 * SIZE, A2, %xmm2)

	decq	I
	jle	.L113
	ALIGN_4

.L112:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 3 - 128 + PREOFFSET(A1)
#endif

	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-28 * SIZE, A1, %xmm0)
	shufps	$0x4e, %xmm1, %xmm12
	mulps	%xmm4, %xmm12
	addps	%xmm12, %xmm9
	MOVUPS_A2 (-26 * SIZE, A1, LDA, 1, %xmm12)
	mulps	%xmm4, %xmm2
	MOVUPS_XL1 (-24 * SIZE, X1, %xmm4)
	addps	%xmm2, %xmm10
	MOVUPS_A1 (-28 * SIZE, A2, %xmm2)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 3 - 128 + PREOFFSET(A1, LDA)
#endif

	mulps	%xmm5, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-24 * SIZE, A1, %xmm0)
	shufps	$0x4e, %xmm12, %xmm1
	mulps	%xmm5, %xmm1
	addps	%xmm1, %xmm9
	MOVUPS_A2 (-22 * SIZE, A1, LDA, 1, %xmm1)
	mulps	%xmm5, %xmm2
	MOVUPS_XL1 (-20 * SIZE, X1, %xmm5)
	addps	%xmm2, %xmm10
	MOVUPS_A1 (-24 * SIZE, A2, %xmm2)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 3 - 128 + PREOFFSET(A2)
#endif

	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-20 * SIZE, A1, %xmm0)
	shufps	$0x4e, %xmm1, %xmm12
	mulps	%xmm4, %xmm12
	addps	%xmm12, %xmm9
	MOVUPS_A2 (-18 * SIZE, A1, LDA, 1, %xmm12)
	mulps	%xmm4, %xmm2
	MOVUPS_XL1 (-16 * SIZE, X1, %xmm4)
	addps	%xmm2, %xmm10
	MOVUPS_A1 (-20 * SIZE, A2, %xmm2)

#ifdef PREFETCHW
	PREFETCH	(PREFETCHSIZE) * 3 - 128 + PREOFFSET(X1)
#endif

	mulps	%xmm5, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-16 * SIZE, A1, %xmm0)
	shufps	$0x4e, %xmm12, %xmm1
	mulps	%xmm5, %xmm1
	addps	%xmm1, %xmm9
	MOVUPS_A2 (-14 * SIZE, A1, LDA, 1, %xmm1)
	mulps	%xmm5, %xmm2
	MOVUPS_XL1 (-12 * SIZE, X1, %xmm5)
	addps	%xmm2, %xmm10
	MOVUPS_A1 (-16 * SIZE, A2, %xmm2)

	addq	$16 * SIZE, A1
	addq	$16 * SIZE, A2
	addq	$16 * SIZE, X1

	decq	I
	jg	.L112
	ALIGN_4

.L113:
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-28 * SIZE, A1, %xmm0)
	shufps	$0x4e, %xmm1, %xmm12
	mulps	%xmm4, %xmm12
	addps	%xmm12, %xmm9
	MOVUPS_A2 (-26 * SIZE, A1, LDA, 1, %xmm12)
	mulps	%xmm4, %xmm2
	MOVUPS_XL1 (-24 * SIZE, X1, %xmm4)
	addps	%xmm2, %xmm10
	MOVUPS_A1 (-28 * SIZE, A2, %xmm2)

	mulps	%xmm5, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-24 * SIZE, A1, %xmm0)
	shufps	$0x4e, %xmm12, %xmm1
	mulps	%xmm5, %xmm1
	addps	%xmm1, %xmm9
	MOVUPS_A2 (-22 * SIZE, A1, LDA, 1, %xmm1)
	mulps	%xmm5, %xmm2
	MOVUPS_XL1 (-20 * SIZE, X1, %xmm5)
	addps	%xmm2, %xmm10
	MOVUPS_A1 (-24 * SIZE, A2, %xmm2)

	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-20 * SIZE, A1, %xmm0)
	shufps	$0x4e, %xmm1, %xmm12
	mulps	%xmm4, %xmm12
	addps	%xmm12, %xmm9
	MOVUPS_A2 (-18 * SIZE, A1, LDA, 1, %xmm12)
	mulps	%xmm4, %xmm2
	MOVUPS_XL1 (-16 * SIZE, X1, %xmm4)
	addps	%xmm2, %xmm10
	MOVUPS_A1 (-20 * SIZE, A2, %xmm2)

	mulps	%xmm5, %xmm0
	addps	%xmm0, %xmm8
	shufps	$0x4e, %xmm12, %xmm1
	mulps	%xmm5, %xmm1
	addps	%xmm1, %xmm9
	mulps	%xmm5, %xmm2
	MOVUPS_XL1 (-12 * SIZE, X1, %xmm5)
	addps	%xmm2, %xmm10

	addq	$16 * SIZE, A1
	addq	$16 * SIZE, A2
	addq	$16 * SIZE, X1
	ALIGN_4

.L115:
	testq	$8, MM
	jle	.L116

	MOVUPS_A1 (-32 * SIZE, A1, %xmm0)
	MOVUPS_A2 (-30 * SIZE, A1, LDA, 1, %xmm1)
	MOVUPS_A1 (-32 * SIZE, A2, %xmm2)

	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-28 * SIZE, A1, %xmm0)
	shufps	$0x4e, %xmm1, %xmm12
	mulps	%xmm4, %xmm12
	addps	%xmm12, %xmm9
	MOVUPS_A2 (-26 * SIZE, A1, LDA, 1, %xmm12)
	mulps	%xmm4, %xmm2
	MOVUPS_XL1 (-24 * SIZE, X1, %xmm4)
	addps	%xmm2, %xmm10
	MOVUPS_A1 (-28 * SIZE, A2, %xmm2)

	mulps	%xmm5, %xmm0
	addps	%xmm0, %xmm8
	shufps	$0x4e, %xmm12, %xmm1
	mulps	%xmm5, %xmm1
	addps	%xmm1, %xmm9
	mulps	%xmm5, %xmm2
	addps	%xmm2, %xmm10

	addq	$8 * SIZE, A1
	addq	$8 * SIZE, A2
	addq	$8 * SIZE, X1
	ALIGN_4

.L116:
	testq	$4, MM
	jle	.L117

	MOVUPS_A1 (-32 * SIZE, A1, %xmm0)
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A2 (-30 * SIZE, A1, LDA, 1, %xmm1)
	shufps	$0x4e, %xmm1, %xmm12
	mulps	%xmm4, %xmm12
	addps	%xmm12, %xmm9

	MOVUPS_A1 (-32 * SIZE, A2, %xmm2)
	mulps	%xmm4, %xmm2
	addps	%xmm2, %xmm10

	addq	$4 * SIZE, A1
	addq	$4 * SIZE, A2
	addq	$4 * SIZE, X1
	ALIGN_4

.L117:
	testq	$2, MM
	jle	.L118

#ifdef movsd
	xorps	%xmm0, %xmm0
#endif
	movsd	-32 * SIZE(A1), %xmm0
#ifdef movsd
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(X1), %xmm4
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
#ifdef movsd
	xorps	%xmm1, %xmm1
#endif
	movsd	-32 * SIZE(A1, LDA), %xmm1
	mulps	%xmm4, %xmm1
	addps	%xmm1, %xmm9
#ifdef movsd
	xorps	%xmm2, %xmm2
#endif
	movsd	-32 * SIZE(A2), %xmm2
	mulps	%xmm4, %xmm2
	addps	%xmm2, %xmm10

	addq	$2 * SIZE, A1
	addq	$2 * SIZE, A2
	addq	$2 * SIZE, X1
	ALIGN_4

.L118:
	testq	$1, MM
	jle	.L119

	movss	-32 * SIZE(A1), %xmm0
	movss	-32 * SIZE(X1), %xmm4
	mulss	%xmm4, %xmm0
	addss	%xmm0, %xmm8
	movss	-32 * SIZE(A1, LDA), %xmm1
	mulss	%xmm4, %xmm1
	addss	%xmm1, %xmm9
	movss	-32 * SIZE(A2), %xmm2
	mulss	%xmm4, %xmm2
	addss	%xmm2, %xmm10
	ALIGN_4

.L119:
#ifdef HAVE_SSE3
	haddps	%xmm9, %xmm8
	haddps	%xmm11, %xmm10
	haddps	%xmm10, %xmm8

	pshufd	$0x1, %xmm8, %xmm9
	pshufd	$0x2, %xmm8, %xmm10
#else
	movaps	%xmm8, %xmm0
	unpcklps %xmm9, %xmm8
	unpckhps %xmm9, %xmm0

	movaps	%xmm10, %xmm1
	unpcklps %xmm11, %xmm10
	unpckhps %xmm11, %xmm1

	movaps	%xmm8, %xmm9
	unpcklps %xmm10, %xmm8
	unpckhps %xmm10, %xmm9

	movaps	%xmm0, %xmm10
	unpcklps %xmm1, %xmm0
	unpckhps %xmm1, %xmm10

	addps	%xmm9, %xmm8
	addps	%xmm0, %xmm10
	addps	%xmm10, %xmm8

	pshufd	$0x2, %xmm8, %xmm9
	pshufd	$0x1, %xmm8, %xmm10
#endif

	mulss	ALPHA, %xmm8
	mulss	ALPHA, %xmm9
	mulss	ALPHA, %xmm10

	addss	(Y), %xmm8
	addq	INCY, Y
	addss	(Y), %xmm9
	addq	INCY, Y
	addss	(Y), %xmm10

	movss	%xmm8, (Y1)
	addq	INCY, Y1
	movss	%xmm9, (Y1)
	addq	INCY, Y1
	movss	%xmm10, (Y1)
	jmp	.L999
	ALIGN_4

.L120:
	cmpq	$2, N
	jne	.L130

	leaq	32 * SIZE(BUFFER), X1

	movq	A, A1
	leaq	(A1, LDA), A2
	leaq	(A1, LDA, 2), A

	xorps	%xmm8, %xmm8
	xorps	%xmm9, %xmm9

	cmpq	$3, M
	jle	.L127

	testq	$SIZE, A1
	je	.L12X

	movss	-32 * SIZE(A1), %xmm0
	movss	-32 * SIZE(X1), %xmm4
	mulss	%xmm4, %xmm0
	addss	%xmm0, %xmm8
	movss	-32 * SIZE(A2), %xmm1
	mulss	%xmm4, %xmm1
	addss	%xmm1, %xmm9

	addq	 $1 * SIZE, A1
	addq	 $1 * SIZE, A2
	addq	 $1 * SIZE, X1
	ALIGN_3

.L12X:
	testq	$2 * SIZE, A1
	je	.L12XX

#ifdef movsd
	xorps	%xmm0, %xmm0
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(A1), %xmm0
	movsd	-32 * SIZE(X1), %xmm4
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
#ifdef movsd
	xorps	%xmm1, %xmm1
#endif
	movsd	-32 * SIZE(A2), %xmm1
	mulps	%xmm4, %xmm1
	addps	%xmm1, %xmm9

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, X1
	ALIGN_3

.L12XX:
	MOVUPS_A1  (-34 * SIZE, A2, %xmm12)

	MOVUPS_XL1 (-32 * SIZE, X1, %xmm4)
	MOVUPS_XL1 (-28 * SIZE, X1, %xmm5)

	movq	MM,  I
	sarq	$4,  I
	jle	.L125

	MOVUPS_A1 (-32 * SIZE, A1, %xmm0)
	MOVUPS_A1 (-30 * SIZE, A2, %xmm1)

	decq	I
	jle	.L123
	ALIGN_4

.L122:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 4 - 128 + PREOFFSET(A1)
#endif

	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-28 * SIZE, A1, %xmm0)
	shufps	$0x4e, %xmm1, %xmm12
	mulps	%xmm4, %xmm12
	MOVUPS_XL1 (-24 * SIZE, X1, %xmm4)
	addps	%xmm12, %xmm9
	MOVUPS_A1 (-26 * SIZE, A2, %xmm12)

	mulps	%xmm5, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-24 * SIZE, A1, %xmm0)
	shufps	$0x4e, %xmm12, %xmm1
	mulps	%xmm5, %xmm1
	MOVUPS_XL1 (-20 * SIZE, X1, %xmm5)
	addps	%xmm1, %xmm9
	MOVUPS_A1 (-22 * SIZE, A2, %xmm1)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 4 - 128 + PREOFFSET(A2)
#endif

	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-20 * SIZE, A1, %xmm0)
	shufps	$0x4e, %xmm1, %xmm12
	mulps	%xmm4, %xmm12
	MOVUPS_XL1 (-16 * SIZE, X1, %xmm4)
	addps	%xmm12, %xmm9
	MOVUPS_A1 (-18 * SIZE, A2, %xmm12)

#ifdef PREFETCHW
	PREFETCH	(PREFETCHSIZE) * 4 - 128 + PREOFFSET(X1)
#endif

	mulps	%xmm5, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-16 * SIZE, A1, %xmm0)
	shufps	$0x4e, %xmm12, %xmm1
	mulps	%xmm5, %xmm1
	MOVUPS_XL1 (-12 * SIZE, X1, %xmm5)
	addps	%xmm1, %xmm9
	MOVUPS_A1 (-14 * SIZE, A2, %xmm1)

	addq	$16 * SIZE, A1
	addq	$16 * SIZE, A2
	addq	$16 * SIZE, X1

	decq	I
	jg	.L122
	ALIGN_4

.L123:
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-28 * SIZE, A1, %xmm0)
	shufps	$0x4e, %xmm1, %xmm12
	mulps	%xmm4, %xmm12
	MOVUPS_XL1 (-24 * SIZE, X1, %xmm4)
	addps	%xmm12, %xmm9
	MOVUPS_A1 (-26 * SIZE, A2, %xmm12)

	mulps	%xmm5, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-24 * SIZE, A1, %xmm0)
	shufps	$0x4e, %xmm12, %xmm1
	mulps	%xmm5, %xmm1
	MOVUPS_XL1 (-20 * SIZE, X1, %xmm5)
	addps	%xmm1, %xmm9
	MOVUPS_A1 (-22 * SIZE, A2, %xmm1)

	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-20 * SIZE, A1, %xmm0)
	shufps	$0x4e, %xmm1, %xmm12
	mulps	%xmm4, %xmm12
	MOVUPS_XL1 (-16 * SIZE, X1, %xmm4)
	addps	%xmm12, %xmm9
	MOVUPS_A1 (-18 * SIZE, A2, %xmm12)

	mulps	%xmm5, %xmm0
	addps	%xmm0, %xmm8
	shufps	$0x4e, %xmm12, %xmm1
	mulps	%xmm5, %xmm1
	MOVUPS_XL1 (-12 * SIZE, X1, %xmm5)
	addps	%xmm1, %xmm9

	addq	$16 * SIZE, A1
	addq	$16 * SIZE, A2
	addq	$16 * SIZE, X1
	ALIGN_4

.L125:
	testq	$8, MM
	jle	.L126

	MOVUPS_A1 (-32 * SIZE, A1, %xmm0)
	MOVUPS_A1 (-30 * SIZE, A2, %xmm1)

	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-28 * SIZE, A1, %xmm0)
	shufps	$0x4e, %xmm1, %xmm12
	mulps	%xmm4, %xmm12
	MOVUPS_XL1 (-24 * SIZE, X1, %xmm4)
	addps	%xmm12, %xmm9
	MOVUPS_A1 (-26 * SIZE, A2, %xmm12)

	mulps	%xmm5, %xmm0
	addps	%xmm0, %xmm8
	shufps	$0x4e, %xmm12, %xmm1
	mulps	%xmm5, %xmm1
	addps	%xmm1, %xmm9

	addq	$8 * SIZE, A1
	addq	$8 * SIZE, A2
	addq	$8 * SIZE, X1
	ALIGN_4

.L126:
	testq	$4, MM
	jle	.L127

	MOVUPS_A1 (-32 * SIZE, A1, %xmm0)
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-30 * SIZE, A2, %xmm1)
	shufps	$0x4e, %xmm1, %xmm12
	mulps	%xmm4, %xmm12
	addps	%xmm12, %xmm9

	addq	$4 * SIZE, A1
	addq	$4 * SIZE, A2
	addq	$4 * SIZE, X1
	ALIGN_4

.L127:
	testq	$2, MM
	jle	.L128

#ifdef movsd
	xorps	%xmm0, %xmm0
#endif
	movsd	-32 * SIZE(A1), %xmm0
#ifdef movsd
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(X1), %xmm4
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
#ifdef movsd
	xorps	%xmm1, %xmm1
#endif
	movsd	-32 * SIZE(A2), %xmm1
	mulps	%xmm4, %xmm1
	addps	%xmm1, %xmm9
	shufps	$0xe, %xmm4, %xmm4

	addq	$2 * SIZE, A1
	addq	$2 * SIZE, A2
	addq	$2 * SIZE, X1
	ALIGN_4

.L128:
	testq	$1, MM
	jle	.L129

	movss	-32 * SIZE(A1), %xmm0
	movss	-32 * SIZE(X1), %xmm4
	mulss	%xmm4, %xmm0
	addss	%xmm0, %xmm8
	movss	-32 * SIZE(A2), %xmm1
	mulss	%xmm4, %xmm1
	addss	%xmm1, %xmm9
	ALIGN_4

.L129:
#ifdef HAVE_SSE3
	haddps	%xmm9, %xmm8
	haddps	%xmm8, %xmm8
#else
	movaps	%xmm8, %xmm10
	unpcklps %xmm9, %xmm8
	unpckhps %xmm9, %xmm10

	addps	%xmm10, %xmm8
	movhlps %xmm8, %xmm9
	addps	%xmm9, %xmm8
#endif

	pshufd	$0x1, %xmm8, %xmm9

	mulss	ALPHA, %xmm8
	mulss	ALPHA, %xmm9

	addss	(Y), %xmm8
	addq	INCY, Y
	addss	(Y), %xmm9
	addq	INCY, Y

	movss	%xmm8, (Y1)
	addq	INCY, Y1
	movss	%xmm9, (Y1)
	addq	INCY, Y1
	jmp	.L999
	ALIGN_4

.L130:
	cmpq	$1, N
	jne	.L999

	leaq	32 * SIZE(BUFFER), X1

	movq	A, A1

	xorps	%xmm8, %xmm8
	xorps	%xmm9, %xmm9

	cmpq	$3, M
	jle	.L137

	testq	$SIZE, A1
	je	.L13X

	movss	-32 * SIZE(A1), %xmm0
	movss	-32 * SIZE(X1), %xmm4
	mulss	%xmm4, %xmm0
	addss	%xmm0, %xmm8

	addq	 $1 * SIZE, A1
	addq	 $1 * SIZE, X1
	ALIGN_3

.L13X:
	testq	$2 * SIZE, A1
	je	.L13XX


#ifdef movsd
	xorps	%xmm0, %xmm0
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(A1), %xmm0
	movsd	-32 * SIZE(X1), %xmm4
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	shufps	$0xe, %xmm4, %xmm4

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, X1
	ALIGN_3

.L13XX:

	MOVUPS_XL1 (-32 * SIZE, X1, %xmm4)
	MOVUPS_XL1 (-28 * SIZE, X1, %xmm5)

	movq	MM,  I
	sarq	$4,  I
	jle	.L135

	MOVUPS_A1 (-32 * SIZE, A1, %xmm0)
	MOVUPS_A1 (-28 * SIZE, A1, %xmm12)

	decq	I
	jle	.L133
	ALIGN_4

.L132:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 8 - 128 + PREOFFSET(A1)
#endif

	mulps	%xmm4, %xmm0
	MOVUPS_XL1 (-24 * SIZE, X1, %xmm4)
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-24 * SIZE, A1, %xmm0)

	mulps	%xmm5, %xmm12
	MOVUPS_XL1 (-20 * SIZE, X1, %xmm5)
	addps	%xmm12, %xmm9
	MOVUPS_A1 (-20 * SIZE, A1, %xmm12)

#ifdef PREFETCHW
	PREFETCH	(PREFETCHSIZE) * 8 - 128 + PREOFFSET(X1)
#endif

	mulps	%xmm4, %xmm0
	MOVUPS_XL1 (-16 * SIZE, X1, %xmm4)
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-16 * SIZE, A1, %xmm0)

	mulps	%xmm5, %xmm12
	MOVUPS_XL1 (-12 * SIZE, X1, %xmm5)
	addps	%xmm12, %xmm9
	MOVUPS_A1 (-12 * SIZE, A1, %xmm12)

	addq	$16 * SIZE, A1
	addq	$16 * SIZE, X1

	decq	I
	jg	.L132
	ALIGN_4

.L133:
	mulps	%xmm4, %xmm0
	MOVUPS_XL1 (-24 * SIZE, X1, %xmm4)
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-24 * SIZE, A1, %xmm0)

	mulps	%xmm5, %xmm12
	MOVUPS_XL1 (-20 * SIZE, X1, %xmm5)
	addps	%xmm12, %xmm9
	MOVUPS_A1 (-20 * SIZE, A1, %xmm12)

	mulps	%xmm4, %xmm0
	MOVUPS_XL1 (-16 * SIZE, X1, %xmm4)
	addps	%xmm0, %xmm8

	mulps	%xmm5, %xmm12
	MOVUPS_XL1 (-12 * SIZE, X1, %xmm5)
	addps	%xmm12, %xmm9

	addq	$16 * SIZE, A1
	addq	$16 * SIZE, X1
	ALIGN_4

.L135:
	testq	$8, MM
	jle	.L136

	MOVUPS_A1 (-32 * SIZE, A1, %xmm0)
	mulps	%xmm4, %xmm0
	MOVUPS_XL1 (-24 * SIZE, X1, %xmm4)
	addps	%xmm0, %xmm8

	MOVUPS_A1 (-28 * SIZE, A1, %xmm12)
	mulps	%xmm5, %xmm12
	addps	%xmm12, %xmm9

	addq	$8 * SIZE, A1
	addq	$8 * SIZE, X1
	ALIGN_4

.L136:
	testq	$4, MM
	jle	.L137

	MOVUPS_A1 (-32 * SIZE, A1, %xmm0)
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8

	addq	$4 * SIZE, A1
	addq	$4 * SIZE, X1
	ALIGN_4

.L137:
	testq	$2, MM
	jle	.L138

#ifdef movsd
	xorps	%xmm0, %xmm0
#endif
	movsd	-32 * SIZE(A1), %xmm0
#ifdef movsd
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(X1), %xmm4
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	shufps	$0xe, %xmm4, %xmm4

	addq	$2 * SIZE, A1
	addq	$2 * SIZE, X1
	ALIGN_4

.L138:
	testq	$1, MM
	jle	.L139

	movss	-32 * SIZE(A1), %xmm0
	movss	-32 * SIZE(X1), %xmm4
	mulss	%xmm4, %xmm0
	addss	%xmm0, %xmm8
	ALIGN_4

.L139:
	addps	%xmm9, %xmm8

#ifdef HAVE_SSE3
	haddps	%xmm8, %xmm8
	haddps	%xmm8, %xmm8
#else
	pshufd	$1, %xmm8, %xmm9
	pshufd	$2, %xmm8, %xmm10
	pshufd	$3, %xmm8, %xmm11

	addss	%xmm9, %xmm8
	addss	%xmm11, %xmm10
	addss	%xmm10, %xmm8
#endif

	mulss	ALPHA, %xmm8

	addss	(Y), %xmm8
	movss	%xmm8, (Y1)
	jmp	.L999
	ALIGN_4

.L200:
	testq	$2 * SIZE, LDA
	jne	.L300

	cmpq	$4, N
	jl	.L210
	ALIGN_3

.L201:
	subq	$4, N

	leaq	32 * SIZE(BUFFER), X1

	movq	A, A1
	leaq	(A1, LDA, 2), A2
	leaq	(A1, LDA, 4), A

	xorps	%xmm8, %xmm8
	xorps	%xmm9, %xmm9
	xorps	%xmm10, %xmm10
	xorps	%xmm11, %xmm11

	cmpq	$3, M
	jle	.L207

	testq	$SIZE, A1
	je	.L20X

	movss	-32 * SIZE(A1), %xmm0
	movss	-32 * SIZE(X1), %xmm4
	mulss	%xmm4, %xmm0
	addss	%xmm0, %xmm8
	movss	-32 * SIZE(A1, LDA), %xmm1
	mulss	%xmm4, %xmm1
	addss	%xmm1, %xmm9
	movss	-32 * SIZE(A2), %xmm2
	mulss	%xmm4, %xmm2
	addss	%xmm2, %xmm10
	movss	-32 * SIZE(A2, LDA), %xmm3
	mulss	%xmm4, %xmm3
	addss	%xmm3, %xmm11

	addq	 $1 * SIZE, A1
	addq	 $1 * SIZE, A2
	addq	 $1 * SIZE, X1
	ALIGN_3

.L20X:
	testq	$2 * SIZE, A1
	je	.L20XX

#ifdef movsd
	xorps	%xmm0, %xmm0
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(A1), %xmm0
	movsd	-32 * SIZE(X1), %xmm4
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
#ifdef movsd
	xorps	%xmm1, %xmm1
#endif
	movsd	-32 * SIZE(A1, LDA), %xmm1
	mulps	%xmm4, %xmm1
	addps	%xmm1, %xmm9
#ifdef movsd
	xorps	%xmm2, %xmm2
#endif
	movsd	-32 * SIZE(A2), %xmm2
	mulps	%xmm4, %xmm2
	addps	%xmm2, %xmm10
#ifdef movsd
	xorps	%xmm3, %xmm3
#endif
	movsd	-32 * SIZE(A2, LDA), %xmm3
	mulps	%xmm4, %xmm3
	addps	%xmm3, %xmm11

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, X1
	ALIGN_3

.L20XX:
	movaps	-33 * SIZE(A1, LDA), %xmm12
	movaps	-34 * SIZE(A2), %xmm13
	movaps	-35 * SIZE(A2, LDA), %xmm14

	MOVUPS_XL1 (-32 * SIZE, X1, %xmm4)
	MOVUPS_XL1 (-28 * SIZE, X1, %xmm5)

#ifdef PREFETCHW
	PREFETCHW	4 * SIZE(Y1)
#endif

	movq	MM,  I
	sarq	$4,  I
	jle	.L205

	MOVUPS_A1 (-32 * SIZE, A1, %xmm0)
	MOVUPS_A2 (-29 * SIZE, A1, LDA, 1, %xmm1)
	MOVUPS_A1 (-30 * SIZE, A2, %xmm2)
	MOVUPS_A2 (-31 * SIZE, A2, LDA, 1, %xmm3)

	decq	I
	jle	.L203
	ALIGN_4

.L202:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(A1)
#endif

	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-28 * SIZE, A1, %xmm0)
	movss	 %xmm1,  %xmm12
	shufps	 $0x39,  %xmm12, %xmm12
	mulps	 %xmm4,  %xmm12
	addps	 %xmm12, %xmm9
	MOVUPS_A2 (-25 * SIZE, A1, LDA, 1, %xmm12)

	shufps	 $0x4e, %xmm2, %xmm13
	mulps	 %xmm4, %xmm13
	addps	 %xmm13, %xmm10
	MOVUPS_A1 (-26 * SIZE, A2, %xmm13)
	movss	 %xmm3,  %xmm14
	shufps	 $0x93, %xmm3, %xmm14
	mulps	 %xmm4, %xmm14
	MOVUPS_XL1 (-24 * SIZE, X1, %xmm4)
	addps	 %xmm14, %xmm11
	MOVUPS_A2 (-27 * SIZE, A2, LDA, 1, %xmm14)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(A1, LDA)
#endif

	mulps	%xmm5, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-24 * SIZE, A1, %xmm0)
	movss	 %xmm12,  %xmm1
	shufps	 $0x39,  %xmm1, %xmm1
	mulps	 %xmm5,  %xmm1
	addps	 %xmm1, %xmm9
	MOVUPS_A2 (-21 * SIZE, A1, LDA, 1, %xmm1)

	shufps	 $0x4e, %xmm13, %xmm2
	mulps	 %xmm5, %xmm2
	addps	 %xmm2, %xmm10
	MOVUPS_A1 (-22 * SIZE, A2, %xmm2)
	movss	 %xmm14,  %xmm3
	shufps	 $0x93, %xmm14, %xmm3
	mulps	 %xmm5, %xmm3
	MOVUPS_XL1 (-20 * SIZE, X1, %xmm5)
	addps	 %xmm3, %xmm11
	MOVUPS_A2 (-23 * SIZE, A2, LDA, 1, %xmm3)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(A2)
#endif

	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-20 * SIZE, A1, %xmm0)
	movss	 %xmm1,  %xmm12
	shufps	 $0x39,  %xmm12, %xmm12
	mulps	 %xmm4,  %xmm12
	addps	 %xmm12, %xmm9
	MOVUPS_A2 (-17 * SIZE, A1, LDA, 1, %xmm12)

	shufps	 $0x4e, %xmm2, %xmm13
	mulps	 %xmm4, %xmm13
	addps	 %xmm13, %xmm10
	MOVUPS_A1 (-18 * SIZE, A2, %xmm13)
	movss	 %xmm3,  %xmm14
	shufps	 $0x93, %xmm3, %xmm14
	mulps	 %xmm4, %xmm14
	MOVUPS_XL1 (-16 * SIZE, X1, %xmm4)
	addps	 %xmm14, %xmm11
	MOVUPS_A2 (-19 * SIZE, A2, LDA, 1, %xmm14)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(A2, LDA)
#endif

	mulps	%xmm5, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-16 * SIZE, A1, %xmm0)
	movss	 %xmm12,  %xmm1
	shufps	 $0x39,  %xmm1, %xmm1
	mulps	 %xmm5,  %xmm1
	addps	 %xmm1, %xmm9
	MOVUPS_A2 (-13 * SIZE, A1, LDA, 1, %xmm1)

#ifdef PREFETCHW
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(X1)
#endif

	shufps	 $0x4e, %xmm13, %xmm2
	mulps	 %xmm5, %xmm2
	addps	 %xmm2, %xmm10
	MOVUPS_A1 (-14 * SIZE, A2, %xmm2)
	movss	 %xmm14,  %xmm3
	shufps	 $0x93, %xmm14, %xmm3
	mulps	 %xmm5, %xmm3
	MOVUPS_XL1 (-12 * SIZE, X1, %xmm5)
	addps	 %xmm3, %xmm11
	MOVUPS_A2 (-15 * SIZE, A2, LDA, 1, %xmm3)

	addq	$16 * SIZE, A1
	addq	$16 * SIZE, A2
	addq	$16 * SIZE, X1

	decq	I
	jg	.L202
	ALIGN_4

.L203:
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-28 * SIZE, A1, %xmm0)
	movss	 %xmm1,  %xmm12
	shufps	 $0x39,  %xmm12, %xmm12
	mulps	 %xmm4,  %xmm12
	addps	 %xmm12, %xmm9
	MOVUPS_A2 (-25 * SIZE, A1, LDA, 1, %xmm12)

	shufps	 $0x4e, %xmm2, %xmm13
	mulps	 %xmm4, %xmm13
	addps	 %xmm13, %xmm10
	MOVUPS_A1 (-26 * SIZE, A2, %xmm13)
	movss	 %xmm3,  %xmm14
	shufps	 $0x93, %xmm3, %xmm14
	mulps	 %xmm4, %xmm14
	MOVUPS_XL1 (-24 * SIZE, X1, %xmm4)
	addps	 %xmm14, %xmm11
	MOVUPS_A2 (-27 * SIZE, A2, LDA, 1, %xmm14)

	mulps	%xmm5, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-24 * SIZE, A1, %xmm0)
	movss	 %xmm12,  %xmm1
	shufps	 $0x39,  %xmm1, %xmm1
	mulps	 %xmm5,  %xmm1
	addps	 %xmm1, %xmm9
	MOVUPS_A2 (-21 * SIZE, A1, LDA, 1, %xmm1)

	shufps	 $0x4e, %xmm13, %xmm2
	mulps	 %xmm5, %xmm2
	addps	 %xmm2, %xmm10
	MOVUPS_A1 (-22 * SIZE, A2, %xmm2)
	movss	 %xmm14,  %xmm3
	shufps	 $0x93, %xmm14, %xmm3
	mulps	 %xmm5, %xmm3
	MOVUPS_XL1 (-20 * SIZE, X1, %xmm5)
	addps	 %xmm3, %xmm11
	MOVUPS_A2 (-23 * SIZE, A2, LDA, 1, %xmm3)

	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-20 * SIZE, A1, %xmm0)
	movss	 %xmm1,  %xmm12
	shufps	 $0x39,  %xmm12, %xmm12
	mulps	 %xmm4,  %xmm12
	addps	 %xmm12, %xmm9
	MOVUPS_A2 (-17 * SIZE, A1, LDA, 1, %xmm12)

	shufps	 $0x4e, %xmm2, %xmm13
	mulps	 %xmm4, %xmm13
	addps	 %xmm13, %xmm10
	MOVUPS_A1 (-18 * SIZE, A2, %xmm13)
	movss	 %xmm3,  %xmm14
	shufps	 $0x93, %xmm3, %xmm14
	mulps	 %xmm4, %xmm14
	MOVUPS_XL1 (-16 * SIZE, X1, %xmm4)
	addps	 %xmm14, %xmm11
	MOVUPS_A2 (-19 * SIZE, A2, LDA, 1, %xmm14)

	mulps	%xmm5, %xmm0
	addps	%xmm0, %xmm8
	movss	 %xmm12,  %xmm1
	shufps	 $0x39,  %xmm1, %xmm1
	mulps	 %xmm5,  %xmm1
	addps	 %xmm1, %xmm9

	shufps	 $0x4e, %xmm13, %xmm2
	mulps	 %xmm5, %xmm2
	addps	 %xmm2, %xmm10
	movss	 %xmm14,  %xmm3
	shufps	 $0x93, %xmm14, %xmm3
	mulps	 %xmm5, %xmm3
	MOVUPS_XL1 (-12 * SIZE, X1, %xmm5)
	addps	 %xmm3, %xmm11

	addq	$16 * SIZE, A1
	addq	$16 * SIZE, A2
	addq	$16 * SIZE, X1
	ALIGN_4

.L205:
	testq	$8, MM
	jle	.L206

	MOVUPS_A1 (-32 * SIZE, A1, %xmm0)
	MOVUPS_A2 (-29 * SIZE, A1, LDA, 1, %xmm1)
	MOVUPS_A1 (-30 * SIZE, A2, %xmm2)
	MOVUPS_A2 (-31 * SIZE, A2, LDA, 1, %xmm3)

	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-28 * SIZE, A1, %xmm0)
	movss	 %xmm1,  %xmm12
	shufps	 $0x39,  %xmm12, %xmm12
	mulps	 %xmm4,  %xmm12
	addps	 %xmm12, %xmm9
	MOVUPS_A2 (-25 * SIZE, A1, LDA, 1, %xmm12)

	shufps	 $0x4e, %xmm2, %xmm13
	mulps	 %xmm4, %xmm13
	addps	 %xmm13, %xmm10
	MOVUPS_A1 (-26 * SIZE, A2, %xmm13)
	movss	 %xmm3,  %xmm14
	shufps	 $0x93, %xmm3, %xmm14
	mulps	 %xmm4, %xmm14
	MOVUPS_XL1 (-24 * SIZE, X1, %xmm4)
	addps	 %xmm14, %xmm11
	MOVUPS_A2 (-27 * SIZE, A2, LDA, 1, %xmm14)

	mulps	%xmm5, %xmm0
	addps	%xmm0, %xmm8
	movss	 %xmm12,  %xmm1
	shufps	 $0x39,  %xmm1, %xmm1
	mulps	 %xmm5,  %xmm1
	addps	 %xmm1, %xmm9

	shufps	 $0x4e, %xmm13, %xmm2
	mulps	 %xmm5, %xmm2
	addps	 %xmm2, %xmm10
	movss	 %xmm14,  %xmm3
	shufps	 $0x93, %xmm14, %xmm3
	mulps	 %xmm5, %xmm3
	addps	 %xmm3, %xmm11

	addq	$8 * SIZE, A1
	addq	$8 * SIZE, A2
	addq	$8 * SIZE, X1
	ALIGN_4

.L206:
	testq	$4, MM
	jle	.L207

	MOVUPS_A1 (-32 * SIZE, A1, %xmm0)
	MOVUPS_A2 (-29 * SIZE, A1, LDA, 1, %xmm1)
	MOVUPS_A1 (-30 * SIZE, A2, %xmm2)
	MOVUPS_A2 (-31 * SIZE, A2, LDA, 1, %xmm3)

	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	movss	 %xmm1,  %xmm12
	shufps	 $0x39,  %xmm12, %xmm12
	mulps	 %xmm4,  %xmm12
	addps	 %xmm12, %xmm9

	shufps	 $0x4e, %xmm2, %xmm13
	mulps	 %xmm4, %xmm13
	addps	 %xmm13, %xmm10
	movss	 %xmm3,  %xmm14
	shufps	 $0x93, %xmm3, %xmm14
	mulps	 %xmm4, %xmm14
	addps	 %xmm14, %xmm11

	addq	$4 * SIZE, A1
	addq	$4 * SIZE, A2
	addq	$4 * SIZE, X1
	ALIGN_4

.L207:
	testq	$2, MM
	jle	.L208

#ifdef movsd
	xorps	%xmm0, %xmm0
#endif
	movsd	-32 * SIZE(A1), %xmm0
#ifdef movsd
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(X1), %xmm4
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
#ifdef movsd
	xorps	%xmm1, %xmm1
#endif
	movsd	-32 * SIZE(A1, LDA), %xmm1
	mulps	%xmm4, %xmm1
	addps	%xmm1, %xmm9
#ifdef movsd
	xorps	%xmm2, %xmm2
#endif
	movsd	-32 * SIZE(A2), %xmm2
	mulps	%xmm4, %xmm2
	addps	%xmm2, %xmm10
#ifdef movsd
	xorps	%xmm3, %xmm3
#endif
	movsd	-32 * SIZE(A2, LDA), %xmm3
	mulps	%xmm4, %xmm3
	addps	%xmm3, %xmm11
	shufps	$0xe, %xmm4, %xmm4

	addq	$2 * SIZE, A1
	addq	$2 * SIZE, A2
	addq	$2 * SIZE, X1
	ALIGN_4

.L208:
	testq	$1, MM
	jle	.L209

	movss	-32 * SIZE(A1), %xmm0
	movss	-32 * SIZE(X1), %xmm4
	mulss	%xmm4, %xmm0
	addss	%xmm0, %xmm8
	movss	-32 * SIZE(A1, LDA), %xmm1
	mulss	%xmm4, %xmm1
	addss	%xmm1, %xmm9
	movss	-32 * SIZE(A2), %xmm2
	mulss	%xmm4, %xmm2
	addss	%xmm2, %xmm10
	movss	-32 * SIZE(A2, LDA), %xmm3
	mulss	%xmm4, %xmm3
	addss	%xmm3, %xmm11
	ALIGN_4

.L209:
#ifdef HAVE_SSE3
	haddps	%xmm9, %xmm8
	haddps	%xmm11, %xmm10
	haddps	%xmm10, %xmm8

	pshufd	$0x1, %xmm8, %xmm9
	pshufd	$0x2, %xmm8, %xmm10
	pshufd	$0x3, %xmm8, %xmm11
#else
	movaps	%xmm8, %xmm0
	unpcklps %xmm9, %xmm8
	unpckhps %xmm9, %xmm0

	movaps	%xmm10, %xmm1
	unpcklps %xmm11, %xmm10
	unpckhps %xmm11, %xmm1

	movaps	%xmm8, %xmm9
	unpcklps %xmm10, %xmm8
	unpckhps %xmm10, %xmm9

	movaps	%xmm0, %xmm10
	unpcklps %xmm1, %xmm0
	unpckhps %xmm1, %xmm10

	addps	%xmm9, %xmm8
	addps	%xmm0, %xmm10
	addps	%xmm10, %xmm8

	pshufd	$0x2, %xmm8, %xmm9
	pshufd	$0x1, %xmm8, %xmm10
	pshufd	$0x3, %xmm8, %xmm11
#endif

	mulss	ALPHA, %xmm8
	mulss	ALPHA, %xmm9
	mulss	ALPHA, %xmm10
	mulss	ALPHA, %xmm11

	addss	(Y), %xmm8
	addq	INCY, Y
	addss	(Y), %xmm9
	addq	INCY, Y
	addss	(Y), %xmm10
	addq	INCY, Y
	addss	(Y), %xmm11
	addq	INCY, Y

	movss	%xmm8, (Y1)
	addq	INCY, Y1
	movss	%xmm9, (Y1)
	addq	INCY, Y1
	movss	%xmm10, (Y1)
	addq	INCY, Y1
	movss	%xmm11, (Y1)
	addq	INCY, Y1

	cmpq	$4, N
	jge	.L201
	ALIGN_4

.L210:
	cmpq	$3, N
	jne	.L220

	leaq	32 * SIZE(BUFFER), X1

	movq	A, A1
	leaq	(A1, LDA, 2), A2
	leaq	(A1, LDA, 4), A

	xorps	%xmm8, %xmm8
	xorps	%xmm9, %xmm9
	xorps	%xmm10, %xmm10

	cmpq	$3, M
	jle	.L217

	testq	$SIZE, A1
	je	.L21X

	movss	-32 * SIZE(A1), %xmm0
	movss	-32 * SIZE(X1), %xmm4
	mulss	%xmm4, %xmm0
	addss	%xmm0, %xmm8
	movss	-32 * SIZE(A1, LDA), %xmm1
	mulss	%xmm4, %xmm1
	addss	%xmm1, %xmm9
	movss	-32 * SIZE(A2), %xmm2
	mulss	%xmm4, %xmm2
	addss	%xmm2, %xmm10

	addq	 $1 * SIZE, A1
	addq	 $1 * SIZE, A2
	addq	 $1 * SIZE, X1
	ALIGN_3

.L21X:
	testq	$2 * SIZE, A1
	je	.L21XX

#ifdef movsd
	xorps	%xmm0, %xmm0
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(A1), %xmm0
	movsd	-32 * SIZE(X1), %xmm4
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
#ifdef movsd
	xorps	%xmm1, %xmm1
#endif
	movsd	-32 * SIZE(A1, LDA), %xmm1
	mulps	%xmm4, %xmm1
	addps	%xmm1, %xmm9
#ifdef movsd
	xorps	%xmm2, %xmm2
#endif
	movsd	-32 * SIZE(A2), %xmm2
	mulps	%xmm4, %xmm2
	addps	%xmm2, %xmm10

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, X1
	ALIGN_3

.L21XX:
	movaps	-33 * SIZE(A1, LDA), %xmm12
	movaps	-34 * SIZE(A2), %xmm13

	MOVUPS_XL1 (-32 * SIZE, X1, %xmm4)
	MOVUPS_XL1 (-28 * SIZE, X1, %xmm5)

#ifdef PREFETCHW
	PREFETCHW	4 * SIZE(Y1)
#endif

	movq	MM,  I
	sarq	$4,  I
	jle	.L215

	MOVUPS_A1 (-32 * SIZE, A1, %xmm0)
	MOVUPS_A2 (-29 * SIZE, A1, LDA, 1, %xmm1)
	MOVUPS_A1 (-30 * SIZE, A2, %xmm2)

	decq	I
	jle	.L213
	ALIGN_4

.L212:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 3 - 128 + PREOFFSET(A1)
#endif

	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-28 * SIZE, A1, %xmm0)
	movss	 %xmm1,  %xmm12
	shufps	 $0x39,  %xmm12, %xmm12
	mulps	 %xmm4,  %xmm12
	addps	 %xmm12, %xmm9
	MOVUPS_A2 (-25 * SIZE, A1, LDA, 1, %xmm12)

	shufps	 $0x4e, %xmm2, %xmm13
	mulps	 %xmm4, %xmm13
	addps	 %xmm13, %xmm10
	MOVUPS_A1 (-26 * SIZE, A2, %xmm13)
	MOVUPS_XL1 (-24 * SIZE, X1, %xmm4)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 3 - 128 + PREOFFSET(A1, LDA)
#endif

	mulps	%xmm5, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-24 * SIZE, A1, %xmm0)
	movss	 %xmm12,  %xmm1
	shufps	 $0x39,  %xmm1, %xmm1
	mulps	 %xmm5,  %xmm1
	addps	 %xmm1, %xmm9
	MOVUPS_A2 (-21 * SIZE, A1, LDA, 1, %xmm1)

	shufps	 $0x4e, %xmm13, %xmm2
	mulps	 %xmm5, %xmm2
	addps	 %xmm2, %xmm10
	MOVUPS_A1 (-22 * SIZE, A2, %xmm2)
	MOVUPS_XL1 (-20 * SIZE, X1, %xmm5)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 3 - 128 + PREOFFSET(A2)
#endif

	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-20 * SIZE, A1, %xmm0)
	movss	 %xmm1,  %xmm12
	shufps	 $0x39,  %xmm12, %xmm12
	mulps	 %xmm4,  %xmm12
	addps	 %xmm12, %xmm9
	MOVUPS_A2 (-17 * SIZE, A1, LDA, 1, %xmm12)
	shufps	 $0x4e, %xmm2, %xmm13
	mulps	 %xmm4, %xmm13
	MOVUPS_XL1 (-16 * SIZE, X1, %xmm4)
	addps	 %xmm13, %xmm10
	MOVUPS_A1 (-18 * SIZE, A2, %xmm13)

#ifdef PREFETCHW
	PREFETCH	(PREFETCHSIZE) * 3 - 128 + PREOFFSET(X1)
#endif

	mulps	%xmm5, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-16 * SIZE, A1, %xmm0)
	movss	 %xmm12,  %xmm1
	shufps	 $0x39,  %xmm1, %xmm1
	mulps	 %xmm5,  %xmm1
	addps	 %xmm1, %xmm9
	MOVUPS_A2 (-13 * SIZE, A1, LDA, 1, %xmm1)
	shufps	 $0x4e, %xmm13, %xmm2
	mulps	 %xmm5, %xmm2
	MOVUPS_XL1 (-12 * SIZE, X1, %xmm5)
	addps	 %xmm2, %xmm10
	MOVUPS_A1 (-14 * SIZE, A2, %xmm2)

	addq	$16 * SIZE, A1
	addq	$16 * SIZE, A2
	addq	$16 * SIZE, X1

	decq	I
	jg	.L212
	ALIGN_4

.L213:
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-28 * SIZE, A1, %xmm0)
	movss	 %xmm1,  %xmm12
	shufps	 $0x39,  %xmm12, %xmm12
	mulps	 %xmm4,  %xmm12
	addps	 %xmm12, %xmm9
	MOVUPS_A2 (-25 * SIZE, A1, LDA, 1, %xmm12)

	shufps	 $0x4e, %xmm2, %xmm13
	mulps	 %xmm4, %xmm13
	addps	 %xmm13, %xmm10
	MOVUPS_A1 (-26 * SIZE, A2, %xmm13)
	movss	 %xmm3,  %xmm14
	shufps	 $0x93, %xmm3, %xmm14
	mulps	 %xmm4, %xmm14
	MOVUPS_XL1 (-24 * SIZE, X1, %xmm4)
	addps	 %xmm14, %xmm11
	MOVUPS_A2 (-27 * SIZE, A2, LDA, 1, %xmm14)

	mulps	%xmm5, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-24 * SIZE, A1, %xmm0)
	movss	 %xmm12,  %xmm1
	shufps	 $0x39,  %xmm1, %xmm1
	mulps	 %xmm5,  %xmm1
	addps	 %xmm1, %xmm9
	MOVUPS_A2 (-21 * SIZE, A1, LDA, 1, %xmm1)

	shufps	 $0x4e, %xmm13, %xmm2
	mulps	 %xmm5, %xmm2
	addps	 %xmm2, %xmm10
	MOVUPS_A1 (-22 * SIZE, A2, %xmm2)
	movss	 %xmm14,  %xmm3
	shufps	 $0x93, %xmm14, %xmm3
	mulps	 %xmm5, %xmm3
	MOVUPS_XL1 (-20 * SIZE, X1, %xmm5)
	addps	 %xmm3, %xmm11
	MOVUPS_A2 (-23 * SIZE, A2, LDA, 1, %xmm3)

	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-20 * SIZE, A1, %xmm0)
	movss	 %xmm1,  %xmm12
	shufps	 $0x39,  %xmm12, %xmm12
	mulps	 %xmm4,  %xmm12
	addps	 %xmm12, %xmm9
	MOVUPS_A2 (-17 * SIZE, A1, LDA, 1, %xmm12)

	shufps	 $0x4e, %xmm2, %xmm13
	mulps	 %xmm4, %xmm13
	addps	 %xmm13, %xmm10
	MOVUPS_A1 (-18 * SIZE, A2, %xmm13)
	movss	 %xmm3,  %xmm14
	shufps	 $0x93, %xmm3, %xmm14
	mulps	 %xmm4, %xmm14
	MOVUPS_XL1 (-16 * SIZE, X1, %xmm4)
	addps	 %xmm14, %xmm11
	MOVUPS_A2 (-19 * SIZE, A2, LDA, 1, %xmm14)

	mulps	%xmm5, %xmm0
	addps	%xmm0, %xmm8
	movss	 %xmm12,  %xmm1
	shufps	 $0x39,  %xmm1, %xmm1
	mulps	 %xmm5,  %xmm1
	addps	 %xmm1, %xmm9

	shufps	 $0x4e, %xmm13, %xmm2
	mulps	 %xmm5, %xmm2
	addps	 %xmm2, %xmm10
	movss	 %xmm14,  %xmm3
	shufps	 $0x93, %xmm14, %xmm3
	mulps	 %xmm5, %xmm3
	MOVUPS_XL1 (-12 * SIZE, X1, %xmm5)
	addps	 %xmm3, %xmm11

	addq	$16 * SIZE, A1
	addq	$16 * SIZE, A2
	addq	$16 * SIZE, X1
	ALIGN_4

.L215:
	testq	$8, MM
	jle	.L216

	MOVUPS_A1 (-32 * SIZE, A1, %xmm0)
	MOVUPS_A2 (-29 * SIZE, A1, LDA, 1, %xmm1)
	MOVUPS_A1 (-30 * SIZE, A2, %xmm2)

	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-28 * SIZE, A1, %xmm0)
	movss	 %xmm1,  %xmm12
	shufps	 $0x39,  %xmm12, %xmm12
	mulps	 %xmm4,  %xmm12
	addps	 %xmm12, %xmm9
	MOVUPS_A2 (-25 * SIZE, A1, LDA, 1, %xmm12)

	shufps	 $0x4e, %xmm2, %xmm13
	mulps	 %xmm4, %xmm13
	MOVUPS_XL1 (-24 * SIZE, X1, %xmm4)
	addps	 %xmm13, %xmm10
	MOVUPS_A1 (-26 * SIZE, A2, %xmm13)

	mulps	%xmm5, %xmm0
	addps	%xmm0, %xmm8
	movss	 %xmm12,  %xmm1
	shufps	 $0x39,  %xmm1, %xmm1
	mulps	 %xmm5,  %xmm1
	addps	 %xmm1, %xmm9

	shufps	 $0x4e, %xmm13, %xmm2
	mulps	 %xmm5, %xmm2
	addps	 %xmm2, %xmm10

	addq	$8 * SIZE, A1
	addq	$8 * SIZE, A2
	addq	$8 * SIZE, X1
	ALIGN_4

.L216:
	testq	$4, MM
	jle	.L217

	MOVUPS_A1 (-32 * SIZE, A1, %xmm0)
	MOVUPS_A2 (-29 * SIZE, A1, LDA, 1, %xmm1)
	MOVUPS_A1 (-30 * SIZE, A2, %xmm2)

	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	movss	 %xmm1,  %xmm12
	shufps	 $0x39,  %xmm12, %xmm12
	mulps	 %xmm4,  %xmm12
	addps	 %xmm12, %xmm9

	shufps	 $0x4e, %xmm2, %xmm13
	mulps	 %xmm4, %xmm13
	addps	 %xmm13, %xmm10

	addq	$4 * SIZE, A1
	addq	$4 * SIZE, A2
	addq	$4 * SIZE, X1
	ALIGN_4

.L217:
	testq	$2, MM
	jle	.L218

#ifdef movsd
	xorps	%xmm0, %xmm0
#endif
	movsd	-32 * SIZE(A1), %xmm0
#ifdef movsd
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(X1), %xmm4
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
#ifdef movsd
	xorps	%xmm1, %xmm1
#endif
	movsd	-32 * SIZE(A1, LDA), %xmm1
	mulps	%xmm4, %xmm1
	addps	%xmm1, %xmm9
#ifdef movsd
	xorps	%xmm2, %xmm2
#endif
	movsd	-32 * SIZE(A2), %xmm2
	mulps	%xmm4, %xmm2
	addps	%xmm2, %xmm10

	addq	$2 * SIZE, A1
	addq	$2 * SIZE, A2
	addq	$2 * SIZE, X1
	ALIGN_4

.L218:
	testq	$1, MM
	jle	.L219

	movss	-32 * SIZE(A1), %xmm0
	movss	-32 * SIZE(X1), %xmm4
	mulss	%xmm4, %xmm0
	addss	%xmm0, %xmm8
	movss	-32 * SIZE(A1, LDA), %xmm1
	mulss	%xmm4, %xmm1
	addss	%xmm1, %xmm9
	movss	-32 * SIZE(A2), %xmm2
	mulss	%xmm4, %xmm2
	addss	%xmm2, %xmm10
	ALIGN_4

.L219:
#ifdef HAVE_SSE3
	haddps	%xmm9, %xmm8
	haddps	%xmm11, %xmm10
	haddps	%xmm10, %xmm8

	pshufd	$0x1, %xmm8, %xmm9
	pshufd	$0x2, %xmm8, %xmm10
#else
	movaps	%xmm8, %xmm0
	unpcklps %xmm9, %xmm8
	unpckhps %xmm9, %xmm0

	movaps	%xmm10, %xmm1
	unpcklps %xmm11, %xmm10
	unpckhps %xmm11, %xmm1

	movaps	%xmm8, %xmm9
	unpcklps %xmm10, %xmm8
	unpckhps %xmm10, %xmm9

	movaps	%xmm0, %xmm10
	unpcklps %xmm1, %xmm0
	unpckhps %xmm1, %xmm10

	addps	%xmm9, %xmm8
	addps	%xmm0, %xmm10
	addps	%xmm10, %xmm8

	pshufd	$0x2, %xmm8, %xmm9
	pshufd	$0x1, %xmm8, %xmm10
#endif

	mulss	ALPHA, %xmm8
	mulss	ALPHA, %xmm9
	mulss	ALPHA, %xmm10

	addss	(Y), %xmm8
	addq	INCY, Y
	addss	(Y), %xmm9
	addq	INCY, Y
	addss	(Y), %xmm10

	movss	%xmm8, (Y1)
	addq	INCY, Y1
	movss	%xmm9, (Y1)
	addq	INCY, Y1
	movss	%xmm10, (Y1)
	jmp	.L999
	ALIGN_4

.L220:
	testq	N, N
	jle	.L999

	cmpq	$2, N
	jne	.L230

	leaq	32 * SIZE(BUFFER), X1

	movq	A, A1
	leaq	(A1, LDA), A2
	leaq	(A1, LDA, 2), A

	xorps	%xmm8, %xmm8
	xorps	%xmm9, %xmm9

	cmpq	$3, M
	jle	.L227

	testq	$SIZE, A1
	je	.L22X

	movss	-32 * SIZE(A1), %xmm0
	movss	-32 * SIZE(X1), %xmm4
	mulss	%xmm4, %xmm0
	addss	%xmm0, %xmm8
	movss	-32 * SIZE(A2), %xmm1
	mulss	%xmm4, %xmm1
	addss	%xmm1, %xmm9

	addq	 $1 * SIZE, A1
	addq	 $1 * SIZE, A2
	addq	 $1 * SIZE, X1
	ALIGN_3

.L22X:
	testq	$2 * SIZE, A1
	je	.L22XX

#ifdef movsd
	xorps	%xmm0, %xmm0
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(A1), %xmm0
	movsd	-32 * SIZE(X1), %xmm4
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
#ifdef movsd
	xorps	%xmm1, %xmm1
#endif
	movsd	-32 * SIZE(A2), %xmm1
	mulps	%xmm4, %xmm1
	addps	%xmm1, %xmm9

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, X1
	ALIGN_3

.L22XX:
	movaps	-33 * SIZE(A2), %xmm12

	MOVUPS_XL1 (-32 * SIZE, X1, %xmm4)
	MOVUPS_XL1 (-28 * SIZE, X1, %xmm5)

	movq	MM,  I
	sarq	$4,  I
	jle	.L225

	MOVUPS_A1 (-32 * SIZE, A1, %xmm0)
	MOVUPS_A1 (-29 * SIZE, A2, %xmm1)

	decq	I
	jle	.L223
	ALIGN_4

.L222:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 4 - 128 + PREOFFSET(A1)
#endif

	mulps	%xmm4,  %xmm0
	addps	%xmm0,  %xmm8
	MOVUPS_A1 (-28 * SIZE, A1, %xmm2)
	movss	%xmm1,  %xmm12
	shufps	$0x39,  %xmm12, %xmm12
	mulps	%xmm4,  %xmm12
	MOVUPS_XL1 (-24 * SIZE, X1, %xmm4)
	addps	%xmm12, %xmm9
	MOVUPS_A1 (-25 * SIZE, A2, %xmm12)

	mulps	%xmm5,  %xmm2
	addps	%xmm2,  %xmm8
	MOVUPS_A1 (-24 * SIZE, A1, %xmm0)
	movss	%xmm12, %xmm1
	shufps	$0x39,  %xmm1, %xmm1
	mulps	%xmm5,  %xmm1
	MOVUPS_XL1 (-20 * SIZE, X1, %xmm5)
	addps	%xmm1,  %xmm9
	MOVUPS_A1 (-21 * SIZE, A2, %xmm1)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 4 - 128 + PREOFFSET(A2)
#endif

	mulps	%xmm4,  %xmm0
	addps	%xmm0,  %xmm8
	MOVUPS_A1 (-20 * SIZE, A1, %xmm2)
	movss	%xmm1,  %xmm12
	shufps	$0x39,  %xmm12, %xmm12
	mulps	%xmm4,  %xmm12
	MOVUPS_XL1 (-16 * SIZE, X1, %xmm4)
	addps	%xmm12, %xmm9
	MOVUPS_A1 (-17 * SIZE, A2, %xmm12)

#ifdef PREFETCHW
	PREFETCH	(PREFETCHSIZE) * 4 - 128 + PREOFFSET(X1)
#endif

	mulps	%xmm5,  %xmm2
	addps	%xmm2,  %xmm8
	MOVUPS_A1 (-16 * SIZE, A1, %xmm0)
	movss	%xmm12, %xmm1
	shufps	$0x39,  %xmm1, %xmm1
	mulps	%xmm5,  %xmm1
	MOVUPS_XL1 (-12 * SIZE, X1, %xmm5)
	addps	%xmm1,  %xmm9
	MOVUPS_A1 (-13 * SIZE, A2, %xmm1)

	addq	$16 * SIZE, A1
	addq	$16 * SIZE, A2
	addq	$16 * SIZE, X1

	decq	I
	jg	.L222
	ALIGN_4

.L223:
	mulps	%xmm4,  %xmm0
	addps	%xmm0,  %xmm8
	MOVUPS_A1 (-28 * SIZE, A1, %xmm2)
	movss	%xmm1,  %xmm12
	shufps	$0x39,  %xmm12, %xmm12
	mulps	%xmm4,  %xmm12
	MOVUPS_XL1 (-24 * SIZE, X1, %xmm4)
	addps	%xmm12, %xmm9
	MOVUPS_A1 (-25 * SIZE, A2, %xmm12)

	mulps	%xmm5,  %xmm2
	addps	%xmm2,  %xmm8
	MOVUPS_A1 (-24 * SIZE, A1, %xmm0)
	movss	%xmm12, %xmm1
	shufps	$0x39,  %xmm1, %xmm1
	mulps	%xmm5,  %xmm1
	MOVUPS_XL1 (-20 * SIZE, X1, %xmm5)
	addps	%xmm1,  %xmm9
	MOVUPS_A1 (-21 * SIZE, A2, %xmm1)

	mulps	%xmm4,  %xmm0
	addps	%xmm0,  %xmm8
	MOVUPS_A1 (-20 * SIZE, A1, %xmm2)
	movss	%xmm1,  %xmm12
	shufps	$0x39,  %xmm12, %xmm12
	mulps	%xmm4,  %xmm12
	MOVUPS_XL1 (-16 * SIZE, X1, %xmm4)
	addps	%xmm12, %xmm9
	MOVUPS_A1 (-17 * SIZE, A2, %xmm12)

	mulps	%xmm5,  %xmm2
	addps	%xmm2,  %xmm8
	movss	%xmm12, %xmm1
	shufps	$0x39,  %xmm1, %xmm1
	mulps	%xmm5,  %xmm1
	MOVUPS_XL1 (-12 * SIZE, X1, %xmm5)
	addps	%xmm1,  %xmm9

	addq	$16 * SIZE, A1
	addq	$16 * SIZE, A2
	addq	$16 * SIZE, X1
	ALIGN_4

.L225:
	testq	$8, MM
	jle	.L226

	MOVUPS_A1 (-32 * SIZE, A1, %xmm0)
	MOVUPS_A1 (-29 * SIZE, A2, %xmm1)

	mulps	%xmm4,  %xmm0
	addps	%xmm0,  %xmm8
	MOVUPS_A1 (-28 * SIZE, A1, %xmm2)
	movss	%xmm1,  %xmm12
	shufps	$0x39,  %xmm12, %xmm12
	mulps	%xmm4,  %xmm12
	MOVUPS_XL1 (-24 * SIZE, X1, %xmm4)
	addps	%xmm12, %xmm9
	MOVUPS_A1 (-25 * SIZE, A2, %xmm12)

	mulps	%xmm5,  %xmm2
	addps	%xmm2,  %xmm8
	movss	%xmm12, %xmm1
	shufps	$0x39,  %xmm1, %xmm1
	mulps	%xmm5,  %xmm1
	addps	%xmm1,  %xmm9

	addq	$8 * SIZE, A1
	addq	$8 * SIZE, A2
	addq	$8 * SIZE, X1
	ALIGN_4

.L226:
	testq	$4, MM
	jle	.L227

	MOVUPS_A1 (-32 * SIZE, A1, %xmm0)
	MOVUPS_A1 (-29 * SIZE, A2, %xmm1)

	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8

	movss	 %xmm1,  %xmm12
	shufps	 $0x39,  %xmm12, %xmm12
	mulps	 %xmm4,  %xmm12
	addps	 %xmm12, %xmm9

	addq	$4 * SIZE, A1
	addq	$4 * SIZE, A2
	addq	$4 * SIZE, X1
	ALIGN_4

.L227:
	testq	$2, MM
	jle	.L228

#ifdef movsd
	xorps	%xmm0, %xmm0
#endif
	movsd	-32 * SIZE(A1), %xmm0
#ifdef movsd
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(X1), %xmm4
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
#ifdef movsd
	xorps	%xmm1, %xmm1
#endif
	movsd	-32 * SIZE(A2), %xmm1
	mulps	%xmm4, %xmm1
	addps	%xmm1, %xmm9
	shufps	$0xe, %xmm4, %xmm4

	addq	$2 * SIZE, A1
	addq	$2 * SIZE, A2
	addq	$2 * SIZE, X1
	ALIGN_4

.L228:
	testq	$1, MM
	jle	.L229

	movss	-32 * SIZE(A1), %xmm0
	movss	-32 * SIZE(X1), %xmm4
	mulss	%xmm4, %xmm0
	addss	%xmm0, %xmm8
	movss	-32 * SIZE(A2), %xmm1
	mulss	%xmm4, %xmm1
	addss	%xmm1, %xmm9
	ALIGN_4

.L229:
#ifdef HAVE_SSE3
	haddps	%xmm9, %xmm8
	haddps	%xmm8, %xmm8
#else
	movaps	%xmm8, %xmm10
	unpcklps %xmm9, %xmm8
	unpckhps %xmm9, %xmm10

	addps	%xmm10, %xmm8
	movhlps %xmm8, %xmm9
	addps	%xmm9, %xmm8
#endif

	pshufd	$0x1, %xmm8, %xmm9

	mulss	ALPHA, %xmm8
	mulss	ALPHA, %xmm9

	addss	(Y), %xmm8
	addq	INCY, Y
	addss	(Y), %xmm9
	addq	INCY, Y

	movss	%xmm8, (Y1)
	addq	INCY, Y1
	movss	%xmm9, (Y1)
	addq	INCY, Y1
	jmp	.L999
	ALIGN_4

.L230:
	cmpq	$1, N
	jne	.L999

	leaq	32 * SIZE(BUFFER), X1

	movq	A, A1

	xorps	%xmm8, %xmm8
	xorps	%xmm9, %xmm9

	cmpq	$3, M
	jle	.L237

	testq	$SIZE, A1
	je	.L23X

	movss	-32 * SIZE(A1), %xmm0
	movss	-32 * SIZE(X1), %xmm4
	mulss	%xmm4, %xmm0
	addss	%xmm0, %xmm8

	addq	 $1 * SIZE, A1
	addq	 $1 * SIZE, X1
	ALIGN_3

.L23X:
	testq	$2 * SIZE, A1
	je	.L23XX

#ifdef movsd
	xorps	%xmm0, %xmm0
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(A1), %xmm0
	movsd	-32 * SIZE(X1), %xmm4
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	shufps	$0xe, %xmm4, %xmm4

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, X1
	ALIGN_3

.L23XX:
	MOVUPS_XL1 (-32 * SIZE, X1, %xmm4)
	MOVUPS_XL1 (-28 * SIZE, X1, %xmm5)


	movq	MM,  I
	sarq	$4,  I
	jle	.L235

	MOVUPS_A1 (-32 * SIZE, A1, %xmm0)
	MOVUPS_A1 (-28 * SIZE, A1, %xmm12)

	decq	I
	jle	.L233
	ALIGN_4

.L232:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 8 - 128 + PREOFFSET(A1)
#endif

	mulps	%xmm4, %xmm0
	MOVUPS_XL1 (-24 * SIZE, X1, %xmm4)
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-24 * SIZE, A1, %xmm0)

	mulps	%xmm5, %xmm12
	MOVUPS_XL1 (-20 * SIZE, X1, %xmm5)
	addps	%xmm12, %xmm9
	MOVUPS_A1 (-20 * SIZE, A1, %xmm12)

#ifdef PREFETCHW
	PREFETCH	(PREFETCHSIZE) * 8 - 128 + PREOFFSET(X1)
#endif

	mulps	%xmm4, %xmm0
	MOVUPS_XL1 (-16 * SIZE, X1, %xmm4)
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-16 * SIZE, A1, %xmm0)

	mulps	%xmm5, %xmm12
	MOVUPS_XL1 (-12 * SIZE, X1, %xmm5)
	addps	%xmm12, %xmm9
	MOVUPS_A1 (-12 * SIZE, A1, %xmm12)

	addq	$16 * SIZE, A1
	addq	$16 * SIZE, X1

	decq	I
	jg	.L232
	ALIGN_4

.L233:
	mulps	%xmm4, %xmm0
	MOVUPS_XL1 (-24 * SIZE, X1, %xmm4)
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-24 * SIZE, A1, %xmm0)

	mulps	%xmm5, %xmm12
	MOVUPS_XL1 (-20 * SIZE, X1, %xmm5)
	addps	%xmm12, %xmm9
	MOVUPS_A1 (-20 * SIZE, A1, %xmm12)

	mulps	%xmm4, %xmm0
	MOVUPS_XL1 (-16 * SIZE, X1, %xmm4)
	addps	%xmm0, %xmm8

	mulps	%xmm5, %xmm12
	MOVUPS_XL1 (-12 * SIZE, X1, %xmm5)
	addps	%xmm12, %xmm9

	addq	$16 * SIZE, A1
	addq	$16 * SIZE, X1
	ALIGN_4

.L235:
	testq	$8, MM
	jle	.L236

	MOVUPS_A1 (-32 * SIZE, A1, %xmm0)
	mulps	%xmm4, %xmm0
	MOVUPS_XL1 (-24 * SIZE, X1, %xmm4)
	addps	%xmm0, %xmm8

	MOVUPS_A1 (-28 * SIZE, A1, %xmm12)
	mulps	%xmm5, %xmm12
	addps	%xmm12, %xmm9

	addq	$8 * SIZE, A1
	addq	$8 * SIZE, X1
	ALIGN_4

.L236:
	testq	$4, MM
	jle	.L237

	MOVUPS_A1 (-32 * SIZE, A1, %xmm0)
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8

	addq	$4 * SIZE, A1
	addq	$4 * SIZE, X1
	ALIGN_4

.L237:
	testq	$2, MM
	jle	.L238

#ifdef movsd
	xorps	%xmm0, %xmm0
#endif
	movsd	-32 * SIZE(A1), %xmm0
#ifdef movsd
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(X1), %xmm4
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	shufps	$0xe, %xmm4, %xmm4

	addq	$2 * SIZE, A1
	addq	$2 * SIZE, X1
	ALIGN_4

.L238:
	testq	$1, MM
	jle	.L239

	movss	-32 * SIZE(A1), %xmm0
	movss	-32 * SIZE(X1), %xmm4
	mulss	%xmm4, %xmm0
	addss	%xmm0, %xmm8
	ALIGN_4

.L239:
	addps	%xmm9, %xmm8

#ifdef HAVE_SSE3
	haddps	%xmm8, %xmm8
	haddps	%xmm8, %xmm8
#else
	pshufd	$1, %xmm8, %xmm9
	pshufd	$2, %xmm8, %xmm10
	pshufd	$3, %xmm8, %xmm11

	addss	%xmm9, %xmm8
	addss	%xmm11, %xmm10
	addss	%xmm10, %xmm8
#endif

	mulss	ALPHA, %xmm8

	addss	(Y), %xmm8
	movss	%xmm8, (Y1)
	jmp	.L999
	ALIGN_4

.L300:
	cmpq	$4, N
	jl	.L310
	ALIGN_3

.L301:
	subq	$4, N

	leaq	32 * SIZE(BUFFER), X1

	movq	A, A1
	leaq	(A1, LDA, 2), A2
	leaq	(A1, LDA, 4), A

	xorps	%xmm8, %xmm8
	xorps	%xmm9, %xmm9
	xorps	%xmm10, %xmm10
	xorps	%xmm11, %xmm11

	cmpq	$3, M
	jle	.L307

	testq	$SIZE, A1
	je	.L30X

	movss	-32 * SIZE(A1), %xmm0
	movss	-32 * SIZE(X1), %xmm4
	mulss	%xmm4, %xmm0
	addss	%xmm0, %xmm8
	movss	-32 * SIZE(A1, LDA), %xmm1
	mulss	%xmm4, %xmm1
	addss	%xmm1, %xmm9
	movss	-32 * SIZE(A2), %xmm2
	mulss	%xmm4, %xmm2
	addss	%xmm2, %xmm10
	movss	-32 * SIZE(A2, LDA), %xmm3
	mulss	%xmm4, %xmm3
	addss	%xmm3, %xmm11

	addq	 $1 * SIZE, A1
	addq	 $1 * SIZE, A2
	addq	 $1 * SIZE, X1
	ALIGN_3

.L30X:
	testq	$2 * SIZE, A1
	je	.L30XX

#ifdef movsd
	xorps	%xmm0, %xmm0
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(A1), %xmm0
	movsd	-32 * SIZE(X1), %xmm4
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
#ifdef movsd
	xorps	%xmm1, %xmm1
#endif
	movsd	-32 * SIZE(A1, LDA), %xmm1
	mulps	%xmm4, %xmm1
	addps	%xmm1, %xmm9
#ifdef movsd
	xorps	%xmm2, %xmm2
#endif
	movsd	-32 * SIZE(A2), %xmm2
	mulps	%xmm4, %xmm2
	addps	%xmm2, %xmm10
#ifdef movsd
	xorps	%xmm3, %xmm3
#endif
	movsd	-32 * SIZE(A2, LDA), %xmm3
	mulps	%xmm4, %xmm3
	addps	%xmm3, %xmm11

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, X1
	ALIGN_3

.L30XX:
	movaps	-35 * SIZE(A1, LDA), %xmm12
	movaps	-34 * SIZE(A2), %xmm13
	movaps	-33 * SIZE(A2, LDA), %xmm14

	MOVUPS_XL1 (-32 * SIZE, X1, %xmm4)
	MOVUPS_XL1 (-28 * SIZE, X1, %xmm5)

#ifdef PREFETCHW
	PREFETCHW	4 * SIZE(Y1)
#endif

	movq	MM,  I
	sarq	$4,  I
	jle	.L305

	MOVUPS_A1 (-32 * SIZE, A1, %xmm0)
	MOVUPS_A2 (-31 * SIZE, A1, LDA, 1, %xmm1)
	MOVUPS_A1 (-30 * SIZE, A2, %xmm2)
	MOVUPS_A2 (-29 * SIZE, A2, LDA, 1, %xmm3)

	decq	I
	jle	.L303
	ALIGN_4

.L302:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(A1)
#endif

	mulps	 %xmm4, %xmm0
	addps	 %xmm0, %xmm8
	MOVUPS_A1 (-28 * SIZE, A1, %xmm0)
	movss	 %xmm1,  %xmm12
	shufps	 $0x93, %xmm1, %xmm12
	mulps	 %xmm4, %xmm12
	addps	 %xmm12, %xmm9
	MOVUPS_A2 (-27 * SIZE, A1, LDA, 1, %xmm12)

	shufps	 $0x4e, %xmm2, %xmm13
	mulps	 %xmm4, %xmm13
	addps	 %xmm13, %xmm10
	MOVUPS_A1 (-26 * SIZE, A2, %xmm13)
	movss	 %xmm3,  %xmm14
	shufps	 $0x39,  %xmm14, %xmm14
	mulps	 %xmm4,  %xmm14
	MOVUPS_XL1 (-24 * SIZE, X1, %xmm4)
	addps	 %xmm14, %xmm11
	MOVUPS_A2 (-25 * SIZE, A2, LDA, 1, %xmm14)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(A1, LDA)
#endif

	mulps	 %xmm5, %xmm0
	addps	 %xmm0, %xmm8
	MOVUPS_A1 (-24 * SIZE, A1, %xmm0)
	movss	 %xmm12,  %xmm1
	shufps	 $0x93, %xmm12, %xmm1
	mulps	 %xmm5, %xmm1
	addps	 %xmm1, %xmm9
	MOVUPS_A2 (-23 * SIZE, A1, LDA, 1, %xmm1)

	shufps	 $0x4e, %xmm13, %xmm2
	mulps	 %xmm5, %xmm2
	addps	 %xmm2, %xmm10
	MOVUPS_A1 (-22 * SIZE, A2, %xmm2)
	movss	 %xmm14,  %xmm3
	shufps	 $0x39,  %xmm3, %xmm3
	mulps	 %xmm5,  %xmm3
	MOVUPS_XL1 (-20 * SIZE, X1, %xmm5)
	addps	 %xmm3, %xmm11
	MOVUPS_A2 (-21 * SIZE, A2, LDA, 1, %xmm3)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(A2)
#endif

	mulps	 %xmm4, %xmm0
	addps	 %xmm0, %xmm8
	MOVUPS_A1 (-20 * SIZE, A1, %xmm0)
	movss	 %xmm1,  %xmm12
	shufps	 $0x93, %xmm1, %xmm12
	mulps	 %xmm4, %xmm12
	addps	 %xmm12, %xmm9
	MOVUPS_A2 (-19 * SIZE, A1, LDA, 1, %xmm12)

	shufps	 $0x4e, %xmm2, %xmm13
	mulps	 %xmm4, %xmm13
	addps	 %xmm13, %xmm10
	MOVUPS_A1 (-18 * SIZE, A2, %xmm13)
	movss	 %xmm3,  %xmm14
	shufps	 $0x39,  %xmm14, %xmm14
	mulps	 %xmm4,  %xmm14
	MOVUPS_XL1 (-16 * SIZE, X1, %xmm4)
	addps	 %xmm14, %xmm11
	MOVUPS_A2 (-17 * SIZE, A2, LDA, 1, %xmm14)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(A2, LDA)
#endif

	mulps	 %xmm5, %xmm0
	addps	 %xmm0, %xmm8
	MOVUPS_A1 (-16 * SIZE, A1, %xmm0)
	movss	 %xmm12,  %xmm1
	shufps	 $0x93, %xmm12, %xmm1
	mulps	 %xmm5, %xmm1
	addps	 %xmm1, %xmm9
	MOVUPS_A2 (-15 * SIZE, A1, LDA, 1, %xmm1)

#ifdef PREFETCHW
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(X1)
#endif

	shufps	 $0x4e, %xmm13, %xmm2
	mulps	 %xmm5, %xmm2
	addps	 %xmm2, %xmm10
	MOVUPS_A1 (-14 * SIZE, A2, %xmm2)
	movss	 %xmm14,  %xmm3
	shufps	 $0x39,  %xmm3, %xmm3
	mulps	 %xmm5,  %xmm3
	MOVUPS_XL1 (-12 * SIZE, X1, %xmm5)
	addps	 %xmm3, %xmm11
	MOVUPS_A2 (-13 * SIZE, A2, LDA, 1, %xmm3)

	addq	$16 * SIZE, A1
	addq	$16 * SIZE, A2
	addq	$16 * SIZE, X1

	decq	I
	jg	.L302
	ALIGN_4

.L303:
	mulps	 %xmm4, %xmm0
	addps	 %xmm0, %xmm8
	MOVUPS_A1 (-28 * SIZE, A1, %xmm0)
	movss	 %xmm1,  %xmm12
	shufps	 $0x93, %xmm1, %xmm12
	mulps	 %xmm4, %xmm12
	addps	 %xmm12, %xmm9
	MOVUPS_A2 (-27 * SIZE, A1, LDA, 1, %xmm12)

	shufps	 $0x4e, %xmm2, %xmm13
	mulps	 %xmm4, %xmm13
	addps	 %xmm13, %xmm10
	MOVUPS_A1 (-26 * SIZE, A2, %xmm13)
	movss	 %xmm3,  %xmm14
	shufps	 $0x39,  %xmm14, %xmm14
	mulps	 %xmm4,  %xmm14
	MOVUPS_XL1 (-24 * SIZE, X1, %xmm4)
	addps	 %xmm14, %xmm11
	MOVUPS_A2 (-25 * SIZE, A2, LDA, 1, %xmm14)

	mulps	 %xmm5, %xmm0
	addps	 %xmm0, %xmm8
	MOVUPS_A1 (-24 * SIZE, A1, %xmm0)
	movss	 %xmm12,  %xmm1
	shufps	 $0x93, %xmm12, %xmm1
	mulps	 %xmm5, %xmm1
	addps	 %xmm1, %xmm9
	MOVUPS_A2 (-23 * SIZE, A1, LDA, 1, %xmm1)

	shufps	 $0x4e, %xmm13, %xmm2
	mulps	 %xmm5, %xmm2
	addps	 %xmm2, %xmm10
	MOVUPS_A1 (-22 * SIZE, A2, %xmm2)
	movss	 %xmm14,  %xmm3
	shufps	 $0x39,  %xmm3, %xmm3
	mulps	 %xmm5,  %xmm3
	MOVUPS_XL1 (-20 * SIZE, X1, %xmm5)
	addps	 %xmm3, %xmm11
	MOVUPS_A2 (-21 * SIZE, A2, LDA, 1, %xmm3)

	mulps	 %xmm4, %xmm0
	addps	 %xmm0, %xmm8
	MOVUPS_A1 (-20 * SIZE, A1, %xmm0)
	movss	 %xmm1,  %xmm12
	shufps	 $0x93, %xmm1, %xmm12
	mulps	 %xmm4, %xmm12
	addps	 %xmm12, %xmm9
	MOVUPS_A2 (-19 * SIZE, A1, LDA, 1, %xmm12)

	shufps	 $0x4e, %xmm2, %xmm13
	mulps	 %xmm4, %xmm13
	addps	 %xmm13, %xmm10
	MOVUPS_A1 (-18 * SIZE, A2, %xmm13)
	movss	 %xmm3,  %xmm14
	shufps	 $0x39,  %xmm14, %xmm14
	mulps	 %xmm4,  %xmm14
	MOVUPS_XL1 (-16 * SIZE, X1, %xmm4)
	addps	 %xmm14, %xmm11
	MOVUPS_A2 (-17 * SIZE, A2, LDA, 1, %xmm14)

	mulps	 %xmm5, %xmm0
	addps	 %xmm0, %xmm8
	movss	 %xmm12,  %xmm1
	shufps	 $0x93, %xmm12, %xmm1
	mulps	 %xmm5, %xmm1
	addps	 %xmm1, %xmm9

	shufps	 $0x4e, %xmm13, %xmm2
	mulps	 %xmm5, %xmm2
	addps	 %xmm2, %xmm10
	movss	 %xmm14,  %xmm3
	shufps	 $0x39,  %xmm3, %xmm3
	mulps	 %xmm5,  %xmm3
	MOVUPS_XL1 (-12 * SIZE, X1, %xmm5)
	addps	 %xmm3, %xmm11

	addq	$16 * SIZE, A1
	addq	$16 * SIZE, A2
	addq	$16 * SIZE, X1
	ALIGN_4

.L305:
	testq	$8, MM
	jle	.L306

	MOVUPS_A1 (-32 * SIZE, A1, %xmm0)
	MOVUPS_A2 (-31 * SIZE, A1, LDA, 1, %xmm1)
	MOVUPS_A1 (-30 * SIZE, A2, %xmm2)
	MOVUPS_A2 (-29 * SIZE, A2, LDA, 1, %xmm3)

	mulps	 %xmm4, %xmm0
	addps	 %xmm0, %xmm8
	MOVUPS_A1 (-28 * SIZE, A1, %xmm0)
	movss	 %xmm1,  %xmm12
	shufps	 $0x93, %xmm1, %xmm12
	mulps	 %xmm4, %xmm12
	addps	 %xmm12, %xmm9
	MOVUPS_A2 (-27 * SIZE, A1, LDA, 1, %xmm12)

	shufps	 $0x4e, %xmm2, %xmm13
	mulps	 %xmm4, %xmm13
	addps	 %xmm13, %xmm10
	MOVUPS_A1 (-26 * SIZE, A2, %xmm13)
	movss	 %xmm3,  %xmm14
	shufps	 $0x39,  %xmm14, %xmm14
	mulps	 %xmm4,  %xmm14
	MOVUPS_XL1 (-24 * SIZE, X1, %xmm4)
	addps	 %xmm14, %xmm11
	MOVUPS_A2 (-25 * SIZE, A2, LDA, 1, %xmm14)

	mulps	 %xmm5, %xmm0
	addps	 %xmm0, %xmm8
	movss	 %xmm12,  %xmm1
	shufps	 $0x93, %xmm12, %xmm1
	mulps	 %xmm5, %xmm1
	addps	 %xmm1, %xmm9

	shufps	 $0x4e, %xmm13, %xmm2
	mulps	 %xmm5, %xmm2
	addps	 %xmm2, %xmm10
	movss	 %xmm14,  %xmm3
	shufps	 $0x39,  %xmm3, %xmm3
	mulps	 %xmm5,  %xmm3
	addps	 %xmm3, %xmm11

	addq	$8 * SIZE, A1
	addq	$8 * SIZE, A2
	addq	$8 * SIZE, X1
	ALIGN_4

.L306:
	testq	$4, MM
	jle	.L307

	MOVUPS_A1 (-32 * SIZE, A1, %xmm0)
	MOVUPS_A2 (-31 * SIZE, A1, LDA, 1, %xmm1)
	MOVUPS_A1 (-30 * SIZE, A2, %xmm2)
	MOVUPS_A2 (-29 * SIZE, A2, LDA, 1, %xmm3)

	mulps	 %xmm4, %xmm0
	addps	 %xmm0, %xmm8
	movss	 %xmm1,  %xmm12
	shufps	 $0x93, %xmm1, %xmm12
	mulps	 %xmm4, %xmm12
	addps	 %xmm12, %xmm9

	shufps	 $0x4e, %xmm2, %xmm13
	mulps	 %xmm4, %xmm13
	addps	 %xmm13, %xmm10
	movss	 %xmm3,  %xmm14
	shufps	 $0x39,  %xmm14, %xmm14
	mulps	 %xmm4,  %xmm14
	addps	 %xmm14, %xmm11

	addq	$4 * SIZE, A1
	addq	$4 * SIZE, A2
	addq	$4 * SIZE, X1
	ALIGN_4

.L307:
	testq	$2, MM
	jle	.L308

#ifdef movsd
	xorps	%xmm0, %xmm0
#endif
	movsd	-32 * SIZE(A1), %xmm0
#ifdef movsd
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(X1), %xmm4
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
#ifdef movsd
	xorps	%xmm1, %xmm1
#endif
	movsd	-32 * SIZE(A1, LDA), %xmm1
	mulps	%xmm4, %xmm1
	addps	%xmm1, %xmm9
#ifdef movsd
	xorps	%xmm2, %xmm2
#endif
	movsd	-32 * SIZE(A2), %xmm2
	mulps	%xmm4, %xmm2
	addps	%xmm2, %xmm10
#ifdef movsd
	xorps	%xmm3, %xmm3
#endif
	movsd	-32 * SIZE(A2, LDA), %xmm3
	mulps	%xmm4, %xmm3
	addps	%xmm3, %xmm11
	shufps	$0xe, %xmm4, %xmm4

	addq	$2 * SIZE, A1
	addq	$2 * SIZE, A2
	addq	$2 * SIZE, X1
	ALIGN_4

.L308:
	testq	$1, MM
	jle	.L309

	movss	-32 * SIZE(A1), %xmm0
	movss	-32 * SIZE(X1), %xmm4
	mulss	%xmm4, %xmm0
	addss	%xmm0, %xmm8
	movss	-32 * SIZE(A1, LDA), %xmm1
	mulss	%xmm4, %xmm1
	addss	%xmm1, %xmm9
	movss	-32 * SIZE(A2), %xmm2
	mulss	%xmm4, %xmm2
	addss	%xmm2, %xmm10
	movss	-32 * SIZE(A2, LDA), %xmm3
	mulss	%xmm4, %xmm3
	addss	%xmm3, %xmm11
	ALIGN_4

.L309:
#ifdef HAVE_SSE3
	haddps	%xmm9, %xmm8
	haddps	%xmm11, %xmm10
	haddps	%xmm10, %xmm8

	pshufd	$0x1, %xmm8, %xmm9
	pshufd	$0x2, %xmm8, %xmm10
	pshufd	$0x3, %xmm8, %xmm11
#else
	movaps	%xmm8, %xmm0
	unpcklps %xmm9, %xmm8
	unpckhps %xmm9, %xmm0

	movaps	%xmm10, %xmm1
	unpcklps %xmm11, %xmm10
	unpckhps %xmm11, %xmm1

	movaps	%xmm8, %xmm9
	unpcklps %xmm10, %xmm8
	unpckhps %xmm10, %xmm9

	movaps	%xmm0, %xmm10
	unpcklps %xmm1, %xmm0
	unpckhps %xmm1, %xmm10

	addps	%xmm9, %xmm8
	addps	%xmm0, %xmm10
	addps	%xmm10, %xmm8

	pshufd	$0x2, %xmm8, %xmm9
	pshufd	$0x1, %xmm8, %xmm10
	pshufd	$0x3, %xmm8, %xmm11
#endif

	mulss	ALPHA, %xmm8
	mulss	ALPHA, %xmm9
	mulss	ALPHA, %xmm10
	mulss	ALPHA, %xmm11

	addss	(Y), %xmm8
	addq	INCY, Y
	addss	(Y), %xmm9
	addq	INCY, Y
	addss	(Y), %xmm10
	addq	INCY, Y
	addss	(Y), %xmm11
	addq	INCY, Y

	movss	%xmm8, (Y1)
	addq	INCY, Y1
	movss	%xmm9, (Y1)
	addq	INCY, Y1
	movss	%xmm10, (Y1)
	addq	INCY, Y1
	movss	%xmm11, (Y1)
	addq	INCY, Y1

	cmpq	$4, N
	jge	.L301
	ALIGN_4

.L310:
	testq	N, N
	jle	.L999

	cmpq	$3, N
	jne	.L320

	leaq	32 * SIZE(BUFFER), X1

	movq	A, A1
	leaq	(A1, LDA, 2), A2
	leaq	(A1, LDA, 4), A

	xorps	%xmm8, %xmm8
	xorps	%xmm9, %xmm9
	xorps	%xmm10, %xmm10

	cmpq	$3, M
	jle	.L317

	testq	$SIZE, A1
	je	.L31X

	movss	-32 * SIZE(A1), %xmm0
	movss	-32 * SIZE(X1), %xmm4
	mulss	%xmm4, %xmm0
	addss	%xmm0, %xmm8
	movss	-32 * SIZE(A1, LDA), %xmm1
	mulss	%xmm4, %xmm1
	addss	%xmm1, %xmm9
	movss	-32 * SIZE(A2), %xmm2
	mulss	%xmm4, %xmm2
	addss	%xmm2, %xmm10

	addq	 $1 * SIZE, A1
	addq	 $1 * SIZE, A2
	addq	 $1 * SIZE, X1
	ALIGN_3

.L31X:
	testq	$2 * SIZE, A1
	je	.L31XX

#ifdef movsd
	xorps	%xmm0, %xmm0
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(A1), %xmm0
	movsd	-32 * SIZE(X1), %xmm4
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
#ifdef movsd
	xorps	%xmm1, %xmm1
#endif
	movsd	-32 * SIZE(A1, LDA), %xmm1
	mulps	%xmm4, %xmm1
	addps	%xmm1, %xmm9
#ifdef movsd
	xorps	%xmm2, %xmm2
#endif
	movsd	-32 * SIZE(A2), %xmm2
	mulps	%xmm4, %xmm2
	addps	%xmm2, %xmm10

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, X1
	ALIGN_3

.L31XX:
	movaps	-35 * SIZE(A1, LDA), %xmm12
	movaps	-34 * SIZE(A2), %xmm13

	MOVUPS_XL1 (-32 * SIZE, X1, %xmm4)
	MOVUPS_XL1 (-28 * SIZE, X1, %xmm5)

	movq	MM,  I
	sarq	$4,  I
	jle	.L315

	MOVUPS_A1 (-32 * SIZE, A1, %xmm0)
	MOVUPS_A2 (-31 * SIZE, A1, LDA, 1, %xmm1)
	MOVUPS_A1 (-30 * SIZE, A2, %xmm2)

	decq	I
	jle	.L313
	ALIGN_4

.L312:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 3 - 128 + PREOFFSET(A1)
#endif

	mulps	 %xmm4, %xmm0
	addps	 %xmm0, %xmm8
	MOVUPS_A1 (-28 * SIZE, A1, %xmm0)
	movss	 %xmm1,  %xmm12
	shufps	 $0x93, %xmm1, %xmm12
	mulps	 %xmm4, %xmm12
	addps	 %xmm12, %xmm9
	MOVUPS_A2 (-27 * SIZE, A1, LDA, 1, %xmm12)

	shufps	 $0x4e, %xmm2, %xmm13
	mulps	 %xmm4, %xmm13
	MOVUPS_XL1 (-24 * SIZE, X1, %xmm4)
	addps	 %xmm13, %xmm10
	MOVUPS_A1 (-26 * SIZE, A2, %xmm13)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 3 - 128 + PREOFFSET(A1, LDA)
#endif

	mulps	 %xmm5, %xmm0
	addps	 %xmm0, %xmm8
	MOVUPS_A1 (-24 * SIZE, A1, %xmm0)
	movss	 %xmm12,  %xmm1
	shufps	 $0x93, %xmm12, %xmm1
	mulps	 %xmm5, %xmm1
	addps	 %xmm1, %xmm9
	MOVUPS_A2 (-23 * SIZE, A1, LDA, 1, %xmm1)
	shufps	 $0x4e, %xmm13, %xmm2
	mulps	 %xmm5, %xmm2
	MOVUPS_XL1 (-20 * SIZE, X1, %xmm5)
	addps	 %xmm2, %xmm10
	MOVUPS_A1 (-22 * SIZE, A2, %xmm2)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 3 - 128 + PREOFFSET(A2)
#endif

	mulps	 %xmm4, %xmm0
	addps	 %xmm0, %xmm8
	MOVUPS_A1 (-20 * SIZE, A1, %xmm0)
	movss	 %xmm1,  %xmm12
	shufps	 $0x93, %xmm1, %xmm12
	mulps	 %xmm4, %xmm12
	addps	 %xmm12, %xmm9
	MOVUPS_A2 (-19 * SIZE, A1, LDA, 1, %xmm12)
	shufps	 $0x4e, %xmm2, %xmm13
	mulps	 %xmm4, %xmm13
	MOVUPS_XL1 (-16 * SIZE, X1, %xmm4)
	addps	 %xmm13, %xmm10
	MOVUPS_A1 (-18 * SIZE, A2, %xmm13)

#ifdef PREFETCHW
	PREFETCH	(PREFETCHSIZE) * 3 - 128 + PREOFFSET(X1)
#endif

	mulps	 %xmm5, %xmm0
	addps	 %xmm0, %xmm8
	MOVUPS_A1 (-16 * SIZE, A1, %xmm0)
	movss	 %xmm12,  %xmm1
	shufps	 $0x93, %xmm12, %xmm1
	mulps	 %xmm5, %xmm1
	addps	 %xmm1, %xmm9
	MOVUPS_A2 (-15 * SIZE, A1, LDA, 1, %xmm1)
	shufps	 $0x4e, %xmm13, %xmm2
	mulps	 %xmm5, %xmm2
	MOVUPS_XL1 (-12 * SIZE, X1, %xmm5)
	addps	 %xmm2, %xmm10
	MOVUPS_A1 (-14 * SIZE, A2, %xmm2)

	addq	$16 * SIZE, A1
	addq	$16 * SIZE, A2
	addq	$16 * SIZE, X1

	decq	I
	jg	.L312
	ALIGN_4

.L313:
	mulps	 %xmm4, %xmm0
	addps	 %xmm0, %xmm8
	MOVUPS_A1 (-28 * SIZE, A1, %xmm0)
	movss	 %xmm1,  %xmm12
	shufps	 $0x93, %xmm1, %xmm12
	mulps	 %xmm4, %xmm12
	addps	 %xmm12, %xmm9
	MOVUPS_A2 (-27 * SIZE, A1, LDA, 1, %xmm12)

	shufps	 $0x4e, %xmm2, %xmm13
	mulps	 %xmm4, %xmm13
	addps	 %xmm13, %xmm10
	MOVUPS_A1 (-26 * SIZE, A2, %xmm13)
	movss	 %xmm3,  %xmm14
	shufps	 $0x39,  %xmm14, %xmm14
	mulps	 %xmm4,  %xmm14
	MOVUPS_XL1 (-24 * SIZE, X1, %xmm4)
	addps	 %xmm14, %xmm11
	MOVUPS_A2 (-25 * SIZE, A2, LDA, 1, %xmm14)

	mulps	 %xmm5, %xmm0
	addps	 %xmm0, %xmm8
	MOVUPS_A1 (-24 * SIZE, A1, %xmm0)
	movss	 %xmm12,  %xmm1
	shufps	 $0x93, %xmm12, %xmm1
	mulps	 %xmm5, %xmm1
	addps	 %xmm1, %xmm9
	MOVUPS_A2 (-23 * SIZE, A1, LDA, 1, %xmm1)

	shufps	 $0x4e, %xmm13, %xmm2
	mulps	 %xmm5, %xmm2
	addps	 %xmm2, %xmm10
	MOVUPS_A1 (-22 * SIZE, A2, %xmm2)
	movss	 %xmm14,  %xmm3
	shufps	 $0x39,  %xmm3, %xmm3
	mulps	 %xmm5,  %xmm3
	MOVUPS_XL1 (-20 * SIZE, X1, %xmm5)
	addps	 %xmm3, %xmm11
	MOVUPS_A2 (-21 * SIZE, A2, LDA, 1, %xmm3)

	mulps	 %xmm4, %xmm0
	addps	 %xmm0, %xmm8
	MOVUPS_A1 (-20 * SIZE, A1, %xmm0)
	movss	 %xmm1,  %xmm12
	shufps	 $0x93, %xmm1, %xmm12
	mulps	 %xmm4, %xmm12
	addps	 %xmm12, %xmm9
	MOVUPS_A2 (-19 * SIZE, A1, LDA, 1, %xmm12)

	shufps	 $0x4e, %xmm2, %xmm13
	mulps	 %xmm4, %xmm13
	addps	 %xmm13, %xmm10
	MOVUPS_A1 (-18 * SIZE, A2, %xmm13)
	movss	 %xmm3,  %xmm14
	shufps	 $0x39,  %xmm14, %xmm14
	mulps	 %xmm4,  %xmm14
	MOVUPS_XL1 (-16 * SIZE, X1, %xmm4)
	addps	 %xmm14, %xmm11
	MOVUPS_A2 (-17 * SIZE, A2, LDA, 1, %xmm14)

	mulps	 %xmm5, %xmm0
	addps	 %xmm0, %xmm8
	movss	 %xmm12,  %xmm1
	shufps	 $0x93, %xmm12, %xmm1
	mulps	 %xmm5, %xmm1
	addps	 %xmm1, %xmm9

	shufps	 $0x4e, %xmm13, %xmm2
	mulps	 %xmm5, %xmm2
	addps	 %xmm2, %xmm10
	movss	 %xmm14,  %xmm3
	shufps	 $0x39,  %xmm3, %xmm3
	mulps	 %xmm5,  %xmm3
	MOVUPS_XL1 (-12 * SIZE, X1, %xmm5)
	addps	 %xmm3, %xmm11

	addq	$16 * SIZE, A1
	addq	$16 * SIZE, A2
	addq	$16 * SIZE, X1
	ALIGN_4

.L315:
	testq	$8, MM
	jle	.L316

	MOVUPS_A1 (-32 * SIZE, A1, %xmm0)
	MOVUPS_A2 (-31 * SIZE, A1, LDA, 1, %xmm1)
	MOVUPS_A1 (-30 * SIZE, A2, %xmm2)

	mulps	 %xmm4, %xmm0
	addps	 %xmm0, %xmm8
	MOVUPS_A1 (-28 * SIZE, A1, %xmm0)
	movss	 %xmm1,  %xmm12
	shufps	 $0x93, %xmm1, %xmm12
	mulps	 %xmm4, %xmm12
	addps	 %xmm12, %xmm9
	MOVUPS_A2 (-27 * SIZE, A1, LDA, 1, %xmm12)

	shufps	 $0x4e, %xmm2, %xmm13
	mulps	 %xmm4, %xmm13
	MOVUPS_XL1 (-24 * SIZE, X1, %xmm4)
	addps	 %xmm13, %xmm10
	MOVUPS_A1 (-26 * SIZE, A2, %xmm13)

	mulps	 %xmm5, %xmm0
	addps	 %xmm0, %xmm8
	movss	 %xmm12,  %xmm1
	shufps	 $0x93, %xmm12, %xmm1
	mulps	 %xmm5, %xmm1
	addps	 %xmm1, %xmm9

	shufps	 $0x4e, %xmm13, %xmm2
	mulps	 %xmm5, %xmm2
	addps	 %xmm2, %xmm10

	addq	$8 * SIZE, A1
	addq	$8 * SIZE, A2
	addq	$8 * SIZE, X1
	ALIGN_4

.L316:
	testq	$4, MM
	jle	.L317

	MOVUPS_A1 (-32 * SIZE, A1, %xmm0)
	MOVUPS_A2 (-31 * SIZE, A1, LDA, 1, %xmm1)
	MOVUPS_A1 (-30 * SIZE, A2, %xmm2)

	mulps	 %xmm4, %xmm0
	addps	 %xmm0, %xmm8
	movss	 %xmm1,  %xmm12
	shufps	 $0x93, %xmm1, %xmm12
	mulps	 %xmm4, %xmm12
	addps	 %xmm12, %xmm9

	shufps	 $0x4e, %xmm2, %xmm13
	mulps	 %xmm4, %xmm13
	addps	 %xmm13, %xmm10

	addq	$4 * SIZE, A1
	addq	$4 * SIZE, A2
	addq	$4 * SIZE, X1
	ALIGN_4

.L317:
	testq	$2, MM
	jle	.L318

#ifdef movsd
	xorps	%xmm0, %xmm0
#endif
	movsd	-32 * SIZE(A1), %xmm0
#ifdef movsd
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(X1), %xmm4
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
#ifdef movsd
	xorps	%xmm1, %xmm1
#endif
	movsd	-32 * SIZE(A1, LDA), %xmm1
	mulps	%xmm4, %xmm1
	addps	%xmm1, %xmm9
#ifdef movsd
	xorps	%xmm2, %xmm2
#endif
	movsd	-32 * SIZE(A2), %xmm2
	mulps	%xmm4, %xmm2
	addps	%xmm2, %xmm10

	addq	$2 * SIZE, A1
	addq	$2 * SIZE, A2
	addq	$2 * SIZE, X1
	ALIGN_4

.L318:
	testq	$1, MM
	jle	.L319

	movss	-32 * SIZE(A1), %xmm0
	movss	-32 * SIZE(X1), %xmm4
	mulss	%xmm4, %xmm0
	addss	%xmm0, %xmm8
	movss	-32 * SIZE(A1, LDA), %xmm1
	mulss	%xmm4, %xmm1
	addss	%xmm1, %xmm9
	movss	-32 * SIZE(A2), %xmm2
	mulss	%xmm4, %xmm2
	addss	%xmm2, %xmm10
	ALIGN_4

.L319:
#ifdef HAVE_SSE3
	haddps	%xmm9, %xmm8
	haddps	%xmm11, %xmm10
	haddps	%xmm10, %xmm8

	pshufd	$0x1, %xmm8, %xmm9
	pshufd	$0x2, %xmm8, %xmm10
#else
	movaps	%xmm8, %xmm0
	unpcklps %xmm9, %xmm8
	unpckhps %xmm9, %xmm0

	movaps	%xmm10, %xmm1
	unpcklps %xmm11, %xmm10
	unpckhps %xmm11, %xmm1

	movaps	%xmm8, %xmm9
	unpcklps %xmm10, %xmm8
	unpckhps %xmm10, %xmm9

	movaps	%xmm0, %xmm10
	unpcklps %xmm1, %xmm0
	unpckhps %xmm1, %xmm10

	addps	%xmm9, %xmm8
	addps	%xmm0, %xmm10
	addps	%xmm10, %xmm8

	pshufd	$0x2, %xmm8, %xmm9
	pshufd	$0x1, %xmm8, %xmm10
#endif

	mulss	ALPHA, %xmm8
	mulss	ALPHA, %xmm9
	mulss	ALPHA, %xmm10

	addss	(Y), %xmm8
	addq	INCY, Y
	addss	(Y), %xmm9
	addq	INCY, Y
	addss	(Y), %xmm10

	movss	%xmm8, (Y1)
	addq	INCY, Y1
	movss	%xmm9, (Y1)
	addq	INCY, Y1
	movss	%xmm10, (Y1)
	jmp	.L999
	ALIGN_3

.L320:
	cmpq	$2, N
	jne	.L330

	leaq	32 * SIZE(BUFFER), X1

	movq	A, A1
	leaq	(A1, LDA), A2

	xorps	%xmm8, %xmm8
	xorps	%xmm9, %xmm9

	cmpq	$3, M
	jle	.L327

	testq	$SIZE, A1
	je	.L32X

	movss	-32 * SIZE(A1), %xmm0
	movss	-32 * SIZE(X1), %xmm4
	mulss	%xmm4, %xmm0
	addss	%xmm0, %xmm8
	movss	-32 * SIZE(A2), %xmm1
	mulss	%xmm4, %xmm1
	addss	%xmm1, %xmm9

	addq	 $1 * SIZE, A1
	addq	 $1 * SIZE, A2
	addq	 $1 * SIZE, X1
	ALIGN_3

.L32X:
	testq	$2 * SIZE, A1
	je	.L32XX

#ifdef movsd
	xorps	%xmm0, %xmm0
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(A1), %xmm0
	movsd	-32 * SIZE(X1), %xmm4
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
#ifdef movsd
	xorps	%xmm1, %xmm1
#endif
	movsd	-32 * SIZE(A2), %xmm1
	mulps	%xmm4, %xmm1
	addps	%xmm1, %xmm9

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, X1
	ALIGN_3

.L32XX:
	movaps	-35 * SIZE(A2), %xmm12

	MOVUPS_XL1 (-32 * SIZE, X1, %xmm4)
	MOVUPS_XL1 (-28 * SIZE, X1, %xmm5)

	movq	MM,  I
	sarq	$4,  I
	jle	.L325

	MOVUPS_A1 (-32 * SIZE, A1, %xmm0)
	MOVUPS_A1 (-31 * SIZE, A2, %xmm1)

	decq	I
	jle	.L323
	ALIGN_4

.L322:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 4 - 128 + PREOFFSET(A1)
#endif

	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-28 * SIZE, A1, %xmm0)
	movss	 %xmm1,  %xmm12
	shufps	 $0x93, %xmm1, %xmm12
	mulps	 %xmm4, %xmm12
	MOVUPS_XL1 (-24 * SIZE, X1, %xmm4)
	addps	 %xmm12, %xmm9
	MOVUPS_A1 (-27 * SIZE, A2, %xmm12)

	mulps	%xmm5, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-24 * SIZE, A1, %xmm0)
	movss	 %xmm12,  %xmm1
	shufps	 $0x93, %xmm12, %xmm1
	mulps	 %xmm5, %xmm1
	MOVUPS_XL1 (-20 * SIZE, X1, %xmm5)
	addps	 %xmm1, %xmm9
	MOVUPS_A1 (-23 * SIZE, A2, %xmm1)

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 4 - 128 + PREOFFSET(A2)
#endif

	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-20 * SIZE, A1, %xmm0)
	movss	 %xmm1,  %xmm12
	shufps	 $0x93, %xmm1, %xmm12
	mulps	 %xmm4, %xmm12
	MOVUPS_XL1 (-16 * SIZE, X1, %xmm4)
	addps	 %xmm12, %xmm9
	MOVUPS_A1 (-19 * SIZE, A2, %xmm12)

#ifdef PREFETCHW
	PREFETCH	(PREFETCHSIZE) * 4 - 128 + PREOFFSET(X1)
#endif

	mulps	%xmm5, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-16 * SIZE, A1, %xmm0)
	movss	 %xmm12,  %xmm1
	shufps	 $0x93, %xmm12, %xmm1
	mulps	 %xmm5, %xmm1
	MOVUPS_XL1 (-12 * SIZE, X1, %xmm5)
	addps	 %xmm1, %xmm9
	MOVUPS_A1 (-15 * SIZE, A2, %xmm1)

	addq	$16 * SIZE, A1
	addq	$16 * SIZE, A2
	addq	$16 * SIZE, X1

	decq	I
	jg	.L322
	ALIGN_4

.L323:
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-28 * SIZE, A1, %xmm0)
	movss	 %xmm1,  %xmm12
	shufps	 $0x93, %xmm1, %xmm12
	mulps	 %xmm4, %xmm12
	MOVUPS_XL1 (-24 * SIZE, X1, %xmm4)
	addps	 %xmm12, %xmm9
	MOVUPS_A1 (-27 * SIZE, A2, %xmm12)

	mulps	%xmm5, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-24 * SIZE, A1, %xmm0)
	movss	 %xmm12,  %xmm1
	shufps	 $0x93, %xmm12, %xmm1
	mulps	 %xmm5, %xmm1
	MOVUPS_XL1 (-20 * SIZE, X1, %xmm5)
	addps	 %xmm1, %xmm9
	MOVUPS_A1 (-23 * SIZE, A2, %xmm1)

	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-20 * SIZE, A1, %xmm0)
	movss	 %xmm1,  %xmm12
	shufps	 $0x93, %xmm1, %xmm12
	mulps	 %xmm4, %xmm12
	MOVUPS_XL1 (-16 * SIZE, X1, %xmm4)
	addps	 %xmm12, %xmm9
	MOVUPS_A1 (-19 * SIZE, A2, %xmm12)

	mulps	%xmm5, %xmm0
	addps	%xmm0, %xmm8
	movss	 %xmm12,  %xmm1
	shufps	 $0x93, %xmm12, %xmm1
	mulps	 %xmm5, %xmm1
	MOVUPS_XL1 (-12 * SIZE, X1, %xmm5)
	addps	 %xmm1, %xmm9

	addq	$16 * SIZE, A1
	addq	$16 * SIZE, A2
	addq	$16 * SIZE, X1
	ALIGN_4

.L325:
	testq	$8, MM
	jle	.L326

	MOVUPS_A1 (-32 * SIZE, A1, %xmm0)
	MOVUPS_A1 (-31 * SIZE, A2, %xmm1)

	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-28 * SIZE, A1, %xmm0)
	movss	 %xmm1,  %xmm12
	shufps	 $0x93, %xmm1, %xmm12
	mulps	 %xmm4, %xmm12
	MOVUPS_XL1 (-24 * SIZE, X1, %xmm4)
	addps	 %xmm12, %xmm9
	MOVUPS_A1 (-27 * SIZE, A2, %xmm12)

	mulps	%xmm5, %xmm0
	addps	%xmm0, %xmm8
	movss	 %xmm12,  %xmm1
	shufps	 $0x93, %xmm12, %xmm1
	mulps	 %xmm5, %xmm1
	addps	 %xmm1, %xmm9

	addq	$8 * SIZE, A1
	addq	$8 * SIZE, A2
	addq	$8 * SIZE, X1
	ALIGN_4

.L326:
	testq	$4, MM
	jle	.L327

	MOVUPS_A1 (-32 * SIZE, A1, %xmm0)
	MOVUPS_A1 (-31 * SIZE, A2, %xmm1)

	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	movss	 %xmm1,  %xmm12
	shufps	 $0x93, %xmm1, %xmm12
	mulps	 %xmm4, %xmm12
	addps	 %xmm12, %xmm9

	addq	$4 * SIZE, A1
	addq	$4 * SIZE, A2
	addq	$4 * SIZE, X1
	ALIGN_4

.L327:
	testq	$2, MM
	jle	.L328

#ifdef movsd
	xorps	%xmm0, %xmm0
#endif
	movsd	-32 * SIZE(A1), %xmm0
#ifdef movsd
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(X1), %xmm4
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
#ifdef movsd
	xorps	%xmm1, %xmm1
#endif
	movsd	-32 * SIZE(A2), %xmm1
	mulps	%xmm4, %xmm1
	addps	%xmm1, %xmm9
	shufps	$0xe, %xmm4, %xmm4

	addq	$2 * SIZE, A1
	addq	$2 * SIZE, A2
	addq	$2 * SIZE, X1
	ALIGN_4

.L328:
	testq	$1, MM
	jle	.L329

	movss	-32 * SIZE(A1), %xmm0
	movss	-32 * SIZE(X1), %xmm4
	mulss	%xmm4, %xmm0
	addss	%xmm0, %xmm8
	movss	-32 * SIZE(A2), %xmm1
	mulss	%xmm4, %xmm1
	addss	%xmm1, %xmm9
	ALIGN_4

.L329:
#ifdef HAVE_SSE3
	haddps	%xmm9, %xmm8
	haddps	%xmm8, %xmm8
#else
	movaps	%xmm8, %xmm10
	unpcklps %xmm9, %xmm8
	unpckhps %xmm9, %xmm10

	addps	%xmm10, %xmm8
	movhlps %xmm8, %xmm9
	addps	%xmm9, %xmm8
#endif

	pshufd	$0x1, %xmm8, %xmm9

	mulss	ALPHA, %xmm8
	mulss	ALPHA, %xmm9

	addss	(Y), %xmm8
	addq	INCY, Y
	addss	(Y), %xmm9
	addq	INCY, Y

	movss	%xmm8, (Y1)
	addq	INCY, Y1
	movss	%xmm9, (Y1)
	addq	INCY, Y1
	jmp	.L999
	ALIGN_4

.L330:
	cmpq	$1, N
	jne	.L999

	leaq	32 * SIZE(BUFFER), X1

	movq	A, A1

	xorps	%xmm8, %xmm8
	xorps	%xmm9, %xmm9

	cmpq	$3, M
	jle	.L337

	testq	$SIZE, A1
	je	.L33X

	movss	-32 * SIZE(A1), %xmm0
	movss	-32 * SIZE(X1), %xmm4
	mulss	%xmm4, %xmm0
	addss	%xmm0, %xmm8

	addq	 $1 * SIZE, A1
	addq	 $1 * SIZE, X1
	ALIGN_3

.L33X:
	testq	$2 * SIZE, A1
	je	.L33XX

#ifdef movsd
	xorps	%xmm0, %xmm0
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(A1), %xmm0
	movsd	-32 * SIZE(X1), %xmm4
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	shufps	$0xe, %xmm4, %xmm4

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, X1
	ALIGN_3

.L33XX:

	MOVUPS_XL1 (-32 * SIZE, X1, %xmm4)
	MOVUPS_XL1 (-28 * SIZE, X1, %xmm5)

	movq	MM,  I
	sarq	$4,  I
	jle	.L335

	MOVUPS_A1 (-32 * SIZE, A1, %xmm0)
	MOVUPS_A1 (-28 * SIZE, A1, %xmm12)

	decq	I
	jle	.L333
	ALIGN_4

.L332:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 8 - 128 + PREOFFSET(A1)
#endif

	mulps	%xmm4, %xmm0
	MOVUPS_XL1 (-24 * SIZE, X1, %xmm4)
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-24 * SIZE, A1, %xmm0)

	mulps	%xmm5, %xmm12
	MOVUPS_XL1 (-20 * SIZE, X1, %xmm5)
	addps	%xmm12, %xmm9
	MOVUPS_A1 (-20 * SIZE, A1, %xmm12)

#ifdef PREFETCHW
	PREFETCH	(PREFETCHSIZE) * 8 - 128 + PREOFFSET(X1)
#endif

	mulps	%xmm4, %xmm0
	MOVUPS_XL1 (-16 * SIZE, X1, %xmm4)
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-16 * SIZE, A1, %xmm0)

	mulps	%xmm5, %xmm12
	MOVUPS_XL1 (-12 * SIZE, X1, %xmm5)
	addps	%xmm12, %xmm9
	MOVUPS_A1 (-12 * SIZE, A1, %xmm12)

	addq	$16 * SIZE, A1
	addq	$16 * SIZE, X1

	decq	I
	jg	.L332
	ALIGN_4

.L333:
	mulps	%xmm4, %xmm0
	MOVUPS_XL1 (-24 * SIZE, X1, %xmm4)
	addps	%xmm0, %xmm8
	MOVUPS_A1 (-24 * SIZE, A1, %xmm0)

	mulps	%xmm5, %xmm12
	MOVUPS_XL1 (-20 * SIZE, X1, %xmm5)
	addps	%xmm12, %xmm9
	MOVUPS_A1 (-20 * SIZE, A1, %xmm12)

	mulps	%xmm4, %xmm0
	MOVUPS_XL1 (-16 * SIZE, X1, %xmm4)
	addps	%xmm0, %xmm8

	mulps	%xmm5, %xmm12
	MOVUPS_XL1 (-12 * SIZE, X1, %xmm5)
	addps	%xmm12, %xmm9

	addq	$16 * SIZE, A1
	addq	$16 * SIZE, X1
	ALIGN_4

.L335:
	testq	$8, MM
	jle	.L336

	MOVUPS_A1 (-32 * SIZE, A1, %xmm0)
	mulps	%xmm4, %xmm0
	MOVUPS_XL1 (-24 * SIZE, X1, %xmm4)
	addps	%xmm0, %xmm8

	MOVUPS_A1 (-28 * SIZE, A1, %xmm12)
	mulps	%xmm5, %xmm12
	addps	%xmm12, %xmm9

	addq	$8 * SIZE, A1
	addq	$8 * SIZE, X1
	ALIGN_4

.L336:
	testq	$4, MM
	jle	.L337

	MOVUPS_A1 (-32 * SIZE, A1, %xmm0)
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8

	addq	$4 * SIZE, A1
	addq	$4 * SIZE, X1
	ALIGN_4

.L337:
	testq	$2, MM
	jle	.L338

#ifdef movsd
	xorps	%xmm0, %xmm0
#endif
	movsd	-32 * SIZE(A1), %xmm0
#ifdef movsd
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(X1), %xmm4
	mulps	%xmm4, %xmm0
	addps	%xmm0, %xmm8
	shufps	$0xe, %xmm4, %xmm4

	addq	$2 * SIZE, A1
	addq	$2 * SIZE, X1
	ALIGN_4

.L338:
	testq	$1, MM
	jle	.L339

	movss	-32 * SIZE(A1), %xmm0
	movss	-32 * SIZE(X1), %xmm4
	mulss	%xmm4, %xmm0
	addss	%xmm0, %xmm8
	ALIGN_4

.L339:
	addps	%xmm9, %xmm8

#ifdef HAVE_SSE3
	haddps	%xmm8, %xmm8
	haddps	%xmm8, %xmm8
#else
	pshufd	$1, %xmm8, %xmm9
	pshufd	$2, %xmm8, %xmm10
	pshufd	$3, %xmm8, %xmm11

	addss	%xmm9, %xmm8
	addss	%xmm11, %xmm10
	addss	%xmm10, %xmm8
#endif

	mulss	ALPHA, %xmm8

	addss	(Y), %xmm8
	movss	%xmm8, (Y1)

	jmp	.L999
#endif
	ALIGN_4

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
	ALIGN_4

	EPILOGUE
