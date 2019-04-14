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
#define ALPHA		224	       (%rsp)

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
#define A1	%r11
#define A2	%r12

#define X1	%rbx
#define Y1	%r13
#define BUFFER	%r14

#ifdef ALIGNED_ACCESS
#define MM	%r15
#else
#define MM	M
#endif

#undef SUBPS

#ifndef CONJ
#define SUBPS	   addps
#else
#define SUBPS	   subps
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

	movq	OLD_INCX,  INCX
	movq	OLD_Y,     Y
	movq	OLD_INCY,  INCY
	movq	OLD_BUFFER, BUFFER

	salq	$ZBASE_SHIFT,   LDA
	salq	$ZBASE_SHIFT,   INCX
	salq	$ZBASE_SHIFT,   INCY

	unpcklps %xmm1, %xmm0

	movlps	 %xmm0, ALPHA

	testq	M, M
	jle	.L999
	testq	N, N
	jle	.L999

	subq	$-32 * SIZE, A

	movq	BUFFER, X1

#ifdef ALIGNED_ACCESS
	movq	M, MM
	movq	A, %rax
	andq	$4 * SIZE - 1, %rax
	cmpq	$2 * SIZE, %rax

	jl	.L0X

	movsd	(X), %xmm0
	addq	INCX, X
	movlps	%xmm0, 2 * SIZE(X1)

	addq	$2 * SIZE, BUFFER
 	addq	$4 * SIZE, X1
	decq	MM

.L0X:
#endif

	movq	MM, I
	sarq	$3, I
	jle	.L05
	ALIGN_4

.L02:
	movsd	(X), %xmm0
	addq	INCX, X
	movhps	(X), %xmm0
	addq	INCX, X

	movsd	(X), %xmm1
	addq	INCX, X
	movhps	(X), %xmm1
	addq	INCX, X

	movsd	(X), %xmm2
	addq	INCX, X
	movhps	(X), %xmm2
	addq	INCX, X

	movsd	(X), %xmm3
	addq	INCX, X
	movhps	(X), %xmm3
	addq	INCX, X

	movaps	%xmm0,  0 * SIZE(X1)
	movaps	%xmm1,  4 * SIZE(X1)
	movaps	%xmm2,  8 * SIZE(X1)
	movaps	%xmm3, 12 * SIZE(X1)

	addq	$16 * SIZE, X1
	decq	I
	jg	.L02
	ALIGN_4

.L05:
	movq	MM, I
	andq	$7, I
	jle	.L10
	ALIGN_2

.L06:
	movsd	(X), %xmm0
	addq	INCX, X
	movlps	%xmm0, 0 * SIZE(X1)
	addq	$2 * SIZE, X1
	decq	I
	jg	.L06
	ALIGN_4

.L10:
	movq	Y, Y1

#ifdef ALIGNED_ACCESS
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

	leaq	32 * SIZE(BUFFER), X1

	movq	A, A1
	leaq	(A1, LDA, 2), A2
	leaq	(A1, LDA, 4), A

	xorps	%xmm0, %xmm0
	xorps	%xmm1, %xmm1
	xorps	%xmm2, %xmm2
	xorps	%xmm3, %xmm3
	xorps	%xmm4, %xmm4
	xorps	%xmm5, %xmm5
	xorps	%xmm6, %xmm6
	xorps	%xmm7, %xmm7

#ifdef ALIGNED_ACCESS
	cmpq	M, MM
	je	.L1X

#ifdef movsd
	xorps	%xmm8, %xmm8
#endif
	movsd	-32 * SIZE(A1), %xmm8
#ifdef movsd
	xorps	%xmm9, %xmm9
#endif
	movsd	-32 * SIZE(A1, LDA), %xmm9
#ifdef movsd
	xorps	%xmm10, %xmm10
#endif
	movsd	-32 * SIZE(A2), %xmm10
#ifdef movsd
	xorps	%xmm11, %xmm11
#endif
	movsd	-32 * SIZE(A2, LDA), %xmm11

#ifdef movsd
	xorps	%xmm12, %xmm12
#endif
	movsd	-32 * SIZE(X1), %xmm12

	pshufd	$0xb1,  %xmm8, %xmm14
	mulps	%xmm12, %xmm8
	addps	%xmm8,  %xmm0
	pshufd	$0xb1,  %xmm9, %xmm15
	mulps	%xmm12, %xmm9
	addps	%xmm9,  %xmm2

	mulps	%xmm12, %xmm14
	SUBPS	%xmm14, %xmm1
	mulps	%xmm12, %xmm15
	SUBPS	%xmm15, %xmm3

	pshufd	$0xb1,  %xmm10, %xmm14
	mulps	%xmm12, %xmm10
	addps	%xmm10, %xmm4
	pshufd	$0xb1,  %xmm11, %xmm15
	mulps	%xmm12, %xmm11
	addps	%xmm11, %xmm6

	mulps	%xmm12, %xmm14
	SUBPS	%xmm14, %xmm5
	mulps	%xmm12, %xmm15
	SUBPS	%xmm15, %xmm7

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, X1
	ALIGN_3
.L1X:
#endif

	movaps	-32 * SIZE(X1), %xmm12
	movaps	-28 * SIZE(X1), %xmm13

#ifdef PREFETCHW
	PREFETCHW	7 * SIZE(Y1)
#endif

	movq	MM,  I
	sarq	$3,  I
	jle	.L15

	MOVUPS_A1(-32 * SIZE, A1, %xmm8)
	MOVUPS_A2(-32 * SIZE, A1, LDA, 1, %xmm9)
	MOVUPS_A1(-32 * SIZE, A2, %xmm10)
	MOVUPS_A2(-32 * SIZE, A2, LDA, 1, %xmm11)

	decq	 I
	jle	 .L14
	ALIGN_3

.L13:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A1)
#endif

	pshufd	$0xb1,  %xmm8, %xmm14
	mulps	%xmm12, %xmm8
	addps	%xmm8,  %xmm0
	MOVUPS_A1(-28 * SIZE, A1, %xmm8)
	pshufd	$0xb1,  %xmm9, %xmm15
	mulps	%xmm12, %xmm9
	addps	%xmm9,  %xmm2
	MOVUPS_A2(-28 * SIZE, A1, LDA, 1, %xmm9)

	mulps	%xmm12, %xmm14
	SUBPS	%xmm14, %xmm1
	mulps	%xmm12, %xmm15
	SUBPS	%xmm15, %xmm3

	pshufd	$0xb1,  %xmm10, %xmm14
	mulps	%xmm12, %xmm10
	addps	%xmm10, %xmm4
	MOVUPS_A1(-28 * SIZE, A2, %xmm10)
	pshufd	$0xb1,  %xmm11, %xmm15
	mulps	%xmm12, %xmm11
	addps	%xmm11, %xmm6
	MOVUPS_A2(-28 * SIZE, A2, LDA, 1, %xmm11)

	mulps	%xmm12, %xmm14
	SUBPS	%xmm14, %xmm5
	mulps	%xmm12, %xmm15
	movaps	-24 * SIZE(X1), %xmm12
	SUBPS	%xmm15, %xmm7

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A1, LDA)
#endif

	pshufd	$0xb1,  %xmm8, %xmm14
	mulps	%xmm13, %xmm8
	addps	%xmm8,  %xmm0
	MOVUPS_A1(-24 * SIZE, A1, %xmm8)
	pshufd	$0xb1,  %xmm9, %xmm15
	mulps	%xmm13, %xmm9
	addps	%xmm9,  %xmm2
	MOVUPS_A2(-24 * SIZE, A1, LDA, 1, %xmm9)

	mulps	%xmm13, %xmm14
	SUBPS	%xmm14, %xmm1
	mulps	%xmm13, %xmm15
	SUBPS	%xmm15, %xmm3

	pshufd	$0xb1,  %xmm10, %xmm14
	mulps	%xmm13, %xmm10
	addps	%xmm10, %xmm4
	MOVUPS_A1(-24 * SIZE, A2, %xmm10)
	pshufd	$0xb1,  %xmm11, %xmm15
	mulps	%xmm13, %xmm11
	addps	%xmm11, %xmm6
	MOVUPS_A2(-24 * SIZE, A2, LDA, 1, %xmm11)

	mulps	%xmm13, %xmm14
	SUBPS	%xmm14, %xmm5
	mulps	%xmm13, %xmm15
	movaps	-20 * SIZE(X1), %xmm13
	SUBPS	%xmm15, %xmm7

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A2)
#endif

	pshufd	$0xb1,  %xmm8, %xmm14
	mulps	%xmm12, %xmm8
	addps	%xmm8,  %xmm0
	MOVUPS_A1(-20 * SIZE, A1, %xmm8)
	pshufd	$0xb1,  %xmm9, %xmm15
	mulps	%xmm12, %xmm9
	addps	%xmm9,  %xmm2
	MOVUPS_A2(-20 * SIZE, A1, LDA, 1, %xmm9)

	mulps	%xmm12, %xmm14
	SUBPS	%xmm14, %xmm1
	mulps	%xmm12, %xmm15
	SUBPS	%xmm15, %xmm3

	pshufd	$0xb1,  %xmm10, %xmm14
	mulps	%xmm12, %xmm10
	addps	%xmm10, %xmm4
	MOVUPS_A1(-20 * SIZE, A2, %xmm10)
	pshufd	$0xb1,  %xmm11, %xmm15
	mulps	%xmm12, %xmm11
	addps	%xmm11, %xmm6
	MOVUPS_A2(-20 * SIZE, A2, LDA, 1, %xmm11)

	mulps	%xmm12, %xmm14
	SUBPS	%xmm14, %xmm5
	mulps	%xmm12, %xmm15
	movaps	-16 * SIZE(X1), %xmm12
	SUBPS	%xmm15, %xmm7

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A2, LDA)
#endif

	pshufd	$0xb1,  %xmm8, %xmm14
	mulps	%xmm13, %xmm8
	addps	%xmm8,  %xmm0
	MOVUPS_A1(-16 * SIZE, A1, %xmm8)
	pshufd	$0xb1,  %xmm9, %xmm15
	mulps	%xmm13, %xmm9
	addps	%xmm9,  %xmm2
	MOVUPS_A2(-16 * SIZE, A1, LDA, 1, %xmm9)

	mulps	%xmm13, %xmm14
	SUBPS	%xmm14, %xmm1
	mulps	%xmm13, %xmm15
	SUBPS	%xmm15, %xmm3

#ifdef PREFETCHW
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(X1)
#endif

	pshufd	$0xb1,  %xmm10, %xmm14
	mulps	%xmm13, %xmm10
	addps	%xmm10, %xmm4
	MOVUPS_A1(-16 * SIZE, A2, %xmm10)
	pshufd	$0xb1,  %xmm11, %xmm15
	mulps	%xmm13, %xmm11
	addps	%xmm11, %xmm6
	MOVUPS_A2(-16 * SIZE, A2, LDA, 1, %xmm11)

	mulps	%xmm13, %xmm14
	SUBPS	%xmm14, %xmm5
	mulps	%xmm13, %xmm15
	movaps	-12 * SIZE(X1), %xmm13
	SUBPS	%xmm15, %xmm7

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, A2
	subq	 $-16 * SIZE, X1

	subq	 $1, I
	BRANCH
	jg	.L13
	ALIGN_3

.L14:
	pshufd	$0xb1,  %xmm8, %xmm14
	mulps	%xmm12, %xmm8
	addps	%xmm8,  %xmm0
	MOVUPS_A1(-28 * SIZE, A1, %xmm8)
	pshufd	$0xb1,  %xmm9, %xmm15
	mulps	%xmm12, %xmm9
	addps	%xmm9,  %xmm2
	MOVUPS_A2(-28 * SIZE, A1, LDA, 1, %xmm9)

	mulps	%xmm12, %xmm14
	SUBPS	%xmm14, %xmm1
	mulps	%xmm12, %xmm15
	SUBPS	%xmm15, %xmm3

	pshufd	$0xb1,  %xmm10, %xmm14
	mulps	%xmm12, %xmm10
	addps	%xmm10, %xmm4
	MOVUPS_A1(-28 * SIZE, A2, %xmm10)
	pshufd	$0xb1,  %xmm11, %xmm15
	mulps	%xmm12, %xmm11
	addps	%xmm11, %xmm6
	MOVUPS_A2(-28 * SIZE, A2, LDA, 1, %xmm11)

	mulps	%xmm12, %xmm14
	SUBPS	%xmm14, %xmm5
	mulps	%xmm12, %xmm15
	movaps	-24 * SIZE(X1), %xmm12
	SUBPS	%xmm15, %xmm7

	pshufd	$0xb1,  %xmm8, %xmm14
	mulps	%xmm13, %xmm8
	addps	%xmm8,  %xmm0
	MOVUPS_A1(-24 * SIZE, A1, %xmm8)
	pshufd	$0xb1,  %xmm9, %xmm15
	mulps	%xmm13, %xmm9
	addps	%xmm9,  %xmm2
	MOVUPS_A2(-24 * SIZE, A1, LDA, 1, %xmm9)

	mulps	%xmm13, %xmm14
	SUBPS	%xmm14, %xmm1
	mulps	%xmm13, %xmm15
	SUBPS	%xmm15, %xmm3

	pshufd	$0xb1,  %xmm10, %xmm14
	mulps	%xmm13, %xmm10
	addps	%xmm10, %xmm4
	MOVUPS_A1(-24 * SIZE, A2, %xmm10)
	pshufd	$0xb1,  %xmm11, %xmm15
	mulps	%xmm13, %xmm11
	addps	%xmm11, %xmm6
	MOVUPS_A2(-24 * SIZE, A2, LDA, 1, %xmm11)

	mulps	%xmm13, %xmm14
	SUBPS	%xmm14, %xmm5
	mulps	%xmm13, %xmm15
	movaps	-20 * SIZE(X1), %xmm13
	SUBPS	%xmm15, %xmm7

	pshufd	$0xb1,  %xmm8, %xmm14
	mulps	%xmm12, %xmm8
	addps	%xmm8,  %xmm0
	MOVUPS_A1(-20 * SIZE, A1, %xmm8)
	pshufd	$0xb1,  %xmm9, %xmm15
	mulps	%xmm12, %xmm9
	addps	%xmm9,  %xmm2
	MOVUPS_A2(-20 * SIZE, A1, LDA, 1, %xmm9)

	mulps	%xmm12, %xmm14
	SUBPS	%xmm14, %xmm1
	mulps	%xmm12, %xmm15
	SUBPS	%xmm15, %xmm3

	pshufd	$0xb1,  %xmm10, %xmm14
	mulps	%xmm12, %xmm10
	addps	%xmm10, %xmm4
	MOVUPS_A1(-20 * SIZE, A2, %xmm10)
	pshufd	$0xb1,  %xmm11, %xmm15
	mulps	%xmm12, %xmm11
	addps	%xmm11, %xmm6
	MOVUPS_A2(-20 * SIZE, A2, LDA, 1, %xmm11)

	mulps	%xmm12, %xmm14
	SUBPS	%xmm14, %xmm5
	mulps	%xmm12, %xmm15
	movaps	-16 * SIZE(X1), %xmm12
	SUBPS	%xmm15, %xmm7

	pshufd	$0xb1,  %xmm8, %xmm14
	mulps	%xmm13, %xmm8
	addps	%xmm8,  %xmm0
	pshufd	$0xb1,  %xmm9, %xmm15
	mulps	%xmm13, %xmm9
	addps	%xmm9,  %xmm2

	mulps	%xmm13, %xmm14
	SUBPS	%xmm14, %xmm1
	mulps	%xmm13, %xmm15
	SUBPS	%xmm15, %xmm3

	pshufd	$0xb1,  %xmm10, %xmm14
	mulps	%xmm13, %xmm10
	addps	%xmm10, %xmm4
	pshufd	$0xb1,  %xmm11, %xmm15
	mulps	%xmm13, %xmm11
	addps	%xmm11, %xmm6

	mulps	%xmm13, %xmm14
	SUBPS	%xmm14, %xmm5
	mulps	%xmm13, %xmm15
	movaps	-12 * SIZE(X1), %xmm13
	SUBPS	%xmm15, %xmm7

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, A2
	subq	 $-16 * SIZE, X1
	ALIGN_3

.L15:
	testq	$4, MM
	je	.L17

	MOVUPS_A1(-32 * SIZE, A1, %xmm8)
	MOVUPS_A2(-32 * SIZE, A1, LDA, 1, %xmm9)
	MOVUPS_A1(-32 * SIZE, A2, %xmm10)
	MOVUPS_A2(-32 * SIZE, A2, LDA, 1, %xmm11)

	pshufd	$0xb1,  %xmm8, %xmm14
	mulps	%xmm12, %xmm8
	addps	%xmm8,  %xmm0
	MOVUPS_A1(-28 * SIZE, A1, %xmm8)
	pshufd	$0xb1,  %xmm9, %xmm15
	mulps	%xmm12, %xmm9
	addps	%xmm9,  %xmm2
	MOVUPS_A2(-28 * SIZE, A1, LDA, 1, %xmm9)

	mulps	%xmm12, %xmm14
	SUBPS	%xmm14, %xmm1
	mulps	%xmm12, %xmm15
	SUBPS	%xmm15, %xmm3

	pshufd	$0xb1,  %xmm10, %xmm14
	mulps	%xmm12, %xmm10
	addps	%xmm10, %xmm4
	MOVUPS_A1(-28 * SIZE, A2, %xmm10)
	pshufd	$0xb1,  %xmm11, %xmm15
	mulps	%xmm12, %xmm11
	addps	%xmm11, %xmm6
	MOVUPS_A2(-28 * SIZE, A2, LDA, 1, %xmm11)

	mulps	%xmm12, %xmm14
	SUBPS	%xmm14, %xmm5
	mulps	%xmm12, %xmm15
	movaps	-24 * SIZE(X1), %xmm12
	SUBPS	%xmm15, %xmm7

	pshufd	$0xb1,  %xmm8, %xmm14
	mulps	%xmm13, %xmm8
	addps	%xmm8,  %xmm0
	pshufd	$0xb1,  %xmm9, %xmm15
	mulps	%xmm13, %xmm9
	addps	%xmm9,  %xmm2

	mulps	%xmm13, %xmm14
	SUBPS	%xmm14, %xmm1
	mulps	%xmm13, %xmm15
	SUBPS	%xmm15, %xmm3

	pshufd	$0xb1,  %xmm10, %xmm14
	mulps	%xmm13, %xmm10
	addps	%xmm10, %xmm4
	pshufd	$0xb1,  %xmm11, %xmm15
	mulps	%xmm13, %xmm11
	addps	%xmm11, %xmm6

	mulps	%xmm13, %xmm14
	SUBPS	%xmm14, %xmm5
	mulps	%xmm13, %xmm15
	movaps	-20 * SIZE(X1), %xmm13
	SUBPS	%xmm15, %xmm7

	addq	 $8 * SIZE, A1
	addq	 $8 * SIZE, A2
	addq	 $8 * SIZE, X1
	ALIGN_3

.L17:
	testq	$2, MM
	je	.L18

	MOVUPS_A1(-32 * SIZE, A1, %xmm8)
	MOVUPS_A2(-32 * SIZE, A1, LDA, 1, %xmm9)
	MOVUPS_A1(-32 * SIZE, A2, %xmm10)
	MOVUPS_A2(-32 * SIZE, A2, LDA, 1, %xmm11)

	pshufd	$0xb1,  %xmm8, %xmm14
	mulps	%xmm12, %xmm8
	addps	%xmm8,  %xmm0
	pshufd	$0xb1,  %xmm9, %xmm15
	mulps	%xmm12, %xmm9
	addps	%xmm9,  %xmm2

	mulps	%xmm12, %xmm14
	SUBPS	%xmm14, %xmm1
	mulps	%xmm12, %xmm15
	SUBPS	%xmm15, %xmm3

	pshufd	$0xb1,  %xmm10, %xmm14
	mulps	%xmm12, %xmm10
	addps	%xmm10, %xmm4
	pshufd	$0xb1,  %xmm11, %xmm15
	mulps	%xmm12, %xmm11
	addps	%xmm11, %xmm6

	mulps	%xmm12, %xmm14
	SUBPS	%xmm14, %xmm5
	mulps	%xmm12, %xmm15
	SUBPS	%xmm15, %xmm7

	movaps	%xmm13, %xmm12

	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, A2
	ALIGN_3

.L18:
	testq	$1, MM
	je	.L19

#ifdef movsd
	xorps	%xmm8, %xmm8
#endif
	movsd	-32 * SIZE(A1), %xmm8
#ifdef movsd
	xorps	%xmm9, %xmm9
#endif
	movsd	-32 * SIZE(A1, LDA), %xmm9
#ifdef movsd
	xorps	%xmm10, %xmm10
#endif
	movsd	-32 * SIZE(A2), %xmm10
#ifdef movsd
	xorps	%xmm11, %xmm11
#endif
	movsd	-32 * SIZE(A2, LDA), %xmm11

	pshufd	$0xb1,  %xmm8, %xmm14
	mulps	%xmm12, %xmm8
	addps	%xmm8,  %xmm0
	pshufd	$0xb1,  %xmm9, %xmm15
	mulps	%xmm12, %xmm9
	addps	%xmm9,  %xmm2

	mulps	%xmm12, %xmm14
	SUBPS	%xmm14, %xmm1
	mulps	%xmm12, %xmm15
	SUBPS	%xmm15, %xmm3

	pshufd	$0xb1,  %xmm10, %xmm14
	mulps	%xmm12, %xmm10
	addps	%xmm10, %xmm4
	pshufd	$0xb1,  %xmm11, %xmm15
	mulps	%xmm12, %xmm11
	addps	%xmm11, %xmm6

	mulps	%xmm12, %xmm14
	SUBPS	%xmm14, %xmm5
	mulps	%xmm12, %xmm15
	SUBPS	%xmm15, %xmm7
	ALIGN_3

.L19:
	pcmpeqb	%xmm11, %xmm11
	psllq	$63,    %xmm11

#if (!defined(CONJ) && !defined(XCONJ)) || (defined(CONJ) && defined(XCONJ))
	xorps	%xmm11, %xmm0
	xorps	%xmm11, %xmm2
	xorps	%xmm11, %xmm4
	xorps	%xmm11, %xmm6
#else
	xorps	%xmm11, %xmm1
	xorps	%xmm11, %xmm3
	xorps	%xmm11, %xmm5
	xorps	%xmm11, %xmm7
#endif

#ifdef HAVE_SSE3
	haddps	%xmm1, %xmm0
	haddps	%xmm3, %xmm2
	haddps	%xmm2, %xmm0

	haddps	%xmm5, %xmm4
	haddps	%xmm7, %xmm6
	haddps	%xmm6, %xmm4
#else

	movaps	%xmm0, %xmm8
	unpcklps %xmm1, %xmm0
	unpckhps %xmm1, %xmm8

	movaps	%xmm2, %xmm9
	unpcklps %xmm3, %xmm2
	unpckhps %xmm3, %xmm9

	movaps	%xmm4, %xmm10
	unpcklps %xmm5, %xmm4
	unpckhps %xmm5, %xmm10

	movaps	%xmm6, %xmm12
	unpcklps %xmm7, %xmm6
	unpckhps %xmm7, %xmm12

	addps	%xmm8,  %xmm0
	addps	%xmm9,  %xmm2
	addps	%xmm10, %xmm4
	addps	%xmm12, %xmm6

	movhlps	%xmm0, %xmm1
	movhlps	%xmm2, %xmm3
	movhlps	%xmm4, %xmm5
	movhlps	%xmm6, %xmm7

	addps	%xmm1, %xmm0
	addps	%xmm3, %xmm2
	addps	%xmm5, %xmm4
	addps	%xmm7, %xmm6

	movlhps %xmm2, %xmm0
	movlhps %xmm6, %xmm4
#endif

	pshufd	$0xb1, %xmm0, %xmm1
	pshufd	$0xb1, %xmm4, %xmm5

#ifdef HAVE_SSE3
	movddup	ALPHA, %xmm15
#else
	movsd	ALPHA, %xmm15
	pshufd	$0x44, %xmm15, %xmm15
#endif

	mulps	%xmm15, %xmm0
	mulps	%xmm15, %xmm1
	mulps	%xmm15, %xmm4
	mulps	%xmm15, %xmm5

	xorps	%xmm11, %xmm0
	xorps	%xmm11, %xmm4

#ifdef HAVE_SSE3
	haddps	%xmm1, %xmm0
	haddps	%xmm5, %xmm4
#else
	movaps	%xmm0, %xmm2
	shufps	$0x88, %xmm1, %xmm0
	shufps	$0xdd, %xmm1, %xmm2

	movaps	%xmm4, %xmm6
	shufps	$0x88, %xmm5, %xmm4
	shufps	$0xdd, %xmm5, %xmm6

	addps	%xmm2, %xmm0
	addps	%xmm6, %xmm4
#endif

	movsd	(Y), %xmm2
	addq	INCY, Y
	movhps	(Y), %xmm2
	addq	INCY, Y
	movsd	(Y), %xmm6
	addq	INCY, Y
	movhps	(Y), %xmm6
	addq	INCY, Y

	shufps	$0xd8, %xmm0, %xmm0
	shufps	$0xd8, %xmm4, %xmm4

	addps	%xmm2, %xmm0
	addps	%xmm6, %xmm4

	movlps	%xmm0, (Y1)
	addq	INCY, Y1
	movhps	%xmm0, (Y1)
	addq	INCY, Y1
	movlps	%xmm4, (Y1)
	addq	INCY, Y1
	movhps	%xmm4, (Y1)
	addq	INCY, Y1

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

	leaq	32 * SIZE(BUFFER), X1

	movq	A, A1
	leaq	(A1, LDA), A2
	leaq	(A1, LDA, 2), A

	xorps	%xmm0, %xmm0
	xorps	%xmm1, %xmm1
	xorps	%xmm2, %xmm2
	xorps	%xmm3, %xmm3

#ifdef ALIGNED_ACCESS
	cmpq	M, MM
	je	.L2X

#ifdef movsd
	xorps	%xmm8, %xmm8
#endif
	movsd	-32 * SIZE(A1), %xmm8
#ifdef movsd
	xorps	%xmm9, %xmm9
#endif
	movsd	-32 * SIZE(A2), %xmm9

#ifdef movsd
	xorps	%xmm12, %xmm12
#endif
	movsd	-32 * SIZE(X1), %xmm12

	pshufd	$0xb1, %xmm8,  %xmm4
	mulps	%xmm12,  %xmm8
	addps	%xmm8,  %xmm0
	mulps	%xmm12,  %xmm4
	SUBPS	%xmm4,  %xmm1

	pshufd	$0xb1, %xmm9, %xmm5
	mulps	%xmm12,  %xmm9
	addps	%xmm9, %xmm2
	mulps	%xmm12,  %xmm5
	SUBPS	%xmm5, %xmm3

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, X1
	ALIGN_3
.L2X:
#endif

	movaps	-32 * SIZE(X1), %xmm12
	movaps	-28 * SIZE(X1), %xmm13

#if (GEMV_UNROLL == 2) && defined(PREFETCHW)
	PREFETCHW	3 * SIZE(Y1)
#endif

	movq	MM,  I
	sarq	$3,  I
	jle	.L25

	MOVUPS_A1(-32 * SIZE, A1, %xmm8)
	MOVUPS_A1(-32 * SIZE, A2, %xmm9)
	MOVUPS_A1(-28 * SIZE, A1, %xmm10)
	MOVUPS_A1(-28 * SIZE, A2, %xmm11)

	decq	 I
	jle	 .L24
	ALIGN_3

.L23:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(A1)
#endif

	pshufd	$0xb1, %xmm8,  %xmm4
	mulps	%xmm12,  %xmm8
	addps	%xmm8,  %xmm0
	MOVUPS_A1(-24 * SIZE, A1, %xmm8)
	mulps	%xmm12,  %xmm4
	SUBPS	%xmm4,  %xmm1

	pshufd	$0xb1, %xmm9, %xmm5
	mulps	%xmm12,  %xmm9
	addps	%xmm9, %xmm2
	MOVUPS_A1(-24 * SIZE, A2, %xmm9)
	mulps	%xmm12,  %xmm5
	SUBPS	%xmm5, %xmm3

	movaps	-24 * SIZE(X1), %xmm12

	pshufd	$0xb1, %xmm10,  %xmm6
	mulps	%xmm13,  %xmm10
	addps	%xmm10,  %xmm0
	MOVUPS_A1(-20 * SIZE, A1, %xmm10)
	mulps	%xmm13,  %xmm6
	SUBPS	%xmm6,  %xmm1

	pshufd	$0xb1, %xmm11, %xmm7
	mulps	%xmm13,  %xmm11
	addps	%xmm11, %xmm2
	MOVUPS_A1(-20 * SIZE, A2, %xmm11)
	mulps	%xmm13,  %xmm7
	SUBPS	%xmm7, %xmm3

	movaps	-20 * SIZE(X1), %xmm13

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(A2)
#endif

	pshufd	$0xb1, %xmm8,  %xmm4
	mulps	%xmm12,  %xmm8
	addps	%xmm8,  %xmm0
	MOVUPS_A1(-16 * SIZE, A1, %xmm8)
	mulps	%xmm12,  %xmm4
	SUBPS	%xmm4,  %xmm1

	pshufd	$0xb1, %xmm9, %xmm5
	mulps	%xmm12,  %xmm9
	addps	%xmm9, %xmm2
	MOVUPS_A1(-16 * SIZE, A2, %xmm9)
	mulps	%xmm12,  %xmm5
	SUBPS	%xmm5, %xmm3

	movaps	-16 * SIZE(X1), %xmm12

#ifdef PREFETCHW
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(X1)
#endif

	pshufd	$0xb1, %xmm10,  %xmm6
	mulps	%xmm13,  %xmm10
	addps	%xmm10,  %xmm0
	MOVUPS_A1(-12 * SIZE, A1, %xmm10)
	mulps	%xmm13,  %xmm6
	SUBPS	%xmm6,  %xmm1

	pshufd	$0xb1, %xmm11, %xmm7
	mulps	%xmm13,  %xmm11
	addps	%xmm11, %xmm2
	MOVUPS_A1(-12 * SIZE, A2, %xmm11)
	mulps	%xmm13,  %xmm7
	SUBPS	%xmm7, %xmm3

	movaps	-12 * SIZE(X1), %xmm13

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, A2
	subq	 $-16 * SIZE, X1

	subq	 $1, I
	BRANCH
	jg	.L23
	ALIGN_3

.L24:
	pshufd	$0xb1, %xmm8,  %xmm4
	mulps	%xmm12,  %xmm8
	addps	%xmm8,  %xmm0
	MOVUPS_A1(-24 * SIZE, A1, %xmm8)
	mulps	%xmm12,  %xmm4
	SUBPS	%xmm4,  %xmm1

	pshufd	$0xb1, %xmm9, %xmm5
	mulps	%xmm12,  %xmm9
	addps	%xmm9, %xmm2
	MOVUPS_A1(-24 * SIZE, A2, %xmm9)
	mulps	%xmm12,  %xmm5
	SUBPS	%xmm5, %xmm3

	movaps	-24 * SIZE(X1), %xmm12

	pshufd	$0xb1, %xmm10,  %xmm6
	mulps	%xmm13,  %xmm10
	addps	%xmm10,  %xmm0
	MOVUPS_A1(-20 * SIZE, A1, %xmm10)
	mulps	%xmm13,  %xmm6
	SUBPS	%xmm6,  %xmm1

	pshufd	$0xb1, %xmm11, %xmm7
	mulps	%xmm13,  %xmm11
	addps	%xmm11, %xmm2
	MOVUPS_A1(-20 * SIZE, A2, %xmm11)
	mulps	%xmm13,  %xmm7
	SUBPS	%xmm7, %xmm3

	movaps	-20 * SIZE(X1), %xmm13

	pshufd	$0xb1, %xmm8,  %xmm4
	mulps	%xmm12,  %xmm8
	addps	%xmm8,  %xmm0
	mulps	%xmm12,  %xmm4
	SUBPS	%xmm4,  %xmm1

	pshufd	$0xb1, %xmm9, %xmm5
	mulps	%xmm12,  %xmm9
	addps	%xmm9, %xmm2
	mulps	%xmm12,  %xmm5
	SUBPS	%xmm5, %xmm3

	movaps	-16 * SIZE(X1), %xmm12

	pshufd	$0xb1, %xmm10,  %xmm6
	mulps	%xmm13,  %xmm10
	addps	%xmm10,  %xmm0
	mulps	%xmm13,  %xmm6
	SUBPS	%xmm6,  %xmm1

	pshufd	$0xb1, %xmm11, %xmm7
	mulps	%xmm13,  %xmm11
	addps	%xmm11, %xmm2
	mulps	%xmm13,  %xmm7
	SUBPS	%xmm7, %xmm3

	movaps	-12 * SIZE(X1), %xmm13

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, A2
	subq	 $-16 * SIZE, X1
	ALIGN_3

.L25:
	testq	$4, MM
	je	.L27

	MOVUPS_A1(-32 * SIZE, A1, %xmm8)
	MOVUPS_A1(-32 * SIZE, A2, %xmm9)
	MOVUPS_A1(-28 * SIZE, A1, %xmm10)
	MOVUPS_A1(-28 * SIZE, A2, %xmm11)

	pshufd	$0xb1, %xmm8,  %xmm4
	mulps	%xmm12,  %xmm8
	addps	%xmm8,  %xmm0
	mulps	%xmm12,  %xmm4
	SUBPS	%xmm4,  %xmm1

	pshufd	$0xb1, %xmm9, %xmm5
	mulps	%xmm12,  %xmm9
	addps	%xmm9, %xmm2
	mulps	%xmm12,  %xmm5
	SUBPS	%xmm5, %xmm3

	movaps	-24 * SIZE(X1), %xmm12

	pshufd	$0xb1, %xmm10,  %xmm6
	mulps	%xmm13,  %xmm10
	addps	%xmm10,  %xmm0
	mulps	%xmm13,  %xmm6
	SUBPS	%xmm6,  %xmm1

	pshufd	$0xb1, %xmm11, %xmm7
	mulps	%xmm13,  %xmm11
	addps	%xmm11, %xmm2
	mulps	%xmm13,  %xmm7
	SUBPS	%xmm7, %xmm3

	movaps	-20 * SIZE(X1), %xmm13

	addq	 $8 * SIZE, A1
	addq	 $8 * SIZE, A2
	ALIGN_3

.L27:
	testq	$2, MM
	je	.L28

	MOVUPS_A1(-32 * SIZE, A1, %xmm8)
	MOVUPS_A1(-32 * SIZE, A2, %xmm9)

	pshufd	$0xb1, %xmm8,  %xmm4
	mulps	%xmm12,  %xmm8
	addps	%xmm8,  %xmm0
	mulps	%xmm12,  %xmm4
	SUBPS	%xmm4,  %xmm1

	pshufd	$0xb1, %xmm9, %xmm5
	mulps	%xmm12,  %xmm9
	addps	%xmm9, %xmm2
	mulps	%xmm12,  %xmm5
	SUBPS	%xmm5, %xmm3

	movaps	%xmm13, %xmm12

	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, A2
	ALIGN_3

.L28:
	testq	$1, MM
	je	.L29

#ifdef movsd
	xorps	%xmm8, %xmm8
#endif
	movsd	-32 * SIZE(A1), %xmm8
#ifdef movsd
	xorps	%xmm9, %xmm9
#endif
	movsd	-32 * SIZE(A2), %xmm9

	pshufd	$0xb1, %xmm8,  %xmm4
	mulps	%xmm12,  %xmm8
	addps	%xmm8,  %xmm0
	mulps	%xmm12,  %xmm4
	SUBPS	%xmm4,  %xmm1

	pshufd	$0xb1, %xmm9, %xmm5
	mulps	%xmm12,  %xmm9
	addps	%xmm9, %xmm2
	mulps	%xmm12,  %xmm5
	SUBPS	%xmm5, %xmm3
	ALIGN_3

.L29:
	pcmpeqb	%xmm5, %xmm5
	psllq	$63,    %xmm5

#if (!defined(CONJ) && !defined(XCONJ)) || (defined(CONJ) && defined(XCONJ))
	xorps	%xmm5, %xmm0
	xorps	%xmm5, %xmm2
#else
	xorps	%xmm5, %xmm1
	xorps	%xmm5, %xmm3
#endif

#ifdef HAVE_SSE3
	haddps	%xmm1, %xmm0
	haddps	%xmm3, %xmm2
	haddps	%xmm2, %xmm0
#else
	movaps	%xmm0, %xmm8
	unpcklps %xmm1, %xmm0
	unpckhps %xmm1, %xmm8

	movaps	%xmm2, %xmm4
	unpcklps %xmm3, %xmm2
	unpckhps %xmm3, %xmm4

	addps	%xmm8, %xmm0
	addps	%xmm4, %xmm2

	movhlps	%xmm0, %xmm1
	movhlps	%xmm2, %xmm3

	addps	%xmm1, %xmm0
	addps	%xmm3, %xmm2
	movlhps %xmm2, %xmm0
#endif

	pshufd	$0xb1, %xmm0, %xmm1

#ifdef HAVE_SSE3
	movddup	ALPHA, %xmm15
#else
	movsd	ALPHA, %xmm15
	pshufd	$0x44, %xmm15, %xmm15
#endif

	mulps	%xmm15, %xmm0
	mulps	%xmm15, %xmm1

	xorps	%xmm5, %xmm0

#ifdef HAVE_SSE3
	haddps	%xmm1, %xmm0
#else
	movaps	%xmm0, %xmm2
	shufps	$0x88, %xmm1, %xmm0
	shufps	$0xdd, %xmm1, %xmm2

	addps	%xmm2, %xmm0
#endif

	movsd	(Y), %xmm12
	addq	INCY, Y
	movhps	(Y), %xmm12
	addq	INCY, Y

	shufps	$0xd8, %xmm0, %xmm0

	addps	%xmm12, %xmm0

	movlps	%xmm0, (Y1)
	addq	INCY, Y1
	movhps	%xmm0, (Y1)
	addq	INCY, Y1

#if GEMV_UNROLL == 2
	cmpq	$2, N
	jge	.L21
#endif
	ALIGN_3

.L30:
	cmpq	$1, N
	jl	.L999

	leaq	32 * SIZE(BUFFER), X1

	movq	A, A1

	xorps	%xmm0, %xmm0
	xorps	%xmm1, %xmm1

#ifdef ALIGNED_ACCESS
	cmpq	M, MM
	je	.L3X

#ifdef movsd
	xorps	%xmm8, %xmm8
#endif
	movsd	-32 * SIZE(A1), %xmm8
#ifdef movsd
	xorps	%xmm12, %xmm12
#endif
	movsd	-32 * SIZE(X1), %xmm12

	pshufd	$0xb1, %xmm8,  %xmm4
	mulps	%xmm12,  %xmm8
	addps	%xmm8,  %xmm0
	mulps	%xmm12,  %xmm4
	SUBPS	%xmm4,  %xmm1

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, X1
	ALIGN_3
.L3X:
#endif

	movaps	-32 * SIZE(X1), %xmm12
	movaps	-28 * SIZE(X1), %xmm13

	movq	MM,  I
	sarq	$3,  I
	jle	.L35

	MOVUPS_A1(-32 * SIZE, A1, %xmm8)
	MOVUPS_A1(-28 * SIZE, A1, %xmm10)

	decq	 I
	jle	 .L34
	ALIGN_3

.L33:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 4 - 128 + PREOFFSET(A1)
#endif

	pshufd	$0xb1, %xmm8,  %xmm4
	mulps	%xmm12,  %xmm8
	addps	%xmm8,  %xmm0
	MOVUPS_A1(-24 * SIZE, A1, %xmm8)
	mulps	%xmm12,  %xmm4
	SUBPS	%xmm4,  %xmm1

	movaps	-24 * SIZE(X1), %xmm12

	pshufd	$0xb1, %xmm10,  %xmm6
	mulps	%xmm13,  %xmm10
	addps	%xmm10,  %xmm0
	MOVUPS_A1(-20 * SIZE, A1, %xmm10)
	mulps	%xmm13,  %xmm6
	SUBPS	%xmm6,  %xmm1

	movaps	-20 * SIZE(X1), %xmm13

	pshufd	$0xb1, %xmm8,  %xmm4
	mulps	%xmm12,  %xmm8
	addps	%xmm8,  %xmm0
	MOVUPS_A1(-16 * SIZE, A1, %xmm8)
	mulps	%xmm12,  %xmm4
	SUBPS	%xmm4,  %xmm1

	movaps	-16 * SIZE(X1), %xmm12

#ifdef PREFETCHW
	PREFETCH	(PREFETCHSIZE) * 4 - 128 + PREOFFSET(X1)
#endif

	pshufd	$0xb1, %xmm10,  %xmm6
	mulps	%xmm13,  %xmm10
	addps	%xmm10,  %xmm0
	MOVUPS_A1(-12 * SIZE, A1, %xmm10)
	mulps	%xmm13,  %xmm6
	SUBPS	%xmm6,  %xmm1

	movaps	-12 * SIZE(X1), %xmm13

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, X1

	subq	 $1, I
	BRANCH
	jg	.L33
	ALIGN_3

.L34:
	pshufd	$0xb1, %xmm8,  %xmm4
	mulps	%xmm12,  %xmm8
	addps	%xmm8,  %xmm0
	MOVUPS_A1(-24 * SIZE, A1, %xmm8)
	mulps	%xmm12,  %xmm4
	SUBPS	%xmm4,  %xmm1

	movaps	-24 * SIZE(X1), %xmm12

	pshufd	$0xb1, %xmm10,  %xmm6
	mulps	%xmm13,  %xmm10
	addps	%xmm10,  %xmm0
	MOVUPS_A1(-20 * SIZE, A1, %xmm10)
	mulps	%xmm13,  %xmm6
	SUBPS	%xmm6,  %xmm1

	movaps	-20 * SIZE(X1), %xmm13

	pshufd	$0xb1, %xmm8,  %xmm4
	mulps	%xmm12,  %xmm8
	addps	%xmm8,  %xmm0
	mulps	%xmm12,  %xmm4
	SUBPS	%xmm4,  %xmm1

	movaps	-16 * SIZE(X1), %xmm12

	pshufd	$0xb1, %xmm10,  %xmm6
	mulps	%xmm13,  %xmm10
	addps	%xmm10,  %xmm0
	mulps	%xmm13,  %xmm6
	SUBPS	%xmm6,  %xmm1

	movaps	-12 * SIZE(X1), %xmm13

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, X1
	ALIGN_3

.L35:
	testq	$4, MM
	je	.L37

	MOVUPS_A1(-32 * SIZE, A1, %xmm8)
	MOVUPS_A1(-28 * SIZE, A1, %xmm10)

	pshufd	$0xb1, %xmm8,  %xmm4
	mulps	%xmm12,  %xmm8
	addps	%xmm8,  %xmm0
	mulps	%xmm12,  %xmm4
	SUBPS	%xmm4,  %xmm1

	movaps	-24 * SIZE(X1), %xmm12

	pshufd	$0xb1, %xmm10,  %xmm6
	mulps	%xmm13,  %xmm10
	addps	%xmm10,  %xmm0
	mulps	%xmm13,  %xmm6
	SUBPS	%xmm6,  %xmm1

	movaps	-20 * SIZE(X1), %xmm13

	addq	 $8 * SIZE, A1
	ALIGN_3

.L37:
	testq	$2, MM
	je	.L38

	MOVUPS_A1(-32 * SIZE, A1, %xmm8)

	pshufd	$0xb1, %xmm8,  %xmm4
	mulps	%xmm12,  %xmm8
	addps	%xmm8,  %xmm0
	mulps	%xmm12,  %xmm4
	SUBPS	%xmm4,  %xmm1

	movaps	%xmm13, %xmm12

	addq	 $4 * SIZE, A1
	ALIGN_3

.L38:
	testq	$1, MM
	je	.L39

#ifdef movsd
	xorps	%xmm8, %xmm8
#endif
	movsd	-32 * SIZE(A1), %xmm8

	pshufd	$0xb1, %xmm8,  %xmm4
	mulps	%xmm12,  %xmm8
	addps	%xmm8,  %xmm0
	mulps	%xmm12,  %xmm4
	SUBPS	%xmm4,  %xmm1
	ALIGN_3

.L39:
	pcmpeqb	%xmm5, %xmm5
	psllq	$63,    %xmm5

#if (!defined(CONJ) && !defined(XCONJ)) || (defined(CONJ) && defined(XCONJ))
	xorps	%xmm5, %xmm0
#else
	xorps	%xmm5, %xmm1
#endif

#ifdef HAVE_SSE3
	haddps	%xmm1, %xmm0
	haddps	%xmm0, %xmm0
#else
	movaps	%xmm0, %xmm8
	unpcklps %xmm1, %xmm0
	unpckhps %xmm1, %xmm8

	addps	%xmm8, %xmm0

	movhlps	%xmm0, %xmm1

	addps	%xmm1, %xmm0
#endif

	pshufd	$0xb1, %xmm0, %xmm1

#ifdef HAVE_SSE3
	movddup	ALPHA, %xmm15
#else
	movsd	ALPHA, %xmm15
	pshufd	$0x44, %xmm15, %xmm15
#endif

	mulps	%xmm15, %xmm0
	mulps	%xmm15, %xmm1

	xorps	%xmm5, %xmm0

#ifdef HAVE_SSE3
	haddps	%xmm1, %xmm0
#else
	movaps	%xmm0, %xmm2
	shufps	$0x88, %xmm1, %xmm0
	shufps	$0xdd, %xmm1, %xmm2

	addps	%xmm2, %xmm0
#endif

	movsd	(Y), %xmm12
	addq	INCY, Y

	shufps	$0xd8, %xmm0, %xmm0

	addps	%xmm12, %xmm0

	movlps	%xmm0, (Y1)
	addq	INCY, Y1
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

	leaq	32 * SIZE(BUFFER), X1

	movq	A, A1
	leaq	(A1, LDA, 2), A2
	leaq	(A1, LDA, 4), A

	xorps	%xmm0, %xmm0
	xorps	%xmm1, %xmm1
	xorps	%xmm2, %xmm2
	xorps	%xmm3, %xmm3
	xorps	%xmm4, %xmm4
	xorps	%xmm5, %xmm5
	xorps	%xmm6, %xmm6
	xorps	%xmm7, %xmm7

#ifdef ALIGNED_ACCESS
	cmpq	M, MM
	je	.L10X

#ifdef movsd
	xorps	%xmm8, %xmm8
#endif
	movsd	-32 * SIZE(A1), %xmm8
#ifdef movsd
	xorps	%xmm9, %xmm9
#endif
	movsd	-32 * SIZE(A1, LDA), %xmm9
#ifdef movsd
	xorps	%xmm10, %xmm10
#endif
	movsd	-32 * SIZE(A2), %xmm10
#ifdef movsd
	xorps	%xmm11, %xmm11
#endif
	movsd	-32 * SIZE(A2, LDA), %xmm11

#ifdef movsd
	xorps	%xmm12, %xmm12
#endif
	movsd	-32 * SIZE(X1), %xmm12

	pshufd	$0xb1,  %xmm8, %xmm14
	mulps	%xmm12, %xmm8
	addps	%xmm8,  %xmm0
	pshufd	$0xb1,  %xmm9, %xmm15
	mulps	%xmm12, %xmm9
	addps	%xmm9,  %xmm2

	mulps	%xmm12, %xmm14
	SUBPS	%xmm14, %xmm1
	mulps	%xmm12, %xmm15
	SUBPS	%xmm15, %xmm3

	pshufd	$0xb1,  %xmm10, %xmm14
	mulps	%xmm12, %xmm10
	addps	%xmm10, %xmm4
	pshufd	$0xb1,  %xmm11, %xmm15
	mulps	%xmm12, %xmm11
	addps	%xmm11, %xmm6

	mulps	%xmm12, %xmm14
	SUBPS	%xmm14, %xmm5
	mulps	%xmm12, %xmm15
	SUBPS	%xmm15, %xmm7

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, X1
	ALIGN_3
.L10X:
#endif

	movaps	-32 * SIZE(X1), %xmm12
	movaps	-28 * SIZE(X1), %xmm13

#ifdef PREFETCHW
	PREFETCHW	7 * SIZE(Y1)
#endif

	movq	MM,  I
	sarq	$3,  I
	jle	.L105

	movaps	-32 * SIZE(A1), %xmm8
	movsd	-32 * SIZE(A1, LDA), %xmm9
	movhps	-30 * SIZE(A1, LDA), %xmm9

	movaps	-32 * SIZE(A2), %xmm10
	movsd	-32 * SIZE(A2, LDA), %xmm11
	movhps	-30 * SIZE(A2, LDA), %xmm11

	decq	 I
	jle	 .L104
	ALIGN_3

.L103:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A1)
#endif

	pshufd	$0xb1,  %xmm8, %xmm14
	mulps	%xmm12, %xmm8
	addps	%xmm8,  %xmm0
	movaps	-28 * SIZE(A1), %xmm8
	pshufd	$0xb1,  %xmm9, %xmm15
	mulps	%xmm12, %xmm9
	addps	%xmm9,  %xmm2
	movsd	-28 * SIZE(A1, LDA), %xmm9
	movhps	-26 * SIZE(A1, LDA), %xmm9

	mulps	%xmm12, %xmm14
	SUBPS	%xmm14, %xmm1
	mulps	%xmm12, %xmm15
	SUBPS	%xmm15, %xmm3

	pshufd	$0xb1,  %xmm10, %xmm14
	mulps	%xmm12, %xmm10
	addps	%xmm10, %xmm4
	movaps	-28 * SIZE(A2), %xmm10
	pshufd	$0xb1,  %xmm11, %xmm15
	mulps	%xmm12, %xmm11
	addps	%xmm11, %xmm6
	movsd	-28 * SIZE(A2, LDA), %xmm11
	movhps	-26 * SIZE(A2, LDA), %xmm11

	mulps	%xmm12, %xmm14
	SUBPS	%xmm14, %xmm5
	mulps	%xmm12, %xmm15
	movaps	-24 * SIZE(X1), %xmm12
	SUBPS	%xmm15, %xmm7

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A1, LDA)
#endif

	pshufd	$0xb1,  %xmm8, %xmm14
	mulps	%xmm13, %xmm8
	addps	%xmm8,  %xmm0
	movaps	-24 * SIZE(A1), %xmm8
	pshufd	$0xb1,  %xmm9, %xmm15
	mulps	%xmm13, %xmm9
	addps	%xmm9,  %xmm2
	movsd	-24 * SIZE(A1, LDA), %xmm9
	movhps	-22 * SIZE(A1, LDA), %xmm9

	mulps	%xmm13, %xmm14
	SUBPS	%xmm14, %xmm1
	mulps	%xmm13, %xmm15
	SUBPS	%xmm15, %xmm3

	pshufd	$0xb1,  %xmm10, %xmm14
	mulps	%xmm13, %xmm10
	addps	%xmm10, %xmm4
	movaps	-24 * SIZE(A2), %xmm10
	pshufd	$0xb1,  %xmm11, %xmm15
	mulps	%xmm13, %xmm11
	addps	%xmm11, %xmm6
	movsd	-24 * SIZE(A2, LDA), %xmm11
	movhps	-22 * SIZE(A2, LDA), %xmm11

	mulps	%xmm13, %xmm14
	SUBPS	%xmm14, %xmm5
	mulps	%xmm13, %xmm15
	movaps	-20 * SIZE(X1), %xmm13
	SUBPS	%xmm15, %xmm7

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A2)
#endif

	pshufd	$0xb1,  %xmm8, %xmm14
	mulps	%xmm12, %xmm8
	addps	%xmm8,  %xmm0
	movaps	-20 * SIZE(A1), %xmm8
	pshufd	$0xb1,  %xmm9, %xmm15
	mulps	%xmm12, %xmm9
	addps	%xmm9,  %xmm2
	movsd	-20 * SIZE(A1, LDA), %xmm9
	movhps	-18 * SIZE(A1, LDA), %xmm9

	mulps	%xmm12, %xmm14
	SUBPS	%xmm14, %xmm1
	mulps	%xmm12, %xmm15
	SUBPS	%xmm15, %xmm3

	pshufd	$0xb1,  %xmm10, %xmm14
	mulps	%xmm12, %xmm10
	addps	%xmm10, %xmm4
	movaps	-20 * SIZE(A2), %xmm10
	pshufd	$0xb1,  %xmm11, %xmm15
	mulps	%xmm12, %xmm11
	addps	%xmm11, %xmm6
	movsd	-20 * SIZE(A2, LDA), %xmm11
	movhps	-18 * SIZE(A2, LDA), %xmm11

	mulps	%xmm12, %xmm14
	SUBPS	%xmm14, %xmm5
	mulps	%xmm12, %xmm15
	movaps	-16 * SIZE(X1), %xmm12
	SUBPS	%xmm15, %xmm7

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(A2, LDA)
#endif

	pshufd	$0xb1,  %xmm8, %xmm14
	mulps	%xmm13, %xmm8
	addps	%xmm8,  %xmm0
	movaps	-16 * SIZE(A1), %xmm8
	pshufd	$0xb1,  %xmm9, %xmm15
	mulps	%xmm13, %xmm9
	addps	%xmm9,  %xmm2
	movsd	-16 * SIZE(A1, LDA), %xmm9
	movhps	-14 * SIZE(A1, LDA), %xmm9

	mulps	%xmm13, %xmm14
	SUBPS	%xmm14, %xmm1
	mulps	%xmm13, %xmm15
	SUBPS	%xmm15, %xmm3

#ifdef PREFETCHW
	PREFETCH	(PREFETCHSIZE) - 128 + PREOFFSET(X1)
#endif

	pshufd	$0xb1,  %xmm10, %xmm14
	mulps	%xmm13, %xmm10
	addps	%xmm10, %xmm4
	movaps	-16 * SIZE(A2), %xmm10
	pshufd	$0xb1,  %xmm11, %xmm15
	mulps	%xmm13, %xmm11
	addps	%xmm11, %xmm6
	movsd	-16 * SIZE(A2, LDA), %xmm11
	movhps	-14 * SIZE(A2, LDA), %xmm11

	mulps	%xmm13, %xmm14
	SUBPS	%xmm14, %xmm5
	mulps	%xmm13, %xmm15
	movaps	-12 * SIZE(X1), %xmm13
	SUBPS	%xmm15, %xmm7

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, A2
	subq	 $-16 * SIZE, X1

	subq	 $1, I
	BRANCH
	jg	.L103
	ALIGN_3

.L104:
	pshufd	$0xb1,  %xmm8, %xmm14
	mulps	%xmm12, %xmm8
	addps	%xmm8,  %xmm0
	movaps	-28 * SIZE(A1), %xmm8
	pshufd	$0xb1,  %xmm9, %xmm15
	mulps	%xmm12, %xmm9
	addps	%xmm9,  %xmm2
	movsd	-28 * SIZE(A1, LDA), %xmm9
	movhps	-26 * SIZE(A1, LDA), %xmm9

	mulps	%xmm12, %xmm14
	SUBPS	%xmm14, %xmm1
	mulps	%xmm12, %xmm15
	SUBPS	%xmm15, %xmm3

	pshufd	$0xb1,  %xmm10, %xmm14
	mulps	%xmm12, %xmm10
	addps	%xmm10, %xmm4
	movaps	-28 * SIZE(A2), %xmm10
	pshufd	$0xb1,  %xmm11, %xmm15
	mulps	%xmm12, %xmm11
	addps	%xmm11, %xmm6
	movsd	-28 * SIZE(A2, LDA), %xmm11
	movhps	-26 * SIZE(A2, LDA), %xmm11

	mulps	%xmm12, %xmm14
	SUBPS	%xmm14, %xmm5
	mulps	%xmm12, %xmm15
	movaps	-24 * SIZE(X1), %xmm12
	SUBPS	%xmm15, %xmm7

	pshufd	$0xb1,  %xmm8, %xmm14
	mulps	%xmm13, %xmm8
	addps	%xmm8,  %xmm0
	movaps	-24 * SIZE(A1), %xmm8
	pshufd	$0xb1,  %xmm9, %xmm15
	mulps	%xmm13, %xmm9
	addps	%xmm9,  %xmm2
	movsd	-24 * SIZE(A1, LDA), %xmm9
	movhps	-22 * SIZE(A1, LDA), %xmm9

	mulps	%xmm13, %xmm14
	SUBPS	%xmm14, %xmm1
	mulps	%xmm13, %xmm15
	SUBPS	%xmm15, %xmm3

	pshufd	$0xb1,  %xmm10, %xmm14
	mulps	%xmm13, %xmm10
	addps	%xmm10, %xmm4
	movaps	-24 * SIZE(A2), %xmm10
	pshufd	$0xb1,  %xmm11, %xmm15
	mulps	%xmm13, %xmm11
	addps	%xmm11, %xmm6
	movsd	-24 * SIZE(A2, LDA), %xmm11
	movhps	-22 * SIZE(A2, LDA), %xmm11

	mulps	%xmm13, %xmm14
	SUBPS	%xmm14, %xmm5
	mulps	%xmm13, %xmm15
	movaps	-20 * SIZE(X1), %xmm13
	SUBPS	%xmm15, %xmm7

	pshufd	$0xb1,  %xmm8, %xmm14
	mulps	%xmm12, %xmm8
	addps	%xmm8,  %xmm0
	movaps	-20 * SIZE(A1), %xmm8
	pshufd	$0xb1,  %xmm9, %xmm15
	mulps	%xmm12, %xmm9
	addps	%xmm9,  %xmm2
	movsd	-20 * SIZE(A1, LDA), %xmm9
	movhps	-18 * SIZE(A1, LDA), %xmm9

	mulps	%xmm12, %xmm14
	SUBPS	%xmm14, %xmm1
	mulps	%xmm12, %xmm15
	SUBPS	%xmm15, %xmm3

	pshufd	$0xb1,  %xmm10, %xmm14
	mulps	%xmm12, %xmm10
	addps	%xmm10, %xmm4
	movaps	-20 * SIZE(A2), %xmm10
	pshufd	$0xb1,  %xmm11, %xmm15
	mulps	%xmm12, %xmm11
	addps	%xmm11, %xmm6
	movsd	-20 * SIZE(A2, LDA), %xmm11
	movhps	-18 * SIZE(A2, LDA), %xmm11

	mulps	%xmm12, %xmm14
	SUBPS	%xmm14, %xmm5
	mulps	%xmm12, %xmm15
	movaps	-16 * SIZE(X1), %xmm12
	SUBPS	%xmm15, %xmm7

	pshufd	$0xb1,  %xmm8, %xmm14
	mulps	%xmm13, %xmm8
	addps	%xmm8,  %xmm0
	pshufd	$0xb1,  %xmm9, %xmm15
	mulps	%xmm13, %xmm9
	addps	%xmm9,  %xmm2

	mulps	%xmm13, %xmm14
	SUBPS	%xmm14, %xmm1
	mulps	%xmm13, %xmm15
	SUBPS	%xmm15, %xmm3

	pshufd	$0xb1,  %xmm10, %xmm14
	mulps	%xmm13, %xmm10
	addps	%xmm10, %xmm4
	pshufd	$0xb1,  %xmm11, %xmm15
	mulps	%xmm13, %xmm11
	addps	%xmm11, %xmm6

	mulps	%xmm13, %xmm14
	SUBPS	%xmm14, %xmm5
	mulps	%xmm13, %xmm15
	movaps	-12 * SIZE(X1), %xmm13
	SUBPS	%xmm15, %xmm7

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, A2
	subq	 $-16 * SIZE, X1
	ALIGN_3

.L105:
	testq	$4, MM
	je	.L107

	movaps	-32 * SIZE(A1), %xmm8
	movsd	-32 * SIZE(A1, LDA), %xmm9
	movhps	-30 * SIZE(A1, LDA), %xmm9

	movaps	-32 * SIZE(A2), %xmm10
	movsd	-32 * SIZE(A2, LDA), %xmm11
	movhps	-30 * SIZE(A2, LDA), %xmm11

	pshufd	$0xb1,  %xmm8, %xmm14
	mulps	%xmm12, %xmm8
	addps	%xmm8,  %xmm0
	movaps	-28 * SIZE(A1), %xmm8
	pshufd	$0xb1,  %xmm9, %xmm15
	mulps	%xmm12, %xmm9
	addps	%xmm9,  %xmm2
	movsd	-28 * SIZE(A1, LDA), %xmm9
	movhps	-26 * SIZE(A1, LDA), %xmm9

	mulps	%xmm12, %xmm14
	SUBPS	%xmm14, %xmm1
	mulps	%xmm12, %xmm15
	SUBPS	%xmm15, %xmm3

	pshufd	$0xb1,  %xmm10, %xmm14
	mulps	%xmm12, %xmm10
	addps	%xmm10, %xmm4
	movaps	-28 * SIZE(A2), %xmm10
	pshufd	$0xb1,  %xmm11, %xmm15
	mulps	%xmm12, %xmm11
	addps	%xmm11, %xmm6
	movsd	-28 * SIZE(A2, LDA), %xmm11
	movhps	-26 * SIZE(A2, LDA), %xmm11

	mulps	%xmm12, %xmm14
	SUBPS	%xmm14, %xmm5
	mulps	%xmm12, %xmm15
	movaps	-24 * SIZE(X1), %xmm12
	SUBPS	%xmm15, %xmm7

	pshufd	$0xb1,  %xmm8, %xmm14
	mulps	%xmm13, %xmm8
	addps	%xmm8,  %xmm0
	pshufd	$0xb1,  %xmm9, %xmm15
	mulps	%xmm13, %xmm9
	addps	%xmm9,  %xmm2

	mulps	%xmm13, %xmm14
	SUBPS	%xmm14, %xmm1
	mulps	%xmm13, %xmm15
	SUBPS	%xmm15, %xmm3

	pshufd	$0xb1,  %xmm10, %xmm14
	mulps	%xmm13, %xmm10
	addps	%xmm10, %xmm4
	pshufd	$0xb1,  %xmm11, %xmm15
	mulps	%xmm13, %xmm11
	addps	%xmm11, %xmm6

	mulps	%xmm13, %xmm14
	SUBPS	%xmm14, %xmm5
	mulps	%xmm13, %xmm15
	movaps	-20 * SIZE(X1), %xmm13
	SUBPS	%xmm15, %xmm7

	addq	 $8 * SIZE, A1
	addq	 $8 * SIZE, A2
	addq	 $8 * SIZE, X1
	ALIGN_3

.L107:
	testq	$2, MM
	je	.L108

	movaps	-32 * SIZE(A1), %xmm8
	movsd	-32 * SIZE(A1, LDA), %xmm9
	movhps	-30 * SIZE(A1, LDA), %xmm9

	movaps	-32 * SIZE(A2), %xmm10
	movsd	-32 * SIZE(A2, LDA), %xmm11
	movhps	-30 * SIZE(A2, LDA), %xmm11

	pshufd	$0xb1,  %xmm8, %xmm14
	mulps	%xmm12, %xmm8
	addps	%xmm8,  %xmm0
	pshufd	$0xb1,  %xmm9, %xmm15
	mulps	%xmm12, %xmm9
	addps	%xmm9,  %xmm2

	mulps	%xmm12, %xmm14
	SUBPS	%xmm14, %xmm1
	mulps	%xmm12, %xmm15
	SUBPS	%xmm15, %xmm3

	pshufd	$0xb1,  %xmm10, %xmm14
	mulps	%xmm12, %xmm10
	addps	%xmm10, %xmm4
	pshufd	$0xb1,  %xmm11, %xmm15
	mulps	%xmm12, %xmm11
	addps	%xmm11, %xmm6

	mulps	%xmm12, %xmm14
	SUBPS	%xmm14, %xmm5
	mulps	%xmm12, %xmm15
	SUBPS	%xmm15, %xmm7

	movaps	%xmm13, %xmm12

	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, A2
	ALIGN_3

.L108:
	testq	$1, MM
	je	.L109

#ifdef movsd
	xorps	%xmm8, %xmm8
#endif
	movsd	-32 * SIZE(A1), %xmm8
#ifdef movsd
	xorps	%xmm9, %xmm9
#endif
	movsd	-32 * SIZE(A1, LDA), %xmm9
#ifdef movsd
	xorps	%xmm10, %xmm10
#endif
	movsd	-32 * SIZE(A2), %xmm10
#ifdef movsd
	xorps	%xmm11, %xmm11
#endif
	movsd	-32 * SIZE(A2, LDA), %xmm11

	pshufd	$0xb1,  %xmm8, %xmm14
	mulps	%xmm12, %xmm8
	addps	%xmm8,  %xmm0
	pshufd	$0xb1,  %xmm9, %xmm15
	mulps	%xmm12, %xmm9
	addps	%xmm9,  %xmm2

	mulps	%xmm12, %xmm14
	SUBPS	%xmm14, %xmm1
	mulps	%xmm12, %xmm15
	SUBPS	%xmm15, %xmm3

	pshufd	$0xb1,  %xmm10, %xmm14
	mulps	%xmm12, %xmm10
	addps	%xmm10, %xmm4
	pshufd	$0xb1,  %xmm11, %xmm15
	mulps	%xmm12, %xmm11
	addps	%xmm11, %xmm6

	mulps	%xmm12, %xmm14
	SUBPS	%xmm14, %xmm5
	mulps	%xmm12, %xmm15
	SUBPS	%xmm15, %xmm7
	ALIGN_3

.L109:
	pcmpeqb	%xmm11, %xmm11
	psllq	$63,    %xmm11

#if (!defined(CONJ) && !defined(XCONJ)) || (defined(CONJ) && defined(XCONJ))
	xorps	%xmm11, %xmm0
	xorps	%xmm11, %xmm2
	xorps	%xmm11, %xmm4
	xorps	%xmm11, %xmm6
#else
	xorps	%xmm11, %xmm1
	xorps	%xmm11, %xmm3
	xorps	%xmm11, %xmm5
	xorps	%xmm11, %xmm7
#endif

#ifdef HAVE_SSE3
	haddps	%xmm1, %xmm0
	haddps	%xmm3, %xmm2
	haddps	%xmm2, %xmm0

	haddps	%xmm5, %xmm4
	haddps	%xmm7, %xmm6
	haddps	%xmm6, %xmm4
#else

	movaps	%xmm0, %xmm8
	unpcklps %xmm1, %xmm0
	unpckhps %xmm1, %xmm8

	movaps	%xmm2, %xmm9
	unpcklps %xmm3, %xmm2
	unpckhps %xmm3, %xmm9

	movaps	%xmm4, %xmm10
	unpcklps %xmm5, %xmm4
	unpckhps %xmm5, %xmm10

	movaps	%xmm6, %xmm11
	unpcklps %xmm7, %xmm6
	unpckhps %xmm7, %xmm11

	addps	%xmm8,  %xmm0
	addps	%xmm9,  %xmm2
	addps	%xmm10, %xmm4
	addps	%xmm11, %xmm6

	movhlps	%xmm0, %xmm1
	movhlps	%xmm2, %xmm3
	movhlps	%xmm4, %xmm5
	movhlps	%xmm6, %xmm7

	addps	%xmm1, %xmm0
	addps	%xmm3, %xmm2
	addps	%xmm5, %xmm4
	addps	%xmm7, %xmm6

	movlhps %xmm2, %xmm0
	movlhps %xmm6, %xmm4
#endif

	pshufd	$0xb1, %xmm0, %xmm1
	pshufd	$0xb1, %xmm4, %xmm5

#ifdef HAVE_SSE3
	movddup	ALPHA, %xmm15
#else
	movsd	ALPHA, %xmm15
	pshufd	$0x44, %xmm15, %xmm15
#endif

	mulps	%xmm15, %xmm0
	mulps	%xmm15, %xmm1
	mulps	%xmm15, %xmm4
	mulps	%xmm15, %xmm5

	xorps	%xmm11, %xmm0
	xorps	%xmm11, %xmm4

#ifdef HAVE_SSE3
	haddps	%xmm1, %xmm0
	haddps	%xmm5, %xmm4
#else
	movaps	%xmm0, %xmm2
	shufps	$0x88, %xmm1, %xmm0
	shufps	$0xdd, %xmm1, %xmm2

	movaps	%xmm4, %xmm6
	shufps	$0x88, %xmm5, %xmm4
	shufps	$0xdd, %xmm5, %xmm6

	addps	%xmm2, %xmm0
	addps	%xmm6, %xmm4
#endif

	movsd	(Y), %xmm2
	addq	INCY, Y
	movhps	(Y), %xmm2
	addq	INCY, Y
	movsd	(Y), %xmm6
	addq	INCY, Y
	movhps	(Y), %xmm6
	addq	INCY, Y

	shufps	$0xd8, %xmm0, %xmm0
	shufps	$0xd8, %xmm4, %xmm4

	addps	%xmm2, %xmm0
	addps	%xmm6, %xmm4

	movlps	%xmm0, (Y1)
	addq	INCY, Y1
	movhps	%xmm0, (Y1)
	addq	INCY, Y1
	movlps	%xmm4, (Y1)
	addq	INCY, Y1
	movhps	%xmm4, (Y1)
	addq	INCY, Y1

	cmpq	$4, N
	jge	.L101
	ALIGN_3

.L110:
#endif

	cmpq	$2, N
	jl	.L120
#if GEMV_UNROLL == 2
	ALIGN_3

.L111:
#endif
	subq	$2, N

	leaq	32 * SIZE(BUFFER), X1

	movq	A, A1
	leaq	(A1, LDA), A2
	leaq	(A1, LDA, 2), A

	xorps	%xmm0, %xmm0
	xorps	%xmm1, %xmm1
	xorps	%xmm2, %xmm2
	xorps	%xmm3, %xmm3

#ifdef ALIGNED_ACCESS
	cmpq	M, MM
	je	.L11X

#ifdef movsd
	xorps	%xmm8, %xmm8
#endif
	movsd	-32 * SIZE(A1), %xmm8
#ifdef movsd
	xorps	%xmm9, %xmm9
#endif
	movsd	-32 * SIZE(A2), %xmm9

#ifdef movsd
	xorps	%xmm12, %xmm12
#endif
	movsd	-32 * SIZE(X1), %xmm12

	pshufd	$0xb1, %xmm8,  %xmm4
	mulps	%xmm12,  %xmm8
	addps	%xmm8,  %xmm0
	mulps	%xmm12,  %xmm4
	SUBPS	%xmm4,  %xmm1

	pshufd	$0xb1, %xmm9, %xmm5
	mulps	%xmm12,  %xmm9
	addps	%xmm9, %xmm2
	mulps	%xmm12,  %xmm5
	SUBPS	%xmm5, %xmm3

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, X1
	ALIGN_3
.L11X:
#endif

	movaps	-32 * SIZE(X1), %xmm12
	movaps	-28 * SIZE(X1), %xmm13

#if (GEMV_UNROLL == 2) && defined(PREFETCHW)
	PREFETCHW	3 * SIZE(Y1)
#endif

	movq	MM,  I
	sarq	$3,  I
	jle	.L115

	movaps	-32 * SIZE(A1), %xmm8
	movsd	-32 * SIZE(A2), %xmm9
	movhps	-30 * SIZE(A2), %xmm9

	movaps	-28 * SIZE(A1), %xmm10
	movsd	-28 * SIZE(A2), %xmm11
	movhps	-26 * SIZE(A2), %xmm11

	decq	 I
	jle	 .L114
	ALIGN_3

.L113:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(A1)
#endif

	pshufd	$0xb1, %xmm8,  %xmm4
	mulps	%xmm12,  %xmm8
	addps	%xmm8,  %xmm0
	movaps	-24 * SIZE(A1), %xmm8
	mulps	%xmm12,  %xmm4
	SUBPS	%xmm4,  %xmm1

	pshufd	$0xb1, %xmm9, %xmm5
	mulps	%xmm12,  %xmm9
	addps	%xmm9, %xmm2
	movsd	-24 * SIZE(A2), %xmm9
	movhps	-22 * SIZE(A2), %xmm9
	mulps	%xmm12,  %xmm5
	SUBPS	%xmm5, %xmm3

	movaps	-24 * SIZE(X1), %xmm12

	pshufd	$0xb1, %xmm10,  %xmm6
	mulps	%xmm13,  %xmm10
	addps	%xmm10,  %xmm0
	movaps	-20 * SIZE(A1), %xmm10
	mulps	%xmm13,  %xmm6
	SUBPS	%xmm6,  %xmm1

	pshufd	$0xb1, %xmm11, %xmm7
	mulps	%xmm13,  %xmm11
	addps	%xmm11, %xmm2
	movsd	-20 * SIZE(A2), %xmm11
	movhps	-18 * SIZE(A2), %xmm11
	mulps	%xmm13,  %xmm7
	SUBPS	%xmm7, %xmm3

	movaps	-20 * SIZE(X1), %xmm13

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(A2)
#endif

	pshufd	$0xb1, %xmm8,  %xmm4
	mulps	%xmm12,  %xmm8
	addps	%xmm8,  %xmm0
	movaps	-16 * SIZE(A1), %xmm8
	mulps	%xmm12,  %xmm4
	SUBPS	%xmm4,  %xmm1

	pshufd	$0xb1, %xmm9, %xmm5
	mulps	%xmm12,  %xmm9
	addps	%xmm9, %xmm2
	movsd	-16 * SIZE(A2), %xmm9
	movhps	-14 * SIZE(A2), %xmm9
	mulps	%xmm12,  %xmm5
	SUBPS	%xmm5, %xmm3

	movaps	-16 * SIZE(X1), %xmm12

#ifdef PREFETCHW
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(X1)
#endif

	pshufd	$0xb1, %xmm10,  %xmm6
	mulps	%xmm13,  %xmm10
	addps	%xmm10,  %xmm0
	movaps	-12 * SIZE(A1), %xmm10
	mulps	%xmm13,  %xmm6
	SUBPS	%xmm6,  %xmm1

	pshufd	$0xb1, %xmm11, %xmm7
	mulps	%xmm13,  %xmm11
	addps	%xmm11, %xmm2
	movsd	-12 * SIZE(A2), %xmm11
	movhps	-10 * SIZE(A2), %xmm11
	mulps	%xmm13,  %xmm7
	SUBPS	%xmm7, %xmm3

	movaps	-12 * SIZE(X1), %xmm13

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, A2
	subq	 $-16 * SIZE, X1

	subq	 $1, I
	BRANCH
	jg	.L113
	ALIGN_3

.L114:
	pshufd	$0xb1, %xmm8,  %xmm4
	mulps	%xmm12,  %xmm8
	addps	%xmm8,  %xmm0
	movaps	-24 * SIZE(A1), %xmm8
	mulps	%xmm12,  %xmm4
	SUBPS	%xmm4,  %xmm1

	pshufd	$0xb1, %xmm9, %xmm5
	mulps	%xmm12,  %xmm9
	addps	%xmm9, %xmm2
	movsd	-24 * SIZE(A2), %xmm9
	movhps	-22 * SIZE(A2), %xmm9
	mulps	%xmm12,  %xmm5
	SUBPS	%xmm5, %xmm3

	movaps	-24 * SIZE(X1), %xmm12

	pshufd	$0xb1, %xmm10,  %xmm6
	mulps	%xmm13,  %xmm10
	addps	%xmm10,  %xmm0
	movaps	-20 * SIZE(A1), %xmm10
	mulps	%xmm13,  %xmm6
	SUBPS	%xmm6,  %xmm1

	pshufd	$0xb1, %xmm11, %xmm7
	mulps	%xmm13,  %xmm11
	addps	%xmm11, %xmm2
	movsd	-20 * SIZE(A2), %xmm11
	movhps	-18 * SIZE(A2), %xmm11
	mulps	%xmm13,  %xmm7
	SUBPS	%xmm7, %xmm3

	movaps	-20 * SIZE(X1), %xmm13

	pshufd	$0xb1, %xmm8,  %xmm4
	mulps	%xmm12,  %xmm8
	addps	%xmm8,  %xmm0
	mulps	%xmm12,  %xmm4
	SUBPS	%xmm4,  %xmm1

	pshufd	$0xb1, %xmm9, %xmm5
	mulps	%xmm12,  %xmm9
	addps	%xmm9, %xmm2
	mulps	%xmm12,  %xmm5
	SUBPS	%xmm5, %xmm3

	movaps	-16 * SIZE(X1), %xmm12

	pshufd	$0xb1, %xmm10,  %xmm6
	mulps	%xmm13,  %xmm10
	addps	%xmm10,  %xmm0
	mulps	%xmm13,  %xmm6
	SUBPS	%xmm6,  %xmm1

	pshufd	$0xb1, %xmm11, %xmm7
	mulps	%xmm13,  %xmm11
	addps	%xmm11, %xmm2
	mulps	%xmm13,  %xmm7
	SUBPS	%xmm7, %xmm3

	movaps	-12 * SIZE(X1), %xmm13

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, A2
	subq	 $-16 * SIZE, X1
	ALIGN_3

.L115:
	testq	$4, MM
	je	.L117

	movaps	-32 * SIZE(A1), %xmm8
	movsd	-32 * SIZE(A2), %xmm9
	movhps	-30 * SIZE(A2), %xmm9

	movaps	-28 * SIZE(A1), %xmm10
	movsd	-28 * SIZE(A2), %xmm11
	movhps	-26 * SIZE(A2), %xmm11

	pshufd	$0xb1, %xmm8,  %xmm4
	mulps	%xmm12,  %xmm8
	addps	%xmm8,  %xmm0
	mulps	%xmm12,  %xmm4
	SUBPS	%xmm4,  %xmm1

	pshufd	$0xb1, %xmm9, %xmm5
	mulps	%xmm12,  %xmm9
	addps	%xmm9, %xmm2
	mulps	%xmm12,  %xmm5
	SUBPS	%xmm5, %xmm3

	movaps	-24 * SIZE(X1), %xmm12

	pshufd	$0xb1, %xmm10,  %xmm6
	mulps	%xmm13,  %xmm10
	addps	%xmm10,  %xmm0
	mulps	%xmm13,  %xmm6
	SUBPS	%xmm6,  %xmm1

	pshufd	$0xb1, %xmm11, %xmm7
	mulps	%xmm13,  %xmm11
	addps	%xmm11, %xmm2
	mulps	%xmm13,  %xmm7
	SUBPS	%xmm7, %xmm3

	movaps	-20 * SIZE(X1), %xmm13

	addq	 $8 * SIZE, A1
	addq	 $8 * SIZE, A2
	ALIGN_3

.L117:
	testq	$2, MM
	je	.L118

	movaps	-32 * SIZE(A1), %xmm8
	movsd	-32 * SIZE(A2), %xmm9
	movhps	-30 * SIZE(A2), %xmm9

	pshufd	$0xb1, %xmm8,  %xmm4
	mulps	%xmm12,  %xmm8
	addps	%xmm8,  %xmm0
	mulps	%xmm12,  %xmm4
	SUBPS	%xmm4,  %xmm1

	pshufd	$0xb1, %xmm9, %xmm5
	mulps	%xmm12,  %xmm9
	addps	%xmm9, %xmm2
	mulps	%xmm12,  %xmm5
	SUBPS	%xmm5, %xmm3

	movaps	%xmm13, %xmm12

	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, A2
	ALIGN_3

.L118:
	testq	$1, MM
	je	.L119

#ifdef movsd
	xorps	%xmm8, %xmm8
#endif
	movsd	-32 * SIZE(A1), %xmm8
#ifdef movsd
	xorps	%xmm9, %xmm9
#endif
	movsd	-32 * SIZE(A2), %xmm9

	pshufd	$0xb1, %xmm8,  %xmm4
	mulps	%xmm12,  %xmm8
	addps	%xmm8,  %xmm0
	mulps	%xmm12,  %xmm4
	SUBPS	%xmm4,  %xmm1

	pshufd	$0xb1, %xmm9, %xmm5
	mulps	%xmm12,  %xmm9
	addps	%xmm9, %xmm2
	mulps	%xmm12,  %xmm5
	SUBPS	%xmm5, %xmm3
	ALIGN_3

.L119:
	pcmpeqb	%xmm5, %xmm5
	psllq	$63,    %xmm5

#if (!defined(CONJ) && !defined(XCONJ)) || (defined(CONJ) && defined(XCONJ))
	xorps	%xmm5, %xmm0
	xorps	%xmm5, %xmm2
#else
	xorps	%xmm5, %xmm1
	xorps	%xmm5, %xmm3
#endif

#ifdef HAVE_SSE3
	haddps	%xmm1, %xmm0
	haddps	%xmm3, %xmm2
	haddps	%xmm2, %xmm0
#else
	movaps	%xmm0, %xmm8
	unpcklps %xmm1, %xmm0
	unpckhps %xmm1, %xmm8

	movaps	%xmm2, %xmm4
	unpcklps %xmm3, %xmm2
	unpckhps %xmm3, %xmm4

	addps	%xmm8, %xmm0
	addps	%xmm4, %xmm2

	movhlps	%xmm0, %xmm1
	movhlps	%xmm2, %xmm3

	addps	%xmm1, %xmm0
	addps	%xmm3, %xmm2
	movlhps %xmm2, %xmm0
#endif

	pshufd	$0xb1, %xmm0, %xmm1

#ifdef HAVE_SSE3
	movddup	ALPHA, %xmm15
#else
	movsd	ALPHA, %xmm15
	pshufd	$0x44, %xmm15, %xmm15
#endif

	mulps	%xmm15, %xmm0
	mulps	%xmm15, %xmm1

	xorps	%xmm5, %xmm0

#ifdef HAVE_SSE3
	haddps	%xmm1, %xmm0
#else
	movaps	%xmm0, %xmm2
	shufps	$0x88, %xmm1, %xmm0
	shufps	$0xdd, %xmm1, %xmm2

	addps	%xmm2, %xmm0
#endif

	movsd	(Y), %xmm12
	addq	INCY, Y
	movhps	(Y), %xmm12
	addq	INCY, Y

	shufps	$0xd8, %xmm0, %xmm0

	addps	%xmm12, %xmm0

	movlps	%xmm0, (Y1)
	addq	INCY, Y1
	movhps	%xmm0, (Y1)
	addq	INCY, Y1

#if GEMV_UNROLL == 2
	cmpq	$2, N
	jge	.L111
#endif
	ALIGN_3

.L120:
	cmpq	$1, N
	jl	.L999

	leaq	32 * SIZE(BUFFER), X1

	movq	A, A1

	xorps	%xmm0, %xmm0
	xorps	%xmm1, %xmm1

#ifdef ALIGNED_ACCESS
	cmpq	M, MM
	je	.L12X

#ifdef movsd
	xorps	%xmm8, %xmm8
#endif
	movsd	-32 * SIZE(A1), %xmm8
#ifdef movsd
	xorps	%xmm12, %xmm12
#endif
	movsd	-32 * SIZE(X1), %xmm12

	pshufd	$0xb1, %xmm8,  %xmm4
	mulps	%xmm12,  %xmm8
	addps	%xmm8,  %xmm0
	mulps	%xmm12,  %xmm4
	SUBPS	%xmm4,  %xmm1

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, X1
	ALIGN_3
.L12X:
#endif

	movaps	-32 * SIZE(X1), %xmm12
	movaps	-28 * SIZE(X1), %xmm13

	movq	MM,  I
	sarq	$3,  I
	jle	.L125

	MOVUPS_A1(-32 * SIZE, A1, %xmm8)
	MOVUPS_A1(-28 * SIZE, A1, %xmm10)

	decq	 I
	jle	 .L124
	ALIGN_3

.L123:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 4 - 128 + PREOFFSET(A1)
#endif

	pshufd	$0xb1, %xmm8,  %xmm4
	mulps	%xmm12,  %xmm8
	addps	%xmm8,  %xmm0
	MOVUPS_A1(-24 * SIZE, A1, %xmm8)
	mulps	%xmm12,  %xmm4
	SUBPS	%xmm4,  %xmm1

	movaps	-24 * SIZE(X1), %xmm12

	pshufd	$0xb1, %xmm10,  %xmm6
	mulps	%xmm13,  %xmm10
	addps	%xmm10,  %xmm0
	MOVUPS_A1(-20 * SIZE, A1, %xmm10)
	mulps	%xmm13,  %xmm6
	SUBPS	%xmm6,  %xmm1

	movaps	-20 * SIZE(X1), %xmm13

	pshufd	$0xb1, %xmm8,  %xmm4
	mulps	%xmm12,  %xmm8
	addps	%xmm8,  %xmm0
	MOVUPS_A1(-16 * SIZE, A1, %xmm8)
	mulps	%xmm12,  %xmm4
	SUBPS	%xmm4,  %xmm1

	movaps	-16 * SIZE(X1), %xmm12

#ifdef PREFETCHW
	PREFETCH	(PREFETCHSIZE) * 4 - 128 + PREOFFSET(X1)
#endif

	pshufd	$0xb1, %xmm10,  %xmm6
	mulps	%xmm13,  %xmm10
	addps	%xmm10,  %xmm0
	MOVUPS_A1(-12 * SIZE, A1, %xmm10)
	mulps	%xmm13,  %xmm6
	SUBPS	%xmm6,  %xmm1

	movaps	-12 * SIZE(X1), %xmm13

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, X1

	subq	 $1, I
	BRANCH
	jg	.L123
	ALIGN_3

.L124:
	pshufd	$0xb1, %xmm8,  %xmm4
	mulps	%xmm12,  %xmm8
	addps	%xmm8,  %xmm0
	MOVUPS_A1(-24 * SIZE, A1, %xmm8)
	mulps	%xmm12,  %xmm4
	SUBPS	%xmm4,  %xmm1

	movaps	-24 * SIZE(X1), %xmm12

	pshufd	$0xb1, %xmm10,  %xmm6
	mulps	%xmm13,  %xmm10
	addps	%xmm10,  %xmm0
	MOVUPS_A1(-20 * SIZE, A1, %xmm10)
	mulps	%xmm13,  %xmm6
	SUBPS	%xmm6,  %xmm1

	movaps	-20 * SIZE(X1), %xmm13

	pshufd	$0xb1, %xmm8,  %xmm4
	mulps	%xmm12,  %xmm8
	addps	%xmm8,  %xmm0
	mulps	%xmm12,  %xmm4
	SUBPS	%xmm4,  %xmm1

	movaps	-16 * SIZE(X1), %xmm12

	pshufd	$0xb1, %xmm10,  %xmm6
	mulps	%xmm13,  %xmm10
	addps	%xmm10,  %xmm0
	mulps	%xmm13,  %xmm6
	SUBPS	%xmm6,  %xmm1

	movaps	-12 * SIZE(X1), %xmm13

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, X1
	ALIGN_3

.L125:
	testq	$4, MM
	je	.L127

	MOVUPS_A1(-32 * SIZE, A1, %xmm8)
	MOVUPS_A1(-28 * SIZE, A1, %xmm10)

	pshufd	$0xb1, %xmm8,  %xmm4
	mulps	%xmm12,  %xmm8
	addps	%xmm8,  %xmm0
	mulps	%xmm12,  %xmm4
	SUBPS	%xmm4,  %xmm1

	movaps	-24 * SIZE(X1), %xmm12

	pshufd	$0xb1, %xmm10,  %xmm6
	mulps	%xmm13,  %xmm10
	addps	%xmm10,  %xmm0
	mulps	%xmm13,  %xmm6
	SUBPS	%xmm6,  %xmm1

	movaps	-20 * SIZE(X1), %xmm13

	addq	 $8 * SIZE, A1
	ALIGN_3

.L127:
	testq	$2, MM
	je	.L128

	MOVUPS_A1(-32 * SIZE, A1, %xmm8)

	pshufd	$0xb1, %xmm8,  %xmm4
	mulps	%xmm12,  %xmm8
	addps	%xmm8,  %xmm0
	mulps	%xmm12,  %xmm4
	SUBPS	%xmm4,  %xmm1

	movaps	%xmm13, %xmm12

	addq	 $4 * SIZE, A1
	ALIGN_3

.L128:
	testq	$1, MM
	je	.L129

#ifdef movsd
	xorps	%xmm8, %xmm8
#endif
	movsd	-32 * SIZE(A1), %xmm8

	pshufd	$0xb1, %xmm8,  %xmm4
	mulps	%xmm12,  %xmm8
	addps	%xmm8,  %xmm0
	mulps	%xmm12,  %xmm4
	SUBPS	%xmm4,  %xmm1
	ALIGN_3

.L129:
	pcmpeqb	%xmm5, %xmm5
	psllq	$63,    %xmm5

#if (!defined(CONJ) && !defined(XCONJ)) || (defined(CONJ) && defined(XCONJ))
	xorps	%xmm5, %xmm0
#else
	xorps	%xmm5, %xmm1
#endif

#ifdef HAVE_SSE3
	haddps	%xmm1, %xmm0
	haddps	%xmm0, %xmm0
#else
	movaps	%xmm0, %xmm8
	unpcklps %xmm1, %xmm0
	unpckhps %xmm1, %xmm8

	addps	%xmm8, %xmm0

	movhlps	%xmm0, %xmm1

	addps	%xmm1, %xmm0
#endif

	pshufd	$0xb1, %xmm0, %xmm1

#ifdef HAVE_SSE3
	movddup	ALPHA, %xmm15
#else
	movsd	ALPHA, %xmm15
	pshufd	$0x44, %xmm15, %xmm15
#endif

	mulps	%xmm15, %xmm0
	mulps	%xmm15, %xmm1

	xorps	%xmm5, %xmm0

#ifdef HAVE_SSE3
	haddps	%xmm1, %xmm0
#else
	movaps	%xmm0, %xmm2
	shufps	$0x88, %xmm1, %xmm0
	shufps	$0xdd, %xmm1, %xmm2

	addps	%xmm2, %xmm0
#endif

	movsd	(Y), %xmm12
	addq	INCY, Y

	shufps	$0xd8, %xmm0, %xmm0

	addps	%xmm12, %xmm0

	movlps	%xmm0, (Y1)
	addq	INCY, Y1
	jmp	.L999
	ALIGN_3


.L200:
	testq	$2 * SIZE, LDA
	jne	.L300

	cmpq	$2, N
	jl	.L210
	ALIGN_3

.L201:
	subq	$2, N

	leaq	32 * SIZE(BUFFER), X1

	movq	A, A1
	leaq	(A1, LDA), A2
	leaq	(A1, LDA, 2), A

	xorps	%xmm0, %xmm0
	xorps	%xmm1, %xmm1
	xorps	%xmm2, %xmm2
	xorps	%xmm3, %xmm3

#ifdef ALIGNED_ACCESS
	cmpq	M, MM
	je	.L20X

#ifdef movsd
	xorps	%xmm8, %xmm8
#endif
	movsd	-32 * SIZE(A1), %xmm8
#ifdef movsd
	xorps	%xmm9, %xmm9
#endif
	movsd	-32 * SIZE(A2), %xmm9

#ifdef movsd
	xorps	%xmm12, %xmm12
#endif
	movsd	-32 * SIZE(X1), %xmm12

	pshufd	$0xb1, %xmm8,  %xmm4
	mulps	%xmm12,  %xmm8
	addps	%xmm8,  %xmm0
	mulps	%xmm12,  %xmm4
	SUBPS	%xmm4,  %xmm1

	pshufd	$0xb1, %xmm9, %xmm5
	mulps	%xmm12,  %xmm9
	addps	%xmm9, %xmm2
	mulps	%xmm12,  %xmm5
	SUBPS	%xmm5, %xmm3

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, X1
	ALIGN_3
.L20X:
#endif

	movaps	-33 * SIZE(A1), %xmm4
	movaps	-33 * SIZE(A2), %xmm5

	movaps	-32 * SIZE(X1), %xmm12
	movaps	-28 * SIZE(X1), %xmm13

#ifdef PREFETCHW
	PREFETCHW	3 * SIZE(Y1)
#endif

	movq	MM,  I
	sarq	$3,  I
	jle	.L205

	movaps	 -29 * SIZE(A1), %xmm6
	movaps	 -29 * SIZE(A2), %xmm7

	decq	 I
	jle	 .L204
	ALIGN_3

.L203:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(A1)
#endif

	movss	 %xmm6, %xmm4
	shufps	 $0x39, %xmm4, %xmm4
	pshufd	$0xb1,  %xmm4,  %xmm14
	mulps	%xmm12, %xmm4
	addps	%xmm4,  %xmm0
	movaps	 -25 * SIZE(A1), %xmm4
	mulps	%xmm12, %xmm14
	SUBPS	%xmm14, %xmm1

	movss	 %xmm7, %xmm5
	shufps	 $0x39, %xmm5, %xmm5
	pshufd	$0xb1,  %xmm5,  %xmm14
	mulps	%xmm12, %xmm5
	addps	%xmm5,  %xmm2
	movaps	 -25 * SIZE(A2), %xmm5
	mulps	%xmm12, %xmm14
	movaps	-24 * SIZE(X1), %xmm12
	SUBPS	%xmm14, %xmm3

	movss	 %xmm4, %xmm6
	shufps	 $0x39, %xmm6, %xmm6
	pshufd	$0xb1,  %xmm6,  %xmm14
	mulps	%xmm13, %xmm6
	addps	%xmm6,  %xmm0
	movaps	 -21 * SIZE(A1), %xmm6
	mulps	%xmm13, %xmm14
	SUBPS	%xmm14, %xmm1

	movss	 %xmm5, %xmm7
	shufps	 $0x39, %xmm7, %xmm7
	pshufd	$0xb1,  %xmm7,  %xmm14
	mulps	%xmm13, %xmm7
	addps	%xmm7,  %xmm2
	movaps	 -21 * SIZE(A2), %xmm7
	mulps	%xmm13, %xmm14
	movaps	-20 * SIZE(X1), %xmm13
	SUBPS	%xmm14, %xmm3

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(A2)
#endif

	movss	 %xmm6, %xmm4
	shufps	 $0x39, %xmm4, %xmm4
	pshufd	$0xb1,  %xmm4,  %xmm14
	mulps	%xmm12, %xmm4
	addps	%xmm4,  %xmm0
	movaps	 -17 * SIZE(A1), %xmm4
	mulps	%xmm12, %xmm14
	SUBPS	%xmm14, %xmm1

	movss	 %xmm7, %xmm5
	shufps	 $0x39, %xmm5, %xmm5
	pshufd	$0xb1,  %xmm5,  %xmm14
	mulps	%xmm12, %xmm5
	addps	%xmm5,  %xmm2
	movaps	 -17 * SIZE(A2), %xmm5
	mulps	%xmm12, %xmm14
	movaps	-16 * SIZE(X1), %xmm12
	SUBPS	%xmm14, %xmm3

#ifdef PREFETCHW
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(X1)
#endif

	movss	 %xmm4, %xmm6
	shufps	 $0x39, %xmm6, %xmm6
	pshufd	$0xb1,  %xmm6,  %xmm14
	mulps	%xmm13, %xmm6
	addps	%xmm6,  %xmm0
	movaps	 -13 * SIZE(A1), %xmm6
	mulps	%xmm13, %xmm14
	SUBPS	%xmm14, %xmm1

	movss	 %xmm5, %xmm7
	shufps	 $0x39, %xmm7, %xmm7
	pshufd	$0xb1,  %xmm7,  %xmm14
	mulps	%xmm13, %xmm7
	addps	%xmm7,  %xmm2
	movaps	 -13 * SIZE(A2), %xmm7
	mulps	%xmm13, %xmm14
	movaps	-12 * SIZE(X1), %xmm13
	SUBPS	%xmm14, %xmm3

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, A2
	subq	 $-16 * SIZE, X1

	subq	 $1, I
	BRANCH
	jg	.L203
	ALIGN_3

.L204:
	movss	 %xmm6, %xmm4
	shufps	 $0x39, %xmm4, %xmm4
	pshufd	$0xb1,  %xmm4,  %xmm14
	mulps	%xmm12, %xmm4
	addps	%xmm4,  %xmm0
	movaps	 -25 * SIZE(A1), %xmm4
	mulps	%xmm12, %xmm14
	SUBPS	%xmm14, %xmm1

	movss	 %xmm7, %xmm5
	shufps	 $0x39, %xmm5, %xmm5
	pshufd	$0xb1,  %xmm5,  %xmm14
	mulps	%xmm12, %xmm5
	addps	%xmm5,  %xmm2
	movaps	 -25 * SIZE(A2), %xmm5
	mulps	%xmm12, %xmm14
	movaps	-24 * SIZE(X1), %xmm12
	SUBPS	%xmm14, %xmm3

	movss	 %xmm4, %xmm6
	shufps	 $0x39, %xmm6, %xmm6
	pshufd	$0xb1,  %xmm6,  %xmm14
	mulps	%xmm13, %xmm6
	addps	%xmm6,  %xmm0
	movaps	 -21 * SIZE(A1), %xmm6
	mulps	%xmm13, %xmm14
	SUBPS	%xmm14, %xmm1

	movss	 %xmm5, %xmm7
	shufps	 $0x39, %xmm7, %xmm7
	pshufd	$0xb1,  %xmm7,  %xmm14
	mulps	%xmm13, %xmm7
	addps	%xmm7,  %xmm2
	movaps	 -21 * SIZE(A2), %xmm7
	mulps	%xmm13, %xmm14
	movaps	-20 * SIZE(X1), %xmm13
	SUBPS	%xmm14, %xmm3

	movss	 %xmm6, %xmm4
	shufps	 $0x39, %xmm4, %xmm4
	pshufd	$0xb1,  %xmm4,  %xmm14
	mulps	%xmm12, %xmm4
	addps	%xmm4,  %xmm0
	movaps	 -17 * SIZE(A1), %xmm4
	mulps	%xmm12, %xmm14
	SUBPS	%xmm14, %xmm1

	movss	 %xmm7, %xmm5
	shufps	 $0x39, %xmm5, %xmm5
	pshufd	$0xb1,  %xmm5,  %xmm14
	mulps	%xmm12, %xmm5
	addps	%xmm5,  %xmm2
	movaps	 -17 * SIZE(A2), %xmm5
	mulps	%xmm12, %xmm14
	movaps	-16 * SIZE(X1), %xmm12
	SUBPS	%xmm14, %xmm3

	movss	 %xmm4, %xmm6
	shufps	 $0x39, %xmm6, %xmm6
	pshufd	$0xb1,  %xmm6,  %xmm14
	mulps	%xmm13, %xmm6
	addps	%xmm6,  %xmm0
	mulps	%xmm13, %xmm14
	SUBPS	%xmm14, %xmm1

	movss	 %xmm5, %xmm7
	shufps	 $0x39, %xmm7, %xmm7
	pshufd	$0xb1,  %xmm7,  %xmm14
	mulps	%xmm13, %xmm7
	addps	%xmm7,  %xmm2
	mulps	%xmm13, %xmm14
	movaps	-12 * SIZE(X1), %xmm13
	SUBPS	%xmm14, %xmm3

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, A2
	subq	 $-16 * SIZE, X1
	ALIGN_3

.L205:
	testq	$4, MM
	je	.L207

	movaps	 -29 * SIZE(A1), %xmm6

	movss	 %xmm6, %xmm4
	shufps	 $0x39, %xmm4, %xmm4
	pshufd	$0xb1,  %xmm4,  %xmm14
	mulps	%xmm12, %xmm4
	addps	%xmm4,  %xmm0
	mulps	%xmm12, %xmm14
	SUBPS	%xmm14, %xmm1

	movaps	 -29 * SIZE(A2), %xmm7

	movss	 %xmm7, %xmm5
	shufps	 $0x39, %xmm5, %xmm5
	pshufd	$0xb1,  %xmm5,  %xmm14
	mulps	%xmm12, %xmm5
	addps	%xmm5,  %xmm2
	mulps	%xmm12, %xmm14
	SUBPS	%xmm14, %xmm3

	movaps	 -25 * SIZE(A1), %xmm8

	movss	 %xmm8, %xmm6
	shufps	 $0x39, %xmm6, %xmm6
	pshufd	$0xb1,  %xmm6,  %xmm14
	mulps	%xmm13, %xmm6
	addps	%xmm6,  %xmm0
	mulps	%xmm13, %xmm14
	SUBPS	%xmm14, %xmm1

	movaps	 -25 * SIZE(A2), %xmm9

	movss	 %xmm9, %xmm7
	shufps	 $0x39, %xmm7, %xmm7
	pshufd	$0xb1,  %xmm7,  %xmm14
	mulps	%xmm13, %xmm7
	addps	%xmm7,  %xmm2
	mulps	%xmm13, %xmm14
	SUBPS	%xmm14, %xmm3

	movaps	%xmm8, %xmm4
	movaps	%xmm9, %xmm5

	movaps	-24 * SIZE(X1), %xmm12
	movaps	-20 * SIZE(X1), %xmm13

	addq	 $8 * SIZE, A1
	addq	 $8 * SIZE, A2
	ALIGN_3

.L207:
	testq	$2, MM
	je	.L208

	movaps	 -29 * SIZE(A1), %xmm6
	movaps	 -29 * SIZE(A2), %xmm7

	movss	 %xmm6, %xmm4
	shufps	 $0x39, %xmm4, %xmm4
	pshufd	$0xb1,  %xmm4,  %xmm14
	mulps	%xmm12, %xmm4
	addps	%xmm4,  %xmm0
	mulps	%xmm12, %xmm14
	SUBPS	%xmm14, %xmm1

	movss	 %xmm7, %xmm5
	shufps	 $0x39, %xmm5, %xmm5
	pshufd	$0xb1,  %xmm5,  %xmm14
	mulps	%xmm12, %xmm5
	addps	%xmm5,  %xmm2
	mulps	%xmm12, %xmm14
	SUBPS	%xmm14, %xmm3

	movaps	 %xmm6, %xmm4
	movaps	 %xmm7, %xmm5
	movaps	 %xmm13, %xmm12

	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, A2
	ALIGN_3

.L208:
	testq	$1, MM
	je	.L209

#ifdef movsd
	xorps	%xmm8, %xmm8
#endif
	movsd	-32 * SIZE(A1), %xmm8
#ifdef movsd
	xorps	%xmm9, %xmm9
#endif
	movsd	-32 * SIZE(A2), %xmm9

	pshufd	$0xb1, %xmm8,  %xmm4
	mulps	%xmm12,  %xmm8
	addps	%xmm8,  %xmm0
	mulps	%xmm12,  %xmm4
	SUBPS	%xmm4,  %xmm1

	pshufd	$0xb1, %xmm9, %xmm5
	mulps	%xmm12,  %xmm9
	addps	%xmm9, %xmm2
	mulps	%xmm12,  %xmm5
	SUBPS	%xmm5, %xmm3
	ALIGN_3

.L209:
	pcmpeqb	%xmm5, %xmm5
	psllq	$63,    %xmm5

#if (!defined(CONJ) && !defined(XCONJ)) || (defined(CONJ) && defined(XCONJ))
	xorps	%xmm5, %xmm0
	xorps	%xmm5, %xmm2
#else
	xorps	%xmm5, %xmm1
	xorps	%xmm5, %xmm3
#endif

#ifdef HAVE_SSE3
	haddps	%xmm1, %xmm0
	haddps	%xmm3, %xmm2
	haddps	%xmm2, %xmm0
#else
	movaps	%xmm0, %xmm8
	unpcklps %xmm1, %xmm0
	unpckhps %xmm1, %xmm8

	movaps	%xmm2, %xmm4
	unpcklps %xmm3, %xmm2
	unpckhps %xmm3, %xmm4

	addps	%xmm8, %xmm0
	addps	%xmm4, %xmm2

	movhlps	%xmm0, %xmm1
	movhlps	%xmm2, %xmm3

	addps	%xmm1, %xmm0
	addps	%xmm3, %xmm2
	movlhps %xmm2, %xmm0
#endif

	pshufd	$0xb1, %xmm0, %xmm1

#ifdef HAVE_SSE3
	movddup	ALPHA, %xmm15
#else
	movsd	ALPHA, %xmm15
	pshufd	$0x44, %xmm15, %xmm15
#endif

	mulps	%xmm15, %xmm0
	mulps	%xmm15, %xmm1

	xorps	%xmm5, %xmm0

#ifdef HAVE_SSE3
	haddps	%xmm1, %xmm0
#else
	movaps	%xmm0, %xmm2
	shufps	$0x88, %xmm1, %xmm0
	shufps	$0xdd, %xmm1, %xmm2

	addps	%xmm2, %xmm0
#endif

	movsd	(Y), %xmm12
	addq	INCY, Y
	movhps	(Y), %xmm12
	addq	INCY, Y

	shufps	$0xd8, %xmm0, %xmm0

	addps	%xmm12, %xmm0

	movlps	%xmm0, (Y1)
	addq	INCY, Y1
	movhps	%xmm0, (Y1)
	addq	INCY, Y1

	cmpq	$2, N
	jge	.L201
	ALIGN_3

.L210:
	cmpq	$1, N
	jl	.L999

	leaq	32 * SIZE(BUFFER), X1

	movq	A, A1

	xorps	%xmm0, %xmm0
	xorps	%xmm1, %xmm1

#ifdef ALIGNED_ACCESS
	cmpq	M, MM
	je	.L21X

#ifdef movsd
	xorps	%xmm8, %xmm8
#endif
	movsd	-32 * SIZE(A1), %xmm8
#ifdef movsd
	xorps	%xmm12, %xmm12
#endif
	movsd	-32 * SIZE(X1), %xmm12

	pshufd	$0xb1, %xmm8,  %xmm4
	mulps	%xmm12,  %xmm8
	addps	%xmm8,  %xmm0
	mulps	%xmm12,  %xmm4
	SUBPS	%xmm4,  %xmm1

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, X1
	ALIGN_3
.L21X:
#endif

	movaps	-33 * SIZE(A1), %xmm4

	movaps	-32 * SIZE(X1), %xmm12
	movaps	-28 * SIZE(X1), %xmm13

	movq	MM,  I
	sarq	$3,  I
	jle	.L215

	movaps	 -29 * SIZE(A1), %xmm5
	movaps	 -25 * SIZE(A1), %xmm6
	movaps	 -21 * SIZE(A1), %xmm7

	decq	 I
	jle	 .L214
	ALIGN_3

.L213:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 4 - 128 + PREOFFSET(A1)
#endif

	movss	 %xmm5, %xmm4
	shufps	 $0x39, %xmm4, %xmm4
	pshufd	$0xb1,  %xmm4,  %xmm14
	mulps	%xmm12, %xmm4
	addps	%xmm4,  %xmm0
	movaps	 -17 * SIZE(A1), %xmm4
	mulps	%xmm12, %xmm14
	movaps	-24 * SIZE(X1), %xmm12
	SUBPS	%xmm14, %xmm1

	movss	 %xmm6, %xmm5
	shufps	 $0x39, %xmm5, %xmm5
	pshufd	$0xb1,  %xmm5,  %xmm15
	mulps	%xmm13, %xmm5
	addps	%xmm5,  %xmm0
	movaps	 -13 * SIZE(A1), %xmm5
	mulps	%xmm13, %xmm15
	movaps	-20 * SIZE(X1), %xmm13
	SUBPS	%xmm15, %xmm1

#ifdef PREFETCHW
	PREFETCH	(PREFETCHSIZE) * 4 - 128 + PREOFFSET(X1)
#endif

	movss	 %xmm7, %xmm6
	shufps	 $0x39, %xmm6, %xmm6
	pshufd	$0xb1,  %xmm6,  %xmm14
	mulps	%xmm12, %xmm6
	addps	%xmm6,  %xmm0
	movaps	  -9 * SIZE(A1), %xmm6
	mulps	%xmm12, %xmm14
	movaps	-16 * SIZE(X1), %xmm12
	SUBPS	%xmm14, %xmm1

	movss	 %xmm4, %xmm7
	shufps	 $0x39, %xmm7, %xmm7
	pshufd	$0xb1,  %xmm7,  %xmm15
	mulps	%xmm13, %xmm7
	addps	%xmm7,  %xmm0
	movaps	  -5 * SIZE(A1), %xmm7
	mulps	%xmm13, %xmm15
	movaps	-12 * SIZE(X1), %xmm13
	SUBPS	%xmm15, %xmm1

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, X1

	subq	 $1, I
	BRANCH
	jg	.L213
	ALIGN_3

.L214:
	movss	 %xmm5, %xmm4
	shufps	 $0x39, %xmm4, %xmm4
	pshufd	$0xb1,  %xmm4,  %xmm14
	mulps	%xmm12, %xmm4
	addps	%xmm4,  %xmm0
	movaps	 -17 * SIZE(A1), %xmm4
	mulps	%xmm12, %xmm14
	movaps	-24 * SIZE(X1), %xmm12
	SUBPS	%xmm14, %xmm1

	movss	 %xmm6, %xmm5
	shufps	 $0x39, %xmm5, %xmm5
	pshufd	$0xb1,  %xmm5,  %xmm15
	mulps	%xmm13, %xmm5
	addps	%xmm5,  %xmm0
	mulps	%xmm13, %xmm15
	movaps	-20 * SIZE(X1), %xmm13
	SUBPS	%xmm15, %xmm1

	movss	 %xmm7, %xmm6
	shufps	 $0x39, %xmm6, %xmm6
	pshufd	$0xb1,  %xmm6,  %xmm14
	mulps	%xmm12, %xmm6
	addps	%xmm6,  %xmm0
	mulps	%xmm12, %xmm14
	movaps	-16 * SIZE(X1), %xmm12
	SUBPS	%xmm14, %xmm1

	movss	 %xmm4, %xmm7
	shufps	 $0x39, %xmm7, %xmm7
	pshufd	$0xb1,  %xmm7,  %xmm15
	mulps	%xmm13, %xmm7
	addps	%xmm7,  %xmm0
	mulps	%xmm13, %xmm15
	movaps	-12 * SIZE(X1), %xmm13
	SUBPS	%xmm15, %xmm1

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, X1
	ALIGN_3

.L215:
	testq	$4, MM
	je	.L217

	movaps	 -29 * SIZE(A1), %xmm5
	movaps	 -25 * SIZE(A1), %xmm6

	movss	 %xmm5, %xmm4
	shufps	 $0x39, %xmm4, %xmm4
	pshufd	$0xb1,  %xmm4,  %xmm14
	mulps	%xmm12, %xmm4
	addps	%xmm4,  %xmm0
	mulps	%xmm12, %xmm14
	SUBPS	%xmm14, %xmm1

	movss	 %xmm6, %xmm5
	shufps	 $0x39, %xmm5, %xmm5
	pshufd	$0xb1,  %xmm5,  %xmm15
	mulps	%xmm13, %xmm5
	addps	%xmm5,  %xmm0
	mulps	%xmm13, %xmm15
	SUBPS	%xmm15, %xmm1

	movaps	-24 * SIZE(X1), %xmm12
	movaps	-20 * SIZE(X1), %xmm13
	movaps	%xmm6, %xmm4

	addq	 $8 * SIZE, A1
	ALIGN_3

.L217:
	testq	$2, MM
	je	.L218

	movaps	 -29 * SIZE(A1), %xmm5

	movss	 %xmm5, %xmm4
	shufps	 $0x39, %xmm4, %xmm4

	pshufd	$0xb1,  %xmm4,  %xmm14
	mulps	%xmm12, %xmm4
	addps	%xmm4,  %xmm0
	mulps	%xmm12, %xmm14
	SUBPS	%xmm14, %xmm1

	movaps	%xmm13, %xmm12

	addq	 $4 * SIZE, A1
	ALIGN_3

.L218:
	testq	$1, MM
	je	.L219

#ifdef movsd
	xorps	%xmm8, %xmm8
#endif
	movsd	-32 * SIZE(A1), %xmm8

	pshufd	$0xb1, %xmm8,  %xmm4
	mulps	%xmm12,  %xmm8
	addps	%xmm8,  %xmm0
	mulps	%xmm12,  %xmm4
	SUBPS	%xmm4,  %xmm1
	ALIGN_3

.L219:
	pcmpeqb	%xmm5, %xmm5
	psllq	$63,    %xmm5

#if (!defined(CONJ) && !defined(XCONJ)) || (defined(CONJ) && defined(XCONJ))
	xorps	%xmm5, %xmm0
#else
	xorps	%xmm5, %xmm1
#endif

#ifdef HAVE_SSE3
	haddps	%xmm1, %xmm0
	haddps	%xmm0, %xmm0
#else
	movaps	%xmm0, %xmm8
	unpcklps %xmm1, %xmm0
	unpckhps %xmm1, %xmm8

	addps	%xmm8, %xmm0

	movhlps	%xmm0, %xmm1

	addps	%xmm1, %xmm0
#endif

	pshufd	$0xb1, %xmm0, %xmm1

#ifdef HAVE_SSE3
	movddup	ALPHA, %xmm15
#else
	movsd	ALPHA, %xmm15
	pshufd	$0x44, %xmm15, %xmm15
#endif

	mulps	%xmm15, %xmm0
	mulps	%xmm15, %xmm1

	xorps	%xmm5, %xmm0

#ifdef HAVE_SSE3
	haddps	%xmm1, %xmm0
#else
	movaps	%xmm0, %xmm2
	shufps	$0x88, %xmm1, %xmm0
	shufps	$0xdd, %xmm1, %xmm2

	addps	%xmm2, %xmm0
#endif

	movsd	(Y), %xmm12
	addq	INCY, Y

	shufps	$0xd8, %xmm0, %xmm0

	addps	%xmm12, %xmm0

	movlps	%xmm0, (Y1)
	addq	INCY, Y1
	jmp	.L999

.L300:
	cmpq	$2, N
	jl	.L310
	ALIGN_3

.L301:
	subq	$2, N

	leaq	32 * SIZE(BUFFER), X1

	movq	A, A1
	leaq	(A1, LDA), A2
	leaq	(A1, LDA, 2), A

	xorps	%xmm0, %xmm0
	xorps	%xmm1, %xmm1
	xorps	%xmm2, %xmm2
	xorps	%xmm3, %xmm3

#ifdef ALIGNED_ACCESS
	cmpq	M, MM
	je	.L30X

#ifdef movsd
	xorps	%xmm8, %xmm8
#endif
	movsd	-32 * SIZE(A1), %xmm8
#ifdef movsd
	xorps	%xmm9, %xmm9
#endif
	movsd	-32 * SIZE(A2), %xmm9

#ifdef movsd
	xorps	%xmm12, %xmm12
#endif
	movsd	-32 * SIZE(X1), %xmm12

	pshufd	$0xb1, %xmm8,  %xmm4
	mulps	%xmm12,  %xmm8
	addps	%xmm8,  %xmm0
	mulps	%xmm12,  %xmm4
	SUBPS	%xmm4,  %xmm1

	pshufd	$0xb1, %xmm9, %xmm5
	mulps	%xmm12,  %xmm9
	addps	%xmm9, %xmm2
	mulps	%xmm12,  %xmm5
	SUBPS	%xmm5, %xmm3

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, X1
	ALIGN_3
.L30X:
#endif

	movaps	-33 * SIZE(A1), %xmm4
	movaps	-35 * SIZE(A2), %xmm5

	movaps	-32 * SIZE(X1), %xmm12
	movaps	-28 * SIZE(X1), %xmm13

#ifdef PREFETCHW
	PREFETCHW	3 * SIZE(Y1)
#endif

	movq	MM,  I
	sarq	$3,  I
	jle	.L305

	movaps	 -29 * SIZE(A1), %xmm6
	movaps	 -31 * SIZE(A2), %xmm7

	decq	 I
	jle	 .L304
	ALIGN_3

.L303:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(A1)
#endif

	movss	 %xmm6, %xmm4
	shufps	 $0x39, %xmm4, %xmm4
	pshufd	$0xb1,  %xmm4,  %xmm14
	mulps	%xmm12, %xmm4
	addps	%xmm4,  %xmm0
	movaps	 -25 * SIZE(A1), %xmm4
	mulps	%xmm12, %xmm14
	SUBPS	%xmm14, %xmm1

	movss	 %xmm7, %xmm5
	shufps	 $0x93, %xmm7, %xmm5
	pshufd	$0xb1,  %xmm5,  %xmm14
	mulps	%xmm12, %xmm5
	addps	%xmm5,  %xmm2
	movaps	 -27 * SIZE(A2), %xmm5
	mulps	%xmm12, %xmm14
	movaps	-24 * SIZE(X1), %xmm12
	SUBPS	%xmm14, %xmm3

	movss	 %xmm4, %xmm6
	shufps	 $0x39, %xmm6, %xmm6
	pshufd	$0xb1,  %xmm6,  %xmm14
	mulps	%xmm13, %xmm6
	addps	%xmm6,  %xmm0
	movaps	 -21 * SIZE(A1), %xmm6
	mulps	%xmm13, %xmm14
	SUBPS	%xmm14, %xmm1

	movss	 %xmm5, %xmm7
	shufps	 $0x93, %xmm5, %xmm7
	pshufd	$0xb1,  %xmm7,  %xmm14
	mulps	%xmm13, %xmm7
	addps	%xmm7,  %xmm2
	movaps	 -23 * SIZE(A2), %xmm7
	mulps	%xmm13, %xmm14
	movaps	-20 * SIZE(X1), %xmm13
	SUBPS	%xmm14, %xmm3

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(A2)
#endif

	movss	 %xmm6, %xmm4
	shufps	 $0x39, %xmm4, %xmm4
	pshufd	$0xb1,  %xmm4,  %xmm14
	mulps	%xmm12, %xmm4
	addps	%xmm4,  %xmm0
	movaps	 -17 * SIZE(A1), %xmm4
	mulps	%xmm12, %xmm14
	SUBPS	%xmm14, %xmm1

	movss	 %xmm7, %xmm5
	shufps	 $0x93, %xmm7, %xmm5
	pshufd	$0xb1,  %xmm5,  %xmm14
	mulps	%xmm12, %xmm5
	addps	%xmm5,  %xmm2
	movaps	 -19 * SIZE(A2), %xmm5
	mulps	%xmm12, %xmm14
	movaps	-16 * SIZE(X1), %xmm12
	SUBPS	%xmm14, %xmm3

#ifdef PREFETCHW
	PREFETCH	(PREFETCHSIZE) * 2 - 128 + PREOFFSET(X1)
#endif

	movss	 %xmm4, %xmm6
	shufps	 $0x39, %xmm6, %xmm6
	pshufd	$0xb1,  %xmm6,  %xmm14
	mulps	%xmm13, %xmm6
	addps	%xmm6,  %xmm0
	movaps	 -13 * SIZE(A1), %xmm6
	mulps	%xmm13, %xmm14
	SUBPS	%xmm14, %xmm1

	movss	 %xmm5, %xmm7
	shufps	 $0x93, %xmm5, %xmm7
	pshufd	$0xb1,  %xmm7,  %xmm14
	mulps	%xmm13, %xmm7
	addps	%xmm7,  %xmm2
	movaps	 -15 * SIZE(A2), %xmm7
	mulps	%xmm13, %xmm14
	movaps	-12 * SIZE(X1), %xmm13
	SUBPS	%xmm14, %xmm3

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, A2
	subq	 $-16 * SIZE, X1

	subq	 $1, I
	BRANCH
	jg	.L303
	ALIGN_3

.L304:
	movss	 %xmm6, %xmm4
	shufps	 $0x39, %xmm4, %xmm4
	pshufd	$0xb1,  %xmm4,  %xmm14
	mulps	%xmm12, %xmm4
	addps	%xmm4,  %xmm0
	movaps	 -25 * SIZE(A1), %xmm4
	mulps	%xmm12, %xmm14
	SUBPS	%xmm14, %xmm1

	movss	 %xmm7, %xmm5
	shufps	 $0x93, %xmm7, %xmm5
	pshufd	$0xb1,  %xmm5,  %xmm14
	mulps	%xmm12, %xmm5
	addps	%xmm5,  %xmm2
	movaps	 -27 * SIZE(A2), %xmm5
	mulps	%xmm12, %xmm14
	movaps	-24 * SIZE(X1), %xmm12
	SUBPS	%xmm14, %xmm3

	movss	 %xmm4, %xmm6
	shufps	 $0x39, %xmm6, %xmm6
	pshufd	$0xb1,  %xmm6,  %xmm14
	mulps	%xmm13, %xmm6
	addps	%xmm6,  %xmm0
	movaps	 -21 * SIZE(A1), %xmm6
	mulps	%xmm13, %xmm14
	SUBPS	%xmm14, %xmm1

	movss	 %xmm5, %xmm7
	shufps	 $0x93, %xmm5, %xmm7
	pshufd	$0xb1,  %xmm7,  %xmm14
	mulps	%xmm13, %xmm7
	addps	%xmm7,  %xmm2
	movaps	 -23 * SIZE(A2), %xmm7
	mulps	%xmm13, %xmm14
	movaps	-20 * SIZE(X1), %xmm13
	SUBPS	%xmm14, %xmm3

	movss	 %xmm6, %xmm4
	shufps	 $0x39, %xmm4, %xmm4
	pshufd	$0xb1,  %xmm4,  %xmm14
	mulps	%xmm12, %xmm4
	addps	%xmm4,  %xmm0
	movaps	 -17 * SIZE(A1), %xmm4
	mulps	%xmm12, %xmm14
	SUBPS	%xmm14, %xmm1

	movss	 %xmm7, %xmm5
	shufps	 $0x93, %xmm7, %xmm5
	pshufd	$0xb1,  %xmm5,  %xmm14
	mulps	%xmm12, %xmm5
	addps	%xmm5,  %xmm2
	movaps	 -19 * SIZE(A2), %xmm5
	mulps	%xmm12, %xmm14
	movaps	-16 * SIZE(X1), %xmm12
	SUBPS	%xmm14, %xmm3

	movss	 %xmm4, %xmm6
	shufps	 $0x39, %xmm6, %xmm6
	pshufd	$0xb1,  %xmm6,  %xmm14
	mulps	%xmm13, %xmm6
	addps	%xmm6,  %xmm0
	mulps	%xmm13, %xmm14
	SUBPS	%xmm14, %xmm1

	movss	 %xmm5, %xmm7
	shufps	 $0x93, %xmm5, %xmm7
	pshufd	$0xb1,  %xmm7,  %xmm14
	mulps	%xmm13, %xmm7
	addps	%xmm7,  %xmm2
	mulps	%xmm13, %xmm14
	movaps	-12 * SIZE(X1), %xmm13
	SUBPS	%xmm14, %xmm3

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, A2
	subq	 $-16 * SIZE, X1
	ALIGN_3

.L305:
	testq	$4, MM
	je	.L307

	movaps	 -29 * SIZE(A1), %xmm6

	movss	 %xmm6, %xmm4
	shufps	 $0x39, %xmm4, %xmm4
	pshufd	$0xb1,  %xmm4,  %xmm14
	mulps	%xmm12, %xmm4
	addps	%xmm4,  %xmm0
	mulps	%xmm12, %xmm14
	SUBPS	%xmm14, %xmm1

	movaps	 -31 * SIZE(A2), %xmm7

	movss	 %xmm7, %xmm5
	shufps	 $0x93, %xmm7, %xmm5
	pshufd	$0xb1,  %xmm5,  %xmm14
	mulps	%xmm12, %xmm5
	addps	%xmm5,  %xmm2
	mulps	%xmm12, %xmm14
	SUBPS	%xmm14, %xmm3

	movaps	 -25 * SIZE(A1), %xmm8

	movss	 %xmm8, %xmm6
	shufps	 $0x39, %xmm6, %xmm6
	pshufd	$0xb1,  %xmm6,  %xmm14
	mulps	%xmm13, %xmm6
	addps	%xmm6,  %xmm0
	mulps	%xmm13, %xmm14
	SUBPS	%xmm14, %xmm1

	movaps	 -27 * SIZE(A2), %xmm9

	movss	 %xmm9, %xmm7
	shufps	 $0x93, %xmm9, %xmm7
	pshufd	$0xb1,  %xmm7,  %xmm14
	mulps	%xmm13, %xmm7
	addps	%xmm7,  %xmm2
	mulps	%xmm13, %xmm14
	SUBPS	%xmm14, %xmm3

	movaps	%xmm8, %xmm4
	movaps	%xmm9, %xmm5

	movaps	-24 * SIZE(X1), %xmm12
	movaps	-20 * SIZE(X1), %xmm13

	addq	 $8 * SIZE, A1
	addq	 $8 * SIZE, A2
	ALIGN_3

.L307:
	testq	$2, MM
	je	.L308

	movaps	 -29 * SIZE(A1), %xmm6
	movaps	 -31 * SIZE(A2), %xmm7

	movss	 %xmm6, %xmm4
	shufps	 $0x39, %xmm4, %xmm4
	pshufd	$0xb1,  %xmm4,  %xmm14
	mulps	%xmm12, %xmm4
	addps	%xmm4,  %xmm0
	mulps	%xmm12, %xmm14
	SUBPS	%xmm14, %xmm1

	movss	 %xmm7, %xmm5
	shufps	 $0x93, %xmm7, %xmm5
	pshufd	$0xb1,  %xmm5,  %xmm14
	mulps	%xmm12, %xmm5
	addps	%xmm5,  %xmm2
	mulps	%xmm12, %xmm14
	SUBPS	%xmm14, %xmm3

	movaps	 %xmm6, %xmm4
	movaps	 %xmm7, %xmm5
	movaps	 %xmm13, %xmm12

	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, A2
	ALIGN_3

.L308:
	testq	$1, MM
	je	.L309

#ifdef movsd
	xorps	%xmm8, %xmm8
#endif
	movsd	-32 * SIZE(A1), %xmm8
#ifdef movsd
	xorps	%xmm9, %xmm9
#endif
	movsd	-32 * SIZE(A2), %xmm9

	pshufd	$0xb1, %xmm8,  %xmm4
	mulps	%xmm12,  %xmm8
	addps	%xmm8,  %xmm0
	mulps	%xmm12,  %xmm4
	SUBPS	%xmm4,  %xmm1

	pshufd	$0xb1, %xmm9, %xmm5
	mulps	%xmm12,  %xmm9
	addps	%xmm9, %xmm2
	mulps	%xmm12,  %xmm5
	SUBPS	%xmm5, %xmm3
	ALIGN_3

.L309:
	pcmpeqb	%xmm5, %xmm5
	psllq	$63,    %xmm5

#if (!defined(CONJ) && !defined(XCONJ)) || (defined(CONJ) && defined(XCONJ))
	xorps	%xmm5, %xmm0
	xorps	%xmm5, %xmm2
#else
	xorps	%xmm5, %xmm1
	xorps	%xmm5, %xmm3
#endif

#ifdef HAVE_SSE3
	haddps	%xmm1, %xmm0
	haddps	%xmm3, %xmm2
	haddps	%xmm2, %xmm0
#else
	movaps	%xmm0, %xmm8
	unpcklps %xmm1, %xmm0
	unpckhps %xmm1, %xmm8

	movaps	%xmm2, %xmm4
	unpcklps %xmm3, %xmm2
	unpckhps %xmm3, %xmm4

	addps	%xmm8, %xmm0
	addps	%xmm4, %xmm2

	movhlps	%xmm0, %xmm1
	movhlps	%xmm2, %xmm3

	addps	%xmm1, %xmm0
	addps	%xmm3, %xmm2
	movlhps %xmm2, %xmm0
#endif

	pshufd	$0xb1, %xmm0, %xmm1

#ifdef HAVE_SSE3
	movddup	ALPHA, %xmm15
#else
	movsd	ALPHA, %xmm15
	pshufd	$0x44, %xmm15, %xmm15
#endif

	mulps	%xmm15, %xmm0
	mulps	%xmm15, %xmm1

	xorps	%xmm5, %xmm0

#ifdef HAVE_SSE3
	haddps	%xmm1, %xmm0
#else
	movaps	%xmm0, %xmm2
	shufps	$0x88, %xmm1, %xmm0
	shufps	$0xdd, %xmm1, %xmm2

	addps	%xmm2, %xmm0
#endif

	movsd	(Y), %xmm12
	addq	INCY, Y
	movhps	(Y), %xmm12
	addq	INCY, Y

	shufps	$0xd8, %xmm0, %xmm0

	addps	%xmm12, %xmm0

	movlps	%xmm0, (Y1)
	addq	INCY, Y1
	movhps	%xmm0, (Y1)
	addq	INCY, Y1

	cmpq	$2, N
	jge	.L301
	ALIGN_3

.L310:
	cmpq	$1, N
	jl	.L999

	leaq	32 * SIZE(BUFFER), X1

	movq	A, A1

	xorps	%xmm0, %xmm0
	xorps	%xmm1, %xmm1

#ifdef ALIGNED_ACCESS
	cmpq	M, MM
	je	.L31X

#ifdef movsd
	xorps	%xmm8, %xmm8
#endif
	movsd	-32 * SIZE(A1), %xmm8
#ifdef movsd
	xorps	%xmm12, %xmm12
#endif
	movsd	-32 * SIZE(X1), %xmm12

	pshufd	$0xb1, %xmm8,  %xmm4
	mulps	%xmm12,  %xmm8
	addps	%xmm8,  %xmm0
	mulps	%xmm12,  %xmm4
	SUBPS	%xmm4,  %xmm1

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, X1
	ALIGN_3
.L31X:
#endif

	movaps	-33 * SIZE(A1), %xmm4

	movaps	-32 * SIZE(X1), %xmm12
	movaps	-28 * SIZE(X1), %xmm13

	movq	MM,  I
	sarq	$3,  I
	jle	.L315

	movaps	 -29 * SIZE(A1), %xmm5
	movaps	 -25 * SIZE(A1), %xmm6
	movaps	 -21 * SIZE(A1), %xmm7

	decq	 I
	jle	 .L314
	ALIGN_3

.L313:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE) * 4 - 128 + PREOFFSET(A1)
#endif

	movss	 %xmm5, %xmm4
	shufps	 $0x39, %xmm4, %xmm4
	pshufd	$0xb1,  %xmm4,  %xmm14
	mulps	%xmm12, %xmm4
	addps	%xmm4,  %xmm0
	movaps	 -17 * SIZE(A1), %xmm4
	mulps	%xmm12, %xmm14
	movaps	-24 * SIZE(X1), %xmm12
	SUBPS	%xmm14, %xmm1

	movss	 %xmm6, %xmm5
	shufps	 $0x39, %xmm5, %xmm5
	pshufd	$0xb1,  %xmm5,  %xmm15
	mulps	%xmm13, %xmm5
	addps	%xmm5,  %xmm0
	movaps	 -13 * SIZE(A1), %xmm5
	mulps	%xmm13, %xmm15
	movaps	-20 * SIZE(X1), %xmm13
	SUBPS	%xmm15, %xmm1

#ifdef PREFETCHW
	PREFETCH	(PREFETCHSIZE) * 4 - 128 + PREOFFSET(X1)
#endif

	movss	 %xmm7, %xmm6
	shufps	 $0x39, %xmm6, %xmm6
	pshufd	$0xb1,  %xmm6,  %xmm14
	mulps	%xmm12, %xmm6
	addps	%xmm6,  %xmm0
	movaps	  -9 * SIZE(A1), %xmm6
	mulps	%xmm12, %xmm14
	movaps	-16 * SIZE(X1), %xmm12
	SUBPS	%xmm14, %xmm1

	movss	 %xmm4, %xmm7
	shufps	 $0x39, %xmm7, %xmm7
	pshufd	$0xb1,  %xmm7,  %xmm15
	mulps	%xmm13, %xmm7
	addps	%xmm7,  %xmm0
	movaps	  -5 * SIZE(A1), %xmm7
	mulps	%xmm13, %xmm15
	movaps	-12 * SIZE(X1), %xmm13
	SUBPS	%xmm15, %xmm1

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, X1

	subq	 $1, I
	BRANCH
	jg	.L313
	ALIGN_3

.L314:
	movss	 %xmm5, %xmm4
	shufps	 $0x39, %xmm4, %xmm4
	pshufd	$0xb1,  %xmm4,  %xmm14
	mulps	%xmm12, %xmm4
	addps	%xmm4,  %xmm0
	movaps	 -17 * SIZE(A1), %xmm4
	mulps	%xmm12, %xmm14
	movaps	-24 * SIZE(X1), %xmm12
	SUBPS	%xmm14, %xmm1

	movss	 %xmm6, %xmm5
	shufps	 $0x39, %xmm5, %xmm5
	pshufd	$0xb1,  %xmm5,  %xmm15
	mulps	%xmm13, %xmm5
	addps	%xmm5,  %xmm0
	mulps	%xmm13, %xmm15
	movaps	-20 * SIZE(X1), %xmm13
	SUBPS	%xmm15, %xmm1

	movss	 %xmm7, %xmm6
	shufps	 $0x39, %xmm6, %xmm6
	pshufd	$0xb1,  %xmm6,  %xmm14
	mulps	%xmm12, %xmm6
	addps	%xmm6,  %xmm0
	mulps	%xmm12, %xmm14
	movaps	-16 * SIZE(X1), %xmm12
	SUBPS	%xmm14, %xmm1

	movss	 %xmm4, %xmm7
	shufps	 $0x39, %xmm7, %xmm7
	pshufd	$0xb1,  %xmm7,  %xmm15
	mulps	%xmm13, %xmm7
	addps	%xmm7,  %xmm0
	mulps	%xmm13, %xmm15
	movaps	-12 * SIZE(X1), %xmm13
	SUBPS	%xmm15, %xmm1

	subq	 $-16 * SIZE, A1
	subq	 $-16 * SIZE, X1
	ALIGN_3

.L315:
	testq	$4, MM
	je	.L317

	movaps	 -29 * SIZE(A1), %xmm5
	movaps	 -25 * SIZE(A1), %xmm6

	movss	 %xmm5, %xmm4
	shufps	 $0x39, %xmm4, %xmm4
	pshufd	$0xb1,  %xmm4,  %xmm14
	mulps	%xmm12, %xmm4
	addps	%xmm4,  %xmm0
	mulps	%xmm12, %xmm14
	SUBPS	%xmm14, %xmm1

	movss	 %xmm6, %xmm5
	shufps	 $0x39, %xmm5, %xmm5
	pshufd	$0xb1,  %xmm5,  %xmm15
	mulps	%xmm13, %xmm5
	addps	%xmm5,  %xmm0
	mulps	%xmm13, %xmm15
	SUBPS	%xmm15, %xmm1

	movaps	-24 * SIZE(X1), %xmm12
	movaps	-20 * SIZE(X1), %xmm13
	movaps	%xmm6, %xmm4

	addq	 $8 * SIZE, A1
	ALIGN_3

.L317:
	testq	$2, MM
	je	.L318

	movaps	 -29 * SIZE(A1), %xmm5

	movss	 %xmm5, %xmm4
	shufps	 $0x39, %xmm4, %xmm4

	pshufd	$0xb1,  %xmm4,  %xmm14
	mulps	%xmm12, %xmm4
	addps	%xmm4,  %xmm0
	mulps	%xmm12, %xmm14
	SUBPS	%xmm14, %xmm1

	movaps	%xmm13, %xmm12

	addq	 $4 * SIZE, A1
	ALIGN_3

.L318:
	testq	$1, MM
	je	.L319

#ifdef movsd
	xorps	%xmm8, %xmm8
#endif
	movsd	-32 * SIZE(A1), %xmm8

	pshufd	$0xb1, %xmm8,  %xmm4
	mulps	%xmm12,  %xmm8
	addps	%xmm8,  %xmm0
	mulps	%xmm12,  %xmm4
	SUBPS	%xmm4,  %xmm1
	ALIGN_3

.L319:
	pcmpeqb	%xmm5, %xmm5
	psllq	$63,    %xmm5

#if (!defined(CONJ) && !defined(XCONJ)) || (defined(CONJ) && defined(XCONJ))
	xorps	%xmm5, %xmm0
#else
	xorps	%xmm5, %xmm1
#endif

#ifdef HAVE_SSE3
	haddps	%xmm1, %xmm0
	haddps	%xmm0, %xmm0
#else
	movaps	%xmm0, %xmm8
	unpcklps %xmm1, %xmm0
	unpckhps %xmm1, %xmm8

	addps	%xmm8, %xmm0

	movhlps	%xmm0, %xmm1

	addps	%xmm1, %xmm0
#endif

	pshufd	$0xb1, %xmm0, %xmm1

#ifdef HAVE_SSE3
	movddup	ALPHA, %xmm15
#else
	movsd	ALPHA, %xmm15
	pshufd	$0x44, %xmm15, %xmm15
#endif

	mulps	%xmm15, %xmm0
	mulps	%xmm15, %xmm1

	xorps	%xmm5, %xmm0

#ifdef HAVE_SSE3
	haddps	%xmm1, %xmm0
#else
	movaps	%xmm0, %xmm2
	shufps	$0x88, %xmm1, %xmm0
	shufps	$0xdd, %xmm1, %xmm2

	addps	%xmm2, %xmm0
#endif

	movsd	(Y), %xmm12
	addq	INCY, Y

	shufps	$0xd8, %xmm0, %xmm0

	addps	%xmm12, %xmm0

	movlps	%xmm0, (Y1)
	addq	INCY, Y1
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
