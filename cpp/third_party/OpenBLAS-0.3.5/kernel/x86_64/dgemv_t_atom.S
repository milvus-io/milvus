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

#define PREFETCH	prefetchnta
#define PREFETCHW	prefetcht0
#define PREFETCH_SIZE	(8 * 6)

#ifndef WINDOWS_ABI

#define STACKSIZE	 64

#define OLD_INCX	  8 + STACKSIZE(%rsp)
#define OLD_Y		 16 + STACKSIZE(%rsp)
#define OLD_INCY	 24 + STACKSIZE(%rsp)
#define OLD_BUFFER	 32 + STACKSIZE(%rsp)

#define M	  %rdi
#define N	  %rsi
#define A	  %rcx
#define LDA	  %r8
#define X	  %r9
#define INCX	  %rdx
#define Y	  %rbp
#define INCY	  %r10
#define BUFFER	  %rbx

#else

#define STACKSIZE	256

#define OLD_A		 40 + STACKSIZE(%rsp)
#define OLD_LDA		 48 + STACKSIZE(%rsp)
#define OLD_X		 56 + STACKSIZE(%rsp)
#define OLD_INCX	 64 + STACKSIZE(%rsp)
#define OLD_Y		 72 + STACKSIZE(%rsp)
#define OLD_INCY	 80 + STACKSIZE(%rsp)
#define OLD_BUFFER	 88 + STACKSIZE(%rsp)

#define M	  %rcx
#define N	  %rdx
#define A	  %rdi
#define LDA	  %r8
#define X	  %r9
#define INCX	  %rsi
#define Y	  %rbp
#define INCY	  %r10
#define BUFFER	  %rbx

#endif

#define I     %rax
#define J     %r11
#define A1    %r12
#define A2    %r13
#define X1    %r14
#define Y1    %r15

#define ALPHA %xmm3

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

	movq	OLD_A,        A
	movq	OLD_LDA,      LDA
	movq	OLD_X,        X
#endif

	movq	OLD_INCX,   INCX
	movq	OLD_Y,      Y
	movq	OLD_INCY,   INCY
	movq	OLD_BUFFER, BUFFER

	leaq	(,INCX, SIZE), INCX
	leaq	(,INCY, SIZE), INCY
	leaq	(, LDA, SIZE), LDA

#ifndef WINDOWS_ABI
	movapd	 %xmm0, ALPHA
#endif

	movq	Y, Y1

	testq	M, M
	jle	.L999
	testq	N, N
	jle	.L999

	cmpq	$SIZE, INCX
	cmoveq	X, BUFFER
	je	.L10

	movq	BUFFER, X1

	movq	M,  I
	sarq	$3, I
	jle	.L05
	ALIGN_3

.L02:
	movsd	(X), %xmm0
	addq	INCX, X
	movsd	(X), %xmm1
	addq	INCX, X

	movsd	(X), %xmm2
	addq	INCX, X
	movsd	(X), %xmm8
	addq	INCX, X

	movsd	(X), %xmm4
	addq	INCX, X
	movsd	(X), %xmm5
	addq	INCX, X

	movsd	(X), %xmm6
	addq	INCX, X
	movsd	(X), %xmm7
	addq	INCX, X

	movsd	%xmm0, 0 * SIZE(X1)
	movsd	%xmm1, 1 * SIZE(X1)
	movsd	%xmm2, 2 * SIZE(X1)
	movsd	%xmm8, 3 * SIZE(X1)
	movsd	%xmm4, 4 * SIZE(X1)
	movsd	%xmm5, 5 * SIZE(X1)
	movsd	%xmm6, 6 * SIZE(X1)
	movsd	%xmm7, 7 * SIZE(X1)

	addq	$8 * SIZE, X1
	decq	I
	jg	.L02
	ALIGN_3

.L05:
	movq	M,  I
	andq	$7, I
	jle	.L10
	ALIGN_3

.L06:
	movsd	(X), %xmm0
	addq	INCX, X

	movsd	%xmm0, (X1)
	addq	$SIZE, X1

	decq	I
	jg	.L06
	ALIGN_3

.L10:
	movq	N,  J
	sarq	$1, J
	jle	.L20
	ALIGN_3

.L11:
	movq	A, A1
	leaq	(A, LDA, 1), A2
	leaq	(A, LDA, 2), A

	movq	BUFFER, X1

	xorps	%xmm0, %xmm0
	xorps	%xmm1, %xmm1
	PREFETCHW	1 * SIZE(X1)

	movq	M,  I
	sarq	$3, I
	jle	.L14

	movsd	 0 * SIZE(X1), %xmm4
	movsd	 0 * SIZE(A1), %xmm8
	movsd	 0 * SIZE(A2), %xmm12

	movsd	 1 * SIZE(X1), %xmm5
	movsd	 1 * SIZE(A1), %xmm9
	movsd	 1 * SIZE(A2), %xmm13

	movsd	 2 * SIZE(X1), %xmm6
	movsd	 2 * SIZE(A1), %xmm10
	movsd	 2 * SIZE(A2), %xmm14

	movsd	 3 * SIZE(X1), %xmm7
	mulsd	%xmm4, %xmm8
	movsd	 3 * SIZE(A1), %xmm11
	mulsd	%xmm4, %xmm12
	movsd	 4 * SIZE(X1), %xmm4

	mulsd	%xmm5, %xmm9
	movsd	 3 * SIZE(A2), %xmm15
	mulsd	%xmm5, %xmm13
	movsd	 5 * SIZE(X1), %xmm5

	decq	I
	jle	.L13
	ALIGN_3

.L12:
	PREFETCH	PREFETCH_SIZE * SIZE(A1)
	addsd	%xmm8,  %xmm0
	PREFETCH	PREFETCH_SIZE * SIZE(A2)
	mulsd	%xmm6, %xmm10
	movsd	 4 * SIZE(A1), %xmm8
	addsd	%xmm12, %xmm1
	movsd	 4 * SIZE(A2), %xmm12
	mulsd	%xmm6, %xmm14
	movsd	 6 * SIZE(X1), %xmm6

	addsd	%xmm9,  %xmm0
	movsd	 5 * SIZE(A1), %xmm9
	mulsd	%xmm7, %xmm11
	addsd	%xmm13, %xmm1
	movsd	 5 * SIZE(A2), %xmm13
	mulsd	%xmm7, %xmm15
	movsd	 7 * SIZE(X1), %xmm7

	addsd	%xmm10, %xmm0
	movsd	 6 * SIZE(A1), %xmm10
	mulsd	%xmm4, %xmm8
	addsd	%xmm14, %xmm1
	movsd	 6 * SIZE(A2), %xmm14
	mulsd	%xmm4, %xmm12
	movsd	 8 * SIZE(X1), %xmm4

	addsd	%xmm11, %xmm0
	movsd	 7 * SIZE(A1), %xmm11
	mulsd	%xmm5, %xmm9
	addsd	%xmm15, %xmm1
	movsd	 7 * SIZE(A2), %xmm15
	mulsd	%xmm5, %xmm13
	movsd	 9 * SIZE(X1), %xmm5

	addsd	%xmm8,  %xmm0
	movsd	 8 * SIZE(A1), %xmm8
	mulsd	%xmm6, %xmm10
	addq	$8 * SIZE, X1
	addsd	%xmm12, %xmm1
	movsd	 8 * SIZE(A2), %xmm12
	mulsd	%xmm6, %xmm14
	movsd	 2 * SIZE(X1), %xmm6

	addsd	%xmm9,  %xmm0
	movsd	 9 * SIZE(A1), %xmm9
	mulsd	%xmm7, %xmm11
	addq	$8 * SIZE, A2
	addsd	%xmm13, %xmm1
	movsd	 1 * SIZE(A2), %xmm13
	mulsd	%xmm7, %xmm15
	movsd	 3 * SIZE(X1), %xmm7

	addsd	%xmm10, %xmm0
	movsd	10 * SIZE(A1), %xmm10
	mulsd	%xmm4, %xmm8
	addq	$8 * SIZE, A1
	addsd	%xmm14, %xmm1
	movsd	 2 * SIZE(A2), %xmm14
	mulsd	%xmm4, %xmm12
	movsd	 4 * SIZE(X1), %xmm4

	addsd	%xmm11, %xmm0
	movsd	 3 * SIZE(A1), %xmm11
	mulsd	%xmm5, %xmm9
	decq	I
	addsd	%xmm15, %xmm1
	movsd	 3 * SIZE(A2), %xmm15
	mulsd	%xmm5, %xmm13
	movsd	 5 * SIZE(X1), %xmm5

	jg	.L12
	ALIGN_3

.L13:
	addsd	%xmm8,  %xmm0
	movsd	 4 * SIZE(A1), %xmm8
	mulsd	%xmm6, %xmm10
	addsd	%xmm12, %xmm1
	movsd	 4 * SIZE(A2), %xmm12
	mulsd	%xmm6, %xmm14
	movsd	 6 * SIZE(X1), %xmm6

	addsd	%xmm9,  %xmm0
	movsd	 5 * SIZE(A1), %xmm9
	mulsd	%xmm7, %xmm11
	addsd	%xmm13, %xmm1
	movsd	 5 * SIZE(A2), %xmm13
	mulsd	%xmm7, %xmm15
	movsd	 7 * SIZE(X1), %xmm7

	addsd	%xmm10, %xmm0
	movsd	 6 * SIZE(A1), %xmm10
	mulsd	%xmm4, %xmm8
	addsd	%xmm14, %xmm1
	movsd	 6 * SIZE(A2), %xmm14
	mulsd	%xmm4, %xmm12

	addsd	%xmm11, %xmm0
	movsd	 7 * SIZE(A1), %xmm11
	mulsd	%xmm5, %xmm9
	addsd	%xmm15, %xmm1
	movsd	 7 * SIZE(A2), %xmm15
	mulsd	%xmm5, %xmm13

	addsd	%xmm8,  %xmm0
	mulsd	%xmm6, %xmm10
	addsd	%xmm12, %xmm1
	mulsd	%xmm6, %xmm14

	addsd	%xmm9,  %xmm0
	mulsd	%xmm7, %xmm11
	addsd	%xmm13, %xmm1
	mulsd	%xmm7, %xmm15

	addsd	%xmm10, %xmm0
	addq	$8 * SIZE, A1
	addsd	%xmm14, %xmm1
	addq	$8 * SIZE, A2
	addsd	%xmm11, %xmm0
	addq	$8 * SIZE, X1
	addsd	%xmm15, %xmm1
	ALIGN_4

.L14:
	testq	$4, M
	je	.L16

	movsd	 0 * SIZE(X1), %xmm4
	movsd	 0 * SIZE(A1), %xmm8
	movsd	 0 * SIZE(A2), %xmm12

	movsd	 1 * SIZE(X1), %xmm5
	movsd	 1 * SIZE(A1), %xmm9
	movsd	 1 * SIZE(A2), %xmm13

	movsd	 2 * SIZE(X1), %xmm6
	movsd	 2 * SIZE(A1), %xmm10
	movsd	 2 * SIZE(A2), %xmm14

	movsd	 3 * SIZE(X1), %xmm7
	movsd	 3 * SIZE(A1), %xmm11
	movsd	 3 * SIZE(A2), %xmm15

	mulsd	%xmm4, %xmm8
	mulsd	%xmm4, %xmm12
	mulsd	%xmm5, %xmm9
	mulsd	%xmm5, %xmm13

	addsd	%xmm8,  %xmm0
	addsd	%xmm12, %xmm1
	addsd	%xmm9,  %xmm0
	addsd	%xmm13, %xmm1

	mulsd	%xmm6, %xmm10
	mulsd	%xmm6, %xmm14
	mulsd	%xmm7, %xmm11
	mulsd	%xmm7, %xmm15

	addsd	%xmm10, %xmm0
	addsd	%xmm14, %xmm1
	addsd	%xmm11, %xmm0
	addsd	%xmm15, %xmm1

	addq	$4 * SIZE, A1
	addq	$4 * SIZE, A2
	addq	$4 * SIZE, X1
	ALIGN_4

.L16:
	testq	$2, M
	je	.L17

	movsd	 0 * SIZE(X1), %xmm4
	movsd	 0 * SIZE(A1), %xmm8
	movsd	 0 * SIZE(A2), %xmm12

	movsd	 1 * SIZE(X1), %xmm5
	movsd	 1 * SIZE(A1), %xmm9
	movsd	 1 * SIZE(A2), %xmm13

	mulsd	%xmm4, %xmm8
	mulsd	%xmm4, %xmm12
	mulsd	%xmm5, %xmm9
	mulsd	%xmm5, %xmm13

	addsd	%xmm8,  %xmm0
	addsd	%xmm12, %xmm1
	addsd	%xmm9,  %xmm0
	addsd	%xmm13, %xmm1

	addq	$2 * SIZE, A1
	addq	$2 * SIZE, A2
	addq	$2 * SIZE, X1

	ALIGN_4

.L17:
	testq	$1, M
	je	.L19

	movsd	 0 * SIZE(X1), %xmm4

	movsd	 0 * SIZE(A1), %xmm8
	movsd	 0 * SIZE(A2), %xmm12

	mulsd	%xmm4, %xmm8
	mulsd	%xmm4, %xmm12

	addsd	%xmm8,  %xmm0
	addsd	%xmm12, %xmm1
	ALIGN_4

.L19:
	mulsd	ALPHA, %xmm0
	addsd	(Y), %xmm0
	addq	INCY, Y

	mulsd	ALPHA, %xmm1
	addsd	(Y), %xmm1
	addq	INCY, Y

	movsd	%xmm0, (Y1)
	addq	INCY, Y1
	movsd	%xmm1, (Y1)
	addq	INCY, Y1

	decq	J
	jg	.L11
	ALIGN_3

.L20:
	testq	$1, N
	jle	.L999

	movq	A, A1
	movq	BUFFER, X1

	xorps	%xmm0, %xmm0
	xorps	%xmm1, %xmm1

	movq	M,  I
	sarq	$3, I
	jle	.L24

	movsd	 0 * SIZE(X1), %xmm4
	movsd	 0 * SIZE(A1), %xmm8
	movsd	 1 * SIZE(X1), %xmm5
	movsd	 1 * SIZE(A1), %xmm9

	movsd	 2 * SIZE(X1), %xmm6
	movsd	 2 * SIZE(A1), %xmm10
	movsd	 3 * SIZE(X1), %xmm7
	movsd	 3 * SIZE(A1), %xmm11

	mulsd	%xmm4, %xmm8
	movsd	 4 * SIZE(X1), %xmm4
	mulsd	%xmm5, %xmm9
	movsd	 5 * SIZE(X1), %xmm5
	mulsd	%xmm6, %xmm10
	movsd	 6 * SIZE(X1), %xmm6
	mulsd	%xmm7, %xmm11
	movsd	 7 * SIZE(X1), %xmm7

	decq	I
	jle	.L23
	ALIGN_3

.L22:
	PREFETCH	PREFETCH_SIZE * SIZE(A1)
	addsd	%xmm8,  %xmm0
	movsd	 4 * SIZE(A1), %xmm8
	addsd	%xmm9,  %xmm0
	movsd	 5 * SIZE(A1), %xmm9
	addsd	%xmm10, %xmm0
	movsd	 6 * SIZE(A1), %xmm10
	addsd	%xmm11, %xmm0
	movsd	 7 * SIZE(A1), %xmm11

	mulsd	%xmm4, %xmm8
	movsd	 8 * SIZE(X1), %xmm4
	mulsd	%xmm5, %xmm9
 	movsd	 9 * SIZE(X1), %xmm5
	mulsd	%xmm6, %xmm10
	movsd	10 * SIZE(X1), %xmm6
	mulsd	%xmm7, %xmm11
	movsd	11 * SIZE(X1), %xmm7

	addsd	%xmm8,  %xmm0
	movsd	 8 * SIZE(A1), %xmm8
	addsd	%xmm9,  %xmm1
	movsd	 9 * SIZE(A1), %xmm9
	addsd	%xmm10, %xmm1
	movsd	10 * SIZE(A1), %xmm10
	addsd	%xmm11, %xmm0
	movsd	11 * SIZE(A1), %xmm11

	mulsd	%xmm4, %xmm8
	movsd	12 * SIZE(X1), %xmm4
	mulsd	%xmm5, %xmm9
	movsd	13 * SIZE(X1), %xmm5
	mulsd	%xmm6, %xmm10
	movsd	14 * SIZE(X1), %xmm6
	mulsd	%xmm7, %xmm11
	movsd	15 * SIZE(X1), %xmm7

	addq	$8 * SIZE, A1
	addq	$8 * SIZE, X1
	decq	I
	jg	.L22
	ALIGN_3

.L23:
	addsd	%xmm8,  %xmm0
	movsd	 4 * SIZE(A1), %xmm8
	addsd	%xmm9,  %xmm1
	movsd	 5 * SIZE(A1), %xmm9
	addsd	%xmm10, %xmm0
	movsd	 6 * SIZE(A1), %xmm10
	addsd	%xmm11, %xmm1
	movsd	 7 * SIZE(A1), %xmm11

	mulsd	%xmm4, %xmm8
	mulsd	%xmm5, %xmm9
	mulsd	%xmm6, %xmm10
	mulsd	%xmm7, %xmm11

	addsd	%xmm8,  %xmm0
	addsd	%xmm9,  %xmm1
	addsd	%xmm10, %xmm0
	addq	$8 * SIZE, A1
	addsd	%xmm11, %xmm1
	addq	$8 * SIZE, X1
	ALIGN_4

.L24:
	testq	$4, M
	je	.L26

	movsd	 0 * SIZE(X1), %xmm4
	movsd	 0 * SIZE(A1), %xmm8
	movsd	 1 * SIZE(X1), %xmm5
	movsd	 1 * SIZE(A1), %xmm9

	movsd	 2 * SIZE(X1), %xmm6
	movsd	 2 * SIZE(A1), %xmm10
	movsd	 3 * SIZE(X1), %xmm7
	movsd	 3 * SIZE(A1), %xmm11

	mulsd	%xmm4,  %xmm8
	mulsd	%xmm5,  %xmm9
	mulsd	%xmm6,  %xmm10
	mulsd	%xmm7,  %xmm11

	addsd	%xmm8,  %xmm0
	addsd	%xmm9,  %xmm1
	addsd	%xmm10, %xmm0
	addq	$4 * SIZE, A1
	addsd	%xmm11, %xmm1
	addq	$4 * SIZE, X1
	ALIGN_4

.L26:
	testq	$2, M
	je	.L27

	movsd	 0 * SIZE(X1), %xmm4
	movsd	 0 * SIZE(A1), %xmm8
	movsd	 1 * SIZE(X1), %xmm5
	movsd	 1 * SIZE(A1), %xmm9

	mulsd	%xmm4, %xmm8
	mulsd	%xmm5, %xmm9
	addsd	%xmm8,  %xmm0
	addq	$2 * SIZE, A1
	addsd	%xmm9,  %xmm1
	addq	$2 * SIZE, X1
	ALIGN_4

.L27:
	testq	$1, M
	je	.L29

	movsd	 0 * SIZE(X1), %xmm4
	movsd	 0 * SIZE(A1), %xmm8

	mulsd	%xmm4,  %xmm8
	addsd	%xmm8,  %xmm0
	ALIGN_4

.L29:
	addsd	%xmm1, %xmm0

	mulsd	ALPHA, %xmm0

	addsd	(Y), %xmm0
	movsd	%xmm0, (Y1)
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
	ALIGN_3

	EPILOGUE
