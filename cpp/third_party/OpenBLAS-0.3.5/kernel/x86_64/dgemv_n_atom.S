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

#define STACKSIZE	64

#define OLD_INCX	 8 + STACKSIZE(%rsp)
#define OLD_Y		16 + STACKSIZE(%rsp)
#define OLD_INCY	24 + STACKSIZE(%rsp)
#define OLD_BUFFER	32 + STACKSIZE(%rsp)
#define STACK_ALPHA	48	      (%rsp)

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

#define OLD_A		 40 + STACKSIZE(%rsp)
#define OLD_LDA		 48 + STACKSIZE(%rsp)
#define OLD_X		 56 + STACKSIZE(%rsp)
#define OLD_INCX	 64 + STACKSIZE(%rsp)
#define OLD_Y		 72 + STACKSIZE(%rsp)
#define OLD_INCY	 80 + STACKSIZE(%rsp)
#define OLD_BUFFER	 88 + STACKSIZE(%rsp)

#define STACK_ALPHA	224	       (%rsp)

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
#define MM	%rbx

#define ALPHA %xmm15

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
#endif
	movq	OLD_INCX,   INCX
	movq	OLD_Y,      Y
	movq	OLD_INCY,   INCY
	movq	OLD_BUFFER, BUFFER

#ifndef WINDOWS_ABI
	movsd	 %xmm0, STACK_ALPHA
#else
	movsd	 %xmm3, STACK_ALPHA
#endif

	leaq	(,INCX, SIZE), INCX
	leaq	(,INCY, SIZE), INCY
	leaq	(,LDA,  SIZE), LDA

	testq	N, N
	jle	.L999
	testq	M, M
	jle	.L999

	cmpq	$SIZE, INCY
	cmoveq	Y, BUFFER
	je	.L10

	movq	BUFFER, Y1
	xorps	%xmm4, %xmm4

	movq	M,  %rax
	addq	$7, %rax
	sarq	$3, %rax
	ALIGN_3

.L01:
	movsd	%xmm4, 0 * SIZE(Y1)
	movsd	%xmm4, 1 * SIZE(Y1)
	movsd	%xmm4, 2 * SIZE(Y1)
	movsd	%xmm4, 3 * SIZE(Y1)
	movsd	%xmm4, 4 * SIZE(Y1)
	movsd	%xmm4, 5 * SIZE(Y1)
	movsd	%xmm4, 6 * SIZE(Y1)
	movsd	%xmm4, 7 * SIZE(Y1)

	addq	$8 * SIZE, Y1
	decq	%rax
	jg	.L01
	ALIGN_3

.L10:
	movq	N,  J
	sarq	$1, J
	jle	.L20
	ALIGN_3

.L11:
	movq	BUFFER, Y1

	movq	A,  A1
	leaq	(A, LDA, 1), A2
	leaq	(A, LDA, 2), A

	movsd	STACK_ALPHA, %xmm0

	movsd	(X), %xmm14
	addq	INCX, X
	movsd	(X), %xmm15
	addq	INCX, X

	mulsd	%xmm0, %xmm14
	mulsd	%xmm0, %xmm15

	movq	M,  I
	sarq	$3, I
	jle	.L15

	movsd	 0 * SIZE(A1), %xmm0
	movsd	 1 * SIZE(A1), %xmm1
	movsd	 2 * SIZE(A1), %xmm2
	movsd	 3 * SIZE(A1), %xmm3

	movsd	 0 * SIZE(A2), %xmm4
	movsd	 1 * SIZE(A2), %xmm5
	movsd	 2 * SIZE(A2), %xmm6
	movsd	 3 * SIZE(A2), %xmm7

	movsd	 0 * SIZE(Y1), %xmm8
	mulsd	 %xmm14, %xmm0
	movsd	 1 * SIZE(Y1), %xmm9
	mulsd	 %xmm14, %xmm1
	movsd	 2 * SIZE(Y1), %xmm10
	mulsd	 %xmm14, %xmm2
	movsd	 3 * SIZE(Y1), %xmm11
	mulsd	 %xmm14, %xmm3

	decq	 I
	jle	 .L14
	ALIGN_3

.L13:
	PREFETCH	PREFETCH_SIZE * SIZE(A1)
	mulsd	 %xmm15, %xmm4
	PREFETCH	PREFETCH_SIZE * SIZE(A2)
	addsd	 %xmm0,  %xmm8
	movsd	 4 * SIZE(A1), %xmm0

	mulsd	 %xmm15, %xmm5
	addsd	 %xmm1,  %xmm9
	movsd	 5 * SIZE(A1), %xmm1

	mulsd	 %xmm15, %xmm6
	addsd	 %xmm2,  %xmm10
	movsd	 6 * SIZE(A1), %xmm2

	mulsd	 %xmm15, %xmm7
	addsd	 %xmm3,  %xmm11
	movsd	 7 * SIZE(A1), %xmm3

	addsd	 %xmm4,  %xmm8
	mulsd	 %xmm14, %xmm0
	movsd	 4 * SIZE(A2), %xmm4

	addsd	 %xmm5,  %xmm9
	mulsd	 %xmm14, %xmm1
	movsd	 5 * SIZE(A2), %xmm5

	addsd	 %xmm6,  %xmm10
	mulsd	 %xmm14, %xmm2
	movsd	 6 * SIZE(A2), %xmm6

	addsd	 %xmm7,  %xmm11
	mulsd	 %xmm14, %xmm3
	movsd	 7 * SIZE(A2), %xmm7

	movsd	 %xmm8,  0 * SIZE(Y1)
	movsd	 4 * SIZE(Y1), %xmm8
	movsd	 %xmm9,  1 * SIZE(Y1)
	movsd	 5 * SIZE(Y1), %xmm9

	movsd	 %xmm10, 2 * SIZE(Y1)
	movsd	 6 * SIZE(Y1), %xmm10
	movsd	 %xmm11, 3 * SIZE(Y1)
	movsd	 7 * SIZE(Y1), %xmm11

	mulsd	 %xmm15, %xmm4
	addsd	 %xmm0,  %xmm8
	movsd	 8 * SIZE(A1), %xmm0

	mulsd	 %xmm15, %xmm5
	addsd	 %xmm1,  %xmm9
	movsd	 9 * SIZE(A1), %xmm1

	mulsd	 %xmm15, %xmm6
	addsd	 %xmm2,  %xmm10
	movsd	10 * SIZE(A1), %xmm2

	mulsd	 %xmm15, %xmm7
	addq	 $8 * SIZE, A2
	addsd	 %xmm3,  %xmm11
	movsd	11 * SIZE(A1), %xmm3

	mulsd	 %xmm14, %xmm0
	addsd	 %xmm4,  %xmm8
	movsd	 0 * SIZE(A2), %xmm4

	mulsd	 %xmm14, %xmm1
	addq	 $8 * SIZE, Y1
	addsd	 %xmm5,  %xmm9
	movsd	 1 * SIZE(A2), %xmm5

	mulsd	 %xmm14, %xmm2
	addq	 $8 * SIZE, A1
	addsd	 %xmm6,  %xmm10
	movsd	 2 * SIZE(A2), %xmm6

	mulsd	 %xmm14, %xmm3
	decq	 I
	addsd	 %xmm7,  %xmm11
	movsd	 3 * SIZE(A2), %xmm7

	movsd	 %xmm8, -4 * SIZE(Y1)
	movsd	 0 * SIZE(Y1), %xmm8
	movsd	 %xmm9, -3 * SIZE(Y1)
	movsd	 1 * SIZE(Y1), %xmm9

	movsd	 %xmm10,-2 * SIZE(Y1)
	movsd	 2 * SIZE(Y1), %xmm10
	movsd	 %xmm11,-1 * SIZE(Y1)
	movsd	 3 * SIZE(Y1), %xmm11
	jg	.L13
	ALIGN_3

.L14:
	mulsd	 %xmm15, %xmm4
	addsd	 %xmm0,  %xmm8
	movsd	 4 * SIZE(A1), %xmm0

	mulsd	 %xmm15, %xmm5
	addsd	 %xmm1,  %xmm9
	movsd	 5 * SIZE(A1), %xmm1

	mulsd	 %xmm15, %xmm6
	addsd	 %xmm2,  %xmm10
	movsd	 6 * SIZE(A1), %xmm2

	mulsd	 %xmm15, %xmm7
	addsd	 %xmm3,  %xmm11
	movsd	 7 * SIZE(A1), %xmm3

	addsd	 %xmm4,  %xmm8
	mulsd	 %xmm14, %xmm0
	movsd	 4 * SIZE(A2), %xmm4

	addsd	 %xmm5,  %xmm9
	mulsd	 %xmm14, %xmm1
	movsd	 5 * SIZE(A2), %xmm5

	addsd	 %xmm6,  %xmm10
	mulsd	 %xmm14, %xmm2
	movsd	 6 * SIZE(A2), %xmm6

	addsd	 %xmm7,  %xmm11
	mulsd	 %xmm14, %xmm3
	movsd	 7 * SIZE(A2), %xmm7

	movsd	 %xmm8,  0 * SIZE(Y1)
	movsd	 4 * SIZE(Y1), %xmm8
	movsd	 %xmm9,  1 * SIZE(Y1)
	movsd	 5 * SIZE(Y1), %xmm9

	movsd	 %xmm10, 2 * SIZE(Y1)
	movsd	 6 * SIZE(Y1), %xmm10
	movsd	 %xmm11, 3 * SIZE(Y1)
	movsd	 7 * SIZE(Y1), %xmm11

	mulsd	 %xmm15, %xmm4
	addsd	 %xmm0,  %xmm8

	mulsd	 %xmm15, %xmm5
	addsd	 %xmm1,  %xmm9

	mulsd	 %xmm15, %xmm6
	addsd	 %xmm2,  %xmm10

	mulsd	 %xmm15, %xmm7
	addq	 $8 * SIZE, A2
	addsd	 %xmm3,  %xmm11

	mulsd	 %xmm14, %xmm0
	addsd	 %xmm4,  %xmm8

	mulsd	 %xmm14, %xmm1
	addq	 $8 * SIZE, Y1
	addsd	 %xmm5,  %xmm9

	mulsd	 %xmm14, %xmm2
	addq	 $8 * SIZE, A1
	addsd	 %xmm6,  %xmm10

	mulsd	 %xmm14, %xmm3
	addsd	 %xmm7,  %xmm11

	movsd	 %xmm8, -4 * SIZE(Y1)
	movsd	 %xmm9, -3 * SIZE(Y1)
	movsd	 %xmm10,-2 * SIZE(Y1)
	movsd	 %xmm11,-1 * SIZE(Y1)
	ALIGN_3

.L15:
	testq	$4, M
	je	.L17

	movsd	 0 * SIZE(A1), %xmm0
	movsd	 1 * SIZE(A1), %xmm1
	movsd	 2 * SIZE(A1), %xmm2
	movsd	 3 * SIZE(A1), %xmm3

	movsd	 0 * SIZE(A2), %xmm4
	movsd	 1 * SIZE(A2), %xmm5
	movsd	 2 * SIZE(A2), %xmm6
	movsd	 3 * SIZE(A2), %xmm7

	movsd	 0 * SIZE(Y1), %xmm8
	mulsd	 %xmm14, %xmm0
	movsd	 1 * SIZE(Y1), %xmm9
	mulsd	 %xmm14, %xmm1
	movsd	 2 * SIZE(Y1), %xmm10
	mulsd	 %xmm14, %xmm2
	movsd	 3 * SIZE(Y1), %xmm11
	mulsd	 %xmm14, %xmm3

	mulsd	 %xmm15, %xmm4
	addsd	 %xmm0,  %xmm8
	mulsd	 %xmm15, %xmm5
	addsd	 %xmm1,  %xmm9
	mulsd	 %xmm15, %xmm6
	addsd	 %xmm2,  %xmm10
	mulsd	 %xmm15, %xmm7
	addsd	 %xmm3,  %xmm11

	addsd	 %xmm4,  %xmm8
	addsd	 %xmm5,  %xmm9
	addsd	 %xmm6,  %xmm10
	addsd	 %xmm7,  %xmm11

	movsd	 %xmm8,  0 * SIZE(Y1)
	movsd	 %xmm9,  1 * SIZE(Y1)
	movsd	 %xmm10, 2 * SIZE(Y1)
	movsd	 %xmm11, 3 * SIZE(Y1)

	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, A2
	addq	 $4 * SIZE, Y1
	ALIGN_3

.L17:
	testq	$2, M
	je	.L18

	movsd	 0 * SIZE(A1), %xmm0
	movsd	 1 * SIZE(A1), %xmm1

	movsd	 0 * SIZE(A2), %xmm4
	movsd	 1 * SIZE(A2), %xmm5

	mulsd	 %xmm14, %xmm0
	movsd	 0 * SIZE(Y1), %xmm8
	mulsd	 %xmm14, %xmm1
	movsd	 1 * SIZE(Y1), %xmm9
	mulsd	 %xmm15, %xmm4
	mulsd	 %xmm15, %xmm5

	addsd	 %xmm0,  %xmm8
	addsd	 %xmm1,  %xmm9
	addsd	 %xmm4,  %xmm8
	addsd	 %xmm5,  %xmm9

	movsd	 %xmm8, 0 * SIZE(Y1)
	movsd	 %xmm9, 1 * SIZE(Y1)

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, A2
	addq	 $2 * SIZE, Y1
	ALIGN_3

.L18:
	testq	$1, M
	je	.L19

	movsd	 0 * SIZE(Y1), %xmm8

	movsd	 0 * SIZE(A1), %xmm0
	movsd	 0 * SIZE(A2), %xmm4

	mulsd	 %xmm14, %xmm0
	mulsd	 %xmm15, %xmm4

	addsd	 %xmm0,  %xmm8
	addsd	 %xmm4,  %xmm8

	movsd	 %xmm8, 0 * SIZE(Y1)
	ALIGN_3

.L19:
	decq	J
	jg	.L11
	ALIGN_3

.L20:
	testq	$1, N
	je	.L990

	movq	BUFFER, Y1
	movq	A,  A1

	movsd	(X), %xmm14
	mulsd	STACK_ALPHA, %xmm14

	movq	M,  I
	sarq	$3, I
	jle	.L25

	movsd	 0 * SIZE(A1), %xmm0
	movsd	 1 * SIZE(A1), %xmm1
	movsd	 2 * SIZE(A1), %xmm2
	movsd	 3 * SIZE(A1), %xmm3

 	movsd	 4 * SIZE(A1), %xmm4
 	movsd	 5 * SIZE(A1), %xmm5
	movsd	 6 * SIZE(A1), %xmm6
	movsd	 7 * SIZE(A1), %xmm7

	movsd	 0 * SIZE(Y1), %xmm8
	mulsd	 %xmm14, %xmm0
	movsd	 1 * SIZE(Y1), %xmm9
	mulsd	 %xmm14, %xmm1
	movsd	 2 * SIZE(Y1), %xmm10
	mulsd	 %xmm14, %xmm2
	movsd	 3 * SIZE(Y1), %xmm11
	mulsd	 %xmm14, %xmm3

	decq	 I
	jle	 .L24
	ALIGN_3

.L23:
	PREFETCH	PREFETCH_SIZE * SIZE(A1)

	addsd	 %xmm0,  %xmm8
	movsd	  8 * SIZE(A1), %xmm0
	addsd	 %xmm1,  %xmm9
	movsd	  9 * SIZE(A1), %xmm1
	addsd	 %xmm2,  %xmm10
	movsd	 10 * SIZE(A1), %xmm2
	addsd	 %xmm3,  %xmm11
	movsd	 11 * SIZE(A1), %xmm3

	movsd	 %xmm8,  0 * SIZE(Y1)
	movsd	 4 * SIZE(Y1), %xmm8
	mulsd	 %xmm14, %xmm4
	movsd	 %xmm9,  1 * SIZE(Y1)
	movsd	 5 * SIZE(Y1), %xmm9
	mulsd	 %xmm14, %xmm5

	movsd	 %xmm10, 2 * SIZE(Y1)
	movsd	 6 * SIZE(Y1), %xmm10
	mulsd	 %xmm14, %xmm6

	movsd	 %xmm11, 3 * SIZE(Y1)
	movsd	 7 * SIZE(Y1), %xmm11
	mulsd	 %xmm14, %xmm7

	addsd	 %xmm4,  %xmm8
 	movsd	 12 * SIZE(A1), %xmm4
	addsd	 %xmm5,  %xmm9
 	movsd	 13 * SIZE(A1), %xmm5
	addsd	 %xmm6,  %xmm10
	movsd	 14 * SIZE(A1), %xmm6
	addsd	 %xmm7,  %xmm11
	movsd	 15 * SIZE(A1), %xmm7

	movsd	 %xmm8,  4 * SIZE(Y1)
	movsd	  8 * SIZE(Y1), %xmm8
	mulsd	 %xmm14, %xmm0
	movsd	 %xmm9,  5 * SIZE(Y1)
	movsd	  9 * SIZE(Y1), %xmm9
	mulsd	 %xmm14, %xmm1
	movsd	 %xmm10, 6 * SIZE(Y1)
	movsd	 10 * SIZE(Y1), %xmm10
	mulsd	 %xmm14, %xmm2
	movsd	 %xmm11, 7 * SIZE(Y1)
	movsd	 11 * SIZE(Y1), %xmm11
	mulsd	 %xmm14, %xmm3

	addq	 $8 * SIZE, Y1
	addq	 $8 * SIZE, A1

	decq	 I
	jg	.L23
	ALIGN_3

.L24:
	addsd	 %xmm0,  %xmm8
	addsd	 %xmm1,  %xmm9
	addsd	 %xmm2,  %xmm10
	addsd	 %xmm3,  %xmm11

	mulsd	 %xmm14, %xmm4
	movsd	 %xmm8,  0 * SIZE(Y1)
	movsd	 4 * SIZE(Y1), %xmm8

	mulsd	 %xmm14, %xmm5
	movsd	 %xmm9,  1 * SIZE(Y1)
	movsd	 5 * SIZE(Y1), %xmm9

	mulsd	 %xmm14, %xmm6
	movsd	 %xmm10, 2 * SIZE(Y1)
	movsd	 6 * SIZE(Y1), %xmm10

	mulsd	 %xmm14, %xmm7
	movsd	 %xmm11, 3 * SIZE(Y1)
	movsd	 7 * SIZE(Y1), %xmm11

	addsd	 %xmm4,  %xmm8
	addsd	 %xmm5,  %xmm9
	addsd	 %xmm6,  %xmm10
	addsd	 %xmm7,  %xmm11

	movsd	 %xmm8,  4 * SIZE(Y1)
	movsd	 %xmm9,  5 * SIZE(Y1)
	movsd	 %xmm10, 6 * SIZE(Y1)
	movsd	 %xmm11, 7 * SIZE(Y1)

	addq	 $8 * SIZE, Y1
	addq	 $8 * SIZE, A1
	ALIGN_3

.L25:
	testq	$4, M
	je	.L27

	movsd	 0 * SIZE(A1), %xmm0
	movsd	 1 * SIZE(A1), %xmm1
	movsd	 2 * SIZE(A1), %xmm2
	movsd	 3 * SIZE(A1), %xmm3

	movsd	 0 * SIZE(Y1), %xmm8
	mulsd	 %xmm14, %xmm0
	movsd	 1 * SIZE(Y1), %xmm9
	mulsd	 %xmm14, %xmm1
	movsd	 2 * SIZE(Y1), %xmm10
	mulsd	 %xmm14, %xmm2
	movsd	 3 * SIZE(Y1), %xmm11
	mulsd	 %xmm14, %xmm3

	addsd	 %xmm0,  %xmm8
	addsd	 %xmm1,  %xmm9
	addsd	 %xmm2,  %xmm10
	addsd	 %xmm3,  %xmm11

	movsd	 %xmm8,  0 * SIZE(Y1)
	movsd	 %xmm9,  1 * SIZE(Y1)
	movsd	 %xmm10, 2 * SIZE(Y1)
	movsd	 %xmm11, 3 * SIZE(Y1)

	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, A2
	addq	 $4 * SIZE, Y1
	ALIGN_3

.L27:
	testq	$2, M
	je	.L28

	movsd	 0 * SIZE(A1), %xmm0
	movsd	 1 * SIZE(A1), %xmm1

	mulsd	 %xmm14, %xmm0
	movsd	 0 * SIZE(Y1), %xmm8
	mulsd	 %xmm14, %xmm1
	movsd	 1 * SIZE(Y1), %xmm9

	addsd	 %xmm0,  %xmm8
	addsd	 %xmm1,  %xmm9

	movsd	 %xmm8, 0 * SIZE(Y1)
	movsd	 %xmm9, 1 * SIZE(Y1)

	addq	 $2 * SIZE, A1
	addq	 $2 * SIZE, Y1
	ALIGN_3

.L28:
	testq	$1, M
	je	.L990

	movsd	 0 * SIZE(Y1), %xmm8
	movsd	 0 * SIZE(A1), %xmm0

	mulsd	 %xmm14, %xmm0
	addsd	 %xmm0,  %xmm8

	movsd	 %xmm8, 0 * SIZE(Y1)
	ALIGN_3

.L990:
	cmpq	$SIZE, INCY
	je	.L999

	movq	Y,  Y1

	movq	M,  %rax
	sarq	$2, %rax
	jle	.L994
	ALIGN_3

.L992:
	movsd	(Y), %xmm0
	addq	INCY, Y
	movsd	(Y), %xmm1
	addq	INCY, Y
	movsd	(Y), %xmm2
	addq	INCY, Y
	movsd	(Y), %xmm3
	addq	INCY, Y

	addsd	0 * SIZE(BUFFER), %xmm0
	addsd	1 * SIZE(BUFFER), %xmm1
	addsd	2 * SIZE(BUFFER), %xmm2
	addsd	3 * SIZE(BUFFER), %xmm3
	addq	$4 * SIZE, BUFFER

	movsd	%xmm0, (Y1)
	addq	INCY, Y1
	movsd	%xmm1, (Y1)
	addq	INCY, Y1
	movsd	%xmm2, (Y1)
	addq	INCY, Y1
	movsd	%xmm3, (Y1)
	addq	INCY, Y1

	decq	%rax
	jg	.L992
	ALIGN_3

.L994:
	testq	$2, M
	jle	.L996

	movsd	(Y), %xmm0
	addq	INCY, Y
	movsd	(Y), %xmm1
	addq	INCY, Y

	addsd	0 * SIZE(BUFFER), %xmm0
	addsd	1 * SIZE(BUFFER), %xmm1
	addq	$2 * SIZE, BUFFER

	movsd	%xmm0, (Y1)
	addq	INCY, Y1
	movsd	%xmm1, (Y1)
	addq	INCY, Y1
	ALIGN_3

.L996:
	testq	$1, M
	jle	.L999

	movsd	(Y), %xmm0

	addsd	(BUFFER), %xmm0

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
	EPILOGUE
